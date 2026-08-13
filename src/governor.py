"""全域 request governor：讓「總請求率」與 worker 數脫鉤（F5 效能優化）。

問題：每個 worker 各自 sleep 2~5 秒，增加 workers 會線性增加總請求
率，且同時醒來時可能形成突發流量（短暫加速換來 45 秒封鎖）。

設計：全 crawler 共用一個 token bucket ——
- acquire()：每次「wire request」發送前取得全域時槽（阻塞直到有 token）。
  worker 數只控制 in-flight 數，不決定總 request rate。重試也必須走
  acquire（SOL P1）：否則拿一次 token 卻送出 5 次請求，限流等於虛設。
- throttle(seconds)：429 Retry-After / 反爬偵測時「暫停所有 workers」
  到指定時刻（等待期間 acquire 同樣阻塞）。
- slow()：偵測到反爬後速率砍半一段時間，自動恢復。

等待一律用 threading.Condition（等待時釋放鎖）：若持有鎖 sleep，
收到 429 的 worker 會卡在鎖外、throttle 無法即時生效（SOL P1）。
"""

import threading
import time

# token bucket 參數下限：防止設定錯誤造成除零或無限等待
MIN_RATE = 0.05
MIN_BURST = 1
# slow 期間的速率倍率（砍半）與預設持續秒數
SLOW_FACTOR = 0.5
SLOW_DEFAULT_SECONDS = 300.0


class RequestGovernor:
    """全域請求閘門：所有 worker 共用的 token bucket。

    acquire() 必須在每次 wire request 前呼叫；throttle()/slow() 由
    任一 worker 觸發後影響所有 worker。
    """

    def __init__(self, rate: float, burst: int):
        self._rate = max(float(rate), MIN_RATE)  # token/s
        self._burst = max(int(burst), MIN_BURST)
        self._tokens = float(self._burst)
        self._last = time.monotonic()
        self._cond = threading.Condition()
        self._block_until = 0.0
        self._slow_until = 0.0

    def _refill(self, now: float) -> float:
        """依流逝時間累積 token，回傳目前的速率（token/s）。"""
        rate = self._rate * SLOW_FACTOR if now < self._slow_until else self._rate
        self._tokens = min(self._burst, self._tokens + (now - self._last) * rate)
        self._last = now
        return rate

    def acquire(self):
        """取得一個請求時槽（必要時阻塞等待；等待時釋放鎖）。"""
        with self._cond:
            while True:
                now = time.monotonic()
                if now < self._block_until:
                    self._cond.wait(timeout=self._block_until - now)
                    continue
                rate = self._refill(now)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                self._cond.wait(timeout=(1.0 - self._tokens) / rate)

    def throttle(self, seconds: float):
        """暫停所有 workers 至少 seconds 秒（429 / 反爬偵測）。"""
        if seconds <= 0:
            return
        with self._cond:
            now = time.monotonic()
            self._block_until = max(self._block_until, now + seconds)
            # cooldown 期間不累積完整 burst。解除時只允許第一個 waiter
            # 立即通過，其餘照正常 rate 逐一取得 token。
            self._tokens = 1.0
            self._last = self._block_until
            self._cond.notify_all()

    def slow(self, seconds: float = SLOW_DEFAULT_SECONDS):
        """偵測到反爬後降速（速率砍半），持續 seconds 秒後自動恢復。"""
        if seconds <= 0:
            return
        with self._cond:
            self._slow_until = max(self._slow_until, time.monotonic() + seconds)
            self._cond.notify_all()
