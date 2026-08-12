"""HTTP 傳輸層（基礎設施）：高速爬蟲 + Cloudflare 驗證自動處理。

使用 CloakBrowser 取得的 Cookie 進行請求。若請求碰到 Cloudflare
驗證頁（cf_clearance 過期），會自動透過 CloakBrowser 刷新 session
並重試 —— 全程無人介入。

本層只負責「把 HTML 拿回來」；解析與資料寫入分別屬於解析器層
與 Repository 層。

穩定性與限流的兩個關鍵設計：
1. 連線池刻意只開 2 條（pool_connections / pool_maxsize）：多 worker
   同時撥號時，避免 macOS ephemeral port 耗盡（OSError 49「無法指定
   要求的位址」）。伺服器關閉的閒置 socket（CLOSE_WAIT）會以
   ConnectionError 呈現，由本層的迴圈重建連線池並重試。
2. 每次請求前先 ensure_fresh() 主動確認 cookie 新鮮度；真正碰上
   驗證時才執行完整的刷新流程。全域限速（F5）：每次 wire request
   前呼叫 governor.acquire()，重試也受控 —— adapter 層不做重試
   （max_retries=0），所有重試都回到 get() 的迴圈，每次都會重新
   取得全域時槽（SOL P1）。
"""

import logging
import random
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import requests
from requests.adapters import HTTPAdapter

from .cloak import (
    REFRESH_RETRY_BACKOFF,
    force_refresh_session,
    get_session,
    session_backoff_remaining,
)
from .config import CRAWL

log = logging.getLogger("http")


def _cf_value(cookies) -> str:
    """從 cookie 列表取出 cf_clearance 的值（無則回傳空字串）。

    作為 cookie 版本的訊號：cf_clearance 每次刷新必然改變。
    """
    for c in cookies or []:
        if c.get("name") == "cf_clearance":
            return c.get("value", "")
    return ""


# Cloudflare 驗證頁的特徵片段（出現在頁面前 8000 字元內即視為驗證）
CHALLENGE_MARKERS = (
    "Just a moment",
    "Please wait",
    "請稍候",
    "sec-cpt-",
    "cf-chl",
    "Attention Required",
    "__cf_chl_rt_tk",
    # 新版 Cloudflare 反爬（Turnstile / Managed Challenge）特徵：
    # 舊標記抓不到時，這些頁面會以 HTTP 200 形式混進來（實測約 141KB），
    # 內容沒有零件表格但沒有「Just a moment」—— 不補偵測的話會被當成
    # 合法空頁面，污染 crawl_state（group 4103 事件）。
    "challenge-platform",
    "Turnstile",
    "Verifying you are human",
    "Checking your browser",
    "Managed Challenge",
    "cf-captcha",
)


class ChallengeError(Exception):
    """代表回應內容是 Cloudflare 驗證頁（需要刷新 cookie 後重試）。"""


class NotFoundError(Exception):
    """代表資源在網站端不存在（HTTP 404）。

    對 unit 頁（零件組）是「此 group 沒有資料」的合法狀態，由
    crawl_group 捕獲並視為該組完成；對 locate/pick/vehicle/category
    頁則代表異常，會讓父層標記失敗。用例外而非空字串當 sentinel，
    避免與「空白 HTTP 200」混淆。
    """


class SessionManager:
    """持有 Cookie、按需刷新、並控制請求節奏的 HTTP 工作階段。

    每個 worker 執行緒一個實例，共用同一份 cookie 來源。
    """

    def __init__(self, cookies=None, no_browser=False, gov=None):
        self.session = requests.Session()
        # F5：全域 request governor（可選）。提供時，429 的 Retry-After
        # 會同時暫停「所有」worker —— 限流是全域的，單一 worker 的
        # 退避不該讓其他 worker 繼續撞牆。
        self.gov = gov
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
        )
        # no_browser 模式（除錯用）：只用已存 cookie，絕不啟動瀏覽器
        # 刷新（見 ensure_fresh / get 的處理）。
        self.no_browser = no_browser
        # 連線池與重試設定：見模組文件說明。SOL P1：adapter 層
        # max_retries=0 —— 重試統一由 get() 的迴圈控制，每次迭代都會
        # 重新 acquire 全域時槽，否則 urllib3 層的重試會繞過限流。
        self._mount_adapter()
        self.cookies = cookies
        if cookies:
            self._apply_cookies()

    def _mount_adapter(self):
        """掛上連線池 adapter（2 條連線，adapter 層不做重試）。"""
        adapter = HTTPAdapter(
            pool_connections=2,
            pool_maxsize=2,
            max_retries=0,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _apply_cookies(self):
        """把 cookie 列表**整份**套進 requests 的 cookie jar。

        SOL review P2：先清空 jar 再套用新快照 —— 舊碼用
        session.cookies.update() 只覆寫新快照中存在的鍵，刷新結果
        缺少舊 PHPSESSID 等 cookie 時舊值會殘留在 jar 裡，請求仍
        帶上已失效的舊 session。
        """
        jar = requests.cookies.RequestsCookieJar()
        for c in self.cookies or []:
            jar.set(
                c["name"],
                c["value"],
                domain=c.get("domain", "partsouq.com"),
                path=c.get("path", "/"),
            )
        self.session.cookies.clear()
        self.session.cookies.update(jar)

    def refresh(self) -> bool:
        """透過 single-flight 的 session 管理員重新取得 cookie。

        成功回傳 True。所有 worker 共用同一條刷新路徑，
        因此永遠只會啟動一個瀏覽器。

        no_browser 模式下直接回傳 False（P2 修復）：refresh 是唯一
        沒檢查 no_browser 的入口，直接呼叫時會啟動瀏覽器。
        """
        if self.no_browser:
            return False
        cookies = get_session()
        if not cookies:
            return False
        self.cookies = cookies
        self._apply_cookies()
        return True

    def ensure_fresh(self):
        """在 cookie 快到期前主動刷新（每次請求前呼叫）。

        get_session() 是 single-flight 且 TTL 感知的，所以成本極低：
        cookie 仍新鮮時只做一次時間檢查。刷新時只有一個 worker 真正
        啟動瀏覽器，其餘短暫等待後直接沿用結果。

        cookie 物件會被整份替換，所以 identity 判斷可能誤判（refresh
        後 worker 的 self.cookies 可能仍與 state 指向同一份舊 list，
        導致新 cookie 沒被套上，繼續用舊 cookie 打請求 —— 實際發生：
        刷新後仍拿 403/反爬頁）。改用「cf_clearance 值」比較：它每次
        刷新必然改變，是可靠的版本訊號。
        """
        if self.no_browser:
            return
        cookies = get_session()
        if cookies is None:
            return
        if cookies is not self.cookies and _cf_value(cookies) != _cf_value(self.cookies):
            self.cookies = cookies
            self._apply_cookies()

    def get(self, url: str) -> str:
        """GET 請求：含重試 + 驗證自動刷新。回傳 HTML 文字。

        404 有特殊語意：代表該資源在網站端不存在（例如某車型的某個
        group 頁）。以 NotFoundError 拋出、由呼叫端決定如何處理
        （unit 頁視為「此 group 無資料」，其他頁視為失敗），不與
        「空白 HTTP 200」混淆。

        連續碰到驗證且刷新失敗超過 challenge_retries 次時直接放棄
        該請求（讓監督迴圈/續爬機制接手），避免在 Cloudflare 封鎖
        期間反覆啟動瀏覽器造成「刷新失敗風暴」。

        F4 修復：
        - 重試預算與刷新預算分開：刷新成功**不消耗** HTTP attempt，
          保證刷新後必有 follow-up 請求 —— 舊碼最後一次 attempt 刷新
          成功後迴圈已耗盡，新 cookie 從未被使用就直接拋舊錯誤。
          （單一請求內成功刷新上限 max_refresh_per_request，防一直
          給 challenge 時無限刷新。）
        - 驗證偵測優先於 429：429 + cf-mitigated challenge 標頭先當
          驗證處理（刷新 cookie），不被當一般限流 —— 舊碼 429 檢查
          在前，5 次請求 0 次刷新，永遠過不了。
        """
        last_err = None
        refresh_failures = 0
        refresh_successes = 0
        attempt = 0
        while attempt < CRAWL["max_retries"]:
            attempt += 1
            try:
                # SOL P1：每次 wire request 前取得全域時槽（重試、刷新
                # 後的 follow-up 也都受控）—— 拿一次 token 打 5 次請求
                # 等於沒有限流。throttle 設定的全域暫停也在這裡生效。
                if self.gov is not None:
                    self.gov.acquire()
                r = self.session.get(url, timeout=CRAWL["http_timeout"])
                text = r.text or ""
                # 驗證偵測優先（F4）：403 或任何帶驗證特徵的回應
                # （含 429 + cf-mitigated: challenge）一律進驗證分支。
                if r.status_code in (403,) or self._is_challenge(r, text):
                    raise ChallengeError(f"http {r.status_code} challenge at {url[:100]}")
                if r.status_code == 429:
                    # P2 修復：429 是「限流」不是「驗證被拒」—— 舊碼把
                    # 429 併入 challenge 分支，每次都會殺掉健康的瀏覽器、
                    # 冷啟動重解驗證（~3-4 分鐘），且刷新成功後 failures
                    # 歸零，等於無視退避連續燒瀏覽器。限流應尊重伺服器
                    # 節奏：依 retry-after（或固定下限）休眠後重試，不動
                    # 瀏覽器、不刷新 cookie。
                    last_err = requests.RequestException(f"http 429 rate-limited at {url[:100]}")
                    log.warning(
                        "rate-limited (429) at %s (attempt %d/%d); backing off",
                        url[:100],
                        attempt,
                        CRAWL["max_retries"],
                    )
                    retry_after = self._retry_after_seconds(r)
                    # F5：限流是全域的 —— 讓其他 worker 也一起暫停，
                    # 避免它們在 Retry-After 期間繼續撞牆。
                    if self.gov is not None:
                        self.gov.throttle(retry_after)
                    time.sleep(retry_after)
                    continue
                if r.status_code == 404:
                    raise NotFoundError(f"http 404 at {url[:100]}")
                if not (200 <= r.status_code < 300):
                    # 其他非 2xx（500/502...）不該被當成成功頁面，重試
                    raise requests.RequestException(f"http {r.status_code} at {url[:100]}")
                return text
            except ChallengeError as e:
                last_err = e
                if self.no_browser:
                    # no_browser 模式：不允許啟動瀏覽器刷新，直接放棄
                    log.error("challenge while no-browser mode; giving up on %s", url[:100])
                    break
                if refresh_failures >= CRAWL["challenge_retries"]:
                    log.error(
                        "too many failed refreshes (%d); giving up on %s",
                        refresh_failures,
                        url[:100],
                    )
                    break
                log.warning("challenge hit (attempt %d/%d)", attempt, CRAWL["max_retries"])
                # 收到 challenge = 快取 cookie 已被伺服器拒絕，強制失效並重新刷新。
                # SOL review P2：帶上被拒的 cf_clearance 版本 —— 若其他
                # worker 已把全域 session 刷新成更新版本（延遲返回的舊
                # challenge），直接沿用新 cookie，不再清掉重刷、再啟動
                # 一次瀏覽器。
                cookies = force_refresh_session(_cf_value(self.cookies))
                if not cookies:
                    refresh_failures += 1
                    log.error("cookie refresh failed (%d consecutive)", refresh_failures)
                    self._sleep_with_backoff(attempt)
                    continue
                self.cookies = cookies
                self._apply_cookies()
                # P2 修復：刷新成功後歸零失敗計數 —— 舊碼不歸零，
                # 「fail, fail, success, fail」序列在第 4 次就達
                # challenge_retries 而提前放棄，即使刷新已恢復。
                refresh_failures = 0
                refresh_successes += 1
                if refresh_successes > CRAWL["max_refresh_per_request"]:
                    log.error(
                        "too many successful refreshes (%d); giving up on %s",
                        refresh_successes,
                        url[:100],
                    )
                    break
                # F4 修復：刷新成功不消耗 attempt 預算 —— 保證下一輪
                # 迭代用新 cookie 發 follow-up 請求（舊碼最後一次
                # attempt 刷新成功後沒有第 6 次請求，直接拋舊錯誤）。
                attempt -= 1
                time.sleep(2 + random.random() * 3)
            except requests.exceptions.ConnectionError as e:
                # F5：只有「連線層」失敗（伺服器關閉的過期 socket /
                # CLOSE_WAIT / 連線被拒）才需要重建連線池 —— 500 等
                # 有正常 response 的錯誤 keep-alive 仍健康，舊碼一律
                # 丟棄池化 socket，白費重新撥號。
                last_err = e
                log.warning(
                    "connection error (attempt %d/%d): %s", attempt, CRAWL["max_retries"], e
                )
                self._reset_connections()
                time.sleep(2 + random.random() * 2)
            except requests.exceptions.Timeout as e:
                last_err = e
                log.warning("request timeout (attempt %d/%d): %s", attempt, CRAWL["max_retries"], e)
                self._reset_connections()
                time.sleep(2 + random.random() * 2)
            except requests.RequestException as e:
                # 其他（500/502 等）：有正常 response，連線池保持健康
                last_err = e
                log.warning("request error (attempt %d/%d): %s", attempt, CRAWL["max_retries"], e)
                time.sleep(2 + random.random() * 2)
        raise last_err or RuntimeError(f"get failed: {url[:100]}")

    @staticmethod
    def _retry_after_seconds(r) -> float:
        """從 429 回應的 Retry-After 標頭取得建議等待秒數。

        F4 修復：
        - 支援 HTTP-date 格式（Retry-After: Wed, 21 Oct 2015 ...）——
          舊碼 float() 對日期拋錯，固定退回 65 秒。
        - 設上限 retry_after_cap：伺服器的 Retry-After 可能錯得離譜
          （實測 Retry-After: 999999 ≈ 11 天），無上限會讓 worker
          睡到地老天荒。
        """
        ra = r.headers.get("retry-after")
        secs = REFRESH_RETRY_BACKOFF + 5
        if ra:
            try:
                secs = float(ra)
            except (TypeError, ValueError):
                try:
                    dt = parsedate_to_datetime(ra)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    secs = (dt - datetime.now(UTC)).total_seconds()
                except (TypeError, ValueError):
                    pass
        return min(max(15.0, secs), CRAWL["retry_after_cap"])

    @staticmethod
    def _is_challenge(r, text: str) -> bool:
        """判斷回應是否為 Cloudflare 驗證頁。

        除了檢查正文特徵片段，也檢查回應標頭：Cloudflare 的驗證回應
        會帶 cf-mitigated: challenge 標頭，比只比對文字更可靠、
        也更早偵測到（不必等整個正文下載完）。
        """
        headers = r.headers
        if headers.get("cf-mitigated") == "challenge":
            return True
        if headers.get("cf-chl"):
            return True
        return any(m in text[:8000] for m in CHALLENGE_MARKERS)

    def _reset_connections(self):
        """丟棄所有池化的 keep-alive socket（例如 CLOSE_WAIT 卡住後）。"""
        try:
            self.session.close()
        except Exception:
            pass
        self._mount_adapter()

    def _sleep_with_backoff(self, attempt: int):
        """刷新失敗後的等待。

        P2 修復：與 cloak 的指數退避對齊 —— cloak 退避窗口
        （60s→120s→…→1200s）尚未走完時，以剩餘時間為準；否則才用
        下限（避免伺服器冷卻期間狂打）。
        """
        remaining = session_backoff_remaining()
        if remaining > 0:
            time.sleep(remaining + 5)
            return
        time.sleep(max(REFRESH_RETRY_BACKOFF + 5, 15 * (attempt + 1)))

    def sleep(self):
        """依設定延遲隨機休息（2~5 秒），模擬人類瀏覽節奏。"""
        time.sleep(random.uniform(CRAWL["min_delay"], CRAWL["max_delay"]))
