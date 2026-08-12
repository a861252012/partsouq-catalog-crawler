"""自癒監督迴圈：讓每月無人值守的爬蟲永遠有人顧。

launchd 每個月觸發一次本程式。它負責「擁有」爬蟲子程序，並執行
一連串健康檢查（loop of checks），確保全程不需要任何人介入：

  1. 爬蟲程序還活著嗎？        -> 崩潰就重啟
  2. 有別的爬蟲在跑嗎？        -> 收養/接管，避免雙寫資料庫
  3. 爬蟲最近有進度嗎？        -> 卡住就重啟（心跳檢查）
  4. 爬蟲記憶體有沒有洩漏？    -> RSS 超過上限就重啟
  5. 磁碟空間還夠嗎？          -> 不足時記錄並提前退場
  6. 資料庫還健康嗎？          -> SELECT 1 失敗時警告
  7. cookie 還新鮮嗎？         -> TTL 到期前主動預先刷新
  8. 爬取完成沒？              -> 全部品牌完成就乾淨退出
  9. 總執行時限到了嗎？        -> 超過上限（25 天）強制結束

重啟風暴保護：在時間窗口內重啟超過 RESTART_MAX 次，監督迴圈會
進入長時間冷卻，而不是繼續狂打網站。每趟結束時把重啟次數、原因
與計數寫入 logs/summary.json，一個月後 10 秒內就能判斷這趟是否正常。
"""

import json
import logging
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from .cloak import COOKIE_TTL
from .config import COOKIE_FILE, CRAWL, LOG_DIR
from .db import Database

log = logging.getLogger("supervisor")

CHECK_INTERVAL = 60  # 健康檢查間隔（秒）
HANG_TIMEOUT = 20 * 60  # 20 分鐘沒有新零件 => 判定卡死，重啟
PROGRESS_QUERY = "SELECT MAX(updated_at) AS last_write FROM parts"
RESTART_MAX = 3  # 每窗口超過 3 次重啟 => 進入冷卻
# 重啟計數器在此時間窗口內有效。SOL review P1：窗口必須**嚴格大於**
# 卡死週期 × 門檻 —— 若固定每 HANG_TIMEOUT 卡死一次且窗口剛好等於
# 週期 × 門檻，第 4 次重啟會剛好把第 1 次排除（now - t == W 不滿足
# now - t < W），永遠只有 3 筆、冷卻永不觸發。多加 2×CHECK_INTERVAL
# 的餘量吸收 tick 粒度（60s）造成的週期抖動。
RESTART_WINDOW = HANG_TIMEOUT * RESTART_MAX + CHECK_INTERVAL * 2  # 20 分鐘卡死 × 3 次門檻 + 餘量
COOLDOWN = 30 * 60  # 重啟風暴後的冷卻時間（秒）
COOKIE_MIN_REMAINING = 5 * 60  # cookie TTL 剩不到 5 分鐘就預先刷新
MEMORY_LIMIT_MB = 2048  # 爬蟲 RSS 超過此值 => 重啟（疑似記憶體洩漏）
DISK_MIN_FREE_MB = 5120  # 磁碟剩餘低於此值（MB）=> 記錄並提前退場
MAX_RUN_SECONDS = 25 * 24 * 3600  # 單趟最長執行時限（25 天）

# crawler 入口的命令列特徵：直譯器大小寫不敏感（macOS 的 Python 安裝在
# /Library/Frameworks/.../MacOS/Python，comm 也可能被截斷）。直譯器 token
# 允許「純 python3」或「絕對路徑結尾是 Python」兩種形式，命令可能是
# 「-m src.run_crawl」或「/path/to/src/run_crawl.py」。
CRAWLER_CMDLINE_RE = re.compile(
    r"^(?:\S*[Pp]ython[\d.]*)(?:\s+)(?:-m\s+src\.run_crawl|"
    r"\S*src[/\\]run_crawl\.py)(?:\s|$)",
)


class Supervisor:
    """監督迴圈：檢查、重啟、冷卻，負責讓爬蟲一路跑到完成。"""

    def __init__(self, workers: int = 4):
        self.workers = workers
        self.proc = None
        self.restarts = []
        # 單一心跳基準：目前 crawler 子程序的啟動時刻（monotonic）。
        # 卡死判斷統一以它為準，避免「寫入老化 + 寬限」疊加造成
        # 約 40 分鐘才偵測到卡死（P1 修復）。
        self.crawler_started_at = 0.0
        self.db = None
        self.cooldown_until = 0.0
        self.started_at = time.monotonic()
        # 這趟的統計（結束時寫入 logs/summary.json）
        self.summary = {
            "restarts": [],
            "cooldowns": 0,
            "started": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished": None,
        }

    # ------------------------------------------------------------ crawler

    def _crawler_cmd(self) -> list:
        """回傳啟動爬蟲子程序的命令列。"""
        return [
            sys.executable,
            "-m",
            "src.run_crawl",
            "--workers",
            str(self.workers),
        ]

    def _kill_other_crawlers(self) -> bool | None:
        """清除其他正在跑的爬蟲程序（排除自己啟動的那隻）。

        用一次 ps 依完整命令列特徵搜尋，排除掉自己啟動的爬蟲。若找到
        代表上一次的 supervisor 已死亡、留下孤兒爬蟲，或有人手動又拉了
        一隻 —— 全部清掉再重新啟動。續爬機制（crawl_state）保證重啟後
        進度不中斷，但絕不能讓兩隻爬蟲同時寫入同一個資料庫。

        排除方式用「pid == self.proc.pid」：self.proc.pid 正是子程序
        的 pid（不是 supervisor 自己的 pid），直接精確對應 ps 列表，
        zombie 狀態下 pid 仍保留在 ps 中，不會誤殺自己的爬蟲。

        回傳 True 代表已確認無 stray；False 代表已找到 stray 但無法
        確認終止；None 代表 ps 等觀測本身失敗。
        """
        try:
            # 用一次 ps 抓「pid、ppid、完整命令列」，直接對 args 比對，
            # 不依賴 comm 欄位（macOS 會把它截斷成不固定的長度，例如
            # /Library/Framewo 或 .../Python.frame，根本無法預期是否
            # 包含 python 字樣 —— 修復前的預先過濾會漏掉真 crawler）。
            ps_out = subprocess.run(
                ["ps", "-eo", "pid=,ppid=,args="],
                capture_output=True,
                text=True,
                timeout=5,
            )
            ps_rc = getattr(ps_out, "returncode", 0)
            if isinstance(ps_rc, int) and ps_rc != 0:
                log.error("cannot enumerate crawler processes: ps rc=%s", ps_out.returncode)
                return None
            crawler_pids = []
            for line in ps_out.stdout.splitlines():
                parts = line.split(None, 2)
                if len(parts) < 3:
                    continue
                pid, ppid, args = parts[0], parts[1], parts[2]
                try:
                    pid_i, ppid_i = int(pid), int(ppid)
                except ValueError:
                    continue
                # 精確比對：命令列是「python[3] -m src.run_crawl ...」
                # 或「python[3] /path/to/src/run_crawl.py ...」。
                # 直譯器大小寫不敏感（真實環境是 .../MacOS/Python），
                # 且不是 shell 或帶其他字元的監控命令。
                if CRAWLER_CMDLINE_RE.search(args):
                    crawler_pids.append((pid_i, ppid_i))
            mine = {self.proc.pid} if self.proc else set()
            others = [pid for pid, _ in crawler_pids if pid not in mine]
            for pid in others:
                # 診斷：被殺的進程是什麼（完整命令 + PPID）
                try:
                    diag = subprocess.run(
                        ["ps", "-o", "ppid=,command=", "-p", str(pid)],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    ).stdout.strip()[:200]
                except Exception:
                    diag = "?"
                log.warning("killing stray crawler pid=%d (%s)", pid, diag)
                killed = subprocess.run(["kill", "-9", str(pid)], capture_output=True)
                kill_rc = getattr(killed, "returncode", 0)
                if isinstance(kill_rc, int) and kill_rc != 0:
                    # ps 與 kill 間的自然退出是正常 race。kill -9 失敗後
                    # 重查 ps；只有 PID 已消失或該 PID 已非 crawler 才能
                    # 視為清理完成。kill -0 的 rc=1 也可能是 EPERM，
                    # 不能當成 ESRCH 放行新 crawler。
                    confirm = subprocess.run(
                        ["ps", "-o", "args=", "-p", str(pid)],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    )
                    current_args = confirm.stdout.strip()
                    if confirm.returncode != 0 or not CRAWLER_CMDLINE_RE.search(current_args):
                        continue
                    log.error(
                        "failed to kill live stray crawler pid=%d (rc=%s)",
                        pid,
                        killed.returncode,
                    )
                    return False
            if others:
                time.sleep(2)
                for pid in others:
                    confirm = subprocess.run(
                        ["ps", "-o", "args=", "-p", str(pid)],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    )
                    if confirm.returncode == 0 and CRAWLER_CMDLINE_RE.search(
                        confirm.stdout.strip()
                    ):
                        log.error("stray crawler pid=%d is still alive", pid)
                        return False
            return True
        except Exception as e:
            log.warning("stray-crawler check failed: %s", e)
            return None

    def start(self) -> bool:
        """啟動爬蟲子程序。若立刻退出則回傳 False。

        啟動前先清掉任何孤兒/重複爬蟲，確保同一時間只有一隻爬蟲
        在寫資料庫。
        """
        if self._kill_other_crawlers() is not True:
            log.error("refusing to start while another crawler may still be alive")
            return False
        log.info("starting crawler child (workers=%d)", self.workers)
        self.proc = subprocess.Popen(
            self._crawler_cmd(),
            cwd=str(Path(__file__).resolve().parent.parent),
            # 子程序的 stdout 不走 crawl.log：run_crawl 自己用
            # RotatingFileHandler 寫 crawl.log，兩者共用同一檔案會
            # 在輪替後造成 fd 失效（內容寫進舊檔）。
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        self.crawler_started_at = time.monotonic()
        return True

    def restart(self, reason: str):
        """殺掉目前的爬蟲（若有的話）並重新啟動；記錄這次重啟。

        超過窗口內的重啟次數上限時進入冷卻，拒絕繼續重啟。
        冷卻期間（cooldown_until 內）每次 restart 都直接拒絕，
        真正的 30 分鐘風暴保護（P1 修復：cooldown_until 原先只被
        設定、從未被讀取，約 15 分鐘窗口過後就會再次重啟）。
        """
        now = time.monotonic()
        if now < self.cooldown_until:
            log.error(
                "cooldown active (until +%.0fs); not restarting: %s",
                self.cooldown_until - now,
                reason,
            )
            # 若進入冷卻時舊 child 無法終止，保留的 proc reference 必須
            # 每 tick 繼續回收；不能因 cooldown 反而 30 分鐘都不再 kill。
            if self.proc is not None:
                self._kill_current(reason)
            if self.proc is None:
                self._kill_other_crawlers()
            return
        self.restarts = [t for t in self.restarts if now - t < RESTART_WINDOW]
        # SOL review P1：先納入本次事件再判斷 —— 舊碼在加入前檢查
        # 門檻，固定週期卡死時第 4 次重啟剛好把窗口邊界上的第 1 次
        # 排除（now - t == W），永遠只有 3 筆、永不進冷卻。
        self.restarts.append(now)
        if len(self.restarts) > RESTART_MAX:
            self.cooldown_until = now + COOLDOWN
            self.summary["cooldowns"] += 1
            log.error(
                "restart storm (%d in window): cooldown until +%.0fs", len(self.restarts), COOLDOWN
            )
            # SOL review P1：進冷卻前先終止故障的 child —— 若當下是
            # 「仍存活但卡死」的 crawler（心跳檢查觸發風暴），舊碼直接
            # return 讓它繼續存在整段 30 分鐘冷卻期，持續打網站。
            self._kill_current(reason)
            return
        self.summary["restarts"].append(
            {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": reason,
            }
        )
        log.warning("restarting crawler: %s", reason)
        if not self._kill_current(reason):
            log.error("cannot restart: old crawler not confirmed dead (%s)", reason)
            return
        self.start()

    def _kill_current(self, reason: str) -> bool:
        """強制結束目前的爬蟲子程序。

        SIGTERM → 等 15 秒 → SIGKILL。回傳 True 代表程序已確認終止
        或本來就不存在；False 代表程序可能仍在執行（D-state 或例外）。

        P1 修復：回傳值讓呼叫端決定是否應啟動新 child —— 舊 PID 無法
        終止時再開新 crawler 會形成雙寫 DB。
        """
        if self.proc is None:
            return True
        pid = getattr(self.proc, "pid", None)
        try:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    log.warning("child %s didn't exit after SIGTERM; SIGKILL", pid)
                    self.proc.kill()
                    try:
                        self.proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        log.error("child %s unkillable (D-state?); not restarting", pid)
                        return False
            self.proc = None
            return True
        except Exception as e:
            log.error("kill_current failed (%s): %s", reason, e)
            # P1 修復：不要清除 self.proc —— terminate()/kill() 丟例外
            # 時（PermissionError、ProcessLookupError 等），舊 child
            # 可能仍存活。若清成 None，下一個 tick 就會啟動新 crawler，
            # 造成雙寫 DB。保留 reference，讓後續 tick 的 poll/terminate
            # 有機會再試（或自然死）。
            return False

    # ------------------------------------------------------------ checks

    def _progress_stalled(self) -> bool:
        """判斷爬蟲是否卡住：HANG_TIMEOUT 內都沒有寫入任何零件。

        以 parts.updated_at 為活動訊號（每次 upsert 的 UPDATE 都會觸發
        ON UPDATE CURRENT_TIMESTAMP，即使資料值不變也會前進，因此
        「健康地重爬既有資料」不會被誤判）。

        基準是單一的 crawler_started_at（start() 設定）：
          - 有新鮮寫入（last_write 在 HANG_TIMEOUT 內）→ 健康
          - 超過 HANG_TIMEOUT 無任何零件寫入（含空表、寫入停滯）→
            若目前 crawler 子程序已存活超過 HANG_TIMEOUT 才判定卡死；
            剛重啟的 crawler 給整個 HANG 寬限期（還沒機會寫第一筆）。

        修復前的問題：last_progress 在「寫入新鮮」期間每 tick 重置，
        寫入停滯後要先等老化 HANG_TIMEOUT，再從最後一次重置等
        HANG_TIMEOUT —— 實際約 40 分鐘才判定卡死。現在統一以單一基準
        HANG_TIMEOUT，寫入停滯 20 分鐘即偵測。
        """
        try:
            row = self.db.query_one(PROGRESS_QUERY)
            last_write = (row or {}).get("last_write")
            if last_write is not None and self._row_age_seconds(last_write) < HANG_TIMEOUT:
                # 資料仍在持續寫入：健康
                return False
            # 到此代表「已超過 HANG_TIMEOUT 沒有任何零件寫入」
            # （含空表、寫入停滯）。
            if self.proc and self.proc.poll() is None:
                return (time.monotonic() - self.crawler_started_at) >= HANG_TIMEOUT
            return True
        except Exception as e:
            log.warning("progress query failed: %s", e)
            return False

    @staticmethod
    def _row_age_seconds(dt) -> float:
        """把 MySQL 的 DATETIME / epoch 整數心跳值換算成距今秒數。"""
        if dt is None:
            return float("inf")
        if isinstance(dt, (int, float)):
            return time.time() - float(dt)
        from datetime import datetime

        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        return time.time() - dt.timestamp()

    def _memory_over_limit(self) -> bool:
        """判斷爬蟲子程序的 RSS 是否超過 MEMORY_LIMIT_MB。"""
        if self.proc is None or self.proc.poll() is not None:
            return False
        try:
            pid = self.proc.pid
            out = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                capture_output=True,
                text=True,
                timeout=5,
            )
            rss_kb = int(out.stdout.strip() or 0)
            return rss_kb > MEMORY_LIMIT_MB * 1024
        except Exception:
            return False

    def _disk_low(self) -> bool:
        """判斷磁碟剩餘空間是否低於安全門檻。"""
        try:
            free = shutil.disk_usage(str(LOG_DIR)).free / (1024 * 1024)
            if free < DISK_MIN_FREE_MB:
                log.error("disk low: %.0f MB free (limit %d MB)", free, DISK_MIN_FREE_MB)
                return True
        except Exception as e:
            log.warning("disk check failed: %s", e)
        return False

    def _db_alive(self) -> bool:
        """判斷資料庫是否還回應（SELECT 1）。"""
        try:
            row = self.db.query_one("SELECT 1 AS x")
            return bool(row)
        except Exception as e:
            log.error("mysql health check failed: %s", e)
            return False

    def _cookie_fresh(self) -> bool:
        """只讀檢查 cookie 檔案的新鮮度，不觸發瀏覽器刷新。

        瀏覽器刷新是 crawler 子程序自己的職責（http_client 的
        ensure_fresh 在每個請求前檢查、403 時觸發 refresh_session，
        single-flight 保證併發 worker 不會重複刷新）。supervisor
        若在這裡呼叫 get_session() 刷新，會與 crawler 進程各自持有一份
        空的 session 狀態，兩邊同時把同一隻 CloakBrowser 當成
        「stale browser」互相殺掉重啟 —— 永遠無法進入正常爬取。
        """
        try:
            age = time.time() - COOKIE_FILE.stat().st_mtime
            return age < COOKIE_TTL
        except OSError:
            return False

    def _crawl_done(self) -> bool:
        """判斷「當月的爬取」是否完成：當月 run_key 有 success 紀錄。

        每月只跑一次（P0 修復）：若當月的 run 已完成，直接退出；
        若機器在月中重啟，會檢查當月 run 是否 success —— 是就退出，
        否則讓 crawler 續爬。不會被上個月的 success 誤導。
        """
        try:
            run_key = time.strftime("%Y-%m")
            row = self.db.query_one(
                "SELECT status FROM crawl_runs WHERE run_key = %s ORDER BY id DESC LIMIT 1",
                (run_key,),
            )
            return bool(row and row.get("status") == "success")
        except Exception as e:
            log.warning("run-status query failed: %s", e)
            return False

    def _cleanup_stale_runs(self):
        """把卡在 running 狀態的舊爬取紀錄標記為 error。

        爬蟲被強殺（kill -9）時 finish_run 來不及執行，會留下
        永遠 running 的紀錄。啟動時清一次，避免誤判（例如把
        上一趟的 running 當成「正在進行」）。

        F1a 連帶修正：判斷基準是「started_at 早於本月一號」（起始月份
        比當前月更早的 running 才是跨月殘留）而非「距今超過 24 小時」
        —— started_at 現在是 logical run 起點（同月重啟不移動），當月
        run 的 started_at 恆在月初，用 24h 判斷會把「正常進行中、只是
        被重啟打斷」的當月 run 誤標 error。
        """
        try:
            self.db._execute(
                "UPDATE crawl_runs SET status = 'error', "
                "error_msg = CONCAT(error_msg, ' | stale running cleaned by supervisor') "
                "WHERE status = 'running' AND started_at < DATE_FORMAT(NOW(), '%Y-%m-01')"
            )
            self.db.commit()
        except Exception as e:
            log.warning("stale-run cleanup failed: %s", e)

    def _write_summary(self, status: str):
        """把這趟的統計寫入 logs/summary.json（事後 10 秒內可判讀）。"""
        self.summary["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
        self.summary["status"] = status
        try:
            (LOG_DIR / "summary.json").write_text(
                json.dumps(self.summary, indent=2, ensure_ascii=False)
            )
            log.info("summary written to %s (status=%s)", LOG_DIR / "summary.json", status)
        except Exception as e:
            log.warning("summary write failed: %s", e)

    # -------------------------------------------------------------- loop

    def run(self) -> int:
        """監督迴圈主體：連接 DB、啟動爬蟲、每 CHECK_INTERVAL 檢查一次。"""
        self.db = Database().connect()
        try:
            self._cleanup_stale_runs()
            if self._crawl_done():
                log.info("crawl already completed; nothing to do")
                if self._kill_other_crawlers() is not True:
                    log.error("completed run has an unconfirmed crawler process")
                    return 1
                self._write_summary("already-complete")
                return 0
            if not self.start():
                return 1
            while True:
                time.sleep(CHECK_INTERVAL)
                self._tick()
        finally:
            for attempt in range(3):
                if self.proc is None or self._kill_current("supervisor exiting"):
                    break
                if attempt < 2:
                    time.sleep(1)
            if self.proc is not None:
                log.critical("supervisor exiting with child ownership unresolved")
            if self.db:
                self.db.close()

    def _tick(self):
        """單次健康檢查（迴圈的核心）。

        依序檢查：程序存活 → 重複爬蟲 → 心跳 → 記憶體 → 磁碟 → DB
        → cookie → 完成 → 時限。整段用 try/except 包住：任何一個
        檢查炸掉都不許讓監督迴圈本身死亡（它是唯一會把爬蟲拉回
        來的東西）。
        """
        try:
            self._tick_inner()
        except Exception:
            log.exception("tick failed; supervisor continues")

    def _tick_inner(self):
        """實際的檢查順序（見 _tick 的 docstring）。"""
        # 1. 程序存活：崩潰的子程序必須被拉回來
        if self.proc is not None:
            rc = self.proc.poll()
            if rc is not None:
                if self._crawl_done():
                    log.info("crawler exited (rc=%s) and crawl marked success: done", rc)
                    self._write_summary("success")
                    sys.exit(0)
                self.restart(f"crawler exited with rc={rc}")
                return
            if time.monotonic() < self.cooldown_until:
                if not self._kill_current("cooldown active"):
                    log.error("cooldown child still alive; will retry next tick")
                return
        else:
            if time.monotonic() < self.cooldown_until:
                if self._kill_other_crawlers() is not True:
                    log.error("cooldown active; stray-crawler cleanup is inconclusive")
                log.error("cooldown active; not starting crawler")
                return
            self.start()
            return

        # 1b. 重複爬蟲：若有人又拉了第二隻（例如手動），清掉
        stray_status = self._kill_other_crawlers()
        if stray_status is False:
            # 已確認另一隻 crawler 存在且殺不掉；不能讓 owned
            # child 繼續雙寫。留下 proc=None，下一 tick 再 fail-closed 清理。
            self._kill_current("unresolved stray crawler")
            return
        if stray_status is None:
            # 只是 ps 觀測失敗時，owned child 的 crawler.lock 仍是單實例
            # 保護；啟動新 child 的 start() 仍會 fail closed。
            log.warning("stray-crawler check inconclusive; crawler lock remains authoritative")

        # 2. 心跳：太久沒有寫入資料庫 => 卡死，重啟
        if self._progress_stalled():
            self.restart(f"no parts written for > {HANG_TIMEOUT // 60} minutes")
            return

        # 2b. 記憶體：RSS 無上限成長 => 洩漏，重啟（續爬機制保證安全）
        if self._memory_over_limit():
            self.restart(f"crawler RSS exceeded {MEMORY_LIMIT_MB}MB")
            return

        # 2c. 磁碟：空間不足 => 記錄並提前退場（寫進去的資料最值錢）
        if self._disk_low():
            log.error("disk space critical: aborting this run")
            if not self._kill_current("disk full"):
                log.error("disk-low child still alive; supervisor will retry next tick")
                return
            self._write_summary("disk-full-abort")
            sys.exit(1)

        # 2d. 資料庫健康：連不上就沒必要繼續檢查下去
        if not self._db_alive():
            log.error("mysql unreachable; skipping remaining checks")
            return

        # 3. cookie 新鮮度：過期就記 warning（刷新是 crawler 自己的職責，
        #    supervisor 只觀察、不碰瀏覽器，避免與 crawler 搶同一隻）
        if not self._cookie_fresh():
            log.warning("cookie file older than TTL; crawler will refresh on demand")

        # 4. 完成：最後一次爬取已成功 => 收工
        #    （爬蟲本身也會自行退出，這裡是兜底處理）
        if self._crawl_done():
            log.info("crawl completed successfully")
            if not self._kill_current("crawl completed"):
                log.error("completed child still alive; supervisor will retry next tick")
                return
            self._write_summary("success")
            sys.exit(0)

        # 5. 總時限：超過 25 天 => 強制結束，讓下個月乾淨開場
        if time.monotonic() - self.started_at > MAX_RUN_SECONDS:
            log.error("max run time reached; forcing clean exit")
            if not self._kill_current("max run time reached"):
                log.error("timed-out child still alive; supervisor will retry next tick")
                return
            self._write_summary("timeout-abort")
            sys.exit(1)


def main():
    import logging.handlers

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.handlers.RotatingFileHandler(
                LOG_DIR / "supervisor.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
            ),
            # launchd 會把 stdout 寫到無上限的 launchd.out.log；
            # 在 launchd 環境下不要重複寫 stdout
            *(
                []
                if "LAUNCHD_JOB" in __import__("os").environ
                else [logging.StreamHandler(sys.stdout)]
            ),
        ],
    )
    import argparse

    parser = argparse.ArgumentParser(description="PartSouq 爬蟲監督迴圈")
    parser.add_argument("--workers", type=int, default=int(CRAWL.get("workers", 4)))
    args = parser.parse_args()

    # P1 修復（單實例鎖）：watchdog（每小時）與 launchd 每月 job 都可能
    # 拉起 supervisor；兩隻並存時各自的 _kill_other_crawlers 會互殺對方
    # 的爬蟲，形成永不停歇的重啟爭奪。flock 拿到獨佔鎖的才是唯一實例，
    # 後到者直接乾淨退場（exit 0，watchdog 視為健康）。
    import fcntl

    lock_path = LOG_DIR / "supervisor.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(lock_path, "a")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.info("another supervisor holds the lock; exiting")
        return 0

    # launchd 以 SIGTERM 停服務時，預設處理會直接終止 interpreter，
    # Supervisor.run 的 finally 不一定有機會回收 child。轉成 SystemExit
    # 後仍保留標準退出語意，並確保 finally 執行。
    def _terminate(signum, _frame):
        # 第一次 TERM 轉成 SystemExit 讓 run.finally 回收 child；隨即忽略
        # 後續 TERM，避免 cleanup sleep/kill 被第二個 signal 重入中斷。
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, _terminate)
    except ValueError:
        # 測試可在非 main thread 呼叫 main；production launchd 一定在
        # main thread，會安裝 handler。
        pass
    return Supervisor(workers=args.workers).run()


if __name__ == "__main__":
    sys.exit(main())
