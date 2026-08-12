"""每小時健康檢查（watchdog）：確保爬蟲永遠有在跑，死了就拉回來。

由 launchd 每小時觸發一次。做的事：
  1. supervisor 還活著嗎？    沒有 → 重新啟動
  2. 爬蟲子程序還活著嗎？     沒有 → 由 supervisor 處理（這裡只記錄）
  3. DB 還回應嗎？            SELECT 1 + MAX(updated_at) FROM parts
  4. 進度有沒有卡住？         距離上次成功寫入 > HANG_TIMEOUT → 記錄警訊
  5. 全部結果寫入 logs/watchdog.log（人類可讀 + 一行摘要）

這是「最後一道防線」：supervisor 是主要守護，watchdog 防止 supervisor
本身掛掉時沒人把它拉回來（今天就是 supervisor 從頭到尾沒被 launchd 管理）。

回傳碼：0 = 正常；1 = 有問題需人工處理；2 = 其他異常。
"""

import datetime as _dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
LOG_DIR = BASE / "logs"
SUPERVISOR_MOD = "src.supervisor"
HANG_TIMEOUT = 20 * 60  # 與 supervisor.py 一致
STATUS_FILE = LOG_DIR / "watchdog_status.json"

# spawn 後等待 supervisor 初始化的秒數；crawler 短暫不在時的緩衝重查秒數
# （獨立成常數讓測試可以縮短，不必真的等 6+8 秒）。
SPAWN_WAIT_SECONDS = 6
CRAWLER_RECHECK_SECONDS = 8

# 精確的程序偵測 regex：只匹配「python[3] -m src.supervisor/run_crawl」
# 或「python[3] /path/to/src/xxx.py」形式，不匹配命令列「含該字串」的
# 無關程序（例如監控 shell 的 rg/grep —— 用 pgrep -f 子字串比對會被
# 誤判成 crawler 存活，造成假健康）。
_SUPERVISOR_RE = re.compile(
    r"(?:^|\s)\S*[Pp]ython[\d.]*(?:\s+)(?:-m\s+src\.supervisor|"
    r"\S*src[/\\]supervisor\.py)(?:\s|$)",
)
_CRAWLER_RE = re.compile(
    r"(?:^|\s)\S*[Pp]ython[\d.]*(?:\s+)(?:-m\s+src\.run_crawl|"
    r"\S*src[/\\]run_crawl\.py)(?:\s|$)",
)


def _log(msg: str, level: str = "INFO"):
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    print(line)
    try:
        with open(LOG_DIR / "watchdog.log", "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def _mysql(args: list, db=None):
    """用 PSQ_DB_* 設定執行 mysql；失敗回傳 None。"""
    env = dict(os.environ)
    password = os.environ.get("PSQ_DB_PASS", "root")
    if password:
        env["MYSQL_PWD"] = password
    else:
        env.pop("MYSQL_PWD", None)
    mysql_bin = os.environ.get("PSQ_MYSQL_BIN") or shutil.which("mysql")
    if not mysql_bin:
        return None
    cmd = [
        mysql_bin,
        "-h",
        os.environ.get("PSQ_DB_HOST", "127.0.0.1"),
        "-P",
        os.environ.get("PSQ_DB_PORT", "3308"),
        "-u",
        os.environ.get("PSQ_DB_USER", "root"),
    ]
    database = os.environ.get("PSQ_DB_NAME", "partsouq_crawler") if db is None else db
    if database:
        cmd.append(database)
    cmd += args
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def _is_running(which: str) -> bool:
    """精確判斷目標程序（supervisor / crawler）是否存活。

    用 ps 全命令列 + 錨定 regex，取代 pgrep -f 的子字串比對：後者會
    匹配任何命令列含該字串的程序（例如除錯時 grep 'src.run_crawl'
    的 shell），造成假陽性。
    """
    pat = _SUPERVISOR_RE if which == "supervisor" else _CRAWLER_RE
    try:
        out = subprocess.run(["ps", "-eo", "args="], capture_output=True, text=True, timeout=10)
        return any(pat.search(line) for line in out.stdout.splitlines())
    except Exception:
        return False


def _month_crawl_done() -> bool:
    """當月的 run 是否已標 success（supervisor 正常退場的依據）。"""
    run_key = _dt.datetime.now().strftime("%Y-%m")
    out = _mysql(
        [
            "-N",
            "-e",
            f"SELECT status FROM crawl_runs WHERE run_key='{run_key}' ORDER BY id DESC LIMIT 1",
        ]
    )
    return out == "success"


def main() -> int:
    LOG_DIR.mkdir(exist_ok=True)
    status = {
        "checked_at": _dt.datetime.now().isoformat(),
        "supervisor": False,
        "crawler": False,
        "db_alive": False,
        "last_write": None,
        "parts_count": None,
        "stalled": False,
    }

    # 1. supervisor 存活
    sup_alive = _is_running("supervisor")
    status["supervisor"] = bool(sup_alive)
    sup_was_down = not sup_alive
    clean_done_exit = False
    if not sup_alive:
        _log("supervisor NOT running — restarting", "WARN")
        try:
            proc = subprocess.Popen(
                [sys.executable, "-m", SUPERVISOR_MOD],
                cwd=str(BASE),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            _log(f"supervisor spawned (pid={proc.pid})")
            time.sleep(SPAWN_WAIT_SECONDS)
            # 重新確認 supervisor 是否真的存活（P1 修復）：spawn 後若
            # 立刻崩潰（import 錯誤、DB 起不來等），舊邏輯不回查、
            # 只要 DB 正常就回傳 0，製造假的「已修好」健康訊號。
            if proc.poll() is not None:
                if proc.returncode == 0 and _month_crawl_done():
                    # supervisor 乾淨退場 = 當月爬取已完成，是健康狀態；
                    # crawler 不在是正常的。
                    _log("supervisor exited cleanly (crawl already complete)")
                    sup_alive = True
                    clean_done_exit = True
                else:
                    _log(f"supervisor exited immediately (rc={proc.returncode})", "ERROR")
                    sup_alive = False
            else:
                sup_alive = True
        except Exception as e:
            _log(f"failed to spawn supervisor: {e}", "ERROR")
            sup_alive = False
        status["supervisor"] = bool(sup_alive)

    # 2. crawler 存活（可能剛由 supervisor 帶起）
    status["crawler"] = bool(_is_running("crawler"))
    # supervisor 在跑但 crawler 短暫不在 = 正在換代重啟，緩衝再確認一次
    if sup_alive and not status["crawler"]:
        time.sleep(CRAWLER_RECHECK_SECONDS)
        status["crawler"] = bool(_is_running("crawler"))

    # 3. DB 健康 + 進度
    alive = _mysql(["-e", "SELECT 1 AS x"])
    status["db_alive"] = bool(alive)
    if alive:
        row = _mysql(["-N", "-e", "SELECT MAX(updated_at), COUNT(*) FROM parts"])
        if row:
            parts = row.split("\t")
            status["last_write"] = parts[0] if len(parts) > 0 else None
            status["parts_count"] = parts[1] if len(parts) > 1 else None
            if status["last_write"]:
                try:
                    last = _dt.datetime.fromisoformat(status["last_write"])
                    age = time.time() - last.timestamp()
                    status["stalled"] = age > HANG_TIMEOUT
                    if status["stalled"]:
                        _log(
                            f"STALLED: last parts write {age / 60:.0f}m ago "
                            f"(> {HANG_TIMEOUT // 60}m)",
                            "ERROR",
                        )
                except ValueError:
                    pass

    # 4. 摘要
    try:
        (STATUS_FILE).write_text(json.dumps(status, indent=2, ensure_ascii=False))
    except OSError as e:
        _log(f"status file write failed: {e}", "WARN")

    summary = (
        f"supervisor={'OK' if status['supervisor'] else 'DOWN'} "
        f"crawler={'OK' if status['crawler'] else 'DOWN'} "
        f"db={'OK' if status['db_alive'] else 'DOWN'} "
        f"parts={status['parts_count'] or '?'} "
        f"last={status['last_write'] or '?'} "
        f"stalled={'YES' if status['stalled'] else 'no'}"
    )
    _log(summary)

    # 回傳碼語意：
    #   0 = 完全健康；1 = 需要人工處理（supervisor 拉不起來、DB 掛、卡死）
    # sup_was_down 在 spawn「前」擷取，代表「這輪我們嘗試拉起過」：
    #   若 spawn 後 supervisor 仍起不來（status 仍 False）→ 異常，回 1；
    #   若已成功拉起（status True）但 crawler 仍 DOWN（section 2 已給過
    #     8 秒緩衝）→ 監督未能帶起 crawler，回 1（P1 修復：舊邏輯只檢查
    #     supervisor/DB/stalled，crawler DOWN 也會誤回 0 = 完全健康）；
    #   若 supervisor 本來就在、crawler 卻 DOWN → 監督失能，回 1。
    if sup_was_down:
        if clean_done_exit:
            # supervisor 因當月已完成而乾淨退場：crawler 不在是正常狀態，
            # 只要 DB 健康且未卡死即回 0。
            if status["stalled"] or not status["db_alive"]:
                return 1
            return 0
        if not status["supervisor"] or not status["crawler"]:
            return 1
        if status["stalled"] or not status["db_alive"]:
            return 1
        return 0
    # supervisor 一直都在，但 crawler 消失 = 監督失能，異常
    if not status["crawler"]:
        return 1
    if status["stalled"] or not status["db_alive"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
