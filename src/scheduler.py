"""每月排程器：每個月 1 號自動執行整趟爬取（替代方案，已被 launchd 取代）。

保持在背景常駐（launchd/cron 啟動）。每月 1 號 00:00 開始爬取，
之後定期檢查爬蟲是否還活著（長爬取可能跨數小時；崩潰時會以
可續爬的方式重新執行）。

另有 --run-now 參數可立即觸發（用於初次填充與測試）。

注意：現行正式部署改為 launchd + src.supervisor（監督迴圈）。
本模組保留為簡易替代方案。
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta

import schedule

from .config import LOG_DIR

log = logging.getLogger("scheduler")

CRAWL_CMD = [sys.executable, "-m", "src.run_crawl"]


def run_crawl_once():
    """執行一次爬取（同步等待完成），回傳程序回傳碼。"""
    log.info("scheduler: starting monthly crawl at %s", datetime.now().isoformat())
    try:
        result = subprocess.run(CRAWL_CMD, cwd=__file__.rsplit("/", 2)[0])
        log.info("scheduler: crawl finished rc=%d", result.returncode)
        return result.returncode
    except Exception as e:
        log.exception("scheduler: crawl launch failed: %s", e)
        return 1


def next_first_of_month(dt: datetime) -> datetime:
    """回傳下一個 1 號的 00:00（若今天就是 1 號 00:00 則回傳今天）。

    用年月進位計算，避免「+31 天」在不同月份會跳過或重複 1 號
    （例如 9/1 +31 天 = 10/2）。
    """
    if dt.day == 1 and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return dt
    year, month = dt.year, dt.month
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    return datetime(year, month, 1)


def _next_run():
    """計算下次執行的時間（每月 1 號 00:00）。"""
    now = datetime.now()
    nxt = next_first_of_month(now)
    if nxt <= now:
        nxt = next_first_of_month(now + timedelta(days=32))
    return nxt


def main():
    parser = argparse.ArgumentParser(description="PartSouq 每月爬蟲排程器")
    parser.add_argument("--run-now", action="store_true", help="立刻執行爬取")
    parser.add_argument("--loop", action="store_true", help="保持常駐（預設行為）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "scheduler.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    if args.run_now:
        log.info("run-now requested")
        return run_crawl_once()

    # 排程模式：每月 1 號 00:00 執行一次爬取
    schedule.every().day.at("00:00").do(run_crawl_once)

    while True:
        schedule.run_pending()
        nxt = _next_run()
        log.info("scheduler alive; next run ~%s", nxt.isoformat())
        time.sleep(60)


if __name__ == "__main__":
    main()
