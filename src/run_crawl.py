"""CLI 進入點：python3 -m src.run_crawl [--brand TOYOTA] [--fresh]

執行整趟 PartSouq 爬取。可續爬：先前完成的型號/車型會自動跳過。
若爬取途中 Cloudflare 的 cookie 過期，HTTP 層會自動透過 CloakBrowser
刷新 session。

本模組是「組合根」（composition root）：組裝資料庫連線、Repository、
HTTP 工作階段與爬蟲服務，然後交給服務層執行 —— 本身不含業務邏輯。
"""

import argparse
import fcntl
import logging
import logging.handlers
import os
import sys

from .cloak import get_session
from .config import CRAWL, LOG_DIR, load_cookies
from .crawler import Crawler
from .db import Database
from .governor import RequestGovernor
from .http_client import SessionManager


def main():
    parser = argparse.ArgumentParser(description="PartSouq 全站爬蟲")
    parser.add_argument("--brand", default=None, help="只爬這個品牌（例如 Toyota）")
    parser.add_argument("--fresh", action="store_true", help="執行前先清除爬取進度（從頭開始）")
    parser.add_argument(
        "--no-browser", action="store_true", help="只用已存 cookie，碰到驗證就直接失敗（除錯用）"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(CRAWL.get("workers", 4)),
        help="並行車型 worker 數（預設取自 PSQ_WORKERS 或 4）",
    )
    args = parser.parse_args()

    # 由 launchd 啟動時 stdout 會寫入無上限的 launchd.out.log：
    # 此時只寫輪替檔，不重複寫 stdout。
    handlers = [
        # 20 MB x 5 輪替：跑好幾天的爬蟲不能讓日誌無限長大
        logging.handlers.RotatingFileHandler(
            LOG_DIR / "crawl.log",
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
        ),
    ]
    if "LAUNCHD_JOB" not in os.environ:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=handlers,
    )
    log = logging.getLogger("main")

    if args.brand:
        CRAWL["start_brand"] = args.brand

    # supervisor 的 flock 只能防兩個 supervisor；直接 CLI（尤其
    # --fresh）也必須共用 crawler lock，否則兩趟 run 會同時重設 state
    # 與發布 snapshot。
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = open(LOG_DIR / "crawler.lock", "a")
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            log.error("another crawler holds the lock; exiting")
            return 2

        # 組合根：組裝各層元件。--fresh 的 reset 已移到 Crawler.run，
        # 與 start_run 在同一交易，不會在 cookie 初始化失敗時先毀進度。
        db = Database().connect()
        crawler = None
        if args.no_browser:
            # no-browser 模式：只用已存 cookie，絕不啟動瀏覽器刷新。
            cookies = load_cookies()
        else:
            cookies = get_session()
        if cookies is None:
            log.warning(
                "no cookies available%s",
                " (no-browser mode; crawling without cookies)"
                if args.no_browser
                else "; challenge will auto-refresh",
            )

        # 全站共用的 request governor：主 session（_brands() 等直發請求）
        # 與 Crawler 的 worker session 共用同一實例，每個 wire request
        # 都受全域限流（SOL P1）。
        governor = RequestGovernor(CRAWL["request_rate"], CRAWL["request_burst"])
        http = SessionManager(cookies, no_browser=args.no_browser, gov=governor)
        crawler = Crawler(http, db, workers=args.workers, governor=governor, fresh=args.fresh)
        counts = crawler.run()
        log.info("crawl complete: %s (status=%s)", counts, crawler.last_status)
        # P2 修復：不完整 run（partial / 殘留錯誤）不該以 exit 0 結束，
        # 否則直接執行、舊 scheduler 或外部監控會誤判成功。
        if crawler.last_status != "success":
            return 1
        return 0
    finally:
        if "crawler" in locals() and crawler is not None:
            crawler.close()
        if "db" in locals():
            db.close()
        lock_fd.close()


if __name__ == "__main__":
    sys.exit(main())
