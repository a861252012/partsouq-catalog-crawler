"""PartSouq 每月爬蟲的設定（集中管理，可用環境變數覆寫）。

這裡是整個專案的「單一事實來源」：資料庫連線、網站網址、CloakBrowser
整合、爬取參數，全部集中在這個模組，其他模組一律從這裡讀取。
"""

import json
import os
from pathlib import Path

# 專案根目錄（src 的上一層）
BASE_DIR = Path(__file__).resolve().parent.parent
# Cookie 的持久化檔案與 CloakBrowser 設定檔目錄
COOKIE_FILE = BASE_DIR / "data" / "cookies.json"
PROFILE_DIR = BASE_DIR / "data" / "cloak_profile"

# MySQL 連線設定（可經由環境變數覆寫；預設連本機 3308 的專用 DB）
DB_CONFIG = {
    "host": os.environ.get("PSQ_DB_HOST", "127.0.0.1"),
    "port": int(os.environ.get("PSQ_DB_PORT", "3308")),
    "user": os.environ.get("PSQ_DB_USER", "root"),
    "password": os.environ.get("PSQ_DB_PASS", "root"),
    "database": os.environ.get("PSQ_DB_NAME", "partsouq_crawler"),
}

# PartSouq 的各層頁面網址模板
SITE = {
    "base": "https://partsouq.com",
    "genuine": "https://partsouq.com/en/catalog/genuine",
    "locate": "https://partsouq.com/en/catalog/genuine/locate?c={brand}",
    "pick": "https://partsouq.com/en/catalog/genuine/pick?c={brand}&model={model}&ssd={ssd}",
    "vehicle": "https://partsouq.com/en/catalog/genuine/vehicle?c={brand}&ssd={ssd}&vid={vid}&q=",
    "unit": "https://partsouq.com/en/catalog/genuine/unit?c={brand}&ssd={ssd}&vid={vid}&cid={cid}&uid={uid}&q=",
}

# CloakBrowser（隱匿瀏覽器）的整合設定
CLOAK = {
    "venv_python": os.environ.get(
        "PSQ_CLOAK_PYTHON",
        str(Path("/Users/a861252012/Desktop/folder/code/.cloak-venv/bin/python")),
    ),
    "cdp_port": 9242,
    "cdp_host": "http://127.0.0.1:9242",
    "cookie_export_file": Path("/tmp/psq_cloak_cookies.json"),
    "user_agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    ),
}

# 爬取行為參數（可用環境變數覆寫；0 代表不設上限）
CRAWL = {
    # 節奏：每個 worker 每個請求之間隨機休息 2~5 秒。
    # 全站總請求速率維持在低檔，避免被 Cloudflare 盯上。
    # 慢一點更安全；爬蟲本來就設計成可以連續跑好幾天。
    "min_delay": float(os.environ.get("PSQ_MIN_DELAY", "2.0")),  # 每請求最小間隔（秒）
    "max_delay": float(os.environ.get("PSQ_MAX_DELAY", "5.0")),  # 每請求最大間隔（秒）
    "http_timeout": 20,  # 單次 HTTP 請求逾時（秒）
    "max_retries": 5,  # 失敗重試次數
    "challenge_retries": 3,  # 碰到驗證時重新取得 cookie 的次數
    "max_refresh_per_request": 3,  # 單一請求內「成功刷新 cookie」的上限（F4：重試與刷新預算分離）
    "retry_after_cap": 300,  # 429 Retry-After 等待上限（秒）（F4：防伺服器給巨額值）
    "start_brand": os.environ.get("PSQ_START_BRAND", ""),  # 只爬指定品牌（空=全部）
    "limit_brands": int(os.environ.get("PSQ_LIMIT_BRANDS", "0")),  # 品牌數上限
    "limit_models": int(os.environ.get("PSQ_LIMIT_MODELS", "0")),  # 每品牌型號數上限（測試用）
    "limit_vehicles": int(os.environ.get("PSQ_LIMIT_VEHICLES", "0")),  # 每型號車型數上限
    "workers": int(os.environ.get("PSQ_WORKERS", "4")),  # 並行 worker 數（in-flight 數，非總速率）
    # F5 全域 request governor：總請求率與 worker 數脫鉤。
    # 預設 0.5 token/s（每 2 秒一個請求）＋ burst 4：2 個 worker 時
    # 速率約等於原本的 2~5s/worker，但 4 個 worker 不會線性翻倍，
    # 也不會在等待結束時形成突發流量。
    "request_rate": float(os.environ.get("PSQ_REQUEST_RATE", "0.5")),  # 全域每秒請求數
    "request_burst": int(os.environ.get("PSQ_REQUEST_BURST", "4")),  # token bucket burst 上限
    # supervisor 的單趟執行期限。Crawler 也使用同一個值記錄最低 request
    # budget；若已知剩餘 group 明顯超出樂觀容量，會在發送網路請求前
    # 警告。此期限在 supervisor 重啟後重算，不能當成跨程序硬 SLA。
    "max_run_days": float(os.environ.get("PSQ_MAX_RUN_DAYS", "25")),
    # 首頁品牌清單的最低品牌數（SOL P1）：首次爬取（空 DB）時閉合檢查
    # 拿「本次解析結果」對「DB 已知」沒有意義（兩者同源），縮水解析
    # 會被誤判成完整。低於此門檻視為網站縮水/反爬頁，run 直接 error。
    # 目前站上有 18 個品牌；若站方增減請用環境變數調整。
    "min_brands": int(os.environ.get("PSQ_MIN_BRANDS", "18")),
    "limit_groups": int(os.environ.get("PSQ_LIMIT_GROUPS", "0")),  # 全站零件組數上限（測試用）
    # 零件數縮水門檻（SOL review P1）：本次解析到的零件數 < 前次
    # receipt 的 row_count × 此比例時視為「格式完整但內容縮水」
    # （反爬變體/版型異常），拒絕寫 terminal receipt。前次 < 3 筆的
    # 小組不做檢查（零件可能被站方合法下架一兩筆）。0 停用。
    "row_count_shrink_ratio": float(os.environ.get("PSQ_ROW_COUNT_SHRINK_RATIO", "0.5")),
    # 疑似反爬頁（大頁面解析 0 零件/0 組）觸發時的強制停頓秒數：
    # Cloudflare 的限速器在 4 worker 連發時會給反爬頁，停頓讓它喘息、
    # 避免 crawler 在封鎖期間用最大速率重錘同一批頁面。
    "block_breather": int(os.environ.get("PSQ_BLOCK_BREATHER", "45")),
}

# 日誌目錄
LOG_DIR = BASE_DIR / "logs"


def load_cookies():
    """從磁碟載入已存 cookie。回傳 dict 列表或 None（不存在/解析失敗）。"""
    if not COOKIE_FILE.exists():
        return None
    try:
        return json.loads(COOKIE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def save_cookies(cookies):
    """把 cookie 寫入磁碟（限所有者讀寫，保護敏感性資料）。"""
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_FILE.write_text(json.dumps(cookies, indent=1))
    COOKIE_FILE.chmod(0o600)
