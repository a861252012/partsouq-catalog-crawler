# PartSouq Catalog Crawler

以 Python 建置的 PartSouq 全站零件目錄爬蟲。資料會正規化寫入 MySQL，並提供斷點續爬、資料完整性檢查、成功快照、全域限流、Cookie 更新、程序監督與每小時 watchdog。

> 本專案會對第三方網站發送長時間請求。使用前請確認符合網站條款、robots 規範與所在地法規，並維持保守的請求速率。

## 1. 收集範圍

```text
品牌 Brand
└── 型號 Model
    └── 車型 Vehicle
        └── 分類 Category
            └── 零件組 Group
                └── 零件 Part
```

只有完整成功的 run 才會發布新的 `v_parts` current snapshot。失敗或 partial run 不會取代上一份成功快照。`crawl_state`、group receipt 與 `seen_run_id` 用來支援中斷後續爬。

主要功能：

- MySQL 正規化資料模型與冪等 upsert
- 每月全站爬取及指定品牌執行
- model、vehicle、group 層級斷點續爬
- 品牌、分類、群組及零件列完整性檢查
- bounded worker pool 與全域 request governor
- 403、429、網路錯誤及交易失敗分層重試
- CloakBrowser Cookie single-flight 更新與退避
- supervisor、watchdog、launchd 長跑監控
- 僅發布完整 success run 的 `v_parts` 快照

## 2. 專案結構

```text
src/
├── config.py          # DB、網站、瀏覽器與爬取設定
├── db.py              # thread-local MySQL 連線與交易
├── repositories.py    # 資料存取、run state、snapshot
├── parsers.py         # HTML 純函式解析器
├── governor.py        # 全域請求速率控制
├── http_client.py     # HTTP、重試、Cookie 管理
├── cloak.py           # CloakBrowser 整合
├── crawler.py         # 爬取流程與 worker 編排
├── run_crawl.py       # CLI 進入點
├── supervisor.py      # crawler 子程序監督與自動復原
└── scheduler.py       # 舊排程入口；正式長跑請用 supervisor

scripts/watchdog.py    # launchd 每小時存活檢查
migrations/            # 既有資料庫升級腳本
schema.sql             # 新資料庫完整 schema
tests/                 # 單元與 DB integration tests
```

更完整的需求、交易邊界與維運說明請參閱 [PROJECT_REVIEW.md](PROJECT_REVIEW.md)。

---

# 新 MacBook 完整安裝手冊

以下步驟假設新 Mac 尚未安裝本專案。請依順序執行，不要先載入 launchd。

## 3. 安裝前先決定資料來源

GitHub repository **不包含**以下本機資料：

- MySQL 資料庫
- `data/cookies.json`
- `data/cloak_profile/`
- `logs/`
- `.env` 或任何密碼

請先選擇其中一種安裝情境：

### 情境 A：新機從空資料庫重新爬

使用 `schema.sql` 建立全新資料庫。**不要再執行 migrations 001–006**，因為 `schema.sql` 已是最新版。

### 情境 B：把舊 Mac 的既有資料搬到新機

先從舊 Mac 匯出 DB，在新 Mac 還原後，再核對並執行 migrations。不要把資料庫 dump 或 Cookie 上傳到 GitHub。

---

## 4. 安裝 macOS 基礎工具

### 4.1 安裝 Command Line Tools

```bash
xcode-select --install
```

若系統顯示已安裝，可以直接繼續。確認：

```bash
git --version
xcode-select -p
```

### 4.2 安裝 Homebrew

依 [Homebrew 官方安裝頁](https://brew.sh/) 執行安裝指令。安裝完成後，依安裝程式最後顯示的指示，把 `brew shellenv` 加入 `~/.zprofile`。

確認目前 Mac 的 Homebrew prefix：

```bash
brew --prefix
```

- Apple Silicon 通常是 `/opt/homebrew`
- Intel Mac 通常是 `/usr/local`

後續不要手動假設 prefix，應使用 `brew --prefix` 或 `brew --prefix <formula>`。

### 4.3 安裝 Python 3.14 與 MySQL 8.4

```bash
brew update
brew install python@3.14 mysql@8.4
```

本專案使用 MySQL 8 的 SQL 語法。不要改裝 MariaDB。Homebrew 目前仍提供 [Python 3.14](https://formulae.brew.sh/formula/python@3.14) 與 [MySQL 8.4](https://formulae.brew.sh/formula/mysql@8.4)。

設定本次 shell 使用的路徑：

```bash
export PYTHON_BIN="$(brew --prefix python@3.14)/bin/python3.14"
export MYSQL_BIN="$(brew --prefix mysql@8.4)/bin/mysql"
export MYSQLDUMP_BIN="$(brew --prefix mysql@8.4)/bin/mysqldump"
```

確認：

```bash
"$PYTHON_BIN" --version
"$MYSQL_BIN" --version
"$MYSQLDUMP_BIN" --version
```

預期 Python 為 `3.14.x`，MySQL client 為 `8.4.x`。

---

## 5. 設定 GitHub SSH 並下載 private repository

先確認新 Mac 是否已能使用個人 GitHub 帳號：

```bash
ssh -T git@github.com
```

預期看到 GitHub 回覆已成功驗證你的帳號。若尚未設定 SSH key，請依 [GitHub 官方 SSH 文件](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) 建立 key 並加入帳號，完成後再重跑上面的驗證。

下載 repository：

```bash
mkdir -p "$HOME/code"
cd "$HOME/code"
git clone git@github.com:a861252012/partsouq-catalog-crawler.git
cd partsouq-catalog-crawler
```

固定後續會使用的專案路徑：

```bash
export PROJECT_DIR="$(pwd)"
```

確認下載內容與 commit：

```bash
git status -sb
git rev-list --count HEAD
git log -1 --oneline
```

此 repository 初始設計維持單一 root commit；`git rev-list --count HEAD` 應為 `1`。

---

## 6. 建立 crawler Python 環境

在專案內建立 `.venv`：

```bash
cd "$PROJECT_DIR"
"$PYTHON_BIN" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install pytest ruff
```

驗證主要套件與程式可 import：

```bash
python -c "import pymysql, requests, bs4, lxml, html5lib; import src.run_crawl, src.supervisor; print('crawler imports OK')"
```

記錄 launchd 之後要使用的 Python 絕對路徑：

```bash
export APP_PYTHON="$PROJECT_DIR/.venv/bin/python"
"$APP_PYTHON" --version
```

---

## 7. 建立獨立 CloakBrowser 環境

Crawler 主環境與 CloakBrowser 必須分開。repository 目前已知相容版本為 `cloakbrowser==0.4.0`。

```bash
mkdir -p "$HOME/.venvs"
"$PYTHON_BIN" -m venv "$HOME/.venvs/partsouq-cloak"
source "$HOME/.venvs/partsouq-cloak/bin/activate"
python -m pip install --upgrade pip
python -m pip install "cloakbrowser==0.4.0"
python -m cloakbrowser install
python -m cloakbrowser info
```

CloakBrowser 官方目前說明首次使用會下載 Chromium binary；若 `install` 或 `info` 要求帳號／license，依官方流程執行：

```bash
python -m cloakbrowser login
python -m cloakbrowser install
python -m cloakbrowser info
```

官方參考：[PyPI](https://pypi.org/project/cloakbrowser/)、[GitHub](https://github.com/CloakHQ/CloakBrowser)。

記錄絕對路徑：

```bash
export CLOAK_PYTHON="$HOME/.venvs/partsouq-cloak/bin/python"
"$CLOAK_PYTHON" -c "import cloakbrowser; print(cloakbrowser.__file__)"
```

不要把 license key、Cookie 或瀏覽器 profile 寫進 repository。

---

## 8. 啟動與設定 MySQL

### 8.1 啟動 Homebrew MySQL 8.4

```bash
brew services start mysql@8.4
brew services list | grep mysql
```

新安裝預設通常使用 TCP port `3306`，而本專案程式預設是 `3308`。本手冊使用環境變數指定 `3306`，不要求你修改 MySQL server port。

先確認本機可連線：

```bash
"$MYSQL_BIN" -h 127.0.0.1 -P 3306 -u root -e "SELECT VERSION();"
```

若 root 已有密碼，加上 `-p`。建議先執行：

```bash
"$(brew --prefix mysql@8.4)/bin/mysql_secure_installation"
```

### 8.2 建立 crawler 專用帳號

以下範例帳號是 `partsouq`。請把 `REPLACE_WITH_A_LONG_LOCAL_PASSWORD` 換成只用於這台 Mac 的強密碼：

```bash
"$MYSQL_BIN" -h 127.0.0.1 -P 3306 -u root -p
```

進入 MySQL 後執行：

```sql
CREATE USER IF NOT EXISTS 'partsouq'@'127.0.0.1'
  IDENTIFIED BY 'REPLACE_WITH_A_LONG_LOCAL_PASSWORD';
CREATE DATABASE IF NOT EXISTS partsouq_crawler
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON partsouq_crawler.* TO 'partsouq'@'127.0.0.1';
FLUSH PRIVILEGES;
EXIT;
```

在 shell 設定本次操作使用的環境變數：

```bash
export PSQ_DB_HOST="127.0.0.1"
export PSQ_DB_PORT="3306"
export PSQ_DB_USER="partsouq"
export PSQ_DB_PASS="REPLACE_WITH_A_LONG_LOCAL_PASSWORD"
export PSQ_DB_NAME="partsouq_crawler"
export PSQ_MYSQL_BIN="$MYSQL_BIN"
export PSQ_CLOAK_PYTHON="$CLOAK_PYTHON"
export PSQ_WORKERS="2"
```

先驗證帳號，不能成功就不要繼續：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  -e "SELECT CURRENT_USER(), VERSION();"
```

---

## 9. 建立或搬移資料庫

### 9.1 情境 A：全新空資料庫

`schema.sql` 內已包含最新版 table、index、view 與 database 設定：

```bash
cd "$PROJECT_DIR"
"$MYSQL_BIN" -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u root -p \
  < schema.sql
```

這一步使用 root，是因為 `schema.sql` 內含 `CREATE DATABASE IF NOT EXISTS`。Crawler 平常執行仍使用權限限縮在該 DB 的 `partsouq` 帳號。

**全新 DB 到這裡就完成，不要再執行 migrations 001–006。**

驗證 schema：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" -e "SHOW TABLES; SHOW CREATE VIEW v_parts\G"
```

應至少看到：

- `brands`
- `models`
- `vehicles`
- `categories`
- `groups_t`
- `parts`
- `published_parts`
- `crawl_state`
- `crawl_runs`
- `v_parts`

### 9.2 情境 B：搬移舊 Mac 的既有資料

#### 舊 Mac：先停止 crawler 再匯出

在舊 Mac 執行：

```bash
launchctl bootout "gui/$(id -u)/com.partsouq.crawler" 2>/dev/null || true
launchctl bootout "gui/$(id -u)/com.partsouq.crawler.watchdog" 2>/dev/null || true
```

確認沒有 crawler／supervisor：

```bash
ps -axo pid,etime,command | grep -E '[p]ython.*-m src\.(supervisor|run_crawl)'
```

匯出：

```bash
MYSQL_PWD="OLD_DB_PASSWORD" /opt/homebrew/bin/mysqldump \
  -h 127.0.0.1 -P 3308 -u root \
  --single-transaction --routines --triggers --events \
  partsouq_crawler > "$HOME/Desktop/partsouq_crawler.sql"
```

確認 dump 不是空檔後，用 AirDrop、加密磁碟或其他安全方式移到新 Mac；不要提交 GitHub。

#### 新 Mac：還原舊 DB

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" < "$HOME/Desktop/partsouq_crawler.sql"
```

還原後先備份一次，再進行 migration：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQLDUMP_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  --single-transaction --routines --triggers --events \
  "$PSQ_DB_NAME" > "$HOME/Desktop/partsouq_before_migration.sql"
```

#### 依序執行 migrations 001–004

Crawler 與 supervisor 必須維持停止：

Migration 001 會先針對實際 unique key 檢查 `NULL` 全部改成空字串後是否
碰撞。只要其中一組碰撞就會在任何資料修改前停止，必須先人工合併；
不得刪除 collision check 或暫時移除 unique key 硬跑。Migration 的
metadata lock 與 InnoDB row lock 最多等待 30 秒，逾時通常代表還有 writer
或長交易，確認完全停止後直接重跑即可。

```bash
cd "$PROJECT_DIR"

for migration in \
  migrations/001_fix_nullable_unique_keys.sql \
  migrations/002_monthly_run_isolation.sql \
  migrations/003_group_receipt_columns.sql \
  migrations/004_current_snapshot_and_vehicle_identity.sql
do
  echo "Running $migration"
  MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
    -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
    "$PSQ_DB_NAME" < "$migration" || break
done
```

任何 migration 回傳非 0，立刻停止，不要啟動 crawler。

#### migration 005：明確授權 vehicle tree 重建

舊 schema 沒有保存完整的 vehicle identity 欄位。migration 005 可能刪除 normalized vehicle tree，接著由 crawler 重新抓取；`published_parts` 會保留上一份成功快照。

005 會先把所有舊的 `success` run 標為 `error`，不依賴 DB 時區推算目前
月份，避免重建後 crawler 因舊 success 直接退出。接著依
`crawl_state -> parts -> groups_t -> categories -> vehicles`，每批 1,000 列
刪除並 commit；
若遇到 30 秒 lock timeout 或連線中斷，已完成的批次不會回滾，保持 crawler
停止後重跑同一 migration 即可從剩餘資料繼續。這段期間 `v_parts` 仍讀取
原本的 `published_parts`。

只有在你已確認備份存在時才執行：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  --init-command="SET @PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD=1" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" < migrations/005_vehicle_identity_v5_and_category_cid.sql
```

若 005 中斷，不要自行刪 index／column；保留錯誤輸出與備份，先查明再重跑。
不要在中斷期間啟動 crawler。先用 `SHOW PROCESSLIST` 確認沒有 crawler writer
或長交易，再以相同授權指令重跑；不要手動執行無 LIMIT 的
`DELETE FROM vehicles`。

005 完成後執行 group row-count high-water migration：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" < migrations/006_group_high_water.sql
```

006 也可安全重跑。若回傳非 0，保持 crawler 停止並查明後再重跑。

驗證 migrations：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" -e "
    SHOW INDEX FROM vehicles WHERE Key_name='uq_vehicle_identity_v5';
    SHOW INDEX FROM categories WHERE Key_name='uq_cat_cid';
    SHOW COLUMNS FROM vehicles LIKE 'body_style';
    SHOW COLUMNS FROM parts LIKE 'seen_run_id';
    SHOW COLUMNS FROM groups_t LIKE 'verified_row_count';
    SELECT COUNT(*) AS published_parts_count FROM published_parts;
  "
```

---

## 10. 安裝前驗證

回到 crawler venv：

```bash
cd "$PROJECT_DIR"
source .venv/bin/activate
```

### 10.1 執行不碰 DB 的安全測試

```bash
PYTHONDONTWRITEBYTECODE=1 python -m pytest -p no:cacheprovider -q \
  tests/test_crawler_contract.py \
  tests/test_migration_safety.py \
  tests/test_parsers.py \
  tests/test_regressions.py \
  tests/test_stability.py \
  tests/test_supervisor.py

python -m ruff check .
python -m ruff format --check .
PYTHONPYCACHEPREFIX=/tmp/partsouq-pycache \
  python -m compileall -q src tests scripts
```

### 10.2 DB integration tests（選用，但建議新機第一次安裝時執行）

`tests/test_repositories.py` 會在每個測試前 TRUNCATE 測試庫。必須建立獨立的 `partsouq_crawler_test`，絕對不能指向正式 DB。

以 root 建立測試庫及權限：

```sql
CREATE DATABASE IF NOT EXISTS partsouq_crawler_test
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON partsouq_crawler_test.* TO 'partsouq'@'127.0.0.1';
FLUSH PRIVILEGES;
```

產生測試 schema 並匯入：

```bash
sed 's/partsouq_crawler/partsouq_crawler_test/g' \
  schema.sql > /tmp/partsouq_crawler_test_schema.sql

"$MYSQL_BIN" -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u root -p \
  < /tmp/partsouq_crawler_test_schema.sql
```

執行 DB tests：

```bash
python -m pytest -q tests/test_repositories.py
```

執行結束後確認正式 DB 仍存在且沒有被清空。

---

## 11. 手動啟動 crawler

### 11.1 每個新 Terminal 都要先設定環境

```bash
cd "$PROJECT_DIR"
source .venv/bin/activate

export PSQ_DB_HOST="127.0.0.1"
export PSQ_DB_PORT="3306"
export PSQ_DB_USER="partsouq"
export PSQ_DB_PASS="REPLACE_WITH_A_LONG_LOCAL_PASSWORD"
export PSQ_DB_NAME="partsouq_crawler"
export PSQ_MYSQL_BIN="$(brew --prefix mysql@8.4)/bin/mysql"
export PSQ_CLOAK_PYTHON="$HOME/.venvs/partsouq-cloak/bin/python"
export PSQ_WORKERS="2"
```

先確認 DB 與 CloakBrowser：

```bash
python -c "from src.config import DB_CONFIG, CLOAK; print(DB_CONFIG); print(CLOAK['venv_python'])"
"$PSQ_CLOAK_PYTHON" -m cloakbrowser info
```

### 11.2 直接啟動完整爬取

```bash
python -m src.run_crawl --workers 2
```

常用參數：

```bash
# 只爬指定品牌；partial run 不會發布全站 success snapshot
python -m src.run_crawl --brand TOYOTA --workers 1

# 清除 run state 與 receipt 後完整重爬
python -m src.run_crawl --fresh --workers 2

# 只用既有 Cookie，不允許啟動瀏覽器刷新
python -m src.run_crawl --no-browser --workers 1
```

同一時間只能有一個 crawler。第二個實例拿不到 `logs/crawler.lock` 時會退出。

### 11.3 建議正式長跑使用 supervisor

```bash
python -m src.supervisor --workers 2
```

Supervisor 會啟動 `src.run_crawl`，每 60 秒檢查程序、DB、進度、記憶體、磁碟與完成狀態；crawler 崩潰或 20 分鐘沒有新零件時會重啟。

先在 Terminal 前景跑到確認以下事件再設定 launchd：

- CloakBrowser 能啟動並取得 Cookie
- `logs/crawl.log` 有品牌／model／vehicle 進度
- `crawl_runs` 出現當月 `running`
- `parts.updated_at` 持續更新
- 沒有兩個 supervisor 或 crawler 互相重啟

按 `Ctrl+C` 結束前景 supervisor，確認 child 也被清理後再繼續。

---

## 12. 設定 launchd 自動執行

Repository 內兩份 plist 是範例，包含原開發機的絕對路徑。**不能直接複製後啟用**。

### 12.1 複製 plist 到使用者 LaunchAgents

```bash
cd "$PROJECT_DIR"
mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_DIR/logs"

cp com.partsouq.crawler.plist \
  "$HOME/Library/LaunchAgents/com.partsouq.crawler.plist"
cp com.partsouq.crawler.watchdog.plist \
  "$HOME/Library/LaunchAgents/com.partsouq.crawler.watchdog.plist"

export MAIN_PLIST="$HOME/Library/LaunchAgents/com.partsouq.crawler.plist"
export WATCHDOG_PLIST="$HOME/Library/LaunchAgents/com.partsouq.crawler.watchdog.plist"
```

### 12.2 改成新 Mac 的專案與 Python 路徑

```bash
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:0 $PROJECT_DIR/.venv/bin/python" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Set :WorkingDirectory $PROJECT_DIR" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Set :StandardOutPath $PROJECT_DIR/logs/launchd.out.log" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Set :StandardErrorPath $PROJECT_DIR/logs/launchd.err.log" "$MAIN_PLIST"

/usr/libexec/PlistBuddy -c "Set :ProgramArguments:0 $PROJECT_DIR/.venv/bin/python" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:1 $PROJECT_DIR/scripts/watchdog.py" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Set :WorkingDirectory $PROJECT_DIR" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Set :StandardOutPath $PROJECT_DIR/logs/watchdog.launchd.out.log" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Set :StandardErrorPath $PROJECT_DIR/logs/watchdog.launchd.err.log" "$WATCHDOG_PLIST"
```

### 12.3 加入 crawler 所需環境變數

以下命令只對剛複製、尚未有 `EnvironmentVariables` 的 plist 執行一次：

```bash
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables dict" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:LAUNCHD_JOB string 1" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_HOST string $PSQ_DB_HOST" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_PORT string $PSQ_DB_PORT" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_USER string $PSQ_DB_USER" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_PASS string $PSQ_DB_PASS" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_NAME string $PSQ_DB_NAME" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_CLOAK_PYTHON string $PSQ_CLOAK_PYTHON" "$MAIN_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_WORKERS string 2" "$MAIN_PLIST"

/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables dict" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:LAUNCHD_JOB string 1" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_MYSQL_BIN string $MYSQL_BIN" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_HOST string $PSQ_DB_HOST" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_PORT string $PSQ_DB_PORT" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_USER string $PSQ_DB_USER" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_PASS string $PSQ_DB_PASS" "$WATCHDOG_PLIST"
/usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:PSQ_DB_NAME string $PSQ_DB_NAME" "$WATCHDOG_PLIST"
```

Plist 會含 DB 密碼，限制權限：

```bash
chmod 600 "$MAIN_PLIST" "$WATCHDOG_PLIST"
```

### 12.4 手動驗證 watchdog 設定

Watchdog 與 crawler 共用 `PSQ_DB_HOST`、`PSQ_DB_PORT`、`PSQ_DB_USER`、`PSQ_DB_PASS`、`PSQ_DB_NAME`。`PSQ_MYSQL_BIN` 必須是新 Mac 的 MySQL client 絕對路徑。

載入 launchd 前先用相同環境手動驗證：

```bash
cd "$PROJECT_DIR"
"$PROJECT_DIR/.venv/bin/python" scripts/watchdog.py
echo $?
```

回傳碼：

- `0`：健康，或當月已完成而乾淨退場
- `1`：supervisor／crawler／DB／進度需要處理
- `2`：其他異常

> 安全提醒：實際 DB 密碼只存在本機 LaunchAgent plist。不要把已客製化的 plist 複製回 repository，也不要 commit 或 push。

### 12.5 驗證 plist 語法

```bash
plutil -lint "$MAIN_PLIST"
plutil -lint "$WATCHDOG_PLIST"
plutil -p "$MAIN_PLIST"
plutil -p "$WATCHDOG_PLIST"
```

逐項確認：

- Python 是 `$PROJECT_DIR/.venv/bin/python`
- WorkingDirectory 是新 Mac 的 repository
- watchdog.py 是新 Mac 的絕對路徑
- log 路徑指向新 repository 的 `logs/`
- DB 與 CloakBrowser 環境變數正確

### 12.6 載入 launchd

先清除可能存在的舊 label：

```bash
launchctl bootout "gui/$(id -u)" "$MAIN_PLIST" 2>/dev/null || true
launchctl bootout "gui/$(id -u)" "$WATCHDOG_PLIST" 2>/dev/null || true
```

載入：

```bash
launchctl bootstrap "gui/$(id -u)" "$MAIN_PLIST"
launchctl bootstrap "gui/$(id -u)" "$WATCHDOG_PLIST"
```

主 job 預設每月 1 日 00:05 執行，`RunAtLoad=false`。Watchdog 每小時執行且 `RunAtLoad=true`；若 supervisor 不在，watchdog 會嘗試帶起。

第一次安裝要立即啟動，不必等到下月：

```bash
launchctl kickstart -k "gui/$(id -u)/com.partsouq.crawler"
```

---

## 13. 確認 crawler 與監控是否正常

### 13.1 查看 launchd 狀態

```bash
launchctl print "gui/$(id -u)/com.partsouq.crawler"
launchctl print "gui/$(id -u)/com.partsouq.crawler.watchdog"
```

重點查看：

- `state`
- `pid`
- `last exit code`
- `program` / `arguments`
- `working directory`

### 13.2 查看實際程序

```bash
ps -axo pid,ppid,etime,rss,command | \
  grep -E '[p]ython.*-m src\.(supervisor|run_crawl)'
```

正常長跑時通常應看到一個 supervisor 與一個 crawler child。不要只看到 supervisor 就判定爬蟲健康。

### 13.3 即時看 log

```bash
tail -f "$PROJECT_DIR/logs/supervisor.log"
```

另一個 Terminal：

```bash
tail -f "$PROJECT_DIR/logs/crawl.log"
```

Watchdog：

```bash
tail -f "$PROJECT_DIR/logs/watchdog.log"
```

launchd 啟動失敗時先看：

```bash
tail -n 100 "$PROJECT_DIR/logs/launchd.err.log"
tail -n 100 "$PROJECT_DIR/logs/watchdog.launchd.err.log"
```

### 13.4 查看 watchdog JSON

```bash
"$PROJECT_DIR/.venv/bin/python" -m json.tool \
  "$PROJECT_DIR/logs/watchdog_status.json"
```

主要欄位：

- `supervisor`：supervisor 是否存活
- `crawler`：crawler child 是否存活
- `db_alive`：DB 是否可查詢
- `parts_count`：normalized parts 數量
- `last_write`：最後一次零件寫入時間
- `stalled`：是否超過 20 分鐘無寫入

`supervisor=true` 不等於 crawler 正常，也不等於全站完成；必須一起看 `crawler`、`db_alive`、`last_write`、run status 與 error state。

### 13.5 用 DB 確認真實進度

最新 run：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" -e "
    SELECT id, run_key, status, started_at, finished_at,
           brands_ok, models_ok, vehicles_ok, groups_ok,
           parts_ok, parts_new, error_msg
    FROM crawl_runs ORDER BY id DESC LIMIT 5;
  "
```

當月 state：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" -e "
    SELECT run_key, scope, status, COUNT(*) AS total
    FROM crawl_state
    WHERE run_key = DATE_FORMAT(CURDATE(), '%Y-%m')
    GROUP BY run_key, scope, status
    ORDER BY scope, status;
  "
```

零件寫入心跳與 current snapshot：

```bash
MYSQL_PWD="$PSQ_DB_PASS" "$MYSQL_BIN" \
  -h "$PSQ_DB_HOST" -P "$PSQ_DB_PORT" -u "$PSQ_DB_USER" \
  "$PSQ_DB_NAME" -e "
    SELECT COUNT(*) AS normalized_parts, MAX(updated_at) AS last_write FROM parts;
    SELECT COUNT(*) AS published_parts FROM published_parts;
    SELECT COUNT(*) AS current_view_parts FROM v_parts;
  "
```

判讀原則：

- `running`：仍在爬，不代表成功
- `error`：有 incomplete／partial／失敗狀態，需看 `error_msg` 與 `crawl_state`
- `success`：完整閉合並已發布 snapshot
- `parts.updated_at` 持續前進：有 DB 寫入進度
- `v_parts`：最近一次完整 success snapshot；爬取中可能仍維持舊快照

### 13.6 確認每月排程

```bash
plutil -p "$MAIN_PLIST" | grep -A5 StartCalendarInterval
```

應顯示 Day `1`、Hour `0`、Minute `5`。

---

## 14. 停止、重啟與移除服務

停止並卸載：

```bash
launchctl bootout "gui/$(id -u)" "$WATCHDOG_PLIST"
launchctl bootout "gui/$(id -u)" "$MAIN_PLIST"
```

重新載入設定：

```bash
launchctl bootstrap "gui/$(id -u)" "$MAIN_PLIST"
launchctl bootstrap "gui/$(id -u)" "$WATCHDOG_PLIST"
launchctl kickstart -k "gui/$(id -u)/com.partsouq.crawler"
```

只重啟主 job：

```bash
launchctl kickstart -k "gui/$(id -u)/com.partsouq.crawler"
```

不要同時在 Terminal 手動跑另一個 supervisor。雖然 flock 會阻擋第二個實例，但混用會增加判讀困難。

---

## 15. 常見問題

### launchd 顯示找不到 Python 或 module

檢查 plist 的 `ProgramArguments[0]` 是否為：

```text
<新 repository 絕對路徑>/.venv/bin/python
```

再檢查 `WorkingDirectory` 是否為 repository 根目錄。

### `CloakBrowser venv python not found`

確認：

```bash
"$HOME/.venvs/partsouq-cloak/bin/python" -m cloakbrowser info
```

並確認 plist 的 `PSQ_CLOAK_PYTHON` 完全相同。

### watchdog 顯示 DB DOWN，但 crawler 可寫 DB

用 `plutil -p "$WATCHDOG_PLIST"` 檢查 `PSQ_MYSQL_BIN` 與全部 `PSQ_DB_*`。Watchdog 由 launchd 啟動時不會讀 Terminal 內的 `export`，只能使用 plist 的 `EnvironmentVariables`。

### crawler 一直重啟

依序查看：

```bash
tail -n 200 "$PROJECT_DIR/logs/supervisor.log"
tail -n 200 "$PROJECT_DIR/logs/crawl.log"
tail -n 200 "$PROJECT_DIR/logs/launchd.err.log"
```

再查 DB `crawl_state` 的 error、磁碟空間、CloakBrowser info 與 `parts.updated_at`。

### MacBook 闔蓋後停止進度

macOS 進入睡眠時，crawler、MySQL、supervisor 與 launchd 都會暫停。這不是 crawler crash。長跑期間請接電並在系統設定避免自動睡眠；喚醒後檢查 Cookie 刷新與 DB 心跳是否恢復。

### 當月已 success，重新啟動卻立即退出

這是正常行為。每月 `run_key` 已成功時，supervisor／crawler 會把該月視為完成。若確定要重爬，停止服務後手動執行 `--fresh`。

---

## 16. 資料與安全

以下內容已由 `.gitignore` 排除，不應提交：

- `data/cookies.json`
- `data/cloak_profile/`
- `logs/`
- `.env`
- virtualenv 與 cache

Cookie 可能含 `cf_clearance` 與 session 資訊。若曾誤提交，請立即撤銷 Cookie，不能只刪除最新 commit。

LaunchAgent plist 若含 DB 密碼，僅能放在本機 `~/Library/LaunchAgents`，權限設為 `600`，不要覆寫並提交 repository 內的範例 plist。

## 17. 維運輸出

- `logs/crawl.log`：爬取、解析與錯誤
- `logs/supervisor.log`：程序重啟與健康檢查
- `logs/watchdog.log`：每小時 watchdog
- `logs/watchdog_status.json`：最近一次健康狀態
- `logs/summary.json`：完成 run 摘要
- `logs/launchd.err.log`：主 launchd 啟動錯誤
- `logs/watchdog.launchd.err.log`：watchdog launchd 啟動錯誤

## 18. 授權

本 repository 目前未附開源授權。未經權利人明確允許，不代表可複製、修改或散布。
