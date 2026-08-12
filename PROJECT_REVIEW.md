# PartSouq 全站爬蟲專案 — 需求敘述與實作說明

> 本文件供 LLM code review 使用。內容涵蓋：需求、架構、各模組實作細節、
> 已知問題與修復歷程、測試策略、維運與監控、以及給 reviewer 的檢查重點。

---

## 1. 專案概述

### 1.1 需求

從 [partsouq.com](https://partsouq.com)（日本車系原廠零件目錄站）爬取完整的
零件資料，寫入 MySQL，供下游查詢系統使用。

**資料模型（5 層樹狀結構）**：

```
品牌 Brand (TOYOTA, Lexus, Nissan, ...)
 └─ 型號 Model (4RUNNER, COROLLA, ...)
     └─ 車型 Vehicle (ALPHARD/VELLFIRE/HV, AGH30W-NFXGK, ...)
         └─ 分類 Category (ENGINE/FUEL/TOOL, POWER TRAIN/CHASSIS, BODY/INTERIOR, ELECTRICAL)
             └─ 零件組 Group (1101: PARTIAL ENGINE ASSY, ...)
                 └─ 零件 Part (190000V200, ...)
```

**頁面對應**：

| 層級 | 頁面 | URL 特徵 |
|------|------|---------|
| Brand | 首頁 `/en/catalog/genuine` | 側邊欄 `<li> <a href="/locate?c=...">` |
| Model | locate 頁 | 手風琴 `<a href="/pick?c=..&model=..&ssd=..">` |
| Vehicle | pick 頁 | 規格表，`<th>` 帶 class 特徵標記 |
| Category/Group | vehicle 頁 | `<a href="/vehicle?..cid=..">` + `<a href="/unit?..uid=..">` |
| Part | unit 頁 | 零件表，第一格連到 `/search/all?q=` |

**非功能需求**：
- 每月全站爬取（無人值守，可連續跑數天至數週）
- 可斷點續爬（`crawl_state` 表記錄完成狀態）
- 必須規避 Cloudflare 驗證（透過 CloakBrowser 隱匿 Chromium 解 Turnstile）
- 節奏要慢（每請求 2~5 秒隨機延遲），避免被封鎖
- 需有監督機制：程序崩潰自動重啟、卡死偵測、記憶體/磁碟/DB 健康檢查

### 1.2 技術棧

- Python 3.14（僅標準庫 + 6 個依賴）
- `pymysql>=1.1`（MySQL 8，InnoDB，DictCursor）
- `requests>=2.31`
- `beautifulsoup4>=4.12`（lxml parser）
- `html5lib>=1.1`
- CloakBrowser（外部安裝在 `.cloak-venv`，透過 CDP 控制隱匿 Chromium）
- launchd 管理監督迴圈與 watchdog

---

## 2. 架構與模組職責

```
src/
├── config.py          # 集中設定（DB、SITE URL、CLOAK、CRAWL 參數）—— 單一事實來源
├── db.py              # MySQL 連線管理（執行緒本地連線、斷線偵測、交易）
├── repositories.py    # 資料存取層（upsert、計數、狀態）
├── parsers.py         # HTML 轉換層（純函式：HTML → dict）
├── http_client.py     # HTTP + Cookie 管理（重試、403/429、驗證自動刷新）
├── cloak.py           # CloakBrowser 整合（啟動瀏覽器、解驗證、匯出 cookie）
├── crawler.py         # 服務層（編排整趟爬取、worker 池、續爬）
├── run_crawl.py       # CLI 進入點（組合根）
├── supervisor.py      # 監督迴圈（自癒：重啟、卡死偵測、健康檢查）
└── scheduler.py       # 排程器（較舊，見 §7 注意事項）
```

**依賴分層**（嚴格單向）：

```
config ← db ← repositories ← crawler
config ← http_client ← crawler
parsers ← crawler (純函式, 不依賴任何層)
cloak ← http_client / supervisor
supervisor → (spawn) run_crawl (獨立 OS 進程)
```

---

## 3. 各模組詳細說明

### 3.1 config.py（109 行）

- `DB_CONFIG`：MySQL 連線（127.0.0.1:3308，root/root，DB `partsouq_crawler`），可經環境變數覆寫
- `SITE`：各層頁面 URL 模板
- `CLOAK`：CloakBrowser 設定（CDP port 9242、cookie 匯出檔、user-agent）
- `CRAWL`：爬取參數（延遲、逾時、重試、worker 數、各層 limit）
- `LOG_DIR`：日誌目錄
- `load_cookies()` / `save_cookies()`：cookie 持久化（`save_cookies` 設 `0o600` 權限）

**注意**：`DB_CONFIG` 在 import 時就固定（module-level），測試用「執行時改 `DB_CONFIG["database"]`」切換測試庫，不能靠環境變數（因 import 順序不保證）。

### 3.2 db.py（254 行）

- `_thread_conn()`：執行緒本地連線（4 個 worker 各自一條連線，不共享）
- `_execute(sql, params)`：執行單一 SQL；死結(1213)/鎖等待逾時(1205) **rollback 後直接重拋**；斷線(2006/2013/InterfaceError) **捨棄舊連線後拋 `ConnectionLost`**（SOL review P1：舊連線未提交的交易已隨斷線回滾，若重連後只重跑單一 SQL，會讓 parts 在 A 連線回滾、receipt 卻在新連線 B 提交 —— 一律由服務層 `crawl_group` 重跑完整冪等區塊）
- `_executemany(sql, rows)`：批次執行（一次往返）；斷線/死結處理與 `_execute` 一致
- `rollback()`：回滾當前執行緒交易（deadlock 後清除殘留狀態）
- `commit()`：提交當前執行緒交易；失敗時重置連線避免「資料沒寫入但狀態標記完成」的靜默缺漏；**斷線(2006/2013/InterfaceError)同樣拋 `ConnectionLost`**（SOL review P2：commit 階段的斷線也必須被服務層的完整區塊重試涵蓋）
- `connect()`：**不建立主連線**（SOL review P3：所有查詢經 `_thread_conn()` 惰性建立，舊碼的 self.conn 是從未被使用的閒置連線）
- `close()`：關閉所有執行緒連線
- `query_one(sql, params)`：**回傳 DictCursor 的 dict**（重要：呼叫端必須用 `row["key"]`，不能用 `row[0]`）

### 3.3 repositories.py（655 行）

**5 層 + 2 個輔助 repo**：

- `BrandRepository`：`upsert_brand`（唯一鍵 name）、`upsert_model`（唯一鍵 brand_id+name，**ssd 用 COALESCE 保留既有值**）、`list_models`
- `VehicleRepository`：`upsert_vehicle`（v5 唯一鍵 `model_id + identity_hash`；hash 含穩定規格，不含 ssd/vid/url token）、`upsert_category`（cid 存在時以 vehicle_id+cid 為身分）、`upsert_group`（唯一鍵 category_id+code，**code 空值寫 `""` 而非 NULL**）、各 list 方法
- `PartRepository`：`upsert_parts(group_id, parts, run_id)`（**先清該 group 舊 membership → SELECT 既有 key → executemany upsert 並寫 `seen_run_id` → 回傳新增數**）、`count_parts_in_group`
- `CrawlRepository`：crawl_state、group receipt、fresh reset、crawl_runs；`publish_success_parts(run_id)` 只發布 `parts.seen_run_id = run_id` 的列

**upsert 模式**（MySQL 8.0.19+ 的 `INSERT ... AS new` 行別名語法）：

```sql
INSERT INTO models (brand_id, name, ssd, url) VALUES (%s, %s, %s, %s) AS new
ON DUPLICATE KEY UPDATE ssd = COALESCE(new.ssd, models.ssd),   -- ⚠ 必須限定表名
                         url = new.url, fetched_at = NOW(), id = LAST_INSERT_ID(id)
```

### 3.4 parsers.py（473 行）

純函式解析器，每個函式簽名統一為 `(html: str, ..., soup=None)`：
- `_soup(html)` / `_abs(href)` / `_qs(url, key)` / `_is_partsouq_endpoint(url, path)` 輔助；導覽連結只接受站內相對 URL 或 `partsouq.com` 的精確 endpoint，不跟隨同 path 的外站 URL
- `parse_brands(html)`：側邊欄 `<li> <a href="/locate?c=..">`，依名稱去重
- `parse_brand_index(html, brand)`：手風琴型號，取 ssd
- `parse_vehicles(html, brand)`：**以 `<th>` 的 class/標題對應欄位**（含 Gearbox/Body Style）；只採計帶 `/vehicle?` 連結的列；以穩定規格去重，不用 ssd token；無 prod_period 時用 year_from/year_to 兜
- `parse_category_links(html, brand)`：vehicle 頁的分類導覽，取 cid/cname
- `parse_groups(html, brand, default_cid, soup)`：**只用 `GROUP_LINK_RE` 匹配「NNNN: NAME」文字格式**；cid 缺省用 default_cid
- `parse_parts(html)`：**只用「第一個儲存格連到 `/search/all?q=`」的列**；欄位順序 Number|Name|Code|Note|Quantity|Range；**回傳 `(parts, malformed)`** —— 料號為空或欄數 < 6 的列計入 malformed 不進 parts，由 crawl_group 拒絕寫 terminal receipt（SOL P1，防缺欄資料以 NULL 落庫後不再重抓）
- `looks_like_challenge(html)`：粗略判斷 CF 驗證頁

**重要契約**：所有 `parse_*` 的第一個參數必須是 `html`。呼叫端若漏傳（曾發生），會直接 `TypeError` 讓整層失敗。

### 3.5 http_client.py（389 行）

`SessionManager`：
- `__init__(cookies)`：建立 requests.Session，套用 cookie（**整份替換 jar**：先清空再套用新快照，SOL review P2 —— 舊 PHPSESSID 不會殘留）
- `get(url)`：**重試迴圈（max_retries=5）+ 驗證自動刷新**
  - 403/429 或 `_is_challenge` → 觸發 `force_refresh_session(被拒的 cf_clearance 版本)`（single-flight；**全域版本已更新時直接沿用，不重啟瀏覽器**，SOL review P2）
  - 刷新失敗累計超過 `challenge_retries`(3) 就放棄該請求
  - `RequestException`（含伺服器關閉的 keep-alive socket）→ `_reset_connections()` 重建連線池
- `ensure_fresh()`：每次請求前確認 cookie 新鮮度（成本極低的 single-flight 檢查）
- `_is_challenge(r, text)`：**檢查 `cf-mitigated: challenge` 標頭 + `cf-chl` 標頭 + 正文特徵**
- `sleep()`：每次請求後的隨機延遲（2~5 秒）
- `_reset_connections()`：關閉 session、重建 Retry adapter

### 3.6 cloak.py（481 行）

CloakBrowser（隱匿 Chromium）整合：
- `_session_state`（dict + `threading.Condition` 保護）：single-flight 狀態
  - `cookies`/`ok_ts`（上次成功時間）/`busy`/`retry_after`/`failures`/`version`（cf_clearance 版本訊號，SOL review P2）
- `COOKIE_TTL = 25 * 60`（25 分鐘主動刷新）
- `refresh_session()`：**single-flight**（併發呼叫者只啟動一個瀏覽器）
  - TTL 內沿用快取；否則取鎖、在鎖外等待退避、執行 `_refresh_impl()`
- `force_refresh_session(rejected_version=None)`：challenge 處理用的強制刷新；**帶被拒版本呼叫時，若全域 session 已是更新版本則直接沿用（SOL review P2：延遲 challenge 不再清掉新 cookie 重刷）**
- `_refresh_impl()`：清殘留瀏覽器 → 啟動 CloakBrowser → 等 cookie 匯出（180s timeout）→ 寫檔 + `save_cookies()`
- `_mark_refresh_failed()`：**指數退避**（60s → 120s → 240s → 上限 20 分鐘）
- `_kill_browsers()`：`pkill -f "remote-debugging-port=9242"` + `pkill -f "cloakbrowser.launch_async"`
- `get_session()`：TTL 感知的取得 cookie（供 http_client 與 supervisor 呼叫）

### 3.7 crawler.py（1043 行）

`Crawler`（服務層）：
- `run()`：品牌清單 → 逐品牌 `crawl_brand` → 品牌間休息 120 秒
- `crawl_brand(brand)`：upsert brand → `parse_brand_index` 得 models → 逐 model
- `crawl_model(brand, brand_id, model)`：upsert model → `parse_vehicles` 得 vehicles → **ThreadPoolExecutor(4) 並行 `crawl_vehicle`**
- `crawl_vehicle(brand, model_id, vehicle)`：upsert vehicle → **分類縮水對帳（SOL review P1：DB 已知分類沒被本次解析到即拋錯；首爬新車以「頁面有 `/vehicle?` 導覽連結卻解析出 0 個帶 cid 分類」的結構契約偵測，車不標 done）** → `parse_category_links` + `parse_groups`（**共用同一個 soup，避免重複解析**）→ `crawl_groups`
- `crawl_groups`/`crawl_group`：canonical unit candidate 對帳 → upsert category/group → fetch unit 頁 → `parse_parts` → malformed / row_count 縮水檢查 → `upsert_parts(..., run_id)`；零件 membership + receipt 同交易，deadlock/斷線重跑完整區塊
- `_bump(key, n)`：記憶體計數（`threading.Lock` 保護，結束時一次寫入 crawl_runs）
- `_get(url)`：`ensure_fresh()` + `sleep()` + `http.get()`
- 續爬：`is_done("model"/"vehicle", key)` 檢查，done 就跳過；error 會在重新訪問該 model 時重試

**交易邊界**：每 group、每 vehicle、每 model 各自 commit（不是每零件一次）。

### 3.8 run_crawl.py（114 行）

CLI 進入點：先取得 `crawler.lock` 單實例鎖，再組裝 DB/HTTP/crawler。`--fresh` reset 在 `Crawler.run()` 與 `start_run` 同一交易，cookie 初始化失敗不會先清掉進度。其餘參數為 `--brand`/`--no-browser`/`--workers`。

### 3.9 supervisor.py（651 行）

**監督迴圈**（關鍵自癒機制，每 60 秒 tick 一次執行健康檢查）：
1. 程序存活：子程序崩潰 → 重啟
2. `_kill_other_crawlers()`：用 `ps` 精確比對命令列，並用自己持有的 child PID 排除 owned crawler；明確 stray 清不掉時 fail closed
3. `_progress_stalled()`：HANG_TIMEOUT(20min) 內無新零件 → 重啟（**滑動計數 + 啟動寬限期**，見 §4）
4. `_memory_over_limit()`：RSS > 2GB → 重啟
5. `_disk_low()`：< 5GB → 記錄並提前退場
6. `_db_alive()`：SELECT 1
7. `_cookie_fresh()`：**只讀檢查 cookie 檔案新鮮度，不觸發瀏覽器刷新**（見 §4 的坑）
8. `_crawl_done()`：最新 crawl_runs 是 success → 乾淨退出
 9. `_cleanup_stale_runs()`：啟動時把 started_at 早於本月一號的 running 標 error（F1a：當月 run 的 started_at 恆在月初，用 24h 判斷會誤殺正常進行中的當月 run）
- `_write_summary()`：寫 logs/summary.json（重啟次數/原因/計數）
- 重啟風暴保護：`RESTART_WINDOW = HANG_TIMEOUT × RESTART_MAX + 2×CHECK_INTERVAL`（62 分鐘）窗口內 >3 次重啟 → **先終止故障 child**、冷卻 30 分鐘（SOL review P1：窗口必須**嚴格大於**卡死週期 × 門檻、且先納入本次事件再判斷 —— 窗口剛好等於週期 × 門檻時，固定週期卡死的第 4 次重啟會剛好把第 1 次排除，永遠累積不到冷卻；冷卻前不終止卡死 child 會讓它繼續存在整段冷卻期）

### 3.10 watchdog（scripts/watchdog.py，244 行）

**最後一道防線**（launchd 每小時觸發）：
- supervisor 不在 → 以目前 Python spawn；MySQL client 與連線讀 `PSQ_MYSQL_BIN`、`PSQ_DB_*`
- crawler 不在（緩衝 8 秒避免換代空窗）→ 記錄
- DB 健康 + parts 最後寫入時間 + 數量
- 寫 logs/watchdog.log + watchdog_status.json
- 回傳碼 0=正常 / 1=有問題

---

## 4. 已知問題與修復歷程（重要！reviewer 必讀）

這包 code 在實際運維中踩過以下坑，已修復並加上迴歸測試。**請特別 review 這些點的修復是否完整、有沒有殘留邊角案例**。

### 4.1 `row[0]` 對 DictCursor 的 KeyError（已修）

- **症狀**：supervisor.log 每 60 秒出現 `run-status query failed: 0` / `progress query failed: 0`，卡死偵測完全失效（爬蟲死了 supervisor 不知道）
- **根因**：`db.query_one()` 回傳 DictCursor 的 **dict**，但 `_progress_stalled`/`_crawl_done` 用 `row[0]`（tuple 索引）→ `KeyError(0)`，`str()` 是 `"0"`
- **修復**：改用 `row.get("last_write")` / `row.get("status")`；PROGRESS_QUERY 加 `AS last_write` 別名
- **測試陷阱**：單元測試 mock 回傳 tuple `("running",)` 所以沒抓到 —— 這是測試與真實契約不一致的案例

### 4.2 supervisor 誤殺 crawler 的 Chrome（已修）

- **症狀**：爬蟲永遠無法進入正常爬取，兩邊互相殘殺重啟瀏覽器
- **根因**：supervisor 與 crawler 是**兩個獨立 OS 進程**，各有各的 `_session_state`。supervisor 的狀態永遠是空的 → 每次 `_cookie_fresh()` 呼叫 `get_session()` 都觸發完整刷新 → `_refresh_impl()` 偵測到 crawler 正在用的 CloakBrowser，當成 stale browser 殺掉重啟。crawler 的 `ensure_fresh()` 也偵測到瀏覽器被殺 → 也刷新 → 無限循環
- **修復**：supervisor 的 `_cookie_fresh()` 改成**唯讀檢查 cookie 檔案 mtime**，完全不碰瀏覽器。刷新只由 crawler 子程序自己負責

### 4.3 parse_* 漏傳 html 參數（已修）

- **症狀**：`parse_category_links() missing 1 required positional argument: 'html'`、`parse_groups() ... missing 'html'`，整層解析失敗，2,143 台車零件遺失
- **根因**：parser 函式簽名改成 `(html, ...)` 後，crawler.py 呼叫端漏傳（`parse_category_links(brand=..., soup=soup)` 缺 html；`parse_groups(brand=..., soup=...)` 缺 html）
- **修復**：補上 html 參數
- **防回歸**：AST 靜態檢查測試（`test_all_parsers_take_html_first` + `test_crawler_always_passes_html_to_parsers`），用 mutation test 驗證能抓到

### 4.4 `_progress_stalled` 無啟動寬限期（已修）

- **症狀**：剛啟動的 crawler（還在刷 cookie，尚無零件寫入）被當成「20 分鐘無進度」誤殺，重啟風暴
- **根因**：心跳判斷拿 parts 表「絕對最後寫入時間」跟現在比，但爬蟲剛啟動時最後寫入是上一輪的舊時間
- **修復**：改成滑動計數（看到新寫入就更新 `last_progress`）+ 給剛啟動的子程序一整段 HANG_TIMEOUT 寬限期

### 4.5 `_kill_other_crawlers` 誤殺（已修）

- **症狀**：supervisor 每 ~3 分鐘「killing stray crawler pid=XXXX」，殺掉自己啟動的 crawler
- **根因**：排除邏輯用 `self.proc.pid`，但子程序短暫變成 zombie（poll() 回傳非 None）時 `mine` 變空集，就把自己的 crawler 當 stray
- **修復**：只以 supervisor 持有的 `self.proc.pid` 排除 owned child，且未確認 child 結束前保留 reference；process scan 採 `clean / unresolved stray / inconclusive` 三態，明確 stray 清不掉時 fail closed
- **另一坑**：`pgrep -f "src.run_crawl"` 會誤匹配**任何命令列含該字串的進程**（包括監控 shell 命令），改用 `ps` 取得 argv，再以錨定 regex 精確辨識 Python module/script 入口

### 4.6 schema 的 NULL 唯一鍵（已修）

- **根因**：`groups_t.code`、`parts.range_str`、`vehicles.model_code/name` 原本 NULLable → MySQL 唯一鍵對 NULL 不視為相同 → upsert 會插入重複列
- **修復**：schema 改 `NOT NULL DEFAULT ''`；repositories 端 `code or ""`、`range_str or ""`

### 4.7 `COALESCE(new.ssd, ssd)` 欄位歧義（已修）

- **根因**：MySQL 8 的 `INSERT ... AS new` 把 `new` 當成第二個表別名 → 未限定的 `ssd` 有歧義（error 1052）
- **修復**：限定 `models.ssd` / `vehicles.ssd`

### 4.8 parts(updated_at) 無索引（已修）

- **影響**：supervisor 每 60 秒跑 `SELECT MAX(updated_at) FROM parts`，無索引時全表掃描，百萬列後變慢
- **修復**：schema 加 `KEY idx_part_updated (updated_at)`；線上已用 `ALTER TABLE ... ALGORITHM=INPLACE, LOCK=NONE` 加（**注意：ALTER 會與 crawler 長交易搶 MDL，需在 crawler 停止時做**）

### 4.9 闔蓋睡眠暫停（已知行為，非 bug）

- macOS Clamshell Sleep 會凍結所有進程。爬蟲不會死，但會暫停。喚醒後 cookie 過期，爬蟲會自動刷新恢復。
- watchdog 每小時兜底；若要睡眠期間繼續跑需 `caffeinate` 或插電防睡（電池模式下 macOS 不允許無限期防睡）。

---

## 5. 測試策略

目前 collect 共 230 個測試。日常安全 suite 為 198 個 non-DB tests；`test_repositories.py` 的 32 個 integration tests 會清空 `partsouq_crawler_test`，只能在確認測試庫後另跑：

| 檔案 | 數量 | 覆蓋 |
|------|-----|------|
| `test_parsers.py` | 42 | 6 個 parser、vehicle 穩定規格、canonical candidate、外站 URL 防護、malformed/duplicate parts |
| `test_supervisor.py` | 25 | 崩潰/卡死/風暴冷卻、cooldown child 回收、stray ownership/race、SIGTERM/finally、單實例鎖 |
| `test_repositories.py` | 32 | **獨立測試 DB**：upsert、run/receipt、vehicle token rotation、category cid、part membership、全 run state reset |
| `test_stability.py` | 14 | HTTP 工作階段：CLOSE_WAIT socket、逾時、challenge 觸發一次刷新、退避、single-flight 只啟動一個瀏覽器、TTL 快取、**cookie 整份替換（SOL review P2）**、**force_refresh 沿用較新全域 session / 傳遞被拒版本（SOL review P2）** |
| `test_crawler_contract.py` | 9 | CRAWL 設定契約（workers/逾時/重試/limit 合理性、**row_count_shrink_ratio**、**重啟窗口嚴格 > 卡死週期 × 門檻**）、SITE URL 齊備、**crawler.py 交易邊界 AST 檢查**（upsert_parts 所在函式必須有 commit） |
| `test_regressions.py` | 108 | run 完整性、bounded futures、parser/receipt、交易重試、fresh 全 scope 原子 reset、snapshot membership、crawler lock、CLI exit/config、watchdog portable DB config |

**測試 DB 連線方式**：`test_repositories.py` 在執行時改 `DB_CONFIG["database"] = "partsouq_crawler_test"`，測試庫由 `schema.sql` 建立。注意與「module import 時固定 DB_CONFIG」的相容性。

---

## 6. 維運與監控

- **supervisor**（`python3 -m src.supervisor`）：監督 crawler 子程序，每 60 秒健康檢查，自癒重啟
- **watchdog**（launchd 每小時）：確保 supervisor 存活，死了就 spawn；寫 logs/watchdog.log + watchdog_status.json
- **launchd**：
  - `com.partsouq.crawler`：主 supervisor（StartCalendarInterval 每月 1 日 00:05，RunAtLoad=false）
  - `com.partsouq.crawler.watchdog`：每小時 watchdog（StartInterval=3600, RunAtLoad=true）
- **日誌**：
  - `logs/crawl.log`（RotatingFileHandler 20MB×5）
  - `logs/supervisor.log`（RotatingFileHandler 5MB×3）
  - `logs/watchdog.log` + `watchdog_status.json`
  - `logs/summary.json`（每趟結束統計）
- **DB**：`partsouq_crawler`（正式）、`partsouq_crawler_test`（測試）、`crawler_runtime`（PartSouqAdmin 系統，與本專案無關）

---

## 7. 給 reviewer 的檢查重點（room of doubt）

請特別 review 以下風險點：

1. **跨進程狀態隔離**：supervisor 與 crawler 是兩個 OS 進程，各自有 in-memory 狀態。任何「假設共享狀態」的設計都會壞掉（見 §4.2）。檢查是否還有其他模組犯了同樣錯誤。
2. **crawler ownership**：`crawler.lock`、supervisor child PID 與 stray kill 的交界是否仍有競態？
3. **`ps` 命令列辨識**：supervisor/watchdog 的 regex 是否同時避免誤殺 shell 字面值、並正確辨識 macOS 絕對路徑 Python？
4. **`query_one` 回傳 dict 的契約**：所有呼叫端是否都用 key 而非 index？新 code 是否遵守？
5. **`_progress_stalled` 滑動計數**：寬限期邏輯是否會在特殊情況（如爬蟲啟動後 20 分鐘內真的卡住）漏判？
6. **交易與 commit 邊界**：crawler.py 每 group/vehicle/model commit 一次。是否有漏 commit 的路徑（異常時）？`db.py` 的斷線處理（拋 ConnectionLost，由服務層重跑完整區塊）是否涵蓋所有 transaction 路徑？
7. **upsert_parts 的先查後寫**：SELECT 既有 key 集合 → executemany upsert → 回傳新增數。並發時（同 group 多 worker 同時爬？實際上不會，但驗證）計數是否正確？
8. **CONCURRENCY**：4 個 worker 各持一條連線。`db._executemany` 是否線程安全？`self.counts` 的 Lock 是否涵蓋所有累加路徑？
9. **CloakBrowser 資源管理**：`_refresh_impl` 的所有退出路徑是否都正確清理瀏覽器進程？單一 CDP port 9242 的衝突風險？
10. **COOKIE 退避**：`failures` 在成功時重置為 0？退避上限 20 分鐘是否合理？`force_refresh_session(rejected_version)` 的「沿用較新全域 session」判斷是否有漏網（如快取已過期但版本不同）？
11. **錯誤處理廣度**：`except Exception` 是否有包太寬、吞掉真錯誤的地方？
12. **schema rollout**：必須停 crawler，依序執行 migration 004/005。舊 vehicle 沒有儲存完整規格，005 首次升級會要求先備份、顯式設 `@PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD=1`，然後清除 normalized vehicle 樹重爬；`published_parts`/`v_parts` 會保留上一版 current snapshot。若偵測到 category identity 碰撞會中止，不得直接合併。
13. **scheduler.py**：此檔案存在但看似是舊設計（可能是之前 supervisor 的前身），是否應該刪除？有沒有被誤用？
14. **launchd 主 job**：StartCalendarInterval 每月一次 + RunAtLoad=false。若機器在非每月 1 日重啟，誰拉起 supervisor？watchdog 是否足夠？
15. **預設密碼**：`DB_CONFIG` 與 watchdog fallback 預設仍是 `root/root`；正式執行必須由 launchd `EnvironmentVariables` 覆寫 `PSQ_DB_*`。
16. **合法下架政策**：目前歷史已知節點缺席時一律 fail closed，避免縮水頁被當成下架。若要自動移除真正下架資料，必須另定連續缺席/tombstone/人工核准規則，不能由單次爬取猜測。

---

## 8. 驗證邊界

本文描述 repository 現行設計，不代表 live DB、migration 或程序已部署。
本輪只執行不碰 DB/外網的測試；正式與測試 DB schema、runtime PID、長跑進度需另行唯讀確認。
