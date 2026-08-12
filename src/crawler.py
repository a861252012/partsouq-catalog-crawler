"""全站爬蟲服務層：負責編排整趟爬取流程（Laravel 服務層的角色）。

每品牌的分層處理管線：
  1. locate 頁面 → 取得型號清單（models）
  2. pick 頁面   → 取得車型清單（vehicles）
  3. vehicle 頁面 → 取得分類（categories）+ 各分類下的零件組（groups）
  4. unit 頁面    → 取得零件明細（parts）

本類別只做「編排」：組合 HTTP 傳輸（基礎設施層）、HTML 解析器（轉換層）
與 Repository（資料存取層）來完成「爬完整個網站」這個使用案例。
這裡不含任何 SQL，也不含任何 HTTP 細節 —— 這是服務層與其他層之間
的依賴分界。

規模說明：單一車型約有 200 個零件組，全站是數百萬個 unit 請求。
執行緒池以「每台車一個 worker」的方式並行；每個 worker 內部自行
序列化自己的零件組請求（共用同一個 ssd 參數）。Cookie 為全域共用，
遇到 Cloudflare 驗證時由鎖保護，只允許一個 worker 刷新。

續爬（Resume）：crawl_state 表記錄每型號/每車型的完成狀態，
重新執行時會跳過已完成的車型。
"""

import gc
import logging
import threading
import time
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import pymysql

from .config import CRAWL, SITE
from .db import ConnectionLost
from .governor import RequestGovernor
from .http_client import NotFoundError, SessionManager
from .parsers import (
    _soup,
    parse_brand_index,
    parse_category_links,
    parse_groups,
    parse_parts,
    parse_vehicles,
)
from .repositories import (
    BrandRepository,
    CrawlRepository,
    PartRepository,
    VehicleRepository,
    vehicle_identity_hash,
)

log = logging.getLogger("crawler")

# 品牌之間切換時的休息秒數：模擬人類換任務的行為，降低被偵測的機率。
BRAND_PAUSE_SECONDS = 120


class Crawler:
    """爬蟲服務：一趟完整的全站爬取（可續爬、多工並行）。

    依賴注入的組件：
    - http  ：HTTP 傳輸 + Cookie 管理（基礎設施層）
    - db    ：MySQL 連線管理（基礎設施層，僅用於 commit 交易）
    - 其餘  ：各 Repository（資料存取層）
    """

    def __init__(
        self, http: SessionManager, db, workers: int = 8, governor=None, fresh: bool = False
    ):
        self.http = http
        self.db = db
        # Repository 層：SQL 全部封裝在這些物件裡
        self.brands = BrandRepository(db)
        self.vehicles = VehicleRepository(db)
        self.parts = PartRepository(db)
        self.crawl = CrawlRepository(db)
        self.workers = workers
        self.fresh = fresh
        self.run_id: int | None = None
        self.lock = threading.Lock()
        # F5：全 crawler 共用的 request governor —— worker 數只控制
        # in-flight 數，總請求率由此 token bucket 決定（見 governor.py）。
        # governor 可由組合根（run_crawl）注入，與主 session 共用同一個
        # 實例，讓 _brands() 等直發請求也受全域限流。
        self.governor = governor or RequestGovernor(CRAWL["request_rate"], CRAWL["request_burst"])
        # run() 的最終狀態（'success'/'error'），供 CLI 決定 exit code
        self.last_status = "error"
        # 本 run 實際處理過的品牌（閉合檢查用，F1b）：_brands() 縮水時
        # 未被回傳的品牌不在這集合裡，光查歷史 done 會被上個月的
        # 完成狀態誤導成 success。
        self._visited_brands = set()
        self.counts = {
            "brands": 0,
            "models": 0,
            "vehicles": 0,
            "groups": 0,
            "parts": 0,
            "parts_new": 0,
        }
        # 每執行緒獨立的 SessionManager，共用同一個 cookie 存放區
        self._local = threading.local()
        # 常駐的 worker 池：執行緒（及其 thread-local 的 DB 連線 / HTTP
        # session）跨型號重複使用，而不是每個型號都重建 —— 連線數維持
        # 平穩，也避免每個型號的建立/銷毀開銷。
        self._pool = ThreadPoolExecutor(
            max_workers=max(1, workers),
            thread_name_prefix="psq",
        )

    def close(self):
        """關閉 worker 池並釋放 thread-local 資源（結束前一定要呼叫）。"""
        self._pool.shutdown(wait=True)
        try:
            self.http.session.close()
        except Exception:
            pass

    # ------------------------------------------------------------ helpers

    def _session(self) -> SessionManager:
        """取得目前執行緒專屬的 session，共用主 cookie jar。

        因為每個 worker 執行緒都需要自己的 requests.Session（requests
        本身不是 thread-safe），所以用 thread-local 暫存，重複使用。
        no_browser 設定須與主 session 一致（否則 --no-browser 模式下
        worker 仍會啟動瀏覽器刷新）。
        """
        s = getattr(self._local, "session", None)
        if s is None:
            s = SessionManager(
                self.http.cookies, no_browser=self.http.no_browser, gov=self.governor
            )
            self._local.session = s
        return s

    def _get(self, url: str) -> str:
        """發 GET 請求：先確保 cookie 新鮮、再取回 HTML。

        全域限速在 SessionManager.get() 內、每次 wire request 前由
        governor.acquire() 執行（SOL P1：重試也受控）；governor 的
        全域暫停（throttle：429 / 反爬偵測）同樣在那裡生效。
        """
        session = self._session()
        session.ensure_fresh()
        session.sleep()
        return session.get(url)

    def _bump(self, key: str, n: int = 1):
        """統計計數累加（鎖保護，避免並行 worker 的 race condition）。"""
        with self.lock:
            self.counts[key] += n

    def _guard_parse(self, html: str, items, what: str, ctx: str):
        """0 解析結果一律視為異常（不再依賴 5,000 bytes 門檻）。

        修復前的問題：只有 HTML 超過 5,000 bytes 才檢查空結果，短版
        異常頁（例如空白的 HTTP 200 `<html></html>`）會被當成「合法
        空資料」靜默成功。現在 0 結果就視為異常 —— 空白回應直接拒絕，
        有內容但解析 0 項代表版型變更（或 Cloudflare 反爬頁），讓續爬
        機制重試。

        404 不經由此處：http_client 對 404 拋 NotFoundError，由
        crawl_group（unit 路徑）捕獲視為「此 group 無資料」。

        疑似反爬頁時（大頁面 0 結果）：
        1. 暫停全域 governor（throttle）：所有 worker 在 acquire 時
           一起阻塞到 breather 結束，避免 thundering herd 重錘同一批
           頁面（Agent 分析建議；SOL：統一由 governor 控管，不再
           另設 crawler 層的全域 block）。
        2. 自己先停頓 CRAWL["block_breather"] 秒再拋出。
        """
        if items:
            return
        if not html or not html.strip():
            raise RuntimeError(f"[{ctx}] empty HTTP response for {what}")
        breather = CRAWL.get("block_breather", 0)
        if breather:
            log.warning(
                "[%s] parsed 0 %s from %d-byte page; suspected block, breathing %ds",
                ctx,
                what,
                len(html),
                breather,
            )
            # 全域暫停：throttle 在 acquire 前阻塞所有 worker，
            # slow 則讓封鎖解除後速率砍半一段時間再恢復。
            self.governor.throttle(breather)
            self.governor.slow()
            time.sleep(breather)
        raise RuntimeError(
            f"[{ctx}] parsed 0 {what} from {len(html)}-byte page (site layout changed?)"
        )

    def _brands(self):
        """從首頁取得 18 個品牌的清單。"""
        html = self.http.get(SITE["genuine"])
        from .parsers import parse_brands

        return parse_brands(html)

    # -------------------------------------------------------------- brands

    def crawl_brand(self, brand: str) -> int:
        """爬取單一品牌：列出型號 → 逐型號爬取 → 品牌間休息。

        回傳失敗的型號數（0 = 全部成功）。失敗的型號會以 mark_error
        寫入 crawl_state，由 run() 在結束時統一用 count_errors 判定
        這趟是否真的全站成功。
        """
        locate_url = SITE["locate"].format(brand=urllib.parse.quote(brand))
        html = self._get(locate_url)
        models = parse_brand_index(html, brand)
        self._guard_parse(html, models, "models", brand)
        log.info("[%s] %d models", brand, len(models))

        brand_id = self.brands.upsert_brand(brand, locate_url)
        self.db.commit()
        self._bump("brands")

        limit = CRAWL["limit_models"]
        if limit:
            models = models[:limit]

        failures = 0
        consecutive = 0
        worked = False
        for model in models:
            key = f"{brand}::{model['name']}"
            # F1b：見即記錄 —— 解析器見到的每個 model 都要有 crawl_state
            # 行，閉合對帳才能抓到「locate 頁只回傳子集」的縮水解析。
            self.crawl.seen("model", key)
            if self.crawl.is_done("model", key):
                continue
            # 前一個 model 大量失敗（被封鎖）時，下一個 model 的車型
            # 極可能同樣失敗：跳過它，讓 crawler 轉往更遠的品牌，稍後
            # 再由心跳重啟後的續爬機制回來補（實際發生：死循環數小時）。
            if consecutive >= 3:
                log.warning(
                    "[%s] %d consecutive model failures; skipping %s for now (backoff)",
                    brand,
                    consecutive,
                    model["name"],
                )
                failures += 1
                consecutive = 0
                continue
            try:
                failed, model_worked = self.crawl_model(brand, brand_id, model)
                # F3 修復：worked 以「crawl_model 是否真的爬了車型/零件」
                # 為準 —— 純收尾的 model（所有車型已 done，只差 model
                # 狀態標記）不該觸發品牌間 120s 休息，否則續爬尾聲
                # 10 個收尾品牌合計 1,200s 無零件寫入，撞上 supervisor
                # 的 20 分鐘卡死門檻而誤重啟。
                worked = worked or model_worked
                if failed:
                    consecutive += 1
                    # 有車型失敗：model 不標 done，下次續爬會重試失敗車型
                    failures += 1
                    log.warning(
                        "[%s/%s] %d vehicle(s) failed; model not marked done",
                        brand,
                        model["name"],
                        failed,
                    )
                    self.crawl.mark_error("model", key, f"{failed} vehicles failed")
                    self.db.commit()
                else:
                    consecutive = 0
                    self.crawl.mark_done("model", key)
                    self.db.commit()
            except Exception as e:
                consecutive += 1
                failures += 1
                log.error("[%s/%s] model failed: %s", brand, model["name"], e)
                self.crawl.mark_error("model", key, str(e))
                self.db.commit()

        # 品牌間休息：模擬人類換任務的行為。P1 修復：品牌完全沒有
        # 可做的工作（所有 model 都已 done，或全部被 backoff 跳過）時
        # 不休息 —— 舊碼每品牌無條件睡 120 秒，續爬尾聲 10 個已完成
        # 品牌就是 20 分鐘沒有任何寫入，會觸發 supervisor 的 20 分鐘
        # 卡死檢查而誤重啟（而每次重啟又回到同一批已完成品牌）。
        if worked:
            log.info(
                "[%s] brand done, resting %ds before next brand",
                brand,
                BRAND_PAUSE_SECONDS,
            )
            time.sleep(BRAND_PAUSE_SECONDS)
        else:
            log.info("[%s] brand done, nothing to crawl (all models done)", brand)
        return failures

    def _vehicle_key(self, model_id: int, vehicle: dict) -> str:
        """產生與 vehicles.identity_hash 完全一致的版本化 resume key。"""
        return f"v5:{vehicle_identity_hash(model_id, vehicle)}"

    def _closure_errors(self, run_key: str, visited: list[str]) -> list[str]:
        """F1b 閉合對帳：model/vehicle 層的縮水解析偵測。

        品牌層的閉合檢查只比對「歷史已知品牌是否 done」，但下層的
        縮水解析（locate/pick 頁只回傳子集）不會留下任何 crawl_state
        行，count_errors 數不到 —— 本方法把「DB 中已知的 model /
        vehicle」與「本 run 見即記錄（seen）的行」比對，本 run 從未
        遇見的項目代表縮水解析（或網站端新增後未爬到），run 不得標
        success。

        未見的歷史 vehicle 也必須 fail closed。目前網站沒有提供可驗證
        的下架訊號；把「未見」直接猜成合法移除，會讓縮水頁發布成
        current snapshot。真正移除需另有連續缺席／tombstone 流程。
        """
        problems = []
        for brand in visited:
            if not self.crawl.is_done("brand", brand):
                continue  # brand 層未完成已由 count_errors 反映
            seen_models = self.crawl.scope_keys(run_key, "model", f"{brand}::")
            for m in self.brands.list_model_names(brand):
                if f"{brand}::{m}" not in seen_models:
                    problems.append(f"model {m} never seen this run")
            seen_vehicles = self.crawl.scope_keys(run_key, "vehicle")
            known_this_brand = set(self.vehicles.list_vehicle_keys(brand))
            missing_vehicles = [vk for vk in known_this_brand if vk not in seen_vehicles]
            if missing_vehicles:
                problems.append(
                    f"{len(missing_vehicles)} vehicle(s) never seen this run: "
                    f"{', '.join(missing_vehicles[:3])}"
                )
        return problems

    def crawl_model(self, brand: str, brand_id: int, model: dict) -> tuple[int, bool]:
        """爬取單一型號：取得車型清單 → 派發給 worker 池並行爬取。

        回傳 (failed, worked)：failed 是失敗的車型數（0 = 全部成功），
        worked 代表「本 model 是否真的爬取了車型/零件」（F3 修復，
        供 crawl_brand 決定是否品牌間休息）。失敗的車型會被標記為
        error，由外層決定是否把 model 標成 done —— 有任何車型失敗時
        不應標 done，否則續爬會永久跳過失敗車型。
        """
        model_id = self.brands.upsert_model(brand_id, model["name"], model["ssd"], model["url"])
        self.db.commit()
        self._bump("models")

        pick_url = SITE["pick"].format(
            brand=urllib.parse.quote(brand),
            model=urllib.parse.quote(model["name"]),
            ssd=urllib.parse.quote(model["ssd"] or ""),
        )
        html = self._get(pick_url)
        vehicles = parse_vehicles(html, brand)
        log.info("  [%s] %d vehicles", model["name"], len(vehicles))
        self._guard_parse(html, vehicles, "vehicles", model["name"])

        limit = CRAWL["limit_vehicles"]
        truncated = 0
        if limit:
            if len(vehicles) > limit:
                # 截斷的車型視為「未完成」：計入回傳的失敗數，
                # 讓外層不要把整個 model 標成 done（P0 修復）——
                # 否則同月全量續爬會因 model done 而跳過這些車型。
                truncated = len(vehicles) - limit
            vehicles = vehicles[:limit]

        pending = []
        for vehicle in vehicles:
            key = self._vehicle_key(model_id, vehicle)
            # F1b：見即記錄（見 crawl_brand 的說明）—— 車型層縮水
            # （pick 頁只回子集）也靠它與 DB 已知集合對帳。
            self.crawl.seen("vehicle", key)
            if self.crawl.is_done("vehicle", key):
                continue
            pending.append(vehicle)

        if not pending:
            # 所有車型都已完成，但若有截斷表示這次沒有全爬 —— 仍計入
            # 失敗（讓 model 不標 done，全量續爬補齊）。
            # F3 修復：這次沒有實際零件工作（純收尾），回報 worked=False。
            return truncated, False

        failed = 0

        def _settle(f, v):
            """backoff 後殘留 futures 的收尾（P2 修復）：已開始執行的
            future 無法取消，完成時由 callback 統一標 done/error，不讓
            它們「寫了資料卻沒有狀態」—— 否則續爬會把已完成的車重抓
            一遍（浪費 rate budget）。被取消的 future 保持無狀態，由
            續爬重試。"""
            key = self._vehicle_key(model_id, v)
            if f.cancelled():
                # 被取消的 future 保持無狀態（不標 done/error），由續爬
                # 機制重試；標 error 會讓這台車的失敗被重複計算。
                return
            try:
                f.result()
                self.crawl.mark_done("vehicle", key)
            except Exception as e:
                log.error("    [%s/%s] vehicle (late) failed: %s", model["name"], v.get("name"), e)
                self.crawl.mark_error("vehicle", key, str(e))
            try:
                self.db.commit()
            except Exception:
                pass

        # F5：bounded 派工 —— 一次只保留 workers*2 個未完成 Future。
        # SOL P2：wait() 一次可能回傳多個完成 Future，每輪要「依完成
        # 數量」補工，否則多個同時完成時 in-flight 會從 workers*2 掉
        # 到 1、退化成近乎單工；失敗門檻觸發後（gave_up）不再補工。
        max_inflight = max(2, self.workers * 2)
        futures = {}
        pending_iter = iter(pending)
        gave_up = False

        def _submit_next() -> bool:
            try:
                vehicle = next(pending_iter)
            except StopIteration:
                return False
            fut = self._pool.submit(self.crawl_vehicle, brand, model_id, vehicle)
            futures[fut] = vehicle
            return True

        for _ in range(max_inflight):
            if not _submit_next():
                break

        while futures and not gave_up:
            done_set, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done_set:
                vehicle = futures.pop(future)
                key = self._vehicle_key(model_id, vehicle)
                try:
                    future.result()
                    self.crawl.mark_done("vehicle", key)
                except Exception as e:
                    failed += 1
                    log.error(
                        "    [%s/%s] vehicle failed: %s", model["name"], vehicle.get("name"), e
                    )
                    self.crawl.mark_error("vehicle", key, str(e))
                    # 連續大量失敗（例如 ssd 過期 / 被封鎖）時，不要再死咬
                    # 這台 model 剩下的車型 —— 它們幾乎必然同樣失敗。退避後
                    # 把控制權交回外層，讓 crawler 轉往其他 model / 品牌
                    # （實際發生：單一 model 的 error 車讓 crawler 死循環
                    # 數小時，零前進）。
                    max_fail = max(3, len(pending) // 2)
                    if failed >= max_fail:
                        log.warning(
                            "    [%s] %d/%d vehicles failed in a row; giving up this "
                            "model for now (backoff)",
                            model["name"],
                            failed,
                            len(pending),
                        )
                        # 已開始執行、無法取消的 future 一律附 callback
                        # 收尾 —— 只取消不附 callback 會讓 running future
                        # 完成後無人標 done/error（P2 修復）。取消成功的
                        # 保持無狀態，由續爬重試。
                        for f, v in list(futures.items()):
                            f.cancel()
                            f.add_done_callback(lambda fut, vv=v: _settle(fut, vv))
                        futures.clear()
                        gave_up = True
                        break
                self.db.commit()
            if not gave_up:
                # SOL P2：依完成數量補工（多個同時完成就一次補多個），
                # in-flight 維持在 max_inflight。
                for _ in range(len(done_set)):
                    if not _submit_next():
                        break

        return failed + truncated, True

    # ------------------------------------------------------------ vehicles

    def crawl_vehicle(self, brand: str, model_id: int, vehicle: dict):
        """爬取單一車型：寫入車型、取得分類清單、逐分類爬取零件組。

        效能：vehicle 頁面的 HTML 只交給 lxml 解析一次，分類連結與
        零件組解析共用同一個 soup（每台車少 4 次完整解析）。
        """
        # P1 修復：必要欄位驗證必須在任何 DB 寫入之前完成。
        # SSD 為 NULL 時 uq_vehicle 允許多筆重複（MySQL UNIQUE 視 NULL
        # 為互不相等），重試就會不斷 insert 新行；且沒有 ssd token 根本
        # 無法取得零件資料 → 攔在最前面。
        ssd = vehicle.get("ssd")
        if not ssd:
            raise RuntimeError(
                f"[{brand} model_id={model_id}] missing ssd token for "
                f"{vehicle.get('name', '?')}/{vehicle.get('model_code', '?')}; cannot fetch"
            )

        vehicle_id = self.vehicles.upsert_vehicle(model_id, vehicle)
        self.db.commit()
        self._bump("vehicles")
        log.info("    [%s %s] groups...", vehicle.get("name"), vehicle.get("model_code"))

        # SOL review P1（分類縮水對帳）：記錄本車「DB 已知分類」
        # （前月遺留；此時尚未寫入任何本 run 的分類），爬完後與
        # 本次解析到的分類集合比對 —— vehicle 頁只回傳部分分類連結
        # 時，沒被爬到的分類其零件組永遠不會補抓，但車仍會被標 done。
        known_categories = {
            (c["cid"] or c["name"]): c["name"] for c in self.vehicles.list_categories(vehicle_id)
        }

        base_vid = str(vehicle.get("vid") or "0")

        vehicle_url = SITE["vehicle"].format(
            brand=urllib.parse.quote(brand),
            ssd=urllib.parse.quote(ssd),
            vid=urllib.parse.quote(base_vid),
        )
        html = self._get(vehicle_url)
        soup = _soup(html)
        category_links, malformed_categories = parse_category_links(
            html,
            brand=brand,
            soup=soup,
            diagnostics=True,
        )
        if malformed_categories:
            raise RuntimeError(
                f"[{brand} vehicle={vehicle_id}] {malformed_categories} malformed "
                "vehicle category link(s); parser may miss categories"
            )
        # 分類清單 = 引擎/燃油/工具（預設第一分類）+ 頁面上的其他分類連結
        categories = [
            {
                "category_name": "ENGINE/FUEL/TOOL",
                "cid": "1",
                "ssd": ssd,
                "vid": base_vid,
                "url": vehicle_url,
            }
        ]
        for link in category_links:
            if link["cid"] not in {c["cid"] for c in categories}:
                categories.append(link)

        # 預設分類（ENGINE/FUEL/TOOL）的截斷數必須一起收（P0：舊碼
        # 忽略此回傳值，僅預設分類的車被 limit_groups 截斷時仍會標
        # done，缺的零件組永久跳過）。
        # F5：一次載入本車的 group receipt map（重試車只補缺的組，
        # 不必每組查一次 DB）。
        fetched = self.crawl.fetched_group_map(vehicle_id, self.crawl.run_key or "")
        # SOL review P1：上一 run 的 row_count map —— 本次解析到的
        # 零件數相較前次大幅縮水（格式完整但內容縮水）時拒絕 receipt。
        prev_rows = self.crawl.previous_row_count_map(vehicle_id, self.crawl.run_key or "")
        truncated = self.crawl_groups(
            brand,
            vehicle_id,
            html,
            default_cid="1",
            soup=soup,
            skip=True,
            fetched=fetched,
            prev_rows=prev_rows,
        )

        for category in categories[1:]:
            category_url = category["url"]
            if not category_url.startswith("http"):
                category_url = SITE["base"] + category_url
            category_html = self._get(category_url)
            category_soup = _soup(category_html)
            truncated += self.crawl_groups(
                brand,
                vehicle_id,
                category_html,
                default_cid=category["cid"],
                soup=category_soup,
                skip=True,
                fetched=fetched,
                prev_rows=prev_rows,
            )

        # SOL review P1：分類縮水對帳 —— DB 已知但本次完全沒解析到的
        # 分類（vehicle 頁縮水/反爬頁只回部分分類連結）代表該分類的
        # 零件組本 run 沒有機會被爬，車不得標 done。
        parsed_cids = {c["cid"] for c in categories}
        parsed_names = {c["category_name"] for c in categories}
        missing = [
            name
            for cid, name in known_categories.items()
            if cid not in parsed_cids and name not in parsed_names
        ]
        if missing:
            raise RuntimeError(
                f"[{brand} vehicle={vehicle_id}] {len(missing)} known category(ies) never "
                f"parsed this run: {', '.join(missing[:4])}"
            )
        if truncated:
            # 有零件組被 limit_groups 截斷：這台車沒有爬完整，
            # 拋出例外讓該 vehicle 標 error、不標 done —— 否則同月
            # 全量續爬會把缺零件的車當成已完成而跳過（P0 修復）。
            raise RuntimeError(
                f"[{brand} vehicle={vehicle_id}] {truncated} group(s) truncated by limit_groups"
            )

    def crawl_groups(
        self,
        brand: str,
        vehicle_id: int,
        html: str,
        default_cid: str,
        soup=None,
        skip=False,
        fetched=None,
        prev_rows=None,
    ) -> int:
        """解析一頁 HTML 內的所有零件組並逐一爬取。回傳被 limit_groups
        截斷而未爬取的零件組數。

        可傳入已建好的 soup 避免重複解析（見 crawl_vehicle）。
        若頁面非空卻解析出 0 個零件組，代表網站版型可能已變更 ——
        視為失敗（讓該車型標 error，續爬重試），而不是靜默跳過，
        避免整台車的資料無聲消失。

        skip=True 時把 skip_if_fetched 傳給 crawl_group（重試車只補缺的組）。
        fetched：本車一次載入的 receipt map（F5），傳給 crawl_group。
        prev_rows：上一 run 的 row_count map（SOL review P1，縮水偵測）。
        """
        groups, malformed = parse_groups(
            html,
            brand,
            default_cid=default_cid,
            soup=soup,
            diagnostics=True,
        )
        if malformed:
            raise RuntimeError(
                f"[{brand} vehicle={vehicle_id} cid={default_cid}] "
                f"{malformed} malformed unit link(s); parser may miss groups"
            )
        self._guard_parse(html, groups, "groups", f"{brand} vehicle={vehicle_id}")
        # P1 修復：group 子集合閉合對帳 —— DB 已知的 group manifest
        # 若未全部出現在本次解析結果中（頁面縮水、版型變更），視為
        # 解析異常而非合法刪減，拋錯讓 vehicle 不標 done（避免缺漏
        # 被續爬固定）。
        known_codes = self.vehicles.list_group_codes_for_category(vehicle_id, default_cid)
        if known_codes:
            parsed_codes = {
                g.get("group_code") or ""
                for g in groups
                if str(g.get("cid") or default_cid) == str(default_cid)
            }
            missing = known_codes - parsed_codes
            if missing:
                raise RuntimeError(
                    f"[{brand} vehicle={vehicle_id} cid={default_cid}] {len(missing)} known "
                    f"group(s) missing from this parse: {', '.join(sorted(missing)[:5])}"
                )
        truncated = 0
        limit = CRAWL["limit_groups"]
        for i, group in enumerate(groups):
            if limit:
                with self.lock:
                    over = self.counts["groups"] >= limit
                if over:
                    # 全站零件組數已達上限：其餘 group 不爬，但回傳截斷數，
                    # 讓 crawl_vehicle 標記該車不完整（P0 修復），避免同月
                    # 全量續爬把缺零件的車當成 done。
                    truncated = len(groups) - i
                    break
            self.crawl_group(
                brand,
                vehicle_id,
                group,
                skip_if_fetched=skip,
                fetched=fetched,
                prev_rows=prev_rows,
            )
        return truncated

    def crawl_group(
        self,
        brand: str,
        vehicle_id: int,
        group: dict,
        skip_if_fetched: bool = False,
        fetched=None,
        prev_rows=None,
    ):
        """爬取單一零件組：寫入分類/零件組 → 爬取零件明細。

        交易刻意切成兩段：寫入分類/零件組後立即 commit（釋放列鎖），
        再做 2~5 秒的 HTTP 請求 —— 並行的 worker 才不會彼此等待，
        也不會持鎖跨越慢速網路。

        skip_if_fetched：續爬/重試場景的優化（Agent 分析建議）。同一
        run 內已成功抓過的 group 不再重抓（判斷在 upsert 之前，skip
        的組不產生任何寫入）—— 否則重試一台失敗車會把 ~200 個 group
        全部重抓，瞬間燒光 rate budget。每月的新 run 因為 run_key
        不同，不會被舊 run 的狀態擋住（P0 完整性由 crawl_state.run_key
        隔離保證）。

        fetched：本車「一次載入」的 receipt map（F5），避免每組查一次
        DB；爬完的組也會同步更新到 map（同車內不重複抓）。map 以
        (cid, code) 為鍵、值為 row_count；
        提供 map 時未命中即視為未抓過，不回頭查 DB（SOL P1）。
        prev_rows：本車上一 run 的 row_count map（SOL review P1）——
        本次解析到的零件數相較前次大幅縮水（格式完整但內容縮水）
        時視為反爬/版型異常並拋錯、不寫 receipt；未提供時退回逐組
        查 DB（僅測試/相容路徑）。

        終態語意：404（NotFoundError）= 網站端「此組無資料」的合法
        訊號，寫 receipt('not_found')；HTTP 200 但解析 0 零件、出現
        結構缺欄的 malformed 列、或相較前次 receipt 的 row_count 大幅
        縮水，一律視為反爬/版型異常並拋錯、不寫 receipt（SOL P2/P1：
        無可驗證的「合法空組」DOM 訊號前不猜測，寧可讓該組留在
        未完成、由續爬補抓）。
        """
        run_key = self.crawl.run_key or ""
        # SOL P1：skip 判斷在 category/group upsert 與 commit「之前」——
        # 已抓過的組不該再產生任何寫入；且已提供 receipt map 時
        # （fetched is not None）未命中直接視為未抓過，不再逐組查 DB
        # （一臺車約 200 組 = 200 次往返，SOL 實測 bulk 6 次 + 逐組
        # 334 次就是空 map 被誤當「沒載入」造成的）。
        # map 鍵是 (cid, code)：DB 的 group 唯一身分是
        # (category_id, code)，只用 code 會讓不同分類的同 code 組
        # 互相覆蓋、誤 skip（SOL P2）。
        map_key = (str(group.get("cid") or ""), group.get("group_code") or "")
        if skip_if_fetched:
            already = (
                map_key in fetched
                if fetched is not None
                else self.crawl.is_group_fetched(vehicle_id, group["group_code"], run_key)
            )
            if already:
                log.debug(
                    "[%s v=%s] group %s already fetched this run; skipping",
                    brand,
                    vehicle_id,
                    group["group_code"],
                )
                return

        category_name = group["category_name"]
        category_id = self.vehicles.upsert_category(vehicle_id, category_name, group["cid"])
        group_id = self.vehicles.upsert_group(
            category_id,
            group["group_code"],
            group["group_name"],
            group["uid"],
            group["url"],
        )
        self.db.commit()
        self._bump("groups")

        unit_url = group["url"]
        if not unit_url.startswith("http"):
            unit_url = SITE["base"] + unit_url
        try:
            html = self._get(unit_url)
        except NotFoundError:
            # 404 = 此 group 在網站端沒有資料（合法狀態）：視為完成，
            # 不讓整台車失敗（實際發生：部分車型的某些 group 頁 404）。
            # F1b：404 也是合法的 terminal state，照樣標記本 run 已抓；
            # F5：status='not_found'，續爬不再重抓 404 組。
            if self.run_id is None:
                raise RuntimeError("crawl run is not initialized") from None
            self.parts.clear_group_membership(group_id)
            self.crawl.mark_group_fetched(group_id, run_key, status="not_found")
            if fetched is not None:
                fetched[map_key] = 0
            self.db.commit()
            return
        parts, malformed = parse_parts(html)
        # SOL P1：結構缺欄/空料號的 candidate 列代表版型異常或反爬變體，
        # 不得寫 terminal receipt —— 否則殘缺列以 NULL 落庫後該組
        # 本月不再重抓，缺漏被固定。檢查在 guard 之前：全 malformed 的
        # 頁面應報「malformed」而非誤報「parsed 0 parts」並無謂觸發
        # 反爬 breather。
        if malformed:
            log.warning(
                "[%s group=%s] %d malformed part row(s); not receipted",
                brand,
                group.get("group_code"),
                malformed,
            )
            raise RuntimeError(
                f"[{brand} group={group.get('group_code')}] "
                f"{malformed} malformed part row(s) (layout changed?)"
            )
        # SOL review P1：相較前次 receipt 的 row_count 大幅縮水 = 頁面
        # 只回傳「少數但格式完整」的資料（反爬變體/內容縮水）——
        # malformed 抓不到這種頁面，guard 也會放行，必須拒絕 receipt，
        # 否則該組以殘缺資料標 done、缺漏被固定。首次爬取（無前次
        # receipt）沒有參考點，維持 best-effort。
        prev_count = 0
        if prev_rows is not None:
            prev_count = prev_rows.get(map_key, 0)
        elif fetched is None:
            prev_count = self.crawl.previous_row_count(group_id)
        shrink_ratio = CRAWL["row_count_shrink_ratio"]
        if prev_count >= 3 and len(parts) < prev_count * shrink_ratio:
            log.warning(
                "[%s group=%s] row count shrank %d -> %d (< %.0f%%); not receipted",
                brand,
                group.get("group_code"),
                prev_count,
                len(parts),
                shrink_ratio * 100,
            )
            raise RuntimeError(
                f"[{brand} group={group.get('group_code')}] row count shrank "
                f"{prev_count} -> {len(parts)} (< {shrink_ratio:.0%})"
            )
        # parts 也走統一空解析檢查：空白 200 或異常頁解析 0 零件必須
        # 視為失敗（P0 修復），不能當成「這組沒零件」靜默跳過。
        self._guard_parse(html, parts, "parts", f"{brand} group={group.get('group_code')}")
        # F1b：零件寫入與 group terminal state 同一交易提交 —— 避免
        # 「零件寫了但狀態沒寫」時，重試把整組重抓一遍（浪費 rate
        # budget），或狀態寫了但零件沒寫時缺漏被固定。
        # F5：receipt 記錄 row_count（content hash 增量更新的基礎）。
        # SOL P2/P1：整個（零件 + receipt）區塊冪等，deadlock / 斷線
        # 失效後由服務層重跑完整區塊一次（db.py 不再重跑單一 SQL）。
        for attempt in (1, 2):
            try:
                if self.run_id is None:
                    raise RuntimeError("crawl run is not initialized")
                new = self.parts.upsert_parts(group_id, parts, self.run_id)
                self.crawl.mark_group_fetched(
                    group_id, run_key, status="done", row_count=len(parts)
                )
                if fetched is not None:
                    fetched[map_key] = len(parts)
                self.db.commit()
                # 計數在 commit 成功後才累加：重跑時不重複計。
                self._bump("parts", len(parts))
                self._bump("parts_new", new)
                break
            except pymysql.err.OperationalError as e:
                code = e.args[0] if e.args else None
                if code in (1205, 1213) and attempt == 1:
                    log.warning(
                        "[%s group=%s] deadlock writing parts; retrying once",
                        brand,
                        group.get("group_code"),
                    )
                    self.db.rollback()
                    continue
                raise
            except ConnectionLost:
                if attempt == 1:
                    log.warning(
                        "[%s group=%s] connection lost writing parts; retrying full block once",
                        brand,
                        group.get("group_code"),
                    )
                    continue
                raise

    # ------------------------------------------------------------- entry

    def run(self) -> dict:
        """執行整趟爬取（進入點）。回傳統計計數；self.last_status 記錄
        最終狀態（'success'/'error'），供 CLI 決定 exit code（P2 修復：
        不完整 run 不該以 exit 0 結束）。"""
        self.last_status = "error"  # 預設：任何失敗路徑都保持 error
        # lxml/bs4 的解析樹會形成循環參考，循環 GC 必須保持開啟；
        # 調高門檻讓它在熱門迴圈中較少觸發，並在每個品牌後強制掃一次，
        # 以控制記憶體成長。
        gc.set_threshold(100_000, 400, 200)
        # run_key = 當月（例如 '2026-08'）：crawl_state 的 done 按 run 隔離，
        # 每月重新爬取時不會被上個月的 done 擋住（P0 修復）。
        from datetime import datetime

        run_key = datetime.now().strftime("%Y-%m")
        self.crawl.run_key = run_key
        partial = any(
            bool(CRAWL.get(k))
            for k in (
                "start_brand",
                "limit_brands",
                "limit_models",
                "limit_vehicles",
                "limit_groups",
            )
        )
        run_id = self.crawl.start_run(run_key, fresh=self.fresh)
        self.run_id = run_id
        # --fresh 的 run 邊界、state、receipt 與 membership reset 必須在
        # 同一交易。若程序在初始化後崩潰，DB 會保留 running，下一次
        # 普通啟動可繼續，而不會因舊 success 被靜默跳過。
        if self.fresh:
            self.crawl.reset_run_state(run_key)
            self.crawl.reset_group_receipts(run_key)
            self.crawl.reset_part_markers(run_id)
        # 交易邊界由服務層決定（db.py 分層契約）。
        self.db.commit()
        # 同月完整 run 已成功時，普通重啟不應再次 publish 舊 logical
        # window；--fresh 已在 start_run 重設邊界，局部 run 則仍允許執行
        # 修補 normalized 資料（但不會發布 current snapshot）。
        if not self.fresh and not partial and self.crawl.run_status(run_id) == "success":
            self.last_status = "success"
            log.info("run %s already completed; skipping duplicate full crawl", run_key)
            return self.counts
        # P1 修復：舊版 resume key（只有 model_code）的行永不關閉且被
        # count_errors 永久計入 —— 開啟本次 run 前先清掉，讓這些車以
        # v5 新格式重新爬取（一次性相容）。
        # best-effort：失敗不阻斷爬取（舊格式殘留頂多讓該月標 error）。
        try:
            purged = self.crawl.purge_legacy_vehicle_state(run_key)
            if purged:
                log.info("purged %d legacy-format vehicle state rows", purged)
            self.db.commit()
        except Exception:
            log.exception("legacy state purge failed; continuing")
        finalizing = False
        try:
            brands = self._brands()
            if not brands:
                # 首頁解析出 0 個品牌：網站可能改版或回應異常，
                # 直接當成失敗（不該寫 success 讓監督迴圈誤判完成）。
                raise RuntimeError("parsed 0 brands from genuine index (site layout changed?)")
            # SOL P1：品牌數低於可信門檻 = 縮水解析/反爬頁（首次爬取
            # 時閉合檢查無參考點，這道門檻就是首跑的防線；後續月份的
            # 閉合檢查用 DB 已知集合）。完整 scope 才檢查，--brand /
            # limit_brands 的局部執行不受影響。
            if (
                not any(bool(CRAWL.get(k)) for k in ("start_brand", "limit_brands"))
                and len(brands) < CRAWL["min_brands"]
            ):
                raise RuntimeError(
                    f"brand index shrank: {len(brands)} < min_brands={CRAWL['min_brands']}"
                )
            log.info("brand list: %s", [b["name"] for b in brands])
            start = CRAWL["start_brand"]
            limit = CRAWL["limit_brands"]
            if start:
                brands = [b for b in brands if b["name"] == start]
            if limit:
                brands = brands[:limit]

            for b in brands:
                log.info("=== brand %s ===", b["name"])
                # F1b：記錄本 run 實際處理過的品牌（閉合對帳用）
                self._visited_brands.add(b["name"])
                try:
                    failures = self.crawl_brand(b["name"])
                    if failures:
                        # 品牌下仍有型號失敗：記 error，讓同月續爬重試
                        self.crawl.mark_error("brand", b["name"], f"{failures} model(s) failed")
                    else:
                        # 品牌完整爬完：清除先前暫態錯誤（P0 修復），
                        # 否則一次暫時失敗會讓 count_errors 永遠包含它，
                        # 該月永遠無法標 success。
                        self.crawl.mark_done("brand", b["name"])
                except Exception as e:
                    # 品牌層失敗（例如 locate 頁解析失敗）：記錄但不放棄
                    # 其他品牌，結束時由 count_errors 一併判定 status。
                    log.error("[%s] brand crawl failed: %s", b["name"], e)
                    self.crawl.mark_error("brand", b["name"], str(e))
                self.db.commit()
                # 每個品牌後回收一次：清掉解析樹的循環參考
                gc.collect()

            # 全站成功 = 完整 scope（沒有 start/limit 縮限）+ 當次 run
            # 沒有任何殘留 error（P0 修復）。局部執行、有 limit、或任何
            # model/vehicle/brand 失敗，都不該標 success，否則監督迴圈
            # 會誤判「當月已爬完」而退出、讓失敗項目永遠缺漏。
            errors = self.crawl.count_errors(run_key)
            if partial:
                scope = CRAWL["start_brand"] or "limited"
                log.warning(
                    "partial run (scope=%s); marking run as error (not full success)", scope
                )
                self.crawl.finish_run(
                    run_id, "error", self.counts, f"partial run (start/limit set: {scope})"
                )
                self.db.commit()
            elif errors:
                log.warning("%d error(s) remain in run %s; marking run as error", errors, run_key)
                self.crawl.finish_run(
                    run_id, "error", self.counts, f"{errors} crawl_state error(s) remain"
                )
                self.db.commit()
            else:
                # P1 修復（全站閉合檢查）：count_errors == 0 還不夠 ——
                # 若 _brands() 只解析出部分品牌（網站暫態故障等），未處理
                # 的品牌在 crawl_state 中連行都沒有，不會被計入 error，
                # 卻會被誤標全站 success。全部已知品牌都必須標 done。
                known = self.brands.list_brands()
                # F1b 修復：品牌層縮水對帳 —— 「本 run 實際爬過的品牌」
                # 必須等於「DB 已知品牌」。舊檢查只查「歷史品牌是否
                # done」，縮水時未被回傳的品牌有上個月的 done 行，會被
                # 誤導成 success。
                visited = sorted(self._visited_brands)
                missing_brands = [b for b in known if b not in visited]
                undone = [b for b in visited if not self.crawl.is_done("brand", b)]
                # F1b：model/vehicle 層的縮水對帳（見 _closure_errors）
                closure = self._closure_errors(run_key, visited)
                problems = (
                    [f"brand never visited: {b}" for b in missing_brands]
                    + [f"brand not completed: {b}" for b in undone]
                    + closure
                )
                if problems:
                    log.warning(
                        "%d closure problem(s): %s; marking run as error",
                        len(problems),
                        "; ".join(problems[:5]),
                    )
                    self.crawl.finish_run(
                        run_id,
                        "error",
                        self.counts,
                        f"closure: {'; '.join(problems[:5])}",
                    )
                    self.db.commit()
                else:
                    # current snapshot 與 success row 必須是同一交易：先重建
                    # snapshot，再寫 success，commit 成功後才更新記憶體狀態。
                    # publish/finish 任一步失敗都會由 except rollback，不會
                    # 留下「run success 但 view 空掉」的矛盾狀態。
                    finalizing = True
                    self.crawl.publish_success_parts(run_id)
                    self.crawl.finish_run(run_id, "success", self.counts)
                    self.db.commit()
                    finalizing = False
                    self.last_status = "success"
            log.info("DONE: %s", self.counts)
            return self.counts
        except Exception as e:
            log.exception("crawl failed")
            self.last_status = "error"
            self.db.rollback()
            # commit 斷線時結果可能已落庫。重新連線後若 DB 已同時保存
            # snapshot + success，就承認成功；否則另開交易標 error。
            if finalizing:
                try:
                    if self.crawl.run_status(run_id) == "success":
                        self.last_status = "success"
                        log.warning("success commit acknowledged after finalize exception")
                        return self.counts
                except Exception:
                    log.exception("could not reconcile run after connection loss")
            try:
                self.crawl.finish_run(run_id, "error", self.counts, str(e))
                self.db.commit()
            except Exception:
                log.exception("failed to persist crawl error status")
            raise
