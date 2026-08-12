"""Repository 層：每個聚合（aggregate）各自的資料存取物件（Laravel 風格）。

每個 repository 包住共用的 :class:`~src.db.Database` 連線管理員，
並擁有自己聚合的所有 SQL。服務層（如 crawler）依賴 repository，
絕不直接接觸原始 SQL —— 這就是資料存取與業務邏輯的分界。

聚合對應（一對一表格群）：
    brands   → brands（品牌）、models（型號）
    vehicles → vehicles（車型）、categories（分類）、groups_t（零件組）
    parts    → parts（零件）
    crawl    → crawl_state（爬取進度）、crawl_runs（爬取紀錄）
"""

import hashlib
import logging

from .db import Database

log = logging.getLogger("repos")

# 零件的搜尋頁網址模板（PartSouq 的零件查詢入口）
PART_URL_TEMPLATE = "https://partsouq.com/en/search/all?q={part_number}"


def vehicle_identity_hash(model_id: int, vehicle: dict) -> str:
    """回傳與 vehicles 唯一鍵一致的穩定 SHA256 identity。

    ssd / vid / url 都是請求用 token 或參數，不屬於車型身分；它們輪替
    時必須更新同一列，而不是建立另一台車或另一個 resume key。
    """
    values = (
        str(model_id),
        str(vehicle.get("model_code") or ""),
        str(vehicle.get("name") or ""),
        str(vehicle.get("description") or ""),
        str(vehicle.get("options") or ""),
        str(vehicle.get("prod_period") or ""),
        str(vehicle.get("grade") or ""),
        str(vehicle.get("market") or ""),
        str(vehicle.get("engine") or ""),
        str(vehicle.get("transmission") or ""),
        str(vehicle.get("body_style") or ""),
    )
    raw = "".join(f"{len(value.encode('utf-8'))}:{value}" for value in values)
    return hashlib.sha256(raw.encode()).hexdigest()


class BrandRepository:
    """品牌與型號的資料存取（目錄最上層的兩階）。"""

    def __init__(self, db: Database):
        self.db = db

    def upsert_brand(self, name: str, url: str | None) -> int:
        """新增或更新品牌（以 name 為唯一鍵）。回傳品牌 id。"""
        cur = self.db._execute(
            "INSERT INTO brands (name, url) VALUES (%s, %s) AS new "
            "ON DUPLICATE KEY UPDATE url = new.url, id = LAST_INSERT_ID(id)",
            (name, url),
        )
        return cur.lastrowid or self._brand_id(name)

    def list_brands(self) -> list[str]:
        """列出資料庫中已知的所有品牌名稱（判定全站完成用）。"""
        cur = self.db._execute("SELECT name FROM brands ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def _brand_id(self, name: str) -> int:
        """依品牌名稱查詢 id（upsert 回傳值為 0 時的備援查詢）。"""
        cur = self.db._execute("SELECT id FROM brands WHERE name = %s", (name,))
        row = cur.fetchone()
        return row["id"] if row else 0

    def upsert_model(self, brand_id: int, name: str, ssd: str | None, url: str | None) -> int:
        """新增或更新型號（以品牌 + 名稱唯一）。回傳型號 id。

        ssd 採用 COALESCE：既有資料有 ssd 時不覆寫為 NULL。
        """
        cur = self.db._execute(
            "INSERT INTO models (brand_id, name, ssd, url) VALUES (%s, %s, %s, %s) AS new "
            "ON DUPLICATE KEY UPDATE ssd = COALESCE(new.ssd, models.ssd), "
            "url = new.url, fetched_at = NOW(), id = LAST_INSERT_ID(id)",
            (brand_id, name, ssd, url),
        )
        return cur.lastrowid

    def list_models(self, brand_id: int) -> list[dict]:
        """列出某品牌下的所有型號（依 id 排序）。"""
        cur = self.db._execute(
            "SELECT id, name, ssd, url FROM models WHERE brand_id = %s ORDER BY id",
            (brand_id,),
        )
        return cur.fetchall()

    def list_model_names(self, brand: str) -> list[str]:
        """列出某品牌下 DB 已知的所有型號名稱（閉合對帳用，F1b）。"""
        cur = self.db._execute(
            "SELECT m.name FROM models m "
            "JOIN brands b ON b.id = m.brand_id WHERE b.name = %s ORDER BY m.name",
            (brand,),
        )
        return [r["name"] for r in cur.fetchall()]


class VehicleRepository:
    """車型、分類、零件組的資料存取（車型 → 分類 → 零件組的樹狀結構）。"""

    def __init__(self, db: Database):
        self.db = db

    def upsert_vehicle(self, model_id: int, vehicle: dict) -> int:
        """新增或更新車型（以 model_id + identity_hash 唯一）。回傳 id。"""
        identity_hash = vehicle_identity_hash(model_id, vehicle)
        cur = self.db._execute(
            "INSERT INTO vehicles (model_id, identity_hash, name, description, model_code, options, "
            "prod_period, grade, market, engine, transmission, body_style, ssd, vid, url) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new "
            "ON DUPLICATE KEY UPDATE name = new.name, model_code = new.model_code, "
            "description = new.description, options = new.options, "
            "prod_period = new.prod_period, grade = new.grade, market = new.market, "
            "engine = new.engine, transmission = new.transmission, "
            "body_style = new.body_style, "
            "ssd = COALESCE(new.ssd, vehicles.ssd), "
            "vid = new.vid, url = new.url, "
            "fetched_at = NOW(), id = LAST_INSERT_ID(id)",
            (
                model_id,
                identity_hash,
                vehicle.get("name"),
                vehicle.get("description"),
                vehicle.get("model_code"),
                vehicle.get("options"),
                vehicle.get("prod_period"),
                vehicle.get("grade"),
                vehicle.get("market"),
                vehicle.get("engine"),
                vehicle.get("transmission"),
                vehicle.get("body_style"),
                vehicle.get("ssd"),
                vehicle.get("vid"),
                vehicle.get("url"),
            ),
        )
        return cur.lastrowid

    def list_vehicles(self, model_id: int) -> list[dict]:
        """列出某型號下的所有車型（依 id 排序）。"""
        cur = self.db._execute(
            "SELECT id, name, model_code, ssd, vid, url FROM vehicles "
            "WHERE model_id = %s ORDER BY id",
            (model_id,),
        )
        return cur.fetchall()

    def list_vehicle_keys(self, brand: str) -> list[str]:
        """列出某品牌下 DB 已知的所有車型 resume key（閉合對帳用，F1b）。

        identity_hash 由 upsert 與 migration 依同一公式產生；scope key 加
        v5 前綴，讓舊公式的 state 能被明確清退。
        """
        cur = self.db._execute(
            "SELECT v.identity_hash "
            "FROM vehicles v "
            "JOIN models m ON m.id = v.model_id "
            "JOIN brands b ON b.id = m.brand_id "
            "WHERE b.name = %s",
            (brand,),
        )
        return [f"v5:{r['identity_hash']}" for r in cur.fetchall()]

    def upsert_category(self, vehicle_id: int, name: str, cid: str | None) -> int:
        """新增或更新分類。cid 存在時以 vehicle_id + cid 為穩定身分。"""
        cid = cid or None
        identity_sql = "cid = %s" if cid else "name = %s"
        identity_value = cid if cid else name
        cur = self.db._execute(
            f"SELECT id FROM categories WHERE vehicle_id = %s AND {identity_sql} "
            "ORDER BY id LIMIT 1",
            (vehicle_id, identity_value),
        )
        row = cur.fetchone()
        if row:
            self.db._execute(
                "UPDATE categories SET name = %s, cid = %s, fetched_at = NOW() WHERE id = %s",
                (name, cid, row["id"]),
            )
            return row["id"]
        cur = self.db._execute(
            "INSERT INTO categories (vehicle_id, name, cid) VALUES (%s, %s, %s) AS new "
            "ON DUPLICATE KEY UPDATE name = new.name, cid = new.cid, fetched_at = NOW(), "
            "id = LAST_INSERT_ID(id)",
            (vehicle_id, name, cid),
        )
        return cur.lastrowid

    def list_categories(self, vehicle_id: int) -> list[dict]:
        """列出某車型下的所有分類（依 id 排序）。"""
        cur = self.db._execute(
            "SELECT id, name, cid FROM categories WHERE vehicle_id = %s ORDER BY id",
            (vehicle_id,),
        )
        return cur.fetchall()

    def upsert_group(
        self, category_id: int, code: str | None, name: str | None, uid: str | None, url: str | None
    ) -> int:
        """新增或更新零件組（以 category_id + code 唯一）。回傳零件組 id。

        code 為 None 時以空字串寫入：MySQL 唯一索引視 NULL 為「互不相等」，
        若放任 NULL 會在同一個分類下長出無限多筆重複零件組。
        """
        cur = self.db._execute(
            "INSERT INTO groups_t (category_id, code, name, uid, url) "
            "VALUES (%s, %s, %s, %s, %s) AS new "
            "ON DUPLICATE KEY UPDATE name = new.name, uid = new.uid, "
            "url = new.url, fetched_at = NOW(), id = LAST_INSERT_ID(id)",
            (category_id, code or "", name, uid, url),
        )
        return cur.lastrowid

    def list_group_codes_for_category(self, vehicle_id: int, cid: str) -> set[str]:
        """回傳某車輛某 cid 下 DB 已知的所有 group code（group manifest 對帳用）。"""
        cur = self.db._execute(
            "SELECT DISTINCT g.code FROM groups_t g "
            "JOIN categories c ON c.id = g.category_id "
            "WHERE c.vehicle_id = %s AND c.cid = %s",
            (vehicle_id, cid),
        )
        return {r["code"] for r in cur.fetchall()}


class PartRepository:
    """零件的資料存取（目錄的葉節點層）。"""

    def __init__(self, db: Database):
        self.db = db

    def upsert_parts(self, group_id: int, parts: list[dict], run_id: int | None = None) -> int:
        """批次新增/更新一個零件組下的所有零件（1 次 SELECT + 1 次批次 INSERT）。

        回傳新插入的筆數。比逐筆 INSERT 快非常多：
        一個約 30 筆零件的零件組，從 30 次往返變成 2 次。

        以 (part_number, range_str) 判定新增：parts 表的唯一鍵就是
        (group_id, part_number, range_str)，同料號不同 range 會真的
        插入兩列，統計必須與唯一鍵一致才不會低估。
        """
        if run_id is not None:
            self.clear_group_membership(group_id)
        if not parts:
            return 0
        # 先查出該零件組既有的料號+範圍 → 用來判斷哪些是新插入
        cur = self.db._execute(
            "SELECT part_number, range_str FROM parts WHERE group_id = %s", (group_id,)
        )
        existing = {(row["part_number"], row["range_str"]) for row in cur.fetchall()}

        rows = [
            (
                group_id,
                p.get("part_number") or "",
                p.get("name"),
                p.get("code"),
                p.get("note"),
                p.get("quantity"),
                p.get("range_str") or "",
                PART_URL_TEMPLATE.format(part_number=p.get("part_number") or ""),
                run_id,
            )
            for p in parts
        ]
        self.db._executemany(
            "INSERT INTO parts (group_id, part_number, name, code, note, quantity, range_str, url, "
            "seen_run_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) AS new "
            "ON DUPLICATE KEY UPDATE name = new.name, code = new.code, "
            "note = new.note, quantity = new.quantity, "
            "seen_run_id = new.seen_run_id, "
            "updated_at = CURRENT_TIMESTAMP",
            rows,
        )
        seen = set()
        new_count = 0
        for p in parts:
            key = (p.get("part_number") or "", p.get("range_str") or "")
            if key not in existing and key not in seen:
                seen.add(key)
                new_count += 1
        return new_count

    def clear_group_membership(self, group_id: int):
        """清除單一 group 的舊 run membership；與後續 upsert 同交易。"""
        self.db._execute("UPDATE parts SET seen_run_id = NULL WHERE group_id = %s", (group_id,))

    def count_parts_in_group(self, group_id: int) -> int:
        """統計某零件組下的零件數量（供驗證與監督使用）。"""
        cur = self.db._execute("SELECT COUNT(*) AS n FROM parts WHERE group_id = %s", (group_id,))
        return cur.fetchone()["n"]


class CrawlRepository:
    """爬取進度（crawl_state）與爬取紀錄（crawl_runs）的資料存取。"""

    def __init__(self, db: Database, run_key: str = ""):
        self.db = db
        # run_key 標記「這趟 run」的範圍（例如 '2026-08'）。空字串 = 相容模式
        # （舊 run 的 done 狀態跨 run 共享）。設定了 run_key 後，done 狀態
        # 按 run 隔離：每個月的新 run 看不到舊 run 的 done，會重新爬取。
        # 用空字串而非 None：MySQL 唯一鍵對 NULL 不視為相同，會破壞
        # ON DUPLICATE 的覆寫語意。
        self.run_key = run_key or ""

    def mark_done(self, scope: str, key: str):
        """把某範圍的某個鍵標記為「完成」。

        scope 例：'model'（型號）、'vehicle'（車型）；
        key 例：'Toyota::COROLLA' 或 'Toyota::COROLLA::ZRE210'。
        """
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES (%s, %s, %s, 'done') "
            "ON DUPLICATE KEY UPDATE status = 'done', error_msg = NULL, "
            "updated_at = NOW()",
            (self.run_key, scope, key),
        )

    def mark_error(self, scope: str, key: str, msg: str):
        """把某範圍的某個鍵標記為「失敗」，並記錄錯誤訊息（截斷 500 字）。"""
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status, error_msg) "
            "VALUES (%s, %s, %s, 'error', %s) "
            "ON DUPLICATE KEY UPDATE status = 'error', error_msg = %s, "
            "updated_at = NOW()",
            (self.run_key, scope, key, msg[:500], msg[:500]),
        )

    def is_done(self, scope: str, key: str) -> bool:
        """判斷某範圍的某鍵是否已完成（續爬時用來跳過）。"""
        cur = self.db._execute(
            "SELECT 1 AS x FROM crawl_state WHERE run_key = %s "
            "AND scope = %s AND scope_key = %s AND status = 'done'",
            (self.run_key, scope, key),
        )
        return cur.fetchone() is not None

    def count_errors(self, run_key: str = "") -> int:
        """統計某個 run 內仍處於「未完成」狀態的項目數（error + pending）。

        run 結束時以這個數字做為「是否真的全站成功」的單一事實來源：
        model／vehicle（及品牌層）有任何失敗或尚未完成的項目，都會被
        計入。pending 也必須算：backoff 跳過的 model/車型、以及任何
        未走到收尾狀態的項目，都代表這趟 run 沒有完整閉合（P1 修復）。
        續爬時完成項目會被 mark_done 覆寫，因此真正完成的全站 run
        這個數字必須是 0。
        """
        cur = self.db._execute(
            "SELECT COUNT(*) AS n FROM crawl_state "
            "WHERE run_key = %s AND status IN ('error', 'pending')",
            (run_key,),
        )
        return cur.fetchone()["n"]

    def is_group_fetched(self, vehicle_id: int, group_code: str, run_key: str = "") -> bool:
        """判斷某車的某零件組是否已在「本 run」抓取完成（有零件或 404）。

        續爬/重試優化用：重試一台失敗車時，只補抓「尚未完成的組」，
        已成功抓過的組直接跳過，避免重抓全部 ~200 個 group 燒光
        rate budget（Agent 分析建議）。

        F1b 修復：完成與否以明確的 group terminal state
        （groups_t.fetched_run_key）為準 —— 舊版用「任一零件
        updated_at >= run 起點」啟發式，頁面只解析出部分非空資料時
        重試會把缺漏固定下來。
        """
        if not run_key:
            return False
        cur = self.db._execute(
            "SELECT 1 FROM groups_t g "
            "JOIN categories c ON c.id = g.category_id "
            "WHERE c.vehicle_id = %s AND g.code = %s "
            "AND g.fetched_run_key = %s LIMIT 1",
            (vehicle_id, group_code, run_key),
        )
        return cur.fetchone() is not None

    def fetched_group_map(self, vehicle_id: int, run_key: str = "") -> dict:
        """一次載入某車「本 run 已抓完」的所有零件組（F5 優化）。

        回傳 {(cid, code): row_count, ...}（存在即代表已抓過）。
        以 (cid, code) 為鍵：DB 的 group 唯一身分是
        (category_id, code)，只用 code 當鍵會讓不同分類的同 code 組
        互相覆蓋、誤 skip。receipt 的 status 詳情留在 DB
        （fetched_status），這裡只做 skip 判斷。
        續爬一臺失敗車時，用這張 map 在記憶體判斷每組是否已完成，
        不必每組各查一次 DB —— 一臺車約 200 組，原本是 200 次往返。
        """
        if not run_key:
            return {}
        cur = self.db._execute(
            "SELECT c.cid, g.code, g.fetched_row_count "
            "FROM groups_t g "
            "JOIN categories c ON c.id = g.category_id "
            "WHERE c.vehicle_id = %s AND g.fetched_run_key = %s",
            (vehicle_id, run_key),
        )
        return {
            (str(r["cid"] or ""), r["code"]): r["fetched_row_count"] or 0 for r in cur.fetchall()
        }

    def previous_row_count_map(self, vehicle_id: int, run_key: str = "") -> dict:
        """一次載入某車「上一 run 之前」每個組的歷史 row_count（SOL review P1）。

        回傳 {(cid, code): 該組歷史上最大的 fetched_row_count}。
        縮水偵測的參考點：crawl_group 解析出「格式完整但數量遠少於
        前次」的零件時，據此拒絕寫 terminal receipt。排除本 run 的
        receipt —— 本 run 已抓過的組會走 skip，不會來到縮水檢查，
        但保險起見仍不把它當參考點。
        """
        if not run_key:
            return {}
        cur = self.db._execute(
            "SELECT c.cid, g.code, MAX(g.fetched_row_count) AS row_count "
            "FROM groups_t g "
            "JOIN categories c ON c.id = g.category_id "
            "WHERE c.vehicle_id = %s AND g.fetched_row_count > 0 "
            "AND (g.fetched_run_key IS NULL OR g.fetched_run_key <> %s) "
            "GROUP BY g.id",
            (vehicle_id, run_key),
        )
        return {(str(r["cid"] or ""), r["code"]): r["row_count"] or 0 for r in cur.fetchall()}

    def previous_row_count(self, group_id: int) -> int:
        """回傳某零件組歷史上最大的 fetched_row_count（無則 0）。

        縮水偵測的後備路徑（未提供 prev_rows map 時逐組查詢，僅測試/
        相容使用；正式爬取一律用 previous_row_count_map 一次載入）。
        """
        cur = self.db._execute(
            "SELECT MAX(fetched_row_count) AS n FROM groups_t WHERE id = %s", (group_id,)
        )
        return cur.fetchone()["n"] or 0

    def mark_group_fetched(
        self, group_id: int, run_key: str = "", status: str = "done", row_count: int = 0
    ):
        """標記某零件組已在本次 run 抓取完成（durable receipt，F1b/F5）。

        status 區分完成種類：'done'（有零件）、'not_found'（404，網站
        端「此組無資料」的合法訊號）。HTTP 200 但解析 0 零件一律視為
        異常（反爬/版型變更）並拋錯，**不寫** receipt（SOL P2：沒有
        可驗證的「合法空組」DOM 訊號前不猜測，避免把封鎖頁當成空組
        標 done）。row_count 記錄本組零件筆數 —— 配合 fetched_run_key
        讓續爬「不再重抓 404 或已完成組」，也為 content hash 增量
        更新打基礎。

        與零件的 upsert 同一交易提交（見 crawl_group）：避免「零件寫了
        但狀態沒寫」的靜默缺漏。
        """
        self.db._execute(
            "UPDATE groups_t SET fetched_run_key = %s, fetched_status = %s, "
            "fetched_row_count = %s WHERE id = %s",
            (run_key, status, row_count, group_id),
        )

    def seen(self, scope: str, key: str):
        """記錄「本 run 遇見」某項目（不改變既有狀態）。

        F1b 修復：閉合對帳需要知道「本 run 從清單層見到過哪些項目」。
        縮水解析（locate/pick 頁只回傳子集）時，未被見到的項目不會有
        crawl_state 行，count_errors 數不到 —— seen 保證每個被解析器
        見到的項目都有一行，run 結束時與 DB 已知集合比對即可抓到縮水。
        """
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES (%s, %s, %s, 'pending') "
            "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
            (self.run_key, scope, key),
        )

    def scope_keys(self, run_key: str, scope: str, prefix: str | None = None) -> set[str]:
        """回傳某 run 中某 scope、以 prefix 開頭的所有 scope_key（閉合對帳用）。

        prefix 為 None 時回傳全部 scope_key（vehicle scope 因 hash key
        格式不再使用 prefix match）。"""
        if prefix is not None:
            cur = self.db._execute(
                "SELECT scope_key FROM crawl_state "
                "WHERE run_key = %s AND scope = %s AND LOCATE(%s, scope_key) = 1",
                (run_key, scope, prefix),
            )
        else:
            cur = self.db._execute(
                "SELECT scope_key FROM crawl_state WHERE run_key = %s AND scope = %s",
                (run_key, scope),
            )
        return {r["scope_key"] for r in cur.fetchall()}

    def reset_scope(self, scope: str, run_key: str = ""):
        """清除某範圍的所有進度紀錄（--fresh 模式用）。

        run_key 為空字串時清除「相容（run_key=''）」與全部；指定時只清該 run。
        """
        if run_key:
            self.db._execute(
                "DELETE FROM crawl_state WHERE scope = %s AND run_key = %s",
                (scope, run_key),
            )
        else:
            self.db._execute("DELETE FROM crawl_state WHERE scope = %s", (scope,))

    def reset_run_state(self, run_key: str):
        """清除指定 run 的所有 scope，包含舊版或未來新增的 scope。"""
        self.db._execute("DELETE FROM crawl_state WHERE run_key = %s", (run_key,))

    def reset_group_receipts(self, run_key: str | None = None):
        """清除所有零件組的抓取收據（--fresh 模式用）。

        同月既有 fetched_run_key 會讓 group 在 HTTP 與 upsert 前直接
        跳過，只重設 crawl_state 不足以讓 --fresh 從頭開始爬所有 group。
        """
        if run_key:
            self.db._execute(
                "UPDATE groups_t SET fetched_run_key = NULL, fetched_status = NULL "
                "WHERE fetched_run_key = %s",
                (run_key,),
            )
        else:
            self.db._execute("UPDATE groups_t SET fetched_run_key = NULL, fetched_status = NULL")

    def reset_part_markers(self, run_id: int):
        """清除同月 fresh run 的舊 membership，不影響已發布 snapshot。"""
        self.db._execute("UPDATE parts SET seen_run_id = NULL WHERE seen_run_id = %s", (run_id,))

    def purge_legacy_vehicle_state(self, run_key: str) -> int:
        """一次性相容（P1 修復）：清除舊版 vehicle resume key 格式。

        v5 key 有明確版本前綴。所有不是 ``v5:<64 hex>`` 的 pending /
        error 都不可能再被新版程式覆寫，會永久卡住 count_errors；清掉
        讓它們依新版 identity 重新爬取。
        """
        cur = self.db._execute(
            "DELETE FROM crawl_state WHERE run_key = %s AND scope = 'vehicle' "
            "AND scope_key NOT REGEXP '^v5:[a-f0-9]{64}$' "
            "AND status IN ('pending', 'error')",
            (run_key,),
        )
        return cur.rowcount

    def start_run(self, run_key: str = "", fresh: bool = False) -> int:
        """新增一筆「執行中」的爬取紀錄，回傳 run id。

        run_key 標記這趟 run 的範圍（例如 '2026-08'）；同月再次呼叫
        時因唯一鍵衝突，改用 ON DUPLICATE 更新回 running，並回傳
        既有 id（保證同月只有一筆 run 紀錄）。

        F1a 修復：started_at 是「logical monthly run 起點」，同月
        重啟**不更新** —— 舊碼每次重啟都覆寫成 NOW()，而 resume 會
        跳過已完成的 vehicle（不會更新其零件時間），最後 success 時
        v_parts 會把「先前 attempt 已完成且仍現存」的零件誤排除
        （實測 3 車 15,300 筆零件全部早於被推後的 cutoff）。
        起點只在同 run_key 首次 INSERT 時設定；跨月新 run_key 自動
        得到新的起點。

        若該月已是 success（全站已完整爬完），不覆寫成 running（P2
        修復）—— 之後的 partial run 不該抹掉「全站已完成」的證據。

        本方法不 commit：交易邊界由服務層決定（見 db.py 分層契約）。
        """
        if fresh:
            cur = self.db._execute(
                "INSERT INTO crawl_runs (run_key, started_at, status) "
                "VALUES (%s, NOW(), 'running') "
                "ON DUPLICATE KEY UPDATE started_at = NOW(), finished_at = NULL, "
                "status = 'running', brands_ok = 0, models_ok = 0, vehicles_ok = 0, "
                "groups_ok = 0, parts_ok = 0, parts_new = 0, error_msg = NULL, "
                "id = LAST_INSERT_ID(id)",
                (run_key,),
            )
        else:
            cur = self.db._execute(
                "INSERT INTO crawl_runs (run_key, started_at, status) "
                "VALUES (%s, NOW(), 'running') "
                "ON DUPLICATE KEY UPDATE "
                "status = IF(status = 'success', 'success', 'running'), "
                "finished_at = IF(status = 'success', finished_at, NULL), "
                "id = LAST_INSERT_ID(id)",
                (run_key,),
            )
        return cur.lastrowid

    def run_status(self, run_id: int) -> str | None:
        """讀取指定 run 的目前狀態（commit 結果不明時用來對帳）。"""
        cur = self.db._execute("SELECT status FROM crawl_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        return row["status"] if row else None

    def finish_run(self, run_id: int, status: str, counts: dict, error: str | None = None):
        """收尾一筆爬取紀錄：寫入完成時間、狀態、各層計數與錯誤訊息。

        若該 run 已是 success（先前全站完成），本次收尾不降級（P2
        修復）—— 不抹掉全站完成的證據。舊碼先 SELECT status 再 UPDATE：
        併行收尾時兩方同時讀到 running → error 可在 success 後覆寫，
        留下「success + 錯誤訊息 + 錯誤計數」的矛盾紀錄。改為單一
        條件 UPDATE（WHERE status != 'success'），由 DB 保證原子性
        （P2 修復：TOCTOU 競態）。

        本方法不 commit：交易邊界由服務層決定（見 db.py 分層契約）。
        """
        self.db._execute(
            "UPDATE crawl_runs SET finished_at = NOW(), "
            "status = %s, "
            "brands_ok = %s, models_ok = %s, vehicles_ok = %s, "
            "groups_ok = %s, parts_ok = %s, parts_new = %s, error_msg = %s "
            "WHERE id = %s AND status != 'success'",
            (
                status,
                counts.get("brands", 0),
                counts.get("models", 0),
                counts.get("vehicles", 0),
                counts.get("groups", 0),
                counts.get("parts", 0),
                counts.get("parts_new", 0),
                error,
                run_id,
            ),
        )

    def publish_success_parts(self, run_id: int):
        """在同一交易內重建不可變的 current snapshot。

        normalized tables 可被後續 failed/partial attempt 原地 upsert；因此
        current view 不直接 join 它們。先清空再依本次 logical run 明確
        標記的資料重建 ``published_parts``。與 finish_run(success) 同次 commit；
        任一步失敗 rollback 後，舊 snapshot 仍完整可讀。
        """
        self.db._execute("DELETE FROM published_parts")
        cur = self.db._execute(
            "INSERT INTO published_parts ("
            "part_id, brand, model, vehicle_name, vehicle_code, prod_period, "
            "part_name, part_number, category_main, category_group, group_code, "
            "part_range, note, quantity, code, snapshot_at) "
            "SELECT p.id, b.name, m.name, v.name, v.model_code, v.prod_period, "
            "p.name, p.part_number, c.name, g.name, g.code, p.range_str, "
            "p.note, p.quantity, p.code, NOW() "
            "FROM parts p "
            "JOIN groups_t g ON g.id = p.group_id "
            "JOIN categories c ON c.id = g.category_id "
            "JOIN vehicles v ON v.id = c.vehicle_id "
            "JOIN models m ON m.id = v.model_id "
            "JOIN brands b ON b.id = m.brand_id "
            "WHERE p.seen_run_id = %s",
            (run_id,),
        )
        if isinstance(cur.rowcount, int) and cur.rowcount <= 0:
            raise RuntimeError(f"run {run_id} produced an empty published snapshot")
        return cur.rowcount
