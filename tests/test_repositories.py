"""Repository 層測試：明確 opt-in 後才對獨立測試資料庫執行。

測試資料庫 partsouq_crawler_test 由 schema.sql 建立（含 FK），每次測試
前清空資料，因此可以安全地驗證 upsert 語意、唯一鍵行為與計數統計。

注意：DB_CONFIG 在 src.config 首次 import 時就已固定，這裡在執行時
（setUp）改寫 DB_CONFIG["database"]，讓新建的 Database 連到測試庫，
完全不影響其他測試模組。
"""

import os
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

import pymysql

from src.config import DB_CONFIG
from src.db import Database
from src.repositories import (
    BrandRepository,
    CrawlRepository,
    PartRepository,
    VehicleRepository,
    vehicle_identity_hash,
)

_TEST_DB = "partsouq_crawler_test"
_DB_TEST_ENV = "PSQ_RUN_REPOSITORY_TESTS"
_RUN_DB_TESTS = os.environ.get(_DB_TEST_ENV) == "1"


def _require_db_tests_opt_in():
    if os.environ.get(_DB_TEST_ENV) != "1":
        raise RuntimeError(f"set {_DB_TEST_ENV}=1 to run destructive repository tests")


def _test_db() -> Database:
    """建立連到測試資料庫的連線。"""
    _require_db_tests_opt_in()
    DB_CONFIG["database"] = _TEST_DB
    return Database().connect()


def _wipe(db: Database):
    """清空所有資料表（由 FK 決定刪除順序）。"""
    _require_db_tests_opt_in()
    row = db.query_one("SELECT DATABASE() AS database_name")
    database_name = row.get("database_name") if row else None
    if database_name != _TEST_DB:
        raise RuntimeError(f"refusing destructive repository tests on database {database_name!r}")
    for sql in (
        "SET FOREIGN_KEY_CHECKS = 0",
        "TRUNCATE published_parts",
        "TRUNCATE parts",
        "TRUNCATE groups_t",
        "TRUNCATE categories",
        "TRUNCATE vehicles",
        "TRUNCATE models",
        "TRUNCATE brands",
        "TRUNCATE crawl_state",
        "TRUNCATE crawl_runs",
        "SET FOREIGN_KEY_CHECKS = 1",
    ):
        db._execute(sql)
    db.commit()


def _brand_chain(brands, vehicles, parts, db):
    """建一條完整的資料鏈：品牌 → 型號 → 車型 → 分類 → 組 → 零件。

    回傳各層 id（供後續斷言使用）。
    """
    _wipe(db)
    brand_id = brands.upsert_brand("TESTBRAND", "http://x/locate?c=TESTBRAND")
    db.commit()
    model_id = brands.upsert_model(brand_id, "MODEL-X", "ssd-1", "http://x/pick")
    db.commit()
    vehicle_id = vehicles.upsert_vehicle(
        model_id,
        {
            "name": "VEHICLE-1",
            "model_code": "CODE-1",
            "ssd": "v-ssd",
            "vid": "1",
        },
    )
    db.commit()
    category_id = vehicles.upsert_category(vehicle_id, "ENGINE/FUEL/TOOL", "1")
    db.commit()
    group_id = vehicles.upsert_group(
        category_id, "1101", "PARTIAL ENGINE", "u1", "http://x/unit?uid=u1"
    )
    db.commit()
    return brand_id, model_id, vehicle_id, category_id, group_id


@unittest.skipUnless(_RUN_DB_TESTS, f"set {_DB_TEST_ENV}=1 to run database tests")
class TestBrandRepository(unittest.TestCase):
    def setUp(self):
        self.db = _test_db()
        self.brands = BrandRepository(self.db)
        _wipe(self.db)

    def tearDown(self):
        self.db.close()

    def test_upsert_brand_returns_same_id_on_rerun(self):
        """重複 upsert 同一品牌必須回傳相同 id（續爬依賴此語意）。"""
        first = self.brands.upsert_brand("TOYOTA", "http://x")
        self.db.commit()
        second = self.brands.upsert_brand("TOYOTA", "http://x/new")
        self.db.commit()
        self.assertEqual(first, second)

    def test_upsert_model_preserves_existing_ssd(self):
        """既有 ssd 不能被 NULL 覆寫（COALESCE 語意）。"""
        brand_id = self.brands.upsert_brand("TOYOTA", None)
        self.db.commit()
        first = self.brands.upsert_model(brand_id, "4RUNNER", "ssd-keep", "u1")
        self.db.commit()
        second = self.brands.upsert_model(brand_id, "4RUNNER", None, "u2")
        self.db.commit()
        self.assertEqual(first, second)
        models = self.brands.list_models(brand_id)
        self.assertEqual(models[0]["ssd"], "ssd-keep")
        self.assertEqual(models[0]["url"], "u2")


@unittest.skipUnless(_RUN_DB_TESTS, f"set {_DB_TEST_ENV}=1 to run database tests")
class TestVehicleRepository(unittest.TestCase):
    def setUp(self):
        self.db = _test_db()
        self.brands = BrandRepository(self.db)
        self.vehicles = VehicleRepository(self.db)
        _wipe(self.db)

    def tearDown(self):
        self.db.close()

    def test_group_code_empty_string_not_null(self):
        """code 為 None 時必須寫入空字串，唯一鍵才不會被 NULL 破壞。

        同一分類下重複插入 code=None 的組，第二次應是更新而非新增。
        """
        brand_id = self.brands.upsert_brand("TOYOTA", None)
        self.db.commit()
        model_id = self.brands.upsert_model(brand_id, "M", "s", None)
        self.db.commit()
        vehicle_id = self.vehicles.upsert_vehicle(
            model_id, {"name": "V", "model_code": "C", "ssd": "s", "vid": "1"}
        )
        self.db.commit()
        category_id = self.vehicles.upsert_category(vehicle_id, "CAT", "1")
        self.db.commit()
        first = self.vehicles.upsert_group(category_id, None, "A", "u1", None)
        self.db.commit()
        second = self.vehicles.upsert_group(category_id, None, "B", "u2", None)
        self.db.commit()
        self.assertEqual(first, second)
        cats = self.vehicles.list_categories(vehicle_id)
        self.assertEqual(len(cats), 1)

    def test_category_name_change_keeps_cid_identity(self):
        brand_id = self.brands.upsert_brand("TOYOTA", None)
        model_id = self.brands.upsert_model(brand_id, "M", "s", None)
        vehicle_id = self.vehicles.upsert_vehicle(
            model_id, {"name": "V", "model_code": "C", "ssd": "s"}
        )
        first = self.vehicles.upsert_category(vehicle_id, "OLD NAME", "2")
        second = self.vehicles.upsert_category(vehicle_id, "NEW NAME", "2")
        self.db.commit()
        self.assertEqual(first, second)
        categories = self.vehicles.list_categories(vehicle_id)
        self.assertEqual([(c["name"], c["cid"]) for c in categories], [("NEW NAME", "2")])

    def test_same_category_name_with_different_cid_stays_distinct(self):
        brand_id = self.brands.upsert_brand("TOYOTA", None)
        model_id = self.brands.upsert_model(brand_id, "M", "s", None)
        vehicle_id = self.vehicles.upsert_vehicle(
            model_id, {"name": "V", "model_code": "C", "ssd": "s"}
        )
        first = self.vehicles.upsert_category(vehicle_id, "SAME NAME", "2")
        second = self.vehicles.upsert_category(vehicle_id, "SAME NAME", "3")
        self.db.commit()
        self.assertNotEqual(first, second)
        categories = self.vehicles.list_categories(vehicle_id)
        self.assertEqual({c["cid"] for c in categories}, {"2", "3"})

    def test_vehicle_session_token_rotation_updates_same_identity(self):
        brand_id = self.brands.upsert_brand("TOYOTA", None)
        model_id = self.brands.upsert_model(brand_id, "M", "model-token", None)
        vehicle = {
            "name": "GRADE A",
            "description": "D",
            "model_code": "C",
            "options": "4WD",
            "prod_period": "2020-2024",
            "grade": "PREMIUM",
            "market": "EU",
            "engine": "V6",
            "transmission": "AT",
            "ssd": "TOKEN-A",
        }
        first = self.vehicles.upsert_vehicle(model_id, vehicle)
        self.db.commit()
        second = self.vehicles.upsert_vehicle(model_id, {**vehicle, "ssd": "TOKEN-B"})
        self.db.commit()
        self.assertEqual(first, second)
        cur = self.db._execute("SELECT COUNT(*) AS n, MAX(ssd) AS ssd FROM vehicles")
        row = cur.fetchone()
        self.assertEqual(row["n"], 1)
        self.assertEqual(row["ssd"], "TOKEN-B")


@unittest.skipUnless(_RUN_DB_TESTS, f"set {_DB_TEST_ENV}=1 to run database tests")
class TestPartRepository(unittest.TestCase):
    def setUp(self):
        self.db = _test_db()
        self.brands = BrandRepository(self.db)
        self.vehicles = VehicleRepository(self.db)
        self.parts = PartRepository(self.db)
        self.brand_id, _, _, _, self.group_id = _brand_chain(
            self.brands, self.vehicles, self.parts, self.db
        )

    def tearDown(self):
        self.db.close()

    def test_upsert_parts_counts_new_by_unique_key(self):
        """新增計數必須以 (part_number, range_str) 為準。

        唯一鍵是 (group_id, part_number, range_str)：同料號不同 range
        是真實的新列，應計入 parts_new。
        """
        parts = [
            {"part_number": "111", "range_str": "01.2015 - 01.2016"},
            {"part_number": "111", "range_str": "01.2016 - 01.2018"},  # 同料號不同 range
            {"part_number": "222", "range_str": ""},
        ]
        new = self.parts.upsert_parts(self.group_id, parts)
        self.db.commit()
        self.assertEqual(new, 3)
        self.assertEqual(self.parts.count_parts_in_group(self.group_id), 3)

        # 重跑一次：全部已存在，新增數應為 0
        again = self.parts.upsert_parts(self.group_id, parts)
        self.db.commit()
        self.assertEqual(again, 0)
        self.assertEqual(self.parts.count_parts_in_group(self.group_id), 3)

    def test_upsert_parts_empty_list_returns_zero(self):
        """空清單必須回傳 0 且不寫任何東西。"""
        self.assertEqual(self.parts.upsert_parts(self.group_id, []), 0)

    def test_upsert_parts_same_key_updates_not_duplicates(self):
        """同 (group_id, part_number, range_str) 重複插入 → 更新而非新增。"""
        p = {"part_number": "AAA", "name": "OLD"}
        self.parts.upsert_parts(self.group_id, [p])
        self.db.commit()
        p = {"part_number": "AAA", "name": "NEW"}
        new = self.parts.upsert_parts(self.group_id, [p])
        self.db.commit()
        self.assertEqual(new, 0)
        self.assertEqual(self.parts.count_parts_in_group(self.group_id), 1)

    def test_upsert_parts_marks_explicit_run_membership(self):
        self.parts.upsert_parts(
            self.group_id,
            [{"part_number": "AAA", "range_str": ""}],
            run_id=46,
        )
        self.db.commit()
        cur = self.db._execute(
            "SELECT seen_run_id FROM parts WHERE group_id = %s", (self.group_id,)
        )
        self.assertEqual(cur.fetchone()["seen_run_id"], 46)


@unittest.skipUnless(_RUN_DB_TESTS, f"set {_DB_TEST_ENV}=1 to run database tests")
class TestCrawlRepository(unittest.TestCase):
    def setUp(self):
        self.db = _test_db()
        self.crawl = CrawlRepository(self.db)
        _wipe(self.db)

    def tearDown(self):
        self.db.close()

    def test_done_error_done_state_cycle(self):
        """done → error → done 的狀態覆寫必須正確。"""
        self.crawl.mark_done("model", "TOYOTA::4RUNNER")
        self.db.commit()
        self.assertTrue(self.crawl.is_done("model", "TOYOTA::4RUNNER"))

        self.crawl.mark_error("model", "TOYOTA::4RUNNER", "boom")
        self.db.commit()
        self.assertFalse(self.crawl.is_done("model", "TOYOTA::4RUNNER"))

        self.crawl.mark_done("model", "TOYOTA::4RUNNER")
        self.db.commit()
        self.assertTrue(self.crawl.is_done("model", "TOYOTA::4RUNNER"))

    def test_reset_scope_only_clears_target_scope(self):
        """reset_scope 只清指定 scope，不影響其他 scope。"""
        self.crawl.mark_done("model", "A")
        self.crawl.mark_done("vehicle", "B")
        self.db.commit()
        self.crawl.reset_scope("model")
        self.db.commit()
        self.assertFalse(self.crawl.is_done("model", "A"))
        self.assertTrue(self.crawl.is_done("vehicle", "B"))

    def test_reset_run_state_clears_all_scopes_for_only_that_run(self):
        august = CrawlRepository(self.db, run_key="2026-08")
        september = CrawlRepository(self.db, run_key="2026-09")
        august.mark_error("part", "P1", "boom")
        august.mark_done("model", "M1")
        september.mark_error("part", "P2", "keep")
        self.db.commit()
        august.reset_run_state("2026-08")
        self.db.commit()
        self.assertEqual(august.count_errors("2026-08"), 0)
        self.assertEqual(september.count_errors("2026-09"), 1)

    def test_mark_error_truncates_long_message(self):
        """超長錯誤訊息必須截斷後落庫（否則整筆 INSERT 失敗）。"""
        self.crawl.mark_error("model", "K", "x" * 5000)
        self.db.commit()
        cur = self.db._execute(
            "SELECT LENGTH(error_msg) AS n FROM crawl_state WHERE scope='model' AND scope_key='K'"
        )
        row = cur.fetchone()
        self.assertEqual(row["n"], 500)

    def test_count_errors_includes_pending(self):
        """count_errors 必須把 pending（未閉合）也計入，不只 error。"""
        run_key = "2026-09"
        crawl = CrawlRepository(self.db, run_key=run_key)
        crawl.mark_done("model", "A")
        crawl.mark_error("model", "B", "boom")
        # pending：直接插一筆未完成的狀態
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES (%s, 'model', 'C', 'pending')",
            (run_key,),
        )
        self.db.commit()
        # 1 error + 1 pending = 2；done 不算
        self.assertEqual(crawl.count_errors(run_key), 2)
        # 全部閉合後歸零
        crawl.mark_done("model", "B")
        crawl.mark_done("model", "C")
        self.db.commit()
        self.assertEqual(crawl.count_errors(run_key), 0)

    def test_is_group_fetched(self):
        """F1b：is_group_fetched 依 group terminal state 判斷該組是否已抓完。"""
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        parts = PartRepository(self.db)
        _, _, vehicle_id, _, group_id = _brand_chain(brands, vehicles, parts, self.db)
        run_key = "2026-07"
        self.crawl.start_run(run_key)
        # _brand_chain 建的 group (code=1101) 尚未標記 => 未抓過
        self.assertFalse(
            self.crawl.is_group_fetched(vehicle_id, "1101", run_key), "未標記 => 未抓過"
        )
        # 只寫入零件不標記 => 仍不算抓完（F1b：一筆零件不能代表整組完成）
        parts.upsert_parts(
            group_id,
            [
                {
                    "part_number": "P1",
                    "name": "n",
                    "code": "c",
                    "note": "x",
                    "quantity": "01",
                    "range_str": "",
                }
            ],
        )
        self.db.commit()
        self.assertFalse(
            self.crawl.is_group_fetched(vehicle_id, "1101", run_key),
            "只有零件、沒有 terminal state => 不算抓完",
        )
        # 標記 group fetched => 視為本 run 已抓過
        self.crawl.mark_group_fetched(group_id, run_key)
        self.db.commit()
        self.assertTrue(
            self.crawl.is_group_fetched(vehicle_id, "1101", run_key), "已標記 => 已抓過"
        )

    def test_is_group_fetched_scoped_by_run(self):
        """P1：跨月時，上個 run 抓過的組必須被視為未抓取（否則月重爬被 skip）。"""
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        parts = PartRepository(self.db)
        _, _, vehicle_id, _, group_id = _brand_chain(brands, vehicles, parts, self.db)
        # 八月 run：抓過 group 1101（寫入零件 + 標記 terminal state）
        self.crawl.start_run("2026-08")
        parts.upsert_parts(group_id, [{"part_number": "P1", "range_str": ""}])
        self.crawl.mark_group_fetched(group_id, "2026-08")
        self.db.commit()
        self.assertTrue(self.crawl.is_group_fetched(vehicle_id, "1101", "2026-08"))
        # 九月新 run：group state 是八月的，必須視為未抓取（月刷新不能空跑）。
        self.crawl.start_run("2026-09")
        self.assertFalse(self.crawl.is_group_fetched(vehicle_id, "1101", "2026-09"))
        # 空 run_key 一律視為未抓取（安全預設）
        self.assertFalse(self.crawl.is_group_fetched(vehicle_id, "1101", ""))

    def test_fetched_group_map_keyed_by_cid_and_code(self):
        """map 以 (cid, code) 為鍵 —— 不同分類的
        同 code 組不得互相覆蓋（DB 的 group 唯一身分是 category+code）。"""
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        _wipe(self.db)
        brand_id = brands.upsert_brand("TESTBRAND", "http://x/locate?c=TESTBRAND")
        self.db.commit()
        model_id = brands.upsert_model(brand_id, "MODEL-X", "s", "u")
        self.db.commit()
        vehicle_id = vehicles.upsert_vehicle(
            model_id, {"name": "V", "model_code": "C", "ssd": "s", "vid": "1"}
        )
        self.db.commit()
        cat_a = vehicles.upsert_category(vehicle_id, "ENGINE", "1")
        cat_b = vehicles.upsert_category(vehicle_id, "BODY", "2")
        self.db.commit()
        g1 = vehicles.upsert_group(cat_a, "G1", "g", "u", "url")
        vehicles.upsert_group(cat_b, "G1", "g", "u", "url")  # 同 code，未標記
        self.db.commit()
        self.crawl.mark_group_fetched(g1, "2026-08", status="done", row_count=2)
        self.db.commit()
        fetched = self.crawl.fetched_group_map(vehicle_id, "2026-08")
        self.assertIn(("1", "G1"), fetched)
        self.assertNotIn(("2", "G1"), fetched, "同 code 不同分類必須是不同的鍵")
        self.assertEqual(fetched[("1", "G1")], 2, "map 值必須是 row_count")
        # 未標記的組（BODY/G1）不在 map 中
        self.assertEqual(len(fetched), 1)
        # 空 run_key 回傳空集合（安全預設）
        self.assertEqual(self.crawl.fetched_group_map(vehicle_id, ""), {})

    def test_previous_row_count_map_uses_monotonic_verified_high_water(self):
        """縮水偵測基準只升不降，not_found 也不能清掉歷史最高值。"""
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        _wipe(self.db)
        brand_id = brands.upsert_brand("TESTBRAND", "http://x/locate?c=TESTBRAND")
        self.db.commit()
        model_id = brands.upsert_model(brand_id, "MODEL-X", "s", "u")
        self.db.commit()
        vehicle_id = vehicles.upsert_vehicle(
            model_id, {"name": "V", "model_code": "C", "ssd": "s", "vid": "1"}
        )
        self.db.commit()
        category_id = vehicles.upsert_category(vehicle_id, "ENGINE", "1")
        self.db.commit()
        g1 = vehicles.upsert_group(category_id, "G1", "g", "u", "url")
        g2 = vehicles.upsert_group(category_id, "G2", "g", "u", "url")
        g3 = vehicles.upsert_group(category_id, "G3", "g", "u", "url")
        self.db.commit()
        # 上月：G1 抓了 30 筆、G2 抓了 5 筆。
        self.crawl.mark_group_fetched(g1, "2026-07", status="done", row_count=30)
        self.crawl.mark_group_fetched(g2, "2026-07", status="done", row_count=5)
        self.db.commit()
        # 本月較小的 done 不得降低 G1；not_found 不得降低 G2。
        self.crawl.mark_group_fetched(g1, "2026-08", status="done", row_count=20)
        self.crawl.mark_group_fetched(g2, "2026-08", status="not_found", row_count=0)
        self.crawl.mark_group_fetched(g3, "2026-08", status="done", row_count=12)
        self.db.commit()
        prev = self.crawl.previous_row_count_map(vehicle_id, "2026-08")
        self.assertEqual(prev[("1", "G1")], 30, "較小的 done 不得降低 30 筆 high-water")
        self.assertEqual(prev[("1", "G2")], 5)
        self.assertEqual(prev[("1", "G3")], 12)
        self.assertEqual(self.crawl.previous_row_count(g1), 30, "逐組後備查詢取歷史最大值")
        # 空 run_key 回傳空 dict（安全預設）
        self.assertEqual(self.crawl.previous_row_count_map(vehicle_id, ""), {})

    def test_publish_snapshot_upserts_deletes_stale_and_rolls_back_atomically(self):
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        parts = PartRepository(self.db)
        _, _, _, _, group_id = _brand_chain(brands, vehicles, parts, self.db)
        run_id = self.crawl.start_run("2026-08")
        payload = [
            {"part_number": "P1", "name": "ONE", "range_str": ""},
            {"part_number": "P2", "name": "TWO", "range_str": ""},
        ]
        parts.upsert_parts(group_id, payload, run_id=run_id)
        self.assertEqual(self.crawl.publish_success_parts(run_id), 2)
        self.db.commit()

        next_run_id = self.crawl.start_run("2026-09")
        self.db.commit()
        parts.upsert_parts(group_id, payload[:1], run_id=next_run_id)
        self.assertEqual(self.crawl.publish_success_parts(next_run_id), 1)
        self.db.rollback()
        row = self.db.query_one("SELECT COUNT(*) AS n FROM published_parts")
        self.assertEqual(row["n"], 2, "rollback 後必須保留上一版完整 snapshot")

        parts.upsert_parts(group_id, payload[:1], run_id=next_run_id)
        self.assertEqual(self.crawl.publish_success_parts(next_run_id), 1)
        self.db.commit()
        row = self.db.query_one(
            "SELECT COUNT(*) AS n, MAX(part_number) AS part_number FROM published_parts"
        )
        self.assertEqual((row["n"], row["part_number"]), (1, "P1"))

    def test_start_run_does_not_downgrade_success(self):
        """P2：同月已有 success 時，start_run 不把它覆寫成 running。"""
        run_id = self.crawl.start_run("2026-10")
        self.crawl.finish_run(run_id, "success", {})
        # 再開一次同月 run：不應降級 success
        run_id2 = self.crawl.start_run("2026-10")
        self.assertEqual(run_id, run_id2)
        cur = self.db._execute("SELECT status FROM crawl_runs WHERE id = %s", (run_id,))
        self.assertEqual(
            cur.fetchone()["status"], "success", "全站成功後 start_run 不得覆寫成 running"
        )

    def test_finish_run_does_not_downgrade_success(self):
        """P2：已 success 的 run 被 partial 收尾 error 時不降級。"""
        run_id = self.crawl.start_run("2026-11")
        self.crawl.finish_run(run_id, "success", {})
        self.crawl.finish_run(run_id, "error", {}, "partial run")
        cur = self.db._execute("SELECT status FROM crawl_runs WHERE id = %s", (run_id,))
        self.assertEqual(
            cur.fetchone()["status"], "success", "partial error 不得抹掉全站 success 證據"
        )

    def test_finish_run_error_does_not_clobber_success_counts(self):
        """P2：success 之後的 partial error 收尾不得覆寫計數與 error_msg。"""
        run_id = self.crawl.start_run("2026-11")
        self.crawl.finish_run(
            run_id,
            "success",
            {
                "brands": 5,
                "models": 6,
                "vehicles": 7,
                "groups": 8,
                "parts": 9,
                "parts_new": 1,
            },
        )
        self.crawl.finish_run(
            run_id,
            "error",
            {
                "brands": 0,
                "models": 0,
                "vehicles": 0,
                "groups": 0,
                "parts": 0,
                "parts_new": 0,
            },
            "partial boom",
        )
        cur = self.db._execute("SELECT * FROM crawl_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["parts_ok"], 9, "success 列不能被 partial 計數覆寫")
        self.assertEqual(row["parts_new"], 1)
        self.assertEqual(row["brands_ok"], 5)
        self.assertIsNone(row["error_msg"], "success 列不得背上 partial 的 error_msg")

    def test_finish_run_preserves_success_finished_at(self):
        """SOL review P2：已是 success 的列，後續 partial/error 收尾
        不得覆寫 finished_at（全站成功的完成時間證據）。"""
        run_id = self.crawl.start_run("2026-11")
        self.crawl.finish_run(run_id, "success", {})
        cur = self.db._execute("SELECT finished_at FROM crawl_runs WHERE id = %s", (run_id,))
        first = cur.fetchone()["finished_at"]
        time.sleep(1.1)
        # partial run 收尾（不降級路徑）
        self.crawl.finish_run(run_id, "error", {}, "partial run")
        cur = self.db._execute(
            "SELECT status, finished_at FROM crawl_runs WHERE id = %s", (run_id,)
        )
        row = cur.fetchone()
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["finished_at"], first, "success 列的完成時間不得被後續收尾覆寫")

    def test_finish_run_records_counts(self):
        """finish_run 必須寫入各層計數。"""
        run_id = self.crawl.start_run()
        self.crawl.finish_run(
            run_id,
            "success",
            {
                "brands": 2,
                "models": 3,
                "vehicles": 4,
                "groups": 5,
                "parts": 100,
                "parts_new": 10,
            },
        )
        cur = self.db._execute("SELECT * FROM crawl_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["parts_ok"], 100)
        self.assertEqual(row["parts_new"], 10)

    def test_purge_legacy_vehicle_state(self):
        """P1：所有非 v5-hash 格式的 vehicle pending/error 都必須被清除。

        新版 _vehicle_key 回傳 SHA256 64-char hex digest。舊版 V0（無 |
        分隔）與 V1（含 | 但非 hex）都不會符合新 hash 模式，且新程式
        永遠不會覆寫舊 key（key 不相符），卻會被 count_errors 永久計入。
        清掉後這些車會以新 hash key 被重新爬取。model 層不受影響。
        """
        self.crawl.run_key = "2026-08"
        # V0 格式（無 |）
        for key in (
            "Toyota::1000::KP36LV-",
            "Toyota::4RUNNER::GRN215L-GKPGK",
        ):
            self.db._execute(
                "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
                "VALUES ('2026-08', 'vehicle', %s, 'pending')",
                (key,),
            )
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status, error_msg) "
            "VALUES ('2026-08', 'vehicle', 'Toyota::1000::KP36V-', 'error', 'old boom')",
        )
        # V1 格式（有 | 但非 hex hash）
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES ('2026-08', 'vehicle', 'Toyota::4RUNNER::GRN215L-GKPGK|V6 GAS', 'pending')",
        )
        # 一個合法的 V5 key（不應被刪）
        good_hash = "v5:" + "a" * 64
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES ('2026-08', 'vehicle', %s, 'done')",
            (good_hash,),
        )
        self.db._execute(
            "INSERT INTO crawl_state (run_key, scope, scope_key, status) "
            "VALUES ('2026-08', 'model', 'Toyota::COROLLA', 'pending')",
        )
        self.db.commit()

        purged = self.crawl.purge_legacy_vehicle_state("2026-08")
        self.assertEqual(purged, 4, "4 筆舊格式 vehicle pending/error 應被清除")
        cur = self.db._execute("SELECT scope, scope_key FROM crawl_state WHERE run_key = '2026-08'")
        rows = {(r["scope"], r["scope_key"]) for r in cur.fetchall()}
        self.assertIn(("vehicle", good_hash), rows)
        self.assertIn(("model", "Toyota::COROLLA"), rows)
        self.assertEqual(len(rows), 2, "合法 hash key 與 model 行必須保留")

    def test_list_brands(self):
        """list_brands 列出資料庫中所有品牌。"""
        brands = BrandRepository(self.db)
        brands.upsert_brand("TOYOTA", "http://x/locate?c=TOYOTA")
        brands.upsert_brand("HONDA", "http://x/locate?c=HONDA")
        self.db.commit()
        self.assertEqual(sorted(brands.list_brands()), ["HONDA", "TOYOTA"])

    def test_run_key_isolation(self):
        """不同 run_key 的 done 狀態必須互不干擾（每月重爬的基礎）。"""
        m1 = CrawlRepository(self.db, run_key="2026-08")
        m2 = CrawlRepository(self.db, run_key="2026-09")

        m1.mark_done("model", "Toyota::COROLLA")
        self.db.commit()
        # 8 月 done，9 月不該看到
        self.assertTrue(m1.is_done("model", "Toyota::COROLLA"))
        self.assertFalse(m2.is_done("model", "Toyota::COROLLA"))

        # 9 月也標 done → 各自獨立
        m2.mark_done("model", "Toyota::COROLLA")
        self.db.commit()
        self.assertTrue(m2.is_done("model", "Toyota::COROLLA"))

        # 同 run_key 重複標記不產生重複列（唯一鍵含 run_key）
        m1.mark_done("model", "Toyota::COROLLA")
        self.db.commit()
        cur = self.db._execute(
            "SELECT COUNT(*) AS n FROM crawl_state "
            "WHERE run_key='2026-08' AND scope='model' AND scope_key='Toyota::COROLLA'"
        )
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_start_run_same_run_key_updates(self):
        """同 run_key 重複 start_run 必須更新既有 run，而不是新增一筆。"""
        a = self.crawl.start_run("2026-08")
        b = self.crawl.start_run("2026-08")
        self.assertEqual(a, b)
        cur = self.db._execute("SELECT COUNT(*) AS n FROM crawl_runs WHERE run_key = '2026-08'")
        self.assertEqual(cur.fetchone()["n"], 1)

    def test_start_run_preserves_started_at_on_restart(self):
        """F1a：同月重啟不得重設 started_at（logical monthly run 起點）。

        v_parts 用 success run 的 started_at 當現存資料 cutoff；若重啟
        把它推到 NOW()，resume 跳過已 done vehicle（不更新其零件時間），
        成功後那些仍現存的零件會被誤排除（實測 3 車 15,300 筆）。
        """
        run_id = self.crawl.start_run("2026-12")
        cur = self.db._execute("SELECT started_at FROM crawl_runs WHERE id = %s", (run_id,))
        first = cur.fetchone()["started_at"]
        # 同月第二次啟動（模擬重啟）：started_at 必須保持不變
        time.sleep(1.1)
        again = self.crawl.start_run("2026-12")
        self.assertEqual(again, run_id)
        cur = self.db._execute("SELECT started_at FROM crawl_runs WHERE id = %s", (run_id,))
        self.assertEqual(cur.fetchone()["started_at"], first, "重啟不得移動 run 起點")

    def test_seen_does_not_change_existing_status(self):
        """F1b：seen 只是「見即記錄」，不得覆寫 done/error 狀態。"""
        self.crawl.mark_done("model", "TOYOTA::COROLLA")
        self.db.commit()
        self.crawl.seen("model", "TOYOTA::COROLLA")
        self.db.commit()
        self.assertTrue(
            self.crawl.is_done("model", "TOYOTA::COROLLA"), "seen 不得把 done 覆寫成 pending"
        )

    def test_scope_keys_prefix_match(self):
        """F1b：scope_keys 只回傳指定 prefix 開頭的鍵。"""
        self.crawl.mark_done("model", "TOYOTA::COROLLA")
        self.crawl.mark_done("model", "HONDA::ACCORD")
        self.crawl.mark_done("vehicle", "TOYOTA::COROLLA::ZRE|A")
        self.db.commit()
        keys = self.crawl.scope_keys(self.crawl.run_key, "model", "TOYOTA::")
        self.assertEqual(keys, {"TOYOTA::COROLLA"})
        vkeys = self.crawl.scope_keys(self.crawl.run_key, "vehicle", "TOYOTA::")
        self.assertEqual(vkeys, {"TOYOTA::COROLLA::ZRE|A"})

    def test_list_model_names_and_vehicle_keys(self):
        """F1b：閉合對帳的 DB 側查詢（model 名與 vehicle resume key）。"""
        brands = BrandRepository(self.db)
        vehicles = VehicleRepository(self.db)
        brand_id = brands.upsert_brand("TOYOTA", None)
        db = self.db
        db.commit()
        model_id = brands.upsert_model(brand_id, "COROLLA", "s", None)
        db.commit()
        vehicles.upsert_vehicle(model_id, {"name": "ALPHARD", "model_code": "AGH30", "ssd": "s"})
        db.commit()
        self.assertEqual(brands.list_model_names("TOYOTA"), ["COROLLA"])
        expected_hash = "v5:" + vehicle_identity_hash(
            model_id, {"name": "ALPHARD", "model_code": "AGH30", "ssd": "s"}
        )
        self.assertEqual(vehicles.list_vehicle_keys("TOYOTA"), [expected_hash])


class TestRepositorySqlContracts(unittest.TestCase):
    """不連資料庫的 SQL／安全邊界測試。"""

    def test_sql_errors_rollback_thread_transaction(self):
        for error in (
            pymysql.err.DataError(1406, "data too long"),
            pymysql.err.IntegrityError(1062, "duplicate key"),
            pymysql.err.OperationalError(1040, "server busy"),
        ):
            for method_name, cursor_method, args in (
                ("_execute", "execute", ("UPDATE parts SET name = %s", ("x",))),
                ("_executemany", "executemany", ("INSERT INTO parts VALUES (%s)", [(1,)])),
            ):
                with self.subTest(method=method_name, error=type(error).__name__):
                    db = Database()
                    conn = MagicMock()
                    cursor = conn.cursor.return_value.__enter__.return_value
                    getattr(cursor, cursor_method).side_effect = error
                    db._thread_conn = Mock(return_value=conn)
                    db.rollback = Mock()

                    with self.assertRaises(type(error)):
                        getattr(db, method_name)(*args)

                    db.rollback.assert_called_once_with()

    def test_category_with_cid_uses_single_safe_upsert(self):
        db = Mock()
        db._execute.return_value = Mock(lastrowid=17)

        category_id = VehicleRepository(db).upsert_category(3, "ENGINE", "1")

        self.assertEqual(category_id, 17)
        db._execute.assert_called_once()
        sql, params = db._execute.call_args.args
        self.assertIn("ON DUPLICATE KEY UPDATE", sql)
        self.assertIn("id = LAST_INSERT_ID(id)", sql)
        self.assertEqual(params, (3, "ENGINE", "1"))

    def test_part_membership_upserts_current_then_clears_only_stale(self):
        db = Mock()
        existing = Mock()
        existing.fetchall.return_value = [{"part_number": "OLD", "range_str": ""}]
        events = []

        def execute(sql, params=None):
            events.append(("execute", sql, params))
            return existing

        def executemany(sql, rows):
            events.append(("executemany", sql, rows))

        db._execute.side_effect = execute
        db._executemany.side_effect = executemany

        new_count = PartRepository(db).upsert_parts(
            7, [{"part_number": "NEW", "range_str": ""}], run_id=46
        )

        self.assertEqual(new_count, 1)
        self.assertEqual([event[0] for event in events], ["execute", "executemany", "execute"])
        stale_sql = events[-1][1]
        self.assertIn("seen_run_id <> %s", stale_sql)
        self.assertEqual(events[-1][2], (7, 46))

    def test_verified_row_count_is_monotonic_and_done_only(self):
        db = Mock()
        repo = CrawlRepository(db)

        repo.mark_group_fetched(9, "2026-08", status="done", row_count=20)
        done_sql, done_params = db._execute.call_args.args
        self.assertIn("GREATEST(verified_row_count, %s)", done_sql)
        self.assertEqual(done_params, ("2026-08", "done", 20, 20, 9))

        db.reset_mock()
        repo.mark_group_fetched(9, "2026-09", status="not_found", row_count=0)
        not_found_sql, _ = db._execute.call_args.args
        self.assertNotIn("verified_row_count", not_found_sql)

    def test_previous_count_queries_durable_high_water(self):
        db = Mock()
        cursor = Mock()
        cursor.fetchall.return_value = [{"cid": "1", "code": "G1", "row_count": 30}]
        db._execute.return_value = cursor

        result = CrawlRepository(db).previous_row_count_map(4, "2026-08")

        self.assertEqual(result, {("1", "G1"): 30})
        sql, params = db._execute.call_args.args
        self.assertIn("g.verified_row_count", sql)
        self.assertNotIn("fetched_run_key", sql)
        self.assertEqual(params, (4,))

    def test_remaining_group_count_excludes_current_run_receipts(self):
        db = Mock()
        cursor = Mock()
        cursor.fetchone.return_value = {"n": 42}
        db._execute.return_value = cursor

        count = CrawlRepository(db).remaining_group_count("2026-08")

        self.assertEqual(count, 42)
        sql, params = db._execute.call_args.args
        self.assertIn("fetched_run_key IS NULL OR fetched_run_key <> %s", sql)
        self.assertEqual(params, ("2026-08",))

        db.reset_mock()
        db._execute.return_value = cursor
        self.assertEqual(CrawlRepository(db).remaining_group_count(), 42)
        db._execute.assert_called_once_with("SELECT COUNT(*) AS n FROM groups_t")

    def test_scope_prefix_uses_escaped_sargable_like(self):
        db = Mock()
        cursor = Mock()
        cursor.fetchall.return_value = []
        db._execute.return_value = cursor

        CrawlRepository(db).scope_keys("2026-08", "model", r"A%_!B")

        sql, params = db._execute.call_args.args
        self.assertIn("scope_key LIKE CONCAT", sql)
        self.assertNotIn("LOCATE", sql)
        self.assertEqual(params, ("2026-08", "model", "A!%!_!!B"))

    def test_wipe_refuses_non_test_database_before_truncate(self):
        db = Mock()
        db.query_one.return_value = {"database_name": "partsouq_crawler"}

        with patch.dict(os.environ, {_DB_TEST_ENV: "1"}):
            with self.assertRaisesRegex(RuntimeError, "refusing destructive"):
                _wipe(db)

        db._execute.assert_not_called()
        db.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
