"""GPT5.6SOL review 修正的迴歸測試。

涵蓋：
  1. [P0] crawler.run 不再把「有 error / 有 limit / 空解析」誤標成 success
  2. [P1] supervisor 重複程序偵測能匹配 macOS 的真實 Python 命令列
  3. [P1] 心跳改為單一計時基準（不再約 40 分鐘才判卡死）
  4. [P1] cooldown_until 真正阻擋冷卻期間的重啟
  5. [P1] watchdog spawn 後重新確認 supervisor 存活（spawn 失敗回 1）
  6. [P2] cookie 匯出缺 cf_clearance 時 fail closed
  7. [P2] --no-browser 模式不啟動瀏覽器刷新
"""

import importlib.util
import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pymysql
import requests

from src import cloak
from src.cloak import CLOAK, REFRESH_RETRY_BACKOFF
from src.config import CRAWL
from src.crawler import Crawler
from src.db import ConnectionLost, Database
from src.governor import RequestGovernor
from src.http_client import ChallengeError, NotFoundError, SessionManager
from src.repositories import CrawlRepository, PartRepository, VehicleRepository
from src.supervisor import (
    CRAWLER_CMDLINE_RE,
    HANG_TIMEOUT,
    Supervisor,
)

_MOD_SPEC = importlib.util.spec_from_file_location(
    "watchdog_under_test",
    Path(__file__).resolve().parent.parent / "scripts" / "watchdog.py",
)
watchdog = importlib.util.module_from_spec(_MOD_SPEC)
_MOD_SPEC.loader.exec_module(watchdog)


class FakeProc:
    """模擬子程序：可設定 poll 結果。"""

    def __init__(self, poll_result=None):
        self._poll = poll_result
        self.pid = 4242

    def poll(self):
        return self._poll

    def terminate(self):
        self._poll = 1

    def kill(self):
        self._poll = 9

    def wait(self, timeout=None):
        return None


class TestCrawlRunStatus(unittest.TestCase):
    """P0：run() 的 final_status 必須反映真實完整性。"""

    def _crawler(self):
        http = mock.MagicMock()
        db = mock.MagicMock()
        c = Crawler(http, db, workers=1)
        c.run_id = 46
        self.addCleanup(c.close)
        c._brands = mock.MagicMock(return_value=[{"name": "TOYOTA"}])
        c.crawl_brand = mock.MagicMock(return_value=0)
        c.crawl.finish_run = mock.MagicMock()
        # SOL P1：品牌數下限預設 18，這些測試以單一品牌模擬 —— 先把
        # 下限降到 1（縮水偵測本身有專門的 test_brand_index_shrink_marks_error）。
        p = mock.patch.dict(CRAWL, {"min_brands": 1})
        p.start()
        self.addCleanup(p.stop)
        return c

    def test_brand_index_shrink_marks_error(self):
        """SOL P1：首頁品牌數低於 min_brands（首次爬取無 DB 參考點）=> run 標 error。"""
        c = self._crawler()
        with mock.patch.dict(CRAWL, {"min_brands": 18}):
            with self.assertRaises(RuntimeError):
                c.run()
        # 例外路徑也會 finish_run 成 error
        self.assertEqual(c.crawl.finish_run.call_args[0][1], "error")

    def test_errors_remain_marks_error(self):
        """crawl_state 有殘留 error => run 必須標 error。"""
        c = self._crawler()
        with mock.patch.object(c.crawl, "count_errors", return_value=7):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error")

    def test_no_errors_full_scope_marks_success(self):
        """無 error、無 limit、全部品牌 done => run 標 success。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
        ):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "success")

    def test_publish_failure_never_marks_run_success(self):
        """publish 失敗必須 rollback，不能先留下 success 再把 error no-op。"""
        c = self._crawler()
        with (
            mock.patch.object(
                c.crawl, "publish_success_parts", side_effect=RuntimeError("snapshot failed")
            ),
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
        ):
            with self.assertRaisesRegex(RuntimeError, "snapshot failed"):
                c.run()
        statuses = [call.args[1] for call in c.crawl.finish_run.call_args_list]
        self.assertEqual(statuses, ["error"])
        c.db.rollback.assert_called()
        self.assertEqual(c.last_status, "error")

    def test_success_publishes_before_status_and_memory_flag(self):
        """current snapshot、DB success、commit、記憶體 success 必須依序完成。"""
        c = self._crawler()
        events = []

        def finish(_run_id, status, _counts, _error=None):
            events.append(f"finish:{status}")

        c.crawl.finish_run.side_effect = finish
        original_commit = c.db.commit.side_effect
        c.db.commit.side_effect = lambda: events.append("commit")
        with (
            mock.patch.object(
                c.crawl,
                "publish_success_parts",
                side_effect=lambda run_id: events.append("publish"),
            ),
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
        ):
            c.run()
        self.assertEqual(events[-3:], ["publish", "finish:success", "commit"])
        self.assertEqual(c.last_status, "success")
        c.db.commit.side_effect = original_commit

    def test_uncompleted_brands_marks_error(self):
        """P1：count_errors=0 但已知品牌未全部完成 => 不得標 success。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA", "HONDA"]),
        ):

            def fake_is_done(scope, key):
                return scope != "brand" or key == "TOYOTA"

            with mock.patch.object(c.crawl, "is_done", side_effect=fake_is_done):
                c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error", "有品牌未完成時不得標全站 success")

    def test_limit_models_marks_error(self):
        """啟用 limit_models 視為局部執行 => run 標 error（不全站成功）。"""
        c = self._crawler()
        old = CRAWL["limit_models"]
        CRAWL["limit_models"] = 3
        try:
            with mock.patch.object(c.crawl, "count_errors", return_value=0):
                c.run()
        finally:
            CRAWL["limit_models"] = old
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error")

    def test_empty_brand_list_raises(self):
        """首頁解析出 0 個品牌 => 拋出例外（不該標 success）。"""
        c = self._crawler()
        c._brands.return_value = []
        with self.assertRaises(RuntimeError):
            c.run()
        # 例外路徑也會 finish_run 成 error
        self.assertEqual(c.crawl.finish_run.call_args[0][1], "error")

    def test_brand_level_failure_marks_error(self):
        """品牌層爬取失敗 => 記入 crawl_state(brand) 且 run 標 error。"""
        c = self._crawler()
        c.crawl_brand.side_effect = RuntimeError("locate parse failed")
        with (
            mock.patch.object(c.crawl, "mark_error") as mark,
            mock.patch.object(c.crawl, "count_errors", return_value=1),
        ):
            c.run()
        brand_err = [call for call in mark.call_args_list if call.args[0] == "brand"]
        self.assertTrue(brand_err, "品牌層失敗必須 mark_error(brand, ...)")
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error")

    def test_brand_success_clears_error(self):
        """品牌完整爬完 => mark_done("brand") 清除先前暫態 error（P0）。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "mark_done") as md,
            mock.patch.object(c.crawl, "count_errors", return_value=0),
        ):
            c.run()
        brand_calls = [call for call in md.call_args_list if call.args[0] == "brand"]
        self.assertTrue(brand_calls, "品牌成功後必須 mark_done(brand, ...)")
        self.assertEqual(brand_calls[0].args[1], "TOYOTA")

    def test_limit_vehicles_returns_truncated_as_failure(self):
        """limit_vehicles 截斷車型 => crawl_model 回傳含截斷數（model 不標 done）。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>pick</html>")
        vehicles = [
            {"name": f"V{i}", "model_code": f"M{i}", "ssd": "s", "vid": "0"} for i in range(10)
        ]
        old = CRAWL["limit_vehicles"]
        CRAWL["limit_vehicles"] = 3
        try:
            with (
                mock.patch("src.crawler.parse_vehicles", return_value=vehicles),
                mock.patch.object(c.crawl, "is_done", return_value=False),
                mock.patch.object(c.crawl, "mark_done") as md,
            ):
                # 每個 worker 都「成功」：截斷數仍必須計入失敗
                def ok_submit(fn, *a, **kw):
                    f = Future()
                    f.set_result(None)
                    return f

                with mock.patch.object(c._pool, "submit", side_effect=ok_submit):
                    failed, _worked = c.crawl_model(
                        "TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"}
                    )
        finally:
            CRAWL["limit_vehicles"] = old
        # 3 台爬完(failed=0) + 7 台被截斷 => 回傳 7
        self.assertEqual(failed, 7)
        # 被截斷的 7 台不該被標 done
        done_keys = [call.args[1] for call in md.call_args_list]
        self.assertEqual(len(done_keys), 3, "只標 done 被處理的 3 台")

    def test_limit_groups_truncated_returned(self):
        """limit_groups 超限 => crawl_groups 回傳截斷數且不再爬其餘 group。"""
        c = self._crawler()
        c.crawl_group = mock.MagicMock()
        groups = [
            {
                "category_name": "c",
                "cid": "1",
                "group_code": f"G{i}",
                "group_name": f"g{i}",
                "uid": "u",
                "url": "u",
            }
            for i in range(5)
        ]
        old = CRAWL["limit_groups"]
        CRAWL["limit_groups"] = 2
        c.counts["groups"] = 3  # 已超限
        try:
            with mock.patch("src.crawler.parse_groups", return_value=(groups, 0)):
                truncated = c.crawl_groups("TOYOTA", 1, "<html>groups</html>", default_cid="1")
        finally:
            CRAWL["limit_groups"] = old
        self.assertEqual(truncated, 5, "所有 group 都因超限而截斷")
        c.crawl_group.assert_not_called()

    def test_malformed_group_link_blocks_first_and_known_vehicle(self):
        """unit link 解析失敗時，不論有無歷史 manifest 都不得放行。"""
        html = (
            '<a href="/en/catalog/genuine/unit?cid=1&uid=U1">bad label</a>'
            '<a href="/en/catalog/genuine/unit?cid=1&uid=U2">1101: OK</a>'
        )
        for known in (set(), {"1101"}):
            c = self._crawler()
            c.crawl_group = mock.MagicMock()
            with mock.patch.object(c.vehicles, "list_group_codes_for_category", return_value=known):
                with self.assertRaisesRegex(RuntimeError, "malformed unit link"):
                    c.crawl_groups("TOYOTA", 1, html, "1")
            c.crawl_group.assert_not_called()

    def test_group_closure_does_not_cross_category(self):
        """cid=2 的同 code 不得掩蓋 cid=1 已知 group 的缺失。"""
        c = self._crawler()
        groups = [
            {
                "category_name": "ENGINE/FUEL/TOOL",
                "cid": "1",
                "group_code": "9999",
                "group_name": "OTHER",
                "uid": "U1",
                "url": "/unit?uid=U1",
            },
            {
                "category_name": "POWER TRAIN/CHASSIS",
                "cid": "2",
                "group_code": "1101",
                "group_name": "SAME CODE",
                "uid": "U2",
                "url": "/unit?uid=U2",
            },
        ]
        with (
            mock.patch("src.crawler.parse_groups", return_value=(groups, 0)),
            mock.patch.object(c.vehicles, "list_group_codes_for_category", return_value={"1101"}),
        ):
            with self.assertRaisesRegex(RuntimeError, "known group"):
                c.crawl_groups("TOYOTA", 1, "<html>groups</html>", default_cid="1")

    def test_default_category_truncation_raises(self):
        """P0：預設分類(ENGINE/FUEL/TOOL)的 limit_groups 截斷也必須讓 vehicle 標 error。

        舊碼忽略預設分類 crawl_groups 的回傳值 → 僅預設分類的車被截斷
        時仍會標 done，缺的零件組永久跳過（sub-agent 審查發現的漏洞）。
        """
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        c._get = mock.MagicMock(return_value="<html>vehicle page</html>")
        with (
            mock.patch("src.crawler.parse_category_links", return_value=([], 0)),
            mock.patch.object(c.vehicles, "list_categories", return_value=[]),
            mock.patch.object(c, "crawl_groups", side_effect=[3, 0]),
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_limit_groups_exact_boundary(self):
        """limit_groups 剛好等於已爬數 => 全部截斷（>= 語意，不超射 1）。"""
        c = self._crawler()
        c.crawl_group = mock.MagicMock()
        groups = [
            {
                "category_name": "c",
                "cid": "1",
                "group_code": f"G{i}",
                "group_name": f"g{i}",
                "uid": "u",
                "url": "u",
            }
            for i in range(5)
        ]
        old = CRAWL["limit_groups"]
        CRAWL["limit_groups"] = 2
        c.counts["groups"] = 2  # 已達上限
        try:
            with mock.patch("src.crawler.parse_groups", return_value=(groups, 0)):
                truncated = c.crawl_groups("TOYOTA", 1, "<html>groups</html>", default_cid="1")
        finally:
            CRAWL["limit_groups"] = old
        self.assertEqual(truncated, 5)
        c.crawl_group.assert_not_called()

    def test_vehicle_without_ssd_raises(self):
        """P0：缺 ssd token 的車型必須標 error，不能靜默 done。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "url": "u"}  # 無 ssd
        with mock.patch.object(c.vehicles, "list_categories", return_value=[]):
            with self.assertRaises(RuntimeError):
                c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_guard_rejects_short_blank_page(self):
        """P0：短版/空白 HTTP 200 解析 0 結果也必須視為失敗。"""
        c = self._crawler()
        old = CRAWL["block_breather"]
        CRAWL["block_breather"] = 0  # 測試不等待喘息
        try:
            # 15 bytes 的空白頁：修復前因 < 5000 bytes 被當成合法空資料
            with self.assertRaises(RuntimeError):
                c._guard_parse("<html></html>", [], "models", "TOYOTA")
        finally:
            CRAWL["block_breather"] = old

    def test_guard_rejects_empty_string(self):
        """P0：guard 不得把空字串當合法空資料放行（404 走例外 sentinel）。"""
        c = self._crawler()
        with self.assertRaises(RuntimeError):
            c._guard_parse("", [], "models", "TOYOTA")

    def test_group_404_is_graceful_skip(self):
        """P0：unit 頁 404 => 該 group 視為完成，不讓整台車失敗。"""
        c = self._crawler()
        c._get = mock.MagicMock(side_effect=NotFoundError("http 404 at /x"))
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        # 不拋例外即代表 404 被優雅跳過
        c.crawl_group("TOYOTA", 1, group)

    def test_non_unit_404_is_failure(self):
        """P0：locate/pick 等非 unit 頁 404 => 視為失敗（不是空資料）。"""
        c = self._crawler()
        c._get = mock.MagicMock(side_effect=NotFoundError("http 404 at /pick"))
        with mock.patch("src.crawler.parse_vehicles", return_value=[]):
            with self.assertRaises(NotFoundError):
                c.crawl_model("TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"})

    def test_short_pick_page_rejected(self):
        """P0：短版 pick 頁解析 0 車型 => crawl_model 拋出例外（不標 done）。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html></html>")
        old = CRAWL["block_breather"]
        CRAWL["block_breather"] = 0
        try:
            with mock.patch("src.crawler.parse_vehicles", return_value=[]):
                with self.assertRaises(RuntimeError):
                    c.crawl_model("TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"})
        finally:
            CRAWL["block_breather"] = old

    def test_group_skip_when_already_fetched(self):
        """優化：同一 run 已抓過的 group（有零件）不再重抓。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        with mock.patch.object(c.crawl, "is_group_fetched", return_value=True):
            c.crawl_group("TOYOTA", 1, group, skip_if_fetched=True)
        # 已抓過 => 不發 unit 請求
        c._get.assert_not_called()

    def test_group_skip_not_applied_when_empty(self):
        """優化：該組沒有零件（未成功抓過）=> 仍要發請求補抓。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        with (
            mock.patch.object(c.crawl, "is_group_fetched", return_value=False),
            mock.patch.object(c.crawl, "previous_row_count", return_value=0),
            mock.patch(
                "src.crawler.parse_parts",
                return_value=(
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
                    0,
                ),
            ),
        ):
            c.crawl_group("TOYOTA", 1, group, skip_if_fetched=True)
        c._get.assert_called_once()

    def test_group_skip_uses_map_without_db_query(self):
        """SOL P1：已提供 receipt map（含空 map）時未命中視為未抓過，
        不得回退逐組查 DB（N+1）。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        with (
            mock.patch.object(c.crawl, "is_group_fetched") as igf,
            mock.patch(
                "src.crawler.parse_parts",
                return_value=(
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
                    0,
                ),
            ),
        ):
            c.crawl_group("TOYOTA", 1, group, skip_if_fetched=True, fetched={})
        igf.assert_not_called()
        c._get.assert_called_once()  # 空 map 的未命中 = 未抓過 → 發請求

    def test_group_skip_before_upsert_and_commit(self):
        """SOL P1：已抓過的組在 upsert / commit「之前」就跳過，
        不產生任何寫入。"""
        c = self._crawler()
        group = {
            "category_name": "ENGINE",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        fetched = {("1", "G1"): 5}
        with (
            mock.patch.object(c.crawl, "is_group_fetched") as igf,
            mock.patch.object(c.vehicles, "upsert_category") as uc,
            mock.patch.object(c.vehicles, "upsert_group") as ug,
        ):
            c.crawl_group("TOYOTA", 1, group, skip_if_fetched=True, fetched=fetched)
        igf.assert_not_called()
        uc.assert_not_called()
        ug.assert_not_called()

    def test_group_skip_scoped_by_category(self):
        """receipt map 以 (cid, code) 為鍵 —— 同 code
        不同分類的組不得被另一分類的 receipt 誤 skip。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "BODY",
            "cid": "2",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        # ENGINE/G1 已抓過，BODY/G1 沒有
        fetched = {("1", "G1"): 5}
        with mock.patch(
            "src.crawler.parse_parts",
            return_value=(
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
                0,
            ),
        ):
            c.crawl_group("TOYOTA", 1, group, skip_if_fetched=True, fetched=fetched)
        c._get.assert_called_once()  # BODY/G1 未抓過 → 必須發請求

    def test_malformed_parts_block_receipt(self):
        """SOL P1：頁面全為結構缺欄的 malformed 列（parts 空但 malformed>0）
        => crawl_group 拋錯、不寫 terminal receipt，且報 malformed 而非
        「parsed 0 parts」。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        with (
            mock.patch("src.crawler.parse_parts", return_value=([], 2)),
            mock.patch.object(c.crawl, "mark_group_fetched") as mgf,
            mock.patch.object(c, "_guard_parse") as guard,
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_group("TOYOTA", 1, group)
        guard.assert_not_called()  # malformed 檢查在 guard 之前
        mgf.assert_not_called()  # 有 malformed 列 => 不得標 done / not_found

    def test_group_deadlock_retries_full_block_once(self):
        """SOL P2：零件+receipt 區塊遇 deadlock => rollback 後由服務層
        重跑**完整區塊**一次（db.py 不再重跑單一 SQL，避免交易殘缺）。

        讓 mark_group_fetched（receipt 步驟）先炸：若實作只重跑 upsert
        而不重跑整個區塊，receipt 不會被呼叫第二次，此測試即失敗。
        """
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": "P1",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
        ]
        calls = {"n": 0}

        def flaky_receipt(group_id, run_key="", status="done", row_count=0):
            calls["n"] += 1
            if calls["n"] == 1:
                raise pymysql.err.OperationalError(1213, "Deadlock found when trying to get lock")
            return None

        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "previous_row_count", return_value=0),
            mock.patch.object(c.parts, "upsert_parts", return_value=1) as ups,
            mock.patch.object(c.crawl, "mark_group_fetched", side_effect=flaky_receipt),
            mock.patch.object(c.db, "rollback") as rb,
        ):
            c.crawl_group("TOYOTA", 1, group)
        self.assertEqual(calls["n"], 2, "deadlock 後必須重跑完整區塊（含 receipt）")
        self.assertEqual(ups.call_count, 2, "重跑完整區塊 = upsert 也要重跑")
        self.assertEqual(rb.call_count, 1, "重跑前必須 rollback")

    def test_group_connection_lost_retries_full_block_once(self):
        """SOL review P1：零件+receipt 區塊遇斷線（2006/2013/InterfaceError）
        => db.py 捨棄舊連線並拋 ConnectionLost，由服務層重跑**完整區塊**
        一次 —— 舊碼「重連後只重跑單一 SQL」會讓 A 的未提交 parts 隨
        斷線回滾、B 卻能提交 terminal receipt（receipt_without_parts）。

        讓 mark_group_fetched（receipt 步驟）先炸：若實作只重跑 upsert
        而不重跑整個區塊，receipt 不會被呼叫第二次，此測試即失敗。
        """
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": "P1",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
        ]
        calls = {"n": 0}

        def flaky_receipt(group_id, run_key="", status="done", row_count=0):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionLost(2013)
            return None

        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "previous_row_count", return_value=0),
            mock.patch.object(c.parts, "upsert_parts", return_value=1) as ups,
            mock.patch.object(c.crawl, "mark_group_fetched", side_effect=flaky_receipt),
        ):
            c.crawl_group("TOYOTA", 1, group)
        self.assertEqual(calls["n"], 2, "斷線後必須重跑完整區塊（含 receipt）")
        self.assertEqual(ups.call_count, 2, "重跑完整區塊 = upsert 也要重跑")

    def test_commit_connection_lost_retries_full_block(self):
        """SOL review P2：commit 階段的斷線也必須重跑完整 parts+receipt
        區塊 —— 舊碼 commit() 重建連線後拋原 OperationalError，而
        crawl_group 只認 ConnectionLost / 1205 / 1213，commit 斷線
        不在重試涵蓋內。現在 commit() 對 2006/2013/InterfaceError
        拋 ConnectionLost，crawl_group 捕獲後重跑完整區塊。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": "P1",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
        ]
        commits = {"n": 0}
        real_commit = c.db.commit

        def flaky_commit():
            commits["n"] += 1
            # 第 1 次 commit 是 category/group upsert（在零件區塊之外，
            # 不該重試）；第 2 次才是零件+receipt 區塊的 commit —— 讓它
            # 斷線一次，驗證完整區塊被重跑。
            if commits["n"] == 2:
                raise ConnectionLost(2013)
            return real_commit()

        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "previous_row_count", return_value=0),
            mock.patch.object(c.parts, "upsert_parts", return_value=1) as ups,
            mock.patch.object(c.crawl, "mark_group_fetched", return_value=None) as mgf,
            mock.patch.object(c.db, "commit", side_effect=flaky_commit),
        ):
            c.crawl_group("TOYOTA", 1, group)
        self.assertEqual(commits["n"], 3, "2 次區塊 commit + 1 次區塊外 commit")
        self.assertEqual(ups.call_count, 2, "commit 斷線重跑 = upsert 也要重跑")
        self.assertEqual(mgf.call_count, 2, "commit 斷線重跑 = receipt 也要重跑")

    def test_category_shrink_marks_vehicle_error(self):
        """SOL review P1：DB 已知分類在本次 vehicle 頁完全沒被解析到
        （分類連結縮水/反爬變體）=> crawl_vehicle 必須拋錯，車不得標
        done —— 否則缺少其他分類時該分類的零件組永遠不會補抓。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        c._get = mock.MagicMock(return_value="<html>vehicle page</html>")
        with (
            mock.patch.object(
                c.vehicles,
                "list_categories",
                return_value=[{"id": 1, "name": "POWER TRAIN/CHASSIS", "cid": "2"}],
            ),
            mock.patch("src.crawler.parse_category_links", return_value=([], 0)),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_category_reconciliation_ok_when_all_present(self):
        """SOL review P1：DB 已知分類全部都被本次解析到 => 不拋錯
        （正常車維持 done 的資格）。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        c._get = mock.MagicMock(return_value="<html>vehicle page</html>")
        with (
            mock.patch.object(
                c.vehicles,
                "list_categories",
                return_value=[
                    {"id": 1, "name": "ENGINE/FUEL/TOOL", "cid": "1"},
                    {"id": 2, "name": "POWER TRAIN/CHASSIS", "cid": "2"},
                ],
            ),
            mock.patch(
                "src.crawler.parse_category_links",
                return_value=(
                    [
                        {
                            "category_name": "POWER TRAIN/CHASSIS",
                            "cid": "2",
                            "ssd": "s",
                            "vid": "0",
                            "url": "/vehicle?cid=2",
                        }
                    ],
                    0,
                ),
            ),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            # 不拋例外即代表對帳通過
            c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_first_time_vehicle_category_shrink_detected(self):
        """SOL review P1：首爬新車（DB 無歷史分類）沒有對帳參考點時，
        改用頁面結構契約 —— vehicle 頁有 `/vehicle?` 導覽連結卻解析出
        0 個帶 cid 的分類 => 車不得標 done（反爬/版型變體）。

        reviewer probe：known_categories 為空時 parse_category_links
        只回空清單，只要預設分類成功舊碼就放行。
        """
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        html = '<a href="/en/catalog/genuine/vehicle?c=TOYOTA&ssd=s&vid=0&cid=2">PT</a>'
        c._get = mock.MagicMock(return_value=html)
        with (
            mock.patch.object(c.vehicles, "list_categories", return_value=[]),
            mock.patch("src.crawler.parse_category_links", return_value=([], 1)),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_first_time_vehicle_no_nav_links_passes(self):
        """SOL review P1：首爬車頁面完全沒有分類導覽連結（只有預設分類）
        => 沒有可偵測的縮水訊號，維持現況（不誤殺）。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        c._get = mock.MagicMock(return_value="<html>vehicle page, no nav links</html>")
        with (
            mock.patch.object(c.vehicles, "list_categories", return_value=[]),
            mock.patch("src.crawler.parse_category_links", return_value=([], 0)),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            # 不拋例外即代表通過（頁面沒有導覽連結 = 無從偵測縮水）
            c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_existing_vehicle_new_category_missing_cid_detected(self):
        """SOL review P1 追補：既有車（DB 已有歷史分類）頁面出現兩個
        新分類連結，其中一個缺 cid 被 parser 跳過 => raw=2、parsed=1 =>
        crawl_vehicle 必須拋錯，不因 known_categories 非空而略過對帳。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        html = (
            '<a href="/en/catalog/genuine/vehicle?c=TOYOTA&ssd=s&vid=0&cid=2">A</a>'
            '<a href="/en/catalog/genuine/vehicle?c=TOYOTA&ssd=s&vid=0">B</a>'
        )
        c._get = mock.MagicMock(return_value=html)
        with (
            mock.patch.object(
                c.vehicles,
                "list_categories",
                return_value=[{"id": 2, "name": "A", "cid": "2"}],
            ),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_existing_vehicle_all_new_categories_parsed_passes(self):
        """SOL review P1 追補：既有車頁面有兩個新分類連結且都成功解析
        => raw=2、parsed=2 => 不應拋錯。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": "0", "url": "u"}
        html = (
            '<a href="/en/catalog/genuine/vehicle?c=TOYOTA&ssd=s&vid=0&cid=2">A</a>'
            '<a href="/en/catalog/genuine/vehicle?c=TOYOTA&ssd=s&vid=0&cid=3">B</a>'
        )
        c._get = mock.MagicMock(return_value=html)
        with (
            mock.patch.object(
                c.vehicles,
                "list_categories",
                return_value=[{"id": 2, "name": "A", "cid": "2"}],
            ),
            mock.patch(
                "src.crawler.parse_category_links",
                return_value=(
                    [
                        {
                            "category_name": "A",
                            "cid": "2",
                            "ssd": "s",
                            "vid": "0",
                            "url": "https://partsouq.com/vehicle?cid=2",
                        },
                        {
                            "category_name": "B",
                            "cid": "3",
                            "ssd": "s",
                            "vid": "0",
                            "url": "https://partsouq.com/vehicle?cid=3",
                        },
                    ],
                    0,
                ),
            ),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            c.crawl_vehicle("TOYOTA", 1, vehicle)

    def test_group_row_count_shrink_blocks_receipt(self):
        """SOL review P1：格式完整但內容大幅縮水（row_count 遠少於
        前次 receipt）=> 拒絕寫 terminal receipt —— malformed 抓不到
        這種頁面（六欄齊全）、guard 也會放行，缺漏不能被固定。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": f"P{i}",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
            for i in range(3)
        ]
        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "mark_group_fetched") as mgf,
            mock.patch.object(c, "_guard_parse") as guard,
        ):
            with self.assertRaises(RuntimeError):
                c.crawl_group("TOYOTA", 1, group, prev_rows={("1", "G1"): 30})
        mgf.assert_not_called()  # 縮水 => 不得標 done
        guard.assert_not_called()  # 縮水檢查在 guard 之前

    def test_group_row_count_normal_no_block(self):
        """SOL review P1：row_count 與前次相當 => 正常寫 receipt（不誤殺）。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": f"P{i}",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
            for i in range(20)
        ]
        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "mark_group_fetched") as mgf,
        ):
            c.crawl_group("TOYOTA", 1, group, prev_rows={("1", "G1"): 30})
        self.assertEqual(mgf.call_args.kwargs.get("row_count"), 20, "正常規模必須寫 receipt")

    def test_group_row_count_small_previous_ignored(self):
        """SOL review P1：前次 receipt < 3 筆的小組不套用縮水檢查
        （零件可能被站方合法下架一兩筆，避免誤殺）。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>unit</html>")
        group = {
            "category_name": "c",
            "cid": "1",
            "group_code": "G1",
            "group_name": "g1",
            "uid": "u",
            "url": "/unit?x=1",
        }
        parts_list = [
            {
                "part_number": "P1",
                "name": "n",
                "code": "c",
                "note": "x",
                "quantity": "01",
                "range_str": "",
            }
        ]
        with (
            mock.patch("src.crawler.parse_parts", return_value=(parts_list, 0)),
            mock.patch.object(c.crawl, "mark_group_fetched") as mgf,
        ):
            c.crawl_group("TOYOTA", 1, group, prev_rows={("1", "G1"): 2})
        self.assertEqual(mgf.call_count, 1, "前次只有 2 筆的小組不套用縮水檢查")

    def test_guard_throttles_global_governor(self):
        """反爬偵測時 guard 必須暫停全域 governor（其他 worker 在
        acquire 一起阻塞，避免 thundering herd 重錘同一批頁面）。"""
        c = self._crawler()
        old = CRAWL["block_breather"]
        CRAWL["block_breather"] = 1  # 只暫停 1 秒
        try:
            t0 = time.monotonic()
            with self.assertRaises(RuntimeError):
                c._guard_parse("<html>stub</html>", [], "parts", "TOYOTA g1")
            # governor 的 block_until 應設定在 guard 進入時 + breather
            self.assertGreater(
                c.governor._block_until,
                t0 + 0.5,
                "guard 必須暫停全域 governor（throttle）",
            )
        finally:
            CRAWL["block_breather"] = old

    def test_get_governor_gates_wire_request(self):
        """SOL P1：全域限流在 session.get 內、每次 wire request 前生效
        （_get 本身不另發請求，也不自行 acquire）。"""
        gov = RequestGovernor(rate=1000, burst=100)
        sm = SessionManager(cookies=[], no_browser=True, gov=gov)
        sm.session.get = mock.MagicMock(
            return_value=mock.MagicMock(status_code=200, text="<html>ok</html>", headers={})
        )
        sm.sleep = mock.MagicMock()
        c = Crawler(sm, mock.MagicMock(), workers=1, governor=gov)
        self.addCleanup(c.close)
        c._local.session = sm  # 讓 _session() 直接拿到這個 mock session
        acquired = {"n": 0}
        real_acquire = gov.acquire

        def counting():
            acquired["n"] += 1
            return real_acquire()

        with mock.patch.object(gov, "acquire", side_effect=counting):
            html = c._get("http://x")
        self.assertEqual(html, "<html>ok</html>")
        self.assertEqual(acquired["n"], 1, "1 次 wire GET = 1 次 acquire")
        self.assertEqual(sm.session.get.call_count, 1)

    def test_vehicle_key_uses_stable_specification(self):
        """v5 key 與 DB identity 對齊，session token 輪替不換 key。"""
        c = self._crawler()
        v1 = {"name": "ALPHARD A", "model_code": "AGH30", "ssd": "s", "url": "u"}
        v2 = {"name": "ALPHARD B", "model_code": "AGH30", "ssd": "s", "url": "u"}
        v3 = {"name": "ALPHARD A", "model_code": "AGH30", "ssd": "s2", "url": "u"}
        k1 = c._vehicle_key(7, v1)
        k2 = c._vehicle_key(7, v2)
        k3 = c._vehicle_key(7, v3)
        self.assertNotEqual(k1, k2, "同 model_code 不同 name 必須是不同的 resume key")
        self.assertEqual(k1, k3, "ssd 是 session token，輪替時必須維持同一 resume key")
        v4 = {**v1, "options": "4WD"}
        self.assertNotEqual(k1, c._vehicle_key(7, v4), "不同穩定規格必須是不同 key")
        stable_variant = {
            **v1,
            "grade": "PREMIUM",
            "market": "EU",
            "engine": "V6",
            "body_style": "WAGON",
        }
        self.assertNotEqual(
            k1,
            c._vehicle_key(7, stable_variant),
            "parser 支援的規格欄不得被 identity 忽略",
        )
        self.assertNotEqual(k1, c._vehicle_key(8, v1), "不同 parent model 不得共用 key")
        # same identity → same key (deterministic)
        self.assertEqual(k1, c._vehicle_key(7, v1))
        # 明確版本前綴 + 64 hex，總長固定 67。
        self.assertEqual(len(k1), 67)
        self.assertRegex(k1, r"^v5:[a-f0-9]{64}$")

        # length-prefix encoding 不會因欄位內含分隔符而碰撞。
        left = {"model_code": "A|B", "name": "C", "ssd": "s"}
        right = {"model_code": "A", "name": "B|C", "ssd": "s"}
        self.assertNotEqual(c._vehicle_key(7, left), c._vehicle_key(7, right))

    def test_missing_vid_uses_zero(self):
        """vid=None 不得傳入 urllib.quote 造成 TypeError。"""
        c = self._crawler()
        vehicle = {"name": "V", "model_code": "M", "ssd": "s", "vid": None, "url": "u"}
        c._get = mock.MagicMock(return_value="<html></html>")
        with (
            mock.patch.object(c.vehicles, "list_categories", return_value=[]),
            mock.patch("src.crawler.parse_category_links", return_value=([], 0)),
            mock.patch.object(c, "crawl_groups", return_value=0),
        ):
            c.crawl_vehicle("TOYOTA", 1, vehicle)
        self.assertIn("vid=0", c._get.call_args.args[0])

    def test_backoff_cancels_remaining_futures(self):
        """P1：達失敗門檻後，尚未執行的 futures 必須被取消/不再派工
        （F5 bounded 派工：一次只保留 workers*2 個 in-flight；
        SOL P2：每輪依完成數量補工，多個同時完成就一次補多個）。"""
        c = self._crawler()
        vehicles = [
            {"name": f"V{i}", "model_code": f"M{i}", "ssd": "s", "vid": "0"} for i in range(4)
        ]
        c._get = mock.MagicMock(return_value="<html>pick page</html>")
        with (
            mock.patch("src.crawler.parse_vehicles", return_value=vehicles),
            mock.patch.object(c.crawl, "is_done", return_value=False),
        ):
            # 所有 submit 回傳「會失敗」的 future（模擬連續失敗）
            def fail_submit(fn, *a, **kw):
                f = Future()
                f.set_exception(RuntimeError("boom"))
                return f

            with (
                mock.patch.object(c._pool, "submit", side_effect=fail_submit) as submit,
                mock.patch.object(c.crawl, "mark_error"),
            ):
                failed, _worked = c.crawl_model(
                    "TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"}
                )
        # 門檻 = max(3, 4//2) = 3：第 3 台失敗時 give-up
        self.assertEqual(failed, 3)
        # F5 bounded + SOL P2 補工：workers=1 → max_inflight=2，
        # 首批 2 台全失敗後一次補 2 台（共 4），第 3 台失敗觸發 give-up
        # —— 其餘組永不派工（對照：一次 submit 全部或每輪只補 1 台
        # 都抓不到這個數量）。
        self.assertEqual(submit.call_count, 4, "bounded 派工不得一次 submit 全部 4 台")

    def test_backoff_running_futures_settled_via_callback(self):
        """P2：give-up 後仍在跑的 futures 完成時必須由 callback 收尾
        （不能只 cancel —— running future 無法取消，結果會蒸發）。"""

        class RunningFuture(Future):
            """模擬已開始執行、無法取消的 future（cancel() 回 False）。"""

            def cancel(self):
                return False

        c = self._crawler()
        c.workers = 2  # max_inflight = 4：讓第 4 台進入 in-flight 時觸發 give-up
        vehicles = [
            {"name": f"V{i}", "model_code": f"M{i}", "ssd": "s", "vid": "0"} for i in range(6)
        ]
        c._get = mock.MagicMock(return_value="<html>pick page</html>")
        running = []
        failed_count = 0

        def mixed_submit(fn, *a, **kw):
            nonlocal failed_count
            if failed_count < 3:
                failed_count += 1
                f = Future()
                f.set_exception(RuntimeError("boom"))
                return f
            f = RunningFuture()
            running.append(f)
            return f

        with (
            mock.patch("src.crawler.parse_vehicles", return_value=vehicles),
            mock.patch.object(c.crawl, "is_done", return_value=False),
            mock.patch.object(c._pool, "submit", side_effect=mixed_submit) as submit,
            mock.patch.object(c.crawl, "mark_done") as mark_done,
        ):
            failed, _worked = c.crawl_model(
                "TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"}
            )
            # 3 台立即失敗觸發 give-up；第 4 台（已 submit）留在 running
            self.assertEqual(failed, 3)
            self.assertEqual(submit.call_count, 4, "bounded 派工最多只派 4 台")
            self.assertEqual(len(running), 1, "give-up 當下只有 1 台 in-flight 未完成")
            # give-up 後才完成的 running future 必須由 _settle callback 收尾
            for f in running:
                f.set_result(None)
            self.assertEqual(mark_done.call_count, 1, "running futures 完成後必須由 callback 收尾")

    def test_crawl_brand_no_work_no_sleep(self):
        """P1：品牌完全無工作（所有 model 已 done）時不得 sleep 120s
        —— 續爬尾聲多個已完成品牌會合計 20+ 分鐘無寫入，觸發
        supervisor 卡死誤重啟。"""
        c = self._crawler()
        # _crawler() helper 把 crawl_brand mock 掉了；這裡恢復真實方法，
        # 否則測試只是呼叫 MagicMock（假陽性）。
        c.crawl_brand = Crawler.crawl_brand.__get__(c)
        c._get = mock.MagicMock(return_value="<html>locate</html>")
        with (
            mock.patch(
                "src.crawler.parse_brand_index", return_value=[{"name": "M1"}, {"name": "M2"}]
            ),
            mock.patch.object(c.crawl, "is_done", return_value=True),
            mock.patch("src.crawler.time.sleep") as sleep,
        ):
            result = c.crawl_brand("TOYOTA")
        self.assertEqual(result, 0)
        self.assertEqual(sleep.call_count, 0, "全部 done 的品牌不得觸發品牌間休息")

    def test_crawl_model_all_done_returns_not_worked(self):
        """F3：vehicles 全 done（純收尾，只差 model 狀態標記）=> 回報 (0, False)。"""
        c = self._crawler()
        c._get = mock.MagicMock(return_value="<html>pick</html>")
        vehicles = [{"name": "V", "model_code": "M", "ssd": "s", "vid": "0"}]
        with (
            mock.patch("src.crawler.parse_vehicles", return_value=vehicles),
            mock.patch.object(c.crawl, "is_done", return_value=True),
        ):
            failed, worked = c.crawl_model("TOYOTA", 1, {"name": "COROLLA", "ssd": "s", "url": "u"})
        self.assertEqual((failed, worked), (0, False), "純收尾 model 沒有實際零件工作")

    def test_crawl_brand_no_sleep_for_finish_only_models(self):
        """F3：品牌只有「收尾 model」（沒實際爬任何車）時不得 sleep 120s
        —— 否則續爬尾聲 10 個收尾品牌合計 1,200s 無零件寫入，撞上
        supervisor 的 20 分鐘卡死門檻而誤重啟。"""
        c = self._crawler()
        c.crawl_brand = Crawler.crawl_brand.__get__(c)
        c._get = mock.MagicMock(return_value="<html>locate</html>")
        with (
            mock.patch("src.crawler.parse_brand_index", return_value=[{"name": "M1"}]),
            mock.patch.object(c.crawl, "is_done", return_value=False),
            mock.patch.object(c, "crawl_model", return_value=(0, False)),
            mock.patch("src.crawler.time.sleep") as sleep,
        ):
            result = c.crawl_brand("TOYOTA")
        self.assertEqual(result, 0)
        self.assertEqual(sleep.call_count, 0, "純收尾品牌不得觸發品牌間休息（F3）")

    def test_crawl_brand_still_rests_when_real_work(self):
        """F3：有實際爬車的品牌維持品牌間休息（回歸確認）。"""
        c = self._crawler()
        c.crawl_brand = Crawler.crawl_brand.__get__(c)
        c._get = mock.MagicMock(return_value="<html>locate</html>")
        with (
            mock.patch("src.crawler.parse_brand_index", return_value=[{"name": "M1"}]),
            mock.patch.object(c.crawl, "is_done", return_value=False),
            mock.patch.object(c, "crawl_model", return_value=(0, True)),
            mock.patch("src.crawler.time.sleep") as sleep,
        ):
            c.crawl_brand("TOYOTA")
        self.assertGreaterEqual(sleep.call_count, 1, "有實際工作的品牌維持品牌間休息")

    def test_missing_brand_marks_error(self):
        """F1b：_brands() 縮水（只回部分品牌）時，即使歷史品牌都 done
        也不得標 success —— 舊檢查只看「歷史品牌是否 done」，縮水的
        品牌有上個月的 done 行會被誤導。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA", "HONDA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
        ):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error", "本 run 未訪問的品牌必須讓 run 標 error")

    def test_closure_detects_missing_models(self):
        """F1b：brand 已 done 但 DB 有 model 本 run 從未見到 => 標 error
        （locate 頁縮水解析的偵測）。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
            mock.patch.object(c.brands, "list_model_names", return_value=["COROLLA", "4RUNNER"]),
            mock.patch.object(c.crawl, "scope_keys", return_value={"TOYOTA::COROLLA"}),
            mock.patch.object(c.vehicles, "list_vehicle_keys", return_value=[]),
        ):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error", "本 run 未見到的 model 必須讓 run 標 error")

    def test_closure_ok_when_all_seen(self):
        """F1b：所有 DB model 本 run 都見到 => 閉合檢查通過（正常標 success）。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
            mock.patch.object(c.brands, "list_model_names", return_value=["COROLLA"]),
            mock.patch.object(c.crawl, "scope_keys", return_value={"TOYOTA::COROLLA"}),
            mock.patch.object(c.vehicles, "list_vehicle_keys", return_value=[]),
        ):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "success")

    def test_closure_missing_vehicle_fails_closed(self):
        """沒有可驗證下架訊號時，未見歷史車型不得發布縮水 snapshot。"""
        c = self._crawler()
        with (
            mock.patch.object(c.crawl, "count_errors", return_value=0),
            mock.patch.object(c.brands, "list_brands", return_value=["TOYOTA"]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
            mock.patch.object(c.brands, "list_model_names", return_value=["COROLLA"]),
            mock.patch.object(c.crawl, "scope_keys", return_value={"TOYOTA::COROLLA"}),
            mock.patch.object(
                c.vehicles,
                "list_vehicle_keys",
                return_value=["TOYOTA::COROLLA::AGH30|ALPHARD"],
            ),
        ):
            c.run()
        status = c.crawl.finish_run.call_args[0][1]
        self.assertEqual(status, "error", "未確認的 vehicle 缺失必須 fail closed")

    def test_seen_called_before_skip(self):
        """F1b：已 done 跳過的 model 也必須 seen（否則閉合對帳誤報縮水）。"""
        c = self._crawler()
        c.crawl_brand = Crawler.crawl_brand.__get__(c)
        c._get = mock.MagicMock(return_value="<html>locate</html>")
        with (
            mock.patch("src.crawler.parse_brand_index", return_value=[{"name": "M1"}]),
            mock.patch.object(c.crawl, "is_done", return_value=True),
            mock.patch.object(c.crawl, "seen") as seen,
        ):
            c.crawl_brand("TOYOTA")
        seen.assert_any_call("model", "TOYOTA::M1")

    def test_run_crawl_module_exits_with_nonzero_on_error(self):
        """P1：`python -m src.run_crawl` 必須以 sys.exit(main()) 傳遞
        exit code —— 否則 partial run 以 0 結束，外部監控誤判成功。"""
        import runpy

        fake_db = mock.MagicMock()
        with (
            mock.patch("src.run_crawl.Database.connect", return_value=fake_db),
            mock.patch("fcntl.flock"),
            mock.patch("src.run_crawl.load_cookies", return_value=[]),
            mock.patch.object(SessionManager, "__init__", return_value=None),
            mock.patch("src.run_crawl.Crawler.run", return_value={}),
            mock.patch("src.run_crawl.Crawler.close"),
            mock.patch("src.run_crawl.logging.basicConfig"),
            mock.patch("sys.argv", ["run_crawl", "--no-browser", "--workers", "1"]),
        ):
            with self.assertRaises(SystemExit) as cm:
                runpy.run_module("src.run_crawl", run_name="__main__")
        self.assertEqual(cm.exception.code, 1, "error run 的 CLI 必須以 exit code 1 結束")

    def test_run_crawl_workers_default_from_config(self):
        """SOL review P3：直接執行 CLI 且未帶 --workers 時，預設值必須
        取自 CRAWL["workers"]（PSQ_WORKERS）而非固定 4。"""
        import runpy

        fake_db = mock.MagicMock()
        captured = {}

        def fake_init(self, http, db, workers=8, governor=None, fresh=False):
            self.last_status = "error"
            captured["workers"] = workers

        with (
            mock.patch("src.run_crawl.Database.connect", return_value=fake_db),
            mock.patch("fcntl.flock"),
            mock.patch("src.run_crawl.load_cookies", return_value=[]),
            mock.patch.object(SessionManager, "__init__", return_value=None),
            mock.patch.object(Crawler, "__init__", fake_init),
            mock.patch("src.run_crawl.Crawler.run", return_value={}),
            mock.patch("src.run_crawl.Crawler.close"),
            mock.patch("src.run_crawl.logging.basicConfig"),
            mock.patch.dict(CRAWL, {"workers": 2}),
            mock.patch("sys.argv", ["run_crawl", "--no-browser"]),
        ):
            with self.assertRaises(SystemExit):
                runpy.run_module("src.run_crawl", run_name="__main__")
        self.assertEqual(captured["workers"], 2, "CLI 預設 worker 數必須尊重 PSQ_WORKERS/CRAWL")


class TestSnapshotAndDbBoundaries(unittest.TestCase):
    def test_category_cid_rename_updates_existing_row(self):
        db = mock.MagicMock()
        existing = mock.MagicMock()
        existing.fetchone.return_value = {"id": 7}
        db._execute.side_effect = [existing, mock.MagicMock()]
        repo = VehicleRepository(db)
        self.assertEqual(repo.upsert_category(3, "NEW NAME", "2"), 7)
        self.assertIn("WHERE vehicle_id = %s AND cid = %s", db._execute.call_args_list[0].args[0])
        self.assertIn("UPDATE categories SET name", db._execute.call_args_list[1].args[0])

    def test_fresh_run_resets_logical_window(self):
        db = mock.MagicMock()
        cur = mock.MagicMock(lastrowid=46)
        db._execute.return_value = cur
        repo = CrawlRepository(db)
        self.assertEqual(repo.start_run("2026-08", fresh=True), 46)
        sql = db._execute.call_args.args[0]
        self.assertIn("started_at = NOW()", sql)
        self.assertIn("finished_at = NULL", sql)
        self.assertIn("status = 'running'", sql)

    def test_publish_rebuilds_independent_snapshot_for_run(self):
        db = mock.MagicMock()
        repo = CrawlRepository(db)
        repo.publish_success_parts(46)
        calls = db._execute.call_args_list
        self.assertEqual(calls[0].args[0], "DELETE FROM published_parts")
        self.assertIn("INSERT INTO published_parts", calls[1].args[0])
        self.assertIn("WHERE p.seen_run_id = %s", calls[1].args[0])
        self.assertEqual(calls[1].args[1], (46,))

    def test_group_receipt_reset_preserves_shrink_baseline(self):
        db = mock.MagicMock()
        repo = CrawlRepository(db)
        repo.reset_group_receipts("2026-08")
        sql, params = db._execute.call_args.args
        self.assertNotIn("fetched_row_count", sql)
        self.assertIn("WHERE fetched_run_key = %s", sql)
        self.assertEqual(params, ("2026-08",))

    def test_fresh_reset_deletes_every_scope_for_run(self):
        db = mock.MagicMock()
        repo = CrawlRepository(db)
        repo.reset_run_state("2026-08")
        sql, params = db._execute.call_args.args
        self.assertEqual(sql, "DELETE FROM crawl_state WHERE run_key = %s")
        self.assertEqual(params, ("2026-08",))
        self.assertNotIn("scope =", sql, "fresh 不得漏掉 part 或未來新增的 scope")

    def test_part_membership_is_explicit_run_id(self):
        db = mock.MagicMock()
        db._execute.return_value.fetchall.return_value = []
        repo = PartRepository(db)
        repo.upsert_parts(7, [{"part_number": "P1", "range_str": ""}], run_id=46)
        self.assertIn("seen_run_id = NULL", db._execute.call_args_list[0].args[0])
        sql, rows = db._executemany.call_args.args
        self.assertIn("seen_run_id", sql)
        self.assertEqual(rows[0][-1], 46)

    def test_fresh_reset_and_start_share_first_transaction(self):
        c = Crawler(mock.MagicMock(), mock.MagicMock(), workers=1, fresh=True)
        self.addCleanup(c.close)
        c._brands = mock.MagicMock(return_value=[{"name": "TOYOTA"}])
        c.crawl_brand = mock.MagicMock(return_value=0)
        with (
            mock.patch.dict(CRAWL, {"min_brands": 1}),
            mock.patch.object(c.crawl, "count_errors", return_value=1),
        ):
            c.run()
        method_calls = c.db.method_calls
        first_commit = next(i for i, call in enumerate(method_calls) if call[0] == "commit")
        sql_before_commit = " ".join(
            call.args[0]
            for call in method_calls[:first_commit]
            if call[0] == "_execute" and call.args
        )
        self.assertIn("INSERT INTO crawl_runs", sql_before_commit)
        self.assertIn("DELETE FROM crawl_state", sql_before_commit)
        self.assertIn("UPDATE groups_t SET fetched_run_key = NULL", sql_before_commit)
        self.assertIn("UPDATE parts SET seen_run_id = NULL", sql_before_commit)

    def test_migration_contains_snapshot_and_vehicle_rollout(self):
        sql = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "004_current_snapshot_and_vehicle_identity.sql"
        ).read_text()
        self.assertIn("ADD COLUMN identity_hash", sql)
        self.assertIn("uq_vehicle_identity", sql)
        identity_section = sql.split("UPDATE vehicles SET identity_hash", 1)[1].split(
            "SET @nullable", 1
        )[0]
        self.assertNotIn("COALESCE(ssd", identity_section)
        self.assertIn("COALESCE(options", identity_section)
        self.assertIn("COALESCE(grade", identity_section)
        self.assertIn("COALESCE(transmission", identity_section)
        self.assertIn("CREATE TABLE IF NOT EXISTS published_parts", sql)
        self.assertIn("ADD COLUMN seen_run_id", sql)
        self.assertIn("FROM v_parts", sql)
        self.assertIn("CREATE OR REPLACE VIEW v_parts", sql)
        self.assertIn("@seen_col_missing = 1", sql)
        self.assertIn("cr.status IS NULL", sql)
        self.assertIn("refusing empty snapshot", sql)

        v5_sql = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "005_vehicle_identity_v5_and_category_cid.sql"
        ).read_text()
        self.assertIn("COALESCE(body_style", v5_sql)
        self.assertNotIn("COALESCE(ssd", v5_sql)
        self.assertIn("duplicate vehicle v5 identity", v5_sql)
        self.assertIn("PARTSOUQ_ALLOW_V5_VEHICLE_REBUILD", v5_sql)
        self.assertIn("DELETE FROM vehicles", v5_sql)
        self.assertIn("uq_cat_cid", v5_sql)
        self.assertIn("DROP INDEX uq_cat", v5_sql)
        self.assertIn("ADD KEY idx_cat_name", v5_sql)
        self.assertIn("cr.status IS NULL", v5_sql)
        self.assertLess(
            v5_sql.index("UPDATE crawl_runs SET status = ''error''"),
            v5_sql.index("DELETE FROM vehicles"),
            "migration 必須先讓舊 success 失效，再刪 normalized vehicle tree",
        )
        self.assertLess(
            v5_sql.index("DELETE cs FROM crawl_state"),
            v5_sql.index("ALTER TABLE vehicles ADD UNIQUE KEY uq_vehicle_identity_v5 ("),
            "v5 completion marker must be created after state invalidation",
        )

    def test_run_crawl_lock_blocks_before_db_or_cookie_initialization(self):
        import src.run_crawl as run_crawl

        with tempfile.TemporaryDirectory() as tmp:
            with (
                mock.patch.object(run_crawl, "LOG_DIR", Path(tmp)),
                mock.patch.object(run_crawl.fcntl, "flock", side_effect=BlockingIOError),
                mock.patch.object(run_crawl.Database, "connect") as connect,
                mock.patch.object(run_crawl, "get_session") as get_session,
                mock.patch.object(run_crawl.logging, "basicConfig"),
                mock.patch("sys.argv", ["run_crawl"]),
            ):
                self.assertEqual(run_crawl.main(), 2)
        connect.assert_not_called()
        get_session.assert_not_called()

    def test_execute_connection_loss_is_not_masked_by_eager_reconnect(self):
        db = Database()
        conn = mock.MagicMock()
        db._local.conn = conn
        with mock.patch.object(
            db, "_new_conn", side_effect=pymysql.err.OperationalError(2003, "still down")
        ) as reconnect:
            with self.assertRaises(ConnectionLost):
                db._raise_connection_lost(2013)
        reconnect.assert_not_called()
        self.assertIsNone(getattr(db._local, "conn", None))

    def test_commit_connection_loss_is_not_masked_by_eager_reconnect(self):
        db = Database()
        conn = mock.MagicMock()
        conn.commit.side_effect = pymysql.err.OperationalError(2013, "lost")
        db._local.conn = conn
        with mock.patch.object(
            db, "_new_conn", side_effect=pymysql.err.OperationalError(2003, "still down")
        ) as reconnect:
            with self.assertRaises(ConnectionLost):
                db.commit()
        reconnect.assert_not_called()
        self.assertIsNone(getattr(db._local, "conn", None))


class TestDbConnectLazy(unittest.TestCase):
    """SOL review P3：connect() 不得建立閒置主連線。"""

    def test_connect_creates_no_idle_connection(self):
        from src.db import Database

        db = Database().connect()
        try:
            self.assertIsNone(db.conn, "connect() 不得預先建立 self.conn（閒置連線）")
        finally:
            db.close()


class TestCrawlerCmdlineMatch(unittest.TestCase):
    """P1：重複程序偵測必須匹配 macOS 真實的 Python 命令列。"""

    def test_capital_python_absolute_path(self):
        """真實環境：comm 截斷、argv 為大寫 Python 絕對路徑。"""
        args = (
            "/Library/Frameworks/Python.framework/Versions/3.14/Resources/"
            "Python.app/Contents/MacOS/Python -m src.run_crawl --workers 4"
        )
        self.assertIsNotNone(CRAWLER_CMDLINE_RE.search(args))

    def test_lowercase_python_module(self):
        """小寫 python3 -m src.run_crawl 仍然匹配。"""
        self.assertIsNotNone(CRAWLER_CMDLINE_RE.search("python3 -m src.run_crawl --workers 4"))

    def test_run_crawl_py_path(self):
        """python /path/to/src/run_crawl.py 匹配。"""
        self.assertIsNotNone(
            CRAWLER_CMDLINE_RE.search("python /Users/x/partsouq-crawler/src/run_crawl.py")
        )

    def test_supervisor_not_matched(self):
        """supervisor 自己的命令列不該被當 crawler。"""
        args = (
            "/Library/Frameworks/Python.framework/Versions/3.14/Resources/"
            "Python.app/Contents/MacOS/Python -m src.supervisor"
        )
        self.assertIsNone(CRAWLER_CMDLINE_RE.search(args))

    def test_shell_monitor_not_matched(self):
        """含 src.run_crawl 字串的監控 shell 不該被當 crawler。"""
        args = "zsh -c cd /x && pgrep -f 'src.run_crawl' && sleep 1"
        self.assertIsNone(CRAWLER_CMDLINE_RE.search(args))

    def test_shell_literal_crawler_command_not_matched(self):
        args = "zsh -c 'echo python3 -m src.run_crawl --workers 4; sleep 9'"
        self.assertIsNone(CRAWLER_CMDLINE_RE.search(args))

    def test_kill_other_detects_real_argv(self):
        """_kill_other_crawlers 用真實 argv 也能把孤兒 crawler 當作 stray。"""
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                cmd = args[0]
                if cmd == "ps":
                    # 單次 ps -eo pid=,ppid=,args=：pid ppid 完整命令列
                    return mock.MagicMock(
                        stdout=(
                            "4242 12345 /Library/Frameworks/Python.framework/Versions/3.14/"
                            "Resources/Python.app/Contents/MacOS/Python -m src.run_crawl "
                            "--workers 4\n"
                        )
                    )
                if cmd == "kill":
                    return mock.MagicMock(stdout="", returncode=1 if args[1] == "-0" else 0)
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            sup = Supervisor(workers=4)
            sup.proc = FakeProc(poll_result=None)
            sup.proc.pid = 99999
            sup._kill_other_crawlers()
            killed = [c for c in run.call_args_list if c.args and c.args[0][:2] == ["kill", "-9"]]
            self.assertEqual(len(killed), 1, "真實 argv 的孤兒 crawler 必須被清除")


class TestHeartbeatSingleBaseline(unittest.TestCase):
    """P1：心跳以單一基準 HANG_TIMEOUT，不再疊加寬限造成 40 分鐘。"""

    def setUp(self):
        self.sup = Supervisor(workers=4)
        self.sup.db = mock.MagicMock()
        self.sup.proc = FakeProc(poll_result=None)

    def test_fresh_write_not_stalled(self):
        """寫入新鮮 => 不卡死。"""
        self.sup.crawler_started_at = time.monotonic() - 30 * 60
        self.sup.db.query_one.return_value = {"last_write": time.time()}
        self.assertFalse(self.sup._progress_stalled())

    def test_stale_write_after_grace_is_stalled(self):
        """寫入停滯 + crawler 已存活超過 HANG_TIMEOUT => 卡死。"""
        self.sup.crawler_started_at = time.monotonic() - 30 * 60
        self.sup.db.query_one.return_value = {"last_write": time.time() - (HANG_TIMEOUT + 60)}
        self.assertTrue(self.sup._progress_stalled())

    def test_stale_write_recent_start_is_grace(self):
        """寫入停滯但 crawler 剛啟動 => 寬限期內不卡死（避免誤殺）。"""
        self.sup.crawler_started_at = time.monotonic() - 60
        self.sup.db.query_one.return_value = {"last_write": time.time() - (HANG_TIMEOUT + 60)}
        self.assertFalse(self.sup._progress_stalled())

    def test_empty_table_long_running_is_stalled(self):
        """空表 + 啟動超過 HANG_TIMEOUT 仍無任何寫入 => 卡死。"""
        self.sup.crawler_started_at = time.monotonic() - (HANG_TIMEOUT + 10)
        self.sup.db.query_one.return_value = {"last_write": None}
        self.assertTrue(self.sup._progress_stalled())


class TestCooldownEffective(unittest.TestCase):
    """P1：cooldown_until 必須真正阻擋冷卻期間的重啟。"""

    def test_restart_blocked_during_cooldown(self):
        sup = Supervisor(workers=4)
        now = time.monotonic()
        sup.cooldown_until = now + 30 * 60
        sup.restarts = [now - 5]  # 冷卻中不應新增
        sup.db = mock.MagicMock()
        sup.db.query_one.return_value = {"status": "running"}
        sup.proc = FakeProc(poll_result=1)
        with (
            mock.patch.object(sup, "start") as start,
            mock.patch.object(sup, "_kill_other_crawlers", return_value=True) as cleanup,
        ):
            sup._tick()
            start.assert_not_called()
            cleanup.assert_called_once()
            self.assertEqual(sup.restarts, [now - 5], "冷卻期間不得記錄新的重啟")

    def test_proc_none_respects_cooldown(self):
        """proc 為 None 的分支也必須尊重冷卻。"""
        sup = Supervisor(workers=4)
        now = time.monotonic()
        sup.cooldown_until = now + 600
        sup.db = mock.MagicMock()
        with mock.patch.object(sup, "start") as start:
            sup._tick()
            start.assert_not_called()


class TestWatchdogRecheck(unittest.TestCase):
    """P1：watchdog spawn supervisor 後必須重新確認存活。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.log_dir = Path(self.tmp.name)
        self._old_log = watchdog.LOG_DIR
        self._old_status = watchdog.STATUS_FILE
        self._old_spawn_wait = watchdog.SPAWN_WAIT_SECONDS
        self._old_recheck = watchdog.CRAWLER_RECHECK_SECONDS
        watchdog.LOG_DIR = self.log_dir
        watchdog.STATUS_FILE = self.log_dir / "watchdog_status.json"
        # 測試不必真的等 6+8 秒：把等待常數縮短為 0
        watchdog.SPAWN_WAIT_SECONDS = 0
        watchdog.CRAWLER_RECHECK_SECONDS = 0

    def tearDown(self):
        watchdog.LOG_DIR = self._old_log
        watchdog.STATUS_FILE = self._old_status
        watchdog.SPAWN_WAIT_SECONDS = self._old_spawn_wait
        watchdog.CRAWLER_RECHECK_SECONDS = self._old_recheck

    def test_spawn_that_immediately_exits_returns_1(self):
        """supervisor spawn 後立刻崩潰 => watchdog 必須回傳 1。"""
        with (
            mock.patch.object(watchdog, "_is_running", return_value=False),
            mock.patch.object(watchdog, "_mysql", return_value="1"),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value = mock.MagicMock(pid=123, poll=lambda: 0, returncode=1)
            rc = watchdog.main()
            self.assertEqual(rc, 1)

    def test_mysql_uses_portable_environment_settings(self):
        """watchdog 必須與 crawler 共用 PSQ_DB_*，不能寫死舊 Mac 路徑。"""
        completed = mock.MagicMock(returncode=0, stdout="1\n")
        settings = {
            "PSQ_MYSQL_BIN": "/custom/mysql",
            "PSQ_DB_HOST": "db.local",
            "PSQ_DB_PORT": "3306",
            "PSQ_DB_USER": "partsouq",
            "PSQ_DB_PASS": "secret",
            "PSQ_DB_NAME": "catalog",
        }
        with (
            mock.patch.dict(watchdog.os.environ, settings, clear=True),
            mock.patch.object(watchdog.subprocess, "run", return_value=completed) as run,
        ):
            self.assertEqual(watchdog._mysql(["-N", "-e", "SELECT 1"]), "1")

        cmd = run.call_args.args[0]
        self.assertEqual(
            cmd,
            [
                "/custom/mysql",
                "-h",
                "db.local",
                "-P",
                "3306",
                "-u",
                "partsouq",
                "catalog",
                "-N",
                "-e",
                "SELECT 1",
            ],
        )
        self.assertEqual(run.call_args.kwargs["env"]["MYSQL_PWD"], "secret")

    def test_spawn_success_returns_0(self):
        """supervisor spawn 成功且存活、crawler 被帶起 => watchdog 回傳 0。"""
        with (
            # supervisor 檢查(第 1 次)DOWN → spawn；crawler 檢查(第 2 次)UP
            mock.patch.object(watchdog, "_is_running", side_effect=[False, True]),
            mock.patch.object(watchdog, "_mysql", return_value="1"),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value = mock.MagicMock(pid=123, poll=lambda: None, returncode=None)
            rc = watchdog.main()
            self.assertEqual(rc, 0)

    def test_spawn_ok_but_crawler_down_returns_1(self):
        """spawn 成功、supervisor 存活，但 crawler 仍 DOWN => 回傳 1（P1）。"""
        with (
            # supervisor 檢查 DOWN → spawn(存活)；crawler 檢查(第 2 次)DOWN；
            # 緩衝重查(第 3 次)仍 DOWN
            mock.patch.object(watchdog, "_is_running", side_effect=[False, False, False]),
            mock.patch.object(watchdog, "_mysql", return_value="1"),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value = mock.MagicMock(pid=123, poll=lambda: None, returncode=None)
            rc = watchdog.main()
            self.assertEqual(rc, 1)

    def test_clean_exit_when_month_done_returns_0(self):
        """supervisor 乾淨退場（rc=0 且當月 run 已 success）=> 健康，回傳 0。"""
        with (
            mock.patch.object(watchdog, "_is_running", return_value=False),
            mock.patch.object(watchdog, "_mysql", return_value="1"),
            mock.patch.object(watchdog, "_month_crawl_done", return_value=True),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value = mock.MagicMock(pid=123, poll=lambda: 0, returncode=0)
            rc = watchdog.main()
            self.assertEqual(rc, 0)

    def test_clean_exit_but_month_not_done_returns_1(self):
        """supervisor 乾淨退場但當月 run 尚未 success => 異常，回傳 1。"""
        with (
            mock.patch.object(watchdog, "_is_running", return_value=False),
            mock.patch.object(watchdog, "_mysql", return_value="1"),
            mock.patch.object(watchdog, "_month_crawl_done", return_value=False),
            mock.patch("subprocess.Popen") as popen,
        ):
            popen.return_value = mock.MagicMock(pid=123, poll=lambda: 0, returncode=0)
            rc = watchdog.main()
            self.assertEqual(rc, 1)


class TestCloakCookieFailClosed(unittest.TestCase):
    """P2：cookie 匯出缺 cf_clearance 必須 fail closed。"""

    def setUp(self):
        # 把 cookie export 路徑隔離到 temp：預設是正式共用的
        # /tmp/psq_cloak_cookies.json，執行中的 crawler 刷新時會與測試
        # 互相覆寫（P2 修復）。
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_export = CLOAK["cookie_export_file"]
        CLOAK["cookie_export_file"] = Path(self._tmp.name) / "cookies.json"

    def tearDown(self):
        CLOAK["cookie_export_file"] = self._orig_export
        self._tmp.cleanup()

    def _write_export(self, names):
        path = CLOAK["cookie_export_file"]

        def fake_launch():
            path.write_text(
                json.dumps(
                    [
                        {"name": n, "value": "v", "domain": "partsouq.com", "path": "/"}
                        for n in names
                    ]
                )
            )
            return True

        return fake_launch

    def test_missing_cf_clearance_fails(self):
        with (
            mock.patch("src.cloak._launch_cloak", side_effect=self._write_export(["PHPSESSID"])),
            mock.patch("src.cloak._mark_refresh_failed") as fail,
            mock.patch("src.cloak._kill_browsers"),
            mock.patch("src.cloak.save_cookies") as save,
        ):
            out = cloak._refresh_impl()
            self.assertIsNone(out)
            fail.assert_called_once()
            save.assert_not_called()

    def test_with_cf_clearance_succeeds(self):
        with (
            mock.patch(
                "src.cloak._launch_cloak",
                side_effect=self._write_export(["PHPSESSID", "cf_clearance"]),
            ),
            mock.patch("src.cloak._mark_refresh_failed") as fail,
            mock.patch("src.cloak._kill_browsers"),
            mock.patch("src.cloak.save_cookies") as save,
        ):
            out = cloak._refresh_impl()
            self.assertIsNotNone(out)
            save.assert_called_once()
            fail.assert_not_called()


class TestNoBrowserMode(unittest.TestCase):
    """P2：--no-browser 模式不得啟動瀏覽器刷新。"""

    def setUp(self):
        self.m = SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
            ],
            no_browser=True,
        )

    def test_challenge_does_not_refresh(self):
        calls = {"n": 0}

        def chal_get(url, timeout=None):
            calls["n"] += 1
            return mock.MagicMock(status_code=403, text="Just a moment...", headers={})

        self.m.session.get = chal_get
        with mock.patch("src.http_client.force_refresh_session") as refresh:
            with self.assertRaises(ChallengeError):
                self.m.get("https://partsouq.com/x")
            refresh.assert_not_called()

    def test_ensure_fresh_skips_get_session(self):
        with mock.patch("src.http_client.get_session") as gs:
            self.m.ensure_fresh()
            gs.assert_not_called()

    def test_refresh_never_launches(self):
        """直接呼叫 refresh() 也必須遵守 no_browser（P2：不再只測 get）。"""
        m = SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
            ],
            no_browser=True,
        )
        with mock.patch("src.http_client.get_session") as gs:
            result = m.refresh()
            self.assertFalse(result, "no_browser 下 refresh 必須回傳 False")
            gs.assert_not_called()


class TestHttpF4EdgeCases(unittest.TestCase):
    """F4：HTTP 層的四個邊界修復（GPT5.6SOL review 驗證的 probes）。"""

    def _sm(self):
        return SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
            ],
            no_browser=False,
        )

    def _cookies(self, value="new"):
        return [{"name": "cf_clearance", "value": value, "domain": "partsouq.com", "path": "/"}]

    def test_retry_after_http_date(self):
        """F4：HTTP-date 格式的 Retry-After 必須被解析（不再固定退 65 秒）。"""
        r = mock.MagicMock()
        r.headers = {"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"}
        target = datetime(2026, 10, 21, 7, 28, tzinfo=UTC)
        expected = max(15.0, (target - datetime.now(UTC)).total_seconds())
        self.assertAlmostEqual(
            SessionManager._retry_after_seconds(r), min(expected, CRAWL["retry_after_cap"]), delta=5
        )

    def test_retry_after_past_http_date_has_floor(self):
        """F4：過去時間的 HTTP-date 也要有 15 秒下限。"""
        r = mock.MagicMock()
        r.headers = {"retry-after": "Wed, 21 Oct 2015 07:28:00 GMT"}
        self.assertEqual(SessionManager._retry_after_seconds(r), 15.0)

    def test_retry_after_capped(self):
        """F4：巨額 Retry-After 必須被 cap（999999 不能再睡 11 天）。"""
        r = mock.MagicMock()
        r.headers = {"retry-after": "999999"}
        self.assertEqual(SessionManager._retry_after_seconds(r), CRAWL["retry_after_cap"])

    def test_retry_after_missing_header_fallback(self):
        """F4：無 Retry-After 標頭 => 固定下限退避。"""
        r = mock.MagicMock()
        r.headers = {}
        self.assertEqual(SessionManager._retry_after_seconds(r), REFRESH_RETRY_BACKOFF + 5)

    def test_final_challenge_refresh_gets_followup(self):
        """F4：最後一次 HTTP attempt 的 challenge 刷新成功後必須有 follow-up
        請求 —— 舊碼 for range(5) 在最後一次 attempt 刷新成功後迴圈耗盡，
        新 cookie 從未被使用就直接拋 ChallengeError。"""
        m = self._sm()
        responses = [
            mock.MagicMock(status_code=403, text="Just a moment...", headers={}) for _ in range(5)
        ] + [mock.MagicMock(status_code=200, text="<html>OK</html>", headers={})]
        calls = {"n": 0}

        def fake_get(url, timeout=None):
            r = responses[calls["n"]]
            calls["n"] += 1
            return r

        m.session.get = fake_get
        old = CRAWL["max_refresh_per_request"]
        CRAWL["max_refresh_per_request"] = 10
        try:
            with (
                mock.patch("src.http_client.force_refresh_session") as refresh,
                mock.patch("src.http_client.time.sleep"),
            ):
                refresh.return_value = self._cookies()
                html = m.get("https://partsouq.com/x")
        finally:
            CRAWL["max_refresh_per_request"] = old
        self.assertEqual(html, "<html>OK</html>")
        self.assertEqual(calls["n"], 6, "最後一次刷新成功後必須有第 6 次 follow-up")
        self.assertEqual(refresh.call_count, 5)

    def test_refresh_success_cap_stops_endless_challenges(self):
        """F4：一直給 challenge 時，成功刷新超過上限就放棄（不無限刷新）。"""
        m = self._sm()

        def fake_get(url, timeout=None):
            return mock.MagicMock(status_code=403, text="Just a moment...", headers={})

        m.session.get = fake_get
        with (
            mock.patch("src.http_client.force_refresh_session") as refresh,
            mock.patch("src.http_client.time.sleep"),
        ):
            refresh.return_value = self._cookies()
            with self.assertRaises(ChallengeError):
                m.get("https://partsouq.com/x")
        # 第 1~3 次刷新成功不消耗 attempt；第 4 次成功時超過上限 break
        self.assertEqual(refresh.call_count, CRAWL["max_refresh_per_request"] + 1)

    def test_429_with_challenge_header_refreshes(self):
        """F4：429 + cf-mitigated: challenge 必須走驗證分支（刷新 cookie）
        —— 舊碼 429 檢查在前，被當一般限流，5 次請求 0 次刷新。"""
        m = self._sm()
        responses = [
            mock.MagicMock(status_code=429, text="", headers={"cf-mitigated": "challenge"}),
            mock.MagicMock(status_code=200, text="<html>OK</html>", headers={}),
        ]
        calls = {"n": 0}

        def fake_get(url, timeout=None):
            r = responses[calls["n"]]
            calls["n"] += 1
            return r

        m.session.get = fake_get
        with (
            mock.patch("src.http_client.force_refresh_session") as refresh,
            mock.patch("src.http_client.time.sleep"),
        ):
            refresh.return_value = self._cookies()
            html = m.get("https://partsouq.com/x")
        self.assertEqual(html, "<html>OK</html>")
        refresh.assert_called_once()
        self.assertEqual(calls["n"], 2, "429+challenge 應刷新後重試，而非 5 次硬碰限流")

    def test_plain_429_no_refresh(self):
        """F4：純 429（無 challenge 標頭）維持限流語意，不刷新 cookie。"""
        m = self._sm()

        def fake_get(url, timeout=None):
            return mock.MagicMock(status_code=429, text="", headers={"retry-after": "15"})

        m.session.get = fake_get
        with (
            mock.patch("src.http_client.force_refresh_session") as refresh,
            mock.patch("src.http_client.time.sleep") as sleep,
        ):
            with self.assertRaises(requests.RequestException):
                m.get("https://partsouq.com/x")
        refresh.assert_not_called()
        self.assertEqual(sleep.call_count, 5, "5 次嘗試都依 retry-after 退避")


class TestRequestGovernor(unittest.TestCase):
    """F5：全域 request governor（token bucket）的行為。"""

    def test_acquire_immediate_within_burst(self):
        """burst 內立即取得時槽，不阻塞。"""
        g = RequestGovernor(rate=0.1, burst=5)
        for _ in range(5):
            t0 = time.monotonic()
            g.acquire()
            self.assertLess(time.monotonic() - t0, 0.1)

    def test_throttle_blocks_until_expiry(self):
        """throttle 期間 acquire 必須阻塞。"""
        g = RequestGovernor(rate=10, burst=1)
        g.throttle(0.5)
        t0 = time.monotonic()
        g.acquire()
        self.assertGreaterEqual(time.monotonic() - t0, 0.4, "throttle 期間不得發請求")

    def test_slow_reduces_rate(self):
        """slow 後速率砍半（token 重生更慢，acquire 需等待更久）。"""
        g = RequestGovernor(rate=1, burst=1)
        g.acquire()  # 耗盡 token
        t0 = time.monotonic()
        g.acquire()
        normal = time.monotonic() - t0  # 1/s → 重生 1 token 約 1s
        g.acquire()  # 再耗盡
        g.slow(seconds=30)
        t0 = time.monotonic()
        g.acquire()
        slowed = time.monotonic() - t0  # slow 0.5/s → 約 2s
        self.assertGreater(slowed, normal * 1.5, "slow 後重生 1 token 必須明顯更慢")

    def test_throttle_not_blocked_by_waiting_worker(self):
        """SOL P1：等待 token 的 worker 不得卡住 throttle（等待時釋放鎖，
        否則 429 的全域暫停會延後到 waiter 取得 token 才生效）。"""
        g = RequestGovernor(rate=0.1, burst=1)
        g.acquire()  # 耗盡 token：下個 acquire 要等 10 秒
        entered = threading.Event()

        def waiter():
            entered.set()
            g.acquire()

        t = threading.Thread(target=waiter, daemon=True)
        t.start()
        entered.wait(timeout=2)  # 確認 waiter 已進入 acquire，再測 throttle
        time.sleep(0.05)
        t0 = time.monotonic()
        g.throttle(0.3)
        latency = time.monotonic() - t0
        self.assertLess(latency, 0.15, "throttle 必須即時生效，不得等 waiter 釋放鎖")

    def test_every_wire_request_acquires_governor(self):
        """SOL P1：每次 wire GET（含重試）前都必須取得全域時槽 ——
        拿一次 token 打 5 次請求等於沒有限流。"""
        gov = RequestGovernor(rate=1000, burst=100)
        m = SessionManager(cookies=[], no_browser=True, gov=gov)
        responses = iter(
            [
                mock.MagicMock(status_code=500, text="err", headers={}),
                mock.MagicMock(status_code=200, text="<html>ok</html>", headers={}),
            ]
        )
        m.session.get = mock.MagicMock(side_effect=lambda *a, **k: next(responses))
        acquired = {"n": 0}
        real_acquire = gov.acquire

        def counting():
            acquired["n"] += 1
            return real_acquire()

        with mock.patch.object(gov, "acquire", side_effect=counting):
            with mock.patch("src.http_client.time.sleep"):
                out = m.get("https://x")
        self.assertEqual(out, "<html>ok</html>")
        self.assertEqual(acquired["n"], 2, "500 重試 1 次 = 2 次 wire GET，每次都要 acquire")
        self.assertEqual(m.session.get.call_count, 2)

    def test_connection_error_resets_pool_http_error_does_not(self):
        """F5：只有連線層錯誤才重建連線池；500 等有 response 的不重建。"""
        # 連線錯誤 => reset 被呼叫
        m = SessionManager(cookies=[], no_browser=True)
        m.session.get = mock.MagicMock(side_effect=requests.exceptions.ConnectionError("down"))
        with (
            mock.patch("src.http_client.time.sleep"),
            mock.patch.object(m, "_reset_connections") as r,
        ):
            with self.assertRaises(requests.exceptions.ConnectionError):
                m.get("https://x")
        self.assertGreaterEqual(r.call_count, 1, "連線錯誤必須重建連線池")

        # 500 => reset 不被呼叫（keep-alive 健康）
        m2 = SessionManager(cookies=[], no_browser=True)
        m2.session.get = mock.MagicMock(
            return_value=mock.MagicMock(status_code=500, text="err", headers={})
        )
        with (
            mock.patch("src.http_client.time.sleep"),
            mock.patch.object(m2, "_reset_connections") as r2,
        ):
            with self.assertRaises(requests.RequestException):
                m2.get("https://x")
        r2.assert_not_called()

    def test_429_throttles_all_workers(self):
        """F5：429 時 governor.throttle 被呼叫（限流是全域的）。

        Retry-After 的地板是 15 秒，完整走完 5 次重試會卡 60 秒；
        這裡用短暫的 throttle 包裝驗證「傳進 throttle 的值」與
        「throttle 確實被呼叫」，等待行為由 governor 層的測試覆蓋。
        """
        gov = RequestGovernor(rate=10, burst=4)
        m = SessionManager(cookies=[], no_browser=True, gov=gov)
        responses = iter(
            [
                mock.MagicMock(status_code=429, text="", headers={"retry-after": "15"}),
                mock.MagicMock(status_code=200, text="<html>ok</html>", headers={}),
            ]
        )
        m.session.get = mock.MagicMock(side_effect=lambda *a, **k: next(responses))
        seen = {"secs": None}
        real_throttle = gov.throttle

        def short_throttle(seconds):
            seen["secs"] = seconds
            return real_throttle(min(seconds, 0.2))

        with (
            mock.patch.object(gov, "throttle", side_effect=short_throttle),
            mock.patch("src.http_client.time.sleep"),
        ):
            out = m.get("https://x")
        self.assertEqual(out, "<html>ok</html>")
        self.assertEqual(seen["secs"], 15.0, "Retry-After 15 必須傳進全域 throttle")
