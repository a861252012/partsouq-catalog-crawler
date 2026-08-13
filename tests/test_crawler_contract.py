"""Crawler 流程與設定的契約測試。

兩個目的：
  1. 驗證 CRAWL 設定的關鍵數值不被無意間改動（這些數值直接影響
     爬蟲會不會被卡死、被打爆、或誤判完成）。
  2. 用 AST 檢查 crawler 的「寫入-提交」結構，確保交易邊界正確
     （每個 group 寫完後要 commit，否則長交易會拖住 DB）。
"""

import ast
import unittest
from pathlib import Path
from unittest import mock

from src.config import CRAWL, LOG_DIR, SITE


class TestCrawlConfigContract(unittest.TestCase):
    """爬蟲運作參數的契約：改動前必須深思熟慮。"""

    def test_workers_reasonable(self):
        """並行度要在合理範圍（太小吃不滿頻寬，太大會被網站封鎖）。"""
        self.assertGreaterEqual(CRAWL["workers"], 1)
        self.assertLessEqual(CRAWL["workers"], 8)

    def test_http_timeout_not_zero(self):
        """HTTP 逾時不能是 0（會變成無限期等待）。"""
        self.assertGreater(CRAWL["http_timeout"], 0)

    def test_max_retries_bounded(self):
        """重試次數要有上限，不能無限重試。"""
        self.assertGreaterEqual(CRAWL["max_retries"], 1)
        self.assertLessEqual(CRAWL["max_retries"], 10)

    def test_limits_are_sane(self):
        """各層數量上限必須存在（0 代表不限制，None/缺失是 bug）。"""
        for key in ("limit_models", "limit_vehicles", "limit_groups"):
            self.assertIn(key, CRAWL, f"CRAWL 缺少 {key!r} 設定")
            self.assertIsInstance(CRAWL[key], int)
            self.assertGreaterEqual(CRAWL[key], 0)

    def test_site_urls_defined(self):
        """所有站內 URL 模板必須齊備（缺一會直接崩潰）。"""
        for key in ("base", "genuine", "locate", "pick", "vehicle", "unit"):
            self.assertIn(key, SITE)
            self.assertTrue(SITE[key].startswith("http"))

    def test_log_dir_is_absolute(self):
        """log 目錄必須是絕對路徑（supervisor/launchd 用）。"""
        self.assertTrue(LOG_DIR.is_absolute())

    def test_restart_window_covers_hang_cycles(self):
        """SOL review P1：重啟窗口必須**嚴格大於** 卡死門檻 × 門檻次數
        —— 固定每 HANG_TIMEOUT 卡死一次時，若窗口剛好等於週期 × 門檻，
        第 4 次重啟剛好把窗口邊界上的第 1 次排除（now - t == W 不滿足
        now - t < W），永遠累積不到冷卻。"""
        from src.supervisor import HANG_TIMEOUT as HANG
        from src.supervisor import RESTART_MAX, RESTART_WINDOW

        self.assertGreater(
            RESTART_WINDOW,
            HANG * RESTART_MAX,
            "重啟窗口必須嚴格大於卡死週期 × 門檻（否則固定週期卡死永遠累積不到冷卻）",
        )

    def test_row_count_shrink_ratio_sane(self):
        """SOL review P1：縮水偵測比例必須存在且在 (0, 1]。"""
        self.assertIn("row_count_shrink_ratio", CRAWL)
        ratio = CRAWL["row_count_shrink_ratio"]
        self.assertGreater(ratio, 0)
        self.assertLessEqual(ratio, 1)

    def test_capacity_inputs_are_positive(self):
        """容量估算的速率與期限都必須大於 0。"""
        for key in ("request_rate", "max_run_days"):
            self.assertIn(key, CRAWL, f"CRAWL 缺少 {key!r} 設定")
            self.assertIsInstance(CRAWL[key], (int, float))
            self.assertGreater(CRAWL[key], 0, f"CRAWL[{key!r}] 必須大於 0")


class TestCrawlerCapacityContract(unittest.TestCase):
    """第一個網路請求前，必須輸出已知工作的容量下限。"""

    def _crawler(self, remaining: int):
        from src.crawler import Crawler

        crawler = object.__new__(Crawler)
        crawler.crawl = mock.MagicMock()
        crawler.crawl.remaining_group_count.return_value = remaining
        return crawler

    def test_known_remaining_groups_over_budget_warn(self):
        crawler = self._crawler(43_205)
        with (
            mock.patch.dict(
                CRAWL,
                {"request_rate": 0.5, "request_burst": 4, "max_run_days": 1.0},
            ),
            self.assertLogs("crawler", level="WARNING") as logs,
        ):
            crawler._check_capacity("2026-08")
        self.assertIn("not feasible", " ".join(logs.output))
        crawler.crawl.remaining_group_count.assert_called_once_with("2026-08")

    def test_optimistic_budget_boundary_is_logged_without_warning(self):
        crawler = self._crawler(43_204)
        with (
            mock.patch.dict(
                CRAWL,
                {"request_rate": 0.5, "request_burst": 4, "max_run_days": 1.0},
            ),
            self.assertLogs("crawler", level="INFO") as logs,
        ):
            crawler._check_capacity("2026-08")
        self.assertNotIn("not feasible", " ".join(logs.output))
        crawler.crawl.remaining_group_count.assert_called_once_with("2026-08")

    def test_capacity_check_precedes_first_crawl_request(self):
        """run() 必須在 _brands() 第一次網路存取前檢查容量。"""
        tree = _crawler_tree()
        run = _find_method(tree, "Crawler", "run")
        self.assertIsNotNone(run)

        capacity_call = _single_call(run, "_check_capacity")
        brands_call = _single_call(run, "_brands")
        capacity_body, capacity_index = _containing_statement_list(run, capacity_call)
        brands_body, brands_index = _containing_statement_list(run, brands_call)

        self.assertIs(
            capacity_body,
            brands_body,
            "_check_capacity() 與 _brands() 必須在同一條無條件的執行路徑",
        )
        self.assertLess(
            capacity_index,
            brands_index,
            "_check_capacity() 必須在 _brands() 發出第一個請求前執行",
        )


class TestCrawlerTransactionBoundary(unittest.TestCase):
    """靜態檢查 crawler.py 的交易邊界。

    規則：在 crawl_group 的零件寫入（upsert_parts）之後必須 commit。
    若漏 commit，4 個 worker 的長交易會同時把交易保持開啟，時間一久
    連 ALTER/DDL 都卡死（今天才剛踩到）。
    """

    def test_parts_and_receipt_commit_in_successful_try_path(self):
        tree = _crawler_tree()
        crawl_group = _find_method(tree, "Crawler", "crawl_group")
        self.assertIsNotNone(crawl_group)

        upsert = _single_call(crawl_group, "upsert_parts")
        transaction_try = _enclosing_try(crawl_group, upsert)
        self.assertIsNotNone(transaction_try, "upsert_parts 必須在明確的 try 交易邊界內")

        expected = ("upsert_parts", "mark_group_fetched", "commit")
        positions = {}
        for name in expected:
            calls = _direct_calls(transaction_try.body, name)
            self.assertEqual(
                len(calls),
                1,
                f"交易 try 的成功路徑必須恰好一次直接呼叫 {name}()",
            )
            positions[name] = calls[0][0]

        self.assertLess(
            positions["upsert_parts"],
            positions["mark_group_fetched"],
            "必須先寫入零件，再寫 terminal receipt",
        )
        self.assertLess(
            positions["mark_group_fetched"],
            positions["commit"],
            "commit() 必須在零件與 receipt 都寫入後；前面或其他 branch 的 commit 不算",
        )

    def test_every_transaction_exception_path_rolls_back_before_exit(self):
        tree = _crawler_tree()
        crawl_group = _find_method(tree, "Crawler", "crawl_group")
        upsert = _single_call(crawl_group, "upsert_parts")
        transaction_try = _enclosing_try(crawl_group, upsert)

        self.assertTrue(transaction_try.handlers, "交易 try 必須處理失敗並 rollback")
        self.assertTrue(
            any(_catches_general_exception(handler) for handler in transaction_try.handlers),
            "交易需要 catch-all Exception handler，不可只處理 deadlock/斷線",
        )

        for handler in transaction_try.handlers:
            rollback_positions = _direct_calls(handler.body, "rollback")
            self.assertTrue(
                rollback_positions,
                f"{_handler_name(handler)} handler 必須在離開前 rollback()",
            )
            rollback_index = rollback_positions[0][0]
            exits = [
                index
                for index, statement in enumerate(handler.body)
                if any(
                    isinstance(node, (ast.Raise, ast.Continue, ast.Break, ast.Return))
                    for node in ast.walk(statement)
                )
            ]
            self.assertTrue(exits, f"{_handler_name(handler)} handler 不得靜默吞掉例外")
            self.assertLess(
                rollback_index,
                min(exits),
                f"{_handler_name(handler)} handler 必須先 rollback() 再 continue/raise",
            )


def _crawler_tree():
    src_path = Path(__file__).resolve().parent.parent / "src" / "crawler.py"
    return ast.parse(src_path.read_text())


def _find_method(tree, class_name, method_name):
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for child in node.body:
            if (
                isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                and child.name == method_name
            ):
                return child
    return None


def _call_name(node):
    if not isinstance(node, ast.Call):
        return None
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _single_call(container, name):
    calls = [node for node in ast.walk(container) if _call_name(node) == name]
    if len(calls) != 1:
        raise AssertionError(f"{container.name}() 必須恰好一次呼叫 {name}()，實際 {len(calls)} 次")
    return calls[0]


def _direct_calls(statements, name):
    """只找 statement list 的直接呼叫；不讓其他 if/try branch 蒙混過關。"""
    found = []
    for index, statement in enumerate(statements):
        if isinstance(
            statement,
            (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.With, ast.AsyncWith, ast.Match),
        ):
            continue
        for node in ast.walk(statement):
            if _call_name(node) == name:
                found.append((index, node))
    return found


def _enclosing_try(function, target):
    candidates = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Try)
        and any(_contains(statement, target) for statement in node.body)
    ]
    if not candidates:
        return None
    return min(candidates, key=_span)


def _containing_statement_list(function, target):
    candidate_lists = []
    for node in ast.walk(function):
        for field in ("body", "orelse", "finalbody"):
            statements = getattr(node, field, None)
            if not isinstance(statements, list):
                continue
            for index, statement in enumerate(statements):
                if _contains(statement, target):
                    candidate_lists.append((statements, index, _span(statement)))
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                for index, statement in enumerate(handler.body):
                    if _contains(statement, target):
                        candidate_lists.append((handler.body, index, _span(statement)))
    if not candidate_lists:
        raise AssertionError("找不到 call 所屬的 statement list")
    statements, index, _ = min(candidate_lists, key=lambda item: item[2])
    return statements, index


def _catches_general_exception(handler):
    if handler.type is None:
        return True
    names = [node.id for node in ast.walk(handler.type) if isinstance(node, ast.Name)]
    return "Exception" in names or "BaseException" in names


def _handler_name(handler):
    if handler.type is None:
        return "bare except"
    return ast.unparse(handler.type)


def _contains(container, node):
    for child in ast.walk(container):
        if child is node:
            return True
    return False


def _span(node):
    return (node.end_lineno - node.lineno, node.lineno)


if __name__ == "__main__":
    unittest.main(verbosity=2)
