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


class TestCrawlerTransactionBoundary(unittest.TestCase):
    """靜態檢查 crawler.py 的交易邊界。

    規則：在 crawl_group 的零件寫入（upsert_parts）之後必須 commit。
    若漏 commit，4 個 worker 的長交易會同時把交易保持開啟，時間一久
    連 ALTER/DDL 都卡死（今天才剛踩到）。
    """

    def test_parts_upsert_followed_by_commit(self):
        src_path = Path(__file__).resolve().parent.parent / "src" / "crawler.py"
        tree = ast.parse(src_path.read_text())

        # 找 upsert_parts 的呼叫，檢查同一 function body 內後面有 commit()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else fn.id
            if name != "upsert_parts":
                continue
            # 找包含此 node 的 FunctionDef，並要求其 body 含 commit()
            func = _find_func(tree, node)
            self.assertIsNotNone(func, "upsert_parts 呼叫不在任何函式內")
            # 該函式的 body 必須包含 commit()
            has_commit = any(
                isinstance(stmt, ast.Call)
                and (stmt.func.attr if isinstance(stmt.func, ast.Attribute) else stmt.func.id)
                == "commit"
                for stmt in ast.walk(func)
            )
            self.assertTrue(
                has_commit,
                f"upsert_parts 所在函式 {func.name} 缺少 commit()（長交易會拖死 DB）",
            )


def _find_func(tree, node):
    """找包含 node 的最內層 FunctionDef（BFS，取最小的）。"""
    found = None
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if _contains(func, node):
            if found is None or _range(func) <= _range(found):
                found = func
    return found


def _contains(container, node):
    for child in ast.walk(container):
        if child is node:
            return True
    return False


def _range(node):
    return (node.lineno, node.end_lineno)


if __name__ == "__main__":
    unittest.main(verbosity=2)
