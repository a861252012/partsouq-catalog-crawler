"""HTTP 工作階段層的穩定性測試。

涵蓋過去會讓全站爬取停擺的幾種故障情境：
  1. CLOSE_WAIT 過期 keep-alive socket 的復用（會卡到逾時）
  2. 請求錯誤後的連線池重建
  3. Single-flight 的 cookie 刷新（永遠只啟動一個瀏覽器）
  4. 刷新失敗後的退避（不會造成重試風暴）
"""

import time
import unittest
from unittest import mock

import requests

from src.config import CRAWL
from src.http_client import ChallengeError, SessionManager


class FakeResponse:
    """模擬 requests 的回應物件。"""

    def __init__(self, status_code=200, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class TestSessionResilience(unittest.TestCase):
    def setUp(self):
        self.m = SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
            ]
        )

    def test_closed_socket_raises_and_pool_resets(self):
        """伺服器關閉的 socket 必須以錯誤呈現，之後重建連線池。

        確保下次請求重新撥號，而不是復用半死的 keep-alive socket。
        """
        calls = {"n": 0}

        def flaky_get(url, timeout=None):
            calls["n"] += 1
            raise requests.ConnectionError("RemoteDisconnected: stale socket")

        self.m.session.get = flaky_get
        with (
            mock.patch.object(self.m, "_reset_connections") as reset,
            mock.patch("src.http_client.time.sleep"),
        ):
            with self.assertRaises(requests.ConnectionError):
                self.m.get("https://partsouq.com/x")
            reset.assert_called()

    def test_every_wire_request_uses_configured_timeout(self):
        """每次 wire request 都必須把有限 timeout 傳給 requests。

        舊測試的 fake 自己 sleep 5 秒，並不會實際模擬 requests
        的 timeout；這裡直接驗證每次 retry 的傳入值，也不用真實等待。
        """
        seen_timeouts = []

        def timeout_get(url, timeout=None):
            seen_timeouts.append(timeout)
            raise requests.Timeout("synthetic timeout")

        self.m.session.get = timeout_get
        with mock.patch("src.http_client.time.sleep"):
            with self.assertRaises(requests.Timeout):
                self.m.get("https://partsouq.com/x")

        self.assertEqual(len(seen_timeouts), CRAWL["max_retries"])
        self.assertEqual(seen_timeouts, [CRAWL["http_timeout"]] * CRAWL["max_retries"])

    def test_challenge_triggers_refresh_once(self):
        """驗證頁回應必須剛好觸發一次 session 刷新。"""
        with (
            mock.patch(
                "src.http_client.force_refresh_session",
                return_value=[
                    {
                        "name": "cf_clearance",
                        "value": "new",
                        "domain": "partsouq.com",
                        "path": "/",
                    },
                ],
            ) as refresh,
            mock.patch("src.http_client.time.sleep"),
        ):
            calls = {"n": 0}

            def chal_get(url, timeout=None):
                calls["n"] += 1
                if calls["n"] == 1:
                    return FakeResponse(403, "Just a moment...")
                return FakeResponse(200, "<html>ok</html>")

            self.m.session.get = chal_get
            out = self.m.get("https://partsouq.com/x")
            self.assertEqual(out, "<html>ok</html>")
            self.assertEqual(refresh.call_count, 1)

    def test_failed_refresh_waits_backoff(self):
        """刷新失敗必須進入退避等待，而不是立刻重試風暴。"""
        with mock.patch("src.http_client.force_refresh_session", return_value=None):
            calls = {"n": 0}

            def chal_get(url, timeout=None):
                calls["n"] += 1
                return FakeResponse(403, "Just a moment...")

            self.m.session.get = chal_get
            with mock.patch.object(self.m, "_sleep_with_backoff") as backoff:
                with self.assertRaises(ChallengeError):
                    self.m.get("https://partsouq.com/x")
                self.assertGreater(backoff.call_count, 0)

    def test_max_retries_preserved(self):
        """重試次數設定不得被改動。"""
        self.assertEqual(CRAWL["max_retries"], 5)

    def test_challenge_detected_via_header(self):
        """回應標頭 cf-mitigated 就足以判定驗證頁（不用看正文）。"""
        self.assertTrue(
            self.m._is_challenge(
                FakeResponse(200, "normal page", headers={"cf-mitigated": "challenge"}),
                "normal page",
            )
        )
        self.assertFalse(
            self.m._is_challenge(FakeResponse(200, "normal page", headers={}), "normal page")
        )
        self.assertTrue(
            self.m._is_challenge(
                FakeResponse(200, "Just a moment...", headers={}), "Just a moment..."
            )
        )

    def test_challenge_body_marker_mutation_and_scan_boundary(self):
        """合成變異 fixture：鎖定 marker 與前 8,000 字元的邊界。"""
        marker = "Turnstile"
        at_boundary = "x" * (8000 - len(marker)) + marker
        after_boundary = "x" * (8001 - len(marker)) + marker
        mutated = "Turnsti1e"

        response = FakeResponse(200, headers={})
        self.assertTrue(self.m._is_challenge(response, at_boundary))
        self.assertFalse(self.m._is_challenge(response, after_boundary))
        self.assertFalse(self.m._is_challenge(response, mutated))

    def test_refresh_replaces_cookies_entirely(self):
        """SOL review P2：刷新結果必須**整份**替換 cookie jar —— 新快照
        缺少舊 PHPSESSID 等 cookie 時舊值不得殘留（update 只覆寫新快照
        中存在的鍵，殘留的舊 session cookie 會讓請求帶上失效 session）。"""
        m = SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
                {"name": "PHPSESSID", "value": "old-sess", "domain": "partsouq.com", "path": "/"},
            ]
        )
        with mock.patch(
            "src.http_client.get_session",
            return_value=[
                {"name": "cf_clearance", "value": "new", "domain": "partsouq.com", "path": "/"},
            ],
        ):
            self.assertTrue(m.refresh())
        jar_names = {c.name for c in m.session.cookies}
        self.assertNotIn("PHPSESSID", jar_names, "新快照沒有的舊 cookie 不得殘留")
        self.assertIn("cf_clearance", jar_names)

    def test_delayed_challenge_passes_rejected_version(self):
        """SOL review P2：challenge 觸發的 force_refresh_session 必須帶上
        被拒的 cf_clearance 版本 —— cloak 才能在「全域 session 已更新」
        時直接沿用新 cookie，而不是清掉重刷、再啟動一次瀏覽器。"""
        m = SessionManager(
            cookies=[
                {"name": "cf_clearance", "value": "x", "domain": "partsouq.com", "path": "/"},
            ]
        )
        calls = {"n": 0}

        def chal_get(url, timeout=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeResponse(403, "Just a moment...")
            return FakeResponse(200, "<html>ok</html>")

        m.session.get = chal_get
        with (
            mock.patch("src.http_client.force_refresh_session") as refresh,
            mock.patch("src.http_client.time.sleep"),
        ):
            refresh.return_value = [
                {"name": "cf_clearance", "value": "y", "domain": "partsouq.com", "path": "/"}
            ]
            m.get("https://partsouq.com/x")
        refresh.assert_called_once_with("x")


class TestSingleFlightRefresh(unittest.TestCase):
    def test_concurrent_callers_only_one_browser(self):
        """N 個並行的 refresh_session() 呼叫必須只啟動一個瀏覽器。"""
        import threading

        from src import cloak

        launch_counts = {"n": 0}

        real_impl = cloak._refresh_impl

        def counting_impl():
            launch_counts["n"] += 1
            time.sleep(0.1)
            return [{"name": "cf_clearance", "value": "v", "domain": "d", "path": "/"}]

        # 清空快取狀態，讓所有呼叫者都真的嘗試刷新
        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = None
            cloak._session_state["ok_ts"] = 0.0
            cloak._session_state["retry_after"] = 0.0

        cloak._refresh_impl = counting_impl
        try:
            results = []
            threads = [
                threading.Thread(target=lambda: results.append(cloak.refresh_session()))
                for _ in range(8)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)
            self.assertTrue(all(r and len(r) == 1 for r in results))
            self.assertEqual(launch_counts["n"], 1)
        finally:
            cloak._refresh_impl = real_impl

    def test_ttl_reuses_cache(self):
        """TTL 內的成功刷新不得再次觸碰瀏覽器。"""
        from src import cloak

        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = [{"name": "cf_clearance", "value": "v"}]
            cloak._session_state["ok_ts"] = time.monotonic()
            cloak._session_state["retry_after"] = 0.0
        try:
            out = cloak.get_session()
            self.assertTrue(out)
        finally:
            with cloak._SESSION_COND:
                cloak._session_state["cookies"] = None
                cloak._session_state["ok_ts"] = 0.0
                cloak._session_state["retry_after"] = 0.0

    def test_force_refresh_preserves_backoff(self):
        """P1：force_refresh_session 不得清掉退避計數（連續失敗要遞增）。"""
        from src import cloak

        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = None
            cloak._session_state["ok_ts"] = 0.0
            cloak._session_state["failures"] = 3
            cloak._session_state["retry_after"] = time.monotonic() + 9999
            cloak._session_state["busy"] = False
        # force_refresh 清 cookie，但退避仍存在 → 回傳 None（不啟動瀏覽器）
        out = cloak.force_refresh_session()
        self.assertIsNone(out)
        with cloak._SESSION_COND:
            self.assertEqual(cloak._session_state["failures"], 3, "force_refresh 不得重置 failures")
            self.assertGreater(
                cloak._session_state["retry_after"],
                time.monotonic(),
                "force_refresh 不得清掉 retry_after",
            )

    def test_force_refresh_reuses_newer_global_session(self):
        """SOL review P2：持舊 cookie 的延遲 challenge 不得清掉已被其他
        worker 刷新的新 cookie —— 全域版本已更新時直接沿用、不啟動
        瀏覽器（舊碼無條件清 cookie，會把新 cookie 清掉再刷一次）。"""
        from src import cloak

        newer = [{"name": "cf_clearance", "value": "V2", "domain": "d", "path": "/"}]
        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = newer
            cloak._session_state["version"] = "V2"
            cloak._session_state["ok_ts"] = time.monotonic()
            cloak._session_state["retry_after"] = 0.0
            cloak._session_state["busy"] = False
        real_impl = cloak._refresh_impl
        cloak._refresh_impl = mock.MagicMock(return_value=None)
        try:
            out = cloak.force_refresh_session(rejected_version="V1")
            self.assertIs(out, newer, "全域版本較新時必須沿用，不重新刷新")
            cloak._refresh_impl.assert_not_called()
        finally:
            cloak._refresh_impl = real_impl
            with cloak._SESSION_COND:
                cloak._session_state["cookies"] = None
                cloak._session_state["version"] = None
                cloak._session_state["ok_ts"] = 0.0
                cloak._session_state["retry_after"] = 0.0

    def test_force_refresh_refreshes_when_version_matches(self):
        """SOL review P2：被拒版本 == 全域版本（沒有其他 worker 刷新）
        => 仍要清掉並重新刷新（這是正常的 challenge 處理路徑）。"""
        from src import cloak

        stale = [{"name": "cf_clearance", "value": "V1", "domain": "d", "path": "/"}]
        fresh = [{"name": "cf_clearance", "value": "V2", "domain": "d", "path": "/"}]
        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = stale
            cloak._session_state["version"] = "V1"
            cloak._session_state["ok_ts"] = time.monotonic()
            cloak._session_state["retry_after"] = 0.0
            cloak._session_state["busy"] = False
        real_impl = cloak._refresh_impl
        cloak._refresh_impl = mock.MagicMock(return_value=fresh)
        try:
            out = cloak.force_refresh_session(rejected_version="V1")
            cloak._refresh_impl.assert_called_once()
            self.assertEqual(out[0]["value"], "V2")
        finally:
            cloak._refresh_impl = real_impl
            with cloak._SESSION_COND:
                cloak._session_state["cookies"] = None
                cloak._session_state["version"] = None
                cloak._session_state["ok_ts"] = 0.0
                cloak._session_state["retry_after"] = 0.0

    def test_backoff_shared_across_callers(self):
        """P1：退避期間的 refresh_session 直接回傳 None，不輪流當 leader。"""
        import threading

        from src import cloak

        with cloak._SESSION_COND:
            cloak._session_state["cookies"] = None
            cloak._session_state["ok_ts"] = 0.0
            cloak._session_state["failures"] = 1
            cloak._session_state["retry_after"] = time.monotonic() + 30
            cloak._session_state["busy"] = False
        real_impl = cloak._refresh_impl
        cloak._refresh_impl = mock.MagicMock(return_value=None)
        try:
            # 4 個並行呼叫：退避期間全部都應直接拿到 None，不啟動瀏覽器
            results = []
            threads = [
                threading.Thread(target=lambda: results.append(cloak.refresh_session()))
                for _ in range(4)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)
            self.assertEqual(len(results), 4)
            self.assertTrue(all(r is None for r in results))
            cloak._refresh_impl.assert_not_called()
        finally:
            cloak._refresh_impl = real_impl
            with cloak._SESSION_COND:
                cloak._session_state["cookies"] = None
                cloak._session_state["ok_ts"] = 0.0
                cloak._session_state["retry_after"] = 0.0
                cloak._session_state["failures"] = 0


if __name__ == "__main__":
    unittest.main(verbosity=2)
