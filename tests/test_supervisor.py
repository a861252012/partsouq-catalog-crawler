"""監督迴圈的測試（自癒機制的不變式驗證）。

確保無人值守爬蟲的關鍵行為都正確：
  1. 爬蟲子程序崩潰 -> 重啟
  2. 爬蟲卡住（長時間沒有寫入資料庫）-> 重啟
  3. 重啟風暴 -> 進入冷卻（不會無限重啟狂打網站）
  4. 爬取完成 -> 乾淨退出
"""

import signal
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import src.supervisor as supervisor_module
from src.supervisor import HANG_TIMEOUT, RESTART_WINDOW, Supervisor


class FakeProc:
    """模擬子程序：可設定 poll 結果與記錄 kill/terminate 呼叫。"""

    def __init__(self, poll_result=None):
        self._poll = poll_result
        self.killed = False
        self.terminated = False
        self.pid = 4242

    def poll(self):
        return self._poll

    def kill(self):
        self.killed = True
        self._poll = 9

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return None


class TestSupervisorLoop(unittest.TestCase):
    def setUp(self):
        self.sup = Supervisor(workers=4)
        # 隔離 _write_summary：測試不得覆寫真實的 logs/summary.json
        # （reviewer 發現 test_done_run_terminates_cleanly 會污染監控證據）
        patcher = mock.patch.object(self.sup, "_write_summary", return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_crashed_child_is_restarted(self):
        """子程序死掉且沒有完成紀錄時，必須重新啟動。"""
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.return_value = {"status": "running"}
        self.sup.proc = FakeProc(poll_result=1)  # 已崩潰（rc=1）
        with mock.patch.object(self.sup, "start") as start:
            self.sup._tick()
            start.assert_called_once()

    def test_hung_crawler_restarted(self):
        """超過 HANG_TIMEOUT 沒有寫入資料庫 => 卡死 => 重啟。"""
        self.sup.db = mock.MagicMock()
        old = time.time() - (20 * 60 + 5)
        # 第 1 次呼叫：心跳查詢回傳過期時間；第 2 次：run 狀態 -> 執行中
        self.sup.db.query_one.side_effect = [{"last_write": old}, {"status": "running"}]
        self.sup.proc = FakeProc(poll_result=None)  # 程序活著但卡住
        with mock.patch.object(self.sup, "restart") as restart:
            self.sup._tick()
            restart.assert_called_once()

    def test_live_crawler_not_touched(self):
        """健康子程序 + 新鮮心跳時，不得重啟。"""
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.side_effect = [{"last_write": time.time()}, {"status": "running"}]
        self.sup.proc = FakeProc(poll_result=None)
        with mock.patch.object(self.sup, "restart") as restart:
            self.sup._tick()
            restart.assert_not_called()

    def test_restart_storm_enters_cooldown(self):
        """窗口內重啟超過 RESTART_MAX 次 => 進入冷卻，不再重啟。"""
        now = time.monotonic()
        self.sup.restarts = [now - 10, now - 20, now - 30]  # 窗口內已有 3 次
        self.sup.db = mock.MagicMock()
        self.sup.proc = FakeProc(poll_result=1)
        with mock.patch.object(self.sup, "start") as start:
            self.sup._tick()  # 這是第 4 次重啟嘗試
            self.assertGreater(self.sup.cooldown_until, now)
            start.assert_not_called()  # 冷卻中：不啟動新程序

    def test_periodic_restarts_still_trigger_cooldown(self):
        """SOL review P1：固定每 HANG_TIMEOUT 卡死一次的週期性重啟也
        必須累積到門檻並進入冷卻。

        reviewer probe：第 4 次重啟剛好落在窗口邊界（now - t == W）時，
        舊碼用 `now - t < W` 過濾把第 1 次排除、且先檢查才加入本次事件
        —— 永遠只有 3 筆、永不進 cooldown。修復：窗口嚴格大於
        週期 × 門檻（+2×CHECK_INTERVAL 餘量），並把本次事件納入後
        再判斷。
        """
        now = time.monotonic()
        p = HANG_TIMEOUT  # 20 分鐘卡死週期
        # 3 次週期性重啟（各間隔一個卡死週期）+ 現在的第 4 次嘗試
        self.sup.restarts = [now - 3 * p, now - 2 * p, now - p]
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.return_value = {"status": "running"}
        self.sup.proc = FakeProc(poll_result=None)  # 存活但卡死的 child
        with mock.patch.object(self.sup, "start") as start:
            self.sup._tick()
        self.assertGreater(self.sup.cooldown_until, now, "週期性重啟也必須在 4 次內累積到冷卻")
        start.assert_not_called()

    def test_cooldown_kills_hung_child_before_sleeping(self):
        """SOL review P1：觸發冷卻時必須先終止「仍存活但卡死」的 child
        —— 舊碼在風暴分支直接 return，卡死的 crawler 會在整段 30 分鐘
        冷卻期間繼續存在、持續打網站。

        reviewer probe：cooldown=True 但 kill_calls=0。這裡直接呼叫
        restart()（等同心跳檢查觸發的風暴路徑），斷言 child 被終止。
        """
        now = time.monotonic()
        self.sup.restarts = [now - 10, now - 20, now - 30]
        self.sup.proc = FakeProc(poll_result=None)  # 存活（會卡死）
        with mock.patch.object(self.sup, "_kill_current") as kill:
            self.sup.restart("hung crawler")
            kill.assert_called_once()  # 進冷卻前必須先終止故障 child
        self.assertGreater(self.sup.cooldown_until, now)

    def test_cooldown_expires_after_window(self):
        """冷卻時間過後，重啟機制恢復運作。"""
        now = time.monotonic()
        self.sup.cooldown_until = now - 1  # 冷卻已結束
        self.sup.restarts = []
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.return_value = {"status": "running"}
        self.sup.proc = FakeProc(poll_result=1)
        with mock.patch.object(self.sup, "start") as start:
            self.sup._tick()
            start.assert_called_once()

    def test_cooldown_retries_unconfirmed_child_kill(self):
        """首次 kill=False 後，cooldown 不得阻止後續 tick 再回收 child。"""
        self.sup.cooldown_until = time.monotonic() + 60
        self.sup.proc = FakeProc(poll_result=None)
        with mock.patch.object(self.sup, "_kill_current", return_value=False) as kill:
            self.sup.restart("still hung")
        kill.assert_called_once_with("still hung")

    def test_cooldown_tick_kills_retained_live_child(self):
        """cooldown 已存在時，健康 poll 不得讓 retained child 繼續跑。"""
        self.sup.cooldown_until = time.monotonic() + 60
        self.sup.proc = FakeProc(poll_result=None)
        with mock.patch.object(self.sup, "_kill_current", return_value=True) as kill:
            self.sup._tick()
        kill.assert_called_once_with("cooldown active")

    def test_start_refuses_when_stray_kill_is_unconfirmed(self):
        with (
            mock.patch.object(self.sup, "_kill_other_crawlers", return_value=False),
            mock.patch("src.supervisor.subprocess.Popen") as popen,
        ):
            self.assertFalse(self.sup.start())
        popen.assert_not_called()

    def test_tick_stops_owned_child_when_stray_is_confirmed_unresolved(self):
        self.sup.proc = FakeProc(poll_result=None)
        with (
            mock.patch.object(self.sup, "_kill_other_crawlers", return_value=False),
            mock.patch.object(self.sup, "_kill_current", return_value=True) as kill,
        ):
            self.sup._tick_inner()
        kill.assert_called_once_with("unresolved stray crawler")

    def test_tick_keeps_owned_child_when_process_scan_is_inconclusive(self):
        self.sup.proc = FakeProc(poll_result=None)
        with (
            mock.patch.object(self.sup, "_kill_other_crawlers", return_value=None),
            mock.patch.object(self.sup, "_kill_current") as kill,
            mock.patch.object(self.sup, "_progress_stalled", return_value=False),
            mock.patch.object(self.sup, "_memory_over_limit", return_value=False),
            mock.patch.object(self.sup, "_disk_low", return_value=False),
            mock.patch.object(self.sup, "_db_alive", return_value=True),
            mock.patch.object(self.sup, "_cookie_fresh", return_value=True),
            mock.patch.object(self.sup, "_crawl_done", return_value=False),
        ):
            self.sup._tick_inner()
        kill.assert_not_called()

    def test_stray_exiting_between_ps_and_kill_is_clean(self):
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                if args[:3] == ["ps", "-eo", "pid=,ppid=,args="]:
                    return mock.MagicMock(
                        stdout="4242 1 python3 -m src.run_crawl --workers 2\n",
                        returncode=0,
                    )
                if args[:2] == ["kill", "-9"]:
                    return mock.MagicMock(returncode=1)
                if args[:3] == ["ps", "-o", "args="]:
                    return mock.MagicMock(stdout="", returncode=1)
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            self.sup.proc = FakeProc(poll_result=None)
            self.sup.proc.pid = 99999
            self.assertTrue(self.sup._kill_other_crawlers())

    def test_stray_kill_permission_error_is_not_clean(self):
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                if args[:3] == ["ps", "-eo", "pid=,ppid=,args="]:
                    return mock.MagicMock(
                        stdout="4242 1 python3 -m src.run_crawl --workers 2\n",
                        returncode=0,
                    )
                if args[:2] == ["kill", "-9"]:
                    return mock.MagicMock(returncode=1, stderr=b"Operation not permitted")
                if args[:3] == ["ps", "-o", "args="]:
                    return mock.MagicMock(
                        stdout="python3 -m src.run_crawl --workers 2\n",
                        returncode=0,
                    )
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            self.sup.proc = FakeProc(poll_result=None)
            self.sup.proc.pid = 99999
            self.assertFalse(self.sup._kill_other_crawlers())

    def test_done_run_terminates_cleanly(self):
        """爬取成功完成時，必須乾淨地停止迴圈（不得重啟）。"""
        self.sup.db = mock.MagicMock()

        def fake_query(sql, params=None):
            if "MAX(updated_at)" in sql or "last_write" in sql:
                return {"last_write": time.time()}  # 不卡死
            if sql == "SELECT 1 AS x":
                return {"x": 1}  # DB 健康
            return {"status": "success"}  # run-status：完成

        self.sup.db.query_one.side_effect = fake_query
        self.sup.proc = FakeProc(poll_result=None)
        with (
            mock.patch.object(self.sup, "restart") as restart,
            mock.patch("src.supervisor.sys.exit") as exit_mock,
        ):
            self.sup._tick()
            restart.assert_not_called()
            exit_mock.assert_called_once_with(0)

    def test_done_run_does_not_exit_until_child_is_confirmed_dead(self):
        self.sup.db = mock.MagicMock()
        self.sup.proc = FakeProc(poll_result=None)
        with (
            mock.patch.object(self.sup, "_progress_stalled", return_value=False),
            mock.patch.object(self.sup, "_memory_over_limit", return_value=False),
            mock.patch.object(self.sup, "_disk_low", return_value=False),
            mock.patch.object(self.sup, "_db_alive", return_value=True),
            mock.patch.object(self.sup, "_cookie_fresh", return_value=True),
            mock.patch.object(self.sup, "_crawl_done", return_value=True),
            mock.patch.object(self.sup, "_kill_current", return_value=False),
            mock.patch("src.supervisor.sys.exit") as exit_mock,
        ):
            self.sup._tick()
        exit_mock.assert_not_called()

    def test_window_counters_reset(self):
        """窗口外的舊重啟紀錄不計入上限。"""
        now = time.monotonic()
        old = now - (RESTART_WINDOW + 10)
        self.sup.restarts = [old, old, old]  # 全部已過期
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.return_value = {"status": "running"}
        self.sup.proc = FakeProc(poll_result=1)
        with mock.patch.object(self.sup, "start") as start:
            self.sup._tick()
            start.assert_called_once()  # 窗口重置，允許重啟

    def test_run_finally_retries_child_cleanup_and_closes_db(self):
        fake_db = mock.MagicMock()
        self.sup.proc = FakeProc(poll_result=None)
        attempts = 0

        def kill(_reason):
            nonlocal attempts
            attempts += 1
            if attempts == 3:
                self.sup.proc = None
                return True
            return False

        with (
            mock.patch("src.supervisor.Database.connect", return_value=fake_db),
            mock.patch.object(self.sup, "_cleanup_stale_runs"),
            mock.patch.object(self.sup, "_crawl_done", return_value=False),
            mock.patch.object(self.sup, "start", side_effect=RuntimeError("boom")),
            mock.patch.object(self.sup, "_kill_current", side_effect=kill) as kill_current,
            mock.patch("src.supervisor.time.sleep"),
            self.assertRaisesRegex(RuntimeError, "boom"),
        ):
            self.sup.run()
        self.assertEqual(kill_current.call_count, 3)
        fake_db.close.assert_called_once()

    def test_main_term_handler_ignores_followup_term_before_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            with (
                mock.patch.object(supervisor_module, "LOG_DIR", Path(tmp)),
                mock.patch.object(supervisor_module.logging, "basicConfig"),
                mock.patch.object(supervisor_module.signal, "signal") as install,
                mock.patch.object(supervisor_module.Supervisor, "run", return_value=0),
                mock.patch("sys.argv", ["supervisor"]),
            ):
                self.assertEqual(supervisor_module.main(), 0)
                handler = install.call_args_list[-1].args[1]
                with self.assertRaises(SystemExit) as stopped:
                    handler(signal.SIGTERM, None)
        self.assertEqual(stopped.exception.code, 128 + signal.SIGTERM)
        install.assert_any_call(signal.SIGTERM, signal.SIG_IGN)

    def test_progress_query_sql(self):
        """心跳查詢必須鎖定 parts 表的 updated_at。"""
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.side_effect = [{"last_write": time.time()}, {"status": "running"}]
        self.sup.proc = FakeProc(poll_result=None)
        self.sup._tick()
        sql = self.sup.db.query_one.call_args_list[0][0][0]
        self.assertIn("updated_at", sql)
        self.assertIn("parts", sql)

    def test_memory_over_limit_restarts(self):
        """RSS 無上限成長（記憶體洩漏）必須觸發重啟。"""
        self.sup.db = mock.MagicMock()
        self.sup.db.query_one.side_effect = [{"last_write": time.time()}, {"status": "running"}]
        self.sup.proc = FakeProc(poll_result=None)
        with (
            mock.patch("src.supervisor.subprocess.run") as ps,
            mock.patch.object(self.sup, "restart") as restart,
        ):
            ps.return_value = mock.MagicMock(stdout="3000000\n")  # 約 3GB RSS
            self.sup._tick()
            restart.assert_called_once()

    def test_own_crawler_not_killed_as_stray(self):
        """自己的爬蟲（pid 是 self.proc.pid）絕不能被當成 stray 誤殺。

        這是修復「supervisor 每 5 分鐘殺掉自己 crawler」的迴歸測試：
        mine = self.proc.pid 會被排除，即使命令列匹配 crawler 特徵。
        """
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                cmd = args[0]
                if cmd == "ps":
                    # 單次 ps -eo pid=,ppid=,args=：pid ppid 完整命令列
                    return mock.MagicMock(
                        stdout="4242 12345 python3 -m src.run_crawl --workers 4\n"
                    )
                if cmd == "kill":
                    return mock.MagicMock(stdout="", returncode=1 if args[1] == "-0" else 0)
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            self.sup.proc = FakeProc(poll_result=None)
            self.sup.proc.pid = 4242  # 4242 是自己的 crawler
            self.sup._kill_other_crawlers()
            killed = [c for c in run.call_args_list if c.args and c.args[0][:2] == ["kill", "-9"]]
            self.assertEqual(killed, [], "自己的 crawler 不該被當 stray 殺掉")

    def test_orphan_crawler_still_killed(self):
        """非本程序 spawn 的 crawler（pid 不是 self.proc.pid）仍必須清除。"""
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                cmd = args[0]
                if cmd == "ps":
                    return mock.MagicMock(
                        stdout="4242 12345 python3 -m src.run_crawl --workers 4\n"
                    )
                if cmd == "kill":
                    return mock.MagicMock(stdout="", returncode=1 if args[1] == "-0" else 0)
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            self.sup.proc = FakeProc(poll_result=None)
            self.sup.proc.pid = 99999  # 不是 4242，4242 是「別人」
            self.sup._kill_other_crawlers()
            killed = [c for c in run.call_args_list if c.args and c.args[0][:2] == ["kill", "-9"]]
            self.assertEqual(len(killed), 1, "孤兒 crawler 必須被清除")

    def test_shell_command_not_killed(self):
        """命令列含 'src.run_crawl' 字串的無關 shell 不得被誤殺。

        這是「pgrep -f 誤殺監控 shell」的迴歸測試：新邏輯直接比對
        完整命令列，監控命令（含該字串但非 crawler 入口）必須被忽略。
        """
        with mock.patch("src.supervisor.subprocess.run") as run:

            def fake_run(args, **kw):
                cmd = args[0]
                if cmd == "ps":
                    return mock.MagicMock(
                        stdout="4242 12345 zsh -c cd /x && pgrep -f 'src.run_crawl' && sleep 1\n"
                    )
                if cmd == "kill":
                    return mock.MagicMock(stdout="", returncode=0)
                return mock.MagicMock(stdout="", returncode=0)

            run.side_effect = fake_run
            self.sup.proc = FakeProc(poll_result=None)
            self.sup.proc.pid = 99999
            self.sup._kill_other_crawlers()
            killed = [c for c in run.call_args_list if c.args and c.args[0][0] == "kill"]
            self.assertEqual(killed, [], "含 src.run_crawl 字串的 shell 命令不該被殺")


class TestSingleInstanceLock(unittest.TestCase):
    """P1：main() 的 flock 單實例鎖 —— 第二個 supervisor 直接退場，
    不得與第一個爭奪爬蟲（互殺重啟死循環）。"""

    def test_second_instance_exits_without_running(self):
        import tempfile
        import threading
        from pathlib import Path

        import src.supervisor as sup

        lock_dir = Path(tempfile.mkdtemp())
        entered = threading.Event()
        original_main = sup.main

        def fake_run(self):
            entered.set()
            time.sleep(2)

        with (
            mock.patch.object(sup, "LOG_DIR", lock_dir),
            mock.patch.object(sup.Supervisor, "run", new=fake_run),
            mock.patch.object(sup.logging, "basicConfig"),
            mock.patch("sys.argv", ["supervisor", "--workers", "2"]),
        ):
            holder = threading.Thread(target=original_main)
            holder.start()
            self.assertTrue(entered.wait(5), "第一個實例必須取得鎖並進入 run")
            rc2 = original_main()  # 第二個實例：鎖被持有
            holder.join(10)
        self.assertEqual(rc2, 0, "第二個 supervisor 必須乾淨退場")
        self.assertFalse(holder.is_alive(), "第一個實例必須正常結束")


if __name__ == "__main__":
    unittest.main(verbosity=2)
