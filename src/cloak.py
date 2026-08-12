"""CloakBrowser 工作階段管理員（基礎設施層）。

啟動隱匿版 Chromium（CloakBrowser），導向 partsouq.com 讓 Cloudflare
的 Turnstile 驗證自動通過，然後匯出 session 的 cookie
（cf_clearance + PHPSESSID）供高速 HTTP 爬蟲使用。

全程無人介入：碰到 Cloudflare 驗證時，瀏覽器本身會自動解決 ——
拜 CloakBrowser 對指紋的原始碼層修補所賜。

執行緒安全：所有刷新都走 single-flight 鎖，因此 N 個並行 worker
同時呼叫時，只會啟動一個 CloakBrowser；其餘 worker 等待結果並沿用
（見 get_session / refresh_session）。
"""

# ruff: noqa: UP031  -- 底下 `%` 格式是刻意保留：內嵌子程序腳本含有大量
# `{}`（dict literal），改用 .format()/f-string 會與腳本內容衝突。
import json
import logging
import subprocess
import threading
import time
import urllib.parse
import urllib.request

from .config import CLOAK, SITE, save_cookies

log = logging.getLogger("cloak")

# CDP HTTP 端點：開新分頁用
HTTP_GET = "http://{host}/json/new?{url}"
# 驗證頁檢查間隔與逾時
TURNSTILE_CHECK_INTERVAL = 3.0
CHALLENGE_TIMEOUT = 90.0

# ---------------------------------------------------------------------------
# 全域 single-flight 的 session 狀態
# ---------------------------------------------------------------------------
_SESSION_LOCK = threading.Lock()
_SESSION_COND = threading.Condition(_SESSION_LOCK)
_session_state = {
    "cookies": None,  # 最近一次成功匯出的 cookie
    "ok_ts": 0.0,  # 上次成功的單調時鐘（monotonic）時間
    "busy": False,  # 目前是否正在刷新（single-flight 旗標）
    "retry_after": 0.0,  # 在此單調時鐘時間之前不得重試（退避）
    "failures": 0,  # 連續失敗次數（退避指數成長用）
    "version": None,  # 目前 cookie 的 cf_clearance 值（session 版本訊號，SOL review P2）
}
# Cookie 的有效期限：每 25 分鐘主動刷新一次
COOKIE_TTL = 25 * 60.0
# 刷新失敗後的退避基礎：連續失敗會指數成長（60s → 120s → 240s ...）
REFRESH_RETRY_BACKOFF = 60.0
# 退避上限：避免長時間封鎖時越等越久
REFRESH_BACKOFF_MAX = 20 * 60.0


def _cf_value(cookies) -> str:
    """從 cookie 列表取出 cf_clearance 的值（無則回傳空字串）。

    cf_clearance 每次刷新必然改變，是可靠的 session 版本訊號
    （與 http_client 的 _cf_value 同義；這裡各自定義避免循環 import）。
    """
    for c in cookies or []:
        if c.get("name") == "cf_clearance":
            return c.get("value", "")
    return ""


def _kill_browsers():
    """清除任何殘留的 CloakBrowser/Chromium 程序（最多嘗試 3 輪）。"""
    for _ in range(3):
        for pat in ("remote-debugging-port=9242", "cloakbrowser.launch_async"):
            # 診斷探針：pkill 前記錄它會匹配到哪些 pid（過去發生過
            # crawler 在 refresh deadline 被 SIGKILL 的謎團，需要確認
            # pkill 是否誤匹配到 crawler 本身）。
            try:
                probe = subprocess.run(
                    ["pgrep", "-fl", pat], capture_output=True, text=True, timeout=5
                )
                log.info(
                    "kill_browsers probe %r matched: %s",
                    pat,
                    probe.stdout.strip()[:400] or "(none)",
                )
            except Exception as e:
                log.warning("kill_browsers probe failed: %s", e)
            subprocess.run(["pkill", "-f", pat], capture_output=True)
        time.sleep(1)
        if not _cdp_alive() and not _any_browser_process():
            return
    log.warning("browser processes did not die after 3 kill rounds")


def _any_browser_process() -> bool:
    """檢查是否有 CloakBrowser 程序還活著（用調試埠特徵搜尋）。"""
    try:
        out = subprocess.run(
            ["pgrep", "-f", "remote-debugging-port=9242"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return bool(out.stdout.strip())
    except Exception:
        return False


def _cdp_alive() -> bool:
    """檢查 CDP 端點是否回應（瀏覽器是否已就緒）。"""
    try:
        with urllib.request.urlopen(f"{CLOAK['cdp_host']}/json/version", timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


def _open_tab(url: str) -> bool:
    """透過 CDP 開新分頁（保留給備援流程使用）。"""
    try:
        req = urllib.request.Request(
            f"{CLOAK['cdp_host']}/json/new?{urllib.parse.quote(url, safe='')}",
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False


def _send_cdp(method: str, params: dict):
    """呼叫 CDP 方法（若端點支援 JSON-RPC HTTP 介面）。"""
    try:
        with urllib.request.urlopen(
            f"{CLOAK['cdp_host']}/json/rpc",
            data=json.dumps({"method": method, "params": params}).encode(),
            timeout=10,
        ) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _export_cookies_via_agent_browser() -> list | None:
    """備援一：用 agent-browser CLI 從 CDP 工作階段匯出 cookie。

    （主要流程已改為瀏覽器內直接匯出；此函式僅保留作備援。）
    """
    cmd = [
        "agent-browser",
        "--cdp",
        str(CLOAK["cdp_port"]),
        "--user-agent",
        CLOAK["user_agent"],
        "cookies",
        "get",
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        log.error("agent-browser not found on PATH")
        return None
    if out.returncode != 0:
        log.error("agent-browser cookies get failed: %s", out.stderr[:300])
        return None
    cookies = []
    for line in out.stdout.splitlines():
        line = line.strip()
        if "=" not in line or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        cookies.append(
            {
                "name": name.strip(),
                "value": value.strip(),
                "domain": "partsouq.com",
                "path": "/",
            }
        )
    return cookies if cookies else None


def _export_cookies_via_eval() -> list | None:
    """備援二：用 agent-browser eval 從頁面讀 document.cookie。"""
    cmd = [
        "agent-browser",
        "--cdp",
        str(CLOAK["cdp_port"]),
        "--user-agent",
        CLOAK["user_agent"],
        "eval",
        "document.cookie",
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        return None
    if out.returncode != 0 or "=" not in out.stdout:
        return None
    cookies = []
    for part in out.stdout.split(";"):
        if "=" in part:
            name, _, value = part.strip().partition("=")
            cookies.append(
                {
                    "name": name.strip(),
                    "value": value.strip(),
                    "domain": "partsouq.com",
                    "path": "/",
                }
            )
    return cookies or None


def _page_has_content() -> bool:
    """檢查目前頁面是否顯示真實內容（而非驗證頁）。"""
    cmd = [
        "agent-browser",
        "--cdp",
        str(CLOAK["cdp_port"]),
        "--user-agent",
        CLOAK["user_agent"],
        "eval",
        "(() => { const t = document.title || ''; const b = document.body ? document.body.innerText : ''; "
        "return JSON.stringify({title: t, len: b.length, brand: /GENUINE CATALOGS/.test(b)}); })()",
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if out.returncode != 0:
            return False
        import re

        m = re.search(r'"brand":\s*(true|false)', out.stdout)
        return bool(m and m.group(1) == "true")
    except Exception:
        return False


def _wait_challenge_resolved() -> bool:
    """等待 Turnstile 自動通過（CloakBrowser 會自己解決驗證）。"""
    deadline = time.time() + CHALLENGE_TIMEOUT
    while time.time() < deadline:
        if _page_has_content():
            return True
        time.sleep(TURNSTILE_CHECK_INTERVAL)
    return False


def _launch_cloak() -> bool:
    """啟動 CloakBrowser 並開啟 CDP 調試埠（冪等：已就緒就直接沿用）。

    子程序腳本負責：驅動 CloakBrowser 前往目標頁面、等待 Turnstile
    驗證自動通過、最後把 session cookie 匯出成 JSON 檔案。
    不再仰賴 agent-browser 的常駐 daemon（其長時間運行曾造成
    cookie 匯出不穩定的問題）。
    """
    if _cdp_alive():
        return True
    script = (
        "import asyncio, json, time, cloakbrowser\n"
        "OUT = %r\n"
        "SITE = %r\n"
        "async def main():\n"
        "    b = await cloakbrowser.launch_async(\n"
        "        headless=False, stealth_args=False,\n"
        "        args=['--remote-debugging-port=%d'])\n"
        "    ctx = await b.new_context(viewport={'width': 1366, 'height': 900})\n"
        "    page = await ctx.new_page()\n"
        "    await page.goto(SITE, wait_until='domcontentloaded', timeout=60000)\n"
        "    deadline = time.time() + 150\n"
        "    while time.time() < deadline:\n"
        "        try:\n"
        "            body = await page.evaluate('document.body ? document.body.innerText.length : 0')\n"
        "            title = await page.title()\n"
        "            if body > 20000 or 'CATALOGS' in title:\n"
        "                break\n"
        "        except Exception:\n"
        "            pass\n"
        "        await asyncio.sleep(3)\n"
        "    time.sleep(2)\n"
        "    cookies = await ctx.cookies()\n"
        "    data = [{'name': c['name'], 'value': c['value'],\n"
        "             'domain': c.get('domain', ''), 'path': c.get('path', '/')}\n"
        "            for c in cookies]\n"
        "    json.dump(data, open(OUT, 'w'))\n"
        "    print('COOKIES_EXPORTED', len(data), flush=True)\n"
        "    while True:\n"
        "        await asyncio.sleep(3600)\n"
        "asyncio.run(main())\n"
    ) % (str(CLOAK["cookie_export_file"]), SITE["genuine"], CLOAK["cdp_port"])  # noqa: UP031
    try:
        # 只保留最後一次的 stderr（"w"），避免無上限增長
        err_log = open("/tmp/cloak_launch_err.log", "w")
        proc = subprocess.Popen(
            [CLOAK["venv_python"], "-u", "-c", script],
            stdout=subprocess.DEVNULL,
            stderr=err_log,
        )
    except FileNotFoundError:
        log.error("CloakBrowser venv python not found: %s", CLOAK["venv_python"])
        return False
    log.info("CloakBrowser launching (pid=%s), waiting for CDP...", proc.pid)
    deadline = time.time() + 60
    while time.time() < deadline:
        if _cdp_alive():
            log.info("CloakBrowser CDP ready on :%s", CLOAK["cdp_port"])
            return True
        time.sleep(2)
    log.error("CloakBrowser CDP never became ready")
    return False


def refresh_session() -> list | None:
    """完整刷新流程：啟動 CloakBrowser、解決驗證、匯出 cookie。

    Single-flight：並行呼叫者會等同一場刷新完成，而不是各自啟動瀏覽器。
    成功回傳 cookie 列表；失敗回傳 None。失敗的退避時間隨連續失敗
    次數指數成長（60s → 120s → 240s，上限 20 分鐘），避免封鎖期間
    反覆啟動瀏覽器造成「刷新失敗風暴」。
    """
    with _SESSION_COND:
        while _session_state["busy"]:
            _SESSION_COND.wait()
        # 若剛才刷新成功且尚未過期，直接沿用，不需動瀏覽器
        if _session_state["cookies"] and time.monotonic() - _session_state["ok_ts"] < COOKIE_TTL:
            return _session_state["cookies"]
        # 退避期間（上次刷新失敗）共用失敗結果（P1 修復）：等待中的
        # worker 直接拿到 None，而不是輪流成為 leader 各自再刷一次。
        # 退避期過後的下一次呼叫才會真正再刷。
        if time.monotonic() < _session_state["retry_after"]:
            return None
        _session_state["busy"] = True

    try:
        cookies = _refresh_impl()
        with _SESSION_COND:
            if cookies:
                _session_state["cookies"] = cookies
                _session_state["ok_ts"] = time.monotonic()
                _session_state["retry_after"] = 0.0
                _session_state["failures"] = 0
                _session_state["version"] = _cf_value(cookies)
        return cookies
    finally:
        with _SESSION_COND:
            _session_state["busy"] = False
            _SESSION_COND.notify_all()


def session_backoff_remaining() -> float:
    """回傳距離下次允許刷新剩餘的秒數（0 = 現在就可以刷新）。

    HTTP 層在 challenge 重試的等待中呼叫：舊碼固定睡 max(65, 15*n)
    秒，完全無視 cloak 的指數退避（60s→120s→…→1200s），結果退避
    窗口還沒走完就放棄了（P2 修復）。對齊後重試節奏與退避一致。
    """
    with _SESSION_COND:
        remaining = _session_state["retry_after"] - time.monotonic()
    return max(0.0, remaining)


def force_refresh_session(rejected_version: str | None = None) -> list | None:
    """強制刷新 cookie：無視 TTL，即使快取仍新鮮也重新解決驗證。

    用於 HTTP 層收到 challenge（403/429/驗證頁）的情境 —— 此時快取
    中的 cookie 已被伺服器拒絕，繼續沿用只會重複踩雷。仍保留
    single-flight，併發呼叫者共用同一場刷新。

    rejected_version（SOL review P2）：呼叫端持有的 cookie 的
    cf_clearance 值（被伺服器拒絕的版本）。若全域 session 已由其他
    worker 刷新成**不同版本**（持舊 cookie 的延遲 challenge 在新
    cookie 產生後才返回），直接沿用新 cookie、不啟動瀏覽器 ——
    舊碼無條件清掉目前 cookie，會把別人剛刷好的新 cookie 清掉並
    再啟動一次瀏覽器。沿用只限「仍在 TTL 內」的快取；已過期則照常
    清掉重刷（否則會拿過期 session 白打一輪再被 challenge）。

    注意：**不清** failures／retry_after（P1 修復）—— 強制刷新代表
    主動重試，但若這次又失敗，退避計數必須繼續累積（否則連續失敗
    永遠停在 60 秒第一階，變成固定的 60/60/60 打點）。退避由
    refresh_session 在發起前檢查，force 只是清掉「可用 cookie」。
    """
    with _SESSION_COND:
        if (
            rejected_version is not None
            and _session_state["cookies"]
            and _session_state["version"] != rejected_version
            and time.monotonic() - _session_state["ok_ts"] < COOKIE_TTL
        ):
            # 全域 session 已是更新版本（且仍在 TTL 內）：直接沿用，
            # 不再清掉重刷
            return _session_state["cookies"]
        _session_state["cookies"] = None
        _session_state["ok_ts"] = 0.0
        _session_state["version"] = None
    return refresh_session()


def _refresh_impl() -> list | None:
    """實際的瀏覽器刷新：清掉殘留瀏覽器 → 重新啟動 → 匯出 cookie。"""
    export_file = CLOAK["cookie_export_file"]
    try:
        export_file.unlink(missing_ok=True)
    except OSError:
        pass

    # 殘留的 CloakBrowser 實例（例如上一場刷新崩潰留下的）會佔住
    # 調試埠卻永遠不匯出新 cookie：先清掉再刷新。
    if _cdp_alive() or _any_browser_process():
        log.info("stale browser detected, killing before refresh")
        _kill_browsers()
        time.sleep(2)

    if not _launch_cloak():
        _mark_refresh_failed()
        return None

    # 等待瀏覽器內部的腳本匯出 cookie（它會自動解決 Turnstile 驗證）
    deadline = time.time() + 180
    last_progress_log = 0.0
    while time.time() < deadline:
        # 每 30 秒記錄一次等待狀態：過去 crawler 曾在此迴圈被 SIGKILL，
        # 需要知道卡在哪個階段（診斷用）。
        if time.time() - last_progress_log >= 30:
            last_progress_log = time.time()
            log.info("waiting for cookie export... %ds elapsed", int(deadline - time.time()))
        if export_file.exists():
            try:
                cookies = json.loads(export_file.read_text())
                if cookies:
                    names = {c["name"] for c in cookies}
                    # 必要證據：cf_clearance = 瀏覽器真正通過 Cloudflare
                    # 驗證的證明。缺少它就代表 Turnstile 尚未解完，
                    # 這批 cookie 馬上會被 challenge —— fail closed，
                    # 不該當成「刷新成功」存檔。
                    if "cf_clearance" not in names:
                        log.error(
                            "exported cookies missing cf_clearance (%s); refresh failed",
                            sorted(names),
                        )
                        _kill_browsers()
                        _mark_refresh_failed()
                        return None
                    save_cookies(cookies)
                    log.info(
                        "session cookies exported: %s (has cf_clearance=%s)",
                        sorted(names),
                        "cf_clearance" in names,
                    )
                    return cookies
            except (json.JSONDecodeError, OSError) as e:
                log.warning("cookie export not ready yet: %s", e)
        time.sleep(3)
    log.error("no cookies exported within %ss", 180)
    _kill_browsers()
    _mark_refresh_failed()
    return None


def _mark_refresh_failed():
    """刷新失敗：退避時間隨連續失敗次數指數成長。

    1 次失敗 60s、2 次 120s、3 次 240s…… 上限 20 分鐘。
    連續失敗代表 Cloudflare 可能正在封鎖我們，退避要拉長而不是
    固定 60 秒就重試一次（那會造成反覆啟動瀏覽器的風暴）。
    """
    with _SESSION_COND:
        _session_state["failures"] = _session_state.get("failures", 0) + 1
        n = _session_state["failures"]
        delay = min(REFRESH_RETRY_BACKOFF * (2 ** (n - 1)), REFRESH_BACKOFF_MAX)
        _session_state["retry_after"] = time.monotonic() + delay
        log.warning("refresh failed (%d consecutive); backing off %.0fs", n, delay)


def get_session() -> list | None:
    """取得可用的 cookie：快取仍新鮮就直接回傳，否則執行刷新。

    可在多執行緒下安全呼叫：底層的刷新是 single-flight。
    """
    with _SESSION_COND:
        if _session_state["cookies"] and time.monotonic() - _session_state["ok_ts"] < COOKIE_TTL:
            return _session_state["cookies"]
    # TTL 已過期（或完全沒有快取）：執行一次刷新
    return refresh_session()
