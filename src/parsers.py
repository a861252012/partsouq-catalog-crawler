"""HTML 解析器（轉換層）：把 PartSouq 四層頁面轉成結構化 dict。

locate 頁面 → 品牌與型號清單（手風琴式列表）
pick 頁面   → 車型清單（規格表）
vehicle 頁面 → 分類與零件組（樹狀結構）
unit 頁面    → 零件明細（料號/名稱/代碼/備註/數量/範圍表）

本層是純函式：輸入 HTML 字串、輸出 dict 列表，不碰網路也不碰資料庫。
"""

import logging
import re
from html import unescape
from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup

log = logging.getLogger("parse")

# 使用 lxml 解析器：比 html5lib 快 3~4 倍，且已驗證四層解析輸出完全一致
PARSER = "lxml"

# 分類編號對應的固定分類名稱（PartSouq 的四個主要分類）
CATEGORY_NAMES = {
    "1": "ENGINE/FUEL/TOOL",
    "2": "POWER TRAIN/CHASSIS",
    "3": "BODY/INTERIOR",
    "4": "ELECTRICAL",
}

# 預先編譯的正規表示式（零件組文字格式：NNNN: NAME）
GROUP_LINK_RE = re.compile(r"^([0-9A-Z]+\s+[0-9]+|[0-9]{3,4}):\s*(.*)$")


def _soup(html: str) -> BeautifulSoup:
    """把 HTML 解析成 BeautifulSoup 物件（lxml 引擎）。

    多個解析函式共用同一份 HTML 時（例如 vehicle 頁面要同時解析
    分類與零件組），請先呼叫本函式一次，再把 soup 傳給各解析函式，
    避免同一份 HTML 被 lxml 重複解析。
    """
    return BeautifulSoup(html, PARSER)


def _abs(href: str) -> str:
    """把網址的 HTML 跳脫字元還原（&amp; → &）。"""
    return unescape(href) if href else ""


def _qs(url: str, key: str):
    """從網址的 query string 取出指定參數（沒有則回傳 None）。"""
    q = parse_qs(urlparse(url).query)
    vals = q.get(key, [])
    return vals[0] if vals else None


def _is_partsouq_endpoint(url: str, path: str) -> bool:
    """只接受站內相對網址或 partsouq.com 的指定 endpoint。"""
    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.scheme and not parsed.netloc:
        return False
    if parsed.netloc:
        try:
            port = parsed.port
        except ValueError:
            return False
        if parsed.hostname != "partsouq.com" or port not in {None, 80, 443}:
            return False
    return parsed.path.rstrip("/") == path


def _candidate_identity(
    url: str,
    *keys: str,
    required: tuple[str, ...] = (),
) -> tuple:
    """以 request context 對 canonical link 去重；缺欄時保留原網址。"""
    params = {key: _qs(url, key) for key in keys}
    values = tuple(params[key] for key in keys)
    if not any(values) or any(params[key] is None for key in required):
        return (*values, url)
    return values


def _context_mismatch(
    url: str,
    key: str,
    expected: str | None,
    *,
    allow_missing: bool = False,
) -> bool:
    """已知 request context 時拒絕外來值；可相容省略 brand 的舊連結。"""
    if expected is None:
        return False
    actual = _qs(url, key)
    if actual is None:
        return not allow_missing
    return actual != str(expected)


# ---------------------------------------------------------------- locate


def parse_brand_index(
    html: str,
    brand: str,
    soup=None,
    diagnostics: bool = False,
) -> list[dict] | tuple[list[dict], int]:
    """解析 locate 頁面 → 型號清單（含 pick 網址）。

    每個型號是手風琴 <h4> 標題，內含連結到
    /en/catalog/genuine/pick?c={brand}&model={name}&ssd={token}
    """
    soup = soup if soup is not None else _soup(html)
    models = []
    candidates = set()
    valid_candidates = set()
    for a in soup.select("a[href]"):
        href = _abs(a.get("href", ""))
        if not _is_partsouq_endpoint(href, "/en/catalog/genuine/pick"):
            continue
        candidate = _candidate_identity(href, "c", "model", "ssd", required=("model",))
        candidates.add(candidate)
        if _context_mismatch(href, "c", brand, allow_missing=True):
            continue
        name = a.get_text(strip=True)
        if not name or not href:
            continue
        valid_candidates.add(candidate)
        params = parse_qs(urlparse(href).query)
        models.append(
            {
                "name": name,
                "ssd": params.get("ssd", [None])[0],
                "url": href,
            }
        )
    if diagnostics:
        return models, len(candidates - valid_candidates)
    return models


def parse_brands(
    html: str,
    soup=None,
    diagnostics: bool = False,
) -> list[dict] | tuple[list[dict], int]:
    """解析原廠目錄首頁 → 品牌清單（含代碼）。

    品牌位於側邊欄：<a href="/en/catalog/genuine/locate?c=NAME">
    只採計側邊欄的連結（指向帶純品牌名的 locate 頁面）；
    依名稱去重，避免把表格列誤判為品牌連結。
    """
    soup = soup if soup is not None else _soup(html)
    brands = []
    seen = set()
    candidates = set()
    valid_candidates = set()
    for a in soup.select("li a[href]"):
        href = _abs(a.get("href", ""))
        if not _is_partsouq_endpoint(href, "/en/catalog/genuine/locate"):
            continue
        candidate = _candidate_identity(href, "c", required=("c",))
        candidates.add(candidate)
        code = _qs(href, "c")
        name = a.get_text(strip=True)
        if not code or not name:
            continue
        valid_candidates.add(candidate)
        if name in seen:
            continue
        seen.add(name)
        brands.append({"name": name, "url": href})
    if diagnostics:
        return brands, len(candidates - valid_candidates)
    return brands


# ----------------------------------------------------------------- pick


def _vehicle_fields(th_classes, th_text):
    """把 pick 頁面的欄位標題對應到車型欄位名稱。

    各品牌的欄位配置不盡相同（Toyota: Name|Description|Model|Options|Prod
    Period；Nissan: Name|Grade|Market|Model|Year From|Options|Gearbox；
    部分表格還多了 Engine/Body Style 欄）。網站會在每個 <th> 上標記
    class 特徵（如 __model/__options/__prodPeriod/__modelyearfrom），
    因此我們以特徵為鍵，而不是靠欄位位置。
    """
    classes = " ".join(th_classes or [])
    classes_lower = classes.lower()
    text = th_text.strip()
    text_lower = text.lower()
    if "n_name" in classes or text == "Name":
        return "name"
    if "__description" in classes or text == "Description":
        return "description"
    if "__grade" in classes:
        return "grade"
    if "__market" in classes:
        return "market"
    if "__modelyearfrom" in classes:
        return "year_from"
    if "__modelyearto" in classes:
        return "year_to"
    if "__model" in classes or text == "Model":
        return "model_code"
    if "__engine" in classes_lower or text_lower == "engine":
        return "engine"
    if "__prodPeriod" in classes or "prod" in classes.lower():
        return "prod_period"
    if "__options" in classes or text == "Options":
        return "options"
    if (
        "__transmission" in classes_lower
        or "__gearbox" in classes_lower
        or text_lower in {"transmission", "gearbox"}
    ):
        return "transmission"
    if "__bodystyle" in classes_lower or text_lower == "body style":
        return "body_style"
    return None


def parse_vehicles(
    html: str,
    brand: str,
    soup=None,
    diagnostics: bool = False,
) -> list[dict] | tuple[list[dict], int]:
    """解析 pick 頁面的規格表 → 車型清單。

    欄位隨品牌與表格而異（部分品牌多了 Engine / Body Style / Grade /
    Market 等欄）。我們以 th 的 class 特徵對應欄位，而且只採計
    帶 /vehicle? 連結的列 —— 那些才是真正的車型。
    """
    soup = soup if soup is not None else _soup(html)
    vehicles = []
    malformed = 0
    candidates = set()
    valid_candidates = set()
    candidate_specs = {}
    for a in soup.select("a[href]"):
        href = _abs(a.get("href", ""))
        if _is_partsouq_endpoint(href, "/en/catalog/genuine/vehicle"):
            candidates.add(_candidate_identity(href, "c", "ssd", "vid"))

    for table in soup.select("table"):
        rows = table.select("tr")
        if not rows:
            continue
        ths = rows[0].select("th")
        # 跳過沒有特徵標記的品牌/標題表格（Brand|Name|Code）
        col_map = {}
        has_marker = False
        for idx, th in enumerate(ths):
            field = _vehicle_fields(th.get("class"), th.get_text())
            if field == "name":
                has_marker = True
            if field:
                col_map[idx] = field
        if not has_marker:
            continue
        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) <= 1:
                continue
            links = [
                a
                for a in tr.select("a[href]")
                if _is_partsouq_endpoint(_abs(a.get("href", "")), "/en/catalog/genuine/vehicle")
            ]
            matching_links = [
                a
                for a in links
                if not _context_mismatch(_abs(a.get("href", "")), "c", brand, allow_missing=True)
            ]
            if not matching_links:
                continue
            url = _abs(matching_links[0].get("href"))
            rec = {
                "name": "",
                "description": "",
                "model_code": "",
                "options": "",
                "prod_period": "",
            }
            for idx, field in col_map.items():
                if idx < len(tds):
                    rec[field] = tds[idx].get_text(strip=True)
            # 沒有明確的 Prod Period 欄時，用 year_from + year_to 兜出期間
            if not rec["prod_period"] and (rec.get("year_from") or rec.get("year_to")):
                yf, yt = rec.get("year_from") or "", rec.get("year_to") or ""
                rec["prod_period"] = f"{yf} - {yt}".strip(" -") if yf and yt else (yf or yt)
            rec["ssd"] = _qs(url, "ssd")
            rec["vid"] = _qs(url, "vid")
            rec["url"] = url
            key = tuple(
                str(rec.get(field) or "")
                for field in (
                    "model_code",
                    "name",
                    "description",
                    "options",
                    "prod_period",
                    "grade",
                    "market",
                    "engine",
                    "transmission",
                    "body_style",
                )
            )
            if not any(key):
                continue
            row_candidates = {
                _candidate_identity(_abs(a.get("href", "")), "c", "ssd", "vid")
                for a in matching_links
            }
            for candidate in row_candidates:
                if candidate in candidate_specs and candidate_specs[candidate] != key:
                    malformed += 1
                    continue
                candidate_specs[candidate] = key
                valid_candidates.add(candidate)
            vehicles.append((rec, key))
    # ssd / vid / url 是請求用 token，不是車型身分。若依 ssd 去重，
    # 同 token 的不同規格會靜默消失；改以 parser 已辨識的穩定規格去重。
    seen = set()
    out = []
    for vehicle, key in vehicles:
        if key in seen:
            continue
        seen.add(key)
        out.append(vehicle)
    malformed += len(candidates - valid_candidates)
    if diagnostics:
        return out, malformed
    return out


# -------------------------------------------------------------- vehicle


def parse_category_links(
    html: str,
    brand: str,
    soup=None,
    diagnostics: bool = False,
    expected_ssd: str | None = None,
    expected_vid: str | None = None,
) -> list[dict] | tuple[list[dict], int]:
    """解析 vehicle 頁面 → 分類導覽連結。

    每個主要分類（Engine、Power Train、Body、Electrical）都是一個
    vehicle 頁面的變體連結，帶 cid + cname 參數。
    """
    soup = soup if soup is not None else _soup(html)
    cats = []
    malformed = 0
    candidates = set()
    valid_candidates = set()
    seen = {}
    for a in soup.select("a[href]"):
        href = _abs(a.get("href", ""))
        if not _is_partsouq_endpoint(href, "/en/catalog/genuine/vehicle"):
            continue
        candidate = _candidate_identity(
            href,
            "c",
            "ssd",
            "vid",
            "cid",
            required=("cid",),
        )
        candidates.add(candidate)
        if (
            _context_mismatch(href, "c", brand, allow_missing=True)
            or _context_mismatch(href, "ssd", expected_ssd)
            or _context_mismatch(href, "vid", expected_vid)
        ):
            continue
        cid = _qs(href, "cid")
        if not cid:
            continue
        text = a.get_text(strip=True)
        if not text:
            # 同一 cid 可能同時有圖片與文字 anchor；最後以 cid
            # 對帳，只有完全沒有文字 peer 才算 malformed。
            continue
        cname = _qs(href, "cname")
        name = unquote(cname) if cname else text
        if cid in seen:
            if seen[cid] != name:
                malformed += 1
            else:
                valid_candidates.add(candidate)
            continue
        seen[cid] = name
        valid_candidates.add(candidate)
        cats.append(
            {
                "category_name": name,
                "cid": cid,
                "ssd": _qs(href, "ssd"),
                "vid": _qs(href, "vid"),
                "url": href,
            }
        )
    malformed += len(candidates - valid_candidates)
    if diagnostics:
        return cats, malformed
    return cats


def parse_groups(
    html: str,
    brand: str,
    default_cid: str = "1",
    soup=None,
    diagnostics: bool = False,
    expected_ssd: str | None = None,
    expected_vid: str | None = None,
    expected_cid: str | None = None,
) -> list[dict] | tuple[list[dict], int]:
    """解析 vehicle 頁面 → 零件組連結（NNNN: NAME → /unit?...）。

    零件組位於目前啟用的分類區塊；每個都連結到
    /en/catalog/genuine/unit?c=..&ssd=..&vid=..&cid=N&uid=M&q=
    """
    soup = soup if soup is not None else _soup(html)
    groups = []
    malformed = 0
    seen = {}
    candidates = set()
    valid_candidates = set()
    candidate_specs = {}
    for a in soup.select("a[href]"):
        href = _abs(a.get("href", ""))
        # 只接受真正的 unit endpoint。redirect?next=/unit?... 或其他
        # query 內含 /unit? 的連結不是 group candidate。
        if not _is_partsouq_endpoint(href, "/en/catalog/genuine/unit"):
            continue
        cid = _qs(href, "cid") or default_cid
        uid = _qs(href, "uid")
        candidate = _candidate_identity(
            href,
            "c",
            "ssd",
            "vid",
            "cid",
            "uid",
            required=("uid",),
        )
        candidates.add(candidate)
        if (
            _context_mismatch(href, "c", brand, allow_missing=True)
            or _context_mismatch(href, "ssd", expected_ssd)
            or _context_mismatch(href, "vid", expected_vid)
            or (expected_cid is not None and cid != str(expected_cid))
        ):
            continue
        if not uid:
            continue
        text = a.get_text(strip=True)
        if not text:
            # 圖片與文字可能同時連到同一組；最後以 cid+uid 對帳，只有
            # 沒有任何可解析文字 anchor 的 candidate 才算 malformed。
            continue
        m = GROUP_LINK_RE.match(text)
        if not m:
            continue
        group_name = m.group(2).strip()
        if not group_name:
            continue
        candidate_spec = (m.group(1), group_name)
        if candidate in candidate_specs and candidate_specs[candidate] != candidate_spec:
            malformed += 1
            continue
        candidate_specs[candidate] = candidate_spec
        valid_candidates.add(candidate)
        identity = (cid, m.group(1))
        if identity in seen:
            if seen[identity] != (uid, group_name):
                malformed += 1
            continue
        seen[identity] = (uid, group_name)
        groups.append(
            {
                "group_code": m.group(1),
                "group_name": group_name,
                "category_name": CATEGORY_NAMES.get(cid, f"CATEGORY {cid}"),
                "cid": cid,
                "uid": uid,
                "ssd": _qs(href, "ssd"),
                "vid": _qs(href, "vid"),
                "url": href,
            }
        )
    malformed += len(candidates - valid_candidates)
    if diagnostics:
        return groups, malformed
    return groups


# ----------------------------------------------------------------- unit


def parse_parts(html: str, soup=None) -> tuple[list[dict], int]:
    """解析 unit 頁面的零件表。

    unit 頁面有兩張表：先是車型資訊的標題表，再來才是零件表
    （class 約為 'glow pop-vin'）。零件列帶
    Number|Name|Code|Note|Quantity|Range，且**第一個儲存格**會連結到
    /search/all?q=。

    嚴謹度（P1 修復）：只接受「搜尋連結出現在第一格」的列 —— 若同頁
    有一筆合法資料讓外層 guard 通過，其他欄位不足或空料號的列必須被
    正確排除，避免靜默漏資料或寫入錯誤料號。

    P2 修復：不依賴 `<tbody>`（部分頁面沒有顯式 tbody，lxml/html.parser
    不會自動補），直接以 table 的直接子 `tr` 為準；td 也只取直接子層，
    避免巢狀 table 的儲存格竄入造成欄位錯位。

    回傳 (parts, malformed)：
    - parts：結構完整（6 欄：Number|Name|Code|Note|Quantity|Range）
      的零件列。第一格有搜尋連結但欄數不足的列**不算零件**。
    - malformed：異常 candidate 列數 —— 欄數不是 6、搜尋網址非
      PartSouq `/en/search/all`、q 為空，或顯示料號與 q 不同。這代表
      頁面版型異常（或反爬變體）仍解析出「看似零件」的殘缺列；呼叫端
      必須拒絕寫 terminal receipt。
    """
    soup = soup if soup is not None else _soup(html)
    parts_by_key = {}
    malformed = 0
    for table in soup.find_all("table"):
        # 巢狀 table（包在另一個 table 的 td 裡）不是零件表本身，
        # 必須排除 —— 否則其內層列會被當成獨立的零件列（P2 修復，
        # fresh probe 會同時產生假料號與真料號）。
        if table.find_parent("table"):
            continue
        # 直接子層 tr（無顯式 tbody）與 tbody 內的 tr 都要解析：
        # 只取其一會漏掉混合結構的另一半（P1 修復：header 直接、
        # data 在 tbody 的頁面舊碼解析出空清單）。
        trs = table.find_all("tr", recursive=False)
        trs += [
            tr
            for tb in table.find_all("tbody", recursive=False)
            for tr in tb.find_all("tr", recursive=False)
        ]
        for tr in trs:
            tds = tr.find_all("td", recursive=False)
            if not tds:
                continue
            # 先以 endpoint path 辨認 candidate，讓同路徑的外站網址也會
            # 被回報 malformed，而不是靜默忽略。
            search_links = []
            for a in tds[0].select("a[href]"):
                href = _abs(a.get("href", ""))
                if urlparse(href).path.rstrip("/") == "/en/search/all":
                    search_links.append(href)
            if not search_links:
                continue
            if len(tds) != 6:
                malformed += 1
                continue
            cells = [td.get_text(strip=True) for td in tds]
            part_number = cells[0]
            queries = set()
            valid_links = True
            for href in search_links:
                if not _is_partsouq_endpoint(href, "/en/search/all"):
                    valid_links = False
                    break
                query = _qs(href, "q")
                if not query:
                    valid_links = False
                    break
                queries.add(query)
            if not valid_links or len(queries) != 1 or part_number not in queries:
                malformed += 1
                continue
            part = {
                "part_number": part_number,
                "name": cells[1],
                "code": cells[2],
                "note": cells[3],
                "quantity": cells[4],
                "range_str": cells[5],
            }
            # receipt/shrink 必須使用 DB natural key 的實際列數；同頁重複
            # DOM row 不得把 fetched_row_count 灌大。後出現的列與 MySQL
            # ON DUPLICATE KEY UPDATE 語意一致，覆蓋同鍵的前一列 payload。
            parts_by_key[(part_number, cells[5])] = part
    return list(parts_by_key.values()), malformed


def looks_like_challenge(html: str) -> bool:
    """粗略判斷回應是否為 Cloudflare 驗證頁（供診斷使用）。"""
    return "Just a moment" in html[:8000] or "請稍候" in html[:8000] or "cf_chl" in html[:8000]
