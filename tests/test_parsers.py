"""Parser 單元測試：對 PartSouq 四層頁面的 HTML 進行解析驗證。

設計動機：先前曾發生「parse_* 函數簽名改成 (html, ...) 但呼叫端漏傳」
的型別錯誤，而 parser 完全沒有測試，導致爬蟲整層失敗卻只有 runtime
log 才抓得到。本測試用合成 HTML 覆蓋每個 parser 的輸出契約。

每個測試只測「輸入 HTML → 輸出 dict」的純函式行為，不碰網路與資料庫。
"""

import unittest

from src.parsers import (
    parse_brand_index,
    parse_brands,
    parse_category_links,
    parse_groups,
    parse_parts,
    parse_vehicles,
)


def _locate_html():
    """locate 頁面：側邊欄品牌 + 手風琴型號清單。"""
    return """
<html><body>
<ul class="brands">
  <li><a href="/en/catalog/genuine/locate?c=TOYOTA">TOYOTA</a></li>
  <li><a href="/en/catalog/genuine/locate?c=Lexus">Lexus</a></li>
</ul>
<h4><a href="/en/catalog/genuine/pick?c=TOYOTA&amp;model=4RUNNER&amp;ssd=abc123">4RUNNER</a></h4>
<h4><a href="/en/catalog/genuine/pick?c=TOYOTA&amp;model=COROLLA&amp;ssd=def456">COROLLA</a></h4>
</body></html>
"""


def _pick_html():
    """pick 頁面：帶 class 特徵標記的車型規格表。"""
    return """
<html><body>
<table>
<thead><tr>
  <th class="__vehicle __name">Name</th>
  <th class="__description">Description</th>
  <th class="__model">Model</th>
  <th class="__options">Options</th>
  <th class="__prodPeriod">Prod Period</th>
</tr></thead>
<tbody>
<tr>
  <td><a href="/en/catalog/genuine/vehicle?ssd=SSD1&amp;vid=1">ALPHARD/VELLFIRE/HV</a></td>
  <td>AGH3#,AYH30</td>
  <td>AGH30W-NFXGK</td>
  <td>ATM,MTM: STM</td>
  <td>01.2015 - 03.2018</td>
</tr>
<tr>
  <td><a href="/en/catalog/genuine/vehicle?ssd=SSD2&amp;vid=2">ALPHARD/HV</a></td>
  <td>GGH3#</td>
  <td>GGH30W-NFXGK</td>
  <td>ATM</td>
  <td>01.2018 -</td>
</tr>
</tbody>
</table>
</body></html>
"""


def _vehicle_html():
    """vehicle 頁面：分類導覽 + 零件組連結（含預設分類 1）。"""
    return """
<html><body>
<a href="/en/catalog/genuine/vehicle?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=1&amp;cname=ENGINE%2FFUEL%2FTOOL">ENGINE/FUEL/TOOL</a>
<a href="/en/catalog/genuine/vehicle?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=2&amp;cname=POWER+TRAIN%2FCHASSIS">POWER TRAIN/CHASSIS</a>
<div class="groups">
  <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=1&amp;uid=U1&amp;q=">1101: PARTIAL ENGINE ASSY</a>
  <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=1&amp;uid=U2&amp;q=">1102: ENGINE ASSY</a>
  <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=2&amp;uid=U3&amp;q=">2701: PROPELLER SHAFT</a>
</div>
</body></html>
"""


def _unit_html():
    """unit 頁面：標題表 + 零件表（第一格連到 /search/all?）。"""
    return """
<html><body>
<table class="glow pop-vin"><tbody>
<tr>
  <td><a href="/en/search/all?q=190000V200">190000V200</a></td>
  <td>ENGINE ASSY, PARTIAL</td>
  <td>11000</td>
  <td></td>
  <td>01</td>
  <td>01.2015 - 01.2018</td>
</tr>
<tr>
  <td><a href="/en/search/all?q=190000V210">190000V210</a></td>
  <td>ENGINE ASSY, PARTIAL</td>
  <td>11000</td>
  <td>NOTE</td>
  <td>02</td>
  <td>01.2018 -</td>
</tr>
</tbody></table>
<table><tbody>
<tr><td>NOT A PART</td><td>foo</td></tr>
</tbody></table>
</body></html>
"""


class TestParseBrands(unittest.TestCase):
    """原廠目錄首頁 → 品牌清單。"""

    def test_parses_sidebar_brands_deduplicated(self):
        html = """
        <html><body>
        <ul>
          <li><a href="/en/catalog/genuine/locate?c=TOYOTA">TOYOTA</a></li>
          <li><a href="/en/catalog/genuine/locate?c=TOYOTA">TOYOTA</a></li>
          <li><a href="/en/catalog/genuine/locate?c=Lexus">Lexus</a></li>
        </ul>
        </body></html>
        """
        brands = parse_brands(html)
        self.assertEqual(len(brands), 2)
        self.assertEqual([b["name"] for b in brands], ["TOYOTA", "Lexus"])
        self.assertIn("/locate?c=TOYOTA", brands[0]["url"])

    def test_ignores_links_not_in_li(self):
        """非 li 下的 /locate? 連結（例如表格列）不得被當成品牌。"""
        html = """
        <html><body>
        <table><tr><td><a href="/en/catalog/genuine/locate?c=NOTBRAND">NOTBRAND</a></td></tr></table>
        </body></html>
        """
        self.assertEqual(parse_brands(html), [])

    def test_ignores_off_domain_locate_endpoint(self):
        html = '<li><a href="https://evil.example/en/catalog/genuine/locate?c=BAD">BAD</a></li>'
        self.assertEqual(parse_brands(html), [])

    def test_mixed_valid_and_image_only_brand_reports_malformed(self):
        html = """
        <ul>
          <li><a href="/en/catalog/genuine/locate?c=TOYOTA">TOYOTA</a></li>
          <li><a href="/en/catalog/genuine/locate?c=NISSAN"><img src="nissan.png"></a></li>
        </ul>
        """
        brands, malformed = parse_brands(html, diagnostics=True)
        self.assertEqual([brand["name"] for brand in brands], ["TOYOTA"])
        self.assertEqual(malformed, 1)


class TestParseBrandIndex(unittest.TestCase):
    """locate 頁面 → 型號清單。"""

    def test_parses_model_accordion_with_ssd(self):
        models = parse_brand_index(_locate_html(), "TOYOTA")
        self.assertEqual(len(models), 2)
        self.assertEqual(models[0]["name"], "4RUNNER")
        self.assertEqual(models[0]["ssd"], "abc123")
        self.assertIn("/pick?", models[0]["url"])

    def test_skips_href_without_ssd(self):
        html = """
        <html><body>
        <h4><a href="/en/catalog/genuine/pick?c=TOYOTA&amp;model=NO-SSD">NO-SSD</a></h4>
        </body></html>
        """
        models = parse_brand_index(html, "TOYOTA")
        self.assertEqual(len(models), 1)
        self.assertIsNone(models[0]["ssd"])

    def test_ignores_off_domain_pick_endpoint(self):
        html = '<a href="https://evil.example/en/catalog/genuine/pick?ssd=S">MODEL</a>'
        self.assertEqual(parse_brand_index(html, "TOYOTA"), [])

    def test_same_domain_foreign_brand_model_is_malformed(self):
        html = '<a href="/en/catalog/genuine/pick?c=NISSAN&amp;model=NOTE&amp;ssd=S">NOTE</a>'
        self.assertEqual(
            parse_brand_index(html, "TOYOTA", diagnostics=True),
            ([], 1),
        )


class TestParseVehicles(unittest.TestCase):
    """pick 頁面 → 車型清單。"""

    def test_parses_spec_table_with_markers(self):
        vehicles = parse_vehicles(_pick_html(), "TOYOTA")
        self.assertEqual(len(vehicles), 2)
        v = vehicles[0]
        self.assertEqual(v["name"], "ALPHARD/VELLFIRE/HV")
        self.assertEqual(v["description"], "AGH3#,AYH30")
        self.assertEqual(v["model_code"], "AGH30W-NFXGK")
        self.assertEqual(v["options"], "ATM,MTM: STM")
        self.assertEqual(v["prod_period"], "01.2015 - 03.2018")
        self.assertEqual(v["ssd"], "SSD1")
        self.assertEqual(v["vid"], "1")

    def test_same_token_does_not_collapse_different_specs(self):
        html = """
        <html><body>
        <table>
        <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?ssd=SAME&amp;vid=1">A</a></td><td>M1</td></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?ssd=SAME&amp;vid=9">B</a></td><td>M2</td></tr>
        </table>
        </body></html>
        """
        vehicles = parse_vehicles(html, "TOYOTA")
        self.assertEqual(len(vehicles), 2)

    def test_deduplicates_same_spec_across_token_rotation(self):
        html = """
        <table>
        <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?ssd=A&amp;vid=1">V</a></td><td>M</td></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?ssd=B&amp;vid=2">V</a></td><td>M</td></tr>
        </table>
        """
        self.assertEqual(len(parse_vehicles(html, "TOYOTA")), 1)

    def test_parses_gearbox_and_body_style(self):
        html = """
        <table>
        <tr>
          <th class="__name">Name</th><th>Gearbox</th><th class="__bodyStyle">Body Style</th>
        </tr>
        <tr>
          <td><a href="/en/catalog/genuine/vehicle?ssd=S&amp;vid=1">V</a></td>
          <td>AT</td><td>WAGON</td>
        </tr>
        </table>
        """
        vehicle = parse_vehicles(html, "NISSAN")[0]
        self.assertEqual(vehicle["transmission"], "AT")
        self.assertEqual(vehicle["body_style"], "WAGON")

    def test_missing_ssd_vid_zero_not_collapsed(self):
        """P2：無 ssd 且 vid=0 的多台車不得 collapse 成一台（資料損失）。"""
        html = """
        <html><body>
        <table>
        <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?vid=0">ALPHARD A</a></td><td>AGH30</td></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?vid=0">ALPHARD B</a></td><td>AGH30</td></tr>
        </table>
        </body></html>
        """
        vehicles = parse_vehicles(html, "TOYOTA")
        self.assertEqual(len(vehicles), 2, "vid=0 且無 ssd 時必須依複合 key（vid+code+name）去重")

    def test_year_fallback_creates_prod_period(self):
        html = """
        <html><body>
        <table>
        <tr><th class="__name">Name</th><th class="__modelyearfrom">Year From</th><th class="__modelyearto">Year To</th></tr>
        <tr>
          <td><a href="/en/catalog/genuine/vehicle?ssd=S&amp;vid=1">V</a></td>
          <td>2010</td><td>2015</td>
        </tr>
        </table>
        </body></html>
        """
        vehicles = parse_vehicles(html, "TOYOTA")
        self.assertEqual(vehicles[0]["prod_period"], "2010 - 2015")

    def test_skips_row_without_vehicle_link(self):
        html = """
        <html><body>
        <table>
        <tr><th class="__name">Name</th></tr>
        <tr><td>NO LINK</td><td>M</td></tr>
        <tr><td><a href="/en/catalog/genuine/vehicle?ssd=S&amp;vid=1">HAS LINK</a></td><td>M</td></tr>
        </table>
        </body></html>
        """
        vehicles = parse_vehicles(html, "TOYOTA")
        self.assertEqual(len(vehicles), 1)
        self.assertEqual(vehicles[0]["name"], "HAS LINK")

    def test_ignores_off_domain_vehicle_endpoint(self):
        html = """
        <table>
          <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
          <tr><td><a href="https://evil.example/en/catalog/genuine/vehicle?ssd=S">BAD</a></td><td>M</td></tr>
        </table>
        """
        self.assertEqual(parse_vehicles(html, "TOYOTA"), [])

    def test_mixed_valid_and_empty_vehicle_candidate_reports_malformed(self):
        html = """
        <table>
          <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
          <tr><td><a href="/en/catalog/genuine/vehicle?ssd=S1&amp;vid=1">GOOD</a></td><td>M1</td></tr>
          <tr><td><a href="/en/catalog/genuine/vehicle?ssd=S2&amp;vid=2"><img src="x"></a></td><td></td></tr>
        </table>
        """
        vehicles, malformed = parse_vehicles(html, "TOYOTA", diagnostics=True)
        self.assertEqual([vehicle["name"] for vehicle in vehicles], ["GOOD"])
        self.assertEqual(malformed, 1)

    def test_same_domain_foreign_vehicle_context_is_malformed(self):
        html = """
        <table>
          <tr><th class="__name">Name</th><th class="__model">Model</th></tr>
          <tr>
            <td><a href="/en/catalog/genuine/vehicle?c=NISSAN&amp;ssd=S1&amp;vid=1">BAD</a></td>
            <td>M1</td>
          </tr>
        </table>
        """
        self.assertEqual(
            parse_vehicles(
                html,
                "TOYOTA",
                diagnostics=True,
            ),
            ([], 1),
        )


class TestParseCategoryLinks(unittest.TestCase):
    """vehicle 頁面 → 分類導覽連結。"""

    def test_parses_categories_with_cid_cname(self):
        cats = parse_category_links(_vehicle_html(), "TOYOTA")
        self.assertEqual(len(cats), 2)
        self.assertEqual(cats[0]["cid"], "1")
        self.assertEqual(cats[0]["category_name"], "ENGINE/FUEL/TOOL")
        self.assertEqual(cats[1]["cid"], "2")
        self.assertEqual(cats[1]["category_name"], "POWER TRAIN/CHASSIS")
        self.assertEqual(cats[0]["ssd"], "S1")

    def test_skips_link_without_cid(self):
        html = """
        <html><body>
        <a href="/en/catalog/genuine/vehicle?ssd=S&amp;vid=1">NO CID</a>
        </body></html>
        """
        self.assertEqual(parse_category_links(html, "TOYOTA"), [])

    def test_category_candidates_are_canonical_and_deduplicated(self):
        html = """
        <a href="/en/catalog/genuine/vehicle?cid=2"><img src="x"></a>
        <a href="/en/catalog/genuine/vehicle?cid=2&amp;cname=BODY">BODY</a>
        <a href="/redirect?next=/en/catalog/genuine/vehicle?cid=3">REDIRECT</a>
        """
        categories, malformed = parse_category_links(html, "TOYOTA", diagnostics=True)
        self.assertEqual([(c["cid"], c["category_name"]) for c in categories], [("2", "BODY")])
        self.assertEqual(malformed, 0)

    def test_image_only_or_missing_cid_category_is_malformed(self):
        html = """
        <a href="/en/catalog/genuine/vehicle?cid=2"><img src="x"></a>
        <a href="/en/catalog/genuine/vehicle?ssd=S">MISSING CID</a>
        """
        categories, malformed = parse_category_links(html, "TOYOTA", diagnostics=True)
        self.assertEqual(categories, [])
        self.assertEqual(malformed, 2)

    def test_off_domain_category_endpoint_is_ignored(self):
        html = (
            '<a href="https://evil.example/en/catalog/genuine/vehicle?cid=2&amp;cname=BAD">BAD</a>'
        )
        self.assertEqual(parse_category_links(html, "TOYOTA", diagnostics=True), ([], 0))

    def test_same_domain_foreign_category_context_is_malformed(self):
        html = """
        <a href="/en/catalog/genuine/vehicle?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=1">ENGINE</a>
        <a href="/en/catalog/genuine/vehicle?c=NISSAN&amp;ssd=S1&amp;vid=1&amp;cid=2">BODY</a>
        <a href="/en/catalog/genuine/vehicle?c=TOYOTA&amp;ssd=OTHER&amp;vid=1&amp;cid=3">OTHER</a>
        """
        categories, malformed = parse_category_links(
            html,
            "TOYOTA",
            diagnostics=True,
            expected_ssd="S1",
            expected_vid="1",
        )
        self.assertEqual([category["cid"] for category in categories], ["1"])
        self.assertEqual(malformed, 2)


class TestParseGroups(unittest.TestCase):
    """vehicle 頁面 → 零件組清單。"""

    def test_parses_groups_with_code_and_name(self):
        groups = parse_groups(_vehicle_html(), "TOYOTA")
        self.assertEqual(len(groups), 3)
        g = groups[0]
        self.assertEqual(g["group_code"], "1101")
        self.assertEqual(g["group_name"], "PARTIAL ENGINE ASSY")
        self.assertEqual(g["uid"], "U1")
        self.assertEqual(g["cid"], "1")
        self.assertEqual(g["category_name"], "ENGINE/FUEL/TOOL")
        self.assertEqual(g["ssd"], "S1")

    def test_default_cid_applied_when_missing(self):
        """網址沒帶 cid 時必須使用 default_cid。"""
        html = """
        <html><body>
        <a href="/en/catalog/genuine/unit?ssd=S&amp;vid=1&amp;uid=U1&amp;q=">1101: PARTIAL ENGINE ASSY</a>
        </body></html>
        """
        groups = parse_groups(html, "TOYOTA", default_cid="2")
        self.assertEqual(groups[0]["cid"], "2")
        self.assertEqual(groups[0]["category_name"], "POWER TRAIN/CHASSIS")

    def test_skips_non_group_link_text(self):
        """沒有「NNNN: NAME」格式的連結不得被當零件組。"""
        html = """
        <html><body>
        <a href="/en/catalog/genuine/unit?uid=U1">IMAGE LINK</a>
        <a href="/en/catalog/genuine/unit?uid=U2">1101: REAL GROUP</a>
        </body></html>
        """
        groups = parse_groups(html, "TOYOTA")
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["group_code"], "1101")

    def test_soup_parameter_avoids_reparse(self):
        """傳入既有 soup 時必須能解析（crawler 的效能路徑）。"""
        from src.parsers import _soup

        soup = _soup(_vehicle_html())
        groups = parse_groups("", "TOYOTA", soup=soup)
        self.assertEqual(len(groups), 3)

    def test_group_candidates_are_canonical_and_deduplicated(self):
        """圖片/重複連結不算缺漏，query 內夾帶 /unit 也不是 endpoint。"""
        html = """
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1"><img src="x"></a>
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">1101: ENGINE</a>
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">1101: ENGINE</a>
        <a href="/redirect?next=/en/catalog/genuine/unit?uid=BAD">9999: REDIRECT</a>
        """
        groups, malformed = parse_groups(html, "TOYOTA", diagnostics=True)
        self.assertEqual([(g["cid"], g["group_code"]) for g in groups], [("1", "1101")])
        self.assertEqual(malformed, 0)

    def test_malformed_canonical_group_is_reported(self):
        html = """
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">unexpected label</a>
        <a href="/en/catalog/genuine/unit?cid=1">1101: MISSING UID</a>
        """
        groups, malformed = parse_groups(html, "TOYOTA", diagnostics=True)
        self.assertEqual(groups, [])
        self.assertEqual(malformed, 2)

    def test_image_only_group_without_text_peer_is_reported(self):
        html = '<a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1"><img src="x"></a>'
        groups, malformed = parse_groups(html, "TOYOTA", diagnostics=True)
        self.assertEqual(groups, [])
        self.assertEqual(malformed, 1)

    def test_conflicting_duplicate_group_is_reported(self):
        html = """
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">1101: ENGINE</a>
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U2">1101: DIFFERENT</a>
        """
        groups, malformed = parse_groups(html, "TOYOTA", diagnostics=True)
        self.assertEqual(len(groups), 1)
        self.assertEqual(malformed, 1)

    def test_same_canonical_uid_with_different_code_is_reported(self):
        html = """
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">1101: ENGINE</a>
        <a href="/en/catalog/genuine/unit?cid=1&amp;uid=U1">1102: DIFFERENT</a>
        """
        groups, malformed = parse_groups(html, "TOYOTA", diagnostics=True)
        self.assertEqual(len(groups), 1)
        self.assertEqual(malformed, 1)

    def test_off_domain_unit_endpoint_is_ignored(self):
        html = (
            '<a href="https://evil.example/en/catalog/genuine/unit?cid=1&amp;uid=U1">1101: BAD</a>'
        )
        self.assertEqual(parse_groups(html, "TOYOTA", diagnostics=True), ([], 0))

    def test_same_domain_foreign_group_context_is_malformed(self):
        html = """
        <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=2&amp;uid=GOOD">2201: GOOD</a>
        <a href="/en/catalog/genuine/unit?c=NISSAN&amp;ssd=S1&amp;vid=1&amp;cid=2&amp;uid=BAD1">2202: BAD BRAND</a>
        <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=9&amp;cid=2&amp;uid=BAD2">2203: BAD VEHICLE</a>
        <a href="/en/catalog/genuine/unit?c=TOYOTA&amp;ssd=S1&amp;vid=1&amp;cid=3&amp;uid=BAD3">3301: BAD CATEGORY</a>
        """
        groups, malformed = parse_groups(
            html,
            "TOYOTA",
            default_cid="2",
            diagnostics=True,
            expected_ssd="S1",
            expected_vid="1",
            expected_cid="2",
        )
        self.assertEqual([group["uid"] for group in groups], ["GOOD"])
        self.assertEqual(malformed, 3)


class TestParseParts(unittest.TestCase):
    """unit 頁面 → 零件明細。"""

    def test_parses_part_rows(self):
        parts, malformed = parse_parts(_unit_html())
        self.assertEqual(len(parts), 2)
        self.assertEqual(malformed, 0)
        p = parts[0]
        self.assertEqual(p["part_number"], "190000V200")
        self.assertEqual(p["name"], "ENGINE ASSY, PARTIAL")
        self.assertEqual(p["code"], "11000")
        self.assertEqual(p["quantity"], "01")
        self.assertEqual(p["range_str"], "01.2015 - 01.2018")

    def test_skips_rows_without_search_link(self):
        """不含 /search/all? 連結的列不是零件列。"""
        html = """
        <html><body>
        <table><tbody>
          <tr><td>NOT A PART</td><td>x</td><td>y</td></tr>
        </tbody></table>
        </body></html>
        """
        self.assertEqual(parse_parts(html), ([], 0))

    def test_soup_parameter_path(self):
        from src.parsers import _soup

        soup = _soup(_unit_html())
        parts, malformed = parse_parts("", soup=soup)
        self.assertEqual(len(parts), 2)
        self.assertEqual(malformed, 0)

    def test_duplicate_natural_key_counts_once(self):
        html = """
        <table><tbody>
          <tr><td><a href="/en/search/all?q=P1">P1</a></td><td>old</td><td>C</td><td>N</td><td>1</td><td>R</td></tr>
          <tr><td><a href="/en/search/all?q=P1">P1</a></td><td>new</td><td>C</td><td>N</td><td>1</td><td>R</td></tr>
        </tbody></table>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0]["name"], "new")
        self.assertEqual(malformed, 0)

    def test_link_must_be_in_first_cell(self):
        """P1：搜尋連結必須在第一格；別的位置有連結不是零件列。"""
        html = """
        <html><body>
        <table><tbody>
          <tr><td>190000V200</td><td><a href="/en/search/all?q=190000V200">x</a></td><td>y</td></tr>
        </tbody></table>
        </body></html>
        """
        self.assertEqual(parse_parts(html), ([], 0), "連結不在第一格 => 不是零件列")

    def test_rejects_empty_part_number(self):
        """SOL P1：第一格有搜尋連結但料號空白 => 排除且計入 malformed
        （不寫入空料號、不無聲吞掉，由呼叫端拒絕 receipt）。"""
        html = """
        <html><body>
        <table><tbody>
          <tr><td><a href="/en/search/all?q="></a></td><td>NAME</td><td>y</td></tr>
        </tbody></table>
        </body></html>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(parts, [])
        self.assertEqual(malformed, 1)

    def test_single_cell_row_is_malformed(self):
        """SOL P1：第一格有料號但欄數不足（< 6 欄）=> 不算零件，計入 malformed。

        舊版把這種列當合法零件（其餘欄位全為 NULL 落庫），之後 group
        寫 done 本月不再重抓，缺欄資料被固定 —— 現在由呼叫端拒絕
        寫 terminal receipt。
        """
        html = """
        <html><body>
        <table><tbody>
          <tr><td><a href="/en/search/all?q=ABC">ABC</a></td></tr>
        </tbody></table>
        </body></html>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(parts, [], "缺欄列不得當零件")
        self.assertEqual(malformed, 1, "缺欄列必須計入 malformed")

    def test_seven_column_row_is_malformed(self):
        html = """
        <table><tbody><tr>
          <td><a href="/en/search/all?q=P1">P1</a></td>
          <td>NAME</td><td>EXTRA</td><td>CODE</td><td>NOTE</td><td>01</td><td>RANGE</td>
        </tr></tbody></table>
        """
        self.assertEqual(parse_parts(html), ([], 1))

    def test_external_search_link_is_malformed(self):
        html = """
        <table><tbody><tr>
          <td><a href="https://evil.example/en/search/all?q=P1">P1</a></td>
          <td>NAME</td><td>CODE</td><td>NOTE</td><td>01</td><td>RANGE</td>
        </tr></tbody></table>
        """
        self.assertEqual(parse_parts(html), ([], 1))

    def test_query_and_displayed_part_number_must_match(self):
        html = """
        <table><tbody><tr>
          <td><a href="/en/search/all?q=REAL">DISPLAY</a></td>
          <td>NAME</td><td>CODE</td><td>NOTE</td><td>01</td><td>RANGE</td>
        </tr></tbody></table>
        """
        self.assertEqual(parse_parts(html), ([], 1))

    def test_parses_without_tbody(self):
        """P2：零件表沒有顯式 <tbody> 時仍要解析（html.parser 不會自動補）。"""
        html = """
        <html><body>
        <table>
          <tr><td><a href="/en/search/all?q=ABC">ABC</a></td><td>NAME</td><td>C</td><td></td><td>01</td><td>R</td></tr>
        </table>
        </body></html>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(len(parts), 1, "無 tbody 的零件表必須照常解析")
        self.assertEqual(parts[0]["part_number"], "ABC")
        self.assertEqual(malformed, 0)

    def test_nested_table_cells_do_not_misalign(self):
        """P2：巢狀 table 的 td 不應竄入欄位造成錯位。"""
        html = """
        <html><body>
        <table>
          <tr>
            <td><a href="/en/search/all?q=ABC">ABC</a></td>
            <td>NAME</td>
            <td>CODE</td>
            <td><table><tr><td>SNEAKY</td><td>INNER</td></tr></table></td>
            <td>01</td>
            <td>RANGE</td>
          </tr>
        </table>
        </body></html>
        """
        parts, _malformed = parse_parts(html)
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0]["code"], "CODE", "巢狀 table 的 td 不得錯位成 code")
        self.assertEqual(parts[0]["note"], "SNEAKYINNER", "巢狀內容併入 note 欄")

    def test_nested_table_not_parsed_as_part_rows(self):
        """P2：巢狀 table 本身不得被當成零件表（會產生假料號）。"""
        html = """
        <html><body>
        <table>
          <tr>
            <td><a href="/en/search/all?q=ABC">ABC</a></td>
            <td>NAME</td>
            <td>CODE</td>
            <td>
              <table>
                <tr><td><a href="/en/search/all?q=FAKE">FAKENAME</a></td><td>inner</td></tr>
              </table>
            </td>
            <td>01</td>
            <td>RANGE</td>
          </tr>
        </table>
        </body></html>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(len(parts), 1, "巢狀 table 不得產生假料號")
        self.assertEqual(parts[0]["part_number"], "ABC")
        self.assertEqual(malformed, 0, "巢狀表的缺欄列不得計入 malformed")

    def test_mixed_direct_and_tbody_rows(self):
        """P1：同一表同時有直接 tr（如標題列）與 tbody 內 tr 都要解析。"""
        html = """
        <html><body>
        <table>
          <tr><th>HEADER</th></tr>
          <tbody>
            <tr><td><a href="/en/search/all?q=ABC">ABC</a></td><td>NAME</td><td>C</td><td></td><td>01</td><td>R</td></tr>
            <tr><td><a href="/en/search/all?q=DEF">DEF</a></td><td>NAME2</td><td>C</td><td></td><td>01</td><td>R</td></tr>
          </tbody>
        </table>
        </body></html>
        """
        parts, malformed = parse_parts(html)
        self.assertEqual(len(parts), 2, "直接 tr + tbody tr 混合結構必須全部解析")
        self.assertEqual({p["part_number"] for p in parts}, {"ABC", "DEF"})
        self.assertEqual(malformed, 0)


class TestCallContract(unittest.TestCase):
    """防回歸：所有 parser 函式的呼叫契約都必須能直接使用。

    先前爬蟲事故的根因之一：parse_* 改成「html 為必傳參數」後，
    crawler 端呼叫卻漏傳 html，導致整層失敗。這裡用 inspect 檢查
    所有公開 parse_* 的第一個參數都是 html。
    """

    def test_all_parsers_take_html_first(self):
        import inspect

        from src import parsers

        for name, fn in vars(parsers).items():
            # 只看本模組定義的 parse_*（排除 import 的 urllib.parse_qs 等）
            if not name.startswith("parse_"):
                continue
            if getattr(fn, "__module__", "") != "src.parsers":
                continue
            params = list(inspect.signature(fn).parameters)
            self.assertEqual(
                params[0],
                "html",
                f"{name}() 的第一個參數必須是 html，目前是 {params[0]}",
            )

    def test_crawler_always_passes_html_to_parsers(self):
        """靜態（AST）檢查 crawler.py 對每個 parse_* 的呼叫。

        防回歸：任何 parse_*(...) 呼叫都必須帶有 positional html，
        或帶 html= 關鍵字 —— 否則就會重演「漏傳 html」的整層事故。
        """
        import ast
        from pathlib import Path

        src_path = Path(__file__).resolve().parent.parent / "src" / "crawler.py"
        tree = ast.parse(src_path.read_text())

        parse_names = {
            "parse_brand_index",
            "parse_brands",
            "parse_vehicles",
            "parse_category_links",
            "parse_groups",
            "parse_parts",
        }
        violations = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else fn.id
            if name not in parse_names:
                continue
            args = node.args
            kwargs = {kw.arg for kw in node.keywords if kw.arg is not None}
            has_html = (
                args and isinstance(args[0], ast.Name) and args[0].id == "html"
            ) or "html" in kwargs
            if not has_html:
                violations.append(f"{name}() at line {node.lineno}")

        self.assertEqual(
            violations,
            [],
            "以下 parse_* 呼叫漏傳 html（會觸發 TypeError）:\n"
            + "\n".join(f"  - {v}" for v in violations),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
