"""Presentation contracts on the real DOM; no provider or production writes."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "services/public_dashboards"
PAGES = {
    "overview": ("/", "public_dashboards", "index.html"),
    "shioaji": ("/shioaji/", "shioaji_api_dashboard", "index.html"),
    "finlab": ("/finlab/", "finlab_dashboard", "index.html"),
    "finmind": ("/finmind/", "finmind_dashboard", "index.html"),
    "tej": ("/tej/", "tej_dashboard", "index.html"),
    "openbb": ("/openbb/", "openbb_archive_dashboard", "index.html"),
    "data-monitor": ("/data-monitor/", "data_monitor_dashboard", "index.html"),
    "provider": ("/data-monitor/providers/TAIFEX/", "data_monitor_dashboard", "provider.html"),
    "traffic": ("/traffic/", "traffic_dashboard", "index.html"),
}


def test_browser_audit_rejects_unknown_page_before_network_or_partial_coverage(tmp_path):
    result = subprocess.run([
        "node", str(ROOT / "scripts/audit_public_dashboards_browser.mjs"),
        "1", "http://127.0.0.1:1", str(tmp_path / "audit"), "320", "568", "unknown-page",
    ], capture_output=True, text=True, timeout=5, check=False)
    assert result.returncode != 0
    assert "Unknown dashboard audit pages: unknown-page" in result.stderr
    assert not (tmp_path / "audit").exists()


@pytest.fixture(scope="module")
def browser():
    from playwright.sync_api import sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options

    with sync_playwright() as runtime:
        instance = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"))
        yield instance
        instance.close()


def mount_page(browser, kind, width=1366):
    route, directory, filename = PAGES[kind]
    html = (ROOT / "services" / directory / filename).read_text()
    original_ids = re.findall(r'\bid="([^"]+)"', html)
    # The actual source apps own network/data. Test only the shared presentation.
    html = re.sub(r'<script\b[^>]*>.*?</script>', '', html, flags=re.S)
    page = browser.new_page(viewport={"width": width, "height": 844})
    requests = []
    errors = []
    page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    page.on("pageerror", lambda error: errors.append(str(error)))

    def static(request):
        path = urlsplit(request.request.url).path
        if path == route:
            request.fulfill(body=html, content_type="text/html")
        else:
            requests.append(path)
            prefix = path.strip("/").split("/", 1)[0]
            known = next((entry for entry in PAGES.values() if entry[0] == f"/{prefix}/"), None)
            asset = ROOT / "services" / known[1] / path.rsplit("/", 1)[-1] if known else PUBLIC / path.rsplit("/", 1)[-1]
            if asset.is_file():
                request.fulfill(path=asset, content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            else:
                request.abort()

    page.route("**/*", static)
    page.goto(f"http://layout.test{route}", wait_until="load")
    page.evaluate("""() => {
      window.originalNodes = new Map([...document.querySelectorAll('[id]')].map(node => [node.id, node]));
      window.detailToggleCount = 0;
      const input = document.querySelector('input[type=search]');
      if (input) input.value = '保留搜尋';
    }""")
    page.add_script_tag(path=str(PUBLIC / "dashboard-core.js"))
    page.add_script_tag(path=str(PUBLIC / "dashboard-acquisition.js"))
    return page, requests, errors, original_ids


def test_catalog_refresh_preserves_missing_selected_filter_instead_of_switching_scope(browser):
    page, _, errors, _ = mount_page(browser, "data-monitor")
    try:
        source = (ROOT / "services/data_monitor_dashboard/app.js").read_text()
        page.add_script_tag(content=source[:source.index('\nfor (const id of ["search",')])
        observed = page.evaluate("""() => {
          const category = document.getElementById('feature-category');
          const source = document.getElementById('feature-source');
          category.add(new Option('台股', 'taiwan_equity')); category.value='taiwan_equity';
          source.add(new Option('先前來源', 'old-source')); source.value='old-source';
          populateFeatureFilters({categories:[],sources:[]});
          return {category:category.value,source:source.value,label:source.selectedOptions[0].textContent};
        }""")
        assert observed["category"] == "taiwan_equity"
        assert observed["source"] == "old-source"
        assert "本次清冊沒有此來源" in observed["label"]
        assert not errors
    finally:
        page.close()


@pytest.mark.parametrize("kind", PAGES)
@pytest.mark.parametrize("width", [320, 1366])
def test_all_acquisition_pages_keep_nodes_and_foreground_summary(browser, kind, width):
    page, requests, errors, ids = mount_page(browser, kind, width)
    try:
        assert not errors
        assert page.evaluate("""() => [...window.originalNodes].every(([id,node]) => document.getElementById(id) === node)""")
        if kind == "overview":
            assert page.locator("#dashboards > a").first.get_attribute("href") == "data-monitor/"
            assert page.locator("#strategy-dashboards > a").count() == 3
        else:
            assert page.locator(".acq-metric").count() == 4
            assert page.locator("main > details.acq-details").count() >= 3
            assert page.locator("details.acq-details[open]").count() == 0
            assert page.locator("#acq-overview").is_visible()
            assert page.locator("main > .jump-nav").evaluate("node => getComputedStyle(node).position") == "static"
            assert page.locator("#acq-overview").evaluate("node => node.getBoundingClientRect().top") < 600
            assert page.locator(".acq-metric-value").evaluate_all("""nodes => nodes.every(node =>
              node.textContent === document.getElementById(node.dataset.acqSource).textContent.trim())""")
            summary = page.locator("details.acq-details > summary").first
            summary.focus()
            summary.press("Enter")
            assert page.locator("details.acq-details[open]").count() == 1
            page.evaluate("""() => {
              const metric = document.querySelector('.acq-metric-value');
              document.getElementById(metric.dataset.acqSource).textContent = '真實新值';
            }""")
            page.wait_for_function("document.querySelector('.acq-metric-value').textContent === '真實新值'")
            assert page.locator("details.acq-details[open]").count() == 1
            if page.locator("input[type=search]").count():
                assert page.locator("input[type=search]").first.input_value() == "保留搜尋"
        assert page.evaluate("Math.max(0,document.documentElement.scrollWidth-innerWidth)") == 0
        assert not any("/api/" in path for path in requests)
        assert set(ids).issubset(set(page.locator("[id]").evaluate_all("nodes => nodes.map(node => node.id)")))
    finally:
        page.close()


@pytest.mark.parametrize("kind", ["shioaji", "finlab", "finmind", "tej", "openbb", "data-monitor", "provider"])
def test_unknown_and_zero_progress_remain_distinct(browser, kind):
    page, _, errors, _ = mount_page(browser, kind)
    try:
        assert not errors
        page.evaluate("""() => {
          const bar = document.querySelector('.acq-progress-bar');
          const source = document.getElementById(bar.dataset.acqSource);
          source.removeAttribute('value'); source.max = 37;
        }""")
        page.wait_for_function("!document.querySelector('.acq-progress-bar').hasAttribute('value')")
        assert page.locator(".acq-progress-bar").get_attribute("max") == "37"
        page.evaluate("document.getElementById(document.querySelector('.acq-progress-bar').dataset.acqSource).value = 0")
        page.wait_for_function("document.querySelector('.acq-progress-bar').getAttribute('value') === '0'")
    finally:
        page.close()


def test_old_and_new_deep_links_open_the_actual_target(browser):
    page, _, errors, _ = mount_page(browser, "finmind")
    try:
        assert not errors
        page.evaluate("location.hash = 'pipelines'")
        page.wait_for_function("document.querySelector('#acq-catalog').open")
        assert page.locator("#pipelines").is_visible()
        page.evaluate("location.hash = 'acq-progress'")
        page.wait_for_function("document.querySelector('#acq-progress').open")
        page.evaluate("window.StockAgentAcquisition.reveal('download-eta-milestones')")
        assert page.locator("#download-eta-milestones").locator("xpath=ancestor::table").is_visible()
    finally:
        page.close()


@pytest.mark.parametrize("width", [1920, 2560])
def test_late_quota_link_has_no_ghost_box_inside_closed_details(browser, width):
    page, _, errors, _ = mount_page(browser, "provider", width)
    try:
        assert not errors
        page.evaluate("""() => {
          const link = document.getElementById('provider-specialized');
          link.hidden = false; link.href = '/finmind/';
          StockAgentAcquisition.reveal('provider-sources');
        }""")
        assert page.locator("#provider-source-search").is_visible()
        assert not page.locator("#provider-specialized").is_visible()
        assert page.locator("#provider-specialized").evaluate("node => node.getClientRects().length") == 0
        assert page.locator("#acq-quota").get_attribute("open") is None
        page.evaluate("StockAgentAcquisition.reveal('provider-specialized')")
        assert page.locator("#provider-specialized").is_visible()
    finally:
        page.close()


def test_layout_module_has_no_network_or_polling_owner():
    script = (PUBLIC / "dashboard-acquisition.js").read_text()
    assert "fetch(" not in script
    assert "setInterval(" not in script
    assert "setTimeout(" not in script
    assert "innerHTML" not in script
    for kind, (_, directory, filename) in PAGES.items():
        html = (ROOT / "services" / directory / filename).read_text()
        assert f'data-acquisition-page="{kind}"' in html
        assert "dashboard-acquisition.js?v=3" in html
        assert "dashboard-acquisition.css?v=4" in html
    # Strategy pages keep their domain-specific presentation.
    # Day-trade and overnight deliberately share the same HTML / renderer.
    for name in ["taifex_dashboard", "tw_day_trade_dashboard"]:
        assert "dashboard-acquisition.js" not in (ROOT / "services" / name / "index.html").read_text()
