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
    "news": ("/news/", "news_dashboard", "index.html"),
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


def mount_page(browser, kind, width=1366, *, delay_hash_notifications=False):
    route, directory, filename = PAGES[kind]
    html = (ROOT / "services" / directory / filename).read_text()
    original_ids = re.findall(r'\bid="([^"]+)"', html)
    # The actual source apps own network/data. Test only the shared presentation.
    html = re.sub(r'<script\b[^>]*>.*?</script>', '', html, flags=re.S)
    page = browser.new_page(viewport={"width": width, "height": 844})
    if delay_hash_notifications:
        # Deterministic version of the retained native trace: the hash task
        # arrives after a newer keyboard disclosure action. Only notification
        # delivery is delayed; clicks, focus, history and details stay native.
        page.add_init_script("""(() => {
          window.delayedHashNotifications = [];
          const listen = window.addEventListener.bind(window);
          window.addEventListener = (kind, callback, options) => {
            if (kind !== 'hashchange') return listen(kind, callback, options);
            return listen(kind, event => window.delayedHashNotifications.push(() => callback(event)), options);
          };
        })();""")
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
      window.originalSearchValues = new Map([...document.querySelectorAll('input[type=search]')]
        .map(node => [node, node.value]));
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
            # FinLab already includes its separate tick estimate; presentation
            # changes must preserve it rather than enforce a four-card ceiling.
            assert page.locator(".acq-metric").count() == (5 if kind == "finlab" else 4)
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
                # Sections may move; preserve each actual input, not whichever
                # becomes first after arranging the acquisition disclosures.
                assert page.evaluate("""() => [...window.originalSearchValues]
                  .every(([node,value]) => node.isConnected && node.value === value)""")
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


@pytest.mark.parametrize("kind", ["news", "finmind"])
def test_delayed_hash_notification_cannot_undo_newer_native_disclosure_action(browser, kind):
    from playwright.sync_api import expect

    page, requests, errors, _ = mount_page(browser, kind, delay_hash_notifications=True)
    try:
        page.locator('a[href="#acq-quota"]').click()
        expect(page.locator('#traffic')).to_be_visible()
        page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
        summary = page.locator('#acq-quota > summary')
        summary.focus()
        summary.press('Enter')
        expect(page.locator('#traffic')).not_to_be_visible()
        delivered = page.evaluate("""() => {
          const pending = window.delayedHashNotifications.splice(0);
          for (const notify of pending) notify();
          return pending.length;
        }""")
        assert page.locator('#acq-quota').get_attribute('open') is None, delivered
        assert not errors and not any('/api/' in path for path in requests)
    finally:
        page.close()


@pytest.mark.parametrize("kind", ["news", "finmind"])
def test_native_anchor_history_same_hash_reopen_keyboard_and_back_forward(browser, kind):
    from playwright.sync_api import expect

    page, requests, errors, _ = mount_page(browser, kind)
    try:
        page.evaluate("history.replaceState({retained: 'other-owner-state'}, '')")
        quota = page.locator('a[href="#acq-quota"]')
        quota.focus()
        quota.press('Enter')
        expect(page.locator('#traffic')).to_be_visible()
        page.wait_for_function("location.hash === '#acq-quota'")
        length = page.evaluate('history.length')
        summary = page.locator('#acq-quota > summary')
        summary.focus()
        summary.press('Enter')
        expect(page.locator('#traffic')).not_to_be_visible()
        quota.click()
        expect(page.locator('#traffic')).to_be_visible()
        assert page.evaluate('history.length') == length
        assert page.evaluate('history.state') == {'retained': 'other-owner-state'}
        page.locator('a[href="#acq-catalog"]').click()
        expect(page.locator('#pipelines')).to_be_visible()
        page.go_back(wait_until='domcontentloaded')
        page.wait_for_function("location.hash === '#acq-quota'")
        expect(page.locator('#traffic')).to_be_visible()
        summary.focus()
        summary.press('Enter')
        expect(page.locator('#traffic')).not_to_be_visible()
        page.go_forward(wait_until='domcontentloaded')
        page.wait_for_function("location.hash === '#acq-catalog'")
        expect(page.locator('#pipelines')).to_be_visible()
        assert page.locator('#acq-quota').get_attribute('open') is None
        assert not errors and not any('/api/' in path for path in requests)
    finally:
        page.close()


@pytest.mark.parametrize("excluded", ["ctrlKey", "metaKey", "shiftKey", "altKey", "middle", "download", "new-tab", "cancelled", "foreign-base"])
def test_nonlocal_modified_and_cancelled_links_do_not_reveal_current_disclosure(browser, excluded):
    page, requests, errors, _ = mount_page(browser, "news")
    try:
        observed = page.evaluate("""excluded => {
          const link = document.querySelector('a[href="#acq-quota"]');
          const options = {bubbles:true,cancelable:true,button:0};
          if (excluded.endsWith('Key')) options[excluded] = true;
          if (excluded === 'middle') options.button = 1;
          if (excluded === 'download') link.download = 'export';
          if (excluded === 'new-tab') link.target = '_blank';
          if (excluded === 'foreign-base') {
            const base = document.createElement('base'); base.href = 'https://other.example.net/';
            document.head.prepend(base);
          }
          if (excluded === 'cancelled') document.addEventListener('click', event => event.preventDefault(), {capture:true,once:true});
          // Unit guard only: suppress the browser default *after* the real
          // component listener, so no test popup/download/network is created.
          document.addEventListener('click', event => event.preventDefault(), {once:true});
          link.dispatchEvent(new MouseEvent('click', options));
          return {open:document.getElementById('acq-quota').open,hash:location.hash};
        }""", excluded)
        assert observed == {'open': False, 'hash': ''}
        assert not errors and not any('/api/' in path for path in requests)
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
        version = re.search(r'dashboard-acquisition\.js\?v=(\d+)', html)
        assert version and int(version.group(1)) >= 3
        assert "dashboard-acquisition.css?v=4" in html
    # Strategy pages keep their domain-specific presentation.
    # Day-trade and overnight deliberately share the same HTML / renderer.
    for name in ["taifex_dashboard", "tw_day_trade_dashboard"]:
        assert "dashboard-acquisition.js" not in (ROOT / "services" / name / "index.html").read_text()
