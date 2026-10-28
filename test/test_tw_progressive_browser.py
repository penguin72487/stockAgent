"""Render candidate inventory/revision rows with the real shared styles.

Includes an isolated canonical-ledger/gateway/SSE/full-page fixture. Neither
fixture is a public-site, acquisition/model benchmark or opening-SLO proof.
"""
from dataclasses import replace
import json
from pathlib import Path
import threading
import time

import pytest

from scripts.probe_browser_runtime import browser_profile_launch_options


def test_six_mode_commits_reach_full_page_without_navigation(tmp_path, monkeypatch):
    """Exercise real DTOs, file notifications, SSE and the unmodified app.

    Prices are explicitly synthetic engineering books in a temporary ledger;
    public quote redaction remains enabled. Timings are retained for diagnosis,
    not asserted as a machine-independent performance promise.
    """
    playwright = pytest.importorskip("playwright.sync_api")
    from scripts import serve_public_dashboards as gateway
    from stockagent.live import tw_day_trade_dashboard as dashboard
    from stockagent.live.public_dashboards import public_paper_signal_key
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    from test_opening_signal_revision import register, revision_spec

    root = Path(__file__).resolve().parents[1]
    fixture_root = tmp_path / "fixture-repo"
    state_dir = fixture_root / "artifacts/live/tw_day_trade_simulation"
    engine = TwDayTradeSimulationEngine(state_dir)
    base = revision_spec(tmp_path)
    specs = [replace(base, market=f"fixture_{index}", label=f"工程測試模式 {index + 1}")
             for index in range(6)]
    previous = {spec.market: register(engine, spec, 0, .2) for spec in specs}
    clock = lambda *_args, **_kwargs: {"display_session_date": "2026-08-13"}
    monkeypatch.setattr(dashboard, "dashboard_session_clock", clock)
    monkeypatch.setattr(gateway, "dashboard_session_clock", clock)
    original_snapshot = gateway.build_dashboard_snapshot
    transient_source_errors = []

    def observed_snapshot(*args, **kwargs):
        try:
            return original_snapshot(*args, **kwargs)
        except OSError as error:
            # Preserve negative evidence without weakening the generation
            # check, injecting retry success or suppressing the gateway log.
            transient_source_errors.append({"type": type(error).__name__,
                                            "errno": error.errno, "message": str(error)})
            raise

    monkeypatch.setattr(gateway, "build_dashboard_snapshot", observed_snapshot)
    server = gateway.PublicDashboardServer(("127.0.0.1", 0),
        public_static_root=root / "services/public_dashboards",
        taifex_static_root=root / "services/taifex_dashboard",
        tw_static_root=root / "services/tw_day_trade_dashboard",
        shioaji_static_root=root / "services/shioaji_dashboard",
        openbb_static_root=root / "services/openbb_dashboard",
        data_monitor_static_root=root / "services/data_monitor_dashboard",
        traffic_static_root=root / "services/traffic_dashboard",
        repo_root=fixture_root, taifex_upstream="http://127.0.0.1:1",
        tw_upstream="http://127.0.0.1:1")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    errors, samples, status_requests, status_streams = [], [], [], []

    def wait_page(page, expression, arg=None):
        # Poll evaluate's browser-automation realm rather than asking
        # wait_for_function to eval source in the CSP-protected application.
        deadline = time.monotonic() + 15.
        while time.monotonic() < deadline:
            if page.evaluate(expression, arg):
                return
            page.wait_for_timeout(20)
        diagnostic = page.evaluate("""() => ({
            cards:[...document.querySelectorAll('#mode-cards .mode-card')].map(card => card.dataset.publicSignalKey),
            paints:window.fixturePaints,
            applied:typeof appliedStatusRevision === 'undefined' ? null : appliedStatusRevision,
            modes:typeof snapshot === 'undefined' ? null : snapshot?.modes?.map(mode => ({
                market:mode.market,key:mode.public_signal_key,revision:mode.opening_signal_revision})),
        })""")
        diagnostic["fresh_http_status"] = page.request.get(
            f"http://127.0.0.1:{server.server_port}/tw-day-trade/api/status").json()
        diagnostic["gateway_revision"] = json.loads(server.tw_revision().body)
        with server._cache_lock:
            diagnostic["gateway_status_cache_keys"] = [key for key in server._cache if key.startswith("tw-status:")]
            diagnostic["gateway_active_builds"] = dict(server._revision_active)
        (tmp_path / "progressive-six-mode-failure.json").write_text(
            json.dumps(diagnostic, ensure_ascii=False, indent=2), encoding="utf-8")
        page.screenshot(path=str(tmp_path / "progressive-six-mode-failure.png"), full_page=True)
        pytest.fail(f"full-page fixture timed out: {expression}; page errors: {errors}")

    try:
        with playwright.sync_playwright() as pw:
            browser = pw.chromium.launch(**browser_profile_launch_options("cpu-2d"))
            try:
                # Match the scheduled observer's disclosed 2K desktop viewport.
                page = browser.new_page(viewport={"width": 2560, "height": 1440})
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on("request", lambda request: status_requests.append(request.url)
                        if "/tw-day-trade/api/status" in request.url else None)
                page.on("response", lambda response: status_streams.append({
                    "url": response.url, "encoding": response.headers.get("content-encoding")})
                    if "/tw-day-trade/api/updates?status=1" in response.url else None)
                # Install before app boot: record the actual shared observer,
                # never invoke observer.notice/applied from the test itself.
                page.add_init_script("""window.fixturePaints = [];
                    window.fixtureNavigations = 0;
                    addEventListener('pageshow', () => window.fixtureNavigations++);
                    document.addEventListener('stockagent-performance-recorded', event => {
                        const metric = event.detail;
                        if (metric?.action === 'status_revision_paint') window.fixturePaints.push(metric);
                    });""")
                page.goto(f"http://127.0.0.1:{server.server_port}/tw-day-trade/",
                          wait_until="domcontentloaded")
                wait_page(page, "() => document.querySelectorAll('#mode-cards .mode-card').length === 6")
                page.locator("#mode-cards").scroll_into_view_if_needed()
                wait_page(page, "() => window.fixturePaints.length >= 6")
                initial_navigation_count = page.evaluate("window.fixtureNavigations")
                # Connection negotiation and initial load are not hidden in
                # the measured revision path. Let the stream settle first.
                page.wait_for_timeout(200)
                initial_status_requests = len(status_requests)
                for index, weight in ((1, .4), (2, .1), (3, 0.)):
                    started = time.perf_counter()
                    expected = {}
                    for spec in specs:
                        summary = register(engine, spec, index, weight, previous[spec.market])
                        previous[spec.market] = summary
                        expected[spec.market] = public_paper_signal_key({
                            "market": spec.market, "session_date": "2026-08-13",
                            "signal_id": summary["signal_id"]})
                    committed = time.perf_counter()
                    wait_page(page, """expected => {
                        const cards = [...document.querySelectorAll('#mode-cards .mode-card')];
                        return cards.length === 6 && Object.values(expected).every(key =>
                            cards.some(card => card.dataset.publicSignalKey === key));
                    }""", expected)
                    wait_page(page, """expected => Object.entries(expected).every(([market,key]) =>
                        window.fixturePaints.some(metric => metric.correlation?.market === market
                            && metric.correlation?.signalKey === key
                            && metric.correlation?.entryCountsVisible === true))""", expected)
                    painted = time.perf_counter()
                    samples.append({"revision": index, "target_weight": weight,
                        "six_mode_register_and_persist_ms": (committed - started) * 1000,
                        "last_commit_to_observed_paint_ms": (painted - committed) * 1000,
                        "first_register_to_observed_paint_ms": (painted - started) * 1000,
                        "signal_keys": expected})
                assert page.evaluate("window.fixtureNavigations") == initial_navigation_count == 1
                assert page.evaluate("document.documentElement.scrollWidth - innerWidth") <= 1
                page.screenshot(path=str(tmp_path / "progressive-six-mode-visible-cards.png"))
                page.screenshot(path=str(tmp_path / "progressive-six-mode-full-page.png"), full_page=True)
                metrics = page.evaluate("window.fixturePaints")
                assert all("status=1" not in url for url in status_requests)
                assert status_streams and all(stream["encoding"] == "gzip" for stream in status_streams)
                assert not errors
                assert all(metric["correlation"]["trigger"] != "initial_or_unmatched"
                           for metric in metrics if metric["correlation"]["signalKey"]
                           in set(samples[-1]["signal_keys"].values()))
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3.)
    (tmp_path / "progressive-six-mode-browser-timing.json").write_text(json.dumps({
        "boundary": "temporary_canonical_paper_ledger_to_loopback_public_gateway_sse_visible_status_paint",
        "real_source_or_model_timing": False, "native_opening_slo_proven": False,
        "public_quote_redaction_enabled": True, "samples": samples, "paint_metrics": metrics,
        "transient_source_errors": transient_source_errors,
        "initial_status_gets": initial_status_requests,
        "revision_status_gets": len(status_requests) - initial_status_requests,
        "status_streams": status_streams,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


def test_progressive_inventory_rows_fit_phone_tablet_laptop_and_desktop(tmp_path):
    playwright = pytest.importorskip("playwright.sync_api")
    root = Path(__file__).resolve().parents[1]
    with playwright.sync_playwright() as pw:
        browser = pw.chromium.launch(**browser_profile_launch_options("cpu-2d"))
        try:
            page = browser.new_page(viewport={"width": 1366, "height": 768})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.route("**/*", lambda route: route.abort())
            page.set_content('''<!doctype html><html lang="zh-Hant"><meta charset="utf-8">
                <meta name="viewport" content="width=device-width,initial-scale=1"><main>
                <div class="table-scroll" role="region" aria-label="所有訊號"><table class="compact-table signal-table">
                <thead><tr><th>時間／模式</th><th>股票／方向／結果</th><th>分數／持倉 %</th>
                <th>開盤計價／市價成交</th><th>現在市價／該檔盈虧</th><th>損益／模式總權益</th></tr></thead>
                <tbody id="signal-body"></tbody></table></div></main></html>''')
            for style in ("tw_day_trade_dashboard/styles.css", "public_dashboards/dashboard-core.css",
                          "public_dashboards/dashboard-responsive.css"):
                page.add_style_tag(path=str(root / "services" / style))
            for script in ("public_dashboards/dashboard-core.js", "tw_day_trade_dashboard/presentation.js",
                           "tw_day_trade_dashboard/detail-components.js"):
                page.add_script_tag(path=str(root / "services" / script))
            page.evaluate('''() => {
                const d = StockAgentDashboard;
                const n = value => value == null ? "—" : d.formatNumber(value, 2);
                const f = Object.fromEntries(["number","pct","money","sourceNumber","summaryMoney",
                    "displayPct","shortTime","replayTimingText","signalTimingText"].map(key => [key,n]));
                f.badge = text => `<span>${d.escapeHtml(text)}</span>`;
                f.pnlClass = () => "";
                f.signalReasonLabel = StockAgentTwPresentation.signalReasonLabel;
                const view = StockAgentTwDetailComponents.create({Dashboard:d,
                    strategyLabel: () => "逐版庫存測試模式", signalRowKey: row => String(row.target_weight),
                    resolvedPositionPnl: () => ({}), format:f});
                const row = {symbol:"2330", name:"台積電", side:"long", target_weight:.2,
                    requested_shares:0, filled_shares:0, sizing_open_price:1000, reduction_filled_shares:2000,
                    current_target_inventory:{signed_shares:2000,target_signed_shares:2000,signal_revision:3}};
                d.setTrustedHtml("signal-body", view.signalRow(row) + view.signalRow({...row,target_weight:0,
                    current_target_inventory:{...row.current_target_inventory,target_signed_shares:0}}));
                d.enhanceResponsiveTables();
            }''')
            assert page.get_by_text("零目標・減倉至零", exact=True).is_visible()
            assert page.get_by_text("本版無增倉成交；既有成交與費用另行保留", exact=True).count() == 2
            for name, width, height in (("phone", 390, 844), ("tablet", 820, 1180),
                                        ("laptop", 1366, 768), ("desktop", 2560, 1440)):
                page.set_viewport_size({"width": width, "height": height})
                page.evaluate("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))")
                overflow = page.evaluate('''() => ({page:document.documentElement.scrollWidth-innerWidth,
                    table:document.querySelector(".table-scroll").scrollWidth-document.querySelector(".table-scroll").clientWidth})''')
                assert overflow["page"] <= 1, (name, overflow)
                assert overflow["table"] <= 1, (name, overflow)
                page.screenshot(path=str(tmp_path / f"progressive-{name}.png"), full_page=True)
            assert not errors
        finally:
            browser.close()
