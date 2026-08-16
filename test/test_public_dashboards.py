from __future__ import annotations

from pathlib import Path
from http.client import HTTPConnection
import json
import re
from types import SimpleNamespace
import threading
import time

import pytest

from scripts.serve_public_dashboards import (
    InvalidPublicRequest,
    PublicRouteNotFound,
    PublicDashboardHandler,
    PublicDashboardServer,
    PublicTrafficObserver,
    build_public_overview,
    summarize_tw_status,
)
from stockagent.live.public_dashboards import (
    PUBLIC_MAX_EVENT_ROWS,
    TokenBucketRateLimiter,
    UnsafePublicDashboardPayload,
    sanitize_taifex_history,
    sanitize_taifex_status,
    sanitize_tw_events,
    sanitize_tw_history,
    sanitize_tw_signals,
    sanitize_tw_status,
)


def _keys(value: object) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            found.add(str(key))
            found.update(_keys(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_keys(item))
    return found


def test_public_projections_fail_closed_on_non_simulation_payload() -> None:
    with pytest.raises(UnsafePublicDashboardPayload):
        sanitize_taifex_status(
            {"simulation_only": False, "production_order_possible": False}
        )
    with pytest.raises(UnsafePublicDashboardPayload):
        sanitize_tw_status({"simulation_only": True, "production_order_possible": True})


def test_taifex_public_projection_removes_local_receipts() -> None:
    source = {
        "simulation_only": True,
        "production_order_possible": False,
        "sources": [{"name": "marks", "path": "marks.jsonl"}],
        "api_round_trip": {"result": "ok", "source_file": "private.json"},
        "active_cycle": {"cycle_id": "opaque", "status": "open"},
        "put_call_parity_tx": {
            "open_position": {"position_id": "private-position", "state": "open"}
        },
    }
    public = sanitize_taifex_status(source)
    assert public["sources"] == [{"name": "marks"}]
    assert public["api_round_trip"] == {"result": "ok"}
    assert public["active_cycle"] == {"status": "open"}
    assert "position_id" not in _keys(public)
    assert source["sources"][0]["path"] == "marks.jsonl"


def test_taifex_history_is_an_explicit_allowlist() -> None:
    public = sanitize_taifex_history(
        {
            "dashboard_schema_version": 6,
            "history": [
                {
                    "strategy_id": "a",
                    "total_equity_twd": 101.1234,
                    "fixed_capital_return": 0.01234567891,
                    "gross_cash_twd": 99.0,
                }
            ],
            "record_counts": {"marks": 1},
            "private": "drop",
        }
    )
    assert "private" not in public
    assert public["history"] == [
        {
            "strategy_id": "a",
            "total_equity_twd": 101.12,
            "fixed_capital_return": 0.01234568,
        }
    ]


def test_tw_public_projection_scrubs_ids_paths_errors_and_bounds_events() -> None:
    rows = [
        {
            "order_id": f"order-{index}",
            "position_id": f"position-{index}",
            "market": "tw",
        }
        for index in range(PUBLIC_MAX_EVENT_ROWS + 10)
    ]
    public = sanitize_tw_status(
        {
            "simulation_only": True,
            "production_order_possible": False,
            "modes": [
                {
                    "checkpoint_path": "/private/checkpoint.pt",
                    "config_path": "/private/config.yaml",
                    "readiness_error": "secret traceback",
                    "eligibility_coverage": {
                        "twse": {"covered": False, "path": "/private", "error": "x"}
                    },
                }
            ],
            "positions": [{"position_id": "p", "signal_id": "s", "symbol": "2330"}],
            "orders": rows,
            "fills": rows,
            "events": rows,
            "payload_window": {},
            "source_contract": {
                "preopen": "artifacts/private.json",
                "signal": "private.parquet",
            },
        }
    )
    forbidden = {
        "checkpoint_path",
        "config_path",
        "order_id",
        "position_id",
        "signal_id",
        "source_path",
        "path",
    }
    assert not (_keys(public) & forbidden)
    assert public["modes"][0]["readiness_error"] == "unavailable"
    assert public["orders"] == []
    assert public["fills"] == []
    assert public["events"] == []
    assert public["payload_window"]["orders"] == 0
    assert "artifacts/" not in public["source_contract"]["preopen"]


def test_tw_signal_projection_removes_internal_signal_id() -> None:
    public = sanitize_tw_signals(
        {
            "simulation_only": True,
            "production_order_possible": False,
            "private_runtime_state": "drop",
            "rows": [
                {
                    "signal_id": "private",
                    "symbol": "2330",
                    "bid": float("nan"),
                }
            ],
        }
    )
    assert public == {
        "simulation_only": True,
        "production_order_possible": False,
        "rows": [{"symbol": "2330", "bid": None}],
    }
    assert "private_runtime_state" not in public
    json.dumps(public, allow_nan=False)
    with pytest.raises(UnsafePublicDashboardPayload):
        sanitize_tw_signals(
            {"simulation_only": False, "production_order_possible": False}
        )


def test_tw_event_projection_enforces_simulation_and_scrubs_ids() -> None:
    public = sanitize_tw_events(
        {
            "simulation_only": True,
            "production_order_possible": False,
            "rows": [
                {
                    "order_id": "private-order",
                    "position_id": "private-position",
                    "symbol": "2330",
                    "price": float("nan"),
                }
            ],
        }
    )
    assert public["rows"] == [{"symbol": "2330", "price": None}]
    with pytest.raises(UnsafePublicDashboardPayload):
        sanitize_tw_events(
            {"simulation_only": False, "production_order_possible": False}
        )


def test_tw_history_projection_and_range_query_are_bounded() -> None:
    public = sanitize_tw_history(
        {
            "simulation_only": True,
            "production_order_possible": False,
            "range": "1y",
            "history": [
                {
                    "series_id": "benchmark_0050",
                    "series_type": "benchmark",
                    "minute": "2026-08-14T01:00+00:00",
                    "return_pct": 1.25,
                    "source_path": "/private/reference.parquet",
                }
            ],
            "private": "drop",
        }
    )
    assert public["range"] == "1y"
    assert public["history"] == [
        {
            "series_id": "benchmark_0050",
            "series_type": "benchmark",
            "minute": "2026-08-14T01:00+00:00",
            "return_pct": 1.25,
        }
    ]
    assert PublicDashboardHandler._history_range_query("range=all") == "all"
    with pytest.raises(ValueError):
        PublicDashboardHandler._history_range_query("range=5y")


def test_public_signal_query_accepts_dashboard_date_contract() -> None:
    normalized = PublicDashboardHandler._signal_query(
        "date=2026-08-13&mode=all&symbol=&status=all&offset=0&limit=250"
    )
    assert normalized == (
        "date=2026-08-13&mode=all&symbol=&status=all&offset=0&limit=250"
    )
    assert PublicDashboardHandler._date_query("date=2026-08-14") == "2026-08-14"
    with pytest.raises(ValueError):
        PublicDashboardHandler._date_query("date=2026-08-14&date=2026-08-13")

    events = PublicDashboardHandler._event_query(
        "date=2026-08-13&mode=all&symbol=&offset=250&limit=999"
    )
    assert events == "date=2026-08-13&mode=all&symbol=&offset=250&limit=250"
    with pytest.raises(ValueError):
        PublicDashboardHandler._event_query("date=2026-08-13&unknown=true")


def test_public_landing_exposes_live_safe_status_without_remote_assets() -> None:
    root = Path(__file__).resolve().parents[1] / "services" / "public_dashboards"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "public.js").read_text(encoding="utf-8")
    assert 'src="public.js?v=7"' in html
    assert 'id="taifex-health"' in html
    assert 'id="tw-health"' in html
    assert 'id="shioaji-health"' in html
    assert 'id="openbb-health"' in html
    assert 'id="data-health"' in html
    assert 'id="traffic-health"' in html
    assert "http://" not in html and "https://" not in html
    assert 'fetchJson("api/overview")' in javascript
    assert "taifex/api/status" not in javascript
    assert "tw-day-trade/api/status" not in javascript
    assert "shioaji/api/status" not in javascript
    assert '"流量保護"' in javascript
    assert "renderOpenbb(data.openbb || {})" in javascript
    assert "renderDataMonitor(data.data_monitor || {})" in javascript
    assert "renderTraffic(data.traffic || {})" in javascript
    assert "textContent" in javascript
    assert 'seconds == null || seconds === ""' in javascript


def test_public_overview_and_tw_summary_exclude_large_ledgers() -> None:
    tw = {
        "health": "active",
        "source_age_seconds": 1.5,
        "session_date": "2026-08-13",
        "modes": [{"market": "tw"}, {"market": "tw_cash"}],
        "execution_records": {"executed_count": 2, "mode_count": 2},
        "positions": [
            {"signed_shares": 1000, "valuation_stale": True},
            {"signed_shares": 0, "valuation_stale": False},
        ],
        "marks": [{"large": "payload"}],
        "orders": [{"large": "payload"}],
    }
    summary = summarize_tw_status(tw)
    assert summary["open_position_count"] == 1
    assert summary["stale_position_count"] == 1
    assert summary["execution_records"] == {"executed_count": 2, "mode_count": 2}
    assert not ({"positions", "marks", "orders"} & set(summary))

    overview = build_public_overview(
        {
            "health": "active",
            "source_age_seconds": 1,
            "strategy_counts": {"live_ideal": 53},
            "market": {"book_coverage_ratio": 0.9},
        },
        tw,
        {
            "health": "waiting",
            "traffic": {"used_ratio": 0.8, "safe_remaining_bytes": 123},
            "backfill": {
                "completed_contracts": 7,
                "inventory_contracts": 743,
                "progress_ratio": 0.25,
            },
            "pipeline_summary": {"total": 8},
        },
        {
            "health": "active",
            "snapshot_state": "stale",
            "source_age_seconds": 3000,
            "archive": {
                "completion_percent": 72.5,
                "accepted_tasks": 72,
                "total_tasks": 100,
                "success_rows": 1234,
            },
        },
        {},
        {
            "windows": {
                "1m": {
                    "requests": 41,
                    "requests_per_second": 2.5,
                    "latency_p95_ms": 8.0,
                }
            },
            "connections": {"in_flight": 3, "peak_in_flight": 57},
        },
    )
    assert overview["taifex"]["live_strategies"] == 53
    assert overview["tw"] == {
        "health": "active",
        "source_age_seconds": 1.5,
        "modes": 2,
        "open_positions": 1,
    }
    assert overview["shioaji"]["pipeline_total"] == 8
    assert overview["openbb"] == {
        "health": "active",
        "snapshot_state": "stale",
        "source_age_seconds": 3000,
        "completion_percent": 72.5,
        "accepted_tasks": 72,
        "total_tasks": 100,
        "success_rows": 1234,
    }
    assert overview["traffic"] == {
        "requests_1m": 41,
        "requests_per_second_1m": 2.5,
        "latency_p95_ms_1m": 8.0,
        "in_flight": 3,
        "peak_in_flight": 57,
    }
    encoded = json.dumps(overview)
    assert '"marks"' not in encoded
    assert '"orders"' not in encoded
    assert '"positions": [' not in encoded


def test_public_pages_share_visual_tokens() -> None:
    root = Path(__file__).resolve().parents[1] / "services"
    shared = (root / "public_dashboards" / "dashboard-core.css").read_text(
        encoding="utf-8"
    )
    assert "--dashboard-cyan" in shared
    assert "content-visibility: auto" in shared
    for relative in (
        "public_dashboards/index.html",
        "taifex_dashboard/index.html",
        "tw_day_trade_dashboard/index.html",
        "shioaji_api_dashboard/index.html",
        "openbb_archive_dashboard/index.html",
        "data_monitor_dashboard/index.html",
        "traffic_dashboard/index.html",
    ):
        html = (root / relative).read_text(encoding="utf-8")
        assert "dashboard-core.css?v=5" in html
        assert (
            'href="../data-monitor/">全資料</a>' in html
            or (
                relative == "data_monitor_dashboard/index.html"
                and 'href="./" aria-current="page">全資料</a>' in html
            )
            or relative == "public_dashboards/index.html"
        )
        assert '<meta name="theme-color" content="#071019">' in html
    for relative in (
        "taifex_dashboard/index.html",
        "tw_day_trade_dashboard/index.html",
        "openbb_archive_dashboard/index.html",
    ):
        html = (root / relative).read_text(encoding="utf-8")
        assert 'src="../time-axis.js?v=3"' in html

    for relative in (
        "public_dashboards/public.js",
        "taifex_dashboard/app.js",
        "tw_day_trade_dashboard/app.js",
        "shioaji_api_dashboard/app.js",
        "openbb_archive_dashboard/app.js",
        "data_monitor_dashboard/app.js",
    ):
        javascript = (root / relative).read_text(encoding="utf-8")
        assert "FETCH_TIMEOUT_MS = 15000" in javascript
        assert "AbortController" in javascript
    traffic_javascript = (root / "traffic_dashboard" / "app.js").read_text(
        encoding="utf-8"
    )
    assert "FETCH_TIMEOUT_MS = 5000" in traffic_javascript
    assert "AbortController" in traffic_javascript

    tw_javascript = (root / "tw_day_trade_dashboard" / "app.js").read_text(
        encoding="utf-8"
    )
    tw_html = (root / "tw_day_trade_dashboard" / "index.html").read_text(
        encoding="utf-8"
    )
    tw_styles = (root / "tw_day_trade_dashboard" / "styles.css").read_text(
        encoding="utf-8"
    )
    assert re.search(r"\bREFRESH_MS\b", tw_javascript) is None
    assert "signalLoadError" in tw_javascript
    assert "alert.textContent = `訊號分頁" not in tw_javascript
    assert 'id="benchmark-cards"' in tw_html
    assert "function renderBenchmarks" in tw_javascript
    assert "timeAxis.buildTimeAxis" in tw_javascript
    assert "TW_STOCK_SESSIONS" in tw_javascript
    assert "舊約 bid 與新約 ask 必須同時存在" in tw_javascript
    assert ".benchmark-grid" in tw_styles
    assert ".compact-table{table-layout:fixed;white-space:normal}" in tw_styles
    assert ".overview-kpis{grid-template-columns:repeat(6,minmax(0,1fr))}" in tw_styles
    assert ".legend .legend-toggle" in tw_styles
    assert ".legend-toggle.is-hidden" in tw_styles
    assert "open_net_liquidation_pnl_twd" in tw_javascript
    assert "reconciled_total_net_pnl_twd" in tw_javascript
    assert 'historical_session_complete: "歷史交易日已完成"' in tw_javascript
    assert "maximumSignificantDigits" not in tw_javascript
    assert "@media(max-width:700px)" in tw_styles
    for dashboard in ("tw_day_trade_dashboard", "shioaji_api_dashboard"):
        javascript = (root / dashboard / "app.js").read_text(encoding="utf-8")
        assert "style=" not in javascript
        assert ".style." not in javascript


def _test_server() -> PublicDashboardServer:
    root = Path(__file__).resolve().parents[1]
    return PublicDashboardServer(
        ("127.0.0.1", 0),
        public_static_root=root / "services/public_dashboards",
        taifex_static_root=root / "services/taifex_dashboard",
        tw_static_root=root / "services/tw_day_trade_dashboard",
        shioaji_static_root=root / "services/shioaji_api_dashboard",
        openbb_static_root=root / "services/openbb_archive_dashboard",
        data_monitor_static_root=root / "services/data_monitor_dashboard",
        traffic_static_root=root / "services/traffic_dashboard",
        repo_root=root,
        taifex_upstream="http://127.0.0.1:1",
        tw_upstream="http://127.0.0.1:1",
    )


def test_public_gateway_serves_shared_time_axis() -> None:
    server = _test_server()
    try:
        handler = SimpleNamespace(server=server)
        response = PublicDashboardHandler._static_response(handler, "/time-axis.js")
        assert response is not None
        assert response.content_type == "text/javascript; charset=utf-8"
        assert b"buildTimeAxis" in response.body
    finally:
        server.server_close()


def test_public_gateway_protocol_is_read_only_fail_closed_and_hardened() -> None:
    server = _test_server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", "/healthz")
        response = connection.getresponse()
        assert response.status == 200
        assert json.loads(response.read()) == {"health": "ok"}
        assert response.getheader("Content-Security-Policy") is not None
        assert "script-src-attr 'none'" in response.getheader("Content-Security-Policy")
        assert response.getheader("Origin-Agent-Cluster") == "?1"
        assert response.getheader("X-Permitted-Cross-Domain-Policies") == "none"

        connection.request("POST", "/", body=b"")
        response = connection.getresponse()
        assert response.status == 405
        assert response.getheader("Allow") == "GET, HEAD"
        assert json.loads(response.read()) == {"error": "method_not_allowed"}

        connection.request("GET", "/.env")
        response = connection.getresponse()
        assert response.status == 404
        assert json.loads(response.read()) == {"error": "not_found"}

        connection.request("GET", "/tw-day-trade/api/history?range=invalid")
        response = connection.getresponse()
        assert response.status == 400
        assert json.loads(response.read()) == {"error": "invalid_request"}

        server.data_monitor_status = lambda: (_ for _ in ()).throw(
            ValueError("private internal detail")
        )
        connection.request("GET", "/data-monitor/api/status")
        response = connection.getresponse()
        assert response.status == 503
        assert json.loads(response.read()) == {
            "health": "unavailable",
            "error": "temporarily_unavailable",
        }

        for _ in range(3):
            connection.request("GET", "/unknown/api/path")
            response = connection.getresponse()
            assert response.status == 404
            assert json.loads(response.read()) == {"error": "not_found"}

        connection.request("GET", "/traffic/api/status")
        response = connection.getresponse()
        assert response.status == 200
        traffic = json.loads(response.read())
        assert traffic["read_only"] is True
        assert traffic["production_control_possible"] is False
        assert traffic["limits"]["global_rate_limit_enabled"] is False
        assert traffic["limits"]["application_concurrency_limit_enabled"] is False
        assert response.getheader("Server-Timing") is not None
    finally:
        connection.close()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_caddy_and_gateway_security_policy_stay_aligned() -> None:
    root = Path(__file__).resolve().parents[1]
    gateway = (root / "scripts/serve_public_dashboards.py").read_text(encoding="utf-8")
    caddy = (root / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    launcher = (root / "scripts/run_public_dashboards.sh").read_text(encoding="utf-8")
    installer = (root / "scripts/install_public_dashboards_service.sh").read_text(
        encoding="utf-8"
    )
    unit = (root / "deploy/systemd/stockagent-public-dashboards.service.in").read_text(
        encoding="utf-8"
    )
    for token in (
        "Origin-Agent-Cluster",
        "X-Permitted-Cross-Domain-Policies",
        "script-src-attr 'none'",
        "style-src-attr 'none'",
        "frame-src 'none'",
    ):
        assert token in gateway
        assert token in caddy
    assert "max_header_size 16KB" in caddy
    assert "read_header 5s" in caddy
    assert "max_conns_per_host" not in caddy
    assert "@write_methods not method GET HEAD" in caddy
    assert 'header @write_methods Allow "GET, HEAD"' in caddy
    assert 'MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"' in launcher
    assert 'Environment="MALLOC_ARENA_MAX=2"' in unit
    assert "systemctl restart stockagent-public-dashboards.service" in installer


def test_expired_cache_returns_stale_while_refreshing_in_background() -> None:
    server = _test_server()
    calls = 0
    refreshed = threading.Event()

    def builder() -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls == 2:
            refreshed.set()
        return {"value": calls}

    try:
        first = server.cached_local_json(
            cache_key="swr",
            ttl_seconds=0.05,
            cache_control="public, max-age=0",
            builder=builder,
        )
        time.sleep(0.06)
        stale = server.cached_local_json(
            cache_key="swr",
            ttl_seconds=0.05,
            cache_control="public, max-age=0",
            builder=builder,
        )
        assert json.loads(first.body) == {"value": 1}
        assert stale.body == first.body
        assert refreshed.wait(1.0)
        deadline = time.monotonic() + 1.0
        while json.loads(server._cache["swr"].response.body)["value"] != 2:
            assert time.monotonic() < deadline
            time.sleep(0.01)
    finally:
        server.server_close()


def test_current_status_cache_does_not_serve_expired_freshness() -> None:
    server = _test_server()
    calls = 0

    def builder() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": calls}

    try:
        first = server.cached_local_json(
            cache_key="current",
            ttl_seconds=0.01,
            stale_grace_seconds=0,
            cache_control="no-store",
            builder=builder,
        )
        time.sleep(0.02)
        second = server.cached_local_json(
            cache_key="current",
            ttl_seconds=0.01,
            stale_grace_seconds=0,
            cache_control="no-store",
            builder=builder,
        )
        assert json.loads(first.body) == {"value": 1}
        assert json.loads(second.body) == {"value": 2}
    finally:
        server.server_close()


def test_unrelated_cache_keys_do_not_block_each_other() -> None:
    server = _test_server()
    slow_started = threading.Event()
    release_slow = threading.Event()
    fast_done = threading.Event()

    def slow_builder() -> dict[str, bool]:
        slow_started.set()
        assert release_slow.wait(1.0)
        return {"slow": True}

    def call_slow() -> None:
        server.cached_local_json(
            cache_key="slow",
            ttl_seconds=1,
            cache_control="no-store",
            builder=slow_builder,
        )

    def call_fast() -> None:
        server.cached_local_json(
            cache_key="fast",
            ttl_seconds=1,
            cache_control="no-store",
            builder=lambda: {"fast": True},
        )
        fast_done.set()

    slow_thread = threading.Thread(target=call_slow)
    fast_thread = threading.Thread(target=call_fast)
    try:
        slow_thread.start()
        assert slow_started.wait(0.5)
        fast_thread.start()
        assert fast_done.wait(0.5)
    finally:
        release_slow.set()
        slow_thread.join(timeout=1)
        fast_thread.join(timeout=1)
        server.server_close()


def test_same_cold_cache_key_is_built_once_under_concurrency() -> None:
    server = _test_server()
    calls = 0
    calls_lock = threading.Lock()
    responses: list[bytes] = []

    def builder() -> dict[str, int]:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return {"value": 1}

    def request() -> None:
        response = server.cached_local_json(
            cache_key="same-key",
            ttl_seconds=10.0,
            cache_control="no-store",
            builder=builder,
            stale_grace_seconds=0.0,
        )
        responses.append(response.body)

    workers = [threading.Thread(target=request) for _ in range(12)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=2)
        assert all(not worker.is_alive() for worker in workers)
        assert calls == 1
        assert len(responses) == len(workers)
        assert all(json.loads(body) == {"value": 1} for body in responses)
    finally:
        server.server_close()


def test_overview_cache_matches_one_minute_client_refresh_contract() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "scripts/serve_public_dashboards.py"
    ).read_text(encoding="utf-8")
    assert 'cache_key="public-overview"' in source
    assert "ttl_seconds=55.0" in source
    assert "ThreadPoolExecutor" in source
    assert "prewarm_overview" in source


def test_token_bucket_rate_limiter_refills_and_caps_client_table() -> None:
    limiter = TokenBucketRateLimiter(
        capacity=2.0, refill_per_second=1.0, maximum_clients=2
    )
    assert limiter.allow("a", now=0.0)
    assert limiter.allow("a", now=0.0)
    assert not limiter.allow("a", now=0.0)
    assert limiter.allow("a", now=1.0)
    assert limiter.allow("b", now=1.0)
    assert limiter.allow("c", now=1.0)
    assert len(limiter._buckets) == 2


def test_public_traffic_observer_is_bounded_anonymous_and_reconcilable() -> None:
    observer = PublicTrafficObserver()
    for path, status, latency_ms, size in (
        ("/", 200, 0.4, 100),
        ("/traffic/api/status", 200, 3.0, 200),
        ("/arbitrary/private-looking/path", 404, 17.0, 30),
    ):
        observed = observer.request_started(path)
        observer.request_finished(
            observed=observed,
            path=path,
            status=status,
            latency_ms=latency_ms,
            response_body_bytes=size,
        )
    observer.record_cache("static_build")
    observer.record_cache("static_hit")

    snapshot = observer.snapshot()
    minute = snapshot["windows"]["1m"]
    assert minute["requests"] == 3
    assert minute["response_body_bytes"] == 330
    assert minute["latency_p50_ms"] <= minute["latency_p95_ms"]
    assert minute["latency_p95_ms"] <= minute["latency_p99_ms"]
    assert minute["latency_p99_ms"] <= minute["latency_max_ms"]
    assert sum(row["requests"] for row in snapshot["routes"]) == 3
    assert {row["route"] for row in snapshot["routes"]} == {
        "/",
        "/traffic/api/status",
        "其他／未命中",
    }
    assert snapshot["cache"]["hit_ratio"] == 0.5
    encoded = json.dumps(snapshot, ensure_ascii=False).lower()
    assert "user_agent" not in _keys(snapshot)
    assert "user-agent" in snapshot["definitions"]["visitor"].lower()
    assert "127.0.0.1" not in encoded
    assert "private-looking" not in encoded


def test_public_gateway_has_no_global_rate_or_fixed_concurrency_ceiling() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "scripts/serve_public_dashboards.py").read_text(encoding="utf-8")
    caddy = (root / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    assert "HTTPStatus.TOO_MANY_REQUESTS" not in source
    assert "global_api_limiter" not in source
    assert "api_slots" not in source
    assert "max_conns_per_host" not in caddy

    server = _test_server()
    original_cached_static = server.cached_static
    all_requests_entered = threading.Barrier(48)

    def delayed_cached_static(*args: object, **kwargs: object):
        all_requests_entered.wait(timeout=5)
        return original_cached_static(*args, **kwargs)

    server.cached_static = delayed_cached_static  # type: ignore[method-assign]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    statuses: list[int] = []
    lock = threading.Lock()

    def request_static() -> None:
        connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        try:
            connection.request("GET", "/traffic/app.js")
            response = connection.getresponse()
            response.read()
            with lock:
                statuses.append(response.status)
        finally:
            connection.close()

    workers = [threading.Thread(target=request_static) for _ in range(48)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=8)
        assert all(not worker.is_alive() for worker in workers)
        assert statuses == [200] * 48
        assert server.traffic_observer.snapshot()["connections"]["peak_in_flight"] > 32
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_public_route_and_request_errors_are_distinct_from_internal_failures() -> None:
    with pytest.raises(InvalidPublicRequest):
        PublicDashboardHandler._signal_query("limit=not-a-number")
    with pytest.raises(InvalidPublicRequest):
        PublicDashboardHandler._history_range_query("range=unsupported")
    assert issubclass(PublicRouteNotFound, KeyError)
