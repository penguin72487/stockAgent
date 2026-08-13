from __future__ import annotations

from pathlib import Path

import pytest

from scripts.serve_public_dashboards import PublicDashboardHandler
from stockagent.live.public_dashboards import (
    PUBLIC_MAX_EVENT_ROWS,
    TokenBucketRateLimiter,
    UnsafePublicDashboardPayload,
    sanitize_taifex_history,
    sanitize_taifex_status,
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
    }
    public = sanitize_taifex_status(source)
    assert public["sources"] == [{"name": "marks"}]
    assert public["api_round_trip"] == {"result": "ok"}
    assert public["active_cycle"] == {"status": "open"}
    assert source["sources"][0]["path"] == "marks.jsonl"


def test_taifex_history_is_an_explicit_allowlist() -> None:
    public = sanitize_taifex_history(
        {
            "dashboard_schema_version": 6,
            "history": [{"strategy_id": "a"}],
            "record_counts": {"marks": 1},
            "private": "drop",
        }
    )
    assert "private" not in public
    assert public["history"] == [{"strategy_id": "a"}]


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
    assert len(public["orders"]) == PUBLIC_MAX_EVENT_ROWS
    assert public["payload_window"]["orders"] == PUBLIC_MAX_EVENT_ROWS
    assert "artifacts/" not in public["source_contract"]["preopen"]


def test_tw_signal_projection_removes_internal_signal_id() -> None:
    public = sanitize_tw_signals({"rows": [{"signal_id": "private", "symbol": "2330"}]})
    assert public == {"rows": [{"symbol": "2330"}]}


def test_public_signal_query_accepts_dashboard_five_field_contract() -> None:
    normalized = PublicDashboardHandler._signal_query(
        "mode=all&symbol=&status=all&offset=0&limit=250"
    )
    assert normalized == "mode=all&symbol=&status=all&offset=0&limit=250"


def test_public_landing_exposes_live_safe_status_without_remote_assets() -> None:
    root = Path(__file__).resolve().parents[1] / "services" / "public_dashboards"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "public.js").read_text(encoding="utf-8")
    assert 'src="public.js"' in html
    assert 'id="taifex-health"' in html
    assert 'id="tw-health"' in html
    assert 'id="shioaji-health"' in html
    assert "http://" not in html and "https://" not in html
    assert 'fetchJson("taifex/api/status")' in javascript
    assert 'fetchJson("tw-day-trade/api/status")' in javascript
    assert 'fetchJson("shioaji/api/status")' in javascript
    assert '"流量保護"' in javascript
    assert "textContent" in javascript


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
