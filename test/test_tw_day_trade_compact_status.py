from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import serve_public_dashboards as gateway
from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live.public_dashboards import sanitize_tw_status


NOW = datetime(2026, 8, 14, 2, 0, tzinfo=UTC)
SESSIONS = ("2026-08-13", "2026-08-14")


@pytest.fixture
def status_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Keep both the fixture and any derived indices away from live artifacts.
    monkeypatch.delenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", raising=False)
    root = tmp_path / "state"
    root.mkdir()
    state = {
        "enabled_markets": ["fixture"],
        "modes": {
            "fixture": {
                "market": "fixture",
                "label": "Fixture",
                "session_date": SESSIONS[-1],
                "initial_capital_twd": 1_000_000,
                "total_equity_twd": 1_000_010,
                "positions": {},
            }
        },
    }
    (root / "state.json").write_text(json.dumps(state), encoding="utf-8")
    (root / "status.json").write_text(
        json.dumps({"updated_at": NOW.isoformat(), "health": "degraded"}),
        encoding="utf-8",
    )
    streams: dict[str, list[dict]] = {
        name: [] for name in (
            "orders", "fills", "events", "marks", "benchmark_marks", "latency",
        )
    }
    for session in SESSIONS:
        history = root / "position_history" / session / "fixture.json"
        history.parent.mkdir(parents=True)
        history.write_text(
            json.dumps({"session_date": session, "positions": []}), encoding="utf-8",
        )
        common = {"market": "fixture", "session_date": session}
        for index in range(3):
            recorded = f"{session}T09:0{index + 1}:00+08:00"
            for name in ("orders", "fills"):
                streams[name].append({**common, "recorded_at": recorded, "row": index})
            streams["marks"].append({
                **common, "minute": recorded, "initial_capital_twd": 1_000_000,
                "total_equity_twd": 1_000_000 + index,
                "open_position_count": 0, "stale_position_count": 0,
            })
            streams["benchmark_marks"].append({
                **common, "benchmark_id": "fixture_benchmark", "minute": recorded,
                "initial_capital_twd": 1_000_000, "total_equity_twd": 1_000_000 + index,
            })
        streams["events"].append({
            **common, "recorded_at": f"{session}T09:01:00+08:00",
            "event": "signal_registered", "signal_id": f"signal-{session}",
            "entry_fill_count": 3,
        })
        streams["latency"].append({
            **common, "recorded_at": f"{session}T09:01:00+08:00",
            "result": "registered", "input_to_ledger_ms": 123.0,
            "signal_started_at": f"{session}T09:00:00+08:00",
            "signal_ready_at": f"{session}T09:00:00.100+08:00",
            "stages": {"model_inference_ms": 25.0},
        })
    for name, rows in streams.items():
        (root / f"{name}.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8",
        )
    return root


def _snapshot_options(root: Path, session: str) -> dict:
    return {
        "state_dir": root,
        "session_date": session,
        "now": NOW,
        "maximum_event_rows": 2,
        "maximum_mark_rows": 1,
        "include_position_rows": False,
        "include_ledger_session_dates": False,
        "unattended_guardian_path": root / "absent-guardian.json",
    }


@pytest.mark.parametrize("session", SESSIONS)
@pytest.mark.parametrize("include_ledger_dates", (False, True))
def test_order_fill_rows_default_is_backward_compatible(
    status_source: Path, session: str, include_ledger_dates: bool,
) -> None:
    options = _snapshot_options(status_source, session)
    options["include_ledger_session_dates"] = include_ledger_dates
    default = dashboard.build_dashboard_snapshot(**options)
    explicit = dashboard.build_dashboard_snapshot(**options, include_order_fill_rows=True)
    compact = dashboard.build_dashboard_snapshot(**options, include_order_fill_rows=False)
    assert default == explicit
    for name in ("orders", "fills"):
        assert [row["row"] for row in default[name]] == [1, 2]
        assert {row["session_date"] for row in default[name]} == {session}
        assert default["payload_window"][name] == 2
        assert compact[name] == []
        assert compact["payload_window"][name] == 0
    assert sanitize_tw_status(default) == sanitize_tw_status(compact)
    # Do not turn complete-history counts off merely to skip display rows.
    assert compact["record_counts"] == default["record_counts"]


@pytest.mark.parametrize("session", SESSIONS)
def test_compact_status_skips_order_fill_reads_but_keeps_all_facts(
    status_source: Path, session: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _snapshot_options(status_source, session)
    baseline = dashboard.build_dashboard_snapshot(**options)
    touched = set()

    def guard_reader(reader):
        def guarded(path, *args, **kwargs):
            assert path.name not in {"orders.jsonl", "fills.jsonl"}
            touched.add(path.name)
            return reader(path, *args, **kwargs)
        return guarded

    for name in (
        "_rows_for_sessions", "_latest_contiguous_session_rows",
        "_tail_for_session", "_tail",
    ):
        monkeypatch.setattr(dashboard, name, guard_reader(getattr(dashboard, name)))
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        assert path.name not in {"orders.jsonl", "fills.jsonl"}
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    compact = dashboard.build_dashboard_snapshot(**options, include_order_fill_rows=False)
    assert sanitize_tw_status(compact) == sanitize_tw_status(baseline)
    assert {"events.jsonl", "marks.jsonl", "benchmark_marks.jsonl", "latency.jsonl"} <= touched
    for name in (
        "events", "marks", "benchmark_marks", "latency", "today_latency",
        "opening_signal_latency", "health", "execution_records", "modes",
        "session_progress", "source_age_seconds", "available_session_dates",
    ):
        assert compact[name] == baseline[name]
    assert compact["events"]
    assert compact["marks"]
    assert compact["benchmark_marks"]
    assert compact["latency"]["sample_count"] == 1
    assert compact["today_latency"]["sample_count"] == 1
    assert compact["execution_records"]["executed_count"] == 1


@pytest.mark.parametrize("requested_session", (None, " 2026-08-13 "))
def test_gateway_tw_status_omits_only_order_fill_display_reads(
    tmp_path: Path, requested_session: str | None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def build_snapshot(**kwargs):
        calls.append(kwargs)
        return {"simulation_only": True, "production_order_possible": False}

    def build_response(**kwargs):
        return kwargs["builder"]()

    monkeypatch.setattr(gateway, "build_dashboard_snapshot", build_snapshot)
    # Call the real route without constructing a listening server or live cache.
    server = SimpleNamespace(
        repo_root=tmp_path,
        tw_revision=lambda: SimpleNamespace(body=b'{"revision_token":"fixed"}'),
        cached_local_json=build_response,
        _revision_stale_or_build=build_response,
    )
    payload = gateway.PublicDashboardServer.tw_status(server, requested_session)
    assert payload["service_sync"]["revision_token"] == "fixed"
    assert len(calls) == 1
    assert calls[0]["include_order_fill_rows"] is False
    assert calls[0]["include_position_rows"] is False
    assert calls[0]["include_ledger_session_dates"] is False
    assert calls[0]["maximum_event_rows"] == 500
    assert calls[0]["maximum_mark_rows"] == 32
    assert calls[0]["session_date"] == (
        requested_session.strip() if requested_session else None
    )
