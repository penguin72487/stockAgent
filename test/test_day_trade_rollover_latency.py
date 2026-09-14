from datetime import datetime, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from scripts import run_tw_day_trade_simulation as runner
from stockagent.live import signal_engine
from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
from test_tw_day_trade_simulation import _spec


def at(text):
    return datetime.fromisoformat(text + "+08:00")


@pytest.fixture
def calendar(monkeypatch):
    dashboard._session_clock_cached.cache_clear()
    monkeypatch.setattr(dashboard, "verified_tw_stock_session_day", lambda day, **kw: (
        day.weekday() < 5 and day.isoformat() != "2026-09-10", "fixture verified calendar",
    ))
    yield
    dashboard._session_clock_cached.cache_clear()


@pytest.mark.parametrize("now,display,next_day", [
    ("2026-09-09T08:29:59", "2026-09-08", "2026-09-09"),
    ("2026-09-09T08:30:00", "2026-09-09", "2026-09-11"),
    ("2026-09-10T08:30:00", "2026-09-09", "2026-09-11"),
    ("2026-09-12T09:00:00", "2026-09-11", "2026-09-14"),
    ("2026-09-14T08:29:59", "2026-09-11", "2026-09-14"),
])
def test_verified_rollover_clock(calendar, now, display, next_day):
    clock = dashboard.dashboard_session_clock(at(now))
    assert clock["display_session_date"] == display
    assert clock["next_rollover_at"] == next_day + "T08:30:00+08:00"
    assert clock["calendar_verified"]
    assert not clock["readiness_implied"]


def test_unverified_calendar_does_not_invent_a_session(calendar, monkeypatch):
    monkeypatch.setattr(dashboard, "verified_tw_stock_session_day", lambda *a, **kw: (False, "official schedule is missing"))
    clock = dashboard.dashboard_session_clock(at("2026-09-09T08:30:00"))
    assert clock["display_session_date"] is None
    assert clock["next_rollover_at"] is None
    assert not clock["calendar_verified"]


def test_rollover_invalidates_revision_and_date_cache_without_writes(calendar, tmp_path):
    state = {"modes": {"a": {"session_date": "2026-09-08"}}}
    before = at("2026-09-09T08:29:59")
    after = at("2026-09-09T08:30:00")
    for observed, expected in [(before, ["2026-09-08"]), (after, ["2026-09-09", "2026-09-08"])]:
        assert dashboard._available_session_dates(root=tmp_path, state=state, observed=observed, include_preopen_session=True) == expected
    first = dashboard.build_dashboard_revision(state_dir=tmp_path, now=before)
    second = dashboard.build_dashboard_revision(state_dir=tmp_path, now=after)
    assert first["revision_token"] != second["revision_token"]
    assert first["state_revision"] == second["state_revision"]
    overnight_before = dashboard.build_dashboard_revision(
        state_dir=tmp_path,
        now=at("2026-09-09T12:59:59"),
        discord_markets_field="overnight_markets",
    )
    overnight_after = dashboard.build_dashboard_revision(
        state_dir=tmp_path,
        now=at("2026-09-09T13:00:00"),
        discord_markets_field="overnight_markets",
    )
    assert overnight_before["session_clock"]["display_session_date"] == "2026-09-08"
    assert overnight_after["session_clock"]["display_session_date"] == "2026-09-09"
    assert overnight_after["session_clock"]["rollover_local_time"] == "13:00"


def test_preopen_carries_account_not_previous_signal_or_fill(calendar, tmp_path):
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.update_readiness([spec], now=at("2026-09-08T08:30:00"))
    state = json.loads(engine.state_path.read_text())
    mode = state["modes"][spec.market]
    mode.update(session_date="2026-09-08", signal_id="yesterday", signal_at="2026-09-08T09:00:02+08:00", entry_fill_count=5, entry_filled_shares=5000, entry_requested_shares=5000, last_mark_at="2026-09-08T13:30:00+08:00", total_equity_twd=10_100_000)
    engine.state_path.write_text(json.dumps(state))
    before = engine.state_path.read_bytes()
    view = dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, now=at("2026-09-09T08:30:00"))
    row = view["modes"][0]
    assert view["session_date"] == row["session_date"] == "2026-09-09"
    assert row["signal_id"] is None
    assert row["entry_fill_count"] == row["entry_filled_shares"] == 0
    assert row["engine_status"] == "waiting_open"
    assert row["account_performance"]["session_date"] == "2026-09-08"
    assert row["account_performance"]["asof"] == "2026-09-08T13:30:00+08:00"
    assert row["account_performance"]["total_equity_twd"] == 10_100_000
    assert not view["signals"] and not view["positions"] and not view["marks"]
    assert engine.state_path.read_bytes() == before
    historical = dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, session_date="2026-09-08", now=at("2026-09-09T08:30:00"))
    assert historical["session_date"] == "2026-09-08"


def test_zero_default_batch_wait_and_detection_after_read(monkeypatch, tmp_path):
    from dataclasses import replace

    monkeypatch.delenv("STOCKAGENT_OPENING_SIGNAL_BATCH_WAIT_SECONDS", raising=False)
    assert runner._opening_batch_max_wait_seconds() == 0
    spec = _spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    observed = at("2026-09-09T09:00:01")
    elapsed = [0.0]

    def latest(mode, now):
        elapsed[0] += .01
        return ({"signal_id": "ready"}, [{"symbol": "2330"}]) if mode.market == spec.market else None

    class Watcher:
        def wait(self, timeout):
            pytest.fail("A ready mode must not wait for a missing model")

    monkeypatch.setattr(runner, "_latest_signal", latest)
    found, meta = runner._collect_opening_signal_batch(
        [spec, replace(spec, market="missing")], engine, observed,
        signal_watcher=Watcher(), max_wait_seconds=runner._opening_batch_max_wait_seconds(), cutoff_seconds=12,
        now_fn=lambda: observed + timedelta(seconds=elapsed[0]), monotonic_fn=lambda: elapsed[0],
    )
    assert set(found) == {spec.market}
    assert found[spec.market][2] == observed + timedelta(milliseconds=10)
    assert meta["wait_ms"] == 0
    assert not meta["complete"] and not meta["timed_out"]


@pytest.mark.parametrize("writer", ["_write_outputs", "_write_outputs_to_dir"])
def test_signal_sidecars_infer_sparse_prices_over_whole_universe(monkeypatch, tmp_path, writer):
    rows = [{"symbol": str(i), "price": None, "quantity": 0, "can_buy": False} for i in range(101)]
    rows.append({"symbol": "2330", "price": 39.84, "quantity": 1000, "can_buy": True})
    result = signal_engine.LiveSignalResult({}, rows, rows, rows, "message")
    monkeypatch.setattr(signal_engine, "_write_text_artifacts", lambda *args: None)
    args = (result, tmp_path, "2026-09-09") if writer == "_write_outputs" else (result, tmp_path)
    directory = Path(getattr(signal_engine, writer)(*args))
    for name in ("target_weights", "rebalance", "decision_explanations"):
        frame = pl.read_parquet(directory / f"{name}.parquet")
        assert frame.height == 102
        assert frame["price"][-1] == 39.84
        assert frame["price"].null_count() == 101
        assert frame.schema["quantity"] == pl.Int64
        assert frame.schema["can_buy"] == pl.Boolean


def test_executor_preserves_quote_receipt_on_later_discord_failure(tmp_path):
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    receipt = {"quality": "observed", "coverage_receipt_from_open_ms": 120.0, "coverage_to_signal_ready_ms": 30.0}
    engine.record_latency_sample(
        market="mode", signal_id="signal", result="registered",
        summary={"signal_started_at": "2026-09-09T09:00:00+08:00", "signal_ready_at": "2026-09-09T09:00:00.150+08:00", "artifact_published_at": "2026-09-09T09:00:00.160+08:00", "price_receipt_timing": receipt, "price_response_received_at": "2026-09-09T09:00:00.130+08:00", "live_latency": {"quote_transport": {"source": "fixture"}}},
        consumer_detected_at=at("2026-09-09T09:00:00.170"), ledger_persisted_at=at("2026-09-09T09:00:00.200"),
        executor_quote_fetch_ms=10, eligibility_load_ms=1, ledger_compute_persist_ms=20,
    )
    row = json.loads(engine.latency_path.read_text().splitlines()[-1])
    assert row["price_receipt_timing"] == receipt
    assert row["quote_transport"] == {"source": "fixture"}
    assert row["stages"]["artifact_discovery_ms"] == 10


@pytest.mark.parametrize("limit", [1, 3, 17, 100])
def test_bounded_reverse_spans_match_full_scan_tail(monkeypatch, tmp_path, limit):
    path = tmp_path / "signals.jsonl"
    rows = [{"session_date": f"2026-09-0{1 + i % 3}", "symbol": str(i), "name": "測試" * 20000} for i in range(35)]
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n")
    monkeypatch.setattr(dashboard, "_columnar_ledger_frame", lambda *a, **kw: None)
    selected = ["2026-09-01", "2026-09-03"]
    expected = [row for row in rows if row["session_date"] in selected][-limit:]
    actual = dashboard._rows_for_sessions(path, selected, limit)
    for day in selected:
        assert list(actual.get(day, ())) == [row for row in expected if row["session_date"] == day]


def test_detail_date_enumeration_never_loads_benchmark_curve(monkeypatch, tmp_path):
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"a": {"session_date": "2026-09-09"}}}))
    (tmp_path / "signals.jsonl").write_text(json.dumps({"session_date": "2026-09-09", "symbol": "2330"}) + "\n")
    monkeypatch.setattr(dashboard, "_benchmark_history_index", lambda *a: pytest.fail("detail filters must not decode benchmark history"))
    for builder in (dashboard.build_dashboard_signal_page, dashboard.build_dashboard_position_page, dashboard.build_dashboard_event_page):
        builder(state_dir=tmp_path, start_date="2026-09-09", end_date="2026-09-09")
