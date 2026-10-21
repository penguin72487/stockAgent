from __future__ import annotations

from copy import deepcopy
from contextlib import nullcontext
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import repair_tw_day_trade_flat_opening as repair
import stockagent.live.tw_day_trade_dashboard as dashboard
from stockagent.backtest.tw_execution import TaiwanFeeSchedule
from stockagent.live.tw_day_trade_simulation import LiveEligibility, ModeSpec, TwDayTradeSimulationEngine


def _flat() -> dict:
    return {"engine_status": "flat_no_executable_signal", "positions": {},
            "pending_entry_orders": {}, "pending_reduction_orders": {},
            "entry_requested_shares": 0, "entry_filled_shares": 0,
            "initial_capital_twd": 10_000_000, "cumulative_realized_net_pnl_twd": 2_000_000,
            "total_equity_twd": 12_000_000, "session_sizing_nav_twd": 12_000_000,
            "session_date": "2026-10-07", "signal_id": "original-7",
            "entry_completed_at": "2026-10-07T09:00:03+08:00",
            "processed_signal_ids": ["original-6", "original-7"]}


@pytest.mark.parametrize("change", [
    {"positions": {"p": {"signed_shares": 0}}},
    {"entry_requested_shares": 1000}, {"entry_filled_shares": 1000},
    {"pending_entry_orders": {"p": {}}}, {"pending_reduction_orders": {"p": {}}},
    {"ledger_state_divergence": {"kind": "unknown"}}, {"engine_status": "active"},
])
def test_repair_refuses_existing_orders_fills_and_even_closed_history(change):
    with pytest.raises(RuntimeError):
        repair._require_empty_target({**_flat(), **change})


def test_missing_session_append_retains_prior_closed_history_without_rearming():
    mode = {**_flat(), "positions": {"closed": {"signed_shares": 0}},
            "entry_requested_shares": 1000, "entry_filled_shares": 1000}
    repair._require_missing_session_target(mode, date(2026, 10, 8))
    assert mode["positions"] == {"closed": {"signed_shares": 0}}
    with pytest.raises(RuntimeError):
        repair._require_empty_target(mode)


@pytest.mark.parametrize("change", [
    {"session_date": "2026-10-08"}, {"session_date": "2026-10-09"},
    {"positions": {"open": {"signed_shares": -1000}}},
    {"ledger_state_divergence": {"bad": True}},
    {"pending_entry_orders": {"pending": {}}},
    {"pending_reduction_orders": {"pending": {}}},
])
def test_missing_session_append_cannot_replace_current_or_bypass_inventory(change):
    with pytest.raises(RuntimeError):
        repair._require_missing_session_target({**_flat(), **change}, date(2026, 10, 8))


def test_outcome_keeps_economics_but_not_new_audit_identity():
    first = _flat()
    first["positions"] = {"p": {"symbol": "2330", "side": "short", "signed_shares": -1000,
                               "entry_price": 100, "entry_fee_twd": 300, "entry_order_id": "first"}}
    second = deepcopy(first)
    second["positions"]["p"]["entry_order_id"] = "second"
    assert repair._outcome(first) == repair._outcome(second)
    second["positions"]["p"]["entry_fee_twd"] += 1
    assert repair._outcome(first) != repair._outcome(second)


def _clock_failed_replay() -> dict:
    return {**_flat(), "entry_requested_shares": 4000, "entry_unfilled_shares": 4000,
            "simulation_replay": True, "counterfactual_open_replay": True,
            "entry_fill_contract": repair.REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
            "signal_reason_counts": {"quote_not_after_signal": 1, "below_one_board_lot": 2,
                                     "zero_target_weight": 2754}}


def test_original_signal_repair_accepts_only_the_explicit_never_filled_clock_failure():
    mode = _clock_failed_replay()
    repair._require_empty_target(mode, preserve_original_signal=True)
    with pytest.raises(RuntimeError):
        repair._require_empty_target(mode)


@pytest.mark.parametrize("change", [
    {"entry_filled_shares": 1000}, {"entry_unfilled_shares": 3000},
    {"simulation_replay": False}, {"counterfactual_open_replay": False},
    {"entry_fill_contract": "live"}, {"signal_reason_counts": {"price_limit_unavailable": 1}},
    {"positions": {"closed": {"signed_shares": 0}}},
    {"pending_entry_orders": {"order": {"remaining_shares": 4000}}},
])
def test_original_signal_repair_cannot_bypass_existing_fills_or_unrelated_source_failures(change):
    with pytest.raises(RuntimeError):
        repair._require_empty_target({**_clock_failed_replay(), **change}, preserve_original_signal=True)


def test_curves_require_all_270_right_labelled_minutes(tmp_path):
    observed = datetime(2026, 10, 6, 9, 1, tzinfo=repair.TAIPEI)
    item = {"day": observed.date(), "observed": observed, "end_at": time(13, 30)}
    rows = [{"market": "m", "minute": (observed+timedelta(minutes=i)).isoformat(timespec="minutes")}
            for i in range(270)]
    path = tmp_path / "marks.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in [*rows, rows[0]])+"\n")
    assert repair._curve_proof(path, "m", [item])[0]["unique_minute_points"] == 270
    path.write_text("\n".join(json.dumps(row) for row in rows[1:])+"\n")
    with pytest.raises(RuntimeError, match="269/270"):
        repair._curve_proof(path, "m", [item])


def test_changed_source_pin_is_rejected(tmp_path):
    path = tmp_path / "source"
    path.write_text("original")
    pins = [{"path": str(path), "sha256": repair._sha256(path)}]
    repair._assert_source_pins(pins)
    path.write_text("changed")
    with pytest.raises(RuntimeError, match="source changed"):
        repair._assert_source_pins(pins)


def test_interrupted_recovery_exchanges_only_after_candidate_validation_and_retains_failed_directory(tmp_path, monkeypatch):
    import hashlib

    live, operation = tmp_path / "live", tmp_path / "operation"
    live.mkdir(); operation.mkdir()
    before_mode = _flat()
    final_mode = {**_flat(), "session_date": "2026-10-08", "signal_id": "new", "total_equity_twd": 12_001_000}
    interrupted_mode = {**final_mode, "total_equity_twd": 11_999_000}
    other = {**_flat(), "signal_id": "other"}
    repair._atomic_json(live / "state.json", {"modes": {"m": interrupted_mode, "other": other}})
    repair._atomic_json(operation / "state_before_apply.json", {"modes": {"m": before_mode, "other": other}})
    repair._atomic_json(operation / "position_history_before_apply.json", {})
    names = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl")
    proofs = {}
    for name in names:
        source = live / name
        source.write_bytes(b"original\nfailed transaction\n")
        stat = source.stat()
        proofs[name] = dict(prefix_bytes=9, sha256=hashlib.sha256(b"original\n").hexdigest(),
                            device=stat.st_dev, inode=stat.st_ino)
    repair._atomic_json(operation / "journal_prefix_baseline.json", {"journals": proofs})
    manifest = dict(status="apply_interrupted_requires_audit", append_missing_session=True,
                    market="m", live_dir=str(live), source_pins=[], spec_sha256="same",
                    journal_prefix_bytes={name: 9 for name in names},
                    candidate_final_outcome=repair._outcome(final_mode))
    repair._atomic_json(operation / "ledger_repair_manifest.json", manifest)
    monkeypatch.setattr(repair, "control_lease", lambda **kw: nullcontext())
    monkeypatch.setattr(repair, "_spec", lambda *a: (SimpleNamespace(market="m"), None))
    monkeypatch.setattr(repair, "_spec_digest", lambda *a: "same")
    monkeypatch.setattr(repair, "_prepared_sessions", lambda *a, **kw: ([{
        "day": date(2026, 10, 8), "summary": {"signal_id": "new"}}], []))
    monkeypatch.setattr(repair, "_acquire_engine_lock", lambda *a: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(repair.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0))
    monkeypatch.setattr(repair, "_curve_proof", lambda *a: [])

    class Engine:
        def __init__(self, path, **kw):
            self.path, self.state = path, repair._object(path / "state.json")

    def execute(engine, *_a, **_kw):
        assert engine.state["modes"]["m"] == before_mode
        assert all((engine.path / name).read_bytes() == b"original\n" for name in names)
        engine.state["modes"]["m"] = final_mode
        repair._atomic_json(engine.path / "state.json", engine.state)
        return [{"signal_id": "new", "session_date": "2026-10-08"}]

    monkeypatch.setattr(repair, "TwDayTradeSimulationEngine", Engine)
    monkeypatch.setattr(repair, "_execute", execute)
    result = repair.recover_interrupted(SimpleNamespace(operation_root=operation, live_dir=live,
                                                       market="m", markets_dir=tmp_path))
    rollback = Path(result["atomic_recovery"]["rollback_directory"])
    assert result["status"] == "applied" and rollback.is_dir()
    assert repair._object(live / "state.json")["modes"]["m"] == final_mode
    assert repair._object(rollback / "state.json")["modes"]["m"] == interrupted_mode
    assert all((rollback / name).read_bytes() == b"original\nfailed transaction\n" for name in names)
    assert repair._object(live / "state.json")["modes"]["other"] == other


def test_read_only_fill_guard_never_constructs_engine(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("read-only prepare constructed live engine")
    monkeypatch.setattr(repair.TwDayTradeSimulationEngine, "__init__", forbidden)
    path = tmp_path / "fills.jsonl"
    path.write_text(json.dumps({"market": "m", "session_date": "2026-10-06", "quantity": 1000})+"\n")
    assert repair._has_live_fill(tmp_path, "m", "2026-10-06")
    assert not repair._has_live_fill(tmp_path, "other", "2026-10-06")


def test_handoff_bridge_fills_only_before_the_first_resumed_live_minute(tmp_path):
    rows = [{"market": "m", "session_date": "2026-10-07", "minute": minute,
             "recorded_at": recorded} for minute,recorded in [
        ("2026-10-07T09:01+08:00", "2026-10-07T09:01:00+08:00"),
        ("2026-10-07T12:46+08:00", "2026-10-07T12:46:00+08:00"),
        ("2026-10-07T12:48+08:00", "2026-10-07T12:48:50+08:00")]]
    path = tmp_path / "marks.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in rows)+"\n")
    manifest = {"market": "m", "current_capture": {"completed_through": "2026-10-07T12:46+08:00"},
                "applied_at": "2026-10-07T12:48:40+08:00", "journal_prefix_bytes": {"marks.jsonl": 0}}
    assert repair._handoff_gap_minutes(path, manifest) == [datetime(2026, 10, 7, 12, 47, tzinfo=repair.TAIPEI)]
    manifest["current_capture"]["completed_through"] = "2026-10-07T12:47+08:00"
    assert repair._handoff_gap_minutes(path, manifest) == []


def test_mark_bridge_inventory_guard_ignores_marks_but_not_fills():
    before = _flat()
    before["positions"] = {"p": {"symbol": "2330", "signed_shares": -1000, "entry_price": 100,
                                 "last_mark_price": 101, "realized_net_pnl_twd": 0}}
    after = deepcopy(before)
    after["positions"]["p"]["last_mark_price"] = 99
    assert repair._inventory_anchor(before) == repair._inventory_anchor(after)
    after["positions"]["p"]["signed_shares"] = 0
    assert repair._inventory_anchor(before) != repair._inventory_anchor(after)


@pytest.mark.parametrize("explicit_dates", [True, False])
def test_chart_keeps_replay_provenance_first_seen_after_inference_prefix(tmp_path, monkeypatch, explicit_dates):
    monkeypatch.setattr(dashboard, "_COLUMNAR_LEDGER_MIN_BYTES", 1)
    monkeypatch.delenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, raising=False)
    repair._atomic_json(tmp_path / "state.json", {"schema_version": repair.SIMULATION_SCHEMA_VERSION,
                        "enabled_markets": ["m"], "modes": {"m": {"market": "m", "initial_capital_twd": 10_000_000}}})
    base = {"session_date": "2026-10-06", "market": "m", "minute": "2026-10-06T09:01+08:00",
            "recorded_at": "2026-10-06T09:01:00+08:00", "initial_capital_twd": 10_000_000,
            "total_equity_twd": 9_999_900, "valuation_stale": False}
    latest = {**base, "historical_minute_replay": True, "minute_valuation_contract": "source-close-replay",
              "valuation_source": "pinned-minute-parquet", "valuation_executable": False,
              "fresh_trade_notional_coverage_ratio": 1.0}
    (tmp_path / "marks.jsonl").write_text("\n".join(json.dumps(row) for row in [*[base]*1001, latest])+"\n")
    kwargs = {"start_date": "2026-10-06", "end_date": "2026-10-06"} if explicit_dates else {}
    result = dashboard.build_dashboard_history_snapshot(state_dir=tmp_path, range_key="all", resolution="1m", **kwargs)
    assert result["historical_minute_replay_points"] == 1
    assert result["historical_minute_valuation_contracts"] == ["source-close-replay"]
    series = next(row for row in result["minute_series"] if row["series_id"] == "m")
    assert int(series["points"][0][3]) & 2 == 2


@pytest.mark.parametrize("weight", [0.025, 0.0001])
@pytest.mark.parametrize("strict_intraday", [False, True])
@pytest.mark.parametrize("postclose", [False, True])
@pytest.mark.parametrize("append_missing", [False, True])
def test_canonical_two_session_replay_keeps_capital_other_modes_and_curves(tmp_path, monkeypatch, weight, strict_intraday, postclose, append_missing):
    if append_missing and not postclose:
        pytest.skip("missing-session append is deliberately post-close only")
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 7, 14 if postclose else 11, 0, tzinfo=repair.TAIPEI)
    monkeypatch.setattr(repair, "datetime", FixedDateTime)
    monkeypatch.setattr(repair, "_close_quotes", lambda *a, **kw: ({}, {}))
    spec = ModeSpec(market="m", label="test", initial_capital_twd=10_000_000,
                    config_path="config.yaml", checkpoint_path="checkpoint.pt",
                    parquet_root=tmp_path / "stocks", live_output_dir=tmp_path / "signals",
                    fee_schedule=TaiwanFeeSchedule(commission_discount=0.2),
                    entry_fill_policy=repair.ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                    entry_price_offset_ticks=0, price_limit_offset_ticks=1,
                    strict_intraday=strict_intraday,
                    terminal_liquidation_unlimited_capacity=True)
    other = {"positions": {"untouched": {"signed_shares": 1000}}, "custom": "keep"}
    prior = {**_flat(), "terminal_curve_mark": ["2026-10-06", 12_000_000, 2_000_000, 0, 0, 0,
                                              "2026-10-06T13:30:00+08:00"]}
    if append_missing:
        prior["session_date"] = "2026-10-05"
    repair._atomic_json(tmp_path / "state.json", {"schema_version": repair.SIMULATION_SCHEMA_VERSION,
                                               "modes": {"m": prior, "other": other}})
    engine = TwDayTradeSimulationEngine(tmp_path, publication_clock=lambda: FixedDateTime.now())
    before_other = deepcopy(engine.state["modes"]["other"])
    sessions = []
    for day, end in [(date(2026, 10, 6), time(13, 30)), (date(2026, 10, 7), time(13, 30) if postclose else time(9, 2))]:
        observed = datetime.combine(day, time(9, 1), tzinfo=repair.TAIPEI)
        bars = {"2330": {}}
        for offset, close, high, volume in [(0, 102, 102, 2000), (1, 104, 104, 4000), (2, 110, 110, 10000)]:
            minute = observed + timedelta(minutes=offset)
            bars["2330"][minute.isoformat(timespec="minutes")] = {
                "open": close-1, "close": close, "high": high, "low": close-2,
                "volume_shares": volume, "vwap": close-1, "source": "pinned:test"}
        summary = {"signal_id": f"repair-{day}", "execution_mode": "tw_day_trade",
                   "signal_started_at": observed.replace(hour=9, minute=0, second=2).isoformat(),
                   "generated_at": "2026-10-07T11:00:00+08:00", "simulation_replay": True,
                   "historical_minute_valuation": True, "live_session_open_feature_applied": True,
                   "entry_fill_contract": repair.REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
                   "opening_repair_of_signal_id": "original-6" if day.day == 6 else "original-7",
                   "opening_model_inputs_ready": True}
        rows = [{"symbol": "2330", "target_weight": weight, "open_price": 100,
                 "tradable": True, "can_buy": True, "can_sell": True}]
        quotes = {"2330": {"symbol": "2330", "open": 100, "execution_price_0901": 101,
                            "execution_price_0901_method": "minute_vwap", "last": 102,
                            "quote_at": observed.isoformat(), "minute_volume_lots": 2,
                            "upper_limit": 110, "lower_limit": 90, "entry_price_source": "pinned:test"}}
        sessions.append({"day": day, "observed": observed, "summary": summary, "rows": rows,
                         "quotes": quotes, "eligibility": {"2330": LiveEligibility(
                             symbol="2330", venue="twse", security_type="stock", eligible=True,
                             short_open=True, covered=True, source_date=str(day))},
                         "coverage": {}, "bars": bars, "end_at": end, "rule_data_dir": tmp_path})
    if append_missing:
        monkeypatch.setattr(engine, "rearm_flat_session", lambda *a, **kw: pytest.fail("a new session must not rearm history"))
        accepted = [repair._execute(engine, spec, [item], reason=repair.MISSING_SESSION_CONTRACT)[0]
                    for item in sessions]
    else:
        accepted = repair._execute(engine, spec, sessions, reason=repair.ORIGINAL_SIGNAL_CONTRACT if postclose else repair.CONTRACT)
    shares = 3000 if weight == 0.025 else 0
    assert accepted[0]["entry_requested_shares"] == shares
    assert accepted[0]["entry_filled_shares"] == shares
    assert accepted[0]["open_position_count"] == 0
    assert accepted[1]["entry_filled_shares"] == shares
    assert (accepted[1]["cumulative_realized_net_pnl_twd"] > 2_000_000) == (shares > 0)
    assert engine.state["modes"]["other"] == before_other
    mode = engine.state["modes"]["m"]
    assert mode["source_signal_at"] == "2026-10-07T09:00:02+08:00"
    assert mode["opening_repair"]["recorded_at"] == FixedDateTime.now().isoformat()
    assert set(_flat()["processed_signal_ids"]) <= set(mode["processed_signal_ids"])
    assert [p["unique_minute_points"] for p in repair._curve_proof(engine.marks_path, "m", sessions)] == [270, 270 if postclose else 2]
    assert engine.state["updated_at"] == FixedDateTime.now().isoformat()
    assert mode["entry_post_0901_minute_price_fill_count"] == (1 if shares else 0)
    restarted = TwDayTradeSimulationEngine(tmp_path)
    assert repair._outcome(restarted.state["modes"]["m"]) == repair._outcome(mode)
    assert not restarted.state["modes"]["m"].get("ledger_state_divergence")


def test_apply_failure_restarts_owner_and_preserves_audit_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(repair, "control_lease", lambda **kw: nullcontext())
    mode = _flat()
    state = {"modes": {"m": mode, "other": {"positions": {"p": {"signed_shares": 1000}}}}}
    repair._atomic_json(tmp_path / "state.json", state)
    for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl"):
        (tmp_path / name).write_text("original\n")
    manifest = {"status": "candidate_validated_not_applied", "market": "m", "live_dir": str(tmp_path),
                "anchor": repair._anchor(mode), "source_pins": [], "sessions": [],
                "current_capture": {"completed_through": (datetime.now(repair.TAIPEI)-timedelta(minutes=1)).isoformat()}}
    repair._atomic_json(tmp_path / "ledger_repair_manifest.json", manifest)
    monkeypatch.setattr(repair, "_spec", lambda *args: (SimpleNamespace(market="m"), None))
    monkeypatch.setattr(repair, "_prepared_sessions", lambda *args, **kw: ([], []))
    class FakeEngine:
        def __init__(self, *args, **kwargs):
            self.state = deepcopy(state)
        def _session_has_fill(self, *args):
            return False
    monkeypatch.setattr(repair, "TwDayTradeSimulationEngine", FakeEngine)
    lock = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(repair, "_acquire_engine_lock", lambda *args: lock)
    def fail(*args, **kwargs):
        raise RuntimeError("injected replay failure")
    monkeypatch.setattr(repair, "_execute", fail)
    calls = []
    def service(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(repair.subprocess, "run", service)
    args = SimpleNamespace(operation_root=tmp_path, market="m", markets_dir=tmp_path, live_dir=tmp_path)
    with pytest.raises(RuntimeError, match="injected replay failure"):
        repair.apply(args)
    assert [call[1] for call in calls] == ["is-active", "stop", "start"]
    assert repair._object(tmp_path / "ledger_repair_manifest.json")["status"] == "apply_interrupted_requires_audit"
    assert repair._object(tmp_path / "state_before_apply.json") == state
    assert (tmp_path / "fills.jsonl").read_text() == "original\n"
    with pytest.raises(RuntimeError, match="inspect interrupted"):
        repair.apply(args)
