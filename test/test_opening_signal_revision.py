"""Progressive decisions are immutable; inventory changes by actual deltas."""
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json

import numpy as np
import pytest

from stockagent.live.market_config import (
    OPENING_PROVISIONAL_REFERENCE_POLICY, SIGNAL_UPDATE_ONCE, SIGNAL_UPDATE_PROGRESSIVE,
)
from stockagent.live.opening_signal_revision import (
    opening_input_fingerprint, opening_source_revision, revision_admission, revision_fields,
)
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE, ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE, TwDayTradeSimulationEngine,
)
from test_tw_day_trade_simulation import (
    _spec, _row, _quote, _eligibility, _provisional_summary, _now,
)


def revision_spec(tmp_path):
    return replace(_spec(tmp_path), strict_intraday=True,
        entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET,
        opening_reference_policy=OPENING_PROVISIONAL_REFERENCE_POLICY,
        signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)


def revision_summary(index, at, previous=None):
    summary = _provisional_summary() | {"signal_id": f"revision-{index}",
        "generated_at": at.isoformat(timespec="microseconds"),
        "signal_ready_at": at.isoformat(timespec="microseconds")}
    return summary | revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE,
        signal_id=summary["signal_id"], input_fingerprint=hashlib.sha256(str(index).encode()).hexdigest(),
        previous=previous)


def book(at, **changes):
    return _quote(bid=1000., ask=1000., last=1000.) | {
        "quote_at": at.isoformat(timespec="microseconds"),
        "book_exchange_at": at.isoformat(timespec="microseconds"),
        "simtrade": False, "source": "shioaji_stock_quote_stream",
    } | changes


def register(engine, spec, index, weight, previous=None, *, quote=None, at=None, rows=None):
    at = at or _now(9, 0, 5) + timedelta(seconds=index)
    summary = revision_summary(index, at, previous)
    result = engine.register_signal(spec=spec, summary=summary,
        signal_rows=rows if rows is not None else [_row(weight)],
        quotes={"2330": book(at + timedelta(microseconds=1)) if quote is None else quote},
        eligibility=_eligibility(), eligibility_coverage={}, now=at + timedelta(milliseconds=1))
    assert result == "registered", (result, engine.state["modes"][spec.market].get("blocked_reason"))
    return summary


def ledger(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def test_new_session_live_signal_does_not_inherit_previous_replay_proof(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    mode = engine._mode(spec)
    mode.update(session_date="2026-08-12", entry_completed_at="2026-08-12T09:01:00+08:00",
                simulation_replay=True,
                published_revision_replay={"source_signal_id": "previous-day-replay",
                                           "publication_upper_bound": "2026-08-12T09:00:10+08:00"})
    register(engine, spec, 0, 0.0)
    current = engine.state["modes"][spec.market]
    assert current["session_date"] == "2026-08-13" and current["simulation_replay"] is False
    assert "published_revision_replay" not in current
    restored = TwDayTradeSimulationEngine(tmp_path / "state")
    assert "published_revision_replay" not in restored.state["modes"][spec.market]


def test_initial_opening_publication_proof_survives_later_revisions(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first_at = _now(9, 0, 5)
    proof = {"kind": "execution_pointer_written_at", "at": first_at.isoformat()}
    first = revision_summary(0, first_at) | {
        "opening_entry_clock_contract": "initial_pointer_publication_before_0901_v2",
        "initial_opening_signal_id": "revision-0",
        "initial_opening_publication_proof": proof,
    }
    assert engine.register_signal(spec=spec, summary=first, signal_rows=[_row(.4)],
        quotes={"2330": book(first_at + timedelta(microseconds=1))},
        eligibility=_eligibility(), eligibility_coverage={},
        now=first_at + timedelta(milliseconds=1)) == "registered"
    register(engine, spec, 1, .2, previous=first)
    mode = engine.state["modes"][spec.market]
    assert mode["initial_opening_signal_id"] == "revision-0"
    assert mode["initial_opening_publication_proof"] == proof
    assert ledger(engine.signals_path)[-1]["initial_opening_publication_proof"] == proof
    assert json.loads(engine.service_sync_path.read_text())["modes"][spec.market][
        "initial_opening_publication_proof"] == proof


@pytest.mark.parametrize("hour,minute", [(13, 20), (22, 20)])
def test_close_cache_cannot_replace_intraday_signal_or_consume_ledger(tmp_path, hour, minute):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    mode = engine.state["modes"][spec.market]
    prior_id = mode["signal_id"]
    prior_processed = list(mode.get("processed_signal_ids") or ())
    prior_events = engine.events_path.read_bytes()
    prior_fills = engine.fills_path.read_bytes()
    # A completed-session cache is not a progressive opening signal.  The
    # old order of checks blocked it and overwrote the genuine signal ID.
    after = _now(hour, minute)
    close_cache = {**first, "signal_id": "postclose-cache",
                   "day_trade_signal_update_policy": SIGNAL_UPDATE_ONCE}
    assert engine.register_signal(spec=spec, summary=close_cache, signal_rows=[_row(.2)],
        quotes={}, eligibility=_eligibility(), eligibility_coverage={}, now=after) == "ignored_after_entry_cutoff"
    assert mode["signal_id"] == prior_id
    assert list(mode.get("processed_signal_ids") or ()) == prior_processed
    assert engine.events_path.read_bytes() == prior_events
    assert engine.fills_path.read_bytes() == prior_fills


def _replayed_initial_with_revision_lineage(tmp_path):
    spec = revision_spec(tmp_path)
    replay_spec = replace(spec, signal_update_policy=SIGNAL_UPDATE_ONCE,
                          entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    initial = revision_summary(0, _now(9, 1, 10))
    replay = initial | {
        "day_trade_signal_update_policy": SIGNAL_UPDATE_ONCE,
        "opening_signal_revision": None,
        "missed_opening_original_signal_revision": initial["opening_signal_revision"],
        "missed_opening_original_signal_update_policy": SIGNAL_UPDATE_PROGRESSIVE,
        "missed_opening_original_policy_fingerprint": initial["day_trade_signal_update_policy_fingerprint"],
        "missed_opening_recovery": True,
        "simulation_replay": True,
        "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
        "replay_basis": "source_backed_test_counterfactual",
    }
    quote = _quote(minute_volume_lots=100.) | {
        "execution_price_0901": 1000., "quote_at": _now(9, 1).isoformat()}
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert engine.register_signal(spec=replay_spec, summary=replay,
        signal_rows=[dict(_row(.4), open_price=1000.)], quotes={"2330": quote},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1),
        counterfactual_open_replay=True) == "registered"
    assert inventory(engine, spec) == 4000
    return engine, spec, initial


def test_replayed_open_can_continue_with_later_causal_revision_after_restart(tmp_path):
    engine, spec, initial = _replayed_initial_with_revision_lineage(tmp_path)
    origin = engine.state["modes"][spec.market]["session_open_replay_origin"]
    assert origin["signal_id"] == initial["signal_id"]
    assert origin["open_reconstructed_at"] == _now(9, 1).isoformat()
    original_fill = ledger(engine.fills_path)[0]
    assert original_fill["simulation_replay"] is True
    # A quote after model-ready but before the pointer was observed cannot
    # execute the revision or its inventory reduction.
    ready = _now(9, 2, 10)
    pointer = ready + timedelta(microseconds=500000)
    revision = revision_summary(1, ready, initial) | {
        "execution_pointer_observed_at": pointer.isoformat()}
    assert engine.register_signal(spec=spec, summary=revision, signal_rows=[_row(.2)],
        quotes={"2330": book(ready + timedelta(microseconds=250000))},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 2, 11)) == "registered"
    assert inventory(engine, spec) == 4000 and len(ledger(engine.fills_path)) == 1
    mode = engine.state["modes"][spec.market]
    assert mode["simulation_replay"] is False
    assert mode["session_open_replay_origin"] == origin
    assert mode["pending_reduction_shares"] == 2000
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine._mode(spec)  # Readiness must not replace the current retry policy.
    assert engine.state["modes"][spec.market]["entry_fill_policy"] == ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET
    engine.process_quotes(quotes={"2330": book(_now(9, 2, 12))}, now=_now(9, 2, 12))
    fills = ledger(engine.fills_path)
    assert inventory(engine, spec) == 2000 and len(fills) == 2
    assert fills[0] == original_fill and fills[1]["simulation_replay"] is False
    assert engine.state["modes"][spec.market]["session_open_replay_origin"] == origin
    later = revision_summary(2, _now(9, 2, 13), revision) | {
        "execution_pointer_observed_at": _now(9, 2, 13).isoformat()}
    assert engine.register_signal(spec=spec, summary=later, signal_rows=[_row(0.)],
        quotes={"2330": book(_now(9, 2, 14))}, eligibility=_eligibility(),
        eligibility_coverage={}, now=_now(9, 2, 14)) == "registered"
    assert inventory(engine, spec) == 0 and ledger(engine.fills_path)[0] == original_fill
    assert engine.state["modes"][spec.market]["session_open_replay_origin"] == origin


def test_replayed_open_with_missing_lineage_waits_without_consuming_revision(tmp_path):
    engine, spec, initial = _replayed_initial_with_revision_lineage(tmp_path)
    mode = engine.state["modes"][spec.market]
    mode.pop("session_open_replay_origin")
    at = _now(9, 2, 10)
    revision = revision_summary(1, at, initial) | {"execution_pointer_observed_at": at.isoformat()}
    assert engine.register_signal(spec=spec, summary=revision, signal_rows=[_row(.2)],
        quotes={"2330": book(_now(9, 2, 11))}, eligibility=_eligibility(),
        eligibility_coverage={}, now=_now(9, 2, 11)) == "waiting_replay_lineage"
    assert inventory(engine, spec) == 4000 and revision["signal_id"] not in mode["processed_signal_ids"]
    assert mode["engine_status"] == "waiting_replay_lineage"


def test_replayed_open_revision_requires_observed_pointer(tmp_path):
    engine, spec, initial = _replayed_initial_with_revision_lineage(tmp_path)
    revision = revision_summary(1, _now(9, 2, 10), initial)
    assert engine.register_signal(spec=spec, summary=revision, signal_rows=[_row(.2)],
        quotes={"2330": book(_now(9, 2, 11))}, eligibility=_eligibility(),
        eligibility_coverage={}, now=_now(9, 2, 11)) == "waiting_quote"
    assert inventory(engine, spec) == 4000
    assert engine.state["modes"][spec.market]["pending_wait_reason"] == "live_revision_pointer_observation_required"


@pytest.mark.parametrize("damage", ["fingerprint", "root", "policy_fingerprint"])
def test_replayed_open_with_corrupted_lineage_fails_closed(tmp_path, damage):
    engine, spec, initial = _replayed_initial_with_revision_lineage(tmp_path)
    origin = engine.state["modes"][spec.market]["session_open_replay_origin"]
    if damage == "root":
        origin["revision"]["root_signal_id"] = "foreign"
    else:
        origin[damage] = "corrupt"
    at = _now(9, 2, 10)
    revision = revision_summary(1, at, initial) | {"execution_pointer_observed_at": at.isoformat()}
    assert engine.register_signal(spec=spec, summary=revision, signal_rows=[_row(.2)],
        quotes={"2330": book(_now(9, 2, 11))}, eligibility=_eligibility(),
        eligibility_coverage={}, now=_now(9, 2, 11)) == "waiting_replay_lineage"
    assert inventory(engine, spec) == 4000 and len(ledger(engine.fills_path)) == 1


def test_source_revision_ignores_receipt_churn_but_not_opening_facts(tmp_path):
    path = tmp_path / "opening.json"
    session = "2026-09-23"
    row = {"available": True, "open_prices": 100.}
    payload = {"session_date": session, "requested_symbols": ["2330", "0050"],
        "rows": {"2330": row}, "official_trading_universe": {"fingerprint": "scope-1"}}
    from datetime import datetime
    row["timestamp_ms"] = int(datetime.fromisoformat(session + "T09:00:00+08:00").timestamp() * 1000)
    def save():
        path.write_text(json.dumps(payload))
        return opening_source_revision(path, session_date=session)
    first = save()
    row.update(price=101., bid_prices=100., ask_prices=102., volumes=100,
        timestamp_ms=row["timestamp_ms"] + 1000, no_open_print_observed_ms=0)
    payload["captured_at_taipei"] = "later"
    payload["requested_symbols"].reverse()
    payload["rows"]["outside-axis"] = dict(row, open_prices=999.)
    assert save() == first
    row["open_prices"] = 101.
    changed = save()
    assert changed != first
    payload["rows"]["0050"] = dict(row, open_prices=100.)
    assert save() not in {first, changed}
    payload["official_trading_universe"]["fingerprint"] = "scope-2"
    assert save() != first


def test_source_revision_retries_invalid_reads_and_fences_retained_mtime(tmp_path):
    import os
    from datetime import datetime
    path = tmp_path / "opening.json"
    session = "2026-09-23"
    assert opening_source_revision(path, session_date=session) is None
    path.write_text("{")
    with pytest.raises(ValueError):
        opening_source_revision(path, session_date=session)
    payload = {"session_date": session, "requested_symbols": ["2330"], "rows": {"2330": {
        "available": True, "open_prices": 100.,
        "timestamp_ms": int(datetime.fromisoformat(session + "T09:00:00+08:00").timestamp() * 1000)}}}
    path.write_text(json.dumps(payload))
    first = opening_source_revision(path, session_date=session)
    info = path.stat()
    payload["rows"]["2330"]["open_prices"] = 101.
    path.write_text(json.dumps(payload))
    os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
    assert path.stat().st_size == info.st_size
    assert opening_source_revision(path, session_date=session) != first
    with pytest.raises(ValueError, match="another session"):
        opening_source_revision(path, session_date="2026-09-24")


@pytest.mark.parametrize("damaged", [[], None, 1, "invalid", {"session_date": "2026-09-23",
    "requested_symbols": ["2330"], "rows": {}, "official_trading_universe": ["invalid"]},
    {"session_date": "2026-09-23", "requested_symbols": [], "rows": {}}])
def test_invalid_source_shape_remains_a_retryable_value_error(tmp_path, damaged):
    path = tmp_path / "opening.json"
    path.write_text(json.dumps(damaged))
    with pytest.raises(ValueError):
        opening_source_revision(path, session_date="2026-09-23")
    # A bad source is not cached as accepted or fatal to the observer task.
    path.write_text(json.dumps({"session_date": "2026-09-23",
        "requested_symbols": ["2330"], "rows": {}}))
    assert opening_source_revision(path, session_date="2026-09-23")


def inventory(engine, spec):
    return sum(int(p["signed_shares"]) for p in engine.state["modes"][spec.market]["positions"].values())


def test_six_mode_financial_oracle_is_repeatable_with_explicit_fixture_clock(tmp_path, monkeypatch):
    from scripts.benchmark_opening_signal_pipeline import _new_isolated_entry_engine, _paper_ledger_financial_oracle
    from stockagent.live import tw_day_trade_simulation as simulation
    original_iso = simulation._iso
    sequences = []
    for run in range(2):
        monkeypatch.setattr(simulation, "_iso", lambda value=None, _run=run:
            original_iso(value) if value is not None else f"2026-10-25T12:00:0{_run}+08:00")
        root = tmp_path / f"independent-{run}"
        root.mkdir()
        engine = _new_isolated_entry_engine(root / "ledger", created_at=_now(8, 50))
        base_spec = revision_spec(root)
        previous, proofs = {}, []
        for index, weight in enumerate((.4, .2, 0.)):
            for mode in range(6):
                spec = replace(base_spec, market=f"fixture_{mode}")
                previous[mode] = register(engine, spec, index, weight, previous.get(mode))
                proofs.append(_paper_ledger_financial_oracle(engine))
        assert proofs[-1]["journals"]["signals.jsonl"]["records"] == 18
        assert proofs[-1]["journals"]["fills.jsonl"]["records"] > 0
        sequences.append(proofs)
    assert sequences[0] == sequences[1]


@pytest.mark.parametrize("change", ["append", "replace", "rewrite"])
def test_strict_snapshot_rejects_journals_ahead_of_committed_state(tmp_path, change):
    from stockagent.live.tw_day_trade_dashboard import build_dashboard_snapshot, DashboardSnapshotChanged
    engine, spec = TwDayTradeSimulationEngine(tmp_path / "state"), revision_spec(tmp_path)
    register(engine, spec, 0, .2)
    kwargs = dict(state_dir=engine.state_dir, now=_now(9, 0, 8), require_committed_revision=True)
    assert build_dashboard_snapshot(**kwargs)["modes"]
    path = engine.orders_path
    body = path.read_bytes()
    if change == "append":
        with path.open("ab") as handle:
            handle.write(b'{"uncommitted":true}\n')
    elif change == "replace":
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(body)
        replacement.replace(path)
    else:
        import os
        old = path.stat()
        path.write_bytes(body)
        os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns))
    with pytest.raises(DashboardSnapshotChanged, match="journals"):
        build_dashboard_snapshot(**kwargs)


def test_strict_snapshot_rejects_append_during_projection_and_commit_retries(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_dashboard as dashboard
    engine, spec = TwDayTradeSimulationEngine(tmp_path / "state"), revision_spec(tmp_path)
    register(engine, spec, 0, .2)
    original = dashboard._available_session_dates

    def concurrent(*args, **kwargs):
        result = original(*args, **kwargs)
        with engine.events_path.open("ab") as handle:
            handle.write(b'{"event":"uncommitted"}\n')
        return result

    monkeypatch.setattr(dashboard, "_available_session_dates", concurrent)
    with pytest.raises(dashboard.DashboardSnapshotChanged):
        dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, now=_now(9, 0, 8), require_committed_revision=True)
    monkeypatch.setattr(dashboard, "_available_session_dates", original)
    engine._persist(_now(9, 0, 9))
    assert dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, now=_now(9, 0, 10), require_committed_revision=True)["modes"]


@pytest.mark.parametrize("change_phase", ["telemetry", "financial_projection"])
def test_opening_telemetry_sample_and_financial_commit_have_separate_boundaries(
    tmp_path, monkeypatch, change_phase,
):
    from stockagent.live import tw_day_trade_dashboard as dashboard
    engine, spec = TwDayTradeSimulationEngine(tmp_path / "state"), revision_spec(tmp_path)
    register(engine, spec, 0, .2)
    original = dashboard._tail
    calls = []

    def concurrent_tail(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path.name == "opening_signal_latency.jsonl":
            calls.append(path.name)
            if change_phase == "telemetry":
                engine._persist(_now(9, 0, 9))
        return result

    monkeypatch.setattr(dashboard, "_tail", concurrent_tail)
    if change_phase == "financial_projection":
        original_projection = dashboard._available_session_dates

        def concurrent_projection(*args, **kwargs):
            result = original_projection(*args, **kwargs)
            engine._persist(_now(9, 0, 9))
            return result

        monkeypatch.setattr(dashboard, "_available_session_dates", concurrent_projection)
    kwargs = dict(state_dir=engine.state_dir, now=_now(9, 0, 10), require_committed_revision=True)
    if change_phase == "telemetry":
        snapshot = dashboard.build_dashboard_snapshot(**kwargs)
        assert snapshot["service_sync"]["state_revision"] == engine.state["state_revision"]
    else:
        with pytest.raises(dashboard.DashboardSnapshotChanged, match="commit"):
            dashboard.build_dashboard_snapshot(**kwargs)
    assert len(calls) == 1


def test_opening_telemetry_appended_after_sample_is_visible_on_next_snapshot(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_dashboard as dashboard
    engine, spec = TwDayTradeSimulationEngine(tmp_path / "state"), revision_spec(tmp_path)
    register(engine, spec, 0, .2)
    path = engine.state_dir / "opening_signal_latency.jsonl"
    path.write_text('{"example":1}\n', encoding="utf-8")
    original, reads = dashboard._tail, []

    def concurrent_tail(source, *args, **kwargs):
        result = original(source, *args, **kwargs)
        if source == path:
            reads.append(result)
            if len(reads) == 1:
                with path.open("a", encoding="utf-8") as handle:
                    handle.write('{"example":2}\n')
        return result

    monkeypatch.setattr(dashboard, "_tail", concurrent_tail)
    dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, now=_now(9, 0, 10),
                                       require_committed_revision=True)
    assert reads == [[{"example": 1}]]
    dashboard.build_dashboard_snapshot(state_dir=engine.state_dir, now=_now(9, 0, 11),
                                       require_committed_revision=True)
    assert reads == [[{"example": 1}], [{"example": 1}, {"example": 2}]]


def test_revision_hash_ignores_receipt_book_and_last_tick_churn():
    stamp = int(_now(9, 0, 1).timestamp() * 1000)
    snapshot = PriceSnapshot(prices=np.array([100., 200.]), source="tw_mis",
        available_mask=np.ones(2, dtype=bool), open_prices=np.array([100., np.nan]),
        timestamps_ms=np.array([stamp, stamp]))
    first = opening_input_fingerprint("axis/checkpoint/reference", snapshot, session_date="2026-08-13")
    snapshot.prices[:] = [101., 201.]
    snapshot.bid_prices = np.array([100., 200.])
    snapshot.timestamp = "receipt-only"
    assert opening_input_fingerprint("axis/checkpoint/reference", snapshot, session_date="2026-08-13") == first
    snapshot.open_prices[1] = 200.
    assert opening_input_fingerprint("axis/checkpoint/reference", snapshot, session_date="2026-08-13") != first
    assert opening_input_fingerprint("other-checkpoint", snapshot, session_date="2026-08-13") != first


def test_reduce_and_retain_fifo_basis_brackets_fees_and_closed_lifecycle(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    immutable = {k: position[k] for k in ("entry_price", "entry_at", "signal_id", "target_weight", "take_profit_price", "stop_trigger_price")}
    assert inventory(engine, spec) == 4000
    second = register(engine, spec, 1, .2, first)
    assert inventory(engine, spec) == 2000
    assert {k: position[k] for k in immutable} == immutable
    assert position["current_target_signal_id"] == second["signal_id"]
    assert position["current_target_weight"] == .2
    assert [row["quantity"] for row in ledger(engine.fills_path)] == [4000, 2000]
    assert all(row["fee_and_tax_twd"] > 0 for row in ledger(engine.fills_path))
    before = engine.fills_path.read_bytes()
    third = register(engine, spec, 2, .2, second)
    assert engine.fills_path.read_bytes() == before
    register(engine, spec, 3, 0., third)
    assert inventory(engine, spec) == 0
    assert len(engine.state["modes"][spec.market]["positions"]) == 1
    assert position["signal_id"] == first["signal_id"] and position["signed_shares"] == 0
    assert len(ledger(engine.signals_path)) == 4


def test_reversal_waits_for_fifo_reduction_and_retries_causally(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    second_at = _now(9, 0, 6)
    register(engine, spec, 1, -.2, first, at=second_at,
        quote=book(second_at + timedelta(microseconds=1), bid_volume=0.))
    mode = engine.state["modes"][spec.market]
    assert inventory(engine, spec) == 4000
    assert mode["pending_reduction_shares"] == 4000
    assert mode["pending_entry_shares"] == 2000
    assert len(ledger(engine.fills_path)) == 1
    # Restart must continue the same frozen target, not issue another full order.
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 7))}, now=_now(9, 0, 7))
    assert inventory(engine, spec) == -2000
    assert [row["quantity"] for row in ledger(engine.fills_path)] == [4000, 4000, 2000]
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 8))}, now=_now(9, 0, 8))
    assert engine.fills_path.read_bytes() == before


def test_subsecond_causality_and_superseded_pending_targets(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    at = _now(9, 0, 5).replace(microsecond=500000)
    first = register(engine, spec, 0, .4, at=at, quote=book(at - timedelta(microseconds=1)))
    assert inventory(engine, spec) == 0
    second = register(engine, spec, 1, .2, first, at=at + timedelta(milliseconds=1))
    assert inventory(engine, spec) == 2000
    assert len(ledger(engine.fills_path)) == 1
    assert any(row["status"] == "cancelled_signal_revision" for row in ledger(engine.orders_path))
    mode = engine.state["modes"][spec.market]
    assert mode["opening_signal_revision"]["index"] == 1
    assert mode["signal_at"].endswith("501000+08:00")
    assert mode["signal_id"] == second["signal_id"]


@pytest.mark.parametrize("damage", ["index", "root", "checkpoint", "fingerprint", "policy"])
def test_old_foreign_or_incompatible_revisions_do_not_trade(tmp_path, damage):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    bad = revision_summary(1, _now(9, 0, 6), first)
    if damage == "index":
        bad["opening_signal_revision"]["index"] = 0
    elif damage == "root":
        bad["opening_signal_revision"]["root_signal_id"] = "foreign"
    elif damage == "checkpoint":
        bad["checkpoint_fingerprint"] = "new-model"
    elif damage == "fingerprint":
        bad["opening_signal_revision"]["input_fingerprint"] = first["opening_signal_revision"]["input_fingerprint"]
    else:
        bad["day_trade_signal_update_policy"] = SIGNAL_UPDATE_ONCE
    before = engine.fills_path.read_bytes()
    result = engine.register_signal(spec=spec, summary=bad, signal_rows=[_row(-.4)],
        quotes={"2330": book(_now(9, 0, 7))}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 0, 7))
    assert result.startswith("blocked")
    assert engine.fills_path.read_bytes() == before and inventory(engine, spec) == 4000


def test_reduction_failure_is_quarantined_in_process_and_after_restart(tmp_path, monkeypatch):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    def interrupted(*args, **kwargs):
        raise OSError("fixture reduction interruption")
    monkeypatch.setattr(engine, "_reconcile_margin_targets", interrupted)
    with pytest.raises(OSError, match="interruption"):
        register(engine, spec, 1, -.2, first)
    assert engine.state["modes"][spec.market]["ledger_state_divergence"]
    restarted = TwDayTradeSimulationEngine(tmp_path / "state")
    assert restarted.state["modes"][spec.market]["ledger_state_divergence"]
    assert inventory(restarted, spec) == 4000 and len(ledger(engine.fills_path)) == 1


def test_policy_requires_explicit_causal_strict_paper_contract(tmp_path):
    with pytest.raises(ValueError, match="causal full-target"):
        replace(_spec(tmp_path), signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)
    with pytest.raises(ValueError, match="unchanged"):
        previous = revision_summary(0, _now(9, 0, 5))
        revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id="other",
            input_fingerprint=previous["opening_signal_revision"]["input_fingerprint"], previous=previous)


def historical_revision_summary(index, published, previous=None):
    from stockagent.live.tw_day_trade_simulation import PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT
    return revision_summary(index, published, previous) | {
        "simulation_replay": True, "historical_minute_valuation": True,
        "simulation_only": True, "production_order_possible": False,
        "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
        "published_revision_replay": {"contract": PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT,
            "plan_sha256": "a" * 64, "source_signal_id": f"original-{index}",
            "publication_upper_bound": published.isoformat()}}


def late_historical_revision_summary(index, published, previous=None, *, first_published=None):
    from stockagent.live.tw_day_trade_simulation import PUBLISHED_REVISION_LATE_OPEN_REPLAY_CONTRACT
    first_published = first_published or published
    summary = historical_revision_summary(index, published, previous)
    source = f"original-{index}"
    summary.update(signal_started_at=published.isoformat(), artifact_published_at=published.isoformat(),
                   execution_pointer_written_at=published.isoformat(), opening_repair_of_signal_id=source,
                   original_weights_preserved=True, counterfactual_signal_regeneration=False)
    summary["published_revision_replay"] = {
        "contract": PUBLISHED_REVISION_LATE_OPEN_REPLAY_CONTRACT, "plan_sha256": "a" * 64,
        "source_signal_id": source, "publication_upper_bound": published.isoformat(),
        "first_entry_is_counterfactual": True, "first_source_signal_id": "original-0",
        "first_publication_upper_bound": first_published.isoformat(),
        "original_revision": {"index": index, "root_signal_id": "original-0"}}
    return summary


def test_late_first_publication_is_explicit_counterfactual_and_later_revisions_stay_causal(tmp_path):
    spec = replace(revision_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_progressive_revision_replay=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first_at = _now(10, 2, 15)
    initial = late_historical_revision_summary(0, first_at)
    quote = _quote(minute_volume_lots=100.) | {"execution_price_0901": 1000.,
        "execution_price_0901_method": "minute_vwap", "quote_at": _now(9, 1).isoformat()}
    assert engine.register_signal(spec=spec, summary=initial, signal_rows=[_row(.4)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1),
        counterfactual_open_replay=True) == "registered"
    mode = engine.state["modes"][spec.market]
    assert inventory(engine, spec) == 4000
    assert datetime.fromisoformat(mode["source_signal_at"]) == first_at
    assert mode["published_revision_replay"]["first_entry_is_counterfactual"] is True
    assert mode["published_revision_replay"]["first_publication_upper_bound"] == first_at.isoformat()
    assert ledger(engine.fills_path)[0]["simulation_replay"] is True
    later = late_historical_revision_summary(1, _now(10, 2, 30), initial, first_published=first_at)
    assert engine.register_signal(spec=spec, summary=later, signal_rows=[dict(_row(.2), open_price=1000.)],
        quotes={}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(10, 4),
        counterfactual_open_replay=True) == "registered"
    assert mode["pending_reduction_orders"]["2330"]["remaining_shares"] == 2000


@pytest.mark.parametrize("damage", ["old_contract", "not_counterfactual", "not_original_weights",
    "regenerated", "wrong_root", "wrong_revision", "wrong_source", "naive_publication",
    "wrong_publication", "after_entry_cutoff", "live_order", "malformed_proof"])
def test_late_first_publication_rejects_unproven_or_live_counterfactual(tmp_path, damage):
    from stockagent.live.tw_day_trade_simulation import PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT
    spec = replace(revision_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_progressive_revision_replay=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = late_historical_revision_summary(0, _now(10, 2, 15))
    proof = first["published_revision_replay"]
    if damage == "old_contract": proof["contract"] = PUBLISHED_REVISION_MINUTE_REPLAY_CONTRACT
    elif damage == "not_counterfactual": proof["first_entry_is_counterfactual"] = False
    elif damage == "not_original_weights": first["original_weights_preserved"] = False
    elif damage == "regenerated": first["counterfactual_signal_regeneration"] = True
    elif damage == "wrong_root": proof["first_source_signal_id"] = "other"
    elif damage == "wrong_revision": proof["original_revision"]["index"] = 1
    elif damage == "wrong_source": first["opening_repair_of_signal_id"] = "other"
    elif damage == "naive_publication": proof["first_publication_upper_bound"] = "2026-08-13T10:02:15"
    elif damage == "wrong_publication": first["artifact_published_at"] = _now(10, 3).isoformat()
    elif damage == "after_entry_cutoff": proof["first_publication_upper_bound"] = _now(13, 20).isoformat()
    elif damage == "live_order": first["production_order_possible"] = True
    elif damage == "malformed_proof": first["published_revision_replay"] = ["invalid"]
    result = engine.register_signal(spec=spec, summary=first, signal_rows=[_row(.4)], quotes={},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1), counterfactual_open_replay=True)
    assert result != "registered"
    assert ledger(engine.fills_path) == []


@pytest.mark.parametrize("damage", ["too_recent", "wrong_root", "wrong_first_clock", "foreign_plan"])
def test_late_counterfactual_later_revision_requires_same_lineage_and_full_minute(tmp_path, damage):
    spec = replace(revision_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_progressive_revision_replay=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first_at = _now(10, 2, 15)
    initial = late_historical_revision_summary(0, first_at)
    kwargs = dict(spec=spec, quotes={}, eligibility=_eligibility(), eligibility_coverage={},
                  signal_rows=[_row(.4)], counterfactual_open_replay=True)
    assert engine.register_signal(**kwargs, summary=initial, now=_now(9, 1)) == "registered"
    later = late_historical_revision_summary(1, _now(10, 2, 30), initial, first_published=first_at)
    proof = later["published_revision_replay"]
    if damage == "too_recent":
        proof["publication_upper_bound"] = _now(10, 3, 1).isoformat()
        later["artifact_published_at"] = later["execution_pointer_written_at"] = proof["publication_upper_bound"]
    elif damage == "wrong_root": proof["first_source_signal_id"] = "other"
    elif damage == "wrong_first_clock": proof["first_publication_upper_bound"] = _now(10, 2, 14).isoformat()
    elif damage == "foreign_plan": proof["plan_sha256"] = "b" * 64
    assert engine.register_signal(**kwargs, summary=later, now=_now(10, 4)) != "registered"
    assert ledger(engine.fills_path) == []


def test_completed_minute_revision_replay_is_opt_in_and_preserves_fifo(tmp_path):
    from scripts.rebuild_tw_day_trade_open_price_replay import _historical_pending_order_quotes
    spec = replace(revision_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_progressive_revision_replay=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    initial = historical_revision_summary(0, _now(9, 0, 10))
    quote = _quote(minute_volume_lots=100.) | {"execution_price_0901": 1000.,
        "execution_price_0901_method": "minute_vwap", "quote_at": _now(9, 1).isoformat()}
    assert engine.register_signal(spec=spec, summary=initial, signal_rows=[_row(.4)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 1),
        counterfactual_open_replay=True) == "registered"
    assert inventory(engine, spec) == 4000
    original_fill = ledger(engine.fills_path)[0]
    later = historical_revision_summary(1, _now(9, 1, 10), initial)
    assert engine.register_signal(spec=spec, summary=later, signal_rows=[dict(_row(.2), open_price=1000.)], quotes={},
        eligibility=_eligibility(), eligibility_coverage={}, now=_now(9, 3),
        counterfactual_open_replay=True) == "registered"
    mode = engine.state["modes"][spec.market]
    assert inventory(engine, spec) == 4000 and mode["pending_reduction_orders"]["2330"]["remaining_shares"] == 2000
    bars = {"2330": {_now(9, 3).isoformat(timespec="minutes"): {
        "open": 1000., "high": 1001., "low": 999., "close": 1000.,
        "volume_shares": 100000., "Amount": 100000000., "vwap": 1000.}}}
    quotes = _historical_pending_order_quotes(mode, bars, observed=_now(9, 3))
    engine._retry_historical_minute_reduction_orders(mode, quotes, _now(9, 3))
    assert inventory(engine, spec) == 2000
    assert ledger(engine.fills_path)[0] == original_fill
    assert all(fill["simulation_replay"] is True for fill in ledger(engine.fills_path))


@pytest.mark.parametrize("damage", ["too_recent", "foreign_plan", "not_simulation", "no_opt_in"])
def test_historical_revision_replay_rejects_unproven_or_overlapping_clocks(tmp_path, damage):
    spec = replace(revision_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_progressive_revision_replay=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    initial = historical_revision_summary(0, _now(9, 0, 10))
    kwargs = dict(spec=spec, quotes={}, eligibility=_eligibility(), eligibility_coverage={},
                  signal_rows=[_row(.4)], counterfactual_open_replay=True)
    assert engine.register_signal(**kwargs, summary=initial, now=_now(9, 1)) == "registered"
    later = historical_revision_summary(1, _now(9, 1, 10), initial)
    if damage == "too_recent":
        later["published_revision_replay"]["publication_upper_bound"] = _now(9, 2, 1).isoformat()
    elif damage == "foreign_plan":
        later["published_revision_replay"]["plan_sha256"] = "b" * 64
    elif damage == "not_simulation":
        kwargs["counterfactual_open_replay"] = False
    else:
        kwargs["spec"] = replace(spec, historical_progressive_revision_replay=False,
                                 entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET)
    assert engine.register_signal(**kwargs, summary=later, now=_now(9, 3)) != "registered"
    assert ledger(engine.fills_path) == []


def test_skipped_superseded_revisions_are_admitted_but_same_input_is_not():
    first = revision_summary(0, _now(9, 0, 5))
    second = revision_summary(1, _now(9, 0, 6), first)
    third = revision_summary(2, _now(9, 0, 7), second)
    mode = first | {"session_date": "2026-08-13", "entry_completed_at": _now(9, 0, 5).isoformat()}
    assert revision_admission(policy=SIGNAL_UPDATE_PROGRESSIVE, summary=third,
        mode=mode, session_date="2026-08-13") == (True, None)


def test_addition_has_its_own_cohort_and_future_reduction_is_fifo(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .2)
    second = register(engine, spec, 1, .4, first)
    assert inventory(engine, spec) == 4000
    positions = list(engine.state["modes"][spec.market]["positions"].values())
    assert len(positions) == 2 and positions[0]["signal_id"] != positions[1]["signal_id"]
    register(engine, spec, 2, .1, second)
    assert inventory(engine, spec) == 1000
    assert [p["signed_shares"] for p in positions] == [0, 1000]
    assert [f["quantity"] for f in ledger(engine.fills_path)] == [2000, 2000, 2000, 1000]


def test_many_reversals_release_principal_but_never_erase_actual_fees(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    previous = None
    for index in range(8):
        weight = .5 if index % 2 == 0 else -.5
        previous = register(engine, spec, index, weight, previous)
        assert inventory(engine, spec) == (5000 if weight > 0 else -5000)
    mode = engine.state["modes"][spec.market]
    assert mode["cumulative_realized_net_pnl_twd"] < 0
    assert len(mode["positions"]) == 8
    assert len(ledger(engine.fills_path)) == 15


def test_zero_target_liquidates_even_without_current_entry_eligibility(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    summary = revision_summary(1, _now(9, 0, 6), first)
    assert engine.register_signal(spec=spec, summary=summary, signal_rows=[],
        quotes={"2330": book(_now(9, 0, 7))}, eligibility={}, eligibility_coverage={},
        now=_now(9, 0, 7)) == "registered"
    assert inventory(engine, spec) == 0


def test_entry_outside_bracket_cannot_bypass_guard_via_rebalance(tmp_path):
    spec = replace(revision_spec(tmp_path), price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .2)
    at = _now(9, 0, 6)
    register(engine, spec, 1, .4, first, quote=book(at + timedelta(microseconds=1), ask=1100.))
    assert inventory(engine, spec) == 2000
    assert len(ledger(engine.fills_path)) == 1


def test_exit_schedule_wins_over_pending_reverse_addition(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    at = _now(9, 0, 6)
    register(engine, spec, 1, -.2, first, quote=book(at + timedelta(microseconds=1), bid_volume=0.))
    engine.process_quotes(quotes={"2330": book(_now(13, 20))}, now=_now(13, 20))
    mode = engine.state["modes"][spec.market]
    assert not any(p["signed_shares"] < 0 for p in mode["positions"].values())
    assert mode["pending_reduction_shares"] == 0
    assert mode["pending_entry_orders"]["2330"]["status"] != "working"


@pytest.mark.parametrize("weight,expected", [(.4, 4000), (.2, 2000), (-.2, -2000)])
def test_unknown_sizing_resolves_against_existing_inventory_not_a_second_full_order(tmp_path, weight, expected):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    register(engine, spec, 1, weight, first, quote={})
    mode = engine.state["modes"][spec.market]
    assert inventory(engine, spec) == 4000
    assert mode["signal_inventory_targets"]["2330"] is None
    for second in (7, 8):
        engine.process_quotes(quotes={"2330": book(_now(9, 0, second))}, now=_now(9, 0, second))
    assert inventory(engine, spec) == expected
    assert mode["signal_inventory_targets"]["2330"] == expected
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 9))}, now=_now(9, 0, 9))
    assert engine.fills_path.read_bytes() == before


def test_stop_exit_has_priority_over_pending_revision_risk_with_subsecond_clocks(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    at = _now(9, 0, 5).replace(microsecond=100000)
    first = register(engine, spec, 0, .2, at=at)
    at += timedelta(milliseconds=1)
    register(engine, spec, 1, .4, first, at=at,
        quote=book(at - timedelta(microseconds=1)))
    assert inventory(engine, spec) == 2000
    stopped = at + timedelta(milliseconds=1)
    engine.process_quotes(quotes={"2330": book(stopped, last=900., bid=900., ask=900.)}, now=stopped)
    position = next(iter(engine.state["modes"][spec.market]["positions"].values()))
    assert position["stop_triggered_at"] == stopped.isoformat(timespec="microseconds")
    assert position["last_exit_at"] == stopped.isoformat(timespec="microseconds")
    assert inventory(engine, spec) == 0
    engine.process_quotes(quotes={"2330": book(stopped + timedelta(milliseconds=1))}, now=stopped + timedelta(milliseconds=1))
    assert inventory(engine, spec) == 0
    assert engine.state["modes"][spec.market]["pending_entry_orders"]["2330"]["status"] == "cancelled_exit_priority"


def test_dashboard_invalidation_keeps_order_changes_but_not_private_template_payload(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    register(engine, spec, 0, .4, quote={})
    mode = engine.state["modes"][spec.market]
    order = mode["pending_entry_orders"]["2330"]
    revision = engine.state["dashboard_content_revision"]
    order["position"]["private_fixture_only"] = "完整私有範本仍落盤"
    engine._persist(_now(9, 0, 7))
    assert engine.state["dashboard_content_revision"] == revision
    assert json.loads(engine.state_path.read_text())["modes"][spec.market]["pending_entry_orders"]["2330"]["position"]["private_fixture_only"] == "完整私有範本仍落盤"
    order["last_wait_reason"] = "fixture_source_progress"
    engine._persist(_now(9, 0, 8))
    assert engine.state["dashboard_content_revision"] == revision + 1


def test_private_capacity_history_is_durable_but_not_rendered_material(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    register(engine, spec, 0, .4, quote={})
    mode = engine.state["modes"][spec.market]
    revision = engine.state["dashboard_content_revision"]
    mode["margin_exit_capacity_used"] = {"2026-10-05:13:20:2330": 2000}
    engine._persist(_now(9, 0, 7))
    assert engine.state["dashboard_content_revision"] == revision
    assert json.loads(engine.state_path.read_bytes())["modes"][spec.market]["margin_exit_capacity_used"] == mode["margin_exit_capacity_used"]
    mode["entry_unfilled_shares"] = 1000
    engine._persist(_now(9, 0, 8))
    assert engine.state["dashboard_content_revision"] == revision + 1


def test_retry_withholds_unpaid_receivable_once_not_twice(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    at = _now(9, 0, 6)
    register(engine, spec, 1, .9, first, at=at, quote=book(at - timedelta(microseconds=1)))
    mode = engine.state["modes"][spec.market]
    mode["corporate_action_receivable_twd"] = 1_000_000.
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 7))}, now=_now(9, 0, 7))
    # 4m original principal + 4m addition + actual fees + 1m withheld claim;
    # another 1,000 shares would exceed the frozen 10m cash ceiling.
    assert inventory(engine, spec) == 8000


def test_future_revision_clock_cannot_reduce_or_reverse_inventory(tmp_path):
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    at = _now(9, 0, 6)
    summary = revision_summary(1, at + timedelta(microseconds=1), first)
    before = engine.fills_path.read_bytes()
    result = engine.register_signal(spec=spec, summary=summary, signal_rows=[_row(-.4)],
        quotes={"2330": book(at)}, eligibility=_eligibility(), eligibility_coverage={}, now=at)
    assert result == "blocked" and inventory(engine, spec) == 4000
    assert engine.state["modes"][spec.market]["blocked_reason"] == "signal_ready_after_observation"
    assert engine.fills_path.read_bytes() == before


def test_bracket_cache_keys_all_legal_inputs_without_reusing_quotes(monkeypatch):
    from stockagent.live import tw_day_trade_simulation as module
    module._entry_bracket_limits.cache_clear()
    original = module.move_price_ticks_numpy
    calls = []
    def observe(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)
    monkeypatch.setattr(module, "move_price_ticks_numpy", observe)
    args = dict(upper_limit=1100., lower_limit=900., offset_ticks=1,
                trading_date=_now(9, 0).date(), security_type="stock")
    long = module.entry_bracket_prices(side="long", **args)
    assert long == (1095., 901.)
    assert module.entry_bracket_prices(side="short", **args) == long[::-1]
    assert len(calls) == 2
    for changed in ({"upper_limit": 1110.}, {"lower_limit": 899.}, {"offset_ticks": 2},
                    {"trading_date": _now(9, 0).date() + timedelta(days=1)}, {"security_type": "etf"}):
        module.entry_bracket_prices(side="long", **(args | changed))
    assert len(calls) == 12
    module._entry_bracket_limits.cache_clear()


def test_reduction_only_completion_requires_actual_inventory_then_logs_real_commit(tmp_path):
    from stockagent.live.opening_execution_latency import execution_quantities
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    at = _now(9, 0, 6)
    second = register(engine, spec, 1, .2, first,
        quote=book(at + timedelta(microseconds=1), bid_volume=0.))
    mode = engine.state["modes"][spec.market]
    assert mode["entry_requested_shares"] == 0
    assert mode["inventory_rebalance"]["remaining_delta_shares"] == 2000
    quantities = execution_quantities(mode)
    assert not quantities["all_target_complete"] and not quantities["no_execution_required"]
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 7))}, now=_now(9, 0, 7))
    assert mode["inventory_rebalance"]["target_complete"]
    assert execution_quantities(mode)["all_target_complete"]
    completed = [row for row in ledger(engine.execution_latency_path)
                 if row["signal_id"] == second["signal_id"] and row["all_target_complete"]]
    assert len(completed) == 1 and completed[0]["first_fill_observed_this_commit"]
    assert execution_quantities(completed[0])["all_target_complete"]
    from stockagent.live.tw_day_trade_dashboard import _signal_position_overlays, _overlay_signal_page_rows
    from types import SimpleNamespace
    overlays, _ = _signal_position_overlays(engine.state)
    page = _overlay_signal_page_rows(ledger(engine.signals_path), overlays,
                                    SimpleNamespace(membership=lambda *_: {}))
    assert page[-1]["reduction_filled_shares"] == 2000
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    engine.process_quotes(quotes={"2330": book(_now(9, 0, 8))}, now=_now(9, 0, 8))
    assert len([row for row in ledger(engine.execution_latency_path)
                if row["signal_id"] == second["signal_id"] and row["all_target_complete"]]) == 1


def test_inventory_overlay_is_not_a_new_fill_and_preserves_original_cohort(tmp_path):
    from stockagent.live.tw_day_trade_dashboard import _signal_position_overlays, _overlay_signal_page_rows
    from types import SimpleNamespace
    spec = revision_spec(tmp_path)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    first = register(engine, spec, 0, .4)
    register(engine, spec, 1, .2, first)
    rows = ledger(engine.signals_path)
    overlays, fingerprint = _signal_position_overlays(engine.state)
    page = _overlay_signal_page_rows(rows, overlays, SimpleNamespace(membership=lambda *_: {}))
    latest = page[-1]
    assert latest["filled_shares"] == 0 and "position" not in latest
    assert latest["current_target_inventory"]["signed_shares"] == 2000
    assert latest["current_target_inventory"]["target_signed_shares"] == 2000
    assert page[0]["position"]["signed_shares"] == 2000
    assert fingerprint


@pytest.mark.parametrize("fail_first", [False, True])
def test_single_flight_coalesces_new_source_generations_and_retries_failure(monkeypatch, fail_first):
    import asyncio
    from datetime import datetime
    from types import SimpleNamespace
    from services.discord_bot import bot as module
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return _now(9, 1).astimezone(tz)
    cfg = SimpleNamespace(market="fixture", day_trade_simulation_enabled=True,
        day_trade_signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "_scheduled_markets", lambda: ["fixture"])
    monkeypatch.setattr(module, "_resolve_market", lambda *_: cfg)
    monkeypatch.setattr(module, "_scheduled_market_session_day", lambda *_: (True, "fixture"))
    monkeypatch.setattr(module, "_opening_revision_initial_work_pending", lambda: False)
    monkeypatch.setattr(module, "_log_exception", lambda *_: None)
    generation = [0]
    monkeypatch.setattr("stockagent.live.opening_signal_revision.opening_source_revision", lambda *_, **__: generation[0])
    for field in ("_opening_revision_jobs", "_opening_revision_seen", "_opening_revision_retry"):
        monkeypatch.setattr(module.bot, field, {})
    async def exercise():
        calls = []
        release = asyncio.Event()
        async def work(function, config):
            calls.append(config)
            await release.wait()
            if fail_first and len(calls) == 1:
                raise RuntimeError("fixture transient failure")
            return None
        monkeypatch.setattr(module.asyncio, "to_thread", work)
        await module.progressive_opening_signal.coro()
        await asyncio.sleep(0)
        generation[0] += 1
        await module.progressive_opening_signal.coro()
        assert len(calls) == 1  # No queued model for every receipt update.
        release.set()
        job = module.bot._opening_revision_jobs["fixture"]
        await asyncio.gather(job, return_exceptions=True)
        await module.progressive_opening_signal.coro()
        module.bot._opening_revision_retry["fixture"] = 0.
        await module.progressive_opening_signal.coro()
        await module.bot._opening_revision_jobs["fixture"]
        await module.progressive_opening_signal.coro()
        assert len(calls) == 2 and not module.bot._opening_revision_jobs
    asyncio.run(exercise())


def test_completed_revision_reports_drain_after_exit_cutoff_without_new_model(monkeypatch):
    import asyncio
    from datetime import datetime
    from types import SimpleNamespace
    from services.discord_bot import bot as module
    at = _now(13, 20)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz)
    monkeypatch.setattr(module, "datetime", Clock)
    cfg = SimpleNamespace(market="fixture")
    monkeypatch.setattr(module, "_resolve_market", lambda *_: cfg)
    monkeypatch.setattr(module, "_scheduled_markets", lambda: pytest.fail("must not start work after cutoff"))
    deliveries = []
    monkeypatch.setattr(module, "_enqueue_scheduled_results", lambda rows, errors: deliveries.extend(rows))
    monkeypatch.setattr(module.bot, "_opening_revision_jobs", {})
    result = SimpleNamespace(summary={"asof_date": at.isoformat(), "signal_id": "committed"})
    async def exercise():
        job = asyncio.get_running_loop().create_future()
        job.set_result(result)
        module.bot._opening_revision_jobs["fixture"] = job
        await module.progressive_opening_signal.coro()
        await module.progressive_opening_signal.coro()
    asyncio.run(exercise())
    assert deliveries == [(cfg, result, f"{at.date()}:fixture:revision")]
    assert not module.bot._opening_revision_jobs


def test_initial_priority_does_not_freeze_healthy_modes_for_source_retries(monkeypatch):
    from types import SimpleNamespace
    from services.discord_bot import bot as module

    monkeypatch.setattr(module.bot, "_opening_attempt_started_monotonic", None)
    monkeypatch.setattr(module.bot, "_scheduled_input_jobs", {})
    # The old first-15-seconds gate would still be True for a source retry.
    monkeypatch.setattr(module, "_intraday_waits_for_opening", lambda: True)
    assert not module._opening_revision_initial_work_pending()
    def job(*, done, error):
        return {"task": SimpleNamespace(done=lambda: done, cancelled=lambda: False,
            result=lambda: (None, error, 0., None, {}))}
    module.bot._scheduled_input_jobs["waiting_source"] = job(done=False, error=None)
    module.bot._scheduled_input_jobs["failed_source"] = job(done=True, error=RuntimeError("source"))
    assert not module._opening_revision_initial_work_pending()
    module.bot._scheduled_input_jobs["ready_initial"] = job(done=True, error=None)
    assert module._opening_revision_initial_work_pending()
    module.bot._scheduled_input_jobs.clear()
    monkeypatch.setattr(module.bot, "_opening_attempt_started_monotonic", 1.)
    monkeypatch.setattr(module.bot, "_opening_attempt_stage", "inference")
    assert module._opening_revision_initial_work_pending()
    monkeypatch.setattr(module.bot, "_opening_attempt_stage", "realtime_prepare")
    assert not module._opening_revision_initial_work_pending()
