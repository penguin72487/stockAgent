"""Progressive decisions are immutable; inventory changes by actual deltas."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json

import numpy as np
import pytest

from stockagent.live.market_config import (
    OPENING_PROVISIONAL_REFERENCE_POLICY, SIGNAL_UPDATE_ONCE, SIGNAL_UPDATE_PROGRESSIVE,
)
from stockagent.live.opening_signal_revision import (
    opening_input_fingerprint, revision_admission, revision_fields,
)
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_CAUSAL_MARKET_FULL_TARGET, TwDayTradeSimulationEngine,
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


def inventory(engine, spec):
    return sum(int(p["signed_shares"]) for p in engine.state["modes"][spec.market]["positions"].values())


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
    monkeypatch.setattr(module, "_intraday_waits_for_opening", lambda: False)
    monkeypatch.setattr(module, "_log_exception", lambda *_: None)
    generation = [0]
    monkeypatch.setattr(module, "_path_revision", lambda *_: generation[0])
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
