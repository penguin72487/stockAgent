from copy import deepcopy
from datetime import datetime, timedelta
import json

import pytest

from stockagent.live.opening_execution_latency import (
    OpeningExecutionObserver, execution_quantities, opening_execution_slo,
)


GATE = datetime.fromisoformat("2026-10-14T09:00:00+08:00")


def mode(**overrides):
    return {"market": "mode", "signal_id": "mode-original", "session_date": "2026-10-14",
            "entry_requested_shares": 10000, "entry_filled_shares": 0,
            "entry_unfilled_shares": 10000, "pending_entry_shares": 10000,
            "pending_entry_source_count": 0, "entry_fill_outcome": "no_fill",
            "pending_entry_orders": {},
            "signal_registration_committed": True,
            "entry_fill_policy": "causal_market_full_target_at_best_quote",
            "signal_reason_counts": {}, **overrides}


def publication(markets=("mode",)):
    return {"modes": {market: {"signal_id": market + "-original",
                              "artifact_published_at": (GATE + timedelta(milliseconds=100)).isoformat()}
                      for market in markets}}


def sample(modes, at, *, observer=None, clock="engine_publication_clock_not_economic_event"):
    return (observer or OpeningExecutionObserver({})).samples(modes, committed_at=at, state_revision=1,
        engine_run_id="native-owner", clock_basis=clock)


def audit(modes, rows, markets=("mode",)):
    return opening_execution_slo("2026-10-14", publication(markets), modes, rows,
                                 observed_at=GATE + timedelta(hours=1))


def filled(**overrides):
    return mode(**{"entry_filled_shares": 10000, "entry_unfilled_shares": 0,
                   "pending_entry_shares": 0, "entry_fill_outcome": "filled", **overrides})


def test_all_modes_full_targets_not_registration_or_fast_subset():
    modes = {"mode": filled(), "slow": filled(signal_id="slow-original", market="slow")}
    rows = sample({"mode": modes["mode"]}, GATE + timedelta(milliseconds=800))
    result = audit(modes, rows, ("mode", "slow"))
    assert result["state"] == "incomplete"
    assert all(not stage["passed"] for stage in result["stages"])
    rows += sample({"slow": modes["slow"]}, GATE + timedelta(seconds=20))
    result = audit(modes, rows, ("mode", "slow"))
    assert result["all_modes_complete_from_open_ms"] == 20000
    assert [stage["passed"] for stage in result["stages"]] == [True, True, False, False, False, False]


def test_partial_unsized_or_bad_quantities_cannot_count_as_full_target():
    for current in (mode(), filled(pending_entry_source_count=1), filled(entry_unfilled_shares=1),
                    filled(entry_filled_shares=True), filled(entry_filled_shares=11000),
                    filled(pending_entry_orders={"unknown": {"sizing_pending": True, "status": "cancelled_exit_priority"}}),
                    filled(signal_registration_committed=False), filled(unsized_entry_target_count=False, pending_entry_orders=None),
                    filled(ledger_state_divergence={"kind": "mismatch"})):
        result = audit({"mode": current}, sample({"mode": current}, GATE + timedelta(milliseconds=900)))
        assert result["all_modes_complete_from_open_ms"] is None
        assert all(not stage["passed"] for stage in result["stages"])


@pytest.mark.parametrize("elapsed,passed", [(11000, True), (11001, False)])
def test_eleven_second_full_fill_deadline_does_not_mean_signal_registration(elapsed, passed):
    modes = {"mode": filled()}
    result = audit(modes, sample(modes, GATE + timedelta(milliseconds=elapsed)))
    stage = next(row for row in result["stages"] if row["target_ms"] == 11000)
    assert stage["passed"] is passed


def test_cash_only_mode_is_no_execution_required_never_a_successful_fill():
    cash = mode(entry_requested_shares=0, entry_unfilled_shares=0, pending_entry_shares=0,
                entry_fill_outcome="no_order", signal_reason_counts={"below_one_board_lot": 4, "zero_target_weight": 100})
    rows = sample({"mode": cash}, GATE + timedelta(seconds=2))
    result = audit({"mode": cash}, rows)
    assert result["state"] == "no_execution_required"
    assert result["completed_fill_mode_count"] == 0
    assert result["no_execution_required_mode_count"] == 1
    assert all(not stage["passed"] for stage in result["stages"])
    rows += sample({"actual": filled(market="actual", signal_id="actual-original")}, GATE + timedelta(seconds=3))
    result = audit({"mode": cash, "actual": filled(market="actual", signal_id="actual-original")},
                   rows, ("mode", "actual"))
    assert result["all_modes_complete_from_open_ms"] == 3000


@pytest.mark.parametrize("at,overrides", [
    (GATE + timedelta(milliseconds=50), {}),
    (GATE.replace(tzinfo=None), {}),
    (GATE + timedelta(hours=2), {}),
    (GATE + timedelta(milliseconds=900), {"signal_id": "wrong-signal"}),
    (GATE + timedelta(milliseconds=900), {"clock_basis": "offline_event_clock_not_live_latency"}),
    (GATE + timedelta(milliseconds=900), {"simulation_only": False}),
    (GATE + timedelta(milliseconds=900), {"production_order_possible": True}),
    (GATE + timedelta(milliseconds=900), {"contract": "registration_only"}),
])
def test_backdated_wrong_signal_offline_and_future_clocks_never_pass(at, overrides):
    current = filled()
    rows = sample({"mode": current}, at)
    rows[0].update(overrides)
    result = audit({"mode": current}, rows)
    assert all(not stage["passed"] for stage in result["stages"])
    assert result["modes"]["mode"]["invalid_execution_records"] == 1


def test_replay_completion_is_reported_but_not_live_slo():
    current = filled(entry_fill_policy="official_open_signal_0900_execute_0901_vwap")
    result = audit({"mode": current}, sample({"mode": current}, GATE + timedelta(milliseconds=800)))
    assert result["modes"]["mode"]["state"] == "replay_not_live_execution"
    assert all(not stage["passed"] for stage in result["stages"])


def test_restore_unchanged_marks_emit_no_rows_or_false_first_fill_clock():
    current = mode(entry_filled_shares=5000, entry_unfilled_shares=5000, pending_entry_shares=5000)
    modes = {"mode": current}
    observer = OpeningExecutionObserver(modes)
    before = deepcopy(modes)
    assert not sample(modes, GATE + timedelta(seconds=500), observer=observer)
    current["total_equity_twd"] = 99999.12
    assert not sample(modes, GATE + timedelta(seconds=501), observer=observer)
    current.update(entry_filled_shares=10000, entry_unfilled_shares=0, pending_entry_shares=0,
                   entry_fill_outcome="filled")
    rows = sample(modes, GATE + timedelta(seconds=502), observer=observer)
    assert rows[0]["first_fill_clock_unknown_after_restore"]
    assert not rows[0]["first_fill_observed_this_commit"]
    observer.acknowledge(modes)
    assert not sample(modes, GATE + timedelta(seconds=503), observer=observer)
    assert before["mode"]["entry_filled_shares"] == 5000


def test_newly_sized_target_invalidates_earlier_smaller_completion():
    earlier = filled()
    current = filled(entry_requested_shares=20000, entry_filled_shares=20000)
    rows = sample({"mode": earlier}, GATE + timedelta(milliseconds=500))
    result = audit({"mode": current}, rows)
    assert result["all_modes_complete_from_open_ms"] is None
    rows += sample({"mode": current}, GATE + timedelta(seconds=4))
    assert audit({"mode": current}, rows)["all_modes_complete_from_open_ms"] == 4000


def test_later_fill_in_same_owner_does_not_claim_first_fill_clock_was_lost_after_restore():
    current = mode(entry_filled_shares=5000, entry_unfilled_shares=5000, pending_entry_shares=5000)
    modes = {"mode": current}
    observer = OpeningExecutionObserver({})
    first = sample(modes, GATE + timedelta(milliseconds=300), observer=observer)
    assert first[0]["first_fill_observed_this_commit"]
    assert not first[0]["first_fill_clock_unknown_after_restore"]
    observer.acknowledge(modes)
    current.update(entry_filled_shares=10000, entry_unfilled_shares=0, pending_entry_shares=0)
    later = sample(modes, GATE + timedelta(milliseconds=700), observer=observer)
    assert not later[0]["first_fill_observed_this_commit"]
    assert not later[0]["first_fill_clock_unknown_after_restore"]
    result = audit(modes, first + later)
    assert result["modes"]["mode"]["first_fill_committed_at"] == first[0]["recorded_at"]
    assert result["all_modes_complete_from_open_ms"] == 700


def test_observer_never_mutates_economic_inputs():
    modes = {"mode": filled()}
    original = deepcopy(modes)
    observer = OpeningExecutionObserver({})
    sample(modes, GATE + timedelta(seconds=1), observer=observer)
    observer.acknowledge(modes)
    assert modes == original


def test_old_quote_failure_does_not_hide_later_successfully_filled_targets():
    current = filled(signal_reason_counts={"quote_not_after_signal": 1, "official_open_price_unavailable": 1})
    result = audit({"mode": current}, sample({"mode": current}, GATE + timedelta(seconds=4)))
    assert result["all_modes_complete_from_open_ms"] == 4000
    assert result["modes"]["mode"]["source_reason_counts_are_registration_history"]


def test_precommit_intent_is_not_a_fill_and_does_not_consume_first_fill_clock():
    current = filled(signal_registration_committed=False)
    modes = {"mode": current}
    observer = OpeningExecutionObserver({})
    intent = sample(modes, GATE + timedelta(milliseconds=200), observer=observer)
    assert not intent[0]["all_target_complete"] and not intent[0]["first_fill_observed_this_commit"]
    observer.acknowledge(modes)
    current["signal_registration_committed"] = True
    completed = sample(modes, GATE + timedelta(milliseconds=250), observer=observer)
    assert completed[0]["first_fill_observed_this_commit"]
    assert audit(modes, intent + completed)["all_modes_complete_from_open_ms"] == 250
