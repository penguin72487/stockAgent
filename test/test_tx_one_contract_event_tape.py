"""Event-ordered TX accounting, including cash settlement and funding.

The intraday times below are synthetic test inputs, not historical receipts.
"""
import copy
from dataclasses import replace
import hashlib
import json

import numpy as np
import pytest

from stockagent.data.tw_futures_benchmark import (
    TxOneContractReplayContext,
    replay_tx_one_contract_on_account_calendar,
    replay_tx_one_contract_event_tape,
    replay_tx_one_contract_event_tape_window,
    tx_one_contract_replay_payload,
    tx_one_contract_replay_from_payload,
    tx_one_contract_replay_input_fingerprint,
    tx_one_contract_continuous_report_segment,
)
from stockagent.data.tw_futures_account_calendar import (
    build_futures_account_calendar,
    slice_futures_account_calendar,
)


def event(time, kind, contract, price=None, *, cost=0, margin=None):
    value = {"time": time + "+08:00", "event": kind, "contract": contract}
    if price is not None:
        value.update(price=price, price_evidence="identified source")
    if kind != "margin":
        value["cost_twd"] = cost
    if margin is not None:
        value.update(margin_twd=margin, margin_evidence="dated margin rule")
    if kind == "expiry":
        value["expiry_evidence"] = "dated expiry rule"
    return value


def sample():
    dates = np.array(["2011-03-15", "2011-03-16", "2011-03-17"], dtype="datetime64[D]")
    events = [
        event("2011-03-15T09:00:00", "open", "201103", 8300, cost=5, margin=100),
        event("2011-03-15T13:45:00", "mark", "201103", 8305, margin=105),
        # The observed last mark 8306 is not the cash settlement 8305.
        event("2011-03-16T13:29:00", "mark", "201103", 8306, margin=105),
        event("2011-03-16T13:30:00", "expiry", "201103"),
        event("2011-03-16T13:31:00", "open", "201104", 8287, cost=3, margin=120),
        event("2011-03-16T13:45:00", "mark", "201104", 8289, margin=120),
        event("2011-03-16T14:30:00", "cash_settlement", "201103", 8305, cost=2),
        event("2011-03-17T13:45:00", "mark", "201104", 8292, margin=130),
    ]
    return events, dict(expected_dates=dates, market_session_mask=np.ones(3, dtype=bool),
                        initial_capital_twd=100, multiplier_twd_per_point=2)


def test_expiry_reopens_before_later_cash_settlement_with_both_margins():
    events, args = sample()
    result = replay_tx_one_contract_event_tape(events, **args)
    np.testing.assert_allclose(result["gross_pnl_twd"], [10, 4, 6])
    np.testing.assert_allclose(result["transaction_and_settlement_cost_twd"], [5, 5, 0])
    np.testing.assert_allclose(result["external_deposit_twd"], [5, 116, 0])
    np.testing.assert_allclose(result["equity_twd"], [110, 225, 231])
    np.testing.assert_allclose(result["peak_reserved_margin_twd"], [105, 225, 130])
    np.testing.assert_allclose(np.exp(result["benchmark_log_returns"]),
                               [110 / 105, (112 / 110) * (225 / 228), 231 / 225])
    np.testing.assert_allclose(
        result["equity_twd"],
        100 + np.cumsum(result["external_deposit_twd"] + result["gross_pnl_twd"]
                        - result["transaction_and_settlement_cost_twd"]),
    )
    assert result["end_contract_months"].tolist() == ["201103", "201104", "201104"]
    assert result["cash_settlement_mask"].tolist() == [False, True, False]
    assert result["open_trade_count"].tolist() == [1, 1, 0]
    assert result["close_trade_count"].tolist() == [0, 0, 0]
    assert result["valuation_mark_count"].tolist() == [1, 2, 1]


def test_no_trade_day_still_requires_a_mark_and_may_change_equity():
    events, args = sample()
    result = replay_tx_one_contract_event_tape(events, **args)
    assert result["gross_pnl_twd"][-1] == 6
    events.pop()
    with pytest.raises(ValueError, match="daily mark"):
        replay_tx_one_contract_event_tape(events, **args)


def test_no_trade_and_unchanged_real_mark_produce_a_flat_day():
    events, args = sample()
    events[-1]["price"] = 8289
    args["initial_capital_twd"] = 1000
    result = replay_tx_one_contract_event_tape(events, **args)
    assert result["open_trade_count"][-1] == 0
    assert result["close_trade_count"][-1] == 0
    assert result["equity_twd"][-1] == result["equity_twd"][-2]
    assert result["benchmark_log_returns"][-1] == 0


def test_account_only_date_requires_a_mark_but_forbids_trading():
    events, args = sample()
    args["market_session_mask"][2] = False
    result = replay_tx_one_contract_event_tape(events, **args)
    assert result["gross_pnl_twd"][-1] == 6
    events[-1]["event"] = "close"
    with pytest.raises(ValueError, match="account-only"):
        replay_tx_one_contract_event_tape(events, **args)


@pytest.mark.parametrize("alter", [
    lambda e: e[2].update(price_evidence=""),
    lambda e: e[3].update(contract="201104"),
    lambda e: e[4].update(time=e[3]["time"]),
    lambda e: e[4].update(time="2011-03-16T13:31:00"),
    lambda e: e[4].update(margin_evidence=""),
    lambda e: e[5].update(cost_twd=1),
])
def test_ambiguous_evidence_or_order_is_rejected(alter):
    events, args = sample()
    alter(events)
    with pytest.raises(ValueError):
        replay_tx_one_contract_event_tape(events, **args)


@pytest.mark.parametrize("field", ["price", "margin_twd", "cost_twd"])
@pytest.mark.parametrize("invalid", [True, np.bool_(True)])
def test_event_numeric_fields_reject_boolean_values(field, invalid):
    events, args = sample()
    events[0][field] = invalid
    with pytest.raises(ValueError, match="invalid|requires"):
        replay_tx_one_contract_event_tape(events, **args)


@pytest.mark.parametrize("field", ["initial_capital_twd", "multiplier_twd_per_point"])
@pytest.mark.parametrize("invalid", [True, np.bool_(True)])
def test_replay_capital_and_multiplier_reject_boolean_values(field, invalid):
    events, args = sample()
    args[field] = invalid
    with pytest.raises(ValueError, match="positive finite"):
        replay_tx_one_contract_event_tape(events, **args)


def test_replay_close_position_policy_requires_an_explicit_boolean():
    events, args = sample()
    args["require_market_close_position"] = "false"
    with pytest.raises(ValueError, match="boolean"):
        replay_tx_one_contract_event_tape(events, **args)


def test_cannot_reopen_before_settling_and_cannot_backfill_insolvency():
    events, args = sample()
    overlap = copy.deepcopy(events)
    overlap.pop(3)
    with pytest.raises(ValueError, match="two contracts"):
        replay_tx_one_contract_event_tape(overlap, **args)
    events[6]["price"] = 8000
    with pytest.raises(ValueError, match="insolvent"):
        replay_tx_one_contract_event_tape(events, **args)


def test_margin_update_is_external_funding_not_profit():
    events, args = sample()
    events.insert(7, event("2011-03-17T08:00:00", "margin", "201104", margin=300))
    result = replay_tx_one_contract_event_tape(events, **args)
    assert result["external_deposit_twd"][-1] == 75
    assert result["equity_twd"][-1] == 306
    assert np.exp(result["benchmark_log_returns"][-1]) == pytest.approx(306 / 300)
    events[7]["cost_twd"] = 1
    with pytest.raises(ValueError, match="margin update"):
        replay_tx_one_contract_event_tape(events, **args)


def test_expiry_requires_an_observed_mark_and_a_later_final_settlement():
    events, args = sample()
    with pytest.raises(ValueError, match="same-day mark"):
        replay_tx_one_contract_event_tape(events[:2] + events[3:], **args)
    with pytest.raises(ValueError, match="lacks the expired-contract settlement"):
        replay_tx_one_contract_event_tape(events[:6] + events[7:], **args)


def test_reset_window_replays_its_own_opening_and_funding():
    events, args = sample()
    continuous = replay_tx_one_contract_event_tape(events, **args)
    opening = event("2011-03-17T09:00:00", "open", "201104", 8290, cost=3, margin=130)
    reset = replay_tx_one_contract_event_tape_window(
        events, opening_event=opening, expected_dates=args["expected_dates"][2:],
        market_session_mask=args["market_session_mask"][2:],
        initial_capital_twd=100, multiplier_twd_per_point=2,
    )
    # The reset account deposits 33 before its own opening; the continuous
    # account already holds a contract and starts the day with equity 225.
    np.testing.assert_allclose(reset["external_deposit_twd"], [33])
    np.testing.assert_allclose(reset["gross_pnl_twd"], [4])
    np.testing.assert_allclose(reset["equity_twd"], [134])
    np.testing.assert_allclose(np.exp(reset["benchmark_log_returns"]), [134 / 133])
    assert np.exp(continuous["benchmark_log_returns"][-1]) == pytest.approx(231 / 225)
    assert reset["benchmark_log_returns"][0] != pytest.approx(continuous["benchmark_log_returns"][-1])


def test_reset_window_tolerates_the_same_record_but_rejects_ambiguous_order():
    events, args = sample()
    opening = events[0].copy()
    same = replay_tx_one_contract_event_tape_window(events, opening_event=opening, **args)
    continuous = replay_tx_one_contract_event_tape(events, **args)
    for key in continuous:
        np.testing.assert_array_equal(same[key], continuous[key])
    coincident = copy.deepcopy(events)
    coincident[0]["price"] += 1
    with pytest.raises(ValueError, match="ambiguous coincident"):
        replay_tx_one_contract_event_tape_window(coincident, opening_event=opening, **args)


def test_reset_window_rejects_a_foreign_settlement_and_missing_opening_evidence():
    events, args = sample()
    # Starting after the old contract expired cannot inherit its later cash
    # settlement merely because a continuous-account tape has that event.
    opening = event("2011-03-16T13:31:00", "open", "201104", 8287, cost=3, margin=120)
    with pytest.raises(ValueError, match="does not match an expired pending"):
        replay_tx_one_contract_event_tape_window(
            events, opening_event=opening, expected_dates=args["expected_dates"][1:],
            market_session_mask=args["market_session_mask"][1:],
            initial_capital_twd=100, multiplier_twd_per_point=2,
        )
    opening = events[0].copy()
    opening["price_evidence"] = ""
    with pytest.raises(ValueError, match="price_evidence"):
        replay_tx_one_contract_event_tape_window(events[1:], opening_event=opening, **args)


def test_reset_window_rejects_nonmarket_start_and_late_unsorted_tape():
    events, args = sample()
    args["market_session_mask"][0] = False
    with pytest.raises(ValueError, match="begin on an ordered market calendar"):
        replay_tx_one_contract_event_tape_window(events, opening_event=events[0], **args)
    args["market_session_mask"][0] = True
    # Out-of-order events beyond the requested window must not evade the audit.
    later = events + [event("2011-03-19T09:00:00", "mark", "201104", 8292, margin=130),
                      event("2011-03-18T09:00:00", "mark", "201104", 8292, margin=130)]
    with pytest.raises(ValueError, match="strictly increasing"):
        replay_tx_one_contract_event_tape_window(later, opening_event=events[0], **args)


def test_account_calendar_retains_holiday_event_and_attributes_it_to_next_market_day():
    dates = np.array(["2011-03-15", "2011-03-16", "2011-03-17"], dtype="datetime64[D]")
    calendar = build_futures_account_calendar(dates[[0, 2]], dates, np.array([False, True, False]))
    events = [
        event("2011-03-15T09:00:00", "open", "201103", 100, margin=100),
        event("2011-03-15T13:00:00", "mark", "201103", 105, margin=100),
        event("2011-03-16T13:00:00", "mark", "201103", 102, margin=150),
        event("2011-03-17T13:00:00", "mark", "201103", 102, margin=150),
    ]
    result = replay_tx_one_contract_on_account_calendar(
        events, calendar=calendar, initial_capital_twd=100,
        multiplier_twd_per_point=1,
    )
    np.testing.assert_array_equal(result["dates"], dates)
    np.testing.assert_array_equal(result["decision_dates"], dates[[0, 2]])
    np.testing.assert_allclose(result["gross_pnl_twd"], [5, -3, 0])
    np.testing.assert_allclose(result["external_deposit_twd"], [0, 48, 0])
    np.testing.assert_allclose(result["equity_twd"], [105, 150, 150])
    # The later market day's unchanged real mark is flat, but its reported
    # decision return includes the intervening, dated holiday loss.
    assert result["benchmark_log_returns"][2] == 0
    np.testing.assert_allclose(np.exp(result["decision_log_returns"]), [1.05, 102 / 105])
    assert result["account_calendar_fingerprint"] == calendar.fingerprint


@pytest.mark.parametrize("independent", [False, True])
def test_source_bound_replay_json_roundtrip_preserves_event_funding_and_scope(tmp_path, independent):
    from downloader.artifact_io import atomic_write_json

    events, args = sample()
    calendar = build_futures_account_calendar(
        args["expected_dates"], args["expected_dates"], ~args["market_session_mask"])
    opening = events[0] if independent else None
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=2, opening_event=opening))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=context, initial_capital_twd=100,
        multiplier_twd_per_point=2, opening_event=opening)
    expected = replay_tx_one_contract_on_account_calendar(
        events, calendar=calendar, initial_capital_twd=100,
        multiplier_twd_per_point=2, opening_event=opening)
    path = tmp_path / "tx_one_contract_benchmark.json"
    atomic_write_json(path, payload)
    saved = json.loads(path.read_text())
    restored = tx_one_contract_replay_from_payload(saved, context=context)
    for key in expected:
        np.testing.assert_array_equal(restored[key], expected[key])
    assert restored["external_deposit_twd"].tolist() == [5, 116, 0]
    # Deposit-inclusive equity growth is deliberately different from return.
    assert np.exp(restored["benchmark_log_returns"].sum()) != pytest.approx(231 / 100)
    assert restored["account_scope"] == ("independent_window_reset" if independent
                                          else "continuous_from_first_supplied_event")
    with pytest.raises(ValueError, match="pinned context differs"):
        tx_one_contract_replay_from_payload(saved, context=replace(
            context, event_source_fingerprint="3" * 64))


def test_continuous_report_segment_owns_prior_holiday_and_preserves_funding():
    dates = np.array(["2011-03-15", "2011-03-16", "2011-03-17"], dtype="datetime64[D]")
    calendar = build_futures_account_calendar(dates[[0, 2]], dates, np.array([False, True, False]))
    events = [
        event("2011-03-15T09:00:00", "open", "201103", 100, margin=100),
        event("2011-03-15T13:00:00", "mark", "201103", 105, margin=100),
        event("2011-03-16T13:00:00", "mark", "201103", 102, margin=150),
        event("2011-03-17T13:00:00", "mark", "201103", 102, margin=150),
    ]
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=1))
    payload = tx_one_contract_replay_payload(events, calendar=calendar, context=context,
        initial_capital_twd=100, multiplier_twd_per_point=1)
    first = tx_one_contract_continuous_report_segment(payload, context=context, start=0, stop=1)
    later = tx_one_contract_continuous_report_segment(payload, context=context, start=1, stop=2)
    np.testing.assert_array_equal(later["dates"], dates[1:])
    np.testing.assert_array_equal(later["decision_dates"], dates[[2]])
    np.testing.assert_array_equal(later["market_session_mask"], [False, True])
    np.testing.assert_allclose(later["external_deposit_twd"], [48, 0])
    np.testing.assert_allclose(np.exp(later["decision_log_returns"]), [102 / 105])
    np.testing.assert_allclose(later["equity_twd"], [150, 150])
    assert later["incoming_equity_twd"] == 105
    assert later["incoming_contract_month"] == "201103"
    assert later["parent_replay_input_fingerprint"] == context.replay_input_fingerprint
    assert later["account_calendar_fingerprint"] == slice_futures_account_calendar(calendar, 1, 2).fingerprint
    assert later["account_scope"] == "continuous_from_first_supplied_event"
    assert np.exp(first["decision_log_returns"].sum() + later["decision_log_returns"].sum()) == pytest.approx(1.02)

    altered = copy.deepcopy(payload)
    altered["arrays"]["external_deposit_twd"]["values"][1] = 0
    with pytest.raises(ValueError, match="TX replay artifact"):
        tx_one_contract_continuous_report_segment(altered, context=context, start=1, stop=2)
    for start, stop in [(True, 1), (0, False), (-1, 1), (1, 1), (0, 3)]:
        with pytest.raises(ValueError, match="decision slice"):
            tx_one_contract_continuous_report_segment(payload, context=context, start=start, stop=stop)

    reset_context = replace(context, replay_input_fingerprint=tx_one_contract_replay_input_fingerprint(
        events, initial_capital_twd=100, multiplier_twd_per_point=1, opening_event=events[0]))
    reset_payload = tx_one_contract_replay_payload(events, calendar=calendar, context=reset_context,
        initial_capital_twd=100, multiplier_twd_per_point=1, opening_event=events[0])
    with pytest.raises(ValueError, match="continuous"):
        tx_one_contract_continuous_report_segment(reset_payload, context=reset_context, start=1, stop=2)


@pytest.mark.parametrize("alter", [
    lambda p: p["arrays"]["benchmark_log_returns"]["values"].__setitem__(0, 0),
    lambda p: p["arrays"]["external_deposit_twd"]["values"].__setitem__(1, 0),
    lambda p: p["arrays"]["equity_twd"]["values"].__setitem__(0, 100),
    lambda p: p["arrays"]["open_trade_count"].update(dtype="<f8"),
    lambda p: p["metadata"].update(account_scope="independent_window_reset"),
    lambda p: p["inputs"]["events"][0].update(price=8200),
    lambda p: p.update(unknown_field=True),
    lambda p: p["arrays"].pop("valuation_mark_count"),
])
def test_source_bound_replay_rejects_changed_path_funding_scope_or_events(alter):
    events, args = sample()
    calendar = build_futures_account_calendar(
        args["expected_dates"], args["expected_dates"], ~args["market_session_mask"])
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=2))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=context, initial_capital_twd=100,
        multiplier_twd_per_point=2)
    alter(payload)
    with pytest.raises(ValueError, match="TX replay artifact"):
        tx_one_contract_replay_from_payload(payload, context=context)


def test_source_bound_replay_detaches_mutable_tape_and_rejects_false_calendar():
    events, args = sample()
    calendar = build_futures_account_calendar(
        args["expected_dates"], args["expected_dates"], ~args["market_session_mask"])
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=2))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=context, initial_capital_twd=100,
        multiplier_twd_per_point=2)
    events[0]["price"] = 1
    assert payload["inputs"]["events"][0]["price"] == 8300
    with pytest.raises(ValueError, match="SHA256"):
        tx_one_contract_replay_from_payload(payload, context=replace(context,
            event_source_fingerprint="unreviewed source"))
    with pytest.raises(ValueError, match="calendar"):
        tx_one_contract_replay_payload(
            events, calendar=calendar, context=replace(context, account_calendar_fingerprint="3" * 64),
            initial_capital_twd=100, multiplier_twd_per_point=2)


def test_rehashing_edited_events_cannot_replace_the_callers_pinned_input():
    events, args = sample()
    calendar = build_futures_account_calendar(
        args["expected_dates"], args["expected_dates"], ~args["market_session_mask"])
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=2))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=context, initial_capital_twd=100,
        multiplier_twd_per_point=2)
    payload["inputs"]["events"][0]["price"] = 8200
    encoded = json.dumps(payload["inputs"], ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode()
    payload["inputs_sha256"] = hashlib.sha256(encoded).hexdigest()
    with pytest.raises(ValueError, match="input fingerprint"):
        tx_one_contract_replay_from_payload(payload, context=context)


def test_persisted_independent_fold_keeps_its_own_opening_instead_of_slicing_equity():
    events, args = sample()
    dates = args["expected_dates"][-1:]
    calendar = build_futures_account_calendar(dates, dates, np.zeros(1, dtype=bool))
    opening = event("2011-03-17T09:00:00", "open", "201104", 8290, cost=3, margin=130)
    context = TxOneContractReplayContext("1" * 64, "2" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=2, opening_event=opening))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=context, initial_capital_twd=100,
        multiplier_twd_per_point=2, opening_event=opening)
    restored = tx_one_contract_replay_from_payload(json.loads(json.dumps(payload)), context=context)
    assert restored["account_scope"] == "independent_window_reset"
    np.testing.assert_allclose(restored["external_deposit_twd"], [33])
    np.testing.assert_allclose(restored["gross_pnl_twd"], [4])
    np.testing.assert_allclose(restored["equity_twd"], [134])
    np.testing.assert_allclose(np.exp(restored["benchmark_log_returns"]), [134 / 133])


def test_account_calendar_reset_window_does_not_slice_continuous_equity():
    events, args = sample()
    dates = args["expected_dates"]
    full_calendar = build_futures_account_calendar(dates, dates, np.zeros(len(dates), dtype=bool))
    continuous = replay_tx_one_contract_on_account_calendar(
        events, calendar=full_calendar, initial_capital_twd=100, multiplier_twd_per_point=2,
    )
    reset_calendar = build_futures_account_calendar(dates[2:], dates[2:], np.array([False]))
    opening = event("2011-03-17T09:00:00", "open", "201104", 8290, cost=3, margin=130)
    reset = replay_tx_one_contract_on_account_calendar(
        events, calendar=reset_calendar, opening_event=opening,
        initial_capital_twd=100, multiplier_twd_per_point=2,
    )
    np.testing.assert_allclose(np.exp(reset["decision_log_returns"]), [134 / 133])
    assert reset["decision_log_returns"][0] != pytest.approx(continuous["decision_log_returns"][-1])


def test_independent_reset_retains_flat_account_days_before_first_market_decision():
    events, args = sample()
    dates = args["expected_dates"]
    full = build_futures_account_calendar(dates[[0, 2]], dates, np.array([False, True, False]))
    reset_calendar = slice_futures_account_calendar(full, 1, 2)
    opening = event("2011-03-17T09:00:00", "open", "201104", 8290, cost=3, margin=130)
    replay = replay_tx_one_contract_on_account_calendar(
        events, calendar=reset_calendar, opening_event=opening,
        initial_capital_twd=100, multiplier_twd_per_point=2,
    )
    np.testing.assert_array_equal(replay["dates"], dates[1:])
    np.testing.assert_array_equal(replay["market_session_mask"], [False, True])
    np.testing.assert_allclose(replay["equity_twd"], [100, 134])
    np.testing.assert_allclose(replay["external_deposit_twd"], [0, 33])
    np.testing.assert_allclose(replay["benchmark_log_returns"], [0, np.log(134 / 133)])
    np.testing.assert_allclose(replay["decision_log_returns"], [np.log(134 / 133)])
    assert replay["account_scope"] == "independent_window_reset"


def test_account_calendar_rejects_leading_prior_events_and_mutated_identity():
    dates = np.array(["2011-03-15", "2011-03-16", "2011-03-17"], dtype="datetime64[D]")
    calendar = build_futures_account_calendar(dates[[0, 2]], dates, np.array([False, True, False]))
    sliced = slice_futures_account_calendar(calendar, 1, 2)
    with pytest.raises(ValueError, match="changed or includes out-of-window"):
        replay_tx_one_contract_on_account_calendar(
            [], calendar=sliced, initial_capital_twd=100, multiplier_twd_per_point=1,
        )
    calendar.decision_indices[0] = 1
    with pytest.raises(ValueError, match="changed or includes out-of-window"):
        replay_tx_one_contract_on_account_calendar(
            [], calendar=calendar, initial_capital_twd=100, multiplier_twd_per_point=1,
        )
