from datetime import date, datetime, timedelta
from copy import deepcopy
import hashlib
import json

import pytest
import polars as pl

from scripts.rebuild_tw_day_trade_minute_curves import (
    _replay_opening_valuation_prices,
    rebuild_carried_strategy_marks,
    validate_carried_endpoint_accounting,
)
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT


def fixture():
    days = ["2026-08-13", "2026-08-14"]
    p = dict(position_id="p", symbol="2330", side="long", entry_price=100.,
             buy_fee_rate=0., sell_fee_rate=0., cash_buy_fee_rate=0., cash_sell_fee_rate=0.,
             margin_carry_contract=MARGIN_CARRY_CONTRACT, margin_converted_at=days[0]+"T13:30:00+08:00")
    fills = [dict(position_id="p", market="a", symbol="2330", purpose="entry", quantity=1000,
                  recorded_at=days[0]+"T09:01:00+08:00", session_date=days[0], price=100., fee_and_tax_twd=10.),
             dict(position_id="p", market="a", symbol="2330", purpose="exit", quantity=750,
                  recorded_at=days[1]+"T09:02:00+08:00", session_date=days[1], price=130.,
                  net_pnl_twd=-2510., entry_fee_allocated_twd=10.)]
    mode = dict(initial_capital_twd=100000., margin_carry_contract=MARGIN_CARRY_CONTRACT,
                share_replacement_ledger=[dict(position_id="p", recorded_at=days[1]+"T09:00:00+08:00",
                    old_signed_shares=1000, new_signed_shares=750, new_entry_price=100/.75, ratio=.75, cash_per_old_share=2.5)],
                corporate_action_ledger=[dict(ex_date=days[1], payment_date="2026-08-15", amount_twd=2500.)],
                carry_cost_ledger=[dict(date=days[1], amount_twd=5., kind="financing_interest")])
    rows = []
    for day in days:
        for clock in ["09:01", "13:30"]:
            realized = -2510. if day == days[1] and clock == "13:30" else 0.
            net = -10. if day == days[0] else (-2510. if clock == "09:01" else 0.)
            earned, cost = (2500., 5.) if day == days[1] else (0., 0.)
            rows.append(dict(market="a", minute=f"{day}T{clock}+08:00", session_date=day,
                initial_capital_twd=100000., cumulative_realized_net_pnl_twd=realized,
                open_net_liquidation_pnl_twd=net, total_equity_twd=100000+realized+net+earned-cost,
                cumulative_corporate_action_net_twd=earned, cumulative_carry_cost_twd=cost,
                margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=int(not realized)))
    class Store:
        def prices(self, symbol, day):
            # No 09:01 print on day 2: carry the prior observed price through
            # the physical action, never use day 2's future close as its open.
            start = datetime.fromisoformat(day+"T09:01:00+08:00")
            return {(start+timedelta(minutes=i)).isoformat(timespec="minutes"): (100. if day == days[0] else 130.)
                    for i in range(270) if day == days[0] or i > 0}
    return rows, {day: {"a": [p]} for day in days}, Store(), {"modes": {"a": mode}}, fills


def test_carried_revaluation_preserves_fills_basis_cash_interest_and_endpoints():
    rows, positions, store, state, fills = fixture()
    unchanged = deepcopy((rows, positions, state, fills))
    rebuilt, stats = rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
                                                    start=date(2026, 8, 13), end=date(2026, 8, 14))
    assert len(rebuilt) == 540 and stats["carried_fill_book_revalued_without_execution"]
    assert rebuilt[1]["total_equity_twd"] == pytest.approx(99990.)
    assert rebuilt[271]["total_equity_twd"] == pytest.approx(99985.)
    assert rebuilt[271]["corporate_action_receivable_twd"] == 2500.
    assert rebuilt[271]["open_position_count"] == 0
    assert (rows, positions, state, fills) == unchanged
    for endpoint in rows:
        actual = next(r for r in rebuilt if r["minute"] == endpoint["minute"])
        assert actual["total_equity_twd"] == endpoint["total_equity_twd"]


def _missing_first_cost_fixture():
    rows, positions, store, state, fills = fixture()
    rows[0]["recorded_at"] = "2026-08-13T09:01:05+08:00"
    fills[0]["recorded_at"] = "2026-08-13T09:01:02+08:00"

    class MissingFirst:
        def prices(self, symbol, day):
            return {key: value for key, value in store.prices(symbol, day).items()
                    if key != "2026-08-13T09:01+08:00"}

    return rows, positions, MissingFirst(), state, fills


def test_explicit_cost_valuation_is_not_a_quote_fill_or_backdated_clock():
    from scripts.rebuild_tw_day_trade_minute_curves import (
        validate_provisional_opening_cost_valuations, historical_minute_mark_has_source,
    )
    rows, positions, store, state, fills = _missing_first_cost_fixture()
    unchanged = deepcopy((rows, positions, state, fills))
    with pytest.raises(RuntimeError, match="missing completed 09:01"):
        rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True)
    rebuilt, stats = rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
        provisional_opening_cost_targets={("a", "2026-08-13", "2330")})
    row = rebuilt[0]
    assert len(rebuilt) == 540 and row["total_equity_twd"] == pytest.approx(99990.)
    assert row["recorded_at"] == rows[0]["recorded_at"]
    assert row["opening_mark_revalued_from_completed_bar"] is False
    assert row["valuation_stale"] is True and row["valuation_executable"] is False
    assert row["fresh_trade_position_count"] == 0
    assert historical_minute_mark_has_source(row) is False
    assert row["provisional_opening_cost_valuation"]["cohorts"][0]["entry_cost_price"] == 100.
    assert row["provisional_opening_cost_valuation"]["cohorts"][0]["entry_at"].startswith("2026-08-13T09:01:02")
    assert "provisional_opening_cost_valuation" not in rebuilt[1]
    assert rebuilt[1]["valuation_stale"] is False
    accepted = validate_provisional_opening_cost_valuations(rebuilt, positions, state=state,
        fill_rows=fills, authorization=stats["provisional_opening_cost_valuation"], source_rows=rows)
    assert accepted["verified_symbol_points"] == 1
    assert accepted["market_observation_verified"] is False
    assert (rows, positions, state, fills) == unchanged


def test_cost_valuation_readback_retains_uniquely_resolved_fractional_fill_clock():
    from scripts.rebuild_tw_day_trade_minute_curves import validate_provisional_opening_cost_valuations

    rows, positions, store, state, fills = _missing_first_cost_fixture()
    fills[0]["recorded_at"] = fills[0]["fill_at"] = "2026-08-13T09:01:05.500000+08:00"
    original = deepcopy((rows, fills))
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
        provisional_opening_cost_targets={("a", "2026-08-13", "2330")},
    )
    accepted = validate_provisional_opening_cost_valuations(
        rebuilt, positions, state=state, fill_rows=fills,
        authorization=stats["provisional_opening_cost_valuation"], source_rows=rows,
    )
    assert accepted["verified_symbol_points"] == 1
    assert rebuilt[0]["recorded_at"] == "2026-08-13T09:01:05+08:00"
    assert rebuilt[0]["provisional_opening_cost_valuation"]["cohorts"][0]["entry_at"] == fills[0]["fill_at"]
    assert (rows, fills) == original


@pytest.mark.parametrize("prior_claimed_sourced", [False, True])
def test_first_bar_assumption_preserves_actual_clock_fills_and_later_prices(prior_claimed_sourced):
    from scripts.rebuild_tw_day_trade_minute_curves import (
        OPENING_FIRST_BAR_VALUATION_CONTRACT, historical_minute_mark_has_source,
        validate_provisional_opening_cost_valuations,
    )
    rows, positions, original_store, state, fills = _missing_first_cost_fixture()
    if prior_claimed_sourced:
        rows[0].update(historical_minute_replay=True,
                       minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
                       valuation_source="legacy_replay_claim", valuation_executable=False,
                       fresh_trade_position_count=1, last_trade_carried_position_count=0,
                       missing_price_position_count=0, fresh_trade_notional_coverage_ratio=1.)
        assert historical_minute_mark_has_source(rows[0]) is True
    original = deepcopy((rows, positions, state, fills))
    reference = {"symbol": "2330", "session_date": "2026-08-13",
        "source_minute": "2026-08-13T09:02+08:00", "price": 103.,
        "source": "verified_fixture", "source_pins": [], "price_method": "observed_completed_minute_close"}

    class Store:
        def prices(self, symbol, day):
            values = original_store.prices(symbol, day)
            if day == "2026-08-13":
                values[reference["source_minute"]] = 103.
            return values

        def first_observed_bar(self, symbol, day):
            assert (symbol, day) == ("2330", "2026-08-13")
            return deepcopy(reference)

        def assert_sources_unchanged(self):
            pass

    store = Store()
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
        provisional_opening_first_bar_targets={("a", "2026-08-13", "2330")})
    row = rebuilt[0]
    key = "provisional_opening_first_bar_valuation"
    assert row["total_equity_twd"] == pytest.approx(102990.)
    assert row["recorded_at"] == "2026-08-13T09:01:05+08:00"
    assert row[key]["contract"] == OPENING_FIRST_BAR_VALUATION_CONTRACT
    assert row[key]["cohorts"][0]["reference_bar"] == reference
    assert row[key]["uses_later_observation"] is True
    assert row[key]["causal_0901_market_observation"] is False
    assert row["valuation_stale"] is True and row["valuation_executable"] is False
    assert historical_minute_mark_has_source(row) is False
    assert key not in rebuilt[1] and rebuilt[1]["total_equity_twd"] == row["total_equity_twd"]
    assert rebuilt[269]["total_equity_twd"] == rows[1]["total_equity_twd"]
    result = validate_provisional_opening_cost_valuations(
        rebuilt, positions, state=state, fill_rows=fills, authorization=stats[key],
        source_rows=rows, first_bar=True, store=Store())
    assert result["later_bar_source_verified"] is True
    assert result["market_observation_verified"] is False
    row[key]["cohorts"][0]["reference_bar"]["source_minute"] = "2026-08-13T09:01+08:00"
    with pytest.raises(ValueError, match="immutable cost, clock or disclosure"):
        validate_provisional_opening_cost_valuations(
            rebuilt, positions, state=state, fill_rows=fills, authorization=stats[key],
            source_rows=rows, first_bar=True, store=Store())
    assert (rows, positions, state, fills) == original


@pytest.mark.parametrize("tamper", ["cost", "quantity", "clock", "hide_stale", "claim_quote", "claim_bar"])
def test_cost_valuation_independent_readback_rejects_changed_basis_or_disclosure(tamper):
    from scripts.rebuild_tw_day_trade_minute_curves import validate_provisional_opening_cost_valuations
    rows, positions, store, state, fills = _missing_first_cost_fixture()
    rebuilt, stats = rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
        provisional_opening_cost_targets={("a", "2026-08-13", "2330")})
    row = rebuilt[0]
    if tamper == "cost":
        row["provisional_opening_cost_valuation"]["cohorts"][0]["entry_cost_price"] += 1
    elif tamper == "quantity":
        row["provisional_opening_cost_valuation"]["cohorts"][0]["signed_shares"] += 1000
    elif tamper == "clock":
        row["recorded_at"] = "2026-08-13T09:01:00+08:00"
    elif tamper == "hide_stale":
        row["valuation_stale"] = False
    elif tamper == "claim_quote":
        row["valuation_executable"] = True
    else:
        row["opening_mark_revalued_from_completed_bar"] = True
    with pytest.raises(ValueError, match="immutable cost, clock or disclosure"):
        validate_provisional_opening_cost_valuations(rebuilt, positions, state=state, fill_rows=fills,
            authorization=stats["provisional_opening_cost_valuation"], source_rows=rows)


@pytest.mark.parametrize("target", [("b", "2026-08-13", "2330"), ("a", "2026-08-14", "2330"),
                                     ("a", "2026-08-13", "2317")])
def test_cost_valuation_authorization_does_not_cover_neighboring_scope(target):
    rows, positions, store, state, fills = _missing_first_cost_fixture()
    with pytest.raises(RuntimeError, match="missing completed 09:01"):
        rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
            provisional_opening_cost_targets={target})


def test_public_first_bar_warning_keeps_actual_source_clock_without_private_prices(tmp_path):
    from stockagent.live.tw_day_trade_dashboard import _provisional_opening_cost_issues
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({"retained_curve_recovery": {
        "provisional_opening_first_bar_valuation_targets": [["a", "2026-10-14", "6624"]],
        "provisional_opening_first_bar_references": [{"market": "a", "session_date": "2026-10-14",
            "symbol": "6624", "source_minute": "2026-10-14T12:43+08:00",
            "private_source_path": "/private/source", "price": 39.85}],
    }}))
    issues = _provisional_opening_cost_issues(tmp_path)
    assert len(issues) == 1 and issues[0]["code"] == "provisional_opening_first_bar_valuation"
    assert "12:43" in issues[0]["detail"] and "事後觀測" in issues[0]["detail"]
    assert issues[0]["observed_at"] == "2026-10-14T12:43+08:00"
    assert "/private/source" not in json.dumps(issues) and "39.85" not in json.dumps(issues)


def test_public_cost_valuation_warning_has_no_private_prices_or_paths(tmp_path):
    from stockagent.live.tw_day_trade_dashboard import _provisional_opening_cost_issues
    assert _provisional_opening_cost_issues(tmp_path) == []
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({
        "retained_curve_recovery": {"provisional_opening_cost_valuation_targets": [
            ["a", "2026-11-02", "5251"], ["a", "2026-11-02", "4767"]]},
        "private": {"path": "/private/hidden", "cost": 27.1}}))
    issues = _provisional_opening_cost_issues(tmp_path)
    assert len(issues) == 1 and issues[0]["count"] == 2
    assert issues[0]["severity"] == "warning"
    assert "不是可成交報價" in issues[0]["detail"]
    assert "原成交" in issues[0]["detail"] and "4767" in issues[0]["detail"]
    assert "/private" not in json.dumps(issues) and "27.1" not in json.dumps(issues)


def test_carried_opening_counts_fill_with_zero_fractional_seconds():
    rows, positions, store, state, fills = fixture()
    fills[0]["fill_at"] = "2026-08-13T09:01:00.000000+08:00"
    rebuilt, _ = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
    )
    assert len(rebuilt) == 540
    assert rebuilt[0]["open_position_count"] == 1
    assert rebuilt[0]["total_equity_twd"] == rows[0]["total_equity_twd"]


def test_second_precision_endpoint_reconciles_one_unique_fractional_fill():
    rows, positions, store, state, fills = fixture()
    rows[0]["recorded_at"] = "2026-08-13T09:01:00+08:00"
    fills[0]["fill_at"] = "2026-08-13T09:01:00.051038+08:00"
    fills[0]["recorded_at"] = fills[0]["fill_at"]
    original = deepcopy((rows, positions, state, fills))
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
    )
    assert rebuilt[0]["recorded_at"] == "2026-08-13T09:01:00+08:00"
    assert rebuilt[0]["open_position_count"] == 1
    assert stats["second_precision_endpoint_reconciliations"] == 1
    assert (rows, positions, state, fills) == original
    proof = validate_carried_endpoint_accounting(
        rows, positions, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
    )
    assert proof == {"status": "validated_component_only", "endpoints_checked": 4,
                     "second_precision_endpoint_reconciliations": 1,
                     "archived_terminal_fill_clock_resolutions": [],
                     "price_and_minute_coverage_validated": False}


def test_second_precision_endpoint_rejects_ambiguous_inventory():
    rows, positions, store, state, fills = fixture()
    rows[0]["recorded_at"] = "2026-08-13T09:01:00+08:00"
    fills[0]["fill_at"] = "2026-08-13T09:01:00.051038+08:00"
    fills[0]["recorded_at"] = fills[0]["fill_at"]
    completion = dict(fills[0], purpose="entry_completion", quantity=500,
                      fill_at="2026-08-13T09:01:00.700000+08:00",
                      recorded_at="2026-08-13T09:01:00.700000+08:00")
    fills.append(completion)
    with pytest.raises(RuntimeError, match="ambiguous.*second.precision"):
        rebuild_carried_strategy_marks(
            rows, positions, store, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
        )
    with pytest.raises(RuntimeError, match="ambiguous.*second.precision"):
        validate_carried_endpoint_accounting(
            rows, positions, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
        )


def test_fractional_precision_endpoint_never_admits_a_later_fill():
    rows, positions, store, state, fills = fixture()
    rows[0]["recorded_at"] = "2026-08-13T09:01:00.000000+08:00"
    fills[0]["fill_at"] = "2026-08-13T09:01:00.051038+08:00"
    fills[0]["recorded_at"] = fills[0]["fill_at"]
    with pytest.raises(RuntimeError, match="endpoint accounting"):
        rebuild_carried_strategy_marks(
            rows, positions, store, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
        )


def test_replayed_minute_label_is_not_treated_as_an_observed_second_interval():
    rows, positions, store, state, fills = fixture()
    rows[0].update(recorded_at="2026-08-13T09:01:00+08:00",
                   historical_minute_replay=True,
                   opening_mark_revalued_from_completed_bar=True)
    fills[0]["fill_at"] = "2026-08-13T09:01:00.051038+08:00"
    fills[0]["recorded_at"] = fills[0]["fill_at"]
    with pytest.raises(RuntimeError, match="endpoint accounting"):
        rebuild_carried_strategy_marks(
            rows, positions, store, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
        )


def test_scoped_replayed_opening_correction_uses_only_the_original_fill_clock():
    rows, positions, store, state, fills = fixture()
    rows[0].update(recorded_at="2026-08-13T09:01:00+08:00",
                   historical_minute_replay=True, simulation_only=True,
                   opening_mark_revalued_from_completed_bar=True,
                   minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
                   valuation_source="retained_1m_close_from_immutable_carried_fill_and_action_book",
                   valuation_executable=False, fresh_trade_position_count=1,
                   last_trade_carried_position_count=0, missing_price_position_count=0,
                   fresh_trade_notional_coverage_ratio=1.,
                   cumulative_realized_net_pnl_twd=-100., open_position_count=0)
    original = deepcopy((rows, positions, state, fills))
    target = {("a", "2026-08-13")}
    preflight = validate_carried_endpoint_accounting(
        rows, positions, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), repair_opening_targets=target,
    )
    assert preflight["status"] == "repair_candidates_identified_not_applied"
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
        repair_opening_targets=target,
    )
    opening = rebuilt[0]
    assert opening["open_position_count"] == 1
    assert opening["cumulative_realized_net_pnl_twd"] == 0.
    assert opening["total_equity_twd"] == pytest.approx(99990.)
    assert opening["historical_opening_endpoint_repair"]["prior_accounting"] == {
        "cumulative_realized_net_pnl_twd": -100., "open_position_count": 0,
    }
    assert opening["historical_opening_endpoint_repair"]["broker_fill"] is False
    assert len(stats["repaired_opening_endpoints"]) == 1
    assert (rows, positions, state, fills) == original


def test_scoped_replayed_opening_repair_fails_closed_for_missing_price_or_provenance():
    rows, positions, store, state, fills = fixture()
    rows[0].update(recorded_at="2026-08-13T09:01:00+08:00",
                   historical_minute_replay=True, simulation_only=True,
                   opening_mark_revalued_from_completed_bar=True,
                   minute_valuation_contract="right_labelled_historical_last_trade_mark_v1",
                   valuation_source="retained_1m_close_from_immutable_carried_fill_and_action_book",
                   valuation_executable=False, fresh_trade_position_count=1,
                   last_trade_carried_position_count=0, missing_price_position_count=0,
                   fresh_trade_notional_coverage_ratio=1., open_position_count=0)
    target = {("a", "2026-08-13")}

    class MissingOpeningStore:
        def prices(self, symbol, day):
            prices = store.prices(symbol, day)
            prices.pop(day + "T09:01+08:00", None)
            return prices

    with pytest.raises(RuntimeError, match="missing completed 09:01 valuation bar"):
        rebuild_carried_strategy_marks(
            rows, positions, MissingOpeningStore(), state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14), revalue_opening_marks=True,
            repair_opening_targets=target,
        )
    rows[0]["simulation_only"] = False
    with pytest.raises(RuntimeError, match="endpoint accounting"):
        validate_carried_endpoint_accounting(
            rows, positions, state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14), repair_opening_targets=target,
        )


def test_carried_revaluation_ignores_future_fills_outside_requested_end():
    rows, positions, store, state, fills = fixture()
    # A later already-recorded session is not part of the selected valuation
    # interval, so its position will correctly be absent from this inventory.
    # It must remain in the original ledger, not fail or enter this replay.
    future = dict(position_id="future", market="a", symbol="2317", purpose="entry",
                  quantity=1000, recorded_at="2026-08-17T09:01:00+08:00",
                  session_date="2026-08-17", price=100., fee_and_tax_twd=0.)
    fills.append(future)
    unchanged = deepcopy(fills)
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, store, state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
    )
    assert len(rebuilt) == 540
    assert stats["carried_fill_book_revalued_without_execution"]
    assert fills == unchanged


def test_future_exit_does_not_close_the_selected_end_day():
    rows, positions, store, state, fills = fixture()
    original = deepcopy((rows, positions, state, fills))
    rebuilt, _ = rebuild_carried_strategy_marks(
        rows, {"2026-08-13": positions["2026-08-13"]}, store,
        state=state, fill_rows=fills, start=date(2026, 8, 13), end=date(2026, 8, 13),
    )
    endpoint = next(row for row in rebuilt if row["minute"] == "2026-08-13T13:30+08:00")
    assert endpoint["open_position_count"] == 1
    assert endpoint["total_equity_twd"] == rows[1]["total_equity_twd"]
    assert [row for row in rebuilt if row["session_date"] == "2026-08-14"] == rows[2:]
    assert (rows, positions, state, fills) == original


def test_carried_revaluation_counts_disclosed_entry_completion_as_inventory():
    day = "2026-08-13"
    position = dict(position_id="p", symbol="2330", side="long", entry_price=100.,
                    buy_fee_rate=0., sell_fee_rate=0., cash_buy_fee_rate=0.,
                    cash_sell_fee_rate=0., margin_carry_contract=MARGIN_CARRY_CONTRACT)
    fills = [
        dict(position_id="p", market="a", symbol="2330", purpose=purpose,
             quantity=1000, fill_at=day + "T09:01:00+08:00",
             recorded_at=recorded_at, session_date=day, price=100., fee_and_tax_twd=0.)
        for purpose, recorded_at in (
            ("entry", day + "T09:01:00+08:00"),
            ("entry_completion", day + "T11:30:00+08:00"),
        )
    ]
    rows = [dict(market="a", minute=f"{day}T{clock}+08:00", session_date=day,
                 initial_capital_twd=100000., cumulative_realized_net_pnl_twd=0.,
                 open_net_liquidation_pnl_twd=0., total_equity_twd=100000.,
                 cumulative_corporate_action_net_twd=0., cumulative_carry_cost_twd=0.,
                 margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=1)
            for clock in ("09:01", "13:30")]

    class Store:
        def prices(self, symbol, session_date):
            assert (symbol, session_date) == ("2330", day)
            start = datetime.fromisoformat(day + "T09:01:00+08:00")
            return {(start + timedelta(minutes=i)).isoformat(timespec="minutes"): 100.
                    for i in range(270)}

    state = {"modes": {"a": dict(initial_capital_twd=100000.,
                                  margin_carry_contract=MARGIN_CARRY_CONTRACT,
                                  share_replacement_ledger=[], corporate_action_ledger=[],
                                  carry_cost_ledger=[])}}
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, {day: {"a": [position]}}, Store(), state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 13),
    )
    assert len(rebuilt) == 270
    assert all(row["total_equity_twd"] == pytest.approx(100000.) for row in rebuilt)
    assert all(row["open_position_count"] == 1 for row in rebuilt)
    assert stats["differing_original_equity_points"] == 0


def test_new_paper_entry_does_not_reprice_older_cohort_without_a_minute_trade():
    days = ["2026-08-13", "2026-08-14"]
    base = dict(symbol="2330", side="long", buy_fee_rate=0., sell_fee_rate=0.,
                cash_buy_fee_rate=0., cash_sell_fee_rate=0.,
                margin_carry_contract=MARGIN_CARRY_CONTRACT)
    old = {**base, "position_id": "old", "entry_price": 9.51,
           "margin_converted_at": days[0] + "T13:30:00+08:00"}
    new = {**base, "position_id": "new", "entry_price": 9.51,
           "margin_converted_at": days[1] + "T13:30:00+08:00"}
    fills = [dict(position_id="old", market="a", symbol="2330", purpose="entry",
                  quantity=1000, fill_at=days[0]+"T09:01:00+08:00",
                  recorded_at=days[0]+"T09:01:00+08:00", session_date=days[0],
                  price=9.51, fee_and_tax_twd=0.),
             dict(position_id="new", market="a", symbol="2330", purpose="entry",
                  quantity=1000, fill_at=days[1]+"T09:01:00+08:00",
                  recorded_at=days[1]+"T09:01:00+08:00", session_date=days[1],
                  price=9.51, fee_and_tax_twd=0.)]
    rows = [dict(market="a", minute=f"{day}T{clock}+08:00", session_date=day,
                 initial_capital_twd=100000., cumulative_realized_net_pnl_twd=0.,
                 open_net_liquidation_pnl_twd=net,
                 total_equity_twd=100000. + net,
                 cumulative_corporate_action_net_twd=0., cumulative_carry_cost_twd=0.,
                 margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=count)
            for day, count, opening, closing in ((days[0], 1, 40., 40.),
                                                  (days[1], 2, 40., 100.))
            for clock, net in (("09:01", opening), ("13:30", closing))]

    class Store:
        def prices(self, symbol, day):
            first = datetime.fromisoformat(day + "T09:01:00+08:00")
            return {(first + timedelta(minutes=i)).isoformat(timespec="minutes"):
                    (9.55 if day == days[0] else 9.56)
                    for i in range(270) if day == days[0] or i >= 44}

    state = {"modes": {"a": dict(initial_capital_twd=100000.,
                                 margin_carry_contract=MARGIN_CARRY_CONTRACT,
                                 share_replacement_ledger=[], corporate_action_ledger=[],
                                 carry_cost_ledger=[])}}
    positions = {days[0]: {"a": [old]}, days[1]: {"a": [old, new]}}
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows, positions, Store(), state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14))
    by_minute = {row["minute"]: row for row in rebuilt}
    assert by_minute[days[1]+"T09:02+08:00"]["total_equity_twd"] == pytest.approx(100040.)
    assert by_minute[days[1]+"T09:44+08:00"]["total_equity_twd"] == pytest.approx(100040.)
    assert by_minute[days[1]+"T09:45+08:00"]["total_equity_twd"] == pytest.approx(100100.)
    assert stats["differing_original_equity_points"] == 0


def test_registered_0901_fill_is_carried_until_the_next_positive_volume_minute():
    day = "2026-09-24"
    position = dict(
        position_id="p",
        symbol="2330",
        side="long",
        entry_price=100.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        cash_buy_fee_rate=0.0,
        cash_sell_fee_rate=0.0,
        margin_carry_contract=MARGIN_CARRY_CONTRACT,
    )
    fills = [
        dict(
            position_id="p",
            market="a",
            symbol="2330",
            purpose="entry",
            quantity=1_000,
            fill_at=day + "T09:01:00+08:00",
            recorded_at=day + "T09:01:00+08:00",
            session_date=day,
            price=100.0,
            fee_and_tax_twd=0.0,
        )
    ]
    common = dict(
        market="a",
        session_date=day,
        initial_capital_twd=100_000.0,
        cumulative_realized_net_pnl_twd=0.0,
        cumulative_corporate_action_net_twd=0.0,
        cumulative_carry_cost_twd=0.0,
        margin_carry_contract=MARGIN_CARRY_CONTRACT,
        open_position_count=1,
    )
    rows = [
        {
            **common,
            "minute": f"{day}T{clock}+08:00",
            "open_net_liquidation_pnl_twd": pnl,
            "total_equity_twd": 100_000.0 + pnl,
        }
        for clock, pnl in (
            ("09:01", 0.0),
            ("09:02", 0.0),
            ("09:03", 20_000.0),
            ("13:30", 20_000.0),
        )
    ]

    class Store:
        def prices(self, symbol, session_date):
            assert (symbol, session_date) == ("2330", day)
            # 09:01 Close differs from the registered VWAP fill. With no 09:02
            # trade, the engine carries 100 until the 09:03 print arrives.
            return {
                day + "T09:01+08:00": 110.0,
                day + "T09:03+08:00": 120.0,
            }

    state = {
        "modes": {
            "a": dict(
                initial_capital_twd=100_000.0,
                margin_carry_contract=MARGIN_CARRY_CONTRACT,
                share_replacement_ledger=[],
                corporate_action_ledger=[],
                carry_cost_ledger=[],
            )
        }
    }
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows,
        {day: {"a": [position]}},
        Store(),
        state=state,
        fill_rows=fills,
        start=date(2026, 9, 24),
        end=date(2026, 9, 24),
    )
    by_minute = {row["minute"]: row for row in rebuilt}
    assert by_minute[day + "T09:02+08:00"]["total_equity_twd"] == 100_000.0
    assert by_minute[day + "T09:03+08:00"]["total_equity_twd"] == 120_000.0
    assert stats["differing_original_equity_points"] == 0


def test_retained_0901_valuation_reprices_inventory_before_later_print():
    day = "2026-09-24"
    position = dict(
        position_id="p",
        symbol="2330",
        side="long",
        entry_price=100.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        cash_buy_fee_rate=0.0,
        cash_sell_fee_rate=0.0,
        margin_carry_contract=MARGIN_CARRY_CONTRACT,
    )
    fills = [
        dict(
            position_id="p",
            market="a",
            symbol="2330",
            purpose="entry",
            quantity=1_000,
            fill_at=day + "T09:01:00+08:00",
            recorded_at=day + "T09:01:00+08:00",
            session_date=day,
            price=100.0,
            fee_and_tax_twd=0.0,
        )
    ]
    rows = [
        dict(
            market="a",
            minute=f"{day}T{clock}+08:00",
            session_date=day,
            initial_capital_twd=100_000.0,
            cumulative_realized_net_pnl_twd=0.0,
            open_net_liquidation_pnl_twd=pnl,
            total_equity_twd=100_000.0 + pnl,
            cumulative_corporate_action_net_twd=0.0,
            cumulative_carry_cost_twd=0.0,
            margin_carry_contract=MARGIN_CARRY_CONTRACT,
            open_position_count=1,
        )
        for clock, pnl in (
            ("09:01", 10_000.0),
            ("09:02", 10_000.0),
            ("09:03", 20_000.0),
            ("13:30", 20_000.0),
        )
    ]

    class Store:
        def prices(self, symbol, session_date):
            assert (symbol, session_date) == ("2330", day)
            return {
                day + "T09:01+08:00": 999.0,
                day + "T09:03+08:00": 120.0,
            }

    state = {
        "modes": {
            "a": dict(
                initial_capital_twd=100_000.0,
                margin_carry_contract=MARGIN_CARRY_CONTRACT,
                share_replacement_ledger=[],
                corporate_action_ledger=[],
                carry_cost_ledger=[],
            )
        }
    }
    rebuilt, stats = rebuild_carried_strategy_marks(
        rows,
        {day: {"a": [position]}},
        Store(),
        state=state,
        fill_rows=fills,
        start=date(2026, 9, 24),
        end=date(2026, 9, 24),
        opening_valuation_prices={day: {"2330": 110.0}},
    )
    by_minute = {row["minute"]: row for row in rebuilt}
    assert by_minute[day + "T09:02+08:00"]["total_equity_twd"] == 110_000.0
    assert by_minute[day + "T09:03+08:00"]["total_equity_twd"] == 120_000.0
    assert stats["differing_original_equity_points"] == 0


def test_opening_valuation_prices_require_exact_replay_entry_book(tmp_path):
    day = "2026-09-24"
    book = tmp_path / "replay_entry_books" / f"{day}.parquet"
    book.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330", "2317"],
            "valuation_price_0901": [101.0, None],
        }
    ).write_parquet(book)
    digest = hashlib.sha256(book.read_bytes()).hexdigest()
    (tmp_path / "rebuild_receipt.json").write_text(
        json.dumps(
            {
                "sessions": [
                    {
                        "session_date": day,
                        "historical_entry_books": {
                            "path": str(book.resolve()),
                            "sha256": digest,
                        },
                    }
                ]
            }
        )
    )

    assert _replay_opening_valuation_prices(
        tmp_path,
        start=date(2026, 9, 24),
        end=date(2026, 9, 24),
    ) == {day: {"2330": 101.0}}

    book.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="entry book changed"):
        _replay_opening_valuation_prices(
            tmp_path,
            start=date(2026, 9, 24),
            end=date(2026, 9, 24),
        )


def test_carried_revaluation_rejects_a_changed_endpoint_or_impossible_exit():
    rows, positions, store, state, fills = fixture()
    rows[-1]["cumulative_realized_net_pnl_twd"] = 99.
    with pytest.raises(RuntimeError, match="endpoint accounting"):
        rebuild_carried_strategy_marks(rows, positions, store, state=state, fill_rows=fills,
                                       start=date(2026, 8, 13), end=date(2026, 8, 14))
    diagnosis = validate_carried_endpoint_accounting(
        rows, positions, state=state, fill_rows=fills, collect_failures=True,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
    )
    assert diagnosis["status"] == "failed_component_check"
    assert diagnosis["failures"] == [{
        "market": "a", "minute": "2026-08-14T13:30+08:00",
        "field": "cumulative_realized_net_pnl_twd", "recorded_at": None,
        "retained_value": 99., "journal_value": -2510.,
        "historical_minute_replay": None,
        "opening_mark_revalued_from_completed_bar": None,
    }]


def test_carried_opening_revaluation_requires_completed_bar_and_preserves_fills():
    rows, positions, store, state, fills = fixture()
    original_fills = deepcopy(fills)
    original_prices = store.prices

    class ShiftedStore:
        def prices(self, symbol, day):
            prices = original_prices(symbol, day)
            if day == "2026-08-13":
                prices["2026-08-13T09:01+08:00"] = 101.
            return prices

    rebuilt, _ = rebuild_carried_strategy_marks(
        rows, positions, ShiftedStore(), state=state, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 14),
        revalue_opening_marks=True,
    )
    opening = next(row for row in rebuilt if row["minute"] == "2026-08-13T09:01+08:00")
    closing = next(row for row in rebuilt if row["minute"] == "2026-08-13T13:30+08:00")
    assert opening["opening_mark_revalued_from_completed_bar"] is True
    assert opening["total_equity_twd"] == pytest.approx(100990.)
    assert closing["total_equity_twd"] == rows[1]["total_equity_twd"]
    assert fills == original_fills

    class MissingOpeningStore:
        def prices(self, symbol, day):
            prices = original_prices(symbol, day)
            if day == "2026-08-13":
                prices.pop("2026-08-13T09:01+08:00")
            return prices

    with pytest.raises(RuntimeError, match="missing completed 09:01 valuation bar"):
        rebuild_carried_strategy_marks(
            rows, positions, MissingOpeningStore(), state=state, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 14),
            revalue_opening_marks=True,
        )


def test_carried_prices_require_exact_source_receipt_and_positive_trade_volume(tmp_path):
    from scripts.rebuild_tw_day_trade_minute_curves import MinutePriceStore
    from downloader.download_shioaji_tw_minute_kbars import RECEIPT_SCHEMA_VERSION, SOURCE_NAME, STORAGE_FREQUENCY
    path = tmp_path / "minute_chunks/2330/2026-08-13_2026-08-13.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2026, 8, 13)]*2,
                  "ts": [datetime(2026, 8, 13, 9, 1), datetime(2026, 8, 13, 9, 2)],
                  "Close": [100., 200.], "Volume": [1., 0.]}).write_parquet(path)
    proof = dict(schema_version=RECEIPT_SCHEMA_VERSION, source=SOURCE_NAME, storage_frequency=STORAGE_FREQUENCY,
                 symbol="2330", start_date="2026-08-13", end_date="2026-08-13", status="ok", rows=2,
                 returned_dates=["2026-08-13"], output_receipt=dict(path=str(path), size=path.stat().st_size,
                     sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    store = MinutePriceStore([tmp_path], [], require_receipts=True)
    store.prepare({"2330": {"2026-08-13"}})
    assert not store.prices("2330", "2026-08-13")
    path.with_suffix(".receipt.json").write_text(json.dumps(proof))
    store.invalidate("2330", "2026-08-13")
    store.prepare({"2330": {"2026-08-13"}})
    assert store.prices("2330", "2026-08-13") == {"2026-08-13T09:01+08:00": 100.}
    proof["output_receipt"]["sha256"] = "changed"
    path.with_suffix(".receipt.json").write_text(json.dumps(proof))
    store.invalidate("2330", "2026-08-13")
    assert not store.prices("2330", "2026-08-13")


def test_live_writer_prevents_late_publication_during_opening_window(tmp_path):
    import fcntl
    from scripts.rebuild_tw_day_trade_minute_curves import _assert_minute_publication_window
    with (tmp_path / ".engine.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="protected window"):
            _assert_minute_publication_window(tmp_path, datetime(2026, 9, 10, 9, 0))
        _assert_minute_publication_window(tmp_path, datetime(2026, 9, 10, 3, 0))
