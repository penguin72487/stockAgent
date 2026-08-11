from __future__ import annotations

from datetime import date

import numpy as np
import pytest

from scripts.backtest_taifex_atm_straddle_rolling import (
    DayMarket,
    Fill,
    OptionContract,
)
from scripts.backtest_taifex_index_arbitrage import (
    CROSS_EXPIRY_VARIANT,
    CycleExit,
    CycleState,
    FilledLeg,
    LegIntent,
    _calendar_box_pair_candidate,
    _complete_series,
    _cycle_actual_result,
    _cycle_exit_result,
    _performance_metrics,
    _relationship_value_from_prices,
    _try_convergence_exit,
)


EXPIRY = date(2026, 7, 15)


def _state(*legs: tuple[LegIntent, float]) -> CycleState:
    return CycleState(
        cycle_id="test",
        variant_id="test",
        family="test",
        series="202607",
        expiry=EXPIRY,
        entry_date=date(2026, 7, 14),
        decision_ns=1,
        completion_ns=2,
        legs=tuple(
            FilledLeg(intent, Fill(event_ns=2, price=price), 0.0, 0.0)
            for intent, price in legs
        ),
        observed_edge_twd=0.0,
        estimated_cost_twd=0.0,
        relationship_value_points=0.0,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction="test",
        strikes=(),
    )


def test_equivalent_futures_payoff_cancels_common_settlement() -> None:
    state = _state(
        (LegIntent("future", "TX", -1, "202607"), 100.0),
        (LegIntent("future", "MTX", +4, "202607"), 99.0),
    )

    low = _cycle_actual_result(state, 80.0)
    high = _cycle_actual_result(state, 120.0)

    assert low["gross_pnl_twd"] == pytest.approx(200.0)
    assert high["gross_pnl_twd"] == pytest.approx(200.0)


def test_put_call_parity_package_cancels_common_settlement() -> None:
    state = _state(
        (LegIntent("option", "TXO", -1, "202607", 90.0, "C"), 17.0),
        (LegIntent("option", "TXO", +1, "202607", 90.0, "P"), 5.0),
        (LegIntent("future", "MTX", +1, "202607"), 100.0),
    )

    below = _cycle_actual_result(state, 80.0)
    above = _cycle_actual_result(state, 120.0)

    assert below["gross_pnl_twd"] == pytest.approx(100.0)
    assert above["gross_pnl_twd"] == pytest.approx(100.0)


def test_vertical_butterfly_and_box_terminal_payoffs() -> None:
    vertical = _state(
        (LegIntent("option", "TXO", +1, "202607", 100.0, "C"), 5.0),
        (LegIntent("option", "TXO", -1, "202607", 110.0, "C"), 7.0),
    )
    butterfly = _state(
        (LegIntent("option", "TXO", +1, "202607", 90.0, "P"), 1.0),
        (LegIntent("option", "TXO", -2, "202607", 100.0, "P"), 3.0),
        (LegIntent("option", "TXO", +1, "202607", 110.0, "P"), 1.0),
    )
    box = _state(
        (LegIntent("option", "TXO", +1, "202607", 90.0, "C"), 15.0),
        (LegIntent("option", "TXO", -1, "202607", 110.0, "C"), 5.0),
        (LegIntent("option", "TXO", +1, "202607", 110.0, "P"), 8.0),
        (LegIntent("option", "TXO", -1, "202607", 90.0, "P"), 3.0),
    )

    assert _cycle_actual_result(vertical, 90.0)["gross_pnl_twd"] == pytest.approx(100.0)
    assert _cycle_actual_result(butterfly, 100.0)["gross_pnl_twd"] == pytest.approx(
        700.0
    )
    assert _cycle_actual_result(box, 120.0)["gross_pnl_twd"] == pytest.approx(250.0)


def test_performance_metrics_compound_and_drawdown() -> None:
    returns = np.asarray([0.01, -0.02, 0.03], dtype=np.float64)
    metrics = _performance_metrics(returns)

    assert metrics["cumulative_compounded_return"] == pytest.approx(
        1.01 * 0.98 * 1.03 - 1.0
    )
    assert metrics["maximum_drawdown"] == pytest.approx(-0.02)
    assert metrics["annualized_sharpe"] is not None
    assert metrics["annualized_sortino"] is not None


def test_convergence_exit_uses_strictly_later_trades_and_round_trip_costs() -> None:
    low = OptionContract("202607", 100.0, "C")
    high = OptionContract("202607", 110.0, "C")
    state = CycleState(
        cycle_id="call_vertical_convergence",
        variant_id="call_vertical_bounds",
        family="vertical_bounds",
        series="202607",
        expiry=EXPIRY,
        entry_date=date(2026, 7, 14),
        decision_ns=1,
        completion_ns=2,
        legs=(
            FilledLeg(
                LegIntent("option", "TXO", +1, "202607", 100.0, "C"),
                Fill(event_ns=2, price=40.0),
                22.0,
                1.0,
            ),
            FilledLeg(
                LegIntent("option", "TXO", -1, "202607", 110.0, "C"),
                Fill(event_ns=2, price=60.0),
                22.0,
                1.0,
            ),
        ),
        observed_edge_twd=100.0,
        estimated_cost_twd=90.0,
        relationship_value_points=-20.0,
        lower_bound_points=0.0,
        upper_bound_points=10.0,
        direction="buy_underpriced_vertical",
        strikes=(100.0, 110.0),
    )
    market = DayMarket(
        trading_date=date(2026, 7, 14),
        tx_times_ns=np.asarray([1], dtype=np.int64),
        tx_prices=np.asarray([105.0]),
        option_events={
            low: (
                np.asarray([2, 10, 12], dtype=np.int64),
                np.asarray([40.0, 70.0, 70.0]),
            ),
            high: (
                np.asarray([2, 10, 13], dtype=np.int64),
                np.asarray([60.0, 50.0, 50.0]),
            ),
        },
        pair_availability=(),
    )

    exit_state, failed_attempts = _try_convergence_exit(
        market=market,
        state=state,
        start_ns=2,
        deadline_ns=20,
    )

    assert exit_state is not None
    assert exit_state.reason == "relationship_convergence"
    assert exit_state.decision_ns == 10
    assert [leg.fill.event_ns for leg in exit_state.legs] == [12, 13]
    assert all(leg.fill.event_ns > exit_state.decision_ns for leg in exit_state.legs)
    assert failed_attempts == 0
    result = _cycle_exit_result(state, exit_state)
    assert result["gross_pnl_twd"] == pytest.approx(2_000.0)
    assert result["total_fixed_fees_twd"] == pytest.approx(88.0)
    assert result["exit_taxes_twd"] > 0.0
    assert result["net_pnl_twd"] < result["gross_pnl_twd"]


def test_expiry_fallback_has_no_trading_exit_fee() -> None:
    state = _state(
        (LegIntent("future", "TX", -1, "202607"), 100.0),
        (LegIntent("future", "MTX", +4, "202607"), 99.0),
    )
    exit_state = CycleExit(
        trading_date=EXPIRY,
        reason="expiry_settlement",
        decision_ns=10,
        completion_ns=10,
        legs=(),
        settlement_price=110.0,
    )

    result = _cycle_exit_result(state, exit_state)

    assert result["exit_fees_twd"] == 0.0
    assert result["exit_taxes_twd"] == 0.0
    assert result["settlement_taxes_twd"] >= 0.0


def test_weekly_series_are_not_filtered_out_of_complete_chain() -> None:
    weekly = OptionContract("202607W2", 100.0, "C")
    monthly = OptionContract("202607", 100.0, "C")
    market = DayMarket(
        trading_date=date(2026, 7, 1),
        tx_times_ns=np.asarray([1], dtype=np.int64),
        tx_prices=np.asarray([100.0]),
        option_events={
            weekly: (np.asarray([1], dtype=np.int64), np.asarray([1.0])),
            monthly: (np.asarray([1], dtype=np.int64), np.asarray([1.0])),
        },
        pair_availability=(),
    )
    settlements = {
        "202607W2": (date(2026, 7, 8), {}),
        "202607": (date(2026, 7, 15), {}),
    }

    complete = _complete_series(
        market=market,
        settlements_by_series=settlements,
        verified_dates={date(2026, 7, 8), date(2026, 7, 15)},
    )

    assert complete == [
        ("202607W2", date(2026, 7, 8)),
        ("202607", date(2026, 7, 15)),
    ]


def test_cross_expiry_calendar_box_uses_both_weekly_series() -> None:
    near_series = "202607W2"
    far_series = "202607F2"
    low, high = 90.0, 110.0
    prices = {
        OptionContract(near_series, low, "C"): 15.0,
        OptionContract(near_series, high, "C"): 5.0,
        OptionContract(near_series, low, "P"): 3.0,
        OptionContract(near_series, high, "P"): 8.0,
        OptionContract(far_series, low, "C"): 30.0,
        OptionContract(far_series, high, "C"): 2.0,
        OptionContract(far_series, low, "P"): 3.0,
        OptionContract(far_series, high, "P"): 15.0,
    }
    market = DayMarket(
        trading_date=date(2026, 7, 1),
        tx_times_ns=np.asarray([1], dtype=np.int64),
        tx_prices=np.asarray([100.0]),
        option_events={},
        pair_availability=(),
    )

    candidate = _calendar_box_pair_candidate(
        decision_ns=1,
        near_series=near_series,
        near_expiry=date(2026, 7, 8),
        far_series=far_series,
        far_expiry=date(2026, 7, 10),
        latest=prices,
        market=market,
    )

    assert candidate is not None
    assert candidate.variant_id == CROSS_EXPIRY_VARIANT
    assert candidate.series == f"{near_series}|{far_series}"
    assert {intent.series for intent in candidate.intents} == {
        near_series,
        far_series,
    }
    assert candidate.observed_net_edge_twd > 0.0
    state = CycleState(
        cycle_id="cross_expiry",
        variant_id=candidate.variant_id,
        family=candidate.family,
        series=candidate.series,
        expiry=candidate.expiry,
        entry_date=market.trading_date,
        decision_ns=1,
        completion_ns=2,
        legs=tuple(
            FilledLeg(intent, Fill(2, prices[intent.option_contract]), 0.0, 0.0)
            for intent in candidate.intents
        ),
        observed_edge_twd=candidate.observed_edge_twd,
        estimated_cost_twd=candidate.estimated_cost_twd,
        relationship_value_points=candidate.relationship_value_points,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction=candidate.direction,
        strikes=candidate.strikes,
    )
    relationship_prices = {
        ("option", contract.series, contract.strike, contract.right): price
        for contract, price in prices.items()
    }
    assert _relationship_value_from_prices(state, relationship_prices) == pytest.approx(
        candidate.relationship_value_points
    )


def test_cross_expiry_box_settles_each_series_on_its_own_expiry() -> None:
    near_series = "202607W2"
    far_series = "202607F2"
    near_expiry = date(2026, 7, 8)
    far_expiry = date(2026, 7, 10)
    low, high = 9_000.0, 11_000.0
    intents = _long_box_intents_for_test(near_series, low, high) + tuple(
        LegIntent(
            intent.instrument_type,
            intent.product,
            -intent.quantity,
            intent.series,
            intent.strike,
            intent.option_right,
        )
        for intent in _long_box_intents_for_test(far_series, low, high)
    )
    state = CycleState(
        cycle_id="settled_cross_expiry",
        variant_id=CROSS_EXPIRY_VARIANT,
        family="calendar_box_spread",
        series=f"{near_series}|{far_series}",
        expiry=far_expiry,
        entry_date=date(2026, 7, 7),
        decision_ns=1,
        completion_ns=2,
        legs=tuple(FilledLeg(intent, Fill(2, 0.0), 0.0, 0.0) for intent in intents),
        observed_edge_twd=1_000.0,
        estimated_cost_twd=400.0,
        relationship_value_points=20.0,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction="test",
        strikes=(low, high),
    )
    settlements_by_series = {
        near_series: (near_expiry, {"settlement_price": 8_000.0}),
        far_series: (far_expiry, {"settlement_price": 12_000.0}),
    }
    exit_state = CycleExit(
        trading_date=far_expiry,
        reason="multi_expiry_settlement",
        decision_ns=10,
        completion_ns=10,
        legs=(),
        settlement_prices={near_series: 8_000.0, far_series: 12_000.0},
    )

    result = _cycle_exit_result(
        state,
        exit_state,
        settlements_by_series=settlements_by_series,
    )

    assert result["gross_pnl_twd"] == pytest.approx(0.0)
    assert result["exit_fees_twd"] == 0.0
    assert result["settlement_taxes_twd"] > 0.0


def _long_box_intents_for_test(
    series: str, low: float = 90.0, high: float = 110.0
) -> tuple[LegIntent, ...]:
    return (
        LegIntent("option", "TXO", +1, series, low, "C"),
        LegIntent("option", "TXO", -1, series, high, "C"),
        LegIntent("option", "TXO", +1, series, high, "P"),
        LegIntent("option", "TXO", -1, series, low, "P"),
    )
