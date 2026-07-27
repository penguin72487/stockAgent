from __future__ import annotations

import numpy as np
import pytest

from stockagent.backtest.tw_integer_execution import TaiwanIntegerState
from stockagent.backtest.tw_dual_session_integer import (
    _execute_signed_integer_event,
    run_tw_cash_dual_session_integer,
    run_tw_overnight_dual_session_integer,
)


def _phase_mask(time: int, symbols: int = 1) -> np.ndarray:
    return np.ones((time, 2, symbols), dtype=np.bool_)


def _no_actions(time: int, symbols: int = 1) -> np.ndarray:
    return np.zeros((time, 2, symbols), dtype=np.bool_)


def _assert_event_weight_audit(result) -> None:
    np.testing.assert_allclose(
        result.executed_buy_weights,
        result.executed_long_buy_weights
        + result.executed_short_cover_weights,
    )
    np.testing.assert_allclose(
        result.executed_sell_weights,
        result.executed_long_sell_weights
        + result.executed_short_open_weights,
    )
    np.testing.assert_allclose(
        result.event_turnovers,
        (
            result.executed_buy_weights
            + result.executed_sell_weights
        ).sum(axis=2),
        rtol=0.0,
        atol=1e-12,
    )


def test_tw_cash_phase_oracle_nets_both_events_and_settles_once_per_day() -> None:
    actions = np.zeros((3, 2, 1), dtype=np.float64)
    actions[0, 0, 0] = 0.5
    masks = _phase_mask(3)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0], [10.0], [10.0]]),
        np.array([[12.0], [12.0], [12.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(3),
        initial_cash=10_000.0,
    )

    # OPEN buys 500 shares for 5,000 and CLOSE sells them for 6,000. The
    # exchange day owns one +1,000 net claim, never simultaneous gross
    # payable/receivable queues from settling or enqueueing each phase.
    np.testing.assert_array_equal(result.open_positions[:, 0], [500, 0, 0])
    np.testing.assert_array_equal(result.positions[:, 0], [0, 0, 0])
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[0, :, 0],
        [500, 0],
    )
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[0, :, 0],
        [0, 500],
    )
    np.testing.assert_allclose(result.settlement_net_history, [1_000.0, 0.0, 0.0])
    np.testing.assert_allclose(result.payable_queue_history, 0.0)
    np.testing.assert_allclose(
        result.receivable_queue_history,
        [[0.0, 1_000.0], [1_000.0, 0.0], [0.0, 0.0]],
    )
    np.testing.assert_allclose(
        result.settled_cash_history,
        [10_000.0, 10_000.0, 11_000.0],
    )
    np.testing.assert_allclose(result.nav_history, [11_000.0] * 3)
    np.testing.assert_allclose(result.event_turnovers[0], [0.5, 0.6])
    _assert_event_weight_audit(result)


def test_tw_cash_phase_oracle_uses_independent_minimum_and_rounded_event_fees() -> None:
    masks = _phase_mask(1)
    actions = np.array([[[0.01], [0.0]]], dtype=np.float64)
    minimum = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[10.0]]),
        masks,
        masks,
        masks,
        0.001,
        0.001,
        minimum_commission=20.0,
        commission_rounding="floor",
        tax_rounding="floor",
        unresolved_corporate_action_mask=_no_actions(1),
        initial_cash=1_000.0,
    )

    np.testing.assert_allclose(minimum.event_fee_history[0], [20.0, 20.0])
    assert minimum.fee_history[0] == pytest.approx(40.0)
    assert minimum.settlement_net_history[0] == pytest.approx(-40.0)
    assert minimum.nav_history[0] == pytest.approx(960.0)

    rounded_actions = np.array([[[0.4], [0.0]]], dtype=np.float64)
    rounded = run_tw_cash_dual_session_integer(
        rounded_actions,
        np.array([[400.0]]),
        np.array([[400.0]]),
        masks,
        masks,
        masks,
        0.00125,
        0.00380,
        commission_rounding="half_up",
        tax_rounding="floor",
        unresolved_corporate_action_mask=_no_actions(1),
        initial_cash=1_000.0,
    )

    # Per phase: buy commission round_half_up(0.5)=1; close sell
    # commission=1 plus floor(TWD 1.02 tax)=1.
    np.testing.assert_allclose(rounded.event_fee_history[0], [1.0, 2.0])
    assert rounded.fee_history[0] == pytest.approx(3.0)


def test_tw_cash_phase_oracle_keeps_one_shared_daily_volume_budget() -> None:
    actions = np.array([[[1.0], [0.0]]], dtype=np.float64)
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        daily_volumes=np.array([[50.0]]),
        max_volume_participation=1.0,
        unresolved_corporate_action_mask=_no_actions(1),
        initial_cash=1_000.0,
    )

    # OPEN consumes all 50 causal shares. CLOSE cannot reuse another 50-share
    # budget merely because it is a different event.
    np.testing.assert_array_equal(result.open_positions[:, 0], [50])
    np.testing.assert_array_equal(result.positions[:, 0], [50])
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[0, :, 0],
        [50, 0],
    )
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[0, :, 0],
        [0, 0],
    )


def test_tw_cash_phase_oracle_reduces_before_crossing_zero() -> None:
    masks = _phase_mask(2)
    volumes = np.array([[1_000.0], [20.0]])
    common = dict(
        open_prices=np.full((2, 1), 10.0),
        close_prices=np.full((2, 1), 10.0),
        tradable_mask=masks,
        can_buy_mask=masks,
        can_sell_mask=masks,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((2, 1), 1_000, dtype=np.int64),
        short_lot_sizes=1,
        daily_volumes=volumes,
        max_volume_participation=1.0,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=1_000.0,
    )

    long_to_short = np.zeros((2, 2, 1), dtype=np.float64)
    long_to_short[0, 1, 0] = 0.4
    long_to_short[1, 0, 0] = -0.4
    long_to_short[1, 1, 0] = 0.2
    long_result = run_tw_cash_dual_session_integer(
        long_to_short,
        **common,
    )
    np.testing.assert_array_equal(long_result.open_positions[:, 0], [0, 20])
    np.testing.assert_array_equal(long_result.positions[:, 0], [40, 20])
    np.testing.assert_array_equal(
        long_result.executed_long_sell_shares[1, :, 0],
        [20, 0],
    )
    np.testing.assert_array_equal(
        long_result.executed_short_open_shares[1, :, 0],
        [0, 0],
    )

    short_to_long = np.zeros((2, 2, 1), dtype=np.float64)
    short_to_long[0, 1, 0] = -0.4
    short_to_long[1, 0, 0] = 0.4
    short_to_long[1, 1, 0] = -0.2
    short_result = run_tw_cash_dual_session_integer(
        short_to_long,
        **common,
    )
    np.testing.assert_array_equal(short_result.open_positions[:, 0], [0, -20])
    np.testing.assert_array_equal(short_result.positions[:, 0], [-40, -20])
    np.testing.assert_array_equal(
        short_result.executed_short_cover_shares[1, :, 0],
        [20, 0],
    )
    np.testing.assert_array_equal(
        short_result.executed_long_buy_shares[1, :, 0],
        [0, 0],
    )


def test_tw_cash_phase_oracle_scales_signed_endpoints_by_shared_turnover() -> None:
    actions = np.zeros((2, 2, 2), dtype=np.float64)
    actions[0, 1] = [0.4, 0.0]
    actions[1, 0] = [-0.4, 0.4]
    actions[1, 1] = [0.0, 0.2]
    masks = _phase_mask(2, symbols=2)
    result = run_tw_cash_dual_session_integer(
        actions,
        np.full((2, 2), 10.0),
        np.full((2, 2), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((2, 2), 1_000, dtype=np.int64),
        max_turnover_ratio=0.6,
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(2, symbols=2),
        initial_cash=1_000.0,
    )

    np.testing.assert_array_equal(result.open_positions[1], [0, 20])
    np.testing.assert_array_equal(result.positions[1], [0, 20])
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[1, 0],
        [40, 0],
    )
    np.testing.assert_array_equal(
        result.executed_short_open_shares[1, 0],
        [0, 0],
    )
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[1, 0],
        [0, 20],
    )


def test_tw_cash_phase_oracle_permissions_block_cross_zero_bypass() -> None:
    actions = np.zeros((2, 2, 1), dtype=np.float64)
    actions[0, 1, 0] = 0.4
    actions[1, :, 0] = -0.4
    masks = _phase_mask(2)
    can_sell = masks.copy()
    can_sell[1, :, 0] = False
    result = run_tw_cash_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        can_sell,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((2, 1), 1_000, dtype=np.int64),
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=1_000.0,
    )

    np.testing.assert_array_equal(result.open_positions[:, 0], [0, 40])
    np.testing.assert_array_equal(result.positions[:, 0], [40, 40])
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[1, :, 0],
        [0, 0],
    )
    np.testing.assert_array_equal(
        result.executed_short_open_shares[1, :, 0],
        [0, 0],
    )


def test_tw_cash_phase_masks_and_prices_are_event_specific() -> None:
    actions = np.array([[[0.5], [0.5]]], dtype=np.float64)
    selection = _phase_mask(1)
    can_buy = _phase_mask(1)
    can_buy[0, 0, 0] = False
    can_sell = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[5.0]]),
        np.array([[10.0]]),
        selection,
        can_buy,
        can_sell,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(1),
        initial_cash=1_000.0,
    )

    # OPEN cannot borrow CLOSE's buy permission. CLOSE allocates against its
    # exact TWD 10 auction price, producing 50 rather than 100 shares.
    np.testing.assert_array_equal(result.open_positions[:, 0], [0])
    np.testing.assert_array_equal(result.positions[:, 0], [50])
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[0, :, 0],
        [0, 50],
    )


def test_tw_cash_phase_signed_short_preserves_exact_margin_collateral() -> None:
    actions = np.zeros((3, 2, 1), dtype=np.float64)
    actions[0, :, 0] = -0.5
    masks = _phase_mask(3)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.03], [9.0], [9.0]]),
        np.array([[10.03], [9.0], [9.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((3, 1), 100_000, dtype=np.int64),
        short_lot_sizes=1_000,
        unresolved_corporate_action_mask=_no_actions(3),
        initial_cash=100_000.0,
    )

    # 0.5 * 100,000 / 10.03 floors to four 1,000-share short lots.
    # Margin ceil(4,000 * 10.03 * 0.9 / 100) * 100 = 36,200.
    np.testing.assert_array_equal(result.positions[:, 0], [-4_000, 0, 0])
    np.testing.assert_array_equal(
        result.executed_short_open_shares[0, :, 0],
        [4_000, 0],
    )
    np.testing.assert_array_equal(
        result.executed_short_cover_shares[1, :, 0],
        [4_000, 0],
    )
    assert result.short_sale_collateral_history[0, 0] == pytest.approx(40_120.0)
    assert result.short_margin_collateral_history[0, 0] == pytest.approx(36_200.0)
    np.testing.assert_allclose(
        result.payable_queue_history[0],
        [0.0, 36_200.0],
    )
    assert result.nav_history[1] == pytest.approx(104_120.0)
    _assert_event_weight_audit(result)


def test_tw_cash_short_inventory_is_shared_across_open_and_close() -> None:
    actions = np.array([[[-0.5], [-1.0]]], dtype=np.float64)
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.array([[1_000]], dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(1),
        initial_cash=100_000.0,
    )

    # OPEN consumes the complete demonstrated 1,000-share inventory. CLOSE
    # cannot manufacture another per-phase capacity budget.
    np.testing.assert_array_equal(
        result.executed_short_open_shares[0, :, 0],
        [1_000, 0],
    )
    assert result.positions[0, 0] == -1_000
    _assert_event_weight_audit(result)


def test_tw_cash_reallocates_orphaned_collateral_to_remaining_short_liability() -> None:
    actions = np.zeros((2, 2, 2), dtype=np.float64)
    actions[0, 1] = [-0.3, -0.3]
    actions[1, 0] = [0.0, -9.0 / 14.0]
    actions[1, 1] = [0.0, -9.0 / 14.0]
    masks = _phase_mask(2, symbols=2)
    result = run_tw_cash_dual_session_integer(
        actions,
        np.asarray([[100.0, 100.0], [150.0, 150.0]]),
        np.asarray([[100.0, 100.0], [150.0, 150.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((2, 2), 10_000, dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(2, symbols=2),
        initial_cash=100_000.0,
    )

    np.testing.assert_array_equal(result.final_state.holdings, [0, -300])
    final_sale = result.final_state.short_sale_collateral
    final_margin = result.final_state.short_margin_collateral
    assert final_sale is not None
    assert final_margin is not None
    assert final_sale[0] == 0.0
    assert final_margin[0] == 0.0
    assert final_sale[1] == pytest.approx(final_sale.sum())
    assert final_margin[1] == pytest.approx(final_margin.sum())
    assert (
        final_sale
        + final_margin
    ).sum() == pytest.approx(1.3 * 300 * 150)


def _underwater_short_state() -> TaiwanIntegerState:
    return TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=100.0,
        holdings=np.asarray([-80], dtype=np.int64),
        payable_queue=np.zeros(2, dtype=np.float64),
        receivable_queue=np.asarray([0.0, 0.0, 800.0]),
        last_nav=300.0,
        alive=True,
        last_weights=np.asarray([-800.0 / 300.0]),
        short_sale_collateral=np.asarray([100.0]),
        short_margin_collateral=np.asarray([100.0]),
    )


def _maintenance_locked_short_state() -> TaiwanIntegerState:
    return TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=0.0,
        holdings=np.asarray([-80], dtype=np.int64),
        payable_queue=np.zeros(2, dtype=np.float64),
        receivable_queue=np.asarray([0.0, 0.0, 540.0]),
        last_nav=700.0,
        alive=True,
        last_weights=np.asarray([-800.0 / 700.0]),
        short_sale_collateral=np.asarray([480.0]),
        short_margin_collateral=np.asarray([480.0]),
    )


def test_tw_cash_maintenance_blocked_release_is_not_a_cover_funding_source() -> None:
    actions = np.full((1, 2, 1), -1.0, dtype=np.float64)
    masks = _phase_mask(1)
    result = run_tw_cash_dual_session_integer(
        actions,
        np.asarray([[10.0]]),
        np.asarray([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.asarray([[1_000]], dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(1),
        claim_queue_sessions=3,
        initial_state=_maintenance_locked_short_state(),
    )

    np.testing.assert_array_equal(
        result.executed_short_cover_shares[0, :, 0],
        [0, 0],
    )
    np.testing.assert_array_equal(result.final_state.holdings, [-80])
    np.testing.assert_allclose(result.final_state.payable_queue, 0.0)


def test_tw_cash_voluntary_underwater_cover_respects_future_buying_power() -> None:
    actions = np.zeros((1, 2, 1), dtype=np.float64)
    masks = _phase_mask(1)
    result = run_tw_cash_dual_session_integer(
        actions,
        np.asarray([[10.0]]),
        np.asarray([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.asarray([[1_000]], dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(1),
        claim_queue_sessions=3,
        initial_state=_underwater_short_state(),
    )

    # Maintenance locks both collateral pools, so each TWD 10 cover consumes
    # TWD 10.  Only ten voluntary shares fit the TWD 100 cash available before
    # the same-due TWD 800 receivable.
    np.testing.assert_array_equal(
        result.executed_short_cover_shares[0, :, 0],
        [10, 0],
    )
    np.testing.assert_array_equal(result.final_state.holdings, [-70])
    np.testing.assert_allclose(result.final_state.payable_queue, [0.0, 100.0])
    np.testing.assert_allclose(
        result.final_state.receivable_queue,
        [0.0, 800.0, 0.0],
    )


def test_integer_cover_funding_finds_non_prefix_feasible_lot_island() -> None:
    zeros_i64 = np.zeros(2, dtype=np.int64)
    zeros_f64 = np.zeros(2, dtype=np.float64)
    allowed = np.ones(2, dtype=np.bool_)
    result = _execute_signed_integer_event(
        holdings=np.asarray([-100, -100], dtype=np.int64),
        desired_holdings=np.asarray([-50, 0], dtype=np.int64),
        mandatory_long_sell=zeros_i64,
        mandatory_short_cover=np.asarray([50, 0], dtype=np.int64),
        prices=np.asarray([10.0, 10.0]),
        raw_price_matrix=np.asarray([[10.0, 10.0]]),
        time_index=0,
        price_name="open_prices",
        can_buy=allowed,
        can_sell=allowed,
        can_short_open=allowed,
        commission_rates=zeros_f64,
        sell_tax_rates=zeros_f64,
        minimum_commission=0.0,
        commission_rounding="none",
        tax_rounding="none",
        long_lot_sizes=np.ones(2, dtype=np.int64),
        short_lot_sizes=np.ones(2, dtype=np.int64),
        short_margin_rates=np.full(2, 0.9),
        short_handling_rates=zeros_f64,
        short_sale_collateral=np.asarray([700.0, 200.0]),
        short_margin_collateral=np.asarray([700.0, 200.0]),
        short_maintenance_ratio=1.3,
        cash=390.0,
        payable=np.zeros(2, dtype=np.float64),
        receivable=np.zeros(2, dtype=np.float64),
        pending_net_claim=0.0,
        remaining_volume=None,
        remaining_turnover=None,
        remaining_short_capacity=None,
        event_nav=190.0,
        gross_budget=1.0,
    )

    # The mandatory 50-share cover initially unlocks too little maintenance
    # collateral.  Additional covers become feasible only for requests 87..98;
    # 99 and 100 are again unaffordable.  Prefix bisection misses this island,
    # while descending exact lot endpoints must select its global maximum.
    np.testing.assert_array_equal(result.short_cover, [50, 98])
    np.testing.assert_array_equal(result.holdings, [-50, -2])
    assert result.settlement_net == pytest.approx(-388.0)


def test_tw_overnight_funding_finds_non_prefix_feasible_lot_island() -> None:
    actions = np.zeros((1, 3, 2), dtype=np.float64)
    actions[0, 0] = [0.5, 1.0]
    masks = _phase_mask(1, symbols=2)
    initial_state = TaiwanIntegerState(
        mode="tw_overnight",
        settled_cash=390.0,
        holdings=np.asarray([-100, -100], dtype=np.int64),
        payable_queue=np.zeros(2, dtype=np.float64),
        receivable_queue=np.zeros(2, dtype=np.float64),
        alive=True,
        last_nav=190.0,
        last_weights=np.asarray([-1_000.0 / 190.0] * 2),
        short_sale_collateral=np.asarray([700.0, 200.0]),
        short_margin_collateral=np.asarray([700.0, 200.0]),
    )

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((1, 2), 10.0),
        np.full((1, 2), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((1, 2), 1_000, dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(1, symbols=2),
        lot_sizes=1,
        initial_state=initial_state,
    )

    # The first symbol's 50-share due cover is the producer anchor. It is not
    # affordable in isolation, but covering enough of the second symbol
    # unlocks maintenance collateral. The OPEN global maximum is 98 shares;
    # the remaining two due shares are then closed mandatorily at CLOSE.
    np.testing.assert_array_equal(
        result.executed_short_cover_shares[0],
        [[50, 98], [50, 2]],
    )
    np.testing.assert_array_equal(result.open_positions[0], [-50, -2])
    np.testing.assert_array_equal(result.positions[0], [0, 0])
    np.testing.assert_allclose(result.settlement_net_history, [-200.0])
    assert not result.default_mask.any()
    assert result.final_state.alive


def test_tw_cash_mandatory_underwater_cover_bypasses_cap_then_defaults() -> None:
    actions = np.zeros((3, 2, 1), dtype=np.float64)
    masks = _phase_mask(3)
    forced = np.zeros((3, 2, 1), dtype=np.bool_)
    forced[0, 0, 0] = True
    result = run_tw_cash_dual_session_integer(
        actions,
        np.full((3, 1), 10.0),
        np.full((3, 1), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        force_short_cover_mask=forced,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((3, 1), 1_000, dtype=np.int64),
        short_lot_sizes=1,
        unresolved_corporate_action_mask=_no_actions(3),
        claim_queue_sessions=3,
        initial_state=_underwater_short_state(),
    )

    np.testing.assert_array_equal(
        result.executed_short_cover_shares[0, :, 0],
        [80, 0],
    )
    np.testing.assert_array_equal(result.default_mask, [False, False, True])
    assert not result.final_state.alive


def test_tw_overnight_cohort_must_exit_by_next_close_and_bypasses_close_cap() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 1, 0] = 0.5
    actions[0, 2, 0] = 0.25
    actions[1, 0, 0] = 0.4
    masks = _phase_mask(2)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        daily_volumes=np.array([[1_000.0], [300.0]]),
        max_volume_participation=1.0,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=10_000.0,
    )

    np.testing.assert_array_equal(result.positions[:, 0], [750, 0])
    np.testing.assert_array_equal(result.due_positions_history[:, 0], [750, 0])
    # Next OPEN voluntarily exits 40%=300 shares, consuming the full daily
    # volume budget. The remaining 450 due shares still exit mandatorily at
    # CLOSE; mandatory cohort expiry bypasses voluntary participation limits.
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[1, :, 0],
        [300, 450],
    )
    assert not result.default_mask.any()
    np.testing.assert_array_equal(result.final_due_positions, [0])
    _assert_event_weight_audit(result)


def test_tw_overnight_signed_short_cohort_uses_both_entry_events() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 1, 0] = -0.5
    actions[0, 2, 0] = -0.25
    actions[1, 0, 0] = 1.0
    masks = _phase_mask(2)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.full((2, 1), 10_000, dtype=np.int64),
        short_lot_sizes=1_000,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=100_000.0,
    )

    np.testing.assert_array_equal(
        result.executed_short_open_shares[0, :, 0],
        [5_000, 2_000],
    )
    np.testing.assert_array_equal(result.positions[:, 0], [-7_000, 0])
    np.testing.assert_array_equal(
        result.executed_short_cover_shares[1, :, 0],
        [7_000, 0],
    )
    assert not result.short_sale_collateral_history[1].any()
    assert not result.short_margin_collateral_history[1].any()
    _assert_event_weight_audit(result)


def test_tw_overnight_same_direction_rollover_is_gross_exit_and_reentry() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 2, 0] = 0.5
    actions[1, 2, 0] = 0.4
    masks = _phase_mask(2)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        masks,
        0.001,
        0.001,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=1_000.0,
    )

    # Yesterday's 50-share cohort is a mandatory gross sale.  The new
    # same-direction target is a separate 39-share purchase after t0 fees;
    # neither order may be netted into an 11-share sale.
    np.testing.assert_array_equal(
        result.executed_long_sell_shares[1, :, 0],
        [0, 50],
    )
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[1, :, 0],
        [0, 39],
    )
    assert result.positions[1, 0] == 39
    assert result.event_fee_history[1, 1] == pytest.approx(0.89)
    _assert_event_weight_audit(result)


def test_tw_overnight_short_rollover_reconsumes_shared_inventory() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 2, 0] = -0.5
    actions[1, 2, 0] = -0.4
    masks = _phase_mask(2)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_shares=np.array([[10_000], [4_000]], dtype=np.int64),
        short_lot_sizes=1_000,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=100_000.0,
    )

    np.testing.assert_array_equal(
        result.executed_short_cover_shares[1, :, 0],
        [0, 5_000],
    )
    np.testing.assert_array_equal(
        result.executed_short_open_shares[1, :, 0],
        [0, 4_000],
    )
    assert result.positions[1, 0] == -4_000
    _assert_event_weight_audit(result)


def test_tw_overnight_unavailable_mandatory_close_is_absorbing_default() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 1, 0] = 0.5
    selection = _phase_mask(2)
    can_buy = _phase_mask(2)
    can_sell = _phase_mask(2)
    can_sell[1, :, 0] = False

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        selection,
        can_buy,
        can_sell,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=10_000.0,
    )

    assert result.default_mask.tolist() == [False, True]
    assert np.isneginf(result.strategy_returns[1])
    assert not result.final_state.alive
    np.testing.assert_array_equal(result.final_state.holdings, [0])


def test_tw_cash_corporate_action_exit_bypasses_shared_volume_and_turnover() -> None:
    actions = np.full((2, 2, 1), 0.5, dtype=np.float64)
    masks = _phase_mask(2)
    action_avoidance = _no_actions(2)
    action_avoidance[1, 1, 0] = True

    result = run_tw_cash_dual_session_integer(
        actions,
        np.full((2, 1), 10.0),
        np.full((2, 1), 10.0),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        daily_volumes=np.array([[100.0], [0.0]]),
        max_volume_participation=1.0,
        max_turnover_ratio=0.01,
        unresolved_corporate_action_mask=action_avoidance,
        initial_cash=1_000.0,
    )

    # The t0 turnover cap admits one share. No voluntary capacity remains at
    # t1, but the official avoidance liquidation is mandatory and exact.
    assert result.positions[0, 0] == 1
    assert result.positions[1, 0] == 0
    assert result.executed_long_sell_shares[1, 1, 0] == 1
    assert not result.default_mask.any()


def test_tw_cash_shared_volume_is_exact_share_inventory_across_auctions() -> None:
    actions = np.ones((1, 2, 1), dtype=np.float64)
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[20.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        daily_volumes=np.array([[100.0]]),
        max_volume_participation=0.5,
        unresolved_corporate_action_mask=_no_actions(1),
        lot_sizes=np.ones(1, dtype=np.int64),
        initial_cash=1_000.0,
    )

    assert result.positions[0, 0] == 50
    np.testing.assert_array_equal(
        result.executed_long_buy_shares[:, :, 0],
        [[50, 0]],
    )
    np.testing.assert_allclose(result.event_turnovers, [[0.5, 0.0]])
    assert result.final_weights[0] == pytest.approx(2.0 / 3.0)


def test_tw_cash_close_entitlement_uses_exact_price_and_claim_queue() -> None:
    actions = np.full((1, 2, 1), 0.5, dtype=np.float64)
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(1),
        cash_dividend_yield=np.array([[0.1]]),
        cash_dividend_payment_delay_sessions=np.array([[3]]),
        claim_queue_sessions=3,
        initial_cash=1_000.0,
    )

    # Fifty shares held at the exact TWD 10 close earn a TWD 50 claim. The
    # entitlement raises NAV immediately but remains in its verified payment
    # bucket instead of being fabricated as settled cash.
    assert result.execution_nav_history[0] == pytest.approx(1_000.0)
    assert result.nav_history[0] == pytest.approx(1_050.0)
    assert result.weights[0, 0] == pytest.approx(0.5)
    assert result.final_weights[0] == pytest.approx(0.5 / 1.05)
    np.testing.assert_allclose(
        result.receivable_queue_history[0],
        [0.0, 0.0, 50.0],
    )


def test_tw_cash_gap_execution_audit_uses_open_nav_denominator() -> None:
    actions = np.zeros((2, 2, 1), dtype=np.float64)
    actions[0, 1, 0] = 0.5
    masks = _phase_mask(2)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.array([[10.0], [20.0]]),
        np.array([[10.0], [20.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(2),
        initial_cash=1_000.0,
    )

    assert result.execution_nav_history[1] == pytest.approx(1_500.0)
    assert result.turnover[1] == pytest.approx(2.0 / 3.0)
    assert result.event_turnovers[1, 0] == pytest.approx(2.0 / 3.0)
    assert result.executed_long_sell_weights[1, 0, 0] == pytest.approx(
        2.0 / 3.0
    )


def test_tw_overnight_dividend_due_history_uses_execution_nav() -> None:
    actions = np.zeros((1, 3, 1), dtype=np.float64)
    actions[0, 2, 0] = 0.5
    masks = _phase_mask(1)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.array([[10.0]]),
        np.array([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        unresolved_corporate_action_mask=_no_actions(1),
        cash_dividend_yield=np.array([[0.1]]),
        cash_dividend_payment_delay_sessions=np.array([[3]]),
        claim_queue_sessions=3,
        initial_cash=1_000.0,
    )

    assert result.weights[0, 0] == pytest.approx(0.5)
    assert result.final_weights[0] == pytest.approx(0.5 / 1.05)
    assert result.due_positions_history is not None
    assert result.due_positions_history[0, 0] == 50
    assert result.final_due_positions is not None
    assert result.final_due_positions[0] == 50


def test_tw_overnight_full_run_equals_chunked_due_cohort_continuation() -> None:
    actions = np.zeros((4, 3, 1), dtype=np.float64)
    actions[0, 1, 0] = 0.4
    actions[0, 2, 0] = 0.2
    actions[1, 0, 0] = 0.5
    actions[1, 2, 0] = 0.3
    actions[2, 0, 0] = 1.0
    actions[2, 1, 0] = 0.2
    actions[3, 0, 0] = 1.0
    opens = np.array([[10.0], [11.0], [9.0], [12.0]])
    closes = np.array([[10.5], [10.0], [11.0], [12.0]])
    masks = _phase_mask(4)
    avoidance = _no_actions(4)
    common = dict(
        buy_fee_rates=0.001,
        sell_fee_rates=0.004,
        minimum_commission=1.0,
        commission_rounding="half_up",
        tax_rounding="floor",
        initial_cash=100_000.0,
    )

    full = run_tw_overnight_dual_session_integer(
        actions,
        opens,
        closes,
        masks,
        masks,
        masks,
        unresolved_corporate_action_mask=avoidance,
        **common,
    )
    first = run_tw_overnight_dual_session_integer(
        actions[:1],
        opens[:1],
        closes[:1],
        masks[:1],
        masks[:1],
        masks[:1],
        unresolved_corporate_action_mask=avoidance[:1],
        **common,
    )
    second = run_tw_overnight_dual_session_integer(
        actions[1:],
        opens[1:],
        closes[1:],
        masks[1:],
        masks[1:],
        masks[1:],
        unresolved_corporate_action_mask=avoidance[1:],
        initial_state=first.final_state,
        **common,
    )

    for name in (
        "strategy_returns",
        "turnover",
        "event_turnovers",
        "positions",
        "open_positions",
        "nav_history",
        "payable_queue_history",
        "receivable_queue_history",
        "settlement_net_history",
        "fee_history",
        "event_fee_history",
        "executed_long_buy_shares",
        "executed_long_sell_shares",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate(
                [getattr(first, name), getattr(second, name)],
                axis=0,
            ),
            rtol=0.0,
            atol=1e-8,
        )
    np.testing.assert_array_equal(
        full.final_state.holdings,
        second.final_state.holdings,
    )
    np.testing.assert_allclose(
        full.final_state.payable_queue,
        second.final_state.payable_queue,
    )
    np.testing.assert_allclose(
        full.final_state.receivable_queue,
        second.final_state.receivable_queue,
    )
