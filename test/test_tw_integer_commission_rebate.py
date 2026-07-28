from __future__ import annotations

import numpy as np
import pytest

from stockagent.backtest.tw_dual_session_integer import (
    run_tw_cash_dual_session_integer,
    run_tw_overnight_dual_session_integer,
)
from stockagent.backtest.tw_integer_execution import (
    TaiwanIntegerState,
    run_tw_cash_integer,
    run_tw_day_trade_integer,
)


def _no_actions(shape: tuple[int, ...]) -> np.ndarray:
    return np.zeros(shape, dtype=np.bool_)


def _phase_mask(time: int) -> np.ndarray:
    return np.ones((time, 2, 1), dtype=np.bool_)


def test_cash_daily_rebate_uses_independent_minimum_and_rounding() -> None:
    targets = np.asarray([[0.5]], dtype=np.float64)

    result = run_tw_cash_integer(
        targets,
        np.asarray([[100.0]]),
        buy_fee_rates=0.0015,
        sell_fee_rates=0.0015,
        commission_rebate_rates=0.0012,
        commission_rebate_timing="daily_close",
        minimum_commission=10.0,
        commission_rounding="floor",
        unresolved_corporate_action_mask=_no_actions(targets.shape),
        initial_cash=20_000.0,
    )

    # TWD10,000 buy: gross=floor(15)=15, discounted=max(floor(3), 10)=10.
    # The exact earned rebate is 15-10=5, paid only after the CLOSE fill.
    np.testing.assert_array_equal(result.positions, [[100]])
    np.testing.assert_allclose(result.fee_history, [15.0])
    np.testing.assert_allclose(result.commission_rebate_accrued_history, [5.0])
    np.testing.assert_allclose(result.commission_rebate_paid_history, [5.0])
    np.testing.assert_allclose(result.settled_cash_history, [20_005.0])
    np.testing.assert_allclose(result.payable_queue_history, [[0.0, 10_015.0]])
    np.testing.assert_allclose(result.nav_history, [19_990.0])
    assert result.final_commission_rebate_current == 0.0
    assert result.final_commission_rebate_due == 0.0
    assert result.final_commission_rebate_month_id == 0


def test_day_trade_rebates_only_commission_not_sell_tax() -> None:
    targets = np.asarray([[0.1]], dtype=np.float64)
    masks = np.ones_like(targets, dtype=np.bool_)

    result = run_tw_day_trade_integer(
        targets,
        np.asarray([[100.0]]),
        np.asarray([[100.0]]),
        buy_fee_rates=0.0015,
        sell_fee_rates=0.0045,
        commission_rebate_rates=0.0012,
        eligibility_mask=masks,
        lot_sizes=1,
        commission_rounding="floor",
        tax_rounding="floor",
        tradable_mask=masks,
        can_buy_mask=masks,
        can_sell_mask=masks,
        day_trade_can_buy_open_mask=masks,
        day_trade_can_sell_open_mask=masks,
        initial_cash=100_000.0,
    )

    # Two TWD10,000 sides: gross commission is 15 each and tax is 30 on the
    # sell.  Only gross-net commission (15-3) is earned back on each side.
    np.testing.assert_allclose(result.fee_history, [60.0])
    np.testing.assert_allclose(result.commission_rebate_accrued_history, [24.0])
    np.testing.assert_allclose(result.commission_rebate_paid_history, [24.0])
    np.testing.assert_allclose(result.settlement_net_history, [-60.0])
    np.testing.assert_allclose(result.nav_history, [99_964.0])


def test_unpaid_monthly_rebate_is_nav_asset_but_not_buying_power() -> None:
    january = 2025 * 12 + 1
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=100.0,
        holdings=np.asarray([0], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=1_000.0,
        last_weights=np.zeros(1),
        commission_rebate_current=900.0,
        commission_rebate_month_id=january,
    )

    result = run_tw_cash_integer(
        np.asarray([[1.0]]),
        np.asarray([[10.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([january]),
        commission_rebate_payment_eligible_mask=np.asarray([True]),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        initial_state=state,
    )

    # NAV can size a 100-share request, but only TWD100 settled cash is
    # purchase-eligible, so the exact oracle admits ten shares.
    np.testing.assert_array_equal(result.positions, [[10]])
    np.testing.assert_allclose(result.nav_history, [1_000.0])
    np.testing.assert_allclose(result.commission_rebate_current_history, [900.0])
    np.testing.assert_allclose(result.commission_rebate_paid_history, [0.0])


def test_monthly_initial_aliases_and_padding_preserve_calendar_state() -> None:
    january = 2025 * 12 + 1
    february = 2025 * 12 + 2

    result = run_tw_cash_integer(
        np.zeros((2, 1)),
        np.ones((2, 1)),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([0, february]),
        commission_rebate_payment_eligible_mask=np.asarray([False, False]),
        state_advance_mask=np.asarray([False, True]),
        unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
        initial_cash=1_000.0,
        initial_commission_rebate_current=100.0,
        initial_commission_rebate_month_id=january,
    )

    # Padding observes no calendar event.  The first active February session
    # then rolls January's current bucket to due without changing NAV.
    np.testing.assert_allclose(result.nav_history, [1_100.0, 1_100.0])
    np.testing.assert_allclose(
        result.commission_rebate_current_history,
        [100.0, 0.0],
    )
    np.testing.assert_allclose(
        result.commission_rebate_due_history,
        [0.0, 100.0],
    )
    np.testing.assert_allclose(result.strategy_returns, [0.0, 0.0])


def test_dual_session_monthly_state_survives_chunk_and_pays_next_month() -> None:
    january = 2025 * 12 + 1
    february = 2025 * 12 + 2
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=1_000.0,
        holdings=np.asarray([0], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=1_100.0,
        last_weights=np.zeros(1),
        commission_rebate_current=100.0,
        commission_rebate_month_id=january,
    )
    actions = np.zeros((3, 2, 1), dtype=np.float64)
    masks = _phase_mask(3)
    common = dict(
        open_prices=np.full((3, 1), 10.0),
        close_prices=np.full((3, 1), 10.0),
        tradable_mask=masks,
        can_buy_mask=masks,
        can_sell_mask=masks,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([february, february, february]),
        commission_rebate_payment_eligible_mask=np.asarray(
            [False, False, True]
        ),
        unresolved_corporate_action_mask=np.zeros_like(masks),
    )

    full = run_tw_cash_dual_session_integer(
        actions,
        initial_state=state,
        **common,
    )
    first = run_tw_cash_dual_session_integer(
        actions[:1],
        open_prices=common["open_prices"][:1],
        close_prices=common["close_prices"][:1],
        tradable_mask=masks[:1],
        can_buy_mask=masks[:1],
        can_sell_mask=masks[:1],
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([february]),
        commission_rebate_payment_eligible_mask=np.asarray([False]),
        unresolved_corporate_action_mask=np.zeros_like(masks[:1]),
        initial_state=state,
    )
    second = run_tw_cash_dual_session_integer(
        actions[1:],
        open_prices=common["open_prices"][1:],
        close_prices=common["close_prices"][1:],
        tradable_mask=masks[1:],
        can_buy_mask=masks[1:],
        can_sell_mask=masks[1:],
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([february, february]),
        commission_rebate_payment_eligible_mask=np.asarray([False, True]),
        unresolved_corporate_action_mask=np.zeros_like(masks[1:]),
        initial_state=first.final_state,
    )

    np.testing.assert_allclose(
        full.commission_rebate_current_history,
        [0.0, 0.0, 0.0],
    )
    np.testing.assert_allclose(
        full.commission_rebate_due_history,
        [100.0, 100.0, 0.0],
    )
    np.testing.assert_allclose(
        full.commission_rebate_paid_history,
        [0.0, 0.0, 100.0],
    )
    np.testing.assert_allclose(
        full.settled_cash_history,
        [1_000.0, 1_000.0, 1_100.0],
    )
    np.testing.assert_allclose(
        full.commission_rebate_paid_history,
        np.concatenate(
            [
                first.commission_rebate_paid_history,
                second.commission_rebate_paid_history,
            ]
        ),
    )
    assert second.final_state.settled_cash == full.final_state.settled_cash
    assert second.final_state.last_nav == full.final_state.last_nav
    np.testing.assert_array_equal(
        second.final_state.holdings,
        full.final_state.holdings,
    )
    np.testing.assert_array_equal(
        second.final_state.payable_queue,
        full.final_state.payable_queue,
    )
    np.testing.assert_array_equal(
        second.final_state.receivable_queue,
        full.final_state.receivable_queue,
    )
    assert (
        second.final_state.commission_rebate_current
        == full.final_state.commission_rebate_current
    )
    assert (
        second.final_state.commission_rebate_due
        == full.final_state.commission_rebate_due
    )
    assert (
        second.final_state.commission_rebate_month_id
        == full.final_state.commission_rebate_month_id
    )


def test_dual_session_daily_rebate_tracks_each_auction() -> None:
    actions = np.asarray([[[0.5], [0.0]]], dtype=np.float64)
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        actions,
        np.asarray([[100.0]]),
        np.asarray([[100.0]]),
        masks,
        masks,
        masks,
        0.0015,
        0.0015,
        commission_rebate_rates=0.0012,
        minimum_commission=10.0,
        commission_rounding="floor",
        unresolved_corporate_action_mask=np.zeros_like(masks),
        initial_cash=20_000.0,
    )

    np.testing.assert_allclose(result.event_fee_history, [[15.0, 15.0]])
    np.testing.assert_allclose(
        result.event_commission_rebate_accrued,
        [[5.0, 5.0]],
    )
    np.testing.assert_allclose(
        result.commission_rebate_accrued_history,
        [10.0],
    )
    np.testing.assert_allclose(result.commission_rebate_paid_history, [10.0])
    np.testing.assert_allclose(result.settled_cash_history, [20_010.0])
    np.testing.assert_allclose(result.settlement_net_history, [-30.0])


def test_overnight_daily_rebate_tracks_close_entry_and_next_open_exit() -> None:
    actions = np.zeros((2, 3, 1), dtype=np.float64)
    actions[0, 2, 0] = 0.5
    actions[1, 0, 0] = 1.0
    masks = _phase_mask(2)

    result = run_tw_overnight_dual_session_integer(
        actions,
        np.full((2, 1), 100.0),
        np.full((2, 1), 100.0),
        masks,
        masks,
        masks,
        0.0015,
        0.0015,
        commission_rebate_rates=0.0012,
        minimum_commission=10.0,
        commission_rounding="floor",
        unresolved_corporate_action_mask=np.zeros_like(masks),
        initial_cash=20_000.0,
    )

    np.testing.assert_array_equal(result.positions[:, 0], [100, 0])
    np.testing.assert_allclose(
        result.event_commission_rebate_accrued,
        [[0.0, 5.0], [5.0, 0.0]],
    )
    np.testing.assert_allclose(
        result.commission_rebate_accrued_history,
        [5.0, 5.0],
    )
    np.testing.assert_allclose(
        result.commission_rebate_paid_history,
        [5.0, 5.0],
    )
    assert result.final_commission_rebate_current == 0.0
    assert result.final_commission_rebate_due == 0.0


def test_default_forfeits_all_pending_commission_rebates() -> None:
    february = 2025 * 12 + 2
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=10.0,
        holdings=np.asarray([0], dtype=np.int64),
        payable_queue=np.asarray([20.0, 0.0]),
        receivable_queue=np.zeros(2),
        last_nav=90.0,
        last_weights=np.zeros(1),
        commission_rebate_due=100.0,
        commission_rebate_month_id=february,
    )
    masks = _phase_mask(1)

    result = run_tw_cash_dual_session_integer(
        np.zeros((1, 2, 1)),
        np.asarray([[10.0]]),
        np.asarray([[10.0]]),
        masks,
        masks,
        masks,
        0.0,
        0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=np.asarray([february]),
        commission_rebate_payment_eligible_mask=np.asarray([True]),
        unresolved_corporate_action_mask=np.zeros_like(masks),
        initial_state=state,
    )

    np.testing.assert_array_equal(result.default_mask, [True])
    assert result.final_state.commission_rebate_current == 0.0
    assert result.final_state.commission_rebate_due == 0.0
    assert result.final_state.commission_rebate_month_id == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"commission_rebate_rates": 0.002},
        {"commission_rebate_timing": "monthly_15th"},
    ],
)
def test_integer_rebate_inputs_fail_closed(kwargs: dict[str, object]) -> None:
    targets = np.zeros((1, 1))

    with pytest.raises(ValueError, match="commission_rebate|session_month_ids"):
        run_tw_cash_integer(
            targets,
            np.ones((1, 1)),
            buy_fee_rates=0.001,
            sell_fee_rates=0.001,
            unresolved_corporate_action_mask=np.zeros_like(
                targets,
                dtype=np.bool_,
            ),
            **kwargs,
        )
