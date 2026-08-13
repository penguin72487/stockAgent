from __future__ import annotations

import numpy as np
import pytest

from stockagent.backtest.tw_integer_execution import (
    TaiwanIntegerState,
    run_tw_cash_integer,
    run_tw_day_trade_integer,
)


def _no_corporate_actions(values: np.ndarray) -> np.ndarray:
    return np.zeros_like(values, dtype=np.bool_)


def _open_masks(values: np.ndarray) -> dict[str, np.ndarray]:
    return {
        "day_trade_can_buy_open_mask": np.ones_like(values, dtype=np.bool_),
        "day_trade_can_sell_open_mask": np.ones_like(values, dtype=np.bool_),
    }


def _assert_cash_ledger_identity(result, prices: np.ndarray) -> None:
    market_value = np.sum(result.positions * prices, axis=1)
    np.testing.assert_allclose(
        result.nav_history,
        result.settled_cash_history
        + market_value
        + result.receivable_history
        - result.payable_history,
        rtol=0.0,
        atol=1e-8,
    )


def _assert_flat_ledger_identity(result) -> None:
    np.testing.assert_allclose(
        result.nav_history,
        result.settled_cash_history
        + result.receivable_history
        - result.payable_history,
        rtol=0.0,
        atol=1e-8,
    )


def test_tw_cash_manual_t_plus_two_ledger_and_pending_claims() -> None:
    targets = np.array([[0.5], [0.0], [0.0], [0.0]])
    closes = np.array([[10.0], [12.0], [12.0], [12.0]])

    result = run_tw_cash_integer(
        targets,
        closes,
        buy_fee_rates=np.array([0.001]),
        sell_fee_rates=np.array([0.003]),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=10_000.0,
    )

    # t0: buy 500 shares, creating a 5,005 payable due at t2.
    # t1: sell them for 6,000 less 18 fee, creating a 5,982 receivable.
    np.testing.assert_array_equal(result.positions[:, 0], [500, 0, 0, 0])
    np.testing.assert_allclose(result.payable_queue_history[0], [0.0, 5_005.0])
    np.testing.assert_allclose(result.payable_queue_history[1], [5_005.0, 0.0])
    np.testing.assert_allclose(result.receivable_queue_history[1], [0.0, 5_982.0])
    np.testing.assert_allclose(
        result.settled_cash_history, [10_000.0, 10_000.0, 4_995.0, 10_977.0]
    )
    np.testing.assert_allclose(result.nav_history, [9_995.0, 10_977.0, 10_977.0, 10_977.0])
    np.testing.assert_allclose(np.exp(result.strategy_returns).prod(), 10_977.0 / 10_000.0)
    assert result.final_state.settled_cash == pytest.approx(10_977.0)
    assert not np.any(result.final_state.payable_queue)
    assert not np.any(result.final_state.receivable_queue)
    _assert_cash_ledger_identity(result, closes)


def test_tw_cash_end_of_horizon_keeps_unsettled_liability() -> None:
    targets = np.array([[0.5]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0]]),
        buy_fee_rates=0.001,
        sell_fee_rates=0.003,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=10_000.0,
    )

    assert result.final_state.settled_cash == pytest.approx(10_000.0)
    np.testing.assert_allclose(result.final_state.payable_queue, [0.0, 5_005.0])
    assert result.final_state.receivable_queue.sum() == 0.0
    assert result.final_state.last_nav == pytest.approx(9_995.0)
    _assert_cash_ledger_identity(result, np.array([[10.0]]))


def test_tw_cash_exact_cash_dividend_survives_sale_until_payment_date() -> None:
    targets = np.array([[1.0], [0.0], [0.0], [0.0]])
    closes = np.array([[10.0], [9.0], [9.0], [9.0]])
    forward_returns = np.log(np.array([[0.9], [1.0], [1.0], [1.0]]))
    dividend_yield = np.array([[0.1], [0.0], [0.0], [0.0]])
    payment_delay = np.array([[3], [0], [0], [0]], dtype=np.int64)

    result = run_tw_cash_integer(
        targets,
        closes,
        future_log_returns=forward_returns,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        cash_dividend_yield=dividend_yield,
        cash_dividend_payment_delay_sessions=payment_delay,
        claim_queue_sessions=4,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        initial_cash=1_000.0,
    )

    np.testing.assert_array_equal(result.positions[:, 0], [100, 0, 0, 0])
    # The raw 10% ex-dividend price drop is offset by a TWD 100 entitlement.
    np.testing.assert_allclose(result.nav_history, [1_000.0] * 4)
    # Selling on the next row adds TWD 900 to the same due-date bucket, but it
    # cannot erase the already-earned TWD 100 dividend claim.
    np.testing.assert_allclose(
        result.receivable_queue_history[1], [0.0, 1_000.0, 0.0, 0.0]
    )
    np.testing.assert_allclose(
        result.settled_cash_history, [1_000.0, 1_000.0, 0.0, 1_000.0]
    )


def test_tw_cash_exact_cash_dividend_rejects_truncated_claim_queue() -> None:
    targets = np.array([[0.0]])
    with pytest.raises(ValueError, match="payment delay exceeds"):
        run_tw_cash_integer(
            targets,
            np.array([[10.0]]),
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            cash_dividend_yield=np.array([[0.1]]),
            cash_dividend_payment_delay_sessions=np.array([[5]]),
            claim_queue_sessions=4,
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
        )


def test_tw_cash_full_run_equals_chunked_state_continuation() -> None:
    targets = np.array([[0.6, 0.2], [0.4, 0.0], [0.0, 0.5], [0.0, 0.0], [0.2, 0.0]])
    closes = np.array([[10.0, 20.0], [11.0, 19.0], [12.0, 18.0], [12.5, 17.0], [13.0, 16.0]])
    kwargs = dict(
        buy_fee_rates=np.array([0.000855, 0.000855]),
        sell_fee_rates=np.array([0.003855, 0.001855]),
        lot_sizes=np.array([1, 1]),
        initial_cash=100_000.0,
    )
    full = run_tw_cash_integer(
        targets,
        closes,
        **kwargs,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
    )
    first = run_tw_cash_integer(
        targets[:2],
        closes[:2],
        **kwargs,
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:2]),
    )
    second = run_tw_cash_integer(
        targets[2:],
        closes[2:],
        **kwargs,
        unresolved_corporate_action_mask=_no_corporate_actions(targets[2:]),
        initial_state=first.final_state,
    )

    for name in (
        "strategy_returns",
        "simple_returns",
        "turnover",
        "positions",
        "weights",
        "settled_cash_history",
        "payable_history",
        "receivable_history",
        "payable_queue_history",
        "receivable_queue_history",
        "execution_nav_history",
        "nav_history",
        "settlement_net_history",
        "fee_history",
        "default_mask",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_allclose(full.final_state.payable_queue, second.final_state.payable_queue)
    np.testing.assert_allclose(
        full.final_state.receivable_queue, second.final_state.receivable_queue
    )
    assert full.final_state.settled_cash == pytest.approx(second.final_state.settled_cash)
    assert full.final_state.last_nav == pytest.approx(second.final_state.last_nav)
    np.testing.assert_allclose(full.final_weights, second.final_weights)
    np.testing.assert_allclose(
        full.final_state.last_weights, second.final_state.last_weights
    )


def test_tw_cash_halted_holding_uses_causal_stale_mark_and_chunks_exactly() -> None:
    targets = np.ones((3, 1), dtype=np.float64)
    closes = np.array([[10.0], [np.nan], [12.0]], dtype=np.float64)
    future_returns = np.array([[0.0], [np.log(1.2)], [0.0]])
    tradable = np.ones((3, 1), dtype=np.bool_)
    side_masks = np.array([[True], [False], [True]], dtype=np.bool_)
    common = dict(
        future_log_returns=future_returns,
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        tradable_mask=tradable,
        can_buy_mask=side_masks,
        can_sell_mask=side_masks,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=1_000.0,
    )

    full = run_tw_cash_integer(targets, closes, **common)
    first = run_tw_cash_integer(
        targets[:1],
        closes[:1],
        **{
            **common,
            "future_log_returns": future_returns[:1],
            "tradable_mask": tradable[:1],
            "can_buy_mask": side_masks[:1],
            "can_sell_mask": side_masks[:1],
            "unresolved_corporate_action_mask": _no_corporate_actions(targets[:1]),
        },
    )
    second = run_tw_cash_integer(
        targets[1:],
        closes[1:],
        **{
            **common,
            "future_log_returns": future_returns[1:],
            "tradable_mask": tradable[1:],
            "can_buy_mask": side_masks[1:],
            "can_sell_mask": side_masks[1:],
            "unresolved_corporate_action_mask": _no_corporate_actions(targets[1:]),
            "initial_state": first.final_state,
        },
    )

    np.testing.assert_array_equal(full.positions[:, 0], [100, 100, 100])
    np.testing.assert_allclose(full.execution_nav_history, [1_000.0, 1_000.0, 1_200.0])
    np.testing.assert_allclose(full.nav_history, [1_000.0, 1_200.0, 1_200.0])
    np.testing.assert_allclose(full.strategy_returns, [0.0, np.log(1.2), 0.0])
    np.testing.assert_allclose(full.weights[:, 0], [1.0, 1.0, 1.0])
    for name in (
        "strategy_returns",
        "simple_returns",
        "turnover",
        "positions",
        "weights",
        "settled_cash_history",
        "payable_queue_history",
        "receivable_queue_history",
        "execution_nav_history",
        "nav_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_allclose(full.final_weights, second.final_weights)
    np.testing.assert_allclose(
        full.final_state.last_weights,
        second.final_state.last_weights,
    )


def test_tw_cash_corporate_action_exit_never_executes_at_stale_mark() -> None:
    targets = np.ones((2, 1), dtype=np.float64)
    actions = np.array([[False], [True]], dtype=np.bool_)

    with pytest.raises(
        ValueError,
        match=r"close_prices.*required prices.*time index 1",
    ):
        run_tw_cash_integer(
            targets,
            np.array([[10.0], [np.nan]], dtype=np.float64),
            future_log_returns=np.array([[0.0], [np.nan]], dtype=np.float64),
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            tradable_mask=np.ones((2, 1), dtype=np.bool_),
            can_buy_mask=np.ones((2, 1), dtype=np.bool_),
            can_sell_mask=np.ones((2, 1), dtype=np.bool_),
            unresolved_corporate_action_mask=actions,
            initial_cash=1_000.0,
        )


def test_tw_cash_morning_default_precedes_later_quote_validation() -> None:
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=1.0,
        holdings=np.array([1], dtype=np.int64),
        payable_queue=np.array([2.0, 0.0]),
        receivable_queue=np.zeros(2),
        last_nav=10.0,
        last_weights=np.array([0.9]),
    )
    targets = np.zeros((1, 1))

    result = run_tw_cash_integer(
        targets,
        np.array([[np.nan]]),
        future_log_returns=np.array([[np.nan]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_state=state,
    )

    assert result.default_mask.tolist() == [True]
    assert result.simple_returns.tolist() == [-1.0]
    assert not result.final_state.alive
    assert result.final_state.settled_cash == 0.0
    assert not np.any(result.final_state.holdings)
    assert not np.any(result.final_state.payable_queue)
    assert not np.any(result.final_state.receivable_queue)
    np.testing.assert_array_equal(result.final_state.last_weights, [0.0])


def test_tw_cash_padding_is_quote_free_exact_state_identity() -> None:
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=1_000.0,
        holdings=np.array([50], dtype=np.int64),
        payable_queue=np.array([0.0, 500.0]),
        receivable_queue=np.zeros(2),
        last_nav=1_000.0,
        last_weights=np.array([0.5]),
    )
    targets = np.zeros((1, 1))

    result = run_tw_cash_integer(
        targets,
        np.array([[np.nan]]),
        future_log_returns=np.array([[np.nan]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        state_advance_mask=np.array([False]),
        initial_state=state,
    )

    np.testing.assert_array_equal(result.strategy_returns, [0.0])
    np.testing.assert_array_equal(result.positions, [[50]])
    np.testing.assert_array_equal(result.weights, [[0.5]])
    np.testing.assert_array_equal(result.nav_history, [1_000.0])
    assert result.final_state.settled_cash == state.settled_cash
    np.testing.assert_array_equal(
        result.final_state.payable_queue, state.payable_queue
    )
    np.testing.assert_array_equal(result.final_weights, state.last_weights)


def test_tw_cash_legacy_state_with_holdings_rejects_leading_padding() -> None:
    legacy_state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=1_000.0,
        holdings=np.array([50], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=1_500.0,
    )
    targets = np.zeros((1, 1))

    with pytest.raises(ValueError, match="last_weights"):
        run_tw_cash_integer(
            targets,
            np.array([[10.0]]),
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            state_advance_mask=np.array([False]),
            initial_state=legacy_state,
        )


@pytest.mark.parametrize(
    ("replacement_weight", "expected_receivable"),
    [(1.0, 0.0), (0.5, 500.0)],
)
def test_tw_cash_integer_nets_same_date_rotation_before_settlement(
    replacement_weight: float,
    expected_receivable: float,
) -> None:
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=0.0,
        holdings=np.array([100, 0], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=1_000.0,
    )
    targets = np.array([[0.0, replacement_weight]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0, 20.0]]),
        buy_fee_rates=np.zeros(2),
        sell_fee_rates=np.zeros(2),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_state=state,
    )

    assert result.positions[0, 0] == 0
    assert result.positions[0, 1] == int(50 * replacement_weight)
    assert result.final_state.payable_queue.sum() == pytest.approx(0.0)
    assert result.final_state.receivable_queue[-1] == pytest.approx(
        expected_receivable
    )
    assert not (
        result.final_state.payable_queue[-1] > 0.0
        and result.final_state.receivable_queue[-1] > 0.0
    )


def test_tw_cash_integer_negative_forced_sale_proceeds_are_not_an_affordability_error() -> None:
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=200.0,
        holdings=np.array([1], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=300.0,
    )
    targets = np.zeros((1, 1))
    result = run_tw_cash_integer(
        targets,
        np.array([[100.0]]),
        buy_fee_rates=np.array([0.0]),
        sell_fee_rates=np.array([1.5]),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        force_exit_mask=np.ones_like(targets, dtype=np.bool_),
        initial_state=state,
    )

    assert result.positions[0, 0] == 0
    assert result.fee_history[0] == pytest.approx(150.0)
    assert result.final_state.payable_queue[-1] == pytest.approx(50.0)
    assert result.final_state.receivable_queue.sum() == pytest.approx(0.0)
    assert result.nav_history[0] == pytest.approx(150.0)


@pytest.mark.parametrize("bad_return", [np.nan, np.inf, -np.inf, 1.0e4])
def test_tw_cash_integer_validates_returns_only_for_executed_holdings(
    bad_return: float,
) -> None:
    inactive_targets = np.zeros((1, 1))
    inactive = run_tw_cash_integer(
        inactive_targets,
        np.array([[10.0]]),
        future_log_returns=np.array([[bad_return]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=_no_corporate_actions(inactive_targets),
        initial_cash=1_000.0,
    )
    assert inactive.strategy_returns[0] == pytest.approx(0.0)

    active_targets = np.ones((1, 1))
    with pytest.raises(ValueError, match="non-finite/non-positive valuation"):
        run_tw_cash_integer(
            active_targets,
            np.array([[10.0]]),
            future_log_returns=np.array([[bad_return]]),
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            unresolved_corporate_action_mask=_no_corporate_actions(active_targets),
            initial_cash=1_000.0,
        )


def test_tw_day_trade_integer_open_sizing_does_not_observe_the_close() -> None:
    targets = np.ones((1, 1))
    mask = np.ones_like(targets, dtype=np.bool_)

    def execute(close: float):
        return run_tw_day_trade_integer(
            targets,
            np.array([[100.0]]),
            np.array([[close]]),
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            eligibility_mask=mask,
            lot_sizes=np.ones(1, dtype=np.int64),
            tradable_mask=mask,
            can_buy_mask=mask,
            can_sell_mask=mask,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            max_turnover_ratio=0.5,
            initial_cash=1_000.0,
        )

    unchanged = execute(100.0)
    doubled = execute(200.0)
    np.testing.assert_array_equal(unchanged.positions, doubled.positions)
    assert unchanged.positions[0, 0] == 2
    assert unchanged.turnover[0] == pytest.approx(0.4)
    assert doubled.turnover[0] == pytest.approx(0.6)


@pytest.mark.parametrize("invalid_price", [np.nan, np.inf, 0.0, -1.0])
def test_empty_lifecycle_masks_do_not_require_inactive_quotes(
    invalid_price: float,
) -> None:
    targets = np.zeros((1, 1))
    false = np.zeros_like(targets, dtype=np.bool_)
    true = np.ones_like(targets, dtype=np.bool_)
    cash = run_tw_cash_integer(
        targets,
        np.array([[invalid_price]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=false,
        tradable_mask=false,
        can_buy_mask=false,
        can_sell_mask=false,
        force_exit_mask=true,
        initial_cash=1_000.0,
    )
    assert cash.turnover[0] == pytest.approx(0.0)
    np.testing.assert_array_equal(
        cash.short_sale_collateral_history, np.zeros((1, 1))
    )
    np.testing.assert_array_equal(
        cash.short_margin_collateral_history, np.zeros((1, 1))
    )

    day = run_tw_day_trade_integer(
        targets,
        np.array([[invalid_price]]),
        np.array([[invalid_price]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        eligibility_mask=false,
        tradable_mask=false,
        can_buy_mask=false,
        can_sell_mask=false,
        can_short_open_mask=false,
        day_trade_can_buy_open_mask=false,
        day_trade_can_sell_open_mask=false,
        force_exit_mask=true,
        force_short_cover_mask=true,
        initial_cash=1_000.0,
    )
    assert day.turnover[0] == pytest.approx(0.0)
    np.testing.assert_array_equal(
        day.short_sale_collateral_history, np.zeros((1, 1))
    )
    np.testing.assert_array_equal(
        day.short_margin_collateral_history, np.zeros((1, 1))
    )


def test_tw_day_trade_integer_skips_entry_without_valid_close_mark() -> None:
    targets = np.ones((1, 1))
    mask = np.ones_like(targets, dtype=np.bool_)
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0]]),
        np.array([[np.nan]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        eligibility_mask=mask,
        lot_sizes=np.ones(1, dtype=np.int64),
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        initial_cash=1_000.0,
    )

    assert result.positions[0, 0] == 0
    assert result.turnover[0] == pytest.approx(0.0)
    assert result.nav_history[0] == pytest.approx(1_000.0)


@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_tw_integer_share_quantity_overflow_fails_closed(mode: str) -> None:
    targets = np.ones((1, 1))
    mask = np.ones_like(targets, dtype=np.bool_)
    with pytest.raises(OverflowError, match="share quantity exceeds int64"):
        if mode == "tw_cash":
            run_tw_cash_integer(
                targets,
                np.ones((1, 1)),
                buy_fee_rates=np.zeros(1),
                sell_fee_rates=np.zeros(1),
                unresolved_corporate_action_mask=_no_corporate_actions(targets),
                initial_cash=1.0e20,
            )
        else:
            run_tw_day_trade_integer(
                targets,
                np.ones((1, 1)),
                np.ones((1, 1)),
                buy_fee_rates=np.zeros(1),
                sell_fee_rates=np.zeros(1),
                eligibility_mask=mask,
                tradable_mask=mask,
                can_buy_mask=mask,
                can_sell_mask=mask,
                day_trade_can_buy_open_mask=mask,
                day_trade_can_sell_open_mask=mask,
                lot_sizes=np.ones(1, dtype=np.int64),
                initial_cash=1.0e20,
            )


@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_tw_integer_inactive_huge_target_does_not_round_or_require_quote(
    mode: str,
) -> None:
    targets = np.ones((1, 1))
    inactive = np.zeros_like(targets, dtype=np.bool_)
    invalid_quote = np.full_like(targets, np.nan)
    if mode == "tw_cash":
        result = run_tw_cash_integer(
            targets,
            invalid_quote,
            future_log_returns=invalid_quote,
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            tradable_mask=inactive,
            can_buy_mask=inactive,
            can_sell_mask=inactive,
            initial_cash=1.0e20,
        )
    else:
        result = run_tw_day_trade_integer(
            targets,
            invalid_quote,
            invalid_quote,
            buy_fee_rates=np.zeros(1),
            sell_fee_rates=np.zeros(1),
            eligibility_mask=inactive,
            tradable_mask=inactive,
            can_buy_mask=inactive,
            can_sell_mask=inactive,
            day_trade_can_buy_open_mask=inactive,
            day_trade_can_sell_open_mask=inactive,
            initial_cash=1.0e20,
        )

    assert result.turnover[0] == pytest.approx(0.0)
    assert result.positions[0, 0] == 0
    assert result.final_state.settled_cash == pytest.approx(1.0e20)


def test_tw_cash_integer_final_weights_are_drifted_but_history_is_execution_time() -> None:
    targets = np.array([[0.5]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0]]),
        future_log_returns=np.array([[np.log(2.0)]]),
        buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1),
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=1_000.0,
    )

    assert result.weights[0, 0] == pytest.approx(0.5)
    assert result.final_weights[0] == pytest.approx(2.0 / 3.0)


def test_etf_sell_tax_vector_changes_only_the_expected_sell_cost() -> None:
    common = dict(
        target_weights=np.array([[0.5], [0.0]]),
        close_prices=np.array([[10.0], [10.0]]),
        buy_fee_rates=np.array([0.000855]),
        unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
        initial_cash=100_000.0,
    )
    stock = run_tw_cash_integer(
        **common,
        sell_fee_rates=np.array([0.003855]),  # commission + 0.3% stock tax
    )
    etf = run_tw_cash_integer(
        **common,
        sell_fee_rates=np.array([0.001855]),  # commission + 0.1% ETF tax
    )

    sold_notional = stock.positions[0, 0] * 10.0
    assert etf.nav_history[-1] - stock.nav_history[-1] == pytest.approx(
        sold_notional * (0.003855 - 0.001855)
    )


def test_tw_cash_broker_minimum_and_rounding_apply_per_nonzero_symbol_side() -> None:
    targets = np.array([[0.05, 0.05], [0.0, 0.0]])
    closes = np.full((2, 2), 100.0)
    result = run_tw_cash_integer(
        targets,
        closes,
        buy_fee_rates=np.array([0.000855, 0.000855]),
        sell_fee_rates=np.array([0.003855, 0.003855]),
        minimum_commission=20.0,
        commission_rounding="floor",
        tax_rounding="floor",
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=20_000.0,
    )

    # Each row has two symbol-side aggregate orders.  Buy: 2 * 20 minimum.
    # Sell: 2 * (20 minimum commission + floor(1000 * 0.003) tax).
    np.testing.assert_array_equal(result.positions[0], [10, 10])
    np.testing.assert_allclose(result.fee_history, [40.0, 46.0])


def test_tw_cash_minimum_commission_affordability_is_fail_safe() -> None:
    targets = np.array([[1.0]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0]]),
        buy_fee_rates=0.000855,
        sell_fee_rates=0.003855,
        minimum_commission=20.0,
        commission_rounding="floor",
        tax_rounding="floor",
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=100.0,
    )

    # Ten shares would cost 120 including the order minimum; the vectorized
    # nonlinear affordability solver conservatively finds eight shares = 100.
    assert result.positions[0, 0] == 8
    assert result.final_state.payable_queue[-1] == pytest.approx(100.0)
    assert result.fee_history[0] == pytest.approx(20.0)


def test_tw_cash_zero_order_never_pays_minimum_commission() -> None:
    targets = np.zeros((1, 2))
    result = run_tw_cash_integer(
        targets,
        np.full((1, 2), 10.0),
        buy_fee_rates=np.full(2, 0.000855),
        sell_fee_rates=np.full(2, 0.003855),
        minimum_commission=20.0,
        commission_rounding="floor",
        tax_rounding="floor",
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=100.0,
    )

    assert result.fee_history[0] == 0.0
    assert result.settlement_net_history[0] == 0.0


def test_tw_cash_half_up_rounds_exact_half_currency_away_from_zero() -> None:
    targets = np.array([[0.1], [0.0]])
    result = run_tw_cash_integer(
        targets,
        np.ones((2, 1)),
        buy_fee_rates=np.array([0.0015]),
        sell_fee_rates=np.array([0.0040]),
        commission_rounding="half_up",
        tax_rounding="half_up",
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        initial_cash=10_000.0,
    )

    # Per side: commission 1.5 -> 2.  On sell, tax 2.5 -> 3 separately.
    np.testing.assert_allclose(result.fee_history, [2.0, 5.0])


def test_tw_integer_rejects_sell_rate_below_symmetric_commission_rate() -> None:
    with pytest.raises(ValueError, match="sell_fee_rates must be at least"):
        run_tw_cash_integer(
            np.zeros((1, 1)),
            np.ones((1, 1)),
            buy_fee_rates=np.array([0.001]),
            sell_fee_rates=np.array([0.0005]),
            unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        )


def test_payable_is_checked_before_same_session_receivable() -> None:
    # Although net assets are positive, the T+2 payable cannot be funded by an
    # incoming receivable that is made available later in the same session.
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=5.0,
        holdings=np.array([0], dtype=np.int64),
        payable_queue=np.array([10.0, 0.0]),
        receivable_queue=np.array([100.0, 0.0]),
        last_nav=95.0,
        alive=True,
    )
    result = run_tw_cash_integer(
        np.array([[0.0], [0.0]]),
        np.array([[10.0], [10.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
        initial_state=state,
    )

    np.testing.assert_array_equal(result.default_mask, [True, False])
    assert result.strategy_returns[0] == -np.inf
    assert result.simple_returns[0] == -1.0
    assert not result.final_state.alive
    assert result.final_state.last_nav == 0.0


def test_earlier_receivable_is_available_for_a_later_new_payable() -> None:
    # After this row's opening queue shift, 50 arrives one session before an
    # older 80 payable.  The exact forward cash projection leaves 70 available
    # for today's still-later claim; simply subtracting all pending payables
    # from current cash would incorrectly cap the purchase at 20.
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=100.0,
        holdings=np.array([0], dtype=np.int64),
        payable_queue=np.array([0.0, 0.0, 80.0]),
        receivable_queue=np.array([0.0, 50.0, 0.0]),
        last_nav=70.0,
        alive=True,
    )
    result = run_tw_cash_integer(
        np.array([[1.0]]),
        np.array([[1.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
        settlement_lag_sessions=3,
        initial_state=state,
    )

    assert result.positions[0, 0] == 70
    np.testing.assert_allclose(result.final_state.payable_queue, [0.0, 80.0, 70.0])
    np.testing.assert_allclose(result.final_state.receivable_queue, [50.0, 0.0, 0.0])


def test_tw_cash_integer_official_action_liquidates_then_allows_reentry() -> None:
    targets = np.array([[0.5], [0.5], [0.5]])
    prices = np.array([[10.0], [10.0], [10.0]])
    actions = np.array([[False], [True], [False]])
    result = run_tw_cash_integer(
        targets,
        prices,
        future_log_returns=np.array([[0.0], [np.log(3.0)], [0.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.10,
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=actions,
        initial_cash=1_000.0,
    )

    assert result.positions[0, 0] == 50
    assert result.positions[1, 0] == 0
    assert result.turnover[1] == pytest.approx(0.5)
    assert result.fee_history[1] == pytest.approx(50.0)
    assert result.strategy_returns[1] == pytest.approx(np.log(0.95))
    assert result.positions[2, 0] > 0


def test_tw_cash_integer_official_action_fails_if_held_sale_is_blocked() -> None:
    targets = np.array([[0.5], [0.5]])
    actions = np.array([[False], [True]])

    with pytest.raises(RuntimeError, match="sell side is unavailable"):
        run_tw_cash_integer(
            targets,
            np.array([[10.0], [10.0]]),
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
            lot_sizes=np.array([1]),
            can_sell_mask=np.array([[True], [False]]),
            unresolved_corporate_action_mask=actions,
            initial_cash=1_000.0,
        )


@pytest.mark.parametrize("signed_contract", [False, True])
def test_tw_cash_integer_selection_mask_does_not_block_current_exit(
    signed_contract: bool,
) -> None:
    targets = np.array([[0.5], [0.5]])
    actions = np.array([[False], [True]])
    selection_mask = np.array([[True], [False]])
    side_mask = np.ones_like(selection_mask)
    signed_kwargs = (
        {
            "can_short_open_mask": np.zeros_like(selection_mask),
            "short_margin_rate": np.full(targets.shape, 0.9),
            "short_capacity_shares": np.full(targets.shape, 1_000),
            "short_lot_sizes": np.array([1]),
        }
        if signed_contract
        else {}
    )

    result = run_tw_cash_integer(
        targets,
        np.full(targets.shape, 10.0),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        lot_sizes=np.array([1]),
        tradable_mask=selection_mask,
        can_buy_mask=side_mask,
        can_sell_mask=side_mask,
        unresolved_corporate_action_mask=actions,
        initial_cash=1_000.0,
        **signed_kwargs,
    )

    assert result.positions[0, 0] > 0
    assert result.positions[1, 0] == 0
    assert result.turnover[1] > 0.0


def test_tw_cash_integer_official_action_blocks_new_entry_without_holding() -> None:
    targets = np.array([[1.0]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        lot_sizes=np.array([1]),
        unresolved_corporate_action_mask=np.ones_like(targets, dtype=np.bool_),
        initial_cash=1_000.0,
    )

    assert result.positions[0, 0] == 0
    assert result.turnover[0] == 0.0


def test_tw_day_trade_manual_long_uses_1000_share_lots_and_t_plus_two() -> None:
    targets = np.array([[1.0], [0.0], [0.0]])
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0], [100.0], [100.0]]),
        np.array([[101.0], [100.0], [100.0]]),
        buy_fee_rates=np.array([0.001]),
        sell_fee_rates=np.array([0.0025]),
        eligibility_mask=np.ones((3, 1), dtype=bool),
        **_open_masks(targets),
        initial_cash=250_000.0,
    )

    # 2,000 shares: sell 202,000 - 505 fee - buy 200,000 - 200 fee.
    assert result.positions[0, 0] == 2_000
    assert result.settlement_net_history[0] == pytest.approx(1_295.0)
    np.testing.assert_allclose(result.receivable_queue_history[0], [0.0, 1_295.0])
    np.testing.assert_allclose(result.settled_cash_history, [250_000.0, 250_000.0, 251_295.0])
    assert np.all(result.final_state.holdings == 0)
    assert result.turnover[0] == pytest.approx(402_000.0 / 250_000.0)
    _assert_flat_ledger_identity(result)


def test_tw_day_trade_rounds_commission_and_tax_components_separately() -> None:
    targets = np.array([[1.0]])
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0]]),
        np.array([[101.0]]),
        buy_fee_rates=np.array([0.000855]),
        sell_fee_rates=np.array([0.002355]),
        eligibility_mask=np.ones_like(targets, dtype=np.bool_),
        commission_rounding="floor",
        tax_rounding="floor",
        **_open_masks(targets),
        initial_cash=250_000.0,
    )

    # Buy commission floor(200000*.000855)=171.  Sell commission is 172;
    # sell tax floor(202000*.0015)=303.  Total fees 646, net claim 1,354.
    assert result.positions[0, 0] == 2_000
    assert result.fee_history[0] == pytest.approx(646.0)
    assert result.settlement_net_history[0] == pytest.approx(1_354.0)


def test_tw_day_trade_blocked_close_prevents_integer_round_trip_entry() -> None:
    targets = np.array([[1.0]])

    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0]]),
        np.array([[101.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        eligibility_mask=np.ones_like(targets, dtype=np.bool_),
        can_sell_mask=np.zeros_like(targets, dtype=np.bool_),
        **_open_masks(targets),
        initial_cash=200_000.0,
    )

    assert result.positions[0, 0] == 0
    assert result.turnover[0] == pytest.approx(0.0)
    assert result.nav_history[0] == pytest.approx(200_000.0)


def test_tw_day_trade_blocked_buy_close_prevents_short_integer_entry() -> None:
    targets = np.array([[-1.0]])

    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0]]),
        np.array([[99.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        eligibility_mask=np.ones_like(targets, dtype=np.bool_),
        can_buy_mask=np.zeros_like(targets, dtype=np.bool_),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        **_open_masks(targets),
        initial_cash=200_000.0,
    )

    assert result.positions[0, 0] == 0
    assert result.turnover[0] == pytest.approx(0.0)
    assert result.nav_history[0] == pytest.approx(200_000.0)


def test_tw_day_trade_short_and_per_symbol_lot_override() -> None:
    targets = np.array([[-0.5, 0.5]])
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0, 100.0]]),
        np.array([[99.0, 101.0]]),
        buy_fee_rates=np.array([0.001, 0.001]),
        sell_fee_rates=np.array([0.0025, 0.0025]),
        eligibility_mask=np.ones((1, 2), dtype=bool),
        can_short_open_mask=np.ones((1, 2), dtype=bool),
        **_open_masks(targets),
        lot_sizes=np.array([1_000, 500]),
        initial_cash=300_000.0,
    )

    # The short side can express only one 1,000-share lot (100,000).  The
    # better-filled long side is reduced by one 500-share lot so whole-lot
    # rounding cannot turn a balanced request into a net-long execution.
    np.testing.assert_array_equal(result.positions[0], [-1_000, 1_000])
    assert np.all(result.final_state.holdings == 0)
    _assert_flat_ledger_identity(result)


def test_tw_day_trade_integer_two_sided_target_fails_flat_when_one_side_has_no_lot() -> None:
    targets = np.array([[0.5, -0.5]])
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0, 100.0]]),
        np.array([[101.0, 99.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        eligibility_mask=np.ones((1, 2), dtype=bool),
        can_short_open_mask=np.array([[True, False]], dtype=bool),
        **_open_masks(targets),
        initial_cash=1_000_000.0,
    )

    np.testing.assert_array_equal(result.positions[0], [0, 0])
    assert result.turnover[0] == pytest.approx(0.0)
    _assert_flat_ledger_identity(result)


def test_tw_day_trade_is_fail_closed_on_historical_eligibility() -> None:
    targets = np.array([[1.0]])
    result = run_tw_day_trade_integer(
        targets,
        np.array([[100.0]]),
        np.array([[110.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        eligibility_mask=np.array([[False]]),
        **_open_masks(targets),
        initial_cash=200_000.0,
    )
    assert result.positions[0, 0] == 0
    assert result.settlement_net_history[0] == 0.0
    assert result.nav_history[0] == 200_000.0

    with pytest.raises(TypeError):
        run_tw_day_trade_integer(  # type: ignore[call-arg]
            np.array([[1.0]]),
            np.array([[100.0]]),
            np.array([[110.0]]),
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
            **_open_masks(targets),
            initial_cash=200_000.0,
        )


def test_tw_day_trade_full_run_equals_chunked_with_pending_loss() -> None:
    targets = np.array([[0.5], [-0.5], [0.0], [0.5], [0.0]])
    opens = np.array([[100.0], [105.0], [103.0], [101.0], [102.0]])
    closes = np.array([[102.0], [108.0], [103.0], [100.0], [102.0]])
    eligible = np.ones_like(targets, dtype=bool)
    kwargs = dict(
        buy_fee_rates=np.array([0.000855]),
        sell_fee_rates=np.array([0.002355]),
        eligibility_mask=eligible,
        can_short_open_mask=np.ones_like(eligible, dtype=bool),
        **_open_masks(targets),
        initial_cash=1_000_000.0,
    )
    full = run_tw_day_trade_integer(targets, opens, closes, **kwargs)
    first = run_tw_day_trade_integer(
        targets[:2],
        opens[:2],
        closes[:2],
        **{
            **kwargs,
            "eligibility_mask": eligible[:2],
            "can_short_open_mask": np.ones_like(eligible[:2], dtype=bool),
            **_open_masks(targets[:2]),
        },
    )
    second = run_tw_day_trade_integer(
        targets[2:],
        opens[2:],
        closes[2:],
        **{
            **kwargs,
            "eligibility_mask": eligible[2:],
            "can_short_open_mask": np.ones_like(eligible[2:], dtype=bool),
            **_open_masks(targets[2:]),
        },
        initial_state=first.final_state,
    )

    for name in (
        "strategy_returns",
        "positions",
        "weights",
        "settled_cash_history",
        "payable_queue_history",
        "receivable_queue_history",
        "nav_history",
        "settlement_net_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    assert full.final_state.settled_cash == pytest.approx(second.final_state.settled_cash)
    np.testing.assert_allclose(full.final_state.payable_queue, second.final_state.payable_queue)
    np.testing.assert_allclose(
        full.final_state.receivable_queue, second.final_state.receivable_queue
    )


def test_state_advance_mask_does_not_age_settlement_queue() -> None:
    targets = np.array([[1.0], [0.0], [0.0], [0.0]])
    result = run_tw_day_trade_integer(
        targets,
        np.full((4, 1), 100.0),
        np.array([[101.0], [100.0], [100.0], [100.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        eligibility_mask=np.ones((4, 1), dtype=bool),
        **_open_masks(targets),
        state_advance_mask=np.array([True, False, True, True]),
        initial_cash=200_000.0,
    )

    # Profit from row 0 ages only on active rows 2 and 3, not padding row 1.
    assert result.settled_cash_history[1] == 200_000.0
    assert result.settled_cash_history[2] == 200_000.0
    assert result.settled_cash_history[3] == 202_000.0


def test_tw_cash_margin_short_manual_t_plus_two_collateral_ledger() -> None:
    targets = np.array([[-0.5], [0.0], [0.0], [0.0]])
    closes = np.array([[10.0], [8.0], [8.0], [8.0]])
    result = run_tw_cash_integer(
        targets,
        closes,
        buy_fee_rates=0.001,
        sell_fee_rates=0.003,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.full_like(targets, 1_000_000.0),
        initial_cash=100_000.0,
    )

    # t0 shorts 5,000 shares.  Net sale proceeds and margin are locked; only
    # the margin cash delivery is a T+2 payable.  Covering at t1 releases both
    # collateral pools into one account-level T+2 receivable.
    np.testing.assert_array_equal(result.positions[:, 0], [-5_000, 0, 0, 0])
    np.testing.assert_allclose(result.payable_queue_history[0], [0.0, 45_000.0])
    np.testing.assert_allclose(result.receivable_queue_history[1], [0.0, 54_810.0])
    np.testing.assert_allclose(result.settlement_net_history[:2], [-45_000.0, 54_810.0])
    np.testing.assert_allclose(result.fee_history[:2], [150.0, 40.0])
    np.testing.assert_allclose(
        result.short_sale_collateral_history[:, 0],
        [49_850.0, 0.0, 0.0, 0.0],
    )
    np.testing.assert_allclose(
        result.short_margin_collateral_history[:, 0],
        [45_000.0, 0.0, 0.0, 0.0],
    )
    np.testing.assert_allclose(
        result.nav_history, [99_850.0, 109_810.0, 109_810.0, 109_810.0]
    )
    np.testing.assert_allclose(
        result.nav_history,
        result.settled_cash_history
        + np.sum(result.positions * closes, axis=1)
        + np.sum(result.short_sale_collateral_history, axis=1)
        + np.sum(result.short_margin_collateral_history, axis=1)
        + result.receivable_history
        - result.payable_history,
    )
    opened = run_tw_cash_integer(
        targets[:1],
        closes[:1],
        buy_fee_rates=0.001,
        sell_fee_rates=0.003,
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:1]),
        can_short_open_mask=np.ones_like(targets[:1], dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.full_like(targets[:1], 1_000_000.0),
        initial_cash=100_000.0,
    )
    np.testing.assert_allclose(
        opened.final_state.short_sale_collateral, [49_850.0]
    )
    np.testing.assert_allclose(
        opened.final_state.short_margin_collateral, [45_000.0]
    )
    assert result.final_state.settled_cash == pytest.approx(109_810.0)
    np.testing.assert_array_equal(result.final_state.short_sale_collateral, [0.0])
    np.testing.assert_array_equal(result.final_state.short_margin_collateral, [0.0])


def test_tw_cash_margin_short_open_requires_explicit_permission() -> None:
    targets = np.array([[-1.0]])
    result = run_tw_cash_integer(
        targets,
        np.array([[10.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.zeros_like(targets, dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.zeros_like(targets),
        initial_cash=10_000.0,
    )

    np.testing.assert_array_equal(result.positions, [[0]])
    np.testing.assert_allclose(result.nav_history, [10_000.0])
    np.testing.assert_allclose(result.settlement_net_history, [0.0])


@pytest.mark.parametrize(
    ("short_kwargs", "message"),
    [
        ({"short_capacity_shares": np.zeros((1, 1))}, "short_margin_rate"),
        ({"short_margin_rate": 0.9}, "short_capacity_shares"),
        (
            {
                "short_margin_ratio": 0.9,
                "short_capacity_shares": np.zeros((1, 1)),
            },
            "short_margin_rate",
        ),
    ],
)
def test_tw_cash_signed_contract_inputs_fail_closed(
    short_kwargs: dict[str, object],
    message: str,
) -> None:
    targets = np.zeros((1, 1))

    with pytest.raises(ValueError, match=message):
        run_tw_cash_integer(
            targets,
            np.full_like(targets, 10.0),
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            # Merely supplying this mask selects the signed contract, even
            # when every cell denies a new short in this particular chunk.
            can_short_open_mask=np.zeros_like(targets, dtype=np.bool_),
            **short_kwargs,
        )


@pytest.mark.parametrize(
    ("sale_collateral", "margin_collateral", "message"),
    [
        (0.0, 9.0, "short_sale_collateral"),
        (10.0, 0.0, "short_margin_collateral"),
    ],
)
def test_tw_cash_negative_initial_holding_requires_both_collateral_pools(
    sale_collateral: float,
    margin_collateral: float,
    message: str,
) -> None:
    targets = np.zeros((1, 1))
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=100.0,
        holdings=np.array([-1], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=109.0,
        last_weights=np.array([-10.0 / 109.0]),
        short_sale_collateral=np.array([sale_collateral]),
        short_margin_collateral=np.array([margin_collateral]),
    )

    with pytest.raises(ValueError, match=message):
        run_tw_cash_integer(
            targets,
            np.full_like(targets, 10.0),
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            short_lot_sizes=1,
            short_margin_rate=0.9,
            short_capacity_shares=np.zeros_like(targets),
            initial_state=state,
        )


@pytest.mark.parametrize("bad_ratio", [0.99, np.array([1.30]), np.inf, True])
def test_tw_cash_short_maintenance_ratio_must_be_a_valid_scalar(
    bad_ratio: object,
) -> None:
    targets = np.zeros((1, 1))

    with pytest.raises(ValueError, match="short_maintenance_ratio"):
        run_tw_cash_integer(
            targets,
            np.full_like(targets, 10.0),
            buy_fee_rates=0.0,
            sell_fee_rates=0.0,
            unresolved_corporate_action_mask=_no_corporate_actions(targets),
            can_short_open_mask=np.zeros_like(targets, dtype=np.bool_),
            short_margin_rate=0.9,
            short_maintenance_ratio=bad_ratio,
            short_capacity_shares=np.zeros_like(targets),
        )


def test_tw_cash_forced_short_cover_bypasses_caps_and_can_cross_to_long() -> None:
    targets = np.array([[-0.2], [0.1]])
    result = run_tw_cash_integer(
        targets,
        np.array([[2.0], [10.0]]),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        force_short_cover_mask=np.array([[False], [True]]),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.full_like(targets, 1_000_000.0),
        daily_volumes=np.array([[10_000.0], [200.0]]),
        max_volume_participation=0.1,
        max_turnover_ratio=0.2,
        initial_cash=10_000.0,
    )

    # The 1,000-share cover is excluded from both caps.  The independent new
    # long is admitted under the ordinary cap and executes on the same day.
    np.testing.assert_array_equal(result.positions[:, 0], [-1_000, 20])
    assert result.turnover[1] == pytest.approx(5.1)
    np.testing.assert_array_equal(result.final_state.short_sale_collateral, [0.0])
    np.testing.assert_array_equal(result.final_state.short_margin_collateral, [0.0])


def test_tw_cash_partial_cover_stops_at_exact_130_percent_maintenance() -> None:
    prices = np.array([[200.0]])
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=10_000.0,
        holdings=np.array([-10], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=9_900.0,
        last_weights=np.array([-2_000.0 / 9_900.0]),
        short_sale_collateral=np.array([1_000.0]),
        short_margin_collateral=np.array([900.0]),
    )
    # 0.102 * TWD9,900 / TWD200 floors to five shares, so five of ten
    # borrowed shares are covered.
    targets = np.array([[-0.102]])

    result = run_tw_cash_integer(
        targets,
        prices,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.zeros_like(targets, dtype=np.bool_),
        short_lot_sizes=1,
        short_margin_rate=0.9,
        short_capacity_shares=np.zeros_like(targets),
        initial_state=state,
    )

    np.testing.assert_array_equal(result.positions, [[-5]])
    remaining_collateral = float(
        result.short_sale_collateral_history[0].sum()
        + result.short_margin_collateral_history[0].sum()
    )
    remaining_liability = 5 * prices[0, 0]
    assert remaining_collateral == pytest.approx(1_300.0)
    assert remaining_collateral / remaining_liability == pytest.approx(1.30)
    # Only TWD600 of the tentative TWD950 proportional release is legal;
    # paying TWD1,000 to cover therefore creates a TWD400 payable.
    assert result.settlement_net_history[0] == pytest.approx(-400.0)


def _multi_symbol_short_maintenance_state() -> TaiwanIntegerState:
    return TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=10_000.0,
        holdings=np.array([-10, -10], dtype=np.int64),
        payable_queue=np.zeros(2),
        receivable_queue=np.zeros(2),
        last_nav=10_800.0,
        last_weights=np.array([-1_000.0 / 10_800.0, -2_000.0 / 10_800.0]),
        short_sale_collateral=np.array([1_000.0, 1_000.0]),
        short_margin_collateral=np.array([900.0, 900.0]),
    )


def test_tw_cash_multi_symbol_cover_retains_excess_in_original_pools() -> None:
    targets = np.array([[0.0, -0.19]])
    prices = np.array([[100.0, 200.0]])

    result = run_tw_cash_integer(
        targets,
        prices,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.zeros_like(targets, dtype=np.bool_),
        short_lot_sizes=1,
        short_margin_rate=0.9,
        short_capacity_shares=np.zeros_like(targets),
        initial_state=_multi_symbol_short_maintenance_state(),
    )

    np.testing.assert_array_equal(result.positions, [[0, -10]])
    per_symbol_collateral = (
        result.short_sale_collateral_history[0]
        + result.short_margin_collateral_history[0]
    )
    # Covering symbol 0 would proportionally release TWD1,900, but only
    # TWD1,200 can leave while symbol 1's TWD2,000 liability remains.  The
    # residual TWD700 stays in symbol 0's original collateral pools.
    np.testing.assert_allclose(per_symbol_collateral, [700.0, 1_900.0])
    assert per_symbol_collateral.sum() == pytest.approx(2_000.0 * 1.30)
    assert result.settlement_net_history[0] == pytest.approx(200.0)


def test_tw_cash_maintenance_retention_full_run_equals_chunked_and_clears() -> None:
    targets = np.array([[0.0, -0.19], [0.0, 0.0], [0.0, 0.0]])
    prices = np.tile(np.array([[100.0, 200.0]]), (3, 1))
    can_short = np.zeros_like(targets, dtype=np.bool_)
    short_capacity = np.zeros_like(targets)
    common = dict(
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        short_lot_sizes=1,
        short_margin_rate=0.9,
        short_maintenance_ratio=1.30,
    )

    full = run_tw_cash_integer(
        targets,
        prices,
        **common,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=can_short,
        short_capacity_shares=short_capacity,
        initial_state=_multi_symbol_short_maintenance_state(),
    )
    first = run_tw_cash_integer(
        targets[:1],
        prices[:1],
        **common,
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:1]),
        can_short_open_mask=can_short[:1],
        short_capacity_shares=short_capacity[:1],
        initial_state=_multi_symbol_short_maintenance_state(),
    )
    second = run_tw_cash_integer(
        targets[1:],
        prices[1:],
        **common,
        unresolved_corporate_action_mask=_no_corporate_actions(targets[1:]),
        can_short_open_mask=can_short[1:],
        short_capacity_shares=short_capacity[1:],
        initial_state=first.final_state,
    )

    for name in (
        "strategy_returns",
        "simple_returns",
        "turnover",
        "positions",
        "weights",
        "settled_cash_history",
        "payable_queue_history",
        "receivable_queue_history",
        "execution_nav_history",
        "nav_history",
        "settlement_net_history",
        "fee_history",
        "default_mask",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_allclose(
        first.final_state.short_sale_collateral
        + first.final_state.short_margin_collateral,
        [700.0, 1_900.0],
    )
    np.testing.assert_array_equal(
        full.positions[1:], np.zeros((2, 2), dtype=np.int64)
    )
    np.testing.assert_array_equal(
        full.short_sale_collateral_history[1:], np.zeros((2, 2))
    )
    np.testing.assert_array_equal(
        full.short_margin_collateral_history[1:], np.zeros((2, 2))
    )
    np.testing.assert_array_equal(
        full.final_state.holdings, second.final_state.holdings
    )
    np.testing.assert_allclose(
        full.final_state.short_sale_collateral,
        second.final_state.short_sale_collateral,
    )
    np.testing.assert_allclose(
        full.final_state.short_margin_collateral,
        second.final_state.short_margin_collateral,
    )


def test_tw_cash_margin_short_full_run_equals_chunked_continuation() -> None:
    # The first chunk deliberately has no negative target.  Supplying the
    # borrow mask must still select the signed contract so a positive prefix is
    # identical to the corresponding prefix of the full long/short horizon.
    targets = np.array([[0.2], [-0.5], [-0.5], [0.0], [0.2]])
    closes = np.array([[10.0], [10.0], [9.0], [8.0], [8.0]])
    can_short = np.ones_like(targets, dtype=np.bool_)
    short_capacity = np.full_like(targets, 1_000_000.0)
    common = dict(
        buy_fee_rates=0.001,
        sell_fee_rates=0.003,
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        initial_cash=100_000.0,
    )
    full = run_tw_cash_integer(
        targets,
        closes,
        **common,
        can_short_open_mask=can_short,
        short_capacity_shares=short_capacity,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
    )
    first = run_tw_cash_integer(
        targets[:1],
        closes[:1],
        **common,
        can_short_open_mask=can_short[:1],
        short_capacity_shares=short_capacity[:1],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:1]),
    )
    second = run_tw_cash_integer(
        targets[1:],
        closes[1:],
        **common,
        can_short_open_mask=can_short[1:],
        short_capacity_shares=short_capacity[1:],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[1:]),
        initial_state=first.final_state,
    )

    for name in (
        "strategy_returns",
        "simple_returns",
        "turnover",
        "positions",
        "weights",
        "settled_cash_history",
        "payable_queue_history",
        "receivable_queue_history",
        "execution_nav_history",
        "nav_history",
        "settlement_net_history",
        "fee_history",
        "default_mask",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_array_equal(full.final_state.holdings, second.final_state.holdings)
    np.testing.assert_allclose(
        full.final_state.short_sale_collateral,
        second.final_state.short_sale_collateral,
    )
    np.testing.assert_allclose(
        full.final_state.short_margin_collateral,
        second.final_state.short_margin_collateral,
    )
    np.testing.assert_allclose(full.final_weights, second.final_weights)


def test_tw_cash_point_in_time_margin_matrix_is_applied_daily_and_chunks_exactly() -> None:
    targets = np.array([[-0.10], [-0.20], [-0.30]])
    closes = np.full((3, 1), 1_000.0)
    margin_rates = np.array([[0.90], [1.00], [1.10]])
    can_short = np.ones_like(targets, dtype=np.bool_)
    short_capacity = np.full_like(targets, 1_000_000.0)
    common = dict(
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        short_lot_sizes=1,
        initial_cash=10_000.0,
    )
    full = run_tw_cash_integer(
        targets,
        closes,
        **common,
        short_margin_rate=margin_rates,
        short_capacity_shares=short_capacity,
        can_short_open_mask=can_short,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
    )
    first = run_tw_cash_integer(
        targets[:1],
        closes[:1],
        **common,
        short_margin_rate=margin_rates[:1],
        short_capacity_shares=short_capacity[:1],
        can_short_open_mask=can_short[:1],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:1]),
    )
    second = run_tw_cash_integer(
        targets[1:],
        closes[1:],
        **common,
        short_margin_rate=margin_rates[1:],
        short_capacity_shares=short_capacity[1:],
        can_short_open_mask=can_short[1:],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[1:]),
        initial_state=first.final_state,
    )

    np.testing.assert_array_equal(full.positions[:, 0], [-1, -2, -3])
    # Each new one-share short posts the rate effective on its own session.
    np.testing.assert_allclose(
        full.short_margin_collateral_history[:, 0], [900.0, 1_900.0, 3_000.0]
    )
    for name in (
        "positions",
        "payable_queue_history",
        "settlement_net_history",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
        "nav_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_allclose(
        full.final_state.short_margin_collateral,
        second.final_state.short_margin_collateral,
    )


def test_tw_cash_short_handling_fee_only_reduces_new_short_sale_collateral() -> None:
    targets = np.array([[-0.5], [0.0]])
    result = run_tw_cash_integer(
        targets,
        np.full((2, 1), 10.0),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        short_handling_fee_rate=0.01,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.full_like(targets, 1_000_000.0),
        initial_cash=100_000.0,
    )

    np.testing.assert_allclose(result.fee_history, [500.0, 0.0])
    np.testing.assert_allclose(
        result.short_sale_collateral_history[:, 0], [49_500.0, 0.0]
    )
    np.testing.assert_allclose(
        result.short_margin_collateral_history[:, 0], [45_000.0, 0.0]
    )
    np.testing.assert_allclose(result.settlement_net_history, [-45_000.0, 44_500.0])

    # The same parameter is dormant for a pure cash-long sale.
    cash_targets = np.array([[0.5], [0.0]])
    cash_result = run_tw_cash_integer(
        cash_targets,
        np.full((2, 1), 10.0),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        short_handling_fee_rate=0.99,
        unresolved_corporate_action_mask=_no_corporate_actions(cash_targets),
        initial_cash=100_000.0,
    )
    np.testing.assert_allclose(cash_result.fee_history, [0.0, 0.0])
    np.testing.assert_array_equal(
        cash_result.short_sale_collateral_history, np.zeros((2, 1))
    )


def test_tw_cash_short_collateral_history_preserves_padding_and_clears_default() -> None:
    targets = np.array([[-1.0], [0.0], [0.0]])
    padded = run_tw_cash_integer(
        targets,
        np.full((3, 1), 10.0),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.full_like(targets, 1_000_000.0),
        state_advance_mask=np.array([True, False, True]),
        initial_cash=10_000.0,
    )
    np.testing.assert_allclose(
        padded.short_sale_collateral_history[:, 0], [10_000.0, 10_000.0, 0.0]
    )
    np.testing.assert_allclose(
        padded.short_margin_collateral_history[:, 0], [9_000.0, 9_000.0, 0.0]
    )

    default_state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=1.0,
        holdings=np.array([-1_000], dtype=np.int64),
        payable_queue=np.array([2.0, 0.0]),
        receivable_queue=np.zeros(2),
        last_nav=10_000.0,
        last_weights=np.array([-1.0]),
        short_sale_collateral=np.array([10_000.0]),
        short_margin_collateral=np.array([9_000.0]),
    )
    defaulted = run_tw_cash_integer(
        np.zeros((2, 1)),
        np.full((2, 1), np.nan),
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=np.zeros((2, 1), dtype=np.bool_),
        short_lot_sizes=1_000,
        short_margin_rate=0.9,
        short_capacity_shares=np.zeros((2, 1), dtype=np.float64),
        initial_state=default_state,
    )
    np.testing.assert_array_equal(defaulted.default_mask, [True, False])
    np.testing.assert_array_equal(
        defaulted.short_sale_collateral_history, np.zeros((2, 1))
    )
    np.testing.assert_array_equal(
        defaulted.short_margin_collateral_history, np.zeros((2, 1))
    )


def test_tw_cash_short_initial_margin_rounds_each_symbol_up_to_twd_hundred() -> None:
    prices = np.array([[100.0, 99.999999, 100.000001, 50.0, 50.0]])
    targets = np.array([[-0.11, -0.11, -0.11, -0.05, -0.05]])
    result = run_tw_cash_integer(
        targets,
        prices,
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
        can_short_open_mask=np.ones_like(targets, dtype=np.bool_),
        short_lot_sizes=1,
        short_margin_rate=1.0,
        short_capacity_shares=np.full_like(targets, 1_000_000.0),
        initial_cash=1_000.0,
    )

    # Exact/below TWD 100 stays 100, a genuine amount just above it becomes
    # 200, and the two TWD-50 symbols each round independently to TWD 100
    # instead of being aggregated into one TWD-100 account margin.
    expected = np.array([100.0, 100.0, 200.0, 100.0, 100.0])
    np.testing.assert_array_equal(result.positions, -np.ones((1, 5), dtype=np.int64))
    np.testing.assert_allclose(result.short_margin_collateral_history[0], expected)
    np.testing.assert_allclose(result.final_state.short_margin_collateral, expected)
    np.testing.assert_allclose(result.payable_queue_history[0], [0.0, 600.0])
    np.testing.assert_allclose(result.settlement_net_history, [-600.0])


def test_tw_cash_short_margin_rounding_is_per_open_order_and_chunk_exact() -> None:
    targets = np.array([[-0.05], [-0.10], [-0.10]])
    prices = np.full((3, 1), 50.0)
    can_short = np.ones_like(targets, dtype=np.bool_)
    short_capacity = np.full_like(targets, 1_000_000.0)
    common = dict(
        buy_fee_rates=0.0,
        sell_fee_rates=0.0,
        short_lot_sizes=1,
        short_margin_rate=1.0,
        initial_cash=1_000.0,
    )
    full = run_tw_cash_integer(
        targets,
        prices,
        **common,
        can_short_open_mask=can_short,
        short_capacity_shares=short_capacity,
        unresolved_corporate_action_mask=_no_corporate_actions(targets),
    )
    first = run_tw_cash_integer(
        targets[:1],
        prices[:1],
        **common,
        can_short_open_mask=can_short[:1],
        short_capacity_shares=short_capacity[:1],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[:1]),
    )
    second = run_tw_cash_integer(
        targets[1:],
        prices[1:],
        **common,
        can_short_open_mask=can_short[1:],
        short_capacity_shares=short_capacity[1:],
        unresolved_corporate_action_mask=_no_corporate_actions(targets[1:]),
        initial_state=first.final_state,
    )

    # Each one-share TWD-50 opening posts TWD 100.  The second opening adds a
    # fresh rounded TWD 100; it does not recompute cumulative margin as TWD 100.
    np.testing.assert_array_equal(full.positions[:, 0], [-1, -2, -2])
    np.testing.assert_allclose(
        full.short_margin_collateral_history[:, 0], [100.0, 200.0, 200.0]
    )
    for name in (
        "positions",
        "payable_queue_history",
        "settlement_net_history",
        "short_sale_collateral_history",
        "short_margin_collateral_history",
        "nav_history",
    ):
        np.testing.assert_allclose(
            getattr(full, name),
            np.concatenate([getattr(first, name), getattr(second, name)], axis=0),
            rtol=0.0,
            atol=1e-9,
        )
    np.testing.assert_allclose(
        full.final_state.short_margin_collateral,
        second.final_state.short_margin_collateral,
    )
