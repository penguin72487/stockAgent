from __future__ import annotations

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_index_futures import (
    FuturesCostSchedule,
    build_tw_index_futures_day_execution_tensor,
    run_tw_index_futures_all_tenors_day_torch,
    run_tw_index_futures_day_continuous,
    run_tw_index_futures_day_integer,
    select_tw_index_futures_contract_basket,
)
from stockagent.backtest.simulator import (
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.data.tw_index_futures import (
    TaiwanIndexFuturesDaySession,
    build_causal_taifex_futures_model_context,
)


def _market() -> TaiwanIndexFuturesDaySession:
    opens = np.asarray(
        [[10000.0, 10000.0, 10000.0], [10000.0, 10000.0, 10000.0]]
    )
    closes = np.asarray(
        [[10100.0, 10100.0, 10100.0], [9900.0, 9900.0, 9900.0]]
    )
    rolling = np.log(closes / opens)
    return TaiwanIndexFuturesDaySession(
        dates=np.asarray(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
        products=("TX", "MTX", "TMF"),
        contract_months=np.full((2, 3), "202501", dtype="U16"),
        open_prices=opens,
        high_prices=np.maximum(opens, closes),
        low_prices=np.minimum(opens, closes),
        close_prices=closes,
        volumes=np.full((2, 3), 1000, dtype=np.int64),
        log_returns=rolling,
        tradable_mask=np.ones((2, 3), dtype=bool),
        multipliers=np.asarray([200, 50, 10], dtype=np.int64),
        rolling_buy_hold_log_returns=rolling,
        rolling_buy_hold_tradable_mask=np.ones((2, 3), dtype=bool),
        front_month_roll_mask=np.zeros((2, 3), dtype=bool),
    )


def _tenor_market() -> TaiwanIndexFuturesDaySession:
    market = _market()
    months = np.asarray(
        [[f"2025{month:02d}" for month in range(1, 7)]] * 2,
        dtype="U6",
    )
    opens = np.repeat(market.open_prices[:, None, :], 6, axis=1)
    closes = np.repeat(market.close_prices[:, None, :], 6, axis=1)
    return TaiwanIndexFuturesDaySession(
        dates=market.dates,
        products=market.products,
        contract_months=market.contract_months,
        open_prices=market.open_prices,
        high_prices=market.high_prices,
        low_prices=market.low_prices,
        close_prices=market.close_prices,
        volumes=market.volumes,
        log_returns=market.log_returns,
        tradable_mask=market.tradable_mask,
        multipliers=market.multipliers,
        rolling_buy_hold_log_returns=market.rolling_buy_hold_log_returns,
        rolling_buy_hold_tradable_mask=market.rolling_buy_hold_tradable_mask,
        front_month_roll_mask=market.front_month_roll_mask,
        tenor_contract_months=months,
        tenor_open_prices=opens,
        tenor_high_prices=np.maximum(opens, closes),
        tenor_low_prices=np.minimum(opens, closes),
        tenor_close_prices=closes,
        tenor_volumes=np.full_like(opens, 1000, dtype=np.int64),
        tenor_log_returns=np.log(closes / opens),
        tenor_tradable_mask=np.ones_like(opens, dtype=bool),
    )


def test_continuous_contract_is_daily_flat_and_masks_missing_session() -> None:
    result = run_tw_index_futures_day_continuous(
        torch.tensor([0.5, -1.0, 0.8]),
        torch.log(torch.tensor([1.01, 0.99, 1.50])),
        torch.tensor([True, True, False]),
        round_trip_cost_rate=0.001,
    )
    expected = torch.tensor(
        [0.5 * 0.01 - 0.5 * 0.001, -1.0 * -0.01 - 0.001, 0.0]
    )
    torch.testing.assert_close(result.strategy_returns, expected)
    torch.testing.assert_close(result.turnovers, torch.tensor([1.0, 2.0, 0.0]))


def test_basket_uses_same_underlying_products_for_integer_sizing() -> None:
    basket = select_tw_index_futures_contract_basket(
        1_000_000.0,
        np.asarray([10000.0, 10000.0, 10000.0]),
        np.ones(3, dtype=bool),
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
        max_notional=1_000_000.0,
    )
    # One TX is TWD 2m, so the exact TWD 1m basket is two MTX.
    np.testing.assert_array_equal(basket.quantities, [0, 2, 0])
    assert basket.actual_notional == pytest.approx(1_000_000.0)


def test_integer_backtest_applies_sell_only_tax_and_both_side_fees() -> None:
    schedule = FuturesCostSchedule(
        tax_rate=0.0002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
        slippage_points_per_side=(0.0, 0.0, 0.0),
    )
    result = run_tw_index_futures_day_integer(
        np.asarray([1.0, -1.0]),
        _market(),
        initial_capital=1_000_000.0,
        cost_schedule=schedule,
    )

    np.testing.assert_array_equal(result.contract_quantities[0], [0, 2, 0])
    assert result.gross_pnl_twd[0] == pytest.approx(10_000.0)
    # Two MTX, TWD 24 per contract per side, two sides.
    assert result.fees_twd[0] == pytest.approx(96.0)
    # Both the opening and closing futures transactions are taxed.
    assert result.tax_twd[0] == pytest.approx(
        2 * 50 * (10_000 + 10_100) * 0.0002
    )
    assert result.net_pnl_twd[0] == pytest.approx(10_000.0 - 96.0 - 402.0)
    # Day two is short and therefore also profits when the index falls.
    assert result.gross_pnl_twd[1] > 0.0
    # Direction does not change the per-transaction tax base.
    assert result.tax_twd[1] == pytest.approx(
        2 * 50 * (10_000 + 9_900) * 0.0002
    )
    assert result.equity[-1] > 1_000_000.0
    assert result.alive.all()


def test_basket_estimate_charges_one_sale_tax_and_two_fixed_fees() -> None:
    schedule = FuturesCostSchedule(
        tax_rate=0.0002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
    )
    basket = select_tw_index_futures_contract_basket(
        1_000_000.0,
        np.asarray([10_000.0, 10_000.0, 10_000.0]),
        np.ones(3, dtype=bool),
        cost_schedule=schedule,
        max_notional=1_000_000.0,
    )
    np.testing.assert_array_equal(basket.quantities, [0, 2, 0])
    # 2 contracts * 2 sides * TWD 24 + two TWD 1m transactions * 0.0002.
    assert basket.estimated_round_trip_cost == pytest.approx(96.0 + 400.0)


def test_one_hundred_million_capital_executes_small_model_exposure() -> None:
    result = run_tw_index_futures_day_integer(
        np.asarray([0.03, 0.03]),
        _market(),
        initial_capital=100_000_000.0,
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
    )

    assert np.all(np.abs(result.contract_quantities).sum(axis=1) > 0)
    assert np.all(result.executed_exposure > 0.0)
    assert np.any(result.strategy_returns != 0.0)


def test_missing_unused_product_does_not_ruin_integer_account() -> None:
    market = _market()
    market.open_prices[:, 2] = np.nan
    market.high_prices[:, 2] = np.nan
    market.low_prices[:, 2] = np.nan
    market.close_prices[:, 2] = np.nan
    market.log_returns[:, 2] = np.nan
    market.tradable_mask[:, 2] = False

    result = run_tw_index_futures_day_integer(
        np.asarray([1.0, -1.0]),
        market,
        initial_capital=1_000_000.0,
        cost_schedule=FuturesCostSchedule(tax_rate=0.0),
    )

    np.testing.assert_array_equal(result.contract_quantities[:, 2], 0)
    assert np.isfinite(result.strategy_returns).all()
    assert np.any(result.strategy_returns != 0.0)
    assert result.alive.all()


def test_nonfinite_price_on_selected_product_fails_closed() -> None:
    market = _market()
    market.close_prices[0, 1] = np.nan

    with pytest.raises(
        ValueError,
        match="selected futures contracts require finite positive open/close prices",
    ):
        run_tw_index_futures_day_integer(
            np.asarray([1.0, -1.0]),
            market,
            initial_capital=1_000_000.0,
            cost_schedule=FuturesCostSchedule(tax_rate=0.0),
        )


def test_canonical_tensor_adapter_collapses_only_to_one_futures_exposure() -> None:
    pseudo_weights = torch.tensor([[0.2, 0.3], [-0.4, -0.1]], requires_grad=True)
    repeated_returns = torch.log(
        torch.tensor([[1.01, 1.01], [0.99, 0.99]])
    )
    mask = torch.ones_like(pseudo_weights, dtype=torch.bool)
    result = run_backtest_torch(
        pseudo_weights,
        repeated_returns,
        mask,
        repeated_returns[:, 0],
        0.0005,
        0.0005,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_index_futures_day",
    )

    expected_simple = torch.tensor([0.5 * 0.01 - 0.0005, 0.5 * 0.01 - 0.0005])
    torch.testing.assert_close(torch.expm1(result.strategy_returns), expected_simple)
    torch.testing.assert_close(result.turnovers, torch.ones(2))
    (-result.strategy_returns.mean()).backward()
    assert pseudo_weights.grad is not None
    assert torch.isfinite(pseudo_weights.grad).all()


def test_canonical_tensor_adapter_keeps_ruin_absorbing_within_chunk() -> None:
    pseudo_weights = torch.ones((2, 1))
    repeated_returns = torch.log(torch.tensor([[0.01], [2.0]]))
    mask = torch.ones_like(pseudo_weights, dtype=torch.bool)
    result = run_backtest_torch(
        pseudo_weights,
        repeated_returns,
        mask,
        repeated_returns[:, 0],
        0.05,
        0.05,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_index_futures_day",
    )

    assert result.strategy_returns[0] < 0.0
    assert result.strategy_returns[1].item() == 0.0
    torch.testing.assert_close(result.turnovers, torch.tensor([2.0, 0.0]))
    assert not bool(result.final_alive)


def test_canonical_integer_adapter_uses_all_in_user_fees() -> None:
    pseudo_weights = np.asarray([[0.5, 0.5], [-0.5, -0.5]])
    returns = np.repeat(_market().log_returns[:, :1], 2, axis=1)
    mask = np.ones_like(pseudo_weights, dtype=bool)
    schedule = FuturesCostSchedule(
        tax_rate=0.0,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
    )
    result, records = run_backtest_integer_shares(
        pseudo_weights,
        returns,
        mask,
        np.asarray(_market().log_returns[:, 0], dtype=np.float32),
        initial_capital=1_000_000.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        dates=_market().dates,
        execution_mode="tw_index_futures_day",
        futures_market=_market(),
        futures_cost_schedule=schedule,
    )

    # TWD 1m exposure is two MTX; two sides * two contracts * TWD 24.
    assert len(records) == 4
    assert result.execution_mode == "tw_index_futures_day"
    first_day_net = np.expm1(result.strategy_returns[0]) * 1_000_000.0
    assert first_day_net == pytest.approx(10_000.0 - 96.0, rel=1e-5)


def test_all_tenor_exact_forward_matches_numpy_integer_ledger() -> None:
    market = _tenor_market()
    schedule = FuturesCostSchedule(
        tax_rate=0.00002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
    )
    actions = np.zeros((2, 18), dtype=np.float32)
    actions[0, 6] = 0.01  # MTX E1: TWD 1m -> exactly two contracts.
    actions[1, 13] = -0.005  # TMF E2: near TWD 0.5m -> whole contracts.
    integer = run_tw_index_futures_day_integer(
        actions,
        market,
        initial_capital=100_000_000.0,
        cost_schedule=schedule,
    )
    execution = build_tw_index_futures_day_execution_tensor(
        market, cost_schedule=schedule
    )
    tensor = run_tw_index_futures_all_tenors_day_torch(
        torch.tensor(actions, requires_grad=True),
        torch.tensor(execution),
        initial_capital=100_000_000.0,
    )

    np.testing.assert_array_equal(integer.contract_quantities[0, 6], 2)
    assert integer.contract_quantities[1, 13] < 0
    torch.testing.assert_close(
        tensor.strategy_returns.detach(),
        torch.tensor(integer.strategy_returns, dtype=torch.float32),
        rtol=2e-6,
        atol=2e-8,
    )
    torch.testing.assert_close(
        tensor.turnovers.detach(),
        torch.tensor(integer.turnovers, dtype=torch.float32),
        rtol=2e-6,
        atol=2e-8,
    )
    (-tensor.strategy_returns.mean()).backward()


def test_all_tenor_zero_action_retains_directional_gradient() -> None:
    actions = torch.zeros((1, 18), dtype=torch.float32, requires_grad=True)
    execution = torch.full((1, 18, 3), float("nan"), dtype=torch.float32)
    execution[0, 0] = torch.tensor([0.01, -0.011, 1_000_000.0])

    result = run_tw_index_futures_all_tenors_day_torch(
        actions,
        execution,
        initial_capital=100_000_000.0,
    )
    loss = -torch.log1p(result.strategy_returns).mean()
    loss.backward()

    # No whole contract is requested, so the exact forward remains flat.  The
    # signed straight-through derivative must nevertheless let the model learn
    # which side would have improved the exact objective instead of making
    # zero exposure an artificial absorbing point.
    assert result.strategy_returns.item() == 0.0
    assert actions.grad is not None
    assert actions.grad[0, 0].item() == pytest.approx(-0.0105)
    assert torch.count_nonzero(actions.grad[0, 1:]) == 0


def test_direct_integer_adapter_exposes_recurrent_equity_scale() -> None:
    market = _tenor_market()
    actions = np.zeros((2, 18), dtype=np.float32)
    actions[:, 6] = 0.01
    returns = np.zeros((2, 2), dtype=np.float32)
    result, _ = run_backtest_integer_shares(
        actions,
        returns,
        np.ones_like(returns, dtype=bool),
        np.zeros(2, dtype=np.float32),
        initial_capital=100_000_000.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        dates=market.dates,
        execution_mode="tw_index_futures_day",
        futures_market=market,
        futures_cost_schedule=FuturesCostSchedule(tax_rate=0.0),
    )

    assert result.equity_scale_history is not None
    assert result.final_equity_scale is not None
    np.testing.assert_allclose(
        result.final_equity_scale,
        result.equity_scale_history[-1],
    )


def test_joint_futures_context_is_shifted_one_complete_session() -> None:
    market = _tenor_market()
    context, mask = build_causal_taifex_futures_model_context(market)

    assert context.shape == (2, 18, 13)
    assert not mask[0].any()
    assert mask[1].all()
    # Row 1 observes row 0's +1% session, never row 1's -1% realization.
    np.testing.assert_allclose(
        context[1, :, 0],
        market.flattened_tenor_panel()[6][0],
        rtol=1e-6,
    )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA chunk compile test")
def test_all_tenor_compiled_chunks_match_eager_forward_and_gradient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    torch.manual_seed(19)
    # Deliberately non-aligned: the final 13 real sessions are padded with
    # inert rows inside the fixed-size compiled ledger and sliced afterward.
    rows = 45
    actions = torch.randn(rows, 18, device="cuda").tanh()
    actions = actions / actions.abs().sum(dim=-1, keepdim=True)
    execution = torch.empty(rows, 18, 3, device="cuda")
    execution[..., 0] = torch.randn(rows, 18, device="cuda") * 0.01 - 0.0002
    execution[..., 1] = -execution[..., 0] - 0.0004
    execution[..., 2] = torch.rand(rows, 18, device="cuda") * 4_000_000 + 100_000
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "32")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")

    def evaluate(compiled: bool) -> tuple[object, torch.Tensor]:
        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1" if compiled else "0")
        requested = actions.detach().clone().requires_grad_(True)
        result = run_tw_index_futures_all_tenors_day_torch(
            requested,
            execution,
            initial_capital=100_000_000.0,
        )
        (-torch.log1p(result.strategy_returns).mean()).backward()
        assert requested.grad is not None
        return result, requested.grad

    compiled, compiled_grad = evaluate(True)
    eager, eager_grad = evaluate(False)

    torch.testing.assert_close(
        compiled.strategy_returns,
        eager.strategy_returns,
        rtol=2e-6,
        atol=2e-8,
    )
    torch.testing.assert_close(
        compiled.equity_scale_history,
        eager.equity_scale_history,
        rtol=2e-6,
        atol=2e-7,
    )
    torch.testing.assert_close(
        compiled_grad,
        eager_grad,
        rtol=2e-6,
        atol=2e-8,
    )
