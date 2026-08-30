from __future__ import annotations

from pathlib import Path

import pytest
import torch

from stockagent.backtest.tw_day_trade_daily import compiled_block_rows
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import load_config
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _format_fold_evaluation_lines


def _run(
    weights: torch.Tensor,
    returns: torch.Tensor,
    *,
    can_buy_close: torch.Tensor | None = None,
    can_sell_close: torch.Tensor | None = None,
    buy_fee: float = 0.0,
    day_sell_fee: float = 0.0,
    normal_sell_fee: float = 0.0,
    financing_ratio: float = 0.60,
    financing_rate: float = 0.16,
    short_handling: float = 0.001,
    short_borrow_rate: float = 0.20,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_weights: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    initial_long_margin_debt: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_commission_rebate_current: torch.Tensor | None = None,
    initial_commission_rebate_due: torch.Tensor | None = None,
    initial_commission_rebate_month_id: torch.Tensor | None = None,
):
    mask = torch.ones_like(weights, dtype=torch.bool)
    symbols = weights.size(1)
    return run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros(weights.size(0)),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask if can_buy_close is None else can_buy_close,
        can_sell_mask=mask if can_sell_close is None else can_sell_close,
        can_short_open_mask=mask,
        can_short_open_open_mask=mask,
        force_short_cover_mask=torch.zeros_like(mask),
        force_exit_mask=torch.zeros_like(mask),
        short_margin_rate=torch.full_like(weights, 0.9),
        short_capacity_weights=torch.full_like(weights, float("inf")),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        execution_mode="tw_day_trade",
        buy_fee_rates=torch.full((symbols,), buy_fee),
        sell_fee_rates=torch.full((symbols,), day_sell_fee),
        normal_sell_fee_rates=torch.full((symbols,), normal_sell_fee),
        commission_rebate_rates=torch.zeros(symbols),
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        day_trade_unlimited_margin_conversion=True,
        day_trade_margin_financing_ratio=financing_ratio,
        day_trade_margin_financing_annual_rate=financing_rate,
        day_trade_margin_short_handling_fee_rate=short_handling,
        day_trade_margin_short_annual_borrow_rate=short_borrow_rate,
        overnight_returns=torch.zeros_like(weights),
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        initial_long_margin_debt=initial_long_margin_debt,
        initial_alive=initial_alive,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
    )


def test_daily_config_uses_twenty_percent_broker_commission_price() -> None:
    config = load_config(
        Path(__file__).resolve().parents[1]
        / "configs/markets/tw_day_trade_daily_no_default.yaml"
    )
    assert config.trading.tw_commission_rate == 0.001425
    assert config.trading.tw_commission_discount == 0.2
    assert abs(
        config.trading.tw_commission_rate
        * config.trading.tw_commission_discount
        - 0.000285
    ) < 1.0e-12


def test_daily_multi_basis_config_keeps_same_tplus3_execution_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    plain = load_config(
        root / "configs/markets/tw_day_trade_daily_no_default.yaml"
    )
    multi = load_config(
        root
        / "configs/markets/tw_day_trade_daily_multi_basis_tplus2_close.yaml"
    )
    assert multi.trading.execution_mode == plain.trading.execution_mode == "tw_day_trade"
    assert multi.trading.tw_settlement_lag_sessions == 2
    assert multi.trading.tw_day_trade_unlimited_margin_conversion is True
    assert multi.trading.tw_commission_rate == 0.001425
    assert multi.trading.tw_commission_discount == 0.2
    assert multi.data.day_trade_minute_execution_root is None
    assert multi.training.loss_type == plain.training.loss_type == "log_utility"
    assert multi.training.model_name == "financial_transformer"
    assert multi.training.epochs == 1000
    basis = multi.training.financial_transformer
    assert basis.temporal_basis_input == "input_features"
    assert len(basis.temporal_basis_families) == 18
    assert basis.temporal_basis_components == 4
    assert "multi_basis_tplus2_close_commission20" in multi.runner.output_dir


def test_daily_multi_basis_projection_l1_is_a_fresh_formal_market_run() -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(
        root
        / "configs/markets/tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_capital10m.yaml"
    )

    assert config.training.model_name == "financial_transformer"
    assert config.training.epochs == 1000
    assert config.training.lookback == 32
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 128
    assert config.training.record_epoch_curve is True
    assert config.training.curve_plot_interval == 1
    assert config.training.curve_plot_async is True
    assert config.training.defer_epoch_curve_plot_until_end is False
    assert config.training.financial_transformer.portfolio_output_mode == "projection_l1"
    assert config.training.loss_portfolio_activation == "pre_normalized"
    assert config.trading.portfolio_activation == "pre_normalized"
    assert config.trading.volume_participation_equity == 10_000_000.0
    assert config.walk_forward.lookback_context == "panel_history"
    assert config.runner.resume is True
    assert config.runner.post_train_infer is False
    assert "artifacts/markets/" in config.runner.output_dir
    assert "projection_l1" in config.runner.output_dir
    assert "panel_history" in config.runner.output_dir
    basis = config.training.financial_transformer
    assert basis.temporal_basis_input == "input_features"
    assert len(basis.temporal_basis_families) == 18
    assert basis.temporal_basis_components == 4


def test_daily_normal_round_trip_uses_open_to_close_return_without_minute_data() -> None:
    result = _run(torch.tensor([[0.10]]), torch.log(torch.tensor([[1.10]])))
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([0.01]),
        atol=1.0e-7,
    )
    assert torch.allclose(result.turnovers, torch.tensor([0.21]))


def test_blocked_long_close_becomes_financed_inventory_and_stays_open() -> None:
    weights = torch.tensor([[0.10], [0.10]])
    returns = torch.zeros_like(weights)
    blocked = torch.zeros_like(weights, dtype=torch.bool)
    result = _run(
        weights,
        returns,
        can_sell_close=blocked,
        buy_fee=0.001,
        day_sell_fee=0.002,
        normal_sell_fee=0.004,
    )
    first_day_cost = 0.10 * (0.001 + 0.60 * 0.16 / 365.0)
    assert torch.allclose(
        result.strategy_returns[0].exp() - 1.0,
        torch.tensor(-first_day_cost),
        atol=1.0e-7,
    )
    assert result.final_weights is not None
    assert result.final_weights[0] > 0.099
    assert result.final_long_margin_debt is not None
    assert result.final_long_margin_debt[0] > 0.059
    assert result.event_turnovers is not None
    # Day two observes the carried mark and trades only the tiny NAV-adjusted
    # target delta; the blocked close does not fabricate another 0.10 buy.
    assert result.event_turnovers[1, 0] < 2.0e-5
    assert result.event_turnovers[:, 1].eq(0.0).all()
    assert result.settlement_default is not None
    assert not bool(result.settlement_default.any())
    assert result.payables_history is not None
    # Only the 40% investor contribution plus costs remains in T+2 payables;
    # the 60% broker-funded principal is carried separately as margin debt.
    assert 0.040 < float(result.payables_history[0, -1]) < 0.041


def test_blocked_short_close_uses_normal_tax_short_fee_and_one_day_borrow() -> None:
    weights = torch.tensor([[-0.10]])
    returns = torch.zeros_like(weights)
    blocked = torch.zeros_like(weights, dtype=torch.bool)
    result = _run(
        weights,
        returns,
        can_buy_close=blocked,
        buy_fee=0.001,
        day_sell_fee=0.002,
        normal_sell_fee=0.004,
        short_handling=0.001,
        short_borrow_rate=0.20,
    )
    assert result.strategy_returns[0] < 0.0
    assert result.final_weights is not None and result.final_weights[0] < -0.099
    assert result.short_sale_collateral_history is not None
    assert result.short_margin_collateral_history is not None
    assert result.short_sale_collateral_history[0, 0] > 0.0
    assert result.short_margin_collateral_history[0, 0] > 0.0


def test_missing_close_mark_causes_absorbing_default_without_nan_gradient() -> None:
    weights = torch.tensor([[0.10]], requires_grad=True)
    result = _run(weights, torch.full_like(weights, float("nan")))
    assert torch.isfinite(result.strategy_returns).all()
    assert result.final_alive is not None and not bool(result.final_alive)
    assert result.strategy_returns[0] < -10.0
    (-result.strategy_returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_t_profit_settles_after_t_plus_2_close_and_sizes_only_t_plus_3() -> None:
    weights = torch.full((4, 1), 0.10)
    returns = torch.zeros_like(weights)
    returns[0] = torch.log(torch.tensor(1.10))
    result = _run(weights, returns)

    # T gain is immediately part of economic NAV/loss.
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([0.01, 0.0, 0.0, 0.0]),
        atol=1.0e-7,
    )
    # State arrays are normalized by each day's own NAV. Absolute wealth is
    # carried separately in equity_scale_history, so cash temporarily reads
    # 1/1.01 and the receivable 0.01/1.01 rather than absolute dollars.
    assert result.cash_history is not None
    assert result.receivables_history is not None
    assert result.equity_scale_history is not None
    assert torch.allclose(
        result.cash_history,
        torch.tensor([1.0 / 1.01, 1.0 / 1.01, 1.0, 1.0]),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.receivables_history,
        torch.tensor(
            [
                [0.0, 0.01 / 1.01],
                [0.01 / 1.01, 0.0],
                [0.0, 0.0],
                [0.0, 0.0],
            ]
        ),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.weights_history[:, 0],
        torch.zeros(4),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.equity_scale_history,
        torch.full((4,), 1.01),
        atol=1.0e-7,
    )


def test_daily_log_utility_loss_carries_same_net_claim_state_and_gradient() -> None:
    weights = torch.tensor([[0.10], [0.0], [0.0]], requires_grad=True)
    returns = torch.zeros_like(weights)
    returns[0] = torch.log(torch.tensor(1.10))
    mask = torch.ones_like(weights, dtype=torch.bool)
    aux: dict[str, torch.Tensor] = {}
    zeros = torch.zeros(1)
    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=torch.zeros(3),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        can_short_open_open_mask=mask,
        force_short_cover_mask=torch.zeros_like(mask),
        force_exit_mask=torch.zeros_like(mask),
        short_margin_rate=torch.full_like(weights, 0.9),
        short_capacity_weights=torch.full_like(weights, float("inf")),
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        commission_rebate_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        day_trade_unlimited_margin_conversion=True,
        overnight_log_returns=torch.zeros_like(weights),
        settlement_lag_sessions=2,
        objective="log_utility",
        gamma_turnover=0.0,
        rank_ic_weight=0.0,
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        concentration_weight=0.0,
        aux_outputs=aux,
    )
    loss.backward()
    assert torch.isfinite(loss)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert torch.allclose(aux["_final_cash"], torch.tensor(1.0), atol=1.0e-7)
    assert torch.allclose(
        aux["_final_equity_scale"], torch.tensor(1.01), atol=1.0e-7
    )
    assert torch.equal(
        aux["_final_payables"], torch.zeros_like(aux["_final_payables"])
    )
    assert torch.equal(
        aux["_final_receivables"], torch.zeros_like(aux["_final_receivables"])
    )


def test_daily_tplus2_state_is_identical_across_batch_boundary() -> None:
    weights = torch.full((4, 1), 0.10)
    returns = torch.zeros_like(weights)
    returns[0] = torch.log(torch.tensor(1.10))
    full = _run(weights, returns)
    first = _run(weights[:2], returns[:2])
    second = _run(
        weights[2:],
        returns[2:],
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_equity_scale=first.final_equity_scale,
        initial_weights=first.final_weights,
        initial_short_sale_collateral=first.final_short_sale_collateral,
        initial_short_margin_collateral=first.final_short_margin_collateral,
        initial_long_margin_debt=first.final_long_margin_debt,
        initial_alive=first.final_alive,
        initial_commission_rebate_current=(
            first.final_commission_rebate_current
        ),
        initial_commission_rebate_due=first.final_commission_rebate_due,
        initial_commission_rebate_month_id=(
            first.final_commission_rebate_month_id
        ),
    )
    assert torch.allclose(
        torch.cat((first.strategy_returns, second.strategy_returns)),
        full.strategy_returns,
        atol=1.0e-7,
    )
    assert torch.allclose(
        torch.cat((first.weights_history, second.weights_history)),
        full.weights_history,
        atol=1.0e-7,
    )
    assert torch.allclose(
        torch.cat((first.cash_history, second.cash_history)),
        full.cash_history,
        atol=1.0e-7,
    )
    assert torch.allclose(second.final_cash, full.final_cash, atol=1.0e-7)
    assert torch.allclose(
        second.final_equity_scale,
        full.final_equity_scale,
        atol=1.0e-7,
    )


def test_daily_tplus2_block_size_is_an_executor_only_knob(
    monkeypatch,
) -> None:
    base_weights = torch.linspace(-0.08, 0.12, 33).reshape(11, 3)
    returns = torch.linspace(-0.03, 0.04, 33).reshape(11, 3)

    observed = []
    for block_rows in (1, 4, 8, 32):
        monkeypatch.setenv(
            "STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS",
            str(block_rows),
        )
        weights = base_weights.detach().clone().requires_grad_(True)
        result = _run(weights, returns)
        loss = -result.strategy_returns.sum()
        loss.backward()
        observed.append((result, weights.grad.detach().clone()))

    reference, reference_grad = observed[0]
    for result, gradient in observed[1:]:
        for field in (
            "strategy_returns",
            "turnovers",
            "weights_history",
            "equity_scale_history",
            "cash_history",
            "payables_history",
            "receivables_history",
            "final_cash",
            "final_payables",
            "final_receivables",
            "final_equity_scale",
        ):
            assert torch.allclose(
                getattr(result, field),
                getattr(reference, field),
                atol=1.0e-7,
                rtol=1.0e-6,
            ), field
        assert torch.allclose(gradient, reference_grad, atol=1.0e-7, rtol=1.0e-6)


def test_daily_tplus2_block_size_environment_validation(monkeypatch) -> None:
    monkeypatch.setenv(
        "STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "not-an-integer"
    )
    with pytest.raises(ValueError, match="must be an integer"):
        compiled_block_rows()


def test_daily_tplus2_close_console_label_does_not_claim_minute_execution() -> None:
    metrics = {
        "cagr": 0.01,
        "cumulative_return": 0.02,
        "excess_return_vs_benchmark": -0.03,
    }
    lines = _format_fold_evaluation_lines(
        execution_mode="tw_day_trade",
        loss_objective="log_utility",
        val_ic={},
        val_metrics=metrics,
        test_ic={},
        test_metrics=metrics,
        test_integer_metrics=metrics,
        canonical_tensor_exact=True,
        canonical_tensor_label="daily-tplus2-close",
    )
    assert "[val daily-tplus2-close]" in lines[0]
    assert "[test daily-tplus2-close]" in lines[1]
    assert all("minute" not in line for line in lines)
