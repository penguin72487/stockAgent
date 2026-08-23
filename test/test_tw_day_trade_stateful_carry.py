from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import torch
from torch import nn

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import load_config
from stockagent.training.trainer import (
    FoldResult,
    _load_backtest_artifact,
    _prefix_backtest_result,
    _save_backtest_artifact,
    _save_fold_output_artifacts,
    _save_settlement_audit_artifacts,
    _slice_backtest_rows,
)


def _run(
    targets: torch.Tensor,
    *,
    close_buy: torch.Tensor,
    close_sell: torch.Tensor,
    overnight: torch.Tensor | None = None,
    intraday: torch.Tensor | None = None,
    day_trade_sell_fee: float = 0.0,
    normal_sell_fee: float = 0.0,
    financing_ratio: float = 0.0,
    financing_rate: float = 0.0,
    short_handling: float = 0.0,
    short_borrow: float = 0.0,
    initial_state: object | None = None,
    tradable: torch.Tensor | None = None,
    open_buy: torch.Tensor | None = None,
    open_sell: torch.Tensor | None = None,
    force_cover: torch.Tensor | None = None,
):
    rows, symbols = targets.shape
    true = torch.ones((rows, symbols), dtype=torch.bool)
    false = torch.zeros((rows, symbols), dtype=torch.bool)
    inf_capacity = torch.full((rows, symbols), float("inf"))
    state = initial_state
    return run_backtest_torch(
        targets,
        torch.zeros_like(targets) if intraday is None else intraday,
        true if tradable is None else tradable,
        torch.zeros(rows),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=close_buy,
        can_sell_mask=close_sell,
        can_short_open_mask=true,
        can_short_open_open_mask=true,
        force_short_cover_mask=(false if force_cover is None else force_cover),
        force_exit_mask=false,
        short_margin_rate=torch.full((rows, symbols), 0.9),
        short_capacity_weights=inf_capacity,
        short_maintenance_ratio=1.3,
        unresolved_corporate_action_mask=false,
        execution_mode="tw_day_trade",
        buy_fee_rates=torch.zeros(symbols),
        sell_fee_rates=torch.full((symbols,), day_trade_sell_fee),
        normal_sell_fee_rates=torch.full((symbols,), normal_sell_fee),
        commission_rebate_rates=torch.zeros(symbols),
        day_trade_unlimited_margin_conversion=True,
        day_trade_margin_financing_ratio=financing_ratio,
        day_trade_margin_financing_annual_rate=financing_rate,
        day_trade_margin_short_handling_fee_rate=short_handling,
        day_trade_margin_short_annual_borrow_rate=short_borrow,
        day_trade_eligible_mask=true,
        day_trade_can_buy_open_mask=(true if open_buy is None else open_buy),
        day_trade_can_sell_open_mask=(true if open_sell is None else open_sell),
        overnight_returns=(
            torch.zeros_like(targets) if overnight is None else overnight
        ),
        initial_weights=(None if state is None else state.final_weights),
        initial_cash=(None if state is None else state.final_cash),
        initial_payables=(None if state is None else state.final_payables),
        initial_receivables=(None if state is None else state.final_receivables),
        initial_commission_rebate_current=(
            None if state is None else state.final_commission_rebate_current
        ),
        initial_commission_rebate_due=(
            None if state is None else state.final_commission_rebate_due
        ),
        initial_commission_rebate_month_id=(
            None if state is None else state.final_commission_rebate_month_id
        ),
        initial_alive=(None if state is None else state.final_alive),
        initial_equity_scale=(
            None if state is None else state.final_equity_scale
        ),
        initial_short_sale_collateral=(
            None if state is None else state.final_short_sale_collateral
        ),
        initial_short_margin_collateral=(
            None if state is None else state.final_short_margin_collateral
        ),
        initial_long_margin_debt=(
            None if state is None else state.final_long_margin_debt
        ),
    )


def test_blocked_close_becomes_real_position_then_next_day_trades_delta() -> None:
    targets = torch.tensor([[0.5], [0.5]])
    result = _run(
        targets,
        close_buy=torch.ones((2, 1), dtype=torch.bool),
        close_sell=torch.tensor([[False], [True]]),
    )

    assert result.close_weights_history is not None
    assert result.event_turnovers is not None
    assert result.close_weights_history[0, 0] > 0.49
    torch.testing.assert_close(
        result.event_turnovers[1, 0],
        torch.zeros(()),
        atol=1.0e-6,
        rtol=0.0,
    )
    assert result.event_turnovers[1, 1] > 0.49
    torch.testing.assert_close(
        result.final_weights,
        torch.zeros_like(result.final_weights),
        atol=1.0e-6,
        rtol=0.0,
    )


def test_single_blocked_close_returns_nonzero_recurrent_state() -> None:
    result = _run(
        torch.tensor([[0.4]]),
        close_buy=torch.ones((1, 1), dtype=torch.bool),
        close_sell=torch.zeros((1, 1), dtype=torch.bool),
    )

    assert result.final_weights is not None
    assert result.final_weights[0] > 0.39


def test_unclosed_long_converts_purchase_principal_to_margin_debt() -> None:
    result = _run(
        torch.full((3, 1), 0.8),
        close_buy=torch.ones((3, 1), dtype=torch.bool),
        close_sell=torch.zeros((3, 1), dtype=torch.bool),
        financing_ratio=0.6,
    )

    # The purchase creates a 0.8 payable, then conversion replaces 0.48 of it
    # with broker margin debt. T+2 therefore settles only the investor's 0.32
    # contribution; economic NAV remains cash + stock - margin debt = 1.
    torch.testing.assert_close(
        result.final_long_margin_debt,
        torch.tensor([0.48]),
        atol=1.0e-6,
        rtol=0.0,
    )
    torch.testing.assert_close(
        result.cash_history,
        torch.tensor([1.0, 1.0, 0.68]),
        atol=1.0e-6,
        rtol=0.0,
    )
    assert result.final_alive is not None and bool(result.final_alive)


def test_selling_carried_long_repays_margin_principal_once() -> None:
    first = _run(
        torch.tensor([[0.8]]),
        close_buy=torch.ones((1, 1), dtype=torch.bool),
        close_sell=torch.zeros((1, 1), dtype=torch.bool),
        financing_ratio=0.6,
    )
    second = _run(
        torch.tensor([[0.0]]),
        close_buy=torch.ones((1, 1), dtype=torch.bool),
        close_sell=torch.ones((1, 1), dtype=torch.bool),
        financing_ratio=0.6,
        initial_state=first,
    )

    torch.testing.assert_close(
        second.final_long_margin_debt,
        torch.zeros(1),
        atol=1.0e-6,
        rtol=0.0,
    )
    assert second.receivables_history is not None
    # Sale proceeds 0.8 less the 0.48 principal repayment leave a 0.32 claim.
    torch.testing.assert_close(
        second.receivables_history[0, -1],
        torch.tensor(0.32),
        atol=1.0e-6,
        rtol=0.0,
    )


def test_partial_then_full_sale_leaves_no_margin_debt_rounding_residue() -> None:
    result = _run(
        torch.tensor([[0.812345], [0.312345], [0.0]]),
        close_buy=torch.ones((3, 1), dtype=torch.bool),
        close_sell=torch.tensor([[False], [False], [True]]),
        financing_ratio=0.6,
    )

    assert torch.count_nonzero(result.final_long_margin_debt).item() == 0
    assert torch.count_nonzero(result.final_weights).item() == 0


def test_margin_debt_history_never_outlives_physical_long_inventory() -> None:
    generator = torch.Generator().manual_seed(2309)
    targets = torch.randn((48, 96), generator=generator)
    targets = targets / targets.abs().sum(dim=1, keepdim=True).clamp_min(1.0e-12)
    close_sell = torch.rand((48, 96), generator=generator) > 0.35
    result = _run(
        targets,
        close_buy=torch.ones((48, 96), dtype=torch.bool),
        close_sell=close_sell,
        financing_ratio=0.6,
    )

    assert result.long_margin_debt_history is not None
    assert result.close_weights_history is not None
    orphaned = (
        (result.long_margin_debt_history > 1.0e-10)
        & (result.close_weights_history <= 1.0e-10)
    )
    assert not bool(orphaned.any())


def test_blocked_forced_cover_remains_borrowed_instead_of_defaulting() -> None:
    first = _run(
        torch.tensor([[-0.4]]),
        close_buy=torch.zeros((1, 1), dtype=torch.bool),
        close_sell=torch.ones((1, 1), dtype=torch.bool),
    )
    second = _run(
        torch.tensor([[-0.4]]),
        close_buy=torch.zeros((1, 1), dtype=torch.bool),
        close_sell=torch.ones((1, 1), dtype=torch.bool),
        open_buy=torch.zeros((1, 1), dtype=torch.bool),
        force_cover=torch.ones((1, 1), dtype=torch.bool),
        initial_state=first,
    )

    assert bool(second.final_alive)
    assert second.final_weights[0] < -0.39
    assert not bool(second.settlement_default.any())


def test_suspended_carried_position_uses_stale_mark_until_price_resumes() -> None:
    first = _run(
        torch.tensor([[0.4]]),
        close_buy=torch.ones((1, 1), dtype=torch.bool),
        close_sell=torch.zeros((1, 1), dtype=torch.bool),
    )
    second = _run(
        torch.tensor([[0.4]]),
        close_buy=torch.zeros((1, 1), dtype=torch.bool),
        close_sell=torch.zeros((1, 1), dtype=torch.bool),
        tradable=torch.zeros((1, 1), dtype=torch.bool),
        open_buy=torch.zeros((1, 1), dtype=torch.bool),
        open_sell=torch.zeros((1, 1), dtype=torch.bool),
        overnight=torch.full((1, 1), float("nan")),
        intraday=torch.full((1, 1), float("nan")),
        initial_state=first,
    )

    assert bool(second.final_alive)
    assert second.final_weights[0] > 0.39
    torch.testing.assert_close(second.strategy_returns, torch.zeros(1))


def test_short_residual_pays_normal_tax_conversion_and_daily_borrow_cost() -> None:
    targets = torch.tensor([[-0.4]])
    masks = torch.zeros((1, 1), dtype=torch.bool)
    baseline = _run(
        targets,
        close_buy=masks,
        close_sell=torch.ones_like(masks),
        day_trade_sell_fee=0.001,
        normal_sell_fee=0.001,
    )
    charged = _run(
        targets,
        close_buy=masks,
        close_sell=torch.ones_like(masks),
        day_trade_sell_fee=0.001,
        normal_sell_fee=0.003,
        short_handling=0.01,
        short_borrow=0.365,
    )

    assert charged.strategy_returns[0] < baseline.strategy_returns[0]
    assert charged.final_payables is not None
    assert baseline.final_payables is not None
    assert charged.final_payables.sum() > baseline.final_payables.sum()


def test_chunk_boundary_preserves_identical_position_and_finance_state() -> None:
    targets = torch.tensor([[0.4], [0.2]])
    close_buy = torch.ones((2, 1), dtype=torch.bool)
    close_sell = torch.tensor([[False], [True]])
    intraday = torch.tensor([[0.10], [-0.05]])
    full = _run(
        targets,
        close_buy=close_buy,
        close_sell=close_sell,
        intraday=intraday,
        financing_ratio=0.6,
    )
    first = _run(
        targets[:1],
        close_buy=close_buy[:1],
        close_sell=close_sell[:1],
        intraday=intraday[:1],
        financing_ratio=0.6,
    )
    second = _run(
        targets[1:],
        close_buy=close_buy[1:],
        close_sell=close_sell[1:],
        intraday=intraday[1:],
        financing_ratio=0.6,
        initial_state=first,
    )

    torch.testing.assert_close(full.strategy_returns, torch.cat(
        (first.strategy_returns, second.strategy_returns)
    ))
    for name in (
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_short_sale_collateral",
        "final_short_margin_collateral",
        "final_long_margin_debt",
        "final_equity_scale",
    ):
        torch.testing.assert_close(getattr(full, name), getattr(second, name))


def test_schema_seven_round_trip_slice_and_audit_preserve_margin_debt(
    tmp_path: Path,
) -> None:
    tensor_result = _run(
        torch.tensor([[0.8], [0.8]]),
        close_buy=torch.ones((2, 1), dtype=torch.bool),
        close_sell=torch.zeros((2, 1), dtype=torch.bool),
        financing_ratio=0.6,
    )
    result = tensor_result.to_numpy()
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    artifact = tmp_path / "stateful_day_trade.npz"

    _save_backtest_artifact(artifact, result, dates)
    with np.load(artifact, allow_pickle=False) as archive:
        assert int(archive["artifact_schema_version"].item()) == 7
        np.testing.assert_allclose(
            archive["long_margin_debt_history"],
            result.long_margin_debt_history,
        )

    loaded, loaded_dates = _load_backtest_artifact(artifact)
    np.testing.assert_array_equal(loaded_dates, dates)
    np.testing.assert_allclose(
        loaded.long_margin_debt_history,
        result.long_margin_debt_history,
    )
    np.testing.assert_allclose(
        loaded.final_long_margin_debt,
        result.final_long_margin_debt,
    )

    prefixed = _prefix_backtest_result(loaded, 2)
    sliced = _slice_backtest_rows(
        loaded,
        0,
        2,
        preserve_terminal_state=True,
    )
    for preserved in (prefixed, sliced):
        np.testing.assert_allclose(
            preserved.long_margin_debt_history,
            loaded.long_margin_debt_history,
        )
        np.testing.assert_allclose(
            preserved.final_long_margin_debt,
            loaded.final_long_margin_debt,
        )

    empty_prefix = _prefix_backtest_result(loaded, 0)
    empty_artifact = tmp_path / "empty_deployment_prefix.npz"
    _save_backtest_artifact(empty_artifact, empty_prefix, dates[:0])
    loaded_empty, loaded_empty_dates = _load_backtest_artifact(empty_artifact)
    assert loaded_empty_dates.size == 0
    assert loaded_empty.long_margin_debt_history.shape == (0, 1)
    assert loaded_empty.final_long_margin_debt is None

    audit_base = tmp_path / "settlement_audit"
    _save_settlement_audit_artifacts(
        audit_base,
        loaded,
        dates,
        table_output_format="csv",
    )
    with audit_base.with_suffix(".csv").open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[-1]["long_margin_debt_total"]) > 0.47
    summary = json.loads(
        audit_base.with_name("settlement_audit_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["final_long_margin_debt_total"] > 0.47


def test_stateful_fold_output_is_canonical_without_legacy_integer_label(
    tmp_path: Path,
) -> None:
    result = _run(
        torch.tensor([[0.8], [0.8]]),
        close_buy=torch.ones((2, 1), dtype=torch.bool),
        close_sell=torch.zeros((2, 1), dtype=torch.bool),
        financing_ratio=0.6,
    ).to_numpy()
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    config = load_config(
        "configs/markets/"
        "tw_day_trade_daily_executable_portfolio_transformer_22_basis_"
        "tplus2_close_capital10m.yaml"
    )
    fold_result = FoldResult(
        fold_id=0,
        train_years=[2024],
        val_years=[2025],
        test_years=[2026],
        best_val_loss=0.0,
        val_ic={},
        val_metrics={},
        test_ic={},
        test_metrics={},
    )

    _save_fold_output_artifacts(
        fold_dir=tmp_path,
        fold_result=fold_result,
        model=nn.Linear(1, 1),
        test_backtest=result,
        test_dates=dates,
        symbols=["2330"],
        config=config,
        test_integer_backtest=None,
        holdings_records=None,
        print_report=False,
        write_plots=False,
    )

    assert (tmp_path / "test_backtest.npz").exists()
    assert not (tmp_path / "test_integer_share_backtest.npz").exists()
    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["test_integer_metrics"] is None
    assert metrics["test_continuous_surrogate_metrics"] is None
