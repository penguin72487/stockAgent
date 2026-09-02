from __future__ import annotations

import pytest
import torch
from torch import nn
from torch.amp import GradScaler

from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _train_epoch_windowed_tensor
from stockagent.training.windowed import WindowedSplitTensors


class _RecordedScalarPolicy(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.scale = nn.Parameter(torch.tensor(0.1))
        self.forward_parameter_values: list[float] = []

    def forward(self, x: torch.Tensor, mask: torch.Tensor, **_: object) -> torch.Tensor:
        self.forward_parameter_values.append(float(self.scale.detach()))
        return self.scale * x[:, -1, :, 0]


class _CountingSGD(torch.optim.SGD):
    def __init__(self, params: object) -> None:
        super().__init__(params, lr=0.1)
        self.step_calls = 0

    def step(self, closure=None):  # type: ignore[no-untyped-def,override]
        self.step_calls += 1
        return super().step(closure)


class _CountingScheduler:
    def __init__(self) -> None:
        self.step_calls = 0

    def step(self) -> None:
        self.step_calls += 1


def _split() -> WindowedSplitTensors:
    rows, symbols = 4, 2
    return WindowedSplitTensors(
        features=(
            torch.arange(rows * symbols, dtype=torch.float32).reshape(rows, symbols, 1)
            / 10.0
            + 1.0
        ),
        valid_indices=torch.arange(rows),
        future_log_returns=torch.tensor([[0.01, -0.005]] * rows),
        tradable_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_buy_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_sell_mask=torch.ones((rows, symbols), dtype=torch.bool),
        benchmark=torch.zeros(rows),
        lookback=1,
        execution_mode="naive",
    )


def test_trajectory_cadence_keeps_parameters_fixed_until_all_batches_finish() -> None:
    split = _split()
    model = _RecordedScalarPolicy()
    optimizer = _CountingSGD(model.parameters())
    scheduler = _CountingScheduler()
    expected_loss = risk_aware_loss(
        model.scale.detach() * split.features[:, :, 0],
        split.future_log_returns,
        split.tradable_mask,
        benchmark_returns=split.benchmark,
        can_buy_mask=split.can_buy_mask,
        can_sell_mask=split.can_sell_mask,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        rank_ic_weight=0.0,
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        concentration_weight=0.0,
    )
    loss, timing = _train_epoch_windowed_tensor(
        model,
        None,
        risk_aware_loss,
        split,
        optimizer,
        GradScaler("cpu", enabled=False),
        batch_size=2,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        grad_clip_norm=1.0,
        rank_ic_weight=0.0,
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        concentration_weight=0.0,
        lr_scheduler=scheduler,  # type: ignore[arg-type]
        lr_scheduler_interval="step",
        optimizer_step_per_trajectory=True,
    )
    assert torch.isfinite(loss)
    assert loss.item() == pytest.approx(expected_loss.item(), abs=1.0e-7)
    assert optimizer.step_calls == 1
    assert scheduler.step_calls == 1
    assert model.forward_parameter_values == pytest.approx([0.1, 0.1])
    assert timing.batches == 2
    assert timing.gradient_norm_observations == 1
