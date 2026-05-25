from __future__ import annotations

import torch
from torch import Tensor

from stockagent.backtest.simulator import run_backtest_torch


def masked_mse_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    mask_f = mask.to(dtype=predictions.dtype)
    error = (predictions - targets).pow(2) * mask_f
    return error.sum() / mask_f.sum().clamp_min(1.0)


def masked_ic_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    ics: list[Tensor] = []
    for i in range(predictions.size(0)):
        m = mask[i]
        if int(m.sum().item()) < 2:
            continue
        p = predictions[i][m].float()
        t = targets[i][m].float()
        p_rank = p.argsort().argsort().float()
        t_rank = t.argsort().argsort().float()
        p_c = p_rank - p_rank.mean()
        t_c = t_rank - t_rank.mean()
        denom = (p_c.norm() * t_c.norm()).clamp_min(1e-8)
        ics.append((p_c * t_c).sum() / denom)
    if not ics:
        return predictions.sum() * 0.0
    return -torch.stack(ics).mean()


def sharpe_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
) -> Tensor:
    """Sharpe-aware loss driven by the same backtest kernel used in evaluation."""
    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    benchmark_zeros = torch.zeros(weights.size(0), device=weights.device, dtype=returns.dtype)

    backtest = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark_zeros,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
    )

    if sample_mask is None:
        valid_mask = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_mask = sample_mask.to(device=weights.device, dtype=torch.bool)

    valid_returns = backtest.strategy_returns[valid_mask]
    if valid_returns.numel() == 0:
        return weights.sum() * 0.0

    mean_return = valid_returns.mean()
    variance = (valid_returns - mean_return).pow(2).mean()
    std_return = torch.sqrt(variance + 1e-8)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=valid_returns.dtype))
    sharpe = mean_return / std_return * annualizer

    # Keep turnover regularization, but source it from the same backtest path.
    valid_turnovers = backtest.turnovers[valid_mask]
    turnover_penalty = valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(())

    loss = -gamma_sharpe * sharpe + gamma_turnover * turnover_penalty
    return loss