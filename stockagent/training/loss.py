from __future__ import annotations

import torch
import torch.nn.functional as F
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
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
) -> Tensor:
    """Sharpe-aware loss driven by the same backtest kernel used in evaluation."""
    weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    can_buy = (
        can_buy_mask.to(dtype=torch.bool, device=weights.device)
        if can_buy_mask is not None
        else tradable
    )
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
        gross_leverage=gross_leverage,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell_mask.to(dtype=torch.bool, device=weights.device) if can_sell_mask is not None else None,
        return_weights_history=False,
    )

    if sample_mask is None:
        valid_mask = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_mask = sample_mask.to(device=weights.device, dtype=torch.bool)

    valid_returns = backtest.strategy_returns[valid_mask]
    valid_returns = torch.nan_to_num(valid_returns, nan=0.0, posinf=0.0, neginf=0.0)
    if valid_returns.numel() == 0:
        return weights.sum() * 0.0

    mean_return = valid_returns.mean()
    variance = (valid_returns - mean_return).pow(2).mean()
    std_return = torch.sqrt(variance + 1e-8)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=valid_returns.dtype))
    sharpe = mean_return / std_return * annualizer

    # Keep turnover regularization, but source it from the same backtest path.
    if gamma_turnover == 0.0:
        return -gamma_sharpe * sharpe

    valid_turnovers = backtest.turnovers[valid_mask]
    valid_turnovers = torch.nan_to_num(valid_turnovers, nan=0.0, posinf=0.0, neginf=0.0)
    turnover_penalty = valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(())
    return -gamma_sharpe * sharpe + gamma_turnover * turnover_penalty


def _compute_risk_ratio(valid_returns: Tensor, objective: str) -> Tensor:
    objective_norm = objective.strip().lower()
    mean_return = valid_returns.mean()
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=valid_returns.device, dtype=valid_returns.dtype))

    if objective_norm == "sortino":
        downside = torch.minimum(valid_returns, torch.zeros_like(valid_returns))
        downside_dev = torch.sqrt(downside.pow(2).mean() + 1e-8)
        return mean_return / downside_dev * annualizer

    variance = (valid_returns - mean_return).pow(2).mean()
    std_return = torch.sqrt(variance + 1e-8)
    return mean_return / std_return * annualizer


def _compute_excess_risk_terms(
    valid_returns: Tensor,
    valid_benchmark: Tensor,
    cvar_alpha: float,
    drawdown_target: float,
) -> tuple[Tensor, Tensor, Tensor, Tensor, Tensor]:
    excess_returns = valid_returns - valid_benchmark
    mean_excess = excess_returns.mean()

    alpha = min(max(float(cvar_alpha), 1e-6), 1.0 - 1e-6)
    losses = -excess_returns
    var_alpha = torch.quantile(losses, alpha)
    tail_excess = torch.relu(losses - var_alpha)
    cvar = var_alpha + tail_excess.mean() / (1.0 - alpha)

    rel_log_equity = torch.cumsum(excess_returns, dim=0)
    rel_equity = torch.exp(torch.clamp(rel_log_equity, -60.0, 60.0))
    running_max = torch.cummax(rel_equity, dim=0).values
    drawdowns = 1.0 - rel_equity / running_max.clamp_min(1e-12)
    mdd = drawdowns.max() if drawdowns.numel() > 0 else mean_excess.new_zeros(())
    drawdown_penalty = F.softplus((mdd - float(drawdown_target)) * 20.0) / 20.0
    return excess_returns, mean_excess, cvar, mdd, drawdown_penalty


def risk_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    benchmark_returns: Tensor | None = None,
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_leverage: float = 1.0,
    gamma_sharpe: float = 1.0,
    gamma_excess: float = 1.0,
    gamma_cvar: float = 1.0,
    cvar_alpha: float = 0.95,
    gamma_drawdown: float = 0.0,
    drawdown_target: float = 0.2,
    gamma_turnover: float = 0.1,
    gamma_underperformance: float = 1.0,
    excess_target: float = 0.0,
    cvar_budget: float = 0.03,
    drawdown_budget: float = 0.2,
    turnover_budget: float = 0.3,
    gamma_cvar_budget: float = 1.0,
    gamma_drawdown_budget: float = 1.0,
    gamma_turnover_budget: float = 0.0,
    objective: str = "sharpe",
) -> Tensor:
    """Risk-aware loss with configurable objective, including excess-CVaR-drawdown."""
    weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    can_buy = (
        can_buy_mask.to(dtype=torch.bool, device=weights.device)
        if can_buy_mask is not None
        else tradable
    )
    if benchmark_returns is None:
        tradable_f = tradable.to(dtype=returns.dtype)
        denom = tradable_f.sum(dim=1).clamp_min(1.0)
        benchmark = (returns * tradable_f).sum(dim=1) / denom
    else:
        benchmark = torch.nan_to_num(
            benchmark_returns.to(device=weights.device, dtype=returns.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )

    backtest = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        gross_leverage=gross_leverage,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell_mask.to(dtype=torch.bool, device=weights.device) if can_sell_mask is not None else None,
        return_weights_history=False,
    )

    if sample_mask is None:
        valid_mask = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_mask = sample_mask.to(device=weights.device, dtype=torch.bool)

    valid_returns = backtest.strategy_returns[valid_mask]
    valid_returns = torch.nan_to_num(valid_returns, nan=0.0, posinf=0.0, neginf=0.0)
    if valid_returns.numel() == 0:
        return weights.sum() * 0.0

    objective_norm = objective.strip().lower()
    if objective_norm in {
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
    }:
        valid_benchmark = backtest.benchmark_returns[valid_mask]
        valid_benchmark = torch.nan_to_num(valid_benchmark, nan=0.0, posinf=0.0, neginf=0.0)
        _, mean_excess, cvar, mdd, drawdown_penalty = _compute_excess_risk_terms(
            valid_returns,
            valid_benchmark,
            cvar_alpha=cvar_alpha,
            drawdown_target=drawdown_target,
        )

        if objective_norm in {"outperformance_risk_budget", "outperformance_budget", "outperformance_first"}:
            underperformance = torch.relu(float(excess_target) - (valid_returns - valid_benchmark)).mean()
            turnover_mean = backtest.turnovers[valid_mask]
            turnover_mean = torch.nan_to_num(turnover_mean, nan=0.0, posinf=0.0, neginf=0.0)
            turnover_mean = turnover_mean.mean() if turnover_mean.numel() > 0 else mean_excess.new_zeros(())

            cvar_budget_penalty = torch.relu(cvar - float(cvar_budget))
            drawdown_budget_penalty = torch.relu(mdd - float(drawdown_budget))
            turnover_budget_penalty = torch.relu(turnover_mean - float(turnover_budget))

            objective_value = (
                -gamma_excess * mean_excess
                + gamma_underperformance * underperformance
                + gamma_cvar_budget * cvar_budget_penalty
                + gamma_drawdown_budget * drawdown_budget_penalty
                + gamma_turnover_budget * turnover_budget_penalty
                + gamma_cvar * cvar
                + gamma_drawdown * drawdown_penalty
            )
        else:
            objective_value = (
                -gamma_excess * mean_excess
                + gamma_cvar * cvar
                + gamma_drawdown * drawdown_penalty
            )
    else:
        risk_ratio = _compute_risk_ratio(valid_returns, objective)
        objective_value = -gamma_sharpe * risk_ratio

    if gamma_turnover == 0.0:
        return objective_value

    valid_turnovers = backtest.turnovers[valid_mask]
    valid_turnovers = torch.nan_to_num(valid_turnovers, nan=0.0, posinf=0.0, neginf=0.0)
    turnover_penalty = valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(())
    return objective_value + gamma_turnover * turnover_penalty