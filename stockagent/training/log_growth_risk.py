"""Decomposable risk-sensitive utility of authoritative net daily log returns.

These are training preferences, never modified fills or reported returns. Daily
second moments and lower partial moments decompose across chronological blocks;
Sharpe ratios and maximum drawdown do not. Date-weighted DDP therefore retains
the exact selected objective under the existing bounded accounting recurrence.
"""
from __future__ import annotations

import torch


def log_growth_risk_kwargs(evaluation) -> dict[str, float]:
    return {name: float(getattr(evaluation, name)) for name in (
        "log_utility_risk_aversion", "log_utility_downside_aversion",
        "log_utility_benchmark_shortfall_aversion")}


def risk_adjusted_log_growth(
    returns: torch.Tensor,
    benchmark: torch.Tensor,
    *,
    risk_aversion: float = 0.0,
    downside_aversion: float = 0.0,
    benchmark_shortfall_aversion: float = 0.0,
) -> torch.Tensor:
    if not (risk_aversion or downside_aversion or benchmark_shortfall_aversion):
        return returns
    penalty = returns.new_zeros(returns.shape)
    if risk_aversion:
        penalty = penalty + float(risk_aversion) * returns.square()
    if downside_aversion:
        penalty = penalty + float(downside_aversion) * returns.clamp_max(0).square()
    if benchmark_shortfall_aversion:
        penalty = penalty + float(benchmark_shortfall_aversion) * (returns - benchmark).clamp_max(0).square()
    return returns - 0.5 * penalty
