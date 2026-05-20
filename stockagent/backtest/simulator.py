from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray   # [T] net daily returns after costs
    benchmark_returns: np.ndarray  # [T] universe-average daily returns
    turnovers: np.ndarray          # [T] total absolute weight change per day
    weights_history: np.ndarray    # [T, S] realised portfolio weights


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    fee_per_side: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    T, S = weights.shape
    strategy_returns = np.zeros(T, dtype=np.float32)
    turnovers = np.zeros(T, dtype=np.float32)
    weights_history = np.zeros((T, S), dtype=np.float32)

    prev_weights = np.zeros(S, dtype=np.float32)
    for t in range(T):
        day_weights = np.asarray(weights[t], dtype=np.float32).copy()
        day_mask = tradable_mask[t].astype(bool)
        day_weights[~day_mask] = 0.0
        weight_sum = float(day_weights.sum())
        if weight_sum > 0:
            day_weights /= weight_sum

        turnover = float(np.abs(day_weights - prev_weights).sum())
        gross = float(np.dot(day_weights, future_returns[t]))
        net = gross - turnover * fee_per_side

        strategy_returns[t] = net
        turnovers[t] = turnover
        weights_history[t] = day_weights
        prev_weights = day_weights

    return strategy_returns, turnovers, weights_history


def run_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    fee_per_side: float,
) -> BacktestResult:
    """Simulate daily portfolio execution from model weights."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
    )

    return BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.astype(np.float32),
        turnovers=turnovers,
        weights_history=weights_history,
    )