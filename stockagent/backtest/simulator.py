from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from tqdm import tqdm

from stockagent.backtest.portfolio import top_k_equal_weight

try:
    from numba import njit, prange
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray   # [T] net daily returns after costs
    benchmark_returns: np.ndarray  # [T] universe-average daily returns
    turnovers: np.ndarray          # [T] total absolute weight change per day
    weights_history: np.ndarray    # [T, S] realised portfolio weights


@staticmethod
def _vectorized_backtest(
    alpha_scores: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    fee_per_side: float,
    top_k: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Vectorized backtest computation using numpy.
    
    Returns:
        (strategy_returns, turnovers, weights_history)
    """
    T, S = alpha_scores.shape
    strategy_returns = np.zeros(T, dtype=np.float32)
    turnovers = np.zeros(T, dtype=np.float32)
    weights_history = np.zeros((T, S), dtype=np.float32)
    
    prev_weights = np.zeros(S, dtype=np.float32)
        for t in range(T):
        # Compute weights for day t using CPU workers
        weights = top_k_equal_weight(alpha_scores[t], tradable_mask[t], top_k)
        
        # Compute turnover and returns
        turnover = float(np.abs(weights - prev_weights).sum())
        gross = float(np.dot(weights, future_returns[t]))
        net = gross - turnover * fee_per_side
        
        strategy_returns[t] = net
        turnovers[t] = turnover
        weights_history[t] = weights
        prev_weights = weights
    
    return strategy_returns, turnovers, weights_history


def run_backtest(
    alpha_scores: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    fee_per_side: float,
    top_k: int,
) -> BacktestResult:
    """Simulate daily portfolio execution from model alpha scores.

    Transaction cost = ``fee_per_side * sum(|w_new - w_old|)`` per day,
    covering both the buy and sell legs of each rebalance.

    Args:
        alpha_scores:      model predictions           [T, S]
        future_returns:    realised next-day returns   [T, S]
        tradable_mask:     boolean tradability flags   [T, S]
        benchmark_returns: universe-average return     [T]
        fee_per_side:      fee rate per unit turnover
        top_k:             maximum portfolio size

    Returns:
        BacktestResult with per-day P&L, turnover, and weight history
    """
    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        alpha_scores,
        future_returns,
        tradable_mask,
        fee_per_side,
        top_k,
    )

    return BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.astype(np.float32),
        turnovers=turnovers,
        weights_history=weights_history,
    )
