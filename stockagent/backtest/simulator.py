from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray   # [T] net daily returns after costs
    benchmark_returns: np.ndarray  # [T] universe-average daily returns
    turnovers: np.ndarray          # [T] total absolute weight change per day
    weights_history: np.ndarray    # [T, S] realised portfolio weights


@dataclass(slots=True)
class BacktestResultTensor:
    """Torch tensor container for a single backtest simulation run."""

    strategy_returns: torch.Tensor   # [T]
    benchmark_returns: torch.Tensor  # [T]
    turnovers: torch.Tensor          # [T]
    weights_history: torch.Tensor    # [T, S]

    def to_numpy(self) -> BacktestResult:
        return BacktestResult(
            strategy_returns=self.strategy_returns.detach().cpu().numpy().astype(np.float32),
            benchmark_returns=self.benchmark_returns.detach().cpu().numpy().astype(np.float32),
            turnovers=self.turnovers.detach().cpu().numpy().astype(np.float32),
            weights_history=self.weights_history.detach().cpu().numpy().astype(np.float32),
        )


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    fee_per_side: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    weights_history = np.asarray(weights, dtype=np.float32).copy()
    weights_history[~tradable_mask.astype(bool)] = 0.0

    weight_sums = weights_history.sum(axis=1, keepdims=True)
    nonzero = weight_sums.squeeze(1) > 0
    weights_history[nonzero] /= weight_sums[nonzero]

    prev = np.concatenate([
        np.zeros((1, weights_history.shape[1]), dtype=np.float32),
        weights_history[:-1],
    ], axis=0)
    turnovers = np.abs(weights_history - prev).sum(axis=1).astype(np.float32)

    gross = np.einsum("ts,ts->t", weights_history, future_returns, dtype=np.float32)
    strategy_returns = gross - fee_per_side * turnovers
    return strategy_returns.astype(np.float32), turnovers, weights_history


def _vectorized_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    fee_per_side: float,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    # ✅ FIXED: Simplified and more numerically stable weight normalization
    weights_history = weights.float().clone()
    
    # Step 1: Mask non-tradable symbols
    weights_history = weights_history.masked_fill(~tradable_mask.bool(), 0.0)
    
    # Step 2: Normalize weights (simplified logic)
    weight_sums = weights_history.sum(dim=1, keepdim=True).clamp_min(1e-12)
    weights_history = weights_history / weight_sums  # Direct broadcast normalization
    
    # Step 3: Compute turnover
    prev = torch.cat(
        [torch.zeros_like(weights_history[:1]), weights_history[:-1]],
        dim=0,
    )
    turnovers = (weights_history - prev).abs().sum(dim=1)

    # Step 4: Compute strategy returns
    gross = (weights_history * future_returns.float()).sum(dim=1)
    strategy_returns = gross - fee_per_side * turnovers
    return strategy_returns.float(), turnovers.float(), weights_history.float()


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


def run_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    fee_per_side: float,
) -> BacktestResultTensor:
    """Simulate daily portfolio execution from model weights in torch."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest_torch(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
    )

    return BacktestResultTensor(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.float(),
        turnovers=turnovers,
        weights_history=weights_history,
    )