from __future__ import annotations

import math

import numpy as np
import torch
from tqdm import tqdm


def masked_mse_loss(predictions: torch.Tensor, targets: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    mask_f = mask.to(dtype=predictions.dtype)
    squared_error = (predictions - targets).pow(2) * mask_f
    denominator = mask_f.sum().clamp_min(1.0)
    return squared_error.sum() / denominator


def select_top_k_weights(scores: np.ndarray, tradable_mask: np.ndarray, top_k: int) -> np.ndarray:
    weights = np.zeros_like(scores, dtype=np.float32)
    valid_idx = np.flatnonzero(tradable_mask & np.isfinite(scores))
    if valid_idx.size == 0:
        return weights
    chosen = valid_idx[np.argsort(scores[valid_idx])[-min(top_k, valid_idx.size) :]]
    weights[chosen] = 1.0 / float(chosen.size)
    return weights


def turnover_from_weights(previous_weights: np.ndarray, current_weights: np.ndarray) -> float:
    return float(np.abs(current_weights - previous_weights).sum())


def compute_ic_series(
    alpha_scores: np.ndarray,    # [T, S]
    future_log_returns: np.ndarray,  # [T, S]
    tradable_mask: np.ndarray,   # [T, S]
) -> np.ndarray:                 # [T] daily Spearman IC
    T = alpha_scores.shape[0]
    ics = np.zeros(T, dtype=np.float32)
        for t in range(T):
        mask = (
            tradable_mask[t].astype(bool)
            & np.isfinite(future_log_returns[t])
            & np.isfinite(alpha_scores[t])
        )
        if mask.sum() < 2:
            continue
        p = alpha_scores[t, mask].astype(np.float64)
        r = future_log_returns[t, mask].astype(np.float64)
        p_rank = p.argsort().argsort().astype(np.float64)
        r_rank = r.argsort().argsort().astype(np.float64)
        p_c = p_rank - p_rank.mean()
        r_c = r_rank - r_rank.mean()
        denom = math.sqrt((p_c**2).sum() * (r_c**2).sum()) + 1e-8
        ics[t] = float((p_c * r_c).sum() / denom)
    return ics


def ic_summary(ic_series: np.ndarray) -> dict[str, float]:
    clean = ic_series[np.isfinite(ic_series)]
    if clean.size == 0:
        return {"ic_mean": 0.0, "ic_std": 0.0, "ic_ir": 0.0, "ic_positive_ratio": 0.0}
    mean = float(clean.mean())
    std = float(clean.std(ddof=0)) + 1e-8
    return {
        "ic_mean": mean,
        "ic_std": std,
        "ic_ir": float(mean / std * math.sqrt(252.0)),
        "ic_positive_ratio": float((clean > 0).mean()),
    }


def summarize_returns(strategy_returns: np.ndarray, benchmark_returns: np.ndarray, turnover: np.ndarray) -> dict[str, float]:
    """Legacy helper retained for compatibility. Assumes log returns."""
    r = np.nan_to_num(strategy_returns, nan=0.0)
    b = np.nan_to_num(benchmark_returns, nan=0.0)
    cum_r = float(np.expm1(r.sum()))
    cum_b = float(np.expm1(b.sum()))
    avg = float(r.mean())
    std = float(r.std(ddof=0))
    ann_r = float(np.expm1(avg * 252.0))
    sharpe = float(avg / std * math.sqrt(252.0)) if std > 0 else 0.0
    equity = np.exp(np.cumsum(r))
    running_max = np.maximum.accumulate(equity)
    dd = equity / np.clip(running_max, 1e-12, None) - 1.0
    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "sharpe": sharpe,
        "max_drawdown": float(dd.min(initial=0.0)),
        "turnover": float(turnover.mean()) if turnover.size else 0.0,
        "daily_hit_rate": float((r > 0).mean()) if r.size else 0.0,
        "excess_return_vs_universe_average": cum_r - cum_b,
    }
