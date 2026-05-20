from __future__ import annotations

import math

import numpy as np
import torch

try:
    import cupy as cp
except Exception:  # pragma: no cover - optional GPU dependency
    cp = None


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
    scores_t = torch.from_numpy(alpha_scores).float()
    returns_t = torch.from_numpy(future_log_returns).float()
    mask_t = torch.from_numpy(tradable_mask.astype(bool))
    return compute_ic_series_torch(scores_t, returns_t, mask_t).cpu().numpy().astype(np.float32)


def compute_ic_series_cupy(
    alpha_scores,
    future_log_returns,
    tradable_mask,
) -> np.ndarray:
    """GPU Spearman-like IC with CuPy; returns numpy array [T]."""
    if cp is None:
        return compute_ic_series(
            np.asarray(alpha_scores),
            np.asarray(future_log_returns),
            np.asarray(tradable_mask),
        )

    scores = cp.asarray(alpha_scores, dtype=cp.float32)
    returns = cp.asarray(future_log_returns, dtype=cp.float32)
    mask = cp.asarray(tradable_mask).astype(cp.bool_)

    valid_mask = mask & cp.isfinite(scores) & cp.isfinite(returns)
    mask_f = valid_mask.astype(cp.float32)
    valid_count = mask_f.sum(axis=1)

    s_fill = cp.where(valid_mask, scores, cp.full_like(scores, -1e30))
    r_fill = cp.where(valid_mask, returns, cp.full_like(returns, -1e30))
    s_rank = cp.argsort(cp.argsort(s_fill, axis=1), axis=1).astype(cp.float32)
    r_rank = cp.argsort(cp.argsort(r_fill, axis=1), axis=1).astype(cp.float32)

    denom_count = cp.clip(valid_count, 1.0, None)[:, None]
    s_mean = (s_rank * mask_f).sum(axis=1, keepdims=True) / denom_count
    r_mean = (r_rank * mask_f).sum(axis=1, keepdims=True) / denom_count

    s_c = (s_rank - s_mean) * mask_f
    r_c = (r_rank - r_mean) * mask_f
    cov = (s_c * r_c).sum(axis=1)
    denom = cp.sqrt(cp.clip((s_c**2).sum(axis=1) * (r_c**2).sum(axis=1), 1e-8, None))
    ic = cov / denom
    ic = cp.where(valid_count >= 2, ic, cp.zeros_like(ic))
    return cp.asnumpy(ic.astype(cp.float32))


def compute_ic_series_torch(
    alpha_scores: torch.Tensor,    # [T, S]
    future_log_returns: torch.Tensor,  # [T, S]
    tradable_mask: torch.Tensor,   # [T, S]
) -> torch.Tensor:                # [T]
    scores_t = alpha_scores.float()
    returns_t = future_log_returns.float()
    mask_t = tradable_mask.bool()

    valid_mask = mask_t & torch.isfinite(scores_t) & torch.isfinite(returns_t)
    mask_f = valid_mask.float()
    valid_count = mask_f.sum(dim=1)

    # Use masked fill then argsort(argsort()) for rank approximation.
    s_fill = torch.where(valid_mask, scores_t, torch.full_like(scores_t, -1e30))
    r_fill = torch.where(valid_mask, returns_t, torch.full_like(returns_t, -1e30))
    s_rank = torch.argsort(torch.argsort(s_fill, dim=1), dim=1).float()
    r_rank = torch.argsort(torch.argsort(r_fill, dim=1), dim=1).float()

    denom_count = valid_count.clamp_min(1.0).unsqueeze(1)
    s_mean = (s_rank * mask_f).sum(dim=1, keepdim=True) / denom_count
    r_mean = (r_rank * mask_f).sum(dim=1, keepdim=True) / denom_count

    s_c = (s_rank - s_mean) * mask_f
    r_c = (r_rank - r_mean) * mask_f
    cov = (s_c * r_c).sum(dim=1)
    denom = torch.sqrt((s_c.pow(2).sum(dim=1) * r_c.pow(2).sum(dim=1)).clamp_min(1e-8))
    ic = cov / denom
    ic = torch.where(valid_count >= 2, ic, torch.zeros_like(ic))
    return ic.float()


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
