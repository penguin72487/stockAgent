from __future__ import annotations

import numpy as np
import torch
from torch import Tensor


def masked_mse_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    """MSE loss computed only over tradable symbols."""
    mask_f = mask.to(dtype=predictions.dtype)
    error = (predictions - targets).pow(2) * mask_f
    return error.sum() / mask_f.sum().clamp_min(1.0)


def masked_ic_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    """Negative mean rank-IC across batch rows (minimise to maximise IC).

    Uses a differentiable rank-correlation approximation via Pearson
    on double-argsort ranks.
    """
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
    alpha_scores: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    top_k: int,
    fee_per_side: float = 0.0,
) -> Tensor:
    """Sharpe-aware loss that optimizes realized Sharpe ratio.
    
    For each batch:
    1. Select top-k by alpha_scores
    2. Compute portfolio returns
    3. Compute Sharpe ratio across batch days
    4. Return negative Sharpe (to minimize)
    """
    batch_size, num_symbols = alpha_scores.shape
    device = alpha_scores.device
    dtype = alpha_scores.dtype
    
    alpha_scores_np = alpha_scores.detach().cpu().numpy()
    future_log_returns_np = future_log_returns.detach().cpu().numpy()
    tradable_mask_np = tradable_mask.detach().cpu().numpy()
    
    portfolio_rets = []
    
    for b in range(batch_size):
        scores = alpha_scores_np[b]
        rets = future_log_returns_np[b]
        mask = tradable_mask_np[b].astype(bool)
        
        valid_idx = np.flatnonzero(mask & np.isfinite(scores))
        if valid_idx.size == 0:
            portfolio_rets.append(0.0)
            continue
            
        k = min(top_k, valid_idx.size)
        top_indices = valid_idx[np.argsort(scores[valid_idx])[-k:]]
        
        weights = np.zeros(num_symbols, dtype=np.float32)
        weights[top_indices] = 1.0 / k
        
        port_ret = float(np.dot(weights, np.nan_to_num(rets, nan=0.0)))
        portfolio_rets.append(port_ret)
    
    rets_array = np.array(portfolio_rets, dtype=np.float32)
    mean_ret = float(rets_array.mean())
    std_ret = float(rets_array.std(ddof=0)) + 1e-8
    sharpe = mean_ret / std_ret * np.sqrt(252.0)
    
    return torch.tensor(-sharpe, dtype=dtype, device=device, requires_grad=False)
