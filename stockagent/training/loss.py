from __future__ import annotations

import torch
from torch import Tensor


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
    fee_per_side: float = 0.0,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
) -> Tensor:
    """Improved Sharpe-aware loss with numerically stable gradient flow."""
    mask_f = tradable_mask.to(dtype=weights.dtype)
    masked_weights = weights * mask_f
    weight_sum = masked_weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    normalized_weights = masked_weights / weight_sum

    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    gross_returns = (normalized_weights * returns).sum(dim=1)

    # Turnover cost
    prev_weights = torch.cat(
        [normalized_weights.new_zeros(1, normalized_weights.size(1)), normalized_weights[:-1]],
        dim=0,
    )
    turnover = (normalized_weights - prev_weights).abs().sum(dim=1)
    turnover_cost = (turnover * fee_per_side).mean()

    # ✅ FIXED: Improved Sharpe with stable gradients
    # Epsilon is added inside the square root to prevent gradient explosion
    mean_return = gross_returns.mean()
    centered = gross_returns - mean_return
    variance = (centered ** 2).mean()
    
    eps = 1e-8
    std_return = torch.sqrt(variance + eps)  # Epsilon inside sqrt for stable gradients
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=weights.dtype))
    
    # Clamp Sharpe to prevent extreme values
    sharpe = torch.clamp(
        mean_return / std_return * annualizer,
        min=-10.0,
        max=10.0
    )
    
    # Composite loss
    loss = -gamma_sharpe * sharpe + gamma_turnover * turnover_cost
    return loss