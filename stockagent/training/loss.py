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
) -> Tensor:
    """Sharpe-aware loss for direct portfolio weights."""
    mask_f = tradable_mask.to(dtype=weights.dtype)
    masked_weights = weights * mask_f
    weight_sum = masked_weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    normalized_weights = masked_weights / weight_sum

    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    gross_returns = (normalized_weights * returns).sum(dim=1)

    prev_weights = torch.cat(
        [normalized_weights.new_zeros(1, normalized_weights.size(1)), normalized_weights[:-1]],
        dim=0,
    )
    turnover = (normalized_weights - prev_weights).abs().sum(dim=1)
    net_returns = gross_returns - fee_per_side * turnover

    mean_return = net_returns.mean()
    std_return = net_returns.std(unbiased=False).clamp_min(1e-8)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=weights.dtype))
    sharpe = mean_return / std_return * annualizer
    return -sharpe