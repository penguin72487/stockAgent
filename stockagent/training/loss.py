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
    sample_mask: Tensor | None = None,
    fee_per_side: float = 0.0,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
    cash_symbol_mask: Tensor | None = None,
) -> Tensor:
    """Improved Sharpe-aware loss with numerically stable gradient flow."""
    mask_f = tradable_mask.to(dtype=weights.dtype)
    if sample_mask is None:
        sample_mask_f = torch.ones(weights.size(0), device=weights.device, dtype=weights.dtype)
    else:
        sample_mask_f = sample_mask.to(device=weights.device, dtype=weights.dtype)

    masked_weights = weights * mask_f
    weight_sum = masked_weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    normalized_weights = masked_weights / weight_sum

    returns = torch.nan_to_num(future_log_returns, nan=0.0, posinf=0.0, neginf=0.0)
    gross_returns = (normalized_weights * returns).sum(dim=1)
    valid_count = sample_mask_f.sum().clamp_min(1.0)

    # Turnover cost
    prev_weights = torch.cat(
        [normalized_weights.new_zeros(1, normalized_weights.size(1)), normalized_weights[:-1]],
        dim=0,
    )
    delta = (normalized_weights - prev_weights).abs()
    if cash_symbol_mask is not None:
        cash_mask = cash_symbol_mask.to(device=weights.device, dtype=torch.bool)
        if cash_mask.ndim != 1 or cash_mask.numel() != delta.size(1):
            raise ValueError(
                "cash_symbol_mask shape must match num_symbols: "
                f"expected ({delta.size(1)},), got {tuple(cash_mask.shape)}"
            )
        if bool(cash_mask.any().item()):
            delta = delta.masked_fill(cash_mask.unsqueeze(0), 0.0)
    turnover = delta.sum(dim=1)
    turnover_cost = ((turnover * sample_mask_f).sum() * fee_per_side) / valid_count

    # ✅ FIXED: Improved Sharpe with stable gradients
    # Epsilon is added inside the square root to prevent gradient explosion
    masked_returns = gross_returns * sample_mask_f
    mean_return = masked_returns.sum() / valid_count
    centered = (gross_returns - mean_return) * sample_mask_f
    variance = (centered ** 2).sum() / valid_count
    
    eps = 1e-8
    std_return = torch.sqrt(variance + eps)  # Epsilon inside sqrt for stable gradients
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=weights.dtype))
    
    sharpe = mean_return / std_return * annualizer
    
    # Composite loss
    loss = -gamma_sharpe * sharpe + gamma_turnover * turnover_cost
    return loss