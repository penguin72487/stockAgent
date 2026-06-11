from __future__ import annotations

import torch


def finite_mask_fill_value(values: torch.Tensor) -> float:
    if not values.dtype.is_floating_point:
        return -1e9
    return float(torch.finfo(values.dtype).min)


def masked_cross_sectional_mean(values: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    values = torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    if mask is None:
        return values.mean(dim=1, keepdim=True)

    mask_f = mask.to(dtype=values.dtype)
    denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    return (values * mask_f).sum(dim=1, keepdim=True) / denom


def masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    return masked_tanh_l1_weights(logits, mask, long_only=True)


def masked_tanh_l1_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    eps: float = 1e-8,
) -> torch.Tensor:
    """Convert scores to portfolio weights via tanh direction + L1 normalization.

    `tanh` is the only source of signed direction.  The L1 denominator controls
    gross exposure and keeps the row sum of absolute weights at 1 for non-empty
    active rows.  For long-only callers, negative tanh outputs are clipped out.
    """
    logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    weights = torch.tanh(logits)
    if long_only:
        weights = weights.clamp_min(0.0)
    if mask is None:
        denom = weights.abs().sum(dim=1, keepdim=True)
        return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))

    mask_bool = mask.bool()
    weights = torch.where(mask_bool, weights, torch.zeros_like(weights))
    denom = weights.abs().sum(dim=1, keepdim=True)
    return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))


def dual_branch_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    return masked_tanh_l1_weights(logits, mask, long_only=False)
