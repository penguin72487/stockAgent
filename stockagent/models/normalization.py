from __future__ import annotations

import torch


def masked_cross_sectional_mean(values: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    values = torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    if mask is None:
        return values.mean(dim=1, keepdim=True)

    mask_f = mask.to(dtype=values.dtype)
    denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    return (values * mask_f).sum(dim=1, keepdim=True) / denom


def masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    if mask is None:
        return torch.softmax(logits, dim=1)

    mask_bool = mask.bool()
    masked_logits = logits.masked_fill(~mask_bool, torch.finfo(logits.dtype).min)
    weights = torch.softmax(masked_logits, dim=1)
    weights = torch.where(mask_bool, weights, torch.zeros_like(weights))
    normalizer = weights.sum(dim=1, keepdim=True)
    return torch.where(normalizer > 0.0, weights / normalizer.clamp_min(1e-8), torch.zeros_like(weights))


def dual_branch_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    if mask is None:
        long_mask = logits > 0.0
        short_mask = logits < 0.0
    else:
        tradable = mask.bool()
        long_mask = tradable & (logits > 0.0)
        short_mask = tradable & (logits < 0.0)

    long_weights = masked_softmax(logits, long_mask)
    short_weights = masked_softmax(-logits, short_mask)

    long_strength = (logits.clamp_min(0.0) * long_mask.to(dtype=logits.dtype)).sum(dim=1, keepdim=True)
    short_strength = ((-logits).clamp_min(0.0) * short_mask.to(dtype=logits.dtype)).sum(dim=1, keepdim=True)
    total_strength = (long_strength + short_strength).clamp_min(1e-8)
    long_budget = long_strength / total_strength
    short_budget = short_strength / total_strength

    active = (long_mask.any(dim=1, keepdim=True) | short_mask.any(dim=1, keepdim=True)).to(dtype=logits.dtype)
    long_budget = long_budget * active
    short_budget = short_budget * active

    return long_budget * long_weights - short_budget * short_weights