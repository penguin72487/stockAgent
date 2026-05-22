from __future__ import annotations

import torch
from torch import nn


def masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    if mask is None:
        return torch.softmax(logits, dim=1)

    mask_bool = mask.bool()
    mask_f = mask.to(dtype=logits.dtype)
    masked_logits = logits.masked_fill(~mask_bool, torch.finfo(logits.dtype).min)
    weights = torch.softmax(masked_logits, dim=1) * mask_f
    normalizer = weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    return weights / normalizer


def normalize_portfolio(
    logits: torch.Tensor,
    tradable_mask: torch.Tensor | None,
    long_only: bool,
) -> torch.Tensor:
    if long_only:
        return masked_softmax(logits, tradable_mask)

    if tradable_mask is not None:
        mask_bool = tradable_mask.bool()
        mask_f = tradable_mask.to(dtype=logits.dtype)
        masked_logits = logits.masked_fill(~mask_bool, 0.0)
        valid_count = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
        centered_logits = masked_logits - (masked_logits.sum(dim=1, keepdim=True) / valid_count)
        signed_scores = torch.tanh(centered_logits) * mask_f
    else:
        centered_logits = logits - logits.mean(dim=1, keepdim=True)
        signed_scores = torch.tanh(centered_logits)

    exposure = signed_scores.abs().sum(dim=1, keepdim=True).clamp_min(1e-8)
    return signed_scores / exposure


class AlphaModel(nn.Module):
    def __init__(self, *, lookback: int, long_only: bool) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.long_only = bool(long_only)

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        raise NotImplementedError
