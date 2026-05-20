from __future__ import annotations

import torch
from torch import nn


def _masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    if mask is None:
        return torch.softmax(logits, dim=1)

    mask_bool = mask.bool()
    mask_f = mask.to(dtype=logits.dtype)
    masked_logits = logits.masked_fill(~mask_bool, torch.finfo(logits.dtype).min)
    weights = torch.softmax(masked_logits, dim=1) * mask_f
    normalizer = weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    return weights / normalizer


class CrossSectionalMLP(nn.Module):
    def __init__(self, lookback: int, num_features: int, num_symbols: int, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        input_dim = lookback * num_features
        self.num_symbols = num_symbols
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Return per-symbol portfolio weights with optional tradability masking."""
        batch_size, lookback, num_symbols, num_features = x.shape
        x = x.permute(0, 2, 1, 3).reshape(batch_size * num_symbols, lookback * num_features)
        logits = self.network(x).reshape(batch_size, num_symbols)
        return _masked_softmax(logits, tradable_mask)