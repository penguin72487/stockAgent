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


class _ResBlock(nn.Module):
    def __init__(self, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        self.norm = nn.LayerNorm(hidden_dim)
        self.fc1 = nn.Linear(hidden_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        x = self.norm(x)
        x = self.fc1(x)
        x = self.act(x)
        x = self.dropout(x)
        x = self.fc2(x)
        x = self.dropout(x)
        return residual + x


class CrossSectionalTabularResNet(nn.Module):
    """Tabular ResNet-style model for cross-sectional portfolio weights."""

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        embedding_dim: int,
        hidden_dim: int,
        n_blocks: int,
        dropout: float,
        long_only: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        input_dim = self.lookback * int(num_features)

        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, int(embedding_dim)),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(int(embedding_dim), int(hidden_dim)),
        )
        self.blocks = nn.Sequential(
            *[_ResBlock(int(hidden_dim), float(dropout)) for _ in range(max(1, int(n_blocks)))],
        )
        self.head = nn.Sequential(
            nn.LayerNorm(int(hidden_dim)),
            nn.Linear(int(hidden_dim), 1),
        )

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        # x: [B, lookback, S, F]
        bsz, lookback, num_symbols, n_features = x.shape
        if lookback != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {lookback}")
        if num_symbols != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {num_symbols}")

        x = x.permute(0, 2, 1, 3).reshape(bsz, num_symbols, lookback * n_features)
        x = self.input_proj(x)
        x = self.blocks(x)
        logits = self.head(x).squeeze(-1)

        if self.long_only:
            return _masked_softmax(logits, tradable_mask)

        if tradable_mask is not None:
            mask_f = tradable_mask.to(dtype=logits.dtype)
            denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
            mean_logits = (logits * mask_f).sum(dim=1, keepdim=True) / denom
            centered = (logits - mean_logits) * mask_f
        else:
            centered = logits - logits.mean(dim=1, keepdim=True)
        gross = centered.abs().sum(dim=1, keepdim=True).clamp_min(1e-8)
        return centered / gross
