from __future__ import annotations

import torch
from torch import nn
from torch.nn import functional as F

from stockagent.models.base import PortfolioModel


def _masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    if mask is None:
        return torch.softmax(logits, dim=1)

    mask_bool = mask.bool()
    mask_f = mask.to(dtype=logits.dtype)
    masked_logits = logits.masked_fill(~mask_bool, torch.finfo(logits.dtype).min)
    weights = torch.softmax(masked_logits, dim=1) * mask_f
    normalizer = weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    return weights / normalizer


class CrossSectionalMLP(PortfolioModel):
    """Cross-sectional MLP that maps per-symbol features to portfolio weights."""
    def __init__(self, lookback: int, num_features: int, num_symbols: int, hidden_dim: int, dropout: float, embedding_dim: int = 64) -> None:
        super().__init__()
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        self.lookback = lookback
        self.num_features = num_features
        # Tensor Core GEMM is most efficient when K dimension is a multiple of 8.
        self.padded_num_features = ((num_features + 7) // 8) * 8

        # Feature embedding shared across timesteps.
        self.feature_embedding = nn.Linear(self.padded_num_features, embedding_dim)

        # Pure-MLP temporal projection (flatten lookback dimension).
        self.temporal_mlp = nn.Sequential(
            nn.Linear(lookback * embedding_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, embedding_dim),
            nn.GELU(),
        )

        self.portfolio_head = nn.Sequential(
            nn.Linear(embedding_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Return per-symbol portfolio weights.

        Args:
            x: [B, lookback, S, F]
            tradable_mask: [B, S] bool (optional)

        Returns:
            weights: [B, S]
        """
        B, lookback, S, feat_dim = x.shape
        x = x.permute(0, 2, 1, 3)  # [B, S, lookback, F]
        x = x.reshape(B * S, lookback, feat_dim)  # [B*S, lookback, F]

        if self.padded_num_features > feat_dim:
            x = F.pad(x, (0, self.padded_num_features - feat_dim), mode="constant", value=0.0)

        # Feature embedding
        x = self.feature_embedding(x)  # [B*S, lookback, embedding_dim]

        # Temporal aggregation via MLP (no Transformer dependency).
        x = x.reshape(B * S, lookback * self.embedding_dim)
        x = self.temporal_mlp(x)

        # Portfolio scoring
        logits = self.portfolio_head(x).squeeze(-1)  # [B*S]
        logits = logits.reshape(B, S)

        # Apply softmax with mask
        if tradable_mask is not None:
            weights = _masked_softmax(logits, tradable_mask)
        else:
            weights = torch.softmax(logits, dim=1)

        return weights
