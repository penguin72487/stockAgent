from __future__ import annotations

import torch
from torch import nn
from torch.nn import functional as F

from stockagent.models.base import PortfolioModel


class ResidualMLPBlock(nn.Module):
    """Residual MLP block with in-network normalization."""

    def __init__(self, dim: int, dropout: float, use_norm: bool) -> None:
        super().__init__()
        self.norm = nn.LayerNorm(dim) if use_norm else nn.Identity()
        self.ffn = nn.Sequential(
            nn.Linear(dim, dim * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(dim * 2, dim),
            nn.Dropout(dropout),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        x = self.norm(x)
        x = self.ffn(x)
        return residual + x


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
    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        hidden_dim: int,
        dropout: float,
        embedding_dim: int = 64,
        num_layers: int = 1,
        residual_norm: bool = True,
    ) -> None:
        super().__init__()
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        self.lookback = lookback
        self.num_features = num_features
        self.num_layers = max(1, int(num_layers))
        self.residual_norm = residual_norm
        # Tensor Core GEMM is most efficient when K dimension is a multiple of 8.
        self.padded_num_features = ((num_features + 7) // 8) * 8

        # Feature embedding shared across timesteps.
        self.feature_embedding = nn.Linear(self.padded_num_features, embedding_dim)
        self.embedding_norm = nn.LayerNorm(embedding_dim) if residual_norm else nn.Identity()

        temporal_input_dim = lookback * embedding_dim
        if residual_norm:
            self.temporal_mlp = nn.Sequential(
                nn.Linear(temporal_input_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.GELU(),
                nn.Dropout(dropout),
            )
            self.temporal_blocks = nn.ModuleList(
                [ResidualMLPBlock(hidden_dim, dropout, use_norm=True) for _ in range(self.num_layers)]
            )
            self.temporal_out = nn.Sequential(
                nn.LayerNorm(hidden_dim),
                nn.Linear(hidden_dim, embedding_dim),
                nn.GELU(),
            )
            self.temporal_plain = None
            self.portfolio_head = nn.Sequential(
                nn.LayerNorm(embedding_dim),
                nn.Linear(embedding_dim, hidden_dim),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(hidden_dim, 1),
            )
        else:
            plain_layers: list[nn.Module] = [
                nn.Linear(temporal_input_dim, hidden_dim),
                nn.GELU(),
                nn.Dropout(dropout),
            ]
            for _ in range(self.num_layers - 1):
                plain_layers.extend(
                    [
                        nn.Linear(hidden_dim, hidden_dim),
                        nn.GELU(),
                        nn.Dropout(dropout),
                    ]
                )
            plain_layers.extend(
                [
                    nn.Linear(hidden_dim, embedding_dim),
                    nn.GELU(),
                ]
            )
            self.temporal_plain = nn.Sequential(*plain_layers)
            self.temporal_mlp = None
            self.temporal_blocks = nn.ModuleList()
            self.temporal_out = None
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
        x = self.embedding_norm(x)

        # Temporal aggregation via MLP (no Transformer dependency).
        x = x.reshape(B * S, lookback * self.embedding_dim)
        if self.residual_norm:
            if self.temporal_mlp is None or self.temporal_out is None:
                raise RuntimeError("Residual-norm MLP path is not initialized.")
            x = self.temporal_mlp(x)
            for block in self.temporal_blocks:
                x = block(x)
            x = self.temporal_out(x)
        else:
            if self.temporal_plain is None:
                raise RuntimeError("Plain MLP path is not initialized.")
            x = self.temporal_plain(x)

        # Portfolio scoring
        logits = self.portfolio_head(x).squeeze(-1)  # [B*S]
        logits = logits.reshape(B, S)

        # Apply softmax with mask
        if tradable_mask is not None:
            weights = _masked_softmax(logits, tradable_mask)
        else:
            weights = torch.softmax(logits, dim=1)

        return weights
