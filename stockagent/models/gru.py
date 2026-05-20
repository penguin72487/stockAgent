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


class CrossSectionalGRU(PortfolioModel):
    """Temporal GRU per symbol, then cross-sectional softmax allocation."""

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        hidden_dim: int,
        dropout: float,
        num_layers: int = 1,
        residual_norm: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.num_features = num_features
        self.num_symbols = num_symbols
        self.hidden_dim = hidden_dim
        self.num_layers = max(1, int(num_layers))

        self.padded_num_features = ((num_features + 7) // 8) * 8
        self.feature_embedding = nn.Linear(self.padded_num_features, hidden_dim)
        self.input_norm = nn.LayerNorm(hidden_dim) if residual_norm else nn.Identity()

        self.temporal_encoder = nn.GRU(
            input_size=hidden_dim,
            hidden_size=hidden_dim,
            num_layers=self.num_layers,
            batch_first=True,
            dropout=dropout if self.num_layers > 1 else 0.0,
        )

        self.portfolio_head = nn.Sequential(
            nn.LayerNorm(hidden_dim) if residual_norm else nn.Identity(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Args: x [B, lookback, S, F], tradable_mask [B, S]. Returns [B, S]."""
        batch_size, lookback, num_symbols, feat_dim = x.shape
        x = x.permute(0, 2, 1, 3)  # [B, S, T, F]
        x = x.reshape(batch_size * num_symbols, lookback, feat_dim)  # [B*S, T, F]

        if self.padded_num_features > feat_dim:
            x = F.pad(x, (0, self.padded_num_features - feat_dim), mode="constant", value=0.0)

        x = self.feature_embedding(x)
        x = self.input_norm(x)
        x, _ = self.temporal_encoder(x)
        x = x[:, -1, :]  # last timestep state

        logits = self.portfolio_head(x).squeeze(-1).reshape(batch_size, num_symbols)
        return _masked_softmax(logits, tradable_mask)
