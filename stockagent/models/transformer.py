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


class CrossSectionalTransformer(PortfolioModel):
    """Temporal Transformer encoder applied independently per symbol."""

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        hidden_dim: int,
        dropout: float,
        num_layers: int = 2,
        residual_norm: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = lookback
        self.num_features = num_features
        self.num_symbols = num_symbols
        self.hidden_dim = hidden_dim
        self.num_layers = max(1, int(num_layers))

        # Tensor Core GEMM is most efficient when K dimension is a multiple of 8.
        self.padded_num_features = ((num_features + 7) // 8) * 8

        # Ensure hidden_dim is divisible by num_heads (prefer 8, then 4, then 2, then 1).
        candidates = (8, 4, 2, 1)
        num_heads = next((head for head in candidates if hidden_dim % head == 0), 1)
        ff_dim = max(hidden_dim * 2, hidden_dim + 32)

        self.feature_embedding = nn.Linear(self.padded_num_features, hidden_dim)
        self.input_norm = nn.LayerNorm(hidden_dim) if residual_norm else nn.Identity()

        self.temporal_positional = nn.Parameter(torch.zeros(1, lookback, hidden_dim))

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=num_heads,
            dim_feedforward=ff_dim,
            dropout=dropout,
            activation="gelu",
            batch_first=True,
            norm_first=residual_norm,
        )
        encoder_norm = nn.LayerNorm(hidden_dim) if residual_norm else None
        self.temporal_encoder = nn.TransformerEncoder(
            encoder_layer=encoder_layer,
            num_layers=self.num_layers,
            norm=encoder_norm,
            enable_nested_tensor=False,
        )

        self.portfolio_head = nn.Sequential(
            nn.LayerNorm(hidden_dim) if residual_norm else nn.Identity(),
            nn.Linear(hidden_dim, hidden_dim),
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
        batch_size, lookback, num_symbols, feat_dim = x.shape
        x = x.permute(0, 2, 1, 3)  # [B, S, lookback, F]
        x = x.reshape(batch_size * num_symbols, lookback, feat_dim)  # [B*S, lookback, F]

        if self.padded_num_features > feat_dim:
            x = F.pad(x, (0, self.padded_num_features - feat_dim), mode="constant", value=0.0)

        x = self.feature_embedding(x)
        x = self.input_norm(x)

        # Learnable temporal position helps when lookback > 1 while staying harmless for lookback = 1.
        x = x + self.temporal_positional[:, :lookback, :]
        x = self.temporal_encoder(x)

        # Pool over time into one per-symbol representation.
        x = x.mean(dim=1)

        logits = self.portfolio_head(x).squeeze(-1)  # [B*S]
        logits = logits.reshape(batch_size, num_symbols)
        return _masked_softmax(logits, tradable_mask)
