from __future__ import annotations

import torch
from torch import nn

from stockagent.models.base import AlphaModel, normalize_portfolio


class CrossSectionalMLP(AlphaModel):
    """Cross-sectional MLP that only uses current-day features (no lookback modeling)."""

    def __init__(
        self,
        num_features: int,
        num_symbols: int,
        hidden_dim: int,
        dropout: float,
        long_only: bool = True,
        hidden_layers: int = 2,
        lookback: int = 1,
        embedding_dim: int = 64,
    ) -> None:
        super().__init__(lookback=1, long_only=long_only)
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        self.hidden_layers = max(0, int(hidden_layers))

        self.feature_embedding = nn.Linear(num_features, embedding_dim)

        head_layers: list[nn.Module] = []
        if self.hidden_layers <= 0:
            head_layers.append(nn.Linear(embedding_dim, 1))
        else:
            in_dim = embedding_dim
            for _ in range(self.hidden_layers):
                head_layers.extend(
                    [
                        nn.Linear(in_dim, hidden_dim),
                        nn.GELU(),
                        nn.Dropout(dropout),
                    ]
                )
                in_dim = hidden_dim
            head_layers.append(nn.Linear(hidden_dim, 1))
        self.portfolio_head = nn.Sequential(*head_layers)

        # Preserve compatibility with previous call signatures while making
        # the model strictly cross-sectional.
        _ = lookback

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        # x can be [B, L, S, F] or [B, S, F]. MLP only consumes the latest day.
        if x.dim() == 4:
            x = x[:, -1, :, :]
        elif x.dim() != 3:
            raise ValueError(f"Expected x to have 3 or 4 dims, got shape {tuple(x.shape)}")

        embedded = self.feature_embedding(x)
        logits = self.portfolio_head(embedded).squeeze(-1)
        return normalize_portfolio(logits, tradable_mask, self.long_only)
