from __future__ import annotations

import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, normalize_portfolio_activation


class CrossSectionalMLP(nn.Module):
    """Cross-sectional MLP for portfolio weights."""
    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        hidden_dim: int,
        dropout: float,
        embedding_dim: int = 64,
        hidden_layers: int = 2,
        long_only: bool = True,
        portfolio_activation: str = "gd",
    ) -> None:
        super().__init__()
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        self.lookback = lookback
        self.hidden_layers = max(0, int(hidden_layers))
        self.long_only = bool(long_only)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        
        # Flatten each symbol's lookback window and compress it to embedding_dim.
        self.feature_embedding = nn.Linear(lookback * num_features, embedding_dim)
        
        # Portfolio scoring head with configurable hidden depth.
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

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Return per-symbol portfolio weights.
        
        Args:
            x: [B, lookback, S, F]
            tradable_mask: [B, S] bool (optional)
        
        Returns:
            weights: [B, S]
        """
        B, lookback, S, F = x.shape
        x = x.permute(0, 2, 1, 3).reshape(B, S, lookback * F)  # [B, S, lookback*F]
        x = self.feature_embedding(x)  # [B, S, embedding_dim]
        logits = self.portfolio_head(x).squeeze(-1)  # [B, S]
        
        if self.long_only:
            weights = masked_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        else:
            weights = dual_branch_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        
        return weights
