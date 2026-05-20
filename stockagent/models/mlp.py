from __future__ import annotations

import torch
from torch import nn


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

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Output shape: [batch_size, num_symbols] - alpha scores per symbol."""
        batch_size, lookback, num_symbols, num_features = x.shape
        x = x.permute(0, 2, 1, 3).reshape(batch_size * num_symbols, lookback * num_features)
        scores = self.network(x)
        return scores.reshape(batch_size, num_symbols)
