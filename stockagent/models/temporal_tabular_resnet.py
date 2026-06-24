from __future__ import annotations

import os

import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, normalize_portfolio_activation


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


class CrossSectionalTemporalTabularResNet(nn.Module):
    """Temporal Tabular ResNet for cross-sectional portfolio weights.

    Architecture:
    1) Per-symbol temporal encoder (GRU) over lookback steps.
    2) Tabular ResNet head to produce one logit per symbol.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        temporal_hidden_dim: int,
        temporal_layers: int,
        temporal_dropout: float,
        embedding_dim: int,
        hidden_dim: int,
        n_blocks: int,
        dropout: float,
        long_only: bool = True,
        portfolio_activation: str = "gd",
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)

        rnn_dropout = float(temporal_dropout) if int(temporal_layers) > 1 else 0.0
        self.temporal_encoder = nn.GRU(
            input_size=self.num_features,
            hidden_size=int(temporal_hidden_dim),
            num_layers=max(1, int(temporal_layers)),
            dropout=rnn_dropout,
            batch_first=True,
        )

        self.input_proj = nn.Sequential(
            nn.Linear(int(temporal_hidden_dim), int(embedding_dim)),
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
        runtime_shape_check = self.runtime_shape_check or os.environ.get("STOCKAGENT_RUNTIME_SHAPE_CHECK", "0").strip().lower() in {"1", "true", "on", "yes"}
        if runtime_shape_check:
            if x.dim() != 4:
                raise ValueError(f"Expected x.ndim=4, got {x.dim()}")
            if int(x.size(1)) != self.lookback:
                raise ValueError(f"Expected lookback={self.lookback}, got {int(x.size(1))}")
            if (not self.allow_dynamic_symbols) and int(x.size(2)) != self.num_symbols:
                raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x.size(2))}")
            if int(x.size(3)) != self.num_features:
                raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")

        bsz = int(x.size(0))
        n_symbols = int(x.size(2))

        # Per-symbol temporal encoding.
        seq = x.permute(0, 2, 1, 3).reshape(bsz * n_symbols, self.lookback, self.num_features)
        seq = torch.nan_to_num(seq, nan=0.0, posinf=0.0, neginf=0.0)
        _, h_n = self.temporal_encoder(seq)
        symbol_repr = torch.nan_to_num(h_n[-1], nan=0.0, posinf=0.0, neginf=0.0)  # [B*S, temporal_hidden_dim]

        y = self.input_proj(symbol_repr)
        y = self.blocks(y)
        logits = self.head(y).reshape(bsz, n_symbols)
        logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)

        if self.long_only:
            return masked_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        return dual_branch_softmax(logits, tradable_mask, activation=self.portfolio_activation)
