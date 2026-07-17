from __future__ import annotations

import os

import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, normalize_portfolio_activation


class _FeatureResBlock(nn.Module):
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


class _TemporalTCNBlock(nn.Module):
    def __init__(self, channels: int, kernel_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        self.kernel_size = int(kernel_size)
        self.dilation = int(dilation)
        self.pad_left = (self.kernel_size - 1) * self.dilation
        self.conv1 = nn.Conv1d(channels, channels, kernel_size=self.kernel_size, dilation=self.dilation)
        self.conv2 = nn.Conv1d(channels, channels, kernel_size=self.kernel_size, dilation=self.dilation)
        self.norm = nn.BatchNorm1d(channels)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(dropout)

    def _causal_conv(self, conv: nn.Conv1d, x: torch.Tensor) -> torch.Tensor:
        x = nn.functional.pad(x, (self.pad_left, 0))
        return conv(x)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [N, C, T]
        residual = x
        x = self._causal_conv(self.conv1, x)
        x = self.act(x)
        x = self.dropout(x)
        x = self._causal_conv(self.conv2, x)
        x = self.norm(x)
        x = self.act(x)
        x = self.dropout(x)
        return residual + x


class CrossSectionalTCNHybridTabularResNet(nn.Module):
    """Tabular ResNet encoder + TCN temporal model for cross-sectional weights.

    Flow:
    - Per-day per-symbol features -> Tabular encoder -> embedding
    - Per-symbol temporal sequence embeddings -> TCN
    - Last timestep representation -> per-symbol score
    - Cross-sectional normalization -> portfolio weights
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        embedding_dim: int,
        encoder_hidden_dim: int,
        encoder_blocks: int,
        tcn_blocks: int,
        tcn_kernel_size: int,
        dropout: float,
        long_only: bool = True,
        portfolio_activation: str = "identity",
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

        self.feature_encoder = nn.Sequential(
            nn.Linear(self.num_features, int(encoder_hidden_dim)),
            nn.GELU(),
            nn.Dropout(dropout),
            *[_FeatureResBlock(int(encoder_hidden_dim), float(dropout)) for _ in range(max(1, int(encoder_blocks)))],
            nn.LayerNorm(int(encoder_hidden_dim)),
            nn.Linear(int(encoder_hidden_dim), int(embedding_dim)),
        )

        blocks: list[nn.Module] = []
        for i in range(max(1, int(tcn_blocks))):
            blocks.append(
                _TemporalTCNBlock(
                    channels=int(embedding_dim),
                    kernel_size=max(2, int(tcn_kernel_size)),
                    dilation=2**i,
                    dropout=float(dropout),
                )
            )
        self.temporal_tcn = nn.Sequential(*blocks)
        self.score_head = nn.Sequential(
            nn.LayerNorm(int(embedding_dim)),
            nn.Linear(int(embedding_dim), 1),
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
        steps = int(x.size(1))
        n_symbols = int(x.size(2))

        # Per-day, per-symbol tabular encoding.
        z = torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0).reshape(bsz * steps * n_symbols, self.num_features)
        z = self.feature_encoder(z)
        z = torch.nan_to_num(z, nan=0.0, posinf=0.0, neginf=0.0)

        # Temporal TCN over each symbol sequence.
        z = z.reshape(bsz, steps, n_symbols, -1).permute(0, 2, 3, 1).reshape(bsz * n_symbols, -1, steps)
        z = self.temporal_tcn(z)

        # Use latest temporal state for scoring.
        latest = z[:, :, -1]
        logits = self.score_head(latest).reshape(bsz, n_symbols)
        logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)

        if self.long_only:
            return masked_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        return dual_branch_softmax(logits, tradable_mask, activation=self.portfolio_activation)
