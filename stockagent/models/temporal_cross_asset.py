from __future__ import annotations

import torch
from torch import nn

from stockagent.models.base import AlphaModel, normalize_portfolio


class TemporalConvEncoder(nn.Module):
    def __init__(self, d_model: int, dropout: float, kernel_size: int = 3, layers: int = 2) -> None:
        super().__init__()
        blocks: list[nn.Module] = []
        for layer_idx in range(max(1, layers)):
            dilation = 2 ** layer_idx
            pad = (kernel_size - 1) * dilation
            blocks.append(
                nn.Sequential(
                    nn.Conv1d(d_model, d_model, kernel_size=kernel_size, padding=pad, dilation=dilation),
                    nn.GELU(),
                    nn.Dropout(dropout),
                    nn.Conv1d(d_model, d_model, kernel_size=1),
                    nn.GELU(),
                )
            )
        self.blocks = nn.ModuleList(blocks)
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [N, L, D]
        h = x.transpose(1, 2)  # [N, D, L]
        for block in self.blocks:
            residual = h
            out = block(h)
            out = out[..., : h.size(-1)]
            h = out + residual
        h = h.transpose(1, 2)
        return self.norm(h[:, -1, :])


class TemporalTransformerEncoder(nn.Module):
    def __init__(
        self,
        d_model: int,
        nhead: int,
        ff_dim: int,
        layers: int,
        dropout: float,
        max_lookback: int,
    ) -> None:
        super().__init__()
        self.position = nn.Parameter(torch.randn(1, max_lookback, d_model) * 0.02)
        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=ff_dim,
            dropout=dropout,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=max(1, layers))
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [N, L, D]
        seq_len = x.size(1)
        if seq_len > self.position.size(1):
            raise ValueError(f"Input lookback {seq_len} exceeds configured max_lookback {self.position.size(1)}")
        h = x + self.position[:, :seq_len, :]
        h = self.encoder(h)
        return self.norm(h[:, -1, :])


class SetCrossAssetEncoder(nn.Module):
    def __init__(self, d_model: int, nhead: int, dropout: float, inducing_points: int = 16) -> None:
        super().__init__()
        m = max(1, inducing_points)
        self.inducing = nn.Parameter(torch.randn(m, d_model) * 0.02)
        self.to_inducing = nn.MultiheadAttention(d_model, nhead, dropout=dropout, batch_first=True)
        self.from_inducing = nn.MultiheadAttention(d_model, nhead, dropout=dropout, batch_first=True)
        self.ffn = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 2, d_model),
        )
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [B, S, D]
        bsz = x.size(0)
        inducing = self.inducing.unsqueeze(0).expand(bsz, -1, -1)
        h, _ = self.to_inducing(inducing, x, x, need_weights=False)
        out, _ = self.from_inducing(x, h, h, need_weights=False)
        out = out + self.ffn(out)
        return self.norm(out)


class TemporalCrossAssetModel(AlphaModel):
    """[B, L, S, F] -> temporal encoder -> [B, S, D] -> cross-asset encoder -> scores."""

    def __init__(
        self,
        *,
        num_features: int,
        num_symbols: int,
        long_only: bool,
        lookback: int = 20,
        d_model: int = 128,
        temporal_encoder: str = "gru",
        temporal_layers: int = 2,
        temporal_nhead: int = 4,
        temporal_ff_dim: int = 256,
        tcn_kernel_size: int = 3,
        cross_asset_encoder: str = "transformer",
        cross_asset_layers: int = 2,
        cross_asset_nhead: int = 4,
        cross_asset_ff_dim: int = 256,
        set_inducing_points: int = 16,
        dropout: float = 0.1,
    ) -> None:
        super().__init__(lookback=lookback, long_only=long_only)
        self.num_symbols = int(num_symbols)
        self.input_proj = nn.Sequential(
            nn.Linear(num_features, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
        )
        self.temporal_encoder_name = str(temporal_encoder).lower()
        self.cross_asset_encoder_name = str(cross_asset_encoder).lower()

        if self.temporal_encoder_name == "gru":
            self.temporal_gru = nn.GRU(
                input_size=d_model,
                hidden_size=d_model,
                num_layers=max(1, temporal_layers),
                dropout=dropout if temporal_layers > 1 else 0.0,
                batch_first=True,
            )
            self.temporal_encoder = None
        elif self.temporal_encoder_name == "tcn":
            self.temporal_gru = None
            self.temporal_encoder = TemporalConvEncoder(
                d_model=d_model,
                dropout=dropout,
                kernel_size=tcn_kernel_size,
                layers=temporal_layers,
            )
        elif self.temporal_encoder_name in {"transformer", "small_transformer"}:
            self.temporal_gru = None
            self.temporal_encoder = TemporalTransformerEncoder(
                d_model=d_model,
                nhead=temporal_nhead,
                ff_dim=temporal_ff_dim,
                layers=temporal_layers,
                dropout=dropout,
                max_lookback=max(lookback, 1),
            )
        else:
            raise ValueError(f"Unsupported temporal_encoder: {temporal_encoder}")

        if self.cross_asset_encoder_name == "transformer":
            cross_layer = nn.TransformerEncoderLayer(
                d_model=d_model,
                nhead=cross_asset_nhead,
                dim_feedforward=cross_asset_ff_dim,
                dropout=dropout,
                activation="gelu",
                batch_first=True,
                norm_first=True,
            )
            self.cross_encoder = nn.TransformerEncoder(cross_layer, num_layers=max(1, cross_asset_layers))
        elif self.cross_asset_encoder_name == "set_transformer":
            self.cross_encoder = SetCrossAssetEncoder(
                d_model=d_model,
                nhead=cross_asset_nhead,
                dropout=dropout,
                inducing_points=set_inducing_points,
            )
        else:
            raise ValueError(f"Unsupported cross_asset_encoder: {cross_asset_encoder}")

        self.score_head = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, 1),
        )

    def _temporal_encode(self, x: torch.Tensor) -> torch.Tensor:
        # x: [B, L, S, F]
        bsz, lookback, symbols, feats = x.shape
        if symbols != self.num_symbols:
            raise ValueError(f"Expected symbols={self.num_symbols}, got {symbols}")

        h = x.reshape(bsz * symbols, lookback, feats)
        h = self.input_proj(h)

        if self.temporal_gru is not None:
            h, _ = self.temporal_gru(h)
            encoded = h[:, -1, :]
        else:
            encoded = self.temporal_encoder(h)

        return encoded.reshape(bsz, symbols, -1)

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        if x.dim() != 4:
            raise ValueError(f"Expected [B, L, S, F], got shape {tuple(x.shape)}")

        per_asset = self._temporal_encode(x)
        fused = self.cross_encoder(per_asset)
        logits = self.score_head(fused).squeeze(-1)
        return normalize_portfolio(logits, tradable_mask, self.long_only)
