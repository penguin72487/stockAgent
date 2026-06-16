from __future__ import annotations

import torch
from torch import nn

from stockagent.models.efficient_tcn_tabular_set_portfolio import LiteISAB
from stockagent.models.normalization import finite_mask_fill_value, masked_softmax


class CausalTCNBlock(nn.Module):
    """Small per-stock causal TCN block.

    Input/output: [B*S, D, L].
    """

    def __init__(self, dim: int, kernel_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        kernel = max(2, int(kernel_size))
        dilation = max(1, int(dilation))
        self.pad_left = (kernel - 1) * dilation
        self.depthwise = nn.Conv1d(dim, dim, kernel_size=kernel, dilation=dilation, groups=dim, bias=False)
        self.pointwise = nn.Conv1d(dim, dim, kernel_size=1)
        self.norm = nn.LayerNorm(dim)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(float(dropout))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        y = nn.functional.pad(x, (self.pad_left, 0))
        y = self.depthwise(y)
        y = self.pointwise(y)
        y = self.act(y)
        y = self.dropout(y)
        y = y + residual
        return self.norm(y.transpose(1, 2)).transpose(1, 2).contiguous()


class BottleneckPortfolioAutoencoder(nn.Module):
    """Encoder-bottleneck-decoder model that directly emits portfolio weights.

    Expected inputs:
        x: [B, L, S, F]
        tradable_mask: [B, S], True means the symbol may be traded.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_model: int = 128,
        z_dim: int = 32,
        temporal_type: str = "gru",
        temporal_layers: int = 1,
        asset_encoder_type: str = "transformer",
        asset_encoder_layers: int = 2,
        n_heads: int = 4,
        num_inducing_points: int = 32,
        ffn_mult: int = 2,
        dropout: float = 0.1,
        long_short: bool = True,
        noise_std: float = 0.01,
        return_aux: bool = True,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.d_model = int(d_model)
        self.z_dim = int(z_dim)
        self.temporal_type = str(temporal_type).strip().lower()
        self.asset_encoder_type = str(asset_encoder_type).strip().lower().replace("-", "_")
        self.long_short = bool(long_short)
        self.noise_std = float(noise_std)
        self.return_aux = bool(return_aux)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)

        if self.d_model <= 0 or self.z_dim <= 0:
            raise ValueError("d_model and z_dim must be positive")

        self.feature_embedding = nn.Sequential(
            nn.Linear(self.num_features, self.d_model),
            nn.LayerNorm(self.d_model),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )

        layers = max(1, int(temporal_layers))
        if self.temporal_type == "gru":
            self.temporal_encoder = nn.GRU(
                input_size=self.d_model,
                hidden_size=self.d_model,
                num_layers=layers,
                batch_first=True,
                dropout=float(dropout) if layers > 1 else 0.0,
            )
            self.temporal_tcn = None
        elif self.temporal_type == "tcn":
            self.temporal_encoder = None
            self.temporal_tcn = nn.Sequential(
                *[
                    CausalTCNBlock(
                        dim=self.d_model,
                        kernel_size=3,
                        dilation=2**idx,
                        dropout=float(dropout),
                    )
                    for idx in range(layers)
                ]
            )
        else:
            raise ValueError("temporal_type must be 'gru' or 'tcn'")

        heads = max(1, int(n_heads))
        if self.d_model % heads != 0:
            raise ValueError(f"d_model={self.d_model} must be divisible by n_heads={heads}")
        if self.asset_encoder_type in {"transformer", "transformer_encoder", "self_attention"}:
            asset_layer = nn.TransformerEncoderLayer(
                d_model=self.d_model,
                nhead=heads,
                dim_feedforward=self.d_model * 2,
                dropout=float(dropout),
                activation="gelu",
                batch_first=True,
                norm_first=False,
            )
            try:
                self.asset_encoder = nn.TransformerEncoder(
                    asset_layer,
                    num_layers=max(1, int(asset_encoder_layers)),
                    enable_nested_tensor=False,
                )
            except TypeError:
                self.asset_encoder = nn.TransformerEncoder(asset_layer, num_layers=max(1, int(asset_encoder_layers)))
        elif self.asset_encoder_type in {"lite_isab", "isab", "set_transformer", "set"}:
            self.asset_encoder = nn.Sequential(
                *[
                    LiteISAB(
                        dim=self.d_model,
                        num_heads=heads,
                        num_inducing_points=max(1, int(num_inducing_points)),
                        ffn_mult=max(1, int(ffn_mult)),
                        dropout=float(dropout),
                    )
                    for _ in range(max(1, int(asset_encoder_layers)))
                ]
            )
        else:
            raise ValueError("asset_encoder_type must be 'transformer' or 'lite_isab'")

        self.bottleneck = nn.Sequential(
            nn.LayerNorm(self.d_model),
            nn.Linear(self.d_model, self.z_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )
        self.decoder = nn.Sequential(
            nn.LayerNorm(self.z_dim),
            nn.Linear(self.z_dim, self.d_model),
            nn.GELU(),
            nn.Linear(self.d_model, self.d_model),
            nn.GELU(),
        )
        self.weight_head = nn.Linear(self.d_model, 1)

    def _check_shapes(self, x: torch.Tensor, mask: torch.Tensor | None) -> None:
        if x.dim() != 4:
            raise ValueError(f"Expected x shape [B,L,S,F], got ndim={x.dim()}")
        if int(x.size(1)) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(x.size(1))}")
        if (not self.allow_dynamic_symbols) and int(x.size(2)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x.size(2))}")
        if int(x.size(3)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")
        if mask is not None and tuple(mask.shape) != (int(x.size(0)), int(x.size(2))):
            raise ValueError(f"Expected mask shape {(int(x.size(0)), int(x.size(2)))}, got {tuple(mask.shape)}")

    @staticmethod
    def _safe_attention_mask(mask_bool: torch.Tensor) -> torch.Tensor:
        torch._assert(
            mask_bool.any(dim=1).all(),
            "tradable mask contains an all-false row; no-fallback path requires at least one tradable symbol per row",
        )
        return mask_bool

    def _encode_temporal(self, embedded: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, dim = embedded.shape
        h = embedded.permute(0, 2, 1, 3).contiguous().reshape(bsz * n_symbols, steps, dim)
        if self.temporal_type == "gru":
            if self.temporal_encoder is None:
                raise RuntimeError("temporal_encoder is unexpectedly None")
            out, _ = self.temporal_encoder(h)
            h_last = out[:, -1, :]
        else:
            if self.temporal_tcn is None:
                raise RuntimeError("temporal_tcn is unexpectedly None")
            h_tcn = self.temporal_tcn(h.transpose(1, 2).contiguous())
            h_last = h_tcn[:, :, -1]
        return h_last.reshape(bsz, n_symbols, dim)

    def _long_short_weights(self, scores: torch.Tensor, mask_bool: torch.Tensor) -> torch.Tensor:
        raw = torch.tanh(torch.nan_to_num(scores, nan=0.0, posinf=20.0, neginf=-20.0))
        raw = raw.masked_fill(~mask_bool, 0.0)
        denom = raw.abs().sum(dim=1, keepdim=True).clamp_min(1e-8)
        return raw / denom

    def forward(
        self,
        x: torch.Tensor,
        tradable_mask: torch.Tensor | None = None,
        prev_weights: torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        if self.runtime_shape_check:
            self._check_shapes(x, tradable_mask)

        if tradable_mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = tradable_mask.to(device=x.device, dtype=torch.bool)
        safe_mask = self._safe_attention_mask(mask_bool)

        embedded = self.feature_embedding(torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0))
        temporal = self._encode_temporal(embedded)
        temporal = temporal.masked_fill(~safe_mask.unsqueeze(-1), 0.0)

        if self.asset_encoder_type in {"transformer", "transformer_encoder", "self_attention"}:
            encoded = self.asset_encoder(temporal, src_key_padding_mask=~safe_mask)
        else:
            encoded = temporal
            for layer in self.asset_encoder:
                encoded = layer(encoded, safe_mask)
        encoded = encoded.masked_fill(~mask_bool.unsqueeze(-1), 0.0)

        z = self.bottleneck(encoded)
        if self.training and self.noise_std > 0.0:
            z = z + torch.randn_like(z) * self.noise_std
        z = z.masked_fill(~mask_bool.unsqueeze(-1), 0.0)

        decoded = self.decoder(z)
        decoded = decoded.masked_fill(~mask_bool.unsqueeze(-1), 0.0)
        scores = self.weight_head(decoded).squeeze(-1)
        masked_scores = scores.masked_fill(~mask_bool, finite_mask_fill_value(scores))

        if self.long_short:
            weights = self._long_short_weights(scores, mask_bool)
        else:
            weights = masked_softmax(masked_scores, mask_bool)
        weights = weights.masked_fill(~mask_bool, 0.0)

        if return_aux is True:
            aux = {
                "encoded": encoded,
                "z": z,
                "latent_z": z,
                "decoded": decoded,
                "prev_weights": prev_weights,
            }
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            aux = {
                "encoded": encoded,
                "z": z,
                "latent_z": z,
                "decoded": decoded,
                "prev_weights": prev_weights,
            }
            return {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
                "z": z,
                "latent_z": z,
                "decoded": decoded,
                "aux": aux,
            }
        return weights
