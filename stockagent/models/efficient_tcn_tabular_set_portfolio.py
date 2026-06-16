from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn

from stockagent.models.normalization import (
    dual_branch_softmax,
    finite_mask_fill_value,
    masked_cross_sectional_mean,
    masked_softmax,
)


class CausalDepthwiseSeparableTCNBlock(nn.Module):
    """Causal depthwise-separable TCN block.

    Input/output: [N, D, L], where N=B*S.
    """

    def __init__(self, dim: int, kernel_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        self.pad_left = (max(2, int(kernel_size)) - 1) * max(1, int(dilation))
        self.depthwise = nn.Conv1d(
            dim,
            dim,
            kernel_size=max(2, int(kernel_size)),
            dilation=max(1, int(dilation)),
            groups=dim,
            bias=False,
        )
        self.pointwise = nn.Conv1d(dim, dim, kernel_size=1)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(float(dropout))
        self.norm = nn.LayerNorm(dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        y = nn.functional.pad(x, (self.pad_left, 0))
        y = self.depthwise(y)
        y = self.pointwise(y)
        y = self.act(y)
        y = self.dropout(y)
        y = y + residual
        return self.norm(y.transpose(1, 2)).transpose(1, 2).contiguous()


class TemporalTCNBranch(nn.Module):
    """Per-stock temporal encoder.

    Input:  x [B, L, S, F]
    Output: z_time [B, S, temporal_dim]
    """

    def __init__(
        self,
        num_features: int,
        temporal_dim: int,
        hidden_channels: int,
        kernel_size: int,
        dilations: Sequence[int],
        dropout: float,
    ) -> None:
        super().__init__()
        self.num_features = int(num_features)
        self.hidden_channels = int(hidden_channels)
        self.in_proj = nn.Linear(self.num_features, self.hidden_channels)
        self.blocks = nn.Sequential(
            *[
                CausalDepthwiseSeparableTCNBlock(
                    dim=self.hidden_channels,
                    kernel_size=kernel_size,
                    dilation=int(dilation),
                    dropout=dropout,
                )
                for dilation in dilations
            ]
        )
        self.out_proj = (
            nn.Identity()
            if int(temporal_dim) == self.hidden_channels
            else nn.Linear(self.hidden_channels, int(temporal_dim))
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, n_features = x.shape
        if int(n_features) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(n_features)}")
        h = x.permute(0, 2, 1, 3).contiguous().reshape(bsz * n_symbols, steps, n_features)
        h = self.in_proj(h)
        h = h.transpose(1, 2).contiguous()
        h = self.blocks(h)
        h_last = h[:, :, -1]
        z_time = self.out_proj(h_last).reshape(bsz, n_symbols, -1)
        return z_time


class TabularResBlock(nn.Module):
    """Feature-interaction residual block over latest-day per-stock features."""

    def __init__(self, dim: int, hidden_dim: int, dropout: float, residual_scale: float) -> None:
        super().__init__()
        self.residual_scale = float(residual_scale)
        self.norm = nn.LayerNorm(dim)
        self.fc1 = nn.Linear(dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, dim)
        self.act = nn.GELU()
        self.dropout = nn.Dropout(float(dropout))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = self.norm(x)
        y = self.fc1(y)
        y = self.act(y)
        y = self.dropout(y)
        y = self.fc2(y)
        y = self.dropout(y)
        return x + self.residual_scale * y


class TabularResNetBranch(nn.Module):
    """Latest-day tabular feature-interaction branch.

    Input:  x_last [B, S, F]
    Output: z_feat [B, S, tabular_dim]
    """

    def __init__(
        self,
        num_features: int,
        tabular_dim: int,
        hidden_dim: int,
        n_blocks: int,
        dropout: float,
        residual_scale: float,
    ) -> None:
        super().__init__()
        self.num_features = int(num_features)
        self.input_proj = nn.Sequential(
            nn.Linear(self.num_features, int(tabular_dim)),
            nn.LayerNorm(int(tabular_dim)),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )
        self.blocks = nn.Sequential(
            *[
                TabularResBlock(
                    dim=int(tabular_dim),
                    hidden_dim=int(hidden_dim),
                    dropout=dropout,
                    residual_scale=residual_scale,
                )
                for _ in range(max(1, int(n_blocks)))
            ]
        )

    def forward(self, x_last: torch.Tensor) -> torch.Tensor:
        if int(x_last.size(-1)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x_last.size(-1))}")
        z_feat = self.input_proj(x_last)
        return self.blocks(z_feat)


class MAB(nn.Module):
    """Multihead attention block with Q x K complexity, not self-attention by default."""

    def __init__(self, dim: int, num_heads: int, ffn_mult: int, dropout: float) -> None:
        super().__init__()
        self.attn = nn.MultiheadAttention(
            embed_dim=int(dim),
            num_heads=max(1, int(num_heads)),
            dropout=float(dropout),
            batch_first=True,
        )
        self.norm1 = nn.LayerNorm(int(dim))
        self.norm2 = nn.LayerNorm(int(dim))
        hidden_dim = int(dim) * max(1, int(ffn_mult))
        self.ffn = nn.Sequential(
            nn.Linear(int(dim), hidden_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(hidden_dim, int(dim)),
            nn.Dropout(float(dropout)),
        )

    def forward(
        self,
        q: torch.Tensor,
        kv: torch.Tensor,
        key_padding_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        attn_out, _ = self.attn(q, kv, kv, key_padding_mask=key_padding_mask, need_weights=False)
        q = self.norm1(q + attn_out)
        q = self.norm2(q + self.ffn(q))
        return q


class LiteISAB(nn.Module):
    """Inducing-token set block with O(B*S*M*D), never O(S^2)."""

    def __init__(
        self,
        dim: int,
        num_heads: int,
        num_inducing_points: int,
        ffn_mult: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.inducing = nn.Parameter(torch.randn(1, int(num_inducing_points), int(dim)) * 0.02)
        self.inducing_reads_stocks = MAB(dim, num_heads, ffn_mult, dropout)
        self.stocks_read_inducing = MAB(dim, num_heads, ffn_mult, dropout)

    def forward(self, x: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
        bsz, n_symbols, _ = x.shape
        if mask is None:
            safe_mask = torch.ones(bsz, n_symbols, dtype=torch.bool, device=x.device)
        else:
            safe_mask = mask.to(device=x.device, dtype=torch.bool)
            torch._assert(
                safe_mask.any(dim=1).all(),
                "tradable mask contains an all-false row; no-fallback path requires at least one tradable symbol per row",
            )

        x_masked = x.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        inducing = self.inducing.expand(bsz, -1, -1)
        market_tokens = self.inducing_reads_stocks(
            inducing,
            x_masked,
            key_padding_mask=~safe_mask,
        )
        z_set = self.stocks_read_inducing(x, market_tokens, key_padding_mask=None)
        return z_set


class EfficientTCNTabularSetPortfolioModel(nn.Module):
    """Low-complexity portfolio model.

    x:    [B, L, S, F]
    mask: [B, S], True means tradable.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        temporal_enabled: bool = True,
        temporal_dim: int = 16,
        temporal_hidden_channels: int = 32,
        temporal_dilations: Sequence[int] = (1, 2),
        temporal_kernel_size: int = 3,
        tabular_dim: int = 64,
        tabular_hidden_dim: int = 128,
        tabular_blocks: int = 2,
        model_dim: int = 64,
        set_enabled: bool = True,
        num_inducing_points: int = 16,
        num_heads: int = 4,
        ffn_mult: int = 2,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        residual_scale: float = 0.5,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_only",
        return_aux: bool = True,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.temporal_enabled = bool(temporal_enabled)
        self.set_enabled = bool(set_enabled)
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = self._normalize_portfolio_mode(portfolio_mode)
        self.return_aux = bool(return_aux)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)

        if self.temporal_enabled:
            self.temporal_branch = TemporalTCNBranch(
                num_features=self.num_features,
                temporal_dim=int(temporal_dim),
                hidden_channels=int(temporal_hidden_channels),
                kernel_size=int(temporal_kernel_size),
                dilations=tuple(int(d) for d in temporal_dilations),
                dropout=float(dropout),
            )
            fusion_in_dim = int(temporal_dim) + int(tabular_dim)
        else:
            self.temporal_branch = None
            fusion_in_dim = int(tabular_dim)

        self.tabular_branch = TabularResNetBranch(
            num_features=self.num_features,
            tabular_dim=int(tabular_dim),
            hidden_dim=int(tabular_hidden_dim),
            n_blocks=int(tabular_blocks),
            dropout=float(dropout),
            residual_scale=float(residual_scale),
        )
        self.fusion = nn.Sequential(
            nn.Linear(fusion_in_dim, int(model_dim)),
            nn.LayerNorm(int(model_dim)),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )
        self.set_branch = (
            LiteISAB(
                dim=int(model_dim),
                num_heads=int(num_heads),
                num_inducing_points=int(num_inducing_points),
                ffn_mult=int(ffn_mult),
                dropout=float(dropout),
            )
            if self.set_enabled
            else None
        )

        head: list[nn.Module] = []
        in_dim = int(model_dim)
        for _ in range(max(0, int(head_layers))):
            head.extend(
                [
                    nn.Linear(in_dim, int(head_hidden_dim)),
                    nn.GELU(),
                    nn.Dropout(float(dropout)),
                ]
            )
            in_dim = int(head_hidden_dim)
        head.append(nn.Linear(in_dim, 1))
        self.score_head = nn.Sequential(*head)

    @staticmethod
    def _normalize_portfolio_mode(portfolio_mode: str) -> str:
        normalized = str(portfolio_mode).strip().lower().replace("-", "_")
        if normalized in {"long", "long_only", "longonly"}:
            return "long_only"
        if normalized in {"long_short", "longshort", "short", "dual_branch", "long_and_short"}:
            return "long_short"
        raise ValueError(
            "EfficientTCNTabularSetPortfolioModel portfolio_mode must be "
            "'long_only' or 'long_short'"
        )

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

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        self._check_shapes(x, mask)
        if mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = mask.to(device=x.device, dtype=torch.bool)

        # Latest-day feature interaction only: z_feat [B, S, tabular_dim].
        z_feat = self.tabular_branch(x[:, -1, :, :])

        if self.temporal_enabled:
            if self.temporal_branch is None:
                raise RuntimeError("temporal_branch is unexpectedly None")
            # Per-stock temporal summary only: z_time [B, S, temporal_dim].
            z_time = self.temporal_branch(x)
            z = torch.cat([z_time, z_feat], dim=-1)
        else:
            z_time = None
            z = z_feat

        z_fused = self.fusion(z)
        z_set = self.set_branch(z_fused, mask_bool) if self.set_branch is not None else z_fused
        scores = self.score_head(z_set).squeeze(-1)
        masked_scores = scores.masked_fill(~mask_bool, finite_mask_fill_value(scores))

        if temperature is None:
            temp = masked_scores.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=masked_scores.device, dtype=masked_scores.dtype)
        else:
            temp = masked_scores.new_tensor(float(temperature))
        temp = torch.clamp(temp, min=0.05)
        if self.portfolio_mode == "long_only":
            weights = masked_softmax(masked_scores / temp, mask_bool)
        else:
            relative_scores = scores - masked_cross_sectional_mean(scores, mask_bool)
            weights = dual_branch_softmax(relative_scores / temp, mask_bool)

        aux = {
            "z_time": z_time,
            "z_feat": z_feat,
            "z_fused": z_fused,
            "z_set": z_set,
        }
        if return_aux is True:
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            return {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
                "aux": aux,
                **aux,
            }
        return weights
