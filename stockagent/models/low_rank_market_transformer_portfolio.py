from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.models.efficient_tcn_tabular_set_portfolio import MAB
from stockagent.models.latent_factor_market_token_portfolio import _safe_attention_mask
from stockagent.models.normalization import (
    dual_branch_softmax,
    finite_mask_fill_value,
    masked_cross_sectional_mean,
    masked_softmax,
)


class TemporalSelfAttentionBlock(nn.Module):
    """Temporal Transformer block that avoids CUDA SDPA's huge flattened batch path."""

    def __init__(self, dim: int, num_heads: int, ffn_dim: int, dropout: float) -> None:
        super().__init__()
        self.dim = int(dim)
        self.num_heads = max(1, int(num_heads))
        if self.dim % self.num_heads != 0:
            raise ValueError("dim must be divisible by num_heads")
        self.head_dim = self.dim // self.num_heads
        self.scale = float(self.head_dim) ** -0.5

        self.norm1 = nn.LayerNorm(self.dim)
        self.qkv = nn.Linear(self.dim, self.dim * 3)
        self.out_proj = nn.Linear(self.dim, self.dim)
        self.attn_dropout = nn.Dropout(float(dropout))
        self.resid_dropout = nn.Dropout(float(dropout))

        self.norm2 = nn.LayerNorm(self.dim)
        hidden_dim = max(self.dim, int(ffn_dim))
        self.ffn = nn.Sequential(
            nn.Linear(self.dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(hidden_dim, self.dim),
            nn.Dropout(float(dropout)),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        bsz, n_symbols, steps, dim = x.shape
        y = self.norm1(x)
        qkv = self.qkv(y).reshape(
            bsz,
            n_symbols,
            steps,
            3,
            self.num_heads,
            self.head_dim,
        )
        q, k, v = qkv.permute(3, 0, 1, 4, 2, 5).unbind(dim=0)
        scores = torch.matmul(q, k.transpose(-2, -1)) * self.scale
        attn = torch.softmax(scores, dim=-1)
        attn = self.attn_dropout(attn)
        context = torch.matmul(attn, v)
        context = context.permute(0, 1, 3, 2, 4).contiguous().reshape(bsz, n_symbols, steps, dim)
        x = x + self.resid_dropout(self.out_proj(context))
        x = x + self.ffn(self.norm2(x))
        return x


class TemporalDepthwiseConvBlock(nn.Module):
    """Tensor-friendly temporal mixer with O(L*k*D) per stock cost."""

    def __init__(self, dim: int, ffn_dim: int, kernel_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        self.dim = int(dim)
        kernel = max(1, int(kernel_size))
        if kernel % 2 == 0:
            kernel += 1
        self.kernel_size = kernel
        self.dilation = max(1, int(dilation))

        self.norm1 = nn.LayerNorm(self.dim)
        self.depthwise = nn.Conv1d(
            self.dim,
            self.dim,
            kernel_size=self.kernel_size,
            padding=(self.kernel_size // 2) * self.dilation,
            dilation=self.dilation,
            groups=self.dim,
        )
        self.pointwise = nn.Conv1d(self.dim, self.dim, kernel_size=1)
        self.dropout = nn.Dropout(float(dropout))

        self.norm2 = nn.LayerNorm(self.dim)
        hidden_dim = max(self.dim, int(ffn_dim))
        self.ffn = nn.Sequential(
            nn.Linear(self.dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(hidden_dim, self.dim),
            nn.Dropout(float(dropout)),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        bsz, n_symbols, steps, dim = x.shape
        y = self.norm1(x).reshape(bsz * n_symbols, steps, dim).transpose(1, 2).contiguous()
        y = self.pointwise(self.depthwise(y))
        y = y.transpose(1, 2).reshape(bsz, n_symbols, steps, dim)
        x = x + self.dropout(y)
        x = x + self.ffn(self.norm2(x))
        return x


class PerStockTemporalConvEncoder(nn.Module):
    """Shared depthwise-conv temporal encoder for longer lookback windows."""

    def __init__(
        self,
        *,
        lookback: int,
        num_features: int,
        feature_dim: int,
        temporal_layers: int,
        temporal_ffn_dim: int,
        temporal_dropout: float,
        temporal_pooling: str,
        temporal_kernel_size: int,
        temporal_dilations: Sequence[int] = (1,),
        checkpoint_blocks: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.feature_dim = int(feature_dim)
        self.temporal_pooling = PerStockTemporalTransformerEncoder._normalize_pooling(temporal_pooling)
        self.checkpoint_blocks = bool(checkpoint_blocks)
        n_layers = max(1, int(temporal_layers))
        dilations = tuple(max(1, int(dilation)) for dilation in temporal_dilations)
        if not dilations:
            dilations = (1,)
        self.temporal_dilations = tuple(
            dilations[idx] if idx < len(dilations) else dilations[-1]
            for idx in range(n_layers)
        )

        self.feature_proj = nn.Linear(self.num_features, self.feature_dim)
        self.position = nn.Parameter(torch.randn(1, 1, self.lookback, self.feature_dim) * 0.02)
        self.blocks = nn.ModuleList(
            [
                TemporalDepthwiseConvBlock(
                    dim=self.feature_dim,
                    ffn_dim=max(self.feature_dim, int(temporal_ffn_dim)),
                    kernel_size=int(temporal_kernel_size),
                    dilation=self.temporal_dilations[idx],
                    dropout=float(temporal_dropout),
                )
                for idx in range(n_layers)
            ]
        )
        self.output_norm = nn.LayerNorm(self.feature_dim)
        self.pool_score = nn.Linear(self.feature_dim, 1) if self.temporal_pooling == "attention" else None

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, n_features = x.shape
        if int(steps) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(steps)}")
        if int(n_features) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(n_features)}")

        h = x.permute(0, 2, 1, 3).contiguous()
        h = self.feature_proj(h)
        h = h + self.position[:, :, :steps, :]
        for block in self.blocks:
            if self.checkpoint_blocks and self.training and torch.is_grad_enabled():
                h = activation_checkpoint(block, h, use_reentrant=False)
            else:
                h = block(h)
        if self.temporal_pooling == "last":
            pooled = h[:, :, -1, :]
        elif self.temporal_pooling == "mean":
            pooled = h.mean(dim=2)
        else:
            if self.pool_score is None:
                raise RuntimeError("pool_score is unexpectedly None")
            weights = torch.softmax(self.pool_score(h).squeeze(-1), dim=2)
            pooled = (h * weights.unsqueeze(-1)).sum(dim=2)
        pooled = self.output_norm(pooled)
        return pooled


class PerStockTemporalTransformerEncoder(nn.Module):
    """Shared temporal Transformer over each stock's lookback window.

    Input:  x [B, L, S, F]
    Output: z_time [B, S, D]

    Attention is only over L days inside the same stock, so this stage costs
    O(B*S*L^2*D), not O((S*L)^2).
    """

    def __init__(
        self,
        *,
        lookback: int,
        num_features: int,
        feature_dim: int,
        temporal_layers: int,
        temporal_heads: int,
        temporal_ffn_dim: int,
        temporal_dropout: float,
        temporal_pooling: str,
        checkpoint_blocks: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.feature_dim = int(feature_dim)
        self.temporal_pooling = self._normalize_pooling(temporal_pooling)
        self.checkpoint_blocks = bool(checkpoint_blocks)
        if self.feature_dim % max(1, int(temporal_heads)) != 0:
            raise ValueError("feature_dim must be divisible by temporal_heads")

        self.feature_proj = nn.Linear(self.num_features, self.feature_dim)
        self.position = nn.Parameter(torch.randn(1, 1, self.lookback, self.feature_dim) * 0.02)
        self.blocks = nn.ModuleList(
            [
                TemporalSelfAttentionBlock(
                    dim=self.feature_dim,
                    num_heads=max(1, int(temporal_heads)),
                    ffn_dim=max(self.feature_dim, int(temporal_ffn_dim)),
                    dropout=float(temporal_dropout),
                )
                for _ in range(max(1, int(temporal_layers)))
            ]
        )
        self.output_norm = nn.LayerNorm(self.feature_dim)
        self.pool_score = nn.Linear(self.feature_dim, 1) if self.temporal_pooling == "attention" else None

    @staticmethod
    def _normalize_pooling(pooling: str) -> str:
        normalized = str(pooling).strip().lower().replace("-", "_")
        if normalized in {"last", "mean", "attention", "attn"}:
            return "attention" if normalized == "attn" else normalized
        raise ValueError("temporal_pooling must be one of: last, mean, attention")

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, n_features = x.shape
        if int(steps) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(steps)}")
        if int(n_features) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(n_features)}")

        h = x.permute(0, 2, 1, 3).contiguous()
        h = self.feature_proj(h)
        h = h + self.position[:, :, :steps, :]
        for block in self.blocks:
            if self.checkpoint_blocks and self.training and torch.is_grad_enabled():
                h = activation_checkpoint(block, h, use_reentrant=False)
            else:
                h = block(h)
        if self.temporal_pooling == "last":
            pooled = h[:, :, -1, :]
        elif self.temporal_pooling == "mean":
            pooled = h.mean(dim=2)
        else:
            if self.pool_score is None:
                raise RuntimeError("pool_score is unexpectedly None")
            weights = torch.softmax(self.pool_score(h).squeeze(-1), dim=2)
            pooled = (h * weights.unsqueeze(-1)).sum(dim=2)
        pooled = self.output_norm(pooled)
        return pooled


class LowRankMarketTransformerPortfolioModel(nn.Module):
    """Low-complexity multi-stock, multi-day Transformer portfolio model.

    Data path:
      OHLCV -> per-stock temporal Transformer -> stock embeddings
      -> K latent factors -> M market tokens -> portfolio head.

    The only full attention is within each stock's short lookback window. Cross
    stock interaction is bottlenecked through K factors and M market tokens.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        feature_dim: int = 64,
        temporal_mixer: str = "conv",
        temporal_layers: int = 2,
        temporal_heads: int = 4,
        temporal_ffn_dim: int = 128,
        temporal_dropout: float = 0.1,
        temporal_pooling: str = "last",
        temporal_kernel_size: int = 5,
        temporal_dilations: Sequence[int] = (1,),
        temporal_checkpoint: bool = True,
        stock_embedding_dim: int = 64,
        num_latent_factors: int = 32,
        num_market_tokens: int = 4,
        cross_heads: int = 4,
        cross_ffn_mult: int = 2,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_only",
        return_aux: bool = True,
        return_aux_details: bool = False,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.feature_dim = int(feature_dim)
        self.temporal_mixer = self._normalize_temporal_mixer(temporal_mixer)
        self.stock_embedding_dim = int(stock_embedding_dim)
        self.num_latent_factors = max(1, int(num_latent_factors))
        self.num_market_tokens = max(1, int(num_market_tokens))
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = self._normalize_portfolio_mode(portfolio_mode)
        self.return_aux = bool(return_aux)
        self.return_aux_details = bool(return_aux_details)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)

        if self.stock_embedding_dim % max(1, int(cross_heads)) != 0:
            raise ValueError("stock_embedding_dim must be divisible by cross_heads")

        if self.temporal_mixer == "attention":
            self.temporal_encoder = PerStockTemporalTransformerEncoder(
                lookback=self.lookback,
                num_features=self.num_features,
                feature_dim=self.feature_dim,
                temporal_layers=int(temporal_layers),
                temporal_heads=int(temporal_heads),
                temporal_ffn_dim=int(temporal_ffn_dim),
                temporal_dropout=float(temporal_dropout),
                temporal_pooling=temporal_pooling,
                checkpoint_blocks=bool(temporal_checkpoint),
            )
        else:
            self.temporal_encoder = PerStockTemporalConvEncoder(
                lookback=self.lookback,
                num_features=self.num_features,
                feature_dim=self.feature_dim,
                temporal_layers=int(temporal_layers),
                temporal_ffn_dim=int(temporal_ffn_dim),
                temporal_dropout=float(temporal_dropout),
                temporal_pooling=temporal_pooling,
                temporal_kernel_size=int(temporal_kernel_size),
                temporal_dilations=tuple(int(dilation) for dilation in temporal_dilations),
                checkpoint_blocks=bool(temporal_checkpoint),
            )
        stock_proj: list[nn.Module] = []
        if self.feature_dim == self.stock_embedding_dim:
            stock_proj.append(nn.Identity())
        else:
            stock_proj.append(nn.Linear(self.feature_dim, self.stock_embedding_dim))
        stock_proj.extend(
            [
                nn.LayerNorm(self.stock_embedding_dim),
                nn.GELU(),
                nn.Dropout(float(dropout)),
            ]
        )
        self.stock_embedding = nn.Sequential(*stock_proj)

        self.latent_factor_queries = nn.Parameter(
            torch.randn(1, self.num_latent_factors, self.stock_embedding_dim) * 0.02
        )
        self.market_token_queries = nn.Parameter(
            torch.randn(1, self.num_market_tokens, self.stock_embedding_dim) * 0.02
        )
        self.stocks_to_factors = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(cross_heads),
            ffn_mult=int(cross_ffn_mult),
            dropout=float(dropout),
        )
        self.factors_to_market = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(cross_heads),
            ffn_mult=int(cross_ffn_mult),
            dropout=float(dropout),
        )
        self.stocks_read_factors = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(cross_heads),
            ffn_mult=int(cross_ffn_mult),
            dropout=float(dropout),
        )
        self.stocks_read_market = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(cross_heads),
            ffn_mult=int(cross_ffn_mult),
            dropout=float(dropout),
        )

        self.portfolio_fusion = nn.Sequential(
            nn.Linear(self.stock_embedding_dim * 3, self.stock_embedding_dim),
            nn.LayerNorm(self.stock_embedding_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )
        head: list[nn.Module] = []
        in_dim = self.stock_embedding_dim
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
            "LowRankMarketTransformerPortfolioModel portfolio_mode must be "
            "'long_only' or 'long_short'"
        )

    @staticmethod
    def _normalize_temporal_mixer(temporal_mixer: str) -> str:
        normalized = str(temporal_mixer).strip().lower().replace("-", "_")
        if normalized in {"attention", "attn", "transformer", "self_attention"}:
            return "attention"
        if normalized in {"conv", "convolution", "depthwise_conv", "depthwise", "tcn"}:
            return "conv"
        raise ValueError(
            "LowRankMarketTransformerPortfolioModel temporal_mixer must be "
            "'attention' or 'conv'"
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
        valid = mask_bool.unsqueeze(-1)
        safe_mask = _safe_attention_mask(mask_bool)

        z_time = self.temporal_encoder(x)
        z_stock = self.stock_embedding(z_time).masked_fill(~valid, 0.0)

        bsz = int(x.size(0))
        latent_queries = self.latent_factor_queries.expand(bsz, -1, -1)
        market_queries = self.market_token_queries.expand(bsz, -1, -1)

        latent_factors = self.stocks_to_factors(
            latent_queries,
            z_stock,
            key_padding_mask=~safe_mask,
        )
        market_tokens = self.factors_to_market(market_queries, latent_factors, key_padding_mask=None)

        z_factor_context = self.stocks_read_factors(z_stock, latent_factors, key_padding_mask=None)
        z_factor_context = z_factor_context.masked_fill(~valid, 0.0)
        z_market_context = self.stocks_read_market(z_stock, market_tokens, key_padding_mask=None)
        z_market_context = z_market_context.masked_fill(~valid, 0.0)

        z_portfolio = self.portfolio_fusion(torch.cat([z_stock, z_factor_context, z_market_context], dim=-1))
        z_portfolio = z_portfolio.masked_fill(~valid, 0.0)
        scores = self.score_head(z_portfolio).squeeze(-1)
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
        weights = weights.masked_fill(~mask_bool, 0.0)

        aux = {
            "z_time": z_time,
            "z_stock": z_stock,
            "stock_embedding": z_stock,
            "latent_factors": latent_factors,
            "market_tokens": market_tokens,
            "z_factor_context": z_factor_context,
            "z_market_context": z_market_context,
            "z_portfolio": z_portfolio,
        }
        if return_aux is True:
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            output = {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
            }
            if self.return_aux_details:
                output["aux"] = aux
                output.update(aux)
            return output
        return weights
