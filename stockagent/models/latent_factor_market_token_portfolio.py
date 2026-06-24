from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn

from stockagent.models.efficient_tcn_tabular_set_portfolio import MAB, TabularResNetBranch, TemporalTCNBranch
from stockagent.models.normalization import (
    dual_branch_softmax,
    finite_mask_fill_value,
    masked_cross_sectional_mean,
    masked_softmax,
    normalize_portfolio_activation,
)


def _safe_attention_mask(mask: torch.Tensor) -> torch.Tensor:
    safe_mask = mask.to(dtype=torch.bool)
    torch._assert(
        safe_mask.any(dim=1).all(),
        "tradable mask contains an all-false row; no-fallback path requires at least one tradable symbol per row",
    )
    return safe_mask


class LatentFactorMarketTokenPortfolioModel(nn.Module):
    """Low-complexity cross-stock portfolio model.

    Cross-stock path:
      stocks -> K latent factors -> M market tokens -> stock portfolio head.

    No stock-to-stock self-attention is used. The cross-stock attention cost is
    O(S*K + K*M + S*M), where S is symbols, K is latent factors, and M is market
    tokens.
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
        stock_embedding_dim: int = 64,
        num_latent_factors: int = 32,
        num_market_tokens: int = 4,
        num_heads: int = 4,
        ffn_mult: int = 2,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        residual_scale: float = 0.5,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_only",
        portfolio_activation: str = "gd",
        return_aux: bool = True,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.temporal_enabled = bool(temporal_enabled)
        self.stock_embedding_dim = int(stock_embedding_dim)
        self.num_latent_factors = max(1, int(num_latent_factors))
        self.num_market_tokens = max(1, int(num_market_tokens))
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = self._normalize_portfolio_mode(portfolio_mode)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
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
            stock_encoder_in_dim = int(temporal_dim) + int(tabular_dim)
        else:
            self.temporal_branch = None
            stock_encoder_in_dim = int(tabular_dim)

        self.tabular_branch = TabularResNetBranch(
            num_features=self.num_features,
            tabular_dim=int(tabular_dim),
            hidden_dim=int(tabular_hidden_dim),
            n_blocks=int(tabular_blocks),
            dropout=float(dropout),
            residual_scale=float(residual_scale),
        )
        self.stock_fusion = nn.Sequential(
            nn.Linear(stock_encoder_in_dim, self.stock_embedding_dim),
            nn.LayerNorm(self.stock_embedding_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )

        self.latent_factor_queries = nn.Parameter(
            torch.randn(1, self.num_latent_factors, self.stock_embedding_dim) * 0.02
        )
        self.market_token_queries = nn.Parameter(
            torch.randn(1, self.num_market_tokens, self.stock_embedding_dim) * 0.02
        )
        self.stocks_to_factors = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(num_heads),
            ffn_mult=int(ffn_mult),
            dropout=float(dropout),
        )
        self.factors_to_market = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(num_heads),
            ffn_mult=int(ffn_mult),
            dropout=float(dropout),
        )
        self.stocks_read_factors = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(num_heads),
            ffn_mult=int(ffn_mult),
            dropout=float(dropout),
        )
        self.stocks_read_market = MAB(
            dim=self.stock_embedding_dim,
            num_heads=int(num_heads),
            ffn_mult=int(ffn_mult),
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
            "LatentFactorMarketTokenPortfolioModel portfolio_mode must be "
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

    def _encode_stocks(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor | None, torch.Tensor]:
        z_feat = self.tabular_branch(x[:, -1, :, :])
        if self.temporal_enabled:
            if self.temporal_branch is None:
                raise RuntimeError("temporal_branch is unexpectedly None")
            z_time = self.temporal_branch(x)
            z = torch.cat([z_time, z_feat], dim=-1)
        else:
            z_time = None
            z = z_feat
        return self.stock_fusion(z), z_time, z_feat

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

        z_stock, z_time, z_feat = self._encode_stocks(x)
        z_stock = z_stock.masked_fill(~valid, 0.0)

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
            weights = masked_softmax(masked_scores / temp, mask_bool, activation=self.portfolio_activation)
        else:
            relative_scores = scores - masked_cross_sectional_mean(scores, mask_bool)
            weights = dual_branch_softmax(relative_scores / temp, mask_bool, activation=self.portfolio_activation)
        weights = weights.masked_fill(~mask_bool, 0.0)

        aux = {
            "z_time": z_time,
            "z_feat": z_feat,
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
            return {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
                "aux": aux,
                **aux,
            }
        return weights
