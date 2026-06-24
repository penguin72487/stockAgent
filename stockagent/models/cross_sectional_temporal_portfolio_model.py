from __future__ import annotations

import os

import torch
from torch import nn

from stockagent.models.normalization import masked_activation_l1_weights, normalize_portfolio_activation


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


def _scatter_candidates(
    values: torch.Tensor,
    indices: torch.Tensor,
    n_symbols: int,
    fill_value: float = 0.0,
) -> torch.Tensor:
    out = values.new_full((values.size(0), n_symbols), fill_value)
    return out.scatter(1, indices, values)


class CrossSectionalTemporalPortfolioModel(nn.Module):
    """Stock-wise scorer -> hard candidates -> cross-asset reranker -> sparse top-k portfolio."""

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        stock_embedding_dim: int,
        stock_hidden_dim: int,
        stock_n_blocks: int,
        temporal_hidden_dim: int,
        temporal_blocks: int,
        temporal_kernel_size: int,
        cross_hidden_dim: int,
        cross_heads: int,
        cross_layers: int,
        dropout: float,
        regime_classes: int = 3,
        long_only: bool = True,
        portfolio_activation: str = "gd",
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
        candidate_top_m: int = 128,
        portfolio_top_k: int = 10,
    ) -> None:
        super().__init__()
        _ = temporal_hidden_dim
        _ = temporal_blocks
        _ = temporal_kernel_size

        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.regime_classes = max(2, int(regime_classes))
        self.candidate_top_m = max(1, int(candidate_top_m))
        self.portfolio_top_k = max(1, int(portfolio_top_k))

        input_dim = self.lookback * self.num_features
        scorer_hidden = int(stock_hidden_dim)
        scorer_dim = int(stock_embedding_dim)
        rerank_dim = int(cross_hidden_dim)

        self.stock_scorer = nn.Sequential(
            nn.Linear(input_dim, scorer_hidden),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            *[_FeatureResBlock(scorer_hidden, float(dropout)) for _ in range(max(1, int(stock_n_blocks)))],
            nn.LayerNorm(scorer_hidden),
            nn.Linear(scorer_hidden, scorer_dim),
            nn.GELU(),
        )
        self.coarse_score_head = nn.Sequential(
            nn.LayerNorm(scorer_dim),
            nn.Linear(scorer_dim, 1),
        )

        self.reranker_input_proj = nn.Linear(scorer_dim + 1, rerank_dim)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=rerank_dim,
            nhead=max(1, int(cross_heads)),
            dim_feedforward=rerank_dim * 4,
            dropout=float(dropout),
            activation="gelu",
            batch_first=True,
            norm_first=False,
        )
        self.reranker = nn.TransformerEncoder(
            encoder_layer,
            num_layers=max(1, int(cross_layers)),
            norm=nn.LayerNorm(rerank_dim),
        )

        self.score_head = nn.Sequential(
            nn.LayerNorm(rerank_dim),
            nn.Linear(rerank_dim, 1),
        )
        self.return_rank_head = nn.Sequential(
            nn.LayerNorm(rerank_dim),
            nn.Linear(rerank_dim, 1),
        )
        self.volatility_head = nn.Sequential(
            nn.LayerNorm(rerank_dim),
            nn.Linear(rerank_dim, 1),
        )
        self.regime_head = nn.Sequential(
            nn.LayerNorm(rerank_dim),
            nn.Linear(rerank_dim, self.regime_classes),
        )
        self.portfolio_head = nn.Sequential(
            nn.LayerNorm(rerank_dim + 3),
            nn.Linear(rerank_dim + 3, rerank_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(rerank_dim, 1),
        )

    def _select_candidates(
        self,
        coarse_scores: torch.Tensor,
        tradable_mask: torch.Tensor | None,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        n_symbols = int(coarse_scores.size(1))
        top_m = min(self.candidate_top_m, n_symbols)
        if tradable_mask is None:
            selection_scores = coarse_scores
            valid_mask = torch.ones_like(coarse_scores, dtype=torch.bool)
        else:
            valid_mask = tradable_mask.to(dtype=torch.bool, device=coarse_scores.device)
            selection_scores = coarse_scores.masked_fill(~valid_mask, torch.finfo(coarse_scores.dtype).min)
        if self.long_only:
            candidate_indices = torch.topk(selection_scores, k=top_m, dim=1).indices
        else:
            long_k = max(1, top_m // 2)
            short_k = max(1, top_m - long_k)
            long_indices = torch.topk(selection_scores, k=long_k, dim=1).indices
            short_scores = coarse_scores.masked_fill(~valid_mask, torch.finfo(coarse_scores.dtype).max)
            short_indices = torch.topk(-short_scores, k=short_k, dim=1).indices
            candidate_indices = torch.cat([long_indices, short_indices], dim=1)
        candidate_mask = valid_mask.gather(1, candidate_indices)
        return candidate_indices, candidate_mask

    def _sparse_long_only_weights(
        self,
        portfolio_logits: torch.Tensor,
        candidate_indices: torch.Tensor,
        candidate_mask: torch.Tensor,
        n_symbols: int,
    ) -> torch.Tensor:
        k = min(self.portfolio_top_k, int(portfolio_logits.size(1)))
        masked_logits = portfolio_logits.masked_fill(~candidate_mask, torch.finfo(portfolio_logits.dtype).min)
        topk_indices_in_candidates = torch.topk(masked_logits, k=k, dim=1).indices
        topk_mask = torch.zeros_like(candidate_mask)
        topk_mask = topk_mask.scatter(1, topk_indices_in_candidates, True) & candidate_mask
        candidate_weights = masked_activation_l1_weights(
            portfolio_logits,
            topk_mask,
            long_only=True,
            activation=self.portfolio_activation,
        )
        return _scatter_candidates(candidate_weights, candidate_indices, n_symbols)

    def _sparse_long_short_weights(
        self,
        portfolio_logits: torch.Tensor,
        candidate_indices: torch.Tensor,
        candidate_mask: torch.Tensor,
        n_symbols: int,
    ) -> torch.Tensor:
        k = min(self.portfolio_top_k, int(portfolio_logits.size(1)))

        long_scores = portfolio_logits.masked_fill(~candidate_mask, torch.finfo(portfolio_logits.dtype).min)
        long_indices = torch.topk(long_scores, k=k, dim=1).indices
        long_mask = torch.zeros_like(candidate_mask).scatter(1, long_indices, True) & candidate_mask

        short_scores = (-portfolio_logits).masked_fill(~candidate_mask, torch.finfo(portfolio_logits.dtype).min)
        short_indices = torch.topk(short_scores, k=k, dim=1).indices
        short_mask = torch.zeros_like(candidate_mask).scatter(1, short_indices, True) & candidate_mask & ~long_mask

        candidate_weights = masked_activation_l1_weights(
            portfolio_logits,
            long_mask | short_mask,
            long_only=False,
            activation=self.portfolio_activation,
        )
        return _scatter_candidates(candidate_weights, candidate_indices, n_symbols)

    def forward(
        self,
        x: torch.Tensor,
        tradable_mask: torch.Tensor | None = None,
    ) -> dict[str, torch.Tensor]:
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
        x_flat = (
            torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
            .permute(0, 2, 1, 3)
            .reshape(bsz * n_symbols, self.lookback * self.num_features)
        )
        stock_features = self.stock_scorer(x_flat).reshape(bsz, n_symbols, -1)
        stock_features = torch.nan_to_num(stock_features, nan=0.0, posinf=0.0, neginf=0.0)
        coarse_scores = self.coarse_score_head(stock_features).squeeze(-1)
        coarse_scores = torch.nan_to_num(coarse_scores, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)

        candidate_indices, candidate_mask = self._select_candidates(coarse_scores, tradable_mask)
        safe_candidate_mask = candidate_mask.clone()
        empty_rows = ~safe_candidate_mask.any(dim=1)
        if empty_rows.any():
            safe_candidate_mask[empty_rows, 0] = True
        gather_index = candidate_indices.unsqueeze(-1).expand(-1, -1, stock_features.size(-1))
        candidate_features = stock_features.gather(1, gather_index)
        candidate_coarse_scores = coarse_scores.gather(1, candidate_indices).unsqueeze(-1)
        reranker_x = self.reranker_input_proj(torch.cat([candidate_features, candidate_coarse_scores], dim=-1))
        reranker_x = torch.nan_to_num(reranker_x, nan=0.0, posinf=0.0, neginf=0.0)
        reranked = self.reranker(reranker_x, src_key_padding_mask=~safe_candidate_mask)
        reranked = torch.nan_to_num(reranked, nan=0.0, posinf=0.0, neginf=0.0)

        candidate_score_logits = self.score_head(reranked).squeeze(-1)
        candidate_rank_logits = self.return_rank_head(reranked).squeeze(-1)
        candidate_volatility = nn.functional.softplus(self.volatility_head(reranked).squeeze(-1))
        portfolio_input = torch.cat(
            [
                reranked,
                candidate_score_logits.unsqueeze(-1),
                candidate_rank_logits.unsqueeze(-1),
                candidate_volatility.unsqueeze(-1),
            ],
            dim=-1,
        )
        candidate_portfolio_logits = self.portfolio_head(portfolio_input).squeeze(-1)
        candidate_score_logits = torch.nan_to_num(candidate_score_logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        candidate_rank_logits = torch.nan_to_num(candidate_rank_logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        candidate_volatility = torch.nan_to_num(candidate_volatility, nan=0.0, posinf=20.0, neginf=0.0).clamp(min=0.0, max=20.0)
        candidate_portfolio_logits = torch.nan_to_num(candidate_portfolio_logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)

        score_logits = coarse_scores.clone().scatter(1, candidate_indices, candidate_score_logits)
        rank_logits = coarse_scores.clone().scatter(1, candidate_indices, candidate_rank_logits)
        volatility_pred = _scatter_candidates(candidate_volatility, candidate_indices, n_symbols)
        if self.long_only:
            weights = self._sparse_long_only_weights(candidate_portfolio_logits, candidate_indices, candidate_mask, n_symbols)
        else:
            weights = self._sparse_long_short_weights(candidate_portfolio_logits, candidate_indices, candidate_mask, n_symbols)

        candidate_mask_f = candidate_mask.to(dtype=reranked.dtype).unsqueeze(-1)
        pooled = (reranked * candidate_mask_f).sum(dim=1) / candidate_mask_f.sum(dim=1).clamp_min(1.0)
        regime_logits = self.regime_head(pooled)

        return {
            "weights": torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0),
            "score_logits": score_logits,
            "rank_logits": rank_logits,
            "volatility_pred": volatility_pred,
            "regime_logits": torch.nan_to_num(regime_logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0),
        }
