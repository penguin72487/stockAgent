"""Joint stock and Taiwan-index-futures cross-sectional policy."""

from __future__ import annotations

import math
from typing import Any

import torch
from torch import nn
from torch.nn import functional as F

from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_ACTION_COUNT,
    TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
)
from stockagent.models.normalization import finite_mask_fill_value
from stockagent.models.transformer_base_portfolio import (
    PortfolioRMSNorm,
    TransformerBasePortfolioModel,
)


class CrossSectionalIndexFuturesModel(TransformerBasePortfolioModel):
    """Read stock history plus all causal futures tokens and emit 18 actions.

    With a futures context, a bidirectional market-token bottleneck mixes the
    encoded stock tokens with every available futures-root token plus the
    executable ``TX/MTX/TMF x E1..E6`` tokens.  The legacy action mode emits
    one signed capital fraction per executable slot.  The directional mode
    instead learns one signed market exposure (including zero/cash) and an
    allocation over valid slots, preventing economically redundant self-
    cancellation.  Requested gross exposure is bounded by
    ``max_abs_exposure``.  A context-free scalar
    path remains for callers that still exercise the previous forward ABI;
    the directional forward semantics are recorded in a fresh checkpoint
    contract even though they reuse the existing scalar exposure head.
    """

    def __init__(
        self,
        *args: Any,
        max_abs_exposure: float = 1.0,
        futures_head_hidden_dim: int | None = None,
        futures_context_capacity: int = 2048,
        futures_joint_market_tokens: int = 4,
        futures_action_mode: str = "independent",
        futures_exposure_activation: str = "tanh",
        futures_allocation_logit_scale: float = 0.0,
        futures_allocation_temperature: float = 1.0,
        futures_require_joint_context: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs["execution_mode"] = "naive"
        kwargs["return_aux"] = bool(kwargs.get("return_aux", False))
        super().__init__(*args, **kwargs)
        if not 0.0 < float(max_abs_exposure) <= 1.0:
            raise ValueError("max_abs_exposure must be in (0, 1]")
        self.max_abs_exposure = float(max_abs_exposure)
        normalized_action_mode = (
            str(futures_action_mode).strip().lower().replace("-", "_")
        )
        if normalized_action_mode not in {
            "independent",
            "directional_allocation",
        }:
            raise ValueError(
                "futures_action_mode must be independent or directional_allocation"
            )
        self.futures_action_mode = normalized_action_mode
        normalized_exposure_activation = (
            str(futures_exposure_activation).strip().lower().replace("-", "_")
        )
        if normalized_exposure_activation not in {"tanh", "softsign"}:
            raise ValueError(
                "futures_exposure_activation must be tanh or softsign"
            )
        allocation_logit_scale = float(futures_allocation_logit_scale)
        allocation_temperature = float(futures_allocation_temperature)
        if not math.isfinite(allocation_logit_scale) or (
            allocation_logit_scale < 0.0
        ):
            raise ValueError(
                "futures_allocation_logit_scale must be finite and non-negative"
            )
        if not math.isfinite(allocation_temperature) or (
            allocation_temperature <= 0.0
        ):
            raise ValueError(
                "futures_allocation_temperature must be finite and positive"
            )
        self.futures_exposure_activation = normalized_exposure_activation
        self.futures_allocation_logit_scale = allocation_logit_scale
        self.futures_allocation_temperature = allocation_temperature
        self.futures_require_joint_context = bool(futures_require_joint_context)
        hidden = (
            self.d_model
            if futures_head_hidden_dim is None
            else int(futures_head_hidden_dim)
        )
        if hidden < 1:
            raise ValueError("futures_head_hidden_dim must be positive")
        # The inherited score head maps every stock to a traded stock.  It is
        # intentionally removed so no dead stock-trading parameters remain.
        del self.score_head
        self.futures_pool_score = (
            None
            if self.futures_require_joint_context
            else nn.Linear(self.d_model, 1)
        )
        self.futures_head = nn.Sequential(
            nn.Linear(self.d_model, hidden),
            nn.SiLU(),
            nn.Linear(hidden, 1),
        )
        self.futures_candidate_encoder = nn.Sequential(
            nn.Linear(TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM, self.d_model),
            nn.SiLU(),
            PortfolioRMSNorm(self.d_model),
        )
        self.futures_product_embedding = nn.Embedding(
            len(TAIFEX_INDEX_FUTURES_PRODUCTS), self.d_model
        )
        self.futures_tenor_embedding = nn.Embedding(
            TAIFEX_INDEX_FUTURES_TENOR_SLOTS, self.d_model
        )
        if int(futures_context_capacity) < TAIFEX_INDEX_FUTURES_ACTION_COUNT:
            raise ValueError("futures_context_capacity must be at least 18")
        if int(futures_joint_market_tokens) < 1:
            raise ValueError("futures_joint_market_tokens must be positive")
        self.futures_context_capacity = int(futures_context_capacity)
        self.futures_context_slot_embedding = nn.Embedding(
            self.futures_context_capacity, self.d_model
        )
        self.futures_joint_market_queries = nn.Parameter(
            torch.empty(1, int(futures_joint_market_tokens), self.d_model)
        )
        nn.init.normal_(
            self.futures_joint_market_queries,
            mean=0.0,
            std=self.d_model ** -0.5,
        )
        joint_heads = min(4, self.d_model)
        while self.d_model % joint_heads != 0:
            joint_heads -= 1
        self.futures_joint_norm = PortfolioRMSNorm(self.d_model)
        self.futures_joint_attention = nn.MultiheadAttention(
            self.d_model,
            joint_heads,
            dropout=0.0,
            batch_first=True,
        )
        self.futures_joint_read_attention = nn.MultiheadAttention(
            self.d_model,
            joint_heads,
            dropout=0.0,
            batch_first=True,
        )
        self.futures_joint_post_norm = PortfolioRMSNorm(self.d_model)
        self.futures_joint_ffn = nn.Sequential(
            nn.Linear(self.d_model, self.d_model * 2),
            nn.SiLU(),
            nn.Linear(self.d_model * 2, self.d_model),
        )
        self.futures_action_head = nn.Linear(self.d_model, 1)
        self.register_buffer(
            "futures_product_indices",
            torch.arange(len(TAIFEX_INDEX_FUTURES_PRODUCTS)).repeat_interleave(
                TAIFEX_INDEX_FUTURES_TENOR_SLOTS
            ),
            persistent=False,
        )
        self.register_buffer(
            "futures_tenor_indices",
            torch.arange(TAIFEX_INDEX_FUTURES_TENOR_SLOTS).repeat(
                len(TAIFEX_INDEX_FUTURES_PRODUCTS)
            ),
            persistent=False,
        )

    def _activate_futures_exposure(
        self, exposure_logit: torch.Tensor
    ) -> torch.Tensor:
        """Map a scalar logit to the feasible signed exposure interval."""

        if self.futures_exposure_activation == "softsign":
            bounded = F.softsign(exposure_logit)
        else:
            bounded = torch.tanh(exposure_logit)
        return bounded * float(self.max_abs_exposure)

    def _allocation_logits(
        self,
        scores: torch.Tensor,
        action_mask: torch.Tensor,
    ) -> torch.Tensor:
        """Return identifiable, optionally bounded logits for contract mix.

        Softmax is invariant to a common shift, so retaining that direction in
        the head only lets weights drift without changing the policy.  Center
        valid scores first.  When a positive bound is configured, softsign
        prevents finite logits from rounding to an exact one-hot allocation
        and preserves useful contract-selection gradients.
        """

        if self.futures_allocation_logit_scale <= 0.0:
            return scores
        valid_f = action_mask.to(dtype=scores.dtype)
        valid_count = valid_f.sum(dim=-1, keepdim=True).clamp_min(1.0)
        centered = scores - (scores * valid_f).sum(
            dim=-1, keepdim=True
        ) / valid_count
        bounded = F.softsign(
            centered / float(self.futures_allocation_temperature)
        ) * float(self.futures_allocation_logit_scale)
        return torch.where(action_mask, bounded, torch.zeros_like(bounded))

    @staticmethod
    def _require_futures_context(
        context: dict[str, torch.Tensor],
        *,
        batch_size: int,
        device: torch.device,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        features = context.get("candidate_features")
        mask = context.get("candidate_mask")
        if features is None or mask is None:
            raise ValueError("futures candidate_features and candidate_mask must be paired")
        if (
            features.ndim != 3
            or int(features.size(0)) != batch_size
            or int(features.size(1)) < TAIFEX_INDEX_FUTURES_ACTION_COUNT
            or int(features.size(2)) != TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM
        ):
            raise ValueError(
                "futures candidate_features must have shape [B,K>=18,13], "
                f"got {tuple(features.shape)}"
            )
        if tuple(mask.shape) != tuple(features.shape[:2]):
            raise ValueError(
                "futures candidate_mask must match [B,K], got "
                f"{tuple(mask.shape)}"
            )
        return features.to(device=device), mask.to(device=device, dtype=torch.bool)

    def _joint_futures_actions(
        self,
        z_stock: torch.Tensor,
        stock_mask: torch.Tensor,
        aux: dict[str, torch.Tensor],
        *,
        return_aux: bool | None,
        return_scores: bool,
        portfolio_context: dict[str, torch.Tensor],
    ):
        candidate_features, candidate_mask = self._require_futures_context(
            portfolio_context,
            batch_size=int(z_stock.size(0)),
            device=z_stock.device,
        )
        futures_tokens = self.futures_candidate_encoder(
            candidate_features.to(dtype=z_stock.dtype)
        )
        context_width = int(futures_tokens.size(1))
        if context_width > self.futures_context_capacity:
            raise ValueError(
                "futures context exceeds configured capacity: "
                f"{context_width} > {self.futures_context_capacity}"
            )
        context_indices = torch.arange(context_width, device=z_stock.device)
        futures_tokens = futures_tokens + self.futures_context_slot_embedding(
            context_indices
        )[None, :, :]
        action_tokens = (
            futures_tokens[:, -TAIFEX_INDEX_FUTURES_ACTION_COUNT:, :]
            + self.futures_product_embedding(self.futures_product_indices)[None, :, :]
            + self.futures_tenor_embedding(self.futures_tenor_indices)[None, :, :]
        )
        futures_tokens = torch.cat(
            (
                futures_tokens[:, :-TAIFEX_INDEX_FUTURES_ACTION_COUNT, :],
                action_tokens,
            ),
            dim=1,
        )
        futures_tokens = futures_tokens.masked_fill(
            ~candidate_mask.unsqueeze(-1), 0.0
        )
        clean_stock = z_stock.masked_fill(~stock_mask.unsqueeze(-1), 0.0)
        joint_mask = torch.cat((stock_mask, candidate_mask), dim=-1)
        joint = torch.cat((clean_stock, futures_tokens), dim=1)
        normalized = self.futures_joint_norm(joint)
        market_queries = self.futures_joint_market_queries.expand(
            int(joint.size(0)), -1, -1
        )
        market_tokens, _ = self.futures_joint_attention(
            market_queries,
            normalized,
            normalized,
            key_padding_mask=~joint_mask,
            need_weights=False,
        )
        attended, _ = self.futures_joint_read_attention(
            normalized,
            market_tokens,
            market_tokens,
            need_weights=False,
        )
        joint = joint + attended
        joint = joint + self.futures_joint_ffn(self.futures_joint_post_norm(joint))
        joint = joint.masked_fill(~joint_mask.unsqueeze(-1), 0.0)
        futures_joint = joint[:, -TAIFEX_INDEX_FUTURES_ACTION_COUNT:, :]
        action_mask = candidate_mask[:, -TAIFEX_INDEX_FUTURES_ACTION_COUNT:]
        scores = torch.nan_to_num(
            self.futures_action_head(futures_joint).squeeze(-1).float(),
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        exposure_logit: torch.Tensor | None = None
        exposure: torch.Tensor | None = None
        allocation: torch.Tensor | None = None
        if self.futures_action_mode == "directional_allocation":
            has_actions = action_mask.any(dim=-1)
            allocation_logits = self._allocation_logits(
                scores, action_mask
            ).masked_fill(
                ~action_mask,
                finite_mask_fill_value(scores),
            )
            allocation = torch.softmax(allocation_logits, dim=-1)
            allocation = allocation * action_mask.to(dtype=allocation.dtype)
            allocation = torch.where(
                has_actions.unsqueeze(-1),
                allocation
                / allocation.sum(dim=-1, keepdim=True).clamp_min(
                    torch.finfo(allocation.dtype).eps
                ),
                torch.zeros_like(allocation),
            )
            exposure_logit = torch.nan_to_num(
                self.futures_head(market_tokens.mean(dim=1)).squeeze(-1).float(),
                nan=0.0,
                posinf=20.0,
                neginf=-20.0,
            )
            exposure = torch.where(
                has_actions,
                self._activate_futures_exposure(exposure_logit),
                torch.zeros_like(exposure_logit),
            )
            actions = allocation * exposure.unsqueeze(-1)
            reported_scores = allocation * exposure_logit.unsqueeze(-1)
        else:
            raw_actions = torch.where(
                action_mask,
                torch.tanh(scores),
                torch.zeros_like(scores),
            )
            raw_gross = raw_actions.abs().sum(dim=-1, keepdim=True)
            actions = raw_actions * torch.clamp(
                raw_actions.new_tensor(self.max_abs_exposure)
                / raw_gross.clamp_min(torch.finfo(raw_actions.dtype).eps),
                max=1.0,
            )
            reported_scores = scores
        if return_scores:
            return actions, reported_scores.masked_fill(
                ~action_mask, finite_mask_fill_value(scores)
            )
        include_aux = bool(return_aux is True or (return_aux is None and self.return_aux))
        if include_aux:
            output_aux = dict(aux)
            output_aux.update(
                {
                    "z_stock": clean_stock,
                    "futures_token_embedding": futures_joint,
                    "futures_candidate_mask": candidate_mask,
                    "futures_action_mask": action_mask,
                    "futures_action_scores": scores,
                    "futures_actions": actions,
                    "gross_exposure": actions.abs().sum(dim=-1),
                }
            )
            if exposure_logit is not None and exposure is not None and allocation is not None:
                output_aux.update(
                    {
                        "futures_exposure_logit": exposure_logit,
                        "futures_exposure": exposure,
                        "futures_action_allocation": allocation,
                        "futures_allocation_logits": allocation_logits,
                    }
                )
            if return_aux is True:
                return actions, reported_scores, output_aux
            return {
                "weights": actions,
                "scores": reported_scores,
                "aux": output_aux,
                **output_aux,
            }
        return actions

    def _portfolio_outputs_from_stock_embeddings(
        self,
        z_stock: torch.Tensor,
        mask_bool: torch.Tensor,
        aux: dict[str, torch.Tensor],
        *,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        return_scores: bool = False,
        portfolio_context: dict[str, torch.Tensor] | None = None,
    ):
        if z_stock.ndim != 3:
            raise ValueError(
                f"z_stock must have shape [B,S,D], got {tuple(z_stock.shape)}"
            )
        if tuple(mask_bool.shape) != tuple(z_stock.shape[:2]):
            raise ValueError("mask_bool must have shape [B,S]")
        mask = mask_bool.to(device=z_stock.device, dtype=torch.bool)
        if portfolio_context is not None:
            return self._joint_futures_actions(
                z_stock,
                mask,
                aux,
                return_aux=return_aux,
                return_scores=return_scores,
                portfolio_context=portfolio_context,
            )
        if self.futures_require_joint_context or self.futures_pool_score is None:
            raise ValueError(
                "futures_require_joint_context=true requires causal futures "
                "candidate_features and candidate_mask on every forward"
            )
        masked_embeddings = z_stock.masked_fill(~mask.unsqueeze(-1), 0.0)
        pool_logits = self.futures_pool_score(masked_embeddings).squeeze(-1)
        pool_logits = torch.nan_to_num(
            pool_logits,
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        if temperature is None:
            temp = pool_logits.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=pool_logits.device, dtype=pool_logits.dtype)
        else:
            temp = pool_logits.new_tensor(float(temperature))
        temp = temp.clamp_min(0.05)
        masked_logits = (pool_logits / temp).masked_fill(
            ~mask,
            finite_mask_fill_value(pool_logits),
        )
        pool_weights = torch.softmax(masked_logits, dim=-1)
        has_stocks = mask.any(dim=-1)
        pool_weights = torch.where(
            has_stocks.unsqueeze(-1),
            pool_weights * mask.to(dtype=pool_weights.dtype),
            torch.zeros_like(pool_weights),
        )
        pool_weights = pool_weights / pool_weights.sum(
            dim=-1,
            keepdim=True,
        ).clamp_min(torch.finfo(pool_weights.dtype).eps)
        market_embedding = torch.sum(
            pool_weights.unsqueeze(-1) * masked_embeddings,
            dim=1,
        )
        exposure_logit = self.futures_head(market_embedding).squeeze(-1)
        exposure_logit = torch.nan_to_num(
            exposure_logit,
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        exposure = self._activate_futures_exposure(exposure_logit)
        exposure = torch.where(has_stocks, exposure, torch.zeros_like(exposure))
        pseudo_weights = pool_weights * exposure.unsqueeze(-1)
        pseudo_scores = pool_weights * exposure_logit.unsqueeze(-1)

        if return_scores:
            return pseudo_weights, pseudo_scores
        include_aux = bool(return_aux is True or (return_aux is None and self.return_aux))
        if include_aux:
            output_aux = dict(aux)
            output_aux.update(
                {
                    "z_stock": masked_embeddings,
                    "futures_pool_weights": pool_weights,
                    "market_embedding": market_embedding,
                    "futures_exposure_logit": exposure_logit,
                    "futures_exposure": exposure,
                }
            )
            if return_aux is True:
                return pseudo_weights, pseudo_scores, output_aux
            return {
                "weights": pseudo_weights,
                "scores": pseudo_scores,
                "futures_exposure": exposure,
                "futures_exposure_logit": exposure_logit,
                "aux": output_aux,
                **output_aux,
            }
        return pseudo_weights


__all__ = ["CrossSectionalIndexFuturesModel"]
