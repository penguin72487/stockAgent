"""Full cash-stock context policy with a fixed all-TAIFEX action axis."""

from __future__ import annotations

import math
from typing import Any

import torch
from torch import nn

from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
)
from stockagent.data.tw_stock_context_futures_portfolio import (
    TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS,
)
from stockagent.models.normalization import (
    finite_mask_fill_value,
    masked_cross_sectional_mean_finite,
)
from stockagent.models.transformer_base_portfolio import (
    PortfolioRMSNorm,
    TransformerBasePortfolioModel,
)


CROSS_SECTIONAL_ALL_FUTURES_LEGACY_MODEL_CONTRACT_VERSION = 2
CROSS_SECTIONAL_ALL_FUTURES_MODEL_CONTRACT_VERSION = 3
CROSS_SECTIONAL_ALL_FUTURES_CURRENT_OPEN_MODEL_CONTRACT_VERSION = 4
CROSS_SECTIONAL_ALL_FUTURES_EXECUTOR_QUANTIZED_MODEL_CONTRACT_VERSION = 5


class CrossSectionalAllFuturesModel(TransformerBasePortfolioModel):
    """Read all stock histories and emit a fixed 1,936-slot futures action.

    Every stock/ETF futures token first receives a learned gated residual from
    its own cash-underlying embedding. Four learned market queries then read
    the union of stock embeddings and causal prior-session futures tokens, and
    every future reads those market summaries before its scalar action head.
    Complexity remains linear in the two universes rather than quadratic in
    ``stocks + futures``.
    """

    def __init__(
        self,
        *args: Any,
        futures_product_capacity: int = 1024,
        futures_joint_market_tokens: int = 4,
        futures_denomination_aware_output: bool = False,
        futures_denomination_hard_projection: bool | None = None,
        futures_current_open_feature: bool = False,
        futures_denomination_reference_capital: float = 10_000_000.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if int(futures_product_capacity) < 2:
            raise ValueError("futures_product_capacity must be at least two")
        if int(futures_joint_market_tokens) < 1:
            raise ValueError("futures_joint_market_tokens must be positive")
        reference_capital = float(futures_denomination_reference_capital)
        if not math.isfinite(reference_capital) or reference_capital <= 0.0:
            raise ValueError("futures_denomination_reference_capital must be positive")
        del self.score_head

        feature_dim = len(FUTURES_MODEL_FEATURE_COLUMNS)
        self.futures_product_capacity = int(futures_product_capacity)
        self.futures_denomination_aware_output = bool(
            futures_denomination_aware_output
        )
        self.futures_denomination_hard_projection = (
            self.futures_denomination_aware_output
            if futures_denomination_hard_projection is None
            else bool(futures_denomination_hard_projection)
        )
        if (
            self.futures_denomination_hard_projection
            and not self.futures_denomination_aware_output
        ):
            raise ValueError(
                "futures denomination hard projection requires denomination context"
            )
        self.futures_current_open_feature = bool(futures_current_open_feature)
        if (
            self.futures_current_open_feature
            and not self.futures_denomination_aware_output
        ):
            raise ValueError(
                "current futures OPEN feature requires denomination-aware output"
            )
        self.futures_denomination_reference_capital = reference_capital
        self.futures_continuous_encoder = nn.Sequential(
            nn.Linear(feature_dim - 1, self.d_model),
            nn.SiLU(),
            PortfolioRMSNorm(self.d_model),
        )
        self.futures_product_embedding = nn.Embedding(
            self.futures_product_capacity,
            self.d_model,
        )
        self.futures_slot_embedding = nn.Embedding(
            TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
            self.d_model,
        )
        self.futures_underlying_norm = PortfolioRMSNorm(self.d_model)
        self.futures_underlying_projection = nn.Linear(
            self.d_model,
            self.d_model,
            bias=False,
        )
        self.futures_underlying_gate = nn.Linear(self.d_model * 2, 1)
        nn.init.constant_(self.futures_underlying_gate.bias, -1.0)
        self.futures_joint_market_queries = nn.Parameter(
            torch.empty(1, int(futures_joint_market_tokens), self.d_model)
        )
        nn.init.normal_(
            self.futures_joint_market_queries,
            mean=0.0,
            std=self.d_model ** -0.5,
        )
        heads = min(4, self.d_model)
        while self.d_model % heads != 0:
            heads -= 1
        self.futures_joint_norm = PortfolioRMSNorm(self.d_model)
        self.futures_market_attention = nn.MultiheadAttention(
            self.d_model,
            heads,
            dropout=0.0,
            batch_first=True,
        )
        self.futures_read_attention = nn.MultiheadAttention(
            self.d_model,
            heads,
            dropout=0.0,
            batch_first=True,
        )
        self.futures_post_norm = PortfolioRMSNorm(self.d_model)
        self.futures_ffn = nn.Sequential(
            nn.Linear(self.d_model, self.d_model * 2),
            nn.SiLU(),
            nn.Linear(self.d_model * 2, self.d_model),
        )
        self.futures_action_head = nn.Linear(self.d_model, 1)
        if self.futures_denomination_aware_output:
            self.futures_denomination_encoder = nn.Sequential(
                nn.Linear(2, self.d_model),
                nn.SiLU(),
                PortfolioRMSNorm(self.d_model),
            )
        else:
            self.futures_denomination_encoder = None
        if self.futures_current_open_feature:
            self.futures_current_open_encoder = nn.Sequential(
                nn.Linear(1, self.d_model),
                nn.SiLU(),
                PortfolioRMSNorm(self.d_model),
            )
        else:
            self.futures_current_open_encoder = None
        self.register_buffer(
            "futures_slot_indices",
            torch.arange(TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT),
            persistent=False,
        )

    def _require_futures_context(
        self,
        context: dict[str, torch.Tensor],
        *,
        batch_size: int,
        device: torch.device,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        features = context.get("candidate_features")
        mask = context.get("candidate_mask")
        base_features = len(FUTURES_MODEL_FEATURE_COLUMNS)
        prior_features = len(TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS)
        expected_features = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)
        current_open_features = len(
            TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS
        )
        if features is None or mask is None:
            raise ValueError(
                "all-futures candidate_features and candidate_mask must be paired"
            )
        if (
            features.ndim != 3
            or tuple(features.shape[:2])
            != (batch_size, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)
            or int(features.size(-1))
            not in {
                base_features,
                prior_features,
                expected_features,
                current_open_features,
            }
        ):
            raise ValueError(
                "all-futures candidate_features must have shape "
                f"[B,{TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT},"
                f"{base_features}, {prior_features}, {expected_features}, or "
                f"{current_open_features}], "
                f"got {tuple(features.shape)}"
            )
        if tuple(mask.shape) != tuple(features.shape[:2]):
            raise ValueError("all-futures candidate_mask must match [B,1936]")
        features = features.to(device=device)
        feature_width = int(features.size(-1))
        if self.futures_current_open_feature and feature_width != current_open_features:
            raise ValueError(
                "current futures OPEN model requires its current OPEN gap context"
            )
        if (
            self.futures_denomination_aware_output
            and not self.futures_current_open_feature
            and feature_width != expected_features
        ):
            raise ValueError(
                "denomination-aware all-futures output requires current group, "
                "tier, and one-contract cash context"
            )
        return features, mask.to(device=device, dtype=torch.bool)

    def _denomination_aware_group_projection(
        self,
        weights: torch.Tensor,
        candidate_mask: torch.Tensor,
        denomination_features: torch.Tensor,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        """Hard group denomination in forward, stable identity STE backward.

        Every valid standard/mini pair is one economic exposure group.  The
        raw L1-ball request is netted within that group, rounded down to an
        integer number of the cheapest member's fully collateralized cash
        denomination, and divided back across the still-present members only
        so the downstream exact executor sees the same aggregate request.
        Rounding down can only release cash; it never renormalizes another
        group and never applies a top-K selection.
        """

        if int(denomination_features.size(-1)) != len(
            TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS
        ):
            raise ValueError("invalid all-futures denomination feature width")
        slots = int(weights.size(1))
        group_index = torch.round(denomination_features[..., 0]).to(torch.int64)
        group_index = group_index.clamp(0, slots - 1)
        one_contract_cash = denomination_features[..., 2].to(dtype=weights.dtype)
        valid_member = (
            candidate_mask
            & torch.isfinite(one_contract_cash)
            & (one_contract_cash > 0.0)
        )
        member_f = valid_member.to(dtype=weights.dtype)
        group_count = torch.zeros_like(weights).scatter_add(
            1, group_index, member_f
        )
        raw_group = torch.zeros_like(weights).scatter_add(
            1,
            group_index,
            torch.where(valid_member, weights, torch.zeros_like(weights)),
        )
        group_minimum_cash = torch.full_like(weights, float("inf")).scatter_reduce(
            1,
            group_index,
            torch.where(
                valid_member,
                one_contract_cash,
                torch.full_like(one_contract_cash, float("inf")),
            ),
            reduce="amin",
            include_self=True,
        )
        minimum_weight = (
            group_minimum_cash / self.futures_denomination_reference_capital
        )
        valid_group = (
            (group_count > 0.0)
            & torch.isfinite(minimum_weight)
            & (minimum_weight > 0.0)
        )
        safe_minimum = torch.where(
            valid_group, minimum_weight, torch.ones_like(minimum_weight)
        )
        units = raw_group.abs() / safe_minimum
        rounding_epsilon = torch.finfo(weights.dtype).eps * units.clamp_min(1.0) * 8.0
        whole_units = torch.floor(units + rounding_epsilon)
        hard_group = torch.sign(raw_group) * whole_units * safe_minimum
        # Exact denomination owns forward.  Backward remains the identity with
        # respect to the continuous group request, including inside a floor
        # plateau, so the optimizer can learn to cross the one-contract edge.
        straight_through_group = raw_group + (hard_group - raw_group).detach()
        straight_through_group = torch.where(
            valid_group,
            straight_through_group,
            torch.zeros_like(straight_through_group),
        )
        count_by_member = group_count.gather(1, group_index).clamp_min(1.0)
        projected = torch.where(
            valid_member,
            straight_through_group.gather(1, group_index) / count_by_member,
            torch.zeros_like(weights),
        )
        return projected, {
            "futures_raw_group_actions": raw_group,
            "futures_denomination_group_actions": straight_through_group,
            "futures_group_minimum_contract_weight": torch.where(
                valid_group, minimum_weight, torch.zeros_like(minimum_weight)
            ),
            "futures_valid_denomination_group": valid_group,
        }

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
        if portfolio_context is None:
            raise ValueError(
                "cross_sectional_all_futures requires causal futures context"
            )
        stock_mask = mask_bool.to(device=z_stock.device, dtype=torch.bool)
        if tuple(stock_mask.shape) != tuple(z_stock.shape[:2]):
            raise ValueError("stock mask must match encoded stock embeddings [B,S]")
        candidate_features, candidate_mask = self._require_futures_context(
            portfolio_context,
            batch_size=int(z_stock.size(0)),
            device=z_stock.device,
        )

        base_feature_count = len(FUTURES_MODEL_FEATURE_COLUMNS)
        prior_feature_count = len(
            TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS
        )
        has_underlying_channel = int(candidate_features.size(-1)) >= (
            prior_feature_count
        )
        has_denomination_channels = int(candidate_features.size(-1)) >= len(
            TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
        )
        has_current_open_channel = int(candidate_features.size(-1)) == len(
            TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS
        )
        base_candidate_features = candidate_features[..., :base_feature_count]
        underlying_indices = (
            torch.round(candidate_features[..., base_feature_count]).to(
                dtype=torch.long
            )
            if has_underlying_channel
            else torch.full(
                candidate_mask.shape,
                -1,
                device=candidate_mask.device,
                dtype=torch.long,
            )
        )
        product_ids = torch.nan_to_num(
            base_candidate_features[..., 0],
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).round().to(dtype=torch.long)
        product_ids = product_ids.clamp(0, self.futures_product_capacity - 1)
        futures_tokens = self.futures_continuous_encoder(
            base_candidate_features[..., 1:].to(dtype=z_stock.dtype)
        )
        futures_tokens = (
            futures_tokens
            + self.futures_product_embedding(product_ids)
            + self.futures_slot_embedding(self.futures_slot_indices)[None, :, :]
        )
        denomination_features = (
            candidate_features[
                ...,
                prior_feature_count : prior_feature_count
                + len(TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS),
            ]
            if has_denomination_channels
            else None
        )
        if self.futures_denomination_encoder is not None:
            if denomination_features is None:
                raise ValueError(
                    "denomination-aware all-futures output requires denomination context"
                )
            denomination_cash = denomination_features[..., 2].float()
            valid_denomination = (
                candidate_mask
                & torch.isfinite(denomination_cash)
                & (denomination_cash > 0.0)
            )
            log_cash_fraction = torch.log(
                (
                    denomination_cash
                    / self.futures_denomination_reference_capital
                ).clamp_min(1.0e-8)
            ).clamp(-18.0, 4.0)
            candidate_tier = torch.nan_to_num(
                denomination_features[..., 1].float(),
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
            ).clamp(0.0, 1.0)
            denomination_token = self.futures_denomination_encoder(
                torch.stack((log_cash_fraction, candidate_tier), dim=-1).to(
                    dtype=z_stock.dtype
                )
            )
            futures_tokens = futures_tokens + torch.where(
                valid_denomination.unsqueeze(-1),
                denomination_token,
                torch.zeros_like(denomination_token),
            )
        if self.futures_current_open_encoder is not None:
            if not has_current_open_channel:
                raise ValueError(
                    "current futures OPEN model requires its current OPEN gap context"
                )
            current_open_gap = torch.nan_to_num(
                candidate_features[..., -1].float(),
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
            ).clamp(-1.0, 1.0)
            current_open_token = self.futures_current_open_encoder(
                current_open_gap.unsqueeze(-1).to(dtype=z_stock.dtype)
            )
            futures_tokens = futures_tokens + torch.where(
                candidate_mask.unsqueeze(-1),
                current_open_token,
                torch.zeros_like(current_open_token),
            )
        valid_underlying = (
            (underlying_indices >= 0)
            & (underlying_indices < int(z_stock.size(1)))
            & candidate_mask
        )
        safe_underlying = underlying_indices.clamp(0, int(z_stock.size(1)) - 1)
        linked_stock = z_stock.gather(
            1,
            safe_underlying.unsqueeze(-1).expand(-1, -1, self.d_model),
        )
        valid_underlying = valid_underlying & stock_mask.gather(1, safe_underlying)
        linked_stock = linked_stock.masked_fill(
            ~valid_underlying.unsqueeze(-1),
            0.0,
        )
        linked_delta = self.futures_underlying_projection(
            self.futures_underlying_norm(linked_stock)
        )
        underlying_gate = torch.sigmoid(
            self.futures_underlying_gate(
                torch.cat((futures_tokens, linked_delta), dim=-1)
            )
        )
        futures_tokens = futures_tokens + torch.where(
            valid_underlying.unsqueeze(-1),
            underlying_gate * linked_delta,
            torch.zeros_like(linked_delta),
        )
        clean_stock = z_stock.masked_fill(~stock_mask.unsqueeze(-1), 0.0)
        clean_futures = futures_tokens.masked_fill(
            ~candidate_mask.unsqueeze(-1), 0.0
        )
        joint_mask = torch.cat((stock_mask, candidate_mask), dim=-1)
        joint = torch.cat((clean_stock, clean_futures), dim=1)
        joint = self.futures_joint_norm(joint)
        queries = self.futures_joint_market_queries.expand(
            int(joint.size(0)), -1, -1
        )
        market_tokens, _ = self.futures_market_attention(
            queries,
            joint,
            joint,
            key_padding_mask=~joint_mask,
            need_weights=False,
        )
        futures_delta, _ = self.futures_read_attention(
            clean_futures,
            market_tokens,
            market_tokens,
            need_weights=False,
        )
        futures_joint = clean_futures + futures_delta
        futures_joint = futures_joint + self.futures_ffn(
            self.futures_post_norm(futures_joint)
        )
        futures_joint = futures_joint.masked_fill(
            ~candidate_mask.unsqueeze(-1), 0.0
        )
        scores = torch.nan_to_num(
            self.futures_action_head(futures_joint).squeeze(-1).float(),
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        if temperature is None:
            temp = scores.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=scores.device, dtype=scores.dtype)
        else:
            temp = scores.new_tensor(float(temperature))
        temp = temp.clamp_min(0.05)
        centered_scores = (
            scores - masked_cross_sectional_mean_finite(scores, candidate_mask)
            if self.center_long_short_logits
            else scores
        )
        target_logits = (centered_scores / temp).masked_fill(
            ~candidate_mask, 0.0
        )
        action_aux: dict[str, torch.Tensor] = {}
        if self.portfolio_output_mode == "logits":
            weights = target_logits
        else:
            weights, action_aux = self._postprocess_flat_target_logits(
                target_logits,
                candidate_mask,
                output_mode=self.portfolio_output_mode,
                portfolio_activation=self.portfolio_activation,
                return_parts=bool(
                    return_aux is True
                    or (
                        return_aux is None
                        and self.return_aux
                        and self.return_aux_details
                    )
                ),
            )
        if self.futures_denomination_hard_projection:
            if denomination_features is None:
                raise ValueError(
                    "denomination-aware all-futures output requires denomination context"
                )
            raw_projected_weights = weights
            weights, denomination_aux = self._denomination_aware_group_projection(
                raw_projected_weights,
                candidate_mask,
                denomination_features,
            )
            action_aux = {
                **action_aux,
                **denomination_aux,
                "futures_pre_denomination_actions": raw_projected_weights,
            }
        weights = weights.masked_fill(~candidate_mask, 0.0)
        reported_scores = scores.masked_fill(
            ~candidate_mask,
            finite_mask_fill_value(scores),
        )
        if return_scores:
            return weights, reported_scores

        include_aux = bool(
            return_aux is True or (return_aux is None and self.return_aux)
        )
        if include_aux:
            output_aux = dict(aux)
            output_aux.update(action_aux)
            output_aux.update(
                {
                    "z_stock": clean_stock,
                    "futures_token_embedding": futures_joint,
                    "futures_candidate_mask": candidate_mask,
                    "futures_action_scores": scores,
                    "futures_actions": weights,
                    "futures_underlying_link_mask": valid_underlying,
                    "futures_underlying_gate": underlying_gate.masked_fill(
                        ~valid_underlying.unsqueeze(-1),
                        0.0,
                    ),
                    "gross_exposure": weights.abs().sum(dim=-1),
                    "implicit_cash_weight": (
                        1.0 - weights.abs().sum(dim=-1)
                    ).clamp_min(0.0),
                }
            )
            if return_aux is True:
                return weights, reported_scores, output_aux
            return {
                "weights": weights,
                "scores": reported_scores,
                "aux": output_aux,
                **output_aux,
            }
        return weights


__all__ = [
    "CROSS_SECTIONAL_ALL_FUTURES_CURRENT_OPEN_MODEL_CONTRACT_VERSION",
    "CROSS_SECTIONAL_ALL_FUTURES_LEGACY_MODEL_CONTRACT_VERSION",
    "CROSS_SECTIONAL_ALL_FUTURES_MODEL_CONTRACT_VERSION",
    "CrossSectionalAllFuturesModel",
]
