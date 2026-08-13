"""Full-stock encoder with causal relative-tenor Taiwan derivative actions."""

from __future__ import annotations

import math
from typing import Any, Final

import torch
from torch import nn

from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    TAIFEX_OPTION_CANDIDATE_CAPACITY,
    TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
)
from stockagent.data.tw_index_futures import TAIFEX_INDEX_FUTURES_TENOR_SLOTS
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.models.normalization import (
    finite_mask_fill_value,
    masked_cash_entmax15_weights,
    masked_l1_projection_weights,
)
from stockagent.models.transformer_base_portfolio import PortfolioRMSNorm


TW_INDEX_DERIVATIVE_DAY_ACTION_SCHEMA: Final[str] = (
    "futures_e1_e6_plus_prior_session_txo_candidates_v4"
)
TW_INDEX_DERIVATIVE_DAY_SHORT_ACTION_SCHEMA: Final[str] = (
    "signed_futures_e1_e6_plus_signed_prior_session_txo_candidates_v5"
)


class CrossSectionalIndexDerivativesDayModel(FinancialTransformerModel):
    """Encode all stocks, then score only today's causal derivative universe.

    Output ``[B,4102]`` contains six signed futures expiry buckets E1..E6 and
    4,096 option candidate slots.  Options are non-negative in the legacy
    contract and signed when the dated short-margin contract is enabled. Each
    option is represented by its own prior-session metadata; concrete contract
    ids never become model parameters and are never carried across sessions.
    """

    def __init__(
        self,
        *args: Any,
        maximum_capital_fraction: float = 0.98,
        derivative_head_hidden_dim: int | None = None,
        use_exposure_gate: bool = False,
        exposure_gate_init_logit: float = -2.0,
        option_maximum_capital_fraction: float = 0.98,
        allow_option_short: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs["execution_mode"] = "naive"
        super().__init__(*args, **kwargs)
        if self.portfolio_output_mode not in {
            "projection_l1",
            "cash_entmax15",
        }:
            raise ValueError(
                "cross_sectional_index_derivatives_day requires "
                "portfolio_output_mode='projection_l1' or "
                "'cash_entmax15'"
            )
        fraction = float(maximum_capital_fraction)
        if not 0.0 < fraction <= 1.0:
            raise ValueError("maximum_capital_fraction must be in (0, 1]")
        option_fraction = float(option_maximum_capital_fraction)
        if not 0.0 <= option_fraction <= fraction:
            raise ValueError(
                "option_maximum_capital_fraction must be in "
                "[0, maximum_capital_fraction]"
            )
        gate_init = float(exposure_gate_init_logit)
        if not math.isfinite(gate_init):
            raise ValueError("exposure_gate_init_logit must be finite")
        hidden = self.d_model if derivative_head_hidden_dim is None else int(
            derivative_head_hidden_dim
        )
        if hidden < 1:
            raise ValueError("derivative_head_hidden_dim must be positive")
        self.maximum_capital_fraction = fraction
        self.option_maximum_capital_fraction = option_fraction
        self.use_exposure_gate = bool(use_exposure_gate)
        self.allow_option_short = bool(allow_option_short)
        if self.allow_option_short and self.portfolio_output_mode != "cash_entmax15":
            raise ValueError(
                "short TXO actions require portfolio_output_mode="
                "'cash_entmax15'; the legacy projection path is long-option only"
            )
        self.execution_mode = "tw_index_derivatives_day"
        self.num_action_channels = 1
        self.num_option_slots = TAIFEX_OPTION_CANDIDATE_CAPACITY
        self.num_derivative_actions = TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4
        self.action_schema = (
            TW_INDEX_DERIVATIVE_DAY_SHORT_ACTION_SCHEMA
            if self.allow_option_short
            else TW_INDEX_DERIVATIVE_DAY_ACTION_SCHEMA
        )
        del self.score_head

        self.derivative_pool_score = nn.Linear(self.d_model, 1)
        self.future_market_query = nn.Linear(self.d_model, self.d_model)
        self.future_tenor_embedding = nn.Embedding(
            TAIFEX_INDEX_FUTURES_TENOR_SLOTS, self.d_model
        )
        self.future_structured_bias = nn.Linear(self.d_model, 1, bias=False)
        self.option_market_query = nn.Linear(self.d_model, self.d_model)
        self.option_candidate_encoder = nn.Sequential(
            nn.Linear(TAIFEX_OPTION_CANDIDATE_FEATURE_DIM, hidden),
            nn.SiLU(),
            nn.Linear(hidden, self.d_model),
            PortfolioRMSNorm(self.d_model),
        )
        self.option_structured_bias = nn.Linear(self.d_model, 1, bias=False)
        if self.use_exposure_gate:
            self.derivative_capital_head = nn.Linear(self.d_model, 1)
            nn.init.zeros_(self.derivative_capital_head.weight)
            nn.init.constant_(self.derivative_capital_head.bias, gate_init)

    @staticmethod
    def _require_candidate_context(
        context: dict[str, torch.Tensor] | None,
        *,
        batch_size: int,
        device: torch.device,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        if context is None:
            raise ValueError(
                "tw_index_derivatives_day requires causal candidate features and mask"
            )
        features = context.get("candidate_features")
        mask = context.get("candidate_mask")
        if features is None or mask is None:
            raise ValueError("candidate_features and candidate_mask must be paired")
        expected_features = (
            batch_size,
            TAIFEX_OPTION_CANDIDATE_CAPACITY,
            TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
        )
        expected_mask = (batch_size, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4)
        if tuple(features.shape) != expected_features:
            raise ValueError(
                f"candidate_features must have shape {expected_features}, got "
                f"{tuple(features.shape)}"
            )
        if tuple(mask.shape) != expected_mask:
            raise ValueError(
                f"candidate_mask must have shape {expected_mask}, got {tuple(mask.shape)}"
            )
        return (
            features.to(device=device),
            mask.to(device=device, dtype=torch.bool),
        )

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
        if z_stock.ndim != 3 or tuple(mask_bool.shape) != tuple(z_stock.shape[:2]):
            raise ValueError("z_stock/mask must have shapes [B,S,D] and [B,S]")
        candidate_features, candidate_mask = self._require_candidate_context(
            portfolio_context,
            batch_size=int(z_stock.size(0)),
            device=z_stock.device,
        )
        stock_mask = mask_bool.to(device=z_stock.device, dtype=torch.bool)
        clean = z_stock.masked_fill(~stock_mask.unsqueeze(-1), 0.0)
        pool_logits = torch.nan_to_num(
            self.derivative_pool_score(clean).squeeze(-1),
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
        pool_logits = (pool_logits / temp.clamp_min(0.05)).masked_fill(
            ~stock_mask, finite_mask_fill_value(pool_logits)
        )
        pool_positive = torch.sigmoid(pool_logits) * stock_mask.to(pool_logits.dtype)
        pool = pool_positive / pool_positive.sum(dim=-1, keepdim=True).clamp_min(
            torch.finfo(pool_positive.dtype).tiny
        )
        has_stocks = stock_mask.any(dim=-1)
        pool = torch.where(has_stocks.unsqueeze(-1), pool, torch.zeros_like(pool))
        market = torch.sum(pool.unsqueeze(-1) * clean, dim=1)

        future_keys = self.future_tenor_embedding.weight
        future_scores = (
            torch.matmul(
                self.future_market_query(market), future_keys.transpose(0, 1)
            )
            / math.sqrt(float(self.d_model))
            + self.future_structured_bias(future_keys).transpose(0, 1)
        ).float()
        future_mask = candidate_mask[:, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS]
        future_raw_actions = torch.where(
            future_mask,
            future_scores,
            torch.zeros_like(future_scores),
        )

        option_features = candidate_features.to(dtype=market.dtype)
        option_keys = self.option_candidate_encoder(option_features)
        option_scores = (
            torch.sum(
                self.option_market_query(market).unsqueeze(1) * option_keys,
                dim=-1,
            )
            / math.sqrt(float(self.d_model))
            + self.option_structured_bias(option_keys).squeeze(-1)
        ).float()
        option_mask = candidate_mask[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:]
        option_raw_actions = torch.where(
            option_mask,
            option_scores
            if self.allow_option_short
            else option_scores.clamp_min(0.0),
            torch.zeros_like(option_scores),
        )
        raw_actions = torch.cat((future_raw_actions, option_raw_actions), dim=-1)
        allocation_logits = torch.cat((future_scores, option_scores), dim=-1)
        allocator_aux: dict[str, torch.Tensor]
        if self.portfolio_output_mode == "projection_l1":
            projected_actions = masked_l1_projection_weights(
                raw_actions,
                candidate_mask,
                long_only=False,
                radius=self.maximum_capital_fraction,
            )
            projected_futures = projected_actions[
                :, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS
            ]
            projected_options = projected_actions[
                :, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:
            ]
            option_gross = projected_options.sum(dim=-1, keepdim=True)
            option_scale = torch.clamp(
                projected_options.new_tensor(self.option_maximum_capital_fraction)
                / option_gross.clamp_min(torch.finfo(projected_options.dtype).eps),
                max=1.0,
            )
            risk_capped_actions = torch.cat(
                (projected_futures, projected_options * option_scale), dim=-1
            )
            if self.use_exposure_gate:
                capital_gate = torch.sigmoid(
                    self.derivative_capital_head(market).float()
                )
                actions = risk_capped_actions * capital_gate
            else:
                capital_gate = torch.ones(
                    (risk_capped_actions.size(0), 1),
                    dtype=risk_capped_actions.dtype,
                    device=risk_capped_actions.device,
                )
                actions = risk_capped_actions
            allocator_aux = {
                "derivative_projected_actions": projected_actions,
                "derivative_risk_capped_actions": risk_capped_actions,
                "derivative_capital_gate": capital_gate.squeeze(-1),
                "projection_gross_exposure": actions.abs().sum(dim=-1),
            }
        else:
            short_mask = torch.cat(
                (
                    future_mask,
                    option_mask
                    if self.allow_option_short
                    else torch.zeros_like(option_mask),
                ),
                dim=-1,
            )
            actions, entmax_parts = masked_cash_entmax15_weights(
                allocation_logits,
                candidate_mask,
                short_mask=short_mask,
                radius=self.maximum_capital_fraction,
                return_parts=True,
            )
            allocator_aux = {
                "derivative_relative_action_alloc": entmax_parts[
                    "cash_entmax_relative_alloc"
                ],
                "derivative_action_conviction": entmax_parts[
                    "cash_entmax_conviction"
                ],
                "derivative_risk_fraction": entmax_parts[
                    "cash_entmax_risk_fraction"
                ],
                "derivative_cash_fraction": entmax_parts[
                    "cash_entmax_cash_fraction"
                ],
            }
        actions = torch.where(
            has_stocks.unsqueeze(-1), actions, torch.zeros_like(actions)
        )
        scores = torch.cat((future_scores, option_scores), dim=-1).masked_fill(
            ~candidate_mask, finite_mask_fill_value(option_scores)
        )
        if return_scores:
            return actions, scores
        include_aux = bool(return_aux is True or (return_aux is None and self.return_aux))
        if include_aux:
            output_aux = dict(aux)
            gross = actions.abs().sum(dim=-1, keepdim=True)
            output_aux.update(
                {
                    "z_stock": clean,
                    "derivative_pool_weights": pool,
                    "market_embedding": market,
                    "derivative_candidate_mask": candidate_mask,
                    "derivative_raw_actions": raw_actions,
                    "derivative_allocation_logits": allocation_logits,
                    "future_active_scores": future_raw_actions.abs(),
                    "option_active_scores": option_raw_actions,
                    "derivative_actions": actions,
                    "gross_exposure": gross.squeeze(-1),
                    "cash_fraction": (1.0 - gross).clamp_min(0.0),
                }
            )
            output_aux.update(allocator_aux)
            if return_aux is True:
                return actions, scores, output_aux
            return {
                "weights": actions,
                "scores": scores,
                "aux": output_aux,
                **output_aux,
            }
        return actions


__all__ = [
    "CrossSectionalIndexDerivativesDayModel",
    "TW_INDEX_DERIVATIVE_DAY_ACTION_SCHEMA",
    "TW_INDEX_DERIVATIVE_DAY_SHORT_ACTION_SCHEMA",
]
