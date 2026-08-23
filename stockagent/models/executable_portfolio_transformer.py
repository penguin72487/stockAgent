from __future__ import annotations

import math
from typing import Any

import torch
from torch import nn

from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.models.transformer_base_portfolio import _make_norm


class ExecutablePortfolioTransformerModel(FinancialTransformerModel):
    """Financial Transformer conditioned on causal execution constraints.

    The policy remains free to discover its own cross-sectional strategy.  This
    class adds only information and transformations owned by the execution
    contract:

    * direction-specific point-in-time permissions;
    * prior-session notional capacity expressed relative to reference equity;
    * a learned context fusion before the shared per-symbol score head; and
    * a final direction-permission gate with no cross-symbol redistribution.

    Cash is represented implicitly by the unused part of the L1 ball.  Volume,
    settlement, fees, lots, and account state remain owned by the canonical
    differentiable executor; this model does not fork those formulas.
    """

    _stockagent_requires_execution_context = True

    # Raw context supplied by the trainer, in this fixed order:
    # open-short, day-trade eligible, open-buy, open-sell,
    # prior-session volume notional, and decision-time-valued short capacity.
    # Close-side permissions and realised session-t volume are deliberately
    # absent: neither exists when the observed-open policy commits its target.
    RAW_EXECUTION_CONTEXT_DIM = 6
    PREPARED_EXECUTION_CONTEXT_DIM = 7

    def __init__(
        self,
        *args: Any,
        use_execution_context_features: bool = True,
        execution_context_hidden_dim: int = 32,
        execution_context_schema_version: int = 2,
        max_volume_participation: float,
        volume_participation_equity: float,
        short_capacity_limit_enabled: bool,
        **kwargs: Any,
    ) -> None:
        portfolio_mode = str(kwargs.get("portfolio_mode", "")).strip().lower()
        output_mode = str(kwargs.get("portfolio_output_mode", "")).strip().lower()
        if portfolio_mode != "long_short":
            raise ValueError(
                "executable_portfolio_transformer requires portfolio_mode='long_short'"
            )
        if output_mode != "projection_l1":
            raise ValueError(
                "executable_portfolio_transformer requires "
                "portfolio_output_mode='projection_l1'"
            )
        if bool(kwargs.get("center_long_short_logits", True)):
            raise ValueError(
                "executable_portfolio_transformer must not center long/short logits"
            )
        if not bool(kwargs.get("projection_l1_scale_by_active_count", False)):
            raise ValueError(
                "executable_portfolio_transformer requires the universe-size-"
                "invariant L1-ball scale"
            )
        if int(execution_context_schema_version) != 2:
            raise ValueError(
                "executable_portfolio_transformer requires causal execution "
                "context schema version 2"
            )

        max_participation = float(max_volume_participation)
        reference_equity = float(volume_participation_equity)
        if not 0.0 < max_participation <= 1.0:
            raise ValueError(
                "max_volume_participation must be in (0, 1] for executable context"
            )
        if not math.isfinite(reference_equity) or reference_equity <= 0.0:
            raise ValueError(
                "volume_participation_equity must be finite and positive"
            )

        super().__init__(*args, **kwargs)
        if self.num_action_channels != 1:
            raise ValueError(
                "executable_portfolio_transformer currently supports one daily "
                "signed action per symbol"
            )

        self.use_execution_context_features = bool(
            use_execution_context_features
        )
        hidden_dim = max(1, int(execution_context_hidden_dim))
        self.execution_context_hidden_dim = hidden_dim
        self.execution_context_schema_version = 2
        self.max_volume_participation = max_participation
        self.volume_participation_equity = reference_equity
        self.short_capacity_limit_enabled = bool(short_capacity_limit_enabled)

        if self.use_execution_context_features:
            self.execution_context_norm = _make_norm(
                self.PREPARED_EXECUTION_CONTEXT_DIM,
                self.norm_type,
            )
            self.execution_context_encoder: nn.Module | None = nn.Sequential(
                nn.Linear(self.PREPARED_EXECUTION_CONTEXT_DIM, hidden_dim),
                nn.SiLU(),
                nn.Linear(hidden_dim, self.d_model),
            )
            self.execution_context_gate: nn.Module | None = nn.Linear(
                self.d_model * 2,
                self.d_model,
            )
            self.execution_context_fusion_norm = _make_norm(
                self.d_model,
                self.norm_type,
            )
        else:
            # Keep no inert trainable branch in the ablation checkpoint.  The
            # context is still parsed below because its legal direction fields
            # own the non-negotiable output gate.
            self.execution_context_norm = None
            self.execution_context_encoder = None
            self.execution_context_gate = None
            self.execution_context_fusion_norm = None
        # Standalone explainability repeatedly expands the same date batch for
        # IG and perturbation scenarios.  It may explicitly bind the matching
        # immutable context once; ordinary train/infer forwards never use this
        # transient attribute and still fail closed when context is absent.
        self._explainability_execution_context: torch.Tensor | None = None

    def bind_execution_context_for_explainability(
        self,
        execution_context: torch.Tensor,
    ) -> None:
        if execution_context.dim() != 3 or int(execution_context.size(-1)) != (
            self.RAW_EXECUTION_CONTEXT_DIM
        ):
            raise ValueError(
                "explainability execution_context must have shape [B,S,6]"
            )
        self._explainability_execution_context = execution_context.detach()

    def clear_execution_context_for_explainability(self) -> None:
        self._explainability_execution_context = None

    def _bound_explainability_context(
        self,
        z_stock: torch.Tensor,
    ) -> torch.Tensor | None:
        raw = self._explainability_execution_context
        if raw is None:
            return None
        base_rows = int(raw.size(0))
        target_rows = int(z_stock.size(0))
        if base_rows <= 0 or target_rows % base_rows != 0:
            raise ValueError(
                "explainability scenario rows must be an integer repetition "
                "of the bound execution context"
            )
        repeats = target_rows // base_rows
        return raw if repeats == 1 else raw.repeat(repeats, 1, 1)

    def _prepare_execution_context(
        self,
        portfolio_context: dict[str, torch.Tensor] | None,
        z_stock: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        raw = (
            None
            if portfolio_context is None
            else portfolio_context.get("execution_context")
        )
        if raw is None:
            raw = self._bound_explainability_context(z_stock)
        if raw is None:
            raise ValueError(
                "executable_portfolio_transformer requires causal execution_context"
            )
        expected_shape = (
            int(z_stock.size(0)),
            int(z_stock.size(1)),
            self.RAW_EXECUTION_CONTEXT_DIM,
        )
        if tuple(raw.shape) != expected_shape:
            raise ValueError(
                "execution_context must have shape [B,S,6], got "
                f"{tuple(raw.shape)} expected {expected_shape}"
            )

        raw_f = torch.nan_to_num(
            raw.to(device=z_stock.device, dtype=torch.float32),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        flags = raw_f[..., :4].clamp(0.0, 1.0)
        volume_notional = raw_f[..., 4].clamp_min(0.0)
        short_notional = raw_f[..., 5].clamp_min(0.0)
        volume_capacity = (
            volume_notional
            * self.max_volume_participation
            / self.volume_participation_equity
        )
        if self.short_capacity_limit_enabled:
            short_capacity = short_notional / self.volume_participation_equity
            short_unbounded = torch.zeros_like(short_capacity)
        else:
            short_capacity = torch.zeros_like(short_notional)
            short_unbounded = torch.ones_like(short_capacity)
        prepared = torch.cat(
            (
                flags,
                torch.log1p(volume_capacity).unsqueeze(-1),
                torch.log1p(short_capacity).unsqueeze(-1),
                short_unbounded.unsqueeze(-1),
            ),
            dim=-1,
        )
        return raw_f, prepared.to(dtype=z_stock.dtype)

    @staticmethod
    def _direction_permissions(
        raw_context: torch.Tensor,
        mask_bool: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        short_open = raw_context[..., 0] > 0.5
        eligible = raw_context[..., 1] > 0.5
        buy_open = raw_context[..., 2] > 0.5
        sell_open = raw_context[..., 3] > 0.5
        common = mask_bool & eligible
        return common & buy_open, common & short_open & sell_open

    @staticmethod
    def _apply_direction_permissions(
        weights: torch.Tensor,
        long_allowed: torch.Tensor,
        short_allowed: torch.Tensor,
    ) -> torch.Tensor:
        permitted = torch.where(weights < 0.0, short_allowed, long_allowed)
        hard_executable = torch.where(
            permitted,
            weights,
            torch.zeros_like(weights),
        )
        # The forward action must obey the exact point-in-time rule, but a
        # hard sign gate has zero derivative on the forbidden side.  During a
        # long-only or short-only session that can strand an otherwise valid
        # policy at all-cash forever.  Use the identity straight-through
        # derivative only when at least one direction is executable.  This
        # supplies a boundary-crossing optimization signal without changing
        # the requested portfolio, redistributing blocked gross exposure, or
        # inventing gradients for a completely ineligible symbol.
        boundary_surrogate = (long_allowed | short_allowed) & ~permitted
        surrogate = torch.where(
            boundary_surrogate,
            weights,
            torch.zeros_like(weights),
        )
        return hard_executable + surrogate - surrogate.detach()

    @staticmethod
    def _refresh_action_aux(
        aux: dict[str, torch.Tensor],
        *,
        weights: torch.Tensor,
        pre_rule_weights: torch.Tensor,
        long_allowed: torch.Tensor,
        short_allowed: torch.Tensor,
    ) -> None:
        gross = weights.abs().sum(dim=1)
        if "projection_gross_exposure" in aux:
            aux["projection_gross_exposure"] = gross
        if "implicit_cash_weight" in aux:
            aux["implicit_cash_weight"] = (1.0 - gross).clamp_min(0.0)
        aux["pre_execution_rule_weights"] = pre_rule_weights
        aux["execution_long_allowed"] = long_allowed
        aux["execution_short_allowed"] = short_allowed

    def _replace_output_weights(
        self,
        output: Any,
        *,
        long_allowed: torch.Tensor,
        short_allowed: torch.Tensor,
    ) -> Any:
        if isinstance(output, torch.Tensor):
            return self._apply_direction_permissions(
                output,
                long_allowed,
                short_allowed,
            )
        if isinstance(output, tuple):
            pre_rule_weights = output[0]
            weights = self._apply_direction_permissions(
                pre_rule_weights,
                long_allowed,
                short_allowed,
            )
            values = list(output)
            values[0] = weights
            if len(values) >= 3 and isinstance(values[2], dict):
                values[2] = dict(values[2])
                self._refresh_action_aux(
                    values[2],
                    weights=weights,
                    pre_rule_weights=pre_rule_weights,
                    long_allowed=long_allowed,
                    short_allowed=short_allowed,
                )
            return tuple(values)
        if isinstance(output, dict):
            result = dict(output)
            pre_rule_weights = result["weights"]
            weights = self._apply_direction_permissions(
                pre_rule_weights,
                long_allowed,
                short_allowed,
            )
            result["weights"] = weights
            self._refresh_action_aux(
                result,
                weights=weights,
                pre_rule_weights=pre_rule_weights,
                long_allowed=long_allowed,
                short_allowed=short_allowed,
            )
            nested_aux = result.get("aux")
            if isinstance(nested_aux, dict):
                nested_aux = dict(nested_aux)
                self._refresh_action_aux(
                    nested_aux,
                    weights=weights,
                    pre_rule_weights=pre_rule_weights,
                    long_allowed=long_allowed,
                    short_allowed=short_allowed,
                )
                result["aux"] = nested_aux
            return result
        raise TypeError(
            "Unsupported executable portfolio model output type: "
            f"{type(output).__name__}"
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
    ) -> Any:
        raw_context, prepared_context = self._prepare_execution_context(
            portfolio_context,
            z_stock,
        )
        if self.use_execution_context_features:
            assert self.execution_context_norm is not None
            assert self.execution_context_encoder is not None
            assert self.execution_context_gate is not None
            assert self.execution_context_fusion_norm is not None
            context_delta = self.execution_context_encoder(
                self.execution_context_norm(prepared_context)
            )
            gate = torch.sigmoid(
                self.execution_context_gate(
                    torch.cat((z_stock, context_delta), dim=-1)
                )
            )
            fused_stock = self.execution_context_fusion_norm(
                z_stock + gate * context_delta
            )
        else:
            context_delta = None
            gate = None
            fused_stock = z_stock
        long_allowed, short_allowed = self._direction_permissions(
            raw_context,
            mask_bool,
        )

        if self.return_aux_details and (
            return_aux is True or (return_aux is None and self.return_aux)
        ):
            aux = dict(aux)
            aux["execution_context_features"] = prepared_context
            if context_delta is not None and gate is not None:
                aux.update(
                    {
                        "execution_context_delta": context_delta,
                        "execution_context_gate": gate,
                    }
                )

        output = super()._portfolio_outputs_from_stock_embeddings(
            fused_stock,
            mask_bool,
            aux,
            temperature=temperature,
            return_aux=return_aux,
            return_scores=return_scores,
            portfolio_context=None,
        )
        return self._replace_output_weights(
            output,
            long_allowed=long_allowed,
            short_allowed=short_allowed,
        )
