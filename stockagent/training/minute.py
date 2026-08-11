from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import asdict, dataclass
import gc
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable, Iterable

import numpy as np
import polars as pl
import torch
from torch import nn
from torch.amp import GradScaler
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    effective_fee_rate_vectors,
)
from stockagent.backtest.tw_minute import (
    MinuteExecutionConfig,
    MinuteExecutionState,
    execute_minute_target,
    force_close_minute_session,
    initialize_minute_execution_state,
    minute_target_weights_from_logits,
)
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.holdings import save_realized_holdings_artifacts
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import ExperimentConfig
from stockagent.data.tw_minute import (
    MINUTE_DAILY_CONTEXT_CONTRACT,
    MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS,
    MINUTE_FEATURE_COLUMNS,
    MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS,
    MinuteDatasetIndex,
    MinuteDayPanel,
    MinuteFeatureNormalizer,
    load_minute_dataset_index,
    minute_static_daily_context_feature_names,
)
from stockagent.data.walkforward import (
    WalkForwardFold,
    build_expanding_year_folds,
    validate_walk_forward_year_contract,
)
from stockagent.models.factory import build_model
from stockagent.portfolio_contract import normalize_portfolio_output_mode
from stockagent.evaluation.metrics import ic_summary
from stockagent.training.checkpoint_contract import (
    _active_model_config,
    _configuration_fingerprint_snapshot,
    _stable_fingerprint,
)
from stockagent.training.trainer import (
    FoldResult,
    TimingBreakdown,
    _EpochCurveLifecycle,
    _PreEpochTimingRecorder,
    _autocast_context,
    _can_enable_torch_compile,
    _create_adamw_optimizer,
    _create_lr_scheduler,
    _distributed_barrier,
    _distributed_is_initialized,
    _distributed_is_rank0,
    _distributed_should_write,
    _distributed_world_size,
    _load_checkpoint,
    _load_completed_fold_result,
    _load_state_dict,
    _log_curve_plot_timing,
    _prefix_backtest_result,
    _progress,
    _progress_bar,
    _refresh_walkforward_artifacts,
    _resolve_amp_dtype,
    _resolve_device,
    _resolve_distributed_data_parallel,
    _restore_rng_state,
    _raise_if_distributed_phase_failed,
    _save_fold_output_artifacts,
    _save_fold_checkpoint,
    _save_group_checkpoint,
    _step_batch_lr_scheduler,
    _timing_curve_payload,
    _torch_compile_options,
    _wrap_distributed_data_parallel_model,
)
from stockagent.training.mode_adapter import (
    chronological_session_indices,
    training_mode_spec,
    write_training_json,
)
from stockagent.training.lifecycle import (
    TrainingArtifactLayout,
    TrainingRunLifecycle,
    canonical_mode_artifact_contract,
)


MINUTE_EXECUTION_CONTRACT_VERSION = 6
MINUTE_BENCHMARK_CONTRACT = (
    "configured_symbol_adjusted_close_buy_hold_first_to_last_v1"
)
# Per-epoch validation/test reporting is audit-only: it does not alter model
# inputs, optimizer state, scheduler state, sample order, or loss/backtest
# semantics. Allocation-order changes require a fresh training contract.
MINUTE_TRAINING_CONTRACT_VERSION = 13


class _MinuteSlabForwardAdapter(nn.Module):
    """Fuse minute portfolio construction into the compiled model forward.

    ``DistributedDataParallel`` synchronizes only calls routed through its own
    ``forward``.  Keeping this adapter separate lets checkpoints continue to
    store the underlying model's historical state-dict keys. Returning final
    target weights also removes the eager model-to-allocation CUDA boundary.
    """

    def __init__(
        self,
        model: nn.Module,
        config: MinuteExecutionConfig,
        *,
        daily_context_is_encoded: bool = False,
    ) -> None:
        super().__init__()
        self.model = model
        self.daily_context_is_encoded = bool(daily_context_is_encoded)
        self.gross_exposure = float(config.gross_exposure)
        self.maximum_name_weight = (
            None
            if config.maximum_name_weight is None
            else float(config.maximum_name_weight)
        )
        self.outside_cash_logit = config.outside_cash_logit
        self.config_long_only = bool(config.long_only)
        self.portfolio_output_mode = normalize_portfolio_output_mode(
            str(getattr(model, "portfolio_output_mode", "logits"))
        )
        self.daily_context_num_features = int(
            getattr(model, "daily_context_num_features", 0)
        )

    def forward(
        self,
        feature_slabs: torch.Tensor,
        mask: torch.Tensor,
        shortable_mask: torch.Tensor | None = None,
        daily_context_features: torch.Tensor | None = None,
    ) -> torch.Tensor:
        if self.daily_context_num_features > 0:
            if daily_context_features is None:
                raise RuntimeError("tw_minute daily context tensor is missing")
            if self.daily_context_is_encoded:
                model_output = self.model.forward_from_batched_panel_slabs_with_encoded_daily_context(
                    feature_slabs,
                    daily_context_features,
                    mask,
                    return_aux=False,
                )
            else:
                model_output = (
                    self.model.forward_from_batched_panel_slabs_with_daily_context(
                        feature_slabs,
                        daily_context_features,
                        mask,
                        return_aux=False,
                    )
                )
        else:
            model_output = self.model.forward_from_batched_panel_slabs(
                feature_slabs,
                mask,
                return_aux=False,
            )
        if isinstance(model_output, dict):
            model_output = model_output.get(
                "score_logits", model_output.get("weights")
            )
        if isinstance(model_output, tuple):
            model_output = model_output[0]
        if not isinstance(model_output, torch.Tensor):
            raise RuntimeError("tw_minute model did not return portfolio outputs")
        flat_mask = mask.reshape(-1, mask.size(-1))
        if self.portfolio_output_mode in {"l1", "cash_l1", "projection_l1"}:
            # Match the canonical daily tw_day_trade ordering exactly: the
            # model first resolves its L1 allocation using only its causal
            # feature/tradability mask. Cash-L1 includes one model-scored cash
            # asset in that same normalization; projection-L1 may also leave
            # part of the budget unused. Directional eligibility, close
            # availability, and volume capacity are executor facts applied
            # afterwards; blocked requests are never redistributed.
            return model_output.float().masked_fill(~flat_mask, 0.0)
        if shortable_mask is None:
            shortable_mask = mask
        return minute_target_weights_from_logits(
            model_output,
            flat_mask,
            gross_exposure=self.gross_exposure,
            maximum_name_weight=self.maximum_name_weight,
            outside_cash_logit=self.outside_cash_logit,
            long_only=self.config_long_only,
            shortable_mask=shortable_mask.reshape(-1, shortable_mask.size(-1)),
        )


class _NullTrainingRunLifecycle:
    """Non-writer lifecycle used by DDP ranks other than rank zero."""

    def start(self, **_kwargs: Any) -> None:
        return None

    def start_group(self, **_kwargs: Any) -> None:
        return None

    def set_phase(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def update_work(self, **_kwargs: Any) -> None:
        return None

    def finish_epoch(self, **_kwargs: Any) -> None:
        return None

    def finish_fold(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def fail(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def complete(self, **_kwargs: Any) -> None:
        return None


def _release_minute_group_runtime(device: torch.device) -> dict[str, int]:
    """Release compiler and allocator state before building the next fold.

    Each expanding-year minute fold builds a fresh model, optimizer, DDP
    wrapper, and pair of fixed-shape compiled graphs.  TorchDynamo/Inductor may
    otherwise retain the preceding graph after its Python owners are gone,
    which leaves too little headroom for the next fold's first forward even
    though the same batch shape fitted in the preceding fold.
    """

    allocated_before = 0
    reserved_before = 0
    if device.type == "cuda":
        allocated_before = int(torch.cuda.memory_allocated(device))
        reserved_before = int(torch.cuda.memory_reserved(device))
    torch.compiler.reset()
    gc.collect()
    if device.type == "cuda":
        torch.cuda.empty_cache()
        try:
            torch.cuda.ipc_collect()
        except RuntimeError:
            # IPC collection is an allocator optimization, not a correctness
            # requirement, and can be unavailable in restricted runtimes.
            pass
        allocated_after = int(torch.cuda.memory_allocated(device))
        reserved_after = int(torch.cuda.memory_reserved(device))
    else:
        allocated_after = 0
        reserved_after = 0
    return {
        "allocated_before": allocated_before,
        "allocated_after": allocated_after,
        "reserved_before": reserved_before,
        "reserved_after": reserved_after,
    }


def _minute_dataset_fingerprint(dataset: MinuteDatasetIndex) -> str:
    return _stable_fingerprint(
        {
            "schema_version": dataset.manifest.get("schema_version"),
            "symbols": list(dataset.symbols),
            "dates": [str(value) for value in dataset.dates.astype("datetime64[D]")],
            "partition_sha256": [
                str(dataset.partitions[str(value)].get("output_sha256", ""))
                for value in dataset.dates.astype("datetime64[D]")
            ],
            "short_rules_fingerprint": dataset.short_rules_fingerprint,
            "daily_context_fingerprint": (
                None
                if dataset.daily_feature_context is None
                else dataset.daily_feature_context.fingerprint
            ),
            "daily_context_lookback": int(dataset.daily_context_lookback),
        }
    )


def _minute_configuration_fingerprint(config: ExperimentConfig) -> str:
    active_model_config = _active_model_config(config)["values"]
    daily_context_feature_names = (
        minute_static_daily_context_feature_names(config.data.feature_include)
        if config.data.minute_daily_context_panel_meta is not None
        else ()
    )
    return _stable_fingerprint(
        {
            "training_contract_version": MINUTE_TRAINING_CONTRACT_VERSION,
            "execution_config": asdict(_execution_config(config)),
            "fee_schedule": asdict(_fee_schedule(config)),
            "model_name": config.training.model_name,
            "lookback": config.training.lookback,
            "loss_type": config.training.loss_type,
            "batch_size_train": config.training.batch_size_train,
            "minute_decision_chunk_rows": config.training.minute_decision_chunk_rows,
            "minute_full_day_credit_assignment": (
                config.training.minute_full_day_credit_assignment
            ),
            "minute_full_day_activation_checkpoint": (
                config.training.minute_full_day_activation_checkpoint
            ),
            "minute_stateful_proximal_allocator": (
                config.training.minute_stateful_proximal_allocator
            ),
            "minute_proximal_cost_multiplier": (
                config.training.minute_proximal_cost_multiplier
            ),
            "amp_dtype": config.environment.amp_dtype,
            "daily_context_panel_meta": (
                config.data.minute_daily_context_panel_meta
            ),
            "daily_context_feature_names": list(daily_context_feature_names),
            "active_model_config": active_model_config,
        }
    )


def _minute_artifact_contract(
    dataset: MinuteDatasetIndex,
    config: ExperimentConfig,
) -> dict[str, Any]:
    normalized_model_name = str(config.training.model_name).strip().lower().replace(
        "-", "_"
    )
    active_model_config = (
        config.training.financial_transformer
        if normalized_model_name
        in {
            "financial_transformer",
            "financial_transformer_model",
            "financial_token_transformer",
            "financial_tokenized_transformer",
        }
        else config.training.transformer_base_portfolio
    )
    output_mode = normalize_portfolio_output_mode(
        active_model_config.portfolio_output_mode
    )
    return {
        **canonical_mode_artifact_contract(config.trading.execution_mode),
        "execution_contract_version": MINUTE_EXECUTION_CONTRACT_VERSION,
        "training_contract_version": MINUTE_TRAINING_CONTRACT_VERSION,
        "dataset_fingerprint": _minute_dataset_fingerprint(dataset),
        "configuration_fingerprint": _minute_configuration_fingerprint(config),
        "benchmark_name": str(config.data.benchmark_name),
        "benchmark_contract": MINUTE_BENCHMARK_CONTRACT,
        "benchmark_fingerprint": (
            None
            if dataset.daily_feature_context is None
            else dataset.daily_feature_context.benchmark_fingerprint
        ),
        "portfolio_output_mode": output_mode,
        "cash_allocation_contract": (
            "contextual_cash_asset_joint_signed_l1_v1"
            if output_mode == "cash_l1"
            else None
        ),
    }


def _fee_schedule(config: ExperimentConfig) -> TaiwanFeeSchedule:
    trading = config.trading
    return TaiwanFeeSchedule(
        commission_rate=trading.tw_commission_rate,
        commission_discount=trading.tw_commission_discount,
        commission_rebate_timing=trading.tw_commission_rebate_timing,
        stock_sell_tax=trading.tw_stock_sell_tax,
        etf_sell_tax=trading.tw_etf_sell_tax,
        day_trade_stock_sell_tax=trading.tw_day_trade_stock_sell_tax,
        day_trade_etf_sell_tax=trading.tw_day_trade_etf_sell_tax,
        minimum_commission=trading.tw_minimum_commission,
        commission_rounding=trading.tw_commission_rounding,
        tax_rounding=trading.tw_tax_rounding,
        settlement_lag_sessions=trading.tw_settlement_lag_sessions,
        cash_lot_size=trading.tw_cash_lot_size,
        day_trade_default_lot_size=trading.tw_day_trade_lot_size,
    )


def _execution_config(config: ExperimentConfig) -> MinuteExecutionConfig:
    trading = config.trading
    participation = float(trading.max_volume_participation)
    if participation <= 0.0:
        raise ValueError(
            "tw_minute requires trading.max_volume_participation in (0,1]; "
            "an unlimited KBar-volume fill is not a defensible execution model"
        )
    return MinuteExecutionConfig(
        initial_equity=float(trading.tw_minute_initial_equity),
        gross_exposure=float(trading.tw_minute_gross_exposure),
        maximum_name_weight=(
            None
            if trading.tw_minute_max_name_weight is None
            else float(trading.tw_minute_max_name_weight)
        ),
        outside_cash_logit=(
            None
            if trading.tw_minute_outside_cash_logit is None
            else float(trading.tw_minute_outside_cash_logit)
        ),
        maximum_volume_participation=participation,
        maximum_order_notional=(
            None
            if trading.tw_minute_max_order_notional is None
            else float(trading.tw_minute_max_order_notional)
        ),
        slippage_bps_per_side=float(trading.tw_minute_slippage_bps_per_side),
        long_only=bool(trading.long_only),
    )


def _freeze_inactive_minute_parameters(model: nn.Module) -> tuple[str, ...]:
    """Remove disabled positional tables from optimizer/DDP bookkeeping.

    The scalable Transformer preserves these tensors in its state dict for old
    checkpoint compatibility even when a config disables their use. Minute
    training intentionally has a fresh checkpoint contract, so keeping an
    unreachable parameter trainable only creates optimizer state and forces
    DDP unused-parameter traversal on every one of the day's forward calls.
    """

    frozen: list[str] = []
    for enabled_name, parameter_name in (
        ("use_time_pos", "time_position"),
        ("use_time_pos", "daily_context_time_position"),
        ("use_symbol_pos", "symbol_position"),
    ):
        if bool(getattr(model, enabled_name, True)):
            continue
        parameter = getattr(model, parameter_name, None)
        if isinstance(parameter, nn.Parameter) and parameter.requires_grad:
            parameter.requires_grad_(False)
            frozen.append(parameter_name)
    return tuple(frozen)


def _build_windows(
    day: MinuteDayPanel,
    decision_indices: np.ndarray,
    *,
    lookback: int,
) -> tuple[np.ndarray, np.ndarray]:
    windows = np.stack(
        [day.features[index - lookback + 1 : index + 1] for index in decision_indices],
        axis=0,
    )
    masks = np.stack(
        [
            # This is a model-input mask, so it may depend only on completed
            # bars. Next-bar liquidity and the eventual session close are
            # executor-only fields and must not leak through symbol support.
            day.feature_mask[index - lookback + 1 : index + 1].all(axis=0)
            for index in decision_indices
        ],
        axis=0,
    )
    return windows, masks


def _batched_slab_targets(
    model_forward: Callable[
        [torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor], Any
    ],
    feature_slabs: torch.Tensor,
    mask: torch.Tensor,
    shortable_mask: torch.Tensor,
    daily_context_features: torch.Tensor,
) -> torch.Tensor:
    output = model_forward(
        feature_slabs,
        mask,
        shortable_mask,
        daily_context_features,
    )
    if isinstance(output, dict):
        output = output.get("score_logits", output.get("weights"))
    if isinstance(output, tuple):
        output = output[0]
    expected_rows = int(feature_slabs.size(0)) * int(mask.size(1))
    if (
        not isinstance(output, torch.Tensor)
        or output.ndim != 2
        or int(output.size(0)) != expected_rows
    ):
        raise RuntimeError(
            "tw_minute batched panel-slab model must return [D*C,S] targets"
        )
    return output


def _stateful_proximal_target_weights(
    requested_weights: torch.Tensor,
    previous_executed_weights: torch.Tensor,
    *,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    cost_multiplier: float,
    long_only: bool,
) -> torch.Tensor:
    """Cost-aware target update around the actual pre-trade portfolio.

    This is the proximal map for a quadratic pull toward the model proposal and
    an asymmetric L1 switching cost.  The threshold uses the exact applicable
    one-way fee: buys (including short covers) pay ``buy_fee_rates`` and sells
    (including short opens) pay ``sell_fee_rates``.  If every proposed change
    lies inside the no-trade region, the previous portfolio is returned exactly.
    """

    if requested_weights.shape != previous_executed_weights.shape:
        raise ValueError("requested and previous minute weights must have one shape")
    if requested_weights.dim() != 2:
        raise ValueError("stateful minute allocator expects [B,S] weights")
    if buy_fee_rates.shape != (requested_weights.size(1),) or sell_fee_rates.shape != (
        requested_weights.size(1),
    ):
        raise ValueError("stateful minute allocator fee vectors must have shape [S]")
    multiplier = float(cost_multiplier)
    if not math.isfinite(multiplier) or multiplier < 0.0:
        raise ValueError("minute proximal cost multiplier must be finite and nonnegative")

    requested = requested_weights.float()
    previous = previous_executed_weights.float()
    if long_only:
        requested = requested.clamp_min(0.0)
        previous = previous.clamp_min(0.0)
    delta = requested - previous
    fee_threshold = torch.where(
        delta >= 0.0,
        buy_fee_rates.float().view(1, -1),
        sell_fee_rates.float().view(1, -1),
    ) * multiplier
    shrunk_delta = torch.sign(delta) * torch.relu(delta.abs() - fee_threshold)
    changed = shrunk_delta.abs().sum(dim=-1, keepdim=True) > 0.0
    candidate = previous + shrunk_delta
    if long_only:
        candidate = candidate.clamp_min(0.0)

    previous_gross = previous.abs().sum(dim=-1, keepdim=True)
    # Do not L1-normalize this candidate.  Every coordinate lies between the
    # previous and requested endpoint, so the convexity of the L1 ball already
    # guarantees feasibility.  Normalizing to either endpoint gross would
    # re-introduce the turnover removed by the proximal threshold and would
    # prevent a deliberate reduction of risky gross in favour of cash.
    # The first allocation has no previous inventory to preserve; charge its
    # real entry fee in the ledger without sparsifying the model proposal.
    initial_entry = previous_gross <= 1e-12
    return torch.where(
        initial_entry,
        requested,
        torch.where(changed, candidate, previous),
    )


def _execute_minute_chunk(
    cash: torch.Tensor,
    shares: torch.Tensor,
    last_prices: torch.Tensor,
    equity: torch.Tensor,
    targets: torch.Tensor,
    execution_masks: torch.Tensor,
    execution_opens: torch.Tensor,
    exit_closes: torch.Tensor,
    future_volumes: torch.Tensor,
    allow_new_entries: torch.Tensor,
    short_open_masks: torch.Tensor | None = None,
    short_capacity_shares: torch.Tensor | None = None,
    *,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    config: MinuteExecutionConfig,
    stateful_proximal_allocator: bool = False,
    proximal_cost_multiplier: float = 1.0,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Fixed-shape differentiable minute ledger chunk used by eager and compile.

    The time loop is deliberately bounded by ``C``. ``torch.compile`` unrolls
    this small sequential recurrence while the eager path remains the semantic
    oracle for numerical and gradient tests.
    """
    state = MinuteExecutionState(
        cash=cash,
        shares=shares,
        last_prices=last_prices,
        equity=equity,
    )
    log_utility = torch.zeros_like(equity)
    turnover = torch.zeros_like(equity)
    fees = torch.zeros_like(equity)
    slippage = torch.zeros_like(equity)
    for local in range(int(targets.size(1))):
        target_weights = targets[:, local]
        if stateful_proximal_allocator:
            mark_prices = torch.where(
                execution_masks[:, local]
                & torch.isfinite(execution_opens[:, local])
                & (execution_opens[:, local] > 0.0),
                execution_opens[:, local],
                state.last_prices,
            )
            marked_nav = state.cash + torch.sum(
                state.shares * mark_prices,
                dim=-1,
            )
            previous_weights = torch.where(
                marked_nav.unsqueeze(-1) > 1e-12,
                state.shares
                * mark_prices
                / torch.clamp_min(marked_nav.unsqueeze(-1), 1e-12),
                torch.zeros_like(state.shares),
            )
            target_weights = _stateful_proximal_target_weights(
                target_weights,
                previous_weights,
                buy_fee_rates=buy_fee_rates,
                sell_fee_rates=sell_fee_rates,
                cost_multiplier=proximal_cost_multiplier,
                long_only=bool(config.long_only),
            )
        step = execute_minute_target(
            state,
            target_weights=target_weights,
            execution_mask=execution_masks[:, local],
            execution_open=execution_opens[:, local],
            exit_close=exit_closes[:, local],
            future_volume_shares=future_volumes[:, local],
            buy_fee_rates=buy_fee_rates,
            sell_fee_rates=sell_fee_rates,
            config=config,
            allow_new_entries=allow_new_entries[local],
            short_open_mask=(
                None if short_open_masks is None else short_open_masks[:, local]
            ),
            short_capacity_shares=short_capacity_shares,
        )
        state = step.state
        log_utility = log_utility + torch.log1p(
            torch.clamp(step.net_return, min=-0.999999)
        )
        turnover = turnover + step.turnover_notional
        fees = fees + step.explicit_fees
        slippage = slippage + step.slippage_cost
    return (
        state.cash,
        state.shares,
        state.last_prices,
        state.equity,
        log_utility,
        turnover,
        fees,
        slippage,
    )


def _minute_chunk_training_loss(
    chunk_log_utility: torch.Tensor,
    *,
    real_days: int,
    global_real_days: int,
    decisions_per_day: int,
    gradient_world_size: int,
) -> torch.Tensor:
    real = int(real_days)
    global_days = int(global_real_days)
    decisions = int(decisions_per_day)
    world_size = int(gradient_world_size)
    if (
        real < 1
        or global_days < real
        or decisions < 1
        or world_size < 1
        or int(chunk_log_utility.numel()) < real
    ):
        raise ValueError("invalid tw_minute distributed loss denominator")
    # DDP averages rank gradients.  Multiplying each local loss by the world
    # size makes the reduced gradient equal the mean over the complete global
    # chronological batch, including a ragged final batch.
    return -chunk_log_utility[:real].sum() * float(world_size) / float(
        global_days * decisions
    )


def _all_reduce_full_day_gradients(
    model: nn.Module,
    *,
    bucket_cap_mb: int,
) -> None:
    """Synchronize one full-day backward accumulated under DDP ``no_sync``.

    A full minute session deliberately performs several policy forwards before
    its single backward so future fees can reach earlier decisions. DDP's
    reducer models every forward as an iteration and therefore cannot own that
    multi-forward graph directly. We keep all forwards and the backward under
    ``no_sync`` and reproduce DDP's averaged-gradient contract here, using
    bounded flat buckets instead of one collective per parameter.
    """

    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return
    parameters = [parameter for parameter in model.parameters() if parameter.requires_grad]
    device = next((parameter.device for parameter in parameters), torch.device("cpu"))
    present = torch.tensor(
        [parameter.grad is not None for parameter in parameters],
        device=device,
        dtype=torch.int32,
    )
    torch.distributed.all_reduce(present, op=torch.distributed.ReduceOp.SUM)
    world_size = _distributed_world_size()
    inconsistent = (present != 0) & (present != world_size)
    if bool(torch.any(inconsistent).item()):
        indices = torch.nonzero(inconsistent, as_tuple=False).flatten().tolist()
        raise RuntimeError(
            "tw_minute full-day DDP ranks used different parameters: "
            f"indices={indices[:16]}"
        )
    gradients = [
        parameter.grad
        for parameter, count in zip(parameters, present.tolist())
        if count == world_size and parameter.grad is not None
    ]
    if any(gradient.is_sparse for gradient in gradients):
        raise RuntimeError("tw_minute full-day DDP does not support sparse gradients")
    cap_bytes = max(1, int(bucket_cap_mb)) * 1024 * 1024
    buckets: list[list[torch.Tensor]] = []
    current: list[torch.Tensor] = []
    current_bytes = 0
    current_key: tuple[torch.device, torch.dtype] | None = None
    for gradient in gradients:
        key = (gradient.device, gradient.dtype)
        gradient_bytes = int(gradient.numel() * gradient.element_size())
        if current and (key != current_key or current_bytes + gradient_bytes > cap_bytes):
            buckets.append(current)
            current = []
            current_bytes = 0
        current.append(gradient)
        current_bytes += gradient_bytes
        current_key = key
    if current:
        buckets.append(current)
    inverse_world_size = 1.0 / float(world_size)
    for bucket in buckets:
        flat = torch.cat([gradient.reshape(-1) for gradient in bucket])
        torch.distributed.all_reduce(flat, op=torch.distributed.ReduceOp.SUM)
        flat.mul_(inverse_world_size)
        offset = 0
        for gradient in bucket:
            count = int(gradient.numel())
            gradient.copy_(flat[offset : offset + count].view_as(gradient))
            offset += count


def _decision_indices(config: ExperimentConfig) -> np.ndarray:
    first = max(
        int(config.trading.tw_minute_first_decision_minute),
        int(config.training.lookback),
    )
    last = int(config.trading.tw_minute_last_decision_minute)
    # Minute label m maps to zero-based dense index m-1.
    return np.arange(first - 1, last, dtype=np.int64)


def _minute_benchmark_log_return(
    day: MinuteDayPanel,
) -> float:
    """Return the configured symbol's session-ending adjusted-close log return.

    This deliberately ignores the minute executor's first tradable open and
    close masks.  A benchmark is one buy-and-hold path, not a sequence of
    independently reset intraday round trips.
    """

    value = float(day.benchmark_log_return)
    if bool(day.benchmark_price_available) and math.isfinite(value):
        return value
    return float("nan")


def _day_panel_nbytes(day: MinuteDayPanel) -> int:
    return int(
        sum(
            value.nbytes
            for value in (
                day.features,
                day.feature_mask,
                day.execution_mask,
                day.execution_open,
                day.exit_close,
                day.future_volume_shares,
                day.session_close,
                day.session_exit_mask,
                np.asarray(day.benchmark_log_return, dtype=np.float32),
                np.asarray(day.benchmark_price_available, dtype=np.bool_),
                *(
                    ()
                    if day.daily_context_features is None
                    else (day.daily_context_features,)
                ),
                *(
                    ()
                    if day.short_open_mask is None
                    else (day.short_open_mask,)
                ),
                *(
                    ()
                    if day.short_capacity_shares is None
                    else (day.short_capacity_shares,)
                ),
            )
        )
    )


@dataclass(frozen=True, slots=True)
class _MinuteHostBatch:
    trade_dates: tuple[np.datetime64, ...]
    fields: dict[str, torch.Tensor]
    real_days: int
    padded_days: int
    nbytes: int


class _MinuteDayCache:
    """Fold-normalized, byte-bounded host cache with parallel parquet loading."""

    def __init__(
        self,
        dataset: MinuteDatasetIndex,
        normalizer: MinuteFeatureNormalizer,
        *,
        maximum_bytes: int,
        workers: int,
        feature_storage_dtype: torch.dtype | None = None,
    ) -> None:
        self.dataset = dataset
        self.normalizer = normalizer
        self.maximum_bytes = max(0, int(maximum_bytes))
        if feature_storage_dtype not in {None, torch.float16, torch.bfloat16}:
            raise ValueError(
                "minute host feature storage dtype must be FP16, BF16, or disabled"
            )
        self.feature_storage_dtype = feature_storage_dtype
        self._cache: OrderedDict[int, MinuteDayPanel] = OrderedDict()
        self._cache_bytes = 0
        self._batch_cache: OrderedDict[tuple[tuple[int, ...], int], _MinuteHostBatch] = (
            OrderedDict()
        )
        self._batch_cache_bytes = 0
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(workers)),
            thread_name_prefix="tw-minute-parquet",
        )
        # Coordinate one complete future batch outside the parquet worker pool.
        # The training thread can then execute the current GPU batch while the
        # otherwise-idle CPU workers read and stack the next chronological one.
        self._prefetch_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="tw-minute-prefetch",
        )
        self.hits = 0
        self.misses = 0

    @property
    def cache_bytes(self) -> int:
        return self._cache_bytes + self._batch_cache_bytes

    def close(self) -> None:
        # The coordinator may be waiting on parquet workers, so always drain it
        # before shutting down the worker pool it depends on.
        self._prefetch_executor.shutdown(wait=True, cancel_futures=False)
        self._executor.shutdown(wait=True, cancel_futures=False)

    def _load_one(self, index: int) -> tuple[int, MinuteDayPanel]:
        return index, self.dataset.load_day(index, normalizer=self.normalizer)

    def get_many(self, indices: list[int]) -> list[MinuteDayPanel]:
        missing: list[int] = []
        seen_missing: set[int] = set()
        for index in indices:
            if index in self._cache:
                self.hits += 1
                self._cache.move_to_end(index)
            elif index not in seen_missing:
                self.misses += 1
                missing.append(index)
                seen_missing.add(index)

        loaded = dict(self._executor.map(self._load_one, missing))
        for index in missing:
            day = loaded[index]
            size = _day_panel_nbytes(day)
            if self.maximum_bytes > 0 and size <= self.maximum_bytes:
                while self._cache and self._cache_bytes + size > self.maximum_bytes:
                    _, evicted = self._cache.popitem(last=False)
                    self._cache_bytes -= _day_panel_nbytes(evicted)
                self._cache[index] = day
                self._cache_bytes += size

        return [self._cache.get(index, loaded.get(index)) for index in indices]

    def get_many_stacked(
        self,
        indices: list[int],
        *,
        padded_days: int,
    ) -> _MinuteHostBatch:
        """Cache the exact contiguous host tensors consumed every epoch.

        Caching individual day objects still paid eight ``np.stack`` copies per
        optimizer batch.  The batch plan is deterministic, so cache its final
        tensor representation instead and keep it within the same byte budget.
        """

        key = (tuple(int(value) for value in indices), int(padded_days))
        cached = self._batch_cache.get(key)
        if cached is not None:
            self.hits += len(indices)
            self._batch_cache.move_to_end(key)
            return cached
        self.misses += len(indices)
        days = [day for _, day in self._executor.map(self._load_one, indices)]
        field_names = (
            "features",
            "feature_mask",
            "execution_mask",
            "execution_open",
            "exit_close",
            "future_volume_shares",
            "session_close",
            "session_exit_mask",
            "short_open_mask",
            "short_capacity_shares",
            "daily_context_features",
            "benchmark_log_return",
            "benchmark_price_available",
        )
        fields: dict[str, torch.Tensor] = {}
        for name in field_names:
            value = _stack_day_field(days, name, padded_days=int(padded_days))
            if (
                self.feature_storage_dtype is not None
                and name in {"features", "daily_context_features"}
            ):
                value = value.to(dtype=self.feature_storage_dtype)
            fields[name] = value
        size = int(
            sum(value.numel() * value.element_size() for value in fields.values())
        )
        batch = _MinuteHostBatch(
            trade_dates=tuple(day.trade_date for day in days),
            fields=fields,
            real_days=len(days),
            padded_days=int(padded_days),
            nbytes=size,
        )
        if self.maximum_bytes > 0 and size <= self.maximum_bytes:
            while (
                self._batch_cache
                and self._batch_cache_bytes + size > self.maximum_bytes
            ):
                _, evicted = self._batch_cache.popitem(last=False)
                self._batch_cache_bytes -= evicted.nbytes
            self._batch_cache[key] = batch
            self._batch_cache_bytes += size
        return batch

    def prefetch_many_stacked(
        self,
        indices: list[int],
        *,
        padded_days: int,
    ) -> Future[_MinuteHostBatch]:
        """Schedule one chronological host batch for CPU/GPU overlap."""

        return self._prefetch_executor.submit(
            self.get_many_stacked,
            indices,
            padded_days=int(padded_days),
        )


def _stack_day_field(
    days: list[MinuteDayPanel],
    name: str,
    *,
    padded_days: int,
) -> torch.Tensor:
    values: list[np.ndarray] = []
    for day in days:
        value = getattr(day, name)
        if value is None:
            if name == "short_open_mask":
                value = np.zeros(day.features.shape[1], dtype=np.bool_)
            elif name == "short_capacity_shares":
                value = np.zeros(day.features.shape[1], dtype=np.int64)
            elif name == "daily_context_features":
                value = np.zeros((day.features.shape[1], 0), dtype=np.float32)
            elif name == "benchmark_log_return":
                value = np.asarray(float("nan"), dtype=np.float32)
            elif name == "benchmark_price_available":
                value = np.asarray(False, dtype=np.bool_)
            else:
                raise RuntimeError(f"minute host field {name!r} is missing")
        values.append(value)
    if len(days) < padded_days:
        template = values[0]
        values.extend(np.zeros_like(template) for _ in range(padded_days - len(days)))
    return torch.from_numpy(np.stack(values, axis=0))


def _run_day_batch(
    *,
    model: nn.Module,
    model_forward: Callable[
        [torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor], Any
    ],
    daily_context_forward: Callable[[torch.Tensor], torch.Tensor] | None = None,
    execution_forward: Callable[..., tuple[torch.Tensor, ...]],
    days: list[MinuteDayPanel] | None,
    host_batch: _MinuteHostBatch | None = None,
    config: ExperimentConfig,
    execution_config: MinuteExecutionConfig,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    day_batch_size: int,
    decision_chunk_rows: int,
    optimizer: torch.optim.Optimizer | None,
    scaler: GradScaler | None,
    scheduler: torch.optim.lr_scheduler.LRScheduler
    | torch.optim.lr_scheduler.ReduceLROnPlateau
    | None,
    gradient_clip_norm: float,
    global_real_days: int | None = None,
    gradient_world_size: int = 1,
) -> tuple[list[dict[str, float]], np.ndarray, TimingBreakdown]:
    training = optimizer is not None
    model.train(training)
    normalized_model_name = (
        str(config.training.model_name).strip().lower().replace("-", "_")
    )
    active_model_config = (
        config.training.financial_transformer
        if normalized_model_name
        in {
            "financial_transformer",
            "financial_transformer_model",
            "financial_token_transformer",
            "financial_tokenized_transformer",
        }
        else config.training.transformer_base_portfolio
    )
    record_model_cash = (
        not training
        and normalize_portfolio_output_mode(
            active_model_config.portfolio_output_mode
        )
        == "cash_l1"
    )
    model_requested_cash_parts: list[torch.Tensor] = []
    if host_batch is None:
        if not days or len(days) > int(day_batch_size):
            raise ValueError("tw_minute day batch must contain 1..day_batch_size days")
        real_days = len(days)
        trade_dates = tuple(day.trade_date for day in days)
    else:
        if days is not None:
            raise ValueError("tw_minute batch accepts days or host_batch, not both")
        if host_batch.real_days < 1 or host_batch.real_days > int(day_batch_size):
            raise ValueError("tw_minute cached host batch has invalid day count")
        real_days = int(host_batch.real_days)
        trade_dates = host_batch.trade_dates
    day_batch_size = int(day_batch_size)
    decision_chunk_rows = max(1, int(decision_chunk_rows))
    decisions = _decision_indices(config)
    timing = TimingBreakdown(batches=1)
    non_blocking = bool(config.training.non_blocking_transfer)
    prepare_started = time.perf_counter()
    cpu_fields = (
        host_batch.fields
        if host_batch is not None
        else {
            name: _stack_day_field(days or [], name, padded_days=day_batch_size)
            for name in (
                "features",
                "feature_mask",
                "execution_mask",
                "execution_open",
                "exit_close",
                "future_volume_shares",
                "session_close",
                "session_exit_mask",
                "short_open_mask",
                "short_capacity_shares",
                "daily_context_features",
                "benchmark_log_return",
                "benchmark_price_available",
            )
        }
    )
    timing.batch_prepare_s += time.perf_counter() - prepare_started
    state = initialize_minute_execution_state(
        num_symbols=int(cpu_fields["features"].size(2)),
        config=execution_config,
        device=device,
        batch_size=day_batch_size,
    )
    total_turnover = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_fees = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_slippage = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_log_return = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    session_close_weights = torch.zeros_like(state.shares)
    transfer_started = time.perf_counter()
    feature_device_dtype = (
        amp_dtype
        if bool(config.training.cache_train_features_in_amp_dtype)
        and device.type == "cuda"
        and amp_dtype in {torch.float16, torch.bfloat16}
        else torch.float32
    )
    day_features = cpu_fields["features"].to(
        device=device, dtype=feature_device_dtype, non_blocking=non_blocking
    )
    day_feature_mask = cpu_fields["feature_mask"].to(
        device=device, dtype=torch.bool, non_blocking=non_blocking
    )
    lookback = int(config.training.lookback)
    execution_masks = cpu_fields["execution_mask"].to(
        device=device, dtype=torch.bool, non_blocking=non_blocking
    )
    execution_opens = cpu_fields["execution_open"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
    )
    exit_closes = cpu_fields["exit_close"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
    )
    future_volumes = cpu_fields["future_volume_shares"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
    )
    session_close = cpu_fields["session_close"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
    )
    session_exit_mask = cpu_fields["session_exit_mask"].to(
        device=device, dtype=torch.bool, non_blocking=non_blocking
    )
    day_short_open_mask = cpu_fields["short_open_mask"].to(
        device=device, dtype=torch.bool, non_blocking=non_blocking
    )
    day_short_capacity = cpu_fields["short_capacity_shares"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
    )
    daily_context_features = cpu_fields["daily_context_features"].to(
        device=device, dtype=feature_device_dtype, non_blocking=non_blocking
    )
    timing.transfer_s += time.perf_counter() - transfer_started
    chunks = list(range(0, len(decisions), decision_chunk_rows))
    full_day_credit_assignment = bool(
        training and config.training.minute_full_day_credit_assignment
    )
    full_day_activation_checkpoint = bool(
        full_day_credit_assignment
        and config.training.minute_full_day_activation_checkpoint
    )
    full_day_log_utility = torch.zeros(
        day_batch_size, device=device, dtype=torch.float32
    )
    if training:
        optimizer.zero_grad(set_to_none=True)
    deferred_ddp_sync = bool(
        full_day_credit_assignment
        and _distributed_is_initialized()
        and _distributed_world_size() > 1
        and hasattr(model, "no_sync")
    )
    full_day_sync_context = model.no_sync() if deferred_ddp_sync else nullcontext()
    full_day_sync_context.__enter__()
    policy_daily_context = daily_context_features
    if daily_context_forward is not None:
        daily_started = time.perf_counter()

        def _encode_daily_context(features: torch.Tensor) -> torch.Tensor:
            with _autocast_context(device, amp_dtype):
                return daily_context_forward(features)

        with torch.set_grad_enabled(training):
            if full_day_activation_checkpoint:
                policy_daily_context = activation_checkpoint(
                    _encode_daily_context,
                    daily_context_features,
                    use_reentrant=False,
                    preserve_rng_state=True,
                )
            else:
                policy_daily_context = _encode_daily_context(
                    daily_context_features
                )
        timing.forward_s += time.perf_counter() - daily_started
    for chunk_number, start in enumerate(chunks):
        is_last_chunk = chunk_number == len(chunks) - 1
        # A minute optimizer batch contains many recurrent ledger chunks. DDP
        # needs to reduce gradients only once, after the last chunk has added
        # its contribution. The official context must cover both forward and
        # backward; in particular, compiled DDP expects its synchronization
        # flag to be restored between consecutive no-sync chunks.
        sync_context = (
            model.no_sync()
            if (
                training
                and not full_day_credit_assignment
                and not is_last_chunk
                and hasattr(model, "no_sync")
            )
            else nullcontext()
        )
        sync_context.__enter__()
        selected = decisions[start : start + decision_chunk_rows]
        real_rows = len(selected)
        slab_start = int(selected[0]) - lookback + 1
        slab_rows = real_rows + lookback - 1
        feature_slabs = day_features[:, slab_start : slab_start + slab_rows]
        mask_rows = day_feature_mask[:, slab_start : slab_start + slab_rows]
        model_masks = mask_rows.unfold(1, lookback, 1).all(dim=-1)
        if real_rows < decision_chunk_rows:
            pad_rows = decision_chunk_rows - real_rows
            feature_slabs = torch.cat(
                [
                    feature_slabs,
                    torch.zeros(
                        day_batch_size,
                        pad_rows,
                        feature_slabs.size(2),
                        feature_slabs.size(3),
                        device=device,
                        dtype=feature_slabs.dtype,
                    ),
                ],
                dim=1,
            )
            model_masks = torch.cat(
                [
                    model_masks,
                    torch.zeros(
                        day_batch_size,
                        pad_rows,
                        model_masks.size(2),
                        device=device,
                        dtype=torch.bool,
                    ),
                ],
                dim=1,
            )
        shortable_rows = (
            model_masks
            & day_short_open_mask.unsqueeze(1)
            & session_exit_mask.unsqueeze(1)
        )
        def _policy_targets(
            slabs: torch.Tensor,
            masks: torch.Tensor,
            short_masks: torch.Tensor,
            daily_features: torch.Tensor,
        ) -> torch.Tensor:
            with _autocast_context(device, amp_dtype):
                return _batched_slab_targets(
                    model_forward,
                    slabs,
                    masks,
                    short_masks,
                    daily_features,
                ).reshape(day_batch_size, decision_chunk_rows, -1)

        forward_started = time.perf_counter()
        with torch.set_grad_enabled(training):
            if full_day_activation_checkpoint:
                # Exact full-session credit assignment otherwise retains all
                # 15 Transformer chunk activations until forced-close
                # backward. Non-reentrant checkpointing retains the recurrent
                # ledger graph and RNG semantics, but recomputes policy
                # activations chunk-by-chunk during backward. This changes the
                # memory/time tradeoff, not the objective or its gradients.
                targets = activation_checkpoint(
                    _policy_targets,
                    feature_slabs,
                    model_masks,
                    shortable_rows,
                    policy_daily_context,
                    use_reentrant=False,
                    preserve_rng_state=True,
                )
            else:
                targets = _policy_targets(
                    feature_slabs,
                    model_masks,
                    shortable_rows,
                    policy_daily_context,
                )
        timing.forward_s += time.perf_counter() - forward_started
        if record_model_cash:
            requested_gross = targets[:, :real_rows].abs().sum(dim=-1)
            model_requested_cash_parts.append(
                (1.0 - requested_gross).clamp(0.0, 1.0).detach()
            )
        chunk_log_utility = torch.zeros(
            day_batch_size, device=device, dtype=torch.float32
        )
        execution_started = time.perf_counter()
        decision_start = int(selected[0])
        execution_mask_chunk = execution_masks[
            :, decision_start : decision_start + real_rows
        ]
        execution_open_chunk = execution_opens[
            :, decision_start : decision_start + real_rows
        ]
        exit_close_chunk = exit_closes[
            :, decision_start : decision_start + real_rows
        ]
        future_volume_chunk = future_volumes[
            :, decision_start : decision_start + real_rows
        ]
        if real_rows < decision_chunk_rows:
            pad_rows = decision_chunk_rows - real_rows

            def _pad_market_rows(value: torch.Tensor) -> torch.Tensor:
                return torch.cat(
                    [
                        value,
                        torch.zeros(
                            day_batch_size,
                            pad_rows,
                            value.size(2),
                            device=device,
                            dtype=value.dtype,
                        ),
                    ],
                    dim=1,
                )

            execution_mask_chunk = _pad_market_rows(execution_mask_chunk)
            execution_open_chunk = _pad_market_rows(execution_open_chunk)
            exit_close_chunk = _pad_market_rows(exit_close_chunk)
            future_volume_chunk = _pad_market_rows(future_volume_chunk)
        # Never open a position that lacks a verified 13:30 close. Existing
        # positions can still be reduced whenever the next bar is executable.
        entry_mask = execution_mask_chunk & session_exit_mask.unsqueeze(1)
        target_chunk = targets * entry_mask.to(dtype=targets.dtype)
        allow_new_entries = torch.arange(
            decision_start + 1,
            decision_start + decision_chunk_rows + 1,
            device=device,
            dtype=torch.long,
        ).le(int(config.trading.tw_minute_last_entry_minute))
        if real_rows < decision_chunk_rows:
            allow_new_entries[real_rows:] = False
        (
            state_cash,
            state_shares,
            state_last_prices,
            state_equity,
            chunk_log_utility,
            chunk_turnover,
            chunk_fees,
            chunk_slippage,
        ) = execution_forward(
            state.cash,
            state.shares,
            state.last_prices,
            state.equity,
            target_chunk,
            execution_mask_chunk,
            execution_open_chunk,
            exit_close_chunk,
            future_volume_chunk,
            allow_new_entries,
            day_short_open_mask.unsqueeze(1).expand_as(execution_mask_chunk),
            day_short_capacity,
        )
        state = MinuteExecutionState(
            cash=state_cash,
            shares=state_shares,
            last_prices=state_last_prices,
            equity=state_equity,
        )
        total_log_return = total_log_return + chunk_log_utility.detach()
        total_turnover = total_turnover + chunk_turnover.detach()
        total_fees = total_fees + chunk_fees.detach()
        total_slippage = total_slippage + chunk_slippage.detach()

        if is_last_chunk:
            close_valuation = torch.where(
                session_exit_mask,
                session_close,
                state.last_prices,
            )
            close_nav = state.cash + torch.sum(
                state.shares * close_valuation,
                dim=-1,
            )
            session_close_weights = torch.where(
                close_nav.unsqueeze(-1) > 0.0,
                state.shares
                * close_valuation
                / torch.clamp_min(close_nav.unsqueeze(-1), 1e-12),
                torch.zeros_like(state.shares),
            ).detach()
            close = force_close_minute_session(
                state,
                session_close=session_close,
                session_exit_mask=session_exit_mask,
                last_minute_volume_shares=future_volumes[
                    :, int(config.trading.tw_minute_last_decision_minute) - 1
                ],
                sell_fee_rates=sell_fee_rates,
                buy_fee_rates=buy_fee_rates,
                config=execution_config,
            )
            state = close.state
            safe_close_return = torch.clamp(close.net_return, min=-0.999999)
            chunk_log_utility = chunk_log_utility + torch.log1p(safe_close_return)
            total_log_return = total_log_return + torch.log1p(
                safe_close_return.detach()
            )
            total_turnover = total_turnover + close.turnover_notional.detach()
            total_fees = total_fees + close.explicit_fees.detach()
            total_slippage = total_slippage + close.slippage_cost.detach()
        timing.backtest_s += time.perf_counter() - execution_started
        if full_day_credit_assignment:
            full_day_log_utility = full_day_log_utility + chunk_log_utility

        if training and not full_day_credit_assignment:
            if scaler is None:
                raise RuntimeError("tw_minute training requires the canonical GradScaler")
            loss = _minute_chunk_training_loss(
                chunk_log_utility,
                real_days=real_days,
                global_real_days=(
                    real_days if global_real_days is None else global_real_days
                ),
                decisions_per_day=len(decisions),
                gradient_world_size=gradient_world_size,
            )
            # Scanner-free production runs deliberately avoid a scalar
            # device-to-host synchronization on every recurrent chunk. The
            # ledger is finite-by-construction (finite inputs, bounded targets,
            # clamped log1p); CPU oracle/debug runs retain the exact check.
            check_finite_loss = (
                device.type != "cuda"
                or int(config.training.finite_check_interval_steps) > 0
            )
            if check_finite_loss and not torch.isfinite(loss):
                raise RuntimeError(f"non-finite tw_minute training loss: {loss}")
            backward_started = time.perf_counter()
            scaler.scale(loss).backward()
            timing.backward_s += time.perf_counter() - backward_started
        # Evaluation and the legacy truncated-gradient path can release the
        # recurrent graph at every compile chunk.  Full-day credit assignment
        # deliberately keeps cash, shares, prices, and NAV connected until the
        # forced-close loss has been added and one backward pass is issued.
        if not full_day_credit_assignment:
            state = state.detached(clone=False)
        sync_context.__exit__(None, None, None)

    if full_day_credit_assignment:
        if scaler is None:
            raise RuntimeError("tw_minute training requires the canonical GradScaler")
        loss = _minute_chunk_training_loss(
            full_day_log_utility,
            real_days=real_days,
            global_real_days=(
                real_days if global_real_days is None else global_real_days
            ),
            decisions_per_day=len(decisions),
            gradient_world_size=gradient_world_size,
        )
        check_finite_loss = (
            device.type != "cuda"
            or int(config.training.finite_check_interval_steps) > 0
        )
        if check_finite_loss and not torch.isfinite(loss):
            raise RuntimeError(f"non-finite tw_minute full-day loss: {loss}")
        backward_started = time.perf_counter()
        scaler.scale(loss).backward()
        timing.backward_s += time.perf_counter() - backward_started

    full_day_sync_context.__exit__(None, None, None)
    if deferred_ddp_sync:
        # ``no_sync`` intentionally bypassed the reducer for all policy
        # forwards participating in this one full-session graph. Restore the
        # next-forward buffer contract and perform exactly one bucketized
        # averaged-gradient synchronization before clipping/stepping.
        if hasattr(model, "require_forward_param_sync"):
            model.require_forward_param_sync = True
        reduce_started = time.perf_counter()
        _all_reduce_full_day_gradients(
            model,
            bucket_cap_mb=int(config.training.ddp_bucket_cap_mb),
        )
        timing.backward_s += time.perf_counter() - reduce_started

    if training:
        step_started = time.perf_counter()
        if gradient_clip_norm > 0.0:
            scaler.unscale_(optimizer)
            torch.nn.utils.clip_grad_norm_(model.parameters(), gradient_clip_norm)
        scaler.step(optimizer)
        scaler.update()
        if scheduler is not None:
            _step_batch_lr_scheduler(scheduler)
        timing.step_s += time.perf_counter() - step_started

    metrics = torch.stack(
        [
            state.equity.detach(),
            total_log_return,
            total_turnover,
            total_fees,
            total_slippage,
        ]
    ).cpu().numpy()
    if bool(torch.any(state.shares != 0.0).item()):
        raise RuntimeError("tw_minute day batch ended with nonzero inventory")
    close_weights = session_close_weights[:real_days].cpu().numpy()
    model_requested_cash = (
        torch.cat(model_requested_cash_parts, dim=1)[:real_days].cpu().numpy()
        if model_requested_cash_parts
        else None
    )
    rows: list[dict[str, float]] = []
    benchmark_log_returns = cpu_fields["benchmark_log_return"]
    benchmark_price_available = cpu_fields["benchmark_price_available"]
    for day_index, trade_date in enumerate(trade_dates):
        ledger_equity_scale = float(metrics[0, day_index])
        log_return = float(metrics[1, day_index])
        # The differentiable objective accumulates small direct P&L returns in
        # normalized units. Convert to currency once in host Float64 so the
        # report cannot reintroduce NT$10m FP32 cancellation.
        net_return = float(math.expm1(log_return))
        final_equity = float(execution_config.initial_equity) * (1.0 + net_return)
        benchmark_log_return = float(benchmark_log_returns[day_index])
        benchmark_valid = bool(
            benchmark_price_available[day_index]
            and math.isfinite(benchmark_log_return)
        )
        if not benchmark_valid:
            benchmark_log_return = float("nan")
        rows.append(
            {
                "date": str(trade_date.astype("datetime64[D]")),
                "initial_equity": execution_config.initial_equity,
                "final_equity": final_equity,
                "net_return": net_return,
                "log_return": log_return,
                "ledger_equity_scale": ledger_equity_scale,
                "ledger_equity_gap": ledger_equity_scale - (1.0 + net_return),
                "benchmark_log_return": benchmark_log_return,
                "benchmark_available": float(math.isfinite(benchmark_log_return)),
                "turnover_notional": float(metrics[2, day_index]),
                "explicit_fees": float(metrics[3, day_index]),
                "slippage_cost": float(metrics[4, day_index]),
                "decisions": float(len(decisions)),
            }
        )
        if model_requested_cash is not None:
            day_cash = model_requested_cash[day_index]
            rows[-1].update(
                {
                    "mean_model_requested_cash_weight": float(np.mean(day_cash)),
                    "min_model_requested_cash_weight": float(np.min(day_cash)),
                    "max_model_requested_cash_weight": float(np.max(day_cash)),
                    "last_model_requested_cash_weight": float(day_cash[-1]),
                    "mean_model_requested_risky_gross_weight": float(
                        np.mean(1.0 - day_cash)
                    ),
                }
            )
    return rows, close_weights, timing


def _summarize_days(rows: list[dict[str, float]]) -> dict[str, float]:
    if not rows:
        raise RuntimeError("tw_minute split produced no day results")
    returns = np.asarray([row["net_return"] for row in rows], dtype=np.float64)
    log_returns = np.asarray([row["log_return"] for row in rows], dtype=np.float64)
    benchmark_available = np.asarray(
        [row.get("benchmark_available", 1.0) for row in rows],
        dtype=np.float64,
    )
    mean = float(np.mean(returns))
    std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
    return {
        "days": float(len(rows)),
        "mean_daily_return": mean,
        "mean_daily_log_return": float(np.mean(log_returns)),
        "annualized_log_utility": float(np.mean(log_returns) * 252.0),
        "annualized_sharpe": (
            float(mean / std * math.sqrt(252.0)) if std > 0.0 else 0.0
        ),
        "positive_day_fraction": float(np.mean(returns > 0.0)),
        "benchmark_coverage": float(np.mean(benchmark_available > 0.0)),
        "total_turnover_notional": float(
            sum(row["turnover_notional"] for row in rows)
        ),
        "total_explicit_fees": float(sum(row["explicit_fees"] for row in rows)),
        "total_slippage_cost": float(sum(row["slippage_cost"] for row in rows)),
    }


def _minute_backtest_result(
    rows: list[dict[str, float]],
    session_close_weights: np.ndarray,
) -> tuple[BacktestResult, np.ndarray]:
    """Adapt exact minute-day results to the canonical reporting container.

    ``weights_history`` is the realised portfolio immediately before mandatory
    session-close flattening.  ``final_weights`` is zero because the minute
    execution contract must end every day flat.
    """

    dates = np.asarray([row["date"] for row in rows], dtype="datetime64[D]")
    if dates.ndim != 1 or dates.size == 0 or np.any(dates[1:] <= dates[:-1]):
        raise RuntimeError(
            "tw_minute reporting rows must be non-empty and strictly chronological"
        )
    weights = np.asarray(session_close_weights, dtype=np.float64)
    if weights.ndim != 2 or weights.shape[0] != dates.size:
        raise ValueError(
            "tw_minute session-close weights must have shape [days,symbols]"
        )
    initial_equity = np.asarray(
        [row["initial_equity"] for row in rows], dtype=np.float64
    )
    turnovers = np.asarray(
        [row["turnover_notional"] for row in rows], dtype=np.float64
    ) / np.maximum(initial_equity, 1e-12)
    return (
        BacktestResult(
            strategy_returns=np.asarray(
                [row["log_return"] for row in rows], dtype=np.float64
            ),
            benchmark_returns=np.asarray(
                [row["benchmark_log_return"] for row in rows], dtype=np.float64
            ),
            turnovers=turnovers,
            weights_history=weights,
            execution_mode="tw_minute",
            final_weights=np.zeros(weights.shape[1], dtype=np.float64),
        ),
        dates,
    )


def _minute_report_metrics(
    backtest: BacktestResult,
    summary: dict[str, float],
) -> dict[str, float]:
    metrics = compute_metrics(backtest)
    metrics.update(
        {
            f"minute_{name}": float(value)
            for name, value in summary.items()
            if isinstance(value, (int, float))
        }
    )
    return metrics


def _global_session_batches(
    indices: Iterable[int],
    *,
    global_batch_size: int,
    world_size: int,
) -> list[list[int]]:
    """Build chronological global batches with work for every DDP rank.

    A final batch smaller than ``world_size`` would leave a rank without a
    forward/backward call and deadlock DDP.  Distribute all sessions over one
    fewer step in that rare case.  No session is skipped or repeated, and the
    flattened result remains strictly chronological.
    """

    selected = chronological_session_indices(indices)
    if not selected:
        return []
    batch_size = max(1, int(global_batch_size))
    ranks = max(1, int(world_size))
    if ranks == 1:
        return [
            selected[start : start + batch_size]
            for start in range(0, len(selected), batch_size)
        ]
    if batch_size < ranks:
        raise ValueError(
            "tw_minute DDP global batch size must cover every rank: "
            f"batch_size={batch_size} world_size={ranks}"
        )
    if len(selected) < ranks:
        raise ValueError(
            "tw_minute DDP split has fewer sessions than ranks: "
            f"sessions={len(selected)} world_size={ranks}"
        )
    steps = max(1, math.ceil(len(selected) / batch_size))
    steps = min(steps, len(selected) // ranks)
    base, remainder = divmod(len(selected), steps)
    sizes = [base + (1 if position < remainder else 0) for position in range(steps)]
    batches: list[list[int]] = []
    offset = 0
    for size in sizes:
        batch = selected[offset : offset + size]
        if len(batch) < ranks:
            raise RuntimeError("tw_minute DDP batch planner produced an empty rank")
        batches.append(batch)
        offset += size
    if offset != len(selected) or [item for batch in batches for item in batch] != selected:
        raise RuntimeError("tw_minute DDP batch planner changed chronological sessions")
    return batches


def _merge_distributed_day_results(
    rows: list[dict[str, float]],
    close_weights: np.ndarray,
    *,
    gather_close_weights: bool,
) -> tuple[list[dict[str, float]], np.ndarray]:
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return rows, close_weights
    if gather_close_weights and int(close_weights.shape[0]) != len(rows):
        raise ValueError("tw_minute close-weight rows must align with report rows")
    local_payload = [
        (
            str(row["date"]),
            row,
            close_weights[position] if gather_close_weights else None,
        )
        for position, row in enumerate(rows)
    ]
    gathered: list[list[tuple[str, dict[str, float], np.ndarray | None]] | None] = [
        None
    ] * _distributed_world_size()
    torch.distributed.all_gather_object(gathered, local_payload)
    combined = [item for payload in gathered if payload is not None for item in payload]
    combined.sort(key=lambda item: item[0])
    dates = [item[0] for item in combined]
    if dates != sorted(set(dates)):
        raise RuntimeError("tw_minute DDP produced duplicate or unsorted session rows")
    merged_rows = [item[1] for item in combined]
    if not gather_close_weights:
        return merged_rows, np.empty((len(merged_rows), 0), dtype=np.float32)
    merged_close_weights = np.stack([item[2] for item in combined], axis=0)
    return merged_rows, merged_close_weights


def _distributed_max_minute_timing(
    timing: TimingBreakdown,
    *,
    device: torch.device,
) -> TimingBreakdown:
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return timing
    fields = (
        "total_s",
        "fetch_s",
        "transfer_s",
        "batch_prepare_s",
        "forward_s",
        "backward_s",
        "step_s",
        "backtest_s",
    )
    values = torch.tensor(
        [float(getattr(timing, name)) for name in fields],
        device=device,
        dtype=torch.float64,
    )
    torch.distributed.all_reduce(values, op=torch.distributed.ReduceOp.MAX)
    for name, value in zip(fields, values.cpu().tolist()):
        setattr(timing, name, float(value))
    batches = torch.tensor(int(timing.batches), device=device, dtype=torch.int64)
    torch.distributed.all_reduce(batches, op=torch.distributed.ReduceOp.MAX)
    timing.batches = int(batches.item())
    return timing


def _run_split(
    *,
    name: str,
    indices: Iterable[int],
    dataset: MinuteDatasetIndex,
    normalizer: MinuteFeatureNormalizer,
    model: nn.Module,
    model_forward: Callable[
        [torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor], Any
    ],
    daily_context_forward: Callable[[torch.Tensor], torch.Tensor] | None = None,
    execution_forward: Callable[..., tuple[torch.Tensor, ...]],
    day_cache: _MinuteDayCache,
    config: ExperimentConfig,
    execution_config: MinuteExecutionConfig,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    optimizer: torch.optim.Optimizer | None = None,
    scaler: GradScaler | None = None,
    scheduler: torch.optim.lr_scheduler.LRScheduler | None = None,
    progress_lifecycle: TrainingRunLifecycle | _NullTrainingRunLifecycle | None = None,
    gather_close_weights: bool = True,
) -> tuple[dict[str, float], list[dict[str, float]], np.ndarray, TimingBreakdown]:
    rows: list[dict[str, float]] = []
    selected = chronological_session_indices(indices)
    started = time.monotonic()
    global_day_batch_size = int(
        config.training.batch_size_train
        if optimizer is not None
        else config.training.batch_size_eval
    )
    world_size = (
        _distributed_world_size() if _distributed_is_initialized() else 1
    )
    rank = (
        int(torch.distributed.get_rank())
        if _distributed_is_initialized() and world_size > 1
        else 0
    )
    global_batches = _global_session_batches(
        selected,
        global_batch_size=global_day_batch_size,
        world_size=world_size,
    )
    local_day_batch_size = max(
        len(batch[rank::world_size]) for batch in global_batches
    )
    eval_chunk_rows = getattr(
        config.training,
        "minute_eval_decision_chunk_rows",
        None,
    )
    decision_chunk_rows = int(
        config.training.minute_decision_chunk_rows
        if optimizer is not None or eval_chunk_rows is None
        else eval_chunk_rows
    )
    timing = TimingBreakdown()
    close_weight_parts: list[np.ndarray] = []
    day_progress = _progress_bar(
        desc=f" {name}",
        unit="day",
        total=len(selected),
        leave=False,
    )
    prefetched_host_batch: Future[_MinuteHostBatch] | None = None
    if global_batches and hasattr(day_cache, "prefetch_many_stacked"):
        first_batch_indices = global_batches[0][rank::world_size]
        prefetched_host_batch = day_cache.prefetch_many_stacked(
            first_batch_indices,
            padded_days=local_day_batch_size,
        )
    try:
        processed_sessions = 0
        for batch_number, global_batch_indices in enumerate(global_batches, start=1):
            batch_indices = global_batch_indices[rank::world_size]
            global_last_date = str(
                dataset.dates[int(global_batch_indices[-1])].astype("datetime64[D]")
            )
            fetch_started = time.perf_counter()
            if prefetched_host_batch is not None:
                host_batch = prefetched_host_batch.result()
                days = None
                if batch_number < len(global_batches):
                    next_batch_indices = global_batches[batch_number][rank::world_size]
                    prefetched_host_batch = day_cache.prefetch_many_stacked(
                        next_batch_indices,
                        padded_days=local_day_batch_size,
                    )
                else:
                    prefetched_host_batch = None
            elif hasattr(day_cache, "get_many_stacked"):
                host_batch = day_cache.get_many_stacked(
                    batch_indices,
                    padded_days=local_day_batch_size,
                )
                days = None
            else:
                # Lightweight test doubles and external callers keep the
                # historical per-day API; production uses the stacked cache.
                days = day_cache.get_many(batch_indices)
                host_batch = None
            timing.fetch_s += time.perf_counter() - fetch_started
            batch_rows, batch_close_weights, batch_timing = _run_day_batch(
                model=model,
                model_forward=model_forward,
                daily_context_forward=daily_context_forward,
                execution_forward=execution_forward,
                days=days,
                host_batch=host_batch,
                config=config,
                execution_config=execution_config,
                buy_fee_rates=buy_fee_rates,
                sell_fee_rates=sell_fee_rates,
                device=device,
                amp_dtype=amp_dtype,
                day_batch_size=local_day_batch_size,
                decision_chunk_rows=decision_chunk_rows,
                optimizer=optimizer,
                scaler=scaler,
                scheduler=scheduler,
                gradient_clip_norm=float(config.training.grad_clip_norm),
                global_real_days=len(global_batch_indices),
                gradient_world_size=world_size,
            )
            rows.extend(batch_rows)
            close_weight_parts.append(batch_close_weights)
            for field_name in (
                "transfer_s",
                "batch_prepare_s",
                "forward_s",
                "backward_s",
                "step_s",
                "backtest_s",
            ):
                setattr(
                    timing,
                    field_name,
                    float(getattr(timing, field_name))
                    + float(getattr(batch_timing, field_name)),
                )
            timing.batches += int(batch_timing.batches)
            day_progress.set_postfix(
                {
                    # The progress axis is the processed-session count;
                    # batch_date is retained only as a chronological audit.
                    "batch_date": global_last_date,
                    "batch_ret": f"{rows[-1]['net_return']:.6f}",
                    "cache": f"{day_cache.cache_bytes / (1024**3):.2f}GiB",
                    "hit": day_cache.hits,
                    "miss": day_cache.misses,
                    "opt_step": (
                        f"{batch_number}/{len(global_batches)}"
                        if optimizer is not None
                        else "-"
                    ),
                },
                refresh=False,
            )
            processed_sessions += len(global_batch_indices)
            day_progress.update(len(global_batch_indices))
            if progress_lifecycle is not None:
                progress_lifecycle.update_work(
                    completed=processed_sessions,
                    total=len(selected),
                    unit="day",
                    last_sample=global_last_date,
                    metrics={
                        "last_return": rows[-1]["net_return"],
                        "cache_gib": day_cache.cache_bytes / (1024**3),
                        "cache_hits": day_cache.hits,
                        "cache_misses": day_cache.misses,
                    },
                )
    finally:
        day_progress.close()
    close_weights = np.concatenate(close_weight_parts, axis=0)
    rows, close_weights = _merge_distributed_day_results(
        rows,
        close_weights,
        gather_close_weights=bool(gather_close_weights),
    )
    # The aligned daily value on session t is close[t-1] -> close[t].  A
    # standalone split starts at its first close, so exclude the preceding
    # split's final close by setting only this split's first row to zero.
    if rows:
        first_index = int(selected[0])
        daily_feature_context = getattr(dataset, "daily_feature_context", None)
        first_price_available = bool(
            daily_feature_context is not None
            and daily_feature_context.benchmark_price_available[first_index]
        )
        rows[0]["benchmark_log_return"] = (
            0.0 if first_price_available else float("nan")
        )
        rows[0]["benchmark_available"] = float(first_price_available)
    summary = _summarize_days(rows)
    timing.total_s = time.monotonic() - started
    summary["elapsed_seconds"] = timing.total_s
    timing = _distributed_max_minute_timing(timing, device=device)
    return summary, rows, close_weights, timing


def _minute_checkpoint_state(
    *,
    dataset: MinuteDatasetIndex,
    normalizer: MinuteFeatureNormalizer,
    config: ExperimentConfig,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "execution_contract_version": MINUTE_EXECUTION_CONTRACT_VERSION,
        "training_contract_version": MINUTE_TRAINING_CONTRACT_VERSION,
        "execution_mode": "tw_minute",
        "sample_order_contract": training_mode_spec(
            config.trading.execution_mode
        ).sample_order_contract,
        "dataset_fingerprint": _minute_dataset_fingerprint(dataset),
        "configuration_fingerprint": _minute_configuration_fingerprint(config),
        "symbols": list(dataset.symbols),
        "feature_names": list(MINUTE_FEATURE_COLUMNS),
        "normalizer_mean": normalizer.mean.tolist(),
        "normalizer_scale": normalizer.scale.tolist(),
        "normalizer_counts": normalizer.counts.tolist(),
    }


def _validate_minute_checkpoint(
    payload: dict[str, Any],
    *,
    dataset: MinuteDatasetIndex,
    config: ExperimentConfig,
) -> MinuteFeatureNormalizer:
    state = payload.get("tw_minute_state")
    if not isinstance(state, dict) or state.get("schema_version") != 1:
        raise RuntimeError("checkpoint lacks the tw_minute state contract")
    if state.get("execution_contract_version") != MINUTE_EXECUTION_CONTRACT_VERSION:
        raise RuntimeError("incompatible tw_minute execution contract")
    if state.get("training_contract_version") != MINUTE_TRAINING_CONTRACT_VERSION:
        raise RuntimeError("incompatible tw_minute optimizer/training contract")
    if state.get("sample_order_contract") != "chronological_sessions":
        raise RuntimeError("incompatible tw_minute session ordering contract")
    if state.get("dataset_fingerprint") != _minute_dataset_fingerprint(dataset):
        raise RuntimeError("tw_minute checkpoint dataset fingerprint changed")
    if state.get("configuration_fingerprint") != _minute_configuration_fingerprint(
        config
    ):
        raise RuntimeError("tw_minute checkpoint configuration contract changed")
    if state.get("symbols") != list(dataset.symbols):
        raise RuntimeError("tw_minute checkpoint symbol universe changed")
    if state.get("feature_names") != list(MINUTE_FEATURE_COLUMNS):
        raise RuntimeError("tw_minute checkpoint feature schema changed")
    return MinuteFeatureNormalizer(
        mean=np.asarray(state["normalizer_mean"], dtype=np.float32),
        scale=np.asarray(state["normalizer_scale"], dtype=np.float32),
        counts=np.asarray(state["normalizer_counts"], dtype=np.int64),
    )


def _folds_for_run(
    folds: list[WalkForwardFold],
    *,
    start_fold: int | None,
    max_folds: int | None,
) -> list[WalkForwardFold]:
    selected = [fold for fold in folds if start_fold is None or fold.fold_id >= start_fold]
    if max_folds is not None:
        selected = selected[: max(0, int(max_folds))]
    if not selected:
        raise ValueError("no tw_minute folds selected")
    return selected


def _first_calendar_year_indices(
    dates: np.ndarray,
    indices: Iterable[int],
) -> tuple[int, np.ndarray]:
    """Return the chronological subset owned by the first calendar year."""

    selected = np.asarray(
        chronological_session_indices(indices),
        dtype=np.int64,
    )
    if selected.size == 0:
        raise ValueError("tw_minute test split is empty")
    date_values = np.asarray(dates)[selected]
    years = (
        date_values.astype("datetime64[Y]").astype(np.int64) + 1970
    )
    first_year = int(years.min())
    first_year_indices = selected[years == first_year]
    if first_year_indices.size == 0:
        raise RuntimeError("tw_minute first test-year selection is empty")
    return first_year, first_year_indices


def _load_completed_minute_fold_result(
    root: Path,
    fold: WalkForwardFold,
    *,
    dataset: MinuteDatasetIndex,
    config: ExperimentConfig,
    experiment_manifest: dict[str, Any],
) -> FoldResult | None:
    contract_path = root / f"fold_{fold.fold_id:02d}" / "mode_artifact_contract.json"
    if not contract_path.is_file():
        return None
    try:
        actual = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if actual != _minute_artifact_contract(dataset, config):
        return None
    return _load_completed_fold_result(
        root,
        fold.fold_id,
        expected_manifest=experiment_manifest,
        expected_fold=fold,
    )


def _run_minute_training_impl(
    config: ExperimentConfig,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
    lifecycle: TrainingRunLifecycle | _NullTrainingRunLifecycle,
) -> list[dict[str, Any]]:
    """Train or infer the dedicated causal full-market one-minute contract."""

    if config.trading.execution_mode != "tw_minute":
        raise ValueError("run_minute_training requires execution_mode='tw_minute'")
    distributed_requested = active_strategy == "distributed_data_parallel"
    if active_strategy not in {
        "none",
        "single",
        "single_gpu",
        "distributed_data_parallel",
    }:
        raise ValueError(
            "tw_minute supports only single-device or distributed_data_parallel"
        )
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    layout = TrainingArtifactLayout(root)
    setup_timing = _PreEpochTimingRecorder(
        root / "minute_setup_timing.jsonl",
        train_years=[],
        fold_ids=[],
        scope="tw_minute_setup",
    )
    _progress(
        "[tw-minute setup] loading research dataset index "
        f"root={config.data.minute_parquet_root} "
        f"verify_sha256={bool(config.data.minute_verify_partition_sha256)}"
    )
    dataset: MinuteDatasetIndex | None = None
    dataset_error: BaseException | None = None
    require_short_rules = not bool(config.trading.long_only)
    short_rule_path = (
        config.data.tw_public_feature_path if require_short_rules else None
    )
    daily_context_feature_names = (
        minute_static_daily_context_feature_names(config.data.feature_include)
        if config.data.minute_daily_context_panel_meta is not None
        else ()
    )
    rank0_verifies = distributed_requested and _distributed_world_size() > 1
    if not rank0_verifies or _distributed_is_rank0():
        try:
            dataset = load_minute_dataset_index(
                config.data.minute_parquet_root,
                require_research_ready=bool(config.data.minute_require_research_ready),
                verify_partition_sha256=bool(
                    config.data.minute_verify_partition_sha256
                ),
                progress=_progress,
                short_rule_path=short_rule_path,
                require_short_rules=require_short_rules,
                daily_context_panel_meta=(
                    config.data.minute_daily_context_panel_meta
                ),
                daily_context_feature_names=(
                    daily_context_feature_names
                ),
                daily_context_lookback=int(
                    config.training.financial_transformer.daily_context_lookback
                ),
                benchmark_name=str(config.data.benchmark_name),
            )
        except BaseException as exc:
            dataset_error = exc
    _raise_if_distributed_phase_failed("tw_minute_dataset_verify", dataset_error)
    if rank0_verifies and not _distributed_is_rank0():
        try:
            dataset = load_minute_dataset_index(
                config.data.minute_parquet_root,
                require_research_ready=bool(config.data.minute_require_research_ready),
                verify_partition_sha256=False,
                progress=None,
                short_rule_path=short_rule_path,
                require_short_rules=require_short_rules,
                daily_context_panel_meta=(
                    config.data.minute_daily_context_panel_meta
                ),
                daily_context_feature_names=(
                    daily_context_feature_names
                ),
                daily_context_lookback=int(
                    config.training.financial_transformer.daily_context_lookback
                ),
                benchmark_name=str(config.data.benchmark_name),
            )
        except BaseException as exc:
            dataset_error = exc
    _raise_if_distributed_phase_failed("tw_minute_dataset_worker_load", dataset_error)
    if dataset is None:
        raise RuntimeError("tw_minute dataset loading completed without an index")
    if require_short_rules and (
        dataset.short_open_mask is None
        or dataset.short_capacity_shares is None
        or not dataset.short_rules_fingerprint
    ):
        raise RuntimeError("tw_minute long/short short-rule sidecar is incomplete")
    mode_spec = training_mode_spec(config.trading.execution_mode)
    for manifest_key, expected in (
        ("decision_clock", mode_spec.decision_clock),
        ("execution_clock", mode_spec.execution_clock),
    ):
        actual = str(dataset.manifest.get(manifest_key, ""))
        if actual != expected:
            raise RuntimeError(
                f"tw_minute dataset {manifest_key}={actual!r} differs from "
                f"registered mode contract {expected!r}"
            )
    benchmark_name = str(config.data.benchmark_name)
    if benchmark_name not in dataset.symbols:
        raise RuntimeError(
            f"tw_minute benchmark {benchmark_name!r} is absent from the dataset"
        )
    if (
        dataset.daily_feature_context is not None
        and dataset.daily_feature_context.benchmark_name != benchmark_name
    ):
        raise RuntimeError(
            "tw_minute daily-context benchmark does not match configuration"
        )
    setup_timing.checkpoint(
        "dataset_index_and_partition_verification",
        dates=int(len(dataset.dates)),
        symbols=int(dataset.num_symbols),
        product_family=mode_spec.product_family,
        frequency=mode_spec.frequency,
        benchmark=benchmark_name,
    )
    validate_walk_forward_year_contract(
        dataset.dates,
        expected_first_year=config.walk_forward.expected_first_year,
        require_contiguous_years=config.walk_forward.require_contiguous_years,
    )
    folds = build_expanding_year_folds(
        dates=dataset.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    )
    selected_folds = _folds_for_run(
        folds, start_fold=start_fold, max_folds=max_folds
    )
    next_fold_by_id = {
        folds[index].fold_id: folds[index + 1]
        for index in range(len(folds) - 1)
    }
    setup_timing.checkpoint(
        "walk_forward_fold_construction",
        selected_folds=[int(fold.fold_id) for fold in selected_folds],
    )
    requested_device = _resolve_device(config)
    distributed = _resolve_distributed_data_parallel(config, requested_device)
    device = (
        torch.device("cuda", torch.cuda.current_device())
        if distributed
        else requested_device
    )
    if distributed:
        for label, batch_size in (
            ("batch_size_train", config.training.batch_size_train),
            ("batch_size_eval", config.training.batch_size_eval),
        ):
            if int(batch_size) < _distributed_world_size():
                raise ValueError(
                    f"tw_minute {label} must be at least DDP world size: "
                    f"batch_size={batch_size} world_size={_distributed_world_size()}"
                )
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    execution_config = _execution_config(config)
    buy_rates_np, sell_rates_np = effective_fee_rate_vectors(
        dataset.symbols, "tw_day_trade", fee_schedule=_fee_schedule(config)
    )
    buy_rates = torch.from_numpy(buy_rates_np.astype(np.float32)).to(device=device)
    sell_rates = torch.from_numpy(sell_rates_np.astype(np.float32)).to(device=device)
    setup_timing.checkpoint(
        "device_and_execution_contract",
        device=str(device),
        distributed=bool(distributed),
        world_size=int(_distributed_world_size()),
        global_batch_size_train=int(config.training.batch_size_train),
        global_batch_size_eval=int(config.training.batch_size_eval),
    )
    experiment_manifest = {
        "schema_version": 1,
        "execution_mode": "tw_minute",
        "execution_contract_version": MINUTE_EXECUTION_CONTRACT_VERSION,
        "training_contract_version": MINUTE_TRAINING_CONTRACT_VERSION,
        "sample_order_contract": mode_spec.sample_order_contract,
        "dataset_fingerprint": _minute_dataset_fingerprint(dataset),
        "configuration_fingerprint": _minute_configuration_fingerprint(config),
    }
    lifecycle.start(
        fold_ids=[fold.fold_id for fold in selected_folds],
        dataset_fingerprint=_minute_dataset_fingerprint(dataset),
        configuration_fingerprint=_minute_configuration_fingerprint(config),
        contract_versions={
            "execution": MINUTE_EXECUTION_CONTRACT_VERSION,
            "training": MINUTE_TRAINING_CONTRACT_VERSION,
        },
        data_summary={
            "benchmark_name": benchmark_name,
            "benchmark_contract": MINUTE_BENCHMARK_CONTRACT,
            "benchmark_fingerprint": (
                None
                if dataset.daily_feature_context is None
                else dataset.daily_feature_context.benchmark_fingerprint
            ),
            "symbols": dataset.num_symbols,
            "dates": len(dataset.dates),
            "source_gap_symbols": dataset.manifest.get("source_gap_symbols", 0),
            "contract_unavailable_symbols": dataset.manifest.get(
                "contract_unavailable_symbols", 0
            ),
            "minute_feature_count": len(MINUTE_FEATURE_COLUMNS),
            "minute_microstructure_feature_count": len(
                MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS
            ),
            "minute_developing_candle_feature_count": len(
                MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
            ),
            "daily_context_feature_count": (
                0
                if dataset.daily_feature_context is None
                else dataset.daily_feature_context.num_features
            ),
            "daily_context_lookback": int(dataset.daily_context_lookback),
        },
        configuration=asdict(config),
        mode_details={
            "dataset_decision_clock": dataset.manifest["decision_clock"],
            "dataset_execution_clock": dataset.manifest["execution_clock"],
            "benchmark_name": benchmark_name,
            "benchmark_contract": MINUTE_BENCHMARK_CONTRACT,
            "minute_feature_names": list(MINUTE_FEATURE_COLUMNS),
            "minute_microstructure_feature_names": list(
                MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS
            ),
            "minute_developing_candle_feature_names": list(
                MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
            ),
            "daily_context_contract": (
                None
                if dataset.daily_feature_context is None
                else MINUTE_DAILY_CONTEXT_CONTRACT
            ),
            "daily_context_feature_names": (
                []
                if dataset.daily_feature_context is None
                else list(dataset.daily_feature_context.feature_names)
            ),
            "daily_context_lookback": int(dataset.daily_context_lookback),
        },
        compatibility_manifest_paths=[root / "minute_run_manifest.json"],
    )
    results: list[dict[str, Any]] = []
    results_by_fold: dict[int, FoldResult] = {}
    fold_progress = _progress_bar(
        selected_folds,
        desc="Train groups" if mode == "train" else "Inference folds",
        unit="group" if mode == "train" else "fold",
    )
    for group_index, fold in enumerate(fold_progress, start=1):
        train_years = [int(year) for year in fold.train_years]
        first_test_year, first_test_year_indices = _first_calendar_year_indices(
            dataset.dates,
            fold.test_indices,
        )
        group_name = layout.year_group_name(train_years)
        lifecycle.start_group(
            group_name=group_name,
            group_index=group_index,
            group_total=len(selected_folds),
            fold_ids=[fold.fold_id],
            epoch_total=int(config.training.epochs),
            message=f"Train {train_years}",
        )
        fold_dir = layout.fold_dir(fold.fold_id)
        completed = (
            _load_completed_minute_fold_result(
                root,
                fold,
                dataset=dataset,
                config=config,
                experiment_manifest=experiment_manifest,
            )
            if mode == "train" and resume
            else None
        )
        if completed is not None:
            results_by_fold[fold.fold_id] = completed
            results.append(
                {
                    "status": "complete",
                    "execution_mode": "tw_minute",
                    **asdict(completed),
                }
            )
            _progress(f"[Train {train_years}] fold {fold.fold_id} already complete; skipping")
            lifecycle.finish_fold(fold.fold_id)
            continue
        fold_dir.mkdir(parents=True, exist_ok=True)
        group_dir = layout.group_dir(group_name)
        group_dir.mkdir(parents=True, exist_ok=True)
        group_checkpoint_path = layout.group_checkpoint_path(group_name)
        group_curve_path = layout.group_curve_path(group_name)
        best_path = layout.best_checkpoint_path(fold.fold_id)
        pre_epoch_timing = _PreEpochTimingRecorder(
            group_dir / "pre_epoch_timing.jsonl",
            train_years=train_years,
            fold_ids=[fold.fold_id],
            scope="tw_minute_train_group",
        )
        _progress("\n" + "=" * 80)
        _progress(
            f"[Train {train_years}] execution_mode=tw_minute "
            f"fold={fold.fold_id} val={fold.val_years} test={fold.test_years}"
        )
        _progress("=" * 80)

        normalizer = dataset.fit_normalizer(fold.train_indices)
        pre_epoch_timing.checkpoint(
            "normalizer_fit",
            train_days=int(len(fold.train_indices)),
        )
        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(MINUTE_FEATURE_COLUMNS),
            num_symbols=dataset.num_symbols,
            feature_names=MINUTE_FEATURE_COLUMNS,
            daily_context_feature_names=(
                None
                if dataset.daily_feature_context is None
                else dataset.daily_feature_context.feature_names
            ),
        ).to(device)
        frozen_inactive_parameters = _freeze_inactive_minute_parameters(model)
        if frozen_inactive_parameters:
            _progress(
                f"[Train {train_years}] frozen inactive parameters="
                f"{','.join(frozen_inactive_parameters)}"
            )
        pre_epoch_timing.checkpoint(
            "model_build",
            parameters=int(sum(parameter.numel() for parameter in model.parameters())),
            trainable_parameters=int(
                sum(
                    parameter.numel()
                    for parameter in model.parameters()
                    if parameter.requires_grad
                )
            ),
        )
        optimizer = _create_adamw_optimizer(
            model,
            config,
            device,
            label=f"Train {train_years}",
        )
        scaler = GradScaler(
            enabled=device.type == "cuda" and amp_dtype == torch.float16
        )
        steps_per_epoch = len(
            _global_session_batches(
                fold.train_indices,
                global_batch_size=int(config.training.batch_size_train),
                world_size=(
                    _distributed_world_size() if distributed else 1
                ),
            )
        )
        (
            scheduler,
            scheduler_name,
            scheduler_requires_metric,
            scheduler_interval,
        ) = _create_lr_scheduler(
            optimizer, config, steps_per_epoch=max(1, steps_per_epoch)
        )
        if scheduler is not None:
            _progress(
                f"[Train {train_years}] lr_scheduler={scheduler_name} "
                f"interval={scheduler_interval} steps_per_epoch={steps_per_epoch}"
            )
        start_epoch = 1
        best_val_loss = float("inf")
        no_improve = 0
        resume_rng_state: dict[str, Any] | None = None
        if resume and group_checkpoint_path.is_file():
            restored = _load_checkpoint(group_checkpoint_path)
            normalizer = _validate_minute_checkpoint(
                restored,
                dataset=dataset,
                config=config,
            )
            _load_state_dict(model, restored["model_state_dict"])
            if mode == "train":
                optimizer.load_state_dict(restored["optimizer_state_dict"])
                scaler_state = restored.get("scaler_state_dict")
                if scaler_state:
                    scaler.load_state_dict(scaler_state)
                scheduler_state = restored.get("scheduler_state_dict")
                if scheduler is not None and scheduler_state:
                    scheduler.load_state_dict(scheduler_state)
                resume_rng_state = restored.get("rng_state")
                start_epoch = int(restored.get("epoch", 0)) + 1
                no_improve = int(restored.get("no_improve_epochs", 0))
                best_val_loss = float(restored.get("best_val_loss", float("inf")))
                _progress(
                    f"[Train {train_years}] resumed from epoch {start_epoch} "
                    f"(checkpoint={group_checkpoint_path})"
                )

        if not hasattr(model, "forward_from_batched_panel_slabs"):
            raise RuntimeError(
                "tw_minute requires a model with forward_from_batched_panel_slabs"
            )
        if dataset.daily_feature_context is not None and not hasattr(
            model, "forward_from_batched_panel_slabs_with_daily_context"
        ):
            raise RuntimeError(
                "tw_minute daily context requires a model with separate "
                "daily-context projection"
            )
        preencode_daily_context = bool(
            dataset.daily_feature_context is not None
            and config.training.minute_full_day_credit_assignment
        )
        slab_forward_module: nn.Module = _MinuteSlabForwardAdapter(
            model,
            execution_config,
            daily_context_is_encoded=preencode_daily_context,
        ).to(device)

        class _DailyContextForwardAdapter(nn.Module):
            def __init__(self, inner_model: nn.Module) -> None:
                super().__init__()
                self.inner_model = inner_model

            def forward(self, features: torch.Tensor) -> torch.Tensor:
                return self.inner_model.encode_daily_context_history(features)

        daily_context_forward_module: nn.Module | None = (
            _DailyContextForwardAdapter(model).to(device)
            if preencode_daily_context
            else None
        )

        def eager_execution_forward(
            cash: torch.Tensor,
            shares: torch.Tensor,
            last_prices: torch.Tensor,
            equity: torch.Tensor,
            targets: torch.Tensor,
            execution_masks: torch.Tensor,
            execution_opens: torch.Tensor,
            exit_closes: torch.Tensor,
            future_volumes: torch.Tensor,
            allow_new_entries: torch.Tensor,
            short_open_masks: torch.Tensor,
            short_capacity_shares: torch.Tensor,
        ) -> tuple[torch.Tensor, ...]:
            return _execute_minute_chunk(
                cash,
                shares,
                last_prices,
                equity,
                targets,
                execution_masks,
                execution_opens,
                exit_closes,
                future_volumes,
                allow_new_entries,
                short_open_masks,
                short_capacity_shares,
                buy_fee_rates=buy_rates,
                sell_fee_rates=sell_rates,
                config=execution_config,
                stateful_proximal_allocator=bool(
                    config.training.minute_stateful_proximal_allocator
                ),
                proximal_cost_multiplier=float(
                    config.training.minute_proximal_cost_multiplier
                ),
            )

        execution_forward: Callable[..., tuple[torch.Tensor, ...]] = (
            eager_execution_forward
        )
        model_compile_status = "off:eager"
        if bool(config.training.enable_torch_compile):
            can_compile, compile_reason = _can_enable_torch_compile(device)
            if not can_compile:
                if bool(config.training.strict_no_fallback):
                    raise RuntimeError(
                        "tw_minute torch.compile requested but unavailable; "
                        f"strict_no_fallback=true: {compile_reason}"
                    )
                model_compile_status = f"skipped:{compile_reason}"
            else:
                try:
                    compile_mode = str(
                        getattr(config.training, "torch_compile_mode", "default")
                        or "default"
                    )
                    slab_forward_module = torch.compile(
                        slab_forward_module,
                        fullgraph=True,
                        dynamic=False,
                        options=_torch_compile_options(
                            compile_mode,
                            cudagraphs=False,
                        ),
                    )
                    if daily_context_forward_module is not None:
                        daily_context_forward_module = torch.compile(
                            daily_context_forward_module,
                            fullgraph=True,
                            dynamic=False,
                            options=_torch_compile_options(
                                compile_mode,
                                cudagraphs=False,
                            ),
                        )
                    execution_forward = torch.compile(
                        eager_execution_forward,
                        fullgraph=True,
                        dynamic=False,
                        options=_torch_compile_options(
                            compile_mode,
                            cudagraphs=False,
                        ),
                    )
                    model_compile_status = (
                        "enabled:fullgraph:fixed_shape:model+daily_context+minute_ledger"
                    )
                    _progress(
                        f"[Train {train_years}] torch.compile enabled "
                        f"(mode={compile_mode}, fullgraph=True, dynamic=False, "
                        "cudagraphs=False)"
                    )
                except Exception as exc:
                    if bool(config.training.strict_no_fallback):
                        raise RuntimeError(
                            "tw_minute torch.compile failed; "
                            "strict_no_fallback=true"
                        ) from exc
                    model_compile_status = "fallback:eager"
                    _progress(
                        f"[Train {train_years}] torch.compile failed; "
                        f"using eager model: {type(exc).__name__}: {exc}"
                    )
        if distributed:
            slab_forward_module = _wrap_distributed_data_parallel_model(
                slab_forward_module,
                config=config,
                device=device,
                # Every optimizer step intentionally contains multiple
                # no_sync forward/backward pairs followed by one synchronized
                # pair. PyTorch's static-graph first-iteration reducer asserts
                # when its initial hooks are suppressed this way.
                static_graph=False,
            )
            model_compile_status += ":ddp"

        model_forward: Callable[..., Any] = slab_forward_module
        def daily_context_forward(features: torch.Tensor) -> torch.Tensor:
            if daily_context_forward_module is None:
                raise RuntimeError("tw_minute daily-context encoder is unavailable")
            return daily_context_forward_module(features)

        runtime_model = slab_forward_module
        pre_epoch_timing.checkpoint(
            "optimizer_scheduler_resume_and_compile",
            start_epoch=int(start_epoch),
            scheduler=str(scheduler_name),
            model_compile_status=model_compile_status,
        )
        cache_limit_bytes = (
            int(float(config.training.minute_cpu_cache_gb) * 1024**3)
            if bool(config.training.minute_cache_days_on_cpu)
            else 0
        )
        day_cache = _MinuteDayCache(
            dataset,
            normalizer,
            maximum_bytes=cache_limit_bytes,
            workers=int(config.training.minute_data_workers),
            feature_storage_dtype=(
                amp_dtype
                if bool(config.training.cache_train_features_in_amp_dtype)
                and device.type == "cuda"
                and amp_dtype in {torch.float16, torch.bfloat16}
                else None
            ),
        )
        rank_count = max(1, int(_distributed_world_size()))
        per_rank_train_batch_capacity = math.ceil(
            int(config.training.batch_size_train) / rank_count
        )
        flat_train_model_rows_per_rank = int(
            per_rank_train_batch_capacity
            * int(config.training.minute_decision_chunk_rows)
        )
        _progress(
            f"[Train {train_years}] minute vectorization: "
            f"days_per_train_batch={config.training.batch_size_train} "
            f"days_per_eval_batch={config.training.batch_size_eval} "
            f"decisions_per_chunk={config.training.minute_decision_chunk_rows} "
            f"flat_train_rows_per_rank={flat_train_model_rows_per_rank} "
            f"optimizer_steps_per_epoch={steps_per_epoch} "
            f"cpu_cache_limit={cache_limit_bytes / (1024 ** 3):.1f}GiB"
        )

        record_epoch_curve = bool(getattr(config.training, "record_epoch_curve", True))
        curve_lifecycle = _EpochCurveLifecycle(
            group_curve_path,
            enabled=record_epoch_curve,
            interval=int(config.training.curve_plot_interval),
            async_enabled=bool(config.training.curve_plot_async),
            defer_until_end=bool(
                getattr(config.training, "defer_epoch_curve_plot_until_end", True)
            ),
            writer_enabled=bool(_distributed_should_write()),
        )
        curve_lifecycle.prepare_resume(start_epoch)
        if curve_lifecycle.defer_until_end and record_epoch_curve:
            _progress(
                f"[Train {train_years}] epoch curve plotting deferred until "
                "training completes"
            )
        curve_plot_request_interval = curve_lifecycle.interval
        val_interval = max(1, int(config.training.val_interval_epochs))
        test_interval = max(1, int(config.training.curve_test_interval))
        epoch_test_curve = bool(config.training.epoch_test_curve)
        patience = int(
            math.ceil(
                int(config.training.epochs)
                * max(0.0, float(config.training.early_stopping_no_improve_ratio))
            )
        )
        _progress(
            f"[Train {train_years}] optimization status: "
            f"model_compile={model_compile_status}; amp={config.environment.amp_dtype}; "
            f"grad_scaler={scaler.is_enabled()}; record_epoch_curve={record_epoch_curve}; "
            f"validation_interval={val_interval}; "
            f"first_test_year={first_test_year}; "
            f"first_test_days={len(first_test_year_indices)}; "
            f"test_interval={test_interval}; "
            f"epoch_test_curve={epoch_test_curve}; "
            f"curve_plot_async={bool(config.training.curve_plot_async)}; "
            f"curve_plot_interval={curve_plot_request_interval}"
        )
        if resume_rng_state is not None:
            restored_rng = _restore_rng_state(resume_rng_state)
            _progress(
                f"[Train {train_years}] checkpoint RNG "
                f"{'restored' if restored_rng else 'incomplete; configured seed retained'}"
            )
        pre_epoch_timing.checkpoint("pre_epoch_total", next_epoch=int(start_epoch))

        if mode == "train":
            epoch_progress = _progress_bar(
                range(start_epoch, int(config.training.epochs) + 1),
                desc=" Epochs",
                unit="epoch",
            )
            for epoch in epoch_progress:
                epoch_started = time.perf_counter()
                train_cache_hits_before = day_cache.hits
                train_cache_misses_before = day_cache.misses
                lifecycle.set_phase(
                    "training",
                    fold_id=fold.fold_id,
                    epoch=epoch,
                    epoch_total=int(config.training.epochs),
                    work_total=len(fold.train_indices),
                    work_unit="day",
                    message=f"Train {train_years} epoch {epoch}",
                )
                train_summary, _, _, train_timing = _run_split(
                    name=f"Train {train_years} epoch {epoch} train",
                    indices=fold.train_indices,
                    dataset=dataset,
                    normalizer=normalizer,
                    model=runtime_model,
                    model_forward=model_forward,
                    daily_context_forward=(
                        daily_context_forward
                        if daily_context_forward_module is not None
                        else None
                    ),
                    execution_forward=execution_forward,
                    day_cache=day_cache,
                    config=config,
                    execution_config=execution_config,
                    buy_fee_rates=buy_rates,
                    sell_fee_rates=sell_rates,
                    device=device,
                    amp_dtype=amp_dtype,
                    optimizer=optimizer,
                    scaler=scaler,
                    scheduler=(scheduler if scheduler_interval == "step" else None),
                    progress_lifecycle=lifecycle,
                    gather_close_weights=False,
                )
                train_cache_hits = day_cache.hits - train_cache_hits_before
                train_cache_misses = day_cache.misses - train_cache_misses_before
                train_loss = -float(train_summary["mean_daily_log_return"])
                should_validate = (
                    epoch == start_epoch
                    or epoch % val_interval == 0
                    or epoch == int(config.training.epochs)
                )
                val_summary: dict[str, float] | None = None
                val_rows: list[dict[str, float]] = []
                val_timing = TimingBreakdown()
                val_loss: float | None = None
                improved = False
                val_cache_hits = 0
                val_cache_misses = 0
                if should_validate:
                    val_cache_hits_before = day_cache.hits
                    val_cache_misses_before = day_cache.misses
                    lifecycle.set_phase(
                        "validation",
                        fold_id=fold.fold_id,
                        epoch=epoch,
                        epoch_total=int(config.training.epochs),
                        work_total=len(fold.val_indices),
                        work_unit="day",
                        message=f"Validate {train_years} epoch {epoch}",
                    )
                    val_summary, val_rows, _, val_timing = _run_split(
                        name=f"Train {train_years} epoch {epoch} validation",
                        indices=fold.val_indices,
                        dataset=dataset,
                        normalizer=normalizer,
                        model=runtime_model,
                        model_forward=model_forward,
                        daily_context_forward=(
                            daily_context_forward
                            if daily_context_forward_module is not None
                            else None
                        ),
                        execution_forward=execution_forward,
                        day_cache=day_cache,
                        config=config,
                        execution_config=execution_config,
                        buy_fee_rates=buy_rates,
                        sell_fee_rates=sell_rates,
                        device=device,
                        amp_dtype=amp_dtype,
                        progress_lifecycle=lifecycle,
                        gather_close_weights=False,
                    )
                    val_cache_hits = day_cache.hits - val_cache_hits_before
                    val_cache_misses = day_cache.misses - val_cache_misses_before
                    val_loss = -float(val_summary["mean_daily_log_return"])
                    improved = val_loss < best_val_loss - float(
                        config.training.early_stopping_min_delta
                    )
                    if improved:
                        best_val_loss = val_loss
                        no_improve = 0
                    else:
                        no_improve += 1
                # Test is an audit-only curve. It must never select a model,
                # change early stopping, or feed the learning-rate scheduler.
                # Bound the per-epoch cost and look-ahead surface to the first
                # calendar year owned by this fold's test interval.
                should_test = epoch_test_curve and (
                    epoch == start_epoch
                    or epoch % test_interval == 0
                    or epoch == int(config.training.epochs)
                )
                test_summary: dict[str, float] | None = None
                test_timing = TimingBreakdown()
                test_loss: float | None = None
                test_cache_hits = 0
                test_cache_misses = 0
                if should_test:
                    test_cache_hits_before = day_cache.hits
                    test_cache_misses_before = day_cache.misses
                    lifecycle.set_phase(
                        "testing",
                        fold_id=fold.fold_id,
                        epoch=epoch,
                        epoch_total=int(config.training.epochs),
                        work_total=len(first_test_year_indices),
                        work_unit="day",
                        message=(
                            f"Test {first_test_year} {train_years} epoch {epoch}"
                        ),
                    )
                    test_summary, _, _, test_timing = _run_split(
                        name=(
                            f"Train {train_years} epoch {epoch} "
                            f"test {first_test_year}"
                        ),
                        indices=first_test_year_indices,
                        dataset=dataset,
                        normalizer=normalizer,
                        model=runtime_model,
                        model_forward=model_forward,
                        daily_context_forward=(
                            daily_context_forward
                            if daily_context_forward_module is not None
                            else None
                        ),
                        execution_forward=execution_forward,
                        day_cache=day_cache,
                        config=config,
                        execution_config=execution_config,
                        buy_fee_rates=buy_rates,
                        sell_fee_rates=sell_rates,
                        device=device,
                        amp_dtype=amp_dtype,
                        progress_lifecycle=lifecycle,
                        gather_close_weights=False,
                    )
                    test_cache_hits = day_cache.hits - test_cache_hits_before
                    test_cache_misses = day_cache.misses - test_cache_misses_before
                    test_loss = -float(test_summary["mean_daily_log_return"])
                scheduler_started = time.perf_counter()
                if scheduler is not None and scheduler_interval != "step":
                    if scheduler_requires_metric:
                        if val_loss is not None:
                            scheduler.step(val_loss)
                    else:
                        scheduler.step()
                scheduler_s = time.perf_counter() - scheduler_started
                checkpoint_state = _minute_checkpoint_state(
                    dataset=dataset,
                    normalizer=normalizer,
                    config=config,
                )
                checkpoint_started = time.perf_counter()
                if improved and val_summary is not None:
                    _save_fold_checkpoint(
                        best_path,
                        fold=fold,
                        epoch=epoch,
                        best_val_loss=best_val_loss,
                        model=model,
                        optimizer=optimizer,
                        scaler=scaler,
                        experiment_manifest=experiment_manifest,
                        extra_payload={"tw_minute_state": checkpoint_state},
                        check_finite=bool(config.training.checkpoint_finite_check),
                    )
                    if _distributed_should_write():
                        pl.DataFrame(val_rows).write_parquet(
                            fold_dir / "best_validation_daily_curve.parquet"
                        )
                _save_group_checkpoint(
                    group_checkpoint_path,
                    train_years=train_years,
                    epoch=epoch,
                    model=model,
                    optimizer=optimizer,
                    scaler=scaler,
                    experiment_manifest=experiment_manifest,
                    effective_train_batch_size=int(config.training.batch_size_train),
                    scheduler=scheduler,
                    extra_payload={
                        "best_val_loss": float(best_val_loss),
                        "fold_id": int(fold.fold_id),
                        "tw_minute_state": checkpoint_state,
                    },
                    no_improve_epochs=no_improve,
                    early_stop_patience=patience,
                    early_stopping_no_improve_ratio=float(
                        config.training.early_stopping_no_improve_ratio
                    ),
                    early_stop_val_interval_epochs=val_interval,
                    check_finite=bool(config.training.checkpoint_finite_check),
                )
                checkpoint_s = time.perf_counter() - checkpoint_started
                epoch_row = {
                    "epoch": int(epoch),
                    "fold_id": int(fold.fold_id),
                    "train_loss": float(train_loss),
                    "val_mean": val_loss,
                    "test_mean": test_loss,
                    "test_mean_scope": "first_calendar_year_of_fold_test",
                    "test_mean_sampled": True,
                    "test_sample_fold_id": int(fold.fold_id),
                    "test_sample_year": int(first_test_year),
                    "test_sample_rows": int(len(first_test_year_indices)),
                    "lr": float(optimizer.param_groups[0]["lr"]),
                    "no_improve": int(no_improve),
                    "best_val_loss": float(best_val_loss),
                    "improved": int(improved),
                    "minute_train_mean_daily_return": float(
                        train_summary["mean_daily_return"]
                    ),
                    "minute_train_annualized_log_utility": float(
                        train_summary["annualized_log_utility"]
                    ),
                    "minute_val_mean_daily_return": (
                        None
                        if val_summary is None
                        else float(val_summary["mean_daily_return"])
                    ),
                    "minute_val_annualized_log_utility": (
                        None
                        if val_summary is None
                        else float(val_summary["annualized_log_utility"])
                    ),
                    "minute_test_mean_daily_return": (
                        None
                        if test_summary is None
                        else float(test_summary["mean_daily_return"])
                    ),
                    "minute_test_annualized_log_utility": (
                        None
                        if test_summary is None
                        else float(test_summary["annualized_log_utility"])
                    ),
                    "minute_days_per_train_batch": int(
                        config.training.batch_size_train
                    ),
                    "minute_days_per_eval_batch": int(
                        config.training.batch_size_eval
                    ),
                    "minute_decision_chunk_rows": int(
                        config.training.minute_decision_chunk_rows
                    ),
                    "minute_eval_decision_chunk_rows": int(
                        config.training.minute_eval_decision_chunk_rows
                        or config.training.minute_decision_chunk_rows
                    ),
                    "minute_flat_train_model_rows": int(
                        flat_train_model_rows_per_rank
                    ),
                    "minute_flat_train_model_rows_scope": (
                        "per_rank_configured_capacity"
                    ),
                    "minute_optimizer_steps_per_epoch": int(steps_per_epoch),
                    "minute_train_cache_hits": int(train_cache_hits),
                    "minute_train_cache_misses": int(train_cache_misses),
                    "minute_val_cache_hits": int(val_cache_hits),
                    "minute_val_cache_misses": int(val_cache_misses),
                    "minute_test_cache_hits": int(test_cache_hits),
                    "minute_test_cache_misses": int(test_cache_misses),
                    "minute_cache_gib": float(
                        day_cache.cache_bytes / (1024**3)
                    ),
                    # These are host enqueue/wait buckets. CUDA event timing is
                    # intentionally not fabricated when timing_synchronized=0.
                    "minute_train_ledger_host_s": float(train_timing.backtest_s),
                    "minute_val_ledger_host_s": float(val_timing.backtest_s),
                    "minute_test_ledger_host_s": float(test_timing.backtest_s),
                    **_timing_curve_payload(
                        train_timing=train_timing,
                        val_timing=val_timing,
                        test_curve_timing=test_timing,
                        val_eval_s=val_timing.total_s,
                        test_curve_s=test_timing.total_s,
                        checkpoint_save_s=checkpoint_s,
                        scheduler_s=scheduler_s,
                        epoch_wall_s=time.perf_counter() - epoch_started,
                        timing_synchronized=(device.type != "cuda"),
                    ),
                }
                request_plot = (
                    epoch == start_epoch
                    or epoch % curve_plot_request_interval == 0
                    or epoch == int(config.training.epochs)
                )
                curve_lifecycle.record(epoch_row, request_plot=request_plot)
                lifecycle.finish_epoch(
                    epoch=epoch,
                    epoch_total=int(config.training.epochs),
                    metrics={
                        "train_loss": train_loss,
                        "val_mean": val_loss,
                        "test_mean": test_loss,
                        "best_val_loss": best_val_loss,
                        "no_improve": no_improve,
                        "lr": optimizer.param_groups[0]["lr"],
                        "epoch_total_s": time.perf_counter() - epoch_started,
                    },
                )
                epoch_progress.set_postfix(
                    {
                        "train_loss": f"{train_loss:.8f}",
                        "val_mean": (
                            "-" if val_loss is None else f"{val_loss:.8f}"
                        ),
                        "test_mean": (
                            "-" if test_loss is None else f"{test_loss:.8f}"
                        ),
                        "best_val": f"{best_val_loss:.8f}",
                        "no_improve": no_improve,
                        "lr": f"{optimizer.param_groups[0]['lr']:.2e}",
                        "wall": f"{time.perf_counter() - epoch_started:.2f}s",
                    }
                )
                if should_validate and patience > 0 and no_improve >= patience:
                    _progress(
                        f"[Train {train_years}] early stop at epoch {epoch}: "
                        f"no improvement for {no_improve} validation check(s) "
                        f"(patience={patience})"
                    )
                    break
            plot_timing = curve_lifecycle.flush()
            _log_curve_plot_timing(
                f"Train {train_years} epoch_curve_plot",
                plot_timing,
            )

        # Rank zero owns checkpoint I/O.  Keep worker ranks from testing or
        # loading the path until the writer has completed the final replace.
        _distributed_barrier()
        if not best_path.is_file():
            raise RuntimeError(f"tw_minute best checkpoint is missing: {best_path}")
        best = _load_checkpoint(best_path)
        checkpoint_normalizer = _validate_minute_checkpoint(
            best,
            dataset=dataset,
            config=config,
        )
        _load_state_dict(model, best["model_state_dict"])
        lifecycle.set_phase(
            "validation",
            fold_id=fold.fold_id,
            work_total=len(fold.val_indices),
            work_unit="day",
            message=f"Fold {fold.fold_id} validation best",
        )
        val_summary, val_rows, val_close_weights, _ = _run_split(
            name=f"Fold {fold.fold_id} validation best",
            indices=fold.val_indices,
            dataset=dataset,
            normalizer=checkpoint_normalizer,
            model=runtime_model,
            model_forward=model_forward,
            daily_context_forward=(
                daily_context_forward
                if daily_context_forward_module is not None
                else None
            ),
            execution_forward=execution_forward,
            day_cache=day_cache,
            config=config,
            execution_config=execution_config,
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            device=device,
            amp_dtype=amp_dtype,
            progress_lifecycle=lifecycle,
        )
        lifecycle.set_phase(
            "testing",
            fold_id=fold.fold_id,
            work_total=len(fold.test_indices),
            work_unit="day",
            message=f"Fold {fold.fold_id} test best",
        )
        test_summary, test_rows, test_close_weights, _ = _run_split(
            name=f"Fold {fold.fold_id} test best",
            indices=fold.test_indices,
            dataset=dataset,
            normalizer=checkpoint_normalizer,
            model=runtime_model,
            model_forward=model_forward,
            daily_context_forward=(
                daily_context_forward
                if daily_context_forward_module is not None
                else None
            ),
            execution_forward=execution_forward,
            day_cache=day_cache,
            config=config,
            execution_config=execution_config,
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            device=device,
            amp_dtype=amp_dtype,
            progress_lifecycle=lifecycle,
        )
        if _distributed_should_write():
            pl.DataFrame(val_rows).write_parquet(
                fold_dir / "validation_daily_curve.parquet"
            )
            pl.DataFrame(test_rows).write_parquet(
                fold_dir / "test_daily_curve.parquet"
            )
        val_backtest, _ = _minute_backtest_result(
            val_rows,
            val_close_weights,
        )
        test_backtest, test_dates = _minute_backtest_result(
            test_rows,
            test_close_weights,
        )
        fold_result = FoldResult(
            fold_id=int(fold.fold_id),
            train_years=[int(value) for value in fold.train_years],
            val_years=[int(value) for value in fold.val_years],
            test_years=[int(value) for value in fold.test_years],
            best_val_loss=float(best_val_loss),
            val_ic=ic_summary(np.asarray([], dtype=np.float64)),
            val_metrics={
                **_minute_report_metrics(val_backtest, val_summary),
                "minute_best_epoch": float(best["epoch"]),
            },
            test_ic=ic_summary(np.asarray([], dtype=np.float64)),
            test_metrics={
                **_minute_report_metrics(test_backtest, test_summary),
                "minute_best_epoch": float(best["epoch"]),
            },
        )
        next_fold = next_fold_by_id.get(fold.fold_id)
        deployment_rows = int(test_dates.size)
        if next_fold is not None and next_fold.test_indices.size > 0:
            cutoff_date = np.asarray(
                dataset.dates[int(next_fold.test_indices[0])],
                dtype="datetime64[D]",
            )
            deployment_rows = int(
                np.searchsorted(test_dates, cutoff_date, side="left")
            )
        deployment_backtest = _prefix_backtest_result(
            test_backtest,
            deployment_rows,
        )
        deployment_dates = test_dates[:deployment_rows]
        _save_fold_output_artifacts(
            fold_dir=fold_dir,
            fold_result=fold_result,
            model=model,
            test_backtest=test_backtest,
            test_dates=test_dates,
            symbols=list(dataset.symbols),
            config=config,
            deployment_backtest=deployment_backtest,
            deployment_dates=deployment_dates,
            print_report=True,
            write_plots=True,
            mark_complete=True,
        )
        if _distributed_should_write():
            save_realized_holdings_artifacts(
                fold_dir,
                test_dates,
                list(dataset.symbols),
                test_backtest.weights_history,
                initial_capital=float(config.trading.tw_minute_initial_equity),
                daily_performance=pl.DataFrame(test_rows),
                write_plots=True,
                source_artifact="test_backtest.npz",
            )
        if _distributed_should_write():
            write_training_json(
                fold_dir / "mode_artifact_contract.json",
                _minute_artifact_contract(dataset, config),
            )
        results_by_fold[fold.fold_id] = fold_result
        if _distributed_should_write():
            _refresh_walkforward_artifacts(
                root,
                list(results_by_fold.values()),
            )
        result = {
            "status": "complete",
            "execution_mode": "tw_minute",
            **asdict(fold_result),
        }
        results.append(result)
        lifecycle.finish_fold(fold.fold_id)
        day_cache.close()
        # Do not let non-writer ranks enter the next fold's DDP forward while
        # rank zero is still finalizing plots, parquet files, and manifests.
        _distributed_barrier()
        # All objects below own or can retain CUDA tensors/compiled callables.
        # Drop every Python owner before resetting compiler caches; emptying the
        # allocator alone cannot release live Inductor/DDP references.
        del curve_lifecycle
        del model_forward
        del eager_execution_forward
        del execution_forward
        del runtime_model
        del slab_forward_module
        del scheduler
        del scaler
        del optimizer
        del model
        del day_cache
        released = _release_minute_group_runtime(device)
        _progress(
            f"[Train {train_years}] released group CUDA runtime: "
            f"allocated={released['allocated_before'] / 1024**3:.2f}->"
            f"{released['allocated_after'] / 1024**3:.2f}GiB "
            f"reserved={released['reserved_before'] / 1024**3:.2f}->"
            f"{released['reserved_after'] / 1024**3:.2f}GiB"
        )
        # Compiler reset and garbage collection are rank-local.  Keep the next
        # group's DDP construction aligned only after both ranks have finished.
        _distributed_barrier()
    return results


def run_minute_training(
    config: ExperimentConfig,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
) -> list[dict[str, Any]]:
    """Run minute training with the same outer lifecycle as daily/tick modes."""

    lifecycle: TrainingRunLifecycle | _NullTrainingRunLifecycle
    if _distributed_should_write():
        lifecycle = TrainingRunLifecycle(
            output_dir,
            execution_mode=config.trading.execution_mode,
            run_mode=mode,
            strategy=active_strategy,
            model_name=config.training.model_name,
        )
    else:
        lifecycle = _NullTrainingRunLifecycle()
    configuration = asdict(config)
    lifecycle.start(
        fold_ids=[],
        configuration_fingerprint=_stable_fingerprint(
            _configuration_fingerprint_snapshot(config)
        ),
        contract_versions={
            "execution": MINUTE_EXECUTION_CONTRACT_VERSION,
            "training": MINUTE_TRAINING_CONTRACT_VERSION,
        },
        configuration=configuration,
    )
    try:
        results = _run_minute_training_impl(
            config,
            output_dir=output_dir,
            mode=mode,
            resume=resume,
            start_fold=start_fold,
            max_folds=max_folds,
            active_strategy=active_strategy,
            lifecycle=lifecycle,
        )
    except BaseException as exc:
        lifecycle.fail(exc)
        raise
    lifecycle.complete(
        fold_ids=[int(result["fold_id"]) for result in results],
        message=f"completed {len(results)} fold(s)",
    )
    return results


_FOLD_ISOLATION_CHILD_ENV = "STOCKAGENT_FOLD_ISOLATION_CHILD"


def _minute_isolated_fold_command(
    argv: list[str],
    *,
    output_dir: str | Path,
    fold_id: int,
    resume: bool,
) -> list[str]:
    """Build one authoritative fresh-process minute-fold invocation."""

    return [
        sys.executable,
        str(Path(__file__).resolve().parents[2] / "train.py"),
        *argv,
        "--output-dir",
        str(output_dir),
        "--mode",
        "train",
        "--resume" if resume else "--no-resume",
        "--start-fold",
        str(int(fold_id)),
        "--max-folds",
        "1",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]


def _minute_isolated_fold_plan(
    config: ExperimentConfig,
    *,
    start_fold: int | None,
    max_folds: int | None,
) -> list[WalkForwardFold]:
    manifest_path = Path(config.data.minute_parquet_root).resolve() / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"minute dataset manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dates = np.asarray(manifest.get("dates", []), dtype="datetime64[D]")
    validate_walk_forward_year_contract(
        dates,
        expected_first_year=config.walk_forward.expected_first_year,
        require_contiguous_years=config.walk_forward.require_contiguous_years,
    )
    folds = build_expanding_year_folds(
        dates=dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    )
    return _folds_for_run(
        folds,
        start_fold=start_fold,
        max_folds=max_folds,
    )


def _load_isolated_minute_fold_result(
    output_dir: Path,
    fold: WalkForwardFold,
) -> FoldResult:
    metrics_path = output_dir / f"fold_{fold.fold_id:02d}" / "metrics.json"
    complete_path = output_dir / f"fold_{fold.fold_id:02d}" / "fold_complete.json"
    if not metrics_path.is_file() or not complete_path.is_file():
        raise RuntimeError(
            f"isolated tw_minute fold {fold.fold_id} did not produce completed artifacts"
        )
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    if complete.get("status") != "complete":
        raise RuntimeError(f"isolated tw_minute fold {fold.fold_id} is not complete")
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    if int(payload.get("fold_id", -1)) != int(fold.fold_id):
        raise RuntimeError("isolated tw_minute metrics fold id is inconsistent")
    return FoldResult(**payload)


def run_minute_training_isolated(
    config: ExperimentConfig,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
) -> list[dict[str, Any]]:
    """Run each minute fold in a fresh process while retaining within-fold DDP."""

    if mode != "train":
        raise ValueError("tw_minute fold isolation is available only in train mode")
    if _distributed_is_initialized():
        raise RuntimeError(
            "tw_minute isolation parent must start outside torchrun; "
            "each child owns its within-fold DDP process group"
        )
    folds = _minute_isolated_fold_plan(
        config,
        start_fold=start_fold,
        max_folds=max_folds,
    )
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    child_env = os.environ.copy()
    child_env[_FOLD_ISOLATION_CHILD_ENV] = "1"
    for position, fold in enumerate(folds, start=1):
        command = _minute_isolated_fold_command(
            list(sys.argv[1:]),
            output_dir=root,
            fold_id=fold.fold_id,
            resume=resume,
        )
        _progress(
            "[tw-minute isolate] fresh process "
            f"fold={fold.fold_id} position={position}/{len(folds)}; "
            "child will relaunch under within-fold DDP"
        )
        completed = subprocess.run(
            command,
            env=child_env,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "isolated tw_minute child failed: "
                f"fold={fold.fold_id} returncode={completed.returncode}"
            )

    fold_results = [
        _load_isolated_minute_fold_result(root, fold)
        for fold in folds
    ]
    _refresh_walkforward_artifacts(root, fold_results)

    # The last child owns a complete, verified manifest. Restore its semantic
    # fields while widening selected/completed ids to the full parent plan.
    child_manifest_path = root / "run_manifest.json"
    child_manifest = json.loads(child_manifest_path.read_text(encoding="utf-8"))
    lifecycle = TrainingRunLifecycle(
        root,
        execution_mode=config.trading.execution_mode,
        run_mode=mode,
        strategy=active_strategy,
        model_name=config.training.model_name,
    )
    lifecycle.start(
        fold_ids=[fold.fold_id for fold in folds],
        dataset_fingerprint=child_manifest.get("dataset_fingerprint"),
        configuration_fingerprint=child_manifest.get("configuration_fingerprint"),
        contract_versions=child_manifest.get("contract_versions"),
        data_summary=child_manifest.get("data_summary"),
        configuration=asdict(config),
        mode_details=child_manifest.get("mode_details"),
        compatibility_manifest_paths=[root / "minute_run_manifest.json"],
    )
    lifecycle.complete(
        fold_ids=[fold.fold_id for fold in folds],
        message=f"completed {len(folds)} isolated fold(s) with within-fold DDP",
    )
    return [
        {
            "status": "complete",
            "execution_mode": "tw_minute",
            **asdict(result),
        }
        for result in fold_results
    ]


__all__ = ["run_minute_training", "run_minute_training_isolated"]
