"""Dedicated trainer for causal completed-second TXO/TX decisions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import time
from typing import Any, Iterable

import numpy as np
import polars as pl
import torch
from torch import nn
from torch.amp import GradScaler

from stockagent.backtest.tw_index_derivatives_tick import (
    TW_INDEX_DERIVATIVES_TICK_BACKTEST_CONTRACT_VERSION,
    TW_INDEX_OPTIONS_TICK_BACKTEST_CONTRACT_VERSION,
    execute_option_tick_target,
    initialize_option_tick_state,
    option_target_weights_from_logits,
    run_tw_index_derivatives_tick_bidask_continuous,
)
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import ExperimentConfig
from stockagent.data.tw_index_derivatives_tick import (
    TICK_DATASET_SCHEMA_VERSION,
    TICK_FEATURE_COLUMNS,
    TICK_FEATURE_CONTRACT_VERSION,
    OPTION_TICK_FEATURE_COLUMNS,
    IndexDerivativesTickDataset,
    IndexDerivativesTickDay,
    TickFeatureNormalizer,
    build_index_derivatives_tick_dataset,
    load_index_derivatives_tick_dataset,
)
from stockagent.data.walkforward import WalkForwardFold
from stockagent.models.factory import build_model
from stockagent.evaluation.metrics import ic_summary
from stockagent.training.mode_adapter import (
    chronological_session_indices,
    date_training_group_name,
    training_mode_spec,
    write_training_json,
)
from stockagent.training.lifecycle import (
    TrainingArtifactLayout,
    TrainingRunLifecycle,
    canonical_mode_artifact_contract,
)
from stockagent.training.trainer import (
    FoldResult,
    _EpochCurveLifecycle,
    _PreEpochTimingRecorder,
    _autocast_context,
    _can_enable_torch_compile,
    _create_adamw_optimizer,
    _create_lr_scheduler,
    _load_checkpoint as _load_checkpoint_payload,
    _load_completed_fold_result,
    _load_state_dict,
    _prefix_backtest_result,
    _progress_bar,
    _refresh_walkforward_artifacts,
    _resolve_amp_dtype,
    _resolve_device,
    _restore_rng_state,
    _save_fold_checkpoint,
    _save_fold_output_artifacts,
    _save_group_checkpoint,
    _stable_fingerprint,
    _step_batch_lr_scheduler,
    _torch_compile_options,
    _validate_checkpoint_effective_train_batch_size,
)


TICK_CHECKPOINT_SCHEMA_VERSION = 5
TICK_TRAINING_STATE_VERSION = 1
OPTION_TICK_MODES = frozenset(
    {"tw_index_options_tick_long", "tw_index_options_tick_short"}
)
DERIVATIVES_TICK_MODES = frozenset({"tw_index_derivatives_tick", *OPTION_TICK_MODES})


def _option_mode(config: ExperimentConfig) -> bool:
    return config.trading.execution_mode in OPTION_TICK_MODES


def _feature_names(config: ExperimentConfig) -> tuple[str, ...]:
    return OPTION_TICK_FEATURE_COLUMNS if _option_mode(config) else TICK_FEATURE_COLUMNS


def _execution_contract_version(config: ExperimentConfig) -> int:
    return (
        TW_INDEX_OPTIONS_TICK_BACKTEST_CONTRACT_VERSION
        if _option_mode(config)
        else TW_INDEX_DERIVATIVES_TICK_BACKTEST_CONTRACT_VERSION
    )


def _tick_configuration_fingerprint(config: ExperimentConfig) -> str:
    return _stable_fingerprint(asdict(config))


def _tick_artifact_contract(
    dataset: IndexDerivativesTickDataset,
    config: ExperimentConfig,
) -> dict[str, Any]:
    return {
        **canonical_mode_artifact_contract(config.trading.execution_mode),
        "checkpoint_schema_version": TICK_CHECKPOINT_SCHEMA_VERSION,
        "dataset_schema_version": TICK_DATASET_SCHEMA_VERSION,
        "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
        "execution_contract_version": _execution_contract_version(config),
        "feature_names": list(_feature_names(config)),
        "dataset_source_manifest_sha256": dataset.manifest[
            "source_manifest_sha256"
        ],
        "configuration_fingerprint": _tick_configuration_fingerprint(config),
    }


@dataclass(frozen=True, slots=True)
class TickWalkForwardFold:
    fold_id: int
    train_indices: np.ndarray
    val_indices: np.ndarray
    test_indices: np.ndarray


def _tick_split_years(
    dataset: IndexDerivativesTickDataset,
    indices: np.ndarray,
) -> list[int]:
    values = np.asarray(dataset.dates[indices], dtype="datetime64[Y]")
    return sorted(
        {int(str(value)[:4]) for value in values if not np.isnat(value)}
    )


def _canonical_tick_fold(
    dataset: IndexDerivativesTickDataset,
    fold: TickWalkForwardFold,
) -> WalkForwardFold:
    return WalkForwardFold(
        fold_id=int(fold.fold_id),
        train_indices=fold.train_indices,
        val_indices=fold.val_indices,
        test_indices=fold.test_indices,
        train_years=_tick_split_years(dataset, fold.train_indices),
        val_years=_tick_split_years(dataset, fold.val_indices),
        test_years=_tick_split_years(dataset, fold.test_indices),
    )


def build_tick_walk_forward_folds(
    num_dates: int,
    *,
    min_train_days: int,
    val_days: int,
    test_days: int,
) -> list[TickWalkForwardFold]:
    if min(min_train_days, val_days, test_days) < 1:
        raise ValueError("tick walk-forward day counts must be positive")
    if num_dates < min_train_days + val_days + test_days:
        raise ValueError(
            "tick dataset has too few dates for the configured train/val/test split: "
            f"dates={num_dates}, required={min_train_days + val_days + test_days}"
        )
    folds: list[TickWalkForwardFold] = []
    train_end = int(min_train_days)
    while train_end + val_days + test_days <= num_dates:
        folds.append(
            TickWalkForwardFold(
                fold_id=len(folds),
                train_indices=np.arange(0, train_end, dtype=np.int64),
                val_indices=np.arange(train_end, train_end + val_days, dtype=np.int64),
                test_indices=np.arange(
                    train_end + val_days,
                    train_end + val_days + test_days,
                    dtype=np.int64,
                ),
            )
        )
        train_end += test_days
    return folds


def _model_exposure(
    model: nn.Module,
    features: torch.Tensor,
    mask: torch.Tensor,
) -> torch.Tensor:
    output = model(features, mask, return_aux=False)
    if isinstance(output, dict):
        direct = output.get("futures_exposure")
        if isinstance(direct, torch.Tensor):
            return direct
        output = output.get("weights")
    if isinstance(output, tuple):
        output = output[0]
    if not isinstance(output, torch.Tensor) or output.ndim != 2:
        raise RuntimeError("tick model must return pseudo weights with shape [B,S]")
    return output.sum(dim=-1)


def _model_logits(
    model: nn.Module,
    features: torch.Tensor,
    mask: torch.Tensor,
) -> torch.Tensor:
    output = model(features, mask, return_aux=False)
    if isinstance(output, dict):
        output = output.get("score_logits", output.get("scores", output.get("weights")))
    if isinstance(output, tuple):
        output = output[0]
    if not isinstance(output, torch.Tensor) or output.ndim != 2:
        raise RuntimeError("option tick model must return logits with shape [B,2]")
    if output.size(1) != 2:
        raise RuntimeError("option tick model must emit one Call and one Put logit")
    return output


def _pad_model_batch(
    features: torch.Tensor,
    mask: torch.Tensor,
    *,
    batch_size: int,
) -> tuple[torch.Tensor, torch.Tensor, int]:
    rows = int(features.size(0))
    if rows >= batch_size:
        return features, mask, rows
    pad_rows = batch_size - rows
    return (
        torch.cat(
            [
                features,
                torch.zeros(
                    (pad_rows, *features.shape[1:]),
                    device=features.device,
                    dtype=features.dtype,
                ),
            ],
            dim=0,
        ),
        torch.cat(
            [
                mask,
                torch.zeros(
                    (pad_rows, mask.size(1)),
                    device=mask.device,
                    dtype=torch.bool,
                ),
            ],
            dim=0,
        ),
        rows,
    )


def _run_day(
    *,
    model: nn.Module,
    day: IndexDerivativesTickDay,
    config: ExperimentConfig,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    batch_size: int,
    optimizer: torch.optim.Optimizer | None,
    scaler: GradScaler | None = None,
    scheduler: torch.optim.lr_scheduler.LRScheduler | None,
) -> dict[str, float | str]:
    training = optimizer is not None
    model.train(training)
    is_option = _option_mode(config)
    lookback = int(config.training.lookback)
    if day.rows < lookback:
        raise ValueError(
            f"tick day {day.trading_date} has {day.rows} rows below lookback={lookback}"
        )
    non_blocking = bool(config.training.non_blocking_transfer)
    features = torch.from_numpy(day.features).to(
        device=device,
        dtype=torch.float32,
        non_blocking=non_blocking,
    )
    feature_windows = features.unfold(0, lookback, 1).permute(0, 3, 1, 2)
    model_masks = torch.ones(
        (feature_windows.size(0), features.size(1)),
        device=device,
        dtype=torch.bool,
    )
    if is_option:
        if (
            day.option_execution_bid_prices is None
            or day.option_execution_bid_volumes is None
            or day.option_execution_ask_prices is None
            or day.option_execution_ask_volumes is None
            or day.option_next_execution_bid_prices is None
            or day.option_next_execution_ask_prices is None
            or day.underlying_execution_prices is None
            or day.option_strike is None
        ):
            raise RuntimeError(
                "option tick mode requires captured Call/Put BidAsk depth"
            )
        option_bid_prices = torch.from_numpy(day.option_execution_bid_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        option_bid_volumes = torch.from_numpy(day.option_execution_bid_volumes).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        option_ask_prices = torch.from_numpy(day.option_execution_ask_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        option_ask_volumes = torch.from_numpy(day.option_execution_ask_volumes).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        option_next_bid_prices = torch.from_numpy(
            day.option_next_execution_bid_prices
        ).to(device=device, dtype=torch.float32, non_blocking=non_blocking)
        option_next_ask_prices = torch.from_numpy(
            day.option_next_execution_ask_prices
        ).to(device=device, dtype=torch.float32, non_blocking=non_blocking)
        execution_prices = torch.from_numpy(day.underlying_execution_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        tradable = torch.from_numpy(day.tradable_mask).to(
            device=device, dtype=torch.bool, non_blocking=non_blocking
        )
        option_state = initialize_option_tick_state(
            initial_equity=float(config.trading.tw_index_options_tick_initial_equity),
            device=device,
        )
        previous_exposure = None
    else:
        if (
            day.execution_bid_prices is None
            or day.execution_bid_volumes is None
            or day.execution_ask_prices is None
            or day.execution_ask_volumes is None
            or day.next_execution_bid_prices is None
            or day.next_execution_ask_prices is None
        ):
            raise RuntimeError("TX tick mode requires captured five-level BidAsk depth")
        bid_prices = torch.from_numpy(day.execution_bid_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        bid_volumes = torch.from_numpy(day.execution_bid_volumes).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        ask_prices = torch.from_numpy(day.execution_ask_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        ask_volumes = torch.from_numpy(day.execution_ask_volumes).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        next_bid_prices = torch.from_numpy(day.next_execution_bid_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        next_ask_prices = torch.from_numpy(day.next_execution_ask_prices).to(
            device=device, dtype=torch.float32, non_blocking=non_blocking
        )
        tradable = torch.from_numpy(day.tradable_mask).to(
            device=device, dtype=torch.bool, non_blocking=non_blocking
        )
        previous_exposure = torch.zeros((), device=device, dtype=torch.float32)
        option_state = None
    selected = np.arange(lookback - 1, day.rows, dtype=np.int64)
    total_log_return = torch.zeros((), device=device, dtype=torch.float32)
    total_gross = torch.zeros((), device=device, dtype=torch.float32)
    total_cost = torch.zeros((), device=device, dtype=torch.float32)
    total_turnover = torch.zeros((), device=device, dtype=torch.float32)
    total_fixed_fees = torch.zeros((), device=device, dtype=torch.float32)
    total_transaction_tax = torch.zeros((), device=device, dtype=torch.float32)
    total_slippage = torch.zeros((), device=device, dtype=torch.float32)
    total_premium_cash_flow = torch.zeros((), device=device, dtype=torch.float32)
    maximum_margin = torch.zeros((), device=device, dtype=torch.float32)
    total_unfilled_contracts = torch.zeros((), device=device, dtype=torch.float32)
    chunks = list(range(0, len(selected), batch_size))
    for chunk_number, start in enumerate(chunks):
        row_indices_np = selected[start : start + batch_size]
        window_indices = torch.from_numpy(row_indices_np - lookback + 1).to(
            device=device, dtype=torch.long
        )
        feature_batch = feature_windows.index_select(0, window_indices)
        mask_batch = model_masks.index_select(0, window_indices)
        feature_batch, mask_batch, real_rows = _pad_model_batch(
            feature_batch,
            mask_batch,
            batch_size=batch_size,
        )
        if training:
            optimizer.zero_grad(set_to_none=True)
        with torch.set_grad_enabled(training), _autocast_context(device, amp_dtype):
            if is_option:
                logits = _model_logits(model, feature_batch, mask_batch)[:real_rows]
                targets = option_target_weights_from_logits(
                    logits,
                    mask_batch[:real_rows],
                    allow_option_short=(
                        config.trading.execution_mode == "tw_index_options_tick_short"
                    ),
                    maximum_capital_fraction=float(
                        config.trading.tw_index_options_tick_maximum_capital_fraction
                    ),
                )
            else:
                exposure = _model_exposure(model, feature_batch, mask_batch)[:real_rows]
        row_indices = torch.from_numpy(row_indices_np).to(
            device=device, dtype=torch.long
        )
        is_last_chunk = chunk_number == len(chunks) - 1
        if is_option:
            assert option_state is not None
            chunk_log_utility = torch.zeros((), device=device, dtype=torch.float32)
            for local, row_index in enumerate(row_indices_np.tolist()):
                equity_before = option_state.equity
                step = execute_option_tick_target(
                    option_state,
                    target_weights=targets[local],
                    tradable_mask=tradable[row_index],
                    bid_prices=option_bid_prices[row_index],
                    bid_volumes=option_bid_volumes[row_index],
                    ask_prices=option_ask_prices[row_index],
                    ask_volumes=option_ask_volumes[row_index],
                    next_bid_prices=option_next_bid_prices[row_index],
                    next_ask_prices=option_next_ask_prices[row_index],
                    underlying_price=execution_prices[row_index],
                    strike_price=float(day.option_strike),
                    allow_option_short=(
                        config.trading.execution_mode == "tw_index_options_tick_short"
                    ),
                    maximum_capital_fraction=float(
                        config.trading.tw_index_options_tick_maximum_capital_fraction
                    ),
                    contract_multiplier=float(
                        config.trading.tw_index_options_tick_contract_multiplier
                    ),
                    risk_margin_a_twd=float(
                        config.trading.tw_index_options_tick_risk_margin_a_twd
                    ),
                    risk_margin_b_twd=float(
                        config.trading.tw_index_options_tick_risk_margin_b_twd
                    ),
                    fixed_fee_per_contract_per_side_twd=float(
                        config.trading.tw_index_options_tick_fixed_fee_per_contract_per_side_twd
                    ),
                    transaction_tax_rate=float(
                        config.trading.tw_index_options_tick_transaction_tax_rate
                    ),
                    slippage_points_per_side=float(
                        config.trading.tw_index_options_tick_slippage_points_per_side
                    ),
                    force_flat=(is_last_chunk and local == real_rows - 1),
                )
                option_state = step.state
                safe_return = torch.clamp(step.net_return, min=-1.0 + 1e-7)
                chunk_log_utility = chunk_log_utility + torch.log1p(safe_return)
                costs = step.fixed_fees + step.transaction_tax + step.slippage_cost
                total_gross += step.gross_pnl.detach()
                total_cost += costs.detach() / equity_before.detach().clamp_min(
                    torch.finfo(torch.float32).tiny
                )
                total_turnover += step.turnover_contracts.detach()
                total_fixed_fees += step.fixed_fees.detach()
                total_transaction_tax += step.transaction_tax.detach()
                total_slippage += step.slippage_cost.detach()
                total_premium_cash_flow += step.premium_cash_flow.detach()
                maximum_margin = torch.maximum(
                    maximum_margin, step.initial_margin_required.detach()
                )
                total_unfilled_contracts += step.unfilled_contracts.detach()
        else:
            assert previous_exposure is not None
            backtest = run_tw_index_derivatives_tick_bidask_continuous(
                exposure,
                bid_prices.index_select(0, row_indices),
                bid_volumes.index_select(0, row_indices),
                ask_prices.index_select(0, row_indices),
                ask_volumes.index_select(0, row_indices),
                next_bid_prices.index_select(0, row_indices),
                next_ask_prices.index_select(0, row_indices),
                tradable.index_select(0, row_indices),
                initial_exposure=previous_exposure,
                initial_equity=float(
                    config.trading.tw_index_derivatives_tick_initial_equity
                ),
                contract_multiplier=float(
                    config.trading.tw_index_derivatives_tick_contract_multiplier
                ),
                fee_rate_per_side=float(
                    config.trading.tw_index_derivatives_tick_fee_rate_per_side
                ),
                slippage_points_per_side=float(
                    config.trading.tw_index_derivatives_tick_slippage_points_per_side
                ),
                max_abs_exposure=float(
                    config.trading.tw_index_futures_max_abs_exposure
                ),
                force_final_flat=is_last_chunk,
            )
            safe_returns = torch.clamp(
                backtest.strategy_returns,
                min=-1.0 + 1e-7,
            )
            chunk_log_utility = torch.log1p(safe_returns).sum()
        if training:
            if scaler is None:
                raise RuntimeError(
                    "tick training requires the canonical AMP GradScaler"
                )
            loss = -chunk_log_utility / max(1, real_rows)
            if not torch.isfinite(loss):
                raise RuntimeError(f"non-finite tick training loss: {loss}")
            scaler.scale(loss).backward()
            if float(config.training.grad_clip_norm) > 0.0:
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(
                    model.parameters(), float(config.training.grad_clip_norm)
                )
            scaler.step(optimizer)
            scaler.update()
            if scheduler is not None:
                _step_batch_lr_scheduler(scheduler)
        total_log_return += chunk_log_utility.detach()
        if is_option:
            assert option_state is not None
            option_state = option_state.detached()
        else:
            total_gross += backtest.gross_returns.detach().sum()
            total_cost += backtest.cost_returns.detach().sum()
            total_turnover += backtest.turnovers.detach().sum()
            previous_exposure = backtest.final_exposure.detach().clone()
    if is_option:
        assert option_state is not None
        if bool(torch.any(option_state.contracts != 0.0).item()):
            raise RuntimeError("tick day ended with nonzero Call/Put contracts")
        initial_equity = float(config.trading.tw_index_options_tick_initial_equity)
        net_return = (
            float(option_state.equity.detach().cpu().item()) / initial_equity - 1.0
        )
        gross_value = float(total_gross.detach().cpu().item()) / initial_equity
    else:
        assert previous_exposure is not None
        if not bool(torch.isclose(previous_exposure, previous_exposure * 0.0).item()):
            raise RuntimeError("tick day ended with a nonzero TX exposure")
        net_return = float(math.expm1(float(total_log_return.detach().cpu().item())))
        gross_value = float(total_gross.detach().cpu().item())
    metrics = (
        torch.stack(
            [
                total_log_return,
                total_cost,
                total_turnover,
                total_fixed_fees,
                total_transaction_tax,
                total_slippage,
                total_premium_cash_flow,
                maximum_margin,
                total_unfilled_contracts,
            ]
        )
        .cpu()
        .numpy()
    )
    result: dict[str, float | str] = {
        "date": str(day.trading_date.astype("datetime64[D]")),
        "tx_contract_month": day.tx_contract_month,
        "net_return": net_return,
        "log_return": float(metrics[0]),
        "gross_return_sum": gross_value,
        "cost_return_sum": float(metrics[1]),
        "turnover": float(metrics[2]),
        "decisions": float(len(selected)),
    }
    if is_option:
        result.update(
            {
                "option_series": str(day.option_series),
                "option_strike": float(day.option_strike),
                "fixed_fees_twd": float(metrics[3]),
                "transaction_tax_twd": float(metrics[4]),
                "slippage_twd": float(metrics[5]),
                "premium_cash_flow_twd": float(metrics[6]),
                "maximum_initial_margin_twd": float(metrics[7]),
                "unfilled_contracts": float(metrics[8]),
            }
        )
    return result


def _summarize_days(rows: list[dict[str, float | str]]) -> dict[str, float]:
    if not rows:
        raise RuntimeError("tick split produced no day results")
    returns = np.asarray([float(row["net_return"]) for row in rows])
    log_returns = np.asarray([float(row["log_return"]) for row in rows])
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
        "total_cost_return": float(sum(float(row["cost_return_sum"]) for row in rows)),
        "total_turnover": float(sum(float(row["turnover"]) for row in rows)),
    }


def _tick_backtest_result(
    rows: list[dict[str, float | str]],
    *,
    execution_mode: str,
    symbols: int,
) -> tuple[BacktestResult, np.ndarray]:
    """Adapt native tick-ledger day results to shared return-curve artifacts.

    Tick strategies are force-flat, so the daily terminal weight snapshot is
    exactly zero.  Intraday fills, costs, margin, and unfilled quantities stay
    in ``test_daily_curve.parquet`` under the mode-specific audit contract.
    """

    dates = np.asarray([row["date"] for row in rows], dtype="datetime64[D]")
    if dates.size == 0 or np.any(dates[1:] <= dates[:-1]):
        raise RuntimeError(
            "tick reporting rows must be non-empty and strictly chronological"
        )
    return (
        BacktestResult(
            strategy_returns=np.asarray(
                [float(row["log_return"]) for row in rows], dtype=np.float64
            ),
            benchmark_returns=np.zeros(dates.size, dtype=np.float64),
            turnovers=np.asarray(
                [float(row["turnover"]) for row in rows], dtype=np.float64
            ),
            weights_history=np.zeros((dates.size, int(symbols)), dtype=np.float64),
            execution_mode=execution_mode,
            final_weights=np.zeros(int(symbols), dtype=np.float64),
        ),
        dates,
    )


def _tick_report_metrics(
    backtest: BacktestResult,
    summary: dict[str, float],
) -> dict[str, float]:
    metrics = compute_metrics(backtest)
    metrics.update(
        {
            f"tick_{name}": float(value)
            for name, value in summary.items()
            if isinstance(value, (int, float))
        }
    )
    return metrics


def _run_split(
    *,
    name: str,
    indices: Iterable[int],
    dataset: IndexDerivativesTickDataset,
    normalizer: TickFeatureNormalizer,
    model: nn.Module,
    config: ExperimentConfig,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    optimizer: torch.optim.Optimizer | None = None,
    scaler: GradScaler | None = None,
    scheduler: torch.optim.lr_scheduler.LRScheduler | None = None,
    progress_lifecycle: TrainingRunLifecycle | None = None,
) -> tuple[dict[str, float], list[dict[str, float | str]]]:
    selected = chronological_session_indices(indices)
    rows: list[dict[str, float | str]] = []
    skipped_dates_below_lookback: list[str] = []
    started = time.monotonic()
    day_progress = _progress_bar(
        desc=f" {name}",
        unit="day",
        total=len(selected),
        leave=False,
    )
    try:
        for index in selected:
            day = dataset.load_day(
                index,
                normalizer=normalizer,
                option_mode=_option_mode(config),
            )
            if day.rows < int(config.training.lookback):
                skipped_dates_below_lookback.append(str(day.trading_date))
                day_progress.set_postfix(
                    {
                        "batch_date": str(day.trading_date),
                        "status": "below_lookback",
                    },
                    refresh=False,
                )
                day_progress.update(1)
                if progress_lifecycle is not None:
                    progress_lifecycle.update_work(
                        completed=(
                            len(rows) + len(skipped_dates_below_lookback)
                        ),
                        total=len(selected),
                        unit="day",
                        last_sample=str(day.trading_date),
                        metrics={"status": "below_lookback"},
                    )
                continue
            rows.append(
                _run_day(
                    model=model,
                    day=day,
                    config=config,
                    device=device,
                    amp_dtype=amp_dtype,
                    batch_size=(
                        int(config.training.batch_size_train)
                        if optimizer is not None
                        else int(config.training.batch_size_eval)
                    ),
                    optimizer=optimizer,
                    scaler=scaler,
                    scheduler=scheduler,
                )
            )
            day_progress.set_postfix(
                {
                    "batch_date": rows[-1]["date"],
                    "batch_ret": f"{float(rows[-1]['net_return']):.6f}",
                },
                refresh=False,
            )
            day_progress.update(1)
            if progress_lifecycle is not None:
                progress_lifecycle.update_work(
                    completed=(len(rows) + len(skipped_dates_below_lookback)),
                    total=len(selected),
                    unit="day",
                    last_sample=str(rows[-1]["date"]),
                    metrics={"last_return": float(rows[-1]["net_return"])},
                )
    finally:
        day_progress.close()
    summary = _summarize_days(rows)
    summary["configured_days"] = float(len(selected))
    summary["skipped_days_below_lookback"] = float(len(skipped_dates_below_lookback))
    summary["elapsed_seconds"] = time.monotonic() - started
    return summary, rows


def _tick_experiment_manifest(
    dataset: IndexDerivativesTickDataset,
    config: ExperimentConfig,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "execution_mode": config.trading.execution_mode,
        "checkpoint_schema_version": TICK_CHECKPOINT_SCHEMA_VERSION,
        "training_state_version": TICK_TRAINING_STATE_VERSION,
        "dataset_schema_version": TICK_DATASET_SCHEMA_VERSION,
        "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
        "execution_contract_version": _execution_contract_version(config),
        "sample_order_contract": training_mode_spec(
            config.trading.execution_mode
        ).sample_order_contract,
        "dataset_source_manifest_sha256": dataset.manifest[
            "source_manifest_sha256"
        ],
        "configuration_fingerprint": _tick_configuration_fingerprint(config),
    }


def _tick_checkpoint_state(
    *,
    dataset: IndexDerivativesTickDataset,
    normalizer: TickFeatureNormalizer,
    config: ExperimentConfig,
) -> dict[str, Any]:
    return {
        "schema_version": TICK_TRAINING_STATE_VERSION,
        "checkpoint_schema_version": TICK_CHECKPOINT_SCHEMA_VERSION,
        "dataset_schema_version": TICK_DATASET_SCHEMA_VERSION,
        "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
        "execution_contract_version": _execution_contract_version(config),
        "execution_mode": config.trading.execution_mode,
        "sample_order_contract": training_mode_spec(
            config.trading.execution_mode
        ).sample_order_contract,
        "feature_names": list(_feature_names(config)),
        "normalizer_mean": normalizer.mean.tolist(),
        "normalizer_scale": normalizer.scale.tolist(),
        "normalizer_counts": normalizer.counts.tolist(),
        "dataset_source_manifest_sha256": dataset.manifest["source_manifest_sha256"],
        "configuration_fingerprint": _tick_configuration_fingerprint(config),
    }


def _load_tick_checkpoint(
    path: Path,
    *,
    model: nn.Module,
    optimizer: torch.optim.Optimizer | None,
    scaler: GradScaler | None,
    scheduler: torch.optim.lr_scheduler.LRScheduler
    | torch.optim.lr_scheduler.ReduceLROnPlateau
    | None,
    dataset: IndexDerivativesTickDataset,
    config: ExperimentConfig,
) -> dict[str, Any]:
    payload = _load_checkpoint_payload(path)
    state = payload.get("tw_index_derivatives_tick_state")
    legacy = not isinstance(state, dict)
    if legacy:
        # Schema 4 was emitted before tick mode adopted the shared checkpoint
        # envelope.  Keep it readable, but all new writes use namespaced state.
        state = payload
    expected = {
        "schema_version": (
            4 if legacy else TICK_TRAINING_STATE_VERSION
        ),
        "dataset_schema_version": TICK_DATASET_SCHEMA_VERSION,
        "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
        "execution_contract_version": _execution_contract_version(config),
        "execution_mode": config.trading.execution_mode,
        "sample_order_contract": training_mode_spec(
            config.trading.execution_mode
        ).sample_order_contract,
        "feature_names": list(_feature_names(config)),
        "dataset_source_manifest_sha256": dataset.manifest["source_manifest_sha256"],
        "configuration_fingerprint": _tick_configuration_fingerprint(config),
    }
    for key, value in expected.items():
        if state.get(key) != value:
            raise RuntimeError(f"incompatible tick checkpoint {key}: {path}")
    if not legacy and state.get("checkpoint_schema_version") != TICK_CHECKPOINT_SCHEMA_VERSION:
        raise RuntimeError(f"incompatible tick checkpoint envelope: {path}")
    if optimizer is not None and not legacy:
        _validate_checkpoint_effective_train_batch_size(
            payload,
            effective_train_batch_size=int(config.training.batch_size_train),
            checkpoint_path=path,
        )
    _load_state_dict(model, payload["model_state_dict"])
    if optimizer is not None and payload.get("optimizer_state_dict"):
        optimizer.load_state_dict(payload["optimizer_state_dict"])
    if scaler is not None and payload.get("scaler_state_dict"):
        scaler.load_state_dict(payload["scaler_state_dict"])
    if scheduler is not None and payload.get("scheduler_state_dict"):
        scheduler.load_state_dict(payload["scheduler_state_dict"])
    if optimizer is not None:
        _restore_rng_state(payload.get("rng_state"))
    restored = dict(payload)
    for name in ("normalizer_mean", "normalizer_scale", "normalizer_counts"):
        restored[name] = state[name]
    return restored


def _selected_folds(
    folds: list[TickWalkForwardFold],
    *,
    start_fold: int | None,
    max_folds: int | None,
) -> list[TickWalkForwardFold]:
    selected = [
        fold for fold in folds if start_fold is None or fold.fold_id >= start_fold
    ]
    if max_folds is not None:
        selected = selected[: max(0, int(max_folds))]
    if not selected:
        raise ValueError("no tw_index_derivatives_tick folds selected")
    return selected


def _load_completed_tick_fold_result(
    root: Path,
    fold_id: int,
    *,
    dataset: IndexDerivativesTickDataset,
    config: ExperimentConfig,
) -> FoldResult | None:
    contract_path = root / f"fold_{fold_id:02d}" / "mode_artifact_contract.json"
    if not contract_path.is_file():
        return None
    try:
        actual = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if actual != _tick_artifact_contract(dataset, config):
        return None
    return _load_completed_fold_result(root, fold_id)


def _ensure_dataset(config: ExperimentConfig) -> IndexDerivativesTickDataset:
    data = config.data
    build_index_derivatives_tick_dataset(
        data.index_derivatives_tick_raw_root,
        data.index_derivatives_tick_dataset_root,
        execution_price_source=(data.index_derivatives_tick_execution_price_source),
        bidask_capture_root=data.index_derivatives_tick_bidask_capture_root,
        execution_latency_ms=data.index_derivatives_tick_execution_latency_ms,
        execution_max_wait_ms=(data.index_derivatives_tick_execution_max_wait_ms),
        max_transport_delay_ms=(data.index_derivatives_tick_max_transport_delay_ms),
        session=data.index_derivatives_tick_session,
        decision_interval_seconds=(
            data.index_derivatives_tick_decision_interval_seconds
        ),
        rolling_window_seconds=data.index_derivatives_tick_rolling_window_seconds,
        warmup_seconds=data.index_derivatives_tick_warmup_seconds,
        atm_moneyness_fraction=(data.index_derivatives_tick_atm_moneyness_fraction),
    )
    return load_index_derivatives_tick_dataset(
        data.index_derivatives_tick_dataset_root,
        verify_partition_sha256=(data.index_derivatives_tick_verify_partition_sha256),
    )


def _run_index_derivatives_tick_training_impl(
    config: ExperimentConfig,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
    lifecycle: TrainingRunLifecycle,
) -> list[dict[str, Any]]:
    """Train or infer the shared TX and direct-TXO completed-second contracts."""

    if config.trading.execution_mode not in DERIVATIVES_TICK_MODES:
        raise ValueError(
            "run_index_derivatives_tick_training requires "
            f"one of execution_mode={sorted(DERIVATIVES_TICK_MODES)}"
        )
    if active_strategy not in {"none", "single", "single_gpu"}:
        raise ValueError(
            "tw_index_derivatives_tick currently requires one training process"
        )
    dataset = _ensure_dataset(config)
    folds = build_tick_walk_forward_folds(
        len(dataset.dates),
        min_train_days=config.data.index_derivatives_tick_min_train_days,
        val_days=config.data.index_derivatives_tick_val_days,
        test_days=config.data.index_derivatives_tick_test_days,
    )
    selected_folds = _selected_folds(folds, start_fold=start_fold, max_folds=max_folds)
    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    layout = TrainingArtifactLayout(root)
    lifecycle.start(
        fold_ids=[fold.fold_id for fold in selected_folds],
        dataset_fingerprint=str(dataset.manifest["source_manifest_sha256"]),
        configuration_fingerprint=_tick_configuration_fingerprint(config),
        contract_versions={
            "execution": _execution_contract_version(config),
            "checkpoint": TICK_CHECKPOINT_SCHEMA_VERSION,
            "dataset": TICK_DATASET_SCHEMA_VERSION,
            "features": TICK_FEATURE_CONTRACT_VERSION,
        },
        data_summary={
            "dates": int(len(dataset.dates)),
            "first_date": str(dataset.dates[0]),
            "last_date": str(dataset.dates[-1]),
            "features": list(_feature_names(config)),
        },
        configuration=asdict(config),
        mode_details={
            "dataset_decision_clock": dataset.manifest["decision_clock"],
            "dataset_execution_clock": dataset.manifest["execution_clock"],
            "position_contract": dataset.manifest["position_contract"],
        },
        compatibility_manifest_paths=[
            root / "index_derivatives_tick_run_manifest.json"
        ],
    )
    experiment_manifest = _tick_experiment_manifest(dataset, config)
    results: list[dict[str, Any]] = []
    results_by_fold: dict[int, FoldResult] = {}
    fold_progress = _progress_bar(
        selected_folds,
        desc="Train groups" if mode == "train" else "Inference folds",
        unit="group" if mode == "train" else "fold",
    )
    for group_index, fold in enumerate(fold_progress, start=1):
        fold_dir = layout.fold_dir(fold.fold_id)
        train_dates = np.asarray(
            dataset.dates[fold.train_indices], dtype="datetime64[D]"
        )
        group_name = date_training_group_name(train_dates)
        lifecycle.start_group(
            group_name=group_name,
            group_index=group_index,
            group_total=len(selected_folds),
            fold_ids=[fold.fold_id],
            epoch_total=int(config.training.epochs),
            message=f"Train {group_name}",
        )
        completed = (
            _load_completed_tick_fold_result(
                root,
                fold.fold_id,
                dataset=dataset,
                config=config,
            )
            if mode == "train" and resume
            else None
        )
        if completed is not None:
            results_by_fold[fold.fold_id] = completed
            results.append(
                {
                    "status": "complete",
                    "execution_mode": config.trading.execution_mode,
                    **asdict(completed),
                }
            )
            print(
                f"[tw-index-derivatives-tick] fold={fold.fold_id} complete; skipping",
                flush=True,
            )
            lifecycle.finish_fold(fold.fold_id)
            continue
        fold_dir.mkdir(parents=True, exist_ok=True)
        group_dir = layout.group_dir(group_name)
        group_dir.mkdir(parents=True, exist_ok=True)
        pre_epoch_timing = _PreEpochTimingRecorder(
            layout.group_pre_epoch_timing_path(group_name),
            train_years=_tick_split_years(dataset, fold.train_indices),
            fold_ids=[fold.fold_id],
            scope="tw_index_derivatives_tick_train_group",
        )
        normalizer = dataset.fit_normalizer(
            fold.train_indices,
            option_mode=_option_mode(config),
        )
        pre_epoch_timing.checkpoint(
            "normalizer_fit",
            train_days=int(len(fold.train_indices)),
        )
        canonical_fold = _canonical_tick_fold(dataset, fold)
        feature_names = _feature_names(config)
        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(feature_names),
            num_symbols=(2 if _option_mode(config) else 1),
            feature_names=feature_names,
        ).to(device)
        optimizer = _create_adamw_optimizer(
            model,
            config,
            device,
            label=f"tw-index-derivatives-tick fold {fold.fold_id}",
        )
        pre_epoch_timing.checkpoint(
            "model_and_optimizer_build",
            parameters=int(sum(parameter.numel() for parameter in model.parameters())),
        )
        scaler = GradScaler(
            device="cuda",
            enabled=device.type == "cuda" and amp_dtype == torch.float16,
        )
        manifest_row_key = "option_rows" if _option_mode(config) else "rows"
        training_rows = sum(
            max(
                0,
                int(dataset.manifest["partitions"][int(index)][manifest_row_key])
                - int(config.training.lookback)
                + 1,
            )
            for index in fold.train_indices
        )
        steps_per_epoch = math.ceil(
            training_rows / int(config.training.batch_size_train)
        )
        (
            scheduler,
            scheduler_name,
            scheduler_requires_metric,
            scheduler_interval,
        ) = _create_lr_scheduler(
            optimizer,
            config,
            steps_per_epoch=max(1, steps_per_epoch),
        )
        latest_path = layout.group_checkpoint_path(group_name)
        best_path = layout.best_checkpoint_path(fold.fold_id)
        curve_path = layout.group_curve_path(group_name)
        start_epoch = 1
        best_val_loss = float("inf")
        no_improve = 0
        if resume and latest_path.is_file():
            restored = _load_tick_checkpoint(
                latest_path,
                model=model,
                optimizer=optimizer if mode == "train" else None,
                scaler=scaler if mode == "train" else None,
                scheduler=scheduler if mode == "train" else None,
                dataset=dataset,
                config=config,
            )
            start_epoch = int(restored["epoch"]) + 1
            best_val_loss = float(restored.get("best_val_loss", float("inf")))
            no_improve = int(restored.get("no_improve_epochs", 0))
        curve_lifecycle = _EpochCurveLifecycle(
            curve_path,
            enabled=bool(getattr(config.training, "record_epoch_curve", True)),
            interval=int(config.training.curve_plot_interval),
            async_enabled=bool(config.training.curve_plot_async),
            defer_until_end=bool(
                getattr(config.training, "defer_epoch_curve_plot_until_end", True)
            ),
        )
        curve_lifecycle.prepare_resume(start_epoch)
        pre_epoch_timing.checkpoint(
            "scheduler_resume_and_curve_state",
            start_epoch=int(start_epoch),
            steps_per_epoch=int(steps_per_epoch),
            scheduler=str(scheduler_name),
        )
        print(
            "[tw-index-derivatives-tick] "
            f"fold={fold.fold_id} amp={config.environment.amp_dtype} "
            f"grad_scaler={scaler.is_enabled()} optimizer=AdamW "
            f"scheduler={scheduler_name} steps_per_epoch={steps_per_epoch} "
            f"curve={curve_path}",
            flush=True,
        )
        if bool(config.training.enable_torch_compile):
            compile_allowed, compile_reason = _can_enable_torch_compile(device)
            if not compile_allowed:
                if bool(config.training.strict_no_fallback):
                    raise RuntimeError(
                        "tick torch.compile unavailable while strict_no_fallback=true: "
                        f"{compile_reason}"
                    )
                print(
                    f"[tw-index-derivatives-tick] torch.compile disabled: {compile_reason}",
                    flush=True,
                )
            else:
                try:
                    model = torch.compile(
                        model,
                        mode=str(config.training.torch_compile_mode),
                        fullgraph=True,
                        dynamic=False,
                        options=_torch_compile_options(
                            config.training.torch_compile_mode,
                            cudagraphs=False,
                        ),
                    )
                except Exception as exc:
                    if bool(config.training.strict_no_fallback):
                        raise RuntimeError(
                            "tick torch.compile failed while "
                            "strict_no_fallback=true"
                        ) from exc
                    print(
                        "[tw-index-derivatives-tick] torch.compile failed; "
                        f"using eager model: {type(exc).__name__}: {exc}",
                        flush=True,
                    )

        pre_epoch_timing.checkpoint("pre_epoch_total", next_epoch=int(start_epoch))

        if mode == "train":
            patience = max(
                1,
                int(
                    math.ceil(
                        config.training.epochs
                        * config.training.early_stopping_no_improve_ratio
                    )
                ),
            )
            epoch_progress = _progress_bar(
                range(start_epoch, int(config.training.epochs) + 1),
                desc=" Epochs",
                unit="epoch",
            )
            for epoch in epoch_progress:
                epoch_started = time.perf_counter()
                lifecycle.set_phase(
                    "training",
                    fold_id=fold.fold_id,
                    epoch=epoch,
                    epoch_total=int(config.training.epochs),
                    work_total=len(fold.train_indices),
                    work_unit="day",
                    message=f"Train {group_name} epoch {epoch}",
                )
                train_summary, _ = _run_split(
                    name=f"fold{fold.fold_id}/train/epoch{epoch}",
                    indices=fold.train_indices,
                    dataset=dataset,
                    normalizer=normalizer,
                    model=model,
                    config=config,
                    device=device,
                    amp_dtype=amp_dtype,
                    optimizer=optimizer,
                    scaler=scaler,
                    scheduler=(scheduler if scheduler_interval == "step" else None),
                    progress_lifecycle=lifecycle,
                )
                lifecycle.set_phase(
                    "validation",
                    fold_id=fold.fold_id,
                    epoch=epoch,
                    epoch_total=int(config.training.epochs),
                    work_total=len(fold.val_indices),
                    work_unit="day",
                    message=f"Validate {group_name} epoch {epoch}",
                )
                val_summary, val_rows = _run_split(
                    name=f"fold{fold.fold_id}/val/epoch{epoch}",
                    indices=fold.val_indices,
                    dataset=dataset,
                    normalizer=normalizer,
                    model=model,
                    config=config,
                    device=device,
                    amp_dtype=amp_dtype,
                    progress_lifecycle=lifecycle,
                )
                val_loss = -float(val_summary["mean_daily_log_return"])
                improved = val_loss < best_val_loss - float(
                    config.training.early_stopping_min_delta
                )
                if improved:
                    best_val_loss = val_loss
                    no_improve = 0
                else:
                    no_improve += 1
                if scheduler is not None and scheduler_interval != "step":
                    if scheduler_requires_metric:
                        scheduler.step(val_loss)
                    else:
                        scheduler.step()
                checkpoint_state = _tick_checkpoint_state(
                    dataset=dataset,
                    normalizer=normalizer,
                    config=config,
                )
                _save_group_checkpoint(
                    latest_path,
                    train_years=canonical_fold.train_years,
                    epoch=epoch,
                    model=model,
                    optimizer=optimizer,
                    scaler=scaler,
                    experiment_manifest=experiment_manifest,
                    effective_train_batch_size=int(
                        config.training.batch_size_train
                    ),
                    scheduler=scheduler,
                    extra_payload={
                        "best_val_loss": float(best_val_loss),
                        "fold_id": int(fold.fold_id),
                        "tw_index_derivatives_tick_state": checkpoint_state,
                    },
                    no_improve_epochs=no_improve,
                    early_stop_patience=patience,
                    early_stopping_no_improve_ratio=float(
                        config.training.early_stopping_no_improve_ratio
                    ),
                    early_stop_val_interval_epochs=1,
                    check_finite=bool(config.training.checkpoint_finite_check),
                )
                if improved:
                    _save_fold_checkpoint(
                        best_path,
                        fold=canonical_fold,
                        epoch=epoch,
                        best_val_loss=best_val_loss,
                        model=model,
                        optimizer=optimizer,
                        scaler=scaler,
                        experiment_manifest=experiment_manifest,
                        extra_payload={
                            "tw_index_derivatives_tick_state": checkpoint_state,
                        },
                        check_finite=bool(
                            config.training.checkpoint_finite_check
                        ),
                    )
                    pl.DataFrame(val_rows).write_parquet(
                        fold_dir / "best_validation_daily_curve.parquet"
                    )
                curve_lifecycle.record(
                    {
                        "epoch": int(epoch),
                        "fold_id": int(fold.fold_id),
                        "train_loss": -float(
                            train_summary["mean_daily_log_return"]
                        ),
                        "val_mean": float(val_loss),
                        "test_mean": None,
                        "lr": float(optimizer.param_groups[0]["lr"]),
                        "no_improve": int(no_improve),
                        "best_val_loss": float(best_val_loss),
                        "improved": int(improved),
                        "train_total_s": float(
                            train_summary.get("elapsed_seconds", 0.0)
                        ),
                        "val_total_s": float(
                            val_summary.get("elapsed_seconds", 0.0)
                        ),
                        "epoch_total_s": float(
                            time.perf_counter() - epoch_started
                        ),
                    },
                    request_plot=(
                        epoch == start_epoch
                        or epoch % curve_lifecycle.interval == 0
                        or epoch == int(config.training.epochs)
                    ),
                )
                lifecycle.finish_epoch(
                    epoch=epoch,
                    epoch_total=int(config.training.epochs),
                    metrics={
                        "train_loss": -float(
                            train_summary["mean_daily_log_return"]
                        ),
                        "val_mean": val_loss,
                        "test_mean": None,
                        "best_val_loss": best_val_loss,
                        "no_improve": no_improve,
                        "lr": optimizer.param_groups[0]["lr"],
                        "epoch_total_s": time.perf_counter() - epoch_started,
                    },
                )
                epoch_progress.set_postfix(
                    {
                        "train_loss": (
                            f"{-float(train_summary['mean_daily_log_return']):.8f}"
                        ),
                        "val_mean": f"{val_loss:.8f}",
                        "best_val": f"{best_val_loss:.8f}",
                        "no_improve": no_improve,
                        "lr": f"{optimizer.param_groups[0]['lr']:.2e}",
                        "wall": f"{time.perf_counter() - epoch_started:.2f}s",
                    }
                )
                if no_improve >= patience:
                    print(
                        f"[tw-index-derivatives-tick] fold={fold.fold_id} "
                        f"early_stop epoch={epoch} patience={patience}",
                        flush=True,
                    )
                    break
            curve_lifecycle.flush()

        if not best_path.is_file():
            raise RuntimeError(f"tick best checkpoint is missing: {best_path}")
        best = _load_tick_checkpoint(
            best_path,
            model=model,
            optimizer=None,
            scaler=None,
            scheduler=None,
            dataset=dataset,
            config=config,
        )
        checkpoint_normalizer = TickFeatureNormalizer(
            mean=np.asarray(best["normalizer_mean"], dtype=np.float32),
            scale=np.asarray(best["normalizer_scale"], dtype=np.float32),
            counts=np.asarray(best["normalizer_counts"], dtype=np.int64),
        )
        lifecycle.set_phase(
            "validation",
            fold_id=fold.fold_id,
            work_total=len(fold.val_indices),
            work_unit="day",
            message=f"Fold {fold.fold_id} validation best",
        )
        val_summary, val_rows = _run_split(
            name=f"fold{fold.fold_id}/validation_best",
            indices=fold.val_indices,
            dataset=dataset,
            normalizer=checkpoint_normalizer,
            model=model,
            config=config,
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
        test_summary, test_rows = _run_split(
            name=f"fold{fold.fold_id}/test_best",
            indices=fold.test_indices,
            dataset=dataset,
            normalizer=checkpoint_normalizer,
            model=model,
            config=config,
            device=device,
            amp_dtype=amp_dtype,
            progress_lifecycle=lifecycle,
        )
        pl.DataFrame(val_rows).write_parquet(
            fold_dir / "validation_daily_curve.parquet"
        )
        pl.DataFrame(test_rows).write_parquet(fold_dir / "test_daily_curve.parquet")
        symbol_names = (
            ["TXO_CALL", "TXO_PUT"] if _option_mode(config) else ["TX"]
        )
        val_backtest, _ = _tick_backtest_result(
            val_rows,
            execution_mode=config.trading.execution_mode,
            symbols=len(symbol_names),
        )
        test_backtest, test_dates = _tick_backtest_result(
            test_rows,
            execution_mode=config.trading.execution_mode,
            symbols=len(symbol_names),
        )

        fold_result = FoldResult(
            fold_id=int(fold.fold_id),
            train_years=canonical_fold.train_years,
            val_years=canonical_fold.val_years,
            test_years=canonical_fold.test_years,
            best_val_loss=float(best_val_loss),
            val_ic=ic_summary(np.asarray([], dtype=np.float64)),
            val_metrics={
                **_tick_report_metrics(val_backtest, val_summary),
                "tick_best_epoch": float(best["epoch"]),
            },
            test_ic=ic_summary(np.asarray([], dtype=np.float64)),
            test_metrics={
                **_tick_report_metrics(test_backtest, test_summary),
                "tick_best_epoch": float(best["epoch"]),
            },
        )
        _save_fold_output_artifacts(
            fold_dir=fold_dir,
            fold_result=fold_result,
            model=model,
            test_backtest=test_backtest,
            test_dates=test_dates,
            symbols=symbol_names,
            config=config,
            deployment_backtest=_prefix_backtest_result(
                test_backtest,
                int(test_dates.size),
            ),
            deployment_dates=test_dates,
            print_report=True,
            write_plots=True,
            mark_complete=True,
        )
        write_training_json(
            fold_dir / "mode_artifact_contract.json",
            _tick_artifact_contract(dataset, config),
        )
        results_by_fold[fold.fold_id] = fold_result
        _refresh_walkforward_artifacts(
            root,
            list(results_by_fold.values()),
        )
        result = {
            "status": "complete",
            "execution_mode": config.trading.execution_mode,
            **asdict(fold_result),
        }
        results.append(result)
        lifecycle.finish_fold(fold.fold_id)
    return results


def run_index_derivatives_tick_training(
    config: ExperimentConfig,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
) -> list[dict[str, Any]]:
    """Run TX/TXO tick training inside the universal lifecycle contract."""

    lifecycle = TrainingRunLifecycle(
        output_dir,
        execution_mode=config.trading.execution_mode,
        run_mode=mode,
        strategy=active_strategy,
        model_name=config.training.model_name,
    )
    configuration = asdict(config)
    lifecycle.start(
        fold_ids=[],
        configuration_fingerprint=_tick_configuration_fingerprint(config),
        contract_versions={
            "execution": _execution_contract_version(config),
            "checkpoint": TICK_CHECKPOINT_SCHEMA_VERSION,
            "dataset": TICK_DATASET_SCHEMA_VERSION,
            "features": TICK_FEATURE_CONTRACT_VERSION,
        },
        configuration=configuration,
    )
    try:
        results = _run_index_derivatives_tick_training_impl(
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


__all__ = [
    "TickWalkForwardFold",
    "build_tick_walk_forward_folds",
    "run_index_derivatives_tick_training",
]
