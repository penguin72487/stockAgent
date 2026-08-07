from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Iterable

import numpy as np
import polars as pl
import torch
from torch import nn
from torch.amp import GradScaler

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
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import ExperimentConfig
from stockagent.data.tw_minute import (
    MINUTE_FEATURE_COLUMNS,
    MinuteDatasetIndex,
    MinuteDayPanel,
    MinuteFeatureNormalizer,
    load_minute_dataset_index,
)
from stockagent.data.walkforward import (
    WalkForwardFold,
    build_expanding_year_folds,
    validate_walk_forward_year_contract,
)
from stockagent.models.factory import build_model
from stockagent.evaluation.metrics import ic_summary
from stockagent.training.trainer import (
    FoldResult,
    TimingBreakdown,
    _EpochCurveLifecycle,
    _PreEpochTimingRecorder,
    _autocast_context,
    _can_enable_torch_compile,
    _create_adamw_optimizer,
    _create_lr_scheduler,
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
    _restore_rng_state,
    _save_fold_output_artifacts,
    _save_fold_checkpoint,
    _save_group_checkpoint,
    _stable_fingerprint,
    _step_batch_lr_scheduler,
    _timing_curve_payload,
    _torch_compile_options,
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


MINUTE_EXECUTION_CONTRACT_VERSION = 1
MINUTE_TRAINING_CONTRACT_VERSION = 3


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
        }
    )


def _minute_configuration_fingerprint(config: ExperimentConfig) -> str:
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
            "amp_dtype": config.environment.amp_dtype,
            "transformer_base_portfolio": asdict(
                config.training.transformer_base_portfolio
            ),
        }
    )


def _minute_artifact_contract(
    dataset: MinuteDatasetIndex,
    config: ExperimentConfig,
) -> dict[str, Any]:
    return {
        **canonical_mode_artifact_contract(config.trading.execution_mode),
        "execution_contract_version": MINUTE_EXECUTION_CONTRACT_VERSION,
        "training_contract_version": MINUTE_TRAINING_CONTRACT_VERSION,
        "dataset_fingerprint": _minute_dataset_fingerprint(dataset),
        "configuration_fingerprint": _minute_configuration_fingerprint(config),
        "benchmark_name": str(config.data.benchmark_name),
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
        maximum_name_weight=float(trading.tw_minute_max_name_weight),
        maximum_volume_participation=participation,
        maximum_order_notional=float(trading.tw_minute_max_order_notional),
        slippage_bps_per_side=float(trading.tw_minute_slippage_bps_per_side),
    )


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


def _batched_slab_logits(
    model_forward: Callable[[torch.Tensor, torch.Tensor], Any],
    feature_slabs: torch.Tensor,
    mask: torch.Tensor,
) -> torch.Tensor:
    output = model_forward(feature_slabs, mask)
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
            "tw_minute batched panel-slab model must return [D*C,S] logits"
        )
    return output


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
    *,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    config: MinuteExecutionConfig,
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
        step = execute_minute_target(
            state,
            target_weights=targets[:, local],
            execution_mask=execution_masks[:, local],
            execution_open=execution_opens[:, local],
            exit_close=exit_closes[:, local],
            future_volume_shares=future_volumes[:, local],
            buy_fee_rates=buy_fee_rates,
            sell_fee_rates=sell_fee_rates,
            config=config,
            allow_new_entries=allow_new_entries[local],
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


def _decision_indices(config: ExperimentConfig) -> np.ndarray:
    first = max(
        int(config.trading.tw_minute_first_decision_minute),
        int(config.training.lookback),
    )
    last = int(config.trading.tw_minute_last_decision_minute)
    # Minute label m maps to zero-based dense index m-1.
    return np.arange(first - 1, last, dtype=np.int64)


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
            )
        )
    )


class _MinuteDayCache:
    """Fold-normalized, byte-bounded host cache with parallel parquet loading."""

    def __init__(
        self,
        dataset: MinuteDatasetIndex,
        normalizer: MinuteFeatureNormalizer,
        *,
        maximum_bytes: int,
        workers: int,
    ) -> None:
        self.dataset = dataset
        self.normalizer = normalizer
        self.maximum_bytes = max(0, int(maximum_bytes))
        self._cache: OrderedDict[int, MinuteDayPanel] = OrderedDict()
        self._cache_bytes = 0
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(workers)),
            thread_name_prefix="tw-minute-parquet",
        )
        self.hits = 0
        self.misses = 0

    @property
    def cache_bytes(self) -> int:
        return self._cache_bytes

    def close(self) -> None:
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


def _stack_day_field(
    days: list[MinuteDayPanel],
    name: str,
    *,
    padded_days: int,
) -> torch.Tensor:
    values = [getattr(day, name) for day in days]
    if len(days) < padded_days:
        template = values[0]
        values.extend(np.zeros_like(template) for _ in range(padded_days - len(days)))
    return torch.from_numpy(np.stack(values, axis=0))


def _run_day_batch(
    *,
    model: nn.Module,
    model_forward: Callable[[torch.Tensor, torch.Tensor], Any],
    execution_forward: Callable[..., tuple[torch.Tensor, ...]],
    days: list[MinuteDayPanel],
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
    benchmark_symbol_index: int,
) -> tuple[list[dict[str, float]], np.ndarray, TimingBreakdown]:
    training = optimizer is not None
    model.train(training)
    if not days or len(days) > int(day_batch_size):
        raise ValueError("tw_minute day batch must contain 1..day_batch_size days")
    real_days = len(days)
    day_batch_size = int(day_batch_size)
    decision_chunk_rows = max(1, int(decision_chunk_rows))
    decisions = _decision_indices(config)
    state = initialize_minute_execution_state(
        num_symbols=days[0].features.shape[1],
        config=execution_config,
        device=device,
        batch_size=day_batch_size,
    )
    total_turnover = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_fees = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_slippage = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    total_log_return = torch.zeros(day_batch_size, device=device, dtype=torch.float32)
    session_close_weights = torch.zeros_like(state.shares)
    timing = TimingBreakdown(batches=1)
    non_blocking = bool(config.training.non_blocking_transfer)
    prepare_started = time.perf_counter()
    cpu_fields = {
        name: _stack_day_field(days, name, padded_days=day_batch_size)
        for name in (
            "features",
            "feature_mask",
            "execution_mask",
            "execution_open",
            "exit_close",
            "future_volume_shares",
            "session_close",
            "session_exit_mask",
        )
    }
    timing.batch_prepare_s += time.perf_counter() - prepare_started
    transfer_started = time.perf_counter()
    day_features = cpu_fields["features"].to(
        device=device, dtype=torch.float32, non_blocking=non_blocking
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
    timing.transfer_s += time.perf_counter() - transfer_started
    chunks = list(range(0, len(decisions), decision_chunk_rows))
    if training:
        optimizer.zero_grad(set_to_none=True)
    for chunk_number, start in enumerate(chunks):
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
        forward_started = time.perf_counter()
        with torch.set_grad_enabled(training), _autocast_context(device, amp_dtype):
            logits = _batched_slab_logits(
                model_forward, feature_slabs, model_masks
            ).reshape(day_batch_size, decision_chunk_rows, -1)
        timing.forward_s += time.perf_counter() - forward_started
        targets = minute_target_weights_from_logits(
            logits.reshape(day_batch_size * decision_chunk_rows, -1),
            model_masks.reshape(day_batch_size * decision_chunk_rows, -1),
            gross_exposure=execution_config.gross_exposure,
            maximum_name_weight=execution_config.maximum_name_weight,
        ).reshape(day_batch_size, decision_chunk_rows, -1)
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

        is_last_chunk = chunk_number == len(chunks) - 1
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

        if training:
            if scaler is None:
                raise RuntimeError("tw_minute training requires the canonical GradScaler")
            loss = -chunk_log_utility[:real_days].sum() / float(
                real_days * len(decisions)
            )
            if not torch.isfinite(loss):
                raise RuntimeError(f"non-finite tw_minute training loss: {loss}")
            backward_started = time.perf_counter()
            scaler.scale(loss).backward()
            timing.backward_s += time.perf_counter() - backward_started
        state = state.detached()

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
    rows: list[dict[str, float]] = []
    for day_index, day in enumerate(days):
        final_equity = float(metrics[0, day_index])
        first_execution_index = int(decisions[0])
        benchmark_open = float(
            day.execution_open[first_execution_index, benchmark_symbol_index]
        )
        benchmark_close = float(day.session_close[benchmark_symbol_index])
        benchmark_valid = bool(
            day.execution_mask[first_execution_index, benchmark_symbol_index]
            and day.session_exit_mask[benchmark_symbol_index]
            and math.isfinite(benchmark_open)
            and math.isfinite(benchmark_close)
            and benchmark_open > 0.0
            and benchmark_close > 0.0
        )
        if not benchmark_valid:
            raise RuntimeError(
                "tw_minute benchmark is not executable from the first strategy "
                f"execution opportunity through session close: date={day.trade_date}"
            )
        rows.append(
            {
                "date": str(day.trade_date.astype("datetime64[D]")),
                "initial_equity": execution_config.initial_equity,
                "final_equity": final_equity,
                "net_return": final_equity / execution_config.initial_equity - 1.0,
                "log_return": float(metrics[1, day_index]),
                "benchmark_log_return": float(
                    math.log(benchmark_close / benchmark_open)
                ),
                "turnover_notional": float(metrics[2, day_index]),
                "explicit_fees": float(metrics[3, day_index]),
                "slippage_cost": float(metrics[4, day_index]),
                "decisions": float(len(decisions)),
            }
        )
    return rows, close_weights, timing


def _summarize_days(rows: list[dict[str, float]]) -> dict[str, float]:
    if not rows:
        raise RuntimeError("tw_minute split produced no day results")
    returns = np.asarray([row["net_return"] for row in rows], dtype=np.float64)
    log_returns = np.asarray([row["log_return"] for row in rows], dtype=np.float64)
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


def _run_split(
    *,
    name: str,
    indices: Iterable[int],
    dataset: MinuteDatasetIndex,
    normalizer: MinuteFeatureNormalizer,
    model: nn.Module,
    model_forward: Callable[[torch.Tensor, torch.Tensor], Any],
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
    benchmark_symbol_index: int,
    progress_lifecycle: TrainingRunLifecycle | None = None,
) -> tuple[dict[str, float], list[dict[str, float]], np.ndarray, TimingBreakdown]:
    rows: list[dict[str, float]] = []
    selected = chronological_session_indices(indices)
    started = time.monotonic()
    day_batch_size = int(
        config.training.batch_size_train
        if optimizer is not None
        else config.training.batch_size_eval
    )
    decision_chunk_rows = int(config.training.minute_decision_chunk_rows)
    timing = TimingBreakdown()
    close_weight_parts: list[np.ndarray] = []
    day_progress = _progress_bar(
        desc=f" {name}",
        unit="day",
        total=len(selected),
        leave=False,
    )
    try:
        for start in range(0, len(selected), day_batch_size):
            batch_indices = selected[start : start + day_batch_size]
            fetch_started = time.perf_counter()
            days = day_cache.get_many(batch_indices)
            timing.fetch_s += time.perf_counter() - fetch_started
            batch_rows, batch_close_weights, batch_timing = _run_day_batch(
                model=model,
                model_forward=model_forward,
                execution_forward=execution_forward,
                days=days,
                config=config,
                execution_config=execution_config,
                buy_fee_rates=buy_fee_rates,
                sell_fee_rates=sell_fee_rates,
                device=device,
                amp_dtype=amp_dtype,
                day_batch_size=day_batch_size,
                decision_chunk_rows=decision_chunk_rows,
                optimizer=optimizer,
                scaler=scaler,
                scheduler=scheduler,
                gradient_clip_norm=float(config.training.grad_clip_norm),
                benchmark_symbol_index=benchmark_symbol_index,
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
                    "batch_date": rows[-1]["date"],
                    "batch_ret": f"{rows[-1]['net_return']:.6f}",
                    "cache": f"{day_cache.cache_bytes / (1024**3):.2f}GiB",
                    "hit": day_cache.hits,
                    "miss": day_cache.misses,
                    "opt_step": (
                        f"{start // day_batch_size + 1}/"
                        f"{math.ceil(len(selected) / day_batch_size)}"
                        if optimizer is not None
                        else "-"
                    ),
                },
                refresh=False,
            )
            day_progress.update(len(batch_indices))
            if progress_lifecycle is not None:
                progress_lifecycle.update_work(
                    completed=len(rows),
                    total=len(selected),
                    unit="day",
                    last_sample=rows[-1]["date"],
                    metrics={
                        "last_return": rows[-1]["net_return"],
                        "cache_gib": day_cache.cache_bytes / (1024**3),
                        "cache_hits": day_cache.hits,
                        "cache_misses": day_cache.misses,
                    },
                )
    finally:
        day_progress.close()
    summary = _summarize_days(rows)
    elapsed = time.monotonic() - started
    summary["elapsed_seconds"] = elapsed
    timing.total_s = elapsed
    close_weights = np.concatenate(close_weight_parts, axis=0)
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
    lifecycle: TrainingRunLifecycle,
) -> list[dict[str, Any]]:
    """Train or infer the dedicated causal full-market one-minute contract."""

    if config.trading.execution_mode != "tw_minute":
        raise ValueError("run_minute_training requires execution_mode='tw_minute'")
    if active_strategy not in {"none", "single", "single_gpu"}:
        raise ValueError(
            "tw_minute currently requires training.multi_gpu_strategy='none'; "
            "day-level state sharding is not yet implemented"
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
    dataset = load_minute_dataset_index(
        config.data.minute_parquet_root,
        require_research_ready=bool(config.data.minute_require_research_ready),
        verify_partition_sha256=bool(config.data.minute_verify_partition_sha256),
        progress=_progress,
    )
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
    try:
        benchmark_symbol_index = dataset.symbols.index(benchmark_name)
    except ValueError as exc:
        raise RuntimeError(
            f"tw_minute benchmark {benchmark_name!r} is absent from the dataset"
        ) from exc
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
    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    execution_config = _execution_config(config)
    buy_rates_np, sell_rates_np = effective_fee_rate_vectors(
        dataset.symbols, "tw_day_trade", fee_schedule=_fee_schedule(config)
    )
    buy_rates = torch.from_numpy(buy_rates_np.astype(np.float32)).to(device=device)
    sell_rates = torch.from_numpy(sell_rates_np.astype(np.float32)).to(device=device)
    setup_timing.checkpoint("device_and_execution_contract", device=str(device))
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
            "symbols": dataset.num_symbols,
            "dates": len(dataset.dates),
            "source_gap_symbols": dataset.manifest.get("source_gap_symbols", 0),
            "contract_unavailable_symbols": dataset.manifest.get(
                "contract_unavailable_symbols", 0
            ),
        },
        configuration=asdict(config),
        mode_details={
            "dataset_decision_clock": dataset.manifest["decision_clock"],
            "dataset_execution_clock": dataset.manifest["execution_clock"],
            "benchmark_name": benchmark_name,
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
        ).to(device)
        pre_epoch_timing.checkpoint(
            "model_build",
            parameters=int(sum(parameter.numel() for parameter in model.parameters())),
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
        steps_per_epoch = int(
            math.ceil(
                len(fold.train_indices) / int(config.training.batch_size_train)
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

        def eager_slab_forward(
            feature_slabs: torch.Tensor, mask: torch.Tensor
        ) -> Any:
            return model.forward_from_batched_panel_slabs(
                feature_slabs, mask, return_aux=False
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
                buy_fee_rates=buy_rates,
                sell_fee_rates=sell_rates,
                config=execution_config,
            )

        model_forward: Callable[[torch.Tensor, torch.Tensor], Any] = eager_slab_forward
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
                    model_forward = torch.compile(
                        eager_slab_forward,
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
                        "enabled:fullgraph:fixed_shape:model+minute_ledger"
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
        )
        _progress(
            f"[Train {train_years}] minute vectorization: "
            f"days_per_train_batch={config.training.batch_size_train} "
            f"days_per_eval_batch={config.training.batch_size_eval} "
            f"decisions_per_chunk={config.training.minute_decision_chunk_rows} "
            f"flat_train_rows={int(config.training.batch_size_train) * int(config.training.minute_decision_chunk_rows)} "
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
        )
        curve_lifecycle.prepare_resume(start_epoch)
        if curve_lifecycle.defer_until_end and record_epoch_curve:
            _progress(
                f"[Train {train_years}] epoch curve plotting deferred until "
                "training completes"
            )
        curve_plot_request_interval = curve_lifecycle.interval
        val_interval = max(1, int(config.training.val_interval_epochs))
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
                    model=model,
                    model_forward=model_forward,
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
                    benchmark_symbol_index=benchmark_symbol_index,
                    progress_lifecycle=lifecycle,
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
                        model=model,
                        model_forward=model_forward,
                        execution_forward=execution_forward,
                        day_cache=day_cache,
                        config=config,
                        execution_config=execution_config,
                        buy_fee_rates=buy_rates,
                        sell_fee_rates=sell_rates,
                        device=device,
                        amp_dtype=amp_dtype,
                        benchmark_symbol_index=benchmark_symbol_index,
                        progress_lifecycle=lifecycle,
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
                    "test_mean": None,
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
                    "minute_days_per_train_batch": int(
                        config.training.batch_size_train
                    ),
                    "minute_days_per_eval_batch": int(
                        config.training.batch_size_eval
                    ),
                    "minute_decision_chunk_rows": int(
                        config.training.minute_decision_chunk_rows
                    ),
                    "minute_flat_train_model_rows": int(
                        config.training.batch_size_train
                        * config.training.minute_decision_chunk_rows
                    ),
                    "minute_optimizer_steps_per_epoch": int(steps_per_epoch),
                    "minute_train_cache_hits": int(train_cache_hits),
                    "minute_train_cache_misses": int(train_cache_misses),
                    "minute_val_cache_hits": int(val_cache_hits),
                    "minute_val_cache_misses": int(val_cache_misses),
                    "minute_cache_gib": float(
                        day_cache.cache_bytes / (1024**3)
                    ),
                    # These are host enqueue/wait buckets. CUDA event timing is
                    # intentionally not fabricated when timing_synchronized=0.
                    "minute_train_ledger_host_s": float(train_timing.backtest_s),
                    "minute_val_ledger_host_s": float(val_timing.backtest_s),
                    **_timing_curve_payload(
                        train_timing=train_timing,
                        val_timing=val_timing,
                        val_eval_s=val_timing.total_s,
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
                        "test_mean": None,
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
            model=model,
            model_forward=model_forward,
            execution_forward=execution_forward,
            day_cache=day_cache,
            config=config,
            execution_config=execution_config,
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            device=device,
            amp_dtype=amp_dtype,
            benchmark_symbol_index=benchmark_symbol_index,
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
            model=model,
            model_forward=model_forward,
            execution_forward=execution_forward,
            day_cache=day_cache,
            config=config,
            execution_config=execution_config,
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            device=device,
            amp_dtype=amp_dtype,
            benchmark_symbol_index=benchmark_symbol_index,
            progress_lifecycle=lifecycle,
        )
        pl.DataFrame(val_rows).write_parquet(fold_dir / "validation_daily_curve.parquet")
        pl.DataFrame(test_rows).write_parquet(fold_dir / "test_daily_curve.parquet")
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
        write_training_json(
            fold_dir / "mode_artifact_contract.json",
            _minute_artifact_contract(dataset, config),
        )
        results_by_fold[fold.fold_id] = fold_result
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
        configuration_fingerprint=_stable_fingerprint(configuration),
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


__all__ = ["run_minute_training"]
