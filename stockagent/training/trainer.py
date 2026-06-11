from __future__ import annotations

import json
import logging
import math
import os
import pickle
import shutil
import subprocess
import sys
import time
import gc
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import numpy as np
import torch
from torch.profiler import ProfilerActivity, profile
from torch import nn
from torch.amp import GradScaler, autocast
from torch.utils.data import DataLoader
from tqdm import tqdm

from stockagent.backtest.report import (
    compute_metrics,
    generate_annual_report,
    plot_annual_performance,
    plot_equity_curve,
    plot_equity_curve_log,
    plot_first_year_fold_metric_bars,
    plot_first_year_turnover_concentration,
    plot_fold_first_year_returns,
)
from stockagent.backtest.simulator import (
    BacktestResult,
    BacktestResultTensor,
    HoldingsRecord,
    get_backtest_compile_stats,
    get_backtest_prep_compile_stats,
    get_backtest_runtime_stats,
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.config import ExperimentConfig
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.evaluation.metrics import compute_ic_series_torch, ic_summary
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.training.dataset import CrossSectionalDataset, collate_batch
from stockagent.training.loss import get_loss_runtime_stats, risk_aware_loss
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


@dataclass(slots=True)
class FoldResult:
    fold_id: int
    train_years: list[int]
    val_years: list[int]
    test_years: list[int]
    best_val_loss: float
    val_ic: dict[str, float]
    val_metrics: dict[str, float]
    test_ic: dict[str, float]
    test_metrics: dict[str, float]
    test_integer_metrics: dict[str, float] | None = None


@dataclass(slots=True)
class FoldRuntimeContext:
    fold: WalkForwardFold
    fold_dir: Path
    val_ds: CrossSectionalDataset
    test_ds: CrossSectionalDataset
    checkpoint_best_path: Path
    best_val_loss: float = float("inf")


@dataclass(slots=True)
class TimingBreakdown:
    total_s: float = 0.0
    fetch_s: float = 0.0
    transfer_s: float = 0.0
    forward_s: float = 0.0
    model_forward_s: float = 0.0
    model_forward_cuda_s: float = 0.0
    factor_aug_s: float = 0.0
    factor_aug_cuda_s: float = 0.0
    loss_s: float = 0.0
    loss_cuda_s: float = 0.0
    portfolio_state_s: float = 0.0
    backward_s: float = 0.0
    grad_s: float = 0.0
    grad_cuda_s: float = 0.0
    clip_s: float = 0.0
    finite_check_s: float = 0.0
    step_s: float = 0.0
    step_cuda_s: float = 0.0
    backtest_s: float = 0.0
    backtest_prepare_s: float = 0.0
    backtest_runner_s: float = 0.0
    backtest_finalize_s: float = 0.0
    ic_s: float = 0.0
    metrics_s: float = 0.0
    concat_s: float = 0.0
    save_s: float = 0.0
    plot_s: float = 0.0
    sync_s: float = 0.0
    cpu_gpu_sync_s: float = 0.0
    gc_s: float = 0.0
    batches: int = 0
    cuda_events: list[tuple[str, torch.cuda.Event, torch.cuda.Event]] = field(default_factory=list)


def _log_timing(label: str, timing: TimingBreakdown) -> None:
    parts = [f"[profile] {label}: total={timing.total_s:.3f}s"]
    if timing.batches:
        parts.append(f"batches={timing.batches}")
    for name in (
        "fetch_s",
        "transfer_s",
        "forward_s",
        "model_forward_s",
        "model_forward_cuda_s",
        "factor_aug_s",
        "factor_aug_cuda_s",
        "loss_s",
        "loss_cuda_s",
        "portfolio_state_s",
        "backward_s",
        "grad_s",
        "grad_cuda_s",
        "clip_s",
        "finite_check_s",
        "step_s",
        "step_cuda_s",
        "backtest_s",
        "backtest_prepare_s",
        "backtest_runner_s",
        "backtest_finalize_s",
        "ic_s",
        "metrics_s",
        "concat_s",
        "save_s",
        "plot_s",
        "sync_s",
        "cpu_gpu_sync_s",
        "gc_s",
    ):
        value = getattr(timing, name)
        if value > 0:
            parts.append(f"{name[:-2]}={value:.3f}s")
    print(" ".join(parts))


def _add_timing(dst: TimingBreakdown, src: TimingBreakdown) -> None:
    for name in (
        "total_s",
        "fetch_s",
        "transfer_s",
        "forward_s",
        "model_forward_s",
        "model_forward_cuda_s",
        "factor_aug_s",
        "factor_aug_cuda_s",
        "loss_s",
        "loss_cuda_s",
        "portfolio_state_s",
        "backward_s",
        "grad_s",
        "grad_cuda_s",
        "clip_s",
        "finite_check_s",
        "step_s",
        "step_cuda_s",
        "backtest_s",
        "backtest_prepare_s",
        "backtest_runner_s",
        "backtest_finalize_s",
        "ic_s",
        "metrics_s",
        "concat_s",
        "save_s",
        "plot_s",
        "sync_s",
        "cpu_gpu_sync_s",
        "gc_s",
    ):
        setattr(dst, name, getattr(dst, name) + getattr(src, name))
    dst.batches += src.batches
    dst.cuda_events.extend(src.cuda_events)


class _CudaTimingRecorder:
    def __init__(self, timing: TimingBreakdown, attr: str, device: torch.device):
        self.timing = timing
        self.attr = attr
        self.enabled = device.type == "cuda" and torch.cuda.is_available()
        self.start: torch.cuda.Event | None = None
        self.end: torch.cuda.Event | None = None

    def __enter__(self) -> "_CudaTimingRecorder":
        if self.enabled:
            self.start = torch.cuda.Event(enable_timing=True)
            self.end = torch.cuda.Event(enable_timing=True)
            self.start.record()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.enabled and self.start is not None and self.end is not None:
            self.end.record()
            self.timing.cuda_events.append((self.attr, self.start, self.end))


def _cuda_timing(timing: TimingBreakdown, attr: str, device: torch.device) -> _CudaTimingRecorder:
    return _CudaTimingRecorder(timing, attr, device)


def _flush_cuda_timing_events(timing: TimingBreakdown) -> None:
    if not timing.cuda_events:
        return
    pending = list(timing.cuda_events)
    timing.cuda_events.clear()
    for attr, start, end in pending:
        try:
            end.synchronize()
            setattr(timing, attr, getattr(timing, attr) + float(start.elapsed_time(end)) / 1000.0)
        except Exception:
            continue


def _maybe_sync_cuda(device: torch.device, enabled: bool) -> None:
    if enabled and device.type == "cuda":
        torch.cuda.synchronize(device)


def _sync_cuda_for_timing(device: torch.device) -> float:
    if device.type != "cuda":
        return 0.0
    start_t = time.perf_counter()
    torch.cuda.synchronize(device)
    return time.perf_counter() - start_t


def _maybe_cudagraph_step_begin() -> None:
    compiler_mod = getattr(torch, "compiler", None)
    if compiler_mod is None:
        return
    marker = getattr(compiler_mod, "cudagraph_mark_step_begin", None)
    if marker is None:
        return
    try:
        marker()
    except Exception:
        return


def _detach_portfolio_state(tensor: torch.Tensor | None) -> torch.Tensor | None:
    if tensor is None:
        return None
    return tensor.detach().clone(memory_format=torch.contiguous_format)


def _is_cudagraph_overwrite_error(exc: RuntimeError) -> bool:
    msg = str(exc).lower()
    return "tensor output of cudagraphs that has been overwritten" in msg


class _CompiledLossFallback:
    def __init__(
        self,
        compiled_fn: Callable[..., torch.Tensor],
        eager_fn: Callable[..., torch.Tensor],
        *,
        label: str,
    ) -> None:
        self._compiled_fn = compiled_fn
        self._eager_fn = eager_fn
        self._label = label
        self._disabled = False
        self._warned = False

    def __call__(self, *args: Any, **kwargs: Any) -> torch.Tensor:
        if self._disabled:
            return self._eager_fn(*args, **kwargs)
        try:
            return self._compiled_fn(*args, **kwargs)
        except RuntimeError as exc:
            if not _is_cudagraph_overwrite_error(exc):
                raise
            self._disabled = True
            if not self._warned:
                print(
                    f"[{self._label}] torch.compile loss hit CUDA Graph state overwrite; "
                    "falling back to eager tensor loss for this fold"
                )
                self._warned = True
            aux_outputs = kwargs.get("aux_outputs")
            if isinstance(aux_outputs, dict):
                aux_outputs.pop("_final_weights", None)
            return self._eager_fn(*args, **kwargs)

    @property
    def disabled(self) -> bool:
        return self._disabled


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "on", "yes"}


def _progress(message: str) -> None:
    print(message, flush=True)


def _should_profile_train_step() -> bool:
    return _env_truthy("STOCKAGENT_TORCH_PROFILER", "0")


def _profile_trace_dir() -> Path:
    raw = os.environ.get("STOCKAGENT_TORCH_PROFILER_DIR", "artifacts/profiler")
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _extract_weights_and_aux(model_output: torch.Tensor | dict[str, torch.Tensor]) -> tuple[torch.Tensor, dict[str, torch.Tensor] | None]:
    if isinstance(model_output, dict):
        if "weights" not in model_output:
            raise ValueError("Model output dict must include 'weights'")
        weights = model_output["weights"]
        return weights, model_output
    return model_output, None


def _print_backward_top10(prof_obj: profile) -> None:
    try:
        events = list(prof_obj.key_averages())
    except Exception:
        return
    backward_events = [
        evt
        for evt in events
        if ("backward" in evt.key.lower()) or ("grad" in evt.key.lower())
    ]
    backward_events.sort(key=lambda evt: float(getattr(evt, "self_cuda_time_total", 0.0)), reverse=True)
    top = backward_events[:10]
    if not top:
        print("[torch.profiler] backward top10: no backward-tagged CUDA ops found")
        return
    print("[torch.profiler] backward top10 (self_cuda_time_total):")
    for idx, evt in enumerate(top, start=1):
        cuda_us = float(getattr(evt, "self_cuda_time_total", 0.0))
        print(f"  {idx:02d}. {evt.key}: {cuda_us/1000.0:.3f} ms")


def _profile_single_train_step(
    *,
    model: nn.Module,
    loss_fn,
    batch: dict[str, torch.Tensor],
    device: torch.device,
    amp_dtype: torch.dtype | None,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str,
    rank_ic_weight: float = 0.20,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    fold_id: int = 0,
) -> None:
    activities = [ProfilerActivity.CPU]
    if device.type == "cuda":
        activities.append(ProfilerActivity.CUDA)

    model.zero_grad(set_to_none=True)
    with profile(
        activities=activities,
        record_shapes=True,
        profile_memory=True,
        with_stack=False,
    ) as prof_obj:
        with _autocast_context(device, amp_dtype):
            model_output = model(batch["x"], batch["tradable_mask"])
            weights, aux_outputs = _extract_weights_and_aux(model_output)
            loss = loss_fn(
                weights,
                batch["future_log_returns"],
                batch["tradable_mask"],
                benchmark_returns=batch.get("benchmark"),
                can_buy_mask=batch["can_buy_mask"],
                can_sell_mask=batch["can_sell_mask"],
                sample_mask=batch.get("sample_mask"),
                long_only=long_only,
                buy_fee_rate=buy_fee_rate,
                sell_fee_rate=sell_fee_rate,
                max_turnover_ratio=max_turnover_ratio,
                gross_leverage=gross_leverage,
                gamma_sharpe=gamma_sharpe,
                gamma_excess=gamma_excess,
                gamma_cvar=gamma_cvar,
                cvar_alpha=cvar_alpha,
                gamma_drawdown=gamma_drawdown,
                drawdown_target=drawdown_target,
                gamma_turnover=gamma_turnover,
                gamma_underperformance=gamma_underperformance,
                excess_target=excess_target,
                cvar_budget=cvar_budget,
                drawdown_budget=drawdown_budget,
                turnover_budget=turnover_budget,
                gamma_cvar_budget=gamma_cvar_budget,
                gamma_drawdown_budget=gamma_drawdown_budget,
                gamma_turnover_budget=gamma_turnover_budget,
                objective=objective,
                aux_outputs=aux_outputs,
                rank_ic_weight=rank_ic_weight,
                direction_weight=direction_weight,
                volatility_regime_weight=volatility_regime_weight,
                concentration_weight=concentration_weight,
                regime_up_threshold=regime_up_threshold,
                regime_down_threshold=regime_down_threshold,
            )
        if torch.isfinite(loss).all():
            loss.backward()

    if device.type == "cuda":
        torch.cuda.synchronize(device)
    model.zero_grad(set_to_none=True)

    trace_path = _profile_trace_dir() / f"train_step_fold_{fold_id:02d}_{int(time.time())}.json"
    try:
        prof_obj.export_chrome_trace(str(trace_path))
        print(f"[torch.profiler] chrome trace exported: {trace_path}")
    except Exception as e:
        print(f"[torch.profiler] failed to export chrome trace: {e}")

    print("[torch.profiler] top10 CUDA ops (self_cuda_time_total):")
    try:
        print(prof_obj.key_averages().table(sort_by="self_cuda_time_total", row_limit=10))
    except Exception as e:
        print(f"[torch.profiler] failed to print CUDA top10: {e}")
    _print_backward_top10(prof_obj)


def _loss_from_backtest_series(
    strategy_returns: torch.Tensor,
    benchmark_returns: torch.Tensor,
    turnovers: torch.Tensor,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str = "sharpe",
    sample_mask: torch.Tensor | None = None,
) -> torch.Tensor:
    if sample_mask is None:
        valid_returns = strategy_returns
        valid_benchmark = benchmark_returns
        valid_turnovers = turnovers
    else:
        valid_mask = sample_mask.to(device=strategy_returns.device, dtype=torch.bool)
        valid_returns = strategy_returns[valid_mask]
        valid_benchmark = benchmark_returns[valid_mask]
        valid_turnovers = turnovers[valid_mask]

    if valid_returns.numel() == 0:
        return strategy_returns.sum() * 0.0

    objective_norm = objective.strip().lower()
    if objective_norm in {"log_utility", "log_util", "kelly", "growth", "mean_log_return"}:
        objective_value = -float(gamma_sharpe) * valid_returns.mean() * valid_returns.new_tensor(252.0)
    elif objective_norm in {
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
    }:
        excess_returns = valid_returns - valid_benchmark
        mean_excess = excess_returns.mean()

        alpha = min(max(float(cvar_alpha), 1e-6), 1.0 - 1e-6)
        losses = -excess_returns
        var_alpha = torch.quantile(losses, alpha)
        tail_excess = torch.relu(losses - var_alpha)
        cvar = var_alpha + tail_excess.mean() / (1.0 - alpha)

        rel_log_equity = torch.cumsum(excess_returns, dim=0)
        rel_equity = torch.exp(torch.clamp(rel_log_equity, -60.0, 60.0))
        running_max = torch.cummax(rel_equity, dim=0).values
        drawdowns = 1.0 - rel_equity / running_max.clamp_min(1e-12)
        mdd = drawdowns.max() if drawdowns.numel() > 0 else mean_excess.new_zeros(())
        drawdown_penalty = torch.nn.functional.softplus((mdd - float(drawdown_target)) * 20.0) / 20.0

        if objective_norm in {"outperformance_risk_budget", "outperformance_budget", "outperformance_first"}:
            underperformance = torch.relu(float(excess_target) - excess_returns).mean()
            turnover_mean = valid_turnovers.mean() if valid_turnovers.numel() > 0 else mean_excess.new_zeros(())
            cvar_budget_penalty = torch.relu(cvar - float(cvar_budget))
            drawdown_budget_penalty = torch.relu(mdd - float(drawdown_budget))
            turnover_budget_penalty = torch.relu(turnover_mean - float(turnover_budget))

            objective_value = (
                -gamma_excess * mean_excess
                + gamma_underperformance * underperformance
                + gamma_cvar_budget * cvar_budget_penalty
                + gamma_drawdown_budget * drawdown_budget_penalty
                + gamma_turnover_budget * turnover_budget_penalty
                + gamma_cvar * cvar
                + gamma_drawdown * drawdown_penalty
            )
        else:
            objective_value = (
                -gamma_excess * mean_excess
                + gamma_cvar * cvar
                + gamma_drawdown * drawdown_penalty
            )
    else:
        mean_return = valid_returns.mean()
        annualizer = torch.sqrt(torch.as_tensor(252.0, device=valid_returns.device, dtype=valid_returns.dtype))
        if objective_norm == "sortino":
            downside = torch.minimum(valid_returns, torch.zeros_like(valid_returns))
            downside_dev = torch.sqrt(downside.pow(2).mean() + 1e-8)
            risk_ratio = mean_return / downside_dev * annualizer
        else:
            variance = (valid_returns - mean_return).pow(2).mean()
            std_return = torch.sqrt(variance + 1e-8)
            risk_ratio = mean_return / std_return * annualizer
        objective_value = -gamma_sharpe * risk_ratio

    if gamma_turnover == 0.0:
        return objective_value

    turnover_penalty = valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(())
    return objective_value + gamma_turnover * turnover_penalty


def _is_return_series_objective(objective: str) -> bool:
    return objective.strip().lower() in {
        "sharpe",
        "sortino",
        "log_utility",
        "log_util",
        "kelly",
        "growth",
        "mean_log_return",
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
    }


def _loss_from_backtest_result(
    backtest: BacktestResultTensor,
    config: ExperimentConfig,
    objective: str,
) -> torch.Tensor:
    return _loss_from_backtest_series(
        backtest.strategy_returns,
        backtest.benchmark_returns,
        backtest.turnovers,
        gamma_sharpe=config.evaluation.gamma_sharpe,
        gamma_excess=config.evaluation.gamma_excess,
        gamma_cvar=config.evaluation.gamma_cvar,
        cvar_alpha=config.evaluation.cvar_alpha,
        gamma_drawdown=config.evaluation.gamma_drawdown,
        drawdown_target=config.evaluation.drawdown_target,
        gamma_turnover=config.evaluation.gamma_turnover,
        gamma_underperformance=config.evaluation.gamma_underperformance,
        excess_target=config.evaluation.excess_target,
        cvar_budget=config.evaluation.cvar_budget,
        drawdown_budget=config.evaluation.drawdown_budget,
        turnover_budget=config.evaluation.turnover_budget,
        gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
        gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
        gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
        objective=objective,
    )


def _evaluated_backtest_loss(
    backtest: BacktestResultTensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    config: ExperimentConfig,
    objective: str,
) -> torch.Tensor:
    if _is_return_series_objective(objective):
        return _loss_from_backtest_result(backtest, config, objective)
    return risk_aware_loss(
        backtest.weights_history,
        future_log_returns,
        tradable_mask,
        benchmark_returns=benchmark_returns,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        long_only=config.trading.long_only,
        buy_fee_rate=config.trading.buy_fee_rate,
        sell_fee_rate=config.trading.sell_fee_rate,
        max_turnover_ratio=config.trading.max_turnover_ratio,
        gross_leverage=config.trading.gross_leverage,
        gamma_sharpe=config.evaluation.gamma_sharpe,
        gamma_excess=config.evaluation.gamma_excess,
        gamma_cvar=config.evaluation.gamma_cvar,
        cvar_alpha=config.evaluation.cvar_alpha,
        gamma_drawdown=config.evaluation.gamma_drawdown,
        drawdown_target=config.evaluation.drawdown_target,
        gamma_turnover=config.evaluation.gamma_turnover,
        gamma_underperformance=config.evaluation.gamma_underperformance,
        excess_target=config.evaluation.excess_target,
        cvar_budget=config.evaluation.cvar_budget,
        drawdown_budget=config.evaluation.drawdown_budget,
        turnover_budget=config.evaluation.turnover_budget,
        gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
        gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
        gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
        objective=objective,
    )


def _batched_loss_from_backtest_segments(
    strategy_returns: torch.Tensor,
    benchmark_returns: torch.Tensor,
    turnovers: torch.Tensor,
    offsets: Sequence[int],
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str = "sharpe",
) -> torch.Tensor:
    """Compute one validation/test loss per fold without per-fold CPU round trips."""
    segment_count = max(0, len(offsets) - 1)
    if segment_count <= 0:
        return strategy_returns.new_empty((0,), dtype=torch.float32)

    objective_norm = objective.strip().lower()
    if objective_norm not in {"sharpe", "sortino", "log_utility"}:
        losses = [
            _loss_from_backtest_series(
                strategy_returns[int(offsets[idx]) : int(offsets[idx + 1])],
                benchmark_returns[int(offsets[idx]) : int(offsets[idx + 1])],
                turnovers[int(offsets[idx]) : int(offsets[idx + 1])],
                gamma_sharpe=gamma_sharpe,
                gamma_excess=gamma_excess,
                gamma_cvar=gamma_cvar,
                cvar_alpha=cvar_alpha,
                gamma_drawdown=gamma_drawdown,
                drawdown_target=drawdown_target,
                gamma_turnover=gamma_turnover,
                gamma_underperformance=gamma_underperformance,
                excess_target=excess_target,
                cvar_budget=cvar_budget,
                drawdown_budget=drawdown_budget,
                turnover_budget=turnover_budget,
                gamma_cvar_budget=gamma_cvar_budget,
                gamma_drawdown_budget=gamma_drawdown_budget,
                gamma_turnover_budget=gamma_turnover_budget,
                objective=objective,
            )
            for idx in range(segment_count)
        ]
        return torch.stack(losses) if losses else strategy_returns.new_empty((0,), dtype=torch.float32)

    lengths_py = [
        max(0, int(offsets[idx + 1]) - int(offsets[idx]))
        for idx in range(segment_count)
    ]
    max_len = max(lengths_py, default=0)
    if max_len <= 0:
        return strategy_returns.new_zeros((segment_count,), dtype=torch.float32)

    device = strategy_returns.device
    calc_dtype = torch.float32 if strategy_returns.dtype in {torch.float16, torch.bfloat16} else strategy_returns.dtype
    starts = torch.as_tensor([int(value) for value in offsets[:-1]], device=device, dtype=torch.long)
    lengths = torch.as_tensor(lengths_py, device=device, dtype=torch.long)
    rows = torch.arange(max_len, device=device, dtype=torch.long)
    valid = rows.unsqueeze(0) < lengths.unsqueeze(1)
    gather_idx = (starts.unsqueeze(1) + rows.unsqueeze(0)).clamp_max(max(0, int(strategy_returns.numel()) - 1))

    valid_f = valid.to(dtype=calc_dtype)
    count = lengths.to(dtype=calc_dtype).clamp_min(1.0)
    r = torch.nan_to_num(strategy_returns[gather_idx].to(dtype=calc_dtype), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
    mean_return = r.sum(dim=1) / count
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=device, dtype=calc_dtype))

    if objective_norm == "log_utility":
        objective_value = -float(gamma_sharpe) * mean_return * torch.as_tensor(252.0, device=device, dtype=calc_dtype)
    elif objective_norm == "sortino":
        downside = torch.minimum(r, torch.zeros_like(r))
        risk_dev = torch.sqrt(downside.pow(2).sum(dim=1) / count + 1e-8)
        objective_value = -float(gamma_sharpe) * (mean_return / risk_dev * annualizer)
    else:
        centered = (r - mean_return.unsqueeze(1)) * valid_f
        risk_dev = torch.sqrt(centered.pow(2).sum(dim=1) / count + 1e-8)
        objective_value = -float(gamma_sharpe) * (mean_return / risk_dev * annualizer)

    if float(gamma_turnover) != 0.0:
        t = torch.nan_to_num(turnovers[gather_idx].to(dtype=calc_dtype), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
        objective_value = objective_value + float(gamma_turnover) * (t.sum(dim=1) / count)
    return torch.where(lengths > 0, objective_value, objective_value.new_zeros(()))


def _normalize_risk_objective(loss_type: str) -> str:
    objective = str(loss_type).strip().lower()
    if objective in {
        "sharpe",
        "sortino",
        "log_utility",
        "log_util",
        "kelly",
        "growth",
        "mean_log_return",
        "rank",
        "rank_ic",
        "ic",
        "multitask_rank_ic",
        "pure_rank",
        "rank_only",
        "score_rank",
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
        "factor_generalization",
        "factor",
        "factor_ic",
        "characteristic_factor",
        "portfolio_autoencoder",
        "bottleneck_portfolio_autoencoder",
        "autoencoder_portfolio",
    }:
        if objective in {"bottleneck_portfolio_autoencoder", "autoencoder_portfolio"}:
            return "portfolio_autoencoder"
        if objective in {"factor", "factor_ic", "characteristic_factor"}:
            return "factor_generalization"
        if objective in {"rank", "ic", "multitask_rank_ic"}:
            return "rank_ic"
        if objective in {"rank_only", "score_rank"}:
            return "pure_rank"
        if objective in {"log_util", "kelly", "growth", "mean_log_return"}:
            return "log_utility"
        if objective in {"cvar", "cvar_drawdown", "excess_cvar"}:
            return "excess_cvar_drawdown"
        if objective in {"outperformance_budget", "outperformance_first"}:
            return "outperformance_risk_budget"
        return objective
    return "sharpe"


def _evaluate_rank_ic_multitask_loss(
    model: nn.Module,
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    chunk_rows: int,
    *,
    objective: str = "rank_ic",
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    rank_ic_weight: float,
    direction_weight: float,
    volatility_regime_weight: float,
    concentration_weight: float,
    regime_up_threshold: float,
    regime_down_threshold: float,
    factor_loss_kwargs: dict[str, Any] | None = None,
) -> tuple[float, float | None, TimingBreakdown]:
    model.eval()
    total_rows = 0
    total_loss_t: torch.Tensor | None = None
    ic_sum_t: torch.Tensor | None = None
    ic_count_t: torch.Tensor | None = None
    timing = TimingBreakdown()
    overall_start = time.perf_counter()

    with torch.inference_mode():
        for start in range(0, x.size(0), chunk_rows):
            end = min(start + chunk_rows, x.size(0))

            transfer_start = time.perf_counter()
            x_chunk = x[start:end].to(device=device, non_blocking=non_blocking)
            returns_chunk = future_log_returns[start:end].to(device=device, non_blocking=non_blocking)
            mask_chunk = tradable_mask[start:end].to(device=device, non_blocking=non_blocking)
            buy_chunk = can_buy_mask[start:end].to(device=device, non_blocking=non_blocking)
            sell_chunk = can_sell_mask[start:end].to(device=device, non_blocking=non_blocking)
            bench_chunk = benchmark_returns[start:end].to(device=device, non_blocking=non_blocking)
            timing.transfer_s += time.perf_counter() - transfer_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_forward_start = time.perf_counter()
                model_output = model(x_chunk, mask_chunk)
                weights_chunk, aux_outputs = _extract_weights_and_aux(model_output)
                timing.model_forward_s += time.perf_counter() - model_forward_start
                loss_start = time.perf_counter()
                loss_t = risk_aware_loss(
                    weights_chunk,
                    returns_chunk,
                    mask_chunk,
                    benchmark_returns=bench_chunk,
                    can_buy_mask=buy_chunk,
                    can_sell_mask=sell_chunk,
                    long_only=long_only,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    max_turnover_ratio=max_turnover_ratio,
                    gross_leverage=gross_leverage,
                    gamma_sharpe=gamma_sharpe,
                    gamma_excess=gamma_excess,
                    gamma_cvar=gamma_cvar,
                    cvar_alpha=cvar_alpha,
                    gamma_drawdown=gamma_drawdown,
                    drawdown_target=drawdown_target,
                    gamma_turnover=gamma_turnover,
                    gamma_underperformance=gamma_underperformance,
                    excess_target=excess_target,
                    cvar_budget=cvar_budget,
                    drawdown_budget=drawdown_budget,
                    turnover_budget=turnover_budget,
                    gamma_cvar_budget=gamma_cvar_budget,
                    gamma_drawdown_budget=gamma_drawdown_budget,
                    gamma_turnover_budget=gamma_turnover_budget,
                    objective=objective,
                    aux_outputs=aux_outputs,
                    rank_ic_weight=rank_ic_weight,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                    concentration_weight=concentration_weight,
                    regime_up_threshold=regime_up_threshold,
                    regime_down_threshold=regime_down_threshold,
                    **(factor_loss_kwargs or {}),
                )
                timing.loss_s += time.perf_counter() - loss_start
            timing.forward_s += time.perf_counter() - forward_start

            rows = end - start
            total_rows += rows
            metrics_start = time.perf_counter()
            weighted_loss = loss_t.detach().float() * float(rows)
            total_loss_t = weighted_loss if total_loss_t is None else total_loss_t + weighted_loss
            timing.metrics_s += time.perf_counter() - metrics_start

            ic_start = time.perf_counter()
            rank_scores = aux_outputs.get("rank_logits") if aux_outputs else weights_chunk
            ic_series = compute_ic_series_torch(rank_scores, returns_chunk, mask_chunk)
            ic_clean = ic_series[torch.isfinite(ic_series)]
            if ic_clean.numel() > 0:
                ic_sum_chunk = ic_clean.float().sum()
                ic_count_chunk = ic_clean.new_tensor(float(ic_clean.numel()), dtype=torch.float32)
                ic_sum_t = ic_sum_chunk if ic_sum_t is None else ic_sum_t + ic_sum_chunk
                ic_count_t = ic_count_chunk if ic_count_t is None else ic_count_t + ic_count_chunk
            timing.ic_s += time.perf_counter() - ic_start

    reduce_start = time.perf_counter()
    if total_loss_t is None:
        mean_loss = 0.0
    else:
        mean_loss = float((total_loss_t / float(max(1, total_rows))).detach().cpu())
    if ic_sum_t is None or ic_count_t is None:
        mean_ic = None
    else:
        mean_ic = float((ic_sum_t / ic_count_t.clamp_min(1.0)).detach().cpu())
    timing.metrics_s += time.perf_counter() - reduce_start
    timing.total_s = time.perf_counter() - overall_start
    timing.batches = int(max(1, (int(x.size(0)) + int(chunk_rows) - 1) // int(chunk_rows)))
    return mean_loss, mean_ic, timing


def _objective_metric_key(objective: str) -> str:
    if objective in {"rank_ic", "pure_rank"}:
        return "rank_ic"
    if objective == "factor_generalization":
        return "factor_generalization"
    if objective == "portfolio_autoencoder":
        return "sharpe"
    if objective == "log_utility":
        return "cagr"
    if objective in {"outperformance_risk_budget", "excess_cvar_drawdown"}:
        return "excess_return_vs_universe_average"
    return objective


def _factor_loss_kwargs(config: ExperimentConfig) -> dict[str, Any]:
    cfg = config.training.factor_generalization_loss
    return {
        "factor_slope_tstat_weight": cfg.slope_tstat_weight,
        "factor_rank_ic_weight": cfg.rank_ic_weight,
        "factor_sharpe_weight": cfg.factor_sharpe_weight,
        "factor_block_stability_weight": cfg.block_stability_weight,
        "factor_regime_stability_weight": cfg.regime_stability_weight,
        "factor_consistency_weight": cfg.consistency_weight,
        "factor_net_exposure_weight": cfg.net_exposure_weight,
        "factor_gross_exposure_weight": cfg.gross_exposure_weight,
        "factor_concentration_weight": cfg.concentration_weight,
        "factor_turnover_weight": cfg.turnover_weight,
        "factor_score_l2_weight": cfg.score_l2_weight,
        "factor_temperature": cfg.factor_temperature,
        "factor_block_count": cfg.block_count,
        "factor_worst_fraction": cfg.worst_fraction,
    }


def _portfolio_autoencoder_loss_kwargs(config: ExperimentConfig) -> dict[str, Any]:
    cfg = config.training.portfolio_autoencoder_loss
    return {
        "autoencoder_cost_rate": cfg.cost_rate,
        "autoencoder_lambda_turnover": cfg.lambda_turnover,
        "autoencoder_lambda_concentration": cfg.lambda_concentration,
        "autoencoder_lambda_latent": cfg.lambda_latent,
    }


def _factor_augmentation_kwargs(config: ExperimentConfig, objective: str) -> dict[str, float]:
    if objective != "factor_generalization":
        return {}
    cfg = config.training.factor_generalization_loss
    if float(cfg.consistency_weight) <= 0.0:
        return {}
    return {
        "feature_dropout": float(cfg.augmentation_feature_dropout),
        "stock_dropout": float(cfg.augmentation_stock_dropout),
        "time_dropout": float(cfg.augmentation_time_dropout),
        "noise_std": float(cfg.augmentation_noise_std),
    }


def _augment_factor_inputs(x: torch.Tensor, aug_kwargs: dict[str, float]) -> torch.Tensor:
    if not aug_kwargs:
        return x
    x_aug = x
    feature_dropout = min(max(float(aug_kwargs.get("feature_dropout", 0.0)), 0.0), 0.95)
    stock_dropout = min(max(float(aug_kwargs.get("stock_dropout", 0.0)), 0.0), 0.95)
    time_dropout = min(max(float(aug_kwargs.get("time_dropout", 0.0)), 0.0), 0.95)
    noise_std = max(0.0, float(aug_kwargs.get("noise_std", 0.0)))

    if noise_std > 0.0:
        x_aug = x_aug + torch.randn_like(x_aug) * noise_std
    if feature_dropout > 0.0:
        keep = (torch.rand(x.size(0), 1, 1, x.size(3), device=x.device) >= feature_dropout).to(dtype=x.dtype)
        x_aug = x_aug * keep / max(1.0 - feature_dropout, 1e-6)
    if stock_dropout > 0.0:
        keep = (torch.rand(x.size(0), 1, x.size(2), 1, device=x.device) >= stock_dropout).to(dtype=x.dtype)
        x_aug = x_aug * keep
    if time_dropout > 0.0 and x.size(1) > 1:
        keep = (torch.rand(x.size(0), x.size(1), 1, 1, device=x.device) >= time_dropout).to(dtype=x.dtype)
        keep[:, -1:, :, :] = 1.0
        x_aug = x_aug * keep
    return x_aug


def _attach_factor_augmented_scores(
    *,
    model: nn.Module,
    aux_outputs: dict[str, torch.Tensor] | None,
    x: torch.Tensor,
    tradable_mask: torch.Tensor,
    aug_kwargs: dict[str, float],
) -> dict[str, torch.Tensor] | None:
    if not aug_kwargs:
        return aux_outputs
    x_aug = _augment_factor_inputs(x, aug_kwargs)
    aug_output = model(x_aug, tradable_mask)
    aug_weights, aug_aux = _extract_weights_and_aux(aug_output)
    aug_scores = aug_weights
    if aug_aux is not None:
        aug_scores = aug_aux.get("rank_logits", aug_aux.get("score_logits", aug_weights))
    merged: dict[str, torch.Tensor] = dict(aux_outputs or {})
    merged["aug_score_logits"] = aug_scores
    return merged


def _normalized_model_name(model_name: str) -> str:
    return model_name.strip().lower().replace("-", "_")


def _is_tree_model_name(model_name: str) -> bool:
    normalized = _normalized_model_name(model_name)
    return normalized in {"lightgbm", "lgbm", "xgboost", "xgb"}


def _fold_dir(output_path: Path, fold_id: int) -> Path:
    return output_path / f"fold_{fold_id:02d}"


def _checkpoint_path(fold_dir: Path) -> Path:
    return fold_dir / "checkpoint_last.pt"


def _best_checkpoint_path(fold_dir: Path) -> Path:
    return fold_dir / "checkpoint_best.pt"


def _metrics_path(fold_dir: Path) -> Path:
    return fold_dir / "metrics.json"


def _model_path(fold_dir: Path) -> Path:
    return fold_dir / "model.pt"


def _backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "test_backtest.npz"


def _integer_backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "test_integer_share_backtest.npz"


def _group_key(train_years: list[int]) -> tuple[int, ...]:
    return tuple(train_years)


def _group_id(train_years: list[int]) -> str:
    return "train_" + "-".join(str(year) for year in train_years)


def _group_dir(output_path: Path, train_years: list[int]) -> Path:
    return output_path / _group_id(train_years)


def _group_checkpoint_path(output_path: Path, train_years: list[int]) -> Path:
    return _group_dir(output_path, train_years) / "checkpoint_last.pt"


def _group_curve_path(output_path: Path, train_years: list[int]) -> Path:
    return _group_dir(output_path, train_years) / "epoch_curve.jsonl"


def _trim_group_curve(path: Path, start_epoch: int) -> None:
    if not path.exists() or start_epoch <= 1:
        return
    kept: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            epoch = int(payload.get("epoch", 0))
            if epoch < start_epoch:
                kept.append(line)
    with path.open("w", encoding="utf-8") as handle:
        for line in kept:
            handle.write(line + "\n")


def _append_group_curve(path: Path, payload: dict[str, float | int | None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _epoch_curve_plot_script() -> Path:
    return Path(__file__).resolve().parents[2] / "plot_epoch_curves.py"


def _epoch_curve_plot_paths(curve_path: Path, interval: int) -> tuple[Path, Path]:
    sample_interval = max(1, int(interval))
    return (
        curve_path.parent / f"epoch_curve_every{sample_interval}.png",
        curve_path.parent / f"epoch_timing_every{sample_interval}.png",
    )


def _epoch_curve_plot_command(curve_path: Path, interval: int) -> list[str]:
    output_path, timing_output_path = _epoch_curve_plot_paths(curve_path, interval)
    return [
        sys.executable,
        str(_epoch_curve_plot_script()),
        "--curve-file",
        str(curve_path),
        "--interval",
        str(max(1, int(interval))),
        "--output",
        str(output_path),
        "--timing-output",
        str(timing_output_path),
    ]


def _run_epoch_curve_plot_once(curve_path: Path, interval: int) -> None:
    script_path = _epoch_curve_plot_script()
    if not script_path.exists():
        return
    proc = subprocess.run(
        _epoch_curve_plot_command(curve_path, interval),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip()[-1000:]
        print(f"[curve plot] failed for {curve_path}: {stderr_tail}")


class _AsyncEpochCurvePlotter:
    """Update epoch loss/timing plots without blocking the training loop."""

    def __init__(self, curve_path: Path, interval: int, async_enabled: bool) -> None:
        self.curve_path = curve_path
        self.interval = max(1, int(interval))
        self.async_enabled = bool(async_enabled)
        self._proc: subprocess.Popen[str] | None = None
        self._pending = False

    def request(self) -> None:
        if not self.curve_path.exists():
            return
        if not self.async_enabled:
            _run_epoch_curve_plot_once(self.curve_path, self.interval)
            return
        self._reap_finished()
        if self._proc is None:
            self._launch()
        else:
            self._pending = True

    def flush(self) -> None:
        if not self.async_enabled:
            return
        self._reap_finished()
        if self._proc is not None:
            _, stderr = self._proc.communicate()
            if self._proc.returncode not in (0, None):
                print(f"[curve plot] failed for {self.curve_path}: {(stderr or '').strip()[-1000:]}")
            self._proc = None
        if self._pending:
            self._pending = False
            _run_epoch_curve_plot_once(self.curve_path, self.interval)

    def _launch(self) -> None:
        script_path = _epoch_curve_plot_script()
        if not script_path.exists():
            return
        self._proc = subprocess.Popen(
            _epoch_curve_plot_command(self.curve_path, self.interval),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _reap_finished(self) -> None:
        if self._proc is None:
            return
        return_code = self._proc.poll()
        if return_code is None:
            return
        _, stderr = self._proc.communicate()
        if return_code != 0:
            print(f"[curve plot] failed for {self.curve_path}: {(stderr or '').strip()[-1000:]}")
        self._proc = None
        if self._pending:
            self._pending = False
            self._launch()


def _timing_curve_payload(
    *,
    train_timing: TimingBreakdown,
    val_timing: TimingBreakdown | None = None,
    test_curve_timing: TimingBreakdown | None = None,
    val_eval_s: float = 0.0,
    val_loss_s: float = 0.0,
    val_metrics_s: float | None = None,
    test_curve_s: float = 0.0,
    test_curve_loss_s: float = 0.0,
    fold_checkpoint_save_s: float = 0.0,
    group_checkpoint_save_s: float = 0.0,
    checkpoint_save_s: float = 0.0,
    scheduler_s: float = 0.0,
    progress_update_s: float = 0.0,
    curve_record_s: float = 0.0,
    scalar_sync_s: float = 0.0,
    cuda_sync_s: float = 0.0,
    gc_s: float = 0.0,
    epoch_wall_s: float | None = None,
    timing_synchronized: bool = False,
    backtest_compile_stats: dict[str, int] | None = None,
    backtest_prep_compile_stats: dict[str, int] | None = None,
    backtest_runtime_stats: dict[str, float] | None = None,
    train_backtest_runtime_stats: dict[str, float] | None = None,
    loss_runtime_stats: dict[str, float] | None = None,
) -> dict[str, float | int]:
    _flush_cuda_timing_events(train_timing)
    if val_timing is not None:
        _flush_cuda_timing_events(val_timing)
    if test_curve_timing is not None:
        _flush_cuda_timing_events(test_curve_timing)
    val_timing = val_timing or TimingBreakdown()
    test_curve_timing = test_curve_timing or TimingBreakdown()
    backtest_compile_stats = backtest_compile_stats or {}
    backtest_prep_compile_stats = backtest_prep_compile_stats or {}
    backtest_runtime_stats = backtest_runtime_stats or {}
    train_backtest_runtime_stats = train_backtest_runtime_stats or backtest_runtime_stats
    loss_runtime_stats = loss_runtime_stats or {}
    batches = max(1, int(train_timing.batches))
    bt_calls = max(1.0, float(backtest_runtime_stats.get("calls", 0.0)))

    def _avg_ms(value_s: float) -> float:
        return float(value_s) * 1000.0 / float(batches)

    def _bt_avg_ms(key: str) -> float:
        return float(backtest_runtime_stats.get(key, 0.0)) * 1000.0 / bt_calls

    def _train_bt_avg_batch_ms(key: str) -> float:
        return float(train_backtest_runtime_stats.get(key, 0.0)) * 1000.0 / float(batches)

    def _train_bt_avg_call_ms(key: str) -> float:
        calls = max(1.0, float(train_backtest_runtime_stats.get("calls", 0.0)))
        return float(train_backtest_runtime_stats.get(key, 0.0)) * 1000.0 / calls

    def _loss_avg_ms(key: str) -> float:
        return float(loss_runtime_stats.get(key, 0.0)) * 1000.0 / float(batches)

    def _timing_ms_per_call(total_s: float, calls: int | float) -> float:
        denom = max(1.0, float(calls))
        return float(total_s) * 1000.0 / denom

    def _epoch_percent(value_s: float) -> float:
        return float(value_s) * 100.0 / max(1e-12, float(epoch_total_s))

    val_metrics_value = float(val_loss_s if val_metrics_s is None else val_metrics_s)
    checkpoint_total_s = (
        float(checkpoint_save_s)
        + float(fold_checkpoint_save_s)
        + float(group_checkpoint_save_s)
    )
    measured_total_s = (
        float(train_timing.total_s)
        + float(val_eval_s)
        + val_metrics_value
        + float(test_curve_s)
        + checkpoint_total_s
        + float(scheduler_s)
        + float(progress_update_s)
        + float(curve_record_s)
        + float(scalar_sync_s)
        + float(cuda_sync_s)
        + float(gc_s)
    )
    epoch_total_s = float(epoch_wall_s) if epoch_wall_s is not None else measured_total_s
    unattributed_s = max(0.0, epoch_total_s - measured_total_s)
    return {
        "train_batches": int(train_timing.batches),
        "timing_synchronized": int(bool(timing_synchronized)),
        "train_total_s": float(train_timing.total_s),
        "train_total_ms_per_batch": _avg_ms(train_timing.total_s),
        "train_fetch_ms_per_batch": _avg_ms(train_timing.fetch_s),
        "train_transfer_ms_per_batch": _avg_ms(train_timing.transfer_s),
        "train_forward_ms_per_batch": _avg_ms(train_timing.forward_s),
        "train_model_forward_ms_per_batch": _avg_ms(train_timing.model_forward_s),
        "train_model_forward_cuda_ms_per_batch": _avg_ms(train_timing.model_forward_cuda_s),
        "train_factor_aug_ms_per_batch": _avg_ms(train_timing.factor_aug_s),
        "train_factor_aug_cuda_ms_per_batch": _avg_ms(train_timing.factor_aug_cuda_s),
        "train_loss_ms_per_batch": _avg_ms(train_timing.loss_s),
        "train_loss_cuda_ms_per_batch": _avg_ms(train_timing.loss_cuda_s),
        "train_loss_bt_calls": int(train_backtest_runtime_stats.get("calls", 0.0)),
        "train_loss_bt_calls_per_batch": float(train_backtest_runtime_stats.get("calls", 0.0)) / float(batches),
        "train_loss_bt_runtime_ms_per_batch": _train_bt_avg_batch_ms("total_s"),
        "train_loss_bt_runtime_cuda_ms_per_batch": _train_bt_avg_batch_ms("total_cuda_s"),
        "train_loss_bt_prep_ms_per_batch": _train_bt_avg_batch_ms("prep_s"),
        "train_loss_bt_prep_cuda_ms_per_batch": _train_bt_avg_batch_ms("prep_cuda_s"),
        "train_loss_bt_prev_init_ms_per_batch": _train_bt_avg_batch_ms("prev_init_s"),
        "train_loss_bt_prev_init_cuda_ms_per_batch": _train_bt_avg_batch_ms("prev_init_cuda_s"),
        "train_loss_bt_runner_resolve_ms_per_batch": _train_bt_avg_batch_ms("runner_resolve_s"),
        "train_loss_bt_runner_call_ms_per_batch": _train_bt_avg_batch_ms("runner_call_s"),
        "train_loss_bt_runner_call_cuda_ms_per_batch": _train_bt_avg_batch_ms("runner_call_cuda_s"),
        "train_loss_bt_runtime_fallback_ms_per_batch": _train_bt_avg_batch_ms("runtime_fallback_s"),
        "train_loss_bt_checkpoint_ms_per_batch": _train_bt_avg_batch_ms("checkpoint_s"),
        "train_loss_bt_checkpoint_cuda_ms_per_batch": _train_bt_avg_batch_ms("checkpoint_cuda_s"),
        "train_loss_bt_finalize_ms_per_batch": _train_bt_avg_batch_ms("finalize_s"),
        "train_loss_bt_finalize_cuda_ms_per_batch": _train_bt_avg_batch_ms("finalize_cuda_s"),
        "train_loss_bt_cpp_ext_ms_per_batch": _train_bt_avg_batch_ms("cpp_ext_s"),
        "train_loss_bt_cpp_ext_cuda_ms_per_batch": _train_bt_avg_batch_ms("cpp_ext_cuda_s"),
        "train_loss_bt_runtime_cuda_ms_per_call": _train_bt_avg_call_ms("total_cuda_s"),
        "train_loss_bt_prep_cuda_ms_per_call": _train_bt_avg_call_ms("prep_cuda_s"),
        "train_loss_bt_runner_call_cuda_ms_per_call": _train_bt_avg_call_ms("runner_call_cuda_s"),
        "train_loss_bt_compiled_runner_calls": int(train_backtest_runtime_stats.get("compiled_runner_calls", 0.0)),
        "train_loss_bt_eager_runner_calls": int(train_backtest_runtime_stats.get("eager_runner_calls", 0.0)),
        "train_loss_bt_compiled_prep_calls": int(train_backtest_runtime_stats.get("compiled_prep_calls", 0.0)),
        "train_loss_bt_eager_prep_calls": int(train_backtest_runtime_stats.get("eager_prep_calls", 0.0)),
        "train_loss_bt_stateful_calls": int(train_backtest_runtime_stats.get("stateful_calls", 0.0)),
        "train_loss_bt_stateful_compiled_runner_calls": int(train_backtest_runtime_stats.get("stateful_compiled_runner_calls", 0.0)),
        "train_loss_bt_stateful_eager_runner_calls": int(train_backtest_runtime_stats.get("stateful_eager_runner_calls", 0.0)),
        "train_loss_bt_runtime_fallback_calls": int(train_backtest_runtime_stats.get("runtime_fallback_calls", 0.0)),
        "train_portfolio_state_ms_per_batch": _avg_ms(train_timing.portfolio_state_s),
        "train_backward_total_ms_per_batch": _avg_ms(train_timing.backward_s),
        "train_backward_autograd_ms_per_batch": _avg_ms(train_timing.grad_s),
        "train_backward_autograd_cuda_ms_per_batch": _avg_ms(train_timing.grad_cuda_s),
        "train_grad_ms_per_batch": _avg_ms(train_timing.grad_s),
        "train_grad_cuda_ms_per_batch": _avg_ms(train_timing.grad_cuda_s),
        "train_clip_ms_per_batch": _avg_ms(train_timing.clip_s),
        "train_finite_check_ms_per_batch": _avg_ms(train_timing.finite_check_s),
        "train_step_ms_per_batch": _avg_ms(train_timing.step_s),
        "train_step_cuda_ms_per_batch": _avg_ms(train_timing.step_cuda_s),
        "val_eval_s": float(val_eval_s),
        "val_transfer_s": float(val_timing.transfer_s),
        "val_eval_transfer_to_gpu_s": float(val_timing.transfer_s),
        "val_forward_s": float(val_timing.forward_s),
        "val_eval_model_forward_s": float(val_timing.model_forward_s),
        "val_model_forward_s": float(val_timing.model_forward_s),
        "val_loss_compute_s": float(val_timing.loss_s),
        "val_backtest_s": float(val_timing.backtest_s),
        "val_eval_backtest_prepare_s": float(val_timing.backtest_prepare_s),
        "val_eval_backtest_runner_s": float(val_timing.backtest_runner_s),
        "val_eval_backtest_finalize_s": float(val_timing.backtest_finalize_s),
        "val_ic_s": float(val_timing.ic_s),
        "val_metrics_reduce_s": float(val_timing.metrics_s),
        "val_eval_metrics_s": float(val_timing.metrics_s),
        "val_concat_s": float(val_timing.concat_s),
        "val_eval_concat_s": float(val_timing.concat_s),
        "val_eval_cpu_gpu_sync_s": float(val_timing.cpu_gpu_sync_s),
        "val_loss_s": float(val_loss_s),
        "val_metrics_s": val_metrics_value,
        "test_curve_s": float(test_curve_s),
        "test_curve_loss_s": float(test_curve_loss_s),
        "test_curve_transfer_s": float(test_curve_timing.transfer_s),
        "test_curve_eval_transfer_to_gpu_s": float(test_curve_timing.transfer_s),
        "test_curve_forward_s": float(test_curve_timing.forward_s),
        "test_curve_model_forward_s": float(test_curve_timing.model_forward_s),
        "test_curve_eval_model_forward_s": float(test_curve_timing.model_forward_s),
        "test_curve_loss_compute_s": float(test_curve_timing.loss_s),
        "test_curve_backtest_s": float(test_curve_timing.backtest_s),
        "test_curve_eval_backtest_prepare_s": float(test_curve_timing.backtest_prepare_s),
        "test_curve_eval_backtest_runner_s": float(test_curve_timing.backtest_runner_s),
        "test_curve_eval_backtest_finalize_s": float(test_curve_timing.backtest_finalize_s),
        "test_curve_ic_s": float(test_curve_timing.ic_s),
        "test_curve_metrics_reduce_s": float(test_curve_timing.metrics_s),
        "test_curve_eval_metrics_s": float(test_curve_timing.metrics_s),
        "test_curve_concat_s": float(test_curve_timing.concat_s),
        "test_curve_eval_concat_s": float(test_curve_timing.concat_s),
        "test_curve_eval_cpu_gpu_sync_s": float(test_curve_timing.cpu_gpu_sync_s),
        "bt_compile_hits": int(backtest_compile_stats.get("hits", 0)),
        "bt_compile_misses": int(backtest_compile_stats.get("misses", 0)),
        "bt_compile_failures": int(backtest_compile_stats.get("failures", 0)),
        "bt_compile_disabled": int(backtest_compile_stats.get("disabled", 0)),
        "bt_compile_nonhit": int(
            backtest_compile_stats.get("misses", 0)
            + backtest_compile_stats.get("failures", 0)
            + backtest_compile_stats.get("disabled", 0)
        ),
        "bt_prep_compile_hits": int(backtest_prep_compile_stats.get("hits", 0)),
        "bt_prep_compile_misses": int(backtest_prep_compile_stats.get("misses", 0)),
        "bt_prep_compile_failures": int(backtest_prep_compile_stats.get("failures", 0)),
        "bt_prep_compile_disabled": int(backtest_prep_compile_stats.get("disabled", 0)),
        "bt_prep_compile_nonhit": int(
            backtest_prep_compile_stats.get("misses", 0)
            + backtest_prep_compile_stats.get("failures", 0)
            + backtest_prep_compile_stats.get("disabled", 0)
        ),
        "bt_runtime_calls": int(backtest_runtime_stats.get("calls", 0.0)),
        "bt_runtime_ms_per_call": _bt_avg_ms("total_s"),
        "bt_runtime_cuda_ms_per_call": _bt_avg_ms("total_cuda_s"),
        "bt_runtime_calls_per_train_batch": float(backtest_runtime_stats.get("calls", 0.0)) / float(batches),
        "bt_prep_ms_per_call": _bt_avg_ms("prep_s"),
        "bt_prep_cuda_ms_per_call": _bt_avg_ms("prep_cuda_s"),
        "bt_prev_init_ms_per_call": _bt_avg_ms("prev_init_s"),
        "bt_prev_init_cuda_ms_per_call": _bt_avg_ms("prev_init_cuda_s"),
        "bt_runner_resolve_ms_per_call": _bt_avg_ms("runner_resolve_s"),
        "bt_runner_call_ms_per_call": _bt_avg_ms("runner_call_s"),
        "bt_runner_call_cuda_ms_per_call": _bt_avg_ms("runner_call_cuda_s"),
        "bt_runtime_fallback_ms_per_call": _bt_avg_ms("runtime_fallback_s"),
        "bt_checkpoint_ms_per_call": _bt_avg_ms("checkpoint_s"),
        "bt_checkpoint_cuda_ms_per_call": _bt_avg_ms("checkpoint_cuda_s"),
        "bt_finalize_ms_per_call": _bt_avg_ms("finalize_s"),
        "bt_finalize_cuda_ms_per_call": _bt_avg_ms("finalize_cuda_s"),
        "bt_dense_fast_path_ms_per_call": _bt_avg_ms("dense_fast_path_s"),
        "bt_dense_fast_path_cuda_ms_per_call": _bt_avg_ms("dense_fast_path_cuda_s"),
        "bt_cpp_ext_ms_per_call": _bt_avg_ms("cpp_ext_s"),
        "bt_cpp_ext_cuda_ms_per_call": _bt_avg_ms("cpp_ext_cuda_s"),
        "bt_compiled_runner_calls": int(backtest_runtime_stats.get("compiled_runner_calls", 0.0)),
        "bt_eager_runner_calls": int(backtest_runtime_stats.get("eager_runner_calls", 0.0)),
        "bt_stateful_calls": int(backtest_runtime_stats.get("stateful_calls", 0.0)),
        "bt_stateful_compiled_runner_calls": int(backtest_runtime_stats.get("stateful_compiled_runner_calls", 0.0)),
        "bt_stateful_eager_runner_calls": int(backtest_runtime_stats.get("stateful_eager_runner_calls", 0.0)),
        "bt_nonstateful_compiled_runner_calls": int(backtest_runtime_stats.get("nonstateful_compiled_runner_calls", 0.0)),
        "bt_nonstateful_eager_runner_calls": int(backtest_runtime_stats.get("nonstateful_eager_runner_calls", 0.0)),
        "bt_runtime_fallback_calls": int(backtest_runtime_stats.get("runtime_fallback_calls", 0.0)),
        "bt_dense_fast_path_calls": int(backtest_runtime_stats.get("dense_fast_path_calls", 0.0)),
        "bt_cpp_ext_calls": int(backtest_runtime_stats.get("cpp_ext_calls", 0.0)),
        "bt_cpp_ext_failures": int(backtest_runtime_stats.get("cpp_ext_failures", 0.0)),
        "bt_checkpoint_calls": int(backtest_runtime_stats.get("checkpoint_calls", 0.0)),
        "bt_compiled_prep_calls": int(backtest_runtime_stats.get("compiled_prep_calls", 0.0)),
        "bt_eager_prep_calls": int(backtest_runtime_stats.get("eager_prep_calls", 0.0)),
        "bt_return_weights_history_calls": int(backtest_runtime_stats.get("return_weights_history_calls", 0.0)),
        "loss_initial_weights_clone_ms_per_batch": _loss_avg_ms("initial_weights_clone_s"),
        "loss_initial_weights_clone_calls": int(loss_runtime_stats.get("initial_weights_clone_calls", 0.0)),
        "loss_final_weights_clone_ms_per_batch": _loss_avg_ms("final_weights_clone_s"),
        "loss_final_weights_clone_calls": int(loss_runtime_stats.get("final_weights_clone_calls", 0.0)),
        "loss_prepare_inputs_ms_per_batch": _loss_avg_ms("prepare_inputs_s"),
        "loss_prepare_inputs_calls": int(loss_runtime_stats.get("prepare_inputs_calls", 0.0)),
        "loss_normalize_weights_ms_per_batch": _loss_avg_ms("normalize_weights_s"),
        "loss_normalize_weights_calls": int(loss_runtime_stats.get("normalize_weights_calls", 0.0)),
        "loss_build_orders_ms_per_batch": _loss_avg_ms("build_orders_s"),
        "loss_build_orders_calls": int(loss_runtime_stats.get("build_orders_calls", 0.0)),
        "loss_backtest_ms_per_batch": _loss_avg_ms("backtest_s"),
        "loss_backtest_calls": int(loss_runtime_stats.get("backtest_calls", 0.0)),
        "loss_returns_postprocess_ms_per_batch": _loss_avg_ms("returns_postprocess_s"),
        "loss_returns_postprocess_calls": int(loss_runtime_stats.get("returns_postprocess_calls", 0.0)),
        "loss_log_utility_ms_per_batch": _loss_avg_ms("log_utility_s"),
        "loss_log_utility_calls": int(loss_runtime_stats.get("log_utility_calls", 0.0)),
        "loss_nan_to_num_ms_per_batch": _loss_avg_ms("nan_to_num_s"),
        "loss_nan_to_num_calls": int(loss_runtime_stats.get("nan_to_num_calls", 0.0)),
        "loss_mask_apply_ms_per_batch": _loss_avg_ms("mask_apply_s"),
        "loss_mask_apply_calls": int(loss_runtime_stats.get("mask_apply_calls", 0.0)),
        "loss_reduce_ms_per_batch": _loss_avg_ms("reduce_s"),
        "loss_reduce_calls": int(loss_runtime_stats.get("reduce_calls", 0.0)),
        "loss_clone_ms_per_batch": _loss_avg_ms("clone_s"),
        "loss_clone_calls": int(loss_runtime_stats.get("clone_calls", 0.0)),
        "loss_state_update_ms_per_batch": _loss_avg_ms("state_update_s"),
        "loss_state_update_calls": int(loss_runtime_stats.get("state_update_calls", 0.0)),
        "loss_autograd_graph_build_ms_per_batch": _loss_avg_ms("autograd_graph_build_s"),
        "loss_autograd_graph_build_calls": int(loss_runtime_stats.get("autograd_graph_build_calls", 0.0)),
        "loss_prepare_inputs_total_s": float(loss_runtime_stats.get("prepare_inputs_s", 0.0)),
        "loss_normalize_weights_total_s": float(loss_runtime_stats.get("normalize_weights_s", 0.0)),
        "loss_build_orders_total_s": float(loss_runtime_stats.get("build_orders_s", 0.0)),
        "loss_backtest_total_s": float(loss_runtime_stats.get("backtest_s", 0.0)),
        "loss_returns_postprocess_total_s": float(loss_runtime_stats.get("returns_postprocess_s", 0.0)),
        "loss_log_utility_total_s": float(loss_runtime_stats.get("log_utility_s", 0.0)),
        "loss_nan_to_num_total_s": float(loss_runtime_stats.get("nan_to_num_s", 0.0)),
        "loss_mask_apply_total_s": float(loss_runtime_stats.get("mask_apply_s", 0.0)),
        "loss_reduce_total_s": float(loss_runtime_stats.get("reduce_s", 0.0)),
        "loss_clone_total_s": float(loss_runtime_stats.get("clone_s", 0.0)),
        "loss_state_update_total_s": float(loss_runtime_stats.get("state_update_s", 0.0)),
        "loss_autograd_graph_build_total_s": float(loss_runtime_stats.get("autograd_graph_build_s", 0.0)),
        "fold_checkpoint_save_s": float(fold_checkpoint_save_s),
        "group_checkpoint_save_s": float(group_checkpoint_save_s),
        "checkpoint_save_s": checkpoint_total_s,
        "scheduler_s": float(scheduler_s),
        "progress_update_s": float(progress_update_s),
        "curve_record_s": float(curve_record_s),
        "scalar_sync_s": float(scalar_sync_s),
        "cuda_sync_s": float(cuda_sync_s),
        "gc_s": float(gc_s),
        "epoch_wall_s": epoch_total_s,
        "epoch_unattributed_s": unattributed_s,
        "epoch_total_s": epoch_total_s,
        "epoch_step_train_loss_total_time_s": float(train_timing.loss_s),
        "epoch_step_train_loss_percent": _epoch_percent(train_timing.loss_s),
        "epoch_step_train_loss_calls": int(train_timing.batches),
        "epoch_step_train_loss_ms_per_call": _timing_ms_per_call(train_timing.loss_s, train_timing.batches),
        "epoch_step_val_backtest_total_time_s": float(val_timing.backtest_s),
        "epoch_step_val_backtest_percent": _epoch_percent(val_timing.backtest_s),
        "epoch_step_val_backtest_calls": int(val_timing.batches),
        "epoch_step_val_backtest_ms_per_call": _timing_ms_per_call(val_timing.backtest_s, val_timing.batches),
        "epoch_step_test_curve_backtest_total_time_s": float(test_curve_timing.backtest_s),
        "epoch_step_test_curve_backtest_percent": _epoch_percent(test_curve_timing.backtest_s),
        "epoch_step_test_curve_backtest_calls": int(test_curve_timing.batches),
        "epoch_step_test_curve_backtest_ms_per_call": _timing_ms_per_call(test_curve_timing.backtest_s, test_curve_timing.batches),
        "epoch_step_test_curve_transfer_total_time_s": float(test_curve_timing.transfer_s),
        "epoch_step_test_curve_transfer_percent": _epoch_percent(test_curve_timing.transfer_s),
        "epoch_step_test_curve_transfer_calls": int(test_curve_timing.batches),
        "epoch_step_test_curve_transfer_ms_per_call": _timing_ms_per_call(test_curve_timing.transfer_s, test_curve_timing.batches),
        "epoch_step_val_transfer_total_time_s": float(val_timing.transfer_s),
        "epoch_step_val_transfer_percent": _epoch_percent(val_timing.transfer_s),
        "epoch_step_val_transfer_calls": int(val_timing.batches),
        "epoch_step_val_transfer_ms_per_call": _timing_ms_per_call(val_timing.transfer_s, val_timing.batches),
    }


def _summary_path(output_path: Path) -> Path:
    return output_path / "summary.json"


def _load_fold_result(metrics_path: Path) -> FoldResult:
    with metrics_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return FoldResult(**payload)


def _write_summary(results: list[FoldResult], output_path: Path) -> None:
    summary_path = _summary_path(output_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)


def _unwrap_model(model: nn.Module) -> nn.Module:
    return getattr(model, "_orig_mod", model)


def _state_dict_for_save(model: nn.Module) -> dict[str, torch.Tensor]:
    return _unwrap_model(model).state_dict()


def _model_parameters_are_finite(model: nn.Module) -> bool:
    for param in _unwrap_model(model).parameters():
        if not torch.isfinite(param.detach()).all():
            return False
    return True


def _model_gradients_are_finite(model: nn.Module) -> bool:
    for param in _unwrap_model(model).parameters():
        if param.grad is not None and not torch.isfinite(param.grad.detach()).all():
            return False
    return True


def _should_check_finite(step: int, interval_steps: int) -> bool:
    interval = int(interval_steps)
    return interval > 0 and int(step) % interval == 0


def _tensor_is_finite(value: torch.Tensor) -> bool:
    return bool(torch.isfinite(value.detach()).all().item())


def _load_state_dict(model: nn.Module, state_dict: dict) -> None:
    cleaned_state_dict = {
        key.removeprefix("_orig_mod."): value for key, value in state_dict.items()
    }
    _unwrap_model(model).load_state_dict(cleaned_state_dict)


def _save_fold_checkpoint(
    checkpoint_path: Path,
    *,
    fold: WalkForwardFold,
    epoch: int,
    best_val_loss: float,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    include_optimizer: bool = False,
) -> None:
    if not _model_parameters_are_finite(model):
        raise RuntimeError(f"Refusing to save non-finite model checkpoint: {checkpoint_path}")
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "fold_id": fold.fold_id,
        "epoch": epoch,
        "train_years": fold.train_years,
        "val_years": fold.val_years,
        "test_years": fold.test_years,
        "best_val_loss": best_val_loss,
        "model_state_dict": _state_dict_for_save(model),
    }
    if include_optimizer:
        payload["optimizer_state_dict"] = optimizer.state_dict()
        payload["scaler_state_dict"] = scaler.state_dict()
    torch.save(payload, checkpoint_path)


def _load_checkpoint(checkpoint_path: Path) -> dict:
    return torch.load(checkpoint_path, map_location="cpu")


def _save_group_checkpoint(
    checkpoint_path: Path,
    *,
    train_years: list[int],
    epoch: int,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    scheduler: torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None = None,
) -> None:
    if not _model_parameters_are_finite(model):
        raise RuntimeError(f"Refusing to save non-finite model checkpoint: {checkpoint_path}")
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    scheduler_state = scheduler.state_dict() if scheduler is not None else None
    torch.save(
        {
            "train_years": train_years,
            "epoch": epoch,
            "model_state_dict": _state_dict_for_save(model),
            "optimizer_state_dict": optimizer.state_dict(),
            "scaler_state_dict": scaler.state_dict(),
            "scheduler_state_dict": scheduler_state,
        },
        checkpoint_path,
    )


def _create_lr_scheduler(
    optimizer: torch.optim.Optimizer,
    config: ExperimentConfig,
) -> tuple[torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None, str, bool]:
    if not bool(getattr(config.training, "enable_lr_scheduler", True)):
        return None, "disabled", False

    name = str(getattr(config.training, "lr_scheduler", "none") or "none").strip().lower().replace("-", "_")
    if name in {"", "none", "off", "false", "disabled"}:
        return None, "none", False

    if name == "cosine":
        t_max = int(config.training.lr_scheduler_t_max)
        if t_max <= 0:
            t_max = max(1, int(config.training.epochs))
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=t_max,
            eta_min=float(config.training.lr_scheduler_eta_min),
        )
        return scheduler, "cosine", False

    if name == "step":
        scheduler = torch.optim.lr_scheduler.StepLR(
            optimizer,
            step_size=max(1, int(config.training.lr_scheduler_step_size)),
            gamma=float(config.training.lr_scheduler_gamma),
        )
        return scheduler, "step", False

    if name in {"plateau", "reduce_on_plateau", "reduce_lr_on_plateau"}:
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer,
            mode="min",
            factor=float(config.training.lr_scheduler_gamma),
            patience=max(1, int(config.training.lr_scheduler_patience)),
            threshold=float(config.training.lr_scheduler_threshold),
        )
        return scheduler, "plateau", True

    print(f"[train] unknown lr_scheduler='{config.training.lr_scheduler}', disabled")
    return None, "none", False


def _benchmark_input_pipeline_throughput(
    *,
    train_ds: CrossSectionalDataset,
    train_x: torch.Tensor,
    train_returns: torch.Tensor,
    train_masks: torch.Tensor,
    train_batch_size: int,
    config: ExperimentConfig,
    device: torch.device,
    non_blocking: bool,
    drop_last: bool = False,
    max_steps: int = 20,
) -> tuple[float, float]:
    """Return (dataloader_samples_per_sec, tensor_samples_per_sec) for quick A/B selection."""
    if max_steps <= 0:
        return 0.0, 0.0

    def _sync_if_cuda() -> None:
        if device.type == "cuda":
            torch.cuda.synchronize(device)

    dataloader_sps = 0.0
    if config.training.num_workers > 0:
        loader = _build_loader(train_ds, train_batch_size, True, config, device, drop_last=drop_last)
        steps = 0
        samples = 0
        start_t = time.perf_counter()
        for batch in loader:
            moved = _move_batch(batch, device, non_blocking)
            samples += int(moved["x"].size(0))
            steps += 1
            if steps >= max_steps:
                break
        _sync_if_cuda()
        elapsed = max(time.perf_counter() - start_t, 1e-6)
        dataloader_sps = samples / elapsed

    total_rows = int(train_x.size(0))
    if total_rows == 0:
        return dataloader_sps, 0.0

    tensor_steps = min(max_steps, max(1, total_rows // train_batch_size))
    samples = 0
    start_t = time.perf_counter()
    for idx in range(tensor_steps):
        start = idx * train_batch_size
        end = min(start + train_batch_size, total_rows)
        batch_x = train_x[start:end].to(device=device, non_blocking=non_blocking)
        batch_ret = train_returns[start:end].to(device=device, non_blocking=non_blocking)
        batch_mask = train_masks[start:end].to(device=device, non_blocking=non_blocking)
        samples += int(batch_x.size(0))
        # Ensure slices are actually materialized on device before timing stops.
        _ = (batch_x, batch_ret, batch_mask)
    _sync_if_cuda()
    elapsed = max(time.perf_counter() - start_t, 1e-6)
    tensor_sps = samples / elapsed

    return dataloader_sps, tensor_sps


def _benchmark_windowed_input_pipeline_throughput(
    *,
    train_ds: CrossSectionalDataset,
    train_windowed: WindowedSplitTensors,
    train_batch_size: int,
    config: ExperimentConfig,
    device: torch.device,
    non_blocking: bool,
    drop_last: bool = False,
    max_steps: int = 20,
) -> tuple[float, float]:
    """Return (dataloader_samples_per_sec, windowed_tensor_samples_per_sec)."""
    if max_steps <= 0:
        return 0.0, 0.0

    def _sync_if_cuda() -> None:
        if device.type == "cuda":
            torch.cuda.synchronize(device)

    dataloader_sps = 0.0
    if config.training.num_workers > 0:
        loader = _build_loader(train_ds, train_batch_size, True, config, device, drop_last=drop_last)
        steps = 0
        samples = 0
        start_t = time.perf_counter()
        for batch in loader:
            moved = _move_batch(batch, device, non_blocking)
            samples += int(moved["x"].size(0))
            steps += 1
            if steps >= max_steps:
                break
        _sync_if_cuda()
        elapsed = max(time.perf_counter() - start_t, 1e-6)
        dataloader_sps = samples / elapsed

    total_rows = int(len(train_windowed))
    if total_rows == 0:
        return dataloader_sps, 0.0

    windowed_steps = min(max_steps, max(1, math.ceil(total_rows / max(1, train_batch_size))))
    samples = 0
    start_t = time.perf_counter()
    for idx in range(windowed_steps):
        start = idx * train_batch_size
        if start >= total_rows:
            break
        end = min(start + train_batch_size, total_rows)
        batch = train_windowed.batch_by_rows(start, end, device=device, non_blocking=non_blocking)
        samples += int(batch["x"].size(0))
        _ = batch
    _sync_if_cuda()
    elapsed = max(time.perf_counter() - start_t, 1e-6)
    windowed_sps = samples / elapsed

    return dataloader_sps, windowed_sps


def _save_backtest_artifact(output_path: Path, result: BacktestResult, dates: np.ndarray) -> None:
    np.savez_compressed(
        output_path,
        strategy_returns=result.strategy_returns,
        benchmark_returns=result.benchmark_returns,
        turnovers=result.turnovers,
        weights_history=result.weights_history,
        dates=np.asarray(dates),
    )


def _save_holdings_csv(
    output_path: Path,
    holdings: list[HoldingsRecord],
) -> None:
    """Save daily holdings detail sorted by holding ratio."""
    import pandas as pd  # already a transitive dependency

    rows = [
        {
            "date": row.date,
            "symbol": row.symbol,
            "shares": int(row.shares),
            "price": float(row.price),
            "market_value": float(row.market_value),
            "holding_ratio": float(row.holding_ratio),
            "is_cash": bool(row.is_cash),
        }
        for row in holdings
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["date", "holding_ratio", "symbol"], ascending=[True, False, True])
    df.to_csv(output_path, index=False)


def _save_daily_portfolio_returns_csv(
    output_path: Path,
    dates: np.ndarray,
    strategy_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    turnovers: np.ndarray,
) -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "date": np.asarray(dates),
            "portfolio_return": np.asarray(strategy_returns, dtype=np.float64),
            "benchmark_return": np.asarray(benchmark_returns, dtype=np.float64),
            "turnover": np.asarray(turnovers, dtype=np.float64),
        }
    )
    df.to_csv(output_path, index=False)


def _save_daily_weights_csv(
    output_path: Path,
    dates: np.ndarray,
    symbols: list[str],
    weights_history: np.ndarray,
) -> None:
    import pandas as pd

    weights = np.asarray(weights_history, dtype=np.float64)
    df = pd.DataFrame(weights, columns=list(symbols))
    df.insert(0, "date", np.asarray(dates))
    df.to_csv(output_path, index=False)


def _save_integer_share_audit_artifacts(
    fold_dir: Path,
    result: BacktestResult,
    dates: np.ndarray,
    symbols: list[str],
    holdings: list[HoldingsRecord],
) -> None:
    _save_backtest_artifact(_integer_backtest_path(fold_dir), result, dates)
    _save_daily_portfolio_returns_csv(
        fold_dir / "integer_share_daily_portfolio_returns.csv",
        dates,
        result.strategy_returns,
        result.benchmark_returns,
        result.turnovers,
    )
    _save_daily_weights_csv(
        fold_dir / "integer_share_daily_weights.csv",
        dates,
        symbols,
        result.weights_history,
    )
    with (fold_dir / "integer_share_annual_report.txt").open("w", encoding="utf-8") as f:
        f.write(generate_annual_report(result, dates))
    _save_holdings_csv(fold_dir / "holdings.csv", holdings)


def _load_backtest_artifact(output_path: Path) -> tuple[BacktestResult, np.ndarray]:
    data = np.load(output_path)
    result = BacktestResult(
        strategy_returns=data["strategy_returns"].astype(np.float32),
        benchmark_returns=data["benchmark_returns"].astype(np.float32),
        turnovers=data["turnovers"].astype(np.float32),
        weights_history=data["weights_history"].astype(np.float32),
    )
    dates = np.asarray(data["dates"])
    return result, dates


def _dataset_to_tensors(
    dataset: CrossSectionalDataset,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    valid_indices = torch.as_tensor(dataset.valid_indices, dtype=torch.long)
    if dataset.lookback == 1:
        x = dataset.features_t[valid_indices].unsqueeze(1)
    else:
        x = torch.stack([dataset[i]["x"] for i in range(len(dataset))], dim=0)
    returns = dataset.future_log_returns_t[valid_indices]
    masks = dataset.tradable_mask_t[valid_indices]
    can_buy_masks = dataset.can_buy_mask_t[valid_indices]
    can_sell_masks = dataset.can_sell_mask_t[valid_indices]
    bench = dataset.benchmark_t[valid_indices]
    return x, returns, masks, can_buy_masks, can_sell_masks, bench


def _prepare_host_tensor(tensor: torch.Tensor, pin_memory: bool) -> torch.Tensor:
    prepared = tensor.contiguous()
    if pin_memory and prepared.device.type == "cpu" and not prepared.is_pinned():
        tensor_bytes = _tensor_nbytes(prepared)
        if tensor_bytes > 2 * 1024**3:
            print(
                f"[pin_memory] skipped for large tensor shape={tuple(prepared.shape)} "
                f"size={tensor_bytes/1024**3:.1f}GB"
            )
            return prepared
        try:
            prepared = prepared.pin_memory()
        except torch.AcceleratorError as exc:
            if "out of memory" not in str(exc).lower():
                raise
            print(
                f"[pin_memory] skipped for tensor shape={tuple(prepared.shape)} "
                f"size={tensor_bytes/1024**3:.1f}GB: CUDA OOM"
            )
    return prepared


def _tensor_nbytes(tensor: torch.Tensor) -> int:
    return int(tensor.numel() * tensor.element_size())


def _maybe_cache_tensors_on_device(
    *,
    name: str,
    tensors: tuple[torch.Tensor, ...],
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
) -> tuple[torch.Tensor, ...]:
    if not enabled or device.type != "cuda":
        return tensors
    if all(tensor.device.type == "cuda" for tensor in tensors):
        return tensors

    required_bytes = sum(_tensor_nbytes(tensor) for tensor in tensors if tensor.device.type != "cuda")
    free_mem, total_mem = torch.cuda.mem_get_info(device)
    margin_bytes = int(max(0.0, float(safety_margin_gb)) * 1024**3)
    usable_bytes = int(max(0, int(free_mem) - margin_bytes) * max(0.0, float(target_fraction)))
    if required_bytes > usable_bytes:
        print(
            f"[gpu cache] skipped {name}: need={required_bytes/1024**3:.2f}GB "
            f"usable={usable_bytes/1024**3:.2f}GB "
            f"(free={free_mem/1024**3:.2f}GB total={total_mem/1024**3:.2f}GB)"
        )
        return tensors

    moved: list[torch.Tensor] = []
    try:
        for tensor in tensors:
            moved.append(tensor if tensor.device.type == "cuda" else tensor.to(device=device, non_blocking=True))
        torch.cuda.synchronize(device)
        print(
            f"[gpu cache] cached {name} on {device}: size={required_bytes/1024**3:.2f}GB "
            f"(free_before={free_mem/1024**3:.2f}GB)"
        )
        return tuple(moved)
    except RuntimeError as exc:
        if "out of memory" not in str(exc).lower():
            raise
        print(f"[gpu cache] skipped {name}: CUDA OOM during cache attempt")
        del moved
        _release_cuda_memory(device)
        return tensors


def _prepare_split_tensors(
    x: torch.Tensor,
    returns: torch.Tensor,
    masks: torch.Tensor,
    can_buy_masks: torch.Tensor,
    can_sell_masks: torch.Tensor,
    bench: torch.Tensor,
    device: torch.device,
    non_blocking: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    pin_memory = device.type == "cuda" and non_blocking
    return (
        _prepare_host_tensor(x, pin_memory),
        _prepare_host_tensor(returns, pin_memory),
        _prepare_host_tensor(masks, pin_memory),
        _prepare_host_tensor(can_buy_masks, pin_memory),
        _prepare_host_tensor(can_sell_masks, pin_memory),
        _prepare_host_tensor(bench, pin_memory),
    )


def _pad_rows(tensor: torch.Tensor, target_rows: int, fill_value: int | float | bool = 0) -> torch.Tensor:
    current_rows = int(tensor.size(0))
    if current_rows >= target_rows:
        return tensor

    pad_shape = (target_rows - current_rows, *tensor.shape[1:])
    padded = tensor.new_full(pad_shape, fill_value)
    return torch.cat([tensor, padded], dim=0)


def _pad_training_tensors(
    x: torch.Tensor,
    returns: torch.Tensor,
    masks: torch.Tensor,
    can_buy_masks: torch.Tensor,
    can_sell_masks: torch.Tensor,
    benchmark: torch.Tensor,
    batch_size: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    total_rows = int(x.size(0))
    if total_rows == 0:
        return x, returns, masks, can_buy_masks, can_sell_masks, benchmark, torch.empty((0,), dtype=torch.bool)

    padded_rows = ((total_rows + batch_size - 1) // batch_size) * batch_size
    sample_mask = torch.ones(total_rows, dtype=torch.bool)
    if padded_rows == total_rows:
        return x, returns, masks, can_buy_masks, can_sell_masks, benchmark, sample_mask

    return (
        _pad_rows(x, padded_rows, 0),
        _pad_rows(returns, padded_rows, 0.0),
        _pad_rows(masks, padded_rows, False),
        _pad_rows(can_buy_masks, padded_rows, False),
        _pad_rows(can_sell_masks, padded_rows, False),
        _pad_rows(benchmark, padded_rows, 0.0),
        _pad_rows(sample_mask, padded_rows, False),
    )


def _combine_datasets_to_tensors(
    datasets: list[CrossSectionalDataset],
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, list[int]]:
    xs: list[torch.Tensor] = []
    returns: list[torch.Tensor] = []
    masks: list[torch.Tensor] = []
    can_buy_masks: list[torch.Tensor] = []
    can_sell_masks: list[torch.Tensor] = []
    bench: list[torch.Tensor] = []
    lengths: list[int] = []
    for dataset in datasets:
        x, y, m, mb, ms, b = _dataset_to_tensors(dataset)
        xs.append(x)
        returns.append(y)
        masks.append(m)
        can_buy_masks.append(mb)
        can_sell_masks.append(ms)
        bench.append(b)
        lengths.append(int(x.size(0)))
    return (
        torch.cat(xs, dim=0),
        torch.cat(returns, dim=0),
        torch.cat(masks, dim=0),
        torch.cat(can_buy_masks, dim=0),
        torch.cat(can_sell_masks, dim=0),
        torch.cat(bench, dim=0),
        lengths,
    )


def _combine_datasets_to_windowed(
    datasets: list[CrossSectionalDataset],
) -> tuple[WindowedSplitTensors, list[int]]:
    if not datasets:
        raise ValueError("datasets must be non-empty")
    first = dataset_to_windowed_tensors(datasets[0])
    lengths = [len(datasets[0])]
    valid_indices = [first.valid_indices]
    for dataset in datasets[1:]:
        split = dataset_to_windowed_tensors(dataset)
        if split.lookback != first.lookback:
            raise ValueError("all windowed datasets must use the same lookback")
        valid_indices.append(split.valid_indices)
        lengths.append(len(dataset))
    combined = WindowedSplitTensors(
        features=first.features,
        valid_indices=torch.cat(valid_indices, dim=0),
        future_log_returns=first.future_log_returns,
        tradable_mask=first.tradable_mask,
        can_buy_mask=first.can_buy_mask,
        can_sell_mask=first.can_sell_mask,
        benchmark=first.benchmark,
        lookback=first.lookback,
    )
    return combined, lengths


def _pad_windowed_training_split(split: WindowedSplitTensors, batch_size: int) -> WindowedSplitTensors:
    total_rows = len(split)
    if total_rows == 0:
        sample_mask = torch.empty((0,), dtype=torch.bool, device=split.valid_indices.device)
        return WindowedSplitTensors(
            features=split.features,
            valid_indices=split.valid_indices,
            future_log_returns=split.future_log_returns,
            tradable_mask=split.tradable_mask,
            can_buy_mask=split.can_buy_mask,
            can_sell_mask=split.can_sell_mask,
            benchmark=split.benchmark,
            lookback=split.lookback,
            sample_mask=sample_mask,
        )
    padded_rows = ((total_rows + batch_size - 1) // batch_size) * batch_size
    sample_mask = torch.ones(total_rows, dtype=torch.bool, device=split.valid_indices.device)
    if padded_rows > total_rows:
        pad_count = padded_rows - total_rows
        valid_indices = torch.cat(
            [
                split.valid_indices,
                split.valid_indices[-1:].expand(pad_count),
            ],
            dim=0,
        )
        sample_mask = torch.cat(
            [
                sample_mask,
                torch.zeros(pad_count, dtype=torch.bool, device=split.valid_indices.device),
            ],
            dim=0,
        )
    else:
        valid_indices = split.valid_indices
    return WindowedSplitTensors(
        features=split.features,
        valid_indices=valid_indices,
        future_log_returns=split.future_log_returns,
        tradable_mask=split.tradable_mask,
        can_buy_mask=split.can_buy_mask,
        can_sell_mask=split.can_sell_mask,
        benchmark=split.benchmark,
        lookback=split.lookback,
        sample_mask=sample_mask,
    )


def _prepare_windowed_split(
    split: WindowedSplitTensors,
    device: torch.device,
    non_blocking: bool,
) -> WindowedSplitTensors:
    pin_memory = device.type == "cuda" and non_blocking
    return WindowedSplitTensors(
        features=_prepare_host_tensor(split.features, pin_memory),
        valid_indices=_prepare_host_tensor(split.valid_indices, pin_memory),
        future_log_returns=_prepare_host_tensor(split.future_log_returns, pin_memory),
        tradable_mask=_prepare_host_tensor(split.tradable_mask, pin_memory),
        can_buy_mask=_prepare_host_tensor(split.can_buy_mask, pin_memory),
        can_sell_mask=_prepare_host_tensor(split.can_sell_mask, pin_memory),
        benchmark=_prepare_host_tensor(split.benchmark, pin_memory),
        lookback=split.lookback,
        sample_mask=None if split.sample_mask is None else _prepare_host_tensor(split.sample_mask, pin_memory),
    )


def _maybe_cache_windowed_split_on_device(
    *,
    name: str,
    split: WindowedSplitTensors,
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
) -> WindowedSplitTensors:
    tensors: tuple[torch.Tensor, ...]
    if split.sample_mask is None:
        tensors = (
            split.features,
            split.valid_indices,
            split.future_log_returns,
            split.tradable_mask,
            split.can_buy_mask,
            split.can_sell_mask,
            split.benchmark,
        )
    else:
        tensors = (
            split.features,
            split.valid_indices,
            split.future_log_returns,
            split.tradable_mask,
            split.can_buy_mask,
            split.can_sell_mask,
            split.benchmark,
            split.sample_mask,
        )
    moved = _maybe_cache_tensors_on_device(
        name=name,
        tensors=tensors,
        device=device,
        enabled=enabled,
        target_fraction=target_fraction,
        safety_margin_gb=safety_margin_gb,
    )
    return WindowedSplitTensors(
        features=moved[0],
        valid_indices=moved[1],
        future_log_returns=moved[2],
        tradable_mask=moved[3],
        can_buy_mask=moved[4],
        can_sell_mask=moved[5],
        benchmark=moved[6],
        lookback=split.lookback,
        sample_mask=None if split.sample_mask is None else moved[7],
    )


def _windowed_base_compatible(a: WindowedSplitTensors, b: WindowedSplitTensors) -> bool:
    attrs = (
        "features",
        "future_log_returns",
        "tradable_mask",
        "can_buy_mask",
        "can_sell_mask",
        "benchmark",
    )
    for attr in attrs:
        lhs = getattr(a, attr)
        rhs = getattr(b, attr)
        if tuple(lhs.shape) != tuple(rhs.shape) or lhs.dtype != rhs.dtype:
            return False
    return int(a.lookback) == int(b.lookback)


def _tensor_on_requested_device(tensor: torch.Tensor, device: torch.device) -> bool:
    if tensor.device.type != device.type:
        return False
    if device.index is not None and tensor.device.index != device.index:
        return False
    return True


def _maybe_share_windowed_base_from_cached(
    *,
    name: str,
    split: WindowedSplitTensors,
    cached_base: WindowedSplitTensors | None,
    device: torch.device,
    non_blocking: bool,
    enabled: bool,
) -> WindowedSplitTensors | None:
    if not enabled or cached_base is None:
        return None
    if not _tensor_on_requested_device(cached_base.features, device):
        return None
    if not _windowed_base_compatible(split, cached_base):
        return None
    valid_indices = split.valid_indices.to(device=device, non_blocking=non_blocking)
    sample_mask = None
    if split.sample_mask is not None:
        sample_mask = split.sample_mask.to(device=device, non_blocking=non_blocking)
    _progress(f"[gpu cache] reused shared base tensors for {name} on {device.type}")
    return WindowedSplitTensors(
        features=cached_base.features,
        valid_indices=valid_indices,
        future_log_returns=cached_base.future_log_returns,
        tradable_mask=cached_base.tradable_mask,
        can_buy_mask=cached_base.can_buy_mask,
        can_sell_mask=cached_base.can_sell_mask,
        benchmark=cached_base.benchmark,
        lookback=split.lookback,
        sample_mask=sample_mask,
    )


def _pad_eval_chunk_first_dim(
    x: torch.Tensor,
    returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark: torch.Tensor,
    *,
    target_rows: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, int]:
    valid_rows = int(x.size(0))
    target_rows = int(target_rows)
    if valid_rows <= 0 or valid_rows >= target_rows:
        return x, returns, tradable_mask, can_buy_mask, can_sell_mask, benchmark, valid_rows

    pad_rows = target_rows - valid_rows
    x_pad = x[-1:].expand((pad_rows,) + tuple(x.shape[1:]))
    returns_pad = returns.new_zeros((pad_rows,) + tuple(returns.shape[1:]))
    tradable_pad = tradable_mask.new_zeros((pad_rows,) + tuple(tradable_mask.shape[1:]))
    buy_pad = can_buy_mask.new_zeros((pad_rows,) + tuple(can_buy_mask.shape[1:]))
    sell_pad = can_sell_mask.new_zeros((pad_rows,) + tuple(can_sell_mask.shape[1:]))
    benchmark_pad = benchmark.new_zeros((pad_rows,) + tuple(benchmark.shape[1:]))
    return (
        torch.cat((x, x_pad), dim=0),
        torch.cat((returns, returns_pad), dim=0),
        torch.cat((tradable_mask, tradable_pad), dim=0),
        torch.cat((can_buy_mask, buy_pad), dim=0),
        torch.cat((can_sell_mask, sell_pad), dim=0),
        torch.cat((benchmark, benchmark_pad), dim=0),
        valid_rows,
    )


def _should_log_eval_chunk(chunk_idx: int, total_chunks: int) -> bool:
    total_chunks = max(1, int(total_chunks))
    interval = max(1, total_chunks // 20)
    return int(chunk_idx) == 1 or int(chunk_idx) == total_chunks or int(chunk_idx) % interval == 0


def _eval_ranges_by_reset(
    total_rows: int,
    chunk_rows: int,
    reset_at_rows: Sequence[int] | None,
) -> list[tuple[int, int, bool]]:
    total_rows = int(total_rows)
    chunk_rows = max(1, int(chunk_rows))
    reset_points = {0, total_rows}
    if reset_at_rows is not None:
        reset_points.update(int(value) for value in reset_at_rows if 0 <= int(value) <= total_rows)
    reset_points_sorted = sorted(reset_points)
    ranges: list[tuple[int, int, bool]] = []
    for segment_start, segment_end in zip(reset_points_sorted[:-1], reset_points_sorted[1:]):
        if segment_end <= segment_start:
            continue
        for start in range(segment_start, segment_end, chunk_rows):
            end = min(start + chunk_rows, segment_end)
            ranges.append((start, end, start == segment_start))
    return ranges


def _finalize_ic_summary_from_series(
    ic_series: torch.Tensor,
    *,
    device: torch.device,
    profile_timing: bool,
    timing: TimingBreakdown,
) -> dict[str, float]:
    ic_finite = torch.isfinite(ic_series)
    ic_clean64 = torch.nan_to_num(ic_series, nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    ic_mask64 = ic_finite.to(torch.float64)
    cpu_gpu_sync_start = time.perf_counter()
    ic_count, ic_sum, ic_sumsq, ic_pos = (
        torch.stack(
            [
                ic_mask64.sum(),
                (ic_clean64 * ic_mask64).sum(),
                (ic_clean64 * ic_clean64 * ic_mask64).sum(),
                ((ic_clean64 > 0).to(torch.float64) * ic_mask64).sum(),
            ]
        )
        .detach()
        .cpu()
        .tolist()
    )
    _maybe_sync_cuda(device, profile_timing)
    timing.cpu_gpu_sync_s += time.perf_counter() - cpu_gpu_sync_start
    if ic_count <= 0:
        return {"ic_mean": 0.0, "ic_std": 0.0, "ic_ir": 0.0, "ic_positive_ratio": 0.0}
    ic_n = float(ic_count)
    ic_mean = ic_sum / ic_n
    ic_var = max(0.0, ic_sumsq / ic_n - ic_mean * ic_mean)
    ic_std = (ic_var ** 0.5) + 1e-8
    return {
        "ic_mean": ic_mean,
        "ic_std": ic_std,
        "ic_ir": float(ic_mean / ic_std * np.sqrt(252.0)),
        "ic_positive_ratio": ic_pos / ic_n,
    }


def _compute_eval_metrics_like_legacy_online(
    strategy_returns: torch.Tensor,
    benchmark_returns: torch.Tensor,
    turnovers: torch.Tensor,
) -> dict[str, float]:
    r = torch.nan_to_num(strategy_returns.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    b = torch.nan_to_num(benchmark_returns.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    t = torch.nan_to_num(turnovers.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    if r.numel() == 0:
        return {
            "cumulative_return": 0.0,
            "annualized_return": 0.0,
            "cagr": 0.0,
            "sharpe": 0.0,
            "baseline_sharpe": 0.0,
            "sortino": 0.0,
            "baseline_sortino": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "turnover": 0.0,
            "daily_hit_rate": 0.0,
            "excess_return_vs_universe_average": 0.0,
            "cumulative_benchmark": 0.0,
        }

    n = float(r.numel())
    sum_r = float(r.sum().item())
    sum_b = float(b.sum().item())
    sumsq_r = float((r * r).sum().item())
    sumsq_b = float((b * b).sum().item())
    mean_r = sum_r / n
    mean_b = sum_b / n
    var_r = max(0.0, sumsq_r / n - mean_r * mean_r)
    var_b = max(0.0, sumsq_b / n - mean_b * mean_b)
    std_r = var_r ** 0.5
    std_b = var_b ** 0.5
    cum_r = float(math.expm1(sum_r))
    cum_b = float(math.expm1(sum_b))
    ann_r = float(math.expm1(mean_r * 252.0))
    downside_dev = float((torch.minimum(r, torch.zeros_like(r)).pow(2).sum().item() / n) ** 0.5)
    downside_dev_b = float((torch.minimum(b, torch.zeros_like(b)).pow(2).sum().item() / n) ** 0.5)
    cum_log = torch.cumsum(r, dim=0)
    running_max_log = torch.maximum(torch.cummax(cum_log, dim=0).values, torch.zeros((), device=r.device, dtype=r.dtype))
    dd = torch.expm1(torch.clamp(cum_log - running_max_log, min=-745.0, max=0.0))
    max_dd = float(dd.min().item()) if dd.numel() else 0.0
    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "cagr": ann_r,
        "sharpe": float(mean_r / std_r * np.sqrt(252.0)) if std_r > 0 else 0.0,
        "baseline_sharpe": float(mean_b / std_b * np.sqrt(252.0)) if std_b > 0 else 0.0,
        "sortino": float(mean_r / downside_dev * np.sqrt(252.0)) if downside_dev > 0 else 0.0,
        "baseline_sortino": float(mean_b / downside_dev_b * np.sqrt(252.0)) if downside_dev_b > 0 else 0.0,
        "max_drawdown": max_dd,
        "calmar": ann_r / abs(max_dd) if max_dd < 0.0 else 0.0,
        "turnover": float(t.sum().item()) / n,
        "daily_hit_rate": float((r > 0).to(torch.float64).sum().item()) / n,
        "excess_return_vs_universe_average": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def _run_eval_backtest_from_weight_buffers(
    weights_all: torch.Tensor,
    future_log_returns_all: torch.Tensor,
    tradable_mask_all: torch.Tensor,
    can_buy_mask_all: torch.Tensor,
    can_sell_mask_all: torch.Tensor,
    benchmark_all: torch.Tensor,
    *,
    device: torch.device,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    backtest_chunk_rows: int,
    compute_metrics_summary: bool,
    return_weights_history: bool,
    profile_timing: bool,
    progress_label: str | None,
    timing: TimingBreakdown,
    reset_at_rows: Sequence[int] | None,
) -> tuple[BacktestResultTensor, dict[str, float]]:
    total_rows = int(weights_all.size(0))
    num_symbols = int(weights_all.size(1))
    if total_rows <= 0:
        empty_returns = torch.empty((0,), device=device, dtype=torch.float32)
        empty_weights = torch.empty((0, num_symbols), device=device, dtype=torch.float32)
        return (
            BacktestResultTensor(
                strategy_returns=empty_returns,
                benchmark_returns=empty_returns.clone(),
                turnovers=empty_returns.clone(),
                weights_history=empty_weights,
            ),
            {},
        )

    backtest_chunk_rows = max(1, int(backtest_chunk_rows))
    backtest_ranges = _eval_ranges_by_reset(total_rows, backtest_chunk_rows, reset_at_rows)
    total_backtest_chunks = max(1, len(backtest_ranges))
    strategy_returns_out = torch.empty((total_rows,), device=device, dtype=weights_all.dtype)
    benchmark_returns_out = torch.empty((total_rows,), device=device, dtype=weights_all.dtype)
    turnovers_out = torch.empty((total_rows,), device=device, dtype=weights_all.dtype)
    if return_weights_history:
        weights_history_out = torch.empty((total_rows, num_symbols), device=device, dtype=weights_all.dtype)
    else:
        weights_history_out = torch.empty((0, num_symbols), device=device, dtype=weights_all.dtype)

    prev_weights: torch.Tensor | None = None
    for chunk_idx, (start, end, reset_state) in enumerate(backtest_ranges, start=1):
        if reset_state:
            prev_weights = None
        log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, total_backtest_chunks)
        if log_chunk_progress:
            _progress(f"{progress_label}: backtest chunk {chunk_idx}/{total_backtest_chunks} rows=[{start},{end})")

        backtest_start = time.perf_counter()
        backtest_prepare_start = time.perf_counter()
        weights_chunk = weights_all[start:end]
        returns_chunk = future_log_returns_all[start:end].to(device=device, non_blocking=non_blocking)
        mask_chunk = tradable_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        buy_mask_chunk = can_buy_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        sell_mask_chunk = can_sell_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        bench_chunk = benchmark_all[start:end].to(device=device, non_blocking=non_blocking)
        (
            weights_chunk,
            returns_chunk,
            mask_chunk,
            buy_mask_chunk,
            sell_mask_chunk,
            bench_chunk,
            valid_rows,
        ) = _pad_eval_chunk_first_dim(
            weights_chunk,
            returns_chunk,
            mask_chunk,
            buy_mask_chunk,
            sell_mask_chunk,
            bench_chunk,
            target_rows=backtest_chunk_rows,
        )
        initial_weights_chunk = prev_weights
        _maybe_sync_cuda(device, profile_timing)
        timing.backtest_prepare_s += time.perf_counter() - backtest_prepare_start

        backtest_runner_start = time.perf_counter()
        backtest_chunk = run_backtest_torch(
            weights_chunk,
            returns_chunk,
            mask_chunk,
            bench_chunk,
            buy_fee_rate,
            sell_fee_rate,
            long_only=long_only,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            can_buy_mask=buy_mask_chunk,
            can_sell_mask=sell_mask_chunk,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights_chunk,
        )
        _maybe_sync_cuda(device, profile_timing)
        timing.backtest_runner_s += time.perf_counter() - backtest_runner_start

        backtest_finalize_start = time.perf_counter()
        prev_weights = _detach_portfolio_state(backtest_chunk.final_weights)
        strategy_returns_out[start:end].copy_(backtest_chunk.strategy_returns[:valid_rows])
        benchmark_returns_out[start:end].copy_(backtest_chunk.benchmark_returns[:valid_rows])
        turnovers_out[start:end].copy_(backtest_chunk.turnovers[:valid_rows])
        if return_weights_history:
            weights_history_out[start:end].copy_(backtest_chunk.weights_history[:valid_rows])
        _maybe_sync_cuda(device, profile_timing)
        timing.backtest_finalize_s += time.perf_counter() - backtest_finalize_start
        timing.backtest_s += time.perf_counter() - backtest_start

    backtest = BacktestResultTensor(
        strategy_returns=strategy_returns_out,
        benchmark_returns=benchmark_returns_out,
        turnovers=turnovers_out,
        weights_history=weights_history_out,
        final_weights=prev_weights,
    )
    metrics_start = time.perf_counter()
    metrics = (
        _compute_eval_metrics_like_legacy_online(strategy_returns_out, benchmark_returns_out, turnovers_out)
        if compute_metrics_summary
        else {}
    )
    _maybe_sync_cuda(device, profile_timing)
    timing.metrics_s += time.perf_counter() - metrics_start
    timing.batches = int(total_backtest_chunks)
    return backtest, metrics


def _evaluate_tensor_batch_decoupled(
    model: nn.Module,
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    model_chunk_rows: int,
    backtest_chunk_rows: int,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    model.eval()
    timing = TimingBreakdown()
    overall_start = time.perf_counter()
    total_rows = int(x.size(0))
    num_symbols = int(future_log_returns.size(1))
    if total_rows <= 0:
        empty_returns = torch.empty((0,), device=device, dtype=torch.float32)
        empty_weights = torch.empty((0, num_symbols), device=device, dtype=torch.float32)
        backtest = BacktestResultTensor(
            strategy_returns=empty_returns,
            benchmark_returns=empty_returns.clone(),
            turnovers=empty_returns.clone(),
            weights_history=empty_weights,
        )
        return backtest, {}, {}

    model_chunk_rows = max(1, int(model_chunk_rows))
    backtest_chunk_rows = max(1, int(backtest_chunk_rows))
    model_ranges = [(start, min(start + model_chunk_rows, total_rows)) for start in range(0, total_rows, model_chunk_rows)]
    if progress_label:
        _progress(
            f"{progress_label}: start eval rows={total_rows} "
            f"model_chunk_rows={model_chunk_rows} backtest_chunk_rows={backtest_chunk_rows}"
        )

    static_start = time.perf_counter()
    future_returns_all = future_log_returns.to(device=device, non_blocking=non_blocking)
    tradable_mask_all = tradable_mask.to(device=device, non_blocking=non_blocking)
    can_buy_mask_all = can_buy_mask.to(device=device, non_blocking=non_blocking)
    can_sell_mask_all = can_sell_mask.to(device=device, non_blocking=non_blocking)
    benchmark_all = benchmark.to(device=device, non_blocking=non_blocking)
    _maybe_sync_cuda(device, profile_timing)
    timing.transfer_s += time.perf_counter() - static_start

    weights_all: torch.Tensor | None = None
    with torch.inference_mode():
        for chunk_idx, (start, end) in enumerate(model_ranges, start=1):
            _maybe_cudagraph_step_begin()
            log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, len(model_ranges))
            if log_chunk_progress:
                _progress(f"{progress_label}: model chunk {chunk_idx}/{len(model_ranges)} rows=[{start},{end})")
            chunk_start = time.perf_counter()
            x_chunk = x[start:end].to(device=device, non_blocking=non_blocking)
            returns_chunk = future_returns_all[start:end]
            mask_chunk = tradable_mask_all[start:end]
            buy_mask_chunk = can_buy_mask_all[start:end]
            sell_mask_chunk = can_sell_mask_all[start:end]
            bench_chunk = benchmark_all[start:end]
            (
                x_chunk,
                returns_chunk_padded,
                mask_chunk_padded,
                buy_mask_chunk_padded,
                sell_mask_chunk_padded,
                bench_chunk_padded,
                valid_rows,
            ) = _pad_eval_chunk_first_dim(
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                target_rows=model_chunk_rows,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.transfer_s += time.perf_counter() - chunk_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_output_chunk = model(x_chunk, mask_chunk_padded)
                weights_chunk, _ = _extract_weights_and_aux(model_output_chunk)
            _maybe_sync_cuda(device, profile_timing)
            forward_elapsed = time.perf_counter() - forward_start
            timing.forward_s += forward_elapsed
            timing.model_forward_s += forward_elapsed

            if weights_all is None:
                weights_all = torch.empty((total_rows, int(weights_chunk.size(1))), device=device, dtype=weights_chunk.dtype)
            weights_all[start:end].copy_(weights_chunk[:valid_rows])
            del returns_chunk_padded, buy_mask_chunk_padded, sell_mask_chunk_padded, bench_chunk_padded

        if weights_all is None:
            raise RuntimeError("eval produced no model weights")

        backtest, metrics = _run_eval_backtest_from_weight_buffers(
            weights_all,
            future_returns_all,
            tradable_mask_all,
            can_buy_mask_all,
            can_sell_mask_all,
            benchmark_all,
            device=device,
            non_blocking=non_blocking,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            backtest_chunk_rows=backtest_chunk_rows,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing=timing,
            reset_at_rows=reset_at_rows,
        )

        ic_start = time.perf_counter()
        if compute_ic:
            ic_series = compute_ic_series_torch(weights_all, future_returns_all, tradable_mask_all)
            _maybe_sync_cuda(device, profile_timing)
            ic = _finalize_ic_summary_from_series(ic_series, device=device, profile_timing=profile_timing, timing=timing)
        else:
            ic = {}
        timing.ic_s += time.perf_counter() - ic_start

    timing.total_s = time.perf_counter() - overall_start
    if progress_label:
        _progress(f"{progress_label}: eval done total={timing.total_s:.1f}s backtest_chunks={timing.batches}")
    if profile_timing:
        _log_timing("eval.decoupled", timing)
    if timing_out is not None:
        _add_timing(timing_out, timing)
    return backtest, ic, metrics


def _evaluate_tensor_batch(
    model: nn.Module,
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    chunk_rows: int,
    backtest_chunk_rows: int | None = None,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    effective_backtest_chunk_rows = int(backtest_chunk_rows) if backtest_chunk_rows is not None else int(chunk_rows)
    if effective_backtest_chunk_rows != int(chunk_rows):
        return _evaluate_tensor_batch_decoupled(
            model,
            x,
            future_log_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            benchmark,
            device,
            amp_dtype,
            non_blocking,
            long_only,
            buy_fee_rate,
            sell_fee_rate,
            max_turnover_ratio,
            gross_leverage,
            model_chunk_rows=chunk_rows,
            backtest_chunk_rows=effective_backtest_chunk_rows,
            compute_ic=compute_ic,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing_out=timing_out,
            reset_at_rows=reset_at_rows,
        )
    model.eval()
    weights_chunks: list[torch.Tensor] = []
    strategy_chunks: list[torch.Tensor] = []
    benchmark_chunks: list[torch.Tensor] = []
    turnover_chunks: list[torch.Tensor] = []

    n_rows = 0
    sum_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sumsq_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sumsq_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_downside_sq_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_downside_sq_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_turnover_t = torch.zeros((), device=device, dtype=torch.float64)
    hit_count_t = torch.zeros((), device=device, dtype=torch.float64)

    ic_count_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_sum_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_sumsq_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_pos_t = torch.zeros((), device=device, dtype=torch.float64)

    # Online max drawdown in log space.
    cum_log = torch.tensor(0.0, device=device, dtype=torch.float64)
    running_max_log = torch.tensor(0.0, device=device, dtype=torch.float64)
    min_dd = torch.tensor(0.0, device=device, dtype=torch.float64)

    timing = TimingBreakdown()
    overall_start = time.perf_counter()
    total_rows_for_eval = int(x.size(0))
    reset_points = {0, total_rows_for_eval}
    if reset_at_rows is not None:
        reset_points.update(int(value) for value in reset_at_rows if 0 <= int(value) <= total_rows_for_eval)
    reset_points_sorted = sorted(reset_points)
    eval_ranges: list[tuple[int, int, bool]] = []
    for segment_start, segment_end in zip(reset_points_sorted[:-1], reset_points_sorted[1:]):
        if segment_end <= segment_start:
            continue
        for start in range(segment_start, segment_end, chunk_rows):
            end = min(start + chunk_rows, segment_end)
            eval_ranges.append((start, end, start == segment_start))
    total_chunks = max(1, len(eval_ranges))
    if progress_label:
        _progress(f"{progress_label}: start eval rows={int(x.size(0))} chunk_rows={int(chunk_rows)} chunks={total_chunks}")

    with torch.inference_mode():
        prev_weights: torch.Tensor | None = None
        for chunk_idx, (start, end, reset_state) in enumerate(eval_ranges, start=1):
            if reset_state:
                prev_weights = None
            _maybe_cudagraph_step_begin()
            log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, total_chunks)
            if log_chunk_progress:
                _progress(f"{progress_label}: chunk {chunk_idx}/{total_chunks} transfer rows=[{start},{end})")
            chunk_start = time.perf_counter()
            x_chunk = x[start:end].to(device=device, non_blocking=non_blocking)
            returns_chunk = future_log_returns[start:end].to(device=device, non_blocking=non_blocking)
            mask_chunk = tradable_mask[start:end].to(device=device, non_blocking=non_blocking)
            buy_mask_chunk = can_buy_mask[start:end].to(device=device, non_blocking=non_blocking)
            sell_mask_chunk = can_sell_mask[start:end].to(device=device, non_blocking=non_blocking)
            bench_chunk = benchmark[start:end].to(device=device, non_blocking=non_blocking)
            (
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                valid_rows,
            ) = _pad_eval_chunk_first_dim(
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                target_rows=chunk_rows,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.transfer_s += time.perf_counter() - chunk_start

            if log_chunk_progress:
                _progress(f"{progress_label}: chunk {chunk_idx}/{total_chunks} forward model")
            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_output_chunk = model(x_chunk, mask_chunk)
                weights_chunk, _ = _extract_weights_and_aux(model_output_chunk)
            _maybe_sync_cuda(device, profile_timing)
            chunk_forward_elapsed = time.perf_counter() - forward_start
            timing.forward_s += chunk_forward_elapsed
            timing.model_forward_s += chunk_forward_elapsed

            if log_chunk_progress:
                _progress(f"{progress_label}: chunk {chunk_idx}/{total_chunks} backtest")
            backtest_start = time.perf_counter()
            backtest_prepare_start = time.perf_counter()
            initial_weights_chunk = prev_weights
            timing.backtest_prepare_s += time.perf_counter() - backtest_prepare_start

            backtest_runner_start = time.perf_counter()
            backtest_chunk = run_backtest_torch(
                weights_chunk,
                returns_chunk,
                mask_chunk,
                bench_chunk,
                buy_fee_rate,
                sell_fee_rate,
                long_only=long_only,
                max_turnover_ratio=max_turnover_ratio,
                gross_leverage=gross_leverage,
                can_buy_mask=buy_mask_chunk,
                can_sell_mask=sell_mask_chunk,
                return_weights_history=return_weights_history,
                initial_weights=initial_weights_chunk,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.backtest_runner_s += time.perf_counter() - backtest_runner_start

            backtest_finalize_start = time.perf_counter()
            prev_weights = _detach_portfolio_state(backtest_chunk.final_weights)
            # These clones are required because compiled/CUDA-graph backtest
            # outputs can be overwritten by the next replay.
            if return_weights_history:
                weights_chunks.append(backtest_chunk.weights_history[:valid_rows].clone())
            strategy_returns_valid = backtest_chunk.strategy_returns[:valid_rows]
            benchmark_returns_valid = backtest_chunk.benchmark_returns[:valid_rows]
            turnovers_valid = backtest_chunk.turnovers[:valid_rows]
            strategy_chunks.append(strategy_returns_valid.clone())
            benchmark_chunks.append(benchmark_returns_valid.clone())
            turnover_chunks.append(turnovers_valid.clone())
            _maybe_sync_cuda(device, profile_timing)
            timing.backtest_finalize_s += time.perf_counter() - backtest_finalize_start
            timing.backtest_s += time.perf_counter() - backtest_start

            if compute_ic:
                if log_chunk_progress:
                    _progress(f"{progress_label}: chunk {chunk_idx}/{total_chunks} compute IC")
                ic_start = time.perf_counter()
                ic_chunk = compute_ic_series_torch(
                    weights_chunk[:valid_rows],
                    returns_chunk[:valid_rows],
                    mask_chunk[:valid_rows],
                )
                _maybe_sync_cuda(device, profile_timing)
                timing.ic_s += time.perf_counter() - ic_start

            if compute_metrics_summary:
                metrics_start = time.perf_counter()
                r = torch.nan_to_num(strategy_returns_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                b = torch.nan_to_num(benchmark_returns_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                t = torch.nan_to_num(turnovers_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)

                n_rows += int(r.numel())
                sum_r_t = sum_r_t + r.sum()
                sumsq_r_t = sumsq_r_t + (r * r).sum()
                sum_b_t = sum_b_t + b.sum()
                sumsq_b_t = sumsq_b_t + (b * b).sum()
                sum_downside_sq_r_t = sum_downside_sq_r_t + torch.minimum(r, torch.zeros_like(r)).pow(2).sum()
                sum_downside_sq_b_t = sum_downside_sq_b_t + torch.minimum(b, torch.zeros_like(b)).pow(2).sum()
                sum_turnover_t = sum_turnover_t + t.sum()
                hit_count_t = hit_count_t + (r > 0).to(torch.float64).sum()

                cum_log_chunk = torch.cumsum(r, dim=0) + cum_log
                running_max_chunk = torch.maximum(torch.cummax(cum_log_chunk, dim=0).values, running_max_log)
                dd_chunk = torch.expm1(torch.clamp(cum_log_chunk - running_max_chunk, min=-745.0, max=0.0))
                min_dd = torch.minimum(min_dd, dd_chunk.min())
                cum_log = cum_log_chunk[-1]
                running_max_log = running_max_chunk[-1]
                timing.metrics_s += time.perf_counter() - metrics_start

            if compute_ic:
                ic_finite = torch.isfinite(ic_chunk)
                ic_clean64 = torch.nan_to_num(ic_chunk, nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                ic_mask64 = ic_finite.to(torch.float64)
                ic_count_t = ic_count_t + ic_mask64.sum()
                ic_sum_t = ic_sum_t + (ic_clean64 * ic_mask64).sum()
                ic_sumsq_t = ic_sumsq_t + (ic_clean64 * ic_clean64 * ic_mask64).sum()
                ic_pos_t = ic_pos_t + ((ic_clean64 > 0).to(torch.float64) * ic_mask64).sum()
            if log_chunk_progress:
                _progress(
                    f"{progress_label}: chunk {chunk_idx}/{total_chunks} done "
                    f"(transfer={timing.transfer_s:.1f}s forward={timing.forward_s:.1f}s "
                    f"backtest={timing.backtest_s:.1f}s ic={timing.ic_s:.1f}s)"
                )

        concat_start = time.perf_counter()
        if return_weights_history:
            weights = torch.cat(weights_chunks, dim=0)
        else:
            weights = torch.empty(
                (0, int(x.size(2))),
                device=device,
                dtype=strategy_chunks[0].dtype if strategy_chunks else torch.float32,
            )
        strategy_returns = torch.cat(strategy_chunks, dim=0)
        benchmark_returns = torch.cat(benchmark_chunks, dim=0)
        turnovers = torch.cat(turnover_chunks, dim=0)
        backtest = BacktestResultTensor(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns,
            turnovers=turnovers,
            weights_history=weights,
        )
        timing.concat_s += time.perf_counter() - concat_start

        metrics_start = time.perf_counter()
        if not compute_metrics_summary:
            metrics = {}
        elif n_rows <= 0:
            metrics = {
                "cumulative_return": 0.0,
                "annualized_return": 0.0,
                "cagr": 0.0,
                "sharpe": 0.0,
                "baseline_sharpe": 0.0,
                "sortino": 0.0,
                "baseline_sortino": 0.0,
                "max_drawdown": 0.0,
                "calmar": 0.0,
                "turnover": 0.0,
                "daily_hit_rate": 0.0,
                "excess_return_vs_universe_average": 0.0,
                "cumulative_benchmark": 0.0,
            }
        else:
            cpu_gpu_sync_start = time.perf_counter()
            (
                sum_r,
                sumsq_r,
                sum_b,
                sumsq_b,
                sum_downside_sq_r,
                sum_downside_sq_b,
                sum_turnover,
                hit_count,
                min_dd_value,
            ) = (
                torch.stack(
                    [
                        sum_r_t,
                        sumsq_r_t,
                        sum_b_t,
                        sumsq_b_t,
                        sum_downside_sq_r_t,
                        sum_downside_sq_b_t,
                        sum_turnover_t,
                        hit_count_t,
                        min_dd,
                    ]
                )
                .detach()
                .cpu()
                .tolist()
            )
            timing.cpu_gpu_sync_s += time.perf_counter() - cpu_gpu_sync_start
            n = float(n_rows)
            mean_r = sum_r / n
            mean_b = sum_b / n
            var_r = max(0.0, sumsq_r / n - mean_r * mean_r)
            var_b = max(0.0, sumsq_b / n - mean_b * mean_b)
            std_r = var_r ** 0.5
            std_b = var_b ** 0.5
            cum_r = float(math.expm1(sum_r))
            cum_b = float(math.expm1(sum_b))
            ann_r = float(math.expm1(mean_r * 252.0))
            sharpe = float(mean_r / std_r * np.sqrt(252.0)) if std_r > 0 else 0.0
            baseline_sharpe = float(mean_b / std_b * np.sqrt(252.0)) if std_b > 0 else 0.0
            downside_dev = float((sum_downside_sq_r / n) ** 0.5)
            downside_dev_b = float((sum_downside_sq_b / n) ** 0.5)
            sortino = float(mean_r / downside_dev * np.sqrt(252.0)) if downside_dev > 0 else 0.0
            baseline_sortino = float(mean_b / downside_dev_b * np.sqrt(252.0)) if downside_dev_b > 0 else 0.0
            calmar = ann_r / abs(float(min_dd_value)) if float(min_dd_value) < 0.0 else 0.0

            metrics = {
                "cumulative_return": cum_r,
                "annualized_return": ann_r,
                "cagr": ann_r,
                "sharpe": sharpe,
                "baseline_sharpe": baseline_sharpe,
                "sortino": sortino,
                "baseline_sortino": baseline_sortino,
                "max_drawdown": float(min_dd_value),
                "calmar": calmar,
                "turnover": sum_turnover / n,
                "daily_hit_rate": hit_count / n,
                "excess_return_vs_universe_average": cum_r - cum_b,
                "cumulative_benchmark": cum_b,
            }
        timing.metrics_s += time.perf_counter() - metrics_start

        ic_summary_start = time.perf_counter()
        if not compute_ic:
            ic = {}
        else:
            cpu_gpu_sync_start = time.perf_counter()
            ic_count, ic_sum, ic_sumsq, ic_pos = (
                torch.stack([ic_count_t, ic_sum_t, ic_sumsq_t, ic_pos_t])
                .detach()
                .cpu()
                .tolist()
            )
            timing.cpu_gpu_sync_s += time.perf_counter() - cpu_gpu_sync_start
            if ic_count <= 0:
                ic = {"ic_mean": 0.0, "ic_std": 0.0, "ic_ir": 0.0, "ic_positive_ratio": 0.0}
            else:
                ic_n = float(ic_count)
                ic_mean = ic_sum / ic_n
                ic_var = max(0.0, ic_sumsq / ic_n - ic_mean * ic_mean)
                ic_std = (ic_var ** 0.5) + 1e-8
                ic = {
                    "ic_mean": ic_mean,
                    "ic_std": ic_std,
                    "ic_ir": float(ic_mean / ic_std * np.sqrt(252.0)),
                    "ic_positive_ratio": ic_pos / ic_n,
                }
        timing.ic_s += time.perf_counter() - ic_summary_start
    timing.total_s = time.perf_counter() - overall_start
    timing.batches = int(max(1, (x.size(0) + chunk_rows - 1) // chunk_rows))
    if progress_label:
        _progress(f"{progress_label}: eval done total={timing.total_s:.1f}s chunks={timing.batches}")
    if profile_timing:
        _log_timing("eval", timing)
    if timing_out is not None:
        _add_timing(timing_out, timing)
    return backtest, ic, metrics


def _evaluate_windowed_tensor_batch_decoupled(
    model: nn.Module,
    split: WindowedSplitTensors,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    model_chunk_rows: int,
    backtest_chunk_rows: int,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    model.eval()
    total_rows = len(split)
    if total_rows <= 0:
        empty_returns = torch.empty((0,), device=device, dtype=torch.float32)
        empty_weights = torch.empty((0, split.num_symbols), device=device, dtype=torch.float32)
        backtest = BacktestResultTensor(
            strategy_returns=empty_returns,
            benchmark_returns=empty_returns.clone(),
            turnovers=empty_returns.clone(),
            weights_history=empty_weights,
        )
        return backtest, {}, {}

    timing = TimingBreakdown()
    overall_start = time.perf_counter()
    model_chunk_rows = max(1, int(model_chunk_rows))
    backtest_chunk_rows = max(1, int(backtest_chunk_rows))
    model_ranges = [(start, min(start + model_chunk_rows, total_rows)) for start in range(0, total_rows, model_chunk_rows)]
    if progress_label:
        _progress(
            f"{progress_label}: start eval mode=windowed rows={total_rows} "
            f"model_chunk_rows={model_chunk_rows} backtest_chunk_rows={backtest_chunk_rows}"
        )

    weights_all: torch.Tensor | None = None
    returns_all: torch.Tensor | None = None
    mask_all: torch.Tensor | None = None
    buy_mask_all: torch.Tensor | None = None
    sell_mask_all: torch.Tensor | None = None
    benchmark_all: torch.Tensor | None = None

    with torch.inference_mode():
        for chunk_idx, (start, end) in enumerate(model_ranges, start=1):
            _maybe_cudagraph_step_begin()
            log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, len(model_ranges))
            if log_chunk_progress:
                _progress(f"{progress_label}: model chunk {chunk_idx}/{len(model_ranges)} rows=[{start},{end})")
            chunk_start = time.perf_counter()
            batch = split.batch_by_rows(start, end, device=device, non_blocking=non_blocking)
            x_chunk = batch["x"]
            returns_chunk = batch["future_log_returns"]
            mask_chunk = batch["tradable_mask"]
            buy_mask_chunk = batch["can_buy_mask"]
            sell_mask_chunk = batch["can_sell_mask"]
            bench_chunk = batch["benchmark"]
            (
                x_chunk,
                returns_chunk_padded,
                mask_chunk_padded,
                buy_mask_chunk_padded,
                sell_mask_chunk_padded,
                bench_chunk_padded,
                valid_rows,
            ) = _pad_eval_chunk_first_dim(
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                target_rows=model_chunk_rows,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.transfer_s += time.perf_counter() - chunk_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_output_chunk = model(x_chunk, mask_chunk_padded)
                weights_chunk, _ = _extract_weights_and_aux(model_output_chunk)
            _maybe_sync_cuda(device, profile_timing)
            forward_elapsed = time.perf_counter() - forward_start
            timing.forward_s += forward_elapsed
            timing.model_forward_s += forward_elapsed

            if weights_all is None:
                weights_all = torch.empty((total_rows, int(weights_chunk.size(1))), device=device, dtype=weights_chunk.dtype)
                returns_all = torch.empty((total_rows, split.num_symbols), device=device, dtype=returns_chunk.dtype)
                mask_all = torch.empty((total_rows, split.num_symbols), device=device, dtype=mask_chunk.dtype)
                buy_mask_all = torch.empty((total_rows, split.num_symbols), device=device, dtype=buy_mask_chunk.dtype)
                sell_mask_all = torch.empty((total_rows, split.num_symbols), device=device, dtype=sell_mask_chunk.dtype)
                benchmark_all = torch.empty((total_rows,), device=device, dtype=bench_chunk.dtype)
            weights_all[start:end].copy_(weights_chunk[:valid_rows])
            returns_all[start:end].copy_(returns_chunk_padded[:valid_rows])
            mask_all[start:end].copy_(mask_chunk_padded[:valid_rows])
            buy_mask_all[start:end].copy_(buy_mask_chunk_padded[:valid_rows])
            sell_mask_all[start:end].copy_(sell_mask_chunk_padded[:valid_rows])
            benchmark_all[start:end].copy_(bench_chunk_padded[:valid_rows])

        if (
            weights_all is None
            or returns_all is None
            or mask_all is None
            or buy_mask_all is None
            or sell_mask_all is None
            or benchmark_all is None
        ):
            raise RuntimeError("windowed eval produced no buffers")

        backtest, metrics = _run_eval_backtest_from_weight_buffers(
            weights_all,
            returns_all,
            mask_all,
            buy_mask_all,
            sell_mask_all,
            benchmark_all,
            device=device,
            non_blocking=non_blocking,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            backtest_chunk_rows=backtest_chunk_rows,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing=timing,
            reset_at_rows=reset_at_rows,
        )

        ic_start = time.perf_counter()
        if compute_ic:
            ic_series = compute_ic_series_torch(weights_all, returns_all, mask_all)
            _maybe_sync_cuda(device, profile_timing)
            ic = _finalize_ic_summary_from_series(ic_series, device=device, profile_timing=profile_timing, timing=timing)
        else:
            ic = {}
        timing.ic_s += time.perf_counter() - ic_start

    timing.total_s = time.perf_counter() - overall_start
    if progress_label:
        _progress(f"{progress_label}: eval done total={timing.total_s:.1f}s backtest_chunks={timing.batches}")
    if profile_timing:
        _log_timing("eval.windowed.decoupled", timing)
    if timing_out is not None:
        _add_timing(timing_out, timing)
    return backtest, ic, metrics


def _evaluate_windowed_tensor_batch(
    model: nn.Module,
    split: WindowedSplitTensors,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    chunk_rows: int,
    backtest_chunk_rows: int | None = None,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    effective_backtest_chunk_rows = int(backtest_chunk_rows) if backtest_chunk_rows is not None else int(chunk_rows)
    if effective_backtest_chunk_rows != int(chunk_rows):
        return _evaluate_windowed_tensor_batch_decoupled(
            model,
            split,
            device,
            amp_dtype,
            non_blocking,
            long_only,
            buy_fee_rate,
            sell_fee_rate,
            max_turnover_ratio,
            gross_leverage,
            model_chunk_rows=chunk_rows,
            backtest_chunk_rows=effective_backtest_chunk_rows,
            compute_ic=compute_ic,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing_out=timing_out,
            reset_at_rows=reset_at_rows,
        )
    model.eval()
    total_rows_for_eval = len(split)
    if total_rows_for_eval <= 0:
        empty_returns = torch.empty((0,), device=device, dtype=torch.float32)
        empty_weights = torch.empty((0, split.num_symbols), device=device, dtype=torch.float32)
        backtest = BacktestResultTensor(
            strategy_returns=empty_returns,
            benchmark_returns=empty_returns.clone(),
            turnovers=empty_returns.clone(),
            weights_history=empty_weights,
        )
        return backtest, {}, {}

    weights_chunks: list[torch.Tensor] = []
    strategy_chunks: list[torch.Tensor] = []
    benchmark_chunks: list[torch.Tensor] = []
    turnover_chunks: list[torch.Tensor] = []

    n_rows = 0
    sum_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sumsq_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sumsq_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_downside_sq_r_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_downside_sq_b_t = torch.zeros((), device=device, dtype=torch.float64)
    sum_turnover_t = torch.zeros((), device=device, dtype=torch.float64)
    hit_count_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_count_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_sum_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_sumsq_t = torch.zeros((), device=device, dtype=torch.float64)
    ic_pos_t = torch.zeros((), device=device, dtype=torch.float64)
    cum_log = torch.tensor(0.0, device=device, dtype=torch.float64)
    running_max_log = torch.tensor(0.0, device=device, dtype=torch.float64)
    min_dd = torch.tensor(0.0, device=device, dtype=torch.float64)

    timing = TimingBreakdown()
    overall_start = time.perf_counter()
    reset_points = {0, total_rows_for_eval}
    if reset_at_rows is not None:
        reset_points.update(int(value) for value in reset_at_rows if 0 <= int(value) <= total_rows_for_eval)
    reset_points_sorted = sorted(reset_points)
    chunk_rows = max(1, int(chunk_rows))
    eval_ranges: list[tuple[int, int, bool]] = []
    for segment_start, segment_end in zip(reset_points_sorted[:-1], reset_points_sorted[1:]):
        if segment_end <= segment_start:
            continue
        for start in range(segment_start, segment_end, chunk_rows):
            end = min(start + chunk_rows, segment_end)
            eval_ranges.append((start, end, start == segment_start))
    total_chunks = max(1, len(eval_ranges))
    if progress_label:
        _progress(
            f"{progress_label}: start eval mode=windowed rows={total_rows_for_eval} "
            f"chunk_rows={chunk_rows} chunks={total_chunks}"
        )

    with torch.inference_mode():
        prev_weights: torch.Tensor | None = None
        for chunk_idx, (start, end, reset_state) in enumerate(eval_ranges, start=1):
            if reset_state:
                prev_weights = None
            _maybe_cudagraph_step_begin()
            log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, total_chunks)
            chunk_start = time.perf_counter()
            batch = split.batch_by_rows(start, end, device=device, non_blocking=non_blocking)
            x_chunk = batch["x"]
            returns_chunk = batch["future_log_returns"]
            mask_chunk = batch["tradable_mask"]
            buy_mask_chunk = batch["can_buy_mask"]
            sell_mask_chunk = batch["can_sell_mask"]
            bench_chunk = batch["benchmark"]
            (
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                valid_rows,
            ) = _pad_eval_chunk_first_dim(
                x_chunk,
                returns_chunk,
                mask_chunk,
                buy_mask_chunk,
                sell_mask_chunk,
                bench_chunk,
                target_rows=chunk_rows,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.transfer_s += time.perf_counter() - chunk_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_output_chunk = model(x_chunk, mask_chunk)
                weights_chunk, _ = _extract_weights_and_aux(model_output_chunk)
            _maybe_sync_cuda(device, profile_timing)
            chunk_forward_elapsed = time.perf_counter() - forward_start
            timing.forward_s += chunk_forward_elapsed
            timing.model_forward_s += chunk_forward_elapsed

            backtest_start = time.perf_counter()
            backtest_prepare_start = time.perf_counter()
            initial_weights_chunk = prev_weights
            timing.backtest_prepare_s += time.perf_counter() - backtest_prepare_start

            backtest_runner_start = time.perf_counter()
            backtest_chunk = run_backtest_torch(
                weights_chunk,
                returns_chunk,
                mask_chunk,
                bench_chunk,
                buy_fee_rate,
                sell_fee_rate,
                long_only=long_only,
                max_turnover_ratio=max_turnover_ratio,
                gross_leverage=gross_leverage,
                can_buy_mask=buy_mask_chunk,
                can_sell_mask=sell_mask_chunk,
                return_weights_history=return_weights_history,
                initial_weights=initial_weights_chunk,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.backtest_runner_s += time.perf_counter() - backtest_runner_start

            backtest_finalize_start = time.perf_counter()
            prev_weights = _detach_portfolio_state(backtest_chunk.final_weights)
            # Required for compiled/CUDA-graph backtest outputs; the next replay
            # may reuse output storage.
            if return_weights_history:
                weights_chunks.append(backtest_chunk.weights_history[:valid_rows].clone())
            strategy_returns_valid = backtest_chunk.strategy_returns[:valid_rows]
            benchmark_returns_valid = backtest_chunk.benchmark_returns[:valid_rows]
            turnovers_valid = backtest_chunk.turnovers[:valid_rows]
            strategy_chunks.append(strategy_returns_valid.clone())
            benchmark_chunks.append(benchmark_returns_valid.clone())
            turnover_chunks.append(turnovers_valid.clone())
            _maybe_sync_cuda(device, profile_timing)
            timing.backtest_finalize_s += time.perf_counter() - backtest_finalize_start
            timing.backtest_s += time.perf_counter() - backtest_start

            if compute_ic:
                ic_start = time.perf_counter()
                ic_chunk = compute_ic_series_torch(
                    weights_chunk[:valid_rows],
                    returns_chunk[:valid_rows],
                    mask_chunk[:valid_rows],
                )
                _maybe_sync_cuda(device, profile_timing)
                timing.ic_s += time.perf_counter() - ic_start

            if compute_metrics_summary:
                metrics_start = time.perf_counter()
                r = torch.nan_to_num(strategy_returns_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                b = torch.nan_to_num(benchmark_returns_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                t = torch.nan_to_num(turnovers_valid.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                n_rows += int(r.numel())
                sum_r_t = sum_r_t + r.sum()
                sumsq_r_t = sumsq_r_t + (r * r).sum()
                sum_b_t = sum_b_t + b.sum()
                sumsq_b_t = sumsq_b_t + (b * b).sum()
                sum_downside_sq_r_t = sum_downside_sq_r_t + torch.minimum(r, torch.zeros_like(r)).pow(2).sum()
                sum_downside_sq_b_t = sum_downside_sq_b_t + torch.minimum(b, torch.zeros_like(b)).pow(2).sum()
                sum_turnover_t = sum_turnover_t + t.sum()
                hit_count_t = hit_count_t + (r > 0).to(torch.float64).sum()
                cum_log_chunk = torch.cumsum(r, dim=0) + cum_log
                running_max_chunk = torch.maximum(torch.cummax(cum_log_chunk, dim=0).values, running_max_log)
                dd_chunk = torch.expm1(torch.clamp(cum_log_chunk - running_max_chunk, min=-745.0, max=0.0))
                min_dd = torch.minimum(min_dd, dd_chunk.min())
                cum_log = cum_log_chunk[-1]
                running_max_log = running_max_chunk[-1]
                timing.metrics_s += time.perf_counter() - metrics_start

            if compute_ic:
                ic_finite = torch.isfinite(ic_chunk)
                ic_clean64 = torch.nan_to_num(ic_chunk, nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
                ic_mask64 = ic_finite.to(torch.float64)
                ic_count_t = ic_count_t + ic_mask64.sum()
                ic_sum_t = ic_sum_t + (ic_clean64 * ic_mask64).sum()
                ic_sumsq_t = ic_sumsq_t + (ic_clean64 * ic_clean64 * ic_mask64).sum()
                ic_pos_t = ic_pos_t + ((ic_clean64 > 0).to(torch.float64) * ic_mask64).sum()

            if log_chunk_progress:
                _progress(
                    f"{progress_label}: chunk {chunk_idx}/{total_chunks} done "
                    f"(transfer={timing.transfer_s:.1f}s forward={timing.forward_s:.1f}s "
                    f"backtest={timing.backtest_s:.1f}s ic={timing.ic_s:.1f}s)"
                )

        concat_start = time.perf_counter()
        weights = (
            torch.cat(weights_chunks, dim=0)
            if return_weights_history
            else torch.empty(
                (0, split.num_symbols),
                device=device,
                dtype=strategy_chunks[0].dtype if strategy_chunks else torch.float32,
            )
        )
        strategy_returns = torch.cat(strategy_chunks, dim=0)
        benchmark_returns = torch.cat(benchmark_chunks, dim=0)
        turnovers = torch.cat(turnover_chunks, dim=0)
        backtest = BacktestResultTensor(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns,
            turnovers=turnovers,
            weights_history=weights,
        )
        timing.concat_s += time.perf_counter() - concat_start

        metrics_start = time.perf_counter()
        if not compute_metrics_summary:
            metrics = {}
        elif n_rows <= 0:
            metrics = {
                "cumulative_return": 0.0,
                "annualized_return": 0.0,
                "cagr": 0.0,
                "sharpe": 0.0,
                "baseline_sharpe": 0.0,
                "sortino": 0.0,
                "baseline_sortino": 0.0,
                "max_drawdown": 0.0,
                "calmar": 0.0,
                "turnover": 0.0,
                "daily_hit_rate": 0.0,
                "excess_return_vs_universe_average": 0.0,
                "cumulative_benchmark": 0.0,
            }
        else:
            cpu_gpu_sync_start = time.perf_counter()
            values = (
                torch.stack(
                    [
                        sum_r_t,
                        sumsq_r_t,
                        sum_b_t,
                        sumsq_b_t,
                        sum_downside_sq_r_t,
                        sum_downside_sq_b_t,
                        sum_turnover_t,
                        hit_count_t,
                        min_dd,
                    ]
                )
                .detach()
                .cpu()
                .tolist()
            )
            timing.cpu_gpu_sync_s += time.perf_counter() - cpu_gpu_sync_start
            sum_r, sumsq_r, sum_b, sumsq_b, sum_down_r, sum_down_b, sum_turnover, hit_count, min_dd_value = values
            n = float(n_rows)
            mean_r = sum_r / n
            mean_b = sum_b / n
            var_r = max(0.0, sumsq_r / n - mean_r * mean_r)
            var_b = max(0.0, sumsq_b / n - mean_b * mean_b)
            std_r = var_r ** 0.5
            std_b = var_b ** 0.5
            cum_r = float(math.expm1(sum_r))
            cum_b = float(math.expm1(sum_b))
            ann_r = float(math.expm1(mean_r * 252.0))
            downside_dev = float((sum_down_r / n) ** 0.5)
            downside_dev_b = float((sum_down_b / n) ** 0.5)
            metrics = {
                "cumulative_return": cum_r,
                "annualized_return": ann_r,
                "cagr": ann_r,
                "sharpe": float(mean_r / std_r * np.sqrt(252.0)) if std_r > 0 else 0.0,
                "baseline_sharpe": float(mean_b / std_b * np.sqrt(252.0)) if std_b > 0 else 0.0,
                "sortino": float(mean_r / downside_dev * np.sqrt(252.0)) if downside_dev > 0 else 0.0,
                "baseline_sortino": float(mean_b / downside_dev_b * np.sqrt(252.0)) if downside_dev_b > 0 else 0.0,
                "max_drawdown": float(min_dd_value),
                "calmar": ann_r / abs(float(min_dd_value)) if float(min_dd_value) < 0.0 else 0.0,
                "turnover": sum_turnover / n,
                "daily_hit_rate": hit_count / n,
                "excess_return_vs_universe_average": cum_r - cum_b,
                "cumulative_benchmark": cum_b,
            }
        timing.metrics_s += time.perf_counter() - metrics_start

        ic_summary_start = time.perf_counter()
        if not compute_ic:
            ic = {}
        else:
            cpu_gpu_sync_start = time.perf_counter()
            ic_count, ic_sum, ic_sumsq, ic_pos = (
                torch.stack([ic_count_t, ic_sum_t, ic_sumsq_t, ic_pos_t])
                .detach()
                .cpu()
                .tolist()
            )
            timing.cpu_gpu_sync_s += time.perf_counter() - cpu_gpu_sync_start
            if ic_count <= 0:
                ic = {"ic_mean": 0.0, "ic_std": 0.0, "ic_ir": 0.0, "ic_positive_ratio": 0.0}
            else:
                ic_n = float(ic_count)
                ic_mean = ic_sum / ic_n
                ic_var = max(0.0, ic_sumsq / ic_n - ic_mean * ic_mean)
                ic_std = (ic_var ** 0.5) + 1e-8
                ic = {
                    "ic_mean": ic_mean,
                    "ic_std": ic_std,
                    "ic_ir": float(ic_mean / ic_std * np.sqrt(252.0)),
                    "ic_positive_ratio": ic_pos / ic_n,
                }
        timing.ic_s += time.perf_counter() - ic_summary_start

    timing.total_s = time.perf_counter() - overall_start
    timing.batches = int(total_chunks)
    if progress_label:
        _progress(f"{progress_label}: eval done total={timing.total_s:.1f}s chunks={timing.batches}")
    if profile_timing:
        _log_timing("eval.windowed", timing)
    if timing_out is not None:
        _add_timing(timing_out, timing)
    return backtest, ic, metrics


def _auto_chunk_rows(
    model: nn.Module,
    x: torch.Tensor,
    tradable_mask: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    target_vram_fraction: float,
    vram_budget_gb: float,
    vram_safety_margin_gb: float,
    measured_free_bytes: int | None = None,
    max_rows: int | None = None,
    max_chunk_rows: int | None = None,
) -> int:
    total_rows = int(x.size(0)) if max_rows is None else int(max_rows)
    if device.type != "cuda":
        return max(1, min(256, total_rows))

    if total_rows <= 1:
        return 1
    probe_available_rows = max(1, int(x.size(0)))

    torch.cuda.empty_cache()
    free_mem, total_mem = torch.cuda.mem_get_info(device)
    effective_free_mem = int(free_mem if measured_free_bytes is None else min(int(free_mem), int(measured_free_bytes)))
    reserved_mem = torch.cuda.memory_reserved(device)

    if measured_free_bytes is not None:
        # Caller already computed the remaining VRAM budget (for example: 16GB - train_batch_used).
        usable_pool_bytes = min(effective_free_mem, int(total_mem))
    else:
        budget_bytes = int(max(0.0, vram_budget_gb - vram_safety_margin_gb) * (1024 ** 3))
        # Respect both: per-process budget headroom and global free VRAM on the device.
        process_remaining = max(0, budget_bytes - int(reserved_mem))
        usable_pool_bytes = min(process_remaining, effective_free_mem, int(total_mem))

    # Measure per-row incremental VRAM cost on a tiny probe, then size chunk
    # from currently free VRAM. This avoids OOM-driven probing.
    probe_rows = max(1, min(32, probe_available_rows))
    _progress(f"[eval chunk auto] probing rows={probe_rows}/{total_rows} to estimate eval VRAM")
    torch.cuda.reset_peak_memory_stats(device)
    base_alloc = torch.cuda.memory_allocated(device)
    with torch.inference_mode():
        x_probe = x[:probe_rows].to(device=device, non_blocking=True)
        mask_probe = tradable_mask[:probe_rows].to(device=device, non_blocking=True)
        with _autocast_context(device, amp_dtype):
            probe_output = model(x_probe, mask_probe)
            _, _ = _extract_weights_and_aux(probe_output)
    torch.cuda.synchronize(device)
    peak_alloc = torch.cuda.max_memory_allocated(device)

    incremental_bytes = max(1, peak_alloc - base_alloc)
    bytes_per_row = max(1, incremental_bytes // probe_rows)

    # Keep headroom for allocator/workspace fluctuations.
    usable_bytes = int(usable_pool_bytes * target_vram_fraction * 0.9)
    estimated_rows = usable_bytes // bytes_per_row
    chunk_rows = _estimate_eval_chunk_rows(
        total_rows=total_rows,
        estimated_rows=int(estimated_rows),
        max_chunk_rows=max_chunk_rows,
    )
    cap_label = "" if max_chunk_rows is None or int(max_chunk_rows) <= 0 else f" cap={int(max_chunk_rows)}"
    _progress(
        f"[eval chunk auto] peak_increment={incremental_bytes/1024**2:.1f}MiB "
        f"bytes_per_row={bytes_per_row/1024**2:.2f}MiB usable={usable_bytes/1024**3:.2f}GB "
        f"chunk_rows={chunk_rows}{cap_label}"
    )

    return chunk_rows


def _estimate_eval_chunk_rows(
    *,
    total_rows: int,
    estimated_rows: int,
    max_chunk_rows: int | None = None,
) -> int:
    upper = int(total_rows)
    if max_chunk_rows is not None and int(max_chunk_rows) > 0:
        upper = min(upper, int(max_chunk_rows))
    return max(1, min(upper, int(estimated_rows)))


def _refresh_walkforward_artifacts(output_path: Path, results: list[FoldResult]) -> None:
    _write_summary(results, output_path)

    stale_combined_log_plot = output_path / "walkforward_equity_curve_log.png"
    if stale_combined_log_plot.exists():
        stale_combined_log_plot.unlink()
    stale_first_test_year_only_plot = output_path / "walkforward_first_test_year_only.png"
    if stale_first_test_year_only_plot.exists():
        stale_first_test_year_only_plot.unlink()

    all_strategy_returns: list[np.ndarray] = []
    all_benchmark_returns: list[np.ndarray] = []
    all_turnovers: list[np.ndarray] = []
    all_weights: list[np.ndarray] = []
    all_dates: list[np.ndarray] = []
    all_first_year_fold_ids: list[int] = []
    all_first_year_dates: list[np.ndarray] = []
    all_first_year_strategy_log: list[np.ndarray] = []
    all_first_year_baseline_log: list[np.ndarray] = []
    all_first_year_turnovers: list[np.ndarray] = []
    all_first_year_weights: list[np.ndarray] = []

    for result in sorted(results, key=lambda item: item.fold_id):
        fold_dir = _fold_dir(output_path, result.fold_id)
        backtest_path = _backtest_path(fold_dir)
        if not backtest_path.exists():
            continue
        fold_backtest, fold_dates = _load_backtest_artifact(backtest_path)
        all_strategy_returns.append(fold_backtest.strategy_returns)
        all_benchmark_returns.append(fold_backtest.benchmark_returns)
        all_turnovers.append(fold_backtest.turnovers)
        all_weights.append(fold_backtest.weights_history)
        all_dates.append(fold_dates)

        years = np.asarray(fold_dates, dtype="datetime64[D]").astype(object)
        years = np.array([d.year for d in years])
        if years.size > 0:
            first_year = int(np.min(years))
            mask = years == first_year
            all_first_year_fold_ids.append(int(result.fold_id))
            all_first_year_dates.append(fold_dates[mask])
            all_first_year_strategy_log.append(np.nan_to_num(fold_backtest.strategy_returns[mask], nan=0.0).astype(np.float64))
            all_first_year_baseline_log.append(np.nan_to_num(fold_backtest.benchmark_returns[mask], nan=0.0).astype(np.float64))
            all_first_year_turnovers.append(np.nan_to_num(fold_backtest.turnovers[mask], nan=0.0).astype(np.float64))
            all_first_year_weights.append(np.nan_to_num(fold_backtest.weights_history[mask], nan=0.0).astype(np.float64))

    if not all_dates:
        return

    if all_first_year_dates:
        plot_fold_first_year_returns(
            all_first_year_dates,
            all_first_year_strategy_log,
            all_first_year_baseline_log,
            output_path / "walkforward_first_year_cumulative_returns.png",
        )
        plot_first_year_fold_metric_bars(
            all_first_year_fold_ids,
            all_first_year_strategy_log,
            all_first_year_baseline_log,
            output_path / "walkforward_first_year_fold_metrics.png",
        )
        plot_first_year_turnover_concentration(
            all_first_year_fold_ids,
            all_first_year_turnovers,
            all_first_year_weights,
            output_path / "walkforward_first_year_turnover_concentration.png",
        )


def _run_fold_explainability(
    *,
    model: nn.Module,
    panel: PanelData,
    config: ExperimentConfig,
    output_path: Path,
    fold: WalkForwardFold,
    device: torch.device,
    checkpoint_path: Path,
) -> Path | None:
    if not bool(getattr(config.training, "explain_after_each_fold", False)):
        return None

    from stockagent.explainability import (
        ExplainabilitySettings,
        _first_year_indices,
        _sample_dataset,
        explain_batch,
        write_explanation_outputs,
    )

    test_indices = fold.test_indices
    first_year_only = bool(getattr(config.training, "explain_first_test_year_only", True))
    if first_year_only:
        test_indices = _first_year_indices(panel, test_indices)
    dataset = CrossSectionalDataset(panel, test_indices, config.training.lookback)
    settings = ExplainabilitySettings(
        top_k=int(getattr(config.training, "explain_top_k", 20)),
        max_rows=int(getattr(config.training, "explain_max_rows", 32)),
        ig_steps=int(getattr(config.training, "explain_ig_steps", 8)),
        perturb=bool(getattr(config.training, "explain_perturb", True)),
        sample_method=str(getattr(config.training, "explain_sample_method", "even")),
        first_test_year_only=first_year_only,
        report_style=str(getattr(config.training, "explain_report_style", "paper")),
        plot_theme=str(getattr(config.training, "explain_plot_theme", "paper")),
        interactive_plots=bool(getattr(config.training, "explain_interactive_plots", False)),
        shap_enabled=bool(getattr(config.training, "explain_shap_enabled", True)),
        shap_mode=str(getattr(config.training, "explain_shap_mode", "score_head_surrogate")),
        case_study_top_k=int(getattr(config.training, "explain_case_study_top_k", 5)),
        regime_analysis=bool(getattr(config.training, "explain_regime_analysis", True)),
        fold_stability=bool(getattr(config.training, "explain_fold_stability", True)),
        umap_enabled=bool(getattr(config.training, "explain_umap_enabled", True)),
        umap_max_points=int(getattr(config.training, "explain_umap_max_points", 10000)),
        umap_n_neighbors=int(getattr(config.training, "explain_umap_n_neighbors", 15)),
        umap_min_dist=float(getattr(config.training, "explain_umap_min_dist", 0.1)),
    )
    batch, date_indices = _sample_dataset(dataset, settings.max_rows, settings.sample_method)
    dates = [str(np.datetime_as_string(panel.dates[int(idx)], unit="D")) for idx in date_indices]
    was_training = model.training
    model.eval()
    try:
        result = explain_batch(
            model,
            batch,
            feature_names=panel.feature_names,
            symbols=panel.symbols,
            dates=dates,
            settings=settings,
            device=device,
        )
    finally:
        if was_training:
            model.train()

    destination = output_path / "explainability" / f"fold_{int(fold.fold_id):02d}_test"
    metadata = {
        "model_name": config.training.model_name,
        "fold_id": int(fold.fold_id),
        "split": "test",
        "checkpoint": str(checkpoint_path),
        "device": str(device),
        "sample_rows": int(len(dates)),
        "first_test_year_only": first_year_only,
        "config_lookback": int(config.training.lookback),
        "date_start": dates[0] if dates else None,
        "date_end": dates[-1] if dates else None,
    }
    write_explanation_outputs(
        result,
        destination,
        metadata=metadata,
        write_plots=bool(getattr(config.training, "explain_write_plots", True)),
        plot_backend=str(getattr(config.training, "plot_backend", "auto")),
        report_style=str(getattr(config.training, "explain_report_style", "paper")),
        plot_theme=str(getattr(config.training, "explain_plot_theme", "paper")),
    )
    if bool(getattr(config.training, "explain_fold_stability", True)):
        try:
            from stockagent.explainability import write_fold_stability_outputs

            write_fold_stability_outputs(output_path / "explainability")
        except Exception as exc:
            print(f"[Fold {fold.fold_id}] fold stability explainability skipped: {type(exc).__name__}: {exc}")
    if device.type == "cuda":
        torch.cuda.empty_cache()
    return destination


def _load_completed_fold_result(output_path: Path, fold_id: int) -> FoldResult | None:
    fold_dir = _fold_dir(output_path, fold_id)
    metrics_path = _metrics_path(fold_dir)
    model_path = _model_path(fold_dir)
    backtest_path = _backtest_path(fold_dir)
    if metrics_path.exists() and model_path.exists() and backtest_path.exists():
        return _load_fold_result(metrics_path)
    return None




def _resolve_device(config: ExperimentConfig) -> torch.device:
    requested = config.environment.device
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA was requested in config (environment.device=cuda), "
            "but torch.cuda.is_available() is False. "
            "Training is aborted to avoid silently falling back to CPU."
        )
    return torch.device(requested)


def _resolve_amp_dtype(amp_dtype: str) -> torch.dtype | None:
    if amp_dtype == "bf16":
        return torch.bfloat16
    if amp_dtype == "fp16":
        return torch.float16
    if amp_dtype == "tf32":
        return None
    raise ValueError(f"Unsupported amp dtype: {amp_dtype}")


def _autocast_context(device: torch.device, amp_dtype: torch.dtype | None):
    if device.type != "cuda" or amp_dtype is None:
        return nullcontext()
    return autocast(device_type="cuda", enabled=True, dtype=amp_dtype)


def _is_compile_backward_shape_error(exc: RuntimeError) -> bool:
    msg = str(exc)
    return (
        "CompiledFunctionBackward returned an invalid gradient" in msg
        and "expected shape compatible" in msg
    )


def _resolve_host_compilers() -> tuple[str | None, str | None]:
    env_bin = Path(sys.executable).resolve().parent
    cc_candidates = [
        os.environ.get("CC"),
        str(env_bin / "x86_64-conda-linux-gnu-gcc"),
        str(env_bin / "gcc"),
        str(env_bin / "cc"),
        "cc",
        "gcc",
        "clang",
        "x86_64-conda-linux-gnu-gcc",
        "x86_64-conda-linux-gnu-cc",
    ]
    cxx_candidates = [
        os.environ.get("CXX"),
        str(env_bin / "x86_64-conda-linux-gnu-g++"),
        str(env_bin / "g++"),
        str(env_bin / "c++"),
        "c++",
        "g++",
        "clang++",
        "x86_64-conda-linux-gnu-g++",
        "x86_64-conda-linux-gnu-c++",
    ]

    def _resolve(candidates: list[str | None]) -> str | None:
        for candidate in candidates:
            if not candidate:
                continue
            resolved = candidate if os.path.isabs(candidate) else shutil.which(candidate)
            if resolved:
                return resolved
        return None

    return _resolve(cc_candidates), _resolve(cxx_candidates)


def _prepend_compile_toolchain_paths() -> None:
    entries: list[str] = []
    env_bin = Path(sys.executable).resolve().parent
    env_root = env_bin.parent
    os.environ.setdefault("CONDA_PREFIX", str(env_root))
    ptxas_path = env_bin / "ptxas"
    if ptxas_path.exists():
        os.environ.setdefault("TRITON_PTXAS_PATH", str(ptxas_path))
        os.environ.setdefault("TRITON_PTXAS_BLACKWELL_PATH", str(ptxas_path))
    entries.append(str(env_bin))
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        entries.append(str(Path(conda_prefix) / "bin"))
    try:
        import site

        for site_dir in site.getsitepackages():
            entries.append(str(Path(site_dir) / "nvidia" / "cuda_nvcc" / "bin"))
    except Exception:
        pass

    existing = os.environ.get("PATH", "")
    existing_parts = [part for part in existing.split(os.pathsep) if part]
    prepend = [part for part in entries if part and Path(part).exists() and part not in existing_parts]
    if prepend:
        os.environ["PATH"] = os.pathsep.join([*prepend, existing])


def _can_enable_torch_compile(device: torch.device) -> tuple[bool, str]:
    """Return whether torch.compile is safe to enable in current environment."""
    if device.type != "cuda":
        return False, "torch.compile is only enabled for CUDA in this project"

    _prepend_compile_toolchain_paths()

    # Inductor+Triton on CUDA needs a host C compiler at runtime.
    cc, cxx = _resolve_host_compilers()
    if not cc or not cxx:
        return False, "no host C/C++ compiler found (set CC/CXX or install gcc/clang)"

    os.environ.setdefault("CC", cc)
    os.environ.setdefault("CXX", cxx)
    ptxas = shutil.which("ptxas")
    if not ptxas:
        return False, "no CUDA ptxas found (install cuda-nvcc/cuda-nvvm-tools in the active env)"

    return True, f"CC={cc}, CXX={cxx}, ptxas={ptxas}"


def _configure_backtest_runtime_from_config(config: ExperimentConfig) -> None:
    training = config.training
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if bool(training.backtest_autotune) else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if bool(training.backtest_compile) else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE_STATEFUL"] = (
        "1" if bool(training.backtest_compile_stateful) else "0"
    )
    os.environ["STOCKAGENT_USE_CPP_BACKTEST_EXT"] = "1" if bool(training.backtest_cpp_ext) else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "1" if bool(training.backtest_verbose) else "0"
    os.environ["STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS"] = str(
        max(0, int(training.backtest_checkpoint_chunk_rows))
    )


def _query_nvidia_smi_free_bytes(device_index: int) -> tuple[int, int, int] | None:
    """Return (total_bytes, used_bytes, free_bytes) from nvidia-smi for one GPU."""
    try:
        proc = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,memory.total,memory.used,memory.free",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None

    for raw in proc.stdout.strip().splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 4:
            continue
        try:
            idx = int(parts[0])
            total_mb = int(parts[1])
            used_mb = int(parts[2])
            free_mb = int(parts[3])
        except ValueError:
            continue
        if idx == device_index:
            mib = 1024**2
            return total_mb * mib, used_mb * mib, free_mb * mib
    return None


def _release_cuda_memory(device: torch.device) -> None:
    if device.type != "cuda":
        return
    try:
        torch.cuda.synchronize(device)
    except Exception:
        pass
    gc.collect()
    torch.cuda.empty_cache()
    try:
        torch.cuda.ipc_collect()
    except Exception:
        pass


def find_optimal_batch_size(
    model: nn.Module,
    sample_loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    long_only: bool,
    target_vram_fraction: float = 0.85,
    vram_budget_gb: float = 12.0,
    vram_safety_margin_gb: float = 1.0,
) -> tuple[int, int]:
    """
    ✅ Binary search to find maximum safe batch size.
    
    Args:
        model: Model to test
        sample_loader: DataLoader with samples
        device: GPU device
        amp_dtype: Mixed precision dtype
        target_vram_fraction: Target VRAM utilization (0.85 = 85%)
        vram_budget_gb: Total VRAM budget in GB
        vram_safety_margin_gb: Reserved headroom in GB to avoid OOM due allocator/workspace spikes
    
    Returns:
        (maximum safe batch size, measured used bytes at that batch size)
    """
    if device.type != 'cuda':
        return len(sample_loader.dataset), 0

    device_index = device.index if device.index is not None else torch.cuda.current_device()
    max_batch_size = len(sample_loader.dataset)
    margin_bytes = int(max(0.0, vram_safety_margin_gb) * 1024**3)
    budget_bytes = int(max(0.0, vram_budget_gb) * 1024**3)

    smi = _query_nvidia_smi_free_bytes(device_index)
    if smi is not None:
        smi_total, smi_used, smi_free = smi
        hard_cap_bytes = max(1, min(budget_bytes, smi_total) - margin_bytes)
        # Use actual global free VRAM from nvidia-smi, then keep safety margin.
        free_after_margin = max(0, smi_free - margin_bytes)
        free_cap_bytes = int(free_after_margin * target_vram_fraction)
        target_bytes = min(hard_cap_bytes, free_cap_bytes)
        mem_source = (
            f"nvidia-smi total={smi_total/1024**3:.1f}GB "
            f"used={smi_used/1024**3:.1f}GB free={smi_free/1024**3:.1f}GB"
        )
    else:
        free_mem, total_mem = torch.cuda.mem_get_info(device)
        hard_cap_bytes = max(1, min(budget_bytes, int(total_mem)) - margin_bytes)
        free_after_margin = max(0, int(free_mem) - margin_bytes)
        free_cap_bytes = int(free_after_margin * target_vram_fraction)
        target_bytes = min(hard_cap_bytes, free_cap_bytes)
        mem_source = (
            f"torch.mem_get_info total={total_mem/1024**3:.1f}GB "
            f"free={free_mem/1024**3:.1f}GB"
        )

    target_bytes = max(1, target_bytes)
    best_batch_size = 1
    best_used_bytes = 0

    # Include optimizer-state allocation (Adam moments) in peak memory estimation.
    temp_optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)

    def _measure_candidate(batch_size: int) -> tuple[bool, int]:
        torch.cuda.reset_peak_memory_stats()
        torch.cuda.empty_cache()
        baseline_reserved = torch.cuda.memory_reserved(device)

        temp_loader = DataLoader(
            sample_loader.dataset,
            batch_size=batch_size,
            shuffle=False,
            num_workers=0,
            pin_memory=True,
        )

        model.train()
        local_batch = next(iter(temp_loader))
        local_batch = _move_batch(local_batch, device, non_blocking=True)
        model.zero_grad(set_to_none=True)

        try:
            with _autocast_context(device, amp_dtype):
                model_output = model(local_batch["x"], local_batch["tradable_mask"])
                logits, _ = _extract_weights_and_aux(model_output)
                loss = risk_aware_loss(
                    logits,
                    local_batch["future_log_returns"],
                    local_batch["tradable_mask"],
                    can_buy_mask=local_batch["can_buy_mask"],
                    can_sell_mask=local_batch["can_sell_mask"],
                    long_only=long_only,
                    buy_fee_rate=0.0,
                    sell_fee_rate=0.0,
                    max_turnover_ratio=0.0,
                    objective="sharpe",
                )

            loss.backward()
            temp_optimizer.step()
            temp_optimizer.zero_grad(set_to_none=True)
            peak_reserved = torch.cuda.max_memory_reserved()
            # Compare candidate-induced memory increase against target_bytes.
            # target_bytes represents usable headroom, not absolute global used VRAM.
            used_memory = max(0, int(peak_reserved) - int(baseline_reserved))

            ok = used_memory <= target_bytes
            return ok, int(used_memory)
        finally:
            model.zero_grad(set_to_none=True)
            temp_optimizer.zero_grad(set_to_none=True)
            del local_batch
            if 'logits' in locals():
                del logits
            if 'loss' in locals():
                del loss
            torch.cuda.empty_cache()

    # Bracket search by actual runtime usage instead of single-sample reserved memory.
    low = 1
    high = 1
    ok, used_bytes = _measure_candidate(1)
    if not ok:
        print(
            f"  [batch search] target: {target_bytes/1024**3:.1f}GB "
            f"(hard_cap={hard_cap_bytes/1024**3:.1f}GB, budget={vram_budget_gb:.1f}GB, margin={vram_safety_margin_gb:.1f}GB, frac={target_vram_fraction:.2f}, {mem_source})"
        )
        print(f"  ❌ batch_size 1: {used_bytes/1024**3:.1f}GB exceeds")
        return 1, int(used_bytes)

    best_batch_size = 1
    best_used_bytes = int(used_bytes)
    print(
        f"  [batch search] target: {target_bytes/1024**3:.1f}GB "
        f"(hard_cap={hard_cap_bytes/1024**3:.1f}GB, budget={vram_budget_gb:.1f}GB, margin={vram_safety_margin_gb:.1f}GB, frac={target_vram_fraction:.2f}, {mem_source})"
    )
    print(f"  ✅ batch_size 1: {used_bytes/1024**3:.1f}GB OK")

    while high < max_batch_size:
        candidate = min(max_batch_size, high * 2)
        ok, used_bytes = _measure_candidate(candidate)
        if ok:
            best_batch_size = candidate
            best_used_bytes = int(used_bytes)
            low = candidate
            high = candidate
            print(f"  ✅ batch_size {candidate}: {used_bytes/1024**3:.1f}GB OK")
            if candidate == max_batch_size:
                print(f"  [batch search] final result: {best_batch_size}")
                return best_batch_size, best_used_bytes
            continue

        low = max(1, high)
        high = candidate
        print(f"  ❌ batch_size {candidate}: {used_bytes/1024**3:.1f}GB exceeds")
        break

    if high == low and best_batch_size == max_batch_size:
        print(f"  [batch search] final result: {best_batch_size}")
        return best_batch_size, best_used_bytes
    
    search_low = best_batch_size + 1
    search_high = high - 1 if high > best_batch_size else best_batch_size
    print(f"  [batch search] refine range: [{search_low}, {search_high}]")

    while search_low <= search_high:
        mid = (search_low + search_high) // 2

        try:
            ok, used_memory = _measure_candidate(mid)
            if used_memory <= target_bytes:
                best_batch_size = mid
                best_used_bytes = int(used_memory)
                search_low = mid + 1
                print(f"  ✅ batch_size {mid}: {used_memory/1024**3:.1f}GB OK")
            else:
                search_high = mid - 1
                print(f"  ❌ batch_size {mid}: {used_memory/1024**3:.1f}GB exceeds")

        except RuntimeError as e:
            if "out of memory" in str(e).lower():
                search_high = mid - 1
                print(f"  ❌ batch_size {mid}: OOM")
            else:
                raise
    
    print(f"  [batch search] final result: {best_batch_size}")
    return best_batch_size, best_used_bytes


def _build_loader(
    dataset: CrossSectionalDataset,
    batch_size: int,
    shuffle: bool,
    config: ExperimentConfig,
    device: torch.device,
    drop_last: bool = False,
) -> DataLoader:
    workers = max(0, int(config.training.num_workers))
    loader_kwargs: dict = {
        "dataset": dataset,
        "batch_size": batch_size,
        "shuffle": shuffle,
        "num_workers": workers,
        "pin_memory": (device.type == "cuda"),
        "drop_last": drop_last,
        "collate_fn": partial(collate_batch, batch_size=batch_size),
    }
    if workers > 0:
        loader_kwargs["persistent_workers"] = True
        loader_kwargs["prefetch_factor"] = 4
    return DataLoader(
        **loader_kwargs,
    )


def _move_batch(batch: dict[str, torch.Tensor], device: torch.device, non_blocking: bool) -> dict[str, torch.Tensor]:
    return {key: value.to(device=device, non_blocking=non_blocking) for key, value in batch.items()}


def _amp_bytes(amp_dtype: torch.dtype | None) -> int:
    if amp_dtype in (torch.float16, torch.bfloat16):
        return 2
    return 4


def _estimate_model_static_bytes(model: nn.Module, training_mode: bool) -> int:
    param_count = sum(param.numel() for param in model.parameters())
    if training_mode:
        # fp32 weights + fp32 grads + Adam m/v states.
        return int(param_count * (4 + 4 + 8))
    # Inference keeps only weights.
    return int(param_count * 4)


def _estimate_sample_bytes(
    *,
    lookback: int,
    num_symbols: int,
    num_features: int,
    hidden_dim: int,
    amp_dtype: torch.dtype | None,
    training_mode: bool,
) -> int:
    input_dim = lookback * num_features
    fp32_bytes = 4
    amp_bytes = _amp_bytes(amp_dtype)

    # Batch tensors moved to GPU each step.
    input_bytes = lookback * num_symbols * num_features * fp32_bytes
    target_bytes = num_symbols * fp32_bytes
    tradable_mask_bytes = num_symbols  # bool tensor
    benchmark_bytes = fp32_bytes

    # Approximate forward activations per sample for MLP.
    activation_elements = num_symbols * (input_dim + hidden_dim + hidden_dim + 1)
    if training_mode:
        # Save activations for backward plus temporary gradients/workspace.
        activation_bytes = int(activation_elements * amp_bytes * 6)
    else:
        activation_bytes = int(activation_elements * amp_bytes * 2)

    return int(input_bytes + target_bytes + tradable_mask_bytes + benchmark_bytes + activation_bytes)


def _estimate_cstpm_sample_bytes(config: ExperimentConfig, panel: PanelData, amp_dtype: torch.dtype | None) -> tuple[int, str]:
    cstpm = config.training.cross_sectional_temporal_portfolio_model
    lookback = int(config.training.lookback)
    num_symbols = int(panel.num_symbols)
    num_features = int(len(panel.feature_names))
    candidate_top_m = min(max(1, int(getattr(cstpm, "candidate_k", getattr(cstpm, "candidate_top_m", 64)))), num_symbols)
    scorer_hidden = int(getattr(cstpm, "scorer_hidden", cstpm.stock_hidden_dim))
    scorer_blocks = int(getattr(cstpm, "scorer_blocks", cstpm.stock_n_blocks))
    d_model = int(getattr(cstpm, "d_model", cstpm.cross_hidden_dim))
    heads = int(getattr(cstpm, "heads", cstpm.cross_heads))
    layers = int(getattr(cstpm, "layers", cstpm.cross_layers))
    amp_bytes = _amp_bytes(amp_dtype)
    fp32_bytes = 4

    input_bytes = lookback * num_symbols * num_features * fp32_bytes
    target_bytes = num_symbols * fp32_bytes
    mask_bytes = num_symbols * 3
    benchmark_bytes = fp32_bytes

    # New CSTPM runs an all-stock tabular ResNet scorer, then limits cross-asset
    # attention to hard candidates. Memory is therefore O(S) for the scorer plus
    # O(M^2) for the reranker, where M=candidate_top_m.
    stock_hidden_slots = 12 + 8 * max(1, scorer_blocks)
    stock_hidden_bytes = (
        num_symbols
        * scorer_hidden
        * amp_bytes
        * stock_hidden_slots
    )

    stock_embedding_bytes = num_symbols * d_model * amp_bytes * 6
    candidate_bytes = candidate_top_m * (d_model + d_model) * amp_bytes * 8
    cross_bytes = candidate_top_m * d_model * amp_bytes * 12
    cross_attn_bytes = max(1, layers) * max(1, heads) * candidate_top_m * candidate_top_m * amp_bytes * 4

    total = int(
        input_bytes
        + target_bytes
        + mask_bytes
        + benchmark_bytes
        + stock_hidden_bytes
        + stock_embedding_bytes
        + candidate_bytes
        + cross_bytes
        + cross_attn_bytes
    )
    detail = (
        f"cstpm_formula input={input_bytes/1024**2:.1f}MiB "
        f"stock_hidden={stock_hidden_bytes/1024**3:.2f}GiB "
        f"candidate={candidate_bytes/1024**2:.1f}MiB "
        f"cross={cross_bytes/1024**3:.2f}GiB "
        f"attn={cross_attn_bytes/1024**2:.1f}MiB "
        f"slots={stock_hidden_slots} candidates={candidate_top_m}"
    )
    return total, detail


def _budget_batch_size(
    *,
    dataset_size: int,
    requested_cap: int,
    budget_bytes: int,
    static_bytes: int,
    sample_bytes: int,
    min_batch_size: int,
) -> int:
    available_bytes = max(0, budget_bytes - static_bytes)
    if sample_bytes <= 0:
        by_budget = 1
    else:
        by_budget = max(1, available_bytes // sample_bytes)
    return max(1, min(dataset_size, requested_cap, max(min_batch_size, int(by_budget))))


def _usable_vram_bytes(
    *,
    device: torch.device,
    budget_gb: float,
    safety_margin_gb: float,
    target_fraction: float,
) -> tuple[int, str]:
    margin_bytes = int(max(0.0, safety_margin_gb) * 1024**3)
    budget_bytes = int(max(0.0, budget_gb) * 1024**3)
    device_index = device.index if device.index is not None else torch.cuda.current_device()

    smi = _query_nvidia_smi_free_bytes(device_index)
    if smi is not None:
        smi_total, smi_used, smi_free = smi
        hard_cap_bytes = max(1, min(budget_bytes, smi_total) - margin_bytes)
        free_cap_bytes = int(max(0, smi_free - margin_bytes) * float(target_fraction))
        usable = max(1, min(hard_cap_bytes, free_cap_bytes))
        source = (
            f"nvidia-smi total={smi_total/1024**3:.1f}GB "
            f"used={smi_used/1024**3:.1f}GB free={smi_free/1024**3:.1f}GB"
        )
    else:
        free_mem, total_mem = torch.cuda.mem_get_info(device)
        hard_cap_bytes = max(1, min(budget_bytes, int(total_mem)) - margin_bytes)
        free_cap_bytes = int(max(0, int(free_mem) - margin_bytes) * float(target_fraction))
        usable = max(1, min(hard_cap_bytes, free_cap_bytes))
        source = f"torch.mem_get_info total={total_mem/1024**3:.1f}GB free={free_mem/1024**3:.1f}GB"

    return usable, source


def _split_batch_size(dataset_size: int, cap: int) -> int:
    return max(1, min(cap, dataset_size))


def _train_epoch(
    model: nn.Module,
    loss_fn: Callable[..., torch.Tensor],
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str,
    grad_clip_norm: float,
    finite_check_interval_steps: int = 0,
    rank_ic_weight: float = 0.20,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    factor_aug_kwargs: dict[str, float] | None = None,
    profile_timing: bool = False,
    progress_label: str | None = None,
) -> tuple[torch.Tensor, TimingBreakdown]:
    model.train()
    total_loss_t = torch.zeros((), device=device, dtype=torch.float32)
    steps = 0
    sequential_return_objective = _is_return_series_objective(objective)
    portfolio_prev_weights: torch.Tensor | None = None

    timing = TimingBreakdown()
    total_start = time.perf_counter()
    loader_iter = iter(loader)
    total_batches = int(len(loader)) if hasattr(loader, "__len__") else 0
    if progress_label:
        _progress(f"{progress_label}: train epoch start mode=dataloader batches={total_batches}")

    while True:
        fetch_start = time.perf_counter()
        try:
            batch = next(loader_iter)
        except StopIteration:
            break
        batch_start = time.perf_counter()
        batch_no = steps + 1
        if progress_label:
            suffix = f"/{total_batches}" if total_batches else ""
            _progress(f"{progress_label}: batch {batch_no}{suffix} fetched; transfer to {device}")
        _maybe_sync_cuda(device, profile_timing)
        timing.fetch_s += time.perf_counter() - fetch_start

        transfer_start = time.perf_counter()
        batch = _move_batch(batch, device, non_blocking)
        _maybe_sync_cuda(device, profile_timing)
        timing.transfer_s += time.perf_counter() - transfer_start

        _maybe_cudagraph_step_begin()
        optimizer.zero_grad(set_to_none=True)

        if progress_label:
            _progress(f"{progress_label}: batch {batch_no}{suffix} forward + {objective} loss")
        forward_start = time.perf_counter()
        with _autocast_context(device, amp_dtype):
            model_forward_start = time.perf_counter()
            with _cuda_timing(timing, "model_forward_cuda_s", device):
                model_output = model(batch["x"], batch["tradable_mask"])
                weights, aux_outputs = _extract_weights_and_aux(model_output)
            _maybe_sync_cuda(device, profile_timing)
            timing.model_forward_s += time.perf_counter() - model_forward_start

            factor_aug_start = time.perf_counter()
            with _cuda_timing(timing, "factor_aug_cuda_s", device):
                aux_outputs = _attach_factor_augmented_scores(
                    model=model,
                    aux_outputs=aux_outputs,
                    x=batch["x"],
                    tradable_mask=batch["tradable_mask"],
                    aug_kwargs=factor_aug_kwargs or {},
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.factor_aug_s += time.perf_counter() - factor_aug_start

            if sequential_return_objective:
                aux_outputs = dict(aux_outputs or {})
                aux_outputs["initial_weights"] = portfolio_prev_weights

            loss_start = time.perf_counter()
            with _cuda_timing(timing, "loss_cuda_s", device):
                loss = loss_fn(
                    weights,
                    batch["future_log_returns"],
                    batch["tradable_mask"],
                    benchmark_returns=batch.get("benchmark"),
                    can_buy_mask=batch["can_buy_mask"],
                    can_sell_mask=batch["can_sell_mask"],
                    sample_mask=batch.get("sample_mask"),
                    long_only=long_only,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    max_turnover_ratio=max_turnover_ratio,
                    gross_leverage=gross_leverage,
                    gamma_sharpe=gamma_sharpe,
                    gamma_excess=gamma_excess,
                    gamma_cvar=gamma_cvar,
                    cvar_alpha=cvar_alpha,
                    gamma_drawdown=gamma_drawdown,
                    drawdown_target=drawdown_target,
                    gamma_turnover=gamma_turnover,
                    gamma_underperformance=gamma_underperformance,
                    excess_target=excess_target,
                    cvar_budget=cvar_budget,
                    drawdown_budget=drawdown_budget,
                    turnover_budget=turnover_budget,
                    gamma_cvar_budget=gamma_cvar_budget,
                    gamma_drawdown_budget=gamma_drawdown_budget,
                    gamma_turnover_budget=gamma_turnover_budget,
                    objective=objective,
                    aux_outputs=aux_outputs,
                    rank_ic_weight=rank_ic_weight,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                    concentration_weight=concentration_weight,
                    regime_up_threshold=regime_up_threshold,
                    regime_down_threshold=regime_down_threshold,
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.loss_s += time.perf_counter() - loss_start
        should_check_finite = _should_check_finite(batch_no, finite_check_interval_steps)
        if should_check_finite:
            finite_start = time.perf_counter()
            loss_is_finite = _tensor_is_finite(loss)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not loss_is_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {batch_no}{suffix} skipped non-finite loss")
                optimizer.zero_grad(set_to_none=True)
                continue
        if sequential_return_objective and aux_outputs is not None:
            next_prev = aux_outputs.get("_final_weights")
            if next_prev is not None:
                state_start = time.perf_counter()
                portfolio_prev_weights = _detach_portfolio_state(next_prev)
                _maybe_sync_cuda(device, profile_timing)
                timing.portfolio_state_s += time.perf_counter() - state_start
        _maybe_sync_cuda(device, profile_timing)
        timing.forward_s += time.perf_counter() - forward_start

        backward_start = time.perf_counter()
        if progress_label:
            _progress(f"{progress_label}: batch {batch_no}{suffix} backward")
        if scaler.is_enabled():
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                scaler.scale(loss).backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start

            if grad_clip_norm > 0.0:
                if progress_label:
                    _progress(f"{progress_label}: batch {batch_no}{suffix} grad clip")
                clip_start = time.perf_counter()
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start

            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            else:
                gradients_are_finite = True
            if not gradients_are_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {batch_no}{suffix} skipped non-finite gradients")
                optimizer.zero_grad(set_to_none=True)
                continue

            if progress_label:
                _progress(f"{progress_label}: batch {batch_no}{suffix} optimizer step")
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                scaler.step(optimizer)
                scaler.update()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
        else:
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                loss.backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start

            if grad_clip_norm > 0.0:
                if progress_label:
                    _progress(f"{progress_label}: batch {batch_no}{suffix} grad clip")
                clip_start = time.perf_counter()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start

            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            else:
                gradients_are_finite = True
            if not gradients_are_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {batch_no}{suffix} skipped non-finite gradients")
                optimizer.zero_grad(set_to_none=True)
                continue

            if progress_label:
                _progress(f"{progress_label}: batch {batch_no}{suffix} optimizer step")
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                optimizer.step()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start

        if should_check_finite:
            finite_start = time.perf_counter()
            parameters_are_finite = _model_parameters_are_finite(model)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not parameters_are_finite:
                raise RuntimeError("Model parameters became non-finite after optimizer step")

        _maybe_sync_cuda(device, profile_timing)
        timing.backward_s += time.perf_counter() - backward_start

        total_loss_t = total_loss_t + loss.detach().to(dtype=torch.float32)
        steps += 1
        if progress_label:
            _progress(
                f"{progress_label}: batch {batch_no}{suffix} done "
                f"loss={float(loss.detach().cpu()):.8f} elapsed={time.perf_counter() - batch_start:.1f}s"
            )

    timing.total_s = time.perf_counter() - total_start
    timing.batches = steps
    if progress_label:
        _progress(f"{progress_label}: train epoch done steps={steps} total={timing.total_s:.1f}s")
    if steps == 0:
        return total_loss_t.detach(), timing
    mean_loss = (total_loss_t / steps).detach()
    return mean_loss, timing


def _train_epoch_tensor(
    model: nn.Module,
    loss_fn: Callable[..., torch.Tensor],
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark: torch.Tensor,
    sample_mask: torch.Tensor,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str,
    grad_clip_norm: float,
    finite_check_interval_steps: int = 0,
    rank_ic_weight: float = 0.20,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    factor_aug_kwargs: dict[str, float] | None = None,
    profile_timing: bool = False,
    progress_label: str | None = None,
) -> tuple[torch.Tensor, TimingBreakdown]:
    model.train()
    total_rows = int(x.size(0))
    if total_rows == 0:
        return torch.zeros((), device=device, dtype=torch.float32), TimingBreakdown()

    num_batches = total_rows // batch_size
    sequential_return_objective = _is_return_series_objective(objective)
    batch_order = list(range(num_batches)) if sequential_return_objective else torch.randperm(num_batches).tolist()
    portfolio_prev_weights: torch.Tensor | None = None
    total_loss_t = torch.zeros((), device=device, dtype=torch.float32)
    steps = 0
    timing = TimingBreakdown()
    total_start = time.perf_counter()
    if progress_label:
        _progress(
            f"{progress_label}: train epoch start mode=tensor rows={total_rows} "
            f"batch_size={batch_size} batches={num_batches} objective={objective}"
        )

    for step_idx, batch_idx in enumerate(batch_order, start=1):
        batch_start = time.perf_counter()
        start = batch_idx * batch_size
        end = min(start + batch_size, total_rows)
        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} select rows=[{start},{end})")
        _maybe_sync_cuda(device, profile_timing)
        timing.fetch_s += time.perf_counter() - batch_start

        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} transfer to {device}")
        transfer_start = time.perf_counter()
        batch_x = x[start:end].to(device=device, non_blocking=non_blocking)
        batch_ret = future_log_returns[start:end].to(device=device, non_blocking=non_blocking)
        batch_mask = tradable_mask[start:end].to(device=device, non_blocking=non_blocking)
        batch_buy_mask = can_buy_mask[start:end].to(device=device, non_blocking=non_blocking)
        batch_sell_mask = can_sell_mask[start:end].to(device=device, non_blocking=non_blocking)
        batch_bench = benchmark[start:end].to(device=device, non_blocking=non_blocking)
        batch_sample_mask = sample_mask[start:end].to(device=device, non_blocking=non_blocking)
        _maybe_sync_cuda(device, profile_timing)
        timing.transfer_s += time.perf_counter() - transfer_start

        _maybe_cudagraph_step_begin()
        optimizer.zero_grad(set_to_none=True)
        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} forward model + {objective} loss")
        forward_start = time.perf_counter()
        with _autocast_context(device, amp_dtype):
            model_forward_start = time.perf_counter()
            with _cuda_timing(timing, "model_forward_cuda_s", device):
                model_output = model(batch_x, batch_mask)
                weights, aux_outputs = _extract_weights_and_aux(model_output)
            _maybe_sync_cuda(device, profile_timing)
            timing.model_forward_s += time.perf_counter() - model_forward_start

            factor_aug_start = time.perf_counter()
            with _cuda_timing(timing, "factor_aug_cuda_s", device):
                aux_outputs = _attach_factor_augmented_scores(
                    model=model,
                    aux_outputs=aux_outputs,
                    x=batch_x,
                    tradable_mask=batch_mask,
                    aug_kwargs=factor_aug_kwargs or {},
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.factor_aug_s += time.perf_counter() - factor_aug_start

            if sequential_return_objective:
                aux_outputs = dict(aux_outputs or {})
                aux_outputs["initial_weights"] = portfolio_prev_weights

            loss_start = time.perf_counter()
            with _cuda_timing(timing, "loss_cuda_s", device):
                loss = loss_fn(
                    weights,
                    batch_ret,
                    batch_mask,
                    benchmark_returns=batch_bench,
                    can_buy_mask=batch_buy_mask,
                    can_sell_mask=batch_sell_mask,
                    sample_mask=batch_sample_mask,
                    long_only=long_only,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    max_turnover_ratio=max_turnover_ratio,
                    gross_leverage=gross_leverage,
                    gamma_sharpe=gamma_sharpe,
                    gamma_excess=gamma_excess,
                    gamma_cvar=gamma_cvar,
                    cvar_alpha=cvar_alpha,
                    gamma_drawdown=gamma_drawdown,
                    drawdown_target=drawdown_target,
                    gamma_turnover=gamma_turnover,
                    gamma_underperformance=gamma_underperformance,
                    excess_target=excess_target,
                    cvar_budget=cvar_budget,
                    drawdown_budget=drawdown_budget,
                    turnover_budget=turnover_budget,
                    gamma_cvar_budget=gamma_cvar_budget,
                    gamma_drawdown_budget=gamma_drawdown_budget,
                    gamma_turnover_budget=gamma_turnover_budget,
                    objective=objective,
                    aux_outputs=aux_outputs,
                    rank_ic_weight=rank_ic_weight,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                    concentration_weight=concentration_weight,
                    regime_up_threshold=regime_up_threshold,
                    regime_down_threshold=regime_down_threshold,
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.loss_s += time.perf_counter() - loss_start
        should_check_finite = _should_check_finite(step_idx, finite_check_interval_steps)
        if should_check_finite:
            finite_start = time.perf_counter()
            loss_is_finite = _tensor_is_finite(loss)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not loss_is_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {step_idx}/{num_batches} skipped non-finite loss")
                optimizer.zero_grad(set_to_none=True)
                continue
        if sequential_return_objective and aux_outputs is not None:
            next_prev = aux_outputs.get("_final_weights")
            if next_prev is not None:
                state_start = time.perf_counter()
                portfolio_prev_weights = _detach_portfolio_state(next_prev)
                _maybe_sync_cuda(device, profile_timing)
                timing.portfolio_state_s += time.perf_counter() - state_start
        _maybe_sync_cuda(device, profile_timing)
        timing.forward_s += time.perf_counter() - forward_start

        backward_start = time.perf_counter()
        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} backward")
        if scaler.is_enabled():
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                scaler.scale(loss).backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start

            if grad_clip_norm > 0.0:
                if progress_label:
                    _progress(f"{progress_label}: batch {step_idx}/{num_batches} grad clip")
                clip_start = time.perf_counter()
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start

            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            else:
                gradients_are_finite = True
            if not gradients_are_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {step_idx}/{num_batches} skipped non-finite gradients")
                optimizer.zero_grad(set_to_none=True)
                continue

            if progress_label:
                _progress(f"{progress_label}: batch {step_idx}/{num_batches} optimizer step")
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                scaler.step(optimizer)
                scaler.update()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
        else:
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                loss.backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start

            if grad_clip_norm > 0.0:
                if progress_label:
                    _progress(f"{progress_label}: batch {step_idx}/{num_batches} grad clip")
                clip_start = time.perf_counter()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start

            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            else:
                gradients_are_finite = True
            if not gradients_are_finite:
                if progress_label:
                    _progress(f"{progress_label}: batch {step_idx}/{num_batches} skipped non-finite gradients")
                optimizer.zero_grad(set_to_none=True)
                continue

            if progress_label:
                _progress(f"{progress_label}: batch {step_idx}/{num_batches} optimizer step")
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                optimizer.step()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start

        if should_check_finite:
            finite_start = time.perf_counter()
            parameters_are_finite = _model_parameters_are_finite(model)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not parameters_are_finite:
                raise RuntimeError("Model parameters became non-finite after optimizer step")

        _maybe_sync_cuda(device, profile_timing)
        timing.backward_s += time.perf_counter() - backward_start

        total_loss_t = total_loss_t + loss.detach().to(dtype=torch.float32)
        steps += 1
        if progress_label:
            _progress(
                f"{progress_label}: batch {step_idx}/{num_batches} done "
                f"loss={float(loss.detach().cpu()):.8f} elapsed={time.perf_counter() - batch_start:.1f}s"
            )

    timing.total_s = time.perf_counter() - total_start
    timing.batches = steps
    if progress_label:
        _progress(f"{progress_label}: train epoch done steps={steps} total={timing.total_s:.1f}s")
    if steps == 0:
        return total_loss_t.detach(), timing
    mean_loss = (total_loss_t / steps).detach()
    return mean_loss, timing


def _train_epoch_windowed_tensor(
    model: nn.Module,
    loss_fn: Callable[..., torch.Tensor],
    split: WindowedSplitTensors,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float,
    gamma_excess: float,
    gamma_cvar: float,
    cvar_alpha: float,
    gamma_drawdown: float,
    drawdown_target: float,
    gamma_turnover: float,
    gamma_underperformance: float,
    excess_target: float,
    cvar_budget: float,
    drawdown_budget: float,
    turnover_budget: float,
    gamma_cvar_budget: float,
    gamma_drawdown_budget: float,
    gamma_turnover_budget: float,
    objective: str,
    grad_clip_norm: float,
    finite_check_interval_steps: int = 0,
    rank_ic_weight: float = 0.20,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    factor_aug_kwargs: dict[str, float] | None = None,
    profile_timing: bool = False,
    progress_label: str | None = None,
) -> tuple[torch.Tensor, TimingBreakdown]:
    model.train()
    total_rows = len(split)
    if total_rows == 0:
        return torch.zeros((), device=device, dtype=torch.float32), TimingBreakdown()

    num_batches = (total_rows + batch_size - 1) // batch_size
    sequential_return_objective = _is_return_series_objective(objective)
    batch_order = list(range(num_batches)) if sequential_return_objective else torch.randperm(num_batches).tolist()
    portfolio_prev_weights: torch.Tensor | None = None
    total_loss_t = torch.zeros((), device=device, dtype=torch.float32)
    steps = 0
    timing = TimingBreakdown()
    total_start = time.perf_counter()
    if progress_label:
        _progress(
            f"{progress_label}: train epoch start mode=windowed rows={total_rows} "
            f"batch_size={batch_size} batches={num_batches} objective={objective}"
        )

    for step_idx, batch_idx in enumerate(batch_order, start=1):
        batch_start = time.perf_counter()
        start = batch_idx * batch_size
        end = min(start + batch_size, total_rows)
        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} gather rows=[{start},{end})")
        _maybe_sync_cuda(device, profile_timing)
        timing.fetch_s += time.perf_counter() - batch_start

        transfer_start = time.perf_counter()
        batch = split.batch_by_rows(start, end, device=device, non_blocking=non_blocking)
        batch_x = batch["x"]
        batch_ret = batch["future_log_returns"]
        batch_mask = batch["tradable_mask"]
        batch_buy_mask = batch["can_buy_mask"]
        batch_sell_mask = batch["can_sell_mask"]
        batch_bench = batch["benchmark"]
        batch_sample_mask = batch["sample_mask"]
        _maybe_sync_cuda(device, profile_timing)
        timing.transfer_s += time.perf_counter() - transfer_start

        _maybe_cudagraph_step_begin()
        optimizer.zero_grad(set_to_none=True)
        forward_start = time.perf_counter()
        with _autocast_context(device, amp_dtype):
            model_forward_start = time.perf_counter()
            with _cuda_timing(timing, "model_forward_cuda_s", device):
                model_output = model(batch_x, batch_mask)
                weights, aux_outputs = _extract_weights_and_aux(model_output)
            _maybe_sync_cuda(device, profile_timing)
            timing.model_forward_s += time.perf_counter() - model_forward_start

            factor_aug_start = time.perf_counter()
            with _cuda_timing(timing, "factor_aug_cuda_s", device):
                aux_outputs = _attach_factor_augmented_scores(
                    model=model,
                    aux_outputs=aux_outputs,
                    x=batch_x,
                    tradable_mask=batch_mask,
                    aug_kwargs=factor_aug_kwargs or {},
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.factor_aug_s += time.perf_counter() - factor_aug_start

            if sequential_return_objective:
                aux_outputs = dict(aux_outputs or {})
                aux_outputs["initial_weights"] = portfolio_prev_weights

            loss_start = time.perf_counter()
            with _cuda_timing(timing, "loss_cuda_s", device):
                loss = loss_fn(
                    weights,
                    batch_ret,
                    batch_mask,
                    benchmark_returns=batch_bench,
                    can_buy_mask=batch_buy_mask,
                    can_sell_mask=batch_sell_mask,
                    sample_mask=batch_sample_mask,
                    long_only=long_only,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    max_turnover_ratio=max_turnover_ratio,
                    gross_leverage=gross_leverage,
                    gamma_sharpe=gamma_sharpe,
                    gamma_excess=gamma_excess,
                    gamma_cvar=gamma_cvar,
                    cvar_alpha=cvar_alpha,
                    gamma_drawdown=gamma_drawdown,
                    drawdown_target=drawdown_target,
                    gamma_turnover=gamma_turnover,
                    gamma_underperformance=gamma_underperformance,
                    excess_target=excess_target,
                    cvar_budget=cvar_budget,
                    drawdown_budget=drawdown_budget,
                    turnover_budget=turnover_budget,
                    gamma_cvar_budget=gamma_cvar_budget,
                    gamma_drawdown_budget=gamma_drawdown_budget,
                    gamma_turnover_budget=gamma_turnover_budget,
                    objective=objective,
                    aux_outputs=aux_outputs,
                    rank_ic_weight=rank_ic_weight,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                    concentration_weight=concentration_weight,
                    regime_up_threshold=regime_up_threshold,
                    regime_down_threshold=regime_down_threshold,
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.loss_s += time.perf_counter() - loss_start

        should_check_finite = _should_check_finite(step_idx, finite_check_interval_steps)
        if should_check_finite:
            finite_start = time.perf_counter()
            loss_is_finite = _tensor_is_finite(loss)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not loss_is_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
        if sequential_return_objective and aux_outputs is not None:
            next_prev = aux_outputs.get("_final_weights")
            if next_prev is not None:
                state_start = time.perf_counter()
                portfolio_prev_weights = _detach_portfolio_state(next_prev)
                _maybe_sync_cuda(device, profile_timing)
                timing.portfolio_state_s += time.perf_counter() - state_start
        _maybe_sync_cuda(device, profile_timing)
        timing.forward_s += time.perf_counter() - forward_start

        backward_start = time.perf_counter()
        if scaler.is_enabled():
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                scaler.scale(loss).backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            if grad_clip_norm > 0.0:
                clip_start = time.perf_counter()
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start
            gradients_are_finite = True
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                scaler.step(optimizer)
                scaler.update()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
        else:
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device):
                loss.backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            if grad_clip_norm > 0.0:
                clip_start = time.perf_counter()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=grad_clip_norm, error_if_nonfinite=True)
                _maybe_sync_cuda(device, profile_timing)
                timing.clip_s += time.perf_counter() - clip_start
            gradients_are_finite = True
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = _model_gradients_are_finite(model)
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device):
                optimizer.step()
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start

        if should_check_finite:
            finite_start = time.perf_counter()
            parameters_are_finite = _model_parameters_are_finite(model)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not parameters_are_finite:
                raise RuntimeError("Model parameters became non-finite after optimizer step")

        _maybe_sync_cuda(device, profile_timing)
        timing.backward_s += time.perf_counter() - backward_start

        total_loss_t = total_loss_t + loss.detach().to(dtype=torch.float32)
        steps += 1
        if progress_label:
            _progress(
                f"{progress_label}: batch {step_idx}/{num_batches} done "
                f"loss={float(loss.detach().cpu()):.8f} elapsed={time.perf_counter() - batch_start:.1f}s"
            )

    timing.total_s = time.perf_counter() - total_start
    timing.batches = steps
    if steps == 0:
        return total_loss_t.detach(), timing
    return (total_loss_t / steps).detach(), timing


def _compute_metrics_from_tensors(
    strategy_returns: torch.Tensor,
    benchmark_returns: torch.Tensor,
    turnovers: torch.Tensor,
) -> dict[str, float]:
    """Compute performance metrics directly from tensors to avoid repeated numpy conversions."""
    r = torch.nan_to_num(strategy_returns.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    b = torch.nan_to_num(benchmark_returns.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)
    t = torch.nan_to_num(turnovers.float(), nan=0.0, posinf=0.0, neginf=0.0).to(torch.float64)

    if r.numel() == 0:
        return {
            "cumulative_return": 0.0,
            "annualized_return": 0.0,
            "cagr": 0.0,
            "sharpe": 0.0,
            "baseline_sharpe": 0.0,
            "sortino": 0.0,
            "baseline_sortino": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "turnover": 0.0,
            "daily_hit_rate": 0.0,
            "excess_return_vs_universe_average": 0.0,
            "cumulative_benchmark": 0.0,
        }

    sum_r = float(r.sum().item())
    sum_b = float(b.sum().item())
    cum_r = float(torch.expm1(torch.tensor(sum_r, dtype=torch.float64)).item())
    cum_b = float(torch.expm1(torch.tensor(sum_b, dtype=torch.float64)).item())

    avg = r.mean()
    std = r.std(unbiased=False)
    avg_b = b.mean()
    std_b = b.std(unbiased=False)
    ann_r = float(torch.expm1(avg * 252.0).item())
    sharpe = float((avg / std * np.sqrt(252.0)).item()) if float(std.item()) > 0 else 0.0
    baseline_sharpe = float((avg_b / std_b * np.sqrt(252.0)).item()) if float(std_b.item()) > 0 else 0.0
    downside = torch.minimum(r, torch.zeros_like(r))
    downside_b = torch.minimum(b, torch.zeros_like(b))
    downside_dev = torch.sqrt((downside.pow(2)).mean())
    downside_dev_b = torch.sqrt((downside_b.pow(2)).mean())
    sortino = float((avg / downside_dev * np.sqrt(252.0)).item()) if float(downside_dev.item()) > 0 else 0.0
    baseline_sortino = float((avg_b / downside_dev_b * np.sqrt(252.0)).item()) if float(downside_dev_b.item()) > 0 else 0.0

    equity = torch.exp(torch.cumsum(r, dim=0))
    running_max = torch.cummax(equity, dim=0).values
    dd = equity / running_max.clamp_min(1e-12) - 1.0
    max_dd = float(dd.min().item()) if dd.numel() else 0.0
    calmar = ann_r / abs(max_dd) if max_dd < 0.0 else 0.0

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "cagr": ann_r,
        "sharpe": sharpe,
        "baseline_sharpe": baseline_sharpe,
        "sortino": sortino,
        "baseline_sortino": baseline_sortino,
        "max_drawdown": max_dd,
        "calmar": calmar,
        "turnover": float(t.mean().item()) if t.numel() else 0.0,
        "daily_hit_rate": float((r > 0).to(torch.float64).mean().item()),
        "excess_return_vs_universe_average": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def _run_training_tree_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    resume: bool = True,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = list(folds)

    if resume:
        for fold in fold_list:
            completed = _load_completed_fold_result(output_path, fold.fold_id)
            if completed is not None:
                results_by_fold[fold.fold_id] = completed
        if results_by_fold:
            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    grouped_folds: dict[tuple[int, ...], list[WalkForwardFold]] = {}
    for fold in fold_list:
        grouped_folds.setdefault(_group_key(fold.train_years), []).append(fold)

    device = torch.device("cpu")
    amp_dtype: torch.dtype | None = None
    non_blocking = False
    eval_chunk_rows = 2048
    loss_objective = _normalize_risk_objective(config.training.loss_type)

    for train_years_key, group_folds in tqdm(grouped_folds.items(), desc="Train groups", unit="group"):
        train_years = list(train_years_key)
        pending_folds = [fold for fold in group_folds if fold.fold_id not in results_by_fold]
        if not pending_folds:
            print(f"[Train {train_years}] already completed, skipping")
            continue

        print(f"\n{'='*80}")
        print(f"[Train {train_years}] tree model={config.training.model_name} folds={len(group_folds)} pending={len(pending_folds)}")
        print(f"{'='*80}")

        train_reference = group_folds[0]
        train_ds = CrossSectionalDataset(panel, train_reference.train_indices, config.training.lookback)
        train_x, train_returns, train_masks, _, _, _ = _dataset_to_tensors(train_ds)

        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
        )
        if not hasattr(model, "fit"):
            raise TypeError(f"Tree training path expects model.fit(), got {type(model).__name__}")

        print(f"[Train {train_years}] fitting tree model on {int(train_x.size(0))} dates x {panel.num_symbols} symbols")
        model.fit(train_x, train_returns, train_masks)  # type: ignore[attr-defined]

        for fold in pending_folds:
            print(f"[Fold {fold.fold_id}]  val={fold.val_years}  test={fold.test_years}")
            val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
            test_ds = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)
            if len(test_ds) == 0:
                print(f"[Fold {fold.fold_id}] skip: empty test split after lookback filtering")
                continue

            fold_dir = _fold_dir(output_path, fold.fold_id)
            fold_dir.mkdir(parents=True, exist_ok=True)

            val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _dataset_to_tensors(val_ds)
            val_bt_t, val_ic, _ = _evaluate_tensor_batch(
                model,
                val_x,
                val_returns,
                val_masks,
                val_buy_masks,
                val_sell_masks,
                val_bench,
                device,
                amp_dtype,
                non_blocking,
                config.trading.long_only,
                config.trading.buy_fee_rate,
                config.trading.sell_fee_rate,
                config.trading.max_turnover_ratio,
                config.trading.gross_leverage,
                chunk_rows=min(eval_chunk_rows, max(1, int(val_x.size(0)))),
            )
            val_loss = float(
                _evaluated_backtest_loss(
                    val_bt_t,
                    val_returns,
                    val_masks,
                    val_buy_masks,
                    val_sell_masks,
                    val_bench,
                    config,
                    loss_objective,
                ).detach().cpu()
            )
            val_met = _compute_metrics_from_tensors(
                val_bt_t.strategy_returns,
                val_bt_t.benchmark_returns,
                val_bt_t.turnovers,
            )

            test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _dataset_to_tensors(test_ds)
            test_bt_t, test_ic, _ = _evaluate_tensor_batch(
                model,
                test_x,
                test_returns,
                test_masks,
                test_buy_masks,
                test_sell_masks,
                test_bench,
                device,
                amp_dtype,
                non_blocking,
                config.trading.long_only,
                config.trading.buy_fee_rate,
                config.trading.sell_fee_rate,
                config.trading.max_turnover_ratio,
                config.trading.gross_leverage,
                chunk_rows=min(eval_chunk_rows, max(1, int(test_x.size(0)))),
            )

            test_dates = panel.dates[test_ds.valid_indices]
            test_close_prices = panel.close_prices[test_ds.valid_indices]
            test_met = _compute_metrics_from_tensors(
                test_bt_t.strategy_returns,
                test_bt_t.benchmark_returns,
                test_bt_t.turnovers,
            )
            test_bt = test_bt_t.to_numpy()
            test_integer_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt_t.weights_history.detach().cpu().numpy(),
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                can_buy_mask=test_buy_masks.detach().cpu().numpy(),
                can_sell_mask=test_sell_masks.detach().cpu().numpy(),
                benchmark_returns=test_bench.detach().cpu().numpy(),
                initial_capital=1_000_000.0,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                long_only=config.trading.long_only,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                gross_leverage=config.trading.gross_leverage,
                close_prices=test_close_prices,
                symbols=panel.symbols,
                dates=test_dates,
            )
            test_integer_met = compute_metrics(test_integer_bt)

            objective_key = _objective_metric_key(loss_objective)
            val_objective_metric = float(val_met.get(objective_key, float("nan")))
            test_objective_metric = float(test_met.get(objective_key, float("nan")))
            print(f"\n  [val]   IC={val_ic['ic_mean']:+.4f}  IC_IR={val_ic['ic_ir']:+.4f}  {loss_objective}={val_objective_metric:+.4f}  cum_ret={val_met['cumulative_return']:+.4f}  excess={val_met['excess_return_vs_universe_average']:+.4f}")
            print(f"  [test]  IC={test_ic['ic_mean']:+.4f}  IC_IR={test_ic['ic_ir']:+.4f}  {loss_objective}={test_objective_metric:+.4f}  cum_ret={test_met['cumulative_return']:+.4f}  excess={test_met['excess_return_vs_universe_average']:+.4f}")

            fold_result = FoldResult(
                fold_id=fold.fold_id,
                train_years=fold.train_years,
                val_years=fold.val_years,
                test_years=fold.test_years,
                best_val_loss=val_loss,
                val_ic=val_ic,
                val_metrics=val_met,
                test_ic=test_ic,
                test_metrics=test_met,
                test_integer_metrics=test_integer_met,
            )
            results_by_fold[fold.fold_id] = fold_result

            with _model_path(fold_dir).open("wb") as model_file:
                pickle.dump(model, model_file)
            with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
                json.dump(asdict(fold_result), f, indent=2)

            _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
            _save_daily_portfolio_returns_csv(
                fold_dir / "daily_portfolio_returns.csv",
                test_dates,
                test_bt.strategy_returns,
                test_bt.benchmark_returns,
                test_bt.turnovers,
            )
            _save_daily_weights_csv(fold_dir / "daily_weights.csv", test_dates, panel.symbols, test_bt.weights_history)
            report = generate_annual_report(test_bt, test_dates)
            print("\n" + report)
            with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
                f.write(report)

            plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
            plot_equity_curve(test_bt, test_dates, fold_dir / "leverage_equity_curve.png")
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "leverage_equity_curve_log.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "leverage_annual_performance.png")
            _save_integer_share_audit_artifacts(
                fold_dir,
                test_integer_bt,
                test_dates,
                panel.symbols,
                holdings_records,
            )

            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


def _run_inference_tree_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = sorted(list(folds), key=lambda item: item.fold_id)

    device = torch.device("cpu")
    amp_dtype: torch.dtype | None = None
    non_blocking = False
    loss_objective = _normalize_risk_objective(config.training.loss_type)

    print(f"[inference] tree model={config.training.model_name} folds={len(fold_list)}")
    for fold in tqdm(fold_list, desc="Inference folds", unit="fold"):
        fold_dir = _fold_dir(output_path, fold.fold_id)
        model_path = _model_path(fold_dir)
        if not model_path.exists():
            print(f"[Fold {fold.fold_id}] skip: missing model file {model_path.name}")
            continue

        with model_path.open("rb") as model_file:
            model = pickle.load(model_file)

        val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
        test_ds = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)
        if len(test_ds) == 0:
            print(f"[Fold {fold.fold_id}] skip: empty test split after lookback filtering")
            continue

        val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _dataset_to_tensors(val_ds)
        val_chunk_rows = max(1, min(2048, int(val_x.size(0))))
        val_bt_t, val_ic, _ = _evaluate_tensor_batch(
            model,
            val_x,
            val_returns,
            val_masks,
            val_buy_masks,
            val_sell_masks,
            val_bench,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            config.trading.gross_leverage,
            chunk_rows=val_chunk_rows,
        )
        val_loss = float(
            _evaluated_backtest_loss(
                val_bt_t,
                val_returns,
                val_masks,
                val_buy_masks,
                val_sell_masks,
                val_bench,
                config,
                loss_objective,
            ).detach().cpu()
        )
        val_met = _compute_metrics_from_tensors(
            val_bt_t.strategy_returns,
            val_bt_t.benchmark_returns,
            val_bt_t.turnovers,
        )

        test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _dataset_to_tensors(test_ds)
        test_chunk_rows = max(1, min(2048, int(test_x.size(0))))
        test_bt_t, test_ic, _ = _evaluate_tensor_batch(
            model,
            test_x,
            test_returns,
            test_masks,
            test_buy_masks,
            test_sell_masks,
            test_bench,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            config.trading.gross_leverage,
            chunk_rows=test_chunk_rows,
        )

        test_dates = panel.dates[test_ds.valid_indices]
        test_close_prices = panel.close_prices[test_ds.valid_indices]
        test_met = _compute_metrics_from_tensors(
            test_bt_t.strategy_returns,
            test_bt_t.benchmark_returns,
            test_bt_t.turnovers,
        )
        test_bt = test_bt_t.to_numpy()
        test_integer_bt, holdings_records = run_backtest_integer_shares(
            weights=test_bt_t.weights_history.detach().cpu().numpy(),
            future_returns=test_returns.detach().cpu().numpy(),
            tradable_mask=test_masks.detach().cpu().numpy(),
            can_buy_mask=test_buy_masks.detach().cpu().numpy(),
            can_sell_mask=test_sell_masks.detach().cpu().numpy(),
            benchmark_returns=test_bench.detach().cpu().numpy(),
            initial_capital=1_000_000.0,
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            gross_leverage=config.trading.gross_leverage,
            close_prices=test_close_prices,
            symbols=panel.symbols,
            dates=test_dates,
        )
        test_integer_met = compute_metrics(test_integer_bt)

        fold_result = FoldResult(
            fold_id=fold.fold_id,
            train_years=fold.train_years,
            val_years=fold.val_years,
            test_years=fold.test_years,
            best_val_loss=val_loss,
            val_ic=val_ic,
            val_metrics=val_met,
            test_ic=test_ic,
            test_metrics=test_met,
            test_integer_metrics=test_integer_met,
        )
        results_by_fold[fold.fold_id] = fold_result

        with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
            json.dump(asdict(fold_result), f, indent=2)

        _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
        _save_daily_portfolio_returns_csv(
            fold_dir / "daily_portfolio_returns.csv",
            test_dates,
            test_bt.strategy_returns,
            test_bt.benchmark_returns,
            test_bt.turnovers,
        )
        _save_daily_weights_csv(fold_dir / "daily_weights.csv", test_dates, panel.symbols, test_bt.weights_history)
        report = generate_annual_report(test_bt, test_dates)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)

        plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
        plot_equity_curve(test_bt, test_dates, fold_dir / "leverage_equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "leverage_equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "leverage_annual_performance.png")
        _save_integer_share_audit_artifacts(
            fold_dir,
            test_integer_bt,
            test_dates,
            panel.symbols,
            holdings_records,
        )

    if results_by_fold:
        _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


def _run_inference_neural_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
) -> list[FoldResult]:
    device = _resolve_device(config)
    non_blocking = config.training.non_blocking_transfer and device.type == "cuda"
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    loss_objective = _normalize_risk_objective(config.training.loss_type)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = sorted(list(folds), key=lambda item: item.fold_id)

    print(f"[inference] model={config.training.model_name} folds={len(fold_list)} device={device}")
    for fold in tqdm(fold_list, desc="Inference folds", unit="fold"):
        fold_dir = _fold_dir(output_path, fold.fold_id)
        model_file = _model_path(fold_dir)
        best_checkpoint_file = _best_checkpoint_path(fold_dir)

        model_state_dict: dict | None = None
        best_val_loss = float("inf")

        if model_file.exists():
            model_state_dict = torch.load(model_file, map_location="cpu")
        elif best_checkpoint_file.exists():
            checkpoint = _load_checkpoint(best_checkpoint_file)
            model_state_dict = checkpoint.get("model_state_dict")
            best_val_loss = float(checkpoint.get("best_val_loss", float("inf")))
        else:
            print(f"[Fold {fold.fold_id}] skip: missing {model_file.name} and {best_checkpoint_file.name}")
            continue

        if not isinstance(model_state_dict, dict):
            print(f"[Fold {fold.fold_id}] skip: invalid model state format")
            continue

        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
        ).to(device)
        _load_state_dict(model, model_state_dict)

        val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
        test_ds = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)
        if len(test_ds) == 0:
            print(f"[Fold {fold.fold_id}] skip: empty test split after lookback filtering")
            continue

        val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _dataset_to_tensors(val_ds)
        val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _prepare_split_tensors(
            val_x,
            val_returns,
            val_masks,
            val_buy_masks,
            val_sell_masks,
            val_bench,
            device,
            non_blocking,
        )
        val_chunk_rows = max(1, min(2048, int(val_x.size(0))))
        if config.training.chunk_rows > 0:
            val_chunk_rows = max(1, min(config.training.chunk_rows, int(val_x.size(0))))

        val_bt_t, val_ic, _ = _evaluate_tensor_batch(
            model,
            val_x,
            val_returns,
            val_masks,
            val_buy_masks,
            val_sell_masks,
            val_bench,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            config.trading.gross_leverage,
            chunk_rows=val_chunk_rows,
        )

        if not np.isfinite(best_val_loss):
            best_val_loss = float(
                _evaluated_backtest_loss(
                    val_bt_t,
                    val_returns,
                    val_masks,
                    val_buy_masks,
                    val_sell_masks,
                    val_bench,
                    config,
                    loss_objective,
                ).detach().cpu()
            )

        val_met = _compute_metrics_from_tensors(
            val_bt_t.strategy_returns,
            val_bt_t.benchmark_returns,
            val_bt_t.turnovers,
        )

        test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _dataset_to_tensors(test_ds)
        test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _prepare_split_tensors(
            test_x,
            test_returns,
            test_masks,
            test_buy_masks,
            test_sell_masks,
            test_bench,
            device,
            non_blocking,
        )
        test_chunk_rows = max(1, min(2048, int(test_x.size(0))))
        if config.training.chunk_rows > 0:
            test_chunk_rows = max(1, min(config.training.chunk_rows, int(test_x.size(0))))

        test_bt_t, test_ic, _ = _evaluate_tensor_batch(
            model,
            test_x,
            test_returns,
            test_masks,
            test_buy_masks,
            test_sell_masks,
            test_bench,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            config.trading.gross_leverage,
            chunk_rows=test_chunk_rows,
        )

        test_dates = panel.dates[test_ds.valid_indices]
        test_close_prices = panel.close_prices[test_ds.valid_indices]
        test_met = _compute_metrics_from_tensors(
            test_bt_t.strategy_returns,
            test_bt_t.benchmark_returns,
            test_bt_t.turnovers,
        )
        test_bt = test_bt_t.to_numpy()
        test_integer_bt, holdings_records = run_backtest_integer_shares(
            weights=test_bt_t.weights_history.detach().cpu().numpy(),
            future_returns=test_returns.detach().cpu().numpy(),
            tradable_mask=test_masks.detach().cpu().numpy(),
            can_buy_mask=test_buy_masks.detach().cpu().numpy(),
            can_sell_mask=test_sell_masks.detach().cpu().numpy(),
            benchmark_returns=test_bench.detach().cpu().numpy(),
            initial_capital=1_000_000.0,
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            gross_leverage=config.trading.gross_leverage,
            close_prices=test_close_prices,
            symbols=panel.symbols,
            dates=test_dates,
        )
        test_integer_met = compute_metrics(test_integer_bt)

        fold_result = FoldResult(
            fold_id=fold.fold_id,
            train_years=fold.train_years,
            val_years=fold.val_years,
            test_years=fold.test_years,
            best_val_loss=best_val_loss,
            val_ic=val_ic,
            val_metrics=val_met,
            test_ic=test_ic,
            test_metrics=test_met,
            test_integer_metrics=test_integer_met,
        )
        results_by_fold[fold.fold_id] = fold_result

        with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
            json.dump(asdict(fold_result), f, indent=2)

        _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
        _save_daily_portfolio_returns_csv(
            fold_dir / "daily_portfolio_returns.csv",
            test_dates,
            test_bt.strategy_returns,
            test_bt.benchmark_returns,
            test_bt.turnovers,
        )
        _save_daily_weights_csv(fold_dir / "daily_weights.csv", test_dates, panel.symbols, test_bt.weights_history)
        report = generate_annual_report(test_bt, test_dates)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)

        plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
        plot_equity_curve(test_bt, test_dates, fold_dir / "leverage_equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "leverage_equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "leverage_annual_performance.png")
        _save_integer_share_audit_artifacts(
            fold_dir,
            test_integer_bt,
            test_dates,
            panel.symbols,
            holdings_records,
        )

    if results_by_fold:
        _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


def run_inference(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
) -> list[FoldResult]:
    if _is_tree_model_name(config.training.model_name):
        return _run_inference_tree_models(panel, folds, config, output_dir)
    return _run_inference_neural_models(panel, folds, config, output_dir)


def run_training(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    resume: bool = True,
    profile_timing: bool = False,
) -> list[FoldResult]:
    if _is_tree_model_name(config.training.model_name):
        return _run_training_tree_models(panel, folds, config, output_dir, resume=resume)

    device = _resolve_device(config)
    non_blocking = config.training.non_blocking_transfer and device.type == "cuda"
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    _configure_backtest_runtime_from_config(config)
    if config.environment.use_tensor_cores and device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
        try:
            # Keep compile/cudagraph enabled while suppressing verbose artifact logs.
            torch._logging.set_logs(
                perf_hints=False,
                cudagraphs=False,
                autotuning=False,
            )
        except Exception:
            pass
        try:
            import torch._inductor.config as inductor_config  # type: ignore

            inductor_config.triton.cudagraph_skip_dynamic_graphs = False
            inductor_config.triton.cudagraph_dynamic_shape_warn_limit = 0
            logging.getLogger("torch._inductor.utils").setLevel(logging.ERROR)
            logging.getLogger("torch._inductor.scheduler").setLevel(logging.ERROR)
        except Exception:
            pass
    print(
        f"[runtime] device={device.type} "
        f"cuda_available={torch.cuda.is_available()} "
        f"num_gpus={torch.cuda.device_count()}"
    )
    if device.type == "cuda":
        amp_mode = "tf32" if amp_dtype is None else str(amp_dtype).replace("torch.", "")
        print(
            f"[runtime] precision_mode={amp_mode} "
            f"allow_tf32_matmul={torch.backends.cuda.matmul.allow_tf32} "
            f"allow_tf32_cudnn={torch.backends.cudnn.allow_tf32}"
        )
    if profile_timing:
        print("[profile] timing mode enabled")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    loss_objective = _normalize_risk_objective(config.training.loss_type)
    factor_loss_kwargs = _factor_loss_kwargs(config)
    portfolio_autoencoder_loss_kwargs = _portfolio_autoencoder_loss_kwargs(config)
    risk_loss_kwargs = {**factor_loss_kwargs, **portfolio_autoencoder_loss_kwargs}
    factor_aug_kwargs = _factor_augmentation_kwargs(config, loss_objective)
    fold_list = list(folds)

    if resume:
        for fold in fold_list:
            completed = _load_completed_fold_result(output_path, fold.fold_id)
            if completed is not None:
                results_by_fold[fold.fold_id] = completed
        if results_by_fold:
            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    grouped_folds: dict[tuple[int, ...], list[WalkForwardFold]] = {}
    for fold in fold_list:
        grouped_folds.setdefault(_group_key(fold.train_years), []).append(fold)

    warm_start_checkpoint_path: Path | None = None

    for train_years_key, group_folds in tqdm(grouped_folds.items(), desc="Train groups", unit="group"):
        train_years = list(train_years_key)
        group_checkpoint_path = _group_checkpoint_path(output_path, train_years)
        group_curve_path = _group_curve_path(output_path, train_years)
        pending_folds = [fold for fold in group_folds if fold.fold_id not in results_by_fold]

        # Ensure each train-group starts from a clean CUDA allocator state.
        if device.type == "cuda":
            _release_cuda_memory(device)

        if not pending_folds:
            if config.training.warm_start_from_previous_fold and group_checkpoint_path.exists():
                warm_start_checkpoint_path = group_checkpoint_path
            print(f"[Train {train_years}] already completed, skipping")
            continue

        print(f"\n{'='*80}")
        print(f"[Train {train_years}] folds={len(group_folds)} pending={len(pending_folds)}")
        print(f"{'='*80}")

        train_reference = group_folds[0]
        train_ds = CrossSectionalDataset(panel, train_reference.train_indices, config.training.lookback)
        min_batch_size = max(1, config.training.min_batch_size)
        train_batch_used_bytes = 0

        if config.training.auto_batch_size and device.type == "cuda":
            estimation_model = build_model(
                config=config,
                lookback=config.training.lookback,
                num_features=len(panel.feature_names),
                num_symbols=panel.num_symbols,
            )
            train_static_bytes = _estimate_model_static_bytes(estimation_model, training_mode=True)
            model_name = str(config.training.model_name).strip().lower()
            if model_name in {"cross_sectional_temporal_portfolio_model", "portfolio_multitask", "cstpm"}:
                train_sample_bytes, sample_detail = _estimate_cstpm_sample_bytes(config, panel, amp_dtype)
            else:
                estimation_hidden_dim = model_hidden_dim_hint(config)
                train_sample_bytes = _estimate_sample_bytes(
                    lookback=config.training.lookback,
                    num_symbols=panel.num_symbols,
                    num_features=len(panel.feature_names),
                    hidden_dim=estimation_hidden_dim,
                    amp_dtype=amp_dtype,
                    training_mode=True,
                )
                sample_detail = "generic_formula"

            usable_vram_bytes, mem_source = _usable_vram_bytes(
                device=device,
                budget_gb=config.training.vram_budget_gb,
                safety_margin_gb=config.training.vram_safety_margin_gb,
                target_fraction=config.training.target_vram_fraction,
            )
            # Keep substantial headroom for CUDA workspaces, allocator
            # fragmentation, validation chunks, and Adam optimizer transients.
            estimated_budget_bytes = int(usable_vram_bytes * 0.70)
            train_batch_size = _budget_batch_size(
                dataset_size=len(train_ds),
                requested_cap=max(1, int(config.training.batch_size_train)),
                budget_bytes=estimated_budget_bytes,
                static_bytes=train_static_bytes,
                sample_bytes=train_sample_bytes,
                min_batch_size=min_batch_size,
            )
            train_batch_used_bytes = train_static_bytes + train_batch_size * train_sample_bytes
            print(
                f"[Train {train_years}] calculated train batch_size={train_batch_size} "
                f"(est_used={train_batch_used_bytes/1024**3:.1f}GB, "
                f"est_budget={estimated_budget_bytes/1024**3:.1f}GB, "
                f"usable={usable_vram_bytes/1024**3:.1f}GB, "
                f"sample={train_sample_bytes/1024**3:.2f}GB, "
                f"static={train_static_bytes/1024**3:.2f}GB, {sample_detail}, {mem_source})"
            )
            del estimation_model
            _release_cuda_memory(device)
        else:
            train_batch_size = _split_batch_size(len(train_ds), config.training.batch_size_train)

        print(f"[Train {train_years}] using batch_size train={train_batch_size}")
        train_setup_start = time.perf_counter()
        rank_tensor_objectives = {"rank_ic", "pure_rank", "factor_generalization", "portfolio_autoencoder"}
        use_windowed_tensors = (
            not bool(getattr(config.training, "materialize_window_tensors", False))
            and loss_objective not in rank_tensor_objectives
        )
        train_windowed: WindowedSplitTensors | None = None
        combined_val_windowed: WindowedSplitTensors | None = None
        combined_test_windowed: WindowedSplitTensors | None = None
        train_x: torch.Tensor | None
        train_returns: torch.Tensor | None
        train_masks: torch.Tensor | None
        train_buy_masks: torch.Tensor | None
        train_sell_masks: torch.Tensor | None
        train_benchmark: torch.Tensor | None
        train_sample_mask: torch.Tensor | None
        if use_windowed_tensors:
            _progress(f"[Train {train_years}] setup train tensors: lazy windowed split")
            train_windowed = _pad_windowed_training_split(dataset_to_windowed_tensors(train_ds), train_batch_size)
            train_windowed = _prepare_windowed_split(train_windowed, device, non_blocking)
            train_x = None
            train_returns = None
            train_masks = None
            train_buy_masks = None
            train_sell_masks = None
            train_benchmark = None
            train_sample_mask = None
        else:
            _progress(f"[Train {train_years}] setup train tensors: materialize dataset -> tensors")
            train_x, train_returns, train_masks, train_buy_masks, train_sell_masks, train_benchmark = _dataset_to_tensors(train_ds)
            _progress(f"[Train {train_years}] setup train tensors: pad to batch_size={train_batch_size}")
            train_x, train_returns, train_masks, train_buy_masks, train_sell_masks, train_benchmark, train_sample_mask = _pad_training_tensors(
                train_x,
                train_returns,
                train_masks,
                train_buy_masks,
                train_sell_masks,
                train_benchmark,
                train_batch_size,
            )
            _progress(f"[Train {train_years}] setup train tensors: prepare host/device memory")
            train_x, train_returns, train_masks, train_buy_masks, train_sell_masks, train_benchmark = _prepare_split_tensors(
                train_x,
                train_returns,
                train_masks,
                train_buy_masks,
                train_sell_masks,
                train_benchmark,
                device,
                non_blocking,
            )
            train_sample_mask = _prepare_host_tensor(
                train_sample_mask,
                pin_memory=(device.type == "cuda" and non_blocking),
            )
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.train_tensors",
                TimingBreakdown(total_s=time.perf_counter() - train_setup_start),
            )

        fold_contexts: dict[int, FoldRuntimeContext] = {}
        for fold in pending_folds:
            print(f"[Fold {fold.fold_id}]  val={fold.val_years}  test={fold.test_years}")
            val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
            test_ds = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)

            if len(test_ds) == 0:
                print(f"[Fold {fold.fold_id}] skip: empty test split after lookback filtering")
                continue

            val_batch_size = _split_batch_size(len(val_ds), config.training.batch_size_eval)
            test_batch_size = _split_batch_size(len(test_ds), config.training.batch_size_eval)
            if config.training.auto_batch_size and device.type == "cuda":
                val_batch_size = min(train_batch_size * 2, len(val_ds))
                test_batch_size = min(train_batch_size * 2, len(test_ds))

            fold_dir = _fold_dir(output_path, fold.fold_id)
            fold_dir.mkdir(parents=True, exist_ok=True)
            checkpoint_best_path = _best_checkpoint_path(fold_dir)

            fold_contexts[fold.fold_id] = FoldRuntimeContext(
                fold=fold,
                fold_dir=fold_dir,
                val_ds=val_ds,
                test_ds=test_ds,
                checkpoint_best_path=checkpoint_best_path,
            )

            if resume and checkpoint_best_path.exists():
                checkpoint = _load_checkpoint(checkpoint_best_path)
                fold_contexts[fold.fold_id].best_val_loss = float(
                    checkpoint.get("best_val_loss", float("inf"))
                )

        if not fold_contexts:
            train_windowed = None
            combined_val_windowed = None
            combined_test_windowed = None
            train_x = None
            train_returns = None
            train_masks = None
            train_buy_masks = None
            train_sell_masks = None
            train_benchmark = None
            train_sample_mask = None
            if device.type == "cuda":
                _release_cuda_memory(device)
            continue

        val_datasets = [context.val_ds for context in fold_contexts.values()]
        val_setup_start = time.perf_counter()
        if use_windowed_tensors:
            _progress(f"[Train {train_years}] setup validation tensors: lazy windowed split")
            combined_val_windowed, val_lengths = _combine_datasets_to_windowed(val_datasets)
            combined_val_windowed = _prepare_windowed_split(combined_val_windowed, device, non_blocking)
            combined_val_x = None
            combined_val_returns = None
            combined_val_masks = None
            combined_val_buy_masks = None
            combined_val_sell_masks = None
            combined_val_bench = None
        else:
            _progress(f"[Train {train_years}] setup validation tensors")
            combined_val_x, combined_val_returns, combined_val_masks, combined_val_buy_masks, combined_val_sell_masks, combined_val_bench, val_lengths = _combine_datasets_to_tensors(
                val_datasets,  # type: ignore[arg-type]
            )
            combined_val_x, combined_val_returns, combined_val_masks, combined_val_buy_masks, combined_val_sell_masks, combined_val_bench = _prepare_split_tensors(
                combined_val_x,
                combined_val_returns,
                combined_val_masks,
                combined_val_buy_masks,
                combined_val_sell_masks,
                combined_val_bench,
                device,
                non_blocking,
            )
        val_offsets: list[int] = [0]
        for length in val_lengths:
            val_offsets.append(val_offsets[-1] + length)
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.val_tensors",
                TimingBreakdown(total_s=time.perf_counter() - val_setup_start),
            )

        curve_test_fold_index = 0
        curve_test_fold_context = list(fold_contexts.values())[curve_test_fold_index]
        curve_test_years = curve_test_fold_context.fold.test_years[:1]
        panel_years = panel.dates.astype("datetime64[Y]").astype(np.int64) + 1970
        curve_test_indices = curve_test_fold_context.fold.test_indices[
            np.isin(panel_years[curve_test_fold_context.fold.test_indices], curve_test_years)
        ]
        if curve_test_indices.size == 0:
            curve_test_indices = curve_test_fold_context.fold.test_indices
            curve_test_years = curve_test_fold_context.fold.test_years
        curve_test_ds = CrossSectionalDataset(panel, curve_test_indices, config.training.lookback)
        if len(curve_test_ds) == 0 and curve_test_indices.size != curve_test_fold_context.fold.test_indices.size:
            curve_test_indices = curve_test_fold_context.fold.test_indices
            curve_test_years = curve_test_fold_context.fold.test_years
            curve_test_ds = curve_test_fold_context.test_ds
        test_datasets = [curve_test_ds]
        test_setup_start = time.perf_counter()
        if use_windowed_tensors:
            _progress(f"[Train {train_years}] setup test tensors: lazy windowed split")
            combined_test_windowed, test_lengths = _combine_datasets_to_windowed(test_datasets)
            combined_test_windowed = _prepare_windowed_split(combined_test_windowed, device, non_blocking)
            combined_test_x = None
            combined_test_returns = None
            combined_test_masks = None
            combined_test_buy_masks = None
            combined_test_sell_masks = None
            combined_test_bench = None
        else:
            _progress(f"[Train {train_years}] setup test tensors")
            combined_test_x, combined_test_returns, combined_test_masks, combined_test_buy_masks, combined_test_sell_masks, combined_test_bench, test_lengths = _combine_datasets_to_tensors(
                test_datasets,  # type: ignore[arg-type]
            )
            combined_test_x, combined_test_returns, combined_test_masks, combined_test_buy_masks, combined_test_sell_masks, combined_test_bench = _prepare_split_tensors(
                combined_test_x,
                combined_test_returns,
                combined_test_masks,
                combined_test_buy_masks,
                combined_test_sell_masks,
                combined_test_bench,
                device,
                non_blocking,
            )
        curve_test_start_row = 0
        curve_test_end_row = int(test_lengths[0])
        curve_test_offsets = [0, curve_test_end_row - curve_test_start_row]
        print(
            f"[Train {train_years}] epoch-level test loss uses fold "
            f"{curve_test_fold_context.fold.fold_id} only "
            f"(years={curve_test_years}, rows={curve_test_offsets[-1]})"
        )
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.test_tensors",
                TimingBreakdown(total_s=time.perf_counter() - test_setup_start),
            )

        model_build_start = time.perf_counter()
        _progress(f"[Train {train_years}] build model")
        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
        ).to(device)
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.model_build",
                TimingBreakdown(total_s=time.perf_counter() - model_build_start),
            )

        if config.training.warm_start_from_previous_fold and warm_start_checkpoint_path is not None and warm_start_checkpoint_path.exists():
            warm_start_checkpoint = _load_checkpoint(warm_start_checkpoint_path)
            if "model_state_dict" in warm_start_checkpoint:
                _load_state_dict(model, warm_start_checkpoint["model_state_dict"])
                print(f"[Train {train_years}] warm-started from {warm_start_checkpoint_path.name}")

        compiled_train_model: nn.Module = model
        eval_model: nn.Module = model
        compiled_loss_fn: Callable[..., torch.Tensor] = partial(risk_aware_loss, **risk_loss_kwargs)

        if device.type == "cuda":
            try:
                optimizer = torch.optim.AdamW(
                    model.parameters(),
                    lr=config.training.learning_rate,
                    weight_decay=config.training.weight_decay,
                    fused=True,
                )
                print(f"[Train {train_years}] optimizer=AdamW(fused=True)")
            except TypeError:
                optimizer = torch.optim.AdamW(
                    model.parameters(),
                    lr=config.training.learning_rate,
                    weight_decay=config.training.weight_decay,
                )
                print(f"[Train {train_years}] optimizer=AdamW(fused=False, unsupported by this torch build)")
        else:
            optimizer = torch.optim.AdamW(
                model.parameters(),
                lr=config.training.learning_rate,
                weight_decay=config.training.weight_decay,
            )
        scaler = GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)
        scheduler, scheduler_name, scheduler_requires_metric = _create_lr_scheduler(optimizer, config)
        if scheduler is not None:
            print(f"[Train {train_years}] lr_scheduler={scheduler_name}")

        start_epoch = 1
        if resume and group_checkpoint_path.exists():
            checkpoint = _load_checkpoint(group_checkpoint_path)
            if list(checkpoint.get("train_years", [])) == train_years:
                _load_state_dict(model, checkpoint["model_state_dict"])
                optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
                scaler_state = checkpoint.get("scaler_state_dict")
                if scaler_state:
                    try:
                        scaler.load_state_dict(scaler_state)
                    except RuntimeError as exc:
                        # A checkpoint may come from a run where GradScaler was disabled.
                        print(f"[Train {train_years}] skip scaler resume: {exc}")
                if scheduler is not None:
                    scheduler_state = checkpoint.get("scheduler_state_dict")
                    if scheduler_state:
                        scheduler.load_state_dict(scheduler_state)
                start_epoch = int(checkpoint.get("epoch", 0)) + 1
                print(f"[Train {train_years}] resumed from epoch {start_epoch}")

        if start_epoch > config.training.epochs:
            print(f"[Train {train_years}] checkpoint already reached epoch {config.training.epochs}; evaluating only")

        record_epoch_curve = bool(getattr(config.training, "record_epoch_curve", True))
        if record_epoch_curve:
            _trim_group_curve(group_curve_path, start_epoch)
            curve_plotter: _AsyncEpochCurvePlotter | None = _AsyncEpochCurvePlotter(
                group_curve_path,
                interval=int(config.training.curve_plot_interval),
                async_enabled=bool(config.training.curve_plot_async),
            )
        else:
            curve_plotter = None
            print(f"[Train {train_years}] epoch curve recording disabled")
        run_epoch_test_curve = bool(getattr(config.training, "epoch_test_curve", True))
        defer_epoch_curve_plot_until_end = bool(
            getattr(config.training, "defer_epoch_curve_plot_until_end", False)
        )
        if not run_epoch_test_curve:
            print(f"[Train {train_years}] epoch-level test curve disabled; final test runs after training")
        if defer_epoch_curve_plot_until_end:
            print(f"[Train {train_years}] epoch curve plotting deferred until training completes")

        def _record_epoch_curve(payload: dict[str, float | int | None], request_plot: bool) -> float:
            if not record_epoch_curve:
                return 0.0
            start_t = time.perf_counter()
            _append_group_curve(group_curve_path, payload)
            if request_plot and not defer_epoch_curve_plot_until_end and curve_plotter is not None:
                curve_plotter.request()
            return time.perf_counter() - start_t

        curve_plot_request_interval = max(1, int(config.training.curve_plot_interval))
        combined_val_rows = len(combined_val_windowed) if combined_val_windowed is not None else int(combined_val_x.size(0))
        eval_auto_chunk_rows_cap = int(getattr(config.training, "eval_auto_chunk_rows_cap", 16))
        eval_model_chunk_rows_config = getattr(config.training, "eval_model_chunk_rows", "auto")

        if config.training.chunk_rows > 0:
            eval_chunk_rows = min(config.training.chunk_rows, combined_val_rows)
            print(f"[Train {train_years}] eval model_chunk_rows={eval_chunk_rows} (manual chunk_rows)")
        else:
            measured_free_bytes: int | None = None
            if train_batch_used_bytes > 0:
                budget_bytes = int(max(0.0, config.training.vram_budget_gb) * (1024 ** 3))
                measured_free_bytes = max(1, budget_bytes - int(train_batch_used_bytes))
                print(
                    f"[Train {train_years}] pre-chunk free VRAM from budget: "
                    f"{measured_free_bytes/1024**3:.2f}GB "
                    f"(={config.training.vram_budget_gb:.1f}GB - {train_batch_used_bytes/1024**3:.2f}GB)"
                )
            else:
                device_index = device.index if device.index is not None else torch.cuda.current_device()
                smi_mem = _query_nvidia_smi_free_bytes(device_index)
                if smi_mem is not None:
                    _, _, smi_free = smi_mem
                    measured_free_bytes = int(smi_free)
                    print(f"[Train {train_years}] pre-chunk free VRAM (nvidia-smi): {smi_free/1024**3:.2f}GB")
            if combined_val_windowed is not None:
                probe_rows = min(combined_val_rows, max(1, min(train_batch_size, 256)))
                probe_batch = combined_val_windowed.batch_by_rows(0, probe_rows, device=device, non_blocking=non_blocking)
                eval_chunk_rows = _auto_chunk_rows(
                    model=model,
                    x=probe_batch["x"],
                    tradable_mask=probe_batch["tradable_mask"],
                    device=device,
                    amp_dtype=amp_dtype,
                    target_vram_fraction=config.training.target_vram_fraction,
                    vram_budget_gb=config.training.vram_budget_gb,
                    vram_safety_margin_gb=config.training.vram_safety_margin_gb,
                    measured_free_bytes=measured_free_bytes,
                    max_rows=combined_val_rows,
                    max_chunk_rows=eval_auto_chunk_rows_cap,
                )
                eval_chunk_rows = max(1, min(eval_chunk_rows, combined_val_rows))
                del probe_batch
            else:
                eval_chunk_rows = _auto_chunk_rows(
                    model=model,
                    x=combined_val_x,
                    tradable_mask=combined_val_masks,
                    device=device,
                    amp_dtype=amp_dtype,
                    target_vram_fraction=config.training.target_vram_fraction,
                    vram_budget_gb=config.training.vram_budget_gb,
                    vram_safety_margin_gb=config.training.vram_safety_margin_gb,
                    measured_free_bytes=measured_free_bytes,
                    max_chunk_rows=eval_auto_chunk_rows_cap,
                )
            print(f"[Train {train_years}] eval chunk_rows={eval_chunk_rows} (auto)")
        if str(eval_model_chunk_rows_config).strip().lower() not in {"", "auto"}:
            eval_chunk_rows = max(1, min(int(eval_model_chunk_rows_config), combined_val_rows))
            print(f"[Train {train_years}] eval model_chunk_rows={eval_chunk_rows} (manual eval_model_chunk_rows)")

        eval_backtest_chunk_rows_config = int(getattr(config.training, "eval_backtest_chunk_rows", 512))
        if bool(getattr(config.training, "eval_backtest_chunk_rows_auto", True)):
            eval_backtest_chunk_rows = max(eval_chunk_rows, eval_backtest_chunk_rows_config)
        else:
            eval_backtest_chunk_rows = max(1, eval_backtest_chunk_rows_config)
        eval_backtest_chunk_rows = max(1, eval_backtest_chunk_rows)
        print(
            f"[Train {train_years}] eval chunks: "
            f"model_chunk_rows={eval_chunk_rows}, backtest_chunk_rows={eval_backtest_chunk_rows}"
        )

        config_auto_compile_risk = bool(getattr(config.training, "auto_torch_compile_sharpe", True))
        auto_compile_risk = (
            loss_objective in {"sharpe", "sortino", "log_utility"}
            and device.type == "cuda"
            and not profile_timing
            and config_auto_compile_risk
            and _env_truthy("STOCKAGENT_AUTO_TORCH_COMPILE_SHARPE", "1")
        )
        should_enable_compile = (config.training.enable_torch_compile or auto_compile_risk) and hasattr(torch, "compile")
        model_compile_status = "requested" if should_enable_compile else "off"
        loss_compile_status = "eager"
        config_compile_loss = bool(getattr(config.training, "compile_loss", auto_compile_risk))
        default_compile_loss = "1" if config_compile_loss else "0"
        compile_loss = _env_truthy("STOCKAGENT_COMPILE_LOSS", default_compile_loss)

        if should_enable_compile:
            can_compile, reason = _can_enable_torch_compile(device)
            if can_compile:
                try:
                    compile_start = time.perf_counter()
                    compile_mode = str(getattr(config.training, "torch_compile_mode", "reduce-overhead") or "default")
                    compile_mode_arg = None if compile_mode == "default" else compile_mode
                    print(f"[Train {train_years}] compile warmup may take extra time at epoch 0")
                    compiled_train_model = torch.compile(model, mode=compile_mode_arg, dynamic=False)
                    eval_model = compiled_train_model
                    compile_source = f"auto({loss_objective})" if (auto_compile_risk and not config.training.enable_torch_compile) else "config"
                    print(
                        f"[Train {train_years}] torch.compile enabled "
                        f"(mode={compile_mode}, dynamic=False, source={compile_source}, {reason})"
                    )
                    model_compile_status = f"enabled:{compile_source}"

                    if compile_loss:
                        try:
                            eager_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                            raw_compiled_loss_fn = torch.compile(
                                eager_loss_fn,
                                dynamic=False,
                                options={"triton.cudagraphs": False},
                            )
                            compiled_loss_fn = _CompiledLossFallback(
                                raw_compiled_loss_fn,
                                eager_loss_fn,
                                label=f"Train {train_years}",
                            )
                            print(
                                f"[Train {train_years}] torch.compile loss enabled "
                                "(mode=default, dynamic=False, cudagraphs=False)"
                            )
                            loss_compile_status = "enabled"
                        except Exception as e:
                            compiled_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                            loss_compile_status = "fallback:eager"
                            print(f"[Train {train_years}] torch.compile loss failed, falling back to eager loss: {e}")
                    else:
                        loss_compile_status = "off:eager"
                    if profile_timing:
                        _log_timing(
                            f"Train {train_years} setup.torch_compile",
                            TimingBreakdown(total_s=time.perf_counter() - compile_start),
                        )
                except Exception as e:
                    model_compile_status = "fallback:eager"
                    loss_compile_status = "eager"
                    print(f"[Train {train_years}] torch.compile failed, falling back to eager: {e}")
            else:
                model_compile_status = f"skipped:{reason}"
                loss_compile_status = "eager"
                print(f"[Train {train_years}] torch.compile skipped: {reason}")
        elif compile_loss:
            loss_compile_status = "off:model_compile_disabled"
        else:
            loss_compile_status = "off:eager"
        print(
            f"[Train {train_years}] optimization status: "
            f"model_compile={model_compile_status}; "
            f"eval_model={'compiled' if eval_model is compiled_train_model and compiled_train_model is not model else 'eager'}; "
            f"loss_compile={loss_compile_status}; "
            f"backtest_compile={bool(config.training.backtest_compile)}; "
            f"backtest_stateful_compile={bool(config.training.backtest_compile_stateful)}; "
            f"backtest_prep_compile={_env_truthy('STOCKAGENT_BACKTEST_COMPILE_PREP', '1')}; "
            f"backtest_cpp_ext={bool(config.training.backtest_cpp_ext)}; "
            f"cache_train_gpu={bool(config.training.cache_train_tensors_on_gpu)}; "
            f"cache_eval_gpu={bool(config.training.cache_eval_tensors_on_gpu)}; "
            f"record_epoch_curve={record_epoch_curve}; "
            f"epoch_test_curve={run_epoch_test_curve}; "
            f"curve_plot_async={bool(config.training.curve_plot_async)}"
        )
        train_loader: DataLoader | None = None
        input_pipeline_ab_test = bool(getattr(config.training, "input_pipeline_ab_test", True))
        input_pipeline_ab_test_steps = max(1, int(getattr(config.training, "input_pipeline_ab_test_steps", 20)))
        use_dataloader = config.training.num_workers > 0
        if use_dataloader:
            if not input_pipeline_ab_test:
                use_dataloader = False
                print(
                    f"[Train {train_years}] input throughput benchmark disabled; "
                    f"prefer {'windowed tensor' if train_windowed is not None else 'tensor'} path"
                )
            elif train_windowed is not None:
                print(
                    f"[Train {train_years}] input throughput benchmark "
                    f"(location=train_setup_pre_cache, steps={input_pipeline_ab_test_steps}): "
                    "dataloader(host->device) vs windowed_tensor(row-slice+to-device)"
                )
                dl_sps, windowed_sps = _benchmark_windowed_input_pipeline_throughput(
                    train_ds=train_ds,
                    train_windowed=train_windowed,
                    train_batch_size=train_batch_size,
                    config=config,
                    device=device,
                    non_blocking=non_blocking,
                    max_steps=input_pipeline_ab_test_steps,
                )
                print(
                    f"[Train {train_years}] input throughput benchmark result: "
                    f"dataloader(host->device)={dl_sps:,.0f} samples/s vs "
                    f"windowed_tensor(row-slice+to-device)={windowed_sps:,.0f} samples/s"
                )
                # Prefer windowed unless dataloader is clearly faster.
                use_dataloader = dl_sps > (windowed_sps * 1.05)
            else:
                print(
                    f"[Train {train_years}] input throughput benchmark "
                    f"(location=train_setup_pre_cache, steps={input_pipeline_ab_test_steps}): "
                    "dataloader(host->device) vs tensor(batch-slice+to-device)"
                )
                dl_sps, tensor_sps = _benchmark_input_pipeline_throughput(
                    train_ds=train_ds,
                    train_x=train_x,
                    train_returns=train_returns,
                    train_masks=train_masks,
                    train_batch_size=train_batch_size,
                    config=config,
                    device=device,
                    non_blocking=non_blocking,
                    max_steps=input_pipeline_ab_test_steps,
                )
                print(
                    f"[Train {train_years}] input throughput benchmark result: "
                    f"dataloader(host->device)={dl_sps:,.0f} samples/s vs "
                    f"tensor(batch-slice+to-device)={tensor_sps:,.0f} samples/s"
                )
                # Prefer tensor mode unless dataloader is clearly faster.
                use_dataloader = dl_sps > (tensor_sps * 1.05)

        if use_dataloader:
            train_shuffle = not _is_return_series_objective(loss_objective)
            train_loader = _build_loader(
                train_ds,
                train_batch_size,
                train_shuffle,
                config,
                device,
                drop_last=False,
            )
            effective_workers = max(0, int(config.training.num_workers))
            print(
                f"[Train {train_years}] training mode=dataloader "
                f"(num_workers={effective_workers}, shuffle={train_shuffle})"
            )
        else:
            mode_label = "windowed tensor" if train_windowed is not None else "tensor"
            print(f"[Train {train_years}] training mode={mode_label} (num_workers={config.training.num_workers})")

        if train_loader is None:
            if train_windowed is not None:
                train_windowed = _maybe_cache_windowed_split_on_device(
                    name=f"train windowed tensors {train_years}",
                    split=train_windowed,
                    device=device,
                    enabled=bool(config.training.cache_train_tensors_on_gpu),
                    target_fraction=float(config.training.target_vram_fraction),
                    safety_margin_gb=float(config.training.vram_safety_margin_gb),
                )
            else:
                (
                    train_x,
                    train_returns,
                    train_masks,
                    train_buy_masks,
                    train_sell_masks,
                    train_benchmark,
                    train_sample_mask,
                ) = _maybe_cache_tensors_on_device(
                    name=f"train tensors {train_years}",
                    tensors=(
                        train_x,
                        train_returns,
                        train_masks,
                        train_buy_masks,
                        train_sell_masks,
                        train_benchmark,
                        train_sample_mask,
                    ),
                    device=device,
                    enabled=bool(config.training.cache_train_tensors_on_gpu),
                    target_fraction=float(config.training.target_vram_fraction),
                    safety_margin_gb=float(config.training.vram_safety_margin_gb),
                )

        if combined_val_windowed is not None:
            combined_val_windowed_shared = _maybe_share_windowed_base_from_cached(
                name=f"validation windowed tensors {train_years}",
                split=combined_val_windowed,
                device=device,
                non_blocking=non_blocking,
                enabled=bool(config.training.cache_eval_tensors_on_gpu),
                cached_base=train_windowed,
            )
            if combined_val_windowed_shared is not None:
                combined_val_windowed = combined_val_windowed_shared
            else:
                combined_val_windowed = _maybe_cache_windowed_split_on_device(
                    name=f"validation windowed tensors {train_years}",
                    split=combined_val_windowed,
                    device=device,
                    enabled=bool(config.training.cache_eval_tensors_on_gpu),
                    target_fraction=float(config.training.target_vram_fraction),
                    safety_margin_gb=float(config.training.vram_safety_margin_gb),
                )
        else:
            (
                combined_val_x,
                combined_val_returns,
                combined_val_masks,
                combined_val_buy_masks,
                combined_val_sell_masks,
                combined_val_bench,
            ) = _maybe_cache_tensors_on_device(
                name=f"validation tensors {train_years}",
                tensors=(
                    combined_val_x,
                    combined_val_returns,
                    combined_val_masks,
                    combined_val_buy_masks,
                    combined_val_sell_masks,
                    combined_val_bench,
                ),
                device=device,
                enabled=bool(config.training.cache_eval_tensors_on_gpu),
                target_fraction=float(config.training.target_vram_fraction),
                safety_margin_gb=float(config.training.vram_safety_margin_gb),
            )
        if combined_test_windowed is not None:
            combined_test_windowed_shared = _maybe_share_windowed_base_from_cached(
                name=f"test windowed tensors {train_years}",
                split=combined_test_windowed,
                device=device,
                non_blocking=non_blocking,
                enabled=bool(config.training.cache_eval_tensors_on_gpu),
                cached_base=train_windowed if train_windowed is not None else combined_val_windowed,
            )
            if combined_test_windowed_shared is not None:
                combined_test_windowed = combined_test_windowed_shared
            else:
                combined_test_windowed = _maybe_cache_windowed_split_on_device(
                    name=f"test windowed tensors {train_years}",
                    split=combined_test_windowed,
                    device=device,
                    enabled=bool(config.training.cache_eval_tensors_on_gpu),
                    target_fraction=float(config.training.target_vram_fraction),
                    safety_margin_gb=float(config.training.vram_safety_margin_gb),
                )
        else:
            (
                combined_test_x,
                combined_test_returns,
                combined_test_masks,
                combined_test_buy_masks,
                combined_test_sell_masks,
                combined_test_bench,
            ) = _maybe_cache_tensors_on_device(
                name=f"test tensors {train_years}",
                tensors=(
                    combined_test_x,
                    combined_test_returns,
                    combined_test_masks,
                    combined_test_buy_masks,
                    combined_test_sell_masks,
                    combined_test_bench,
                ),
                device=device,
                enabled=bool(config.training.cache_eval_tensors_on_gpu),
                target_fraction=float(config.training.target_vram_fraction),
                safety_margin_gb=float(config.training.vram_safety_margin_gb),
            )

        if _should_profile_train_step():
            profile_batch: dict[str, torch.Tensor] | None = None
            try:
                if train_loader is not None:
                    raw_batch = next(iter(train_loader))
                    profile_batch = _move_batch(raw_batch, device, non_blocking)
                elif train_windowed is not None:
                    slice_end = min(train_batch_size, len(train_windowed))
                    if slice_end > 0:
                        profile_batch = train_windowed.batch_by_rows(
                            0,
                            slice_end,
                            device=device,
                            non_blocking=non_blocking,
                        )
                else:
                    slice_end = min(train_batch_size, int(train_x.size(0)))
                    if slice_end > 0:
                        profile_batch = {
                            "x": train_x[:slice_end].to(device=device, non_blocking=non_blocking),
                            "future_log_returns": train_returns[:slice_end].to(device=device, non_blocking=non_blocking),
                            "tradable_mask": train_masks[:slice_end].to(device=device, non_blocking=non_blocking),
                            "can_buy_mask": train_buy_masks[:slice_end].to(device=device, non_blocking=non_blocking),
                            "can_sell_mask": train_sell_masks[:slice_end].to(device=device, non_blocking=non_blocking),
                        }
                        if train_benchmark is not None:
                            profile_batch["benchmark"] = train_benchmark[:slice_end].to(device=device, non_blocking=non_blocking)
                        if train_sample_mask is not None:
                            profile_batch["sample_mask"] = train_sample_mask[:slice_end].to(device=device, non_blocking=non_blocking)
                if profile_batch is not None:
                    _profile_single_train_step(
                        model=compiled_train_model,
                        loss_fn=compiled_loss_fn,
                        batch=profile_batch,
                        device=device,
                        amp_dtype=amp_dtype,
                        long_only=config.trading.long_only,
                        buy_fee_rate=config.trading.buy_fee_rate,
                        sell_fee_rate=config.trading.sell_fee_rate,
                        max_turnover_ratio=config.trading.max_turnover_ratio,
                        gross_leverage=config.trading.gross_leverage,
                        gamma_sharpe=config.evaluation.gamma_sharpe,
                        gamma_excess=config.evaluation.gamma_excess,
                        gamma_cvar=config.evaluation.gamma_cvar,
                        cvar_alpha=config.evaluation.cvar_alpha,
                        gamma_drawdown=config.evaluation.gamma_drawdown,
                        drawdown_target=config.evaluation.drawdown_target,
                        gamma_turnover=config.evaluation.gamma_turnover,
                        gamma_underperformance=config.evaluation.gamma_underperformance,
                        excess_target=config.evaluation.excess_target,
                        cvar_budget=config.evaluation.cvar_budget,
                        drawdown_budget=config.evaluation.drawdown_budget,
                        turnover_budget=config.evaluation.turnover_budget,
                        gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                        gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                        gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                        objective=loss_objective,
                        rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                        direction_weight=config.training.multitask_loss.direction_weight,
                        volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                        concentration_weight=config.training.multitask_loss.concentration_weight,
                        regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                        regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                        fold_id=fold.fold_id,
                    )
            except Exception as e:
                print(f"[torch.profiler] single-step profiling failed: {e}")

        early_stop_ratio = max(0.0, float(config.training.early_stopping_no_improve_ratio))
        early_stop_patience = int(np.ceil(config.training.epochs * early_stop_ratio))
        val_interval = max(1, int(config.training.val_interval_epochs))
        print(f"[Train {train_years}] validation interval={val_interval} epoch(s)")
        no_improve_epochs = 0
        last_epoch = start_epoch - 1
        if early_stop_patience > 0:
            print(
                f"[Train {train_years}] early stopping enabled: "
                f"patience={early_stop_patience} epochs "
                f"(ratio={early_stop_ratio:.2f})"
            )
        print(f"Train {train_years}")
        epoch_pbar = tqdm(
            range(start_epoch, config.training.epochs + 1),
            desc=" Epochs",
            leave=True,
            dynamic_ncols=True,
        )
        val_backtest: BacktestResultTensor | None = None
        test_mean_best_by_val: float | None = None
        get_backtest_compile_stats(reset=True)
        get_backtest_prep_compile_stats(reset=True)
        get_backtest_runtime_stats(reset=True)
        get_loss_runtime_stats(reset=True)

        def _run_one_train_epoch(train_model: nn.Module) -> tuple[torch.Tensor, TimingBreakdown]:
            if train_loader is not None:
                return _train_epoch(
                    train_model,
                    compiled_loss_fn,
                    train_loader,
                    optimizer,
                    scaler,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    config.trading.gross_leverage,
                    config.evaluation.gamma_sharpe,
                    config.evaluation.gamma_excess,
                    config.evaluation.gamma_cvar,
                    config.evaluation.cvar_alpha,
                    config.evaluation.gamma_drawdown,
                    config.evaluation.drawdown_target,
                    config.evaluation.gamma_turnover,
                    config.evaluation.gamma_underperformance,
                    config.evaluation.excess_target,
                    config.evaluation.cvar_budget,
                    config.evaluation.drawdown_budget,
                    config.evaluation.turnover_budget,
                    config.evaluation.gamma_cvar_budget,
                    config.evaluation.gamma_drawdown_budget,
                    config.evaluation.gamma_turnover_budget,
                    loss_objective,
                    config.training.grad_clip_norm,
                    finite_check_interval_steps=config.training.finite_check_interval_steps,
                    rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                    direction_weight=config.training.multitask_loss.direction_weight,
                    volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                    concentration_weight=config.training.multitask_loss.concentration_weight,
                    regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                    regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                    factor_aug_kwargs=factor_aug_kwargs,
                    profile_timing=profile_timing,
                )
            if train_windowed is not None:
                return _train_epoch_windowed_tensor(
                    train_model,
                    compiled_loss_fn,
                    train_windowed,
                    optimizer,
                    scaler,
                    batch_size=train_batch_size,
                    device=device,
                    amp_dtype=amp_dtype,
                    non_blocking=non_blocking,
                    long_only=config.trading.long_only,
                    buy_fee_rate=config.trading.buy_fee_rate,
                    sell_fee_rate=config.trading.sell_fee_rate,
                    max_turnover_ratio=config.trading.max_turnover_ratio,
                    gross_leverage=config.trading.gross_leverage,
                    gamma_sharpe=config.evaluation.gamma_sharpe,
                    gamma_excess=config.evaluation.gamma_excess,
                    gamma_cvar=config.evaluation.gamma_cvar,
                    cvar_alpha=config.evaluation.cvar_alpha,
                    gamma_drawdown=config.evaluation.gamma_drawdown,
                    drawdown_target=config.evaluation.drawdown_target,
                    gamma_turnover=config.evaluation.gamma_turnover,
                    gamma_underperformance=config.evaluation.gamma_underperformance,
                    excess_target=config.evaluation.excess_target,
                    cvar_budget=config.evaluation.cvar_budget,
                    drawdown_budget=config.evaluation.drawdown_budget,
                    turnover_budget=config.evaluation.turnover_budget,
                    gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                    gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                    gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                    objective=loss_objective,
                    grad_clip_norm=config.training.grad_clip_norm,
                    finite_check_interval_steps=config.training.finite_check_interval_steps,
                    rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                    direction_weight=config.training.multitask_loss.direction_weight,
                    volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                    concentration_weight=config.training.multitask_loss.concentration_weight,
                    regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                    regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                    factor_aug_kwargs=factor_aug_kwargs,
                    profile_timing=profile_timing,
                )
            return _train_epoch_tensor(
                train_model,
                compiled_loss_fn,
                train_x,
                train_returns,
                train_masks,
                train_buy_masks,
                train_sell_masks,
                train_benchmark,
                train_sample_mask,
                optimizer,
                scaler,
                batch_size=train_batch_size,
                device=device,
                amp_dtype=amp_dtype,
                non_blocking=non_blocking,
                long_only=config.trading.long_only,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                gross_leverage=config.trading.gross_leverage,
                gamma_sharpe=config.evaluation.gamma_sharpe,
                gamma_excess=config.evaluation.gamma_excess,
                gamma_cvar=config.evaluation.gamma_cvar,
                cvar_alpha=config.evaluation.cvar_alpha,
                gamma_drawdown=config.evaluation.gamma_drawdown,
                drawdown_target=config.evaluation.drawdown_target,
                gamma_turnover=config.evaluation.gamma_turnover,
                gamma_underperformance=config.evaluation.gamma_underperformance,
                excess_target=config.evaluation.excess_target,
                cvar_budget=config.evaluation.cvar_budget,
                drawdown_budget=config.evaluation.drawdown_budget,
                turnover_budget=config.evaluation.turnover_budget,
                gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                objective=loss_objective,
                grad_clip_norm=config.training.grad_clip_norm,
                finite_check_interval_steps=config.training.finite_check_interval_steps,
                rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                direction_weight=config.training.multitask_loss.direction_weight,
                volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                concentration_weight=config.training.multitask_loss.concentration_weight,
                regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                factor_aug_kwargs=factor_aug_kwargs,
                profile_timing=profile_timing,
            )

        for epoch in epoch_pbar:
            epoch_start = time.perf_counter()
            last_epoch = epoch
            get_backtest_compile_stats(reset=True)
            get_backtest_prep_compile_stats(reset=True)
            get_backtest_runtime_stats(reset=True)
            get_loss_runtime_stats(reset=True)
            train_loss_t, train_timing = _run_one_train_epoch(compiled_train_model)
            train_bt_runtime_after = get_backtest_runtime_stats()
            train_loss_runtime_after = get_loss_runtime_stats()

            should_validate = (
                epoch == start_epoch
                or (epoch % val_interval == 0)
                or (epoch == config.training.epochs)
            )
            if not should_validate:
                scheduler_total = 0.0
                if scheduler is not None and not scheduler_requires_metric:
                    scheduler_start = time.perf_counter()
                    scheduler.step()
                    scheduler_total = time.perf_counter() - scheduler_start
                scalar_sync_start = time.perf_counter()
                train_loss = float(train_loss_t.detach().float().cpu())
                scalar_sync_total = time.perf_counter() - scalar_sync_start
                progress_update_start = time.perf_counter()
                bt_stats_after = get_backtest_compile_stats()
                bt_prep_stats_after = get_backtest_prep_compile_stats()
                bt_runtime_after = get_backtest_runtime_stats()
                bt_nonhit_total = (
                    bt_stats_after["misses"]
                    + bt_stats_after["failures"]
                    + bt_stats_after["disabled"]
                )
                loss_compile_fallback = int(
                    isinstance(compiled_loss_fn, _CompiledLossFallback) and compiled_loss_fn.disabled
                )
                epoch_pbar.set_postfix(
                    {
                        "train_loss": f"{train_loss:.8f}",
                        "val_mean": "-",
                        "best_val": f"{min(c.best_val_loss for c in fold_contexts.values()):.6f}",
                        "no_improve": no_improve_epochs,
                        "lr": f"{optimizer.param_groups[0]['lr']:.2e}",
                        "bt": (
                            f"{bt_stats_after['hits']}/"
                            f"{bt_stats_after['misses']}/"
                            f"{bt_stats_after['failures']}/"
                            f"{bt_stats_after['disabled']}"
                        ),
                        "bt_nonhit": bt_nonhit_total,
                        "loss_fb": loss_compile_fallback,
                    }
                )
                progress_update_total = time.perf_counter() - progress_update_start
                if profile_timing:
                    _log_timing(
                        f"Train {train_years} epoch {epoch} (train-only)",
                        train_timing,
                    )
                cuda_sync_total = _sync_cuda_for_timing(device)
                request_plot = (
                    epoch == start_epoch
                    or (epoch % curve_plot_request_interval == 0)
                    or (epoch == config.training.epochs)
                )
                curve_record_total = _record_epoch_curve(
                    {
                        "epoch": int(epoch),
                        "train_loss": float(train_loss),
                        "val_mean": None,
                        "test_mean": test_mean_best_by_val,
                        "lr": float(optimizer.param_groups[0]["lr"]),
                        **_timing_curve_payload(
                            train_timing=train_timing,
                            scheduler_s=scheduler_total,
                            progress_update_s=progress_update_total,
                            cuda_sync_s=cuda_sync_total,
                            scalar_sync_s=scalar_sync_total,
                            epoch_wall_s=time.perf_counter() - epoch_start,
                            timing_synchronized=(device.type != "cuda" or profile_timing),
                            backtest_compile_stats=bt_stats_after,
                            backtest_prep_compile_stats=bt_prep_stats_after,
                            backtest_runtime_stats=bt_runtime_after,
                            train_backtest_runtime_stats=train_bt_runtime_after,
                            loss_runtime_stats=train_loss_runtime_after,
                        ),
                    },
                    request_plot=request_plot,
                )
                if profile_timing and curve_record_total > 0.0:
                    _log_timing(
                        f"Train {train_years} epoch {epoch} curve_record",
                        TimingBreakdown(plot_s=curve_record_total, total_s=curve_record_total),
                    )
                continue

            val_losses: list[float] = []
            val_ics: list[float] = []
            any_fold_improved = False
            fold_ckpt_total = 0.0
            group_ckpt_total = 0.0
            scheduler_total = 0.0
            progress_update_total = 0.0
            curve_record_total = 0.0
            scalar_sync_total = 0.0
            cuda_sync_total = 0.0
            gc_total = 0.0
            val_timing = TimingBreakdown()
            test_curve_timing = TimingBreakdown()
            deferred_val_loss_contexts: list[FoldRuntimeContext] = []
            deferred_val_loss_tensors: torch.Tensor | None = None
            deferred_test_loss_tensors: torch.Tensor | None = None
            if loss_objective in {"rank_ic", "pure_rank", "factor_generalization", "portfolio_autoencoder"}:
                val_eval_start = time.perf_counter()
                eval_model.eval()
                with torch.inference_mode():
                    for index, (_, context) in enumerate(fold_contexts.items()):
                        start = val_offsets[index]
                        end = val_offsets[index + 1]

                        val_loss, val_ic, val_fold_timing = _evaluate_rank_ic_multitask_loss(
                            eval_model,
                            combined_val_x[start:end],
                            combined_val_returns[start:end],
                            combined_val_masks[start:end],
                            combined_val_buy_masks[start:end],
                            combined_val_sell_masks[start:end],
                            combined_val_bench[start:end],
                            device,
                            amp_dtype,
                            non_blocking,
                            chunk_rows=eval_chunk_rows,
                            objective=loss_objective,
                            long_only=config.trading.long_only,
                            buy_fee_rate=config.trading.buy_fee_rate,
                            sell_fee_rate=config.trading.sell_fee_rate,
                            max_turnover_ratio=config.trading.max_turnover_ratio,
                            gross_leverage=config.trading.gross_leverage,
                            gamma_sharpe=config.evaluation.gamma_sharpe,
                            gamma_excess=config.evaluation.gamma_excess,
                            gamma_cvar=config.evaluation.gamma_cvar,
                            cvar_alpha=config.evaluation.cvar_alpha,
                            gamma_drawdown=config.evaluation.gamma_drawdown,
                            drawdown_target=config.evaluation.drawdown_target,
                            gamma_turnover=config.evaluation.gamma_turnover,
                            gamma_underperformance=config.evaluation.gamma_underperformance,
                            excess_target=config.evaluation.excess_target,
                            cvar_budget=config.evaluation.cvar_budget,
                            drawdown_budget=config.evaluation.drawdown_budget,
                            turnover_budget=config.evaluation.turnover_budget,
                            gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                            gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                            gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                            rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                            direction_weight=config.training.multitask_loss.direction_weight,
                            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                            concentration_weight=config.training.multitask_loss.concentration_weight,
                            regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                            regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                            factor_loss_kwargs=risk_loss_kwargs,
                        )
                        _add_timing(val_timing, val_fold_timing)
                        val_losses.append(val_loss)
                        if val_ic is not None:
                            val_ics.append(val_ic)

                        if val_loss < context.best_val_loss:
                            any_fold_improved = True
                            context.best_val_loss = val_loss
                            fold_ckpt_start = time.perf_counter()
                            _save_fold_checkpoint(
                                context.checkpoint_best_path,
                                fold=context.fold,
                                epoch=epoch,
                                best_val_loss=val_loss,
                                model=model,
                                optimizer=optimizer,
                                scaler=scaler,
                            )
                            fold_ckpt_total += time.perf_counter() - fold_ckpt_start
                val_eval_total = max(0.0, time.perf_counter() - val_eval_start - fold_ckpt_total)
                val_loss_total = 0.0
            else:
                val_eval_start = time.perf_counter()
                if combined_val_windowed is not None:
                    val_backtest_epoch, _, _ = _evaluate_windowed_tensor_batch(
                        eval_model,
                        combined_val_windowed,
                        device,
                        amp_dtype,
                        non_blocking,
                        config.trading.long_only,
                        config.trading.buy_fee_rate,
                        config.trading.sell_fee_rate,
                        config.trading.max_turnover_ratio,
                        config.trading.gross_leverage,
                        chunk_rows=eval_chunk_rows,
                        backtest_chunk_rows=eval_backtest_chunk_rows,
                        compute_ic=False,
                        compute_metrics_summary=False,
                        return_weights_history=False,
                        profile_timing=profile_timing,
                        timing_out=val_timing,
                        reset_at_rows=val_offsets,
                    )
                else:
                    val_backtest_epoch, _, _ = _evaluate_tensor_batch(
                        eval_model,
                        combined_val_x,
                        combined_val_returns,
                        combined_val_masks,
                        combined_val_buy_masks,
                        combined_val_sell_masks,
                        combined_val_bench,
                        device,
                        amp_dtype,
                        non_blocking,
                        config.trading.long_only,
                        config.trading.buy_fee_rate,
                        config.trading.sell_fee_rate,
                        config.trading.max_turnover_ratio,
                        config.trading.gross_leverage,
                        chunk_rows=eval_chunk_rows,
                        backtest_chunk_rows=eval_backtest_chunk_rows,
                        compute_ic=False,
                        compute_metrics_summary=False,
                        return_weights_history=False,
                        profile_timing=profile_timing,
                        timing_out=val_timing,
                        reset_at_rows=val_offsets,
                    )
                val_eval_total = time.perf_counter() - val_eval_start

                val_loss_start = time.perf_counter()
                deferred_val_loss_contexts = list(fold_contexts.values())
                deferred_val_loss_tensors = _batched_loss_from_backtest_segments(
                    val_backtest_epoch.strategy_returns,
                    val_backtest_epoch.benchmark_returns,
                    val_backtest_epoch.turnovers,
                    val_offsets,
                    gamma_sharpe=config.evaluation.gamma_sharpe,
                    gamma_excess=config.evaluation.gamma_excess,
                    gamma_cvar=config.evaluation.gamma_cvar,
                    cvar_alpha=config.evaluation.cvar_alpha,
                    gamma_drawdown=config.evaluation.gamma_drawdown,
                    drawdown_target=config.evaluation.drawdown_target,
                    gamma_turnover=config.evaluation.gamma_turnover,
                    gamma_underperformance=config.evaluation.gamma_underperformance,
                    excess_target=config.evaluation.excess_target,
                    cvar_budget=config.evaluation.cvar_budget,
                    drawdown_budget=config.evaluation.drawdown_budget,
                    turnover_budget=config.evaluation.turnover_budget,
                    gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                    gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                    gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                    objective=loss_objective,
                )
                val_loss_total = time.perf_counter() - val_loss_start

            curve_test_interval = max(1, int(getattr(config.training, "curve_test_interval", 100)))
            should_compute_test_mean = (
                run_epoch_test_curve
                and (
                    epoch == start_epoch
                    or (epoch % curve_test_interval == 0)
                    or (epoch == config.training.epochs)
                )
            )
            sampled_test_mean: float | None = None
            curve_test_total = 0.0
            test_loss_total = 0.0
            test_losses_epoch: list[float] = []
            if should_compute_test_mean:
                curve_test_start = time.perf_counter()
                if loss_objective in {"rank_ic", "pure_rank", "factor_generalization", "portfolio_autoencoder"}:
                    eval_model.eval()
                    with torch.inference_mode():
                        start = curve_test_start_row
                        end = curve_test_end_row
                        test_loss, _, test_fold_timing = _evaluate_rank_ic_multitask_loss(
                            eval_model,
                            combined_test_x[start:end],
                            combined_test_returns[start:end],
                            combined_test_masks[start:end],
                            combined_test_buy_masks[start:end],
                            combined_test_sell_masks[start:end],
                            combined_test_bench[start:end],
                            device,
                            amp_dtype,
                            non_blocking,
                            chunk_rows=eval_chunk_rows,
                            objective=loss_objective,
                            long_only=config.trading.long_only,
                            buy_fee_rate=config.trading.buy_fee_rate,
                            sell_fee_rate=config.trading.sell_fee_rate,
                            max_turnover_ratio=config.trading.max_turnover_ratio,
                            gross_leverage=config.trading.gross_leverage,
                            gamma_sharpe=config.evaluation.gamma_sharpe,
                            gamma_excess=config.evaluation.gamma_excess,
                            gamma_cvar=config.evaluation.gamma_cvar,
                            cvar_alpha=config.evaluation.cvar_alpha,
                            gamma_drawdown=config.evaluation.gamma_drawdown,
                            drawdown_target=config.evaluation.drawdown_target,
                            gamma_turnover=config.evaluation.gamma_turnover,
                            gamma_underperformance=config.evaluation.gamma_underperformance,
                            excess_target=config.evaluation.excess_target,
                            cvar_budget=config.evaluation.cvar_budget,
                            drawdown_budget=config.evaluation.drawdown_budget,
                            turnover_budget=config.evaluation.turnover_budget,
                            gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                            gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                            gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                            rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                            direction_weight=config.training.multitask_loss.direction_weight,
                            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                            concentration_weight=config.training.multitask_loss.concentration_weight,
                            regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                            regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                            factor_loss_kwargs=risk_loss_kwargs,
                        )
                        _add_timing(test_curve_timing, test_fold_timing)
                        test_losses_epoch.append(test_loss)
                else:
                    if combined_test_windowed is not None:
                        test_backtest_epoch, _, _ = _evaluate_windowed_tensor_batch(
                            eval_model,
                            combined_test_windowed,
                            device,
                            amp_dtype,
                            non_blocking,
                            config.trading.long_only,
                            config.trading.buy_fee_rate,
                            config.trading.sell_fee_rate,
                            config.trading.max_turnover_ratio,
                            config.trading.gross_leverage,
                            chunk_rows=eval_chunk_rows,
                            backtest_chunk_rows=eval_backtest_chunk_rows,
                            compute_ic=False,
                            compute_metrics_summary=False,
                            return_weights_history=False,
                            profile_timing=False,
                            timing_out=test_curve_timing,
                            reset_at_rows=curve_test_offsets,
                        )
                    else:
                        test_backtest_epoch, _, _ = _evaluate_tensor_batch(
                            eval_model,
                            combined_test_x[curve_test_start_row:curve_test_end_row],
                            combined_test_returns[curve_test_start_row:curve_test_end_row],
                            combined_test_masks[curve_test_start_row:curve_test_end_row],
                            combined_test_buy_masks[curve_test_start_row:curve_test_end_row],
                            combined_test_sell_masks[curve_test_start_row:curve_test_end_row],
                            combined_test_bench[curve_test_start_row:curve_test_end_row],
                            device,
                            amp_dtype,
                            non_blocking,
                            config.trading.long_only,
                            config.trading.buy_fee_rate,
                            config.trading.sell_fee_rate,
                            config.trading.max_turnover_ratio,
                            config.trading.gross_leverage,
                            chunk_rows=eval_chunk_rows,
                            backtest_chunk_rows=eval_backtest_chunk_rows,
                            compute_ic=False,
                            compute_metrics_summary=False,
                            return_weights_history=False,
                            profile_timing=False,
                            timing_out=test_curve_timing,
                            reset_at_rows=curve_test_offsets,
                        )
                    test_loss_start = time.perf_counter()
                    deferred_test_loss_tensors = _batched_loss_from_backtest_segments(
                        test_backtest_epoch.strategy_returns,
                        test_backtest_epoch.benchmark_returns,
                        test_backtest_epoch.turnovers,
                        curve_test_offsets,
                        gamma_sharpe=config.evaluation.gamma_sharpe,
                        gamma_excess=config.evaluation.gamma_excess,
                        gamma_cvar=config.evaluation.gamma_cvar,
                        cvar_alpha=config.evaluation.cvar_alpha,
                        gamma_drawdown=config.evaluation.gamma_drawdown,
                        drawdown_target=config.evaluation.drawdown_target,
                        gamma_turnover=config.evaluation.gamma_turnover,
                        gamma_underperformance=config.evaluation.gamma_underperformance,
                        excess_target=config.evaluation.excess_target,
                        cvar_budget=config.evaluation.cvar_budget,
                        drawdown_budget=config.evaluation.drawdown_budget,
                        turnover_budget=config.evaluation.turnover_budget,
                        gamma_cvar_budget=config.evaluation.gamma_cvar_budget,
                        gamma_drawdown_budget=config.evaluation.gamma_drawdown_budget,
                        gamma_turnover_budget=config.evaluation.gamma_turnover_budget,
                        objective=loss_objective,
                    )
                    test_loss_total = time.perf_counter() - test_loss_start
                curve_test_total = time.perf_counter() - curve_test_start

            scalar_sync_start = time.perf_counter()
            scalar_parts = [train_loss_t.detach().float().reshape(1)]
            val_count = int(deferred_val_loss_tensors.numel()) if deferred_val_loss_tensors is not None else 0
            test_count = int(deferred_test_loss_tensors.numel()) if deferred_test_loss_tensors is not None else 0
            if val_count > 0 and deferred_val_loss_tensors is not None:
                scalar_parts.append(deferred_val_loss_tensors.detach().float().reshape(-1))
            if test_count > 0 and deferred_test_loss_tensors is not None:
                scalar_parts.append(deferred_test_loss_tensors.detach().float().reshape(-1))
            scalar_values = torch.cat(scalar_parts).cpu().tolist()
            scalar_sync_total += time.perf_counter() - scalar_sync_start

            scalar_offset = 0
            train_loss = float(scalar_values[scalar_offset])
            scalar_offset += 1
            if val_count > 0:
                val_loss_values = scalar_values[scalar_offset : scalar_offset + val_count]
                scalar_offset += val_count
                for context, val_loss_raw in zip(deferred_val_loss_contexts, val_loss_values, strict=False):
                    val_loss = float(val_loss_raw)
                    val_losses.append(val_loss)
                    if val_loss < context.best_val_loss:
                        any_fold_improved = True
                        context.best_val_loss = val_loss
                        fold_ckpt_start = time.perf_counter()
                        _save_fold_checkpoint(
                            context.checkpoint_best_path,
                            fold=context.fold,
                            epoch=epoch,
                            best_val_loss=val_loss,
                            model=model,
                            optimizer=optimizer,
                            scaler=scaler,
                        )
                        fold_ckpt_total += time.perf_counter() - fold_ckpt_start
            if test_count > 0:
                test_loss_values = scalar_values[scalar_offset : scalar_offset + test_count]
                test_losses_epoch.extend(float(value) for value in test_loss_values)
            val_mean_loss = float(np.mean(val_losses)) if val_losses else float("nan")
            if should_compute_test_mean:
                sampled_test_mean = float(np.mean(test_losses_epoch)) if test_losses_epoch else None
                test_mean_best_by_val = sampled_test_mean

            if any_fold_improved:
                group_ckpt_start = time.perf_counter()
                _save_group_checkpoint(
                    group_checkpoint_path,
                    train_years=train_years,
                    epoch=epoch,
                    model=model,
                    optimizer=optimizer,
                    scaler=scaler,
                )
                group_ckpt_total = time.perf_counter() - group_ckpt_start
            else:
                group_ckpt_total = 0.0

            if val_losses:
                if any_fold_improved:
                    no_improve_epochs = 0
                else:
                    no_improve_epochs += 1

                if scheduler is not None:
                    scheduler_start = time.perf_counter()
                    if scheduler_requires_metric:
                        scheduler.step(float(np.mean(val_losses)))
                    else:
                        scheduler.step()
                    scheduler_total = time.perf_counter() - scheduler_start

                progress_update_start = time.perf_counter()
                bt_stats_after = get_backtest_compile_stats()
                bt_prep_stats_after = get_backtest_prep_compile_stats()
                bt_runtime_after = get_backtest_runtime_stats()
                bt_nonhit_total = (
                    bt_stats_after["misses"]
                    + bt_stats_after["failures"]
                    + bt_stats_after["disabled"]
                )
                loss_compile_fallback = int(
                    isinstance(compiled_loss_fn, _CompiledLossFallback) and compiled_loss_fn.disabled
                )

                epoch_pbar.set_postfix(
                    {
                        "train_loss": f"{train_loss:.8f}",
                        "val_loss": f"{val_mean_loss:.8f}",
                        "test_loss": f"{float(np.mean(test_losses_epoch)):.8f}" if test_losses_epoch else "-",
                        "best_val": f"{min(c.best_val_loss for c in fold_contexts.values()):.8f}",
                        "no_improve": no_improve_epochs,
                        "lr": f"{optimizer.param_groups[0]['lr']:.2e}",
                        "bt": (
                            f"{bt_stats_after['hits']}/"
                            f"{bt_stats_after['misses']}/"
                            f"{bt_stats_after['failures']}/"
                            f"{bt_stats_after['disabled']}"
                        ),
                        "bt_nonhit": bt_nonhit_total,
                        "loss_fb": loss_compile_fallback,
                    }
                )
                progress_update_total = time.perf_counter() - progress_update_start

                if profile_timing:
                    _log_timing(
                        f"Train {train_years} epoch {epoch}",
                        TimingBreakdown(
                            total_s=(
                                train_timing.total_s
                                + val_eval_total
                                + val_loss_total
                                + curve_test_total
                                + fold_ckpt_total
                                + group_ckpt_total
                                + scheduler_total
                                + progress_update_total
                                + scalar_sync_total
                            ),
                            fetch_s=train_timing.fetch_s,
                            transfer_s=train_timing.transfer_s,
                            forward_s=train_timing.forward_s,
                            model_forward_s=train_timing.model_forward_s,
                            factor_aug_s=train_timing.factor_aug_s,
                            loss_s=train_timing.loss_s,
                            backward_s=train_timing.backward_s,
                            grad_s=train_timing.grad_s,
                            clip_s=train_timing.clip_s,
                            finite_check_s=train_timing.finite_check_s,
                            step_s=train_timing.step_s,
                            backtest_s=val_eval_total,
                            metrics_s=val_loss_total,
                            save_s=fold_ckpt_total + group_ckpt_total,
                            batches=train_timing.batches,
                        ),
                    )

                cuda_sync_total = _sync_cuda_for_timing(device)
                request_plot = (
                    epoch == start_epoch
                    or (epoch % curve_plot_request_interval == 0)
                    or (epoch == config.training.epochs)
                )
                curve_record_total = _record_epoch_curve(
                    {
                        "epoch": int(epoch),
                        "train_loss": float(train_loss),
                        "val_mean": val_mean_loss,
                        "test_mean": test_mean_best_by_val,
                        "lr": float(optimizer.param_groups[0]["lr"]),
                        **_timing_curve_payload(
                            train_timing=train_timing,
                            val_timing=val_timing,
                            test_curve_timing=test_curve_timing,
                            val_eval_s=val_eval_total,
                            val_loss_s=val_loss_total,
                            test_curve_s=curve_test_total,
                            test_curve_loss_s=test_loss_total,
                            fold_checkpoint_save_s=fold_ckpt_total,
                            group_checkpoint_save_s=group_ckpt_total,
                            scheduler_s=scheduler_total,
                            progress_update_s=progress_update_total,
                            cuda_sync_s=cuda_sync_total,
                            scalar_sync_s=scalar_sync_total,
                            gc_s=gc_total,
                            epoch_wall_s=time.perf_counter() - epoch_start,
                            timing_synchronized=(device.type != "cuda" or profile_timing),
                            backtest_compile_stats=bt_stats_after,
                            backtest_prep_compile_stats=bt_prep_stats_after,
                            backtest_runtime_stats=bt_runtime_after,
                            train_backtest_runtime_stats=train_bt_runtime_after,
                            loss_runtime_stats=train_loss_runtime_after,
                        ),
                    },
                    request_plot=request_plot,
                )
                if profile_timing and curve_record_total > 0.0:
                    _log_timing(
                        f"Train {train_years} epoch {epoch} curve_record",
                        TimingBreakdown(plot_s=curve_record_total, total_s=curve_record_total),
                    )

                if early_stop_patience > 0 and no_improve_epochs > early_stop_patience:
                    print(
                        f"[Train {train_years}] early stop at epoch {epoch}: "
                        f"no improvement for {no_improve_epochs} epochs "
                        f"(patience={early_stop_patience})"
                    )
                    break

        curve_flush_start = time.perf_counter()
        if curve_plotter is not None:
            if defer_epoch_curve_plot_until_end:
                curve_plotter.request()
            curve_plotter.flush()
        curve_flush_total = time.perf_counter() - curve_flush_start
        if profile_timing and curve_flush_total > 0.0:
            _log_timing(
                f"Train {train_years} setup.curve_plot_flush",
                TimingBreakdown(plot_s=curve_flush_total, total_s=curve_flush_total),
            )

        # In eval-only mode (e.g., resumed checkpoint already beyond max epochs),
        # the epoch loop does not run; compute validation backtest once for reporting.
        if val_backtest is None:
            eval_only_start = time.perf_counter()
            if combined_val_windowed is not None:
                val_backtest, _, _ = _evaluate_windowed_tensor_batch(
                    eval_model,
                    combined_val_windowed,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    config.trading.gross_leverage,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                    progress_label=f"[Train {train_years} final-val]",
                    reset_at_rows=val_offsets,
                )
            else:
                val_backtest, _, _ = _evaluate_tensor_batch(
                    eval_model,
                    combined_val_x,
                    combined_val_returns,
                    combined_val_masks,
                    combined_val_buy_masks,
                    combined_val_sell_masks,
                    combined_val_bench,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    config.trading.gross_leverage,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                    progress_label=f"[Train {train_years} final-val]",
                    reset_at_rows=val_offsets,
                )
            if profile_timing:
                _log_timing(
                    f"Train {train_years} final.val_eval_only",
                    TimingBreakdown(total_s=time.perf_counter() - eval_only_start),
                )
        if val_backtest is None:
            raise RuntimeError("Validation backtest is unavailable in eval stage.")

        if combined_val_windowed is not None:
            val_date_idx = combined_val_windowed.valid_indices.to(
                device=combined_val_windowed.future_log_returns.device,
                dtype=torch.long,
            )
            val_returns_device = combined_val_windowed.future_log_returns[val_date_idx].to(
                device=val_backtest.weights_history.device,
                non_blocking=False,
            )
            val_masks_device = combined_val_windowed.tradable_mask[val_date_idx].to(
                device=val_backtest.weights_history.device,
                non_blocking=False,
            )
        else:
            val_returns_device = combined_val_returns.to(
                device=val_backtest.weights_history.device,
                non_blocking=False,
            )
            val_masks_device = combined_val_masks.to(
                device=val_backtest.weights_history.device,
                non_blocking=False,
            )

        for index, (_, context) in enumerate(fold_contexts.items()):
            fold = context.fold
            fold_dir = context.fold_dir
            best_checkpoint_path = context.checkpoint_best_path
            if best_checkpoint_path.exists():
                checkpoint = _load_checkpoint(best_checkpoint_path)
                _load_state_dict(model, checkpoint["model_state_dict"])
                best_val_loss = float(checkpoint.get("best_val_loss", context.best_val_loss))
            else:
                best_val_loss = context.best_val_loss

            test_eval_start = time.perf_counter()
            if use_windowed_tensors:
                test_windowed = _prepare_windowed_split(
                    dataset_to_windowed_tensors(context.test_ds),
                    device,
                    non_blocking,
                )
                test_bt_t, test_ic, _ = _evaluate_windowed_tensor_batch(
                    eval_model,
                    test_windowed,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    config.trading.gross_leverage,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                )
                test_date_idx = test_windowed.valid_indices.to(
                    device=test_windowed.future_log_returns.device,
                    dtype=torch.long,
                )
                test_returns = test_windowed.future_log_returns[test_date_idx]
                test_masks = test_windowed.tradable_mask[test_date_idx]
                test_buy_masks = test_windowed.can_buy_mask[test_date_idx]
                test_sell_masks = test_windowed.can_sell_mask[test_date_idx]
                test_bench = test_windowed.benchmark[test_date_idx]
            else:
                test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _dataset_to_tensors(context.test_ds)
                test_x, test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _prepare_split_tensors(
                    test_x,
                    test_returns,
                    test_masks,
                    test_buy_masks,
                    test_sell_masks,
                    test_bench,
                    device,
                    non_blocking,
                )
                test_bt_t, test_ic, _ = _evaluate_tensor_batch(
                    eval_model,
                    test_x,
                    test_returns,
                    test_masks,
                    test_buy_masks,
                    test_sell_masks,
                    test_bench,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    config.trading.gross_leverage,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                )
            test_eval_total = time.perf_counter() - test_eval_start

            start = val_offsets[index]
            end = val_offsets[index + 1]

            test_report_start = time.perf_counter()
            val_ic = ic_summary(
                compute_ic_series_torch(
                    val_backtest.weights_history[start:end],
                    val_returns_device[start:end],
                    val_masks_device[start:end],
                ).cpu().numpy()
            )
            val_met = _compute_metrics_from_tensors(
                val_backtest.strategy_returns[start:end],
                val_backtest.benchmark_returns[start:end],
                val_backtest.turnovers[start:end],
            )

            test_dates = panel.dates[context.test_ds.valid_indices]
            test_close_prices = panel.close_prices[context.test_ds.valid_indices]
            test_met = _compute_metrics_from_tensors(
                test_bt_t.strategy_returns,
                test_bt_t.benchmark_returns,
                test_bt_t.turnovers,
            )
            test_bt = test_bt_t.to_numpy()
            test_integer_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt_t.weights_history.detach().cpu().numpy(),
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                can_buy_mask=test_buy_masks.detach().cpu().numpy(),
                can_sell_mask=test_sell_masks.detach().cpu().numpy(),
                benchmark_returns=test_bench.detach().cpu().numpy(),
                initial_capital=1_000_000.0,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                long_only=config.trading.long_only,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                gross_leverage=config.trading.gross_leverage,
                close_prices=test_close_prices,
                symbols=panel.symbols,
                dates=test_dates,
            )
            test_integer_met = compute_metrics(test_integer_bt)
            test_report_total = time.perf_counter() - test_report_start

            objective_key = _objective_metric_key(loss_objective)
            val_objective_metric = float(val_met.get(objective_key, float("nan")))
            test_objective_metric = float(test_met.get(objective_key, float("nan")))
            print(f"\n  [val]   IC={val_ic['ic_mean']:+.4f}  IC_IR={val_ic['ic_ir']:+.4f}  {loss_objective}={val_objective_metric:+.4f}  cum_ret={val_met['cumulative_return']:+.4f}  excess={val_met['excess_return_vs_universe_average']:+.4f}")
            print(f"  [test]  IC={test_ic['ic_mean']:+.4f}  IC_IR={test_ic['ic_ir']:+.4f}  {loss_objective}={test_objective_metric:+.4f}  cum_ret={test_met['cumulative_return']:+.4f}  excess={test_met['excess_return_vs_universe_average']:+.4f}")
            if profile_timing:
                _log_timing(
                    f"Train {train_years} fold {fold.fold_id} test_stage",
                    TimingBreakdown(
                        total_s=test_eval_total + test_report_total,
                        backtest_s=test_eval_total,
                        metrics_s=test_report_total,
                    ),
                )

            fold_result = FoldResult(
                fold_id=fold.fold_id,
                train_years=fold.train_years,
                val_years=fold.val_years,
                test_years=fold.test_years,
                best_val_loss=best_val_loss,
                val_ic=val_ic,
                val_metrics=val_met,
                test_ic=test_ic,
                test_metrics=test_met,
                test_integer_metrics=test_integer_met,
            )
            results_by_fold[fold.fold_id] = fold_result

            save_start = time.perf_counter()
            torch.save(_state_dict_for_save(model), _model_path(fold_dir))
            with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
                json.dump(asdict(fold_result), f, indent=2)

            _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
            _save_daily_portfolio_returns_csv(
                fold_dir / "daily_portfolio_returns.csv",
                test_dates,
                test_bt.strategy_returns,
                test_bt.benchmark_returns,
                test_bt.turnovers,
            )
            _save_daily_weights_csv(fold_dir / "daily_weights.csv", test_dates, panel.symbols, test_bt.weights_history)
            report = generate_annual_report(test_bt, test_dates)
            print("\n" + report)
            with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
                f.write(report)

            plot_start = time.perf_counter()
            plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
            plot_equity_curve(test_bt, test_dates, fold_dir / "leverage_equity_curve.png")
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "leverage_equity_curve_log.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "leverage_annual_performance.png")
            _save_integer_share_audit_artifacts(
                fold_dir,
                test_integer_bt,
                test_dates,
                panel.symbols,
                holdings_records,
            )
            plot_total = time.perf_counter() - plot_start

            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))
            explain_start = time.perf_counter()
            explain_path = _run_fold_explainability(
                model=model,
                panel=panel,
                config=config,
                output_path=output_path,
                fold=fold,
                device=device,
                checkpoint_path=best_checkpoint_path,
            )
            if explain_path is not None:
                print(f"[Fold {fold.fold_id}] explainability output: {explain_path}")
            if profile_timing:
                _log_timing(
                    f"Train {train_years} fold {fold.fold_id} save_plot",
                    TimingBreakdown(
                        total_s=time.perf_counter() - save_start,
                        save_s=plot_start - save_start,
                        plot_s=plot_total + (time.perf_counter() - explain_start),
                    ),
                )

        _save_group_checkpoint(
            group_checkpoint_path,
            train_years=train_years,
            epoch=last_epoch,
            model=model,
            optimizer=optimizer,
            scaler=scaler,
            scheduler=scheduler,
        )

        if config.training.warm_start_from_previous_fold:
            warm_start_checkpoint_path = group_checkpoint_path

        # Drop large per-group tensors/models so next group's batch-search sees true free VRAM.
        train_windowed = None
        combined_val_windowed = None
        combined_test_windowed = None
        train_x = None
        train_returns = None
        train_masks = None
        train_buy_masks = None
        train_sell_masks = None
        train_benchmark = None
        train_sample_mask = None
        combined_val_x = None
        combined_val_returns = None
        combined_val_masks = None
        combined_val_buy_masks = None
        combined_val_sell_masks = None
        combined_val_bench = None
        combined_test_x = None
        combined_test_returns = None
        combined_test_masks = None
        combined_test_buy_masks = None
        combined_test_sell_masks = None
        combined_test_bench = None
        val_returns_device = None
        val_masks_device = None
        model = None
        compiled_train_model = None
        optimizer = None
        scaler = None
        scheduler = None
        if device.type == "cuda":
            _release_cuda_memory(device)

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]
