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
import inspect
import csv
import hashlib
import random
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from functools import partial, wraps
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np
import torch
import torch.distributed as dist
import torch.distributed.nn.functional as dist_nn_f
try:
    import torch.distributed._functional_collectives as dist_fc
except Exception:  # pragma: no cover - older torch fallback
    dist_fc = None
from torch import nn
from torch.amp import GradScaler, autocast
from torch.nn.parallel import DistributedDataParallel as DistributedDataParallel
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
    plot_fold_first_year_returns_log10,
)
from stockagent.backtest.simulator import (
    CANONICAL_BACKTEST_CONTRACT_VERSION,
    BacktestResult,
    BacktestResultTensor,
    HoldingsRecord,
    _asset_log_returns_to_simple_numpy,
    _portfolio_simple_returns_to_log_numpy,
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
from stockagent.models.factory import (
    _feature_indices_from_patterns,
    build_model,
    model_hidden_dim_hint,
)
from stockagent.models.normalization import DEFAULT_PORTFOLIO_ACTIVATION
from stockagent.models.transformer_base_portfolio import (
    TransformerBasePortfolioModel,
    _normalize_ffn_type,
    _normalize_norm_type,
)
from stockagent.portfolio_contract import (
    normalize_portfolio_activation,
    normalize_portfolio_mode,
    normalize_portfolio_output_mode,
)
from stockagent.profiling import PROFILE_RANGES_ENABLED, profile_range
from stockagent.runtime_env import normalize_cuda_env
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import _resolve_rank_scores, get_loss_runtime_stats, risk_aware_loss
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


def _distributed_is_initialized() -> bool:
    return dist.is_available() and dist.is_initialized()


def _distributed_rank() -> int:
    if _distributed_is_initialized():
        return int(dist.get_rank())
    return int(os.environ.get("RANK", "0"))


def _distributed_world_size() -> int:
    if _distributed_is_initialized():
        return int(dist.get_world_size())
    return int(os.environ.get("WORLD_SIZE", "1"))


def _distributed_is_rank0() -> bool:
    return _distributed_rank() == 0


def _distributed_should_write() -> bool:
    return _distributed_world_size() <= 1 or _distributed_is_rank0()


def _distributed_barrier() -> None:
    if _distributed_is_initialized() and _distributed_world_size() > 1:
        if torch.cuda.is_available():
            try:
                dist.barrier(device_ids=[torch.cuda.current_device()])
                return
            except TypeError:
                pass
        dist.barrier()


def _raise_if_distributed_phase_failed(
    phase: str,
    local_error: BaseException | None,
) -> None:
    """Synchronize a phase result so one rank cannot strand the others."""
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        if local_error is not None:
            raise RuntimeError(
                f"Distributed phase {phase!r} failed: "
                f"{type(local_error).__name__}: {local_error}"
            ) from local_error
        return

    local_status = {
        "rank": _distributed_rank(),
        "ok": local_error is None,
        "error": (
            None
            if local_error is None
            else f"{type(local_error).__name__}: {local_error}"
        ),
    }
    statuses: list[dict[str, Any] | None] = [None] * _distributed_world_size()
    dist.all_gather_object(statuses, local_status)
    failures = sorted(
        (
            status
            for status in statuses
            if status is not None and not bool(status.get("ok", False))
        ),
        key=lambda status: int(status.get("rank", -1)),
    )
    if not failures:
        return
    details = "; ".join(
        f"rank{int(status.get('rank', -1))}: {status.get('error', 'unknown error')}"
        for status in failures
    )
    message = f"Distributed phase {phase!r} failed consistently across ranks ({details})"
    if local_error is not None:
        raise RuntimeError(message) from local_error
    raise RuntimeError(message)


def _run_rank0_store_synchronized_phase(
    phase: str,
    operation: Callable[[], None],
    *,
    timeout: timedelta | None = None,
) -> None:
    """Run long rank-0-only work without holding an NCCL collective open.

    Object collectives inherit the process-group timeout, which is too short
    for a multi-fold artifact inference migration.  A quick broadcast gives
    every rank a fresh key, then nonzero ranks wait on the process store while
    rank 0 performs the work.  The final status is therefore propagated
    without keeping a CUDA collective pending for the duration of inference.
    """
    if timeout is None:
        timeout_seconds = float(
            os.environ.get("STOCKAGENT_DISTRIBUTED_PHASE_TIMEOUT_SECONDS", "86400")
        )
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0.0:
            raise ValueError(
                "STOCKAGENT_DISTRIBUTED_PHASE_TIMEOUT_SECONDS must be finite and > 0"
            )
        timeout = timedelta(seconds=timeout_seconds)

    if not _distributed_is_initialized():
        if _distributed_world_size() > 1:
            raise RuntimeError(
                f"Distributed phase {phase!r} requires an initialized process group"
            )
        operation()
        return
    if _distributed_world_size() <= 1:
        operation()
        return

    token: list[str | None] = [
        f"{time.time_ns()}-{os.getpid()}" if _distributed_is_rank0() else None
    ]
    dist.broadcast_object_list(token, src=0)
    if token[0] is None:
        raise RuntimeError(f"Distributed phase {phase!r} did not receive a store token")
    phase_key = f"stockagent/{phase}/{token[0]}"
    store = dist.distributed_c10d._get_default_store()
    rank = _distributed_rank()
    ack_key = f"{phase_key}/ack/{rank}"
    ack_keys = [
        f"{phase_key}/ack/{item_rank}"
        for item_rank in range(_distributed_world_size())
    ]

    local_error: BaseException | None = None
    status: dict[str, Any]
    if _distributed_is_rank0():
        try:
            operation()
            status = {"ok": True, "error": None}
        except BaseException as exc:
            local_error = exc
            status = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        store.set(phase_key, json.dumps(status))
        store.set(ack_key, json.dumps({"ok": True, "error": None}))
        try:
            store.wait(ack_keys, timeout)
            acknowledgements = [
                json.loads(store.get(key).decode("utf-8"))
                for key in ack_keys
            ]
        except BaseException as exc:
            raise RuntimeError(
                f"Distributed phase {phase!r} did not receive every rank acknowledgement"
            ) from exc
        failed_acknowledgements = [
            (item_rank, acknowledgement.get("error"))
            for item_rank, acknowledgement in enumerate(acknowledgements)
            if not bool(acknowledgement.get("ok", False))
        ]
        if failed_acknowledgements:
            details = "; ".join(
                f"rank{item_rank}: {error}"
                for item_rank, error in failed_acknowledgements
            )
            raise RuntimeError(
                f"Distributed phase {phase!r} acknowledgement failed ({details})"
            )
    else:
        receive_error: BaseException | None = None
        try:
            store.wait([phase_key], timeout)
            raw_status = store.get(phase_key)
            status = json.loads(raw_status.decode("utf-8"))
        except BaseException as exc:
            receive_error = exc
            status = {
                "ok": False,
                "error": f"rank 0 status unavailable: {type(exc).__name__}: {exc}",
            }
        store.set(
            ack_key,
            json.dumps(
                {
                    "ok": receive_error is None,
                    "error": None if receive_error is None else status["error"],
                }
            ),
        )
        if receive_error is not None:
            raise RuntimeError(
                f"Distributed phase {phase!r} did not receive rank 0 status"
            ) from receive_error

    if not bool(status.get("ok", False)):
        message = f"Distributed phase {phase!r} failed on rank 0: {status.get('error')}"
        if local_error is not None:
            raise RuntimeError(message) from local_error
        raise RuntimeError(message)


@contextmanager
def _preserve_process_rng_state(device: torch.device | None = None):
    """Keep non-training preflight work from advancing any stochastic stream."""
    python_state = random.getstate()
    numpy_state = np.random.get_state()
    rng_devices: list[int] = []
    if device is not None and device.type == "cuda":
        rng_devices = [
            int(device.index)
            if device.index is not None
            else int(torch.cuda.current_device())
        ]
    try:
        with torch.random.fork_rng(devices=rng_devices, enabled=True):
            yield
    finally:
        random.setstate(python_state)
        np.random.set_state(numpy_state)


def _all_gather_autograd(x: torch.Tensor) -> torch.Tensor:
    if _distributed_world_size() <= 1:
        return x
    if dist_fc is not None and hasattr(dist_fc, "all_gather_tensor"):
        gathered = dist_fc.all_gather_tensor(x, 0, group=dist.group.WORLD)
        # Do not pass AsyncCollectiveTensor/unbacked symbolic storage into a
        # separately compiled loss graph. PyTorch 2.12 can otherwise attempt a
        # second guard specialization in can_use_32bit_indexing and fail while
        # constructing its ShapeEnv guard. wait_tensor preserves autograd while
        # presenting the canonical loss with an ordinary realized tensor.
        if hasattr(dist_fc, "wait_tensor"):
            gathered = dist_fc.wait_tensor(gathered)
        return gathered
    parts = dist_nn_f.all_gather(x)
    return torch.cat(tuple(parts), dim=0)


def _all_gather_no_grad(x: torch.Tensor) -> torch.Tensor:
    if _distributed_world_size() <= 1:
        return x
    parts = [torch.empty_like(x) for _ in range(_distributed_world_size())]
    dist.all_gather(parts, x)
    return torch.cat(parts, dim=0)


@contextmanager
def _temporary_env(name: str, value: str):
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


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
    deployment_test_rows: int
    checkpoint_best_path: Path
    best_val_loss: float = float("inf")


@dataclass(slots=True)
class TimingBreakdown:
    total_s: float = 0.0
    fetch_s: float = 0.0
    transfer_s: float = 0.0
    batch_prepare_s: float = 0.0
    prepare_index_build_s: float = 0.0
    prepare_window_slice_s: float = 0.0
    prepare_feature_gather_s: float = 0.0
    prepare_target_gather_s: float = 0.0
    prepare_mask_build_s: float = 0.0
    prepare_nan_fill_s: float = 0.0
    prepare_dtype_cast_s: float = 0.0
    prepare_contiguous_s: float = 0.0
    prepare_clone_s: float = 0.0
    prepare_device_move_s: float = 0.0
    prepare_panel_slab_batch_s: float = 0.0
    prepare_metadata_batch_s: float = 0.0
    prepare_materialized_batch_s: float = 0.0
    prepare_move_windowed_batch_s: float = 0.0
    window_materialize_s: float = 0.0
    h2d_transfer_s: float = 0.0
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
    clip_cuda_s: float = 0.0
    finite_check_s: float = 0.0
    step_s: float = 0.0
    step_cuda_s: float = 0.0
    scheduler_s: float = 0.0
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
    iter_start_sync_s: float = 0.0
    after_prepare_sync_s: float = 0.0
    after_forward_sync_s: float = 0.0
    after_loss_sync_s: float = 0.0
    after_backward_sync_s: float = 0.0
    after_step_sync_s: float = 0.0
    gc_s: float = 0.0
    batches: int = 0
    cuda_events: list[tuple[str, torch.device, torch.cuda.Event, torch.cuda.Event]] = field(
        default_factory=list
    )


def _log_timing(label: str, timing: TimingBreakdown) -> None:
    parts = [f"[profile] {label}: total={timing.total_s:.3f}s"]
    if timing.batches:
        parts.append(f"batches={timing.batches}")
    for name in (
        "fetch_s",
        "transfer_s",
        "batch_prepare_s",
        "prepare_index_build_s",
        "prepare_window_slice_s",
        "prepare_feature_gather_s",
        "prepare_target_gather_s",
        "prepare_mask_build_s",
        "prepare_nan_fill_s",
        "prepare_dtype_cast_s",
        "prepare_contiguous_s",
        "prepare_clone_s",
        "prepare_device_move_s",
        "prepare_panel_slab_batch_s",
        "prepare_metadata_batch_s",
        "prepare_materialized_batch_s",
        "prepare_move_windowed_batch_s",
        "window_materialize_s",
        "h2d_transfer_s",
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
        "clip_cuda_s",
        "finite_check_s",
        "step_s",
        "step_cuda_s",
        "scheduler_s",
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
        "iter_start_sync_s",
        "after_prepare_sync_s",
        "after_forward_sync_s",
        "after_loss_sync_s",
        "after_backward_sync_s",
        "after_step_sync_s",
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
        "batch_prepare_s",
        "prepare_index_build_s",
        "prepare_window_slice_s",
        "prepare_feature_gather_s",
        "prepare_target_gather_s",
        "prepare_mask_build_s",
        "prepare_nan_fill_s",
        "prepare_dtype_cast_s",
        "prepare_contiguous_s",
        "prepare_clone_s",
        "prepare_device_move_s",
        "prepare_panel_slab_batch_s",
        "prepare_metadata_batch_s",
        "prepare_materialized_batch_s",
        "prepare_move_windowed_batch_s",
        "window_materialize_s",
        "h2d_transfer_s",
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
        "clip_cuda_s",
        "finite_check_s",
        "step_s",
        "step_cuda_s",
        "scheduler_s",
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
        "iter_start_sync_s",
        "after_prepare_sync_s",
        "after_forward_sync_s",
        "after_loss_sync_s",
        "after_backward_sync_s",
        "after_step_sync_s",
        "gc_s",
    ):
        setattr(dst, name, getattr(dst, name) + getattr(src, name))
    dst.batches += src.batches
    dst.cuda_events.extend(src.cuda_events)


class _CudaTimingRecorder:
    def __init__(
        self,
        timing: TimingBreakdown,
        attr: str,
        device: torch.device,
        *,
        enabled: bool = True,
    ):
        self.timing = timing
        self.attr = attr
        self.device = device
        self.enabled = bool(enabled) and device.type == "cuda" and torch.cuda.is_available()
        self.start: torch.cuda.Event | None = None
        self.end: torch.cuda.Event | None = None

    def __enter__(self) -> "_CudaTimingRecorder":
        if self.enabled:
            with torch.cuda.device(self.device):
                self.start = torch.cuda.Event(enable_timing=True)
                self.end = torch.cuda.Event(enable_timing=True)
                self.start.record()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.enabled and self.start is not None and self.end is not None:
            with torch.cuda.device(self.device):
                self.end.record()
            self.timing.cuda_events.append((self.attr, self.device, self.start, self.end))


def _cuda_timing(
    timing: TimingBreakdown,
    attr: str,
    device: torch.device,
    *,
    enabled: bool = True,
) -> _CudaTimingRecorder:
    return _CudaTimingRecorder(timing, attr, device, enabled=enabled)


def _flush_cuda_timing_events(timing: TimingBreakdown) -> None:
    if not timing.cuda_events:
        return
    pending = list(timing.cuda_events)
    timing.cuda_events.clear()
    # One synchronization per participating device completes every recorded
    # event. Synchronizing each end event separately adds thousands of redundant
    # CUDA API calls per full-US epoch.
    devices = {device for _attr, device, _start, _end in pending}
    for device in devices:
        try:
            torch.cuda.synchronize(device)
        except Exception:
            pass
    for attr, _device, start, end in pending:
        try:
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


def _record_debug_cuda_sync(timing: TimingBreakdown, attr: str, device: torch.device, enabled: bool) -> None:
    if not enabled or device.type != "cuda":
        return
    start_t = time.perf_counter()
    torch.cuda.synchronize(device)
    setattr(timing, attr, getattr(timing, attr) + time.perf_counter() - start_t)


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
        strict_no_fallback: bool | None = None,
    ) -> None:
        self._compiled_fn = compiled_fn
        self._eager_fn = eager_fn
        self._label = label
        self._strict_no_fallback = _strict_no_fallback_enabled() if strict_no_fallback is None else bool(strict_no_fallback)
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
            if self._strict_no_fallback:
                raise RuntimeError(
                    f"[{self._label}] torch.compile loss hit CUDA Graph state overwrite; "
                    "strict_no_fallback=true so eager loss fallback is disabled."
                ) from exc
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
                aux_outputs.pop("_final_alive", None)
            return self._eager_fn(*args, **kwargs)

    @property
    def disabled(self) -> bool:
        return self._disabled


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "on", "yes"}


def _strict_no_fallback_enabled() -> bool:
    return _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0")


def _progress(message: str) -> None:
    if not _distributed_should_write():
        return
    print(message, flush=True)


def _extract_weights_and_aux(
    model_output: torch.Tensor | dict[str, torch.Tensor] | tuple[Any, ...],
) -> tuple[torch.Tensor, dict[str, torch.Tensor] | None]:
    if isinstance(model_output, dict):
        if "weights" not in model_output:
            raise ValueError("Model output dict must include 'weights'")
        weights = model_output["weights"]
        return weights, model_output
    if isinstance(model_output, tuple):
        weights = model_output[0]
        aux = model_output[2] if len(model_output) >= 3 and isinstance(model_output[2], dict) else None
        return weights, aux
    return model_output, None


def _model_accepts_return_aux(model: nn.Module) -> bool:
    target = _unwrap_model(model)
    cached = getattr(target, "_stockagent_accepts_return_aux", None)
    if cached is not None:
        return bool(cached)
    try:
        accepts = "return_aux" in inspect.signature(target.forward).parameters
    except (TypeError, ValueError):
        accepts = hasattr(target, "return_aux")
    try:
        setattr(target, "_stockagent_accepts_return_aux", bool(accepts))
    except Exception:
        pass
    return bool(accepts)


def _call_model(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    return_aux: bool | None = None,
    symbol_indices: torch.Tensor | None = None,
):
    mask = _model_attention_mask(mask)
    if symbol_indices is not None:
        if return_aux is None or not _model_accepts_return_aux(model):
            return model(x, mask, symbol_indices=symbol_indices)
        return model(x, mask, return_aux=return_aux, symbol_indices=symbol_indices)
    if return_aux is None or not _model_accepts_return_aux(model):
        return model(x, mask)
    return model(x, mask, return_aux=return_aux)


def _model_attention_mask(mask: torch.Tensor) -> torch.Tensor:
    """Ensure every model row has a visible attention token.

    Dataset rows containing only a forced exit legitimately have no tradable
    symbols.  The model still needs one visible token to keep attention kernels
    well-defined, while callers retain the original mask for every loss and
    backtest calculation.
    """
    mask_bool = mask.to(dtype=torch.bool)
    empty_rows = ~mask_bool.any(dim=-1, keepdim=True)
    return torch.cat((mask_bool[..., :1] | empty_rows, mask_bool[..., 1:]), dim=-1)


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
    log_utility_pre_log_power: float = 0.0,
    log_utility_periods_per_year: float = 252.0,
    log_utility_log_shift: float = 0.0,
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
        annual_utility = _annual_log_utility_eval_value(
            valid_returns,
            pre_log_power=log_utility_pre_log_power,
            periods_per_year=log_utility_periods_per_year,
            log_shift=log_utility_log_shift,
        )
        objective_value = -float(gamma_sharpe) * annual_utility
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


def _annual_log_utility_eval_value(
    strategy_log_returns: torch.Tensor,
    *,
    pre_log_power: float = 0.0,
    periods_per_year: float = 252.0,
    log_shift: float = 0.0,
) -> torch.Tensor:
    clean = torch.nan_to_num(strategy_log_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)
    if clean.numel() == 0:
        return clean.sum() * 0.0
    manual_years = float(pre_log_power)
    if manual_years > 0.0:
        years = manual_years
    else:
        periods = max(float(periods_per_year), 1e-12)
        years = float(clean.numel()) / periods
    annual_log_return = clean.sum() / clean.new_tensor(max(years, 1e-12))
    return annual_log_return - clean.new_tensor(float(log_shift))


def _is_return_series_objective(objective: str) -> bool:
    return objective.strip().lower() in {
        "sharpe",
        "sharp",
        "sharpck",
        "sharpe_ratio",
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
        "factor_generalization",
        "factor",
        "factor_ic",
        "characteristic_factor",
        "portfolio_autoencoder",
        "bottleneck_portfolio_autoencoder",
        "autoencoder_portfolio",
    }


def _is_log_utility_objective(objective: str) -> bool:
    return objective.strip().lower() in {"log_utility", "log_util", "kelly", "growth", "mean_log_return"}


def _volume_participation_enabled(max_volume_participation: float, volume_participation_equity: float) -> bool:
    return float(max_volume_participation) > 0.0 and float(volume_participation_equity) > 0.0


def _volume_limit_weights_from_notional(
    volume_notional: torch.Tensor | None,
    *,
    max_volume_participation: float,
    volume_participation_equity: float,
    device: torch.device,
    dtype: torch.dtype,
) -> torch.Tensor | None:
    if volume_notional is None:
        return None
    if not _volume_participation_enabled(max_volume_participation, volume_participation_equity):
        return None
    notional = volume_notional.to(device=device, dtype=dtype)
    cap = notional * (float(max_volume_participation) / float(volume_participation_equity))
    return torch.where(
        torch.isfinite(cap) & (cap >= 0.0),
        cap,
        torch.full_like(cap, float("inf")),
    )


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
        log_utility_pre_log_power=config.evaluation.eval_log_utility_pre_log_power,
        log_utility_periods_per_year=config.evaluation.eval_log_utility_periods_per_year,
        log_utility_log_shift=config.evaluation.eval_log_utility_log_shift,
    )


def _evaluated_backtest_loss(
    backtest: BacktestResultTensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    force_short_cover_mask: torch.Tensor,
    force_exit_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    volume_notional: torch.Tensor | None,
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
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        force_exit_mask=force_exit_mask,
        volume_limit_weights=_volume_limit_weights_from_notional(
            volume_notional,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
            device=backtest.weights_history.device,
            dtype=backtest.weights_history.dtype,
        ),
        long_only=config.trading.long_only,
        buy_fee_rate=config.trading.buy_fee_rate,
        sell_fee_rate=config.trading.sell_fee_rate,
        max_turnover_ratio=config.trading.max_turnover_ratio,
        gross_leverage=1.0,
        min_trade_weight=config.trading.min_trade_weight,
        portfolio_activation=config.trading.portfolio_activation,
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
    log_utility_pre_log_power: float = 0.0,
    log_utility_periods_per_year: float = 252.0,
    log_utility_log_shift: float = 0.0,
) -> torch.Tensor:
    """Compute one validation/test loss per fold without per-fold CPU round trips.

    For ``log_utility`` this is intentionally the same fee-adjusted return and
    turnover reduction used by the canonical training loss, applied to already
    computed canonical backtest curves. The full curves are still needed for
    equity/risk plots and metrics; this reduction must not introduce a separate
    validation/test loss formula.
    """
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
                log_utility_pre_log_power=log_utility_pre_log_power,
                log_utility_periods_per_year=log_utility_periods_per_year,
                log_utility_log_shift=log_utility_log_shift,
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
        manual_years = float(log_utility_pre_log_power)
        if manual_years > 0.0:
            years = torch.full_like(count, manual_years)
        else:
            years = count / torch.as_tensor(
                max(float(log_utility_periods_per_year), 1e-12),
                device=device,
                dtype=calc_dtype,
            )
        annual_utility = r.sum(dim=1) / years.clamp_min(1e-12) - torch.as_tensor(
            float(log_utility_log_shift),
            device=device,
            dtype=calc_dtype,
        )
        objective_value = -float(gamma_sharpe) * annual_utility
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
        "sharp",
        "sharpck",
        "sharpe_ratio",
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
        if objective in {"sharp", "sharpck", "sharpe_ratio"}:
            return "sharpe"
        if objective in {"log_util", "kelly", "growth", "mean_log_return"}:
            return "log_utility"
        if objective in {"cvar", "cvar_drawdown", "excess_cvar"}:
            return "excess_cvar_drawdown"
        if objective in {"outperformance_budget", "outperformance_first"}:
            return "outperformance_risk_budget"
        return objective
    return "sharpe"


def _evaluate_windowed_aux_objective_loss(
    model: nn.Module,
    split: WindowedSplitTensors,
    row_start: int,
    row_end: int,
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
    return_rank_ic_weight: float,
    direction_weight: float,
    volatility_regime_weight: float,
    concentration_weight: float,
    regime_up_threshold: float,
    regime_down_threshold: float,
    factor_loss_kwargs: dict[str, Any] | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
) -> tuple[float, float | None, TimingBreakdown]:
    """Evaluate an auxiliary objective through the canonical windowed path.

    Model inference is chunked for memory, but the objective is evaluated once
    over the complete chronological segment.  This is required for objectives
    such as factor stability and portfolio Sharpe: averaging independent chunk
    losses would reset holdings and change the mathematics of the objective.
    """
    model.eval()
    row_start = max(0, int(row_start))
    row_end = min(int(row_end), len(split))
    if row_end <= row_start:
        return 0.0, None, TimingBreakdown()

    timing = TimingBreakdown()
    overall_start = time.perf_counter()
    panel_forward_model = _panel_forward_module(model)
    model_return_aux = _training_needs_aux(
        objective,
        direction_weight=direction_weight,
        volatility_regime_weight=volatility_regime_weight,
    )
    weights_chunks: list[torch.Tensor] = []
    returns_chunks: list[torch.Tensor] = []
    tradable_chunks: list[torch.Tensor] = []
    buy_chunks: list[torch.Tensor] = []
    sell_chunks: list[torch.Tensor] = []
    short_open_chunks: list[torch.Tensor] = []
    force_cover_chunks: list[torch.Tensor] = []
    force_exit_chunks: list[torch.Tensor] = []
    benchmark_chunks: list[torch.Tensor] = []
    sample_chunks: list[torch.Tensor] = []
    volume_chunks: list[torch.Tensor] = []
    aux_chunks: dict[str, list[torch.Tensor]] = {}
    aux_static: dict[str, torch.Tensor] = {}

    with torch.inference_mode():
        for start in range(row_start, row_end, max(1, int(chunk_rows))):
            end = min(start + max(1, int(chunk_rows)), row_end)

            transfer_start = time.perf_counter()
            prepare_device = _windowed_prepare_device(split, device)
            if panel_forward_model is not None:
                batch = split.batch_metadata_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                )
            else:
                batch = split.batch_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                )
            batch = _move_windowed_batch_to_device(batch, device, non_blocking)
            returns_chunk = batch["future_log_returns"]
            mask_chunk = batch["tradable_mask"]
            timing.transfer_s += time.perf_counter() - transfer_start

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                model_forward_start = time.perf_counter()
                if panel_forward_model is not None:
                    model_output = _call_panel_forward_for_batch(
                        panel_forward_model=panel_forward_model,
                        panel_slab_model=None,
                        split=split,
                        batch=batch,
                        mask=mask_chunk,
                        device=device,
                        non_blocking=non_blocking,
                        return_aux=model_return_aux,
                        allow_slab=False,
                    )
                else:
                    model_output = _call_model(
                        model,
                        batch["x"],
                        mask_chunk,
                        return_aux=model_return_aux,
                        symbol_indices=batch.get("symbol_indices"),
                    )
                weights_chunk, aux_outputs = _extract_weights_and_aux(model_output)
                _require_training_aux_outputs(
                    objective,
                    aux_outputs,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                )
                timing.model_forward_s += time.perf_counter() - model_forward_start
            timing.forward_s += time.perf_counter() - forward_start

            weights_chunks.append(weights_chunk.float())
            returns_chunks.append(returns_chunk)
            tradable_chunks.append(mask_chunk)
            buy_chunks.append(batch["can_buy_mask"])
            sell_chunks.append(batch["can_sell_mask"])
            short_open_chunks.append(batch["can_short_open_mask"])
            force_cover_chunks.append(batch["force_short_cover_mask"])
            force_exit_chunks.append(batch["force_exit_mask"])
            benchmark_chunks.append(batch["benchmark"])
            sample_chunks.append(batch["sample_mask"])
            if "volume_notional" in batch:
                volume_chunks.append(batch["volume_notional"])
            for key, value in (aux_outputs or {}).items():
                if not isinstance(value, torch.Tensor) or key.startswith("_"):
                    continue
                if value.dim() > 0 and int(value.size(0)) == end - start:
                    aux_chunks.setdefault(key, []).append(value)
                else:
                    aux_static.setdefault(key, value)

    weights_all = torch.cat(weights_chunks, dim=0)
    returns_all = torch.cat(returns_chunks, dim=0)
    tradable_all = torch.cat(tradable_chunks, dim=0)
    benchmark_all = torch.cat(benchmark_chunks, dim=0)
    aux_all = dict(aux_static)
    expected_chunks = len(weights_chunks)
    for key, values in aux_chunks.items():
        if len(values) == expected_chunks:
            aux_all[key] = torch.cat(values, dim=0)
    volume_notional = torch.cat(volume_chunks, dim=0) if len(volume_chunks) == expected_chunks else None
    volume_limit_weights = _volume_limit_weights_from_notional(
        volume_notional,
        max_volume_participation=max_volume_participation,
        volume_participation_equity=volume_participation_equity,
        device=device,
        dtype=weights_all.dtype,
    )

    loss_start = time.perf_counter()
    loss_t = risk_aware_loss(
        weights_all,
        returns_all,
        tradable_all,
        benchmark_returns=benchmark_all,
        can_buy_mask=torch.cat(buy_chunks, dim=0),
        can_sell_mask=torch.cat(sell_chunks, dim=0),
        can_short_open_mask=torch.cat(short_open_chunks, dim=0),
        force_short_cover_mask=torch.cat(force_cover_chunks, dim=0),
        force_exit_mask=torch.cat(force_exit_chunks, dim=0),
        sample_mask=torch.cat(sample_chunks, dim=0),
        long_only=long_only,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
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
        aux_outputs=aux_all,
        rank_ic_weight=rank_ic_weight,
        return_rank_ic_weight=return_rank_ic_weight,
        direction_weight=direction_weight,
        volatility_regime_weight=volatility_regime_weight,
        concentration_weight=concentration_weight,
        regime_up_threshold=regime_up_threshold,
        regime_down_threshold=regime_down_threshold,
        **{
            key: value
            for key, value in (factor_loss_kwargs or {}).items()
        },
    )
    timing.loss_s += time.perf_counter() - loss_start

    reduce_start = time.perf_counter()
    mean_loss = float(loss_t.detach().float().cpu())
    rank_scores = _resolve_rank_scores(weights_all, aux_all)
    rank_tradable = tradable_all & torch.cat(sample_chunks, dim=0).to(dtype=torch.bool).unsqueeze(1)
    ic_series = compute_ic_series_torch(rank_scores, returns_all, rank_tradable)
    ic_clean = ic_series[torch.isfinite(ic_series)]
    mean_ic = None if ic_clean.numel() == 0 else float(ic_clean.float().mean().cpu())
    timing.metrics_s += time.perf_counter() - reduce_start
    timing.total_s = time.perf_counter() - overall_start
    timing.batches = int(expected_chunks)
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
        return "excess_return_vs_benchmark"
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


def _best_checkpoint_path(fold_dir: Path) -> Path:
    return fold_dir / "checkpoint_best.pt"


def _metrics_path(fold_dir: Path) -> Path:
    return fold_dir / "metrics.json"


def _fold_complete_marker_path(fold_dir: Path) -> Path:
    return fold_dir / "fold_complete.json"


def _model_path(fold_dir: Path) -> Path:
    return fold_dir / "model.pt"


def _backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "test_backtest.npz"


def _deployment_backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "deployment_test_backtest.npz"


def _best_val_backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "best_val_backtest.npz"


def _best_val_snapshot_path(fold_dir: Path) -> Path:
    return fold_dir / "best_val_snapshot.json"


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


def _parse_group_years_from_dir_name(name: str) -> list[int] | None:
    if not name.startswith("train_"):
        return None
    raw_years = name[len("train_") :].split("-")
    if not raw_years:
        return None
    years: list[int] = []
    for token in raw_years:
        try:
            years.append(int(token))
        except ValueError:
            return None
    return years


def _latest_group_checkpoint(output_path: Path) -> tuple[Path, list[int]] | None:
    candidates: list[tuple[list[int], float, Path]] = []
    for candidate in output_path.glob("train_*/checkpoint_last.pt"):
        years = _parse_group_years_from_dir_name(candidate.parent.name)
        if years is None:
            continue
        try:
            mtime = candidate.stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((years, mtime, candidate))
    if not candidates:
        return None

    # Prefer the checkpoint with the widest train-year span, then newest year, then latest mtime.
    candidates.sort(key=lambda item: (len(item[0]), item[0][-1], item[1]))
    years, _, path = candidates[-1]
    return path, years


def _group_curve_path(output_path: Path, train_years: list[int]) -> Path:
    return _group_dir(output_path, train_years) / "epoch_curve.jsonl"


def _write_loss_contract_metadata(
    path: Path,
    *,
    objective: str,
    return_rank_ic_weight: float,
    direction_weight: float,
    volatility_regime_weight: float,
    factor_aug_kwargs: Mapping[str, float] | None,
) -> None:
    """Persist which loss components are shared with evaluation versus train-only."""
    if not _distributed_should_write():
        return
    factor_aug_enabled = bool(factor_aug_kwargs)
    payload = {
        "schema_version": 1,
        "objective": _normalize_risk_objective(objective),
        "rank_score_contract": ["rank_logits", "score_logits", "weights"],
        "deterministic_aux_penalties": {
            "return_rank_ic_weight": float(return_rank_ic_weight),
            "direction_weight": float(direction_weight),
            "volatility_regime_weight": float(volatility_regime_weight),
            "scope": "train_validation_test",
        },
        "factor_consistency_augmentation": {
            "enabled": factor_aug_enabled,
            "scope": "train_only" if factor_aug_enabled else "disabled",
            "validation_test_included": False,
            "parameters": dict(factor_aug_kwargs or {}),
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _trim_group_curve(path: Path, start_epoch: int) -> None:
    if not _distributed_should_write():
        return
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


def _coerce_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, parsed)


def _finite_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _infer_no_improve_epochs_from_curve(path: Path, stop_before_epoch: int | None = None) -> int:
    if not path.exists():
        return 0
    best_val = float("inf")
    no_improve_epochs = 0
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                epoch = int(payload.get("epoch", 0))
            except (TypeError, ValueError):
                continue
            if epoch <= 0:
                continue
            if stop_before_epoch is not None and epoch >= stop_before_epoch:
                continue

            explicit_no_improve = _coerce_nonnegative_int(payload.get("no_improve"))
            val_mean = _finite_float_or_none(payload.get("val_mean"))
            if explicit_no_improve is not None:
                no_improve_epochs = explicit_no_improve
                if val_mean is not None and val_mean < best_val:
                    best_val = val_mean
                continue
            if val_mean is None:
                continue
            if val_mean < best_val:
                best_val = val_mean
                no_improve_epochs = 0
            else:
                no_improve_epochs += 1
    return no_improve_epochs


def _resume_no_improve_epochs_from_checkpoint(
    checkpoint: Mapping[str, Any],
    curve_path: Path,
) -> tuple[int, str]:
    checkpoint_no_improve = _coerce_nonnegative_int(checkpoint.get("no_improve_epochs"))
    if checkpoint_no_improve is not None:
        return checkpoint_no_improve, "checkpoint"
    if not curve_path.exists():
        return 0, "default"
    return _infer_no_improve_epochs_from_curve(curve_path), "epoch_curve"


def _append_group_curve(path: Path, payload: dict[str, float | int | None]) -> None:
    if not _distributed_should_write():
        return
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


def _epoch_curve_plot_command(curve_path: Path, interval: int, *, write_parquet_cache: bool = False) -> list[str]:
    output_path, timing_output_path = _epoch_curve_plot_paths(curve_path, interval)
    command = [
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
    if not write_parquet_cache:
        command.append("--no-write-parquet-cache")
    return command


def _run_epoch_curve_plot_once(
    curve_path: Path,
    interval: int,
    *,
    write_parquet_cache: bool = True,
) -> dict[str, float | int | str]:
    script_path = _epoch_curve_plot_script()
    if not script_path.exists():
        return {}
    try:
        from plot_epoch_curves import plot_curve_file

        output_path, timing_output_path = _epoch_curve_plot_paths(curve_path, interval)
        _, _, timing = plot_curve_file(
            curve_path,
            interval=max(1, int(interval)),
            output_path=output_path,
            timing_output_path=timing_output_path,
            write_parquet_cache=bool(write_parquet_cache),
            quiet=True,
        )
        return timing
    except Exception as exc:
        print(f"[curve plot] failed for {curve_path}: {type(exc).__name__}: {exc}")
        return {}


def _log_curve_plot_timing(label: str, timing: Mapping[str, Any]) -> None:
    if not timing:
        return
    parts = [f"[profile] {label}: total={float(timing.get('total_s', 0.0)):.3f}s"]
    for key in ("load_s", "parquet_cache_s", "sample_s", "loss_plot_s", "timing_plot_s"):
        value = float(timing.get(key, 0.0))
        if value > 0:
            parts.append(f"{key[:-2]}={value:.3f}s")
    rows = timing.get("plotted_rows")
    if rows is not None:
        parts.append(f"rows={rows}")
    timing_json = timing.get("timing_json")
    if timing_json:
        parts.append(f"json={timing_json}")
    print(" ".join(parts))


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
            _run_epoch_curve_plot_once(self.curve_path, self.interval, write_parquet_cache=False)
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
            _epoch_curve_plot_command(self.curve_path, self.interval, write_parquet_cache=False),
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

    train_prepare_child_s = (
        float(train_timing.prepare_index_build_s)
        + float(train_timing.prepare_window_slice_s)
        + float(train_timing.prepare_feature_gather_s)
        + float(train_timing.prepare_target_gather_s)
        + float(train_timing.prepare_mask_build_s)
        + float(train_timing.prepare_nan_fill_s)
        + float(train_timing.prepare_dtype_cast_s)
        + float(train_timing.prepare_contiguous_s)
        + float(train_timing.prepare_clone_s)
        + float(train_timing.prepare_device_move_s)
    )
    train_prepare_unaccounted_s = float(train_timing.batch_prepare_s) - train_prepare_child_s

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
        "train_batch_prepare_ms_per_batch": _avg_ms(train_timing.batch_prepare_s),
        "train_prepare_index_build_ms_per_batch": _avg_ms(train_timing.prepare_index_build_s),
        "train_prepare_window_slice_ms_per_batch": _avg_ms(train_timing.prepare_window_slice_s),
        "train_prepare_feature_gather_ms_per_batch": _avg_ms(train_timing.prepare_feature_gather_s),
        "train_prepare_target_gather_ms_per_batch": _avg_ms(train_timing.prepare_target_gather_s),
        "train_prepare_mask_build_ms_per_batch": _avg_ms(train_timing.prepare_mask_build_s),
        "train_prepare_nan_fill_ms_per_batch": _avg_ms(train_timing.prepare_nan_fill_s),
        "train_prepare_dtype_cast_ms_per_batch": _avg_ms(train_timing.prepare_dtype_cast_s),
        "train_prepare_contiguous_ms_per_batch": _avg_ms(train_timing.prepare_contiguous_s),
        "train_prepare_clone_ms_per_batch": _avg_ms(train_timing.prepare_clone_s),
        "train_prepare_device_move_ms_per_batch": _avg_ms(train_timing.prepare_device_move_s),
        "train_prepare_panel_slab_batch_ms_per_batch": _avg_ms(train_timing.prepare_panel_slab_batch_s),
        "train_prepare_metadata_batch_ms_per_batch": _avg_ms(train_timing.prepare_metadata_batch_s),
        "train_prepare_materialized_batch_ms_per_batch": _avg_ms(train_timing.prepare_materialized_batch_s),
        "train_prepare_move_windowed_batch_ms_per_batch": _avg_ms(train_timing.prepare_move_windowed_batch_s),
        "train_prepare_unaccounted_ms_per_batch": _avg_ms(train_prepare_unaccounted_s),
        "train_iter_start_sync_ms_per_batch": _avg_ms(train_timing.iter_start_sync_s),
        "train_after_prepare_sync_ms_per_batch": _avg_ms(train_timing.after_prepare_sync_s),
        "train_after_forward_sync_ms_per_batch": _avg_ms(train_timing.after_forward_sync_s),
        "train_after_loss_sync_ms_per_batch": _avg_ms(train_timing.after_loss_sync_s),
        "train_after_backward_sync_ms_per_batch": _avg_ms(train_timing.after_backward_sync_s),
        "train_after_step_sync_ms_per_batch": _avg_ms(train_timing.after_step_sync_s),
        "train_window_materialize_ms_per_batch": _avg_ms(train_timing.window_materialize_s),
        "train_h2d_transfer_ms_per_batch": _avg_ms(train_timing.h2d_transfer_s),
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
        "train_clip_cuda_ms_per_batch": _avg_ms(train_timing.clip_cuda_s),
        "train_finite_check_ms_per_batch": _avg_ms(train_timing.finite_check_s),
        "train_step_ms_per_batch": _avg_ms(train_timing.step_s),
        "train_step_cuda_ms_per_batch": _avg_ms(train_timing.step_cuda_s),
        "train_scheduler_ms_per_batch": _avg_ms(train_timing.scheduler_s),
        "val_eval_s": float(val_eval_s),
        "val_transfer_s": float(val_timing.transfer_s),
        "val_eval_transfer_to_gpu_s": float(val_timing.transfer_s),
        "val_batch_prepare_s": float(val_timing.batch_prepare_s),
        "val_eval_batch_prepare_s": float(val_timing.batch_prepare_s),
        "val_prepare_index_build_s": float(val_timing.prepare_index_build_s),
        "val_prepare_window_slice_s": float(val_timing.prepare_window_slice_s),
        "val_prepare_feature_gather_s": float(val_timing.prepare_feature_gather_s),
        "val_prepare_target_gather_s": float(val_timing.prepare_target_gather_s),
        "val_prepare_mask_build_s": float(val_timing.prepare_mask_build_s),
        "val_prepare_nan_fill_s": float(val_timing.prepare_nan_fill_s),
        "val_prepare_dtype_cast_s": float(val_timing.prepare_dtype_cast_s),
        "val_prepare_contiguous_s": float(val_timing.prepare_contiguous_s),
        "val_prepare_clone_s": float(val_timing.prepare_clone_s),
        "val_prepare_device_move_s": float(val_timing.prepare_device_move_s),
        "val_prepare_panel_slab_batch_s": float(val_timing.prepare_panel_slab_batch_s),
        "val_prepare_metadata_batch_s": float(val_timing.prepare_metadata_batch_s),
        "val_prepare_materialized_batch_s": float(val_timing.prepare_materialized_batch_s),
        "val_prepare_move_windowed_batch_s": float(val_timing.prepare_move_windowed_batch_s),
        "val_window_materialize_s": float(val_timing.window_materialize_s),
        "val_eval_window_materialize_s": float(val_timing.window_materialize_s),
        "val_h2d_transfer_s": float(val_timing.h2d_transfer_s),
        "val_eval_h2d_transfer_s": float(val_timing.h2d_transfer_s),
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
        "test_curve_batch_prepare_s": float(test_curve_timing.batch_prepare_s),
        "test_curve_eval_batch_prepare_s": float(test_curve_timing.batch_prepare_s),
        "test_curve_prepare_index_build_s": float(test_curve_timing.prepare_index_build_s),
        "test_curve_prepare_window_slice_s": float(test_curve_timing.prepare_window_slice_s),
        "test_curve_prepare_feature_gather_s": float(test_curve_timing.prepare_feature_gather_s),
        "test_curve_prepare_target_gather_s": float(test_curve_timing.prepare_target_gather_s),
        "test_curve_prepare_mask_build_s": float(test_curve_timing.prepare_mask_build_s),
        "test_curve_prepare_nan_fill_s": float(test_curve_timing.prepare_nan_fill_s),
        "test_curve_prepare_dtype_cast_s": float(test_curve_timing.prepare_dtype_cast_s),
        "test_curve_prepare_contiguous_s": float(test_curve_timing.prepare_contiguous_s),
        "test_curve_prepare_clone_s": float(test_curve_timing.prepare_clone_s),
        "test_curve_prepare_device_move_s": float(test_curve_timing.prepare_device_move_s),
        "test_curve_prepare_panel_slab_batch_s": float(test_curve_timing.prepare_panel_slab_batch_s),
        "test_curve_prepare_metadata_batch_s": float(test_curve_timing.prepare_metadata_batch_s),
        "test_curve_prepare_materialized_batch_s": float(test_curve_timing.prepare_materialized_batch_s),
        "test_curve_prepare_move_windowed_batch_s": float(test_curve_timing.prepare_move_windowed_batch_s),
        "test_curve_window_materialize_s": float(test_curve_timing.window_materialize_s),
        "test_curve_eval_window_materialize_s": float(test_curve_timing.window_materialize_s),
        "test_curve_h2d_transfer_s": float(test_curve_timing.h2d_transfer_s),
        "test_curve_eval_h2d_transfer_s": float(test_curve_timing.h2d_transfer_s),
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
        "bt_compiled_runner_calls": int(backtest_runtime_stats.get("compiled_runner_calls", 0.0)),
        "bt_eager_runner_calls": int(backtest_runtime_stats.get("eager_runner_calls", 0.0)),
        "bt_stateful_calls": int(backtest_runtime_stats.get("stateful_calls", 0.0)),
        "bt_stateful_compiled_runner_calls": int(backtest_runtime_stats.get("stateful_compiled_runner_calls", 0.0)),
        "bt_stateful_eager_runner_calls": int(backtest_runtime_stats.get("stateful_eager_runner_calls", 0.0)),
        "bt_nonstateful_compiled_runner_calls": int(backtest_runtime_stats.get("nonstateful_compiled_runner_calls", 0.0)),
        "bt_nonstateful_eager_runner_calls": int(backtest_runtime_stats.get("nonstateful_eager_runner_calls", 0.0)),
        "bt_runtime_fallback_calls": int(backtest_runtime_stats.get("runtime_fallback_calls", 0.0)),
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
        "scheduler_s": float(scheduler_s) + float(train_timing.scheduler_s),
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
        "epoch_step_test_curve_batch_prepare_total_time_s": float(test_curve_timing.batch_prepare_s),
        "epoch_step_test_curve_batch_prepare_percent": _epoch_percent(test_curve_timing.batch_prepare_s),
        "epoch_step_test_curve_batch_prepare_calls": int(test_curve_timing.batches),
        "epoch_step_test_curve_batch_prepare_ms_per_call": _timing_ms_per_call(test_curve_timing.batch_prepare_s, test_curve_timing.batches),
        "epoch_step_test_curve_h2d_transfer_total_time_s": float(test_curve_timing.h2d_transfer_s),
        "epoch_step_test_curve_h2d_transfer_percent": _epoch_percent(test_curve_timing.h2d_transfer_s),
        "epoch_step_test_curve_h2d_transfer_calls": int(test_curve_timing.batches),
        "epoch_step_test_curve_h2d_transfer_ms_per_call": _timing_ms_per_call(test_curve_timing.h2d_transfer_s, test_curve_timing.batches),
        "epoch_step_val_transfer_total_time_s": float(val_timing.transfer_s),
        "epoch_step_val_transfer_percent": _epoch_percent(val_timing.transfer_s),
        "epoch_step_val_transfer_calls": int(val_timing.batches),
        "epoch_step_val_transfer_ms_per_call": _timing_ms_per_call(val_timing.transfer_s, val_timing.batches),
        "epoch_step_val_batch_prepare_total_time_s": float(val_timing.batch_prepare_s),
        "epoch_step_val_batch_prepare_percent": _epoch_percent(val_timing.batch_prepare_s),
        "epoch_step_val_batch_prepare_calls": int(val_timing.batches),
        "epoch_step_val_batch_prepare_ms_per_call": _timing_ms_per_call(val_timing.batch_prepare_s, val_timing.batches),
        "epoch_step_val_h2d_transfer_total_time_s": float(val_timing.h2d_transfer_s),
        "epoch_step_val_h2d_transfer_percent": _epoch_percent(val_timing.h2d_transfer_s),
        "epoch_step_val_h2d_transfer_calls": int(val_timing.batches),
        "epoch_step_val_h2d_transfer_ms_per_call": _timing_ms_per_call(val_timing.h2d_transfer_s, val_timing.batches),
    }


def _summary_path(output_path: Path) -> Path:
    return output_path / "summary.json"


def _load_fold_result(metrics_path: Path) -> FoldResult:
    with metrics_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return FoldResult(**payload)


def _write_fold_complete_marker(
    fold_dir: Path,
    fold_result: FoldResult,
    *,
    source: str,
) -> None:
    if not _distributed_should_write():
        return

    def _date_coverage(path: Path) -> dict[str, int | str | None]:
        if not path.exists():
            return {"rows": 0, "date_start": None, "date_end": None}
        with np.load(path) as archive:
            dates = np.asarray(archive["dates"], dtype="datetime64[D]").reshape(-1)
        dates = dates[~np.isnat(dates)]
        return {
            "rows": int(dates.size),
            "date_start": str(dates.min()) if dates.size else None,
            "date_end": str(dates.max()) if dates.size else None,
        }

    test_coverage = _date_coverage(_backtest_path(fold_dir))
    deployment_coverage = _date_coverage(_deployment_backtest_path(fold_dir))
    marker = {
        "artifact_scope_version": 2,
        "status": "complete",
        "source": str(source),
        "fold_id": int(fold_result.fold_id),
        "train_years": [int(year) for year in fold_result.train_years],
        "val_years": [int(year) for year in fold_result.val_years],
        "test_years": [int(year) for year in fold_result.test_years],
        "metrics_json": _metrics_path(fold_dir).name,
        "model_path": _model_path(fold_dir).name,
        "checkpoint_path": (
            _best_checkpoint_path(fold_dir).name
            if _best_checkpoint_path(fold_dir).exists()
            else None
        ),
        "backtest_npz": _backtest_path(fold_dir).name,
        "test_scope": "full_horizon",
        "test_rows": test_coverage["rows"],
        "test_date_start": test_coverage["date_start"],
        "test_date_end": test_coverage["date_end"],
        "deployment_backtest_npz": (
            _deployment_backtest_path(fold_dir).name
            if _deployment_backtest_path(fold_dir).exists()
            else None
        ),
        "deployment_scope": "stitched_deployment",
        "deployment_rows": deployment_coverage["rows"],
        "deployment_date_start": deployment_coverage["date_start"],
        "deployment_date_end": deployment_coverage["date_end"],
        "written_at_unix_seconds": float(time.time()),
    }
    marker_path = _fold_complete_marker_path(fold_dir)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    with marker_path.open("w", encoding="utf-8") as handle:
        json.dump(marker, handle, indent=2)


def _read_fold_complete_marker(fold_dir: Path) -> dict[str, Any] | None:
    marker_path = _fold_complete_marker_path(fold_dir)
    if not marker_path.exists():
        return None
    try:
        with marker_path.open("r", encoding="utf-8") as handle:
            marker = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return marker if isinstance(marker, dict) else None


def _has_completed_fold_marker(fold_dir: Path) -> bool:
    marker = _read_fold_complete_marker(fold_dir)
    if marker is None:
        return False
    try:
        scope_version = int(marker.get("artifact_scope_version", 0))
    except (TypeError, ValueError):
        scope_version = 0
    return (
        str(marker.get("status", "")).lower() == "complete"
        and scope_version >= 2
        and str(marker.get("test_scope", "")) == "full_horizon"
        and str(marker.get("deployment_scope", "")) == "stitched_deployment"
        and _backtest_path(fold_dir).exists()
        and _deployment_backtest_path(fold_dir).exists()
    )


def _fold_needs_artifact_scope_migration(fold_dir: Path) -> bool:
    marker = _read_fold_complete_marker(fold_dir)
    return (
        marker is not None
        and str(marker.get("status", "")).lower() == "complete"
        and not _has_completed_fold_marker(fold_dir)
    )


def _write_summary(results: list[FoldResult], output_path: Path) -> None:
    if not _distributed_should_write():
        return
    summary_path = _summary_path(output_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)


def _unwrap_model(model: nn.Module) -> nn.Module:
    target = model
    seen: set[int] = set()
    while True:
        if id(target) in seen:
            return target
        seen.add(id(target))
        next_target = getattr(target, "_orig_mod", None)
        if next_target is None and isinstance(target, _PanelSlabForwardWrapper):
            next_target = getattr(target, "model", None)
        if next_target is None and isinstance(target, DistributedDataParallel):
            next_target = getattr(target, "module", None)
        if next_target is None or next_target is target:
            return target
        target = next_target


def _unwrap_distributed_data_parallel(model: nn.Module) -> nn.Module:
    if isinstance(model, DistributedDataParallel):
        return getattr(model, "module", model)
    return model


def _clip_model_gradients_(model: nn.Module, max_norm: float) -> torch.Tensor | None:
    params = [param for param in _unwrap_model(model).parameters() if param.grad is not None]
    if not params:
        return None
    try:
        return torch.nn.utils.clip_grad_norm_(
            params,
            max_norm=float(max_norm),
            error_if_nonfinite=False,
            foreach=True,
        )
    except RuntimeError as exc:
        message = str(exc).lower()
        if "foreach" not in message and "not implemented" not in message and "not support" not in message:
            raise
    return torch.nn.utils.clip_grad_norm_(
        params,
        max_norm=float(max_norm),
        error_if_nonfinite=False,
        foreach=False,
    )


def _run_gradient_clip_(
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    *,
    grad_clip_norm: float,
    timing: TimingBreakdown,
    device: torch.device,
    profile_timing: bool,
) -> torch.Tensor | None:
    if float(grad_clip_norm) <= 0.0:
        return None
    clip_start = time.perf_counter()
    with _cuda_timing(timing, "clip_cuda_s", device, enabled=profile_timing):
        if scaler.is_enabled():
            scaler.unscale_(optimizer)
        gradient_total_norm = _clip_model_gradients_(model, float(grad_clip_norm))
    _maybe_sync_cuda(device, profile_timing)
    timing.clip_s += time.perf_counter() - clip_start
    return gradient_total_norm


def _panel_forward_module(model: nn.Module) -> nn.Module | None:
    if bool(getattr(model, "_stockagent_disable_panel_forward", False)):
        return None
    if hasattr(model, "forward_from_panel"):
        return model
    unwrapped = _unwrap_model(model)
    return unwrapped if hasattr(unwrapped, "forward_from_panel") else None


def _panel_slab_forward_module(model: nn.Module) -> nn.Module | None:
    if bool(getattr(model, "_stockagent_disable_panel_forward", False)):
        return None
    if hasattr(model, "forward_from_panel_slab"):
        return model
    unwrapped = _unwrap_model(model)
    return unwrapped if hasattr(unwrapped, "forward_from_panel_slab") else None


def _model_supports_panel_slab_forward(model: nn.Module) -> bool:
    return _panel_slab_forward_module(model) is not None


def _callable_accepts_parameter(fn: Callable[..., Any], parameter: str) -> bool:
    try:
        return str(parameter) in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


class _PanelSlabForwardWrapper(nn.Module):
    def __init__(self, model: nn.Module) -> None:
        super().__init__()
        slab_model = _panel_slab_forward_module(model)
        if slab_model is None:
            raise ValueError("model does not support forward_from_panel_slab")
        self.model = slab_model
        self._accepts_symbol_indices = _callable_accepts_parameter(
            self.model.forward_from_panel_slab,
            "symbol_indices",
        )

    def forward(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        # A fixed slab can contain sample-masked dates that were absent from
        # CrossSectionalDataset because every symbol was non-tradable.  The
        # Transformer attention path requires at least one visible token per
        # row.  Expose one dummy token to the model only; callers retain the
        # original all-false trading mask for the canonical loss/backtest, so
        # this cannot create a position, return, fee, or recurrent state update.
        mask = _model_attention_mask(mask)
        kwargs: dict[str, Any] = {"return_aux": False}
        if symbol_indices is not None:
            if not self._accepts_symbol_indices:
                raise ValueError("compact symbol batches require forward_from_panel_slab(..., symbol_indices=...)")
            kwargs["symbol_indices"] = symbol_indices
        return self.model.forward_from_panel_slab(feature_slab, mask, **kwargs)


def _metadata_rows_are_contiguous(batch: Mapping[str, torch.Tensor]) -> bool:
    flag = batch.get("rows_are_contiguous")
    if flag is not None:
        return bool(flag.detach().cpu().item())
    date_indices = batch.get("date_indices")
    if date_indices is None or int(date_indices.numel()) <= 1:
        return True
    diffs = date_indices[1:] - date_indices[:-1]
    return bool(torch.all(diffs == 1).detach().cpu().item())


def _feature_slab_from_metadata(
    split: WindowedSplitTensors,
    batch: Mapping[str, torch.Tensor],
    device: torch.device,
    non_blocking: bool,
    *,
    rows: int | None = None,
) -> torch.Tensor | None:
    if not _metadata_rows_are_contiguous(batch):
        return None
    date_start_tensor = batch.get("date_start")
    if date_start_tensor is None:
        date_indices = batch.get("date_indices")
        if date_indices is None or int(date_indices.numel()) == 0:
            return None
        date_start_tensor = date_indices[:1]
    if int(date_start_tensor.numel()) == 0:
        return None
    batch_rows = int(rows) if rows is not None else int(batch["date_indices"].numel())
    if batch_rows <= 0:
        return None
    date_start = int(date_start_tensor.reshape(-1)[0].detach().cpu().item())
    feature_start = date_start - int(split.lookback) + 1
    slab_rows = batch_rows + int(split.lookback) - 1
    if feature_start < 0 or feature_start + slab_rows > int(split.features.size(0)):
        return None
    feature_slab = split.features.narrow(0, feature_start, slab_rows)
    if feature_slab.device != device:
        feature_slab = feature_slab.to(device=device, non_blocking=non_blocking)
    return feature_slab


_WINDOWED_CONTROL_KEYS = frozenset({"date_start", "rows_are_contiguous"})


def _windowed_prepare_device(split: WindowedSplitTensors, target_device: torch.device) -> torch.device:
    if split.features.device == target_device:
        return target_device
    return split.features.device


def _move_windowed_batch_to_device(
    batch: Mapping[str, torch.Tensor],
    device: torch.device,
    non_blocking: bool,
) -> dict[str, torch.Tensor]:
    moved: dict[str, torch.Tensor] = {}
    for key, value in batch.items():
        if key in _WINDOWED_CONTROL_KEYS:
            moved[key] = value
        else:
            moved[key] = value.to(device=device, non_blocking=non_blocking)
    return moved


def _call_panel_forward_for_batch(
    *,
    panel_forward_model: nn.Module,
    panel_slab_model: nn.Module | None,
    split: WindowedSplitTensors,
    batch: Mapping[str, torch.Tensor],
    mask: torch.Tensor,
    device: torch.device,
    non_blocking: bool,
    return_aux: bool | None,
    allow_slab: bool = True,
    rows: int | None = None,
):
    model_mask = _model_attention_mask(mask)
    if allow_slab and panel_slab_model is not None and return_aux is False:
        feature_slab = _feature_slab_from_metadata(
            split,
            batch,
            device,
            non_blocking,
            rows=rows,
        )
        if feature_slab is not None:
            return panel_slab_model(feature_slab, model_mask, batch.get("symbol_indices"))
    kwargs: dict[str, Any] = {"return_aux": return_aux}
    symbol_indices = batch.get("symbol_indices")
    if symbol_indices is not None:
        if not _callable_accepts_parameter(panel_forward_model.forward_from_panel, "symbol_indices"):
            raise ValueError("compact symbol batches require forward_from_panel(..., symbol_indices=...)")
        kwargs["symbol_indices"] = symbol_indices
    return panel_forward_model.forward_from_panel(split.features, batch["date_indices"], model_mask, **kwargs)


def _objective_uses_deterministic_multitask_aux(objective: str) -> bool:
    objective_norm = _normalize_risk_objective(objective)
    return objective_norm not in {
        "pure_rank",
        "factor_generalization",
        "portfolio_autoencoder",
    }


def _training_needs_aux(
    objective: str,
    factor_aug_kwargs: dict[str, float] | None = None,
    *,
    direction_weight: float = 0.0,
    volatility_regime_weight: float = 0.0,
) -> bool:
    objective_norm = _normalize_risk_objective(objective)
    objective_aux = objective_norm in {
        "rank_ic",
        "pure_rank",
        "factor_generalization",
        "portfolio_autoencoder",
    }
    deterministic_multitask_aux = _objective_uses_deterministic_multitask_aux(objective_norm) and (
        float(direction_weight) > 0.0 or float(volatility_regime_weight) > 0.0
    )
    return objective_aux or bool(factor_aug_kwargs) or deterministic_multitask_aux


def _requires_full_objective_evaluation(
    objective: str,
    *,
    return_rank_ic_weight: float,
    direction_weight: float,
    volatility_regime_weight: float,
) -> bool:
    """Whether validation/test must retain model outputs beyond backtest series."""
    objective_norm = _normalize_risk_objective(objective)
    return (
        objective_norm in {
            "rank_ic",
            "pure_rank",
            "factor_generalization",
            "portfolio_autoencoder",
        }
        or float(return_rank_ic_weight) > 0.0
        or (
            _objective_uses_deterministic_multitask_aux(objective_norm)
            and (float(direction_weight) > 0.0 or float(volatility_regime_weight) > 0.0)
        )
    )


def _require_training_aux_outputs(
    objective: str,
    aux_outputs: Mapping[str, torch.Tensor] | None,
    *,
    factor_aug_required: bool = False,
    direction_weight: float = 0.0,
    volatility_regime_weight: float = 0.0,
) -> None:
    """Fail only when an enabled loss component lacks its required tensor."""
    objective_norm = _normalize_risk_objective(objective)
    available = aux_outputs or {}
    if objective_norm == "portfolio_autoencoder" and not any(
        isinstance(available.get(key), torch.Tensor) for key in ("latent_z", "z")
    ):
        raise RuntimeError(
            "objective='portfolio_autoencoder' requires aux['latent_z'] or aux['z']; "
            f"available keys={sorted(available)}"
        )
    if factor_aug_required and not isinstance(available.get("aug_score_logits"), torch.Tensor):
        raise RuntimeError(
            "factor consistency augmentation requires aux['aug_score_logits']; "
            f"available keys={sorted(available)}"
        )
    if not _objective_uses_deterministic_multitask_aux(objective_norm):
        return
    if float(direction_weight) > 0.0 and not isinstance(available.get("score_logits"), torch.Tensor):
        raise RuntimeError(
            "direction_weight > 0 requires aux['score_logits']; "
            f"objective={objective_norm!r}, available keys={sorted(available)}"
        )
    if float(volatility_regime_weight) > 0.0 and not any(
        isinstance(available.get(key), torch.Tensor) for key in ("volatility_pred", "regime_logits")
    ):
        raise RuntimeError(
            "volatility_regime_weight > 0 requires aux['volatility_pred'] or aux['regime_logits']; "
            f"objective={objective_norm!r}, available keys={sorted(available)}"
        )


def _state_dict_for_save(model: nn.Module) -> dict[str, torch.Tensor]:
    return _unwrap_model(model).state_dict()


def _first_nonfinite_model_parameter(model: nn.Module) -> str | None:
    for name, param in _unwrap_model(model).named_parameters():
        if not torch.isfinite(param.detach()).all():
            return name
    return None


def _model_parameters_are_finite(model: nn.Module) -> bool:
    parameters = [param.detach() for param in _unwrap_model(model).parameters()]
    if not parameters:
        return True
    try:
        # Infinity-norm is finite exactly when every tensor element is finite,
        # and foreach reduces all parameter tensors without launching a separate
        # isfinite/all/logical-and chain for each one.
        norms = torch._foreach_norm(parameters, float("inf"))
        return bool(torch.isfinite(torch.stack(norms)).all().item())
    except (RuntimeError, TypeError):
        finite_flag: torch.Tensor | None = None
        for param in parameters:
            current = torch.isfinite(param).all()
            finite_flag = current if finite_flag is None else torch.logical_and(finite_flag, current)
        return bool(finite_flag.item()) if finite_flag is not None else True


def _model_gradients_are_finite(model: nn.Module) -> bool:
    gradients = [
        param.grad.detach()
        for param in _unwrap_model(model).parameters()
        if param.grad is not None
    ]
    if not gradients:
        return True
    try:
        norms = torch._foreach_norm(gradients, float("inf"))
        return bool(torch.isfinite(torch.stack(norms)).all().item())
    except (RuntimeError, TypeError):
        finite_flag: torch.Tensor | None = None
        for gradient in gradients:
            current = torch.isfinite(gradient).all()
            finite_flag = current if finite_flag is None else torch.logical_and(finite_flag, current)
        return bool(finite_flag.item()) if finite_flag is not None else True


def _stabilize_model_parameters_after_step(model: nn.Module) -> None:
    target = _unwrap_model(model)
    hook = getattr(target, "stabilize_parameters_after_step_", None)
    if callable(hook):
        hook()


def _should_check_finite(step: int, interval_steps: int, *, final_step: bool = False) -> bool:
    interval = int(interval_steps)
    if interval <= 0:
        return False
    return bool(final_step) or int(step) % interval == 0


def _raise_if_model_parameters_nonfinite(model: nn.Module, context: str) -> None:
    bad_name = _first_nonfinite_model_parameter(model)
    if bad_name is not None:
        raise RuntimeError(f"{context}: first non-finite parameter={bad_name}")


def _checkpoint_parameters_are_saveable(
    model: nn.Module,
    checkpoint_path: Path,
    label: str,
    *,
    check_finite: bool = True,
) -> bool:
    _stabilize_model_parameters_after_step(model)
    if not bool(check_finite):
        return True
    if _model_parameters_are_finite(model):
        return True
    bad_name = _first_nonfinite_model_parameter(model) or "<unknown>"
    message = (
        f"non-finite model checkpoint not saved: {checkpoint_path}: "
        f"first non-finite parameter={bad_name}"
    )
    if checkpoint_path.exists():
        print(f"[checkpoint] skipped {label}; keeping existing checkpoint; training continues. {message}")
        return False
    print(f"[checkpoint] skipped {label}; no existing checkpoint yet; training continues and will retry. {message}")
    return False


def _tensor_is_finite(value: torch.Tensor) -> bool:
    return bool(torch.isfinite(value.detach()).all().item())


def _load_state_dict(model: nn.Module, state_dict: dict) -> None:
    def _strip_wrapper_prefixes(key: str) -> str:
        cleaned = str(key)
        while True:
            previous = cleaned
            for prefix in ("module.", "_orig_mod."):
                if cleaned.startswith(prefix):
                    cleaned = cleaned[len(prefix) :]
                    break
            if cleaned == previous:
                return cleaned

    cleaned_state_dict = {
        _strip_wrapper_prefixes(key): value for key, value in state_dict.items()
    }
    target = _unwrap_model(model)
    try:
        target.load_state_dict(cleaned_state_dict)
        return
    except RuntimeError as exc:
        message = str(exc)
        if not (hasattr(target, "forward_from_panel") and "Unexpected key(s)" in message):
            raise
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "Checkpoint state_dict is not strictly compatible with the model; "
                "strict_no_fallback=true so strict=False checkpoint loading is disabled."
            ) from exc

    incompatible = target.load_state_dict(cleaned_state_dict, strict=False)
    allowed_prefixes = (
        "cross_blocks.",
        "joint_blocks.",
        "latent_queries",
        "market_queries",
        "temporal_pool_score.",
        "latent_blocks.",
        "market_blocks.",
        "stock_read_latent_blocks.",
        "stock_read_market_blocks.",
    )
    unexpected = list(getattr(incompatible, "unexpected_keys", []))
    missing = list(getattr(incompatible, "missing_keys", []))
    disallowed_unexpected = [
        key for key in unexpected if not any(key.startswith(prefix) for prefix in allowed_prefixes)
    ]
    if missing or disallowed_unexpected:
        details = []
        if missing:
            details.append(f"missing={missing[:8]}")
        if disallowed_unexpected:
            details.append(f"unexpected={disallowed_unexpected[:8]}")
        raise RuntimeError("Checkpoint is incompatible with model state_dict: " + ", ".join(details))
    if unexpected:
        _progress(
            "Loaded checkpoint with strict=False; ignored unused TransformerBasePortfolioModel keys: "
            + ", ".join(unexpected[:8])
            + (" ..." if len(unexpected) > 8 else "")
        )


def _stable_fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _array_content_fingerprint(value: np.ndarray | None) -> dict[str, Any]:
    """Hash an array's complete logical C-order content without a full-size copy."""
    if value is None:
        return {"present": False}

    array = np.asarray(value)
    digest = hashlib.sha256()
    header = {
        "present": True,
        "shape": [int(size) for size in array.shape],
        "dtype": str(array.dtype),
    }
    digest.update(
        json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    if array.dtype.hasobject:
        for item in array.flat:
            encoded = json.dumps(item, ensure_ascii=False, default=str).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, byteorder="little", signed=False))
            digest.update(encoded)
    elif array.flags.c_contiguous:
        digest.update(memoryview(array.view(np.uint8).reshape(-1)))
    else:
        iterator = np.nditer(
            array,
            flags=["external_loop", "buffered", "zerosize_ok"],
            op_flags=["readonly"],
            order="C",
            buffersize=1 << 20,
        )
        for chunk in iterator:
            contiguous = np.ascontiguousarray(chunk)
            digest.update(memoryview(contiguous.view(np.uint8).reshape(-1)))
    header["sha256"] = digest.hexdigest()
    return header


def _active_model_config(config: ExperimentConfig) -> dict[str, Any]:
    normalized = _normalized_model_name(config.training.model_name)
    aliases = {
        "cross_sectional_mlp": "mlp",
        "ft": "ft_transformer",
        "transformer": "ft_transformer",
        "tabresnet": "tabular_resnet",
        "resnet": "tabular_resnet",
        "simple_multi_stock_tcn": "multi_stock_tcn",
        "mean_pool_tcn": "multi_stock_tcn",
        "efficient_tcn_tabular_set_portfolio_model": "efficient_tcn_tabular_set_portfolio",
        "efficient_portfolio": "efficient_tcn_tabular_set_portfolio",
        "lite_isab_portfolio": "efficient_tcn_tabular_set_portfolio",
        "latent_factor_market_token_portfolio_model": "latent_factor_market_token_portfolio",
        "latent_factor_market_token": "latent_factor_market_token_portfolio",
        "lfmt_portfolio": "latent_factor_market_token_portfolio",
        "latent_market_token_portfolio": "latent_factor_market_token_portfolio",
        "low_rank_market_transformer_portfolio_model": "low_rank_market_transformer_portfolio",
        "temporal_latent_factor_market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "factorized_market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "lrmt_portfolio": "low_rank_market_transformer_portfolio",
        "lgbm": "lightgbm",
        "xgb": "xgboost",
        "bottleneck_autoencoder": "bottleneck_portfolio_autoencoder",
        "portfolio_autoencoder": "bottleneck_portfolio_autoencoder",
        "bpae": "bottleneck_portfolio_autoencoder",
        "transformer_base_portfolio_model": "transformer_base_portfolio",
        "flash_transformer_portfolio": "transformer_base_portfolio",
        "scalable_transformer_portfolio": "transformer_base_portfolio",
        "multi_axis_transformer_portfolio": "transformer_base_portfolio",
        "tbp": "transformer_base_portfolio",
        "gradient_boosted_portfolio_transformer_model": "gradient_boosted_portfolio_transformer",
        "gradient_boosted_transformer_portfolio": "gradient_boosted_portfolio_transformer",
        "boosted_portfolio_transformer": "gradient_boosted_portfolio_transformer",
        "boosted_transformer_portfolio": "gradient_boosted_portfolio_transformer",
        "gbpt": "gradient_boosted_portfolio_transformer",
        "cstpm": "cross_sectional_temporal_portfolio_model",
        "portfolio_multitask": "cross_sectional_temporal_portfolio_model",
        "tcn_hybrid": "tcn_hybrid_tabular_resnet",
        "tcn_tabresnet": "tcn_hybrid_tabular_resnet",
        "temporal_resnet": "temporal_tabular_resnet",
        "temporal_tabresnet": "temporal_tabular_resnet",
    }
    candidates = [normalized, aliases.get(normalized, "")]
    for candidate in candidates:
        model_config = getattr(config.training, candidate, None)
        if model_config is not None:
            return {
                "config_name": candidate,
                "values": asdict(model_config),
            }
    raise ValueError(
        f"Cannot resolve active model config for training.model_name={config.training.model_name!r}"
    )


def _active_scheduler_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Fingerprint only the scheduler branch that can affect future optimizer steps."""
    training = config.training
    raw_name = str(training.lr_scheduler or "none").strip().lower().replace("-", "_")
    aliases = {
        "cosine_warmup": "warmup_cosine",
        "linear_warmup_cosine": "warmup_cosine",
        "reduce_on_plateau": "plateau",
        "reduce_lr_on_plateau": "plateau",
    }
    name = aliases.get(raw_name, raw_name)
    enabled = bool(training.enable_lr_scheduler) and name not in {
        "",
        "none",
        "off",
        "false",
        "disabled",
    }
    if name not in {"cosine", "warmup_cosine", "step", "plateau"}:
        enabled = False
    contract: dict[str, Any] = {"enabled": enabled}
    if not enabled:
        return contract

    contract["name"] = name
    if name == "cosine":
        configured_t_max = int(training.lr_scheduler_t_max)
        contract.update(
            {
                "t_max": (
                    configured_t_max
                    if configured_t_max > 0
                    else max(1, int(training.epochs))
                ),
                "t_max_source": "configured" if configured_t_max > 0 else "epochs",
                "eta_min": float(training.lr_scheduler_eta_min),
            }
        )
    elif name == "warmup_cosine":
        configured_total_steps = int(training.lr_scheduler_t_max)
        contract.update(
            {
                "configured_total_steps": max(0, configured_total_steps),
                "derived_epochs": (
                    max(1, int(training.epochs))
                    if configured_total_steps <= 0
                    else None
                ),
                "eta_min": max(0.0, float(training.lr_scheduler_eta_min)),
                "warmup_steps": max(0, int(training.lr_scheduler_warmup_steps)),
                "interval": "step",
            }
        )
    elif name == "step":
        contract.update(
            {
                "step_size": max(1, int(training.lr_scheduler_step_size)),
                "gamma": float(training.lr_scheduler_gamma),
            }
        )
    else:
        contract.update(
            {
                "gamma": float(training.lr_scheduler_gamma),
                "patience": max(1, int(training.lr_scheduler_patience)),
                "threshold": float(training.lr_scheduler_threshold),
            }
        )
    return contract


def _active_multitask_checkpoint_contract(
    config: ExperimentConfig,
    objective: str,
) -> dict[str, Any]:
    """Return only multitask controls consumed by ``risk_aware_loss`` for this objective."""
    cfg = config.training.multitask_loss
    objective = _normalize_risk_objective(objective)
    if objective == "pure_rank":
        return {"rank_ic_weight": float(cfg.rank_ic_weight)}
    if objective == "rank_ic":
        direction_weight = max(0.0, float(cfg.direction_weight))
        volatility_weight = max(0.0, float(cfg.volatility_regime_weight))
        contract = {
            "rank_ic_weight": float(cfg.rank_ic_weight),
            "direction_weight": direction_weight,
            "volatility_regime_weight": volatility_weight,
        }
        if volatility_weight > 0.0:
            contract.update(
                {
                    "regime_up_threshold": float(cfg.regime_up_threshold),
                    "regime_down_threshold": float(cfg.regime_down_threshold),
                }
            )
        return contract
    if objective == "factor_generalization":
        # The factor objective consumes these two target-regime thresholds even
        # though its remaining weights live in factor_generalization_loss.
        if float(config.training.factor_generalization_loss.regime_stability_weight) > 0.0:
            return {
                "regime_up_threshold": float(cfg.regime_up_threshold),
                "regime_down_threshold": float(cfg.regime_down_threshold),
            }
        return {}
    if objective == "portfolio_autoencoder":
        return {}

    volatility_weight = max(0.0, float(cfg.volatility_regime_weight))
    contract = {
        "return_rank_ic_weight": max(0.0, float(cfg.return_rank_ic_weight)),
        "direction_weight": max(0.0, float(cfg.direction_weight)),
        "volatility_regime_weight": volatility_weight,
        "concentration_weight": max(0.0, float(cfg.concentration_weight)),
        "net_exposure_weight": max(0.0, float(cfg.net_exposure_weight)),
    }
    if volatility_weight > 0.0:
        contract.update(
            {
                "regime_up_threshold": float(cfg.regime_up_threshold),
                "regime_down_threshold": float(cfg.regime_down_threshold),
            }
        )
    return contract


def _active_factor_loss_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    cfg = config.training.factor_generalization_loss
    contract: dict[str, Any] = {
        "slope_tstat_weight": max(0.0, float(cfg.slope_tstat_weight)),
        "rank_ic_weight": max(0.0, float(cfg.rank_ic_weight)),
        "factor_sharpe_weight": max(0.0, float(cfg.factor_sharpe_weight)),
        "block_stability_weight": max(0.0, float(cfg.block_stability_weight)),
        "regime_stability_weight": max(0.0, float(cfg.regime_stability_weight)),
        "consistency_weight": max(0.0, float(cfg.consistency_weight)),
        "net_exposure_weight": max(0.0, float(cfg.net_exposure_weight)),
        "gross_exposure_weight": max(0.0, float(cfg.gross_exposure_weight)),
        "concentration_weight": max(0.0, float(cfg.concentration_weight)),
        "turnover_weight": max(0.0, float(cfg.turnover_weight)),
        "score_l2_weight": max(0.0, float(cfg.score_l2_weight)),
        "factor_temperature": max(0.05, float(cfg.factor_temperature)),
    }
    if contract["block_stability_weight"] > 0.0:
        contract["block_count"] = max(2, int(cfg.block_count))
        contract["worst_fraction"] = min(max(float(cfg.worst_fraction), 0.0), 1.0)
    if contract["consistency_weight"] > 0.0:
        contract["augmentation"] = {
            "feature_dropout": min(max(float(cfg.augmentation_feature_dropout), 0.0), 0.95),
            "stock_dropout": min(max(float(cfg.augmentation_stock_dropout), 0.0), 0.95),
            "time_dropout": min(max(float(cfg.augmentation_time_dropout), 0.0), 0.95),
            "noise_std": max(0.0, float(cfg.augmentation_noise_std)),
        }
    return contract


def _active_autoencoder_loss_checkpoint_contract(config: ExperimentConfig) -> dict[str, float]:
    cfg = config.training.portfolio_autoencoder_loss
    return {
        "lambda_turnover": max(0.0, float(cfg.lambda_turnover)),
        "lambda_concentration": max(0.0, float(cfg.lambda_concentration)),
        "lambda_latent": max(0.0, float(cfg.lambda_latent)),
    }


def _training_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Return the explicit schema-4 controls that can change a resumed trajectory.

    Machine-local execution, compilation, ordinary cache placement, DDP, eval
    chunking, VRAM, plotting, explainability, table, and post-processing settings
    deliberately remain in ``configuration`` only.  A cache option that changes
    immutable tensor storage precision is a semantic exception recorded below.
    Active model architecture is fingerprinted independently by the model contract.
    """
    training = config.training
    objective = _normalize_risk_objective(training.loss_type)

    # Tree estimators fit one fully materialized dataset and own their fit
    # hyperparameters in the active model contract. They never construct the
    # neural optimizer, scheduler, mini-batches, AMP/scaler, finite-step checks,
    # or fold warm-start state below. The normalized objective still determines
    # the persisted validation loss and objective-labelled evaluation artifacts.
    if _is_tree_model_name(training.model_name):
        return {"objective": objective}

    compaction_mode = _normalize_train_symbol_compaction(
        training.train_symbol_compaction
    )
    batching_contract: dict[str, Any] = {
        "batch_size_train": int(training.batch_size_train),
        "min_batch_size": int(training.min_batch_size),
        "auto_batch_size": bool(training.auto_batch_size),
        "train_symbol_compaction": compaction_mode,
    }
    if compaction_mode != "none":
        batching_contract["train_symbol_compaction_bucket_size"] = (
            _normalize_train_symbol_compaction_bucket_size(
                training.train_symbol_compaction_bucket_size
            )
        )
    contract: dict[str, Any] = {
        "seed": int(training.seed),
        "objective": objective,
        "optimizer": {
            "name": "AdamW",
            "learning_rate": float(training.learning_rate),
            "weight_decay": float(training.weight_decay),
            "grad_clip_norm": float(training.grad_clip_norm),
        },
        "scheduler": _active_scheduler_checkpoint_contract(config),
        "batching": batching_contract,
        "fold_continuation": {
            "warm_start_from_previous_fold": bool(training.warm_start_from_previous_fold),
        },
        "validation_and_stopping": {
            "early_stopping_no_improve_ratio": float(
                training.early_stopping_no_improve_ratio
            ),
            "early_stopping_min_delta": float(training.early_stopping_min_delta),
            "best_checkpoint_max_epoch": int(training.best_checkpoint_max_epoch),
            "val_interval_epochs": int(training.val_interval_epochs),
        },
        "finite_checks": {
            "finite_check_interval_steps": max(
                0,
                int(training.finite_check_interval_steps),
            ),
        },
        "precision": {
            "amp_dtype": str(config.environment.amp_dtype),
            "use_tensor_cores": bool(config.environment.use_tensor_cores),
        },
    }
    if bool(getattr(training, "cache_train_features_in_amp_dtype", False)):
        # This opt-in changes the immutable feature storage seen by the train
        # executor. Keep default=false schema-4 checkpoints backward compatible,
        # but never resume an opted-in trajectory under a different precision path.
        contract["precision"]["cache_train_features_in_amp_dtype"] = True
    # These rank objectives return before portfolio post-processing/backtest in
    # risk_aware_loss, so changing the loss activation cannot change gradients.
    if objective not in {"pure_rank", "rank_ic"}:
        contract["loss_portfolio_activation"] = normalize_portfolio_activation(
            _training_loss_portfolio_activation(config)
        )
        loss_min_trade_weight = _training_loss_min_trade_weight(config)
        if loss_min_trade_weight != float(config.trading.min_trade_weight):
            contract["loss_min_trade_weight"] = loss_min_trade_weight
    multitask = _active_multitask_checkpoint_contract(config, objective)
    if multitask:
        contract["multitask_loss"] = multitask
    if objective == "factor_generalization":
        contract["factor_generalization_loss"] = (
            _active_factor_loss_checkpoint_contract(config)
        )
    elif objective == "portfolio_autoencoder":
        contract["portfolio_autoencoder_loss"] = (
            _active_autoencoder_loss_checkpoint_contract(config)
        )
    return contract


def _training_checkpoint_contract_schema_3(
    config: ExperimentConfig,
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    """Exact training contract written by schema 3 before semantic layering."""
    contract = asdict(config.training)
    # This storage-precision option was introduced by schema 4.  It must not be
    # synthesized into a historical schema-3 fingerprint; the compatibility
    # constraint below separately prevents unsafe optimizer resume when enabled.
    contract.pop("cache_train_features_in_amp_dtype", None)
    # This decoupling control did not exist in schema 3; historical runs always
    # used trading.min_trade_weight in the loss.
    contract.pop("loss_min_trade_weight", None)
    # Schema 3 recorded this field even though no executor ever consumed it.
    # Reconstruct the only honest/effective historical value after removing the
    # no-op from TrainingConfig so old fingerprints remain verifiable.
    contract["train_symbol_subsample_ratio"] = 1.0
    contract["lr_scheduler_interval"] = str(legacy_lr_scheduler_interval)
    contract["precision"] = {
        "amp_dtype": str(config.environment.amp_dtype),
        "use_tensor_cores": bool(config.environment.use_tensor_cores),
    }
    return contract


def _training_checkpoint_contract_schema_2(
    config: ExperimentConfig,
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    """Compatibility contract emitted by the first layered-manifest release."""
    training = config.training
    model_values = _active_model_config(config)["values"]
    return {
        "seed": int(training.seed),
        "loss_type": str(training.loss_type),
        "loss_portfolio_activation": str(training.loss_portfolio_activation),
        "optimizer": {
            "name": "AdamW",
            "learning_rate": float(training.learning_rate),
            "weight_decay": float(training.weight_decay),
            "grad_clip_norm": float(training.grad_clip_norm),
        },
        "scheduler": {
            "enabled": bool(training.enable_lr_scheduler),
            "name": str(training.lr_scheduler),
            "interval": str(legacy_lr_scheduler_interval),
            "t_max": int(training.lr_scheduler_t_max),
            "eta_min": float(training.lr_scheduler_eta_min),
            "warmup_steps": int(training.lr_scheduler_warmup_steps),
            "step_size": int(training.lr_scheduler_step_size),
            "gamma": float(training.lr_scheduler_gamma),
            "patience": int(training.lr_scheduler_patience),
            "threshold": float(training.lr_scheduler_threshold),
        },
        "batching": {
            "batch_size_train": int(training.batch_size_train),
            "train_symbol_subsample_ratio": 1.0,
            "train_symbol_compaction": str(training.train_symbol_compaction),
            "train_symbol_compaction_bucket_size": int(training.train_symbol_compaction_bucket_size),
        },
        "precision": {
            "amp_dtype": str(config.environment.amp_dtype),
            "use_tensor_cores": bool(config.environment.use_tensor_cores),
        },
        "model_training_outputs": {
            name: model_values[name]
            for name in ("return_aux", "return_aux_details")
            if name in model_values
        },
        "multitask_loss": asdict(training.multitask_loss),
        "factor_generalization_loss": asdict(training.factor_generalization_loss),
        "portfolio_autoencoder_loss": asdict(training.portfolio_autoencoder_loss),
    }


def _trading_checkpoint_contract_schema_2(config: ExperimentConfig) -> dict[str, Any]:
    """Map the reporting-only leverage name back to the schema-2 spelling."""
    contract = asdict(config.trading)
    contract["leverage"] = contract.pop("reporting_leverage")
    return contract


def _legacy_checkpoint_setting_contract(
    config: ExperimentConfig,
    active_model: Mapping[str, Any],
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    return {
        "model_name": str(config.training.model_name),
        "model": active_model["values"],
        "loss_type": str(config.training.loss_type),
        "loss_portfolio_activation": str(config.training.loss_portfolio_activation),
        "trading": _trading_checkpoint_contract_schema_2(config),
        "evaluation": asdict(config.evaluation),
        "multitask_loss": asdict(config.training.multitask_loss),
        "optimizer": {
            "learning_rate": float(config.training.learning_rate),
            "weight_decay": float(config.training.weight_decay),
            "lr_scheduler": str(config.training.lr_scheduler),
            "lr_scheduler_interval": str(legacy_lr_scheduler_interval),
            "lr_scheduler_warmup_steps": int(
                config.training.lr_scheduler_warmup_steps
            ),
        },
        "train_symbol_compaction": str(config.training.train_symbol_compaction),
    }


def _trading_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Return trading controls consumed by canonical loss/backtest execution."""
    trading = config.trading
    return {
        "canonical_backtest_contract_version": int(
            CANONICAL_BACKTEST_CONTRACT_VERSION
        ),
        "buy_fee_rate": float(trading.buy_fee_rate),
        "sell_fee_rate": float(trading.sell_fee_rate),
        "long_only": bool(trading.long_only),
        "max_turnover_ratio": float(trading.max_turnover_ratio),
        "max_volume_participation": float(trading.max_volume_participation),
        "volume_participation_equity": float(trading.volume_participation_equity),
        "min_trade_weight": float(trading.min_trade_weight),
        "portfolio_activation": normalize_portfolio_activation(
            trading.portfolio_activation
        ),
    }


_SCHEMA_3_MODEL_RUNTIME_FIELDS = {
    "checkpoint_blocks",
    "max_full_tokens",
    "gpu_device_id",
    "n_jobs",
    "return_aux",
    "return_aux_details",
    "sdpa_batch_limit",
    "use_gpu",
    "use_flash_attention",
}
_SCHEMA_4_MODEL_RUNTIME_FIELDS = _SCHEMA_3_MODEL_RUNTIME_FIELDS | {
    "temporal_checkpoint",
}


def _schema_3_checkpoint_model_values(active_model: Mapping[str, Any]) -> dict[str, Any]:
    """Reproduce schema 3's exact generic model-field filtering."""
    return {
        name: value
        for name, value in dict(active_model["values"]).items()
        if name not in _SCHEMA_3_MODEL_RUNTIME_FIELDS
    }


def _effective_model_portfolio_mode(
    config: ExperimentConfig,
    config_name: str,
    values: Mapping[str, Any],
) -> str:
    if "portfolio_mode" in values:
        raw_mode = str(values["portfolio_mode"] or "").strip().lower().replace("-", "_")
        if raw_mode in {"", "auto"}:
            raw_mode = "long_only" if config.trading.long_only else "long_short"
        return normalize_portfolio_mode(raw_mode)
    if config_name == "bottleneck_portfolio_autoencoder":
        is_long_short = bool(values.get("long_short", False)) and not bool(
            config.trading.long_only
        )
        return "long_short" if is_long_short else "long_only"
    return "long_only" if config.trading.long_only else "long_short"


def _transformer_base_checkpoint_model_values(
    config: ExperimentConfig,
    values: Mapping[str, Any],
    feature_names: Sequence[str],
) -> dict[str, Any]:
    """Resolve only architecture/behavior fields active in the selected TBP mode."""
    mode = TransformerBasePortfolioModel._normalize_attention_mode(
        str(values["attention_mode"])
    )
    pooling = TransformerBasePortfolioModel._normalize_pooling(
        str(values["temporal_pooling"])
    )
    portfolio_mode = _effective_model_portfolio_mode(
        config,
        "transformer_base_portfolio",
        values,
    )
    output_mode = normalize_portfolio_output_mode(
        str(values["portfolio_output_mode"])
    )
    head_layers = max(0, int(values["head_layers"]))
    contract: dict[str, Any] = {
        "d_model": int(values["d_model"]),
        "attention_mode": mode,
        "use_time_pos": bool(values["use_time_pos"]),
        "use_symbol_pos": bool(values["use_symbol_pos"]),
        "input_dropout": float(values["input_dropout"]),
        "norm_type": _normalize_norm_type(str(values["norm_type"])),
        "ffn_type": _normalize_ffn_type(str(values["ffn_type"])),
        "qk_norm": bool(values["qk_norm"]),
        "temporal_pooling": pooling,
        "head_layers": head_layers,
        "dropout": float(values["dropout"]),
        "default_temperature": float(values["default_temperature"]),
        "portfolio_mode": portfolio_mode,
        "portfolio_output_mode": output_mode,
    }
    if not bool(values.get("sanitize_inputs", True)):
        contract["sanitize_inputs"] = False
    if bool(values.get("amp_native_position_add", False)):
        contract["amp_native_position_add"] = True
    if bool(values.get("temporal_self_attention_fast_path", False)):
        contract["temporal_self_attention_fast_path"] = True
    if head_layers > 0:
        contract["head_hidden_dim"] = int(values["head_hidden_dim"])
    if portfolio_mode == "long_short":
        contract["center_long_short_logits"] = bool(
            values["center_long_short_logits"]
        )
    if output_mode == "activation_l1":
        contract["portfolio_activation"] = normalize_portfolio_activation(
            config.trading.portfolio_activation
        )

    categorical_indices = _feature_indices_from_patterns(
        feature_names,
        values.get("categorical_feature_names", []),
    )
    contract["categorical_feature_indices"] = categorical_indices
    if categorical_indices:
        contract["categorical_feature_names"] = [
            str(feature_names[index]) for index in categorical_indices
        ]
        contract["categorical_embedding_dim"] = max(
            1,
            int(values["categorical_embedding_dim"]),
        )
        contract["categorical_embedding_cardinality"] = max(
            2,
            int(values["categorical_embedding_cardinality"]),
        )

    temporal_layers = max(0, int(values["temporal_layers"]))
    if mode == "full":
        # Full attention does not execute temporal blocks, but the constructor
        # still materializes them.  Their count/FFN width therefore determine
        # whether strict state_dict loading is possible.
        if temporal_layers > 0:
            contract["checkpoint_only_temporal_blocks"] = {
                "layers": temporal_layers,
                "ffn_mult": int(values["temporal_ffn_mult"]),
            }
    else:
        contract["temporal_layers"] = temporal_layers
        if temporal_layers > 0:
            contract.update(
                {
                    "temporal_heads": int(values["temporal_heads"]),
                    "temporal_ffn_mult": int(values["temporal_ffn_mult"]),
                    "rope_temporal": bool(values["rope_temporal"]),
                }
            )
            if bool(values["rope_temporal"]):
                contract["rope_base"] = float(values["rope_base"])
            if pooling == "last":
                contract["temporal_query_mode"] = (
                    TransformerBasePortfolioModel._normalize_temporal_query_mode(
                        str(values["temporal_query_mode"])
                    )
                )

    if mode == "full":
        layers = max(0, int(values["joint_layers"]))
        contract["joint_layers"] = layers
        if layers > 0:
            contract["joint_heads"] = int(values["joint_heads"])
            contract["joint_ffn_mult"] = int(values["joint_ffn_mult"])
    elif mode == "axial":
        layers = max(0, int(values["cross_layers"]))
        contract["cross_layers"] = layers
        if layers > 0:
            contract["cross_heads"] = int(values["cross_heads"])
            contract["cross_ffn_mult"] = int(values["cross_ffn_mult"])
    elif mode == "latent":
        contract.update(
            {
                "latent_layers": max(1, int(values["latent_layers"])),
                "num_latent_factors": max(1, int(values["num_latent_factors"])),
                "num_market_tokens": max(1, int(values["num_market_tokens"])),
                "market_layers": max(1, int(values["market_layers"])),
                "cross_heads": int(values["cross_heads"]),
                "cross_ffn_mult": int(values["cross_ffn_mult"]),
            }
        )
    elif mode == "market_token":
        contract.update(
            {
                "num_market_tokens": max(1, int(values["num_market_tokens"])),
                "market_layers": max(1, int(values["market_layers"])),
                "cross_heads": int(values["cross_heads"]),
                "cross_ffn_mult": int(values["cross_ffn_mult"]),
            }
        )
    return contract


def _checkpoint_model_values(
    config: ExperimentConfig,
    active_model: Mapping[str, Any],
    feature_names: Sequence[str],
) -> dict[str, Any]:
    config_name = str(active_model["config_name"])
    values = dict(active_model["values"])
    if config_name == "transformer_base_portfolio":
        return _transformer_base_checkpoint_model_values(
            config,
            values,
            feature_names,
        )

    contract = {
        name: value
        for name, value in values.items()
        if name not in _SCHEMA_4_MODEL_RUNTIME_FIELDS
    }
    effective_mode = _effective_model_portfolio_mode(config, config_name, values)
    contract.pop("portfolio_mode", None)
    contract["portfolio_mode"] = effective_mode
    output_mode: str | None = None
    if "portfolio_output_mode" in contract:
        output_mode = normalize_portfolio_output_mode(
            str(contract["portfolio_output_mode"])
        )
        contract["portfolio_output_mode"] = output_mode
    if output_mode in {None, "activation_l1"}:
        contract["portfolio_activation"] = normalize_portfolio_activation(
            config.trading.portfolio_activation
        )
    return contract


def _checkpoint_manifest(
    panel: PanelData,
    config: ExperimentConfig,
    *,
    include_data_content: bool = True,
) -> dict[str, Any]:
    """Build a portable, content-addressed checkpoint compatibility manifest."""
    effective_short_open = (
        panel.can_short_open_mask
        if panel.can_short_open_mask is not None
        else panel.can_sell_mask
    )
    effective_force_cover = (
        panel.force_short_cover_mask
        if panel.force_short_cover_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    effective_force_exit = (
        panel.force_exit_mask
        if panel.force_exit_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    schema_2_force_exit_compatible = not bool(
        np.asarray(effective_force_exit, dtype=bool).any()
    )
    if include_data_content:
        panel_arrays = {
            "dates": _array_content_fingerprint(panel.dates),
            "features": _array_content_fingerprint(panel.features),
            "returns_1d": _array_content_fingerprint(panel.returns_1d),
            "tradable_mask": _array_content_fingerprint(panel.tradable_mask),
            "alive_mask": _array_content_fingerprint(panel.alive_mask),
            "benchmark_returns": _array_content_fingerprint(panel.benchmark_returns),
            "close_prices": _array_content_fingerprint(panel.close_prices),
            "daily_volumes": _array_content_fingerprint(panel.daily_volumes),
            "can_buy_mask": _array_content_fingerprint(panel.can_buy_mask),
            "can_sell_mask": _array_content_fingerprint(panel.can_sell_mask),
            "can_short_open_mask": _array_content_fingerprint(effective_short_open),
            "force_short_cover_mask": _array_content_fingerprint(effective_force_cover),
            "force_exit_mask": _array_content_fingerprint(effective_force_exit),
        }
        schema_2_panel_arrays = {
            name: value for name, value in panel_arrays.items() if name != "force_exit_mask"
        }
        schema_3_without_force_exit_panel_arrays = dict(schema_2_panel_arrays)
    else:
        panel_arrays = {"omitted_for_inference": True}
        schema_2_panel_arrays = dict(panel_arrays)
        schema_3_without_force_exit_panel_arrays = dict(panel_arrays)
    preprocessing_contract = {
        "benchmark_name": str(config.data.benchmark_name),
        "security_filter": str(config.data.security_filter),
        "usd_only_trading_pairs": bool(config.data.usd_only_trading_pairs),
        "tradable_mode": str(config.data.tradable_mode),
        "trading_volume_policy": str(config.data.trading_volume_policy),
        "use_tw_public_features": bool(config.data.use_tw_public_features),
        "use_tw_public_rules": bool(config.data.use_tw_public_rules),
        "tw_public_market_symbol": str(config.data.tw_public_market_symbol),
        "feature_include": list(config.data.feature_include),
        "feature_exclude": list(config.data.feature_exclude),
    }
    schema_2_preprocessing_contract = {
        name: value for name, value in preprocessing_contract.items() if name != "use_tw_public_rules"
    }
    data_schema_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "preprocessing": preprocessing_contract,
    }
    data_contract = {
        **data_schema_contract,
        "panel_arrays": panel_arrays,
    }
    schema_2_data_schema_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "preprocessing": schema_2_preprocessing_contract,
    }
    schema_2_data_contract = {
        **schema_2_data_schema_contract,
        "panel_arrays": schema_2_panel_arrays,
    }
    schema_3_without_force_exit_data_contract = {
        **data_schema_contract,
        "panel_arrays": schema_3_without_force_exit_panel_arrays,
    }
    active_model = _active_model_config(config)
    symbols = [str(symbol) for symbol in panel.symbols]
    feature_names = [str(name) for name in panel.feature_names]
    model_contract = {
        "model_name": active_model["config_name"],
        "model": _checkpoint_model_values(config, active_model, feature_names),
        "lookback": int(config.training.lookback),
        "num_symbols": int(panel.num_symbols),
        "num_features": int(len(panel.feature_names)),
        "symbols": symbols,
        "feature_names": feature_names,
    }
    schema_3_model_contract = {
        "model_name": active_model["config_name"],
        "model": _schema_3_checkpoint_model_values(active_model),
        "lookback": int(config.training.lookback),
        "num_symbols": int(panel.num_symbols),
        "num_features": int(len(panel.feature_names)),
        "symbols": symbols,
        "feature_names": feature_names,
    }
    contracts = {
        "data": data_contract,
        "model": model_contract,
        "training": _training_checkpoint_contract(config),
        "evaluation": asdict(config.evaluation),
        "trading": _trading_checkpoint_contract(config),
        "walk_forward": asdict(config.walk_forward),
    }
    fingerprints = {
        name: _stable_fingerprint(contract) for name, contract in contracts.items()
    }
    fingerprints["data_schema"] = _stable_fingerprint(data_schema_contract)
    settings_fingerprint = _stable_fingerprint(
        {
            name: fingerprints[name]
            for name in contracts
            if name != "data"
        }
    )
    legacy_data_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "lookback": int(config.training.lookback),
        "tradable_mode": str(config.data.tradable_mode),
        "feature_include": list(config.data.feature_include),
        "feature_exclude": list(config.data.feature_exclude),
        "use_tw_public_features": bool(config.data.use_tw_public_features),
    }
    legacy_intervals = ("epoch", "step", "batch")
    legacy_setting_contracts = {
        interval: _legacy_checkpoint_setting_contract(
            config,
            active_model,
            legacy_lr_scheduler_interval=interval,
        )
        for interval in legacy_intervals
    }
    legacy_setting_contract = legacy_setting_contracts["epoch"]
    configuration_snapshot = asdict(config)

    schema_3_contracts = {
        "data": data_contract,
        "model": schema_3_model_contract,
        "training": _training_checkpoint_contract_schema_3(config),
        "evaluation": asdict(config.evaluation),
        "trading": asdict(config.trading),
        "walk_forward": asdict(config.walk_forward),
    }
    schema_3_fingerprints = {
        name: _stable_fingerprint(contract)
        for name, contract in schema_3_contracts.items()
    }
    schema_3_fingerprints["data_schema"] = _stable_fingerprint(
        data_schema_contract
    )
    schema_3_interval_fingerprints: dict[str, dict[str, str]] = {}
    schema_3_interval_contracts: dict[str, dict[str, Any]] = {}
    for interval in legacy_intervals[1:]:
        interval_contracts = dict(schema_3_contracts)
        interval_contracts["training"] = _training_checkpoint_contract_schema_3(
            config,
            legacy_lr_scheduler_interval=interval,
        )
        interval_fingerprints = dict(schema_3_fingerprints)
        interval_fingerprints["training"] = _stable_fingerprint(
            interval_contracts["training"]
        )
        schema_3_interval_contracts[interval] = interval_contracts
        schema_3_interval_fingerprints[interval] = interval_fingerprints
    schema_3_without_force_exit_contracts = dict(schema_3_contracts)
    schema_3_without_force_exit_contracts["data"] = (
        schema_3_without_force_exit_data_contract
    )
    schema_3_without_force_exit_fingerprints = dict(schema_3_fingerprints)
    schema_3_without_force_exit_fingerprints["data"] = _stable_fingerprint(
        schema_3_without_force_exit_data_contract
    )

    schema_2_contracts = dict(schema_3_contracts)
    schema_2_contracts["data"] = schema_2_data_contract
    schema_2_contracts["training"] = _training_checkpoint_contract_schema_2(config)
    schema_2_contracts["trading"] = _trading_checkpoint_contract_schema_2(config)
    schema_2_fingerprints = dict(schema_3_fingerprints)
    schema_2_fingerprints["data"] = _stable_fingerprint(schema_2_data_contract)
    schema_2_fingerprints["data_schema"] = _stable_fingerprint(schema_2_data_schema_contract)
    schema_2_fingerprints["training"] = _stable_fingerprint(schema_2_contracts["training"])
    schema_2_fingerprints["trading"] = _stable_fingerprint(schema_2_contracts["trading"])
    schema_2_interval_fingerprints: dict[str, dict[str, str]] = {}
    schema_2_interval_contracts: dict[str, dict[str, Any]] = {}
    for interval in legacy_intervals[1:]:
        interval_contracts = dict(schema_2_contracts)
        interval_contracts["training"] = _training_checkpoint_contract_schema_2(
            config,
            legacy_lr_scheduler_interval=interval,
        )
        interval_fingerprints = dict(schema_2_fingerprints)
        interval_fingerprints["training"] = _stable_fingerprint(
            interval_contracts["training"]
        )
        schema_2_interval_contracts[interval] = interval_contracts
        schema_2_interval_fingerprints[interval] = interval_fingerprints
    return {
        "schema_version": 4,
        "contracts": contracts,
        "fingerprints": fingerprints,
        # Preserve the complete submitted configuration for audit/debugging.
        # Resume compatibility is checked through the semantic layers above so
        # machine-local runner/output paths do not make a portable checkpoint
        # unreadable on another host.
        "configuration": configuration_snapshot,
        "configuration_fingerprint": _stable_fingerprint(configuration_snapshot),
        "compatibility_fingerprints": {
            "schema_3": schema_3_fingerprints,
            "schema_3_without_force_exit": schema_3_without_force_exit_fingerprints,
            "schema_3_lr_interval_step": schema_3_interval_fingerprints["step"],
            "schema_3_lr_interval_batch": schema_3_interval_fingerprints["batch"],
            "schema_2": schema_2_fingerprints,
            "schema_2_lr_interval_step": schema_2_interval_fingerprints["step"],
            "schema_2_lr_interval_batch": schema_2_interval_fingerprints["batch"],
        },
        "compatibility_contracts": {
            "schema_3": schema_3_contracts,
            "schema_3_without_force_exit": schema_3_without_force_exit_contracts,
            "schema_3_lr_interval_step": schema_3_interval_contracts["step"],
            "schema_3_lr_interval_batch": schema_3_interval_contracts["batch"],
            "schema_2": schema_2_contracts,
            "schema_2_lr_interval_step": schema_2_interval_contracts["step"],
            "schema_2_lr_interval_batch": schema_2_interval_contracts["batch"],
        },
        "compatibility_constraints": {
            "schema_1": {
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
            },
            "schema_2": {
                # Schema 2 predates terminal-exit events. It is safe to resume
                # only when the current panel would not exercise that new rule.
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
            },
            "schema_3": {
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
            },
            "schema_3_without_force_exit": {
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
            },
        },
        # Stable aliases keep old tooling readable while layered validation
        # uses the independently actionable layer fingerprints above.
        "data_fingerprint": fingerprints["data"],
        "settings_fingerprint": settings_fingerprint,
        "legacy_fingerprints": {
            "data_fingerprint": _stable_fingerprint(legacy_data_contract),
            "settings_fingerprint": _stable_fingerprint(legacy_setting_contract),
        },
        "legacy_settings_compatibility_fingerprints": [
            _stable_fingerprint(legacy_setting_contracts[interval])
            for interval in legacy_intervals
        ],
    }


def _validate_checkpoint_fold_contract(
    checkpoint: Mapping[str, Any],
    *,
    checkpoint_path: Path,
    expected_fold: WalkForwardFold | None,
    expected_train_years: Sequence[int] | None,
) -> None:
    if expected_fold is not None:
        fold_fields = {
            "fold_id": int(expected_fold.fold_id),
            "train_years": [int(year) for year in expected_fold.train_years],
            "val_years": [int(year) for year in expected_fold.val_years],
            "test_years": [int(year) for year in expected_fold.test_years],
        }
        fold_mismatches = [
            name for name, value in fold_fields.items() if checkpoint.get(name) != value
        ]
        if fold_mismatches:
            details = ", ".join(
                f"{name}: saved={checkpoint.get(name)} current={fold_fields[name]}"
                for name in fold_mismatches
            )
            raise RuntimeError(
                f"Checkpoint fold contract mismatch ({details}): {checkpoint_path}."
            )
    elif expected_train_years is not None:
        years = [int(year) for year in expected_train_years]
        if checkpoint.get("train_years") != years:
            raise RuntimeError(
                "Checkpoint training-year contract mismatch "
                f"(saved={checkpoint.get('train_years')} current={years}): {checkpoint_path}."
            )


def _validate_checkpoint_manifest(
    checkpoint: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    checkpoint_path: Path,
    scope: str = "resume",
    expected_fold: WalkForwardFold | None = None,
    expected_train_years: Sequence[int] | None = None,
) -> None:
    _validate_checkpoint_fold_contract(
        checkpoint,
        checkpoint_path=checkpoint_path,
        expected_fold=expected_fold,
        expected_train_years=expected_train_years,
    )
    normalized_scope = str(scope).strip().lower().replace("-", "_")
    scopes = {
        "resume": ("data", "model", "training", "evaluation", "trading", "walk_forward"),
        "artifact": ("data", "model", "training", "evaluation", "trading", "walk_forward"),
        # New dates and labels are expected at inference time. Preserve the
        # feature/universe and model semantics without hashing future rows.
        "inference": ("data_schema", "model"),
        "model": ("data_schema", "model"),
    }
    if normalized_scope not in scopes:
        raise ValueError(f"Unknown checkpoint validation scope: {scope!r}")

    actual = checkpoint.get("experiment_manifest")
    if actual is None:
        amp_feature_cache_enabled = bool(
            expected.get("contracts", {})
            .get("training", {})
            .get("precision", {})
            .get("cache_train_features_in_amp_dtype", False)
        )
        if normalized_scope in {"resume", "artifact"} and amp_feature_cache_enabled:
            raise RuntimeError(
                "Legacy checkpoint without a semantic manifest cannot safely resume "
                "with cache_train_features_in_amp_dtype=true because its feature-storage "
                f"precision was not fingerprinted: {checkpoint_path}. Start a fresh training run."
            )
        print(f"[checkpoint] legacy checkpoint has no fingerprint; loading compatibly: {checkpoint_path}")
        return
    if not isinstance(actual, Mapping):
        raise RuntimeError(
            f"Checkpoint experiment_manifest must be a mapping, got {type(actual).__name__}: "
            f"{checkpoint_path}."
        )

    try:
        actual_schema = int(actual.get("schema_version", 1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Checkpoint manifest schema_version is invalid: {checkpoint_path}."
        ) from exc
    if actual_schema not in {1, 2, 3, 4}:
        raise RuntimeError(
            "Unsupported checkpoint manifest schema "
            f"{actual_schema}: {checkpoint_path}. This runtime supports schemas 1 through 4; "
            "use a runtime that understands the saved schema instead of guessing compatibility."
        )
    if actual_schema < 4 and normalized_scope in {"resume", "artifact"}:
        schema_constraints = expected.get("compatibility_constraints", {}).get(
            f"schema_{actual_schema}",
            {},
        )
        if not bool(schema_constraints.get("amp_feature_cache_disabled", True)):
            raise RuntimeError(
                f"Schema {actual_schema} checkpoint cannot safely resume with "
                "cache_train_features_in_amp_dtype=true because that immutable "
                f"feature-storage precision was not fingerprinted: {checkpoint_path}. "
                "Start a fresh training run."
            )
    expected_for_validation: Mapping[str, Any] = expected
    legacy_expected: Mapping[str, Any] = {}
    if actual_schema < 2:
        schema_1_constraints = expected.get("compatibility_constraints", {}).get(
            "schema_1",
            {},
        )
        if normalized_scope in {"resume", "artifact"} and not bool(
            schema_1_constraints.get("force_exit_mask_is_empty", True)
        ):
            raise RuntimeError(
                "Schema 1 checkpoint cannot safely resume with a non-empty "
                f"force_exit_mask because that execution rule was not fingerprinted: {checkpoint_path}. "
                "Start a fresh training run so terminal exits are part of the checkpoint contract."
            )
        legacy_expected = expected.get("legacy_fingerprints", expected)
        legacy_settings_candidates = set(
            expected.get(
                "legacy_settings_compatibility_fingerprints",
                [legacy_expected.get("settings_fingerprint")],
            )
        )
        mismatches = []
        if actual.get("data_fingerprint") != legacy_expected.get("data_fingerprint"):
            mismatches.append("data_fingerprint")
        if actual.get("settings_fingerprint") not in legacy_settings_candidates:
            mismatches.append("settings_fingerprint")
    else:
        actual_fingerprints = actual.get("fingerprints", {})
        if not isinstance(actual_fingerprints, Mapping):
            raise RuntimeError(
                "Checkpoint manifest fingerprints must be a mapping: "
                f"{checkpoint_path}."
            )
        if actual_schema == 2:
            schema_2_constraints = expected.get("compatibility_constraints", {}).get(
                "schema_2", {}
            )
            if normalized_scope in {"resume", "artifact"} and not bool(
                schema_2_constraints.get("force_exit_mask_is_empty", True)
            ):
                raise RuntimeError(
                    "Schema 2 checkpoint cannot safely resume with a non-empty "
                    f"force_exit_mask because that execution rule was not fingerprinted: {checkpoint_path}. "
                    "Start a fresh training run so terminal exits are part of the checkpoint contract."
                )
            compatibility = expected.get("compatibility_fingerprints", {})
            expected_fingerprints = compatibility.get(
                "schema_2",
                expected.get("fingerprints", {}),
            )
            for key in (
                "schema_2_lr_interval_step",
                "schema_2_lr_interval_batch",
            ):
                candidate = compatibility.get(key, {})
                if actual_fingerprints.get("training") == candidate.get("training"):
                    expected_fingerprints = dict(expected_fingerprints)
                    expected_fingerprints["training"] = candidate["training"]
                    break
        elif actual_schema == 3:
            compatibility = expected.get("compatibility_fingerprints", {})
            schema_3_fingerprints = compatibility.get(
                "schema_3",
                expected.get("fingerprints", {}),
            )
            schema_3_without_force_exit = compatibility.get(
                "schema_3_without_force_exit",
                schema_3_fingerprints,
            )
            expected_fingerprints = dict(schema_3_fingerprints)
            if actual_fingerprints.get("data") == schema_3_without_force_exit.get("data"):
                constraints = expected.get("compatibility_constraints", {}).get(
                    "schema_3_without_force_exit",
                    {},
                )
                if normalized_scope in {"resume", "artifact"} and not bool(
                    constraints.get("force_exit_mask_is_empty", True)
                ):
                    raise RuntimeError(
                        "Schema 3 checkpoint without a force_exit_mask fingerprint "
                        "cannot safely resume with non-empty terminal-exit events: "
                        f"{checkpoint_path}. Start a fresh training run."
                    )
                expected_fingerprints["data"] = schema_3_without_force_exit["data"]
            # The removed lr_scheduler_interval never affected runtime cadence.
            # Accept every spelling emitted by schema 3 while keeping all other
            # historical training fields exact.
            for key in (
                "schema_3_lr_interval_step",
                "schema_3_lr_interval_batch",
            ):
                candidate = compatibility.get(key, {})
                if actual_fingerprints.get("training") == candidate.get("training"):
                    expected_fingerprints["training"] = candidate["training"]
                    break
        else:
            expected_fingerprints = expected.get("fingerprints", {})
        expected_for_validation = {"fingerprints": expected_fingerprints}
        mismatches = [
            name
            for name in scopes[normalized_scope]
            if actual_fingerprints.get(name) != expected_fingerprints.get(name)
        ]
    if mismatches:
        details = ", ".join(
            f"{name}: saved={actual.get('fingerprints', {}).get(name, actual.get(name))} "
            f"current={legacy_expected.get(name) if actual_schema < 2 else expected_for_validation.get('fingerprints', {}).get(name)}"
            for name in mismatches
        )
        raise RuntimeError(
            f"Checkpoint semantic fingerprint mismatch ({details}): {checkpoint_path}. "
            "Use a matching config/data schema or start a fresh output directory."
        )

def _save_fold_checkpoint(
    checkpoint_path: Path,
    *,
    fold: WalkForwardFold,
    epoch: int,
    best_val_loss: float,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    experiment_manifest: Mapping[str, Any] | None = None,
    include_optimizer: bool = False,
    check_finite: bool = True,
) -> None:
    if not _distributed_should_write():
        return
    if not _checkpoint_parameters_are_saveable(model, checkpoint_path, "fold_best", check_finite=check_finite):
        return
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
    if experiment_manifest is not None:
        payload["experiment_manifest"] = dict(experiment_manifest)
    if include_optimizer:
        payload["optimizer_state_dict"] = optimizer.state_dict()
        payload["scaler_state_dict"] = scaler.state_dict()
    _atomic_torch_save(payload, checkpoint_path)


def _atomic_torch_save(payload: Mapping[str, Any], checkpoint_path: Path) -> None:
    """Keep the previous readable checkpoint until the replacement is complete."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = checkpoint_path.with_name(
        f".{checkpoint_path.name}.{os.getpid()}.tmp"
    )
    try:
        torch.save(dict(payload), temporary_path)
        os.replace(temporary_path, checkpoint_path)
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass


def _capture_rng_state() -> dict[str, Any]:
    """Capture process RNGs as storage-free Python payloads.

    ``all_gather_object`` in PyTorch 2.12 cannot reliably round-trip Torch
    storages embedded in an object payload. Store NumPy keys as integers and
    Torch RNG byte tensors as ``bytes``; both remain safe for weights-only
    checkpoint loading and preserve every bit. Neural executors are
    single-device per process (including one process per DDP rank), so touching
    every visible CUDA generator would only create foreign-device contexts and
    waste VRAM.
    """
    numpy_state = np.random.get_state()
    cuda_device_index: int | None = None
    cuda_current_state: bytes | None = None
    if torch.cuda.is_available() and torch.cuda.is_initialized():
        cuda_device_index = int(torch.cuda.current_device())
        cuda_current_state = bytes(torch.cuda.get_rng_state(cuda_device_index).cpu().tolist())
    return {
        "schema_version": 3,
        "python": random.getstate(),
        "numpy": {
            "bit_generator": str(numpy_state[0]),
            "keys": [int(value) for value in np.asarray(numpy_state[1], dtype=np.uint32).tolist()],
            "position": int(numpy_state[2]),
            "has_gauss": int(numpy_state[3]),
            "cached_gaussian": float(numpy_state[4]),
        },
        "torch_cpu": bytes(torch.get_rng_state().cpu().tolist()),
        "torch_cuda_current": cuda_current_state,
        "torch_cuda_device_index": cuda_device_index,
    }


def _capture_checkpoint_rng_state() -> dict[str, Any]:
    """Capture every DDP rank, not only rank 0's process-local CUDA stream."""
    local_state = _capture_rng_state()
    world_size = _distributed_world_size()
    if not _distributed_is_initialized() or world_size <= 1:
        return local_state
    gathered: list[dict[str, Any] | None] = [None] * world_size
    dist.all_gather_object(gathered, local_state)
    if any(item is None for item in gathered):
        raise RuntimeError("Failed to gather RNG state from every DDP rank")
    return {
        "schema_version": 3,
        "world_size": world_size,
        "by_rank": gathered,
    }


def _rng_state_bytes_to_tensor(value: Any) -> torch.Tensor:
    """Decode schema-3 RNG bytes while retaining tensor checkpoint compatibility."""
    if torch.is_tensor(value):
        return value.detach().to(device="cpu", dtype=torch.uint8).contiguous()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return torch.frombuffer(bytearray(value), dtype=torch.uint8).clone()
    return torch.as_tensor(value, dtype=torch.uint8, device="cpu").contiguous()


def _update_rng_seed_digest(digest: Any, value: Any) -> None:
    """Hash nested RNG payloads without relying on pickle storage identities."""
    if torch.is_tensor(value):
        tensor = value.detach().cpu().contiguous()
        digest.update(b"tensor\0")
        digest.update(str(tensor.dtype).encode("utf-8"))
        digest.update(json.dumps(list(tensor.shape)).encode("ascii"))
        digest.update(tensor.numpy().tobytes(order="C"))
        return
    if isinstance(value, np.ndarray):
        array = np.ascontiguousarray(value)
        digest.update(b"ndarray\0")
        digest.update(str(array.dtype).encode("utf-8"))
        digest.update(json.dumps(list(array.shape)).encode("ascii"))
        digest.update(array.tobytes(order="C"))
        return
    if isinstance(value, np.generic):
        _update_rng_seed_digest(digest, value.item())
        return
    if isinstance(value, Mapping):
        digest.update(b"mapping\0")
        for key in sorted(value, key=lambda item: repr(item)):
            _update_rng_seed_digest(digest, key)
            _update_rng_seed_digest(digest, value[key])
        return
    if isinstance(value, (list, tuple)):
        digest.update(b"tuple\0" if isinstance(value, tuple) else b"list\0")
        digest.update(str(len(value)).encode("ascii"))
        for item in value:
            _update_rng_seed_digest(digest, item)
        return
    if isinstance(value, bytes):
        digest.update(b"bytes\0")
        digest.update(value)
        return
    if value is None or isinstance(value, (str, bool, int, float)):
        digest.update(type(value).__name__.encode("ascii"))
        digest.update(b"\0")
        digest.update(
            json.dumps(value, sort_keys=True, allow_nan=True).encode("utf-8")
        )
        return
    # RNG payloads should only contain the types above. Keep legacy custom
    # objects deterministic enough to remain readable rather than failing hard.
    digest.update(type(value).__qualname__.encode("utf-8"))
    digest.update(pickle.dumps(value, protocol=4))


def _derived_rng_seed(
    saved_state: Mapping[str, Any],
    *,
    rank: int,
    world_size: int,
    stream: str,
) -> int:
    digest = hashlib.sha256()
    digest.update(b"stockagent.checkpoint.rng.rank-expansion.v1\0")
    _update_rng_seed_digest(digest, saved_state)
    digest.update(f"\0rank={int(rank)}\0world={int(world_size)}\0{stream}".encode("utf-8"))
    return int.from_bytes(digest.digest()[:8], "big") & ((1 << 63) - 1)


def _restore_derived_rng_streams(
    saved_state: Mapping[str, Any],
    *,
    rank: int,
    world_size: int,
) -> bool:
    """Create a stable, distinct stream for a rank absent from the checkpoint."""
    random.seed(
        _derived_rng_seed(
            saved_state,
            rank=rank,
            world_size=world_size,
            stream="python",
        )
    )
    np.random.seed(
        _derived_rng_seed(
            saved_state,
            rank=rank,
            world_size=world_size,
            stream="numpy",
        )
        & 0xFFFFFFFF
    )
    cpu_generator = torch.Generator(device="cpu")
    cpu_generator.manual_seed(
        _derived_rng_seed(
            saved_state,
            rank=rank,
            world_size=world_size,
            stream="torch_cpu",
        )
    )
    torch.set_rng_state(cpu_generator.get_state())
    if torch.cuda.is_available() and torch.cuda.is_initialized():
        torch.cuda.manual_seed(
            _derived_rng_seed(
                saved_state,
                rank=rank,
                world_size=world_size,
                stream="torch_cuda_current",
            )
        )
    return True


def _restore_rng_state(state: Mapping[str, Any] | None) -> bool:
    """Restore checkpoint RNGs; return False for a legacy/incomplete payload."""
    if not state:
        return False
    saved_payload = state
    rank = _distributed_rank()
    world_size = _distributed_world_size()
    if "by_rank" in state:
        saved_by_rank = list(state.get("by_rank", []))
        if not saved_by_rank:
            return False
        if len(saved_by_rank) != world_size:
            print(
                "[checkpoint] DDP world size changed; exact stochastic replay is "
                f"not possible for added ranks (saved={len(saved_by_rank)}, current={world_size}, "
                f"rank={rank})"
            )
        if rank >= len(saved_by_rank):
            print(
                "[checkpoint] deriving a distinct deterministic RNG stream for "
                f"new rank {rank}; saved ranks are never duplicated"
            )
            return _restore_derived_rng_streams(
                saved_payload,
                rank=rank,
                world_size=world_size,
            )
        state = saved_by_rank[rank]
        if state is None:
            return False
    elif rank > 0 or world_size > 1:
        if rank > 0:
            print(
                "[checkpoint] single-rank RNG checkpoint expanded to DDP; deriving "
                f"a distinct deterministic stream for new rank {rank}"
            )
            return _restore_derived_rng_streams(
                saved_payload,
                rank=rank,
                world_size=world_size,
            )
        print(
            "[checkpoint] single-rank RNG checkpoint expanded to DDP; rank 0 "
            "retains its exact saved stream"
        )
    required = {"python", "numpy", "torch_cpu"}
    if not required.issubset(state):
        return False

    random.setstate(state["python"])
    numpy_state = state["numpy"]
    numpy_keys = numpy_state["keys"]
    if torch.is_tensor(numpy_keys):
        numpy_keys = numpy_keys.detach().cpu().numpy()
    np.random.set_state(
        (
            str(numpy_state["bit_generator"]),
            np.asarray(numpy_keys, dtype=np.uint32),
            int(numpy_state["position"]),
            int(numpy_state["has_gauss"]),
            float(numpy_state["cached_gaussian"]),
        )
    )
    torch.set_rng_state(_rng_state_bytes_to_tensor(state["torch_cpu"]))

    saved_cuda_current = state.get("torch_cuda_current")
    cuda_ready = torch.cuda.is_available() and torch.cuda.is_initialized()
    if cuda_ready and saved_cuda_current is not None:
        # Map the saved rank-local stream onto this process's current device;
        # CUDA ordinals legitimately change with CUDA_VISIBLE_DEVICES/host.
        torch.cuda.set_rng_state(
            _rng_state_bytes_to_tensor(saved_cuda_current),
            device=torch.cuda.current_device(),
        )
    else:
        # Schema v1 stored every visible CUDA device. Keep it readable without
        # requiring new checkpoints to initialize foreign-device contexts.
        saved_cuda = list(state.get("torch_cuda", []))
        if not cuda_ready or not saved_cuda:
            return True
        current_count = torch.cuda.device_count()
        if len(saved_cuda) == current_count:
            torch.cuda.set_rng_state_all([_rng_state_bytes_to_tensor(item) for item in saved_cuda])
        else:
            # A checkpoint must stay readable on a host with a different GPU
            # count. Exact multi-device replay is impossible in that case, but
            # the active rank/device can still continue from a deterministic
            # saved stream.
            device_index = torch.cuda.current_device()
            if device_index < len(saved_cuda):
                torch.cuda.set_rng_state(
                    _rng_state_bytes_to_tensor(saved_cuda[device_index]),
                    device=device_index,
                )
            else:
                torch.cuda.manual_seed(
                    _derived_rng_seed(
                        saved_payload,
                        rank=rank,
                        world_size=world_size,
                        stream=f"legacy_cuda_device_{device_index}",
                    )
                )
            print(
                "[checkpoint] CUDA RNG device-count changed; restored the active "
                f"device stream without duplicating another rank "
                f"(saved={len(saved_cuda)}, current={current_count})"
            )
    return True


def _save_tree_checkpoint_metadata(
    checkpoint_path: Path,
    *,
    fold: WalkForwardFold,
    best_val_loss: float,
    experiment_manifest: Mapping[str, Any],
) -> None:
    """Write the same resumability contract for pickled tree models as neural folds."""
    if not _distributed_should_write():
        return
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_torch_save(
        {
            "checkpoint_kind": "tree_model_metadata",
            "fold_id": int(fold.fold_id),
            "epoch": 0,
            "train_years": [int(year) for year in fold.train_years],
            "val_years": [int(year) for year in fold.val_years],
            "test_years": [int(year) for year in fold.test_years],
            "best_val_loss": float(best_val_loss),
            "model_path": _model_path(checkpoint_path.parent).name,
            "experiment_manifest": dict(experiment_manifest),
        },
        checkpoint_path,
    )


def _load_checkpoint(checkpoint_path: Path) -> dict:
    return torch.load(checkpoint_path, map_location="cpu")


def _validate_checkpoint_effective_train_batch_size(
    checkpoint: Mapping[str, Any],
    *,
    effective_train_batch_size: int,
    checkpoint_path: Path,
) -> None:
    """Prevent host-dependent auto-batch selection from changing a resumed trajectory."""
    saved_value = checkpoint.get("effective_train_batch_size")
    if saved_value is None:
        manifest = checkpoint.get("experiment_manifest", {})
        try:
            manifest_schema = int(manifest.get("schema_version", 1))
        except (AttributeError, TypeError, ValueError):
            manifest_schema = 1
        if manifest_schema >= 4:
            raise RuntimeError(
                "Schema 4 checkpoint is missing effective_train_batch_size, so exact "
                f"resume cannot be proven: {checkpoint_path}."
            )
        print(
            "[checkpoint] legacy checkpoint has no effective_train_batch_size; "
            "batch-exact resume cannot be verified"
        )
        return
    saved_batch_size = int(saved_value)
    current_batch_size = int(effective_train_batch_size)
    if saved_batch_size != current_batch_size:
        raise RuntimeError(
            "Checkpoint effective training batch size mismatch "
            f"(saved={saved_batch_size}, current={current_batch_size}): {checkpoint_path}. "
            "Use the same effective global batch size or start a fresh run."
        )


def _save_group_checkpoint(
    checkpoint_path: Path,
    *,
    train_years: list[int],
    epoch: int,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    experiment_manifest: Mapping[str, Any],
    effective_train_batch_size: int,
    scheduler: torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None = None,
    no_improve_epochs: int = 0,
    early_stop_patience: int = 0,
    early_stopping_no_improve_ratio: float = 0.0,
    early_stop_val_interval_epochs: int = 1,
    check_finite: bool = True,
) -> None:
    # Every rank must enter this collective before non-writers return.
    rng_state = _capture_checkpoint_rng_state()
    if not _distributed_should_write():
        return
    if not _checkpoint_parameters_are_saveable(model, checkpoint_path, "group_last", check_finite=check_finite):
        return
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    scheduler_state = scheduler.state_dict() if scheduler is not None else None
    _atomic_torch_save(
        {
            "train_years": train_years,
            "epoch": epoch,
            "model_state_dict": _state_dict_for_save(model),
            "optimizer_state_dict": optimizer.state_dict(),
            "scaler_state_dict": scaler.state_dict(),
            "rng_state": rng_state,
            "experiment_manifest": dict(experiment_manifest),
            "effective_train_batch_size": int(effective_train_batch_size),
            "scheduler_state_dict": scheduler_state,
            "no_improve_epochs": int(max(0, no_improve_epochs)),
            "early_stop_patience": int(max(0, early_stop_patience)),
            "early_stopping_no_improve_ratio": float(max(0.0, early_stopping_no_improve_ratio)),
            "early_stop_val_interval_epochs": int(max(1, early_stop_val_interval_epochs)),
        },
        checkpoint_path,
    )


def _create_lr_scheduler(
    optimizer: torch.optim.Optimizer,
    config: ExperimentConfig,
    *,
    steps_per_epoch: int = 1,
) -> tuple[torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None, str, bool, str]:
    if not bool(getattr(config.training, "enable_lr_scheduler", True)):
        return None, "disabled", False, "epoch"

    name = str(getattr(config.training, "lr_scheduler", "none") or "none").strip().lower().replace("-", "_")
    if name in {"", "none", "off", "false", "disabled"}:
        return None, "none", False, "epoch"

    if name == "cosine":
        t_max = int(config.training.lr_scheduler_t_max)
        if t_max <= 0:
            t_max = max(1, int(config.training.epochs))
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            T_max=t_max,
            eta_min=float(config.training.lr_scheduler_eta_min),
        )
        return scheduler, "cosine", False, "epoch"

    if name in {"warmup_cosine", "cosine_warmup", "linear_warmup_cosine"}:
        steps_per_epoch = max(1, int(steps_per_epoch))
        total_steps = int(config.training.lr_scheduler_t_max)
        if total_steps <= 0:
            total_steps = max(1, int(config.training.epochs) * steps_per_epoch)
        warmup_steps = max(0, int(getattr(config.training, "lr_scheduler_warmup_steps", 0)))
        warmup_steps = min(warmup_steps, max(0, total_steps - 1))
        base_lr = max(float(config.training.learning_rate), 1e-12)
        eta_min = max(0.0, float(config.training.lr_scheduler_eta_min))
        eta_factor = min(1.0, eta_min / base_lr)

        def lr_lambda(step: int) -> float:
            current = max(0, int(step))
            if warmup_steps > 0 and current < warmup_steps:
                return max(eta_factor, float(current + 1) / float(warmup_steps))
            if total_steps <= warmup_steps + 1:
                return 1.0
            progress = float(current - warmup_steps) / float(max(1, total_steps - warmup_steps - 1))
            progress = min(1.0, max(0.0, progress))
            cosine = 0.5 * (1.0 + math.cos(math.pi * progress))
            return eta_factor + (1.0 - eta_factor) * cosine

        scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda=lr_lambda)
        return scheduler, f"warmup_cosine(steps={total_steps}, warmup={warmup_steps})", False, "step"

    if name == "step":
        scheduler = torch.optim.lr_scheduler.StepLR(
            optimizer,
            step_size=max(1, int(config.training.lr_scheduler_step_size)),
            gamma=float(config.training.lr_scheduler_gamma),
        )
        return scheduler, "step", False, "epoch"

    if name in {"plateau", "reduce_on_plateau", "reduce_lr_on_plateau"}:
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer,
            mode="min",
            factor=float(config.training.lr_scheduler_gamma),
            patience=max(1, int(config.training.lr_scheduler_patience)),
            threshold=float(config.training.lr_scheduler_threshold),
        )
        return scheduler, "plateau", True, "epoch"

    print(f"[train] unknown lr_scheduler='{config.training.lr_scheduler}', disabled")
    return None, "none", False, "epoch"


def _estimate_train_steps_per_epoch(
    *,
    train_windowed: WindowedSplitTensors,
    train_batch_size: int,
) -> int:
    batch_size = max(1, int(train_batch_size))
    return max(1, math.ceil(len(train_windowed) / batch_size))


def _step_batch_lr_scheduler(
    scheduler: torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None,
) -> float:
    if scheduler is None:
        return 0.0
    start = time.perf_counter()
    scheduler.step()
    return time.perf_counter() - start


def _save_backtest_artifact(
    output_path: Path,
    result: BacktestResult,
    dates: np.ndarray,
    *,
    compression: str = "none",
) -> None:
    if not _distributed_should_write():
        return
    writer = np.savez_compressed if str(compression).strip().lower() == "compressed" else np.savez
    writer(
        output_path,
        strategy_returns=result.strategy_returns,
        benchmark_returns=result.benchmark_returns,
        turnovers=result.turnovers,
        weights_history=result.weights_history,
        dates=np.asarray(dates),
    )


def _realized_leverage_backtest(
    result: BacktestResult,
    future_returns: np.ndarray | torch.Tensor,
    *,
    leverage_multiplier: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
) -> BacktestResult:
    multiplier = max(0.0, float(leverage_multiplier))
    weights = np.nan_to_num(
        np.asarray(result.weights_history, dtype=np.float64),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    returns = np.nan_to_num(
        future_returns.detach().cpu().numpy() if torch.is_tensor(future_returns) else np.asarray(future_returns),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    ).astype(np.float64, copy=False)
    if weights.ndim != 2 or returns.shape != weights.shape:
        raise ValueError(
            "future_returns must have the same shape as weights_history for realised leverage plotting: "
            f"weights={weights.shape}, future_returns={returns.shape}"
        )

    leveraged_weights = weights * multiplier
    deltas = np.empty_like(leveraged_weights)
    if leveraged_weights.shape[0] > 0:
        deltas[0] = leveraged_weights[0]
        if leveraged_weights.shape[0] > 1:
            deltas[1:] = leveraged_weights[1:] - leveraged_weights[:-1]
    buy_turnovers = np.clip(deltas, 0.0, None).sum(axis=1)
    sell_turnovers = np.clip(-deltas, 0.0, None).sum(axis=1)
    turnovers = buy_turnovers + sell_turnovers
    asset_simple_returns = _asset_log_returns_to_simple_numpy(returns)
    gross_simple_returns = np.einsum("ts,ts->t", leveraged_weights, asset_simple_returns)
    net_simple_returns = (
        gross_simple_returns
        - float(buy_fee_rate) * buy_turnovers
        - float(sell_fee_rate) * sell_turnovers
    )
    strategy_returns = _portfolio_simple_returns_to_log_numpy(net_simple_returns)
    return BacktestResult(
        strategy_returns=strategy_returns.astype(np.float32),
        benchmark_returns=np.asarray(result.benchmark_returns, dtype=np.float32),
        turnovers=turnovers.astype(np.float32),
        weights_history=leveraged_weights.astype(np.float32),
    )


def _save_best_val_backtest_snapshot(
    *,
    fold_dir: Path,
    fold: WalkForwardFold,
    epoch: int,
    val_loss: float,
    val_backtest: BacktestResultTensor,
    row_start: int,
    row_end: int,
    dates: np.ndarray,
    objective: str,
) -> None:
    if not _distributed_should_write():
        return
    row_start = max(0, int(row_start))
    row_end = max(row_start, int(row_end))
    strategy_returns = val_backtest.strategy_returns[row_start:row_end].detach().to(device="cpu", dtype=torch.float32).numpy()
    benchmark_returns = val_backtest.benchmark_returns[row_start:row_end].detach().to(device="cpu", dtype=torch.float32).numpy()
    turnovers = val_backtest.turnovers[row_start:row_end].detach().to(device="cpu", dtype=torch.float32).numpy()
    if val_backtest.weights_history.numel() and int(val_backtest.weights_history.size(0)) >= row_end:
        weights_history = val_backtest.weights_history[row_start:row_end].detach().to(device="cpu", dtype=torch.float32).numpy()
    else:
        num_symbols = int(val_backtest.weights_history.size(1)) if val_backtest.weights_history.dim() == 2 else 0
        weights_history = np.empty((0, num_symbols), dtype=np.float32)

    snapshot_dates = np.asarray(dates)
    result = BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns,
        turnovers=turnovers,
        weights_history=weights_history,
    )
    _save_backtest_artifact(
        _best_val_backtest_path(fold_dir),
        result,
        snapshot_dates,
        compression="compressed",
    )
    metrics = _compute_metrics_from_tensors(
        val_backtest.strategy_returns[row_start:row_end],
        val_backtest.benchmark_returns[row_start:row_end],
        val_backtest.turnovers[row_start:row_end],
    )
    metadata = {
        "fold_id": int(fold.fold_id),
        "epoch": int(epoch),
        "split": "val",
        "objective": str(objective),
        "best_val_loss": float(val_loss),
        "train_years": [int(year) for year in fold.train_years],
        "val_years": [int(year) for year in fold.val_years],
        "test_years": [int(year) for year in fold.test_years],
        "rows": int(strategy_returns.shape[0]),
        "has_weights_history": bool(weights_history.size > 0),
        "date_start": str(snapshot_dates[0]) if snapshot_dates.size else None,
        "date_end": str(snapshot_dates[-1]) if snapshot_dates.size else None,
        "backtest_npz": str(_best_val_backtest_path(fold_dir).name),
        "compression": "compressed",
        "metrics": metrics,
    }
    with _best_val_snapshot_path(fold_dir).open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)


def _save_integer_share_holdings_table_enabled(config: ExperimentConfig) -> bool:
    return bool(config.training.save_integer_share_holdings_table)


def _save_deployment_test_artifacts(
    fold_dir: Path,
    result: BacktestResult,
    dates: np.ndarray,
    *,
    backtest_artifact_compression: str = "none",
) -> dict[str, float | int | str]:
    """Persist the non-overlapping deployment view separately from full test."""
    date_values = np.asarray(dates)
    rows = int(date_values.size)
    lengths = {
        int(result.strategy_returns.shape[0]),
        int(result.benchmark_returns.shape[0]),
        int(result.turnovers.shape[0]),
        int(result.weights_history.shape[0]),
        rows,
    }
    if len(lengths) != 1:
        raise ValueError(
            "deployment backtest arrays and dates must have equal row counts: "
            f"dates={rows}, strategy={int(result.strategy_returns.shape[0])}, "
            f"benchmark={int(result.benchmark_returns.shape[0])}, "
            f"turnover={int(result.turnovers.shape[0])}, "
            f"weights={int(result.weights_history.shape[0])}"
        )

    timing: dict[str, float | int | str] = {"rows": rows}
    stage_start = time.perf_counter()
    _save_backtest_artifact(
        _deployment_backtest_path(fold_dir),
        result,
        date_values,
        compression=backtest_artifact_compression,
    )
    timing["backtest_npz_s"] = float(time.perf_counter() - stage_start)
    timing["backtest_artifact_compression"] = str(backtest_artifact_compression)

    stage_start = time.perf_counter()
    if rows > 0:
        report = generate_annual_report(result, date_values).replace(
            "Annual Performance Report",
            "Annual Performance Report (Stitched Deployment)",
            1,
        )
    else:
        report = (
            "Annual Performance Report (Stitched Deployment)\n"
            "No rows are owned by this fold after in-split lookback and "
            "next-fold handoff.\n"
        )
    (fold_dir / "deployment_annual_report.txt").write_text(report, encoding="utf-8")
    timing["annual_report_s"] = float(time.perf_counter() - stage_start)
    timing["total_s"] = float(
        float(timing["backtest_npz_s"]) + float(timing["annual_report_s"])
    )
    return timing


def _save_fold_output_artifacts(
    *,
    fold_dir: Path,
    fold_result: FoldResult,
    model: nn.Module,
    test_backtest: BacktestResult,
    test_dates: np.ndarray,
    symbols: list[str],
    config: ExperimentConfig,
    test_future_returns: np.ndarray | torch.Tensor | None = None,
    test_integer_backtest: BacktestResult | None = None,
    holdings_records: list[HoldingsRecord] | None = None,
    deployment_backtest: BacktestResult | None = None,
    deployment_dates: np.ndarray | None = None,
    backtest_artifact_compression: str | None = None,
    print_report: bool = True,
    write_plots: bool = True,
    mark_complete: bool = False,
) -> tuple[dict[str, float | int | str], dict[str, float | str]]:
    if not _distributed_should_write():
        return {"skipped_nonzero_rank": "1", "total_s": 0.0}, {"skipped_nonzero_rank": "1", "total_s": 0.0}
    fold_dir.mkdir(parents=True, exist_ok=True)
    save_start = time.perf_counter()
    save_timing: dict[str, float | int | str] = {}
    stage_start = time.perf_counter()
    torch.save(_state_dict_for_save(model), _model_path(fold_dir))
    save_timing["model_checkpoint_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
        json.dump(asdict(fold_result), f, indent=2)
    save_timing["metrics_json_s"] = float(time.perf_counter() - stage_start)

    stage_start = time.perf_counter()
    compression = str(
        backtest_artifact_compression
        if backtest_artifact_compression is not None
        else getattr(config.training, "backtest_artifact_compression", "none")
    )
    _save_backtest_artifact(
        _backtest_path(fold_dir),
        test_backtest,
        test_dates,
        compression=compression,
    )
    save_timing["backtest_npz_s"] = float(time.perf_counter() - stage_start)
    save_timing["backtest_artifact_compression"] = compression

    stage_start = time.perf_counter()
    table_output_format = str(getattr(config.training, "table_output_format", "csv"))
    _save_daily_portfolio_returns_table(
        fold_dir / "daily_portfolio_returns",
        test_dates,
        test_backtest.strategy_returns,
        test_backtest.benchmark_returns,
        test_backtest.turnovers,
        table_output_format=table_output_format,
    )
    elapsed = float(time.perf_counter() - stage_start)
    save_timing["daily_returns_table_s"] = elapsed
    save_timing["daily_returns_csv_s"] = elapsed if table_output_format == "csv" else 0.0
    save_timing["daily_returns_parquet_s"] = elapsed if table_output_format == "parquet" else 0.0

    stage_start = time.perf_counter()
    _maybe_save_daily_weights_table(
        fold_dir / "daily_weights",
        test_dates,
        symbols,
        test_backtest.weights_history,
        enabled=bool(config.training.save_daily_weights_table),
        table_output_format=table_output_format,
    )
    save_timing["daily_weights_table_s"] = float(time.perf_counter() - stage_start)
    save_timing["table_output_format"] = table_output_format

    stage_start = time.perf_counter()
    report = generate_annual_report(test_backtest, test_dates)
    save_timing["annual_report_compute_s"] = float(time.perf_counter() - stage_start)
    if print_report:
        print("\n" + report)
    stage_start = time.perf_counter()
    with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
        f.write(report)
    save_timing["annual_report_write_s"] = float(time.perf_counter() - stage_start)
    if (deployment_backtest is None) != (deployment_dates is None):
        raise ValueError(
            "deployment_backtest and deployment_dates must either both be provided or both be omitted"
        )
    if deployment_backtest is not None and deployment_dates is not None:
        deployment_timing = _save_deployment_test_artifacts(
            fold_dir,
            deployment_backtest,
            deployment_dates,
            backtest_artifact_compression=compression,
        )
        for key, value in deployment_timing.items():
            save_timing[f"deployment_{key}"] = value
    save_timing["total_s"] = float(time.perf_counter() - save_start)
    (fold_dir / "save_timing.json").write_text(
        json.dumps(save_timing, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    plot_start = time.perf_counter()
    plot_timing: dict[str, float | str] = {}

    def _time_plot(name: str, fn: Callable[..., Any], *args: Any) -> None:
        item_start = time.perf_counter()
        fn(*args)
        plot_timing[f"{name}_s"] = float(time.perf_counter() - item_start)

    def _copy_plot(name: str, source: Path, target: Path) -> None:
        item_start = time.perf_counter()
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        plot_timing[f"{name}_s"] = float(time.perf_counter() - item_start)
        plot_timing[f"{name}_copied_from"] = str(source.name)

    if write_plots:
        _time_plot("equity_curve", plot_equity_curve, test_backtest, test_dates, fold_dir / "equity_curve.png")
        _time_plot("equity_curve_log", plot_equity_curve_log, test_backtest, test_dates, fold_dir / "equity_curve_log.png")
        _time_plot(
            "annual_performance",
            plot_annual_performance,
            test_backtest,
            test_dates,
            fold_dir / "annual_performance.png",
        )
        leverage_multiplier = float(getattr(config.trading, "reporting_leverage", 1.0))
        plot_timing["leverage_multiplier"] = float(leverage_multiplier)
        if test_future_returns is not None:
            leverage_backtest = _realized_leverage_backtest(
                test_backtest,
                test_future_returns,
                leverage_multiplier=leverage_multiplier,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
            )
            _time_plot(
                "leverage_equity_curve",
                plot_equity_curve,
                leverage_backtest,
                test_dates,
                fold_dir / "leverage_equity_curve.png",
            )
            _time_plot(
                "leverage_equity_curve_log",
                plot_equity_curve_log,
                leverage_backtest,
                test_dates,
                fold_dir / "leverage_equity_curve_log.png",
            )
            _time_plot(
                "leverage_annual_performance",
                plot_annual_performance,
                leverage_backtest,
                test_dates,
                fold_dir / "leverage_annual_performance.png",
            )
        else:
            _copy_plot("leverage_equity_curve", fold_dir / "equity_curve.png", fold_dir / "leverage_equity_curve.png")
            _copy_plot(
                "leverage_equity_curve_log",
                fold_dir / "equity_curve_log.png",
                fold_dir / "leverage_equity_curve_log.png",
            )
            _copy_plot(
                "leverage_annual_performance",
                fold_dir / "annual_performance.png",
                fold_dir / "leverage_annual_performance.png",
            )

    if test_integer_backtest is not None:
        audit_start = time.perf_counter()
        integer_audit_timing = _save_integer_share_audit_artifacts(
            fold_dir,
            test_integer_backtest,
            test_dates,
            symbols,
            holdings_records or [],
            write_daily_weights_table=bool(config.training.save_integer_share_daily_weights_table),
            write_holdings_table=_save_integer_share_holdings_table_enabled(config),
            table_output_format=table_output_format,
            backtest_artifact_compression=compression,
        )
        plot_timing["integer_share_audit_s"] = float(time.perf_counter() - audit_start)
        for key, value in integer_audit_timing.items():
            if isinstance(value, bool):
                plot_timing[f"integer_share_audit_{key}"] = float(1 if value else 0)
            elif isinstance(value, str):
                plot_timing[f"integer_share_audit_{key}"] = value
            else:
                plot_timing[f"integer_share_audit_{key}"] = float(value)

    plot_timing["total_s"] = float(time.perf_counter() - plot_start)
    (fold_dir / "plot_timing.json").write_text(
        json.dumps(plot_timing, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if mark_complete:
        _write_fold_complete_marker(fold_dir, fold_result, source="fold_output_artifacts")
    return save_timing, plot_timing


def _normalize_table_output_format(value: str | None) -> str:
    fmt = str(value or "csv").strip().lower()
    if fmt not in {"csv", "parquet"}:
        raise ValueError(f"Unsupported table_output_format={value!r}; expected 'csv' or 'parquet'")
    return fmt


def _table_path(base_path: Path, table_output_format: str) -> Path:
    return base_path.with_suffix("." + _normalize_table_output_format(table_output_format))


def _unlink_table_variants(base_path: Path) -> None:
    for suffix in (".csv", ".parquet"):
        candidate = base_path.with_suffix(suffix)
        if candidate.exists():
            candidate.unlink()


def _write_dataframe_table(df: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.table(df), output_path, compression="snappy")
    elif output_path.suffix.lower() == ".csv":
        columns = list(df.keys())
        values = [np.asarray(df[column]) for column in columns]
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            for row in zip(*values):
                writer.writerow([value.item() if isinstance(value, np.generic) else value for value in row])
    else:
        raise ValueError(f"Unsupported table output extension: {output_path}")


def _state_dict_symbol_count(state_dict: Mapping[str, Any]) -> int | None:
    for key, value in state_dict.items():
        if str(key).endswith("symbol_position") and torch.is_tensor(value) and value.ndim == 4:
            return int(value.size(2))
    return None


def _weight_table_symbols(fold_dir: Path) -> tuple[list[str], Path] | tuple[None, None]:
    for path in (fold_dir / "daily_weights.parquet", fold_dir / "daily_weights.csv"):
        if not path.exists():
            continue
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq

            columns = pq.read_schema(path).names
        else:
            with path.open("r", encoding="utf-8", newline="") as handle:
                columns = next(csv.reader(handle))
        return [str(column) for column in columns if str(column) != "date"], path
    return None, None


def _subset_panel_symbols(panel: PanelData, symbols: Sequence[str]) -> PanelData:
    index_by_symbol = {str(symbol): idx for idx, symbol in enumerate(panel.symbols)}
    missing = [str(symbol) for symbol in symbols if str(symbol) not in index_by_symbol]
    if missing:
        preview = ", ".join(missing[:10])
        raise ValueError(
            f"Cannot align panel to checkpoint universe; {len(missing)} trained symbols are missing "
            f"from the current panel: {preview}"
        )
    indices = np.asarray([index_by_symbol[str(symbol)] for symbol in symbols], dtype=np.int64)
    return PanelData(
        dates=panel.dates,
        symbols=[str(symbol) for symbol in symbols],
        feature_names=list(panel.feature_names),
        features=panel.features[:, indices, :],
        returns_1d=panel.returns_1d[:, indices],
        tradable_mask=panel.tradable_mask[:, indices],
        alive_mask=panel.alive_mask[:, indices],
        benchmark_returns=panel.benchmark_returns,
        close_prices=panel.close_prices[:, indices],
        can_buy_mask=panel.can_buy_mask[:, indices] if panel.can_buy_mask is not None else None,
        can_sell_mask=panel.can_sell_mask[:, indices] if panel.can_sell_mask is not None else None,
        can_short_open_mask=(
            panel.can_short_open_mask[:, indices] if panel.can_short_open_mask is not None else None
        ),
        force_short_cover_mask=(
            panel.force_short_cover_mask[:, indices] if panel.force_short_cover_mask is not None else None
        ),
        force_exit_mask=(
            panel.force_exit_mask[:, indices] if panel.force_exit_mask is not None else None
        ),
    )


def _align_panel_to_state_dict_universe(
    panel: PanelData,
    fold_dir: Path,
    state_dict: Mapping[str, Any],
    *,
    context: str = "inference",
) -> PanelData:
    expected_symbols = _state_dict_symbol_count(state_dict)
    if expected_symbols is None:
        return panel

    trained_symbols, weights_path = _weight_table_symbols(fold_dir)
    if trained_symbols is None:
        if int(panel.num_symbols) != int(expected_symbols):
            raise ValueError(
                f"Checkpoint expects {expected_symbols} symbols but current panel has {panel.num_symbols}; "
                f"cannot align because daily_weights table is missing in {fold_dir}."
            )
        return panel

    if len(trained_symbols) != int(expected_symbols):
        if int(panel.num_symbols) != int(expected_symbols):
            raise ValueError(
                f"Checkpoint expects {expected_symbols} symbols but current panel has {panel.num_symbols}; "
                f"{weights_path} has {len(trained_symbols)} symbols, so it cannot be used for alignment."
            )
        return panel

    if list(trained_symbols) == list(panel.symbols):
        return panel

    trained_set = set(trained_symbols)
    extras = [str(symbol) for symbol in panel.symbols if str(symbol) not in trained_set]
    if int(panel.num_symbols) != int(expected_symbols) or extras:
        preview = ", ".join(extras[:10])
        print(
            f"[{context}] aligning panel symbols to checkpoint universe from {weights_path}: "
            f"current={panel.num_symbols}, checkpoint={expected_symbols}, removed={len(extras)}"
            + (f" ({preview})" if preview else "")
        )
    else:
        print(f"[{context}] reordering panel symbols to match checkpoint universe from {weights_path}")
    return _subset_panel_symbols(panel, trained_symbols)


def _save_holdings_table(
    output_path: Path,
    holdings: list[HoldingsRecord],
) -> None:
    """Save daily holdings detail sorted by date and absolute holding ratio."""
    ordered = sorted(holdings, key=lambda row: (row.date, -abs(float(row.holding_ratio)), row.symbol))
    _write_dataframe_table(
        {
            "date": [row.date for row in ordered],
            "symbol": [row.symbol for row in ordered],
            "shares": [int(row.shares) for row in ordered],
            "price": [float(row.price) for row in ordered],
            "market_value": [float(row.market_value) for row in ordered],
            "holding_ratio": [float(row.holding_ratio) for row in ordered],
            "is_cash": [bool(row.is_cash) for row in ordered],
        },
        output_path,
    )


def _save_daily_portfolio_returns_table(
    base_path: Path,
    dates: np.ndarray,
    strategy_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    turnovers: np.ndarray,
    *,
    table_output_format: str,
) -> None:
    output_path = _table_path(base_path, table_output_format)
    _write_dataframe_table(
        {
            "date": np.asarray(dates),
            "portfolio_return": np.asarray(strategy_returns, dtype=np.float64),
            "benchmark_return": np.asarray(benchmark_returns, dtype=np.float64),
            "turnover": np.asarray(turnovers, dtype=np.float64),
        },
        output_path,
    )
    for suffix in (".csv", ".parquet"):
        stale_path = base_path.with_suffix(suffix)
        if stale_path != output_path and stale_path.exists():
            stale_path.unlink()


def _save_daily_weights_table(
    output_path: Path,
    dates: np.ndarray,
    symbols: list[str],
    weights_history: np.ndarray,
) -> None:
    weights = np.asarray(weights_history, dtype=np.float64)
    data = {"date": np.asarray(dates)}
    data.update({symbol: weights[:, idx] for idx, symbol in enumerate(symbols)})
    _write_dataframe_table(data, output_path)


def _maybe_save_daily_weights_table(
    base_path: Path,
    dates: np.ndarray,
    symbols: list[str],
    weights_history: np.ndarray,
    *,
    enabled: bool,
    table_output_format: str,
) -> None:
    if enabled:
        output_path = _table_path(base_path, table_output_format)
        _save_daily_weights_table(output_path, dates, symbols, weights_history)
        for suffix in (".csv", ".parquet"):
            stale_path = base_path.with_suffix(suffix)
            if stale_path != output_path and stale_path.exists():
                stale_path.unlink()
        return
    _unlink_table_variants(base_path)


def _save_integer_share_audit_artifacts(
    fold_dir: Path,
    result: BacktestResult,
    dates: np.ndarray,
    symbols: list[str],
    holdings: list[HoldingsRecord],
    *,
    write_daily_weights_table: bool = True,
    write_holdings_table: bool = True,
    table_output_format: str = "csv",
    backtest_artifact_compression: str = "none",
) -> dict[str, float | bool | str]:
    table_output_format = _normalize_table_output_format(table_output_format)
    timing: dict[str, float | bool | str] = {
        "table_output_format": table_output_format,
        "write_daily_weights_table": bool(write_daily_weights_table),
        "write_holdings_table": bool(write_holdings_table),
        "write_daily_weights_csv": bool(write_daily_weights_table and table_output_format == "csv"),
        "write_holdings_csv": bool(write_holdings_table and table_output_format == "csv"),
        "backtest_artifact_compression": str(backtest_artifact_compression).strip().lower(),
    }
    total_start = time.perf_counter()
    stage_start = time.perf_counter()
    _save_backtest_artifact(
        _integer_backtest_path(fold_dir),
        result,
        dates,
        compression=backtest_artifact_compression,
    )
    timing["npz_s"] = float(time.perf_counter() - stage_start)
    stage_start = time.perf_counter()
    _save_daily_portfolio_returns_table(
        fold_dir / "integer_share_daily_portfolio_returns",
        dates,
        result.strategy_returns,
        result.benchmark_returns,
        result.turnovers,
        table_output_format=table_output_format,
    )
    elapsed = float(time.perf_counter() - stage_start)
    timing["daily_returns_table_s"] = elapsed
    timing["daily_returns_csv_s"] = elapsed if table_output_format == "csv" else 0.0
    timing["daily_returns_parquet_s"] = elapsed if table_output_format == "parquet" else 0.0
    daily_weights_base = fold_dir / "integer_share_daily_weights"
    if write_daily_weights_table:
        stage_start = time.perf_counter()
        daily_weights_path = _table_path(daily_weights_base, table_output_format)
        _save_daily_weights_table(
            daily_weights_path,
            dates,
            symbols,
            result.weights_history,
        )
        elapsed = float(time.perf_counter() - stage_start)
        timing["daily_weights_table_s"] = elapsed
        timing["daily_weights_csv_s"] = elapsed if table_output_format == "csv" else 0.0
        timing["daily_weights_parquet_s"] = elapsed if table_output_format == "parquet" else 0.0
        for suffix in (".csv", ".parquet"):
            stale_path = daily_weights_base.with_suffix(suffix)
            if stale_path != daily_weights_path and stale_path.exists():
                stale_path.unlink()
    else:
        timing["daily_weights_table_s"] = 0.0
        timing["daily_weights_csv_s"] = 0.0
        timing["daily_weights_parquet_s"] = 0.0
        _unlink_table_variants(daily_weights_base)
    stage_start = time.perf_counter()
    with (fold_dir / "integer_share_annual_report.txt").open("w", encoding="utf-8") as f:
        f.write(generate_annual_report(result, dates))
    timing["annual_report_s"] = float(time.perf_counter() - stage_start)
    holdings_base = fold_dir / "holdings"
    if write_holdings_table:
        stage_start = time.perf_counter()
        holdings_path = _table_path(holdings_base, table_output_format)
        _save_holdings_table(holdings_path, holdings)
        elapsed = float(time.perf_counter() - stage_start)
        timing["holdings_table_s"] = elapsed
        timing["holdings_csv_s"] = elapsed if table_output_format == "csv" else 0.0
        timing["holdings_parquet_s"] = elapsed if table_output_format == "parquet" else 0.0
        for suffix in (".csv", ".parquet"):
            stale_path = holdings_base.with_suffix(suffix)
            if stale_path != holdings_path and stale_path.exists():
                stale_path.unlink()
    else:
        timing["holdings_table_s"] = 0.0
        timing["holdings_csv_s"] = 0.0
        timing["holdings_parquet_s"] = 0.0
        _unlink_table_variants(holdings_base)
    timing["total_s"] = float(time.perf_counter() - total_start)
    return timing


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


def _dataset_volume_notional_to_tensor(dataset: CrossSectionalDataset) -> torch.Tensor:
    valid_indices = torch.as_tensor(dataset.valid_indices, dtype=torch.long)
    if dataset.volume_notional_t is None:
        raise ValueError("dataset was created without volume_notional tensors")
    return dataset.volume_notional_t[valid_indices]


def _dataset_short_rule_masks_to_tensors(
    dataset: CrossSectionalDataset,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    valid_indices = torch.as_tensor(dataset.valid_indices, dtype=torch.long)
    return (
        dataset.can_short_open_mask_t[valid_indices],
        dataset.force_short_cover_mask_t[valid_indices],
        dataset.force_exit_mask_t[valid_indices],
    )


def _windowed_targets_to_tensors(
    split: WindowedSplitTensors,
    *,
    device: torch.device | None = None,
    non_blocking: bool = False,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    date_idx = split.valid_indices
    target_device = split.future_log_returns.device
    if date_idx.device != target_device:
        date_idx = date_idx.to(device=target_device, dtype=torch.long)
    else:
        date_idx = date_idx.to(dtype=torch.long)
    returns = split.future_log_returns[date_idx]
    masks = split.tradable_mask[date_idx]
    can_buy_masks = split.can_buy_mask[date_idx]
    can_sell_masks = split.can_sell_mask[date_idx]
    bench = split.benchmark[date_idx]
    if device is None:
        return returns, masks, can_buy_masks, can_sell_masks, bench
    return (
        returns.to(device=device, non_blocking=non_blocking),
        masks.to(device=device, non_blocking=non_blocking),
        can_buy_masks.to(device=device, non_blocking=non_blocking),
        can_sell_masks.to(device=device, non_blocking=non_blocking),
        bench.to(device=device, non_blocking=non_blocking),
    )


def _windowed_valid_indices_numpy(split: WindowedSplitTensors) -> np.ndarray:
    return split.valid_indices.detach().to(device="cpu", dtype=torch.long).numpy().astype(np.int64, copy=False)


def _resolve_inference_model_chunk_rows(
    config: ExperimentConfig,
    total_rows: int,
) -> int:
    total_rows = int(total_rows)
    if total_rows <= 0:
        return 1
    if int(config.training.chunk_rows) > 0:
        return max(1, min(int(config.training.chunk_rows), total_rows))

    eval_model_chunk_rows_config = str(getattr(config.training, "eval_model_chunk_rows", "auto")).strip().lower()
    if eval_model_chunk_rows_config not in {"", "auto"}:
        return max(1, min(int(eval_model_chunk_rows_config), total_rows))

    cap = int(getattr(config.training, "eval_auto_chunk_rows_cap", 16))
    if cap <= 0:
        cap = int(getattr(config.training, "batch_size_eval", 16))
    return max(1, min(cap, total_rows))


def _resolve_inference_backtest_chunk_rows(
    config: ExperimentConfig,
    model_chunk_rows: int,
    total_rows: int,
) -> int:
    configured = max(1, int(getattr(config.training, "eval_backtest_chunk_rows", model_chunk_rows)))
    if bool(getattr(config.training, "eval_backtest_chunk_rows_auto", True)):
        return _auto_backtest_chunk_rows(
            total_rows=total_rows,
            model_chunk_rows=model_chunk_rows,
            configured_cap=configured,
        )
    return configured


def _auto_backtest_chunk_rows(
    *,
    total_rows: int,
    model_chunk_rows: int,
    configured_cap: int,
) -> int:
    """Choose the smallest stable power-of-two compile bucket that fits rows."""
    total_rows = max(1, int(total_rows))
    model_chunk_rows = max(1, int(model_chunk_rows))
    configured_cap = max(model_chunk_rows, int(configured_cap))
    logical_rows = min(total_rows, configured_cap)
    bucket = 1 << max(0, int(logical_rows - 1).bit_length())
    return min(configured_cap, max(model_chunk_rows, bucket))


def _split_valid_indices(
    panel: PanelData,
    date_indices: np.ndarray,
    lookback: int,
) -> np.ndarray:
    indices = np.array(sorted(np.asarray(date_indices, dtype=np.int64).tolist()), dtype=np.int64)
    if indices.size == 0:
        return indices
    fold_start_idx = int(indices[0])
    min_valid_idx = fold_start_idx + int(lookback) - 1
    valid_indices = indices[indices >= min_valid_idx]
    if valid_indices.size > 0:
        target_mask = panel.tradable_mask & np.isfinite(panel.returns_1d)
        force_exit = (
            panel.force_exit_mask
            if panel.force_exit_mask is not None
            else np.zeros_like(target_mask, dtype=bool)
        )
        valid_indices = valid_indices[
            target_mask[valid_indices].any(axis=1)
            | force_exit[valid_indices].any(axis=1)
        ]
    return valid_indices


def _next_fold_by_id(folds: Iterable[WalkForwardFold]) -> dict[int, WalkForwardFold | None]:
    ordered = sorted(list(folds), key=lambda item: int(item.fold_id))
    result: dict[int, WalkForwardFold | None] = {}
    for idx, fold in enumerate(ordered):
        result[int(fold.fold_id)] = ordered[idx + 1] if idx + 1 < len(ordered) else None
    return result


def _deployment_test_indices(
    panel: PanelData,
    fold: WalkForwardFold,
    next_fold: WalkForwardFold | None,
    lookback: int,
) -> np.ndarray:
    """Raw test date indices owned by this fold's model.

    CrossSectionalDataset later drops the first ``lookback - 1`` rows of this
    raw interval. The interval ends just before the next fold's first valid row,
    so the new year's lookback warmup days remain assigned to the previous
    model instead of being prepended to the next model's backtest.
    """
    indices = np.array(sorted(np.asarray(fold.test_indices, dtype=np.int64).tolist()), dtype=np.int64)
    if indices.size == 0 or next_fold is None:
        return indices
    next_valid = _split_valid_indices(panel, next_fold.test_indices, lookback)
    if next_valid.size == 0:
        return indices
    return indices[indices < int(next_valid[0])]


def _deployment_test_prefix_rows(
    panel: PanelData,
    fold: WalkForwardFold,
    next_fold: WalkForwardFold | None,
    lookback: int,
    full_valid_indices: np.ndarray,
) -> int:
    """Return the stitched-deployment prefix length inside a full test split.

    The deployment interval and the full fold test interval have the same
    starting row.  Therefore, after applying the same in-split lookback and
    executable-row filters, deployment rows must be an exact prefix of the
    full test rows.  Keeping this as an asserted invariant lets final
    evaluation run once over the full horizon and derive the non-overlapping
    deployment artifact without a second model/backtest pass.
    """
    deployment_indices = _deployment_test_indices(
        panel,
        fold,
        next_fold,
        lookback,
    )
    deployment_valid = _split_valid_indices(panel, deployment_indices, lookback)
    full_valid = np.asarray(full_valid_indices, dtype=np.int64)
    deployment_rows = int(deployment_valid.size)
    if deployment_rows > int(full_valid.size) or not np.array_equal(
        deployment_valid,
        full_valid[:deployment_rows],
    ):
        raise RuntimeError(
            "Stitched deployment rows are not a prefix of the full fold test split: "
            f"fold={fold.fold_id}, deployment_rows={deployment_rows}, "
            f"full_rows={int(full_valid.size)}"
        )
    return deployment_rows


def _prefix_backtest_result(result: BacktestResult, rows: int) -> BacktestResult:
    rows = max(0, min(int(rows), int(result.strategy_returns.shape[0])))
    return BacktestResult(
        strategy_returns=np.asarray(result.strategy_returns[:rows]).copy(),
        benchmark_returns=np.asarray(result.benchmark_returns[:rows]).copy(),
        turnovers=np.asarray(result.turnovers[:rows]).copy(),
        weights_history=np.asarray(result.weights_history[:rows]).copy(),
    )


def _upgrade_full_horizon_artifacts_without_inference(
    *,
    panel: PanelData,
    fold: WalkForwardFold,
    next_fold: WalkForwardFold | None,
    config: ExperimentConfig,
    output_path: Path,
) -> bool:
    """Upgrade a legacy full-horizon artifact set without rerunning the model.

    Legacy artifacts produced before scope version 2 can already contain every
    nominal test year. In that case the saved causal backtest is sufficient to
    regenerate the corrected annual report and derive the deployment prefix.
    Truncated legacy artifacts return ``False`` and must be rebuilt by the
    normal inference path.
    """
    fold_dir = _fold_dir(output_path, fold.fold_id)
    backtest_path = _backtest_path(fold_dir)
    metrics_path = _metrics_path(fold_dir)
    if not backtest_path.exists() or not metrics_path.exists():
        return False

    try:
        full_backtest, saved_dates = _load_backtest_artifact(backtest_path)
    except (OSError, ValueError, KeyError):
        return False
    dates = np.asarray(saved_dates, dtype="datetime64[D]").reshape(-1)
    row_count = int(dates.size)
    if row_count <= 0 or np.isnat(dates).any():
        return False
    lengths = {
        row_count,
        int(full_backtest.strategy_returns.shape[0]),
        int(full_backtest.benchmark_returns.shape[0]),
        int(full_backtest.turnovers.shape[0]),
        int(full_backtest.weights_history.shape[0]),
    }
    if len(lengths) != 1 or np.any(dates[1:] <= dates[:-1]):
        return False
    expected_indices = _split_valid_indices(
        panel,
        fold.test_indices,
        config.training.lookback,
    )
    expected_dates = np.asarray(
        panel.dates[expected_indices],
        dtype="datetime64[D]",
    ).reshape(-1)
    if not np.array_equal(dates, expected_dates):
        return False

    deployment_rows = row_count
    if next_fold is not None:
        next_valid = _split_valid_indices(
            panel,
            next_fold.test_indices,
            config.training.lookback,
        )
        if next_valid.size > 0:
            cutoff_date = np.asarray(panel.dates[int(next_valid[0])], dtype="datetime64[D]")
            deployment_rows = int(np.searchsorted(dates, cutoff_date, side="left"))

    deployment_backtest = _prefix_backtest_result(full_backtest, deployment_rows)
    deployment_dates = dates[:deployment_rows]
    _save_deployment_test_artifacts(
        fold_dir,
        deployment_backtest,
        deployment_dates,
        backtest_artifact_compression=str(
            getattr(config.training, "backtest_artifact_compression", "none")
        ),
    )
    (fold_dir / "annual_report.txt").write_text(
        generate_annual_report(full_backtest, dates),
        encoding="utf-8",
    )
    fold_result = _load_fold_result(metrics_path)
    fold_result.test_metrics = compute_metrics(full_backtest)
    metrics_path.write_text(
        json.dumps(asdict(fold_result), indent=2),
        encoding="utf-8",
    )
    _write_fold_complete_marker(
        fold_dir,
        fold_result,
        source="artifact_scope_v2_migration",
    )
    return True


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


def _resolve_train_feature_cache_dtype(
    config: ExperimentConfig,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    *,
    train_uses_panel_slab: bool,
    has_categorical_features: bool,
) -> torch.dtype | None:
    """Resolve the only pre-quantized feature-cache path proven AMP-equivalent."""
    if not bool(getattr(config.training, "cache_train_features_in_amp_dtype", False)):
        return None
    active_model_name = str(_active_model_config(config)["config_name"])
    tbp_cfg = config.training.transformer_base_portfolio
    if (
        device.type != "cuda"
        or amp_dtype not in {torch.float16, torch.bfloat16}
        or active_model_name != "transformer_base_portfolio"
        or bool(getattr(tbp_cfg, "sanitize_inputs", True))
        or bool(has_categorical_features)
        or not train_uses_panel_slab
    ):
        raise ValueError(
            "cache_train_features_in_amp_dtype requires CUDA BF16/FP16, "
            "transformer_base_portfolio panel-slab training, sanitize_inputs=false, "
            "and no categorical feature embeddings"
        )
    return amp_dtype


def _maybe_cache_tensors_on_device(
    *,
    name: str,
    tensors: tuple[torch.Tensor | None, ...],
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
    target_dtypes: tuple[torch.dtype | None, ...] | None = None,
) -> tuple[torch.Tensor | None, ...]:
    if not enabled or device.type != "cuda":
        return tensors
    if target_dtypes is None:
        target_dtypes = tuple(None for _ in tensors)
    if len(target_dtypes) != len(tensors):
        raise ValueError("target_dtypes must align with tensors")

    def _target_dtype(tensor: torch.Tensor, requested: torch.dtype | None) -> torch.dtype:
        if requested is None or not tensor.dtype.is_floating_point:
            return tensor.dtype
        return requested

    if all(
        tensor is None
        or (
            _tensor_on_requested_device(tensor, device)
            and tensor.dtype == _target_dtype(tensor, requested_dtype)
        )
        for tensor, requested_dtype in zip(tensors, target_dtypes, strict=True)
    ):
        return tensors

    required_bytes = sum(
        int(tensor.numel())
        * torch.empty((), dtype=_target_dtype(tensor, requested_dtype)).element_size()
        for tensor, requested_dtype in zip(tensors, target_dtypes, strict=True)
        if tensor is not None
        and (
            not _tensor_on_requested_device(tensor, device)
            or tensor.dtype != _target_dtype(tensor, requested_dtype)
        )
    )
    free_mem, total_mem = torch.cuda.mem_get_info(device)
    margin_bytes = int(max(0.0, float(safety_margin_gb)) * 1024**3)
    min_post_cache_free_bytes = 0
    if required_bytes >= 4 * 1024**3:
        # Large shared panels can fit by a static free-memory check but still
        # starve compiled eval/train workspaces on 16GB cards.
        min_post_cache_free_bytes = int(8 * 1024**3)
    post_cache_free_bytes = int(free_mem) - int(required_bytes)
    if min_post_cache_free_bytes > 0 and post_cache_free_bytes < min_post_cache_free_bytes:
        print(
            f"[gpu cache] skipped {name}: need={required_bytes/1024**3:.2f}GB "
            f"post_free={post_cache_free_bytes/1024**3:.2f}GB "
            f"min_post_free={min_post_cache_free_bytes/1024**3:.2f}GB "
            f"(free={free_mem/1024**3:.2f}GB total={total_mem/1024**3:.2f}GB)"
        )
        return tensors
    usable_bytes = int(max(0, int(free_mem) - margin_bytes) * max(0.0, float(target_fraction)))
    if required_bytes > usable_bytes:
        print(
            f"[gpu cache] skipped {name}: need={required_bytes/1024**3:.2f}GB "
            f"usable={usable_bytes/1024**3:.2f}GB "
            f"(free={free_mem/1024**3:.2f}GB total={total_mem/1024**3:.2f}GB)"
        )
        return tensors

    moved: list[torch.Tensor | None] = []
    converted_dtype = any(
        tensor is not None and tensor.dtype != _target_dtype(tensor, requested_dtype)
        for tensor, requested_dtype in zip(tensors, target_dtypes, strict=True)
    )
    try:
        for tensor, requested_dtype in zip(tensors, target_dtypes, strict=True):
            if tensor is None:
                moved.append(tensor)
            else:
                target_dtype = _target_dtype(tensor, requested_dtype)
                if _tensor_on_requested_device(tensor, device) and tensor.dtype == target_dtype:
                    moved.append(tensor)
                else:
                    moved.append(
                        tensor.to(
                            device=device,
                            dtype=target_dtype,
                            non_blocking=True,
                        )
                    )
        torch.cuda.synchronize(device)
        if converted_dtype:
            # A cross-device dtype conversion can leave the temporary source-
            # dtype staging allocation in the CUDA caching allocator. Release
            # it now so the advertised AMP-cache saving is available to the
            # compiled forward/backward workspace.
            torch.cuda.empty_cache()
        free_after, _ = torch.cuda.mem_get_info(device)
        print(
            f"[gpu cache] cached {name} on {device}: size={required_bytes/1024**3:.2f}GB "
            f"(free_before={free_mem/1024**3:.2f}GB free_after={free_after/1024**3:.2f}GB)"
        )
        return tuple(moved)
    except RuntimeError as exc:
        if "out of memory" not in str(exc).lower():
            raise
        print(f"[gpu cache] skipped {name}: CUDA OOM during cache attempt")
        del moved
        _release_cuda_memory(device)
        return tensors


def _pad_rows(tensor: torch.Tensor, target_rows: int, fill_value: int | float | bool = 0) -> torch.Tensor:
    current_rows = int(tensor.size(0))
    if current_rows >= target_rows:
        return tensor

    pad_shape = (target_rows - current_rows, *tensor.shape[1:])
    padded = tensor.new_full(pad_shape, fill_value)
    return torch.cat([tensor, padded], dim=0)


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
        volume_notional=first.volume_notional,
        can_short_open_mask=first.can_short_open_mask,
        force_short_cover_mask=first.force_short_cover_mask,
        force_exit_mask=first.force_exit_mask,
        symbol_indices=first.symbol_indices,
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
            volume_notional=split.volume_notional,
            can_short_open_mask=split.can_short_open_mask,
            force_short_cover_mask=split.force_short_cover_mask,
            force_exit_mask=split.force_exit_mask,
            sample_mask=sample_mask,
            symbol_indices=split.symbol_indices,
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
        volume_notional=split.volume_notional,
        can_short_open_mask=split.can_short_open_mask,
        force_short_cover_mask=split.force_short_cover_mask,
        force_exit_mask=split.force_exit_mask,
        sample_mask=sample_mask,
        symbol_indices=split.symbol_indices,
    )


def _densify_windowed_training_split_for_panel_slab(
    split: WindowedSplitTensors,
    date_indices: np.ndarray,
) -> WindowedSplitTensors:
    """Represent filtered, in-split dates as sample-masked slab rows.

    ``CrossSectionalDataset`` omits dates on which no asset has an executable
    return or a forced exit.  That is harmless for materialized windows, but a
    panel slab requires consecutive panel dates.  Restore only the dates that
    belong to the original split (never bridge a gap outside the split) and use
    ``sample_mask=False`` for the restored rows. The canonical backtest then
    freezes those all-nontradable rows while the loss excludes them, preserving
    the filtered-dataset semantics and a fixed compiled shape.
    """
    if len(split) == 0:
        return split
    allowed = np.asarray(date_indices, dtype=np.int64)
    if allowed.size == 0:
        return split
    min_valid = int(allowed[0]) + int(split.lookback) - 1
    dense_np = allowed[allowed >= min_valid]
    if dense_np.size == 0:
        return split

    valid_cpu = split.valid_indices.detach().to(device="cpu", dtype=torch.long).numpy()
    if np.array_equal(dense_np, valid_cpu):
        return split
    existing_mask = (
        None
        if split.sample_mask is None
        else split.sample_mask.detach().to(device="cpu", dtype=torch.bool).numpy()
    )
    original_enabled = {
        int(value): bool(existing_mask[idx]) if existing_mask is not None else True
        for idx, value in enumerate(valid_cpu.tolist())
    }
    sample_mask = torch.as_tensor(
        [original_enabled.get(int(value), False) for value in dense_np.tolist()],
        dtype=torch.bool,
        device=split.valid_indices.device,
    )
    dense_indices = torch.as_tensor(
        dense_np,
        dtype=torch.long,
        device=split.valid_indices.device,
    )
    return WindowedSplitTensors(
        features=split.features,
        valid_indices=dense_indices,
        future_log_returns=split.future_log_returns,
        tradable_mask=split.tradable_mask,
        can_buy_mask=split.can_buy_mask,
        can_sell_mask=split.can_sell_mask,
        benchmark=split.benchmark,
        lookback=split.lookback,
        volume_notional=split.volume_notional,
        can_short_open_mask=split.can_short_open_mask,
        force_short_cover_mask=split.force_short_cover_mask,
        force_exit_mask=split.force_exit_mask,
        sample_mask=sample_mask,
        symbol_indices=split.symbol_indices,
    )


def _batch_coupled_training_module_names(model: nn.Module) -> list[str]:
    """Return modules whose train-mode result depends on other batch rows."""
    batch_norm_types = (
        nn.BatchNorm1d,
        nn.BatchNorm2d,
        nn.BatchNorm3d,
        nn.SyncBatchNorm,
    )
    return [
        name or "<root>"
        for name, module in model.named_modules()
        if isinstance(module, batch_norm_types)
    ]


def _prepare_training_split_batch_shape(
    split: WindowedSplitTensors,
    model: nn.Module,
    *,
    batch_size: int,
    ddp_enabled: bool,
) -> WindowedSplitTensors:
    """Pad only when duplicate rows cannot affect another sample's forward."""
    coupled_modules = _batch_coupled_training_module_names(model)
    remainder = len(split) % max(1, int(batch_size))
    if coupled_modules:
        if ddp_enabled and remainder:
            preview = ", ".join(coupled_modules[:5])
            raise ValueError(
                "DDP cannot duplicate-pad a ragged training tail for a model with batch-coupled "
                "modules because padded rows would change valid-sample statistics. "
                f"rows={len(split)}, global_batch_size={batch_size}, remainder={remainder}, "
                f"batch_coupled_modules=[{preview}]. Choose a global batch size that exactly "
                "divides the training rows; rows will not be dropped."
            )
        return split
    return _pad_windowed_training_split(split, batch_size)


def _normalize_train_symbol_compaction(value: object) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    if normalized in {"", "0", "false", "no", "off", "none", "disabled"}:
        return "none"
    if normalized in {"1", "true", "yes", "on", "train_union", "union", "active_union"}:
        return "train_union"
    raise ValueError("training.train_symbol_compaction must be 'none' or 'train_union'")


def _normalize_train_symbol_compaction_bucket_size(value: object) -> int:
    if value is None:
        return 0
    bucket_size = int(value)
    if bucket_size < 0:
        raise ValueError("training.train_symbol_compaction_bucket_size must be >= 0")
    return bucket_size


def _bucketed_symbol_count(active_count: int, total_symbols: int, bucket_size: int) -> int:
    active_count = int(active_count)
    total_symbols = int(total_symbols)
    bucket_size = int(bucket_size)
    if active_count <= 0:
        return active_count
    if bucket_size <= 0:
        return active_count
    bucketed = ((active_count + bucket_size - 1) // bucket_size) * bucket_size
    return min(total_symbols, max(active_count, bucketed))


def _maybe_compact_train_windowed_symbols(
    split: WindowedSplitTensors,
    config: ExperimentConfig,
    *,
    label: str,
) -> WindowedSplitTensors:
    mode = _normalize_train_symbol_compaction(getattr(config.training, "train_symbol_compaction", "none"))
    if mode == "none":
        return split
    if int(split.num_symbols) <= 0 or int(split.valid_indices.numel()) == 0:
        return split
    row_mask = split.sample_mask
    if row_mask is not None:
        row_mask_cpu = row_mask.detach().to(device="cpu", dtype=torch.bool)
        if not bool(torch.any(row_mask_cpu).item()):
            return split
        date_indices = split.valid_indices.detach().to(device="cpu", dtype=torch.long)[row_mask_cpu]
    else:
        date_indices = split.valid_indices.detach().to(device="cpu", dtype=torch.long)
    if int(date_indices.numel()) == 0:
        return split

    tradable_source = split.tradable_mask
    date_indices = date_indices.to(device=tradable_source.device, dtype=torch.long)
    active = tradable_source.index_select(0, date_indices).to(dtype=torch.bool).any(dim=0)
    active_indices = active.nonzero(as_tuple=False).reshape(-1)
    active_count = int(active_indices.numel())
    total_symbols = int(split.num_symbols)
    if active_count <= 0 or active_count >= total_symbols:
        _progress(
            f"[{label}] train_symbol_compaction={mode}: skipped "
            f"active_symbols={active_count}/{total_symbols}"
        )
        return split
    compacted = split.subset_symbols(active_indices)
    bucket_size = _normalize_train_symbol_compaction_bucket_size(
        getattr(config.training, "train_symbol_compaction_bucket_size", 0)
    )
    target_symbols = _bucketed_symbol_count(active_count, total_symbols, bucket_size)
    padded_symbols = int(compacted.num_symbols)
    if target_symbols > padded_symbols:
        pad_symbol_index = None
        if compacted.symbol_indices is not None:
            pad_symbol_index = int(compacted.symbol_indices[0].detach().cpu().item())
        compacted = compacted.pad_symbols(target_symbols, pad_symbol_index=pad_symbol_index)
        padded_symbols = int(compacted.num_symbols)
    symbol_index_note = ""
    if compacted.symbol_indices is not None:
        before_indices = compacted.symbol_indices.detach()
        before_min = int(before_indices.min().detach().cpu().item())
        before_max = int(before_indices.max().detach().cpu().item())
        compacted_clamped = compacted.clamp_symbol_indices(total_symbols)
        clamped = compacted_clamped is not compacted
        compacted = compacted_clamped
        after_indices = compacted.symbol_indices.detach()
        after_min = int(after_indices.min().detach().cpu().item())
        after_max = int(after_indices.max().detach().cpu().item())
        if clamped:
            symbol_index_note = (
                f", symbol_index_range=[{after_min},{after_max}]"
                f", clamped_from=[{before_min},{before_max}]"
            )
        else:
            symbol_index_note = f", symbol_index_range=[{after_min},{after_max}]"
    bucket_note = ""
    if bucket_size > 0:
        bucket_note = f", padded_symbols={padded_symbols}, bucket_size={bucket_size}"
    _progress(
        f"[{label}] train_symbol_compaction={mode}: "
        f"active_symbols={active_count}/{total_symbols} ({active_count / max(1, total_symbols):.1%})"
        f"{bucket_note}{symbol_index_note}"
    )
    return compacted


def _prepare_windowed_split(
    split: WindowedSplitTensors,
    device: torch.device,
    non_blocking: bool,
    *,
    shared_base: WindowedSplitTensors | None = None,
    name: str | None = None,
) -> WindowedSplitTensors:
    start = time.perf_counter()
    pin_memory = device.type == "cuda" and non_blocking
    if shared_base is not None and _windowed_base_compatible(split, shared_base):
        if name:
            _progress(f"[windowed base] reused prepared shared base tensors for {name}")
        if _tensor_on_requested_device(shared_base.features, device):
            valid_indices = split.valid_indices.to(device=device, non_blocking=non_blocking)
            sample_mask = None
            if split.sample_mask is not None:
                sample_mask = split.sample_mask.to(device=device, non_blocking=non_blocking)
        else:
            valid_indices = _prepare_host_tensor(split.valid_indices, pin_memory)
            sample_mask = None if split.sample_mask is None else _prepare_host_tensor(split.sample_mask, pin_memory)
        prepared = WindowedSplitTensors(
            features=shared_base.features,
            valid_indices=valid_indices,
            future_log_returns=shared_base.future_log_returns,
            tradable_mask=shared_base.tradable_mask,
            can_buy_mask=shared_base.can_buy_mask,
            can_sell_mask=shared_base.can_sell_mask,
            benchmark=shared_base.benchmark,
            lookback=split.lookback,
            volume_notional=shared_base.volume_notional,
            can_short_open_mask=shared_base.can_short_open_mask,
            force_short_cover_mask=shared_base.force_short_cover_mask,
            force_exit_mask=shared_base.force_exit_mask,
            sample_mask=sample_mask,
            symbol_indices=shared_base.symbol_indices,
        )
        if name:
            _progress(
                f"[windowed base] prepared {name} rows={len(prepared)} symbols={prepared.num_symbols} "
                f"shared_base=true device={prepared.features.device} elapsed={time.perf_counter() - start:.2f}s"
            )
        return prepared
    if name:
        shared_note = "incompatible" if shared_base is not None else "none"
        _progress(
            f"[windowed base] preparing independent base for {name} "
            f"rows={len(split)} symbols={split.num_symbols} shared_base={shared_note}"
        )
    prepared = WindowedSplitTensors(
        features=_prepare_host_tensor(split.features, pin_memory),
        valid_indices=_prepare_host_tensor(split.valid_indices, pin_memory),
        future_log_returns=_prepare_host_tensor(split.future_log_returns, pin_memory),
        tradable_mask=_prepare_host_tensor(split.tradable_mask, pin_memory),
        can_buy_mask=_prepare_host_tensor(split.can_buy_mask, pin_memory),
        can_sell_mask=_prepare_host_tensor(split.can_sell_mask, pin_memory),
        benchmark=_prepare_host_tensor(split.benchmark, pin_memory),
        lookback=split.lookback,
        volume_notional=(
            None if split.volume_notional is None else _prepare_host_tensor(split.volume_notional, pin_memory)
        ),
        can_short_open_mask=_prepare_host_tensor(split.can_short_open_mask, pin_memory),
        force_short_cover_mask=_prepare_host_tensor(split.force_short_cover_mask, pin_memory),
        force_exit_mask=_prepare_host_tensor(split.force_exit_mask, pin_memory),
        sample_mask=None if split.sample_mask is None else _prepare_host_tensor(split.sample_mask, pin_memory),
        symbol_indices=None if split.symbol_indices is None else _prepare_host_tensor(split.symbol_indices, pin_memory),
    )
    if name:
        _progress(
            f"[windowed base] prepared {name} rows={len(prepared)} symbols={prepared.num_symbols} "
            f"shared_base=false device={prepared.features.device} elapsed={time.perf_counter() - start:.2f}s"
        )
    return prepared


def _windowed_base_tensors(split: WindowedSplitTensors) -> tuple[torch.Tensor | None, ...]:
    return (
        split.features,
        split.future_log_returns,
        split.tradable_mask,
        split.can_buy_mask,
        split.can_sell_mask,
        split.can_short_open_mask,
        split.force_short_cover_mask,
        split.force_exit_mask,
        split.benchmark,
        split.volume_notional,
    )


def _windowed_metadata_tensors(split: WindowedSplitTensors) -> tuple[torch.Tensor, ...]:
    if split.sample_mask is None:
        return (split.valid_indices,)
    return (split.valid_indices, split.sample_mask)


def _with_windowed_base(
    split: WindowedSplitTensors,
    base_tensors: tuple[torch.Tensor | None, ...],
) -> WindowedSplitTensors:
    if any(tensor is None for tensor in base_tensors[:9]):
        raise ValueError("required windowed base tensor is missing")
    return WindowedSplitTensors(
        features=base_tensors[0],
        valid_indices=split.valid_indices,
        future_log_returns=base_tensors[1],
        tradable_mask=base_tensors[2],
        can_buy_mask=base_tensors[3],
        can_sell_mask=base_tensors[4],
        can_short_open_mask=base_tensors[5],
        force_short_cover_mask=base_tensors[6],
        force_exit_mask=base_tensors[7],
        benchmark=base_tensors[8],
        volume_notional=base_tensors[9],
        lookback=split.lookback,
        sample_mask=split.sample_mask,
        symbol_indices=split.symbol_indices,
    )


def _with_windowed_metadata(
    split: WindowedSplitTensors,
    metadata_tensors: tuple[torch.Tensor, ...],
) -> WindowedSplitTensors:
    return WindowedSplitTensors(
        features=split.features,
        valid_indices=metadata_tensors[0],
        future_log_returns=split.future_log_returns,
        tradable_mask=split.tradable_mask,
        can_buy_mask=split.can_buy_mask,
        can_sell_mask=split.can_sell_mask,
        can_short_open_mask=split.can_short_open_mask,
        force_short_cover_mask=split.force_short_cover_mask,
        force_exit_mask=split.force_exit_mask,
        benchmark=split.benchmark,
        volume_notional=split.volume_notional,
        lookback=split.lookback,
        sample_mask=None if split.sample_mask is None else metadata_tensors[1],
        symbol_indices=split.symbol_indices,
    )


def _maybe_cache_windowed_base_on_device(
    *,
    name: str,
    split: WindowedSplitTensors,
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
    feature_dtype: torch.dtype | None = None,
) -> WindowedSplitTensors:
    base_tensors = _windowed_base_tensors(split)
    moved = _maybe_cache_tensors_on_device(
        name=f"{name} shared base",
        tensors=base_tensors,
        device=device,
        enabled=enabled,
        target_fraction=target_fraction,
        safety_margin_gb=safety_margin_gb,
        target_dtypes=(feature_dtype, *(None for _ in base_tensors[1:])),
    )
    return _with_windowed_base(split, moved)


def _maybe_cache_windowed_metadata_on_device(
    *,
    name: str,
    split: WindowedSplitTensors,
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
) -> WindowedSplitTensors:
    if not _tensor_on_requested_device(split.features, device):
        _progress(f"[gpu cache] skipped {name} metadata: shared base remains on {split.features.device.type}")
        return split
    moved = _maybe_cache_tensors_on_device(
        name=f"{name} metadata",
        tensors=_windowed_metadata_tensors(split),
        device=device,
        enabled=enabled,
        target_fraction=target_fraction,
        safety_margin_gb=safety_margin_gb,
    )
    return _with_windowed_metadata(split, moved)


def _maybe_cache_windowed_split_on_device(
    *,
    name: str,
    split: WindowedSplitTensors,
    device: torch.device,
    enabled: bool,
    target_fraction: float,
    safety_margin_gb: float,
    feature_dtype: torch.dtype | None = None,
) -> WindowedSplitTensors:
    split = _maybe_cache_windowed_base_on_device(
        name=name,
        split=split,
        device=device,
        enabled=enabled,
        target_fraction=target_fraction,
        safety_margin_gb=safety_margin_gb,
        feature_dtype=feature_dtype,
    )
    return _maybe_cache_windowed_metadata_on_device(
        name=name,
        split=split,
        device=device,
        enabled=enabled,
        target_fraction=target_fraction,
        safety_margin_gb=safety_margin_gb,
    )


def _windowed_base_compatible(a: WindowedSplitTensors, b: WindowedSplitTensors) -> bool:
    attrs = (
        "features",
        "future_log_returns",
        "tradable_mask",
        "can_buy_mask",
        "can_sell_mask",
        "can_short_open_mask",
        "force_short_cover_mask",
        "force_exit_mask",
        "benchmark",
        "volume_notional",
    )
    for attr in attrs:
        lhs = getattr(a, attr)
        rhs = getattr(b, attr)
        if lhs is None or rhs is None:
            if lhs is not rhs:
                return False
            continue
        if tuple(lhs.shape) != tuple(rhs.shape) or lhs.dtype != rhs.dtype:
            return False
    if (a.symbol_indices is None) != (b.symbol_indices is None):
        return False
    if a.symbol_indices is not None and b.symbol_indices is not None:
        if tuple(a.symbol_indices.shape) != tuple(b.symbol_indices.shape):
            return False
        if not torch.equal(
            a.symbol_indices.detach().to(device="cpu", dtype=torch.long),
            b.symbol_indices.detach().to(device="cpu", dtype=torch.long),
        ):
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
        can_short_open_mask=cached_base.can_short_open_mask,
        force_short_cover_mask=cached_base.force_short_cover_mask,
        force_exit_mask=cached_base.force_exit_mask,
        benchmark=cached_base.benchmark,
        volume_notional=cached_base.volume_notional,
        lookback=split.lookback,
        sample_mask=sample_mask,
        symbol_indices=cached_base.symbol_indices,
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
    tradable_pad = tradable_mask[-1:].expand((pad_rows,) + tuple(tradable_mask.shape[1:]))
    buy_pad = can_buy_mask[-1:].expand((pad_rows,) + tuple(can_buy_mask.shape[1:]))
    sell_pad = can_sell_mask[-1:].expand((pad_rows,) + tuple(can_sell_mask.shape[1:]))
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


def _pad_eval_metadata_first_dim(
    date_indices: torch.Tensor,
    returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    benchmark: torch.Tensor,
    *,
    target_rows: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, int]:
    valid_rows = int(date_indices.size(0))
    target_rows = int(target_rows)
    if valid_rows <= 0 or valid_rows >= target_rows:
        return date_indices, returns, tradable_mask, can_buy_mask, can_sell_mask, benchmark, valid_rows

    pad_rows = target_rows - valid_rows
    date_pad = date_indices[-1:].expand(pad_rows)
    returns_pad = returns.new_zeros((pad_rows,) + tuple(returns.shape[1:]))
    tradable_pad = tradable_mask[-1:].expand((pad_rows,) + tuple(tradable_mask.shape[1:]))
    buy_pad = can_buy_mask[-1:].expand((pad_rows,) + tuple(can_buy_mask.shape[1:]))
    sell_pad = can_sell_mask[-1:].expand((pad_rows,) + tuple(can_sell_mask.shape[1:]))
    benchmark_pad = benchmark.new_zeros((pad_rows,) + tuple(benchmark.shape[1:]))
    return (
        torch.cat((date_indices, date_pad), dim=0),
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


def _safe_expm1(log_sum: float) -> float:
    # Keep conversion from cumulative log-return to arithmetic return numerically safe.
    if log_sum >= 709.78:
        return math.inf
    if log_sum <= -745.13:
        return -1.0
    return math.expm1(log_sum)


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
            "benchmark_sharpe": 0.0,
            "sortino": 0.0,
            "benchmark_sortino": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "turnover": 0.0,
            "daily_hit_rate": 0.0,
            "excess_return_vs_benchmark": 0.0,
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
    cum_r = _safe_expm1(sum_r)
    cum_b = _safe_expm1(sum_b)
    ann_r = _safe_expm1(mean_r * 252.0)
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
        "benchmark_sharpe": float(mean_b / std_b * np.sqrt(252.0)) if std_b > 0 else 0.0,
        "sortino": float(mean_r / downside_dev * np.sqrt(252.0)) if downside_dev > 0 else 0.0,
        "benchmark_sortino": float(mean_b / downside_dev_b * np.sqrt(252.0)) if downside_dev_b > 0 else 0.0,
        "max_drawdown": max_dd,
        "calmar": ann_r / abs(max_dd) if max_dd < 0.0 else 0.0,
        "turnover": float(t.sum().item()) / n,
        "daily_hit_rate": float((r > 0).to(torch.float64).sum().item()) / n,
        "excess_return_vs_benchmark": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def _run_eval_backtest_from_weight_buffers(
    weights_all: torch.Tensor,
    future_log_returns_all: torch.Tensor,
    tradable_mask_all: torch.Tensor,
    can_buy_mask_all: torch.Tensor,
    can_sell_mask_all: torch.Tensor,
    can_short_open_mask_all: torch.Tensor,
    force_short_cover_mask_all: torch.Tensor,
    force_exit_mask_all: torch.Tensor,
    benchmark_all: torch.Tensor,
    *,
    device: torch.device,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    min_trade_weight: float,
    backtest_chunk_rows: int,
    compute_metrics_summary: bool,
    return_weights_history: bool,
    profile_timing: bool,
    progress_label: str | None,
    timing: TimingBreakdown,
    reset_at_rows: Sequence[int] | None,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    volume_notional_all: torch.Tensor | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
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
    prev_alive: torch.Tensor | None = None
    for chunk_idx, (start, end, reset_state) in enumerate(backtest_ranges, start=1):
        if reset_state:
            prev_weights = None
            prev_alive = None
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
        short_open_mask_chunk = can_short_open_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        force_cover_mask_chunk = force_short_cover_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        force_exit_mask_chunk = force_exit_mask_all[start:end].to(device=device, non_blocking=non_blocking)
        bench_chunk = benchmark_all[start:end].to(device=device, non_blocking=non_blocking)
        volume_notional_chunk = (
            None
            if volume_notional_all is None
            else volume_notional_all[start:end].to(device=device, non_blocking=non_blocking)
        )
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
        short_open_mask_chunk = _pad_rows(
            short_open_mask_chunk,
            int(weights_chunk.size(0)),
            fill_value=False,
        )
        force_cover_mask_chunk = _pad_rows(
            force_cover_mask_chunk,
            int(weights_chunk.size(0)),
            fill_value=False,
        )
        force_exit_mask_chunk = _pad_rows(
            force_exit_mask_chunk,
            int(weights_chunk.size(0)),
            fill_value=False,
        )
        if valid_rows < int(weights_chunk.size(0)):
            # Padded compile-shape rows must be exact recurrent no-ops.  With
            # MTM state, repeating the last real return would otherwise drift
            # holdings or even fabricate ruin after the logical segment ended.
            returns_chunk = returns_chunk.clone()
            mask_chunk = mask_chunk.clone()
            buy_mask_chunk = buy_mask_chunk.clone()
            sell_mask_chunk = sell_mask_chunk.clone()
            returns_chunk[valid_rows:] = 0.0
            mask_chunk[valid_rows:] = False
            buy_mask_chunk[valid_rows:] = False
            sell_mask_chunk[valid_rows:] = False
        if volume_notional_chunk is not None and int(volume_notional_chunk.size(0)) < int(weights_chunk.size(0)):
            volume_notional_chunk = _pad_rows(volume_notional_chunk, int(weights_chunk.size(0)), fill_value=float("nan"))
        volume_limit_chunk = _volume_limit_weights_from_notional(
            volume_notional_chunk,
            max_volume_participation=max_volume_participation,
            volume_participation_equity=volume_participation_equity,
            device=device,
            dtype=weights_chunk.dtype,
        )
        initial_weights_chunk = prev_weights
        _maybe_sync_cuda(device, profile_timing)
        timing.backtest_prepare_s += time.perf_counter() - backtest_prepare_start

        backtest_runner_start = time.perf_counter()
        # Artifact passes request the full weights history only once. Compiling
        # that distinct recurrent output graph costs minutes on short folds and
        # cannot amortize; repeated epoch metrics keep the compiled fast path.
        compile_default = "0" if return_weights_history else "1"
        compile_context = (
            nullcontext()
            if _env_truthy("STOCKAGENT_EVAL_BACKTEST_COMPILE", compile_default)
            else _temporary_env("STOCKAGENT_BACKTEST_COMPILE", "0")
        )
        with compile_context:
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
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
                can_buy_mask=buy_mask_chunk,
                can_sell_mask=sell_mask_chunk,
                can_short_open_mask=short_open_mask_chunk,
                force_short_cover_mask=force_cover_mask_chunk,
                force_exit_mask=force_exit_mask_chunk,
                return_weights_history=return_weights_history,
                initial_weights=initial_weights_chunk,
                initial_alive=prev_alive,
                volume_limit_weights=volume_limit_chunk,
            )
        _maybe_sync_cuda(device, profile_timing)
        timing.backtest_runner_s += time.perf_counter() - backtest_runner_start

        backtest_finalize_start = time.perf_counter()
        prev_weights = _detach_portfolio_state(backtest_chunk.final_weights)
        prev_alive = _detach_portfolio_state(backtest_chunk.final_alive)
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
        final_alive=prev_alive,
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
    min_trade_weight: float,
    model_chunk_rows: int,
    backtest_chunk_rows: int,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
    volume_notional: torch.Tensor | None = None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
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
    can_short_open_mask_all = (
        can_sell_mask_all.clone()
        if can_short_open_mask is None
        else can_short_open_mask.to(device=device, non_blocking=non_blocking)
    )
    force_short_cover_mask_all = (
        torch.zeros_like(tradable_mask_all, dtype=torch.bool)
        if force_short_cover_mask is None
        else force_short_cover_mask.to(device=device, non_blocking=non_blocking)
    )
    force_exit_mask_all = (
        torch.zeros_like(tradable_mask_all, dtype=torch.bool)
        if force_exit_mask is None
        else force_exit_mask.to(device=device, non_blocking=non_blocking)
    )
    benchmark_all = benchmark.to(device=device, non_blocking=non_blocking)
    volume_notional_all = (
        None
        if volume_notional is None
        else volume_notional.to(device=device, non_blocking=non_blocking)
    )
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
                model_output_chunk = _call_model(model, x_chunk, mask_chunk_padded, return_aux=False)
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
            can_short_open_mask_all,
            force_short_cover_mask_all,
            force_exit_mask_all,
            benchmark_all,
            device=device,
            non_blocking=non_blocking,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            backtest_chunk_rows=backtest_chunk_rows,
            portfolio_activation=portfolio_activation,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing=timing,
            reset_at_rows=reset_at_rows,
            volume_notional_all=volume_notional_all,
            max_volume_participation=max_volume_participation,
            volume_participation_equity=volume_participation_equity,
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
    min_trade_weight: float = 0.0,
    chunk_rows: int = 1,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    backtest_chunk_rows: int | None = None,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
    volume_notional: torch.Tensor | None = None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    """Evaluate materialized inputs through the same decoupled canonical backtest."""
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
        min_trade_weight,
        model_chunk_rows=chunk_rows,
        backtest_chunk_rows=(
            int(backtest_chunk_rows)
            if backtest_chunk_rows is not None
            else int(chunk_rows)
        ),
        portfolio_activation=portfolio_activation,
        compute_ic=compute_ic,
        compute_metrics_summary=compute_metrics_summary,
        return_weights_history=return_weights_history,
        profile_timing=profile_timing,
        progress_label=progress_label,
        timing_out=timing_out,
        reset_at_rows=reset_at_rows,
        volume_notional=volume_notional,
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        force_exit_mask=force_exit_mask,
        max_volume_participation=max_volume_participation,
        volume_participation_equity=volume_participation_equity,
    )


def _evaluate_windowed_tensor_batch_decoupled(
    model: nn.Module,
    panel_slab_model: nn.Module | None,
    split: WindowedSplitTensors,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    min_trade_weight: float,
    model_chunk_rows: int,
    backtest_chunk_rows: int,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    model.eval()
    if panel_slab_model is not None:
        panel_slab_model.eval()
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
    short_open_mask_all: torch.Tensor | None = None
    force_cover_mask_all: torch.Tensor | None = None
    force_exit_mask_all: torch.Tensor | None = None
    benchmark_all: torch.Tensor | None = None
    volume_notional_all: torch.Tensor | None = None
    panel_forward_model = _panel_forward_module(model)

    with torch.inference_mode():
        for chunk_idx, (start, end) in enumerate(model_ranges, start=1):
            _maybe_cudagraph_step_begin()
            log_chunk_progress = bool(progress_label) and _should_log_eval_chunk(chunk_idx, len(model_ranges))
            if log_chunk_progress:
                _progress(f"{progress_label}: model chunk {chunk_idx}/{len(model_ranges)} rows=[{start},{end})")
            prepare_device = _windowed_prepare_device(split, device)
            batch_prepare_start = time.perf_counter()
            direct_panel_slab = False
            if panel_forward_model is not None:
                batch = None
                if panel_slab_model is not None and int(end - start) == int(model_chunk_rows):
                    batch = split.panel_slab_batch_by_rows(
                        start,
                        end,
                        device=prepare_device,
                        non_blocking=False,
                        prepare_timing=timing,
                    )
                    direct_panel_slab = batch is not None
                if batch is None:
                    batch = split.batch_metadata_by_rows(
                        start,
                        end,
                        device=prepare_device,
                        non_blocking=False,
                        prepare_timing=timing,
                    )
                    date_indices_chunk = batch["date_indices"]
            else:
                window_materialize_start = time.perf_counter()
                batch = split.batch_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                    prepare_timing=timing,
                )
                _maybe_sync_cuda(prepare_device, profile_timing)
                timing.window_materialize_s += time.perf_counter() - window_materialize_start
                x_chunk = batch["x"]
            _maybe_sync_cuda(prepare_device, profile_timing)
            batch_prepare_elapsed = time.perf_counter() - batch_prepare_start
            timing.batch_prepare_s += batch_prepare_elapsed

            h2d_start = time.perf_counter()
            batch = _move_windowed_batch_to_device(batch, device, non_blocking)
            if panel_forward_model is not None and not direct_panel_slab:
                date_indices_chunk = batch["date_indices"]
            elif panel_forward_model is None:
                x_chunk = batch["x"]
            returns_chunk = batch["future_log_returns"]
            mask_chunk = batch["tradable_mask"]
            buy_mask_chunk = batch["can_buy_mask"]
            sell_mask_chunk = batch["can_sell_mask"]
            short_open_mask_chunk = batch["can_short_open_mask"]
            force_cover_mask_chunk = batch["force_short_cover_mask"]
            force_exit_mask_chunk = batch["force_exit_mask"]
            bench_chunk = batch["benchmark"]
            volume_notional_chunk = batch.get("volume_notional")
            _maybe_sync_cuda(device, profile_timing)
            h2d_elapsed = time.perf_counter() - h2d_start
            timing.h2d_transfer_s += h2d_elapsed

            pad_start = time.perf_counter()
            if panel_forward_model is not None and direct_panel_slab:
                returns_chunk_padded = returns_chunk
                mask_chunk_padded = mask_chunk
                buy_mask_chunk_padded = buy_mask_chunk
                sell_mask_chunk_padded = sell_mask_chunk
                bench_chunk_padded = bench_chunk
                valid_rows = int(end - start)
            elif panel_forward_model is not None:
                (
                    date_indices_chunk,
                    returns_chunk_padded,
                    mask_chunk_padded,
                    buy_mask_chunk_padded,
                    sell_mask_chunk_padded,
                    bench_chunk_padded,
                    valid_rows,
                ) = _pad_eval_metadata_first_dim(
                    date_indices_chunk,
                    returns_chunk,
                    mask_chunk,
                    buy_mask_chunk,
                    sell_mask_chunk,
                    bench_chunk,
                    target_rows=model_chunk_rows,
                )
                panel_batch_for_forward = dict(batch)
                panel_batch_for_forward["date_indices"] = date_indices_chunk
            else:
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
            pad_elapsed = time.perf_counter() - pad_start
            timing.batch_prepare_s += pad_elapsed
            timing.transfer_s += batch_prepare_elapsed + h2d_elapsed + pad_elapsed

            forward_start = time.perf_counter()
            with _autocast_context(device, amp_dtype):
                if panel_forward_model is not None:
                    if direct_panel_slab:
                        if panel_slab_model is None:
                            raise RuntimeError("panel slab model unexpectedly unavailable")
                        model_output_chunk = panel_slab_model(
                            batch["feature_slab"],
                            mask_chunk_padded,
                            batch.get("symbol_indices"),
                        )
                    else:
                        model_output_chunk = _call_panel_forward_for_batch(
                            panel_forward_model=panel_forward_model,
                            panel_slab_model=panel_slab_model,
                            split=split,
                            batch=batch if int(valid_rows) == int(model_chunk_rows) else panel_batch_for_forward,
                            mask=mask_chunk_padded,
                            device=device,
                            non_blocking=non_blocking,
                            return_aux=False,
                            allow_slab=int(valid_rows) == int(model_chunk_rows),
                            rows=int(valid_rows),
                        )
                else:
                    model_output_chunk = _call_model(model, x_chunk, mask_chunk_padded, return_aux=False)
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
                short_open_mask_all = torch.empty(
                    (total_rows, split.num_symbols), device=device, dtype=short_open_mask_chunk.dtype
                )
                force_cover_mask_all = torch.empty(
                    (total_rows, split.num_symbols), device=device, dtype=force_cover_mask_chunk.dtype
                )
                force_exit_mask_all = torch.empty(
                    (total_rows, split.num_symbols), device=device, dtype=force_exit_mask_chunk.dtype
                )
                benchmark_all = torch.empty((total_rows,), device=device, dtype=bench_chunk.dtype)
                if volume_notional_chunk is not None:
                    volume_notional_all = torch.empty(
                        (total_rows, split.num_symbols),
                        device=device,
                        dtype=volume_notional_chunk.dtype,
                    )
            weights_all[start:end].copy_(weights_chunk[:valid_rows])
            returns_all[start:end].copy_(returns_chunk_padded[:valid_rows])
            mask_all[start:end].copy_(mask_chunk_padded[:valid_rows])
            buy_mask_all[start:end].copy_(buy_mask_chunk_padded[:valid_rows])
            sell_mask_all[start:end].copy_(sell_mask_chunk_padded[:valid_rows])
            short_open_mask_all[start:end].copy_(short_open_mask_chunk[:valid_rows])
            force_cover_mask_all[start:end].copy_(force_cover_mask_chunk[:valid_rows])
            force_exit_mask_all[start:end].copy_(force_exit_mask_chunk[:valid_rows])
            benchmark_all[start:end].copy_(bench_chunk_padded[:valid_rows])
            if volume_notional_all is not None and volume_notional_chunk is not None:
                volume_notional_all[start:end].copy_(volume_notional_chunk[:valid_rows])

        if (
            weights_all is None
            or returns_all is None
            or mask_all is None
            or buy_mask_all is None
            or sell_mask_all is None
            or short_open_mask_all is None
            or force_cover_mask_all is None
            or force_exit_mask_all is None
            or benchmark_all is None
        ):
            raise RuntimeError("windowed eval produced no buffers")

        backtest, metrics = _run_eval_backtest_from_weight_buffers(
            weights_all,
            returns_all,
            mask_all,
            buy_mask_all,
            sell_mask_all,
            short_open_mask_all,
            force_cover_mask_all,
            force_exit_mask_all,
            benchmark_all,
            device=device,
            non_blocking=non_blocking,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            backtest_chunk_rows=backtest_chunk_rows,
            portfolio_activation=portfolio_activation,
            compute_metrics_summary=compute_metrics_summary,
            return_weights_history=return_weights_history,
            profile_timing=profile_timing,
            progress_label=progress_label,
            timing=timing,
            reset_at_rows=reset_at_rows,
            volume_notional_all=volume_notional_all,
            max_volume_participation=max_volume_participation,
            volume_participation_equity=volume_participation_equity,
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
    panel_slab_model: nn.Module | None,
    split: WindowedSplitTensors,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    long_only: bool,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_leverage: float,
    min_trade_weight: float = 0.0,
    chunk_rows: int = 1,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    backtest_chunk_rows: int | None = None,
    compute_ic: bool = True,
    compute_metrics_summary: bool = True,
    return_weights_history: bool = True,
    profile_timing: bool = False,
    progress_label: str | None = None,
    timing_out: TimingBreakdown | None = None,
    reset_at_rows: Sequence[int] | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    effective_backtest_chunk_rows = int(backtest_chunk_rows) if backtest_chunk_rows is not None else int(chunk_rows)
    return _evaluate_windowed_tensor_batch_decoupled(
        model,
        panel_slab_model,
        split,
        device,
        amp_dtype,
        non_blocking,
        long_only,
        buy_fee_rate,
        sell_fee_rate,
        max_turnover_ratio,
        gross_leverage,
        min_trade_weight,
        model_chunk_rows=chunk_rows,
        backtest_chunk_rows=effective_backtest_chunk_rows,
        portfolio_activation=portfolio_activation,
        compute_ic=compute_ic,
        compute_metrics_summary=compute_metrics_summary,
        return_weights_history=return_weights_history,
        profile_timing=profile_timing,
        progress_label=progress_label,
        timing_out=timing_out,
        reset_at_rows=reset_at_rows,
        max_volume_participation=max_volume_participation,
        volume_participation_equity=volume_participation_equity,
    )

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
    # This is a sizing probe, not a training observation.  In particular it
    # must not update BatchNorm buffers or consume the stochastic stream that a
    # fresh/resumed epoch will see.  Preserve per-module modes because some
    # models deliberately keep selected submodules in eval mode while the root
    # module is training.
    module_modes = [(module, bool(module.training)) for module in model.modules()]
    rng_device = device.index if device.index is not None else torch.cuda.current_device()
    try:
        model.eval()
        with torch.random.fork_rng(devices=[rng_device], enabled=True), torch.inference_mode():
            x_probe = x[:probe_rows].to(device=device, non_blocking=True)
            mask_probe = tradable_mask[:probe_rows].to(device=device, non_blocking=True)
            with _autocast_context(device, amp_dtype):
                probe_output = model(x_probe, mask_probe)
                _, _ = _extract_weights_and_aux(probe_output)
    finally:
        for module, training in module_modes:
            module.training = training
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
    del probe_output, x_probe, mask_probe
    _release_cuda_memory(device)

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
    if not _distributed_should_write():
        return
    _write_summary(results, output_path)

    stale_combined_log_plot = output_path / "walkforward_equity_curve_log.png"
    if stale_combined_log_plot.exists():
        stale_combined_log_plot.unlink()
    stale_first_test_year_only_plot = output_path / "walkforward_first_test_year_only.png"
    if stale_first_test_year_only_plot.exists():
        stale_first_test_year_only_plot.unlink()
    first_year_plot_paths = (
        output_path / "walkforward_first_year_cumulative_returns.png",
        output_path / "walkforward_first_year_cumulative_returns_log10.png",
        output_path / "walkforward_first_year_fold_metrics.png",
        output_path / "walkforward_first_year_turnover_concentration.png",
    )
    for stale_path in first_year_plot_paths:
        if stale_path.exists():
            stale_path.unlink()

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
        deployment_path = _deployment_backtest_path(fold_dir)
        backtest_path = deployment_path if deployment_path.exists() else _backtest_path(fold_dir)
        if not backtest_path.exists():
            continue
        fold_backtest, fold_dates = _load_backtest_artifact(backtest_path)
        if int(fold_dates.size) == 0:
            continue
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
        plot_fold_first_year_returns_log10(
            all_first_year_dates,
            all_first_year_strategy_log,
            all_first_year_baseline_log,
            output_path / "walkforward_first_year_cumulative_returns_log10.png",
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
    if not _distributed_should_write():
        return None
    if not bool(getattr(config.training, "explain_after_each_fold", False)):
        return None

    from stockagent.explainability import (
        run_loaded_model_explanation,
        settings_from_training_config,
    )

    settings = settings_from_training_config(config.training)
    return run_loaded_model_explanation(
        config=config,
        panel=panel,
        fold=fold,
        model=model,
        checkpoint_path=checkpoint_path,
        output_dir=output_path,
        split="test",
        explain_output_dir=None,
        settings=settings,
        write_plots=bool(getattr(config.training, "explain_write_plots", True)),
        plot_backend=str(getattr(config.training, "plot_backend", "auto")),
        device=device,
        timing_file_name="train_explainability_timing.json",
        write_fold_stability=bool(getattr(config.training, "explain_fold_stability", False)),
    )


def _load_completed_fold_result(
    output_path: Path,
    fold_id: int,
    expected_manifest: Mapping[str, Any] | None = None,
    expected_fold: WalkForwardFold | None = None,
) -> FoldResult | None:
    fold_dir = _fold_dir(output_path, fold_id)
    metrics_path = _metrics_path(fold_dir)
    model_path = _model_path(fold_dir)
    backtest_path = _backtest_path(fold_dir)
    if (
        _has_completed_fold_marker(fold_dir)
        and metrics_path.exists()
        and model_path.exists()
        and backtest_path.exists()
    ):
        if expected_manifest is not None:
            checkpoint_path = _best_checkpoint_path(fold_dir)
            if not checkpoint_path.exists():
                return None
            checkpoint = _load_checkpoint(checkpoint_path)
            _validate_checkpoint_manifest(
                checkpoint,
                expected_manifest,
                checkpoint_path=checkpoint_path,
                scope="artifact",
                expected_fold=expected_fold,
            )
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


def _resolve_distributed_data_parallel(config: ExperimentConfig, device: torch.device) -> bool:
    strategy = str(getattr(config.training, "multi_gpu_strategy", "none") or "none").strip().lower().replace("-", "_")
    if strategy not in {"distributed_data_parallel", "ddp", "distributed", "torch_ddp"}:
        return False
    if device.type != "cuda":
        raise RuntimeError("training.multi_gpu_strategy=distributed_data_parallel requires environment.device=cuda")
    if not torch.cuda.is_available():
        raise RuntimeError("training.multi_gpu_strategy=distributed_data_parallel requires CUDA, but CUDA is unavailable")
    if not _distributed_is_initialized():
        raise RuntimeError(
            "training.multi_gpu_strategy=distributed_data_parallel requires torchrun. "
            "Run via train.py so it can relaunch automatically, or launch with torchrun manually."
        )
    if _distributed_world_size() < 2:
        raise RuntimeError("distributed_data_parallel requires WORLD_SIZE >= 2")
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    if local_rank < 0 or local_rank >= torch.cuda.device_count():
        raise RuntimeError(
            f"LOCAL_RANK={local_rank} is unavailable; visible CUDA device_count={torch.cuda.device_count()}"
        )
    torch.cuda.set_device(local_rank)
    return True


def _wrap_distributed_data_parallel_model(
    module: nn.Module,
    *,
    config: ExperimentConfig,
    device: torch.device,
) -> DistributedDataParallel:
    ddp_kwargs: dict[str, Any] = {
        "device_ids": [device.index] if device.type == "cuda" else None,
        "output_device": device.index if device.type == "cuda" else None,
        "bucket_cap_mb": int(getattr(config.training, "ddp_bucket_cap_mb", 4)),
        "find_unused_parameters": False,
    }
    try:
        ddp_params = inspect.signature(DistributedDataParallel).parameters
        if "gradient_as_bucket_view" in ddp_params:
            ddp_kwargs["gradient_as_bucket_view"] = True
    except (TypeError, ValueError):
        pass
    try:
        return DistributedDataParallel(module, static_graph=True, **ddp_kwargs)
    except TypeError:
        return DistributedDataParallel(module, **ddp_kwargs)


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


def _runtime_cache_path(value: object) -> Path | None:
    raw = str(value or "").strip()
    if raw.lower() in {"", "0", "false", "off", "no", "none"}:
        return None
    return Path(os.path.expandvars(raw)).expanduser()


def _configure_compile_cache_paths(
    *,
    torchinductor_cache_dir: object = "~/.cache/torchinductor",
    triton_cache_dir: object = "~/.cache/triton",
    cuda_cache_path: object = "~/.cache/nv_cuda",
    force: bool = False,
) -> None:
    for env_name, raw_path in (
        ("TORCHINDUCTOR_CACHE_DIR", torchinductor_cache_dir),
        ("TRITON_CACHE_DIR", triton_cache_dir),
        ("CUDA_CACHE_PATH", cuda_cache_path),
    ):
        path = _runtime_cache_path(raw_path)
        if path is None:
            continue
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception:
            continue
        if force or not os.environ.get(env_name):
            os.environ[env_name] = str(path)


def _prepend_compile_toolchain_paths() -> None:
    _configure_compile_cache_paths()
    normalize_cuda_env()
    entries: list[str] = []
    env_bin = Path(sys.executable).resolve().parent
    env_root = env_bin.parent
    os.environ.setdefault("CONDA_PREFIX", str(env_root))
    ptxas_path = env_bin / "ptxas"
    if ptxas_path.exists():
        os.environ.setdefault("TRITON_PTXAS_PATH", str(ptxas_path))
        os.environ.setdefault("TRITON_PTXAS_BLACKWELL_PATH", str(ptxas_path))
    cc_path = env_bin / "x86_64-conda-linux-gnu-gcc"
    cxx_path = env_bin / "x86_64-conda-linux-gnu-g++"
    if cc_path.exists():
        os.environ.setdefault("CC", str(cc_path))
    if cxx_path.exists():
        os.environ.setdefault("CXX", str(cxx_path))
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


def _configure_torch_compile_runtime() -> None:
    try:
        import torch._dynamo.config as dynamo_config

        dynamo_config.log_compilation_metrics = False
    except Exception:
        pass
    try:
        import torch._inductor.config as inductor_config

        # PyTorch CUDA 13 / RTX 5090 currently hits an Inductor scheduler
        # assertion in MixOrderReduction for some compiled backward shapes
        # such as S=1024, D=32. Keep Inductor/Triton compile enabled but avoid
        # this one unstable fusion pass unless explicitly re-enabled.
        inductor_config.triton.mix_order_reduction = _env_truthy(
            "STOCKAGENT_INDUCTOR_MIX_ORDER_REDUCTION",
            "0",
        )
    except Exception:
        pass
    try:
        from torch._dynamo.utils import CompileEventLogger

        CompileEventLogger.compilation_metric = staticmethod(lambda *args, **kwargs: None)
    except Exception:
        pass


def _torch_compile_options(mode: object, *, cudagraphs: bool = False) -> dict[str, Any]:
    """Expand a compile mode into explicit options so cudagraph policy stays singular."""
    normalized = str(mode or "default").strip().lower()
    try:
        options = dict(torch._inductor.list_mode_options(normalized))
    except Exception as exc:
        raise ValueError(f"Unsupported torch compile mode: {normalized!r}") from exc
    options["triton.cudagraphs"] = bool(cudagraphs)
    return options


def _distributed_probe_succeeded(local_success: bool, device: torch.device) -> bool:
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return bool(local_success)
    flag_device = device if device.type == "cuda" else torch.device("cpu")
    flag = torch.tensor(1 if local_success else 0, device=flag_device, dtype=torch.int32)
    dist.all_reduce(flag, op=dist.ReduceOp.MIN)
    return bool(int(flag.detach().cpu().item()))


def _distributed_min_int(local_value: int, device: torch.device) -> int:
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return int(local_value)
    value_device = device if device.type == "cuda" else torch.device("cpu")
    value = torch.tensor(int(local_value), device=value_device, dtype=torch.int64)
    dist.all_reduce(value, op=dist.ReduceOp.MIN)
    return int(value.detach().cpu().item())


def _distributed_rank0_decision(local_value: bool, device: torch.device) -> bool:
    """Use rank 0 as the single authority for a conditional collective branch."""
    if not _distributed_is_initialized() or _distributed_world_size() <= 1:
        return bool(local_value)
    value_device = device if device.type == "cuda" else torch.device("cpu")
    flag = torch.tensor(
        1 if (_distributed_is_rank0() and local_value) else 0,
        device=value_device,
        dtype=torch.int32,
    )
    dist.broadcast(flag, src=0)
    return bool(int(flag.detach().cpu().item()))


def _probe_compiled_train_forward(
    model: nn.Module,
    split: WindowedSplitTensors,
    *,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    use_panel_slab: bool,
    return_aux: bool,
    objective: str,
    factor_aug_kwargs: Mapping[str, float] | None,
    direction_weight: float,
    volatility_regime_weight: float,
) -> tuple[bool, str | None]:
    """Materialize the lazy compiled forward/backward before DDP wraps it.

    The probe deliberately executes a real train-mode graph, but it is not a
    training step. Preserve module modes, mutable buffers (for example BatchNorm
    running statistics), gradients, and RNG state so compilation cannot alter
    the initial state seen by the first real epoch.
    """
    module_modes = [(module, bool(module.training)) for module in model.modules()]
    buffer_snapshots: list[tuple[torch.Tensor, torch.Tensor]] = []
    gradient_snapshots: list[
        tuple[nn.Parameter, torch.Tensor | None, torch.Tensor | None]
    ] = []
    local_error: str | None = None
    rng_devices = []
    if device.type == "cuda":
        rng_devices = [device.index if device.index is not None else torch.cuda.current_device()]
    with torch.random.fork_rng(devices=rng_devices, enabled=True):
        try:
            buffer_snapshots = [
                (buffer, buffer.detach().clone(memory_format=torch.preserve_format))
                for buffer in model.buffers()
            ]
            gradient_snapshots = [
                (
                    parameter,
                    parameter.grad,
                    None
                    if parameter.grad is None
                    else parameter.grad.detach().clone(memory_format=torch.preserve_format),
                )
                for parameter in model.parameters()
            ]
            model.train()
            world_size = _distributed_world_size() if _distributed_is_initialized() else 1
            rank = _distributed_rank() if world_size > 1 else 0
            if int(batch_size) % int(world_size) != 0:
                raise ValueError(
                    f"compile probe batch_size={batch_size} is not divisible by world_size={world_size}"
                )
            local_rows = int(batch_size) // int(world_size)
            start = int(rank) * local_rows
            end = start + local_rows
            prepare_device = _windowed_prepare_device(split, device)
            if use_panel_slab:
                batch = split.panel_slab_batch_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                )
                if batch is None:
                    raise RuntimeError(
                        "compiled panel-slab probe could not build the first fixed local shard: "
                        f"rows=[{start},{end})"
                    )
            else:
                batch = split.batch_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                )
            batch = _move_windowed_batch_to_device(batch, device, non_blocking)
            model.zero_grad(set_to_none=True)
            with torch.enable_grad(), _autocast_context(device, amp_dtype):
                if use_panel_slab:
                    output = model(
                        batch["feature_slab"],
                        batch["tradable_mask"],
                        batch.get("symbol_indices"),
                    )
                else:
                    output = _call_model(
                        model,
                        batch["x"],
                        batch["tradable_mask"],
                        return_aux=return_aux,
                        symbol_indices=batch.get("symbol_indices"),
                    )
                weights, aux_outputs = _extract_weights_and_aux(output)
                aux_outputs = _attach_factor_augmented_scores(
                    model=model,
                    aux_outputs=aux_outputs,
                    x=batch.get("x"),
                    tradable_mask=batch["tradable_mask"],
                    aug_kwargs=dict(factor_aug_kwargs or {}),
                )
                if return_aux:
                    _require_training_aux_outputs(
                        objective,
                        aux_outputs,
                        factor_aug_required=bool(factor_aug_kwargs),
                        direction_weight=direction_weight,
                        volatility_regime_weight=volatility_regime_weight,
                    )
                probe_loss = weights.float().square().mean()
                if factor_aug_kwargs and aux_outputs is not None:
                    probe_loss = probe_loss + aux_outputs["aug_score_logits"].float().square().mean()
            probe_loss.backward()
            if device.type == "cuda":
                torch.cuda.synchronize(device)
        except Exception as exc:
            local_error = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                with torch.no_grad():
                    for buffer, saved_value in buffer_snapshots:
                        buffer.copy_(saved_value)
                    for parameter, original_grad, saved_value in gradient_snapshots:
                        if original_grad is None:
                            parameter.grad = None
                        else:
                            original_grad.copy_(saved_value)
                            parameter.grad = original_grad
            except Exception as exc:
                restore_error = f"state restore failed: {type(exc).__name__}: {exc}"
                local_error = restore_error if local_error is None else f"{local_error}; {restore_error}"
            for module, training in module_modes:
                module.training = training

    local_success = local_error is None
    global_success = _distributed_probe_succeeded(local_success, device)
    if global_success:
        return True, None
    if local_error is None:
        local_error = "compile probe failed on another distributed rank"
    return False, local_error


def _probe_compiled_loss_forward_backward(
    loss_fn: Callable[..., torch.Tensor],
    split: WindowedSplitTensors,
    *,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    loss_kwargs: Mapping[str, Any],
    max_volume_participation: float,
    volume_participation_equity: float,
) -> tuple[bool, str | None]:
    """Materialize the real fixed-shape canonical loss graph before epoch 1."""
    local_error: str | None = None
    try:
        prepare_device = _windowed_prepare_device(split, device)
        batch = split.batch_metadata_by_rows(
            0,
            int(batch_size),
            device=prepare_device,
            non_blocking=False,
        )
        batch = _move_windowed_batch_to_device(batch, device, non_blocking)
        weights_dtype = (
            amp_dtype
            if device.type == "cuda" and amp_dtype is not None
            else torch.float32
        )
        weights = torch.zeros_like(
            batch["future_log_returns"],
            device=device,
            dtype=weights_dtype,
            requires_grad=True,
        )
        volume_limit_weights = _volume_limit_weights_from_notional(
            batch.get("volume_notional"),
            max_volume_participation=max_volume_participation,
            volume_participation_equity=volume_participation_equity,
            device=device,
            dtype=weights.dtype,
        )
        aux_outputs = {
            "initial_weights": torch.zeros(
                (split.num_symbols,),
                device=device,
                dtype=torch.float32,
            ),
            "initial_alive": torch.ones((), device=device, dtype=torch.bool),
        }
        with torch.enable_grad(), _autocast_context(device, amp_dtype):
            loss = loss_fn(
                weights,
                batch["future_log_returns"],
                batch["tradable_mask"],
                benchmark_returns=batch["benchmark"],
                can_buy_mask=batch["can_buy_mask"],
                can_sell_mask=batch["can_sell_mask"],
                can_short_open_mask=batch["can_short_open_mask"],
                force_short_cover_mask=batch["force_short_cover_mask"],
                force_exit_mask=batch["force_exit_mask"],
                sample_mask=batch["sample_mask"],
                volume_limit_weights=volume_limit_weights,
                aux_outputs=aux_outputs,
                **dict(loss_kwargs),
            )
            loss.backward()
        if device.type == "cuda":
            torch.cuda.synchronize(device)
    except Exception as exc:
        local_error = f"{type(exc).__name__}: {exc}"
    return local_error is None, local_error


def _can_enable_torch_compile(device: torch.device) -> tuple[bool, str]:
    """Return whether torch.compile is safe to enable in current environment."""
    if device.type != "cuda":
        return False, "torch.compile is only enabled for CUDA in this project"

    _prepend_compile_toolchain_paths()
    _configure_torch_compile_runtime()

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
    _configure_compile_cache_paths(
        torchinductor_cache_dir=getattr(training, "torchinductor_cache_dir", "~/.cache/torchinductor"),
        triton_cache_dir=getattr(training, "triton_cache_dir", "~/.cache/triton"),
        cuda_cache_path=getattr(training, "cuda_cache_path", "~/.cache/nv_cuda"),
        force=True,
    )
    _prepend_compile_toolchain_paths()
    _configure_torch_compile_runtime()
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = "1" if bool(training.backtest_autotune) else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = "1" if bool(training.backtest_compile) else "0"
    os.environ["STOCKAGENT_BACKTEST_COMPILE_STATEFUL"] = (
        "1" if bool(training.backtest_compile_stateful) else "0"
    )
    os.environ["STOCKAGENT_BACKTEST_COMPILE_DYNAMIC"] = (
        "1" if bool(getattr(training, "backtest_compile_dynamic", False)) else "0"
    )
    eval_backtest_compile = getattr(training, "eval_backtest_compile", None)
    if eval_backtest_compile is not None:
        os.environ["STOCKAGENT_EVAL_BACKTEST_COMPILE"] = "1" if bool(eval_backtest_compile) else "0"
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = "1" if bool(training.backtest_verbose) else "0"
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1" if bool(training.strict_no_fallback) else "0"
    os.environ["STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS"] = str(
        max(0, int(training.backtest_checkpoint_chunk_rows))
    )


_BACKTEST_RUNTIME_ENV_NAMES = (
    "STOCKAGENT_BACKTEST_AUTOTUNE",
    "STOCKAGENT_BACKTEST_COMPILE",
    "STOCKAGENT_BACKTEST_COMPILE_STATEFUL",
    "STOCKAGENT_BACKTEST_COMPILE_DYNAMIC",
    "STOCKAGENT_EVAL_BACKTEST_COMPILE",
    "STOCKAGENT_BACKTEST_VERBOSE",
    "STOCKAGENT_STRICT_NO_FALLBACK",
    "STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS",
)


@contextmanager
def _preserve_backtest_runtime_environment():
    """Restore process-wide backtest flags after a public trainer entrypoint exits.

    The snapshot is local to each context invocation, so nested/re-entrant calls
    restore their caller's effective runtime contract rather than clearing it.
    """
    previous = {name: os.environ.get(name) for name in _BACKTEST_RUNTIME_ENV_NAMES}
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _isolate_backtest_runtime_environment(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        with _preserve_backtest_runtime_environment():
            return func(*args, **kwargs)

    return wrapped


def _training_loss_portfolio_activation(config: ExperimentConfig) -> str:
    activation = str(getattr(config.training, "loss_portfolio_activation", "auto")).strip().lower().replace("-", "_")
    if activation in {"", "auto", "trading", "same", "same_as_trading"}:
        return str(config.trading.portfolio_activation)
    return activation


def _training_loss_min_trade_weight(config: ExperimentConfig) -> float:
    configured = getattr(config.training, "loss_min_trade_weight", None)
    if configured is None:
        return max(0.0, float(config.trading.min_trade_weight))
    return max(0.0, float(configured))


def _postprocess_benchmark_script() -> Path:
    return Path(__file__).resolve().parents[2] / "scripts" / "benchmark_postprocess.py"


def _postprocess_benchmark_config_path() -> str:
    return os.environ.get("STOCKAGENT_CONFIG_PATH", "configs/markets/tw.yaml")


def _postprocess_benchmark_metric_name(raw: str) -> str:
    name = str(raw).strip().lower().replace("-", "_")
    aliases = {
        "sharp": "sharpe",
        "cum_return": "cumulative_return",
        "return": "cumulative_return",
        "returns": "cumulative_return",
        "total_return": "cumulative_return",
        "total_returns": "cumulative_return",
    }
    return aliases.get(name, name)


def _postprocess_benchmark_plot_metrics(config: ExperimentConfig, rank_metric: str) -> list[str]:
    raw = str(getattr(config.training, "postprocess_benchmark_plot_metrics", rank_metric))
    metrics: list[str] = []
    for item in raw.split(","):
        metric = _postprocess_benchmark_metric_name(item)
        if not metric:
            continue
        if metric not in metrics:
            metrics.append(metric)
    if rank_metric not in metrics:
        metrics.insert(0, rank_metric)
    return metrics


def _postprocess_benchmark_supported(config: ExperimentConfig) -> tuple[bool, str]:
    model_name = str(getattr(config.training, "model_name", "")).strip()
    if model_name == "transformer_base_portfolio":
        return True, ""
    return False, (
        "postprocess activation/threshold sweep needs raw transformer_base_portfolio scores; "
        f"got model_name={model_name!r}"
    )


def _run_postprocess_benchmark_after_fold(
    *,
    config: ExperimentConfig,
    output_path: Path,
    fold_id: int,
    enabled: bool | None = None,
) -> None:
    if not _distributed_should_write():
        return
    if enabled is None:
        enabled = bool(getattr(config.training, "postprocess_benchmark_after_fold", False))
    if not bool(enabled):
        return
    supported, reason = _postprocess_benchmark_supported(config)
    if not supported:
        message = f"[Fold {fold_id}] postprocess benchmark skipped: {reason}"
        if bool(getattr(config.training, "postprocess_benchmark_strict", False)):
            raise RuntimeError(message)
        print(message)
        return

    script_path = _postprocess_benchmark_script()
    if not script_path.exists():
        message = f"[Fold {fold_id}] postprocess benchmark skipped: missing {script_path}"
        if bool(getattr(config.training, "postprocess_benchmark_strict", False)):
            raise FileNotFoundError(message)
        print(message)
        return

    split = str(getattr(config.training, "postprocess_benchmark_split", "test")).strip().lower()
    rank_metric = _postprocess_benchmark_metric_name(
        str(getattr(config.training, "postprocess_benchmark_rank_metric", "sharpe"))
    )
    plot_metrics = _postprocess_benchmark_plot_metrics(config, rank_metric)
    fold_dir = _fold_dir(output_path, fold_id)
    checkpoint_path = fold_dir / "checkpoint_best.pt"
    if not checkpoint_path.exists():
        message = f"[Fold {fold_id}] postprocess benchmark skipped: missing {checkpoint_path}"
        if bool(getattr(config.training, "postprocess_benchmark_strict", False)):
            raise FileNotFoundError(message)
        print(message)
        return
    output_dir = fold_dir / "postprocess_benchmark"
    best_plot_paths = [output_dir / f"{split}_best_{metric}_equity_curve.png" for metric in plot_metrics]
    json_path = output_dir / f"{split}_activation_threshold_sweep.json"
    has_trained_output_candidate = False
    if json_path.exists():
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
            results = payload.get("results", []) if isinstance(payload, dict) else []
            summary_metrics = summary.get("plot_metrics", [])
            has_trained_output_candidate = bool(summary.get("trained_output_mode")) and any(
                isinstance(row, dict) and row.get("mode") == "trained_output"
                for row in results
            )
            has_trained_output_candidate = has_trained_output_candidate and all(
                metric in summary_metrics for metric in plot_metrics
            )
        except Exception:
            has_trained_output_candidate = False
    if best_plot_paths and all(
        path.exists() and path.stat().st_mtime >= checkpoint_path.stat().st_mtime
        for path in best_plot_paths
    ) and has_trained_output_candidate:
        print(f"[Fold {fold_id}] postprocess benchmark already exists: {output_dir}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / f"{split}_activation_threshold_sweep.log"
    cmd = [
        sys.executable,
        str(script_path),
        "--config",
        _postprocess_benchmark_config_path(),
        "--fold",
        str(int(fold_id)),
        "--split",
        split,
        "--activations",
        str(getattr(config.training, "postprocess_benchmark_activations", "identity,softsign,tanh,isru,erf,atan,gd")),
        "--thresholds",
        str(
            getattr(
                config.training,
                "postprocess_benchmark_thresholds",
                "0,0.0001,0.00025,0.0005,0.001,0.0025,0.005,0.01,0.02",
            )
        ),
        "--rank-metric",
        rank_metric,
        "--plot-metrics",
        ",".join(plot_metrics),
        "--plot-top-n",
        str(int(getattr(config.training, "postprocess_benchmark_plot_top_n", 20))),
        "--output-root",
        str(output_path),
    ]
    max_rows = int(getattr(config.training, "postprocess_benchmark_max_rows", 0))
    if max_rows > 0:
        cmd.extend(["--max-rows", str(max_rows)])
    if bool(getattr(config.training, "postprocess_benchmark_backtest_compile", False)):
        cmd.append("--backtest-compile")

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    started = time.perf_counter()
    print(f"[Fold {fold_id}] running postprocess benchmark for best checkpoint: {output_dir}")
    output_lines: list[str] = []
    with log_path.open("w", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            cmd,
            cwd=Path(__file__).resolve().parents[2],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            output_lines.append(line)
            log_handle.write(line)
            log_handle.flush()
            print(line, end="", flush=True)
        return_code = proc.wait()
    elapsed_s = time.perf_counter() - started
    combined_output = "".join(output_lines)

    if return_code != 0:
        tail = "\n".join(combined_output.strip().splitlines()[-40:])
        message = (
            f"[Fold {fold_id}] postprocess benchmark failed in {elapsed_s:.1f}s "
            f"(exit={return_code}); log={log_path}\n{tail}"
        )
        if bool(getattr(config.training, "postprocess_benchmark_strict", False)):
            raise RuntimeError(message)
        print(message)
        return

    best_payload: dict[str, Any] | None = None
    best_by_metric_payload: dict[str, Any] | None = None
    output_payload: dict[str, Any] | None = None
    for raw_line in combined_output.splitlines():
        line = raw_line.strip()
        if line.startswith("BEST "):
            try:
                parsed = json.loads(line[len("BEST ") :])
                if isinstance(parsed, dict):
                    best_payload = parsed
            except json.JSONDecodeError:
                pass
        elif line.startswith("BEST_BY_METRIC "):
            try:
                parsed = json.loads(line[len("BEST_BY_METRIC ") :])
                if isinstance(parsed, dict):
                    best_by_metric_payload = parsed
            except json.JSONDecodeError:
                pass
        elif line.startswith("{") and "best_equity_curve" in line:
            try:
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    output_payload = parsed
            except json.JSONDecodeError:
                pass

    if best_payload:
        print(
            f"[Fold {fold_id}] postprocess best {rank_metric}: "
            f"activation={best_payload.get('activation')} "
            f"threshold={float(best_payload.get('min_trade_weight', 0.0)):.6g} "
            f"sharpe={float(best_payload.get('sharpe', 0.0)):+.4f} "
            f"cum={float(best_payload.get('cumulative_return', 0.0)):+.4f} "
            f"plots={output_dir}"
        )
        if best_by_metric_payload:
            for metric in plot_metrics:
                row = best_by_metric_payload.get(metric)
                if not isinstance(row, dict):
                    continue
                print(
                    f"[Fold {fold_id}] postprocess best {metric}: "
                    f"activation={row.get('activation')} "
                    f"threshold={float(row.get('min_trade_weight', 0.0)):.6g} "
                    f"sharpe={float(row.get('sharpe', 0.0)):+.4f} "
                    f"sortino={float(row.get('sortino', 0.0)):+.4f} "
                    f"cum={float(row.get('cumulative_return', 0.0)):+.4f}"
                )
    elif output_payload:
        print(f"[Fold {fold_id}] postprocess benchmark outputs: {output_payload}")
    else:
        print(f"[Fold {fold_id}] postprocess benchmark finished in {elapsed_s:.1f}s; log={log_path}")


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
    candidate_top_m = min(max(1, int(cstpm.candidate_k)), num_symbols)
    scorer_hidden = int(cstpm.scorer_hidden)
    scorer_blocks = int(cstpm.scorer_blocks)
    d_model = int(cstpm.d_model)
    heads = int(cstpm.heads)
    layers = int(cstpm.layers)
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


def _normalize_ddp_global_batch_size(
    batch_size: int,
    world_size: int,
    *,
    auto_selected: bool,
) -> int:
    """Resolve one fixed global batch divisible across all DDP ranks.

    A manually requested shape is a user contract and therefore fails instead
    of being silently changed.  Only a VRAM-derived automatic candidate may be
    rounded down.
    """
    requested = int(batch_size)
    ranks = int(world_size)
    if requested <= 0:
        raise ValueError(f"DDP global batch size must be positive; got {requested}")
    if ranks <= 1:
        return requested
    if requested % ranks == 0:
        return requested
    if not auto_selected:
        raise ValueError(
            "DDP global batch size must be divisible by world_size; "
            f"selected_batch_size={requested}, world_size={ranks}. "
            "Set training.batch_size_train to a divisible value."
        )
    normalized = (requested // ranks) * ranks
    if normalized < ranks:
        raise ValueError(
            "DDP requires a global batch-size cap of at least world_size so every rank receives a row; "
            f"selected_cap={requested}, world_size={ranks}. Increase training.batch_size_train or reduce ranks."
        )
    return normalized


def _train_epoch_windowed_tensor(
    model: nn.Module,
    panel_slab_model: nn.Module | None,
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
    return_rank_ic_weight: float = 0.0,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    factor_aug_kwargs: dict[str, float] | None = None,
    lr_scheduler: torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None = None,
    lr_scheduler_interval: str = "epoch",
    profile_timing: bool = False,
    debug_timing_sync: bool = False,
    progress_label: str | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
) -> tuple[torch.Tensor, TimingBreakdown]:
    model.train()
    if panel_slab_model is not None:
        panel_slab_model.train()
    total_rows = len(split)
    if total_rows == 0:
        return torch.zeros((), device=device, dtype=torch.float32), TimingBreakdown()

    num_batches = (total_rows + batch_size - 1) // batch_size
    sequential_return_objective = _is_return_series_objective(objective)
    batch_order = list(range(num_batches)) if sequential_return_objective else torch.randperm(num_batches).tolist()
    panel_forward_model = _panel_forward_module(model)
    model_return_aux = bool(
        _training_needs_aux(
            objective,
            factor_aug_kwargs,
            direction_weight=direction_weight,
            volatility_regime_weight=volatility_regime_weight,
        )
    )
    use_panel_forward = (
        panel_forward_model is not None
        and not bool(factor_aug_kwargs)
        and not model_return_aux
        and (panel_slab_model is not None or split.features.device == device)
    )
    # A zero state is semantically identical to None at a segment boundary, but
    # keeping the value a Tensor from batch 1 avoids separate non-stateful and
    # stateful torch.compile graphs.
    portfolio_prev_weights = torch.zeros(
        (split.num_symbols,),
        device=device,
        dtype=torch.float32,
    )
    portfolio_prev_alive = torch.ones((), device=device, dtype=torch.bool)
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
        _record_debug_cuda_sync(timing, "iter_start_sync_s", device, debug_timing_sync)
        batch_start = time.perf_counter()
        start = batch_idx * batch_size
        end = min(start + batch_size, total_rows)
        if progress_label:
            _progress(f"{progress_label}: batch {step_idx}/{num_batches} gather rows=[{start},{end})")
        _maybe_sync_cuda(device, profile_timing)
        timing.fetch_s += time.perf_counter() - batch_start

        prepare_device = _windowed_prepare_device(split, device)
        batch_prepare_start = time.perf_counter()
        direct_panel_slab = False
        if use_panel_forward:
            batch = None
            if panel_slab_model is not None and model_return_aux is False:
                route_start = time.perf_counter()
                batch = split.panel_slab_batch_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                    prepare_timing=timing,
                )
                timing.prepare_panel_slab_batch_s += time.perf_counter() - route_start
                direct_panel_slab = batch is not None
            if batch is None:
                route_start = time.perf_counter()
                batch = split.batch_metadata_by_rows(
                    start,
                    end,
                    device=prepare_device,
                    non_blocking=False,
                    prepare_timing=timing,
                )
                timing.prepare_metadata_batch_s += time.perf_counter() - route_start
            batch_x = None
        else:
            window_materialize_start = time.perf_counter()
            route_start = time.perf_counter()
            batch = split.batch_by_rows(
                start,
                end,
                device=prepare_device,
                non_blocking=False,
                prepare_timing=timing,
            )
            timing.prepare_materialized_batch_s += time.perf_counter() - route_start
            _maybe_sync_cuda(prepare_device, profile_timing)
            timing.window_materialize_s += time.perf_counter() - window_materialize_start
            batch_x = batch["x"]
        _maybe_sync_cuda(prepare_device, profile_timing)
        batch_prepare_elapsed = time.perf_counter() - batch_prepare_start
        timing.batch_prepare_s += batch_prepare_elapsed
        _record_debug_cuda_sync(timing, "after_prepare_sync_s", device, debug_timing_sync)

        h2d_start = time.perf_counter()
        move_start = time.perf_counter()
        batch = _move_windowed_batch_to_device(batch, device, non_blocking)
        timing.prepare_move_windowed_batch_s += time.perf_counter() - move_start
        if batch_x is not None:
            batch_x = batch["x"]
        batch_ret = batch["future_log_returns"]
        batch_mask = batch["tradable_mask"]
        batch_buy_mask = batch["can_buy_mask"]
        batch_sell_mask = batch["can_sell_mask"]
        batch_bench = batch["benchmark"]
        batch_sample_mask = batch["sample_mask"]
        _maybe_sync_cuda(device, profile_timing)
        h2d_elapsed = time.perf_counter() - h2d_start
        timing.h2d_transfer_s += h2d_elapsed
        timing.transfer_s += batch_prepare_elapsed + h2d_elapsed

        _maybe_cudagraph_step_begin()
        optimizer.zero_grad(set_to_none=True)
        forward_start = time.perf_counter()
        with _autocast_context(device, amp_dtype):
            model_forward_start = time.perf_counter()
            with _cuda_timing(
                timing,
                "model_forward_cuda_s",
                device,
                enabled=profile_timing,
            ):
                if PROFILE_RANGES_ENABLED:
                    with profile_range("train.forward.model"):
                        if use_panel_forward:
                            if panel_forward_model is None:
                                raise RuntimeError("panel forward model unexpectedly unavailable")
                            if direct_panel_slab:
                                if panel_slab_model is None:
                                    raise RuntimeError("panel slab model unexpectedly unavailable")
                                model_output = panel_slab_model(
                                    batch["feature_slab"],
                                    batch_mask,
                                    batch.get("symbol_indices"),
                                )
                            else:
                                model_output = _call_panel_forward_for_batch(
                                    panel_forward_model=panel_forward_model,
                                    panel_slab_model=panel_slab_model,
                                    split=split,
                                    batch=batch,
                                    mask=batch_mask,
                                    device=device,
                                    non_blocking=non_blocking,
                                    return_aux=model_return_aux,
                                )
                        else:
                            if batch_x is None:
                                raise RuntimeError("window tensor batch unexpectedly unavailable")
                            model_output = _call_model(
                                model,
                                batch_x,
                                batch_mask,
                                return_aux=model_return_aux,
                                symbol_indices=batch.get("symbol_indices"),
                            )
                elif use_panel_forward:
                    if panel_forward_model is None:
                        raise RuntimeError("panel forward model unexpectedly unavailable")
                    if direct_panel_slab:
                        if panel_slab_model is None:
                            raise RuntimeError("panel slab model unexpectedly unavailable")
                        model_output = panel_slab_model(
                            batch["feature_slab"],
                            batch_mask,
                            batch.get("symbol_indices"),
                        )
                    else:
                        model_output = _call_panel_forward_for_batch(
                            panel_forward_model=panel_forward_model,
                            panel_slab_model=panel_slab_model,
                            split=split,
                            batch=batch,
                            mask=batch_mask,
                            device=device,
                            non_blocking=non_blocking,
                            return_aux=model_return_aux,
                        )
                else:
                    if batch_x is None:
                        raise RuntimeError("window tensor batch unexpectedly unavailable")
                    model_output = _call_model(
                        model,
                        batch_x,
                        batch_mask,
                        return_aux=model_return_aux,
                        symbol_indices=batch.get("symbol_indices"),
                    )
                weights, aux_outputs = _extract_weights_and_aux(model_output)
            batch_volume_limit_weights = _volume_limit_weights_from_notional(
                batch.get("volume_notional"),
                max_volume_participation=max_volume_participation,
                volume_participation_equity=volume_participation_equity,
                device=device,
                dtype=weights.dtype,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.model_forward_s += time.perf_counter() - model_forward_start

            factor_aug_start = time.perf_counter()
            with _cuda_timing(
                timing,
                "factor_aug_cuda_s",
                device,
                enabled=profile_timing,
            ):
                aux_outputs = _attach_factor_augmented_scores(
                    model=model,
                    aux_outputs=aux_outputs,
                    x=batch_x,
                    tradable_mask=batch_mask,
                    aug_kwargs=factor_aug_kwargs or {},
                )
                if model_return_aux:
                    _require_training_aux_outputs(
                        objective,
                        aux_outputs,
                        factor_aug_required=bool(factor_aug_kwargs),
                        direction_weight=direction_weight,
                        volatility_regime_weight=volatility_regime_weight,
                    )
            _maybe_sync_cuda(device, profile_timing)
            timing.factor_aug_s += time.perf_counter() - factor_aug_start

            if sequential_return_objective:
                aux_outputs = dict(aux_outputs or {})
                aux_outputs["initial_weights"] = portfolio_prev_weights
                aux_outputs["initial_alive"] = portfolio_prev_alive

            _record_debug_cuda_sync(timing, "after_forward_sync_s", device, debug_timing_sync)
            loss_start = time.perf_counter()
            with _cuda_timing(timing, "loss_cuda_s", device, enabled=profile_timing):
                loss_context = profile_range("train.forward.loss") if PROFILE_RANGES_ENABLED else nullcontext()
                with loss_context:
                    loss = loss_fn(
                        weights,
                        batch_ret,
                        batch_mask,
                        benchmark_returns=batch_bench,
                        can_buy_mask=batch_buy_mask,
                        can_sell_mask=batch_sell_mask,
                        can_short_open_mask=batch["can_short_open_mask"],
                        force_short_cover_mask=batch["force_short_cover_mask"],
                        force_exit_mask=batch["force_exit_mask"],
                        sample_mask=batch_sample_mask,
                        long_only=long_only,
                        buy_fee_rate=buy_fee_rate,
                        sell_fee_rate=sell_fee_rate,
                        max_turnover_ratio=max_turnover_ratio,
                        volume_limit_weights=batch_volume_limit_weights,
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
                        return_rank_ic_weight=return_rank_ic_weight,
                        direction_weight=direction_weight,
                        volatility_regime_weight=volatility_regime_weight,
                        concentration_weight=concentration_weight,
                        regime_up_threshold=regime_up_threshold,
                        regime_down_threshold=regime_down_threshold,
                    )
            _maybe_sync_cuda(device, profile_timing)
            timing.loss_s += time.perf_counter() - loss_start
            _record_debug_cuda_sync(timing, "after_loss_sync_s", device, debug_timing_sync)

        should_check_finite = _should_check_finite(
            step_idx,
            finite_check_interval_steps,
            final_step=step_idx >= num_batches,
        )
        if should_check_finite:
            finite_start = time.perf_counter()
            loss_is_finite = _tensor_is_finite(loss)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not loss_is_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
        if sequential_return_objective and aux_outputs is not None:
            next_prev = aux_outputs.get("_final_weights")
            next_alive = aux_outputs.get("_final_alive")
        else:
            next_prev = None
            next_alive = None
        if next_prev is not None:
            state_start = time.perf_counter()
            portfolio_prev_weights = _detach_portfolio_state(next_prev)
            if next_alive is not None:
                portfolio_prev_alive = _detach_portfolio_state(next_alive)
            _maybe_sync_cuda(device, profile_timing)
            timing.portfolio_state_s += time.perf_counter() - state_start
        _maybe_sync_cuda(device, profile_timing)
        timing.forward_s += time.perf_counter() - forward_start

        backward_start = time.perf_counter()
        if scaler.is_enabled():
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device, enabled=profile_timing):
                scaler.scale(loss).backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            gradients_are_finite = True
            gradient_total_norm: torch.Tensor | None = None
            if grad_clip_norm > 0.0:
                gradient_total_norm = _run_gradient_clip_(
                    model,
                    optimizer,
                    scaler,
                    grad_clip_norm=grad_clip_norm,
                    timing=timing,
                    device=device,
                    profile_timing=profile_timing,
                )
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = (
                    _tensor_is_finite(gradient_total_norm)
                    if gradient_total_norm is not None
                    else _model_gradients_are_finite(model)
                )
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device, enabled=profile_timing):
                scaler.step(optimizer)
                scaler.update()
            _stabilize_model_parameters_after_step(model)
            _record_debug_cuda_sync(timing, "after_step_sync_s", device, debug_timing_sync)
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
            if lr_scheduler is not None and lr_scheduler_interval == "step":
                timing.scheduler_s += _step_batch_lr_scheduler(lr_scheduler)
        else:
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device, enabled=profile_timing):
                loss.backward()
            _record_debug_cuda_sync(timing, "after_backward_sync_s", device, debug_timing_sync)
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            gradients_are_finite = True
            gradient_total_norm = None
            if grad_clip_norm > 0.0:
                gradient_total_norm = _run_gradient_clip_(
                    model,
                    optimizer,
                    scaler,
                    grad_clip_norm=grad_clip_norm,
                    timing=timing,
                    device=device,
                    profile_timing=profile_timing,
                )
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = (
                    _tensor_is_finite(gradient_total_norm)
                    if gradient_total_norm is not None
                    else _model_gradients_are_finite(model)
                )
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device, enabled=profile_timing):
                optimizer.step()
            _stabilize_model_parameters_after_step(model)
            _record_debug_cuda_sync(timing, "after_step_sync_s", device, debug_timing_sync)
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
            if lr_scheduler is not None and lr_scheduler_interval == "step":
                timing.scheduler_s += _step_batch_lr_scheduler(lr_scheduler)

        if should_check_finite:
            finite_start = time.perf_counter()
            parameters_are_finite = _model_parameters_are_finite(model)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not parameters_are_finite:
                _raise_if_model_parameters_nonfinite(model, "Model parameters became non-finite after optimizer step")

        _maybe_sync_cuda(device, profile_timing)
        timing.backward_s += time.perf_counter() - backward_start

        total_loss_t.add_(loss.detach().to(dtype=torch.float32))
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


def _train_epoch_windowed_tensor_ddp(
    model: nn.Module,
    loss_fn: Callable[..., torch.Tensor],
    split: WindowedSplitTensors,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    objective: str,
    grad_clip_norm: float,
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
    finite_check_interval_steps: int = 0,
    rank_ic_weight: float = 0.20,
    return_rank_ic_weight: float = 0.0,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    lr_scheduler: torch.optim.lr_scheduler.LRScheduler | torch.optim.lr_scheduler.ReduceLROnPlateau | None = None,
    lr_scheduler_interval: str = "epoch",
    profile_timing: bool = False,
    debug_timing_sync: bool = False,
    progress_label: str | None = None,
    max_volume_participation: float = 0.0,
    volume_participation_equity: float = 1_000_000.0,
    use_panel_slab: bool = False,
) -> tuple[torch.Tensor, TimingBreakdown]:
    if _training_needs_aux(
        objective,
        direction_weight=direction_weight,
        volatility_regime_weight=volatility_regime_weight,
    ):
        raise RuntimeError(
            f"DDP windowed executor cannot discard auxiliary outputs required by objective={objective!r}"
        )
    if not _is_return_series_objective(objective):
        raise RuntimeError("DDP windowed hotpath requires log_utility, sharpe, sortino, or another return-series objective")
    world_size = _distributed_world_size()
    rank = _distributed_rank()
    if world_size <= 1 or not _distributed_is_initialized():
        raise RuntimeError("DDP windowed hotpath requires an initialized distributed process group")
    if int(batch_size) % world_size != 0:
        raise RuntimeError(
            f"DDP global batch_size must be divisible by world_size; got batch_size={batch_size}, world_size={world_size}"
        )
    local_batch_size = int(batch_size) // world_size
    if local_batch_size <= 0:
        raise RuntimeError(f"DDP local batch size must be positive; got {local_batch_size}")

    model.train()
    total_rows = len(split)
    if total_rows == 0:
        return torch.zeros((), device=device, dtype=torch.float32), TimingBreakdown()
    if total_rows % int(batch_size) != 0:
        raise RuntimeError(
            f"DDP windowed split must be padded to the global batch size; rows={total_rows}, batch_size={batch_size}"
        )

    num_batches = total_rows // int(batch_size)
    portfolio_prev_weights = torch.zeros(
        (split.num_symbols,),
        device=device,
        dtype=torch.float32,
    )
    portfolio_prev_alive = torch.ones((), device=device, dtype=torch.bool)
    total_loss_t = torch.zeros((), device=device, dtype=torch.float32)
    steps = 0
    timing = TimingBreakdown()
    total_start = time.perf_counter()
    if progress_label and _distributed_is_rank0():
        _progress(
            f"{progress_label}: train epoch start mode=windowed_ddp rows={total_rows} "
            f"global_batch_size={batch_size} local_batch_size={local_batch_size} "
            f"batches={num_batches} world_size={world_size} objective={objective}"
        )

    prepare_device = _windowed_prepare_device(split, device)
    for step_idx in range(1, num_batches + 1):
        _record_debug_cuda_sync(timing, "iter_start_sync_s", device, debug_timing_sync)
        batch_start = time.perf_counter()
        global_start = (step_idx - 1) * int(batch_size)
        local_start = global_start + rank * local_batch_size
        local_end = local_start + local_batch_size
        if progress_label and _distributed_is_rank0():
            _progress(
                f"{progress_label}: batch {step_idx}/{num_batches} "
                f"global_rows=[{global_start},{global_start + int(batch_size)})"
            )
        _maybe_sync_cuda(device, profile_timing)
        timing.fetch_s += time.perf_counter() - batch_start

        batch_prepare_start = time.perf_counter()
        route_start = time.perf_counter()
        if use_panel_slab:
            batch = split.panel_slab_batch_by_rows(
                local_start,
                local_end,
                device=prepare_device,
                non_blocking=False,
                prepare_timing=timing,
            )
            if batch is None:
                raise RuntimeError(
                    "DDP panel-slab executor requires every local shard to have a fixed-shape slab; "
                    f"rank={rank}, local_rows=[{local_start},{local_end})"
                )
            timing.prepare_panel_slab_batch_s += time.perf_counter() - route_start
        else:
            batch = split.batch_by_rows(
                local_start,
                local_end,
                device=prepare_device,
                non_blocking=False,
                prepare_timing=timing,
            )
            timing.prepare_materialized_batch_s += time.perf_counter() - route_start
        _maybe_sync_cuda(prepare_device, profile_timing)
        batch_prepare_elapsed = time.perf_counter() - batch_prepare_start
        timing.batch_prepare_s += batch_prepare_elapsed
        if not use_panel_slab:
            timing.window_materialize_s += batch_prepare_elapsed
        _record_debug_cuda_sync(timing, "after_prepare_sync_s", device, debug_timing_sync)

        h2d_start = time.perf_counter()
        move_start = time.perf_counter()
        batch = _move_windowed_batch_to_device(batch, device, non_blocking)
        timing.prepare_move_windowed_batch_s += time.perf_counter() - move_start
        batch_x = None if use_panel_slab else batch["x"]
        batch_mask = batch["tradable_mask"]
        _maybe_sync_cuda(device, profile_timing)
        h2d_elapsed = time.perf_counter() - h2d_start
        timing.h2d_transfer_s += h2d_elapsed
        timing.transfer_s += batch_prepare_elapsed + h2d_elapsed

        _maybe_cudagraph_step_begin()
        optimizer.zero_grad(set_to_none=True)
        forward_start = time.perf_counter()
        with _autocast_context(device, amp_dtype):
            model_forward_start = time.perf_counter()
            with _cuda_timing(
                timing,
                "model_forward_cuda_s",
                device,
                enabled=profile_timing,
            ):
                if use_panel_slab:
                    model_output = model(
                        batch["feature_slab"],
                        batch_mask,
                        batch.get("symbol_indices"),
                    )
                else:
                    if batch_x is None:
                        raise RuntimeError("DDP materialized window batch is unavailable")
                    model_output = _call_model(
                        model,
                        batch_x,
                        batch_mask,
                        return_aux=False,
                        symbol_indices=batch.get("symbol_indices"),
                    )
                local_weights, _ = _extract_weights_and_aux(model_output)
            _maybe_sync_cuda(device, profile_timing)
            timing.model_forward_s += time.perf_counter() - model_forward_start

            gather_start = time.perf_counter()
            weights = _all_gather_autograd(local_weights.contiguous())
            future_returns = _all_gather_no_grad(batch["future_log_returns"].contiguous())
            tradable_mask = _all_gather_no_grad(batch["tradable_mask"].contiguous())
            can_buy_mask = _all_gather_no_grad(batch["can_buy_mask"].contiguous())
            can_sell_mask = _all_gather_no_grad(batch["can_sell_mask"].contiguous())
            can_short_open_mask = _all_gather_no_grad(batch["can_short_open_mask"].contiguous())
            force_short_cover_mask = _all_gather_no_grad(batch["force_short_cover_mask"].contiguous())
            force_exit_mask = _all_gather_no_grad(batch["force_exit_mask"].contiguous())
            sample_mask = _all_gather_no_grad(batch["sample_mask"].contiguous())
            benchmark = _all_gather_no_grad(batch["benchmark"].contiguous())
            volume_notional = (
                _all_gather_no_grad(batch["volume_notional"].contiguous())
                if _volume_participation_enabled(max_volume_participation, volume_participation_equity)
                and "volume_notional" in batch
                else None
            )
            volume_limit_weights = _volume_limit_weights_from_notional(
                volume_notional,
                max_volume_participation=max_volume_participation,
                volume_participation_equity=volume_participation_equity,
                device=device,
                dtype=weights.dtype,
            )
            _maybe_sync_cuda(device, profile_timing)
            timing.factor_aug_s += time.perf_counter() - gather_start

            _record_debug_cuda_sync(timing, "after_forward_sync_s", device, debug_timing_sync)
            loss_start = time.perf_counter()
            with _cuda_timing(timing, "loss_cuda_s", device, enabled=profile_timing):
                aux_outputs: dict[str, torch.Tensor] | None = None
                aux_outputs = {
                    "initial_weights": portfolio_prev_weights,
                    "initial_alive": portfolio_prev_alive,
                }
                loss = loss_fn(
                    weights,
                    future_returns,
                    tradable_mask,
                    benchmark_returns=benchmark,
                    can_buy_mask=can_buy_mask,
                    can_sell_mask=can_sell_mask,
                    can_short_open_mask=can_short_open_mask,
                    force_short_cover_mask=force_short_cover_mask,
                    force_exit_mask=force_exit_mask,
                    sample_mask=sample_mask,
                    long_only=long_only,
                    buy_fee_rate=buy_fee_rate,
                    sell_fee_rate=sell_fee_rate,
                    max_turnover_ratio=max_turnover_ratio,
                    volume_limit_weights=volume_limit_weights,
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
                    return_rank_ic_weight=return_rank_ic_weight,
                    direction_weight=direction_weight,
                    volatility_regime_weight=volatility_regime_weight,
                    concentration_weight=concentration_weight,
                    regime_up_threshold=regime_up_threshold,
                    regime_down_threshold=regime_down_threshold,
                )
            _maybe_sync_cuda(device, profile_timing)
            timing.loss_s += time.perf_counter() - loss_start
            _record_debug_cuda_sync(timing, "after_loss_sync_s", device, debug_timing_sync)

        should_check_finite = _should_check_finite(
            step_idx,
            finite_check_interval_steps,
            final_step=step_idx >= num_batches,
        )
        if should_check_finite:
            finite_start = time.perf_counter()
            loss_is_finite = _tensor_is_finite(loss)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not loss_is_finite:
                optimizer.zero_grad(set_to_none=True)
                continue

        next_prev = (aux_outputs or {}).get("_final_weights")
        next_alive = (aux_outputs or {}).get("_final_alive")
        if next_prev is not None:
            state_start = time.perf_counter()
            portfolio_prev_weights = _detach_portfolio_state(next_prev)
            if next_alive is not None:
                portfolio_prev_alive = _detach_portfolio_state(next_alive)
            _maybe_sync_cuda(device, profile_timing)
            timing.portfolio_state_s += time.perf_counter() - state_start
        timing.forward_s += time.perf_counter() - forward_start

        backward_start = time.perf_counter()
        if scaler.is_enabled():
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device, enabled=profile_timing):
                scaler.scale(loss).backward()
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            gradients_are_finite = True
            gradient_total_norm: torch.Tensor | None = None
            if float(grad_clip_norm) > 0.0:
                gradient_total_norm = _run_gradient_clip_(
                    model,
                    optimizer,
                    scaler,
                    grad_clip_norm=grad_clip_norm,
                    timing=timing,
                    device=device,
                    profile_timing=profile_timing,
                )
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = (
                    _tensor_is_finite(gradient_total_norm)
                    if gradient_total_norm is not None
                    else _model_gradients_are_finite(model)
                )
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device, enabled=profile_timing):
                scaler.step(optimizer)
                scaler.update()
            _stabilize_model_parameters_after_step(model)
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
        else:
            grad_start = time.perf_counter()
            with _cuda_timing(timing, "grad_cuda_s", device, enabled=profile_timing):
                loss.backward()
            _record_debug_cuda_sync(timing, "after_backward_sync_s", device, debug_timing_sync)
            _maybe_sync_cuda(device, profile_timing)
            timing.grad_s += time.perf_counter() - grad_start
            gradients_are_finite = True
            gradient_total_norm = None
            if float(grad_clip_norm) > 0.0:
                gradient_total_norm = _run_gradient_clip_(
                    model,
                    optimizer,
                    scaler,
                    grad_clip_norm=grad_clip_norm,
                    timing=timing,
                    device=device,
                    profile_timing=profile_timing,
                )
            if should_check_finite:
                finite_start = time.perf_counter()
                gradients_are_finite = (
                    _tensor_is_finite(gradient_total_norm)
                    if gradient_total_norm is not None
                    else _model_gradients_are_finite(model)
                )
                timing.finite_check_s += time.perf_counter() - finite_start
            if not gradients_are_finite:
                optimizer.zero_grad(set_to_none=True)
                continue
            step_start = time.perf_counter()
            with _cuda_timing(timing, "step_cuda_s", device, enabled=profile_timing):
                optimizer.step()
            _stabilize_model_parameters_after_step(model)
            _record_debug_cuda_sync(timing, "after_step_sync_s", device, debug_timing_sync)
            _maybe_sync_cuda(device, profile_timing)
            timing.step_s += time.perf_counter() - step_start
        if lr_scheduler is not None and lr_scheduler_interval == "step":
            timing.scheduler_s += _step_batch_lr_scheduler(lr_scheduler)

        if should_check_finite:
            finite_start = time.perf_counter()
            parameters_are_finite = _model_parameters_are_finite(model)
            timing.finite_check_s += time.perf_counter() - finite_start
            if not parameters_are_finite:
                _raise_if_model_parameters_nonfinite(model, "Model parameters became non-finite after optimizer step")

        _maybe_sync_cuda(device, profile_timing)
        timing.backward_s += time.perf_counter() - backward_start
        total_loss_t.add_(loss.detach().to(dtype=torch.float32))
        steps += 1
        if progress_label and _distributed_is_rank0():
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
            "benchmark_sharpe": 0.0,
            "sortino": 0.0,
            "benchmark_sortino": 0.0,
            "max_drawdown": 0.0,
            "calmar": 0.0,
            "turnover": 0.0,
            "daily_hit_rate": 0.0,
            "excess_return_vs_benchmark": 0.0,
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
    benchmark_sharpe = float((avg_b / std_b * np.sqrt(252.0)).item()) if float(std_b.item()) > 0 else 0.0
    downside = torch.minimum(r, torch.zeros_like(r))
    downside_b = torch.minimum(b, torch.zeros_like(b))
    downside_dev = torch.sqrt((downside.pow(2)).mean())
    downside_dev_b = torch.sqrt((downside_b.pow(2)).mean())
    sortino = float((avg / downside_dev * np.sqrt(252.0)).item()) if float(downside_dev.item()) > 0 else 0.0
    benchmark_sortino = float((avg_b / downside_dev_b * np.sqrt(252.0)).item()) if float(downside_dev_b.item()) > 0 else 0.0

    cumulative_log = torch.cumsum(r, dim=0)
    cumulative_with_initial = torch.cat(
        (torch.zeros(1, dtype=r.dtype, device=r.device), cumulative_log),
        dim=0,
    )
    running_max_log = torch.cummax(cumulative_with_initial, dim=0).values[1:]
    dd = torch.expm1((cumulative_log - running_max_log).clamp(min=-60.0, max=0.0))
    max_dd = float(dd.min().item()) if dd.numel() else 0.0
    calmar = ann_r / abs(max_dd) if max_dd < 0.0 else 0.0

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "cagr": ann_r,
        "sharpe": sharpe,
        "benchmark_sharpe": benchmark_sharpe,
        "sortino": sortino,
        "benchmark_sortino": benchmark_sortino,
        "max_drawdown": max_dd,
        "calmar": calmar,
        "turnover": float(t.mean().item()) if t.numel() else 0.0,
        "daily_hit_rate": float((r > 0).to(torch.float64).mean().item()),
        "excess_return_vs_benchmark": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def _run_training_tree_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    resume: bool = True,
    deployment_folds: Iterable[WalkForwardFold] | None = None,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    experiment_manifest = _checkpoint_manifest(panel, config)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = list(folds)
    deployment_fold_list = (
        fold_list if deployment_folds is None else list(deployment_folds)
    )
    next_fold_by_id = _next_fold_by_id(deployment_fold_list)

    if resume:
        for fold in fold_list:
            completed = _load_completed_fold_result(
                output_path,
                fold.fold_id,
                expected_manifest=experiment_manifest,
                expected_fold=fold,
            )
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

    for train_years_key, group_folds in tqdm(
        grouped_folds.items(),
        desc="Train groups",
        unit="group",
        disable=not _distributed_is_rank0(),
    ):
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
            feature_names=panel.feature_names,
        )
        if not hasattr(model, "fit"):
            raise TypeError(f"Tree training path expects model.fit(), got {type(model).__name__}")

        print(f"[Train {train_years}] fitting tree model on {int(train_x.size(0))} dates x {panel.num_symbols} symbols")
        model.fit(train_x, train_returns, train_masks)  # type: ignore[attr-defined]

        for fold in pending_folds:
            print(f"[Fold {fold.fold_id}]  val={fold.val_years}  test={fold.test_years}")
            val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
            test_ds = CrossSectionalDataset(
                panel,
                fold.test_indices,
                config.training.lookback,
                allow_empty=True,
            )
            if len(test_ds) == 0:
                print(f"[Fold {fold.fold_id}] skip: empty full test split after lookback filtering")
                continue
            deployment_test_rows = _deployment_test_prefix_rows(
                panel,
                fold,
                next_fold_by_id.get(int(fold.fold_id)),
                config.training.lookback,
                test_ds.valid_indices,
            )

            fold_dir = _fold_dir(output_path, fold.fold_id)
            fold_dir.mkdir(parents=True, exist_ok=True)

            val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _dataset_to_tensors(val_ds)
            val_short_open_masks, val_force_cover_masks, val_force_exit_masks = _dataset_short_rule_masks_to_tensors(val_ds)
            val_volume_notional = _dataset_volume_notional_to_tensor(val_ds)
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
                1.0,
                config.trading.min_trade_weight,
                portfolio_activation=config.trading.portfolio_activation,
                chunk_rows=min(eval_chunk_rows, max(1, int(val_x.size(0)))),
                volume_notional=val_volume_notional,
                can_short_open_mask=val_short_open_masks,
                force_short_cover_mask=val_force_cover_masks,
                force_exit_mask=val_force_exit_masks,
                max_volume_participation=config.trading.max_volume_participation,
                volume_participation_equity=config.trading.volume_participation_equity,
            )
            val_loss = float(
                _evaluated_backtest_loss(
                    val_bt_t,
                    val_returns,
                    val_masks,
                    val_buy_masks,
                    val_sell_masks,
                    val_short_open_masks,
                    val_force_cover_masks,
                    val_force_exit_masks,
                    val_bench,
                    val_volume_notional,
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
            test_short_open_masks, test_force_cover_masks, test_force_exit_masks = _dataset_short_rule_masks_to_tensors(test_ds)
            test_volume_notional = _dataset_volume_notional_to_tensor(test_ds)
            test_bt_t, test_ic, test_met = _evaluate_tensor_batch(
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
                1.0,
                config.trading.min_trade_weight,
                portfolio_activation=config.trading.portfolio_activation,
                chunk_rows=min(eval_chunk_rows, max(1, int(test_x.size(0)))),
                volume_notional=test_volume_notional,
                can_short_open_mask=test_short_open_masks,
                force_short_cover_mask=test_force_cover_masks,
                force_exit_mask=test_force_exit_masks,
                max_volume_participation=config.trading.max_volume_participation,
                volume_participation_equity=config.trading.volume_participation_equity,
            )
            test_eval_indices = np.asarray(test_ds.valid_indices, dtype=np.int64)

            test_dates = panel.dates[test_eval_indices]
            test_close_prices = panel.close_prices[test_eval_indices]
            test_daily_volumes = None if panel.daily_volumes is None else panel.daily_volumes[test_eval_indices]
            test_bt = test_bt_t.to_numpy()
            deployment_test_bt = _prefix_backtest_result(test_bt, deployment_test_rows)
            deployment_test_dates = test_dates[:deployment_test_rows]
            write_integer_holdings_table = _save_integer_share_holdings_table_enabled(config)
            test_integer_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt.weights_history,
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                can_buy_mask=test_buy_masks.detach().cpu().numpy(),
                can_sell_mask=test_sell_masks.detach().cpu().numpy(),
                can_short_open_mask=test_short_open_masks.detach().cpu().numpy(),
                force_short_cover_mask=test_force_cover_masks.detach().cpu().numpy(),
                force_exit_mask=test_force_exit_masks.detach().cpu().numpy(),
                benchmark_returns=test_bt.benchmark_returns,
                initial_capital=1_000_000.0,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                long_only=config.trading.long_only,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                max_volume_participation=config.trading.max_volume_participation,
                gross_leverage=1.0,
                min_trade_weight=config.trading.min_trade_weight,
                portfolio_activation=config.trading.portfolio_activation,
                close_prices=test_close_prices,
                daily_volumes=test_daily_volumes,
                symbols=panel.symbols,
                dates=test_dates,
                collect_holdings=write_integer_holdings_table,
            )
            test_integer_met = compute_metrics(test_integer_bt)

            objective_key = _objective_metric_key(loss_objective)
            val_objective_metric = float(val_met.get(objective_key, float("nan")))
            test_objective_metric = float(test_met.get(objective_key, float("nan")))
            print(f"\n  [val]   IC={val_ic['ic_mean']:+.4f}  IC_IR={val_ic['ic_ir']:+.4f}  {loss_objective}={val_objective_metric:+.4f}  cum_ret={val_met['cumulative_return']:+.4f}  excess={val_met['excess_return_vs_benchmark']:+.4f}")
            print(f"  [test]  IC={test_ic['ic_mean']:+.4f}  IC_IR={test_ic['ic_ir']:+.4f}  {loss_objective}={test_objective_metric:+.4f}  cum_ret={test_met['cumulative_return']:+.4f}  excess={test_met['excess_return_vs_benchmark']:+.4f}")

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
            _save_tree_checkpoint_metadata(
                _best_checkpoint_path(fold_dir),
                fold=fold,
                best_val_loss=val_loss,
                experiment_manifest=experiment_manifest,
            )
            with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
                json.dump(asdict(fold_result), f, indent=2)

            _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
            table_output_format = str(getattr(config.training, "table_output_format", "csv"))
            _save_daily_portfolio_returns_table(
                fold_dir / "daily_portfolio_returns",
                test_dates,
                test_bt.strategy_returns,
                test_bt.benchmark_returns,
                test_bt.turnovers,
                table_output_format=table_output_format,
            )
            _maybe_save_daily_weights_table(
                fold_dir / "daily_weights.csv",
                test_dates,
                panel.symbols,
                test_bt.weights_history,
                enabled=bool(config.training.save_daily_weights_table),
                table_output_format=table_output_format,
            )
            report = generate_annual_report(test_bt, test_dates)
            print("\n" + report)
            with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
                f.write(report)
            _save_deployment_test_artifacts(
                fold_dir,
                deployment_test_bt,
                deployment_test_dates,
                backtest_artifact_compression=str(
                    getattr(config.training, "backtest_artifact_compression", "none")
                ),
            )

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
                write_daily_weights_table=bool(config.training.save_integer_share_daily_weights_table),
                write_holdings_table=write_integer_holdings_table,
                table_output_format=table_output_format,
            )
            _write_fold_complete_marker(fold_dir, fold_result, source="tree_training_final")

            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


def _run_inference_tree_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    deployment_folds: Iterable[WalkForwardFold] | None = None,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    experiment_manifest = _checkpoint_manifest(panel, config)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = sorted(list(folds), key=lambda item: item.fold_id)
    deployment_fold_list = (
        fold_list if deployment_folds is None else list(deployment_folds)
    )
    next_fold_by_id = _next_fold_by_id(deployment_fold_list)

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

        checkpoint_path = _best_checkpoint_path(fold_dir)
        if checkpoint_path.exists():
            checkpoint = _load_checkpoint(checkpoint_path)
            _validate_checkpoint_manifest(
                checkpoint,
                experiment_manifest,
                checkpoint_path=checkpoint_path,
                scope="inference",
                expected_fold=fold,
            )
        else:
            print(
                f"[Fold {fold.fold_id}] legacy tree model has no semantic fingerprint: "
                f"{model_path}"
            )

        with model_path.open("rb") as model_file:
            model = pickle.load(model_file)

        val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
        test_ds = CrossSectionalDataset(
            panel,
            fold.test_indices,
            config.training.lookback,
            allow_empty=True,
        )
        if len(test_ds) == 0:
            print(f"[Fold {fold.fold_id}] skip: empty full test split after lookback filtering")
            continue
        deployment_test_rows = _deployment_test_prefix_rows(
            panel,
            fold,
            next_fold_by_id.get(int(fold.fold_id)),
            config.training.lookback,
            test_ds.valid_indices,
        )

        val_x, val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _dataset_to_tensors(val_ds)
        val_short_open_masks, val_force_cover_masks, val_force_exit_masks = _dataset_short_rule_masks_to_tensors(val_ds)
        val_volume_notional = _dataset_volume_notional_to_tensor(val_ds)
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
            1.0,
            config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            chunk_rows=val_chunk_rows,
            volume_notional=val_volume_notional,
            can_short_open_mask=val_short_open_masks,
            force_short_cover_mask=val_force_cover_masks,
            force_exit_mask=val_force_exit_masks,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
        )
        val_loss = float(
            _evaluated_backtest_loss(
                val_bt_t,
                val_returns,
                val_masks,
                val_buy_masks,
                val_sell_masks,
                val_short_open_masks,
                val_force_cover_masks,
                val_force_exit_masks,
                val_bench,
                val_volume_notional,
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
        test_short_open_masks, test_force_cover_masks, test_force_exit_masks = _dataset_short_rule_masks_to_tensors(test_ds)
        test_volume_notional = _dataset_volume_notional_to_tensor(test_ds)
        test_chunk_rows = max(1, min(2048, int(test_x.size(0))))
        test_bt_t, test_ic, test_met = _evaluate_tensor_batch(
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
            1.0,
            config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            chunk_rows=test_chunk_rows,
            volume_notional=test_volume_notional,
            can_short_open_mask=test_short_open_masks,
            force_short_cover_mask=test_force_cover_masks,
            force_exit_mask=test_force_exit_masks,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
        )
        test_eval_indices = np.asarray(test_ds.valid_indices, dtype=np.int64)

        test_dates = panel.dates[test_eval_indices]
        test_close_prices = panel.close_prices[test_eval_indices]
        test_daily_volumes = None if panel.daily_volumes is None else panel.daily_volumes[test_eval_indices]
        test_bt = test_bt_t.to_numpy()
        deployment_test_bt = _prefix_backtest_result(test_bt, deployment_test_rows)
        deployment_test_dates = test_dates[:deployment_test_rows]
        write_integer_holdings_table = bool(config.training.save_integer_share_holdings_table)
        test_integer_bt, holdings_records = run_backtest_integer_shares(
            weights=test_bt.weights_history,
            future_returns=test_returns.detach().cpu().numpy(),
            tradable_mask=test_masks.detach().cpu().numpy(),
            can_buy_mask=test_buy_masks.detach().cpu().numpy(),
            can_sell_mask=test_sell_masks.detach().cpu().numpy(),
            can_short_open_mask=test_short_open_masks.detach().cpu().numpy(),
            force_short_cover_mask=test_force_cover_masks.detach().cpu().numpy(),
            force_exit_mask=test_force_exit_masks.detach().cpu().numpy(),
            benchmark_returns=test_bt.benchmark_returns,
            initial_capital=1_000_000.0,
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            max_volume_participation=config.trading.max_volume_participation,
            gross_leverage=1.0,
            min_trade_weight=config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            close_prices=test_close_prices,
            daily_volumes=test_daily_volumes,
            symbols=panel.symbols,
            dates=test_dates,
            collect_holdings=write_integer_holdings_table,
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
        table_output_format = str(getattr(config.training, "table_output_format", "csv"))
        _save_daily_portfolio_returns_table(
            fold_dir / "daily_portfolio_returns",
            test_dates,
            test_bt.strategy_returns,
            test_bt.benchmark_returns,
            test_bt.turnovers,
            table_output_format=table_output_format,
        )
        _maybe_save_daily_weights_table(
            fold_dir / "daily_weights.csv",
            test_dates,
            panel.symbols,
            test_bt.weights_history,
            enabled=bool(config.training.save_daily_weights_table),
            table_output_format=table_output_format,
        )
        report = generate_annual_report(test_bt, test_dates)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)
        _save_deployment_test_artifacts(
            fold_dir,
            deployment_test_bt,
            deployment_test_dates,
            backtest_artifact_compression=str(
                getattr(config.training, "backtest_artifact_compression", "none")
            ),
        )

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
            write_daily_weights_table=bool(config.training.save_integer_share_daily_weights_table),
            write_holdings_table=write_integer_holdings_table,
            table_output_format=table_output_format,
        )
        _write_fold_complete_marker(fold_dir, fold_result, source="tree_inference_final")

    if results_by_fold:
        _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


def _run_inference_neural_models(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    deployment_folds: Iterable[WalkForwardFold] | None = None,
) -> list[FoldResult]:
    _configure_backtest_runtime_from_config(config)
    experiment_manifest = _checkpoint_manifest(panel, config)
    if getattr(config.training, "inference_backtest_autotune", None) is not None:
        os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = (
            "1" if bool(config.training.inference_backtest_autotune) else "0"
        )
    if getattr(config.training, "inference_backtest_compile", None) is not None:
        os.environ["STOCKAGENT_BACKTEST_COMPILE"] = (
            "1" if bool(config.training.inference_backtest_compile) else "0"
        )
    device = _resolve_device(config)
    non_blocking = config.training.non_blocking_transfer and device.type == "cuda"
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    loss_objective = _normalize_risk_objective(config.training.loss_type)
    include_volume_notional = _volume_participation_enabled(
        config.trading.max_volume_participation,
        config.trading.volume_participation_equity,
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = sorted(list(folds), key=lambda item: item.fold_id)
    deployment_fold_list = (
        fold_list if deployment_folds is None else list(deployment_folds)
    )
    next_fold_by_id = _next_fold_by_id(deployment_fold_list)

    print(f"[inference] model={config.training.model_name} folds={len(fold_list)} device={device}")
    for fold in tqdm(fold_list, desc="Inference folds", unit="fold"):
        fold_dir = _fold_dir(output_path, fold.fold_id)
        model_file = _model_path(fold_dir)
        best_checkpoint_file = _best_checkpoint_path(fold_dir)

        model_state_dict: dict | None = None
        best_val_loss = float("inf")

        if best_checkpoint_file.exists():
            checkpoint = _load_checkpoint(best_checkpoint_file)
            _validate_checkpoint_manifest(
                checkpoint,
                experiment_manifest,
                checkpoint_path=best_checkpoint_file,
                scope="inference",
                expected_fold=fold,
            )
            model_state_dict = checkpoint.get("model_state_dict")
            best_val_loss = float(checkpoint.get("best_val_loss", float("inf")))
        elif model_file.exists():
            print(
                f"[Fold {fold.fold_id}] legacy model artifact has no semantic fingerprint: {model_file}"
            )
            model_state_dict = torch.load(model_file, map_location="cpu")
        else:
            print(f"[Fold {fold.fold_id}] skip: missing {model_file.name} and {best_checkpoint_file.name}")
            continue

        if not isinstance(model_state_dict, dict):
            print(f"[Fold {fold.fold_id}] skip: invalid model state format")
            continue

        fold_panel = _align_panel_to_state_dict_universe(
            panel,
            fold_dir,
            model_state_dict,
            context=f"inference fold {fold.fold_id}",
        )
        model = build_model(
            config=config,
            lookback=config.training.lookback,
            num_features=len(fold_panel.feature_names),
            num_symbols=fold_panel.num_symbols,
            feature_names=fold_panel.feature_names,
        ).to(device)
        _load_state_dict(model, model_state_dict)
        panel_slab_model: nn.Module | None = None
        if _model_supports_panel_slab_forward(model):
            panel_slab_model = _PanelSlabForwardWrapper(model)
            print(f"[Fold {fold.fold_id}] inference mode=windowed panel_slab_forward=eager")
        else:
            print(f"[Fold {fold.fold_id}] inference mode=windowed panel_forward=fallback")

        val_ds = CrossSectionalDataset(
            fold_panel,
            fold.val_indices,
            config.training.lookback,
            include_volume_notional=include_volume_notional,
        )
        test_ds = CrossSectionalDataset(
            fold_panel,
            fold.test_indices,
            config.training.lookback,
            allow_empty=True,
            include_volume_notional=include_volume_notional,
        )
        if len(test_ds) == 0:
            print(f"[Fold {fold.fold_id}] skip: empty full test split after lookback filtering")
            continue
        deployment_test_rows = _deployment_test_prefix_rows(
            fold_panel,
            fold,
            next_fold_by_id.get(int(fold.fold_id)),
            config.training.lookback,
            test_ds.valid_indices,
        )

        val_windowed = _prepare_windowed_split(
            dataset_to_windowed_tensors(val_ds),
            device,
            non_blocking,
            name=f"inference fold {fold.fold_id} validation windowed tensors",
        )
        val_chunk_rows = _resolve_inference_model_chunk_rows(config, len(val_windowed))
        val_backtest_chunk_rows = _resolve_inference_backtest_chunk_rows(
            config,
            val_chunk_rows,
            len(val_windowed),
        )
        print(
            f"[Fold {fold.fold_id}] inference val chunks: "
            f"rows={len(val_windowed)} model_chunk_rows={val_chunk_rows} "
            f"backtest_chunk_rows={val_backtest_chunk_rows}"
        )

        val_bt_t, val_ic, _ = _evaluate_windowed_tensor_batch(
            model,
            panel_slab_model,
            val_windowed,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            1.0,
            config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            chunk_rows=val_chunk_rows,
            backtest_chunk_rows=val_backtest_chunk_rows,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
        )

        val_returns, val_masks, val_buy_masks, val_sell_masks, val_bench = _windowed_targets_to_tensors(
            val_windowed,
            device=device,
            non_blocking=non_blocking,
        )
        val_rule_idx = val_windowed.valid_indices.to(
            device=val_windowed.force_exit_mask.device,
            dtype=torch.long,
        )
        val_short_open_masks = val_windowed.can_short_open_mask[val_rule_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        val_force_cover_masks = val_windowed.force_short_cover_mask[val_rule_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        val_force_exit_masks = val_windowed.force_exit_mask[val_rule_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        val_volume_notional = (
            None
            if val_windowed.volume_notional is None
            else val_windowed.volume_notional[val_rule_idx].to(device=device, non_blocking=non_blocking)
        )
        if not np.isfinite(best_val_loss):
            best_val_loss = float(
                _evaluated_backtest_loss(
                    val_bt_t,
                    val_returns,
                    val_masks,
                    val_buy_masks,
                    val_sell_masks,
                    val_short_open_masks,
                    val_force_cover_masks,
                    val_force_exit_masks,
                    val_bench,
                    val_volume_notional,
                    config,
                    loss_objective,
                ).detach().cpu()
            )

        val_met = _compute_metrics_from_tensors(
            val_bt_t.strategy_returns,
            val_bt_t.benchmark_returns,
            val_bt_t.turnovers,
        )

        test_windowed = _prepare_windowed_split(
            dataset_to_windowed_tensors(test_ds),
            device,
            non_blocking,
            shared_base=val_windowed,
            name=f"inference fold {fold.fold_id} test windowed tensors",
        )
        test_chunk_rows = _resolve_inference_model_chunk_rows(config, len(test_windowed))
        test_backtest_chunk_rows = _resolve_inference_backtest_chunk_rows(
            config,
            test_chunk_rows,
            len(test_windowed),
        )
        print(
            f"[Fold {fold.fold_id}] inference test chunks: "
            f"rows={len(test_windowed)} model_chunk_rows={test_chunk_rows} "
            f"backtest_chunk_rows={test_backtest_chunk_rows}"
        )

        test_bt_t, test_ic, test_met = _evaluate_windowed_tensor_batch(
            model,
            panel_slab_model,
            test_windowed,
            device,
            amp_dtype,
            non_blocking,
            config.trading.long_only,
            config.trading.buy_fee_rate,
            config.trading.sell_fee_rate,
            config.trading.max_turnover_ratio,
            1.0,
            config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            chunk_rows=test_chunk_rows,
            backtest_chunk_rows=test_backtest_chunk_rows,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
        )
        test_returns, test_masks, test_buy_masks, test_sell_masks, test_bench = _windowed_targets_to_tensors(
            test_windowed,
            device=device,
            non_blocking=non_blocking,
        )
        test_date_idx = test_windowed.valid_indices.to(
            device=test_windowed.can_short_open_mask.device,
            dtype=torch.long,
        )
        test_short_open_masks = test_windowed.can_short_open_mask[test_date_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        test_force_cover_masks = test_windowed.force_short_cover_mask[test_date_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        test_force_exit_masks = test_windowed.force_exit_mask[test_date_idx].to(
            device=device,
            non_blocking=non_blocking,
        )
        test_valid_indices = _windowed_valid_indices_numpy(test_windowed)
        test_eval_indices = test_valid_indices

        test_dates = fold_panel.dates[test_eval_indices]
        test_close_prices = fold_panel.close_prices[test_eval_indices]
        test_daily_volumes = None if fold_panel.daily_volumes is None else fold_panel.daily_volumes[test_eval_indices]
        test_bt = test_bt_t.to_numpy()
        deployment_test_bt = _prefix_backtest_result(test_bt, deployment_test_rows)
        deployment_test_dates = test_dates[:deployment_test_rows]
        write_integer_holdings_table = bool(config.training.save_integer_share_holdings_table)
        test_integer_bt, holdings_records = run_backtest_integer_shares(
            weights=test_bt.weights_history,
            future_returns=test_returns.detach().cpu().numpy(),
            tradable_mask=test_masks.detach().cpu().numpy(),
            can_buy_mask=test_buy_masks.detach().cpu().numpy(),
            can_sell_mask=test_sell_masks.detach().cpu().numpy(),
            can_short_open_mask=test_short_open_masks.detach().cpu().numpy(),
            force_short_cover_mask=test_force_cover_masks.detach().cpu().numpy(),
            force_exit_mask=test_force_exit_masks.detach().cpu().numpy(),
            benchmark_returns=test_bt.benchmark_returns,
            initial_capital=1_000_000.0,
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            max_volume_participation=config.trading.max_volume_participation,
            gross_leverage=1.0,
            min_trade_weight=config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            close_prices=test_close_prices,
            daily_volumes=test_daily_volumes,
            symbols=fold_panel.symbols,
            dates=test_dates,
            collect_holdings=write_integer_holdings_table,
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
        table_output_format = str(getattr(config.training, "table_output_format", "csv"))
        _save_daily_portfolio_returns_table(
            fold_dir / "daily_portfolio_returns",
            test_dates,
            test_bt.strategy_returns,
            test_bt.benchmark_returns,
            test_bt.turnovers,
            table_output_format=table_output_format,
        )
        _maybe_save_daily_weights_table(
            fold_dir / "daily_weights.csv",
            test_dates,
            fold_panel.symbols,
            test_bt.weights_history,
            enabled=bool(config.training.save_daily_weights_table),
            table_output_format=table_output_format,
        )
        report = generate_annual_report(test_bt, test_dates)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)
        _save_deployment_test_artifacts(
            fold_dir,
            deployment_test_bt,
            deployment_test_dates,
            backtest_artifact_compression=str(
                getattr(config.training, "backtest_artifact_compression", "none")
            ),
        )

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
            fold_panel.symbols,
            holdings_records,
            write_daily_weights_table=bool(config.training.save_integer_share_daily_weights_table),
            write_holdings_table=write_integer_holdings_table,
            table_output_format=table_output_format,
        )
        _write_fold_complete_marker(fold_dir, fold_result, source="neural_inference_final")

    if results_by_fold:
        _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]


@_isolate_backtest_runtime_environment
def run_inference(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    deployment_folds: Iterable[WalkForwardFold] | None = None,
) -> list[FoldResult]:
    if _is_tree_model_name(config.training.model_name):
        return _run_inference_tree_models(
            panel,
            folds,
            config,
            output_dir,
            deployment_folds=deployment_folds,
        )
    return _run_inference_neural_models(
        panel,
        folds,
        config,
        output_dir,
        deployment_folds=deployment_folds,
    )


@_isolate_backtest_runtime_environment
def run_training(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    resume: bool = True,
    profile_timing: bool = False,
    deployment_folds: Iterable[WalkForwardFold] | None = None,
) -> list[FoldResult]:
    fold_list_for_run = list(folds)
    deployment_fold_list_for_run = (
        fold_list_for_run
        if deployment_folds is None
        else list(deployment_folds)
    )
    multi_gpu_strategy = str(
        getattr(config.training, "multi_gpu_strategy", "none") or "none"
    ).strip().lower().replace("-", "_")
    distributed_data_parallel_enabled = multi_gpu_strategy in {
        "distributed_data_parallel",
        "ddp",
        "distributed",
        "torch_ddp",
    }
    if (
        resume
        and distributed_data_parallel_enabled
        and _distributed_world_size() > 1
    ):
        migration_device = _resolve_device(config)
        if migration_device.type != "cuda":
            raise RuntimeError(
                "training.multi_gpu_strategy=distributed_data_parallel requires "
                "environment.device=cuda"
            )
        local_rank = int(os.environ.get("LOCAL_RANK", "0"))
        if local_rank < 0 or local_rank >= torch.cuda.device_count():
            raise RuntimeError(
                f"LOCAL_RANK={local_rank} is unavailable; "
                f"visible CUDA device_count={torch.cuda.device_count()}"
            )
        torch.cuda.set_device(local_rank)
        if not _distributed_is_initialized():
            dist.init_process_group(backend="nccl")

    if resume:
        if _distributed_is_initialized() and _distributed_world_size() > 1:
            candidate_ids_payload: list[list[int] | None] = [
                [
                    int(fold.fold_id)
                    for fold in fold_list_for_run
                    if _fold_needs_artifact_scope_migration(
                        _fold_dir(Path(output_dir), fold.fold_id)
                    )
                ]
                if _distributed_is_rank0()
                else None
            ]
            dist.broadcast_object_list(candidate_ids_payload, src=0)
            candidate_ids = candidate_ids_payload[0]
            if candidate_ids is None:
                raise RuntimeError("Rank 0 did not broadcast artifact migration candidates")
            folds_by_id = {int(fold.fold_id): fold for fold in fold_list_for_run}
            unknown_ids = sorted(set(candidate_ids) - set(folds_by_id))
            if unknown_ids:
                raise RuntimeError(
                    "Rank 0 broadcast unknown artifact migration folds: "
                    f"{unknown_ids}"
                )
            migration_candidates = [folds_by_id[fold_id] for fold_id in candidate_ids]
        else:
            migration_candidates = [
                fold
                for fold in fold_list_for_run
                if _fold_needs_artifact_scope_migration(
                    _fold_dir(Path(output_dir), fold.fold_id)
                )
            ]
        if migration_candidates:

            def _migrate_legacy_fold_artifacts() -> None:
                migration_rng_device = (
                    None if _is_tree_model_name(config.training.model_name)
                    else _resolve_device(config)
                )
                try:
                    with _preserve_process_rng_state(migration_rng_device):
                        next_fold_by_id = _next_fold_by_id(deployment_fold_list_for_run)
                        inference_migrations: list[WalkForwardFold] = []
                        for fold in migration_candidates:
                            upgraded = _upgrade_full_horizon_artifacts_without_inference(
                                panel=panel,
                                fold=fold,
                                next_fold=next_fold_by_id.get(int(fold.fold_id)),
                                config=config,
                                output_path=Path(output_dir),
                            )
                            if upgraded:
                                print(
                                    f"[resume] fold {fold.fold_id} artifacts upgraded to "
                                    "full_horizon + stitched_deployment without model inference"
                                )
                            else:
                                inference_migrations.append(fold)
                        if inference_migrations:
                            fold_ids = [int(fold.fold_id) for fold in inference_migrations]
                            print(
                                "[resume] rebuilding legacy fold artifacts by inference: "
                                f"folds={fold_ids}"
                            )
                            run_inference(
                                panel,
                                inference_migrations,
                                config,
                                output_dir,
                                deployment_folds=deployment_fold_list_for_run,
                            )
                        incomplete_fold_ids = [
                            int(fold.fold_id)
                            for fold in migration_candidates
                            if not _has_completed_fold_marker(
                                _fold_dir(Path(output_dir), fold.fold_id)
                            )
                        ]
                        if incomplete_fold_ids:
                            raise RuntimeError(
                                "Artifact scope migration did not produce complete v2 "
                                f"artifacts for folds={incomplete_fold_ids}"
                            )
                finally:
                    if migration_rng_device is not None:
                        _release_cuda_memory(migration_rng_device)

            _run_rank0_store_synchronized_phase(
                "legacy_fold_artifact_scope_migration",
                _migrate_legacy_fold_artifacts,
            )

    if _is_tree_model_name(config.training.model_name):
        return _run_training_tree_models(
            panel,
            fold_list_for_run,
            config,
            output_dir,
            resume=resume,
            deployment_folds=deployment_fold_list_for_run,
        )

    folds = fold_list_for_run
    deployment_folds = deployment_fold_list_for_run

    loss_objective = _normalize_risk_objective(config.training.loss_type)
    factor_loss_kwargs = _factor_loss_kwargs(config)
    portfolio_autoencoder_loss_kwargs = _portfolio_autoencoder_loss_kwargs(config)
    loss_portfolio_activation = _training_loss_portfolio_activation(config)
    loss_min_trade_weight = _training_loss_min_trade_weight(config)
    risk_loss_kwargs = {**factor_loss_kwargs, **portfolio_autoencoder_loss_kwargs}
    risk_loss_kwargs["min_trade_weight"] = loss_min_trade_weight
    risk_loss_kwargs["portfolio_activation"] = loss_portfolio_activation
    risk_loss_kwargs["net_exposure_weight"] = config.training.multitask_loss.net_exposure_weight
    factor_aug_kwargs = _factor_augmentation_kwargs(config, loss_objective)
    if factor_aug_kwargs and _distributed_is_rank0():
        print(
            "[loss-contract] factor consistency augmentation scope=train_only; "
            "validation/test use the deterministic factor objective"
        )

    if multi_gpu_strategy in {"data_parallel", "dp"}:
        raise ValueError(
            "training.multi_gpu_strategy=data_parallel was removed because it duplicated the neural executor "
            "and disabled the panel-slab fast path; use 'distributed_data_parallel' (torchrun) or 'none'"
        )
    if distributed_data_parallel_enabled and _training_needs_aux(
        loss_objective,
        factor_aug_kwargs,
        direction_weight=config.training.multitask_loss.direction_weight,
        volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
    ):
        raise ValueError(
            "distributed_data_parallel supports canonical return-series objectives without auxiliary model "
            f"outputs; objective={loss_objective!r} requires aux tensors. Use multi_gpu_strategy=none "
            "or choose log_utility/sharpe/sortino."
        )

    device = _resolve_device(config)
    if distributed_data_parallel_enabled and not _distributed_is_initialized():
        backend = "nccl" if device.type == "cuda" else "gloo"
        dist.init_process_group(backend=backend)
    if distributed_data_parallel_enabled:
        _resolve_distributed_data_parallel(config, device)
        local_rank = int(os.environ.get("LOCAL_RANK", "0"))
        device = torch.device(f"cuda:{local_rank}")
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
        f"[runtime] device={device} "
        f"cuda_available={torch.cuda.is_available()} "
        f"num_gpus={torch.cuda.device_count()}"
    )
    if distributed_data_parallel_enabled:
        print(
            "[multi-gpu] strategy=distributed_data_parallel "
            f"rank={_distributed_rank()}/{_distributed_world_size()} "
            f"local_rank={os.environ.get('LOCAL_RANK', '0')} "
            f"device={device} "
            f"device_name={torch.cuda.get_device_name(device) if device.type == 'cuda' else 'cpu'}"
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
    experiment_manifest = _checkpoint_manifest(panel, config)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = list(folds)
    deployment_fold_list = (
        fold_list if deployment_folds is None else list(deployment_folds)
    )
    next_fold_by_id = _next_fold_by_id(deployment_fold_list)

    retrain_completed_folds = _env_truthy("STOCKAGENT_RETRAIN_COMPLETED_FOLDS", "0")
    resume_artifact_error: BaseException | None = None
    if resume and not retrain_completed_folds:
        for fold in fold_list:
            completed = _load_completed_fold_result(
                output_path,
                fold.fold_id,
                expected_manifest=experiment_manifest,
                expected_fold=fold,
            )
            if completed is not None:
                results_by_fold[fold.fold_id] = completed
        if results_by_fold:
            try:
                _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))
            except BaseException as exc:
                resume_artifact_error = exc
    elif resume and retrain_completed_folds:
        print("[resume] retrain_completed_folds=true: ignoring completed fold markers")
    _raise_if_distributed_phase_failed(
        "resume_walkforward_artifacts",
        resume_artifact_error,
    )

    grouped_folds: dict[tuple[int, ...], list[WalkForwardFold]] = {}
    for fold in fold_list:
        grouped_folds.setdefault(_group_key(fold.train_years), []).append(fold)

    latest_resume_checkpoint: Path | None = None
    latest_resume_years: list[int] | None = None
    if resume:
        latest_candidate = _latest_group_checkpoint(output_path)
        if latest_candidate is not None:
            latest_resume_checkpoint, latest_resume_years = latest_candidate
            print(f"[resume] latest group checkpoint: {latest_resume_checkpoint}")

    warm_start_checkpoint_path: Path | None = None

    for train_years_key, group_folds in tqdm(grouped_folds.items(), desc="Train groups", unit="group"):
        train_years = list(train_years_key)
        group_checkpoint_path = _group_checkpoint_path(output_path, train_years)
        group_curve_path = _group_curve_path(output_path, train_years)
        loss_contract_error: BaseException | None = None
        try:
            if _distributed_should_write():
                _write_loss_contract_metadata(
                    group_checkpoint_path.parent / "loss_contract.json",
                    objective=loss_objective,
                    return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
                    direction_weight=config.training.multitask_loss.direction_weight,
                    volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                    factor_aug_kwargs=factor_aug_kwargs,
                )
        except BaseException as exc:
            loss_contract_error = exc
        _raise_if_distributed_phase_failed(
            "loss_contract_metadata",
            loss_contract_error,
        )
        pending_folds = [fold for fold in group_folds if fold.fold_id not in results_by_fold]

        # Ensure each train-group starts from a clean CUDA allocator state.
        if device.type == "cuda":
            _release_cuda_memory(device)

        if not pending_folds:
            if config.training.warm_start_from_previous_fold and group_checkpoint_path.exists():
                warm_start_checkpoint_path = group_checkpoint_path
            print(f"[Train {train_years}] already completed, skipping")
            completed_postprocess_error: BaseException | None = None
            try:
                for fold in group_folds:
                    if fold.fold_id in results_by_fold:
                        _run_postprocess_benchmark_after_fold(
                            config=config,
                            output_path=output_path,
                            fold_id=fold.fold_id,
                            enabled=bool(getattr(config.training, "postprocess_benchmark_after_fold", False)),
                        )
            except BaseException as exc:
                completed_postprocess_error = exc
            _raise_if_distributed_phase_failed(
                f"completed_group_{_group_id(train_years)}_postprocess",
                completed_postprocess_error,
            )
            continue

        print(f"\n{'='*80}")
        print(f"[Train {train_years}] folds={len(group_folds)} pending={len(pending_folds)}")
        print(f"{'='*80}")

        train_reference = group_folds[0]
        include_volume_notional = _volume_participation_enabled(
            config.trading.max_volume_participation,
            config.trading.volume_participation_equity,
        )
        train_ds = CrossSectionalDataset(
            panel,
            train_reference.train_indices,
            config.training.lookback,
            include_volume_notional=include_volume_notional,
        )
        min_batch_size = max(1, config.training.min_batch_size)
        train_batch_used_bytes = 0

        if config.training.auto_batch_size and device.type == "cuda":
            estimation_model = build_model(
                config=config,
                lookback=config.training.lookback,
                num_features=len(panel.feature_names),
                num_symbols=panel.num_symbols,
                feature_names=panel.feature_names,
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

        if distributed_data_parallel_enabled:
            pre_ddp_batch_size = int(train_batch_size)
            auto_selected_batch = bool(config.training.auto_batch_size and device.type == "cuda")
            if auto_selected_batch:
                train_batch_size = _distributed_min_int(pre_ddp_batch_size, device)
                if train_batch_size != pre_ddp_batch_size:
                    print(
                        f"[Train {train_years}] DDP auto batch cross-rank minimum "
                        f"{pre_ddp_batch_size}->{train_batch_size}"
                    )
            train_batch_size = _normalize_ddp_global_batch_size(
                train_batch_size,
                _distributed_world_size(),
                auto_selected=auto_selected_batch,
            )
            if train_batch_size != pre_ddp_batch_size:
                if auto_selected_batch:
                    train_batch_used_bytes = train_static_bytes + train_batch_size * train_sample_bytes
                print(
                    f"[Train {train_years}] DDP auto batch normalized "
                    f"{pre_ddp_batch_size}->{train_batch_size} for world_size={_distributed_world_size()}"
                )

        print(f"[Train {train_years}] using batch_size train={train_batch_size}")
        train_setup_start = time.perf_counter()
        # All neural objectives share one lazy-windowed input executor.
        # Objective-specific behavior belongs in the canonical loss.
        _progress(f"[Train {train_years}] setup train tensors: lazy windowed split")
        train_windowed = dataset_to_windowed_tensors(train_ds)
        train_windowed = _maybe_compact_train_windowed_symbols(
            train_windowed,
            config,
            label=f"Train {train_years}",
        )
        train_windowed = _prepare_windowed_split(
            train_windowed,
            device,
            non_blocking,
            name=f"train windowed tensors {train_years}",
        )
        combined_val_windowed: WindowedSplitTensors | None = None
        combined_test_windowed: WindowedSplitTensors | None = None
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.train_tensors",
                TimingBreakdown(total_s=time.perf_counter() - train_setup_start),
            )

        fold_contexts: dict[int, FoldRuntimeContext] = {}
        for fold in pending_folds:
            print(f"[Fold {fold.fold_id}]  val={fold.val_years}  test={fold.test_years}")
            val_ds = CrossSectionalDataset(
                panel,
                fold.val_indices,
                config.training.lookback,
                include_volume_notional=include_volume_notional,
            )
            test_ds = CrossSectionalDataset(
                panel,
                fold.test_indices,
                config.training.lookback,
                allow_empty=True,
                include_volume_notional=include_volume_notional,
            )

            if len(test_ds) == 0:
                print(f"[Fold {fold.fold_id}] skip: empty full test split after lookback filtering")
                continue
            deployment_test_rows = _deployment_test_prefix_rows(
                panel,
                fold,
                next_fold_by_id.get(int(fold.fold_id)),
                config.training.lookback,
                test_ds.valid_indices,
            )

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
                deployment_test_rows=deployment_test_rows,
                checkpoint_best_path=checkpoint_best_path,
            )

            if resume and checkpoint_best_path.exists():
                checkpoint = _load_checkpoint(checkpoint_best_path)
                _validate_checkpoint_manifest(
                    checkpoint,
                    experiment_manifest,
                    checkpoint_path=checkpoint_best_path,
                    scope="resume",
                    expected_fold=fold,
                )
                fold_contexts[fold.fold_id].best_val_loss = float(
                    checkpoint.get("best_val_loss", float("inf"))
                )

        if not fold_contexts:
            train_windowed = None
            combined_val_windowed = None
            combined_test_windowed = None
            if device.type == "cuda":
                _release_cuda_memory(device)
            continue

        val_datasets = [context.val_ds for context in fold_contexts.values()]
        val_setup_start = time.perf_counter()
        _progress(f"[Train {train_years}] setup validation tensors: lazy windowed split")
        combined_val_windowed, val_lengths = _combine_datasets_to_windowed(val_datasets)
        combined_val_windowed = _prepare_windowed_split(
            combined_val_windowed,
            device,
            non_blocking,
            shared_base=train_windowed,
            name=f"validation windowed tensors {train_years}",
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
        curve_test_ds = CrossSectionalDataset(
            panel,
            curve_test_indices,
            config.training.lookback,
            allow_empty=True,
            include_volume_notional=include_volume_notional,
        )
        if len(curve_test_ds) == 0 and curve_test_indices.size != curve_test_fold_context.fold.test_indices.size:
            curve_test_indices = curve_test_fold_context.fold.test_indices
            curve_test_years = curve_test_fold_context.fold.test_years
            curve_test_ds = curve_test_fold_context.test_ds
        test_datasets = [curve_test_ds]
        test_setup_start = time.perf_counter()
        _progress(f"[Train {train_years}] setup test tensors: lazy windowed split")
        combined_test_windowed, test_lengths = _combine_datasets_to_windowed(test_datasets)
        combined_test_windowed = _prepare_windowed_split(
            combined_test_windowed,
            device,
            non_blocking,
            # Validation is the canonical full-universe eval base. The train
            # split may be symbol-compacted, while validation and test always
            # share the same immutable panel tensors.
            shared_base=combined_val_windowed,
            name=f"test windowed tensors {train_years}",
        )
        curve_test_start_row = 0
        curve_test_end_row = int(test_lengths[0])
        curve_test_offsets = [0, curve_test_end_row - curve_test_start_row]
        curve_test_scope = "sampled_first_fold_first_test_year"
        curve_test_fold_id = int(curve_test_fold_context.fold.fold_id)
        curve_test_rows = int(curve_test_offsets[-1])
        print(
            f"[Train {train_years}] epoch-level test loss uses fold "
            f"{curve_test_fold_id} only "
            f"(scope={curve_test_scope}, years={curve_test_years}, rows={curve_test_rows})"
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
            feature_names=panel.feature_names,
        ).to(device)
        batch_coupled_modules = _batch_coupled_training_module_names(model)
        if _model_supports_panel_slab_forward(model):
            filtered_train_rows = len(train_windowed)
            train_windowed = _densify_windowed_training_split_for_panel_slab(
                train_windowed,
                train_ds.date_indices,
            )
            restored_train_rows = len(train_windowed) - filtered_train_rows
            if restored_train_rows:
                print(
                    f"[Train {train_years}] restored {restored_train_rows} filtered in-split date row(s) "
                    "as sample-masked panel-slab rows"
                )
        train_remainder = len(train_windowed) % max(1, int(train_batch_size))
        train_windowed = _prepare_training_split_batch_shape(
            train_windowed,
            model,
            batch_size=train_batch_size,
            ddp_enabled=bool(distributed_data_parallel_enabled),
        )
        if batch_coupled_modules and train_remainder:
            print(
                f"[Train {train_years}] keeping ragged final batch rows={train_remainder} "
                "because train-mode BatchNorm is batch-coupled; valid rows are not duplicated or dropped"
            )
        elif not batch_coupled_modules and train_remainder:
            print(
                f"[Train {train_years}] duplicate-padded {train_batch_size - train_remainder} tail row(s) "
                "for a fixed compile shape; sample_mask excludes them from every loss term"
            )
        if profile_timing:
            _log_timing(
                f"Train {train_years} setup.model_build",
                TimingBreakdown(total_s=time.perf_counter() - model_build_start),
            )

        if config.training.warm_start_from_previous_fold and warm_start_checkpoint_path is not None and warm_start_checkpoint_path.exists():
            warm_start_checkpoint = _load_checkpoint(warm_start_checkpoint_path)
            _validate_checkpoint_manifest(
                warm_start_checkpoint,
                experiment_manifest,
                checkpoint_path=warm_start_checkpoint_path,
                scope="model",
            )
            if "model_state_dict" in warm_start_checkpoint:
                _load_state_dict(model, warm_start_checkpoint["model_state_dict"])
                print(f"[Train {train_years}] warm-started from {warm_start_checkpoint_path.name}")

        compiled_train_model: nn.Module = model
        eval_model: nn.Module = model
        panel_slab_model: nn.Module | None = None
        panel_slab_compile_status = "off"
        ddp_enabled = bool(distributed_data_parallel_enabled)
        ddp_panel_slab_enabled = bool(ddp_enabled and _model_supports_panel_slab_forward(model))
        if _model_supports_panel_slab_forward(model):
            panel_slab_model = _PanelSlabForwardWrapper(model)
            panel_slab_compile_status = "eager"
        if ddp_enabled:
            print(
                f"[Train {train_years}] multi_gpu=DistributedDataParallel "
                f"rank={_distributed_rank()}/{_distributed_world_size()} "
                f"local_batch={max(1, int(train_batch_size) // max(1, _distributed_world_size()))}; "
                "wrapper is applied after torch.compile setup; "
                f"executor={'fixed_shape_panel_slab' if ddp_panel_slab_enabled else 'fixed_shape_materialized_guard'}"
            )
            if not ddp_panel_slab_enabled:
                panel_slab_compile_status = "unavailable:model_has_no_panel_slab"
                print(
                    f"[Train {train_years}] DDP panel-slab unavailable for model={type(model).__name__}; "
                    "using the single guarded materialized-window executor"
                )
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
        train_steps_per_epoch = _estimate_train_steps_per_epoch(
            train_windowed=train_windowed,
            train_batch_size=train_batch_size,
        )
        scheduler, scheduler_name, scheduler_requires_metric, scheduler_step_interval = _create_lr_scheduler(
            optimizer,
            config,
            steps_per_epoch=train_steps_per_epoch,
        )
        if scheduler is not None:
            print(
                f"[Train {train_years}] lr_scheduler={scheduler_name} "
                f"interval={scheduler_step_interval} steps_per_epoch={train_steps_per_epoch}"
            )

        start_epoch = 1
        resume_no_improve_epochs = 0
        resume_no_improve_source: str | None = None
        resume_rng_state: Mapping[str, Any] | None = None
        resume_checkpoint_path = group_checkpoint_path
        if (
            resume
            and latest_resume_checkpoint is not None
            and latest_resume_years is not None
            and latest_resume_checkpoint.exists()
            and latest_resume_checkpoint != group_checkpoint_path
            and tuple(latest_resume_years) == train_years_key
        ):
            resume_checkpoint_path = latest_resume_checkpoint

        if resume and resume_checkpoint_path.exists():
            checkpoint = _load_checkpoint(resume_checkpoint_path)
            _validate_checkpoint_manifest(
                checkpoint,
                experiment_manifest,
                checkpoint_path=resume_checkpoint_path,
                scope="resume",
                expected_train_years=train_years,
            )
            _validate_checkpoint_effective_train_batch_size(
                checkpoint,
                effective_train_batch_size=train_batch_size,
                checkpoint_path=resume_checkpoint_path,
            )
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
                resume_rng_state = checkpoint.get("rng_state")
                start_epoch = int(checkpoint.get("epoch", 0)) + 1
                resume_no_improve_epochs, resume_no_improve_source = _resume_no_improve_epochs_from_checkpoint(
                    checkpoint,
                    group_curve_path,
                )
                print(
                    f"[Train {train_years}] resumed from epoch {start_epoch} "
                    f"(checkpoint={resume_checkpoint_path})"
                )

        if start_epoch > config.training.epochs:
            print(f"[Train {train_years}] checkpoint already reached epoch {config.training.epochs}; evaluating only")

        record_epoch_curve = bool(getattr(config.training, "record_epoch_curve", True))
        if record_epoch_curve:
            curve_trim_error: BaseException | None = None
            try:
                if _distributed_should_write():
                    _trim_group_curve(group_curve_path, start_epoch)
            except BaseException as exc:
                curve_trim_error = exc
            _raise_if_distributed_phase_failed(
                "resume_epoch_curve_trim",
                curve_trim_error,
            )
            curve_plotter: _AsyncEpochCurvePlotter | None = (
                _AsyncEpochCurvePlotter(
                    group_curve_path,
                    interval=int(config.training.curve_plot_interval),
                    async_enabled=bool(config.training.curve_plot_async),
                )
                if _distributed_should_write()
                else None
            )
        else:
            curve_plotter = None
            print(f"[Train {train_years}] epoch curve recording disabled")
        run_epoch_test_curve = bool(getattr(config.training, "epoch_test_curve", True))
        defer_epoch_curve_plot_until_end = bool(
            getattr(config.training, "defer_epoch_curve_plot_until_end", True)
        )
        if not run_epoch_test_curve:
            print(f"[Train {train_years}] epoch-level test curve disabled; final test runs after training")
        if defer_epoch_curve_plot_until_end:
            print(f"[Train {train_years}] epoch curve plotting deferred until training completes")

        def _record_epoch_curve(payload: dict[str, float | int | None], request_plot: bool) -> float:
            if not record_epoch_curve:
                return 0.0
            start_t = time.perf_counter()
            curve_error: BaseException | None = None
            try:
                _append_group_curve(group_curve_path, payload)
                if request_plot and not defer_epoch_curve_plot_until_end and curve_plotter is not None:
                    curve_plotter.request()
            except BaseException as exc:
                curve_error = exc
            _raise_if_distributed_phase_failed(
                f"epoch_{int(payload.get('epoch', 0))}_curve_record",
                curve_error,
            )
            return time.perf_counter() - start_t

        curve_plot_request_interval = max(1, int(config.training.curve_plot_interval))
        combined_val_rows = len(combined_val_windowed)
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
            probe_rows = min(combined_val_rows, max(1, min(train_batch_size, 256)))
            probe_batch = combined_val_windowed.batch_by_rows(
                0,
                probe_rows,
                device=device,
                non_blocking=non_blocking,
            )
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
            print(f"[Train {train_years}] eval chunk_rows={eval_chunk_rows} (auto)")
        if str(eval_model_chunk_rows_config).strip().lower() not in {"", "auto"}:
            eval_chunk_rows = max(1, min(int(eval_model_chunk_rows_config), combined_val_rows))
            print(f"[Train {train_years}] eval model_chunk_rows={eval_chunk_rows} (manual eval_model_chunk_rows)")

        eval_backtest_chunk_rows_config = int(getattr(config.training, "eval_backtest_chunk_rows", 512))
        if bool(getattr(config.training, "eval_backtest_chunk_rows_auto", True)):
            eval_backtest_chunk_rows = _auto_backtest_chunk_rows(
                total_rows=combined_val_rows,
                model_chunk_rows=eval_chunk_rows,
                configured_cap=eval_backtest_chunk_rows_config,
            )
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
        model_compile_probe_status = "not_run"
        compile_setup_error: str | None = None
        loss_compile_status = "eager"
        config_compile_loss = bool(getattr(config.training, "compile_loss", auto_compile_risk))
        default_compile_loss = "1" if config_compile_loss else "0"
        compile_loss = _env_truthy("STOCKAGENT_COMPILE_LOSS", default_compile_loss)
        aux_training_required = _training_needs_aux(
            loss_objective,
            factor_aug_kwargs,
            direction_weight=config.training.multitask_loss.direction_weight,
            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
        )
        full_objective_eval_required = _requires_full_objective_evaluation(
            loss_objective,
            return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
            direction_weight=config.training.multitask_loss.direction_weight,
            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
        )
        eval_risk_loss_kwargs = dict(risk_loss_kwargs)
        if factor_aug_kwargs:
            # Input augmentation is stochastic regularization.  Validation and
            # test report the deterministic factor objective and explicitly
            # exclude this train-only consistency component.
            eval_risk_loss_kwargs["factor_consistency_weight"] = 0.0
        train_uses_panel_slab = panel_slab_model is not None and not aux_training_required

        if should_enable_compile:
            can_compile, reason = _can_enable_torch_compile(device)
            if can_compile:
                try:
                    compile_start = time.perf_counter()
                    compile_mode = str(getattr(config.training, "torch_compile_mode", "default") or "default")
                    model_compile_options = _torch_compile_options(compile_mode, cudagraphs=False)
                    print(f"[Train {train_years}] compile warmup may take extra time at epoch 0")
                    compile_source = f"auto({loss_objective})" if (auto_compile_risk and not config.training.enable_torch_compile) else "config"
                    if train_uses_panel_slab:
                        try:
                            panel_slab_model = torch.compile(
                                panel_slab_model,
                                fullgraph=True,
                                dynamic=False,
                                options=model_compile_options,
                            )
                            panel_slab_compile_status = "compiled:fullgraph:cudagraphs_false"
                            model_compile_status = f"enabled:panel_slab_fullgraph:{compile_source}"
                            print(
                                f"[Train {train_years}] torch.compile panel-slab forward enabled "
                                f"(mode={compile_mode}, fullgraph=True, dynamic=False, cudagraphs=False)"
                            )
                        except Exception as e:
                            compile_setup_error = f"panel-slab compile constructor: {type(e).__name__}: {e}"
                            if bool(config.training.strict_no_fallback) and not ddp_enabled:
                                raise RuntimeError(
                                    f"[Train {train_years}] torch.compile panel-slab forward failed; "
                                    "strict_no_fallback=true so eager slab forward fallback is disabled."
                                ) from e
                            panel_slab_compile_status = "fallback:eager"
                            model_compile_status = "fallback:eager_panel_slab"
                            print(
                                f"[Train {train_years}] torch.compile panel-slab forward failed, "
                                f"falling back to eager slab forward: {e}"
                            )
                        if ddp_panel_slab_enabled:
                            compiled_train_model = panel_slab_model
                            eval_model = model
                    elif ddp_enabled:
                        compiled_train_model = torch.compile(
                            model,
                            fullgraph=True,
                            dynamic=False,
                            options=model_compile_options,
                        )
                        eval_model = compiled_train_model
                        print(
                            f"[Train {train_years}] torch.compile model-before-DDP enabled "
                            f"(mode={compile_mode}, fullgraph=True, dynamic=False, cudagraphs=False, "
                            f"source={compile_source}, {reason})"
                        )
                        model_compile_status = f"enabled:model_before_ddp_fullgraph:{compile_source}"
                    else:
                        compiled_train_model = torch.compile(
                            model,
                            fullgraph=not aux_training_required,
                            dynamic=False,
                            options=model_compile_options,
                        )
                        eval_model = compiled_train_model
                        print(
                            f"[Train {train_years}] torch.compile enabled "
                            f"(mode={compile_mode}, fullgraph={not aux_training_required}, dynamic=False, "
                            f"cudagraphs=False, source={compile_source}, {reason})"
                        )
                        model_compile_status = f"enabled:{compile_source}"

                    if compile_loss:
                        try:
                            eager_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                            loss_fullgraph = _is_log_utility_objective(loss_objective)
                            raw_compiled_loss_fn = torch.compile(
                                eager_loss_fn,
                                fullgraph=loss_fullgraph,
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
                                f"(mode=default, fullgraph={loss_fullgraph}, "
                                "dynamic=False, cudagraphs=False)"
                            )
                            loss_compile_status = "enabled:fullgraph" if loss_fullgraph else "enabled"
                        except Exception as e:
                            compile_setup_error = f"loss compile constructor: {type(e).__name__}: {e}"
                            if bool(config.training.strict_no_fallback) and not ddp_enabled:
                                raise RuntimeError(
                                    f"[Train {train_years}] torch.compile loss failed; "
                                    "strict_no_fallback=true so eager loss fallback is disabled."
                                ) from e
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
                    compile_setup_error = f"model compile constructor: {type(e).__name__}: {e}"
                    if bool(config.training.strict_no_fallback) and not ddp_enabled:
                        raise RuntimeError(
                            f"[Train {train_years}] torch.compile failed; "
                            "strict_no_fallback=true so eager model fallback is disabled."
                        ) from e
                    # A constructor can fail after an earlier wrapper was
                    # assigned.  Reset every executable reference together so
                    # status and behavior cannot diverge.
                    compiled_train_model = model
                    eval_model = model
                    if _model_supports_panel_slab_forward(model):
                        panel_slab_model = _PanelSlabForwardWrapper(model)
                        panel_slab_compile_status = "eager"
                    else:
                        panel_slab_model = None
                        panel_slab_compile_status = "off"
                    compiled_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                    model_compile_status = "fallback:eager"
                    loss_compile_status = "eager"
                    print(f"[Train {train_years}] torch.compile failed, falling back to eager: {e}")
            else:
                compile_setup_error = f"compile unavailable: {reason}"
                if bool(config.training.strict_no_fallback) and not ddp_enabled:
                    raise RuntimeError(
                        f"[Train {train_years}] torch.compile requested but unavailable: {reason}. "
                        "strict_no_fallback=true so eager model fallback is disabled."
                    )
                model_compile_status = f"skipped:{reason}"
                loss_compile_status = "eager"
                print(f"[Train {train_years}] torch.compile skipped: {reason}")
        elif compile_loss:
            loss_compile_status = "off:model_compile_disabled"
        else:
            loss_compile_status = "off:eager"

        if ddp_enabled and should_enable_compile:
            local_compile_setup_ok = model_compile_status.startswith("enabled") and (
                not compile_loss or loss_compile_status.startswith("enabled")
            )
            distributed_compile_setup_ok = _distributed_probe_succeeded(local_compile_setup_ok, device)
            if not distributed_compile_setup_ok:
                if compile_setup_error is None:
                    compile_setup_error = "compile constructor failed or was unavailable on another rank"
                if bool(config.training.strict_no_fallback):
                    raise RuntimeError(
                        f"[Train {train_years}] distributed compile setup failed before probe: "
                        f"{compile_setup_error}. strict_no_fallback=true, so every rank aborts consistently."
                    )
                model_compile_status = "fallback:eager_after_distributed_setup"
                model_compile_probe_status = "skipped:distributed_setup_failed"
                eval_model = model
                if train_uses_panel_slab:
                    panel_slab_model = _PanelSlabForwardWrapper(model)
                    panel_slab_compile_status = "fallback:eager_after_distributed_setup"
                    compiled_train_model = panel_slab_model if ddp_panel_slab_enabled else model
                else:
                    compiled_train_model = model
                compiled_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                loss_compile_status = "fallback:eager_after_distributed_setup"
                print(
                    f"[Train {train_years}] distributed compile setup failed on at least one rank; "
                    f"all ranks use eager before probe/DDP wrap: {compile_setup_error}"
                )

        requested_train_feature_cache_dtype = _resolve_train_feature_cache_dtype(
            config,
            device,
            amp_dtype,
            train_uses_panel_slab=train_uses_panel_slab,
            has_categorical_features=bool(
                getattr(_unwrap_model(model), "categorical_feature_indices", ())
            ),
        )

        # Cache before probing so torch.compile validates the exact dtype/device
        # representation that the epoch executor will receive. An opted-in AMP
        # feature cache is a strict semantic contract: never silently fall back
        # to FP32 or let DDP ranks specialize different graphs.
        _release_cuda_memory(device)
        train_cache_error: BaseException | None = None
        try:
            train_windowed = _maybe_cache_windowed_split_on_device(
                name=f"train windowed tensors {train_years}",
                split=train_windowed,
                device=device,
                enabled=bool(config.training.cache_train_tensors_on_gpu),
                target_fraction=float(config.training.target_vram_fraction),
                safety_margin_gb=float(config.training.vram_safety_margin_gb),
                feature_dtype=requested_train_feature_cache_dtype,
            )
        except Exception as exc:
            # Every DDP rank must reach the phase decision below. Raising here
            # would strand successful peers in their next collective.
            train_cache_error = exc
        effective_train_feature_dtype = train_windowed.features.dtype
        effective_train_feature_on_device = _tensor_on_requested_device(
            train_windowed.features,
            device,
        )
        if requested_train_feature_cache_dtype is not None and (
            not effective_train_feature_on_device
            or effective_train_feature_dtype != requested_train_feature_cache_dtype
        ):
            train_cache_error = RuntimeError(
                "cache_train_features_in_amp_dtype could not be honored; "
                f"requested={requested_train_feature_cache_dtype} on {device}, "
                f"effective={effective_train_feature_dtype} on {train_windowed.features.device}. "
                "Reduce the panel/universe or disable the option explicitly."
            )
        _raise_if_distributed_phase_failed(
            f"train_feature_cache_{_group_id(train_years)}",
            train_cache_error,
        )
        dtype_code = {
            torch.float32: 0,
            torch.float16: 1,
            torch.bfloat16: 2,
        }.get(effective_train_feature_dtype, 99)
        representation_code = int(dtype_code) + (10 if effective_train_feature_on_device else 0)
        min_representation_code = _distributed_min_int(representation_code, device)
        max_representation_code = -_distributed_min_int(-representation_code, device)
        if min_representation_code != max_representation_code:
            raise RuntimeError(
                "DDP ranks selected inconsistent train feature cache representations: "
                f"local={effective_train_feature_dtype}@{train_windowed.features.device}"
            )

        if model_compile_status.startswith("enabled"):
            probe_uses_panel_slab = bool(train_uses_panel_slab and panel_slab_model is not None)
            probe_model = panel_slab_model if probe_uses_panel_slab else compiled_train_model
            if probe_model is None:
                raise RuntimeError("compiled train probe selected an unavailable model")
            probe_ok, probe_error = _probe_compiled_train_forward(
                probe_model,
                train_windowed,
                batch_size=train_batch_size,
                device=device,
                amp_dtype=amp_dtype,
                non_blocking=non_blocking,
                use_panel_slab=probe_uses_panel_slab,
                return_aux=aux_training_required,
                objective=loss_objective,
                factor_aug_kwargs=factor_aug_kwargs,
                direction_weight=config.training.multitask_loss.direction_weight,
                volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
            )
            if probe_ok:
                model_compile_probe_status = "passed:forward_backward"
                print(
                    f"[Train {train_years}] compiled train probe passed "
                    f"(executor={'panel_slab' if probe_uses_panel_slab else 'materialized'}, "
                    "grad_enabled=true, backward=true)"
                )
            else:
                model_compile_probe_status = f"failed:{probe_error}"
                if bool(config.training.strict_no_fallback):
                    raise RuntimeError(
                        f"[Train {train_years}] compiled train probe failed before DDP wrap: {probe_error}. "
                        "strict_no_fallback=true so eager model fallback is disabled."
                    )
                model_compile_status = "fallback:eager_after_probe"
                eval_model = model
                if probe_uses_panel_slab:
                    panel_slab_model = _PanelSlabForwardWrapper(model)
                    panel_slab_compile_status = "fallback:eager_after_probe"
                    compiled_train_model = panel_slab_model if ddp_panel_slab_enabled else model
                else:
                    compiled_train_model = model
                print(
                    f"[Train {train_years}] compiled train probe failed on at least one rank; "
                    f"all ranks will use eager before DDP wrap: {probe_error}"
                )

        if loss_compile_status.startswith("enabled") and not aux_training_required:
            loss_probe_kwargs = {
                "long_only": config.trading.long_only,
                "buy_fee_rate": config.trading.buy_fee_rate,
                "sell_fee_rate": config.trading.sell_fee_rate,
                "max_turnover_ratio": config.trading.max_turnover_ratio,
                "gross_leverage": 1.0,
                "gamma_sharpe": config.evaluation.gamma_sharpe,
                "gamma_excess": config.evaluation.gamma_excess,
                "gamma_cvar": config.evaluation.gamma_cvar,
                "cvar_alpha": config.evaluation.cvar_alpha,
                "gamma_drawdown": config.evaluation.gamma_drawdown,
                "drawdown_target": config.evaluation.drawdown_target,
                "gamma_turnover": config.evaluation.gamma_turnover,
                "gamma_underperformance": config.evaluation.gamma_underperformance,
                "excess_target": config.evaluation.excess_target,
                "cvar_budget": config.evaluation.cvar_budget,
                "drawdown_budget": config.evaluation.drawdown_budget,
                "turnover_budget": config.evaluation.turnover_budget,
                "gamma_cvar_budget": config.evaluation.gamma_cvar_budget,
                "gamma_drawdown_budget": config.evaluation.gamma_drawdown_budget,
                "gamma_turnover_budget": config.evaluation.gamma_turnover_budget,
                "objective": loss_objective,
                "rank_ic_weight": config.training.multitask_loss.rank_ic_weight,
                "return_rank_ic_weight": config.training.multitask_loss.return_rank_ic_weight,
                "direction_weight": config.training.multitask_loss.direction_weight,
                "volatility_regime_weight": config.training.multitask_loss.volatility_regime_weight,
                "concentration_weight": config.training.multitask_loss.concentration_weight,
                "regime_up_threshold": config.training.multitask_loss.regime_up_threshold,
                "regime_down_threshold": config.training.multitask_loss.regime_down_threshold,
            }
            loss_probe_ok, loss_probe_error = _probe_compiled_loss_forward_backward(
                compiled_loss_fn,
                train_windowed,
                batch_size=train_batch_size,
                device=device,
                amp_dtype=amp_dtype,
                non_blocking=non_blocking,
                loss_kwargs=loss_probe_kwargs,
                max_volume_participation=config.trading.max_volume_participation,
                volume_participation_equity=config.trading.volume_participation_equity,
            )
            loss_still_compiled = not (
                isinstance(compiled_loss_fn, _CompiledLossFallback)
                and compiled_loss_fn.disabled
            )
            loss_probe_ok = loss_probe_ok and loss_still_compiled
            distributed_loss_probe_ok = _distributed_probe_succeeded(loss_probe_ok, device)
            if distributed_loss_probe_ok:
                loss_compile_status += ":probe_passed"
                print(
                    f"[Train {train_years}] compiled canonical loss forward/backward probe passed "
                    f"(rows={train_batch_size}, objective={loss_objective})"
                )
            else:
                if loss_probe_error is None:
                    loss_probe_error = "compiled loss failed or fell back on another distributed rank"
                if bool(config.training.strict_no_fallback):
                    raise RuntimeError(
                        f"[Train {train_years}] compiled loss probe failed before epoch 1: "
                        f"{loss_probe_error}. strict_no_fallback=true."
                    )
                compiled_loss_fn = partial(risk_aware_loss, **risk_loss_kwargs)
                loss_compile_status = "fallback:eager_after_probe"
                print(
                    f"[Train {train_years}] compiled loss probe failed; all ranks use eager loss: "
                    f"{loss_probe_error}"
                )

        if ddp_enabled:
            if ddp_panel_slab_enabled:
                if panel_slab_model is None:
                    raise RuntimeError("DDP panel-slab was selected but its forward wrapper is unavailable")
                compiled_train_model = panel_slab_model
                eval_model = model
            compiled_train_model = _wrap_distributed_data_parallel_model(
                compiled_train_model,
                config=config,
                device=device,
            )
        final_eval_model: nn.Module = (
            _unwrap_distributed_data_parallel(eval_model) if ddp_enabled else eval_model
        )

        eval_model_status = (
            "eager_with_compiled_panel_slab"
            if panel_slab_compile_status.startswith("compiled")
            else "compiled"
            if eval_model is not model
            else "eager"
        )
        effective_train_feature_label = (
            f"{effective_train_feature_dtype}@{train_windowed.features.device}"
        )
        print(
            f"[Train {train_years}] optimization status: "
            f"model_compile={model_compile_status}; "
            f"model_compile_probe={model_compile_probe_status}; "
            f"eval_model={eval_model_status}; "
            f"multi_gpu={'distributed_data_parallel' if ddp_enabled else 'none'}; "
            f"panel_slab_forward={panel_slab_compile_status}; "
            f"loss_compile={loss_compile_status}; "
            f"backtest_compile={bool(config.training.backtest_compile)}; "
            f"backtest_stateful_compile={bool(config.training.backtest_compile_stateful)}; "
            f"backtest_compile_dynamic={bool(getattr(config.training, 'backtest_compile_dynamic', False))}; "
            f"backtest_prep_compile={_env_truthy('STOCKAGENT_BACKTEST_COMPILE_PREP', '1')}; "
            f"cache_train_gpu={bool(config.training.cache_train_tensors_on_gpu)}; "
            f"cache_train_features={effective_train_feature_label}; "
            f"cache_eval_gpu={bool(config.training.cache_eval_tensors_on_gpu)}; "
            f"record_epoch_curve={record_epoch_curve}; "
            f"epoch_test_curve={run_epoch_test_curve}; "
            f"curve_plot_async={bool(config.training.curve_plot_async)}"
        )
        if int(config.training.finite_check_interval_steps) == 1 and device.type == "cuda":
            print(
                f"[Train {train_years}] WARN finite_check_interval_steps=1 scans loss, gradients, "
                "and parameters every batch; this synchronizes CUDA and can make training CPU-bound. "
                "Use 100+ for throughput runs unless debugging NaNs/Infs."
            )
        print(f"[Train {train_years}] training mode=windowed tensor")
        # Compile probes intentionally allocate real train/eval workspaces. The
        # immutable train base is already cached and remains live; release only
        # temporary allocator blocks before considering eval caches.
        _release_cuda_memory(device)

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

        combined_test_windowed_shared = _maybe_share_windowed_base_from_cached(
            name=f"test windowed tensors {train_years}",
            split=combined_test_windowed,
            device=device,
            non_blocking=non_blocking,
            enabled=bool(config.training.cache_eval_tensors_on_gpu),
            cached_base=combined_val_windowed,
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

        early_stop_ratio = max(0.0, float(config.training.early_stopping_no_improve_ratio))
        early_stop_patience = int(np.ceil(config.training.epochs * early_stop_ratio))
        early_stop_min_delta = max(0.0, float(getattr(config.training, "early_stopping_min_delta", 0.0)))
        best_checkpoint_max_epoch = max(0, int(getattr(config.training, "best_checkpoint_max_epoch", 0)))
        val_interval = max(1, int(config.training.val_interval_epochs))
        no_improve_epochs = resume_no_improve_epochs
        last_epoch = start_epoch - 1
        early_stop_improvement_label = (
            "checkpoint-eligible improvement" if best_checkpoint_max_epoch > 0 else "improvement"
        )
        print(f"[Train {train_years}] validation interval={val_interval} epoch(s)")
        print(f"Train {train_years}")
        if resume_rng_state is not None:
            if _restore_rng_state(resume_rng_state):
                print(f"[Train {train_years}] restored Python/NumPy/Torch RNG state")
            else:
                print(f"[Train {train_years}] checkpoint RNG state is incomplete; continuing from configured seed")
        epoch_pbar = tqdm(
            range(start_epoch, config.training.epochs + 1),
            desc=" Epochs",
            leave=True,
            dynamic_ncols=True,
            disable=not _distributed_is_rank0(),
        )
        val_backtest: BacktestResultTensor | None = None
        test_mean_best_by_val: float | None = None
        get_backtest_compile_stats(reset=True)
        get_backtest_prep_compile_stats(reset=True)
        get_backtest_runtime_stats(reset=True)
        get_loss_runtime_stats(reset=True)

        def _run_one_train_epoch(train_model: nn.Module) -> tuple[torch.Tensor, TimingBreakdown]:
            if train_windowed is not None:
                if ddp_enabled:
                    return _train_epoch_windowed_tensor_ddp(
                        train_model,
                        compiled_loss_fn,
                        train_windowed,
                        optimizer,
                        scaler,
                        batch_size=train_batch_size,
                        device=device,
                        amp_dtype=amp_dtype,
                        non_blocking=non_blocking,
                        objective=loss_objective,
                        grad_clip_norm=config.training.grad_clip_norm,
                        long_only=config.trading.long_only,
                        buy_fee_rate=config.trading.buy_fee_rate,
                        sell_fee_rate=config.trading.sell_fee_rate,
                        max_turnover_ratio=config.trading.max_turnover_ratio,
                        gross_leverage=1.0,
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
                        finite_check_interval_steps=config.training.finite_check_interval_steps,
                        rank_ic_weight=config.training.multitask_loss.rank_ic_weight,
                        return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
                        direction_weight=config.training.multitask_loss.direction_weight,
                        volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                        concentration_weight=config.training.multitask_loss.concentration_weight,
                        regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                        regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                        lr_scheduler=scheduler,
                        lr_scheduler_interval=scheduler_step_interval,
                        profile_timing=profile_timing,
                        debug_timing_sync=bool(getattr(config.training, "debug_timing_sync", False)),
                        max_volume_participation=config.trading.max_volume_participation,
                        volume_participation_equity=config.trading.volume_participation_equity,
                        use_panel_slab=ddp_panel_slab_enabled,
                    )
                return _train_epoch_windowed_tensor(
                    train_model,
                    panel_slab_model,
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
                    gross_leverage=1.0,
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
                    return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
                    direction_weight=config.training.multitask_loss.direction_weight,
                    volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                    concentration_weight=config.training.multitask_loss.concentration_weight,
                    regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                    regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                    factor_aug_kwargs=factor_aug_kwargs,
                    lr_scheduler=scheduler,
                    lr_scheduler_interval=scheduler_step_interval,
                    profile_timing=profile_timing,
                    debug_timing_sync=bool(getattr(config.training, "debug_timing_sync", False)),
                    max_volume_participation=config.trading.max_volume_participation,
                    volume_participation_equity=config.trading.volume_participation_equity,
                )

        def _save_best_val_complete_fold_artifacts(
            *,
            context: FoldRuntimeContext,
            epoch: int,
            val_loss: float,
            val_backtest_epoch: BacktestResultTensor,
            val_row_start: int,
            val_row_end: int,
        ) -> FoldResult | None:
            if not bool(getattr(config.training, "save_best_val_artifacts", False)):
                return None
            if not bool(getattr(config.training, "save_best_val_fold_artifacts", False)):
                return None

            fold = context.fold
            fold_dir = context.fold_dir
            artifact_start = time.perf_counter()
            print(
                f"[Fold {fold.fold_id}] best val improved at epoch {epoch}; "
                "writing full fold artifacts from current checkpoint"
            )

            test_eval_start = time.perf_counter()
            test_windowed = _prepare_windowed_split(
                dataset_to_windowed_tensors(context.test_ds),
                device,
                non_blocking,
                shared_base=train_windowed,
                name=f"fold {fold.fold_id} best-val test windowed tensors",
            )
            test_bt_t, test_ic, test_met = _evaluate_windowed_tensor_batch(
                final_eval_model,
                panel_slab_model,
                test_windowed,
                device,
                amp_dtype,
                non_blocking,
                config.trading.long_only,
                config.trading.buy_fee_rate,
                config.trading.sell_fee_rate,
                config.trading.max_turnover_ratio,
                1.0,
                config.trading.min_trade_weight,
                portfolio_activation=config.trading.portfolio_activation,
                chunk_rows=eval_chunk_rows,
                backtest_chunk_rows=eval_backtest_chunk_rows,
                profile_timing=profile_timing,
                progress_label=f"[Train {train_years} fold {fold.fold_id} final-test]",
                max_volume_participation=config.trading.max_volume_participation,
                volume_participation_equity=config.trading.volume_participation_equity,
            )
            test_date_idx = test_windowed.valid_indices.to(
                device=test_windowed.future_log_returns.device,
                dtype=torch.long,
            )
            test_returns = test_windowed.future_log_returns[test_date_idx]
            test_masks = test_windowed.tradable_mask[test_date_idx]
            test_buy_masks = test_windowed.can_buy_mask[test_date_idx]
            test_sell_masks = test_windowed.can_sell_mask[test_date_idx]
            test_short_open_masks = test_windowed.can_short_open_mask[test_date_idx]
            test_force_cover_masks = test_windowed.force_short_cover_mask[test_date_idx]
            test_force_exit_masks = test_windowed.force_exit_mask[test_date_idx]
            test_bench = test_windowed.benchmark[test_date_idx]
            test_eval_indices = _windowed_valid_indices_numpy(test_windowed)
            test_eval_total = time.perf_counter() - test_eval_start

            val_row_start = max(0, int(val_row_start))
            val_row_end = max(val_row_start, int(val_row_end))
            if (
                val_backtest_epoch.weights_history.numel()
                and int(val_backtest_epoch.weights_history.size(0)) >= val_row_end
            ):
                val_date_idx = combined_val_windowed.valid_indices.to(
                    device=combined_val_windowed.future_log_returns.device,
                    dtype=torch.long,
                )
                val_returns_device = combined_val_windowed.future_log_returns[val_date_idx].to(
                    device=val_backtest_epoch.weights_history.device,
                    non_blocking=False,
                )
                val_masks_device = combined_val_windowed.tradable_mask[val_date_idx].to(
                    device=val_backtest_epoch.weights_history.device,
                    non_blocking=False,
                )
                val_ic = ic_summary(
                    compute_ic_series_torch(
                        val_backtest_epoch.weights_history[val_row_start:val_row_end],
                        val_returns_device[val_row_start:val_row_end],
                        val_masks_device[val_row_start:val_row_end],
                    ).to(device="cpu", dtype=torch.float32).numpy()
                )
            else:
                val_ic = ic_summary(np.asarray([], dtype=np.float32))
            val_met = _compute_metrics_from_tensors(
                val_backtest_epoch.strategy_returns[val_row_start:val_row_end],
                val_backtest_epoch.benchmark_returns[val_row_start:val_row_end],
                val_backtest_epoch.turnovers[val_row_start:val_row_end],
            )

            test_dates = panel.dates[test_eval_indices]
            test_close_prices = panel.close_prices[test_eval_indices]
            test_daily_volumes = None if panel.daily_volumes is None else panel.daily_volumes[test_eval_indices]
            test_bt = test_bt_t.to_numpy()
            deployment_test_rows = int(context.deployment_test_rows)
            deployment_test_bt = _prefix_backtest_result(test_bt, deployment_test_rows)
            deployment_test_dates = test_dates[:deployment_test_rows]
            write_integer_holdings_table = _save_integer_share_holdings_table_enabled(config)
            test_integer_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt.weights_history,
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                can_buy_mask=test_buy_masks.detach().cpu().numpy(),
                can_sell_mask=test_sell_masks.detach().cpu().numpy(),
                can_short_open_mask=test_short_open_masks.detach().cpu().numpy(),
                force_short_cover_mask=test_force_cover_masks.detach().cpu().numpy(),
                force_exit_mask=test_force_exit_masks.detach().cpu().numpy(),
                benchmark_returns=test_bt.benchmark_returns,
                initial_capital=1_000_000.0,
                buy_fee_rate=config.trading.buy_fee_rate,
                sell_fee_rate=config.trading.sell_fee_rate,
                long_only=config.trading.long_only,
                max_turnover_ratio=config.trading.max_turnover_ratio,
                max_volume_participation=config.trading.max_volume_participation,
                gross_leverage=1.0,
                min_trade_weight=config.trading.min_trade_weight,
                portfolio_activation=config.trading.portfolio_activation,
                close_prices=test_close_prices,
                daily_volumes=test_daily_volumes,
                symbols=panel.symbols,
                dates=test_dates,
                collect_holdings=write_integer_holdings_table,
            )
            test_integer_met = compute_metrics(test_integer_bt)
            fold_result = FoldResult(
                fold_id=fold.fold_id,
                train_years=fold.train_years,
                val_years=fold.val_years,
                test_years=fold.test_years,
                best_val_loss=float(val_loss),
                val_ic=val_ic,
                val_metrics=val_met,
                test_ic=test_ic,
                test_metrics=test_met,
                test_integer_metrics=test_integer_met,
            )
            _save_fold_output_artifacts(
                fold_dir=fold_dir,
                fold_result=fold_result,
                model=model,
                test_backtest=test_bt,
                test_dates=test_dates,
                symbols=panel.symbols,
                config=config,
                test_future_returns=test_returns,
                test_integer_backtest=test_integer_bt,
                holdings_records=holdings_records,
                deployment_backtest=deployment_test_bt,
                deployment_dates=deployment_test_dates,
                backtest_artifact_compression="compressed",
                print_report=False,
                write_plots=bool(getattr(config.training, "save_best_val_fold_plots", True)),
            )
            print(
                f"[Fold {fold.fold_id}] best-val fold artifacts written in "
                f"{time.perf_counter() - artifact_start:.1f}s "
                f"(test_eval={test_eval_total:.1f}s, compression=compressed): {fold_dir}"
            )
            if bool(getattr(config.training, "postprocess_benchmark_after_best_val", False)):
                _run_postprocess_benchmark_after_fold(
                    config=config,
                    output_path=output_path,
                    fold_id=fold.fold_id,
                    enabled=bool(getattr(config.training, "postprocess_benchmark_after_best_val", False)),
                )
            return fold_result

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
                if (
                    scheduler is not None
                    and not scheduler_requires_metric
                    and scheduler_step_interval != "step"
                ):
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
                        "test_mean_scope": curve_test_scope,
                        "test_mean_sampled": True,
                        "test_sample_fold_id": curve_test_fold_id,
                        "test_sample_rows": curve_test_rows,
                        "lr": float(optimizer.param_groups[0]["lr"]),
                        "no_improve": int(no_improve_epochs),
                        **_timing_curve_payload(
                            train_timing=train_timing,
                            scheduler_s=scheduler_total,
                            progress_update_s=progress_update_total,
                            cuda_sync_s=cuda_sync_total,
                            scalar_sync_s=scalar_sync_total,
                            epoch_wall_s=time.perf_counter() - epoch_start,
                            timing_synchronized=(
                                device.type != "cuda"
                                or profile_timing
                                or bool(getattr(config.training, "debug_timing_sync", False))
                            ),
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
            val_return_weights_history = bool(getattr(config.training, "save_best_val_artifacts", False))
            if full_objective_eval_required:
                val_eval_start = time.perf_counter()
                eval_model.eval()
                with torch.inference_mode():
                    for index, (_, context) in enumerate(fold_contexts.items()):
                        start = val_offsets[index]
                        end = val_offsets[index + 1]

                        val_loss, val_ic, val_fold_timing = _evaluate_windowed_aux_objective_loss(
                            eval_model,
                            combined_val_windowed,
                            start,
                            end,
                            device,
                            amp_dtype,
                            non_blocking,
                            chunk_rows=eval_chunk_rows,
                            objective=loss_objective,
                            long_only=config.trading.long_only,
                            buy_fee_rate=config.trading.buy_fee_rate,
                            sell_fee_rate=config.trading.sell_fee_rate,
                            max_turnover_ratio=config.trading.max_turnover_ratio,
                            gross_leverage=1.0,
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
                            return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
                            direction_weight=config.training.multitask_loss.direction_weight,
                            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                            concentration_weight=config.training.multitask_loss.concentration_weight,
                            regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                            regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                            factor_loss_kwargs=eval_risk_loss_kwargs,
                            max_volume_participation=config.trading.max_volume_participation,
                            volume_participation_equity=config.trading.volume_participation_equity,
                        )
                        _add_timing(val_timing, val_fold_timing)
                        val_losses.append(val_loss)
                        if val_ic is not None:
                            val_ics.append(val_ic)

                        checkpoint_epoch_allowed = (
                            best_checkpoint_max_epoch <= 0
                            or int(epoch) <= best_checkpoint_max_epoch
                        )
                        should_checkpoint_fold = _distributed_rank0_decision(
                            checkpoint_epoch_allowed
                            and val_loss < (context.best_val_loss - early_stop_min_delta),
                            device,
                        )
                        if should_checkpoint_fold:
                            any_fold_improved = True
                            context.best_val_loss = val_loss
                            fold_ckpt_start = time.perf_counter()
                            fold_checkpoint_error: BaseException | None = None
                            try:
                                _save_fold_checkpoint(
                                    context.checkpoint_best_path,
                                    fold=context.fold,
                                    epoch=epoch,
                                    best_val_loss=val_loss,
                                    model=model,
                                    optimizer=optimizer,
                                    scaler=scaler,
                                    experiment_manifest=experiment_manifest,
                                    check_finite=config.training.checkpoint_finite_check,
                                )
                            except BaseException as exc:
                                fold_checkpoint_error = exc
                            _raise_if_distributed_phase_failed(
                                f"epoch_{epoch}_fold_{context.fold.fold_id}_checkpoint",
                                fold_checkpoint_error,
                            )
                            fold_ckpt_total += time.perf_counter() - fold_ckpt_start
                val_eval_total = max(0.0, time.perf_counter() - val_eval_start - fold_ckpt_total)
                val_loss_total = 0.0
            else:
                val_eval_start = time.perf_counter()
                val_backtest_epoch, _, _ = _evaluate_windowed_tensor_batch(
                    eval_model,
                    panel_slab_model,
                    combined_val_windowed,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    1.0,
                    config.trading.min_trade_weight,
                    portfolio_activation=config.trading.portfolio_activation,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    compute_ic=False,
                    compute_metrics_summary=False,
                    return_weights_history=val_return_weights_history,
                    profile_timing=profile_timing,
                    timing_out=val_timing,
                    reset_at_rows=val_offsets,
                    max_volume_participation=config.trading.max_volume_participation,
                    volume_participation_equity=config.trading.volume_participation_equity,
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
                    log_utility_pre_log_power=config.evaluation.eval_log_utility_pre_log_power,
                    log_utility_periods_per_year=config.evaluation.eval_log_utility_periods_per_year,
                    log_utility_log_shift=config.evaluation.eval_log_utility_log_shift,
                )
                val_loss_total = time.perf_counter() - val_loss_start

            curve_test_interval = max(1, int(getattr(config.training, "curve_test_interval", 1)))
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
                if full_objective_eval_required:
                    eval_model.eval()
                    with torch.inference_mode():
                        start = curve_test_start_row
                        end = curve_test_end_row
                        test_loss, _, test_fold_timing = _evaluate_windowed_aux_objective_loss(
                            eval_model,
                            combined_test_windowed,
                            start,
                            end,
                            device,
                            amp_dtype,
                            non_blocking,
                            chunk_rows=eval_chunk_rows,
                            objective=loss_objective,
                            long_only=config.trading.long_only,
                            buy_fee_rate=config.trading.buy_fee_rate,
                            sell_fee_rate=config.trading.sell_fee_rate,
                            max_turnover_ratio=config.trading.max_turnover_ratio,
                            gross_leverage=1.0,
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
                            return_rank_ic_weight=config.training.multitask_loss.return_rank_ic_weight,
                            direction_weight=config.training.multitask_loss.direction_weight,
                            volatility_regime_weight=config.training.multitask_loss.volatility_regime_weight,
                            concentration_weight=config.training.multitask_loss.concentration_weight,
                            regime_up_threshold=config.training.multitask_loss.regime_up_threshold,
                            regime_down_threshold=config.training.multitask_loss.regime_down_threshold,
                            factor_loss_kwargs=eval_risk_loss_kwargs,
                            max_volume_participation=config.trading.max_volume_participation,
                            volume_participation_equity=config.trading.volume_participation_equity,
                        )
                        _add_timing(test_curve_timing, test_fold_timing)
                        test_losses_epoch.append(test_loss)
                else:
                    test_backtest_epoch, _, _ = _evaluate_windowed_tensor_batch(
                        eval_model,
                        panel_slab_model,
                        combined_test_windowed,
                        device,
                        amp_dtype,
                        non_blocking,
                        config.trading.long_only,
                        config.trading.buy_fee_rate,
                        config.trading.sell_fee_rate,
                        config.trading.max_turnover_ratio,
                        1.0,
                        config.trading.min_trade_weight,
                        portfolio_activation=config.trading.portfolio_activation,
                        chunk_rows=eval_chunk_rows,
                        backtest_chunk_rows=eval_backtest_chunk_rows,
                        compute_ic=False,
                        compute_metrics_summary=False,
                        return_weights_history=False,
                        profile_timing=False,
                        timing_out=test_curve_timing,
                        reset_at_rows=curve_test_offsets,
                        max_volume_participation=config.trading.max_volume_participation,
                        volume_participation_equity=config.trading.volume_participation_equity,
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
                        log_utility_pre_log_power=config.evaluation.eval_log_utility_pre_log_power,
                        log_utility_periods_per_year=config.evaluation.eval_log_utility_periods_per_year,
                        log_utility_log_shift=config.evaluation.eval_log_utility_log_shift,
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
                for index, (context, val_loss_raw) in enumerate(
                    zip(deferred_val_loss_contexts, val_loss_values, strict=False)
                ):
                    val_loss = float(val_loss_raw)
                    val_losses.append(val_loss)
                    checkpoint_epoch_allowed = (
                        best_checkpoint_max_epoch <= 0 or int(epoch) <= best_checkpoint_max_epoch
                    )
                    should_checkpoint_fold = _distributed_rank0_decision(
                        checkpoint_epoch_allowed
                        and val_loss < (context.best_val_loss - early_stop_min_delta),
                        device,
                    )
                    if should_checkpoint_fold:
                        any_fold_improved = True
                        context.best_val_loss = val_loss
                        fold_ckpt_start = time.perf_counter()
                        fold_checkpoint_error: BaseException | None = None
                        try:
                            _save_fold_checkpoint(
                                context.checkpoint_best_path,
                                fold=context.fold,
                                epoch=epoch,
                                best_val_loss=val_loss,
                                model=model,
                                optimizer=optimizer,
                                scaler=scaler,
                                experiment_manifest=experiment_manifest,
                                check_finite=config.training.checkpoint_finite_check,
                            )
                            if bool(getattr(config.training, "save_best_val_artifacts", False)):
                                _save_best_val_backtest_snapshot(
                                    fold_dir=context.fold_dir,
                                    fold=context.fold,
                                    epoch=epoch,
                                    val_loss=val_loss,
                                    val_backtest=val_backtest_epoch,
                                    row_start=val_offsets[index],
                                    row_end=val_offsets[index + 1],
                                    dates=panel.dates[context.val_ds.valid_indices],
                                    objective=loss_objective,
                                )
                                fold_result = _save_best_val_complete_fold_artifacts(
                                    context=context,
                                    epoch=epoch,
                                    val_loss=val_loss,
                                    val_backtest_epoch=val_backtest_epoch,
                                    val_row_start=val_offsets[index],
                                    val_row_end=val_offsets[index + 1],
                                )
                                if fold_result is not None:
                                    results_by_fold[context.fold.fold_id] = fold_result
                        except BaseException as exc:
                            fold_checkpoint_error = exc
                        _raise_if_distributed_phase_failed(
                            f"epoch_{epoch}_fold_{context.fold.fold_id}_checkpoint_artifacts",
                            fold_checkpoint_error,
                        )
                        fold_ckpt_total += time.perf_counter() - fold_ckpt_start
            if test_count > 0:
                test_loss_values = scalar_values[scalar_offset : scalar_offset + test_count]
                test_losses_epoch.extend(float(value) for value in test_loss_values)
            val_mean_loss = float(np.mean(val_losses)) if val_losses else float("nan")
            if should_compute_test_mean:
                sampled_test_mean = float(np.mean(test_losses_epoch)) if test_losses_epoch else None
                test_mean_best_by_val = sampled_test_mean

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
                    elif scheduler_step_interval != "step":
                        scheduler.step()
                    scheduler_total = time.perf_counter() - scheduler_start

                group_ckpt_start = time.perf_counter()
                group_checkpoint_error: BaseException | None = None
                try:
                    _save_group_checkpoint(
                        group_checkpoint_path,
                        train_years=train_years,
                        epoch=epoch,
                        model=model,
                        optimizer=optimizer,
                        scaler=scaler,
                        experiment_manifest=experiment_manifest,
                        effective_train_batch_size=train_batch_size,
                        scheduler=scheduler,
                        no_improve_epochs=no_improve_epochs,
                        early_stop_patience=early_stop_patience,
                        early_stopping_no_improve_ratio=early_stop_ratio,
                        early_stop_val_interval_epochs=val_interval,
                        check_finite=config.training.checkpoint_finite_check,
                    )
                except BaseException as exc:
                    group_checkpoint_error = exc
                _raise_if_distributed_phase_failed(
                    f"epoch_{epoch}_group_checkpoint",
                    group_checkpoint_error,
                )
                group_ckpt_total = time.perf_counter() - group_ckpt_start

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
                        "test_mean_scope": curve_test_scope,
                        "test_mean_sampled": True,
                        "test_sample_fold_id": curve_test_fold_id,
                        "test_sample_rows": curve_test_rows,
                        "lr": float(optimizer.param_groups[0]["lr"]),
                        "no_improve": int(no_improve_epochs),
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
                            timing_synchronized=(
                                device.type != "cuda"
                                or profile_timing
                                or bool(getattr(config.training, "debug_timing_sync", False))
                            ),
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

                if early_stop_patience > 0 and no_improve_epochs >= early_stop_patience:
                    print(
                        f"[Train {train_years}] early stop at epoch {epoch}: "
                        f"no {early_stop_improvement_label} for "
                        f"{no_improve_epochs} validation check(s) "
                        f"(patience={early_stop_patience})"
                    )
                    break

        _distributed_barrier()
        # Group checkpoints capture RNG from every rank.  All ranks must enter
        # this collective in the same order before non-writers peel off into
        # the rank0-only artifact stage.
        group_checkpoint_error: BaseException | None = None
        try:
            _save_group_checkpoint(
                group_checkpoint_path,
                train_years=train_years,
                epoch=last_epoch,
                model=model,
                optimizer=optimizer,
                scaler=scaler,
                experiment_manifest=experiment_manifest,
                effective_train_batch_size=train_batch_size,
                scheduler=scheduler,
                no_improve_epochs=no_improve_epochs,
                early_stop_patience=early_stop_patience,
                early_stopping_no_improve_ratio=early_stop_ratio,
                early_stop_val_interval_epochs=val_interval,
                check_finite=config.training.checkpoint_finite_check,
            )
        except BaseException as exc:
            group_checkpoint_error = exc
        _raise_if_distributed_phase_failed("final_group_checkpoint", group_checkpoint_error)

        curve_flush_error: BaseException | None = None
        try:
            curve_flush_start = time.perf_counter()
            curve_plot_timing: dict[str, float | int | str] = {}
            if curve_plotter is not None:
                if defer_epoch_curve_plot_until_end:
                    curve_plot_timing = _run_epoch_curve_plot_once(group_curve_path, curve_plot_request_interval)
                else:
                    curve_plotter.flush()
            curve_flush_total = time.perf_counter() - curve_flush_start
            if profile_timing and curve_flush_total > 0.0:
                _log_timing(
                    f"Train {train_years} setup.curve_plot_flush",
                    TimingBreakdown(plot_s=curve_flush_total, total_s=curve_flush_total),
                )
                _log_curve_plot_timing(f"Train {train_years} epoch_curve_plot", curve_plot_timing)
        except BaseException as exc:
            curve_flush_error = exc
        _raise_if_distributed_phase_failed("final_curve_flush", curve_flush_error)

        if ddp_enabled and not _distributed_should_write():
            # Final fold metrics, plots, and parquet artifacts are rank0-only.
            # Drop rank-local model/data references before waiting so rank0 has
            # maximum PCIe/CPU/VRAM headroom for the artifact pass.
            train_windowed = None
            combined_val_windowed = None
            combined_test_windowed = None
            model = None
            compiled_train_model = None
            eval_model = None
            panel_slab_model = None
            optimizer = None
            scaler = None
            scheduler = None
            if device.type == "cuda":
                _release_cuda_memory(device)
            _raise_if_distributed_phase_failed("final_fold_artifacts", None)
            _raise_if_distributed_phase_failed("final_postprocess", None)
            continue

        final_artifact_error: BaseException | None = None
        try:
            for index, (_, context) in enumerate(fold_contexts.items()):
                fold = context.fold
                fold_dir = context.fold_dir
                best_checkpoint_path = context.checkpoint_best_path
                if best_checkpoint_path.exists():
                    artifact_checkpoint_path = best_checkpoint_path
                    checkpoint = _load_checkpoint(best_checkpoint_path)
                    _validate_checkpoint_manifest(
                        checkpoint,
                        experiment_manifest,
                        checkpoint_path=best_checkpoint_path,
                        scope="artifact",
                        expected_fold=fold,
                    )
                    _load_state_dict(model, checkpoint["model_state_dict"])
                    best_val_loss = float(checkpoint.get("best_val_loss", context.best_val_loss))
                else:
                    artifact_checkpoint_path = group_checkpoint_path
                    group_checkpoint = _load_checkpoint(group_checkpoint_path)
                    _validate_checkpoint_manifest(
                        group_checkpoint,
                        experiment_manifest,
                        checkpoint_path=group_checkpoint_path,
                        scope="artifact",
                        expected_train_years=train_years,
                    )
                    _load_state_dict(model, group_checkpoint["model_state_dict"])
                    best_val_loss = context.best_val_loss

                fold_eval_start = time.perf_counter()
                val_windowed = _prepare_windowed_split(
                    dataset_to_windowed_tensors(context.val_ds),
                    device,
                    non_blocking,
                    shared_base=train_windowed,
                    name=f"fold {fold.fold_id} best-checkpoint val windowed tensors",
                )
                val_bt_t, val_ic, val_met = _evaluate_windowed_tensor_batch(
                    eval_model,
                    panel_slab_model,
                    val_windowed,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    1.0,
                    config.trading.min_trade_weight,
                    portfolio_activation=config.trading.portfolio_activation,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                    progress_label=f"[Train {train_years} fold {fold.fold_id} best-val]",
                    max_volume_participation=config.trading.max_volume_participation,
                    volume_participation_equity=config.trading.volume_participation_equity,
                )
                test_windowed = _prepare_windowed_split(
                    dataset_to_windowed_tensors(context.test_ds),
                    device,
                    non_blocking,
                    shared_base=train_windowed,
                    name=f"fold {fold.fold_id} final-test windowed tensors",
                )
                test_bt_t, test_ic, test_met = _evaluate_windowed_tensor_batch(
                    eval_model,
                    panel_slab_model,
                    test_windowed,
                    device,
                    amp_dtype,
                    non_blocking,
                    config.trading.long_only,
                    config.trading.buy_fee_rate,
                    config.trading.sell_fee_rate,
                    config.trading.max_turnover_ratio,
                    1.0,
                    config.trading.min_trade_weight,
                    portfolio_activation=config.trading.portfolio_activation,
                    chunk_rows=eval_chunk_rows,
                    backtest_chunk_rows=eval_backtest_chunk_rows,
                    profile_timing=profile_timing,
                    max_volume_participation=config.trading.max_volume_participation,
                    volume_participation_equity=config.trading.volume_participation_equity,
                )
                test_date_idx = test_windowed.valid_indices.to(
                    device=test_windowed.future_log_returns.device,
                    dtype=torch.long,
                )
                test_returns = test_windowed.future_log_returns[test_date_idx]
                test_masks = test_windowed.tradable_mask[test_date_idx]
                test_buy_masks = test_windowed.can_buy_mask[test_date_idx]
                test_sell_masks = test_windowed.can_sell_mask[test_date_idx]
                test_short_open_masks = test_windowed.can_short_open_mask[test_date_idx]
                test_force_cover_masks = test_windowed.force_short_cover_mask[test_date_idx]
                test_force_exit_masks = test_windowed.force_exit_mask[test_date_idx]
                test_bench = test_windowed.benchmark[test_date_idx]
                test_eval_indices = _windowed_valid_indices_numpy(test_windowed)
                fold_eval_total = time.perf_counter() - fold_eval_start

                test_report_start = time.perf_counter()
                test_dates = panel.dates[test_eval_indices]
                test_close_prices = panel.close_prices[test_eval_indices]
                test_daily_volumes = None if panel.daily_volumes is None else panel.daily_volumes[test_eval_indices]
                test_bt = test_bt_t.to_numpy()
                deployment_test_rows = int(context.deployment_test_rows)
                deployment_test_bt = _prefix_backtest_result(test_bt, deployment_test_rows)
                deployment_test_dates = test_dates[:deployment_test_rows]
                write_integer_holdings_table = _save_integer_share_holdings_table_enabled(config)
                test_integer_bt, holdings_records = run_backtest_integer_shares(
                    weights=test_bt.weights_history,
                    future_returns=test_returns.detach().cpu().numpy(),
                    tradable_mask=test_masks.detach().cpu().numpy(),
                    can_buy_mask=test_buy_masks.detach().cpu().numpy(),
                    can_sell_mask=test_sell_masks.detach().cpu().numpy(),
                    can_short_open_mask=test_short_open_masks.detach().cpu().numpy(),
                    force_short_cover_mask=test_force_cover_masks.detach().cpu().numpy(),
                    force_exit_mask=test_force_exit_masks.detach().cpu().numpy(),
                    benchmark_returns=test_bt.benchmark_returns,
                    initial_capital=1_000_000.0,
                    buy_fee_rate=config.trading.buy_fee_rate,
                    sell_fee_rate=config.trading.sell_fee_rate,
                    long_only=config.trading.long_only,
                    max_turnover_ratio=config.trading.max_turnover_ratio,
                    max_volume_participation=config.trading.max_volume_participation,
                    gross_leverage=1.0,
                    min_trade_weight=config.trading.min_trade_weight,
                    portfolio_activation=config.trading.portfolio_activation,
                    close_prices=test_close_prices,
                    daily_volumes=test_daily_volumes,
                    symbols=panel.symbols,
                    dates=test_dates,
                    collect_holdings=write_integer_holdings_table,
                )
                test_integer_met = compute_metrics(test_integer_bt)
                test_report_total = time.perf_counter() - test_report_start

                objective_key = _objective_metric_key(loss_objective)
                val_objective_metric = float(val_met.get(objective_key, float("nan")))
                test_objective_metric = float(test_met.get(objective_key, float("nan")))
                print(f"\n  [val]   IC={val_ic['ic_mean']:+.4f}  IC_IR={val_ic['ic_ir']:+.4f}  {loss_objective}={val_objective_metric:+.4f}  cum_ret={val_met['cumulative_return']:+.4f}  excess={val_met['excess_return_vs_benchmark']:+.4f}")
                print(f"  [test]  IC={test_ic['ic_mean']:+.4f}  IC_IR={test_ic['ic_ir']:+.4f}  {loss_objective}={test_objective_metric:+.4f}  cum_ret={test_met['cumulative_return']:+.4f}  excess={test_met['excess_return_vs_benchmark']:+.4f}")
                if profile_timing:
                    _log_timing(
                        f"Train {train_years} fold {fold.fold_id} test_stage",
                        TimingBreakdown(
                            total_s=fold_eval_total + test_report_total,
                            backtest_s=fold_eval_total,
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
                save_timing, plot_timing = _save_fold_output_artifacts(
                    fold_dir=fold_dir,
                    fold_result=fold_result,
                    model=model,
                    test_backtest=test_bt,
                    test_dates=test_dates,
                    symbols=panel.symbols,
                    config=config,
                    test_future_returns=test_returns,
                    test_integer_backtest=test_integer_bt,
                    holdings_records=holdings_records,
                    deployment_backtest=deployment_test_bt,
                    deployment_dates=deployment_test_dates,
                    print_report=True,
                    write_plots=True,
                    mark_complete=True,
                )
                plot_total = float(plot_timing.get("total_s", 0.0))

                refresh_start = time.perf_counter()
                _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))
                plot_timing["walkforward_refresh_s"] = float(time.perf_counter() - refresh_start)
                explain_start = time.perf_counter()
                explain_path = _run_fold_explainability(
                    model=model,
                    panel=panel,
                    config=config,
                    output_path=output_path,
                    fold=fold,
                    device=device,
                    checkpoint_path=artifact_checkpoint_path,
                )
                plot_timing["explainability_total_s"] = float(time.perf_counter() - explain_start)
                plot_timing["post_plot_total_s"] = float(time.perf_counter() - save_start)
                if _distributed_should_write():
                    (fold_dir / "plot_timing.json").write_text(
                        json.dumps(plot_timing, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
                if explain_path is not None:
                    print(f"[Fold {fold.fold_id}] explainability output: {explain_path}")
                if profile_timing:
                    _log_timing(
                        f"Train {train_years} fold {fold.fold_id} save_plot",
                        TimingBreakdown(
                            total_s=time.perf_counter() - save_start,
                            save_s=float(save_timing.get("total_s", 0.0)),
                            plot_s=plot_total + float(plot_timing.get("walkforward_refresh_s", 0.0))
                            + float(plot_timing.get("explainability_total_s", 0.0)),
                        ),
                    )
        except BaseException as exc:
            final_artifact_error = exc
        _raise_if_distributed_phase_failed("final_fold_artifacts", final_artifact_error)

        if config.training.warm_start_from_previous_fold:
            warm_start_checkpoint_path = group_checkpoint_path

        # Drop large per-group tensors/models so next group's batch-search sees true free VRAM.
        train_windowed = None
        combined_val_windowed = None
        combined_test_windowed = None
        val_returns_device = None
        val_masks_device = None
        model = None
        compiled_train_model = None
        optimizer = None
        scaler = None
        scheduler = None
        if device.type == "cuda":
            _release_cuda_memory(device)

        final_postprocess_error: BaseException | None = None
        try:
            for fold in group_folds:
                if fold.fold_id in results_by_fold:
                    _run_postprocess_benchmark_after_fold(
                        config=config,
                        output_path=output_path,
                        fold_id=fold.fold_id,
                        enabled=bool(getattr(config.training, "postprocess_benchmark_after_fold", False)),
                    )
        except BaseException as exc:
            final_postprocess_error = exc
        _raise_if_distributed_phase_failed("final_postprocess", final_postprocess_error)

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]
