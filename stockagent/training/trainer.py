from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import torch
from torch import nn
from torch.amp import GradScaler, autocast
from torch.utils.data import DataLoader
from tqdm import tqdm

# ✅ OPTIMIZATION: Configure logging for training monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

CASH_SYMBOL_NAMES = {"CASH", "現金"}

from stockagent.backtest.report import (
    compute_metrics,
    generate_annual_report,
    plot_annual_performance,
    plot_equity_curve,
    plot_equity_curve_log,
    plot_fold_first_year_returns,
)
from stockagent.backtest.simulator import (
    BacktestResult,
    BacktestResultTensor,
    HoldingsRecord,
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.config import ExperimentConfig
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.evaluation.metrics import compute_ic_series_torch, ic_summary
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.training.dataset import CrossSectionalDataset, collate_batch
from stockagent.training.loss import sharpe_aware_loss


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


@dataclass(slots=True)
class PredictionBufferPool:
    weights: torch.Tensor | None = None
    returns: torch.Tensor | None = None
    masks: torch.Tensor | None = None
    bench: torch.Tensor | None = None
    capacity_rows: int = 0
    capacity_symbols: int = 0

    def ensure(self, rows: int, symbols: int) -> None:
        if (
            self.weights is not None
            and rows <= self.capacity_rows
            and symbols <= self.capacity_symbols
        ):
            return

        self.capacity_rows = max(rows, self.capacity_rows)
        self.capacity_symbols = max(symbols, self.capacity_symbols)
        self.weights = torch.empty((self.capacity_rows, self.capacity_symbols), dtype=torch.float32)
        self.returns = torch.empty((self.capacity_rows, self.capacity_symbols), dtype=torch.float32)
        self.masks = torch.empty((self.capacity_rows, self.capacity_symbols), dtype=torch.bool)
        self.bench = torch.empty((self.capacity_rows,), dtype=torch.float32)


def _cash_symbol_mask_from_symbols(symbols: list[str]) -> torch.Tensor:
    normalized = [str(symbol).strip().upper() for symbol in symbols]
    return torch.tensor([name in CASH_SYMBOL_NAMES for name in normalized], dtype=torch.bool)


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


def _group_key(train_years: list[int]) -> tuple[int, ...]:
    return tuple(train_years)


def _group_id(train_years: list[int]) -> str:
    return "train_" + "-".join(str(year) for year in train_years)


def _group_dir(output_path: Path, train_years: list[int]) -> Path:
    return output_path / _group_id(train_years)


def _group_checkpoint_path(output_path: Path, train_years: list[int]) -> Path:
    return _group_dir(output_path, train_years) / "checkpoint_last.pt"


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
) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(
        {
            "fold_id": fold.fold_id,
            "epoch": epoch,
            "train_years": fold.train_years,
            "val_years": fold.val_years,
            "test_years": fold.test_years,
            "best_val_loss": best_val_loss,
            "model_state_dict": _state_dict_for_save(model),
            "optimizer_state_dict": optimizer.state_dict(),
            "scaler_state_dict": scaler.state_dict(),
        },
        checkpoint_path,
    )


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
) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(
        {
            "train_years": train_years,
            "epoch": epoch,
            "model_state_dict": _state_dict_for_save(model),
            "optimizer_state_dict": optimizer.state_dict(),
            "scaler_state_dict": scaler.state_dict(),
        },
        checkpoint_path,
    )


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

    columns = [
        "date",
        "symbol",
        "shares",
        "price",
        "market_value",
        "holding_ratio",
        "is_cash",
        "traded_notional",
        "buy_fee",
        "sell_fee",
    ]

    rows = [
        {
            "date": row.date,
            "symbol": row.symbol,
            "shares": int(row.shares),
            "price": float(row.price),
            "market_value": float(row.market_value),
            "holding_ratio": float(row.holding_ratio),
            "is_cash": bool(row.is_cash),
            "traded_notional": float(row.traded_notional),
            "buy_fee": float(row.buy_fee),
            "sell_fee": float(row.sell_fee),
        }
        for row in holdings
    ]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df = df.sort_values(["date", "holding_ratio", "symbol"], ascending=[True, False, True])
    df.to_csv(output_path, index=False, columns=columns)


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


def _dataset_to_tensors(dataset: CrossSectionalDataset) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    return dataset.to_tensors()


def _prepare_host_tensor(tensor: torch.Tensor, pin_memory: bool) -> torch.Tensor:
    prepared = tensor.contiguous()
    if pin_memory and prepared.device.type == "cpu" and not prepared.is_pinned():
        prepared = prepared.pin_memory()
    return prepared


def _prepare_split_tensors(
    x: torch.Tensor,
    returns: torch.Tensor,
    masks: torch.Tensor,
    bench: torch.Tensor,
    device: torch.device,
    non_blocking: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    pin_memory = device.type == "cuda" and non_blocking
    return (
        _prepare_host_tensor(x, pin_memory),
        _prepare_host_tensor(returns, pin_memory),
        _prepare_host_tensor(masks, pin_memory),
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
    batch_size: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    total_rows = int(x.size(0))
    if total_rows == 0:
        return x, returns, masks, torch.empty((0,), dtype=torch.bool)

    padded_rows = ((total_rows + batch_size - 1) // batch_size) * batch_size
    sample_mask = torch.ones(total_rows, dtype=torch.bool)
    if padded_rows == total_rows:
        return x, returns, masks, sample_mask

    return (
        _pad_rows(x, padded_rows, 0),
        _pad_rows(returns, padded_rows, 0.0),
        _pad_rows(masks, padded_rows, False),
        _pad_rows(sample_mask, padded_rows, False),
    )


def _combine_datasets_to_tensors(
    datasets: list[CrossSectionalDataset],
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, list[int]]:
    xs: list[torch.Tensor] = []
    returns: list[torch.Tensor] = []
    masks: list[torch.Tensor] = []
    bench: list[torch.Tensor] = []
    lengths: list[int] = []
    for dataset in datasets:
        x, y, m, b = _dataset_to_tensors(dataset)
        xs.append(x)
        returns.append(y)
        masks.append(m)
        bench.append(b)
        lengths.append(int(x.size(0)))
    return (
        torch.cat(xs, dim=0),
        torch.cat(returns, dim=0),
        torch.cat(masks, dim=0),
        torch.cat(bench, dim=0),
        lengths,
    )


def _evaluate_tensor_batch(
    model: nn.Module,
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    benchmark: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    fee_per_side: float,
    chunk_rows: int,
    cash_symbol_mask: torch.Tensor | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    model.eval()
    total_rows = int(x.size(0))
    if total_rows == 0:
        raise RuntimeError("Cannot evaluate empty tensor split.")

    num_symbols = int(x.size(2))
    weights_t = torch.empty((total_rows, num_symbols), dtype=torch.float32, device=device)
    returns_t = torch.empty((total_rows, num_symbols), dtype=torch.float32, device=device)
    masks_t = torch.empty((total_rows, num_symbols), dtype=torch.bool, device=device)
    bench_t = torch.empty((total_rows,), dtype=torch.float32, device=device)

    with torch.inference_mode():
        for start in range(0, x.size(0), chunk_rows):
            end = min(start + chunk_rows, x.size(0))
            x_chunk = x[start:end].to(device=device, non_blocking=non_blocking)
            returns_chunk = future_log_returns[start:end].to(device=device, non_blocking=non_blocking)
            mask_chunk = tradable_mask[start:end].to(device=device, non_blocking=non_blocking)
            bench_chunk = benchmark[start:end].to(device=device, non_blocking=non_blocking)
            with _autocast_context(device, amp_dtype):
                weights_chunk = model(x_chunk, mask_chunk)

            weights_t[start:end].copy_(weights_chunk.float())
            returns_t[start:end].copy_(returns_chunk.float())
            masks_t[start:end].copy_(mask_chunk)
            bench_t[start:end].copy_(bench_chunk.float())

        backtest = run_backtest_torch(
            weights_t,
            returns_t,
            masks_t,
            bench_t,
            fee_per_side,
            cash_symbol_mask=cash_symbol_mask,
        )
        ic = ic_summary(compute_ic_series_torch(weights_t, returns_t, masks_t).detach().cpu().numpy())
        metrics = _compute_metrics_from_tensors(
            backtest.strategy_returns,
            backtest.benchmark_returns,
            backtest.turnovers,
        )
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
) -> int:
    if device.type != "cuda":
        return max(1, min(256, int(x.size(0))))

    total_rows = int(x.size(0))
    if total_rows <= 1:
        return 1

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
    probe_rows = max(1, min(32, total_rows))
    torch.cuda.reset_peak_memory_stats(device)
    base_alloc = torch.cuda.memory_allocated(device)
    with torch.inference_mode():
        x_probe = x[:probe_rows].to(device=device, non_blocking=True)
        mask_probe = tradable_mask[:probe_rows].to(device=device, non_blocking=True)
        with _autocast_context(device, amp_dtype):
            _ = model(x_probe, mask_probe)
    torch.cuda.synchronize(device)
    peak_alloc = torch.cuda.max_memory_allocated(device)

    incremental_bytes = max(1, peak_alloc - base_alloc)
    bytes_per_row = max(1, incremental_bytes // probe_rows)

    # Keep headroom for allocator/workspace fluctuations.
    usable_bytes = int(usable_pool_bytes * target_vram_fraction * 0.9)
    estimated_rows = usable_bytes // bytes_per_row

    return max(1, min(total_rows, int(estimated_rows)))


def _refresh_walkforward_artifacts(output_path: Path, results: list[FoldResult]) -> None:
    _write_summary(results, output_path)

    all_strategy_returns: list[np.ndarray] = []
    all_benchmark_returns: list[np.ndarray] = []
    all_turnovers: list[np.ndarray] = []
    all_weights: list[np.ndarray] = []
    all_dates: list[np.ndarray] = []
    first_year_fold_ids: list[int] = []
    first_year_labels: list[int] = []
    all_first_year_dates: list[np.ndarray] = []
    all_first_year_strategy_log: list[np.ndarray] = []
    all_first_year_baseline_log: list[np.ndarray] = []

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
            all_first_year_dates.append(fold_dates[mask])
            all_first_year_strategy_log.append(np.nan_to_num(fold_backtest.strategy_returns[mask], nan=0.0).astype(np.float64))
            all_first_year_baseline_log.append(np.nan_to_num(fold_backtest.benchmark_returns[mask], nan=0.0).astype(np.float64))

    if not all_dates:
        return

    combined_backtest = BacktestResult(
        strategy_returns=np.concatenate(all_strategy_returns, axis=0),
        benchmark_returns=np.concatenate(all_benchmark_returns, axis=0),
        turnovers=np.concatenate(all_turnovers, axis=0),
        weights_history=np.concatenate(all_weights, axis=0),
    )
    combined_dates = np.concatenate(all_dates, axis=0)

    plot_equity_curve_log(
        combined_backtest,
        combined_dates,
        output_path / "walkforward_equity_curve_log.png",
    )

    if all_first_year_dates:
        plot_fold_first_year_returns(
            all_first_year_dates,
            all_first_year_strategy_log,
            all_first_year_baseline_log,
            output_path / "walkforward_first_year_cumulative_returns.png",
        )


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


def _resolve_host_compilers() -> tuple[str | None, str | None]:
    cc_candidates = [
        os.environ.get("CC"),
        "cc",
        "gcc",
        "clang",
        "x86_64-conda-linux-gnu-cc",
    ]
    cxx_candidates = [
        os.environ.get("CXX"),
        "c++",
        "g++",
        "clang++",
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


def _can_enable_torch_compile(device: torch.device) -> tuple[bool, str]:
    """Return whether torch.compile is safe to enable in current environment."""
    if device.type != "cuda":
        return False, "torch.compile is only enabled for CUDA in this project"

    # Inductor+Triton on CUDA needs a host C compiler at runtime.
    cc, cxx = _resolve_host_compilers()
    if not cc or not cxx:
        return False, "no host C/C++ compiler found (set CC/CXX or install gcc/clang)"

    os.environ.setdefault("CC", cc)
    os.environ.setdefault("CXX", cxx)

    # Strict aggressive mode requirement: require enough SMs for max-autotune GEMM.
    try:
        props = torch.cuda.get_device_properties(device)
        sm_count = int(getattr(props, "multi_processor_count", 0))
    except Exception:
        sm_count = 0
    min_required_sms = 66
    if sm_count < min_required_sms:
        return (
            False,
            f"insufficient SMs for strict max-autotune GEMM (have {sm_count}, require >= {min_required_sms})",
        )

    return True, f"CC={cc}, CXX={cxx}, SMs={sm_count}"


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


def _build_adamw(model: nn.Module, lr: float, weight_decay: float, device: torch.device) -> torch.optim.Optimizer:
    kwargs: dict[str, object] = {
        "lr": lr,
        "weight_decay": weight_decay,
    }
    if device.type == "cuda":
        kwargs["foreach"] = True
        try:
            return torch.optim.AdamW(model.parameters(), fused=True, **kwargs)
        except Exception:
            pass
    return torch.optim.AdamW(model.parameters(), **kwargs)


def find_optimal_batch_size(
    model: nn.Module,
    sample_loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype | None,
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
    temp_optimizer = _build_adamw(model, lr=1e-3, weight_decay=0.0, device=device)

    def _measure_candidate(batch_size: int) -> tuple[bool, int]:
        torch.cuda.reset_peak_memory_stats()
        torch.cuda.empty_cache()

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
                logits = model(local_batch["x"], local_batch["tradable_mask"])
                loss = sharpe_aware_loss(
                    logits,
                    local_batch["future_log_returns"],
                    local_batch["tradable_mask"],
                    fee_per_side=0.0,
                )

            loss.backward()
            temp_optimizer.step()
            temp_optimizer.zero_grad(set_to_none=True)
            used_memory = torch.cuda.max_memory_reserved()

            smi_after = _query_nvidia_smi_free_bytes(device_index)
            if smi_after is not None:
                _, smi_used_after, _ = smi_after
                used_memory = max(used_memory, int(smi_used_after))

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
    workers = config.training.num_workers
    loader_kwargs: dict = {
        "dataset": dataset,
        "batch_size": batch_size,
        "shuffle": shuffle,
        "num_workers": workers,
        "pin_memory": (device.type == "cuda"),
        "drop_last": drop_last,
        "collate_fn": collate_batch,
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


def _split_batch_size(dataset_size: int, cap: int) -> int:
    return max(1, min(cap, dataset_size))


def _train_epoch(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
    cash_symbol_mask: torch.Tensor | None = None,
) -> float:
    model.train()
    total_loss = 0.0
    steps = 0
    
    for batch in loader:
        batch = _move_batch(batch, device, non_blocking)
        optimizer.zero_grad(set_to_none=True)
        
        with _autocast_context(device, amp_dtype):
            weights = model(batch["x"], batch["tradable_mask"])
            loss = sharpe_aware_loss(
                weights,
                batch["future_log_returns"],
                batch["tradable_mask"],
                fee_per_side=fee_per_side,
                gamma_sharpe=gamma_sharpe,
                gamma_turnover=gamma_turnover,
                cash_symbol_mask=cash_symbol_mask,
            )
        
        scaler.scale(loss).backward()
        
        # ✅ OPTIMIZATION: Monitor gradient norms for training stability
        grad_norm = 0.0
        for param in model.parameters():
            if param.grad is not None:
                grad_norm += param.grad.data.norm(2).item() ** 2
        grad_norm = grad_norm ** 0.5
        
        scaler.unscale_(optimizer)
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        scaler.step(optimizer)
        scaler.update()
        
        total_loss += float(loss.detach().cpu())
        steps += 1
        
        # ✅ OPTIMIZATION: Log VRAM usage every 10 steps
        if steps % 10 == 0 and device.type == "cuda":
            vram_gb = torch.cuda.memory_allocated(device) / 1e9
            vram_reserved_gb = torch.cuda.memory_reserved(device) / 1e9
            logger.info(
                f"Step {steps} | Loss {loss:.6f} | GradNorm {grad_norm:.4f} | "
                f"VRAM {vram_gb:.2f}GB/{vram_reserved_gb:.2f}GB"
            )
    
    return total_loss / max(steps, 1)


def _train_epoch_tensor(
    model: nn.Module,
    x: torch.Tensor,
    future_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    sample_mask: torch.Tensor,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
    cash_symbol_mask: torch.Tensor | None = None,
) -> float:
    model.train()
    total_rows = int(x.size(0))
    if total_rows == 0:
        return 0.0

    num_batches = total_rows // batch_size
    batch_order = torch.randperm(num_batches).tolist()
    total_loss = 0.0
    steps = 0

    for batch_idx in batch_order:
        start = batch_idx * batch_size
        end = min(start + batch_size, total_rows)
        batch_x = x[start:end].to(device=device, non_blocking=non_blocking)
        batch_ret = future_log_returns[start:end].to(device=device, non_blocking=non_blocking)
        batch_mask = tradable_mask[start:end].to(device=device, non_blocking=non_blocking)
        batch_sample_mask = sample_mask[start:end].to(device=device, non_blocking=non_blocking)

        optimizer.zero_grad(set_to_none=True)
        with _autocast_context(device, amp_dtype):
            weights = model(batch_x, batch_mask)
            loss = sharpe_aware_loss(
                weights,
                batch_ret,
                batch_mask,
                sample_mask=batch_sample_mask,
                fee_per_side=fee_per_side,
                gamma_sharpe=gamma_sharpe,
                gamma_turnover=gamma_turnover,
                cash_symbol_mask=cash_symbol_mask,
            )

        scaler.scale(loss).backward()
        scaler.unscale_(optimizer)
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        scaler.step(optimizer)
        scaler.update()

        total_loss += float(loss.detach().cpu())
        steps += 1

    return total_loss / max(steps, 1)


def _eval_val_loss(
    model: nn.Module,
    loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
    cash_symbol_mask: torch.Tensor | None = None,
) -> float:
    model.eval()
    losses: list[float] = []
    with torch.inference_mode():
        for batch in loader:
            batch = _move_batch(batch, device, non_blocking)
            with _autocast_context(device, amp_dtype):
                weights = model(batch["x"], batch["tradable_mask"])
                loss = sharpe_aware_loss(
                    weights, 
                    batch["future_log_returns"], 
                    batch["tradable_mask"], 
                    fee_per_side=fee_per_side,
                    gamma_sharpe=gamma_sharpe,
                    gamma_turnover=gamma_turnover,
                    cash_symbol_mask=cash_symbol_mask,
                )
            losses.append(float(loss.detach().cpu()))
    return float(np.mean(losses)) if losses else float("inf")


def _collect_predictions(
    model: nn.Module,
    loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    buffers: PredictionBufferPool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Collect all (weights, future_log_returns, tradable_mask, benchmark) from a loader."""
    model.eval()
    total_rows = len(loader.dataset)
    all_weights: torch.Tensor | None = None
    all_log_ret: torch.Tensor | None = None
    all_masks: torch.Tensor | None = None
    all_bench: torch.Tensor | None = None
    cursor = 0

    with torch.inference_mode():
        for batch in loader:
            batch = _move_batch(batch, device, non_blocking)
            with _autocast_context(device, amp_dtype):
                weights = model(batch["x"], batch["tradable_mask"])
            weights_cpu = weights.float().cpu()
            returns_cpu = batch["future_log_returns"].float().cpu()
            masks_cpu = batch["tradable_mask"].cpu()
            bench_cpu = batch["benchmark"].float().cpu()

            if all_weights is None:
                num_symbols = weights_cpu.size(1)
                buffers.ensure(total_rows, num_symbols)
                if buffers.weights is None or buffers.returns is None or buffers.masks is None or buffers.bench is None:
                    raise RuntimeError("Failed to initialize prediction buffers.")
                all_weights = buffers.weights[:total_rows, :num_symbols]
                all_log_ret = buffers.returns[:total_rows, :num_symbols]
                all_masks = buffers.masks[:total_rows, :num_symbols]
                all_bench = buffers.bench[:total_rows]

            batch_size = weights_cpu.size(0)
            end = cursor + batch_size
            all_weights[cursor:end] = weights_cpu
            all_log_ret[cursor:end] = returns_cpu
            all_masks[cursor:end] = masks_cpu
            all_bench[cursor:end] = bench_cpu
            cursor = end

    if all_weights is None or all_log_ret is None or all_masks is None or all_bench is None:
        raise RuntimeError("Prediction collection produced no batches.")

    return (
        all_weights,
        all_log_ret,
        all_masks,
        all_bench,
    )


def _evaluate_split_torch(
    model: nn.Module,
    loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    non_blocking: bool,
    fee_per_side: float,
    buffers: PredictionBufferPool,
    cash_symbol_mask: torch.Tensor | None = None,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    weights, log_ret, masks, bench = _collect_predictions(
        model,
        loader,
        device,
        amp_dtype,
        non_blocking,
        buffers,
    )
    backtest = run_backtest_torch(
        weights,
        log_ret,
        masks,
        bench,
        fee_per_side,
        cash_symbol_mask=cash_symbol_mask,
    )
    ic = ic_summary(compute_ic_series_torch(weights, log_ret, masks).cpu().numpy())
    metrics = compute_metrics(backtest.to_numpy())
    return backtest, ic, metrics


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
            "sharpe": 0.0,
            "baseline_sharpe": 0.0,
            "max_drawdown": 0.0,
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

    equity = torch.exp(torch.cumsum(r, dim=0))
    running_max = torch.cummax(equity, dim=0).values
    dd = equity / running_max.clamp_min(1e-12) - 1.0
    max_dd = float(dd.min().item()) if dd.numel() else 0.0

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "sharpe": sharpe,
        "baseline_sharpe": baseline_sharpe,
        "max_drawdown": max_dd,
        "turnover": float(t.mean().item()) if t.numel() else 0.0,
        "daily_hit_rate": float((r > 0).to(torch.float64).mean().item()),
        "excess_return_vs_universe_average": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def run_training(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
    resume: bool = True,
) -> list[FoldResult]:
    device = _resolve_device(config)
    cash_symbol_mask = _cash_symbol_mask_from_symbols(panel.symbols)
    non_blocking = config.training.non_blocking_transfer and device.type == "cuda"
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    if config.environment.use_tensor_cores and device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
        if hasattr(torch.backends.cuda, "enable_flash_sdp"):
            torch.backends.cuda.enable_flash_sdp(True)
        if hasattr(torch.backends.cuda, "enable_mem_efficient_sdp"):
            torch.backends.cuda.enable_mem_efficient_sdp(True)
        if hasattr(torch.backends.cuda, "enable_math_sdp"):
            torch.backends.cuda.enable_math_sdp(True)
        try:
            import torch._inductor.config as inductor_config  # type: ignore

            # Aggressive path: allow cudagraph on dynamic graphs when possible.
            inductor_config.triton.cudagraph_skip_dynamic_graphs = False
            inductor_config.triton.cudagraph_dynamic_shape_warn_limit = None
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

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results_by_fold: dict[int, FoldResult] = {}
    buffer_pool = PredictionBufferPool()
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

    for train_years_key, group_folds in tqdm(grouped_folds.items(), desc="Train groups", unit="group"):
        train_years = list(train_years_key)
        pending_folds = [fold for fold in group_folds if fold.fold_id not in results_by_fold]
        if not pending_folds:
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
            budget_bytes = int(config.training.vram_budget_gb * (1024 ** 3))
            margin_bytes = int(config.training.vram_safety_margin_gb * (1024 ** 3))
            effective_budget_bytes = max(0, budget_bytes - margin_bytes)

            estimation_model = CrossSectionalMLP(
                lookback=config.training.lookback,
                num_features=len(panel.feature_names),
                num_symbols=panel.num_symbols,
                hidden_dim=config.training.hidden_dim,
                dropout=config.training.dropout,
                hidden_layers=config.training.hidden_layers,
            )
            train_static_bytes = _estimate_model_static_bytes(estimation_model, training_mode=True)
            eval_static_bytes = _estimate_model_static_bytes(estimation_model, training_mode=False)
            train_sample_bytes = _estimate_sample_bytes(
                lookback=config.training.lookback,
                num_symbols=panel.num_symbols,
                num_features=len(panel.feature_names),
                hidden_dim=config.training.hidden_dim,
                amp_dtype=amp_dtype,
                training_mode=True,
            )
            eval_sample_bytes = _estimate_sample_bytes(
                lookback=config.training.lookback,
                num_symbols=panel.num_symbols,
                num_features=len(panel.feature_names),
                hidden_dim=config.training.hidden_dim,
                amp_dtype=amp_dtype,
                training_mode=False,
            )

            temp_model = CrossSectionalMLP(
                lookback=config.training.lookback,
                num_features=len(panel.feature_names),
                num_symbols=panel.num_symbols,
                hidden_dim=config.training.hidden_dim,
                dropout=config.training.dropout,
                hidden_layers=config.training.hidden_layers,
            ).to(device)

            print(f"[Train {train_years}] searching optimal train batch size...")
            temp_train_loader = _build_loader(train_ds, 1, False, config, device)
            train_batch_size, train_batch_used_bytes = find_optimal_batch_size(
                model=temp_model,
                sample_loader=temp_train_loader,
                device=device,
                amp_dtype=amp_dtype,
                target_vram_fraction=config.training.target_vram_fraction,
                vram_budget_gb=config.training.vram_budget_gb,
                vram_safety_margin_gb=config.training.vram_safety_margin_gb,
            )
            train_batch_size = max(min_batch_size, train_batch_size)
        else:
            train_batch_size = _split_batch_size(len(train_ds), config.training.batch_size_train)

        print(f"[Train {train_years}] using batch_size train={train_batch_size}")
        train_x, train_returns, train_masks, _ = _dataset_to_tensors(train_ds)
        train_x, train_returns, train_masks, train_sample_mask = _pad_training_tensors(
            train_x,
            train_returns,
            train_masks,
            train_batch_size,
        )
        train_x, train_returns, train_masks, _ = _prepare_split_tensors(
            train_x,
            train_returns,
            train_masks,
            torch.empty(0),
            device,
            non_blocking,
        )
        train_sample_mask = _prepare_host_tensor(
            train_sample_mask,
            pin_memory=(device.type == "cuda" and non_blocking),
        )

        fold_contexts: dict[int, dict[str, object]] = {}
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
            checkpoint_last_path = _checkpoint_path(fold_dir)
            checkpoint_best_path = _best_checkpoint_path(fold_dir)

            fold_contexts[fold.fold_id] = {
                "fold": fold,
                "fold_dir": fold_dir,
                "val_ds": val_ds,
                "test_ds": test_ds,
                "val_loader": _build_loader(val_ds, val_batch_size, False, config, device),
                "test_loader": _build_loader(test_ds, test_batch_size, False, config, device),
                "checkpoint_last_path": checkpoint_last_path,
                "checkpoint_best_path": checkpoint_best_path,
                "best_val_loss": float("inf"),
            }

            if resume and checkpoint_best_path.exists():
                checkpoint = _load_checkpoint(checkpoint_best_path)
                fold_contexts[fold.fold_id]["best_val_loss"] = float(checkpoint.get("best_val_loss", float("inf")))

        if not fold_contexts:
            continue

        val_datasets = [context["val_ds"] for context in fold_contexts.values()]
        combined_val_x, combined_val_returns, combined_val_masks, combined_val_bench, val_lengths = _combine_datasets_to_tensors(
            val_datasets,  # type: ignore[arg-type]
        )
        combined_val_x, combined_val_returns, combined_val_masks, combined_val_bench = _prepare_split_tensors(
            combined_val_x,
            combined_val_returns,
            combined_val_masks,
            combined_val_bench,
            device,
            non_blocking,
        )
        val_offsets: list[int] = [0]
        for length in val_lengths:
            val_offsets.append(val_offsets[-1] + length)

        model = CrossSectionalMLP(
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
            hidden_dim=config.training.hidden_dim,
            dropout=config.training.dropout,
            hidden_layers=config.training.hidden_layers,
        ).to(device)
        compiled_train_model: nn.Module = model

        optimizer = _build_adamw(
            model,
            lr=config.training.learning_rate,
            weight_decay=config.training.weight_decay,
            device=device,
        )
        scaler = GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)

        group_checkpoint_path = _group_checkpoint_path(output_path, train_years)
        start_epoch = 1
        if resume and group_checkpoint_path.exists():
            checkpoint = _load_checkpoint(group_checkpoint_path)
            if list(checkpoint.get("train_years", [])) == train_years:
                _load_state_dict(model, checkpoint["model_state_dict"])
                optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
                scaler.load_state_dict(checkpoint["scaler_state_dict"])
                start_epoch = int(checkpoint.get("epoch", 0)) + 1
                print(f"[Train {train_years}] resumed from epoch {start_epoch}")

        if start_epoch > config.training.epochs:
            print(f"[Train {train_years}] checkpoint already reached epoch {config.training.epochs}; evaluating only")

        if config.training.chunk_rows > 0:
            eval_chunk_rows = min(config.training.chunk_rows, int(combined_val_x.size(0)))
            print(f"[Train {train_years}] eval chunk_rows={eval_chunk_rows} (manual)")
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
            )
            print(f"[Train {train_years}] eval chunk_rows={eval_chunk_rows} (auto)")

        if config.training.enable_torch_compile:
            if not hasattr(torch, "compile"):
                raise RuntimeError("Aggressive compile mode requested, but torch.compile is unavailable in this runtime.")

            can_compile, reason = _can_enable_torch_compile(device)
            if not can_compile:
                raise RuntimeError(f"Aggressive compile mode requested, but compile precheck failed: {reason}")

            try:
                compiled_train_model = torch.compile(model, mode="max-autotune")
                print(f"[Train {train_years}] torch.compile enabled (mode=max-autotune, {reason})")
            except Exception as e:
                raise RuntimeError(
                    f"Aggressive compile mode requested and compile failed. No eager fallback is allowed. Root cause: {e}"
                ) from e

        epoch_pbar = tqdm(
            range(start_epoch, config.training.epochs + 1),
            desc=f"Train {train_years} Epochs",
            leave=True,
            dynamic_ncols=True,
        )
        val_backtest: BacktestResultTensor | None = None
        for epoch in epoch_pbar:
            train_loss = _train_epoch_tensor(
                compiled_train_model,
                train_x,
                train_returns,
                train_masks,
                train_sample_mask,
                optimizer,
                scaler,
                batch_size=train_batch_size,
                device=device,
                amp_dtype=amp_dtype,
                non_blocking=non_blocking,
                fee_per_side=config.trading.fee_per_side,
                gamma_sharpe=config.evaluation.gamma_sharpe,
                gamma_turnover=config.evaluation.gamma_turnover,
                cash_symbol_mask=cash_symbol_mask,
            )

            val_backtest, _, _ = _evaluate_tensor_batch(
                model,
                combined_val_x,
                combined_val_returns,
                combined_val_masks,
                combined_val_bench,
                device,
                amp_dtype,
                non_blocking,
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
                cash_symbol_mask=cash_symbol_mask,
            )

            val_losses: list[float] = []
            for index, (fold_id, context) in enumerate(fold_contexts.items()):
                start = val_offsets[index]
                end = val_offsets[index + 1]
                val_returns_slice = combined_val_returns[start:end].to(device=device, non_blocking=non_blocking)
                val_masks_slice = combined_val_masks[start:end].to(device=device, non_blocking=non_blocking)
                val_loss_tensor = sharpe_aware_loss(
                    val_backtest.weights_history[start:end],
                    val_returns_slice,
                    val_masks_slice,
                    fee_per_side=config.trading.fee_per_side,
                    gamma_sharpe=config.evaluation.gamma_sharpe,
                    gamma_turnover=config.evaluation.gamma_turnover,
                    cash_symbol_mask=cash_symbol_mask,
                )
                val_loss = float(val_loss_tensor.detach().cpu())
                val_losses.append(val_loss)
                if val_loss < float(context["best_val_loss"]):
                    context["best_val_loss"] = val_loss
                    _save_fold_checkpoint(
                        context["checkpoint_best_path"],
                        fold=context["fold"],
                        epoch=epoch,
                        best_val_loss=val_loss,
                        model=model,
                        optimizer=optimizer,
                        scaler=scaler,
                    )

                _save_fold_checkpoint(
                    context["checkpoint_last_path"],
                    fold=context["fold"],
                    epoch=epoch,
                    best_val_loss=float(context["best_val_loss"]),
                    model=model,
                    optimizer=optimizer,
                    scaler=scaler,
                )

            _save_group_checkpoint(
                group_checkpoint_path,
                train_years=train_years,
                epoch=epoch,
                model=model,
                optimizer=optimizer,
                scaler=scaler,
            )

            if val_losses:
                epoch_pbar.set_postfix(
                    {
                        "train_loss": f"{train_loss:.6f}",
                        "val_mean": f"{float(np.mean(val_losses)):.6f}",
                        "best_val": f"{min(float(c['best_val_loss']) for c in fold_contexts.values()):.6f}",
                    }
                )

        # In eval-only mode (e.g., resumed checkpoint already beyond max epochs),
        # the epoch loop does not run; compute validation backtest once for reporting.
        if val_backtest is None:
            val_backtest, _, _ = _evaluate_tensor_batch(
                model,
                combined_val_x,
                combined_val_returns,
                combined_val_masks,
                combined_val_bench,
                device,
                amp_dtype,
                non_blocking,
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
                cash_symbol_mask=cash_symbol_mask,
            )
        if val_backtest is None:
            raise RuntimeError("Validation backtest is unavailable in eval stage.")

        for index, (fold_id, context) in enumerate(fold_contexts.items()):
            fold = context["fold"]
            fold_dir = context["fold_dir"]
            best_checkpoint_path = context["checkpoint_best_path"]
            if best_checkpoint_path.exists():
                checkpoint = _load_checkpoint(best_checkpoint_path)
                _load_state_dict(model, checkpoint["model_state_dict"])
                best_val_loss = float(checkpoint.get("best_val_loss", context["best_val_loss"]))
            else:
                best_val_loss = float(context["best_val_loss"])

            test_x, test_returns, test_masks, test_bench = _dataset_to_tensors(context["test_ds"])
            test_x, test_returns, test_masks, test_bench = _prepare_split_tensors(
                test_x,
                test_returns,
                test_masks,
                test_bench,
                device,
                non_blocking,
            )
            test_bt_t, test_ic, _ = _evaluate_tensor_batch(
                model,
                test_x,
                test_returns,
                test_masks,
                test_bench,
                device,
                amp_dtype,
                non_blocking,
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
                cash_symbol_mask=cash_symbol_mask,
            )

            start = val_offsets[index]
            end = val_offsets[index + 1]

            val_ic = ic_summary(
                compute_ic_series_torch(
                    val_backtest.weights_history[start:end],
                    combined_val_returns[start:end].to(device=device, non_blocking=non_blocking),
                    combined_val_masks[start:end].to(device=device, non_blocking=non_blocking),
                ).cpu().numpy()
            )
            val_met = _compute_metrics_from_tensors(
                val_backtest.strategy_returns[start:end],
                val_backtest.benchmark_returns[start:end],
                val_backtest.turnovers[start:end],
            )

            test_dates = panel.dates[context["test_ds"].valid_indices]
            test_open_prices = panel.open_prices[context["test_ds"].valid_indices]
            test_close_prices = panel.close_prices[context["test_ds"].valid_indices]
            test_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt_t.weights_history.detach().cpu().numpy(),
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                benchmark_returns=test_bench.detach().cpu().numpy(),
                initial_capital=1_000_000.0,
                buy_fee_rate=0.001425,
                sell_fee_rate=0.002925,
                lot_size=1000,
                open_prices=test_open_prices,
                close_prices=test_close_prices,
                symbols=panel.symbols,
                dates=test_dates,
            )
            test_met = compute_metrics(test_bt)

            print(f"\n  [val]   IC={val_ic['ic_mean']:+.4f}  IC_IR={val_ic['ic_ir']:+.4f}  sharpe={val_met['sharpe']:+.4f}  cum_ret={val_met['cumulative_return']:+.4f}  excess={val_met['excess_return_vs_universe_average']:+.4f}")
            print(f"  [test]  IC={test_ic['ic_mean']:+.4f}  IC_IR={test_ic['ic_ir']:+.4f}  sharpe={test_met['sharpe']:+.4f}  cum_ret={test_met['cumulative_return']:+.4f}  excess={test_met['excess_return_vs_universe_average']:+.4f}")

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
            )
            results_by_fold[fold.fold_id] = fold_result

            torch.save(_state_dict_for_save(model), _model_path(fold_dir))
            with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
                json.dump(asdict(fold_result), f, indent=2)

            _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
            report = generate_annual_report(test_bt, test_dates)
            print("\n" + report)
            with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
                f.write(report)

            plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
            _save_holdings_csv(fold_dir / "holdings.csv", holdings_records)

            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]