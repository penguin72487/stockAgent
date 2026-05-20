from __future__ import annotations

import json
import logging
import os
import shutil
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

from stockagent.backtest.report import (
    compute_god_mode_returns,
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
    valid_indices = torch.as_tensor(dataset.valid_indices, dtype=torch.long)
    if dataset.lookback == 1:
        x = dataset.features_t[valid_indices].unsqueeze(1)
    else:
        x = torch.stack([dataset[i]["x"] for i in range(len(dataset))], dim=0)
    returns = dataset.future_log_returns_t[valid_indices]
    masks = dataset.tradable_mask_t[valid_indices]
    bench = dataset.benchmark_t[valid_indices]
    return x, returns, masks, bench


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
    amp_dtype: torch.dtype,
    fee_per_side: float,
    chunk_rows: int,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    model.eval()
    weights_chunks: list[torch.Tensor] = []
    returns_chunks: list[torch.Tensor] = []
    masks_chunks: list[torch.Tensor] = []
    benchmark_chunks: list[torch.Tensor] = []
    with torch.inference_mode():
        for start in range(0, x.size(0), chunk_rows):
            end = min(start + chunk_rows, x.size(0))
            x_chunk = x[start:end].to(device=device, non_blocking=True)
            returns_chunk = future_log_returns[start:end].to(device=device, non_blocking=True)
            mask_chunk = tradable_mask[start:end].to(device=device, non_blocking=True)
            bench_chunk = benchmark[start:end].to(device=device, non_blocking=True)
            with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
                weights_chunk = model(x_chunk, mask_chunk)

            weights_chunks.append(weights_chunk.float().cpu())
            returns_chunks.append(returns_chunk.float().cpu())
            masks_chunks.append(mask_chunk.cpu())
            benchmark_chunks.append(bench_chunk.float().cpu())

        weights = torch.cat(weights_chunks, dim=0)
        future_log_returns_cpu = torch.cat(returns_chunks, dim=0)
        tradable_mask_cpu = torch.cat(masks_chunks, dim=0)
        benchmark_cpu = torch.cat(benchmark_chunks, dim=0)

        backtest = run_backtest_torch(weights, future_log_returns_cpu, tradable_mask_cpu, benchmark_cpu, fee_per_side)
        ic = ic_summary(compute_ic_series_torch(weights, future_log_returns_cpu, tradable_mask_cpu).cpu().numpy())
        metrics = compute_metrics(backtest.to_numpy())
    return backtest, ic, metrics


def _auto_chunk_rows(
    model: nn.Module,
    x: torch.Tensor,
    tradable_mask: torch.Tensor,
    device: torch.device,
    amp_dtype: torch.dtype,
    target_vram_fraction: float,
) -> int:
    if device.type != "cuda":
        return max(1, min(256, int(x.size(0))))

    total_rows = int(x.size(0))
    if total_rows <= 1:
        return 1

    torch.cuda.empty_cache()
    free_mem, _ = torch.cuda.mem_get_info(device)

    # Measure per-row incremental VRAM cost on a tiny probe, then size chunk
    # from currently free VRAM. This avoids OOM-driven probing.
    probe_rows = max(1, min(32, total_rows))
    torch.cuda.reset_peak_memory_stats(device)
    base_alloc = torch.cuda.memory_allocated(device)
    with torch.inference_mode():
        x_probe = x[:probe_rows].to(device=device, non_blocking=True)
        mask_probe = tradable_mask[:probe_rows].to(device=device, non_blocking=True)
        with autocast(device_type=device.type, enabled=True, dtype=amp_dtype):
            _ = model(x_probe, mask_probe)
    torch.cuda.synchronize(device)
    peak_alloc = torch.cuda.max_memory_allocated(device)

    incremental_bytes = max(1, peak_alloc - base_alloc)
    bytes_per_row = max(1, incremental_bytes // probe_rows)

    # Keep headroom for allocator/workspace fluctuations.
    usable_bytes = int(free_mem * target_vram_fraction * 0.9)
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


def _resolve_amp_dtype(amp_dtype: str) -> torch.dtype:
    if amp_dtype == "bf16":
        return torch.bfloat16
    if amp_dtype == "fp16":
        return torch.float16
    raise ValueError(f"Unsupported amp dtype: {amp_dtype}")


def _can_enable_torch_compile(device: torch.device) -> tuple[bool, str]:
    """Return whether torch.compile is safe to enable in current environment."""
    if device.type != "cuda":
        return False, "torch.compile is only enabled for CUDA in this project"

    # Inductor+Triton on CUDA needs a host C compiler at runtime.
    compiler = os.environ.get("CC") or shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    if not compiler:
        return False, "no host C compiler found (set CC or install gcc/clang)"

    return True, f"compiler={compiler}"


def find_optimal_batch_size(
    model: nn.Module,
    sample_loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype,
    target_vram_fraction: float = 0.85,
    vram_budget_gb: float = 12.0,
) -> int:
    """
    ✅ Binary search to find maximum safe batch size.
    
    Args:
        model: Model to test
        sample_loader: DataLoader with samples
        device: GPU device
        amp_dtype: Mixed precision dtype
        target_vram_fraction: Target VRAM utilization (0.85 = 85%)
        vram_budget_gb: Total VRAM budget in GB
    
    Returns:
        Maximum safe batch size
    """
    if device.type != 'cuda':
        return len(sample_loader.dataset)
    
    # Get a single sample to estimate memory
    model.eval()
    test_batch = next(iter(sample_loader))
    test_batch = _move_batch(test_batch, device, non_blocking=True)
    
    torch.cuda.reset_peak_memory_stats()
    torch.cuda.empty_cache()
    
    with torch.inference_mode():
        with autocast(device_type='cuda', enabled=True, dtype=amp_dtype):
            _ = model(test_batch["x"], test_batch["tradable_mask"])
    
    single_sample_bytes = torch.cuda.max_memory_allocated()
    torch.cuda.reset_peak_memory_stats()
    
    max_batch_size = len(sample_loader.dataset)
    target_bytes = int(vram_budget_gb * 1024**3 * target_vram_fraction)
    estimated_max = max(1, target_bytes // max(single_sample_bytes, 1))
    
    # Binary search
    low, high = 1, min(estimated_max, max_batch_size)
    best_batch_size = 1
    
    print(f"  [batch search] single sample: {single_sample_bytes/1024**2:.1f}MB, target: {target_bytes/1024**3:.1f}GB, range: [{low}, {high}]")
    
    while low <= high:
        mid = (low + high) // 2
        
        try:
            torch.cuda.reset_peak_memory_stats()
            torch.cuda.empty_cache()
            
            # Create temporary loader for testing
            temp_loader = DataLoader(
                sample_loader.dataset,
                batch_size=mid,
                shuffle=False,
                num_workers=0,
                pin_memory=True,
            )
            
            model.train()
            test_batch = next(iter(temp_loader))
            test_batch = _move_batch(test_batch, device, non_blocking=True)
            
            with autocast(device_type='cuda', enabled=True, dtype=amp_dtype):
                logits = model(test_batch["x"], test_batch["tradable_mask"])
                loss = sharpe_aware_loss(
                    logits,
                    test_batch["future_log_returns"],
                    test_batch["tradable_mask"],
                    fee_per_side=0.0,
                )
            
            loss.backward()
            used_memory = torch.cuda.max_memory_allocated()
            
            if used_memory <= target_bytes:
                best_batch_size = mid
                low = mid + 1
                print(f"  ✅ batch_size {mid}: {used_memory/1024**3:.1f}GB OK")
            else:
                high = mid - 1
                print(f"  ❌ batch_size {mid}: {used_memory/1024**3:.1f}GB exceeds")
            
            torch.cuda.empty_cache()
            
        except RuntimeError as e:
            if "out of memory" in str(e).lower():
                high = mid - 1
                print(f"  ❌ batch_size {mid}: OOM")
            else:
                raise
    
    print(f"  [batch search] final result: {best_batch_size}")
    return best_batch_size


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


def _amp_bytes(amp_dtype: torch.dtype) -> int:
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
    amp_dtype: torch.dtype,
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
    amp_dtype: torch.dtype,
    non_blocking: bool,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
) -> float:
    model.train()
    total_loss = 0.0
    steps = 0
    
    for batch in loader:
        batch = _move_batch(batch, device, non_blocking)
        optimizer.zero_grad(set_to_none=True)
        
        with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
            weights = model(batch["x"], batch["tradable_mask"])
            loss = sharpe_aware_loss(
                weights,
                batch["future_log_returns"],
                batch["tradable_mask"],
                fee_per_side,
                gamma_sharpe=gamma_sharpe,
                gamma_turnover=gamma_turnover,
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
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    batch_size: int,
    device: torch.device,
    amp_dtype: torch.dtype,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
) -> float:
    model.train()
    total_rows = int(x.size(0))
    if total_rows == 0:
        return 0.0

    order = torch.randperm(total_rows)
    total_loss = 0.0
    steps = 0

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        idx = order[start:end]
        batch_x = x[idx].to(device=device, non_blocking=True)
        batch_ret = future_log_returns[idx].to(device=device, non_blocking=True)
        batch_mask = tradable_mask[idx].to(device=device, non_blocking=True)

        optimizer.zero_grad(set_to_none=True)
        with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
            weights = model(batch_x, batch_mask)
            loss = sharpe_aware_loss(
                weights,
                batch_ret,
                batch_mask,
                fee_per_side,
                gamma_sharpe=gamma_sharpe,
                gamma_turnover=gamma_turnover,
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
    amp_dtype: torch.dtype,
    non_blocking: bool,
    fee_per_side: float,
    gamma_sharpe: float,
    gamma_turnover: float,
) -> float:
    model.eval()
    losses: list[float] = []
    with torch.inference_mode():
        for batch in loader:
            batch = _move_batch(batch, device, non_blocking)
            with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
                weights = model(batch["x"], batch["tradable_mask"])
                loss = sharpe_aware_loss(
                    weights, 
                    batch["future_log_returns"], 
                    batch["tradable_mask"], 
                    fee_per_side=fee_per_side,
                    gamma_sharpe=gamma_sharpe,
                    gamma_turnover=gamma_turnover,
                )
            losses.append(float(loss.detach().cpu()))
    return float(np.mean(losses)) if losses else float("inf")


def _collect_predictions(
    model: nn.Module,
    loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype,
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
            with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
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
    amp_dtype: torch.dtype,
    non_blocking: bool,
    fee_per_side: float,
    buffers: PredictionBufferPool,
) -> tuple[BacktestResultTensor, dict[str, float], dict[str, float]]:
    weights, log_ret, masks, bench = _collect_predictions(
        model,
        loader,
        device,
        amp_dtype,
        non_blocking,
        buffers,
    )
    backtest = run_backtest_torch(weights, log_ret, masks, bench, fee_per_side)
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
    ann_r = float(torch.expm1(avg * 252.0).item())
    sharpe = float((avg / std * np.sqrt(252.0)).item()) if float(std.item()) > 0 else 0.0

    equity = torch.exp(torch.cumsum(r, dim=0))
    running_max = torch.cummax(equity, dim=0).values
    dd = equity / running_max.clamp_min(1e-12) - 1.0
    max_dd = float(dd.min().item()) if dd.numel() else 0.0

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "sharpe": sharpe,
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
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    if config.environment.use_tensor_cores and device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
        try:
            import torch._inductor.config as inductor_config  # type: ignore

            inductor_config.triton.cudagraph_skip_dynamic_graphs = True
            inductor_config.triton.cudagraph_dynamic_shape_warn_limit = None
        except Exception:
            pass
    print(
        f"[runtime] device={device.type} "
        f"cuda_available={torch.cuda.is_available()} "
        f"num_gpus={torch.cuda.device_count()}"
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
            ).to(device)

            print(f"[Train {train_years}] searching optimal train batch size...")
            temp_train_loader = _build_loader(train_ds, 1, False, config, device)
            train_batch_size = find_optimal_batch_size(
                model=temp_model,
                sample_loader=temp_train_loader,
                device=device,
                amp_dtype=amp_dtype,
                target_vram_fraction=config.training.target_vram_fraction,
                vram_budget_gb=config.training.vram_budget_gb,
            )
            train_batch_size = max(min_batch_size, train_batch_size)
        else:
            train_batch_size = _split_batch_size(len(train_ds), config.training.batch_size_train)

        print(f"[Train {train_years}] using batch_size train={train_batch_size}")
        train_x, train_returns, train_masks, _ = _dataset_to_tensors(train_ds)

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
        val_offsets: list[int] = [0]
        for length in val_lengths:
            val_offsets.append(val_offsets[-1] + length)

        model = CrossSectionalMLP(
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
            hidden_dim=config.training.hidden_dim,
            dropout=config.training.dropout,
        ).to(device)

        if config.training.enable_torch_compile and hasattr(torch, "compile"):
            can_compile, reason = _can_enable_torch_compile(device)
            if can_compile:
                try:
                    model = torch.compile(model, mode="reduce-overhead")
                    print(f"[Train {train_years}] torch.compile enabled (mode=reduce-overhead, {reason})")
                except Exception as e:
                    print(f"[Train {train_years}] torch.compile failed, falling back to eager: {e}")
            else:
                print(f"[Train {train_years}] torch.compile skipped: {reason}")

        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.training.learning_rate,
            weight_decay=config.training.weight_decay,
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
            eval_chunk_rows = _auto_chunk_rows(
                model=model,
                x=combined_val_x,
                tradable_mask=combined_val_masks,
                device=device,
                amp_dtype=amp_dtype,
                target_vram_fraction=config.training.target_vram_fraction,
            )
            print(f"[Train {train_years}] eval chunk_rows={eval_chunk_rows} (auto)")

        epoch_pbar = tqdm(
            range(start_epoch, config.training.epochs + 1),
            desc=f"Train {train_years} Epochs",
            leave=True,
            dynamic_ncols=True,
        )
        val_backtest: BacktestResultTensor | None = None
        for epoch in epoch_pbar:
            train_loss = _train_epoch_tensor(
                model,
                train_x,
                train_returns,
                train_masks,
                optimizer,
                scaler,
                batch_size=train_batch_size,
                device=device,
                amp_dtype=amp_dtype,
                fee_per_side=config.trading.fee_per_side,
                gamma_sharpe=config.evaluation.gamma_sharpe,
                gamma_turnover=config.evaluation.gamma_turnover,
            )

            val_backtest, _, _ = _evaluate_tensor_batch(
                model,
                combined_val_x,
                combined_val_returns,
                combined_val_masks,
                combined_val_bench,
                device,
                amp_dtype,
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
            )

            val_losses: list[float] = []
            for index, (fold_id, context) in enumerate(fold_contexts.items()):
                start = val_offsets[index]
                end = val_offsets[index + 1]
                val_loss_tensor = sharpe_aware_loss(
                    val_backtest.weights_history[start:end],
                    combined_val_returns[start:end],
                    combined_val_masks[start:end],
                    fee_per_side=config.trading.fee_per_side,
                    gamma_sharpe=config.evaluation.gamma_sharpe,
                    gamma_turnover=config.evaluation.gamma_turnover,
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
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
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
            test_bt_t, test_ic, _ = _evaluate_tensor_batch(
                model,
                test_x,
                test_returns,
                test_masks,
                test_bench,
                device,
                amp_dtype,
                config.trading.fee_per_side,
                chunk_rows=eval_chunk_rows,
            )

            start = val_offsets[index]
            end = val_offsets[index + 1]

            val_ic = ic_summary(
                compute_ic_series_torch(
                    val_backtest.weights_history[start:end],
                    combined_val_returns[start:end],
                    combined_val_masks[start:end],
                ).cpu().numpy()
            )
            val_met = _compute_metrics_from_tensors(
                val_backtest.strategy_returns[start:end],
                val_backtest.benchmark_returns[start:end],
                val_backtest.turnovers[start:end],
            )

            test_dates = panel.dates[context["test_ds"].valid_indices]
            test_close_prices = panel.close_prices[context["test_ds"].valid_indices]
            test_bt, holdings_records = run_backtest_integer_shares(
                weights=test_bt_t.weights_history.detach().cpu().numpy(),
                future_returns=test_returns.detach().cpu().numpy(),
                tradable_mask=test_masks.detach().cpu().numpy(),
                benchmark_returns=test_bench.detach().cpu().numpy(),
                initial_capital=1_000_000.0,
                buy_fee_rate=0.001425,
                sell_fee_rate=0.004425,
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

            god_returns = compute_god_mode_returns(
                test_returns.numpy(),
                test_masks.numpy(),
            )
            _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
            report = generate_annual_report(test_bt, test_dates, god_returns=god_returns)
            print("\n" + report)
            with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
                f.write(report)

            plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png", god_returns=god_returns)
            plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png", god_returns=god_returns)
            plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
            _save_holdings_csv(fold_dir / "holdings.csv", holdings_records)

            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]