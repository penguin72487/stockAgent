from __future__ import annotations

import json
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

from stockagent.backtest.report import compute_metrics, generate_annual_report, plot_annual_performance, plot_equity_curve, plot_equity_curve_log
from stockagent.backtest.simulator import BacktestResultTensor, run_backtest_torch
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


def _build_loader(dataset: CrossSectionalDataset, batch_size: int, shuffle: bool, config: ExperimentConfig, device: torch.device) -> DataLoader:
    workers = config.training.num_workers
    loader_kwargs: dict = {
        "dataset": dataset,
        "batch_size": batch_size,
        "shuffle": shuffle,
        "num_workers": workers,
        "pin_memory": (device.type == "cuda"),
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


def run_training(panel: PanelData, folds: Iterable[WalkForwardFold], config: ExperimentConfig, output_dir: str | Path) -> list[FoldResult]:
    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    if config.environment.use_tensor_cores and device.type == "cuda":
        torch.set_float32_matmul_precision("high")
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.backends.cudnn.benchmark = True
    print(
        f"[runtime] device={device.type} "
        f"cuda_available={torch.cuda.is_available()} "
        f"num_gpus={torch.cuda.device_count()}"
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results: list[FoldResult] = []
    buffer_pool = PredictionBufferPool()
    fold_list = list(folds)
    for fold in tqdm(fold_list, desc="Folds", unit="fold"):
        print(f"\n{'='*60}")
        print(f"[Fold {fold.fold_id}]  train={fold.train_years}  val={fold.val_years}  test={fold.test_years}")
        print(f"{'='*60}")

        train_ds = CrossSectionalDataset(panel, fold.train_indices, config.training.lookback)
        val_ds = CrossSectionalDataset(panel, fold.val_indices, config.training.lookback)
        test_ds = CrossSectionalDataset(panel, fold.test_indices, config.training.lookback)

        if len(test_ds) == 0:
            print(f"[Fold {fold.fold_id}] skip: empty test split after lookback filtering")
            continue

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

            # ✅ FIXED: Use binary search for adaptive batch size
            # Create a temporary model for batch size search
            temp_model = CrossSectionalMLP(
                lookback=config.training.lookback,
                num_features=len(panel.feature_names),
                num_symbols=panel.num_symbols,
                hidden_dim=config.training.hidden_dim,
                dropout=config.training.dropout,
            ).to(device)
            
            # Binary search for training batch size
            print(f"[Fold {fold.fold_id}] searching optimal train batch size...")
            temp_train_loader = _build_loader(train_ds, 1, False, config, device)
            train_batch_size = find_optimal_batch_size(
                model=temp_model,
                sample_loader=temp_train_loader,
                device=device,
                amp_dtype=amp_dtype,
                target_vram_fraction=config.training.target_vram_fraction,
                vram_budget_gb=config.training.vram_budget_gb,
            )
            
            # Use smaller batch sizes for validation/test (inference is less memory intensive)
            val_batch_size = min(train_batch_size * 2, len(val_ds))
            test_batch_size = min(train_batch_size * 2, len(test_ds))
        else:
            train_batch_size = _split_batch_size(len(train_ds), config.training.batch_size_train)
            val_batch_size = _split_batch_size(len(val_ds), config.training.batch_size_eval)
            test_batch_size = _split_batch_size(len(test_ds), config.training.batch_size_eval)

        print(
            f"[Fold {fold.fold_id}] using batch_size "
            f"train={train_batch_size} val={val_batch_size} test={test_batch_size}"
        )
        train_loader = _build_loader(train_ds, train_batch_size, False, config, device)
        val_loader = _build_loader(val_ds, val_batch_size, False, config, device)
        test_loader = _build_loader(test_ds, test_batch_size, False, config, device)

        model = CrossSectionalMLP(
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
            hidden_dim=config.training.hidden_dim,
            dropout=config.training.dropout,
        ).to(device)

        # Compile model for speed (PyTorch 2.0+) when environment supports it.
        if hasattr(torch, "compile"):
            can_compile, reason = _can_enable_torch_compile(device)
            if can_compile:
                try:
                    model = torch.compile(model, mode="reduce-overhead")
                    print(f"[Fold {fold.fold_id}] torch.compile enabled (mode=reduce-overhead, {reason})")
                except Exception as e:
                    print(f"[Fold {fold.fold_id}] torch.compile failed, falling back to eager: {e}")
            else:
                print(f"[Fold {fold.fold_id}] torch.compile skipped: {reason}")

        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.training.learning_rate,
            weight_decay=config.training.weight_decay,
        )
        scaler = GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)

        best_state: dict | None = None
        best_val_loss = float("inf")

        epoch_pbar = tqdm(range(1, config.training.epochs + 1), desc=f"Fold {fold.fold_id} Epochs", leave=True, dynamic_ncols=True)
        for epoch in epoch_pbar:
            train_loss = _train_epoch(
                model,
                train_loader,
                optimizer,
                scaler,
                device,
                amp_dtype,
                config.training.non_blocking_transfer,
                config.trading.fee_per_side,
                config.evaluation.gamma_sharpe,
                config.evaluation.gamma_turnover,
            )
            val_loss = _eval_val_loss(
                model,
                val_loader,
                device,
                amp_dtype,
                config.training.non_blocking_transfer,
                config.trading.fee_per_side,
                config.evaluation.gamma_sharpe,
                config.evaluation.gamma_turnover,
            )
            epoch_pbar.set_postfix({
                "train_loss": f"{train_loss:.6f}",
                "val_loss": f"{val_loss:.6f}",
                "best": f"{best_val_loss:.6f}",
            })
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}

        if best_state is not None:
            model.load_state_dict(best_state)

        val_bt_t, val_ic, val_met = _evaluate_split_torch(
            model,
            val_loader,
            device,
            amp_dtype,
            config.training.non_blocking_transfer,
            config.trading.fee_per_side,
            buffer_pool,
        )

        test_bt_t, test_ic, test_met = _evaluate_split_torch(
            model,
            test_loader,
            device,
            amp_dtype,
            config.training.non_blocking_transfer,
            config.trading.fee_per_side,
            buffer_pool,
        )

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
        results.append(fold_result)

        fold_dir = output_path / f"fold_{fold.fold_id:02d}"
        fold_dir.mkdir(parents=True, exist_ok=True)
        torch.save(model.state_dict(), fold_dir / "model.pt")
        with (fold_dir / "metrics.json").open("w", encoding="utf-8") as f:
            json.dump(asdict(fold_result), f, indent=2)

        test_dates = panel.dates[test_ds.valid_indices]
        test_bt = test_bt_t.to_numpy()
        report = generate_annual_report(test_bt, test_dates)
        print("\n" + report)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)

        plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")

    return results