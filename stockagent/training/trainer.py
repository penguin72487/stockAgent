from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import torch
from torch import nn
from torch.amp import GradScaler, autocast
from torch.utils.data import DataLoader
from tqdm import tqdm

from stockagent.backtest.report import compute_metrics, generate_annual_report, plot_annual_performance, plot_equity_curve
from stockagent.backtest.simulator import run_backtest
from stockagent.config import ExperimentConfig
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.evaluation.metrics import compute_ic_series, ic_summary
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.training.dataset import CrossSectionalDataset, collate_batch
from stockagent.training.loss import masked_mse_loss, masked_ic_loss, sharpe_aware_loss


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


def _build_loader(dataset: CrossSectionalDataset, batch_size: int, shuffle: bool, config: ExperimentConfig, device: torch.device) -> DataLoader:
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=config.training.num_workers,
        pin_memory=(device.type == "cuda"),  # only pin when GPU is present
        collate_fn=collate_batch,
    )


def _move_batch(batch: dict[str, torch.Tensor], device: torch.device, non_blocking: bool) -> dict[str, torch.Tensor]:
    return {
        key: value.to(device=device, non_blocking=non_blocking)
        for key, value in batch.items()
    }


def _train_epoch(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    scaler: GradScaler,
    device: torch.device,
    amp_dtype: torch.dtype,
    non_blocking: bool,
    loss_type: str = "mse",
    top_k: int = 20,
    fee_per_side: float = 0.001,
) -> float:
    model.train()
    total_loss = 0.0
    steps = 0
        for batch in loader:
        batch = _move_batch(batch, device, non_blocking)
        optimizer.zero_grad(set_to_none=True)
        with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
            predictions = model(batch["x"])
            if loss_type == "sharpe":
                loss = sharpe_aware_loss(
                    predictions,
                    batch["future_log_returns"],
                    batch["tradable_mask"],
                    top_k,
                    fee_per_side,
                )
            else:  # mse
                loss = masked_mse_loss(predictions, batch["y"], batch["tradable_mask"])
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
) -> float:
    model.eval()
    losses: list[float] = []
    with torch.no_grad():
          for batch in loader:
            batch = _move_batch(batch, device, non_blocking)
            with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
                predictions = model(batch["x"])
                loss = masked_mse_loss(predictions, batch["y"], batch["tradable_mask"])
            losses.append(float(loss.detach().cpu()))
    return float(np.mean(losses)) if losses else float("inf")


def _collect_predictions(
    model: nn.Module,
    loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype,
    non_blocking: bool,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Collect all (alpha_scores, future_log_returns, tradable_mask, benchmark) from a loader."""
    model.eval()
    all_scores: list[np.ndarray] = []
    all_log_ret: list[np.ndarray] = []
    all_masks: list[np.ndarray] = []
    all_bench: list[np.ndarray] = []

    with torch.no_grad():
            for batch in loader:
            batch = _move_batch(batch, device, non_blocking)
            with autocast(device_type=device.type, enabled=device.type == "cuda", dtype=amp_dtype):
                predictions = model(batch["x"])
            all_scores.append(predictions.detach().float().cpu().numpy())
            all_log_ret.append(batch["future_log_returns"].detach().float().cpu().numpy())
            all_masks.append(batch["tradable_mask"].detach().cpu().numpy())
            all_bench.append(batch["benchmark"].detach().float().cpu().numpy())

    return (
        np.concatenate(all_scores, axis=0),
        np.concatenate(all_log_ret, axis=0),
        np.concatenate(all_masks, axis=0),
        np.concatenate(all_bench, axis=0),
    )


def run_training(panel: PanelData, folds: Iterable[WalkForwardFold], config: ExperimentConfig, output_dir: str | Path) -> list[FoldResult]:
    device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    if config.environment.use_tensor_cores and device.type == "cuda":
        torch.set_float32_matmul_precision("high")
    print(
        f"[runtime] device={device.type} "
        f"cuda_available={torch.cuda.is_available()} "
        f"num_gpus={torch.cuda.device_count()}"
    )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results: list[FoldResult] = []
    fold_list = list(folds)  # Convert to list for progress bar
        for fold in fold_list:
        print(f"\n{'='*60}")
        print(f"[Fold {fold.fold_id}]  train={fold.train_years}  val={fold.val_years}  test={fold.test_years}")
        print(f"{'='*60}")

        train_ds = CrossSectionalDataset(panel, fold.train_indices, config.training.lookback)
        val_ds   = CrossSectionalDataset(panel, fold.val_indices,   config.training.lookback)
        test_ds  = CrossSectionalDataset(panel, fold.test_indices,  config.training.lookback)

        train_loader = _build_loader(train_ds, config.training.batch_size, True,  config, device)
        val_loader   = _build_loader(val_ds,   config.training.batch_size, False, config, device)
        test_loader  = _build_loader(test_ds,  config.training.batch_size, False, config, device)

        model = CrossSectionalMLP(
            lookback=config.training.lookback,
            num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,
            hidden_dim=config.training.hidden_dim,
            dropout=config.training.dropout,
        ).to(device)

        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.training.learning_rate,
            weight_decay=config.training.weight_decay,
        )
        scaler = GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)

        best_state: dict | None = None
        best_val_loss = float("inf")

            epoch_pbar = tqdm(range(1, config.training.epochs + 1), desc=f"Fold {fold.fold_id} Epochs", leave=True)
        for epoch in epoch_pbar:
            train_loss = _train_epoch(
                model,
                train_loader,
                optimizer,
                scaler,
                device,
                amp_dtype,
                config.training.non_blocking_transfer,
                config.training.loss_type,
                config.training.top_k,
                config.trading.fee_per_side,
            )
            val_loss   = _eval_val_loss(model, val_loader, device, amp_dtype, config.training.non_blocking_transfer)
            marker = " *" if val_loss < best_val_loss else ""
            epoch_pbar.set_postfix({
                "train_loss": f"{train_loss:.6f}",
                "val_loss": f"{val_loss:.6f}",
                "best": f"{best_val_loss:.6f}"
            })
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}

        if best_state is not None:
            model.load_state_dict(best_state)

        # --- val backtest + IC ---
        val_scores, val_log_ret, val_masks, val_bench = _collect_predictions(
            model, val_loader, device, amp_dtype, config.training.non_blocking_transfer
        )
        val_bt     = run_backtest(val_scores, val_log_ret, val_masks, val_bench, config.trading.fee_per_side, config.training.top_k)
        val_ic     = ic_summary(compute_ic_series(val_scores, val_log_ret, val_masks))
        val_met    = compute_metrics(val_bt)

        # --- test backtest + IC ---
        test_scores, test_log_ret, test_masks, test_bench = _collect_predictions(
            model, test_loader, device, amp_dtype, config.training.non_blocking_transfer
        )
        test_bt    = run_backtest(test_scores, test_log_ret, test_masks, test_bench, config.trading.fee_per_side, config.training.top_k)
        test_ic    = ic_summary(compute_ic_series(test_scores, test_log_ret, test_masks))
        test_met   = compute_metrics(test_bt)

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
        
        # Generate annual reports and plots for test set
        test_dates = panel.dates[fold.test_indices]
        
        # Text report
        report = generate_annual_report(test_bt, test_dates)
        print("\n" + report)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)
        
        # Equity curve plot
        try:
            plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
            plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
        except Exception as e:
            print(f"  Warning: could not generate plots: {e}")

    return results
