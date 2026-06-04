from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all epoch loss and timing curves by default."
    )
    parser.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root directory to recursively search for epoch_curve.jsonl when --curve-file is omitted.",
    )
    parser.add_argument(
        "--curve-file",
        type=str,
        default="",
        help="Optional single epoch_curve.jsonl. If omitted, plot every curve under --artifacts-root.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output image path. Default: <curve_dir>/epoch_curve_every100.png",
    )
    parser.add_argument(
        "--timing-output",
        type=str,
        default="",
        help="Timing image path. Default: <curve_dir>/epoch_timing_everyN.png",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="Sampling interval by epoch (default: 1)",
    )
    return parser.parse_args()


def _find_curve_files(root: Path) -> list[Path]:
    candidates = sorted(root.rglob("epoch_curve.jsonl"), key=lambda p: (p.parent.as_posix(), p.stat().st_mtime))
    if not candidates:
        raise FileNotFoundError(f"No epoch_curve.jsonl found under {root}")
    return candidates


def _load_curve(curve_path: Path) -> list[dict]:
    rows: list[dict] = []
    with curve_path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        raise ValueError(f"Curve file is empty: {curve_path}")
    return rows


def _sample_rows(rows: list[dict], interval: int) -> list[dict]:
    interval = max(1, int(interval))
    sampled = [row for row in rows if int(row.get("epoch", 0)) % interval == 0]
    if rows and int(rows[-1].get("epoch", 0)) % interval != 0:
        sampled.append(rows[-1])
    # Deduplicate by epoch while preserving order.
    unique: dict[int, dict] = {}
    for row in sampled:
        unique[int(row.get("epoch", 0))] = row
    return [unique[k] for k in sorted(unique.keys())]


def _to_float_array(rows: list[dict], key: str) -> np.ndarray:
    values: list[float] = []
    for row in rows:
        val = row.get(key)
        if val is None:
            values.append(np.nan)
        else:
            values.append(float(val))
    return np.asarray(values, dtype=np.float64)


def _has_finite(values: np.ndarray) -> bool:
    return bool(np.isfinite(values).any())


def _plot_loss_curve(rows: list[dict], curve_path: Path, output_path: Path, interval: int) -> None:
    epochs = np.asarray([int(row.get("epoch", 0)) for row in rows], dtype=np.int64)
    train_loss = _to_float_array(rows, "train_loss")
    val_mean = _to_float_array(rows, "val_mean")
    test_mean = _to_float_array(rows, "test_mean")
    all_loss_values = np.concatenate([train_loss, val_mean, test_mean])
    finite_loss_values = all_loss_values[np.isfinite(all_loss_values)]

    fig, ax = plt.subplots(figsize=(12, 6), dpi=130)
    ax.plot(epochs, train_loss, marker="o", linewidth=1.8, markersize=4, label="train_loss")
    ax.plot(epochs, val_mean, marker="s", linewidth=1.8, markersize=4, label="val_mean")
    ax.plot(epochs, test_mean, marker="^", linewidth=1.8, markersize=4, label="test_mean")
    ax.set_title(f"Loss Curves (sample every {max(1, int(interval))} epochs)")
    ax.set_xlabel("Epoch")
    if finite_loss_values.size == 0:
        ax.set_ylabel("Loss")
    elif np.any(finite_loss_values <= 0.0):
        ax.set_ylabel("Loss (symlog)")
        ax.set_yscale("symlog", linthresh=1e-3)
    else:
        ax.set_ylabel("Loss")
        ax.set_yscale("log")
    ax.grid(True, alpha=0.25)
    ax.legend()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)

    print(f"curve_file: {curve_path}")
    print(f"output: {output_path}")
    print(f"points: {len(epochs)}")


def _plot_timing_curve(rows: list[dict], curve_path: Path, output_path: Path, interval: int) -> None:
    epochs = np.asarray([int(row.get("epoch", 0)) for row in rows], dtype=np.int64)
    batch_series = [
        ("train_fetch_ms_per_batch", "fetch"),
        ("train_transfer_ms_per_batch", "transfer"),
        ("train_forward_ms_per_batch", "forward total"),
        ("train_model_forward_ms_per_batch", "model forward"),
        ("train_factor_aug_ms_per_batch", "factor aug"),
        ("train_loss_ms_per_batch", "loss"),
        ("train_backward_total_ms_per_batch", "backward total"),
        ("train_grad_ms_per_batch", "grad"),
        ("train_clip_ms_per_batch", "clip"),
        ("train_finite_check_ms_per_batch", "finite check"),
        ("train_step_ms_per_batch", "optimizer"),
        ("train_total_ms_per_batch", "train total"),
    ]
    epoch_series = [
        ("train_total_s", "train"),
        ("val_eval_s", "val eval"),
        ("val_transfer_s", "val transfer"),
        ("val_forward_s", "val forward"),
        ("val_model_forward_s", "val model"),
        ("val_loss_compute_s", "val loss compute"),
        ("val_backtest_s", "val backtest"),
        ("val_ic_s", "val ic"),
        ("val_metrics_reduce_s", "val reduce"),
        ("val_concat_s", "val concat"),
        ("val_loss_s", "val loss"),
        ("val_metrics_s", "val metrics"),
        ("test_curve_s", "curve test"),
        ("test_curve_loss_s", "test loss"),
        ("test_curve_transfer_s", "test transfer"),
        ("test_curve_forward_s", "test forward"),
        ("test_curve_model_forward_s", "test model"),
        ("test_curve_loss_compute_s", "test loss compute"),
        ("test_curve_backtest_s", "test backtest"),
        ("test_curve_ic_s", "test ic"),
        ("test_curve_metrics_reduce_s", "test reduce"),
        ("test_curve_concat_s", "test concat"),
        ("fold_checkpoint_save_s", "fold ckpt"),
        ("group_checkpoint_save_s", "group ckpt"),
        ("checkpoint_save_s", "checkpoint"),
        ("scheduler_s", "scheduler"),
        ("progress_update_s", "progress"),
        ("curve_record_s", "curve record"),
        ("scalar_sync_s", "scalar sync"),
        ("cuda_sync_s", "cuda sync"),
        ("gc_s", "gc"),
        ("epoch_unattributed_s", "other"),
        ("epoch_wall_s", "epoch wall"),
        ("epoch_total_s", "epoch total"),
    ]

    has_timing = any(_has_finite(_to_float_array(rows, key)) for key, _ in batch_series + epoch_series)
    if not has_timing:
        print(f"timing skipped: {curve_path} has no timing fields yet")
        return

    synced = _to_float_array(rows, "timing_synchronized")
    sync_note = "CUDA synchronized" if _has_finite(synced) and np.nanmin(synced) >= 1.0 else "CUDA async/approx"
    fig, (ax_batch, ax_epoch) = plt.subplots(2, 1, figsize=(13, 9), dpi=130, sharex=True)
    for key, label in batch_series:
        values = _to_float_array(rows, key)
        if _has_finite(values):
            ax_batch.plot(epochs, values, linewidth=1.6, label=label)
    ax_batch.set_title(
        f"Average Train Step Time ({sync_note}, sample every {max(1, int(interval))} epochs)"
    )
    ax_batch.set_ylabel("ms / batch")
    ax_batch.grid(True, alpha=0.25)
    ax_batch.legend(ncol=4, fontsize=8)

    for key, label in epoch_series:
        values = _to_float_array(rows, key)
        if _has_finite(values):
            ax_epoch.plot(epochs, values, linewidth=1.6, label=label)
    ax_epoch.set_title("Epoch-Level Timing")
    ax_epoch.set_xlabel("Epoch")
    ax_epoch.set_ylabel("seconds")
    ax_epoch.grid(True, alpha=0.25)
    ax_epoch.legend(ncol=3, fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    print(f"timing_output: {output_path}")


def main() -> None:
    args = _parse_args()

    if args.curve_file:
        curve_paths = [Path(args.curve_file)]
    else:
        curve_paths = _find_curve_files(Path(args.artifacts_root))
        print(f"plotting all curve files under {args.artifacts_root}: {len(curve_paths)} found")

    for curve_path in curve_paths:
        rows = _load_curve(curve_path)
        rows = _sample_rows(rows, args.interval)

        if args.output and len(curve_paths) == 1:
            output_path = Path(args.output)
        else:
            output_path = curve_path.parent / f"epoch_curve_every{max(1, int(args.interval))}.png"
        _plot_loss_curve(rows, curve_path, output_path, args.interval)

        if args.timing_output and len(curve_paths) == 1:
            timing_output_path = Path(args.timing_output)
        else:
            timing_output_path = curve_path.parent / f"epoch_timing_every{max(1, int(args.interval))}.png"
        _plot_timing_curve(rows, curve_path, timing_output_path, args.interval)


if __name__ == "__main__":
    main()
