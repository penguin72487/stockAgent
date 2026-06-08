from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np


_PLOT_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#4c78a8",
    "#f58518",
    "#54a24b",
    "#e45756",
    "#72b7b2",
    "#b279a2",
    "#ff9da6",
    "#9d755d",
    "#bab0ac",
]


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
    parser.add_argument(
        "--preprocess-device",
        type=str,
        default=os.environ.get("STOCKAGENT_PLOT_PREPROCESS_DEVICE", "auto"),
        choices=("auto", "cpu", "cuda"),
        help="Device for numeric preprocessing when using Matplotlib fallback.",
    )
    parser.add_argument(
        "--raster-backend",
        type=str,
        default=os.environ.get("STOCKAGENT_PLOT_BACKEND", "auto"),
        choices=("auto", "matplotlib", "rapids_datashader"),
        help=(
            "Raster backend. auto uses Matplotlib for epoch JSON curves; "
            "use rapids_datashader explicitly for GPU raster."
        ),
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


def _resolve_preprocess_device(requested: str) -> str:
    requested = str(requested).strip().lower()
    if requested == "cpu":
        return "cpu"
    try:
        import torch
    except Exception:
        if requested == "cuda":
            raise RuntimeError("preprocess-device=cuda requested but torch is unavailable")
        return "cpu"
    cuda_available = bool(torch.cuda.is_available())
    if requested == "cuda" and not cuda_available:
        raise RuntimeError("preprocess-device=cuda requested but CUDA is unavailable")
    if requested == "auto":
        return "cuda" if cuda_available else "cpu"
    return requested


def _array_from_values(values: list[float | int], dtype: np.dtype, preprocess_device: str) -> np.ndarray:
    if preprocess_device != "cuda":
        return np.asarray(values, dtype=dtype)
    import torch

    if np.issubdtype(dtype, np.integer):
        tensor = torch.as_tensor(values, dtype=torch.int64, device="cuda")
    else:
        tensor = torch.as_tensor(values, dtype=torch.float64, device="cuda")
    return tensor.detach().cpu().numpy().astype(dtype, copy=False)


def _epoch_array(rows: list[dict], preprocess_device: str) -> np.ndarray:
    values = [int(row.get("epoch", 0)) for row in rows]
    return _array_from_values(values, np.dtype(np.int64), preprocess_device)


def _to_float_array(rows: list[dict], key: str, preprocess_device: str = "cpu") -> np.ndarray:
    values: list[float] = []
    for row in rows:
        val = row.get(key)
        if val is None:
            values.append(np.nan)
        else:
            values.append(float(val))
    return _array_from_values(values, np.dtype(np.float64), preprocess_device)


def _has_finite(values: np.ndarray) -> bool:
    return bool(np.isfinite(values).any())


def _resolve_raster_backend(requested: str) -> str:
    requested = str(requested).strip().lower()
    if requested == "auto":
        return "matplotlib"
    if requested == "matplotlib":
        return "matplotlib"
    if requested == "rapids_datashader":
        from stockagent.backtest.gpu_plot import rapids_datashader_available

        if not rapids_datashader_available(require_cuda=True):
            raise RuntimeError("raster-backend=rapids_datashader requested but RAPIDS Datashader is unavailable")
        return "rapids_datashader"
    return "matplotlib"


def _matplotlib_pyplot():
    import matplotlib.pyplot as plt

    return plt


def _plot_loss_curve(
    rows: list[dict],
    curve_path: Path,
    output_path: Path,
    interval: int,
    preprocess_device: str,
) -> None:
    plt = _matplotlib_pyplot()
    epochs = _epoch_array(rows, preprocess_device)
    train_loss = _to_float_array(rows, "train_loss", preprocess_device)
    val_mean = _to_float_array(rows, "val_mean", preprocess_device)
    test_mean = _to_float_array(rows, "test_mean", preprocess_device)
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


def _plot_loss_curve_datashader(
    rows: list[dict],
    curve_path: Path,
    output_path: Path,
    interval: int,
) -> None:
    from stockagent.backtest.gpu_plot import save_line_series_datashader

    epochs = np.asarray([int(row.get("epoch", 0)) for row in rows], dtype=np.float64)
    train_loss = _to_float_array(rows, "train_loss", "cpu")
    val_mean = _to_float_array(rows, "val_mean", "cpu")
    test_mean = _to_float_array(rows, "test_mean", "cpu")
    all_loss_values = np.concatenate([train_loss, val_mean, test_mean])
    finite_loss_values = all_loss_values[np.isfinite(all_loss_values)]
    y_label = "Loss"
    if finite_loss_values.size > 0 and np.all(finite_loss_values > 0.0):
        train_loss = np.log10(train_loss)
        val_mean = np.log10(val_mean)
        test_mean = np.log10(test_mean)
        y_label = "log10 Loss"
    save_line_series_datashader(
        [
            ("train_loss", epochs, train_loss, "#1f77b4"),
            ("val_mean", epochs, val_mean, "#ff7f0e"),
            ("test_mean", epochs, test_mean, "#2ca02c"),
        ],
        output_path=output_path,
        title=f"Loss Curves (RAPIDS Datashader, sample every {max(1, int(interval))} epochs)",
        y_label=y_label,
        width=1500,
        height=720,
    )
    print(f"curve_file: {curve_path}")
    print(f"output: {output_path}")
    print(f"points: {len(epochs)}")


def _plot_timing_curve(
    rows: list[dict],
    curve_path: Path,
    output_path: Path,
    interval: int,
    preprocess_device: str,
) -> None:
    plt = _matplotlib_pyplot()
    epochs = _epoch_array(rows, preprocess_device)
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

    has_timing = any(
        _has_finite(_to_float_array(rows, key, preprocess_device))
        for key, _ in batch_series + epoch_series
    )
    if not has_timing:
        print(f"timing skipped: {curve_path} has no timing fields yet")
        return

    synced = _to_float_array(rows, "timing_synchronized", preprocess_device)
    sync_note = "CUDA synchronized" if _has_finite(synced) and np.nanmin(synced) >= 1.0 else "CUDA async/approx"
    fig, (ax_batch, ax_epoch) = plt.subplots(2, 1, figsize=(13, 9), dpi=130, sharex=True)
    for key, label in batch_series:
        values = _to_float_array(rows, key, preprocess_device)
        if _has_finite(values):
            ax_batch.plot(epochs, values, linewidth=1.6, label=label)
    ax_batch.set_title(
        f"Average Train Step Time ({sync_note}, sample every {max(1, int(interval))} epochs)"
    )
    ax_batch.set_ylabel("ms / batch")
    ax_batch.grid(True, alpha=0.25)
    ax_batch.legend(ncol=4, fontsize=8)

    for key, label in epoch_series:
        values = _to_float_array(rows, key, preprocess_device)
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


def _timing_series_definitions() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
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
    return batch_series, epoch_series


def _plot_timing_curve_datashader(
    rows: list[dict],
    curve_path: Path,
    output_path: Path,
    interval: int,
) -> None:
    from stockagent.backtest.gpu_plot import save_two_panel_line_series_datashader

    batch_series, epoch_series = _timing_series_definitions()
    epochs = np.asarray([int(row.get("epoch", 0)) for row in rows], dtype=np.float64)
    has_timing = any(_has_finite(_to_float_array(rows, key, "cpu")) for key, _ in batch_series + epoch_series)
    if not has_timing:
        print(f"timing skipped: {curve_path} has no timing fields yet")
        return
    synced = _to_float_array(rows, "timing_synchronized", "cpu")
    sync_note = "CUDA synchronized" if _has_finite(synced) and np.nanmin(synced) >= 1.0 else "CUDA async/approx"
    top = []
    bottom = []
    for idx, (key, label) in enumerate(batch_series):
        values = _to_float_array(rows, key, "cpu")
        if _has_finite(values):
            top.append((label, epochs, values, _PLOT_COLORS[idx % len(_PLOT_COLORS)]))
    for idx, (key, label) in enumerate(epoch_series):
        values = _to_float_array(rows, key, "cpu")
        if _has_finite(values):
            bottom.append((label, epochs, values, _PLOT_COLORS[idx % len(_PLOT_COLORS)]))
    save_two_panel_line_series_datashader(
        top,
        bottom,
        output_path=output_path,
        title_top=f"Average Train Step Time ({sync_note}, RAPIDS Datashader, sample every {max(1, int(interval))} epochs)",
        title_bottom="Epoch-Level Timing (RAPIDS Datashader)",
        y_label_top="ms / batch",
        y_label_bottom="seconds",
        width=1500,
        panel_height=520,
    )
    print(f"timing_output: {output_path}")


def main() -> None:
    args = _parse_args()
    preprocess_device = _resolve_preprocess_device(args.preprocess_device)
    raster_backend = _resolve_raster_backend(args.raster_backend)
    print(f"plot_preprocess_device: {preprocess_device}")
    print(f"plot_raster_backend: {raster_backend}")

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
        if raster_backend == "rapids_datashader":
            _plot_loss_curve_datashader(rows, curve_path, output_path, args.interval)
        else:
            _plot_loss_curve(rows, curve_path, output_path, args.interval, preprocess_device)

        if args.timing_output and len(curve_paths) == 1:
            timing_output_path = Path(args.timing_output)
        else:
            timing_output_path = curve_path.parent / f"epoch_timing_every{max(1, int(args.interval))}.png"
        if raster_backend == "rapids_datashader":
            _plot_timing_curve_datashader(rows, curve_path, timing_output_path, args.interval)
        else:
            _plot_timing_curve(rows, curve_path, timing_output_path, args.interval, preprocess_device)


if __name__ == "__main__":
    main()
