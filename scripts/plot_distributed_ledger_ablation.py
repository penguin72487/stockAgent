#!/usr/bin/env python3
"""Render comparable Fold-11 systems-ablation charts for the v6 ledger."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np


LABELS = {
    "baseline": "v6 control",
    "replicated_ledger": "Replicated ledger",
    "unpacked_metadata_collectives": "Unpacked metadata",
    "unpacked_scalar_collectives": "Unpacked account scalars",
    "keep_noop_collectives": "Keep no-op collectives",
    "eager_model": "Eager model",
    "eager_ledger": "Eager ledger",
    "batch64": "Global batch 64",
}

V5_LABELS = {
    "baseline": "V5 CUDA Graph",
    "regional_replay": "V5 regional replay",
    "metadata_collectives": "V5 metadata AllGather",
    "symbol_sharded_ledger": "V6 symbol-sharded ledger",
    "eager_model": "Eager model",
    "eager_ledger": "Eager ledger",
    "batch64": "Global batch 64",
    "host_panel": "Host-resident panel",
}

V5_LOCAL_LABELS = {
    "baseline": "V5 local metadata",
    "metadata_collectives": "V5 metadata AllGather",
    "symbol_sharded_ledger": "V6 symbol-sharded ledger",
    "eager_model": "Eager model",
    "eager_ledger": "Eager ledger",
    "batch64": "Global batch 64",
    "host_panel": "Host-resident panel",
}

ORDER = tuple(LABELS)
CONTROL_LABEL = "v6 control"
INK = "#20242A"
MUTED = "#6B7280"
GRID = "#D9DEE7"
BLUE = "#2F6B9A"
BLUE_LIGHT = "#AFC8DC"
GOLD = "#C28B2C"
ORANGE = "#D97745"
OLIVE = "#7A8B4B"
PINK = "#B86B82"
SERIES_COLORS = (
    "#4C78A8",
    "#F58518",
    "#B279A2",
    "#72B7B2",
    "#FF9DA6",
    "#9D755D",
    "#7A8B4B",
    "#A0A7B4",
)


def _json_lines(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _median(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return float(statistics.median(values)) if values else float("nan")


def _training_shape(run_dir: Path) -> tuple[int, int]:
    candidates = sorted(run_dir.glob("train_*/pre_epoch_timing.jsonl"))
    if len(candidates) != 1:
        return 0, 0
    train_rows = 0
    padded_rows = 0
    for row in _json_lines(candidates[0]):
        train_rows = max(train_rows, int(row.get("train_rows", 0)))
        padded_rows = max(padded_rows, int(row.get("padded_train_rows", 0)))
    return train_rows, padded_rows


def _run_row(name: str, run_dir: Path) -> dict[str, Any] | None:
    curves = sorted(run_dir.glob("train_*/epoch_curve.jsonl"))
    if len(curves) != 1:
        return None
    all_epochs = _json_lines(curves[0])
    steady = [
        row
        for row in all_epochs
        if int(row.get("epoch", 0)) > 1
        and int(row.get("dynamo_unique_graphs_epoch_delta", 0)) == 0
        and int(row.get("timing_synchronized", 0)) == 0
        and float(row.get("train_total_s", 0.0)) > 0.0
    ]
    if not steady:
        return None
    train_rows, padded_rows = _training_shape(run_dir)
    batch_ms = [_median([row], "train_total_ms_per_batch") for row in steady]
    valid_rows_s = [
        float(train_rows) / float(row["train_total_s"])
        for row in steady
        if train_rows > 0
    ]
    processed_rows_s = [
        float(padded_rows) / float(row["train_total_s"])
        for row in steady
        if padded_rows > 0
    ]
    manifest_path = run_dir / "run_manifest.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.is_file()
        else {}
    )
    configuration = manifest.get("configuration") or {}
    training = configuration.get("training") or {}
    fold_complete = (run_dir / "fold_11" / "fold_complete.json").is_file()
    total_ms = _median(steady, "train_total_ms_per_batch")
    model_ms = _median(steady, "train_model_forward_ms_per_batch")
    loss_ms = _median(steady, "train_loss_ms_per_batch")
    backward_ms = _median(steady, "train_backward_total_ms_per_batch")
    return {
        "name": name,
        "label": LABELS.get(name, name.replace("_", " ").title()),
        "complete": bool(fold_complete),
        "steady_epochs": len(steady),
        "steady_epoch_ids": ";".join(str(int(row["epoch"])) for row in steady),
        "train_rows": train_rows,
        "padded_rows": padded_rows,
        "global_batch": int(training.get("batch_size_train", 0)),
        "dataset_fingerprint": str(manifest.get("dataset_fingerprint", "")),
        "selected_fold_ids": json.dumps(manifest.get("selected_fold_ids", [])),
        "batch_ms_median": total_ms,
        "batch_ms_min": min(batch_ms),
        "batch_ms_max": max(batch_ms),
        "valid_rows_s_median": float(statistics.median(valid_rows_s)),
        "processed_rows_s_median": float(statistics.median(processed_rows_s)),
        "model_ms_median": model_ms,
        "loss_ms_median": loss_ms,
        "backward_ms_median": backward_ms,
        "other_ms_median": max(0.0, total_ms - model_ms - loss_ms - backward_ms),
        "batch_ms_observations": batch_ms,
        "curve_epoch_ids": [int(row["epoch"]) for row in all_epochs],
        "curve_train_loss": [float(row["train_loss"]) for row in all_epochs],
        "curve_val_loss": [float(row["val_mean"]) for row in all_epochs],
        "curve_test_loss": [float(row["test_mean"]) for row in all_epochs],
    }


def collect(root: Path) -> list[dict[str, Any]]:
    names = list(ORDER)
    generated = root / "generated_configs"
    if generated.is_dir():
        unknown = sorted(
            path.stem for path in generated.glob("*.yaml") if path.stem not in names
        )
        names.extend(unknown)
    rows = [row for name in names if (row := _run_row(name, root / name))]
    if not rows:
        raise ValueError(f"no steady-state epoch observations under {root}")
    baseline = next((row for row in rows if row["name"] == "baseline"), None)
    if baseline is None:
        raise ValueError("baseline needs at least one graph-stable unsynchronized epoch")
    fingerprint = baseline["dataset_fingerprint"]
    folds = baseline["selected_fold_ids"]
    incompatible = [
        row["name"]
        for row in rows
        if row["dataset_fingerprint"] != fingerprint
        or row["selected_fold_ids"] != folds
        or row["train_rows"] != baseline["train_rows"]
    ]
    if incompatible:
        raise ValueError(
            "incompatible ablation observations: " + ", ".join(incompatible)
        )
    baseline_speed = float(baseline["valid_rows_s_median"])
    for row in rows:
        row["throughput_delta_pct"] = (
            float(row["valid_rows_s_median"]) / baseline_speed - 1.0
        ) * 100.0
        row["relative_batch_latency"] = (
            float(row["batch_ms_median"]) / float(baseline["batch_ms_median"])
        )
    return rows


def _style_axis(ax: plt.Axes, *, xgrid: bool = True) -> None:
    ax.set_facecolor("white")
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(INK)
    ax.tick_params(colors=INK, labelsize=9)
    if xgrid:
        ax.grid(axis="x", color=GRID, linewidth=0.8, alpha=0.75)
        ax.set_axisbelow(True)


def _save_throughput(rows: list[dict[str, Any]], path: Path) -> None:
    ordered = sorted(rows, key=lambda row: float(row["throughput_delta_pct"]))
    values = [float(row["throughput_delta_pct"]) for row in ordered]
    labels = [str(row["label"]) for row in ordered]
    colors = [BLUE if row["name"] == "baseline" else ORANGE for row in ordered]
    fig, ax = plt.subplots(figsize=(10.5, max(4.8, 0.58 * len(rows) + 1.8)))
    y = np.arange(len(rows))
    ax.barh(y, values, color=colors, edgecolor=INK, linewidth=0.7)
    ax.axvline(0.0, color=INK, linewidth=1.0)
    ax.set_yticks(y, labels)
    padding = max(1.5, max(abs(value) for value in values) * 0.035)
    for index, value in enumerate(values):
        # Tiny negative effects are visually indistinguishable from the zero
        # reference at this scale.  Put their signed label on the plot side of
        # the axis so it cannot collide with the category label.
        tiny_effect = value < 0.0 and abs(value) < 2.0 * padding
        negative_bar = value < 0.0 and not tiny_effect
        # Negative bars extend toward the category labels, so place meaningful
        # negative effects just inside the bar.  Tiny negatives remain on the
        # positive side of the zero line where a text label still fits.
        label_x = value + padding if value >= 0.0 or negative_bar else padding
        ax.text(
            label_x,
            index,
            f"{value:+.1f}%",
            va="center",
            ha="left",
            color="white" if negative_bar else INK,
            fontsize=9,
        )
    ax.set_xlabel(
        f"Valid training rows per second vs {CONTROL_LABEL} (%)", color=INK
    )
    fig.text(0.16, 0.965, "Distributed-ledger throughput effects", color=INK, weight="bold", fontsize=14, va="top")
    fig.text(0.16, 0.928, "Fold 11; BF16 DDP; graph-stable epochs only; higher is better", color=MUTED, fontsize=9, va="top")
    _style_axis(ax)
    fig.subplots_adjust(left=0.22, right=0.91, top=0.88, bottom=0.13)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _save_latency_decomposition(rows: list[dict[str, Any]], path: Path) -> None:
    ordered = sorted(rows, key=lambda row: float(row["batch_ms_median"]))
    labels = [str(row["label"]) for row in ordered]
    keys = (
        ("model_ms_median", "Model forward", BLUE),
        ("loss_ms_median", "Ledger + loss", GOLD),
        ("backward_ms_median", "Backward", PINK),
        ("other_ms_median", "Other", "#B8BEC7"),
    )
    fig, ax = plt.subplots(figsize=(11.5, max(5.2, 0.62 * len(rows) + 2.0)))
    y = np.arange(len(rows))
    left = np.zeros(len(rows), dtype=np.float64)
    for key, label, color in keys:
        values = np.asarray([float(row[key]) for row in ordered])
        ax.barh(
            y,
            values,
            left=left,
            label=label,
            color=color,
            edgecolor="white",
            linewidth=0.5,
        )
        left += values
    for index, total in enumerate(left):
        ax.text(total + max(left) * 0.012, index, f"{total:,.0f} ms", va="center", color=INK, fontsize=9)
    ax.set_yticks(y, labels)
    ax.set_xlabel("Median steady-state milliseconds per global batch", color=INK)
    fig.text(
        0.16,
        0.965,
        "Training latency decomposition",
        color=INK,
        weight="bold",
        fontsize=14,
        va="top",
    )
    fig.text(
        0.16,
        0.928,
        "Components are additive timing fields; lower total is better",
        color=MUTED,
        fontsize=9,
        va="top",
    )
    handles, legend_labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        legend_labels,
        frameon=False,
        ncol=4,
        loc="upper left",
        bbox_to_anchor=(0.15, 0.902),
    )
    _style_axis(ax)
    fig.subplots_adjust(left=0.22, right=0.91, top=0.81, bottom=0.12)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _save_steady_distribution(rows: list[dict[str, Any]], path: Path) -> None:
    ordered = sorted(rows, key=lambda row: float(row["batch_ms_median"]))
    fig, ax = plt.subplots(figsize=(11.0, max(5.0, 0.60 * len(rows) + 1.8)))
    for index, row in enumerate(ordered):
        values = np.asarray(row["batch_ms_observations"], dtype=np.float64)
        low, high = float(values.min()), float(values.max())
        median = float(np.median(values))
        color = BLUE if row["name"] == "baseline" else OLIVE
        ax.hlines(index, low, high, color=color, linewidth=3.0)
        ax.scatter(values, np.full(values.shape, index), facecolors="white", edgecolors=color, s=45, linewidths=1.4, zorder=3)
        ax.scatter([median], [index], color=color, marker="D", s=42, zorder=4)
        ax.text(high + max(ordered_row["batch_ms_max"] for ordered_row in ordered) * 0.012, index, f"n={len(values)}", va="center", color=MUTED, fontsize=8)
    ax.set_yticks(np.arange(len(ordered)), [str(row["label"]) for row in ordered])
    ax.set_xlabel("Milliseconds per global training batch", color=INK)
    fig.text(0.16, 0.965, "Steady-state epoch timing observations", color=INK, weight="bold", fontsize=14, va="top")
    fig.text(0.16, 0.928, "Open circles are epochs; diamonds are medians; lines show observed range", color=MUTED, fontsize=9, va="top")
    _style_axis(ax)
    fig.subplots_adjust(left=0.22, right=0.91, top=0.88, bottom=0.13)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _save_objective_curves(rows: list[dict[str, Any]], path: Path) -> None:
    panels = (
        ("curve_train_loss", "Train objective"),
        ("curve_val_loss", "Validation objective"),
        ("curve_test_loss", "Sampled test objective"),
    )
    fig, axes = plt.subplots(1, 3, figsize=(15.0, 6.2), sharex=True)
    for run_index, row in enumerate(rows):
        color = SERIES_COLORS[run_index % len(SERIES_COLORS)]
        epochs = np.asarray(row["curve_epoch_ids"], dtype=np.int64)
        for ax, (key, _) in zip(axes, panels, strict=True):
            ax.plot(
                epochs,
                np.asarray(row[key], dtype=np.float64),
                marker="o",
                markersize=3.5,
                linewidth=1.6,
                color=color,
                label=str(row["label"]),
            )
    for ax, (_, title) in zip(axes, panels, strict=True):
        ax.axhline(0.0, color=GRID, linewidth=0.8)
        ax.set_title(title, color=INK, fontsize=10, weight="bold")
        ax.set_xlabel("Epoch", color=INK)
        ax.set_xticks(sorted({epoch for row in rows for epoch in row["curve_epoch_ids"]}))
        _style_axis(ax, xgrid=False)
        ax.grid(axis="y", color=GRID, linewidth=0.8, alpha=0.75)
    axes[0].set_ylabel("Recorded objective value; lower is better", color=INK)
    fig.text(
        0.08,
        0.975,
        "Objective curves retained across every systems ablation",
        color=INK,
        weight="bold",
        fontsize=14,
        va="top",
    )
    fig.text(
        0.08,
        0.938,
        "Diagnostic only: three epochs do not establish strategy quality",
        color=MUTED,
        fontsize=9,
        va="top",
    )
    handles, legend_labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        legend_labels,
        frameon=False,
        ncol=4,
        loc="upper left",
        bbox_to_anchor=(0.075, 0.905),
        fontsize=8,
    )
    fig.subplots_adjust(left=0.08, right=0.98, top=0.78, bottom=0.12, wspace=0.25)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_outputs(root: Path, rows: list[dict[str, Any]]) -> Path:
    analysis = root / "analysis"
    analysis.mkdir(parents=True, exist_ok=True)
    serializable: list[dict[str, Any]] = []
    for row in rows:
        serializable.append(
            {
                key: value
                for key, value in row.items()
                if key
                not in {
                    "batch_ms_observations",
                    "curve_epoch_ids",
                    "curve_train_loss",
                    "curve_val_loss",
                    "curve_test_loss",
                }
            }
        )
    fields = list(serializable[0])
    with (analysis / "system_ablation_summary.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(serializable)
    (analysis / "system_ablation_summary.json").write_text(
        json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    _save_throughput(rows, analysis / "throughput_effects.png")
    _save_latency_decomposition(rows, analysis / "latency_decomposition.png")
    _save_steady_distribution(rows, analysis / "steady_epoch_distribution.png")
    _save_objective_curves(rows, analysis / "objective_curves.png")
    return analysis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(
            "artifacts/ablations/"
            "executable_portfolio_transformer_symbol_sharded_ledger_fold11_v1"
        ),
    )
    parser.add_argument(
        "--suite",
        choices=("v6", "v5_local", "v5_extreme"),
        default="v6",
        help="Select labels and control wording for the systems suite.",
    )
    return parser.parse_args()


def main() -> None:
    global LABELS, ORDER, CONTROL_LABEL
    args = parse_args()
    if args.suite == "v5_local":
        LABELS = V5_LOCAL_LABELS
        ORDER = tuple(V5_LOCAL_LABELS)
        CONTROL_LABEL = "V5 local-metadata control"
    elif args.suite == "v5_extreme":
        LABELS = V5_LABELS
        ORDER = tuple(V5_LABELS)
        CONTROL_LABEL = "V5 CUDA-Graph control"
    root = args.root.resolve()
    rows = collect(root)
    analysis = write_outputs(root, rows)
    print(
        f"rendered distributed-ledger ablation: runs={len(rows)} output={analysis}"
    )


if __name__ == "__main__":
    main()
