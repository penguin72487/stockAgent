#!/usr/bin/env python3
"""Render paired, fold-level charts for the Transformer OFAT ablation matrix."""

from __future__ import annotations

import argparse
import csv
import json
import math
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


LABELS = {
    "baseline": "Baseline (capital TWD 1M)",
    "lookback256_batch32": "Lookback 256 / batch 32",
    "lookback64_batch64": "Lookback 64 / batch 64",
    "lookback32_batch128": "Lookback 32 / batch 128",
    "latent_only": "Latent only",
    "market_only": "Market only",
    "temporal_only": "Temporal only",
    "no_rope_temporal": "No temporal RoPE",
    "no_time_position": "No learned time pos.",
    "with_symbol_position": "With symbol pos.",
    "no_qk_norm": "No QK norm",
    "mean_pooling": "Mean pooling",
    "attention_pooling": "Attention pooling",
    "layernorm": "LayerNorm",
    "gelu_ffn": "GELU FFN",
    "initial_capital_10m": "Capital TWD 10M",
    "initial_capital_100m": "Capital TWD 100M",
    "output_activation_l1": "Output: activation L1",
    "output_l1": "Output: raw L1",
    "output_logits": "Output: logits",
    "output_signed_softmax": "Output: signed softmax",
    "output_signed_entmax15": "Output: signed entmax15",
    "output_signed_sparsemax": "Output: signed sparsemax",
}


def _years(value: str) -> list[int]:
    return [int(year) for year in value.split("-") if year]


def _load_baseline_snapshot(root: Path, split: str) -> list[dict]:
    """Recover the exact baseline fold values exported by a prior successful plot.

    Some consolidated ablation roots keep ``baseline`` as a symlink to a preserved
    run.  If that source is temporarily unavailable, the fold-level CSV beside the
    charts is still a lossless source for every baseline metric used here.
    """

    path = root / f"{split}_baseline_fold_metrics.csv"
    if not path.is_file():
        return []
    rows: list[dict] = []
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(
                {
                    "fold_id": int(row["fold_id"]),
                    "train_years": _years(row["train_years"]),
                    "val_years": _years(row["val_years"]),
                    "test_years": _years(row["test_years"]),
                    f"{split}_metrics": {
                        name: float(row[name])
                        for name in (
                            "cagr",
                            "sharpe",
                            "sortino",
                            "max_drawdown",
                            "turnover",
                            "daily_hit_rate",
                        )
                    },
                }
            )
    return rows


def display_label(name: str) -> str:
    """Return a readable label without excluding newly added experiments."""

    return LABELS.get(name, name.replace("_", " ").title())


def ordered_variant_names(runs: dict[str, list[dict]]) -> list[str]:
    """Keep curated order for known runs and include every unknown complete run."""

    rank = {name: index for index, name in enumerate(LABELS)}
    names = (name for name in runs if name != "baseline")
    return sorted(names, key=lambda name: (rank.get(name, len(rank)), name))


def load(root: Path, split: str) -> dict[str, list[dict]]:
    runs: dict[str, list[dict]] = {}
    for path in sorted(root.glob("*/summary.json")):
        rows = json.loads(path.read_text())
        if isinstance(rows, list) and rows:
            runs[path.parent.name] = sorted(rows, key=lambda row: int(row["fold_id"]))
    if "baseline" not in runs:
        baseline_rows = _load_baseline_snapshot(root, split)
        if not baseline_rows:
            raise RuntimeError(
                "A baseline summary was not found and no baseline fold-metrics "
                f"snapshot exists for split={split!r}"
            )
        warnings.warn(
            f"baseline/summary.json is unavailable; using {split} baseline "
            "fold-metrics snapshot",
            stacklevel=2,
        )
        runs["baseline"] = sorted(
            baseline_rows, key=lambda row: int(row["fold_id"])
        )
    fold_count = len(runs["baseline"])
    baseline_folds = tuple(int(row["fold_id"]) for row in runs["baseline"])
    compatible: dict[str, list[dict]] = {}
    rejected: list[str] = []
    for name, rows in runs.items():
        folds = tuple(int(row["fold_id"]) for row in rows)
        if len(rows) == fold_count and folds == baseline_folds:
            compatible[name] = rows
        else:
            rejected.append(name)
    if rejected:
        warnings.warn(
            "excluding runs without the same completed fold IDs as baseline: "
            + ", ".join(sorted(rejected)),
            stacklevel=2,
        )
    return compatible


def metric(rows: list[dict], split: str, name: str) -> np.ndarray:
    return np.asarray([r[f"{split}_metrics"][name] for r in rows], dtype=float)


def bootstrap_ci(values: np.ndarray, rng: np.random.Generator) -> tuple[float, float]:
    draws = rng.choice(values, size=(20_000, len(values)), replace=True).mean(axis=1)
    return tuple(np.quantile(draws, [0.025, 0.975]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--split", choices=("val", "test"), default="val")
    parser.add_argument("--prefix", default=None)
    args = parser.parse_args()
    runs = load(args.root, args.split)
    fold_count = len(runs["baseline"])
    prefix = args.prefix or args.split
    split_label = "validation" if args.split == "val" else "test"
    args.output_dir.mkdir(parents=True, exist_ok=True)

    names = ordered_variant_names(runs)
    base = metric(runs["baseline"], args.split, "sharpe")
    rng = np.random.default_rng(20260802)
    stats = []
    for name in names:
        delta = metric(runs[name], args.split, "sharpe") - base
        lo, hi = bootstrap_ci(delta, rng)
        stats.append((name, delta.mean(), lo, hi, np.median(delta), (delta > 0).mean()))
    stats.sort(key=lambda x: x[1])

    with (args.output_dir / f"{prefix}_paired_sharpe_summary.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["variant", "mean_delta_sharpe", "ci95_low", "ci95_high", "median_delta_sharpe", "fold_win_rate"])
        writer.writerow(["baseline", 0.0, 0.0, 0.0, 0.0, "reference"])
        writer.writerows(stats)

    absolute_stats = []
    for name in ["baseline"] + names:
        values = metric(runs[name], args.split, "sharpe")
        lo, hi = bootstrap_ci(values, rng)
        absolute_stats.append((name, values.mean(), lo, hi, np.median(values)))
    absolute_stats.sort(key=lambda row: row[1])
    with (args.output_dir / f"{prefix}_absolute_sharpe_summary.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["variant", "mean_sharpe", "ci95_low", "ci95_high", "median_sharpe"])
        writer.writerows(absolute_stats)

    baseline_rows = runs["baseline"]
    baseline_metrics_path = args.output_dir / f"{prefix}_baseline_fold_metrics.csv"
    with baseline_metrics_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "fold_id", "train_years", "val_years", "test_years", "cagr", "sharpe",
            "sortino", "max_drawdown", "turnover", "daily_hit_rate",
        ])
        for row in baseline_rows:
            values = row[f"{args.split}_metrics"]
            writer.writerow([
                row["fold_id"], "-".join(map(str, row["train_years"])),
                "-".join(map(str, row["val_years"])), "-".join(map(str, row["test_years"])),
                values["cagr"], values["sharpe"], values["sortino"], values["max_drawdown"],
                values["turnover"], values["daily_hit_rate"],
            ])

    plt.rcParams.update({"font.size": 10, "axes.titleweight": "bold", "figure.facecolor": "white"})
    blue, orange, ink, grid = "#2864A8", "#D9782D", "#252A34", "#D9DEE7"

    effect_stats = stats + [("baseline", 0.0, 0.0, 0.0, 0.0, math.nan)]
    fig, ax = plt.subplots(figsize=(11, 7.6))
    y = np.arange(len(effect_stats))
    means = np.array([s[1] for s in effect_stats])
    low = np.array([s[2] for s in effect_stats])
    high = np.array([s[3] for s in effect_stats])
    colors = [ink if s[0] == "baseline" else blue if s[1] >= 0 else orange for s in effect_stats]
    ax.errorbar(means, y, xerr=[means - low, high - means], fmt="none", ecolor=ink, capsize=3, lw=1.2)
    ax.scatter(means, y, c=colors, s=58, edgecolor=ink, linewidth=.6, zorder=3)
    ax.axvline(0, color=ink, lw=1)
    ax.set_yticks(y, [display_label(s[0]) for s in effect_stats])
    ax.set_xlabel(f"Mean paired difference in {split_label} Sharpe vs baseline")
    fig.suptitle(f"Transformer ablations: paired change in {split_label} Sharpe", y=.985, fontweight="bold")
    fig.text(.5, .95, f"{fold_count} walk-forward folds; whiskers are fold-bootstrap 95% CIs; positive favors variant",
             ha="center", color="#596273")
    ax.grid(axis="x", color=grid, lw=.7)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, .93))
    fig.savefig(args.output_dir / f"{prefix}_paired_sharpe_effects.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    absolute_display = [row for row in absolute_stats if row[0] != "baseline"]
    absolute_display.append(next(row for row in absolute_stats if row[0] == "baseline"))
    fig, ax = plt.subplots(figsize=(11, 7.6))
    y = np.arange(len(absolute_display))
    means = np.asarray([row[1] for row in absolute_display])
    low = np.asarray([row[2] for row in absolute_display])
    high = np.asarray([row[3] for row in absolute_display])
    colors = [ink if row[0] == "baseline" else blue for row in absolute_display]
    ax.errorbar(means, y, xerr=[means - low, high - means], fmt="none", ecolor=ink, capsize=3, lw=1.2)
    ax.scatter(means, y, c=colors, s=58, edgecolor=ink, linewidth=.6, zorder=3)
    ax.set_yticks(y, [display_label(row[0]) for row in absolute_display])
    ax.set_xlabel(f"Mean absolute {split_label} Sharpe")
    fig.suptitle(f"Absolute {split_label} Sharpe across ablations", y=.985, fontweight="bold")
    fig.text(.5, .95, f"{fold_count} walk-forward folds; whiskers are fold-bootstrap 95% CIs; baseline is black",
             ha="center", color="#596273")
    ax.grid(axis="x", color=grid, lw=.7)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, .93))
    fig.savefig(args.output_dir / f"{prefix}_absolute_sharpe_by_variant.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    order = ["baseline"] + [s[0] for s in sorted(stats, key=lambda x: x[1], reverse=True)]
    matrix = np.vstack([
        np.zeros_like(base) if n == "baseline" else metric(runs[n], args.split, "sharpe") - base
        for n in order
    ])
    limit = max(1.0, float(np.nanquantile(np.abs(matrix), .95)))
    fig, ax = plt.subplots(figsize=(14, 7.5))
    image = ax.imshow(matrix, aspect="auto", cmap="RdBu", vmin=-limit, vmax=limit)
    ax.set_yticks(np.arange(len(order)), [display_label(n) for n in order])
    ax.set_xticks(np.arange(fold_count), [str(i) for i in range(1, fold_count + 1)])
    ax.set_xlabel("Walk-forward fold")
    fig.suptitle(f"Paired {split_label}-Sharpe difference by fold", y=.985, fontweight="bold")
    fig.text(.5, .95, "Blue beats baseline; orange trails baseline; colors clipped at the 95th percentile",
             ha="center", color="#596273")
    cb = fig.colorbar(image, ax=ax, pad=.015)
    cb.set_label("Delta Sharpe")
    fig.tight_layout(rect=(0, 0, 1, .93))
    fig.savefig(args.output_dir / f"{prefix}_fold_sharpe_heatmap.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    # Risk/return view uses robust fold medians because long-horizon cumulative returns are heavy-tailed.
    fig, ax = plt.subplots(figsize=(10, 7.2))
    all_names = ["baseline"] + names
    label_offsets = {
        "temporal_only": (5, -14), "no_time_position": (5, 7),
        "lookback256_batch32": (5, 7), "lookback64_batch64": (5, 7),
        "with_symbol_position": (5, -14),
    }
    for name in all_names:
        x = np.median(metric(runs[name], args.split, "max_drawdown"))
        yv = np.median(metric(runs[name], args.split, "cagr"))
        color = ink if name == "baseline" else blue
        ax.scatter(x, yv, s=80 if name == "baseline" else 48, color=color, edgecolor="white", linewidth=.7)
        ax.annotate(display_label(name), (x, yv), xytext=label_offsets.get(name, (5, 4)),
                    textcoords="offset points", fontsize=8)
    ax.axhline(0, color=grid, lw=.9)
    ax.set_xlabel(f"Median {split_label} max drawdown (less negative is better)")
    ax.set_ylabel(f"Median {split_label} CAGR")
    fig.suptitle("Risk-return profile across ablations", y=.985, fontweight="bold")
    fig.text(.5, .95, f"Each point summarizes {fold_count} walk-forward folds using medians",
             ha="center", color="#596273")
    ax.grid(color=grid, lw=.7)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, .93))
    fig.savefig(args.output_dir / f"{prefix}_risk_return_medians.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    folds = np.asarray([row["fold_id"] for row in baseline_rows])
    baseline_sharpe = metric(baseline_rows, args.split, "sharpe")
    baseline_cagr = metric(baseline_rows, args.split, "cagr")
    baseline_drawdown = metric(baseline_rows, args.split, "max_drawdown")
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    panels = [
        (baseline_sharpe, "Sharpe", blue, 0.0),
        (baseline_cagr, "CAGR", blue, 0.0),
        (baseline_drawdown, "Max drawdown", orange, 0.0),
    ]
    for ax, (values, ylabel, color, reference) in zip(axes, panels, strict=True):
        ax.bar(folds, values, color=color, edgecolor=ink, linewidth=.5)
        ax.axhline(reference, color=ink, lw=.8)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", color=grid, lw=.7)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
    axes[-1].set_xticks(folds)
    axes[-1].set_xlabel("Walk-forward fold")
    fig.suptitle(f"Baseline absolute {split_label} metrics by fold", y=.985, fontweight="bold")
    fig.text(.5, .955, "Raw baseline values; no subtraction against baseline",
             ha="center", color="#596273")
    fig.tight_layout(rect=(0, 0, 1, .94))
    fig.savefig(args.output_dir / f"{prefix}_baseline_metrics_by_fold.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
