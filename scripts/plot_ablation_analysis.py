#!/usr/bin/env python3
"""Render paired, fold-level charts for an OFAT ablation matrix."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.cm import ScalarMappable
from matplotlib.colors import BoundaryNorm, ListedColormap


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


LABELS = {
    "baseline": "Baseline (capital TWD 10M)",
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
    "initial_capital_1m": "Capital TWD 1M",
    # Compatibility label for preserved historical roots. New OFAT contracts
    # use TWD 10M as baseline rather than generating this variant.
    "initial_capital_10m": "Capital TWD 10M",
    "initial_capital_100m": "Capital TWD 100M",
    "output_activation_l1": "Output: activation L1",
    "output_l1": "Output: raw L1",
    "output_logits": "Output: logits",
    "output_signed_softmax": "Output: signed softmax",
    "output_signed_entmax15": "Output: signed entmax15",
    "output_signed_sparsemax": "Output: signed sparsemax",
}

METRIC_NAMES = (
    "cagr",
    "sharpe",
    "sortino",
    "max_drawdown",
    "turnover",
    "daily_hit_rate",
)

RISK_SCATTER_Y_METRICS = (
    ("cagr", "CAGR"),
    ("sharpe", "Sharpe ratio"),
    ("sortino", "Sortino ratio"),
    ("turnover", "turnover"),
    ("daily_hit_rate", "daily hit rate"),
)


def _fold_color_map(fold_ids: list[int] | tuple[int, ...]) -> dict[int, tuple]:
    """Map chronological folds to a light-to-dark, perceptually ordered blue."""

    ordered = sorted({int(fold_id) for fold_id in fold_ids})
    if not ordered:
        raise ValueError("fold_ids must not be empty")
    shades = plt.get_cmap("Blues")(np.linspace(0.30, 0.90, len(ordered)))
    return {
        fold_id: tuple(float(channel) for channel in shade)
        for fold_id, shade in zip(ordered, shades, strict=True)
    }


def _years(value: str) -> list[int]:
    return [int(year) for year in value.split("-") if year]


def _load_baseline_snapshot(root: Path, split: str) -> list[dict]:
    """Recover the exact baseline fold values exported by a prior successful plot.

    Some consolidated ablation roots keep ``baseline`` as a symlink to a preserved
    run.  If that source is temporarily unavailable, the fold-level CSV beside the
    charts is still a lossless source for every baseline metric used here.
    """

    candidates = [root / f"{split}_baseline_fold_metrics.csv"]
    if split == "deployment":
        # The owned handoff interval is the primary test surface, so current
        # runs intentionally publish it under the user-facing ``test`` prefix.
        candidates.append(root / "test_baseline_fold_metrics.csv")
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        return []
    rows: list[dict] = []
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            parsed = {
                "fold_id": int(row["fold_id"]),
                "train_years": _years(row["train_years"]),
                "val_years": _years(row["val_years"]),
                "test_years": _years(row["test_years"]),
                f"{split}_metrics": {
                    name: float(row[name])
                    for name in METRIC_NAMES
                },
            }
            if split == "deployment" and row.get("calculation_rows"):
                parsed.update(
                    deployment_rows=int(row["calculation_rows"]),
                    deployment_date_start=str(row["calculation_date_start"]),
                    deployment_date_end=str(row["calculation_date_end"]),
                )
            rows.append(parsed)
    return rows


def _load_summary_rows(run_dir: Path) -> list[dict]:
    path = run_dir / "summary.json"
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        return []
    return sorted(payload, key=lambda row: int(row["fold_id"]))


def _deployment_metric_rows(run_dir: Path, summary_rows: list[dict]) -> list[dict]:
    """Compute non-overlapping fold metrics from persisted deployment ledgers.

    These artifacts are the stitched, stateful tensor executions used for the
    deployable walk-forward view.  They must remain separate from ``test_metrics``,
    which can be an exact TWD-capital/whole-lot audit with economically correct
    all-flat folds.
    """

    from stockagent.backtest.report import compute_metrics
    from stockagent.backtest.simulator import BacktestResult

    # The optional final experimental fold can duplicate the preceding fold's
    # first test year. It is not a next-year handoff and must not replace the
    # earlier unbiased model in the non-overlapping test series.
    canonical_fold_ids: set[int] = set()
    duplicate_test_starts: set[int] = set()
    seen_test_starts: set[int] = set()
    for summary_row in summary_rows:
        test_years = [int(year) for year in summary_row.get("test_years", [])]
        if not test_years:
            raise ValueError(
                f"fold {summary_row.get('fold_id')} has no test-year ownership"
            )
        test_start = int(test_years[0])
        fold_id = int(summary_row["fold_id"])
        if test_start in seen_test_starts:
            duplicate_test_starts.add(test_start)
        else:
            seen_test_starts.add(test_start)
            canonical_fold_ids.add(fold_id)

    rows: list[dict] = []
    for summary_row in summary_rows:
        fold_id = int(summary_row["fold_id"])
        if fold_id not in canonical_fold_ids:
            continue
        artifact_path = (
            run_dir / f"fold_{fold_id:02d}" / "deployment_test_backtest.npz"
        )
        if not artifact_path.is_file():
            raise FileNotFoundError(artifact_path)
        with np.load(artifact_path, allow_pickle=False) as archive:
            strategy = np.asarray(archive["strategy_returns"], dtype=np.float64)
            benchmark = np.asarray(archive["benchmark_returns"], dtype=np.float64)
            turnovers = np.asarray(archive["turnovers"], dtype=np.float64)
            dates = np.asarray(archive["dates"], dtype="datetime64[D]")
        lengths = {
            int(strategy.size),
            int(benchmark.size),
            int(turnovers.size),
            int(dates.size),
        }
        if len(lengths) != 1:
            raise ValueError(
                f"invalid deployment artifact rows for {artifact_path}: "
                f"strategy={strategy.size}, benchmark={benchmark.size}, "
                f"turnover={turnovers.size}, dates={dates.size}"
            )
        # Scope-v2 artifacts assigned a same-year duplicate to the experimental
        # fold, leaving the earlier unbiased fold empty. Recover the correct
        # final interval from that fold's full test ledger without inference.
        test_start = int(summary_row["test_years"][0])
        if not strategy.size and test_start in duplicate_test_starts:
            fallback_path = run_dir / f"fold_{fold_id:02d}" / "test_backtest.npz"
            if not fallback_path.is_file():
                raise FileNotFoundError(fallback_path)
            with np.load(fallback_path, allow_pickle=False) as archive:
                strategy = np.asarray(archive["strategy_returns"], dtype=np.float64)
                benchmark = np.asarray(archive["benchmark_returns"], dtype=np.float64)
                turnovers = np.asarray(archive["turnovers"], dtype=np.float64)
                dates = np.asarray(archive["dates"], dtype="datetime64[D]")
            lengths = {
                int(strategy.size),
                int(benchmark.size),
                int(turnovers.size),
                int(dates.size),
            }
            if len(lengths) != 1:
                raise ValueError(
                    f"invalid recovered owned-test rows for {fallback_path}"
                )
        # An empty canonical interval is absence of an observation, not a
        # synthetic zero-return fold.
        if not strategy.size:
            continue
        result = BacktestResult(
            strategy_returns=strategy,
            benchmark_returns=benchmark,
            turnovers=turnovers,
            weights_history=np.empty((strategy.size, 0), dtype=np.float64),
        )
        row = dict(summary_row)
        row["deployment_metrics"] = compute_metrics(result)
        row["deployment_rows"] = int(strategy.size)
        row["deployment_date_start"] = str(dates[0])
        row["deployment_date_end"] = str(dates[-1])
        rows.append(row)
    return rows


def display_label(name: str) -> str:
    """Return a readable label without excluding newly added experiments."""

    return LABELS.get(name, name.replace("_", " ").title())


def ordered_variant_names(runs: dict[str, list[dict]]) -> list[str]:
    """Keep curated order for known runs and include every unknown complete run."""

    rank = {name: index for index, name in enumerate(LABELS)}
    names = (name for name in runs if name != "baseline")
    return sorted(names, key=lambda name: (rank.get(name, len(rank)), name))


def load(
    root: Path,
    split: str,
    *,
    baseline_root: Path | None = None,
) -> dict[str, list[dict]]:
    runs: dict[str, list[dict]] = {}
    for path in sorted(root.glob("*/summary.json")):
        rows = _load_summary_rows(path.parent)
        if not rows:
            continue
        if split == "deployment":
            try:
                rows = _deployment_metric_rows(path.parent, rows)
            except (FileNotFoundError, KeyError, OSError, ValueError) as exc:
                warnings.warn(
                    f"excluding {path.parent.name}: incomplete deployment metrics: {exc}",
                    stacklevel=2,
                )
                continue
        runs[path.parent.name] = rows
    if "baseline" not in runs:
        baseline_rows: list[dict] = []
        if baseline_root is not None:
            baseline_rows = _load_summary_rows(baseline_root)
            if split == "deployment" and baseline_rows:
                baseline_rows = _deployment_metric_rows(
                    baseline_root,
                    baseline_rows,
                )
        if not baseline_rows:
            baseline_rows = _load_baseline_snapshot(root, split)
        if not baseline_rows:
            raise RuntimeError(
                "A baseline summary was not found and no baseline fold-metrics "
                f"snapshot exists for split={split!r}; baseline_root={baseline_root}"
            )
        if baseline_root is None:
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


def _row_coverage(row: dict, split: str) -> tuple[int | str, str, str]:
    if split != "deployment":
        return "", "", ""
    if "deployment_rows" not in row:
        return "", "", ""
    return (
        int(row["deployment_rows"]),
        str(row["deployment_date_start"]),
        str(row["deployment_date_end"]),
    )


def bootstrap_ci(values: np.ndarray, rng: np.random.Generator) -> tuple[float, float]:
    draws = rng.choice(values, size=(20_000, len(values)), replace=True).mean(axis=1)
    return tuple(np.quantile(draws, [0.025, 0.975]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--split",
        choices=("val", "test", "deployment"),
        default="val",
    )
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--baseline-root", type=Path, default=None)
    parser.add_argument("--scope-label", default=None)
    args = parser.parse_args()
    runs = load(args.root, args.split, baseline_root=args.baseline_root)
    fold_count = len(runs["baseline"])
    prefix = args.prefix or args.split
    split_label = args.scope_label or {
        "val": "validation",
        "test": "test",
        "deployment": "non-overlapping deployment",
    }[args.split]
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
            "fold_id", "train_years", "val_years", "test_years",
            "calculation_rows", "calculation_date_start", "calculation_date_end",
            "cagr", "sharpe", "sortino", "max_drawdown", "turnover",
            "daily_hit_rate",
        ])
        for row in baseline_rows:
            values = row[f"{args.split}_metrics"]
            coverage = _row_coverage(row, args.split)
            writer.writerow([
                row["fold_id"], "-".join(map(str, row["train_years"])),
                "-".join(map(str, row["val_years"])), "-".join(map(str, row["test_years"])),
                *coverage,
                values["cagr"], values["sharpe"], values["sortino"], values["max_drawdown"],
                values["turnover"], values["daily_hit_rate"],
            ])

    with (args.output_dir / f"{prefix}_flat_fold_diagnostics.csv").open(
        "w", newline=""
    ) as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "variant",
                "fold_id",
                "calculation_rows",
                "calculation_date_start",
                "calculation_date_end",
                "all_flat",
                "cagr",
                "sharpe",
                "max_drawdown",
                "turnover",
                "daily_hit_rate",
            ]
        )
        for name in ["baseline"] + names:
            for row in runs[name]:
                values = row[f"{args.split}_metrics"]
                coverage = _row_coverage(row, args.split)
                all_flat = bool(
                    float(values["turnover"]) == 0.0
                    and float(values["cagr"]) == 0.0
                    and float(values["max_drawdown"]) == 0.0
                    and float(values["daily_hit_rate"]) == 0.0
                )
                writer.writerow(
                    [
                        name,
                        int(row["fold_id"]),
                        *coverage,
                        int(all_flat),
                        values["cagr"],
                        values["sharpe"],
                        values["max_drawdown"],
                        values["turnover"],
                        values["daily_hit_rate"],
                    ]
                )

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
    fig.suptitle(f"Model ablations: paired change in {split_label} Sharpe", y=.985, fontweight="bold")
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

    # One relationship per file: x is always median max drawdown and y changes
    # across the other canonical metrics. Every chart keeps every compatible
    # ablation on one scatter axes. Full experiment names are placed on the
    # nearest side of the axes and connected back to their data points; this
    # keeps names readable without replacing them with opaque point codes.
    all_names = ["baseline"] + names
    median_metrics = {
        name: {
            metric_name: float(
                np.median(metric(runs[name], args.split, metric_name))
            )
            for metric_name in METRIC_NAMES
        }
        for name in all_names
    }
    risk_axis_label = {
        "val": "validation",
        "deployment": "owned stitched deployment test",
        "test": "full-horizon fold test (reset state)",
    }[args.split]
    all_drawdowns = np.asarray(
        [median_metrics[name]["max_drawdown"] for name in all_names],
        dtype=np.float64,
    )
    drawdown_span = max(float(np.ptp(all_drawdowns)), 1e-6)
    common_xlim = (
        float(np.min(all_drawdowns) - 0.06 * drawdown_span),
        float(np.max(all_drawdowns) + 0.12 * drawdown_span),
    )

    def render_metric_scatter(
        y_metric: str,
        y_label: str,
        *,
        output_path: Path,
    ) -> None:
        y_values = np.asarray(
            [median_metrics[name][y_metric] for name in all_names],
            dtype=np.float64,
        )
        y_span = max(float(np.ptp(y_values)), 1e-6)
        y_min = float(np.min(y_values) - 0.08 * y_span)
        y_max = float(np.max(y_values) + 0.10 * y_span)
        reference_value: float | None = None
        if y_metric in {"cagr", "sharpe", "sortino"}:
            reference_value = 0.0
        elif y_metric == "daily_hit_rate":
            reference_value = 0.5
        if reference_value is not None:
            y_min = min(reference_value, y_min)
            y_max = max(reference_value, y_max)

        fig, ax = plt.subplots(figsize=(16, 8.6))
        for name in all_names:
            drawdown = median_metrics[name]["max_drawdown"]
            y_value = median_metrics[name][y_metric]
            color = ink if name == "baseline" else blue
            ax.scatter(
                drawdown,
                y_value,
                s=100 if name == "baseline" else 72,
                color=color,
                edgecolor="white",
                linewidth=.9,
                zorder=3,
            )
        ax.set_xlim(*common_xlim)
        ax.set_ylim(y_min, y_max)

        # Split by x rank rather than the numeric midpoint so each side gets a
        # balanced number of labels even when drawdowns are highly clustered.
        x_ranked_names = sorted(
            all_names,
            key=lambda name: median_metrics[name]["max_drawdown"],
        )
        split_at = (len(x_ranked_names) + 1) // 2
        side_names = {
            "left": x_ranked_names[:split_at],
            "right": x_ranked_names[split_at:],
        }

        def spread_label_positions(side: str) -> dict[str, float]:
            side_order = sorted(
                side_names[side],
                key=lambda name: median_metrics[name][y_metric],
            )
            if not side_order:
                return {}
            positions = np.asarray(
                [
                    (median_metrics[name][y_metric] - y_min) / (y_max - y_min)
                    for name in side_order
                ],
                dtype=np.float64,
            )
            low, high = .035, .965
            positions = np.clip(positions, low, high)
            minimum_gap = min(.052, (high - low) / max(len(side_order) - 1, 1))
            for index in range(1, len(positions)):
                positions[index] = max(
                    positions[index], positions[index - 1] + minimum_gap
                )
            if positions[-1] > high:
                positions -= positions[-1] - high
            for index in range(len(positions) - 2, -1, -1):
                positions[index] = min(
                    positions[index], positions[index + 1] - minimum_gap
                )
            if positions[0] < low:
                positions += low - positions[0]
            return dict(zip(side_order, positions, strict=True))

        for side, label_x, horizontal_alignment in (
            ("left", -.035, "right"),
            ("right", 1.035, "left"),
        ):
            label_positions = spread_label_positions(side)
            for name in side_names[side]:
                drawdown = median_metrics[name]["max_drawdown"]
                y_value = median_metrics[name][y_metric]
                color = ink if name == "baseline" else blue
                ax.annotate(
                    display_label(name),
                    xy=(drawdown, y_value),
                    xycoords="data",
                    xytext=(label_x, label_positions[name]),
                    textcoords="axes fraction",
                    ha=horizontal_alignment,
                    va="center",
                    fontsize=8.1,
                    fontweight="bold" if name == "baseline" else "normal",
                    color=color,
                    annotation_clip=False,
                    arrowprops={
                        "arrowstyle": "-",
                        "color": color,
                        "alpha": .62,
                        "linewidth": .7,
                        "shrinkA": 2,
                        "shrinkB": 4,
                    },
                    zorder=2,
                )
        if reference_value is not None and y_min <= reference_value <= y_max:
            ax.axhline(reference_value, color=ink, lw=.8)
        ax.set_xlabel(
            f"Median {risk_axis_label} max drawdown (less negative is better)"
        )
        # A vertical y label would intersect the full experiment names placed
        # on the left. Keep the metric explicit inside the one scatter axes.
        ax.text(
            .012,
            .982,
            f"Median {risk_axis_label} {y_label}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            color=ink,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": .84},
            zorder=5,
        )
        fig.suptitle(
            f"Median {risk_axis_label} max drawdown vs {y_label} across ablations",
            y=.985,
            fontweight="bold",
        )
        fig.text(
            .5,
            .95,
            f"All {len(all_names)} experiments; each point summarizes "
            f"{fold_count} walk-forward folds using medians",
            ha="center",
            color="#596273",
        )
        ax.grid(color=grid, lw=.7)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        fig.subplots_adjust(left=.23, right=.77, bottom=.11, top=.89)
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)

    for y_metric, y_label in RISK_SCATTER_Y_METRICS:
        render_metric_scatter(
            y_metric,
            y_label,
            output_path=(
                args.output_dir
                / f"{prefix}_risk_return_{y_metric}_medians.png"
            ),
        )
    # Historical filename remains an exact duplicate of the CAGR scatter for
    # downstream consumers that have not migrated to the explicit metric name.
    render_metric_scatter(
        "cagr",
        "CAGR",
        output_path=args.output_dir / f"{prefix}_risk_return_medians.png",
    )

    # Preserve fold-level movement instead of collapsing every experiment to a
    # single median. Each metric gets comparable one-fold views plus one
    # connected trajectory view. Fold color and marker size both progress in
    # chronological order so the combined chart remains interpretable without
    # relying on color alone.
    fold_ids = [int(row["fold_id"]) for row in baseline_rows]
    fold_colors = _fold_color_map(fold_ids)
    fold_sizes = {
        fold_id: float(size)
        for fold_id, size in zip(
            fold_ids,
            np.linspace(34.0, 92.0, len(fold_ids)),
            strict=True,
        )
    }
    fold_scatter_root = args.output_dir / f"{prefix}_risk_return_by_fold"

    def padded_limits(
        values: np.ndarray,
        *,
        lower_padding: float,
        upper_padding: float,
        reference: float | None = None,
    ) -> tuple[float, float]:
        finite = np.asarray(values, dtype=np.float64)
        finite = finite[np.isfinite(finite)]
        if not finite.size:
            raise ValueError("cannot plot a metric without finite observations")
        span = max(float(np.ptp(finite)), 1e-6)
        lower = float(np.min(finite) - lower_padding * span)
        upper = float(np.max(finite) + upper_padding * span)
        if reference is not None:
            lower = min(lower, reference)
            upper = max(upper, reference)
        return lower, upper

    def annotate_experiment_names(
        ax: plt.Axes,
        point_values: dict[str, tuple[float, float]],
        y_limits: tuple[float, float],
    ) -> None:
        x_ranked_names = sorted(
            point_values,
            key=lambda name: point_values[name][0],
        )
        split_at = (len(x_ranked_names) + 1) // 2
        side_names = {
            "left": x_ranked_names[:split_at],
            "right": x_ranked_names[split_at:],
        }
        y_min, y_max = y_limits

        def spread_label_positions(side: str) -> dict[str, float]:
            side_order = sorted(
                side_names[side],
                key=lambda name: point_values[name][1],
            )
            if not side_order:
                return {}
            positions = np.asarray(
                [
                    (point_values[name][1] - y_min) / (y_max - y_min)
                    for name in side_order
                ],
                dtype=np.float64,
            )
            low, high = .035, .965
            positions = np.clip(positions, low, high)
            minimum_gap = min(
                .052,
                (high - low) / max(len(side_order) - 1, 1),
            )
            for index in range(1, len(positions)):
                positions[index] = max(
                    positions[index],
                    positions[index - 1] + minimum_gap,
                )
            if positions[-1] > high:
                positions -= positions[-1] - high
            for index in range(len(positions) - 2, -1, -1):
                positions[index] = min(
                    positions[index],
                    positions[index + 1] - minimum_gap,
                )
            if positions[0] < low:
                positions += low - positions[0]
            return dict(zip(side_order, positions, strict=True))

        for side, label_x, horizontal_alignment in (
            ("left", -.035, "right"),
            ("right", 1.035, "left"),
        ):
            positions = spread_label_positions(side)
            for name in side_names[side]:
                x_value, y_value = point_values[name]
                color = ink if name == "baseline" else blue
                ax.annotate(
                    display_label(name),
                    xy=(x_value, y_value),
                    xycoords="data",
                    xytext=(label_x, positions[name]),
                    textcoords="axes fraction",
                    ha=horizontal_alignment,
                    va="center",
                    fontsize=8.0,
                    fontweight="bold" if name == "baseline" else "normal",
                    color=color,
                    annotation_clip=False,
                    arrowprops={
                        "arrowstyle": "-",
                        "color": color,
                        "alpha": .58,
                        "linewidth": .7,
                        "shrinkA": 2,
                        "shrinkB": 4,
                    },
                    zorder=2,
                )

    drawdown_by_name = {
        name: metric(runs[name], args.split, "max_drawdown")
        for name in all_names
    }
    all_fold_drawdowns = np.concatenate(list(drawdown_by_name.values()))
    fold_xlim = padded_limits(
        all_fold_drawdowns,
        lower_padding=.055,
        upper_padding=.09,
    )

    for y_metric, y_label in RISK_SCATTER_Y_METRICS:
        y_by_name = {
            name: metric(runs[name], args.split, y_metric)
            for name in all_names
        }
        reference_value: float | None = None
        if y_metric in {"cagr", "sharpe", "sortino"}:
            reference_value = 0.0
        elif y_metric == "daily_hit_rate":
            reference_value = 0.5
        fold_ylim = padded_limits(
            np.concatenate(list(y_by_name.values())),
            lower_padding=.075,
            upper_padding=.09,
            reference=reference_value,
        )
        metric_dir = fold_scatter_root / y_metric
        metric_dir.mkdir(parents=True, exist_ok=True)

        for fold_index, fold_id in enumerate(fold_ids):
            point_values = {
                name: (
                    float(drawdown_by_name[name][fold_index]),
                    float(y_by_name[name][fold_index]),
                )
                for name in all_names
            }
            fig, ax = plt.subplots(figsize=(16, 8.6))
            fold_color = fold_colors[fold_id]
            for name in all_names:
                x_value, y_value = point_values[name]
                ax.scatter(
                    x_value,
                    y_value,
                    s=104 if name == "baseline" else 74,
                    marker="D" if name == "baseline" else "o",
                    color=fold_color,
                    edgecolor=ink if name == "baseline" else "white",
                    linewidth=1.5 if name == "baseline" else .8,
                    zorder=3,
                )
            ax.set_xlim(*fold_xlim)
            ax.set_ylim(*fold_ylim)
            annotate_experiment_names(ax, point_values, fold_ylim)
            if reference_value is not None:
                ax.axhline(reference_value, color=ink, lw=.8)
            ax.set_xlabel(
                f"Fold {fold_id} {risk_axis_label} max drawdown "
                "(less negative is better)"
            )
            ax.text(
                .012,
                .982,
                f"Fold {fold_id} {risk_axis_label} {y_label}",
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=10,
                color=ink,
                bbox={"facecolor": "white", "edgecolor": "none", "alpha": .84},
                zorder=5,
            )
            coverage = _row_coverage(baseline_rows[fold_index], args.split)
            coverage_text = (
                f"{coverage[1]} to {coverage[2]}; {coverage[0]} sessions; "
                if coverage[0] != ""
                else ""
            )
            fig.suptitle(
                f"Fold {fold_id}: {risk_axis_label} max drawdown vs {y_label}",
                y=.985,
                fontweight="bold",
            )
            fig.text(
                .5,
                .95,
                f"{coverage_text}all {len(all_names)} experiments; "
                "x/y scales are shared across folds",
                ha="center",
                color="#596273",
            )
            ax.grid(color=grid, lw=.7)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
            fig.subplots_adjust(left=.23, right=.77, bottom=.11, top=.89)
            fig.savefig(
                metric_dir / f"fold_{fold_id:02d}.png",
                dpi=180,
                bbox_inches="tight",
            )
            plt.close(fig)

        # One connected path per experiment. The latest fold receives the
        # darkest/largest point and the direct experiment-name label.
        fig, ax = plt.subplots(figsize=(17, 9.2))
        for name in all_names:
            x_values = drawdown_by_name[name]
            y_values = y_by_name[name]
            ax.plot(
                x_values,
                y_values,
                color=ink if name == "baseline" else blue,
                linewidth=1.45 if name == "baseline" else .75,
                alpha=.72 if name == "baseline" else .24,
                zorder=1,
            )
            for fold_index, fold_id in enumerate(fold_ids):
                ax.scatter(
                    x_values[fold_index],
                    y_values[fold_index],
                    s=(
                        fold_sizes[fold_id] * (1.18 if name == "baseline" else 1.0)
                    ),
                    marker="D" if name == "baseline" else "o",
                    color=fold_colors[fold_id],
                    edgecolor=ink if name == "baseline" else "white",
                    linewidth=1.45 if name == "baseline" else .55,
                    zorder=3,
                )
        ax.set_xlim(*fold_xlim)
        ax.set_ylim(*fold_ylim)
        latest_index = len(fold_ids) - 1
        latest_points = {
            name: (
                float(drawdown_by_name[name][latest_index]),
                float(y_by_name[name][latest_index]),
            )
            for name in all_names
        }
        annotate_experiment_names(ax, latest_points, fold_ylim)
        if reference_value is not None:
            ax.axhline(reference_value, color=ink, lw=.8)
        ax.set_xlabel(
            f"Per-fold {risk_axis_label} max drawdown "
            "(less negative is better)"
        )
        ax.text(
            .012,
            .982,
            f"Per-fold {risk_axis_label} {y_label}",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            color=ink,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": .84},
            zorder=5,
        )
        fig.suptitle(
            f"Fold trajectories: {risk_axis_label} max drawdown vs {y_label}",
            y=.988,
            fontweight="bold",
        )
        fig.text(
            .5,
            .955,
            f"Each line is one experiment; Fold {fold_ids[-1]} is darkest and "
            "largest; labels mark latest endpoints",
            ha="center",
            color="#596273",
        )
        ordered_colors = [fold_colors[fold_id] for fold_id in fold_ids]
        discrete_cmap = ListedColormap(ordered_colors)
        discrete_norm = BoundaryNorm(
            np.arange(len(fold_ids) + 1) - .5,
            discrete_cmap.N,
        )
        colorbar = fig.colorbar(
            ScalarMappable(norm=discrete_norm, cmap=discrete_cmap),
            ax=ax,
            orientation="horizontal",
            fraction=.045,
            pad=.105,
            ticks=np.arange(len(fold_ids)),
        )
        colorbar.ax.set_xticklabels([f"F{fold_id}" for fold_id in fold_ids])
        colorbar.set_label("Owned-test fold: later folds are darker and larger")
        ax.grid(color=grid, lw=.7)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        fig.subplots_adjust(left=.22, right=.78, bottom=.18, top=.89)
        fig.savefig(
            metric_dir / "all_folds_connected.png",
            dpi=180,
            bbox_inches="tight",
        )
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
