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

    # Risk/return comparison uses aligned categorical dot plots.  A scatter
    # becomes unreadable when the exact whole-lot audit legitimately places
    # many variants at the same (0, 0) coordinate.
    all_names = ["baseline"] + names
    risk_rows = [
        (
            name,
            float(np.median(metric(runs[name], args.split, "cagr"))),
            float(np.median(metric(runs[name], args.split, "max_drawdown"))),
        )
        for name in all_names
    ]
    figure_height = max(7.2, 0.44 * len(risk_rows) + 2.3)
    fig, (ax_return, ax_risk) = plt.subplots(
        1,
        2,
        figsize=(14, figure_height),
        sharey=True,
        gridspec_kw={"wspace": 0.08},
    )
    y = np.arange(len(risk_rows), dtype=np.float64)
    cagr_values = np.asarray([row[1] for row in risk_rows], dtype=np.float64)
    drawdown_values = np.asarray([row[2] for row in risk_rows], dtype=np.float64)
    colors = [ink if row[0] == "baseline" else blue for row in risk_rows]
    sizes = [78 if row[0] == "baseline" else 48 for row in risk_rows]
    markers = ["D" if row[0] == "baseline" else "o" for row in risk_rows]
    for index, marker in enumerate(markers):
        ax_return.scatter(
            cagr_values[index],
            y[index],
            s=sizes[index],
            color=colors[index],
            marker=marker,
            edgecolor="white",
            linewidth=.7,
            zorder=3,
        )
        ax_risk.scatter(
            drawdown_values[index],
            y[index],
            s=sizes[index],
            color=colors[index],
            marker=marker,
            edgecolor="white",
            linewidth=.7,
            zorder=3,
        )
    ax_return.set_yticks(y, [display_label(row[0]) for row in risk_rows])
    ax_return.invert_yaxis()
    ax_return.axvline(0, color=ink, lw=.8)
    ax_risk.axvline(0, color=ink, lw=.8)
    ax_return.set_xlabel(f"Median {split_label} CAGR")
    ax_risk.set_xlabel(
        f"Median {split_label} max drawdown\n(less negative is better)"
    )
    fig.suptitle("Risk and return medians across ablations", y=.985, fontweight="bold")
    fig.text(.5, .95, f"Each point summarizes {fold_count} walk-forward folds using medians",
             ha="center", color="#596273")
    for ax in (ax_return, ax_risk):
        ax.grid(axis="x", color=grid, lw=.7)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
    fig.subplots_adjust(
        left=0.24,
        right=0.98,
        bottom=0.11,
        top=0.89,
        wspace=0.08,
    )
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
