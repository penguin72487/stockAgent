#!/usr/bin/env python3
"""Build a complete, no-Top-K chart gallery from multi-fold explainability CSVs."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


BLUE = "#2F6B9A"
GOLD = "#C6922B"
ORANGE = "#D56A3A"
INK = "#202428"
GRID = "#D9DEE3"
METHOD_COLORS = {
    "Gradient": BLUE,
    "Integrated Gradients": GOLD,
    "SHAP": ORANGE,
}


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": INK,
            "axes.labelcolor": INK,
            "axes.titlecolor": INK,
            "xtick.color": INK,
            "ytick.color": INK,
            "font.size": 10,
            "axes.grid": True,
            "grid.color": GRID,
            "grid.linewidth": 0.6,
            "grid.alpha": 0.7,
        }
    )


def _label(feature: str) -> str:
    return feature.removeprefix("twpub_")


def _save(fig: plt.Figure, path: Path) -> None:
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _read_fold_tables(root: Path) -> tuple[list[Path], pl.DataFrame, pl.DataFrame]:
    fold_dirs = sorted(path for path in root.glob("fold_*_test") if path.is_dir())
    if not fold_dirs:
        raise FileNotFoundError(f"No fold_*_test directories found below {root}")

    frames: list[pl.DataFrame] = []
    for fold_dir in fold_dirs:
        fold_id = int(fold_dir.name.removeprefix("fold_").removesuffix("_test"))
        path = fold_dir / "paper_tables" / "global_feature_attribution.csv"
        if not path.exists():
            raise FileNotFoundError(path)
        frames.append(pl.read_csv(path).with_columns(pl.lit(fold_id).alias("fold_id")))
    long = pl.concat(frames, how="diagonal_relaxed")
    stability_path = root / "paper_fold_stability" / "paper_tables" / "fold_feature_stability.csv"
    if stability_path.exists():
        stability = pl.read_csv(stability_path)
    else:
        stability = (
            long.group_by("feature")
            .agg(
                pl.col("mean_available_share").mean().alias("mean_share"),
                pl.col("mean_available_share").std().fill_null(0.0).alias("std_share"),
            )
            .sort("mean_share", descending=True)
            .with_row_index("mean_rank", offset=1)
        )
    return fold_dirs, long, stability


def _importance_pages(stability: pl.DataFrame, output: Path, page_size: int) -> list[Path]:
    paths: list[Path] = []
    rows = stability.sort(["mean_rank", "mean_share"], descending=[False, True]).to_dicts()
    pages = math.ceil(len(rows) / page_size)
    xmax = max(float(row["mean_share"]) + float(row.get("std_share") or 0.0) for row in rows) * 1.08
    for page in range(pages):
        chunk = rows[page * page_size : (page + 1) * page_size]
        labels = [_label(str(row["feature"])) for row in chunk][::-1]
        means = np.array([float(row["mean_share"]) * 100.0 for row in chunk][::-1])
        stds = np.array([float(row.get("std_share") or 0.0) * 100.0 for row in chunk][::-1])
        fig, ax = plt.subplots(figsize=(13, max(9, 0.34 * len(chunk) + 2.6)))
        y = np.arange(len(chunk))
        ax.barh(y, means, xerr=stds, color=BLUE, edgecolor="#244F70", alpha=0.9, capsize=2)
        ax.set_yticks(y, labels, fontsize=8)
        ax.set_xlim(0, xmax * 100.0)
        ax.set_xlabel("Mean attribution share across 21 folds (%)")
        ax.set_title(
            f"Complete feature importance — page {page + 1}/{pages}",
            loc="left",
            weight="bold",
            pad=26,
        )
        ax.text(
            0,
            1.01,
            "Mean of Gradient, Integrated Gradients and SHAP; error bars show fold-to-fold SD. No features omitted.",
            transform=ax.transAxes,
            fontsize=9,
            color="#50565C",
        )
        ax.grid(axis="y", visible=False)
        path = output / f"01_feature_importance_all_p{page + 1:02d}.png"
        _save(fig, path)
        paths.append(path)
    return paths


def _fold_rank_heatmap(long: pl.DataFrame, stability: pl.DataFrame, output: Path) -> Path:
    order = stability.sort(["mean_rank", "mean_share"], descending=[False, True])["feature"].to_list()
    folds = sorted(long["fold_id"].unique().to_list())
    ranked = long.with_columns(
        pl.col("mean_available_share").rank(method="average", descending=True).over("fold_id").alias("rank")
    )
    lookup = {(row["feature"], row["fold_id"]): float(row["rank"]) for row in ranked.iter_rows(named=True)}
    denom = max(1, len(order) - 1)
    matrix = np.array(
        [[100.0 * (lookup.get((feature, fold), len(order)) - 1.0) / denom for fold in folds] for feature in order]
    )
    fig, ax = plt.subplots(figsize=(16, 31))
    image = ax.imshow(matrix, aspect="auto", interpolation="nearest", cmap="Blues_r", vmin=0, vmax=100)
    ax.grid(False)
    ax.set_xticks(np.arange(len(folds)), [str(fold) for fold in folds])
    ax.set_yticks(np.arange(len(order)), [_label(str(feature)) for feature in order], fontsize=6)
    ax.set_xlabel("Fold")
    ax.set_ylabel("Feature, ordered by cross-fold mean rank")
    ax.set_title("Feature rank stability across all 21 folds", loc="left", weight="bold", pad=26)
    ax.text(
        0,
        1.005,
        "Darker cells are more important within that fold (0 = best percentile, 100 = worst). All 131 features shown.",
        transform=ax.transAxes,
        fontsize=9,
        color="#50565C",
    )
    colorbar = fig.colorbar(image, ax=ax, fraction=0.018, pad=0.015)
    colorbar.set_label("Within-fold rank percentile (lower is better)")
    path = output / "02_feature_rank_by_fold_all.png"
    _save(fig, path)
    return path


def _method_pages(long: pl.DataFrame, stability: pl.DataFrame, output: Path, page_size: int) -> list[Path]:
    order = stability.sort(["mean_rank", "mean_share"], descending=[False, True])["feature"].to_list()
    means = long.group_by("feature").agg(
        pl.col("gradient_share").mean().alias("Gradient"),
        pl.col("integrated_gradients_share").mean().alias("Integrated Gradients"),
        pl.col("shap_share").mean().alias("SHAP"),
    )
    lookup = {row["feature"]: row for row in means.iter_rows(named=True)}
    pages = math.ceil(len(order) / page_size)
    paths: list[Path] = []
    for page in range(pages):
        features = order[page * page_size : (page + 1) * page_size]
        matrix = np.array(
            [[100.0 * float(lookup[feature][method]) for method in METHOD_COLORS] for feature in features]
        )
        fig, ax = plt.subplots(figsize=(10, max(9, 0.34 * len(features) + 2.8)))
        image = ax.imshow(matrix, aspect="auto", interpolation="nearest", cmap="YlOrBr", vmin=0)
        ax.grid(False)
        ax.set_xticks(np.arange(3), list(METHOD_COLORS), fontsize=9)
        ax.set_yticks(np.arange(len(features)), [_label(str(feature)) for feature in features], fontsize=8)
        for y in range(len(features)):
            for x in range(3):
                value = matrix[y, x]
                text_color = "white" if value >= 0.58 * float(np.nanmax(matrix)) else INK
                ax.text(x, y, f"{value:.2f}", ha="center", va="center", fontsize=6, color=text_color)
        ax.set_title(
            f"Attribution method agreement — page {page + 1}/{pages}",
            loc="left",
            weight="bold",
            pad=26,
        )
        ax.text(
            0,
            1.01,
            "Cell values are mean attribution share across 21 folds (%). Compare rows horizontally; no features omitted.",
            transform=ax.transAxes,
            fontsize=9,
            color="#50565C",
        )
        colorbar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.03)
        colorbar.set_label("Mean attribution share (%)")
        path = output / f"03_method_agreement_all_p{page + 1:02d}.png"
        _save(fig, path)
        paths.append(path)
    return paths


def _lookback_plot(fold_dirs: list[Path], output: Path) -> Path:
    records: list[pl.DataFrame] = []
    for fold_dir in fold_dirs:
        fold_id = int(fold_dir.name.removeprefix("fold_").removesuffix("_test"))
        gradient = pl.read_csv(fold_dir / "time_importance_gradient.csv").select(
            "lookback_from_end", pl.col("share").alias("Gradient")
        )
        ig = pl.read_csv(fold_dir / "time_importance_integrated_gradients.csv").select(
            "lookback_from_end", pl.col("share").alias("Integrated Gradients")
        )
        records.append(gradient.join(ig, on="lookback_from_end").with_columns(pl.lit(fold_id).alias("fold_id")))
    data = pl.concat(records)
    summary = data.group_by("lookback_from_end").agg(
        pl.col("Gradient").mean().alias("Gradient_mean"),
        pl.col("Gradient").std().fill_null(0.0).alias("Gradient_std"),
        pl.col("Integrated Gradients").mean().alias("Integrated Gradients_mean"),
        pl.col("Integrated Gradients").std().fill_null(0.0).alias("Integrated Gradients_std"),
    ).sort("lookback_from_end", descending=True)
    x = summary["lookback_from_end"].to_numpy()
    fig, ax = plt.subplots(figsize=(13, 6.5))
    for method in METHOD_COLORS.keys():
        if method == "SHAP":
            continue
        mean = summary[f"{method}_mean"].to_numpy() * 100.0
        std = summary[f"{method}_std"].to_numpy() * 100.0
        ax.plot(x, mean, label=method, color=METHOD_COLORS[method], linewidth=2.0, marker="o", markersize=3)
        ax.fill_between(x, np.maximum(0.0, mean - std), mean + std, color=METHOD_COLORS[method], alpha=0.14)
    ax.invert_xaxis()
    ax.set_xlabel("Trading days before the portfolio decision (31 = oldest, 0 = latest)")
    ax.set_ylabel("Mean attribution share across 21 folds (%)")
    ax.set_title("Complete 32-day lookback attribution", loc="left", weight="bold", pad=26)
    ax.text(0, 1.01, "Lines show fold means; shaded bands show fold-to-fold SD.", transform=ax.transAxes, fontsize=9, color="#50565C")
    ax.legend(frameon=False, ncol=2)
    ax.grid(axis="x", alpha=0.25)
    path = output / "04_lookback_importance_all_32_days.png"
    _save(fig, path)
    return path


def _correlation_pages(fold_dirs: list[Path], stability: pl.DataFrame, output: Path, page_size: int) -> list[Path]:
    frames: list[pl.DataFrame] = []
    for fold_dir in fold_dirs:
        frames.append(pl.read_csv(fold_dir / "feature_correlations.csv").filter(pl.col("source") == "last"))
    mean = pl.concat(frames).group_by("feature").agg(
        pl.col("score_corr").mean().alias("Score correlation"),
        pl.col("weight_corr").mean().alias("Weight correlation"),
    )
    lookup = {row["feature"]: row for row in mean.iter_rows(named=True)}
    order = stability.sort(["mean_rank", "mean_share"], descending=[False, True])["feature"].to_list()
    pages = math.ceil(len(order) / page_size)
    paths: list[Path] = []
    for page in range(pages):
        features = order[page * page_size : (page + 1) * page_size]
        y = np.arange(len(features))[::-1]
        score = np.array([float(lookup[feature]["Score correlation"]) for feature in features][::-1])
        weight = np.array([float(lookup[feature]["Weight correlation"]) for feature in features][::-1])
        labels = [_label(str(feature)) for feature in features][::-1]
        fig, ax = plt.subplots(figsize=(13, max(9, 0.34 * len(features) + 2.7)))
        ax.axvline(0, color=INK, linewidth=0.8)
        ax.scatter(score, y + 0.13, color=BLUE, s=25, label="Score correlation")
        ax.scatter(weight, y - 0.13, facecolors="white", edgecolors=ORANGE, linewidths=1.2, s=25, label="Weight correlation")
        ax.set_yticks(y, labels, fontsize=8)
        ax.set_xlim(-1, 1)
        ax.set_xlabel("Mean Pearson correlation across 21 folds")
        ax.set_title(
            f"Feature correlation with model outputs — page {page + 1}/{pages}",
            loc="left",
            weight="bold",
            pad=26,
        )
        ax.text(
            0,
            1.01,
            "Uses each feature's latest lookback value. Correlation shows association, not causal importance; no features omitted.",
            transform=ax.transAxes,
            fontsize=9,
            color="#50565C",
        )
        ax.legend(frameon=False, ncol=2, loc="lower right")
        ax.grid(axis="y", visible=False)
        path = output / f"05_feature_correlations_all_p{page + 1:02d}.png"
        _save(fig, path)
        paths.append(path)
    return paths


def _shap_quality(fold_dirs: list[Path], output: Path) -> Path:
    folds: list[int] = []
    values: list[float] = []
    for fold_dir in fold_dirs:
        table = pl.read_csv(fold_dir / "feature_importance_shap.csv")
        folds.append(int(fold_dir.name.removeprefix("fold_").removesuffix("_test")))
        values.append(float(table["surrogate_r2"][0]))
    colors = [BLUE if value >= 0.5 else GOLD if value >= 0.25 else ORANGE for value in values]
    fig, ax = plt.subplots(figsize=(13, 5.8))
    ax.bar(folds, values, color=colors, edgecolor="#244F70", linewidth=0.5)
    ax.axhline(0.5, color=INK, linestyle="--", linewidth=1.0, label="R² = 0.50 reference")
    ax.set_xticks(folds)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Fold")
    ax.set_ylabel("SHAP surrogate R²")
    ax.set_title("SHAP surrogate fidelity by fold", loc="left", weight="bold", pad=26)
    ax.text(0, 1.01, "Higher R² means the surrogate more faithfully approximates the model for that fold.", transform=ax.transAxes, fontsize=9, color="#50565C")
    ax.legend(frameon=False)
    ax.grid(axis="x", visible=False)
    path = output / "06_shap_surrogate_quality_all_folds.png"
    _save(fig, path)
    return path


def _write_gallery(output: Path, groups: list[tuple[str, list[Path], str]]) -> None:
    lines = [
        "# 21-Fold Feature Explainability 圖集",
        "",
        "這套圖保留全部 131 個 feature；分頁只是為了可讀性，不是 Top-K 篩選。",
        "",
        "執行範圍：21 folds、每 fold 均勻抽樣 64 個 test 日期、Gradient、Integrated Gradients、SHAP。",
        "",
    ]
    for title, paths, note in groups:
        lines.extend([f"## {title}", "", note, ""])
        for path in paths:
            lines.extend([f"![{path.stem}]({path.name})", ""])
    (output / "README.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="Multi-fold explainability output root")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--page-size", type=int, default=33)
    args = parser.parse_args()
    root = args.root.resolve()
    output = (args.output or (root / "feature_scan_plots")).resolve()
    output.mkdir(parents=True, exist_ok=True)
    _style()
    fold_dirs, long, stability = _read_fold_tables(root)

    importance = _importance_pages(stability, output, args.page_size)
    fold_heatmap = _fold_rank_heatmap(long, stability, output)
    methods = _method_pages(long, stability, output, args.page_size)
    lookback = _lookback_plot(fold_dirs, output)
    correlations = _correlation_pages(fold_dirs, stability, output, args.page_size)
    shap_quality = _shap_quality(fold_dirs, output)
    _write_gallery(
        output,
        [
            ("完整 Feature 重要度", importance, "藍色長條是跨 fold 平均重要度，誤差棒是 fold 間標準差。"),
            ("跨 Fold 排名穩定度", [fold_heatmap], "深色代表該 feature 在該 fold 排名較前。"),
            ("三種歸因方法比較", methods, "同一列橫向比較 Gradient、IG 與 SHAP；三者一致時可信度較高。"),
            ("Lookback 32 天", [lookback], "觀察模型較依賴近期資料或較早資料。"),
            ("Feature 與輸出的相關性", correlations, "實心點是 score，空心點是最終 weight；相關不等於因果。"),
            ("SHAP 代理品質", [shap_quality], "R² 越高，該 fold 的 SHAP 代理模型越可信。"),
        ],
    )
    print(f"chart gallery: {output / 'README.md'}")
    print(f"png files: {len(list(output.glob('*.png')))}")


if __name__ == "__main__":
    main()
