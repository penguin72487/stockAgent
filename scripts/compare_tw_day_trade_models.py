from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


ROOT = Path("artifacts/model_experiments/tw_day_trade")
MODELS = (
    ("MLP", "mlp"),
    ("FT-Transformer", "ft_transformer"),
    ("Tabular ResNet", "tabular_resnet"),
    ("TCN Hybrid ResNet", "tcn_hybrid_tabular_resnet"),
    ("LightGBM", "lightgbm"),
    ("XGBoost", "xgboost"),
)


def main() -> None:
    output = ROOT / "comparison"
    output.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, float | str]] = []
    curves: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    benchmark: tuple[np.ndarray, np.ndarray] | None = None

    for label, directory in MODELS:
        fold = ROOT / directory / "fold_01"
        metrics = json.loads((fold / "metrics.json").read_text())
        test = metrics["test_metrics"]
        ic = metrics["test_ic"]
        rows.append(
            {
                "model": label,
                "cumulative_return": float(test["cumulative_return"]),
                "cagr": float(test["cagr"]),
                "sharpe": float(test["sharpe"]),
                "sortino": float(test["sortino"]),
                "max_drawdown": float(test["max_drawdown"]),
                "turnover": float(test["turnover"]),
                "daily_hit_rate": float(test["daily_hit_rate"]),
                "ic_mean": float(ic["ic_mean"]),
                "ic_ir": float(ic["ic_ir"]),
                "best_val_loss": float(metrics["best_val_loss"]),
            }
        )
        with np.load(fold / "test_backtest.npz", allow_pickle=False) as data:
            dates = data["dates"].astype("datetime64[D]")
            strategy = np.cumprod(1.0 + data["strategy_returns"].astype(np.float64))
            curves[label] = dates, strategy
            if benchmark is None:
                benchmark = (
                    dates,
                    np.cumprod(1.0 + data["benchmark_returns"].astype(np.float64)),
                )

    fields = list(rows[0])
    with (output / "model_metrics.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    (output / "model_metrics.json").write_text(
        json.dumps(rows, indent=2, ensure_ascii=False) + "\n"
    )

    fig, ax = plt.subplots(figsize=(13, 7))
    for label, (dates, equity) in curves.items():
        ax.plot(dates, equity, label=label, linewidth=1.7)
    if benchmark is not None:
        ax.plot(benchmark[0], benchmark[1], label="2330 benchmark", color="black", linestyle="--")
    ax.set_yscale("log")
    ax.set_title("TW Day-Trade Model Comparison — Canonical Test Equity (2016–2026)")
    ax.set_ylabel("Growth of 1 (log scale)")
    ax.grid(alpha=0.25)
    ax.legend(ncol=2)
    fig.tight_layout()
    fig.savefig(output / "equity_curves_log.png", dpi=180)
    plt.close(fig)

    labels = [str(row["model"]) for row in rows]
    metric_specs = (
        ("sharpe", "Sharpe", False),
        ("sortino", "Sortino", False),
        ("cagr", "CAGR", True),
        ("max_drawdown", "Max drawdown", True),
        ("turnover", "Mean turnover", False),
        ("ic_mean", "Mean IC", False),
    )
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    for ax, (key, title, percent) in zip(axes.flat, metric_specs, strict=True):
        values = np.asarray([float(row[key]) for row in rows])
        bars = ax.bar(labels, values)
        ax.axhline(0.0, color="black", linewidth=0.7)
        ax.set_title(title)
        ax.tick_params(axis="x", rotation=35)
        for bar, value in zip(bars, values, strict=True):
            text = f"{value:.1%}" if percent else f"{value:.3f}"
            ax.annotate(text, (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        ha="center", va="bottom" if value >= 0 else "top", fontsize=8)
    fig.suptitle("TW Day-Trade Model Metrics — Same Lookback 32 / Fold 1")
    fig.tight_layout()
    fig.savefig(output / "model_metrics.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    main()
