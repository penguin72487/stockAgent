from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import mean
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.training.trainer import _normalize_risk_objective


DEFAULT_CONFIGS = [
    "configs/markets/tw_parallel_latefold_gated_net_short12.yaml",
    "configs/markets/tw_parallel_latefold_stable.yaml",
    "configs/markets/tw_parallel_latefold_gated.yaml",
    "configs/markets/tw_parallel_latefold_gated_net.yaml",
    "configs/markets/tw_parallel_latefold_sharpck.yaml",
    "configs/markets/tw_parallel_latefold_sortino.yaml",
]


def _parse_folds(raw: str) -> list[int]:
    folds: list[int] = []
    for item in str(raw).split(","):
        item = item.strip()
        if not item:
            continue
        folds.append(int(item))
    return folds


def _metric(metrics: dict[str, Any], split: str, key: str) -> float | None:
    section = metrics.get(f"{split}_metrics") or {}
    value = section.get(key)
    return None if value is None else float(value)


def _load_metrics(output_dir: Path, fold: int) -> dict[str, Any] | None:
    path = output_dir / f"fold_{fold:02d}" / "metrics.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _fold_row(config_path: Path, fold: int) -> dict[str, Any]:
    config = load_config(config_path)
    output_dir = Path(config.runner.output_dir)
    metrics = _load_metrics(output_dir, fold)
    objective = _normalize_risk_objective(config.training.loss_type)
    row: dict[str, Any] = {
        "config": str(config_path),
        "experiment_name": config.experiment_name,
        "objective": objective,
        "raw_loss_type": config.training.loss_type,
        "output_dir": str(output_dir),
        "fold": fold,
        "status": "missing",
    }
    if metrics is None:
        return row

    complete = (output_dir / f"fold_{fold:02d}" / "fold_complete.json").exists()
    val_return = _metric(metrics, "val", "cumulative_return")
    test_return = _metric(metrics, "test", "cumulative_return")
    val_sharpe = _metric(metrics, "val", "sharpe")
    test_sharpe = _metric(metrics, "test", "sharpe")
    val_sortino = _metric(metrics, "val", "sortino")
    test_sortino = _metric(metrics, "test", "sortino")
    row.update(
        {
            "status": "complete" if complete else "metrics_only",
            "best_val_loss": float(metrics.get("best_val_loss", 0.0)),
            "train_years": "|".join(str(year) for year in metrics.get("train_years", [])),
            "val_years": "|".join(str(year) for year in metrics.get("val_years", [])),
            "test_years": "|".join(str(year) for year in metrics.get("test_years", [])),
            "val_cumulative_return": val_return,
            "val_sharpe": val_sharpe,
            "val_sortino": val_sortino,
            "val_max_drawdown": _metric(metrics, "val", "max_drawdown"),
            "val_turnover": _metric(metrics, "val", "turnover"),
            "test_cumulative_return": test_return,
            "test_sharpe": test_sharpe,
            "test_sortino": test_sortino,
            "test_max_drawdown": _metric(metrics, "test", "max_drawdown"),
            "test_turnover": _metric(metrics, "test", "turnover"),
            "val_profitable": bool(val_return is not None and val_return > 0.0),
            "test_profitable": bool(test_return is not None and test_return > 0.0),
            "test_positive_sharpe": bool(test_sharpe is not None and test_sharpe > 0.0),
            "test_positive_sortino": bool(test_sortino is not None and test_sortino > 0.0),
        }
    )
    row["profitability_gate"] = bool(row["val_profitable"] and row["test_profitable"])
    row["risk_gate"] = bool(row["test_positive_sharpe"] and row["test_positive_sortino"])
    row["all_gates"] = bool(row["status"] == "complete" and row["profitability_gate"] and row["risk_gate"])
    return row


def _finite_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if value is None or value == "":
            continue
        values.append(float(value))
    return values


def _summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row["config"]), []).append(row)

    summaries: list[dict[str, Any]] = []
    for config_path, group in groups.items():
        completed = [row for row in group if row.get("status") == "complete"]
        summary = {
            "config": config_path,
            "experiment_name": group[0].get("experiment_name", ""),
            "objective": group[0].get("objective", ""),
            "output_dir": group[0].get("output_dir", ""),
            "folds_requested": len(group),
            "folds_complete": len(completed),
            "folds_all_gates": sum(1 for row in group if row.get("all_gates")),
            "folds_val_profitable": sum(1 for row in group if row.get("val_profitable")),
            "folds_test_profitable": sum(1 for row in group if row.get("test_profitable")),
            "mean_test_cumulative_return": None,
            "mean_test_sharpe": None,
            "mean_test_sortino": None,
            "mean_test_turnover": None,
            "worst_test_max_drawdown": None,
        }
        for key in ("test_cumulative_return", "test_sharpe", "test_sortino", "test_turnover"):
            values = _finite_values(group, key)
            if values:
                summary[f"mean_{key}"] = mean(values)
        drawdowns = _finite_values(group, "test_max_drawdown")
        if drawdowns:
            summary["worst_test_max_drawdown"] = min(drawdowns)
        summaries.append(summary)

    return sorted(
        summaries,
        key=lambda row: (
            int(row["folds_all_gates"]),
            int(row["folds_test_profitable"]),
            float(row["mean_test_cumulative_return"] if row["mean_test_cumulative_return"] is not None else -1e9),
            float(row["mean_test_sharpe"] if row["mean_test_sharpe"] is not None else -1e9),
        ),
        reverse=True,
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _write_markdown(path: Path, summaries: list[dict[str, Any]], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Late-Fold Objective Comparison",
        "",
        "Gate definition: complete fold, validation cumulative return > 0, test cumulative return > 0, test Sharpe > 0, and test Sortino > 0.",
        "",
        "## Summary",
        "",
        "| rank | objective | complete | all_gates | test_profit | mean_test_return | mean_test_sharpe | mean_test_sortino | worst_test_mdd | output_dir |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for idx, row in enumerate(summaries, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    _fmt(row["objective"]),
                    f"{row['folds_complete']}/{row['folds_requested']}",
                    _fmt(row["folds_all_gates"]),
                    _fmt(row["folds_test_profitable"]),
                    _fmt(row["mean_test_cumulative_return"]),
                    _fmt(row["mean_test_sharpe"]),
                    _fmt(row["mean_test_sortino"]),
                    _fmt(row["worst_test_max_drawdown"]),
                    _fmt(row["output_dir"]),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Fold Detail",
            "",
            "| config | fold | status | val_return | test_return | test_sharpe | test_sortino | test_mdd | all_gates |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    Path(str(row["config"])).stem,
                    _fmt(row["fold"]),
                    _fmt(row["status"]),
                    _fmt(row.get("val_cumulative_return")),
                    _fmt(row.get("test_cumulative_return")),
                    _fmt(row.get("test_sharpe")),
                    _fmt(row.get("test_sortino")),
                    _fmt(row.get("test_max_drawdown")),
                    _fmt(row.get("all_gates")),
                ]
            )
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare fold24-26 TW late-fold objective artifacts.")
    parser.add_argument("--configs", nargs="+", default=DEFAULT_CONFIGS)
    parser.add_argument("--folds", default="24,25,26")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/markets/tw_latefold_objective_comparison"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    folds = _parse_folds(args.folds)
    config_paths = [Path(path) for path in args.configs]
    rows = [_fold_row(config_path, fold) for config_path in config_paths for fold in folds]
    summaries = _summary_rows(rows)

    output_dir: Path = args.output_dir
    _write_csv(output_dir / "fold_metrics.csv", rows)
    _write_csv(output_dir / "summary.csv", summaries)
    _write_markdown(output_dir / "comparison.md", summaries, rows)
    print(f"wrote {output_dir / 'comparison.md'}")


if __name__ == "__main__":
    main()
