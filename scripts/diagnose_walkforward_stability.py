from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pyarrow.parquet as pq


@dataclass(frozen=True)
class RunSpec:
    name: str
    path: Path


@dataclass(frozen=True)
class WeightDiagnostics:
    rows: int
    symbols: int
    avg_gross: float
    avg_net: float
    avg_positions: float
    avg_effective_positions: float
    avg_max_abs_weight: float
    p95_max_abs_weight: float
    top_symbol: str
    top_date: str
    top_weight: float


def _parse_run(raw: str) -> RunSpec:
    if "=" in raw:
        name, path = raw.split("=", 1)
        name = name.strip()
        if not name:
            raise argparse.ArgumentTypeError(f"run name is empty: {raw!r}")
    else:
        path = raw
        name = Path(path).name
    return RunSpec(name=name, path=Path(path).expanduser())


def _parse_folds(raw: str) -> list[int]:
    folds: list[int] = []
    for item in str(raw).split(","):
        item = item.strip()
        if not item:
            continue
        folds.append(int(item))
    if not folds:
        raise argparse.ArgumentTypeError("at least one fold is required")
    return list(dict.fromkeys(folds))


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _metric(metrics: dict[str, Any], split: str, key: str) -> float | None:
    section = metrics.get(f"{split}_metrics")
    if not isinstance(section, dict):
        return None
    return _finite_float(section.get(key))


def _column_to_float64(table, name: str) -> np.ndarray:
    column = table.column(name).combine_chunks()
    values = np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.float64)
    return np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)


def diagnose_weights(path: Path, *, position_epsilon: float) -> WeightDiagnostics | None:
    if not path.exists():
        return None
    table = pq.read_table(path)
    symbols = [name for name in table.column_names if name != "date"]
    if not symbols or table.num_rows == 0:
        return None

    weights = np.column_stack([_column_to_float64(table, symbol) for symbol in symbols])
    abs_weights = np.abs(weights)
    gross = abs_weights.sum(axis=1)
    net = weights.sum(axis=1)
    sum_squares = np.square(abs_weights).sum(axis=1)
    effective = np.divide(
        np.square(gross),
        sum_squares,
        out=np.zeros_like(gross),
        where=sum_squares > 0.0,
    )
    positions = (abs_weights > float(position_epsilon)).sum(axis=1)
    max_abs = abs_weights.max(axis=1)
    top_row, top_col = np.unravel_index(int(np.argmax(abs_weights)), abs_weights.shape)
    dates = table.column("date").combine_chunks().to_pylist() if "date" in table.column_names else []
    top_date = "" if not dates else str(dates[top_row])[:10]

    return WeightDiagnostics(
        rows=int(table.num_rows),
        symbols=len(symbols),
        avg_gross=float(np.mean(gross)),
        avg_net=float(np.mean(net)),
        avg_positions=float(np.mean(positions)),
        avg_effective_positions=float(np.mean(effective)),
        avg_max_abs_weight=float(np.mean(max_abs)),
        p95_max_abs_weight=float(np.quantile(max_abs, 0.95)),
        top_symbol=symbols[top_col],
        top_date=top_date,
        top_weight=float(weights[top_row, top_col]),
    )


def _epoch_curve_path(run_dir: Path, train_years: Iterable[int]) -> Path | None:
    years = [int(year) for year in train_years]
    exact = run_dir / ("train_" + "-".join(str(year) for year in years)) / "epoch_curve.jsonl"
    if exact.exists():
        return exact
    candidates = sorted(run_dir.glob("train_*/epoch_curve.jsonl"), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def diagnose_epochs(
    run_dir: Path,
    train_years: Iterable[int],
    *,
    batch_size: int | None = None,
) -> dict[str, Any]:
    path = _epoch_curve_path(run_dir, train_years)
    if path is None:
        return {}
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                records.append(payload)
    if not records:
        return {}

    finite_val = [row for row in records if _finite_float(row.get("val_mean")) is not None]
    best = min(finite_val, key=lambda row: float(row["val_mean"])) if finite_val else {}
    steady = records[1:] if len(records) > 1 else records
    steady = steady[-min(5, len(steady)) :]

    def median(key: str) -> float | None:
        values = [_finite_float(row.get(key)) for row in steady]
        finite = [value for value in values if value is not None]
        return statistics.median(finite) if finite else None

    train_total = median("train_total_s")
    train_batches = median("train_batches")
    batches_per_s = (
        float(train_batches / train_total)
        if train_batches is not None and train_total not in {None, 0.0}
        else None
    )
    return {
        "epochs_completed": len(records),
        "best_epoch_from_curve": int(best["epoch"]) if best else None,
        "best_val_mean_from_curve": _finite_float(best.get("val_mean")) if best else None,
        "steady_epoch_wall_s_median": median("epoch_wall_s"),
        "steady_train_s_median": train_total,
        "steady_batches_per_s_median": batches_per_s,
        "steady_padded_samples_per_s_median": (
            float(batches_per_s * batch_size)
            if batches_per_s is not None and batch_size is not None
            else None
        ),
        "timing_synchronized": bool(median("timing_synchronized") or 0.0),
        "epoch_curve": str(path),
    }


def diagnose_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import torch

        checkpoint = torch.load(path, map_location="cpu", weights_only=True)
    except (ImportError, OSError, RuntimeError, TypeError, ValueError):
        return {}
    if not isinstance(checkpoint, dict):
        return {}

    model_state = checkpoint.get("model_state_dict")
    state_parameter_count = None
    if isinstance(model_state, dict):
        state_parameter_count = sum(
            int(value.numel()) for value in model_state.values() if hasattr(value, "numel")
        )
    manifest = checkpoint.get("experiment_manifest")
    batch_size = None
    if isinstance(manifest, dict):
        try:
            batch_size = int(manifest["contracts"]["training"]["batching"]["batch_size_train"])
        except (KeyError, TypeError, ValueError):
            batch_size = None
    return {
        "checkpoint_best_epoch": int(checkpoint["epoch"]) if checkpoint.get("epoch") is not None else None,
        "batch_size_train": batch_size,
        "model_state_parameter_count": state_parameter_count,
    }


def _fold_row(run: RunSpec, fold: int, *, position_epsilon: float) -> dict[str, Any]:
    fold_dir = run.path / f"fold_{fold:02d}"
    metrics = _load_json(fold_dir / "metrics.json")
    row: dict[str, Any] = {
        "run": run.name,
        "run_dir": str(run.path),
        "fold": int(fold),
        "status": "missing",
    }
    if metrics is None:
        return row

    complete = _load_json(fold_dir / "fold_complete.json")
    row.update(
        {
            "status": "complete" if complete and complete.get("status") == "complete" else "metrics_only",
            "train_years": ",".join(str(year) for year in metrics.get("train_years", [])),
            "val_years": ",".join(str(year) for year in metrics.get("val_years", [])),
            "test_years": ",".join(str(year) for year in metrics.get("test_years", [])),
            "best_val_loss": _finite_float(metrics.get("best_val_loss")),
        }
    )
    for split in ("val", "test"):
        for key in (
            "cumulative_return",
            "sharpe",
            "sortino",
            "max_drawdown",
            "turnover",
            "cumulative_benchmark",
        ):
            row[f"{split}_{key}"] = _metric(metrics, split, key)
        ic = metrics.get(f"{split}_ic")
        row[f"{split}_ic_mean"] = _finite_float(ic.get("ic_mean")) if isinstance(ic, dict) else None

    weights = diagnose_weights(fold_dir / "daily_weights.parquet", position_epsilon=position_epsilon)
    if weights is not None:
        row.update({f"weights_{key}": value for key, value in asdict(weights).items()})
    checkpoint = diagnose_checkpoint(fold_dir / "checkpoint_best.pt")
    row.update(checkpoint)
    row.update(
        diagnose_epochs(
            run.path,
            metrics.get("train_years", []),
            batch_size=checkpoint.get("batch_size_train"),
        )
    )
    return row


def collect_rows(runs: list[RunSpec], folds: list[int], *, position_epsilon: float) -> list[dict[str, Any]]:
    rows = [_fold_row(run, fold, position_epsilon=position_epsilon) for run in runs for fold in folds]
    for run in runs:
        by_fold = {int(row["fold"]): row for row in rows if row["run"] == run.name}
        for fold, row in by_fold.items():
            if row["status"] != "missing":
                continue
            successor = by_fold.get(fold + 1)
            if successor is None or successor.get("status") != "complete":
                continue
            val_years = str(successor.get("val_years", ""))
            test_years = str(successor.get("test_years", ""))
            if val_years and val_years == test_years:
                row["status"] = "superseded_by_final_overlap"
                row["superseded_by_fold"] = int(successor["fold"])
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _format_metric(value: Any, *, percent: bool = False) -> str:
    parsed = _finite_float(value)
    if parsed is None:
        return ""
    return f"{parsed * 100:+.2f}%" if percent else f"{parsed:.3f}"


def _write_markdown(path: Path, rows: list[dict[str, Any]], position_epsilon: float) -> None:
    lines = [
        "# Walk-Forward Stability Diagnostics",
        "",
        "This report reads saved artifacts only. Missing means no completed artifact exists; it does not imply a failed trained model. `superseded_by_final_overlap` means the next experimental fold reused validation as test and owned the same deployment dates.",
        "",
        "## Fold Metrics",
        "",
        "| run | fold | status | val years | test years | val return | test return | test Sharpe | test MDD | benchmark |",
        "| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["run"]),
                    str(row["fold"]),
                    str(row["status"]),
                    str(row.get("val_years", "")),
                    str(row.get("test_years", "")),
                    _format_metric(row.get("val_cumulative_return"), percent=True),
                    _format_metric(row.get("test_cumulative_return"), percent=True),
                    _format_metric(row.get("test_sharpe")),
                    _format_metric(row.get("test_max_drawdown"), percent=True),
                    _format_metric(row.get("test_cumulative_benchmark"), percent=True),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Portfolio Geometry",
            "",
            f"Positions use `abs(weight) > {position_epsilon:g}`. Effective positions are `(sum(abs(w))^2 / sum(w^2))`.",
            "",
            "| run | fold | avg gross | avg positions | effective positions | avg max weight | p95 max weight | largest position |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in rows:
        top = ""
        if row.get("weights_top_symbol"):
            top = (
                f"{row['weights_top_symbol']} {row.get('weights_top_date', '')} "
                f"{_format_metric(row.get('weights_top_weight'), percent=True)}"
            )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["run"]),
                    str(row["fold"]),
                    _format_metric(row.get("weights_avg_gross")),
                    _format_metric(row.get("weights_avg_positions")),
                    _format_metric(row.get("weights_avg_effective_positions")),
                    _format_metric(row.get("weights_avg_max_abs_weight"), percent=True),
                    _format_metric(row.get("weights_p95_max_abs_weight"), percent=True),
                    top,
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Training Runtime",
            "",
            "Steady-state values are medians of the final five epochs after excluding epoch 1 when possible.",
            "",
            "| run | fold | epochs | best epoch | batch | sync timing | epoch wall (s) | train (s) | samples/s | params |",
            "| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["run"]),
                    str(row["fold"]),
                    str(row.get("epochs_completed", "")),
                    str(row.get("checkpoint_best_epoch", row.get("best_epoch_from_curve", ""))),
                    str(row.get("batch_size_train", "")),
                    str(row.get("timing_synchronized", "")),
                    _format_metric(row.get("steady_epoch_wall_s_median")),
                    _format_metric(row.get("steady_train_s_median")),
                    _format_metric(row.get("steady_padded_samples_per_s_median")),
                    str(row.get("model_state_parameter_count", "")),
                ]
            )
            + " |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare walk-forward accuracy, concentration, and steady-state runtime from saved artifacts."
    )
    parser.add_argument(
        "--run",
        action="append",
        type=_parse_run,
        required=True,
        help="Run artifact directory as NAME=PATH. Repeat to compare runs.",
    )
    parser.add_argument("--folds", type=_parse_folds, default=_parse_folds("23,24,25,26"))
    parser.add_argument("--position-epsilon", type=float, default=1e-8)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/diagnostics/walkforward_stability"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = collect_rows(args.run, args.folds, position_epsilon=max(0.0, args.position_epsilon))
    _write_csv(output_dir / "fold_diagnostics.csv", rows)
    (output_dir / "fold_diagnostics.json").write_text(
        json.dumps(rows, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _write_markdown(output_dir / "report.md", rows, max(0.0, args.position_epsilon))
    print(f"wrote {output_dir / 'report.md'}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
