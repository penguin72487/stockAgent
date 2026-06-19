from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from stockagent.backtest.report import (
    generate_annual_report,
    plot_annual_performance,
    plot_equity_curve,
    plot_equity_curve_log,
    plot_first_year_fold_metric_bars,
    plot_first_year_turnover_concentration,
    plot_fold_first_year_returns,
)
from stockagent.backtest.simulator import BacktestResult

_CURVE_FILENAMES = ("epoch_curve.parquet", "epoch_curve.jsonl", "epoch_curve.csv")
_BACKTEST_ARTIFACT_FILENAME = "test_backtest.npz"
_STALE_FOLD_LOG_PLOT_FILENAMES = ("equity_curve_log.png", "leverage_equity_curve_log.png")
_STALE_WALKFORWARD_LOG_PLOT_FILENAMES = (
    "walkforward_equity_curve_log.png",
    "walkforward_equity_curve_log10.png",
    "walkforward_first_year_cumulative_returns.png",
    "walkforward_first_test_year_only.png",
)
_REPORT_PARQUET_FILENAMES = (
    "attention_capture_summary.parquet",
    "daily_portfolio_returns.parquet",
    "daily_weights.parquet",
    "edge_metrics.parquet",
    "holdings.parquet",
    "integer_share_daily_portfolio_returns.parquet",
    "integer_share_daily_weights.parquet",
    "role_embeddings.parquet",
    "shock_summary.parquet",
    "source_summary.parquet",
    "target_summary.parquet",
    "top_edges.parquet",
)
_DEFAULT_REPORT_CSV_BATCH_SIZE = 65_536


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all epoch loss and timing curves by default."
    )
    parser.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root directory to recursively search for epoch_curve.parquet/jsonl/csv when --curve-file is omitted.",
    )
    parser.add_argument(
        "--curve-file",
        type=str,
        default="",
        help="Optional single epoch_curve.parquet/jsonl/csv. If omitted, plot every curve under --artifacts-root.",
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
        "--write-parquet-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When reading jsonl/csv, also write <curve_dir>/epoch_curve.parquet for faster reloads.",
    )
    parser.add_argument(
        "--export-report-csvs",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Convert parquet report tables under --artifacts-root to same-name CSVs after plotting.",
    )
    parser.add_argument(
        "--force-report-csvs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Rewrite report CSV exports even when the CSV is newer than the parquet source.",
    )
    parser.add_argument(
        "--report-csv-batch-size",
        type=int,
        default=_DEFAULT_REPORT_CSV_BATCH_SIZE,
        help="Rows per streaming parquet->CSV batch for report table exports.",
    )
    parser.add_argument(
        "--skip-plots",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Skip epoch curve plotting; useful with --export-report-csvs for CSV export only.",
    )
    parser.add_argument(
        "--redraw-backtest-plots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also redraw fold backtest plots from test_backtest.npz files under --artifacts-root.",
    )
    return parser.parse_args()


def _find_curve_files(root: Path) -> list[Path]:
    candidates: list[Path] = []
    for filename in _CURVE_FILENAMES:
        candidates.extend(root.rglob(filename))
    if not candidates:
        raise FileNotFoundError(f"No epoch_curve.parquet/jsonl/csv found under {root}")

    priority = {".parquet": 0, ".jsonl": 1, ".csv": 2}
    selected: dict[Path, Path] = {}
    for candidate in candidates:
        key = candidate.with_suffix("")
        current = selected.get(key)
        if current is None:
            selected[key] = candidate
            continue
        try:
            candidate_mtime = candidate.stat().st_mtime
        except OSError:
            candidate_mtime = 0.0
        try:
            current_mtime = current.stat().st_mtime
        except OSError:
            current_mtime = 0.0
        if candidate_mtime > current_mtime:
            selected[key] = candidate
        elif candidate_mtime == current_mtime and priority.get(candidate.suffix, 99) < priority.get(current.suffix, 99):
            selected[key] = candidate
    return sorted(selected.values(), key=lambda p: (p.parent.as_posix(), p.stat().st_mtime))


def _load_jsonl_curve(curve_path: Path) -> list[dict]:
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


def _load_curve(curve_path: Path) -> list[dict]:
    suffix = curve_path.suffix.lower()
    if suffix == ".jsonl":
        return _load_jsonl_curve(curve_path)
    if suffix not in {".csv", ".parquet"}:
        raise ValueError(f"Unsupported curve file extension: {curve_path}")

    if suffix == ".csv":
        frame = pl.read_csv(curve_path)
    else:
        frame = pl.from_arrow(pq.read_table(curve_path))
    if frame.is_empty():
        raise ValueError(f"Curve file is empty: {curve_path}")
    if "epoch" not in frame.columns:
        raise ValueError(f"Curve file is missing required column 'epoch': {curve_path}")
    return frame.with_columns(pl.all().fill_nan(None)).to_dicts()


def _write_curve_parquet_cache(curve_path: Path, rows: list[dict]) -> tuple[Path | None, float]:
    if curve_path.suffix.lower() == ".parquet":
        return None, 0.0
    start = time.perf_counter()

    parquet_path = curve_path.with_suffix(".parquet")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, parquet_path, compression="snappy")
    return parquet_path, float(time.perf_counter() - start)


def _arrow_type_is_nested(dtype: pa.DataType) -> bool:
    return (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or pa.types.is_list_view(dtype)
        or pa.types.is_large_list_view(dtype)
        or pa.types.is_map(dtype)
        or pa.types.is_struct(dtype)
    )


def _csv_schema_from_arrow_schema(schema: pa.Schema) -> pa.Schema:
    fields = [
        pa.field(field.name, pa.string()) if _arrow_type_is_nested(field.type) else field
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _record_batch_for_csv(batch: pa.RecordBatch) -> pa.RecordBatch:
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    for field, column in zip(batch.schema, batch.columns, strict=False):
        if _arrow_type_is_nested(field.type):
            values = [
                None if value is None else json.dumps(value, ensure_ascii=False)
                for value in column.to_pylist()
            ]
            arrays.append(pa.array(values, type=pa.string()))
            fields.append(pa.field(field.name, pa.string()))
        else:
            arrays.append(column)
            fields.append(field)
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=batch.schema.metadata))


def _write_empty_csv_from_schema(csv_path: Path, schema: pa.Schema) -> None:
    csv_schema = _csv_schema_from_arrow_schema(schema)
    arrays = [pa.array([], type=field.type) for field in csv_schema]
    table = pa.Table.from_arrays(arrays, schema=csv_schema)
    pacsv.write_csv(table, csv_path)


def _write_parquet_table_as_csv(
    parquet_path: Path,
    csv_path: Path,
    *,
    batch_size: int = _DEFAULT_REPORT_CSV_BATCH_SIZE,
) -> None:
    parquet_file = pq.ParquetFile(parquet_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = csv_path.with_name(csv_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    writer: pacsv.CSVWriter | None = None
    try:
        for batch in parquet_file.iter_batches(batch_size=max(1, int(batch_size))):
            csv_batch = _record_batch_for_csv(batch)
            if writer is None:
                writer = pacsv.CSVWriter(str(tmp_path), csv_batch.schema)
            writer.write_batch(csv_batch)
        if writer is None:
            _write_empty_csv_from_schema(tmp_path, parquet_file.schema_arrow)
        else:
            writer.close()
            writer = None
        tmp_path.replace(csv_path)
    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def export_report_csvs(
    root: Path,
    *,
    force: bool = False,
    quiet: bool = False,
    batch_size: int = _DEFAULT_REPORT_CSV_BATCH_SIZE,
) -> dict[str, float | int | str]:
    """Export training report parquet tables to CSV outside the training hot path."""
    total_start = time.perf_counter()
    root = root if root.is_dir() else root.parent
    candidates = sorted(
        path
        for path in root.rglob("*.parquet")
        if path.name in _REPORT_PARQUET_FILENAMES
    )
    written = 0
    skipped = 0
    for parquet_path in candidates:
        csv_path = parquet_path.with_suffix(".csv")
        if csv_path.exists() and not force:
            try:
                if csv_path.stat().st_mtime >= parquet_path.stat().st_mtime:
                    skipped += 1
                    continue
            except OSError:
                pass
        _write_parquet_table_as_csv(parquet_path, csv_path, batch_size=batch_size)
        written += 1
        if not quiet:
            print(f"exported csv: {csv_path}")
    elapsed = float(time.perf_counter() - total_start)
    result = {
        "root": str(root),
        "candidates": int(len(candidates)),
        "written": int(written),
        "skipped": int(skipped),
        "total_s": elapsed,
    }
    if not quiet:
        print(
            "report_csv_export: "
            f"candidates={len(candidates)} written={written} skipped={skipped} total={elapsed:.3f}s"
        )
    return result


def _fold_id_from_dir(fold_dir: Path) -> int:
    name = fold_dir.name
    if name.startswith("fold_"):
        try:
            return int(name[len("fold_") :])
        except ValueError:
            pass
    return 10**9


def _find_backtest_artifacts(root: Path) -> list[Path]:
    root = root if root.is_dir() else root.parent
    paths = list(root.rglob(_BACKTEST_ARTIFACT_FILENAME))
    return sorted(paths, key=lambda path: (_fold_id_from_dir(path.parent), path.parent.as_posix()))


def _load_backtest_artifact(backtest_path: Path) -> tuple[BacktestResult, np.ndarray]:
    with np.load(backtest_path) as data:
        result = BacktestResult(
            strategy_returns=data["strategy_returns"].astype(np.float32),
            benchmark_returns=data["benchmark_returns"].astype(np.float32),
            turnovers=data["turnovers"].astype(np.float32),
            weights_history=data["weights_history"].astype(np.float32),
        )
        dates = np.asarray(data["dates"])
    return result, dates


def _remove_stale_log_plot_files(fold_dir: Path) -> None:
    for filename in _STALE_FOLD_LOG_PLOT_FILENAMES:
        path = fold_dir / filename
        if path.exists():
            path.unlink()


def _remove_stale_walkforward_log_plot_files(root: Path) -> None:
    for filename in _STALE_WALKFORWARD_LOG_PLOT_FILENAMES:
        path = root / filename
        if path.exists():
            path.unlink()


def _first_year_mask(dates: np.ndarray) -> np.ndarray:
    date_values = np.asarray(dates, dtype="datetime64[D]")
    if date_values.size == 0:
        return np.asarray([], dtype=bool)
    years = date_values.astype(object)
    year_values = np.asarray([date.year for date in years], dtype=np.int64)
    return year_values == int(year_values.min())


def _write_single_backtest_plots(
    backtest_path: Path,
    result: BacktestResult,
    dates: np.ndarray,
) -> None:
    fold_dir = backtest_path.parent
    _remove_stale_log_plot_files(fold_dir)
    with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as handle:
        handle.write(generate_annual_report(result, dates))
    plot_equity_curve(result, dates, fold_dir / "equity_curve.png")
    plot_equity_curve_log(result, dates, fold_dir / "equity_curve_log10.png")
    plot_annual_performance(result, dates, fold_dir / "annual_performance.png")
    plot_equity_curve(result, dates, fold_dir / "leverage_equity_curve.png")
    plot_equity_curve_log(result, dates, fold_dir / "leverage_equity_curve_log10.png")
    plot_annual_performance(result, dates, fold_dir / "leverage_annual_performance.png")


def _write_walkforward_backtest_plots(
    root: Path,
    loaded: list[tuple[int, BacktestResult, np.ndarray]],
) -> None:
    _remove_stale_walkforward_log_plot_files(root)
    all_first_year_fold_ids: list[int] = []
    all_first_year_dates: list[np.ndarray] = []
    all_first_year_strategy_log: list[np.ndarray] = []
    all_first_year_baseline_log: list[np.ndarray] = []
    all_first_year_turnovers: list[np.ndarray] = []
    all_first_year_weights: list[np.ndarray] = []

    for fold_id, result, dates in sorted(loaded, key=lambda item: item[0]):
        mask = _first_year_mask(dates)
        if not mask.any():
            continue
        all_first_year_fold_ids.append(int(fold_id))
        all_first_year_dates.append(np.asarray(dates)[mask])
        all_first_year_strategy_log.append(
            np.nan_to_num(result.strategy_returns[mask], nan=0.0).astype(np.float64)
        )
        all_first_year_baseline_log.append(
            np.nan_to_num(result.benchmark_returns[mask], nan=0.0).astype(np.float64)
        )
        all_first_year_turnovers.append(
            np.nan_to_num(result.turnovers[mask], nan=0.0).astype(np.float64)
        )
        all_first_year_weights.append(
            np.nan_to_num(result.weights_history[mask], nan=0.0).astype(np.float64)
        )

    if not all_first_year_dates:
        return
    plot_fold_first_year_returns(
        all_first_year_dates,
        all_first_year_strategy_log,
        all_first_year_baseline_log,
        root / "walkforward_first_year_log10_nav.png",
    )
    plot_first_year_fold_metric_bars(
        all_first_year_fold_ids,
        all_first_year_strategy_log,
        all_first_year_baseline_log,
        root / "walkforward_first_year_fold_metrics.png",
    )
    plot_first_year_turnover_concentration(
        all_first_year_fold_ids,
        all_first_year_turnovers,
        all_first_year_weights,
        root / "walkforward_first_year_turnover_concentration.png",
    )


def redraw_backtest_plots(
    root: Path,
    *,
    quiet: bool = False,
) -> dict[str, float | int | str]:
    total_start = time.perf_counter()
    root = root if root.is_dir() else root.parent
    backtest_paths = _find_backtest_artifacts(root)
    if not backtest_paths:
        result = {
            "root": str(root),
            "backtest_artifacts": 0,
            "written_folds": 0,
            "total_s": float(time.perf_counter() - total_start),
        }
        if not quiet:
            print(f"backtest_plot_redraw skipped: no {_BACKTEST_ARTIFACT_FILENAME} found under {root}")
        return result

    loaded: list[tuple[int, BacktestResult, np.ndarray]] = []
    for backtest_path in backtest_paths:
        fold_id = _fold_id_from_dir(backtest_path.parent)
        result, dates = _load_backtest_artifact(backtest_path)
        _write_single_backtest_plots(backtest_path, result, dates)
        loaded.append((fold_id, result, dates))
        if not quiet:
            print(f"redrew backtest plots: {backtest_path.parent}")

    _write_walkforward_backtest_plots(root, loaded)
    timing = {
        "root": str(root),
        "backtest_artifacts": int(len(backtest_paths)),
        "written_folds": int(len(loaded)),
        "total_s": float(time.perf_counter() - total_start),
    }
    timing_json = root / "backtest_plot_redraw_timing.json"
    timing_json.write_text(json.dumps(timing, indent=2, ensure_ascii=False), encoding="utf-8")
    timing["timing_json"] = str(timing_json)
    if not quiet:
        print(
            "backtest_plot_redraw: "
            f"folds={len(loaded)} total={float(timing['total_s']):.3f}s timing={timing_json}"
        )
    return timing


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


def _log10_positive_values(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64)
    out = np.full_like(arr, np.nan, dtype=np.float64)
    mask = np.isfinite(arr) & (arr > 0.0)
    out[mask] = np.log10(arr[mask])
    return out


def _signed_log10_values(values: np.ndarray, *, linthresh: float = 1e-3) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64)
    out = np.full_like(arr, np.nan, dtype=np.float64)
    mask = np.isfinite(arr)
    scale = max(float(linthresh), np.finfo(np.float64).tiny)
    out[mask] = np.sign(arr[mask]) * np.log10(1.0 + np.abs(arr[mask]) / scale)
    return out


def _plot_loss_curve(rows: list[dict], curve_path: Path, output_path: Path, interval: int, *, quiet: bool = False) -> None:
    epochs = np.asarray([int(row.get("epoch", 0)) for row in rows], dtype=np.int64)
    train_loss = _to_float_array(rows, "train_loss")
    val_mean = _to_float_array(rows, "val_mean")
    test_mean = _to_float_array(rows, "test_mean")
    all_loss_values = np.concatenate([train_loss, val_mean, test_mean])
    finite_loss_values = all_loss_values[np.isfinite(all_loss_values)]
    if finite_loss_values.size == 0:
        train_plot = train_loss
        val_plot = val_mean
        test_plot = test_mean
        ylabel = "Loss"
    elif np.any(finite_loss_values <= 0.0):
        train_plot = _signed_log10_values(train_loss)
        val_plot = _signed_log10_values(val_mean)
        test_plot = _signed_log10_values(test_mean)
        ylabel = "signed log10(1 + abs(Loss) / 1e-3)"
    else:
        train_plot = _log10_positive_values(train_loss)
        val_plot = _log10_positive_values(val_mean)
        test_plot = _log10_positive_values(test_mean)
        ylabel = "log10(Loss)"

    fig, ax = plt.subplots(figsize=(12, 6), dpi=130)
    ax.plot(epochs, train_plot, marker="o", linewidth=1.8, markersize=4, label="train_loss")
    ax.plot(epochs, val_plot, marker="s", linewidth=1.8, markersize=4, label="val_mean")
    ax.plot(epochs, test_plot, marker="^", linewidth=1.8, markersize=4, label="test_mean")
    ax.set_title(f"Loss Curves (sample every {max(1, int(interval))} epochs)")
    ax.set_xlabel("Epoch")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    ax.legend()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)

    if not quiet:
        print(f"curve_file: {curve_path}")
        print(f"output: {output_path}")
        print(f"points: {len(epochs)}")


def _plot_timing_curve(rows: list[dict], curve_path: Path, output_path: Path, interval: int, *, quiet: bool = False) -> None:
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
        if not quiet:
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
    handles, labels = ax_batch.get_legend_handles_labels()
    if handles:
        ax_batch.legend(handles, labels, ncol=4, fontsize=8)

    for key, label in epoch_series:
        values = _to_float_array(rows, key)
        if _has_finite(values):
            ax_epoch.plot(epochs, values, linewidth=1.6, label=label)
    ax_epoch.set_title("Epoch-Level Timing")
    ax_epoch.set_xlabel("Epoch")
    ax_epoch.set_ylabel("seconds")
    ax_epoch.grid(True, alpha=0.25)
    handles, labels = ax_epoch.get_legend_handles_labels()
    if handles:
        ax_epoch.legend(handles, labels, ncol=3, fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    if not quiet:
        print(f"timing_output: {output_path}")


def plot_curve_file(
    curve_path: Path,
    *,
    interval: int,
    output_path: Path | None = None,
    timing_output_path: Path | None = None,
    write_parquet_cache: bool = False,
    quiet: bool = False,
) -> tuple[Path, Path, dict[str, float | int | str]]:
    total_start = time.perf_counter()
    load_start = time.perf_counter()
    loaded_rows = _load_curve(curve_path)
    load_s = time.perf_counter() - load_start
    parquet_cache_path: Path | None = None
    parquet_cache_s = 0.0
    if write_parquet_cache:
        parquet_cache_path, parquet_cache_s = _write_curve_parquet_cache(curve_path, loaded_rows)
    sample_start = time.perf_counter()
    rows = _sample_rows(loaded_rows, interval)
    sample_s = time.perf_counter() - sample_start
    if output_path is None:
        output_path = curve_path.parent / f"epoch_curve_every{max(1, int(interval))}.png"
    if timing_output_path is None:
        timing_output_path = curve_path.parent / f"epoch_timing_every{max(1, int(interval))}.png"
    loss_start = time.perf_counter()
    _plot_loss_curve(rows, curve_path, output_path, interval, quiet=quiet)
    loss_plot_s = time.perf_counter() - loss_start
    timing_start = time.perf_counter()
    _plot_timing_curve(rows, curve_path, timing_output_path, interval, quiet=quiet)
    timing_plot_s = time.perf_counter() - timing_start
    timing = {
        "curve_file": str(curve_path),
        "output": str(output_path),
        "timing_output": str(timing_output_path),
        "interval": int(max(1, int(interval))),
        "loaded_rows": int(len(loaded_rows)),
        "plotted_rows": int(len(rows)),
        "load_s": float(load_s),
        "parquet_cache_s": float(parquet_cache_s),
        "sample_s": float(sample_s),
        "loss_plot_s": float(loss_plot_s),
        "timing_plot_s": float(timing_plot_s),
        "total_s": float(time.perf_counter() - total_start),
    }
    if parquet_cache_path is not None:
        timing["parquet_cache"] = str(parquet_cache_path)
    timing_json = curve_path.parent / f"epoch_curve_plot_timing_every{max(1, int(interval))}.json"
    timing_json.write_text(json.dumps(timing, indent=2, ensure_ascii=False), encoding="utf-8")
    timing["timing_json"] = str(timing_json)
    return output_path, timing_output_path, timing


def main() -> None:
    args = _parse_args()

    if args.skip_plots:
        curve_paths = []
    elif args.curve_file:
        curve_paths = [Path(args.curve_file)]
    else:
        try:
            curve_paths = _find_curve_files(Path(args.artifacts_root))
        except FileNotFoundError:
            if not args.export_report_csvs:
                raise
            curve_paths = []
        if curve_paths:
            print(f"plotting all curve files under {args.artifacts_root}: {len(curve_paths)} found")

    for curve_path in curve_paths:
        if args.output and len(curve_paths) == 1:
            output_path = Path(args.output)
        else:
            output_path = curve_path.parent / f"epoch_curve_every{max(1, int(args.interval))}.png"

        if args.timing_output and len(curve_paths) == 1:
            timing_output_path = Path(args.timing_output)
        else:
            timing_output_path = curve_path.parent / f"epoch_timing_every{max(1, int(args.interval))}.png"
        plot_curve_file(
            curve_path,
            interval=args.interval,
            output_path=output_path,
            timing_output_path=timing_output_path,
            write_parquet_cache=bool(args.write_parquet_cache),
        )

    if args.export_report_csvs:
        export_report_csvs(
            Path(args.artifacts_root),
            force=bool(args.force_report_csvs),
            batch_size=max(1, int(args.report_csv_batch_size)),
        )

    if args.redraw_backtest_plots:
        redraw_backtest_plots(Path(args.artifacts_root))


if __name__ == "__main__":
    main()
