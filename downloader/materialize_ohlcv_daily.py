from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import threading
import sys

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import PersistentProgress, atomic_write_text  # noqa: E402
from artifact_io import atomic_write_parquet  # noqa: E402
from ohlcv_hot_tail import logical_mtime_ns, read_logical_parquet  # noqa: E402


@dataclass(slots=True)
class MaterializeResult:
    symbol: str
    status: str
    rows: int
    partial_days: int
    output_path: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize deterministic UTC daily OHLCV from canonical one-minute "
            "parquet without issuing another provider request."
        )
    )
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument(
        "--workers", type=int, default=max(1, min(16, os.cpu_count() or 1))
    )
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(path, frame, compression="snappy", write_statistics=True)


def _daily_frame(path: Path, *, start_day: date | None = None) -> pl.DataFrame:
    required = {"date", "open", "max", "min", "close", "Trading_Volume"}
    schema = set(pq.read_schema(path).names)
    missing = required - schema
    if missing:
        raise ValueError(f"missing canonical one-minute columns: {sorted(missing)}")
    selected = [
        name
        for name in (
            "date",
            "open",
            "max",
            "min",
            "close",
            "adjclose",
            "Trading_Volume",
        )
        if name in schema
    ]
    filters = None
    if start_day is not None:
        date_type = pq.read_schema(path).field("date").type
        if pa.types.is_string(date_type) or pa.types.is_large_string(date_type):
            filter_value: object = start_day.isoformat()
        elif pa.types.is_date(date_type):
            filter_value = start_day
        elif pa.types.is_timestamp(date_type):
            filter_value = datetime.combine(start_day, time.min)
            if date_type.tz:
                filter_value = filter_value.replace(tzinfo=timezone.utc)
        else:
            filter_value = start_day.isoformat()
        filters = [("date", ">=", filter_value)]
    frame = read_logical_parquet(
        path,
        columns=selected,
        filters=filters,
    )
    if frame.is_empty():
        return pl.DataFrame()
    timestamp = (
        pl.col("date").str.to_datetime(strict=False, time_zone="UTC")
        if frame.schema["date"] == pl.String
        else pl.col("date").cast(pl.Datetime("us", "UTC"), strict=False)
    )
    normalized = (
        frame.with_columns(timestamp.alias("__ts"))
        .drop_nulls(["__ts", "open", "max", "min", "close"])
        .sort("__ts")
        .with_columns(pl.col("__ts").dt.date().alias("__day"))
    )
    if normalized.is_empty():
        return pl.DataFrame()
    if normalized.select(pl.col("__ts").is_duplicated().any()).item():
        raise ValueError(
            "duplicate one-minute timestamps would double-count daily volume"
        )
    off_grid = normalized.select(
        (
            (pl.col("__ts").dt.second() != 0) | (pl.col("__ts").dt.microsecond() != 0)
        ).any()
    ).item()
    if off_grid:
        raise ValueError("timestamps are not aligned to the canonical one-minute grid")
    # A daily bar is a completed UTC calendar day.  The current day's partial
    # minute stream remains solely in the 1m dataset until the next UTC day.
    normalized = normalized.filter(
        pl.col("__day") < pl.lit(datetime.now(timezone.utc).date())
    )
    if normalized.is_empty():
        return pl.DataFrame()
    return (
        normalized.group_by("__day", maintain_order=True)
        .agg(
            pl.col("open").first().cast(pl.Float64).alias("open"),
            pl.col("max").max().cast(pl.Float64).alias("max"),
            pl.col("min").min().cast(pl.Float64).alias("min"),
            pl.col("close").last().cast(pl.Float64).alias("close"),
            pl.col("close").last().cast(pl.Float64).alias("adjclose"),
            pl.col("Trading_Volume").sum().cast(pl.Float64).alias("Trading_Volume"),
            pl.len().cast(pl.Int32).alias("source_minute_rows"),
            pl.col("__ts").first().alias("first_minute_utc"),
            pl.col("__ts").last().alias("last_minute_utc"),
        )
        .with_columns(
            pl.col("__day").cast(pl.String).alias("date"),
            pl.lit(1440, dtype=pl.Int32).alias("expected_minute_rows"),
            (pl.col("source_minute_rows") / 1440.0).alias("minute_coverage_ratio"),
            (pl.col("source_minute_rows") == 1440).alias("minute_grid_complete"),
        )
        .select(
            "date",
            "open",
            "max",
            "min",
            "close",
            "adjclose",
            "Trading_Volume",
            "source_minute_rows",
            "expected_minute_rows",
            "minute_coverage_ratio",
            "minute_grid_complete",
            "first_minute_utc",
            "last_minute_utc",
        )
        .sort("date")
    )


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    paths = sorted(input_dir.glob("*_features.parquet"))
    output_dir.mkdir(parents=True, exist_ok=True)
    progress = PersistentProgress(
        output_dir / "progress.json",
        label=f"{args.provider} 1 分鐘轉日線",
        total=len(paths),
        unit="symbol",
        basis="completed local one-minute parquet files divided by full elapsed time",
        started_at=started,
    )
    lock = threading.Lock()
    results: list[MaterializeResult] = []

    def work(source: Path) -> MaterializeResult:
        target = output_dir / source.name
        if (
            not args.refresh
            and target.is_file()
            and target.stat().st_mtime_ns >= logical_mtime_ns(source)
            and "minute_grid_complete" in pq.read_schema(target).names
        ):
            rows = int(pq.ParquetFile(target, memory_map=True).metadata.num_rows)
            partial_days = int(
                pl.scan_parquet(target)
                .select((~pl.col("minute_grid_complete")).sum())
                .collect()
                .item()
            )
            return MaterializeResult(
                source.stem,
                "skipped_up_to_date",
                rows,
                partial_days,
                str(target),
            )
        existing: pl.DataFrame | None = None
        recompute_start: date | None = None
        if target.is_file() and "minute_grid_complete" in pq.read_schema(target).names:
            existing = pl.read_parquet(target)
            if existing.height and "date" in existing.columns:
                last_day = date.fromisoformat(str(existing["date"].max())[:10])
                recompute_start = last_day - timedelta(days=1)
        frame = _daily_frame(source, start_day=recompute_start)
        if frame.is_empty():
            if existing is not None:
                partial_days = int(
                    existing.select((~pl.col("minute_grid_complete")).sum()).item()
                )
                return MaterializeResult(
                    source.stem,
                    "skipped_no_completed_day",
                    existing.height,
                    partial_days,
                    str(target),
                )
            return MaterializeResult(source.stem, "skipped_no_data", 0, 0, None)
        if existing is not None:
            replace_from = str(frame["date"].min())
            frame = pl.concat(
                [existing.filter(pl.col("date") < replace_from), frame],
                how="diagonal_relaxed",
            ).sort("date")
            if frame.equals(existing):
                partial_days = int(
                    existing.select((~pl.col("minute_grid_complete")).sum()).item()
                )
                return MaterializeResult(
                    source.stem,
                    "skipped_daily_unchanged",
                    existing.height,
                    partial_days,
                    str(target),
                )
        _write_parquet_atomic(frame, target)
        partial_days = int(frame.select((~pl.col("minute_grid_complete")).sum()).item())
        return MaterializeResult(
            source.stem,
            "updated",
            frame.height,
            partial_days,
            str(target),
        )

    def record(result: MaterializeResult) -> None:
        with lock:
            results.append(result)
        progress.update("daily_materialize", result.status)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(work, path): path for path in paths}
        for future in as_completed(futures):
            source = futures[future]
            try:
                record(future.result())
            except Exception as exc:
                record(
                    MaterializeResult(
                        source.stem,
                        "failed",
                        0,
                        0,
                        None,
                        f"{type(exc).__name__}: {exc}",
                    )
                )

    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    ended = datetime.now(timezone.utc)
    payload = {
        "schema_version": 1,
        "provider": args.provider,
        "source_granularity": "1m",
        "output_granularity": "daily",
        "timestamp_contract": "UTC calendar day aggregated from completed one-minute bars",
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "symbol_count": len(paths),
        "row_count": sum(result.rows for result in results),
        "partial_day_count": sum(result.partial_days for result in results),
        "status_counts": dict(sorted(status_counts.items())),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
    }
    atomic_write_text(
        output_dir / "download_summary.json",
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )
    report = pl.DataFrame(
        [asdict(result) for result in results], infer_schema_length=None
    )
    atomic_write_text(
        output_dir / "download_report.csv",
        report.sort(["status", "symbol"]).write_csv() if report.height else "",
    )
    failed = any(result.status == "failed" for result in results)
    progress.finish(failed=failed)
    if failed:
        raise RuntimeError("daily materialization incomplete; see download_report.csv")


if __name__ == "__main__":
    main()
