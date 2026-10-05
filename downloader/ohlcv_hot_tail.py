from __future__ import annotations

"""Logical Parquet reads for an immutable OHLCV base plus a small hot tail.

Parquet files are immutable.  Rewriting a multi-year, one-minute file to append
one closed candle makes incremental work proportional to all historical rows.
The collectors therefore keep the historical file unchanged during tail-only
refreshes and publish recent rows below ``_hot_tail``.  Readers use this module
to expose the two physical files as one timestamp-deduplicated logical table.
Periodic non-tail reconciliation compacts the tail back into the base.
"""

from pathlib import Path
from typing import Iterable, Sequence

import polars as pl
import pyarrow.parquet as pq


HOT_TAIL_DIRNAME = "_hot_tail"


def hot_tail_path(base_path: Path) -> Path:
    return base_path.parent / HOT_TAIL_DIRNAME / base_path.name


def logical_parts(base_path: Path) -> tuple[Path, ...]:
    tail_path = hot_tail_path(base_path)
    return tuple(path for path in (base_path, tail_path) if path.is_file())


def logical_mtime_ns(base_path: Path) -> int:
    parts = logical_parts(base_path)
    return max((path.stat().st_mtime_ns for path in parts), default=0)


def remove_hot_tail(base_path: Path) -> None:
    tail_path = hot_tail_path(base_path)
    tail_path.unlink(missing_ok=True)
    try:
        tail_path.parent.rmdir()
    except OSError:
        pass


def _read_tail_row_groups(path: Path, rows: int):
    parquet = pq.ParquetFile(path, memory_map=True)
    metadata = parquet.metadata
    if metadata is None or int(metadata.num_row_groups) <= 0:
        return pq.read_table(path, memory_map=True)

    wanted = max(1, int(rows))
    row_groups: list[int] = []
    row_count = 0
    for group_idx in range(int(metadata.num_row_groups) - 1, -1, -1):
        row_groups.append(group_idx)
        row_count += int(metadata.row_group(group_idx).num_rows)
        if row_count >= wanted:
            break
    table = parquet.read_row_groups(sorted(row_groups))
    if int(table.num_rows) <= wanted:
        return table
    return table.slice(int(table.num_rows) - wanted, wanted)


def _read_part(
    path: Path,
    *,
    columns: Sequence[str] | None,
    filters: Iterable[tuple[str, str, object]] | None,
    base_tail_rows: int | None,
) -> pl.DataFrame:
    schema_names = set(pq.read_schema(path).names)
    selected = None
    if columns is not None:
        selected = [name for name in columns if name in schema_names]
        if not selected:
            return pl.DataFrame({name: [] for name in columns})

    if base_tail_rows is not None:
        table = _read_tail_row_groups(path, base_tail_rows)
        if selected is not None:
            table = table.select(selected)
    else:
        table = pq.read_table(
            path,
            columns=selected,
            filters=list(filters) if filters is not None else None,
            memory_map=True,
        )
    frame = pl.from_arrow(table)
    if columns is not None:
        missing = [name for name in columns if name not in frame.columns]
        if missing:
            frame = frame.with_columns(
                [pl.lit(None).alias(name) for name in missing]
            )
        frame = frame.select(list(columns))
    return frame


def _timestamp_expr(frame: pl.DataFrame) -> pl.Expr:
    dtype = frame.schema.get("date")
    if dtype == pl.String:
        return pl.col("date").str.to_datetime(strict=False, time_zone="UTC")
    return pl.col("date").cast(pl.Datetime("us", "UTC"), strict=False)


def read_logical_parquet(
    base_path: Path,
    *,
    columns: Sequence[str] | None = None,
    filters: Iterable[tuple[str, str, object]] | None = None,
    tail_rows: int | None = None,
) -> pl.DataFrame:
    """Read base and hot-tail Parquet as one last-non-null-wins table.

    ``tail_rows`` bounds the base read to its final row groups and then bounds
    the merged result.  It is intended for live-panel reads; full research
    reads leave it unset.
    """

    parts = logical_parts(base_path)
    if not parts:
        raise FileNotFoundError(base_path)

    if len(parts) == 1:
        frame = _read_part(
            parts[0],
            columns=columns,
            filters=filters,
            base_tail_rows=tail_rows,
        )
        if tail_rows is not None and frame.height > int(tail_rows):
            frame = frame.tail(int(tail_rows))
        return frame

    frames: list[pl.DataFrame] = []
    for priority, path in enumerate(parts):
        frame = _read_part(
            path,
            columns=columns,
            filters=filters,
            base_tail_rows=(tail_rows if path == base_path else None),
        )
        if frame.is_empty():
            continue
        frames.append(frame.with_columns(pl.lit(priority).alias("__part_priority")))
    if not frames:
        return pl.DataFrame({name: [] for name in (columns or ())})

    combined = pl.concat(frames, how="diagonal_relaxed")
    if "date" not in combined.columns:
        return combined.drop("__part_priority")
    combined = combined.with_columns(_timestamp_expr(combined).alias("__merge_ts"))
    if combined.select(pl.col("__merge_ts").is_null().any()).item():
        raise ValueError(f"logical parquet contains invalid timestamps: {base_path}")

    value_columns = [
        name
        for name in combined.columns
        if name not in {"date", "__merge_ts", "__part_priority"}
    ]
    merged = (
        combined.sort(["__merge_ts", "__part_priority"])
        .group_by("__merge_ts", maintain_order=True)
        .agg(
            pl.col("date").last().alias("date"),
            *[
                pl.col(name).drop_nulls().last().alias(name)
                for name in value_columns
            ],
        )
        .sort("__merge_ts")
        .drop("__merge_ts")
    )
    if columns is not None:
        merged = merged.select(list(columns))
    if tail_rows is not None and merged.height > int(tail_rows):
        merged = merged.tail(int(tail_rows))
    return merged

def _validated_candle_timestamps(
    frame: pl.DataFrame, *, earliest_ms: int | None, latest_ms: int | None,
    interval_ms: int,
) -> pl.Series | None:
    """Verify actual grid-aligned dates against the planning bounds."""
    if interval_ms <= 0 or frame.is_empty() or "date" not in frame.columns:
        return None
    try:
        values = frame.get_column("date")
        dates = (
            values.str.to_datetime(strict=False, time_zone="UTC", time_unit="ns")
            if values.dtype == pl.String
            else values.cast(pl.Datetime("ns", "UTC"), strict=False)
        )
    except (pl.exceptions.PolarsError, TypeError, ValueError):
        return None
    if dates.null_count() or len(dates) != frame.height:
        return None
    timestamps = dates.dt.epoch("ns").sort()
    interval_ns = interval_ms * 1_000_000
    if (
        timestamps[0] // 1_000_000 != earliest_ms
        or timestamps[-1] // 1_000_000 != latest_ms
        or not bool((timestamps % interval_ns == 0).all())
    ):
        return None
    return timestamps


def has_contiguous_timestamps(
    frame: pl.DataFrame, *, earliest_ms: int | None, latest_ms: int | None,
    interval_ms: int,
) -> bool:
    """Retain the strict contiguous-middle contract used by Bybit."""
    timestamps = _validated_candle_timestamps(
        frame, earliest_ms=earliest_ms, latest_ms=latest_ms, interval_ms=interval_ms,
    )
    return timestamps is not None and bool(
        (timestamps.diff().drop_nulls() == interval_ms * 1_000_000).all()
    )


def plan_candle_reconcile_windows(
    frame: pl.DataFrame, *, earliest_ms: int | None, latest_ms: int | None,
    start_ms: int, end_ms: int, interval_ms: int,
) -> list[tuple[int, int]] | None:
    """Inclusive head, internal-gap and revision-tail windows; None means rebuild.

    An absent minute is a request, never an invented observation. Adjacent valid
    rows bound each internal request so existing values and provider revisions
    can be merged using the caller's canonical non-null precedence.
    """
    timestamps = _validated_candle_timestamps(
        frame, earliest_ms=earliest_ms, latest_ms=latest_ms, interval_ms=interval_ms,
    )
    if timestamps is None:
        return None
    lower = ((start_ms + interval_ms - 1) // interval_ms) * interval_ms
    upper = (end_ms // interval_ms) * interval_ms
    if lower > upper:
        return []
    deltas = timestamps.diff().drop_nulls()
    if not bool((deltas > 0).all()):
        return None
    first = int(timestamps[0] // 1_000_000)
    last = int(timestamps[-1] // 1_000_000)
    windows: list[tuple[int, int]] = []
    if lower < first:
        windows.append((lower, min(first, upper)))
    if len(timestamps) > 1:
        gap_mask = deltas > interval_ms * 1_000_000
        preceding = timestamps.head(-1).filter(gap_mask).to_list()
        following = timestamps.slice(1).filter(gap_mask).to_list()
        for left, right in zip(preceding, following, strict=True):
            lo = max(lower, left // 1_000_000)
            hi = min(upper, right // 1_000_000)
            if lo <= hi:
                windows.append((lo, hi))
    # Valid dates do not imply valid prices. Re-fetch only the exact offending
    # observations; signed financial features outside OHLCV are not examined.
    if all(name in frame.columns for name in ("open", "max", "min", "close", "Trading_Volume")):
        bad_dates = frame.filter(invalid_candle_values()).select(
            _timestamp_expr(frame).dt.epoch("ms").alias("ms"))
        windows.extend((value, value) for value in bad_dates["ms"].to_list()
                       if value is not None and lower <= value <= upper)
    tail_start = max(lower, last - interval_ms)
    if tail_start <= upper:
        windows.append((tail_start, upper))
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(windows):
        if merged and lo <= merged[-1][1] + interval_ms:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    return merged


def invalid_candle_values() -> pl.Expr:
    """Exchange OHLCV only: finite positive prices, coherent bounds/quantity.

    This must not be applied to financial statements, interest rates, spreads,
    or normal broker no-trade placeholder rows.
    """
    prices = ("open", "max", "min", "close")
    invalid = pl.any_horizontal(*(
        (~pl.col(name).cast(pl.Float64, strict=False).is_finite()
         | (pl.col(name).cast(pl.Float64, strict=False) <= 0)).fill_null(True)
        for name in prices))
    invalid |= (pl.col("max") < pl.max_horizontal("open", "min", "close")) | (
        pl.col("min") > pl.min_horizontal("open", "max", "close"))
    # All canonical exchange candle writers supply Trading_Volume.
    quantity = pl.col("Trading_Volume").cast(pl.Float64, strict=False)
    return invalid | (~quantity.is_finite() | (quantity < 0)).fill_null(True)
