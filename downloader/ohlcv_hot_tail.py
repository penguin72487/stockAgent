from __future__ import annotations

"""Logical Parquet reads for an immutable OHLCV base plus a small hot tail.

Parquet files are immutable.  Rewriting a multi-year, one-minute file to append
one closed candle makes incremental work proportional to all historical rows.
The collectors therefore keep the historical file unchanged during tail-only
refreshes and publish recent rows below ``_hot_tail``.  Readers use this module
to expose the two physical files as one timestamp-deduplicated logical table.
Periodic non-tail reconciliation compacts the tail back into the base.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


HOT_TAIL_DIRNAME = "_hot_tail"


def canonical_date_frame(frame: pl.DataFrame) -> pl.DataFrame | None:
    """Reuse already-normalized writer dates, or decline to the caller's rules.

    Validate every value, not just the first/last date or a sortedness marker.
    The exact ASCII grammar excludes leap seconds and variable-width fields
    whose parse/format round trip can change text. Explicit parsing still
    rejects impossible calendar dates. Other dtypes, nulls, timezones and
    subsecond values retain the collector's established normalization path.
    Strict ascending order is checked from the values, not metadata. Duplicate
    or unordered dates retain the original sort's row-priority semantics.
    This neither deduplicates rows nor changes any value column.
    """
    if frame.is_empty() or frame.schema.get("date") != pl.String:
        return None
    dates = frame.get_column("date")
    if dates.null_count() or not bool(dates.str.contains(
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-5][0-9]:[0-5][0-9]$"
    ).all()):
        return None
    parsed = dates.str.to_datetime(
        format="%Y-%m-%d %H:%M:%S", time_unit="us", strict=False, exact=True,
    )
    if parsed.null_count():
        return None
    if len(dates) > 1 and not bool((dates.slice(1) > dates.slice(0, len(dates) - 1)).all()):
        return None
    return frame


def tail_newer_summary(
    frame: pl.DataFrame,
    *,
    base_latest_ms: int,
    normalize: Callable[[pl.DataFrame], pl.DataFrame],
) -> tuple[int, datetime | None]:
    """Count/min actual tail dates without a canonical string round trip.

    Only non-null, exact second-resolution writer strings take the native
    path. Everything else retains the caller's complete legacy normalization
    (including its invalid-date handling and exceptions). This computes the
    same overlap facts, not a footer-only or inferred-contiguity certificate.
    No input frame, source file, timestamp or provider policy is modified.
    """
    dates = None
    if not frame.is_empty() and frame.schema.get("date") == pl.String:
        values = frame.get_column("date")
        if values.null_count() == 0 and bool((values.str.len_bytes() == 19).all()):
            try:
                parsed = values.str.to_datetime(
                    format="%Y-%m-%d %H:%M:%S", time_unit="us",
                    strict=False, exact=True,
                )
            except pl.exceptions.PolarsError:
                parsed = None
            if parsed is not None and parsed.null_count() == 0:
                dates = parsed
    if dates is None:
        # Keep the same post-normalization parse as all three CEX callers.
        dates = normalize(frame).select(
            pl.col("date").str.to_datetime(strict=False).alias("__ts")
        ).get_column("__ts")
    cutoff = datetime.fromtimestamp(base_latest_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
    newer = dates.filter(dates > cutoff)
    return len(newer), newer.min() if len(newer) else None


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


def _native_tail_schema_supported(schema: pa.Schema) -> bool:
    """Keep Arrow's established decoding for nested, dictionary/extension types."""
    return all(
        pa.types.is_integer(field.type)
        or pa.types.is_float32(field.type)
        or pa.types.is_float64(field.type)
        or pa.types.is_boolean(field.type)
        or pa.types.is_string(field.type)
        or pa.types.is_large_string(field.type)
        or pa.types.is_timestamp(field.type)
        or pa.types.is_date32(field.type)
        or pa.types.is_date64(field.type)
        or pa.types.is_null(field.type)
        for field in schema
    )


def _read_tail_row_groups(
    path: Path, rows: int, *, columns: Sequence[str] | None = None,
):
    before = path.stat()
    parquet = pq.ParquetFile(path, memory_map=True)
    metadata = parquet.metadata
    if metadata is None or int(metadata.num_row_groups) <= 0:
        return pq.read_table(path, columns=columns, memory_map=True)

    wanted = max(1, int(rows))
    row_groups: list[int] = []
    row_count = 0
    for group_idx in range(int(metadata.num_row_groups) - 1, -1, -1):
        row_groups.append(group_idx)
        row_count += int(metadata.row_group(group_idx).num_rows)
        if row_count >= wanted:
            break
    # Decode only requested rows/pages instead of a potentially million-row
    # final group. Small reads retain Arrow to avoid another engine's startup.
    schema = parquet.schema_arrow
    selected_schema = (schema if columns is None else
                       pa.schema([schema.field(name) for name in columns]))
    if row_count > max(8192, wanted) and _native_tail_schema_supported(selected_schema):
        query = pl.scan_parquet(path, glob=False)
        if columns is not None:
            query = query.select(list(columns))
        frame = query.tail(wanted).collect()
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
        ):
            raise OSError(f"Parquet generation changed during bounded read: {path}")
        expected_schema = pl.from_arrow(pa.Table.from_batches([], schema=selected_schema)).schema
        if frame.schema != expected_schema:
            # An unfamiliar scalar mapping is not permission to change dtype.
            table = parquet.read_row_groups(sorted(row_groups), columns=columns)
        else:
            return frame.to_arrow()
    else:
        table = parquet.read_row_groups(sorted(row_groups), columns=columns)
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
        table = _read_tail_row_groups(path, base_tail_rows, columns=selected)
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

    from stockagent.data.partitioned_parquet import MANIFEST_SUFFIX, read_table, resolve_source
    resolved = resolve_source(base_path)
    if resolved.name.endswith(MANIFEST_SUFFIX):
        logical_base = resolved.with_name(resolved.name.removesuffix(MANIFEST_SUFFIX))
        if hot_tail_path(logical_base).exists():
            raise ValueError("partitioned BASE cannot also carry an OHLCV hot tail")
        return pl.from_arrow(read_table(resolved, columns=columns, filters=filters, tail_rows=tail_rows))
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
