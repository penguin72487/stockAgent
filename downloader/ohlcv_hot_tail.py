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
