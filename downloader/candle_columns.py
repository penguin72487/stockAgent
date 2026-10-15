"""Columnar decoding of documented numeric exchange candle arrays.

Providers keep their own units, confirmation, malformed-row and duplicate
policies. A noncanonical payload returns None so the original scalar parser can
preserve Python conversion/error semantics; it never triggers a new HTTP call.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl


def numeric_candle_page(
    rows: Sequence[Sequence[Any]],
    schema: dict[str, pl.DataType],
    *,
    timestamp: str,
    skip_short_rows: bool,
) -> pl.DataFrame | None:
    """Decode numbers once and render the UTC date column in native code.

    Scope the fast timestamp path to 1970--2099. Other dates/formats are not
    dropped or guessed; the provider's existing scalar parser remains the
    reference for those uncommon cases and for malformed source values.
    """
    width = len(schema)
    if not rows:
        return None  # preserve each provider's exact empty schema
    if not skip_short_rows and any(len(row) < width for row in rows):
        return None
    payload = [row[:width] for row in rows if len(row) >= width]
    if not payload:
        return None
    try:
        # Polars' row constructor can truncate a decimal *string* into Int64;
        # Python int("1.5") rejects it. Validate the original token first.
        integers = [name for name, dtype in schema.items() if dtype == pl.Int64]
        input_schema = {name: pl.String if name in integers else dtype
                        for name, dtype in schema.items()}
        frame = pl.DataFrame(payload, schema=input_schema, orient="row", strict=True)
        if any(frame.null_count().row(0)):
            return None
        if integers and frame.select(pl.any_horizontal([
            ~pl.col(name).str.contains(r"^\s*[+-]?[0-9]+\s*$") for name in integers
        ]).any()).item():
            return None
        frame = frame.with_columns(pl.col(integers).cast(pl.Int64, strict=True))
        times = frame[timestamp]
        if times.min() < 0 or times.max() >= 4_102_444_800_000:
            return None
        return frame.with_columns(
            pl.from_epoch(pl.col(timestamp), time_unit="ms")
            .dt.strftime("%Y-%m-%d %H:%M:%S").alias("date")
        )
    except (pl.exceptions.PolarsError, TypeError, ValueError, OverflowError):
        return None
