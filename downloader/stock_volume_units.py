"""Vectorized stock-volume inference shared by ingestion and minute research.

Only a unique, whole-share quantity supported by independently supplied trade
amount and the bar's price range is usable. Candidate ordering is not evidence.
"""

from __future__ import annotations

from pathlib import Path
from collections.abc import Sequence

import polars as pl


STOCK_MINUTE_RAW_COLUMNS = (
    "ts", "Open", "High", "Low", "Close", "Volume", "Amount",
    "date", "symbol", "market", "contract_unit",
)
STOCK_MINUTE_READER_CONTRACT = "required_raw_schema_grouped_nullable_metadata_v1"


def scan_stock_minute_sources(paths: Sequence[str | Path]) -> pl.LazyFrame:
    """Scan receipt-selected raw chunks while retaining optional source fields.

    A missing provider value cannot become an inserted NULL. Only additional
    metadata may be absent in older chunks. Equal schemas share a bulk scan,
    avoiding thousands of separate lazy scans; incompatible types fail closed.
    Source files remain unchanged and volume is interpreted by the caller's
    existing amount/OHLC contract.
    """
    if not paths:
        raise ValueError("stock minute source scan requires selected chunks")
    groups: dict[tuple, list[str]] = {}
    union: dict[str, pl.DataType] = {}
    for raw_path in paths:
        path = str(raw_path)
        schema = pl.read_parquet_schema(path)
        missing = set(STOCK_MINUTE_RAW_COLUMNS) - set(schema)
        if missing:
            raise ValueError(f"minute source lacks required raw fields {sorted(missing)}: {path}")
        for name, dtype in schema.items():
            previous = union.get(name)
            if (name == "ts" and isinstance(dtype, pl.Datetime)
                    and isinstance(previous, pl.Datetime) and previous.time_zone == dtype.time_zone):
                union[name] = pl.Datetime("ns", time_zone=dtype.time_zone)
                continue
            if previous is not None and previous != dtype and previous != pl.Null and dtype != pl.Null:
                raise pl.exceptions.SchemaError(
                    f"minute source field {name} has incompatible types {previous} / {dtype}: {path}"
                )
            if previous is None or previous == pl.Null:
                union[name] = dtype
        groups.setdefault(tuple(schema.items()), []).append(path)
    columns = [*STOCK_MINUTE_RAW_COLUMNS, *sorted(set(union)-set(STOCK_MINUTE_RAW_COLUMNS))]
    frames = []
    for items, selected in groups.items():
        schema = dict(items)
        frame = pl.scan_parquet(selected, schema=schema)
        additions = [pl.lit(None, dtype=union[name]).alias(name) for name in columns if name not in schema]
        safe_casts = [pl.col(name).cast(union[name]) for name in schema
                      if schema[name] != union[name] and (
                          schema[name] == pl.Null or name == "ts" and isinstance(schema[name], pl.Datetime))]
        frames.append(frame.with_columns(*additions, *safe_casts).select(columns))
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="vertical")


def stock_volume_multiplier_expr(*, tolerance: float) -> pl.Expr:
    if not 0 <= tolerance < 1:
        raise ValueError("volume notional tolerance must be in [0, 1)")
    positive = (
        (pl.col("Volume") > 0) & pl.col("Volume").is_finite()
        & (pl.col("Amount") > 0) & pl.col("Amount").is_finite()
        & (pl.col("Low") > 0) & pl.col("Low").is_finite()
        & (pl.col("High") >= pl.col("Low")) & pl.col("High").is_finite()
    )
    candidates = []
    for value in (pl.col("contract_unit"), pl.lit(1000.), pl.lit(100.), pl.lit(10.), pl.lit(1.)):
        shares = pl.col("Volume") * value
        valid = (
            positive & value.is_finite() & (value > 0)
            & shares.is_finite() & (shares > 0)
            & (pl.col("Amount") >= shares * pl.col("Low") * (1 - tolerance))
            & (pl.col("Amount") <= shares * pl.col("High") * (1 + tolerance))
        )
        candidates.append(pl.when(valid).then(value.cast(pl.Float64)).otherwise(None))
    # Equal min/max means exactly one distinct candidate. Horizontal numeric
    # reductions avoid constructing and deduplicating a five-item list for
    # every row of a multi-hundred-million-row minute dataset.
    smallest = pl.min_horizontal(candidates)
    largest = pl.max_horizontal(candidates)
    resolved_shares = pl.col("Volume") * smallest
    return (
        pl.when((pl.col("Volume") == 0) & (pl.col("Amount") == 0))
        .then(pl.lit(1.))
        .when(smallest.is_not_null() & (smallest == largest)
              & ((resolved_shares - resolved_shares.round(0)).abs() <= 1e-6))
        .then(smallest)
        .otherwise(None).cast(pl.Float64)
    )


def with_stock_share_volume(frame, *, tolerance: float):
    """Recompute canonical columns from raw evidence, including legacy frames."""

    return frame.with_columns(
        stock_volume_multiplier_expr(tolerance=tolerance).alias("source_volume_multiplier")
    ).with_columns(
        (pl.col("Volume") * pl.col("source_volume_multiplier")).round(0).alias("volume_shares"),
        pl.when((pl.col("Volume") == 0) & (pl.col("Amount") == 0))
        .then(pl.lit("zero_trade"))
        .when(pl.col("source_volume_multiplier").is_not_null())
        .then(pl.lit("amount_ohlc"))
        .otherwise(pl.lit("unresolved")).alias("volume_unit_proof"),
    )
