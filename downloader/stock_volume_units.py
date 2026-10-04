"""Vectorized stock-volume inference shared by ingestion and minute research.

Only a unique, whole-share quantity supported by independently supplied trade
amount and the bar's price range is usable. Candidate ordering is not evidence.
"""

from __future__ import annotations

import polars as pl


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
