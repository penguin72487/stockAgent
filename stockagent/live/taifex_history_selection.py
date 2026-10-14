"""Exact display-curve extrema, without per-row broadcast window arrays.

The grouped selector is an offline experiment, not wired to a live producer.
It keeps the original timestamp ties, null/legacy rows, duplicate rows and
source order; it changes no financial expression, return or source observation.
"""
from __future__ import annotations

import polars as pl

def legacy_rows() -> pl.Expr:
    return pl.col("valuation_available").is_null() | (
        pl.col("valuation_carried_forward").fill_null(False)
        & pl.col("valuation_age_seconds").is_null()
    )


def select_grouped_rows(selected: pl.LazyFrame) -> pl.LazyFrame:
    """Reduce four narrow columns to keys, then filter the actual source rows.

    Each bucket selects the earliest timestamp attaining its min/max return.
    Each strategy additionally selects its first/last valid timestamp. The
    Native struct membership cannot fan out original duplicates or reorder
    the source. Bucket keys and global strategy boundaries stay distinct;
    null equality retains the original window oracle's null strategy group.
    Null timestamps never match the original equality predicates.
    """
    timestamp = pl.col("decision_ts_ns")
    value = pl.col("__selection_return")
    buckets = selected.select(
        "strategy_id", "__bucket", "decision_ts_ns", "__selection_return"
    ).group_by("strategy_id", "__bucket").agg(
        timestamp.filter(value == value.min()).min().alias("minimum_ts"),
        timestamp.filter(value == value.max()).min().alias("maximum_ts"),
        timestamp.filter(value.is_not_null()).min().alias("first_ts"),
        timestamp.filter(value.is_not_null()).max().alias("last_ts"),
    ).collect(engine="streaming")
    boundaries = buckets.group_by("strategy_id").agg(
        pl.col("first_ts").min(), pl.col("last_ts").max()
    )
    bucket_keys = pl.concat([
        buckets.select("strategy_id", "__bucket", pl.col(name).alias("decision_ts_ns"))
        for name in ("minimum_ts", "maximum_ts")
    ]).drop_nulls("decision_ts_ns").unique()
    boundary_keys = pl.concat([
        boundaries.select("strategy_id", pl.col(name).alias("decision_ts_ns"))
        for name in ("first_ts", "last_ts")
    ]).drop_nulls("decision_ts_ns").unique()
    # A left join broadcasts original 30-column rows before the keep filter;
    # a small literal key set instead lets the source filter push down. Its
    # native hash membership preserves source order and every matching row.
    bucket_columns = ("strategy_id", "__bucket", "decision_ts_ns")
    boundary_columns = ("strategy_id", "decision_ts_ns")
    buckets_set = bucket_keys.select(pl.struct(bucket_columns)).to_series().implode()
    boundaries_set = boundary_keys.select(pl.struct(boundary_columns)).to_series().implode()
    return selected.filter(
        legacy_rows()
        | pl.struct(bucket_columns).is_in(buckets_set, nulls_equal=True)
        | pl.struct(boundary_columns).is_in(boundaries_set, nulls_equal=True)
    ).drop("__selection_return", "__bucket")


def select_window_rows(selected: pl.LazyFrame) -> pl.LazyFrame:
    """Original canonical selector, retained as the fallback and equality oracle."""
    group_columns = ["strategy_id", "__bucket"]
    selected = selected.with_columns(
        pl.col("__selection_return").min().over(group_columns).alias("__minimum_return"),
        pl.col("__selection_return").max().over(group_columns).alias("__maximum_return"),
    ).with_columns(
        pl.when(pl.col("__selection_return") == pl.col("__minimum_return"))
        .then(pl.col("decision_ts_ns")).otherwise(None)
        .min().over(group_columns).alias("__minimum_ts"),
        pl.when(pl.col("__selection_return") == pl.col("__maximum_return"))
        .then(pl.col("decision_ts_ns")).otherwise(None)
        .min().over(group_columns).alias("__maximum_ts"),
    ).with_columns(
        pl.when(pl.col("__selection_return").is_not_null())
        .then(pl.col("decision_ts_ns")).otherwise(None)
        .min().over("strategy_id").alias("__first_strategy_ts"),
        pl.when(pl.col("__selection_return").is_not_null())
        .then(pl.col("decision_ts_ns")).otherwise(None)
        .max().over("strategy_id").alias("__last_strategy_ts"),
    )
    return selected.filter(
        legacy_rows()
        | (pl.col("decision_ts_ns") == pl.col("__minimum_ts"))
        | (pl.col("decision_ts_ns") == pl.col("__maximum_ts"))
        | (pl.col("decision_ts_ns") == pl.col("__first_strategy_ts"))
        | (pl.col("decision_ts_ns") == pl.col("__last_strategy_ts"))
    ).drop(
        "__selection_return", "__bucket", "__minimum_return", "__maximum_return",
        "__minimum_ts", "__maximum_ts", "__first_strategy_ts", "__last_strategy_ts",
    )
