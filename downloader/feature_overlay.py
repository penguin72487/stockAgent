"""Narrow columnar updates without copying unrelated multi-year feature columns."""
from __future__ import annotations

import polars as pl


def latest_non_null_ms(frame: pl.DataFrame, column: str, fallback_start_ms: int,
                       *, interval_ms: int = 60_000) -> int:
    if column not in frame.columns or "date" not in frame.columns:
        return fallback_start_ms
    # Filter the date Series, not every unrelated feature in the full table.
    latest = frame.select(
        pl.col("date").filter(pl.col(column).is_not_null())
        .cast(pl.String, strict=False).str.to_datetime(strict=False)
        .cast(pl.Int64).floordiv(1_000).max()
    ).item()
    return max(fallback_start_ms, int(latest) - interval_ms) if latest is not None else fallback_start_ms


def feature_coverage(frame: pl.DataFrame, primary_columns: dict[str, str]) -> dict:
    """One narrow aggregate, not ten full-width filtered frame copies."""
    expressions = []
    for family, column in primary_columns.items():
        if column in frame.columns:
            valid = pl.col(column).is_not_null()
            expressions.extend((
                valid.sum().alias(f"{family}:rows"),
                pl.col("date").filter(valid).min().alias(f"{family}:start"),
                pl.col("date").filter(valid).max().alias(f"{family}:end"),
            ))
    values = frame.select(expressions).row(0, named=True) if expressions else {}
    return {family: {"rows": int(values.get(f"{family}:rows") or 0),
                     "start": values.get(f"{family}:start"), "end": values.get(f"{family}:end")}
            for family in primary_columns}


def overlay_feature_frame(base: pl.DataFrame, fresh: pl.DataFrame,
                          columns: tuple[str, ...]) -> pl.DataFrame:
    if fresh.is_empty():
        return base
    selected = [column for column in columns if column in fresh.columns]
    if not selected:
        return base
    renamed = fresh.select(["date", *selected]).rename(
        {column: f"__fresh_{column}" for column in selected}
    )
    # A nonunique source expands rows in the original left join. Preserve that
    # oracle for malformed/unusual input rather than dropping observations or
    # assigning a length-changing result back into the original frame.
    narrow = (fresh["date"].n_unique() == fresh.height
              and not set(renamed.columns[1:]).intersection(base.columns))
    left = base.select(["date", *[name for name in selected if name in base.columns]]) if narrow else base
    joined = left.join(renamed, on="date", how="left", maintain_order="left")
    expressions = []
    for name in selected:
        incoming = pl.col(f"__fresh_{name}")
        expressions.append((pl.coalesce([incoming, pl.col(name)])
                            if name in base.columns else incoming).alias(name))
    if narrow:
        # with_columns replaces only the updated Series. Untouched Arrow
        # buffers, raw values, units and missingness stay shared with base.
        updates = joined.select(expressions)
        return base.with_columns(updates.get_columns())
    return joined.with_columns(expressions).drop([f"__fresh_{name}" for name in selected])
