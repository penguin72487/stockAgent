"""Audit period omissions without confusing a sparse panel with missing data.

Finite values bracketing a NULL are a recheck hint, not proof that an optional
account was mandatory. Exact alternate observations remain a separate proven
missing-only repair. No interpolation, unlimited carry, backdating or zeros.
"""
from __future__ import annotations

from datetime import date
import re

import polars as pl

CONTRACT = "tw_expected_native_period_observation_audit_v1"
GAP_SCHEMA = {"period": pl.String, "symbol": pl.String, "source_index": pl.String}


def fiscal_period(index: str, kind: str) -> tuple[str, int, date]:
    if kind == "quarter":
        found = re.fullmatch(r"(\d{4})-?Q([1-4])", index)
        if not found:
            raise ValueError("invalid quarterly source axis")
        y, q = map(int, found.groups())
    elif kind == "quarter_date":
        day = date.fromisoformat(index[:10])
        if day.month not in {3, 5, 8, 11}:
            raise ValueError("unknown revised financial reporting axis")
        y, q = day.year - (day.month == 3), {3: 4, 5: 1, 8: 2, 11: 3}[day.month]
    elif kind == "revenue":
        # Supplied dates already belong to the release month, not subject month.
        day = date.fromisoformat(index[:10])
        y, m = (day.year - 1, 12) if day.month == 1 else (day.year, day.month - 1)
        return f"{y:04d}-{m:02d}", y * 12 + m - 1, day
    else:
        raise ValueError("this grid is only for financial quarters and revenue months")
    # Research clock only, matching the canonical general deadline envelope.
    deadline = {1: date(y, 5, 15), 2: date(y, 8, 14), 3: date(y, 11, 14), 4: date(y + 1, 3, 31)}[q]
    return f"{y}Q{q}", y * 4 + q - 1, deadline


def interior_period_gaps(wide: pl.DataFrame, kind: str, *, start: date, as_of: date):
    """All exact interior period slots, never pre-first/post-last expectations.

    Return full keys plus observations for independent source comparison. This
    includes globally absent periods but does not assert issuer applicability.
    Future release rows cannot bracket an artificial 'missing' prior release.
    """
    if "source_index" not in wide.columns:
        raise ValueError("missing source axis")
    indexes = wide["source_index"].to_list()
    if len(indexes) != len(set(indexes)) or None in indexes:
        raise ValueError("duplicate or NULL source axis")
    mapped = []
    for index in indexes:
        period, ordinal, deadline = fiscal_period(str(index), kind)
        if deadline <= as_of and int(period[:4]) >= start.year:
            mapped.append({"source_index": index, "period": period, "ordinal": ordinal})
    if not mapped:
        return pl.DataFrame(schema=GAP_SCHEMA), pl.DataFrame(schema={"period": pl.String, "symbol": pl.String, "value": pl.Float64})
    axis = pl.DataFrame(mapped)
    if axis["period"].n_unique() != axis.height:
        raise ValueError("ambiguous multiple source rows for one subject period")
    columns = [n for n in wide.columns if n != "source_index"]
    long = (wide.join(axis, on="source_index", how="inner", validate="1:1")
            .unpivot(index=["source_index", "period", "ordinal"], on=columns,
                     variable_name="symbol", value_name="value")
            .with_columns(pl.col("value").cast(pl.Float64)))
    finite = long.filter(pl.col("value").is_finite())
    bounds = finite.group_by("symbol").agg(pl.col("ordinal").min().alias("first"), pl.col("ordinal").max().alias("last"))
    grid = bounds.select("symbol", pl.int_ranges(pl.col("first") + 1, pl.col("last")).alias("ordinal")).explode("ordinal").drop_nulls()
    missing = grid.join(finite.select("symbol", "ordinal"), on=["symbol", "ordinal"], how="anti")
    if kind == "revenue":
        subject = pl.concat_str((pl.col("ordinal") // 12).cast(pl.String), pl.lit("-"),
                               (pl.col("ordinal") % 12 + 1).cast(pl.String).str.pad_start(2, "0"))
    else:
        subject = pl.concat_str((pl.col("ordinal") // 4).cast(pl.String), pl.lit("Q"),
                               (pl.col("ordinal") % 4 + 1).cast(pl.String))
    gaps = missing.with_columns(subject.alias("period")).join(axis.select("ordinal", "source_index"), on="ordinal", how="left").select("period", "symbol", "source_index")
    return gaps.sort("symbol", "period"), long.select("period", "symbol", "value")


def annotate_gap_candidates(gaps: pl.DataFrame, alternate: pl.DataFrame | None, *, accepted: bool):
    keys = ["period", "symbol"]
    if alternate is not None and accepted:
        proven = alternate.filter(pl.col("value").is_finite()).select(keys).unique().with_columns(pl.lit(True).alias("_found"))
        result = gaps.join(proven, on=keys, how="left", validate="1:1")
        return result.with_columns(pl.when(pl.col("_found").fill_null(False))
            .then(pl.lit("already_resolved_by_verified_source_union"))
            .otherwise(pl.lit("interior_period_omission_requires_provider_recheck"))
            .alias("state")).drop("_found")
    return gaps.with_columns(pl.lit("interior_period_omission_requires_provider_recheck").alias("state"))


def mandatory_trading_price_gaps(frame: pl.LazyFrame, *, start: date, as_of: date) -> pl.DataFrame:
    """Read each stock file once; volume-zero/no-session is not a required fill."""
    fields = ['open', 'max', 'min', 'close']
    bad = lambda name: (~pl.col(name).is_finite() | (pl.col(name) <= 0)).fill_null(True)
    traded = frame.filter(pl.col('date').is_between(start, as_of) & (pl.col('Trading_Volume') > 0))
    broken = traded.filter(pl.any_horizontal(*(bad(name) for name in fields)))
    return (broken.select('date', 'symbol', *fields).unpivot(index=['date', 'symbol'], on=fields,
        variable_name='feature', value_name='value').filter(bad('value')).select(
        pl.col('date').cast(pl.String).alias('period'), 'symbol', 'feature',
        pl.lit('verified_trading_without_required_positive_price').alias('state'))
        .collect(engine='streaming'))
