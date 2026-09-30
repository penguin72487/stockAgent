"""Evidence-based missingness, event zeros and causal accounting identities.

NULL is not a download error. State observations can be carried within the
existing release rule; daily flows cannot. An absent event is zero only on a
reconciled market report. This module never changes source files or price data.
"""
from __future__ import annotations

import polars as pl

from stockagent.data.tw_public_cross_source_fill import missing_only
from stockagent.data.tw_public_release_schedule import ReleaseRule, NMI_FIRST_RELEASE, carry_limit
from stockagent.data.tw_security import TW_ETF_SYMBOL_PATTERN

CONTRACT = "tw_feature_semantics_research_v1"
BLOCK_PARTS = (
    "配對交易單一證券成交金額", "逐筆交易單一證券成交金額",
    "配對交易股票組合成交金額", "逐筆交易股票組合成交金額",
)
# Explicit, same-period identities, not fitted correlations or name matching.
FINANCIAL_IDENTITIES = {
    "資產總額": (("負債及股東權益總額", 1.),),
    "負債及股東權益總額": (("資產總額", 1.),),
    "負債總額": (("資產總額", 1.), ("股東權益總額", -1.)),
    "股東權益總額": (("資產總額", 1.), ("負債總額", -1.)),
    "流動資產": (("資產總額", 1.), ("非流動資產", -1.)),
    "非流動資產": (("資產總額", 1.), ("流動資產", -1.)),
    "流動負債": (("負債總額", 1.), ("非流動負債", -1.)),
    "非流動負債": (("負債總額", 1.), ("流動負債", -1.)),
    "營業毛利": (("營業收入淨額", 1.), ("營業成本", -1.)),
    "營業成本": (("營業收入淨額", 1.), ("營業毛利", -1.)),
    "營業利益": (("營業毛利", 1.), ("營業費用", -1.)),
    "營業費用": (("營業毛利", 1.), ("營業利益", -1.)),
}


def unique(table: pl.DataFrame, keys: list[str]):
    if table.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError(f"duplicate economic keys: {keys}")


def reconcile_block_reports(amounts: pl.DataFrame, markets: pl.DataFrame,
                            summaries: pl.DataFrame) -> pl.DataFrame:
    """Reconcile both venues separately, using ALL symbols, not model universe.

    All four summary components must exist. A positive market total with no
    detail, ambiguous/negative amounts or missing market labels cannot certify
    absence. A verified total of zero CAN certify a genuinely empty report.
    """
    keys = ["source_index", "symbol"]
    unique(amounts, keys)
    unique(markets, keys)
    unique(summaries, ["source_index", "market"])
    rows = amounts.join(markets, on=keys, how="left", validate="1:1")
    unknown_dates = rows.filter(~pl.col("market").is_in(["上市", "上櫃"]).fill_null(False))["source_index"]
    grouped = rows.group_by("source_index", "market").agg(
        pl.col("value").sum().alias("detail_amount"), pl.len().alias("detail_rows"),
        (~pl.col("value").is_finite() | (pl.col("value") < 0)).fill_null(True).sum().alias("invalid_rows"))
    result = summaries.join(grouped, on=["source_index", "market"], how="left", validate="1:1")
    result = result.with_columns(pl.col("detail_amount").fill_null(0.),
        pl.col("detail_rows").fill_null(0), pl.col("invalid_rows").fill_null(0),
        pl.when(pl.all_horizontal(pl.col(p).is_finite() & (pl.col(p) >= 0) for p in BLOCK_PARTS))
        .then(pl.sum_horizontal(BLOCK_PARTS)).alias("summary_amount"))
    return result.with_columns((
        pl.col("summary_amount").is_not_null() & (pl.col("invalid_rows") == 0)
        & ~pl.col("source_index").is_in(unknown_dates.implode())
        & ((pl.col("summary_amount") - pl.col("detail_amount")).abs() <= .005)
    ).fill_null(False).alias("absence_zero_verified"))


def event_zero_patch(keys_with_market: pl.DataFrame, reports: pl.DataFrame,
                     events: pl.DataFrame, feature: str) -> pl.DataFrame:
    """Inputs use already next-session dates; no ffill or universe inference."""
    unique(keys_with_market, ["date", "symbol"])
    unique(reports, ["date", "market"])
    unique(events, ["date", "symbol"])
    covered = reports.filter(pl.col("absence_zero_verified")).select("date", "market")
    # A NULL event row is an explicit barrier, not evidence of no event.
    return (keys_with_market.join(covered, on=["date", "market"], how="inner", validate="m:1")
        .join(events.select("date", "symbol"), on=["date", "symbol"], how="anti")
        .select("date", "symbol", pl.lit(0., dtype=pl.Float32).alias(feature)))


def identity_candidate(tables: dict[str, pl.DataFrame], terms) -> pl.DataFrame:
    """All inputs have period, symbol, date, value. Max clock prevents leakage."""
    joined = None
    for i, (name, coefficient) in enumerate(terms):
        t = tables[name].filter(pl.col("value").is_finite())
        unique(t, ["period", "symbol"])
        t = t.select("period", "symbol", pl.col("date").alias(f"d{i}"),
                     (pl.col("value") * coefficient).alias(f"v{i}"))
        joined = t if joined is None else joined.join(t, on=["period", "symbol"], validate="1:1")
    if joined is None:
        raise ValueError("empty identity")
    return joined.select("period", "symbol",
        pl.max_horizontal([f"d{i}" for i in range(len(terms))]).alias("date"),
        pl.sum_horizontal([f"v{i}" for i in range(len(terms))]).alias("value"))


def accepted_identity_fill(primary, candidate, *, min_overlap=100):
    # Keep invalid primary observation keys blocked as well as finite values.
    fill, audit, _ = missing_only(primary.select("period", "symbol", "value"),
        candidate.select("period", "symbol", "value"), min_overlap=min_overlap)
    fill = fill.join(primary.select("period", "symbol"), on=["period", "symbol"], how="anti")
    fill = fill.join(candidate.select("period", "symbol", "date"),
                     on=["period", "symbol"], validate="1:1")
    audit["filled_keys"] = fill.height
    return fill, audit


def collapse_period_observations(table, feature):
    """Late old reports never supersede a newer reporting period."""
    unique(table, ["source_index", "symbol"])
    table = table.sort("symbol", "date", "source_index").with_columns(
        pl.col("source_index").rank("dense").over("symbol").alias("_period"))
    table = table.with_columns(pl.col("_period").cum_max().over("symbol").alias("_latest"))
    return (table.filter(pl.col("_period") == pl.col("_latest"))
        .unique(["date", "symbol"], keep="last", maintain_order=True)
        .select("date", "symbol", feature, "source_index"))


def missingness_reasons(missing: pl.DataFrame, observations: pl.DataFrame,
                       feature: str, rule: ReleaseRule, source_symbols: list[str],
                       dataset: str, report_context: pl.DataFrame | None = None) -> pl.DataFrame:
    """Partition NULL cells into evidence-based causes, not corruption verdicts.

    Source membership/history are diagnostic ONLY: never model inputs or a
    reason to backfill before the first release. A TTL expiry is not fixed by
    unlimited carry; a missing optional account is not implicitly zero.
    """
    if not missing.height:
        return pl.DataFrame(schema={"reason": pl.String, "cells": pl.UInt32})
    by = [] if rule.scope == "market" else ["symbol"]
    right = observations.select("date", *by, feature).rename({"date": "_released", feature: "_value"})
    unique(right, ["_released", *by])
    j = missing.sort("date").join_asof(right.sort("_released"),
        left_on="date", right_on="_released", by=by or None, strategy="backward", check_sortedness=False)
    if report_context is not None:
        # A verified report exists, but this particular optional account was
        # omitted. This does NOT prove zero or a broken download.
        j = j.sort("date").join_asof(report_context.sort("_report_date"),
            left_on="date", right_on="_report_date", by="symbol", strategy="backward",
            tolerance=f"{rule.carry_days}d", check_sortedness=False)
        accounts = observations.filter(pl.col(feature).is_not_null()).select("symbol",
            pl.col("source_index").alias("_report_period")).unique().with_columns(pl.lit(True).alias("_account_reported"))
        j = j.join(accounts, on=["symbol", "_report_period"], how="left", validate="m:1")
    age = (pl.col("date") - pl.col("_released")).dt.total_days()
    reason = pl.when(pl.lit(False)).then(pl.lit("unused"))
    etf = pl.col("symbol").str.contains(TW_ETF_SYMBOL_PATTERN)
    if dataset.startswith(("financial_statement:", "monthly_revenue:")):
        reason = reason.when(etf).then(pl.lit("not_applicable_company_report_to_etf"))
        reason = reason.when(pl.col("symbol").str.starts_with("91")).then(pl.lit("tdr_reporting_clock_not_admitted"))
    if dataset.startswith("tw_etf_nav_daily:"):
        reason = reason.when(~etf).then(pl.lit("not_applicable_etf_measure_to_stock"))
    if dataset.startswith("tw_total_nmi:"):
        reason = reason.when(pl.col("date") <= NMI_FIRST_RELEASE).then(pl.lit("before_first_public_release"))
    first = observations["date"].min()
    reason = reason.when(pl.col("date") < pl.lit(first, dtype=pl.Date)).then(pl.lit("before_first_observation_in_archive"))
    if by:
        reason = reason.when(~pl.col("symbol").is_in(source_symbols)).then(pl.lit("symbol_not_in_source_schema"))
    if report_context is not None:
        reason = reason.when(pl.col("_report_period").is_not_null() & pl.col("_account_reported").is_null()).then(pl.lit("account_not_reported_in_available_company_report"))
    if by:
        observed_symbols = observations.filter(pl.col(feature).is_not_null())["symbol"].unique().to_list()
        reason = reason.when(~pl.col("symbol").is_in(observed_symbols)).then(pl.lit("no_value_for_symbol_in_local_archive"))
    reason = reason.when(pl.col("_released").is_null()).then(pl.lit("no_published_observation_for_symbol_yet"))
    if rule.carry_days:
        reason = reason.when(pl.col("_value").is_null() & (age <= carry_limit(rule))).then(pl.lit("explicit_invalid_observation_barrier"))
        reason = reason.when(age > carry_limit(rule)).then(pl.lit("state_older_than_carry_limit"))
        reason = reason.otherwise(pl.lit("alignment_or_downstream_mask_gap"))
    else:
        reason = reason.when((age == 0) & pl.col("_value").is_null()).then(pl.lit("explicit_invalid_observation_barrier"))
        reason = reason.when(age > 0).then(pl.lit("daily_report_has_no_symbol_value"))
        reason = reason.otherwise(pl.lit("alignment_or_downstream_mask_gap"))
    return j.select(reason.alias("reason")).group_by("reason").agg(pl.len().alias("cells"))
