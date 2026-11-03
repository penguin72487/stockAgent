"""Recover a *candidate* prior-session feature source, never an account row.

The accounting release may omit a provably empty inventory prefix.  Omission
of that account row does not imply the preceding market observation is absent.
This module only identifies exact prior-date, same-physical-contract observations
for later source review; it does not admit them to training or change positions.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS

PRIOR_CONTEXT_CANDIDATE_CONTRACT = "tw_futures_prior_market_context_candidate_v1"


def recover_prior_market_context_candidates(
    market_dates: np.ndarray, release: pl.DataFrame, original: pl.DataFrame,
) -> pl.DataFrame:
    """Find omitted prior market rows with an independently observed settlement.

    There is no filter on today's executable flag, volume, price or outcome.
    The release's retrospective membership and product listing clocks still
    require separate admission before these candidates may enter a model.
    """
    dates = np.asarray(market_dates, dtype="datetime64[D]")
    if dates.ndim != 1 or dates.size < 2 or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("market dates must be finite, unique and strictly increasing")
    keys = ["date", "symbol"]
    common = {"date", "symbol", "physical_contract", "accounting_only_date"}
    required_release = common | {"same_contract_as_previous_session"}
    required_original = common | {
        "official_row_observed", "valuation_research_applied", "official_settlement",
        "settlement", "underlying_symbol", *FUTURES_MODEL_FEATURE_COLUMNS,
    }
    if not required_release <= set(release.columns) or not required_original <= set(original.columns):
        raise ValueError("prior context source schema is incomplete")
    for name, frame in (("release", release), ("original", original)):
        if (frame.select(keys).is_duplicated().any()
                or frame.select(pl.any_horizontal(pl.col(c).is_null() for c in common).any()).item()
                or frame.schema["accounting_only_date"] != pl.Boolean):
            raise ValueError(f"{name} has ambiguous date/slot identities or account-day markers")
    for frame, field in ((release, "same_contract_as_previous_session"),
                         (original, "official_row_observed"),
                         (original, "valuation_research_applied")):
        if frame.schema[field] != pl.Boolean or frame[field].null_count():
            raise ValueError(f"prior context requires an explicit Boolean {field}")

    market_calendar = pl.DataFrame({"decision_date": dates[1:], "context_date": dates[:-1]}).with_columns(
        pl.all().cast(pl.Date)
    )
    release_market = release.filter(~pl.col("accounting_only_date"))
    current = (release_market.select(pl.col("date").alias("decision_date"), "symbol",
                                     "physical_contract", "same_contract_as_previous_session")
               .join(market_calendar, on="decision_date", how="inner", validate="m:1")
               .filter(pl.col("same_contract_as_previous_session")))
    prior_release = release_market.select(pl.col("date").alias("context_date"), "symbol",
                                          pl.lit(True).alias("_already_in_release"))
    missing = (current.join(prior_release, on=["context_date", "symbol"], how="left", validate="m:1")
               .filter(pl.col("_already_in_release").is_null()).drop("_already_in_release"))
    prior = (original.filter(~pl.col("accounting_only_date"))
             .select(pl.col("date").alias("context_date"), "symbol", "physical_contract",
                     "underlying_symbol", "official_row_observed", "valuation_research_applied",
                     "official_settlement", "settlement", *FUTURES_MODEL_FEATURE_COLUMNS))
    matched = missing.join(prior, on=["context_date", "symbol", "physical_contract"],
                           how="inner", validate="m:1")
    # This conservative candidate screen avoids publishing research-generated
    # settlement values as official marks. Nulls stay null in model features.
    matched = matched.with_columns(
        pl.col("official_settlement").cast(pl.String).str.replace_all(",", "")
        .cast(pl.Float64, strict=False).alias("_official_settlement_numeric")
    ).filter(
        pl.col("official_row_observed") & ~pl.col("valuation_research_applied")
        & pl.col("_official_settlement_numeric").is_finite() & (pl.col("_official_settlement_numeric") > 0)
        & pl.col("settlement").is_finite() & (pl.col("settlement") == pl.col("_official_settlement_numeric"))
    )
    return matched.select(
        "decision_date", "context_date", "symbol", "physical_contract", "underlying_symbol",
        "official_settlement", "settlement", *FUTURES_MODEL_FEATURE_COLUMNS,
    ).sort("decision_date", "symbol")
