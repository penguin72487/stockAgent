from datetime import date

import numpy as np
import polars as pl
import pytest

from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.data.tw_futures_prior_context import recover_prior_market_context_candidates


def _source_row(day, slot, physical, *, observed=True, research=False, settlement=100.0):
    return dict(date=day, symbol=slot, physical_contract=physical, accounting_only_date=False,
                official_row_observed=observed, valuation_research_applied=research,
                official_settlement=settlement, settlement=settlement, underlying_symbol="2330",
                **{name: 1.0 for name in FUTURES_MODEL_FEATURE_COLUMNS})


def _release_row(day, slot, physical, *, same=True, executable=False):
    return dict(date=day, symbol=slot, physical_contract=physical, accounting_only_date=False,
                same_contract_as_previous_session=same, executable=executable)


def test_restores_prior_feature_candidate_without_using_today_execution():
    dates = np.array(["2020-01-02", "2020-01-03"], dtype="datetime64[D]")
    prior, today = date(2020, 1, 2), date(2020, 1, 3)
    source = pl.DataFrame([_source_row(prior, "s1", "p1"), _source_row(prior, "s2", "p2")])
    release = pl.DataFrame([_release_row(today, "s1", "p1", executable=False),
                            _release_row(today, "s2", "p2", executable=True)])
    result = recover_prior_market_context_candidates(dates, release, source)
    assert result["symbol"].to_list() == ["s1", "s2"]
    assert result["context_date"].to_list() == [prior, prior]
    assert "executable" not in result.columns
    changed = release.with_columns((~pl.col("executable")).alias("executable"))
    assert recover_prior_market_context_candidates(dates, changed, source).equals(result)


def test_rejects_wrong_generation_research_and_nonofficial_values():
    dates = np.array(["2020-01-02", "2020-01-03"], dtype="datetime64[D]")
    prior, today = date(2020, 1, 2), date(2020, 1, 3)
    source = pl.DataFrame([_source_row(prior, "a", "old"), _source_row(prior, "b", "b", research=True),
                           _source_row(prior, "c", "c", observed=False), _source_row(prior, "d", "d")])
    source = source.with_columns(pl.when(pl.col("symbol") == "d").then(99.0)
                                 .otherwise(pl.col("settlement")).alias("settlement"))
    release = pl.DataFrame([_release_row(today, "a", "new"), _release_row(today, "b", "b"),
                            _release_row(today, "c", "c"), _release_row(today, "d", "d")])
    assert recover_prior_market_context_candidates(dates, release, source).is_empty()


def test_existing_prior_slot_is_not_overwritten_and_duplicate_identity_fails():
    dates = np.array(["2020-01-02", "2020-01-03"], dtype="datetime64[D]")
    prior, today = date(2020, 1, 2), date(2020, 1, 3)
    source = pl.DataFrame([_source_row(prior, "s1", "p1")])
    release = pl.DataFrame([_release_row(prior, "s1", "p1"), _release_row(today, "s1", "p1")])
    assert recover_prior_market_context_candidates(dates, release, source).is_empty()
    with pytest.raises(ValueError, match="ambiguous"):
        recover_prior_market_context_candidates(dates, pl.concat([release, release.head(1)]), source)
    with pytest.raises(ValueError, match="strictly increasing"):
        recover_prior_market_context_candidates(dates[::-1], release, source)
