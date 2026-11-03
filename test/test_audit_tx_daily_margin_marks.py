from datetime import date

import polars as pl
import pytest

from scripts.audit_tx_daily_margin_marks import audit


def data():
    dates = [date(2011, 3, 15), date(2011, 3, 16)]
    reference = pl.DataFrame({"date": dates, "contract": ["201103"] * 2,
                              "observed_front_close": [8305., 8306.],
                              "cash_settlement_event": [False, True],
                              "final_settlement_price": [8305., 8305.]})
    daily = pl.DataFrame({"date": dates, "contract": ["201103"] * 2, "product": ["TX"] * 2,
                          "accounting_only_date": [False] * 2,
                          "official_settlement": ["8304", "8305"], "settlement": [8304., 8305.],
                          "daily_mark": [8304., 8305.], "observed_close": [8305., 8306.],
                          "official_row_observed": [True] * 2, "source_row_observed": [True] * 2,
                          "cash_settlement": [False, True], "final_settlement_price": [8305., 8305.]})
    return reference, daily


def test_separates_nonexpiry_margin_marks_from_final_settlement():
    reference, daily = data()
    result = audit(reference, daily)
    assert result["candidate_market_sessions"] == result["valid_own_contract_marks"] == 2
    assert result["valid_nonexpiry_official_daily_settlements"] == 1
    assert result["valid_expiry_final_settlements"] == 1
    assert result["nonexpiry_observed_close_differs_from_official_settlement"] == 1
    assert result["nonexpiry_max_absolute_points_difference"] == 1
    assert not result["one_contract_benchmark_admitted"]


def test_missing_or_duplicated_marks_are_not_silently_counted_complete():
    reference, daily = data()
    result = audit(reference, daily.head(1))
    assert result["invalid_or_missing_own_contract_marks"] == 1
    with pytest.raises(ValueError, match="ambiguous"):
        audit(reference, pl.concat([daily, daily.head(1)]))
