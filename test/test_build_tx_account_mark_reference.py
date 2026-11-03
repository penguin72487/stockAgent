from datetime import date

import polars as pl
import pytest

from scripts.build_tx_account_mark_reference import build


def data():
    dates = [date(2011, 3, 15), date(2011, 3, 16)]
    reference = pl.DataFrame({"date": dates, "contract": ["201103"] * 2,
                              "observed_front_close": [8305., 8306.],
                              "cash_settlement_event": [False, True],
                              "final_settlement_price": [8305., 8305.]})
    daily = pl.DataFrame({"date": dates, "contract": ["201103"] * 2, "product": ["TX"] * 2,
                          "accounting_only_date": [False] * 2, "official_settlement": ["8304", ""],
                          "settlement": [8304., 8305.], "daily_mark": [8304., 8305.],
                          "observed_close": [8305., 8306.], "official_row_observed": [True] * 2,
                          "source_row_observed": [True] * 2, "cash_settlement": [False, True],
                          "final_settlement_price": [8305., 8305.]})
    return reference, daily


def test_nonexpiry_uses_daily_settlement_expiry_uses_final_fixing():
    reference, daily = data()
    reference = reference.with_columns(pl.lit(False).alias("front_month_roll"))
    result = build(reference, daily)
    assert result["account_settlement_mark"].to_list() == [8304., 8305.]
    assert result["observed_front_close"].to_list() == [8305., 8306.]


def test_account_mark_reference_cannot_synthesize_missing_day():
    reference, daily = data()
    reference = reference.with_columns(pl.lit(False).alias("front_month_roll"))
    with pytest.raises(ValueError, match="every candidate day"):
        build(reference, daily.head(1))
