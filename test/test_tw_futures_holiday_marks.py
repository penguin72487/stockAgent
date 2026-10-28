from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_holiday_marks import (
    HOLIDAY_NIGHT_RESEARCH_ABI,
    HolidayNightMarkSource,
    _digest,
    apply_holiday_night_marks,
)


def _source():
    rows = pl.DataFrame([{
        "date": date(2024, 2, 6), "product": "UDF", "contract": "202403",
        "physical_instance": "UDF:202403@generation=0", "previous_mark": 38660.,
        "night_close": 38456., "tick_timestamp": "2024-02-06 04:59:42",
        "attributed_trading_date": date(2024, 2, 15),
        "holiday_notice_sha256": "a" * 64, "category_notice_sha256": "b" * 64,
    }])
    return HolidayNightMarkSource(rows, "c" * 64, "d" * 64, _digest(rows, "d" * 64))


def _physical():
    return pl.DataFrame([
        {"date": date(2024, 2, 5), "product": "UDF", "contract": "202403",
         "physical_instance": "UDF:202403@generation=0", "valuation_price": 38660.,
         "accounting_only_date": False, "cash_settlement": False, "volume": 7},
        {"date": date(2024, 2, 6), "product": "UDF", "contract": "202403",
         "physical_instance": "UDF:202403@generation=0", "valuation_price": 38660.,
         "accounting_only_date": True, "cash_settlement": False, "volume": None},
        {"date": date(2024, 2, 6), "product": "TGF", "contract": "202408",
         "physical_instance": "TGF:202408@generation=0", "valuation_price": 7903.,
         "accounting_only_date": True, "cash_settlement": False, "volume": None},
    ])


def test_only_the_bound_account_mark_changes_and_missing_source_stays_unchanged():
    source, original = _source(), _physical()
    result = apply_holiday_night_marks(original, source).sort("date", "product")
    assert result["valuation_price"].to_list() == [38660., 7903., 38456.]
    assert result["volume"].to_list() == [7, None, None]
    assert result["holiday_valuation_contract"].to_list() == [None, None, HOLIDAY_NIGHT_RESEARCH_ABI]
    assert result["holiday_valuation_source_fingerprint"].to_list() == [None, None, source.fingerprint]
    assert original["valuation_price"].to_list() == [38660., 38660., 7903.]


@pytest.mark.parametrize("change", ["new_generation", "changed_parent", "cash_settlement", "decision_day"])
def test_rejects_wrong_generation_or_parent_and_cannot_replace_trades_or_cash_settlement(change):
    physical = _physical()
    selected = pl.col("date") == date(2024, 2, 6)
    selected &= pl.col("product") == "UDF"
    if change == "new_generation":
        physical = physical.with_columns(pl.when(selected).then(pl.lit("UDF:202403@generation=1"))
                                         .otherwise(pl.col("physical_instance")).alias("physical_instance"))
    elif change == "changed_parent":
        physical = physical.with_columns(pl.when(selected).then(1.).otherwise(pl.col("valuation_price"))
                                         .alias("valuation_price"))
    else:
        column = "cash_settlement" if change == "cash_settlement" else "accounting_only_date"
        physical = physical.with_columns(pl.when(selected).then(pl.lit(change == "cash_settlement"))
                                         .otherwise(pl.col(column)).alias(column))
    with pytest.raises(ValueError):
        apply_holiday_night_marks(physical, _source())


def test_source_identity_detects_in_place_mutation():
    source = _source()
    source.rows.replace_column(source.rows.get_column_index("night_close"), pl.Series("night_close", [1.]))
    with pytest.raises(ValueError, match="source rows changed"):
        apply_holiday_night_marks(_physical(), source)
