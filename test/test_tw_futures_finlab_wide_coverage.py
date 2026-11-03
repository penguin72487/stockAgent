import polars as pl
import pytest

from scripts.audit_tw_futures_finlab_wide_coverage import compare_wide


def source_tables():
    days = ["2011-01-03 00:00:00", "2026-10-29 00:00:00"]
    close = pl.DataFrame({"source_index": days, "TX一般": [100.0, 105.0],
                          "A一般": [10.0, 11.0], "PK1一般": [None, 295.0]})
    volume = pl.DataFrame({"source_index": days, "TX一般": [10.0, 20.0],
                           "A一般": [0.0, None], "PK1一般": [None, 45.0]})
    month = pl.DataFrame({"source_index": days, "TX一般": ["201101", "202611"],
                          "A一般": ["201101", "202611"], "PK1一般": [None, "202611"]})
    inventory = pl.DataFrame({"product": ["TX", "A", "MISSING"]})
    official = {"market_day": "2026-10-29", "products": [
        {"product": "TX", "settlement_currency": "TWD", "positive_volume_contracts": 1, "months": ["202611"]},
        {"product": "PK1", "settlement_currency": "TWD", "positive_volume_contracts": 1, "months": ["202611"]},
        {"product": "A", "settlement_currency": "TWD", "positive_volume_contracts": 1, "months": ["202611"]},
    ]}
    return close, volume, month, inventory, official


def test_positive_candidate_requires_same_day_price_volume_and_month():
    result = compare_wide(*source_tables(), first="2011-01-01", last="2026-10-29")
    assert result["historical_inventory_products"] == 3
    assert result["historical_products_with_any_positive_daily_candidate"] == 1
    assert result["historical_products_without_positive_daily_candidate"] == ["A", "MISSING"]
    assert result["official_twd_products_with_positive_volume"] == 3
    assert result["official_positive_with_finlab_positive_candidate_and_reported_month"] == 2
    assert result["official_positive_without_that_finlab_candidate"] == ["A"]
    pk = next(row for row in result["products"] if row["product"] == "PK1")
    assert not pk["historical_inventory"] and pk["latest_month_in_official_snapshot"]


def test_month_must_be_the_six_digit_reported_physical_month():
    close, volume, month, inventory, official = source_tables()
    month = month.with_columns(pl.lit("202612").alias("PK1一般"))
    result = compare_wide(close, volume, month, inventory, official, first="2011-01-01", last="2026-10-29")
    assert result["official_positive_without_that_finlab_candidate"] == ["A", "PK1"]


def test_duplicate_day_and_missing_latest_day_are_rejected():
    close, volume, month, inventory, official = source_tables()
    with pytest.raises(ValueError, match="ambiguous market-day"):
        compare_wide(pl.concat([close, close.tail(1)]), volume, month, inventory, official,
                     first="2011-01-01", last="2026-10-29")
    with pytest.raises(ValueError, match="lack the requested common"):
        compare_wide(close, volume.head(1), month, inventory, official,
                     first="2011-01-01", last="2026-10-29")
