import csv
from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_daily_universe_readback import audit, sha256


def sources(tmp_path):
    daily = tmp_path / "daily.parquet"
    pl.DataFrame({"date": [date(2010, 1, 1), date(2011, 1, 3), date(2011, 1, 4), date(2011, 1, 4)],
                  "product": ["OLD", "A", "A", "B"],
                  "asset_class": ["stock_future"] * 4,
                  "executable": [True, True, False, False]}).write_parquet(daily)
    inventory = tmp_path / "inventory.csv"
    fields = ["product", "product_name", "asset_class", "settlement_currency", "source_start", "source_end",
              "raw_positive_volume_rows", "source_status"]
    with inventory.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for code in ("A", "B", "OLD", "MISSING"):
            writer.writerow(dict(product=code, product_name=code, asset_class="stock_future",
                                 settlement_currency="TWD", source_start="2010-01-01",
                                 source_end="2011-01-04", raw_positive_volume_rows="0", source_status="unknown"))
    return daily, inventory


def test_daily_universe_separates_horizon_and_execution(tmp_path):
    daily, inventory = sources(tmp_path)
    result = audit(daily, inventory, date(2011, 1, 3), date(2011, 1, 4), sha256(daily))
    assert (result["inventory_products"], result["daily_products_with_rows"],
            result["daily_products_with_executable_rows"]) == (4, 2, 1)
    assert [row["product"] for row in result["inventory_products_absent_from_daily"]] == ["MISSING", "OLD"]
    assert [row["product"] for row in result["products_without_daily_executable_rows"]] == ["B"]
    assert result["by_asset_class"]["stock_future"]["daily_executable_rows"] == 1


def test_daily_universe_rejects_stale_hash_and_unknown_flag(tmp_path):
    daily, inventory = sources(tmp_path)
    with pytest.raises(ValueError, match="expected SHA-256"):
        audit(daily, inventory, date(2011, 1, 3), date(2011, 1, 4), "wrong")
    pl.read_parquet(daily).with_columns(
        pl.when(pl.col("product") == "B").then(None).otherwise(pl.col("executable")).alias("executable")
    ).write_parquet(daily)
    with pytest.raises(ValueError, match="ambiguous"):
        audit(daily, inventory, date(2011, 1, 3), date(2011, 1, 4), sha256(daily))
