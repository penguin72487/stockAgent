from pathlib import Path

import numpy as np
import polars as pl

from scripts.audit_tw_price_precision import (
    _aggregate_audit_status, _counts, _record_grid_result, audit_file,
)
from stockagent.data.tw_exchange_price_classification import (
    classify_tw_exchange_security,
)
from downloader.download_tw_public_data import (
    _malformed_twse_ohlcv_source_dates,
    _write_parquet_merged,
)


def test_audit_keeps_invalid_dates_separate_from_off_grid_quotes(tmp_path: Path):
    path = tmp_path / "data_tw_public/twse_daily_ohlcv.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": ["2026-07-06", "2026-07-06", "2014-12-;1"],
        "證券代號": ["2330", "0050", "8070"],
        "開盤價": ["1000", "50.05", "70.00"],
        "最高價": ["1000", "50.05", "70.00"],
        "最低價": ["1000", "50.05", "70.00"],
        "收盤價": ["1001", "50.05", "70.00"],
        "最後揭示買價": ["1000", "50.05", "70.00"],
        "最後揭示賣價": ["1005", "50.05", "70.10"],
        "均價": ["1000.123456", "50.05123456", "70.01234567"],
    }).write_parquet(path)
    result = audit_file("twse_daily_ohlcv", tmp_path, batch_size=2)
    assert result["status"] == "source_value_problem"
    assert result["invalid_date_rows"] == 1
    assert result["fields"]["收盤價"]["off_grid"] == 1
    assert result["off_grid_values"] == 1
    assert "均價" not in result["fields"]


def test_audit_uses_stock_futures_effective_date_and_excludes_settlement(tmp_path: Path):
    path = tmp_path / "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": ["2026-07-03", "2026-07-06", "2026-07-06"],
        "physical_contract": ["CDF:202607"] * 3,
        "asset_class": ["stock_future"] * 3,
        "source_row_observed": [True, True, False],
        "open": [1001., 1001., 1002.],
        "high": [1005., 1001., 1002.],
        "low": [1000., 1001., 1002.],
        "close": [1005., 1001., 1002.],
        "last_bid": [1000., 1001., 1002.],
        "last_ask": [1005., 1001., 1002.],
        "settlement": [1001.25, 1001.25, 1001.25],
    }).with_columns(pl.col("date").str.strptime(pl.Date)).write_parquet(path)
    result = audit_file("taifex_stock_futures_daily", tmp_path, batch_size=2)
    assert result["eligible_rows"] == 2
    assert result["fields"]["open"]["off_grid"] == 1
    assert result["fields"]["close"]["off_grid"] == 0
    assert "settlement" not in result["fields"]


def test_audit_labels_foreign_currency_etf_without_changing_its_tick(tmp_path: Path):
    path = tmp_path / "data_tw_public/twse_daily_ohlcv.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": ["2026-07-06", "2026-07-06"],
        "證券代號": ["00625K", "00632R"],
        **{field: ["50.05", "50.05"] for field in
           ("開盤價", "最高價", "最低價", "收盤價", "最後揭示買價", "最後揭示賣價")},
    }).write_parquet(path)
    result = audit_file("twse_daily_ohlcv", tmp_path)
    assert result["status"] == "quote_grid_valid"
    assert result["currency_classes"] == {"foreign_currency_etf": 1, "twd": 1}


def test_official_refetch_retires_only_matching_malformed_date_copy(tmp_path: Path):
    path = tmp_path / "twse_daily_ohlcv.parquet"
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20141231&type=ALLBUT0999&response=json"
    old = pl.DataFrame({"date": ["2014-12-;1", "2014-12-;1"],
                        "證券代號": ["8070", "9999"], "收盤價": ["70.00", "1.00"],
                        "_url": [url, url]})
    old.write_parquet(path)
    assert {str(x) for x in _malformed_twse_ohlcv_source_dates(path)} == {"2014-12-31"}
    fresh = pl.DataFrame({"date": ["2014-12-31"], "證券代號": ["8070"],
                          "收盤價": ["70.00"], "_url": [url]})
    _write_parquet_merged(path, fresh, refresh=False)
    result = pl.read_parquet(path)
    assert result.filter(pl.col("證券代號") == "8070")["date"].to_list() == ["2014-12-31"]
    assert result.filter(pl.col("證券代號") == "9999")["date"].to_list() == ["2014-12-;1"]


def test_audit_classifier_covers_non_training_exchange_products_without_reclassifying_universe():
    assert classify_tw_exchange_security("twse", "2887Z1") == "stock"
    assert classify_tw_exchange_security("twse", "910322") == "stock"
    assert classify_tw_exchange_security("twse", "23051") == "convertible_bond"
    assert classify_tw_exchange_security("twse", "01004T") == "reit"
    assert classify_tw_exchange_security("twse", "020034") == "etn"
    assert classify_tw_exchange_security("tpex", "70001") == "warrant"


def test_quote_audit_rejects_infinity_without_treating_missing_values_as_prices():
    counts = _counts(("Close",))
    values = np.array([50.0, np.inf, -np.inf, np.nan])
    _record_grid_result(
        counts, "Close", values, np.array([True, False, False, False]),
        dates=np.full(4, np.datetime64("2026-09-18")),
        symbols=np.full(4, "2330"), kinds=np.full(4, "stock"),
    )
    assert counts["Close"]["observed"] == 1
    assert counts["Close"]["off_grid"] == 2


def test_historical_rule_uncertainty_is_distinct_from_bad_source_values():
    assert _aggregate_audit_status([
        {"status": "quote_grid_valid"}, {"status": "historical_rule_unverified"}
    ]) == "historical_rule_unverified"
    assert _aggregate_audit_status([
        {"status": "historical_rule_unverified"}, {"status": "source_value_problem"}
    ]) == "source_value_problem"
