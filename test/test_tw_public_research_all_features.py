from __future__ import annotations

from datetime import date
from fnmatch import fnmatchcase
import math
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from stockagent.data.tw_public_research_all_features import (
    DERIVED_SOURCES,
    _year_predicate,
    build_all_research_features,
)
from stockagent.config import load_config


@pytest.mark.parametrize("dtype", [pl.Date, pl.Datetime("us"), pl.Datetime("us", "Asia/Taipei")])
@pytest.mark.parametrize("year", [2013, 2014, 2016, None])
def test_range_predicate_keeps_exact_year_and_null_calendar_semantics(dtype, year):
    frame = pl.DataFrame({"date": [date(2013, 12, 31), date(2014, 1, 1),
                                   date(2014, 12, 31), date(2015, 1, 1),
                                   date(2016, 2, 29), None]}).with_columns(pl.col("date").cast(dtype))
    original = pl.col("date").is_null() if year is None else pl.col("date").dt.year() == year
    assert_frame_equal(frame.filter(_year_predicate(year, dtype)), frame.filter(original), check_exact=True)


def _source_tables(tmp_path):
    raw_names = sorted(set(DERIVED_SOURCES.values()))
    dates = [date(2014, 1, 2), date(2014, 1, 3), date(2014, 1, 6)]
    rows = []
    for index, day in enumerate(dates):
        row = {"date": day, "symbol": "__MARKET__"}
        row.update({name: 100.0 + index for name in raw_names})
        row["twpub_cbc_overnight_pct_raw"] = 0.00386 + index * 0.00001
        row["twpub_usdtwd_raw"] = 30.0 + index
        row["twpub_mof_trade_balance_raw"] = -1_000_000.0 if index == 0 else 2_000_000.0
        row["twpub_pe_raw"] = 4.0
        rows.append(row)
    rows.append({"date": dates[0], "symbol": "2330", **dict.fromkeys(raw_names), "twpub_pe_raw": None})
    wide = tmp_path / "wide.parquet"
    pl.DataFrame(rows).write_parquet(wide)
    official = tmp_path / "official.parquet"
    pl.DataFrame({
        "date": [dates[0], dates[0]],
        "symbol": ["__MARKET__", "2330"],
        "twpub_official_trades_log": [1.0, 2.0],
        "twpub_cbc_overnight_rate": [None, None],
        "twpub_pe_raw": [4.2, 5.0],
    }).write_parquet(official)
    return wide, official


def test_all_features_reconstructs_macro_history_and_keeps_sparse_cells(tmp_path):
    wide, official = _source_tables(tmp_path)
    output = tmp_path / "all.parquet"
    receipt = build_all_research_features(wide_path=wide, official_path=official, output_path=output)
    assert receipt["rows"] == 4
    assert len(receipt["reconstructed_columns"]) == 15
    frame = pl.read_parquet(output).sort("date", "symbol")
    market = frame.filter(pl.col("symbol") == "__MARKET__").sort("date")
    first = market.row(0, named=True)
    second = market.row(1, named=True)
    assert first["twpub_cbc_overnight_rate"] == pytest.approx(0.00386)
    assert second["twpub_cbc_overnight_rate_chg"] == pytest.approx(0.00001)
    assert first["twpub_usdtwd_logret_1d"] is None
    assert second["twpub_usdtwd_logret_1d"] == pytest.approx(math.log(31 / 30))
    assert first["twpub_mof_trade_balance_asinh"] == pytest.approx(math.asinh(-1))
    assert first["twpub_mof_tax_total_log"] == pytest.approx(math.log1p(100))
    assert first["twpub_official_trades_log"] == 1.0
    assert first["twpub_pe_raw"] == 4.0
    stock = frame.filter(pl.col("symbol") == "2330").row(0, named=True)
    assert stock["twpub_dgbas_gdp_log"] is None
    assert stock["twpub_official_trades_log"] == 2.0
    assert stock["twpub_pe_raw"] == 5.0
    assert build_all_research_features(wide_path=wide, official_path=official, output_path=output)["reused"]


def test_all_features_rejects_missing_official_keys(tmp_path):
    wide, official = _source_tables(tmp_path)
    broken = pl.read_parquet(official).with_columns(
        pl.when(pl.col("symbol") == "2330").then(pl.lit("9999")).otherwise(pl.col("symbol")).alias("symbol")
    )
    broken.write_parquet(official)
    with pytest.raises(ValueError, match="keys absent"):
        build_all_research_features(
            wide_path=wide, official_path=official, output_path=tmp_path / "all.parquet"
        )


def test_annual_join_keeps_cross_year_lags_raw_values_and_null_fallbacks(tmp_path):
    wide, official = _source_tables(tmp_path)
    for source in (wide, official):
        pl.read_parquet(source).with_columns(
            pl.when(pl.col("date") == date(2014, 1, 2)).then(pl.lit(date(2013, 12, 31)))
            .otherwise(pl.col("date")).alias("date")
        ).write_parquet(source)
    receipt = build_all_research_features(wide_path=wide, official_path=official,
                                          output_path=tmp_path / "annual.parquet")
    frame = pl.read_parquet(tmp_path / "annual.parquet")
    # A bounded fixture keeps the previous full-union relational expression as
    # an independent whole-table comparator, including schema and every cell.
    from stockagent.data.tw_public_research_all_features import _derived_market_values
    source = pl.scan_parquet(wide)
    expected = (
        source.drop(*[name for name in DERIVED_SOURCES if name in source.collect_schema().names()])
        .join(pl.scan_parquet(official).select(
            "date", "symbol", "twpub_official_trades_log", pl.col("twpub_pe_raw").alias("fallback")),
              on=["date", "symbol"], how="left")
        .with_columns(pl.coalesce("twpub_pe_raw", "fallback").alias("twpub_pe_raw")).drop("fallback")
        .join(_derived_market_values(source).lazy(), on=["date", "symbol"], how="left")
        .collect()
    )
    assert_frame_equal(frame.select(sorted(frame.columns)).sort("date", "symbol"),
                       expected.select(sorted(expected.columns)).sort("date", "symbol"), check_exact=True)
    market = frame.filter(pl.col("symbol") == "__MARKET__").sort("date")
    assert market.get_column("twpub_usdtwd_logret_1d").to_list() == [
        None, math.log(31 / 30), math.log(32 / 31)]
    assert market.get_column("twpub_cbc_overnight_rate_chg")[1] == pytest.approx(0.00001)
    assert market.get_column("twpub_mof_trade_balance_raw")[0] == -1_000_000
    stock = frame.filter(pl.col("symbol") == "2330").row(0, named=True)
    assert stock["twpub_pe_raw"] == 5.0 and stock["twpub_dgbas_gdp_log"] is None
    assert receipt["rows"] == 4 and receipt["peak_join_partition_rows"] == 2
    assert receipt["inputs"]["build_strategy"] == receipt["build_strategy"]


@pytest.mark.parametrize("duplicate", ["wide", "official"])
def test_annual_join_keeps_duplicate_validation_and_old_output_on_failure(tmp_path, duplicate):
    wide, official = _source_tables(tmp_path)
    source = wide if duplicate == "wide" else official
    frame = pl.read_parquet(source)
    # Duplicate a stock, not the separately checked market transform series.
    stock = frame.filter(pl.col("symbol") == "2330")
    pl.concat([frame, stock]).write_parquet(source)
    output = tmp_path / "previous.parquet"
    output.write_bytes(b"previous accepted source remains untouched")
    with pytest.raises(ValueError, match="duplicate"):
        build_all_research_features(wide_path=wide, official_path=official, output_path=output)
    assert output.read_bytes() == b"previous accepted source remains untouched"


def test_preopen_research_shifts_capture_snapshots_without_double_shifting_new_taifex():
    config = load_config(
        Path(__file__).resolve().parents[1]
        / "configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml"
    )
    patterns = config.data.feature_shift_next_session
    assert any(fnmatchcase("twpub_financial_eps", pattern) for pattern in patterns)
    assert any(fnmatchcase("twpub_company_industry_code", pattern) for pattern in patterns)
    assert any(fnmatchcase("twpub_taifex_tx_settlement_logret_1d", pattern) for pattern in patterns)
    assert not any(
        fnmatchcase("twpub_taifex_txo_official_put_volume_raw", pattern)
        for pattern in patterns
    )
