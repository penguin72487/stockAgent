from __future__ import annotations

from datetime import date
from fnmatch import fnmatchcase
import math
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_public_research_all_features import (
    DERIVED_SOURCES,
    build_all_research_features,
)
from stockagent.config import load_config


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
