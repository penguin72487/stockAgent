from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

from stockagent.data.panel import build_panel
from stockagent.data.tw_public_features import (
    DEFAULT_MARKET_SYMBOL,
    FEATURE_COLUMNS,
    build_tw_public_training_features,
)


def _write_symbol(path: Path, closes: list[float]) -> None:
    rows = len(closes)
    dates = np.arange(np.datetime64("2024-01-02"), np.datetime64("2024-01-02") + rows)
    close = np.asarray(closes, dtype=np.float64)
    table = pa.table(
        {
            "date": pa.array(dates),
            "open": pa.array(close * 0.99),
            "max": pa.array(close * 1.01),
            "min": pa.array(close * 0.98),
            "close": pa.array(close),
            "adjclose": pa.array(close),
            "Trading_Volume": pa.array(np.full(rows, 1000.0)),
        }
    )
    pq.write_table(table, path)


def test_tw_public_feature_builder_outputs_sparse_stock_and_market_rows(tmp_path: Path) -> None:
    input_dir = tmp_path / "tw_public"
    input_dir.mkdir()
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 11.0])

    pl.DataFrame(
        {
            "證券代號": ["2330", "9999"],
            "本益比": ["20.5", "10.0"],
            "股價淨值比": ["5.2", "1.0"],
            "殖利率(%)": ["2.5", "0.0"],
            "date": ["2024-01-02", "2024-01-02"],
        }
    ).write_parquet(input_dir / "twse_daily_valuation.parquet")
    pl.DataFrame(
        {
            "代號": ["2330"],
            "前資餘額(張)": ["1,000"],
            "資買": ["100"],
            "資賣": ["50"],
            "資餘額": ["1,050"],
            "前券餘額(張)": ["10"],
            "券賣": ["3"],
            "券買": ["1"],
            "券餘額": ["12"],
            "date": ["2024-01-02"],
        }
    ).write_parquet(input_dir / "tpex_margin_balance.parquet")
    pl.DataFrame(
        {
            "日期": ["20240102", "20240103"],
            "NTD/USD": ["31.0", "31.31"],
        }
    ).write_parquet(input_dir / "cbc_usdtwd_closing_rate.parquet")
    pl.DataFrame(
        {
            "Date": ["20240102", "20240103"],
            "Contract": ["TX", "TX"],
            "ContractMonth(Week)": ["202401", "202401"],
            "Volume": ["10,000", "12,000"],
            "OpenInterest": ["30,000", "31,000"],
            "SettlementPrice": ["17500", "17675"],
            "TradingSession": ["一般", "一般"],
        }
    ).write_parquet(input_dir / "taifex_daily_futures.parquet")

    output_path = tmp_path / "tw_public_features.parquet"
    result = build_tw_public_training_features(input_dir, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path)

    assert result.rows == 3
    assert set(out["symbol"].to_list()) == {"2330", DEFAULT_MARKET_SYMBOL}
    assert "9999" not in set(out["symbol"].to_list())
    assert set(FEATURE_COLUMNS).issubset(set(out.columns))
    stock = out.filter(pl.col("symbol") == "2330").row(0, named=True)
    assert stock["twpub_pe_log"] is not None
    assert stock["twpub_margin_balance_log"] is not None
    market = out.filter(pl.col("symbol") == DEFAULT_MARKET_SYMBOL).sort("date")
    assert market.height == 2
    assert market["twpub_usdtwd_logret_1d"][1] is not None


def test_build_panel_aligns_external_stock_and_market_features(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    _write_symbol(tmp_path / "2317_features.parquet", [50.0, 50.5, 51.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-03"],
            "symbol": [DEFAULT_MARKET_SYMBOL, DEFAULT_MARKET_SYMBOL, "2330"],
            "twpub_usdtwd_logret_1d": [0.01, 0.02, None],
            "twpub_pe_log": [None, None, 3.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        benchmark_name="universe_average_return",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
    )

    assert "twpub_usdtwd_logret_1d" in panel.feature_names
    assert "twpub_pe_log" in panel.feature_names
    market_idx = panel.feature_names.index("twpub_usdtwd_logret_1d")
    pe_idx = panel.feature_names.index("twpub_pe_log")
    symbol_2330 = panel.symbols.index("2330")
    symbol_2317 = panel.symbols.index("2317")
    date_0103 = int(np.where(panel.dates == np.datetime64("2024-01-03T00:00:00.000000000"))[0][0])

    assert panel.features[date_0103, symbol_2330, market_idx] == np.float32(0.02)
    assert panel.features[date_0103, symbol_2317, market_idx] == np.float32(0.02)
    assert panel.features[date_0103, symbol_2330, pe_idx] == np.float32(3.0)
    assert panel.features[date_0103, symbol_2317, pe_idx] == np.float32(0.0)
