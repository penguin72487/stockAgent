from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

from stockagent.data.panel import build_panel
from stockagent.data.tw_public_features import (
    DEFAULT_MARKET_SYMBOL,
    FEATURE_COLUMNS,
    RULE_COLUMNS,
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
    assert set(RULE_COLUMNS).issubset(set(out.columns))
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


def test_build_panel_manual_feature_switch_supports_glob_include_and_exclude(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "symbol": [DEFAULT_MARKET_SYMBOL, "2330"],
            "twpub_usdtwd_logret_1d": [0.01, None],
            "twpub_pe_log": [None, 3.0],
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
        feature_include=["close_logret_1d", "twpub_*"],
        feature_exclude=["twpub_pe_log"],
    )

    assert panel.feature_names == ["close_logret_1d", "twpub_usdtwd_logret_1d"]
    assert panel.features.shape[-1] == 2


def test_external_tpex_limit_rule_columns_update_masks_without_becoming_features(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 110.0, 99.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "symbol": ["2330", "2330"],
            "_twpub_tpex_next_limit_up_ret": [np.log(110.0 / 100.0), np.log(121.0 / 110.0)],
            "_twpub_tpex_next_limit_down_ret": [np.log(90.0 / 100.0), np.log(99.0 / 110.0)],
            "twpub_pe_log": [3.0, 3.1],
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

    assert "_twpub_tpex_next_limit_up_ret" not in panel.feature_names
    assert "_twpub_tpex_next_limit_down_ret" not in panel.feature_names
    assert "twpub_pe_log" in panel.feature_names

    symbol_idx = panel.symbols.index("2330")
    date_0103 = int(np.where(panel.dates == np.datetime64("2024-01-03T00:00:00.000000000"))[0][0])
    date_0104 = int(np.where(panel.dates == np.datetime64("2024-01-04T00:00:00.000000000"))[0][0])
    assert bool(panel.can_buy_mask[date_0103, symbol_idx]) is False
    assert bool(panel.can_sell_mask[date_0103, symbol_idx]) is True
    assert bool(panel.can_buy_mask[date_0104, symbol_idx]) is True
    assert bool(panel.can_sell_mask[date_0104, symbol_idx]) is False


def test_official_missing_day_inside_listing_lifetime_is_frozen_suspension(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 100.0, 101.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "symbol": ["2330", "2330"],
            "_twpub_official_traded": [1.0, 1.0],
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
    symbol_idx = panel.symbols.index("2330")
    suspended_idx = int(np.where(panel.dates == np.datetime64("2024-01-03T00:00:00.000000000"))[0][0])
    assert bool(panel.tradable_mask[suspended_idx, symbol_idx]) is True
    assert bool(panel.can_buy_mask[suspended_idx, symbol_idx]) is False
    assert bool(panel.can_sell_mask[suspended_idx, symbol_idx]) is False
    assert panel.returns_1d[suspended_idx, symbol_idx] == np.float32(0.0)


def test_official_delisting_event_extends_suspension_then_marks_untradable(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 100.0, 100.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "symbol": ["2330", "2330"],
            "_twpub_official_traded": [1.0, None],
            "_twpub_delisted": [None, 1.0],
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
    symbol_idx = panel.symbols.index("2330")
    suspended_idx = int(np.where(panel.dates == np.datetime64("2024-01-03T00:00:00.000000000"))[0][0])
    delisted_idx = int(np.where(panel.dates == np.datetime64("2024-01-04T00:00:00.000000000"))[0][0])
    assert bool(panel.tradable_mask[suspended_idx, symbol_idx]) is True
    assert bool(panel.can_sell_mask[suspended_idx, symbol_idx]) is False
    assert bool(panel.tradable_mask[delisted_idx, symbol_idx]) is False


def test_feature_builder_emits_official_delisting_rule(tmp_path: Path) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            "market": ["twse"],
            "symbol": ["2330"],
            "company_name": ["測試"],
            "delisting_reason": ["測試原因"],
        }
    ).write_parquet(tmp_path / "twse_delisted_company.parquet")
    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "2330")
    assert out.filter(pl.col("_twpub_delisted") == 1.0).height == 1


def test_model_useful_features_use_official_report_date_and_emit_rules(tmp_path: Path) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "出表日期": ["1130215"],
            "公司代號": ["2330"],
            "營業收入-當月營收": ["1000000"],
            "營業收入-上月比較增減(%)": ["10"],
            "營業收入-去年同月增減(%)": ["20"],
            "累計營業收入-前期比較增減(%)": ["30"],
            "date": ["2024-03-01"],
        }
    ).write_parquet(tmp_path / "twse_api_opendata_t187ap05_l.parquet")
    pl.DataFrame(
        {
            "Date": ["1130216"],
            "SecuritiesCompanyCode": ["2330"],
            "ShortSaleSuspensionStartDate": ["1130219"],
            "ShortSaleSuspensionEndDate": ["1130220"],
            "date": ["2024-03-01"],
        }
    ).write_parquet(tmp_path / "tpex_api_tpex_margin_trading_term.parquet")
    pl.DataFrame(
        {
            "Code": ["2330"],
            "TradingHaltDate": ["1130221"],
            "TradingResumptionDate": ["1130223"],
            "date": ["2024-03-01"],
        }
    ).write_parquet(tmp_path / "twse_api_exchangereport_twtawu.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "2330").sort("date")

    revenue = out.filter(pl.col("twpub_monthly_revenue_log").is_not_null())
    assert revenue["date"].to_list() == [date(2024, 2, 15)]
    assert revenue["twpub_monthly_revenue_yoy"].to_list() == [0.2]
    assert out.filter(pl.col("_twpub_short_open_ban") == 1.0)["date"].to_list() == [
        date(2024, 2, 19),
        date(2024, 2, 20),
    ]
    assert out.filter(pl.col("_twpub_trading_halt") == 1.0)["date"].to_list() == [
        date(2024, 2, 21),
        date(2024, 2, 22),
    ]


def test_external_short_ban_and_halt_rules_update_directional_masks(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 100.0, 100.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "symbol": ["2330", "2330"],
            "_twpub_short_open_ban": [1.0, None],
            "_twpub_trading_halt": [None, 1.0],
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
    symbol_idx = panel.symbols.index("2330")
    assert bool(panel.can_short_open_mask[0, symbol_idx]) is False
    assert bool(panel.can_buy_mask[0, symbol_idx]) is True
    assert bool(panel.can_buy_mask[1, symbol_idx]) is False
    assert bool(panel.can_sell_mask[1, symbol_idx]) is False


def test_point_in_time_financial_features_forward_fill_only_after_publication(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            "symbol": ["2330"],
            "twpub_monthly_revenue_yoy": [0.25],
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
    feature_idx = panel.feature_names.index("twpub_monthly_revenue_yoy")
    symbol_idx = panel.symbols.index("2330")
    assert panel.features[0, symbol_idx, feature_idx] == np.float32(0.0)
    assert panel.features[1, symbol_idx, feature_idx] == np.float32(0.25)
    assert panel.features[2, symbol_idx, feature_idx] == np.float32(0.25)
