from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import pytest

import stockagent.data.panel as panel_module
from scripts.audit_tw_public_data_layer import audit_feature_build_receipt
from stockagent.data.panel import build_panel, build_tail_panel
from stockagent.data.tw_public_features import (
    DEFAULT_MARKET_SYMBOL,
    FEATURE_COLUMNS,
    RULE_COLUMNS,
    _build_institutional_features,
    _build_margin_features,
    _build_official_ohlcv_features,
    _build_tdcc_features,
    _build_twse_market_index_features,
    _snapshot_date_expr,
    build_tw_public_training_features,
)


def test_tdcc_canonical_mirror_supersedes_overlap_and_direct_keeps_history(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "\ufeff資料日期": ["20240101", "20240108"],
            "證券代號": ["2330", "2330"],
            "持股分級": ["1", "1"],
            "人數": ["5", "10"],
            "占集保庫存數比例%": ["5.0", "10.0"],
        }
    ).write_parquet(tmp_path / "tdcc_shareholding_distribution.parquet")
    pl.DataFrame(
        {
            "資料日期": ["20240108"],
            "證券代號": ["2330"],
            "持股分級": ["1"],
            "人數": ["20"],
            "占集保庫存數比例%": ["20.0"],
        }
    ).write_parquet(tmp_path / "data_gov_tdcc_shareholding_distribution.parquet")

    result = _build_tdcc_features(tmp_path).sort("date")

    assert result.height == 2
    assert result.get_column("date").to_list() == [date(2024, 1, 8), date(2024, 1, 15)]
    assert result.get_column("twpub_tdcc_retail_holder_ratio").to_list() == [0.05, 0.2]
    assert result.get_column("twpub_tdcc_holder_count_log").to_list() == pytest.approx(
        [np.log1p(5), np.log1p(20)]
    )


def test_margin_short_rules_convert_official_lots_to_exact_shares_and_fail_closed(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "代號": ["2330", "2317", "1301"],
            "前日餘額": ["0", "0", "0"],
            "今日餘額": ["0", "0", "0"],
            "前日餘額_2": ["10", "7", "1"],
            "今日餘額_2": ["20", "7", "bad"],
            "次一營業日限額_2": ["100", "10", "20"],
            "買進": ["0", "0", "0"],
            "賣出": ["0", "0", "0"],
            "賣出_2": ["0", "0", "0"],
            "買進_2": ["0", "0", "0"],
            "註記": ["", "X", ""],
            "date": ["2024-01-02"] * 3,
        }
    ).write_parquet(tmp_path / "twse_margin_balance.parquet")
    pl.DataFrame(
        {
            "代號": ["6488", "5347"],
            "前資餘額(張)": ["0", "0"],
            "資買": ["0", "0"],
            "資賣": ["0", "0"],
            "資餘額": ["0", "0"],
            "前券餘額(張)": ["2", "1"],
            "券賣": ["0", "0"],
            "券買": ["0", "0"],
            "券餘額": ["3", "1"],
            "券限額": ["10", ""],
            "備註": ["", ""],
            "date": ["2024-01-02", "2024-01-02"],
        }
    ).write_parquet(tmp_path / "tpex_margin_balance.parquet")

    rules = _build_margin_features(tmp_path).select(
        [
            "symbol",
            "_twpub_margin_short_evidence_next_session",
            "_twpub_short_capacity_shares_next_session",
        ]
    )
    by_symbol = {row["symbol"]: row for row in rules.to_dicts()}

    assert by_symbol["2330"]["_twpub_margin_short_evidence_next_session"] == 1.0
    assert by_symbol["2330"]["_twpub_short_capacity_shares_next_session"] == 80_000.0
    assert by_symbol["6488"]["_twpub_margin_short_evidence_next_session"] == 1.0
    assert by_symbol["6488"]["_twpub_short_capacity_shares_next_session"] == 7_000.0
    for symbol in ("2317", "1301", "5347"):
        assert by_symbol[symbol]["_twpub_margin_short_evidence_next_session"] == 0.0
        assert by_symbol[symbol]["_twpub_short_capacity_shares_next_session"] == 0.0


def test_post_close_chip_history_moves_to_next_verified_exchange_session(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": ["2024-01-05", "2024-01-08", "2024-01-09"],
            "opening_index": [100.0, 101.0, 102.0],
            "highest_index": [101.0, 102.0, 103.0],
            "lowest_index": [99.0, 100.0, 101.0],
            "closing_index": [100.5, 101.5, 102.5],
        }
    ).write_parquet(tmp_path / "twse_taiex_ohlc.parquet")
    pl.DataFrame(
        {
            "代號": ["2330"],
            "前日餘額": ["100"],
            "今日餘額": ["120"],
            "前日餘額_2": ["10"],
            "今日餘額_2": ["12"],
            "次一營業日限額_2": ["100"],
            "買進": ["30"],
            "賣出": ["10"],
            "賣出_2": ["4"],
            "買進_2": ["2"],
            "註記": [""],
            "date": ["2024-01-05"],
        }
    ).write_parquet(tmp_path / "twse_margin_balance.parquet")
    pl.DataFrame(
        {
            "證券代號": ["2330"],
            "外陸資買賣超股數(不含外資自營商)": ["1000"],
            "投信買賣超股數": ["200"],
            "自營商買賣超股數": ["-50"],
            "三大法人買賣超股數": ["1150"],
            "date": ["2024-01-05"],
        }
    ).write_parquet(tmp_path / "twse_institutional_trades.parquet")

    margin = _build_margin_features(tmp_path).sort("date")
    institutional = _build_institutional_features(tmp_path).sort("date")

    margin_source = margin.filter(pl.col("date") == date(2024, 1, 5)).row(
        0, named=True
    )
    margin_available = margin.filter(pl.col("date") == date(2024, 1, 8)).row(
        0, named=True
    )
    assert margin_source["twpub_margin_balance_log"] is None
    assert margin_source["_twpub_margin_short_evidence_next_session"] == 1.0
    assert margin_available["twpub_margin_balance_log"] == pytest.approx(
        np.log1p(120)
    )
    assert margin_available["_twpub_margin_short_evidence_next_session"] is None
    assert institutional.get_column("date").to_list() == [date(2024, 1, 8)]
    assert institutional.row(0, named=True)[
        "twpub_investment_trust_net_buy_flow"
    ] == pytest.approx(np.arcsinh(200 / 1000.0))


def test_margin_short_capacity_is_exactly_next_session_and_never_forward_filled(
    tmp_path: Path,
) -> None:
    dates = np.asarray(
        ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
        dtype="datetime64[D]",
    )
    close = np.full((dates.size,), 100.0, dtype=np.float64)
    pq.write_table(
        pa.table(
            {
                "date": pa.array(dates),
                "open": pa.array(close),
                "max": pa.array(close),
                "min": pa.array(close),
                "close": pa.array(close),
                "adjclose": pa.array(close),
                "Trading_Volume": pa.array(np.full(dates.size, 1_000.0)),
            }
        ),
        tmp_path / "2330_features.parquet",
    )
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04", "2024-01-05"],
            "symbol": ["2330", "2330", "2330"],
            "_twpub_margin_short_evidence_next_session": [1.0, 1.0, 1.0],
            "_twpub_short_capacity_shares_next_session": [
                6_483_021_000.0,
                2_000.0,
                9_000.0,
            ],
            # The 2024-01-05 ban intersects the capacity shifted from 01-04.
            "_twpub_short_open_ban": [0.0, 0.0, 1.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )

    # Source rows describe the *next* exchange session.  Missing evidence on
    # 01-03 therefore closes 01-04 instead of carrying the 01-02 receipt.
    np.testing.assert_array_equal(
        panel.short_capacity_shares[:, 0],
        np.asarray([0, 6_483_021_000, 0, 0, 9_000], dtype=np.int64),
    )
    np.testing.assert_array_equal(
        panel.can_short_open_mask[:, 0],
        [False, True, False, False, True],
    )


def test_margin_short_eligibility_is_independent_of_zero_capacity(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02"],
            "symbol": ["2330"],
            "_twpub_margin_short_evidence_next_session": [1.0],
            "_twpub_short_capacity_shares_next_session": [0.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )

    np.testing.assert_array_equal(panel.short_capacity_shares[:, 0], [0, 0])
    np.testing.assert_array_equal(panel.can_short_open_mask[:, 0], [False, True])


def test_all_null_margin_short_schema_is_fail_closed_not_generic_sell_permission(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02"],
            "symbol": ["2330"],
            "_twpub_margin_short_evidence_next_session": [None],
            "_twpub_short_capacity_shares_next_session": [None],
        },
        schema_overrides={
            "_twpub_margin_short_evidence_next_session": pl.Float64,
            "_twpub_short_capacity_shares_next_session": pl.Float64,
        },
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )

    assert panel.can_sell_mask.all()
    assert not panel.can_short_open_mask.any()
    np.testing.assert_array_equal(
        panel.short_capacity_shares,
        np.zeros_like(panel.tradable_mask, dtype=np.int64),
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
            "date": ["2024-01-02", "2024-01-03"],
            "opening_index": [100.0, 101.0],
            "highest_index": [101.0, 102.0],
            "lowest_index": [99.0, 100.0],
            "closing_index": [100.5, 101.5],
        }
    ).write_parquet(input_dir / "twse_taiex_ohlc.parquet")

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

    assert result.rows == 4
    assert set(out["symbol"].to_list()) == {"2330", DEFAULT_MARKET_SYMBOL}
    assert "9999" not in set(out["symbol"].to_list())
    assert set(FEATURE_COLUMNS).issubset(set(out.columns))
    assert set(RULE_COLUMNS).issubset(set(out.columns))
    assert "_twpub_force_cover_delisting_ordinal" not in out.columns
    stock = out.filter(pl.col("symbol") == "2330").sort("date")
    assert stock.filter(pl.col("date") == date(2024, 1, 2)).row(0, named=True)[
        "twpub_pe_log"
    ] is not None
    assert stock.filter(pl.col("date") == date(2024, 1, 3)).row(0, named=True)[
        "twpub_margin_balance_log"
    ] is not None
    market = out.filter(pl.col("symbol") == DEFAULT_MARKET_SYMBOL).sort("date")
    assert market.height == 2
    assert market["twpub_usdtwd_logret_1d"][1] is not None
    summary = json.loads(output_path.with_suffix(".summary.json").read_text(encoding="utf-8"))
    assert summary["source_receipts"]
    assert summary["output_receipt"]["sha256"]
    assert summary["symbol_universe_receipt"]["file_count"] == 1
    receipt, findings = audit_feature_build_receipt(output_path, input_dir, symbols_root)
    assert receipt["valid"] is True
    assert findings == []

    with (input_dir / "twse_daily_valuation.parquet").open("ab") as handle:
        handle.write(b"changed")
    receipt, findings = audit_feature_build_receipt(output_path, input_dir, symbols_root)
    assert receipt["valid"] is False
    assert [item.code for item in findings] == ["stale_feature_build_receipt"]


def test_tw_public_feature_builder_excludes_rows_after_completed_cutoff(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "tw_public"
    input_dir.mkdir()
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 11.0])
    pl.DataFrame(
        {
            "證券代號": ["2330", "2330"],
            "本益比": ["20.5", "21.0"],
            "股價淨值比": ["5.2", "5.3"],
            "殖利率(%)": ["2.5", "2.4"],
            "date": ["2024-01-02", "2024-01-03"],
        }
    ).write_parquet(input_dir / "twse_daily_valuation.parquet")

    output_path = tmp_path / "tw_public_features.parquet"
    build_tw_public_training_features(
        input_dir,
        output_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 2),
    )

    assert pl.read_parquet(output_path).get_column("date").max() == date(2024, 1, 2)


def test_legacy_tpex_quote_statistics_feed_public_features_without_fabricating_price(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": ["2007-01-02", "2007-01-03"],
            "代號": ["4801", "4801"],
            "收盤": ["0.00", "26.80"],
            "最高": ["0.00", "27.00"],
            "最低": ["0.00", "26.50"],
            "成交股數": ["240", "1,000"],
            "成交金額(元)": ["6,408", "26,800"],
            "成交筆數": ["1", "10"],
            "發行股數": ["46,347,952", "46,347,952"],
            "次日漲停價": ["30.70", "30.80"],
            "次日跌停價": ["26.70", "24.80"],
        }
    ).write_parquet(tmp_path / "tpex_daily_ohlcv.parquet")

    output = _build_official_ohlcv_features(tmp_path).sort("date")

    first = output.row(0, named=True)
    assert first["_twpub_official_traded"] == 1.0
    assert first["twpub_official_trading_volume_log"] == pytest.approx(np.log1p(240))
    assert first["twpub_official_trading_value_log"] == pytest.approx(np.log1p(6408))
    assert first["twpub_official_trades_log"] == pytest.approx(np.log1p(1))
    assert first["twpub_official_intraday_range"] is None
    assert first["twpub_official_close_to_high"] is None
    assert first["twpub_official_close_to_low"] is None


def test_taiex_monthly_archive_is_complete_base_and_ind_has_same_day_priority(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5), date(2000, 1, 6)],
            "opening_index": [100.0, 100.5, 101.5],
            "highest_index": [100.5, 101.5, 102.5],
            "lowest_index": [99.5, 100.0, 101.0],
            "closing_index": [100.0, 101.0, 102.0],
        }
    ).write_parquet(tmp_path / "twse_taiex_ohlc.parquet")
    pl.DataFrame(
        {
            "date": ["2000-01-05", "2000-01-06"],
            "指數": ["發行量加權股價指數", "發行量加權股價指數"],
            "收盤指數": ["101.00", "102.00"],
            # Deliberately differs from close-to-close on 01-05 so the test
            # proves that a valid same-day IND percentage has priority.
            "漲跌百分比(%)": ["1.50", "0.99"],
        }
    ).write_parquet(tmp_path / "twse_market_index.parquet")

    output = _build_twse_market_index_features(
        tmp_path,
        market_symbol=DEFAULT_MARKET_SYMBOL,
    ).sort("date")

    assert output.get_column("date").to_list() == [
        date(2000, 1, 4),
        date(2000, 1, 5),
        date(2000, 1, 6),
    ]
    assert output.get_column("symbol").unique().to_list() == [DEFAULT_MARKET_SYMBOL]
    assert output.get_column("_twpub_official_traded").to_list() == [1.0, 1.0, 1.0]
    assert output.get_column("twpub_twse_taiex_pct").to_list()[1:] == pytest.approx(
        [0.015, 0.0099]
    )
    assert output.get_column("twpub_twse_taiex_logret_1d").to_list()[1:] == pytest.approx(
        [np.log(1.01), np.log(102.0 / 101.0)]
    )


def test_taiex_session_marker_keeps_quote_missing_session_fail_closed(
    tmp_path: Path,
) -> None:
    symbol_path = tmp_path / "2330_features.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 4)],
            "open": [100.0, 101.0],
            "max": [101.0, 102.0],
            "min": [99.0, 100.0],
            "close": [100.0, 101.0],
            "adjclose": [10.0, 10.1],
            "Trading_Volume": [1000.0, 1100.0],
        }
    ).write_parquet(symbol_path)
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": [
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
            ],
            "symbol": [DEFAULT_MARKET_SYMBOL] * 3,
            "_twpub_official_traded": [1.0, 1.0, 1.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
    )

    missing_idx = int(np.flatnonzero(panel.dates == np.datetime64("2024-01-03"))[0])
    symbol_idx = panel.symbols.index("2330")
    assert np.isnan(panel.close_prices[missing_idx, symbol_idx])
    assert np.isnan(panel.daily_volumes[missing_idx, symbol_idx])
    assert np.isnan(panel.returns_1d[missing_idx, symbol_idx])
    assert bool(panel.tradable_mask[missing_idx, symbol_idx]) is False
    assert bool(panel.can_buy_mask[missing_idx, symbol_idx]) is False
    assert bool(panel.can_sell_mask[missing_idx, symbol_idx]) is False


@pytest.mark.parametrize("panel_backend", ["pyarrow", "polars_lazy"])
def test_explicit_return_quarantine_masks_only_the_forward_label(
    tmp_path: Path,
    panel_backend: str,
) -> None:
    symbol_path = tmp_path / "2330_features.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "open": [10.0, 40.0],
            "max": [10.0, 40.0],
            "min": [10.0, 40.0],
            "close": [10.0, 40.0],
            "adjclose": [10.0, 40.0],
            "Trading_Volume": [1000.0, 1000.0],
            "return_quarantined": [True, False],
            "return_quarantine_reason": [
                "unverified_extreme_adjusted_return",
                None,
            ],
        }
    ).write_parquet(symbol_path)

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend=panel_backend,
        panel_load_workers=0,
    )

    symbol_idx = panel.symbols.index("2330")
    assert panel.close_prices[:, symbol_idx].tolist() == [10.0, 40.0]
    assert np.isnan(panel.returns_1d[0, symbol_idx])


def test_taiex_archive_and_ind_overlap_mismatch_fails_closed(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": [date(2009, 1, 5)],
            "opening_index": [4_600.0],
            "highest_index": [4_710.0],
            "lowest_index": [4_590.0],
            "closing_index": [4_698.31],
        }
    ).write_parquet(tmp_path / "twse_taiex_ohlc.parquet")
    pl.DataFrame(
        {
            "date": ["2009-01-05"],
            "指數": ["發行量加權股價指數"],
            "收盤指數": ["4,698.33"],
            "漲跌百分比(%)": ["2.33"],
        }
    ).write_parquet(tmp_path / "twse_market_index.parquet")

    with pytest.raises(ValueError, match="TAIEX close mismatch"):
        _build_twse_market_index_features(
            tmp_path,
            market_symbol=DEFAULT_MARKET_SYMBOL,
        )


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


def test_build_panel_zero_fill_keeps_feature_slots_and_zeros_matching_values(tmp_path: Path) -> None:
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
        feature_zero_fill=["twpub_*"],
    )

    assert panel.feature_names == [
        "close_logret_1d",
        "twpub_usdtwd_logret_1d",
        "twpub_pe_log",
    ]
    assert panel.features.shape[-1] == 3
    assert np.count_nonzero(panel.features[:, :, 1:]) == 0

    # A second experiment using the same parquet root but the unmodified
    # feature values gets its own immutable cache generation. Switching back
    # must reuse the zero-filled variant instead of rebuilding or inheriting
    # the other experiment's values.
    raw_panel = build_panel(
        tmp_path,
        benchmark_name="universe_average_return",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        feature_include=["close_logret_1d", "twpub_*"],
        feature_zero_fill=[],
    )
    assert np.count_nonzero(raw_panel.features[:, :, 1:]) > 0

    cache_dir = tmp_path / "panel_cache_v2"
    assert len(list((cache_dir / "variants").glob("*.json"))) == 2
    assert len(list((cache_dir / "generations").iterdir())) == 2

    zero_filled_again = build_panel(
        tmp_path,
        benchmark_name="universe_average_return",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        feature_include=["close_logret_1d", "twpub_*"],
        feature_zero_fill=["twpub_*"],
    )
    assert np.count_nonzero(zero_filled_again.features[:, :, 1:]) == 0
    assert len(list((cache_dir / "generations").iterdir())) == 2


def test_market_point_in_time_state_is_forward_filled_after_release(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    _write_symbol(tmp_path / "2317_features.parquet", [50.0, 51.0, 52.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "symbol": [DEFAULT_MARKET_SYMBOL, DEFAULT_MARKET_SYMBOL],
            "twpub_dgbas_cpi_log": [4.5, 4.6],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        benchmark_name="2330",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        feature_include=["twpub_dgbas_cpi_log"],
    )

    feature_idx = panel.feature_names.index("twpub_dgbas_cpi_log")
    date_0103 = int(np.flatnonzero(panel.dates == np.datetime64("2024-01-03"))[0])
    assert np.allclose(panel.features[date_0103, :, feature_idx], 4.5)


@pytest.mark.parametrize("benchmark_symbol", ["2330", "0050"])
def test_equity_and_etf_benchmarks_use_total_return_adjusted_close(
    tmp_path: Path,
    benchmark_symbol: str,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "open": [100.0, 90.0, 91.0],
            "max": [100.0, 90.0, 91.0],
            "min": [100.0, 90.0, 91.0],
            "close": [100.0, 90.0, 91.0],
            # The raw 10% price drop is an ex-distribution boundary.  Total
            # return is flat across it, then gains 1% on the next session.
            "adjclose": [50.0, 50.0, 50.5],
            "Trading_Volume": [1_000.0, 1_000.0, 1_000.0],
        }
    ).write_parquet(tmp_path / f"{benchmark_symbol}_features.parquet")

    panel = build_panel(
        tmp_path,
        benchmark_name=benchmark_symbol,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
    )

    assert panel.benchmark_returns.tolist() == pytest.approx(
        [0.0, np.log(50.5 / 50.0), 0.0]
    )
    assert panel.benchmark_returns[0] != pytest.approx(np.log(90.0 / 100.0))


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


def test_external_rules_can_be_enabled_without_appending_model_features(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            "symbol": ["2330"],
            "twpub_pe_log": [3.0],
            "_twpub_short_open_ban": [1.0],
        }
    ).write_parquet(external_path)

    rule_only = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )
    symbol_idx = rule_only.symbols.index("2330")
    date_idx = int(
        np.where(
            rule_only.dates == np.datetime64("2024-01-03T00:00:00.000000000")
        )[0][0]
    )
    assert "twpub_pe_log" not in rule_only.feature_names
    assert bool(rule_only.can_short_open_mask[date_idx, symbol_idx]) is False
    assert bool(rule_only.can_sell_mask[date_idx, symbol_idx]) is True

    # The same source path with the opposite switches must not reuse the
    # rule-only panel cache.
    feature_only = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=True,
        external_include_rules=False,
        external_data_required=True,
    )
    assert "twpub_pe_log" in feature_only.feature_names
    assert bool(feature_only.can_short_open_mask[date_idx, symbol_idx]) is True

    live_tail = build_tail_panel(
        tmp_path,
        tail_rows=3,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )
    assert "twpub_pe_log" not in live_tail.feature_names
    assert bool(live_tail.can_short_open_mask[date_idx, symbol_idx]) is False


def test_day_trade_direction_rule_is_exact_session_and_does_not_change_naive_shorting(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "symbol": ["2330", "2330", "2330"],
            "_twpub_day_trade_eligible": [1.0, 0.0, 1.0],
            "_twpub_day_trade_short_open": [0.0, 0.0, 1.0],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )

    np.testing.assert_array_equal(
        panel.day_trade_eligible_mask[:, 0], [True, False, True]
    )
    np.testing.assert_array_equal(
        panel.day_trade_can_short_open_mask[:, 0], [False, False, True]
    )
    # The ordinary short-open rule remains independent for naive/margin paths.
    np.testing.assert_array_equal(panel.can_short_open_mask[:, 0], [True, True, True])


def test_all_null_day_trade_rule_schema_is_absent_evidence_not_false_history(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02"],
            "symbol": ["2330"],
            "_twpub_day_trade_eligible": [None],
            "_twpub_day_trade_short_open": [None],
        },
        schema_overrides={
            "_twpub_day_trade_eligible": pl.Float64,
            "_twpub_day_trade_short_open": pl.Float64,
        },
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
        external_data_required=True,
    )
    assert panel.day_trade_eligible_mask is None
    assert panel.day_trade_can_short_open_mask is None


def test_rules_only_external_loader_projects_rule_columns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            "symbol": ["2330"],
            "twpub_large_unused_feature": [123.0],
            "_twpub_short_open_ban": [1.0],
        }
    ).write_parquet(external_path)
    observed_columns: list[list[str] | None] = []
    original_read_table = panel_module.pq.read_table

    def recording_read_table(path, *args, **kwargs):
        observed_columns.append(kwargs.get("columns"))
        return original_read_table(path, *args, **kwargs)

    monkeypatch.setattr(panel_module.pq, "read_table", recording_read_table)
    loaded = panel_module._load_external_feature_arrays(
        external_path,
        include_features=False,
        include_rules=True,
    )

    assert observed_columns == [["date", "symbol", "_twpub_short_open_ban"]]
    assert loaded.feature_names == []
    assert loaded.rule_names == ["_twpub_short_open_ban"]


def test_required_external_rule_source_fails_fast_when_missing(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0])
    with pytest.raises(FileNotFoundError, match="external_feature_path not found"):
        build_panel(
            tmp_path,
            panel_backend="pyarrow",
            panel_load_workers=0,
            external_feature_path=tmp_path / "missing.parquet",
            external_include_features=False,
            external_include_rules=True,
            external_data_required=True,
        )


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
    last_executable_idx = int(
        np.where(
            panel.dates == np.datetime64("2024-01-02T00:00:00.000000000")
        )[0][0]
    )
    suspended_idx = int(np.where(panel.dates == np.datetime64("2024-01-03T00:00:00.000000000"))[0][0])
    delisted_idx = int(np.where(panel.dates == np.datetime64("2024-01-04T00:00:00.000000000"))[0][0])
    assert bool(panel.tradable_mask[suspended_idx, symbol_idx]) is True
    assert bool(panel.can_sell_mask[suspended_idx, symbol_idx]) is False
    assert bool(panel.tradable_mask[delisted_idx, symbol_idx]) is False
    assert bool(panel.force_exit_mask[last_executable_idx, symbol_idx]) is True
    assert bool(panel.force_exit_mask[suspended_idx, symbol_idx]) is False
    assert bool(panel.force_exit_mask[delisted_idx, symbol_idx]) is False
    assert not bool(panel.can_buy_mask[suspended_idx:, symbol_idx].any())
    assert not bool(panel.can_sell_mask[suspended_idx:, symbol_idx].any())


def test_delisting_without_event_day_quote_exits_at_final_positive_close(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0])
    # A second symbol extends the global panel calendar beyond 2330's final
    # quote, reproducing a suspension before the official termination date.
    _write_symbol(
        tmp_path / "2317_features.parquet",
        [50.0, 51.0, 52.0, 53.0],
    )
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-05"],
            "symbol": ["2330"],
            "_twpub_delisted": [1.0],
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
    last_quote_idx = int(
        np.where(panel.dates == np.datetime64("2024-01-03"))[0][0]
    )
    termination_idx = int(
        np.where(panel.dates == np.datetime64("2024-01-05"))[0][0]
    )

    assert panel.close_prices[last_quote_idx, symbol_idx] == pytest.approx(101.0)
    assert np.isnan(panel.close_prices[termination_idx, symbol_idx])
    assert bool(panel.force_exit_mask[last_quote_idx, symbol_idx]) is True
    assert bool(panel.force_exit_mask[termination_idx, symbol_idx]) is False
    assert bool(panel.tradable_mask[termination_idx, symbol_idx]) is False


def test_same_symbol_trading_next_session_is_not_a_terminal_delisting(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2301_features.parquet", [100.0, 101.0, 102.0, 103.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-04"],
            "symbol": ["2301"],
            "_twpub_delisted": [1.0],
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
    symbol_idx = panel.symbols.index("2301")
    transition_idx = int(
        np.where(
            panel.dates == np.datetime64("2024-01-04T00:00:00.000000000")
        )[0][0]
    )
    next_session_idx = int(
        np.where(
            panel.dates == np.datetime64("2024-01-05T00:00:00.000000000")
        )[0][0]
    )

    assert bool(panel.force_exit_mask[transition_idx, symbol_idx]) is False
    assert bool(panel.tradable_mask[transition_idx, symbol_idx]) is True
    assert bool(panel.tradable_mask[next_session_idx, symbol_idx]) is True
    assert bool(panel.can_sell_mask[next_session_idx, symbol_idx]) is True


def test_sparse_official_traded_snapshots_do_not_imply_a_multi_year_halt(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0] * 10)
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-11"],
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
    gap_rows = (panel.dates > np.datetime64("2024-01-02")) & (
        panel.dates < np.datetime64("2024-01-11")
    )

    assert bool(panel.can_buy_mask[gap_rows, symbol_idx].all()) is True
    assert bool(panel.can_sell_mask[gap_rows, symbol_idx].all()) is True


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


def test_feature_builder_does_not_force_exit_same_symbol_tpex_to_twse_transfer(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "4722_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "date": ["2012-08-15"],
            "market": ["tpex"],
            "symbol": ["4722"],
            "company_name": ["國精化學"],
            "delisting_reason": ["依本中心業務規則第12條之2第1項第1款"],
        }
    ).write_parquet(tmp_path / "tpex_delisted_company.parquet")
    pl.DataFrame(
        {
            "Code": ["4722"],
            "Company": ["國精化"],
            "ApprovedListingDate": ["1010815"],
            "Note": ["櫃轉市"],
        }
    ).write_parquet(tmp_path / "twse_api_company_newlisting.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "4722")

    assert out.filter(pl.col("_twpub_delisted") == 1.0).is_empty()


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


def test_upcoming_delisting_announcement_bans_new_shorts_without_post_event_delisted_file(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": ["2024-01-02"],
            "symbols": ["2330"],
            "subject": ["公告2330將終止上市"],
            "body_text": ["應於終止上市前第10個營業日前償還或還券了結"],
            "short_open_ban_date": [None],
            "short_cover_deadline": ["2024-01-04"],
            "delisting_date": ["2024-01-05"],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")
    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "2330").sort("date")

    assert out.filter(pl.col("_twpub_short_open_ban") == 1.0)["date"].to_list() == [
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]
    assert out.filter(pl.col("_twpub_force_short_cover") == 1.0)["date"].to_list() == [date(2024, 1, 4)]
    relative = out.filter(pl.col("_twpub_force_cover_lead_sessions") == 10.0)
    assert relative["date"].to_list() == [date(2024, 1, 2)]
    assert relative["_twpub_force_cover_anchor_ordinal"].to_list() == [
        float(date(2024, 1, 5).toordinal())
    ]
    assert out.filter(pl.col("_twpub_delisted").is_not_null()).is_empty()


def test_uncancelled_etf_delisting_notice_emits_terminal_rule_without_company_file(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "00925_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": ["2025-04-28"],
            "symbols": ["00925"],
            "subject": ["新光標普電動車ETF（00925）受益憑證終止上市"],
            "body_text": ["自114年6月5日起終止上市並暫停融資融券交易"],
            "short_open_ban_date": ["2025-04-28"],
            "short_cover_deadline": [None],
            "delisting_date": ["2025-06-05"],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "00925")

    assert out.filter(pl.col("_twpub_short_open_ban") == 1.0).height > 0
    assert out.filter(pl.col("_twpub_delisted") == 1.0)["date"].to_list() == [
        date(2025, 6, 5)
    ]


def test_article_78_exempt_notice_does_not_infer_short_ban_or_relative_cover(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "4130_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": ["2024-01-02"],
            "symbols": ["4130"],
            "subject": ["公告健亞公司（股票代號：4130）終止上櫃"],
            "body_text": [
                "依第78條第1項第3款規定，證券商無須通知委託人於股票終止上櫃"
                "前10個營業日前償還或還券了結"
            ],
            "short_open_ban_date": [None],
            "short_cover_deadline": [None],
            "short_cover_lead_trading_days": [10],
            "delisting_date": ["2024-01-05"],
            "article_78_exempt": [True],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "4130")

    assert out.filter(pl.col("_twpub_short_open_ban").is_not_null()).is_empty()
    assert out.filter(pl.col("_twpub_force_short_cover").is_not_null()).is_empty()
    assert out.filter(pl.col("_twpub_force_cover_lead_sessions").is_not_null()).is_empty()


def test_historical_short_exemptions_and_cancelled_ban_are_clause_specific(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    for symbol in ("3068", "5349", "5301"):
        _write_symbol(symbols_root / f"{symbol}_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": [
                "2018-05-17",
                "2020-10-15",
                "2018-05-16",
                "2018-05-17",
            ],
            "symbols": ["3068", "5349", "5301", "5301"],
            "subject": [
                "3068普通股自107年6月13日起終止櫃檯買賣",
                "5349普通股自109年11月4日起終止櫃檯買賣",
                "5301自107年5月18日起暫停融資融券交易",
                "5301原公告自107年5月18日起暫停融資融券交易乙案，免予執行",
            ],
            "body_text": [
                "股票於終止櫃檯買賣前，融資融券交易無須提前了結。",
                (
                    "融券餘額應於停止過戶第六個營業日前還券了結，"
                    "融資餘額則無須適用終止上櫃前第十個營業日前償還之規定。"
                ),
                "自107年5月18日起停止買賣，並自同日起暫停融資融券交易。",
                "原停止買賣及暫停融資融券交易原因業已消滅，免予執行。",
            ],
            # Include stale cached fields to verify the shared classifier wins.
            "short_open_ban_date": [None, None, "2018-05-18", "2018-05-18"],
            "short_cover_deadline": [None, None, None, None],
            "short_cover_lead_trading_days": [10, None, None, None],
            "short_cover_anchor_date": [None, "2020-10-31", None, None],
            "short_cover_anchor_lead_trading_days": [None, 6, None, None],
            "delisting_date": ["2018-06-13", "2020-11-04", None, None],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path)

    exempt = out.filter(pl.col("symbol") == "3068")
    assert exempt.filter(pl.col("_twpub_short_open_ban").is_not_null()).is_empty()
    assert exempt.filter(pl.col("_twpub_force_cover_lead_sessions").is_not_null()).is_empty()

    financing_only = out.filter(pl.col("symbol") == "5349")
    assert financing_only.filter(pl.col("_twpub_short_open_ban") == 1.0).height > 0
    stop_transfer_cover = financing_only.filter(
        pl.col("_twpub_force_cover_lead_sessions") == 6.0
    )
    assert stop_transfer_cover["date"].to_list() == [date(2020, 10, 15)]
    assert stop_transfer_cover["_twpub_force_cover_anchor_ordinal"].to_list() == [
        float(date(2020, 10, 31).toordinal())
    ]

    cancelled = out.filter(pl.col("symbol") == "5301")
    assert cancelled.filter(pl.col("_twpub_short_open_ban").is_not_null()).is_empty()


def test_delisting_cancellation_removes_pending_cover_but_preserves_continuing_ban(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "1435_features.parquet", [10.0, 10.0])
    cancellation_text = (
        "上市有價證券原將於112年4月2日終止上市，已融資融券者應於"
        "終止上市前第10個營業日前償還或還券了結，惟已免除終止上市，"
        "故同步免除前開了結事宜，爰繼續暫停融資融券交易。"
    )
    pl.DataFrame(
        {
            "announcement_date": ["2023-02-20", "2023-03-16"],
            "market": ["twse", "twse"],
            "symbols": ["1435", "1435"],
            "subject": [
                "1435上市有價證券將於112年4月2日終止上市",
                cancellation_text,
            ],
            "body_text": [
                "應於終止上市前第10個營業日前償還或還券了結。",
                cancellation_text,
            ],
            "short_open_ban_date": [None, None],
            "short_cover_deadline": [None, None],
            # The second row deliberately mimics a stale cached parse.  Its
            # cancellation semantics must override these copied values.
            "short_cover_lead_trading_days": [10, 10],
            "delisting_date": ["2023-04-02", "2023-04-02"],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "1435")

    pending_cover = out.filter(
        pl.col("_twpub_force_cover_lead_sessions").is_not_null()
    )
    assert pending_cover.height == 1
    assert pending_cover["_twpub_force_cover_anchor_ordinal"].to_list() == [
        float(date(2023, 4, 2).toordinal())
    ]
    assert pending_cover["_twpub_force_cover_cancel_ordinal"].to_list() == [
        float(date(2023, 3, 16).toordinal())
    ]
    ban_dates = out.filter(pl.col("_twpub_short_open_ban") == 1.0)["date"]
    assert ban_dates.min() == date(2023, 2, 21)
    assert date(2023, 3, 15) in ban_dates
    assert date(2023, 3, 16) in ban_dates
    assert ban_dates.max() >= date.today()


def test_replacement_share_termination_is_not_treated_as_company_delisting(tmp_path: Path) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "6531_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": ["2024-01-02"],
            "symbols": ["6531"],
            "subject": ["因變更股票面額，舊股票將終止上市並換發新股票"],
            "body_text": ["新股票將於同日繼續上市買賣"],
            "short_open_ban_date": [None],
            "short_cover_deadline": [None],
            "delisting_date": ["2024-01-05"],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "6531")
    assert out.filter(pl.col("_twpub_short_open_ban").is_not_null()).is_empty()


def test_announcement_short_ban_ends_before_separate_resume_notice(tmp_path: Path) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "1333_features.parquet", [10.0, 10.0])
    pl.DataFrame(
        {
            "announcement_date": ["2024-01-02", "2024-01-04"],
            "symbols": ["1333", "1333"],
            "subject": ["暫停融資融券", "恢復融資融券"],
            "body_text": ["", ""],
            "short_open_ban_date": ["2024-01-03", None],
            "short_open_resume_date": [None, "2024-01-05"],
            "short_cover_deadline": [None, None],
            "delisting_date": [None, None],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    out = pl.read_parquet(output_path).filter(pl.col("symbol") == "1333")
    assert out.filter(pl.col("_twpub_short_open_ban") == 1.0)["date"].sort().to_list() == [
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]


def test_open_ended_explicit_short_ban_persists_through_build_date(
    tmp_path: Path,
) -> None:
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "1333_features.parquet", [10.0, 10.0])
    announcement = date.today() - timedelta(days=3)
    ban_start = date.today() - timedelta(days=2)
    pl.DataFrame(
        {
            "announcement_date": [announcement.isoformat()],
            "symbols": ["1333"],
            "subject": ["1333自明日起暫停融資融券"],
            "body_text": ["禁令另行公告恢復"],
            "short_open_ban_date": [ban_start.isoformat()],
            "short_open_resume_date": [None],
            "short_cover_deadline": [None],
            "delisting_date": [None],
        }
    ).write_parquet(tmp_path / "tw_delisting_short_sale_announcements.parquet")

    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(tmp_path, output_path, symbols_root=symbols_root)
    banned_dates = (
        pl.read_parquet(output_path)
        .filter(
            (pl.col("symbol") == "1333")
            & (pl.col("_twpub_short_open_ban") == 1.0)
        )["date"]
        .to_list()
    )

    assert ban_start in banned_dates
    assert date.today() in banned_dates


def test_external_short_ban_and_halt_rules_update_directional_masks(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 100.0, 100.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "symbol": ["2330", "2330", "2330"],
            "_twpub_short_open_ban": [None, 1.0, None],
            "_twpub_force_short_cover": [1.0, None, 1.0],
            "_twpub_trading_halt": [None, None, 1.0],
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
    assert bool(panel.force_short_cover_mask[0, symbol_idx]) is True
    assert bool(panel.force_short_cover_mask[1, symbol_idx]) is False
    assert bool(panel.force_short_cover_mask[2, symbol_idx]) is True
    assert bool(panel.can_buy_mask[0, symbol_idx]) is True
    # A short-sale ban blocks borrowing/increasing a short.  It must not block
    # selling an already-owned long position.
    assert bool(panel.can_sell_mask[0, symbol_idx]) is True
    assert bool(panel.can_buy_mask[1, symbol_idx]) is False
    assert bool(panel.can_sell_mask[1, symbol_idx]) is False


def test_relative_delisting_cover_rule_uses_panel_sessions_without_lookahead(tmp_path: Path) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0] * 20)
    announcement = date(2024, 1, 2)
    delisting = date(2024, 1, 18)
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": [announcement],
            "symbol": ["2330"],
            "_twpub_force_cover_lead_sessions": [10.0],
            "_twpub_force_cover_anchor_ordinal": [float(delisting.toordinal())],
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
    forced_rows = np.flatnonzero(panel.force_short_cover_mask[:, symbol_idx])
    assert forced_rows.tolist() == [6]
    assert panel.dates[forced_rows[0]] == np.datetime64("2024-01-08")


def test_relative_cover_cancellation_is_prospective_at_actual_session_deadline(
    tmp_path: Path,
) -> None:
    for symbol in ("1111", "2222"):
        _write_symbol(tmp_path / f"{symbol}_features.parquet", [100.0] * 20)
    anchor = date(2024, 1, 18)
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 2)],
            "symbol": ["1111", "2222"],
            "_twpub_force_cover_lead_sessions": [10.0, 10.0],
            "_twpub_force_cover_anchor_ordinal": [
                float(anchor.toordinal()),
                float(anchor.toordinal()),
            ],
            # 1111 is cancelled before its Jan-08 deadline; 2222 is cancelled
            # after that deadline and therefore cannot undo an executed cover.
            "_twpub_force_cover_cancel_ordinal": [
                float(date(2024, 1, 5).toordinal()),
                float(date(2024, 1, 10).toordinal()),
            ],
        }
    ).write_parquet(external_path)

    panel = build_panel(
        tmp_path,
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
    )

    early_idx = panel.symbols.index("1111")
    late_idx = panel.symbols.index("2222")
    assert np.flatnonzero(panel.force_short_cover_mask[:, early_idx]).tolist() == []
    assert np.flatnonzero(panel.force_short_cover_mask[:, late_idx]).tolist() == [6]


def test_relative_cover_moves_forward_when_all_predeadline_sessions_are_blocked(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0] * 20)
    announcement = date(2024, 1, 2)
    delisting = date(2024, 1, 18)
    blocked_dates = [date(2024, 1, day) for day in range(3, 9)]
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": [announcement, *blocked_dates],
            "symbol": ["2330"] * (1 + len(blocked_dates)),
            "_twpub_force_cover_lead_sessions": [10.0, *([None] * len(blocked_dates))],
            # Legacy column remains readable for already-built public parquets.
            "_twpub_force_cover_delisting_ordinal": [
                float(delisting.toordinal()),
                *([None] * len(blocked_dates)),
            ],
            "_twpub_trading_halt": [None, *([1.0] * len(blocked_dates))],
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
    forced_rows = np.flatnonzero(panel.force_short_cover_mask[:, symbol_idx])

    assert forced_rows.tolist() == [7]
    assert panel.dates[forced_rows[0]] == np.datetime64("2024-01-09")
    assert panel.dates[forced_rows[0]] < np.datetime64(delisting)


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


def test_snapshot_date_never_precedes_archived_vintage() -> None:
    frame = pl.DataFrame(
        {
            "出表日期": ["2024-05-10", "2024-07-12"],
            "_as_of_date": ["2024-07-11", "2024-07-11"],
        }
    )

    dates = frame.select(
        _snapshot_date_expr(set(frame.columns)).alias("available_date")
    ).get_column("available_date")

    assert dates.to_list() == [date(2024, 7, 11), date(2024, 7, 12)]
