from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from polars.testing import assert_frame_equal
import pytest

import stockagent.data.panel as panel_module
from scripts.audit_tw_public_data_layer import (
    audit_feature_availability_contract,
    audit_feature_build_receipt,
    audit_feature_lineage_registry,
    audit_release_vintage_contract,
)
from scripts.build_tw_public_training_features import _writer_lock_path
from stockagent.config import load_config
from stockagent.data.panel import build_panel, build_tail_panel
from stockagent.data.tw_public_features import (
    DEFAULT_MARKET_SYMBOL,
    FEATURE_COLUMNS,
    RULE_COLUMNS,
    _build_institutional_features,
    _build_margin_features,
    _build_official_ohlcv_features,
    _build_tdcc_features,
    _build_material_info_features,
    _build_cbc_monthly_macro_features,
    _build_cbc_overnight_rate_features,
    _build_dgbas_macro_features,
    _merge_feature_frames,
    _finalize_feature_frame,
    _map_available_dates_to_sessions,
    _next_exchange_session_lookup,
    _material_info_available_date_expr,
    _build_twse_market_index_features,
    _snapshot_date_expr,
    _snapshot_not_before_expr,
    _source_content_receipts,
    build_tw_public_training_features,
)


def test_large_session_aligned_frame_skips_asof_without_skipping_holiday_mapping(
    tmp_path: Path,
) -> None:
    sessions = pl.DataFrame({
        "_session_date": [date(2024, 1, 2), date(2024, 1, 3)],
    })
    aligned = pl.DataFrame({
        "date": [date(2024, 1, 2)] * 100_000,
        "symbol": ["2330"] * 100_000,
        "value": list(range(100_000)),
    })
    assert _map_available_dates_to_sessions(aligned, tmp_path, sessions=sessions) is aligned

    with_holiday = pl.concat([
        aligned,
        pl.DataFrame({
            "date": [date(2024, 1, 1)], "symbol": ["0050"], "value": [100_000],
        }),
    ])
    mapped = _map_available_dates_to_sessions(with_holiday, tmp_path, sessions=sessions)
    assert mapped.height == with_holiday.height
    assert mapped.filter(pl.col("value") == 100_000).select("date").item() == date(2024, 1, 2)


def test_unique_key_merge_matches_redundant_final_group_by() -> None:
    frames = [
        pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["2330", "2330", "0050"],
            "twpub_official_trading_volume_raw": [1.0, 2.0, None],
        }),
        pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["2330", "0050"],
            "twpub_pe_raw": [20.0, 10.0],
            "twpub_official_trading_volume_raw": [99.0, 88.0],
        }),
        pl.DataFrame({
            "date": [date(2024, 1, 3)], "symbol": ["2330"],
            "twpub_pb_raw": [5.0],
        }),
    ]
    cleaned = [_finalize_feature_frame(frame) for frame in frames]
    old = cleaned[0]
    for frame in cleaned[1:]:
        old = old.join(frame, on=["date", "symbol"], how="full", coalesce=True)
    old = _finalize_feature_frame(old)
    new = _merge_feature_frames(frames)
    assert_frame_equal(
        new.sort(["date", "symbol"]), old.sort(["date", "symbol"]),
        check_row_order=True, check_column_order=True,
    )


def test_large_feature_frame_skips_groupby_only_when_keys_are_unique() -> None:
    unique = pl.DataFrame({
        "date": [date(2024, 1, 2)] * 100_000,
        "symbol": [f"{index:06d}" for index in range(100_000)],
        "twpub_pe_raw": [float(index) for index in range(100_000)],
    })
    normalized = _finalize_feature_frame(unique)
    assert normalized.height == unique.height
    assert_frame_equal(normalized, unique, check_row_order=True)

    duplicated = pl.concat([
        unique,
        pl.DataFrame({
            "date": [date(2024, 1, 2)], "symbol": ["000000"],
            "twpub_pe_raw": [999.0],
        }),
    ])
    result = _finalize_feature_frame(duplicated)
    assert result.height == unique.height
    assert result.filter(pl.col("symbol") == "000000")["twpub_pe_raw"].item() == 999.0


def test_sparse_duplicate_feature_keys_keep_last_non_null_per_column() -> None:
    frame = pl.DataFrame({
        "date": [date(2024, 1, 2)] * 100_003,
        "symbol": [f"{index:06d}" for index in range(100_000)]
        + ["000000", "000000", "000001"],
        "twpub_pe_raw": [float(index) for index in range(100_000)]
        + [None, 321.0, None],
        "twpub_pb_raw": [float(index) for index in range(100_000)]
        + [123.0, None, None],
    })
    expected = frame.group_by(["date", "symbol"]).agg([
        pl.col(name).drop_nulls().last().alias(name)
        for name in ("twpub_pe_raw", "twpub_pb_raw")
    ])
    assert_frame_equal(
        _finalize_feature_frame(frame).sort(["date", "symbol"]),
        expected.sort(["date", "symbol"]),
        check_row_order=True,
        check_column_order=True,
    )


def test_large_feature_join_preserves_extra_keys_and_first_column_wins() -> None:
    frames = [
        pl.DataFrame({
            "date": [date(2024, 1, 2)] * 100_000,
            "symbol": [f"{index:06d}" for index in range(100_000)],
            "twpub_pe_raw": [float(index) for index in range(100_000)],
        }),
        pl.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["000000", "999999"],
            "twpub_pe_raw": [999.0, 3.0],
            "twpub_pb_raw": [1.0, 2.0],
        }),
        pl.DataFrame({
            "date": [date(2024, 1, 4)],
            "symbol": ["888888"],
            "twpub_dividend_yield": [0.04],
        }),
    ]
    expected = _finalize_feature_frame(frames[0])
    for frame in frames[1:]:
        expected = expected.join(
            _finalize_feature_frame(frame),
            on=["date", "symbol"], how="full", coalesce=True,
        )
    expected = expected.select(
        ["date", "symbol", *[name for name in expected.columns if name in ("twpub_pe_raw", "twpub_pb_raw", "twpub_dividend_yield")]]
    )
    stages: dict[str, float] = {}
    result = _merge_feature_frames(frames, stage_seconds=stages)
    assert "join_keys" in stages
    assert_frame_equal(
        result.sort(["date", "symbol"]),
        expected.sort(["date", "symbol"]),
        check_row_order=True, check_column_order=True,
    )


def test_feature_writer_lock_uses_resolved_output_identity(tmp_path: Path) -> None:
    live = tmp_path / "live"
    live.mkdir()
    alias = tmp_path / "data_tw_public"
    alias.symlink_to(live, target_is_directory=True)
    assert _writer_lock_path(alias / "features.parquet") == _writer_lock_path(
        live / "features.parquet"
    )


def test_supplemental_cbc_receipt_keeps_relative_path(tmp_path: Path) -> None:
    supplemental = tmp_path / "supplemental"
    supplemental.mkdir()
    pq.write_table(pa.table({"rate": [1.0]}), supplemental / "cbc_overnight_official_pages.parquet")

    receipts = _source_content_receipts(tmp_path)

    assert receipts[-1]["name"] == "supplemental/cbc_overnight_official_pages.parquet"
    assert receipts[-1]["sha256"]


def test_background_catalog_updates_do_not_invalidate_training_feature_sources(
    tmp_path: Path,
) -> None:
    source = tmp_path / "twse_daily_ohlcv.parquet"
    gcis = tmp_path / "gcis_open_data_catalog.parquet"
    fsc = tmp_path / "fsc_open_data_catalog.parquet"
    source.write_bytes(b"official-day-one")
    gcis.write_bytes(b"catalog-one")
    fsc.write_bytes(b"catalog-one")
    original = _source_content_receipts(tmp_path)
    assert [item["name"] for item in original] == [source.name]

    gcis.write_bytes(b"catalog-two")
    fsc.write_bytes(b"catalog-two")
    assert _source_content_receipts(tmp_path) == original

    source.write_bytes(b"official-day-two")
    assert _source_content_receipts(tmp_path) != original


def test_raw_preopen_feature_selection_has_one_causal_availability_class() -> None:
    config = load_config("configs/markets/tw_public_preopen_raw_v1.yaml")
    summary, findings = audit_feature_availability_contract(config)
    assert len(config.data.feature_include) == 22
    assert summary["unclassified_active_features"] == []
    assert summary["classification_overlaps"] == []
    assert summary["missing_required_shifts"] == []
    assert summary["unexpected_panel_shifts"] == []
    assert findings == []
    assert audit_feature_lineage_registry(config) == []


def test_long_history_raw_preopen_excludes_later_starting_families() -> None:
    config = load_config("configs/markets/tw_public_preopen_raw_long_history_v1.yaml")
    summary, findings = audit_feature_availability_contract(config)
    assert len(config.data.feature_include) == 15
    assert config.walk_forward.expected_first_year == 2005
    assert all(not name.startswith(("twpub_pe_", "twpub_pb_", "twpub_foreign_"))
               for name in config.data.feature_include)
    assert summary["missing_required_shifts"] == []
    assert summary["unclassified_active_features"] == []
    assert findings == []
    assert audit_feature_lineage_registry(config) == []


def test_2014_original_value_contract_accepts_full_horizon() -> None:
    config = load_config("configs/markets/tw_public_preopen_raw_2014_v1.yaml")
    summary, findings = audit_feature_availability_contract(config)
    assert config.walk_forward.expected_first_year == 2014
    assert len(config.data.feature_include) == 27
    assert len(summary["release_vintage_active_features"]) == 6
    assert not findings
    assert not audit_feature_lineage_registry(config)


def test_cbc_missing_2000_release_does_not_block_2014_horizon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.audit_tw_official_release_archives as archive_audit

    state = tmp_path / "state"
    state.mkdir()
    (state / "cbc_money_release_vintages.json").write_text(json.dumps({
        "earliest_period": "1999-12", "latest_period": "2026-07",
        "missing_periods": ["2000-06"], "value_history_complete": False,
    }))
    monkeypatch.setattr(archive_audit, "audit_one", lambda root, name: {
        "integrity_ok": True, "coverage_complete": True,
        "saved_releases": 321, "registered_releases": 321, "errors": [],
    })
    config = load_config("configs/markets/tw_public_preopen_raw_2014_v1.yaml")
    config.data.feature_include = ["twpub_cbc_m1b_yoy_pct_raw"]
    assert audit_release_vintage_contract(tmp_path, config) == []
    (state / "cbc_money_release_vintages.json").write_text(json.dumps({
        "earliest_period": "1999-12", "latest_period": "2026-07",
        "missing_periods": ["2019-06"], "value_history_complete": False,
    }))
    assert any(f.item == "cbc_money_release_vintages"
               for f in audit_release_vintage_contract(tmp_path, config))


def test_dgbas_original_value_gap_blocks_2014_horizon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.audit_tw_official_release_archives as archive_audit

    monkeypatch.setattr(archive_audit, "audit_one", lambda root, name: {
        "integrity_ok": True, "coverage_complete": True,
        "saved_releases": 1, "registered_releases": 1, "errors": [],
    })
    pl.DataFrame({
        "source": ["cpi"], "period": ["2013-01"],
        "metric": ["cpi_yoy_pct"], "value_pct": [1.0],
        "value_evidence": ["official_release_headline"],
        "published_on": ["2013-02-06"], "html_sha256": ["original"],
    }).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    config = load_config("configs/markets/tw_public_preopen_raw_2014_v1.yaml")
    config.data.feature_include = ["twpub_dgbas_cpi_yoy_pct_raw"]
    assert any(f.item == "dgbas_release_vintages"
               for f in audit_release_vintage_contract(tmp_path, config))


def test_release_feature_receipt_ignores_html_rewrap_but_tracks_value_and_clock(
    tmp_path: Path,
) -> None:
    path = tmp_path / "dgbas_release_vintages.parquet"
    row = {
        "source": ["gdp"], "period": ["2013-Q1"], "published_on": ["2013-04-30"],
        "release_id": ["1"], "release_kind": ["article"],
        "metric": ["gdp_yoy_pct"], "value_pct": [1.54],
        "value_evidence": ["official_release_headline"],
        "published_time_precision": ["official_document_time"],
        "published_clock_taipei": ["08:30:00"],
        "html_sha256": ["first"], "observed_at_utc": ["2026-09-17T01:00:00Z"],
    }
    pl.DataFrame(row).write_parquet(path)
    first = _source_content_receipts(tmp_path)
    row["html_sha256"] = ["second"]
    row["observed_at_utc"] = ["2026-09-17T02:00:00Z"]
    pl.DataFrame(row).write_parquet(path)
    assert _source_content_receipts(tmp_path) == first
    row["value_pct"] = [1.55]
    pl.DataFrame(row).write_parquet(path)
    assert _source_content_receipts(tmp_path) != first
    row["value_pct"] = [1.54]
    row["published_clock_taipei"] = ["16:00:00"]
    pl.DataFrame(row).write_parquet(path)
    assert _source_content_receipts(tmp_path) != first


def test_cbc_release_feature_receipt_ignores_html_rewrap_only(tmp_path: Path) -> None:
    path = tmp_path / "cbc_fx_reserve_release_vintages.parquet"
    row = {
        "period": ["2024-08"], "published_on": ["2024-09-06"],
        "metric": ["fx_reserves_usd_100m"], "value": [6019.04],
        "value_evidence": ["original_press_release_text"],
        "html_sha256": ["first"], "observed_at_utc": ["2026-09-17T01:00:00Z"],
    }
    pl.DataFrame(row).write_parquet(path)
    first = _source_content_receipts(tmp_path)
    row["html_sha256"] = ["second"]
    pl.DataFrame(row).write_parquet(path)
    assert _source_content_receipts(tmp_path) == first
    row["value"] = [6019.05]
    pl.DataFrame(row).write_parquet(path)
    assert _source_content_receipts(tmp_path) != first


def test_cbc_current_overnight_page_waits_until_first_observed_session(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-05-10", "2024-05-13", "2024-05-14"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    supplemental = tmp_path / "supplemental"
    supplemental.mkdir()
    pl.DataFrame([
        {"subject_date": "2024-05-10", "rate_pct": 0.8, "status": "ok",
         "observed_at_utc": "2024-05-13T00:00:00+00:00"},
        {"subject_date": "2024-05-13", "rate_pct": 0.9, "status": "ok",
         "observed_at_utc": "2024-05-13T08:00:00+00:00"},
    ]).write_parquet(supplemental / "cbc_overnight_official_pages.parquet")
    result = _build_cbc_overnight_rate_features(tmp_path, market_symbol="__MARKET__")
    assert result.get_column("date").to_list() == [date(2024, 5, 13), date(2024, 5, 14)]
    assert result.get_column("twpub_cbc_overnight_rate").to_list() == pytest.approx([0.008, 0.009])


def test_tdcc_canonical_mirror_supersedes_overlap_and_direct_keeps_history(
    tmp_path: Path,
) -> None:
    pl.DataFrame({"date": ["2024-01-05", "2024-01-08", "2024-01-12", "2024-01-15"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "\ufeff資料日期": ["20240101", "20240108"],
            "證券代號": ["2330", "2330"],
            "持股分級": ["1", "1"],
            "人數": ["5", "10"],
            "占集保庫存數比例%": ["5.0", "10.0"],
            "_downloaded_at_utc": ["2024-01-05T06:00:00+00:00", "2024-01-12T06:00:00+00:00"],
        }
    ).write_parquet(tmp_path / "tdcc_shareholding_distribution.parquet")
    pl.DataFrame(
        {
            "資料日期": ["20240108"],
            "證券代號": ["2330"],
            "持股分級": ["1"],
            "人數": ["20"],
            "占集保庫存數比例%": ["20.0"],
            "_downloaded_at_utc": ["2024-01-12T07:00:00+00:00"],
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
    assert margin_source["twpub_margin_balance_lots_raw"] is None
    assert margin_source["_twpub_margin_short_evidence_next_session"] == 1.0
    assert margin_available["twpub_margin_balance_lots_raw"] == 120.0
    assert margin_available["twpub_short_balance_lots_raw"] == 12.0
    assert margin_available["twpub_margin_buy_lots_raw"] == 30.0
    assert margin_available["twpub_short_sell_lots_raw"] == 4.0
    assert margin_available["twpub_margin_balance_log"] == pytest.approx(
        np.log1p(120)
    )
    assert margin_available["_twpub_margin_short_evidence_next_session"] is None
    assert institutional.get_column("date").to_list() == [date(2024, 1, 8)]
    assert institutional.row(0, named=True)[
        "twpub_investment_trust_net_buy_flow"
    ] == pytest.approx(np.arcsinh(200 / 1000.0))
    assert institutional.row(0, named=True)["twpub_dealer_net_buy_shares_raw"] == -50.0
    assert institutional.row(0, named=True)["twpub_foreign_net_buy_shares_raw"] == 1000.0


def test_twse_legacy_foreign_header_is_preserved_in_raw_and_engineered_history(
    tmp_path: Path,
) -> None:
    pl.DataFrame({"date": ["2017-12-28", "2017-12-29"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "date": ["2017-12-28"],
            "證券代號": ["2330"],
            "外資買賣超股數": ["-1,250"],
            "投信買賣超股數": ["0"],
            "自營商買賣超股數": ["0"],
            "三大法人買賣超股數": ["-1,250"],
        }
    ).write_parquet(tmp_path / "twse_institutional_trades.parquet")
    row = _build_institutional_features(tmp_path).row(0, named=True)
    assert row["date"] == date(2017, 12, 29)
    assert row["twpub_foreign_net_buy_shares_raw"] == -1250.0
    assert row["twpub_foreign_net_buy_flow"] == pytest.approx(
        -np.arcsinh(1.25)
    )


def test_next_session_can_be_proved_before_its_index_close(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-01-05"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    for market in ("twse", "tpex"):
        pl.DataFrame({"date": ["2024-01-08"], "證券代號": ["2330"]}).write_parquet(
            tmp_path / f"{market}_day_trade_eligibility.parquet"
        )
    lookup = _next_exchange_session_lookup(tmp_path)
    assert lookup.to_dicts() == [{
        "_source_date": date(2024, 1, 5),
        "_available_date": date(2024, 1, 8),
    }]


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
    start = np.datetime64("2024-01-02", "D")
    dates = np.arange(start, start + np.timedelta64(rows, "D"))
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


def test_raw_preopen_panel_shifts_daily_values_once_but_not_pre_shifted_chip(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["2330", "2330"],
            "twpub_pe_raw": [20.5, None],
            # This is the source-day 01-02 chip value already relabelled 01-03.
            "twpub_margin_balance_lots_raw": [None, 120.0],
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
        feature_include=["open_raw", "twpub_pe_raw", "twpub_margin_balance_lots_raw"],
        feature_shift_next_session=["open_raw", "twpub_pe_raw"],
    )
    assert panel.feature_names == ["open_raw", "twpub_pe_raw", "twpub_margin_balance_lots_raw"]
    np.testing.assert_allclose(panel.features[0, 0], [0.0, 0.0, 0.0])
    np.testing.assert_allclose(panel.features[1, 0], [99.0, 20.5, 120.0])


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
            "日期": ["20240101", "20240102"],
            "NTD/USD": ["31.0", "31.31"],
            "_downloaded_at_utc": [
                "2024-01-02T00:59:00+00:00",
                "2024-01-03T00:59:00+00:00",
            ],
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
    first_stock = stock.filter(pl.col("date") == date(2024, 1, 2)).row(0, named=True)
    assert first_stock["twpub_pe_raw"] == 20.5
    assert first_stock["twpub_pb_raw"] == 5.2
    assert first_stock["twpub_dividend_yield_pct_raw"] == 2.5
    assert stock.filter(pl.col("date") == date(2024, 1, 3)).row(0, named=True)[
        "twpub_margin_balance_log"
    ] is not None
    assert stock.filter(pl.col("date") == date(2024, 1, 3)).row(0, named=True)[
        "twpub_margin_balance_lots_raw"
    ] == 1050.0
    market = out.filter(pl.col("symbol") == DEFAULT_MARKET_SYMBOL).sort("date")
    assert market.height == 2
    assert market["twpub_twse_taiex_raw"].to_list() == [100.5, 101.5]
    assert market["twpub_usdtwd_raw"][1] == 31.31
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


def test_incremental_feature_tail_matches_full_rebuild(tmp_path: Path) -> None:
    input_dir = tmp_path / "tw_public"
    input_dir.mkdir()
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 11.0, 12.0])

    def write_source(dates: list[str], closes: list[str]) -> None:
        pl.DataFrame(
            {
                "證券代號": ["2330"] * len(dates),
                "收盤價": closes,
                "最高價": closes,
                "最低價": closes,
                "成交股數": ["1000"] * len(dates),
                "成交金額": ["100000"] * len(dates),
                "成交筆數": ["10"] * len(dates),
                "date": dates,
            }
        ).write_parquet(input_dir / "twse_daily_ohlcv.parquet")

    incremental_path = tmp_path / "incremental.parquet"
    write_source(["2024-01-02", "2024-01-03"], ["100", "101"])
    build_tw_public_training_features(
        input_dir,
        incremental_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )

    write_source(
        ["2024-01-02", "2024-01-03", "2024-01-04"],
        ["100", "101", "103"],
    )
    incremental = build_tw_public_training_features(
        input_dir,
        incremental_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 4),
        incremental_start_date=date(2024, 1, 3),
    )
    full_path = tmp_path / "full.parquet"
    full = build_tw_public_training_features(
        input_dir,
        full_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 4),
    )

    # A changed source parquet could contain corrections before the tail.
    # Without a per-partition proof, a full rebuild is required.
    assert incremental.build_mode == "full"
    assert incremental.incremental_start_date is None
    assert incremental.reused_rows == 0
    assert incremental.incremental_fallback_reason == "base_contract_not_verified"
    assert incremental.stage_elapsed_seconds["total_before_summary"] >= 0
    assert {
        "source_proof",
        "stock_build",
        "market_build",
        "output_assembly",
        "parquet_write_and_proof",
        "total_before_summary",
        "stock_official_ohlcv",
        "stock_day_trade_rule",
        "market_twse_index",
        "market_taifex_settlement",
    } <= set(incremental.stage_elapsed_seconds)
    summary = json.loads(incremental_path.with_suffix(".summary.json").read_text())
    assert summary["incremental_fallback_reason"] == "base_contract_not_verified"
    assert summary["stage_elapsed_seconds"] == incremental.stage_elapsed_seconds
    assert incremental.rows == full.rows
    assert_frame_equal(
        pl.read_parquet(incremental_path),
        pl.read_parquet(full_path),
        check_row_order=True,
        check_column_order=True,
    )
    prior_identity = incremental_path.stat()
    prior_summary = incremental_path.with_suffix(".summary.json").stat()
    unchanged = build_tw_public_training_features(
        input_dir,
        incremental_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 4),
        incremental_start_date=date(2024, 1, 3),
    )
    assert unchanged.build_mode == "unchanged_verified"
    assert unchanged.reused_rows == incremental.rows
    assert unchanged.stage_elapsed_seconds["total_before_summary"] >= 0
    assert incremental_path.stat().st_ino == prior_identity.st_ino
    assert incremental_path.stat().st_mtime_ns == prior_identity.st_mtime_ns
    assert incremental_path.with_suffix(".summary.json").stat().st_mtime_ns == prior_summary.st_mtime_ns

    # The same bytes with a different causal-lag policy are not equivalent.
    rebuilt = build_tw_public_training_features(
        input_dir,
        incremental_path,
        symbols_root=symbols_root,
        end_date=date(2024, 1, 4),
        allow_daily_publication_lag=True,
    )
    assert rebuilt.build_mode == "full"
    assert json.loads(incremental_path.with_suffix(".summary.json").read_text())[
        "allow_daily_publication_lag"
    ] is True


def test_taifex_futures_only_change_reuses_verified_stock_rows(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "tw_public"
    input_dir.mkdir()
    symbols_root = tmp_path / "symbols"
    symbols_root.mkdir()
    _write_symbol(symbols_root / "2330_features.parquet", [10.0, 11.0])
    stock_path = input_dir / "twse_daily_ohlcv.parquet"
    stock = pl.DataFrame({
        "證券代號": ["2330", "2330"],
        "收盤價": ["100", "101"], "最高價": ["101", "102"],
        "最低價": ["99", "100"], "成交股數": ["1000", "1100"],
        "成交金額": ["100000", "111100"], "成交筆數": ["10", "11"],
        "date": ["2024-01-02", "2024-01-03"],
    })
    stock.write_parquet(stock_path)
    futures_path = input_dir / "taifex_daily_futures.parquet"

    def write_futures(last_settlement: str) -> None:
        pl.DataFrame({
            "Date": ["2024/01/02", "2024/01/03"],
            "Contract": ["TX", "TX"],
            "TradingSession": ["一般", "一般"],
            "ContractMonth(Week)": ["202401", "202401"],
            "Volume": ["100", "110"],
            "OpenInterest": ["500", "510"],
            "SettlementPrice": ["18000", last_settlement],
        }).write_parquet(futures_path)

    write_futures("18010")
    settlement_path = input_dir / "taifex_final_settlement_price.parquet"

    def write_settlement(last_price: str) -> None:
        pl.DataFrame({
            "商品代號": ["TX", "TX"],
            "最後結算日": ["2024-01-02", "2024-01-03"],
            "最後結算價": ["18000", last_price],
        }).write_parquet(settlement_path)

    write_settlement("18010")
    output_path = tmp_path / "features.parquet"
    build_tw_public_training_features(
        input_dir, output_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )
    write_futures("18020")
    reused = build_tw_public_training_features(
        input_dir, output_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
        incremental_start_date=date(2024, 1, 3),
    )
    assert reused.build_mode == "market_only_rebuild"
    assert reused.reused_rows == reused.stock_rows == 2
    assert "stock_official_ohlcv" not in reused.stage_elapsed_seconds
    full_path = tmp_path / "full.parquet"
    build_tw_public_training_features(
        input_dir, full_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )
    assert_frame_equal(
        pl.read_parquet(output_path), pl.read_parquet(full_path),
        check_row_order=True, check_column_order=True,
    )

    write_settlement("18030")
    settlement_only = build_tw_public_training_features(
        input_dir, output_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )
    assert settlement_only.build_mode == "market_only_rebuild"
    build_tw_public_training_features(
        input_dir, full_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )
    assert_frame_equal(
        pl.read_parquet(output_path), pl.read_parquet(full_path),
        check_row_order=True, check_column_order=True,
    )

    stock.with_columns(pl.lit("102").alias("收盤價")).write_parquet(stock_path)
    revised_stock = build_tw_public_training_features(
        input_dir, output_path, symbols_root=symbols_root,
        end_date=date(2024, 1, 3),
    )
    assert revised_stock.build_mode == "full"


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
    assert first["twpub_official_trading_volume_raw"] == 240
    assert first["twpub_official_trading_value_raw"] == 6408
    assert first["twpub_official_trades_raw"] == 1
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
    assert output.get_column("twpub_twse_taiex_raw").to_list() == [100.0, 101.0, 102.0]
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


def test_panel_cache_reuses_canonical_source_behind_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(source, target_is_directory=True)
    _write_symbol(source / "2330_features.parquet", [100.0, 101.0, 102.0])
    pl.DataFrame({
        "date": ["2024-01-02"],
        "symbol": [DEFAULT_MARKET_SYMBOL],
        "twpub_usdtwd_logret_1d": [0.01],
    }).write_parquet(source / "external.parquet")
    kwargs = {
        "benchmark_name": "2330", "panel_backend": "pyarrow",
        "panel_load_workers": 0,
        "feature_include": ["close_logret_1d", "twpub_usdtwd_logret_1d"],
    }
    first = build_panel(
        source, external_feature_path=source / "external.parquet", **kwargs
    )
    second = build_panel(
        alias, external_feature_path=alias / "external.parquet", **kwargs
    )
    np.testing.assert_array_equal(first.features, second.features)
    assert len(list((source / "panel_cache_v2" / "generations").iterdir())) == 1


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


def test_sparse_xbrl_state_has_availability_channel_and_daily_missing_stays_missing(
    tmp_path: Path,
) -> None:
    _write_symbol(tmp_path / "2330_features.parquet", [100.0, 101.0, 102.0])
    external_path = tmp_path / "external.parquet"
    pl.DataFrame({
        "date": ["2024-01-03"],
        "symbol": ["2330"],
        "twpub_xbrl_assets_twd_raw": [0.0],
        "twpub_pe_raw": [0.0],
    }).write_parquet(external_path)
    kwargs = dict(
        benchmark_name="universe_average_return",
        tradable_mode="tradable",
        trading_volume_policy="required",
        panel_backend="pyarrow",
        panel_load_workers=0,
        external_feature_path=external_path,
        feature_include=["twpub_xbrl_assets_twd_raw", "twpub_pe_raw"],
        feature_availability_indicators=["twpub_xbrl_*", "twpub_pe_raw"],
        feature_shift_next_session=["twpub_pe_raw"],
    )
    panel = build_panel(tmp_path, **kwargs)
    asset = panel.feature_names.index("twpub_xbrl_assets_twd_raw")
    asset_available = panel.feature_names.index("twpub_xbrl_assets_twd_raw__available")
    pe_available = panel.feature_names.index("twpub_pe_raw__available")
    assert np.all(panel.features[:, 0, asset] == 0.0)
    assert panel.features[:, 0, asset_available].tolist() == [0.0, 1.0, 1.0]
    assert panel.features[:, 0, pe_available].tolist() == [0.0, 0.0, 1.0]
    cached = build_panel(tmp_path, **kwargs)
    assert cached.feature_names == panel.feature_names
    assert np.array_equal(cached.features, panel.features)


def test_retired_tifrs_state_expires_without_changing_m2_carry() -> None:
    dates = np.array(["2018-01-01", "2018-06-01", "2019-03-01"], dtype="datetime64[D]")
    values = np.array([
        [100.0, 10.0], [np.nan, np.nan], [np.nan, np.nan],
    ])
    names = ["twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw", "twpub_cbc_m2_raw"]
    panel_module._forward_fill_point_in_time_features(
        values, names, dates.astype(np.int64),
    )
    assert values[1].tolist() == [100.0, 10.0]
    assert np.isnan(values[2, 0])
    assert values[2, 1] == 10.0
    seeded = np.full((2, 2), np.nan)
    seed_panel_dates = np.array(["2019-01-01", "2020-03-01"], dtype="datetime64[D]")
    seed_days = panel_module._seed_point_in_time_features_before_panel_start(
        seeded, names,
        seed_panel_dates,
        np.array(["2018-12-31"], dtype="datetime64[D]"),
        np.array([[100.0, 10.0]]),
    )
    assert seeded[0, 0] == 100.0
    assert seeded[0, 1] == 10.0
    assert seed_days[0] == np.datetime64("2018-12-31", "D").astype(np.int64)
    panel_module._forward_fill_point_in_time_features(
        seeded, names, seed_panel_dates.astype(np.int64), seed_days,
    )
    assert np.isnan(seeded[1, 0])
    assert seeded[1, 1] == 10.0
    stale_seed = np.full((2, 2), np.nan)
    panel_module._seed_point_in_time_features_before_panel_start(
        stale_seed, names,
        np.array(["2019-03-01", "2019-03-04"], dtype="datetime64[D]"),
        np.array(["2018-01-01"], dtype="datetime64[D]"),
        np.array([[100.0, 10.0]]),
    )
    assert np.isnan(stale_seed[0, 0])
    assert stale_seed[0, 1] == 10.0


def test_tifrs_carry_version_only_changes_relevant_panel_cache(tmp_path: Path) -> None:
    source = tmp_path / "research.parquet"
    pl.DataFrame({
        "twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw": [1.0],
        "twpub_cbc_m2_raw": [2.0],
    }).write_parquet(source)
    assert panel_module._tifrs_carry_cache_contract(
        source, ("twpub_xbrl_tifrs_*",), (),
    ) == "tifrs_carry_v2_days=400|"
    assert panel_module._tifrs_carry_cache_contract(
        source, ("twpub_cbc_m2_raw",), (),
    ) == ""


def test_tifrs_expiry_updates_model_availability_mask(tmp_path: Path) -> None:
    dates = np.array(["2018-01-02", "2018-06-01", "2019-03-01"], dtype="datetime64[D]")
    close = np.array([100.0, 101.0, 102.0])
    pq.write_table(pa.table({
        "date": pa.array(dates),
        "open": pa.array(close), "max": pa.array(close),
        "min": pa.array(close), "close": pa.array(close),
        "adjclose": pa.array(close),
        "Trading_Volume": pa.array(np.full(3, 1000.0)),
    }), tmp_path / "2330_features.parquet")
    external = tmp_path / "external.parquet"
    old_name = "twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw"
    pl.DataFrame({
        "date": [date(2017, 12, 31)], "symbol": ["2330"],
        old_name: [100.0], "twpub_cbc_m2_raw": [10.0],
    }).write_parquet(external)
    panel = build_panel(
        tmp_path, benchmark_name="universe_average_return",
        tradable_mode="tradable", trading_volume_policy="required",
        panel_backend="pyarrow", panel_load_workers=0,
        external_feature_path=external,
        feature_include=[old_name, "twpub_cbc_m2_raw"],
        feature_availability_indicators=[old_name, "twpub_cbc_m2_raw"],
    )
    old_available = panel.feature_names.index(f"{old_name}__available")
    m2_available = panel.feature_names.index("twpub_cbc_m2_raw__available")
    assert panel.features[:, 0, old_available].tolist() == [1.0, 1.0, 0.0]
    assert panel.features[:, 0, m2_available].tolist() == [1.0, 1.0, 1.0]


def test_snapshot_date_never_precedes_archived_vintage() -> None:
    frame = pl.DataFrame(
        {
            "出表日期": ["2024-05-10", "2024-07-12"],
            "_as_of_date": ["2024-07-11", "2024-07-11"],
            "_downloaded_at_utc": ["2024-07-11T00:59:59+00:00", "2024-07-11T00:59:59+00:00"],
        }
    )

    dates = frame.select(
        _snapshot_date_expr(set(frame.columns)).alias("available_date")
    ).get_column("available_date")

    assert dates.to_list() == [date(2024, 7, 11), date(2024, 7, 12)]


def test_snapshot_response_at_or_after_open_is_next_session() -> None:
    frame = pl.DataFrame(
        {
            "出表日期": ["2024-01-05"] * 3,
            "_as_of_date": ["2024-01-05"] * 3,
            "_downloaded_at_utc": [
                "2024-01-05T00:59:59+00:00",
                "2024-01-05T01:00:00+00:00",
                "2024-01-05T01:00:01+00:00",
            ],
        }
    )
    assert frame.select(_snapshot_date_expr(set(frame.columns)).alias("date"))["date"].to_list() == [
        date(2024, 1, 5), date(2024, 1, 6), date(2024, 1, 6)
    ]


def test_event_effective_date_cannot_precede_observed_snapshot() -> None:
    frame = pl.DataFrame({
        "出表日期": ["2024-01-05"],
        "_downloaded_at_utc": ["2024-01-05T01:05:00+00:00"],
        "_as_of_date": ["2024-01-05"],
    })
    assert frame.select(
        _snapshot_not_before_expr(
            set(frame.columns), pl.lit(date(2024, 1, 4))
        ).alias("date")
    )["date"].to_list() == [date(2024, 1, 6)]


def test_material_info_speaking_clock_and_weekend_session(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-01-05", "2024-01-08"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "發言日期": ["1130105"] * 3,
            "發言時間": ["085959", "090000", "170000"],
            "事實發生日": ["1130104"] * 3,
            "公司代號": ["2330"] * 3,
            "符合條款": ["1"] * 3,
        }
    ).write_parquet(tmp_path / "twse_listed_material_info.parquet")
    result = _build_material_info_features(tmp_path).sort("date")
    assert result["date"].to_list() == [date(2024, 1, 5), date(2024, 1, 8)]
    assert result["twpub_material_event_count_log"].to_list() == pytest.approx(
        [np.log(2), np.log(3)]
    )


def test_material_info_colon_clock_and_invalid_clock_fail_closed() -> None:
    frame = pl.DataFrame({
        "發言日期": ["1130105"] * 3,
        "發言時間": ["08:59:59", "09:00", "unknown"],
    })
    assert frame.select(
        _material_info_available_date_expr(set(frame.columns)).alias("date")
    )["date"].to_list() == [date(2024, 1, 5), date(2024, 1, 6), None]


def test_monthly_period_is_not_a_release_receipt(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-02-07", "2024-02-08"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "日期": ["2024.01"] * 3, "金額": ["100", "110", "120"],
            "_downloaded_at_utc": [
                "2024-02-07T00:00:00+00:00",
                "2024-02-07T00:30:00+00:00",
                "2024-02-07T01:05:00+00:00",
            ],
        }
    ).write_parquet(tmp_path / "cbc_fx_reserves.parquet")
    result = _build_cbc_monthly_macro_features(
        tmp_path, market_symbol="__MARKET__"
    ).sort("date")
    assert result["date"].to_list() == [date(2024, 2, 7), date(2024, 2, 8)]
    assert result["twpub_cbc_fx_reserves_log"].to_list() == pytest.approx(
        [np.log(110), np.log(120)]
    )


def test_official_cbc_reserve_release_restores_original_value_next_session(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-09-06", "2024-09-09"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame({
        "period": ["2024-08"], "published_on": ["2024-09-06"],
        "metric": ["fx_reserves_usd_100m"], "value": [6019.04],
        "value_evidence": ["original_press_release_text"], "html_sha256": ["abc"],
    }).write_parquet(tmp_path / "cbc_fx_reserve_release_vintages.parquet")
    result = _build_cbc_monthly_macro_features(tmp_path, market_symbol="__MARKET__")
    assert result["date"].to_list() == [date(2024, 9, 9)]
    assert result["twpub_cbc_fx_reserves_log"].to_list() == pytest.approx([np.log(601.904)])
    assert result["twpub_cbc_fx_reserves_usd_billion_raw"].to_list() == pytest.approx([601.904])


def test_conflicting_same_day_cbc_reserve_values_fail_closed(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-09-09"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame({
        "period": ["2024-08", "2024-08"],
        "published_on": ["2024-09-06", "2024-09-06"],
        "metric": ["fx_reserves_usd_100m"] * 2,
        "value": [6019.04, 6020.0],
        "value_evidence": ["original_press_release_text"] * 2,
        "html_sha256": ["abc", "def"],
    }).write_parquet(tmp_path / "cbc_fx_reserve_release_vintages.parquet")
    with pytest.raises(ValueError, match="conflicting same-day CBC"):
        _build_cbc_monthly_macro_features(tmp_path, market_symbol="__MARKET__")


def test_official_dgbas_release_headlines_recover_causal_history(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-09-06", "2024-09-09", "2024-09-10"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "source": ["cpi", "unemployment", "gdp"],
            "period": ["2024-08", "2024-08", "2024-Q2"],
            "published_on": ["2024-09-06"] * 3,
            "release_id": ["1", "2", "3"],
            "release_kind": ["article"] * 3,
            "metric": ["cpi_yoy_pct", "unemployment_rate_pct", "gdp_yoy_pct"],
            "value_pct": [-1.25, 3.39, 7.12],
            "value_evidence": ["official_release_headline"] * 3,
            "html_sha256": ["abc"] * 3,
        }
    ).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    result = _merge_feature_frames([
        _build_dgbas_macro_features(tmp_path, market_symbol="__MARKET__")
    ])
    assert result["date"].to_list() == [date(2024, 9, 9)]
    assert result["twpub_dgbas_cpi_yoy"].to_list() == pytest.approx([-0.0125])
    assert result["twpub_dgbas_unemployment_rate"].to_list() == pytest.approx([0.0339])
    assert result["twpub_dgbas_gdp_yoy"].to_list() == pytest.approx([0.0712])
    assert result["twpub_dgbas_cpi_yoy_pct_raw"].to_list() == pytest.approx([-1.25])
    assert result["twpub_dgbas_unemployment_pct_raw"].to_list() == pytest.approx([3.39])
    assert result["twpub_dgbas_gdp_yoy_pct_raw"].to_list() == pytest.approx([7.12])


def test_dgbas_original_unemployment_pdf_value_enters_next_verified_session(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2005-02-25", "2005-03-01"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame({
        "source": ["unemployment"], "period": ["2005-01"],
        "published_on": ["2005-02-25"], "release_id": ["1"],
        "release_kind": ["article"], "metric": ["unemployment_rate_pct"],
        "value_pct": [4.06], "value_evidence": ["original_attachment_unemployment_text"],
        "html_sha256": ["abc"],
    }).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    result = _build_dgbas_macro_features(tmp_path, market_symbol="__MARKET__")
    assert result["date"].to_list() == [date(2005, 3, 1)]
    assert result["twpub_dgbas_unemployment_rate"].to_list() == pytest.approx([0.0406])


def test_original_dgbas_pdf_clock_controls_exact_opening_boundary(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-09-06", "2024-09-09"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame({
        "source": ["cpi", "cpi"], "period": ["2024-07", "2024-08"],
        "published_on": ["2024-09-06"] * 2,
        "release_id": ["1", "2"], "release_kind": ["article"] * 2,
        "metric": ["cpi_yoy_pct"] * 2, "value_pct": [1.0, 2.0],
        "value_evidence": ["original_attachment_cpi_text"] * 2,
        "html_sha256": ["abc", "def"],
        "published_time_precision": ["official_document_time"] * 2,
        "published_clock_taipei": ["08:59:59", "09:00:00"],
    }).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    result = _build_dgbas_macro_features(tmp_path, market_symbol="__MARKET__").sort("date")
    assert result["date"].to_list() == [date(2024, 9, 6), date(2024, 9, 9)]
    assert result["twpub_dgbas_cpi_yoy"].to_list() == pytest.approx([0.01, 0.02])


def test_dgbas_official_0830_schedule_uses_same_session_until_2017(tmp_path: Path) -> None:
    pl.DataFrame({"date": [
        "2017-05-05", "2017-05-08", "2017-07-05", "2017-07-06",
    ]}).write_parquet(tmp_path / "twse_taiex_ohlc.parquet")
    pl.DataFrame({
        "source": ["cpi", "unemployment", "gdp", "gdp", "cpi", "cpi"],
        "period": ["2017-04", "2017-04", "2017-Q1", "2016-Q4", "2017-06", "2017-03"],
        "published_on": ["2017-05-05"] * 4 + ["2017-07-05", "2017-05-05"],
        "release_id": ["1", "2", "3", "4", "5", "6"],
        "release_kind": ["article"] * 6,
        "title": ["物價", "失業率", "GDP 概估統計", "GDP 初步統計", "物價", "物價"],
        "metric": ["cpi_yoy_pct", "unemployment_rate_pct", "gdp_yoy_pct", "gdp_yoy_pct", "cpi_yoy_pct", "cpi_yoy_pct"],
        "value_pct": [1.0, 3.5, 2.0, 1.9, 1.5, 1.2],
        "value_evidence": ["official_release_headline"] * 6,
        "html_sha256": ["abc"] * 6,
        "published_time_precision": ["official_date_only"] * 5 + ["official_document_time"],
        "published_clock_taipei": [None] * 5 + ["16:00:00"],
    }).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    result = _merge_feature_frames([
        _build_dgbas_macro_features(tmp_path, market_symbol="__MARKET__")
    ]).sort("date")
    assert result["date"].to_list() == [
        date(2017, 5, 5), date(2017, 5, 8), date(2017, 7, 6)
    ]
    assert result["twpub_dgbas_cpi_yoy"].to_list()[0] == pytest.approx(0.01)
    assert result["twpub_dgbas_cpi_yoy"].to_list()[1] == pytest.approx(0.012)
    assert result["twpub_dgbas_unemployment_rate"].to_list()[0] == pytest.approx(0.035)
    assert result["twpub_dgbas_gdp_yoy"].to_list()[:2] == pytest.approx([0.02, 0.019])


def test_conflicting_same_day_dgbas_headlines_fail_closed(tmp_path: Path) -> None:
    pl.DataFrame({"date": ["2024-09-09"]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame(
        {
            "source": ["cpi", "cpi"],
            "period": ["2024-08", "2024-08"],
            "published_on": ["2024-09-06", "2024-09-06"],
            "release_id": ["1", "2"],
            "release_kind": ["article", "article"],
            "metric": ["cpi_yoy_pct", "cpi_yoy_pct"],
            "value_pct": [1.0, 2.0],
            "value_evidence": ["official_release_headline"] * 2,
            "html_sha256": ["abc", "def"],
        }
    ).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    with pytest.raises(ValueError, match="conflicting same-day DGBAS"):
        _build_dgbas_macro_features(tmp_path, market_symbol="__MARKET__")
