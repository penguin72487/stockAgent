from __future__ import annotations

import argparse

import polars as pl
import pyarrow.parquet as pq

from downloader import download_yahoo_ohlcv as yahoo


def _write_parquet(frame: pl.DataFrame, path) -> None:
    pq.write_table(frame.to_arrow(), path, compression="snappy", write_statistics=True)


def _base_args(tmp_path, **overrides):
    values = {
        "asset": "tw_stocks",
        "mode": "daily-update",
        "output_root": str(tmp_path),
        "output_dir": None,
        "start_date": "2000-01-01",
        "end_date": "2026-06-11",
        "limit": None,
        "symbols": None,
        "symbols_file": None,
        "include_tw_delisted": False,
        "include_us_delisted": False,
        "alpha_vantage_api_key": "",
        "repair_overlap_days": 7,
        "precheck_file_timeout_seconds": 0,
        "daily_stale_max_lag_days": 14,
        "daily_discover_symbols": True,
        "daily_retry_known_missing_symbols": False,
        "retry_blacklisted_repair_symbols": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_daily_resolution_preserves_known_manifest_without_retrying_missing(tmp_path, monkeypatch):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "1111", "name": "tracked", "market": "tw_stocks", "yahoo_symbol": "1111.TW"},
            {"code": "9999", "name": "known_missing", "market": "tw_stocks", "yahoo_symbol": "9999.TW"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "1111_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(
        yahoo,
        "_load_repo_symbol_fallback",
        lambda asset_class: [
            yahoo.SymbolRecord("9999", "known_missing", "tw_stocks", "9999.TW"),
            yahoo.SymbolRecord("2222", "repo_new", "tw_stocks", "2222.TW"),
        ],
    )
    monkeypatch.setattr(
        yahoo,
        "_discover_daily_stock_records",
        lambda asset_class, args, cached: [
            yahoo.SymbolRecord("9999", "known_missing", "tw_stocks", "9999.TW"),
            yahoo.SymbolRecord("3333", "discovered_new", "tw_stocks", "3333.TW"),
        ],
    )

    resolution = yahoo._resolve_symbol_resolution("tw_stocks", _base_args(tmp_path))

    assert [record.code for record in resolution.scheduled_records] == ["1111", "2222", "3333"]
    assert [record.code for record in resolution.manifest_records] == ["1111", "9999", "2222", "3333"]


def test_tw_exchange_parser_excludes_warrant_like_listings(monkeypatch):
    def fake_read_html_table_rows(url):
        if "strMode=2" not in url:
            return []
        return [
            ["股票"],
            ["2330 台積電", "", "", "上市"],
            ["2888A 華南金甲特", "", "", "上市"],
            ["030037 景碩群益57購01", "", "", "上市"],
            ["03003T 聯發科群益5A售09", "", "", "上市"],
            ["03006X 元展06", "", "", "上市"],
            ["ETF"],
            ["0050 元大台灣50", "", "", "上市"],
            ["00632R 元大台灣50反1", "", "", "上市"],
            ["087644 臺股指凱基58購01", "", "", "上市"],
        ]

    monkeypatch.setattr(yahoo, "_read_html_table_rows", fake_read_html_table_rows)

    records = yahoo._load_tw_symbols_from_exchange()
    by_code = {record.code: record for record in records}

    assert {"2330", "2888A", "0050", "00632R"}.issubset(by_code)
    assert {"030037", "03003T", "03006X", "087644"}.isdisjoint(by_code)


def test_daily_resolution_prunes_cached_tw_warrants_from_manifest_and_schedule(tmp_path, monkeypatch):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "2330", "name": "台積電", "market": "tw_stocks", "yahoo_symbol": "2330.TW"},
            {"code": "03003T", "name": "聯發科群益5A售09", "market": "listed", "yahoo_symbol": "03003T.TW"},
            {"code": "030037", "name": "景碩群益57購01", "market": "listed", "yahoo_symbol": "030037.TW"},
        ]
    ).write_csv(output_dir / "symbols.csv")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])
    monkeypatch.setattr(
        yahoo,
        "_discover_daily_stock_records",
        lambda asset_class, args, cached: [
            yahoo.SymbolRecord("2330", "台積電", "listed", "2330.TW"),
            yahoo.SymbolRecord("0050", "元大台灣50", "listed", "0050.TW"),
        ],
    )

    resolution = yahoo._resolve_symbol_resolution("tw_stocks", _base_args(tmp_path))

    assert [record.code for record in resolution.scheduled_records] == ["2330", "0050"]
    assert [record.code for record in resolution.manifest_records] == ["2330", "0050"]
    assert {"03003T", "030037"}.isdisjoint({record.code for record in resolution.scheduled_records})
    assert {"03003T", "030037"}.isdisjoint({record.code for record in resolution.manifest_records})


def test_tw_daily_cache_fast_path_prunes_unsupported_cached_records(tmp_path):
    cached = [
        yahoo.SymbolRecord("2330", "台積電", "tw_stocks", "2330.TW"),
        yahoo.SymbolRecord("03003T", "聯發科群益5A售09", "listed", "03003T.TW"),
    ]

    records = yahoo._resolve_tw_symbols(_base_args(tmp_path, daily_discover_symbols=False), cached)

    assert [record.code for record in records] == ["2330"]


def test_unavailable_yahoo_timezone_message_is_blacklist_trigger():
    captured = "$03003T.TW: possibly delisted; no timezone found"

    assert yahoo._captured_indicates_unavailable(captured.lower())


def test_report_frame_handles_late_optional_string_values():
    rows = [{"code": str(i), "message": None} for i in range(101)]
    rows.append({"code": "9999", "message": "All candidate Yahoo symbols are in blacklist."})

    frame = yahoo._report_frame_from_rows(rows, ["code", "message"])

    assert frame.schema["message"] == pl.String
    assert frame["message"].to_list()[-1] == "All candidate Yahoo symbols are in blacklist."


def test_blacklisted_missing_symbols_skip_repair_until_forced(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    (output_dir / "yahoo_blacklist.txt").write_text("9999.TW\n", encoding="utf-8")
    record = yahoo.SymbolRecord("9999", "known_missing", "tw_stocks", "9999.TW")

    checks = yahoo._resolve_repair_plan("tw_stocks", _base_args(tmp_path), [record], output_dir)
    assert [(check.status, check.repair_start_date) for check in checks] == [("not_found_skip", None)]

    forced = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path, retry_blacklisted_repair_symbols=True),
        [record],
        output_dir,
    )
    assert [(check.status, check.repair_start_date) for check in forced] == [("missing", "2000-01-01")]


def test_repair_plan_separates_new_symbols_and_delisted_symbols(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    new_record = yahoo.SymbolRecord("2222", "new_listing", "tw_stocks", "2222.TW")
    delisted_record = yahoo.SymbolRecord("1111_TW", "delisted", "tw_delisted", "1111.TW")

    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path),
        [new_record, delisted_record],
        output_dir,
        new_codes={"2222"},
    )

    assert [(check.record.code, check.status, check.repair_start_date) for check in checks] == [
        ("2222", "new_symbol", "2000-01-01"),
        ("1111_TW", "delisted_no_history", None),
    ]


def test_yahoo_forex_ignores_orphan_frankfurter_parquets(tmp_path, monkeypatch):
    output_dir = tmp_path / "forex"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "EURUSD", "name": "EURUSD", "market": "forex", "yahoo_symbol": "EURUSD=X"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "AUDBRL_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])

    args = _base_args(tmp_path, asset="forex", output_root=str(tmp_path), daily_discover_symbols=False)
    resolution = yahoo._resolve_symbol_resolution("forex", args)

    assert [record.code for record in resolution.scheduled_records] == ["EURUSD"]
    assert [record.code for record in resolution.manifest_records] == ["EURUSD"]


def test_normalize_preserves_zero_volume_for_stock_like_assets():
    frame = pl.DataFrame(
        {
            "Date": ["2026-06-10", "2026-06-11"],
            "Open": [10.0, 10.5],
            "High": [11.0, 11.0],
            "Low": [9.5, 10.0],
            "Close": [10.5, 10.2],
            "Adj Close": [10.5, 10.2],
            "Volume": [0, 0],
        }
    )

    normalized = yahoo._normalize_download_frame(frame, keep_zero_volume=True)

    assert "Trading_Volume" in normalized.columns
    assert normalized["Trading_Volume"].to_list() == [0.0, 0.0]


def test_normalize_can_drop_zero_volume_for_assets_without_meaningful_volume():
    frame = pl.DataFrame(
        {
            "Date": ["2026-06-10", "2026-06-11"],
            "Open": [1.1, 1.2],
            "High": [1.2, 1.3],
            "Low": [1.0, 1.1],
            "Close": [1.15, 1.25],
            "Adj Close": [1.15, 1.25],
            "Volume": [0, 0],
        }
    )

    normalized = yahoo._normalize_download_frame(frame, keep_zero_volume=False)

    assert "Trading_Volume" not in normalized.columns


def test_daily_resolution_marks_cached_active_symbol_as_delisted(tmp_path, monkeypatch):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "OLDW", "name": "old warrant", "market": "us_stocks", "yahoo_symbol": "OLDW"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "OLDW_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])
    monkeypatch.setattr(
        yahoo,
        "_discover_daily_stock_records",
        lambda asset_class, args, cached: [
            yahoo.SymbolRecord("OLDW_DL", "old warrant", "us_delisted", "OLDW"),
        ],
    )

    resolution = yahoo._resolve_symbol_resolution("us_stocks", _base_args(tmp_path, asset="us_stocks"))

    assert [(record.code, record.market, record.yahoo_symbol) for record in resolution.scheduled_records] == [
        ("OLDW", "us_delisted", "OLDW"),
    ]
    assert [(record.code, record.market, record.yahoo_symbol) for record in resolution.manifest_records] == [
        ("OLDW", "us_delisted", "OLDW"),
    ]


def test_repair_plan_removes_delisted_file_without_usable_history(tmp_path):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    output_path = output_dir / "OLDW_features.parquet"
    _write_parquet(pl.DataFrame({"close": [1.0]}), output_path)

    record = yahoo.SymbolRecord("OLDW", "old warrant", "us_delisted", "OLDW")
    checks = yahoo._resolve_repair_plan("us_stocks", _base_args(tmp_path, asset="us_stocks"), [record], output_dir)

    assert [(check.record.code, check.status, check.repair_start_date) for check in checks] == [
        ("OLDW", "delisted_removed", None),
    ]
    assert not output_path.exists()
    assert (output_dir / "yahoo_blacklist.txt").read_text(encoding="utf-8").strip() == "OLDW"


def test_repair_plan_keeps_delisted_file_with_history_without_refetch(tmp_path):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    output_path = output_dir / "OLDW_features.parquet"
    _write_parquet(pl.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02"],
            "open": [1.0, 1.1],
            "max": [1.2, 1.2],
            "min": [0.9, 1.0],
            "close": [1.1, 1.0],
            "adjclose": [1.1, 1.0],
            "Trading_Volume": [100, 0],
        }
    ), output_path)

    record = yahoo.SymbolRecord("OLDW", "old warrant", "us_delisted", "OLDW")
    checks = yahoo._resolve_repair_plan("us_stocks", _base_args(tmp_path, asset="us_stocks"), [record], output_dir)

    assert [(check.record.code, check.status, check.repair_start_date) for check in checks] == [
        ("OLDW", "delisted_skip", None),
    ]
    assert output_path.exists()


def test_daily_repair_plan_treats_weekend_stock_target_as_current(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2330_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-11", "2026-06-12"],
                "open": [100.0, 101.0],
                "max": [102.0, 103.0],
                "min": [99.0, 100.0],
                "close": [101.0, 102.0],
                "adjclose": [101.0, 102.0],
                "Trading_Volume": [1000, 1100],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_end_date="2026-06-12",
    )

    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path, end_date="2026-06-14"),
        [record],
        output_dir,
    )

    assert [(check.status, check.last_date, check.repair_start_date) for check in checks] == [
        ("current", "2026-06-12", None),
    ]


def test_daily_repair_plan_uses_checked_through_metadata_to_avoid_same_day_refetch(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2330_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-11", "2026-06-12"],
                "open": [100.0, 101.0],
                "max": [102.0, 103.0],
                "min": [99.0, 100.0],
                "close": [101.0, 102.0],
                "adjclose": [101.0, 102.0],
                "Trading_Volume": [1000, 1100],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_end_date="2026-06-15",
    )

    info = yahoo._load_existing_file_info(output_path)
    assert info.checked_through_date == "2026-06-15"

    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path, end_date="2026-06-15"),
        [record],
        output_dir,
    )

    assert [(check.status, check.last_date, check.checked_through_date, check.repair_start_date) for check in checks] == [
        ("current", "2026-06-12", "2026-06-15", None),
    ]
