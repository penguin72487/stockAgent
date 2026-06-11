from __future__ import annotations

import argparse

import pandas as pd

from downloader import download_yahoo_ohlcv as yahoo


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
    pd.DataFrame(
        [
            {"code": "1111", "name": "tracked", "market": "tw_stocks", "yahoo_symbol": "1111.TW"},
            {"code": "9999", "name": "known_missing", "market": "tw_stocks", "yahoo_symbol": "9999.TW"},
        ]
    ).to_csv(output_dir / "symbols.csv", index=False)
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
        ("1111_TW", "delisted_skip", None),
    ]


def test_yahoo_forex_ignores_orphan_frankfurter_parquets(tmp_path, monkeypatch):
    output_dir = tmp_path / "forex"
    output_dir.mkdir()
    pd.DataFrame(
        [
            {"code": "EURUSD", "name": "EURUSD", "market": "forex", "yahoo_symbol": "EURUSD=X"},
        ]
    ).to_csv(output_dir / "symbols.csv", index=False)
    (output_dir / "AUDBRL_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])

    args = _base_args(tmp_path, asset="forex", output_root=str(tmp_path), daily_discover_symbols=False)
    resolution = yahoo._resolve_symbol_resolution("forex", args)

    assert [record.code for record in resolution.scheduled_records] == ["EURUSD"]
    assert [record.code for record in resolution.manifest_records] == ["EURUSD"]
