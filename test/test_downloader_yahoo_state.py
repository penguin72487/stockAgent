from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from urllib.error import HTTPError

import polars as pl
import pyarrow.parquet as pq
import pytest

from downloader import download_yahoo_ohlcv as yahoo


def _write_parquet(frame: pl.DataFrame, path) -> None:
    pq.write_table(frame.to_arrow(), path, compression="snappy", write_statistics=True)


def _yahoo_chart_payload(
    timestamp: int,
    *,
    meta: dict[str, object] | None = None,
    events: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "timestamp": [timestamp],
        "indicators": {
            "quote": [
                {
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000.0],
                }
            ],
            "adjclose": [{"adjclose": [50.0]}],
        },
    }
    if meta is not None:
        result["meta"] = meta
    if events is not None:
        result["events"] = events
    return {"chart": {"result": [result], "error": None}}


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
    # A caller-provided namespace is the resolved configuration boundary.  An
    # unrelated in-process workflow may have set this global environment flag,
    # but it must not turn off the repo fallback behind the caller's back.
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
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


def test_targeted_incremental_resolution_reuses_known_provider_candidates(tmp_path, monkeypatch):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "3303", "name": "existing", "market": "tw_stocks", "yahoo_symbol": "3303.TWO"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    monkeypatch.setattr(
        yahoo,
        "_load_repo_symbol_fallback",
        lambda _asset: [yahoo.SymbolRecord("8111", "repo", "tw_stocks", "8111.TWO")],
    )
    args = _base_args(tmp_path, symbols=["3303"])

    resolution = yahoo._resolve_symbol_resolution("tw_stocks", args)

    assert [record.code for record in resolution.scheduled_records] == ["3303"]
    assert [record.code for record in resolution.manifest_records] == ["3303"]
    assert resolution.scheduled_records[0].yahoo_symbol == "3303.TWO"
    assert resolution.manifest_records[0].yahoo_symbol == "3303.TW"


def test_targeted_repair_preserves_full_cached_manifest(tmp_path, monkeypatch):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "3303", "name": "existing", "market": "tw_stocks", "yahoo_symbol": "3303.TWO"},
            {"code": "2330", "name": "TSMC", "market": "tw_stocks", "yahoo_symbol": "2330.TW"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    args = _base_args(tmp_path, mode="repair", symbols=["3303"])

    resolution = yahoo._resolve_symbol_resolution("tw_stocks", args)

    assert [record.code for record in resolution.scheduled_records] == ["3303"]
    assert resolution.scheduled_records[0].yahoo_symbol == "3303.TWO"
    assert [record.code for record in resolution.manifest_records] == ["3303", "2330"]


def test_targeted_refresh_preserves_full_cached_manifest(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "3303", "name": "existing", "market": "tw_stocks", "yahoo_symbol": "3303.TWO"},
            {"code": "2330", "name": "TSMC", "market": "tw_stocks", "yahoo_symbol": "2330.TW"},
        ]
    ).write_csv(output_dir / "symbols.csv")

    resolution = yahoo._resolve_symbol_resolution(
        "tw_stocks",
        _base_args(tmp_path, mode="download", symbols=["3303"]),
    )

    assert [record.code for record in resolution.scheduled_records] == ["3303"]
    assert resolution.scheduled_records[0].yahoo_symbol == "3303.TWO"
    assert [record.code for record in resolution.manifest_records] == ["3303", "2330"]


def test_targeted_tw_delisted_archive_alias_resolves_provider_symbol():
    record = yahoo._record_from_input("tw_stocks", "9801_TW")

    assert record == yahoo.SymbolRecord(
        code="9801_TW",
        name="9801 (delisted)",
        market="tw_delisted",
        yahoo_symbol="9801.TW",
    )


def test_targeted_download_report_preserves_other_manifest_terminal_rows(tmp_path):
    old_result = yahoo.DownloadResult(
        asset_class="tw_stocks",
        code="2833",
        yahoo_symbol="2833.TW",
        market="tw_delisted",
        status="not_found",
        rows=0,
        output_path=None,
    )
    yahoo._write_download_artifacts(tmp_path, "tw_stocks", [old_result])

    refreshed = yahoo.DownloadResult(
        asset_class="tw_stocks",
        code="2330",
        yahoo_symbol="2330.TW",
        market="listed",
        status="updated",
        rows=10,
        output_path="2330_features.parquet",
    )
    yahoo._write_download_artifacts(
        tmp_path,
        "tw_stocks",
        [refreshed],
        manifest_codes={"2330", "2833"},
    )

    report = pl.read_csv(tmp_path / "download_report.csv")
    assert dict(zip(report["code"].cast(pl.String), report["status"], strict=True)) == {
        "2833": "not_found",
        "2330": "updated",
    }


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

    assert {"2330", "0050", "00632R"}.issubset(by_code)
    assert {"2888A", "030037", "03003T", "03006X", "087644"}.isdisjoint(
        by_code
    )


def test_official_tw_delisted_parquets_keep_only_stock_and_etf(tmp_path):
    public_dir = tmp_path / "data_tw_public"
    public_dir.mkdir()
    _write_parquet(
        pl.DataFrame(
            [
                {"symbol": "2330", "company_name": "台積電"},
                {"symbol": "0050", "company_name": "元大台灣50"},
                {"symbol": "2888A", "company_name": "華南金甲特"},
                {"symbol": "030037", "company_name": "景碩群益57購01"},
                {"symbol": "01001T", "company_name": "土銀富邦R1"},
            ]
        ),
        public_dir / "twse_delisted_company.parquet",
    )
    _write_parquet(
        pl.DataFrame(
            [
                {"symbol": "6488", "company_name": "環球晶"},
                {"symbol": "006201", "company_name": "元大富櫃50"},
                {"symbol": "020001", "company_name": "富邦特選蘋果N"},
                {"symbol": "726001", "company_name": "上櫃權證"},
            ]
        ),
        public_dir / "tpex_delisted_company.parquet",
    )

    records = yahoo._load_tw_delisted_symbols_from_parquet(public_dir)

    assert [
        (record.code, record.name, record.market, record.yahoo_symbol)
        for record in records
    ] == [
        ("2330_TW", "台積電", "tw_delisted", "2330.TW"),
        ("0050_TW", "元大台灣50", "tw_delisted", "0050.TW"),
        ("6488_TWO", "環球晶", "tw_delisted", "6488.TWO"),
        ("006201_TWO", "元大富櫃50", "tw_delisted", "006201.TWO"),
    ]


@pytest.mark.parametrize("exchange_fails", [False, True])
def test_tw_repair_unions_live_cached_repo_and_tracked_symbols(
    tmp_path,
    monkeypatch,
    exchange_fails,
):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {
                "code": "2330",
                "name": "stale cached name",
                "market": "tw_stocks",
                "yahoo_symbol": "2330.TW",
            },
            {
                "code": "7777",
                "name": "cached historical",
                "market": "tw_delisted",
                "yahoo_symbol": "7777.TW",
            },
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "9999_features.parquet").write_text(
        "tracked placeholder",
        encoding="utf-8",
    )

    def load_exchange():
        if exchange_fails:
            raise RuntimeError("exchange unavailable")
        return [yahoo.SymbolRecord("2330", "live name", "listed", "2330.TW")]

    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_exchange", load_exchange)
    monkeypatch.setattr(
        yahoo,
        "_load_repo_symbol_fallback",
        lambda _asset: [
            yahoo.SymbolRecord("8888", "repo historical", "tw_delisted", "8888.TW"),
            yahoo.SymbolRecord("03003T", "warrant", "listed", "03003T.TW"),
        ],
    )
    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_local_manifest", lambda: [])
    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_local_parquet", lambda: [])
    monkeypatch.setattr(yahoo, "_load_tw_delisted_symbols", lambda: [])
    monkeypatch.setattr(
        yahoo,
        "_fetch_with_hard_timeout",
        lambda function, timeout: function(),
    )

    resolution = yahoo._resolve_symbol_resolution(
        "tw_stocks",
        _base_args(tmp_path, mode="repair"),
    )

    assert [record.code for record in resolution.scheduled_records] == [
        "2330",
        "7777",
        "8888",
        "9999",
    ]
    assert [record.code for record in resolution.manifest_records] == [
        "2330",
        "7777",
        "8888",
        "9999",
    ]
    assert resolution.scheduled_records[0].name == (
        "stale cached name" if exchange_fails else "live name"
    )


def test_tw_repair_reconciles_cached_plain_code_with_official_delisting(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    cached = [
        yahoo.SymbolRecord("2833", "台壽保", "tw_stocks", "2833.TW"),
    ]
    monkeypatch.setattr(
        yahoo,
        "_load_tw_symbols_from_exchange",
        lambda: [yahoo.SymbolRecord("2330", "台積電", "listed", "2330.TW")],
    )
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda _asset: [])
    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_local_manifest", lambda: [])
    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_local_parquet", lambda: [])
    monkeypatch.setattr(yahoo, "_load_local_tracked_records", lambda *_args: [])
    monkeypatch.setattr(
        yahoo,
        "_load_tw_delisted_symbols_from_parquet",
        lambda _root: [
            yahoo.SymbolRecord("2833_TW", "台壽保", "tw_delisted", "2833.TW")
        ],
    )
    monkeypatch.setattr(yahoo, "_load_tw_delisted_symbols", lambda: [])
    monkeypatch.setattr(
        yahoo,
        "_fetch_with_hard_timeout",
        lambda function, timeout: function(),
    )

    records = yahoo._resolve_tw_symbols(
        _base_args(
            tmp_path,
            mode="repair",
            include_tw_delisted=True,
            tw_delisted_dir=tmp_path,
        ),
        cached,
    )

    by_code = {record.code: record for record in records}
    assert by_code["2330"].market == "listed"
    assert by_code["2833"].market == "tw_delisted"


def test_strict_no_fallback_rejects_tw_symbol_discovery_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(yahoo, "_load_tw_symbols_from_exchange", lambda: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(
        yahoo,
        "_load_repo_symbol_fallback",
        lambda asset_class: [yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")],
    )

    with pytest.raises(RuntimeError, match="strict_no_fallback=true"):
        yahoo._resolve_tw_symbols(_base_args(tmp_path, strict_no_fallback=True), cached=[])


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


def test_tw_manifest_writer_excludes_classifier_rejected_records_with_receipt(
    tmp_path,
):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    records = [
        yahoo.SymbolRecord("2330", "台積電", "listed", "2330.TW"),
        yahoo.SymbolRecord("0050", "元大台灣50", "listed", "0050.TW"),
        yahoo.SymbolRecord("1214_TW", "尚德實業", "tw_delisted", "1214.TW"),
        yahoo.SymbolRecord("1101B", "台泥乙特", "listed", "1101B.TW"),
        yahoo.SymbolRecord(
            "2888A_TW",
            "華南金甲特",
            "tw_delisted",
            "2888A.TW",
        ),
    ]

    yahoo._write_symbol_manifest(
        output_dir,
        records,
        asset_class="tw_stocks",
    )

    manifest = pl.read_csv(output_dir / "symbols.csv")
    assert manifest["code"].to_list() == ["2330", "0050", "1214_TW"]
    summary = json.loads(
        (output_dir / "symbols_manifest_summary.json").read_text(encoding="utf-8")
    )
    assert summary["included_record_count"] == 3
    assert summary["excluded_record_count"] == 2
    assert summary["excluded_reason_counts"] == {
        yahoo.TW_SECURITY_CLASSIFIER_EXCLUSION_REASON: 2
    }
    assert {row["code"] for row in summary["excluded_records"]} == {
        "1101B",
        "2888A_TW",
    }


def test_us_broker_filter_keeps_delisted_common_but_excludes_special_tools():
    records = [
        yahoo.SymbolRecord("AAPL", "Apple Inc. - Common Stock", "us_stocks", "AAPL"),
        yahoo.SymbolRecord("TSM", "Taiwan Semiconductor Manufacturing Company Ltd. ADR", "us_stocks", "TSM"),
        yahoo.SymbolRecord("SPY", "SPDR S&P 500 ETF", "us_stocks", "SPY"),
        yahoo.SymbolRecord("OLD_DL", "Old Winner Corp. - Common Stock", "us_delisted", "OLD"),
        yahoo.SymbolRecord("AACIW", "Armada Acquisition Corp. III - Warrant", "us_stocks", "AACIW"),
        yahoo.SymbolRecord("AACIR", "Armada Acquisition Corp. III - Rights", "us_stocks", "AACIR"),
        yahoo.SymbolRecord("AACIU", "Armada Acquisition Corp. III - Units", "us_stocks", "AACIU"),
        yahoo.SymbolRecord("BHFAL", "Brighthouse Financial, Inc. - Junior Subordinated Debentures due 2058", "us_stocks", "BHFAL"),
        yahoo.SymbolRecord("ACGLN", "Arch Capital Group Ltd. - Depositary Shares representing Preferred Shares", "us_stocks", "ACGLN"),
    ]

    filtered = yahoo._filter_us_records_for_broker_tradable_universe(records)

    assert [record.code for record in filtered] == ["AAPL", "TSM", "SPY", "OLD_DL"]


def test_us_daily_resolution_prunes_cached_untradable_tools_from_manifest_and_schedule(tmp_path, monkeypatch):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "AAPL", "name": "Apple Inc. - Common Stock", "market": "us_stocks", "yahoo_symbol": "AAPL"},
            {"code": "AACIW", "name": "Armada Acquisition Corp. III - Warrant", "market": "us_stocks", "yahoo_symbol": "AACIW"},
            {"code": "OLD_DL", "name": "Old Winner Corp. - Common Stock", "market": "us_delisted", "yahoo_symbol": "OLD"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "AAPL_features.parquet").write_text("placeholder", encoding="utf-8")
    (output_dir / "AACIW_features.parquet").write_text("placeholder", encoding="utf-8")
    (output_dir / "OLD_DL_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_discover_daily_stock_records", lambda asset_class, args, cached: [])

    resolution = yahoo._resolve_symbol_resolution("us_stocks", _base_args(tmp_path, asset="us_stocks"))

    assert {record.code for record in resolution.scheduled_records} == {"AAPL", "OLD_DL"}
    assert {record.code for record in resolution.manifest_records} == {"AAPL", "OLD_DL"}


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


def test_verify_tw_delisted_history_repairs_missing_file_and_start_receipt(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-11"],
                "open": [100.0],
                "max": [102.0],
                "min": [99.0],
                "close": [101.0],
                "adjclose": [101.0],
                "Trading_Volume": [1000],
            }
        ),
        output_dir / "1111_TW_features.parquet",
        asset_class="tw_stocks",
        requested_end_date="2026-06-11",
    )
    records = [
        yahoo.SymbolRecord("1111_TW", "old listed stock", "tw_delisted", "1111.TW"),
        yahoo.SymbolRecord("2222_TWO", "old OTC stock", "tw_delisted", "2222.TWO"),
    ]

    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path, verify_tw_delisted_history=True),
        records,
        output_dir,
    )

    assert [
        (check.record.code, check.status, check.repair_start_date, check.merge_existing)
        for check in checks
    ] == [
        ("1111_TW", "historical_start_unverified", "2000-01-01", True),
        ("2222_TWO", "missing", "2000-01-01", False),
    ]


def test_terminal_unavailable_quarantines_unverified_history_with_jsonl_receipt(
    tmp_path,
):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2833_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2015-10-13"],
                "open": [10.0],
                "max": [10.5],
                "min": [9.5],
                "close": [10.1],
                "adjclose": [10.1],
                "Trading_Volume": [1000],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_end_date="2026-06-11",
    )
    original_hash = yahoo._sha256_file(output_path)
    original_bytes = output_path.stat().st_size
    record = yahoo.SymbolRecord("2833", "old bank", "tw_stocks", "2833.TW")
    check = yahoo.RepairCheck(
        record=record,
        status="historical_start_unverified",
        output_path=output_path,
        first_date="2000-01-04",
        last_date="2015-10-13",
        repair_start_date="2000-01-01",
        merge_existing=True,
        message="requested_start=2000-01-01, checked_start=None",
    )
    result = yahoo.DownloadResult(
        asset_class="tw_stocks",
        code="2833",
        yahoo_symbol="2833.TW",
        market="tw_stocks",
        status="failed",
        rows=0,
        output_path=None,
        message="2833.TW: possibly delisted; no timezone found",
    )

    transformed = yahoo._transform_repair_result(
        result,
        check,
        output_dir=output_dir,
    )

    assert transformed.status == "not_found"
    assert transformed.output_path is None
    assert not output_path.exists()
    quarantine_dir = output_dir / "quarantine" / "unverified_yahoo"
    quarantined = list(quarantine_dir.glob("2833_features.*.parquet"))
    assert len(quarantined) == 1
    assert original_hash[:16] in quarantined[0].name
    assert yahoo._sha256_file(quarantined[0]) == original_hash
    assert quarantined[0].stat().st_size == original_bytes
    events = [
        json.loads(line)
        for line in (quarantine_dir / "events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert len(events) == 1
    assert events[0] == {
        "timestamp_utc": events[0]["timestamp_utc"],
        "code": "2833",
        "yahoo_symbol": "2833.TW",
        "original_path": str(output_path),
        "quarantine_path": str(quarantined[0]),
        "sha256": original_hash,
        "bytes": original_bytes,
        "precheck_status": "historical_start_unverified",
        "reason": events[0]["reason"],
    }
    assert datetime.fromisoformat(events[0]["timestamp_utc"]).tzinfo is not None
    assert "terminal unavailable" in events[0]["reason"]


def test_unverified_history_is_not_quarantined_on_transport_failure(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2833_features.parquet"
    _write_parquet(pl.DataFrame({"date": ["2015-10-13"], "close": [10.1]}), output_path)
    record = yahoo.SymbolRecord("2833", "old bank", "tw_stocks", "2833.TW")
    check = yahoo.RepairCheck(
        record=record,
        status="historical_start_unverified",
        output_path=output_path,
        first_date="2015-10-13",
        last_date="2015-10-13",
        repair_start_date="2000-01-01",
    )
    result = yahoo.DownloadResult(
        asset_class="tw_stocks",
        code="2833",
        yahoo_symbol="2833.TW",
        market="tw_stocks",
        status="failed",
        rows=0,
        output_path=None,
        message="2833.TW: connection reset by peer",
    )

    transformed = yahoo._transform_repair_result(
        result,
        check,
        output_dir=output_dir,
    )

    assert transformed.status == "failed"
    assert output_path.exists()
    assert not (output_dir / "quarantine").exists()


def test_unverified_yahoo_quarantine_never_overwrites_and_appends_jsonl(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2833_features.parquet"
    record = yahoo.SymbolRecord("2833", "old bank", "tw_stocks", "2833.TW")
    payload = pl.DataFrame({"date": ["2015-10-13"], "close": [10.1]})
    quarantined_paths = []
    for reason in ("first terminal confirmation", "second terminal confirmation"):
        _write_parquet(payload, output_path)
        quarantined_paths.append(
            yahoo._quarantine_unverified_yahoo_file(
                output_dir=output_dir,
                record=record,
                original_path=output_path,
                precheck_status="schema_mismatch",
                reason=reason,
            )
        )

    assert all(path is not None and path.exists() for path in quarantined_paths)
    assert quarantined_paths[0] != quarantined_paths[1]
    assert yahoo._sha256_file(quarantined_paths[0]) == yahoo._sha256_file(
        quarantined_paths[1]
    )
    journal = (
        output_dir
        / "quarantine"
        / "unverified_yahoo"
        / "events.jsonl"
    )
    events = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    assert [event["reason"] for event in events] == [
        "first terminal confirmation",
        "second terminal confirmation",
    ]
    assert [event["quarantine_path"] for event in events] == [
        str(quarantined_paths[0]),
        str(quarantined_paths[1]),
    ]


def test_terminal_unavailable_keeps_valid_metadata_history_active(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2330_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-10"],
                "open": [100.0],
                "max": [102.0],
                "min": [99.0],
                "close": [101.0],
                "adjclose": [101.0],
                "Trading_Volume": [1000],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_start_date="2000-01-01",
        requested_end_date="2026-06-11",
    )
    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    check = yahoo.RepairCheck(
        record=record,
        status="stale",
        output_path=output_path,
        first_date="2000-01-04",
        last_date="2026-06-10",
        repair_start_date="2026-06-03",
    )
    result = yahoo.DownloadResult(
        asset_class="tw_stocks",
        code="2330",
        yahoo_symbol="2330.TW",
        market="tw_stocks",
        status="failed",
        rows=0,
        output_path=None,
        message="2330.TW: unavailable or delisted",
    )

    transformed = yahoo._transform_repair_result(
        result,
        check,
        output_dir=output_dir,
    )

    assert transformed.status == "not_found"
    assert output_path.exists()
    assert not (output_dir / "quarantine").exists()


def test_post_repair_coverage_excludes_quarantined_terminal_history(tmp_path):
    quarantined_active_path = tmp_path / "2833_features.parquet"
    retained_active_path = tmp_path / "2330_features.parquet"
    retained_active_path.write_bytes(b"active verified parquet placeholder")
    checks = [
        yahoo.RepairCheck(
            record=yahoo.SymbolRecord("2833", "old bank", "tw_stocks", "2833.TW"),
            status="historical_start_unverified",
            output_path=quarantined_active_path,
            first_date="2000-01-04",
            last_date="2015-10-13",
            repair_start_date="2000-01-01",
        ),
        yahoo.RepairCheck(
            record=yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW"),
            status="stale",
            output_path=retained_active_path,
            first_date="2001-01-02",
            last_date="2026-06-10",
            repair_start_date="2026-06-03",
        ),
    ]
    results = [
        yahoo.DownloadResult(
            asset_class="tw_stocks",
            code="2833",
            yahoo_symbol="2833.TW",
            market="tw_stocks",
            status="not_found",
            rows=0,
            output_path=None,
        ),
        yahoo.DownloadResult(
            asset_class="tw_stocks",
            code="2330",
            yahoo_symbol="2330.TW",
            market="tw_stocks",
            status="not_found",
            rows=0,
            output_path=None,
        ),
    ]

    summary = yahoo._summarize_post_repair_coverage(
        checks,
        results,
        "2026-06-11",
    )

    assert summary == ("2001-01-02", "2026-06-10", 1, 1)


def test_repair_plan_marks_wrong_yahoo_source_metadata_invalid(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2330_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-11"],
                "open": [100.0],
                "max": [102.0],
                "min": [99.0],
                "close": [101.0],
                "adjclose": [101.0],
                "Trading_Volume": [1000],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_start_date="2000-01-01",
        requested_end_date="2026-06-11",
        source="legacy_unknown",
    )
    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")

    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path),
        [record],
        output_dir,
    )

    assert [
        (check.status, check.repair_start_date, check.merge_existing)
        for check in checks
    ] == [("metadata_invalid", "2000-01-01", False)]
    assert "expected='yahoo'" in str(checks[0].message)


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


def test_daily_normalization_deduplicates_wall_clock_variants_of_one_session():
    frame = pl.DataFrame(
        {
            "Date": ["2018-07-03 00:00:00", "2018-07-03 13:30:00"],
            "Open": [13.8, 13.8],
            "High": [13.8, 13.8],
            "Low": [13.8, 13.8],
            "Close": [13.8, 13.8],
            "Adj Close": [13.8, 13.8],
            "Volume": [0, 0],
        }
    )

    normalized = yahoo._normalize_download_frame(frame, daily=True)

    assert normalized.height == 1
    assert normalized["date"].dt.date().to_list() == [datetime(2018, 7, 3).date()]


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
            {"code": "OLDC", "name": "Old Champion Corp. - Common Stock", "market": "us_stocks", "yahoo_symbol": "OLDC"},
        ]
    ).write_csv(output_dir / "symbols.csv")
    (output_dir / "OLDC_features.parquet").write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(yahoo, "_records_from_defaults", lambda asset_class: [])
    monkeypatch.setattr(yahoo, "_load_repo_symbol_fallback", lambda asset_class: [])
    monkeypatch.setattr(
        yahoo,
        "_discover_daily_stock_records",
        lambda asset_class, args, cached: [
            yahoo.SymbolRecord("OLDC_DL", "Old Champion Corp. - Common Stock", "us_delisted", "OLDC"),
        ],
    )

    resolution = yahoo._resolve_symbol_resolution("us_stocks", _base_args(tmp_path, asset="us_stocks"))

    assert [(record.code, record.market, record.yahoo_symbol) for record in resolution.scheduled_records] == [
        ("OLDC", "us_delisted", "OLDC"),
    ]
    assert [(record.code, record.market, record.yahoo_symbol) for record in resolution.manifest_records] == [
        ("OLDC", "us_delisted", "OLDC"),
    ]


def test_cached_us_archive_manifest_symbol_uses_base_yahoo_symbol(tmp_path):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    pl.DataFrame(
        [
            {"code": "ACCU_DL", "name": "ACCU_DL", "market": "us_stocks", "yahoo_symbol": "ACCU_DL"},
        ]
    ).write_csv(output_dir / "symbols.csv")

    records = yahoo._resolve_cached_manifest(output_dir, "us_stocks")

    assert [(record.code, record.market, record.yahoo_symbol) for record in records] == [
        ("ACCU_DL", "us_delisted", "ACCU"),
    ]


def test_local_us_archive_file_symbol_uses_base_yahoo_symbol(tmp_path):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    (output_dir / "ACCU_DL_features.parquet").write_text("placeholder", encoding="utf-8")

    records = yahoo._load_local_tracked_records("us_stocks", output_dir, cached=[])

    assert [(record.code, record.market, record.yahoo_symbol) for record in records] == [
        ("ACCU_DL", "us_delisted", "ACCU"),
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
    quarantine_dir = output_dir / "quarantine" / "unverified_yahoo"
    quarantined = list(quarantine_dir.glob("OLDW_features.*.parquet"))
    assert len(quarantined) == 1
    assert pl.read_parquet(quarantined[0]).to_dicts() == [{"close": 1.0}]
    events = [
        json.loads(line)
        for line in (quarantine_dir / "events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert [(event["code"], event["precheck_status"]) for event in events] == [
        ("OLDW", "delisted_no_history")
    ]


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


def test_repair_plan_removes_delisted_schema_mismatch_file(tmp_path):
    output_dir = tmp_path / "us_stocks"
    output_dir.mkdir()
    output_path = output_dir / "OLDW_features.parquet"
    _write_parquet(
        pl.DataFrame(
            {
                "date": ["2026-01-01", "2026-01-02"],
                "open": [1.0, 1.1],
                "max": [1.2, 1.2],
                "min": [0.9, 1.0],
                "close": [1.1, 1.0],
                "adjclose": [1.1, 1.0],
            }
        ),
        output_path,
    )

    record = yahoo.SymbolRecord("OLDW", "old warrant", "us_delisted", "OLDW")
    checks = yahoo._resolve_repair_plan("us_stocks", _base_args(tmp_path, asset="us_stocks"), [record], output_dir)

    assert [(check.record.code, check.status, check.repair_start_date) for check in checks] == [
        ("OLDW", "delisted_removed", None),
    ]
    assert not output_path.exists()


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
        requested_start_date="2000-01-01",
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


@pytest.mark.parametrize("mode", ["daily-update", "repair"])
def test_repair_plan_uses_checked_through_metadata_to_avoid_same_day_refetch(
    tmp_path,
    mode,
):
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
        requested_start_date="2000-01-01",
        requested_end_date="2026-06-15",
    )

    info = yahoo._load_existing_file_info(output_path)
    assert info.checked_through_date == "2026-06-15"
    assert info.requested_start_date == "2000-01-01"

    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path, mode=mode, end_date="2026-06-15"),
        [record],
        output_dir,
    )

    assert [(check.status, check.last_date, check.checked_through_date, check.repair_start_date) for check in checks] == [
        ("current", "2026-06-12", "2026-06-15", None),
    ]


def test_repair_plan_backfills_when_historical_start_was_never_checked(tmp_path):
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    output_path = output_dir / "2330_features.parquet"
    yahoo._write_feature_parquet_atomic(
        pl.DataFrame(
            {
                "date": ["2026-06-11"],
                "open": [100.0],
                "max": [102.0],
                "min": [99.0],
                "close": [101.0],
                "adjclose": [101.0],
                "Trading_Volume": [1000],
            }
        ),
        output_path,
        asset_class="tw_stocks",
        requested_end_date="2026-06-11",
    )

    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    checks = yahoo._resolve_repair_plan(
        "tw_stocks",
        _base_args(tmp_path),
        [record],
        output_dir,
    )

    assert [(check.status, check.repair_start_date, check.merge_existing) for check in checks] == [
        ("historical_start_unverified", "2000-01-01", True),
    ]


def test_chart_transport_failure_falls_back_under_same_provider_limit(tmp_path, monkeypatch):
    class RecordingLimiter:
        def __init__(self) -> None:
            self.wait_calls = 0
            self.deferred: list[float] = []

        def wait(self) -> None:
            self.wait_calls += 1

        def defer(self, seconds: float) -> None:
            self.deferred.append(seconds)

    def service_unavailable(**kwargs):
        raise HTTPError(kwargs["symbol"], 503, "Service Unavailable", None, None)

    fallback_calls = []

    def yfinance_fallback(**kwargs):
        fallback_calls.append(kwargs["symbol"])
        return pl.DataFrame(
            {
                "Date": ["2000-01-04"],
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.0],
                "Adj Close": [50.0],
                "Volume": [1000.0],
            }
        )

    monkeypatch.setattr(yahoo, "_download_yahoo_chart_frame", service_unavailable)
    monkeypatch.setattr(yahoo, "_download_yfinance_frame", yfinance_fallback)
    record = yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW")
    limiter = RecordingLimiter()

    result = yahoo._download_symbol(
        "tw_stocks",
        record,
        tmp_path,
        "2000-01-01",
        "2000-01-10",
        retries=0,
        refresh=True,
        request_rate_limiter=limiter,
    )

    assert result.status == "updated"
    assert fallback_calls == ["2330.TW"]
    assert limiter.wait_calls == 2
    assert limiter.deferred == []
    info = yahoo._load_existing_file_info(tmp_path / "2330_features.parquet")
    assert info.requested_start_date == "2000-01-01"


def test_yfinance_fallback_uses_raise_errors_single_symbol_history(monkeypatch):
    import pandas as pd
    import yfinance as yf

    calls = []

    class FakeTicker:
        def __init__(self, symbol, session=None):
            assert symbol == "2330.TW"
            assert session is None

        def history(self, **kwargs):
            calls.append(kwargs)
            return pd.DataFrame(
                {
                    "Open": [100.0],
                    "High": [101.0],
                    "Low": [99.0],
                    "Close": [100.0],
                    "Volume": [1000.0],
                },
                index=pd.DatetimeIndex(["2000-01-04"], name="Date"),
            )

    monkeypatch.setattr(yf, "Ticker", FakeTicker)

    frame = yahoo._download_yfinance_frame(
        symbol="2330.TW",
        start_date="2000-01-01",
        end_date_exclusive="2000-01-11",
        interval="1d",
        timeout=20,
    )

    assert frame.height == 1
    assert calls[0]["raise_errors"] is True
    assert calls[0]["actions"] is True


def test_chart_daily_uses_exchange_local_date_and_aligns_events(monkeypatch):
    bar_timestamp = int(
        datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc).timestamp()
    )
    event_timestamp = int(
        datetime(2026, 7, 10, 5, 0, tzinfo=timezone.utc).timestamp()
    )
    payload = _yahoo_chart_payload(
        bar_timestamp,
        meta={
            "exchangeTimezoneName": "Asia/Taipei",
            # Deliberately wrong so the test proves ZoneInfo takes precedence.
            "gmtoffset": 0,
        },
        events={
            "dividends": {
                str(event_timestamp): {
                    "date": event_timestamp,
                    "amount": 1.25,
                }
            },
            "splits": {
                str(event_timestamp): {
                    "date": event_timestamp,
                    "numerator": 2.0,
                    "denominator": 1.0,
                }
            },
        },
    )
    monkeypatch.setattr(
        yahoo,
        "_http_get_text",
        lambda *_args, **_kwargs: json.dumps(payload),
    )

    frame = yahoo._download_yahoo_chart_frame(
        symbol="2301.TW",
        start_date="2026-07-09",
        end_date_exclusive="2026-07-11",
        interval="1d",
    )

    row = frame.row(0, named=True)
    assert row["Date"] == datetime(2026, 7, 10)
    assert row["Dividends"] == 1.25
    assert row["Stock Splits"] == 2.0


def test_chart_daily_falls_back_to_gmtoffset_when_zone_name_is_invalid(
    monkeypatch,
):
    timestamp = int(
        datetime(2026, 7, 9, 16, 0, tzinfo=timezone.utc).timestamp()
    )
    payload = _yahoo_chart_payload(
        timestamp,
        meta={
            "exchangeTimezoneName": "Invalid/ExchangeZone",
            "gmtoffset": 8 * 60 * 60,
        },
    )
    monkeypatch.setattr(
        yahoo,
        "_http_get_text",
        lambda *_args, **_kwargs: json.dumps(payload),
    )

    frame = yahoo._download_yahoo_chart_frame(
        symbol="2301.TW",
        start_date="2026-07-09",
        end_date_exclusive="2026-07-11",
        interval="1d",
    )

    assert frame["Date"].to_list() == [datetime(2026, 7, 10)]


def test_chart_daily_without_timezone_meta_falls_back_to_utc(monkeypatch):
    timestamp = int(
        datetime(2026, 7, 10, 5, 30, tzinfo=timezone.utc).timestamp()
    )
    payload = _yahoo_chart_payload(timestamp)
    monkeypatch.setattr(
        yahoo,
        "_http_get_text",
        lambda *_args, **_kwargs: json.dumps(payload),
    )

    frame = yahoo._download_yahoo_chart_frame(
        symbol="UNKNOWN",
        start_date="2026-07-10",
        end_date_exclusive="2026-07-11",
        interval="1d",
    )

    assert frame["Date"].to_list() == [datetime(2026, 7, 10)]


def test_chart_intraday_preserves_exchange_local_time(monkeypatch):
    timestamp = int(
        datetime(2026, 7, 10, 1, 15, tzinfo=timezone.utc).timestamp()
    )
    payload = _yahoo_chart_payload(
        timestamp,
        meta={"exchangeTimezoneName": "Asia/Taipei", "gmtoffset": 0},
    )
    monkeypatch.setattr(
        yahoo,
        "_http_get_text",
        lambda *_args, **_kwargs: json.dumps(payload),
    )

    frame = yahoo._download_yahoo_chart_frame(
        symbol="BTC-USD",
        start_date="2026-07-10",
        end_date_exclusive="2026-07-11",
        interval="15m",
    )

    assert frame["Datetime"].to_list() == [datetime(2026, 7, 10, 9, 15)]


def test_tw_delisted_valid_empty_history_is_terminal_without_retry(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(yahoo, "_YAHOO_CHART_DISABLED_UNTIL", 0.0)
    chart_calls: list[str] = []

    def empty_chart(**kwargs):
        chart_calls.append(kwargs["symbol"])
        return pl.DataFrame()

    monkeypatch.setattr(yahoo, "_download_yahoo_chart_frame", empty_chart)
    monkeypatch.setattr(
        yahoo,
        "_download_yfinance_frame",
        lambda **_kwargs: pytest.fail("a valid chart response must not use fallback"),
    )
    monkeypatch.setattr(
        yahoo.time,
        "sleep",
        lambda _seconds: pytest.fail("a terminal delisted empty must not retry"),
    )
    blacklist: set[str] = set()
    blacklist_path = tmp_path / "yahoo_blacklist.txt"

    result = yahoo._download_symbol(
        "tw_stocks",
        yahoo.SymbolRecord("1111_TW", "old stock", "tw_delisted", "1111.TW"),
        tmp_path,
        "2000-01-01",
        "2026-06-11",
        retries=3,
        refresh=True,
        blacklist_symbols=blacklist,
        blacklist_path=blacklist_path,
    )

    assert result.status == "failed"
    assert chart_calls == ["1111.TW"]
    assert yahoo._captured_indicates_unavailable((result.message or "").lower())
    assert "valid empty history" in (result.message or "")
    assert blacklist == {"1111.TW"}
    assert blacklist_path.read_text(encoding="utf-8") == "1111.TW\n"


def test_active_tw_valid_empty_history_remains_fail_closed_and_retries(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(yahoo, "_YAHOO_CHART_DISABLED_UNTIL", 0.0)
    chart_calls: list[str] = []
    sleep_calls: list[float] = []

    def empty_chart(**kwargs):
        chart_calls.append(kwargs["symbol"])
        return pl.DataFrame()

    monkeypatch.setattr(yahoo, "_download_yahoo_chart_frame", empty_chart)
    monkeypatch.setattr(yahoo.time, "sleep", sleep_calls.append)

    result = yahoo._download_symbol(
        "tw_stocks",
        yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW"),
        tmp_path,
        "2000-01-01",
        "2026-06-11",
        retries=2,
        refresh=True,
    )

    assert result.status == "failed"
    assert chart_calls == ["2330.TW", "2330.TW", "2330.TW"]
    assert sleep_calls == [0.8, 1.6]
    assert "Yahoo returned no rows" in (result.message or "")
    assert not yahoo._captured_indicates_unavailable((result.message or "").lower())


def test_chart_rate_limit_defers_globally_before_successful_fallback(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(yahoo, "_YAHOO_CHART_DISABLED_UNTIL", 0.0)
    class RecordingLimiter:
        def __init__(self) -> None:
            self.wait_calls = 0
            self.deferred: list[float] = []

        def wait(self) -> None:
            self.wait_calls += 1

        def defer(self, seconds: float) -> None:
            self.deferred.append(seconds)

    def chart(**kwargs):
        raise HTTPError(kwargs["symbol"], 429, "Too Many Requests", None, None)

    def fallback(**_kwargs):
        return pl.DataFrame(
            {
                "Date": ["2000-01-04"],
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.0],
                "Adj Close": [50.0],
                "Volume": [1000.0],
            }
        )

    monkeypatch.setattr(yahoo, "_download_yahoo_chart_frame", chart)
    monkeypatch.setattr(yahoo, "_download_yfinance_frame", fallback)
    limiter = RecordingLimiter()

    result = yahoo._download_symbol(
        "tw_stocks",
        yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW"),
        tmp_path,
        "2000-01-01",
        "2000-01-10",
        retries=0,
        refresh=True,
        request_rate_limiter=limiter,
    )

    assert result.status == "updated"
    assert limiter.wait_calls == 2
    assert limiter.deferred == [5.0]
    assert yahoo._yahoo_chart_route_available() is False


def test_chart_rate_limit_does_not_certify_empty_fallback_as_delisted(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(yahoo, "_YAHOO_CHART_DISABLED_UNTIL", 0.0)
    class RecordingLimiter:
        def __init__(self) -> None:
            self.wait_calls = 0
            self.deferred: list[float] = []

        def wait(self) -> None:
            self.wait_calls += 1

        def defer(self, seconds: float) -> None:
            self.deferred.append(seconds)

    def rate_limited(**kwargs):
        raise HTTPError(kwargs["symbol"], 429, "Too Many Requests", None, None)

    def false_delisted(**_kwargs):
        print("possibly delisted; no timezone found")
        return pl.DataFrame()

    monkeypatch.setattr(yahoo, "_download_yahoo_chart_frame", rate_limited)
    monkeypatch.setattr(yahoo, "_download_yfinance_frame", false_delisted)
    limiter = RecordingLimiter()
    blacklist: set[str] = set()

    result = yahoo._download_symbol(
        "tw_stocks",
        yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW"),
        tmp_path,
        "2000-01-01",
        "2000-01-10",
        retries=0,
        refresh=True,
        blacklist_symbols=blacklist,
        request_rate_limiter=limiter,
    )

    assert result.status == "failed"
    assert "429" in str(result.message)
    assert blacklist == set()
    assert limiter.wait_calls == 2
    assert limiter.deferred == [5.0]


def test_chart_circuit_breaker_routes_next_request_directly_to_yfinance(
    tmp_path,
    monkeypatch,
):
    class RecordingLimiter:
        def __init__(self) -> None:
            self.wait_calls = 0

        def wait(self) -> None:
            self.wait_calls += 1

        def defer(self, _seconds: float) -> None:
            pytest.fail("an already-open chart circuit must not add another defer")

    monkeypatch.setattr(
        yahoo,
        "_YAHOO_CHART_DISABLED_UNTIL",
        yahoo.time.monotonic() + 60.0,
    )
    monkeypatch.setattr(
        yahoo,
        "_download_yahoo_chart_frame",
        lambda **_kwargs: pytest.fail("open chart circuit must skip the chart route"),
    )
    monkeypatch.setattr(
        yahoo,
        "_download_yfinance_frame",
        lambda **_kwargs: pl.DataFrame(
            {
                "Date": ["2000-01-04"],
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.0],
                "Adj Close": [50.0],
                "Volume": [1000.0],
            }
        ),
    )
    limiter = RecordingLimiter()

    result = yahoo._download_symbol(
        "tw_stocks",
        yahoo.SymbolRecord("2330", "TSMC", "tw_stocks", "2330.TW"),
        tmp_path,
        "2000-01-01",
        "2000-01-10",
        retries=0,
        refresh=True,
        request_rate_limiter=limiter,
    )

    assert result.status == "updated"
    assert limiter.wait_calls == 1
