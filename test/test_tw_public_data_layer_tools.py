from __future__ import annotations

import hashlib
import json
import math
import os
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

import downloader.download_tw_public_data as twpub

from scripts.audit_tw_public_data_layer import (
    _audit_tpex_daily_name_provenance,
    _benchmark_sessions,
    audit_delisted_universe_coverage,
    audit_feature_lineage_registry,
    audit_historical_sources,
    audit_non_vintage_archive_contract,
    audit_official_symbol_build,
    audit_panel_contract,
    audit_quote_source_files,
    audit_return_price_provenance,
    audit_source_receipts,
    audit_snapshot_contract,
    audit_walk_forward_availability,
)
from scripts.rebuild_tw_public_data_layer import (
    RebuildRunner,
    _daily_yahoo_refresh_symbols,
    _default_tw_end_date,
    _file_receipt as _runner_file_receipt,
    _promote_one,
    _retained_daily_yahoo_symbols,
    _rollback_promoted_tree,
    _validate_official_symbol_build_summary,
)
from scripts.build_tw_official_symbol_parquets import (
    LIFECYCLE_EVIDENCE_FILENAMES,
    MIXED_FALLBACK_SOURCE_NAME,
    OFFICIAL_CORE_SOURCE_FILENAMES,
    OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD,
    OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION,
    _receipt,
    _write_official_quote_parquet,
)
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_public_features import _file_content_receipt


def test_daily_yahoo_refresh_symbols_extracts_only_actionable_gaps(tmp_path: Path) -> None:
    summary = tmp_path / "transfer.summary.json"
    summary.write_text(
        json.dumps(
            {
                "unresolved": [
                    {
                        "symbol": "8926",
                        "error": "TransferAdjustmentError: Yahoo source metadata is not coverage-eligible for 8926",
                    },
                    {
                        "symbol": "4526",
                        "error": "TransferAdjustmentError: Yahoo source metadata is not coverage-eligible for 4526",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    assert _daily_yahoo_refresh_symbols(summary) == ["4526", "8926"]


def test_daily_yahoo_refresh_symbols_rejects_non_yahoo_failures(tmp_path: Path) -> None:
    summary = tmp_path / "transfer.summary.json"
    summary.write_text(
        json.dumps({"unresolved": [{"symbol": "2330", "error": "bad official row"}]}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="non-Yahoo failure"):
        _daily_yahoo_refresh_symbols(summary)


def test_default_tw_end_date_uses_last_completed_session() -> None:
    before_open = datetime.fromisoformat("2026-07-15T01:00:00+08:00")
    after_close = datetime.fromisoformat("2026-07-15T14:00:00+08:00")
    after_data_ready = datetime.fromisoformat("2026-07-15T18:01:00+08:00")
    monday_before_open = datetime.fromisoformat("2026-07-13T10:00:00+08:00")
    assert _default_tw_end_date(before_open) == "2026-07-14"
    assert _default_tw_end_date(after_close) == "2026-07-15"
    assert _default_tw_end_date(after_data_ready) == "2026-07-15"
    assert _default_tw_end_date(monday_before_open) == "2026-07-10"


def test_retained_daily_yahoo_symbols_keeps_archive_and_missing_adjustments(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "fallback.parquet"
    stock_root = tmp_path / "stocks"
    stock_root.mkdir()
    pl.DataFrame({"symbol": ["4526", "8926"]}).write_parquet(archive)
    (stock_root / "official_symbol_build_summary.json").write_text(
        json.dumps({"missing_adjustment_rows": 1}),
        encoding="utf-8",
    )
    pl.DataFrame({"adjclose": [10.0, float("nan")]}).write_parquet(
        stock_root / "3303_features.parquet"
    )

    assert _retained_daily_yahoo_symbols(
        archive_path=archive,
        stock_root=stock_root,
    ) == ["3303", "4526", "8926"]


def _panel(dates: list[str]) -> PanelData:
    rows = len(dates)
    tradable = np.ones((rows, 1), dtype=bool)
    returns = np.zeros((rows, 1), dtype=np.float32)
    if rows > 1:
        returns[0, 0] = np.float32(math.log(1.01))
    return PanelData(
        dates=np.asarray(dates, dtype="datetime64[ns]"),
        symbols=["2330"],
        feature_names=["body_ratio"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=returns,
        tradable_mask=tradable,
        can_buy_mask=tradable.copy(),
        can_sell_mask=tradable.copy(),
        can_short_open_mask=tradable.copy(),
        force_short_cover_mask=np.zeros_like(tradable),
        force_exit_mask=np.zeros_like(tradable),
        alive_mask=tradable.copy(),
        benchmark_returns=returns[:, 0].copy(),
        close_prices=np.full((rows, 1), 100.0, dtype=np.float32),
        daily_volumes=np.full((rows, 1), 1000.0, dtype=np.float32),
    )


def _write_verified_taiex_calendar(
    public_dir: Path,
    sessions: list[date],
    *,
    effective_end: date | None = None,
) -> str:
    dates = sorted({date(1999, 1, 5), *sessions})
    path = public_dir / "twse_taiex_ohlc.parquet"
    pl.DataFrame(
        {
            "date": dates,
            "opening_index": [100.0] * len(dates),
            "highest_index": [101.0] * len(dates),
            "lowest_index": [99.0] * len(dates),
            "closing_index": [100.5] * len(dates),
            "_dataset": ["twse_taiex_ohlc"] * len(dates),
            "_source": ["TWSE"] * len(dates),
            "_source_product": ["indicesReport/MI_5MINS_HIST"] * len(dates),
            "_request_month": [value.strftime("%Y-%m") for value in dates],
            "_downloaded_at_utc": ["2026-07-12T00:00:00+00:00"] * len(dates),
            "_url": ["https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"]
            * len(dates),
        }
    ).write_parquet(path)
    receipt = _file_content_receipt(path)
    receipt["path"] = path.name
    (public_dir / "twse_taiex_ohlc.summary.json").write_text(
        json.dumps(
            {
                "schema_version": twpub.TAIEX_SESSION_CALENDAR_SUMMARY_SCHEMA_VERSION,
                "dataset": "twse_taiex_ohlc",
                "source": "TWSE",
                "official_start_date": "1999-01-05",
                "effective_start_date": "1999-01-05",
                "effective_end_date": str(effective_end or dates[-1]),
                "canonical_path": path.name,
                "coverage_complete": True,
                "baseline_established": True,
                "replacement_promoted": True,
                "failed_count": 0,
                "unresolved_month_count": 0,
                "output_rows": len(dates),
                "output_receipt": receipt,
            }
        ),
        encoding="utf-8",
    )
    return str(receipt["sha256"])


def test_snapshot_only_cumulative_revenue_is_quarantined() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    findings = audit_snapshot_contract(Path("/does/not/need/to/exist"), config)
    cumulative = next(
        item for item in findings if item.item == "twpub_cumulative_revenue_yoy"
    )
    assert cumulative.severity == "low"
    assert "all_zero_filled=True" in cumulative.evidence

    config.data.feature_zero_fill = [
        pattern
        for pattern in config.data.feature_zero_fill
        if pattern != "twpub_cumulative_revenue_yoy"
    ]
    findings = audit_snapshot_contract(Path("/does/not/need/to/exist"), config)
    cumulative = next(
        item for item in findings if item.item == "twpub_cumulative_revenue_yoy"
    )
    assert cumulative.severity == "critical"


def test_single_vintage_macro_archives_are_quarantined() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    findings = audit_non_vintage_archive_contract(Path("/not/required"), config)
    dgbas = next(item for item in findings if item.item == "twpub_dgbas_*")
    mof = next(item for item in findings if item.item == "twpub_mof_*")
    assert dgbas.severity == "low"
    assert mof.severity == "low"
    assert audit_feature_lineage_registry(config) == []

    config.data.feature_zero_fill = [
        pattern for pattern in config.data.feature_zero_fill if pattern != "twpub_dgbas_*"
    ]
    findings = audit_non_vintage_archive_contract(Path("/not/required"), config)
    dgbas = next(item for item in findings if item.item == "twpub_dgbas_*")
    assert dgbas.severity == "critical"


def test_zero_filled_unregistered_feature_has_no_effective_lineage() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    assert audit_feature_lineage_registry(config) == []

    config.data.feature_zero_fill = [
        pattern
        for pattern in config.data.feature_zero_fill
        if pattern != "twpub_cbc_*"
    ]

    findings = audit_feature_lineage_registry(config)
    assert len(findings) == 1
    assert findings[0].severity == "critical"
    assert "twpub_cbc_overnight_rate" in findings[0].evidence


def test_rule_receipt_is_required_and_machine_checked(tmp_path: Path) -> None:
    config = load_config("configs/markets/tw_public.yaml")
    (tmp_path / "download_summary.json").write_text(
        json.dumps({"mode": "full", "failed_count": 0}),
        encoding="utf-8",
    )

    _, findings = audit_source_receipts(tmp_path, config)
    assert any(item.code == "missing_short_rule_receipt" for item in findings)

    (tmp_path / "tw_short_sale_download_report.json").write_text(
        json.dumps(
            {
                "requests_complete": True,
                "failure_count": 0,
                "unparseable": 0,
                "data_output_written": True,
                "archive_cohort_coverage_complete": True,
                "data_quality": {
                    "rows": 2,
                    "symbols_nonempty_rows": 2,
                    "composite_key_duplicate_rows": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    taiex_path = tmp_path / "twse_taiex_ohlc.parquet"
    pl.DataFrame(
        {
            "date": [date(1999, 1, 5)],
            "opening_index": [6407.03],
            "highest_index": [6421.30],
            "lowest_index": [6376.55],
            "closing_index": [6421.30],
            "_dataset": ["twse_taiex_ohlc"],
            "_source": ["TWSE"],
            "_source_product": ["indicesReport/MI_5MINS_HIST"],
            "_request_month": ["1999-01"],
            "_downloaded_at_utc": ["2026-07-11T00:00:00+00:00"],
            "_url": ["https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"],
        }
    ).write_parquet(taiex_path)
    taiex_receipt = _file_content_receipt(taiex_path)
    (tmp_path / "twse_taiex_ohlc.summary.json").write_text(
        json.dumps(
            {
                "dataset": "twse_taiex_ohlc",
                "source": "TWSE",
                "official_start_date": "1999-01-05",
                "effective_start_date": "1999-01-05",
                "effective_end_date": "1999-01-05",
                "coverage_complete": True,
                "baseline_established": True,
                "replacement_promoted": True,
                "failed_count": 0,
                "unresolved_month_count": 0,
                "output_rows": 1,
                "output_receipt": {
                    "path": taiex_path.name,
                    "size": taiex_receipt["size"],
                    "sha256": taiex_receipt["sha256"],
                },
            }
        ),
        encoding="utf-8",
    )
    _, findings = audit_source_receipts(tmp_path, config)
    assert findings == []


def test_tpex_lossy_receipt_requires_every_name_row_to_be_marked(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "tpex_daily_ohlcv"
    raw_dir.mkdir(parents=True)
    (raw_dir / "2004-03-04.html").write_bytes(
        b"official-prefix" + bytes.fromhex("efbfbd") + b"official-suffix"
    )
    path = tmp_path / "tpex_daily_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": ["2004-03-04", "2004-03-04"],
            "代號": ["3067", "5351"],
            "名稱": ["嚙踝蕭嚙踝蕭", "鈺喉蕭"],
            "_name_decode_status": [
                "official_receipt_name_bytes_unrecoverable",
                "official_receipt_name_bytes_unrecoverable",
            ],
        }
    ).write_parquet(path)

    summary, findings = _audit_tpex_daily_name_provenance(tmp_path)

    assert findings == []
    assert summary["lossy_receipt_dates"] == 1
    assert summary["unrecoverable_name_rows"] == 2

    pl.DataFrame(
        {
            "date": ["2004-03-04", "2004-03-04"],
            "代號": ["3067", "5351"],
            "名稱": ["嚙踝蕭嚙踝蕭", "鈺喉蕭"],
            "_name_decode_status": [
                "official_receipt_name_bytes_unrecoverable",
                "",
            ],
        }
    ).write_parquet(path)

    summary, findings = _audit_tpex_daily_name_provenance(tmp_path)

    assert summary["missing_or_partial_status_dates"] == ["2004-03-04"]
    assert any(item.code == "tpex_name_provenance_mismatch" for item in findings)


def test_tpex_legacy_json_name_damage_reconciles_exact_symbol_rows(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "tpex_daily_ohlcv"
    raw_dir.mkdir(parents=True)
    raw_html = (
        "<html><tr><td>6174</td><td>安��</td><td>13.85</td><td>+0.00</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr>"
        "<tr><td>3087</td><td>翔準</td><td>20.00</td><td>+0.10</td>"
        "<td>19.90</td><td>20.10</td><td>19.80</td><td>20.00</td>"
        "<td>1,000</td><td>20,000</td><td>10</td><td>19.90</td>"
        "<td>20.00</td><td></td><td></td><td></td><td></td></tr></html>"
    )
    (raw_dir / "2007-01-02.json").write_text(
        json.dumps(
            {"html": raw_html, "date": "20070102", "stat": "ok"},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    path = tmp_path / "tpex_daily_ohlcv.parquet"
    expected_status = "official_receipt_name_bytes_unrecoverable"

    def write_statuses(damaged: str, clean: str) -> None:
        pl.DataFrame(
            {
                "date": ["2007-01-02", "2007-01-02"],
                "代號": ["6174", "3087"],
                "名稱": ["安��", "翔準"],
                "_name_decode_status": [damaged, clean],
            }
        ).write_parquet(path)

    write_statuses(expected_status, "")
    summary, findings = _audit_tpex_daily_name_provenance(tmp_path)

    assert findings == []
    assert summary["lossy_receipt_dates"] == 0
    assert summary["row_damage_evidence_dates"] == 1
    assert summary["row_damage_evidence_rows"] == 1
    assert summary["unrecoverable_name_rows"] == 1

    write_statuses("", "")
    summary, findings = _audit_tpex_daily_name_provenance(tmp_path)

    assert summary["missing_row_damage_status_keys"] == [
        {"date": "2007-01-02", "symbol": "6174"}
    ]
    assert any(item.code == "tpex_name_provenance_mismatch" for item in findings)

    write_statuses(expected_status, expected_status)
    summary, findings = _audit_tpex_daily_name_provenance(tmp_path)

    assert summary["status_without_raw_evidence_keys"] == [
        {"date": "2007-01-02", "symbol": "3087"}
    ]
    assert any(item.code == "tpex_name_provenance_mismatch" for item in findings)


def test_historical_source_audit_separates_observed_and_receipt_resolved_sessions(
    tmp_path: Path,
) -> None:
    config = load_config("configs/markets/tw_public.yaml")
    expected = np.asarray(
        ["2007-06-01", "2007-06-04", "2007-06-05"],
        dtype="datetime64[D]",
    )
    pl.DataFrame(
        {
            "date": ["2007-06-01", "2007-06-04"],
            "代號": ["1234", "1234"],
        }
    ).write_parquet(tmp_path / "tpex_margin_balance.parquet")
    taiex_sha256 = _write_verified_taiex_calendar(
        tmp_path,
        [date(2007, 6, 1), date(2007, 6, 4), date(2007, 6, 5)],
    )
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    unavailable_day = date(2007, 6, 5)
    raw_content = json.dumps(
        {"stat": "很抱歉，沒有符合條件的資料!"},
        ensure_ascii=False,
    ).encode("utf-8")
    raw_sha256 = hashlib.sha256(raw_content).hexdigest()
    raw_path = (
        tmp_path
        / "raw_empty"
        / spec.name
        / f"{unavailable_day.isoformat()}.{raw_sha256[:16]}.json"
    )
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(raw_content)
    journal_path = twpub._historical_journal_path(tmp_path, spec)
    journal_path.parent.mkdir(parents=True)
    request_url, _ = twpub._historical_request_info(spec, unavailable_day)
    journal_path.write_text(
        json.dumps(
            {
                "schema_version": twpub.HISTORICAL_JOURNAL_SCHEMA_VERSION,
                "cache_key": twpub._historical_resume_cache_key(spec),
                "dataset": spec.name,
                "date": unavailable_day.isoformat(),
                "status": "empty",
                "source": "network",
                "url": request_url,
                "rows": 0,
                "raw_path": str(raw_path.relative_to(tmp_path)),
                "raw_size": len(raw_content),
                "raw_sha256": raw_sha256,
                "http_status": 200,
                "content_type": "application/json",
                "content_length": len(raw_content),
                "body_sha256": raw_sha256,
                "source_unavailable_reason": "official_endpoint_archive_gap",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    state_path = state_dir / "tpex_margin_balance.json"
    state = {
        "dataset": "tpex_margin_balance",
        "baseline_established": True,
        "coverage_complete": True,
        "replacement_promoted": True,
        "failed_dates": {},
        "missing_dates_after": 0,
        "coverage_calendar_kind": "validated_official_open_sessions",
        "coverage_calendar_source": twpub.TPEX_OFFICIAL_CALENDAR_DATASET,
        "root_coverage_calendar_source": twpub.TAIEX_SESSION_CALENDAR_DATASET,
        "root_coverage_calendar_sha256": taiex_sha256,
        "coverage_start": "2003-08-01",
        "coverage_end": "2026-07-11",
        "confirmed_empty_dates": ["2007-06-05"],
        "confirmed_source_unavailable_dates": ["2007-06-05"],
        "confirmed_empty_date_accounting": {
            "source_unavailable": 1,
            "other_confirmed_no_data": 0,
            "total": 1,
        },
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")

    profiles, findings = audit_historical_sources(tmp_path, expected, config)
    profile = next(item for item in profiles if item.source == "tpex_margin_balance")
    assert profile.covered_sessions == 2
    assert profile.session_coverage == pytest.approx(2 / 3)
    assert profile.observed_sessions == 2
    assert profile.observed_session_coverage == pytest.approx(2 / 3)
    assert profile.source_unavailable_sessions == 1
    assert profile.resolved_sessions == 3
    assert profile.resolved_session_coverage == 1.0
    assert profile.status == "complete"
    assert not any(
        item.code == "historical_session_coverage"
        and item.item == "tpex_margin_balance"
        for item in findings
    )

    raw_path.write_bytes(raw_content + b" ")
    profiles, findings = audit_historical_sources(tmp_path, expected, config)
    profile = next(item for item in profiles if item.source == "tpex_margin_balance")
    assert profile.source_unavailable_sessions == 0
    assert profile.resolved_sessions == 2
    assert profile.resolved_session_coverage == pytest.approx(2 / 3)
    assert profile.status == "incomplete"
    assert any(
        item.code == "historical_session_coverage"
        and item.item == "tpex_margin_balance"
        for item in findings
    )


def test_panel_contract_accepts_zero_terminal_label_and_valid_destination() -> None:
    panel = _panel(["2024-01-02", "2024-01-03"])
    summary, findings = audit_panel_contract(
        panel,
        np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    assert findings == []
    assert summary["destination_missing_quote_label_violations"] == 0
    assert summary["terminal_nonzero_labels"] == 0


def test_panel_contract_rejects_non_benchmark_time_axis_rows() -> None:
    panel = _panel(["2024-01-02", "2024-01-03", "2024-01-04"])
    _, findings = audit_panel_contract(
        panel,
        np.asarray(["2024-01-02", "2024-01-04"], dtype="datetime64[D]"),
    )

    assert any(item.code == "extra_non_benchmark_dates" for item in findings)


def test_delisted_universe_requires_pre_delisting_history(tmp_path: Path) -> None:
    parquet_root = tmp_path / "stocks"
    public_dir = tmp_path / "public"
    parquet_root.mkdir()
    public_dir.mkdir()
    pl.DataFrame(
        {
            "symbol": ["1111", "2222"],
            "date": [date(2024, 6, 1), date(2024, 6, 1)],
            "company_name": ["covered", "relisted"],
        }
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    pl.DataFrame(
        schema={"symbol": pl.String, "date": pl.Date, "company_name": pl.String}
    ).write_parquet(public_dir / "tpex_delisted_company.parquet")
    for symbol, history_date in (("1111", date(2024, 5, 31)), ("2222", date(2025, 1, 2))):
        pl.DataFrame({"date": [history_date], "close": [10.0]}).write_parquet(
            parquet_root / f"{symbol}_features.parquet"
        )

    profiles, missing, findings = audit_delisted_universe_coverage(
        parquet_root,
        public_dir,
        np.asarray(["2024-01-02", "2025-12-31"], dtype="datetime64[D]"),
    )

    twse = next(item for item in profiles if item["market"] == "twse")
    assert twse["canonical_symbol_histories"] == 1
    assert twse["missing_symbol_histories"] == 1
    assert missing[0]["symbol"] == "2222"
    assert missing[0]["missing_reason"] == "no_history_on_or_before_delisting"
    assert any(item.code == "delisted_universe_coverage" for item in findings)


def test_delisted_universe_ignores_terminal_cohort_before_declared_panel_start(
    tmp_path: Path,
) -> None:
    parquet_root = tmp_path / "stocks"
    public_dir = tmp_path / "public"
    parquet_root.mkdir()
    public_dir.mkdir()
    pl.DataFrame(
        {
            "symbol": ["1111", "2222"],
            "date": [date(2004, 4, 28), date(2005, 6, 1)],
            "company_name": ["pre-horizon", "in-horizon"],
        }
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    pl.DataFrame(
        schema={"symbol": pl.String, "date": pl.Date, "company_name": pl.String}
    ).write_parquet(public_dir / "tpex_delisted_company.parquet")
    pl.DataFrame({"date": [date(2005, 5, 31)], "close": [10.0]}).write_parquet(
        parquet_root / "2222_features.parquet"
    )

    profiles, missing, findings = audit_delisted_universe_coverage(
        parquet_root,
        public_dir,
        np.asarray(["2005-01-03", "2006-12-29"], dtype="datetime64[D]"),
    )

    twse = next(item for item in profiles if item["market"] == "twse")
    assert twse["official_delisted_symbols"] == 1
    assert twse["canonical_symbol_histories"] == 1
    assert twse["missing_symbol_histories"] == 0
    assert missing == []
    assert not any(item.code == "delisted_universe_coverage" for item in findings)


def test_quote_source_audit_detects_duplicates_and_impossible_bars(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 2)],
            "open": [10.0, 10.0],
            "max": [11.0, 9.0],
            "min": [9.0, 8.0],
            "close": [10.5, 10.0],
            "adjclose": [10.0, 10.1],
            "Trading_Volume": [1000.0, -1.0],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    profiles, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02"], dtype="datetime64[D]"),
        workers=1,
    )

    assert profiles[0]["duplicate_dates"] == 1
    assert summary["invalid_ohlc_geometry"] == 1
    assert summary["negative_volume"] == 1
    codes = {item.code for item in findings}
    assert {"duplicate_quote_dates", "invalid_ohlc_geometry", "negative_quote_volume"} <= codes


def test_quote_source_audit_separates_raw_corporate_actions_from_adjusted_jumps(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "open": [10.0, 1.0, 1.0],
            "max": [10.0, 1.0, 1.0],
            "min": [10.0, 1.0, 1.0],
            "close": [10.0, 1.0, 1.0],
            "adjclose": [10.0, 10.1, 40.4],
            "Trading_Volume": [1000.0, 1000.0, 1000.0],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02", "2024-01-03", "2024-01-04"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["raw_close_jumps_gt_2x"] == 1
    assert summary["adjusted_index_jumps_gt_3x"] == 1
    assert summary["quarantined_adjusted_index_jumps_gt_3x"] == 0
    assert summary["unresolved_adjusted_index_jumps_gt_3x"] == 1
    assert any(item.code == "extreme_adjusted_index_jump" for item in findings)
    assert not any(item.code == "extreme_source_price_jump" for item in findings)


def test_quote_source_audit_separates_quarantined_from_unresolved_extremes(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "open": [10.0, 10.0, 40.0],
            "max": [10.0, 10.0, 40.0],
            "min": [10.0, 10.0, 40.0],
            "close": [10.0, 10.0, 40.0],
            "adjclose": [10.0, 10.1, 40.4],
            "Trading_Volume": [1000.0, 1000.0, 1000.0],
            "return_quarantined": [False, True, False],
            "return_quarantine_reason": [
                None,
                "unverified_extreme_adjusted_return",
                None,
            ],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(
            ["2024-01-02", "2024-01-03", "2024-01-04"],
            dtype="datetime64[D]",
        ),
        workers=1,
    )

    assert summary["adjusted_index_jumps_gt_3x"] == 1
    assert summary["quarantined_adjusted_index_jumps_gt_3x"] == 1
    assert summary["unresolved_adjusted_index_jumps_gt_3x"] == 0
    assert summary["return_quarantined_rows"] == 1
    assert not any(item.code == "extreme_adjusted_index_jump" for item in findings)
    assert any(
        item.code == "quarantined_extreme_adjusted_index_jump"
        for item in findings
    )


def test_quote_source_audit_rejects_whitelisted_reason_without_destination_evidence(
    tmp_path: Path,
) -> None:
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
                "official_listing_boundary",
                None,
            ],
            "lifecycle_episode_id": [0, 0],
            "official_listing_evidence": [None, None],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["return_quarantine_evidence_mismatch"] == 1
    assert summary["quarantined_adjusted_index_jumps_gt_3x"] == 0
    assert summary["unresolved_adjusted_index_jumps_gt_3x"] == 1
    assert any(
        item.code == "return_quarantine_evidence_mismatch" for item in findings
    )
    assert any(item.code == "extreme_adjusted_index_jump" for item in findings)


@pytest.mark.parametrize(
    ("return_quarantined", "reason", "expected_mismatches"),
    [
        (True, "official_listing_boundary", 0),
        (False, None, 1),
    ],
)
def test_quote_source_audit_requires_listing_boundary_source_label_quarantine(
    tmp_path: Path,
    return_quarantined: bool,
    reason: str | None,
    expected_mismatches: int,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "open": [10.0, 11.0],
            "max": [10.0, 11.0],
            "min": [10.0, 11.0],
            "close": [10.0, 11.0],
            "adjclose": [10.0, 11.0],
            "Trading_Volume": [1000.0, 1000.0],
            "return_quarantined": [return_quarantined, False],
            "return_quarantine_reason": [reason, None],
            "lifecycle_episode_id": [0, 0],
            "official_listing_evidence": [
                None,
                "twse_listed_company_basic@2024-01-03",
            ],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    _, summary, _ = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["return_quarantine_evidence_mismatch"] == expected_mismatches


def test_quote_source_audit_rejects_non_stock_non_etf_security(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "open": [10.0],
            "max": [10.0],
            "min": [10.0],
            "close": [10.0],
            "adjclose": [10.0],
            "Trading_Volume": [1000.0],
        }
    ).write_parquet(tmp_path / "01001T_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["unsupported_security_files"] == 1
    assert any(item.code == "unsupported_tw_security_type" for item in findings)


def test_quote_source_audit_accepts_audited_yahoo_fallback_lineage(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2004, 2, 11)],
            "open": [10.0, 20.0],
            "max": [10.5, 21.0],
            "min": [9.5, 19.0],
            "close": [10.0, 20.0],
            "Trading_Volume": [1000.0, 2000.0],
            "data_source": ["yahoo_fallback", "twse_official"],
            "adjustment_source": ["yahoo_fallback", "twse_official"],
            "fallback_reason": ["official_ohlcv_unusable", None],
            "ohlc_normalization": [None, None],
            "adjclose": [10.0, 10.5],
        }
    )
    _write_official_quote_parquet(
        frame,
        tmp_path / "2330_features.parquet",
        checked_through="2004-02-11",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2000-01-04", "2004-02-11"], dtype="datetime64[D]"),
        workers=1,
        require_official=True,
    )

    assert summary["unapproved_source_files"] == 0
    assert summary["yahoo_fallback_rows"] == 1
    assert summary["yahoo_fallback_adjustment_rows"] == 1
    assert summary["source_lineage_mismatch"] == 0
    assert summary["fallback_reason_rows"] == 1
    assert summary["fallback_reason_lineage_mismatch"] == 0
    assert summary["unapproved_ohlc_normalization_rows"] == 0
    assert not any("source" in finding.code for finding in findings)


def test_quote_source_audit_keeps_pre_horizon_rows_in_full_source_calendar(
    tmp_path: Path,
) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2004, 12, 31), date(2005, 1, 3)],
            "open": [10.0, 10.1],
            "max": [10.0, 10.1],
            "min": [10.0, 10.1],
            "close": [10.0, 10.1],
            "Trading_Volume": [1000.0, 1000.0],
            "data_source": ["twse_official", "twse_official"],
            "adjustment_source": ["twse_official", "twse_official"],
            "fallback_reason": [None, None],
            "ohlc_normalization": [None, None],
            "adjclose": [10.0, 10.1],
        }
    )
    _write_official_quote_parquet(
        frame,
        tmp_path / "2330_features.parquet",
        checked_through="2005-01-03",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2005-01-03"], dtype="datetime64[D]"),
        source_sessions=np.asarray(
            ["2004-12-31", "2005-01-03"], dtype="datetime64[D]"
        ),
        workers=1,
        require_official=True,
    )

    assert summary["audited_rows"] == 1
    assert summary["outside_panel_range_rows"] == 1
    assert summary["off_calendar_rows"] == 0
    assert not any(item.code == "off_calendar_quote_rows" for item in findings)


def test_audit_uses_taiex_calendar_and_rejects_canonical_holiday_rows(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "data_tw_public"
    parquet_root = public_dir / "stocks"
    parquet_root.mkdir(parents=True)
    _write_verified_taiex_calendar(
        public_dir,
        [date(2026, 7, 9)],
        effective_end=date(2026, 7, 10),
    )
    _write_official_quote_parquet(
        pl.DataFrame(
            {
                "date": [date(2026, 7, 9), date(2026, 7, 10)],
                "open": [2410.0, 2415.0],
                "max": [2430.0, 2415.0],
                "min": [2400.0, 2415.0],
                "close": [2415.0, 2415.0],
                "adjclose": [10.0, 10.0],
                "Trading_Volume": [1000.0, 0.0],
                "data_source": ["twse_official", "yahoo_fallback"],
                "adjustment_source": ["twse_official", "yahoo_fallback"],
                "fallback_reason": [None, None],
                "ohlc_normalization": [None, None],
            }
        ),
        parquet_root / "2330_features.parquet",
        checked_through="2026-07-10",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )

    sessions = _benchmark_sessions(
        parquet_root,
        "2330",
        public_dir / "features" / "tw_public_stock_daily.parquet",
        public_dir,
        2026,
    )
    _, summary, findings = audit_quote_source_files(
        parquet_root,
        sessions,
        workers=1,
        require_official=True,
    )

    assert sessions.astype(str).tolist() == ["2026-07-09"]
    assert summary["off_calendar_rows"] == 1
    finding = next(item for item in findings if item.code == "off_calendar_quote_rows")
    assert finding.severity == "critical"


def test_official_symbol_build_audit_accepts_relocated_yahoo_receipt(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "data_tw_public"
    parquet_root = public_dir / "stocks"
    fallback_dir = public_dir / "fallback"
    parquet_root.mkdir(parents=True)
    fallback_dir.mkdir()
    for name in (*OFFICIAL_CORE_SOURCE_FILENAMES, *LIFECYCLE_EVIDENCE_FILENAMES):
        pl.DataFrame({"marker": [name]}).write_parquet(public_dir / name)
    _write_verified_taiex_calendar(public_dir, [date(2000, 1, 4)])

    fallback = fallback_dir / "yahoo_tw_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4)],
            "symbol": ["2330"],
            "name": ["TSMC"],
            "market": ["twse"],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.0],
            "Trading_Volume": [1000.0],
            "source_adjclose": [50.0],
            "source_factor": [None],
            "quote_source": ["yahoo_fallback"],
        }
    ).write_parquet(fallback)
    fallback_receipt = _receipt(fallback)
    stale_fallback_receipt = {
        **fallback_receipt,
        "path": "/old/staged/data_tw_public/fallback/yahoo_tw_ohlcv.parquet",
    }
    yahoo_input_dir = fallback_dir / "yahoo_tw_stocks"
    yahoo_input_dir.mkdir()
    yahoo_input = yahoo_input_dir / "2330_features.parquet"
    yahoo_table = pl.DataFrame(
        {
            "date": [date(2000, 1, 4)],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.0],
            "adjclose": [50.0],
            "Trading_Volume": [1000.0],
        }
    ).to_arrow()
    yahoo_table = yahoo_table.replace_schema_metadata(
        {
            b"stockagent.source": b"yahoo",
            b"stockagent.asset_class": b"tw_stocks",
            b"stockagent.yahoo_requested_start": b"2000-01-01",
            b"stockagent.yahoo_checked_through": b"2000-01-04",
        }
    )
    pq.write_table(yahoo_table, yahoo_input)
    yahoo_symbols = yahoo_input_dir / "symbols.csv"
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "yahoo_symbol": ["2330.TW"],
        }
    ).write_csv(yahoo_symbols)
    yahoo_input_receipt = {
        **_receipt(yahoo_input),
        "path": "/old/staged/data_tw_public/fallback/yahoo_tw_stocks/2330_features.parquet",
        "symbol": "2330",
        "venue": None,
        "source": "yahoo",
        "asset_class": "tw_stocks",
        "requested_start": "2000-01-01",
        "checked_through": "2000-01-04",
    }
    input_manifest_path = fallback.with_suffix(".inputs.json")
    input_manifest = {
        "schema_version": 1,
        "source": "yahoo",
        "start_date": "2000-01-01",
        "end_date": "2000-01-04",
        "file_count": 1,
        "files": [yahoo_input_receipt],
        "manifest_symbol_count": 1,
        "manifest_record_count": 1,
        "terminal_unavailable_codes": [],
        "symbol_manifest_receipt": {
            **_receipt(yahoo_symbols),
            "path": "/old/staged/data_tw_public/fallback/yahoo_tw_stocks/symbols.csv",
        },
        "terminal_report_receipt": None,
    }
    input_manifest_path.write_text(json.dumps(input_manifest), encoding="utf-8")
    fallback.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source": "yahoo_fallback",
                "input_dir": "/old/staged/data_tw_public/fallback/yahoo_tw_stocks",
                "start_date": "2000-01-01",
                "end_date": "2000-01-04",
                "source_symbol_count": 1,
                "source_file_count": 1,
                "verified_source_file_count": 1,
                "manifest_symbol_count": 1,
                "manifest_record_count": 1,
                "terminal_unavailable_record_count": 0,
                "symbol_accounting_complete": True,
                "ok_symbol_count": 1,
                "empty_symbol_count": 0,
                "failed_symbol_count": 0,
                "coverage_receipts_complete": True,
                "input_manifest_path": "/old/staged/data_tw_public/fallback/yahoo_tw_ohlcv.inputs.json",
                "input_manifest_receipt": _receipt(input_manifest_path),
                "output_receipt": stale_fallback_receipt,
            }
        ),
        encoding="utf-8",
    )

    _write_official_quote_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4)],
                "open": [100.0],
                "max": [101.0],
                "min": [99.0],
                "close": [100.0],
                "Trading_Volume": [1000.0],
                "data_source": ["yahoo_fallback"],
                "adjustment_source": ["yahoo_fallback"],
                "fallback_reason": ["official_ohlcv_unusable"],
                "ohlc_normalization": [None],
                "adjclose": [10.0],
                "return_quarantined": [False],
                "return_quarantine_reason": [None],
                "lifecycle_episode_id": [0],
                "official_listing_evidence": [None],
            }
        ),
        parquet_root / "2330_features.parquet",
        checked_through="2000-01-04",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "market": ["twse"],
            "security_type": ["stock"],
            "source": [MIXED_FALLBACK_SOURCE_NAME],
        }
    ).write_csv(parquet_root / "symbols.csv")
    source_paths = [
        public_dir / name
        for name in (*OFFICIAL_CORE_SOURCE_FILENAMES, *LIFECYCLE_EVIDENCE_FILENAMES)
    ]
    lifecycle_paths = [
        public_dir / name for name in LIFECYCLE_EVIDENCE_FILENAMES
    ]
    (parquet_root / "official_symbol_build_summary.json").write_text(
        json.dumps(
            {
                "schema_version": OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION,
                "source": MIXED_FALLBACK_SOURCE_NAME,
                "adjusted_price_method": OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD,
                "missing_adjustment_rows": 0,
                "source_receipts": [_file_content_receipt(path) for path in source_paths],
                "lifecycle_source_receipts": [
                    _file_content_receipt(path) for path in lifecycle_paths
                ],
                "legacy_source_name": "",
                "legacy_source_receipts": [],
                "fallback_source_name": "yahoo_fallback",
                "fallback_source_receipts": [stale_fallback_receipt],
                "session_calendar_rows": 2,
                "session_calendar_receipt": _file_content_receipt(
                    public_dir / "twse_taiex_ohlc.parquet"
                ),
                "session_calendar_summary_receipt": _file_content_receipt(
                    public_dir / "twse_taiex_ohlc.summary.json"
                ),
                "dropped_off_calendar_fallback_rows": 0,
                "dropped_off_calendar_fallback_examples": [],
                "fallback_rows": 1,
                "fallback_adjustment_rows": 0,
                "fallback_symbols": 1,
                "normalized_zero_ohlc_rows": 0,
                "return_quarantined_rows": 0,
                "lifecycle_episode_quarantined_rows": 0,
                "listing_boundary_quarantined_rows": 0,
                "unverified_extreme_quarantined_rows": 0,
                "official_unusable_ohlcv_rows": 1,
                "fallback_replaced_unusable_official_rows": 1,
                "unfilled_unusable_official_rows": 0,
                "symbols": 1,
            }
        ),
        encoding="utf-8",
    )

    summary, findings = audit_official_symbol_build(parquet_root, public_dir)

    assert summary["valid"] is True
    assert findings == []

    input_manifest["files"][0]["checked_through"] = "2000-01-03"
    input_manifest_path.write_text(json.dumps(input_manifest), encoding="utf-8")
    converter_summary_path = fallback.with_suffix(".summary.json")
    converter_summary = json.loads(converter_summary_path.read_text(encoding="utf-8"))
    converter_summary["input_manifest_receipt"] = _receipt(input_manifest_path)
    converter_summary_path.write_text(json.dumps(converter_summary), encoding="utf-8")

    stale_summary, stale_findings = audit_official_symbol_build(parquet_root, public_dir)

    assert stale_summary["valid"] is False
    assert stale_summary["checks"]["fallback_converter_receipts"] is False
    assert [item.code for item in stale_findings] == [
        "stale_official_symbol_build_receipt"
    ]


def test_walk_forward_audit_fails_when_configured_fold_is_unavailable() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    config.runner.start_fold = 3
    config.walk_forward.expected_first_year = 2004
    panel = _panel(["2004-01-02", "2005-01-03", "2006-01-03"])

    summary, findings = audit_walk_forward_availability(panel, config)

    assert summary["fold_count"] == 2
    assert summary["last_fold"] == 2
    assert summary["target_folds"][0]["fold_id"] == 3
    assert summary["target_folds"][0]["available"] is False
    assert [item.code for item in findings] == ["configured_fold_unavailable"]


def test_raw_close_provenance_blocks_model_safe_audit(tmp_path: Path) -> None:
    (tmp_path / "1234_features.parquet").touch()
    (tmp_path / "return_price_provenance.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "symbols": {
                    "1234": {
                        "kind": "official_raw_close",
                        "source": "official",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    summary, findings = audit_return_price_provenance(tmp_path)

    assert summary["raw_close_symbols"] == 1
    assert findings[0].code == "unadjusted_delisted_return_history"
    assert findings[0].severity == "high"


def test_rebuild_resume_requires_matching_command_and_sha256(tmp_path: Path) -> None:
    output = tmp_path / "output.txt"
    manifest = tmp_path / "manifest.json"
    write_v1 = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(output)!r}).write_text('v1')",
    ]
    runner = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    runner.run("example", write_v1, outputs=[output])
    first_mtime = output.stat().st_mtime_ns

    resumed = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    resumed.run("example", write_v1, outputs=[output])
    assert output.stat().st_mtime_ns == first_mtime

    output.write_text("tampered", encoding="utf-8")
    repaired = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    repaired.run("example", write_v1, outputs=[output])
    assert output.read_text(encoding="utf-8") == "v1"

    write_v2 = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(output)!r}).write_text('v2')",
    ]
    changed_command = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    changed_command.run("example", write_v2, outputs=[output])
    assert output.read_text(encoding="utf-8") == "v2"


def test_official_symbol_summary_validator_requires_resolved_adjustments(
    tmp_path: Path,
) -> None:
    summary = tmp_path / "official_symbol_build_summary.json"
    summary.write_text(
        json.dumps({"missing_adjustment_rows": 0}),
        encoding="utf-8",
    )
    _validate_official_symbol_build_summary(summary)

    summary.write_text(
        json.dumps(
            {
                "missing_adjustment_rows": 0,
                "all_adjustments_resolved": False,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="did not certify all adjustments"):
        _validate_official_symbol_build_summary(summary)

    summary.write_text(
        json.dumps({"missing_adjustment_rows": 7}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="missing_adjustment_rows=7"):
        _validate_official_symbol_build_summary(summary)

    summary.write_text(
        json.dumps({"missing_adjustment_rows": "0"}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="no valid missing_adjustment_rows"):
        _validate_official_symbol_build_summary(summary)


def test_rebuild_runner_marks_output_validation_failure_and_keeps_receipt(
    tmp_path: Path,
) -> None:
    summary = tmp_path / "official_symbol_build_summary.json"
    manifest = tmp_path / "manifest.json"
    payload = json.dumps({"missing_adjustment_rows": 3})
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; "
            f"Path({str(summary)!r}).write_text({payload!r}, encoding='utf-8')"
        ),
    ]
    runner = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)

    with pytest.raises(RuntimeError, match="missing_adjustment_rows=3"):
        runner.run(
            "official_symbol_parquets",
            command,
            outputs=[summary],
            validate_outputs=lambda: _validate_official_symbol_build_summary(summary),
        )

    assert summary.exists()
    stage = json.loads(manifest.read_text(encoding="utf-8"))["stages"][-1]
    assert stage["name"] == "official_symbol_parquets"
    assert stage["status"] == "failed"
    assert stage["output_receipts"] == [_runner_file_receipt(summary)]
    assert "missing_adjustment_rows=3" in stage["message"]


def test_rebuild_resume_revalidates_legacy_complete_symbol_stage(
    tmp_path: Path,
) -> None:
    summary = tmp_path / "official_symbol_build_summary.json"
    manifest = tmp_path / "manifest.json"
    payload = json.dumps({"missing_adjustment_rows": 2})
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; "
            f"Path({str(summary)!r}).write_text({payload!r}, encoding='utf-8')"
        ),
    ]
    legacy_runner = RebuildRunner(
        manifest_path=manifest,
        resume=True,
        dry_run=False,
    )
    legacy_runner.run("official_symbol_parquets", command, outputs=[summary])

    resumed = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    with pytest.raises(RuntimeError, match="missing_adjustment_rows=2"):
        resumed.run(
            "official_symbol_parquets",
            command,
            outputs=[summary],
            validate_outputs=lambda: _validate_official_symbol_build_summary(summary),
        )

    stages = json.loads(manifest.read_text(encoding="utf-8"))["stages"]
    assert [stage["status"] for stage in stages] == ["complete", "failed"]
    assert stages[-1]["output_receipts"] == [_runner_file_receipt(summary)]


def test_single_promotion_restores_old_data_when_new_move_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged = tmp_path / "stage" / "new"
    production = tmp_path / "production" / "current"
    backup = tmp_path / "backup" / "old"
    staged.mkdir(parents=True)
    production.mkdir(parents=True)
    (staged / "marker").write_text("new", encoding="utf-8")
    (production / "marker").write_text("old", encoding="utf-8")

    original_replace = os.replace

    def fail_new_move(source, destination):
        if Path(source) == staged and Path(destination) == production:
            raise OSError("simulated staged move failure")
        return original_replace(source, destination)

    monkeypatch.setattr("scripts.rebuild_tw_public_data_layer.os.replace", fail_new_move)
    with pytest.raises(OSError, match="simulated staged move failure"):
        _promote_one(staged, production, backup)

    assert (production / "marker").read_text(encoding="utf-8") == "old"
    assert (staged / "marker").read_text(encoding="utf-8") == "new"
    assert not backup.exists()


def test_single_public_tree_promotion_can_roll_back(tmp_path: Path) -> None:
    staged = tmp_path / "stage" / "data_tw_public"
    production = tmp_path / "production" / "data_tw_public"
    backup = tmp_path / "backup" / "data_tw_public"
    staged.mkdir(parents=True)
    production.mkdir(parents=True)
    (staged / "marker").write_text("new", encoding="utf-8")
    (production / "marker").write_text("old", encoding="utf-8")

    _promote_one(staged, production, backup)
    _rollback_promoted_tree(staged=staged, production=production, backup=backup)

    assert (production / "marker").read_text(encoding="utf-8") == "old"
    assert (staged / "marker").read_text(encoding="utf-8") == "new"
