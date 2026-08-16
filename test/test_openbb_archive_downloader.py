from __future__ import annotations

from collections import deque
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import math
import os
import threading
import time
from types import SimpleNamespace
from typing import Literal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.download_openbb_archive import (
    ARCHIVE_TIME_SHARDED_ENDPOINTS,
    ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST,
    AssetRecord,
    BLS_SERIES_BATCH_SIZE,
    COMPOSITE_LEADING_INDICATOR_ADJUSTMENTS,
    CPI_HARMONIZED_MODES,
    CoverageDecision,
    CFTC_REPORT_MODES,
    DEFAULT_PROVIDER_CONCURRENCY,
    DEFAULT_PROVIDER_RPS,
    DEFAULT_UNDOCUMENTED_PROVIDER_RPS,
    DownloadTask,
    DWPCR_RAW_PARAMETERS,
    FMP_CONSTITUENT_INDEXES,
    FORWARD_ESTIMATE_ENDPOINTS,
    FRED_RELEASE_PAGE_SIZE,
    GDP_FORECAST_DIMENSIONS,
    GDP_NOMINAL_DIMENSIONS,
    GDP_REAL_FREQUENCIES,
    GOVERNMENT_YIELD_CURVE_TYPES,
    HISTORICAL_PRICE_ENDPOINTS,
    INTEREST_RATE_DURATIONS,
    LOCAL_ONLY_ARCHIVE_DATE_FILTERS,
    MORTGAGE_INDEX_GROUPS,
    Manifest,
    OpenBBWorker,
    PlannerContext,
    ProviderExecutorPool,
    ProviderRuntime,
    PROVIDER_RATE_POLICIES,
    PLANNER_STATE_VERSION,
    SNAPSHOT_ENDPOINTS,
    SEC_ALL_COMPANY_FACTS,
    SYMBOL_PERIOD_ENDPOINTS,
    SPREAD_MATURITIES,
    TASK_RETRY_BASE_SECONDS,
    TASK_RETRY_MAX_SECONDS,
    TaskResult,
    TOTAL_FACTOR_PRODUCTIVITY_FREQUENCIES,
    UNEMPLOYMENT_DIMENSIONS,
    RETAIL_PRICE_REGIONS,
    build_initial_plan,
    classify_error,
    discover_followup_tasks,
    execute_download_tasks,
    make_task,
    normalize_records,
    populate_initial_plan,
    providers_missing_required_credentials,
    provider_execution_order,
    provider_order,
    _adaptive_executor_lane_limit,
    quarantine_stale_atomic_parquet_temps,
    select_providers,
    _fetch_econdb_country_profile_workaround,
    _fetch_econdb_indicators_workaround,
    _fetch_econdb_yield_curve_archive_workaround,
    _fetch_eia_petroleum_status_workaround,
    _fetch_federal_reserve_central_bank_holdings_workaround,
    _fetch_yfinance_etf_info_workaround,
    _fetch_bls_series_labstat_table,
    _fetch_bls_series_resilient,
    _download_bls_labstat_file,
    _create_bls_labstat_series_table,
    _fetch_congress_info_workaround,
    _fetch_cftc_cot_catalog_workaround,
    _fetch_fmp_discovery_filings_workaround,
    _fetch_fmp_fundamental_ratio_workaround,
    _fetch_fmp_government_trades_workaround,
    _fetch_fmp_insider_trading_workaround,
    _fetch_fmp_price_targets_workaround,
    _fetch_fmp_world_articles_workaround,
    _fetch_un_comtrade_export_destinations,
    _fetch_fred_calendar_workaround,
    _fetch_fred_bond_indices_workaround,
    _fetch_fred_hqm_workaround,
    _fetch_fred_release_search_workaround,
    _fetch_fred_retail_prices_workaround,
    _fetch_fred_series_workaround,
    _fetch_sec_company_facts_bulk_workaround,
    _fetch_sec_companyfacts_response,
    _fetch_sec_ftd_report_workaround,
    _fetch_sec_filing_headers_workaround,
    _fetch_sec_nport_workaround,
    _fetch_sec_statement_workaround,
    _filter_company_news_to_task_range,
    _fred_bond_index_combinations,
    _completed_quarters,
    _econdb_country_codes,
    _eia_petroleum_schema_mismatch_tables,
    _imf_direction_countries,
    _is_sec_http_url,
    _is_intrinio_large_page_or_bulk_url,
    _is_yahoo_http_url,
    _load_resumable_plan,
    _pop_fairest_endpoint_task,
    _task_execution_affinity,
    _task_retry_delay_seconds,
    _make_sec_async_request_wrapper,
    _make_sec_sync_request_wrapper,
    _make_provider_aiohttp_request_wrapper,
    _make_yfinance_request_wrapper,
    _begin_yfinance_http_evidence,
    _consume_yfinance_transport_failure,
    _provider_for_http_url,
    _normalize_retail_items,
    _normalize_nport_transformer_contract,
    _normalize_bls_search_result,
    _bls_labstat_catalog_files,
    _preferred_tw_symbol,
    _provider_capability_domain,
    _quarantine_obsolete_task_output,
    _repair_xlsx_core_datetimes,
    _sec_ftd_report_periods,
    _sec_submission_records,
    _symbol_tasks,
    _un_comtrade_area_reference,
)


def test_sec_submission_records_normalizes_historical_columnar_shards() -> None:
    payload = {
        "accessionNumber": ["0001-01", "0001-02"],
        "filingDate": ["2000-01-03", "2000-02-04"],
        "form": ["10-K"],
        "metadata": "ignored non-column value",
    }

    assert _sec_submission_records(payload) == [
        {
            "accessionNumber": "0001-01",
            "filingDate": "2000-01-03",
            "form": "10-K",
        },
        {
            "accessionNumber": "0001-02",
            "filingDate": "2000-02-04",
            "form": None,
        },
    ]


def test_sec_identifier_maps_share_one_catalog_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    mapping = {
        "AAPL": "0000320193",
        "BRK-A": "0001067983",
        "BRK-B": "0001067983",
    }
    monkeypatch.setattr(archive, "_SEC_SYMBOL_CIK_CACHE", mapping)
    monkeypatch.setattr(
        archive,
        "_SEC_CIK_SYMBOL_CACHE",
        {"0000320193": ("AAPL",), "0001067983": ("BRK-A", "BRK-B")},
    )

    assert archive._fetch_sec_identifier_map_workaround(
        "regulators.sec.cik_map", {"query": "BRK.A"}, page_limiter=None
    ) == [{"symbol": "BRK-A", "cik": "0001067983"}]
    assert archive._fetch_sec_identifier_map_workaround(
        "regulators.sec.symbol_map", {"query": "1067983"}, page_limiter=None
    ) == [
        {"symbol": "BRK-A", "cik": "0001067983"},
        {"symbol": "BRK-B", "cik": "0001067983"},
    ]


def test_manifest_requeues_pre_fix_sec_filing_cap_once(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.filings",
        "FCF/all/page=0",
        {"symbol": "FCF", "limit": 1000},
        ("sec",),
    )
    plan_token = "plan"
    manifest.upsert_tasks([task], plan_token=plan_token)
    manifest.claim([task])
    manifest.complete(
        TaskResult(
            task=task,
            status="success",
            provider="sec",
            rows=1000,
            output_path=task.output_path,
            attempts=1,
        )
    )

    assert manifest.repair_sec_filings_columnar_shard_bug(plan_token=plan_token) == 1
    row = manifest.connection.execute(
        "SELECT status,selected_provider,rows,error,updated_at FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert tuple(row[:3]) == ("pending", None, 0)
    assert "historical columnar" in row[3]
    assert row[4] == "0001-01-01T00:00:00+00:00"
    assert manifest.repair_sec_filings_columnar_shard_bug(plan_token=plan_token) == 0
    manifest.close()


def test_manifest_requeues_false_empty_sec_filing_headers_once(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "regulators.sec.filing_headers",
        "https://www.sec.gov/Archives/edgar/data/1/000000000100000001/a.htm",
        {"url": "https://www.sec.gov/Archives/edgar/data/1/000000000100000001/a.htm"},
        ("sec",),
    )
    plan_token = "plan"
    manifest.upsert_tasks([task], plan_token=plan_token)
    manifest.claim([task])
    manifest.complete(
        TaskResult(
            task=task,
            status="empty",
            provider="sec",
            rows=0,
            output_path=None,
            attempts=1,
            error=(
                "sec: Failed to download and read the index headers table: "
                "404 index-headers.htm"
            ),
            provider_outcomes={"sec": "empty"},
        )
    )

    assert manifest.repair_sec_filing_headers_index_page_bug(plan_token=plan_token) == 1
    row = manifest.connection.execute(
        "SELECT status,selected_provider,rows,error,provider_outcomes_json,"
        "execution_started_at,updated_at FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert tuple(row[:3]) == ("pending", None, 0)
    assert "canonical SEC filing index page" in row[3]
    assert row[4] == "{}"
    assert row[5] is None
    assert row[6] == "0001-01-01T00:00:00+00:00"
    assert manifest.repair_sec_filing_headers_index_page_bug(plan_token=plan_token) == 0
    manifest.close()


def test_manifest_repairs_only_nonportable_country_all_filters_once(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    etf_task = make_task(
        context,
        "etf.search",
        "exchange=nyse",
        {"exchange": "nyse", "country": "all"},
        ("fmp",),
    )
    macro_task = make_task(
        context,
        "economy.calendar",
        "month=2000-01/page=0",
        {"start_date": "2000-01-01", "end_date": "2000-01-31", "country": "all"},
        ("fred",),
    )
    manifest.upsert_tasks([etf_task, macro_task], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='fmp',attempts=1,"
        "error='country all rejected',provider_outcomes_json='{\"fmp\":\"permanent\"}' "
        "WHERE task_id=?",
        (etf_task.task_id,),
    )
    manifest.connection.commit()

    assert manifest.repair_invalid_country_all_filters(plan_token="plan") == 1
    etf_row = manifest.connection.execute(
        "SELECT kwargs_json,status,selected_provider,attempts,error,"
        "provider_outcomes_json,updated_at FROM tasks WHERE task_id=?",
        (etf_task.task_id,),
    ).fetchone()
    assert json.loads(etf_row[0]) == {"exchange": "nyse"}
    assert tuple(etf_row[1:4]) == ("pending", None, 0)
    assert etf_row[4].startswith("requeued:")
    assert etf_row[5] == "{}"
    assert etf_row[6] == "0001-01-01T00:00:00+00:00"
    macro_kwargs = manifest.connection.execute(
        "SELECT kwargs_json FROM tasks WHERE task_id=?", (macro_task.task_id,)
    ).fetchone()[0]
    assert json.loads(macro_kwargs)["country"] == "all"
    assert manifest.repair_invalid_country_all_filters(plan_token="plan") == 0
    manifest.close()


def test_manifest_repairs_old_provider_error_classification_once(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    transient = make_task(
        context,
        "equity.discovery.filings",
        "page=0",
        {"page": 0},
        ("fmp",),
    )
    entitlement = make_task(
        context,
        "equity.ownership.government_trades",
        "all/page=0",
        {"page": 0},
        ("fmp",),
    )
    manifest.upsert_tasks([transient, entitlement], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='fmp',attempts=1,"
        "error='fmp: JSONDecodeError: Expecting value',"
        'provider_outcomes_json=\'{"fmp":"permanent"}\' WHERE task_id=?',
        (transient.task_id,),
    )
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='fmp',attempts=1,"
        "error='fmp: RuntimeError: FMP HTTP 402: Payment Required',"
        'provider_outcomes_json=\'{"fmp":"permanent"}\' WHERE task_id=?',
        (entitlement.task_id,),
    )
    manifest.connection.commit()

    assert manifest.repair_provider_error_classification(plan_token="plan") == (1, 1)
    rows = {
        row["task_id"]: row
        for row in manifest.connection.execute(
            "SELECT task_id,status,selected_provider,attempts,error,"
            "provider_outcomes_json,updated_at FROM tasks"
        )
    }
    assert rows[transient.task_id]["status"] == "pending"
    assert rows[transient.task_id]["selected_provider"] is None
    assert rows[transient.task_id]["attempts"] == 0
    assert rows[transient.task_id]["updated_at"] == "0001-01-01T00:00:00+00:00"
    assert rows[entitlement.task_id]["status"] == "unavailable"
    assert rows[entitlement.task_id]["selected_provider"] == "fmp"
    assert json.loads(rows[entitlement.task_id]["provider_outcomes_json"]) == {
        "fmp": "unavailable"
    }
    assert manifest.repair_provider_error_classification(plan_token="plan") == (0, 0)
    manifest.close()


def test_manifest_requeues_bls_nullable_title_failure_once(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.survey.bls_series",
        "series=INUS0001",
        {"symbol": "INUS0001"},
        ("bls",),
    )
    manifest.upsert_tasks([task], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='bls',attempts=1,"
        "error=?,provider_outcomes_json=? WHERE task_id=?",
        (
            'bls: BinderException: Column "series_title" referenced before defined',
            '{"bls":"permanent"}',
            task.task_id,
        ),
    )
    manifest.connection.commit()

    assert manifest.repair_bls_missing_series_title_bug(plan_token="plan") == 1
    row = manifest.connection.execute(
        "SELECT status,selected_provider,attempts,rows,error,"
        "provider_outcomes_json,updated_at FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert tuple(row[:4]) == ("pending", None, 0, 0)
    assert row[4].startswith("requeued:")
    assert row[5] == "{}"
    assert row[6] == "0001-01-01T00:00:00+00:00"
    assert manifest.repair_bls_missing_series_title_bug(plan_token="plan") == 0
    manifest.close()


def test_manifest_requeues_sec_nport_list_container_bug_once(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.nport_disclosure",
        "BEDZ/year=2020/quarter=4",
        {"symbol": "BEDZ", "year": 2020, "quarter": 4},
        ("sec", "fmp"),
    )
    manifest.upsert_tasks([task], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='pending',attempts=1,"
        "error=\"sec: AttributeError: 'list' object has no attribute 'get' | "
        'fmp: skipped (cooldown)",'
        'provider_outcomes_json=\'{"sec":"permanent"}\' WHERE task_id=?',
        (task.task_id,),
    )
    manifest.connection.commit()

    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 1
    row = manifest.connection.execute(
        "SELECT status,attempts,error,provider_outcomes_json,updated_at "
        "FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert row[0] == "pending"
    assert row[1] == 0
    assert row[2].startswith("requeued:")
    assert row[3] == "{}"
    assert row[4] == "0001-01-01T00:00:00+00:00"
    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 0
    manifest.close()


def test_manifest_requeues_sec_nport_nullable_container_bug_once(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.nport_disclosure",
        "COMB/year=2023/quarter=4",
        {"symbol": "COMB", "year": 2023, "quarter": 4},
        ("sec", "fmp"),
    )
    manifest.upsert_tasks([task], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='fmp',attempts=1,"
        "error=\"sec: TypeError: argument of type 'NoneType' is not iterable | "
        'fmp: skipped (cooldown)",'
        'provider_outcomes_json=\'{"sec":"permanent","fmp":"unavailable"}\' '
        "WHERE task_id=?",
        (task.task_id,),
    )
    manifest.connection.commit()

    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 1
    row = manifest.connection.execute(
        "SELECT status,selected_provider,attempts,error,provider_outcomes_json,updated_at "
        "FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert row[0] == "pending"
    assert row[1] is None
    assert row[2] == 0
    assert row[3].startswith("requeued:")
    assert json.loads(row[4]) == {"fmp": "unavailable"}
    assert row[5] == "0001-01-01T00:00:00+00:00"
    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 0
    manifest.close()


def test_manifest_requeues_sec_nport_missing_optional_mapping_bug_once(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.nport_disclosure",
        "CDX/year=2025/quarter=3",
        {"symbol": "CDX", "year": 2025, "quarter": 3},
        ("sec", "fmp"),
    )
    manifest.upsert_tasks([task], plan_token="plan")
    manifest.connection.execute(
        "UPDATE tasks SET status='failed',selected_provider='fmp',attempts=1,"
        "error=\"sec: KeyError: 'descRefInstrmnt' | fmp: skipped (cooldown)\","
        'provider_outcomes_json=\'{"sec":"permanent","fmp":"unavailable"}\' '
        "WHERE task_id=?",
        (task.task_id,),
    )
    manifest.connection.commit()

    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 1
    row = manifest.connection.execute(
        "SELECT status,attempts,provider_outcomes_json FROM tasks WHERE task_id=?",
        (task.task_id,),
    ).fetchone()
    assert row[0] == "pending"
    assert row[1] == 0
    assert json.loads(row[2]) == {"fmp": "unavailable"}
    assert manifest.repair_sec_nport_list_container_bug(plan_token="plan") == 0
    manifest.close()


def test_sec_filing_headers_uses_canonical_index_and_matching_cik(
    monkeypatch,
) -> None:
    from io import BytesIO

    requested: list[str] = []
    html = b"""
    <html><body>
      <div id="formName">Form 10-K - Annual report</div>
      <div class="formContent">
        <div class="infoHead">Filing Date</div><div class="info">2013-12-20</div>
        <div class="infoHead">Period of Report</div><div class="info">2013-12-01</div>
      </div>
      <div class="companyInfo">
        <span class="companyName">WRONG CO (Subject)</span>
        <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=111">CIK</a>
        <p class="identInfo">CIK: 0000000111 | SIC: 9999 Wrong | Type: 10-K</p>
      </div>
      <div class="companyInfo">
        <span class="companyName">RIGHT CORP (Filer)</span>
        <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=916618">CIK</a>
        <p class="identInfo">CIK: 0000916618 | SIC: 2834 Pharmaceutical Preparations | Type: 10-K | Act: 34 | Fiscal Year End: 1231</p>
      </div>
      <table class="tableFile" summary="Document Format Files">
        <tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th></tr>
        <tr><td>1</td><td>Annual report</td><td><a href="/Archives/edgar/data/916618/000110465913019360/report.htm">report.htm</a></td><td>10-K</td></tr>
      </table>
    </body></html>
    """

    class _Response(BytesIO):
        headers: dict[str, str] = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    def _urlopen(request, timeout):
        requested.append(request.full_url)
        assert timeout == 60
        return _Response(html)

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    records = _fetch_sec_filing_headers_workaround(
        {
            "url": "https://www.sec.gov/Archives/edgar/data/916618/"
            "000110465913019360/primary.htm"
        }
    )

    assert requested == [
        "https://www.sec.gov/Archives/edgar/data/916618/"
        "000110465913019360/0001104659-13-019360-index.html"
    ]
    assert records[0]["name"] == "RIGHT CORP"
    assert records[0]["cik"] == "916618"
    assert records[0]["sic"] == "2834"
    assert records[0]["sic_organization_name"] == "Pharmaceutical Preparations"
    assert records[0]["document_type"] == "10-K"
    assert records[0]["fiscal_year_end"] == "12-31"
    assert records[0]["filing_date"] == "2013-12-20"
    assert records[0]["period_ending"] == "2013-12-01"
    assert records[0]["document_urls"] == [
        {
            "sequence": "1",
            "description": "Annual report",
            "filename": "report.htm",
            "type": "10-K",
            "url": "https://www.sec.gov/Archives/edgar/data/916618/"
            "000110465913019360/report.htm",
        }
    ]


def test_fred_series_workaround_skips_discarded_metadata_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_fred.utils import rate_limiter

    calls: list[tuple[str, int, bool]] = []

    async def _fred_get(url: str, *, timeout: int, use_cache: bool):
        calls.append((url, timeout, use_cache))
        return {
            "observations": [
                {
                    "date": "2000-01-01",
                    "value": "1.25",
                    "realtime_start": "2000-01-01",
                    "realtime_end": "2000-01-01",
                },
                {
                    "date": "2000-02-01",
                    "value": ".",
                    "realtime_start": "2000-02-01",
                    "realtime_end": "2000-02-01",
                },
            ]
        }

    monkeypatch.setattr(rate_limiter, "fred_get", _fred_get)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key="secret"))
    )

    records = _fetch_fred_series_workaround(
        {
            "symbol": "TEST",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
            "limit": 100000,
        },
        obb,
    )

    assert [record.model_dump() for record in records] == [
        {"date": date(2000, 1, 1), "TEST": 1.25}
    ]
    assert len(calls) == 1
    url, timeout, use_cache = calls[0]
    assert "/fred/series/observations?" in url
    assert "series_id=TEST" in url
    assert "observation_start=2000-01-01" in url
    assert "observation_end=2026-07-18" in url
    assert "api_key=secret" in url
    assert "/fred/series?" not in url
    assert timeout == 60
    assert use_cache is False


class _Field:
    def __init__(self, annotation=object, description=""):
        self.annotation = annotation
        self.description = description


class _InputModel:
    model_fields = {
        "symbol": _Field(str),
        "start_date": _Field(str),
        "end_date": _Field(str),
        "provider": _Field(str),
    }


class _NoopLimiter:
    def wait(self) -> None:
        return None

    def defer(self, seconds: float) -> None:
        return None


def _runtime(
    rps: dict[str, float], concurrency: dict[str, int], cooldown: float = 1
) -> ProviderRuntime:
    runtime = ProviderRuntime(rps, concurrency, cooldown)
    runtime._limiters.update({provider: _NoopLimiter() for provider in rps})
    return runtime


def test_provider_rate_policies_use_official_caps_or_eight_rps_default() -> None:
    expected = {
        "bls": 5.0,
        "congress_gov": 5000 / 3600,
        "eia": 9000 / 3600,
        "fred": 2.0,
        "intrinio": 100.0,
        "intrinio_large_page": 1 / 60,
        "oecd": 60 / 3600,
        "sec": 10.0,
        "tradingeconomics": 2.0,
    }
    for provider, rps in expected.items():
        assert DEFAULT_PROVIDER_RPS[provider] == pytest.approx(rps)
        assert PROVIDER_RATE_POLICIES[provider].source_url.startswith("https://")
    for provider in set(DEFAULT_PROVIDER_RPS) - set(expected):
        assert DEFAULT_PROVIDER_RPS[provider] == DEFAULT_UNDOCUMENTED_PROVIDER_RPS

    runtime = ProviderRuntime({}, {}, 1)
    unknown = runtime.limiter("new_provider_without_documented_limit")
    sec = runtime.limiter("sec")
    assert unknown.interval_seconds == pytest.approx(1 / 8)
    assert sec is not unknown

    assert DEFAULT_PROVIDER_CONCURRENCY["federal_reserve"] == 28
    assert DEFAULT_PROVIDER_CONCURRENCY["yfinance"] == 28
    assert DEFAULT_PROVIDER_CONCURRENCY["fred"] == 8
    assert DEFAULT_PROVIDER_CONCURRENCY["congress_gov"] == 4
    assert DEFAULT_PROVIDER_CONCURRENCY["sec"] == 72
    assert DEFAULT_PROVIDER_CONCURRENCY["intrinio"] == 100
    assert DEFAULT_PROVIDER_CONCURRENCY["oecd"] == 1


def test_sec_http_boundary_paces_each_child_request(monkeypatch) -> None:
    import asyncio
    import downloader.download_openbb_archive as archive

    calls: list[str] = []

    class _CountingLimiter:
        waits = 0

        def wait(self) -> None:
            self.waits += 1

    limiter = _CountingLimiter()
    runtime = SimpleNamespace(
        availability=lambda provider: (True, None),
        limiter=lambda provider: limiter,
    )
    monkeypatch.setattr(archive, "_SEC_HTTP_RUNTIME", runtime)

    async def original_async(url, **kwargs):
        calls.append(str(url))
        return kwargs

    def original_sync(url, **kwargs):
        calls.append(str(url))
        return kwargs

    async_request = _make_sec_async_request_wrapper(original_async)
    sync_request = _make_sec_sync_request_wrapper(original_sync)

    assert _is_sec_http_url("https://data.sec.gov/submissions/test.json")
    assert _is_sec_http_url("https://www.sec.gov/Archives/test.xml")
    assert not _is_sec_http_url("https://example.com/sec.gov/test")
    asyncio.run(async_request("https://data.sec.gov/one", value=1))
    asyncio.run(async_request("https://www.sec.gov/two", value=2))
    sync_request("https://efts.sec.gov/three", value=3)
    asyncio.run(async_request("https://example.com/four", value=4))

    assert limiter.waits == 3
    assert len(calls) == 4


def test_yfinance_http_boundary_paces_cookie_crumb_and_data(monkeypatch) -> None:
    import downloader.download_openbb_archive as archive

    calls: list[tuple[str, str]] = []

    class _CountingLimiter:
        waits = 0

        def wait(self) -> None:
            self.waits += 1

    limiter = _CountingLimiter()
    runtime = SimpleNamespace(
        availability=lambda provider: (True, None),
        limiter=lambda provider: limiter,
    )
    monkeypatch.setattr(archive, "_YFINANCE_HTTP_RUNTIME", runtime)

    def original(session, method, url, **kwargs):
        calls.append((str(method), str(url)))
        return kwargs

    request = _make_yfinance_request_wrapper(original)
    assert _is_yahoo_http_url("https://fc.yahoo.com")
    assert _is_yahoo_http_url("https://query2.finance.yahoo.com/v1/test/getcrumb")
    assert not _is_yahoo_http_url("https://example.com/yahoo.com/test")

    request(object(), "GET", "https://fc.yahoo.com", timeout=1)
    request(
        object(),
        "GET",
        "https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL",
    )
    request(object(), "GET", "https://example.com/yahoo.com/test")

    assert limiter.waits == 2
    assert len(calls) == 3


def test_yfinance_http_evidence_distinguishes_recovered_and_partial_responses(
    monkeypatch,
) -> None:
    import downloader.download_openbb_archive as archive

    class _Response:
        def __init__(self, status: int, detail: str = "") -> None:
            self.status_code = status
            self.text = detail

        def json(self):
            return {"description": self.text}

    responses = iter(
        [
            _Response(401, "Invalid Crumb"),
            _Response(200),
            _Response(401, "User is unable to access this feature"),
            _Response(200),
            _Response(200),
            _Response(503, "upstream unavailable"),
        ]
    )
    monkeypatch.setattr(archive, "_YFINANCE_HTTP_RUNTIME", None)

    def original(session, method, url, **kwargs):
        return next(responses)

    request = _make_yfinance_request_wrapper(original)
    quote_summary = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL"

    _begin_yfinance_http_evidence()
    request(object(), "GET", quote_summary)
    request(object(), "GET", quote_summary)
    assert _consume_yfinance_transport_failure() is None

    _begin_yfinance_http_evidence()
    request(object(), "GET", quote_summary)
    request(object(), "GET", "https://query2.finance.yahoo.com/v1/test/getcrumb")
    failure = _consume_yfinance_transport_failure()
    assert failure is not None
    assert "HTTP 401" in failure
    assert "/v10/finance/quoteSummary/AAPL" in failure
    assert classify_error(RuntimeError(failure)) == "transient"

    _begin_yfinance_http_evidence()
    request(object(), "GET", quote_summary)
    request(
        object(),
        "GET",
        "https://query2.finance.yahoo.com/v8/finance/chart/MSFT",
    )
    partial_failure = _consume_yfinance_transport_failure()
    assert partial_failure is not None
    assert "HTTP 503" in partial_failure


def test_aiohttp_boundary_paces_fanout_and_intrinio_route_bucket(
    monkeypatch,
) -> None:
    import asyncio
    import downloader.download_openbb_archive as archive

    class _CountingLimiter:
        def __init__(self) -> None:
            self.waits = 0

        def wait(self) -> None:
            self.waits += 1

    limiters = {
        "fmp": _CountingLimiter(),
        "intrinio": _CountingLimiter(),
        "intrinio_large_page": _CountingLimiter(),
    }
    runtime = SimpleNamespace(
        availability=lambda provider: (True, None),
        limiter=lambda provider: limiters[provider],
    )
    monkeypatch.setattr(archive, "_PROVIDER_HTTP_RUNTIME", runtime)
    calls: list[str] = []

    async def original(session, method, url, **kwargs):
        calls.append(str(url))
        return kwargs

    request = _make_provider_aiohttp_request_wrapper(original)
    assert (
        _provider_for_http_url(
            "https://financialmodelingprep.com/stable/profile?symbol=AAPL"
        )
        == "fmp"
    )
    assert (
        _provider_for_http_url(
            "https://api-v2.intrinio.com/etfs/SPY/holdings?page_size=10000"
        )
        == "intrinio"
    )
    assert _provider_for_http_url("https://example.com/econdb.com") is None
    assert _is_intrinio_large_page_or_bulk_url(
        "https://api-v2.intrinio.com/x?page_size=101"
    )
    assert not _is_intrinio_large_page_or_bulk_url(
        "https://api-v2.intrinio.com/x?page_size=100"
    )

    asyncio.run(
        request(
            object(),
            "GET",
            "https://financialmodelingprep.com/stable/profile?symbol=AAPL",
        )
    )
    asyncio.run(
        request(
            object(),
            "GET",
            "https://api-v2.intrinio.com/etfs/SPY/holdings?page_size=10000",
        )
    )
    asyncio.run(request(object(), "GET", "https://example.com/ignored"))

    assert limiters["fmp"].waits == 1
    assert limiters["intrinio"].waits == 1
    assert limiters["intrinio_large_page"].waits == 1
    assert len(calls) == 3


def test_every_openbb_provider_uses_a_real_http_boundary_or_special_wrapper() -> None:
    import downloader.download_openbb_archive as archive

    expected_boundary_providers = set(PROVIDER_RATE_POLICIES) - {
        "intrinio_large_page",
        "sec",
        "un_comtrade",
        "yfinance",
    }
    assert expected_boundary_providers <= archive.HTTP_BOUNDARY_PACED_PROVIDERS
    sample_hosts = {
        "benzinga": "https://api.benzinga.com/api/v2/news",
        "bls": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "cftc": "https://publicreporting.cftc.gov/resource/test.json",
        "congress_gov": "https://api.congress.gov/v3/bill",
        "econdb": "https://www.econdb.com/api/series/",
        "eia": "https://api.eia.gov/v2/",
        "federal_reserve": "https://www.federalreserve.gov/data/test.htm",
        "fmp": "https://financialmodelingprep.com/stable/profile",
        "fred": "https://api.stlouisfed.org/fred/series",
        "government_us": "https://api.data.gov/example",
        "imf": "https://api.imf.org/external/sdmx/2.1/data",
        "intrinio": "https://api-v2.intrinio.com/companies",
        "oecd": "https://sdmx.oecd.org/public/rest/data/",
        "tiingo": "https://api.tiingo.com/tiingo/daily/AAPL/prices",
        "tradingeconomics": "https://api.tradingeconomics.com/markets/historical",
    }
    assert {
        provider: _provider_for_http_url(url) for provider, url in sample_hosts.items()
    } == {provider: provider for provider in sample_hosts}


def test_explicit_page_ticket_is_not_double_counted_at_http_boundary() -> None:
    import downloader.download_openbb_archive as archive

    claims: list[int] = []
    limiter = archive.HttpBoundaryRateLimiter(
        0,
        on_claim=lambda: claims.append(1),
    )

    limiter.wait()
    limiter.wait_at_http_boundary()
    assert len(claims) == 1

    limiter.wait_at_http_boundary()
    assert len(claims) == 2

    limiter.wait_explicit_boundary()
    assert len(claims) == 3
    limiter.wait_at_http_boundary()
    assert len(claims) == 4


def test_requests_urllib_and_httpx_share_provider_http_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import downloader.download_openbb_archive as archive

    class _CountingLimiter:
        def __init__(self) -> None:
            self.waits = 0

        def wait(self) -> None:
            self.waits += 1

    limiters = {
        provider: _CountingLimiter()
        for provider in ("imf", "oecd", "congress_gov", "federal_reserve")
    }
    runtime = SimpleNamespace(
        availability=lambda provider: (True, None),
        limiter=lambda provider: limiters[provider],
    )
    monkeypatch.setattr(archive, "_PROVIDER_HTTP_RUNTIME", runtime)

    requests_wrapper = archive._make_provider_requests_request_wrapper(
        lambda session, method, url, **kwargs: url
    )
    urllib_wrapper = archive._make_provider_urllib_open_wrapper(
        lambda opener, fullurl, **kwargs: fullurl
    )
    httpx_wrapper = archive._make_provider_httpx_send_wrapper(
        lambda client, request, **kwargs: request
    )

    async def _async_original(client, request, **kwargs):
        return request

    async_wrapper = archive._make_provider_httpx_async_send_wrapper(_async_original)
    requests_wrapper(object(), "GET", "https://api.imf.org/test")
    urllib_wrapper(
        object(), SimpleNamespace(full_url="https://sdmx.oecd.org/public/rest/data")
    )
    httpx_wrapper(object(), SimpleNamespace(url="https://api.congress.gov/v3/bill"))
    asyncio.run(
        async_wrapper(
            object(),
            SimpleNamespace(url="https://www.federalreserve.gov/data/test.htm"),
        )
    )

    assert {provider: limiter.waits for provider, limiter in limiters.items()} == {
        "imf": 1,
        "oecd": 1,
        "congress_gov": 1,
        "federal_reserve": 1,
    }


def test_bls_labstat_transport_bypasses_only_api_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    runtime = SimpleNamespace(
        availability=lambda provider: (False, "cooldown until tomorrow"),
        limiter=lambda provider: pytest.fail("LABSTAT must not claim an API ticket"),
    )
    monkeypatch.setattr(archive, "_PROVIDER_HTTP_RUNTIME", runtime)

    archive._wait_provider_http_boundary(
        "bls", "https://download.bls.gov/pub/time.series/ws/ws.data.1.AllData"
    )
    with pytest.raises(archive.ProviderDeferredError, match="cooldown"):
        archive._wait_provider_http_boundary(
            "bls", "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        )


def test_worker_disables_openbb_http_cache_without_changing_task_identity(
    tmp_path: Path,
) -> None:
    seen: dict[str, object] = {}

    def endpoint(**kwargs):
        seen.update(kwargs)
        return [{"date": "2000-01-03", "value": 1}]

    obb = SimpleNamespace(
        economy=SimpleNamespace(cache_probe=endpoint),
    )
    runtime = _runtime({"econdb": 1000.0}, {"econdb": 1})
    worker = OpenBBWorker(
        obb,
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
        cache_capable_endpoints={"economy.cache_probe"},
    )
    task = DownloadTask(
        task_id="cache-contract",
        endpoint="economy.cache_probe",
        category="economy",
        scope_key="country=US",
        kwargs={"country": "US", "use_cache": True},
        providers=("econdb",),
        output_path=str(tmp_path / "result.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert seen["provider"] == "econdb"
    assert seen["use_cache"] is False
    assert task.kwargs["use_cache"] is True


def test_worker_learns_pageable_limit_and_preserves_continuation_contract(
    tmp_path: Path,
) -> None:
    seen_limits: list[int] = []

    def endpoint(**kwargs):
        limit = int(kwargs["limit"])
        seen_limits.append(limit)
        if limit > 20:
            raise RuntimeError(
                "Unauthorized FMP request -> 402 -> Premium Query Parameter: "
                "The values for 'limit' must be between 0 and 20"
            )
        return [{"filing_id": index} for index in range(limit)]

    obb = SimpleNamespace(
        equity=SimpleNamespace(
            fundamental=SimpleNamespace(filings=endpoint),
        )
    )
    runtime = ProviderRuntime(
        {"fmp": 1000.0}, {"fmp": 1}, 60, tmp_path / "runtime.json"
    )
    worker = OpenBBWorker(
        obb,
        runtime,
        max_retries=2,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.filings",
        "2330.TW/year=2025/page=0",
        {"symbol": "2330.TW", "limit": 1000, "page": 0},
        ("fmp",),
    )

    result = worker(task)

    assert result.status == "success"
    assert result.rows == 20
    assert seen_limits == [1000, 20]
    assert result.task.task_id == task.task_id
    assert result.task.kwargs["limit"] == 20
    followups = discover_followup_tasks(context, result)
    assert len(followups) == 1
    assert followups[0].kwargs["limit"] == 20
    assert followups[0].kwargs["page"] == 1
    assert followups[0].scope_key.endswith("page=1")

    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="plan")
        manifest.complete(result)
        stored = json.loads(
            str(
                manifest.connection.execute(
                    "SELECT kwargs_json FROM tasks WHERE task_id=?",
                    (task.task_id,),
                ).fetchone()[0]
            )
        )
        assert stored["limit"] == 20
    finally:
        manifest.close()


def test_worker_saves_nonpageable_entitlement_capped_rows(
    tmp_path: Path,
) -> None:
    seen_limits: list[int | None] = []

    def endpoint(**kwargs):
        limit = kwargs.get("limit")
        seen_limits.append(limit)
        if limit is None or int(limit) > 5:
            raise RuntimeError(
                "Unauthorized FMP request -> 402 -> Premium Query Parameter: "
                "The values for 'limit' must be between 0 and 5"
            )
        return [
            {
                "date": f"2020-{index + 1:02d}-01",
                "employees": 100 + index,
            }
            for index in range(int(limit))
        ]

    obb = SimpleNamespace(
        equity=SimpleNamespace(
            fundamental=SimpleNamespace(employee_count=endpoint),
        )
    )
    runtime = ProviderRuntime(
        {"fmp": 1000.0}, {"fmp": 1}, 60, tmp_path / "runtime.json"
    )
    worker = OpenBBWorker(
        obb,
        runtime,
        max_retries=2,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    task = DownloadTask(
        task_id="employee-cap",
        endpoint="equity.fundamental.employee_count",
        category="equity",
        scope_key="AAPL/year=2020",
        kwargs={
            "symbol": "AAPL",
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
        },
        providers=("fmp",),
        output_path=str(tmp_path / "employee.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert result.rows == 5
    assert result.task.kwargs["limit"] == 5
    assert seen_limits == [None, 5]
    assert runtime.parameter_maximums() == {
        ("fmp", "equity.fundamental.employee_count", "limit"): 5
    }


def test_worker_retries_market_cap_without_restricted_dates(
    tmp_path: Path,
) -> None:
    seen: list[dict[str, object]] = []

    def endpoint(**kwargs):
        seen.append(dict(kwargs))
        if "start_date" in kwargs or "end_date" in kwargs:
            raise RuntimeError(
                "Unauthorized FMP request -> 402 -> Premium Query Parameter: "
                "This value set for 'from' is not available under your subscription"
            )
        return [{"date": "2025-01-02", "market_cap": 1_000_000}]

    obb = SimpleNamespace(equity=SimpleNamespace(historical_market_cap=endpoint))
    runtime = ProviderRuntime(
        {"fmp": 1000.0}, {"fmp": 1}, 60, tmp_path / "runtime.json"
    )
    worker = OpenBBWorker(
        obb,
        runtime,
        max_retries=2,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    task = DownloadTask(
        task_id="market-cap-undated",
        endpoint="equity.historical_market_cap",
        category="equity",
        scope_key="AAPL",
        kwargs={
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        providers=("fmp",),
        output_path=str(tmp_path / "market-cap.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert result.rows == 1
    assert task.kwargs["start_date"] == "2000-01-01"
    assert result.task.kwargs == {"symbol": "AAPL"}
    assert seen == [
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
            "provider": "fmp",
        },
        {"symbol": "AAPL", "provider": "fmp"},
    ]


def test_quarantine_stale_atomic_parquet_temps_keeps_live_writers(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data" / "equity" / "price"
    data_dir.mkdir(parents=True)
    dead = data_dir / ".AAPL.parquet.2147483647.123.tmp"
    live = data_dir / f".MSFT.parquet.{os.getpid()}.456.tmp"
    unrelated = data_dir / "notes.tmp"
    dead.write_bytes(b"partial")
    live.write_bytes(b"writing")
    unrelated.write_bytes(b"keep")

    moved = quarantine_stale_atomic_parquet_temps(tmp_path)

    assert not dead.exists()
    assert len(moved) == 1
    assert (
        moved[0]
        .relative_to(tmp_path)
        .as_posix()
        .startswith("_state/quarantine/stale_atomic_tmp/equity/price/")
    )
    assert moved[0].read_bytes() == b"partial"
    assert live.read_bytes() == b"writing"
    assert unrelated.read_bytes() == b"keep"


def test_atomic_parquet_preserves_fields_introduced_after_first_row(
    tmp_path: Path,
) -> None:
    import downloader.download_openbb_archive as archive

    output = tmp_path / "heterogeneous.parquet"
    task = DownloadTask(
        task_id="heterogeneous",
        endpoint="economy.survey.bls_search",
        category="economy",
        scope_key="catalog",
        kwargs={},
        providers=("bls",),
        output_path=str(output),
    )
    recovery = '[{"field":"value","error_type":"null_mapping"}]'
    rows = archive._atomic_write_parquet(
        [
            {"series_id": "A", "value": 1},
            {
                "code_field": "industry",
                "code": "10",
                "openbb_validation_recoveries": recovery,
            },
        ],
        task,
        "bls",
    )

    assert rows == 2
    table = pq.read_table(output)
    assert table.column_names == [
        "series_id",
        "value",
        "_openbb_endpoint",
        "_provider",
        "_scope_key",
        "_retrieved_at",
        "_query_json",
        "code_field",
        "code",
        "openbb_validation_recoveries",
    ]
    assert table["code_field"].to_pylist() == [None, "industry"]
    assert table["openbb_validation_recoveries"].to_pylist() == [None, recovery]


def test_quarantine_obsolete_terminal_task_output_preserves_recovery_copy(
    tmp_path: Path,
) -> None:
    output = tmp_path / "data" / "equity" / "profile" / "aa" / "AAPL.parquet"
    output.parent.mkdir(parents=True)
    pq.write_table(pa.table({"value": [1]}), output)
    task = DownloadTask(
        task_id="obsolete-aapl",
        endpoint="equity.profile",
        category="equity",
        scope_key="AAPL",
        kwargs={"symbol": "AAPL"},
        providers=("yfinance",),
        output_path=str(output),
    )

    moved = _quarantine_obsolete_task_output(task)

    assert moved is not None
    assert not output.exists()
    assert moved.relative_to(tmp_path).as_posix() == (
        "_quarantine/obsolete_task_outputs/equity/profile/aa/AAPL.parquet"
    )
    assert pq.read_table(moved).to_pylist() == [{"value": 1}]


def test_manifest_reconciles_terminal_and_orphan_shards_across_data_tree(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "_state"
    data_dir = tmp_path / "data" / "equity" / "profile"
    data_dir.mkdir(parents=True)
    statuses = {
        "success": data_dir / "success.parquet",
        "pending": data_dir / "pending.parquet",
        "unavailable": data_dir / "unavailable.parquet",
    }
    orphan = data_dir / "orphan.parquet"
    for path in [*statuses.values(), orphan]:
        path.write_bytes(b"shard")
    tasks = [
        DownloadTask(
            task_id=f"task-{status}",
            endpoint="equity.profile",
            category="equity",
            scope_key=status,
            kwargs={"symbol": status.upper()},
            providers=("yfinance",),
            output_path=str(path),
        )
        for status, path in statuses.items()
    ]
    manifest = Manifest(state_dir / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="plan")
        manifest.connection.executemany(
            "UPDATE tasks SET status=? WHERE task_id=?",
            [(status, f"task-{status}") for status in statuses],
        )
        manifest.connection.commit()

        scanned, quarantined = manifest.quarantine_terminal_output_shards(
            plan_token="plan", batch_size=2
        )
    finally:
        manifest.close()

    assert (scanned, quarantined) == (4, 2)
    assert statuses["success"].is_file()
    assert statuses["pending"].is_file()
    assert not statuses["unavailable"].exists()
    assert not orphan.exists()
    quarantine = tmp_path / "_quarantine" / "obsolete_task_outputs"
    assert (quarantine / "equity/profile/unavailable.parquet").is_file()
    assert (quarantine / "equity/profile/orphan.parquet").is_file()


def test_worker_terminal_outcome_overrides_earlier_transient_attempt(
    tmp_path: Path,
) -> None:
    calls = 0

    def endpoint(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("Temporary failure in name resolution")
        return []

    obb = SimpleNamespace(economy=SimpleNamespace(test_endpoint=endpoint))
    runtime = ProviderRuntime({"econdb": 1000.0}, {"econdb": 1}, 1.0)
    runtime.block = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    worker = OpenBBWorker(
        obb,
        runtime,
        max_retries=2,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    task = DownloadTask(
        task_id="transient-then-empty",
        endpoint="economy.test_endpoint",
        category="economy",
        scope_key="test",
        kwargs={},
        providers=("econdb",),
        output_path=str(tmp_path / "result.parquet"),
    )

    result = worker(task)

    assert calls == 2
    assert result.status == "empty"
    assert result.provider_outcomes == {"econdb": "empty"}


def test_provider_runtime_restores_persisted_cooldown(tmp_path: Path) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    first = ProviderRuntime(
        {"fmp": 1000.0},
        {"fmp": 1},
        60,
        state_path,
    )
    first.block("fmp", 60)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 8
    assert payload["providers"]["fmp"]["blocked_until"] > time.time()
    assert payload["providers"]["fmp"]["kind"] == "transient"
    assert payload["rate_limits_rps"] == {"fmp": 1000.0}
    assert payload["concurrency"] == {"fmp": 1}
    assert "rate_activity" in payload

    restored = ProviderRuntime(
        {"fmp": 1000.0},
        {"fmp": 1},
        60,
        state_path,
    )
    available, reason = restored.availability("fmp")
    assert available is False
    assert "cooldown until" in str(reason)
    assert set(restored.cooldown_deadlines()) == {"fmp"}


def test_provider_runtime_converts_pageable_limit_denial_to_constraint(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    reason = (
        "fmp: UnauthorizedError: 402 Premium Query Parameter: "
        "The values for 'limit' must be between 0 and 20"
    )
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "providers": {},
                "unavailable_domains": [
                    {
                        "provider": "fmp",
                        "endpoint": "equity.fundamental.filings",
                        "domain": "tw",
                        "reason": reason,
                    },
                    {
                        "provider": "fmp",
                        "endpoint": "equity.fundamental.metrics",
                        "domain": "us",
                        "reason": reason.replace("20", "5"),
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 60, state_path)

    assert runtime.availability("fmp", "equity.fundamental.filings", "tw")[0] is False
    assert runtime.clear_adaptable_limit_unavailable_domains() == (
        ("fmp", "equity.fundamental.filings", "tw"),
        ("fmp", "equity.fundamental.metrics", "us"),
    )
    assert runtime.availability("fmp", "equity.fundamental.filings", "tw") == (
        True,
        None,
    )
    assert runtime.availability("fmp", "equity.fundamental.metrics", "us") == (
        True,
        None,
    )
    adjusted, applied = runtime.apply_parameter_maximums(
        "fmp", "equity.fundamental.filings", {"limit": 1000}
    )
    assert adjusted["limit"] == 20
    assert applied == {"limit": 20}
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["parameter_maximums"] == [
        {
            "endpoint": "equity.fundamental.filings",
            "maximum": 20,
            "parameter": "limit",
            "provider": "fmp",
        },
        {
            "endpoint": "equity.fundamental.metrics",
            "maximum": 5,
            "parameter": "limit",
            "provider": "fmp",
        },
    ]


def test_provider_runtime_converts_dated_market_cap_denial_to_omission(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    reason = (
        "fmp: UnauthorizedError: 402 Premium Query Parameter: "
        "This value set for 'from' is not available under your subscription"
    )
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 5,
                "providers": {},
                "unavailable_domains": [
                    {
                        "provider": "fmp",
                        "endpoint": "equity.historical_market_cap",
                        "domain": "us",
                        "reason": reason,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 60, state_path)

    assert runtime.clear_adaptable_query_shape_unavailable_domains() == (
        ("fmp", "equity.historical_market_cap", "us"),
    )
    assert runtime.availability("fmp", "equity.historical_market_cap", "us") == (
        True,
        None,
    )
    adjusted, omitted = runtime.apply_omitted_parameters(
        "fmp",
        "equity.historical_market_cap",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
    )
    assert adjusted == {"symbol": "AAPL"}
    assert omitted == ("end_date", "start_date")
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["schema_version"] == 8
    assert persisted["omitted_parameters"] == [
        {
            "provider": "fmp",
            "endpoint": "equity.historical_market_cap",
            "parameters": ["end_date", "start_date"],
        }
    ]


def test_provider_runtime_drops_self_reinforcing_sec_cooldown(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "providers": {
                    "sec": {
                        "blocked_until": time.time() + 3600,
                        "kind": "quota",
                        "reason": (
                            "sec: RuntimeError: SEC rate limit cooldown until "
                            "2099-01-01T00:00:00+00:00"
                        ),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    runtime = ProviderRuntime(
        {"sec": 10.0},
        {"sec": 320},
        3600,
        state_path,
    )

    assert runtime.availability("sec") == (True, None)
    assert runtime.cooldown_deadlines() == {}
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["providers"] == {}


def test_provider_runtime_records_limiter_claim_activity(tmp_path: Path) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 2}, 60, state_path)

    runtime.limiter("fmp").wait()
    deadline = time.monotonic() + 1.0
    while True:
        activity = runtime.rate_activity()["providers"]["fmp"]
        if (
            activity["limiter_observed_claims_total"] == 1
            or time.monotonic() >= deadline
        ):
            break
        time.sleep(0.005)

    assert activity["limiter_claims_total"] == 1
    assert activity["limiter_observed_claims_total"] == 1
    assert activity["limiter_claims_last_60s"] == 1
    assert activity["observed_claims_per_second"] > 0
    assert activity["ticket_waiters"] == 0
    assert activity["active_calls"] == 0
    with runtime.try_semaphore("fmp") as acquired:
        assert acquired
        assert runtime.rate_activity()["providers"]["fmp"]["active_calls"] == 1
    assert runtime.rate_activity()["providers"]["fmp"]["active_calls"] == 0
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["rate_activity"]["providers"]["fmp"]["limiter_claims_total"] == 1


def test_provider_runtime_attributes_and_restores_endpoint_http_costs(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime(
        {"fmp": 1000.0, "fred": 1000.0},
        {"fmp": 1, "fred": 1},
        60,
        state_path,
    )

    observation = runtime.begin_request_observation("equity.fundamental.metrics")
    runtime.limiter("fmp").wait()
    runtime.limiter("fmp").wait()
    runtime.limiter("fred").wait()
    runtime.finish_request_observation(observation)

    activity = runtime.rate_activity()["providers"]
    fmp_cost = activity["fmp"]["endpoint_request_costs"]["equity.fundamental.metrics"]
    fred_cost = activity["fred"]["endpoint_request_costs"]["equity.fundamental.metrics"]
    assert fmp_cost["requests"] == 2
    assert fmp_cost["claiming_attempts"] == 1
    assert fmp_cost["max_requests_per_attempt"] == 2
    assert fmp_cost["average_requests_per_claiming_attempt"] == 2.0
    assert fred_cost["requests"] == 1
    assert fred_cost["average_requests_per_claiming_attempt"] == 1.0

    # Provider state transitions atomically flush the rolling telemetry just
    # as a live cooldown/constraint observation does.
    runtime.block("fmp", 1, "test flush")
    restored = ProviderRuntime(
        {"fmp": 1000.0, "fred": 1000.0},
        {"fmp": 1, "fred": 1},
        60,
        state_path,
    )
    restored_cost = restored.rate_activity()["providers"]["fmp"][
        "endpoint_request_costs"
    ]["equity.fundamental.metrics"]
    assert restored_cost == fmp_cost
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["schema_version"] == 8
    assert {
        (row["provider"], row["endpoint"]): row["requests"]
        for row in persisted["endpoint_request_costs"]
    } == {
        ("fmp", "equity.fundamental.metrics"): 2,
        ("fred", "equity.fundamental.metrics"): 1,
    }


def test_provider_runtime_discards_only_stale_endpoint_cost_revision(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 8,
                "providers": {},
                "endpoint_request_costs": [
                    {
                        "provider": "fred",
                        "endpoint": "economy.fred_series",
                        "requests": 200,
                        "claiming_attempts": 100,
                        "max_requests_per_attempt": 2,
                        "implementation_revision": 1,
                    },
                    {
                        "provider": "congress_gov",
                        "endpoint": "uscongress.bill_info",
                        "requests": 75,
                        "claiming_attempts": 10,
                        "max_requests_per_attempt": 8,
                        "implementation_revision": 1,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    runtime = ProviderRuntime(
        {"fred": 1000.0, "congress_gov": 1000.0},
        {"fred": 1, "congress_gov": 1},
        60,
        state_path,
    )
    activity = runtime.rate_activity()["providers"]

    assert activity["fred"]["endpoint_request_costs"] == {}
    assert (
        activity["congress_gov"]["endpoint_request_costs"]["uscongress.bill_info"][
            "average_requests_per_claiming_attempt"
        ]
        == 7.5
    )


def test_provider_runtime_restores_provider_day_claims_across_restart(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    first = ProviderRuntime({"bls": 1000.0}, {"bls": 1}, 60, state_path)
    first_limiter = first.limiter("bls")
    first_limiter.wait()
    first_limiter.flush_claim_observations()

    first_activity = first.rate_activity()["providers"]["bls"]
    assert first_activity["limiter_claims_current_provider_day"] == 1
    assert first_activity["declared_daily_request_cap"] == 500
    assert first_activity["declared_daily_requests_remaining"] == 499

    restored = ProviderRuntime({"bls": 1000.0}, {"bls": 1}, 60, state_path)
    restored_activity = restored.rate_activity()["providers"]["bls"]
    assert restored_activity["limiter_claims_total"] == 0
    assert restored_activity["limiter_claims_current_provider_day"] == 1
    restored_limiter = restored.limiter("bls")
    restored_limiter.wait()
    restored_limiter.flush_claim_observations()
    assert (
        restored.rate_activity()["providers"]["bls"][
            "limiter_claims_current_provider_day"
        ]
        == 2
    )


def test_provider_runtime_persists_observed_quota_limit_evidence(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime({"tiingo": 1000.0}, {"tiingo": 1}, 60, state_path)
    runtime.limiter("tiingo").wait()
    runtime.block_quota("tiingo", "hourly request allocation exceeded")

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    evidence = payload["observed_quota_limits"]["tiingo"]
    assert evidence["window"] == "hourly"
    assert evidence["observed_claims_at_limit_response"] == 1
    assert evidence["window_key"].endswith(":00:00Z")
    assert evidence["declared_cap"] == 50
    assert evidence["managed_claim_count_is_observational"] is True


def test_provider_runtime_adapts_saturated_concurrency_without_changing_rps(
    tmp_path: Path,
) -> None:
    runtime = ProviderRuntime({"fred": 2.0}, {"fred": 8}, 60)
    semaphore = runtime.semaphore("fred")
    for _ in range(8):
        assert semaphore.acquire(blocking=False)
    now = time.time()
    runtime._rate_session_started_at = now - 60.0
    runtime._rate_claim_totals["fred"] = 10
    runtime._rate_claim_times["fred"] = deque(now - index for index in range(10))
    runtime._active_calls["fred"] = 8
    try:
        activity = runtime.rate_activity()["providers"]["fred"]
        assert runtime.rps["fred"] == 2.0
        assert runtime.concurrency["fred"] == 9
        assert activity["effective_concurrency"] == 9
        assert activity["adaptive_concurrency_cap"] == 30
        assert activity["concurrency_expansions"] == 1
        assert semaphore.limit == 9
        assert semaphore.acquire(blocking=False)
        semaphore.release()
    finally:
        runtime._active_calls["fred"] = 0
        for _ in range(8):
            semaphore.release()


def test_provider_runtime_expands_sec_supply_without_changing_http_rps() -> None:
    runtime = ProviderRuntime({"sec": 10.0}, {"sec": 72}, 60)
    now = time.time()
    runtime._rate_session_started_at = now - 60.0
    runtime._rate_claim_totals["sec"] = 50
    runtime._rate_claim_times["sec"] = deque(now - (index * 1.1) for index in range(50))
    runtime._active_calls["sec"] = 72

    activity = runtime.rate_activity()["providers"]["sec"]

    assert runtime.rps["sec"] == 10.0
    assert runtime.concurrency["sec"] == 77
    assert activity["effective_concurrency"] == 77
    assert activity["adaptive_concurrency_cap"] == 128
    assert activity["concurrency_expansions"] == 1


def test_provider_runtime_uses_semantic_daily_and_hourly_quota_windows(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime(
        {"bls": 1000.0, "fmp": 1000.0, "tiingo": 1000.0},
        {"bls": 1, "fmp": 1, "tiingo": 1},
        3600,
        state_path,
    )
    before = time.time()
    runtime.block_quota("fmp", "429 Limit Reach")
    runtime.block_quota("bls", "daily threshold has been reached")
    runtime.block_quota("tiingo", "hourly request allocation exceeded")
    deadlines = runtime.cooldown_deadlines()

    fixed_est = timezone(timedelta(hours=-5))
    fmp_reset = datetime.fromisoformat(deadlines["fmp"]).astimezone(fixed_est)
    assert (fmp_reset.hour, fmp_reset.minute) == (15, 5)
    bls_reset = datetime.fromisoformat(deadlines["bls"]).astimezone(
        ZoneInfo("America/New_York")
    )
    assert (bls_reset.hour, bls_reset.minute) == (0, 5)
    tiingo_reset = datetime.fromisoformat(deadlines["tiingo"]).timestamp()
    assert 3600 <= tiingo_reset - before <= 3720


def test_provider_runtime_treats_daily_request_allocation_as_daily_window(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime({"tiingo": 1000.0}, {"tiingo": 1}, 3600, state_path)

    runtime.block_quota("tiingo", "daily request allocation exceeded")

    fixed_est = timezone(timedelta(hours=-5))
    reset = datetime.fromisoformat(runtime.cooldown_deadlines()["tiingo"]).astimezone(
        fixed_est
    )
    assert (reset.hour, reset.minute) == (0, 5)
    evidence = json.loads(state_path.read_text(encoding="utf-8"))[
        "observed_quota_limits"
    ]["tiingo"]
    assert evidence["window"] == "daily"
    assert evidence["declared_cap"] == 1000
    activity = runtime.rate_activity()["providers"]["tiingo"]
    assert activity["provider_day_basis"] == "midnight EST"
    assert activity["current_provider_day_key"].startswith("tiingo-est/")


def test_provider_runtime_upgrades_old_daily_quota_checkpoint(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    old_deadline = time.time() + 3600
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 6,
                "providers": {
                    "tiingo": {
                        "blocked_until": old_deadline,
                        "kind": "quota",
                        "reason": "daily request allocation exceeded",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    runtime = ProviderRuntime({"tiingo": 1000.0}, {"tiingo": 1}, 3600, state_path)

    fixed_est = timezone(timedelta(hours=-5))
    upgraded = datetime.fromisoformat(
        runtime.cooldown_deadlines()["tiingo"]
    ).astimezone(fixed_est)
    assert upgraded.timestamp() > time.time()
    assert (upgraded.hour, upgraded.minute) == (0, 5)


def test_provider_runtime_publishes_normalized_legacy_daily_deadlines(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    legacy_deadline = time.time() + 3600
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "providers": {"bls": legacy_deadline, "fmp": legacy_deadline},
            }
        ),
        encoding="utf-8",
    )
    runtime = ProviderRuntime(
        {"bls": 1000.0, "fmp": 1000.0},
        {"bls": 1, "fmp": 1},
        3600,
        state_path,
    )

    persisted = json.loads(state_path.read_text(encoding="utf-8"))["providers"]
    for provider, deadline_iso in runtime.cooldown_deadlines().items():
        assert persisted[provider]["blocked_until"] == pytest.approx(
            datetime.fromisoformat(deadline_iso).timestamp()
        )
        assert persisted[provider]["blocked_until"] >= legacy_deadline
        assert persisted[provider]["kind"] == "legacy"


def test_provider_runtime_persists_unavailable_provider_and_route(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    first = ProviderRuntime(
        {"fmp": 1000.0, "tiingo": 1000.0},
        {"fmp": 1, "tiingo": 1},
        3600,
        state_path,
    )
    first.disable_route("fmp", "equity.ownership.institutional", "premium route")
    first.disable_domain("fmp", "equity.fundamental.metrics", "tw", "global plan")
    first.disable(
        "fmp",
        "fmp: UnauthorizedError: Unauthorized FMP request -> 404 -> []",
    )
    first.disable("tiingo", "invalid credential")

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 8
    restored = ProviderRuntime(
        {"fmp": 1000.0, "tiingo": 1000.0},
        {"fmp": 1, "tiingo": 1},
        3600,
        state_path,
    )
    assert restored.clear_false_global_unavailable() == ("fmp",)
    assert restored.availability("fmp", "equity.ownership.institutional")[0] is False
    assert restored.availability("fmp", "equity.profile")[0] is True
    assert restored.availability("fmp", "equity.fundamental.metrics", "tw")[0] is False
    assert restored.availability("fmp", "equity.fundamental.metrics", "us")[0] is True
    assert restored.availability("tiingo", "equity.price.historical")[0] is False
    assert restored.clear_false_global_unavailable() == ()


def test_provider_runtime_does_not_parse_series_id_401_as_http_auth(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    runtime = ProviderRuntime(
        {"fred": 2.0},
        {"fred": 1},
        3600,
        state_path,
    )
    runtime.disable(
        "fred",
        "fred: ContentTypeError: 500 for "
        "https://api.stlouisfed.org/fred/series/observations?"
        "series_id=GRCPREND401IXOBQ&api_key=<redacted>",
    )

    restored = ProviderRuntime(
        {"fred": 2.0},
        {"fred": 1},
        3600,
        state_path,
    )
    assert restored.clear_false_global_unavailable() == ("fred",)
    assert restored.availability("fred", "economy.fred_series")[0] is True


@pytest.mark.parametrize(
    "message",
    (
        "fred: HTTP 401 Unauthorized",
        "fred: status code=401",
        "fred: Unauthorized request -> 401 -> invalid key",
        "fred: 401 Client Error: Unauthorized",
    ),
)
def test_provider_runtime_preserves_contextual_http_401_auth(
    tmp_path: Path, message: str
) -> None:
    state_path = tmp_path / message.split(":", 1)[1].strip().replace("/", "_")
    runtime = ProviderRuntime(
        {"fred": 2.0},
        {"fred": 1},
        3600,
        state_path,
    )
    runtime.disable("fred", message)

    assert runtime.clear_false_global_unavailable() == ()
    assert runtime.availability("fred", "economy.fred_series")[0] is False


def test_manifest_prioritizes_latest_canonical_fmp_domain_probes(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")

    def task(task_id: str, symbol: str, year: int) -> DownloadTask:
        return DownloadTask(
            task_id=task_id,
            endpoint="equity.ownership.major_holders",
            category="equity",
            scope_key=f"{symbol}/year={year}/quarter=1/page=0",
            kwargs={"symbol": symbol, "year": year, "quarter": 1, "page": 0},
            providers=("fmp",),
            output_path=str(tmp_path / "data" / f"{task_id}.parquet"),
        )

    tasks = [
        task("aapl-old", "AAPL", 2000),
        task("aapl-latest", "AAPL", 2026),
        task("random-latest", "ZZZZ", 2026),
        task("tw-latest", "2330.TW", 2026),
    ]
    try:
        manifest.upsert_tasks(tasks, plan_token="plan")
        selected = manifest.prioritize_fmp_entitlement_probes("plan", "2026-07-18")
        assert selected == {"aapl-latest", "tw-latest"}
        prioritized = {
            str(row["task_id"]): str(row["updated_at"])
            for row in manifest.connection.execute(
                "SELECT task_id,updated_at FROM tasks WHERE task_id IN (?,?)",
                ("aapl-latest", "tw-latest"),
            )
        }
        assert set(prioritized.values()) == {"0001-01-01T00:00:00+00:00"}

        resolved = manifest.finalize_provider_domain_unavailable(
            "fmp",
            "equity.ownership.major_holders",
            "tw",
            "subscription excludes global coverage",
            plan_token="plan",
        )
        assert resolved == 1
        statuses = {
            str(row["task_id"]): str(row["status"])
            for row in manifest.connection.execute(
                "SELECT task_id,status FROM tasks ORDER BY task_id"
            )
        }
        assert statuses["tw-latest"] == "unavailable"
        assert statuses["aapl-latest"] == "pending"
        assert statuses["random-latest"] == "pending"
    finally:
        manifest.close()


def test_manifest_finalizes_only_tasks_with_no_available_route_provider(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    both_denied = make_task(
        context,
        "news.company",
        "AAPL/year=2020/page=0",
        {"symbol": "AAPL"},
        ("fmp", "tiingo"),
    )
    fallback_remains = make_task(
        context,
        "news.company",
        "MSFT/year=2020/page=0",
        {"symbol": "MSFT"},
        ("yfinance", "fmp", "tiingo"),
    )
    fmp_only = make_task(
        context,
        "etf.search",
        "exchange=nyse",
        {"exchange": "nyse"},
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [both_denied, fallback_remains, fmp_only], plan_token="plan"
        )
        resolved = manifest.finalize_fully_route_unavailable(
            {},
            {
                ("fmp", "news.company"): "restricted",
                ("tiingo", "news.company"): "restricted",
                ("fmp", "etf.search"): "restricted",
            },
            plan_token="plan",
        )
        assert resolved == 2
        rows = {
            row["task_id"]: row
            for row in manifest.connection.execute(
                "SELECT task_id,status,provider_outcomes_json FROM tasks"
            )
        }
        assert rows[both_denied.task_id]["status"] == "unavailable"
        assert rows[fmp_only.task_id]["status"] == "unavailable"
        assert rows[fallback_remains.task_id]["status"] == "pending"
        assert json.loads(rows[both_denied.task_id]["provider_outcomes_json"]) == {
            "fmp": "unavailable",
            "tiingo": "unavailable",
        }
    finally:
        manifest.close()


def test_manifest_combines_provider_route_and_market_domain_capabilities(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    mixed_scope_denied = make_task(
        context,
        "equity.price.historical",
        "2330.TW",
        {"symbol": "2330.TW"},
        ("fmp", "tiingo"),
    )
    market_fallback_remains = make_task(
        context,
        "equity.price.historical",
        "2317.TW",
        {"symbol": "2317.TW"},
        ("fmp", "tiingo", "yfinance"),
    )
    other_market_remains = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {"symbol": "AAPL"},
        ("fmp", "tiingo"),
    )
    multi_symbol_denied = make_task(
        context,
        "equity.price.historical",
        "2330.TW,2317.TW",
        {"symbol": "2330.TW,2317.TW"},
        ("fmp", "tiingo"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [
                mixed_scope_denied,
                market_fallback_remains,
                other_market_remains,
                multi_symbol_denied,
            ],
            plan_token="plan",
        )
        resolved = manifest.finalize_fully_capability_unavailable(
            {},
            {("fmp", "equity.price.historical"): "route restricted"},
            {
                (
                    "tiingo",
                    "equity.price.historical",
                    "tw",
                ): "TW namespace unsupported"
            },
            plan_token="plan",
        )

        assert resolved == 2
        statuses = {
            str(row["task_id"]): str(row["status"])
            for row in manifest.connection.execute("SELECT task_id,status FROM tasks")
        }
        assert statuses[mixed_scope_denied.task_id] == "unavailable"
        assert statuses[multi_symbol_denied.task_id] == "unavailable"
        assert statuses[market_fallback_remains.task_id] == "pending"
        assert statuses[other_market_remains.task_id] == "pending"
    finally:
        manifest.close()


def test_manifest_requeues_all_pageable_tasks_for_learned_limit(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    tasks = [
        make_task(
            context,
            "equity.fundamental.filings",
            f"{symbol}/year=2025/page=0",
            {"symbol": symbol, "limit": 1000, "page": 0},
            ("fmp",),
        )
        for symbol in ("2330.TW", "2317.TW")
    ]
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="plan")
        manifest.connection.execute(
            "UPDATE tasks SET status='unavailable', error=?",
            (
                "fmp: Premium Query Parameter: The values for 'limit' "
                "must be between 0 and 20",
            ),
        )
        manifest.connection.commit()

        repaired = manifest.repair_adaptable_parameter_constraints(
            {("fmp", "equity.fundamental.filings", "limit"): 20},
            plan_token="plan",
        )

        assert repaired == 2
        rows = manifest.connection.execute(
            "SELECT status,kwargs_json FROM tasks ORDER BY task_id"
        ).fetchall()
        assert {str(row["status"]) for row in rows} == {"pending"}
        assert {int(json.loads(str(row["kwargs_json"]))["limit"]) for row in rows} == {
            20
        }
    finally:
        manifest.close()


def test_manifest_requeues_nonpageable_task_without_explicit_limit(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.employee_count",
        "AAPL/year=2025",
        {
            "symbol": "AAPL",
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
        },
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="plan")
        manifest.connection.execute(
            "UPDATE tasks SET status='unavailable', error=?",
            (
                "fmp: Premium Query Parameter: The values for 'limit' "
                "must be between 0 and 5",
            ),
        )
        manifest.connection.commit()

        repaired = manifest.repair_adaptable_parameter_constraints(
            {("fmp", "equity.fundamental.employee_count", "limit"): 5},
            plan_token="plan",
        )

        row = manifest.connection.execute(
            "SELECT status,kwargs_json FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert repaired == 1
        assert row["status"] == "pending"
        assert json.loads(row["kwargs_json"])["limit"] == 5
    finally:
        manifest.close()


def test_manifest_requeues_market_cap_for_undated_query_shape(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.historical_market_cap",
        "AAPL",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="plan")
        manifest.connection.execute(
            "UPDATE tasks SET status='unavailable', error=?",
            (
                "fmp: Premium Query Parameter: This value set for 'from' "
                "is not available under your subscription",
            ),
        )
        manifest.connection.commit()

        repaired = manifest.repair_adaptable_query_shapes(
            {
                ("fmp", "equity.historical_market_cap"): (
                    "end_date",
                    "start_date",
                )
            },
            plan_token="plan",
        )

        row = manifest.connection.execute(
            "SELECT status,attempts,error FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert repaired == 1
        assert tuple(row) == (
            "pending",
            0,
            "requeued: use entitlement-compatible undated query",
        )
    finally:
        manifest.close()


def test_manifest_finalizes_pending_when_all_provider_outcomes_are_terminal(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    empty = make_task(context, "equity.profile", "empty", {"symbol": "A"}, ("fmp",))
    mixed = make_task(
        context,
        "equity.profile",
        "mixed",
        {"symbol": "B"},
        ("fmp", "yfinance"),
    )
    permanent = make_task(
        context, "equity.profile", "permanent", {"symbol": "C"}, ("fmp",)
    )
    incomplete = make_task(
        context,
        "equity.profile",
        "incomplete",
        {"symbol": "D"},
        ("fmp", "yfinance"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([empty, mixed, permanent, incomplete], plan_token="plan")
        manifest.connection.executemany(
            "UPDATE tasks SET provider_outcomes_json=?,error='old transient' "
            "WHERE task_id=?",
            [
                ('{"fmp":"empty"}', empty.task_id),
                (
                    '{"fmp":"empty","yfinance":"unavailable"}',
                    mixed.task_id,
                ),
                ('{"fmp":"permanent"}', permanent.task_id),
                ('{"fmp":"empty"}', incomplete.task_id),
            ],
        )
        manifest.connection.commit()

        finalized = manifest.finalize_resolved_provider_outcome_pending(
            plan_token="plan"
        )

        assert finalized == 3
        statuses = {
            str(row["scope_key"]): str(row["status"])
            for row in manifest.connection.execute("SELECT scope_key,status FROM tasks")
        }
        assert statuses == {
            "empty": "empty",
            "mixed": "unavailable",
            "permanent": "failed",
            "incomplete": "pending",
        }
        repaired_error = manifest.connection.execute(
            "SELECT error FROM tasks WHERE task_id=?", (empty.task_id,)
        ).fetchone()[0]
        assert "old transient" in str(repaired_error)
    finally:
        manifest.close()


def test_manifest_requeues_unavailable_outcome_without_durable_evidence(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    unproven = make_task(
        context,
        "equity.estimates.consensus",
        "DYNB",
        {"symbol": "DYNB"},
        ("fmp", "yfinance"),
    )
    proven = make_task(
        context,
        "equity.estimates.consensus",
        "JGBS",
        {"symbol": "JGBS"},
        ("fmp", "yfinance"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([unproven, proven], plan_token="plan")
        manifest.connection.execute(
            "UPDATE tasks SET status='unavailable',attempts=1,"
            "provider_outcomes_json=? WHERE task_id=?",
            (json.dumps({"fmp": "unavailable", "yfinance": "empty"}), unproven.task_id),
        )
        manifest.connection.execute(
            "UPDATE tasks SET status='unavailable',attempts=1,"
            "provider_outcomes_json=?,provider_evidence_json=? WHERE task_id=?",
            (
                json.dumps({"fmp": "unavailable", "yfinance": "empty"}),
                json.dumps({"fmp": "HTTP 402 Restricted Endpoint"}),
                proven.task_id,
            ),
        )
        manifest.connection.commit()

        repaired = manifest.repair_unproven_provider_outcomes(
            {}, {}, {}, plan_token="plan"
        )

        assert repaired == 1
        rows = {
            row["scope_key"]: row
            for row in manifest.connection.execute(
                "SELECT scope_key,status,attempts,provider_outcomes_json FROM tasks"
            )
        }
        assert rows["DYNB"]["status"] == "pending"
        assert rows["DYNB"]["attempts"] == 0
        assert json.loads(rows["DYNB"]["provider_outcomes_json"]) == {
            "yfinance": "empty"
        }
        assert rows["JGBS"]["status"] == "unavailable"
        assert json.loads(rows["JGBS"]["provider_outcomes_json"]) == {
            "fmp": "unavailable",
            "yfinance": "empty",
        }
    finally:
        manifest.close()


def test_manifest_requeues_only_permanent_outcome_without_error_evidence(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    unproven = make_task(
        context,
        "equity.fundamental.income",
        "PRCH/period=quarter",
        {"symbol": "PRCH", "period": "quarter"},
        ("sec", "yfinance"),
    )
    proven = make_task(
        context,
        "equity.fundamental.income",
        "PROVEN/period=quarter",
        {"symbol": "PROVEN", "period": "quarter"},
        ("sec", "yfinance"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([unproven, proven], plan_token="plan")
        manifest.connection.execute(
            "UPDATE tasks SET status='success',selected_provider='yfinance',"
            "attempts=2,rows=5,provider_outcomes_json=? WHERE task_id=?",
            (json.dumps({"sec": "permanent"}), unproven.task_id),
        )
        manifest.connection.execute(
            "UPDATE tasks SET status='success',selected_provider='yfinance',"
            "attempts=2,rows=5,provider_outcomes_json=?,provider_evidence_json=? "
            "WHERE task_id=?",
            (
                json.dumps({"sec": "permanent"}),
                json.dumps({"sec": "sec: ValueError: proven schema mismatch"}),
                proven.task_id,
            ),
        )
        manifest.connection.commit()

        repaired = manifest.repair_unproven_permanent_outcomes(plan_token="plan")

        assert repaired == 1
        rows = {
            row["scope_key"]: row
            for row in manifest.connection.execute(
                "SELECT scope_key,status,selected_provider,attempts,rows,"
                "provider_outcomes_json FROM tasks"
            )
        }
        assert rows["PRCH/period=quarter"]["status"] == "pending"
        assert rows["PRCH/period=quarter"]["selected_provider"] is None
        assert rows["PRCH/period=quarter"]["attempts"] == 1
        assert rows["PRCH/period=quarter"]["rows"] == 0
        assert json.loads(rows["PRCH/period=quarter"]["provider_outcomes_json"]) == {}
        assert rows["PROVEN/period=quarter"]["status"] == "success"
        assert json.loads(rows["PROVEN/period=quarter"]["provider_outcomes_json"]) == {
            "sec": "permanent"
        }
    finally:
        manifest.close()


def test_manifest_infers_only_empty_tiingo_tw_route_and_keeps_other_domains(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    empty_tasks = [
        make_task(
            context,
            "equity.price.historical",
            f"{1000 + index}.TW",
            {"symbol": f"{1000 + index}.TW"},
            ("tiingo",),
        )
        for index in range(25)
    ]
    etf_tasks = [
        make_task(
            context,
            "etf.historical",
            f"00{index:03d}.TWO",
            {"symbol": f"00{index:03d}.TWO"},
            ("tiingo",),
        )
        for index in range(25)
    ]
    etf_success = etf_tasks[0]
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([*empty_tasks, *etf_tasks], plan_token="archive")
        manifest.connection.executemany(
            "UPDATE tasks SET provider_outcomes_json=? WHERE task_id=?",
            [('{"tiingo":"empty"}', task.task_id) for task in empty_tasks],
        )
        manifest.connection.executemany(
            "UPDATE tasks SET provider_outcomes_json=? WHERE task_id=?",
            [('{"tiingo":"empty"}', task.task_id) for task in etf_tasks],
        )
        manifest.connection.execute(
            "UPDATE tasks SET status='success',selected_provider='tiingo',rows=1 "
            "WHERE task_id=?",
            (etf_success.task_id,),
        )
        manifest.connection.commit()

        inferred = manifest.empty_only_provider_domain_routes(
            "tiingo",
            "tw",
            plan_token="archive",
            minimum_distinct_scopes=25,
        )

        assert inferred == {"equity.price.historical": 25}
        assert _provider_capability_domain("tiingo", {"symbol": "2330.TW"}) == "tw"
        assert _provider_capability_domain("tiingo", {"symbol": "AAPL"}) is None
        resolved = manifest.finalize_provider_domain_unavailable(
            "tiingo",
            "equity.price.historical",
            "tw",
            "TW namespace unsupported",
            plan_token="archive",
        )
        assert resolved == 25
        assert (
            manifest.connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE endpoint='etf.historical' "
                "AND status='success'"
            ).fetchone()[0]
            == 1
        )
    finally:
        manifest.close()


def test_provider_runtime_clears_only_reasonless_legacy_dns_cooldown(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "_state" / "provider_cooldowns.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "providers": {
                    "federal_reserve": time.time() + 3600,
                    "fmp": time.time() + 3600,
                },
            }
        ),
        encoding="utf-8",
    )
    runtime = ProviderRuntime(
        {"federal_reserve": 10.0, "fmp": 10.0},
        {"federal_reserve": 1, "fmp": 1},
        3600,
        state_path,
    )

    assert runtime.legacy_cooldown_providers() == {"federal_reserve", "fmp"}
    assert runtime.clear_legacy_cooldowns({"federal_reserve"}) == ("federal_reserve",)
    assert runtime.availability("federal_reserve")[0] is True
    assert runtime.availability("fmp")[0] is False
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert set(persisted["providers"]) == {"fmp"}


def _context(tmp_path: Path) -> PlannerContext:
    commands = {
        ".equity.price.historical": ["tiingo", "fmp", "yfinance"],
        ".equity.price.quote": ["fmp", "yfinance"],
    }
    schemas = {
        ".equity.price.historical": {
            "input": _InputModel,
            "callable": lambda symbol, **kwargs: None,
        },
        ".equity.price.quote": {
            "input": _InputModel,
            "callable": lambda symbol, **kwargs: None,
        },
    }
    from downloader.download_openbb_archive import AssetRecord

    asset = AssetRecord("AAPL", "Apple", "us", "stock")
    return PlannerContext(
        schemas=schemas,
        commands=commands,
        output_dir=tmp_path,
        start_date="2000-01-01",
        end_date="2000-12-31",
        assets=[asset],
        etfs=[],
        currencies=[],
        indices=[],
        countries=["us"],
        allowed_providers=None,
        disabled_providers=set(),
        endpoint_filters=(),
        categories=None,
    )


def test_snapshot_routes_are_excluded_and_tiingo_is_fallback(tmp_path: Path) -> None:
    context = _context(tmp_path)
    tasks, coverage = build_initial_plan(context)
    assert "equity.price.quote" in SNAPSHOT_ENDPOINTS
    assert HISTORICAL_PRICE_ENDPOINTS
    assert [task.endpoint for task in tasks] == ["equity.price.historical"]
    assert tasks[0].providers == ("yfinance", "fmp", "tiingo")
    quote = next(item for item in coverage if item.endpoint == "equity.price.quote")
    assert quote.decision == "excluded"


def test_streamed_planner_publishes_atomic_phase_progress(tmp_path: Path) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "manifest.sqlite3")
    try:
        task_count, _ = populate_initial_plan(
            context,
            manifest,
            plan_token="plan",
            plan_generation="generation",
        )
    finally:
        manifest.close()

    payload = json.loads(
        (tmp_path / "_state" / "downloader_phase.json").read_text(encoding="utf-8")
    )
    assert payload["phase"] == "planning"
    assert payload["endpoints_completed"] == len(context.commands)
    assert payload["endpoints_total"] == len(context.commands)
    assert payload["generated_tasks"] == task_count


def test_provider_order_never_makes_tiingo_primary() -> None:
    assert provider_order("equity.price.historical", ["tiingo", "fmp", "yfinance"]) == (
        "yfinance",
        "fmp",
        "tiingo",
    )


def test_provider_order_preserves_scarce_fmp_for_exclusive_routes() -> None:
    assert provider_order("equity.profile", ["fmp", "tiingo", "yfinance"]) == (
        "yfinance",
        "fmp",
        "tiingo",
    )
    assert provider_execution_order(("sec", "fmp", "yfinance", "tiingo")) == (
        "sec",
        "yfinance",
        "fmp",
        "tiingo",
    )


def test_every_time_sharded_endpoint_has_closed_provider_contract() -> None:
    assert set(ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST) == set(
        ARCHIVE_TIME_SHARDED_ENDPOINTS
    )
    assert not {
        pair
        for pair in LOCAL_ONLY_ARCHIVE_DATE_FILTERS
        if pair[1] in ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST.get(pair[0], frozenset())
    }


@pytest.mark.parametrize(
    ("endpoint", "available", "expected"),
    [
        (
            "news.company",
            ["yfinance", "fmp", "tiingo"],
            ("fmp", "tiingo"),
        ),
        (
            "economy.calendar",
            ["tradingeconomics", "fred", "fmp"],
            ("fred", "fmp"),
        ),
        (
            "equity.calendar.ipo",
            ["yfinance", "intrinio", "fmp"],
            ("fmp", "intrinio"),
        ),
    ],
)
def test_time_shards_keep_only_upstream_bounded_providers(
    tmp_path: Path,
    endpoint: str,
    available: list[str],
    expected: tuple[str, ...],
) -> None:
    assert select_providers(endpoint, available, _context(tmp_path)) == expected


def test_full_range_local_filter_is_not_mistaken_for_a_time_shard(
    tmp_path: Path,
) -> None:
    endpoint = "equity.fundamental.employee_count"
    assert endpoint not in ARCHIVE_TIME_SHARDED_ENDPOINTS
    assert (endpoint, "fmp") in LOCAL_ONLY_ARCHIVE_DATE_FILTERS
    assert select_providers(endpoint, ["fmp"], _context(tmp_path)) == ("fmp",)


def test_employee_count_is_planned_once_per_symbol_for_full_archive_range(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.end_date = "2002-12-31"
    endpoint = "equity.fundamental.employee_count"
    context.schemas[f".{endpoint}"] = {
        "input": _InputModel,
        "callable": lambda symbol, **kwargs: None,
    }
    tasks = list(_symbol_tasks(context, endpoint, ("fmp",)))

    assert len(tasks) == 1
    assert tasks[0].scope_key == "AAPL"
    assert tasks[0].kwargs == {
        "symbol": "AAPL",
        "start_date": "2000-01-01",
        "end_date": "2002-12-31",
    }
    assert tasks[0].providers == ("fmp",)


def test_sec_ftd_is_planned_once_per_stable_half_month_report(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.start_date = "2008-01-01"
    context.end_date = "2010-02-20"
    context.commands = {".equity.shorts.fails_to_deliver": ["sec"]}
    context.schemas = {
        ".equity.shorts.fails_to_deliver": {
            "input": _InputModel,
            "callable": lambda symbol, **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert len(tasks) == 28
    assert coverage[0].initial_task_count == 28
    assert tasks[0].scope_key == "report=200901a"
    assert tasks[-1].scope_key == "report=201002b"
    assert tasks[-1].kwargs == {
        "report_key": "201002b",
        "start_date": "2010-02-16",
        "end_date": "2010-02-20",
        "use_cache": True,
    }
    assert all("symbol" not in task.kwargs for task in tasks)
    assert list(_sec_ftd_report_periods("2008-01-01", "2008-12-31")) == []
    assert len(list(_symbol_tasks(context, tasks[0].endpoint, ("sec",)))) == 28


def test_sec_ftd_report_workaround_caches_catalog_and_filters_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive
    import openbb_sec.utils.helpers as sec_helpers

    catalog_calls = 0
    download_calls: list[tuple[str, str | None, bool]] = []

    async def fake_catalog():
        nonlocal catalog_calls
        catalog_calls += 1
        return {"202601a": "https://www.sec.gov/files/cnsfails202601a.zip"}

    async def fake_download(url, symbol=None, use_cache=True):
        download_calls.append((url, symbol, use_cache))
        return [
            {"date": "2025-12-31", "symbol": "OLD"},
            {"date": "2026-01-02", "symbol": "AAPL"},
            {"date": "2026-01-16", "symbol": "LATE"},
        ]

    limiter = _NoopLimiter()
    monkeypatch.setattr(sec_helpers, "get_ftd_urls", fake_catalog)
    monkeypatch.setattr(sec_helpers, "download_zip_file", fake_download)
    monkeypatch.setattr(archive, "_SEC_FTD_URLS_CACHE", None)

    kwargs = {
        "report_key": "202601a",
        "start_date": "2026-01-01",
        "end_date": "2026-01-15",
        "use_cache": True,
    }
    assert _fetch_sec_ftd_report_workaround(kwargs, page_limiter=limiter) == [
        {"date": "2026-01-02", "symbol": "AAPL"}
    ]
    assert _fetch_sec_ftd_report_workaround(kwargs, page_limiter=limiter) == [
        {"date": "2026-01-02", "symbol": "AAPL"}
    ]
    assert catalog_calls == 1
    assert download_calls == [
        ("https://www.sec.gov/files/cnsfails202601a.zip", None, True),
        ("https://www.sec.gov/files/cnsfails202601a.zip", None, True),
    ]


def test_fmp_ownership_archives_completed_quarters_and_pages(
    tmp_path: Path,
) -> None:
    class _OwnershipInput:
        model_fields = {
            "symbol": _Field(str),
            "year": _Field(int),
            "quarter": _Field(int),
            "page": _Field(int),
            "limit": _Field(int),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-02-01"
    context.end_date = "2000-10-01"
    endpoints = (
        "equity.ownership.institutional",
        "equity.ownership.major_holders",
    )
    context.commands = {f".{endpoint}": ["fmp"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {
            "input": _OwnershipInput,
            "callable": lambda symbol, **kwargs: None,
        }
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)

    assert list(_completed_quarters(context.start_date, context.end_date)) == [
        (2000, 1),
        (2000, 2),
        (2000, 3),
    ]
    assert [item.initial_task_count for item in coverage] == [3, 3]
    institutional = [
        task for task in tasks if task.endpoint == "equity.ownership.institutional"
    ]
    holders = [
        task for task in tasks if task.endpoint == "equity.ownership.major_holders"
    ]
    assert institutional[0].scope_key == "AAPL/year=2000/quarter=1"
    assert institutional[-1].kwargs == {
        "symbol": "AAPL",
        "year": 2000,
        "quarter": 3,
    }
    assert holders[0].scope_key == "AAPL/year=2000/quarter=1/page=0"
    assert holders[0].kwargs["limit"] == 100
    assert holders[0].kwargs["page"] == 0

    continuation = discover_followup_tasks(
        context,
        TaskResult(
            holders[0],
            "success",
            "fmp",
            100,
            holders[0].output_path,
            1,
            records=[{}],
        ),
    )
    assert len(continuation) == 1
    assert continuation[0].scope_key == "AAPL/year=2000/quarter=1/page=1"
    assert continuation[0].kwargs["page"] == 1


def test_insider_trading_keeps_transactions_and_fmp_statistics(
    tmp_path: Path,
) -> None:
    class _InsiderInput:
        model_fields = {
            "symbol": _Field(str),
            "start_date": _Field(str),
            "end_date": _Field(str),
            "limit": _Field(int),
            "statistics": _Field(bool),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {".equity.ownership.insider_trading": ["fmp", "sec"]}
    context.schemas = {
        ".equity.ownership.insider_trading": {
            "input": _InsiderInput,
            "callable": lambda symbol, **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 2
    transactions = next(
        task for task in tasks if task.scope_key.startswith("AAPL/legacy=")
    )
    statistics = next(
        task for task in tasks if task.scope_key == "AAPL/mode=statistics"
    )
    assert transactions.providers == ("sec", "fmp")
    assert transactions.kwargs["limit"] == 10000
    assert transactions.kwargs["start_date"] == "2000-01-01"
    assert transactions.kwargs["end_date"] == "2000-12-31"
    assert transactions.kwargs["_archive_sec_insider_range"] is True
    assert statistics.providers == ("fmp",)
    assert statistics.kwargs == {"symbol": "AAPL", "statistics": True}


def test_insider_trading_uses_one_sec_task_per_published_quarter(
    tmp_path: Path,
) -> None:
    import downloader.download_openbb_archive as archive

    class _InsiderInput:
        model_fields = {
            "symbol": _Field(str),
            "start_date": _Field(str),
            "end_date": _Field(str),
            "limit": _Field(int),
            "statistics": _Field(bool),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2006-01-01"
    context.end_date = date.today().isoformat()
    context.commands = {".equity.ownership.insider_trading": ["fmp", "sec"]}
    context.schemas = {
        ".equity.ownership.insider_trading": {
            "input": _InsiderInput,
            "callable": lambda symbol, **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    expected_bulk = list(
        archive._sec_insider_bulk_quarters(context.start_date, context.end_date)
    )
    bulk = [task for task in tasks if task.scope_key.startswith("bulk/year=")]
    tail = [task for task in tasks if "/tail=" in task.scope_key]
    statistics = [task for task in tasks if task.scope_key.endswith("/mode=statistics")]
    assert len(bulk) == len(expected_bulk)
    assert len(tail) == 1
    assert len(statistics) == 1
    assert coverage[0].initial_task_count == len(bulk) + 2
    assert all(task.providers == ("sec",) for task in [*bulk, *tail])
    assert all(task.kwargs["_archive_sec_insider_bulk"] for task in bulk)
    assert tail[0].kwargs["_archive_sec_insider_tail"] is True
    assert not any(task.scope_key == "AAPL" for task in tasks)


def test_sec_insider_bulk_parser_joins_official_quarter_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from zipfile import ZIP_DEFLATED, ZipFile

    import downloader.download_openbb_archive as archive

    archive_path = tmp_path / "quarterly" / "2024q1_form345.zip"
    archive_path.parent.mkdir(parents=True)
    tables = {
        "SUBMISSION.tsv": (
            "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDATE_OF_ORIG_SUB\t"
            "DOCUMENT_TYPE\tISSUERCIK\tISSUERNAME\tISSUERTRADINGSYMBOL\tREMARKS\tAFF10B5ONE\n"
            "0000320193-24-000001\t15-FEB-2024\t14-FEB-2024\t\t4\t"
            "0000320193\tAPPLE INC\tAAPL\tplanned sale\t1\n"
        ),
        "REPORTINGOWNER.tsv": (
            "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP\t"
            "RPTOWNER_TITLE\tRPTOWNER_TXT\n"
            "0000320193-24-000001\t0000000001\tEXAMPLE OWNER\tOfficer\tCEO\t\n"
        ),
        "NONDERIV_TRANS.tsv": (
            "ACCESSION_NUMBER\tNONDERIV_TRANS_SK\tSECURITY_TITLE\tSECURITY_TITLE_FN\t"
            "TRANS_DATE\tDEEMED_EXECUTION_DATE\tTRANS_FORM_TYPE\tTRANS_CODE\t"
            "EQUITY_SWAP_INVOLVED\tTRANS_TIMELINESS\tTRANS_SHARES\t"
            "TRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\tSHRS_OWND_FOLWNG_TRANS\t"
            "VALU_OWND_FOLWNG_TRANS\tDIRECT_INDIRECT_OWNERSHIP\t"
            "NATURE_OF_OWNERSHIP\n"
            "0000320193-24-000001\t1\tCommon Stock\tF1\t14-FEB-2024\t\t4\tS\t"
            "false\t\t10\t20.5\tD\t90\t1845\tD\t\n"
        ),
        "NONDERIV_HOLDING.tsv": "ACCESSION_NUMBER\tNONDERIV_HOLDING_SK\n",
        "DERIV_TRANS.tsv": "ACCESSION_NUMBER\tDERIV_TRANS_SK\n",
        "DERIV_HOLDING.tsv": "ACCESSION_NUMBER\tDERIV_HOLDING_SK\n",
        "FOOTNOTES.tsv": (
            "ACCESSION_NUMBER\tFOOTNOTE_ID\tFOOTNOTE_TXT\n"
            "0000320193-24-000001\tF1\tRule 10b5-1 plan.\n"
        ),
        "OWNER_SIGNATURE.tsv": (
            "ACCESSION_NUMBER\tOWNERSIGNATURENAME\tOWNERSIGNATUREDATE\n"
            "0000320193-24-000001\t/s/ Example Owner\t15-FEB-2024\n"
        ),
    }
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as zip_file:
        for name, content in tables.items():
            zip_file.writestr(name, content)

    source_url = "https://www.sec.gov/files/2024q1_form345.zip"
    monkeypatch.setattr(
        archive,
        "_sec_insider_dataset_catalog",
        lambda limiter: {(2024, 1): source_url},
    )
    records = archive._fetch_sec_insider_bulk_workaround(
        {
            "year": 2024,
            "quarter": 1,
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
        },
        cache_dir=tmp_path,
        page_limiter=None,
        show_progress=False,
    )

    assert len(records) == 1
    record = records[0]
    assert record["symbol"] == "AAPL"
    assert record["owner_name"] == "EXAMPLE OWNER"
    assert record["officer"] is True
    assert record["transaction_code"] == "S"
    assert record["acquisition_or_disposition"] == "Disposition"
    assert record["transaction_value"] == 205.0
    assert record["footnote"] == "Rule 10b5-1 plan."
    assert record["source_dataset_url"] == source_url


def test_sec_insider_range_fetches_raw_xml_not_xsl_rendered_html(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import downloader.download_openbb_archive as archive
    import openbb_sec.utils.form4 as form4
    import xmltodict

    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001140361-26-013192"],
                "filingDate": ["2026-04-03"],
                "form": ["4"],
                "primaryDocument": ["xslF345X06/form4.xml"],
            },
            "files": [],
        }
    }
    requested_urls: list[str] = []

    def _fetch(url, cache_path, **unused):
        requested_urls.append(url)
        if "/submissions/" in url:
            return json.dumps(submissions).encode()
        return b"<ownershipDocument/>"

    async def _parse(data):
        return [
            {
                "symbol": "AAPL",
                "filing_date": "2026-04-03",
                "transaction_type": "S",
            }
        ]

    monkeypatch.setattr(
        archive, "_sec_symbol_cik_map", lambda limiter: {"AAPL": "0000320193"}
    )
    monkeypatch.setattr(archive, "_fetch_sec_cached_bytes", _fetch)
    monkeypatch.setattr(xmltodict, "parse", lambda content: {"ownershipDocument": {}})
    monkeypatch.setattr(form4, "parse_form_4_data", _parse)

    records = archive._fetch_sec_insider_range_workaround(
        {
            "symbol": "AAPL",
            "start_date": "2026-04-01",
            "end_date": "2026-06-30",
        },
        cache_dir=tmp_path,
        page_limiter=None,
        show_progress=False,
    )

    assert len(records) == 1
    assert requested_urls[-1].endswith("/320193/000114036126013192/form4.xml")
    assert "xslF345X06" not in requested_urls[-1]


def test_sec_insider_range_recovers_nullable_form4_mapping_with_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import downloader.download_openbb_archive as archive

    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001493152-26-032674"],
                "filingDate": ["2026-07-02"],
                "form": ["4"],
                "primaryDocument": ["form4.xml"],
            },
            "files": [],
        }
    }
    ownership_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-07-01</periodOfReport>
  <issuer>
    <issuerCik>0001039280</issuerCik>
    <issuerName>NETSOL TECHNOLOGIES INC</issuerName>
    <issuerTradingSymbol>NTWK</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0000000001</rptOwnerCik>
      <rptOwnerName>EXAMPLE OWNER</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship><isDirector>0</isDirector></reportingOwnerRelationship>
  </reportingOwner>
  <ownerSignature><signatureDate>2026-07-02</signatureDate></ownerSignature>
  <nonDerivativeTable>
    <nonDerivativeHolding>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionCoding/>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>100</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeHolding>
  </nonDerivativeTable>
</ownershipDocument>"""

    def _fetch(url, cache_path, **unused):
        del cache_path, unused
        if "/submissions/" in url:
            return json.dumps(submissions).encode()
        return ownership_xml

    monkeypatch.setattr(
        archive,
        "_sec_symbol_cik_map",
        lambda limiter: {"NTWK": "0001039280"},
    )
    monkeypatch.setattr(archive, "_fetch_sec_cached_bytes", _fetch)

    records = archive._fetch_sec_insider_range_workaround(
        {
            "symbol": "NTWK",
            "start_date": "2026-07-01",
            "end_date": "2026-07-18",
        },
        cache_dir=tmp_path,
        page_limiter=None,
        show_progress=False,
    )

    assert len(records) == 1
    assert records[0]["symbol"] == "NTWK"
    recoveries = json.loads(records[0]["openbb_validation_recoveries"])
    assert recoveries == [
        {
            "error_type": "null_mapping",
            "field": "nonDerivativeTable.nonDerivativeHolding.transactionCoding",
            "invalid_value": None,
        }
    ]


def test_sec_form4_normalizer_rejects_unexpected_non_null_shape() -> None:
    import downloader.download_openbb_archive as archive

    with pytest.raises(
        archive.ProviderResponseShapeError,
        match="expected mapping at issuer, got list",
    ) as caught:
        archive._normalize_sec_form4_ownership_document({"issuer": []})
    assert classify_error(caught.value) == "transient"


def test_fmp_insider_trading_pages_to_end_and_filters_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    calls: list[int] = []

    def fake_page(endpoint, params, credential):
        assert endpoint == "insider-trading/search"
        assert credential == "secret"
        page = params["page"]
        calls.append(page)
        if page == 0:
            return [
                {
                    "filingDate": "2000-06-01",
                    "symbol": "AAPL",
                    "link": f"https://example.test/{index}",
                }
                for index in range(1000)
            ]
        return [
            {
                "filingDate": "1999-12-31",
                "symbol": "AAPL",
                "link": "https://example.test/old",
            },
            {
                "filingDate": "2000-07-01",
                "symbol": "AAPL",
                "link": "https://example.test/new",
            },
        ]

    class _CountingLimiter:
        waits = 0

        def wait(self):
            self.waits += 1

    monkeypatch.setattr(archive, "_fmp_page_json", fake_page)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key="secret"))
    )
    limiter = _CountingLimiter()

    rows = _fetch_fmp_insider_trading_workaround(
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
        },
        obb,
        page_limiter=limiter,
    )

    assert len(rows) == 1001
    assert calls == [0, 1]
    assert limiter.waits == 2


def test_fmp_all_history_kwargs_override_recent_defaults(tmp_path: Path) -> None:
    class _ArchiveInput:
        model_fields = {
            "symbol": _Field(str),
            "start_date": _Field(str),
            "end_date": _Field(str),
            "year": _Field(int),
            "limit": _Field(int),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    endpoints = (
        "equity.fundamental.dividends",
        "equity.fundamental.management_compensation",
    )
    context.commands = {f".{endpoint}": ["fmp"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {
            "input": _ArchiveInput,
            "callable": lambda symbol, **kwargs: None,
        }
        for endpoint in endpoints
    }

    tasks, _ = build_initial_plan(context)
    by_endpoint = {task.endpoint: task for task in tasks}
    assert by_endpoint["equity.fundamental.dividends"].kwargs["limit"] == 10000
    assert by_endpoint["equity.fundamental.management_compensation"].kwargs["year"] == 0


def test_fmp_unbounded_catalog_tasks_keep_archive_dates(tmp_path: Path) -> None:
    class _GovernmentInput:
        model_fields = {
            "symbol": _Field(str),
            "chamber": _Field(str),
            "limit": _Field(int),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {".equity.ownership.government_trades": ["fmp"]}
    context.schemas = {
        ".equity.ownership.government_trades": {
            "input": _GovernmentInput,
            "callable": lambda **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 1
    assert tasks[0].scope_key == "all/page=0"
    assert tasks[0].kwargs == {
        "start_date": "2000-01-01",
        "end_date": "2000-12-31",
        "limit": 0,
    }


def test_fmp_price_targets_paginates_until_short_page_and_filters_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    calls: list[tuple[str, int]] = []

    def fake_page(endpoint, params, credential):
        assert credential == "secret"
        calls.append((endpoint, params["page"]))
        if params["page"] == 0:
            return [
                {
                    "publishedDate": "2000-06-01",
                    "symbol": "AAPL",
                    "newsURL": f"https://example.test/{index}",
                }
                for index in range(100)
            ]
        return [
            {
                "publishedDate": "1999-12-31",
                "symbol": "AAPL",
                "newsURL": "https://example.test/old",
            },
            {
                "publishedDate": "2000-07-01",
                "symbol": "AAPL",
                "newsURL": "https://example.test/new",
            },
        ]

    class _CountingLimiter:
        waits = 0

        def wait(self):
            self.waits += 1

    monkeypatch.setattr(archive, "_fmp_page_json", fake_page)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key="secret"))
    )
    limiter = _CountingLimiter()

    rows = _fetch_fmp_price_targets_workaround(
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
        },
        obb,
        page_limiter=limiter,
    )

    assert len(rows) == 101
    assert calls == [("price-target-news", 0), ("price-target-news", 1)]
    assert limiter.waits == 2


def test_fmp_government_trades_paginates_each_chamber_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    calls: list[tuple[str, int]] = []

    def fake_page(endpoint, params, credential):
        assert credential == "secret"
        page = params["page"]
        calls.append((endpoint, page))
        if endpoint == "house-latest" and page == 0:
            return [
                {
                    "disclosureDate": "2000-06-01",
                    "ticker": "AAPL",
                    "link": f"https://example.test/house/{index}",
                }
                for index in range(250)
            ]
        if endpoint == "senate-latest" and page == 0:
            return [
                {
                    "disclosureDate": "1999-12-31",
                    "ticker": "MSFT",
                    "link": "https://example.test/senate/old",
                }
            ]
        if endpoint == "house-latest" and page == 1:
            return [
                {
                    "disclosureDate": "2000-07-01",
                    "ticker": "AAPL",
                    "link": "https://example.test/house/final",
                }
            ]
        raise AssertionError((endpoint, page))

    class _CountingLimiter:
        waits = 0

        def wait(self):
            self.waits += 1

    monkeypatch.setattr(archive, "_fmp_page_json", fake_page)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key="secret"))
    )
    limiter = _CountingLimiter()

    rows = _fetch_fmp_government_trades_workaround(
        {"start_date": "2000-01-01", "end_date": "2000-12-31", "limit": 0},
        obb,
        page_limiter=limiter,
    )

    assert len(rows) == 251
    assert calls == [
        ("house-latest", 0),
        ("senate-latest", 0),
        ("house-latest", 1),
    ]
    assert limiter.waits == 3


def test_missing_mandatory_provider_credentials_are_disabled() -> None:
    assert providers_missing_required_credentials({"benzinga_api_key"}) == {"intrinio"}
    assert (
        providers_missing_required_credentials({"benzinga_api_key", "intrinio_api_key"})
        == set()
    )


def test_historical_company_news_excludes_yfinance_and_enforces_dates(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    assert select_providers("news.company", ["tiingo", "yfinance", "fmp"], context) == (
        "fmp",
        "tiingo",
    )
    task = make_task(
        context,
        "news.company",
        "AAPL/year=2000/page=0",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "limit": 1000,
            "page": 0,
        },
        ("fmp",),
    )
    assert _filter_company_news_to_task_range(
        [
            {"date": "2000-05-01T12:00:00Z", "title": "keep"},
            {"date": "2026-05-01T12:00:00Z", "title": "drop"},
            {"title": "unverifiable"},
        ],
        task,
    ) == [{"date": "2000-05-01T12:00:00Z", "title": "keep"}]


def test_world_news_plans_every_non_crypto_fmp_feed_once_per_period(
    tmp_path: Path,
) -> None:
    class _WorldNewsInput:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "topic": _Field(str),
            "limit": _Field(int),
            "page": _Field(int),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2001-12-31"
    context.commands = {".news.world": ["tiingo", "fmp"]}
    context.schemas = {
        ".news.world": {
            "input": _WorldNewsInput,
            "callable": lambda **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 9
    articles = [task for task in tasks if task.kwargs["topic"] == "fmp_articles"]
    assert len(articles) == 1
    assert articles[0].providers == ("fmp",)
    assert articles[0].scope_key == "topic=fmp_articles/page=0"
    annual = [task for task in tasks if task.kwargs["topic"] != "fmp_articles"]
    assert {task.kwargs["topic"] for task in annual} == {
        "general",
        "press_releases",
        "stocks",
        "forex",
    }
    assert "crypto" not in {task.kwargs["topic"] for task in tasks}
    assert all(
        task.providers == ("fmp", "tiingo")
        for task in annual
        if task.kwargs["topic"] == "general"
    )
    assert all(
        task.providers == ("fmp",)
        for task in annual
        if task.kwargs["topic"] != "general"
    )


def test_fmp_world_articles_pages_to_end_and_filters_archive_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import downloader.download_openbb_archive as archive

    calls: list[int] = []

    def fake_page(endpoint, params, credential):
        assert endpoint == "news/fmp-articles"
        assert credential == "secret"
        page = params["page"]
        calls.append(page)
        if page == 0:
            return [
                {
                    "publishedDate": "2000-06-01T12:00:00Z",
                    "title": f"article-{index}",
                    "site": "FMP",
                }
                for index in range(100)
            ]
        return [
            {
                "publishedDate": "1999-12-31T12:00:00Z",
                "title": "old",
                "site": "FMP",
            },
            {
                "publishedDate": "2000-07-01T12:00:00Z",
                "title": "new",
                "site": "FMP",
            },
        ]

    class _CountingLimiter:
        waits = 0

        def wait(self):
            self.waits += 1

    monkeypatch.setattr(archive, "_fmp_page_json", fake_page)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key="secret"))
    )
    limiter = _CountingLimiter()

    rows = _fetch_fmp_world_articles_workaround(
        {"start_date": "2000-01-01", "end_date": "2000-12-31"},
        obb,
        page_limiter=limiter,
    )

    assert len(rows) == 101
    assert calls == [0, 1]
    assert limiter.waits == 2


def test_global_economy_calendar_uses_monthly_fred_fmp_tasks(
    tmp_path: Path,
) -> None:
    class _CalendarInput:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "country": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-15"
    context.end_date = "2000-03-05"
    context.commands = {".economy.calendar": ["tradingeconomics", "fmp", "fred"]}
    context.schemas = {
        ".economy.calendar": {
            "input": _CalendarInput,
            "callable": lambda **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 3
    assert [task.scope_key for task in tasks] == [
        "month=2000-01/page=0",
        "month=2000-02/page=0",
        "month=2000-03/page=0",
    ]
    assert [(task.kwargs["start_date"], task.kwargs["end_date"]) for task in tasks] == [
        ("2000-01-15", "2000-01-31"),
        ("2000-02-01", "2000-02-29"),
        ("2000-03-01", "2000-03-05"),
    ]
    assert all(task.providers == ("fred", "fmp") for task in tasks)


def test_hqm_date_grid_enumerates_spot_and_par_curves(tmp_path: Path) -> None:
    class _HqmInput:
        model_fields = {
            "date": _Field(str),
            "yield_curve": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2000-02-20"
    context.commands = {".fixedincome.corporate.hqm": ["fred"]}
    context.schemas = {
        ".fixedincome.corporate.hqm": {
            "input": _HqmInput,
            "callable": lambda **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 4
    assert [task.scope_key for task in tasks] == [
        "date=2000-01-01/curve=spot",
        "date=2000-01-01/curve=par",
        "date=2000-02-01/curve=spot",
        "date=2000-02-01/curve=par",
    ]
    assert [task.kwargs["yield_curve"] for task in tasks] == [
        "spot",
        "par",
        "spot",
        "par",
    ]


def test_central_bank_holdings_enumerates_all_distinct_history_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _HoldingsInput:
        model_fields = {
            "date": _Field(str),
            "holding_type": _Field(str),
            "summary": _Field(bool),
            "monthly": _Field(bool),
            "wam": _Field(bool),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2003-07-16"
    context.commands = {".economy.central_bank_holdings": ["federal_reserve"]}
    context.schemas = {
        ".economy.central_bank_holdings": {
            "input": _HoldingsInput,
            "callable": lambda **kwargs: None,
        }
    }

    monkeypatch.setattr(
        "downloader.download_openbb_archive._soma_as_of_dates",
        lambda: ("2003-07-09", "2003-07-16", "2003-07-23"),
    )

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 10
    assert tasks[0].scope_key == "mode=summary"
    assert tasks[0].kwargs == {"summary": True}
    assert tasks[1].scope_key == "mode=monthly"
    assert tasks[1].kwargs == {
        "monthly": True,
        "holding_type": "all_treasury",
    }
    dated = tasks[2:]
    assert {task.kwargs["date"] for task in dated} == {
        "2003-07-09",
        "2003-07-16",
    }
    assert {
        (task.kwargs["holding_type"], bool(task.kwargs.get("wam"))) for task in dated
    } == {
        ("all_treasury", False),
        ("all_treasury", True),
        ("all_agency", False),
        ("all_agency", True),
    }


def test_etf_search_partitions_every_exchange_instead_of_capped_catalog(
    tmp_path: Path,
) -> None:
    exchanges = Literal["amex", "nyse", "nasdaq", "tsx", "euronext"]

    class _EtfSearchInput:
        model_fields = {
            "query": _Field(str),
            "exchange": _Field(exchanges),
            # The merged provider schema now exposes this optional filter, but
            # FMP does not accept the synthetic literal ``all``.
            "country": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {".etf.search": ["fmp"]}
    context.schemas = {
        ".etf.search": {
            "input": _EtfSearchInput,
            "callable": lambda **kwargs: None,
        }
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 5
    assert [task.scope_key for task in tasks] == [
        "exchange=amex",
        "exchange=nyse",
        "exchange=nasdaq",
        "exchange=tsx",
        "exchange=euronext",
    ]
    assert [task.kwargs for task in tasks] == [
        {"exchange": exchange}
        for exchange in ("amex", "nyse", "nasdaq", "tsx", "euronext")
    ]


def test_eia_petroleum_schema_map_mismatch_uses_direct_fetcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_us_eia.models.petroleum_status_report import (
        EiaPetroleumStatusReportFetcher,
    )

    assert _eia_petroleum_schema_mismatch_tables() == frozenset(
        {"inputs_utilization_avg"}
    )
    query = SimpleNamespace(table="inputs_utilization_avg")

    async def extract(actual_query, credentials):
        assert actual_query is query
        assert credentials is None
        return {"file": "workbook"}

    monkeypatch.setattr(
        EiaPetroleumStatusReportFetcher,
        "transform_query",
        staticmethod(lambda kwargs: query),
    )
    monkeypatch.setattr(
        EiaPetroleumStatusReportFetcher,
        "aextract_data",
        staticmethod(extract),
    )
    monkeypatch.setattr(
        EiaPetroleumStatusReportFetcher,
        "transform_data",
        staticmethod(lambda actual_query, raw: [{"date": "2000-01-07", **raw}]),
    )

    assert _fetch_eia_petroleum_status_workaround(
        {"category": "weekly_estimates", "table": "inputs_utilization_avg"}
    ) == [{"date": "2000-01-07", "file": "workbook"}]


def test_soma_workaround_classifies_empty_mapping_as_authoritative_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_federal_reserve.models.central_bank_holdings import (
        FederalReserveCentralBankHoldingsFetcher,
    )

    query = SimpleNamespace(date="2003-12-31")

    async def extract(actual_query, credentials):
        assert actual_query is query
        assert credentials is None
        return [{}]

    monkeypatch.setattr(
        FederalReserveCentralBankHoldingsFetcher,
        "transform_query",
        staticmethod(lambda kwargs: query),
    )
    monkeypatch.setattr(
        FederalReserveCentralBankHoldingsFetcher,
        "aextract_data",
        staticmethod(extract),
    )

    with pytest.raises(EmptyDataError, match="authoritative empty"):
        _fetch_federal_reserve_central_bank_holdings_workaround(
            {"date": "2003-12-31", "holding_type": "all_agency", "wam": True}
        )


def test_soma_workaround_validates_meaningful_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openbb_federal_reserve.models.central_bank_holdings import (
        FederalReserveCentralBankHoldingsFetcher,
    )

    query = SimpleNamespace(date="2010-01-06")
    raw = [{"asOfDate": "2010-01-06", "wam": 3.36}]

    async def extract(actual_query, credentials):
        return raw

    monkeypatch.setattr(
        FederalReserveCentralBankHoldingsFetcher,
        "transform_query",
        staticmethod(lambda kwargs: query),
    )
    monkeypatch.setattr(
        FederalReserveCentralBankHoldingsFetcher,
        "aextract_data",
        staticmethod(extract),
    )
    monkeypatch.setattr(
        FederalReserveCentralBankHoldingsFetcher,
        "transform_data",
        staticmethod(lambda actual_query, rows: [{"validated": rows[0]["wam"]}]),
    )

    assert _fetch_federal_reserve_central_bank_holdings_workaround(
        {"date": "2010-01-06", "holding_type": "all_agency", "wam": True}
    ) == [{"validated": 3.36}]


def test_fixed_income_semantic_dimensions_are_fully_enumerated(
    tmp_path: Path,
) -> None:
    class _Input:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "date": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2000-01-10"
    endpoints = (
        "fixedincome.bond_indices",
        "fixedincome.mortgage_indices",
        "fixedincome.government.yield_curve",
    )
    context.commands = {f".{endpoint}": ["fred"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {"input": _Input, "callable": lambda **kwargs: None}
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)
    by_endpoint = {
        endpoint: [task for task in tasks if task.endpoint == endpoint]
        for endpoint in endpoints
    }

    assert len(by_endpoint["fixedincome.bond_indices"]) == len(
        _fred_bond_index_combinations()
    )
    assert {
        task.kwargs["index"] for task in by_endpoint["fixedincome.mortgage_indices"]
    } == set(MORTGAGE_INDEX_GROUPS)
    curve_tasks = by_endpoint["fixedincome.government.yield_curve"]
    assert {task.kwargs["yield_curve_type"] for task in curve_tasks} == set(
        GOVERNMENT_YIELD_CURVE_TYPES
    )
    assert all(
        sum(task.kwargs["yield_curve_type"] == curve_type for task in curve_tasks) == 1
        for curve_type in GOVERNMENT_YIELD_CURVE_TYPES
    )
    assert len(curve_tasks) == 6
    assert {task.providers for task in curve_tasks} == {("fred",)}
    assert all(task.scope_key.endswith("/archive") for task in curve_tasks)
    assert next(
        task for task in curve_tasks if task.kwargs["yield_curve_type"] == "nominal"
    ).kwargs["date"].split(",") == [
        "2000-01-03",
        "2000-01-04",
        "2000-01-05",
        "2000-01-06",
        "2000-01-07",
        "2000-01-10",
    ]
    assert {item.initial_task_count for item in coverage} == {
        len(by_endpoint[item.endpoint]) for item in coverage
    }


def test_yield_curve_provider_capabilities_are_partitioned_without_silent_fallback(
    tmp_path: Path,
) -> None:
    class _Input:
        model_fields = {
            "date": _Field(str),
            "country": _Field(str),
            "yield_curve_type": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2000-01-10"
    endpoint = "fixedincome.government.yield_curve"
    context.commands = {f".{endpoint}": ["econdb", "federal_reserve", "fmp", "fred"]}
    context.schemas = {
        f".{endpoint}": {"input": _Input, "callable": lambda **kwargs: None}
    }

    tasks, _ = build_initial_plan(context)
    us_nominal = [
        task
        for task in tasks
        if task.kwargs.get("country") == "united_states"
        and task.kwargs.get("yield_curve_type") == "nominal"
    ]
    fred_specialized = [
        task
        for task in tasks
        if task.kwargs.get("country") == "united_states"
        and task.kwargs.get("yield_curve_type") != "nominal"
    ]
    econdb_country_curves = [
        task
        for task in tasks
        if task.kwargs.get("country") not in {None, "united_states"}
    ]

    assert len(us_nominal) == 1
    assert set(us_nominal[0].providers) == {
        "econdb",
        "federal_reserve",
        "fred",
    }
    assert us_nominal[0].scope_key.endswith("/archive")
    assert "fmp" not in us_nominal[0].providers
    assert len(fred_specialized) == 5
    assert {task.providers for task in fred_specialized} == {("fred",)}
    assert all(task.scope_key.endswith("/archive") for task in fred_specialized)
    assert len(econdb_country_curves) == 19
    assert {task.providers for task in econdb_country_curves} == {("econdb",)}
    assert len({task.kwargs["country"] for task in econdb_country_curves}) == 19
    assert all(task.scope_key.endswith("/archive") for task in econdb_country_curves)
    daily_country = next(
        task for task in econdb_country_curves if task.kwargs["country"] == "australia"
    )
    monthly_country = next(
        task for task in econdb_country_curves if task.kwargs["country"] == "singapore"
    )
    assert daily_country.kwargs["date"].split(",") == [
        "2000-01-03",
        "2000-01-04",
        "2000-01-05",
        "2000-01-06",
        "2000-01-07",
        "2000-01-10",
    ]
    assert monthly_country.kwargs["date"] == "2000-01-01"
    assert len(tasks) == 25


def test_yield_curve_date_window_provider_keeps_resumable_daily_shards(
    tmp_path: Path,
) -> None:
    class _Input:
        model_fields = {
            "date": _Field(str),
            "country": _Field(str),
            "yield_curve_type": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2000-01-10"
    endpoint = "fixedincome.government.yield_curve"
    context.commands = {f".{endpoint}": ["fmp"]}
    context.schemas = {
        f".{endpoint}": {"input": _Input, "callable": lambda **kwargs: None}
    }

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 6
    assert [task.kwargs["date"] for task in tasks] == [
        "2000-01-03",
        "2000-01-04",
        "2000-01-05",
        "2000-01-06",
        "2000-01-07",
        "2000-01-10",
    ]
    assert {task.providers for task in tasks} == {("fmp",)}
    assert all(not task.scope_key.endswith("/archive") for task in tasks)


def test_fred_bond_combinations_deduplicate_provider_aliases() -> None:
    combinations = _fred_bond_index_combinations()
    assert len(combinations) == 125
    assert ("high_yield", "us", "oas") in combinations
    assert ("us", "high_yield", "oas") not in combinations
    assert ("high_yield", "emerging", "yield") in combinations
    assert ("emerging_markets", "high_yield", "yield") not in combinations


def test_macro_semantic_dimensions_use_distinct_raw_series(tmp_path: Path) -> None:
    class _MacroInput:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "country": _Field(str),
            "item": _Field(
                str,
                "Choices forfred: 'milk', 'ground_beef'",
            ),
            "provider": _Field(str),
        }

    endpoints = (
        "economy.unemployment",
        "economy.gdp.forecast",
        "economy.gdp.nominal",
        "economy.gdp.real",
        "economy.interest_rates",
        "economy.total_factor_productivity",
        "economy.composite_leading_indicator",
        "economy.cpi",
        "economy.retail_prices",
    )
    context = _context(tmp_path)
    context.commands = {f".{endpoint}": ["oecd"] for endpoint in endpoints}
    context.commands[".economy.retail_prices"] = ["fred"]
    context.commands[".economy.total_factor_productivity"] = ["federal_reserve"]
    context.schemas = {
        f".{endpoint}": {"input": _MacroInput, "callable": lambda **kwargs: None}
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)
    by_endpoint = {
        endpoint: [task for task in tasks if task.endpoint == endpoint]
        for endpoint in endpoints
    }
    assert len(by_endpoint["economy.unemployment"]) == len(UNEMPLOYMENT_DIMENSIONS)
    assert len(by_endpoint["economy.gdp.forecast"]) == len(GDP_FORECAST_DIMENSIONS)
    assert not any(
        task.kwargs["frequency"] == "quarter" and task.kwargs["units"] == "capita"
        for task in by_endpoint["economy.gdp.forecast"]
    )
    assert len(by_endpoint["economy.gdp.nominal"]) == len(GDP_NOMINAL_DIMENSIONS)
    assert len(by_endpoint["economy.gdp.real"]) == len(GDP_REAL_FREQUENCIES)
    assert len(by_endpoint["economy.interest_rates"]) == len(INTEREST_RATE_DURATIONS)
    assert {
        task.kwargs["frequency"]
        for task in by_endpoint["economy.total_factor_productivity"]
    } == set(TOTAL_FACTOR_PRODUCTIVITY_FREQUENCIES)
    assert {
        task.kwargs["adjustment"]
        for task in by_endpoint["economy.composite_leading_indicator"]
    } == set(COMPOSITE_LEADING_INDICATOR_ADJUSTMENTS)
    assert {task.kwargs["harmonized"] for task in by_endpoint["economy.cpi"]} == set(
        CPI_HARMONIZED_MODES
    )
    assert all(
        (task.kwargs["frequency"], task.kwargs["transform"]) == ("monthly", "index")
        for task in by_endpoint["economy.cpi"]
    )
    assert len(by_endpoint["economy.retail_prices"]) == 2 * len(RETAIL_PRICE_REGIONS)
    assert all(
        task.kwargs.get("frequency") == "monthly"
        for endpoint in ("economy.unemployment", "economy.interest_rates")
        for task in by_endpoint[endpoint]
    )
    assert {item.initial_task_count for item in coverage} == {
        len(by_endpoint[item.endpoint]) for item in coverage
    }


def test_equity_period_dimensions_request_complete_history(tmp_path: Path) -> None:
    class _SymbolInput:
        model_fields = {
            "symbol": _Field(str),
            "provider": _Field(str),
        }

    def _symbol_callable(symbol, **kwargs):
        return None

    endpoints = tuple(sorted(FORWARD_ESTIMATE_ENDPOINTS | SYMBOL_PERIOD_ENDPOINTS))
    context = _context(tmp_path)
    context.commands = {f".{endpoint}": ["fmp"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {"input": _SymbolInput, "callable": _symbol_callable}
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)
    by_endpoint = {
        endpoint: [task for task in tasks if task.endpoint == endpoint]
        for endpoint in endpoints
    }
    assert all(len(endpoint_tasks) == 2 for endpoint_tasks in by_endpoint.values())
    for endpoint in FORWARD_ESTIMATE_ENDPOINTS:
        assert {task.kwargs["fiscal_period"] for task in by_endpoint[endpoint]} == {
            "annual",
            "quarter",
        }
        assert all(task.kwargs["include_historical"] for task in by_endpoint[endpoint])
        assert all(task.kwargs["limit"] == 1000 for task in by_endpoint[endpoint])
    for endpoint in SYMBOL_PERIOD_ENDPOINTS:
        assert {task.kwargs["period"] for task in by_endpoint[endpoint]} == {
            "annual",
            "quarter",
        }
    for task in by_endpoint["equity.estimates.historical"]:
        assert (task.kwargs["limit"], task.kwargs["page"]) == (1000, 0)
    assert all(item.initial_task_count == 2 for item in coverage)


def test_raw_rate_spread_and_ny_survey_dimensions(tmp_path: Path) -> None:
    from typing import Literal

    class _Input:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "topic": _Field(Literal["business_outlook", "new_orders"]),
            "provider": _Field(str),
        }

    endpoints = (
        "fixedincome.rate.dpcredit",
        *SPREAD_MATURITIES,
        "economy.survey.manufacturing_outlook_ny",
    )
    context = _context(tmp_path)
    context.commands = {f".{endpoint}": ["fred"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {"input": _Input, "callable": lambda **kwargs: None}
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)
    by_endpoint = {
        endpoint: [task for task in tasks if task.endpoint == endpoint]
        for endpoint in endpoints
    }
    assert {
        task.kwargs["parameter"] for task in by_endpoint["fixedincome.rate.dpcredit"]
    } == set(DWPCR_RAW_PARAMETERS)
    for endpoint, maturities in SPREAD_MATURITIES.items():
        assert {task.kwargs["maturity"] for task in by_endpoint[endpoint]} == set(
            maturities
        )
    ny_tasks = by_endpoint["economy.survey.manufacturing_outlook_ny"]
    assert len(ny_tasks) == 4
    assert {
        (task.kwargs["topic"], task.kwargs["seasonally_adjusted"]) for task in ny_tasks
    } == {
        (topic, adjusted)
        for topic in ("business_outlook", "new_orders")
        for adjusted in (False, True)
    }
    assert {item.initial_task_count for item in coverage} == {
        len(by_endpoint[item.endpoint]) for item in coverage
    }


def test_provider_owned_semantic_fields_cannot_fall_back_to_ignoring_provider(
    tmp_path: Path,
) -> None:
    class _NominalGdpInput:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "country": _Field(str, "Country. (provider: econdb, oecd)"),
            "frequency": _Field(str, "Frequency. (provider: oecd)"),
            "units": _Field(str, "Units. (provider: oecd)"),
            "price_base": _Field(str, "Price base. (provider: oecd)"),
            "provider": _Field(str),
        }

    class _MetricsInput:
        model_fields = {
            "symbol": _Field(str),
            "period": _Field(str, "Fiscal period. (provider: fmp)"),
            "ttm": _Field(str, "Trailing twelve months. (provider: fmp)"),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {
        ".economy.gdp.nominal": ["econdb", "oecd"],
        ".equity.fundamental.metrics": ["yfinance", "fmp"],
    }
    context.schemas = {
        ".economy.gdp.nominal": {
            "input": _NominalGdpInput,
            "callable": lambda **kwargs: None,
        },
        ".equity.fundamental.metrics": {
            "input": _MetricsInput,
            "callable": lambda symbol, **kwargs: None,
        },
    }

    tasks, _ = build_initial_plan(context)
    gdp_tasks = [task for task in tasks if task.endpoint == "economy.gdp.nominal"]
    metric_tasks = [
        task for task in tasks if task.endpoint == "equity.fundamental.metrics"
    ]
    assert len(gdp_tasks) == len(GDP_NOMINAL_DIMENSIONS)
    assert {task.providers for task in gdp_tasks} == {("oecd",)}
    assert metric_tasks
    assert {task.providers for task in metric_tasks} == {("fmp",)}
    assert {task.kwargs["ttm"] for task in metric_tasks} == {"exclude", "only"}


def test_statement_tasks_include_preliminary_sec_data(tmp_path: Path) -> None:
    class _Input:
        model_fields = {"symbol": _Field(str), "provider": _Field(str)}

    def _call(symbol, **kwargs):
        return None

    context = _context(tmp_path)
    context.commands = {".equity.fundamental.income": ["sec", "fmp"]}
    context.schemas = {
        ".equity.fundamental.income": {"input": _Input, "callable": _call}
    }
    tasks, _ = build_initial_plan(context)
    assert {task.kwargs["period"] for task in tasks} == {"annual", "quarter"}
    assert all(task.kwargs["include_preliminary"] for task in tasks)


def test_fmp_metrics_and_ratios_do_not_collapse_to_ttm_only(tmp_path: Path) -> None:
    class _Input:
        model_fields = {
            "symbol": _Field(str),
            "period": _Field(str),
            "ttm": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    endpoints = (
        "equity.fundamental.metrics",
        "equity.fundamental.ratios",
    )
    context.commands = {f".{endpoint}": ["fmp"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {"input": _Input, "callable": lambda symbol, **kwargs: None}
        for endpoint in endpoints
    }

    tasks, _ = build_initial_plan(context)

    for endpoint in endpoints:
        endpoint_tasks = [task for task in tasks if task.endpoint == endpoint]
        assert {
            (task.kwargs.get("period", "ttm"), task.kwargs["ttm"])
            for task in endpoint_tasks
        } == {
            ("annual", "exclude"),
            ("quarter", "exclude"),
            ("ttm", "only"),
        }


@pytest.mark.parametrize(
    ("endpoint", "route"),
    (
        ("equity.fundamental.metrics", "key-metrics"),
        ("equity.fundamental.ratios", "ratios"),
    ),
)
@pytest.mark.parametrize(
    ("ttm_mode", "expected_suffixes"),
    (
        ("exclude", ("historical",)),
        ("only", ("ttm",)),
        ("include", ("historical", "ttm")),
    ),
)
def test_fmp_fundamental_workaround_avoids_discarded_http_requests(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    route: str,
    ttm_mode: str,
    expected_suffixes: tuple[str, ...],
) -> None:
    from openbb_fmp.utils import helpers as fmp_helpers

    urls: list[str] = []

    async def _fake_get_data_many(url: str, **_kwargs):
        urls.append(url)
        if f"/{route}-ttm?" in url:
            return [{"symbol": "AAPL", "marketCapTTM": 123.0}]
        return [
            {
                "symbol": "AAPL",
                "date": "2025-12-31",
                "calendarYear": "2025",
                "period": "FY",
                "reportedCurrency": "USD",
            }
        ]

    monkeypatch.setattr(fmp_helpers, "get_data_many", _fake_get_data_many)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key="test-key"))
    )

    records = _fetch_fmp_fundamental_ratio_workaround(
        endpoint,
        {
            "symbol": "AAPL",
            "period": "annual",
            "limit": 5,
            "ttm": ttm_mode,
        },
        obb,
    )

    assert records
    actual_suffixes = tuple(
        "ttm" if f"/{route}-ttm?" in url else "historical" for url in urls
    )
    assert actual_suffixes == expected_suffixes
    assert all("apikey=test-key" in url for url in urls)
    if "historical" in expected_suffixes:
        historical_url = next(url for url in urls if f"/{route}-ttm?" not in url)
        assert "period=annual" in historical_url
        assert "limit=5" in historical_url


def test_date_grid_tables_enumerate_every_native_category(tmp_path: Path) -> None:
    class _PceInput:
        model_fields = {
            "date": _Field(str),
            "category": _Field(Literal["income", "prices", "wages"]),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.start_date = "2000-01-01"
    context.end_date = "2000-02-01"
    endpoints = ("economy.pce", "economy.survey.nonfarm_payrolls")
    context.commands = {f".{endpoint}": ["fred"] for endpoint in endpoints}
    context.schemas = {
        f".{endpoint}": {
            "input": _PceInput,
            "callable": lambda **kwargs: None,
        }
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)

    for endpoint in endpoints:
        endpoint_tasks = [task for task in tasks if task.endpoint == endpoint]
        assert len(endpoint_tasks) == 6
        assert {task.kwargs["category"] for task in endpoint_tasks} == {
            "income",
            "prices",
            "wages",
        }
        assert {task.kwargs["date"] for task in endpoint_tasks} == {
            "2000-01-01",
            "2000-02-01",
        }
    assert {item.initial_task_count for item in coverage} == {6}


def test_house_prices_and_primary_dealers_cover_distinct_series_groups(
    tmp_path: Path,
) -> None:
    class _MacroInput:
        model_fields = {
            "start_date": _Field(str),
            "end_date": _Field(str),
            "country": _Field(str),
            "frequency": _Field(str),
            "transform": _Field(str),
            "category": _Field(str),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    endpoints = (
        "economy.house_price_index",
        "economy.primary_dealer_positioning",
    )
    context.commands = {
        ".economy.house_price_index": ["oecd"],
        ".economy.primary_dealer_positioning": ["federal_reserve"],
    }
    context.schemas = {
        f".{endpoint}": {
            "input": _MacroInput,
            "callable": lambda **kwargs: None,
        }
        for endpoint in endpoints
    }

    tasks, coverage = build_initial_plan(context)
    house = [task for task in tasks if task.endpoint == "economy.house_price_index"]
    dealers = [
        task for task in tasks if task.endpoint == "economy.primary_dealer_positioning"
    ]
    assert len(house) == 9
    assert (
        len({(task.kwargs["frequency"], task.kwargs["transform"]) for task in house})
        == 9
    )
    assert {task.kwargs["category"] for task in dealers} == {
        "treasuries",
        "mbs",
        "municipal",
        "corporate",
        "abs",
    }
    assert {item.initial_task_count for item in coverage} == {9, 5}


def test_cftc_catalog_enumerates_report_modes_and_preserves_followup_mode(
    tmp_path: Path,
) -> None:
    class _CftcSearchInput:
        model_fields = {
            "query": _Field(str),
            "report_type": _Field(str),
            "futures_only": _Field(bool),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {
        ".cftc.cot_search": ["cftc"],
        ".cftc.cot": ["cftc"],
    }
    context.schemas = {
        ".cftc.cot_search": {
            "input": _CftcSearchInput,
            "callable": lambda **kwargs: None,
        }
    }
    tasks, coverage = build_initial_plan(context)
    search_coverage = next(
        item for item in coverage if item.endpoint == "cftc.cot_search"
    )
    assert search_coverage.initial_task_count == len(CFTC_REPORT_MODES)
    assert {
        (task.kwargs["report_type"], task.kwargs["futures_only"]) for task in tasks
    } == set(CFTC_REPORT_MODES)

    catalog = next(
        task
        for task in tasks
        if task.kwargs
        == {
            "query": "",
            "report_type": "financial",
            "futures_only": True,
            "start_date": context.start_date,
            "end_date": context.end_date,
        }
    )
    result = TaskResult(
        catalog,
        "success",
        "cftc",
        1,
        catalog.output_path,
        1,
        records=[{"code": "123456", "name": "Historical Contract"}],
    )
    followups = discover_followup_tasks(context, result)
    assert len(followups) == 1
    assert followups[0].scope_key == "report=financial/mode=futures/code=123456"
    assert followups[0].kwargs == {
        "code": "123456",
        "start_date": context.start_date,
        "end_date": context.end_date,
        "report_type": "financial",
        "futures_only": True,
        "measure": "all",
        "limit": 0,
    }


def test_cftc_catalog_workaround_uses_full_archive_range(monkeypatch) -> None:
    from io import BytesIO
    from urllib.parse import parse_qs, urlsplit

    requested = []

    class _Response(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    def _urlopen(request, timeout):
        requested.append(request.full_url)
        assert timeout == 60
        return _Response(
            json.dumps(
                [
                    {
                        "cftc_contract_market_code": "001234",
                        "contract_market_name": "Inactive Contract",
                        "commodity_name": "Commodity",
                        "commodity_group_name": "AGRICULTURE",
                        "commodity_subgroup_name": "GRAINS",
                        "contract_units": "CONTRACTS",
                    }
                ]
            ).encode()
        )

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    records = _fetch_cftc_cot_catalog_workaround(
        {
            "report_type": "financial",
            "futures_only": True,
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        }
    )
    assert records == [
        {
            "code": "001234",
            "name": "Inactive Contract",
            "commodity": "Commodity",
            "category": "AGRICULTURE",
            "subcategory": "GRAINS",
            "units": "CONTRACTS",
        }
    ]
    split = urlsplit(requested[0])
    assert split.path.endswith("/gpe5-46if.json")
    where = parse_qs(split.query)["$where"][0]
    assert "2000-01-01" in where
    assert "2026-07-18" in where


def test_fred_hqm_workaround_filters_unpublished_nodes(monkeypatch) -> None:
    from io import BytesIO

    payload = {
        "elements": {
            "1": {
                "name": "0.5 - Year",
                "observation_date": "Jun 2026",
                "observation_value": "4.25",
            },
            "2": {
                "name": "1 - Year",
                "observation_date": ".",
                "observation_value": ".",
            },
        }
    }

    class _Response(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    def _urlopen(request, timeout):
        assert timeout == 60
        assert "element_id=219299" in request.full_url
        return _Response(json.dumps(payload).encode())

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key=_Secret()))
    )
    records = _fetch_fred_hqm_workaround(
        {"date": "2026-06-15", "yield_curve": "spot"}, obb
    )
    assert records == [{"date": "2026-06-01", "rate": 0.0425, "maturity": "year_0.5"}]


def test_worker_skips_out_of_range_company_news_and_uses_fallback(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "news.company",
        "AAPL/year=2000/page=0",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "limit": 1000,
            "page": 0,
        },
        ("bad_dates", "archive"),
    )

    class _Result:
        def __init__(self, result):
            self.results = result

    class _News:
        @staticmethod
        def company(*, provider: str, **kwargs):
            row = {
                "date": (
                    "2026-05-01T12:00:00Z"
                    if provider == "bad_dates"
                    else "2000-05-01T12:00:00Z"
                ),
                "title": "Article",
                "url": "https://example.com/article",
            }
            return _Result([row])

    class _Obb:
        news = _News()

    worker = OpenBBWorker(
        _Obb(),
        _runtime(
            {"bad_dates": 1000, "archive": 1000},
            {"bad_dates": 1, "archive": 1},
        ),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )

    result = worker(task)

    assert result.status == "success"
    assert result.provider == "archive"
    assert result.rows == 1


def test_sec_company_facts_plan_uses_one_bulk_task_per_symbol(tmp_path: Path) -> None:
    class _CompanyFactsInput:
        model_fields = {
            "symbol": _Field(str),
            "fact": _Field(str),
            "use_cache": _Field(bool),
            "provider": _Field(str),
        }

    context = replace(
        _context(tmp_path),
        commands={".equity.compare.company_facts": ["sec"]},
        schemas={
            ".equity.compare.company_facts": {
                "input": _CompanyFactsInput,
                "callable": lambda symbol, fact="Revenues", **kwargs: None,
            }
        },
    )
    tasks, coverage = build_initial_plan(context)
    assert coverage[0].initial_task_count == 1
    assert len(tasks) == 1
    assert tasks[0].scope_key == f"AAPL/fact={SEC_ALL_COMPANY_FACTS}"
    assert tasks[0].kwargs == {
        "symbol": "AAPL",
        "fact": SEC_ALL_COMPANY_FACTS,
        "use_cache": True,
    }
    assert tasks[0].providers == ("sec",)


def test_sec_filings_plan_uses_one_full_task_for_us_and_years_for_non_us(
    tmp_path: Path,
) -> None:
    class _FilingsInput:
        model_fields = {
            "symbol": _Field(str),
            "start_date": _Field(str),
            "end_date": _Field(str),
            "limit": _Field(int),
            "page": _Field(int),
            "use_cache": _Field(bool),
            "provider": _Field(str),
        }

    context = replace(
        _context(tmp_path),
        start_date="2000-01-01",
        end_date="2001-12-31",
        assets=[
            AssetRecord("AAPL", "Apple", "us", "stock"),
            AssetRecord("2330.TW", "TSMC", "tw", "stock"),
        ],
        commands={".equity.fundamental.filings": ["sec", "fmp", "intrinio"]},
        schemas={
            ".equity.fundamental.filings": {
                "input": _FilingsInput,
                "callable": lambda symbol=None, **kwargs: None,
            }
        },
    )

    tasks, coverage = build_initial_plan(context)

    assert coverage[0].initial_task_count == 3
    assert [task.scope_key for task in tasks] == [
        "AAPL/all/page=0",
        "2330.TW/year=2000/page=0",
        "2330.TW/year=2001/page=0",
    ]
    assert tasks[0].kwargs == {
        "start_date": "2000-01-01",
        "end_date": "2001-12-31",
        "limit": 1000,
        "use_cache": True,
        "symbol": "AAPL",
        "page": 0,
    }
    assert tasks[0].providers == ("sec", "fmp", "intrinio")
    assert all(task.providers == ("fmp", "intrinio") for task in tasks[1:])


def test_tw_symbol_prefers_official_market_when_yahoo_lists_two_suffixes() -> None:
    assert _preferred_tw_symbol("1701.TW,1701.TWO", "1701.TW") == "1701.TW"
    assert _preferred_tw_symbol("1701.TW,1701.TWO", "1701.TWO") == "1701.TWO"
    assert _preferred_tw_symbol("2330.TW", "2330.TW") == "2330.TW"
    assert _preferred_tw_symbol("", "2330.TW") == "2330.TW"


def test_retail_item_typo_is_normalized_and_deduplicated() -> None:
    assert _normalize_retail_items(["beef", "groud_beef", "ground_beef"]) == [
        "beef",
        "ground_beef",
    ]


def test_provider_country_universes_preserve_valid_special_names() -> None:
    assert "w00" not in _econdb_country_codes()
    countries = _imf_direction_countries()
    assert "lao_people's_democratic_republic" in countries
    assert "côte_d'ivoire" in countries
    assert "s_democratic_republic" not in countries


def test_http_status_classification_does_not_match_numeric_ticker_substrings() -> None:
    from http.client import IncompleteRead

    class EmptyDataError(RuntimeError):
        pass

    assert (
        classify_error(RuntimeError("Could not find CIK for symbol: 4401.TWO"))
        == "empty"
    )
    assert (
        classify_error(
            EmptyDataError(
                "No data found for the item and region combination. "
                "You may also be experiencing rate limiting."
            )
        )
        == "empty"
    )
    assert (
        classify_error(
            EmptyDataError(
                "No data found: the daily threshold for total number of "
                "requests allocated to the user has been reached."
            )
        )
        == "rate"
    )
    assert (
        classify_error(
            EmptyDataError("No data found: The registration key provided is invalid.")
        )
        == "auth"
    )
    assert classify_error(RuntimeError("symbol 4401.TWO is unsupported")) == "permanent"
    assert classify_error(RuntimeError("HTTP 401 Unauthorized")) == "auth"
    assert (
        classify_error(RuntimeError("HTTP 401 Unauthorized: Invalid Crumb"))
        == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "No data found: The key provided by the User is invalid. "
                "Please provide a proper key"
            )
        )
        == "auth"
    )
    assert classify_error(RuntimeError("status 403 forbidden")) == "auth"

    class UnauthorizedError(RuntimeError):
        pass

    assert (
        classify_error(UnauthorizedError("Unauthorized FMP request -> 404 -> []"))
        == "empty"
    )
    assert classify_error(RuntimeError("FMP HTTP 402: Payment Required")) == "auth"
    assert classify_error(json.JSONDecodeError("Expecting value", "", 0)) == "transient"
    assert (
        classify_error(AttributeError("'NoneType' object has no attribute 'get'"))
        == "transient"
    )
    assert (
        classify_error(AttributeError("'list' object has no attribute 'values'"))
        == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "OpenBBError: parser: 'NoneType' object has no attribute 'replace'"
            )
        )
        == "transient"
    )
    assert (
        classify_error(TypeError("argument of type 'NoneType' is not iterable"))
        == "transient"
    )
    assert classify_error(RuntimeError("HTTP 429 too many requests")) == "rate"
    assert (
        classify_error(
            RuntimeError("__archive_provider_deferred__: SEC cooldown until tomorrow")
        )
        == "deferred"
    )
    assert classify_error(RuntimeError("FMP API error: Limit Reach")) == "rate"
    assert (
        classify_error(
            RuntimeError(
                "You have run over your hourly request allocation. "
                "Please upgrade your plan."
            )
        )
        == "rate"
    )
    assert classify_error(RuntimeError("HTTP 500 server response")) == "transient"
    assert (
        classify_error(
            RuntimeError(
                "Max retries exceeded with url: /download "
                "(Caused by NameResolutionError: Failed to resolve host)"
            )
        )
        == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "DNSError: Failed to perform, curl: (6) Could not resolve host: "
                "query2.finance.yahoo.com"
            )
        )
        == "transient"
    )
    assert classify_error(RuntimeError("calculation exceeded tolerance")) == "permanent"
    assert (
        classify_error(
            RuntimeError(
                "URLError: <urlopen error [Errno -3] Temporary failure in name resolution>"
            )
        )
        == "transient"
    )
    assert (
        classify_error(RuntimeError("HTTP Error 520: status code 520")) == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "[Error] -> 500, message='Attempt to decode JSON with unexpected "
                "mimetype: text/xml'"
            )
        )
        == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "ContentTypeError -> 502, message='Attempt to decode JSON with "
                "unexpected mimetype: text/html'"
            )
        )
        == "transient"
    )
    assert classify_error(RuntimeError("Failed to download file")) == "transient"
    assert classify_error(IncompleteRead(b"partial", 10)) == "transient"
    assert (
        classify_error(RuntimeError("attempt to write a readonly database"))
        == "transient"
    )
    assert classify_error(RuntimeError("database is locked")) == "transient"
    assert (
        classify_error(RuntimeError("database disk image is malformed")) == "transient"
    )
    assert (
        classify_error(
            RuntimeError(
                "ContentTypeError -> 404, message='Attempt to decode JSON with "
                "unexpected mimetype: application/xml', "
                "url='https://data.sec.gov/api/xbrl/companyfacts/CIK0001.json'"
            )
        )
        == "empty"
    )
    assert classify_error(RuntimeError("No quarterly filing dates found")) == "empty"
    assert classify_error(RuntimeError("No dividend data found for ABC")) == "empty"
    assert classify_error(RuntimeError("HOOZ is not an ETF.")) == "empty"
    assert (
        classify_error(RuntimeError("Unexpected response from SEC for CIK 0001667919"))
        == "empty"
    )


def test_econdb_country_profile_workaround_allows_missing_gdp(monkeypatch) -> None:
    from openbb_econdb.models.country_profile import EconDbCountryProfileFetcher

    async def _extract(*args, **kwargs):
        return [
            {
                "date": "2010-04-01",
                "Country": "Mongolia",
                "Population": math.nan,
                "GDP QoQ": 51.72,
                "CPI YoY": 8.5,
            }
        ]

    monkeypatch.setattr(EconDbCountryProfileFetcher, "aextract_data", _extract)
    records = _fetch_econdb_country_profile_workaround(
        {"country": "mn", "latest": False, "use_cache": True}
    )
    assert records == [
        {
            "country": "Mongolia",
            "population": None,
            "gdp_usd": None,
            "gdp_qoq": 0.5172,
            "gdp_yoy": None,
            "cpi_yoy": 0.085,
            "core_yoy": None,
            "retail_sales_yoy": None,
            "industrial_production_yoy": None,
            "policy_rate": None,
            "yield_10y": None,
            "govt_debt_gdp": None,
            "current_account_gdp": None,
            "jobless_rate": None,
            "date": "2010-04-01",
        }
    ]


def test_econdb_yield_curve_archive_flattens_native_rows_and_keeps_zero(
    monkeypatch,
) -> None:
    from openbb_econdb.models.yield_curve import EconDbYieldCurveFetcher
    from openbb_econdb.utils import helpers
    from openbb_econdb.utils.yield_curves import COUNTRIES_DICT

    ticker, maturity = next(iter(COUNTRIES_DICT["australia"].items()))

    def _query(kwargs):
        assert kwargs["date"] == "2026-07-17"
        assert kwargs["use_cache"] is False
        return SimpleNamespace(country="australia")

    async def _extract(url, timeout):
        assert timeout == 60
        assert "token=configured-test-token" in url
        return {
            "results": [
                {
                    "ticker": ticker,
                    "data": {
                        "dates": [
                            "1999-12-31",
                            "2000-01-03",
                            "2000-01-04",
                            "2026-07-17",
                        ],
                        "values": [9.0, 0.0, 5.0, None],
                    },
                },
            ]
        }

    monkeypatch.setattr(
        EconDbYieldCurveFetcher, "transform_query", staticmethod(_query)
    )
    monkeypatch.setattr(helpers, "amake_request", _extract)
    obb = SimpleNamespace(
        user=SimpleNamespace(
            credentials=SimpleNamespace(econdb_api_key="configured-test-token")
        )
    )

    records = _fetch_econdb_yield_curve_archive_workaround(
        {
            "country": "australia",
            "date": "2000-01-03,2000-01-04,2026-07-17",
        },
        obb,
    )

    assert [(row["date"], row["maturity"], row["rate"]) for row in records] == [
        ("2000-01-03", maturity, 0.0),
        ("2000-01-04", maturity, 0.05),
    ]
    assert all(row["country"] == "australia" for row in records)


def test_econdb_yield_curve_archive_rejects_missing_anonymous_token(
    monkeypatch,
) -> None:
    from openbb_econdb.utils import helpers

    async def _missing_token(*args, **kwargs):
        return ""

    monkeypatch.setattr(helpers, "create_token", _missing_token)
    monkeypatch.setattr("downloader.download_openbb_archive._ECONDB_CACHED_TOKEN", None)
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(econdb_api_key=None))
    )

    with pytest.raises(RuntimeError, match="EconDB API key is required") as exc:
        _fetch_econdb_yield_curve_archive_workaround(
            {
                "country": "australia",
                "date": "2000-01-03,2026-07-17",
            },
            obb,
        )

    assert classify_error(exc.value) == "auth"


def test_yfinance_etf_info_workaround_infers_only_missing_name(monkeypatch) -> None:
    from openbb_yfinance.models.etf_info import YFinanceEtfInfoFetcher

    async def _extract(*args, **kwargs):
        return [
            {
                "symbol": "PCCE",
                "legalType": "Exchange Traded Fund",
                "fundFamily": "Polen Capital",
                "category": "Greater China Region",
                "exchange": "PCX",
                "fundInceptionDate": 1710374400,
                "currency": "USD",
                "navPrice": 12.1346,
                "totalAssets": 1548390,
                "longBusinessSummary": "China equity fund.",
            },
            {
                "symbol": "NAMED",
                "longName": "Named ETF",
                "legalType": "Exchange Traded Fund",
            },
        ]

    monkeypatch.setattr(
        YFinanceEtfInfoFetcher,
        "aextract_data",
        staticmethod(_extract),
    )
    records = _fetch_yfinance_etf_info_workaround({"symbol": "PCCE,NAMED"})

    assert records[0]["symbol"] == "PCCE"
    assert records[0]["name"] == "PCCE"
    assert records[0]["name_inferred_from_symbol"] is True
    assert records[0]["nav_price"] == pytest.approx(12.1346)
    assert records[0]["inception_date"] == "2024-03-14"
    assert records[1]["name"] == "Named ETF"
    assert records[1]["name_inferred_from_symbol"] is False


def test_econdb_indicators_workaround_normalizes_null_units(monkeypatch) -> None:
    from openbb_econdb.models.economic_indicators import (
        EconDbEconomicIndicatorsFetcher,
    )
    from openbb_econdb.utils import helpers

    raw = [
        {
            "description": "Consumer confidence",
            "ticker": "CONFKR",
            "geography": "Korea",
            "frequency": "M",
            "dataset": "CONF",
            "data": {"dates": ["2020-01-01"], "values": [100.0]},
        }
    ]

    def _fetch(*, provider, **kwargs):
        assert provider == "econdb"
        query = EconDbEconomicIndicatorsFetcher.transform_query(kwargs)
        return EconDbEconomicIndicatorsFetcher.transform_data(query, raw)

    monkeypatch.setitem(helpers.UNITS, "CONFKR", None)
    monkeypatch.setattr(
        "downloader.download_openbb_archive._resolve_callable",
        lambda obb, endpoint: _fetch,
    )
    result = _fetch_econdb_indicators_workaround(
        {
            "symbol": "CONFKR~",
            "country": None,
            "frequency": "month",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        SimpleNamespace(),
    )
    records = [item.model_dump() for item in result.result]
    assert records == [
        {
            "date": date(2020, 1, 1),
            "value": 100.0,
            "symbol_root": "CONF",
            "symbol": "CONFKR",
            "country": "Korea",
        }
    ]
    assert result.metadata["CONFKR"]["units"] is None


def test_econdb_indicators_workaround_fetches_rejected_exact_ticker(
    monkeypatch,
) -> None:
    from openbb_econdb.utils import helpers

    pages = [
        {
            "results": [
                {
                    "description": "Credit to the private sector",
                    "ticker": "CREDEA",
                    "geography": "Euro area",
                    "frequency": "Q",
                    "dataset": "CRED",
                    "data": {"dates": ["2020-01-01"], "values": [100.0]},
                }
            ],
            "next": "https://www.econdb.com/api/series/?page=2&token=temp-token",
        },
        {
            "results": [
                {
                    "description": "Credit to the private sector",
                    "ticker": "CREDEA",
                    "geography": "Euro area",
                    "frequency": "Q",
                    "dataset": "CRED",
                    "data": {"dates": ["2020-04-01"], "values": [101.0]},
                }
            ],
            "next": None,
        },
    ]
    requested: list[tuple[str, int]] = []

    async def _create_token(*, use_cache):
        assert use_cache is True
        return "temp-token"

    async def _request(url, *, timeout):
        requested.append((url, timeout))
        return pages[len(requested) - 1]

    class _Limiter:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self) -> None:
            self.calls += 1

    def _fetch(*, provider, **kwargs):
        assert provider == "econdb"
        raise RuntimeError(
            "Invalid symbol: 'CREDEA~'. It must have a two-letter country code."
        )

    monkeypatch.setattr(
        "downloader.download_openbb_archive._resolve_callable",
        lambda obb, endpoint: _fetch,
    )
    monkeypatch.setattr(helpers, "create_token", _create_token)
    monkeypatch.setattr(helpers, "amake_request", _request)
    limiter = _Limiter()
    result = _fetch_econdb_indicators_workaround(
        {
            "symbol": "CREDEA~",
            "country": None,
            "frequency": "quarter",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        SimpleNamespace(
            user=SimpleNamespace(credentials=SimpleNamespace(econdb_api_key=None))
        ),
        page_limiter=limiter,
    )

    assert len(requested) == 2
    assert "%5BCREDEA%5D" in requested[0][0]
    assert "from=2000-01-01" in requested[0][0]
    assert "to=2026-07-18" in requested[0][0]
    assert requested[0][1] == 60
    assert limiter.calls == 1
    records = [item.model_dump() for item in result]
    assert [record["date"] for record in records] == [
        date(2020, 1, 1),
        date(2020, 4, 1),
    ]
    assert [record["value"] for record in records] == [100.0, 101.0]


def test_econdb_indicators_exact_ticker_redacts_temporary_token(
    monkeypatch,
) -> None:
    from openbb_econdb.utils import helpers

    async def _create_token(*, use_cache):
        return "do-not-leak"

    async def _request(url, *, timeout):
        raise TimeoutError(f"timed out requesting {url}")

    def _fetch(*, provider, **kwargs):
        raise RuntimeError(
            "Invalid symbol: 'CREDEA~'. It must have a two-letter country code."
        )

    monkeypatch.setattr(
        "downloader.download_openbb_archive._resolve_callable",
        lambda obb, endpoint: _fetch,
    )
    monkeypatch.setattr(helpers, "create_token", _create_token)
    monkeypatch.setattr(helpers, "amake_request", _request)
    with pytest.raises(RuntimeError, match="EconDB exact-ticker request failed") as exc:
        _fetch_econdb_indicators_workaround(
            {
                "symbol": "CREDEA~",
                "country": None,
                "start_date": "2000-01-01",
                "end_date": "2026-07-18",
            },
            SimpleNamespace(
                user=SimpleNamespace(credentials=SimpleNamespace(econdb_api_key=None))
            ),
        )
    assert "do-not-leak" not in str(exc.value)
    assert "token=<redacted>" in str(exc.value)


def test_sec_filing_discovery_uses_provider_safe_90_day_ranges(tmp_path: Path) -> None:
    context = _context(tmp_path)
    context = replace(
        context,
        commands={".equity.discovery.filings": ["fmp"]},
        schemas={
            ".equity.discovery.filings": {
                "input": _InputModel,
                "callable": lambda **kwargs: None,
            }
        },
        start_date="2000-01-01",
        end_date="2000-12-31",
    )
    tasks, coverage = build_initial_plan(context)
    assert coverage[0].initial_task_count == 5
    assert [task.kwargs["start_date"] for task in tasks] == [
        "2000-01-01",
        "2000-03-31",
        "2000-06-29",
        "2000-09-27",
        "2000-12-26",
    ]
    assert [task.kwargs["end_date"] for task in tasks] == [
        "2000-03-30",
        "2000-06-28",
        "2000-09-26",
        "2000-12-25",
        "2000-12-31",
    ]
    for task in tasks:
        assert task.kwargs["limit"] == 1000
        assert task.kwargs["page"] == 0
        assert task.scope_key.endswith("/page=0")
        assert (
            date.fromisoformat(task.kwargs["end_date"])
            - date.fromisoformat(task.kwargs["start_date"])
        ).days < 90


def test_sec_nport_workaround_wraps_single_holding(monkeypatch) -> None:
    from io import BytesIO

    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher

    def _transform(query, data):
        assert data["edgarSubmission"]["formData"]["invstOrSecs"]["invstOrSec"] == [
            {"name": "Only holding", "identifiers": {}}
        ]
        return SimpleNamespace(result=[{"name": "Only holding"}])

    responses = iter(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "0001-25-000001:primary.xml",
                            "_source": {
                                "ciks": [1],
                                "file_date": "2025-10-01",
                                "period_ending": "2025-09-30",
                            },
                        }
                    ]
                }
            },
            b"<edgarSubmission><formData><invstOrSecs><invstOrSec>"
            b"<name>Only holding</name></invstOrSec></invstOrSecs>"
            b"</formData></edgarSubmission>",
        ]
    )

    def _urlopen(request, timeout):
        assert timeout == 60
        value = next(responses)
        return BytesIO(json.dumps(value).encode() if isinstance(value, dict) else value)

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"SMQ": "0000000001"},
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_FUND_SERIES_CACHE",
        {"SMQ": "S0001"},
    )
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(SecNportDisclosureFetcher, "transform_data", _transform)
    assert _fetch_sec_nport_workaround(
        {"symbol": "SMQ", "year": 2025, "quarter": 3, "use_cache": True}
    ) == [{"name": "Only holding"}]


def test_sec_nport_normalizes_all_optional_transformer_mappings() -> None:
    normalized = _normalize_nport_transformer_contract(
        [
            {
                "derivativeInfo": {
                    "optionSwaptionWarrantDeriv": {},
                    "futrDeriv": {},
                    "fwdDeriv": {},
                    "swapDeriv": {
                        "floatingRecDesc": {},
                        "floatingPmntDesc": {},
                    },
                },
                "repurchaseAgrmt": {
                    "repurchaseCollaterals": {"repurchaseCollateral": {}}
                },
            }
        ]
    )[0]

    assert normalized["identifiers"] == {}
    derivative = normalized["derivativeInfo"]
    assert derivative["optionSwaptionWarrantDeriv"]["counterparties"] == {}
    assert derivative["optionSwaptionWarrantDeriv"]["descRefInstrmnt"] == {}
    assert derivative["futrDeriv"]["descRefInstrmnt"] == {}
    assert derivative["fwdDeriv"]["counterparties"] == {}
    swap = derivative["swapDeriv"]
    assert swap["counterparties"] == {}
    assert swap["descRefInstrmnt"] == {}
    assert swap["floatingRecDesc"]["rtResetTenors"]["rtResetTenor"] == {}
    assert swap["floatingPmntDesc"]["rtResetTenors"]["rtResetTenor"] == {}
    collateral = normalized["repurchaseAgrmt"]["repurchaseCollaterals"][
        "repurchaseCollateral"
    ]
    assert math.isnan(float(collateral["principalAmt"]))
    assert math.isnan(float(collateral["collateralVal"]))


def test_sec_nport_rejects_unknown_non_null_optional_mapping_shape() -> None:
    import downloader.download_openbb_archive as archive

    with pytest.raises(
        archive.ProviderResponseShapeError,
        match="descRefInstrmnt must be a mapping or null",
    ):
        _normalize_nport_transformer_contract(
            [{"derivativeInfo": {"futrDeriv": {"descRefInstrmnt": "unexpected"}}}]
        )


def test_sec_nport_workaround_prefers_latest_amendment_and_normalizes_empty_holdings(
    monkeypatch,
) -> None:
    from io import BytesIO

    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher

    requested: list[str] = []
    search_response = {
        "hits": {
            "hits": [
                {
                    "_id": "0001-24-000001:original.xml",
                    "_source": {
                        "ciks": [1],
                        "file_date": "2024-02-28",
                        "period_ending": "2023-12-31",
                    },
                },
                {
                    "_id": "0001-24-000002:amended.xml",
                    "_source": {
                        "ciks": [1],
                        "file_date": "2024-05-01",
                        "period_ending": "2023-12-31",
                    },
                },
            ]
        }
    }

    def _transform(query, data):
        assert data["edgarSubmission"]["formData"]["invstOrSecs"] == {}
        return SimpleNamespace(result=[])

    def _urlopen(request, timeout):
        assert timeout == 60
        requested.append(request.full_url)
        if len(requested) == 1:
            return BytesIO(json.dumps(search_response).encode())
        assert request.full_url.endswith("/000124000002/amended.xml")
        return BytesIO(
            b"<edgarSubmission><formData><invstOrSecs/></formData></edgarSubmission>"
        )

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"COMB": "0000000001"},
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_FUND_SERIES_CACHE",
        {"COMB": "S0001"},
    )
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(SecNportDisclosureFetcher, "transform_data", _transform)

    assert (
        _fetch_sec_nport_workaround(
            {"symbol": "COMB", "year": 2023, "quarter": 4, "use_cache": True}
        )
        == []
    )
    assert len(requested) == 2


def test_sec_nport_workaround_flattens_repeated_investment_containers(
    monkeypatch,
) -> None:
    from io import BytesIO

    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher

    expected = [
        {"name": "First", "identifiers": {}},
        {"name": "Second", "identifiers": {}},
    ]

    def _transform(query, data):
        assert data["edgarSubmission"]["formData"]["invstOrSecs"] == {
            "invstOrSec": expected
        }
        return SimpleNamespace(result=expected)

    responses = iter(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "0001-20-000001:primary.xml",
                            "_source": {
                                "ciks": [1],
                                "file_date": "2021-01-31",
                                "period_ending": "2020-12-31",
                            },
                        }
                    ]
                }
            },
            b"<edgarSubmission><formData>"
            b"<invstOrSecs><invstOrSec><name>First</name></invstOrSec></invstOrSecs>"
            b"<invstOrSecs><invstOrSec><name>Second</name></invstOrSec></invstOrSecs>"
            b"</formData></edgarSubmission>",
        ]
    )

    def _urlopen(request, timeout):
        value = next(responses)
        return BytesIO(json.dumps(value).encode() if isinstance(value, dict) else value)

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"BEDZ": "0000000001"},
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_FUND_SERIES_CACHE",
        {"BEDZ": "S0001"},
    )
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(SecNportDisclosureFetcher, "transform_data", _transform)
    assert (
        _fetch_sec_nport_workaround(
            {"symbol": "BEDZ", "year": 2020, "quarter": 4, "use_cache": True}
        )
        == expected
    )


def test_sec_nport_workaround_expands_every_repurchase_collateral(monkeypatch) -> None:
    from io import BytesIO

    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher

    def _transform(query, data):
        holdings = data["edgarSubmission"]["formData"]["invstOrSecs"]["invstOrSec"]
        assert len(holdings) == 2
        assert [
            item["repurchaseAgrmt"]["repurchaseCollaterals"]["repurchaseCollateral"][
                "principalAmt"
            ]
            for item in holdings
        ] == ["100", "200"]
        return SimpleNamespace(result=holdings)

    responses = iter(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "0001-20-000001:primary.xml",
                            "_source": {
                                "ciks": [1],
                                "file_date": "2021-01-31",
                                "period_ending": "2020-12-31",
                            },
                        }
                    ]
                }
            },
            b"<edgarSubmission><formData><invstOrSecs><invstOrSec>"
            b"<name>Repo</name><repurchaseAgrmt><repurchaseCollaterals>"
            b"<repurchaseCollateral><principalAmt>100</principalAmt></repurchaseCollateral>"
            b"<repurchaseCollateral><principalAmt>200</principalAmt></repurchaseCollateral>"
            b"</repurchaseCollaterals></repurchaseAgrmt>"
            b"</invstOrSec></invstOrSecs></formData></edgarSubmission>",
        ]
    )

    def _urlopen(request, timeout):
        value = next(responses)
        return BytesIO(json.dumps(value).encode() if isinstance(value, dict) else value)

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"BEDZ": "0000000001"},
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_FUND_SERIES_CACHE",
        {"BEDZ": "S0001"},
    )
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(SecNportDisclosureFetcher, "transform_data", _transform)
    result = _fetch_sec_nport_workaround(
        {"symbol": "BEDZ", "year": 2020, "quarter": 4, "use_cache": True}
    )
    assert len(result) == 2


def test_sec_nport_workaround_relationally_expands_nested_mapping_lists(
    monkeypatch,
) -> None:
    from io import BytesIO

    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher

    def _transform(query, data):
        holdings = data["edgarSubmission"]["formData"]["invstOrSecs"]["invstOrSec"]
        assert len(holdings) == 4
        assert {
            (
                item["identifiers"]["other"]["@value"],
                item["derivativeInfo"]["optionSwaptionWarrantDeriv"]["counterparties"][
                    "counterpartyName"
                ],
            )
            for item in holdings
        } == {
            ("ID-1", "Counterparty A"),
            ("ID-1", "Counterparty B"),
            ("ID-2", "Counterparty A"),
            ("ID-2", "Counterparty B"),
        }
        return SimpleNamespace(result=holdings)

    responses = iter(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "0001-23-000001:primary.xml",
                            "_source": {
                                "ciks": [1],
                                "file_date": "2024-01-31",
                                "period_ending": "2023-12-31",
                            },
                        }
                    ]
                }
            },
            b"<edgarSubmission><formData><invstOrSecs><invstOrSec>"
            b"<name>Option holding</name><identifiers>"
            b"<other value='ID-1'/><other value='ID-2'/>"
            b"</identifiers><derivativeInfo><optionSwaptionWarrantDeriv>"
            b"<counterparties><counterpartyName>Counterparty A</counterpartyName>"
            b"</counterparties><counterparties>"
            b"<counterpartyName>Counterparty B</counterpartyName></counterparties>"
            b"</optionSwaptionWarrantDeriv></derivativeInfo>"
            b"</invstOrSec></invstOrSecs></formData></edgarSubmission>",
        ]
    )

    def _urlopen(request, timeout):
        value = next(responses)
        return BytesIO(json.dumps(value).encode() if isinstance(value, dict) else value)

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"BNDD": "0000000001"},
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_FUND_SERIES_CACHE",
        {"BNDD": "S0001"},
    )
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(SecNportDisclosureFetcher, "transform_data", _transform)
    result = _fetch_sec_nport_workaround(
        {"symbol": "BNDD", "year": 2023, "quarter": 4, "use_cache": True}
    )
    assert len(result) == 4


def test_sec_company_facts_bulk_workaround_flattens_all_concepts(monkeypatch) -> None:
    from openbb_sec.utils import frames

    async def _fetch_data(url, use_cache, persist):
        assert url.endswith("/CIK0000320193.json")
        assert (use_cache, persist) == (True, False)
        return {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "facts": {
                "dei": {
                    "EntityCommonStockSharesOutstanding": {
                        "label": "Entity Common Stock, Shares Outstanding",
                        "description": "Shares outstanding.",
                        "units": {
                            "shares": [
                                {
                                    "end": "2025-09-27",
                                    "val": 14_773_000_000,
                                    "accn": "0000320193-25-000079",
                                    "fy": 2025,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2025-10-31",
                                    "frame": "CY2025Q3I",
                                }
                            ]
                        },
                    }
                },
                "us-gaap": {
                    "RevenueFromContractWithCustomerExcludingAssessedTax": {
                        "label": "Revenue",
                        "description": "Revenue from contracts.",
                        "units": {
                            "USD": [
                                {
                                    "start": "2024-09-29",
                                    "end": "2025-09-27",
                                    "val": 416_161_000_000,
                                    "accn": "0000320193-25-000079",
                                    "fy": 2025,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2025-10-31",
                                }
                            ]
                        },
                    }
                },
            },
        }

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"AAPL": "0000320193"},
    )
    monkeypatch.setattr(frames, "fetch_data", _fetch_data)
    records = _fetch_sec_company_facts_bulk_workaround(
        {"symbol": "aapl", "fact": SEC_ALL_COMPANY_FACTS, "use_cache": True}
    )
    assert len(records) == 2
    assert {record["taxonomy"] for record in records} == {"dei", "us-gaap"}
    assert {record["fact_tag"] for record in records} == {
        "EntityCommonStockSharesOutstanding",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
    }
    assert all(record["symbol"] == "AAPL" for record in records)
    assert all(record["cik"] == "0000320193" for record in records)
    assert all(record["accession"] == "0000320193-25-000079" for record in records)


def test_sec_company_facts_bulk_workaround_treats_404_as_empty(monkeypatch) -> None:
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.utils import frames

    class _NotFoundError(RuntimeError):
        status = 404

    async def _fetch_data(url, use_cache, persist):
        raise _NotFoundError("missing companyfacts resource")

    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_SYMBOL_CIK_CACHE",
        {"THH": "0002044241"},
    )
    monkeypatch.setattr(frames, "fetch_data", _fetch_data)
    with pytest.raises(EmptyDataError, match="No company facts were found"):
        _fetch_sec_company_facts_bulk_workaround(
            {"symbol": "THH", "fact": SEC_ALL_COMPANY_FACTS, "use_cache": True}
        )


def test_sec_companyfacts_durable_cache_singleflights_concurrent_tasks(
    monkeypatch, tmp_path: Path
) -> None:
    from openbb_sec.utils import frames

    calls = 0
    calls_lock = threading.Lock()

    async def _fetch_data(url, use_cache, persist):
        nonlocal calls
        assert url.endswith("/CIK0000320193.json")
        assert (use_cache, persist) == (False, False)
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "facts": {"us-gaap": {"Revenue": {"units": {"USD": []}}}},
        }

    class _Limiter:
        def __init__(self) -> None:
            self.calls = 0
            self.lock = threading.Lock()

        def wait(self) -> None:
            with self.lock:
                self.calls += 1

    monkeypatch.setattr(frames, "fetch_data", _fetch_data)
    monkeypatch.setattr("downloader.download_openbb_archive._SEC_HTTP_RUNTIME", None)
    limiter = _Limiter()
    results: list[dict | None] = [None, None]

    def _run(index: int) -> None:
        results[index] = _fetch_sec_companyfacts_response(
            "320193",
            page_limiter=limiter,
            cache_dir=tmp_path,
            use_cache=False,
        )

    threads = [threading.Thread(target=_run, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert calls == 1
    assert limiter.calls == 1
    assert results[0] == results[1]
    assert (tmp_path / "0000" / "CIK0000320193.json.gz").is_file()


def test_sec_companyfacts_cache_injects_requested_cik_when_sec_omits_it(
    monkeypatch, tmp_path: Path
) -> None:
    from openbb_sec.utils import frames

    async def _fetch_data(url, use_cache, persist):
        assert url.endswith("/CIK0002081199.json")
        return {
            "entityName": "Issuer without redundant CIK",
            "facts": {"us-gaap": {"Revenue": {"units": {"USD": []}}}},
        }

    monkeypatch.setattr(frames, "fetch_data", _fetch_data)
    first = _fetch_sec_companyfacts_response(
        "2081199",
        page_limiter=None,
        cache_dir=tmp_path,
        use_cache=False,
    )
    monkeypatch.setattr(
        frames,
        "fetch_data",
        lambda *args, **kwargs: pytest.fail("durable cache was not reused"),
    )
    second = _fetch_sec_companyfacts_response(
        "2081199",
        page_limiter=None,
        cache_dir=tmp_path,
        use_cache=False,
    )

    assert first["cik"] == "0002081199"
    assert second["cik"] == "0002081199"


def test_sec_statement_siblings_share_one_standardized_parse(
    monkeypatch, tmp_path: Path
) -> None:
    import openbb_sec.utils.company_facts as company_facts
    from openbb_sec.models.balance_sheet import SecBalanceSheetFetcher
    from openbb_sec.models.cash_flow import SecCashFlowStatementFetcher
    from openbb_sec.models.income_statement import SecIncomeStatementFetcher

    response = {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {"us-gaap": {"Revenue": {"units": {"USD": []}}}},
    }
    monkeypatch.setattr(
        "downloader.download_openbb_archive._sec_companyfacts_responses_for_symbol",
        lambda *args, **kwargs: ("AAPL", [response]),
    )
    parse_periods: list[str] = []
    resolved = SimpleNamespace(marker="one parse")

    def _resolve(*args, **kwargs):
        parse_periods.append(kwargs["period"])
        return resolved

    monkeypatch.setattr(company_facts, "resolve_company_facts", _resolve)
    for fetcher in (
        SecBalanceSheetFetcher,
        SecCashFlowStatementFetcher,
        SecIncomeStatementFetcher,
    ):
        monkeypatch.setattr(
            fetcher,
            "transform_data",
            staticmethod(lambda query, data, **kwargs: data["result"]),
        )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_STANDARDIZED_CACHE", {}
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_STANDARDIZED_CACHE_ORDER", deque()
    )

    kwargs = {
        "symbol": "AAPL",
        "period": "annual",
        "limit": 1000,
        "pit_mode": True,
        "include_preliminary": True,
        "use_cache": False,
    }
    for endpoint in (
        "equity.fundamental.balance",
        "equity.fundamental.cash",
        "equity.fundamental.income",
    ):
        transformed = _fetch_sec_statement_workaround(
            endpoint,
            kwargs,
            page_limiter=None,
            cache_dir=tmp_path,
        )
        assert len(transformed) == 1
        assert transformed[0] is resolved

    assert parse_periods == ["annual"]


def test_sec_statement_standardized_disk_cache_survives_memory_eviction(
    monkeypatch, tmp_path: Path
) -> None:
    import openbb_sec.utils.company_facts as company_facts
    from openbb_sec.models.balance_sheet import SecBalanceSheetFetcher
    from openbb_sec.models.cash_flow import SecCashFlowStatementFetcher

    response = {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {"us-gaap": {"Revenue": {"units": {"USD": []}}}},
    }
    monkeypatch.setattr(
        "downloader.download_openbb_archive._sec_companyfacts_responses_for_symbol",
        lambda *args, **kwargs: ("AAPL", [response]),
    )
    resolved = company_facts.StandardizedStatements(
        entity_name="Apple Inc.",
        cik=320193,
        income_statement=[{"period_ending": "2025-09-30", "tag": "revenue"}],
        balance_sheet=[{"period_ending": "2025-09-30", "tag": "assets"}],
        cash_flow=[{"period_ending": "2025-09-30", "tag": "cash"}],
    )
    parse_calls = 0

    def _resolve(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return resolved

    monkeypatch.setattr(company_facts, "resolve_company_facts", _resolve)
    for fetcher in (SecBalanceSheetFetcher, SecCashFlowStatementFetcher):
        monkeypatch.setattr(
            fetcher,
            "transform_data",
            staticmethod(lambda query, data, **kwargs: data["result"]),
        )
    memory_cache: dict = {}
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_STANDARDIZED_CACHE", memory_cache
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._SEC_STANDARDIZED_CACHE_ORDER", deque()
    )
    kwargs = {
        "symbol": "AAPL",
        "period": "annual",
        "limit": 1000,
        "pit_mode": True,
        "include_preliminary": True,
        "use_cache": False,
    }

    first = _fetch_sec_statement_workaround(
        "equity.fundamental.balance",
        kwargs,
        page_limiter=None,
        cache_dir=tmp_path,
    )
    memory_cache.clear()
    second = _fetch_sec_statement_workaround(
        "equity.fundamental.cash",
        kwargs,
        page_limiter=None,
        cache_dir=tmp_path,
    )

    assert parse_calls == 1
    assert first == second == [resolved]
    assert len(list((tmp_path / "_standardized").glob("*/*.json.gz"))) == 1


def test_sec_statement_spawned_process_reads_cache_without_network(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import downloader.download_openbb_archive as archive
    from openbb_sec.utils.company_facts import StandardizedStatements

    cik = "0000320193"
    raw = {
        "cik": cik,
        "entityName": "Apple Inc.",
        "facts": {"us-gaap": {"Assets": {"units": {"USD": []}}}},
    }
    archive._write_sec_companyfacts_cache(
        archive._sec_companyfacts_cache_path(tmp_path, cik), cik, raw
    )
    key = ("sec", (cik,), "annual", True, True)
    resolved = StandardizedStatements(
        entity_name="Apple Inc.",
        cik=320193,
        balance_sheet=[
            {
                "period_ending": "2025-09-30",
                "fiscal_period": "FY",
                "fiscal_year": 2025,
                "currency": "USD",
                "tag": "total_assets",
                "value": 100.0,
                "label": "Assets",
                "description": "Assets",
            }
        ],
        income_statement=[],
        cash_flow=[],
    )
    archive._write_sec_standardized_disk_cache(
        archive._sec_standardized_disk_cache_path(tmp_path, key), key, resolved
    )
    monkeypatch.setenv("OPENBB_SEC_PROCESS_WORKERS", "1")
    pool = archive._create_sec_statement_process_pool()
    assert pool is not None
    try:
        result = pool.submit(
            archive._project_sec_statement_cached_process,
            "equity.fundamental.balance",
            {
                "symbol": "AAPL",
                "period": "annual",
                "limit": 1000,
                "pit_mode": True,
                "include_preliminary": True,
                "use_cache": False,
            },
            str(tmp_path),
            (cik,),
        ).result(timeout=30)
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    assert len(result) == 1
    assert result[0].total_assets == pytest.approx(100.0)


def test_sec_statement_drops_only_exact_invalid_mapped_cell(
    monkeypatch, tmp_path: Path
) -> None:
    import openbb_sec.utils.company_facts as company_facts

    response = {
        "cik": 1,
        "entityName": "Validation Recovery Inc.",
        "facts": {},
    }
    resolved = company_facts.StandardizedStatements(
        entity_name="Validation Recovery Inc.",
        cik=1,
        income_statement=[
            {
                "period_ending": "2025-12-31",
                "tag": "weighted_ave_basic_diluted_shares_os",
                "value": 0.04,
            },
            {
                "period_ending": "2025-12-31",
                "tag": "total_revenue",
                "value": 123.0,
            },
        ],
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._sec_companyfacts_responses_for_symbol",
        lambda *args, **kwargs: ("RECOVERY", [response]),
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._resolve_sec_standardized_cached",
        lambda *args, **kwargs: resolved,
    )

    transformed = _fetch_sec_statement_workaround(
        "equity.fundamental.income",
        {
            "symbol": "RECOVERY",
            "period": "annual",
            "limit": 1000,
            "pit_mode": True,
            "include_preliminary": True,
        },
        page_limiter=None,
        cache_dir=tmp_path,
    )

    assert isinstance(transformed, list)
    row = transformed[0].model_dump()
    assert row["weighted_ave_basic_diluted_shares_os"] is None
    assert row["total_revenue"] == 123.0
    recoveries = json.loads(row["openbb_validation_recoveries"])
    assert recoveries == [
        {
            "error_type": "int_from_float",
            "field": "weighted_ave_basic_diluted_shares_os",
            "invalid_value": 0.04,
        }
    ]


def test_manifest_requeues_sec_statement_validation_permanent_once(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.income",
        "PRCH/period=quarter",
        {"symbol": "PRCH", "period": "quarter"},
        ("sec", "yfinance"),
    )
    output_path = task.output_path
    result = TaskResult(
        task,
        "success",
        "yfinance",
        5,
        output_path,
        1,
        provider_outcomes={"sec": "permanent"},
        provider_evidence={
            "sec": (
                "sec: ValidationError: weighted_ave_basic_diluted_shares_os "
                "Input should be a valid integer"
            )
        },
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="sec-validation-recovery")
        manifest.complete(result)

        assert (
            manifest.repair_sec_statement_validation_permanents(
                plan_token="sec-validation-recovery"
            )
            == 1
        )
        row = manifest.connection.execute(
            """
            SELECT status,selected_provider,rows,error,output_path,
                   provider_outcomes_json,provider_evidence_json
            FROM tasks WHERE task_id=?
            """,
            (task.task_id,),
        ).fetchone()
        assert row["status"] == "pending"
        assert row["selected_provider"] is None
        assert row["rows"] == 0
        assert row["output_path"] == output_path
        assert json.loads(row["provider_outcomes_json"]) == {}
        assert json.loads(row["provider_evidence_json"]) == {}
        assert "recoverable" in row["error"]
        assert (
            manifest.repair_sec_statement_validation_permanents(
                plan_token="sec-validation-recovery"
            )
            == 0
        )
    finally:
        manifest.close()


def test_manifest_requeues_only_physical_sec_statement_wrapper_shards(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    wrapper = make_task(
        context,
        "equity.fundamental.income",
        "WRAPPER/period=annual",
        {"symbol": "WRAPPER", "period": "annual"},
        ("sec",),
    )
    columnar = make_task(
        context,
        "equity.fundamental.cash",
        "COLUMNAR/period=annual",
        {"symbol": "COLUMNAR", "period": "annual"},
        ("sec",),
    )
    for task, records in (
        (wrapper, [{"result": "[]", "metadata": "{}"}]),
        (columnar, [{"period_ending": "2025-12-31", "cash": 1.0}]),
    ):
        path = Path(task.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(records), path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([wrapper, columnar], plan_token="sec-wrapper-recovery")
        manifest.complete(
            TaskResult(wrapper, "success", "sec", 1, wrapper.output_path, 1)
        )
        manifest.complete(
            TaskResult(columnar, "success", "sec", 1, columnar.output_path, 1)
        )

        assert (
            manifest.repair_sec_statement_wrapper_shards(
                plan_token="sec-wrapper-recovery"
            )
            == 1
        )
        statuses = {
            str(row["task_id"]): str(row["status"])
            for row in manifest.connection.execute(
                "SELECT task_id,status FROM tasks ORDER BY task_id"
            )
        }
        assert statuses == {
            wrapper.task_id: "pending",
            columnar.task_id: "success",
        }
        assert (
            manifest.repair_sec_statement_wrapper_shards(
                plan_token="sec-wrapper-recovery"
            )
            == 0
        )
    finally:
        manifest.close()


def test_worker_routes_sec_statement_after_provider_period_translation(
    monkeypatch, tmp_path: Path
) -> None:
    seen: list[tuple[str, dict[str, object]]] = []

    def _workaround(endpoint, kwargs, **unused):
        seen.append((endpoint, dict(kwargs)))
        return [{"period_ending": "2025-03-31", "total_assets": 1.0}]

    def _must_not_call(**kwargs):
        raise AssertionError("generic OpenBB statement route was called")

    monkeypatch.setattr(
        "downloader.download_openbb_archive._install_sec_http_limiter",
        lambda runtime: None,
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_sec_statement_workaround",
        _workaround,
    )
    obb = SimpleNamespace(
        equity=SimpleNamespace(
            fundamental=SimpleNamespace(balance=_must_not_call),
        )
    )
    worker = OpenBBWorker(
        obb,
        _runtime({"sec": 1000.0}, {"sec": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
        sec_companyfacts_cache_dir=tmp_path / "raw",
    )
    task = DownloadTask(
        task_id="sec-statement-route",
        endpoint="equity.fundamental.balance",
        category="equity",
        scope_key="AAPL/period=quarter",
        kwargs={"symbol": "AAPL", "period": "quarter", "limit": 1000},
        providers=("sec",),
        output_path=str(tmp_path / "balance.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert seen == [
        (
            "equity.fundamental.balance",
            {"symbol": "AAPL", "period": "quarterly", "limit": 1000},
        )
    ]


def test_worker_routes_sec_insider_bulk_without_generic_openbb(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[dict[str, object]] = []

    def _bulk(kwargs, **unused):
        seen.append(dict(kwargs))
        return [{"symbol": "AAPL", "filing_date": "2024-02-15"}]

    monkeypatch.setattr(
        "downloader.download_openbb_archive._install_sec_http_limiter",
        lambda runtime: None,
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_sec_insider_bulk_workaround",
        _bulk,
    )
    worker = OpenBBWorker(
        SimpleNamespace(),
        _runtime({"sec": 1000.0}, {"sec": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
        sec_insider_cache_dir=tmp_path / "raw",
    )
    task = DownloadTask(
        task_id="sec-insider-bulk-route",
        endpoint="equity.ownership.insider_trading",
        category="equity",
        scope_key="bulk/year=2024/quarter=1",
        kwargs={
            "symbol": "__all__",
            "year": 2024,
            "quarter": 1,
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
            "_archive_sec_insider_bulk": True,
        },
        providers=("sec",),
        output_path=str(tmp_path / "insider.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert seen == [
        {
            "symbol": "__all__",
            "year": 2024,
            "quarter": 1,
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
        }
    ]


def test_sec_insider_private_markers_are_not_sent_to_fmp_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[dict[str, object]] = []

    def _fmp(kwargs, *unused_args, **unused_kwargs):
        seen.append(dict(kwargs))
        return [{"symbol": "AAPL", "filing_date": "2005-01-03"}]

    monkeypatch.setattr(
        "downloader.download_openbb_archive._install_provider_http_limiter",
        lambda runtime: None,
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_fmp_insider_trading_workaround",
        _fmp,
    )
    worker = OpenBBWorker(
        SimpleNamespace(),
        _runtime({"fmp": 1000.0}, {"fmp": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    task = DownloadTask(
        task_id="fmp-insider-fallback-route",
        endpoint="equity.ownership.insider_trading",
        category="equity",
        scope_key="AAPL/legacy=2000-01-01_2005-12-31",
        kwargs={
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2005-12-31",
            "_archive_sec_insider_range": True,
        },
        providers=("sec", "fmp"),
        output_path=str(tmp_path / "fmp-insider.parquet"),
        provider_outcomes={"sec": "empty"},
    )

    result = worker(task)

    assert result.status == "success"
    assert result.provider == "fmp"
    assert seen == [
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2005-12-31",
        }
    ]


def test_fred_ground_beef_workaround_unwraps_secret(monkeypatch) -> None:
    from openbb_fred.models.retail_prices import FredRetailPricesFetcher

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key=_Secret()))
    )

    async def _extract(query, credentials):
        assert query.item == "ground_beef"
        assert credentials == {"fred_api_key": "test-key"}
        return {"data": "raw"}

    def _transform(query, data):
        assert data == {"data": "raw"}
        return SimpleNamespace(result=[{"description": "Ground Beef"}])

    monkeypatch.setattr(FredRetailPricesFetcher, "aextract_data", _extract)
    monkeypatch.setattr(FredRetailPricesFetcher, "transform_data", _transform)
    assert _fetch_fred_retail_prices_workaround({"item": "ground_beef"}, obb) == [
        {"description": "Ground Beef"}
    ]


def test_fred_bond_indices_workaround_bypasses_shared_router_schema(
    monkeypatch,
) -> None:
    from openbb_fred.models.bond_indices import FredBondIndicesFetcher

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key=_Secret()))
    )

    async def _fetch(params, credentials):
        assert params == {
            "category": "high_yield",
            "index": "europe",
            "index_type": "oas",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        }
        assert credentials == {"fred_api_key": "test-key"}
        return SimpleNamespace(result=[{"symbol": "BAMLHE00EHYIOAS"}])

    monkeypatch.setattr(FredBondIndicesFetcher, "fetch_data", staticmethod(_fetch))
    assert _fetch_fred_bond_indices_workaround(
        {
            "category": "high_yield",
            "index": "europe",
            "index_type": "oas",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        obb,
    ) == [{"symbol": "BAMLHE00EHYIOAS"}]


def test_fred_calendar_workaround_uses_api_and_paginates(monkeypatch) -> None:
    from io import BytesIO
    from urllib.parse import parse_qs, urlparse

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key=_Secret()))
    )
    pages = {
        0: {
            "count": 3,
            "offset": 0,
            "limit": 1000,
            "release_dates": [
                {"release_id": 1, "release_name": "First", "date": "2000-01-03"},
                {"release_id": 2, "release_name": "Second", "date": "2000-01-04"},
            ],
        },
        2: {
            "count": 3,
            "offset": 2,
            "limit": 1000,
            "release_dates": [
                {"release_id": 3, "release_name": "Third", "date": "2000-01-05"}
            ],
        },
    }

    def _urlopen(request, timeout):
        assert timeout == 60
        query = parse_qs(urlparse(request.full_url).query)
        assert query["api_key"] == ["test-key"]
        assert query["realtime_start"] == ["2000-01-01"]
        assert query["realtime_end"] == ["2000-12-31"]
        assert query["include_release_dates_with_no_data"] == ["true"]
        payload = pages[int(query["offset"][0])]
        return BytesIO(json.dumps(payload).encode())

    limiter_calls = []

    class _PageLimiter:
        def wait(self):
            limiter_calls.append(True)

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    records = _fetch_fred_calendar_workaround(
        {"start_date": "2000-01-01", "end_date": "2000-12-31", "country": "all"},
        obb,
        page_limiter=_PageLimiter(),
    )
    assert [record["event"] for record in records] == ["First", "Second", "Third"]
    assert all(record["source"] == "FRED" for record in records)
    # The worker already claimed the first page's provider slot; only the
    # continuation page needs an additional limiter claim.
    assert len(limiter_calls) == 1


def test_fred_release_search_workaround_uses_archive_timeout(monkeypatch) -> None:
    from io import BytesIO
    from urllib.parse import parse_qs, urlparse

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key=_Secret()))
    )

    def _urlopen(request, timeout):
        assert timeout == 60
        query = parse_qs(urlparse(request.full_url).query)
        assert query["api_key"] == ["test-key"]
        assert query["release_id"] == ["429"]
        assert query["offset"] == ["45000"]
        assert query["search_type"] == ["release"]
        return BytesIO(
            json.dumps(
                {
                    "count": 45001,
                    "seriess": [
                        {
                            "id": "TESTSERIES",
                            "title": "Test Series",
                            "observation_start": "2000-01-01",
                            "observation_end": "2026-07-18",
                        }
                    ],
                }
            ).encode()
        )

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    records = _fetch_fred_release_search_workaround(
        {
            "query": "",
            "release_id": 429,
            "search_type": "release",
            "limit": 1000,
            "offset": 45000,
        },
        obb,
    )
    assert len(records) == 1
    assert records[0].series_id == "TESTSERIES"
    assert records[0].title == "Test Series"


def test_fred_release_search_workaround_redacts_timeout_secret(monkeypatch) -> None:
    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fred_api_key="test-key"))
    )

    def _urlopen(request, timeout):
        raise TimeoutError("request containing test-key timed out")

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    with pytest.raises(ConnectionError, match="TimeoutError") as caught:
        _fetch_fred_release_search_workaround(
            {"query": "", "release_id": 429, "search_type": "release"},
            obb,
        )
    assert "test-key" not in str(caught.value)


def test_un_comtrade_export_destinations_workaround_uses_current_codes(
    monkeypatch,
) -> None:
    from io import BytesIO

    reference = {
        "results": [
            {
                "id": 841,
                "PartnerCode": 841,
                "PartnerDesc": "USA and Puerto Rico (...1980)",
                "PartnerCodeIsoAlpha2": "US",
                "entryEffectiveDate": "1900-01-01T00:00:00",
                "entryExpiredDate": "1980-12-31T00:00:00",
                "isGroup": False,
            },
            {
                "id": 842,
                "PartnerCode": 842,
                "PartnerDesc": "USA",
                "PartnerCodeIsoAlpha2": "US",
                "entryEffectiveDate": "1981-01-01T00:00:00",
                "isGroup": False,
            },
            {
                "id": 156,
                "PartnerCode": 156,
                "PartnerDesc": "China",
                "PartnerCodeIsoAlpha2": "CN",
                "entryEffectiveDate": "1900-01-01T00:00:00",
                "isGroup": False,
            },
            {
                "id": 756,
                "PartnerCode": 756,
                "PartnerDesc": "Switzerland",
                "PartnerCodeIsoAlpha2": "CH",
                "entryEffectiveDate": "1900-01-01T00:00:00",
                "isGroup": False,
            },
            {
                "id": 757,
                "PartnerCode": 757,
                "PartnerDesc": "Switzerland, Liechtenstein",
                "PartnerCodeIsoAlpha2": "CH",
                "entryEffectiveDate": "1900-01-01T00:00:00",
                "isGroup": False,
            },
        ]
    }
    trade = {
        "data": [
            {"partnerCode": 0, "primaryValue": 9_000_000_000},
            {"partnerCode": 156, "primaryValue": 123_000_000},
        ]
    }

    def _urlopen(request, timeout):
        assert timeout == 60
        payload = reference if "partnerAreas.json" in request.full_url else trade
        if payload is trade:
            assert "reporterCode=842" in request.full_url
            assert "period=2024" in request.full_url
        return BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    monkeypatch.setattr(
        "downloader.download_openbb_archive.UN_COMTRADE_REQUEST_LIMITER",
        _NoopLimiter(),
    )
    _un_comtrade_area_reference.cache_clear()
    records = _fetch_un_comtrade_export_destinations(
        {"country": "us", "end_date": "2024-12-31"}
    )
    assert len(records) == 1
    assert records[0]["origin_country"] == "USA"
    assert records[0]["destination_country"] == "China"
    assert records[0]["value"] == 123.0
    assert records[0]["reference_year"] == 2024
    assert records[0]["source"] == "UN Comtrade"
    reporters, _ = _un_comtrade_area_reference()
    assert [row["PartnerCode"] for row in reporters["CH"]] == [757, 756]
    _un_comtrade_area_reference.cache_clear()


def test_fmp_discovery_filings_workaround_infers_missing_accepted_date(
    monkeypatch,
) -> None:
    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(fmp_api_key=_Secret()))
    )
    requested_urls: list[str] = []

    async def _amake_request(url):
        requested_urls.append(url)
        return [
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "filingDate": "2007-01-15",
                "formType": "10-K",
                "link": "https://www.sec.gov/filing-index",
                "finalLink": "https://www.sec.gov/filing.htm",
            }
        ]

    monkeypatch.setattr(
        "openbb_core.provider.utils.helpers.amake_request", _amake_request
    )
    records = _fetch_fmp_discovery_filings_workaround(
        {
            "start_date": "2006-11-25",
            "end_date": "2007-02-22",
            "limit": 1000,
            "page": 7,
        },
        obb,
        page_limiter=_NoopLimiter(),
    )
    assert len(requested_urls) == 1
    assert "page=7" in requested_urls[0]
    assert "limit=1000" in requested_urls[0]
    assert records[0]["accepted_date"] == "2007-01-15T00:00:00"
    assert records[0]["accepted_date_inferred"] is True


def test_bls_labstat_catalog_prefers_complete_history_file() -> None:
    page = """
    <a href="ws.data.0.Current">current</a>
    <a href="ws.data.1.AllData">all</a>
    <a href="ws.data.2.Region">partition</a>
    <a href="ws.series">series</a>
    <a href="ws.footnote">footnotes</a>
    """

    assert _bls_labstat_catalog_files(page, "ws") == (
        "ws.series",
        "ws.footnote",
        ["ws.data.1.AllData"],
    )


def test_bls_labstat_raw_download_resumes_and_receipt_skips_network(
    monkeypatch, tmp_path: Path
) -> None:
    from io import BytesIO

    output = tmp_path / "ws.data.1.AllData"
    partial = tmp_path / ".ws.data.1.AllData.part"
    partial.write_bytes(b"abc")
    seen_ranges: list[str | None] = []

    class _Response:
        status = 206
        headers = {"Content-Length": "3", "Content-Range": "bytes 3-5/6"}

        def __init__(self) -> None:
            self.stream = BytesIO(b"def")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, size: int = -1) -> bytes:
            return self.stream.read(size)

    def _urlopen(request, timeout):
        del timeout
        seen_ranges.append(request.get_header("Range"))
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    url = "https://download.bls.gov/pub/time.series/ws/ws.data.1.AllData"

    assert _download_bls_labstat_file(url, output) == output
    assert output.read_bytes() == b"abcdef"
    assert seen_ranges == ["bytes=3-"]
    receipt = json.loads(
        (tmp_path / "ws.data.1.AllData.receipt.json").read_text(encoding="utf-8")
    )
    assert receipt["size_bytes"] == 6
    assert len(receipt["sha256"]) == 64

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *args, **kwargs: pytest.fail("valid receipt must skip the network"),
    )
    assert _download_bls_labstat_file(url, output) == output


def test_bls_labstat_series_catalog_allows_missing_title_column(
    tmp_path: Path,
) -> None:
    import duckdb

    series_path = tmp_path / "in.series"
    series_path.write_text(
        "series_id\tseasonal\teconomicseries_code\tbegin_year\tend_year\n"
        "INUS0001 \tU\tGDP\t2000\t2026\n",
        encoding="utf-8",
    )
    connection = duckdb.connect()
    try:
        _create_bls_labstat_series_table(connection, series_path)
        rows = connection.execute(
            "SELECT series_id, series_title FROM series"
        ).fetchall()
    finally:
        connection.close()

    assert rows == [("INUS0001", None)]


def test_bls_labstat_reads_indexed_bulk_and_computes_changes(
    monkeypatch, tmp_path: Path
) -> None:
    import duckdb

    database = tmp_path / "ws.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "CREATE TABLE observations("
            "series_id VARCHAR, year INTEGER, period VARCHAR, value DOUBLE, "
            "footnote_codes VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO observations VALUES (?,?,?,?,?)",
            [
                ("WSU001", 2020, "M01", 100.0, None),
                ("WSU001", 2020, "M02", 110.0, "P"),
                ("WSU001", 2020, "M03", 121.0, None),
                ("WSU001", 2020, "M13", 111.0, None),
            ],
        )
        connection.execute(
            "CREATE TABLE series(series_id VARCHAR, series_title VARCHAR)"
        )
        connection.execute("INSERT INTO series VALUES ('WSU001','Weekly wages')")
        connection.execute(
            "CREATE TABLE footnotes(footnote_code VARCHAR, footnote_text VARCHAR)"
        )
        connection.execute("INSERT INTO footnotes VALUES ('P','Preliminary')")
    finally:
        connection.close()
    monkeypatch.setattr(
        "downloader.download_openbb_archive._ensure_bls_labstat_database",
        lambda *args, **kwargs: database,
    )

    table = _fetch_bls_series_labstat_table(
        {
            "symbol": "WSU001",
            "start_date": "2020-02-01",
            "end_date": "2020-03-31",
            "calculations": True,
            "annual_average": False,
        },
        cache_dir=tmp_path,
    )
    records = table.to_pylist()

    assert [row["date"] for row in records] == ["2020-03-01", "2020-02-01"]
    assert records[0]["latest"] is True
    assert records[0]["change_1M"] == pytest.approx(11.0)
    assert records[0]["change_percent_1M"] == pytest.approx(0.1)
    assert records[1]["change_1M"] == pytest.approx(10.0)
    assert records[1]["footnotes"] == "Preliminary"
    assert all(row["title"] == "Weekly wages" for row in records)
    assert all(row["_bls_source"] == "labstat_bulk" for row in records)
    assert all(row["_bls_labstat_prefix"] == "ws" for row in records)


def test_worker_uses_bls_bulk_during_api_cooldown(
    monkeypatch, tmp_path: Path
) -> None:
    runtime = ProviderRuntime(
        {"bls": 5.0},
        {"bls": 2},
        quota_cooldown=3600,
    )
    runtime.block_quota("bls", "daily threshold reached")
    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_bls_series_labstat_table",
        lambda *args, **kwargs: pa.Table.from_pylist(
            [
                {
                    "symbol": "WSU001",
                    "date": "2020-01-01",
                    "value": 1.0,
                    "latest": True,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive.normalize_records",
        lambda *args, **kwargs: pytest.fail(
            "columnar BLS results must bypass Python row normalization"
        ),
    )
    worker = OpenBBWorker(
        SimpleNamespace(user=SimpleNamespace(credentials=SimpleNamespace())),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
        bls_labstat_cache_dir=tmp_path / "bulk",
    )
    task = DownloadTask(
        task_id="bls-bulk-cooldown",
        endpoint="economy.survey.bls_series",
        category="economy",
        scope_key="category=wages/batch=00000/n=1",
        kwargs={
            "symbol": "WSU001",
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
        },
        providers=("bls",),
        output_path=str(tmp_path / "bls.parquet"),
    )

    result = worker(task)

    assert result.status == "success"
    assert result.provider == "bls"
    table = pq.read_table(tmp_path / "bls.parquet")
    assert table.num_rows == 1
    assert table.column("_provider").to_pylist() == ["bls"]
    assert table.column("_openbb_endpoint").to_pylist() == [
        "economy.survey.bls_series"
    ]


def test_bls_resilient_fetch_splits_only_timed_out_payloads(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], int, int]] = []

    async def _fetch(*, series_ids, start_year, end_year, **kwargs):
        symbols = tuple(series_ids)
        calls.append((symbols, start_year, end_year))
        if len(symbols) > 2:
            raise TimeoutError("large BLS response")
        return {
            "data": [
                {
                    "symbol": symbol,
                    "date": f"{start_year}-01-01",
                    "value": 1.0,
                }
                for symbol in symbols
            ],
            "metadata": {},
            "messages": [],
        }

    monkeypatch.setattr("openbb_bls.utils.helpers.get_bls_timeseries", _fetch)

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(bls_api_key=_Secret()))
    )
    records = _fetch_bls_series_resilient(
        {
            "symbol": "SERIES1,SERIES2,SERIES3,SERIES4",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
        },
        obb,
        page_limiter=_NoopLimiter(),
    )
    assert [len(symbols) for symbols, _, _ in calls] == [4, 2, 2]
    assert {record["symbol"] for record in records} == {
        "SERIES1",
        "SERIES2",
        "SERIES3",
        "SERIES4",
    }


def test_bls_resilient_fetch_splits_null_parser_response(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], int, int]] = []

    async def _fetch(*, series_ids, start_year, end_year, **kwargs):
        symbols = tuple(series_ids)
        calls.append((symbols, start_year, end_year))
        if len(symbols) > 1:
            raise AttributeError("'NoneType' object has no attribute 'get'")
        return {
            "data": [
                {
                    "symbol": symbols[0],
                    "date": f"{start_year}-01-01",
                    "value": 1.0,
                }
            ],
            "metadata": {},
            "messages": [],
        }

    monkeypatch.setattr("openbb_bls.utils.helpers.get_bls_timeseries", _fetch)

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(bls_api_key=_Secret()))
    )
    records = _fetch_bls_series_resilient(
        {
            "symbol": "SERIES1,SERIES2",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
        },
        obb,
        page_limiter=_NoopLimiter(),
    )
    assert [len(symbols) for symbols, _, _ in calls] == [2, 1, 1]
    assert {record["symbol"] for record in records} == {"SERIES1", "SERIES2"}


def test_bls_resilient_fetch_marks_unsplittable_null_response_transient(
    monkeypatch,
) -> None:
    async def _fetch(**kwargs):
        raise AttributeError("'NoneType' object has no attribute 'get'")

    monkeypatch.setattr("openbb_bls.utils.helpers.get_bls_timeseries", _fetch)

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(bls_api_key=_Secret()))
    )
    with pytest.raises(RuntimeError, match="BLS server error") as caught:
        _fetch_bls_series_resilient(
            {
                "symbol": "SERIES1",
                "start_date": "2000-01-01",
                "end_date": "2000-12-31",
            },
            obb,
            page_limiter=_NoopLimiter(),
        )
    assert classify_error(caught.value) == "transient"


def test_bls_resilient_fetch_preserves_raised_daily_quota_signal(monkeypatch) -> None:
    from openbb_core.provider.utils.errors import EmptyDataError

    async def _fetch(**kwargs):
        raise EmptyDataError(
            "No data found: the daily threshold for total number of requests "
            "allocated to the user has been reached."
        )

    monkeypatch.setattr("openbb_bls.utils.helpers.get_bls_timeseries", _fetch)

    obb = SimpleNamespace(
        user=SimpleNamespace(credentials=SimpleNamespace(bls_api_key="test-key"))
    )
    with pytest.raises(RuntimeError, match="daily threshold") as caught:
        _fetch_bls_series_resilient(
            {
                "symbol": "SERIES1",
                "start_date": "2000-01-01",
                "end_date": "2000-12-31",
            },
            obb,
            page_limiter=_NoopLimiter(),
        )
    assert classify_error(caught.value) == "rate"


def test_congress_info_workaround_paginates_metadata_without_leaking_key(
    monkeypatch,
) -> None:
    requested_urls: list[str] = []

    def _fetch(url: str):
        from urllib.parse import parse_qs, urlsplit

        requested_urls.append(url)
        split = urlsplit(url)
        query = parse_qs(split.query)
        assert query["api_key"] == ["test-key"]
        if split.path.endswith("/cosponsors"):
            offset = int(query.get("offset", ["0"])[0])
            count = 250 if offset == 0 else 50
            payload = {
                "cosponsors": [
                    {"bioguideId": f"ID{offset + index:03d}"} for index in range(count)
                ],
                "pagination": {},
            }
            if offset == 0:
                payload["pagination"] = {
                    "next": (
                        "https://api.congress.gov/v3/amendment/114/samdt/1545/"
                        "cosponsors?format=json&offset=250"
                    )
                }
            return payload
        return {
            "amendment": {
                "congress": 114,
                "type": "SAMDT",
                "number": "1545",
                "cosponsors": {
                    "count": 300,
                    "url": (
                        "https://api.congress.gov/v3/amendment/114/samdt/1545/"
                        "cosponsors?format=json"
                    ),
                },
                "actions": {"count": 0},
                "textVersions": {"count": 0},
            }
        }

    monkeypatch.setattr("downloader.download_openbb_archive._congress_json", _fetch)

    class _Secret:
        def get_secret_value(self):
            return "test-key"

    obb = SimpleNamespace(
        user=SimpleNamespace(
            credentials=SimpleNamespace(congress_gov_api_key=_Secret())
        )
    )
    source_url = "https://api.congress.gov/v3/amendment/114/samdt/1545?format=json"
    records = _fetch_congress_info_workaround(
        {"amendment_url": source_url},
        obb,
        kind="amendment",
        page_limiter=_NoopLimiter(),
    )
    assert len(requested_urls) == 3
    assert len(records[0]["cosponsors"]) == 300
    assert records[0]["source_url"] == source_url
    assert "api_key" not in records[0]["source_url"]


def test_congress_info_workaround_resumes_from_subrequest_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import downloader.download_openbb_archive as archive
    from urllib.parse import parse_qs, urlsplit

    checkpoint_dir = tmp_path / "request_checkpoints" / "task"
    requested_urls: list[str] = []
    fail_last_page = True

    def _fetch(url: str):
        nonlocal fail_last_page
        requested_urls.append(url)
        split = urlsplit(url)
        query = parse_qs(split.query)
        assert query["api_key"] == ["test-key"]
        if not split.path.endswith("/cosponsors"):
            return {
                "amendment": {
                    "congress": 114,
                    "type": "SAMDT",
                    "number": "1545",
                    "cosponsors": {
                        "count": 300,
                        "url": (
                            "https://api.congress.gov/v3/amendment/114/samdt/1545/"
                            "cosponsors?format=json"
                        ),
                    },
                    "actions": {"count": 0},
                    "textVersions": {"count": 0},
                }
            }
        offset = int(query.get("offset", ["0"])[0])
        if offset == 250 and fail_last_page:
            fail_last_page = False
            raise TimeoutError("simulated final-page timeout")
        count = 250 if offset == 0 else 50
        payload = {
            "cosponsors": [
                {"bioguideId": f"ID{offset + index:03d}"} for index in range(count)
            ],
            "pagination": {},
        }
        if offset == 0:
            payload["pagination"] = {
                "next": (
                    "https://api.congress.gov/v3/amendment/114/samdt/1545/"
                    "cosponsors?format=json&offset=250"
                )
            }
        return payload

    monkeypatch.setattr(archive, "_congress_json", _fetch)
    obb = SimpleNamespace(
        user=SimpleNamespace(
            credentials=SimpleNamespace(congress_gov_api_key="test-key")
        )
    )
    kwargs = {
        "amendment_url": (
            "https://api.congress.gov/v3/amendment/114/samdt/1545?format=json"
        )
    }

    with pytest.raises(TimeoutError, match="final-page"):
        _fetch_congress_info_workaround(
            kwargs,
            obb,
            kind="amendment",
            page_limiter=_NoopLimiter(),
            checkpoint_dir=checkpoint_dir,
        )

    first_attempt_urls = list(requested_urls)
    assert len(first_attempt_urls) == 3
    checkpoint_files = sorted(checkpoint_dir.glob("*.json"))
    assert len(checkpoint_files) == 2
    assert all("test-key" not in path.read_text() for path in checkpoint_files)

    requested_urls.clear()
    records = _fetch_congress_info_workaround(
        kwargs,
        obb,
        kind="amendment",
        page_limiter=_NoopLimiter(),
        checkpoint_dir=checkpoint_dir,
    )

    assert len(records[0]["cosponsors"]) == 300
    assert len(requested_urls) == 1
    assert "offset=250" in requested_urls[0]
    assert len(list(checkpoint_dir.glob("*.json"))) == 3
    archive._clear_request_checkpoints(checkpoint_dir)
    assert not checkpoint_dir.exists()


def test_manifest_resume_skips_success_and_recovers_missing_output(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "interval": "1d",
        },
        ("yfinance",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        from downloader.download_openbb_archive import TaskResult

        output = Path(task.output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.touch()
        manifest.complete(TaskResult(task, "success", "yfinance", 1, str(output), 1))
        manifest.prepare_run(retry_failed=True, retry_empty=False, refresh=False)
        assert manifest.pending_batch(10, 20) == []
        output.unlink()
        manifest.prepare_run(retry_failed=True, retry_empty=False, refresh=False)
        assert [item.task_id for item in manifest.pending_batch(10, 20)] == [
            task.task_id
        ]
    finally:
        manifest.close()


def test_manifest_retry_clears_only_outcomes_that_must_be_retried(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    failed = make_task(
        context,
        "equity.profile",
        "FAILED",
        {"symbol": "FAILED"},
        ("sec", "fmp"),
    )
    empty = make_task(
        context,
        "equity.profile",
        "EMPTY",
        {"symbol": "EMPTY"},
        ("sec", "fmp"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([failed, empty], plan_token="retry-outcomes")
        manifest.complete(
            TaskResult(
                failed,
                "failed",
                "fmp",
                0,
                None,
                2,
                provider_outcomes={"sec": "empty", "fmp": "permanent"},
            )
        )
        manifest.complete(
            TaskResult(
                empty,
                "empty",
                "fmp",
                0,
                None,
                2,
                provider_outcomes={"sec": "empty", "fmp": "empty"},
            )
        )

        manifest.prepare_run(
            retry_failed=True,
            retry_empty=True,
            refresh=False,
            repair_legacy=False,
            plan_token="retry-outcomes",
        )
        rows = {
            row["scope_key"]: json.loads(row["provider_outcomes_json"])
            for row in manifest.connection.execute(
                "SELECT scope_key,provider_outcomes_json FROM tasks "
                "WHERE plan_token='retry-outcomes'"
            )
        }
        assert rows == {
            "FAILED": {"sec": "empty"},
            "EMPTY": {},
        }
    finally:
        manifest.close()


def test_manifest_migrates_yfinance_etf_missing_name_outcome(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.info",
        "PCCE",
        {"symbol": "PCCE"},
        ("fmp", "yfinance"),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([task], plan_token="etf-name")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key='yfinance_etf_info_missing_name_v1'"
        )
        manifest.connection.execute(
            """UPDATE tasks SET attempts=1,
            provider_outcomes_json='{"yfinance":"permanent"}',
            error='YFinanceEtfInfoData name Field required'
            WHERE task_id=?""",
            (task.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        row = reopened.connection.execute(
            "SELECT status,attempts,provider_outcomes_json FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert tuple(row) == ("pending", 0, "{}")
        assert reopened.meta_value("yfinance_etf_info_missing_name_v1") == "complete"
    finally:
        reopened.close()


def test_manifest_revalidates_pre_transport_evidence_yfinance_results(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    success = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {"symbol": "AAPL"},
        ("yfinance", "fmp"),
    )
    terminal_empty = make_task(
        context,
        "equity.profile",
        "EMPTY",
        {"symbol": "EMPTY"},
        ("fmp", "yfinance"),
    )
    unaffected = make_task(
        context,
        "equity.profile",
        "FMP",
        {"symbol": "FMP"},
        ("fmp", "yfinance"),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks(
            [success, terminal_empty, unaffected], plan_token="yahoo-evidence"
        )
        manifest.connection.execute(
            "DELETE FROM archive_meta "
            "WHERE key='yfinance_http_evidence_revalidation_v1'"
        )
        manifest.connection.execute(
            """UPDATE tasks SET status='success',selected_provider='yfinance',
            rows=10,attempts=1,provider_outcomes_json='{"fmp":"empty"}'
            WHERE task_id=?""",
            (success.task_id,),
        )
        manifest.connection.execute(
            """UPDATE tasks SET status='unavailable',selected_provider='fmp',
            attempts=2,
            provider_outcomes_json='{"fmp":"unavailable","yfinance":"empty"}'
            WHERE task_id=?""",
            (terminal_empty.task_id,),
        )
        manifest.connection.execute(
            """UPDATE tasks SET status='success',selected_provider='fmp',
            rows=1,attempts=1,
            provider_outcomes_json='{"yfinance":"empty"}'
            WHERE task_id=?""",
            (unaffected.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        rows = {
            row["scope_key"]: (
                row["status"],
                row["selected_provider"],
                row["rows"],
                row["attempts"],
                json.loads(row["provider_outcomes_json"]),
            )
            for row in reopened.connection.execute(
                "SELECT scope_key,status,selected_provider,rows,attempts,"
                "provider_outcomes_json FROM tasks "
                "WHERE plan_token='yahoo-evidence'"
            )
        }
        assert rows["AAPL"] == ("pending", None, 0, 0, {"fmp": "empty"})
        assert rows["EMPTY"] == (
            "pending",
            None,
            0,
            0,
            {"fmp": "unavailable"},
        )
        assert rows["FMP"] == (
            "success",
            "fmp",
            1,
            1,
            {"yfinance": "empty"},
        )
        assert reopened.meta_value("yfinance_http_evidence_revalidation_v1") == "2"
    finally:
        reopened.close()


def test_manifest_migrates_tiingo_hourly_allocation_across_markets(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    tasks = [
        make_task(
            context,
            "currency.price.historical",
            "ALLEUR",
            {"symbol": "ALLEUR"},
            ("yfinance", "fmp", "tiingo"),
        ),
        make_task(
            context,
            "etf.historical",
            "00872B.TWO",
            {"symbol": "00872B.TWO"},
            ("yfinance", "fmp", "tiingo"),
        ),
    ]
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks(tasks, plan_token="tiingo-allocation")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key='tiingo_hourly_allocation_rate_v1'"
        )
        manifest.connection.execute(
            """UPDATE tasks SET attempts=2,
            provider_outcomes_json='{"yfinance":"empty","tiingo":"permanent"}',
            error='tiingo: You have run over your hourly request allocation.'
            WHERE plan_token='tiingo-allocation'""",
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        rows = reopened.connection.execute(
            "SELECT endpoint,status,attempts,provider_outcomes_json,"
            "created_at,updated_at FROM tasks "
            "WHERE plan_token='tiingo-allocation' ORDER BY endpoint"
        ).fetchall()
        assert [
            (
                row["endpoint"],
                row["status"],
                row["attempts"],
                json.loads(row["provider_outcomes_json"]),
                row["created_at"] == row["updated_at"],
            )
            for row in rows
        ] == [
            ("currency.price.historical", "pending", 1, {"yfinance": "empty"}, True),
            ("etf.historical", "pending", 1, {"yfinance": "empty"}, True),
        ]
        assert reopened.meta_value("tiingo_hourly_allocation_rate_v1") == "complete"
    finally:
        reopened.close()


def test_manifest_requeues_non_authoritative_tiingo_historical_empties(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    affected = [
        (
            "currency.price.historical",
            "AEDJOD",
            "tiingo: AEDJOD: Start date must be >= 2020-01-01",
        ),
        (
            "equity.price.historical",
            "ABC",
            "tiingo: You have run over your hourly request allocation.",
        ),
        (
            "etf.historical",
            "ETF",
            "tiingo: You do not have permission to access the News API",
        ),
        (
            "index.price.historical",
            "^IDX",
            "tiingo: You have run over your daily request allocation.",
        ),
    ]
    tasks = [
        make_task(
            context,
            endpoint,
            symbol,
            {"symbol": symbol, "start_date": "2000-01-01"},
            ("yfinance", "fmp", "tiingo"),
        )
        for endpoint, symbol, _ in affected
    ]
    unaffected = make_task(
        context,
        "currency.price.historical",
        "EMPTY",
        {"symbol": "EMPTY", "start_date": "2000-01-01"},
        ("yfinance", "fmp", "tiingo"),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([*tasks, unaffected], plan_token="tiingo-false-empty")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key="
            "'tiingo_historical_non_authoritative_empty_v1'"
        )
        for task, (_, _, error) in zip(tasks, affected, strict=True):
            manifest.connection.execute(
                """UPDATE tasks SET status='empty',selected_provider='tiingo',
                attempts=3,error=?,provider_outcomes_json='{}'
                WHERE task_id=?""",
                (error, task.task_id),
            )
        manifest.connection.execute(
            """UPDATE tasks SET status='empty',selected_provider='tiingo',
            attempts=1,error='tiingo: no results',provider_outcomes_json='{}'
            WHERE task_id=?""",
            (unaffected.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        rows = reopened.connection.execute(
            "SELECT scope_key,status,selected_provider,attempts,error,"
            "provider_outcomes_json,created_at,updated_at FROM tasks "
            "WHERE plan_token='tiingo-false-empty' ORDER BY scope_key"
        ).fetchall()
        repaired = {task.scope_key for task in tasks}
        for row in rows:
            if row["scope_key"] in repaired:
                assert row["status"] == "pending"
                assert row["selected_provider"] is None
                assert row["attempts"] == 0
                assert row["error"].startswith("requeued: Tiingo historical")
                assert json.loads(row["provider_outcomes_json"]) == {}
                assert row["created_at"] == row["updated_at"]
            else:
                assert row["status"] == "empty"
                assert row["error"] == "tiingo: no results"
        assert (
            reopened.meta_value("tiingo_historical_non_authoritative_empty_v1") == "4"
        )
    finally:
        reopened.close()


def test_manifest_migrates_dns_permanent_outcome_across_providers(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    tasks = [
        make_task(
            context,
            "equity.fundamental.management",
            "ICA",
            {"symbol": "ICA"},
            ("fmp", "yfinance"),
        ),
        make_task(
            context,
            "fixedincome.government.yield_curve",
            "date=2026-07-18/type=nominal",
            {
                "date": "2026-07-18",
                "country": "united_states",
                "yield_curve_type": "nominal",
            },
            ("federal_reserve", "fred"),
        ),
    ]
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks(tasks, plan_token="dns")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key='provider_dns_transient_outcomes_v1'"
        )
        manifest.connection.execute(
            """
            UPDATE tasks SET status='failed',attempts=2,
              provider_outcomes_json='{"fmp":"empty","yfinance":"permanent"}',
              error='fmp: empty | yfinance: DNSError: Failed to perform, curl: (6) Could not resolve host: query2.finance.yahoo.com'
            WHERE endpoint='equity.fundamental.management'
            """
        )
        manifest.connection.execute(
            """
            UPDATE tasks SET status='failed',attempts=1,
              provider_outcomes_json='{"federal_reserve":"permanent"}',
              error='federal_reserve: NameResolutionError: Max retries exceeded: Failed to resolve host'
            WHERE endpoint='fixedincome.government.yield_curve'
            """
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        rows = reopened.connection.execute(
            "SELECT endpoint,status,attempts,provider_outcomes_json FROM tasks "
            "WHERE plan_token='dns' ORDER BY endpoint"
        ).fetchall()
        assert [
            (
                row["endpoint"],
                row["status"],
                row["attempts"],
                json.loads(row["provider_outcomes_json"]),
            )
            for row in rows
        ] == [
            (
                "equity.fundamental.management",
                "pending",
                1,
                {"fmp": "empty"},
            ),
            ("fixedincome.government.yield_curve", "pending", 0, {}),
        ]
        assert reopened.meta_value("provider_dns_transient_outcomes_v1") == "2"
    finally:
        reopened.close()


def test_manifest_migrates_sec_nport_no_records_to_empty(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.nport_disclosure",
        "ASEC/year=2023/quarter=4",
        {"symbol": "ASEC", "year": 2023, "quarter": 4},
        ("sec", "fmp"),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([task], plan_token="sec-nport-empty")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key='sec_nport_no_records_empty_v1'"
        )
        manifest.connection.execute(
            """
            UPDATE tasks SET status='pending',attempts=0,
              provider_outcomes_json='{"sec":"permanent"}',
              error='sec: OpenBBError: No N-Port records found for ASEC. | fmp: skipped (cooldown)'
            WHERE task_id=?
            """,
            (task.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        row = reopened.connection.execute(
            "SELECT status,attempts,provider_outcomes_json,created_at,updated_at "
            "FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert row["status"] == "pending"
        assert row["attempts"] == 0
        assert json.loads(row["provider_outcomes_json"]) == {"sec": "empty"}
        assert row["created_at"] == row["updated_at"]
        assert reopened.meta_value("sec_nport_no_records_empty_v1") == "1"
    finally:
        reopened.close()


def test_manifest_requeues_stale_sec_form4_cache_failures(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.ownership.insider_trading",
        "AAPL/2000-01-01_2000-12-31",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "use_cache": True,
        },
        ("sec", "fmp"),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([task], plan_token="sec-form4-cache")
        manifest.connection.execute(
            "DELETE FROM archive_meta WHERE key='sec_form4_cache_permanent_v1'"
        )
        manifest.connection.execute(
            """
            UPDATE tasks SET status='pending',attempts=2,
              provider_outcomes_json='{"sec":"permanent","fmp":"empty"}',
              error='sec: OperationalError: database is locked in sec_form4.db | fmp: empty'
            WHERE task_id=?
            """,
            (task.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    reopened = Manifest(path)
    try:
        row = reopened.connection.execute(
            "SELECT status,attempts,provider_outcomes_json,created_at,updated_at "
            "FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert row["status"] == "pending"
        assert row["attempts"] == 1
        assert json.loads(row["provider_outcomes_json"]) == {"fmp": "empty"}
        assert row["created_at"] == row["updated_at"]
        assert reopened.meta_value("sec_form4_cache_permanent_v1") == "1"
    finally:
        reopened.close()


def test_manifest_requeues_parser_shape_permanents_for_all_providers(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    sec_task = make_task(
        context,
        "equity.ownership.insider_trading",
        "NTWK/tail=2026-07-01_2026-07-18",
        {
            "symbol": "NTWK",
            "start_date": "2026-07-01",
            "end_date": "2026-07-18",
        },
        ("sec", "fmp"),
    )
    economy_task = make_task(
        context,
        "economy.fred_release_table",
        "release=1",
        {"release_id": 1},
        ("fred",),
    )
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([sec_task, economy_task], plan_token="parser-shapes")
        manifest.connection.execute(
            """
            UPDATE tasks SET status='failed',attempts=2,
              provider_outcomes_json='{"sec":"permanent","fmp":"empty"}',
              provider_evidence_json='{"sec":"sec: AttributeError: ''NoneType'' object has no attribute ''get''"}',
              error='sec parser failed'
            WHERE task_id=?
            """,
            (sec_task.task_id,),
        )
        manifest.connection.execute(
            """
            UPDATE tasks SET status='failed',attempts=1,
              provider_outcomes_json='{"fred":"permanent"}',
              provider_evidence_json='{"fred":"fred: AttributeError: ''list'' object has no attribute ''values''"}',
              error='fred parser failed'
            WHERE task_id=?
            """,
            (economy_task.task_id,),
        )
        manifest.connection.commit()

        assert (
            manifest.repair_provider_parser_shape_permanents(plan_token="parser-shapes")
            == 2
        )
        rows = manifest.connection.execute(
            """
            SELECT task_id,status,attempts,provider_outcomes_json,
                   provider_evidence_json
            FROM tasks ORDER BY task_id
            """
        ).fetchall()
        by_id = {row["task_id"]: row for row in rows}
        assert by_id[sec_task.task_id]["status"] == "pending"
        assert by_id[sec_task.task_id]["attempts"] == 1
        assert json.loads(by_id[sec_task.task_id]["provider_outcomes_json"]) == {
            "fmp": "empty"
        }
        assert json.loads(by_id[sec_task.task_id]["provider_evidence_json"]) == {}
        assert by_id[economy_task.task_id]["status"] == "pending"
        assert by_id[economy_task.task_id]["attempts"] == 0
        assert json.loads(by_id[economy_task.task_id]["provider_outcomes_json"]) == {}
        assert (
            manifest.repair_provider_parser_shape_permanents(plan_token="parser-shapes")
            == 0
        )
    finally:
        manifest.close()


def test_manifest_requeues_known_heterogeneous_schema_shards(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    bls_task = make_task(
        context,
        "economy.survey.bls_search",
        "category=ip",
        {"category": "ip"},
        ("bls",),
    )
    sec_statement = make_task(
        context,
        "equity.fundamental.balance",
        "AAPL/annual",
        {"symbol": "AAPL", "period": "annual"},
        ("sec",),
    )
    yfinance_statement = make_task(
        context,
        "equity.fundamental.balance",
        "MSFT/annual",
        {"symbol": "MSFT", "period": "annual"},
        ("yfinance",),
    )
    recent_form4 = make_task(
        context,
        "equity.ownership.insider_trading",
        "NTWK/tail",
        {"symbol": "NTWK", "_archive_sec_insider_tail": True},
        ("sec",),
    )
    old_form4 = make_task(
        context,
        "equity.ownership.insider_trading",
        "AAPL/range",
        {"symbol": "AAPL", "_archive_sec_insider_range": True},
        ("sec",),
    )
    tasks = [
        bls_task,
        sec_statement,
        yfinance_statement,
        recent_form4,
        old_form4,
    ]
    path = tmp_path / "state.sqlite3"
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks(tasks, plan_token="union-schema")
        selected_providers = {
            bls_task.task_id: "bls",
            sec_statement.task_id: "sec",
            yfinance_statement.task_id: "yfinance",
            recent_form4.task_id: "sec",
            old_form4.task_id: "sec",
        }
        for task in tasks:
            updated_at = (
                "2026-07-19T04:15:00+00:00"
                if task is recent_form4
                else "2026-07-19T04:00:00+00:00"
            )
            manifest.connection.execute(
                """
                UPDATE tasks SET status='success',rows=1,
                    selected_provider=?,updated_at=?
                WHERE task_id=?
                """,
                (selected_providers[task.task_id], updated_at, task.task_id),
            )
        manifest.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
            """,
            (
                "provider_parser_shape_recovery_revision:union-schema",
                "1",
                "2026-07-19T04:14:00+00:00",
            ),
        )
        manifest.connection.commit()

        assert (
            manifest.repair_heterogeneous_parquet_schema_shards(
                plan_token="union-schema"
            )
            == 3
        )
        statuses = {
            row["task_id"]: row["status"]
            for row in manifest.connection.execute(
                "SELECT task_id,status FROM tasks"
            ).fetchall()
        }
        assert statuses[bls_task.task_id] == "pending"
        assert statuses[sec_statement.task_id] == "pending"
        assert statuses[recent_form4.task_id] == "pending"
        assert statuses[yfinance_statement.task_id] == "success"
        assert statuses[old_form4.task_id] == "success"
        assert (
            manifest.repair_heterogeneous_parquet_schema_shards(
                plan_token="union-schema"
            )
            == 0
        )
    finally:
        manifest.close()


def test_verified_existing_plan_resume_is_fail_closed(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
        },
        ("yfinance",),
    )
    plan_token = "verified-plan"
    catalog = tmp_path / "catalog"
    catalog.mkdir(parents=True)
    coverage = CoverageDecision(
        endpoint="equity.price.historical",
        category="equity",
        available_providers="yfinance",
        selected_providers="yfinance",
        decision="included",
        reason="",
        initial_task_count=1,
    )
    pq.write_table(
        pa.Table.from_pylist([asdict(coverage)]),
        catalog / "coverage.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"credential_field": "fmp_api_key", "configured": True}]),
        catalog / "configured_credentials.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"status": "pass"}]),
        catalog / "completeness_contract_audit.parquet",
    )
    (catalog / "completeness_contract_summary.json").write_text(
        json.dumps(
            {
                "start_date": "2000-01-01",
                "end_date": "2026-07-18",
                "included_endpoints": 1,
                "deferred_catalog_endpoints": 0,
                "contract_rows": 1,
                "unresolved": 0,
                "passed": True,
            }
        ),
        encoding="utf-8",
    )

    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [task],
            plan_token=plan_token,
            plan_generation="generation-1",
        )
        manifest.reconcile_initial_plan(plan_token, "generation-1")
        resumed = _load_resumable_plan(
            tmp_path,
            manifest,
            plan_token=plan_token,
            start_date="2000-01-01",
            end_date="2026-07-18",
            credential_names={"fmp_api_key"},
        )
        assert resumed is not None
        assert resumed[0] == 1
        assert resumed[1] == [coverage]
        assert manifest.meta_value(f"planner_state_version:{plan_token}") == str(
            PLANNER_STATE_VERSION
        )

        assert (
            _load_resumable_plan(
                tmp_path,
                manifest,
                plan_token=plan_token,
                start_date="2000-01-01",
                end_date="2026-07-19",
                credential_names={"fmp_api_key"},
            )
            is None
        )
        assert (
            _load_resumable_plan(
                tmp_path,
                manifest,
                plan_token=plan_token,
                start_date="2000-01-01",
                end_date="2026-07-18",
                credential_names=set(),
            )
            is None
        )
    finally:
        manifest.close()


def test_manifest_pending_count_uses_terminal_evidence_not_attempt_limit(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    first = make_task(
        context, "equity.price.historical", "AAPL", {"symbol": "AAPL"}, ("yfinance",)
    )
    second = make_task(
        context, "equity.price.historical", "MSFT", {"symbol": "MSFT"}, ("yfinance",)
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([first], plan_token="first")
        manifest.upsert_tasks([second], plan_token="second")
        assert manifest.pending_count(20) == 2
        assert manifest.pending_count(20, "first") == 1
        manifest.connection.execute(
            "UPDATE tasks SET attempts=20 WHERE task_id=?", (first.task_id,)
        )
        manifest.connection.commit()
        assert manifest.pending_count(20) == 2
        assert manifest.pending_count(20, "first") == 1
        assert [task.task_id for task in manifest.pending_batch(10, 20, "first")] == [
            first.task_id
        ]
        assert manifest.finalize_exhausted_pending(20, plan_token="first") == 0
        status = manifest.connection.execute(
            "SELECT status FROM tasks WHERE task_id=?", (first.task_id,)
        ).fetchone()[0]
        assert status == "pending"
    finally:
        manifest.close()


def test_manifest_bulk_finalizes_only_provider_only_unavailable_route(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    provider_only = make_task(
        context,
        "equity.ownership.institutional",
        "AAPL/year=2025/quarter=1",
        {"symbol": "AAPL", "year": 2025, "quarter": 1},
        ("fmp",),
    )
    fallback = make_task(
        context,
        "equity.ownership.institutional",
        "MSFT/year=2025/quarter=1",
        {"symbol": "MSFT", "year": 2025, "quarter": 1},
        ("fmp", "yfinance"),
    )
    other_route = make_task(
        context,
        "equity.profile",
        "AAPL",
        {"symbol": "AAPL"},
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [provider_only, fallback, other_route], plan_token="archive"
        )
        changed = manifest.finalize_provider_only_unavailable(
            "fmp",
            "premium route",
            plan_token="archive",
            endpoint="equity.ownership.institutional",
        )
        assert changed == 1
        rows = {
            row["task_id"]: row["status"]
            for row in manifest.connection.execute("SELECT task_id,status FROM tasks")
        }
        assert rows[provider_only.task_id] == "unavailable"
        assert rows[fallback.task_id] == "pending"
        assert rows[other_route.task_id] == "pending"
    finally:
        manifest.close()


def test_manifest_requeues_empty_task_that_skipped_cooldown_provider(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "etf.nport_disclosure",
        "AAPL/year=2025/quarter=1",
        {"symbol": "AAPL", "year": 2025, "quarter": 1},
        ("sec", "fmp"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="archive")
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "empty",
                "fmp",
                0,
                None,
                1,
                error=(
                    "sec: empty | fmp: skipped "
                    "(cooldown until 2030-01-01T00:00:00+00:00)"
                ),
            )
        )
        manifest.prepare_run(
            retry_failed=True,
            retry_empty=False,
            refresh=False,
            repair_legacy=False,
            plan_token="archive",
        )
        row = manifest.connection.execute(
            "SELECT status,attempts FROM tasks WHERE task_id=?", (task.task_id,)
        ).fetchone()
        assert tuple(row) == ("empty", 1)
        manifest.prepare_run(
            retry_failed=True,
            retry_empty=False,
            refresh=False,
            repair_legacy=True,
            plan_token="archive",
        )
        row = manifest.connection.execute(
            "SELECT status,attempts FROM tasks WHERE task_id=?", (task.task_id,)
        ).fetchone()
        assert tuple(row) == ("pending", 0)
    finally:
        manifest.close()


def test_prepare_run_keeps_interrupted_correctness_repairs_first(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    repair = make_task(
        context,
        "regulators.sec.filing_headers",
        "repair-accession",
        {"url": "https://www.sec.gov/Archives/example-index.html"},
        ("sec",),
    )
    ordinary = make_task(
        context,
        "regulators.sec.filing_headers",
        "ordinary-accession",
        {"url": "https://www.sec.gov/Archives/ordinary-index.html"},
        ("sec",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([repair, ordinary], plan_token="archive")
        manifest.connection.execute(
            "UPDATE tasks SET status='running',error=? WHERE task_id=?",
            ("requeued: use canonical SEC filing index page", repair.task_id),
        )
        manifest.connection.commit()

        manifest.prepare_run(
            retry_failed=True,
            retry_empty=False,
            refresh=False,
            repair_legacy=False,
            plan_token="archive",
        )

        rows = manifest.connection.execute(
            "SELECT task_id,status,updated_at FROM tasks ORDER BY updated_at,task_id"
        ).fetchall()
        assert rows[0]["task_id"] == repair.task_id
        assert rows[0]["status"] == "pending"
        assert rows[0]["updated_at"] == "0001-01-01T00:00:00+00:00"
    finally:
        manifest.close()


def test_manifest_resets_legacy_rate_limit_attempts(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.compare.peers",
        "1529.TW",
        {"symbol": "1529.TW"},
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="archive")
        manifest.connection.execute(
            "UPDATE tasks SET attempts=4,error=? WHERE task_id=?",
            (
                "fmp: UnauthorizedError: 429 -> Limit Reach. Please upgrade",
                task.task_id,
            ),
        )
        manifest.connection.commit()
        manifest.prepare_run(
            retry_failed=True,
            retry_empty=False,
            refresh=False,
            plan_token="archive",
        )
        row = manifest.connection.execute(
            "SELECT status,attempts FROM tasks WHERE task_id=?", (task.task_id,)
        ).fetchone()
        assert tuple(row) == ("pending", 0)
    finally:
        manifest.close()


def test_manifest_requeues_bls_empty_created_by_daily_quota(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.survey.bls_series",
        "category=bed/batch=00001/n=50",
        {"symbol": "SERIES1,SERIES2"},
        ("bls",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="archive")
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "empty",
                "bls",
                0,
                None,
                1,
                error=(
                    "bls: EmptyDataError: No data found: the daily threshold "
                    "for total number of requests has been reached."
                ),
            )
        )
        manifest.prepare_run(
            retry_failed=True,
            retry_empty=False,
            refresh=False,
            plan_token="archive",
        )
        row = manifest.connection.execute(
            "SELECT status,attempts FROM tasks WHERE task_id=?", (task.task_id,)
        ).fetchone()
        assert tuple(row) == ("pending", 0)
    finally:
        manifest.close()


def test_manifest_provider_route_change_resets_unfinished_attempt_budget(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    old = make_task(
        context,
        "news.world",
        "year=2024/page=0",
        {"start_date": "2024-01-01", "end_date": "2024-12-31"},
        ("benzinga", "tiingo"),
    )
    new = replace(old, providers=("fmp", "tiingo"))
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([old], plan_token="archive")
        manifest.connection.execute(
            """
            UPDATE tasks SET status='failed',selected_provider='tiingo',
                             attempts=20,error='old route failed'
            WHERE task_id=?
            """,
            (old.task_id,),
        )
        manifest.connection.commit()
        manifest.upsert_tasks([new], plan_token="archive")
        row = manifest.connection.execute(
            """
            SELECT status,selected_provider,attempts,error,providers_json
            FROM tasks WHERE task_id=?
            """,
            (old.task_id,),
        ).fetchone()
        assert tuple(row[:4]) == ("pending", None, 0, None)
        assert json.loads(row[4]) == ["fmp", "tiingo"]
    finally:
        manifest.close()


def test_manifest_provider_contract_change_revalidates_only_affected_acceptance(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    old = make_task(
        context,
        "fixedincome.government.yield_curve",
        "date=2024-01-02/type=real",
        {
            "date": "2024-01-02",
            "country": "united_states",
            "yield_curve_type": "real",
        },
        ("federal_reserve", "fred"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([old], plan_token="archive")
        manifest.connection.execute(
            "UPDATE tasks SET status='success',selected_provider='federal_reserve',"
            "attempts=1,rows=10 WHERE task_id=?",
            (old.task_id,),
        )
        manifest.connection.commit()

        # The provider that produced the old success cannot implement `real`;
        # removing it invalidates that success and clears its proof state.
        manifest.upsert_tasks([replace(old, providers=("fred",))], plan_token="archive")
        row = manifest.connection.execute(
            "SELECT status,selected_provider,attempts,rows,provider_outcomes_json "
            "FROM tasks WHERE task_id=?",
            (old.task_id,),
        ).fetchone()
        assert tuple(row[:4]) == ("pending", None, 0, 0)
        assert json.loads(row[4]) == {}

        # An empty result is still valid when providers are merely removed,
        # but adding a previously unqueried provider requires another attempt.
        manifest.connection.execute(
            "UPDATE tasks SET status='empty',selected_provider='fred',attempts=1 "
            "WHERE task_id=?",
            (old.task_id,),
        )
        manifest.connection.commit()
        manifest.upsert_tasks(
            [replace(old, providers=("fred", "econdb"))], plan_token="archive"
        )
        row = manifest.connection.execute(
            "SELECT status,selected_provider,attempts FROM tasks WHERE task_id=?",
            (old.task_id,),
        ).fetchone()
        assert tuple(row) == ("pending", None, 0)
    finally:
        manifest.close()


def test_manifest_reconciles_obsolete_initial_tasks_but_preserves_followups(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    current = make_task(context, "equity.profile", "AAPL", {"symbol": "AAPL"}, ("fmp",))
    obsolete = make_task(context, "equity.profile", "OLD", {"symbol": "OLD"}, ("fmp",))
    followup = make_task(
        context,
        "economy.survey.bls_series",
        "catalog/batch=00000/n=2",
        {"symbol": "A,B"},
        ("bls",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([obsolete], plan_token="archive", plan_generation="old")
        manifest.upsert_tasks([followup], plan_token="archive", task_source="followup")
        manifest.upsert_tasks([current], plan_token="archive", plan_generation="new")
        assert manifest.reconcile_initial_plan("archive", "new") == 1
        active = manifest.connection.execute(
            "SELECT scope_key,task_source FROM tasks WHERE active=1 ORDER BY scope_key"
        ).fetchall()
        assert [(row[0], row[1]) for row in active] == [
            ("AAPL", "initial"),
            ("catalog/batch=00000/n=2", "followup"),
        ]
        assert manifest.counts("archive") == {"pending": 2}
    finally:
        manifest.close()


def test_manifest_plan_transition_adopts_compatible_followups_and_retires_old_plan(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    compatible_followup = make_task(
        context,
        "economy.fred_series",
        "series=GDP",
        {"symbol": "GDP", "start_date": "2000-01-01"},
        ("fred",),
    )
    removed_followup = make_task(
        context,
        "news.company",
        "OLD/year=2000",
        {"symbol": "OLD", "start_date": "2000-01-01"},
        ("fmp",),
    )
    obsolete_initial = make_task(
        context,
        "equity.ownership.institutional",
        "OLD/year=2000/quarter=1",
        {"symbol": "OLD", "year": 2000, "quarter": 1},
        ("fmp",),
    )
    current_initial = make_task(
        context,
        "equity.profile",
        "AAPL",
        {"symbol": "AAPL"},
        ("fmp",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [compatible_followup, removed_followup],
            plan_token="old-plan",
            task_source="followup",
        )
        manifest.upsert_tasks(
            [obsolete_initial],
            plan_token="old-plan",
            plan_generation="old",
        )
        manifest.upsert_tasks(
            [current_initial],
            plan_token="new-plan",
            plan_generation="new",
        )
        migrated, retired = manifest.reconcile_active_plan_membership(
            "new-plan",
            compatible_plan_tokens={"old-plan"},
            followup_endpoints={"economy.fred_series"},
        )
        assert (migrated, retired) == (1, 2)
        active = manifest.connection.execute(
            "SELECT scope_key,plan_token,task_source FROM tasks "
            "WHERE active=1 ORDER BY scope_key"
        ).fetchall()
        assert [tuple(row) for row in active] == [
            ("AAPL", "new-plan", "initial"),
            ("series=GDP", "new-plan", "followup"),
        ]
        assert (
            manifest.connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE active=1 AND plan_token!='new-plan'"
            ).fetchone()[0]
            == 0
        )
    finally:
        manifest.close()


def test_manifest_prunes_disabled_providers_from_existing_followups(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    mixed = make_task(
        context,
        "economy.fred_series",
        "MIXED",
        {"symbol": "MIXED"},
        ("fred", "intrinio"),
    )
    unavailable = make_task(
        context,
        "economy.fred_series",
        "ONLY",
        {"symbol": "ONLY"},
        ("intrinio",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [mixed, unavailable],
            plan_token="archive",
            task_source="followup",
        )
        assert manifest.prune_disabled_providers("archive", {"intrinio"}) == (2, 1)
        rows = manifest.connection.execute(
            "SELECT scope_key,providers_json,active FROM tasks ORDER BY scope_key"
        ).fetchall()
        assert [(row[0], json.loads(row[1]), row[2]) for row in rows] == [
            ("MIXED", ["fred"], 1),
            ("ONLY", [], 0),
        ]
    finally:
        manifest.close()


def test_manifest_deactivates_only_legacy_cftc_followups(tmp_path: Path) -> None:
    context = _context(tmp_path)
    legacy = make_task(
        context,
        "cftc.cot",
        "code=001234",
        {"code": "001234", "start_date": context.start_date},
        ("cftc",),
    )
    complete = make_task(
        context,
        "cftc.cot",
        "report=financial/mode=futures/code=001234",
        {
            "code": "001234",
            "start_date": context.start_date,
            "end_date": context.end_date,
            "report_type": "financial",
            "futures_only": True,
        },
        ("cftc",),
    )
    unrelated = make_task(
        context,
        "economy.fred_series",
        "GDP",
        {"symbol": "GDP"},
        ("fred",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [legacy, complete, unrelated],
            plan_token="archive",
            task_source="followup",
        )
        assert manifest.deactivate_legacy_cftc_followups("archive") == 1
        rows = manifest.connection.execute(
            "SELECT scope_key,active FROM tasks ORDER BY scope_key"
        ).fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("GDP", 1),
            ("code=001234", 0),
            ("report=financial/mode=futures/code=001234", 1),
        ]
    finally:
        manifest.close()


def test_manifest_backfills_full_fred_release_search_page(tmp_path: Path) -> None:
    context = _context(tmp_path)
    full_page = make_task(
        context,
        "economy.fred_search",
        "release=148",
        {
            "query": "",
            "release_id": 148,
            "search_type": "release",
            "limit": FRED_RELEASE_PAGE_SIZE,
        },
        ("fred",),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks([full_page], plan_token="archive", task_source="followup")
        manifest.connection.execute(
            "UPDATE tasks SET status='success',rows=? WHERE task_id=?",
            (FRED_RELEASE_PAGE_SIZE, full_page.task_id),
        )
        manifest.connection.commit()

        assert manifest.ensure_fred_release_continuations(context, "archive") == 1
        rows = manifest.connection.execute(
            "SELECT scope_key,status,kwargs_json,task_source FROM tasks ORDER BY scope_key"
        ).fetchall()
        assert len(rows) == 2
        assert rows[1][0] == "release=148/offset=0001000"
        assert rows[1][1] == "pending"
        assert json.loads(rows[1][2])["offset"] == FRED_RELEASE_PAGE_SIZE
        assert rows[1][3] == "followup"
        assert manifest.ensure_fred_release_continuations(context, "archive") == 0
    finally:
        manifest.close()


def test_manifest_backfills_missing_fred_series_from_successful_catalog(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands[".economy.fred_series"] = ["fred"]
    release_page = make_task(
        context,
        "economy.fred_search",
        "release=453",
        {
            "query": "",
            "release_id": 453,
            "search_type": "release",
            "limit": FRED_RELEASE_PAGE_SIZE,
        },
        ("fred",),
    )
    existing = make_task(
        context,
        "economy.fred_series",
        "SERIES_A",
        {
            "symbol": "SERIES_A",
            "start_date": context.start_date,
            "end_date": context.end_date,
            "limit": 100000,
        },
        ("fred",),
    )
    release_path = Path(release_page.output_path)
    release_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"series_id": ["SERIES_A", "SERIES_B", "SERIES_B", None]}),
        release_path,
    )

    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [release_page, existing],
            plan_token="archive",
            task_source="followup",
        )
        manifest.connection.execute(
            "UPDATE tasks SET status='success',rows=4 WHERE task_id=?",
            (release_page.task_id,),
        )
        manifest.connection.commit()

        assert manifest.ensure_fred_series_followups(context, "archive") == 1
        assert manifest.ensure_fred_series_followups(context, "archive") == 0
        rows = manifest.connection.execute(
            """
            SELECT scope_key,status,task_source
            FROM tasks
            WHERE endpoint='economy.fred_series'
            ORDER BY scope_key
            """
        ).fetchall()
        assert [(row[0], row[1], row[2]) for row in rows] == [
            ("SERIES_A", "pending", "followup"),
            ("SERIES_B", "pending", "followup"),
        ]
    finally:
        manifest.close()


def test_manifest_retires_legacy_invalid_index_constituent_tasks(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    invalid = make_task(
        context,
        "index.constituents",
        "^GSPC",
        {"symbol": "^GSPC", "historical": True},
        ("fmp",),
    )
    valid = make_task(
        context,
        "index.constituents",
        "sp500",
        {"symbol": "sp500", "historical": True},
        ("fmp",),
    )
    manifest_path = tmp_path / "state.sqlite3"
    manifest = Manifest(manifest_path)
    manifest.upsert_tasks(
        [invalid, valid], plan_token="archive", task_source="followup"
    )
    manifest.close()

    reopened = Manifest(manifest_path)
    try:
        rows = reopened.connection.execute(
            "SELECT scope_key,active,task_source FROM tasks ORDER BY scope_key"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("^GSPC", 0, "followup"),
            ("sp500", 1, "initial"),
        ]
    finally:
        reopened.close()


def test_manifest_fair_batch_includes_multiple_endpoints(tmp_path: Path) -> None:
    context = _context(tmp_path)
    price_tasks = [
        make_task(
            context,
            "equity.price.historical",
            f"P{i}",
            {"symbol": f"P{i}"},
            ("yfinance",),
        )
        for i in range(8)
    ]
    profile_tasks = [
        make_task(context, "equity.profile", f"C{i}", {"symbol": f"C{i}"}, ("fmp",))
        for i in range(2)
    ]
    mixed_provider_task = make_task(
        context,
        "equity.profile",
        "MIXED",
        {"symbol": "MIXED"},
        ("fmp", "yfinance"),
    )
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(
            [*price_tasks, *profile_tasks, mixed_provider_task], plan_token="fair"
        )
        batch = manifest.pending_batch(4, 20, "fair")
        assert len(batch) == 4
        assert {task.endpoint for task in batch} == {
            "equity.price.historical",
            "equity.profile",
        }
        assert batch[0].endpoint != batch[1].endpoint
        non_fmp_batch = manifest.pending_batch(
            4,
            20,
            "fair",
            excluded_providers={"fmp"},
        )
        assert len(non_fmp_batch) == 4
        assert all("yfinance" in task.providers for task in non_fmp_batch)
        assert mixed_provider_task.task_id in {task.task_id for task in non_fmp_batch}
        no_profile_batch = manifest.pending_batch(
            4,
            20,
            "fair",
            excluded_endpoints={"equity.profile"},
        )
        assert len(no_profile_batch) == 4
        assert {task.endpoint for task in no_profile_batch} == {
            "equity.price.historical"
        }
    finally:
        manifest.close()


def test_manifest_fair_batch_rotates_after_visiting_every_endpoint(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    endpoints = (
        "equity.profile",
        "equity.ownership.institutional",
        "equity.fundamental.ratios",
        "equity.estimates.historical",
    )
    tasks = [
        make_task(
            context,
            endpoint,
            f"{endpoint}-{index}",
            {"symbol": f"S{index}"},
            ("fmp",),
        )
        for endpoint in endpoints
        for index in range(2)
    ]
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="fair")
        first = manifest.pending_batch(8, 20, "fair")
        second = manifest.pending_batch(8, 20, "fair")
        assert len(first) == len(second) == 8
        assert first[0].endpoint != second[0].endpoint
        assert {task.endpoint for task in first} == set(endpoints)
        assert {task.endpoint for task in second} == set(endpoints)
    finally:
        manifest.close()


def test_manifest_fair_batch_rotates_retryable_pending_task_to_back(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    tasks = [
        make_task(
            context,
            "equity.price.historical",
            f"S{i}",
            {"symbol": f"S{i}"},
            ("yfinance",),
        )
        for i in range(3)
    ]
    manifest = Manifest(tmp_path / "state.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="rotate")
        first = manifest.pending_batch(1, 20, "rotate")[0]
        manifest.claim([first])
        manifest.complete(
            TaskResult(
                first,
                "pending",
                "yfinance",
                0,
                None,
                0,
                error="provider cooldown",
            )
        )

        second = manifest.pending_batch(1, 20, "rotate")[0]

        assert second.task_id != first.task_id
    finally:
        manifest.close()


def test_worker_falls_back_writes_parquet_and_drops_document_body(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context, "equity.price.historical", "AAPL", {"symbol": "AAPL"}, ("bad", "good")
    )

    class _Result:
        results = [{"date": "2000-01-03", "close": 10.0, "content": "do not persist"}]

    class _Price:
        @staticmethod
        def historical(*, provider: str, **kwargs):
            if provider == "bad":
                raise RuntimeError("401 unauthorized")
            return _Result()

    class _Equity:
        price = _Price()

    class _Obb:
        equity = _Equity()

    runtime = _runtime({"bad": 1000, "good": 1000}, {"bad": 1, "good": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "success"
    assert result.provider == "good"
    table = pq.read_table(task.output_path)
    assert table.num_rows == 1
    assert "content" not in table.column_names
    assert table.column("_provider")[0].as_py() == "good"


def test_worker_persists_provider_outcomes_across_fallback_cooldown(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.profile",
        "AAPL",
        {"symbol": "AAPL"},
        ("sec", "fmp"),
    )
    calls: list[str] = []

    class _Result:
        def __init__(self, records):
            self.results = records

    class _Equity:
        @staticmethod
        def profile(*, provider: str, **kwargs):
            calls.append(provider)
            return _Result([] if provider == "sec" else [{"symbol": "AAPL"}])

    class _Obb:
        equity = _Equity()

    runtime = _runtime(
        {"sec": 1000, "fmp": 1000},
        {"sec": 1, "fmp": 1},
    )
    runtime._blocked_until["fmp"] = time.time() + 60
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    first = worker(task)
    assert first.status == "pending"
    assert first.attempts == 1
    assert first.provider_outcomes == {"sec": "empty"}

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="partial-provider")
        manifest.complete(first)
        assert (
            manifest.pending_batch(
                1,
                20,
                "partial-provider",
                excluded_providers={"fmp"},
            )
            == []
        )
        resumed = manifest.pending_batch(1, 20, "partial-provider")[0]
        assert resumed.provider_outcomes == {"sec": "empty"}

        runtime._blocked_until.pop("fmp")
        second = worker(resumed)
        assert second.status == "success"
        assert second.provider == "fmp"
        assert calls == ["sec", "fmp"]
    finally:
        manifest.close()


def test_fmp_route_restriction_does_not_disable_provider_globally(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    restricted = make_task(
        context, "equity.profile", "restricted", {"symbol": "BAD"}, ("fmp",)
    )
    available = make_task(
        context, "equity.compare.peers", "available", {"symbol": "GOOD"}, ("fmp",)
    )

    class _Result:
        results = [{"symbol": "GOOD", "name": "Available"}]

    class _Equity:
        class compare:
            @staticmethod
            def peers(**kwargs):
                return _Result()

        @staticmethod
        def profile(*, symbol: str, **kwargs):
            if symbol == "BAD":
                raise RuntimeError(
                    "402 Restricted Endpoint: not available under your current subscription"
                )
            return _Result()

    class _Obb:
        equity = _Equity()

    runtime = _runtime({"fmp": 1000}, {"fmp": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    assert worker(restricted).status == "unavailable"
    assert "fmp" not in runtime.unavailable()
    assert ("fmp", "equity.profile") in runtime.unavailable_routes()
    assert worker(available).status == "success"


def test_fmp_parameter_restriction_does_not_disable_endpoint(tmp_path: Path) -> None:
    context = _context(tmp_path)
    old_scope = make_task(
        context,
        "equity.ownership.major_holders",
        "A/year=2000/quarter=1/page=0",
        {"symbol": "A", "year": 2000, "quarter": 1, "page": 0},
        ("fmp",),
    )
    recent_scope = make_task(
        context,
        "equity.ownership.major_holders",
        "A/year=2026/quarter=1/page=0",
        {"symbol": "A", "year": 2026, "quarter": 1, "page": 0},
        ("fmp",),
    )

    class _Result:
        results = [{"symbol": "A", "date": "2026-03-31"}]

    class UnauthorizedError(RuntimeError):
        pass

    class _Ownership:
        @staticmethod
        def major_holders(*, year: int, **kwargs):
            if year == 2000:
                raise UnauthorizedError(
                    "402 Premium Query Parameter: Special Endpoint: "
                    "This value set for 'from' is not available under your "
                    "current subscription"
                )
            return _Result()

    class _Equity:
        ownership = _Ownership()

    class _Obb:
        equity = _Equity()

    runtime = _runtime({"fmp": 1000}, {"fmp": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )

    restricted = worker(old_scope)
    assert restricted.status == "unavailable"
    assert restricted.provider_outcomes == {"fmp": "unavailable"}
    assert "402 Premium Query Parameter" in restricted.provider_evidence["fmp"]
    assert runtime.unavailable_routes() == {}
    assert worker(recent_scope).status == "success"


def test_missing_sec_cik_does_not_disable_later_sec_tasks(tmp_path: Path) -> None:
    context = _context(tmp_path)
    missing = make_task(
        context,
        "regulators.sec.cik_map",
        "4401.TWO",
        {"symbol": "4401.TWO"},
        ("sec",),
    )
    available = make_task(
        context,
        "regulators.sec.cik_map",
        "AAPL",
        {"symbol": "AAPL"},
        ("sec",),
    )

    class _Result:
        results = [{"symbol": "AAPL", "cik": "0000320193"}]

    class _Sec:
        @staticmethod
        def cik_map(*, symbol: str, **kwargs):
            if symbol == "4401.TWO":
                raise RuntimeError("Could not find CIK for symbol: 4401.TWO")
            return _Result()

    class _Regulators:
        sec = _Sec()

    class _Obb:
        regulators = _Regulators()

    runtime = _runtime({"sec": 1000}, {"sec": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    assert worker(missing).status == "empty"
    assert "sec" not in runtime.unavailable()
    assert worker(available).status == "success"


def test_sec_insider_trading_uses_cache_with_single_provider_slot(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.ownership.insider_trading",
        "AAPL",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2025-12-31",
            "use_cache": True,
        },
        ("sec",),
    )
    observed: dict[str, object] = {}

    class _Result:
        results = [{"symbol": "AAPL", "filing_date": "2025-01-02"}]

    class _Ownership:
        @staticmethod
        def insider_trading(**kwargs):
            observed.update(kwargs)
            return _Result()

    class _Equity:
        ownership = _Ownership()

    class _Obb:
        equity = _Equity()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"sec": 1000}, {"sec": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "success"
    assert observed["provider"] == "sec"
    assert observed["use_cache"] is True


def test_tiingo_historical_fallback_uses_entitled_2020_start(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "currency.price.historical",
        "AEDJOD",
        {
            "symbol": "AEDJOD",
            "start_date": "2000-01-01",
            "end_date": "2025-12-31",
            "interval": "1d",
        },
        ("tiingo",),
    )
    observed: dict[str, object] = {}

    class _Result:
        results = [{"date": "2020-01-02", "close": 1.0}]

    class _Price:
        @staticmethod
        def historical(**kwargs):
            observed.update(kwargs)
            return _Result()

    class _Currency:
        price = _Price()

    class _Obb:
        currency = _Currency()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"tiingo": 1000}, {"tiingo": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "success"
    assert observed["start_date"] == "2020-01-01"
    assert task.kwargs["start_date"] == "2000-01-01"


def test_worker_defers_cooldown_without_consuming_attempt(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.pce",
        "date=2025-01-01",
        {"date": "2025-01-01"},
        ("fred",),
    )

    class _Obb:
        pass

    runtime = _runtime({"fred": 1000}, {"fred": 1}, 60)
    runtime._blocked_until["fred"] = time.time() + 60
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "pending"
    assert result.attempts == 0
    assert "cooldown until" in (result.error or "")


def test_worker_does_not_accept_empty_before_cooldown_provider_is_tried(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.pce",
        "date=2025-01-01",
        {"date": "2025-01-01"},
        ("fred", "fmp"),
    )

    class _Result:
        results: list[dict[str, object]] = []

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            return _Result()

    class _Obb:
        economy = _Economy()

    runtime = _runtime({"fred": 1000, "fmp": 1000}, {"fred": 1, "fmp": 1}, 60)
    runtime._blocked_until["fmp"] = time.time() + 60
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "pending"
    assert result.attempts == 1
    assert result.provider_outcomes == {"fred": "empty"}
    assert "fred: empty" in (result.error or "")
    assert "fmp: skipped (cooldown until" in (result.error or "")


def test_tiingo_news_permission_failure_disables_only_news_route(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "news.world",
        "year=2025/page=0",
        {"start_date": "2025-01-01", "end_date": "2025-12-31"},
        ("tiingo",),
    )

    class _News:
        @staticmethod
        def world(**kwargs):
            raise RuntimeError(
                "Unauthorized Tiingo request: You do not have permission "
                "to access the News API"
            )

    class _Obb:
        news = _News()

    runtime = _runtime({"tiingo": 1000}, {"tiingo": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "unavailable"
    assert runtime.availability("tiingo")[0] is True
    available, reason = runtime.availability("tiingo", "news.world")
    assert available is False
    assert "News API" in str(reason)


def test_worker_defers_transient_failure_and_counts_attempt(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context, "economy.pce", "date=2025-01-01", {"date": "2025-01-01"}, ("fred",)
    )

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            raise TimeoutError("temporary timeout")

    class _Obb:
        economy = _Economy()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"fred": 1000}, {"fred": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "pending"
    assert result.attempts == 1
    assert result.transient_failures == 1
    assert result.retry_not_before is not None


def test_worker_http_500_uses_task_backoff_without_immediate_or_global_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "uscongress.bill_info",
        "118/hr/1",
        {"congress": 118, "bill_type": "hr", "bill_number": 1},
        ("congress_gov",),
    )
    calls = 0

    def fail_congress(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("HTTP Error 500: Internal Server Error")

    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_congress_info_workaround",
        fail_congress,
    )

    class _Obb:
        pass

    runtime = _runtime({"congress_gov": 1000}, {"congress_gov": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=3,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)

    assert calls == 1
    assert result.status == "pending"
    assert result.attempts == 1
    assert result.transient_failures == 1
    assert result.retry_not_before is not None
    assert runtime.cooldown_providers() == set()
    delay = datetime.fromisoformat(result.retry_not_before) - datetime.now(timezone.utc)
    assert TASK_RETRY_BASE_SECONDS * 0.79 < delay.total_seconds()
    assert delay.total_seconds() <= TASK_RETRY_BASE_SECONDS


def test_task_retry_delay_is_exponential_bounded_and_deterministic() -> None:
    delays = [_task_retry_delay_seconds("task-a", streak) for streak in range(1, 20)]

    assert delays[0] == _task_retry_delay_seconds("task-a", 1)
    assert delays[0] >= TASK_RETRY_BASE_SECONDS * 0.8
    assert all(delay <= TASK_RETRY_MAX_SECONDS for delay in delays)
    assert delays[-1] >= TASK_RETRY_MAX_SECONDS * 0.8


def test_worker_rate_limit_does_not_exhaust_task_attempt_budget(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.pce",
        "date=2025-01-01",
        {"date": "2025-01-01"},
        ("fred",),
    )

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            raise RuntimeError("HTTP 429 too many requests")

    class _Obb:
        economy = _Economy()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"fred": 1000}, {"fred": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "pending"
    assert result.attempts == 0


def test_worker_deferred_child_does_not_extend_provider_cooldown(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.pce",
        "date=2025-01-01",
        {"date": "2025-01-01"},
        ("fred",),
    )

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            raise RuntimeError(
                "__archive_provider_deferred__: fred cooldown already active"
            )

    class _Obb:
        economy = _Economy()

    runtime = _runtime({"fred": 1000}, {"fred": 1})
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )

    result = worker(task)

    assert result.status == "pending"
    assert result.attempts == 0
    assert runtime.availability("fred") == (True, None)


def test_worker_rechecks_cooldown_after_limiter_wait(tmp_path: Path) -> None:
    import downloader.download_openbb_archive as archive

    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.pce",
        "date=2025-01-01",
        {"date": "2025-01-01"},
        ("government_us",),
    )
    network_called = False

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            nonlocal network_called
            archive._wait_provider_http_boundary(
                "government_us", "https://api.data.gov/example"
            )
            network_called = True
            raise AssertionError("network request must not start during cooldown")

    class _Obb:
        economy = _Economy()

    runtime = _runtime({"government_us": 1000}, {"government_us": 1}, 60)

    class _BlockingLimiter:
        def wait(self):
            runtime.block("government_us", 60)

        def wait_at_http_boundary(self):
            self.wait()

    runtime._limiters["government_us"] = _BlockingLimiter()
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert network_called is False
    assert result.status == "pending"
    assert result.attempts == 0
    assert "cooldown until" in (result.error or "")


def test_worker_waits_without_task_churn_when_provider_capacity_is_busy(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context, "economy.pce", "date=2025-01-01", {"date": "2025-01-01"}, ("fred",)
    )
    called = False

    class _Economy:
        @staticmethod
        def pce(**kwargs):
            nonlocal called
            called = True
            return type("Result", (), {"results": []})()

    class _Obb:
        economy = _Economy()

    runtime = _runtime({"fred": 1000}, {"fred": 1}, 60)
    semaphore = runtime.semaphore("fred")
    assert semaphore.acquire(blocking=False)
    results: list[TaskResult] = []
    worker = OpenBBWorker(
        _Obb(),
        runtime,
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    thread = threading.Thread(target=lambda: results.append(worker(task)))
    try:
        thread.start()
        time.sleep(0.02)
        assert thread.is_alive()
        assert called is False
    finally:
        semaphore.release()
        thread.join(timeout=1.0)

    assert thread.is_alive() is False
    assert called is True
    assert len(results) == 1
    assert results[0].status == "empty"
    assert results[0].attempts == 1


def test_out_of_requested_date_range_is_authoritative_empty(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context, "economy.direction_of_trade", "former-country", {}, ("imf",)
    )

    class _Economy:
        @staticmethod
        def direction_of_trade(**kwargs):
            raise RuntimeError(
                "Requested start_date is after the latest available data"
            )

    class _Obb:
        economy = _Economy()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"imf": 1000}, {"imf": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "empty"
    assert result.attempts == 1


def test_worker_normalizes_statement_kwargs_for_provider(tmp_path: Path) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.balance",
        "AAPL/period=quarter",
        {"symbol": "AAPL", "period": "quarter", "limit": 1000},
        ("sec",),
    )
    captured = {}

    class _Result:
        results = [{"date": "2025-03-31", "assets": 1}]

    class _Fundamental:
        @staticmethod
        def balance(**kwargs):
            captured.update(kwargs)
            return _Result()

    class _Equity:
        fundamental = _Fundamental()

    class _Obb:
        equity = _Equity()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"sec": 1000}, {"sec": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "success"
    assert captured["period"] == "quarterly"


def test_worker_requests_all_sec_filings_without_changing_fallback_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.filings",
        "AAPL/all/page=0",
        {
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "limit": 1000,
            "page": 0,
            "use_cache": True,
        },
        ("sec",),
    )
    captured = {}

    class _Result:
        results = [
            {
                "filing_date": "2000-10-30",
                "report_type": "10-K",
                "report_url": "https://www.sec.gov/example.htm",
            }
        ]

    class _Fundamental:
        @staticmethod
        def filings(**kwargs):
            captured.update(kwargs)
            return _Result()

    class _Equity:
        fundamental = _Fundamental()

    class _Obb:
        equity = _Equity()

    def _filings_workaround(kwargs, **options):
        captured.update(kwargs)
        return _Result.results

    monkeypatch.setattr(
        "downloader.download_openbb_archive._fetch_sec_filings_workaround",
        _filings_workaround,
    )

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"sec": 1000}, {"sec": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )

    result = worker(task)

    assert result.status == "success"
    assert captured["limit"] == 0
    assert task.kwargs["limit"] == 1000


def test_worker_treats_fred_empty_release_table_parser_bug_as_empty(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "economy.fred_release_table",
        "release=738",
        {"release_id": "738"},
        ("fred",),
    )

    class _FredReleaseTable:
        @staticmethod
        def __call__(**kwargs):
            raise RuntimeError("'list' object has no attribute 'values'")

    class _Economy:
        fred_release_table = _FredReleaseTable()

    class _Obb:
        economy = _Economy()

    worker = OpenBBWorker(
        _Obb(),
        _runtime({"fred": 1000}, {"fred": 1}),
        max_retries=1,
        base_backoff=0,
        max_backoff=1,
        metadata_only=True,
    )
    result = worker(task)
    assert result.status == "empty"
    assert result.attempts == 1
    assert result.error == "fred: empty release table"


def test_normalize_records_serializes_nested_values() -> None:
    class _Result:
        results = [{"a": {"nested": 1}, "body": "drop", "value": 2}]

    records = normalize_records(_Result(), metadata_only=True)
    assert records == [{"a": '{"nested":1}', "value": 2}]


def test_normalize_records_preserves_single_model_fields() -> None:
    class _Model:
        def model_dump(self, *, mode: str):
            assert mode == "json"
            return {"cik": "0000320193"}

        def __iter__(self):
            yield ("cik", "0000320193")

    class _Result:
        results = _Model()

    assert normalize_records(_Result(), metadata_only=True) == [{"cik": "0000320193"}]


def test_bls_search_normalization_preserves_series_and_complete_code_map() -> None:
    result = SimpleNamespace(
        results=[{"symbol": "TU001", "title": "Series", "sex_code": "Both"}],
        extra={
            "results_metadata": {
                "tu": {"sex_code": {"0": "Both", "1": "Male", "2": "Female"}}
            }
        },
    )
    records = _normalize_bls_search_result(result, metadata_only=True)
    assert records[0] == {
        "symbol": "TU001",
        "title": "Series",
        "sex_code": "Both",
        "_bls_record_type": "series",
    }
    assert records[1:] == [
        {
            "_bls_record_type": "code_map",
            "survey_name": "tu",
            "code_field": "sex_code",
            "code": code,
            "label": label,
        }
        for code, label in (("0", "Both"), ("1", "Male"), ("2", "Female"))
    ]


def test_bls_catalog_plan_requests_extra_fields_and_code_maps(tmp_path: Path) -> None:
    from typing import Literal

    class _Input:
        model_fields = {
            "category": _Field(Literal["cpi", "ppi"]),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {".economy.survey.bls_search": ["bls"]}
    context.schemas = {
        ".economy.survey.bls_search": {
            "input": _Input,
            "callable": lambda **kwargs: None,
        }
    }
    tasks, coverage = build_initial_plan(context)
    assert coverage[0].initial_task_count == 2
    assert {task.kwargs["category"] for task in tasks} == {"cpi", "ppi"}
    assert all(task.kwargs["include_extras"] for task in tasks)
    assert all(task.kwargs["include_code_map"] for task in tasks)


def test_country_literals_and_psd_commodities_are_enumerated(tmp_path: Path) -> None:
    class _CountryInput:
        model_fields = {
            "country": _Field(__import__("typing").Literal["canada", "united_states"]),
            "start_date": _Field(str),
            "end_date": _Field(str),
            "provider": _Field(str),
        }

    commodity_field = _Field(str)
    commodity_field.description = (
        "Valid commodities are: wheat, corn, rice (provider: government_us)"
    )

    class _PsdInput:
        model_fields = {
            "commodity": commodity_field,
            "country": _Field(str),
            "start_year": _Field(int),
            "end_year": _Field(int),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {
        ".economy.balance_of_payments": ["fred"],
        ".commodity.psd_data": ["government_us"],
    }
    context.schemas = {
        ".economy.balance_of_payments": {
            "input": _CountryInput,
            "callable": lambda **kwargs: None,
        },
        ".commodity.psd_data": {"input": _PsdInput, "callable": lambda **kwargs: None},
    }
    tasks, coverage = build_initial_plan(context)
    assert all(item.decision == "included" for item in coverage)
    assert [task.scope_key for task in tasks] == [
        "commodity=wheat",
        "commodity=corn",
        "commodity=rice",
        "country=canada",
        "country=united_states",
    ]
    assert all(task.kwargs.get("country") != "all" for task in tasks)
    psd_tasks = [task for task in tasks if task.endpoint == "commodity.psd_data"]
    assert all(task.kwargs["aggregate_regions"] for task in psd_tasks)


def test_country_profile_uses_econdb_universe_and_full_history(tmp_path: Path) -> None:
    class _CountryProfileInput:
        model_fields = {
            "country": _Field(str),
            "latest": _Field(bool),
            "use_cache": _Field(bool),
            "provider": _Field(str),
        }

    context = _context(tmp_path)
    context.commands = {".economy.country_profile": ["econdb"]}
    context.schemas = {
        ".economy.country_profile": {
            "input": _CountryProfileInput,
            "callable": lambda country, **kwargs: None,
        }
    }
    tasks, coverage = build_initial_plan(context)
    assert coverage[0].decision == "included"
    assert tasks
    assert any(task.scope_key == "country=uk" for task in tasks)
    assert all(task.scope_key != "country=gb" for task in tasks)
    assert all(task.kwargs["latest"] is False for task in tasks)
    assert all(task.kwargs["use_cache"] is True for task in tasks)


def test_indicator_discovery_preserves_catalog_provider_and_dimensions(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands[".economy.indicators"] = ["imf", "econdb"]
    catalog = make_task(context, "economy.available_indicators", "all", {}, ("econdb",))
    result = TaskResult(
        catalog,
        "success",
        "econdb",
        1,
        catalog.output_path,
        1,
        records=[
            {
                "symbol_root": "Y10YD",
                "symbol": "Y10YDCN",
                "country": "China",
                "frequency": "D",
            }
        ],
    )
    tasks = discover_followup_tasks(context, result)
    assert len(tasks) == 1
    assert tasks[0].providers == ("econdb",)
    assert tasks[0].kwargs == {
        "symbol": "Y10YDCN~",
        "country": None,
        "frequency": "month",
        "start_date": "2000-01-01",
        "end_date": "2000-12-31",
    }


def test_indicator_discovery_maps_yearly_econdb_frequency(tmp_path: Path) -> None:
    context = _context(tmp_path)
    context.commands[".economy.indicators"] = ["econdb"]
    catalog = make_task(context, "economy.available_indicators", "all", {}, ("econdb",))
    result = TaskResult(
        catalog,
        "success",
        "econdb",
        1,
        catalog.output_path,
        1,
        records=[
            {
                "symbol_root": "RGDPPC",
                "symbol": "RGDPPCEA",
                "iso": "EA",
                "frequency": "Y",
            }
        ],
    )
    tasks = discover_followup_tasks(context, result)
    assert tasks[0].kwargs["symbol"] == "RGDPPCEA~"
    assert tasks[0].kwargs["country"] is None
    assert tasks[0].kwargs["frequency"] == "annual"


def test_fred_release_discovery_paginates_only_full_pages(tmp_path: Path) -> None:
    context = _context(tmp_path)
    context.commands.update({".economy.fred_series": ["fred"]})
    page = make_task(
        context,
        "economy.fred_search",
        "release=148",
        {
            "query": "",
            "release_id": 148,
            "search_type": "release",
            "limit": FRED_RELEASE_PAGE_SIZE,
        },
        ("fred",),
    )
    records = [{"series_id": f"SERIES{i:04d}"} for i in range(1000)]

    tasks = discover_followup_tasks(
        context,
        TaskResult(
            page,
            "success",
            "fred",
            FRED_RELEASE_PAGE_SIZE,
            page.output_path,
            1,
            records=records,
        ),
    )

    assert len(tasks) == FRED_RELEASE_PAGE_SIZE + 1
    continuation = tasks[-1]
    assert continuation.endpoint == "economy.fred_search"
    assert continuation.scope_key == "release=148/offset=0001000"
    assert continuation.kwargs["offset"] == FRED_RELEASE_PAGE_SIZE
    assert continuation.providers == ("fred",)

    short_page = make_task(
        context,
        "economy.fred_search",
        "release=148/offset=0001000",
        dict(page.kwargs, offset=FRED_RELEASE_PAGE_SIZE),
        ("fred",),
    )
    short_tasks = discover_followup_tasks(
        context,
        TaskResult(
            short_page,
            "success",
            "fred",
            999,
            short_page.output_path,
            1,
            records=records[:999],
        ),
    )
    assert len(short_tasks) == 999
    assert all(task.endpoint == "economy.fred_series" for task in short_tasks)


def test_bls_discovery_batches_fifty_series_and_deduplicates(tmp_path: Path) -> None:
    context = _context(tmp_path)
    context.commands[".economy.survey.bls_series"] = ["bls"]
    catalog = make_task(
        context,
        "economy.survey.bls_search",
        "category=wages",
        {"query": "", "category": "wages"},
        ("bls",),
    )
    records = [{"series_id": f"SERIES{i:04d}"} for i in range(121)]
    records.append({"series_id": "series0000"})
    records.append(
        {
            "_bls_record_type": "code_map",
            "code_field": "industry_code",
            "code": "000000",
            "label": "Total Nonfarm",
        }
    )
    result = TaskResult(
        catalog,
        "success",
        "bls",
        len(records),
        catalog.output_path,
        1,
        records=records,
    )

    tasks = discover_followup_tasks(context, result)

    assert len(tasks) == 3
    assert [len(task.kwargs["symbol"].split(",")) for task in tasks] == [
        BLS_SERIES_BATCH_SIZE,
        BLS_SERIES_BATCH_SIZE,
        21,
    ]
    assert tasks[0].scope_key == "category=wages/batch=00000/n=50"
    assert tasks[-1].scope_key == "category=wages/batch=00002/n=21"
    assert tasks[0].kwargs["start_date"] == "2000-01-01"
    assert tasks[0].kwargs["end_date"] == "2000-12-31"
    assert all(task.providers == ("bls",) for task in tasks)


def test_index_constituents_plan_uses_only_supported_fmp_aliases(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands = {".index.constituents": ["fmp"]}
    context.schemas = {
        ".index.constituents": {
            "input": _InputModel,
            "callable": lambda symbol, **kwargs: None,
        }
    }
    context.indices = ["^GSPC", "^DJI", "^IXIC", "^SP500-2010"]

    tasks, coverage = build_initial_plan(context)

    assert [task.scope_key for task in tasks] == list(FMP_CONSTITUENT_INDEXES)
    assert all(task.kwargs["historical"] is True for task in tasks)
    assert coverage[0].initial_task_count == 3


def test_index_catalog_discovers_prices_but_not_invalid_constituent_symbols(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands.update(
        {
            ".index.available": ["fmp"],
            ".index.price.historical": ["yfinance", "fmp"],
            ".index.constituents": ["fmp"],
        }
    )
    catalog = make_task(context, "index.available", "all", {}, ("fmp",))
    result = TaskResult(
        catalog,
        "success",
        "fmp",
        2,
        catalog.output_path,
        1,
        records=[{"symbol": "^GSPC"}, {"symbol": "^SP500-2010"}],
    )

    tasks = discover_followup_tasks(context, result)

    assert [task.endpoint for task in tasks] == [
        "index.price.historical",
        "index.price.historical",
    ]
    assert [task.scope_key for task in tasks] == ["^GSPC", "^SP500-2010"]


def test_repair_xlsx_core_datetimes_pads_one_digit_hour() -> None:
    from io import BytesIO
    from zipfile import ZipFile

    source = BytesIO()
    with ZipFile(source, "w") as archive:
        archive.writestr(
            "docProps/core.xml",
            "<dcterms:modified>2026-05-13T 2:39:25-04:00</dcterms:modified>",
        )
        archive.writestr("xl/worksheets/sheet1.xml", "unchanged")
    repaired = _repair_xlsx_core_datetimes(source.getvalue())
    with ZipFile(BytesIO(repaired)) as archive:
        assert b"2026-05-13T02:39:25-04:00" in archive.read("docProps/core.xml")
        assert archive.read("xl/worksheets/sheet1.xml") == b"unchanged"


def test_sec_cik_map_discovers_symbol_map_from_direct_and_legacy_records(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands.update(
        {
            ".regulators.sec.cik_map": ["sec"],
            ".regulators.sec.symbol_map": ["sec"],
        }
    )
    source = make_task(
        context, "regulators.sec.cik_map", "AAPL", {"symbol": "AAPL"}, ("sec",)
    )
    for record in ({"cik": "0000320193"}, {"value": '["cik","0000320193"]'}):
        result = TaskResult(
            source,
            "success",
            "sec",
            1,
            source.output_path,
            1,
            records=[record],
        )
        tasks = discover_followup_tasks(context, result)
        assert len(tasks) == 1
        assert tasks[0].endpoint == "regulators.sec.symbol_map"
        assert tasks[0].scope_key == "0000320193"
        assert tasks[0].kwargs == {"query": "0000320193", "use_cache": True}
        assert tasks[0].providers == ("sec",)


def test_sec_symbol_map_is_discovery_only_and_sic_search_is_not_enumerated(
    tmp_path: Path,
) -> None:
    class _SecInput:
        model_fields = {"query": _Field(str), "provider": _Field(str)}

    context = _context(tmp_path)
    context.commands = {
        ".regulators.sec.symbol_map": ["sec"],
        ".regulators.sec.sic_search": ["sec"],
    }
    context.schemas = {
        endpoint: {"input": _SecInput, "callable": lambda query, **kwargs: None}
        for endpoint in context.commands
    }
    tasks, coverage = build_initial_plan(context)
    assert tasks == []
    decisions = {item.endpoint: item.decision for item in coverage}
    assert decisions == {
        "regulators.sec.sic_search": "not_enumerable",
        "regulators.sec.symbol_map": "deferred",
    }


def test_rolling_executor_refills_before_a_straggler_finishes(tmp_path: Path) -> None:
    context = _context(tmp_path)
    tasks = [
        replace(
            make_task(
                context,
                "equity.price.historical",
                scope,
                {"symbol": scope},
                ("yfinance",),
            ),
            task_id=f"{index:02d}",
        )
        for index, scope in enumerate(("01-slow", "02-fast", "03-third"), start=1)
    ]
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    third_started = threading.Event()
    release_slow = threading.Event()

    def worker(task):
        if task.scope_key == "01-slow":
            if not release_slow.wait(timeout=2):
                raise AssertionError("rolling refill did not start the third task")
        elif task.scope_key == "03-third":
            third_started.set()
            release_slow.set()
        return TaskResult(task, "empty", "yfinance", 0, None, 1)

    try:
        manifest.upsert_tasks(tasks, plan_token="rolling")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            worker,
            plan_token="rolling",
            workers=2,
            batch_size=2,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert third_started.is_set()
        assert attempted == 3
        assert totals["empty"] == 3
        assert manifest.counts("rolling") == {"empty": 3}
    finally:
        release_slow.set()
        manifest.close()


def test_manifest_task_retry_deadline_survives_restart_and_gates_claim(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = replace(
        make_task(
            context,
            "uscongress.bill_info",
            "118/hr/1",
            {"congress": 118, "bill_type": "hr", "bill_number": 1},
            ("congress_gov",),
        ),
        task_id="congress-http-500",
    )
    path = tmp_path / "_state" / "openbb_archive.sqlite3"
    deadline = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    manifest = Manifest(path)
    try:
        manifest.upsert_tasks([task], plan_token="durable-retry")
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "pending",
                "congress_gov",
                0,
                None,
                1,
                error="congress_gov: HTTP Error 500",
                retry_not_before=deadline,
                transient_failures=7,
            )
        )
    finally:
        manifest.close()

    resumed = Manifest(path)
    try:
        assert resumed.pending_count(20, "durable-retry") == 1
        assert resumed.pending_batch(1, 20, "durable-retry") == []
        deferred, next_retry_at = resumed.retry_deferred_state("durable-retry")
        assert deferred == 1
        assert next_retry_at == deadline
        row = resumed.connection.execute(
            "SELECT attempts, transient_failures, retry_not_before "
            "FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert dict(row) == {
            "attempts": 1,
            "transient_failures": 7,
            "retry_not_before": deadline,
        }

        resumed.connection.execute(
            "UPDATE tasks SET retry_not_before=? WHERE task_id=?",
            (
                (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
                task.task_id,
            ),
        )
        resumed.connection.commit()
        candidates = resumed.pending_batch(1, 20, "durable-retry")
        assert [candidate.task_id for candidate in candidates] == [task.task_id]
        assert candidates[0].transient_failures == 7
        assert candidates[0].attempts == 1
    finally:
        resumed.close()


def test_executor_waits_for_task_retry_deadline_instead_of_churning(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = replace(
        make_task(
            context,
            "economy.pce",
            "date=2025-01-01",
            {"date": "2025-01-01"},
            ("fred",),
        ),
        task_id="retry-once",
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    calls = 0

    def worker(current: DownloadTask) -> TaskResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            return TaskResult(
                current,
                "pending",
                "fred",
                0,
                None,
                1,
                error="temporary failure",
                retry_not_before=(
                    datetime.now(timezone.utc) + timedelta(seconds=0.15)
                ).isoformat(),
                transient_failures=1,
            )
        return TaskResult(current, "empty", "fred", 0, None, 1)

    try:
        manifest.upsert_tasks([task], plan_token="retry-wait")
        started = time.monotonic()
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            worker,
            plan_token="retry-wait",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        elapsed = time.monotonic() - started

        assert attempted == 2
        assert calls == 2
        assert totals["pending"] == 1
        assert totals["empty"] == 1
        assert elapsed >= 0.1
        row = manifest.connection.execute(
            "SELECT status, retry_not_before, transient_failures "
            "FROM tasks WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        assert dict(row) == {
            "status": "empty",
            "retry_not_before": None,
            "transient_failures": 0,
        }
    finally:
        manifest.close()


def test_followup_discovery_runs_in_executor_after_provider_worker_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {"symbol": "AAPL"},
        ("yfinance",),
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    caller_thread = threading.get_ident()
    worker_active: dict[int, bool] = {}
    discovery_threads: list[int] = []
    produced_results: list[TaskResult] = []

    def worker(current: DownloadTask) -> TaskResult:
        thread_id = threading.get_ident()
        worker_active[thread_id] = True
        result = TaskResult(
            current,
            "success",
            "yfinance",
            1,
            current.output_path,
            1,
            records=[{"date": "2026-01-02", "close": 1.0}],
        )
        produced_results.append(result)
        worker_active[thread_id] = False
        return result

    def discover(_context: PlannerContext, result: TaskResult) -> list[DownloadTask]:
        thread_id = threading.get_ident()
        discovery_threads.append(thread_id)
        assert worker_active[thread_id] is False
        assert result.records
        return []

    monkeypatch.setattr(
        "downloader.download_openbb_archive.discover_followup_tasks", discover
    )
    try:
        manifest.upsert_tasks([task], plan_token="parallel-discovery")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            worker,
            plan_token="parallel-discovery",
            workers=1,
            batch_size=1,
            max_tasks=1,
            max_total_attempts=20,
            no_discovery=False,
            no_progress=True,
        )

        assert attempted == 1
        assert totals["success"] == 1
        assert discovery_threads and discovery_threads[0] != caller_thread
        assert produced_results[0].records == []
    finally:
        manifest.close()


def test_large_followup_upserts_are_chunked_before_parent_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "downloader.download_openbb_archive.FOLLOWUP_UPSERT_CHUNK_SIZE", 2
    )
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.price.historical",
        "AAPL",
        {"symbol": "AAPL"},
        ("yfinance",),
    )
    followups = [
        make_task(
            context,
            "equity.profile",
            f"FOLLOWUP{index}",
            {"symbol": f"FOLLOWUP{index}"},
            ("yfinance",),
        )
        for index in range(5)
    ]
    monkeypatch.setattr(
        "downloader.download_openbb_archive.discover_followup_tasks",
        lambda _context, _result: followups,
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="chunked-followups")
        real_upsert = manifest.upsert_tasks
        followup_chunk_sizes: list[int] = []

        def observed_upsert(tasks, **kwargs):
            materialized = list(tasks)
            if kwargs.get("task_source") == "followup":
                followup_chunk_sizes.append(len(materialized))
            return real_upsert(materialized, **kwargs)

        monkeypatch.setattr(manifest, "upsert_tasks", observed_upsert)

        def worker(current: DownloadTask) -> TaskResult:
            return TaskResult(
                current,
                "success",
                "yfinance",
                1,
                current.output_path,
                1,
                records=[{"date": "2026-01-02", "close": 1.0}],
            )

        attempted, totals = execute_download_tasks(
            context,
            manifest,
            worker,
            plan_token="chunked-followups",
            workers=1,
            batch_size=1,
            max_tasks=1,
            max_total_attempts=20,
            no_discovery=False,
            no_progress=True,
        )

        assert attempted == 1
        assert totals["success"] == 1
        assert totals["discovered"] == 5
        assert followup_chunk_sizes == [2, 2, 1]
        assert manifest.active_task_count("chunked-followups") == 6
    finally:
        manifest.close()


def test_hot_provider_refill_rotates_across_all_endpoints(tmp_path: Path) -> None:
    context = _context(tmp_path)
    endpoints = (
        "equity.price.historical",
        "equity.profile",
        "equity.fundamental.dividends",
        "equity.fundamental.management",
        "equity.ownership.share_statistics",
        "etf.info",
    )
    context.commands = {f".{endpoint}": ["yfinance"] for endpoint in endpoints}
    tasks = [
        replace(
            make_task(
                context,
                endpoint,
                f"{endpoint}-{index}",
                {"symbol": f"S{index}"},
                ("yfinance",),
            ),
            task_id=f"{endpoint}-{index:02d}",
        )
        for endpoint in endpoints
        for index in range(10)
    ]
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    seen: list[str] = []

    class Worker:
        def __init__(self) -> None:
            # The small global batch forces provider-specific hot refill
            # repeatedly while generic-worker execution stays serial.
            self.runtime = ProviderRuntime(
                {"yfinance": 0.1},
                {"yfinance": 1},
                60,
            )

        def __call__(self, task: DownloadTask) -> TaskResult:
            seen.append(task.endpoint)
            return TaskResult(task, "empty", "yfinance", 0, None, 1)

    try:
        manifest.upsert_tasks(tasks, plan_token="hot-fair")
        attempted, _ = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="hot-fair",
            workers=4,
            batch_size=4,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )

        assert attempted == len(tasks)
        assert set(seen[:12]) == set(endpoints)
        scheduler = json.loads(
            (tmp_path / "_state" / "provider_scheduler.json").read_text(
                encoding="utf-8"
            )
        )
        assert scheduler["phase"] == "stopped"
        assert scheduler["attempted_this_run"] == len(tasks)
        assert scheduler["global_worker_limit"] == 4
        yfinance_pool = scheduler["providers"]["yfinance"]
        assert yfinance_pool["requests_per_second"] == 0.1
        assert yfinance_pool["execution_limit"] == 1
        assert yfinance_pool["queue_limit"] == 13
        assert yfinance_pool["active"] == 0
        assert yfinance_pool["buffered"] == 0
        assert yfinance_pool["reservations"] == 0
        assert yfinance_pool["refill_threshold"] >= yfinance_pool["execution_limit"]
        assert yfinance_pool["refill_threshold"] <= yfinance_pool["queue_limit"]
        assert yfinance_pool["seed_route_count"] == len(endpoints)
        assert yfinance_pool["cooldown"] is False
        assert yfinance_pool["unavailable"] is False
    finally:
        manifest.close()


def test_hot_provider_refill_keeps_local_bulk_path_alive_during_api_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    bls_endpoint = "economy.survey.bls_series"
    yahoo_endpoint = "equity.price.historical"
    context.commands = {
        f".{bls_endpoint}": ["bls"],
        f".{yahoo_endpoint}": ["yfinance"],
    }
    bls_tasks = [
        replace(
            make_task(
                context,
                bls_endpoint,
                f"series-{index}",
                {"symbol": f"SERIES{index}"},
                ("bls",),
            ),
            task_id=f"bls-cooldown-{index:02d}",
        )
        for index in range(20)
    ]
    yahoo_task = replace(
        make_task(
            context,
            yahoo_endpoint,
            "AAPL",
            {"symbol": "AAPL"},
            ("yfinance",),
        ),
        task_id="slow-yahoo",
    )
    runtime = ProviderRuntime(
        {"bls": 1000.0, "yfinance": 1000.0},
        {"bls": 1, "yfinance": 1},
        60.0,
    )
    runtime._blocked_until["bls"] = time.time() + 3600.0
    seen: list[str] = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def can_run_during_provider_cooldown(
            self, provider: str, endpoint: str
        ) -> bool:
            return provider == "bls" and endpoint == bls_endpoint

        def local_cooldown_bypass_providers(self) -> set[str]:
            return {"bls"}

        def __call__(self, task: DownloadTask) -> TaskResult:
            if task.task_id == "slow-yahoo":
                time.sleep(0.2)
            seen.append(task.task_id)
            return TaskResult(task, "empty", task.providers[0], 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    pending_batch_calls = 0
    real_pending_batch = manifest.pending_batch

    def one_full_scan_only(*args, **kwargs):
        nonlocal pending_batch_calls
        pending_batch_calls += 1
        if pending_batch_calls > 1:
            raise AssertionError("hot BLS refill fell back to a full manifest scan")
        return real_pending_batch(*args, **kwargs)

    monkeypatch.setattr(manifest, "pending_batch", one_full_scan_only)
    try:
        manifest.upsert_tasks(
            [*bls_tasks, yahoo_task], plan_token="bls-local-refill"
        )
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="bls-local-refill",
            workers=8,
            batch_size=8,
            max_tasks=12,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )

        assert attempted == 12
        assert totals["empty"] == 12
        assert pending_batch_calls == 1
        assert len([task_id for task_id in seen if task_id.startswith("bls-")]) == 11
        assert "slow-yahoo" in seen
    finally:
        manifest.close()


def test_provider_buffer_pops_oldest_task_from_least_active_endpoint(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    income_one = make_task(
        context,
        "equity.fundamental.income",
        "income-1",
        {"symbol": "A"},
        ("sec",),
    )
    income_two = make_task(
        context,
        "equity.fundamental.income",
        "income-2",
        {"symbol": "B"},
        ("sec",),
    )
    headers = make_task(
        context,
        "regulators.sec.filing_headers",
        "headers",
        {"url": "https://www.sec.gov/Archives/example-index.html"},
        ("sec",),
    )
    filings = make_task(
        context,
        "equity.fundamental.filings",
        "filings",
        {"symbol": "A"},
        ("sec",),
    )
    tasks = deque([income_one, income_two, headers, filings])
    active = {"equity.fundamental.income": 4}

    assert _pop_fairest_endpoint_task(tasks, active) is headers
    active["regulators.sec.filing_headers"] = 1
    assert _pop_fairest_endpoint_task(tasks, active) is filings
    assert list(tasks) == [income_one, income_two]


def test_provider_buffer_prefers_sec_statement_cache_affinity(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    balance_a = make_task(
        context,
        "equity.fundamental.balance",
        "A/period=annual",
        {"symbol": "A", "period": "annual"},
        ("sec",),
    )
    cash_b = make_task(
        context,
        "equity.fundamental.cash",
        "B/period=annual",
        {"symbol": "B", "period": "annual"},
        ("sec",),
    )
    cash_a = make_task(
        context,
        "equity.fundamental.cash",
        "A/period=annual",
        {"symbol": "A", "period": "annual"},
        ("sec",),
    )
    tasks = deque([cash_b, cash_a])

    selected = _pop_fairest_endpoint_task(
        tasks,
        {"equity.fundamental.cash": 0},
        preferred_affinities={_task_execution_affinity(balance_a)},
    )

    assert selected is cash_a
    assert list(tasks) == [cash_b]


def test_manifest_orders_sec_statement_candidates_by_scope_affinity(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "manifest.sqlite3")
    try:
        tasks = [
            make_task(
                context,
                "equity.fundamental.balance",
                f"{symbol}/period=annual",
                {"symbol": symbol, "period": "annual"},
                ("sec",),
            )
            for symbol in ("ZZZ", "AAA", "MMM")
        ]
        manifest.upsert_tasks(tasks, plan_token="affinity")

        selected = manifest.pending_endpoint_batch(
            "equity",
            "equity.fundamental.balance",
            3,
            20,
            "affinity",
            required_provider="sec",
        )
        indexes = {
            str(row[0])
            for row in manifest.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
    finally:
        manifest.close()

    assert [task.scope_key for task in selected] == [
        "AAA/period=annual",
        "MMM/period=annual",
        "ZZZ/period=annual",
    ]
    assert "idx_tasks_sec_statement_affinity" in indexes


def test_provider_buffer_respects_local_endpoint_concurrency_cap(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    income = make_task(
        context,
        "equity.fundamental.income",
        "income",
        {"symbol": "A"},
        ("sec",),
    )
    headers = make_task(
        context,
        "regulators.sec.filing_headers",
        "headers",
        {"url": "https://www.sec.gov/Archives/example-index.html"},
        ("sec",),
    )
    active = {"equity.fundamental.income": 2}
    caps = {"equity.fundamental.income": 2}

    tasks = deque([income, headers])
    assert _pop_fairest_endpoint_task(tasks, active, caps) is headers
    assert list(tasks) == [income]
    assert _pop_fairest_endpoint_task(tasks, active, caps) is None


def test_entitlement_probe_bulk_resolves_only_matching_fmp_domain(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    endpoint = "equity.fundamental.metrics"

    def task(task_id: str, symbol: str) -> DownloadTask:
        return DownloadTask(
            task_id=task_id,
            endpoint=endpoint,
            category="equity",
            scope_key=f"{symbol}/period=annual",
            kwargs={"symbol": symbol, "period": "annual", "limit": 1000},
            providers=("fmp",),
            output_path=str(tmp_path / "data" / f"{task_id}.parquet"),
        )

    probe = task("probe", "AAPL")
    same_domain = task("same-domain", "MSFT")
    other_domain = task("other-domain", "2330.TW")
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 1.0)
    called: list[str] = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current: DownloadTask) -> TaskResult:
            called.append(current.task_id)
            if current.task_id == "probe":
                return TaskResult(
                    current,
                    "unavailable",
                    "fmp",
                    0,
                    None,
                    1,
                    error=(
                        "fmp: UnauthorizedError: 402 Premium Query Parameter: "
                        "Special Endpoint: This value set for 'symbol' is not "
                        "available under your current subscription"
                    ),
                    provider_outcomes={"fmp": "unavailable"},
                )
            return TaskResult(current, "empty", "fmp", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([probe, same_domain, other_domain], plan_token="plan")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="plan",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
            entitlement_probe_task_ids={probe.task_id},
        )
        assert attempted == 2
        assert set(called) == {"probe", "other-domain"}
        assert "same-domain" not in called
        assert totals["unavailable"] == 1
        assert totals["bulk_unavailable"] == 1
        assert totals["empty"] == 1
        statuses = {
            str(row["task_id"]): str(row["status"])
            for row in manifest.connection.execute(
                "SELECT task_id,status FROM tasks ORDER BY task_id"
            )
        }
        assert statuses == {
            "other-domain": "empty",
            "probe": "unavailable",
            "same-domain": "unavailable",
        }
        assert runtime.availability("fmp", endpoint, "us")[0] is False
        assert runtime.availability("fmp", endpoint, "tw")[0] is True
    finally:
        manifest.close()


def test_executor_waits_for_provider_cooldown_without_task_churn(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.profile",
        "AAPL",
        {"symbol": "AAPL"},
        ("fmp",),
    )
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 1.0)
    runtime._blocked_until["fmp"] = time.time() + 0.05
    called_at: list[float] = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            called_at.append(time.time())
            return TaskResult(current, "empty", "fmp", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="cooldown")
        started = time.time()
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="cooldown",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 1
        assert totals["empty"] == 1
        assert called_at[0] - started >= 0.04
        assert manifest.counts("cooldown") == {"empty": 1}
    finally:
        manifest.close()


def test_executor_does_not_block_available_provider_behind_cooldown_fallback(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    task = make_task(
        context,
        "equity.fundamental.balance_growth",
        "AAPL",
        {"symbol": "AAPL"},
        ("sec", "fmp"),
    )
    runtime = ProviderRuntime(
        {"sec": 1000.0, "fmp": 1000.0},
        {"sec": 1, "fmp": 1},
        1.0,
    )
    runtime._blocked_until["fmp"] = time.time() + 1.0
    called_before = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            called_before.append(time.time() < runtime._blocked_until["fmp"])
            return TaskResult(current, "success", "sec", 1, current.output_path, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task], plan_token="mixed-cooldown")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="mixed-cooldown",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 1
        assert totals["success"] == 1
        assert called_before == [True]
    finally:
        manifest.close()


def test_executor_reserves_provider_capacity_before_claim(tmp_path: Path) -> None:
    context = _context(tmp_path)
    tasks = [
        replace(
            make_task(
                context,
                "equity.price.historical",
                f"symbol-{index}",
                {"symbol": f"symbol-{index}"},
                ("fmp", "yfinance"),
            ),
            task_id=f"capacity-{index}",
        )
        for index in range(4)
    ]
    runtime = ProviderRuntime(
        {"fmp": 1000.0, "yfinance": 1000.0},
        {"fmp": 4, "yfinance": 1},
        1.0,
    )
    runtime.disable_route("fmp", "equity.price.historical", "route unavailable")
    active = 0
    max_active = 0
    lock = threading.Lock()

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.01)
            with lock:
                active -= 1
            return TaskResult(current, "empty", "yfinance", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="capacity")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="capacity",
            workers=4,
            batch_size=4,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 4
        assert totals["empty"] == 4
        assert max_active == 1
        assert manifest.counts("capacity") == {"empty": 4}
        assert all(
            "provider concurrency capacity busy" not in str(row[0] or "")
            for row in manifest.connection.execute(
                "SELECT error FROM tasks WHERE plan_token='capacity'"
            )
        )
    finally:
        manifest.close()


def test_executor_deepens_single_endpoint_to_fill_provider_capacity(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    yfinance_tasks = [
        replace(
            make_task(
                context,
                "economy.aaa_target",
                f"yf-{index}",
                {"symbol": f"YF{index}"},
                ("yfinance",),
            ),
            task_id=f"yf-{index}",
        )
        for index in range(20)
    ]
    dilution_tasks = [
        replace(
            make_task(
                context,
                f"economy.synthetic_{endpoint_index}",
                f"fmp-{endpoint_index}-{row_index}",
                {"symbol": f"F{endpoint_index}-{row_index}"},
                ("fmp",),
            ),
            task_id=f"fmp-{endpoint_index}-{row_index}",
        )
        for endpoint_index in range(20)
        for row_index in range(10)
    ]
    runtime = ProviderRuntime(
        {"yfinance": 1000.0, "fmp": 1000.0},
        {"yfinance": 10, "fmp": 1},
        1.0,
    )
    active_yfinance = 0
    max_active_yfinance = 0
    lock = threading.Lock()
    all_started = threading.Barrier(11)

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal active_yfinance, max_active_yfinance
            if current.providers[0] == "yfinance":
                with lock:
                    active_yfinance += 1
                    max_active_yfinance = max(max_active_yfinance, active_yfinance)
            all_started.wait(timeout=2.0)
            if current.providers[0] == "yfinance":
                with lock:
                    active_yfinance -= 1
            return TaskResult(current, "empty", current.providers[0], 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(
            [*yfinance_tasks, *dilution_tasks], plan_token="deep-provider"
        )
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="deep-provider",
            workers=11,
            batch_size=11,
            max_tasks=11,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 11
        assert totals["empty"] == 11
        assert max_active_yfinance == 10
    finally:
        manifest.close()


def test_manifest_seeds_every_endpoint_before_deepening_earlier_routes(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    dilution_tasks = [
        replace(
            make_task(
                context,
                f"economy.synthetic_{endpoint_index:03d}",
                f"fmp-{endpoint_index}-{row_index}",
                {"symbol": f"F{endpoint_index}-{row_index}"},
                ("fmp",),
            ),
            task_id=f"fmp-{endpoint_index}-{row_index}",
        )
        for endpoint_index in range(100)
        for row_index in range(10)
    ]
    late_provider_task = replace(
        make_task(
            context,
            "zz.target",
            "late-provider",
            {"symbol": "LATE"},
            ("yfinance",),
        ),
        task_id="late-provider",
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(
            [*dilution_tasks, late_provider_task], plan_token="endpoint-seeds"
        )

        candidates = manifest.pending_batch(
            512,
            20,
            "endpoint-seeds",
        )

        assert len(candidates) == 512
        assert any(task.task_id == "late-provider" for task in candidates)
        assert {task.endpoint for task in candidates} == {
            *(f"economy.synthetic_{index:03d}" for index in range(100)),
            "zz.target",
        }
    finally:
        manifest.close()


def test_manifest_required_provider_respects_fallback_chain_order(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    earlier_provider_task = replace(
        make_task(
            context,
            "fixedincome.government.yield_curve",
            "federal-first",
            {"date": "2000-01-03"},
            ("federal_reserve", "econdb"),
        ),
        task_id="aaa-federal-first",
    )
    econdb_only_task = replace(
        make_task(
            context,
            "fixedincome.government.yield_curve",
            "econdb-only",
            {"date": "2000-01-04"},
            ("econdb",),
        ),
        task_id="zzz-econdb-only",
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(
            [earlier_provider_task, econdb_only_task],
            plan_token="provider-chain-order",
        )

        normal = manifest.pending_endpoint_batch(
            "fixedincome",
            "fixedincome.government.yield_curve",
            8,
            20,
            "provider-chain-order",
            required_provider="econdb",
        )
        federal_cooling_down = manifest.pending_endpoint_batch(
            "fixedincome",
            "fixedincome.government.yield_curve",
            8,
            20,
            "provider-chain-order",
            excluded_providers={"federal_reserve"},
            required_provider="econdb",
        )

        assert [task.task_id for task in normal] == ["zzz-econdb-only"]
        assert [task.task_id for task in federal_cooling_down] == [
            "aaa-federal-first",
            "zzz-econdb-only",
        ]
    finally:
        manifest.close()


def test_executor_seeds_each_provider_within_one_mixed_endpoint(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands = {
        ".fixedincome.government.yield_curve": ["fred", "federal_reserve"]
    }
    fred_tasks = [
        replace(
            make_task(
                context,
                "fixedincome.government.yield_curve",
                f"fred-{index}",
                {"date": f"2000-01-{index % 28 + 1:02d}"},
                ("fred",),
            ),
            task_id=f"aaa-fred-{index:03d}",
        )
        for index in range(100)
    ]
    federal_tasks = [
        replace(
            make_task(
                context,
                "fixedincome.government.yield_curve",
                f"federal-{index}",
                {"date": f"2001-01-{index % 28 + 1:02d}"},
                ("federal_reserve",),
            ),
            task_id=f"zzz-federal-{index:03d}",
        )
        for index in range(20)
    ]
    runtime = ProviderRuntime(
        {"fred": 1000.0, "federal_reserve": 1000.0},
        {"fred": 1, "federal_reserve": 10},
        1.0,
    )
    active_federal = 0
    max_active_federal = 0
    lock = threading.Lock()
    all_started = threading.Barrier(11)

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal active_federal, max_active_federal
            if current.providers[0] == "federal_reserve":
                with lock:
                    active_federal += 1
                    max_active_federal = max(max_active_federal, active_federal)
            all_started.wait(timeout=2.0)
            if current.providers[0] == "federal_reserve":
                with lock:
                    active_federal -= 1
            return TaskResult(current, "empty", current.providers[0], 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(
            [*fred_tasks, *federal_tasks], plan_token="mixed-provider-endpoint"
        )
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="mixed-provider-endpoint",
            workers=11,
            batch_size=11,
            max_tasks=11,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 11
        assert totals["empty"] == 11
        assert max_active_federal == 10
    finally:
        manifest.close()


def test_executor_refills_fast_provider_without_waiting_for_slow_provider(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands = {
        ".economy.fast": ["fast_provider"],
        ".economy.slow": ["slow_provider"],
    }
    fast_tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"fast-{index}",
                {"symbol": f"FAST{index}"},
                ("fast_provider",),
            ),
            task_id=f"fast-{index:02d}",
        )
        for index in range(8)
    ]
    slow_tasks = [
        replace(
            make_task(
                context,
                "economy.slow",
                f"slow-{index}",
                {"symbol": f"SLOW{index}"},
                ("slow_provider",),
            ),
            task_id=f"slow-{index:02d}",
        )
        for index in range(20)
    ]
    runtime = ProviderRuntime(
        {"fast_provider": 1000.0, "slow_provider": 1000.0},
        {"fast_provider": 4, "slow_provider": 20},
        1.0,
    )
    fast_started = 0
    slow_timeouts = 0
    fast_second_wave_started = threading.Event()
    lock = threading.Lock()

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal fast_started, slow_timeouts
            if current.providers[0] == "fast_provider":
                with lock:
                    fast_started += 1
                    if fast_started == 8:
                        fast_second_wave_started.set()
            elif not fast_second_wave_started.wait(timeout=0.5):
                with lock:
                    slow_timeouts += 1
            return TaskResult(current, "empty", current.providers[0], 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([*fast_tasks, *slow_tasks], plan_token="provider-refill")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="provider-refill",
            workers=24,
            batch_size=24,
            max_tasks=28,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 28
        assert totals["empty"] == 28
        assert fast_started == 8
        assert slow_timeouts == 0
    finally:
        manifest.close()


def test_executor_resupplies_from_claimed_buffer_before_manifest_refill(
    tmp_path: Path, monkeypatch
) -> None:
    context = _context(tmp_path)
    context.commands = {".economy.fast": ["fast_provider"]}
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"fast-{index}",
                {"symbol": f"FAST{index}"},
                ("fast_provider",),
            ),
            task_id=f"buffer-fast-{index:02d}",
        )
        for index in range(8)
    ]
    runtime = ProviderRuntime({"fast_provider": 1.0}, {"fast_provider": 1}, 1.0)
    second_started = threading.Event()
    starts = 0
    starts_lock = threading.Lock()

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal starts
            with starts_lock:
                starts += 1
                if starts == 2:
                    second_started.set()
            return TaskResult(current, "empty", "fast_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_pending_batch = manifest.pending_batch
    pending_batch_calls = 0
    pending_before_full_refill: list[int] = []

    def observed_pending_batch(*args, **kwargs):
        nonlocal pending_batch_calls
        pending_batch_calls += 1
        pending_before_full_refill.append(
            int(
                manifest.connection.execute(
                    "SELECT COUNT(*) FROM tasks WHERE status='pending'"
                ).fetchone()[0]
            )
        )
        if pending_batch_calls == 2:
            assert second_started.wait(timeout=1.0), (
                "provider execution slot was not refilled from memory before "
                "the next manifest scan"
            )
        return real_pending_batch(*args, **kwargs)

    monkeypatch.setattr(manifest, "pending_batch", observed_pending_batch)
    try:
        manifest.upsert_tasks(tasks, plan_token="provider-buffer")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="provider-buffer",
            workers=4,
            batch_size=4,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 8
        assert totals["empty"] == 8
        # The initial full fairness scan seeds the provider. Subsequent
        # depletion may now use its indexed endpoint refill and therefore does
        # not need another global manifest scan while provider work remains.
        # A final scan is allowed only to prove global exhaustion.
        assert pending_batch_calls >= 1
        assert all(count == 0 for count in pending_before_full_refill[1:])
    finally:
        manifest.close()


def test_executor_resupplies_before_slow_completion_persistence(
    tmp_path: Path, monkeypatch
) -> None:
    context = _context(tmp_path)
    context.commands = {".economy.fast": ["fast_provider"]}
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"fast-{index}",
                {"symbol": f"FAST{index}"},
                ("fast_provider",),
            ),
            task_id=f"persist-fast-{index:02d}",
        )
        for index in range(2)
    ]
    runtime = ProviderRuntime({"fast_provider": 1000.0}, {"fast_provider": 1}, 1.0)
    second_started = threading.Event()
    starts = 0
    starts_lock = threading.Lock()

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal starts
            with starts_lock:
                starts += 1
                if starts == 2:
                    second_started.set()
            return TaskResult(current, "empty", "fast_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_complete = manifest.complete
    complete_calls = 0

    def observed_complete(result):
        nonlocal complete_calls
        complete_calls += 1
        if complete_calls == 1:
            assert second_started.wait(timeout=1.0), (
                "provider execution slot was not refilled before slow "
                "completion persistence"
            )
        return real_complete(result)

    monkeypatch.setattr(manifest, "complete", observed_complete)
    try:
        manifest.upsert_tasks(tasks, plan_token="provider-persist-buffer")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="provider-persist-buffer",
            workers=2,
            batch_size=2,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 2
        assert totals["empty"] == 2
        assert starts == 2
    finally:
        manifest.close()


def test_preloaded_provider_queue_runs_while_manifest_thread_is_busy(
    tmp_path: Path, monkeypatch
) -> None:
    context = _context(tmp_path)
    context.commands = {".economy.fast": ["fast_provider"]}
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"fast-{index}",
                {"symbol": f"FAST{index}"},
                ("fast_provider",),
            ),
            task_id=f"preloaded-fast-{index:02d}",
        )
        for index in range(3)
    ]
    runtime = ProviderRuntime({"fast_provider": 0.1}, {"fast_provider": 1}, 1.0)
    third_started = threading.Event()
    starts = 0
    active = 0
    max_active = 0
    starts_lock = threading.Lock()

    class Worker:
        preload_provider_queues = True

        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal starts, active, max_active
            with runtime.semaphore_slot("fast_provider"):
                with starts_lock:
                    starts += 1
                    active += 1
                    max_active = max(max_active, active)
                    if starts == 3:
                        third_started.set()
                time.sleep(0.005)
                with starts_lock:
                    active -= 1
            return TaskResult(current, "empty", "fast_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_complete = manifest.complete
    complete_calls = 0

    def busy_complete(result):
        nonlocal complete_calls
        complete_calls += 1
        if complete_calls == 1:
            assert third_started.wait(timeout=1.0), (
                "provider queue stopped while the manifest thread was busy"
            )
        return real_complete(result)

    monkeypatch.setattr(manifest, "complete", busy_complete)
    try:
        manifest.upsert_tasks(tasks, plan_token="preloaded-provider-queue")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="preloaded-provider-queue",
            workers=3,
            batch_size=3,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 3
        assert totals["empty"] == 3
        assert starts == 3
        assert max_active == 1
    finally:
        manifest.close()


def test_provider_executor_keeps_prefetch_as_queue_not_threads() -> None:
    gate = threading.Event()
    entered = 0
    maximum_entered = 0
    entered_lock = threading.Lock()

    def worker() -> None:
        nonlocal entered, maximum_entered
        with entered_lock:
            entered += 1
            maximum_entered = max(maximum_entered, entered)
        gate.wait(timeout=2.0)
        with entered_lock:
            entered -= 1

    with ProviderExecutorPool(100, lambda _provider: 2) as executor:
        futures = [executor.submit("bounded", worker) for _ in range(40)]
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            with entered_lock:
                if entered == 2:
                    break
            time.sleep(0.005)
        with entered_lock:
            assert entered == 2
            assert maximum_entered == 2
        assert executor.lane_limits() == {"bounded": 2}
        gate.set()
        for future in futures:
            future.result(timeout=2.0)


def test_provider_executor_lane_reserves_only_one_adaptive_step() -> None:
    runtime = ProviderRuntime({"sec": 10.0, "slow": 0.1}, {"sec": 72, "slow": 4}, 1)

    assert runtime._adaptive_concurrency_caps["sec"] == 128
    assert _adaptive_executor_lane_limit(runtime, "sec", 72) == 77
    # Low-rate providers remain bounded by their actual adaptive cap.
    assert runtime._adaptive_concurrency_caps["slow"] == 4
    assert _adaptive_executor_lane_limit(runtime, "slow", 4) == 4


def test_completed_future_persistence_is_batched_before_provider_refill(
    tmp_path: Path, monkeypatch
) -> None:
    context = _context(tmp_path)
    context.commands = {".economy.fast": ["fast_provider"]}
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"fast-{index}",
                {"symbol": f"FAST{index}"},
                ("fast_provider",),
            ),
            task_id=f"completion-batch-fast-{index:02d}",
        )
        for index in range(40)
    ]
    # The synthetic worker intentionally starts a 20-call producer wave and
    # does not acquire ProviderRuntime's semaphore.  Declare that service
    # capacity explicitly now that prefetched work is held by a bounded
    # provider executor instead of becoming global executor threads.
    runtime = ProviderRuntime({"fast_provider": 1.0}, {"fast_provider": 20}, 1.0)
    first_wave = threading.Barrier(20)
    started = 0
    started_lock = threading.Lock()

    class Worker:
        preload_provider_queues = True

        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal started
            with started_lock:
                started += 1
                in_first_wave = started <= 20
            if in_first_wave:
                first_wave.wait(timeout=2.0)
            return TaskResult(current, "empty", "fast_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_complete = manifest.complete
    complete_calls = 0

    def observed_complete(result):
        nonlocal complete_calls
        complete_calls += 1
        if complete_calls == 17:
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                with started_lock:
                    if started > 20:
                        break
                time.sleep(0.005)
            with started_lock:
                assert started > 20, (
                    "the next provider wave did not start before persisting "
                    "an unbounded completed-future set"
                )
        return real_complete(result)

    monkeypatch.setattr(manifest, "complete", observed_complete)
    try:
        manifest.upsert_tasks(tasks, plan_token="completion-persistence-batch")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="completion-persistence-batch",
            workers=20,
            batch_size=20,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 40
        assert totals["empty"] == 40
        assert complete_calls == 40
    finally:
        manifest.close()


def test_manifest_completion_batch_uses_one_durable_commit(tmp_path: Path) -> None:
    context = _context(tmp_path)
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"batch-{index}",
                {"symbol": f"BATCH{index}"},
                ("fast_provider",),
            ),
            task_id=f"manifest-completion-batch-{index}",
        )
        for index in range(3)
    ]
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    statements: list[str] = []
    try:
        manifest.upsert_tasks(tasks, plan_token="manifest-completion-batch")
        manifest.claim(tasks)
        manifest.connection.set_trace_callback(statements.append)
        with manifest.completion_batch():
            for task in tasks:
                manifest.complete(
                    TaskResult(task, "empty", "fast_provider", 0, None, 1)
                )
        manifest.connection.set_trace_callback(None)

        assert sum(statement == "COMMIT" for statement in statements) == 1
        assert manifest.counts("manifest-completion-batch") == {"empty": 3}
    finally:
        manifest.connection.set_trace_callback(None)
        manifest.close()


def test_manifest_keeps_only_nonredundant_scheduler_indexes(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        indexes = {
            str(row[1])
            for row in manifest.connection.execute("PRAGMA index_list(tasks)")
        }

        assert "idx_tasks_schedule_age_v2" in indexes
        assert "idx_tasks_active_plan" in indexes
        assert not indexes.intersection(
            {
                "idx_tasks_status",
                "idx_tasks_schedule",
                "idx_tasks_schedule_age",
            }
        )
    finally:
        manifest.close()


def test_completed_result_backpressure_is_global_and_bounded(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "downloader.download_openbb_archive.COMPLETION_PERSISTENCE_BATCH_CAP", 2
    )
    monkeypatch.setattr(
        "downloader.download_openbb_archive.COMPLETION_BACKPRESSURE_CAP", 2
    )
    context = _context(tmp_path)
    context.commands = {".economy.fast": ["fast_provider"]}
    tasks = [
        replace(
            make_task(
                context,
                "economy.fast",
                f"bounded-{index}",
                {"symbol": f"BOUNDED{index}"},
                ("fast_provider",),
            ),
            task_id=f"bounded-completion-{index:02d}",
        )
        for index in range(8)
    ]
    runtime = ProviderRuntime({"fast_provider": 1000.0}, {"fast_provider": 4}, 1.0)
    first_wave = threading.Barrier(4)
    started = 0
    started_lock = threading.Lock()

    class Worker:
        preload_provider_queues = True

        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal started
            with started_lock:
                started += 1
                initial = started <= 4
            if initial:
                first_wave.wait(timeout=2.0)
            return TaskResult(current, "empty", "fast_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_complete = manifest.complete
    completions = 0

    def observed_complete(result):
        nonlocal completions
        completions += 1
        with started_lock:
            current_started = started
        if completions == 1:
            assert current_started == 4, (
                "producer admission continued while the completed-result "
                "handoff exceeded its memory bound"
            )
        if completions == 3:
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                with started_lock:
                    if started > 4:
                        break
                time.sleep(0.005)
            with started_lock:
                assert started > 4, (
                    "producer admission did not resume after persistence "
                    "drained the completed-result handoff"
                )
        return real_complete(result)

    monkeypatch.setattr(manifest, "complete", observed_complete)
    try:
        manifest.upsert_tasks(tasks, plan_token="bounded-completion")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="bounded-completion",
            workers=4,
            batch_size=4,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == len(tasks)
        assert totals["empty"] == len(tasks)
    finally:
        manifest.close()


def test_hot_refill_deepens_the_routes_that_have_provider_backlog(
    tmp_path: Path, monkeypatch
) -> None:
    """Sparse route coverage must still restore the provider's whole queue.

    Provider discovery can expose dozens of selected endpoints even when only
    one currently has pending work.  The hot refill must give every route its
    first fair share and then reuse the remaining global capacity on the live
    route; otherwise its API lane receives only one tiny refill per completion
    transaction.
    """
    context = _context(tmp_path)
    live_endpoint = "economy.hot"
    context.commands = {
        f".{endpoint}": ["sparse_provider"]
        for endpoint in (
            live_endpoint,
            *(f"economy.idle_{index:02d}" for index in range(35)),
        )
    }
    tasks = [
        replace(
            make_task(
                context,
                live_endpoint,
                f"hot-{index}",
                {"symbol": f"HOT{index}"},
                ("sparse_provider",),
            ),
            task_id=f"sparse-hot-refill-{index:02d}",
        )
        for index in range(40)
    ]
    runtime = ProviderRuntime({"sparse_provider": 1000.0}, {"sparse_provider": 1}, 1.0)
    from concurrent.futures import ALL_COMPLETED, wait as futures_wait

    # This test models a deliberately drained provider wave. Production uses
    # FIRST_COMPLETED, whose returned set may contain only a scheduling-dependent
    # subset even after the worker barrier opens. Make the test wave boundary
    # explicit so the assertion measures deep refill, not Future callback timing.
    monkeypatch.setattr(
        "downloader.download_openbb_archive.wait",
        lambda futures, timeout, return_when: futures_wait(
            futures,
            timeout=timeout,
            return_when=ALL_COMPLETED,
        ),
    )
    first_wave = threading.Barrier(20)
    second_wave_started = threading.Event()
    starts = 0
    starts_lock = threading.Lock()

    class Worker:
        preload_provider_queues = True

        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            nonlocal starts
            with starts_lock:
                starts += 1
                current_start = starts
                if starts == len(tasks):
                    second_wave_started.set()
            if current_start <= 20:
                first_wave.wait(timeout=2.0)
            return TaskResult(current, "empty", "sparse_provider", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    real_complete = manifest.complete
    complete_calls = 0

    def observed_complete(result):
        nonlocal complete_calls
        complete_calls += 1
        if complete_calls == 1:
            # Preserve the ordering assertion while tolerating host
            # scheduling jitter from the live multi-process downloader.
            assert second_wave_started.wait(timeout=5.0), (
                "hot refill did not deepen the only route with provider backlog"
            )
        return real_complete(result)

    monkeypatch.setattr(manifest, "complete", observed_complete)
    try:
        manifest.upsert_tasks(tasks, plan_token="sparse-hot-refill")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="sparse-hot-refill",
            workers=20,
            batch_size=20,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == len(tasks)
        assert totals["empty"] == len(tasks)
        assert starts == len(tasks)
    finally:
        manifest.close()


def test_executor_rotates_endpoints_within_one_scarce_provider(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    endpoints = (
        "regulators.sec.symbol_map",
        "equity.compare.company_facts",
    )
    tasks = [
        replace(
            make_task(
                context,
                endpoint,
                f"{endpoint}-{index}",
                {"query": f"S{index}"},
                ("sec",),
            ),
            task_id=f"sec-{endpoint}-{index}",
        )
        for endpoint in endpoints
        for index in range(4)
    ]
    runtime = ProviderRuntime({"sec": 1000.0}, {"sec": 1}, 3600)
    observed: list[str] = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            observed.append(current.endpoint)
            return TaskResult(current, "empty", "sec", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="provider-fair")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="provider-fair",
            workers=4,
            batch_size=4,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 8
        assert totals["empty"] == 8
        assert set(observed[:2]) == set(endpoints)
        assert all(left != right for left, right in zip(observed, observed[1:]))
    finally:
        manifest.close()


def test_executor_prioritizes_endpoint_without_any_accepted_evidence(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    accepted_done = replace(
        make_task(
            context,
            "equity.compare.company_facts",
            "accepted-done",
            {"symbol": "DONE"},
            ("fmp",),
        ),
        task_id="accepted-done",
    )
    accepted_pending = replace(
        make_task(
            context,
            "equity.compare.company_facts",
            "accepted-pending",
            {"symbol": "PENDING"},
            ("fmp",),
        ),
        task_id="accepted-pending",
    )
    zero_pending = replace(
        make_task(
            context,
            "news.world",
            "zero-pending",
            {"page": 0, "limit": 100},
            ("fmp",),
        ),
        task_id="zero-pending",
    )
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 3600)
    observed: list[str] = []

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            observed.append(current.endpoint)
            return TaskResult(current, "empty", "fmp", 0, None, 1)

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(
            [accepted_done, accepted_pending, zero_pending],
            plan_token="zero-first",
        )
        manifest.complete(
            TaskResult(
                accepted_done,
                "success",
                "fmp",
                1,
                accepted_done.output_path,
                1,
            )
        )
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="zero-first",
            workers=1,
            batch_size=1,
            max_tasks=1,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 1
        assert totals["empty"] == 1
        assert observed == ["news.world"]
        assert manifest.counts("zero-first") == {
            "empty": 1,
            "pending": 1,
            "success": 1,
        }
    finally:
        manifest.close()


def test_executor_bulk_finalizes_provider_only_disabled_route(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    tasks = [
        replace(
            make_task(
                context,
                "equity.ownership.institutional",
                f"S{index}/year=2025/quarter=1",
                {"symbol": f"S{index}", "year": 2025, "quarter": 1},
                ("fmp",),
            ),
            task_id=f"unavailable-{index}",
        )
        for index in range(3)
    ]
    runtime = ProviderRuntime({"fmp": 1000.0}, {"fmp": 1}, 3600)
    runtime.disable_route("fmp", "equity.ownership.institutional", "premium route")

    class Worker:
        def __init__(self) -> None:
            self.runtime = runtime

        def __call__(self, current):
            return TaskResult(
                current,
                "unavailable",
                "fmp",
                0,
                None,
                0,
                error="fmp: premium route",
            )

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks(tasks, plan_token="unavailable")
        attempted, totals = execute_download_tasks(
            context,
            manifest,
            Worker(),
            plan_token="unavailable",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=True,
            no_progress=True,
        )
        assert attempted == 1
        assert totals["unavailable"] == 1
        assert totals["bulk_unavailable"] == 2
        assert manifest.counts("unavailable") == {"unavailable": 3}
    finally:
        manifest.close()


def test_executor_persists_followups_before_parent_success(tmp_path: Path) -> None:
    events: list[str] = []

    class RecordingManifest(Manifest):
        def upsert_tasks(self, tasks, **kwargs):
            materialized = list(tasks)
            if kwargs.get("task_source") == "followup" and materialized:
                events.append("followup_upsert")
            return super().upsert_tasks(materialized, **kwargs)

        def complete(self, result):
            events.append(f"complete:{result.task.endpoint}")
            return super().complete(result)

    context = _context(tmp_path)
    context.commands.update({".index.price.historical": ["yfinance"]})
    parent = make_task(context, "index.available", "all", {}, ("fmp",))
    manifest = RecordingManifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    parent_result = TaskResult(
        parent,
        "success",
        "fmp",
        1,
        parent.output_path,
        1,
        records=[{"symbol": "^TEST"}],
    )

    def worker(task):
        if task.endpoint == "index.available":
            return parent_result
        return TaskResult(task, "empty", "yfinance", 0, None, 1)

    try:
        manifest.upsert_tasks([parent], plan_token="archive")
        events.clear()
        attempted, _ = execute_download_tasks(
            context,
            manifest,
            worker,
            plan_token="archive",
            workers=1,
            batch_size=1,
            max_tasks=None,
            max_total_attempts=20,
            no_discovery=False,
            no_progress=True,
        )
        assert attempted == 2
        assert events.index("followup_upsert") < events.index(
            "complete:index.available"
        )
        assert parent_result.records == []
    finally:
        manifest.close()
