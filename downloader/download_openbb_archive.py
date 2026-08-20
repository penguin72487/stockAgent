from __future__ import annotations

import argparse
from copy import deepcopy
import csv
from collections import deque
import gc
import gzip
import hashlib
import inspect
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    get_args,
    get_origin,
)
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

try:
    from downloader.common import SharedRateLimiter
    from downloader.openbb_credentials import apply_openbb_environment_credentials
    from downloader.openbb_archive_contracts import (
        ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST,
        FMP_MANIFEST_PAGINATED_ENDPOINTS,
        LOCAL_ONLY_ARCHIVE_DATE_FILTERS,
        PlanContractAuditor,
        is_authoritative_unavailable_evidence,
        write_contract_audit,
    )
except ModuleNotFoundError:  # Direct execution from downloader/.
    from common import SharedRateLimiter
    from openbb_credentials import apply_openbb_environment_credentials
    from openbb_archive_contracts import (
        ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST,
        FMP_MANIFEST_PAGINATED_ENDPOINTS,
        LOCAL_ONLY_ARCHIVE_DATE_FILTERS,
        PlanContractAuditor,
        is_authoritative_unavailable_evidence,
        write_contract_audit,
    )


ARCHIVE_SCHEMA_VERSION = 1
PLANNER_STATE_VERSION = 10
RESUME_MAINTENANCE_VERSION = 2

# Request-cost observations are implementation-specific.  When an adapter
# workaround changes the number of HTTP calls without changing the manifest
# task contract, reject only that endpoint's stale cost samples on restart.
# Other providers/endpoints retain their useful long-running telemetry.
ENDPOINT_REQUEST_COST_REVISIONS: dict[tuple[str, str], int] = {
    ("fmp", "equity.fundamental.metrics"): 2,
    ("fmp", "equity.fundamental.ratios"): 2,
    ("fred", "economy.fred_series"): 2,
}


def _endpoint_request_cost_revision(provider: str, endpoint: str) -> int:
    return ENDPOINT_REQUEST_COST_REVISIONS.get((provider, endpoint), 1)


DEFAULT_START_DATE = "2000-01-01"
DEFAULT_OUTPUT_DIR = Path("data_openBB")
COMPLETION_PERSISTENCE_BATCH_CAP = 256
# Normalized source records are cleared in ``execute_and_discover`` before a
# Future becomes visible to the control thread. A 512-result handoff therefore
# left half of the bounded 1,792-slot global queue unused during catalog writes
# without materially reducing the dominant follow-up-task memory. Keep enough
# live work for all provider lanes while retaining a strict finite bound; the
# supervisor's independent 16-GiB RSS guard remains the final process envelope.
COMPLETION_BACKPRESSURE_CAP = 1024
DEFAULT_ARCHIVE_THREAD_SWITCH_INTERVAL_SECONDS = 0.001
FOLLOWUP_UPSERT_CHUNK_SIZE = 8192
# A retryable response is not evidence that a task is unavailable, but it is
# also not permission to call the same URL in a tight loop.  Keep this clock in
# the manifest (rather than process memory) so supervisor and host restarts
# preserve backpressure.  The deterministic 0.8--1.0 spread prevents a wave of
# failed catalog URLs from becoming eligible in the same second.
TASK_RETRY_BASE_SECONDS = 5 * 60.0
TASK_RETRY_MAX_SECONDS = 24 * 60 * 60.0
REPAIR_QUEUE_STATUS = "repair"
CONGRESS_REPAIR_QUEUE_ENDPOINTS = frozenset(
    {"uscongress.bill_info", "uscongress.amendment_info"}
)
SEC_STATEMENT_VALIDATION_RECOVERY_REVISION = 1
SEC_STATEMENT_WRAPPER_SHARD_RECOVERY_REVISION = 1
PROVIDER_PARSER_SHAPE_RECOVERY_REVISION = 1
PROVIDER_TRANSIENT_OUTCOME_RECOVERY_REVISION = 1
UNPROVEN_PROVIDER_OUTCOME_RECOVERY_REVISION = 1
UNPROVEN_PERMANENT_OUTCOME_RECOVERY_REVISION = 1
FMP_ADAPTER_BOUNDARY_RECOVERY_REVISION = 1
HETEROGENEOUS_PARQUET_SCHEMA_RECOVERY_REVISION = 1
SEC_ALL_COMPANY_FACTS = "__all__"
BLS_SERIES_BATCH_SIZE = 50
BLS_LABSTAT_BASE_URL = "https://download.bls.gov/pub/time.series/"
BLS_LABSTAT_CACHE_SCHEMA_VERSION = 1
BLS_LABSTAT_PARALLEL_BUILDS = 2
FRED_RELEASE_PAGE_SIZE = 1000
FMP_CONSTITUENT_INDEXES = ("dowjones", "sp500", "nasdaq")
FMP_OWNERSHIP_SYMBOL_BATCH_SIZE = 50
SEC_FTD_START_DATE = date(2009, 1, 1)
SEC_INSIDER_DATASET_START_DATE = date(2006, 1, 1)
SEC_INSIDER_DATASET_CATALOG_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
)
UN_COMTRADE_REQUEST_LIMITER = SharedRateLimiter(
    1.0 / 8.0, name="un-comtrade-public-preview"
)
_ECONDB_TOKEN_LOCK = threading.Lock()
_ECONDB_CACHED_TOKEN: str | None = None
_SEC_COMPANYFACTS_LOCKS_GUARD = threading.Lock()
_SEC_COMPANYFACTS_LOCKS: dict[str, threading.Lock] = {}
_BLS_LABSTAT_LOCKS_GUARD = threading.Lock()
_BLS_LABSTAT_LOCKS: dict[str, threading.Lock] = {}
_BLS_LABSTAT_READY_DATABASES: dict[str, tuple[int, int]] = {}
# LABSTAT files are whole-survey snapshots and can be hundreds of MiB. Two
# concurrent transfers keep the network busy without multiplying peak disk and
# DuckDB import pressure across every BLS worker.
_BLS_LABSTAT_BUILD_SEMAPHORE = threading.Semaphore(BLS_LABSTAT_PARALLEL_BUILDS)
_SEC_STANDARDIZED_CACHE_GUARD = threading.Lock()
_SEC_STANDARDIZED_CACHE: dict[
    tuple[str, tuple[str, ...], str, bool, bool], tuple[Any, set[str]]
] = {}
_SEC_STANDARDIZED_CACHE_ORDER: deque[tuple[str, tuple[str, ...], str, bool, bool]] = (
    deque()
)
_SEC_STANDARDIZED_CACHE_MAX_ENTRIES = 16
_SEC_STANDARDIZED_DISK_CACHE_SCHEMA_VERSION = 2
_SEC_INSIDER_DATASET_LOCK = threading.Lock()
_SEC_INSIDER_DATASET_URLS: dict[tuple[int, int], str] | None = None
_SEC_INSIDER_BULK_SEMAPHORE = threading.Semaphore(2)
_SEC_INSIDER_REQUIRED_TABLES = frozenset(
    {
        "SUBMISSION.tsv",
        "REPORTINGOWNER.tsv",
        "NONDERIV_TRANS.tsv",
        "NONDERIV_HOLDING.tsv",
        "DERIV_TRANS.tsv",
        "DERIV_HOLDING.tsv",
        "FOOTNOTES.tsv",
        "OWNER_SIGNATURE.tsv",
    }
)

SEC_COMPANYFACTS_STATEMENT_ENDPOINTS = frozenset(
    {
        "equity.fundamental.balance",
        "equity.fundamental.balance_growth",
        "equity.fundamental.cash",
        "equity.fundamental.cash_growth",
        "equity.fundamental.income",
        "equity.fundamental.income_growth",
    }
)
# These routes project the same large local companyfacts objects and are
# CPU/GIL/cache bound after at most one SEC fetch per CIK.  Letting all 72 SEC
# execution slots run projections starves truly network-bound filing, CIK,
# N-PORT, and filing-header routes without increasing the 10 req/s HTTP rate.
# Three tasks per sibling endpoint exposes up to 18 independent projections to
# the 14-slot default CPU budget. The fair budget admits only host capacity,
# while the remaining 54 of 72 SEC slots stay available to network-bound work.
PROVIDER_ENDPOINT_CONCURRENCY_CAPS: dict[tuple[str, str], int] = {
    ("sec", endpoint): 3 for endpoint in SEC_COMPANYFACTS_STATEMENT_ENDPOINTS
}


class FairCpuSlotBudget:
    """Bound native threads plus child processes to one host CPU budget."""

    def __init__(self, total: int) -> None:
        self.total = max(1, int(total))
        self._available = self.total
        self._condition = threading.Condition()
        self._waiters: deque[tuple[object, int]] = deque()

    def acquire(self, requested: int = 1) -> int:
        count = max(1, min(self.total, int(requested)))
        token = object()
        with self._condition:
            self._waiters.append((token, count))
            try:
                while self._waiters[0][0] is not token or self._available < count:
                    self._condition.wait()
            except BaseException:
                self._waiters = deque(
                    item for item in self._waiters if item[0] is not token
                )
                self._condition.notify_all()
                raise
            self._waiters.popleft()
            self._available -= count
            self._condition.notify_all()
        return count

    def release(self, count: int = 1) -> None:
        count = max(1, min(self.total, int(count)))
        with self._condition:
            self._available = min(self.total, self._available + count)
            self._condition.notify_all()

    @contextmanager
    def claim(self, requested: int = 1) -> Iterator[None]:
        count = self.acquire(requested)
        try:
            yield
        finally:
            self.release(count)


def _local_cpu_slot_count() -> int:
    cpu_count = max(1, os.cpu_count() or 1)
    default = max(1, cpu_count - min(2, cpu_count - 1))
    raw_override = os.environ.get("OPENBB_LOCAL_CPU_SLOTS")
    if raw_override is None:
        return default
    try:
        requested = int(raw_override)
    except ValueError as exc:
        raise ValueError("OPENBB_LOCAL_CPU_SLOTS must be a positive integer") from exc
    if requested <= 0:
        raise ValueError("OPENBB_LOCAL_CPU_SLOTS must be a positive integer")
    return min(cpu_count, requested)


_LOCAL_CPU_BUDGET = FairCpuSlotBudget(_local_cpu_slot_count())
_SEC_PROCESS_THREADPOOL_LIMITER: Any | None = None


def _sec_statement_process_worker_count() -> int:
    raw_override = os.environ.get("OPENBB_SEC_PROCESS_WORKERS")
    if raw_override is None:
        return _LOCAL_CPU_BUDGET.total
    try:
        requested = int(raw_override)
    except ValueError as exc:
        raise ValueError(
            "OPENBB_SEC_PROCESS_WORKERS must be a non-negative integer"
        ) from exc
    if requested < 0:
        raise ValueError("OPENBB_SEC_PROCESS_WORKERS must be a non-negative integer")
    return min(_LOCAL_CPU_BUDGET.total, requested)


def _initialize_sec_statement_process() -> None:
    """Prevent one child from starting its own nested native thread pool."""
    global _SEC_PROCESS_THREADPOOL_LIMITER
    try:
        import ctypes
        import signal

        # Linux parent-death signal prevents workers from surviving a
        # supervisor TERM/restart of the archive downloader.
        parent_pid = os.getppid()
        ctypes.CDLL(None).prctl(1, signal.SIGTERM)
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGTERM)
    except (AttributeError, OSError):  # pragma: no cover - non-Linux fallback
        pass
    pa.set_cpu_count(1)
    try:
        from threadpoolctl import threadpool_limits

        _SEC_PROCESS_THREADPOOL_LIMITER = threadpool_limits(limits=1)
    except ImportError:
        pass


def _create_sec_statement_process_pool() -> ProcessPoolExecutor | None:
    workers = _sec_statement_process_worker_count()
    if workers <= 0:
        return None
    import multiprocessing

    return ProcessPoolExecutor(
        max_workers=workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_initialize_sec_statement_process,
    )


@contextmanager
def _sqlite_progress(
    connection: sqlite3.Connection,
    description: str,
    *,
    enabled: bool,
    position: int = 1,
    vm_steps: int = 50_000,
) -> Iterator[tqdm[Any]]:
    """Show liveness for one SQLite statement whose final row count is unknown.

    SQLite exposes virtual-machine instruction callbacks rather than a reliable
    percentage for arbitrary UPDATE/JSON scans.  Counting fixed VM batches still
    gives the operator a continuously moving elapsed/work indicator during the
    multi-million-row manifest maintenance phases.
    """
    progress = tqdm(
        total=None,
        desc=description[:64],
        unit="vm-batch",
        position=position,
        leave=False,
        mininterval=0.5,
        disable=not enabled,
    )

    def update() -> int:
        progress.update(1)
        return 0

    if enabled:
        connection.set_progress_handler(update, max(1_000, int(vm_steps)))
    try:
        yield progress
    finally:
        if enabled:
            connection.set_progress_handler(None, 0)
        progress.close()


# OpenBB advertises every installed provider in command coverage even when a
# provider cannot make any request without an account key.  Keep the mapping
# intentionally narrow: providers not listed here may have anonymous routes.
REQUIRED_PROVIDER_CREDENTIALS = {
    "benzinga": "benzinga_api_key",
    "intrinio": "intrinio_api_key",
}

# These routes are observations of "now" rather than archives.  Slow-changing
# metadata (profiles, ETF info, holders, share statistics) is deliberately not
# in this set and is saved once with _retrieved_at.
SNAPSHOT_ENDPOINTS = frozenset(
    {
        "currency.snapshots",
        "equity.discovery.active",
        "equity.discovery.aggressive_small_caps",
        "equity.discovery.gainers",
        "equity.discovery.growth_tech",
        "equity.discovery.latest_financial_reports",
        "equity.discovery.losers",
        "equity.discovery.undervalued_growth",
        "equity.discovery.undervalued_large_caps",
        "equity.market_snapshots",
        "equity.price.performance",
        "equity.price.quote",
        "equity.screener",
        "etf.price_performance",
        "regulators.sec.rss_litigation",
    }
)

# The user requested document metadata only.  The corresponding discovery/list
# endpoints remain enabled; only body/file download routes are excluded.
DOCUMENT_BODY_ENDPOINTS = frozenset(
    {
        "commodity.weather_bulletins_download",
        "equity.fundamental.management_discussion_analysis",
        "equity.fundamental.transcript",
        "regulators.sec.htm_file",
        "regulators.sec.schema_files",
        "uscongress.amendment_text",
        "uscongress.bill_text",
    }
)

DISCOVERY_ONLY_ENDPOINTS = frozenset(
    {
        "cftc.cot",
        "economy.fred_series",
        "economy.fred_release_table",
        "economy.indicators",
        "economy.survey.bls_series",
        "equity.fundamental.latest_attributes",
        "equity.fundamental.historical_attributes",
        "regulators.sec.filing_headers",
        "regulators.sec.symbol_map",
        "uscongress.amendment_info",
        "uscongress.bill_info",
    }
)

NOT_ENUMERABLE_ENDPOINTS: dict[str, str] = {
    "commodity.psd_report": "requires an open-ended commodity identifier plus year/month",
    "economy.fred_regional": "requires regional-series group metadata not exposed as a complete OpenBB catalog",
    "equity.fundamental.search_attributes": "arbitrary text search and currently Intrinio-only",
    "equity.ownership.form_13f": "requires an institutional-manager ticker/CIK universe, not the equity security universe",
    "regulators.sec.institutions_search": "arbitrary institution query has no finite complete universe",
    "regulators.sec.sic_search": "arbitrary SIC text search has no finite complete query universe",
    "uscongress.committee_documents": "requires committee system codes; OpenBB exposes chambers but no complete committee-code catalog",
    "uscongress.committee_info": "requires committee system codes; OpenBB exposes chambers but no complete committee-code catalog",
}

HISTORICAL_PRICE_ENDPOINTS = frozenset(
    {
        "currency.price.historical",
        "equity.price.historical",
        "etf.historical",
        "index.price.historical",
    }
)

STATEMENT_ENDPOINTS = frozenset(
    {
        "equity.fundamental.balance",
        "equity.fundamental.balance_growth",
        "equity.fundamental.cash",
        "equity.fundamental.cash_growth",
        "equity.fundamental.income",
        "equity.fundamental.income_growth",
        "equity.fundamental.metrics",
        "equity.fundamental.ratios",
    }
)

OPTIONAL_SYMBOL_ENDPOINTS = frozenset(
    {
        "equity.compare.company_facts",
        "equity.estimates.consensus",
        "equity.estimates.forward_ebitda",
        "equity.estimates.forward_eps",
        "equity.estimates.price_target",
        "equity.fundamental.filings",
        "news.company",
    }
)

GLOBAL_YEAR_CHUNK_ENDPOINTS = frozenset(
    {
        "equity.calendar.dividend",
        "equity.calendar.earnings",
        "equity.calendar.events",
        "equity.calendar.ipo",
        "equity.calendar.splits",
        "news.world",
    }
)

FMP_WORLD_NEWS_TOPICS = ("general", "press_releases", "stocks", "forex")

SYMBOL_YEAR_CHUNK_ENDPOINTS = frozenset(
    {
        "news.company",
    }
)

# FMP's institutional-ownership routes silently default to the latest completed
# quarter.  Archive them as explicit quarter checkpoints so a retry never needs
# to repeat decades of history and the manifest can prove every period exists.
FMP_QUARTERLY_OWNERSHIP_ENDPOINTS = frozenset(
    {
        "equity.ownership.institutional",
        "equity.ownership.major_holders",
    }
)

# Routes whose no-date form is a current snapshot, but whose date argument can
# be used to build a historical archive.  The frequency follows the native data
# cadence to avoid downloading the same release thousands of times.
DATE_GRID_ENDPOINTS: dict[str, str] = {
    "economy.central_bank_holdings": "weekly_wednesday",
    "economy.pce": "month_start",
    "economy.survey.nonfarm_payrolls": "month_start",
    "fixedincome.corporate.hqm": "month_start",
    "fixedincome.government.treasury_prices": "weekdays",
    "fixedincome.government.yield_curve": "weekdays",
}

ARCHIVE_TIME_SHARDED_ENDPOINTS = frozenset(
    set(GLOBAL_YEAR_CHUNK_ENDPOINTS)
    | set(SYMBOL_YEAR_CHUNK_ENDPOINTS)
    | set(DATE_GRID_ENDPOINTS)
    | {
        "economy.calendar",
        "equity.discovery.filings",
        "equity.fundamental.filings",
    }
)


def _validate_archive_time_shard_contract() -> None:
    """Fail closed when planning gains an unaudited historical shard route."""
    missing = ARCHIVE_TIME_SHARDED_ENDPOINTS - set(
        ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST
    )
    extra = set(ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST) - set(
        ARCHIVE_TIME_SHARDED_ENDPOINTS
    )
    invalid = {
        (endpoint, provider)
        for endpoint, provider in LOCAL_ONLY_ARCHIVE_DATE_FILTERS
        if provider in ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST.get(endpoint, frozenset())
    }
    if missing or extra or invalid:
        raise RuntimeError(
            "invalid archive time-shard provider contract: "
            f"missing={sorted(missing)}, extra={sorted(extra)}, "
            f"local_only={sorted(invalid)}"
        )


_validate_archive_time_shard_contract()

DATE_GRID_LITERAL_FIELDS: dict[str, str] = {
    "economy.pce": "category",
    "economy.survey.nonfarm_payrolls": "category",
}

ENUMERATE_LITERAL_FIELD: dict[str, str] = {
    "commodity.petroleum_status_report": "category",
    "commodity.short_term_energy_outlook": "table",
    "economy.balance_of_payments": "country",
    "economy.shipping.port_info": "country",
    "economy.shipping.port_volume": "country",
    "economy.survey.bls_search": "category",
    "economy.survey.sloos": "category",
    "economy.survey.manufacturing_outlook_ny": "topic",
    "economy.survey.manufacturing_outlook_texas": "topic",
    "etf.search": "exchange",
    "fixedincome.rate.ecb": "interest_rate_type",
    "fixedincome.rate.sonia": "parameter",
    "fixedincome.government.treasury_auctions": "security_type",
}

# ``country=all`` is not a portable OpenBB convention.  It is a real aggregate
# selector only on these macro-data routes; on filters such as ``etf.search``
# the merged command schema merely exposes an optional country string and FMP
# rejects the literal before making an HTTP request.  Keep this as an allowlist
# so newly added markets/endpoints cannot inherit an invalid filter by default.
COUNTRY_ALL_ENDPOINTS = frozenset(
    {
        "economy.calendar",
        "economy.composite_leading_indicator",
        "economy.cpi",
        "economy.gdp.forecast",
        "economy.gdp.nominal",
        "economy.gdp.real",
        "economy.house_price_index",
        "economy.interest_rates",
        "economy.share_price_index",
        "economy.unemployment",
    }
)

CONGRESS_BILL_TYPES = (
    "hr",
    "s",
    "hjres",
    "sjres",
    "hconres",
    "sconres",
    "hres",
    "sres",
)
CONGRESS_AMENDMENT_TYPES = ("hamdt", "samdt", "suamdt")

CFTC_REPORT_MODES: tuple[tuple[str, bool], ...] = (
    ("legacy", False),
    ("legacy", True),
    ("disaggregated", False),
    ("disaggregated", True),
    ("financial", False),
    ("financial", True),
    ("supplemental", False),
)

MORTGAGE_INDEX_GROUPS = ("primary", "ltv_lte_80", "ltv_gt_80")

GOVERNMENT_YIELD_CURVE_TYPES = (
    "nominal",
    "real",
    "breakeven",
    "treasury_minus_fed_funds",
    "corporate_spot",
    "corporate_par",
)

# These adapters download their complete upstream series before OpenBB applies
# the requested date filter locally.  Planning one task per date would multiply
# identical network transfers by every business day in the archive.  FMP is
# deliberately absent: its adapter puts a bounded window around every date in
# the actual request URL and therefore remains date-sharded when it is the only
# available provider.
FULL_HISTORY_YIELD_CURVE_PROVIDERS = frozenset({"econdb", "federal_reserve", "fred"})

UNEMPLOYMENT_DIMENSIONS: tuple[tuple[str, str, bool], ...] = tuple(
    (sex, age, seasonal_adjustment)
    for sex in ("total", "male", "female")
    for age in ("total", "15-24", "25+")
    for seasonal_adjustment in (False, True)
)
GDP_FORECAST_DIMENSIONS: tuple[tuple[str, str], ...] = tuple(
    (frequency, units)
    for frequency in ("annual", "quarter")
    for units in ("current_prices", "volume", "capita", "growth", "deflator")
    # OECD silently substitutes annual data for quarterly per-capita data.
    if not (frequency == "quarter" and units == "capita")
)
GDP_NOMINAL_DIMENSIONS: tuple[tuple[str, str, str], ...] = tuple(
    (frequency, units, price_base)
    for frequency in ("quarter", "annual")
    for units in ("level", "index", "capita")
    for price_base in ("current_prices", "volume")
)
GDP_REAL_FREQUENCIES = ("quarter", "annual")
RETAIL_PRICE_REGIONS = ("all_city", "northeast", "midwest", "south", "west")
INTEREST_RATE_DURATIONS = ("immediate", "short", "long")
TOTAL_FACTOR_PRODUCTIVITY_FREQUENCIES = ("quarter", "annual", "summary")
COMPOSITE_LEADING_INDICATOR_ADJUSTMENTS = ("amplitude", "normalized")
CPI_HARMONIZED_MODES = (False, True)
DWPCR_RAW_PARAMETERS = ("daily_excl_weekend", "daily")
PRIMARY_DEALER_POSITION_GROUPS = (
    "treasuries",
    "mbs",
    "municipal",
    "corporate",
    "abs",
)
HOUSE_PRICE_DIMENSIONS: tuple[tuple[str, str], ...] = tuple(
    (frequency, transform)
    for frequency in ("monthly", "quarter", "annual")
    for transform in ("index", "yoy", "period")
)
SPREAD_MATURITIES: dict[str, tuple[str, ...]] = {
    "fixedincome.spreads.tcm": ("3m", "2y"),
    "fixedincome.spreads.tcm_effr": ("10y", "5y", "1y", "6m", "3m"),
    "fixedincome.spreads.treasury_effr": ("3m", "6m"),
}

SYMBOL_PERIOD_ENDPOINTS = frozenset(
    {
        "equity.estimates.historical",
        "equity.fundamental.revenue_per_geography",
        "equity.fundamental.revenue_per_segment",
    }
)
FORWARD_ESTIMATE_ENDPOINTS = frozenset(
    {
        "equity.estimates.forward_ebitda",
        "equity.estimates.forward_eps",
    }
)


@dataclass(frozen=True, slots=True)
class ProviderRatePolicy:
    """Auditable upstream pacing rule for one independently limited provider."""

    requests: float
    seconds: float
    basis: str
    source_url: str
    quota_note: str = ""

    @property
    def requests_per_second(self) -> float:
        return max(0.001, float(self.requests) / float(self.seconds))


# The operator contract is intentionally explicit: use the documented
# instantaneous/sustained ceiling when one exists; otherwise use 10 req/s.
# Hourly/daily allocations do not imply a lower instantaneous rate.  Those are
# enforced independently through durable quota cooldowns when the provider
# reports exhaustion.
DEFAULT_UNDOCUMENTED_PROVIDER_RPS = 8.0
PROVIDER_RATE_POLICIES: dict[str, ProviderRatePolicy] = {
    "benzinga": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no numeric request rate",
        "https://docs.benzinga.com/api-reference/errors",
    ),
    "bls": ProviderRatePolicy(
        50,
        10,
        "official maximum of 50 requests per 10 seconds",
        "https://www.bls.gov/developers/api_faqs.htm",
        "Registered v2 users also have a 500-query daily allocation.",
    ),
    "cftc": ProviderRatePolicy(
        8,
        1,
        "operator default; CFTC Public Reporting publishes no numeric rate",
        "https://publicreporting.cftc.gov/stories/s/Public-Reporting-FAQ/inwp-fmhz/",
    ),
    "congress_gov": ProviderRatePolicy(
        5000,
        3600,
        "official API-key allocation of 5,000 requests per hour",
        "https://github.com/LibraryOfCongress/api.congress.gov/",
    ),
    "econdb": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no numeric request rate",
        "https://developers.econdb.com/docs/",
    ),
    "eia": ProviderRatePolicy(
        9000,
        3600,
        "official sustained ceiling; also below the documented 5 req/s burst ceiling",
        "https://www.eia.gov/opendata/faqs.php",
    ),
    "federal_reserve": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no numeric request rate",
        "https://www.federalreserve.gov/data.htm",
    ),
    "fmp": ProviderRatePolicy(
        8,
        1,
        "operator default for Basic; official page publishes only its daily allocation",
        "https://site.financialmodelingprep.com/pricing-plans",
        "Basic has 250 requests/day; paid-plan minute rates can use --provider-rps.",
    ),
    "fred": ProviderRatePolicy(
        2,
        1,
        "OpenBB FRED provider ceiling; FRED confirms 429 throttling but publishes no numeric limit",
        "https://fred.stlouisfed.org/docs/api/fred/errors.html",
    ),
    "government_us": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no numeric request rate",
        "https://fiscaldata.treasury.gov/api-documentation/",
    ),
    "imf": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no numeric request rate",
        "https://data.imf.org/en/Resource-Pages/IMF-API",
    ),
    "intrinio": ProviderRatePolicy(
        100,
        1,
        "official free-feed throttle for ordinary calls; large-page and bulk calls are route-specific",
        "https://docs.intrinio.com/documentation/api_v2/limits",
        "page_size > 100 and bulk calls are 1/minute for free or 1/second for paid accounts.",
    ),
    "intrinio_large_page": ProviderRatePolicy(
        1,
        60,
        "official free-account throttle for page_size > 100 and bulk calls",
        "https://docs.intrinio.com/documentation/api_v2/limits",
        "Paid accounts may override this route bucket to 1 req/s with --provider-rps intrinio_large_page=1.",
    ),
    "oecd": ProviderRatePolicy(
        60,
        3600,
        "official maximum of 60 data downloads per hour",
        "https://www.oecd.org/en/data/insights/data-explainers/2024/11/Api-best-practices-and-recommendations.html",
    ),
    "sec": ProviderRatePolicy(
        10,
        1,
        "official SEC fair-access ceiling",
        "https://www.sec.gov/about/developer-resources",
    ),
    "tiingo": ProviderRatePolicy(
        8,
        1,
        "operator smoothing default; upstream states no per-second or per-minute limit",
        "https://www.tiingo.com/documentation/general",
        "Starter publishes 50/hour and 1,000/day; Power publishes 10,000/hour and 100,000/day.",
    ),
    "tradingeconomics": ProviderRatePolicy(
        2,
        1,
        "official general request ceiling",
        "https://docs.tradingeconomics.com/get_started/rate-limits/",
    ),
    "un_comtrade": ProviderRatePolicy(
        8,
        1,
        "operator default; upstream publishes no sustained numeric request rate",
        "https://uncomtrade.org/docs/what-is-data-preview/",
        "Anonymous preview is limited to 500 records; its 429 message requests a one-second retry, which is backoff rather than a published 1 req/s ceiling.",
    ),
    "yfinance": ProviderRatePolicy(
        8,
        1,
        "operator default; Yahoo has no supported public Finance API limit",
        "https://finance.yahoo.com/",
    ),
}

DEFAULT_PROVIDER_RPS: dict[str, float] = {
    provider: policy.requests_per_second
    for provider, policy in PROVIDER_RATE_POLICIES.items()
}

# Hard allocation ceilings that can be translated into a completion-time lower
# bound.  These are separate from RPS: concurrency cannot make a daily account
# budget larger.  BLS batching in this archive uses the registered-v2 contract
# (50 series, 20 years), whose account allocation is 500 queries/day.
PROVIDER_DECLARED_DAILY_REQUEST_CAPS: dict[str, int] = {
    "bls": 500,
    "fmp": 250,
    # The configured credential currently exhibits the free Starter daily
    # allocation. This is an ETA/accounting floor, not a local request gate;
    # an upgraded account remains free to run until its authoritative server
    # response and can override the monitor policy in a future plan profile.
    "tiingo": 1000,
}
PROVIDER_DECLARED_HOURLY_REQUEST_CAPS: dict[str, int] = {
    "congress_gov": 5000,
    "eia": 9000,
    "oecd": 60,
    "tiingo": 50,
}

# RPS is a request-start ceiling, not a concurrency setting. Concurrency follows
# Little's Law covers real HTTP/parse service time.  Manifest refill latency is
# covered separately by the scheduler's claimed in-memory provider buffer; it
# must not be converted into extra threads waiting on limiter tickets.
# SEC child HTTP starts are protected separately by the hard 10 req/s boundary
# limiter, and its persistent HTTP cache is disabled by the archive worker so
# a limiter claim always corresponds to a real network request.
DEFAULT_PROVIDER_LATENCY_HORIZON_SECONDS = 3.5
DEFAULT_PROVIDER_CONCURRENCY_CAP = 28
ADAPTIVE_PROVIDER_LATENCY_HORIZON_SECONDS = 15.0
ADAPTIVE_PROVIDER_CONCURRENCY_CAP = 128
DEFAULT_PROVIDER_CONCURRENCY: dict[str, int] = {
    provider: max(
        1,
        min(
            DEFAULT_PROVIDER_CONCURRENCY_CAP,
            math.ceil(rps * DEFAULT_PROVIDER_LATENCY_HORIZON_SECONDS),
        ),
    )
    for provider, rps in DEFAULT_PROVIDER_RPS.items()
}
# SEC tasks can fan out to several filing child requests after one outer call.
# The child starts still share the exact 10 req/s limiter. Live archive
# telemetry measured roughly 6.6 seconds of outer-task residence time, so 72
# outer slots keep that request queue supplied while leaving room for heavier
# XML/XBRL parsing.
DEFAULT_PROVIDER_CONCURRENCY["sec"] = 72
# FRED's mixed observation/catalog workload measured about 3.6 seconds of
# residence time. Seven is the rounded 3.5-second baseline; one extra slot
# avoids systematically undersupplying its independent 2 req/s limiter.
DEFAULT_PROVIDER_CONCURRENCY["fred"] = 8
# Congress already sustains its exact 5,000/hour cadence with four calls.
# Avoid spending a fifth thread on a provider whose binding limit is met.
DEFAULT_PROVIDER_CONCURRENCY["congress_gov"] = 4
# Intrinio ordinary free-feed calls are documented at 100 req/s.  This source
# is disabled without credentials; when enabled, retain enough service slots
# to make that independent limit reachable without changing other providers.
DEFAULT_PROVIDER_CONCURRENCY["intrinio"] = 100
# One OECD request per minute cannot benefit from parallel response bodies.
DEFAULT_PROVIDER_CONCURRENCY["oecd"] = 1

OFFICIAL_PROVIDER_PRIORITY = (
    "cftc",
    "congress_gov",
    "sec",
    "federal_reserve",
    "government_us",
    "eia",
    "imf",
    "oecd",
    "fred",
    "econdb",
    "fmp",
    "yfinance",
    "benzinga",
    "intrinio",
    "tradingeconomics",
    "bls",
    "tiingo",
)

DOCUMENT_BODY_KEYS = frozenset(
    {
        "body",
        "content",
        "full_text",
        "html",
        "markdown",
        "raw_content",
        "text",
        "transcript",
    }
)

AUTH_ERROR_MARKERS = (
    "api key",
    "apikey",
    "authentication",
    "invalid key",
    "invalid token",
    "missing credential",
    "unauthorized",
)
RATE_ERROR_MARKERS = (
    "daily limit",
    "daily threshold",
    "limit reach",
    "quota",
    "rate limit",
    "request allocation",
    "too many requests",
)
STRONG_RATE_ERROR_MARKERS = (
    "daily limit",
    "daily threshold",
    "limit reach",
    "quota exceeded",
    "request allocation",
    "threshold for total number of requests",
    "too many requests",
)
TRANSIENT_ERROR_MARKERS = (
    "cannot connect",
    "cannot operate on a closed database",
    "connect call failed",
    "connection",
    "could not resolve host",
    "database is locked",
    "database disk image is malformed",
    "dnserror",
    "failed to resolve",
    "gateway",
    "internal server",
    "incompleteread",
    "name resolution",
    "nameresolutionerror",
    "network",
    "readonly database",
    "server error",
    "temporarily",
    "temporary failure",
    "timeout",
    "timed out",
    "failed to download",
)
EMPTY_ERROR_MARKERS = (
    "after the latest available data",
    "could not find cik for symbol",
    "empty",
    "no dividend data found",
    "no executive data found",
    "no form 4 data was returned",
    "no quarterly filing dates found",
    "no data",
    "not found",
    "is not an etf",
    "only period='annual' is available",
    "results not found",
    "returned empty",
    "unexpected response from sec for cik",
)


def _has_strong_rate_evidence(text: str) -> bool:
    """Return whether an error contains an explicit quota/rate-limit signal."""
    lowered = text.lower()
    return any(marker in lowered for marker in STRONG_RATE_ERROR_MARKERS) or bool(
        re.search(r"(?<!\d)429(?!\d)", lowered)
    )


def _has_invalid_credential_evidence(text: str) -> bool:
    """Return whether an upstream response explicitly rejects its API key."""
    lowered = text.lower()
    return (
        ("key provided" in lowered and "invalid" in lowered)
        or "invalid api key" in lowered
        or "invalid registration key" in lowered
        or "invalid token" in lowered
        or "invalid credential" in lowered
    )


DNS_ERROR_MARKERS = (
    "could not resolve host",
    "dnserror",
    "failed to resolve",
    "name resolution",
    "nameresolutionerror",
)


def _has_dns_error_evidence(text: str) -> bool:
    """Return whether an error is a resolver/network failure, not a data fact."""
    lowered = text.lower()
    return any(marker in lowered for marker in DNS_ERROR_MARKERS)


def _provider_has_dns_error(error: str, provider: str) -> bool:
    """Match DNS evidence to one provider inside a combined fallback error."""
    provider_prefix = f"{provider.lower()}:"
    return any(
        segment.strip().lower().startswith(provider_prefix)
        and _has_dns_error_evidence(segment)
        for segment in str(error or "").split(" | ")
    )


@dataclass(frozen=True, slots=True)
class AssetRecord:
    symbol: str
    name: str
    market: str
    security_type: str


@dataclass(frozen=True, slots=True)
class DownloadTask:
    task_id: str
    endpoint: str
    category: str
    scope_key: str
    kwargs: dict[str, Any]
    providers: tuple[str, ...]
    output_path: str
    # Durable per-provider terminal outcomes let a fallback chain make forward
    # progress even when another provider is cooling down.  Without this, a
    # task such as SEC -> FMP either has to wait for FMP before trying SEC, or
    # repeat an authoritative SEC empty response after every FMP cooldown.
    provider_outcomes: dict[str, str] = field(default_factory=dict, compare=False)
    # Terminal capability claims need their positive evidence to survive
    # fallback deferrals and process restarts.  A categorical ``unavailable``
    # outcome alone cannot distinguish a stable subscription restriction from
    # a transient request failure.
    provider_evidence: dict[str, str] = field(default_factory=dict, compare=False)
    # These scheduling fields are observations, not part of task identity.
    # ``attempts`` remains the lifetime request audit counter, while
    # ``transient_failures`` is the consecutive task-local failure streak used
    # exclusively for durable exponential backoff.
    attempts: int = field(default=0, compare=False)
    transient_failures: int = field(default=0, compare=False)


@dataclass(slots=True)
class TaskResult:
    task: DownloadTask
    status: str
    provider: str | None
    rows: int
    output_path: str | None
    attempts: int
    error: str | None = None
    records: list[dict[str, Any]] = field(default_factory=list, repr=False)
    followups: list[DownloadTask] = field(default_factory=list, repr=False)
    provider_outcomes: dict[str, str] = field(default_factory=dict, repr=False)
    provider_evidence: dict[str, str] = field(default_factory=dict, repr=False)
    retry_not_before: str | None = None
    transient_failures: int = 0


@dataclass(frozen=True, slots=True)
class ColumnarTaskPayload:
    """A provider result already normalized as an Arrow table.

    Keeping this marker distinct from ordinary OpenBB results prevents the
    worker from materializing a large Python ``list[dict]`` only to convert it
    back to Arrow during Parquet publication.
    """

    table: pa.Table


@dataclass(slots=True)
class CoverageDecision:
    endpoint: str
    category: str
    available_providers: str
    selected_providers: str
    decision: str
    reason: str
    initial_task_count: int = 0


@dataclass(slots=True)
class PlannerContext:
    schemas: Mapping[str, Mapping[str, Any]]
    commands: Mapping[str, Sequence[str]]
    output_dir: Path
    start_date: str
    end_date: str
    assets: list[AssetRecord]
    etfs: list[AssetRecord]
    currencies: list[str]
    indices: list[str]
    countries: list[str]
    allowed_providers: set[str] | None
    disabled_providers: set[str]
    endpoint_filters: tuple[str, ...]
    categories: set[str] | None
    metadata_only: bool = True
    show_progress: bool = False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a resumable OpenBB Parquet archive from 2000 onward. "
            "Crypto, derivatives, real-time snapshots, and document bodies are excluded."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Canonical OpenBB credential dotenv file (default: repository .env)",
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default="today")
    parser.add_argument(
        "--markets", default="us,tw", help="Comma-separated equity universes: us,tw"
    )
    parser.add_argument(
        "--categories",
        default="",
        help="Optional comma-separated OpenBB top-level categories",
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        help="Optional endpoint or endpoint-prefix filter; repeatable.",
    )
    parser.add_argument(
        "--providers",
        default="",
        help="Optional comma-separated provider allow-list. Tiingo remains fallback-only.",
    )
    parser.add_argument("--disable-provider", action="append", default=[])
    parser.add_argument(
        "--limit-symbols",
        type=int,
        default=None,
        help="Smoke-test cap per symbol universe",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(256, min(1792, (os.cpu_count() or 4) * 112)),
        help=(
            "Global I/O worker count. Provider-specific RPS and concurrency "
            "semaphores remain authoritative (default: 112x CPU, capped at 1792)."
        ),
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Stop after this many attempted tasks",
    )
    parser.add_argument(
        "--max-retries", type=int, default=3, help="Retries per provider within one run"
    )
    parser.add_argument(
        "--max-total-attempts",
        type=int,
        default=20,
        help=(
            "Compatibility/monitoring threshold for unusually repeated tasks. "
            "Transient tasks remain schedulable regardless of this value; only "
            "evidence-backed terminal provider outcomes stop retries."
        ),
    )
    parser.add_argument("--base-backoff", type=float, default=2.0)
    parser.add_argument("--max-backoff", type=float, default=120.0)
    parser.add_argument("--quota-cooldown", type=float, default=3600.0)
    parser.add_argument(
        "--provider-rps",
        action="append",
        default=[],
        metavar="PROVIDER=RPS",
        help="Override a provider request rate; repeatable (for example fmp=10).",
    )
    parser.add_argument(
        "--provider-concurrency",
        action="append",
        default=[],
        metavar="PROVIDER=N",
        help="Override concurrent calls for one provider; repeatable.",
    )
    parser.add_argument(
        "--bls-api-only",
        action="store_true",
        help=(
            "Disable the official LABSTAT bulk-file path and use only the "
            "quota-limited BLS Public Data API."
        ),
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Create catalog/manifest without API calls",
    )
    parser.add_argument(
        "--resume-existing-plan",
        action="store_true",
        help=(
            "Resume a previously audited plan without re-enumerating millions "
            "of initial tasks. Falls back to a full plan if any plan, date, "
            "credential, coverage, or planner-version check fails."
        ),
    )
    parser.add_argument(
        "--no-discovery",
        action="store_true",
        help="Do not add follow-up tasks from catalogs",
    )
    parser.add_argument(
        "--retry-failed", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--retry-repair-queue",
        action="store_true",
        help=(
            "Requeue task-local upstream failures that were parked outside the "
            "main scheduler. A repeated Congress.gov HTTP 500 is attempted once "
            "and parked again, so this is intended for a later repair pass."
        ),
    )
    parser.add_argument(
        "--retry-permanent",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Retry provider outcomes classified as permanent. Disabled by "
            "default so a subscription/unsupported route is not retried on "
            "every supervisor restart."
        ),
    )
    parser.add_argument("--retry-empty", action="store_true")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-run successful tasks and atomically replace files",
    )
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args(argv)


def _parse_csv_set(value: str) -> set[str] | None:
    result = {item.strip().lower() for item in value.split(",") if item.strip()}
    return result or None


def _parse_positive_overrides(
    items: Sequence[str], *, integer: bool = False
) -> dict[str, float | int]:
    result: dict[str, float | int] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Expected PROVIDER=VALUE, received: {item}")
        provider, raw_value = item.split("=", 1)
        provider = provider.strip().lower()
        value: float | int = int(raw_value) if integer else float(raw_value)
        if not provider or value <= 0:
            raise ValueError(f"Provider overrides must be positive: {item}")
        result[provider] = value
    return result


def _resolve_end_date(value: str) -> str:
    if value.strip().lower() in {"today", "now"}:
        return date.today().isoformat()
    return value.strip()


def _validate_dates(start_date: str, end_date: str) -> None:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError(f"start date {start_date} is after end date {end_date}")


def _safe_secret_present(value: Any) -> bool:
    if value is None:
        return False
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    return bool(str(value).strip())


def configured_credential_names(obb: Any) -> set[str]:
    """Return configured credential field names without exposing their values."""
    try:
        values = obb.user.credentials.model_dump()
    except Exception:
        return set()
    return {str(key) for key, value in values.items() if _safe_secret_present(value)}


def providers_missing_required_credentials(
    credential_names: set[str],
) -> set[str]:
    """Return providers that are installed but unusable without a key."""
    return {
        provider
        for provider, credential in REQUIRED_PROVIDER_CREDENTIALS.items()
        if credential not in credential_names
    }


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [
            {str(k): str(v or "") for k, v in row.items()}
            for row in csv.DictReader(handle)
        ]


ETF_NAME_PATTERN = re.compile(
    r"(?:\bETF\b|\bETN\b|exchange[- ]traded|iShares|SPDR|Vanguard .* ETF|"
    r"WisdomTree|ProShares|Direxion|Global X|VanEck|First Trust .* ETF)",
    flags=re.IGNORECASE,
)


def _preferred_tw_symbol(raw_yahoo_symbol: str, inferred_symbol: str) -> str:
    """Choose one Yahoo symbol, preferring the official TWSE/TPEx market suffix."""
    candidates = [
        item.strip().upper() for item in raw_yahoo_symbol.split(",") if item.strip()
    ]
    inferred = inferred_symbol.strip().upper()
    if inferred in candidates:
        return inferred
    return candidates[0] if len(candidates) == 1 else inferred


def load_asset_universe(
    markets: set[str], limit_symbols: int | None = None
) -> list[AssetRecord]:
    records: dict[str, AssetRecord] = {}
    fallback_names = {
        row.get("yahoo_symbol", "").strip().upper(): row.get("name", "").strip()
        for row in _read_csv_rows(Path("configs/fallback_us_stocks_symbols.csv"))
        if row.get("yahoo_symbol", "").strip()
    }

    if "us" in markets:
        for row in _read_csv_rows(Path("data_yahoo/us_stocks/symbols.csv")):
            symbol = (row.get("yahoo_symbol") or row.get("code") or "").strip().upper()
            if not symbol:
                continue
            name = (row.get("name") or fallback_names.get(symbol) or symbol).strip()
            if name == symbol and symbol in fallback_names:
                name = fallback_names[symbol]
            security_type = "etf" if ETF_NAME_PATTERN.search(name) else "stock"
            records.setdefault(symbol, AssetRecord(symbol, name, "us", security_type))

    if "tw" in markets:
        yahoo_map = {
            row.get("code", "").strip().upper(): row.get("yahoo_symbol", "")
            .strip()
            .upper()
            for row in _read_csv_rows(Path("data_yahoo/tw_stocks/symbols.csv"))
            if row.get("code", "").strip()
        }
        for row in _read_csv_rows(Path("data_tw_public/stocks/symbols.csv")):
            code = row.get("code", "").strip().upper()
            if not code:
                continue
            market = row.get("market", "").strip().lower()
            inferred = f"{code}.TWO" if market == "tpex" else f"{code}.TW"
            symbol = _preferred_tw_symbol(yahoo_map.get(code, ""), inferred)
            security_type = row.get("security_type", "stock").strip().lower() or "stock"
            records.setdefault(
                symbol,
                AssetRecord(
                    symbol, row.get("name", "").strip() or code, "tw", security_type
                ),
            )

    ordered = sorted(records.values(), key=lambda item: (item.market, item.symbol))
    if limit_symbols is None:
        return ordered
    by_market: list[AssetRecord] = []
    for market in sorted(markets):
        by_market.extend(
            [item for item in ordered if item.market == market][: max(0, limit_symbols)]
        )
    return by_market


def load_currency_universe(limit_symbols: int | None = None) -> list[str]:
    values: set[str] = set()
    for row in _read_csv_rows(Path("data_yahoo/forex/symbols.csv")):
        raw = (row.get("yahoo_symbol") or row.get("code") or "").strip().upper()
        raw = raw.removesuffix("=X").replace("-", "")
        if len(raw) == 6 and raw.isalpha():
            values.add(raw)
    ordered = sorted(values)
    return ordered if limit_symbols is None else ordered[: max(0, limit_symbols)]


def load_country_codes() -> list[str]:
    try:
        from babel import Locale

        territories = Locale.parse("en").territories
        codes = sorted(
            code.lower() for code in territories if len(code) == 2 and code.isalpha()
        )
        if codes:
            return codes
    except Exception:
        pass
    return [
        "au",
        "br",
        "ca",
        "ch",
        "cn",
        "de",
        "es",
        "fr",
        "gb",
        "hk",
        "id",
        "in",
        "it",
        "jp",
        "kr",
        "mx",
        "nl",
        "nz",
        "se",
        "sg",
        "tw",
        "us",
        "za",
    ]


def _econdb_country_codes() -> list[str]:
    """Return the finite country/region universe accepted by EconDB."""
    try:
        from openbb_econdb.utils.helpers import COUNTRY_MAP

        values = {
            str(value).strip().lower()
            for value in COUNTRY_MAP.values()
            if str(value).strip() and str(value).strip().lower() != "w00"
        }
        if values:
            return sorted(values)
    except (ImportError, AttributeError, TypeError):
        pass
    # OpenBB's EconDB convention uses UK instead of the ISO GB code.
    return [
        "au",
        "br",
        "ca",
        "ch",
        "cn",
        "de",
        "es",
        "fr",
        "hk",
        "in",
        "it",
        "jp",
        "kr",
        "uk",
        "us",
    ]


@lru_cache(maxsize=1)
def _econdb_yield_curve_dimensions() -> tuple[tuple[str, str], ...]:
    """Return every EconDB curve dataset with its native archive cadence.

    OpenBB exposes these values through a provider-only ``country`` field.
    Some values are sovereign countries while the ECB entries are distinct
    spot/par/forward curve datasets.  Treating ``country=all`` as a portable
    fallback is invalid: EconDB rejects it and the other providers silently
    ignore the field.
    """
    try:
        from openbb_econdb.utils.yield_curves import DAILY, MONTHLY

        values = [
            *((str(value), "weekdays") for value in DAILY),
            *((str(value), "month_start") for value in MONTHLY),
        ]
        if values:
            return tuple(dict.fromkeys(values))
    except (ImportError, AttributeError, TypeError):
        pass
    return (("united_states", "weekdays"),)


def _imf_direction_countries() -> list[str]:
    """Return IMF direction-of-trade labels without parsing quoted prose."""
    try:
        from openbb_imf.utils.dot_helpers import get_label_to_code_map

        values = [
            str(value).strip()
            for value in get_label_to_code_map()
            if str(value).strip()
        ]
        if values:
            return list(dict.fromkeys(values))
    except (ImportError, AttributeError, TypeError):
        pass
    return []


def _endpoint_matches(endpoint: str, filters: Sequence[str]) -> bool:
    if not filters:
        return True
    return any(
        endpoint == value or endpoint.startswith(f"{value}.") for value in filters
    )


def provider_order(endpoint: str, available: Sequence[str]) -> tuple[str, ...]:
    providers = list(dict.fromkeys(str(item).lower() for item in available))
    if endpoint in HISTORICAL_PRICE_ENDPOINTS:
        preferred = ("yfinance", "fmp", "intrinio", "tiingo")
    elif endpoint.startswith("regulators.sec") or endpoint in {
        "equity.compare.company_facts",
        "equity.discovery.latest_financial_reports",
        "equity.fundamental.balance",
        "equity.fundamental.balance_growth",
        "equity.fundamental.cash",
        "equity.fundamental.cash_growth",
        "equity.fundamental.filings",
        "equity.fundamental.income",
        "equity.fundamental.income_growth",
        "equity.ownership.form_13f",
        "equity.ownership.insider_trading",
        "equity.shorts.fails_to_deliver",
        "etf.nport_disclosure",
    }:
        preferred = ("sec", "fmp", "yfinance", "intrinio", "tiingo")
    else:
        preferred = OFFICIAL_PROVIDER_PRIORITY
    rank = {provider: idx for idx, provider in enumerate(preferred)}
    ordered = sorted(
        providers, key=lambda provider: (rank.get(provider, len(rank)), provider)
    )
    return provider_execution_order(ordered)


def provider_execution_order(providers: Sequence[str]) -> tuple[str, ...]:
    """Apply runtime scarcity policy without changing fallback membership.

    Yahoo and FMP overlap on several equity metadata/fundamental routes.  A
    free Yahoo answer is already accepted as semantically equivalent by those
    task contracts, while the configured FMP Basic account has only 250 calls
    per provider day and owns hundreds of thousands of exclusive tasks.  Try
    Yahoo before FMP whenever both are present, preserving official providers
    such as SEC ahead of both. Tiingo remains last by explicit archive policy.

    This normalization also applies to old manifest rows, so correcting the
    scarcity order never requires requeueing successful data or replanning a
    multi-million-task archive.
    """
    ordered = list(dict.fromkeys(str(item).lower() for item in providers))
    if "yfinance" in ordered and "fmp" in ordered:
        ordered.remove("yfinance")
        ordered.insert(ordered.index("fmp"), "yfinance")
    ordered = [item for item in ordered if item != "tiingo"] + [
        item for item in ordered if item == "tiingo"
    ]
    return tuple(ordered)


def select_providers(
    endpoint: str, available: Sequence[str], context: PlannerContext
) -> tuple[str, ...]:
    ordered = provider_order(endpoint, available)
    if allowed := ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST.get(endpoint):
        ordered = tuple(provider for provider in ordered if provider in allowed)
    if context.allowed_providers is not None:
        ordered = tuple(item for item in ordered if item in context.allowed_providers)
    selected = tuple(item for item in ordered if item not in context.disabled_providers)
    if selected == ("tiingo",) and context.allowed_providers is None:
        return ()
    return selected


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    )


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish small runtime state without exposing a partial JSON document."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _request_checkpoint_path(checkpoint_dir: Path, url: str) -> tuple[Path, str]:
    """Return a credential-free content-addressed path for one GET request."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    split = urlsplit(url)
    safe_query = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=True)
        if key.lower() not in {"api_key", "apikey", "token"}
    ]
    safe_url = urlunsplit(
        (
            split.scheme.lower(),
            split.netloc.lower(),
            split.path,
            urlencode(sorted(safe_query)),
            "",
        )
    )
    fingerprint = hashlib.sha256(safe_url.encode("utf-8")).hexdigest()
    return checkpoint_dir / f"{fingerprint}.json", fingerprint


def _load_request_checkpoint(
    checkpoint_dir: Path | None,
    url: str,
) -> dict[str, Any] | None:
    """Load a proven complete subrequest or quarantine a damaged checkpoint."""
    if checkpoint_dir is None:
        return None
    path, fingerprint = _request_checkpoint_path(checkpoint_dir, url)
    if not path.is_file():
        return None
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(envelope, Mapping)
            or int(envelope.get("schema_version") or 0) != 1
            or str(envelope.get("request_fingerprint") or "") != fingerprint
            or not isinstance(envelope.get("payload"), Mapping)
        ):
            raise ValueError("invalid request checkpoint envelope")
        return dict(envelope["payload"])
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        quarantine = path.with_name(f"{path.name}.corrupt.{time.time_ns()}")
        try:
            path.replace(quarantine)
        except OSError:
            pass
        return None


def _save_request_checkpoint(
    checkpoint_dir: Path | None,
    url: str,
    payload: Mapping[str, Any],
) -> None:
    """Atomically persist one successful provider subrequest for task resume."""
    if checkpoint_dir is None:
        return
    path, fingerprint = _request_checkpoint_path(checkpoint_dir, url)
    _write_json_atomic(
        path,
        {
            "schema_version": 1,
            "request_fingerprint": fingerprint,
            "payload": dict(payload),
        },
    )


def _clear_request_checkpoints(checkpoint_dir: Path | None) -> None:
    """Remove one exact task's generated request checkpoints after publication."""
    if checkpoint_dir is None or not checkpoint_dir.is_dir():
        return
    for path in checkpoint_dir.iterdir():
        if path.is_file() and (
            path.name.endswith(".json") or ".json.corrupt." in path.name
        ):
            path.unlink(missing_ok=True)
    try:
        checkpoint_dir.rmdir()
    except OSError:
        # Unknown files are preserved; never recursively delete a broad or
        # user-controlled directory merely because cleanup was incomplete.
        pass


def _safe_scope(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._=-]+", "_", value).strip("._")
    return (cleaned or "all")[:100]


def make_task(
    context: PlannerContext,
    endpoint: str,
    scope_key: str,
    kwargs: Mapping[str, Any],
    providers: Sequence[str] | None = None,
) -> DownloadTask:
    raw_key = {
        "schema": ARCHIVE_SCHEMA_VERSION,
        "endpoint": endpoint,
        "scope": scope_key,
        "kwargs": dict(kwargs),
    }
    task_id = hashlib.sha256(_canonical_json(raw_key).encode("utf-8")).hexdigest()
    category = endpoint.split(".", 1)[0]
    endpoint_dir = endpoint.replace(".", "/")
    filename = f"{_safe_scope(scope_key)}-{task_id[:16]}.parquet"
    output_path = context.output_dir / "data" / endpoint_dir / task_id[:2] / filename
    if providers is None:
        raw = context.commands.get(f".{endpoint}", context.commands.get(endpoint, []))
        providers = select_providers(endpoint, raw, context)
    return DownloadTask(
        task_id=task_id,
        endpoint=endpoint,
        category=category,
        scope_key=scope_key,
        kwargs=dict(kwargs),
        providers=tuple(providers),
        output_path=str(output_path),
    )


def _fred_release_continuation_task(
    context: PlannerContext,
    task: DownloadTask,
    result_rows: int,
) -> DownloadTask | None:
    """Create the next FRED release/series page when the current page is full."""
    if (
        task.endpoint != "economy.fred_search"
        or not task.scope_key.startswith("release=")
        or result_rows < FRED_RELEASE_PAGE_SIZE
        or "fred" not in task.providers
    ):
        return None
    release_id = task.kwargs.get("release_id")
    if release_id is None:
        return None
    next_offset = int(task.kwargs.get("offset") or 0) + FRED_RELEASE_PAGE_SIZE
    kwargs = dict(task.kwargs, offset=next_offset, limit=FRED_RELEASE_PAGE_SIZE)
    return make_task(
        context,
        task.endpoint,
        f"release={int(release_id)}/offset={next_offset:07d}",
        kwargs,
        ("fred",),
    )


def _model_fields(context: PlannerContext, endpoint: str) -> Mapping[str, Any]:
    item = context.schemas.get(f".{endpoint}", context.schemas.get(endpoint, {}))
    model = item.get("input") if isinstance(item, Mapping) else None
    return getattr(model, "model_fields", {})


def _providers_supporting_query_fields(
    context: PlannerContext,
    endpoint: str,
    providers: Sequence[str],
    fields: Iterable[str],
) -> tuple[str, ...]:
    """Restrict a semantic task to providers that implement all its fields.

    OpenBB's command input is a union of provider query models. Unsupported
    union fields are often silently discarded, which is safe for transport
    hints but corrupts a task whose scope is defined by that field. Provider
    ownership is embedded in the merged field descriptions, including multiple
    ``(provider: ...)`` clauses for fields shared by selected adapters.
    """
    model_fields = _model_fields(context, endpoint)
    required_support: list[set[str]] = []
    for field_name in fields:
        field = model_fields.get(field_name)
        description = str(getattr(field, "description", "") or "")
        owners: set[str] = set()
        for raw_owners in re.findall(
            r"\(providers?:\s*([^)]+)\)", description, flags=re.IGNORECASE
        ):
            owners.update(
                value.lower()
                for value in re.findall(r"[A-Za-z][A-Za-z0-9_]*", raw_owners)
            )
        if owners:
            required_support.append(owners)
    return tuple(
        provider
        for provider in providers
        if all(provider in owners for owners in required_support)
    )


def _callable_signature(
    context: PlannerContext, endpoint: str
) -> inspect.Signature | None:
    item = context.schemas.get(f".{endpoint}", context.schemas.get(endpoint, {}))
    func = item.get("callable") if isinstance(item, Mapping) else None
    try:
        return inspect.signature(func)
    except (TypeError, ValueError):
        return None


def _literal_values(annotation: Any) -> list[Any]:
    origin = get_origin(annotation)
    if str(origin).endswith("Literal"):
        return list(get_args(annotation))
    values: list[Any] = []
    for arg in get_args(annotation):
        values.extend(_literal_values(arg))
    return list(dict.fromkeys(values))


def _description_values(field: Any, heading: str) -> list[str]:
    description = str(getattr(field, "description", "") or "")
    match = re.search(
        rf"{re.escape(heading)}\s*(.*?)\s*\(provider:", description, flags=re.DOTALL
    )
    if match is None:
        return []
    return [
        value.strip()
        for value in match.group(1).replace("\n", " ").split(",")
        if value.strip()
    ]


def _quoted_description_values(field: Any, heading: str) -> list[str]:
    description = str(getattr(field, "description", "") or "")
    if heading not in description:
        return []
    section = description.split(heading, 1)[1]
    values = re.findall(r"'([^']+)'", section)
    return list(
        dict.fromkeys(
            value
            for value in values
            if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:]*", value)
        )
    )


def _normalize_retail_items(items: Iterable[str]) -> list[str]:
    """Repair known provider-description typos before OpenBB validation."""
    return list(
        dict.fromkeys("ground_beef" if item == "groud_beef" else item for item in items)
    )


def _fred_bond_index_combinations() -> list[tuple[str, str, str]]:
    """Return one valid combination for every unique raw FRED BAML series set."""
    from openbb_fred.models.bond_indices import BAML_CATEGORIES

    combinations: list[tuple[str, str, str]] = []
    seen_series: set[tuple[str, ...]] = set()
    for category, indices in BAML_CATEGORIES.items():
        for index, values in indices.items():
            if category == "us" and index == "yield_curve":
                measures = sorted(
                    {
                        measure
                        for maturity_values in values.values()
                        for measure, symbol in maturity_values.items()
                        if symbol
                    }
                )
            else:
                measures = sorted(
                    measure for measure, symbol in values.items() if symbol
                )
            for measure in measures:
                if category == "us" and index == "yield_curve":
                    symbols = tuple(
                        str(maturity_values[measure])
                        for maturity_values in values.values()
                        if maturity_values.get(measure)
                    )
                else:
                    symbols = tuple(str(values[measure]).split(","))
                if symbols in seen_series:
                    continue
                seen_series.add(symbols)
                combinations.append((str(category), str(index), str(measure)))
    return combinations


def _petroleum_status_dimensions() -> list[tuple[str, str]]:
    """Return every EIA workbook/table dataset without relying on defaults."""
    from openbb_us_eia.models.petroleum_status_report import WpsrTableMap

    dimensions: list[tuple[str, str]] = []
    for category, tables in WpsrTableMap.items():
        if category == "weekly_estimates":
            dimensions.extend((str(category), str(table)) for table in tables)
        else:
            # The provider's explicit 'all' value reads every table from this
            # workbook. Weekly estimates is the sole category that rejects it.
            dimensions.append((str(category), "all"))
    return dimensions


def _eia_petroleum_schema_mismatch_tables() -> frozenset[str]:
    """Return real workbook tables omitted from EIA's declared query choices.

    The provider currently has one spelling mismatch between WpsrTableMap and
    WpsrTableChoices.  Derive this set from the installed provider instead of
    hard-coding the affected table so upgrades either repair or expose new
    mismatches automatically.
    """
    from openbb_us_eia.utils.constants import WpsrTableChoices, WpsrTableMap

    workbook_tables = {
        str(table)
        for category_tables in WpsrTableMap.values()
        for table in category_tables
    }
    return frozenset(workbook_tables - set(map(str, WpsrTableChoices)))


@lru_cache(maxsize=1)
def _soma_as_of_dates() -> tuple[str, ...]:
    """Load the authoritative NY Fed SOMA date universe."""
    import asyncio

    from openbb_federal_reserve.utils.ny_fed_api import SomaHoldings

    raw_dates = asyncio.run(SomaHoldings().get_as_of_dates())
    dates = tuple(
        sorted(
            {
                str(value)[:10]
                for value in raw_dates
                if value is not None
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)[:10])
            }
        )
    )
    if not dates:
        raise RuntimeError("NY Fed SOMA returned no authoritative as-of dates")
    return dates


def _base_kwargs(context: PlannerContext, endpoint: str) -> dict[str, Any]:
    fields = _model_fields(context, endpoint)
    kwargs: dict[str, Any] = {}
    if "start_date" in fields:
        kwargs["start_date"] = context.start_date
    if "end_date" in fields:
        kwargs["end_date"] = context.end_date
    if endpoint in HISTORICAL_PRICE_ENDPOINTS:
        kwargs["interval"] = "1d"
    if endpoint in STATEMENT_ENDPOINTS:
        kwargs["limit"] = 1000
        kwargs["pit_mode"] = True
    if endpoint == "equity.ownership.insider_trading":
        kwargs["limit"] = 10000
    if endpoint == "equity.fundamental.dividends":
        # FMP otherwise returns only the latest 1,000 payments before applying
        # the requested date range.  Ten thousand covers daily distributions
        # throughout the requested 2000+ archive without changing request count.
        kwargs["limit"] = 10000
    if endpoint == "equity.fundamental.management_compensation":
        # FMP uses -1/latest when omitted; zero is its documented all-years mode.
        kwargs["year"] = 0
    if endpoint == "equity.fundamental.filings":
        kwargs["limit"] = 1000
        kwargs["use_cache"] = True
    if endpoint.startswith("news."):
        kwargs["limit"] = 1000
        kwargs["display"] = "headline"
    if "country" in fields and endpoint in COUNTRY_ALL_ENDPOINTS:
        kwargs["country"] = "all"
    if endpoint == "commodity.price.spot":
        kwargs["commodity"] = "all"
    if endpoint == "commodity.psd_data":
        kwargs.update(
            {
                "start_year": int(context.start_date[:4]),
                "end_year": int(context.end_date[:4]),
            }
        )
    if endpoint == "cftc.cot":
        kwargs.update({"measure": "all", "limit": 0})
    if endpoint == "fixedincome.government.svensson_yield_curve":
        kwargs.update(
            {
                "series_type": "all",
                "start_date": context.start_date,
                "end_date": context.end_date,
            }
        )
    return kwargs


def _year_ranges(start_date: str, end_date: str) -> Iterator[tuple[int, str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    for year in range(start.year, end.year + 1):
        left = max(start, date(year, 1, 1))
        right = min(end, date(year, 12, 31))
        yield year, left.isoformat(), right.isoformat()


def _completed_quarters(start_date: str, end_date: str) -> Iterator[tuple[int, int]]:
    """Yield quarter ends inside the requested archive interval."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    quarter_ends = ((3, 31), (6, 30), (9, 30), (12, 31))
    for year in range(start.year, end.year + 1):
        for quarter, (month, day) in enumerate(quarter_ends, start=1):
            quarter_end = date(year, month, day)
            if start <= quarter_end <= end:
                yield year, quarter


def _quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    """Return calendar bounds for one validated quarter."""
    if quarter not in {1, 2, 3, 4}:
        raise ValueError(f"Invalid quarter: {quarter}")
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    next_start = (
        date(year + 1, 1, 1) if quarter == 4 else date(year, start_month + 3, 1)
    )
    return start, next_start - timedelta(days=1)


def _sec_insider_bulk_quarters(
    start_date: str,
    end_date: str,
) -> Iterator[tuple[int, int, str, str]]:
    """Yield published SEC quarterly Form 3/4/5 bulk archives.

    The official structured dataset starts in January 2006 and is published
    only after a quarter closes.  The still-open quarter remains a separate
    tail obligation so a planner never invents a ZIP that cannot exist yet.
    """
    requested_start = max(
        date.fromisoformat(start_date), SEC_INSIDER_DATASET_START_DATE
    )
    requested_end = date.fromisoformat(end_date)
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    current_quarter_start, _ = _quarter_bounds(today.year, current_quarter)
    published_end = min(requested_end, current_quarter_start - timedelta(days=1))
    if requested_start > published_end:
        return
    for year, quarter in _completed_quarters(
        requested_start.isoformat(), published_end.isoformat()
    ):
        quarter_start, quarter_end = _quarter_bounds(year, quarter)
        yield (
            year,
            quarter,
            max(requested_start, quarter_start).isoformat(),
            min(published_end, quarter_end).isoformat(),
        )


def _sec_insider_unpublished_tail(
    start_date: str,
    end_date: str,
) -> tuple[str, str] | None:
    """Return the requested current-quarter range not yet in a bulk ZIP."""
    requested_start = max(
        date.fromisoformat(start_date), SEC_INSIDER_DATASET_START_DATE
    )
    requested_end = date.fromisoformat(end_date)
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    current_quarter_start, _ = _quarter_bounds(today.year, current_quarter)
    left = max(requested_start, current_quarter_start)
    right = min(requested_end, today)
    return (left.isoformat(), right.isoformat()) if left <= right else None


def _month_ranges(start_date: str, end_date: str) -> Iterator[tuple[str, str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    current = start.replace(day=1)
    while current <= end:
        next_month = date(
            current.year + int(current.month == 12),
            1 if current.month == 12 else current.month + 1,
            1,
        )
        left = max(start, current)
        right = min(end, next_month - timedelta(days=1))
        yield current.strftime("%Y-%m"), left.isoformat(), right.isoformat()
        current = next_month


def _sec_ftd_report_periods(
    start_date: str,
    end_date: str,
) -> Iterator[tuple[str, str, str]]:
    """Yield stable SEC fails-to-deliver half-month report checkpoints."""
    archive_start = max(date.fromisoformat(start_date), SEC_FTD_START_DATE)
    archive_end = date.fromisoformat(end_date)
    if archive_start > archive_end:
        return
    current = archive_start.replace(day=1)
    while current <= archive_end:
        next_month = date(
            current.year + int(current.month == 12),
            1 if current.month == 12 else current.month + 1,
            1,
        )
        for half, left, right in (
            ("a", current, current.replace(day=15)),
            ("b", current.replace(day=16), next_month - timedelta(days=1)),
        ):
            bounded_left = max(archive_start, left)
            bounded_right = min(archive_end, right)
            if bounded_left <= bounded_right:
                yield (
                    f"{current.year:04d}{current.month:02d}{half}",
                    bounded_left.isoformat(),
                    bounded_right.isoformat(),
                )
        current = next_month


def _bounded_date_ranges(
    start_date: str,
    end_date: str,
    *,
    max_inclusive_days: int,
) -> Iterator[tuple[int, str, str]]:
    """Yield contiguous date ranges whose inclusive length stays within a provider limit."""
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    width = max(1, int(max_inclusive_days))
    index = 0
    while current <= end:
        right = min(end, current + timedelta(days=width - 1))
        yield index, current.isoformat(), right.isoformat()
        index += 1
        current = right + timedelta(days=1)


def _date_grid(start_date: str, end_date: str, cadence: str) -> Iterator[str]:
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if cadence == "month_start":
        current = current.replace(day=1)
        while current <= end:
            yield current.isoformat()
            current = date(
                current.year + (current.month == 12),
                1 if current.month == 12 else current.month + 1,
                1,
            )
        return
    while current <= end:
        if cadence == "weekdays" and current.weekday() < 5:
            yield current.isoformat()
        elif cadence == "weekly_wednesday" and current.weekday() == 2:
            yield current.isoformat()
        current += timedelta(days=1)


def _congress_for_year(year: int) -> int:
    # Congress 106 began in 1999; each Congress spans two calendar years.
    return 106 + (year - 1999) // 2


def _symbol_tasks(
    context: PlannerContext, endpoint: str, providers: tuple[str, ...]
) -> Iterator[DownloadTask]:
    base = _base_kwargs(context, endpoint)
    all_providers = tuple(providers)
    asset_market_by_symbol = {
        str(asset.symbol): str(asset.market).lower() for asset in context.assets
    }
    if endpoint == "equity.shorts.fails_to_deliver":
        # SEC distributes one bulk ZIP per half-month.  The stock-symbol API
        # wrapper re-downloads and re-parses the same reports for every symbol
        # and defaults to only the latest 24 reports.  Archive each official
        # report once, retaining all symbols and the complete 2009+ history.
        for report_key, left, right in _sec_ftd_report_periods(
            context.start_date, context.end_date
        ):
            yield make_task(
                context,
                endpoint,
                f"report={report_key}",
                {
                    "report_key": report_key,
                    "start_date": left,
                    "end_date": right,
                    "use_cache": True,
                },
                providers,
            )
        return
    if endpoint in FMP_QUARTERLY_OWNERSHIP_ENDPOINTS:
        # These endpoints are derived from US 13F holdings.  Querying every
        # international listing only produces deterministic empty responses.
        # Explicit year/quarter values avoid FMP's latest-quarter default.
        us_symbols = [
            str(asset.symbol) for asset in context.assets if asset.market == "us"
        ]
        # FMP's institutional-ownership query model explicitly accepts a
        # comma-separated symbol list and expands it to one upstream request
        # per symbol.  Batch that provider-native unit so the manifest does
        # not create millions of identical quarter tasks; each HTTP child is
        # still paced by the provider-boundary limiter.
        symbol_groups = (
            [
                us_symbols[offset : offset + FMP_OWNERSHIP_SYMBOL_BATCH_SIZE]
                for offset in range(0, len(us_symbols), FMP_OWNERSHIP_SYMBOL_BATCH_SIZE)
            ]
            if endpoint == "equity.ownership.institutional"
            else [[symbol] for symbol in us_symbols]
        )
        for symbol_group in symbol_groups:
            if not symbol_group:
                continue
            symbol_value = ",".join(symbol_group)
            group_label = (
                f"{symbol_group[0]}..{symbol_group[-1]}"
                if len(symbol_group) > 1
                else symbol_group[0]
            )
            for year, quarter in _completed_quarters(
                context.start_date, context.end_date
            ):
                kwargs = dict(
                    base,
                    symbol=symbol_value,
                    year=year,
                    quarter=quarter,
                )
                scope = f"{group_label}/year={year}/quarter={quarter}"
                if endpoint == "equity.ownership.major_holders":
                    kwargs.update(page=0, limit=100)
                    scope += "/page=0"
                yield make_task(context, endpoint, scope, kwargs, providers)
        return
    if endpoint == "equity.ownership.insider_trading":
        sec_providers = tuple(provider for provider in providers if provider == "sec")
        fmp_providers = tuple(provider for provider in providers if provider == "fmp")

        # SEC's official structured Form 3/4/5 data is one complete ZIP per
        # published quarter.  Planning that archive once is the native unit of
        # work; the OpenBB symbol adapter would otherwise retrieve the same
        # submissions indexes and thousands of individual XML documents once
        # per ticker.  The current, unpublished quarter remains symbol-scoped.
        if sec_providers and any(asset.market == "us" for asset in context.assets):
            for year, quarter, left, right in _sec_insider_bulk_quarters(
                context.start_date, context.end_date
            ):
                yield make_task(
                    context,
                    endpoint,
                    f"bulk/year={year}/quarter={quarter}",
                    {
                        "symbol": "__all__",
                        "start_date": left,
                        "end_date": right,
                        "year": year,
                        "quarter": quarter,
                        "use_cache": True,
                        "_archive_sec_insider_bulk": True,
                    },
                    sec_providers,
                )

            tail = _sec_insider_unpublished_tail(context.start_date, context.end_date)
            if tail is not None:
                left, right = tail
                for asset in context.assets:
                    if asset.market != "us":
                        continue
                    yield make_task(
                        context,
                        endpoint,
                        f"{asset.symbol}/tail={left}_{right}",
                        dict(
                            base,
                            symbol=asset.symbol,
                            start_date=left,
                            end_date=right,
                            use_cache=True,
                            _archive_sec_insider_tail=True,
                        ),
                        sec_providers,
                    )

        # The SEC quarterly structured dataset begins in 2006.  Preserve the
        # pre-2006 OpenBB transaction route, with FMP as fallback where it is
        # available, because there is no semantically equivalent bulk ZIP.
        legacy_end = min(
            date.fromisoformat(context.end_date),
            SEC_INSIDER_DATASET_START_DATE - timedelta(days=1),
        )
        legacy_start = date.fromisoformat(context.start_date)
        legacy_providers = tuple(
            provider for provider in providers if provider in {"sec", "fmp"}
        )
        if legacy_start <= legacy_end and legacy_providers:
            for asset in context.assets:
                yield make_task(
                    context,
                    endpoint,
                    (
                        f"{asset.symbol}/legacy="
                        f"{legacy_start.isoformat()}_{legacy_end.isoformat()}"
                    ),
                    dict(
                        base,
                        symbol=asset.symbol,
                        start_date=legacy_start.isoformat(),
                        end_date=legacy_end.isoformat(),
                        use_cache=True,
                        _archive_sec_insider_range=True,
                    ),
                    legacy_providers,
                )

        # FMP's statistics mode is a separate dataset, not a substitute for
        # SEC transaction rows, and therefore remains one explicit task.
        for asset in context.assets:
            if fmp_providers:
                yield make_task(
                    context,
                    endpoint,
                    f"{asset.symbol}/mode=statistics",
                    {"symbol": asset.symbol, "statistics": True},
                    fmp_providers,
                )
        return
    if endpoint == "index.constituents":
        # OpenBB validates this route against FMP's three canonical aliases;
        # exchange symbols from index.available (for example ^GSPC) are not
        # valid inputs for the constituents endpoint.
        for symbol in FMP_CONSTITUENT_INDEXES:
            yield make_task(
                context,
                endpoint,
                symbol,
                dict(base, symbol=symbol, historical=True),
                providers,
            )
        return
    if endpoint == "equity.fundamental.filings":
        for asset in context.assets:
            symbol = asset.symbol
            if asset.market == "us":
                # The SEC provider's submissions response already exposes all
                # historical shards for a filer. The worker changes limit to
                # zero for SEC so one task returns every filing from 2000 on;
                # FMP keeps its page limit if it is needed as a fallback.
                yield make_task(
                    context,
                    endpoint,
                    f"{symbol}/all/page=0",
                    dict(base, symbol=symbol, page=0),
                    providers,
                )
                continue
            # Non-US symbols cannot use SEC submissions. Keep yearly fallback
            # queries so a provider's restricted full-history window cannot
            # hide a recent year that remains available on the user's plan.
            non_sec_providers = tuple(
                provider for provider in providers if provider != "sec"
            )
            for year, left, right in _year_ranges(context.start_date, context.end_date):
                yield make_task(
                    context,
                    endpoint,
                    f"{symbol}/year={year}/page=0",
                    dict(
                        base,
                        symbol=symbol,
                        start_date=left,
                        end_date=right,
                        page=0,
                    ),
                    non_sec_providers,
                )
        return
    if endpoint.startswith("currency."):
        symbols: Iterable[str] = context.currencies
    elif endpoint.startswith("etf."):
        symbols = (item.symbol for item in context.etfs)
    elif endpoint.startswith("index."):
        symbols = context.indices
    elif endpoint.startswith("regulators.sec."):
        symbols = (item.symbol for item in context.assets if item.market == "us")
    elif endpoint == "equity.compare.company_facts":
        symbols = (item.symbol for item in context.assets if item.market == "us")
    else:
        symbols = (item.symbol for item in context.assets)

    for symbol in symbols:
        # Provider capability is a planning concern, not a fallback error.
        # SEC companyfacts/submissions are keyed by US CIKs; sending TW or
        # other non-US symbols to SEC first only burns fair-access requests and
        # creates the same 404/permanent churn in every statement route.
        symbol_providers = tuple(
            provider
            for provider in all_providers
            if not (
                provider == "sec"
                and asset_market_by_symbol.get(str(symbol), "us") != "us"
            )
        )
        if not symbol_providers:
            continue
        providers = symbol_providers
        if endpoint == "equity.compare.company_facts":
            # SEC's companyfacts endpoint returns every historical concept for
            # one filer in a single response.  Planning symbol x fact would
            # issue millions of companyconcept requests for the same CIK.
            yield make_task(
                context,
                endpoint,
                f"{symbol}/fact={SEC_ALL_COMPANY_FACTS}",
                dict(base, symbol=symbol, fact=SEC_ALL_COMPANY_FACTS, use_cache=True),
                providers,
            )
            continue
        if endpoint in SYMBOL_YEAR_CHUNK_ENDPOINTS:
            for year, left, right in _year_ranges(context.start_date, context.end_date):
                kwargs = dict(base, symbol=symbol, start_date=left, end_date=right)
                scope = f"{symbol}/year={year}"
                if endpoint.startswith("news."):
                    kwargs["page"] = 0
                    scope += "/page=0"
                elif endpoint == "equity.fundamental.filings":
                    kwargs["page"] = 0
                    scope += "/page=0"
                yield make_task(context, endpoint, scope, kwargs, providers)
            continue
        if endpoint == "etf.nport_disclosure":
            # Form N-PORT was introduced long after 2000; pre-2019 tasks can
            # only be empty and waste API quota.
            for year in range(
                max(2019, int(context.start_date[:4])), int(context.end_date[:4]) + 1
            ):
                for quarter in range(1, 5):
                    yield make_task(
                        context,
                        endpoint,
                        f"{symbol}/year={year}/quarter={quarter}",
                        dict(
                            base,
                            symbol=symbol,
                            year=year,
                            quarter=quarter,
                            use_cache=True,
                        ),
                        providers,
                    )
            continue
        if endpoint == "equity.ownership.form_13f":
            start_year = max(2013, int(context.start_date[:4]))
            for year in range(start_year, int(context.end_date[:4]) + 1):
                for month_day in ("03-31", "06-30", "09-30", "12-31"):
                    report_date = f"{year}-{month_day}"
                    if report_date > context.end_date:
                        continue
                    yield make_task(
                        context,
                        endpoint,
                        f"{symbol}/date={report_date}",
                        dict(base, symbol=symbol, date=report_date, limit=1),
                        providers,
                    )
            continue
        if endpoint in FORWARD_ESTIMATE_ENDPOINTS:
            for fiscal_period in ("annual", "quarter"):
                yield make_task(
                    context,
                    endpoint,
                    f"{symbol}/period={fiscal_period}",
                    dict(
                        base,
                        symbol=symbol,
                        fiscal_period=fiscal_period,
                        limit=1000,
                        include_historical=True,
                    ),
                    providers,
                )
            continue
        if endpoint in SYMBOL_PERIOD_ENDPOINTS:
            for period in ("annual", "quarter"):
                kwargs = dict(base, symbol=symbol, period=period)
                if endpoint == "equity.estimates.historical":
                    kwargs.update(limit=1000, page=0)
                yield make_task(
                    context,
                    endpoint,
                    f"{symbol}/period={period}",
                    kwargs,
                    providers,
                )
            continue
        if endpoint in STATEMENT_ENDPOINTS:
            period_providers = _providers_supporting_query_fields(
                context, endpoint, providers, ("period",)
            )
            if endpoint in {
                "equity.fundamental.metrics",
                "equity.fundamental.ratios",
            }:
                # Raw annual/quarter rows and TTM are separate completeness
                # obligations. A fallback that supports period but not TTM may
                # still satisfy the raw task without silently erasing TTM.
                for period in ("annual", "quarter"):
                    kwargs = dict(
                        base,
                        symbol=symbol,
                        period=period,
                        include_preliminary=True,
                        ttm="exclude",
                    )
                    if period_providers:
                        yield make_task(
                            context,
                            endpoint,
                            f"{symbol}/period={period}",
                            kwargs,
                            period_providers,
                        )
                ttm_providers = _providers_supporting_query_fields(
                    context, endpoint, providers, ("ttm",)
                )
                if ttm_providers:
                    yield make_task(
                        context,
                        endpoint,
                        f"{symbol}/period=ttm",
                        dict(
                            base,
                            symbol=symbol,
                            ttm="only",
                            include_preliminary=True,
                        ),
                        ttm_providers,
                    )
                continue
            for period in ("annual", "quarter"):
                kwargs = dict(
                    base,
                    symbol=symbol,
                    period=period,
                    include_preliminary=True,
                )
                if period_providers:
                    yield make_task(
                        context,
                        endpoint,
                        f"{symbol}/period={period}",
                        kwargs,
                        period_providers,
                    )
            continue
        yield make_task(context, endpoint, symbol, dict(base, symbol=symbol), providers)


def _singleton_tasks(
    context: PlannerContext, endpoint: str, providers: tuple[str, ...]
) -> Iterator[DownloadTask]:
    fields = _model_fields(context, endpoint)
    base = _base_kwargs(context, endpoint)

    if endpoint == "commodity.petroleum_status_report":
        for category, table in _petroleum_status_dimensions():
            yield make_task(
                context,
                endpoint,
                f"category={category}/table={table}",
                dict(base, category=category, table=table),
                providers,
            )
        return

    if endpoint == "economy.central_bank_holdings":
        # The NY Fed SOMA adapter has four distinct datasets.  Its defaults
        # return only Treasury CUSIP holdings for one date; summary/monthly and
        # agency/WAM rows are not contained in that response.  Use the NY Fed's
        # authoritative as-of catalog: synthetic Wednesdays can be silently
        # mapped to another release and duplicate data under the wrong scope.
        yield make_task(
            context,
            endpoint,
            "mode=summary",
            {"summary": True},
            providers,
        )
        yield make_task(
            context,
            endpoint,
            "mode=monthly",
            {"monthly": True, "holding_type": "all_treasury"},
            providers,
        )
        for value in _soma_as_of_dates():
            if not (context.start_date <= value <= context.end_date):
                continue
            for security, holding_type in (
                ("treasury", "all_treasury"),
                ("agency", "all_agency"),
            ):
                yield make_task(
                    context,
                    endpoint,
                    f"date={value}/security={security}/mode=holdings",
                    {"date": value, "holding_type": holding_type},
                    providers,
                )
                yield make_task(
                    context,
                    endpoint,
                    f"date={value}/security={security}/mode=wam",
                    {"date": value, "holding_type": holding_type, "wam": True},
                    providers,
                )
        return

    if endpoint == "equity.ownership.government_trades":
        # The stock-less FMP route is the complete House/Senate catalog.  A
        # custom paged worker treats limit=0 as unbounded and applies this date
        # window after retrieval; OpenBB itself defaults to only 1,000 rows.
        yield make_task(
            context,
            endpoint,
            "all/page=0",
            {
                "start_date": context.start_date,
                "end_date": context.end_date,
                "limit": 0,
            },
            providers,
        )
        return

    if endpoint == "news.world":
        # FMP exposes separate non-crypto feeds.  General keeps Tiingo as its
        # fallback; applying a FMP-only topic to Tiingo would repeat the same
        # feed once per topic.  FMP editorial articles do not accept dates, so
        # one custom worker paginates the catalog and filters it after fetch.
        fmp_providers = tuple(provider for provider in providers if provider == "fmp")
        if fmp_providers:
            yield make_task(
                context,
                endpoint,
                "topic=fmp_articles/page=0",
                {
                    "topic": "fmp_articles",
                    "start_date": context.start_date,
                    "end_date": context.end_date,
                    "limit": 0,
                    "page": 0,
                    "display": "headline",
                },
                fmp_providers,
            )
        for year, left, right in _year_ranges(context.start_date, context.end_date):
            topics = FMP_WORLD_NEWS_TOPICS if fmp_providers else ("general",)
            for topic in topics:
                topic_providers = providers if topic == "general" else fmp_providers
                yield make_task(
                    context,
                    endpoint,
                    f"year={year}/topic={topic}/page=0",
                    dict(
                        base,
                        topic=topic,
                        start_date=left,
                        end_date=right,
                        page=0,
                    ),
                    topic_providers,
                )
        return

    if endpoint == "economy.unemployment":
        for sex, age, seasonal_adjustment in UNEMPLOYMENT_DIMENSIONS:
            seasonal = "adjusted" if seasonal_adjustment else "unadjusted"
            yield make_task(
                context,
                endpoint,
                f"sex={sex}/age={age}/seasonal={seasonal}",
                dict(
                    base,
                    frequency="monthly",
                    sex=sex,
                    age=age,
                    seasonal_adjustment=seasonal_adjustment,
                ),
                providers,
            )
        return

    if endpoint == "economy.gdp.forecast":
        for frequency, units in GDP_FORECAST_DIMENSIONS:
            yield make_task(
                context,
                endpoint,
                f"frequency={frequency}/units={units}",
                dict(base, frequency=frequency, units=units),
                providers,
            )
        return

    if endpoint == "economy.gdp.nominal":
        dimension_providers = _providers_supporting_query_fields(
            context,
            endpoint,
            providers,
            ("frequency", "units", "price_base"),
        )
        for frequency, units, price_base in GDP_NOMINAL_DIMENSIONS:
            yield make_task(
                context,
                endpoint,
                f"frequency={frequency}/units={units}/price={price_base}",
                dict(
                    base,
                    frequency=frequency,
                    units=units,
                    price_base=price_base,
                ),
                dimension_providers,
            )
        return

    if endpoint == "economy.gdp.real":
        dimension_providers = _providers_supporting_query_fields(
            context, endpoint, providers, ("frequency",)
        )
        for frequency in GDP_REAL_FREQUENCIES:
            yield make_task(
                context,
                endpoint,
                f"frequency={frequency}",
                dict(base, frequency=frequency),
                dimension_providers,
            )
        return

    if endpoint == "economy.interest_rates":
        for duration in INTEREST_RATE_DURATIONS:
            yield make_task(
                context,
                endpoint,
                f"duration={duration}",
                dict(base, duration=duration, frequency="monthly"),
                providers,
            )
        return

    if endpoint == "economy.total_factor_productivity":
        for frequency in TOTAL_FACTOR_PRODUCTIVITY_FREQUENCIES:
            yield make_task(
                context,
                endpoint,
                f"frequency={frequency}",
                dict(base, frequency=frequency),
                providers,
            )
        return

    if endpoint == "economy.house_price_index":
        for frequency, transform in HOUSE_PRICE_DIMENSIONS:
            yield make_task(
                context,
                endpoint,
                f"frequency={frequency}/transform={transform}",
                dict(
                    base,
                    frequency=frequency,
                    transform=transform,
                ),
                providers,
            )
        return

    if endpoint == "economy.primary_dealer_positioning":
        # These five top-level groups form a non-overlapping cover of every
        # NY Fed dealer-position series; narrower choices are subsets.
        for category in PRIMARY_DEALER_POSITION_GROUPS:
            yield make_task(
                context,
                endpoint,
                f"category={category}",
                dict(base, category=category),
                providers,
            )
        return

    if endpoint == "economy.composite_leading_indicator":
        for adjustment in COMPOSITE_LEADING_INDICATOR_ADJUSTMENTS:
            yield make_task(
                context,
                endpoint,
                f"adjustment={adjustment}",
                dict(base, adjustment=adjustment, growth_rate=False),
                providers,
            )
        return

    if endpoint == "economy.cpi":
        for harmonized in CPI_HARMONIZED_MODES:
            mode = "harmonized" if harmonized else "standard"
            yield make_task(
                context,
                endpoint,
                f"mode={mode}",
                dict(
                    base,
                    frequency="monthly",
                    transform="index",
                    harmonized=harmonized,
                ),
                providers,
            )
        return

    if endpoint == "fixedincome.rate.dpcredit":
        for parameter in DWPCR_RAW_PARAMETERS:
            yield make_task(
                context,
                endpoint,
                f"parameter={parameter}",
                dict(base, parameter=parameter),
                providers,
            )
        return

    if endpoint in SPREAD_MATURITIES:
        for maturity in SPREAD_MATURITIES[endpoint]:
            yield make_task(
                context,
                endpoint,
                f"maturity={maturity}",
                dict(base, maturity=maturity),
                providers,
            )
        return

    if endpoint == "economy.calendar":
        # Recent annual FRED calendars contain several thousand rows. A
        # timeout on any page previously restarted the whole year from offset
        # zero and could hold a fair-scheduler batch for many minutes. Monthly
        # checkpoints keep each retry bounded while the worker still follows
        # every API page within the month.
        for month, left, right in _month_ranges(context.start_date, context.end_date):
            yield make_task(
                context,
                endpoint,
                f"month={month}/page=0",
                dict(base, start_date=left, end_date=right),
                providers,
            )
        return

    if endpoint == "equity.discovery.filings":
        for chunk, left, right in _bounded_date_ranges(
            context.start_date,
            context.end_date,
            max_inclusive_days=90,
        ):
            kwargs = dict(
                base,
                start_date=left,
                end_date=right,
                limit=1000,
                page=0,
            )
            yield make_task(
                context,
                endpoint,
                f"chunk={chunk:03d}/start={left}/end={right}/page=0",
                kwargs,
                providers,
            )
        return

    if endpoint in GLOBAL_YEAR_CHUNK_ENDPOINTS:
        for year, left, right in _year_ranges(context.start_date, context.end_date):
            kwargs = dict(base, start_date=left, end_date=right)
            if endpoint.startswith("news."):
                kwargs["page"] = 0
            yield make_task(context, endpoint, f"year={year}/page=0", kwargs, providers)
        return

    if endpoint in DATE_GRID_ENDPOINTS:
        if endpoint == "fixedincome.government.yield_curve":
            # The merged OpenBB command schema is a union, not a promise that
            # every provider implements every field. FRED alone implements
            # yield_curve_type; EconDB alone implements country. Federal
            # Reserve/FMP/EconDB/FRED all default to a nominal US government
            # curve when neither provider-only field is supplied.
            nominal_archive_providers = tuple(
                provider
                for provider in providers
                if provider in FULL_HISTORY_YIELD_CURVE_PROVIDERS
            )
            nominal_window_providers = tuple(
                provider
                for provider in providers
                if provider not in FULL_HISTORY_YIELD_CURVE_PROVIDERS
            )
            nominal_dates = ",".join(
                _date_grid(
                    context.start_date,
                    context.end_date,
                    DATE_GRID_ENDPOINTS[endpoint],
                )
            )
            if nominal_archive_providers:
                yield make_task(
                    context,
                    endpoint,
                    "country=united_states/type=nominal/archive",
                    dict(
                        base,
                        date=nominal_dates,
                        country="united_states",
                        yield_curve_type="nominal",
                    ),
                    nominal_archive_providers,
                )
            elif nominal_window_providers:
                # FMP and any future provider whose HTTP request is genuinely
                # date-bounded keep small resumable shards.  Do not put these
                # providers behind the archive task: a late fallback would
                # otherwise turn one retry into thousands of HTTP requests.
                for value in nominal_dates.split(","):
                    yield make_task(
                        context,
                        endpoint,
                        f"date={value}/type=nominal",
                        dict(
                            base,
                            date=value,
                            country="united_states",
                            yield_curve_type="nominal",
                        ),
                        nominal_window_providers,
                    )

            fred_providers = tuple(
                provider for provider in providers if provider == "fred"
            )
            for yield_curve_type in GOVERNMENT_YIELD_CURVE_TYPES:
                if yield_curve_type == "nominal" or not fred_providers:
                    continue
                cadence = (
                    DATE_GRID_ENDPOINTS[endpoint]
                    if yield_curve_type == "real"
                    else "month_start"
                )
                dates = ",".join(
                    _date_grid(context.start_date, context.end_date, cadence)
                )
                yield make_task(
                    context,
                    endpoint,
                    f"country=united_states/type={yield_curve_type}/archive",
                    dict(
                        base,
                        date=dates,
                        country="united_states",
                        yield_curve_type=yield_curve_type,
                    ),
                    fred_providers,
                )

            econdb_providers = tuple(
                provider for provider in providers if provider == "econdb"
            )
            if econdb_providers:
                for country, cadence in _econdb_yield_curve_dimensions():
                    # The portable nominal task above already covers the US
                    # default through the complete primary+fallback chain.
                    if country == "united_states":
                        continue
                    # EconDB's yield-curve HTTP request always returns the
                    # country's complete history; `date` is applied only by
                    # OpenBB's local transform and explicitly supports a
                    # comma-separated list. One task per date therefore
                    # redownloads the identical payload thousands of times.
                    # Submit the entire archive date grid in one provider call
                    # and persist all selected observations in one resumable
                    # country shard.
                    dates = ",".join(
                        _date_grid(context.start_date, context.end_date, cadence)
                    )
                    yield make_task(
                        context,
                        endpoint,
                        f"country={country}/archive",
                        dict(base, date=dates, country=country),
                        econdb_providers,
                    )
        else:
            for value in _date_grid(
                context.start_date, context.end_date, DATE_GRID_ENDPOINTS[endpoint]
            ):
                if endpoint == "fixedincome.corporate.hqm":
                    for yield_curve in ("spot", "par"):
                        yield make_task(
                            context,
                            endpoint,
                            f"date={value}/curve={yield_curve}",
                            dict(base, date=value, yield_curve=yield_curve),
                            providers,
                        )
                elif endpoint in DATE_GRID_LITERAL_FIELDS:
                    field_name = DATE_GRID_LITERAL_FIELDS[endpoint]
                    model_field = fields.get(field_name)
                    values = (
                        _literal_values(getattr(model_field, "annotation", None))
                        if model_field is not None
                        else []
                    )
                    values = [item for item in values if item not in {None, "all"}]
                    if values:
                        for item in values:
                            yield make_task(
                                context,
                                endpoint,
                                f"date={value}/{field_name}={item}",
                                dict(base, date=value, **{field_name: item}),
                                providers,
                            )
                    else:
                        yield make_task(
                            context,
                            endpoint,
                            f"date={value}",
                            dict(base, date=value),
                            providers,
                        )
                else:
                    yield make_task(
                        context,
                        endpoint,
                        f"date={value}",
                        dict(base, date=value),
                        providers,
                    )
        return

    if endpoint == "fixedincome.mortgage_indices":
        for index in MORTGAGE_INDEX_GROUPS:
            yield make_task(
                context,
                endpoint,
                f"index={index}",
                dict(base, index=index),
                providers,
            )
        return

    if endpoint == "fixedincome.bond_indices":
        for category, index, index_type in _fred_bond_index_combinations():
            yield make_task(
                context,
                endpoint,
                f"category={category}/index={index}/type={index_type}",
                dict(
                    base,
                    category=category,
                    index=index,
                    index_type=index_type,
                ),
                providers,
            )
        return

    if endpoint == "commodity.weather_bulletins":
        for year in range(int(context.start_date[:4]), int(context.end_date[:4]) + 1):
            for month in range(1, 13):
                if f"{year}-{month:02d}-01" > context.end_date:
                    continue
                yield make_task(
                    context,
                    endpoint,
                    f"year={year}/month={month:02d}",
                    {"year": year, "month": month},
                    providers,
                )
        return

    if endpoint == "commodity.psd_data":
        commodities = _description_values(
            fields.get("commodity"), "Valid commodities are:"
        )
        if commodities:
            for commodity in commodities:
                yield make_task(
                    context,
                    endpoint,
                    f"commodity={commodity}",
                    dict(base, commodity=commodity, aggregate_regions=True),
                    providers,
                )
            return

    if endpoint == "economy.direction_of_trade":
        countries = _imf_direction_countries()
        if countries:
            for country in countries:
                yield make_task(
                    context,
                    endpoint,
                    f"country={country}",
                    dict(
                        base,
                        country=country,
                        counterpart="all",
                        direction="all",
                        frequency="month",
                    ),
                    providers,
                )
            return

    if endpoint == "economy.retail_prices":
        items = _quoted_description_values(fields.get("item"), "Choices forfred:")
        if items:
            for item in _normalize_retail_items(items):
                for region in RETAIL_PRICE_REGIONS:
                    yield make_task(
                        context,
                        endpoint,
                        f"item={item}/region={region}",
                        dict(base, item=item, region=region, frequency="monthly"),
                        providers,
                    )
            return

    if endpoint == "economy.shipping.port_volume":
        base["start_date"] = max(context.start_date, "2019-01-02")

    if endpoint == "economy.country_profile":
        for country in _econdb_country_codes():
            yield make_task(
                context,
                endpoint,
                f"country={country}",
                dict(base, country=country, latest=False, use_cache=True),
                providers,
            )
        return

    if endpoint == "economy.export_destinations":
        countries = [
            "uk" if country.lower() == "gb" else country.lower()
            for country in context.countries
        ]
        for country in dict.fromkeys(countries):
            yield make_task(
                context,
                endpoint,
                f"country={country}",
                dict(base, country=country),
                providers,
            )
        return

    if endpoint == "uscongress.bills":
        first = _congress_for_year(max(2000, int(context.start_date[:4])))
        last = _congress_for_year(int(context.end_date[:4]))
        for congress in range(first, last + 1):
            for bill_type in CONGRESS_BILL_TYPES:
                kwargs = {
                    "congress": congress,
                    "bill_type": bill_type,
                    "limit": 0,
                    "sort_by": "asc",
                }
                yield make_task(
                    context,
                    endpoint,
                    f"congress={congress}/type={bill_type}",
                    kwargs,
                    providers,
                )
        return

    if endpoint == "uscongress.amendments":
        first = _congress_for_year(max(2000, int(context.start_date[:4])))
        last = _congress_for_year(int(context.end_date[:4]))
        for congress in range(first, last + 1):
            for amendment_type in CONGRESS_AMENDMENT_TYPES:
                kwargs = {
                    "congress": congress,
                    "amendment_type": amendment_type,
                    "limit": 0,
                    "sort_by": "asc",
                }
                yield make_task(
                    context,
                    endpoint,
                    f"congress={congress}/type={amendment_type}",
                    kwargs,
                    providers,
                )
        return

    if endpoint == "cftc.cot_search":
        for report_type, futures_only in CFTC_REPORT_MODES:
            mode = "futures" if futures_only else "combined"
            yield make_task(
                context,
                endpoint,
                f"report={report_type}/mode={mode}",
                {
                    "query": "",
                    "report_type": report_type,
                    "futures_only": futures_only,
                    "start_date": context.start_date,
                    "end_date": context.end_date,
                },
                providers,
            )
        return

    if endpoint == "economy.fred_search":
        yield make_task(
            context,
            endpoint,
            "release_catalog",
            {"query": "", "search_type": "release", "limit": 1000, "offset": 0},
            providers,
        )
        return

    if endpoint == "economy.survey.bls_search":
        field = fields.get("category")
        categories = (
            _literal_values(getattr(field, "annotation", None))
            if field is not None
            else []
        )
        for category in categories:
            yield make_task(
                context,
                endpoint,
                f"category={category}",
                {
                    "query": "",
                    "category": category,
                    "include_extras": True,
                    "include_code_map": True,
                },
                providers,
            )
        return

    if endpoint == "economy.survey.manufacturing_outlook_ny":
        field = fields.get("topic")
        topics = (
            _literal_values(getattr(field, "annotation", None))
            if field is not None
            else []
        )
        for topic in topics:
            for seasonally_adjusted in (False, True):
                seasonal = "adjusted" if seasonally_adjusted else "unadjusted"
                yield make_task(
                    context,
                    endpoint,
                    f"topic={topic}/seasonal={seasonal}",
                    dict(
                        base,
                        topic=topic,
                        seasonally_adjusted=seasonally_adjusted,
                    ),
                    providers,
                )
        return

    if endpoint in {"currency.search", "equity.search"}:
        yield make_task(context, endpoint, "catalog", {"query": ""}, providers)
        return

    if endpoint in ENUMERATE_LITERAL_FIELD:
        field_name = ENUMERATE_LITERAL_FIELD[endpoint]
        model_field = fields.get(field_name)
        values = (
            _literal_values(getattr(model_field, "annotation", None))
            if model_field is not None
            else []
        )
        values = [item for item in values if item not in {None, "all"}]
        if values:
            for value in values:
                yield make_task(
                    context,
                    endpoint,
                    f"{field_name}={value}",
                    dict(base, **{field_name: value}),
                    providers,
                )
            return

    yield make_task(context, endpoint, "all", base, providers)


def _plan_endpoint(
    context: PlannerContext,
    raw_endpoint: str,
    available: Sequence[str],
) -> tuple[Iterator[DownloadTask], CoverageDecision]:
    endpoint = raw_endpoint.lstrip(".")
    category = endpoint.split(".", 1)[0]
    selected = select_providers(endpoint, available, context)
    decision = "included"
    reason = ""
    endpoint_tasks: Iterator[DownloadTask] = iter(())

    if category in {"crypto", "derivatives"}:
        decision, reason = (
            "excluded",
            "crypto and derivatives are outside the requested scope",
        )
    elif endpoint in SNAPSHOT_ENDPOINTS:
        decision, reason = "excluded", "real-time/current snapshot route"
    elif endpoint in DOCUMENT_BODY_ENDPOINTS:
        decision, reason = (
            "excluded",
            "document body/full-text route; metadata-only policy",
        )
    elif endpoint in DISCOVERY_ONLY_ENDPOINTS and not selected:
        decision, reason = "unavailable", "no enabled discovery provider remains"
    elif endpoint in DISCOVERY_ONLY_ENDPOINTS:
        decision, reason = "deferred", "created only from a parent catalog result"
    elif endpoint in NOT_ENUMERABLE_ENDPOINTS:
        decision, reason = "not_enumerable", NOT_ENUMERABLE_ENDPOINTS[endpoint]
    elif context.categories is not None and category not in context.categories:
        decision, reason = "filtered", "category filter"
    elif not _endpoint_matches(endpoint, context.endpoint_filters):
        decision, reason = "filtered", "endpoint filter"
    elif not selected:
        decision, reason = "unavailable", "no enabled provider remains"
    else:
        signature = _callable_signature(context, endpoint)
        explicit_required: set[str] = set()
        if signature is not None:
            explicit_required = {
                name
                for name, param in signature.parameters.items()
                if param.default is inspect.Parameter.empty and name != "kwargs"
            }
        symbol_route = (
            "symbol" in explicit_required or endpoint in OPTIONAL_SYMBOL_ENDPOINTS
        )
        endpoint_tasks = (
            _symbol_tasks(context, endpoint, selected)
            if symbol_route
            else _singleton_tasks(context, endpoint, selected)
        )

    coverage = CoverageDecision(
        endpoint=endpoint,
        category=category,
        available_providers=",".join(available),
        selected_providers=",".join(selected),
        decision=decision,
        reason=reason,
    )
    return endpoint_tasks, coverage


def _estimated_task_count(
    context: PlannerContext,
    endpoint: str,
    decision: CoverageDecision,
) -> int:
    """Cheap exact estimate for progress totals of the built-in planners."""
    if decision.decision != "included":
        return 0
    signature = _callable_signature(context, endpoint)
    explicit_required: set[str] = set()
    if signature is not None:
        explicit_required = {
            name
            for name, param in signature.parameters.items()
            if param.default is inspect.Parameter.empty and name != "kwargs"
        }
    symbol_route = (
        "symbol" in explicit_required or endpoint in OPTIONAL_SYMBOL_ENDPOINTS
    )
    if symbol_route:
        if endpoint == "index.constituents":
            return len(FMP_CONSTITUENT_INDEXES)
        if endpoint == "equity.shorts.fails_to_deliver":
            return sum(
                1 for _ in _sec_ftd_report_periods(context.start_date, context.end_date)
            )
        if endpoint in FMP_QUARTERLY_OWNERSHIP_ENDPOINTS:
            us_symbols = sum(item.market == "us" for item in context.assets)
            quarters = sum(
                1 for _ in _completed_quarters(context.start_date, context.end_date)
            )
            if endpoint == "equity.ownership.institutional":
                return (
                    math.ceil(us_symbols / FMP_OWNERSHIP_SYMBOL_BATCH_SIZE) * quarters
                )
            return us_symbols * quarters
        if endpoint == "equity.ownership.insider_trading":
            providers = set(decision.selected_providers.split(","))
            has_sec = "sec" in providers and any(
                asset.market == "us" for asset in context.assets
            )
            bulk = (
                sum(
                    1
                    for _ in _sec_insider_bulk_quarters(
                        context.start_date, context.end_date
                    )
                )
                if has_sec
                else 0
            )
            tail = (
                sum(asset.market == "us" for asset in context.assets)
                if has_sec
                and _sec_insider_unpublished_tail(context.start_date, context.end_date)
                is not None
                else 0
            )
            legacy_start = date.fromisoformat(context.start_date)
            legacy_end = min(
                date.fromisoformat(context.end_date),
                SEC_INSIDER_DATASET_START_DATE - timedelta(days=1),
            )
            legacy = (
                len(context.assets)
                if legacy_start <= legacy_end and providers & {"sec", "fmp"}
                else 0
            )
            statistics = len(context.assets) if "fmp" in providers else 0
            return bulk + tail + legacy + statistics
        if endpoint.startswith("currency."):
            symbols = len(context.currencies)
        elif endpoint.startswith("etf."):
            symbols = len(context.etfs)
        elif endpoint.startswith("index."):
            symbols = len(context.indices)
        elif endpoint.startswith("regulators.sec."):
            symbols = sum(item.market == "us" for item in context.assets)
        elif endpoint == "equity.compare.company_facts":
            symbols = sum(item.market == "us" for item in context.assets)
        else:
            symbols = len(context.assets)
        if endpoint == "equity.compare.company_facts":
            return symbols
        if endpoint == "equity.fundamental.filings":
            years = sum(1 for _ in _year_ranges(context.start_date, context.end_date))
            us_symbols = sum(item.market == "us" for item in context.assets)
            return us_symbols + (len(context.assets) - us_symbols) * years
        if endpoint in SYMBOL_YEAR_CHUNK_ENDPOINTS:
            years = sum(1 for _ in _year_ranges(context.start_date, context.end_date))
            return symbols * years
        if endpoint == "etf.nport_disclosure":
            years = max(
                0,
                int(context.end_date[:4]) - max(2019, int(context.start_date[:4])) + 1,
            )
            return symbols * years * 4
        if endpoint == "equity.ownership.form_13f":
            dates = sum(
                1
                for year in range(
                    max(2013, int(context.start_date[:4])),
                    int(context.end_date[:4]) + 1,
                )
                for month_day in ("03-31", "06-30", "09-30", "12-31")
                if f"{year}-{month_day}" <= context.end_date
            )
            return symbols * dates
        if endpoint in STATEMENT_ENDPOINTS:
            return symbols * (
                3
                if endpoint
                in {
                    "equity.fundamental.metrics",
                    "equity.fundamental.ratios",
                }
                else 2
            )
        if endpoint in FORWARD_ESTIMATE_ENDPOINTS | SYMBOL_PERIOD_ENDPOINTS:
            return symbols * 2
        return symbols

    if endpoint == "economy.calendar":
        return sum(1 for _ in _month_ranges(context.start_date, context.end_date))
    if endpoint == "economy.central_bank_holdings":
        official_dates = sum(
            context.start_date <= value <= context.end_date
            for value in _soma_as_of_dates()
        )
        return 2 + official_dates * 4
    if endpoint == "economy.unemployment":
        return len(UNEMPLOYMENT_DIMENSIONS)
    if endpoint == "economy.gdp.forecast":
        return len(GDP_FORECAST_DIMENSIONS)
    if endpoint == "economy.gdp.nominal":
        return len(GDP_NOMINAL_DIMENSIONS)
    if endpoint == "economy.gdp.real":
        return len(GDP_REAL_FREQUENCIES)
    if endpoint == "economy.interest_rates":
        return len(INTEREST_RATE_DURATIONS)
    if endpoint == "economy.total_factor_productivity":
        return len(TOTAL_FACTOR_PRODUCTIVITY_FREQUENCIES)
    if endpoint == "economy.house_price_index":
        return len(HOUSE_PRICE_DIMENSIONS)
    if endpoint == "economy.primary_dealer_positioning":
        return len(PRIMARY_DEALER_POSITION_GROUPS)
    if endpoint == "economy.composite_leading_indicator":
        return len(COMPOSITE_LEADING_INDICATOR_ADJUSTMENTS)
    if endpoint == "economy.cpi":
        return len(CPI_HARMONIZED_MODES)
    if endpoint == "fixedincome.rate.dpcredit":
        return len(DWPCR_RAW_PARAMETERS)
    if endpoint in SPREAD_MATURITIES:
        return len(SPREAD_MATURITIES[endpoint])
    if endpoint in GLOBAL_YEAR_CHUNK_ENDPOINTS:
        years = sum(1 for _ in _year_ranges(context.start_date, context.end_date))
        if endpoint == "news.world":
            has_fmp = "fmp" in decision.selected_providers.split(",")
            return years * (len(FMP_WORLD_NEWS_TOPICS) if has_fmp else 1) + int(has_fmp)
        return years
    if endpoint == "equity.discovery.filings":
        return sum(
            1
            for _ in _bounded_date_ranges(
                context.start_date,
                context.end_date,
                max_inclusive_days=90,
            )
        )
    if endpoint in DATE_GRID_ENDPOINTS:
        if endpoint == "fixedincome.government.yield_curve":
            selected = set(decision.selected_providers.split(","))
            weekdays = sum(
                1
                for _ in _date_grid(
                    context.start_date,
                    context.end_date,
                    DATE_GRID_ENDPOINTS[endpoint],
                )
            )
            has_full_history_provider = bool(
                selected & FULL_HISTORY_YIELD_CURVE_PROVIDERS
            )
            # Full-history adapters need one portable nominal task. Date-window
            # adapters (currently FMP) retain one resumable task per weekday.
            total = 1 if has_full_history_provider else weekdays
            if "fred" in selected:
                # One full-history archive task for each non-nominal curve
                # family; the adapter downloads the source series only once.
                total += len(GOVERNMENT_YIELD_CURVE_TYPES) - 1
            if "econdb" in selected:
                for country, cadence in _econdb_yield_curve_dimensions():
                    if country != "united_states":
                        total += 1
            return total
        count = sum(
            1
            for _ in _date_grid(
                context.start_date, context.end_date, DATE_GRID_ENDPOINTS[endpoint]
            )
        )
        if endpoint in DATE_GRID_LITERAL_FIELDS:
            fields = _model_fields(context, endpoint)
            model_field = fields.get(DATE_GRID_LITERAL_FIELDS[endpoint])
            values = (
                _literal_values(getattr(model_field, "annotation", None))
                if model_field is not None
                else []
            )
            dimension_count = len(
                [item for item in values if item not in {None, "all"}]
            )
            return count * max(1, dimension_count)
        return count * 2 if endpoint == "fixedincome.corporate.hqm" else count
    if endpoint == "fixedincome.mortgage_indices":
        return len(MORTGAGE_INDEX_GROUPS)
    if endpoint == "fixedincome.bond_indices":
        return len(_fred_bond_index_combinations())
    if endpoint == "cftc.cot_search":
        return len(CFTC_REPORT_MODES)
    if endpoint == "commodity.weather_bulletins":
        return sum(
            1
            for year in range(
                int(context.start_date[:4]), int(context.end_date[:4]) + 1
            )
            for month in range(1, 13)
            if f"{year}-{month:02d}-01" <= context.end_date
        )
    if endpoint == "commodity.petroleum_status_report":
        return len(_petroleum_status_dimensions())
    if endpoint == "commodity.psd_data":
        field = _model_fields(context, endpoint).get("commodity")
        return len(_description_values(field, "Valid commodities are:")) or 1
    if endpoint == "economy.direction_of_trade":
        return len(_imf_direction_countries()) or 1
    if endpoint == "economy.retail_prices":
        field = _model_fields(context, endpoint).get("item")
        item_count = len(_quoted_description_values(field, "Choices forfred:")) or 1
        return item_count * len(RETAIL_PRICE_REGIONS)
    if endpoint == "economy.survey.manufacturing_outlook_ny":
        field = _model_fields(context, endpoint).get("topic")
        return len(_literal_values(getattr(field, "annotation", None))) * 2
    if endpoint == "economy.country_profile":
        return len(_econdb_country_codes())
    if endpoint == "economy.export_destinations":
        return len(context.countries)
    if endpoint == "uscongress.bills":
        first = _congress_for_year(max(2000, int(context.start_date[:4])))
        last = _congress_for_year(int(context.end_date[:4]))
        return max(0, last - first + 1) * len(CONGRESS_BILL_TYPES)
    if endpoint == "uscongress.amendments":
        first = _congress_for_year(max(2000, int(context.start_date[:4])))
        last = _congress_for_year(int(context.end_date[:4]))
        return max(0, last - first + 1) * len(CONGRESS_AMENDMENT_TYPES)
    if endpoint in ENUMERATE_LITERAL_FIELD:
        field = _model_fields(context, endpoint).get(ENUMERATE_LITERAL_FIELD[endpoint])
        values = (
            _literal_values(getattr(field, "annotation", None))
            if field is not None
            else []
        )
        count = sum(item not in {None, "all"} for item in values)
        return count or 1
    return 1


def build_initial_plan(
    context: PlannerContext,
) -> tuple[list[DownloadTask], list[CoverageDecision]]:
    """Materialize a plan for tests and deliberately small filtered runs."""
    tasks: list[DownloadTask] = []
    coverage: list[CoverageDecision] = []
    for raw_endpoint, available in sorted(context.commands.items()):
        endpoint_iter, decision = _plan_endpoint(context, raw_endpoint, available)
        endpoint_tasks = list(endpoint_iter)
        if decision.decision == "included" and not endpoint_tasks:
            decision.decision = "unavailable"
            decision.reason = "resolved symbol or parameter universe is empty"
        decision.initial_task_count = len(endpoint_tasks)
        tasks.extend(endpoint_tasks)
        coverage.append(decision)
    return tasks, coverage


def populate_initial_plan(
    context: PlannerContext,
    manifest: "Manifest",
    *,
    insert_batch_size: int = 5000,
    plan_token: str = "default",
    plan_generation: str | None = None,
    show_progress: bool = False,
    contract_auditor: PlanContractAuditor | None = None,
) -> tuple[int, list[CoverageDecision]]:
    """Stream a potentially multi-million-task plan into SQLite in bounded memory."""
    total = 0
    coverage: list[CoverageDecision] = []
    insert_batch_size = max(1, int(insert_batch_size))
    commands = sorted(context.commands.items())
    phase_path = context.output_dir / "_state" / "downloader_phase.json"
    _write_json_atomic(
        phase_path,
        {
            "phase": "planning",
            "endpoints_completed": 0,
            "endpoints_total": len(commands),
            "generated_tasks": 0,
            "current_endpoint": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    progress = tqdm(
        total=len(commands),
        desc="openbb:plan",
        unit="endpoint",
        disable=not show_progress,
    )
    try:
        for endpoint_index, (raw_endpoint, available) in enumerate(commands):
            _write_json_atomic(
                phase_path,
                {
                    "phase": "planning",
                    "endpoints_completed": endpoint_index,
                    "endpoints_total": len(commands),
                    "generated_tasks": total,
                    "current_endpoint": raw_endpoint.lstrip("."),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            endpoint_iter, decision = _plan_endpoint(context, raw_endpoint, available)
            if contract_auditor is not None and decision.decision in {
                "included",
                "deferred",
            }:
                contract_auditor.register_endpoint(
                    decision.endpoint,
                    tuple(
                        value
                        for value in decision.selected_providers.split(",")
                        if value
                    ),
                )
                if decision.decision == "deferred":
                    contract_auditor.seed_discovery_contract(decision.endpoint)
            estimated = _estimated_task_count(
                context, raw_endpoint.lstrip("."), decision
            )
            endpoint_progress = tqdm(
                total=estimated or None,
                desc=f"plan:{raw_endpoint.lstrip('.')}"[:52],
                unit="task",
                position=1,
                leave=False,
                miniters=max(1, min(1000, estimated // 100 if estimated else 1000)),
                disable=not show_progress,
            )
            buffer: list[DownloadTask] = []
            endpoint_count = 0
            try:
                for task in endpoint_iter:
                    if contract_auditor is not None:
                        contract_auditor.observe_task(task)
                    buffer.append(task)
                    endpoint_count += 1
                    endpoint_progress.update(1)
                    if len(buffer) >= insert_batch_size:
                        manifest.upsert_tasks(
                            buffer,
                            plan_token=plan_token,
                            task_source="initial",
                            plan_generation=plan_generation,
                        )
                        buffer.clear()
                        endpoint_progress.set_postfix(
                            sqlite=endpoint_count, refresh=False
                        )
                if buffer:
                    manifest.upsert_tasks(
                        buffer,
                        plan_token=plan_token,
                        task_source="initial",
                        plan_generation=plan_generation,
                    )
            finally:
                endpoint_progress.close()
            if decision.decision == "included" and endpoint_count == 0:
                decision.decision = "unavailable"
                decision.reason = "resolved symbol or parameter universe is empty"
            decision.initial_task_count = endpoint_count
            total += endpoint_count
            coverage.append(decision)
            _write_json_atomic(
                phase_path,
                {
                    "phase": "planning",
                    "endpoints_completed": endpoint_index + 1,
                    "endpoints_total": len(commands),
                    "generated_tasks": total,
                    "current_endpoint": raw_endpoint.lstrip("."),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            progress.update(1)
            progress.set_postfix(tasks=total, refresh=False)
    finally:
        progress.close()
    return total, coverage


class Manifest:
    def __init__(self, path: Path, *, show_progress: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.show_progress = show_progress
        self.connection = sqlite3.connect(path, timeout=60.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function(
            "openbb_capability_domain",
            2,
            _sqlite_provider_capability_domain,
            deterministic=True,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._create_schema()
        self._schedule_keys: list[tuple[str, str]] = []
        self._schedule_token: str | None = None
        self._schedule_calls = 0
        self._schedule_cursor = 0
        self._completion_batch_depth = 0
        self._completion_batch_quarantines: list[DownloadTask] = []

    def _create_schema(self) -> None:
        progress = tqdm(
            total=4,
            desc="openbb:manifest schema",
            unit="stage",
            disable=not self.show_progress,
        )
        progress.set_postfix(stage="base tables", refresh=False)
        with _sqlite_progress(
            self.connection,
            "manifest:base tables",
            enabled=self.show_progress,
        ):
            self.connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                category TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                kwargs_json TEXT NOT NULL,
                providers_json TEXT NOT NULL,
                output_path TEXT NOT NULL,
                plan_token TEXT NOT NULL DEFAULT 'default',
                status TEXT NOT NULL DEFAULT 'pending',
                selected_provider TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                rows INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                provider_outcomes_json TEXT NOT NULL DEFAULT '{}',
                provider_evidence_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                execution_started_at TEXT,
                retry_not_before TEXT,
                transient_failures INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS provider_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                event TEXT NOT NULL,
                message TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
            )
        progress.update(1)
        progress.set_postfix(stage="column migrations", refresh=False)
        columns = {
            str(row[1]) for row in self.connection.execute("PRAGMA table_info(tasks)")
        }
        if "plan_token" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN plan_token TEXT NOT NULL DEFAULT 'default'"
            )
        if "active" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN active INTEGER NOT NULL DEFAULT 1"
            )
        if "task_source" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN task_source TEXT NOT NULL DEFAULT 'initial'"
            )
        if "plan_generation" not in columns:
            self.connection.execute("ALTER TABLE tasks ADD COLUMN plan_generation TEXT")
        if "provider_outcomes_json" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN provider_outcomes_json "
                "TEXT NOT NULL DEFAULT '{}'"
            )
        if "provider_evidence_json" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN provider_evidence_json "
                "TEXT NOT NULL DEFAULT '{}'"
            )
        if "execution_started_at" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN execution_started_at TEXT"
            )
        if "retry_not_before" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN retry_not_before TEXT"
            )
        if "transient_failures" not in columns:
            self.connection.execute(
                "ALTER TABLE tasks ADD COLUMN transient_failures "
                "INTEGER NOT NULL DEFAULT 0"
            )
        congress_retry_migration = "congress_http_500_task_backoff_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?", (congress_retry_migration,)
            ).fetchone()
            is None
        ):
            now_dt = datetime.now(timezone.utc)
            retry_rows = self.connection.execute(
                """
                SELECT task_id, attempts
                FROM tasks
                WHERE active=1
                  AND status IN ('pending','running')
                  AND retry_not_before IS NULL
                  AND attempts>0
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value='congress_gov'
                  )
                  AND (
                      LOWER(COALESCE(error,'')) LIKE '%http error 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%http 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%-> 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%status 500%'
                  )
                """
            ).fetchall()
            retry_updates: list[tuple[int, str, str]] = []
            for row in retry_rows:
                streak = max(1, min(16, int(row["attempts"] or 0)))
                retry_updates.append(
                    (
                        streak,
                        _task_retry_deadline(str(row["task_id"]), streak, now=now_dt),
                        str(row["task_id"]),
                    )
                )
            self.connection.executemany(
                """
                UPDATE tasks SET transient_failures=?, retry_not_before=?
                WHERE task_id=?
                """,
                retry_updates,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (
                    congress_retry_migration,
                    str(len(retry_updates)),
                    now_dt.isoformat(),
                ),
            )
        congress_cooldown_retry_migration = (
            "congress_legacy_transient_cooldown_backoff_v1"
        )
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (congress_cooldown_retry_migration,),
            ).fetchone()
            is None
        ):
            now_dt = datetime.now(timezone.utc)
            # The old provider-global transient block could overwrite the
            # preceding HTTP 500 in later task results with only "cooldown
            # until".  A large positive attempt count distinguishes these
            # legacy churn rows from quota responses, whose attempts are
            # deliberately decremented to zero by the worker.
            retry_rows = self.connection.execute(
                """
                SELECT task_id, attempts
                FROM tasks
                WHERE active=1
                  AND status IN ('pending','running')
                  AND retry_not_before IS NULL
                  AND attempts>=20
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value='congress_gov'
                  )
                  AND LOWER(COALESCE(error,'')) LIKE '%cooldown until%'
                  AND LOWER(COALESCE(error,'')) NOT LIKE '%quota%'
                  AND LOWER(COALESCE(error,'')) NOT LIKE '%rate limit%'
                """
            ).fetchall()
            retry_updates = []
            for row in retry_rows:
                streak = max(1, min(16, int(row["attempts"] or 0)))
                retry_updates.append(
                    (
                        streak,
                        _task_retry_deadline(str(row["task_id"]), streak, now=now_dt),
                        str(row["task_id"]),
                    )
                )
            self.connection.executemany(
                """
                UPDATE tasks SET transient_failures=?, retry_not_before=?
                WHERE task_id=?
                """,
                retry_updates,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (
                    congress_cooldown_retry_migration,
                    str(len(retry_updates)),
                    now_dt.isoformat(),
                ),
            )
        congress_repair_queue_migration = "congress_http_500_repair_queue_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (congress_repair_queue_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            cursor = self.connection.execute(
                """
                UPDATE tasks SET status=?, retry_not_before=NULL,
                    execution_started_at=NULL, updated_at=?
                WHERE active=1
                  AND status IN ('pending','running','failed')
                  AND endpoint IN (
                      'uscongress.bill_info','uscongress.amendment_info'
                  )
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value='congress_gov'
                  )
                  AND (
                      LOWER(COALESCE(error,'')) LIKE '%http error 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%http 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%-> 500%'
                      OR LOWER(COALESCE(error,'')) LIKE '%status 500%'
                  )
                """,
                (REPAIR_QUEUE_STATUS, now),
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (
                    congress_repair_queue_migration,
                    str(max(0, int(cursor.rowcount))),
                    now,
                ),
            )
        etf_name_migration = "yfinance_etf_info_missing_name_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?", (etf_name_migration,)
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    attempts=MAX(0, attempts-1),
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.yfinance'
                    ),
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND endpoint='etf.info'
                  AND provider_outcomes_json LIKE '%\"yfinance\":\"permanent\"%'
                  AND error LIKE '%YFinanceEtfInfoData%'
                  AND error LIKE '%name%Field required%'
                """,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (etf_name_migration, "complete", now),
            )
        yfinance_transport_revalidation = "yfinance_http_evidence_revalidation_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (yfinance_transport_revalidation,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # Older yfinance releases can print an HTTP 401/403 for one
            # quoteSummary/chart module and still return an empty or partial
            # model. Before transport evidence was captured, those responses
            # were indistinguishable from authoritative absence. Revalidate
            # every accepted Yahoo shard once across currencies, equities,
            # ETFs, indices, and fundamentals, plus any terminal row whose
            # Yahoo fallback was recorded empty. Existing Parquet remains in
            # place until an atomic replacement succeeds.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    selected_provider=NULL,
                    attempts=0,
                    rows=0,
                    error='requeued: Yahoo result predates per-request HTTP '
                        || 'evidence | ' || COALESCE(error,''),
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.yfinance'
                    ),
                    execution_started_at=NULL,
                    updated_at=created_at
                WHERE active=1
                  AND (
                      (status='success' AND selected_provider='yfinance')
                      OR (
                          status IN ('empty','unavailable')
                          AND json_extract(
                              provider_outcomes_json, '$.yfinance'
                          )='empty'
                      )
                  )
                """,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (
                    yfinance_transport_revalidation,
                    str(max(0, int(cursor.rowcount))),
                    now,
                ),
            )
        tiingo_allocation_migration = "tiingo_hourly_allocation_rate_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (tiingo_allocation_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # Tiingo describes HTTP quota exhaustion as running over an
            # "hourly request allocation".  Older classifiers persisted that
            # provider result as permanent, which could suppress Tiingo for
            # every endpoint sharing the same central worker.  Repair every
            # affected market/endpoint while preserving outcomes already
            # learned from the other providers.
            self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    attempts=MAX(0, attempts-1),
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.tiingo'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND provider_outcomes_json LIKE '%\"tiingo\":\"permanent\"%'
                  AND LOWER(COALESCE(error,'')) LIKE
                      '%hourly request allocation%'
                """,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (tiingo_allocation_migration, "complete", now),
            )
        tiingo_historical_false_empty_migration = (
            "tiingo_historical_non_authoritative_empty_v1"
        )
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (tiingo_historical_false_empty_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # Before provider outcomes were persisted, an authoritative empty
            # from an earlier fallback could win even when Tiingo ended with a
            # retryable quota error, the entitlement's 2020 history floor, or
            # a route-unrelated News API permission cached at provider scope.
            # Those rows are not evidence that the instrument has no price
            # history. Requeue the affected historical-price task across every
            # asset class; the current worker clamps Tiingo to 2020 and keeps
            # route/domain capability plus quota state separate.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    selected_provider=NULL,
                    attempts=0,
                    rows=0,
                    error='requeued: Tiingo historical result was not an '
                        || 'authoritative empty | ' || COALESCE(error,''),
                    provider_outcomes_json='{}',
                    execution_started_at=NULL,
                    updated_at=created_at
                WHERE active=1
                  AND status='empty'
                  AND endpoint IN (
                      'currency.price.historical',
                      'equity.price.historical',
                      'etf.historical',
                      'index.price.historical'
                  )
                  AND EXISTS (
                      SELECT 1 FROM json_each(providers_json)
                      WHERE value='tiingo'
                  )
                  AND (
                      LOWER(COALESCE(error,'')) LIKE
                          '%start date must be >= 2020-01-01%'
                      OR LOWER(COALESCE(error,'')) LIKE
                          '%hourly request allocation%'
                      OR LOWER(COALESCE(error,'')) LIKE
                          '%daily request allocation%'
                      OR LOWER(COALESCE(error,'')) LIKE
                          '%permission to access the news api%'
                  )
                """,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (
                    tiingo_historical_false_empty_migration,
                    str(max(0, int(cursor.rowcount))),
                    now,
                ),
            )
        dns_outcome_migration = "provider_dns_transient_outcomes_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (dns_outcome_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # Older classifiers persisted resolver failures as permanent (and
            # therefore skipped that fallback forever). Repair the provider
            # named by each combined error segment across every market while
            # preserving authoritative outcomes learned from other providers.
            rows = self.connection.execute(
                """
                SELECT task_id,status,attempts,error,provider_outcomes_json
                FROM tasks
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND provider_outcomes_json LIKE '%:\"permanent\"%'
                  AND (
                      LOWER(COALESCE(error,'')) LIKE '%could not resolve host%'
                      OR LOWER(COALESCE(error,'')) LIKE '%dnserror%'
                      OR LOWER(COALESCE(error,'')) LIKE '%failed to resolve%'
                      OR LOWER(COALESCE(error,'')) LIKE '%name resolution%'
                      OR LOWER(COALESCE(error,'')) LIKE '%nameresolutionerror%'
                  )
                """
            ).fetchall()
            updates: list[tuple[str, int, str, str]] = []
            for row in rows:
                outcomes = json.loads(row["provider_outcomes_json"] or "{}")
                repaired = {
                    provider: outcome
                    for provider, outcome in outcomes.items()
                    if not (
                        outcome == "permanent"
                        and _provider_has_dns_error(row["error"], provider)
                    )
                }
                removed = len(outcomes) - len(repaired)
                if removed:
                    updates.append(
                        (
                            _canonical_json(repaired),
                            max(0, int(row["attempts"] or 0) - removed),
                            now,
                            row["task_id"],
                        )
                    )
            self.connection.executemany(
                """
                UPDATE tasks SET status='pending',provider_outcomes_json=?,
                    attempts=?,updated_at=? WHERE task_id=?
                """,
                updates,
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (dns_outcome_migration, str(len(updates)), now),
            )
        sec_nport_empty_migration = "sec_nport_no_records_empty_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (sec_nport_empty_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # SEC's N-PORT search returns "No N-Port records found" when the
            # requested fund/quarter has no filing.  Older OpenBB exception
            # wrapping lost EmptyDataError's type and persisted this
            # authoritative empty result as a permanent adapter failure.  Keep
            # the empty SEC outcome so the next fallback remains eligible.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    provider_outcomes_json=json_set(
                        provider_outcomes_json, '$.sec', 'empty'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND endpoint='etf.nport_disclosure'
                  AND provider_outcomes_json LIKE '%\"sec\":\"permanent\"%'
                  AND (
                      LOWER(COALESCE(error,'')) LIKE '%no n-port records found%'
                      OR LOWER(COALESCE(error,'')) LIKE '%no nport records found%'
                  )
                """
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (sec_nport_empty_migration, str(cursor.rowcount), now),
            )
        econdb_archive_auth_migration = "econdb_archive_false_empty_auth_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (econdb_archive_auth_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # OpenBB's anonymous-token helper currently receives {"code":"anon"}
            # instead of an api_key. The subsequent authenticated series route
            # returns a detail-only object, which the adapter discards and
            # reports as an authoritative empty country. Requeue only the new
            # country archive shards affected by that exact adapter message;
            # the custom worker now preserves auth evidence explicitly.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error=NULL,
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.econdb'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND endpoint='fixedincome.government.yield_curve'
                  AND scope_key LIKE 'country=%/archive'
                  AND providers_json='["econdb"]'
                  AND status='empty'
                  AND LOWER(COALESCE(error,'')) LIKE '%response for,%returned empty%'
                """
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (econdb_archive_auth_migration, str(cursor.rowcount), now),
            )
        sec_form4_cache_migration = "sec_form4_cache_permanent_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (sec_form4_cache_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # Before the archive disabled every route's optional OpenBB HTTP/
            # SQL cache, concurrent Form 4 tasks could corrupt or race on the
            # adapter's process-global sec_form4.db. Those failures describe a
            # transport/cache implementation, not an authoritative symbol
            # outcome. Remove only SEC's stale permanent marker so the current
            # use_cache=False path gets one clean retry; preserve any fallback
            # outcomes already learned.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    attempts=MAX(0, attempts-1),
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.sec'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND endpoint='equity.ownership.insider_trading'
                  AND provider_outcomes_json LIKE '%"sec":"permanent"%'
                  AND (
                      LOWER(COALESCE(error,'')) LIKE '%sec_form4.db%'
                      OR LOWER(COALESCE(error,'')) LIKE '%database is locked%'
                      OR LOWER(COALESCE(error,'')) LIKE '%no such table: form4_data%'
                      OR LOWER(COALESCE(error,'')) LIKE '%local variable ''conn''%'
                  )
                """
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (sec_form4_cache_migration, str(cursor.rowcount), now),
            )
        sec_statement_period_migration = "sec_statement_quarter_translation_v1"
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (sec_statement_period_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # One archive revision invoked the shared companyfacts workaround
            # before translating the portable `quarter` value to SEC's
            # `quarterly` literal. Requeue only that exact adapter validation
            # outcome and preserve any fallback outcomes already learned.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    attempts=MAX(0, attempts-1),
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.sec'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND status NOT IN ('success','empty','unavailable')
                  AND endpoint IN (
                      'equity.fundamental.balance',
                      'equity.fundamental.balance_growth',
                      'equity.fundamental.cash',
                      'equity.fundamental.cash_growth',
                      'equity.fundamental.income',
                      'equity.fundamental.income_growth'
                  )
                  AND provider_outcomes_json LIKE '%"sec":"permanent"%'
                  AND LOWER(COALESCE(error,'')) LIKE
                      '%input should be%input_value=''quarter''%'
                """
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (sec_statement_period_migration, str(cursor.rowcount), now),
            )
        sec_statement_fallback_migration = (
            "sec_statement_quarter_translation_fallback_success_v1"
        )
        if (
            self.connection.execute(
                "SELECT 1 FROM archive_meta WHERE key=?",
                (sec_statement_fallback_migration,),
            ).fetchone()
            is None
        ):
            now = datetime.now(timezone.utc).isoformat()
            # A fallback could finish while the transient SEC literal bug was
            # still recorded as permanent, clearing the combined error text.
            # Requeue this exact quarterly statement shape so the primary SEC
            # result gets its intended chance to replace the fallback file.
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL,
                    attempts=MAX(0, attempts-1), rows=0, error=NULL,
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json, '$.sec'
                    ),
                    updated_at=created_at
                WHERE active=1
                  AND status='success'
                  AND scope_key LIKE '%/period=quarter'
                  AND endpoint IN (
                      'equity.fundamental.balance',
                      'equity.fundamental.balance_growth',
                      'equity.fundamental.cash',
                      'equity.fundamental.cash_growth',
                      'equity.fundamental.income',
                      'equity.fundamental.income_growth'
                  )
                  AND provider_outcomes_json LIKE '%"sec":"permanent"%'
                """
            )
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (sec_statement_fallback_migration, str(cursor.rowcount), now),
            )
        progress.update(1)
        progress.set_postfix(stage="legacy task labels", refresh=False)

        # These endpoints/scopes only exist after a finite parent catalog has
        # discovered them.  Preserve legacy follow-ups across initial-plan
        # reconciliation instead of mistaking them for obsolete planner rows.
        followup_endpoints = tuple(sorted(DISCOVERY_ONLY_ENDPOINTS))
        placeholders = ",".join("?" for _ in followup_endpoints)
        migration_key = "legacy_task_labels_v1"
        migration_done = self.connection.execute(
            "SELECT 1 FROM archive_meta WHERE key=?", (migration_key,)
        ).fetchone()
        has_tasks = False
        if migration_done is None:
            has_tasks = (
                self.connection.execute("SELECT 1 FROM tasks LIMIT 1").fetchone()
                is not None
            )
        if migration_done is None and has_tasks:
            with _sqlite_progress(
                self.connection,
                "manifest:legacy task labels",
                enabled=self.show_progress,
            ):
                self.connection.execute(
                    f"UPDATE tasks SET task_source='followup' "
                    f"WHERE endpoint IN ({placeholders}) "
                    "AND task_source!='followup'",
                    followup_endpoints,
                )
                self.connection.execute(
                    """
                UPDATE tasks SET task_source='followup'
                WHERE endpoint='economy.fred_search' AND scope_key LIKE 'release=%'
                  AND task_source!='followup'
                """
                )
                self.connection.execute(
                    """
                UPDATE tasks SET task_source='followup'
                WHERE endpoint IN ('index.price.historical','currency.price.historical')
                  AND task_source!='followup'
                """
                )
                # A previous planner expanded every index.available symbol into
                # invalid FMP constituent requests. Retire only those legacy rows.
                self.connection.execute(
                    """
                UPDATE tasks SET active=0
                WHERE endpoint='index.constituents' AND active!=0
                  AND scope_key NOT IN ('dowjones','sp500','nasdaq')
                """
                )
                self.connection.execute(
                    """
                UPDATE tasks SET task_source='initial'
                WHERE endpoint='index.constituents'
                  AND scope_key IN ('dowjones','sp500','nasdaq')
                  AND task_source!='initial'
                """
                )
                self.connection.execute(
                    """
                UPDATE tasks SET task_source='followup'
                WHERE endpoint IN ('news.company','news.world',
                                   'equity.fundamental.filings',
                                   'equity.discovery.filings')
                  AND scope_key GLOB '*page=[1-9]*'
                  AND task_source!='followup'
                """
                )
                self.connection.execute(
                    """
                    INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                    ON CONFLICT(key) DO UPDATE SET
                        value=excluded.value,
                        updated_at=excluded.updated_at
                    """,
                    (migration_key, "complete", datetime.now(timezone.utc).isoformat()),
                )
        progress.update(1)
        progress.set_postfix(stage="indexes and commit", refresh=False)
        with _sqlite_progress(
            self.connection,
            "manifest:indexes",
            enabled=self.show_progress,
        ):
            # The active-aware age index below is the hot scheduler contract.
            # Three older indexes contain the same leading scheduling fields
            # but consumed roughly 4.6 GiB at eight million tasks and forced
            # every discovered follow-up through three redundant B-tree
            # writes. Drop them idempotently before accepting more catalog
            # rows; ``idx_tasks_active_plan`` retains the monitor/count path.
            self.connection.execute("DROP INDEX IF EXISTS idx_tasks_status")
            self.connection.execute("DROP INDEX IF EXISTS idx_tasks_schedule")
            self.connection.execute("DROP INDEX IF EXISTS idx_tasks_schedule_age")
            self.connection.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_tasks_schedule_age_v2
            ON tasks(active, status, plan_token, category, endpoint, updated_at, task_id)
            """
            )
            self.connection.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_tasks_active_plan
            ON tasks(active, plan_token, status, endpoint)
            """
            )
            self.connection.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_tasks_retry_not_before
            ON tasks(active, status, plan_token, retry_not_before)
            WHERE retry_not_before IS NOT NULL
            """
            )
            # SEC statements are six projections of the same companyfacts
            # artifact. Scope ordering lets the scheduler admit sibling
            # balance/cash/income tasks together so the in-memory standardized
            # object is parsed once instead of once per endpoint.
            self.connection.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_tasks_sec_statement_affinity
            ON tasks(active, status, plan_token, endpoint, updated_at, scope_key, task_id)
            WHERE endpoint IN (
                'equity.fundamental.balance',
                'equity.fundamental.balance_growth',
                'equity.fundamental.cash',
                'equity.fundamental.cash_growth',
                'equity.fundamental.income',
                'equity.fundamental.income_growth'
            )
            """
            )
            self.connection.commit()
        progress.update(1)
        progress.close()

    def close(self) -> None:
        self.connection.close()

    def meta_value(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM archive_meta WHERE key=?", (str(key),)
        ).fetchone()
        return None if row is None else str(row[0])

    def set_meta_value(self, key: str, value: str) -> None:
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (str(key), str(value), datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()

    def active_task_count(self, plan_token: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE active=1 AND plan_token=?",
            (plan_token,),
        ).fetchone()
        return int(row[0])

    def has_active_tasks_outside_plan(self, plan_token: str) -> bool:
        """Cheaply detect an interrupted replacement plan before fast resume."""
        return (
            self.connection.execute(
                "SELECT 1 FROM tasks WHERE active=1 AND plan_token!=? LIMIT 1",
                (plan_token,),
            ).fetchone()
            is not None
        )

    def upsert_tasks(
        self,
        tasks: Iterable[DownloadTask],
        *,
        plan_token: str = "default",
        task_source: str = "initial",
        plan_generation: str | None = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                task.task_id,
                task.endpoint,
                task.category,
                task.scope_key,
                _canonical_json(task.kwargs),
                _canonical_json(task.providers),
                task.output_path,
                plan_token,
                task_source,
                plan_generation,
                now,
                now,
            )
            for task in tasks
        ]
        if not rows:
            return 0
        before = self.connection.total_changes
        # A provider-chain update invalidates a task only when it changes the
        # proof behind its current state: pending/failure work is retried,
        # success is retried if its producing provider was removed, and empty
        # is retried if a newly added provider has not yet answered. Merely
        # reordering the same fallback set never redownloads accepted data.
        provider_contract_changed = """
            tasks.providers_json != excluded.providers_json AND (
                tasks.status NOT IN ('success','empty')
                OR (
                    tasks.status='success'
                    AND NOT EXISTS (
                        SELECT 1 FROM json_each(excluded.providers_json)
                        WHERE value=tasks.selected_provider
                    )
                )
                OR (
                    tasks.status='empty'
                    AND EXISTS (
                        SELECT value FROM json_each(excluded.providers_json)
                        EXCEPT
                        SELECT value FROM json_each(tasks.providers_json)
                    )
                )
            )
        """
        self.connection.executemany(
            f"""
            INSERT INTO tasks (
                task_id, endpoint, category, scope_key, kwargs_json,
                providers_json, output_path, plan_token, task_source,
                plan_generation, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                status=CASE
                    WHEN {provider_contract_changed}
                    THEN 'pending'
                    ELSE tasks.status
                END,
                selected_provider=CASE
                    WHEN {provider_contract_changed}
                    THEN NULL
                    ELSE tasks.selected_provider
                END,
                attempts=CASE
                    WHEN {provider_contract_changed}
                    THEN 0
                    ELSE tasks.attempts
                END,
                rows=CASE
                    WHEN {provider_contract_changed}
                    THEN 0
                    ELSE tasks.rows
                END,
                error=CASE
                    WHEN {provider_contract_changed}
                    THEN NULL
                    ELSE tasks.error
                END,
                retry_not_before=CASE
                    WHEN {provider_contract_changed}
                    THEN NULL
                    ELSE tasks.retry_not_before
                END,
                transient_failures=CASE
                    WHEN {provider_contract_changed}
                    THEN 0
                    ELSE tasks.transient_failures
                END,
                provider_outcomes_json=CASE
                    WHEN {provider_contract_changed}
                    THEN '{{}}'
                    ELSE tasks.provider_outcomes_json
                END,
                providers_json=excluded.providers_json,
                output_path=excluded.output_path,
                plan_token=excluded.plan_token,
                task_source=excluded.task_source,
                plan_generation=excluded.plan_generation,
                active=1,
                updated_at=CASE
                    WHEN {provider_contract_changed}
                    THEN excluded.updated_at
                    ELSE tasks.updated_at
                END
            """,
            rows,
        )
        self.connection.commit()
        # Follow-up discovery can introduce an endpoint that was absent from
        # the cached fair-scheduler key list.  Invalidate after every upsert so
        # rolling refill sees newly persisted FRED/CFTC/BLS/index/Congress work
        # in the same process instead of deferring it until a periodic refresh
        # or supervisor restart.
        self._schedule_keys = []
        self._schedule_token = None
        return self.connection.total_changes - before

    def reconcile_initial_plan(
        self,
        plan_token: str,
        plan_generation: str,
        *,
        show_progress: bool = False,
    ) -> int:
        """Deactivate initial tasks that the current planner no longer emits."""
        before = self.connection.total_changes
        with _sqlite_progress(
            self.connection,
            "manifest:reconcile initial plan",
            enabled=show_progress,
        ):
            self.connection.execute(
                """
            UPDATE tasks SET active=0
            WHERE plan_token=? AND task_source='initial' AND active=1
              AND COALESCE(plan_generation,'') != ?
            """,
                (plan_token, plan_generation),
            )
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at)
            VALUES ('active_plan_token',?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (plan_token, datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()
        return self.connection.total_changes - before - 1

    def reconcile_active_plan_membership(
        self,
        plan_token: str,
        *,
        compatible_plan_tokens: set[str],
        followup_endpoints: set[str],
        show_progress: bool = False,
    ) -> tuple[int, int]:
        """Adopt compatible follow-ups and retire every superseded active plan.

        Task IDs are independent of a plan token, so regenerated initial tasks
        automatically move to the new plan through ``upsert_tasks``.  Dynamic
        catalog follow-ups are different: they may not be rediscovered until
        their already-completed parent is queried again.  Preserve those only
        when the CLI archive scope is compatible and the endpoint remains in
        the new coverage contract, then deactivate every remaining task owned
        by another plan.  This enforces the invariant that one output
        directory has exactly one active plan.
        """
        other_tokens = {
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT DISTINCT plan_token FROM tasks
                WHERE active=1 AND plan_token!=?
                """,
                (plan_token,),
            ).fetchall()
        }
        if not other_tokens:
            return 0, 0

        adopt_tokens = sorted(other_tokens & set(compatible_plan_tokens))
        allowed_endpoints = sorted(str(item) for item in followup_endpoints)
        migrated = 0
        if adopt_tokens and allowed_endpoints:
            token_marks = ",".join("?" for _ in adopt_tokens)
            endpoint_marks = ",".join("?" for _ in allowed_endpoints)
            before = self.connection.total_changes
            with _sqlite_progress(
                self.connection,
                "manifest:adopt compatible followups",
                enabled=show_progress,
            ):
                self.connection.execute(
                    f"""
                    UPDATE tasks SET plan_token=?
                    WHERE active=1 AND task_source='followup'
                      AND plan_token IN ({token_marks})
                      AND endpoint IN ({endpoint_marks})
                    """,
                    (plan_token, *adopt_tokens, *allowed_endpoints),
                )
            migrated = self.connection.total_changes - before

        before = self.connection.total_changes
        now = datetime.now(timezone.utc).isoformat()
        with _sqlite_progress(
            self.connection,
            "manifest:retire superseded plans",
            enabled=show_progress,
        ):
            self.connection.execute(
                """
                UPDATE tasks SET active=0, updated_at=?
                WHERE active=1 AND plan_token!=?
                """,
                (now, plan_token),
            )
        retired = self.connection.total_changes - before
        self.connection.commit()
        self._schedule_keys = []
        self._schedule_token = None
        return migrated, retired

    def prune_disabled_providers(
        self,
        plan_token: str,
        disabled_providers: set[str],
        *,
        show_progress: bool = False,
    ) -> tuple[int, int]:
        """Remove unusable providers from existing active follow-up tasks."""
        if not disabled_providers:
            return 0, 0
        disabled = tuple(sorted(disabled_providers))
        placeholders = ",".join("?" for _ in disabled)
        with _sqlite_progress(
            self.connection,
            "manifest:scan disabled providers",
            enabled=show_progress,
        ):
            rows = self.connection.execute(
                f"""
            SELECT task_id,providers_json
            FROM tasks
            WHERE active=1 AND plan_token=?
              AND EXISTS (
                  SELECT 1 FROM json_each(tasks.providers_json)
                  WHERE value IN ({placeholders})
              )
            ORDER BY task_id
            """,
                (plan_token, *disabled),
            ).fetchall()
        progress = tqdm(
            rows,
            total=len(rows),
            desc="openbb:prune providers",
            unit="task",
            disable=not show_progress,
        )
        updates: list[tuple[str, int, str]] = []
        updated = 0
        deactivated = 0
        try:
            for row in progress:
                providers = list(json.loads(row["providers_json"]))
                filtered = [
                    provider
                    for provider in providers
                    if provider not in disabled_providers
                ]
                if filtered == providers:
                    continue
                active = int(bool(filtered))
                updates.append((_canonical_json(filtered), active, row["task_id"]))
                updated += 1
                deactivated += int(not active)
                if len(updates) >= 5000:
                    self.connection.executemany(
                        "UPDATE tasks SET providers_json=?,active=? WHERE task_id=?",
                        updates,
                    )
                    self.connection.commit()
                    updates.clear()
                    progress.set_postfix(
                        updated=updated,
                        inactive=deactivated,
                        refresh=False,
                    )
            if updates:
                self.connection.executemany(
                    "UPDATE tasks SET providers_json=?,active=? WHERE task_id=?",
                    updates,
                )
                self.connection.commit()
        finally:
            progress.close()
        return updated, deactivated

    def ensure_fred_release_continuations(
        self,
        context: PlannerContext,
        plan_token: str,
        *,
        show_progress: bool = False,
    ) -> int:
        """Backfill continuation pages for full FRED release searches."""
        with _sqlite_progress(
            self.connection,
            "manifest:scan FRED pagination",
            enabled=show_progress,
        ):
            rows = self.connection.execute(
                """
            SELECT * FROM tasks
            WHERE active=1 AND plan_token=?
              AND endpoint='economy.fred_search'
              AND status='success' AND rows>=?
              AND scope_key LIKE 'release=%'
            ORDER BY scope_key
            """,
                (plan_token, FRED_RELEASE_PAGE_SIZE),
            ).fetchall()
            existing_scopes = {
                str(row["scope_key"])
                for row in self.connection.execute(
                    """
                    SELECT scope_key FROM tasks
                    WHERE active=1 AND plan_token=?
                      AND endpoint='economy.fred_search'
                    """,
                    (plan_token,),
                )
            }
        progress = tqdm(
            rows,
            total=len(rows),
            desc="openbb:fred release pagination",
            unit="page",
            disable=not show_progress,
        )
        continuations: list[DownloadTask] = []
        try:
            for row in progress:
                task = self._task_from_row(row)
                continuation = _fred_release_continuation_task(
                    context, task, int(row["rows"])
                )
                if (
                    continuation is not None
                    and continuation.scope_key not in existing_scopes
                ):
                    continuations.append(continuation)
                    existing_scopes.add(continuation.scope_key)
        finally:
            progress.close()
        self.upsert_tasks(
            continuations,
            plan_token=plan_token,
            task_source="followup",
        )
        return len(continuations)

    def ensure_fmp_page_continuations(
        self,
        context: PlannerContext,
        plan_token: str,
        *,
        show_progress: bool = False,
    ) -> int:
        """Repair every missing FMP continuation after a full successful page."""
        endpoints = tuple(sorted(FMP_MANIFEST_PAGINATED_ENDPOINTS))
        placeholders = ",".join("?" for _ in endpoints)
        with _sqlite_progress(
            self.connection,
            "manifest:scan FMP pagination",
            enabled=show_progress,
        ):
            rows = self.connection.execute(
                f"""
                SELECT * FROM tasks
                WHERE active=1 AND plan_token=?
                  AND endpoint IN ({placeholders})
                  AND selected_provider='fmp'
                  AND status='success'
                  AND CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)>0
                  AND rows>=CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)
                ORDER BY endpoint,scope_key
                """,
                (plan_token, *endpoints),
            ).fetchall()
            existing_scopes = {
                (str(row["endpoint"]), str(row["scope_key"]))
                for row in self.connection.execute(
                    f"""
                    SELECT endpoint,scope_key FROM tasks
                    WHERE active=1 AND plan_token=?
                      AND endpoint IN ({placeholders})
                    """,
                    (plan_token, *endpoints),
                )
            }
        progress = tqdm(
            rows,
            total=len(rows),
            desc="openbb:FMP pagination repair",
            unit="page",
            disable=not show_progress,
        )
        continuations: list[DownloadTask] = []
        unreadable = 0
        try:
            for row in progress:
                task = self._task_from_row(row)
                page = int(task.kwargs.get("page") or 0)
                next_scope = re.sub(r"page=\d+", f"page={page + 1}", task.scope_key)
                if next_scope == task.scope_key:
                    next_scope = f"{task.scope_key}/page={page + 1}"
                scope_id = (task.endpoint, next_scope)
                if scope_id in existing_scopes:
                    continue
                try:
                    records = pq.read_table(str(row["output_path"])).to_pylist()
                    signature = _page_content_signature(records)
                except (OSError, pa.ArrowException, TypeError, ValueError):
                    unreadable += 1
                    continue
                kwargs = dict(
                    task.kwargs,
                    page=page + 1,
                    _previous_page_signature=signature,
                )
                continuations.append(
                    make_task(
                        context,
                        task.endpoint,
                        next_scope,
                        kwargs,
                        ("fmp",),
                    )
                )
                existing_scopes.add(scope_id)
                progress.set_postfix(
                    added=len(continuations), unreadable=unreadable, refresh=False
                )
        finally:
            progress.close()
        self.upsert_tasks(
            continuations,
            plan_token=plan_token,
            task_source="followup",
        )
        return len(continuations)

    def ensure_fred_series_followups(
        self,
        context: PlannerContext,
        plan_token: str,
        *,
        show_progress: bool = False,
    ) -> int:
        """Backfill FRED series tasks missing from successful release catalogs.

        Follow-up discovery normally persists these tasks immediately after a
        release page succeeds.  This reconciliation covers legacy manifests or
        interrupted discovery without rewriting the hundreds of thousands of
        child tasks that already exist.
        """
        providers = select_providers(
            "economy.fred_series",
            context.commands.get(".economy.fred_series", []),
            context,
        )
        if not providers:
            return 0

        with _sqlite_progress(
            self.connection,
            "manifest:load FRED series scopes",
            enabled=show_progress,
        ):
            existing_rows = self.connection.execute(
                """
                SELECT scope_key FROM tasks
                WHERE active=1 AND plan_token=?
                  AND endpoint='economy.fred_series'
                """,
                (plan_token,),
            ).fetchall()
        existing_scopes = {str(row["scope_key"]) for row in existing_rows}

        with _sqlite_progress(
            self.connection,
            "manifest:load FRED release catalogs",
            enabled=show_progress,
        ):
            parent_rows = self.connection.execute(
                """
                SELECT output_path FROM tasks
                WHERE active=1 AND plan_token=?
                  AND endpoint='economy.fred_search'
                  AND status='success'
                  AND scope_key LIKE 'release=%'
                ORDER BY scope_key
                """,
                (plan_token,),
            ).fetchall()

        progress = tqdm(
            parent_rows,
            total=len(parent_rows),
            desc="openbb:fred series reconciliation",
            unit="catalog",
            disable=not show_progress,
        )
        pending: list[DownloadTask] = []
        ensured = 0
        skipped_files = 0

        def flush() -> None:
            if not pending:
                return
            self.upsert_tasks(
                pending,
                plan_token=plan_token,
                task_source="followup",
            )
            pending.clear()

        try:
            for row in progress:
                output_path = row["output_path"]
                if not output_path:
                    skipped_files += 1
                    continue
                path = Path(str(output_path))
                try:
                    parquet_file = pq.ParquetFile(path)
                    names = parquet_file.schema_arrow.names
                    series_field = next(
                        (
                            candidate
                            for candidate in ("series_id", "symbol", "id")
                            if candidate in names
                        ),
                        None,
                    )
                    if series_field is None:
                        skipped_files += 1
                        continue
                    values = parquet_file.read(columns=[series_field]).column(
                        series_field
                    )
                except (OSError, pa.ArrowException):
                    # prepare_run/audit will independently requeue a missing or
                    # unreadable successful parent file.
                    skipped_files += 1
                    continue

                for raw_series_id in values.to_pylist():
                    if raw_series_id is None:
                        continue
                    series_id = str(raw_series_id).strip()
                    if not series_id or series_id in existing_scopes:
                        continue
                    pending.append(
                        make_task(
                            context,
                            "economy.fred_series",
                            series_id,
                            {
                                "symbol": series_id,
                                "start_date": context.start_date,
                                "end_date": context.end_date,
                                "limit": 100000,
                            },
                            providers,
                        )
                    )
                    existing_scopes.add(series_id)
                    ensured += 1
                    if len(pending) >= 5000:
                        flush()
                progress.set_postfix(
                    added=ensured,
                    skipped=skipped_files,
                    refresh=False,
                )
            flush()
        finally:
            progress.close()
        return ensured

    def repair_sec_filings_columnar_shard_bug(self, *, plan_token: str) -> int:
        """Requeue SEC filing results produced before columnar shards were read.

        The historical SEC submissions files are column-oriented JSON.  The
        previous workaround ignored that mapping layout, so any company with
        an older shard could be persisted as a plausible success containing
        exactly the 1,000 rows from ``filings.recent``.  This one-time repair
        conservatively rechecks every such realized partition; the corrected
        fetch is atomic and will overwrite the same Parquet shard.

        Record the migration even when no rows exist.  A database first
        created by corrected code must not requeue a legitimate exact-1,000
        result on every later restart.
        """
        migration_key = "sec_filings_columnar_shards_v1"
        if self.meta_value(migration_key) is not None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    selected_provider=NULL,
                    rows=0,
                    error='requeued: verify SEC historical columnar submissions shards',
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND endpoint='equity.fundamental.filings'
                  AND selected_provider='sec'
                  AND status='success' AND rows=1000
                """,
                (plan_token,),
            )
            repaired = max(0, int(cursor.rowcount))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (migration_key, str(repaired), now),
            )
        return repaired

    def repair_sec_filing_headers_index_page_bug(self, *, plan_token: str) -> int:
        """Requeue SEC filing headers falsely emptied by a derived 404 URL.

        Older SEC accessions commonly expose ``*-index.html`` without the
        OpenBB-derived ``*-index-headers.htm`` companion.  Requeue only rows
        carrying that exact historical failure signature; genuine empty
        results and unrelated failures remain untouched.  Reset provider
        outcomes so the corrected SEC implementation is eligible immediately.
        """
        migration_key = "sec_filing_headers_index_page_v1"
        if self.meta_value(migration_key) is not None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending',
                    selected_provider=NULL,
                    rows=0,
                    error='requeued: use canonical SEC filing index page',
                    provider_outcomes_json='{}',
                    execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND endpoint='regulators.sec.filing_headers'
                  AND selected_provider='sec'
                  AND status='empty'
                  AND LOWER(COALESCE(error,'')) LIKE '%index headers table%'
                  AND LOWER(COALESCE(error,'')) LIKE '%404%'
                """,
                (plan_token,),
            )
            repaired = max(0, int(cursor.rowcount))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (migration_key, str(repaired), now),
            )
        return repaired

    def repair_invalid_country_all_filters(self, *, plan_token: str) -> int:
        """Remove non-portable ``country=all`` filters from unfinished tasks.

        OpenBB command schemas merge provider fields.  A plain optional
        ``country: str`` therefore does not establish that the provider accepts
        an ``all`` sentinel.  Older plans added that sentinel to every route and
        FMP rejected ETF-search tasks during local validation.  Preserve the
        stable task/scope identity, remove only the no-op filter, and prioritize
        every affected unfinished task so an existing resumable plan is fixed
        without a destructive rebuild.
        """
        migration_key = "invalid_country_all_filters_v1"
        if self.meta_value(migration_key) is not None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" for _ in COUNTRY_ALL_ENDPOINTS)
        with self.connection:
            cursor = self.connection.execute(
                f"""
                UPDATE tasks SET
                    kwargs_json=json_remove(kwargs_json, '$.country'),
                    status='pending',
                    selected_provider=NULL,
                    attempts=0,
                    rows=0,
                    error='requeued: remove invalid optional country=all filter',
                    provider_outcomes_json='{{}}',
                    execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND status!='success'
                  AND json_extract(kwargs_json, '$.country')='all'
                  AND endpoint NOT IN ({placeholders})
                """,
                (plan_token, *sorted(COUNTRY_ALL_ENDPOINTS)),
            )
            repaired = max(0, int(cursor.rowcount))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (migration_key, str(repaired), now),
            )
        return repaired

    def repair_provider_error_classification(
        self, *, plan_token: str
    ) -> tuple[int, int]:
        """Repair terminal rows created by older cross-provider classifiers."""
        migration_key = "provider_error_classification_v1"
        if self.meta_value(migration_key) is not None:
            return 0, 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            transient = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: retry transient provider JSON response',
                    provider_outcomes_json='{}', execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status='failed'
                  AND LOWER(COALESCE(error,'')) LIKE '%jsondecodeerror%'
                """,
                (plan_token,),
            ).rowcount
            entitlement = self.connection.execute(
                """
                UPDATE tasks SET
                    status='unavailable', selected_provider='fmp', rows=0,
                    error='fmp: unavailable for task (HTTP 402 Payment Required)',
                    provider_outcomes_json='{"fmp":"unavailable"}',
                    execution_started_at=NULL, updated_at=?
                WHERE active=1 AND plan_token=? AND status='failed'
                  AND selected_provider='fmp'
                  AND LOWER(COALESCE(error,'')) LIKE '%http 402%payment required%'
                """,
                (now, plan_token),
            ).rowcount
            repaired_transient = max(0, int(transient))
            repaired_entitlement = max(0, int(entitlement))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (
                    migration_key,
                    _canonical_json(
                        {
                            "transient": repaired_transient,
                            "entitlement": repaired_entitlement,
                        }
                    ),
                    now,
                ),
            )
        return repaired_transient, repaired_entitlement

    def repair_bls_missing_series_title_bug(self, *, plan_token: str) -> int:
        """Requeue LABSTAT tasks failed before nullable title normalization."""
        migration_key = "bls_missing_series_title_v1"
        if self.meta_value(migration_key) is not None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: LABSTAT series catalog has nullable title',
                    provider_outcomes_json='{}', execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status='failed'
                  AND endpoint='economy.survey.bls_series'
                  AND LOWER(COALESCE(error,'')) LIKE '%binderexception%'
                  AND LOWER(COALESCE(error,'')) LIKE '%series_title%'
                """,
                (plan_token,),
            )
            repaired = max(0, int(cursor.rowcount))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (migration_key, str(repaired), now),
            )
        return repaired

    def repair_adaptable_parameter_constraints(
        self,
        parameter_maximums: Mapping[tuple[str, str, str], int],
        *,
        plan_token: str,
    ) -> int:
        """Requeue false denials at the maximum allowed by the credential.

        Pageable routes retain complete continuation. For non-pageable routes,
        the explicit upstream maximum is the complete surface OpenBB can
        expose under the configured entitlement; retaining those rows is more
        accurate than discarding the route as wholly unavailable.
        """
        total = 0
        for (provider, endpoint, parameter), maximum in sorted(
            parameter_maximums.items()
        ):
            if provider != "fmp" or parameter != "limit" or int(maximum) <= 0:
                continue
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    kwargs_json=json_set(kwargs_json, '$.limit', ?),
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: apply provider limit and continue by page',
                    provider_outcomes_json='{}', execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status='unavailable'
                  AND endpoint=?
                  AND (
                      json_type(kwargs_json,'$.limit') IS NULL
                      OR CAST(json_extract(kwargs_json,'$.limit') AS INTEGER)>?
                  )
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value=?
                  )
                  AND LOWER(COALESCE(error,'')) LIKE '%limit%must be between%'
                """,
                (int(maximum), plan_token, endpoint, int(maximum), provider),
            )
            total += max(0, int(cursor.rowcount))
        self.connection.commit()
        return total

    def repair_adaptable_query_shapes(
        self,
        omitted_parameters: Mapping[tuple[str, str], Sequence[str]],
        *,
        plan_token: str,
    ) -> int:
        """Requeue terminal denials now covered by a legal query shape."""
        total = 0
        for (provider, endpoint), parameters in sorted(omitted_parameters.items()):
            if (
                provider != "fmp"
                or endpoint != "equity.historical_market_cap"
                or not {"start_date", "end_date"}.issubset(set(parameters))
            ):
                continue
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: use entitlement-compatible undated query',
                    provider_outcomes_json='{}', execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status='unavailable'
                  AND endpoint=?
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value=?
                  )
                  AND LOWER(COALESCE(error,'')) LIKE
                      '%value set for ''from''%not available%subscription%'
                """,
                (plan_token, endpoint, provider),
            )
            total += max(0, int(cursor.rowcount))
        self.connection.commit()
        return total

    def finalize_resolved_provider_outcome_pending(self, *, plan_token: str) -> int:
        """Finalize pending rows after every fallback has a terminal outcome.

        Older workers let an early transient-attempt flag outrank a later
        authoritative outcome from the same provider. Outcome completeness is
        the invariant: a task is retryable only while at least one configured
        provider lacks a terminal outcome.
        """
        cursor = self.connection.execute(
            """
            UPDATE tasks SET
                status=CASE
                    WHEN EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='unavailable'
                    ) AND EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='empty'
                    ) THEN 'unavailable'
                    WHEN EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='empty'
                    ) AND NOT EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='permanent'
                    ) THEN 'empty'
                    WHEN EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='permanent'
                    ) THEN 'failed'
                    WHEN EXISTS (
                        SELECT 1 FROM json_each(provider_outcomes_json)
                        WHERE value='unavailable'
                    ) THEN 'unavailable'
                    ELSE 'failed'
                END,
                error='finalized: every configured provider has a terminal outcome'
                    || CASE WHEN COALESCE(error,'')='' THEN ''
                            ELSE ' | ' || error END,
                execution_started_at=NULL,
                updated_at=?
            WHERE active=1 AND plan_token=? AND status='pending'
              AND json_array_length(providers_json)>0
              AND NOT EXISTS (
                  SELECT 1 FROM json_each(tasks.providers_json) AS provider
                  WHERE json_type(
                      tasks.provider_outcomes_json,
                      '$.' || provider.value
                  ) IS NULL
              )
            """,
            (datetime.now(timezone.utc).isoformat(), plan_token),
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def repair_sec_nport_list_container_bug(self, *, plan_token: str) -> int:
        """Retry N-PORT filings rejected before XML containers were normalized."""
        migration_key = "sec_nport_transformer_contract_normalization_v4"
        if self.meta_value(migration_key) is not None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL,
                    attempts=MAX(0, attempts-1), rows=0,
                    error='requeued: normalize complete SEC N-PORT transformer contract',
                    provider_outcomes_json=json_remove(provider_outcomes_json, '$.sec'),
                    execution_started_at=NULL,
                    updated_at='0001-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND endpoint='etf.nport_disclosure'
                  AND status NOT IN ('success','empty','unavailable')
                  AND json_extract(provider_outcomes_json, '$.sec')='permanent'
                  AND (
                      LOWER(COALESCE(error,''))
                          LIKE '%list%object has no attribute%get%'
                      OR LOWER(COALESCE(error,''))
                          LIKE '%argument of type ''nonetype'' is not iterable%'
                      OR LOWER(COALESCE(error,'')) LIKE '%keyerror:%'
                      OR LOWER(COALESCE(error,''))
                          LIKE '%providerresponseshapeerror:%'
                  )
                """,
                (plan_token,),
            )
            repaired = max(0, int(cursor.rowcount))
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (migration_key, str(repaired), now),
            )
        return repaired

    def prepare_run(
        self,
        *,
        retry_failed: bool,
        retry_repair_queue: bool = False,
        retry_permanent: bool = True,
        retry_empty: bool,
        refresh: bool,
        repair_legacy: bool = True,
        verify_successful_shards: bool = True,
        plan_token: str | None = None,
        show_progress: bool = False,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        plan_clause = "" if plan_token is None else " AND plan_token=?"
        plan_args: tuple[Any, ...] = () if plan_token is None else (plan_token,)
        stages = (
            (7 if verify_successful_shards else 4)
            + 3 * int(repair_legacy)
            + int(retry_failed)
            + int(retry_repair_queue)
            + int(retry_empty)
            + int(refresh)
        )
        progress = tqdm(
            total=stages,
            desc="openbb:prepare manifest",
            unit="stage",
            disable=not show_progress,
        )

        def execute_stage(
            label: str,
            statement: str,
            parameters: Sequence[Any] = (),
        ) -> None:
            progress.set_postfix(stage=label, refresh=False)
            with _sqlite_progress(
                self.connection,
                f"prepare:{label}",
                enabled=show_progress,
            ):
                self.connection.execute(statement, parameters)
            progress.update(1)

        self.adopt_published_outputs(
            plan_token=plan_token,
            show_progress=show_progress,
        )
        execute_stage(
            "recover running",
            f"UPDATE tasks SET status='pending', execution_started_at=NULL, "
            f"updated_at=? WHERE active=1 AND status='running'{plan_clause}",
            (now, *plan_args),
        )
        execute_stage(
            "recover legacy exhausted",
            f"UPDATE tasks SET status='pending', execution_started_at=NULL, "
            f"error='requeued: transient retries are evidence-driven, not count-limited', "
            f"updated_at=? WHERE active=1 AND status='exhausted'{plan_clause}",
            (now, *plan_args),
        )
        execute_stage(
            "prioritize correctness repairs",
            f"UPDATE tasks SET updated_at='0001-01-01T00:00:00+00:00' "
            f"WHERE active=1 AND status='pending' "
            f"AND error LIKE 'requeued:%'{plan_clause}",
            plan_args,
        )
        if retry_failed:
            execute_stage(
                "retry failed",
                """
                UPDATE tasks SET
                    status='pending',
                    retry_not_before=NULL,
                    transient_failures=0,
                    provider_outcomes_json=COALESCE(
                        (
                            SELECT json_group_object(outcome.key, outcome.value)
                            FROM json_each(tasks.provider_outcomes_json) AS outcome
                            WHERE outcome.value!='permanent'
                              OR {keep_permanent_literal}
                        ),
                        '{{}}'
                    ),
                    updated_at=?
                WHERE active=1 AND status='failed'
                  AND ({retry_all_literal} OR EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json) AS provider
                      WHERE NOT EXISTS (
                          SELECT 1 FROM json_each(tasks.provider_outcomes_json)
                          AS outcome WHERE outcome.key=provider.value
                      )
                  )){plan_clause}
                """.format(
                    keep_permanent_literal=int(not bool(retry_permanent)),
                    retry_all_literal=int(bool(retry_permanent)),
                    plan_clause=plan_clause,
                ),
                (now, *plan_args),
            )
        if retry_repair_queue:
            execute_stage(
                "retry repair queue",
                f"UPDATE tasks SET status='pending', retry_not_before=NULL, "
                f"transient_failures=0, updated_at='0001-01-01T00:00:00+00:00' "
                f"WHERE active=1 AND status='{REPAIR_QUEUE_STATUS}'{plan_clause}",
                plan_args,
            )
        if retry_empty:
            execute_stage(
                "retry empty",
                f"UPDATE tasks SET status='pending', provider_outcomes_json='{{}}', "
                f"retry_not_before=NULL, transient_failures=0, updated_at=? "
                f"WHERE active=1 AND status='empty'{plan_clause}",
                (now, *plan_args),
            )
        if repair_legacy:
            # These three repairs migrate results produced by older worker
            # semantics. A verified maintenance-version marker makes repeating
            # their multi-million-row scans on every supervisor recycle unsafe
            # and unnecessary.
            execute_stage(
                "repair cooldown empty",
                f"""
                UPDATE tasks SET status='pending', updated_at=?
                WHERE active=1 AND status='empty'
                  AND error LIKE '%skipped (cooldown until%'{plan_clause}
                """,
                (now, *plan_args),
            )
            execute_stage(
                "repair BLS quota empty",
                f"""
                UPDATE tasks SET status='pending', attempts=0, updated_at=?
                WHERE active=1 AND endpoint='economy.survey.bls_series'
                  AND status='empty'
                  AND LOWER(error) LIKE '%daily threshold%'{plan_clause}
                """,
                (now, *plan_args),
            )
            execute_stage(
                "reset legacy quota attempts",
                f"""
                UPDATE tasks SET attempts=0
                WHERE active=1 AND status IN ('pending','failed') AND attempts>0
                  AND (
                      LOWER(error) LIKE '%skipped (cooldown until%'
                      OR LOWER(error) LIKE '%429%'
                      OR LOWER(error) LIKE '%limit reach%'
                      OR LOWER(error) LIKE '%rate limit%'
                      OR LOWER(error) LIKE '%too many requests%'
                      OR LOWER(error) LIKE '%daily limit%'
                      OR LOWER(error) LIKE '%daily threshold%'
                      OR LOWER(error) LIKE '%quota%'
                  ){plan_clause}
                """,
                plan_args,
            )
        if refresh:
            execute_stage(
                "refresh success",
                f"UPDATE tasks SET status='pending', provider_outcomes_json='{{}}', "
                f"retry_not_before=NULL, transient_failures=0, updated_at=? "
                f"WHERE active=1 AND status='success'{plan_clause}",
                (now, *plan_args),
            )
        if verify_successful_shards:
            # This is an explicit integrity audit, not crash recovery. A worker
            # atomically publishes Parquet before its SQLite success commit, so
            # verified resume only needs to adopt/recover interrupted `running`
            # rows. Re-statting millions of immutable successes on every
            # supervisor recycle consumes gigabytes and delays useful HTTP work.
            progress.set_postfix(stage="load successful shards", refresh=False)
            with _sqlite_progress(
                self.connection,
                "prepare:load successful shards",
                enabled=show_progress,
            ):
                complete_rows = self.connection.execute(
                    f"SELECT task_id, output_path FROM tasks WHERE active=1 AND status='success'{plan_clause}",
                    plan_args,
                ).fetchall()
            progress.update(1)
            missing: list[tuple[str, str]] = []
            file_progress = tqdm(
                complete_rows,
                total=len(complete_rows),
                desc="prepare:verify successful shards",
                unit="file",
                position=1,
                leave=False,
                disable=not show_progress,
            )
            try:
                for row in file_progress:
                    if not Path(row["output_path"]).is_file():
                        missing.append((now, row["task_id"]))
                    if file_progress.n % 1000 == 0:
                        file_progress.set_postfix(missing=len(missing), refresh=False)
            finally:
                file_progress.close()
            progress.update(1)
            progress.set_postfix(stage="requeue missing shards", refresh=False)
            with _sqlite_progress(
                self.connection,
                "prepare:requeue missing shards",
                enabled=show_progress,
            ):
                self.connection.executemany(
                    "UPDATE tasks SET status='pending', retry_not_before=NULL, "
                    "transient_failures=0, updated_at=? WHERE task_id=?",
                    missing,
                )
            progress.update(1)
        progress.set_postfix(stage="commit", refresh=False)
        with _sqlite_progress(
            self.connection,
            "prepare:commit",
            enabled=show_progress,
        ):
            self.connection.commit()
        progress.update(1)
        progress.close()

    def adopt_published_outputs(
        self,
        *,
        plan_token: str | None,
        show_progress: bool = False,
    ) -> int:
        """Commit Parquet files published before a manifest transaction.

        The worker publishes a shard atomically before updating SQLite.  A
        process kill in that small window leaves the task in ``running``;
        adopt that durable shard before recovering running tasks.  Pending
        tasks are never scanned because every planned task has an output path
        even before its first attempt.
        """
        plan_clause = "" if plan_token is None else " AND plan_token=?"
        parameters: tuple[Any, ...] = () if plan_token is None else (plan_token,)
        rows = self.connection.execute(
            f"""
            SELECT task_id,endpoint,scope_key,output_path
            FROM tasks
            WHERE active=1 AND status='running'
              AND output_path IS NOT NULL AND output_path!=''{plan_clause}
            ORDER BY task_id
            """,
            parameters,
        ).fetchall()
        progress = tqdm(
            rows,
            total=len(rows),
            desc="openbb:adopt published shards",
            unit="file",
            position=1,
            leave=False,
            disable=not show_progress,
        )
        adopted: list[tuple[str, str, int, str]] = []
        try:
            for row in progress:
                try:
                    path = Path(str(row["output_path"]))
                    parquet = pq.ParquetFile(path)
                    if parquet.metadata is None or parquet.metadata.num_rows <= 0:
                        continue
                    names = set(parquet.schema_arrow.names)
                    required = {
                        "_openbb_endpoint",
                        "_provider",
                        "_scope_key",
                    }
                    if not required.issubset(names):
                        continue
                    first = next(
                        parquet.iter_batches(
                            batch_size=1,
                            columns=["_openbb_endpoint", "_provider", "_scope_key"],
                        )
                    ).to_pylist()[0]
                    if str(first.get("_openbb_endpoint")) != str(
                        row["endpoint"]
                    ) or str(first.get("_scope_key")) != str(row["scope_key"]):
                        continue
                    adopted.append(
                        (
                            str(first.get("_provider") or ""),
                            str(row["task_id"]),
                            int(parquet.metadata.num_rows),
                            datetime.now(timezone.utc).isoformat(),
                        )
                    )
                except (
                    OSError,
                    StopIteration,
                    pa.ArrowException,
                    ValueError,
                    TypeError,
                ):
                    continue
                progress.set_postfix(adopted=len(adopted), refresh=False)
        finally:
            progress.close()
        if not adopted:
            return 0
        self.connection.executemany(
            """
            UPDATE tasks SET status='success', selected_provider=?, rows=?,
                error=NULL, retry_not_before=NULL, transient_failures=0,
                execution_started_at=NULL, updated_at=?
            WHERE task_id=? AND status='running'
            """,
            [
                (provider, rows_count, updated, task_id)
                for provider, task_id, rows_count, updated in adopted
            ],
        )
        self.connection.commit()
        return len(adopted)

    def deactivate_legacy_cftc_followups(
        self, plan_token: str, *, show_progress: bool = False
    ) -> int:
        """Retire pre-report-type CFTC tasks superseded by complete catalogs."""
        with _sqlite_progress(
            self.connection,
            "manifest:retire legacy CFTC",
            enabled=show_progress,
        ):
            cursor = self.connection.execute(
                """
            UPDATE tasks SET active=0
            WHERE active=1 AND plan_token=? AND task_source='followup'
              AND endpoint='cftc.cot'
              AND json_type(kwargs_json, '$.report_type') IS NULL
            """,
                (plan_token,),
            )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def pending_batch(
        self,
        limit: int,
        max_total_attempts: int,
        plan_token: str | None = None,
        *,
        excluded_providers: set[str] | None = None,
        excluded_endpoints: set[str] | None = None,
    ) -> list[DownloadTask]:
        if plan_token is not None:
            return self._fair_pending_batch(
                limit,
                max_total_attempts,
                plan_token,
                excluded_providers=excluded_providers,
                excluded_endpoints=excluded_endpoints,
            )
        excluded = tuple(sorted(excluded_providers or ()))
        excluded_clause = ""
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            excluded_clause = f"AND provider.value NOT IN ({placeholders})"
        provider_clause = f"""
          AND EXISTS (
              SELECT 1 FROM json_each(tasks.providers_json) AS provider
              WHERE NOT EXISTS (
                  SELECT 1 FROM json_each(tasks.provider_outcomes_json) AS outcome
                  WHERE outcome.key=provider.value
              )
              {excluded_clause}
          )
        """
        # ``max_total_attempts`` is retained in the public call shape for old
        # wrappers, but scheduling is evidence-driven. A transient network,
        # quota, or parser-negotiation failure never becomes terminal merely
        # because a counter crossed a threshold.
        _ = max_total_attempts
        now = datetime.now(timezone.utc).isoformat()
        parameters: tuple[Any, ...] = (now, *excluded, limit)
        rows = self.connection.execute(
            f"""
            SELECT * FROM tasks
            WHERE active=1 AND status='pending'
              AND (retry_not_before IS NULL OR retry_not_before<=?)
              {provider_clause}
            ORDER BY category, endpoint, updated_at, task_id
            LIMIT ?
            """,
            parameters,
        ).fetchall()
        return [self._task_from_row(row) for row in rows]

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> DownloadTask:
        return DownloadTask(
            task_id=row["task_id"],
            endpoint=row["endpoint"],
            category=row["category"],
            scope_key=row["scope_key"],
            kwargs=json.loads(row["kwargs_json"]),
            providers=tuple(json.loads(row["providers_json"])),
            output_path=row["output_path"],
            provider_outcomes=dict(json.loads(row["provider_outcomes_json"] or "{}")),
            provider_evidence=dict(json.loads(row["provider_evidence_json"] or "{}")),
            attempts=int(row["attempts"] or 0),
            transient_failures=int(row["transient_failures"] or 0),
        )

    def _refresh_schedule(self, max_total_attempts: int, plan_token: str) -> None:
        _ = max_total_attempts
        now = datetime.now(timezone.utc).isoformat()
        self._schedule_keys = [
            (str(row["category"]), str(row["endpoint"]))
            for row in self.connection.execute(
                """
                SELECT category,endpoint
                FROM tasks
                WHERE active=1 AND status='pending' AND plan_token=?
                  AND (retry_not_before IS NULL OR retry_not_before<=?)
                GROUP BY category,endpoint
                ORDER BY category,endpoint
                """,
                (plan_token, now),
            )
        ]
        self._schedule_token = plan_token
        self._schedule_cursor = 0

    def _pending_endpoint_rows(
        self,
        category: str,
        endpoint: str,
        row_limit: int,
        max_total_attempts: int,
        plan_token: str,
        *,
        excluded_providers: set[str] | None = None,
        required_provider: str | None = None,
        offset: int = 0,
    ) -> list[sqlite3.Row]:
        excluded = tuple(sorted(excluded_providers or ()))
        excluded_clause = ""
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            excluded_clause = f"AND provider.value NOT IN ({placeholders})"
        provider_clause = f"""
          AND EXISTS (
              SELECT 1 FROM json_each(tasks.providers_json) AS provider
              WHERE NOT EXISTS (
                  SELECT 1 FROM json_each(tasks.provider_outcomes_json) AS outcome
                  WHERE outcome.key=provider.value
              )
              {excluded_clause}
          )
        """
        required_provider_clause = ""
        required_provider_parameters: tuple[str, ...] = ()
        required_excluded_parameters: tuple[str, ...] = ()
        if required_provider:
            earlier_excluded_clause = ""
            if excluded:
                placeholders = ",".join("?" for _ in excluded)
                earlier_excluded_clause = f"AND earlier.value NOT IN ({placeholders})"
                required_excluded_parameters = excluded
            required_provider_clause = f"""
              AND EXISTS (
                  SELECT 1 FROM json_each(tasks.providers_json) AS required
                  WHERE required.value=?
                    AND NOT EXISTS (
                        SELECT 1
                        FROM json_each(tasks.provider_outcomes_json) AS outcome
                        WHERE outcome.key=required.value
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM json_each(tasks.providers_json) AS earlier
                        WHERE CAST(earlier.key AS INTEGER)
                              < CAST(required.key AS INTEGER)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM json_each(
                                  tasks.provider_outcomes_json
                              ) AS earlier_outcome
                              WHERE earlier_outcome.key=earlier.value
                          )
                          {earlier_excluded_clause}
                    )
              )
            """
            required_provider_parameters = (str(required_provider),)
        order_fields = (
            "updated_at, scope_key, task_id"
            if endpoint in SEC_COMPANYFACTS_STATEMENT_ENDPOINTS
            else "updated_at, task_id"
        )
        with _sqlite_progress(
            self.connection,
            f"manifest:pending {category}/{endpoint}",
            enabled=self.show_progress,
        ):
            now = datetime.now(timezone.utc).isoformat()
            rows = self.connection.execute(
                f"""
            SELECT * FROM tasks
            WHERE active=1 AND status='pending' AND plan_token=?
              AND category=? AND endpoint=?
              AND (retry_not_before IS NULL OR retry_not_before<=?)
              {required_provider_clause}
              {provider_clause}
            ORDER BY {order_fields}
            LIMIT ? OFFSET ?
                """,
                (
                    plan_token,
                    category,
                    endpoint,
                    now,
                    *required_provider_parameters,
                    *required_excluded_parameters,
                    *excluded,
                    max(0, int(row_limit)),
                    max(0, int(offset)),
                ),
            ).fetchall()
        return rows

    def pending_endpoint_batch(
        self,
        category: str,
        endpoint: str,
        limit: int,
        max_total_attempts: int,
        plan_token: str,
        *,
        excluded_providers: set[str] | None = None,
        required_provider: str | None = None,
        offset: int = 0,
    ) -> list[DownloadTask]:
        """Read deeper into one indexed endpoint without diluting all markets.

        ``required_provider`` selects tasks where that provider is the first
        unresolved, non-excluded provider in the fallback chain.  This is
        essential when one endpoint contains provider-specific task families
        whose timestamps place thousands of another provider's tasks first.
        """
        return [
            self._task_from_row(row)
            for row in self._pending_endpoint_rows(
                category,
                endpoint,
                limit,
                max_total_attempts,
                plan_token,
                excluded_providers=excluded_providers,
                required_provider=required_provider,
                offset=offset,
            )
        ]

    def _fair_pending_batch(
        self,
        limit: int,
        max_total_attempts: int,
        plan_token: str,
        *,
        excluded_providers: set[str] | None = None,
        excluded_endpoints: set[str] | None = None,
    ) -> list[DownloadTask]:
        limit = max(0, int(limit))
        if limit == 0:
            return []
        self._schedule_calls += 1
        if (
            not self._schedule_keys
            or self._schedule_token != plan_token
            or self._schedule_calls % 100 == 0
        ):
            self._refresh_schedule(max_total_attempts, plan_token)
        if not self._schedule_keys:
            return []

        start = self._schedule_cursor % len(self._schedule_keys)
        keys = self._schedule_keys[start:] + self._schedule_keys[:start]
        if excluded_endpoints:
            keys = [key for key in keys if key[1] not in excluded_endpoints]
        if not keys:
            return []
        endpoint_buckets: dict[tuple[str, str], list[sqlite3.Row]] = {}
        candidate_count = 0
        visited = 0

        # Seed every endpoint before deepening any endpoint.  A single-pass
        # ceil(limit / endpoint_count) allocation can fill the candidate pool
        # before reaching the final schedule keys.  That is more than an
        # endpoint-order fairness issue: if those late endpoints are the only
        # routes for a provider, the provider receives no reservation and its
        # independent rate limiter remains completely idle.
        for category, endpoint in keys:
            if candidate_count >= limit:
                break
            endpoint_rows = self._pending_endpoint_rows(
                category,
                endpoint,
                1,
                max_total_attempts,
                plan_token,
                excluded_providers=excluded_providers,
            )
            if endpoint_rows:
                endpoint_buckets[(category, endpoint)] = endpoint_rows
                candidate_count += len(endpoint_rows)
            visited += 1

        # Once every endpoint has exposed one schedulable task, distribute the
        # remaining read budget evenly.  The executor can then deepen only the
        # providers whose latency-sized reservations are still underfilled.
        eligible_keys = [key for key in keys if endpoint_buckets.get(key)]
        if eligible_keys and candidate_count < limit:
            per_endpoint = max(
                1, math.ceil((limit - candidate_count) / len(eligible_keys))
            )
            for category, endpoint in eligible_keys:
                remaining = limit - candidate_count
                if remaining <= 0:
                    break
                key = (category, endpoint)
                bucket = endpoint_buckets[key]
                extra = self._pending_endpoint_rows(
                    category,
                    endpoint,
                    min(per_endpoint, remaining),
                    max_total_attempts,
                    plan_token,
                    excluded_providers=excluded_providers,
                    offset=len(bucket),
                )
                if extra:
                    bucket.extend(extra)
                    candidate_count += len(extra)

        # Finite endpoints and provider cooldown filters can leave unused read
        # budget after the equal-share pass.  Fill that budget only after every
        # eligible endpoint has been seeded, so throughput cannot reintroduce
        # provider starvation.
        if candidate_count < limit:
            for category, endpoint in keys:
                remaining = limit - candidate_count
                if remaining <= 0:
                    break
                key = (category, endpoint)
                bucket = endpoint_buckets.setdefault(key, [])
                extra = self._pending_endpoint_rows(
                    category,
                    endpoint,
                    remaining,
                    max_total_attempts,
                    plan_token,
                    excluded_providers=excluded_providers,
                    offset=len(bucket),
                )
                if extra:
                    bucket.extend(extra)
                    candidate_count += len(extra)
        if visited:
            advance = visited
            if advance % len(keys) == 0:
                # A batch that happens to visit every endpoint must not reset
                # to the same first endpoint.  Provider reservations can be
                # much smaller than this candidate batch (for example four
                # FMP calls), so a one-position rotation is what guarantees
                # scarce daily quota probes every route before repeatedly
                # consuming the earliest alphabetical routes.
                advance += 1
            self._schedule_cursor = (start + advance) % len(self._schedule_keys)
        materialized_buckets = [
            endpoint_buckets[key] for key in keys if endpoint_buckets.get(key)
        ]
        max_bucket_size = max(
            (len(bucket) for bucket in materialized_buckets), default=0
        )
        rows = [
            bucket[offset]
            for offset in range(max_bucket_size)
            for bucket in materialized_buckets
            if offset < len(bucket)
        ][:limit]
        return [self._task_from_row(row) for row in rows]

    def pending_count(
        self, max_total_attempts: int, plan_token: str | None = None
    ) -> int:
        _ = max_total_attempts
        plan_clause = "" if plan_token is None else " AND plan_token=?"
        parameters: tuple[Any, ...] = () if plan_token is None else (plan_token,)
        row = self.connection.execute(
            f"SELECT COUNT(*) FROM tasks WHERE active=1 AND status='pending'{plan_clause}",
            parameters,
        ).fetchone()
        return int(row[0])

    def retry_deferred_state(
        self, plan_token: str | None = None
    ) -> tuple[int, str | None]:
        """Return pending tasks held by durable task-local backoff and next due."""
        now = datetime.now(timezone.utc).isoformat()
        plan_clause = "" if plan_token is None else " AND plan_token=?"
        parameters: tuple[Any, ...] = (
            (now,) if plan_token is None else (now, plan_token)
        )
        row = self.connection.execute(
            f"""
            SELECT COUNT(*), MIN(retry_not_before)
            FROM tasks
            WHERE active=1 AND status='pending'
              AND retry_not_before>?{plan_clause}
            """,
            parameters,
        ).fetchone()
        return int(row[0]), (None if row[1] is None else str(row[1]))

    def next_retry_delay(self, plan_token: str | None = None) -> float | None:
        _, deadline = self.retry_deferred_state(plan_token)
        if deadline is None:
            return None
        try:
            due = datetime.fromisoformat(deadline)
            if due.tzinfo is None:
                due = due.replace(tzinfo=timezone.utc)
        except ValueError:
            return 0.0
        return max(0.0, (due - datetime.now(timezone.utc)).total_seconds())

    def accepted_endpoints(self, plan_token: str) -> set[str]:
        """Return routes with evidence of data or authoritative absence."""
        return {
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT DISTINCT endpoint FROM tasks
                WHERE active=1 AND plan_token=? AND status IN ('success','empty')
                """,
                (plan_token,),
            )
        }

    def dns_error_providers(self, plan_token: str) -> set[str]:
        """Return providers with durable resolver-failure evidence in this plan."""
        rows = self.connection.execute(
            """
            SELECT error FROM tasks
            WHERE active=1 AND plan_token=?
              AND (
                  LOWER(COALESCE(error,'')) LIKE '%could not resolve host%'
                  OR LOWER(COALESCE(error,'')) LIKE '%dnserror%'
                  OR LOWER(COALESCE(error,'')) LIKE '%failed to resolve%'
                  OR LOWER(COALESCE(error,'')) LIKE '%name resolution%'
                  OR LOWER(COALESCE(error,'')) LIKE '%nameresolutionerror%'
              )
            """,
            (plan_token,),
        ).fetchall()
        providers: set[str] = set()
        for row in rows:
            for segment in str(row["error"] or "").split(" | "):
                if not _has_dns_error_evidence(segment) or ":" not in segment:
                    continue
                provider = segment.split(":", 1)[0].strip().lower()
                if re.fullmatch(r"[a-z][a-z0-9_]*", provider):
                    providers.add(provider)
        return providers

    def claim(self, tasks: Sequence[DownloadTask]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.connection.executemany(
            "UPDATE tasks SET status='running', execution_started_at=NULL, "
            "updated_at=? WHERE task_id=? AND status='pending'",
            [(now, task.task_id) for task in tasks],
        )
        self.connection.commit()

    def mark_executing(self, tasks: Sequence[DownloadTask]) -> None:
        """Record when claimed work actually enters the executor.

        ``claim`` is a durable prefetch reservation and may precede execution
        by minutes for a saturated provider.  Keeping this second timestamp
        lets monitoring distinguish a healthy provider queue from an actually
        stuck network/parser call without adding another task status or losing
        restart recovery semantics.
        """
        if not tasks:
            return
        now = datetime.now(timezone.utc).isoformat()
        self.connection.executemany(
            "UPDATE tasks SET execution_started_at=?, updated_at=? "
            "WHERE task_id=? AND status='running' "
            "AND execution_started_at IS NULL",
            [(now, now, task.task_id) for task in tasks],
        )
        self.connection.commit()

    @contextmanager
    def completion_batch(self) -> Iterator[None]:
        """Commit a group of task outcomes with one durable transaction.

        Worker futures can finish much faster than one ``synchronous=FULL``
        SQLite commit per task.  The old one-result transaction boundary made
        completed ``Future`` objects retain their normalized records, consume
        the global queue, and eventually idle every provider at once.  Keep
        the public ``complete`` method (and its test/instrumentation hooks),
        but defer its commit and terminal-shard quarantine until the outermost
        completion batch succeeds.

        Published success Parquet files remain crash-safe: if this transaction
        is interrupted, startup adoption reconciles those immutable shards.
        Terminal files are quarantined only after the manifest commit so the
        filesystem never gets ahead of the durable task state.
        """
        outermost = self._completion_batch_depth == 0
        if outermost:
            self._completion_batch_quarantines = []
        self._completion_batch_depth += 1
        try:
            yield
        except BaseException:
            if outermost:
                self.connection.rollback()
                self._completion_batch_quarantines = []
            raise
        else:
            if outermost:
                self.connection.commit()
                quarantines = self._completion_batch_quarantines
                self._completion_batch_quarantines = []
                for task in quarantines:
                    _quarantine_obsolete_task_output(task)
        finally:
            self._completion_batch_depth -= 1

    def complete(self, result: TaskResult) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.connection.execute(
            """
            UPDATE tasks SET status=?, selected_provider=?, attempts=attempts+?, rows=?,
                kwargs_json=?,
                output_path=COALESCE(?, output_path), error=?,
                provider_outcomes_json=?, provider_evidence_json=?,
                retry_not_before=?, transient_failures=?,
                execution_started_at=NULL, updated_at=?
            WHERE task_id=?
            """,
            (
                result.status,
                result.provider,
                result.attempts,
                result.rows,
                _canonical_json(result.task.kwargs),
                result.output_path,
                result.error,
                _canonical_json(result.provider_outcomes),
                _canonical_json(result.provider_evidence),
                result.retry_not_before,
                max(0, int(result.transient_failures)),
                now,
                result.task.task_id,
            ),
        )
        terminal = result.status in {
            "empty",
            "unavailable",
            "failed",
            REPAIR_QUEUE_STATUS,
        }
        if self._completion_batch_depth:
            if terminal:
                self._completion_batch_quarantines.append(result.task)
            return
        self.connection.commit()
        if terminal:
            _quarantine_obsolete_task_output(result.task)

    def repair_unproven_provider_outcomes(
        self,
        unavailable_providers: Mapping[str, str],
        unavailable_routes: Mapping[tuple[str, str], str],
        unavailable_domains: Mapping[tuple[str, str, str], str],
        *,
        plan_token: str,
    ) -> int:
        """Requeue capability claims whose positive evidence was not durable.

        This is intentionally provider- and market-agnostic.  A task may keep
        an unavailable outcome only when either its own provider evidence or a
        persisted provider/route/market capability checkpoint proves it.  Old
        categorical-only outcomes are removed so they are observed again under
        the evidence-preserving schema.
        """
        global_scopes = frozenset(map(str, unavailable_providers))
        route_scopes = frozenset(
            (str(provider), str(endpoint)) for provider, endpoint in unavailable_routes
        )
        domain_scopes = frozenset(
            (str(provider), str(endpoint), str(domain))
            for provider, endpoint, domain in unavailable_domains
        )
        recovery_key = "unproven_provider_outcome_recovery_revision:" + str(plan_token)
        if self.meta_value(recovery_key) == str(
            UNPROVEN_PROVIDER_OUTCOME_RECOVERY_REVISION
        ):
            return 0

        def capability_proven(
            provider: object, endpoint: object, kwargs_json: object
        ) -> int:
            provider_name = str(provider or "")
            endpoint_name = str(endpoint or "")
            if provider_name in global_scopes:
                return 1
            if (provider_name, endpoint_name) in route_scopes:
                return 1
            try:
                kwargs = json.loads(str(kwargs_json or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                return 0
            if not isinstance(kwargs, Mapping):
                return 0
            domain = _provider_capability_domain(provider_name, kwargs)
            return int(
                domain is not None
                and (provider_name, endpoint_name, domain) in domain_scopes
            )

        self.connection.create_function(
            "openbb_provider_capability_proven",
            3,
            capability_proven,
            deterministic=True,
        )
        self.connection.create_function(
            "openbb_authoritative_unavailable_evidence",
            1,
            lambda value: int(_is_authoritative_unavailable_evidence(value)),
            deterministic=True,
        )
        unproven = """
            outcome.value='unavailable'
            AND openbb_provider_capability_proven(
                outcome.key,tasks.endpoint,tasks.kwargs_json
            )=0
            AND openbb_authoritative_unavailable_evidence(
                COALESCE(
                    (
                        SELECT evidence.value
                        FROM json_each(tasks.provider_evidence_json) AS evidence
                        WHERE evidence.key=outcome.key
                    ),
                    ''
                )
            )=0
        """
        with _sqlite_progress(
            self.connection,
            "manifest:repair unavailable evidence",
            enabled=self.show_progress,
        ):
            cursor = self.connection.execute(
                f"""
            UPDATE tasks SET
                provider_outcomes_json=COALESCE(
                    (
                        SELECT json_group_object(outcome.key,outcome.value)
                        FROM json_each(tasks.provider_outcomes_json) AS outcome
                        WHERE NOT ({unproven})
                    ),
                    '{{}}'
                ),
                status='pending', selected_provider=NULL,
                attempts=MAX(0,attempts-1), rows=0,
                error='requeued: unavailable provider outcome lacked durable positive evidence',
                execution_started_at=NULL,
                updated_at='1970-01-01T00:00:00+00:00'
            WHERE active=1 AND plan_token=? AND status!='success'
              AND EXISTS (
                  SELECT 1
                  FROM json_each(tasks.provider_outcomes_json) AS outcome
                  WHERE {unproven}
              )
                """,
                (plan_token,),
            )
        self.set_meta_value(
            recovery_key, str(UNPROVEN_PROVIDER_OUTCOME_RECOVERY_REVISION)
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def repair_unproven_permanent_outcomes(self, *, plan_token: str) -> int:
        """Requeue permanent provider claims that lost their error evidence.

        A fallback can still publish a successful shard after an earlier
        provider failed permanently. The permanent classification is durable
        only when its redacted exception was stored beside the outcome; older
        workers omitted that evidence. Preserve the fallback file, remove only
        unproven provider markers, and let the primary provider run once under
        the current adapter.
        """
        recovery_key = "unproven_permanent_outcome_recovery_revision:" + str(plan_token)
        if self.meta_value(recovery_key) == str(
            UNPROVEN_PERMANENT_OUTCOME_RECOVERY_REVISION
        ):
            return 0
        unproven = """
            outcome.value='permanent'
            AND NOT EXISTS (
                SELECT 1
                FROM json_each(tasks.provider_evidence_json) AS evidence
                WHERE evidence.key=outcome.key
                  AND LENGTH(TRIM(COALESCE(evidence.value,'')))>0
            )
        """
        with _sqlite_progress(
            self.connection,
            "manifest:repair permanent evidence",
            enabled=self.show_progress,
        ):
            cursor = self.connection.execute(
                f"""
                UPDATE tasks SET
                    provider_outcomes_json=COALESCE(
                        (
                            SELECT json_group_object(outcome.key,outcome.value)
                            FROM json_each(tasks.provider_outcomes_json) AS outcome
                            WHERE NOT ({unproven})
                        ),
                        '{{}}'
                    ),
                    status='pending', selected_provider=NULL,
                    attempts=MAX(0,attempts-1), rows=0,
                    error='requeued: permanent provider outcome lacked durable error evidence',
                    execution_started_at=NULL,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND EXISTS (
                      SELECT 1
                      FROM json_each(tasks.provider_outcomes_json) AS outcome
                      WHERE {unproven}
                  )
                """,
                (plan_token,),
            )
        self.set_meta_value(
            recovery_key, str(UNPROVEN_PERMANENT_OUTCOME_RECOVERY_REVISION)
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def repair_provider_parser_shape_permanents(self, *, plan_token: str) -> int:
        """Requeue provider parser/schema mismatches across every market.

        A provider adapter calling ``.get``/``.items``/``.values`` on an
        unexpected null/list response describes a parser contract mismatch,
        not authoritative data absence.  Older workers classified a few such
        AttributeErrors as permanent.  Remove only the affected provider
        outcome and its evidence once per plan/recovery revision so every
        endpoint benefits from the same boundary rule.
        """
        revision_key = "provider_parser_shape_recovery_revision:" + str(plan_token)
        if self.meta_value(revision_key) == str(
            PROVIDER_PARSER_SHAPE_RECOVERY_REVISION
        ):
            return 0
        self.connection.create_function(
            "openbb_provider_parser_shape_error",
            1,
            lambda value: int(_is_provider_parser_shape_error_text(value)),
            deterministic=True,
        )
        recoverable = """
            outcome.value='permanent'
            AND EXISTS (
                SELECT 1
                FROM json_each(tasks.provider_evidence_json) AS evidence
                WHERE evidence.key=outcome.key
                  AND openbb_provider_parser_shape_error(evidence.value)=1
            )
        """
        with _sqlite_progress(
            self.connection,
            "manifest:repair provider parser shapes",
            enabled=self.show_progress,
        ):
            cursor = self.connection.execute(
                f"""
                UPDATE tasks SET
                    provider_outcomes_json=COALESCE(
                        (
                            SELECT json_group_object(outcome.key,outcome.value)
                            FROM json_each(tasks.provider_outcomes_json) AS outcome
                            WHERE NOT ({recoverable})
                        ),
                        '{{}}'
                    ),
                    provider_evidence_json=COALESCE(
                        (
                            SELECT json_group_object(evidence.key,evidence.value)
                            FROM json_each(tasks.provider_evidence_json) AS evidence
                            WHERE NOT (
                                openbb_provider_parser_shape_error(
                                    evidence.value
                                )=1
                                AND EXISTS (
                                    SELECT 1
                                    FROM json_each(
                                        tasks.provider_outcomes_json
                                    ) AS failed_outcome
                                    WHERE failed_outcome.key=evidence.key
                                      AND failed_outcome.value='permanent'
                                )
                            )
                        ),
                        '{{}}'
                    ),
                    status='pending', selected_provider=NULL,
                    attempts=MAX(0,attempts-1), rows=0,
                    error='requeued: provider parser shape mismatch is recoverable',
                    execution_started_at=NULL,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND provider_evidence_json LIKE '%has no attribute%'
                  AND EXISTS (
                      SELECT 1
                      FROM json_each(tasks.provider_outcomes_json) AS outcome
                      WHERE {recoverable}
                  )
                """,
                (plan_token,),
            )
        repaired = max(0, int(cursor.rowcount))
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value, updated_at=excluded.updated_at
            """,
            (
                revision_key,
                str(PROVIDER_PARSER_SHAPE_RECOVERY_REVISION),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()
        return repaired

    def repair_provider_transient_permanent_outcomes(self, *, plan_token: str) -> int:
        """Requeue historical permanent labels that current rules prove transient."""
        revision_key = "provider_transient_outcome_recovery_revision:" + str(plan_token)
        if self.meta_value(revision_key) == str(
            PROVIDER_TRANSIENT_OUTCOME_RECOVERY_REVISION
        ):
            return 0
        self.connection.create_function(
            "openbb_provider_retryable_error",
            1,
            lambda value: int(
                classify_error(RuntimeError(str(value or "")))
                in {"deferred", "rate", "transient"}
            ),
            deterministic=True,
        )
        recoverable = """
            outcome.value='permanent'
            AND EXISTS (
                SELECT 1
                FROM json_each(tasks.provider_evidence_json) AS evidence
                WHERE evidence.key=outcome.key
                  AND openbb_provider_retryable_error(evidence.value)=1
            )
        """
        with _sqlite_progress(
            self.connection,
            "manifest:repair transient permanent outcomes",
            enabled=self.show_progress,
        ):
            cursor = self.connection.execute(
                f"""
                UPDATE tasks SET
                    provider_outcomes_json=COALESCE(
                        (
                            SELECT json_group_object(outcome.key,outcome.value)
                            FROM json_each(tasks.provider_outcomes_json) AS outcome
                            WHERE NOT ({recoverable})
                        ),
                        '{{}}'
                    ),
                    provider_evidence_json=COALESCE(
                        (
                            SELECT json_group_object(evidence.key,evidence.value)
                            FROM json_each(tasks.provider_evidence_json) AS evidence
                            WHERE openbb_provider_retryable_error(evidence.value)=0
                               OR NOT EXISTS (
                                   SELECT 1
                                   FROM json_each(
                                       tasks.provider_outcomes_json
                                   ) AS failed_outcome
                                   WHERE failed_outcome.key=evidence.key
                                     AND failed_outcome.value='permanent'
                               )
                        ),
                        '{{}}'
                    ),
                    status='pending', selected_provider=NULL,
                    attempts=MAX(0,attempts-1), rows=0,
                    error='requeued: provider outcome is retryable transport evidence',
                    execution_started_at=NULL, retry_not_before=NULL,
                    transient_failures=0,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status!='success'
                  AND provider_outcomes_json LIKE '%:"permanent"%'
                  AND EXISTS (
                      SELECT 1
                      FROM json_each(tasks.provider_outcomes_json) AS outcome
                      WHERE {recoverable}
                  )
                """,
                (plan_token,),
            )
        repaired = max(0, int(cursor.rowcount))
        self.set_meta_value(
            revision_key,
            str(PROVIDER_TRANSIENT_OUTCOME_RECOVERY_REVISION),
        )
        self.connection.commit()
        return repaired

    def repair_fmp_adapter_boundary_failures(
        self, *, plan_token: str
    ) -> tuple[int, int]:
        """Requeue rows fixed by narrow FMP response/query normalization."""
        revision_key = "fmp_adapter_boundary_recovery_revision:" + str(plan_token)
        if self.meta_value(revision_key) == str(FMP_ADAPTER_BOUNDARY_RECOVERY_REVISION):
            return 0, 0
        with self.connection:
            eps = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: bypass OpenBB historical EPS hidden limit offset',
                    provider_outcomes_json='{}', provider_evidence_json='{}',
                    execution_started_at=NULL, retry_not_before=NULL,
                    transient_failures=0,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND endpoint='equity.fundamental.historical_eps'
                  AND status='unavailable'
                  AND LOWER(COALESCE(error,'')) LIKE '%limit%must be between%'
                  AND CAST(
                      COALESCE(json_extract(kwargs_json,'$.limit'),0) AS INTEGER
                  )<=5
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value='fmp'
                  )
                """,
                (plan_token,),
            ).rowcount
            peers = self.connection.execute(
                """
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, attempts=0, rows=0,
                    error='requeued: normalize fractional FMP peer market cap',
                    provider_outcomes_json='{}', provider_evidence_json='{}',
                    execution_started_at=NULL, retry_not_before=NULL,
                    transient_failures=0,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=?
                  AND endpoint='equity.compare.peers'
                  AND status='failed'
                  AND LOWER(COALESCE(error,'')) LIKE '%market_cap%'
                  AND LOWER(COALESCE(error,'')) LIKE '%fractional part%'
                  AND EXISTS (
                      SELECT 1 FROM json_each(tasks.providers_json)
                      WHERE value='fmp'
                  )
                """,
                (plan_token,),
            ).rowcount
            self.connection.execute(
                """
                INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
                """,
                (
                    revision_key,
                    str(FMP_ADAPTER_BOUNDARY_RECOVERY_REVISION),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return max(0, int(eps)), max(0, int(peers))

    def repair_heterogeneous_parquet_schema_shards(self, *, plan_token: str) -> int:
        """Rebuild old shards whose later-row fields Arrow could omit.

        Older shared writers let PyArrow infer field names from row zero. The
        known heterogeneous producers are BLS search (series plus code maps),
        SEC statement validation recoveries, and Form 4 tasks completed after
        nullable-container recovery was introduced but before the union-schema
        writer loaded. Requeue those provider results once; cached SEC inputs
        remain reusable and unrelated fallback outcomes stay intact.
        """
        revision_key = "heterogeneous_parquet_schema_recovery_revision:" + str(
            plan_token
        )
        if self.meta_value(revision_key) == str(
            HETEROGENEOUS_PARQUET_SCHEMA_RECOVERY_REVISION
        ):
            return 0
        parser_revision = self.connection.execute(
            """
            SELECT updated_at FROM archive_meta
            WHERE key=?
            """,
            ("provider_parser_shape_recovery_revision:" + str(plan_token),),
        ).fetchone()
        parser_loaded_at = (
            str(parser_revision["updated_at"])
            if parser_revision is not None
            else "9999-12-31T23:59:59+00:00"
        )
        statement_endpoints = tuple(sorted(SEC_COMPANYFACTS_STATEMENT_ENDPOINTS))
        placeholders = ",".join("?" for _ in statement_endpoints)
        with _sqlite_progress(
            self.connection,
            "manifest:repair heterogeneous Parquet schemas",
            enabled=self.show_progress,
        ):
            cursor = self.connection.execute(
                f"""
                UPDATE tasks SET
                    status='pending', selected_provider=NULL, rows=0,
                    error='requeued: later-row Parquet fields require union schema',
                    execution_started_at=NULL,
                    updated_at='1970-01-01T00:00:00+00:00'
                WHERE active=1 AND plan_token=? AND status='success'
                  AND (
                    (
                      endpoint='economy.survey.bls_search'
                      AND selected_provider='bls'
                    )
                    OR (
                      endpoint IN ({placeholders})
                      AND selected_provider='sec'
                    )
                    OR (
                      endpoint='equity.ownership.insider_trading'
                      AND selected_provider='sec'
                      AND updated_at>=?
                      AND (
                        json_extract(
                          kwargs_json,'$._archive_sec_insider_tail'
                        )=1
                        OR json_extract(
                          kwargs_json,'$._archive_sec_insider_range'
                        )=1
                      )
                    )
                  )
                """,
                (
                    plan_token,
                    *statement_endpoints,
                    parser_loaded_at,
                ),
            )
        repaired = max(0, int(cursor.rowcount))
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value, updated_at=excluded.updated_at
            """,
            (
                revision_key,
                str(HETEROGENEOUS_PARQUET_SCHEMA_RECOVERY_REVISION),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()
        return repaired

    def repair_sec_statement_validation_permanents(self, *, plan_token: str) -> int:
        """Retry statement cells now handled by resilient model validation.

        Older runs discarded an entire SEC statement when one mapped XBRL
        cell did not satisfy the OpenBB Pydantic field type.  The current
        adapter drops only that exact invalid cell and preserves an auditable
        recovery marker in the row. Requeue each affected provider outcome
        once per recovery revision while preserving any fallback Parquet file.
        """
        revision_key = "sec_statement_validation_recovery_revision:" + str(plan_token)
        if self.meta_value(revision_key) == str(
            SEC_STATEMENT_VALIDATION_RECOVERY_REVISION
        ):
            return 0
        endpoints = tuple(sorted(SEC_COMPANYFACTS_STATEMENT_ENDPOINTS))
        placeholders = ",".join("?" for _ in endpoints)
        now = "1970-01-01T00:00:00+00:00"
        cursor = self.connection.execute(
            f"""
            UPDATE tasks SET
                provider_outcomes_json=json_remove(
                    provider_outcomes_json,'$.sec'
                ),
                provider_evidence_json=json_remove(
                    provider_evidence_json,'$.sec'
                ),
                status='pending', selected_provider=NULL,
                attempts=MAX(0,attempts-1), rows=0,
                error='requeued: SEC statement cell validation is recoverable',
                execution_started_at=NULL, updated_at=?
            WHERE active=1 AND plan_token=?
              AND endpoint IN ({placeholders})
              AND json_extract(provider_outcomes_json,'$.sec')='permanent'
              AND LOWER(COALESCE(
                    json_extract(provider_evidence_json,'$.sec'),''
                  )) LIKE '%validationerror:%'
            """,
            (now, plan_token, *endpoints),
        )
        repaired = max(0, int(cursor.rowcount))
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value, updated_at=excluded.updated_at
            """,
            (
                revision_key,
                str(SEC_STATEMENT_VALIDATION_RECOVERY_REVISION),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()
        return repaired

    def repair_sec_statement_wrapper_shards(self, *, plan_token: str) -> int:
        """Requeue SEC statement shards that stored AnnotatedResult wrappers.

        Direct provider fetchers return ``AnnotatedResult.result`` whereas an
        OpenBB command returns ``OBBject.results``. Older statement workaround
        code passed the former wrapper to the generic normalizer, producing a
        one-row ``result``/``metadata`` JSON Parquet instead of one row per
        fiscal period. Inspect every sibling statement endpoint uniformly and
        requeue only files whose physical schema proves that wrapper bug.
        """
        revision_key = "sec_statement_wrapper_shard_recovery_revision:" + str(
            plan_token
        )
        if self.meta_value(revision_key) == str(
            SEC_STATEMENT_WRAPPER_SHARD_RECOVERY_REVISION
        ):
            return 0
        endpoints = tuple(sorted(SEC_COMPANYFACTS_STATEMENT_ENDPOINTS))
        placeholders = ",".join("?" for _ in endpoints)
        rows = self.connection.execute(
            f"""
            SELECT task_id,output_path FROM tasks
            WHERE active=1 AND plan_token=? AND status='success'
              AND selected_provider='sec'
              AND endpoint IN ({placeholders})
            ORDER BY task_id
            """,
            (plan_token, *endpoints),
        ).fetchall()
        malformed: list[str] = []
        progress = tqdm(
            rows,
            total=len(rows),
            desc="openbb:audit SEC statement shards",
            unit="file",
            position=1,
            leave=False,
            disable=not self.show_progress,
        )
        try:
            for row in progress:
                try:
                    names = set(
                        pq.ParquetFile(Path(str(row["output_path"]))).schema_arrow.names
                    )
                except (OSError, pa.ArrowException, ValueError, TypeError):
                    continue
                if {"result", "metadata"}.issubset(
                    names
                ) and "period_ending" not in names:
                    malformed.append(str(row["task_id"]))
                if progress.n % 250 == 0:
                    progress.set_postfix(malformed=len(malformed), refresh=False)
        finally:
            progress.close()
        if malformed:
            now = "1970-01-01T00:00:00+00:00"
            self.connection.executemany(
                """
                UPDATE tasks SET status='pending', selected_provider=NULL,
                    attempts=MAX(0,attempts-1), rows=0,
                    error='requeued: SEC statement AnnotatedResult wrapper shard',
                    provider_outcomes_json=json_remove(
                        provider_outcomes_json,'$.sec'
                    ),
                    provider_evidence_json=json_remove(
                        provider_evidence_json,'$.sec'
                    ),
                    execution_started_at=NULL, updated_at=?
                WHERE task_id=?
                """,
                [(now, task_id) for task_id in malformed],
            )
        self.connection.execute(
            """
            INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value, updated_at=excluded.updated_at
            """,
            (
                revision_key,
                str(SEC_STATEMENT_WRAPPER_SHARD_RECOVERY_REVISION),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.connection.commit()
        return len(malformed)

    def quarantine_terminal_output_shards(
        self, *, plan_token: str, batch_size: int = 1000
    ) -> tuple[int, int]:
        """Move terminal/orphan Parquet shards out of the active data tree.

        Pending and running tasks deliberately retain their last successful
        shard until revalidation settles.  Every terminal non-success task and
        every file with no active manifest owner is quarantined, regardless of
        provider, endpoint, or market.
        """
        data_dir = self.path.parent.parent / "data"
        if not data_dir.is_dir():
            return 0, 0
        paths: list[Path] = []
        scan_progress = tqdm(
            desc="openbb:reconcile parquet ownership",
            unit="shard",
            disable=not self.show_progress,
        )
        try:
            for path in data_dir.rglob("*.parquet"):
                paths.append(path)
                scan_progress.update(1)
        finally:
            scan_progress.close()

        self.connection.execute(
            "DROP TABLE IF EXISTS temp.openbb_existing_output_paths"
        )
        self.connection.execute(
            "CREATE TEMP TABLE openbb_existing_output_paths("
            "path TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        insert_size = max(1, int(batch_size))
        insert_progress = tqdm(
            total=len(paths),
            desc="openbb:index active parquet paths",
            unit="shard",
            disable=not self.show_progress,
        )
        try:
            for offset in range(0, len(paths), insert_size):
                batch = paths[offset : offset + insert_size]
                self.connection.executemany(
                    "INSERT OR IGNORE INTO openbb_existing_output_paths(path) VALUES (?)",
                    ((str(path),) for path in batch),
                )
                insert_progress.update(len(batch))
        finally:
            insert_progress.close()
        with _sqlite_progress(
            self.connection,
            "manifest:join parquet ownership",
            enabled=self.show_progress,
        ):
            rows = self.connection.execute(
                """
                SELECT tasks.output_path,tasks.status
                FROM tasks
                JOIN openbb_existing_output_paths AS existing
                  ON existing.path=tasks.output_path
                WHERE tasks.active=1 AND tasks.plan_token=?
                """,
                (plan_token,),
            ).fetchall()
        statuses = {str(row["output_path"]): str(row["status"]) for row in rows}
        quarantined = 0
        reconcile_progress = tqdm(
            paths,
            total=len(paths),
            desc="openbb:quarantine terminal parquet",
            unit="shard",
            disable=not self.show_progress,
        )
        try:
            for path in reconcile_progress:
                status = statuses.get(str(path))
                if status in {"success", "pending", "running"}:
                    continue
                if _quarantine_obsolete_output_path(path) is not None:
                    quarantined += 1
                    reconcile_progress.set_postfix(
                        quarantined=quarantined, refresh=False
                    )
        finally:
            reconcile_progress.close()
            self.connection.execute(
                "DROP TABLE IF EXISTS temp.openbb_existing_output_paths"
            )
        return len(paths), quarantined

    def finalize_provider_only_unavailable(
        self,
        provider: str,
        reason: str,
        *,
        plan_token: str,
        endpoint: str | None = None,
    ) -> int:
        """Resolve every still-pending task that has no provider fallback."""
        endpoint_clause = "" if endpoint is None else " AND endpoint=?"
        parameters: list[Any] = [
            provider,
            f"{provider}: unavailable for run ({reason[:1500]})",
            datetime.now(timezone.utc).isoformat(),
            plan_token,
            _canonical_json([provider]),
        ]
        if endpoint is not None:
            parameters.append(endpoint)
        cursor = self.connection.execute(
            f"""
            UPDATE tasks
            SET status='unavailable', selected_provider=?, error=?, updated_at=?
            WHERE active=1 AND plan_token=? AND status='pending'
              AND providers_json=?{endpoint_clause}
            """,
            tuple(parameters),
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def finalize_fully_route_unavailable(
        self,
        unavailable_providers: Mapping[str, str],
        unavailable_routes: Mapping[tuple[str, str], str],
        *,
        plan_token: str,
        endpoint: str | None = None,
    ) -> int:
        """Compatibility wrapper for route-only capability finalization."""
        return self.finalize_fully_capability_unavailable(
            unavailable_providers,
            unavailable_routes,
            {},
            plan_token=plan_token,
            endpoint=endpoint,
        )

    def finalize_fully_capability_unavailable(
        self,
        unavailable_providers: Mapping[str, str],
        unavailable_routes: Mapping[tuple[str, str], str],
        unavailable_domains: Mapping[tuple[str, str, str], str],
        *,
        plan_token: str,
        endpoint: str | None = None,
    ) -> int:
        """Resolve tasks only when every fallback is capability-disabled.

        Availability is one contract with three scopes: provider, route, and
        provider/route/market domain.  Evaluating those scopes together avoids
        both failure modes of one-off finalizers: retaining a task whose
        fallbacks are disabled at different scopes, or discarding a task that
        still has one valid provider in the same market.
        """
        globally_disabled = sorted(map(str, unavailable_providers))
        route_disabled = sorted(
            (str(provider), str(route))
            for provider, route in unavailable_routes
            if endpoint is None or str(route) == endpoint
        )
        domain_disabled = sorted(
            (str(provider), str(route), str(domain))
            for provider, route, domain in unavailable_domains
            if endpoint is None or str(route) == endpoint
        )
        if not globally_disabled and not route_disabled and not domain_disabled:
            return 0

        disabled_expressions: list[str] = []
        disabled_parameters: list[Any] = []
        if globally_disabled:
            placeholders = ",".join("?" for _ in globally_disabled)
            disabled_expressions.append(f"provider.value IN ({placeholders})")
            disabled_parameters.extend(globally_disabled)
        for provider, route in route_disabled:
            disabled_expressions.append("(provider.value=? AND tasks.endpoint=?)")
            disabled_parameters.extend((provider, route))
        for provider, route, domain in domain_disabled:
            disabled_expressions.append(
                "(provider.value=? AND tasks.endpoint=? "
                "AND COALESCE(openbb_capability_domain("
                "provider.value, tasks.kwargs_json),'')=?)"
            )
            disabled_parameters.extend((provider, route, domain))

        candidate_parameters: list[Any] = []
        if endpoint is not None:
            candidate_clause = " AND tasks.endpoint=?"
            candidate_parameters.append(endpoint)
        elif globally_disabled:
            # A provider-wide denial can affect every endpoint in the plan.
            candidate_clause = ""
        else:
            candidate_endpoints = sorted(
                {route for _, route in route_disabled}
                | {route for _, route, _ in domain_disabled}
            )
            placeholders = ",".join("?" for _ in candidate_endpoints)
            candidate_clause = f" AND tasks.endpoint IN ({placeholders})"
            candidate_parameters.extend(candidate_endpoints)

        now = datetime.now(timezone.utc).isoformat()
        cursor = self.connection.execute(
            f"""
            UPDATE tasks SET
                status='unavailable', selected_provider=NULL,
                error='all configured providers unavailable for task capability',
                provider_outcomes_json=COALESCE(
                    (
                        SELECT json_group_object(provider.value, 'unavailable')
                        FROM json_each(tasks.providers_json) AS provider
                    ),
                    '{{}}'
                ),
                execution_started_at=NULL, updated_at=?
            WHERE active=1 AND plan_token=? AND status='pending'
              {candidate_clause}
              AND NOT EXISTS (
                  SELECT 1 FROM json_each(tasks.providers_json) AS provider
                  WHERE NOT ({" OR ".join(disabled_expressions)})
              )
            """,
            (
                now,
                plan_token,
                *candidate_parameters,
                *disabled_parameters,
            ),
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def finalize_provider_domain_unavailable(
        self,
        provider: str,
        endpoint: str,
        domain: str,
        reason: str,
        *,
        plan_token: str,
    ) -> int:
        """Resolve provider-only tasks inside one proven market domain."""
        if domain not in {"us", "tw", "global"}:
            return 0
        symbol = "UPPER(COALESCE(json_extract(kwargs_json,'$.symbol'),''))"
        if domain == "tw":
            domain_clause = f"AND ({symbol} LIKE '%.TW' OR {symbol} LIKE '%.TWO')"
        elif domain == "global":
            domain_clause = f"AND {symbol}=''"
        else:
            domain_clause = (
                f"AND {symbol}!='' AND {symbol} NOT LIKE '%.TW' "
                f"AND {symbol} NOT LIKE '%.TWO'"
            )
        cursor = self.connection.execute(
            f"""
            UPDATE tasks
            SET status='unavailable', selected_provider=?, error=?, updated_at=?
            WHERE active=1 AND plan_token=? AND status='pending'
              AND providers_json=? AND endpoint=? {domain_clause}
            """,
            (
                provider,
                f"{provider}: unavailable for {domain} domain ({reason[:1500]})",
                datetime.now(timezone.utc).isoformat(),
                plan_token,
                _canonical_json([provider]),
                endpoint,
            ),
        )
        self.connection.commit()
        return max(0, int(cursor.rowcount))

    def empty_only_provider_domain_routes(
        self,
        provider: str,
        domain: str,
        *,
        plan_token: str,
        minimum_distinct_scopes: int,
    ) -> dict[str, int]:
        """Return routes with broad empty evidence and no domain success.

        This supports narrow provider/endpoint/market capability state without
        promoting one missing ticker to a whole-market denial.  It is used for
        providers whose symbol namespace is empirically unsupported in one
        market, while retaining successful routes and other markets.
        """
        if domain not in {"us", "tw", "global"}:
            return {}
        symbol = "UPPER(COALESCE(json_extract(kwargs_json,'$.symbol'),''))"
        if domain == "tw":
            domain_clause = f"({symbol} LIKE '%.TW' OR {symbol} LIKE '%.TWO')"
        elif domain == "global":
            domain_clause = f"{symbol}=''"
        else:
            domain_clause = (
                f"{symbol}!='' AND {symbol} NOT LIKE '%.TW' "
                f"AND {symbol} NOT LIKE '%.TWO'"
            )
        rows = self.connection.execute(
            f"""
            SELECT endpoint,
                   COUNT(DISTINCT scope_key) distinct_scopes,
                   SUM(CASE WHEN json_extract(provider_outcomes_json,?)='empty'
                            THEN 1 ELSE 0 END) empty_scopes,
                   SUM(CASE WHEN status='success' AND selected_provider=?
                            THEN 1 ELSE 0 END) success_scopes
            FROM tasks
            WHERE active=1 AND plan_token=? AND {domain_clause}
              AND (
                  json_type(provider_outcomes_json,?) IS NOT NULL
                  OR selected_provider=?
              )
            GROUP BY endpoint
            HAVING distinct_scopes>=? AND empty_scopes>=?
                   AND success_scopes=0
            """,
            (
                f"$.{provider}",
                provider,
                plan_token,
                f"$.{provider}",
                provider,
                max(1, int(minimum_distinct_scopes)),
                max(1, int(minimum_distinct_scopes)),
            ),
        ).fetchall()
        return {str(row["endpoint"]): int(row["empty_scopes"]) for row in rows}

    def prioritize_fmp_entitlement_probes(
        self,
        plan_token: str,
        archive_end_date: str,
    ) -> set[str]:
        """Move one latest canonical FMP-only scope per endpoint/domain first.

        The free tier cannot afford to discover endpoint coverage after
        millions of historical calls.  These representative tasks establish
        the current credential's capability breadth before deep backfill.
        """
        end = date.fromisoformat(archive_end_date)
        recent_year = end.year - 1
        recent_date = date(end.year - 1, end.month, min(end.day, 28)).isoformat()
        symbol = "UPPER(COALESCE(json_extract(kwargs_json,'$.symbol'),''))"
        domain_expression = (
            f"CASE WHEN {symbol}='' THEN 'global' "
            f"WHEN {symbol} LIKE '%.TW' OR {symbol} LIKE '%.TWO' THEN 'tw' "
            "ELSE 'us' END"
        )
        rows = self.connection.execute(
            f"""
            SELECT endpoint,{domain_expression} AS domain
            FROM tasks
            WHERE active=1 AND plan_token=? AND status='pending'
              AND providers_json=?
            GROUP BY endpoint,domain
            ORDER BY endpoint,domain
            """,
            (plan_token, _canonical_json(["fmp"])),
        ).fetchall()
        selected: set[str] = set()
        for row in rows:
            endpoint = str(row["endpoint"])
            domain = str(row["domain"])
            candidate = self.connection.execute(
                f"""
                SELECT task_id FROM tasks
                WHERE active=1 AND plan_token=? AND status='pending'
                  AND providers_json=? AND endpoint=?
                  AND {domain_expression}=?
                  AND (
                    (json_type(kwargs_json,'$.year') IS NOT NULL
                     AND CAST(json_extract(kwargs_json,'$.year') AS INTEGER)>=?)
                    OR (
                      json_type(kwargs_json,'$.year') IS NULL
                      AND (
                        json_type(kwargs_json,'$.end_date') IS NULL
                        OR SUBSTR(CAST(json_extract(kwargs_json,'$.end_date') AS TEXT),1,10)>=?
                      )
                    )
                  )
                ORDER BY
                  CASE {symbol}
                    WHEN 'AAPL' THEN 0 WHEN 'SPY' THEN 1
                    WHEN '2330.TW' THEN 0 WHEN '0050.TW' THEN 1
                    ELSE 2
                  END,
                  COALESCE(CAST(json_extract(kwargs_json,'$.year') AS INTEGER),0) DESC,
                  COALESCE(CAST(json_extract(kwargs_json,'$.quarter') AS INTEGER),0) DESC,
                  COALESCE(CAST(json_extract(kwargs_json,'$.end_date') AS TEXT),'') DESC,
                  task_id
                LIMIT 1
                """,
                (
                    plan_token,
                    _canonical_json(["fmp"]),
                    endpoint,
                    domain,
                    recent_year,
                    recent_date,
                ),
            ).fetchone()
            if candidate is not None:
                selected.add(str(candidate["task_id"]))
        if selected:
            self.connection.executemany(
                "UPDATE tasks SET updated_at=? WHERE task_id=? AND status='pending'",
                [("0001-01-01T00:00:00+00:00", task_id) for task_id in selected],
            )
            self.connection.commit()
        return selected

    def finalize_exhausted_pending(
        self,
        max_total_attempts: int,
        *,
        plan_token: str,
    ) -> int:
        """Compatibility no-op: transient work is never count-exhausted."""
        _ = (max_total_attempts, plan_token)
        return 0

    def record_provider_event(
        self, provider: str, event: str, message: str | None
    ) -> None:
        safe_message = (message or "")[:2000]
        self.connection.execute(
            "INSERT INTO provider_events(provider, event, message, created_at) VALUES (?, ?, ?, ?)",
            (provider, event, safe_message, datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()

    def counts(self, plan_token: str | None = None) -> dict[str, int]:
        clause = " WHERE active=1"
        if plan_token is not None:
            clause += " AND plan_token=?"
        parameters: tuple[Any, ...] = () if plan_token is None else (plan_token,)
        return {
            row["status"]: int(row["count"])
            for row in self.connection.execute(
                f"SELECT status, COUNT(*) AS count FROM tasks{clause} GROUP BY status",
                parameters,
            )
        }


class ResizableBoundedSemaphore:
    """A small non-blocking semaphore whose capacity can grow at runtime."""

    def __init__(self, value: int) -> None:
        self._limit = max(1, int(value))
        self._acquired = 0
        self._condition = threading.Condition()

    @property
    def limit(self) -> int:
        with self._condition:
            return self._limit

    def set_limit(self, value: int) -> None:
        with self._condition:
            self._limit = max(self._limit, max(1, int(value)))
            self._condition.notify_all()

    def acquire(self, blocking: bool = True, timeout: float | None = None) -> bool:
        with self._condition:
            if not blocking:
                if self._acquired >= self._limit:
                    return False
                self._acquired += 1
                return True
            deadline = None if timeout is None else time.monotonic() + timeout
            while self._acquired >= self._limit:
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            self._acquired += 1
            return True

    def release(self) -> None:
        with self._condition:
            if self._acquired <= 0:
                raise ValueError("semaphore released too many times")
            self._acquired -= 1
            self._condition.notify()


class ProviderExecutorPool:
    """Keep prefetched work in independent bounded provider executors.

    A single large ``ThreadPoolExecutor`` eagerly creates one thread for every
    submitted prefetch task until its global maximum is reached.  Those
    threads then block on provider semaphores, so a 1,792-task archive buffer
    can become roughly 1,792 operating-system threads even though only a few
    hundred provider service slots exist.  Besides wasting memory, that thread
    storm delays the dedicated rate-limiter dispatchers and lowers every busy
    provider's request-start cadence.

    Each lane below has its own bounded executor and internal work queue.
    Provider queues therefore remain independent while queued tasks consume no
    thread.  The shared semaphore preserves the caller's aggregate ``workers``
    ceiling when several provider lane limits add up beyond it.
    """

    def __init__(
        self,
        global_worker_limit: int,
        lane_limit: Callable[[str | None], int],
    ) -> None:
        self.global_worker_limit = max(1, int(global_worker_limit))
        self._lane_limit = lane_limit
        self._global_slots = threading.BoundedSemaphore(self.global_worker_limit)
        self._executors: dict[str | None, ThreadPoolExecutor] = {}
        self._limits: dict[str | None, int] = {}
        self._lock = threading.Lock()
        self._closed = False

    @staticmethod
    def _lane_name(provider: str | None) -> str:
        raw = "unassigned" if provider is None else str(provider)
        return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw)[:40] or "provider"

    def _executor(self, provider: str | None) -> ThreadPoolExecutor:
        with self._lock:
            if self._closed:
                raise RuntimeError("provider executor pool is closed")
            executor = self._executors.get(provider)
            if executor is not None:
                return executor
            limit = max(
                1,
                min(
                    self.global_worker_limit,
                    int(self._lane_limit(provider)),
                ),
            )
            executor = ThreadPoolExecutor(
                max_workers=limit,
                thread_name_prefix=f"openbb-{self._lane_name(provider)}",
            )
            self._executors[provider] = executor
            self._limits[provider] = limit
            return executor

    def submit(
        self,
        provider: str | None,
        function: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        executor = self._executor(provider)

        def run_with_global_slot() -> Any:
            with self._global_slots:
                return function(*args, **kwargs)

        return executor.submit(run_with_global_slot)

    def lane_limits(self) -> dict[str | None, int]:
        with self._lock:
            return dict(self._limits)

    def shutdown(self, wait_for_tasks: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            executors = tuple(self._executors.values())
        for executor in executors:
            executor.shutdown(wait=wait_for_tasks)

    def __enter__(self) -> ProviderExecutorPool:
        return self

    def __exit__(self, *_: Any) -> None:
        self.shutdown(wait_for_tasks=True)


def _adaptive_executor_lane_limit(
    runtime: ProviderRuntime,
    provider: str,
    current: int,
) -> int:
    """Reserve one adaptive service step, never the whole standby horizon.

    The runtime's adaptive cap represents up to fifteen seconds of observed
    provider residence time. Reserving that entire ceiling as executor
    threads eagerly creates workers that merely wait on the provider
    semaphore, increasing GIL/context-switch pressure for every market. The
    executor needs only the current Little's-Law service slots plus one
    evidence-driven expansion step. All additional prefetched work remains in
    the executor queue and consumes no thread.
    """
    current = max(1, int(current))
    adaptive_caps = getattr(runtime, "_adaptive_concurrency_caps", {})
    adaptive_cap = max(current, int(adaptive_caps.get(provider, current)))
    runtime_rps = getattr(runtime, "rps", {})
    rate = max(
        0.001,
        float(runtime_rps.get(provider, DEFAULT_UNDOCUMENTED_PROVIDER_RPS)),
    )
    expansion_step = max(1, math.ceil(rate * 0.5))
    return min(adaptive_cap, current + expansion_step)


class HttpBoundaryRateLimiter(SharedRateLimiter):
    """Unify explicit page tickets with real HTTP-boundary tickets.

    Several provider adapters explicitly claim a continuation-page ticket and
    then call a shared HTTP client.  The process-wide HTTP governor must not
    count that same request a second time.  A preclaim is thread-local because
    the matching synchronous request or async event loop runs in the same
    archive worker thread; any later child requests still claim fresh slots.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._boundary_preclaims = threading.local()

    def wait(self) -> None:
        super().wait()
        self._boundary_preclaims.count = (
            int(getattr(self._boundary_preclaims, "count", 0)) + 1
        )

    def wait_at_http_boundary(self) -> None:
        preclaims = int(getattr(self._boundary_preclaims, "count", 0))
        if preclaims > 0:
            self._boundary_preclaims.count = preclaims - 1
            return
        super().wait()

    def wait_explicit_boundary(self) -> None:
        """Claim a suppressed direct request without leaving a preclaim."""
        super().wait()


class ProviderRuntime:
    def __init__(
        self,
        rps: Mapping[str, float],
        concurrency: Mapping[str, int],
        quota_cooldown: float,
        cooldown_state_path: Path | None = None,
    ) -> None:
        self.rps = dict(rps)
        self.concurrency = dict(concurrency)
        self.quota_cooldown = max(1.0, float(quota_cooldown))
        self.cooldown_state_path = cooldown_state_path
        self._limiters: dict[str, SharedRateLimiter] = {}
        self._semaphores: dict[str, ResizableBoundedSemaphore] = {}
        self._active_calls: dict[str, int] = {}
        self._adaptive_concurrency_caps = {}
        for provider, rate in self.rps.items():
            baseline = max(1, int(self.concurrency.get(provider, 1)))
            self._adaptive_concurrency_caps[provider] = max(
                baseline,
                min(
                    ADAPTIVE_PROVIDER_CONCURRENCY_CAP,
                    max(
                        1,
                        math.ceil(
                            float(rate) * ADAPTIVE_PROVIDER_LATENCY_HORIZON_SECONDS
                        ),
                    ),
                ),
            )
        self._concurrency_expansions: dict[str, int] = {}
        self._blocked_until: dict[str, float] = {}
        self._blocked_reason: dict[str, str] = {}
        self._blocked_kind: dict[str, str] = {}
        self._unavailable: dict[str, str] = {}
        self._route_unavailable: dict[tuple[str, str], str] = {}
        self._domain_unavailable: dict[tuple[str, str, str], str] = {}
        self._parameter_maximums: dict[tuple[str, str, str], int] = {}
        self._omitted_parameters: dict[tuple[str, str], tuple[str, ...]] = {}
        self._rate_claim_times: dict[str, deque[float]] = {}
        self._rate_claim_totals: dict[str, int] = {}
        # Thread-local task attribution lets the transport wrappers report the
        # true HTTP fan-out of every endpoint without coupling OpenBB provider
        # adapters to the scheduler.  Aggregates survive restarts and drive
        # quota/ETA projections; they are observations, never rate controls.
        self._request_observation_context = threading.local()
        self._endpoint_request_costs: dict[tuple[str, str], dict[str, Any]] = {}
        self._request_window_counts: dict[str, dict[str, Any]] = {}
        self._observed_quota_limits: dict[str, dict[str, Any]] = {}
        self._rate_session_started_at = time.time()
        self._last_rate_activity_persisted_at = 0.0
        self._lock = threading.Lock()
        self._load_cooldown_state()
        if self.cooldown_state_path is not None:
            # Publish effective RPS/concurrency and any schema-1 daily-window
            # normalization immediately.  The monitor can then audit the live
            # pacing contract even before the first cooldown occurs.
            with self._lock:
                self._persist_cooldown_state_locked()

    def _load_cooldown_state(self) -> None:
        path = self.cooldown_state_path
        if path is None or not path.is_file():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            providers = payload.get("providers", {})
            now = time.time()
            if not isinstance(providers, Mapping):
                return
            schema_version = int(payload.get("schema_version") or 1)
            for provider, value in providers.items():
                if isinstance(value, Mapping):
                    blocked_until = float(value.get("blocked_until") or 0.0)
                    reason = str(value.get("reason") or "")[:1000]
                    kind = str(value.get("kind") or "unknown")[:50]
                else:
                    blocked_until = float(value)
                    reason = "legacy cooldown without persisted reason"
                    kind = "legacy"
                if blocked_until > now:
                    provider_name = str(provider)
                    if (
                        provider_name == "sec"
                        and "sec rate limit cooldown until" in reason.lower()
                    ):
                        # Schema-3 could persist an internal waiter recheck as
                        # though it were a fresh upstream quota response. That
                        # created a self-extending one-hour SEC cooldown. The
                        # original transient/rate event has already supplied
                        # its own backpressure; never restore this control-plane
                        # echo on restart.
                        continue
                    # Schema-1 checkpoints did not record why a provider was
                    # blocked.  A long (>15m) BLS/FMP block can only have come
                    # from their quota path because transient retry backoff is
                    # capped at two minutes.  Upgrade those legacy deadlines to
                    # the provider's real daily reset instead of retrying every
                    # hour after a supervisor restart.
                    if provider_name in {"bls", "fmp"} and blocked_until - now > 900:
                        blocked_until = max(
                            blocked_until,
                            self._quota_reset_deadline(
                                provider_name,
                                "daily limit",
                                now,
                            ),
                        )
                    self._blocked_until[provider_name] = blocked_until
                    self._blocked_reason[provider_name] = reason
                    self._blocked_kind[provider_name] = (
                        kind if schema_version >= 3 else "legacy"
                    )
                    # Older checkpoints treated messages such as Tiingo's
                    # explicit "daily request allocation" as the generic
                    # one-hour cooldown. Normalize every provider's clear
                    # daily wording to its semantic day boundary on load.
                    if (
                        self._blocked_kind[provider_name] == "quota"
                        and "daily" in reason.lower()
                    ):
                        # The old checkpoint deadline was a generic cooldown,
                        # not upstream reset evidence. Replace it with the
                        # semantic provider boundary: keeping the maximum can
                        # oversleep past midnight when the archive restarts in
                        # the final hour of the provider day.
                        self._blocked_until[provider_name] = self._quota_reset_deadline(
                            provider_name, reason, now
                        )
            unavailable = payload.get("unavailable_providers", {})
            if isinstance(unavailable, Mapping):
                self._unavailable.update(
                    {
                        str(provider): str(reason)[:1000]
                        for provider, reason in unavailable.items()
                    }
                )
            unavailable_routes = payload.get("unavailable_routes", [])
            if isinstance(unavailable_routes, list):
                for item in unavailable_routes:
                    if not isinstance(item, Mapping):
                        continue
                    provider = str(item.get("provider", "")).strip()
                    endpoint = str(item.get("endpoint", "")).strip()
                    if provider and endpoint:
                        self._route_unavailable[(provider, endpoint)] = str(
                            item.get("reason", "route unavailable")
                        )[:1000]
            unavailable_domains = payload.get("unavailable_domains", [])
            if isinstance(unavailable_domains, list):
                for item in unavailable_domains:
                    if not isinstance(item, Mapping):
                        continue
                    provider = str(item.get("provider", "")).strip()
                    endpoint = str(item.get("endpoint", "")).strip()
                    domain = str(item.get("domain", "")).strip()
                    if provider and endpoint and domain:
                        self._domain_unavailable[(provider, endpoint, domain)] = str(
                            item.get("reason", "domain unavailable")
                        )[:1000]
            parameter_maximums = payload.get("parameter_maximums", [])
            if isinstance(parameter_maximums, list):
                for item in parameter_maximums:
                    if not isinstance(item, Mapping):
                        continue
                    provider = str(item.get("provider", "")).strip()
                    endpoint = str(item.get("endpoint", "")).strip()
                    parameter = str(item.get("parameter", "")).strip().lower()
                    maximum = int(item.get("maximum") or 0)
                    if provider and endpoint and parameter and maximum > 0:
                        self._parameter_maximums[(provider, endpoint, parameter)] = (
                            maximum
                        )
            omitted_parameters = payload.get("omitted_parameters", [])
            if isinstance(omitted_parameters, list):
                for item in omitted_parameters:
                    if not isinstance(item, Mapping):
                        continue
                    provider = str(item.get("provider", "")).strip()
                    endpoint = str(item.get("endpoint", "")).strip()
                    parameters = tuple(
                        sorted(
                            {
                                str(value).strip()
                                for value in item.get("parameters", [])
                                if str(value).strip()
                            }
                        )
                    )
                    if provider and endpoint and parameters:
                        self._omitted_parameters[(provider, endpoint)] = parameters
            request_windows = payload.get("request_windows", {})
            raw_window_providers = (
                request_windows.get("providers", {})
                if isinstance(request_windows, Mapping)
                else {}
            )
            if isinstance(raw_window_providers, Mapping):
                for provider, raw in raw_window_providers.items():
                    if not isinstance(raw, Mapping):
                        continue
                    provider_name = str(provider)
                    current = self._request_window_identity(provider_name, now)
                    restored = dict(current)
                    if str(raw.get("utc_hour_key") or "") == current["utc_hour_key"]:
                        restored["utc_hour_claims"] = max(
                            0, int(raw.get("utc_hour_claims") or 0)
                        )
                    if (
                        str(raw.get("provider_day_key") or "")
                        == current["provider_day_key"]
                    ):
                        restored["provider_day_claims"] = max(
                            0, int(raw.get("provider_day_claims") or 0)
                        )
                    self._request_window_counts[provider_name] = restored
            observed_limits = payload.get("observed_quota_limits", {})
            if isinstance(observed_limits, Mapping):
                self._observed_quota_limits.update(
                    {
                        str(provider): dict(raw)
                        for provider, raw in observed_limits.items()
                        if isinstance(raw, Mapping)
                    }
                )
            endpoint_request_costs = payload.get("endpoint_request_costs", [])
            if isinstance(endpoint_request_costs, list):
                for raw in endpoint_request_costs:
                    if not isinstance(raw, Mapping):
                        continue
                    provider = str(raw.get("provider") or "").strip()
                    endpoint = str(raw.get("endpoint") or "").strip()
                    requests = max(0, int(raw.get("requests") or 0))
                    claiming_attempts = max(0, int(raw.get("claiming_attempts") or 0))
                    expected_revision = _endpoint_request_cost_revision(
                        provider, endpoint
                    )
                    observed_revision = max(
                        1, int(raw.get("implementation_revision") or 1)
                    )
                    if (
                        not provider
                        or not endpoint
                        or requests <= 0
                        or observed_revision != expected_revision
                    ):
                        continue
                    self._endpoint_request_costs[(provider, endpoint)] = {
                        "requests": requests,
                        "claiming_attempts": claiming_attempts,
                        "max_requests_per_attempt": max(
                            1, int(raw.get("max_requests_per_attempt") or 1)
                        ),
                        "last_observed_at": str(raw.get("last_observed_at") or ""),
                        "implementation_revision": expected_revision,
                    }
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            # A damaged optional runtime checkpoint must never prevent the
            # manifest-backed archive from starting.  The next block event
            # atomically replaces it with a valid document.
            return

    @staticmethod
    def _request_window_identity(provider: str, now: float) -> dict[str, Any]:
        """Resolve durable accounting buckets using each provider's reset basis."""
        now_utc = datetime.fromtimestamp(float(now), tz=timezone.utc)
        utc_hour_key = now_utc.strftime("%Y-%m-%dT%H:00:00Z")
        if provider == "fmp":
            # FMP Basic resets at 15:00 EST (fixed UTC-05), matching the
            # cooldown calculation rather than daylight-saving wall time.
            local = now_utc.astimezone(timezone(timedelta(hours=-5)))
            accounting_date = (
                local - timedelta(days=1) if local.hour < 15 else local
            ).date()
            provider_day_key = f"fmp-est-15/{accounting_date.isoformat()}"
            day_basis = "15:00 EST"
        elif provider == "tiingo":
            # Tiingo explicitly resets daily allocations at midnight EST.
            # Treat EST literally as UTC-05 instead of daylight-saving ET,
            # matching the provider wording and quota cooldown deadline.
            local = now_utc.astimezone(timezone(timedelta(hours=-5)))
            provider_day_key = f"tiingo-est/{local.date().isoformat()}"
            day_basis = "midnight EST"
        elif provider == "bls":
            local = now_utc.astimezone(ZoneInfo("America/New_York"))
            provider_day_key = f"bls-us-eastern/{local.date().isoformat()}"
            day_basis = "US/Eastern calendar day"
        else:
            provider_day_key = f"utc/{now_utc.date().isoformat()}"
            day_basis = "UTC calendar day"
        return {
            "utc_hour_key": utc_hour_key,
            "utc_hour_claims": 0,
            "provider_day_key": provider_day_key,
            "provider_day_claims": 0,
            "provider_day_basis": day_basis,
        }

    def _request_window_locked(self, provider: str, now: float) -> dict[str, Any]:
        current = self._request_window_identity(provider, now)
        stored = self._request_window_counts.get(provider)
        if stored is not None:
            if stored.get("utc_hour_key") == current["utc_hour_key"]:
                current["utc_hour_claims"] = max(
                    0, int(stored.get("utc_hour_claims", 0))
                )
            if stored.get("provider_day_key") == current["provider_day_key"]:
                current["provider_day_claims"] = max(
                    0, int(stored.get("provider_day_claims", 0))
                )
        self._request_window_counts[provider] = current
        return current

    def _request_windows_snapshot_locked(self, now: float) -> dict[str, Any]:
        providers: dict[str, dict[str, Any]] = {}
        for provider in sorted(
            set(self.rps)
            | set(self._request_window_counts)
            | set(self._rate_claim_totals)
        ):
            providers[provider] = dict(self._request_window_locked(provider, now))
        return {"providers": providers}

    def _persist_cooldown_state_locked(self) -> None:
        path = self.cooldown_state_path
        if path is None:
            return
        now = time.time()
        providers = {
            provider: {
                "blocked_until": blocked_until,
                "kind": self._blocked_kind.get(provider, "unknown"),
                "reason": self._blocked_reason.get(provider, "")[:1000],
            }
            for provider, blocked_until in self._blocked_until.items()
            if blocked_until > now
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        rate_activity = self._rate_activity_snapshot_locked(now)
        payload = {
            "schema_version": 8,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "rate_limits_rps": dict(sorted(self.rps.items())),
            "concurrency": dict(sorted(self.concurrency.items())),
            "rate_activity": rate_activity,
            "request_windows": self._request_windows_snapshot_locked(now),
            "observed_quota_limits": dict(sorted(self._observed_quota_limits.items())),
            "endpoint_request_costs": [
                {
                    "provider": provider,
                    "endpoint": endpoint,
                    **cost,
                    "average_requests_per_claiming_attempt": round(
                        int(cost["requests"]) / max(1, int(cost["claiming_attempts"])),
                        6,
                    ),
                }
                for (provider, endpoint), cost in sorted(
                    self._endpoint_request_costs.items()
                )
            ],
            "providers": providers,
            "unavailable_providers": dict(sorted(self._unavailable.items())),
            "unavailable_routes": [
                {
                    "provider": provider,
                    "endpoint": endpoint,
                    "reason": reason,
                }
                for (provider, endpoint), reason in sorted(
                    self._route_unavailable.items()
                )
            ],
            "unavailable_domains": [
                {
                    "provider": provider,
                    "endpoint": endpoint,
                    "domain": domain,
                    "reason": reason,
                }
                for (provider, endpoint, domain), reason in sorted(
                    self._domain_unavailable.items()
                )
            ],
            "parameter_maximums": [
                {
                    "provider": provider,
                    "endpoint": endpoint,
                    "parameter": parameter,
                    "maximum": maximum,
                }
                for (provider, endpoint, parameter), maximum in sorted(
                    self._parameter_maximums.items()
                )
            ],
            "omitted_parameters": [
                {
                    "provider": provider,
                    "endpoint": endpoint,
                    "parameters": list(parameters),
                }
                for (provider, endpoint), parameters in sorted(
                    self._omitted_parameters.items()
                )
            ],
        }
        try:
            temporary.write_text(
                json.dumps(payload, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def _rate_activity_snapshot_locked(
        self, now: float | None = None
    ) -> dict[str, Any]:
        now = time.time() if now is None else float(now)
        session_seconds = max(0.001, now - self._rate_session_started_at)
        providers: dict[str, dict[str, float | int]] = {}
        for provider in sorted(set(self.rps) | set(self._rate_claim_totals)):
            claims = self._rate_claim_times.setdefault(provider, deque())
            cutoff = now - 60.0
            while claims and claims[0] < cutoff:
                claims.popleft()
            target_rps = max(
                0.001,
                float(self.rps.get(provider, DEFAULT_UNDOCUMENTED_PROVIDER_RPS)),
            )
            limiter = self._limiters.get(provider)
            waiters = limiter.pending_waiters() if limiter is not None else 0
            grant_activity_method = (
                getattr(limiter, "grant_activity", None)
                if limiter is not None
                else None
            )
            grant_activity = (
                grant_activity_method(now)
                if callable(grant_activity_method)
                else {
                    "grants_total": int(self._rate_claim_totals.get(provider, 0)),
                    "grants_last_60s": len(claims),
                    "window_seconds": min(60.0, session_seconds),
                    "pending_claim_observations": 0,
                }
            )
            window_seconds = max(0.001, float(grant_activity["window_seconds"]))
            grants_last_60s = int(grant_activity["grants_last_60s"])
            observed_rps = grants_last_60s / window_seconds
            active_calls = int(self._active_calls.get(provider, 0))
            request_window = self._request_window_locked(provider, now)
            daily_cap = PROVIDER_DECLARED_DAILY_REQUEST_CAPS.get(provider)
            hourly_cap = PROVIDER_DECLARED_HOURLY_REQUEST_CAPS.get(provider)
            current_concurrency = max(1, int(self.concurrency.get(provider, 1)))
            adaptive_cap = max(
                current_concurrency,
                int(self._adaptive_concurrency_caps.get(provider, current_concurrency)),
            )
            # A full execution pool with no limiter waiter means all slots are
            # occupied after their request-start tickets (network response,
            # provider parsing, Parquet preparation, or child-request fanout).
            # If the measured HTTP-boundary start rate is still below target,
            # Little's Law requires more outer service slots. Fanout remains
            # safe because every child HTTP start shares the exact provider
            # boundary limiter: concurrency can increase work supply but can
            # never raise the request-rate ceiling.
            if (
                session_seconds >= 30.0
                and current_concurrency < adaptive_cap
                and active_calls >= current_concurrency
                and waiters == 0
                and observed_rps < target_rps * 0.95
                and int(grant_activity["grants_total"])
                >= max(5, math.ceil(target_rps * 5.0))
            ):
                step = max(1, math.ceil(target_rps * 0.5))
                expanded = min(adaptive_cap, current_concurrency + step)
                self.concurrency[provider] = expanded
                semaphore = self._semaphores.get(provider)
                if semaphore is not None:
                    semaphore.set_limit(expanded)
                self._concurrency_expansions[provider] = (
                    self._concurrency_expansions.get(provider, 0) + 1
                )
                current_concurrency = expanded
            providers[provider] = {
                # Grant timing belongs to the cadence-owning dispatcher. The
                # asynchronous observer below remains the durable quota
                # accountant but may lag under high fanout; timestamping that
                # lag would falsely report a request-rate deficit.
                "limiter_claims_total": int(grant_activity["grants_total"]),
                "limiter_observed_claims_total": int(
                    self._rate_claim_totals.get(provider, 0)
                ),
                "pending_claim_observations": int(
                    grant_activity["pending_claim_observations"]
                ),
                # Session totals diagnose live utilization. Window totals are
                # the durable account-allocation evidence across restarts.
                "limiter_claims_current_utc_hour": int(
                    request_window["utc_hour_claims"]
                ),
                "current_utc_hour_key": str(request_window["utc_hour_key"]),
                "limiter_claims_current_provider_day": int(
                    request_window["provider_day_claims"]
                ),
                "current_provider_day_key": str(request_window["provider_day_key"]),
                "provider_day_basis": str(request_window["provider_day_basis"]),
                "declared_hourly_request_cap": hourly_cap,
                "declared_hourly_requests_remaining": (
                    max(0, hourly_cap - int(request_window["utc_hour_claims"]))
                    if hourly_cap is not None
                    else None
                ),
                "declared_daily_request_cap": daily_cap,
                "declared_daily_requests_remaining": (
                    max(0, daily_cap - int(request_window["provider_day_claims"]))
                    if daily_cap is not None
                    else None
                ),
                "limiter_claims_last_60s": grants_last_60s,
                "window_seconds": round(window_seconds, 6),
                "observed_claims_per_second": round(observed_rps, 6),
                "target_requests_per_second": target_rps,
                "utilization_percent": round(
                    min(100.0, observed_rps / target_rps * 100.0), 3
                ),
                "ticket_waiters": waiters,
                "active_calls": active_calls,
                "effective_concurrency": current_concurrency,
                "adaptive_concurrency_cap": adaptive_cap,
                "concurrency_expansions": int(
                    self._concurrency_expansions.get(provider, 0)
                ),
                "endpoint_request_costs": {
                    endpoint: {
                        **cost,
                        "average_requests_per_claiming_attempt": round(
                            int(cost["requests"])
                            / max(1, int(cost["claiming_attempts"])),
                            6,
                        ),
                    }
                    for (cost_provider, endpoint), cost in sorted(
                        self._endpoint_request_costs.items()
                    )
                    if cost_provider == provider
                },
            }
        return {
            "session_started_at": datetime.fromtimestamp(
                self._rate_session_started_at, tz=timezone.utc
            ).isoformat(),
            "providers": providers,
        }

    def _record_rate_claim(self, provider: str) -> None:
        now = time.time()
        with self._lock:
            claims = self._rate_claim_times.setdefault(provider, deque())
            claims.append(now)
            cutoff = now - 60.0
            while claims and claims[0] < cutoff:
                claims.popleft()
            self._rate_claim_totals[provider] = (
                self._rate_claim_totals.get(provider, 0) + 1
            )
            request_window = self._request_window_locked(provider, now)
            request_window["utc_hour_claims"] = (
                int(request_window["utc_hour_claims"]) + 1
            )
            request_window["provider_day_claims"] = (
                int(request_window["provider_day_claims"]) + 1
            )
            if (
                self.cooldown_state_path is not None
                and now - self._last_rate_activity_persisted_at >= 5.0
            ):
                self._last_rate_activity_persisted_at = now
                self._persist_cooldown_state_locked()

    def _record_endpoint_rate_claim(self, provider: str) -> None:
        """Attach one granted request to the current data-worker endpoint."""
        observation = getattr(self._request_observation_context, "current", None)
        if not isinstance(observation, dict):
            return
        claims_by_provider = observation.setdefault("claims_by_provider", {})
        claims_by_provider[provider] = int(claims_by_provider.get(provider, 0)) + 1

    def begin_request_observation(self, endpoint: str) -> dict[str, Any]:
        """Attribute subsequent limiter claims in this worker to one endpoint."""
        observation = {
            "endpoint": str(endpoint).lstrip("."),
            "claims_by_provider": {},
            "previous": getattr(self._request_observation_context, "current", None),
        }
        self._request_observation_context.current = observation
        return observation

    def finish_request_observation(self, observation: Mapping[str, Any]) -> None:
        """Publish one endpoint attempt's actual per-provider request cost."""
        current = getattr(self._request_observation_context, "current", None)
        if current is observation:
            previous = observation.get("previous")
            if previous is None:
                try:
                    del self._request_observation_context.current
                except AttributeError:
                    pass
            else:
                self._request_observation_context.current = previous
        endpoint = str(observation.get("endpoint") or "").strip()
        claims_by_provider = observation.get("claims_by_provider", {})
        if not endpoint or not isinstance(claims_by_provider, Mapping):
            return
        observed_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            for provider, raw_claims in claims_by_provider.items():
                claims = max(0, int(raw_claims or 0))
                if claims <= 0:
                    continue
                key = (str(provider), endpoint)
                aggregate = self._endpoint_request_costs.setdefault(
                    key,
                    {
                        "requests": 0,
                        "claiming_attempts": 0,
                        "max_requests_per_attempt": 0,
                        "last_observed_at": "",
                        "implementation_revision": (
                            _endpoint_request_cost_revision(str(provider), endpoint)
                        ),
                    },
                )
                aggregate["requests"] = int(aggregate["requests"]) + claims
                aggregate["claiming_attempts"] = int(aggregate["claiming_attempts"]) + 1
                aggregate["max_requests_per_attempt"] = max(
                    int(aggregate["max_requests_per_attempt"]), claims
                )
                aggregate["last_observed_at"] = observed_at

    def rate_activity(self) -> dict[str, Any]:
        """Return current per-provider limiter-claim rates for live auditing."""
        with self._lock:
            return self._rate_activity_snapshot_locked()

    def limiter(self, provider: str) -> SharedRateLimiter:
        with self._lock:
            if provider not in self._limiters:
                rps = max(
                    0.001,
                    float(self.rps.get(provider, DEFAULT_UNDOCUMENTED_PROVIDER_RPS)),
                )
                limiter_class = (
                    HttpBoundaryRateLimiter
                    if provider in HTTP_BOUNDARY_PACED_PROVIDERS
                    else SharedRateLimiter
                )
                # Keep independently-run Yahoo downloaders on the same
                # process-shared limiter bucket.  ``yfinance`` is OpenBB's
                # provider name; the direct Yahoo downloader uses the
                # canonical upstream/account bucket ``yahoo_finance``.
                limiter_name = {
                    "yfinance": "yahoo_finance",
                }.get(provider, provider)
                self._limiters[provider] = limiter_class(
                    1.0 / rps,
                    name=limiter_name,
                    on_claim=lambda provider=provider: self._record_rate_claim(
                        provider
                    ),
                    on_caller_claim=lambda provider=provider: (
                        self._record_endpoint_rate_claim(provider)
                    ),
                )
            return self._limiters[provider]

    def semaphore(self, provider: str) -> ResizableBoundedSemaphore:
        with self._lock:
            if provider not in self._semaphores:
                value = max(1, int(self.concurrency.get(provider, 2)))
                self._semaphores[provider] = ResizableBoundedSemaphore(value)
            return self._semaphores[provider]

    @contextmanager
    def semaphore_slot(self, provider: str) -> Iterator[None]:
        """Wait for one provider service slot without returning task churn.

        The archive executor intentionally preloads a bounded provider queue so
        network work can continue while the control-plane thread persists
        Parquet/manifest results. A queued worker therefore waits here; the
        provider's HTTP-boundary limiter remains the authoritative request-rate
        ceiling after the service slot is acquired.
        """
        semaphore = self.semaphore(provider)
        semaphore.acquire()
        with self._lock:
            self._active_calls[provider] = self._active_calls.get(provider, 0) + 1
        try:
            yield
        finally:
            with self._lock:
                self._active_calls[provider] = max(
                    0, self._active_calls.get(provider, 0) - 1
                )
            semaphore.release()

    @contextmanager
    def try_semaphore(self, provider: str) -> Iterator[bool]:
        """Yield immediately instead of occupying a worker while capacity is busy."""
        semaphore = self.semaphore(provider)
        acquired = semaphore.acquire(blocking=False)
        if acquired:
            with self._lock:
                self._active_calls[provider] = self._active_calls.get(provider, 0) + 1
        try:
            yield acquired
        finally:
            if acquired:
                with self._lock:
                    self._active_calls[provider] = max(
                        0, self._active_calls.get(provider, 0) - 1
                    )
                semaphore.release()

    def availability(
        self,
        provider: str,
        endpoint: str | None = None,
        domain: str | None = None,
    ) -> tuple[bool, str | None]:
        with self._lock:
            if provider in self._unavailable:
                return False, self._unavailable[provider]
            if endpoint is not None and (provider, endpoint) in self._route_unavailable:
                return False, self._route_unavailable[(provider, endpoint)]
            if (
                endpoint is not None
                and domain is not None
                and (provider, endpoint, domain) in self._domain_unavailable
            ):
                return False, self._domain_unavailable[(provider, endpoint, domain)]
            until = self._blocked_until.get(provider, 0.0)
            if until > time.time():
                return (
                    False,
                    f"cooldown until {datetime.fromtimestamp(until, tz=timezone.utc).isoformat()}",
                )
            return True, None

    def cooldown_providers(self) -> set[str]:
        now = time.time()
        with self._lock:
            return {
                provider
                for provider, blocked_until in self._blocked_until.items()
                if blocked_until > now
            }

    def next_cooldown_delay(self) -> float | None:
        now = time.time()
        with self._lock:
            delays = [
                blocked_until - now
                for blocked_until in self._blocked_until.values()
                if blocked_until > now
            ]
        return min(delays) if delays else None

    def cooldown_deadlines(self) -> dict[str, str]:
        now = time.time()
        with self._lock:
            return {
                provider: datetime.fromtimestamp(
                    blocked_until, tz=timezone.utc
                ).isoformat()
                for provider, blocked_until in self._blocked_until.items()
                if blocked_until > now
            }

    def clear_legacy_cooldowns(self, providers: Iterable[str]) -> tuple[str, ...]:
        """Clear only reasonless legacy blocks disproved by manifest evidence."""
        cleared: list[str] = []
        with self._lock:
            for provider in set(map(str, providers)):
                if self._blocked_kind.get(provider) != "legacy":
                    continue
                self._blocked_until.pop(provider, None)
                self._blocked_reason.pop(provider, None)
                self._blocked_kind.pop(provider, None)
                cleared.append(provider)
            if cleared:
                self._persist_cooldown_state_locked()
        return tuple(sorted(cleared))

    def clear_false_global_unavailable(self) -> tuple[str, ...]:
        """Remove persisted provider bans not proven by credential evidence.

        Older workers promoted every exception whose class contained
        ``Unauthorized`` to provider-wide state.  Some adapters use that class
        for 402 endpoint entitlements and even ``404 -> []`` query misses.
        Revalidate the durable reason using the stricter provider-level rule so
        one market/symbol cannot keep every route disabled after restart.
        """
        cleared: list[str] = []
        with self._lock:
            for provider, reason in list(self._unavailable.items()):
                if _is_provider_global_auth_failure(provider, reason):
                    continue
                self._unavailable.pop(provider, None)
                cleared.append(provider)
            if cleared:
                self._persist_cooldown_state_locked()
        return tuple(sorted(cleared))

    def legacy_cooldown_providers(self) -> set[str]:
        """Return active schema-1/2 cooldowns whose cause was not persisted."""
        now = time.time()
        with self._lock:
            return {
                provider
                for provider, kind in self._blocked_kind.items()
                if kind == "legacy" and self._blocked_until.get(provider, 0.0) > now
            }

    @staticmethod
    def _quota_reset_deadline(
        provider: str, message: str, now_timestamp: float | None = None
    ) -> float:
        """Resolve semantic quota windows to a concrete UTC deadline."""
        now_timestamp = time.time() if now_timestamp is None else now_timestamp
        now_utc = datetime.fromtimestamp(now_timestamp, tz=timezone.utc)
        text = message.lower()
        if provider == "fmp" and any(
            marker in text for marker in ("limit reach", "daily limit", "429")
        ):
            # FMP documents the Basic-plan daily reset as 15:00 EST.  Treat
            # EST literally (UTC-05:00) and add five minutes of propagation
            # grace so the first post-reset task does useful work.
            est = timezone(timedelta(hours=-5))
            local_now = now_utc.astimezone(est)
            reset = local_now.replace(hour=15, minute=5, second=0, microsecond=0)
            if reset <= local_now:
                reset += timedelta(days=1)
            return reset.timestamp()
        if provider == "bls" and any(
            marker in text
            for marker in ("daily threshold", "daily limit", "daily quota")
        ):
            # BLS specifies a daily query budget but not a public reset clock.
            # Use the next US/Eastern calendar day with five minutes of grace;
            # if the upstream counter is delayed, the next response re-arms the
            # following daily window without consuming the task attempt budget.
            eastern = ZoneInfo("America/New_York")
            local_now = now_utc.astimezone(eastern)
            reset = (local_now + timedelta(days=1)).replace(
                hour=0, minute=5, second=0, microsecond=0
            )
            return reset.timestamp()
        if provider == "tiingo" and any(
            marker in text
            for marker in (
                "daily allocation",
                "daily request allocation",
                "daily limit",
                "daily quota",
            )
        ):
            # Tiingo documents a midnight EST reset for its daily request
            # allocation. Five minutes of propagation grace prevents a
            # restart exactly on the boundary from immediately re-arming the
            # same account-level 429.
            est = timezone(timedelta(hours=-5))
            local_now = now_utc.astimezone(est)
            reset = (local_now + timedelta(days=1)).replace(
                hour=0, minute=5, second=0, microsecond=0
            )
            return reset.timestamp()
        if "hourly" in text or "per hour" in text:
            return now_timestamp + 3660.0
        if "daily" in text or "per day" in text:
            reset = (now_utc + timedelta(days=1)).replace(
                hour=0, minute=5, second=0, microsecond=0
            )
            return reset.timestamp()
        return now_timestamp

    def block_quota(self, provider: str, message: str) -> None:
        with self._lock:
            limiter = self._limiters.get(provider)
        flush_claim_observations = getattr(limiter, "flush_claim_observations", None)
        if callable(flush_claim_observations):
            flush_claim_observations()
        now = time.time()
        semantic_deadline = self._quota_reset_deadline(provider, message, now)
        until = (
            semantic_deadline if semantic_deadline > now else now + self.quota_cooldown
        )
        with self._lock:
            request_window = self._request_window_locked(provider, now)
            lower_message = str(message).lower()
            quota_window = (
                "hourly"
                if "hourly" in lower_message or "per hour" in lower_message
                else "daily"
                if "daily" in lower_message
                or "per day" in lower_message
                or "limit reach" in lower_message
                else "unknown"
            )
            observed_claims = (
                int(request_window["utc_hour_claims"])
                if quota_window == "hourly"
                else int(request_window["provider_day_claims"])
                if quota_window == "daily"
                else int(self._rate_claim_totals.get(provider, 0))
            )
            self._observed_quota_limits[provider] = {
                "window": quota_window,
                "observed_claims_at_limit_response": observed_claims,
                "window_key": (
                    request_window["utc_hour_key"]
                    if quota_window == "hourly"
                    else request_window["provider_day_key"]
                    if quota_window == "daily"
                    else "session"
                ),
                "declared_cap": (
                    PROVIDER_DECLARED_HOURLY_REQUEST_CAPS.get(provider)
                    if quota_window == "hourly"
                    else PROVIDER_DECLARED_DAILY_REQUEST_CAPS.get(provider)
                    if quota_window == "daily"
                    else None
                ),
                # Other programs may share the account/IP, so this is direct
                # evidence, not a claim that the managed count equals the cap.
                "managed_claim_count_is_observational": True,
                "observed_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                "reason": str(message)[:1000],
            }
            current = self._blocked_until.get(provider, 0.0)
            if until >= current:
                self._blocked_until[provider] = until
                self._blocked_reason[provider] = str(message)[:1000]
                self._blocked_kind[provider] = "quota"
            self._persist_cooldown_state_locked()
        # The JSON checkpoint protects one archive process; the shared
        # limiter state protects every process that uses the same provider
        # account/IP.  Extend that schedule as soon as quota evidence arrives
        # so a second market/downloader cannot immediately hit the same limit.
        if limiter is not None:
            limiter.defer(max(0.0, until - now))

    def block(
        self, provider: str, seconds: float, reason: str = "transient retry backoff"
    ) -> None:
        until = time.time() + max(1.0, seconds)
        with self._lock:
            current = self._blocked_until.get(provider, 0.0)
            if until >= current:
                self._blocked_until[provider] = until
                self._blocked_reason[provider] = str(reason)[:1000]
                self._blocked_kind[provider] = "transient"
            self._persist_cooldown_state_locked()
        # `_blocked_until` is the authoritative provider cooldown.  Do not
        # also push the rate limiter minutes into the future: workers that
        # passed their first availability check just before another thread
        # observed a quota response would otherwise sleep inside limiter.wait
        # and occupy the global executor.  The worker re-checks availability
        # immediately after its normal RPS wait and defers without consuming
        # a task attempt.

    def disable(self, provider: str, reason: str) -> None:
        with self._lock:
            self._unavailable[provider] = reason[:1000]
            self._persist_cooldown_state_locked()

    def disable_route(self, provider: str, endpoint: str, reason: str) -> None:
        """Disable one subscription-restricted route without losing the provider."""
        with self._lock:
            self._route_unavailable[(provider, endpoint)] = reason[:1000]
            self._persist_cooldown_state_locked()

    def disable_domain(
        self, provider: str, endpoint: str, domain: str, reason: str
    ) -> None:
        """Disable one proven market-domain entitlement, not the whole route."""
        with self._lock:
            self._domain_unavailable[(provider, endpoint, domain)] = reason[:1000]
            self._persist_cooldown_state_locked()

    def learn_parameter_maximum(
        self, provider: str, endpoint: str, parameter: str, maximum: int
    ) -> None:
        """Persist one provider query constraint at its narrowest stable scope."""
        maximum = int(maximum)
        if maximum <= 0:
            return
        key = (str(provider), str(endpoint), str(parameter).lower())
        with self._lock:
            current = self._parameter_maximums.get(key)
            if current is None or maximum < current:
                self._parameter_maximums[key] = maximum
                self._persist_cooldown_state_locked()

    def apply_parameter_maximums(
        self, provider: str, endpoint: str, kwargs: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, int]]:
        """Apply learned caps to one provider call and report changed fields."""
        adjusted = dict(kwargs)
        applied: dict[str, int] = {}
        with self._lock:
            constraints = {
                parameter: maximum
                for (item_provider, item_endpoint, parameter), maximum in (
                    self._parameter_maximums.items()
                )
                if item_provider == provider and item_endpoint == endpoint
            }
        for parameter, maximum in constraints.items():
            raw = adjusted.get(parameter)
            if raw is None or int(raw) > maximum:
                adjusted[parameter] = maximum
                applied[parameter] = maximum
        return adjusted, applied

    def learn_omitted_parameters(
        self, provider: str, endpoint: str, parameters: Sequence[str]
    ) -> None:
        """Persist provider/endpoint fields rejected by this entitlement."""
        key = (str(provider), str(endpoint))
        learned = tuple(sorted({str(value) for value in parameters if str(value)}))
        if not learned:
            return
        with self._lock:
            current = set(self._omitted_parameters.get(key, ()))
            merged = tuple(sorted(current | set(learned)))
            if merged != self._omitted_parameters.get(key):
                self._omitted_parameters[key] = merged
                self._persist_cooldown_state_locked()

    def apply_omitted_parameters(
        self, provider: str, endpoint: str, kwargs: Mapping[str, Any]
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        """Remove only fields proven unavailable for this credential route."""
        adjusted = dict(kwargs)
        with self._lock:
            configured = self._omitted_parameters.get((provider, endpoint), ())
        applied = tuple(name for name in configured if name in adjusted)
        for name in applied:
            adjusted.pop(name, None)
        return adjusted, applied

    def clear_adaptable_limit_unavailable_domains(
        self,
    ) -> tuple[tuple[str, str, str], ...]:
        """Convert bounded-limit denials into credential query constraints."""
        cleared: list[tuple[str, str, str]] = []
        with self._lock:
            for key, reason in list(self._domain_unavailable.items()):
                provider, endpoint, _domain = key
                maximum = _adaptable_limit_maximum(provider, endpoint, reason)
                if maximum is None:
                    continue
                self._domain_unavailable.pop(key, None)
                constraint_key = (provider, endpoint, "limit")
                current = self._parameter_maximums.get(constraint_key)
                if current is None or maximum < current:
                    self._parameter_maximums[constraint_key] = maximum
                cleared.append(key)
            if cleared:
                self._persist_cooldown_state_locked()
        return tuple(sorted(cleared))

    def clear_adaptable_query_shape_unavailable_domains(
        self,
    ) -> tuple[tuple[str, str, str], ...]:
        """Replace a false domain denial with a narrower legal query shape."""
        cleared: list[tuple[str, str, str]] = []
        with self._lock:
            for key, reason in list(self._domain_unavailable.items()):
                provider, endpoint, _domain = key
                omitted = _adaptable_omitted_parameters(provider, endpoint, reason)
                if not omitted:
                    continue
                self._domain_unavailable.pop(key, None)
                constraint_key = (provider, endpoint)
                current = set(self._omitted_parameters.get(constraint_key, ()))
                self._omitted_parameters[constraint_key] = tuple(
                    sorted(current | set(omitted))
                )
                cleared.append(key)
            if cleared:
                self._persist_cooldown_state_locked()
        return tuple(sorted(cleared))

    def unavailable(self) -> dict[str, str]:
        with self._lock:
            return dict(self._unavailable)

    def unavailable_routes(self) -> dict[tuple[str, str], str]:
        with self._lock:
            return dict(self._route_unavailable)

    def unavailable_domains(self) -> dict[tuple[str, str, str], str]:
        with self._lock:
            return dict(self._domain_unavailable)

    def parameter_maximums(self) -> dict[tuple[str, str, str], int]:
        with self._lock:
            return dict(self._parameter_maximums)

    def omitted_parameters(self) -> dict[tuple[str, str], tuple[str, ...]]:
        with self._lock:
            return dict(self._omitted_parameters)


_SEC_HTTP_PATCH_LOCK = threading.Lock()
_SEC_HTTP_PACING_LOCAL = threading.local()
_SEC_HTTP_RUNTIME: ProviderRuntime | None = None
_SEC_HTTP_PATCHED = False
_SEC_HTTP_ORIGINAL_AMAKE_REQUEST: Any | None = None
_SEC_HTTP_ORIGINAL_MAKE_REQUEST: Any | None = None
_SEC_HTTP_PACED_AMAKE_REQUEST: Any | None = None
_SEC_HTTP_PACED_MAKE_REQUEST: Any | None = None

_YFINANCE_HTTP_PATCH_LOCK = threading.Lock()
_YFINANCE_HTTP_EVIDENCE_LOCAL = threading.local()
_YFINANCE_HTTP_RUNTIME: ProviderRuntime | None = None
_YFINANCE_HTTP_PATCHED = False
_YFINANCE_HTTP_SESSION_CLASS: type[Any] | None = None
_YFINANCE_HTTP_ORIGINAL_REQUEST: Any | None = None
_YFINANCE_HTTP_PACED_REQUEST: Any | None = None

_PROVIDER_HTTP_PATCH_LOCK = threading.Lock()
_PROVIDER_HTTP_RUNTIME: ProviderRuntime | None = None
_PROVIDER_HTTP_PATCHED = False
_PROVIDER_HTTP_ORIGINAL_REQUEST: Any | None = None
_PROVIDER_HTTP_PACED_REQUEST: Any | None = None
_PROVIDER_REQUESTS_ORIGINAL_REQUEST: Any | None = None
_PROVIDER_REQUESTS_PACED_REQUEST: Any | None = None
_PROVIDER_URLLIB_ORIGINAL_OPEN: Any | None = None
_PROVIDER_URLLIB_PACED_OPEN: Any | None = None
_PROVIDER_HTTPX_ORIGINAL_SEND: Any | None = None
_PROVIDER_HTTPX_PACED_SEND: Any | None = None
_PROVIDER_HTTPX_ORIGINAL_ASYNC_SEND: Any | None = None
_PROVIDER_HTTPX_PACED_ASYNC_SEND: Any | None = None

# Resolve every installed provider's data hosts to one independent bucket.
# aiohttp, requests, urllib and httpx are patched below, so the policy is the
# same across all markets instead of depending on which transport a provider
# adapter happens to use. SEC and Yahoo retain their more specialized wrappers.
PROVIDER_HTTP_HOST_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("api.benzinga.com", "benzinga"),
    ("api.bls.gov", "bls"),
    ("data.bls.gov", "bls"),
    ("download.bls.gov", "bls"),
    ("www.bls.gov", "bls"),
    ("publicreporting.cftc.gov", "cftc"),
    ("evergreen.data.socrata.com", "cftc"),
    ("cftc.gov", "cftc"),
    ("api.congress.gov", "congress_gov"),
    ("www.congress.gov", "congress_gov"),
    ("www.govinfo.gov", "congress_gov"),
    ("financialmodelingprep.com", "fmp"),
    ("intrinio.com", "intrinio"),
    ("stlouisfed.org", "fred"),
    ("econdb.com", "econdb"),
    ("api.eia.gov", "eia"),
    ("eia.gov", "eia"),
    ("ir.eia.gov", "eia"),
    ("www.eia.gov", "eia"),
    ("markets.newyorkfed.org", "federal_reserve"),
    ("federalreserve.gov", "federal_reserve"),
    ("newyorkfed.org", "federal_reserve"),
    ("frbsf.org", "federal_reserve"),
    ("philadelphiafed.org", "federal_reserve"),
    ("api.data.gov", "government_us"),
    ("apps.fas.usda.gov", "government_us"),
    ("esmis.nal.usda.gov", "government_us"),
    ("treasurydirect.gov", "government_us"),
    ("api.imf.org", "imf"),
    ("hub.arcgis.com", "imf"),
    ("portwatch.imf.org", "imf"),
    ("services9.arcgis.com", "imf"),
    ("sdmx.oecd.org", "oecd"),
    ("data-explorer.oecd.org", "oecd"),
    ("api.tiingo.com", "tiingo"),
    ("api.tradingeconomics.com", "tradingeconomics"),
)
HTTP_BOUNDARY_PACED_PROVIDERS = frozenset(
    provider for _, provider in PROVIDER_HTTP_HOST_SUFFIXES
)


class ProviderDeferredError(RuntimeError):
    """A request was withdrawn because another thread started a cooldown."""


class BlsLabstatUnsupportedError(RuntimeError):
    """LABSTAT cannot represent this exact request; the API may be used."""


class SecCompanyfactsCacheInvalidError(RuntimeError):
    """A child process found a missing or invalid main-process SEC raw cache."""


class ProviderResponseShapeError(RuntimeError):
    """An upstream payload did not match the provider adapter's shape contract."""


def _is_sec_http_url(url: Any) -> bool:
    """Return whether a child HTTP call is governed by SEC fair access."""
    from urllib.parse import urlsplit

    try:
        host = str(urlsplit(str(url)).hostname or "").lower().rstrip(".")
    except (TypeError, ValueError):
        return False
    return host == "sec.gov" or host.endswith(".sec.gov")


def _is_yahoo_http_url(url: Any) -> bool:
    """Return whether a real HTTP call consumes Yahoo Finance capacity."""
    from urllib.parse import urlsplit

    try:
        host = str(urlsplit(str(url)).hostname or "").lower().rstrip(".")
    except (TypeError, ValueError):
        return False
    return host == "yahoo.com" or host.endswith(".yahoo.com")


def _provider_for_http_url(url: Any) -> str | None:
    """Resolve an aiohttp URL to its independently limited provider."""
    from urllib.parse import urlsplit

    try:
        host = str(urlsplit(str(url)).hostname or "").lower().rstrip(".")
    except (TypeError, ValueError):
        return None
    for suffix, provider in PROVIDER_HTTP_HOST_SUFFIXES:
        if host == suffix or host.endswith(f".{suffix}"):
            return provider
    return None


def _is_bls_labstat_bulk_url(url: Any) -> bool:
    """Return whether a URL is the quota-free official LABSTAT file host."""
    from urllib.parse import urlsplit

    try:
        host = str(urlsplit(str(url)).hostname or "").lower().rstrip(".")
        path = str(urlsplit(str(url)).path or "")
    except (TypeError, ValueError):
        return False
    return host == "download.bls.gov" and path.startswith("/pub/time.series/")


def _is_intrinio_large_page_or_bulk_url(url: Any) -> bool:
    """Return whether Intrinio's documented low-rate route bucket applies."""
    from urllib.parse import parse_qs, urlsplit

    try:
        parsed = urlsplit(str(url))
        path = parsed.path.lower()
        query = parse_qs(parsed.query)
    except (TypeError, ValueError):
        return False
    if "bulk" in path:
        return True
    for raw_value in query.get("page_size", ()):  # one value in normal URLs
        try:
            if int(raw_value) > 100:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _wait_sec_http_limiter(limiter: Any) -> None:
    """Claim one real SEC request start and honor a concurrent cooldown."""
    runtime = _SEC_HTTP_RUNTIME
    if runtime is not None:
        available, reason = runtime.availability("sec")
        if not available:
            raise ProviderDeferredError(f"__archive_provider_deferred__: SEC {reason}")
    explicit_wait = getattr(limiter, "wait_explicit_boundary", None)
    if callable(explicit_wait):
        explicit_wait()
    else:
        limiter.wait()
    if runtime is not None:
        available, reason = runtime.availability("sec")
        if not available:
            raise ProviderDeferredError(f"__archive_provider_deferred__: SEC {reason}")


def _wait_yfinance_http_limiter(limiter: Any) -> None:
    """Claim exactly one real Yahoo HTTP start and honor shared cooldowns."""
    runtime = _YFINANCE_HTTP_RUNTIME
    if runtime is not None:
        available, reason = runtime.availability("yfinance")
        if not available:
            raise ProviderDeferredError(
                f"__archive_provider_deferred__: Yahoo Finance {reason}"
            )
    limiter.wait()
    if runtime is not None:
        available, reason = runtime.availability("yfinance")
        if not available:
            raise ProviderDeferredError(
                f"__archive_provider_deferred__: Yahoo Finance {reason}"
            )


@contextmanager
def _suppress_sec_helper_pacing() -> Iterator[None]:
    """Avoid double tickets when a direct workaround paces every child call."""
    depth = int(getattr(_SEC_HTTP_PACING_LOCAL, "suppress_depth", 0))
    _SEC_HTTP_PACING_LOCAL.suppress_depth = depth + 1
    try:
        yield
    finally:
        _SEC_HTTP_PACING_LOCAL.suppress_depth = depth


def _make_sec_async_request_wrapper(original: Any) -> Any:
    async def paced(url: Any, *args: Any, **kwargs: Any) -> Any:
        runtime = _SEC_HTTP_RUNTIME
        suppressed = int(getattr(_SEC_HTTP_PACING_LOCAL, "suppress_depth", 0)) > 0
        if runtime is not None and not suppressed and _is_sec_http_url(url):
            _wait_sec_http_limiter(runtime.limiter("sec"))
        return await original(url, *args, **kwargs)

    return paced


def _make_sec_sync_request_wrapper(original: Any) -> Any:
    def paced(url: Any, *args: Any, **kwargs: Any) -> Any:
        runtime = _SEC_HTTP_RUNTIME
        suppressed = int(getattr(_SEC_HTTP_PACING_LOCAL, "suppress_depth", 0)) > 0
        if runtime is not None and not suppressed and _is_sec_http_url(url):
            _wait_sec_http_limiter(runtime.limiter("sec"))
        return original(url, *args, **kwargs)

    return paced


def _make_yfinance_request_wrapper(original: Any) -> Any:
    """Wrap yfinance's low-level session boundary, including crumb retries."""

    def paced(session: Any, method: Any, url: Any, *args: Any, **kwargs: Any) -> Any:
        runtime = _YFINANCE_HTTP_RUNTIME
        if runtime is not None and _is_yahoo_http_url(url):
            _wait_yfinance_http_limiter(runtime.limiter("yfinance"))
        response = original(session, method, url, *args, **kwargs)
        if _is_yahoo_http_url(url):
            _record_yfinance_http_evidence(url, response)
        return response

    return paced


def _begin_yfinance_http_evidence() -> None:
    """Start one worker-attempt transport evidence window."""
    _YFINANCE_HTTP_EVIDENCE_LOCAL.events = []


def _is_yahoo_data_url(url: Any) -> bool:
    """Exclude cookie/crumb bootstrap calls from dataset response evidence."""
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(str(url))
    except (TypeError, ValueError):
        return False
    path = parsed.path.rstrip("/").lower()
    if not _is_yahoo_http_url(url):
        return False
    return path not in {"", "/v1/test/getcrumb"} and parsed.hostname != "fc.yahoo.com"


def _record_yfinance_http_evidence(url: Any, response: Any) -> None:
    """Record a data response without interfering with yfinance crumb retries."""
    events = getattr(_YFINANCE_HTTP_EVIDENCE_LOCAL, "events", None)
    if not isinstance(events, list) or not _is_yahoo_data_url(url):
        return
    from urllib.parse import urlsplit

    try:
        status = int(getattr(response, "status_code", 0) or 0)
    except (TypeError, ValueError):
        status = 0
    if status <= 0:
        return
    parsed = urlsplit(str(url))
    detail = ""
    if status >= 400:
        try:
            payload = response.json()
            detail = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        except Exception:  # curl_cffi/requests response bodies are heterogeneous.
            try:
                detail = str(response.text)
            except Exception:
                detail = ""
    events.append(
        {
            # Ignore query parameters (including crumb material) and host
            # switching between query1/query2. The path identifies the same
            # logical request across yfinance's automatic credential refresh.
            "key": parsed.path,
            "status": status,
            "detail": detail[:500],
        }
    )


def _consume_yfinance_transport_failure() -> str | None:
    """Return an unresolved retryable Yahoo data response, then clear state.

    A later 2xx/404 response for the same logical path proves yfinance's crumb
    retry recovered. A 401/403/429/5xx that remains last for any path means a
    multi-module response may be partial, so neither success nor empty is safe
    to persist.
    """
    raw_events = getattr(_YFINANCE_HTTP_EVIDENCE_LOCAL, "events", [])
    _YFINANCE_HTTP_EVIDENCE_LOCAL.events = []
    unresolved: dict[str, dict[str, Any]] = {}
    for event in raw_events if isinstance(raw_events, list) else []:
        status = int(event.get("status") or 0)
        key = str(event.get("key") or "")
        if status in {401, 403, 408, 429} or status >= 500:
            unresolved[key] = event
        else:
            unresolved.pop(key, None)
    if not unresolved:
        return None
    event = list(unresolved.values())[-1]
    detail = re.sub(
        r"(?i)(crumb[\"'=:\s]+)[^&\s\"']+",
        r"\1<redacted>",
        str(event.get("detail") or ""),
    )
    return (
        "__archive_yfinance_transport__: "
        f"HTTP {int(event.get('status') or 0)} for {event.get('key')}: {detail[:500]}"
    )


def _wait_provider_http_boundary(provider: str, url: Any) -> None:
    """Claim exactly one real HTTP start for a non-SEC/Yahoo provider."""
    if provider == "bls" and _is_bls_labstat_bulk_url(url):
        # LABSTAT flat files are not BLS Public Data API queries and do not
        # consume its daily account allocation. They are already bounded by
        # _BLS_LABSTAT_BUILD_SEMAPHORE; applying a persisted API cooldown here
        # would defeat the very fallback that is meant to bypass that quota.
        return
    runtime = _PROVIDER_HTTP_RUNTIME
    if runtime is None:
        return
    available, reason = runtime.availability(provider)
    if not available:
        raise ProviderDeferredError(
            f"__archive_provider_deferred__: {provider} {reason}"
        )
    if provider == "intrinio" and _is_intrinio_large_page_or_bulk_url(url):
        runtime.limiter("intrinio_large_page").wait()
    limiter = runtime.limiter(provider)
    boundary_wait = getattr(limiter, "wait_at_http_boundary", None)
    if callable(boundary_wait):
        boundary_wait()
    else:
        limiter.wait()
    available, reason = runtime.availability(provider)
    if not available:
        raise ProviderDeferredError(
            f"__archive_provider_deferred__: {provider} {reason}"
        )


def _make_provider_aiohttp_request_wrapper(original: Any) -> Any:
    """Pace known providers at aiohttp's real request boundary."""

    async def paced(
        session: Any, method: Any, url: Any, *args: Any, **kwargs: Any
    ) -> Any:
        provider = _provider_for_http_url(url)
        if provider is not None:
            _wait_provider_http_boundary(provider, url)
        return await original(session, method, url, *args, **kwargs)

    return paced


def _make_provider_requests_request_wrapper(original: Any) -> Any:
    """Pace known providers at requests.Session.request."""

    def paced(session: Any, method: Any, url: Any, *args: Any, **kwargs: Any) -> Any:
        provider = _provider_for_http_url(url)
        if provider is not None:
            _wait_provider_http_boundary(provider, url)
        return original(session, method, url, *args, **kwargs)

    return paced


def _make_provider_urllib_open_wrapper(original: Any) -> Any:
    """Pace urllib calls even when provider modules imported urlopen aliases."""

    def paced(opener: Any, fullurl: Any, *args: Any, **kwargs: Any) -> Any:
        url = getattr(fullurl, "full_url", fullurl)
        provider = _provider_for_http_url(url)
        if provider is not None:
            _wait_provider_http_boundary(provider, url)
        return original(opener, fullurl, *args, **kwargs)

    return paced


def _make_provider_httpx_send_wrapper(original: Any) -> Any:
    """Pace synchronous httpx clients at the prepared-request boundary."""

    def paced(client: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        url = getattr(request, "url", "")
        provider = _provider_for_http_url(url)
        if provider is not None:
            _wait_provider_http_boundary(provider, url)
        return original(client, request, *args, **kwargs)

    return paced


def _make_provider_httpx_async_send_wrapper(original: Any) -> Any:
    """Pace asynchronous httpx clients at the prepared-request boundary."""

    async def paced(client: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        url = getattr(request, "url", "")
        provider = _provider_for_http_url(url)
        if provider is not None:
            _wait_provider_http_boundary(provider, url)
        return await original(client, request, *args, **kwargs)

    return paced


def _install_sec_http_limiter(runtime: ProviderRuntime) -> None:
    """Pace every OpenBB SEC child request, including gathered Form 4 calls.

    OpenBB's public command is an outer unit of work, not an HTTP request: one
    command can gather eight or more SEC documents.  Patch the shared request
    helpers and any already-imported aliases so the official 10 req/s ceiling
    is enforced exactly where each network request starts.
    """
    import sys
    import openbb_core.provider.utils.helpers as core_helpers

    global _SEC_HTTP_RUNTIME
    global _SEC_HTTP_PATCHED
    global _SEC_HTTP_ORIGINAL_AMAKE_REQUEST
    global _SEC_HTTP_ORIGINAL_MAKE_REQUEST
    global _SEC_HTTP_PACED_AMAKE_REQUEST
    global _SEC_HTTP_PACED_MAKE_REQUEST

    with _SEC_HTTP_PATCH_LOCK:
        _SEC_HTTP_RUNTIME = runtime
        if not _SEC_HTTP_PATCHED:
            _SEC_HTTP_ORIGINAL_AMAKE_REQUEST = core_helpers.amake_request
            _SEC_HTTP_ORIGINAL_MAKE_REQUEST = core_helpers.make_request
            _SEC_HTTP_PACED_AMAKE_REQUEST = _make_sec_async_request_wrapper(
                _SEC_HTTP_ORIGINAL_AMAKE_REQUEST
            )
            _SEC_HTTP_PACED_MAKE_REQUEST = _make_sec_sync_request_wrapper(
                _SEC_HTTP_ORIGINAL_MAKE_REQUEST
            )
            core_helpers.amake_request = _SEC_HTTP_PACED_AMAKE_REQUEST
            core_helpers.make_request = _SEC_HTTP_PACED_MAKE_REQUEST
            _SEC_HTTP_PATCHED = True

        # Some SEC utility modules import the helpers at module load time.  A
        # package-wide identity replacement covers those aliases without
        # touching unrelated provider functions or non-SEC URLs.
        for name, module in tuple(sys.modules.items()):
            if not name.startswith("openbb_sec") or module is None:
                continue
            if (
                getattr(module, "amake_request", None)
                is _SEC_HTTP_ORIGINAL_AMAKE_REQUEST
            ):
                setattr(module, "amake_request", _SEC_HTTP_PACED_AMAKE_REQUEST)
            if getattr(module, "make_request", None) is _SEC_HTTP_ORIGINAL_MAKE_REQUEST:
                setattr(module, "make_request", _SEC_HTTP_PACED_MAKE_REQUEST)


def _install_yfinance_http_limiter(runtime: ProviderRuntime) -> None:
    """Pace real Yahoo HTTP starts instead of outer OpenBB commands.

    A yfinance command can fetch a cookie, mint or refresh a crumb, retry a
    rejected query, and then fetch the requested resource.  Counting that as
    one request allowed the archive to exceed the operator's 10 req/s Yahoo
    contract even though outer-task telemetry looked correct.  Patch the
    concrete yfinance session class so every Yahoo-host request shares the one
    provider limiter; unrelated curl/requests traffic is left untouched.
    """
    from yfinance.data import YfData

    global _YFINANCE_HTTP_RUNTIME
    global _YFINANCE_HTTP_PATCHED
    global _YFINANCE_HTTP_SESSION_CLASS
    global _YFINANCE_HTTP_ORIGINAL_REQUEST
    global _YFINANCE_HTTP_PACED_REQUEST

    with _YFINANCE_HTTP_PATCH_LOCK:
        _YFINANCE_HTTP_RUNTIME = runtime
        session_class = type(YfData()._session)
        if _YFINANCE_HTTP_PATCHED:
            if session_class is not _YFINANCE_HTTP_SESSION_CLASS:
                raise RuntimeError(
                    "yfinance changed HTTP session classes after pacing was installed"
                )
            return
        _YFINANCE_HTTP_SESSION_CLASS = session_class
        _YFINANCE_HTTP_ORIGINAL_REQUEST = session_class.request
        _YFINANCE_HTTP_PACED_REQUEST = _make_yfinance_request_wrapper(
            _YFINANCE_HTTP_ORIGINAL_REQUEST
        )
        session_class.request = _YFINANCE_HTTP_PACED_REQUEST
        _YFINANCE_HTTP_PATCHED = True


def _install_provider_http_limiter(runtime: ProviderRuntime) -> None:
    """Install transport-independent request-start governors once."""
    import aiohttp
    import httpx
    import requests
    import urllib.request

    global _PROVIDER_HTTP_RUNTIME
    global _PROVIDER_HTTP_PATCHED
    global _PROVIDER_HTTP_ORIGINAL_REQUEST
    global _PROVIDER_HTTP_PACED_REQUEST
    global _PROVIDER_REQUESTS_ORIGINAL_REQUEST
    global _PROVIDER_REQUESTS_PACED_REQUEST
    global _PROVIDER_URLLIB_ORIGINAL_OPEN
    global _PROVIDER_URLLIB_PACED_OPEN
    global _PROVIDER_HTTPX_ORIGINAL_SEND
    global _PROVIDER_HTTPX_PACED_SEND
    global _PROVIDER_HTTPX_ORIGINAL_ASYNC_SEND
    global _PROVIDER_HTTPX_PACED_ASYNC_SEND

    with _PROVIDER_HTTP_PATCH_LOCK:
        _PROVIDER_HTTP_RUNTIME = runtime
        if _PROVIDER_HTTP_PATCHED:
            return
        _PROVIDER_HTTP_ORIGINAL_REQUEST = aiohttp.ClientSession._request
        _PROVIDER_HTTP_PACED_REQUEST = _make_provider_aiohttp_request_wrapper(
            _PROVIDER_HTTP_ORIGINAL_REQUEST
        )
        aiohttp.ClientSession._request = _PROVIDER_HTTP_PACED_REQUEST

        _PROVIDER_REQUESTS_ORIGINAL_REQUEST = requests.Session.request
        _PROVIDER_REQUESTS_PACED_REQUEST = _make_provider_requests_request_wrapper(
            _PROVIDER_REQUESTS_ORIGINAL_REQUEST
        )
        requests.Session.request = _PROVIDER_REQUESTS_PACED_REQUEST

        _PROVIDER_URLLIB_ORIGINAL_OPEN = urllib.request.OpenerDirector.open
        _PROVIDER_URLLIB_PACED_OPEN = _make_provider_urllib_open_wrapper(
            _PROVIDER_URLLIB_ORIGINAL_OPEN
        )
        urllib.request.OpenerDirector.open = _PROVIDER_URLLIB_PACED_OPEN

        _PROVIDER_HTTPX_ORIGINAL_SEND = httpx.Client.send
        _PROVIDER_HTTPX_PACED_SEND = _make_provider_httpx_send_wrapper(
            _PROVIDER_HTTPX_ORIGINAL_SEND
        )
        httpx.Client.send = _PROVIDER_HTTPX_PACED_SEND

        _PROVIDER_HTTPX_ORIGINAL_ASYNC_SEND = httpx.AsyncClient.send
        _PROVIDER_HTTPX_PACED_ASYNC_SEND = _make_provider_httpx_async_send_wrapper(
            _PROVIDER_HTTPX_ORIGINAL_ASYNC_SEND
        )
        httpx.AsyncClient.send = _PROVIDER_HTTPX_PACED_ASYNC_SEND
        _PROVIDER_HTTP_PATCHED = True


def _resolve_callable(obb: Any, endpoint: str) -> Any:
    obj = obb
    for component in endpoint.split("."):
        obj = getattr(obj, component)
    return obj


def _normalize_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "item"):
        try:
            return _normalize_scalar(value.item())
        except Exception:
            pass
    if isinstance(value, (dict, list, tuple, set)):
        return _canonical_json(value)
    return str(value)


def _provider_result_rows(transformed: Any) -> list[Any]:
    """Normalize direct provider fetcher output to observation rows.

    OpenBB provider versions are not uniform: ``transform_data`` may return a
    plain list, one model, or ``AnnotatedResult(result=[...], metadata=...)``.
    Routed commands expose ``OBBject.results`` and are handled elsewhere.  All
    archive workarounds use this one boundary so no market can accidentally
    persist a one-row ``result``/``metadata`` wrapper or iterate BaseModel
    field tuples.
    """
    values = getattr(transformed, "result", transformed)
    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    if isinstance(values, Mapping) or hasattr(values, "model_dump"):
        return [values]
    try:
        return list(values)
    except TypeError:
        return [values]


def normalize_records(
    result: Any,
    *,
    metadata_only: bool,
    show_progress: bool = False,
    progress_desc: str = "openbb:normalize",
) -> list[dict[str, Any]]:
    values = getattr(result, "results", result)
    if values is None:
        return []
    # Pydantic models are iterable as ``(field, value)`` pairs.  Treat a
    # singleton model as one record before the generic iterable fallback, or a
    # result such as ``SecCikMapData(cik=...)`` is incorrectly persisted as a
    # stringified tuple in a generic ``value`` column.
    if hasattr(values, "model_dump"):
        values = [values]
    elif isinstance(values, Mapping):
        values = [values]
    elif not isinstance(values, (list, tuple)):
        try:
            values = list(values)
        except TypeError:
            values = [values]

    records: list[dict[str, Any]] = []
    progress = tqdm(
        values,
        total=len(values),
        desc=progress_desc[:64],
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for value in progress:
            if hasattr(value, "model_dump"):
                raw = value.model_dump(mode="json")
            elif isinstance(value, Mapping):
                raw = dict(value)
            else:
                raw = {"value": value}
            record: dict[str, Any] = {}
            for key, item in raw.items():
                key_text = str(key)
                if metadata_only and key_text.strip().lower() in DOCUMENT_BODY_KEYS:
                    continue
                record[key_text] = _normalize_scalar(item)
            records.append(record)
    finally:
        progress.close()
    return records


def _normalize_bls_search_result(
    result: Any, *, metadata_only: bool, show_progress: bool = False
) -> list[dict[str, Any]]:
    """Preserve both the BLS series catalog and its complete code maps."""
    records = normalize_records(
        result,
        metadata_only=metadata_only,
        show_progress=show_progress,
        progress_desc="bls:normalize series catalog",
    )
    type_progress = tqdm(
        records,
        total=len(records),
        desc="bls:tag series catalog",
        unit="series",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for record in type_progress:
            record["_bls_record_type"] = "series"
    finally:
        type_progress.close()

    extra = getattr(result, "extra", None)
    metadata = extra.get("results_metadata", {}) if isinstance(extra, Mapping) else {}
    if not isinstance(metadata, Mapping):
        return records
    code_map_total = sum(
        len(code_map)
        for field_maps in metadata.values()
        if isinstance(field_maps, Mapping)
        for code_map in field_maps.values()
        if isinstance(code_map, Mapping)
    )
    progress = tqdm(
        total=code_map_total,
        desc="bls:normalize code maps",
        unit="code",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for survey, field_maps in metadata.items():
            if not isinstance(field_maps, Mapping):
                continue
            for field_name, code_map in field_maps.items():
                if not isinstance(code_map, Mapping):
                    continue
                for code, label in code_map.items():
                    records.append(
                        {
                            "_bls_record_type": "code_map",
                            "survey_name": str(survey),
                            "code_field": str(field_name),
                            "code": str(code),
                            "label": _normalize_scalar(label),
                        }
                    )
                    progress.update(1)
    finally:
        progress.close()
    return records


def _repair_xlsx_core_datetimes(content: bytes) -> bytes:
    """Repair malformed one-digit XLSX core-property hours without touching sheet data."""
    from io import BytesIO
    from zipfile import ZIP_DEFLATED, ZipFile

    source = BytesIO(content)
    target = BytesIO()
    with (
        ZipFile(source, "r") as input_zip,
        ZipFile(target, "w", compression=ZIP_DEFLATED) as output_zip,
    ):
        for info in input_zip.infolist():
            data = input_zip.read(info.filename)
            if info.filename == "docProps/core.xml":
                text = data.decode("utf-8")
                text = re.sub(r"T\s+(\d):", lambda match: f"T0{match.group(1)}:", text)
                data = text.encode("utf-8")
            output_zip.writestr(info, data)
    return target.getvalue()


def _fetch_inflation_expectations_workaround(kwargs: Mapping[str, Any]) -> list[Any]:
    """Use the OpenBB provider model after repairing malformed workbook metadata."""
    from openbb_federal_reserve.models.inflation_expectations import (
        FederalReserveInflationExpectationsFetcher,
        download_inflation_excel,
    )

    query = FederalReserveInflationExpectationsFetcher.transform_query(dict(kwargs))
    content = _repair_xlsx_core_datetimes(download_inflation_excel())
    return _provider_result_rows(
        FederalReserveInflationExpectationsFetcher.transform_data(
            query, {"file": content}
        )
    )


def _fetch_yfinance_etf_info_workaround(
    kwargs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Preserve valid ETF metadata when Yahoo omits ``longName``.

    The OpenBB standard model declares ``name`` as nullable but required.  A
    valid Yahoo ETF response without ``longName`` therefore fails the entire
    provider transform despite carrying the rest of the fund metadata.  Fetch
    through the provider's normal extraction layer, use the requested symbol
    as a traceable fallback name, and retain an explicit inference flag.
    """
    import asyncio

    from openbb_yfinance.models.etf_info import YFinanceEtfInfoFetcher

    query = YFinanceEtfInfoFetcher.transform_query(dict(kwargs))
    raw = asyncio.run(YFinanceEtfInfoFetcher.aextract_data(query, {}))
    requested_symbols = [item.strip() for item in query.symbol.split(",")]
    inferred: list[bool] = []
    prepared: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        record = dict(item)
        missing_name = not str(record.get("longName") or "").strip()
        if missing_name:
            fallback = str(record.get("symbol") or "").strip()
            if not fallback and index < len(requested_symbols):
                fallback = requested_symbols[index]
            record["longName"] = fallback
        prepared.append(record)
        inferred.append(missing_name)

    transformed = _provider_result_rows(
        YFinanceEtfInfoFetcher.transform_data(query, prepared)
    )
    records: list[dict[str, Any]] = []
    for model, was_inferred in zip(transformed, inferred, strict=True):
        record = model.model_dump(mode="json")
        record["name_inferred_from_symbol"] = was_inferred
        records.append(record)
    return records


def _fetch_econdb_country_profile_workaround(
    kwargs: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Preserve profiles when EconDB omits the GDP column used for sorting.

    OpenBB's EconDB transform sorts unconditionally by ``GDP ($B USD)``.
    Countries such as Mongolia return other useful indicators without that
    column, so reuse the provider extraction layer and normalize its aliases
    without applying the invalid sort.
    """
    import asyncio

    from openbb_econdb.models.country_profile import (
        EconDbCountryProfileData,
        EconDbCountryProfileFetcher,
    )

    query = EconDbCountryProfileFetcher.transform_query(dict(kwargs))
    raw = asyncio.run(EconDbCountryProfileFetcher.aextract_data(query, {}))
    aliases = {
        alias: field for field, alias in EconDbCountryProfileData.__alias_dict__.items()
    }
    records: list[dict[str, Any]] = []
    for item in raw:
        normalized = {
            aliases.get(key, key): _normalize_scalar(value)
            for key, value in item.items()
        }
        date_value = normalized.pop("date", None)
        record = EconDbCountryProfileData.model_validate(normalized).model_dump()
        if date_value is not None:
            record["date"] = date_value
        records.append(record)
    return records


def _fetch_econdb_yield_curve_archive_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Flatten one complete EconDB country response in linear time.

    EconDB returns every observation for every requested maturity in one HTTP
    response.  OpenBB's generic transform then finds the nearest source date by
    subtracting the complete index once for every requested archive date.  For
    a 2000+ daily archive that is quadratic work and can occupy a worker for
    many minutes after its only HTTP request has already completed.  Preserve
    the provider extraction and validation layers, but retain each native
    observation in the requested boundary directly.
    """
    import asyncio
    from urllib.parse import urlencode

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_econdb.models.yield_curve import (
        EconDbYieldCurveData,
        EconDbYieldCurveFetcher,
    )
    from openbb_econdb.utils import helpers
    from openbb_econdb.utils.yield_curves import COUNTRIES_DICT

    raw_dates = [
        item.strip()
        for item in str(kwargs.get("date") or "").split(",")
        if item.strip()
    ]
    if not raw_dates:
        raise ValueError("EconDB yield-curve archive requires an explicit date grid")
    start_date = min(raw_dates)[:10]
    end_date = max(raw_dates)[:10]
    country = str(kwargs.get("country") or "united_states")
    if "," in country:
        raise ValueError("EconDB yield-curve archive requires exactly one country")

    # Extraction ignores date and always returns the full series.  Supplying a
    # single date keeps the Pydantic query compact without changing upstream
    # behavior; filtering is performed below against the native observations.
    query_kwargs = dict(kwargs)
    query_kwargs["date"] = end_date
    query_kwargs["use_cache"] = False
    EconDbYieldCurveFetcher.transform_query(query_kwargs)
    credential = getattr(obb.user.credentials, "econdb_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    token = str(credential or "")
    if not token:
        # EconDB no longer issues a new anonymous key from create_token, but a
        # previously issued cache entry can remain valid. Read it once under a
        # process-wide lock so dozens of country workers do not open/close the
        # same aiohttp SQLite cache from different event loops.
        global _ECONDB_CACHED_TOKEN  # noqa: PLW0603
        with _ECONDB_TOKEN_LOCK:
            if _ECONDB_CACHED_TOKEN is None:
                _ECONDB_CACHED_TOKEN = asyncio.run(helpers.create_token(use_cache=True))
            token = _ECONDB_CACHED_TOKEN
    if not token:
        raise RuntimeError(
            "EconDB API key is required; the anonymous token endpoint did not "
            "return a usable credential"
        )

    symbols = list(COUNTRIES_DICT[country])
    url = "https://www.econdb.com/api/series/?" + urlencode(
        {
            "ticker": f"[{','.join(symbols)}]",
            "page_size": 50,
            "format": "json",
            "token": token,
        }
    )

    async def extract_country() -> list[dict[str, Any]]:
        payload = await helpers.amake_request(url, timeout=60)
        if not isinstance(payload, Mapping):
            raise TypeError("EconDB yield-curve API returned a non-object")
        detail = str(payload.get("detail") or "").strip()
        if detail:
            if re.search(r"(?i)auth|credential|token|api[ -]?key|permission", detail):
                raise RuntimeError(f"EconDB authentication failed: {detail}")
            raise RuntimeError(f"EconDB API error: {detail}")
        results = payload.get("results")
        if not isinstance(results, list):
            raise TypeError("EconDB yield-curve API returned invalid results")
        return [dict(item) for item in results if isinstance(item, Mapping)]

    try:
        country_rows = asyncio.run(extract_country())
    except Exception as exc:
        redacted = str(exc)
        if token:
            redacted = redacted.replace(token, "<redacted>")
        redacted = re.sub(
            r"([?&](?:token|api_key)=)[^&\s]+",
            r"\1<redacted>",
            redacted,
            flags=re.IGNORECASE,
        )
        raise RuntimeError(f"EconDB yield-curve request failed: {redacted}") from exc

    if not country_rows:
        raise EmptyDataError(f"No EconDB yield-curve data returned for {country}")

    maturity_map = COUNTRIES_DICT[country]
    estimated = sum(
        len(item.get("data", {}).get("dates", []))
        for item in country_rows
        if isinstance(item, Mapping) and isinstance(item.get("data"), Mapping)
    )
    progress = tqdm(
        total=estimated,
        desc=f"econdb:yield_curve {country}"[:64],
        unit="observation",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    records: dict[tuple[str, str], dict[str, Any]] = {}

    def maturity_years(value: str) -> float:
        period, unit = value.split("_", 1)
        if period == "long" and unit == "term":
            return 30.0
        if period == "year":
            return float(unit)
        return float(unit) / 12.0

    try:
        for item in country_rows:
            if not isinstance(item, Mapping):
                continue
            maturity = maturity_map.get(str(item.get("ticker") or ""))
            data = item.get("data")
            if maturity is None or not isinstance(data, Mapping):
                continue
            dates = data.get("dates") or []
            values = data.get("values") or []
            for observed_date, raw_rate in zip(dates, values):
                progress.update(1)
                date_text = str(observed_date)[:10]
                if not (start_date <= date_text <= end_date):
                    continue
                if raw_rate is None or str(raw_rate).strip() == "":
                    continue
                try:
                    rate = float(raw_rate) / 100.0
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(rate):
                    continue
                record = EconDbYieldCurveData.model_validate(
                    {
                        "date": date_text,
                        "maturity": maturity,
                        "rate": rate,
                        "country": country,
                        "maturity_years": maturity_years(maturity),
                    }
                ).model_dump(mode="json")
                records[(date_text, maturity)] = record
        progress.set_postfix(rows=len(records), refresh=False)
    finally:
        progress.close()

    if not records:
        raise EmptyDataError(
            f"No EconDB yield-curve observations for {country} "
            f"between {start_date} and {end_date}"
        )
    return [records[key] for key in sorted(records)]


def _fetch_econdb_indicators_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    page_limiter: Any | None = None,
    *,
    show_progress: bool = False,
) -> Any:
    """Preserve valid EconDB series rejected by inconsistent static metadata.

    OpenBB's EconDB units map contains a small number of legitimate symbols
    with a JSON null value, while its economic-indicators transform
    unconditionally calls ``units.replace(...)``. Normalize only those null
    entries to the transform's existing empty-unit representation.

    A second upstream inconsistency marks some catalog series (for example,
    ``CREDEA``) as country-qualified with the pseudo-country ``XM``, while the
    public query validator accepts only two-letter country codes from a
    different map.  OpenBB consequently rejects the documented exact-ticker
    escape form ``CREDEA~`` before making an HTTP request.  When, and only
    when, that exact validation error occurs, request the catalog ticker
    directly from EconDB's official series API and retain OpenBB's query and
    result transformations.
    """
    import asyncio
    from urllib.parse import urlencode

    from openbb_econdb.models.economic_indicators import (
        EconDbEconomicIndicatorsFetcher,
    )
    from openbb_econdb.utils import helpers

    raw_symbols = str(kwargs.get("symbol") or "")
    for raw_symbol in raw_symbols.split(","):
        symbol = raw_symbol.strip().upper().split("~", 1)[0]
        if symbol in helpers.UNITS and helpers.UNITS[symbol] is None:
            helpers.UNITS[symbol] = ""
    func = _resolve_callable(obb, "economy.indicators")
    try:
        return func(provider="econdb", **dict(kwargs))
    except Exception as exc:
        message = str(exc).lower()
        supplied_symbols = [
            item.strip().upper() for item in raw_symbols.split(",") if item.strip()
        ]
        if (
            "must have a two-letter country code" not in message
            or not supplied_symbols
            or any(not symbol.endswith("~") for symbol in supplied_symbols)
        ):
            raise

    query = EconDbEconomicIndicatorsFetcher.transform_query(dict(kwargs))
    exact_symbols = [symbol[:-1] for symbol in supplied_symbols]
    credential = getattr(obb.user.credentials, "econdb_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()

    token_holder = {"value": str(credential or "")}

    async def extract_exact_tickers() -> list[dict[str, Any]]:
        token = token_holder["value"]
        if not token:
            token = await helpers.create_token(use_cache=query.use_cache)
            token_holder["value"] = token
        parameters: dict[str, Any] = {
            "ticker": f"[{','.join(exact_symbols)}]",
            "format": "json",
            "token": token,
        }
        if query.start_date:
            parameters["from"] = query.start_date.isoformat()
        if query.end_date:
            parameters["to"] = query.end_date.isoformat()
        next_url: str | None = "https://www.econdb.com/api/series/?" + urlencode(
            parameters
        )
        records: list[dict[str, Any]] = []
        page_number = 0
        seen_urls: set[str] = set()
        seen_page_signatures: set[str] = set()
        progress = tqdm(
            total=None,
            desc=f"econdb:{','.join(exact_symbols)} exact ticker"[:64],
            unit="page",
            position=2,
            leave=False,
            disable=not show_progress,
        )
        try:
            while next_url:
                if next_url in seen_urls:
                    raise RuntimeError(
                        f"EconDB exact-ticker pagination cycle at page {page_number}"
                    )
                seen_urls.add(next_url)
                if page_number > 0 and page_limiter is not None:
                    page_limiter.wait()
                payload = await helpers.amake_request(next_url, timeout=60)
                if not isinstance(payload, Mapping):
                    raise TypeError("EconDB exact-ticker API returned a non-object")
                page = payload.get("results") or []
                if not isinstance(page, list):
                    raise TypeError("EconDB exact-ticker API returned invalid results")
                page_signature = _raw_page_signature(page)
                if page_signature in seen_page_signatures:
                    raise RuntimeError(
                        "EconDB exact-ticker pagination repeated page content"
                    )
                seen_page_signatures.add(page_signature)
                records.extend(dict(item) for item in page if isinstance(item, Mapping))
                page_number += 1
                progress.update(1)
                progress.set_postfix(rows=len(records), refresh=False)
                raw_next = payload.get("next")
                next_url = str(raw_next) if raw_next else None
        finally:
            progress.close()
        return records

    try:
        raw = asyncio.run(extract_exact_tickers())
    except Exception as exc:
        # Async HTTP exceptions often include the full request URL. Never let
        # a configured or temporary token reach the manifest error column.
        redacted = str(exc)
        if token_holder["value"]:
            redacted = redacted.replace(token_holder["value"], "<redacted>")
        redacted = re.sub(
            r"([?&](?:token|api_key)=)[^&\s]+",
            r"\1<redacted>",
            redacted,
            flags=re.IGNORECASE,
        )
        raise RuntimeError(f"EconDB exact-ticker request failed: {redacted}") from exc
    if not raw:
        from openbb_core.provider.utils.errors import EmptyDataError

        raise EmptyDataError(
            f"No EconDB data found for exact ticker(s): {','.join(exact_symbols)}"
        )
    transformed = EconDbEconomicIndicatorsFetcher.transform_data(query, raw)
    # Direct provider fetchers return AnnotatedResult.result, while routed
    # OpenBB commands expose OBBject.results. Return the observation list so
    # the archive writer persists one Parquet row per date instead of one
    # JSON-encoded wrapper row.
    return _provider_result_rows(transformed)


def _un_comtrade_json(url: str, page_limiter: Any | None = None) -> Any:
    """Read one anonymous UN Comtrade request with conservative 429 backoff."""
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    limiter = page_limiter or UN_COMTRADE_REQUEST_LIMITER
    for attempt in range(4):
        limiter.wait()
        request = Request(
            url,
            headers={"User-Agent": "stockAgent-openbb-archive/1.0"},
        )
        try:
            with urlopen(request, timeout=60) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code != 429 or attempt == 3:
                raise
            raw_retry_after = exc.headers.get("Retry-After")
            try:
                retry_after = float(raw_retry_after or 0)
            except ValueError:
                retry_after = 0
            delay = max(retry_after, min(30.0, 3.0 * (2**attempt)))
            limiter.defer(delay)
            time.sleep(delay)
    raise RuntimeError("UN Comtrade retry loop exited unexpectedly")


@lru_cache(maxsize=4)
def _un_comtrade_area_reference(
    page_limiter: Any | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[int, str]]:
    """Load current reporter/partner codes from UN Comtrade's reference file."""
    url = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
    payload = _un_comtrade_json(url, page_limiter)
    rows = payload.get("results", []) if isinstance(payload, Mapping) else []
    by_alpha2: dict[str, list[dict[str, Any]]] = {}
    partner_names: dict[int, str] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        code = int(row.get("PartnerCode") or row.get("id") or 0)
        name = str(row.get("PartnerDesc") or row.get("text") or code).strip()
        if not bool(row.get("isGroup")):
            partner_names[code] = name
        alpha2 = str(row.get("PartnerCodeIsoAlpha2") or "").upper()
        if alpha2 and not row.get("entryExpiredDate") and not bool(row.get("isGroup")):
            by_alpha2.setdefault(alpha2, []).append(row)

    reporters: dict[str, list[dict[str, Any]]] = {}
    for alpha2, candidates in by_alpha2.items():
        # Current customs areas such as USA (code 842) and Switzerland plus
        # Liechtenstein (757) have a later effective date than the plain M49
        # country row.  This is the reporter code used by current trade data.
        reporters[alpha2] = sorted(
            candidates,
            key=lambda row: (
                str(row.get("entryEffectiveDate") or ""),
                int(row.get("PartnerCode") or row.get("id") or 0),
            ),
            reverse=True,
        )
    return reporters, partner_names


def _fetch_un_comtrade_export_destinations(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Reconstruct EconDB export destinations from its UN Comtrade source."""
    from urllib.parse import urlencode

    raw_country = str(kwargs.get("country") or "").strip().upper()
    alpha2 = "GB" if raw_country == "UK" else raw_country
    reporters, partner_names = _un_comtrade_area_reference(page_limiter)
    reporter_candidates = reporters.get(alpha2)
    if not reporter_candidates:
        raise LookupError(f"UN Comtrade has no reporter code for {raw_country}")
    endpoint = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
    end_year = min(
        date.today().year, int(str(kwargs.get("end_date") or date.today().year)[:4])
    )

    years = tuple(range(end_year, max(1999, end_year - 6), -1))
    progress = tqdm(
        total=len(reporter_candidates) * len(years),
        desc=f"comtrade:{raw_country} fallback",
        unit="query",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for reporter in reporter_candidates:
            reporter_code = int(reporter.get("PartnerCode") or reporter.get("id") or 0)
            origin_name = str(
                reporter.get("PartnerDesc") or reporter.get("text") or raw_country
            ).strip()
            for year in years:
                progress.set_postfix(year=year, reporter=reporter_code, refresh=False)
                query = urlencode(
                    {
                        "period": year,
                        "reporterCode": reporter_code,
                        "cmdCode": "TOTAL",
                        "flowCode": "X",
                        "partner2Code": 0,
                        "customsCode": "C00",
                        "motCode": 0,
                        "maxRecords": 500,
                    }
                )
                url = f"{endpoint}?{query}"
                payload = _un_comtrade_json(url, page_limiter)
                progress.update(1)
                data = payload.get("data", []) if isinstance(payload, Mapping) else []
                records: list[dict[str, Any]] = []
                for row in data:
                    if not isinstance(row, Mapping):
                        continue
                    partner_code = int(row.get("partnerCode") or 0)
                    value = float(row.get("primaryValue") or 0)
                    if partner_code == 0 or value <= 0:
                        continue
                    destination = partner_names.get(partner_code)
                    if not destination:
                        continue
                    records.append(
                        {
                            "origin_country": origin_name,
                            "destination_country": destination,
                            "value": value / 1_000_000.0,
                            "units": "Millions of USD",
                            "title": (
                                f"Top export destinations for {origin_name}, {year}"
                            ),
                            "footnote": (
                                "UN Comtrade public preview; total merchandise "
                                "exports by partner, current customs area."
                            ),
                            "reference_year": year,
                            "source": "UN Comtrade",
                            "source_url": url,
                        }
                    )
                if records:
                    return sorted(
                        records,
                        key=lambda row: float(row["value"]),
                        reverse=True,
                    )
    finally:
        progress.close()
    raise ConnectionError(
        f"UN Comtrade returned no recent export-destination data for {raw_country}"
    )


def _fetch_econdb_export_destinations_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> Any:
    """Use EconDB normally, falling back only when its UN widget is broken."""
    func = _resolve_callable(obb, "economy.export_destinations")
    try:
        return func(provider="econdb", **dict(kwargs))
    except Exception as exc:
        message = str(exc).lower()
        if "top-trade-items" not in message or "500" not in message:
            raise
        return _fetch_un_comtrade_export_destinations(
            kwargs,
            page_limiter=page_limiter,
            show_progress=show_progress,
        )


def _fetch_fmp_discovery_filings_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    page_limiter: Any | None = None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Tolerate FMP rows that omit acceptedDate and stop at the last page."""
    import asyncio

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_core.provider.utils.helpers import amake_request, get_querystring
    from openbb_fmp.models.discovery_filings import (
        FMPDiscoveryFilingsData,
        FMPDiscoveryFilingsFetcher,
    )

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for discovery filings")

    query_params = dict(kwargs)
    requested_page = int(query_params.pop("page", 0) or 0)
    for field_name in ("start_date", "end_date"):
        raw_value = query_params.get(field_name)
        if isinstance(raw_value, str):
            query_params[field_name] = date.fromisoformat(raw_value)
    query = FMPDiscoveryFilingsFetcher.transform_query(query_params)
    page_size = min(1000, max(1, int(query.limit or 1000)))
    base_url = (
        "https://financialmodelingprep.com/stable/sec-filings-search/form-type"
        if query.form_type
        else "https://financialmodelingprep.com/stable/sec-filings-financials/"
    )
    start_date = (
        query.start_date
        or (datetime.now() - timedelta(days=89 if query.form_type else 2)).date()
    )
    end_date = query.end_date or datetime.now().date()
    query.start_date = start_date
    query.end_date = end_date
    query_string = get_querystring(query.model_dump(by_alias=True), ["limit"])

    async def _extract() -> list[dict[str, Any]]:
        raw_records: list[dict[str, Any]] = []
        page_progress = tqdm(
            total=1,
            desc="fmp:discovery filings pages",
            unit="page",
            position=2,
            leave=False,
            disable=not show_progress,
        )
        try:
            # The worker's outer limiter accounts for this task's one API
            # request. Each continuation is a separate manifest checkpoint.
            url = (
                f"{base_url}?{query_string}&page={requested_page}"
                f"&limit={page_size}&apikey={credential}"
            )
            payload = await amake_request(url)
            if isinstance(payload, Mapping):
                message = str(
                    payload.get("Error Message")
                    or payload.get("error")
                    or payload.get("message")
                    or payload
                )
                raise RuntimeError(f"FMP API error: {message[:1000]}")
            page_rows = payload if isinstance(payload, list) else []
            raw_records.extend(
                dict(row) for row in page_rows if isinstance(row, Mapping)
            )
            page_progress.update(1)
            page_progress.set_postfix(rows=len(raw_records), refresh=False)
        finally:
            page_progress.close()
        return raw_records[:page_size]

    raw = asyncio.run(_extract())
    if not raw:
        raise EmptyDataError("No data was returned for the given query.")

    records: list[dict[str, Any]] = []
    ordered = sorted(
        raw,
        key=lambda row: str(row.get("acceptedDate") or row.get("filingDate") or ""),
        reverse=True,
    )
    row_progress = tqdm(
        ordered,
        total=len(ordered),
        desc="fmp:discovery filings normalize",
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for item in row_progress:
            inferred = not bool(item.get("acceptedDate"))
            if inferred:
                filing_date = item.get("filingDate")
                if not filing_date:
                    continue
                item["acceptedDate"] = f"{str(filing_date)[:10]}T00:00:00"
            record = FMPDiscoveryFilingsData.model_validate(item).model_dump(
                mode="json"
            )
            record["accepted_date_inferred"] = inferred
            records.append(record)
    finally:
        row_progress.close()
    if not records:
        raise EmptyDataError("FMP returned no valid discovery filing rows.")
    return records


def _fetch_fmp_fundamental_ratio_workaround(
    endpoint: str,
    kwargs: Mapping[str, Any],
    obb: Any,
) -> list[Any]:
    """Fetch only the FMP fundamental representation the task requested.

    OpenBB's FMP key-metrics and financial-ratios adapters unconditionally
    request both the historical and TTM URLs and discard one response for
    ``ttm=exclude``/``ttm=only``.  That hidden two-request fan-out halves the
    useful throughput of a daily-capped credential.  Archive tasks make the
    representation explicit, so one upstream request is sufficient unless the
    caller deliberately asks for ``ttm=include``.

    The returned models and the synthesized TTM fields intentionally match the
    installed OpenBB adapters; only the unnecessary HTTP call is removed.
    Every real request still passes through the process-wide FMP HTTP-boundary
    limiter installed by :class:`OpenBBWorker`.
    """
    import asyncio
    from urllib.parse import urlencode

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.utils.helpers import get_data_many

    fetchers: dict[str, tuple[Any, str]] = {}
    if endpoint == "equity.fundamental.metrics":
        from openbb_fmp.models.key_metrics import FMPKeyMetricsFetcher

        fetchers[endpoint] = (FMPKeyMetricsFetcher, "key-metrics")
    elif endpoint == "equity.fundamental.ratios":
        from openbb_fmp.models.financial_ratios import FMPFinancialRatiosFetcher

        fetchers[endpoint] = (FMPFinancialRatiosFetcher, "ratios")
    else:
        raise ValueError(f"Unsupported FMP fundamental endpoint: {endpoint}")

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for key metrics and ratios")
    credential = str(credential)

    fetcher, route = fetchers[endpoint]
    query = fetcher.transform_query(dict(kwargs))
    ttm_mode = str(query.ttm or "only").lower()
    if ttm_mode not in {"exclude", "include", "only"}:
        raise ValueError(f"Unsupported FMP ttm mode: {ttm_mode}")

    async def _extract() -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for symbol in (part.strip() for part in str(query.symbol).split(",")):
            if not symbol:
                continue
            historical: list[dict[str, Any]] = []
            if ttm_mode != "only":
                limit = max(1, int(query.limit or 1))
                historical_url = (
                    f"https://financialmodelingprep.com/stable/{route}?"
                    + urlencode(
                        {
                            "symbol": symbol,
                            "period": query.period,
                            "limit": limit,
                            "apikey": credential,
                        }
                    )
                )
                payload = await get_data_many(historical_url)
                historical = [dict(row) for row in payload if isinstance(row, Mapping)]
                results.extend(historical)

            if ttm_mode != "exclude":
                ttm_url = (
                    f"https://financialmodelingprep.com/stable/{route}-ttm?"
                    + urlencode({"symbol": symbol, "apikey": credential})
                )
                payload = await get_data_many(ttm_url)
                ttm_rows = [dict(row) for row in payload if isinstance(row, Mapping)]
                if ttm_rows:
                    ttm_row = ttm_rows[0]
                    today = datetime.today()
                    ttm_row["date"] = today.date().isoformat()
                    ttm_row["fiscal_period"] = "TTM"
                    ttm_row["fiscal_year"] = today.year
                    if historical and historical[0].get("reportedCurrency"):
                        ttm_row["reportedCurrency"] = historical[0]["reportedCurrency"]
                    results.insert(0, ttm_row)
        if not results:
            raise EmptyDataError("No data found for given symbols.")
        return results

    raw = asyncio.run(_extract())
    return _provider_result_rows(fetcher.transform_data(query, raw))


def _fmp_api_key(obb: Any, purpose: str) -> str:
    """Read the configured FMP secret without ever returning it in errors."""
    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError(f"FMP API key is required for {purpose}")
    return str(credential)


def _fetch_fmp_historical_eps_workaround(
    kwargs: Mapping[str, Any], obb: Any
) -> list[Any]:
    """Honor FMP's learned API cap without OpenBB's hidden ``limit + 5``.

    The installed OpenBB FMP adapter adds five rows to the requested limit
    before sending the HTTP query.  On an entitlement capped at five, a legal
    archive query therefore becomes an illegal request for ten.  Query the
    same official route once with the actual cap, then use OpenBB's canonical
    model transformer so the persisted schema remains unchanged.
    """
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.historical_eps import FMPHistoricalEpsFetcher

    credential = _fmp_api_key(obb, "historical EPS")
    query = FMPHistoricalEpsFetcher.transform_query(dict(kwargs))
    limit = max(1, int(query.limit or 5))
    raw = _fmp_page_json(
        "earnings",
        {"symbol": query.symbol, "limit": limit},
        credential,
    )
    today = date.today().isoformat()
    filtered = [
        row
        for row in sorted(
            raw,
            key=lambda item: str(item.get("date") or ""),
            reverse=True,
        )
        if str(row.get("date") or "") <= today
        and (row.get("epsActual") is not None or row.get("revenueActual") is not None)
    ][:limit]
    if not filtered:
        raise EmptyDataError(f"No data found for symbol: {query.symbol}")
    return _provider_result_rows(
        FMPHistoricalEpsFetcher.transform_data(query, filtered)
    )


def _fetch_fmp_equity_peers_workaround(
    kwargs: Mapping[str, Any], obb: Any
) -> list[Any]:
    """Normalize fractional upstream market caps before strict model parsing."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.equity_peers import FMPEquityPeersFetcher

    credential = _fmp_api_key(obb, "equity peers")
    query = FMPEquityPeersFetcher.transform_query(dict(kwargs))
    raw = _fmp_page_json("stock-peers", {"symbol": query.symbol}, credential)
    if not raw:
        raise EmptyDataError(f"No peer data found for symbol: {query.symbol}")
    for row in raw:
        value = row.get("mktCap")
        if isinstance(value, float) and math.isfinite(value):
            row["mktCap"] = int(round(value))
    return _provider_result_rows(FMPEquityPeersFetcher.transform_data(query, raw))


def _fmp_page_json(
    endpoint: str,
    params: Mapping[str, Any],
    credential: str,
) -> list[dict[str, Any]]:
    """Read one FMP page without exposing its credential in exceptions."""
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    query = urlencode({**params, "apikey": credential})
    url = f"https://financialmodelingprep.com/stable/{endpoint}?{query}"
    request = Request(url, headers={"User-Agent": "stockAgent-openbb-archive/1.0"})
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"FMP HTTP {exc.code}: {exc.reason}") from exc
    except (TimeoutError, URLError) as exc:
        reason = str(getattr(exc, "reason", type(exc).__name__))
        raise ConnectionError(
            f"FMP connection error: {reason.replace(credential, '<redacted>')}"
        ) from exc
    if isinstance(payload, Mapping):
        message = str(
            payload.get("Error Message")
            or payload.get("error")
            or payload.get("message")
            or payload
        )
        raise RuntimeError(
            f"FMP API error: {message.replace(credential, '<redacted>')[:1000]}"
        )
    if not isinstance(payload, list):
        raise TypeError(f"FMP returned {type(payload).__name__}, expected a list")
    return [dict(row) for row in payload if isinstance(row, Mapping)]


def _page_content_signature(rows: Sequence[Mapping[str, Any]]) -> str:
    """Fingerprint provider rows while ignoring archive envelope columns."""
    normalized = [
        {str(key): value for key, value in row.items() if not str(key).startswith("_")}
        for row in rows
    ]
    return hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()


def _raw_page_signature(page: Any) -> str:
    """Stable fingerprint for provider pages before OpenBB normalization."""
    payload = json.dumps(
        page,
        sort_keys=True,
        ensure_ascii=False,
        default=_json_default,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _iter_fmp_pages(
    endpoint: str,
    params: Mapping[str, Any],
    credential: str,
    *,
    page_size: int,
    page_limiter: Any | None,
    start_page: int = 0,
    wait_before_first: bool = True,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield FMP pages until a short page, rejecting repeated-page cycles.

    There is deliberately no arbitrary maximum page number.  Completeness is
    proved by the provider's terminal short/empty page; if a provider ignores
    the page parameter, the repeated-page signature fails loudly instead of
    silently truncating or looping forever.
    """
    page = max(0, int(start_page))
    seen_signatures: set[str] = set()
    first = True
    while True:
        if page_limiter is not None and (wait_before_first or not first):
            page_limiter.wait()
        first = False
        page_rows = _fmp_page_json(
            endpoint,
            {**params, "page": page, "limit": page_size},
            credential,
        )
        if page_rows:
            signature = _page_content_signature(page_rows)
            if signature in seen_signatures:
                raise RuntimeError(
                    f"FMP pagination cycle: {endpoint} repeated page content at page {page}"
                )
            seen_signatures.add(signature)
        yield page, page_rows
        if len(page_rows) < page_size:
            return
        page += 1


def _fetch_fmp_price_targets_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Fetch every FMP price-target page instead of the default first 100 rows."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.price_target import FMPPriceTargetFetcher

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for price targets")
    credential = str(credential)

    symbol = str(kwargs.get("symbol") or "").strip()
    if not symbol:
        raise ValueError("symbol is required for FMP price targets")
    start = str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10]
    end = str(kwargs.get("end_date") or date.today().isoformat())[:10]
    page_size = 100
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    progress = tqdm(
        total=None,
        desc=f"fmp:price targets {symbol}"[:64],
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for page, page_rows in _iter_fmp_pages(
            "price-target-news",
            {"symbol": symbol},
            credential,
            page_size=page_size,
            page_limiter=page_limiter,
        ):
            progress.update(1)
            for row in page_rows:
                published = str(row.get("publishedDate") or "")[:10]
                if published and not (start <= published <= end):
                    continue
                fingerprint = _canonical_json(row)
                if fingerprint not in seen:
                    seen.add(fingerprint)
                    records.append(row)
            progress.set_postfix(
                rows=len(records), page_rows=len(page_rows), refresh=False
            )
            if len(page_rows) < page_size:
                break
    finally:
        progress.close()
    if not records:
        raise EmptyDataError("No FMP price-target rows matched the archive range.")
    query = FMPPriceTargetFetcher.transform_query({"symbol": symbol})
    return _provider_result_rows(FMPPriceTargetFetcher.transform_data(query, records))


def _fetch_fmp_world_articles_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Page the undated FMP editorial feed and retain the archive date range."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.world_news import FMPWorldNewsFetcher

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for world articles")
    credential = str(credential)

    start = str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10]
    end = str(kwargs.get("end_date") or date.today().isoformat())[:10]
    page_size = 100
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    progress = tqdm(
        total=None,
        desc="fmp:world articles pages",
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for page, page_rows in _iter_fmp_pages(
            "news/fmp-articles",
            {},
            credential,
            page_size=page_size,
            page_limiter=page_limiter,
        ):
            progress.update(1)
            for row in page_rows:
                published = str(row.get("publishedDate") or row.get("date") or "")[:10]
                if not published or not (start <= published <= end):
                    continue
                fingerprint = _canonical_json(row)
                if fingerprint not in seen:
                    seen.add(fingerprint)
                    records.append(row)
            progress.set_postfix(
                rows=len(records), page_rows=len(page_rows), refresh=False
            )
            if len(page_rows) < page_size:
                break
    finally:
        progress.close()
    if not records:
        raise EmptyDataError("No FMP world articles matched the archive range.")
    query = FMPWorldNewsFetcher.transform_query({"topic": "fmp_articles"})
    return _provider_result_rows(FMPWorldNewsFetcher.transform_data(query, records))


def _fetch_fmp_insider_trading_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Fetch all FMP insider transactions instead of its capped first page."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.insider_trading import FMPInsiderTradingFetcher

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for insider trading")
    credential = str(credential)

    symbol = str(kwargs.get("symbol") or "").strip()
    if not symbol:
        raise ValueError("symbol is required for FMP insider trading")
    start = str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10]
    end = str(kwargs.get("end_date") or date.today().isoformat())[:10]
    page_size = 1000
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    progress = tqdm(
        total=None,
        desc=f"fmp:insider trading {symbol}"[:64],
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for page, page_rows in _iter_fmp_pages(
            "insider-trading/search",
            {"symbol": symbol},
            credential,
            page_size=page_size,
            page_limiter=page_limiter,
        ):
            progress.update(1)
            for row in page_rows:
                filing_date = str(
                    row.get("filingDate") or row.get("transactionDate") or ""
                )[:10]
                if not filing_date or not (start <= filing_date <= end):
                    continue
                fingerprint = _canonical_json(row)
                if fingerprint not in seen:
                    seen.add(fingerprint)
                    records.append(row)
            progress.set_postfix(
                rows=len(records), page_rows=len(page_rows), refresh=False
            )
            if len(page_rows) < page_size:
                break
    finally:
        progress.close()
    if not records:
        raise EmptyDataError("No FMP insider-trading rows matched the archive range.")
    query = FMPInsiderTradingFetcher.transform_query({"symbol": symbol})
    return _provider_result_rows(
        FMPInsiderTradingFetcher.transform_data(query, records)
    )


def _fetch_fmp_government_trades_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Fetch the complete paginated House and Senate transaction catalogs."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_fmp.models.government_trades import FMPGovernmentTradesFetcher

    credential = getattr(obb.user.credentials, "fmp_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FMP API key is required for government trades")
    credential = str(credential)

    start = str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10]
    end = str(kwargs.get("end_date") or date.today().isoformat())[:10]
    page_size = 250
    active = {"house": "House", "senate": "Senate"}
    page_signatures: dict[str, set[str]] = {chamber: set() for chamber in active}
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    request_count = 0
    progress = tqdm(
        total=None,
        desc="fmp:government trades pages",
        unit="request",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        page = 0
        while active:
            completed: list[str] = []
            for chamber, chamber_label in active.items():
                if page_limiter is not None:
                    page_limiter.wait()
                page_rows = _fmp_page_json(
                    f"{chamber}-latest",
                    {"page": page, "limit": page_size},
                    credential,
                )
                request_count += 1
                if page_rows:
                    signature = _page_content_signature(page_rows)
                    if signature in page_signatures[chamber]:
                        raise RuntimeError(
                            "FMP pagination cycle: "
                            f"{chamber}-latest repeated page content at page {page}"
                        )
                    page_signatures[chamber].add(signature)
                progress.update(1)
                for row in page_rows:
                    row["chamber"] = chamber_label
                    report_date = str(
                        row.get("disclosureDate")
                        or row.get("date")
                        or row.get("transactionDate")
                        or row.get("dateReceived")
                        or ""
                    )[:10]
                    if report_date and not (start <= report_date <= end):
                        continue
                    fingerprint = _canonical_json(row)
                    if fingerprint not in seen:
                        seen.add(fingerprint)
                        records.append(row)
                progress.set_postfix(
                    page=page,
                    chamber=chamber,
                    rows=len(records),
                    page_rows=len(page_rows),
                    refresh=False,
                )
                if len(page_rows) < page_size:
                    completed.append(chamber)
            for chamber in completed:
                active.pop(chamber, None)
            page += 1
    finally:
        progress.close()
    if not records:
        raise EmptyDataError("No FMP government-trade rows matched the archive range.")
    query = FMPGovernmentTradesFetcher.transform_query({"chamber": "all"})
    return _provider_result_rows(
        FMPGovernmentTradesFetcher.transform_data(query, records)
    )


def _fetch_tiingo_news_workaround(
    endpoint: str,
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Fetch every Tiingo news offset instead of its implicit first 1,000 rows."""
    import asyncio
    from urllib.parse import urlencode

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_tiingo.models.company_news import TiingoCompanyNewsFetcher
    from openbb_tiingo.models.world_news import TiingoWorldNewsFetcher
    from openbb_tiingo.utils.helpers import get_data

    credential = getattr(obb.user.credentials, "tiingo_token", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("Tiingo token is required for news")

    fetcher = (
        TiingoCompanyNewsFetcher
        if endpoint == "news.company"
        else TiingoWorldNewsFetcher
    )
    query_params = {
        key: value
        for key, value in dict(kwargs).items()
        if key not in {"display", "page", "topic"}
    }
    query_params["limit"] = 1000
    query_params["offset"] = 0
    query = fetcher.transform_query(query_params)
    base_params = query.model_dump(by_alias=True, exclude_none=True)
    base_params.pop("limit", None)
    base_params.pop("offset", None)
    page_size = 1000
    offset = 0
    records: list[dict[str, Any]] = []
    seen_page_signatures: set[str] = set()
    progress = tqdm(
        total=None,
        desc=f"tiingo:{endpoint} pages"[:64],
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        first = True
        while True:
            if not first and page_limiter is not None:
                page_limiter.wait()
            first = False
            encoded = urlencode(
                {
                    **base_params,
                    "limit": page_size,
                    "offset": offset,
                    "token": str(credential),
                }
            )
            payload = asyncio.run(
                get_data(f"https://api.tiingo.com/tiingo/news?{encoded}")
            )
            page_rows = (
                [dict(row) for row in payload if isinstance(row, Mapping)]
                if isinstance(payload, list)
                else [dict(payload)]
                if isinstance(payload, Mapping)
                else []
            )
            if page_rows:
                signature = _page_content_signature(page_rows)
                if signature in seen_page_signatures:
                    raise RuntimeError(
                        "Tiingo pagination cycle: repeated page content at "
                        f"offset {offset}"
                    )
                seen_page_signatures.add(signature)
                records.extend(page_rows)
            progress.update(1)
            progress.set_postfix(
                offset=offset,
                rows=len(records),
                page_rows=len(page_rows),
                refresh=False,
            )
            if len(page_rows) < page_size:
                break
            offset += page_size
    finally:
        progress.close()
    if not records:
        raise EmptyDataError("No Tiingo news rows matched the archive range.")
    return _provider_result_rows(fetcher.transform_data(query, records))


def _fetch_eia_petroleum_status_workaround(kwargs: Mapping[str, Any]) -> list[Any]:
    """Fetch a real EIA workbook table hidden by provider schema drift."""
    import asyncio

    from openbb_us_eia.models.petroleum_status_report import (
        EiaPetroleumStatusReportFetcher,
    )

    query = EiaPetroleumStatusReportFetcher.transform_query(dict(kwargs))
    raw = asyncio.run(EiaPetroleumStatusReportFetcher.aextract_data(query, None))
    return _provider_result_rows(
        EiaPetroleumStatusReportFetcher.transform_data(query, raw)
    )


def _fetch_federal_reserve_central_bank_holdings_workaround(
    kwargs: Mapping[str, Any],
) -> list[Any]:
    """Normalize authoritative empty SOMA WAM responses before validation."""
    import asyncio

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_federal_reserve.models.central_bank_holdings import (
        FederalReserveCentralBankHoldingsFetcher,
    )

    query = FederalReserveCentralBankHoldingsFetcher.transform_query(dict(kwargs))
    raw = asyncio.run(
        FederalReserveCentralBankHoldingsFetcher.aextract_data(query, None)
    )
    meaningful = [
        row
        for row in raw
        if isinstance(row, Mapping) and (row.get("asOfDate") or row.get("date"))
    ]
    if not meaningful:
        raise EmptyDataError(
            "NY Fed SOMA returned an authoritative empty response for this as-of date"
        )
    return _provider_result_rows(
        FederalReserveCentralBankHoldingsFetcher.transform_data(query, meaningful)
    )


_NPORT_SINGLETON_MAPPING_PATHS: tuple[tuple[str, ...], ...] = (
    # xmltodict returns a list whenever an XML element is repeated.  OpenBB's
    # N-PORT transformer flattens each of these fields as one mapping per
    # output row, so repeated values must become repeated relational rows.
    ("identifiers",),
    ("identifiers", "isin"),
    ("identifiers", "other"),
    ("securityLending",),
    ("securityLending", "loanByFundCondition"),
    ("debtSec",),
    ("issuerConditional",),
    ("assetConditional",),
    ("currencyConditional",),
    ("derivativeInfo",),
    ("derivativeInfo", "optionSwaptionWarrantDeriv"),
    ("derivativeInfo", "optionSwaptionWarrantDeriv", "counterparties"),
    ("derivativeInfo", "optionSwaptionWarrantDeriv", "descRefInstrmnt"),
    (
        "derivativeInfo",
        "optionSwaptionWarrantDeriv",
        "descRefInstrmnt",
        "otherRefInst",
    ),
    (
        "derivativeInfo",
        "optionSwaptionWarrantDeriv",
        "descRefInstrmnt",
        "nestedDerivInfo",
    ),
    (
        "derivativeInfo",
        "optionSwaptionWarrantDeriv",
        "descRefInstrmnt",
        "nestedDerivInfo",
        "fwdDeriv",
    ),
    (
        "derivativeInfo",
        "optionSwaptionWarrantDeriv",
        "descRefInstrmnt",
        "nestedDerivInfo",
        "fwdDeriv",
        "derivAddlInfo",
    ),
    ("derivativeInfo", "futrDeriv"),
    ("derivativeInfo", "futrDeriv", "counterparties"),
    ("derivativeInfo", "futrDeriv", "descRefInstrmnt"),
    ("derivativeInfo", "futrDeriv", "descRefInstrmnt", "indexBasketInfo"),
    ("derivativeInfo", "fwdDeriv"),
    ("derivativeInfo", "fwdDeriv", "counterparties"),
    ("derivativeInfo", "swapDeriv"),
    ("derivativeInfo", "swapDeriv", "counterparties"),
    ("derivativeInfo", "swapDeriv", "descRefInstrmnt"),
    ("derivativeInfo", "swapDeriv", "descRefInstrmnt", "otherRefInst"),
    ("derivativeInfo", "swapDeriv", "descRefInstrmnt", "indexBasketInfo"),
    ("derivativeInfo", "swapDeriv", "otherRecDesc"),
    ("derivativeInfo", "swapDeriv", "floatingRecDesc"),
    ("derivativeInfo", "swapDeriv", "floatingRecDesc", "rtResetTenors"),
    (
        "derivativeInfo",
        "swapDeriv",
        "floatingRecDesc",
        "rtResetTenors",
        "rtResetTenor",
    ),
    ("derivativeInfo", "swapDeriv", "floatingPmntDesc"),
    ("derivativeInfo", "swapDeriv", "floatingPmntDesc", "rtResetTenors"),
    (
        "derivativeInfo",
        "swapDeriv",
        "floatingPmntDesc",
        "rtResetTenors",
        "rtResetTenor",
    ),
    ("repurchaseAgrmt",),
    ("repurchaseAgrmt", "clearedCentCparty"),
    ("repurchaseAgrmt", "repurchaseCollaterals"),
    ("repurchaseAgrmt", "repurchaseCollaterals", "repurchaseCollateral"),
)


def _expand_mapping_list_paths(
    records: Sequence[Mapping[str, Any]],
    paths: Sequence[Sequence[str]],
) -> list[dict[str, Any]]:
    """Relationally expand repeated XML mappings without discarding values."""

    expanded = [deepcopy(dict(record)) for record in records]
    for path in paths:
        next_records: list[dict[str, Any]] = []
        for record in expanded:
            parent: Any = record
            for key in path[:-1]:
                if not isinstance(parent, Mapping):
                    break
                parent = parent.get(key)
            else:
                if isinstance(parent, Mapping):
                    repeated = parent.get(path[-1])
                    if isinstance(repeated, list):
                        # An empty repeated element is equivalent to an absent
                        # optional mapping.  A non-empty list is a one-to-many
                        # relationship and produces one complete output row per
                        # member, including Cartesian expansion when more than
                        # one independent nested relationship repeats.
                        members: Sequence[Any] = repeated or (None,)
                        for member in members:
                            clone = deepcopy(record)
                            clone_parent: Any = clone
                            for key in path[:-1]:
                                clone_parent = clone_parent[key]
                            if member is None:
                                clone_parent.pop(path[-1], None)
                            else:
                                clone_parent[path[-1]] = deepcopy(member)
                            next_records.append(clone)
                        continue
            next_records.append(record)
        expanded = next_records
    return expanded


def _normalize_nport_transformer_contract(
    holdings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Normalize every nullable mapping OpenBB's N-PORT parser dereferences.

    SEC XML permits several derivative/reference containers to be absent or
    empty. ``xmltodict`` represents those elements as missing keys or ``None``,
    while the current OpenBB transformer directly indexes them. Normalize the
    entire transformer boundary in one place instead of adding one filing- or
    symbol-specific exception each time a legal optional field appears.
    Unknown non-null scalar/container shapes fail closed so they remain visible
    as retryable provider schema drift rather than silently losing values.
    """

    def mapping_at(parent: dict[str, Any], key: str, path: str) -> dict[str, Any]:
        value = parent.get(key)
        if value is None:
            normalized: dict[str, Any] = {}
            parent[key] = normalized
            return normalized
        if not isinstance(value, Mapping):
            raise ProviderResponseShapeError(
                f"SEC NPORT field {path} must be a mapping or null"
            )
        if not isinstance(value, dict):
            value = dict(value)
            parent[key] = value
        return value

    def floatable(parent: dict[str, Any], *keys: str) -> None:
        # OpenBB applies ``float(value)`` directly to these schema-optional
        # values. NaN lets pandas/model normalization preserve them as null;
        # zero would fabricate a financial amount.
        for key in keys:
            value = parent.get(key)
            if value is None or value == "":
                parent[key] = "nan"

    normalized_holdings = [deepcopy(dict(holding)) for holding in holdings]
    for holding in normalized_holdings:
        mapping_at(holding, "identifiers", "invstOrSec.identifiers")

        if "securityLending" in holding:
            security = mapping_at(
                holding,
                "securityLending",
                "invstOrSec.securityLending",
            )
            if "loanByFundCondition" in security:
                mapping_at(
                    security,
                    "loanByFundCondition",
                    "invstOrSec.securityLending.loanByFundCondition",
                )

        if "derivativeInfo" in holding:
            derivative = mapping_at(
                holding,
                "derivativeInfo",
                "invstOrSec.derivativeInfo",
            )
            option = derivative.get("optionSwaptionWarrantDeriv")
            if option is not None:
                option = mapping_at(
                    derivative,
                    "optionSwaptionWarrantDeriv",
                    "invstOrSec.derivativeInfo.optionSwaptionWarrantDeriv",
                )
                mapping_at(
                    option,
                    "counterparties",
                    "invstOrSec.derivativeInfo.optionSwaptionWarrantDeriv.counterparties",
                )
                mapping_at(
                    option,
                    "descRefInstrmnt",
                    "invstOrSec.derivativeInfo.optionSwaptionWarrantDeriv.descRefInstrmnt",
                )
                floatable(option, "unrealizedAppr")

            future = derivative.get("futrDeriv")
            if future is not None:
                future = mapping_at(
                    derivative,
                    "futrDeriv",
                    "invstOrSec.derivativeInfo.futrDeriv",
                )
                mapping_at(
                    future,
                    "descRefInstrmnt",
                    "invstOrSec.derivativeInfo.futrDeriv.descRefInstrmnt",
                )
                if "counterparties" in future:
                    mapping_at(
                        future,
                        "counterparties",
                        "invstOrSec.derivativeInfo.futrDeriv.counterparties",
                    )
                floatable(future, "notionalAmt", "unrealizedAppr")

            forward = derivative.get("fwdDeriv")
            if forward is not None:
                forward = mapping_at(
                    derivative,
                    "fwdDeriv",
                    "invstOrSec.derivativeInfo.fwdDeriv",
                )
                mapping_at(
                    forward,
                    "counterparties",
                    "invstOrSec.derivativeInfo.fwdDeriv.counterparties",
                )
                floatable(
                    forward,
                    "amtCurSold",
                    "amtCurPur",
                    "unrealizedAppr",
                )

            swap = derivative.get("swapDeriv")
            if swap is not None:
                swap = mapping_at(
                    derivative,
                    "swapDeriv",
                    "invstOrSec.derivativeInfo.swapDeriv",
                )
                mapping_at(
                    swap,
                    "counterparties",
                    "invstOrSec.derivativeInfo.swapDeriv.counterparties",
                )
                description = mapping_at(
                    swap,
                    "descRefInstrmnt",
                    "invstOrSec.derivativeInfo.swapDeriv.descRefInstrmnt",
                )
                # OpenBB tests for this value under descRefInstrmnt but then
                # reads it from the swap root. Preserve the SEC value at both
                # paths so the provider typo cannot discard it or raise.
                if "otherRecDesc" in description and "otherRecDesc" not in swap:
                    swap["otherRecDesc"] = deepcopy(description["otherRecDesc"])
                if "otherRecDesc" in swap:
                    mapping_at(
                        swap,
                        "otherRecDesc",
                        "invstOrSec.derivativeInfo.swapDeriv.otherRecDesc",
                    )
                for leg_name in ("floatingRecDesc", "floatingPmntDesc"):
                    if leg_name not in swap:
                        continue
                    leg = mapping_at(
                        swap,
                        leg_name,
                        f"invstOrSec.derivativeInfo.swapDeriv.{leg_name}",
                    )
                    reset_tenors = mapping_at(
                        leg,
                        "rtResetTenors",
                        f"invstOrSec.derivativeInfo.swapDeriv.{leg_name}.rtResetTenors",
                    )
                    mapping_at(
                        reset_tenors,
                        "rtResetTenor",
                        "invstOrSec.derivativeInfo.swapDeriv."
                        f"{leg_name}.rtResetTenors.rtResetTenor",
                    )
                    floatable(leg, "@floatingRtSpread", "@pmntAmt")
                floatable(
                    swap,
                    "upfrontPmnt",
                    "upfrontRcpt",
                    "notionalAmt",
                    "unrealizedAppr",
                )

        if "repurchaseAgrmt" in holding:
            repurchase = mapping_at(
                holding,
                "repurchaseAgrmt",
                "invstOrSec.repurchaseAgrmt",
            )
            if "clearedCentCparty" in repurchase:
                mapping_at(
                    repurchase,
                    "clearedCentCparty",
                    "invstOrSec.repurchaseAgrmt.clearedCentCparty",
                )
            if "repurchaseCollaterals" in repurchase:
                collaterals = mapping_at(
                    repurchase,
                    "repurchaseCollaterals",
                    "invstOrSec.repurchaseAgrmt.repurchaseCollaterals",
                )
                if "repurchaseCollateral" in collaterals:
                    collateral = mapping_at(
                        collaterals,
                        "repurchaseCollateral",
                        "invstOrSec.repurchaseAgrmt.repurchaseCollaterals."
                        "repurchaseCollateral",
                    )
                    floatable(collateral, "principalAmt", "collateralVal")

    return normalized_holdings


def _fetch_sec_nport_workaround(
    kwargs: Mapping[str, Any], *, page_limiter: Any | None = None
) -> list[Any]:
    """Fetch NPORT search + XML sequentially under the SEC provider limiter."""
    import calendar
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    import xmltodict
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.models.nport_disclosure import SecNportDisclosureFetcher
    from openbb_sec.utils.definitions import HEADERS

    query = SecNportDisclosureFetcher.transform_query(dict(kwargs))
    symbol = str(query.symbol).strip().upper().replace(".", "-")
    cik_map = _sec_symbol_cik_map(page_limiter)
    identifier = (_SEC_FUND_SERIES_CACHE or {}).get(symbol) or cik_map.get(symbol)
    if not identifier:
        raise EmptyDataError(f"No SEC fund identifier was found for {symbol}")

    if page_limiter is not None:
        _wait_sec_http_limiter(page_limiter)
    search_url = "https://efts.sec.gov/LATEST/search-index?" + urlencode(
        {"q": identifier, "dateRange": "all", "forms": "NPORT-P"}
    )
    try:
        with urlopen(
            Request(search_url, headers=dict(HEADERS)), timeout=60
        ) as response:
            search_content = response.read()
            if search_content.startswith(b"\x1f\x8b"):
                search_content = gzip.decompress(search_content)
            search_payload = json.loads(search_content)
    except HTTPError as exc:
        raise RuntimeError(f"SEC NPORT search HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise ConnectionError(f"SEC NPORT search error: {exc.reason}") from exc

    if not isinstance(search_payload, Mapping):
        raise ProviderResponseShapeError("SEC NPORT search response must be a mapping")
    hits_envelope = search_payload.get("hits")
    if not isinstance(hits_envelope, Mapping):
        raise ProviderResponseShapeError(
            "SEC NPORT search response field hits must be a mapping"
        )
    hits = hits_envelope.get("hits", [])
    if not isinstance(hits, list):
        raise ProviderResponseShapeError(
            "SEC NPORT search response field hits.hits must be a list"
        )
    candidates: list[dict[str, Any]] = []
    for hit in hits:
        if not isinstance(hit, Mapping) or not isinstance(hit.get("_source"), Mapping):
            continue
        source = hit["_source"]
        ciks = source.get("ciks") or []
        if not ciks:
            continue
        candidates.append(
            {
                "period_ending": str(source.get("period_ending") or "")[:10],
                "file_date": str(source.get("file_date") or "")[:10],
                "primary_doc": (
                    "https://www.sec.gov/Archives/edgar/data/"
                    f"{int(ciks[0])}/{str(hit.get('_id') or '').replace('-', '').replace(':', '/')}"
                ),
            }
        )
    candidates = [item for item in candidates if item["period_ending"]]
    if not candidates:
        raise EmptyDataError(f"No NPORT-P records found for {symbol}")

    selected = max(candidates, key=lambda item: item["file_date"])
    if query.year is not None and query.quarter is not None:
        month = int(query.quarter) * 3
        target = date(
            int(query.year), month, calendar.monthrange(int(query.year), month)[1]
        )

        def candidate_distance(item: Mapping[str, Any]) -> tuple[int, int]:
            """Prefer the latest amendment when periods are equally close."""
            period_distance = abs(
                (date.fromisoformat(str(item["period_ending"])) - target).days
            )
            try:
                filing_ordinal = date.fromisoformat(
                    str(item.get("file_date") or "")
                ).toordinal()
            except ValueError:
                filing_ordinal = date.min.toordinal()
            return period_distance, -filing_ordinal

        selected = min(
            candidates,
            key=candidate_distance,
        )

    if page_limiter is not None:
        _wait_sec_http_limiter(page_limiter)
    try:
        with urlopen(
            Request(selected["primary_doc"], headers=dict(HEADERS)), timeout=60
        ) as response:
            content = response.read()
            if content.startswith(b"\x1f\x8b"):
                content = gzip.decompress(content)
    except HTTPError as exc:
        if exc.code == 404:
            raise EmptyDataError(
                f"NPORT filing document was not found for {symbol}"
            ) from exc
        raise RuntimeError(f"SEC NPORT filing HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise ConnectionError(f"SEC NPORT filing error: {exc.reason}") from exc

    raw = xmltodict.parse(content)
    submission = raw.get("edgarSubmission") if isinstance(raw, Mapping) else None
    if submission is not None and not isinstance(submission, Mapping):
        raise ProviderResponseShapeError(
            "SEC NPORT field edgarSubmission must be a mapping"
        )
    form_data = submission.get("formData") if isinstance(submission, Mapping) else None
    if form_data is not None and not isinstance(form_data, Mapping):
        raise ProviderResponseShapeError(
            "SEC NPORT field edgarSubmission.formData must be a mapping"
        )
    if isinstance(form_data, Mapping):
        investments = form_data.get("invstOrSecs")
        if investments is None:
            # An amended or original NPORT-P can explicitly declare an empty
            # investment container.  OpenBB performs ``'invstOrSec' in value``
            # and crashes when xmltodict represents that legal empty element
            # as None.  Preserve the empty meaning with the mapping shape the
            # transformer already understands.
            containers: list[Any] = []
        elif isinstance(investments, Mapping):
            containers = [investments]
        elif isinstance(investments, list):
            containers = list(investments)
        else:
            raise ProviderResponseShapeError(
                "SEC NPORT field invstOrSecs must be a mapping, list, or null"
            )
        holdings: list[Any] = []
        for container in containers:
            if container is None:
                continue
            if not isinstance(container, Mapping):
                raise ProviderResponseShapeError(
                    "SEC NPORT investment containers must be mappings or null"
                )
            value = container.get("invstOrSec")
            if isinstance(value, list):
                holdings.extend(value)
            elif isinstance(value, Mapping):
                holdings.append(value)
            elif value is not None:
                raise ProviderResponseShapeError(
                    "SEC NPORT field invstOrSec must be a mapping, list, or null"
                )
        if holdings:
            invalid_holding_types = sorted(
                {
                    type(holding).__name__
                    for holding in holdings
                    if not isinstance(holding, Mapping)
                }
            )
            if invalid_holding_types:
                raise ProviderResponseShapeError(
                    "SEC N-PORT holdings must be mappings; received "
                    + ", ".join(invalid_holding_types)
                )
            holdings = _expand_mapping_list_paths(
                holdings,
                _NPORT_SINGLETON_MAPPING_PATHS,
            )
            holdings = _normalize_nport_transformer_contract(holdings)
            # The OpenBB transformer expects exactly one mapping container and
            # a list of records. xmltodict emits a list of containers when the
            # filing repeats <invstOrSecs>, and a mapping when it does not.
            form_data["invstOrSecs"] = {"invstOrSec": holdings}
        else:
            form_data["invstOrSecs"] = {}
        fund_info = form_data.get("fundInfo")
        if isinstance(fund_info, Mapping):
            borrowers = fund_info.get("borrowers")
            if borrowers is None:
                fund_info["borrowers"] = {"borrower": []}
            elif isinstance(borrowers, Mapping):
                borrower = borrowers.get("borrower")
                if isinstance(borrower, Mapping):
                    borrowers["borrower"] = [borrower]
                elif borrower is None:
                    borrowers["borrower"] = []
    try:
        transformed = SecNportDisclosureFetcher.transform_data(query, raw)
    except (AttributeError, KeyError, TypeError) as exc:
        raise ProviderResponseShapeError(
            "SEC NPORT transformer rejected a normalized provider shape: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    return _provider_result_rows(transformed)


_SEC_FTD_URLS_LOCK = threading.Lock()
_SEC_FTD_URLS_CACHE: dict[str, str] | None = None


def _fetch_sec_ftd_report_workaround(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Download one stable SEC half-month FTD bulk report.

    OpenBB's symbol route defaults to the latest 24 reports and gathers every
    selected ZIP concurrently for each symbol.  The archive instead resolves
    SEC's YYYYMMa/b catalog key once per process, downloads one bulk ZIP per
    task, and retains every symbol row in that official report.
    """
    import asyncio

    from openbb_sec.utils.helpers import download_zip_file, get_ftd_urls

    report_key = str(kwargs.get("report_key") or "").strip().lower()
    if not re.fullmatch(r"\d{6}[ab]", report_key):
        raise ValueError(f"Invalid SEC FTD report key: {report_key!r}")

    global _SEC_FTD_URLS_CACHE
    with _SEC_FTD_URLS_LOCK:
        if _SEC_FTD_URLS_CACHE is None:
            if page_limiter is not None:
                _wait_sec_http_limiter(page_limiter)
            # Cache the catalog so hundreds of report tasks do not repeatedly
            # request SEC data.json. The helper wrapper is suppressed because
            # this direct workaround already claimed the request ticket.
            with _suppress_sec_helper_pacing():
                raw_urls = asyncio.run(get_ftd_urls())
            _SEC_FTD_URLS_CACHE = {
                str(key).lower(): str(value) for key, value in raw_urls.items()
            }
        report_url = _SEC_FTD_URLS_CACHE.get(report_key)
    if not report_url:
        return []

    if page_limiter is not None:
        _wait_sec_http_limiter(page_limiter)
    with _suppress_sec_helper_pacing():
        records = asyncio.run(
            download_zip_file(
                report_url,
                symbol=None,
                use_cache=bool(kwargs.get("use_cache", True)),
            )
        )
    start = date.fromisoformat(str(kwargs["start_date"]))
    end = date.fromisoformat(str(kwargs["end_date"]))
    progress = tqdm(
        records,
        total=len(records),
        desc=f"sec:ftd {report_key} filter",
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    filtered: list[dict[str, Any]] = []
    try:
        for record in progress:
            raw_date = record.get("date")
            if raw_date is None:
                continue
            try:
                record_date = date.fromisoformat(str(raw_date)[:10])
            except ValueError:
                continue
            if start <= record_date <= end:
                filtered.append(record)
            progress.set_postfix(rows=len(filtered), refresh=False)
    finally:
        progress.close()
    return filtered


def _sec_archive_request_headers() -> dict[str, str]:
    """Return a declared SEC identity without accepting compressed envelopes."""
    return {
        "User-Agent": os.environ.get(
            "SEC_USER_AGENT",
            "stockAgent-openbb-archive/1.0 local-operator@example.com",
        ),
        "Accept-Encoding": "identity",
    }


def _sec_insider_dataset_catalog(
    page_limiter: Any | None,
) -> dict[tuple[int, int], str]:
    """Resolve immutable quarterly ZIP URLs from SEC's official catalog."""
    from html import unescape
    from urllib.parse import urljoin
    from urllib.request import Request, urlopen

    global _SEC_INSIDER_DATASET_URLS
    with _SEC_INSIDER_DATASET_LOCK:
        if _SEC_INSIDER_DATASET_URLS is not None:
            return dict(_SEC_INSIDER_DATASET_URLS)
        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        request = Request(
            SEC_INSIDER_DATASET_CATALOG_URL,
            headers=_sec_archive_request_headers(),
        )
        with urlopen(request, timeout=60) as response:
            page = response.read().decode("utf-8", errors="replace")
        urls: dict[tuple[int, int], str] = {}
        for href, year_text, quarter_text in re.findall(
            r"href\s*=\s*['\"]([^'\"]*?((?:19|20)\d{2})q([1-4])_form345\.zip[^'\"]*)['\"]",
            page,
            flags=re.IGNORECASE,
        ):
            clean_href = unescape(href).split("?", 1)[0]
            urls[(int(year_text), int(quarter_text))] = urljoin(
                SEC_INSIDER_DATASET_CATALOG_URL, clean_href
            )
        if not urls:
            raise RuntimeError(
                "SEC insider-transactions catalog contained no quarterly ZIP links"
            )
        _SEC_INSIDER_DATASET_URLS = urls
        return dict(urls)


def _sec_insider_zip_members(path: Path, *, deep: bool) -> dict[str, str]:
    """Validate one official ZIP and return canonical-to-physical members."""
    from zipfile import BadZipFile, ZipFile

    try:
        with ZipFile(path) as archive:
            members = {
                Path(name).name.upper(): name
                for name in archive.namelist()
                if not name.endswith("/")
            }
            missing = sorted(
                member
                for member in _SEC_INSIDER_REQUIRED_TABLES
                if member.upper() not in members
            )
            if missing:
                raise ValueError(
                    "SEC insider ZIP is missing tables: " + ", ".join(missing)
                )
            if deep:
                corrupt = archive.testzip()
                if corrupt:
                    raise ValueError(f"SEC insider ZIP failed CRC at {corrupt}")
            return {
                member: members[member.upper()]
                for member in _SEC_INSIDER_REQUIRED_TABLES
            }
    except (BadZipFile, OSError) as exc:
        raise ValueError(f"Invalid SEC insider ZIP: {path}") from exc


def _fetch_sec_insider_quarter_zip(
    year: int,
    quarter: int,
    *,
    cache_dir: Path,
    page_limiter: Any | None,
    show_progress: bool,
) -> tuple[Path, str]:
    """Fetch one immutable SEC quarter with atomic, restart-safe caching."""
    from urllib.request import Request, urlopen

    cache_path = cache_dir / "quarterly" / f"{year}q{quarter}_form345.zip"
    lock = _sec_companyfacts_lock(f"insider-quarter:{year}q{quarter}")
    with lock:
        if cache_path.exists():
            try:
                _sec_insider_zip_members(cache_path, deep=False)
                source_url = _sec_insider_dataset_catalog(page_limiter).get(
                    (year, quarter), ""
                )
                return cache_path, source_url
            except ValueError:
                quarantine = cache_path.with_name(
                    f"{cache_path.name}.invalid.{int(time.time())}"
                )
                cache_path.replace(quarantine)

        source_url = _sec_insider_dataset_catalog(page_limiter).get((year, quarter))
        if not source_url:
            raise LookupError(
                f"SEC catalog has no published insider dataset for {year} Q{quarter}"
            )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = cache_path.with_name(
            f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            if page_limiter is not None:
                _wait_sec_http_limiter(page_limiter)
            request = Request(source_url, headers=_sec_archive_request_headers())
            with urlopen(request, timeout=180) as response, temporary.open("wb") as out:
                raw_total = response.headers.get("Content-Length")
                try:
                    total = int(raw_total) if raw_total else None
                except ValueError:
                    total = None
                progress = tqdm(
                    total=total,
                    desc=f"sec:insider {year}Q{quarter} download",
                    unit="B",
                    unit_scale=True,
                    position=2,
                    leave=False,
                    disable=not show_progress,
                )
                try:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        out.write(chunk)
                        progress.update(len(chunk))
                finally:
                    progress.close()
                out.flush()
                os.fsync(out.fileno())
            _sec_insider_zip_members(temporary, deep=True)
            temporary.replace(cache_path)
        finally:
            temporary.unlink(missing_ok=True)
        return cache_path, source_url


def _sec_insider_tsv_rows(
    archive: Any,
    member: str,
    physical_member: str,
    *,
    show_progress: bool,
) -> Iterator[dict[str, str]]:
    """Stream one ZIP TSV with a byte-accurate progress bar."""
    info = archive.getinfo(physical_member)
    progress = tqdm(
        total=info.file_size,
        desc=f"sec:insider parse {member}",
        unit="B",
        unit_scale=True,
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        with archive.open(physical_member) as stream:
            first = True

            def decoded_lines() -> Iterator[str]:
                nonlocal first
                for raw_line in stream:
                    progress.update(len(raw_line))
                    text_line = raw_line.decode("utf-8", errors="replace")
                    if first:
                        text_line = text_line.lstrip("\ufeff")
                        first = False
                    yield text_line

            reader = csv.DictReader(decoded_lines(), delimiter="\t")
            for raw_row in reader:
                yield {
                    str(key).strip(): str(value or "").strip()
                    for key, value in raw_row.items()
                    if key is not None
                }
    finally:
        progress.close()


def _sec_insider_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def _sec_insider_float(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _sec_insider_bool(value: Any) -> bool | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _unique_join(values: Iterable[Any]) -> str | None:
    unique = list(
        dict.fromkeys(str(value).strip() for value in values if str(value).strip())
    )
    return "; ".join(unique) if unique else None


def _fetch_sec_insider_bulk_workaround(
    kwargs: Mapping[str, Any],
    *,
    cache_dir: Path,
    page_limiter: Any | None,
    show_progress: bool,
) -> list[dict[str, Any]]:
    """Flatten one official SEC Form 3/4/5 quarterly structured dataset."""
    from zipfile import ZipFile

    year = int(kwargs["year"])
    quarter = int(kwargs["quarter"])
    start = date.fromisoformat(str(kwargs["start_date"]))
    end = date.fromisoformat(str(kwargs["end_date"]))
    zip_path, source_url = _fetch_sec_insider_quarter_zip(
        year,
        quarter,
        cache_dir=cache_dir,
        page_limiter=page_limiter,
        show_progress=show_progress,
    )

    with _SEC_INSIDER_BULK_SEMAPHORE, ZipFile(zip_path) as archive:
        members = _sec_insider_zip_members(zip_path, deep=False)
        submissions: dict[str, dict[str, str]] = {}
        for row in _sec_insider_tsv_rows(
            archive,
            "SUBMISSION.tsv",
            members["SUBMISSION.tsv"],
            show_progress=show_progress,
        ):
            filing_date = _sec_insider_date(row.get("FILING_DATE"))
            form = row.get("DOCUMENT_TYPE", "").upper()
            if (
                filing_date
                and start <= date.fromisoformat(filing_date) <= end
                and form in {"3", "3/A", "4", "4/A", "5", "5/A"}
            ):
                submissions[row["ACCESSION_NUMBER"]] = row

        owners: dict[str, list[dict[str, str]]] = {}
        for row in _sec_insider_tsv_rows(
            archive,
            "REPORTINGOWNER.tsv",
            members["REPORTINGOWNER.tsv"],
            show_progress=show_progress,
        ):
            accession = row.get("ACCESSION_NUMBER", "")
            if accession in submissions:
                owners.setdefault(accession, []).append(row)

        footnotes: dict[str, dict[str, str]] = {}
        for row in _sec_insider_tsv_rows(
            archive,
            "FOOTNOTES.tsv",
            members["FOOTNOTES.tsv"],
            show_progress=show_progress,
        ):
            accession = row.get("ACCESSION_NUMBER", "")
            if accession in submissions and row.get("FOOTNOTE_ID"):
                footnotes.setdefault(accession, {})[row["FOOTNOTE_ID"]] = row.get(
                    "FOOTNOTE_TXT", ""
                )

        signatures: dict[str, list[dict[str, str]]] = {}
        for row in _sec_insider_tsv_rows(
            archive,
            "OWNER_SIGNATURE.tsv",
            members["OWNER_SIGNATURE.tsv"],
            show_progress=show_progress,
        ):
            accession = row.get("ACCESSION_NUMBER", "")
            if accession in submissions:
                signatures.setdefault(accession, []).append(row)

        from openbb_sec.models.insider_trading import (
            TIMELINESS_MAP,
            TRANSACTION_CODE_MAP,
        )

        records: list[dict[str, Any]] = []
        table_specs = (
            ("NONDERIV_TRANS.tsv", "non_derivative_transaction", False, True),
            ("NONDERIV_HOLDING.tsv", "non_derivative_holding", False, False),
            ("DERIV_TRANS.tsv", "derivative_transaction", True, True),
            ("DERIV_HOLDING.tsv", "derivative_holding", True, False),
        )
        for member, record_type, derivative, transaction in table_specs:
            for row in _sec_insider_tsv_rows(
                archive,
                member,
                members[member],
                show_progress=show_progress,
            ):
                accession = row.get("ACCESSION_NUMBER", "")
                submission = submissions.get(accession)
                if submission is None:
                    continue
                owner_rows = owners.get(accession, [])
                relationships = _unique_join(
                    item.get("RPTOWNER_RELATIONSHIP") for item in owner_rows
                )
                relationship_text = str(relationships or "").lower()
                signature_rows = signatures.get(accession, [])
                footnote_ids = list(
                    dict.fromkeys(
                        match
                        for key, value in row.items()
                        if key.endswith("_FN") and value
                        for match in re.findall(r"F\d+", value, flags=re.IGNORECASE)
                    )
                )
                footnote = _unique_join(
                    footnotes.get(accession, {}).get(identifier)
                    for identifier in footnote_ids
                )
                transaction_code = row.get("TRANS_CODE") if transaction else None
                timeliness_code = row.get("TRANS_TIMELINESS") if transaction else None
                shares = (
                    _sec_insider_float(row.get("TRANS_SHARES")) if transaction else None
                )
                price = (
                    _sec_insider_float(row.get("TRANS_PRICEPERSHARE"))
                    if transaction
                    else None
                )
                transaction_value = _sec_insider_float(row.get("TRANS_TOTAL_VALUE"))
                if (
                    transaction_value is None
                    and shares is not None
                    and price is not None
                ):
                    transaction_value = shares * price
                ownership_code = row.get("DIRECT_INDIRECT_OWNERSHIP", "")
                acquisition_code = (
                    row.get("TRANS_ACQUIRED_DISP_CD", "") if transaction else ""
                )
                company_cik = submission.get("ISSUERCIK", "")
                filing_url = (
                    "https://www.sec.gov/Archives/edgar/data/"
                    f"{company_cik.lstrip('0')}/{accession.replace('-', '')}/"
                    f"{accession}-index.html"
                )
                security_key = next((key for key in row if key.endswith("_SK")), "")
                records.append(
                    {
                        "accession_number": accession,
                        "record_key": row.get(security_key) if security_key else None,
                        "record_type": record_type,
                        "is_derivative": derivative,
                        "filing_date": _sec_insider_date(submission.get("FILING_DATE")),
                        "period_of_report": _sec_insider_date(
                            submission.get("PERIOD_OF_REPORT")
                        ),
                        "date_of_original_submission": _sec_insider_date(
                            submission.get("DATE_OF_ORIG_SUB")
                        ),
                        "symbol": submission.get("ISSUERTRADINGSYMBOL") or None,
                        "company_name": submission.get("ISSUERNAME") or None,
                        "company_cik": company_cik or None,
                        "form": row.get("TRANS_FORM_TYPE")
                        or submission.get("DOCUMENT_TYPE")
                        or None,
                        "owner_name": _unique_join(
                            item.get("RPTOWNERNAME") for item in owner_rows
                        ),
                        "owner_cik": _unique_join(
                            item.get("RPTOWNERCIK") for item in owner_rows
                        ),
                        "owner_title": _unique_join(
                            item.get("RPTOWNER_TITLE") for item in owner_rows
                        ),
                        "owner_relationship": relationships,
                        "director": "director" in relationship_text,
                        "officer": "officer" in relationship_text,
                        "ten_percent_owner": (
                            "ten percent" in relationship_text
                            or "10 percent" in relationship_text
                            or "10%" in relationship_text
                        ),
                        "other": "other" in relationship_text,
                        "other_text": _unique_join(
                            item.get("RPTOWNER_TXT") for item in owner_rows
                        ),
                        "security_type": row.get("SECURITY_TITLE") or None,
                        "transaction_date": (
                            _sec_insider_date(row.get("TRANS_DATE"))
                            if transaction
                            else None
                        ),
                        "deemed_execution_date": _sec_insider_date(
                            row.get("DEEMED_EXECUTION_DATE")
                        ),
                        "transaction_type": (
                            TRANSACTION_CODE_MAP.get(transaction_code, transaction_code)
                            if transaction_code
                            else None
                        ),
                        "transaction_code": transaction_code or None,
                        "transaction_timeliness": (
                            TIMELINESS_MAP.get(timeliness_code or "Empty")
                            if transaction
                            else None
                        ),
                        "acquisition_or_disposition": (
                            "Acquisition"
                            if acquisition_code == "A"
                            else "Disposition"
                            if acquisition_code == "D"
                            else acquisition_code or None
                        ),
                        "securities_transacted": shares,
                        "transaction_price": price,
                        "transaction_value": transaction_value,
                        "securities_owned": _sec_insider_float(
                            row.get("SHRS_OWND_FOLWNG_TRANS")
                        ),
                        "value_owned": _sec_insider_float(
                            row.get("VALU_OWND_FOLWNG_TRANS")
                        ),
                        "ownership_type": (
                            "Direct"
                            if ownership_code == "D"
                            else "Indirect"
                            if ownership_code == "I"
                            else ownership_code or None
                        ),
                        "nature_of_ownership": row.get("NATURE_OF_OWNERSHIP") or None,
                        "conversion_exercise_price": _sec_insider_float(
                            row.get("CONV_EXERCISE_PRICE")
                        ),
                        "exercise_date": _sec_insider_date(
                            row.get("EXERCISE_DATE") or row.get("EXCERCISE_DATE")
                        ),
                        "expiration_date": _sec_insider_date(
                            row.get("EXPIRATION_DATE")
                        ),
                        "underlying_security_title": row.get("UNDLYNG_SEC_TITLE")
                        or None,
                        "underlying_security_shares": _sec_insider_float(
                            row.get("UNDLYNG_SEC_SHARES")
                        ),
                        "underlying_security_value": _sec_insider_float(
                            row.get("UNDLYNG_SEC_VALUE")
                        ),
                        "equity_swap_involved": _sec_insider_bool(
                            row.get("EQUITY_SWAP_INVOLVED")
                        ),
                        "footnote": footnote,
                        "remarks": submission.get("REMARKS") or None,
                        "aff10b5one": _sec_insider_bool(submission.get("AFF10B5ONE")),
                        "signature_name": _unique_join(
                            item.get("OWNERSIGNATURENAME") for item in signature_rows
                        ),
                        "signature_date": _unique_join(
                            _sec_insider_date(item.get("OWNERSIGNATUREDATE"))
                            for item in signature_rows
                        ),
                        "filing_url": filing_url,
                        "source_quarter": f"{year}Q{quarter}",
                        "source_dataset_url": source_url,
                    }
                )
        return records


def _write_gzip_bytes(path: Path, content: bytes) -> None:
    """Atomically persist immutable SEC source bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with gzip.open(temporary, "wb", compresslevel=6) as stream:
            stream.write(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _fetch_sec_cached_bytes(
    url: str,
    cache_path: Path,
    *,
    page_limiter: Any | None,
) -> bytes:
    """Fetch one SEC object once, with cache hits consuming no rate ticket."""
    from urllib.request import Request, urlopen

    lock_key = "insider-object:" + hashlib.sha256(url.encode("utf-8")).hexdigest()
    with _sec_companyfacts_lock(lock_key):
        if cache_path.exists():
            try:
                with gzip.open(cache_path, "rb") as stream:
                    cached = stream.read()
                if cached:
                    return cached
            except (OSError, EOFError):
                quarantine = cache_path.with_name(
                    f"{cache_path.name}.invalid.{int(time.time())}"
                )
                cache_path.replace(quarantine)
        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        request = Request(url, headers=_sec_archive_request_headers())
        with urlopen(request, timeout=90) as response:
            content = response.read()
        if not content:
            raise RuntimeError(f"SEC returned an empty response for {url}")
        _write_gzip_bytes(cache_path, content)
        return content


def _sec_submission_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Transpose SEC's column-oriented submissions payload without pandas."""
    if "filings" in payload and isinstance(payload.get("filings"), Mapping):
        payload = payload["filings"].get("recent") or {}  # type: ignore[index]
    if not isinstance(payload, Mapping):
        return []
    columns = {
        str(key): value for key, value in payload.items() if isinstance(value, list)
    }
    row_count = max((len(value) for value in columns.values()), default=0)
    return [
        {
            key: values[index] if index < len(values) else None
            for key, values in columns.items()
        }
        for index in range(row_count)
    ]


def _normalize_sec_form4_ownership_document(
    ownership: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate and normalize nullable mappings at the Form 4 XML boundary.

    Empty XML elements are decoded as ``None`` by xmltodict.  OpenBB's Form 4
    parser assumes several of those elements are mappings and calls ``.get``
    unconditionally.  Convert only semantically empty/null containers to the
    empty shape expected by the parser and record every recovery.  Unexpected
    non-null shapes still fail closed instead of silently discarding data.
    """
    if not isinstance(ownership, Mapping):
        raise ProviderResponseShapeError(
            "SEC Form 4 ownershipDocument must be a mapping"
        )
    document = deepcopy(dict(ownership))
    recoveries: list[dict[str, Any]] = []

    def recovered(path: str, error_type: str) -> None:
        recoveries.append(
            {
                "field": path,
                "error_type": error_type,
                "invalid_value": None,
            }
        )

    def require_mapping(value: Any, path: str) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ProviderResponseShapeError(
                f"SEC Form 4 expected mapping at {path}, got {type(value).__name__}"
            )
        return dict(value)

    def normalize_optional_mapping(
        parent: dict[str, Any], key: str, path: str
    ) -> dict[str, Any] | None:
        if key not in parent:
            return None
        value = parent[key]
        if value is None:
            normalized: dict[str, Any] = {}
            parent[key] = normalized
            recovered(path, "null_mapping")
            return normalized
        normalized = require_mapping(value, path)
        parent[key] = normalized
        return normalized

    issuer = normalize_optional_mapping(document, "issuer", "issuer")
    if issuer is not None and issuer.get("issuerTradingSymbol") is None:
        if "issuerTradingSymbol" in issuer:
            recovered("issuer.issuerTradingSymbol", "null_string")
        issuer["issuerTradingSymbol"] = ""

    owner_value = document.get("reportingOwner")
    owners: list[tuple[dict[str, Any], str]] = []
    if owner_value is None:
        if "reportingOwner" in document:
            document["reportingOwner"] = {}
            recovered("reportingOwner", "null_mapping")
            owners.append((document["reportingOwner"], "reportingOwner"))
    elif isinstance(owner_value, Mapping):
        normalized_owner = dict(owner_value)
        document["reportingOwner"] = normalized_owner
        owners.append((normalized_owner, "reportingOwner"))
    elif isinstance(owner_value, list):
        if not owner_value:
            document["reportingOwner"] = {}
            recovered("reportingOwner", "empty_list_mapping")
            owners.append((document["reportingOwner"], "reportingOwner"))
        else:
            normalized_owners: list[dict[str, Any]] = []
            for index, item in enumerate(owner_value):
                path = f"reportingOwner[{index}]"
                if item is None:
                    normalized_owner = {}
                    recovered(path, "null_mapping")
                else:
                    normalized_owner = require_mapping(item, path)
                normalized_owners.append(normalized_owner)
                owners.append((normalized_owner, path))
            document["reportingOwner"] = normalized_owners
    else:
        raise ProviderResponseShapeError(
            "SEC Form 4 expected mapping/list at reportingOwner, got "
            f"{type(owner_value).__name__}"
        )

    multiple_owners = isinstance(document.get("reportingOwner"), list)
    for owner, owner_path in owners:
        owner_id = normalize_optional_mapping(
            owner,
            "reportingOwnerId",
            f"{owner_path}.reportingOwnerId",
        )
        normalize_optional_mapping(
            owner,
            "reportingOwnerRelationship",
            f"{owner_path}.reportingOwnerRelationship",
        )
        if owner_id is None or not multiple_owners:
            continue
        for key in ("rptOwnerName", "rptOwnerCik"):
            value = owner_id.get(key)
            if value is None:
                error_type = "null_string" if key in owner_id else "missing_string"
                owner_id[key] = ""
                recovered(f"{owner_path}.reportingOwnerId.{key}", error_type)
            elif not isinstance(value, str):
                raise ProviderResponseShapeError(
                    "SEC Form 4 expected string at "
                    f"{owner_path}.reportingOwnerId.{key}, got "
                    f"{type(value).__name__}"
                )

    signature = document.get("ownerSignature")
    if signature is None:
        if "ownerSignature" in document:
            document["ownerSignature"] = {}
            recovered("ownerSignature", "null_mapping")
    elif isinstance(signature, Mapping):
        document["ownerSignature"] = dict(signature)
    elif isinstance(signature, list):
        normalized_signatures: list[dict[str, Any]] = []
        for index, item in enumerate(signature):
            path = f"ownerSignature[{index}]"
            if item is None:
                normalized_signatures.append({})
                recovered(path, "null_mapping")
            else:
                normalized_signatures.append(require_mapping(item, path))
        document["ownerSignature"] = normalized_signatures
    else:
        raise ProviderResponseShapeError(
            "SEC Form 4 expected mapping/list at ownerSignature, got "
            f"{type(signature).__name__}"
        )

    footnotes = normalize_optional_mapping(document, "footnotes", "footnotes")
    if footnotes is not None and "footnote" in footnotes:
        footnote_value = footnotes["footnote"]
        if footnote_value is None:
            footnotes["footnote"] = []
            recovered("footnotes.footnote", "null_list")
        else:
            footnote_items = (
                [footnote_value]
                if isinstance(footnote_value, Mapping)
                else footnote_value
            )
            if not isinstance(footnote_items, list):
                raise ProviderResponseShapeError(
                    "SEC Form 4 expected mapping/list at footnotes.footnote, got "
                    f"{type(footnote_value).__name__}"
                )
            normalized_footnotes: list[dict[str, Any]] = []
            for index, item in enumerate(footnote_items):
                path = f"footnotes.footnote[{index}]"
                if item is None:
                    recovered(path, "dropped_null_mapping")
                    continue
                normalized_item = require_mapping(item, path)
                for key in ("@id", "#text"):
                    if normalized_item.get(key) is None:
                        error_type = (
                            "null_string"
                            if key in normalized_item
                            else "missing_string"
                        )
                        normalized_item[key] = ""
                        recovered(f"{path}.{key}", error_type)
                normalized_footnotes.append(normalized_item)
            footnotes["footnote"] = (
                normalized_footnotes[0]
                if isinstance(footnote_value, Mapping) and normalized_footnotes
                else normalized_footnotes
            )

    def normalize_footnote_references(value: Any, path: str) -> None:
        if isinstance(value, Mapping):
            mapping = value if isinstance(value, dict) else dict(value)
            for key, item in list(mapping.items()):
                item_path = f"{path}.{key}"
                if key == "footnoteId":
                    if item is None:
                        mapping.pop(key)
                        recovered(item_path, "dropped_null_mapping")
                    elif isinstance(item, Mapping):
                        normalized_reference = dict(item)
                        mapping[key] = normalized_reference
                        if normalized_reference.get("@id") is None:
                            error_type = (
                                "null_string"
                                if "@id" in normalized_reference
                                else "missing_string"
                            )
                            normalized_reference["@id"] = ""
                            recovered(f"{item_path}.@id", error_type)
                    elif isinstance(item, list):
                        normalized_references: list[dict[str, Any]] = []
                        for index, reference in enumerate(item):
                            reference_path = f"{item_path}[{index}]"
                            if reference is None:
                                recovered(reference_path, "dropped_null_mapping")
                                continue
                            normalized_reference = require_mapping(
                                reference, reference_path
                            )
                            if normalized_reference.get("@id") is None:
                                error_type = (
                                    "null_string"
                                    if "@id" in normalized_reference
                                    else "missing_string"
                                )
                                normalized_reference["@id"] = ""
                                recovered(f"{reference_path}.@id", error_type)
                            normalized_references.append(normalized_reference)
                        mapping[key] = normalized_references
                    else:
                        raise ProviderResponseShapeError(
                            "SEC Form 4 expected mapping/list at "
                            f"{item_path}, got {type(item).__name__}"
                        )
                else:
                    normalize_footnote_references(item, item_path)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                normalize_footnote_references(item, f"{path}[{index}]")

    def normalize_table_rows(parent: dict[str, Any], key: str, path: str) -> None:
        if key not in parent or parent[key] is None:
            return
        raw_rows = parent[key]
        rows = [raw_rows] if isinstance(raw_rows, Mapping) else raw_rows
        if isinstance(rows, str):
            return
        if not isinstance(rows, list):
            raise ProviderResponseShapeError(
                f"SEC Form 4 expected mapping/list at {path}, got "
                f"{type(raw_rows).__name__}"
            )
        normalized_rows: list[Any] = []
        for index, item in enumerate(rows):
            item_path = f"{path}[{index}]"
            if item is None:
                recovered(item_path, "dropped_null_mapping")
                continue
            if isinstance(item, str):
                normalized_rows.append(item)
                continue
            normalized_item = require_mapping(item, item_path)
            if (
                "transactionCoding" in normalized_item
                and normalized_item["transactionCoding"] is None
            ):
                normalized_item["transactionCoding"] = {}
                recovered(
                    f"{path}.transactionCoding"
                    if isinstance(raw_rows, Mapping)
                    else f"{item_path}.transactionCoding",
                    "null_mapping",
                )
            elif "transactionCoding" in normalized_item:
                normalized_item["transactionCoding"] = require_mapping(
                    normalized_item["transactionCoding"],
                    f"{item_path}.transactionCoding",
                )
            normalize_footnote_references(normalized_item, item_path)
            normalized_rows.append(normalized_item)
        parent[key] = (
            normalized_rows[0]
            if isinstance(raw_rows, Mapping) and normalized_rows
            else normalized_rows
        )

    for table_name, row_names in (
        (
            "nonDerivativeTable",
            ("nonDerivativeTransaction", "nonDerivativeHolding"),
        ),
        ("derivativeTable", ("derivativeTransaction", "derivativeHolding")),
    ):
        table = normalize_optional_mapping(document, table_name, table_name)
        if table is None:
            continue
        for row_name in row_names:
            normalize_table_rows(table, row_name, f"{table_name}.{row_name}")
    normalize_table_rows(document, "derivativeSecurity", "derivativeSecurity")
    return document, recoveries


def _fetch_sec_insider_range_workaround(
    kwargs: Mapping[str, Any],
    *,
    cache_dir: Path,
    page_limiter: Any | None,
    show_progress: bool,
) -> list[dict[str, Any]]:
    """Fetch only SEC submissions shards intersecting one symbol/date range."""
    import asyncio
    from urllib.error import HTTPError

    import xmltodict
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.utils.form4 import clean_xml, field_map, parse_form_4_data

    symbol = str(kwargs.get("symbol") or "").strip().upper()
    start = date.fromisoformat(str(kwargs["start_date"]))
    end = date.fromisoformat(str(kwargs["end_date"]))
    cik = _sec_symbol_cik_map(page_limiter).get(symbol.replace(".", "-"))
    if not cik:
        raise EmptyDataError(f"No CIK was found for symbol: {symbol}")
    cik = str(cik).lstrip("0").zfill(10)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    payload = json.loads(
        _fetch_sec_cached_bytes(
            submissions_url,
            cache_dir
            / "submissions"
            / f"as_of={end.isoformat()}"
            / f"CIK{cik}.json.gz",
            page_limiter=page_limiter,
        )
    )
    filing_rows = _sec_submission_rows(payload)
    files = (
        payload.get("filings", {}).get("files", [])
        if isinstance(payload, Mapping) and isinstance(payload.get("filings"), Mapping)
        else []
    )
    shard_progress = tqdm(
        files,
        total=len(files),
        desc=f"sec:insider {symbol} select shards",
        unit="shard",
        position=2,
        leave=False,
        disable=not show_progress or not files,
    )
    try:
        for file_info in shard_progress:
            if not isinstance(file_info, Mapping) or not file_info.get("name"):
                continue
            filing_from = str(file_info.get("filingFrom") or "0001-01-01")[:10]
            filing_to = str(file_info.get("filingTo") or "9999-12-31")[:10]
            if filing_to < start.isoformat() or filing_from > end.isoformat():
                continue
            name = str(file_info["name"])
            shard_url = f"https://data.sec.gov/submissions/{name}"
            shard_payload = json.loads(
                _fetch_sec_cached_bytes(
                    shard_url,
                    cache_dir / "submission_shards" / f"{name}.gz",
                    page_limiter=page_limiter,
                )
            )
            filing_rows.extend(_sec_submission_rows(shard_payload))
    finally:
        shard_progress.close()

    selected: list[dict[str, Any]] = []
    seen_accessions: set[str] = set()
    for row in filing_rows:
        accession = str(row.get("accessionNumber") or "")
        filing_date = str(row.get("filingDate") or "")[:10]
        form = str(row.get("form") or "").upper()
        primary_document = str(row.get("primaryDocument") or "")
        if (
            accession
            and accession not in seen_accessions
            and start.isoformat() <= filing_date <= end.isoformat()
            and form in {"3", "3/A", "4", "4/A", "5", "5/A"}
            and primary_document.lower().endswith(".xml")
        ):
            seen_accessions.add(accession)
            selected.append(row)
    selected.sort(key=lambda row: str(row.get("filingDate") or ""))
    if not selected:
        raise EmptyDataError(
            f"No SEC Form 3/4/5 filings found for {symbol} from {start} to {end}"
        )

    records: list[dict[str, Any]] = []
    progress = tqdm(
        selected,
        total=len(selected),
        desc=f"sec:insider {symbol} filings",
        unit="filing",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for filing in progress:
            accession = str(filing["accessionNumber"])
            primary_document = str(filing["primaryDocument"])
            # SEC submissions often prefixes ownership XML with an XSL
            # rendering directory (for example xslF345X06/form4.xml).  That
            # URL returns transformed HTML; the raw XML lives beside the XSL
            # directory at the accession root, which is also what OpenBB's
            # Form 4 helper removes before parsing.
            raw_primary_document = primary_document.rsplit("/", 1)[-1]
            filing_url = (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{cik.lstrip('0')}/{accession.replace('-', '')}/"
                f"{raw_primary_document}"
            )
            try:
                xml_content = _fetch_sec_cached_bytes(
                    filing_url,
                    cache_dir / "filings" / cik / f"{accession}.xml.gz",
                    page_limiter=page_limiter,
                )
            except HTTPError as exc:
                if exc.code == 404:
                    progress.set_postfix(missing=accession, refresh=False)
                    continue
                raise
            parsed_xml = xmltodict.parse(
                clean_xml(xml_content.decode("utf-8", errors="replace"))
            )
            ownership, validation_recoveries = _normalize_sec_form4_ownership_document(
                parsed_xml.get("ownershipDocument") or {}
            )
            parsed_rows = asyncio.run(parse_form_4_data(ownership))
            for parsed_row in parsed_rows:
                if not isinstance(parsed_row, Mapping):
                    raise ProviderResponseShapeError(
                        "SEC Form 4 parser returned a non-mapping row: "
                        f"{type(parsed_row).__name__}"
                    )
                normalized = {
                    field_map.get(str(key), str(key)): _normalize_scalar(value)
                    for key, value in parsed_row.items()
                }
                for bool_field in (
                    "director",
                    "officer",
                    "ten_percent_owner",
                    "other",
                ):
                    normalized[bool_field] = _sec_insider_bool(
                        normalized.get(bool_field)
                    )
                transaction_code = str(normalized.get("transaction_type") or "")
                from openbb_sec.models.insider_trading import (
                    TIMELINESS_MAP,
                    TRANSACTION_CODE_MAP,
                )

                if transaction_code:
                    normalized["transaction_code"] = transaction_code
                    normalized["transaction_type"] = TRANSACTION_CODE_MAP.get(
                        transaction_code, transaction_code
                    )
                timeliness = str(normalized.get("transaction_timeliness") or "")
                normalized["transaction_timeliness"] = TIMELINESS_MAP.get(
                    timeliness or "Empty"
                )
                acquisition = str(normalized.get("acquisition_or_disposition") or "")
                normalized["acquisition_or_disposition"] = (
                    "Acquisition"
                    if acquisition == "A"
                    else "Disposition"
                    if acquisition == "D"
                    else acquisition or None
                )
                ownership_type = str(normalized.get("ownership_type") or "")
                normalized["ownership_type"] = (
                    "Direct"
                    if ownership_type == "D"
                    else "Indirect"
                    if ownership_type == "I"
                    else ownership_type or None
                )
                normalized.update(
                    {
                        "accession_number": accession,
                        "filing_url": filing_url,
                        "source": "SEC EDGAR ownership filing XML",
                    }
                )
                if validation_recoveries:
                    normalized["openbb_validation_recoveries"] = _canonical_json(
                        validation_recoveries
                    )
                records.append(normalized)
            progress.set_postfix(rows=len(records), refresh=False)
    finally:
        progress.close()
    if not records:
        raise EmptyDataError(
            f"No SEC ownership transaction rows found for {symbol} from {start} to {end}"
        )
    return records


_SEC_SYMBOL_CIK_LOCK = threading.Lock()
_SEC_SYMBOL_CIK_CACHE: dict[str, str] | None = None
_SEC_CIK_SYMBOL_CACHE: dict[str, tuple[str, ...]] | None = None
_SEC_FUND_SERIES_CACHE: dict[str, str] | None = None


def _sec_symbol_cik_map(page_limiter: Any | None = None) -> dict[str, str]:
    """Load SEC stock/fund identifiers once so each archive task is one request."""
    global _SEC_SYMBOL_CIK_CACHE, _SEC_CIK_SYMBOL_CACHE, _SEC_FUND_SERIES_CACHE
    with _SEC_SYMBOL_CIK_LOCK:
        if _SEC_SYMBOL_CIK_CACHE is not None:
            return _SEC_SYMBOL_CIK_CACHE

        # OpenBB provider initialization is expensive and currently emits
        # upstream deprecation warnings. Do not initialize it when the shared
        # catalog is already populated (including tests and warm workers).
        import asyncio

        from openbb_sec.utils.helpers import get_all_companies, get_mf_and_etf_map

        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        with _suppress_sec_helper_pacing():
            companies = asyncio.run(get_all_companies(use_cache=True))
        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        with _suppress_sec_helper_pacing():
            funds = asyncio.run(get_mf_and_etf_map(use_cache=True))
        mapping: dict[str, str] = {}
        for frame in (companies, funds):
            if "symbol" not in frame.columns or "cik" not in frame.columns:
                continue
            for symbol, cik in zip(frame["symbol"], frame["cik"], strict=True):
                normalized = str(symbol).strip().upper().replace(".", "-")
                if normalized and str(cik).strip():
                    mapping[normalized] = str(cik).strip().lstrip("0").zfill(10)
        series_mapping: dict[str, str] = {}
        if "symbol" in funds.columns and "seriesId" in funds.columns:
            for symbol, series_id in zip(
                funds["symbol"], funds["seriesId"], strict=True
            ):
                normalized = str(symbol).strip().upper().replace(".", "-")
                if normalized and str(series_id).strip():
                    series_mapping[normalized] = str(series_id).strip()
        _SEC_SYMBOL_CIK_CACHE = mapping
        reverse_mapping: dict[str, list[str]] = {}
        for symbol, cik in mapping.items():
            reverse_mapping.setdefault(cik, []).append(symbol)
        _SEC_CIK_SYMBOL_CACHE = {
            cik: tuple(sorted(symbols)) for cik, symbols in reverse_mapping.items()
        }
        _SEC_FUND_SERIES_CACHE = series_mapping
        return mapping


def _fetch_sec_identifier_map_workaround(
    endpoint: str,
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None,
) -> list[dict[str, str]]:
    """Project symbol/CIK tasks from one shared official SEC catalog load."""
    mapping = _sec_symbol_cik_map(page_limiter)
    query = str(kwargs.get("query") or kwargs.get("symbol") or "").strip().upper()
    if endpoint == "regulators.sec.cik_map":
        symbol = query.replace(".", "-")
        cik = mapping.get(symbol)
        if not symbol or not cik:
            from openbb_core.provider.utils.errors import EmptyDataError

            raise EmptyDataError(f"No SEC CIK was found for symbol: {query}")
        return [{"symbol": symbol, "cik": cik}]
    if endpoint != "regulators.sec.symbol_map":
        raise ValueError(f"Unsupported SEC identifier-map endpoint: {endpoint}")
    cik = query.lstrip("0").zfill(10)
    reverse = _SEC_CIK_SYMBOL_CACHE
    if reverse is None:
        rebuilt: dict[str, list[str]] = {}
        for symbol, item_cik in mapping.items():
            rebuilt.setdefault(item_cik, []).append(symbol)
        reverse = {
            item_cik: tuple(sorted(symbols)) for item_cik, symbols in rebuilt.items()
        }
    symbols = reverse.get(cik, ())
    if not symbols:
        from openbb_core.provider.utils.errors import EmptyDataError

        raise EmptyDataError(f"No SEC symbol was found for CIK: {query}")
    return [{"symbol": symbol, "cik": cik} for symbol in symbols]


def _sec_companyfacts_lock(cik: str) -> threading.Lock:
    """Return one process-wide single-flight lock for an SEC filer."""
    with _SEC_COMPANYFACTS_LOCKS_GUARD:
        return _SEC_COMPANYFACTS_LOCKS.setdefault(cik, threading.Lock())


def _read_sec_companyfacts_cache(path: Path, cik: str) -> dict[str, Any] | None:
    """Read one validated durable SEC companyfacts response."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, EOFError, ValueError, TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping) or payload.get("schema_version") != 1:
        return None
    if str(payload.get("cik") or "") != cik:
        return None
    if payload.get("status") == "empty":
        return {"_archive_cache_status": "empty", "cik": cik}
    response = payload.get("response")
    if not isinstance(response, Mapping) or not response.get("facts"):
        return None
    normalized = dict(response)
    # A small number of valid SEC companyfacts responses contain ``facts``
    # but omit their redundant top-level CIK.  The requested CIK is the
    # authoritative cache identity; without this normalization unrelated
    # filers would all share the standardized key ``0000000000``.
    if not normalized.get("cik"):
        normalized["cik"] = cik
    return normalized


def _write_sec_companyfacts_cache(
    path: Path,
    cik: str,
    response: Mapping[str, Any] | None,
) -> None:
    """Atomically persist one raw SEC response or authoritative empty marker."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "cik": cik,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "status": "success" if response is not None else "empty",
    }
    if response is not None:
        payload["response"] = dict(response)
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as stream:
            json.dump(payload, stream, ensure_ascii=False, separators=(",", ":"))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _fetch_sec_companyfacts_response(
    cik: str,
    *,
    page_limiter: Any | None,
    cache_dir: Path | None,
    use_cache: bool,
) -> dict[str, Any]:
    """Fetch one complete filer response with durable, exact-key single-flight.

    SEC's financial-statement routes are local projections of this same raw
    object.  The cache key is the exact CIK inside an archive-end-date namespace,
    so annual/quarterly and statement/growth tasks share one upstream request
    without making a cache hit look like a limiter claim.
    """
    import asyncio

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.utils.frames import fetch_data

    cik_text = str(cik).lstrip("0").zfill(10)
    cache_path = (
        cache_dir / cik_text[:4] / f"CIK{cik_text}.json.gz"
        if cache_dir is not None
        else None
    )
    lock = _sec_companyfacts_lock(cik_text)
    with lock:
        cached = (
            _read_sec_companyfacts_cache(cache_path, cik_text)
            if cache_path is not None and cache_path.exists()
            else None
        )
        if cached is not None:
            if cached.get("_archive_cache_status") == "empty":
                raise EmptyDataError(f"No company facts were found for CIK: {cik_text}")
            return cached

        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_text}.json"

        async def _extract() -> dict[str, Any]:
            try:
                # The archive's gzip file is the durable cache.  Do not open
                # OpenBB's process-global SQLite cache from concurrent loops.
                response = await fetch_data(
                    url,
                    False if cache_dir is not None else use_cache,
                    False,
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    if cache_path is not None:
                        _write_sec_companyfacts_cache(cache_path, cik_text, None)
                    raise EmptyDataError(
                        f"No company facts were found for CIK: {cik_text}"
                    ) from exc
                raise
            if not isinstance(response, Mapping) or not response.get("facts"):
                if cache_path is not None:
                    _write_sec_companyfacts_cache(cache_path, cik_text, None)
                raise EmptyDataError(f"No company facts were found for CIK: {cik_text}")
            normalized = dict(response)
            if not normalized.get("cik"):
                normalized["cik"] = cik_text
            return normalized

        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        with _suppress_sec_helper_pacing():
            response = asyncio.run(_extract())
        if cache_path is not None:
            _write_sec_companyfacts_cache(cache_path, cik_text, response)
        return response


def _sec_companyfacts_ciks_for_symbol(
    symbol: str, *, page_limiter: Any | None
) -> tuple[str, tuple[str, ...]]:
    """Resolve the exact CIK set without loading any large companyfacts object."""
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.utils.company_facts import MULTI_CIK_TICKERS

    symbol_upper = symbol.strip().upper()
    if not symbol_upper:
        raise ValueError("symbol is required for SEC company facts")
    cik_values = MULTI_CIK_TICKERS.get(symbol_upper)
    if cik_values is None:
        mapped = _sec_symbol_cik_map(page_limiter).get(symbol_upper.replace(".", "-"))
        cik_values = [mapped] if mapped else []
    normalized = tuple(
        dict.fromkeys(
            str(cik).lstrip("0").zfill(10)
            for cik in cik_values
            if str(cik or "").strip()
        )
    )
    if not normalized:
        raise EmptyDataError(f"No CIK was found for symbol: {symbol_upper}")
    return symbol_upper, normalized


def _sec_companyfacts_cache_path(cache_dir: Path, cik: str) -> Path:
    cik_text = str(cik).lstrip("0").zfill(10)
    return cache_dir / cik_text[:4] / f"CIK{cik_text}.json.gz"


def _sec_companyfacts_responses_for_symbol(
    symbol: str,
    *,
    page_limiter: Any | None,
    cache_dir: Path | None,
    use_cache: bool,
) -> tuple[str, list[dict[str, Any]]]:
    """Resolve every CIK required for a ticker's complete SEC history."""
    symbol_upper, cik_values = _sec_companyfacts_ciks_for_symbol(
        symbol, page_limiter=page_limiter
    )
    responses = [
        _fetch_sec_companyfacts_response(
            str(cik),
            page_limiter=page_limiter,
            cache_dir=cache_dir,
            use_cache=use_cache,
        )
        for cik in cik_values
    ]
    return symbol_upper, responses


@lru_cache(maxsize=1)
def _sec_standardized_cache_version() -> str:
    """Bind durable standardized rows to the installed SEC implementation."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("openbb-sec")
    except PackageNotFoundError:
        return "unknown"


def _sec_standardized_disk_cache_path(
    cache_dir: Path,
    key: tuple[str, tuple[str, ...], str, bool, bool],
) -> Path:
    cache_identity = {
        "schema_version": _SEC_STANDARDIZED_DISK_CACHE_SCHEMA_VERSION,
        "openbb_sec_version": _sec_standardized_cache_version(),
        "key": [key[0], list(key[1]), key[2], key[3], key[4]],
    }
    digest = hashlib.sha256(_canonical_json(cache_identity).encode("utf-8")).hexdigest()
    return cache_dir / "_standardized" / digest[:2] / f"{digest}.json.gz"


@contextmanager
def _interprocess_cache_lock(path: Path) -> Iterator[None]:
    """Serialize one cache build across spawned SEC projection processes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        try:
            import fcntl
        except ImportError:  # pragma: no cover - non-POSIX fallback
            yield
            return
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _read_sec_standardized_disk_cache(
    path: Path,
    key: tuple[str, tuple[str, ...], str, bool, bool],
) -> Any | None:
    """Load one validated, versioned standardized companyfacts object."""
    from openbb_sec.utils.company_facts import (
        StandardizedStatements,
        ValidationWarning,
    )

    expected_key = [key[0], list(key[1]), key[2], key[3], key[4]]
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            payload = json.load(stream)
        if not isinstance(payload, Mapping):
            return None
        if (
            payload.get("schema_version") != _SEC_STANDARDIZED_DISK_CACHE_SCHEMA_VERSION
            or payload.get("openbb_sec_version") != _sec_standardized_cache_version()
            or payload.get("key") != expected_key
        ):
            return None
        statements = payload.get("statements")
        if not isinstance(statements, Mapping):
            return None
        statement_names = (
            "income_statement",
            "balance_sheet",
            "cash_flow",
        )
        if any(not isinstance(statements.get(name), list) for name in statement_names):
            return None
        diagnostics = payload.get("diagnostics", [])
        if not isinstance(diagnostics, list):
            return None
        return StandardizedStatements(
            entity_name=str(payload.get("entity_name") or ""),
            cik=payload.get("cik") or "",
            company_type=str(payload.get("company_type") or "industrial"),
            currency=str(payload.get("currency") or "USD"),
            income_statement=statements["income_statement"],
            balance_sheet=statements["balance_sheet"],
            cash_flow=statements["cash_flow"],
            diagnostics=[ValidationWarning(**item) for item in diagnostics],
        )
    except (
        OSError,
        EOFError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return None


def _write_sec_standardized_disk_cache(
    path: Path,
    key: tuple[str, tuple[str, ...], str, bool, bool],
    resolved: Any,
) -> None:
    """Atomically persist reusable SEC statement projections.

    This is an optimization cache only.  Unknown objects from tests or a future
    provider implementation are left uncached rather than affecting results.
    """
    statement_names = ("income_statement", "balance_sheet", "cash_flow")
    if any(
        not isinstance(getattr(resolved, name, None), list) for name in statement_names
    ):
        return
    diagnostics = getattr(resolved, "diagnostics", [])
    try:
        diagnostic_rows = [asdict(item) for item in diagnostics]
    except (TypeError, ValueError):
        return
    payload = {
        "schema_version": _SEC_STANDARDIZED_DISK_CACHE_SCHEMA_VERSION,
        "openbb_sec_version": _sec_standardized_cache_version(),
        "key": [key[0], list(key[1]), key[2], key[3], key[4]],
        "entity_name": getattr(resolved, "entity_name", ""),
        "cik": getattr(resolved, "cik", ""),
        "company_type": getattr(resolved, "company_type", "industrial"),
        "currency": getattr(resolved, "currency", "USD"),
        "statements": {name: getattr(resolved, name) for name in statement_names},
        "diagnostics": diagnostic_rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=1) as stream:
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        temporary.replace(path)
    except (OSError, ValueError, TypeError):
        # A cache write must never turn a valid provider result into failure.
        return
    finally:
        temporary.unlink(missing_ok=True)


def _resolve_sec_standardized_cached(
    endpoint: str,
    responses: Sequence[Mapping[str, Any]],
    *,
    period: str,
    pit_mode: bool,
    include_preliminary: bool,
    cache_dir: Path | None = None,
) -> Any:
    """Parse one raw companyfacts object once per three sibling statements."""
    import openbb_sec.utils.company_facts as company_facts

    cik_values = tuple(
        str(response.get("cik") or "").lstrip("0").zfill(10) for response in responses
    )
    key = ("sec", cik_values, period, pit_mode, include_preliminary)
    lock_key = "standardized:" + hashlib.sha256(repr(key).encode("utf-8")).hexdigest()
    with _sec_companyfacts_lock(lock_key):
        with _SEC_STANDARDIZED_CACHE_GUARD:
            entry = _SEC_STANDARDIZED_CACHE.get(key)
        if entry is None:
            disk_path = (
                _sec_standardized_disk_cache_path(cache_dir, key)
                if cache_dir is not None
                else None
            )

            def load_or_resolve() -> Any:
                cached = (
                    _read_sec_standardized_disk_cache(disk_path, key)
                    if disk_path is not None and disk_path.is_file()
                    else None
                )
                if cached is not None:
                    return cached
                if len(responses) == 1:
                    facts_json = dict(responses[0])
                else:
                    primary = responses[0]
                    facts_json = {
                        "entityName": primary.get("entityName", ""),
                        "cik": primary.get("cik", ""),
                        "facts": company_facts._schema.merge_facts(  # type: ignore[attr-defined]  # pylint: disable=protected-access
                            *(dict(response) for response in responses)
                        ),
                    }
                parsed = company_facts.resolve_company_facts(
                    facts_json,
                    period=period,
                    pit_mode=pit_mode,
                    include_preliminary=include_preliminary,
                )
                if disk_path is not None:
                    _write_sec_standardized_disk_cache(disk_path, key, parsed)
                return parsed

            if disk_path is None:
                resolved = load_or_resolve()
            else:
                with _interprocess_cache_lock(
                    disk_path.with_name(f".{disk_path.name}.lock")
                ):
                    # Re-read after acquiring the OS lock: a sibling process may
                    # have completed the shared three-statement projection.
                    resolved = load_or_resolve()
            consumers: set[str] = set()
            entry = (resolved, consumers)
            with _SEC_STANDARDIZED_CACHE_GUARD:
                _SEC_STANDARDIZED_CACHE[key] = entry
                _SEC_STANDARDIZED_CACHE_ORDER.append(key)
                while (
                    len(_SEC_STANDARDIZED_CACHE) > _SEC_STANDARDIZED_CACHE_MAX_ENTRIES
                    and _SEC_STANDARDIZED_CACHE_ORDER
                ):
                    oldest = _SEC_STANDARDIZED_CACHE_ORDER.popleft()
                    if oldest != key:
                        _SEC_STANDARDIZED_CACHE.pop(oldest, None)
        resolved, consumers = entry
        with _SEC_STANDARDIZED_CACHE_GUARD:
            consumers.add(endpoint)
            # Each resolved object already contains income, balance and cash.
            # The raw or growth family has exactly three sibling consumers.
            if len(consumers) >= 3:
                _SEC_STANDARDIZED_CACHE.pop(key, None)
        return resolved


def _sec_statement_components(
    endpoint: str, kwargs: Mapping[str, Any]
) -> tuple[Any, str, bool, Any]:
    """Resolve one SEC adapter and its normalized query in any process."""
    from openbb_sec.models.balance_sheet import SecBalanceSheetFetcher
    from openbb_sec.models.balance_sheet_growth import SecBalanceSheetGrowthFetcher
    from openbb_sec.models.cash_flow import SecCashFlowStatementFetcher
    from openbb_sec.models.cash_flow_growth import (
        SecCashFlowStatementGrowthFetcher,
    )
    from openbb_sec.models.income_statement import SecIncomeStatementFetcher
    from openbb_sec.models.income_statement_growth import (
        SecIncomeStatementGrowthFetcher,
    )

    adapters: dict[str, tuple[Any, str, bool]] = {
        "equity.fundamental.balance": (
            SecBalanceSheetFetcher,
            "balance_sheet",
            False,
        ),
        "equity.fundamental.balance_growth": (
            SecBalanceSheetGrowthFetcher,
            "balance_sheet",
            True,
        ),
        "equity.fundamental.cash": (
            SecCashFlowStatementFetcher,
            "cash_flow",
            False,
        ),
        "equity.fundamental.cash_growth": (
            SecCashFlowStatementGrowthFetcher,
            "cash_flow",
            True,
        ),
        "equity.fundamental.income": (
            SecIncomeStatementFetcher,
            "income_statement",
            False,
        ),
        "equity.fundamental.income_growth": (
            SecIncomeStatementGrowthFetcher,
            "income_statement",
            True,
        ),
    }
    try:
        fetcher, statement_name, is_growth = adapters[endpoint]
    except KeyError as exc:
        raise ValueError(f"Unsupported SEC statement endpoint: {endpoint}") from exc
    return fetcher, statement_name, is_growth, fetcher.transform_query(dict(kwargs))


def _project_sec_statement_responses(
    endpoint: str,
    kwargs: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    *,
    cache_dir: Path,
) -> list[dict[str, Any]]:
    """CPU-only SEC standardization and statement-model projection."""
    fetcher, statement_name, is_growth, query = _sec_statement_components(
        endpoint, kwargs
    )

    period = str(query.period)
    if is_growth:
        period = {
            "annual": "yoy",
            "quarterly": "pop",
            "quarterly_yoy": "yoy_quarterly",
            "ttm": "pop",
        }[period]
    result = _resolve_sec_standardized_cached(
        endpoint,
        responses,
        period=period,
        pit_mode=bool(query.pit_mode),
        include_preliminary=bool(query.include_preliminary),
        cache_dir=cache_dir,
    )
    transformed = _transform_sec_statement_resilient(
        fetcher,
        query,
        result,
        statement_name=statement_name,
    )
    # Direct fetchers return AnnotatedResult.result.  The generic archive
    # normalizer expects either OBBject.results or the observation sequence
    # itself; passing the AnnotatedResult model would serialize one giant
    # ``result``/``metadata`` wrapper row.
    return _provider_result_rows(transformed)


def _read_sec_companyfacts_responses_from_cache(
    cache_dir: Path, cik_values: Sequence[str]
) -> list[dict[str, Any]]:
    """Load raw companyfacts inside a CPU worker without any network access."""
    from openbb_core.provider.utils.errors import EmptyDataError

    responses: list[dict[str, Any]] = []
    for cik in cik_values:
        cik_text = str(cik).lstrip("0").zfill(10)
        cache_path = _sec_companyfacts_cache_path(cache_dir, cik_text)
        cached = _read_sec_companyfacts_cache(cache_path, cik_text)
        if cached is None:
            raise SecCompanyfactsCacheInvalidError(
                f"SEC companyfacts cache is missing or invalid for CIK {cik_text}"
            )
        if cached.get("_archive_cache_status") == "empty":
            raise EmptyDataError(f"No company facts were found for CIK: {cik_text}")
        responses.append(cached)
    return responses


def _project_sec_statement_cached_process(
    endpoint: str,
    kwargs: dict[str, Any],
    cache_dir: str,
    cik_values: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Spawn-safe cache-only entrypoint for GIL-bound SEC projection work."""
    path = Path(cache_dir)
    responses = _read_sec_companyfacts_responses_from_cache(path, cik_values)
    return _project_sec_statement_responses(
        endpoint,
        kwargs,
        responses,
        cache_dir=path,
    )


def _fetch_sec_statement_workaround(
    endpoint: str,
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None,
    cache_dir: Path,
    process_pool: ProcessPoolExecutor | None = None,
) -> Any:
    """Fetch centrally, then project cached companyfacts on CPU processes."""
    _, _, _, query = _sec_statement_components(endpoint, kwargs)
    if process_pool is None:
        _, responses = _sec_companyfacts_responses_for_symbol(
            str(query.symbol),
            page_limiter=page_limiter,
            cache_dir=cache_dir,
            use_cache=False,
        )
        return _project_sec_statement_responses(
            endpoint,
            kwargs,
            responses,
            cache_dir=cache_dir,
        )

    _, cik_values = _sec_companyfacts_ciks_for_symbol(
        str(query.symbol), page_limiter=page_limiter
    )
    for cik in cik_values:
        if not _sec_companyfacts_cache_path(cache_dir, cik).is_file():
            _fetch_sec_companyfacts_response(
                cik,
                page_limiter=page_limiter,
                cache_dir=cache_dir,
                use_cache=False,
            )

    def project_once() -> list[dict[str, Any]]:
        future = process_pool.submit(
            _project_sec_statement_cached_process,
            endpoint,
            dict(kwargs),
            str(cache_dir),
            cik_values,
        )
        return future.result()

    with _LOCAL_CPU_BUDGET.claim(1):
        try:
            return project_once()
        except SecCompanyfactsCacheInvalidError:
            # Only the network-owning main process may repair a corrupt raw
            # cache. Retry the pure child projection exactly once afterwards.
            for cik in cik_values:
                _fetch_sec_companyfacts_response(
                    cik,
                    page_limiter=page_limiter,
                    cache_dir=cache_dir,
                    use_cache=False,
                )
            return project_once()


def _transform_sec_statement_resilient(
    fetcher: Any,
    query: Any,
    result: Any,
    *,
    statement_name: str,
) -> Any:
    """Preserve a statement when one exact mapped XBRL cell is ill-typed.

    SEC companyfacts is a heterogeneous taxonomy and OpenBB projects it into a
    stricter wide Pydantic model. A single bad mapping (for example a ratio in
    an integer share-count field) must not discard every other fact and period
    for the filer. Retry validation on a private copy after removing only the
    long-form cell whose field *and input value* match Pydantic's evidence.
    Add a row-level recovery marker so this loss is visible downstream.

    Structural errors, unmatched inputs, and more than 32 bad cells still
    raise normally; this is narrow data salvage, not blanket validation
    suppression. The same policy covers all six SEC raw/growth statements.
    """
    from pydantic import ValidationError

    sanitized = result
    private_copy = False
    recovery_by_period: dict[str, list[dict[str, Any]]] = {}
    for _ in range(32):
        try:
            transformed = fetcher.transform_data(
                query,
                {"result": sanitized, "statement": statement_name},
            )
            if recovery_by_period:
                rows = getattr(transformed, "result", transformed)
                for row in rows:
                    period_ending = str(
                        getattr(row, "period_ending", "")
                        or (
                            row.get("period_ending", "")
                            if isinstance(row, Mapping)
                            else ""
                        )
                    )[:10]
                    recoveries = recovery_by_period.get(period_ending)
                    if not recoveries:
                        continue
                    marker = _canonical_json(recoveries)
                    if isinstance(row, dict):
                        row["openbb_validation_recoveries"] = marker
                    else:
                        setattr(row, "openbb_validation_recoveries", marker)
            return transformed
        except ValidationError as exc:
            if not private_copy:
                sanitized = deepcopy(result)
                private_copy = True
            records = list(getattr(sanitized, statement_name))
            rejected: list[tuple[str, Any, str]] = []
            for error in exc.errors():
                location = tuple(error.get("loc") or ())
                if len(location) != 1 or not isinstance(location[0], str):
                    raise
                rejected.append(
                    (
                        location[0],
                        error.get("input"),
                        str(error.get("type") or "validation_error"),
                    )
                )
            removed_indexes: set[int] = set()
            for field_name, invalid_input, error_type in rejected:
                matched = False
                for index, record in enumerate(records):
                    if index in removed_indexes or record.get("tag") != field_name:
                        continue
                    value = record.get("value")
                    equal = value == invalid_input
                    if (
                        isinstance(value, float)
                        and isinstance(invalid_input, float)
                        and math.isnan(value)
                        and math.isnan(invalid_input)
                    ):
                        equal = True
                    if not equal:
                        continue
                    period_ending = str(record.get("period_ending") or "")
                    if not period_ending:
                        raise
                    removed_indexes.add(index)
                    recovery_by_period.setdefault(period_ending, []).append(
                        {
                            "field": field_name,
                            "error_type": error_type,
                            "invalid_value": _normalize_scalar(invalid_input),
                        }
                    )
                    matched = True
                    break
                if not matched:
                    raise
            records = [
                record
                for index, record in enumerate(records)
                if index not in removed_indexes
                and record.get("tag") != "openbb_validation_recoveries"
            ]
            setattr(sanitized, statement_name, records)
    raise RuntimeError(
        "SEC statement contains more than 32 independently invalid mapped cells"
    )


def _fetch_sec_company_facts_bulk_workaround(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Fetch and flatten every SEC company fact with one request per symbol.

    The routed OpenBB model accepts one fact at a time and calls SEC's
    ``companyconcept`` endpoint.  SEC's ``companyfacts`` endpoint returns all
    namespaces, tags, units, and historical observations for the same filer,
    so this preserves a strict superset while avoiding a symbol-by-fact request
    Cartesian product.
    """
    from openbb_core.provider.utils.errors import EmptyDataError

    symbol = str(kwargs.get("symbol") or "").strip().upper()
    use_cache = bool(kwargs.get("use_cache", True))
    if not symbol:
        raise ValueError("symbol is required for SEC bulk company facts")
    symbol, responses = _sec_companyfacts_responses_for_symbol(
        symbol,
        page_limiter=page_limiter,
        cache_dir=cache_dir,
        use_cache=use_cache,
    )
    # The bulk facts artifact preserves every CIK independently.  Standardized
    # statements merge multi-CIK histories, but flattening them here avoids
    # discarding provenance or concepts when taxonomies overlap.
    response_cik_pairs = [
        (str(response.get("cik") or "").lstrip("0").zfill(10), response)
        for response in responses
    ]
    response_cik_pairs = [
        (cik, response) for cik, response in response_cik_pairs if cik.strip("0")
    ]
    if not response_cik_pairs:
        raise EmptyDataError(f"No company facts were found for symbol: {symbol}")
    flattened_responses = response_cik_pairs

    def observations() -> Iterator[
        tuple[str, str, Mapping[str, Any], str, str, str, str, str]
    ]:
        for cik, response in flattened_responses:
            entity_name = str(response.get("entityName") or "")
            facts = response.get("facts") or {}
            for taxonomy, concepts in facts.items():
                if not isinstance(concepts, Mapping):
                    continue
                for fact_tag, concept_value in concepts.items():
                    if not isinstance(concept_value, Mapping):
                        continue
                    fact_label = str(concept_value.get("label") or fact_tag)
                    fact_description = str(concept_value.get("description") or "")
                    units = concept_value.get("units") or {}
                    if not isinstance(units, Mapping):
                        continue
                    for unit, unit_observations in units.items():
                        if not isinstance(unit_observations, list):
                            continue
                        for observation in unit_observations:
                            if isinstance(observation, Mapping):
                                yield (
                                    str(taxonomy),
                                    str(fact_tag),
                                    observation,
                                    fact_label,
                                    fact_description,
                                    str(unit),
                                    cik,
                                    entity_name,
                                )

    observation_total = sum(
        len(unit_observations)
        for _, response in flattened_responses
        for facts in (response.get("facts") or {},)
        if isinstance(facts, Mapping)
        for concepts in facts.values()
        if isinstance(concepts, Mapping)
        for concept_value in concepts.values()
        if isinstance(concept_value, Mapping)
        for units in (concept_value.get("units") or {},)
        if isinstance(units, Mapping)
        for unit_observations in units.values()
        if isinstance(unit_observations, list)
    )
    records: list[dict[str, Any]] = []
    progress = tqdm(
        observations(),
        total=observation_total,
        desc=f"sec:{symbol} company facts",
        unit="observation",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for (
            taxonomy,
            fact_tag,
            observation,
            fact_label,
            fact_description,
            unit,
            cik,
            entity_name,
        ) in progress:
            if observation.get("val") is None:
                continue
            records.append(
                {
                    "symbol": symbol,
                    "name": entity_name,
                    "value": float(observation["val"]),
                    "reported_date": observation.get("filed"),
                    "period_beginning": observation.get("start"),
                    "period_ending": observation.get("end"),
                    "fiscal_year": observation.get("fy"),
                    "fiscal_period": observation.get("fp"),
                    "cik": cik,
                    "location": None,
                    "form": observation.get("form"),
                    "frame": observation.get("frame"),
                    "accession": observation.get("accn"),
                    "fact": fact_label,
                    "unit": unit,
                    "taxonomy": taxonomy,
                    "fact_tag": fact_tag,
                    "fact_description": fact_description,
                }
            )
    finally:
        progress.close()
    if not records:
        raise EmptyDataError(f"No company facts were found for symbol: {symbol}")
    return records


def _sec_submission_records(payload: Any) -> list[dict[str, Any]]:
    """Normalize both SEC submissions JSON layouts to row dictionaries.

    The root ``filings.recent`` object and every historical
    ``*-submissions-*.json`` shard are column-oriented mappings of lists.
    Some test/proxy responses may already be a list of row mappings.  Treating
    historical mappings as row lists silently drops every pre-``recent``
    filing while still returning a plausible 1,000-row success, so both
    layouts must share one strict conversion path.
    """
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    if not isinstance(payload, Mapping):
        return []
    columns = {
        str(key): value for key, value in payload.items() if isinstance(value, list)
    }
    row_count = max((len(value) for value in columns.values()), default=0)
    return [
        {
            key: (values[index] if index < len(values) else None)
            for key, values in columns.items()
        }
        for index in range(row_count)
    ]


def _fetch_sec_filings_workaround(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[Any]:
    """Fetch SEC submission shards sequentially under the provider limiter.

    OpenBB's provider gathers every historical submissions shard concurrently.
    One outer task can therefore exceed SEC's ten-request/second fair-access
    ceiling even when the task scheduler itself is perfectly paced.  Preserve
    the same query/result models, but claim one shared limiter slot before each
    child shard.
    """
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.models.company_filings import SecCompanyFilingsFetcher
    from openbb_sec.utils.definitions import HEADERS

    original_query = SecCompanyFilingsFetcher.transform_query(dict(kwargs))
    symbol = str(original_query.symbol or "").strip().upper()
    cik = (
        str(original_query.cik).lstrip("0").zfill(10)
        if original_query.cik
        else _sec_symbol_cik_map(page_limiter).get(symbol.replace(".", "-"), "")
    )
    if not cik:
        raise EmptyDataError(f"No CIK was found for symbol: {symbol}")

    # XOM changed CIK in July 2026.  Query both identities explicitly and let
    # each result model construct URLs from the CIK that actually owns it.
    ciks = [cik]
    if symbol == "XOM" and "0000034088" not in ciks:
        ciks.append("0000034088")

    def fetch_json(url: str) -> dict[str, Any] | list[dict[str, Any]]:
        if page_limiter is not None:
            _wait_sec_http_limiter(page_limiter)
        request = Request(url, headers=dict(HEADERS))
        try:
            with urlopen(request, timeout=60) as response:
                content = response.read()
                if content.startswith(b"\x1f\x8b"):
                    content = gzip.decompress(content)
                payload = json.loads(content)
        except HTTPError as exc:
            if exc.code == 404:
                return {}
            raise RuntimeError(
                f"SEC submissions HTTP {exc.code}: {exc.reason}"
            ) from exc
        except URLError as exc:
            raise ConnectionError(
                f"SEC submissions connection error: {exc.reason}"
            ) from exc
        if not isinstance(payload, (Mapping, list)):
            raise TypeError("SEC submissions API returned invalid JSON")
        return dict(payload) if isinstance(payload, Mapping) else payload

    transformed: list[Any] = []
    for current_cik in ciks:
        root = fetch_json(f"https://data.sec.gov/submissions/CIK{current_cik}.json")
        if not isinstance(root, Mapping) or not root:
            continue
        filings = root.get("filings")
        if not isinstance(filings, Mapping):
            continue
        recent = filings.get("recent")
        rows = _sec_submission_records(recent)

        shard_files = [
            str(item.get("name"))
            for item in filings.get("files", [])
            if isinstance(item, Mapping) and item.get("name")
        ]
        progress = tqdm(
            shard_files,
            total=len(shard_files),
            desc=f"sec:{symbol or current_cik} filing shards"[:64],
            unit="shard",
            position=2,
            leave=False,
            disable=not show_progress,
        )
        try:
            for shard_name in progress:
                shard = fetch_json(f"https://data.sec.gov/submissions/{shard_name}")
                rows.extend(_sec_submission_records(shard))
                progress.set_postfix(rows=len(rows), refresh=False)
        finally:
            progress.close()

        if not rows:
            continue
        query_kwargs = dict(kwargs)
        query_kwargs.update({"symbol": None, "cik": current_cik})
        query = SecCompanyFilingsFetcher.transform_query(query_kwargs)
        transformed.extend(
            _provider_result_rows(SecCompanyFilingsFetcher.transform_data(query, rows))
        )

    if not transformed:
        raise EmptyDataError(f"No SEC filings found for {symbol or cik}")
    return transformed


def _fetch_sec_filing_headers_workaround(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
) -> list[dict[str, Any]]:
    """Read filing metadata from SEC's consistently available index page.

    OpenBB's current model derives ``*-index-headers.htm``.  Many legitimate
    historical accessions publish only ``*-index.html``; treating that first
    URL's 404 as authoritative empty silently drops metadata for hundreds of
    thousands of filings.  The index page already contains the requested
    header fields and document catalog, so parse it directly without fetching
    filing PDF/HTML bodies.
    """
    import gzip

    from urllib.error import HTTPError, URLError
    from urllib.parse import parse_qs, urljoin, urlsplit
    from urllib.request import Request, urlopen

    from bs4 import BeautifulSoup
    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_sec.utils.definitions import HEADERS

    raw_url = str(kwargs.get("url") or "").strip()
    match = re.search(
        r"/Archives/edgar/data/(\d+)/(\d{18})(?:/|$)",
        raw_url,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise EmptyDataError(f"Invalid SEC filing URL: {raw_url}")
    cik_path, accession = match.groups()
    cik = cik_path.lstrip("0") or "0"
    accession_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"
    index_url = f"{base_url}{accession_dashed}-index.html"
    if page_limiter is not None:
        _wait_sec_http_limiter(page_limiter)
    try:
        with urlopen(Request(index_url, headers=dict(HEADERS)), timeout=60) as response:
            content = response.read()
            content_encoding = str(response.headers.get("Content-Encoding", "")).lower()
    except HTTPError as exc:
        if exc.code == 404:
            raise EmptyDataError(
                f"No SEC filing index page exists for {accession_dashed}"
            ) from exc
        raise RuntimeError(f"SEC filing index HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise ConnectionError(
            f"SEC filing index connection error: {exc.reason}"
        ) from exc
    if not content:
        raise EmptyDataError(f"Empty SEC filing index page for {accession_dashed}")
    if content.startswith(b"\x1f\x8b") or "gzip" in content_encoding:
        content = gzip.decompress(content)

    soup = BeautifulSoup(content, "html.parser")
    info: dict[str, str] = {}
    for heading in soup.select("div.infoHead"):
        value = heading.find_next_sibling("div", class_="info")
        if value is not None:
            info[heading.get_text(" ", strip=True)] = value.get_text(" ", strip=True)

    selected_company = None
    for company in soup.select("div.companyInfo"):
        link = company.select_one('a[href*="CIK="]')
        if link is None:
            continue
        query_cik = parse_qs(urlsplit(str(link.get("href") or "")).query).get(
            "CIK", [""]
        )[0]
        if str(query_cik).lstrip("0") == cik:
            selected_company = company
            break
    if selected_company is None:
        selected_company = soup.select_one("div.companyInfo")

    company_text = (
        selected_company.get_text(" ", strip=True) if selected_company else ""
    )
    company_name_node = (
        selected_company.select_one("span.companyName")
        if selected_company is not None
        else None
    )
    company_name = (
        re.split(
            r"\s+\((?:Filed by|Subject|Filer)\)",
            company_name_node.get_text(" ", strip=True),
        )[0]
        if company_name_node is not None
        else ""
    )
    cik_match = re.search(r"\bCIK\s*:\s*(\d+)", company_text)
    sic_match = re.search(
        r"\bSIC\s*:\s*(\d+)(?:\s+(.+?))?"
        r"(?=\s*(?:\||Type:|State of Incorp\.:|Fiscal Year End:|$))",
        company_text,
    )
    fiscal_match = re.search(r"Fiscal Year End:\s*(\d{4})", company_text)
    type_match = re.search(
        r"\bType:\s*(.+?)"
        r"(?=\s*(?:\||Act:|File No\.:|Film No\.:|$))",
        company_text,
    )
    form_name = soup.select_one("#formName")
    document_type = (
        type_match.group(1).strip()
        if type_match
        else re.sub(
            r"^Form\s+|\s+-.*$",
            "",
            form_name.get_text(" ", strip=True) if form_name else "",
        ).strip()
    )

    documents: list[dict[str, Any]] = []
    table = soup.select_one('table.tableFile[summary="Document Format Files"]')
    if table is not None:
        for row in table.select("tr"):
            cells = row.select("td")
            if len(cells) < 4:
                continue
            link = cells[2].find("a")
            href = str(link.get("href") or "") if link is not None else ""
            if not href:
                continue
            documents.append(
                {
                    "sequence": cells[0].get_text(" ", strip=True),
                    "description": cells[1].get_text(" ", strip=True),
                    "filename": (
                        link.get_text(" ", strip=True) if link is not None else ""
                    ),
                    "type": cells[3].get_text(" ", strip=True),
                    "url": urljoin("https://www.sec.gov", href),
                }
            )

    filing_date = info.get("Filing Date")
    if not filing_date or not document_type:
        raise EmptyDataError(
            f"SEC filing index lacks required metadata for {accession_dashed}"
        )
    fiscal_year_end = (
        f"{fiscal_match.group(1)[:2]}-{fiscal_match.group(1)[2:]}"
        if fiscal_match
        else None
    )
    period_ending = info.get("Period of Report") or None
    return [
        {
            "base_url": base_url,
            "name": company_name,
            "cik": (cik_match.group(1) if cik_match else cik_path).lstrip("0"),
            "trading_symbols": None,
            "sic": sic_match.group(1) if sic_match else "",
            "sic_organization_name": (
                (sic_match.group(2) or "").strip() if sic_match else ""
            ),
            "filing_date": filing_date,
            "period_ending": period_ending,
            "fiscal_year_end": fiscal_year_end,
            "document_type": document_type,
            "has_cover_page": any(
                str(item.get("filename") or "").endswith("R1.htm") for item in documents
            ),
            "description": form_name.get_text(" ", strip=True) if form_name else None,
            "cover_page": None,
            "document_urls": documents,
            "index_url": index_url,
        }
    ]


def _fetch_fred_retail_prices_workaround(
    kwargs: Mapping[str, Any], obb: Any
) -> list[Any]:
    """Bypass OpenBB's router typo for the valid ``ground_beef`` provider item."""
    import asyncio

    from openbb_fred.models.retail_prices import FredRetailPricesFetcher

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    credentials = {"fred_api_key": str(credential)} if credential else {}
    query = FredRetailPricesFetcher.transform_query(dict(kwargs))
    raw = asyncio.run(FredRetailPricesFetcher.aextract_data(query, credentials))
    transformed = FredRetailPricesFetcher.transform_data(query, raw)
    return _provider_result_rows(transformed)


def _fetch_fred_series_workaround(kwargs: Mapping[str, Any], obb: Any) -> list[Any]:
    """Fetch FRED observations without a discarded metadata request.

    Archive workers create one short-lived asyncio loop per blocking task.
    FRED's process cache keys in-flight Futures by loop, so it cannot collapse
    work across these workers.  On an upstream 5xx or a shared cooldown it
    sets an exception on an owner-only Future with no waiter, producing
    ``Future exception was never retrieved`` tracebacks.

    The installed adapter also fetches ``/fred/series`` metadata after every
    observations request.  This archive deliberately normalizes only
    ``AnnotatedResult.result`` and discards that metadata, so the second HTTP
    call cannot affect any persisted row.  Build the same raw transform shape
    from observations alone: one manifest series task now consumes exactly
    one FRED request while retaining the provider query model, result model,
    transforms, and HTTP-boundary limiter.
    """
    import asyncio

    from openbb_core.provider.utils.helpers import get_querystring
    from openbb_fred.models.series import FredSeriesFetcher
    from openbb_fred.utils.rate_limiter import fred_get

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    credentials = {"fred_api_key": str(credential)} if credential else {}
    query = FredSeriesFetcher.transform_query(dict(kwargs))
    api_key = credentials.get("fred_api_key", "")
    querystring = get_querystring(query.model_dump(), ["series_id"])

    async def _fetch_one(series_id: str) -> dict[str, Any]:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&{querystring}&file_type=json&api_key={api_key}"
        )
        response = await fred_get(url, timeout=60, use_cache=False)
        observations = (
            response.get("observations", []) if isinstance(response, Mapping) else []
        )
        values: dict[str, float] = {}
        for observation in observations or []:
            if not isinstance(observation, Mapping):
                continue
            raw_date = observation.get("date")
            raw_value = observation.get("value")
            if raw_date in {None, ""} or raw_value in {None, "", "."}:
                continue
            try:
                values[str(raw_date)] = float(raw_value)
            except (TypeError, ValueError):
                continue
        return {
            series_id: {
                "title": None,
                "units": None,
                "frequency": None,
                "seasonal_adjustment": None,
                "notes": None,
                "data": values,
            }
        }

    async def _extract() -> list[dict[str, Any]]:
        series_ids = [
            item.strip() for item in str(query.symbol).split(",") if item.strip()
        ]
        return list(await asyncio.gather(*(_fetch_one(item) for item in series_ids)))

    raw = asyncio.run(_extract())
    transformed = FredSeriesFetcher.transform_data(query, raw)
    return _provider_result_rows(transformed)


def _fetch_fred_bond_indices_workaround(
    kwargs: Mapping[str, Any], obb: Any
) -> list[Any]:
    """Call the FRED fetcher directly for provider-valid BAML combinations.

    OpenBB's router-level shared schema rejects the ``europe`` and ``emerging``
    indices even though the FRED provider explicitly implements them under the
    ``high_yield`` category.  The provider fetcher retains its own validation,
    credentials, normalization, and metadata handling without that lossy
    shared-schema gate.
    """
    import asyncio

    from openbb_fred.models.bond_indices import FredBondIndicesFetcher

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    credentials = {"fred_api_key": str(credential)} if credential else {}
    transformed = asyncio.run(
        FredBondIndicesFetcher.fetch_data(dict(kwargs), credentials)
    )
    return list(getattr(transformed, "result", transformed) or [])


def _fetch_fred_calendar_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    page_limiter: Any | None = None,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Fetch the FRED calendar from its documented JSON API.

    OpenBB's current FRED adapter scrapes the public HTML calendar one day at
    a time.  That website route can time out even for a single day, while the
    official ``fred/releases/dates`` API supports an entire real-time period
    and deterministic pagination.
    """
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError(
            "FRED API key is required for the release-dates calendar API"
        )

    start_date = str(kwargs.get("start_date") or DEFAULT_START_DATE)
    end_date = str(kwargs.get("end_date") or date.today().isoformat())
    release_id = kwargs.get("release_id")
    endpoint = "release/dates" if release_id else "releases/dates"
    limit = 1000
    offset = 0
    records: list[dict[str, Any]] = []
    seen_page_signatures: set[str] = set()
    progress = tqdm(
        total=None,
        desc=f"fred:calendar {start_date}..{end_date}"[:64],
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        while True:
            params: dict[str, Any] = {
                "api_key": str(credential),
                "file_type": "json",
                "realtime_start": start_date,
                "realtime_end": end_date,
                "limit": limit,
                "offset": offset,
                "sort_order": "asc",
                "include_release_dates_with_no_data": "true",
            }
            if release_id:
                params["release_id"] = release_id
            else:
                params["order_by"] = "release_date"
            url = f"https://api.stlouisfed.org/fred/{endpoint}?{urlencode(params)}"
            request = Request(
                url, headers={"User-Agent": "stockAgent-openbb-archive/1.0"}
            )
            # One annual task can require several API pages.  Reuse the provider's
            # process-wide limiter for every page instead of accounting only for
            # the outer task, otherwise concurrent years can exceed the API cap.
            if offset > 0 and page_limiter is not None:
                page_limiter.wait()
            with urlopen(request, timeout=60) as response:
                payload = json.load(response)
            if payload.get("error_code"):
                raise RuntimeError(
                    f"FRED API error {payload.get('error_code')}: "
                    f"{payload.get('error_message', '')}"
                )

            page = payload.get("release_dates") or []
            page_signature = _raw_page_signature(page)
            if page_signature in seen_page_signatures:
                raise RuntimeError(f"FRED calendar pagination cycle at offset {offset}")
            seen_page_signatures.add(page_signature)
            for raw in page:
                record = dict(raw)
                release_name = record.get("release_name")
                if release_name is not None:
                    record["event"] = release_name
                record["source"] = "FRED"
                records.append(record)

            raw_count = payload.get("count")
            if raw_count is None:
                if len(page) >= limit:
                    raise RuntimeError(
                        "FRED calendar omitted pagination count on a full page"
                    )
                total = offset + len(page)
            else:
                total = int(raw_count)
            progress.total = max(1, math.ceil(total / limit))
            progress.update(1)
            progress.set_postfix(rows=len(records), total_rows=total, refresh=False)
            offset += len(page)
            if offset >= total:
                break
            if not page:
                raise ConnectionError(
                    "FRED releases/dates pagination stopped at offset "
                    f"{offset} of {total}"
                )
    finally:
        progress.close()

    return records


def _fetch_fred_release_search_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
) -> list[Any]:
    """Fetch one FRED release-series page with an archive-safe timeout.

    OpenBB's shared async request helper defaults to ten seconds. Deep offsets
    in the largest FRED releases can legitimately take longer, causing the same
    deterministic page to consume three worker retries. Keep OpenBB's query
    validation and result model transform, but allow the official API sixty
    seconds and never propagate the credential-bearing URL in exceptions.
    """
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    from openbb_fred.models.search import FredSearchFetcher

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FRED API key is required for release-series search")

    query = FredSearchFetcher.transform_query(dict(kwargs))
    if query.release_id is None:
        raise ValueError(
            "release_id is required for the FRED archive search workaround"
        )
    if query.order_by == "search_rank":
        query.order_by = None  # type: ignore[assignment]
    parameters = {
        key: value
        for key, value in query.model_dump().items()
        if key not in {"search_text", "limit"} and value is not None
    }
    parameters.update({"file_type": "json", "api_key": str(credential)})
    url = "https://api.stlouisfed.org/fred/release/series?" + urlencode(parameters)
    request = Request(url, headers={"User-Agent": "stockAgent-openbb-archive/1.0"})
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(
            f"FRED release search HTTP {exc.code}: {exc.reason}"
        ) from exc
    except (TimeoutError, URLError) as exc:
        reason = getattr(exc, "reason", type(exc).__name__)
        reason_text = str(reason).replace(str(credential), "<redacted>")
        raise ConnectionError(
            f"FRED release search connection error: {reason_text}"
        ) from exc

    if not isinstance(payload, Mapping):
        raise TypeError("FRED release search returned a non-object JSON payload")
    if payload.get("error_code"):
        raise RuntimeError(
            f"FRED API error {payload.get('error_code')}: "
            f"{payload.get('error_message', '')}"
        )
    raw_records = payload.get("seriess")
    if not isinstance(raw_records, list):
        raise TypeError("FRED release search response omitted the seriess collection")
    return _provider_result_rows(
        FredSearchFetcher.transform_data(
            query,
            [dict(item) for item in raw_records if isinstance(item, Mapping)],
        )
    )


def _fetch_fred_hqm_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    page_limiter: Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch HQM curves while treating FRED's ``.`` as an unpublished value.

    OpenBB's current FRED adapter tests only for an empty observation value.
    FRED uses a literal period for an unavailable monthly observation, causing
    the adapter to parse ``.`` as a date and discard an otherwise valid task.
    Read the same documented release-table JSON, skip only unavailable nodes,
    and preserve any valid maturities that coexist with them.
    """
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    credential = getattr(obb.user.credentials, "fred_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("FRED API key is required for HQM curves")

    raw_date = str(kwargs.get("date") or "").strip()
    observation_date = date.fromisoformat(raw_date[:10]).replace(day=1).isoformat()
    yield_curve = str(kwargs.get("yield_curve") or "spot").strip().lower()
    if yield_curve not in {"spot", "par"}:
        raise ValueError(f"Unsupported HQM yield curve: {yield_curve}")
    element_id = "219299" if yield_curve == "spot" else "219294"
    params = {
        "release_id": "402",
        "element_id": element_id,
        "observation_date": observation_date,
        "include_observation_values": "true",
        "api_key": str(credential),
        "file_type": "json",
    }
    url = "https://api.stlouisfed.org/fred/release/tables?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "stockAgent-openbb-archive/1.0"})
    # The worker's outer limiter claim is this request's slot.  This helper
    # performs exactly one HTTP request, so no child-page claim is needed.
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as exc:
        # Never propagate the credential-bearing URL embedded in HTTPError.
        raise RuntimeError(f"FRED HQM HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise ConnectionError(f"FRED HQM connection error: {exc.reason}") from exc

    if payload.get("error_code"):
        raise RuntimeError(
            f"FRED API error {payload.get('error_code')}: "
            f"{payload.get('error_message', '')}"
        )
    elements = payload.get("elements") or {}
    if not isinstance(elements, Mapping):
        raise TypeError("FRED HQM response contained invalid elements")

    records: list[dict[str, Any]] = []
    for item in elements.values():
        if not isinstance(item, Mapping):
            continue
        value = str(item.get("observation_value") or "").strip()
        observed = str(item.get("observation_date") or "").strip()
        if value in {"", "."} or observed in {"", "."}:
            continue
        match = re.fullmatch(
            r"\s*(\d+(?:\.\d+)?)\s*-\s*(year|month)s?\s*",
            str(item.get("name") or ""),
            flags=re.IGNORECASE,
        )
        if match is None:
            continue
        try:
            normalized_observation_date = date.fromisoformat(observed[:10]).isoformat()
        except ValueError:
            # HQM release tables normally use values such as "May 2026" or
            # the abbreviated "Dec 2017".
            # Anchor monthly observations to day one instead of allowing a
            # permissive parser to inject the downloader's current day.
            parsed_month = None
            for date_format in ("%b %Y", "%B %Y"):
                try:
                    parsed_month = datetime.strptime(observed, date_format).date()
                    break
                except ValueError:
                    continue
            if parsed_month is None:
                raise ValueError(f"Unsupported FRED HQM observation date: {observed}")
            normalized_observation_date = parsed_month.isoformat()
        amount, unit = match.groups()
        records.append(
            {
                "date": normalized_observation_date,
                "rate": float(value) / 100,
                "maturity": f"{unit.lower()}_{amount}",
            }
        )
    return records


def _fetch_cftc_cot_catalog_workaround(
    kwargs: Mapping[str, Any],
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Enumerate every historical CFTC contract code for one report dataset.

    OpenBB's search adapter intentionally limits its catalog to contracts seen
    during the last 52 weeks.  An archive beginning in 2000 must also discover
    inactive contracts to avoid survivorship bias.  Query the same anonymous
    Socrata datasets over the requested archive interval and retain only
    metadata needed to create COT follow-up tasks.
    """
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    from openbb_cftc.utils import reports_dict

    report_type = str(kwargs.get("report_type") or "legacy").lower()
    futures_only = bool(kwargs.get("futures_only", False))
    dataset_key = report_type.replace("financial", "tff")
    if dataset_key != "supplemental":
        dataset_key += "_futures_only" if futures_only else "_combined"
    dataset_id = reports_dict[dataset_key]
    start_date = str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10]
    end_date = str(kwargs.get("end_date") or date.today().isoformat())[:10]
    columns = (
        "cftc_contract_market_code,contract_market_name,commodity_name,"
        "commodity_group_name,commodity_subgroup_name,contract_units"
    )
    limit = 50000
    offset = 0
    records: list[dict[str, Any]] = []
    seen_page_signatures: set[str] = set()
    mode = "futures" if futures_only else "combined"
    progress = tqdm(
        total=None,
        desc=f"cftc:{report_type}:{mode} catalog",
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        while True:
            params = {
                "$select": columns,
                "$group": columns,
                "$where": (
                    f"Report_Date_as_YYYY_MM_DD >= '{start_date}' AND "
                    f"Report_Date_as_YYYY_MM_DD <= '{end_date}'"
                ),
                "$limit": limit,
                "$offset": offset,
                "$order": (
                    "commodity_group_name,commodity_subgroup_name,"
                    "contract_market_name,cftc_contract_market_code"
                ),
            }
            url = (
                f"https://publicreporting.cftc.gov/resource/{dataset_id}.json?"
                + urlencode(params)
            )
            request = Request(
                url, headers={"User-Agent": "stockAgent-openbb-archive/1.0"}
            )
            if offset > 0 and page_limiter is not None:
                page_limiter.wait()
            try:
                with urlopen(request, timeout=60) as response:
                    payload = json.load(response)
            except HTTPError as exc:
                raise RuntimeError(
                    f"CFTC catalog HTTP {exc.code}: {exc.reason}"
                ) from exc
            except URLError as exc:
                raise ConnectionError(
                    f"CFTC catalog connection error: {exc.reason}"
                ) from exc
            if isinstance(payload, Mapping):
                raise RuntimeError(
                    "CFTC catalog API error: "
                    f"{payload.get('message', 'invalid response')}"
                )
            if not isinstance(payload, list):
                raise TypeError("CFTC catalog returned a non-list response")
            page_signature = _raw_page_signature(payload)
            if page_signature in seen_page_signatures:
                raise RuntimeError(f"CFTC catalog pagination cycle at offset {offset}")
            seen_page_signatures.add(page_signature)
            row_progress = tqdm(
                payload,
                total=len(payload),
                desc=f"cftc:{report_type}:{mode} rows",
                unit="contract",
                position=3,
                leave=False,
                disable=not show_progress,
            )
            try:
                for raw in row_progress:
                    if not isinstance(raw, Mapping):
                        continue
                    records.append(
                        {
                            "code": raw.get("cftc_contract_market_code"),
                            "name": raw.get("contract_market_name"),
                            "commodity": raw.get("commodity_name"),
                            "category": raw.get("commodity_group_name"),
                            "subcategory": raw.get("commodity_subgroup_name"),
                            "units": raw.get("contract_units"),
                        }
                    )
            finally:
                row_progress.close()
            progress.update(1)
            progress.set_postfix(contracts=len(records), refresh=False)
            if len(payload) < limit:
                break
            offset += len(payload)
    finally:
        progress.close()
    return records


def _fetch_congress_collection_workaround(
    endpoint: str,
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
    checkpoint_dir: Path | None = None,
) -> list[Any]:
    """Fetch all Congress bills/amendments without OpenBB's concurrent fan-out."""
    from urllib.parse import urlencode

    from openbb_core.provider.utils.errors import EmptyDataError
    from openbb_congress_gov.models.congress_amendments import (
        CongressAmendmentsFetcher,
    )
    from openbb_congress_gov.models.congress_bills import CongressBillsFetcher

    if endpoint == "uscongress.bills":
        fetcher = CongressBillsFetcher
        collection = "bills"
        kind = "bill"
        type_field = "bill_type"
    elif endpoint == "uscongress.amendments":
        fetcher = CongressAmendmentsFetcher
        collection = "amendments"
        kind = "amendment"
        type_field = "amendment_type"
    else:
        raise ValueError(f"Unsupported Congress collection endpoint: {endpoint}")

    query = fetcher.transform_query(dict(kwargs))
    congress = int(getattr(query, "congress"))
    subtype = str(getattr(query, type_field))
    credential = getattr(obb.user.credentials, "congress_gov_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    if not credential:
        raise RuntimeError("Congress.gov API key is required")

    page_size = 250
    offset = 0
    total: int | None = None
    records: list[dict[str, Any]] = []
    seen_page_signatures: set[str] = set()
    progress = tqdm(
        total=None,
        desc=f"congress:{congress}/{subtype}"[:64],
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        while total is None or offset < total:
            params: dict[str, Any] = {
                "limit": page_size,
                "offset": offset,
                "sort": f"updateDate+{getattr(query, 'sort_by', 'asc')}",
                "format": "json",
                "api_key": str(credential),
            }
            if getattr(query, "start_date", None):
                params["fromDateTime"] = f"{query.start_date.isoformat()}T00:00:00Z"
            if getattr(query, "end_date", None):
                params["toDateTime"] = f"{query.end_date.isoformat()}T23:59:59Z"
            url = (
                f"https://api.congress.gov/v3/{kind}/{congress}/{subtype}?"
                + urlencode(params)
            )
            checkpoint = _load_request_checkpoint(checkpoint_dir, url)
            if checkpoint is not None:
                payload = checkpoint
            else:
                if offset > 0 and page_limiter is not None:
                    page_limiter.wait()
                payload = _congress_json(url)
            error = payload.get("error")
            if isinstance(error, Mapping) and error:
                raise RuntimeError(
                    f"Congress.gov {error.get('code', '')}: {error.get('message', '')}"
                )
            page = payload.get(collection) or []
            if not isinstance(page, list):
                raise TypeError(f"Congress.gov omitted the {collection} collection")
            page_signature = _raw_page_signature(page)
            if page_signature in seen_page_signatures:
                raise RuntimeError(f"Congress.gov pagination cycle at offset {offset}")
            seen_page_signatures.add(page_signature)
            records.extend(dict(item) for item in page if isinstance(item, Mapping))
            pagination = payload.get("pagination") or {}
            raw_count = (
                pagination.get("count") if isinstance(pagination, Mapping) else None
            )
            if raw_count is None:
                if len(page) >= page_size:
                    raise RuntimeError(
                        "Congress.gov omitted pagination count on a full page"
                    )
                total = offset + len(page)
            else:
                total = int(raw_count)
            if checkpoint is None:
                _save_request_checkpoint(checkpoint_dir, url, payload)
            progress.total = max(1, math.ceil(total / page_size))
            progress.update(1)
            progress.set_postfix(rows=len(records), total_rows=total, refresh=False)
            if not page or len(page) < page_size:
                break
            offset += len(page)
    finally:
        progress.close()

    if not records:
        raise EmptyDataError(
            f"Congress.gov returned no {collection} for {congress}/{subtype}"
        )
    if collection == "bills":
        for record in records:
            if record.get("latestAction") is None:
                record["latestAction"] = {}
    return _provider_result_rows(fetcher.transform_data(query, records))


def _congress_json(url: str) -> dict[str, Any]:
    """Read one Congress.gov JSON page with an archive-appropriate timeout."""
    from urllib.request import Request, urlopen

    request = Request(
        url,
        headers={"User-Agent": "stockAgent-openbb-archive/1.0"},
    )
    with urlopen(request, timeout=60) as response:
        payload = json.load(response)
    if not isinstance(payload, Mapping):
        raise TypeError("Congress.gov returned a non-object JSON payload")
    return dict(payload)


def _fetch_congress_info_workaround(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    kind: str,
    page_limiter: Any | None = None,
    show_progress: bool = False,
    checkpoint_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Fetch complete bill/amendment metadata with paced pagination.

    OpenBB's info adapters use the short default HTTP timeout and read only the
    first page of each linked metadata collection.  Archive tasks need a longer
    timeout and all pages, while still excluding PDF/HTML document bodies.
    """
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    from openbb_core.provider.utils.errors import EmptyDataError

    if kind not in {"bill", "amendment"}:
        raise ValueError(f"Unsupported Congress info kind: {kind}")
    argument = f"{kind}_url"
    raw_url = str(kwargs.get(argument) or "").strip()
    if not raw_url:
        raise ValueError(f"{argument} is required")
    if raw_url[0].isdigit() or (raw_url.startswith("/") and raw_url[1:2].isdigit()):
        path = raw_url[1:] if raw_url.startswith("/") else raw_url
        raw_url = f"https://api.congress.gov/v3/{kind}/{path}?format=json"

    credential = getattr(obb.user.credentials, "congress_gov_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    api_key = str(credential or "")

    def authorized_url(url: str, *, page_size: int | None = None) -> str:
        split = urlsplit(url)
        query = dict(parse_qsl(split.query, keep_blank_values=True))
        query["format"] = "json"
        query["api_key"] = api_key
        if page_size is not None:
            query["limit"] = str(page_size)
        return urlunsplit(
            (split.scheme, split.netloc, split.path, urlencode(query), split.fragment)
        )

    request_count = 0

    def fetch(url: str, *, page_size: int | None = None) -> dict[str, Any]:
        nonlocal request_count
        request_url = authorized_url(url, page_size=page_size)
        checkpoint = _load_request_checkpoint(checkpoint_dir, request_url)
        if checkpoint is not None:
            return checkpoint
        if request_count > 0 and page_limiter is not None:
            page_limiter.wait()
        request_count += 1
        payload = _congress_json(request_url)
        error = payload.get("error")
        if isinstance(error, Mapping) and error:
            code = str(error.get("code") or "")
            if "API_KEY" in code.upper():
                raise RuntimeError(
                    f"Congress.gov invalid API key: {code} {error.get('message', '')}"
                )
            raise RuntimeError(f"Congress.gov {code}: {error.get('message', '')}")
        _save_request_checkpoint(checkpoint_dir, request_url, payload)
        return payload

    base = fetch(raw_url)
    entity = base.get(kind)
    if not isinstance(entity, Mapping) or not entity:
        raise EmptyDataError(f"Congress.gov returned no {kind} metadata for {raw_url}")
    record = dict(entity)

    if kind == "amendment":
        collections: tuple[tuple[str, str, tuple[str, ...]], ...] = (
            ("cosponsors", "cosponsors", ()),
            ("actions", "actions", ()),
            ("textVersions", "textVersions", ()),
        )
    else:
        collections = (
            ("cosponsors", "cosponsors", ()),
            ("subjects", "subjects", ("legislativeSubjects",)),
            ("summaries", "summaries", ()),
            ("committees", "committees", ()),
            ("actions", "actions", ()),
            ("titles", "titles", ()),
            ("relatedBills", "relatedBills", ()),
        )

    references = [
        (field, response_key, nested, reference)
        for field, response_key, nested in collections
        if isinstance((reference := record.get(field)), Mapping)
        and reference.get("url")
        and int(reference.get("count") or 0) > 0
    ]
    progress = tqdm(
        total=1 + len(references),
        initial=1,
        desc=f"congress:{kind} metadata",
        unit="page",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for field, response_key, nested, reference in references:
            next_url = str(reference["url"])
            values: list[Any] = []
            seen_urls: set[str] = set()
            while next_url:
                if next_url in seen_urls:
                    raise RuntimeError(
                        f"Congress.gov pagination cycle for {field}: {next_url}"
                    )
                seen_urls.add(next_url)
                page = fetch(next_url, page_size=250)
                payload: Any = page.get(response_key, [])
                for component in nested:
                    payload = (
                        payload.get(component, [])
                        if isinstance(payload, Mapping)
                        else []
                    )
                if isinstance(payload, list):
                    values.extend(payload)
                pagination = page.get("pagination")
                candidate = (
                    pagination.get("next") if isinstance(pagination, Mapping) else None
                )
                next_url = str(candidate) if candidate else ""
                if next_url:
                    progress.total = int(progress.total or 0) + 1
                progress.update(1)
                progress.set_postfix(field=field, items=len(values), refresh=False)
            if values:
                record[field] = values
    finally:
        progress.close()

    # Provenance never contains the credential-bearing URL.
    record["source_url"] = raw_url
    return [record]


def _quarantine_obsolete_output_path(
    output_path: Path, *, task_id: str = "manifest-reconcile"
) -> Path | None:
    """Move a prior successful shard out of ``data/`` after terminal recheck.

    A task can be requeued while its last known-good Parquet remains available
    for crash recovery. If the fresh provider evidence proves the partition is
    empty or inaccessible, that old file must not survive as an unreferenced
    data shard. Preserve it under ``_quarantine`` for audit/recovery instead of
    deleting it.
    """
    output_path = Path(output_path)
    if not output_path.is_file():
        return None
    data_dir = next(
        (parent for parent in output_path.parents if parent.name == "data"),
        None,
    )
    if data_dir is None:
        return None
    try:
        relative = output_path.relative_to(data_dir)
    except ValueError:
        return None
    destination = data_dir.parent / "_quarantine" / "obsolete_task_outputs" / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination = destination.with_name(
            f"{destination.stem}.{time.time_ns()}{destination.suffix}"
        )
    try:
        output_path.replace(destination)
    except OSError as exc:
        print(
            "[openbb-quarantine] unable to move obsolete shard "
            f"task={task_id} path={output_path}: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return None
    return destination


def _quarantine_obsolete_task_output(task: DownloadTask) -> Path | None:
    return _quarantine_obsolete_output_path(
        Path(task.output_path), task_id=task.task_id
    )


def _table_from_pylist_union_schema(
    rows: Sequence[Mapping[str, Any]],
) -> pa.Table:
    """Build an Arrow table without dropping fields absent from the first row.

    ``Table.from_pylist`` infers field names from the first mapping. Provider
    result sets are legitimately heterogeneous: validation evidence may occur
    only on a later row, and BLS catalog rows mix series with code-map fields.
    Discover the ordered key union first. If later rows introduce fields, add
    one all-null schema sentinel and slice it away after inference; this avoids
    copying every large record while preserving every endpoint's columns.
    """
    if not rows:
        return pa.Table.from_pylist([])
    union_keys = list(rows[0])
    seen = set(union_keys)
    has_late_fields = False
    for row in rows[1:]:
        for key in row:
            if key in seen:
                continue
            seen.add(key)
            union_keys.append(key)
            has_late_fields = True
    if not has_late_fields:
        return pa.Table.from_pylist(rows)
    schema_sentinel = {key: None for key in union_keys}
    return pa.Table.from_pylist([schema_sentinel, *rows]).slice(1)


def _atomic_write_parquet(
    records: list[dict[str, Any]],
    task: DownloadTask,
    provider: str,
    *,
    show_progress: bool = False,
) -> int:
    output_path = Path(task.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    query_json = _canonical_json(task.kwargs)
    enriched: list[dict[str, Any]] = []
    stage_progress = tqdm(
        total=4,
        desc=f"parquet:{task.endpoint}"[:64],
        unit="stage",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    temporary = output_path.with_name(
        f".{output_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        stage_progress.set_postfix(
            stage="enrich rows", rows=len(records), refresh=False
        )
        row_progress = tqdm(
            records,
            total=len(records),
            desc=f"write:{task.endpoint} enrich"[:64],
            unit="row",
            position=3,
            leave=False,
            disable=not show_progress,
        )
        try:
            for record in row_progress:
                row = dict(record)
                row.update(
                    {
                        "_openbb_endpoint": task.endpoint,
                        "_provider": provider,
                        "_scope_key": task.scope_key,
                        "_retrieved_at": retrieved_at,
                        "_query_json": query_json,
                    }
                )
                enriched.append(row)
        finally:
            row_progress.close()
        stage_progress.update(1)
        stage_progress.set_postfix(stage="build Arrow table", refresh=False)
        table = _table_from_pylist_union_schema(enriched)
        stage_progress.update(1)
        stage_progress.set_postfix(stage="zstd parquet write", refresh=False)
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            compression_level=6,
            use_dictionary=True,
            write_statistics=True,
        )
        stage_progress.update(1)
        stage_progress.set_postfix(stage="fsync and publish", refresh=False)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        temporary.replace(output_path)
        stage_progress.update(1)
        return table.num_rows
    finally:
        temporary.unlink(missing_ok=True)
        stage_progress.close()


def _atomic_write_arrow_table(
    table: pa.Table,
    task: DownloadTask,
    provider: str,
    *,
    show_progress: bool = False,
) -> int:
    """Publish an already normalized Arrow table without Python row materialization."""
    output_path = Path(task.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(
        f".{output_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    progress = tqdm(
        total=3,
        desc=f"parquet:{task.endpoint}"[:64],
        unit="stage",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        progress.set_postfix(stage="add provenance", rows=table.num_rows, refresh=False)
        provenance = {
            "_openbb_endpoint": task.endpoint,
            "_provider": provider,
            "_scope_key": task.scope_key,
            "_retrieved_at": datetime.now(timezone.utc).isoformat(),
            "_query_json": _canonical_json(task.kwargs),
        }
        for name, value in provenance.items():
            column = pa.repeat(pa.scalar(value, type=pa.string()), table.num_rows)
            if name in table.column_names:
                index = table.column_names.index(name)
                table = table.set_column(index, name, column)
            else:
                table = table.append_column(name, column)
        progress.update(1)
        progress.set_postfix(stage="zstd parquet write", refresh=False)
        with _LOCAL_CPU_BUDGET.claim(1):
            pq.write_table(
                table,
                temporary,
                compression="zstd",
                compression_level=6,
                use_dictionary=True,
                write_statistics=True,
            )
        progress.update(1)
        progress.set_postfix(stage="fsync and publish", refresh=False)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        temporary.replace(output_path)
        progress.update(1)
        return table.num_rows
    finally:
        temporary.unlink(missing_ok=True)
        progress.close()


_ATOMIC_PARQUET_TEMP_RE = re.compile(
    r"^\.(?P<final_name>.+\.parquet)\."
    r"(?P<pid>[1-9][0-9]*)\.(?P<thread>[1-9][0-9]*)\.tmp$"
)


def quarantine_stale_atomic_parquet_temps(output_dir: Path) -> list[Path]:
    """Move dead-writer Parquet temporaries out of the product data tree.

    Atomic writers intentionally leave an incomplete temporary behind when a
    process is terminated mid-write. Only the encoded writer PID establishes
    whether a file is stale; age alone would race a legitimately slow write.
    Preserve stale files under quarantine instead of deleting them.
    """
    data_dir = output_dir / "data"
    if not data_dir.is_dir():
        return []
    quarantine_dir = output_dir / "_state" / "quarantine" / "stale_atomic_tmp"
    moved: list[Path] = []
    for temporary in data_dir.rglob("*.tmp"):
        match = _ATOMIC_PARQUET_TEMP_RE.fullmatch(temporary.name)
        if match is None:
            continue
        pid = int(match.group("pid"))
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        except PermissionError:
            continue
        else:
            continue
        relative = temporary.relative_to(data_dir)
        target = quarantine_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target = target.with_name(f"{target.name}.{time.time_ns()}")
        try:
            temporary.replace(target)
        except FileNotFoundError:
            # Another cleanup process or a finishing writer won the race.
            continue
        moved.append(target)
    return moved


def _filter_company_news_to_task_range(
    records: Sequence[Mapping[str, Any]],
    task: DownloadTask,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Reject provider rows outside a historical company-news task window."""
    raw_start = task.kwargs.get("start_date")
    raw_end = task.kwargs.get("end_date")
    if not raw_start and not raw_end:
        return [dict(record) for record in records]
    start = date.fromisoformat(str(raw_start)) if raw_start else date.min
    end = date.fromisoformat(str(raw_end)) if raw_end else date.max
    filtered: list[dict[str, Any]] = []
    progress = tqdm(
        records,
        total=len(records),
        desc=f"filter:{task.endpoint}:{task.scope_key}"[:64],
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    for record in progress:
        value = _first_present(record, ("date", "published_date", "publishedDate"))
        if value is None:
            continue
        try:
            record_date = (
                value.date()
                if isinstance(value, datetime)
                else value
                if isinstance(value, date)
                else date.fromisoformat(str(value)[:10])
            )
        except (TypeError, ValueError):
            continue
        if start <= record_date <= end:
            filtered.append(dict(record))
    progress.close()
    return filtered


_TEMPORAL_RECORD_FIELDS = (
    "date",
    "datetime",
    "timestamp",
    "observation_date",
    "published_date",
    "publishedDate",
    "filing_date",
    "filingDate",
    "accepted_date",
    "acceptedDate",
    "period_ending",
    "periodEnding",
)


def _filter_records_to_task_range(
    records: Sequence[Mapping[str, Any]],
    task: DownloadTask,
    *,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Apply one date-boundary rule to every historical route.

    Providers do not consistently honor OpenBB's start/end fields.  The
    planner still owns the requested interval, so any row carrying a
    recognizable temporal field is checked before publication.  Metadata rows
    without a temporal field are retained; they are not observations that can
    be incorrectly rejected by this guard.
    """
    raw_start = task.kwargs.get("start_date")
    raw_end = task.kwargs.get("end_date")
    if not raw_start and not raw_end:
        return [dict(record) for record in records]
    start = date.fromisoformat(str(raw_start)[:10]) if raw_start else date.min
    end = date.fromisoformat(str(raw_end)[:10]) if raw_end else date.max

    def parse_value(value: Any) -> date | None:
        try:
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value)[:10])
        except (TypeError, ValueError):
            return None

    has_temporal = any(
        any(record.get(field) not in {None, ""} for field in _TEMPORAL_RECORD_FIELDS)
        for record in records
    )
    if not has_temporal:
        return [dict(record) for record in records]
    filtered: list[dict[str, Any]] = []
    progress = tqdm(
        records,
        total=len(records),
        desc=f"filter:{task.endpoint}:{task.scope_key}"[:64],
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for record in progress:
            values = [
                parsed
                for field in _TEMPORAL_RECORD_FIELDS
                if (parsed := parse_value(record.get(field))) is not None
            ]
            if not values or any(start <= value <= end for value in values):
                filtered.append(dict(record))
    finally:
        progress.close()
    return filtered


def _bls_labstat_lock(prefix: str) -> threading.Lock:
    with _BLS_LABSTAT_LOCKS_GUARD:
        return _BLS_LABSTAT_LOCKS.setdefault(prefix, threading.Lock())


def _bls_labstat_build_threads() -> int:
    """Divide native CPU threads across the bounded concurrent index builds."""
    cpu_count = max(1, os.cpu_count() or 1)
    default = max(1, _LOCAL_CPU_BUDGET.total // BLS_LABSTAT_PARALLEL_BUILDS)
    raw_override = os.environ.get("OPENBB_BLS_BUILD_THREADS")
    if raw_override is None:
        return default
    try:
        requested = int(raw_override)
    except ValueError as exc:
        raise ValueError("OPENBB_BLS_BUILD_THREADS must be a positive integer") from exc
    if requested <= 0:
        raise ValueError("OPENBB_BLS_BUILD_THREADS must be a positive integer")
    return min(cpu_count, requested)


def _bls_labstat_headers() -> dict[str, str]:
    """Use a declared archive identity accepted by the BLS download host."""
    return {
        "User-Agent": os.environ.get(
            "BLS_USER_AGENT",
            "stockAgent-openbb-archive/1.0 local-operator@example.com",
        ),
        "Accept": "text/plain,text/html;q=0.9,*/*;q=0.1",
    }


def _bls_labstat_receipt_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.receipt.json")


def _bls_labstat_cached_file_valid(path: Path, url: str) -> bool:
    receipt_path = _bls_labstat_receipt_path(path)
    if not path.is_file() or not receipt_path.is_file():
        return False
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        return bool(
            receipt.get("schema_version") == BLS_LABSTAT_CACHE_SCHEMA_VERSION
            and receipt.get("url") == url
            and int(receipt.get("size_bytes") or -1) == path.stat().st_size
            and re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("sha256") or ""))
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def _download_bls_labstat_file(
    url: str,
    path: Path,
    *,
    show_progress: bool = False,
) -> Path:
    """Download one LABSTAT artifact with a byte-resumable durable receipt."""
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    if _bls_labstat_cached_file_valid(path, url):
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.part")
    offset = partial.stat().st_size if partial.is_file() else 0
    headers = _bls_labstat_headers()
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = Request(url, headers=headers)
    try:
        response = urlopen(request, timeout=180)
    except HTTPError as exc:
        if exc.code == 416 and partial.is_file():
            partial.unlink(missing_ok=True)
        raise
    with response:
        status = int(getattr(response, "status", 200) or 200)
        content_range = str(response.headers.get("Content-Range") or "")
        appending = bool(
            offset and status == 206 and content_range.startswith(f"bytes {offset}-")
        )
        if not appending:
            offset = 0
        content_length = response.headers.get("Content-Length")
        expected_total = (
            offset + int(content_length) if content_length is not None else None
        )
        progress = tqdm(
            total=expected_total,
            initial=offset,
            desc=f"bls:bulk {path.name}"[:64],
            unit="B",
            unit_scale=True,
            position=2,
            leave=False,
            disable=not show_progress,
        )
        try:
            with partial.open("ab" if appending else "wb") as stream:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    stream.write(chunk)
                    progress.update(len(chunk))
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            progress.close()
    if expected_total is not None and partial.stat().st_size != expected_total:
        raise OSError(
            f"Incomplete LABSTAT download for {url}: "
            f"{partial.stat().st_size} != {expected_total}"
        )
    digest = hashlib.sha256()
    hash_progress = tqdm(
        total=partial.stat().st_size,
        desc=f"bls:verify {path.name}"[:64],
        unit="B",
        unit_scale=True,
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        with partial.open("rb") as stream:
            for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                digest.update(chunk)
                hash_progress.update(len(chunk))
    finally:
        hash_progress.close()
    partial.replace(path)
    _write_json_atomic(
        _bls_labstat_receipt_path(path),
        {
            "schema_version": BLS_LABSTAT_CACHE_SCHEMA_VERSION,
            "url": url,
            "size_bytes": path.stat().st_size,
            "sha256": digest.hexdigest(),
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return path


def _bls_labstat_catalog_files(
    page: str, prefix: str
) -> tuple[str, str | None, list[str]]:
    """Select a complete survey data file plus title/footnote dictionaries."""
    from html import unescape
    from urllib.parse import unquote, urlsplit

    names: set[str] = set()
    for href in re.findall(r"href\s*=\s*['\"]([^'\"]+)['\"]", page, re.I):
        name = Path(unquote(urlsplit(unescape(href)).path)).name
        if name and re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            names.add(name)
    lower_to_name = {name.lower(): name for name in names}
    series_name = lower_to_name.get(f"{prefix}.series")
    if series_name is None:
        raise BlsLabstatUnsupportedError(
            f"LABSTAT directory {prefix!r} has no {prefix}.series catalog"
        )
    footnote_name = lower_to_name.get(f"{prefix}.footnote")
    data_names = sorted(
        name for name in names if name.lower().startswith(f"{prefix}.data.")
    )
    canonical_complete_suffixes = (".alldata", ".allitems", ".allcesseries")
    complete = [
        name
        for name in data_names
        if name.lower().endswith(canonical_complete_suffixes)
    ]
    if not complete:
        complete = [
            name
            for name in data_names
            if name.rsplit(".", 1)[-1].lower().startswith("all")
        ]
    selected = complete or [
        name for name in data_names if not name.lower().endswith(".current")
    ]
    if not selected:
        raise BlsLabstatUnsupportedError(
            f"LABSTAT directory {prefix!r} has no complete historical data file"
        )
    return series_name, footnote_name, selected


def _bls_labstat_database_valid(database: Path, receipt_path: Path) -> bool:
    if not database.is_file() or not receipt_path.is_file():
        return False
    stat = database.stat()
    fingerprint = (int(stat.st_size), int(stat.st_mtime_ns))
    with _BLS_LABSTAT_LOCKS_GUARD:
        if _BLS_LABSTAT_READY_DATABASES.get(str(database)) == fingerprint:
            return True
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("schema_version") != BLS_LABSTAT_CACHE_SCHEMA_VERSION:
            return False
        import duckdb

        connection = duckdb.connect(str(database), read_only=True)
        try:
            observed = int(
                connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            )
        finally:
            connection.close()
        valid = observed == int(receipt.get("observation_rows") or -1)
        if valid:
            with _BLS_LABSTAT_LOCKS_GUARD:
                _BLS_LABSTAT_READY_DATABASES[str(database)] = fingerprint
        return valid
    except Exception:
        # DuckDB exception classes are version-specific. Invalid local caches
        # are rebuilt under the prefix lock and never change task semantics.
        return False


def _create_bls_labstat_series_table(connection: Any, series_path: Path) -> None:
    """Normalize survey-specific LABSTAT series schemas.

    Not every official ``*.series`` catalog publishes a ``series_title``
    column (notably the international and productivity surveys).  Inspect the
    normalized CSV schema once at build time and preserve a nullable title
    instead of letting DuckDB bind the output alias as a missing input column.
    """
    describe_rows = connection.execute(
        """
        DESCRIBE SELECT * FROM read_csv(
            ?, delim='\t', header=true, all_varchar=true,
            normalize_names=true, ignore_errors=true
        )
        """,
        [str(series_path)],
    ).fetchall()
    columns = {str(row[0]) for row in describe_rows}
    if "series_id" not in columns:
        raise BlsLabstatUnsupportedError(
            f"LABSTAT series catalog {series_path.name!r} has no series_id column"
        )
    title_expression = (
        "nullif(trim(series_title), '')"
        if "series_title" in columns
        else "NULL::VARCHAR"
    )
    connection.execute(
        f"""
        CREATE TABLE series AS
        SELECT trim(series_id) AS series_id,
               {title_expression} AS series_title
        FROM read_csv(
            ?, delim='\t', header=true, all_varchar=true,
            normalize_names=true, ignore_errors=true
        )
        WHERE trim(series_id) != ''
        """,
        [str(series_path)],
    )


def _ensure_bls_labstat_database(
    prefix: str,
    cache_dir: Path,
    *,
    show_progress: bool = False,
) -> Path:
    """Materialize one official LABSTAT survey into an indexed DuckDB cache."""
    from urllib.error import HTTPError
    from urllib.parse import urljoin

    prefix = str(prefix).strip().lower()
    if not re.fullmatch(r"[a-z0-9]{2}", prefix):
        raise BlsLabstatUnsupportedError(f"Invalid LABSTAT series prefix: {prefix!r}")
    prefix_dir = cache_dir / prefix
    database = prefix_dir / f"{prefix}.duckdb"
    database_receipt = prefix_dir / f"{prefix}.duckdb.receipt.json"
    if _bls_labstat_database_valid(database, database_receipt):
        return database
    with _bls_labstat_lock(prefix):
        if _bls_labstat_database_valid(database, database_receipt):
            return database
        with _BLS_LABSTAT_BUILD_SEMAPHORE:
            directory_url = urljoin(BLS_LABSTAT_BASE_URL, f"{prefix}/")
            listing_path = prefix_dir / "index.html"
            try:
                _download_bls_labstat_file(
                    directory_url,
                    listing_path,
                    show_progress=show_progress,
                )
            except HTTPError as exc:
                if exc.code in {400, 401, 403, 404}:
                    raise BlsLabstatUnsupportedError(
                        f"LABSTAT directory {prefix!r} returned HTTP {exc.code}"
                    ) from exc
                raise
            page = listing_path.read_text(encoding="utf-8", errors="replace")
            series_name, footnote_name, data_names = _bls_labstat_catalog_files(
                page, prefix
            )
            raw_dir = prefix_dir / "raw"
            requested_names = [series_name, *data_names]
            if footnote_name is not None:
                requested_names.append(footnote_name)
            raw_paths: dict[str, Path] = {}
            for name in requested_names:
                try:
                    raw_paths[name] = _download_bls_labstat_file(
                        urljoin(directory_url, name),
                        raw_dir / name,
                        show_progress=show_progress,
                    )
                except HTTPError as exc:
                    if exc.code in {400, 401, 403, 404}:
                        raise BlsLabstatUnsupportedError(
                            f"LABSTAT file {name!r} returned HTTP {exc.code}"
                        ) from exc
                    raise

            import duckdb

            prefix_dir.mkdir(parents=True, exist_ok=True)
            temporary = database.with_name(
                f".{database.name}.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            temporary.unlink(missing_ok=True)
            observation_rows = 0
            build_threads = _bls_labstat_build_threads()
            cpu_slots = _LOCAL_CPU_BUDGET.acquire(build_threads)
            connection = None
            import_progress = None
            try:
                connection = duckdb.connect(str(temporary))
                import_progress = tqdm(
                    total=len(data_names) + 3,
                    desc=f"bls:index {prefix}",
                    unit="stage",
                    position=2,
                    leave=False,
                    disable=not show_progress,
                )
                connection.execute(f"PRAGMA threads={build_threads}")
                connection.execute(
                    """
                    CREATE TABLE observations(
                        series_id VARCHAR,
                        year INTEGER,
                        period VARCHAR,
                        value DOUBLE,
                        footnote_codes VARCHAR
                    )
                    """
                )
                for name in data_names:
                    import_progress.set_postfix(file=name[:40], refresh=False)
                    connection.execute(
                        """
                        INSERT INTO observations
                        SELECT
                            trim(series_id),
                            try_cast(trim(_year) AS INTEGER),
                            trim(period),
                            try_cast(trim(_value) AS DOUBLE),
                            nullif(trim(footnote_codes), '')
                        FROM read_csv(
                            ?, delim='\t', header=true, all_varchar=true,
                            normalize_names=true, ignore_errors=true
                        )
                        WHERE trim(series_id) != ''
                          AND try_cast(trim(_year) AS INTEGER) IS NOT NULL
                        """,
                        [str(raw_paths[name])],
                    )
                    import_progress.update(1)
                connection.execute(
                    "CREATE INDEX observations_series_year "
                    "ON observations(series_id, year)"
                )
                import_progress.update(1)
                _create_bls_labstat_series_table(connection, raw_paths[series_name])
                connection.execute("CREATE INDEX series_id_idx ON series(series_id)")
                import_progress.update(1)
                if footnote_name is not None:
                    connection.execute(
                        """
                        CREATE TABLE footnotes AS
                        SELECT trim(footnote_code) AS footnote_code,
                               trim(footnote_text) AS footnote_text
                        FROM read_csv(
                            ?, delim='\t', header=true, all_varchar=true,
                            normalize_names=true, ignore_errors=true
                        )
                        WHERE trim(footnote_code) != ''
                        """,
                        [str(raw_paths[footnote_name])],
                    )
                else:
                    connection.execute(
                        "CREATE TABLE footnotes("
                        "footnote_code VARCHAR, footnote_text VARCHAR)"
                    )
                import_progress.update(1)
                observation_rows = int(
                    connection.execute("SELECT COUNT(*) FROM observations").fetchone()[
                        0
                    ]
                )
                if observation_rows <= 0:
                    raise BlsLabstatUnsupportedError(
                        f"LABSTAT {prefix!r} import produced no observations"
                    )
                connection.execute("CHECKPOINT")
            finally:
                if connection is not None:
                    connection.close()
                if import_progress is not None:
                    import_progress.close()
                _LOCAL_CPU_BUDGET.release(cpu_slots)
            temporary.replace(database)
            _write_json_atomic(
                database_receipt,
                {
                    "schema_version": BLS_LABSTAT_CACHE_SCHEMA_VERSION,
                    "prefix": prefix,
                    "source_url": directory_url,
                    "source_files": requested_names,
                    "observation_rows": observation_rows,
                    "built_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            built_stat = database.stat()
            with _BLS_LABSTAT_LOCKS_GUARD:
                _BLS_LABSTAT_READY_DATABASES[str(database)] = (
                    int(built_stat.st_size),
                    int(built_stat.st_mtime_ns),
                )
    return database


def _fetch_bls_series_labstat_table(
    kwargs: Mapping[str, Any],
    *,
    cache_dir: Path,
    show_progress: bool = False,
) -> pa.Table:
    """Read and normalize official BLS bulk history on the native columnar path."""
    import duckdb
    from openbb_core.provider.utils.errors import EmptyDataError

    raw_symbols = kwargs.get("symbol") or ""
    symbols = [
        str(item).strip().upper()
        for item in (
            raw_symbols.split(",")
            if isinstance(raw_symbols, str)
            else list(raw_symbols)
        )
        if str(item).strip()
    ]
    if not symbols:
        raise EmptyDataError("No BLS series IDs were supplied.")
    by_prefix: dict[str, list[str]] = {}
    for symbol in dict.fromkeys(symbols):
        if len(symbol) < 2 or not symbol[:2].isalnum():
            raise BlsLabstatUnsupportedError(
                f"Cannot map BLS series {symbol!r} to a LABSTAT survey"
            )
        by_prefix.setdefault(symbol[:2].lower(), []).append(symbol)

    start = date.fromisoformat(str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10])
    end = date.fromisoformat(
        str(kwargs.get("end_date") or date.today().isoformat())[:10]
    )
    include_annual_average = bool(kwargs.get("annual_average", False))
    calculate = bool(kwargs.get("calculations", True))
    tables: list[pa.Table] = []
    for prefix, prefix_symbols in by_prefix.items():
        database = _ensure_bls_labstat_database(
            prefix,
            cache_dir,
            show_progress=show_progress,
        )
        cpu_slots = _LOCAL_CPU_BUDGET.acquire(1)
        connection = None
        try:
            connection = duckdb.connect(str(database), read_only=True)
            # Every task query gets one native DuckDB thread. Parallelism lives
            # across independent archive tasks, so N workers consume about N
            # cores instead of each query recursively oversubscribing the host.
            connection.execute("PRAGMA threads=1")
            table = connection.execute(
                r"""
                WITH normalized AS (
                    SELECT
                        o.series_id,
                        o.period,
                        o.value,
                        o.footnote_codes,
                        s.series_title,
                        CASE
                            WHEN o.period BETWEEN 'M01' AND 'M12'
                                THEN make_date(
                                    o.year,
                                    try_cast(substr(o.period, 2) AS INTEGER),
                                    1
                                )
                            WHEN o.period = 'M13' THEN make_date(o.year, 12, 31)
                            WHEN o.period IN ('A01', 'S01', 'Q01')
                                THEN make_date(o.year, 1, 1)
                            WHEN o.period = 'Q02' THEN make_date(o.year, 4, 1)
                            WHEN o.period IN ('S02', 'Q03')
                                THEN make_date(o.year, 7, 1)
                            WHEN o.period = 'Q04' THEN make_date(o.year, 10, 1)
                            WHEN o.period IN ('S03', 'Q05')
                                THEN make_date(o.year, 12, 31)
                            ELSE NULL
                        END AS observation_date
                    FROM observations AS o
                    LEFT JOIN series AS s USING(series_id)
                    WHERE o.series_id IN (SELECT unnest(?))
                      AND o.year BETWEEN ? AND ?
                      AND o.value IS NOT NULL
                      AND (? OR o.period NOT IN ('M13', 'Q05', 'S03'))
                ),
                dated AS (
                    SELECT * EXCLUDE(duplicate_rank)
                    FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id, observation_date
                            ORDER BY period
                        ) AS duplicate_rank
                        FROM normalized
                        WHERE observation_date IS NOT NULL
                    )
                    WHERE duplicate_rank = 1
                ),
                enriched AS (
                    SELECT
                        d.*,
                        max(d.observation_date) OVER (
                            PARTITION BY d.series_id
                        ) AS latest_date,
                        (
                            SELECT string_agg(
                                f.footnote_text,
                                '; ' ORDER BY u.ordinality
                            )
                            FROM unnest(
                                regexp_split_to_array(
                                    coalesce(d.footnote_codes, ''),
                                    '[,;\s]+'
                                )
                            ) WITH ORDINALITY AS u(code, ordinality)
                            JOIN footnotes AS f
                              ON f.footnote_code = trim(u.code)
                            WHERE trim(u.code) != ''
                        ) AS footnotes
                    FROM dated AS d
                )
                SELECT
                    d.series_id AS symbol,
                    CASE
                        WHEN d.series_title IS NULL THEN NULL
                        WHEN d.period IN ('M13', 'Q05', 'S03')
                            THEN d.series_title || ' (Annual Average)'
                        ELSE d.series_title
                    END AS title,
                    strftime(d.observation_date, '%Y-%m-%d') AS date,
                    d.value,
                    d.observation_date = d.latest_date AS latest,
                    d.footnotes,
                    CASE WHEN ? THEN d.value - p1.value ELSE NULL::DOUBLE END
                        AS "change_1M",
                    CASE WHEN ? THEN d.value - p3.value ELSE NULL::DOUBLE END
                        AS "change_3M",
                    CASE WHEN ? THEN d.value - p6.value ELSE NULL::DOUBLE END
                        AS "change_6M",
                    CASE WHEN ? THEN d.value - p12.value ELSE NULL::DOUBLE END
                        AS "change_12M",
                    CASE WHEN ? AND p1.value != 0
                        THEN round((d.value - p1.value) / p1.value, 3)
                        ELSE NULL::DOUBLE END AS "change_percent_1M",
                    CASE WHEN ? AND p3.value != 0
                        THEN round((d.value - p3.value) / p3.value, 3)
                        ELSE NULL::DOUBLE END AS "change_percent_3M",
                    CASE WHEN ? AND p6.value != 0
                        THEN round((d.value - p6.value) / p6.value, 3)
                        ELSE NULL::DOUBLE END AS "change_percent_6M",
                    CASE WHEN ? AND p12.value != 0
                        THEN round((d.value - p12.value) / p12.value, 3)
                        ELSE NULL::DOUBLE END AS "change_percent_12M",
                    'labstat_bulk' AS _bls_source,
                    ? AS _bls_labstat_prefix
                FROM enriched AS d
                LEFT JOIN dated AS p1
                  ON p1.series_id = d.series_id
                 AND p1.observation_date = d.observation_date - INTERVAL '1 month'
                LEFT JOIN dated AS p3
                  ON p3.series_id = d.series_id
                 AND p3.observation_date = d.observation_date - INTERVAL '3 months'
                LEFT JOIN dated AS p6
                  ON p6.series_id = d.series_id
                 AND p6.observation_date = d.observation_date - INTERVAL '6 months'
                LEFT JOIN dated AS p12
                  ON p12.series_id = d.series_id
                 AND p12.observation_date = d.observation_date - INTERVAL '12 months'
                WHERE d.observation_date BETWEEN ? AND ?
                ORDER BY list_position(?, d.series_id), d.observation_date DESC
                """,
                [
                    prefix_symbols,
                    start.year - 1,
                    end.year,
                    include_annual_average,
                    *([calculate] * 8),
                    prefix,
                    start,
                    end,
                    prefix_symbols,
                ],
            ).to_arrow_table()
            tables.append(table)
        finally:
            if connection is not None:
                connection.close()
            _LOCAL_CPU_BUDGET.release(cpu_slots)
    result = (
        tables[0]
        if len(tables) == 1
        else pa.concat_tables(tables, promote_options="default")
    )
    if result.num_rows == 0:
        raise EmptyDataError("The LABSTAT bulk request was returned empty.")
    return result


def _fetch_bls_series_labstat(
    kwargs: Mapping[str, Any],
    *,
    cache_dir: Path,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Compatibility wrapper for callers that explicitly require Python rows."""
    return _fetch_bls_series_labstat_table(
        kwargs,
        cache_dir=cache_dir,
        show_progress=show_progress,
    ).to_pylist()


def _fetch_bls_series_resilient(
    kwargs: Mapping[str, Any],
    obb: Any,
    *,
    page_limiter: Any | None = None,
    show_progress: bool = False,
) -> list[dict[str, Any]]:
    """Fetch BLS series sequentially and split oversized failing requests.

    OpenBB normally creates all 50-series/20-year BLS subrequests concurrently.
    That bypasses the archive's provider-wide limiter and a few large BED
    payloads repeatedly time out or return HTTP 200 with ``Results: null``.
    Keep the official maximum chunk sizes for throughput, account for every
    HTTP request with the shared limiter, and recursively bisect payloads that
    prove too large.  A malformed response remains transient only after the
    request has reached one series and one year, where no finer split exists.
    """
    import asyncio

    from openbb_bls.utils.helpers import get_bls_timeseries
    from openbb_core.provider.utils.errors import EmptyDataError

    credential = getattr(obb.user.credentials, "bls_api_key", None)
    if hasattr(credential, "get_secret_value"):
        credential = credential.get_secret_value()
    api_key = str(credential or "")

    raw_symbols = kwargs.get("symbol") or ""
    symbols = [
        str(item).strip()
        for item in (
            raw_symbols.split(",")
            if isinstance(raw_symbols, str)
            else list(raw_symbols)
        )
        if str(item).strip()
    ]
    if not symbols:
        raise EmptyDataError("No BLS series IDs were supplied.")

    start = date.fromisoformat(str(kwargs.get("start_date") or DEFAULT_START_DATE)[:10])
    end = date.fromisoformat(
        str(kwargs.get("end_date") or date.today().isoformat())[:10]
    )
    jobs: list[tuple[list[str], int, int]] = []
    for offset in range(0, len(symbols), 50):
        symbol_chunk = symbols[offset : offset + 50]
        for first_year in range(start.year, end.year + 1, 20):
            jobs.append((symbol_chunk, first_year, min(first_year + 19, end.year)))

    records: list[dict[str, Any]] = []
    empty_messages: list[str] = []

    def split_job(
        symbol_chunk: list[str], first_year: int, last_year: int
    ) -> list[tuple[list[str], int, int]]:
        if len(symbol_chunk) > 1:
            midpoint = len(symbol_chunk) // 2
            return [
                (symbol_chunk[:midpoint], first_year, last_year),
                (symbol_chunk[midpoint:], first_year, last_year),
            ]
        if first_year < last_year:
            midpoint = (first_year + last_year) // 2
            return [
                (symbol_chunk, first_year, midpoint),
                (symbol_chunk, midpoint + 1, last_year),
            ]
        return []

    progress = tqdm(
        total=len(jobs),
        desc="bls:series requests",
        unit="request",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    index = 0
    try:
        while index < len(jobs):
            symbol_chunk, first_year, last_year = jobs[index]
            index += 1
            if index > 1 and page_limiter is not None:
                page_limiter.wait()
            try:
                payload = asyncio.run(
                    get_bls_timeseries(
                        api_key=api_key,
                        series_ids=symbol_chunk,
                        start_year=first_year,
                        end_year=last_year,
                        calculations=bool(kwargs.get("calculations", True)),
                        catalog=True,
                        annual_average=bool(kwargs.get("annual_average", False)),
                        aspects=bool(kwargs.get("aspects", False)),
                    )
                )
            except AttributeError as exc:
                if "'NoneType' object has no attribute 'get'" not in str(exc):
                    raise
                malformed = RuntimeError(
                    "BLS server error: malformed response contained a null object"
                )
                replacements = split_job(symbol_chunk, first_year, last_year)
                if not replacements:
                    raise malformed from exc
                jobs[index:index] = replacements
                # The original request was already part of the total.  Both
                # replacement requests are new work and must be visible in the
                # fine-grained progress bar.
                progress.total = int(progress.total or 0) + len(replacements)
                progress.set_postfix(
                    split=f"null:{len(symbol_chunk)}x{last_year - first_year + 1}",
                    refresh=False,
                )
                progress.update(1)
                continue
            except (TimeoutError, asyncio.TimeoutError):
                replacements = split_job(symbol_chunk, first_year, last_year)
                if not replacements:
                    raise
                jobs[index:index] = replacements
                progress.total = int(progress.total or 0) + len(replacements)
                progress.set_postfix(
                    split=f"timeout:{len(symbol_chunk)}x{last_year - first_year + 1}",
                    refresh=False,
                )
                progress.update(1)
                continue
            except EmptyDataError as exc:
                message = str(getattr(exc, "message", None) or exc)
                if _has_strong_rate_evidence(
                    message
                ) or _has_invalid_credential_evidence(message):
                    # BLS wraps quota exhaustion and invalid registration keys
                    # in EmptyDataError. Preserve the actionable upstream
                    # signal so the worker leaves the task pending or marks the
                    # credential unavailable instead of recording false empty.
                    raise RuntimeError(message) from exc
                empty_messages.append(message)
                progress.update(1)
                progress.set_postfix(
                    empty=len(empty_messages),
                    request=f"{len(symbol_chunk)}x{last_year - first_year + 1}",
                    refresh=False,
                )
                continue

            if isinstance(payload, EmptyDataError):
                message = str(getattr(payload, "message", None) or payload)
                if _has_strong_rate_evidence(
                    message
                ) or _has_invalid_credential_evidence(message):
                    raise RuntimeError(message)
                empty_messages.append(message)
            elif isinstance(payload, Mapping):
                records.extend(
                    dict(item)
                    for item in payload.get("data", [])
                    if isinstance(item, Mapping)
                )
            progress.update(1)
            progress.set_postfix(
                rows=len(records),
                request=f"{len(symbol_chunk)}x{last_year - first_year + 1}",
                refresh=False,
            )
    finally:
        progress.close()

    filtered: list[dict[str, Any]] = []
    filter_progress = tqdm(
        records,
        total=len(records),
        desc="bls:filter archive dates",
        unit="row",
        position=2,
        leave=False,
        disable=not show_progress,
    )
    try:
        for record in filter_progress:
            if (
                record.get("date")
                and start <= date.fromisoformat(str(record["date"])[:10]) <= end
            ):
                filtered.append(record)
    finally:
        filter_progress.close()
    if not filtered:
        detail = "; ".join(dict.fromkeys(empty_messages))
        raise EmptyDataError(detail or "The BLS request was returned empty.")
    return filtered


def _is_provider_parser_shape_error_text(value: object) -> bool:
    """Return whether evidence proves an adapter/container shape mismatch."""
    text = str(value or "").lower()
    return bool(
        re.search(
            r"['\"](?:nonetype|list|str|int|float)['\"] object has no "
            r"attribute ['\"](?:get|items|values|replace|upper|lower)['\"]",
            text,
        )
        or re.search(
            r"argument of type ['\"](?:nonetype|int|float)['\"] is not iterable",
            text,
        )
    )


def _is_provider_parser_shape_error(exc: Exception) -> bool:
    return _is_provider_parser_shape_error_text(f"{type(exc).__name__}: {exc}")


def _task_retry_delay_seconds(task_id: str, transient_failures: int) -> float:
    """Return bounded exponential delay with stable per-task spreading."""
    streak = max(1, int(transient_failures))
    exponent = min(30, streak - 1)
    raw_delay = min(TASK_RETRY_MAX_SECONDS, TASK_RETRY_BASE_SECONDS * (2**exponent))
    digest = hashlib.sha256(f"{task_id}:{streak}".encode("utf-8")).digest()
    fraction = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
    return max(1.0, raw_delay * (0.8 + 0.2 * fraction))


def _task_retry_deadline(
    task_id: str,
    transient_failures: int,
    *,
    now: datetime | None = None,
) -> str:
    current = now or datetime.now(timezone.utc)
    return (
        current
        + timedelta(seconds=_task_retry_delay_seconds(task_id, transient_failures))
    ).isoformat()


def _is_http_server_error(exc: Exception) -> bool:
    """Return whether the failure is a task-local HTTP 5xx response."""
    text = f"{type(exc).__name__}: {exc}".lower()
    status_code = getattr(exc, "code", None) or getattr(exc, "status", None)
    return bool(
        (isinstance(status_code, int) and 500 <= status_code <= 599)
        or re.search(
            r"(?:http(?:error| error)?|status(?: code)?)\s*[:=]?\s*5\d{2}(?!\d)",
            text,
        )
        or re.search(r"(?<!\d)5\d{2}(?!\d)\s*,\s*message\s*=", text)
    )


def _is_http_status_500(exc: Exception) -> bool:
    """Return whether an exception preserves an explicit HTTP 500 response."""
    status_code = getattr(exc, "code", None) or getattr(exc, "status", None)
    if status_code == 500:
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    return bool(
        re.search(
            r"(?:http(?:error| error)?|status(?: code)?)\s*[:=]?\s*500(?!\d)",
            text,
        )
        or re.search(r"(?<!\d)500(?!\d)\s*,\s*message\s*=", text)
    )


def classify_error(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}".lower()
    if "__archive_provider_deferred__" in text:
        return "deferred"
    if (
        isinstance(exc, ProviderResponseShapeError)
        or "providerresponseshapeerror" in text
    ):
        return "transient"
    # Yahoo's public client may swallow a failed module response and return a
    # partial/empty model. The HTTP boundary records this marker only when the
    # final response for a logical data path remains retryable after yfinance's
    # own crumb refresh. It is never provider credential evidence.
    if "__archive_yfinance_transport__" in text:
        if re.search(r"(?<!\d)429(?!\d)", text):
            return "rate"
        return "transient"
    # Yahoo periodically invalidates its session crumb and answers one request
    # with HTTP 401 before the client refreshes it. This is a session refresh
    # condition, not evidence that yfinance requires credentials or should be
    # disabled for the remainder of a multi-week archive run.
    if "invalid crumb" in text:
        return "transient"
    # A syntactically broken/empty HTTP response body is transport evidence,
    # not a stable fact about the requested dataset.  Treat every provider's
    # JSON decoder failure uniformly so a brief gateway/CDN response cannot
    # become a terminal archive hole.
    if "jsondecodeerror" in text:
        return "transient"
    # Container-shape assumptions inside provider adapters are schema
    # negotiation failures, regardless of market/provider. They are not proof
    # that the requested data is permanently absent. Endpoint-specific
    # boundary normalizers may recover known shapes; unknown shapes stay
    # resumable for a later adapter revision.
    if _is_provider_parser_shape_error(exc):
        return "transient"
    # BLS uses EmptyDataError for daily-quota exhaustion and invalid
    # registration keys. Explicit upstream evidence must override the generic
    # exception type, while a message that merely says the caller *may* be rate
    # limited remains authoritative empty.
    if "emptydataerror" in text:
        if _has_strong_rate_evidence(text):
            return "rate"
        if _has_invalid_credential_evidence(text):
            return "auth"
        return "empty"
    # SEC returns an XML/HTML 404 page for a missing companyfacts JSON
    # resource. OpenBB surfaces that as ContentTypeError rather than its usual
    # EmptyDataError, but the response is authoritative absence for that CIK.
    if (
        "contenttypeerror" in text
        and "404" in text
        and "data.sec.gov/api/xbrl/companyfacts/" in text
    ):
        return "empty"
    if any(marker in text for marker in RATE_ERROR_MARKERS) or re.search(
        r"(?<!\d)429(?!\d)", text
    ):
        return "rate"
    # BLS wraps an invalid registration key in a generic "No data found"
    # OpenBBError. Preserve the explicit upstream authentication signal so a
    # bad key cannot turn every catalog series into an authoritative empty.
    if _has_invalid_credential_evidence(text):
        return "auth"
    # HTTP 402 means the request requires a different subscription/payment
    # entitlement.  It may be route- or parameter-local, but it is never a
    # malformed task and therefore must not consume the permanent-error budget.
    if re.search(r"(?<!\d)402(?!\d)", text):
        return "auth"
    # Provider SDKs sometimes use an UnauthorizedError wrapper for every
    # non-2xx response.  HTTP 404 is not authentication evidence.  An explicit
    # empty JSON payload is authoritative query absence; any other 404 remains
    # task-local/permanent so it can never disable unrelated routes or markets.
    if re.search(r"(?<!\d)404(?!\d)", text):
        if re.search(r"(?:->|:)\s*(?:\[\]|\{\}|null)(?:\s|$)", text) or any(
            marker in text for marker in EMPTY_ERROR_MARKERS
        ):
            return "empty"
        return "permanent"
    # Provider messages frequently embed symbols and CIKs containing strings
    # such as "401".  An explicit no-result marker is stronger evidence than
    # an auth-looking numeric substring and must never disable a provider.
    if any(marker in text for marker in EMPTY_ERROR_MARKERS):
        return "empty"
    if any(marker in text for marker in AUTH_ERROR_MARKERS) or re.search(
        r"(?<!\d)(?:401|403)(?!\d)", text
    ):
        return "auth"
    status_code = getattr(exc, "code", None) or getattr(exc, "status", None)
    if (
        any(marker in text for marker in TRANSIENT_ERROR_MARKERS)
        or (isinstance(status_code, int) and 500 <= status_code <= 599)
        or re.search(
            r"(?:http(?:error| error)?|status(?: code)?)\s*[:=]?\s*5\d{2}(?!\d)",
            text,
        )
        # OpenBB wraps aiohttp ContentTypeError responses as strings such as
        # ``[Error] -> 500, message=...`` or
        # ``ContentTypeError -> 502, message=...`` without preserving the
        # response status on the outer exception object.
        or re.search(r"(?<!\d)5\d{2}(?!\d)\s*,\s*message\s*=", text)
    ):
        return "transient"
    return "permanent"


def _redact_sensitive_error(text: str, obb: Any) -> str:
    """Remove credential values and query-token material from manifest errors."""
    redacted = str(text)
    credentials = getattr(getattr(obb, "user", None), "credentials", None)
    values: Iterable[Any] = ()
    if credentials is not None:
        try:
            values = credentials.model_dump().values()
        except AttributeError:
            values = getattr(credentials, "__dict__", {}).values()
    for value in values:
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        if value not in {None, ""}:
            redacted = redacted.replace(str(value), "<redacted>")
    return re.sub(
        r"([?&](?:api_key|apikey|token|access_token|key)=)[^&\s]+",
        r"\1<redacted>",
        redacted,
    )


def _is_authoritative_unavailable_evidence(value: object) -> bool:
    """Require positive credential/subscription evidence for unavailability."""
    return is_authoritative_unavailable_evidence(value)


def _is_endpoint_specific_auth_failure(
    provider: str, endpoint: str, message: str
) -> bool:
    """Return True when an auth-like response restricts only this query/route."""
    text = message.lower()
    if provider == "fmp":
        # FMP also reports parameter/value restrictions with HTTP 402.  Those
        # are scope-local capability failures (for example an old ``from``
        # date or one unsupported index symbol), not evidence that every
        # symbol/date on the endpoint is unavailable.  Keep route disabling
        # for unambiguously endpoint-wide messages only.
        if _is_scope_specific_auth_failure(provider, message):
            return False
        return any(
            marker in text
            for marker in (
                "legacy endpoint",
                "restricted endpoint",
                "endpoint is not available under your current subscription",
            )
        )
    return (
        provider == "tiingo"
        and endpoint in {"news.company", "news.world"}
        and any(
            marker in text
            for marker in (
                "permission to access the news api",
                "news api permission",
                "news api subscription",
            )
        )
    )


def _is_provider_global_auth_failure(provider: str, message: str) -> bool:
    """Require provider-wide credential evidence before durable disablement.

    HTTP status and capability scope are separate facts.  A 402 is a paid-plan
    restriction, a 403 can be a route permission, and a 404 is a resource/query
    result.  Only an explicitly rejected/missing credential or a bare 401 may
    safely disable every endpoint for a provider.
    """
    text = message.lower()
    if _has_invalid_credential_evidence(text):
        return True
    if any(
        marker in text
        for marker in (
            "missing credential",
            "missing api key",
            "api key is required",
            "authentication required",
        )
    ):
        return True
    # A bare numeric search is unsafe here: provider URLs and series symbols
    # legitimately contain tokens such as ``GRCPREND401IXOBQ``.  Treat 401 as
    # provider-wide credential evidence only when the exception supplies HTTP
    # status context.  This keeps one upstream 500 for that FRED series from
    # disabling the entire provider across millions of independent tasks.
    has_http_401 = bool(
        re.search(
            r"(?:\bhttp(?:\s+status)?\s*[:=]?\s*401\b"
            r"|\bstatus(?:\s+code)?\s*[:=]\s*401\b"
            r"|->\s*401\s*->"
            r"|\b401\s+(?:client\s+error|unauthorized)\b)",
            text,
        )
    )
    return has_http_401 and "invalid crumb" not in text


def _bounded_parameter_maximum(
    message: str, parameter: str | None = None
) -> tuple[str, int] | None:
    """Extract an upstream numeric query bound without endpoint assumptions."""
    match = re.search(
        r"values?\s+for\s+['\"]([^'\"]+)['\"]\s+must\s+be\s+between\s+"
        r"-?\d+\s+and\s+(-?\d+)",
        str(message),
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    name = match.group(1).strip().lower()
    if parameter is not None and name != parameter.strip().lower():
        return None
    return name, int(match.group(2))


def _adaptable_limit_maximum(provider: str, endpoint: str, message: str) -> int | None:
    """Return the maximum query surface allowed by the FMP entitlement."""
    if provider != "fmp":
        return None
    constraint = _bounded_parameter_maximum(message, "limit")
    if constraint is None or constraint[1] <= 0:
        return None
    return constraint[1]


def _adaptable_omitted_parameters(
    provider: str, endpoint: str, message: str
) -> tuple[str, ...]:
    """Map one proven entitlement rejection to an alternate OpenBB query.

    The FMP historical-market-cap fetcher has two official shapes: dated
    requests use ``from``/``to`` while an undated request returns the latest
    5,000 observations. Basic rejects only the dated shape, so preserving the
    undated surface is strictly better than disabling the US domain.
    """
    text = str(message).lower()
    if (
        provider == "fmp"
        and endpoint == "equity.historical_market_cap"
        and "value set for 'from'" in text
    ):
        return ("end_date", "start_date")
    return ()


def _is_scope_specific_auth_failure(provider: str, message: str) -> bool:
    """Return whether an entitlement failure applies only to task parameters.

    Provider, endpoint and request scope are different capability domains.  A
    response rejecting one historical date or symbol must complete only that
    manifest scope; promoting it to route-wide state silently discards newer
    dates and other markets that the same credential can still access.
    """
    if provider != "fmp":
        return False
    text = message.lower()
    return "premium query parameter" in text or (
        "special endpoint" in text and "value set for" in text
    )


def _provider_capability_domain(provider: str, kwargs: Mapping[str, Any]) -> str | None:
    """Return the smallest stable market domain used for entitlement state.

    FMP subscriptions distinguish US and global coverage.  Persisting a
    symbol-specific 402 as an endpoint-wide denial loses accessible US data;
    persisting only one exact ticker causes millions of equivalent global
    scopes to consume quota.  Domain state is therefore endpoint + market,
    while temporal restrictions remain task-local until a latest-period probe
    proves the whole domain unavailable.
    """
    raw_symbol = str(kwargs.get("symbol") or "").strip().upper()
    if provider == "tiingo":
        symbols = [item.strip() for item in raw_symbol.split(",") if item.strip()]
        return (
            "tw"
            if symbols
            and all(
                symbol.endswith(".TW") or symbol.endswith(".TWO") for symbol in symbols
            )
            else None
        )
    if provider != "fmp":
        return None
    if not raw_symbol:
        return "global"
    symbols = [item.strip() for item in raw_symbol.split(",") if item.strip()]
    if symbols and all(
        symbol.endswith(".TW") or symbol.endswith(".TWO") for symbol in symbols
    ):
        return "tw"
    return "us"


def _sqlite_provider_capability_domain(
    provider: object, kwargs_json: object
) -> str | None:
    """SQLite adapter for the canonical provider capability-domain resolver."""
    try:
        kwargs = json.loads(str(kwargs_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(kwargs, Mapping):
        return None
    return _provider_capability_domain(str(provider), kwargs)


class OpenBBWorker:
    # The scheduler may submit its bounded provider prefetch queues directly to
    # the executor. ``semaphore_slot`` below then turns executor threads into
    # independent provider lanes, so a slow manifest commit or discovery pass
    # cannot starve unrelated APIs.
    preload_provider_queues = True

    def __init__(
        self,
        obb: Any,
        runtime: ProviderRuntime,
        *,
        max_retries: int,
        base_backoff: float,
        max_backoff: float,
        metadata_only: bool,
        show_progress: bool = False,
        cache_capable_endpoints: Iterable[str] = (),
        bls_labstat_cache_dir: Path | None = None,
        sec_companyfacts_cache_dir: Path | None = None,
        sec_statement_process_pool: ProcessPoolExecutor | None = None,
        sec_insider_cache_dir: Path | None = None,
        request_checkpoint_dir: Path | None = None,
    ) -> None:
        self.obb = obb
        self.runtime = runtime
        self.max_retries = max(1, int(max_retries))
        self.base_backoff = max(0.0, float(base_backoff))
        self.max_backoff = max(1.0, float(max_backoff))
        self.metadata_only = metadata_only
        self.show_progress = show_progress
        self.cache_capable_endpoints = frozenset(
            str(endpoint).lstrip(".") for endpoint in cache_capable_endpoints
        )
        self.bls_labstat_cache_dir = bls_labstat_cache_dir
        self.sec_companyfacts_cache_dir = sec_companyfacts_cache_dir
        self.sec_statement_process_pool = sec_statement_process_pool
        self.sec_insider_cache_dir = sec_insider_cache_dir
        self.request_checkpoint_dir = request_checkpoint_dir

    def can_run_during_provider_cooldown(self, provider: str, endpoint: str) -> bool:
        """Return whether a local official bulk path bypasses an API cooldown."""
        return bool(
            provider == "bls"
            and endpoint == "economy.survey.bls_series"
            and self.bls_labstat_cache_dir is not None
        )

    def local_cooldown_bypass_providers(self) -> set[str]:
        return {"bls"} if self.bls_labstat_cache_dir is not None else set()

    def _availability(
        self,
        provider: str,
        endpoint: str,
        capability_domain: str | None,
    ) -> tuple[bool, str | None]:
        available, reason = self.runtime.availability(
            provider, endpoint, capability_domain
        )
        if (
            not available
            and str(reason or "").startswith("cooldown until")
            and self.can_run_during_provider_cooldown(provider, endpoint)
        ):
            return True, "official local bulk path bypasses API cooldown"
        return available, reason

    def _request_checkpoint_dir(self, task: DownloadTask, provider: str) -> Path | None:
        if self.request_checkpoint_dir is None:
            return None
        return (
            self.request_checkpoint_dir
            / _safe_scope(provider)
            / _safe_scope(task.task_id)
        )

    def __call__(self, task: DownloadTask) -> TaskResult:
        errors: list[str] = []
        attempts = 0
        provider_outcomes = {
            provider: outcome
            for provider, outcome in task.provider_outcomes.items()
            if provider in task.providers
        }
        provider_evidence = {
            provider: evidence
            for provider, evidence in task.provider_evidence.items()
            if provider in task.providers
        }
        saw_retryable = False
        saw_task_transient = False
        repair_queue_providers: set[str] = set()
        last_provider: str | None = None
        for provider in provider_execution_order(task.providers):
            if provider in provider_outcomes:
                continue
            last_provider = provider
            capability_domain = _provider_capability_domain(provider, task.kwargs)
            available, reason = self._availability(
                provider, task.endpoint, capability_domain
            )
            if not available:
                errors.append(f"{provider}: skipped ({reason})")
                if reason and reason.startswith("cooldown until"):
                    saw_retryable = True
                else:
                    provider_outcomes[provider] = "unavailable"
                    if reason:
                        provider_evidence[provider] = str(reason)[:2000]
                continue
            for attempt in range(self.max_retries):
                attempts += 1
                provider_kwargs = dict(task.kwargs)
                if provider == "yfinance":
                    _begin_yfinance_http_evidence()
                request_observation = self.runtime.begin_request_observation(
                    task.endpoint
                )
                try:
                    limiter = self.runtime.limiter(provider)
                    available, reason = self._availability(
                        provider, task.endpoint, capability_domain
                    )
                    if not available:
                        attempts = max(0, attempts - 1)
                        errors.append(f"{provider}: skipped ({reason})")
                        if reason and reason.startswith("cooldown until"):
                            saw_retryable = True
                        else:
                            provider_outcomes[provider] = "unavailable"
                            if reason:
                                provider_evidence[provider] = str(reason)[:2000]
                        break
                    with self.runtime.semaphore_slot(provider):
                        # Pace actual provider-call starts after acquiring the
                        # concurrency slot. Providers that can fan out are
                        # governed at their real HTTP boundary; direct
                        # urllib/SDK workarounds explicitly claim the first and
                        # every continuation page. Only proven one-request
                        # routes use the outer command ticket.
                        local_bls_bulk = bool(
                            provider == "bls"
                            and task.endpoint == "economy.survey.bls_series"
                            and self.bls_labstat_cache_dir is not None
                        )
                        if provider == "sec":
                            _install_sec_http_limiter(self.runtime)
                        elif provider == "yfinance":
                            _install_yfinance_http_limiter(self.runtime)
                        elif provider in HTTP_BOUNDARY_PACED_PROVIDERS:
                            _install_provider_http_limiter(self.runtime)
                        elif not local_bls_bulk:
                            limiter.wait()
                        available, reason = self._availability(
                            provider, task.endpoint, capability_domain
                        )
                        if not available:
                            attempts = max(0, attempts - 1)
                            errors.append(f"{provider}: skipped ({reason})")
                            if reason and reason.startswith("cooldown until"):
                                saw_retryable = True
                            else:
                                provider_outcomes[provider] = "unavailable"
                                if reason:
                                    provider_evidence[provider] = str(reason)[:2000]
                            break
                        provider_kwargs, applied_parameter_maximums = (
                            self.runtime.apply_parameter_maximums(
                                provider, task.endpoint, task.kwargs
                            )
                        )
                        provider_kwargs, applied_omitted_parameters = (
                            self.runtime.apply_omitted_parameters(
                                provider, task.endpoint, provider_kwargs
                            )
                        )
                        effective_kwargs = dict(task.kwargs)
                        effective_kwargs.update(applied_parameter_maximums)
                        for parameter in applied_omitted_parameters:
                            effective_kwargs.pop(parameter, None)
                        effective_task = (
                            replace(
                                task,
                                kwargs=effective_kwargs,
                            )
                            if (
                                applied_parameter_maximums or applied_omitted_parameters
                            )
                            else task
                        )
                        sec_insider_bulk = bool(
                            provider_kwargs.pop("_archive_sec_insider_bulk", False)
                        )
                        sec_insider_tail = bool(
                            provider_kwargs.pop("_archive_sec_insider_tail", False)
                        )
                        sec_insider_range = bool(
                            provider_kwargs.pop("_archive_sec_insider_range", False)
                        )
                        if task.endpoint in self.cache_capable_endpoints:
                            # The archive manifest and atomic Parquet files are
                            # the durable retry/cache layer. OpenBB's optional
                            # aiohttp SQLite cache opens one database session
                            # per blocking portal and is not safe under this
                            # multi-provider executor (it can close another
                            # event loop's connection). It also makes a cache
                            # hit look like an HTTP limiter claim. Disable it
                            # for every provider route that exposes the flag so
                            # telemetry and upstream request limits stay exact.
                            provider_kwargs["use_cache"] = False
                        previous_page_signature = provider_kwargs.pop(
                            "_previous_page_signature", None
                        )
                        if task.endpoint == "fixedincome.government.yield_curve":
                            # Translate the normalized archive contract to the
                            # provider-specific OpenBB query model. The task
                            # records country/type explicitly for auditability;
                            # providers that implement the same US nominal
                            # default must not receive fields they silently
                            # ignore or reject.
                            if provider != "econdb":
                                provider_kwargs.pop("country", None)
                                provider_kwargs.pop("use_cache", None)
                            if provider != "fred":
                                provider_kwargs.pop("yield_curve_type", None)
                        if (
                            provider == "tiingo"
                            and task.endpoint in HISTORICAL_PRICE_ENDPOINTS
                            and provider_kwargs.get("start_date")
                            and str(provider_kwargs["start_date"]) < "2020-01-01"
                        ):
                            # The configured Tiingo entitlement rejects price
                            # history before 2020. Tiingo is fallback-only, so
                            # preserve whatever 2000-era coverage the primary
                            # providers returned and request its accessible
                            # tail instead of discarding the whole fallback.
                            provider_kwargs["start_date"] = "2020-01-01"
                        if task.endpoint == "news.world" and provider != "fmp":
                            provider_kwargs.pop("topic", None)
                        if (
                            task.endpoint == "equity.fundamental.filings"
                            and provider == "sec"
                        ):
                            # SEC's limit=0 triggers retrieval of every older
                            # submissions shard before the requested date
                            # filters are applied. Other providers retain the
                            # task's finite page limit and pagination contract.
                            provider_kwargs["limit"] = 0
                        if task.endpoint in STATEMENT_ENDPOINTS:
                            if (
                                provider == "sec"
                                and provider_kwargs.get("period") == "quarter"
                            ):
                                provider_kwargs["period"] = "quarterly"
                            if provider == "yfinance" and "limit" in provider_kwargs:
                                provider_kwargs["limit"] = min(
                                    5, int(provider_kwargs["limit"])
                                )
                            if provider != "fmp":
                                provider_kwargs.pop("ttm", None)
                        if (
                            task.endpoint
                            in {"regulators.sec.cik_map", "regulators.sec.symbol_map"}
                            and provider == "sec"
                        ):
                            result = _fetch_sec_identifier_map_workaround(
                                task.endpoint,
                                provider_kwargs,
                                page_limiter=limiter,
                            )
                        elif (
                            task.endpoint in SEC_COMPANYFACTS_STATEMENT_ENDPOINTS
                            and provider == "sec"
                            and self.sec_companyfacts_cache_dir is not None
                        ):
                            result = _fetch_sec_statement_workaround(
                                task.endpoint,
                                provider_kwargs,
                                page_limiter=limiter,
                                cache_dir=self.sec_companyfacts_cache_dir,
                                process_pool=self.sec_statement_process_pool,
                            )
                        elif (
                            task.endpoint == "equity.ownership.insider_trading"
                            and provider == "sec"
                            and sec_insider_bulk
                            and self.sec_insider_cache_dir is not None
                        ):
                            result = _fetch_sec_insider_bulk_workaround(
                                provider_kwargs,
                                cache_dir=self.sec_insider_cache_dir,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.ownership.insider_trading"
                            and provider == "sec"
                            and (sec_insider_tail or sec_insider_range)
                            and self.sec_insider_cache_dir is not None
                        ):
                            result = _fetch_sec_insider_range_workaround(
                                provider_kwargs,
                                cache_dir=self.sec_insider_cache_dir,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.fundamental.filings"
                            and provider == "sec"
                        ):
                            result = _fetch_sec_filings_workaround(
                                provider_kwargs,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "regulators.sec.filing_headers"
                            and provider == "sec"
                        ):
                            result = _fetch_sec_filing_headers_workaround(
                                provider_kwargs,
                                page_limiter=limiter,
                            )
                        elif (
                            task.endpoint == "economy.survey.inflation_expectations"
                            and provider == "federal_reserve"
                        ):
                            result = _fetch_inflation_expectations_workaround(
                                provider_kwargs
                            )
                        elif task.endpoint == "etf.info" and provider == "yfinance":
                            result = _fetch_yfinance_etf_info_workaround(
                                provider_kwargs
                            )
                        elif (
                            task.endpoint == "economy.country_profile"
                            and provider == "econdb"
                        ):
                            result = _fetch_econdb_country_profile_workaround(
                                provider_kwargs
                            )
                        elif (
                            task.endpoint == "fixedincome.government.yield_curve"
                            and provider == "econdb"
                            and task.scope_key.endswith("/archive")
                        ):
                            result = _fetch_econdb_yield_curve_archive_workaround(
                                provider_kwargs,
                                self.obb,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "economy.indicators"
                            and provider == "econdb"
                        ):
                            result = _fetch_econdb_indicators_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "economy.export_destinations"
                            and provider == "econdb"
                        ):
                            result = _fetch_econdb_export_destinations_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=self.runtime.limiter("un_comtrade"),
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.discovery.filings"
                            and provider == "fmp"
                        ):
                            result = _fetch_fmp_discovery_filings_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint
                            in {
                                "equity.fundamental.metrics",
                                "equity.fundamental.ratios",
                            }
                            and provider == "fmp"
                        ):
                            result = _fetch_fmp_fundamental_ratio_workaround(
                                task.endpoint,
                                provider_kwargs,
                                self.obb,
                            )
                        elif (
                            task.endpoint == "equity.fundamental.historical_eps"
                            and provider == "fmp"
                            and provider_kwargs.get("limit") is not None
                            and int(provider_kwargs["limit"]) <= 5
                        ):
                            result = _fetch_fmp_historical_eps_workaround(
                                provider_kwargs,
                                self.obb,
                            )
                        elif (
                            task.endpoint == "equity.compare.peers"
                            and provider == "fmp"
                            and getattr(
                                getattr(self.obb, "user", None),
                                "credentials",
                                None,
                            )
                            is not None
                        ):
                            result = _fetch_fmp_equity_peers_workaround(
                                provider_kwargs,
                                self.obb,
                            )
                        elif (
                            task.endpoint == "equity.estimates.price_target"
                            and provider == "fmp"
                        ):
                            result = _fetch_fmp_price_targets_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "news.world"
                            and provider == "fmp"
                            and provider_kwargs.get("topic") == "fmp_articles"
                        ):
                            result = _fetch_fmp_world_articles_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint in {"news.company", "news.world"}
                            and provider == "tiingo"
                            and getattr(
                                getattr(self.obb, "user", None),
                                "credentials",
                                None,
                            )
                            is not None
                        ):
                            result = _fetch_tiingo_news_workaround(
                                task.endpoint,
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.ownership.government_trades"
                            and provider == "fmp"
                        ):
                            result = _fetch_fmp_government_trades_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.ownership.insider_trading"
                            and provider == "fmp"
                            and not provider_kwargs.get("statistics")
                        ):
                            result = _fetch_fmp_insider_trading_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.compare.company_facts"
                            and provider == "sec"
                            and provider_kwargs.get("fact") == SEC_ALL_COMPANY_FACTS
                        ):
                            result = _fetch_sec_company_facts_bulk_workaround(
                                provider_kwargs,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                                cache_dir=self.sec_companyfacts_cache_dir,
                            )
                        elif (
                            task.endpoint == "economy.fred_series"
                            and provider == "fred"
                        ):
                            result = _fetch_fred_series_workaround(
                                provider_kwargs, self.obb
                            )
                        elif (
                            task.endpoint == "economy.retail_prices"
                            and provider == "fred"
                            and provider_kwargs.get("item") == "ground_beef"
                        ):
                            result = _fetch_fred_retail_prices_workaround(
                                provider_kwargs, self.obb
                            )
                        elif (
                            task.endpoint == "fixedincome.bond_indices"
                            and provider == "fred"
                        ):
                            result = _fetch_fred_bond_indices_workaround(
                                provider_kwargs, self.obb
                            )
                        elif (
                            task.endpoint == "economy.survey.bls_series"
                            and provider == "bls"
                        ):
                            if self.bls_labstat_cache_dir is not None:
                                try:
                                    result = ColumnarTaskPayload(
                                        _fetch_bls_series_labstat_table(
                                            provider_kwargs,
                                            cache_dir=self.bls_labstat_cache_dir,
                                            show_progress=self.show_progress,
                                        )
                                    )
                                except BlsLabstatUnsupportedError:
                                    api_available, api_reason = (
                                        self.runtime.availability(
                                            provider,
                                            task.endpoint,
                                            capability_domain,
                                        )
                                    )
                                    if not api_available:
                                        raise ProviderDeferredError(
                                            "__archive_provider_deferred__: BLS "
                                            + str(
                                                api_reason or "BLS API is unavailable"
                                            )
                                        )
                                    # The bulk route made no API claim. Claim
                                    # the first API request here; the resilient
                                    # helper accounts for every continuation.
                                    limiter.wait()
                                    result = _fetch_bls_series_resilient(
                                        provider_kwargs,
                                        self.obb,
                                        page_limiter=limiter,
                                        show_progress=self.show_progress,
                                    )
                            else:
                                result = _fetch_bls_series_resilient(
                                    provider_kwargs,
                                    self.obb,
                                    page_limiter=limiter,
                                    show_progress=self.show_progress,
                                )
                        elif (
                            task.endpoint == "commodity.petroleum_status_report"
                            and provider == "eia"
                            and str(provider_kwargs.get("table"))
                            in _eia_petroleum_schema_mismatch_tables()
                        ):
                            result = _fetch_eia_petroleum_status_workaround(
                                provider_kwargs
                            )
                        elif (
                            task.endpoint == "economy.central_bank_holdings"
                            and provider == "federal_reserve"
                        ):
                            result = (
                                _fetch_federal_reserve_central_bank_holdings_workaround(
                                    provider_kwargs
                                )
                            )
                        elif task.endpoint == "economy.calendar" and provider == "fred":
                            result = _fetch_fred_calendar_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "economy.fred_search"
                            and provider == "fred"
                            and provider_kwargs.get("release_id") is not None
                        ):
                            result = _fetch_fred_release_search_workaround(
                                provider_kwargs,
                                self.obb,
                            )
                        elif (
                            task.endpoint == "fixedincome.corporate.hqm"
                            and provider == "fred"
                        ):
                            result = _fetch_fred_hqm_workaround(
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                            )
                        elif task.endpoint == "cftc.cot_search" and provider == "cftc":
                            result = _fetch_cftc_cot_catalog_workaround(
                                {
                                    **provider_kwargs,
                                    "start_date": task.kwargs.get(
                                        "start_date", DEFAULT_START_DATE
                                    ),
                                    "end_date": task.kwargs.get(
                                        "end_date", date.today().isoformat()
                                    ),
                                },
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "equity.shorts.fails_to_deliver"
                            and provider == "sec"
                            and provider_kwargs.get("report_key")
                        ):
                            result = _fetch_sec_ftd_report_workaround(
                                provider_kwargs,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                            )
                        elif (
                            task.endpoint == "etf.nport_disclosure"
                            and provider == "sec"
                        ):
                            result = _fetch_sec_nport_workaround(
                                provider_kwargs, page_limiter=limiter
                            )
                        elif (
                            task.endpoint
                            in {"uscongress.bill_info", "uscongress.amendment_info"}
                            and provider == "congress_gov"
                        ):
                            result = _fetch_congress_info_workaround(
                                provider_kwargs,
                                self.obb,
                                kind=(
                                    "bill"
                                    if task.endpoint == "uscongress.bill_info"
                                    else "amendment"
                                ),
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                                checkpoint_dir=self._request_checkpoint_dir(
                                    task, provider
                                ),
                            )
                        elif (
                            task.endpoint
                            in {"uscongress.bills", "uscongress.amendments"}
                            and provider == "congress_gov"
                        ):
                            result = _fetch_congress_collection_workaround(
                                task.endpoint,
                                provider_kwargs,
                                self.obb,
                                page_limiter=limiter,
                                show_progress=self.show_progress,
                                checkpoint_dir=self._request_checkpoint_dir(
                                    task, provider
                                ),
                            )
                        else:
                            func = _resolve_callable(self.obb, task.endpoint)
                            result = func(provider=provider, **provider_kwargs)
                    if isinstance(result, ColumnarTaskPayload):
                        rows = _atomic_write_arrow_table(
                            result.table,
                            effective_task,
                            provider,
                            show_progress=self.show_progress,
                        )
                        _clear_request_checkpoints(
                            self._request_checkpoint_dir(task, provider)
                        )
                        return TaskResult(
                            effective_task,
                            "success",
                            provider,
                            rows,
                            task.output_path,
                            attempts,
                            records=[],
                            provider_outcomes=provider_outcomes,
                            provider_evidence=provider_evidence,
                        )
                    records = (
                        _normalize_bls_search_result(
                            result,
                            metadata_only=self.metadata_only,
                            show_progress=self.show_progress,
                        )
                        if task.endpoint == "economy.survey.bls_search"
                        else normalize_records(
                            result,
                            metadata_only=self.metadata_only,
                            show_progress=self.show_progress,
                            progress_desc=f"normalize:{task.endpoint}",
                        )
                    )
                    if task.endpoint in {"news.company", "news.world"}:
                        records = _filter_company_news_to_task_range(
                            records, task, show_progress=self.show_progress
                        )
                    else:
                        records = _filter_records_to_task_range(
                            records, task, show_progress=self.show_progress
                        )
                    if provider == "yfinance":
                        transport_failure = _consume_yfinance_transport_failure()
                        if transport_failure is not None:
                            raise RuntimeError(transport_failure)
                    if records and previous_page_signature is not None:
                        current_page_signature = _page_content_signature(records)
                        if current_page_signature == previous_page_signature:
                            raise RuntimeError(
                                "Provider pagination cycle: repeated normalized page "
                                f"for {task.endpoint} scope={task.scope_key}"
                            )
                    if not records:
                        provider_outcomes[provider] = "empty"
                        errors.append(f"{provider}: empty")
                        break
                    rows = _atomic_write_parquet(
                        records,
                        effective_task,
                        provider,
                        show_progress=self.show_progress,
                    )
                    _clear_request_checkpoints(
                        self._request_checkpoint_dir(task, provider)
                    )
                    return TaskResult(
                        effective_task,
                        "success",
                        provider,
                        rows,
                        task.output_path,
                        attempts,
                        records=records,
                        provider_outcomes=provider_outcomes,
                        provider_evidence=provider_evidence,
                    )
                except (
                    Exception
                ) as exc:  # Provider exceptions are intentionally heterogeneous.
                    if provider == "yfinance":
                        # Do not let one failed attempt's response window leak
                        # into a later retry or a different market task.
                        _consume_yfinance_transport_failure()
                    if (
                        task.endpoint == "economy.fred_release_table"
                        and provider == "fred"
                        and "'list' object has no attribute 'values'" in str(exc)
                    ):
                        # The FRED API represents a release with no table as
                        # elements=[], while the current OpenBB parser assumes
                        # a dictionary and raises.  This is an authoritative
                        # empty response, not a retryable download failure.
                        provider_outcomes[provider] = "empty"
                        errors.append(f"{provider}: empty release table")
                        break
                    kind = classify_error(exc)
                    safe_error = _redact_sensitive_error(str(exc), self.obb)
                    message = f"{provider}: {type(exc).__name__}: {safe_error[:1500]}"
                    errors.append(message)
                    adaptable_limit = _adaptable_limit_maximum(
                        provider, task.endpoint, message
                    )
                    adaptable_omissions = _adaptable_omitted_parameters(
                        provider, task.endpoint, message
                    )
                    requested_limit = provider_kwargs.get("limit")
                    if adaptable_limit is not None and (
                        requested_limit is None
                        or int(requested_limit) > adaptable_limit
                    ):
                        # This is query-surface negotiation, not a denial of
                        # the route. Learn it once for the provider/endpoint.
                        # Pageable endpoints continue through manifest pages;
                        # fixed-cap endpoints retain every row the configured
                        # credential can expose instead of losing them all.
                        self.runtime.learn_parameter_maximum(
                            provider,
                            task.endpoint,
                            "limit",
                            adaptable_limit,
                        )
                        saw_retryable = True
                        attempts = max(0, attempts - 1)
                        if attempt + 1 < self.max_retries:
                            continue
                        break
                    if adaptable_omissions and any(
                        parameter in provider_kwargs
                        for parameter in adaptable_omissions
                    ):
                        self.runtime.learn_omitted_parameters(
                            provider,
                            task.endpoint,
                            adaptable_omissions,
                        )
                        saw_retryable = True
                        attempts = max(0, attempts - 1)
                        if attempt + 1 < self.max_retries:
                            continue
                        break
                    if kind == "empty":
                        _clear_request_checkpoints(
                            self._request_checkpoint_dir(task, provider)
                        )
                        provider_outcomes[provider] = "empty"
                        break
                    if kind == "auth":
                        # SEC endpoints used by this archive are anonymous.
                        # Auth-like responses are normally fair-access/WAF
                        # responses and must not disable every later SEC task.
                        if provider == "sec":
                            saw_retryable = True
                            attempts = max(0, attempts - 1)
                            self.runtime.block_quota(provider, message)
                            break
                        # A parameter-specific restriction is local to this
                        # exact manifest scope.  Do not promote it to an
                        # endpoint capability: the same route may allow newer
                        # dates or another symbol/market.
                        if _is_scope_specific_auth_failure(provider, message):
                            provider_outcomes[provider] = "unavailable"
                            provider_evidence[provider] = message[:2000]
                            break
                        # FMP uses 402/403 for endpoint-specific subscription
                        # restrictions. Disabling FMP globally here would
                        # incorrectly skip unrelated free routes.
                        if _is_endpoint_specific_auth_failure(
                            provider, task.endpoint, message
                        ):
                            provider_outcomes[provider] = "unavailable"
                            provider_evidence[provider] = message[:2000]
                            self.runtime.disable_route(provider, task.endpoint, message)
                            break
                        # Ambiguous auth wrappers (notably FMP 402 and provider
                        # 403 responses) are task-local unless the upstream
                        # explicitly rejects the credential.  This prevents a
                        # single market/symbol from shutting down every route.
                        if _is_provider_global_auth_failure(provider, message):
                            self.runtime.disable(provider, message)
                        provider_outcomes[provider] = "unavailable"
                        provider_evidence[provider] = message[:2000]
                        break
                    if kind == "deferred":
                        # Another request already established the provider's
                        # transient/quota cooldown. Return this task to the
                        # queue without consuming attempts or extending that
                        # deadline a second time.
                        saw_retryable = True
                        attempts = max(0, attempts - 1)
                        break
                    if kind == "rate":
                        saw_retryable = True
                        # Quota and 429 responses must not consume the finite
                        # per-task error budget. The provider-wide cooldown
                        # supplies backpressure, and the task must remain
                        # eligible when the quota window resets.
                        attempts = max(0, attempts - 1)
                        self.runtime.block_quota(provider, message)
                        break
                    if kind == "permanent":
                        _clear_request_checkpoints(
                            self._request_checkpoint_dir(task, provider)
                        )
                        provider_outcomes[provider] = "permanent"
                        provider_evidence[provider] = message[:2000]
                        break
                    saw_retryable = True
                    saw_task_transient = True
                    if (
                        provider == "congress_gov"
                        and task.endpoint in CONGRESS_REPAIR_QUEUE_ENDPOINTS
                        and _is_http_status_500(exc)
                    ):
                        # A 500 tied to one concrete Congress.gov resource is
                        # neither authoritative absence nor a useful main-pass
                        # retry. Park it as an explicit data gap. A later
                        # --retry-repair-queue pass gets exactly one fresh
                        # attempt and parks it again if upstream is unchanged.
                        repair_queue_providers.add(provider)
                        break
                    # A server response for this concrete URL is task-local
                    # evidence.  Repeating it immediately only consumes the
                    # upstream request budget; persist a task deadline below
                    # and let unrelated URLs continue through the provider.
                    if _is_http_server_error(exc):
                        break
                    if attempt + 1 < self.max_retries:
                        delay = min(self.max_backoff, self.base_backoff * (2**attempt))
                        delay *= random.uniform(0.8, 1.2)
                        self.runtime.block(provider, delay, message)
                        time.sleep(delay)
                finally:
                    self.runtime.finish_request_observation(request_observation)
        # Retry flags describe attempts, while provider outcomes describe the
        # final state. A transient error followed by an authoritative empty or
        # permanent answer from the same provider must not remain pending.
        # Conversely, any provider without an outcome still needs another
        # chance after its cooldown, even if another fallback answered empty.
        outcome_values = set(provider_outcomes.values())
        unresolved_provider = any(
            provider not in provider_outcomes for provider in task.providers
        )
        unresolved_providers = {
            provider for provider in task.providers if provider not in provider_outcomes
        }
        terminal_empty = "empty" in outcome_values
        terminal_unavailable = "unavailable" in outcome_values
        terminal_permanent = "permanent" in outcome_values
        status = (
            REPAIR_QUEUE_STATUS
            if unresolved_providers
            and unresolved_providers.issubset(repair_queue_providers)
            else "pending"
            if saw_retryable and unresolved_provider
            # An empty primary plus an unavailable fallback is not proof of
            # absence. Record capability-unavailable instead of publishing a
            # false authoritative empty archive.
            else "unavailable"
            if terminal_unavailable and terminal_empty
            else "empty"
            if terminal_empty and not terminal_permanent
            else "failed"
            if terminal_permanent
            else "unavailable"
            if terminal_unavailable
            else "failed"
        )
        transient_failures = (
            task.transient_failures + 1
            if status == "pending" and saw_task_transient
            else task.transient_failures
            if status == "pending"
            else 0
        )
        retry_not_before = (
            _task_retry_deadline(task.task_id, transient_failures)
            if status == "pending" and saw_task_transient
            else None
        )
        return TaskResult(
            task=task,
            status=status,
            provider=last_provider,
            rows=0,
            output_path=None,
            attempts=(attempts if status == "pending" else max(1, attempts)),
            error=" | ".join(errors)[-8000:] or "no available provider",
            provider_outcomes=provider_outcomes,
            provider_evidence=provider_evidence,
            retry_not_before=retry_not_before,
            transient_failures=transient_failures,
        )


def _first_present(record: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        value = record.get(name)
        if value not in {None, ""}:
            return value
    return None


def _mapping_result_value(record: Mapping[str, Any], name: str) -> Any:
    """Read a direct model field or the legacy serialized pair representation."""
    direct = record.get(name)
    if direct not in {None, ""}:
        return direct
    raw = record.get("value")
    if raw in {None, ""}:
        return None
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if isinstance(decoded, Mapping):
        value = decoded.get(name)
        return value if value not in {None, ""} else None
    if (
        isinstance(decoded, (list, tuple))
        and len(decoded) == 2
        and str(decoded[0]) == name
    ):
        return decoded[1]
    return None


def _followup_record_progress(
    context: PlannerContext,
    result: TaskResult,
    records: Sequence[Mapping[str, Any]],
) -> Iterator[Mapping[str, Any]]:
    """Expose progress for every potentially large catalog expansion loop."""
    progress = tqdm(
        records,
        total=len(records),
        desc=f"discover:{result.task.endpoint}"[:64],
        unit="record",
        position=2,
        leave=False,
        miniters=max(1, min(1000, len(records) // 100 or 1)),
        disable=not context.show_progress,
    )
    try:
        yield from progress
    finally:
        progress.close()


def discover_followup_tasks(
    context: PlannerContext,
    result: TaskResult,
) -> list[DownloadTask]:
    if result.status != "success" or not result.records:
        return []
    endpoint = result.task.endpoint
    records = result.records
    tasks: list[DownloadTask] = []

    if endpoint == "cftc.cot_search":
        providers = select_providers(
            "cftc.cot", context.commands.get(".cftc.cot", []), context
        )
        report_type = str(result.task.kwargs.get("report_type") or "legacy")
        futures_only = bool(result.task.kwargs.get("futures_only", False))
        mode = "futures" if futures_only else "combined"
        for record in _followup_record_progress(context, result, records):
            code = _first_present(record, ("code", "cftc_contract_market_code"))
            if code:
                tasks.append(
                    make_task(
                        context,
                        "cftc.cot",
                        f"report={report_type}/mode={mode}/code={code}",
                        {
                            "code": str(code),
                            "start_date": context.start_date,
                            "end_date": context.end_date,
                            "report_type": report_type,
                            "futures_only": futures_only,
                            "measure": "all",
                            "limit": 0,
                        },
                        providers,
                    )
                )

    elif (
        endpoint == "economy.fred_search" and result.task.scope_key == "release_catalog"
    ):
        providers = ("fred",)
        for record in _followup_record_progress(context, result, records):
            release_id = _first_present(record, ("release_id", "id"))
            if release_id is None:
                continue
            release_id = int(release_id)
            tasks.append(
                make_task(
                    context,
                    endpoint,
                    f"release={release_id}",
                    {
                        "query": "",
                        "release_id": release_id,
                        "search_type": "release",
                        "limit": 1000,
                    },
                    providers,
                )
            )
            release_providers = select_providers(
                "economy.fred_release_table",
                context.commands.get(".economy.fred_release_table", []),
                context,
            )
            if release_providers:
                tasks.append(
                    make_task(
                        context,
                        "economy.fred_release_table",
                        f"release={release_id}",
                        {"release_id": str(release_id)},
                        release_providers,
                    )
                )

    elif endpoint == "economy.fred_search" and result.task.scope_key.startswith(
        "release="
    ):
        providers = select_providers(
            "economy.fred_series",
            context.commands.get(".economy.fred_series", []),
            context,
        )
        for record in _followup_record_progress(context, result, records):
            series_id = _first_present(record, ("series_id", "symbol", "id"))
            if not series_id:
                continue
            series_id = str(series_id)
            tasks.append(
                make_task(
                    context,
                    "economy.fred_series",
                    series_id,
                    {
                        "symbol": series_id,
                        "start_date": context.start_date,
                        "end_date": context.end_date,
                        "limit": 100000,
                    },
                    providers,
                )
            )
        continuation = _fred_release_continuation_task(
            context, result.task, result.rows
        )
        if continuation is not None:
            tasks.append(continuation)

    elif endpoint == "economy.survey.bls_search":
        providers = select_providers(
            "economy.survey.bls_series",
            context.commands.get(".economy.survey.bls_series", []),
            context,
        )
        series_ids: list[str] = []
        seen_series: set[str] = set()
        record_progress = tqdm(
            records,
            total=len(records),
            desc=f"bls:{result.task.scope_key} scan"[:64],
            unit="series",
            position=2,
            leave=False,
            disable=not context.show_progress,
        )
        for record in record_progress:
            if record.get("_bls_record_type") == "code_map":
                continue
            series_id = _first_present(record, ("series_id", "symbol", "code"))
            normalized_id = str(series_id).strip().upper() if series_id else ""
            if normalized_id and normalized_id not in seen_series:
                seen_series.add(normalized_id)
                series_ids.append(normalized_id)
        record_progress.close()

        batch_offsets = range(0, len(series_ids), BLS_SERIES_BATCH_SIZE)
        batch_progress = tqdm(
            batch_offsets,
            total=math.ceil(len(series_ids) / BLS_SERIES_BATCH_SIZE),
            desc=f"bls:{result.task.scope_key} batches"[:64],
            unit="batch",
            position=2,
            leave=False,
            disable=not context.show_progress,
        )
        for batch_index, offset in enumerate(batch_progress):
            batch = series_ids[offset : offset + BLS_SERIES_BATCH_SIZE]
            if providers:
                tasks.append(
                    make_task(
                        context,
                        "economy.survey.bls_series",
                        f"{result.task.scope_key}/batch={batch_index:05d}/n={len(batch)}",
                        {
                            "symbol": ",".join(batch),
                            "start_date": context.start_date,
                            "end_date": context.end_date,
                            "calculations": True,
                            "annual_average": False,
                            "aspects": True,
                        },
                        providers,
                    )
                )
        batch_progress.close()

    elif endpoint == "economy.available_indicators":
        providers = (str(result.provider),) if result.provider else ()
        for record in _followup_record_progress(context, result, records):
            symbol = _first_present(
                record, ("symbol", "indicator", "code", "series_id")
            )
            if symbol and providers:
                raw_frequency = str(
                    _first_present(record, ("frequency",)) or "month"
                ).lower()
                frequency = {
                    "a": "annual",
                    "y": "annual",
                    "q": "quarter",
                    "m": "month",
                }.get(
                    raw_frequency,
                    "month",
                )
                if result.provider == "econdb":
                    # Supplying the exact catalog ticker with a trailing '~'
                    # bypasses OpenBB's incomplete COUNTRY_MAP while retaining
                    # the catalog's valid indicator-country combination.
                    query_symbol = f"{str(symbol).rstrip('~')}~"
                    country = None
                else:
                    query_symbol = str(
                        _first_present(record, ("symbol_root",)) or symbol
                    )
                    country = _first_present(record, ("country", "iso"))
                tasks.append(
                    make_task(
                        context,
                        "economy.indicators",
                        str(symbol),
                        {
                            "symbol": query_symbol,
                            "country": str(country) if country else None,
                            "frequency": frequency,
                            "start_date": context.start_date,
                            "end_date": context.end_date,
                        },
                        providers,
                    )
                )

    elif endpoint == "regulators.sec.cik_map":
        providers = select_providers(
            "regulators.sec.symbol_map",
            context.commands.get(".regulators.sec.symbol_map", []),
            context,
        )
        if providers:
            for record in _followup_record_progress(context, result, records):
                cik = _mapping_result_value(record, "cik")
                if cik in {None, ""}:
                    continue
                cik = str(cik).strip()
                tasks.append(
                    make_task(
                        context,
                        "regulators.sec.symbol_map",
                        cik,
                        {"query": cik, "use_cache": True},
                        providers,
                    )
                )

    elif endpoint == "index.available":
        history_providers = select_providers(
            "index.price.historical",
            context.commands.get(".index.price.historical", []),
            context,
        )
        for record in _followup_record_progress(context, result, records):
            symbol = _first_present(record, ("symbol", "code"))
            if not symbol:
                continue
            symbol = str(symbol)
            if history_providers:
                tasks.append(
                    make_task(
                        context,
                        "index.price.historical",
                        symbol,
                        {
                            "symbol": symbol,
                            "start_date": context.start_date,
                            "end_date": context.end_date,
                            "interval": "1d",
                        },
                        history_providers,
                    )
                )

    elif endpoint == "currency.search":
        providers = select_providers(
            "currency.price.historical",
            context.commands.get(".currency.price.historical", []),
            context,
        )
        for record in _followup_record_progress(context, result, records):
            symbol = _first_present(record, ("symbol", "code"))
            if symbol:
                tasks.append(
                    make_task(
                        context,
                        "currency.price.historical",
                        str(symbol),
                        {
                            "symbol": str(symbol),
                            "start_date": context.start_date,
                            "end_date": context.end_date,
                            "interval": "1d",
                        },
                        providers,
                    )
                )

    elif endpoint in {"uscongress.bills", "uscongress.amendments"}:
        child_endpoint = (
            "uscongress.bill_info"
            if endpoint.endswith("bills")
            else "uscongress.amendment_info"
        )
        url_names = (
            ("url", "bill_url")
            if endpoint.endswith("bills")
            else ("url", "amendment_url")
        )
        child_providers = select_providers(
            child_endpoint, context.commands.get(f".{child_endpoint}", []), context
        )
        argument = "bill_url" if endpoint.endswith("bills") else "amendment_url"
        for record in _followup_record_progress(context, result, records):
            url = _first_present(record, url_names)
            if url and child_providers:
                tasks.append(
                    make_task(
                        context,
                        child_endpoint,
                        str(url),
                        {argument: str(url)},
                        child_providers,
                    )
                )

    elif endpoint == "equity.fundamental.filings":
        child_providers = select_providers(
            "regulators.sec.filing_headers",
            context.commands.get(".regulators.sec.filing_headers", []),
            context,
        )
        for record in _followup_record_progress(context, result, records):
            url = _first_present(record, ("filing_url", "report_url", "url"))
            if url and "sec.gov" in str(url).lower() and child_providers:
                tasks.append(
                    make_task(
                        context,
                        "regulators.sec.filing_headers",
                        str(url),
                        {"url": str(url), "use_cache": True},
                        child_providers,
                    )
                )

    # Provider pagination is added only when the page was full and the provider
    # actually supports page-based retrieval.  A fallback provider cannot
    # accidentally repeat page zero.
    if endpoint in FMP_MANIFEST_PAGINATED_ENDPOINTS and result.provider == "fmp":
        limit = int(result.task.kwargs.get("limit") or 0)
        page = int(result.task.kwargs.get("page") or 0)
        if limit > 0 and result.rows >= limit:
            kwargs = dict(
                result.task.kwargs,
                page=page + 1,
                _previous_page_signature=_page_content_signature(result.records),
            )
            scope = re.sub(r"page=\d+", f"page={page + 1}", result.task.scope_key)
            if scope == result.task.scope_key:
                scope = f"{scope}/page={page + 1}"
            tasks.append(make_task(context, endpoint, scope, kwargs, ("fmp",)))

    filtered_tasks: list[DownloadTask] = []
    filter_progress = tqdm(
        tasks,
        total=len(tasks),
        desc=f"discover:{endpoint} filter"[:64],
        unit="task",
        position=2,
        leave=False,
        disable=not context.show_progress,
    )
    try:
        for task in filter_progress:
            if (
                (context.categories is None or task.category in context.categories)
                and _endpoint_matches(task.endpoint, context.endpoint_filters)
                and task.endpoint not in SNAPSHOT_ENDPOINTS
                and task.endpoint not in DOCUMENT_BODY_ENDPOINTS
            ):
                filtered_tasks.append(task)
    finally:
        filter_progress.close()
    return filtered_tasks


def _write_rows_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = _table_from_pylist_union_schema(rows)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            compression_level=6,
            write_statistics=True,
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_catalogs(
    context: PlannerContext,
    coverage: Sequence[CoverageDecision],
    credential_names: set[str],
    *,
    write_universes: bool = True,
) -> None:
    catalog_dir = context.output_dir / "catalog"
    _write_rows_parquet(
        [asdict(item) for item in coverage], catalog_dir / "coverage.parquet"
    )
    if write_universes:
        _write_rows_parquet(
            [asdict(item) for item in context.assets],
            catalog_dir / "equity_universe.parquet",
        )
        _write_rows_parquet(
            [{"symbol": item} for item in context.currencies],
            catalog_dir / "currency_universe.parquet",
        )
    _write_rows_parquet(
        [
            {"credential_field": name, "configured": True}
            for name in sorted(credential_names)
        ],
        catalog_dir / "configured_credentials.parquet",
    )
    _write_rows_parquet(
        [
            {
                "provider": provider,
                "requests": policy.requests,
                "seconds": policy.seconds,
                "requests_per_second": policy.requests_per_second,
                "basis": policy.basis,
                "source_url": policy.source_url,
                "quota_note": policy.quota_note,
            }
            for provider, policy in sorted(PROVIDER_RATE_POLICIES.items())
        ],
        catalog_dir / "provider_rate_limits.parquet",
    )


def _print_plan_summary(
    context: PlannerContext,
    task_count: int,
    coverage: Sequence[CoverageDecision],
) -> None:
    decisions: dict[str, int] = {}
    for item in coverage:
        decisions[item.decision] = decisions.get(item.decision, 0) + 1
    print(
        "[openbb-plan] "
        f"start={context.start_date} end={context.end_date} assets={len(context.assets)} "
        f"etfs={len(context.etfs)} currencies={len(context.currencies)} "
        f"initial_tasks={task_count} decisions={decisions}",
        flush=True,
    )


def _load_openbb(
    env_file: str | Path = Path(".env"),
) -> tuple[Any, Mapping[str, Mapping[str, Any]], Mapping[str, Sequence[str]]]:
    from openbb import obb

    applied_fields = apply_openbb_environment_credentials(obb, env_file=env_file)
    if applied_fields:
        print(
            "[openbb-credentials] source=environment configured_fields="
            + ",".join(sorted(applied_fields)),
            flush=True,
        )

    # CFTC's Public Reporting API works anonymously.  Clearing a stale/invalid
    # app token only affects this process and prevents it from breaking valid
    # anonymous calls.
    try:
        obb.user.credentials.cftc_app_token = None
    except Exception:
        pass
    return obb, obb.coverage.command_schemas(), obb.coverage.commands


def _plan_token(
    *,
    start_date: str,
    end_date: str,
    markets: set[str],
    categories: set[str] | None,
    endpoint_filters: Sequence[str],
    allowed_providers: set[str] | None,
    disabled_providers: set[str],
    limit_symbols: int | None,
    universe_fingerprint: str = "",
    command_fingerprint: str = "",
) -> str:
    payload = {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "start_date": start_date,
        "end_date": end_date,
        "markets": sorted(markets),
        "categories": sorted(categories or []),
        "endpoint_filters": sorted(endpoint_filters),
        "allowed_providers": sorted(allowed_providers or []),
        "disabled_providers": sorted(disabled_providers),
        "limit_symbols": limit_symbols,
        "universe_fingerprint": universe_fingerprint,
        "command_fingerprint": command_fingerprint,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()[:24]


def _plan_scope_fingerprint(
    *,
    start_date: str,
    end_date: str,
    markets: set[str],
    categories: set[str] | None,
    endpoint_filters: Sequence[str],
    allowed_providers: set[str] | None,
    disabled_providers: set[str],
    limit_symbols: int | None,
) -> str:
    """Identify user-selected archive scope independently of planner code.

    Universe and command/schema fingerprints intentionally change the full
    plan token when provider coverage evolves.  They do not, by themselves,
    make existing catalog-derived follow-up shards invalid.  This stable
    fingerprint lets a replacement plan adopt those durable follow-ups while
    still refusing migration across a user-requested date/market/filter change.
    """
    payload = {
        "start_date": start_date,
        "end_date": end_date,
        "markets": sorted(markets),
        "categories": sorted(categories or []),
        "endpoint_filters": sorted(endpoint_filters),
        "allowed_providers": sorted(allowed_providers or []),
        "disabled_providers": sorted(disabled_providers),
        "limit_symbols": limit_symbols,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()[:24]


def _universe_fingerprint(
    assets: Sequence[AssetRecord],
    etfs: Sequence[AssetRecord],
    currencies: Sequence[str],
    indices: Sequence[str],
    countries: Sequence[str],
) -> str:
    """Identify the actual enumerated universe, not just CLI market flags."""
    payload = {
        "assets": [asdict(item) for item in assets],
        "etfs": [asdict(item) for item in etfs],
        "currencies": list(currencies),
        "indices": list(indices),
        "countries": list(countries),
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()[:24]


def _command_fingerprint(
    commands: Mapping[str, Sequence[str]],
    schemas: Mapping[str, Mapping[str, Any]],
) -> str:
    """Detect OpenBB coverage/provider-schema changes before resuming."""
    schema_fields: dict[str, dict[str, str]] = {}
    for endpoint, schema in schemas.items():
        input_model = schema.get("input") if isinstance(schema, Mapping) else None
        fields = getattr(input_model, "model_fields", {})
        if isinstance(fields, Mapping):

            def field_signature(field: Any) -> str:
                required = getattr(field, "is_required", False)
                required = required() if callable(required) else bool(required)
                return "|".join(
                    (
                        str(getattr(field, "annotation", "")),
                        repr(getattr(field, "default", "")),
                        str(required),
                    )
                )

            schema_fields[str(endpoint)] = {
                str(name): field_signature(field)
                for name, field in sorted(fields.items())
            }
    payload = {
        "commands": {
            str(endpoint): sorted(str(provider) for provider in providers)
            for endpoint, providers in sorted(commands.items())
        },
        "schema_endpoints": sorted(str(endpoint) for endpoint in schemas),
        "schema_fields": schema_fields,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()[:24]


def _load_resumable_plan(
    output_dir: Path,
    manifest: Manifest,
    *,
    plan_token: str,
    start_date: str,
    end_date: str,
    credential_names: set[str],
) -> tuple[int, list[CoverageDecision], dict[str, Any]] | None:
    """Return a verified persisted plan, otherwise require full regeneration.

    A supervisor recycle must be cheap, but skipping planning is safe only when
    the durable contract proves that it describes this exact run boundary and
    credential surface.  A legacy plan without a version can be adopted once;
    future plan-logic changes must bump ``PLANNER_STATE_VERSION``.
    """

    if manifest.meta_value("active_plan_token") != plan_token:
        return None
    version_key = f"planner_state_version:{plan_token}"
    saved_version = manifest.meta_value(version_key)
    if saved_version not in {None, str(PLANNER_STATE_VERSION)}:
        return None
    if manifest.active_task_count(plan_token) <= 0:
        return None

    catalog_dir = output_dir / "catalog"
    summary_path = catalog_dir / "completeness_contract_summary.json"
    coverage_path = catalog_dir / "coverage.parquet"
    credentials_path = catalog_dir / "configured_credentials.parquet"
    audit_path = catalog_dir / "completeness_contract_audit.parquet"
    if not all(
        path.is_file()
        for path in (summary_path, coverage_path, credentials_path, audit_path)
    ):
        return None
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
            not bool(summary.get("passed"))
            or int(summary.get("unresolved", -1)) != 0
            or str(summary.get("start_date")) != start_date
            or str(summary.get("end_date")) != end_date
        ):
            return None
        audit_table = pq.read_table(audit_path, columns=["status"])
        if audit_table.num_rows != int(summary.get("contract_rows", -1)):
            return None
        if any(value != "pass" for value in audit_table.column(0).to_pylist()):
            return None

        credential_rows = pq.read_table(credentials_path).to_pylist()
        saved_credentials = {
            str(item["credential_field"])
            for item in credential_rows
            if bool(item.get("configured"))
        }
        if saved_credentials != credential_names:
            return None

        coverage_rows = pq.read_table(coverage_path).to_pylist()
        coverage = [CoverageDecision(**item) for item in coverage_rows]
        if not coverage:
            return None
        if sum(item.decision == "included" for item in coverage) != int(
            summary.get("included_endpoints", -1)
        ):
            return None
        if sum(item.decision == "deferred" for item in coverage) != int(
            summary.get("deferred_catalog_endpoints", -1)
        ):
            return None
    except (OSError, ValueError, TypeError, KeyError, pa.ArrowException):
        return None

    manifest.set_meta_value(version_key, str(PLANNER_STATE_VERSION))
    initial_task_count = sum(int(item.initial_task_count) for item in coverage)
    return initial_task_count, coverage, dict(summary)


def _select_resumable_plan(
    output_dir: Path,
    manifest: Manifest,
    *,
    candidate_plan_token: str,
    plan_scope_fingerprint: str,
    command_fingerprint: str,
    start_date: str,
    end_date: str,
    credential_names: set[str],
) -> (
    tuple[
        str,
        tuple[int, list[CoverageDecision], dict[str, Any]],
        str,
    ]
    | None
):
    """Select an exact or scope-compatible durable plan without regenerating it.

    The live symbol CSVs are refreshed independently of a pinned historical
    archive.  Their fingerprint therefore must not turn a normal supervisor
    recycle into millions of redundant SQLite UPSERTs.  Scope, planner version,
    OpenBB command/schema surface, credentials, and the persisted completeness
    contract remain fail-closed boundaries.
    """

    active_plan_token = manifest.meta_value("active_plan_token")
    candidates: list[tuple[str, str]] = [(candidate_plan_token, "exact")]
    if active_plan_token and active_plan_token != candidate_plan_token:
        candidates.append((active_plan_token, "compatible_scope"))

    for plan_token, resume_mode in candidates:
        saved_scope = manifest.meta_value(f"plan_scope_fingerprint:{plan_token}")
        if saved_scope not in {None, plan_scope_fingerprint}:
            continue
        if resume_mode == "compatible_scope" and saved_scope is None:
            # Cross-token adoption is deliberately stricter than exact legacy
            # adoption: without a scope receipt we cannot prove equivalence.
            continue

        saved_version = manifest.meta_value(f"planner_state_version:{plan_token}")
        if resume_mode == "compatible_scope" and saved_version != str(
            PLANNER_STATE_VERSION
        ):
            continue

        command_key = f"plan_command_fingerprint:{plan_token}"
        saved_command = manifest.meta_value(command_key)
        if saved_command not in {None, command_fingerprint}:
            continue

        resumed = _load_resumable_plan(
            output_dir,
            manifest,
            plan_token=plan_token,
            start_date=start_date,
            end_date=end_date,
            credential_names=credential_names,
        )
        if resumed is None:
            continue

        # Planner-state version 10 predates these per-plan receipts.  Persisting
        # the current command fingerprint is a one-time migration; a future
        # OpenBB schema change then fails closed instead of silently adopting.
        manifest.set_meta_value(
            f"plan_scope_fingerprint:{plan_token}", plan_scope_fingerprint
        )
        manifest.set_meta_value(command_key, command_fingerprint)
        return plan_token, resumed, resume_mode
    return None


def _task_execution_affinity(task: DownloadTask) -> str | None:
    """Return a shared-expensive-artifact key for cache-local scheduling."""
    if task.endpoint not in SEC_COMPANYFACTS_STATEMENT_ENDPOINTS:
        return None
    family = "growth" if task.endpoint.endswith("_growth") else "raw"
    return _canonical_json(
        {
            "kind": "sec_statement",
            "family": family,
            "symbol": str(task.kwargs.get("symbol") or "").upper(),
            "period": str(task.kwargs.get("period") or ""),
            "pit_mode": bool(task.kwargs.get("pit_mode", True)),
            "include_preliminary": bool(task.kwargs.get("include_preliminary", True)),
        }
    )


def _pop_fairest_endpoint_task(
    tasks: deque[DownloadTask],
    active_by_endpoint: Mapping[str, int],
    endpoint_caps: Mapping[str, int] | None = None,
    preferred_affinities: set[str] | None = None,
) -> DownloadTask | None:
    """Pop the oldest task from the least-active endpoint in one provider.

    Provider rate limits are independent, but a provider's slow CPU/fan-out
    route must not occupy every execution slot while already-buffered routes
    wait behind it.  The queue is small and bounded, so an O(queue) selection
    gives deterministic endpoint fairness while retaining FIFO inside each
    endpoint.
    """
    if not tasks:
        raise IndexError("cannot pop from an empty provider task queue")
    eligible_indices = [
        index
        for index in range(len(tasks))
        if int(active_by_endpoint.get(tasks[index].endpoint, 0))
        < int((endpoint_caps or {}).get(tasks[index].endpoint, 2**31 - 1))
    ]
    if not eligible_indices:
        return None
    best_index = min(
        eligible_indices,
        key=lambda index: (
            (
                0
                if (
                    preferred_affinities
                    and _task_execution_affinity(tasks[index]) in preferred_affinities
                )
                else 1
            ),
            int(active_by_endpoint.get(tasks[index].endpoint, 0)),
            index,
        ),
    )
    tasks.rotate(-best_index)
    try:
        return tasks.popleft()
    finally:
        tasks.rotate(best_index)


def execute_download_tasks(
    context: PlannerContext,
    manifest: Manifest,
    worker: Any,
    *,
    plan_token: str,
    workers: int,
    batch_size: int,
    max_tasks: int | None,
    max_total_attempts: int,
    no_discovery: bool,
    no_progress: bool,
    entitlement_probe_task_ids: set[str] | None = None,
) -> tuple[int, dict[str, int]]:
    """Drain eligible tasks with rolling refill instead of batch barriers.

    A provider task can legitimately run for minutes while expanding SEC or
    Congress metadata.  Waiting for the final future in a fixed batch leaves
    every other worker idle.  Keep at most ``batch_size`` claimed futures and
    refill once the queue falls to one worker-width, while preserving the
    manifest's fair endpoint selection and checkpoint semantics.
    """
    attempted = 0
    entitlement_probe_task_ids = set(entitlement_probe_task_ids or ())
    totals = {
        "success": 0,
        "empty": 0,
        "pending": 0,
        "failed": 0,
        "unavailable": 0,
        "bulk_unavailable": 0,
        "discovered": 0,
    }
    worker_count = max(1, int(workers))
    target_inflight = max(1, int(batch_size))
    refill_threshold = 0
    provider_refill_thresholds: dict[str | None, int] = {}
    last_gc_attempt = 0
    eligible_tasks = manifest.pending_count(max_total_attempts, plan_token)
    displayed_total = (
        min(eligible_tasks, max_tasks) if max_tasks is not None else eligible_tasks
    )
    download_progress = tqdm(
        total=displayed_total,
        desc="openbb:download total",
        unit="task",
        position=0,
        disable=no_progress,
    )
    scheduler_progress = tqdm(
        total=0,
        desc="openbb:rolling scheduler",
        unit="task",
        position=1,
        leave=False,
        disable=no_progress,
    )
    wave_number = 0
    futures: dict[Any, DownloadTask] = {}
    submitted_at: dict[Any, float] = {}
    scheduled_provider: dict[Any, str | None] = {}
    # Claimed tasks waiting for an execution slot are the control-plane
    # prefetch buffer.  They remain out of ThreadPoolExecutor until a provider
    # future finishes, so a slow SQLite refill cannot starve the provider while
    # limiter waiters do not inflate the process thread count.
    buffered_tasks: dict[str | None, deque[DownloadTask]] = {}
    # Provider-specific indexed top-ups normally complete well inside this
    # window.  Thirty seconds also bridges the deliberately infrequent full
    # 135-route fairness scan without converting buffered work into executor
    # threads.  This is Little's-Law sizing: queue >= request rate * control-
    # plane refill latency.
    # The executor consumes this queue without help from the manifest thread.
    # Size it for two minutes of request starts so fsync, discovery, bulk status
    # updates, and Python/Arrow cleanup cannot create provider-wide request
    # gaps. Fanout tasks consume several requests each, making this estimate
    # conservative. The global batch/worker limit remains the hard aggregate
    # memory/thread bound.
    provider_prefetch_horizon_seconds = 120.0
    provider_prefetch_cap = 512
    full_fair_refill_interval_seconds = 300.0
    # Network producers and the durable manifest consumer need an explicit,
    # bounded hand-off. Reap enough results per SQLite transaction to amortize
    # FULL fsync cost, but stop admitting replacements when retained completed
    # results fill the hand-off queue. This bound is provider-agnostic: a slow
    # Yahoo result, SEC discovery, or Congress page cannot consume capacity for
    # every other market indefinitely.
    completion_persistence_batch_size = min(
        COMPLETION_PERSISTENCE_BATCH_CAP, target_inflight
    )
    completion_backpressure_limit = min(COMPLETION_BACKPRESSURE_CAP, target_inflight)
    last_full_refill_monotonic = 0.0
    runtime = getattr(worker, "runtime", None)
    preload_provider_queues = bool(getattr(worker, "preload_provider_queues", False))
    unavailable_bucket = "__runtime_unavailable__"
    cooldown_bucket = "__runtime_cooldown__"
    provider_endpoint_last_selected: dict[tuple[str | None, str], int] = {}
    provider_endpoint_selection_order = 0
    scheduler_state_path = context.output_dir / "_state" / "provider_scheduler.json"
    last_scheduler_state_monotonic = 0.0
    # Scarce daily quotas must first establish whether every selected route is
    # usable. Otherwise a provider can spend its entire allowance deepening
    # already-started endpoints while another market remains at zero forever.
    accepted_endpoints = manifest.accepted_endpoints(plan_token)
    provider_seed_routes: dict[str, list[tuple[str, str]]] = {}
    endpoint_selected_providers: dict[str, tuple[str, ...]] = {}
    for raw_endpoint, providers in context.commands.items():
        endpoint = str(raw_endpoint).strip().lstrip(".")
        if not endpoint or "." not in endpoint:
            continue
        category = endpoint.split(".", 1)[0]
        selected_providers = select_providers(endpoint, providers, context)
        endpoint_selected_providers[endpoint] = selected_providers
        for provider in selected_providers:
            provider_seed_routes.setdefault(str(provider).lower(), []).append(
                (category, endpoint)
            )
    provider_seed_miss_until_wave: dict[str, int] = {}
    provider_seed_route_cursor: dict[str, int] = {}
    last_total_refresh_monotonic = time.monotonic()

    def execute_and_discover(task: DownloadTask) -> TaskResult:
        """Run provider I/O, then expand its catalog outside provider slots.

        Discovery can scan and materialize tens of thousands of follow-up
        tasks. Doing that in the manifest/control thread couples every market
        to the largest catalog result and lets otherwise full provider queues
        drain while no replacement work is admitted. The provider worker has
        already left its semaphore when it returns here, so independent API
        lanes can immediately reuse the slot while this executor thread does
        the CPU-only expansion. The control thread receives compact follow-up
        tasks instead of retaining the source records during SQLite commits.
        """
        result = worker(task)
        try:
            if not no_discovery:
                result.followups = discover_followup_tasks(context, result)
            return result
        finally:
            result.records.clear()

    def cooldown_providers() -> set[str]:
        method = getattr(runtime, "cooldown_providers", None)
        return set(method()) if callable(method) else set()

    def scheduler_excluded_providers(endpoint: str | None = None) -> set[str]:
        """Providers that cannot be the next runnable fallback for a route."""
        cooldown = cooldown_providers()
        excluded = set(cooldown)
        globally_unavailable: set[str] = set()
        unavailable_method = getattr(runtime, "unavailable", None)
        if callable(unavailable_method):
            globally_unavailable.update(str(item) for item in unavailable_method())
            excluded.update(globally_unavailable)
        if endpoint is not None:
            unavailable_routes_method = getattr(runtime, "unavailable_routes", None)
            if callable(unavailable_routes_method):
                excluded.update(
                    str(provider)
                    for provider, route_endpoint in unavailable_routes_method()
                    if route_endpoint == endpoint
                )
        local_bypass = getattr(worker, "can_run_during_provider_cooldown", None)
        if callable(local_bypass):
            candidates = (
                cooldown
                if endpoint is not None
                else set(
                    getattr(worker, "local_cooldown_bypass_providers", lambda: set())()
                )
            )
            for provider in candidates:
                if provider in globally_unavailable:
                    continue
                if endpoint is None or local_bypass(provider, endpoint):
                    excluded.discard(provider)
        return excluded

    def scheduler_excluded_endpoints() -> set[str]:
        """Skip routes whose entire selected chain is temporarily unusable.

        Without this route-level guard, asking SQLite for one schedulable row
        from a 1.5-million-row FMP-only endpoint during its daily cooldown
        scans every row just to return nothing.  The same rule applies to every
        market/provider combination and is recalculated on each refill so a
        route becomes eligible immediately when its cooldown expires.
        """
        excluded: set[str] = set()
        for endpoint, providers in endpoint_selected_providers.items():
            if providers and all(
                provider in scheduler_excluded_providers(endpoint)
                for provider in providers
            ):
                excluded.add(endpoint)
        return excluded

    def next_cooldown_delay() -> float | None:
        method = getattr(runtime, "next_cooldown_delay", None)
        return method() if callable(method) else None

    def task_retry_wait_state() -> tuple[int, str | None, float | None]:
        """Read the durable retry frontier once before a scheduler sleep.

        The manifest contains millions of rows. Re-running COUNT/MIN over it
        every 30 seconds merely to keep the scheduler heartbeat fresh turns a
        quota wait into continuous page-cache traffic. No other process owns
        task retry deadlines, so this frontier cannot move until this executor
        wakes and attempts work again.
        """
        state_method = getattr(manifest, "retry_deferred_state", None)
        if callable(state_method):
            deferred, deadline = state_method(plan_token)
            if deadline is None:
                return int(deferred), None, None
            try:
                due = datetime.fromisoformat(str(deadline))
                if due.tzinfo is None:
                    due = due.replace(tzinfo=timezone.utc)
                delay = max(
                    0.0,
                    (due - datetime.now(timezone.utc)).total_seconds(),
                )
            except ValueError:
                delay = 0.0
            return int(deferred), str(deadline), delay

        delay_method = getattr(manifest, "next_retry_delay", None)
        delay = delay_method(plan_token) if callable(delay_method) else None
        return 0, None, delay

    def scheduling_provider(task: DownloadTask) -> str | None:
        """Predict the first provider that can actually consume this task.

        The worker still owns the authoritative availability recheck and
        semaphore.  This scheduler-side prediction is a reservation: it keeps
        more tasks than a provider can run from being claimed and immediately
        returned to pending.  ProviderRuntime is intentionally optional so
        simple/test workers retain the historical global-worker behavior.
        """
        availability = getattr(runtime, "availability", None)
        if not callable(availability):
            return None
        saw_cooldown = False
        for provider in provider_execution_order(task.providers):
            if provider in task.provider_outcomes:
                continue
            available, reason = availability(provider, task.endpoint)
            if available:
                return provider
            local_bypass = getattr(worker, "can_run_during_provider_cooldown", None)
            if (
                callable(local_bypass)
                and str(reason or "").startswith("cooldown until")
                and local_bypass(provider, task.endpoint)
            ):
                return provider
            if reason and str(reason).startswith("cooldown until"):
                saw_cooldown = True
        if saw_cooldown:
            # Other providers in the chain may already have returned durable
            # empty/unavailable outcomes.  Wait only when no unresolved
            # provider can currently make progress.
            return cooldown_bucket
        # Permanently unavailable routes must still run once so their terminal
        # status is persisted.  Give those cheap bookkeeping tasks a bounded,
        # provider-independent bucket rather than starving them forever.
        return unavailable_bucket

    def provider_available_for_scheduling(provider: str, endpoint: str) -> bool:
        """Apply the worker's local cooldown bypass at every refill boundary.

        ``ProviderRuntime.availability`` describes the remote API.  A worker
        can still make progress through a local/official bulk path, so refill
        code must use the same effective availability rule as
        ``scheduling_provider``.  Keeping this predicate here prevents the
        initial queue and provider-specific top-ups from disagreeing after an
        API quota enters cooldown.
        """
        availability = getattr(runtime, "availability", None)
        if not callable(availability):
            return True
        available, reason = availability(provider, endpoint)
        if available:
            return True
        local_bypass = getattr(worker, "can_run_during_provider_cooldown", None)
        return bool(
            callable(local_bypass)
            and str(reason or "").startswith("cooldown until")
            and local_bypass(provider, endpoint)
        )

    def scheduling_limit(provider: str | None) -> int:
        if provider is None:
            return target_inflight
        if provider == unavailable_bucket:
            return worker_count
        concurrency = getattr(runtime, "concurrency", {})
        return max(1, int(concurrency.get(provider, 2)))

    def endpoint_scheduling_limit(provider: str | None, endpoint: str) -> int:
        if provider is None:
            return 2**31 - 1
        return max(
            1,
            int(
                PROVIDER_ENDPOINT_CONCURRENCY_CAPS.get(
                    (str(provider), str(endpoint)),
                    2**31 - 1,
                )
            ),
        )

    def endpoint_caps(provider: str | None) -> dict[str, int]:
        if provider is None:
            return {}
        return {
            endpoint: limit
            for (item_provider, endpoint), limit in (
                PROVIDER_ENDPOINT_CONCURRENCY_CAPS.items()
            )
            if item_provider == provider
        }

    def scheduling_queue_limit(provider: str | None) -> int:
        """Return execution slots plus a request-rate-sized refill buffer."""
        execution_slots = scheduling_limit(provider)
        if provider in {None, unavailable_bucket, cooldown_bucket}:
            return execution_slots
        runtime_rps = getattr(runtime, "rps", {})
        rps = max(
            0.001,
            float(runtime_rps.get(provider, DEFAULT_UNDOCUMENTED_PROVIDER_RPS)),
        )
        prefetch = min(
            provider_prefetch_cap,
            max(1, math.ceil(rps * provider_prefetch_horizon_seconds)),
        )
        return execution_slots + prefetch

    def executor_lane_limit(provider: str | None) -> int:
        """Bound provider threads independently from prefetched task count.

        Production lanes reserve the current Little's-Law service capacity
        plus one evidence-driven adaptive expansion step. Reserving the whole
        fifteen-second adaptive ceiling eagerly created idle semaphore waiters
        and made all provider limiter dispatchers compete for the GIL. Tasks
        above this finite near-term service horizon stay in the lane executor's
        internal queue and consume no thread. Cheap unavailable bookkeeping
        receives a small control-plane lane instead of inheriting the archive's
        full 1,792-task buffer as operating-system threads.
        """
        current = scheduling_limit(provider)
        if provider in {None, unavailable_bucket, cooldown_bucket}:
            return min(current, 32)
        if not preload_provider_queues:
            return current
        return _adaptive_executor_lane_limit(runtime, str(provider), current)

    def refill_trigger_for_restored(provider: str | None, restored: int) -> int:
        """Return a 95%-full refill trigger bounded by provider capacity."""
        queue_limit = scheduling_queue_limit(provider)
        bounded = min(queue_limit, max(1, int(restored)))
        return max(
            scheduling_limit(provider),
            bounded - max(1, math.ceil(bounded * 0.05)),
        )

    def effective_refill_threshold(provider: str | None) -> int:
        """Clamp stale thresholds after completed futures/provider resizing."""
        raw = int(provider_refill_thresholds.get(provider, 0))
        if raw <= 0:
            return 0
        return min(
            raw,
            refill_trigger_for_restored(provider, scheduling_queue_limit(provider)),
        )

    def buffered_task_count() -> int:
        return sum(len(tasks) for tasks in buffered_tasks.values())

    def active_reservation_counts() -> dict[str | None, int]:
        """Count only futures that still occupy provider execution capacity.

        SQLite refill can take several seconds on a multi-million-row
        manifest. Futures that finish during that read remain in the main
        dictionary until the event loop reaps them, but must not masquerade as
        live provider reservations and suppress the next refill wave.
        """
        counts: dict[str | None, int] = {}
        for future, provider in scheduled_provider.items():
            if future.done():
                continue
            counts[provider] = counts.get(provider, 0) + 1
        return counts

    def queued_reservation_counts() -> dict[str | None, int]:
        """Count running futures and claimed tasks waiting in memory."""
        counts = active_reservation_counts()
        for provider, tasks in buffered_tasks.items():
            if tasks:
                counts[provider] = counts.get(provider, 0) + len(tasks)
        return counts

    def publish_scheduler_state(
        *,
        phase: str = "running",
        wait_reason: str | None = None,
        wait_delay: float | None = None,
        retry_state: tuple[int, str | None] | None = None,
        force: bool = False,
    ) -> None:
        """Publish each provider's independent live queue and execution pool."""
        nonlocal last_scheduler_state_monotonic
        now_monotonic = time.monotonic()
        if not force and now_monotonic - last_scheduler_state_monotonic < 5.0:
            return
        active = active_reservation_counts()
        buffered = {
            provider: len(tasks) for provider, tasks in buffered_tasks.items() if tasks
        }
        reservations = queued_reservation_counts()
        runtime_rps = getattr(runtime, "rps", {})
        runtime_concurrency = getattr(runtime, "concurrency", {})
        providers = (
            set(runtime_rps)
            | set(runtime_concurrency)
            | set(active)
            | set(buffered)
            | set(reservations)
            | set(provider_refill_thresholds)
        )
        cooldown = cooldown_providers()
        unavailable_method = getattr(runtime, "unavailable", None)
        unavailable = (
            set(str(item) for item in unavailable_method())
            if callable(unavailable_method)
            else set()
        )
        if retry_state is None:
            retry_state_method = getattr(manifest, "retry_deferred_state", None)
            retry_deferred_total, next_task_retry_at = (
                retry_state_method(plan_token)
                if callable(retry_state_method)
                else (0, None)
            )
        else:
            retry_deferred_total, next_task_retry_at = retry_state

        def provider_key(provider: str | None) -> str:
            return "__unassigned__" if provider is None else str(provider)

        provider_rows: dict[str, dict[str, Any]] = {}
        for provider in sorted(providers, key=lambda item: str(item)):
            key = provider_key(provider)
            special = provider in {
                None,
                unavailable_bucket,
                cooldown_bucket,
            }
            provider_rows[key] = {
                "requests_per_second": (
                    None if special else float(runtime_rps.get(provider, 0.0))
                ),
                "execution_limit": int(scheduling_limit(provider)),
                "executor_thread_limit": int(executor_lane_limit(provider)),
                "queue_limit": int(scheduling_queue_limit(provider)),
                "active": int(active.get(provider, 0)),
                "buffered": int(buffered.get(provider, 0)),
                "reservations": int(reservations.get(provider, 0)),
                "refill_threshold": int(effective_refill_threshold(provider)),
                "seed_route_count": int(
                    len(provider_seed_routes.get(str(provider), ()))
                ),
                "cooldown": bool(provider in cooldown),
                "unavailable": bool(provider in unavailable),
            }
        wait_until = None
        if phase == "waiting" and wait_delay is not None:
            wait_until = (
                datetime.now(timezone.utc)
                + timedelta(seconds=max(0.0, float(wait_delay)))
            ).isoformat()
        _write_json_atomic(
            scheduler_state_path,
            {
                "schema_version": 4,
                "phase": phase,
                "wait_reason": wait_reason if phase == "waiting" else None,
                "wait_until": wait_until,
                "pid": os.getpid(),
                "plan_token": plan_token,
                "wave": wave_number,
                "attempted_this_run": attempted,
                "global_worker_limit": worker_count,
                "global_queue_limit": target_inflight,
                "preloaded_provider_queues": preload_provider_queues,
                "active_total": sum(1 for future in futures if not future.done()),
                "completed_pending_total": sum(
                    1 for future in futures if future.done()
                ),
                "completion_persistence_batch_size": int(
                    completion_persistence_batch_size
                ),
                "completion_backpressure_limit": int(completion_backpressure_limit),
                "completion_backpressure_active": sum(
                    1 for future in futures if future.done()
                )
                > completion_backpressure_limit,
                "buffered_total": buffered_task_count(),
                "retry_deferred_total": int(retry_deferred_total),
                "next_task_retry_at": next_task_retry_at,
                "providers": provider_rows,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        last_scheduler_state_monotonic = now_monotonic

    def submit_buffered(executor: ProviderExecutorPool) -> int:
        """Dispatch provider queues from memory without another DB read.

        Production archive workers preload the bounded queue into its provider
        lane's internal executor queue.  Queued tasks consume no thread; only
        that provider's finite service horizon can run or wait on its adaptive
        semaphore. Lightweight/test workers retain the historical
        execution-slot-only dispatch contract.
        """
        ready: list[tuple[str | None, DownloadTask]] = []
        active = active_reservation_counts()
        active_endpoints: dict[tuple[str | None, str], int] = {}
        active_affinities: dict[str | None, set[str]] = {}
        for future, task in futures.items():
            if future.done():
                continue
            provider = scheduled_provider.get(future)
            key = (provider, task.endpoint)
            active_endpoints[key] = active_endpoints.get(key, 0) + 1
            affinity = _task_execution_affinity(task)
            if affinity is not None:
                active_affinities.setdefault(provider, set()).add(affinity)
        global_capacity = max(0, target_inflight - len(futures))
        if global_capacity <= 0:
            return 0
        # Give every provider its real execution slots before any provider can
        # consume aggregate capacity with standby workers. This remains fair
        # even when an unavailable/bookkeeping bucket contains thousands of
        # cheap tasks or the configured batch is smaller than all queue limits.
        dispatch_targets = [scheduling_limit]
        if preload_provider_queues:
            dispatch_targets.append(scheduling_queue_limit)
        for dispatch_target in dispatch_targets:
            providers = sorted(
                (provider for provider, tasks in buffered_tasks.items() if tasks),
                key=lambda provider: (
                    active.get(provider, 0) / dispatch_target(provider),
                    str(provider),
                ),
            )
            for provider in providers:
                provider_capacity = max(
                    0, dispatch_target(provider) - active.get(provider, 0)
                )
                provider_capacity = min(provider_capacity, global_capacity - len(ready))
                tasks = buffered_tasks[provider]
                while provider_capacity > 0 and tasks:
                    task = _pop_fairest_endpoint_task(
                        tasks,
                        {
                            endpoint: count
                            for (
                                active_provider,
                                endpoint,
                            ), count in active_endpoints.items()
                            if active_provider == provider
                        },
                        endpoint_caps(provider),
                        active_affinities.get(provider),
                    )
                    if task is None:
                        break
                    ready.append((provider, task))
                    active[provider] = active.get(provider, 0) + 1
                    endpoint_key = (provider, task.endpoint)
                    active_endpoints[endpoint_key] = (
                        active_endpoints.get(endpoint_key, 0) + 1
                    )
                    affinity = _task_execution_affinity(task)
                    if affinity is not None:
                        active_affinities.setdefault(provider, set()).add(affinity)
                    provider_capacity -= 1
                if not tasks:
                    buffered_tasks.pop(provider, None)
                if len(ready) >= global_capacity:
                    break
            if len(ready) >= global_capacity:
                break
        # One SQLite transaction marks the whole executor admission wave.  Do
        # this immediately before submit so a crash can recover every admitted
        # task while monitor timestamps remain faithful to actual execution.
        manifest.mark_executing([task for _, task in ready])
        for provider, task in ready:
            future = executor.submit(provider, execute_and_discover, task)
            futures[future] = task
            submitted_at[future] = time.monotonic()
            scheduled_provider[future] = provider
        return len(ready)

    def refresh_total() -> None:
        nonlocal last_total_refresh_monotonic
        now_monotonic = time.monotonic()
        # The initial total is exact. Recounting nearly six million pending
        # rows on every small provider refill creates a request-supply gap
        # longer than an 80-call queue. Discovery can change the total, so
        # refresh periodically while keeping refill off the count-scan path.
        if now_monotonic - last_total_refresh_monotonic < 60.0:
            return
        remaining = manifest.pending_count(max_total_attempts, plan_token)
        refreshed_total = attempted + len(futures) + buffered_task_count() + remaining
        if max_tasks is not None:
            refreshed_total = min(max_tasks, refreshed_total)
        download_progress.total = max(download_progress.n, refreshed_total)
        download_progress.refresh()
        last_total_refresh_monotonic = now_monotonic

    def refill(executor: ProviderExecutorPool) -> int:
        nonlocal wave_number, provider_endpoint_selection_order
        nonlocal refill_threshold, provider_refill_thresholds
        nonlocal last_full_refill_monotonic
        last_full_refill_monotonic = time.monotonic()
        capacity = target_inflight - len(futures) - buffered_task_count()
        if max_tasks is not None:
            capacity = min(
                capacity,
                max_tasks - attempted - len(futures) - buffered_task_count(),
            )
        if capacity <= 0:
            return 0
        # Read beyond the immediate global capacity so a large backlog for one
        # saturated provider cannot hide runnable tasks for other markets or
        # providers.  Only the selected tasks are claimed.
        # 512 candidates seed all 135 current routes several times. Provider-
        # specific indexed deepening below fills any remaining reservation.
        # Reading 4,096 JSON-bearing rows on every refill outlived the smaller
        # Little's-Law queues and produced visible zero-active-call gaps.
        candidate_limit = max(
            1,
            min(512, max(capacity, worker_count) * 8),
        )
        candidates = manifest.pending_batch(
            candidate_limit,
            max_total_attempts,
            plan_token,
            excluded_providers=scheduler_excluded_providers(),
            excluded_endpoints=scheduler_excluded_endpoints(),
        )
        if not candidates:
            refresh_total()
            return 0
        reservations = queued_reservation_counts()
        provider_buckets: dict[
            str | None, dict[str, list[tuple[int, DownloadTask]]]
        ] = {}
        for candidate_index, task in enumerate(candidates):
            provider = scheduling_provider(task)
            if provider == cooldown_bucket:
                continue
            provider_buckets.setdefault(provider, {}).setdefault(
                task.endpoint, []
            ).append((candidate_index, task))

        # Endpoint fairness alone is not provider fairness.  A single endpoint
        # can contain large provider-specific task families; for example,
        # thousands of older FRED-only yield-curve rows may sort before the
        # first Federal Reserve row.  Give every currently usable provider one
        # indexed seed before provider-specific deepening so each independent
        # limiter can make progress at its own ceiling.
        next_candidate_index = len(candidates)
        known_task_ids = {task.task_id for task in candidates}
        availability = getattr(runtime, "availability", None)
        runtime_concurrency = getattr(runtime, "concurrency", {})
        if callable(availability) and isinstance(runtime_concurrency, Mapping):
            excluded_now = scheduler_excluded_providers()
            for provider in sorted(str(item) for item in runtime_concurrency):
                if provider in provider_buckets or provider in excluded_now:
                    continue
                if wave_number < provider_seed_miss_until_wave.get(provider, 0):
                    continue
                routes = provider_seed_routes.get(provider, [])
                if not routes:
                    provider_seed_miss_until_wave[provider] = wave_number + 100
                    continue
                route_start = provider_seed_route_cursor.get(provider, 0) % len(routes)
                rotated_routes = routes[route_start:] + routes[:route_start]
                found_provider_seed = False
                visited_routes = 0
                for category, endpoint in rotated_routes:
                    visited_routes += 1
                    if not provider_available_for_scheduling(provider, endpoint):
                        continue
                    seed_tasks = manifest.pending_endpoint_batch(
                        category,
                        endpoint,
                        8,
                        max_total_attempts,
                        plan_token,
                        excluded_providers=scheduler_excluded_providers(endpoint),
                        required_provider=provider,
                    )
                    for seed_task in seed_tasks:
                        actual_provider = scheduling_provider(seed_task)
                        if actual_provider == cooldown_bucket:
                            continue
                        if seed_task.task_id not in known_task_ids:
                            provider_buckets.setdefault(actual_provider, {}).setdefault(
                                seed_task.endpoint, []
                            ).append((next_candidate_index, seed_task))
                            known_task_ids.add(seed_task.task_id)
                            next_candidate_index += 1
                        if actual_provider == provider:
                            found_provider_seed = True
                            break
                    if found_provider_seed:
                        break
                if visited_routes:
                    provider_seed_route_cursor[provider] = (
                        route_start + visited_routes
                    ) % len(routes)
                if found_provider_seed:
                    provider_seed_miss_until_wave.pop(provider, None)
                else:
                    # Follow-up discovery can add work later, so a miss is a
                    # bounded probe cache rather than a permanent conclusion.
                    provider_seed_miss_until_wave[provider] = wave_number + 100

        # The fair manifest read intentionally gives every endpoint an equal
        # first share. With 135 routes, however, a 4,096-row candidate pool
        # exposes only about 31 rows from a large single-endpoint provider and
        # cannot fill a latency-sized reservation (for example 320 Federal
        # Reserve yield-curve calls). Deepen only the indexed endpoints already
        # mapped to an underfilled provider. This preserves cross-market first
        # coverage without materializing tens of thousands of unused tasks.
        provider_endpoint_offsets: dict[tuple[str, str, str], int] = {}
        provider_queue = [
            provider
            for provider in provider_buckets
            if provider not in {None, unavailable_bucket, cooldown_bucket}
        ]
        queued_providers = set(provider_queue)
        provider_index = 0
        while provider_index < len(provider_queue):
            provider = provider_queue[provider_index]
            provider_index += 1
            provider_slots = max(
                0,
                scheduling_queue_limit(provider) - reservations.get(provider, 0),
            )
            endpoint_buckets = provider_buckets.get(provider, {})
            needed = provider_slots - sum(
                len(items) for items in endpoint_buckets.values()
            )
            while needed > 0 and endpoint_buckets:
                endpoint_names = list(endpoint_buckets)
                per_endpoint_extra = max(1, math.ceil(needed / len(endpoint_names)))
                loaded = 0
                for endpoint in endpoint_names:
                    if needed <= 0:
                        break
                    sample_task = endpoint_buckets[endpoint][0][1]
                    endpoint_exclusions = scheduler_excluded_providers(
                        sample_task.endpoint
                    )
                    if provider in endpoint_exclusions:
                        needed = 0
                        break
                    key = (
                        provider,
                        sample_task.category,
                        sample_task.endpoint,
                    )
                    extra_tasks = manifest.pending_endpoint_batch(
                        sample_task.category,
                        sample_task.endpoint,
                        min(per_endpoint_extra, needed),
                        max_total_attempts,
                        plan_token,
                        excluded_providers=endpoint_exclusions,
                        required_provider=provider,
                        offset=provider_endpoint_offsets.get(key, 0),
                    )
                    provider_endpoint_offsets[key] = provider_endpoint_offsets.get(
                        key, 0
                    ) + len(extra_tasks)
                    loaded += len(extra_tasks)
                    for extra_task in extra_tasks:
                        actual_provider = scheduling_provider(extra_task)
                        if actual_provider == cooldown_bucket:
                            continue
                        if extra_task.task_id in known_task_ids:
                            continue
                        provider_buckets.setdefault(actual_provider, {}).setdefault(
                            extra_task.endpoint, []
                        ).append((next_candidate_index, extra_task))
                        known_task_ids.add(extra_task.task_id)
                        next_candidate_index += 1
                        if (
                            actual_provider
                            not in {None, unavailable_bucket, cooldown_bucket}
                            and actual_provider not in queued_providers
                        ):
                            provider_queue.append(actual_provider)
                            queued_providers.add(actual_provider)
                    needed = provider_slots - sum(
                        len(items)
                        for items in provider_buckets.get(provider, {}).values()
                    )
                if loaded == 0:
                    break

        selected: list[tuple[int, DownloadTask, str | None]] = []
        local_last_selected = dict(provider_endpoint_last_selected)
        local_selection_order = provider_endpoint_selection_order
        queued_endpoint_counts: dict[tuple[str | None, str], int] = {}
        for future, task in futures.items():
            if future.done():
                continue
            provider = scheduled_provider.get(future)
            key = (provider, task.endpoint)
            queued_endpoint_counts[key] = queued_endpoint_counts.get(key, 0) + 1
        for provider, tasks in buffered_tasks.items():
            for task in tasks:
                key = (provider, task.endpoint)
                queued_endpoint_counts[key] = queued_endpoint_counts.get(key, 0) + 1
        # When the global/max-task capacity is tighter than the sum of all
        # deficits, refill the most depleted provider first. This prevents a
        # mostly full slow provider from consuming the slots needed to restart
        # a fast provider whose queue reached zero during the manifest read.
        selected_counts: dict[str | None, int] = {}
        # First restore every provider's execution slots, then spend remaining
        # global capacity on the refill buffer. Otherwise a large buffer for
        # the alphabetically first provider can consume a tight batch before a
        # second provider receives even one runnable task.
        for queue_target in (scheduling_limit, scheduling_queue_limit):
            provider_fill_order = sorted(
                provider_buckets,
                key=lambda provider: (
                    (reservations.get(provider, 0) + selected_counts.get(provider, 0))
                    / queue_target(provider),
                    str(provider),
                ),
            )
            for provider in provider_fill_order:
                endpoint_buckets = provider_buckets[provider]
                provider_slots = max(
                    0,
                    queue_target(provider)
                    - reservations.get(provider, 0)
                    - selected_counts.get(provider, 0),
                )
                provider_slots = min(provider_slots, capacity - len(selected))
                while provider_slots > 0:
                    available_endpoints = [
                        endpoint
                        for endpoint, items in endpoint_buckets.items()
                        if items
                        and queued_endpoint_counts.get((provider, endpoint), 0)
                        < endpoint_scheduling_limit(provider, endpoint)
                    ]
                    if not available_endpoints:
                        break
                    endpoint = min(
                        available_endpoints,
                        key=lambda item: (
                            (
                                item in accepted_endpoints
                                or queued_endpoint_counts.get((provider, item), 0) > 0
                            ),
                            local_last_selected.get((provider, item), -1),
                            endpoint_buckets[item][0][0],
                        ),
                    )
                    candidate_index, task = endpoint_buckets[endpoint].pop(0)
                    selected.append((candidate_index, task, provider))
                    selected_counts[provider] = selected_counts.get(provider, 0) + 1
                    queued_key = (provider, task.endpoint)
                    queued_endpoint_counts[queued_key] = (
                        queued_endpoint_counts.get(queued_key, 0) + 1
                    )
                    local_selection_order += 1
                    local_last_selected[(provider, endpoint)] = local_selection_order
                    provider_slots -= 1
                    if len(selected) >= capacity:
                        break
                if len(selected) >= capacity:
                    break
            if len(selected) >= capacity:
                break
        batch = [item[1] for item in selected]
        batch_providers = [item[2] for item in selected]
        if not batch:
            refresh_total()
            return 0
        manifest.claim(batch)
        for task, provider in zip(batch, batch_providers, strict=True):
            buffered_tasks.setdefault(provider, deque()).append(task)
            provider_endpoint_selection_order += 1
            provider_endpoint_last_selected[(provider, task.endpoint)] = (
                provider_endpoint_selection_order
            )
        submit_buffered(executor)
        wave_number += 1
        scheduler_progress.total = int(scheduler_progress.total or 0) + len(batch)
        endpoints = sorted({task.endpoint for task in batch})
        endpoint_label = (
            endpoints[0] if len(endpoints) == 1 else f"{endpoints[0]}..{endpoints[-1]}"
        )
        scheduler_progress.set_description(
            f"rolling {wave_number}:{endpoint_label}"[:64], refresh=False
        )
        scheduler_progress.set_postfix(
            queued=len(futures) + buffered_task_count(),
            buffered=buffered_task_count(),
            refill=len(batch),
            refresh=False,
        )
        # The global queue uses a coarse quarter-drained fallback. Each
        # provider starts its own refill after only 5% drains, giving the
        # indexed manifest read time to finish before that provider goes idle.
        queued_count = len(futures) + buffered_task_count()
        refill_chunk = max(1, math.ceil(queued_count * 0.25))
        refill_threshold = max(0, queued_count - refill_chunk)
        # Threshold membership must include the batch just submitted even if a
        # very fast future finishes before this bookkeeping line. The actual
        # refill capacity calculation above still excludes done futures.
        reservation_counts: dict[str | None, int] = {}
        for provider in scheduled_provider.values():
            reservation_counts[provider] = reservation_counts.get(provider, 0) + 1
        for provider, tasks in buffered_tasks.items():
            reservation_counts[provider] = reservation_counts.get(provider, 0) + len(
                tasks
            )
        provider_refill_thresholds = {
            provider: refill_trigger_for_restored(provider, count)
            for provider, count in reservation_counts.items()
        }
        refresh_total()
        return len(batch)

    def refill_depleted_provider_buffers(
        executor: ProviderExecutorPool, *, reserved_completion_count: int = 0
    ) -> int:
        """Top up active providers through indexed endpoint reads.

        A full fair refill probes every selected endpoint and is necessary to
        discover new runnable routes, but it is the wrong hot path when an
        already-active provider merely consumed a few queued tasks.  Read the
        known routes for only the depleted provider, claim those tasks, and
        resupply its execution slots without coupling every market's API
        cadence to a global manifest scan.
        """
        nonlocal provider_endpoint_selection_order
        capacity = target_inflight - len(futures) - buffered_task_count()
        if max_tasks is not None:
            capacity = min(
                capacity,
                max_tasks
                - attempted
                - len(futures)
                - buffered_task_count()
                - max(0, int(reserved_completion_count)),
            )
        if capacity <= 0:
            return 0

        availability = getattr(runtime, "availability", None)
        if not callable(availability):
            return 0
        reservations = queued_reservation_counts()
        selected: list[tuple[DownloadTask, str]] = []
        known_task_ids: set[str] = set()
        queued_endpoint_counts: dict[tuple[str | None, str], int] = {}
        for future, task in futures.items():
            if future.done():
                continue
            provider = scheduled_provider.get(future)
            key = (provider, task.endpoint)
            queued_endpoint_counts[key] = queued_endpoint_counts.get(key, 0) + 1
        for provider, tasks in buffered_tasks.items():
            for task in tasks:
                key = (provider, task.endpoint)
                queued_endpoint_counts[key] = queued_endpoint_counts.get(key, 0) + 1
        providers = sorted(
            (
                provider
                for provider in provider_refill_thresholds
                if provider not in {None, unavailable_bucket, cooldown_bucket}
                and reservations.get(provider, 0)
                <= effective_refill_threshold(provider)
            ),
            key=lambda provider: (
                reservations.get(provider, 0) / scheduling_queue_limit(provider),
                str(provider),
            ),
        )
        for provider in providers:
            if len(selected) >= capacity:
                break
            needed = min(
                scheduling_queue_limit(provider) - reservations.get(provider, 0),
                capacity - len(selected),
            )
            if needed <= 0:
                continue
            routes = provider_seed_routes.get(str(provider), [])
            if not routes:
                continue
            route_start = provider_seed_route_cursor.get(str(provider), 0) % len(routes)
            rotated_routes = routes[route_start:] + routes[:route_start]
            # A provider's HTTP budget is shared by every selected endpoint.
            # Rotate across the full route list in progressive rounds: every
            # route gets an equal first share, then routes that actually have
            # backlog are deepened until the provider queue is restored.  A
            # single pass capped each route at ceil(deficit / route_count), so
            # FRED with one live route among 36 selected routes received only
            # seven tasks for a 252-task queue.  The same structural gap could
            # idle any provider whenever its backlog was concentrated in only
            # a few markets/endpoints.
            rotated_routes = list(enumerate(rotated_routes))
            route_offsets: dict[tuple[str, str, str], int] = {}
            exhausted_routes: set[tuple[str, str, str]] = set()
            visited = 0
            while needed > 0 and len(selected) < capacity:
                active_routes = [
                    item
                    for item in rotated_routes
                    if (provider, item[1][0], item[1][1]) not in exhausted_routes
                ]
                if not active_routes:
                    break
                per_route_limit = max(
                    1,
                    math.ceil(needed / len(active_routes)),
                )
                loaded_this_round = 0
                for original_offset, (category, endpoint) in active_routes:
                    if needed <= 0 or len(selected) >= capacity:
                        break
                    visited += 1
                    route_key = (provider, category, endpoint)
                    if not provider_available_for_scheduling(provider, endpoint):
                        exhausted_routes.add(route_key)
                        continue
                    endpoint_exclusions = scheduler_excluded_providers(endpoint)
                    if provider in endpoint_exclusions:
                        exhausted_routes.add(route_key)
                        continue
                    endpoint_key = (provider, endpoint)
                    endpoint_capacity = endpoint_scheduling_limit(
                        provider, endpoint
                    ) - queued_endpoint_counts.get(endpoint_key, 0)
                    if endpoint_capacity <= 0:
                        exhausted_routes.add(route_key)
                        continue
                    requested = min(needed, per_route_limit, endpoint_capacity)
                    candidates = manifest.pending_endpoint_batch(
                        category,
                        endpoint,
                        requested,
                        max_total_attempts,
                        plan_token,
                        excluded_providers=endpoint_exclusions,
                        required_provider=provider,
                        offset=route_offsets.get(route_key, 0),
                    )
                    route_offsets[route_key] = route_offsets.get(route_key, 0) + len(
                        candidates
                    )
                    if len(candidates) < requested:
                        exhausted_routes.add(route_key)
                    for task in candidates:
                        if task.task_id in known_task_ids:
                            continue
                        if scheduling_provider(task) != provider:
                            continue
                        selected.append((task, provider))
                        known_task_ids.add(task.task_id)
                        queued_endpoint_counts[endpoint_key] = (
                            queued_endpoint_counts.get(endpoint_key, 0) + 1
                        )
                        needed -= 1
                        loaded_this_round += 1
                        if needed <= 0 or len(selected) >= capacity:
                            break
                    # Advance past the actual route just probed, even though
                    # the list was rotated from the provider's persistent
                    # cursor.
                    provider_seed_route_cursor[str(provider)] = (
                        route_start + original_offset + 1
                    ) % len(routes)
                if loaded_this_round == 0:
                    break
            if visited == 0:
                provider_seed_route_cursor[str(provider)] = (route_start + 1) % len(
                    routes
                )

        if not selected:
            return 0
        batch = [task for task, _ in selected]
        manifest.claim(batch)
        selected_counts: dict[str, int] = {}
        for task, provider in selected:
            buffered_tasks.setdefault(provider, deque()).append(task)
            selected_counts[provider] = selected_counts.get(provider, 0) + 1
            provider_endpoint_selection_order += 1
            provider_endpoint_last_selected[(provider, task.endpoint)] = (
                provider_endpoint_selection_order
            )
        submit_buffered(executor)
        for provider, count in selected_counts.items():
            restored = reservations.get(provider, 0) + count
            provider_refill_thresholds[provider] = refill_trigger_for_restored(
                provider, restored
            )
        scheduler_progress.total = int(scheduler_progress.total or 0) + len(batch)
        scheduler_progress.set_postfix(
            queued=len(futures) + buffered_task_count(),
            buffered=buffered_task_count(),
            provider_refill=len(batch),
            refresh=False,
        )
        return len(batch)

    try:
        with ProviderExecutorPool(worker_count, executor_lane_limit) as executor:
            refill(executor)
            publish_scheduler_state(force=True)
            try:
                while True:
                    retained_completed = sum(1 for future in futures if future.done())
                    if retained_completed <= completion_backpressure_limit:
                        submit_buffered(executor)
                    if not futures:
                        # Claimed buffered work is always preferred to another
                        # manifest scan. With no live futures every provider
                        # has an execution slot, so this is only a defensive
                        # guard for a zero global-capacity configuration.
                        if buffered_task_count() > 0:
                            continue
                        if max_tasks is not None and attempted >= max_tasks:
                            break
                        if manifest.pending_count(max_total_attempts, plan_token) <= 0:
                            break
                        if refill(executor) > 0:
                            continue
                        provider_delay = next_cooldown_delay()
                        (
                            retry_deferred_total,
                            next_task_retry_at,
                            task_retry_delay,
                        ) = task_retry_wait_state()
                        wait_candidates = [
                            (reason, delay)
                            for reason, delay in (
                                ("provider cooldown", provider_delay),
                                ("task retry backoff", task_retry_delay),
                            )
                            if delay is not None
                        ]
                        if not wait_candidates:
                            break
                        wait_reason, delay = min(
                            wait_candidates, key=lambda item: float(item[1])
                        )
                        # Keep the observable heartbeat fresh while sleeping,
                        # but do not rescan the multi-million-row manifest at
                        # every 30-second heartbeat. The executor is the sole
                        # owner of retry deadlines and provider cooldowns.
                        # Wall-clock deadlines are converted to a monotonic
                        # sleep. Wake just after, not exactly on, the boundary:
                        # an early wake by even a millisecond makes SQLite find
                        # no eligible row and can defer that task until the next
                        # unrelated retry deadline after an expensive empty scan.
                        wait_deadline = time.monotonic() + max(0.05, delay + 0.1)
                        while True:
                            remaining = max(0.0, wait_deadline - time.monotonic())
                            if remaining <= 0:
                                break
                            scheduler_progress.set_postfix(
                                status=wait_reason,
                                waiting_s=max(0, int(remaining)),
                                refresh=False,
                            )
                            scheduler_progress.refresh()
                            publish_scheduler_state(
                                phase="waiting",
                                wait_reason=wait_reason.replace(" ", "_"),
                                wait_delay=remaining,
                                retry_state=(
                                    retry_deferred_total,
                                    next_task_retry_at,
                                ),
                                force=True,
                            )
                            time.sleep(min(30.0, remaining))
                        continue
                    completed, _ = wait(
                        tuple(futures), timeout=1.0, return_when=FIRST_COMPLETED
                    )
                    if not completed:
                        oldest = min(
                            futures,
                            key=lambda item: submitted_at.get(item, float("inf")),
                        )
                        oldest_task = futures[oldest]
                        oldest_seconds = max(
                            0,
                            int(time.monotonic() - submitted_at[oldest]),
                        )
                        scheduler_progress.set_postfix(
                            inflight=len(futures),
                            workers=worker_count,
                            oldest_age_s=oldest_seconds,
                            endpoint=oldest_task.endpoint[:24],
                            scope=oldest_task.scope_key[:24],
                            refresh=False,
                        )
                        scheduler_progress.refresh()
                        publish_scheduler_state()
                        continue
                    completed_backlog_before = len(completed)
                    completed_batch = sorted(
                        completed,
                        key=lambda item: submitted_at.get(item, float("inf")),
                    )[:completion_persistence_batch_size]
                    completion_results: list[tuple[DownloadTask, TaskResult]] = []
                    discovered_followups: list[DownloadTask] = []
                    for future in completed_batch:
                        task = futures.pop(future)
                        submitted_at.pop(future, None)
                        scheduled_provider.pop(future, None)
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = TaskResult(
                                task=task,
                                status="failed",
                                provider=None,
                                rows=0,
                                output_path=None,
                                attempts=1,
                                error=(f"worker crash: {type(exc).__name__}: {exc}"),
                                provider_outcomes=dict(task.provider_outcomes),
                                provider_evidence=dict(task.provider_evidence),
                            )
                        followups = result.followups
                        if followups:
                            discovered_followups.extend(followups)
                            totals["discovered"] += len(followups)
                        # ``execute_and_discover`` clears source records before
                        # publishing the Future. Keep this defensive clear for
                        # custom/test result implementations.
                        result.records.clear()
                        completion_results.append((task, result))

                    # If result retention is still below the explicit bound,
                    # replace all reaped producer slots before any SQLite or
                    # discovery transaction.  Above the bound, deliberately
                    # stop producer admission until enough completed results
                    # have been persisted; otherwise the executor queue grows
                    # in memory and every provider eventually loses capacity.
                    if completed_backlog_before <= completion_backpressure_limit:
                        submit_buffered(executor)
                        precommit_reservations = queued_reservation_counts()
                        if any(
                            precommit_reservations.get(provider, 0)
                            <= effective_refill_threshold(provider)
                            for provider in provider_refill_thresholds
                        ):
                            refill_depleted_provider_buffers(
                                executor,
                                reserved_completion_count=len(completion_results),
                            )

                    # Publish every catalog continuation before its parent page
                    # becomes visible as success. Large SEC/FRED/Congress
                    # catalogs can expand one completion batch into hundreds
                    # of thousands of tasks. Persist them in bounded chunks
                    # and resupply every depleted provider between chunks so a
                    # long SQLite write cannot couple unrelated API lanes.
                    # Every child still becomes durable before the parent
                    # completion transaction, preserving crash ordering.
                    if discovered_followups:
                        followup_progress = tqdm(
                            total=len(discovered_followups),
                            desc="openbb:persist followups",
                            unit="task",
                            position=2,
                            leave=False,
                            disable=no_progress,
                        )
                        try:
                            for offset in range(
                                0,
                                len(discovered_followups),
                                FOLLOWUP_UPSERT_CHUNK_SIZE,
                            ):
                                followup_chunk = discovered_followups[
                                    offset : offset + FOLLOWUP_UPSERT_CHUNK_SIZE
                                ]
                                manifest.upsert_tasks(
                                    followup_chunk,
                                    plan_token=plan_token,
                                    task_source="followup",
                                )
                                followup_progress.update(len(followup_chunk))
                                retained_during_upsert = sum(
                                    1 for future in futures if future.done()
                                )
                                if (
                                    retained_during_upsert
                                    <= completion_backpressure_limit
                                ):
                                    submit_buffered(executor)
                                    upsert_reservations = queued_reservation_counts()
                                    if any(
                                        upsert_reservations.get(provider, 0)
                                        <= effective_refill_threshold(provider)
                                        for provider in provider_refill_thresholds
                                    ):
                                        refill_depleted_provider_buffers(
                                            executor,
                                            reserved_completion_count=len(
                                                completion_results
                                            ),
                                        )
                                publish_scheduler_state()
                        finally:
                            followup_progress.close()
                    with manifest.completion_batch():
                        for _, result in completion_results:
                            manifest.complete(result)

                    for task, result in completion_results:
                        bulk_unavailable = 0
                        if (
                            task.task_id in entitlement_probe_task_ids
                            and result.status == "unavailable"
                            and result.provider == "fmp"
                            and task.providers == ("fmp",)
                            and _is_scope_specific_auth_failure(
                                "fmp", result.error or ""
                            )
                            and _adaptable_limit_maximum(
                                "fmp", task.endpoint, result.error or ""
                            )
                            is None
                            and not _adaptable_omitted_parameters(
                                "fmp", task.endpoint, result.error or ""
                            )
                        ):
                            domain = _provider_capability_domain("fmp", task.kwargs)
                            if domain is not None:
                                runtime.disable_domain(
                                    "fmp", task.endpoint, domain, result.error or ""
                                )
                                bulk_unavailable += (
                                    manifest.finalize_fully_capability_unavailable(
                                        runtime.unavailable(),
                                        runtime.unavailable_routes(),
                                        runtime.unavailable_domains(),
                                        plan_token=plan_token,
                                        endpoint=task.endpoint,
                                    )
                                )
                        if result.status == "unavailable" and result.provider:
                            unavailable_providers = (
                                runtime.unavailable()
                                if callable(getattr(runtime, "unavailable", None))
                                else {}
                            )
                            unavailable_routes = (
                                runtime.unavailable_routes()
                                if callable(
                                    getattr(runtime, "unavailable_routes", None)
                                )
                                else {}
                            )
                            unavailable_domains = (
                                runtime.unavailable_domains()
                                if callable(
                                    getattr(runtime, "unavailable_domains", None)
                                )
                                else {}
                            )
                            if result.provider in unavailable_providers:
                                bulk_unavailable = (
                                    manifest.finalize_fully_capability_unavailable(
                                        unavailable_providers,
                                        unavailable_routes,
                                        unavailable_domains,
                                        plan_token=plan_token,
                                    )
                                )
                            elif (
                                result.provider,
                                task.endpoint,
                            ) in unavailable_routes:
                                bulk_unavailable = (
                                    manifest.finalize_fully_capability_unavailable(
                                        unavailable_providers,
                                        unavailable_routes,
                                        unavailable_domains,
                                        plan_token=plan_token,
                                        endpoint=task.endpoint,
                                    )
                                )
                            if bulk_unavailable:
                                totals["bulk_unavailable"] += bulk_unavailable
                                download_progress.update(bulk_unavailable)
                        if result.status in {"success", "empty"}:
                            accepted_endpoints.add(task.endpoint)
                        totals[result.status] = totals.get(result.status, 0) + 1
                        attempted += 1
                        scheduler_progress.update(1)
                        download_progress.update(1)
                        status_postfix = {
                            "ok": totals["success"],
                            "empty": totals["empty"],
                            "pending": totals["pending"],
                            "fail": totals["failed"],
                            "bulk_unavailable": totals["bulk_unavailable"],
                            "new": totals["discovered"],
                            "provider": result.provider or "-",
                        }
                        scheduler_progress.set_postfix(
                            active=len(futures),
                            buffered=buffered_task_count(),
                            status=result.status,
                            provider=result.provider or "-",
                            scope=task.scope_key[:24],
                            refresh=False,
                        )
                        download_progress.set_postfix(status_postfix, refresh=False)
                    # Resume producer admission only after the retained-result
                    # hand-off falls back inside its memory bound. SQLite
                    # prefetch runs after this in-memory provider resupply.
                    completed_backlog_after = sum(
                        1 for future in futures if future.done()
                    )
                    completion_backpressure_active = (
                        completed_backlog_after > completion_backpressure_limit
                    )
                    if not completion_backpressure_active:
                        submit_buffered(executor)
                    current_reservations = queued_reservation_counts()
                    provider_needs_refill = not completion_backpressure_active and any(
                        current_reservations.get(provider, 0)
                        <= effective_refill_threshold(provider)
                        for provider in provider_refill_thresholds
                    )
                    queued_count = len(futures) + buffered_task_count()
                    provider_refilled = 0
                    if provider_needs_refill:
                        provider_refilled = refill_depleted_provider_buffers(executor)
                        current_reservations = queued_reservation_counts()
                        provider_needs_refill = any(
                            current_reservations.get(provider, 0)
                            <= effective_refill_threshold(provider)
                            for provider in provider_refill_thresholds
                        )
                        queued_count = len(futures) + buffered_task_count()
                    full_refill_due = (
                        time.monotonic() - last_full_refill_monotonic
                        >= full_fair_refill_interval_seconds
                    )
                    if not completion_backpressure_active and (
                        (queued_count <= refill_threshold and provider_refilled == 0)
                        or full_refill_due
                        or (provider_needs_refill and provider_refilled == 0)
                    ):
                        if attempted - last_gc_attempt >= max(64, worker_count):
                            gc.collect()
                            pa.default_memory_pool().release_unused()
                            last_gc_attempt = attempted
                        refill(executor)
                    publish_scheduler_state()
            except KeyboardInterrupt:
                for future in futures:
                    future.cancel()
                raise
    finally:
        publish_scheduler_state(phase="stopped", force=True)
        scheduler_progress.close()
        download_progress.close()
    finalize_exhausted = getattr(manifest, "finalize_exhausted_pending", None)
    if callable(finalize_exhausted):
        exhausted = int(finalize_exhausted(max_total_attempts, plan_token=plan_token))
        if exhausted:
            totals["exhausted"] = exhausted
    return attempted, totals


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configured_rps: dict[str, float] = dict(DEFAULT_PROVIDER_RPS)
    configured_rps.update(
        {
            key: float(value)
            for key, value in _parse_positive_overrides(args.provider_rps).items()
        }
    )
    # OpenBB's FRED adapter has an internal multi-request helper.  Configure
    # that limiter before importing OpenBB so its child requests obey the same
    # ceiling as the archive's outer task scheduler.
    os.environ["OPENBB_FRED_MIN_INTERVAL"] = str(
        1.0 / max(0.001, configured_rps["fred"])
    )
    start_date = args.start_date.strip()
    end_date = _resolve_end_date(args.end_date)
    _validate_dates(start_date, end_date)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    phase_path = args.output_dir / "_state" / "downloader_phase.json"
    _write_json_atomic(
        phase_path,
        {
            "phase": "bootstrap",
            "stage": "load_openbb_and_planner_fingerprints",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    quarantined_stale_temps = quarantine_stale_atomic_parquet_temps(args.output_dir)
    if quarantined_stale_temps:
        print(
            "[openbb-storage] quarantined_stale_atomic_temps="
            f"{len(quarantined_stale_temps):,}",
            flush=True,
        )
    markets = _parse_csv_set(args.markets) or {"us", "tw"}
    unsupported_markets = markets - {"us", "tw"}
    if unsupported_markets:
        raise ValueError(f"Unsupported --markets values: {sorted(unsupported_markets)}")

    allowed_providers = _parse_csv_set(args.providers)
    disabled_providers = {
        str(item).strip().lower() for item in args.disable_provider if str(item).strip()
    }
    categories = _parse_csv_set(args.categories)
    endpoint_filters = tuple(
        str(item).strip().lstrip(".") for item in args.endpoint if str(item).strip()
    )

    bootstrap_progress = tqdm(
        total=4,
        desc="openbb:bootstrap",
        unit="stage",
        disable=args.no_progress,
    )
    bootstrap_progress.set_postfix(stage="load OpenBB coverage", refresh=False)
    obb, schemas, commands = _load_openbb(args.env_file)
    for command_providers in commands.values():
        for provider in command_providers:
            configured_rps.setdefault(
                str(provider).lower(), DEFAULT_UNDOCUMENTED_PROVIDER_RPS
            )
    bootstrap_progress.update(1)
    bootstrap_progress.set_postfix(stage="read credentials", refresh=False)
    credential_names = configured_credential_names(obb)
    credential_disabled_providers = providers_missing_required_credentials(
        credential_names
    )
    # Official LABSTAT flat files do not require a registration key. Keep BLS
    # selectable when bulk mode is enabled; the API key remains only a fallback
    # credential for an unsupported survey shape.
    if not args.bls_api_only:
        credential_disabled_providers.discard("bls")
    bootstrap_progress.update(1)
    bootstrap_progress.set_postfix(stage="load symbol universes", refresh=False)
    effective_disabled_providers = disabled_providers | credential_disabled_providers
    assets = load_asset_universe(markets, args.limit_symbols)
    etfs = [item for item in assets if item.security_type == "etf"]
    currencies = load_currency_universe(args.limit_symbols)
    indices = ["^DJI", "^GSPC", "^IXIC", "^RUT", "^TWII", "^VIX"]
    if args.limit_symbols is not None:
        indices = indices[: max(0, args.limit_symbols)]
    countries = load_country_codes()
    universe_fingerprint = _universe_fingerprint(
        assets, etfs, currencies, indices, countries
    )
    command_fingerprint = _command_fingerprint(commands, schemas)
    bootstrap_progress.update(1)
    bootstrap_progress.set_postfix(stage="build planner context", refresh=False)

    context = PlannerContext(
        schemas=schemas,
        commands=commands,
        output_dir=args.output_dir,
        start_date=start_date,
        end_date=end_date,
        assets=assets,
        etfs=etfs,
        currencies=currencies,
        indices=indices,
        countries=countries,
        allowed_providers=allowed_providers,
        disabled_providers=effective_disabled_providers,
        endpoint_filters=endpoint_filters,
        categories=categories,
        show_progress=not args.no_progress,
    )
    contract_auditor = PlanContractAuditor(
        obb,
        start_date=start_date,
        end_date=end_date,
        excluded_literal_values=("crypto",),
    )
    candidate_plan_token = _plan_token(
        start_date=start_date,
        end_date=end_date,
        markets=markets,
        categories=categories,
        endpoint_filters=endpoint_filters,
        allowed_providers=allowed_providers,
        disabled_providers=disabled_providers,
        limit_symbols=args.limit_symbols,
        universe_fingerprint=universe_fingerprint,
        command_fingerprint=command_fingerprint,
    )
    plan_scope_fingerprint = _plan_scope_fingerprint(
        start_date=start_date,
        end_date=end_date,
        markets=markets,
        categories=categories,
        endpoint_filters=endpoint_filters,
        allowed_providers=allowed_providers,
        disabled_providers=disabled_providers,
        limit_symbols=args.limit_symbols,
    )
    bootstrap_progress.update(1)
    bootstrap_progress.close()

    manifest = Manifest(
        args.output_dir / "_state" / "openbb_archive.sqlite3",
        show_progress=not args.no_progress,
    )
    try:
        selected_plan = (
            _select_resumable_plan(
                args.output_dir,
                manifest,
                candidate_plan_token=candidate_plan_token,
                plan_scope_fingerprint=plan_scope_fingerprint,
                command_fingerprint=command_fingerprint,
                start_date=start_date,
                end_date=end_date,
                credential_names=credential_names,
            )
            if args.resume_existing_plan
            else None
        )
        if selected_plan is None:
            plan_token = candidate_plan_token
            resumed_plan = None
            resume_mode = "regenerated"
        else:
            plan_token, resumed_plan, resume_mode = selected_plan
        contract_rows = []
        if resumed_plan is None:
            plan_generation = datetime.now(timezone.utc).isoformat()
            task_count, coverage = populate_initial_plan(
                context,
                manifest,
                plan_token=plan_token,
                plan_generation=plan_generation,
                show_progress=not args.no_progress,
                contract_auditor=contract_auditor,
            )
            contract_rows, contract_summary = contract_auditor.finalize(coverage)
        else:
            task_count, coverage, contract_summary = resumed_plan
        _write_json_atomic(
            phase_path,
            {
                "phase": "manifest_maintenance",
                "plan_token": plan_token,
                "initial_tasks": task_count,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        maintenance_version_key = f"resume_maintenance_version:{plan_token}"
        maintenance_current = resumed_plan is not None and manifest.meta_value(
            maintenance_version_key
        ) == str(RESUME_MAINTENANCE_VERSION)
        maintenance_progress = tqdm(
            total=17,
            desc="openbb:manifest maintenance",
            unit="stage",
            disable=args.no_progress,
        )
        if resumed_plan is None:
            maintenance_progress.set_postfix(stage="reconcile plan", refresh=False)
            deactivated_tasks = manifest.reconcile_initial_plan(
                plan_token,
                plan_generation,
                show_progress=not args.no_progress,
            )
            manifest.set_meta_value(
                f"planner_state_version:{plan_token}",
                str(PLANNER_STATE_VERSION),
            )
        else:
            maintenance_progress.set_postfix(
                stage="verified existing plan", refresh=False
            )
            deactivated_tasks = 0
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="reconcile active plan membership", refresh=False
        )
        scope_key = f"plan_scope_fingerprint:{plan_token}"
        has_foreign_active_tasks = manifest.has_active_tasks_outside_plan(plan_token)
        _write_json_atomic(
            phase_path,
            {
                "phase": "manifest_maintenance",
                "stage": "reconcile_active_plan_membership",
                "plan_token": plan_token,
                "foreign_active_tasks_present": has_foreign_active_tasks,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        if maintenance_current and not has_foreign_active_tasks:
            # This plan already owns the fully reconciled active membership.
            # Avoid a redundant DISTINCT scan and multi-million-row UPDATE on
            # every supervisor recycle.
            migrated_plan_followups = 0
            retired_other_plan_tasks = 0
        else:
            compatible_plan_tokens = {
                str(row[0])
                for row in manifest.connection.execute(
                    """
                    SELECT DISTINCT plan_token FROM tasks
                    WHERE active=1 AND plan_token!=?
                    """,
                    (plan_token,),
                ).fetchall()
                if manifest.meta_value(f"plan_scope_fingerprint:{row[0]}")
                == plan_scope_fingerprint
            }
            # Planner state predating scope fingerprints exists only for the
            # default full archive entrypoint.  Adopt its catalog follow-ups once
            # when the current run is that same pinned default scope; future
            # transitions use the explicit fingerprint above.
            legacy_scope_compatible = (
                markets == {"us", "tw"}
                and categories is None
                and not endpoint_filters
                and allowed_providers is None
                and not disabled_providers
                and args.limit_symbols is None
            )
            if legacy_scope_compatible:
                compatible_plan_tokens.update(
                    str(row[0])
                    for row in manifest.connection.execute(
                        """
                        SELECT DISTINCT plan_token FROM tasks
                        WHERE active=1 AND plan_token!=?
                        """,
                        (plan_token,),
                    ).fetchall()
                    if manifest.meta_value(f"plan_scope_fingerprint:{row[0]}") is None
                )
            followup_endpoints = {
                item.endpoint
                for item in coverage
                if item.decision in {"included", "deferred"}
            }
            migrated_plan_followups, retired_other_plan_tasks = (
                manifest.reconcile_active_plan_membership(
                    plan_token,
                    compatible_plan_tokens=compatible_plan_tokens,
                    followup_endpoints=followup_endpoints,
                    show_progress=not args.no_progress,
                )
            )
        manifest.set_meta_value(scope_key, plan_scope_fingerprint)
        manifest.set_meta_value(
            f"plan_command_fingerprint:{plan_token}", command_fingerprint
        )
        if resume_mode != "compatible_scope":
            manifest.set_meta_value(
                f"plan_universe_fingerprint:{plan_token}", universe_fingerprint
            )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="retire legacy CFTC", refresh=False)
        deactivated_legacy_cftc = (
            0
            if maintenance_current
            else manifest.deactivate_legacy_cftc_followups(
                plan_token,
                show_progress=not args.no_progress,
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="prune providers", refresh=False)
        if maintenance_current:
            provider_updates, provider_deactivated = 0, 0
        else:
            provider_updates, provider_deactivated = manifest.prune_disabled_providers(
                plan_token,
                effective_disabled_providers,
                show_progress=not args.no_progress,
            )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="repair FRED pagination", refresh=False)
        fred_release_continuations = (
            0
            if maintenance_current
            else manifest.ensure_fred_release_continuations(
                context,
                plan_token,
                show_progress=not args.no_progress,
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="repair FMP pagination", refresh=False)
        fmp_page_continuations = (
            0
            if maintenance_current
            else manifest.ensure_fmp_page_continuations(
                context,
                plan_token,
                show_progress=not args.no_progress,
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair FRED series followups", refresh=False
        )
        fred_series_followups = (
            0
            if maintenance_current
            else manifest.ensure_fred_series_followups(
                context,
                plan_token,
                show_progress=not args.no_progress,
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="write catalogs", refresh=False)
        # Provider pacing policy can improve between resumptions. Rewrite the
        # small atomic runtime catalogs on every run, but retain the persisted
        # universe when compatible resume deliberately ignores live CSV drift.
        write_catalogs(
            context,
            coverage,
            credential_names,
            write_universes=resume_mode != "compatible_scope",
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="write completeness contracts", refresh=False
        )
        if resumed_plan is None:
            write_contract_audit(args.output_dir, contract_rows, contract_summary)
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair SEC filing shards", refresh=False
        )
        repaired_sec_filing_tasks = (
            0
            if maintenance_current
            else manifest.repair_sec_filings_columnar_shard_bug(plan_token=plan_token)
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair SEC filing headers", refresh=False
        )
        repaired_sec_header_tasks = (
            0
            if maintenance_current
            else manifest.repair_sec_filing_headers_index_page_bug(
                plan_token=plan_token
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="repair country filters", refresh=False)
        repaired_country_all_tasks = (
            0
            if maintenance_current
            else manifest.repair_invalid_country_all_filters(plan_token=plan_token)
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair error classifications", refresh=False
        )
        repaired_transient_errors, repaired_entitlement_errors = (
            (0, 0)
            if maintenance_current
            else manifest.repair_provider_error_classification(plan_token=plan_token)
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair BLS nullable titles", refresh=False
        )
        repaired_bls_title_tasks = (
            0
            if maintenance_current
            else manifest.repair_bls_missing_series_title_bug(plan_token=plan_token)
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="finalize provider outcomes", refresh=False
        )
        finalized_provider_outcomes = (
            0
            if maintenance_current
            else manifest.finalize_resolved_provider_outcome_pending(
                plan_token=plan_token
            )
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(
            stage="repair N-PORT containers", refresh=False
        )
        repaired_sec_nport_containers = (
            0
            if maintenance_current
            else manifest.repair_sec_nport_list_container_bug(plan_token=plan_token)
        )
        maintenance_progress.update(1)
        maintenance_progress.set_postfix(stage="prepare resumable run", refresh=False)
        _write_json_atomic(
            phase_path,
            {
                "phase": "manifest_maintenance",
                "stage": "prepare_run",
                "plan_token": plan_token,
                "full_success_shard_audit": not maintenance_current,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        manifest.prepare_run(
            retry_failed=args.retry_failed,
            retry_repair_queue=args.retry_repair_queue,
            retry_permanent=args.retry_permanent,
            retry_empty=args.retry_empty,
            refresh=args.refresh,
            repair_legacy=not maintenance_current,
            verify_successful_shards=not maintenance_current,
            plan_token=plan_token,
            show_progress=not args.no_progress,
        )
        manifest.set_meta_value(
            maintenance_version_key,
            str(RESUME_MAINTENANCE_VERSION),
        )
        maintenance_progress.update(1)
        maintenance_progress.close()
        _write_json_atomic(
            phase_path,
            {
                "phase": "runtime_reconciliation",
                "stage": "provider_state",
                "plan_token": plan_token,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        if resumed_plan is not None:
            print(
                "[openbb-plan] resumed_existing_plan="
                f"{plan_token} resume_mode={resume_mode} "
                f"candidate_plan={candidate_plan_token} "
                f"live_universe_drift_ignored="
                f"{str(resume_mode == 'compatible_scope').lower()} "
                f"active_tasks={manifest.active_task_count(plan_token):,}",
                flush=True,
            )
        _print_plan_summary(context, task_count, coverage)
        print(
            "[openbb-contract] "
            f"passed={contract_summary['passed']} "
            f"rows={contract_summary['contract_rows']:,} "
            f"unresolved={contract_summary['unresolved']:,} "
            f"by_axis={contract_summary['unresolved_by_axis']}",
            flush=True,
        )
        if not contract_summary["passed"]:
            samples = [row for row in contract_rows if row.status == "unresolved"][:25]
            detail = "; ".join(
                f"{row.endpoint}:{row.provider}:{row.axis}:{row.field}"
                for row in samples
            )
            raise RuntimeError(
                "OpenBB completeness contract has unresolved primary/schema "
                f"obligations ({contract_summary['unresolved']}): {detail}"
            )
        if credential_disabled_providers:
            print(
                "[openbb-credentials] unavailable="
                f"{sorted(credential_disabled_providers)}",
                flush=True,
            )
        if deactivated_tasks:
            print(
                f"[openbb-plan] deactivated_obsolete_tasks={deactivated_tasks:,}",
                flush=True,
            )
        if migrated_plan_followups or retired_other_plan_tasks:
            print(
                "[openbb-plan] migrated_compatible_followups="
                f"{migrated_plan_followups:,} "
                f"retired_other_plan_tasks={retired_other_plan_tasks:,}",
                flush=True,
            )
        if deactivated_legacy_cftc:
            print(
                "[openbb-plan] deactivated_legacy_cftc_tasks="
                f"{deactivated_legacy_cftc:,}",
                flush=True,
            )
        if repaired_sec_filing_tasks:
            print(
                "[openbb-manifest] requeued_truncated_sec_filing_tasks="
                f"{repaired_sec_filing_tasks:,}",
                flush=True,
            )
        if repaired_sec_header_tasks:
            print(
                "[openbb-manifest] requeued_false_empty_sec_filing_headers="
                f"{repaired_sec_header_tasks:,}",
                flush=True,
            )
        if repaired_country_all_tasks:
            print(
                "[openbb-manifest] requeued_invalid_country_all_filters="
                f"{repaired_country_all_tasks:,}",
                flush=True,
            )
        if repaired_transient_errors or repaired_entitlement_errors:
            print(
                "[openbb-manifest] repaired_provider_error_classification="
                f"transient:{repaired_transient_errors:,},"
                f"entitlement:{repaired_entitlement_errors:,}",
                flush=True,
            )
        if repaired_bls_title_tasks:
            print(
                "[openbb-manifest] requeued_bls_nullable_title_tasks="
                f"{repaired_bls_title_tasks:,}",
                flush=True,
            )
        if finalized_provider_outcomes:
            print(
                "[openbb-manifest] finalized_complete_provider_outcomes="
                f"{finalized_provider_outcomes:,}",
                flush=True,
            )
        if repaired_sec_nport_containers:
            print(
                "[openbb-manifest] requeued_sec_nport_list_containers="
                f"{repaired_sec_nport_containers:,}",
                flush=True,
            )
        if provider_updates:
            print(
                "[openbb-plan] pruned_disabled_provider_tasks="
                f"{provider_updates:,} deactivated={provider_deactivated:,}",
                flush=True,
            )
        if fred_release_continuations:
            print(
                "[openbb-plan] ensured_fred_release_continuations="
                f"{fred_release_continuations:,}",
                flush=True,
            )
        if fmp_page_continuations:
            print(
                "[openbb-plan] ensured_fmp_page_continuations="
                f"{fmp_page_continuations:,}",
                flush=True,
            )
        if fred_series_followups:
            print(
                "[openbb-plan] ensured_fred_series_followups="
                f"{fred_series_followups:,}",
                flush=True,
            )
        print(
            f"[openbb-manifest] plan={plan_token} counts={manifest.counts(plan_token)}",
            flush=True,
        )
        if args.plan_only:
            return 0

        rps = configured_rps
        concurrency: dict[str, int] = {
            provider: DEFAULT_PROVIDER_CONCURRENCY.get(
                provider,
                max(
                    1,
                    min(
                        8,
                        math.ceil(
                            float(rps.get(provider, DEFAULT_UNDOCUMENTED_PROVIDER_RPS))
                        ),
                    ),
                ),
            )
            for provider in set(rps)
            | {item for values in commands.values() for item in values}
        }
        concurrency_overrides = {
            key: int(value)
            for key, value in _parse_positive_overrides(
                args.provider_concurrency, integer=True
            ).items()
        }
        concurrency.update(concurrency_overrides)
        if not args.bls_api_only and "bls" not in concurrency_overrides:
            # LABSTAT query connections are deliberately single-threaded. Use
            # at most one task lane per logical CPU; extra lanes only increase
            # scheduler and storage contention. An explicit operator override
            # remains authoritative for hardware-specific measurements.
            concurrency["bls"] = min(
                int(concurrency.get("bls", 1)), max(1, os.cpu_count() or 1)
            )
            print(
                "[openbb-bulk] bls_task_concurrency="
                f"{concurrency['bls']} parallel_builds="
                f"{BLS_LABSTAT_PARALLEL_BUILDS} threads_per_build="
                f"{_bls_labstat_build_threads()}",
                flush=True,
            )
        runtime_state_path = args.output_dir / "_state" / "provider_cooldowns.json"
        if args.refresh:
            runtime_state_path.unlink(missing_ok=True)
        runtime = ProviderRuntime(
            rps,
            concurrency,
            args.quota_cooldown,
            runtime_state_path,
        )
        cleared_false_global_unavailable = runtime.clear_false_global_unavailable()
        if cleared_false_global_unavailable:
            print(
                "[openbb-provider] cleared_false_global_unavailable="
                f"{list(cleared_false_global_unavailable)}",
                flush=True,
            )
        cleared_limit_domains = runtime.clear_adaptable_limit_unavailable_domains()
        repaired_adaptable_limits = manifest.repair_adaptable_parameter_constraints(
            runtime.parameter_maximums(),
            plan_token=plan_token,
        )
        if cleared_limit_domains or repaired_adaptable_limits:
            print(
                "[openbb-provider] normalized_parameter_limits="
                f"domains:{len(cleared_limit_domains):,},"
                f"tasks:{repaired_adaptable_limits:,}",
                flush=True,
            )
        cleared_query_shape_domains = (
            runtime.clear_adaptable_query_shape_unavailable_domains()
        )
        repaired_query_shapes = manifest.repair_adaptable_query_shapes(
            runtime.omitted_parameters(),
            plan_token=plan_token,
        )
        if cleared_query_shape_domains or repaired_query_shapes:
            print(
                "[openbb-provider] normalized_query_shapes="
                f"domains:{len(cleared_query_shape_domains):,},"
                f"tasks:{repaired_query_shapes:,}",
                flush=True,
            )
        legacy_cooldowns = runtime.legacy_cooldown_providers()
        cleared_legacy_dns_cooldowns = (
            runtime.clear_legacy_cooldowns(
                manifest.dns_error_providers(plan_token) & legacy_cooldowns
            )
            if legacy_cooldowns
            else ()
        )
        if cleared_legacy_dns_cooldowns:
            print(
                "[openbb-provider] cleared_legacy_dns_cooldowns="
                f"{list(cleared_legacy_dns_cooldowns)}",
                flush=True,
            )
        restored_cooldowns = runtime.cooldown_deadlines()
        if restored_cooldowns:
            print(
                f"[openbb-provider] restored_cooldowns={restored_cooldowns}",
                flush=True,
            )
        repaired_unproven_outcomes = manifest.repair_unproven_provider_outcomes(
            runtime.unavailable(),
            runtime.unavailable_routes(),
            runtime.unavailable_domains(),
            plan_token=plan_token,
        )
        if repaired_unproven_outcomes:
            print(
                "[openbb-provider] requeued_unproven_unavailable_outcomes="
                f"{repaired_unproven_outcomes:,}",
                flush=True,
            )
        repaired_unproven_permanent = manifest.repair_unproven_permanent_outcomes(
            plan_token=plan_token
        )
        if repaired_unproven_permanent:
            print(
                "[openbb-provider] requeued_unproven_permanent_outcomes="
                f"{repaired_unproven_permanent:,}",
                flush=True,
            )
        repaired_parser_shapes = manifest.repair_provider_parser_shape_permanents(
            plan_token=plan_token
        )
        if repaired_parser_shapes:
            print(
                "[openbb-provider] requeued_provider_parser_shapes="
                f"{repaired_parser_shapes:,}",
                flush=True,
            )
        repaired_transient_outcomes = (
            manifest.repair_provider_transient_permanent_outcomes(plan_token=plan_token)
        )
        if repaired_transient_outcomes:
            print(
                "[openbb-provider] requeued_transient_provider_outcomes="
                f"{repaired_transient_outcomes:,}",
                flush=True,
            )
        repaired_fmp_eps, repaired_fmp_peers = (
            manifest.repair_fmp_adapter_boundary_failures(plan_token=plan_token)
        )
        if repaired_fmp_eps or repaired_fmp_peers:
            print(
                "[openbb-provider] requeued_fmp_adapter_boundaries="
                f"historical_eps:{repaired_fmp_eps:,},"
                f"equity_peers:{repaired_fmp_peers:,}",
                flush=True,
            )
        repaired_sec_statement_validation = (
            manifest.repair_sec_statement_validation_permanents(plan_token=plan_token)
        )
        if repaired_sec_statement_validation:
            print(
                "[openbb-provider] requeued_sec_statement_validation="
                f"{repaired_sec_statement_validation:,}",
                flush=True,
            )
        repaired_sec_statement_wrappers = manifest.repair_sec_statement_wrapper_shards(
            plan_token=plan_token
        )
        if repaired_sec_statement_wrappers:
            print(
                "[openbb-provider] requeued_sec_statement_wrapper_shards="
                f"{repaired_sec_statement_wrappers:,}",
                flush=True,
            )
        repaired_union_schemas = manifest.repair_heterogeneous_parquet_schema_shards(
            plan_token=plan_token
        )
        if repaired_union_schemas:
            print(
                "[openbb-provider] requeued_heterogeneous_parquet_schemas="
                f"{repaired_union_schemas:,}",
                flush=True,
            )
        restored_bulk_unavailable = manifest.finalize_fully_capability_unavailable(
            runtime.unavailable(),
            runtime.unavailable_routes(),
            runtime.unavailable_domains(),
            plan_token=plan_token,
        )
        if restored_bulk_unavailable:
            print(
                "[openbb-provider] restored_bulk_unavailable="
                f"{restored_bulk_unavailable:,}",
                flush=True,
            )
        persisted_tiingo_tw_routes = {
            endpoint
            for provider, endpoint, domain in runtime.unavailable_domains()
            if provider == "tiingo" and domain == "tw"
        }
        inferred_tiingo_tw_routes = (
            {}
            if persisted_tiingo_tw_routes
            else manifest.empty_only_provider_domain_routes(
                "tiingo",
                "tw",
                plan_token=plan_token,
                minimum_distinct_scopes=25,
            )
        )
        inferred_tiingo_tw_unavailable = 0
        for endpoint, empty_scopes in inferred_tiingo_tw_routes.items():
            if ("tiingo", endpoint, "tw") in runtime.unavailable_domains():
                continue
            reason = (
                f"observed {empty_scopes} distinct TW scopes return ticker-empty "
                "and zero successful TW scopes"
            )
            runtime.disable_domain("tiingo", endpoint, "tw", reason)
            inferred_tiingo_tw_unavailable += (
                manifest.finalize_fully_capability_unavailable(
                    runtime.unavailable(),
                    runtime.unavailable_routes(),
                    runtime.unavailable_domains(),
                    plan_token=plan_token,
                    endpoint=endpoint,
                )
            )
        if inferred_tiingo_tw_routes:
            print(
                "[openbb-provider] inferred_tiingo_tw_unavailable_routes="
                f"{dict(sorted(inferred_tiingo_tw_routes.items()))} "
                f"fully_disabled_tasks={inferred_tiingo_tw_unavailable:,}",
                flush=True,
            )
        if maintenance_current:
            reconciled_shards, quarantined_shards = 0, 0
            print(
                "[openbb-quarantine] full_filesystem_reconciliation="
                "deferred_verified_resume",
                flush=True,
            )
        else:
            _write_json_atomic(
                phase_path,
                {
                    "phase": "runtime_reconciliation",
                    "stage": "parquet_ownership_audit",
                    "plan_token": plan_token,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            reconciled_shards, quarantined_shards = (
                manifest.quarantine_terminal_output_shards(plan_token=plan_token)
            )
            print(
                "[openbb-quarantine] reconciled_active_shards="
                f"{reconciled_shards:,} quarantined={quarantined_shards:,}",
                flush=True,
            )
        entitlement_probe_task_ids = manifest.prioritize_fmp_entitlement_probes(
            plan_token,
            end_date,
        )
        if entitlement_probe_task_ids:
            print(
                "[openbb-provider] prioritized_fmp_entitlement_probes="
                f"{len(entitlement_probe_task_ids):,}",
                flush=True,
            )
        sec_statement_process_pool = _create_sec_statement_process_pool()
        print(
            "[openbb-runtime] local_cpu_slots="
            f"{_LOCAL_CPU_BUDGET.total} sec_statement_process_workers="
            f"{_sec_statement_process_worker_count()}",
            flush=True,
        )
        worker = OpenBBWorker(
            obb,
            runtime,
            max_retries=args.max_retries,
            base_backoff=args.base_backoff,
            max_backoff=args.max_backoff,
            metadata_only=True,
            show_progress=not args.no_progress,
            bls_labstat_cache_dir=(
                None
                if args.bls_api_only
                else (
                    args.output_dir
                    / "_state"
                    / "raw_cache"
                    / "bls_labstat"
                    / f"as_of={end_date}"
                )
            ),
            sec_companyfacts_cache_dir=(
                args.output_dir
                / "_state"
                / "raw_cache"
                / "sec_companyfacts"
                / f"as_of={end_date}"
            ),
            sec_statement_process_pool=sec_statement_process_pool,
            sec_insider_cache_dir=(
                args.output_dir / "_state" / "raw_cache" / "sec_insider_transactions"
            ),
            request_checkpoint_dir=(args.output_dir / "_state" / "request_checkpoints"),
            cache_capable_endpoints={
                str(route).lstrip(".")
                for route, schema in schemas.items()
                if "use_cache"
                in getattr(
                    schema.get("input") if isinstance(schema, Mapping) else None,
                    "model_fields",
                    {},
                )
            },
        )

        # Python's default 5 ms GIL switch interval lets CPU-heavy OpenBB XML,
        # JSON and pandas transforms delay a woken rate-dispatcher thread by
        # multiple request intervals. A local 10 req/s limiter remains exact
        # without load, but a controlled CPU/GIL probe measured only 1.14 req/s
        # at 5 ms versus 5.54 req/s at 1 ms. A more aggressive 0.25 ms reached
        # 9.71 req/s in the small probe but caused excessive context-switch
        # overhead in the old 300+ thread OpenBB workload, delaying manifest
        # persistence and eventually draining provider prefetch queues. After
        # bounded provider lanes reduced the live process to about 190 threads,
        # an isolated alternating probe measured 9.91 req/s at 0.5 ms versus
        # 9.32 at 1 ms. The full archive workload nevertheless regressed across
        # SEC, Yahoo, FRED and Congress at 0.5 ms while CPU cost rose and the
        # scheduler checkpoint became stale. Keep the live-validated 1 ms
        # balance for this I/O archive process; every market/provider shares it.
        thread_switch_interval = float(
            os.environ.get(
                "OPENBB_THREAD_SWITCH_INTERVAL_SECONDS",
                DEFAULT_ARCHIVE_THREAD_SWITCH_INTERVAL_SECONDS,
            )
        )
        if thread_switch_interval <= 0:
            raise ValueError(
                "OPENBB_THREAD_SWITCH_INTERVAL_SECONDS must be greater than zero"
            )
        sys.setswitchinterval(thread_switch_interval)
        print(
            "[openbb-runtime] thread_switch_interval_seconds="
            f"{sys.getswitchinterval():.6f}",
            flush=True,
        )

        _write_json_atomic(
            phase_path,
            {
                "phase": "download",
                "plan_token": plan_token,
                "initial_tasks": task_count,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        try:
            attempted, totals = execute_download_tasks(
                context,
                manifest,
                worker,
                plan_token=plan_token,
                workers=args.workers,
                batch_size=args.batch_size,
                max_tasks=args.max_tasks,
                max_total_attempts=args.max_total_attempts,
                no_discovery=args.no_discovery,
                no_progress=args.no_progress,
                entitlement_probe_task_ids=entitlement_probe_task_ids,
            )
        finally:
            if sec_statement_process_pool is not None:
                sec_statement_process_pool.shutdown(
                    wait=True,
                    cancel_futures=True,
                )

        _write_json_atomic(
            phase_path,
            {
                "phase": "finalizing_round",
                "plan_token": plan_token,
                "attempted_this_run": attempted,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        for provider, reason in runtime.unavailable().items():
            manifest.record_provider_event(provider, "disabled_for_run", reason)
        for (provider, endpoint), reason in runtime.unavailable_routes().items():
            manifest.record_provider_event(
                provider, "route_disabled_for_run", f"{endpoint}: {reason}"
            )
        for (
            provider,
            endpoint,
            domain,
        ), reason in runtime.unavailable_domains().items():
            manifest.record_provider_event(
                provider,
                "domain_disabled_for_run",
                f"{endpoint}/{domain}: {reason}",
            )
        final_counts = manifest.counts(plan_token)
        summary = {
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "start_date": start_date,
            "end_date": end_date,
            "plan_token": plan_token,
            "attempted_this_run": attempted,
            "run_totals": totals,
            "manifest_counts": final_counts,
            "disabled_providers": runtime.unavailable(),
            "disabled_provider_routes": {
                f"{provider}:{endpoint}": reason
                for (provider, endpoint), reason in runtime.unavailable_routes().items()
            },
            "disabled_provider_domains": {
                f"{provider}:{endpoint}:{domain}": reason
                for (provider, endpoint, domain), reason in (
                    runtime.unavailable_domains().items()
                )
            },
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        summary_path = args.output_dir / "_state" / "last_run_summary.json"
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(
            f"[openbb-done] attempted={attempted} totals={totals} manifest={final_counts}",
            flush=True,
        )
        _write_json_atomic(
            phase_path,
            {
                "phase": "round_complete",
                "plan_token": plan_token,
                "attempted_this_run": attempted,
                "manifest_counts": final_counts,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return (
            0 if totals.get("failed", 0) == 0 and totals.get("exhausted", 0) == 0 else 2
        )
    finally:
        manifest.close()


if __name__ == "__main__":
    raise SystemExit(run())
