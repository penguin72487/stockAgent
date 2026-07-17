from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import html
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import threading
import time
from typing import Any
from urllib.parse import parse_qsl, quote, urlparse
import zipfile
import xml.etree.ElementTree as ET

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None

import polars as pl
import pyarrow.parquet as pq
import requests
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

try:
    from downloader.common import (
        SharedRateLimiter,
        describe_rate_limit,
        provider_rate_limit,
        resolve_end_date,
        resolve_request_interval,
        run_parallel_tasks,
    )
except ImportError:  # pragma: no cover - direct script execution from downloader/
    from common import (
        SharedRateLimiter,
        describe_rate_limit,
        provider_rate_limit,
        resolve_end_date,
        resolve_request_interval,
        run_parallel_tasks,
    )


DATA_GOV_DATASET_API = "https://data.gov.tw/api/v2/rest/dataset/{dataset_id}"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 stockAgent/1.0"
)
DATE_COLUMN = "date"
ROC_DATE_PATTERN = re.compile(r"^\d{2,3}/\d{1,2}/\d{1,2}$")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
HTML_ROW_START_PATTERN = re.compile(r"<tr\b[^>]*>", re.IGNORECASE)
HTML_CELL_START_PATTERN = re.compile(r"<td\b[^>]*>", re.IGNORECASE)
HTML_ROW_CAPTURE_PATTERN = re.compile(r"<tr\b(?P<attrs>[^>]*)>", re.IGNORECASE)
HTML_CELL_CAPTURE_PATTERN = re.compile(r"<td\b(?P<attrs>[^>]*)>", re.IGNORECASE)
SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
_HTTP_LOCAL = threading.local()
_RATE_LIMITER: SharedRateLimiter | None = None
_RATE_LIMITER_LOCK = threading.Lock()
_JOURNAL_LOCK = threading.Lock()
_TPEX_SESSION_CACHE_LOCK = threading.Lock()
_TPEX_SESSION_CACHE: dict[
    tuple[str, int, int, int, int],
    tuple[date, date, frozenset[date], str | None, str | None],
] = {}
_TAIEX_SESSION_CACHE_LOCK = threading.Lock()
_TAIEX_SESSION_CACHE: dict[
    tuple[str, int, int, str, int, int, str],
    tuple[date, date, frozenset[date], str],
] = {}


def _http_session() -> requests.Session:
    session = getattr(_HTTP_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _HTTP_LOCAL.session = session
    return session


def _discard_http_session() -> None:
    """Drop this worker's keep-alive connection after an unsafe HTTP 200."""

    session = getattr(_HTTP_LOCAL, "session", None)
    if session is None:
        return
    delattr(_HTTP_LOCAL, "session")
    session.close()


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    name: str
    kind: str
    source: str
    description: str
    tags: tuple[str, ...]
    url: str | None = None
    url_template: str | None = None
    data_gov_id: str | None = None
    date_format: str = "%Y%m%d"
    start_date: str | None = None
    request_params: tuple[tuple[str, str], ...] = ()
    table_mode: str = "all"
    output_mode: str = "merge"


@dataclass(slots=True)
class DownloadResult:
    dataset: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None
    raw_path: str | None = None
    requested_dates: int = 0
    fetched_dates: int = 0
    skipped_dates: int = 0
    empty_dates: int = 0
    failed_dates: int = 0
    missing_dates_before: int = 0
    missing_dates_after: int = 0
    coverage_complete: bool | None = None
    source_unavailable_dates: int = 0


@dataclass(slots=True)
class HistoricalDateResult:
    day: date
    url: str
    frame: pl.DataFrame
    raw_path: str | None = None
    error: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    content_length: int | None = None
    body_sha256: str | None = None
    body_snippet: str | None = None
    response_attempts: int = 0
    source_unavailable_reason: str | None = None


class HistoricalResponseError(RuntimeError):
    """An HTTP-success response that is unsafe to accept as historical data."""


@dataclass(slots=True)
class HistoricalDownloadPlan:
    start: date
    end: date
    dates: list[date]
    all_weekdays: set[date]
    existing_dates: set[date]
    confirmed_empty_dates: set[date]
    suspicious_dates: set[date]
    missing_before: set[date]
    replace_output: bool
    state: dict[str, Any]


@dataclass(slots=True)
class HistoricalResumeCache:
    cache_key: str
    journal_path: Path
    partial_path: Path
    data_dates: set[date]
    empty_dates: set[date]


MODE_ALIASES = {
    "full": "repair",
    "daily-update": "daily",
    "from-zero": "rebuild",
}
DOWNLOAD_MODES = ("rebuild", "repair", "daily", "list", *MODE_ALIASES)
COVERAGE_STATE_SCHEMA_VERSION = 1
HISTORICAL_JOURNAL_SCHEMA_VERSION = 1
# Latest parser contract and the safe default for a newly added historical
# dataset. Bump only the affected entries below when a parser change is scoped
# to one exchange; otherwise unrelated durable journals would stop resuming.
HISTORICAL_PARSER_CONTRACT_VERSION = 11
HISTORICAL_PARSER_CONTRACT_VERSION_BY_DATASET = {
    # v5 accepted holiday responses whose selected-table title had been
    # rewritten to the requested date while the rows were stale. v6 bound both
    # datasets to the verified TAIEX session calendar, but a measured rebuild
    # still found retitled stale payloads on real sessions: 18 daily-OHLCV
    # dates and 7 market-index dates. v7 requires the selected-table title,
    # top-level payload.date, and any supplied params.date to declare the
    # requested date, so old v6 partials are quarantined and only invalid raw
    # receipts are refetched.
    "twse_daily_ohlcv": 7,
    "twse_market_index": 7,
    "twse_margin_balance": 5,
    "twse_institutional_trades": 5,
    "twse_daily_valuation": 5,
    # Point-in-time day-trade membership is a daily exchange rule, not a
    # current-list snapshot.  v1 binds the selected table and top-level date,
    # validates unique symbols, and rejects unknown sell-first suspension
    # markers instead of guessing their meaning. v2 additionally rejects
    # truncated rows and a declared table total that differs from the payload,
    # so a missing sell-first marker or partial member list cannot become an
    # implicit permission/denial downstream.
    "twse_day_trade_eligibility": 2,
    # v6 treated two real TPEx OHLCV HTML generations as the same layout and
    # silently shifted their price/volume columns. v7 separated the layouts,
    # but missed three corporate-action rows whose split direction cell is
    # blank. v8 identifies the generation by its close/status cell. v9 retains
    # every available quote/statistics field and removes historical limit-price
    # glyphs from numeric price cells. v10 accepts only isolated, permanently
    # damaged security names (with row provenance), validates every other cell,
    # and recognizes the official zero-trade average-note sentinel plus the
    # exact legacy styled ROC-date header. v11 losslessly recovers the three
    # corporate-action labels whose surviving CP950 byte pattern is unique and
    # records row-level recovery provenance; every unknown damaged token still
    # fails closed. v12 propagates receipt-level replacement-byte evidence to
    # every security name in that receipt.  CP950 can otherwise re-pair the
    # injected EF BF BD bytes into plausible CJK text that evades a rendered-
    # character check even though the original name is unrecoverable.
    "tpex_daily_ohlcv": 12,
    # v7 records the official 2007-06-01..2008-09-29 backend archive gap
    # only through per-session immutable explicit-no-data receipts. v8 adds
    # the 2004-10-19 onward 16-cell archive generation and its exact standalone
    # styled ROC-date header without weakening response-date binding.
    "tpex_margin_balance": 8,
    "tpex_institutional_trades": 6,
    # v7 recognizes the official labeled ROC date used by the 2004--2006
    # valuation archive (for example ``交易日期:94年08月08日``).
    "tpex_daily_valuation": 7,
    "tpex_day_trade_eligibility": 2,
}
TW_PUBLIC_WAF_COOLDOWN_SECONDS = 30.0
NO_DATA_STATUS_MARKERS = (
    "沒有符合條件",
    "查無資料",
    "無資料",
    "no data",
)
OHLCV_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "twse_daily_ohlcv": ("證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"),
    "tpex_daily_ohlcv": ("代號", "成交股數", "開盤", "最高", "最低", "收盤"),
}
OHLCV_SYMBOL_COLUMNS = {
    "twse_daily_ohlcv": "證券代號",
    "tpex_daily_ohlcv": "代號",
}
TPEX_OFFICIAL_CALENDAR_DATASET = "tpex_daily_ohlcv"
TAIEX_SESSION_CALENDAR_DATASET = "twse_taiex_ohlc"
TAIEX_SESSION_CALENDAR_SUMMARY_SCHEMA_VERSION = 1
TAIEX_SESSION_CALENDAR_REQUIRED_COLUMNS = (
    "date",
    "opening_index",
    "highest_index",
    "lowest_index",
    "closing_index",
    "_dataset",
    "_source",
    "_source_product",
)
TPEX_SESSION_DEPENDENT_DATASETS = frozenset(
    {
        "tpex_margin_balance",
        "tpex_institutional_trades",
        "tpex_daily_valuation",
        "tpex_day_trade_eligibility",
    }
)
DAY_TRADE_ELIGIBILITY_DATASETS = frozenset(
    {
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    }
)
TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES: dict[
    str,
    tuple[tuple[date, date, str], ...],
] = {
    # Live checks against the official endpoint, using the receipt-verified
    # TAIEX calendar, found data through 2007-05-31 and again from 2008-09-30.
    # Every intervening open session returns the same explicit structured
    # no-data response.  These dates are coverage only after their individual
    # immutable response receipts have been journaled and verified.
    "tpex_margin_balance": (
        (
            date(2007, 6, 1),
            date(2008, 9, 29),
            "official_endpoint_archive_gap",
        ),
    ),
}
TPEX_LEGACY_HTML_RESPONSE_KINDS = frozenset(
    {
        "tpex_margin_archive_html",
        "tpex_institutional_archive_html",
        "tpex_valuation_archive_html",
    }
)
HISTORICAL_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "twse_day_trade_eligibility": (
        "證券代號",
        "證券名稱",
    ),
    "tpex_day_trade_eligibility": (
        "證券代號",
        "證券名稱",
    ),
    "tpex_margin_balance": (
        "代號",
        "名稱",
        "前資餘額(張)",
        "資買",
        "資賣",
        "資餘額",
        "前券餘額(張)",
        "券賣",
        "券買",
        "券餘額",
    ),
    "tpex_institutional_trades": (
        "代號",
        "名稱",
        "外資及陸資淨買股數",
        "投信淨買股數",
        "自營淨買股數",
        "三大法人買賣超股數",
    ),
    "tpex_daily_valuation": (
        "股票代號",
        "公司名稱",
        "本益比",
        "殖利率(%)",
        "股價淨值比",
    ),
}
HISTORICAL_SYMBOL_COLUMNS = {
    **OHLCV_SYMBOL_COLUMNS,
    "twse_day_trade_eligibility": "證券代號",
    "tpex_day_trade_eligibility": "證券代號",
    "tpex_margin_balance": "代號",
    "tpex_institutional_trades": "代號",
    "tpex_daily_valuation": "股票代號",
}
TPEX_INSTITUTIONAL_GROUPED_SOURCE_FIELDS = (
    "代號",
    "名稱",
    *(("買進股數", "賣出股數", "買賣超股數") * 7),
    "三大法人買賣超股數合計",
)
TPEX_INSTITUTIONAL_GROUPED_CANONICAL_FIELDS = (
    "代號",
    "名稱",
    "外資及陸資(不含外資自營商)買股數",
    "外資及陸資(不含外資自營商)賣股數",
    "外資及陸資(不含外資自營商)淨買股數",
    "外資自營商買股數",
    "外資自營商賣股數",
    "外資自營商淨買股數",
    "外資及陸資買股數",
    "外資及陸資賣股數",
    "外資及陸資淨買股數",
    "投信買進股數",
    "投信賣出股數",
    "投信淨買股數",
    "自營商(自行買賣)買股數",
    "自營商(自行買賣)賣股數",
    "自營商(自行買賣)淨買股數",
    "自營商(避險)買股數",
    "自營商(避險)賣股數",
    "自營商(避險)淨買股數",
    "自營商買股數",
    "自營商賣股數",
    "自營淨買股數",
    "三大法人買賣超股數",
)


HISTORICAL_DAILY_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="twse_daily_ohlcv",
        kind="historical_json_table",
        source="TWSE",
        description="TWSE listed daily OHLCV from official historical MI_INDEX JSON.",
        tags=("twse", "price", "liquidity", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
            "?date={date}&type=ALLBUT0999&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2004-02-11",
        table_mode="title_contains:每日收盤行情",
    ),
    DatasetSpec(
        name="twse_market_index",
        kind="historical_json_table",
        source="TWSE",
        description="TWSE market index tables from official historical MI_INDEX JSON.",
        tags=("twse", "index", "market", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
            "?date={date}&type=IND&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2009-01-05",
        table_mode="title_contains:指數",
    ),
    DatasetSpec(
        name="twse_margin_balance",
        kind="historical_json_table",
        source="TWSE",
        description="TWSE margin and short balance from official historical MI_MARGN JSON.",
        tags=("twse", "chip", "margin", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
            "?date={date}&selectType=ALL&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2001-01-01",
        table_mode="title_contains:融資融券彙總",
    ),
    DatasetSpec(
        name="twse_institutional_trades",
        kind="historical_json_table",
        source="TWSE",
        description="TWSE three major institutional investors by stock from official T86 JSON.",
        tags=("twse", "chip", "institutional", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/fund/T86"
            "?date={date}&selectType=ALLBUT0999&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2012-05-02",
    ),
    DatasetSpec(
        name="twse_daily_valuation",
        kind="historical_json_table",
        source="TWSE",
        description="TWSE daily dividend yield, PE, and PB by stock.",
        tags=("twse", "fundamental", "valuation", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
            "?date={date}&selectType=ALL&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2005-09-02",
    ),
    DatasetSpec(
        name="twse_day_trade_eligibility",
        kind="historical_json_table",
        source="TWSE",
        description=(
            "TWSE point-in-time cash day-trade eligible securities and "
            "sell-first suspension markers."
        ),
        tags=("twse", "execution", "day-trade", "daily", "historical"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U"
            "?date={date}&selectType=All&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2014-01-06",
        table_mode="title_contains:當日沖銷交易標的及成交量值",
    ),
    DatasetSpec(
        name="tpex_daily_ohlcv",
        kind="historical_json_table",
        source="TPEx",
        description="TPEx mainboard daily OHLCV from official historical JSON.",
        tags=("tpex", "price", "liquidity", "daily", "historical"),
        url_template=(
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
            "?date={date}&type=EW&response=json"
        ),
        date_format="%Y/%m/%d",
        start_date="2003-08-01",
    ),
    DatasetSpec(
        name="tpex_margin_balance",
        kind="historical_json_table",
        source="TPEx",
        description="TPEx margin and short balance from official historical JSON.",
        tags=("tpex", "chip", "margin", "daily", "historical"),
        url_template=(
            "https://www.tpex.org.tw/www/zh-tw/margin/balance"
            "?date={date}&response=json"
        ),
        date_format="%Y/%m/%d",
        start_date="2003-08-01",
        table_mode="title_contains:上櫃股票融資融券餘額",
    ),
    DatasetSpec(
        name="tpex_institutional_trades",
        kind="historical_json_table",
        source="TPEx",
        description="TPEx three major institutional investors by stock.",
        tags=("tpex", "chip", "institutional", "daily", "historical"),
        url_template=(
            "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"
            "?date={date}&type=Daily&sect=EW&response=json"
        ),
        date_format="%Y/%m/%d",
        start_date="2004-06-01",
        table_mode="title_contains:三大法人買賣明細資訊",
    ),
    DatasetSpec(
        name="tpex_daily_valuation",
        kind="historical_json_table",
        source="TPEx",
        description="TPEx daily dividend yield, PE, and PB by stock.",
        tags=("tpex", "fundamental", "valuation", "daily", "historical"),
        url_template=(
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate"
            "?date={date}&response=json"
        ),
        date_format="%Y/%m/%d",
        start_date="2003-08-01",
    ),
    DatasetSpec(
        name="tpex_day_trade_eligibility",
        kind="historical_json_table",
        source="TPEx",
        description=(
            "TPEx point-in-time cash day-trade eligible securities and "
            "sell-first suspension markers."
        ),
        tags=("tpex", "execution", "day-trade", "daily", "historical"),
        url_template=(
            "https://www.tpex.org.tw/www/zh-tw/intraday/list"
            "?date={date}&code="
        ),
        date_format="%Y/%m/%d",
        start_date="2014-01-06",
        table_mode="title_contains:現股當沖交易標的",
    ),
)


DELISTED_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="twse_delisted_company",
        kind="delisted_history",
        source="TWSE OpenAPI",
        description="TWSE official historical delisted-company records.",
        tags=("twse", "company", "delisted", "universe", "historical"),
        url="https://openapi.twse.com.tw/v1/company/suspendListingCsvAndHtml",
        start_date="2001-01-01",
    ),
    DatasetSpec(
        name="tpex_delisted_company",
        kind="delisted_history",
        source="TPEx",
        description="TPEx official historical delisted-company records.",
        tags=("tpex", "company", "delisted", "universe", "historical"),
        url="https://www.tpex.org.tw/www/zh-tw/company/deListed",
        start_date="1994-01-01",
    ),
)


# First-principles subset for a daily stock model. These are point-in-time
# company/fundamental/ownership/lifecycle/shorting/index inputs. Intraday ranks,
# broker leaderboards, warrants, bonds, gold, funds, and auction-frequency-only
# feeds are intentionally excluded.
TWSE_MODEL_USEFUL_PATHS: tuple[tuple[str, str], ...] = (
    ("company/applylistingForeign", "lifecycle"),
    ("company/applylistingLocal", "lifecycle"),
    ("company/newlisting", "lifecycle"),
    ("opendata/t187ap02_L", "ownership"),
    ("opendata/t187ap05_L", "fundamental"),
    ("opendata/t187ap08_L", "ownership"),
    ("opendata/t187ap09_L", "ownership"),
    ("opendata/t187ap10_L", "ownership"),
    ("opendata/t187ap11_L", "ownership"),
    ("opendata/t187ap12_L", "ownership"),
    ("opendata/t187ap13_L", "ownership"),
    ("opendata/t187ap22_L", "event"),
    ("opendata/t187ap23_L", "event"),
    ("opendata/t187ap24_L", "event"),
    ("opendata/t187ap25_L", "event"),
    ("opendata/t187ap26_L", "lifecycle"),
    ("opendata/t187ap27_L", "lifecycle"),
    ("opendata/t187ap15_L", "fundamental"),
    ("opendata/t187ap16_L", "fundamental"),
    ("opendata/t187ap17_L", "fundamental"),
    ("opendata/t187ap31_L", "fundamental"),
    ("SBL/TWT96U", "shorting"),
    ("exchangeReport/BFI84U", "shorting"),
    ("exchangeReport/MI_MARGN", "shorting"),
    ("exchangeReport/TWT84U", "market_rule"),
    ("exchangeReport/TWT85U", "lifecycle"),
    ("exchangeReport/TWT88U", "market_rule"),
    ("exchangeReport/TWTAWU", "lifecycle"),
    ("fund/MI_QFIIS_cat", "ownership"),
    ("fund/MI_QFIIS_sort_20", "ownership"),
    ("holidaySchedule/holidaySchedule", "calendar"),
    ("opendata/twtazu_od", "market_state"),
    ("indicesReport/FRMSA", "index"),
    ("indicesReport/MFI94U", "index"),
    ("indicesReport/MI_5MINS_HIST", "index"),
    ("indicesReport/TAI50I", "index"),
    *tuple((f"opendata/t187ap06_L_{suffix}", "fundamental") for suffix in ("basi", "bd", "ci", "fh", "ins", "mim")),
    *tuple((f"opendata/t187ap07_L_{suffix}", "fundamental") for suffix in ("basi", "bd", "ci", "fh", "ins", "mim")),
)

TPEX_MODEL_USEFUL_PATHS: tuple[tuple[str, str], ...] = (
    ("mopsfin_t187ap02_O", "ownership"),
    ("mopsfin_t187ap04_O", "event"),
    ("mopsfin_t187ap05_O", "fundamental"),
    ("mopsfin_t187ap05_OA", "fundamental"),
    ("mopsfin_t187ap05_OB", "fundamental"),
    ("mopsfin_t187ap08_O", "ownership"),
    ("mopsfin_t187ap09_O", "ownership"),
    ("mopsfin_t187ap10_O", "ownership"),
    ("mopsfin_t187ap11_O", "ownership"),
    ("mopsfin_t187ap12_O", "ownership"),
    ("mopsfin_t187ap13_O", "ownership"),
    ("mopsfin_t187ap22_O", "event"),
    ("mopsfin_t187ap23_O", "event"),
    ("mopsfin_t187ap24_O", "event"),
    ("mopsfin_t187ap25_O", "event"),
    ("mopsfin_t187ap26_O", "lifecycle"),
    ("mopsfin_t187ap27_O", "lifecycle"),
    ("mopsfin_t187ap15_O", "fundamental"),
    ("mopsfin_t187ap16_O", "fundamental"),
    ("mopsfin_187ap17_O", "fundamental"),
    ("mopsfin_t187ap31_O", "fundamental"),
    ("tpex_3insti_dealer_trading", "flow"),
    ("tpex_3insti_qfii", "ownership"),
    ("tpex_3insti_qfii_industry", "ownership"),
    ("tpex_3insti_qfii_trading", "flow"),
    ("tpex_3insti_summary", "flow"),
    ("tpex_3insti_trading", "flow"),
    ("tpex_ceil_non_trading", "market_rule"),
    ("tpex_cmode", "lifecycle"),
    ("tpex_esb_applicant_companies", "lifecycle"),
    ("tpex_exright_daily", "corporate_action"),
    ("tpex_exright_prepost", "corporate_action"),
    ("tpex_ipo_no_limit", "market_rule"),
    ("tpex_margin_sbl", "shorting"),
    ("tpex_margin_trading_adjust", "shorting"),
    ("tpex_margin_trading_margin_mark", "shorting"),
    ("tpex_margin_trading_margin_used", "shorting"),
    ("tpex_margin_trading_marginspot", "shorting"),
    ("tpex_margin_trading_short_sell", "shorting"),
    ("tpex_margin_trading_term", "shorting"),
    ("tpex_short_sell", "shorting"),
    ("tpex_spendi_history", "lifecycle"),
    ("tpex_spendi_today", "lifecycle"),
    ("tpex_index", "index"),
    ("tpex_index_consti", "index"),
    ("tpex_reward_index", "index"),
    ("tpex50_index", "index"),
    ("tpex50_constituents", "index"),
    ("tpex200_constituents", "index"),
    ("tphd_index", "index"),
    ("tphd_constituents", "index"),
    *tuple((f"mopsfin_t187ap06_O_{suffix}", "fundamental") for suffix in ("basi", "basiA", "bd", "bdA", "ci", "ciA", "fh", "fhA", "ins", "insA", "mim", "mimA")),
    *tuple((f"mopsfin_t187ap07_O_{suffix}", "fundamental") for suffix in ("basi", "bd", "ci", "fh", "ins", "mim")),
)


def _api_dataset_name(source: str, path: str) -> str:
    return f"{source}_api_{SAFE_NAME_PATTERN.sub('_', path).strip('_').lower()}"


MODEL_USEFUL_SNAPSHOT_DATASETS: tuple[DatasetSpec, ...] = tuple(
    DatasetSpec(
        name=_api_dataset_name("twse", path),
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description=f"Daily point-in-time snapshot of TWSE {group} endpoint {path}.",
        tags=("twse", "model_useful", group, "snapshot"),
        url=f"https://openapi.twse.com.tw/v1/{path}",
    )
    for path, group in TWSE_MODEL_USEFUL_PATHS
) + tuple(
    DatasetSpec(
        name=_api_dataset_name("tpex", path),
        kind="snapshot_url",
        source="TPEx OpenAPI",
        description=f"Daily point-in-time snapshot of TPEx {group} endpoint {path}.",
        tags=("tpex", "model_useful", group, "snapshot"),
        url=f"https://www.tpex.org.tw/openapi/v1/{path}",
    )
    for path, group in TPEX_MODEL_USEFUL_PATHS
)


SNAPSHOT_OPEN_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="twse_listed_company_basic",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="Listed company basic information.",
        tags=("twse", "mops", "universe", "fundamental", "snapshot"),
        url="https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    ),
    DatasetSpec(
        name="twse_listed_dividend",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="Listed company dividend distribution.",
        tags=("twse", "mops", "fundamental", "dividend", "snapshot"),
        url="https://openapi.twse.com.tw/v1/opendata/t187ap45_L",
    ),
    DatasetSpec(
        name="twse_listed_material_info",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="Listed company daily material information.",
        tags=("twse", "mops", "event", "material", "snapshot"),
        url="https://openapi.twse.com.tw/v1/opendata/t187ap04_L",
    ),
    DatasetSpec(
        name="twse_ex_dividend_preview",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="Listed stock ex-right/ex-dividend preview.",
        tags=("twse", "event", "dividend", "snapshot"),
        url="https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL",
    ),
    DatasetSpec(
        name="twse_notice_stock",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="TWSE current announced attention stocks.",
        tags=("twse", "event", "attention", "snapshot"),
        url="https://openapi.twse.com.tw/v1/announcement/notice",
    ),
    DatasetSpec(
        name="twse_disposal_stock",
        kind="snapshot_url",
        source="TWSE OpenAPI",
        description="TWSE current disposal stocks.",
        tags=("twse", "event", "disposal", "snapshot"),
        url="https://openapi.twse.com.tw/v1/announcement/punish",
    ),
    DatasetSpec(
        name="tpex_basic_company",
        kind="snapshot_url",
        source="TPEx OpenAPI",
        description="TPEx mainboard company basic information.",
        tags=("tpex", "mops", "universe", "fundamental", "snapshot"),
        url="https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
    ),
    DatasetSpec(
        name="tpex_dividend",
        kind="snapshot_url",
        source="TPEx OpenAPI",
        description="TPEx dividend distribution approved by board.",
        tags=("tpex", "mops", "fundamental", "dividend", "snapshot"),
        url="https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap39_O",
    ),
    DatasetSpec(
        name="tpex_attention_stock",
        kind="snapshot_url",
        source="TPEx OpenAPI",
        description="TPEx current attention stock information.",
        tags=("tpex", "event", "attention", "snapshot"),
        url="https://www.tpex.org.tw/openapi/v1/tpex_trading_warning_information",
    ),
    DatasetSpec(
        name="tpex_disposal_stock",
        kind="snapshot_url",
        source="TPEx OpenAPI",
        description="TPEx current disposal securities information.",
        tags=("tpex", "event", "disposal", "snapshot"),
        url="https://www.tpex.org.tw/openapi/v1/tpex_disposal_information",
    ),
    DatasetSpec(
        name="taifex_daily_futures",
        kind="snapshot_url",
        source="TAIFEX OpenAPI",
        description="TAIFEX daily futures market report.",
        tags=("taifex", "futures", "regime", "daily", "snapshot"),
        url="https://openapi.taifex.com.tw/v1/DailyMarketReportFut",
    ),
    DatasetSpec(
        name="taifex_daily_options",
        kind="snapshot_url",
        source="TAIFEX OpenAPI",
        description="TAIFEX daily options market report.",
        tags=("taifex", "options", "regime", "daily", "snapshot"),
        url="https://openapi.taifex.com.tw/v1/DailyMarketReportOpt",
    ),
    DatasetSpec(
        name="taifex_institutional_total",
        kind="snapshot_url",
        source="TAIFEX OpenAPI",
        description="TAIFEX three major institutional traders, total table by date.",
        tags=("taifex", "institutional", "regime", "daily", "snapshot"),
        url="https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersGeneralBytheDate",
    ),
    DatasetSpec(
        name="taifex_large_trader_futures_oi",
        kind="snapshot_url",
        source="TAIFEX OpenAPI",
        description="TAIFEX large trader futures open interest.",
        tags=("taifex", "open_interest", "regime", "daily", "snapshot"),
        url="https://openapi.taifex.com.tw/v1/OpenInterestOfLargeTradersFutures",
    ),
    DatasetSpec(
        name="taifex_final_settlement_price",
        kind="snapshot_url",
        source="TAIFEX OpenAPI",
        description="TAIFEX final settlement prices.",
        tags=("taifex", "settlement", "regime", "snapshot"),
        url="https://openapi.taifex.com.tw/v1/FinalSettlementPrice",
    ),
    DatasetSpec(
        name="tdcc_shareholding_distribution",
        kind="snapshot_url",
        source="TDCC OpenAPI",
        description="TDCC shareholding distribution by tier.",
        tags=("tdcc", "ownership", "shareholding", "snapshot"),
        url="https://openapi-t.tdcc.com.tw/v1/opendata/1-5",
    ),
)


DATA_GOV_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="data_gov_tdcc_shareholding_distribution",
        kind="data_gov",
        source="data.gov.tw",
        description="TDCC shareholding distribution metadata-resolved CSV.",
        tags=("tdcc", "ownership", "data_gov", "snapshot"),
        data_gov_id="11452",
    ),
    DatasetSpec(
        name="cbc_usdtwd_closing_rate",
        kind="data_gov",
        source="data.gov.tw",
        description="CBC interbank USD/TWD closing rates.",
        tags=("cbc", "macro", "fx", "daily", "data_gov"),
        data_gov_id="7232",
    ),
    DatasetSpec(
        name="cbc_overnight_rate",
        kind="data_gov",
        source="data.gov.tw",
        description="CBC financial industry overnight call loan rate.",
        tags=("cbc", "macro", "rate", "daily", "data_gov"),
        data_gov_id="6023",
    ),
    DatasetSpec(
        name="cbc_money_aggregates",
        kind="data_gov",
        source="data.gov.tw",
        description="CBC money aggregates.",
        tags=("cbc", "macro", "money", "monthly", "data_gov"),
        data_gov_id="6024",
    ),
    DatasetSpec(
        name="cbc_fx_reserves",
        kind="data_gov",
        source="data.gov.tw",
        description="CBC foreign exchange reserves.",
        tags=("cbc", "macro", "reserves", "monthly", "data_gov"),
        data_gov_id="6025",
    ),
    DatasetSpec(
        name="dgbas_cpi_basic",
        kind="data_gov",
        source="data.gov.tw",
        description="DGBAS CPI basic classification index.",
        tags=("dgbas", "macro", "cpi", "monthly", "data_gov"),
        data_gov_id="6019",
    ),
    DatasetSpec(
        name="dgbas_unemployment_rate",
        kind="data_gov",
        source="data.gov.tw",
        description="DGBAS unemployment rate.",
        tags=("dgbas", "macro", "unemployment", "monthly", "data_gov"),
        data_gov_id="6637",
    ),
    DatasetSpec(
        name="dgbas_gdp_expenditure_sa",
        kind="data_gov",
        source="data.gov.tw",
        description="DGBAS seasonally adjusted GDP by expenditure.",
        tags=("dgbas", "macro", "gdp", "quarterly", "data_gov"),
        data_gov_id="6689",
    ),
    DatasetSpec(
        name="mof_customs_trade",
        kind="data_gov",
        source="data.gov.tw",
        description="MOF customs import/export trade statistics.",
        tags=("mof", "customs", "macro", "trade", "monthly", "data_gov"),
        data_gov_id="6053",
    ),
    DatasetSpec(
        name="mof_tax_revenue",
        kind="data_gov",
        source="data.gov.tw",
        description="MOF net tax revenue by tax item.",
        tags=("mof", "tax", "macro", "monthly", "data_gov"),
        data_gov_id="6671",
    ),
)


DEFAULT_DATASETS: dict[str, DatasetSpec] = {
    spec.name: spec
    for spec in (
        *HISTORICAL_DAILY_DATASETS,
        *DELISTED_DATASETS,
        *MODEL_USEFUL_SNAPSHOT_DATASETS,
        *SNAPSHOT_OPEN_DATASETS,
        *DATA_GOV_DATASETS,
    )
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Taiwan free public datasets from TWSE, TPEx, MOPS, TDCC, TAIFEX, CBC, DGBAS, and MOF."
    )
    parser.add_argument(
        "--mode",
        choices=DOWNLOAD_MODES,
        default="daily",
        help=(
            "rebuild: refetch the requested range into an atomic replacement; "
            "repair: inspect local coverage and fetch only missing/suspicious dates; "
            "daily: refresh a short overlap and append new dates. "
            "Legacy aliases full and daily-update remain accepted."
        ),
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["all"],
        help="Dataset names or tags/sources, e.g. all twse tpex macro price.",
    )
    parser.add_argument("--start-date", default="earliest", help="Historical start date or 'earliest'.")
    parser.add_argument("--end-date", default="today", help="Historical end date, today, or now.")
    parser.add_argument(
        "--allow-daily-publication-lag",
        action="store_true",
        help=(
            "In daily mode only, do not fail the close-price pipeline when the "
            "current session's TWSE/TPEx margin-balance reports have not been "
            "published yet. All other dataset failures remain fatal."
        ),
    )
    parser.add_argument("--output-dir", default="data_tw_public", help="Output directory.")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent historical dataset workers.")
    parser.add_argument(
        "--date-workers",
        type=int,
        default=4,
        help="Concurrent date requests inside each historical dataset.",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--retries", type=int, default=3, help="Transient HTTP retry count per request.")
    parser.add_argument("--retry-backoff", type=float, default=1.0, help="Base seconds for exponential retry backoff.")
    parser.add_argument("--sleep", type=float, default=0.15, help="Delay between historical date requests per dataset.")
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help="Global minimum seconds between public HTTP requests. Default uses TW public profile.",
    )
    parser.add_argument(
        "--flush-every-dates",
        type=int,
        default=250,
        help="Write historical parquet after this many fetched dates; 0 writes once at the end.",
    )
    parser.add_argument("--max-dates", type=int, default=None, help="Optional smoke-test cap per historical dataset.")
    parser.add_argument("--refresh", action="store_true", help="Overwrite existing parquet instead of merging.")
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Reuse validated per-date JSONL journal, partial parquet, and raw receipts. "
            "Use --no-resume for a deliberately fresh historical rebuild."
        ),
    )
    parser.add_argument(
        "--daily-overlap-days",
        type=int,
        default=7,
        help="Calendar-day overlap refreshed by daily mode to capture official corrections.",
    )
    parser.add_argument(
        "--empty-recheck-days",
        type=int,
        default=30,
        help="Repair mode rechecks recently empty weekdays instead of trusting them as holidays.",
    )
    parser.add_argument(
        "--require-taiex-session-calendar",
        action="store_true",
        help=(
            "Require the receipt-verified, coverage-complete twse_taiex_ohlc archive "
            "as the expected-session calendar for historical TWSE/TPEx sources; "
            "TPEx feature histories remain downstream of audited TPEx OHLCV sessions. "
            "The canonical outer TW data-layer workflow enables this fail-closed mode."
        ),
    )
    parser.add_argument("--skip-raw", action="store_true", help="Do not persist raw response bytes.")
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show tqdm progress bars, including per-date bars for historical backfills.",
    )
    parser.add_argument("--include-weekends", action="store_true", help="Do not skip weekends for historical daily URLs.")
    parser.add_argument(
        "--verify-ssl",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify HTTPS certificates; retries once without verification on certificate errors.",
    )
    return parser.parse_args()


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_name(value: str, default: str = "resource") -> str:
    text = SAFE_NAME_PATTERN.sub("_", value.strip()).strip("._")
    return text or default


def _strip_html(value: Any) -> str:
    text = "" if value is None else str(value)
    text = html.unescape(text)
    text = HTML_TAG_PATTERN.sub("", text)
    return " ".join(text.replace("\u3000", " ").split())


def _strip_html_with_tag_boundaries(value: Any) -> str:
    text = "" if value is None else str(value)
    text = html.unescape(text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    return " ".join(text.replace("\u3000", " ").split())


def _make_unique(names: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    output: list[str] = []
    for raw in names:
        name = _strip_html(raw) or "column"
        count = counts.get(name, 0)
        counts[name] = count + 1
        output.append(name if count == 0 else f"{name}_{count + 1}")
    return output


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_dates(start: date, end: date, *, include_weekends: bool) -> list[date]:
    if start > end:
        return []
    days: list[date] = []
    cur = start
    while cur <= end:
        if include_weekends or cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _canonical_mode(value: str) -> str:
    return MODE_ALIASES.get(str(value), str(value))


def _coverage_state_path(output_dir: Path, spec: DatasetSpec) -> Path:
    return output_dir / "state" / f"{spec.name}.json"


def _historical_resume_cache_key(spec: DatasetSpec) -> str:
    payload = {
        "journal_schema_version": HISTORICAL_JOURNAL_SCHEMA_VERSION,
        "parser_contract_version": _historical_parser_contract_version(spec),
        "spec": asdict(spec),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _historical_parser_contract_version(spec: DatasetSpec) -> int:
    return HISTORICAL_PARSER_CONTRACT_VERSION_BY_DATASET.get(
        spec.name,
        HISTORICAL_PARSER_CONTRACT_VERSION,
    )


def _historical_journal_path(output_dir: Path, spec: DatasetSpec) -> Path:
    return output_dir / "state" / "journals" / f"{spec.name}.jsonl"


def _historical_partial_path(
    output_dir: Path,
    spec: DatasetSpec,
    cache_key: str,
) -> Path:
    return output_dir / "state" / "partials" / f"{spec.name}.{cache_key}.parquet"


@contextmanager
def _historical_dataset_lock(output_dir: Path, spec: DatasetSpec):
    """Prevent two processes from mutating one dataset stage concurrently."""

    lock_path = output_dir / "state" / "locks" / f"{spec.name}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(
                    f"another process is already updating {spec.name} in {output_dir}"
                ) from exc
        yield
    finally:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    with _JOURNAL_LOCK:
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            offset = 0
            while offset < len(encoded):
                written = os.write(descriptor, encoded[offset:])
                if written <= 0:
                    raise OSError(f"short append while writing JSONL journal: {path}")
                offset += written
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _append_historical_journal_record(
    cache: HistoricalResumeCache,
    spec: DatasetSpec,
    result: HistoricalDateResult,
    *,
    status: str,
    source: str,
) -> None:
    raw_path_value: str | None = None
    raw_size: int | None = None
    raw_sha256: str | None = None
    if result.raw_path:
        raw_path = Path(result.raw_path)
        if raw_path.is_file():
            try:
                output_dir = cache.journal_path.parents[2]
                raw_path_value = str(raw_path.resolve().relative_to(output_dir.resolve()))
            except (OSError, ValueError):
                raw_path_value = str(raw_path)
            raw_bytes = raw_path.read_bytes()
            raw_size = len(raw_bytes)
            raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
        else:
            raw_path_value = str(raw_path)
    _append_jsonl(
        cache.journal_path,
        {
            "schema_version": HISTORICAL_JOURNAL_SCHEMA_VERSION,
            "cache_key": cache.cache_key,
            "dataset": spec.name,
            "date": result.day.isoformat(),
            "recorded_at_utc": _now_utc(),
            "status": status,
            "source": source,
            "url": result.url,
            "rows": int(result.frame.height),
            "raw_path": raw_path_value,
            "raw_size": raw_size,
            "raw_sha256": raw_sha256,
            "http_status": result.http_status,
            "content_type": result.content_type,
            "content_length": result.content_length,
            "body_sha256": result.body_sha256,
            "body_snippet": result.body_snippet,
            "response_attempts": int(result.response_attempts),
            "source_unavailable_reason": result.source_unavailable_reason,
            "error": result.error,
        },
    )


def _iter_historical_journal_records(
    path: Path,
    spec: DatasetSpec,
):
    """Yield valid append-only records, tolerating only a torn final write."""

    if not path.exists():
        return
    try:
        raw_text = path.read_text(encoding="utf-8")
        lines = raw_text.splitlines()
        for index, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                if index == len(lines) - 1 and not raw_text.endswith("\n"):
                    # A killed process may leave one incomplete trailing record.
                    # Earlier append-only records remain valid and remain usable.
                    continue
                raise ValueError(
                    f"corrupt non-terminal JSONL journal record {index + 1}: {path}"
                )
            if not isinstance(payload, dict):
                continue
            if payload.get("dataset") != spec.name:
                continue
            try:
                schema_version = int(payload.get("schema_version", -1))
            except (TypeError, ValueError):
                continue
            if schema_version != HISTORICAL_JOURNAL_SCHEMA_VERSION:
                continue
            try:
                day = _parse_date(str(payload.get("date", ""))[:10])
            except ValueError:
                continue
            yield day, payload
    except OSError:
        return


def _load_historical_journal_latest(
    path: Path,
    spec: DatasetSpec,
    cache_key: str,
) -> dict[date, dict[str, Any]]:
    latest: dict[date, dict[str, Any]] = {}
    for day, payload in _iter_historical_journal_records(path, spec):
        if payload.get("cache_key") == cache_key:
            latest[day] = payload
    return latest


def _validated_historical_failed_receipts(
    output_dir: Path,
    spec: DatasetSpec,
    allowed_dates: set[date],
) -> list[tuple[date, dict[str, Any], Path]]:
    """Return journal- and content-verified failed receipts for reparsing."""

    expected_parent = (output_dir / "raw_failures" / spec.name).resolve()
    candidates: dict[date, tuple[dict[str, Any], Path]] = {}
    journal_path = _historical_journal_path(output_dir, spec)
    for day, payload in _iter_historical_journal_records(journal_path, spec):
        if day not in allowed_dates:
            continue
        if (
            payload.get("status") != "failed"
            or payload.get("source") != "network"
            or payload.get("source_unavailable_reason") is not None
            or not str(payload.get("error") or "").strip()
        ):
            continue
        try:
            if int(payload.get("rows", -1)) != 0 or int(
                payload.get("http_status", -1)
            ) != 200:
                continue
        except (TypeError, ValueError):
            continue

        expected_url, _response_kind = _historical_request_info(spec, day)
        allowed_urls = [expected_url]
        fallback_url = _historical_response_fallback_url(spec, expected_url)
        if fallback_url is not None:
            allowed_urls.append(fallback_url)
        actual_url = str(payload.get("url") or "")
        if not any(
            _historical_urls_match_receipt(actual_url, candidate_url)
            for candidate_url in allowed_urls
        ):
            continue

        raw_path_value = payload.get("raw_path")
        if not raw_path_value:
            continue
        raw_path = Path(str(raw_path_value))
        if not raw_path.is_absolute():
            raw_path = output_dir / raw_path
        try:
            raw_path = raw_path.resolve()
        except OSError:
            continue
        if raw_path.parent != expected_parent or not raw_path.is_file():
            continue
        filename_match = re.fullmatch(
            rf"{re.escape(day.isoformat())}\.([0-9a-f]{{16}})\.(html|json|bin)",
            raw_path.name,
        )
        if filename_match is None:
            continue
        try:
            content = raw_path.read_bytes()
            raw_size = int(payload.get("raw_size", -1))
            content_length = int(payload.get("content_length", -1))
        except (OSError, TypeError, ValueError):
            continue
        digest = hashlib.sha256(content).hexdigest()
        if (
            raw_size != len(content)
            or content_length != len(content)
            or payload.get("raw_sha256") != digest
            or payload.get("body_sha256") != digest
            or filename_match.group(1) != digest[:16]
        ):
            continue
        candidates[day] = (payload, raw_path)
    return [
        (day, *candidates[day])
        for day in sorted(candidates)
    ]


def _known_source_unavailable_reason(
    spec: DatasetSpec,
    day: date,
) -> str | None:
    for range_start, range_end, reason in TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES.get(
        spec.name,
        (),
    ):
        if range_start <= day <= range_end:
            return reason
    return None


def _known_source_unavailable_range_summaries(
    spec: DatasetSpec,
    start: date,
    end: date,
    *,
    confirmed_dates: set[date] | None = None,
    expected_dates: set[date] | None = None,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    confirmed = confirmed_dates or set()
    expected = expected_dates or set()
    for range_start, range_end, reason in TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES.get(
        spec.name,
        (),
    ):
        selected_start = max(start, range_start)
        selected_end = min(end, range_end)
        if selected_start > selected_end:
            continue
        summaries.append(
            {
                "start": selected_start.isoformat(),
                "end": selected_end.isoformat(),
                "reason": reason,
                "confirmed_dates": sum(
                    selected_start <= day <= selected_end for day in confirmed
                ),
                "expected_session_dates": sum(
                    selected_start <= day <= selected_end for day in expected
                ),
            }
        )
    return summaries


def _historical_urls_match_receipt(actual_url: str, expected_url: str) -> bool:
    try:
        actual = urlparse(actual_url)
        expected = urlparse(expected_url)
        actual_query = sorted(
            (key, value)
            for key, value in parse_qsl(actual.query, keep_blank_values=True)
            if key != "_"
        )
        expected_query = sorted(
            (key, value)
            for key, value in parse_qsl(expected.query, keep_blank_values=True)
            if key != "_"
        )
    except (TypeError, ValueError):
        return False
    return (
        actual.scheme.lower(),
        actual.netloc.lower(),
        actual.path,
        actual_query,
    ) == (
        expected.scheme.lower(),
        expected.netloc.lower(),
        expected.path,
        expected_query,
    )


def _source_unavailable_content_is_explicit_no_data(
    spec: DatasetSpec,
    day: date,
    content: bytes,
) -> bool:
    if not content:
        return False
    try:
        decoded, _ = _decode_bytes(content)
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return False
        _validate_json_historical_response_date(payload, spec, day)
        explicit_no_data = _json_payload_explicitly_reports_no_data(
            payload
        ) or _json_payload_has_structured_empty_table(payload, spec)
        if not explicit_no_data or _json_payload_status_error(payload) is not None:
            return False
        frame = _normalize_historical_frame(
            spec,
            _parse_json_table_payload(payload, spec, day),
        )
        return frame.is_empty()
    except Exception:
        return False


def _source_unavailable_receipt_path(
    output_dir: Path,
    spec: DatasetSpec,
    raw_path_value: Any,
) -> Path | None:
    if not raw_path_value:
        return None
    candidate = Path(str(raw_path_value))
    if not candidate.is_absolute():
        candidate = output_dir / candidate
    try:
        resolved = candidate.resolve()
        expected_parent = (output_dir / "raw_empty" / spec.name).resolve()
    except OSError:
        return None
    if resolved.parent != expected_parent or not resolved.is_file():
        return None
    return resolved


def _source_unavailable_receipt_payload_is_valid(
    output_dir: Path,
    spec: DatasetSpec,
    day: date,
    payload: dict[str, Any],
) -> bool:
    reason = _known_source_unavailable_reason(spec, day)
    if reason is None or payload.get("source_unavailable_reason") != reason:
        return False
    try:
        if int(payload.get("http_status", -1)) != 200:
            return False
    except (TypeError, ValueError):
        return False
    expected_url, response_kind = _historical_request_info(spec, day)
    if response_kind != "json" or not _historical_urls_match_receipt(
        str(payload.get("url", "")),
        expected_url,
    ):
        return False
    raw_path = _source_unavailable_receipt_path(
        output_dir,
        spec,
        payload.get("raw_path"),
    )
    if raw_path is None:
        return False
    try:
        content = raw_path.read_bytes()
        raw_size = int(payload.get("raw_size", -1))
        content_length = int(payload.get("content_length", -1))
    except (OSError, TypeError, ValueError):
        return False
    digest = hashlib.sha256(content).hexdigest()
    if (
        raw_size != len(content)
        or content_length != len(content)
        or payload.get("raw_sha256") != digest
        or payload.get("body_sha256") != digest
    ):
        return False
    return _source_unavailable_content_is_explicit_no_data(
        spec,
        day,
        content,
    )


def _validated_source_unavailable_receipt_dates(
    output_dir: Path,
    spec: DatasetSpec,
    allowed_dates: set[date],
) -> set[date]:
    if spec.name not in TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES:
        return set()
    latest = _load_historical_journal_latest(
        _historical_journal_path(output_dir, spec),
        spec,
        _historical_resume_cache_key(spec),
    )
    return {
        day
        for day, payload in latest.items()
        if day in allowed_dates
        and payload.get("status") == "empty"
        and _source_unavailable_receipt_payload_is_valid(
            output_dir,
            spec,
            day,
            payload,
        )
    }


def _source_unavailable_result_receipt_is_valid(
    output_dir: Path,
    spec: DatasetSpec,
    result: HistoricalDateResult,
) -> bool:
    raw_path = Path(result.raw_path) if result.raw_path else None
    if raw_path is None:
        return False
    # Download commands commonly receive a stage-relative ``--output-dir``.
    # In that case _write_immutable_raw() returns a cwd-relative path that
    # already contains the output directory. Treating it as relative to
    # output_dir a second time would produce ``stage/stage/raw_empty/...`` and
    # incorrectly journal a verified known-gap receipt as failed. Normalize
    # the existing path to absolute before the confined receipt validator.
    if not raw_path.is_absolute():
        if raw_path.is_file():
            raw_path = raw_path.resolve()
        else:
            candidate = output_dir / raw_path
            if candidate.is_file():
                raw_path = candidate.resolve()
    if not raw_path.is_file():
        return False
    try:
        content = raw_path.read_bytes()
    except OSError:
        return False
    digest = hashlib.sha256(content).hexdigest()
    return _source_unavailable_receipt_payload_is_valid(
        output_dir,
        spec,
        result.day,
        {
            "source_unavailable_reason": result.source_unavailable_reason,
            "http_status": result.http_status,
            "url": result.url,
            "raw_path": str(raw_path),
            "raw_size": len(content),
            "content_length": result.content_length,
            "raw_sha256": digest,
            "body_sha256": result.body_sha256,
        },
    )


def _load_coverage_state(path: Path, spec: DatasetSpec) -> dict[str, Any]:
    empty = {
        "schema_version": COVERAGE_STATE_SCHEMA_VERSION,
        "dataset": spec.name,
        "confirmed_empty_dates": [],
        "failed_dates": {},
        "coverage_complete": False,
    }
    if not path.exists():
        return empty
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return empty
    if not isinstance(payload, dict):
        return empty
    if payload.get("dataset") != spec.name:
        return empty
    if int(payload.get("schema_version", -1)) != COVERAGE_STATE_SCHEMA_VERSION:
        return empty
    return {**empty, **payload}


def _state_date_set(state: dict[str, Any], key: str) -> set[date]:
    values = state.get(key, [])
    if not isinstance(values, list):
        return set()
    parsed: set[date] = set()
    for value in values:
        try:
            parsed.add(_parse_date(str(value)[:10]))
        except ValueError:
            continue
    return parsed


def _state_failed_date_set(state: dict[str, Any]) -> set[date]:
    values = state.get("failed_dates", {})
    if not isinstance(values, dict):
        return set()
    parsed: set[date] = set()
    for value in values:
        try:
            parsed.add(_parse_date(str(value)[:10]))
        except ValueError:
            continue
    return parsed


def _existing_date_counts(path: Path) -> tuple[dict[date, int], list[str]]:
    if not path.exists():
        return {}, []
    schema = pq.read_schema(path).names
    if DATE_COLUMN not in schema:
        raise ValueError(f"existing parquet has no {DATE_COLUMN!r} column: {path}")
    counts = (
        pl.scan_parquet(path)
        .select(pl.col(DATE_COLUMN).cast(pl.Utf8, strict=False).alias(DATE_COLUMN))
        .group_by(DATE_COLUMN)
        .len()
        .collect()
    )
    output: dict[date, int] = {}
    invalid: list[str] = []
    for raw_date, count in counts.iter_rows():
        try:
            parsed = _parse_date(str(raw_date)[:10])
        except (TypeError, ValueError):
            invalid.append(str(raw_date))
            continue
        output[parsed] = output.get(parsed, 0) + int(count)
    return output, invalid


def _suspicious_ohlcv_dates(
    path: Path,
    spec: DatasetSpec,
    counts: dict[date, int],
) -> tuple[set[date], list[str]]:
    required = OHLCV_REQUIRED_COLUMNS.get(spec.name) or HISTORICAL_REQUIRED_COLUMNS.get(
        spec.name
    )
    if not required or not path.exists() or not counts:
        return set(), []
    schema = set(pq.read_schema(path).names)
    missing_columns = [column for column in required if column not in schema]
    if missing_columns:
        return set(counts), [f"missing_columns={','.join(missing_columns)}"]

    suspicious: set[date] = set()
    issues: list[str] = []
    symbol_column = HISTORICAL_SYMBOL_COLUMNS[spec.name]
    duplicate_dates = (
        pl.scan_parquet(path)
        .select(
            pl.col(DATE_COLUMN).cast(pl.Utf8, strict=False).alias(DATE_COLUMN),
            pl.col(symbol_column).cast(pl.Utf8, strict=False).alias("_symbol"),
        )
        .drop_nulls([DATE_COLUMN, "_symbol"])
        .group_by([DATE_COLUMN, "_symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .select(DATE_COLUMN)
        .unique()
        .collect()
    )
    for value in duplicate_dates.get_column(DATE_COLUMN).to_list():
        try:
            suspicious.add(_parse_date(str(value)[:10]))
        except ValueError:
            continue
    if suspicious:
        issues.append(f"duplicate_symbol_dates={len(suspicious)}")

    if spec.name not in OHLCV_REQUIRED_COLUMNS:
        return suspicious, issues

    def number(column: str) -> pl.Expr:
        return (
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
        )

    volume_column, open_column, high_column, low_column, close_column = required[1:]
    has_tpex_average_evidence = spec.name == "tpex_daily_ohlcv" and {
        "均價",
        "成交金額(元)",
    }.issubset(schema)
    # Legacy TPEx daily reports use O=H=L=0 as an explicit unavailable K-line
    # sentinel for a small number of rows that still have an official positive
    # close and volume.  It is not a corrupt date: the raw archive remains
    # unchanged, while the downstream per-symbol builder records and converts
    # this exact sentinel to a flat bar at the same official close.
    legacy_tpex_zero_ohlc = (
        (
            pl.all_horizontal(
                pl.col("_open") == 0.0,
                pl.col("_high") == 0.0,
                pl.col("_low") == 0.0,
            )
            & pl.col("_close").is_not_null()
            & (pl.col("_close") > 0.0)
        )
        | (
            pl.all_horizontal(
                pl.col("_open") == 0.0,
                pl.col("_high") == 0.0,
                pl.col("_low") == 0.0,
                pl.col("_close") == 0.0,
            )
            & (pl.col("_volume") > 0.0)
            & (pl.col("_average") > 0.0)
            & (pl.col("_amount") > 0.0)
            & (
                ((pl.col("_amount") / pl.col("_volume")) - pl.col("_average"))
                .abs()
                <= 0.011
            )
        )
        if spec.name == "tpex_daily_ohlcv"
        else pl.lit(False)
    ).fill_null(False)
    invalid_bar_dates = (
        pl.scan_parquet(path)
        .select(
            pl.col(DATE_COLUMN).cast(pl.Utf8, strict=False).alias(DATE_COLUMN),
            number(volume_column).alias("_volume"),
            number(open_column).alias("_open"),
            number(high_column).alias("_high"),
            number(low_column).alias("_low"),
            number(close_column).alias("_close"),
            (
                number("均價")
                if has_tpex_average_evidence
                else pl.lit(None, dtype=pl.Float64)
            ).alias("_average"),
            (
                number("成交金額(元)")
                if has_tpex_average_evidence
                else pl.lit(None, dtype=pl.Float64)
            ).alias("_amount"),
        )
        .filter(
            (pl.col("_volume").is_not_null() & (pl.col("_volume") < 0.0))
            | (
                pl.all_horizontal(
                    pl.col("_open").is_not_null(),
                    pl.col("_high").is_not_null(),
                    pl.col("_low").is_not_null(),
                    pl.col("_close").is_not_null(),
                )
                & (pl.col("_volume").fill_null(0.0) > 0.0)
                & ~legacy_tpex_zero_ohlc
                & (
                    (pl.min_horizontal("_open", "_high", "_low", "_close") <= 0.0)
                    | (pl.col("_high") < pl.max_horizontal("_open", "_low", "_close"))
                    | (pl.col("_low") > pl.min_horizontal("_open", "_high", "_close"))
                )
            )
        )
        .select(DATE_COLUMN)
        .unique()
        .collect()
    )
    invalid_dates: set[date] = set()
    for value in invalid_bar_dates.get_column(DATE_COLUMN).to_list():
        try:
            invalid_dates.add(_parse_date(str(value)[:10]))
        except ValueError:
            continue
    suspicious.update(invalid_dates)
    if invalid_dates:
        issues.append(f"invalid_ohlcv_dates={len(invalid_dates)}")

    ordered = sorted(counts)
    low_row_dates: set[date] = set()
    for idx in range(1, len(ordered) - 1):
        current = ordered[idx]
        neighbor_floor = min(counts[ordered[idx - 1]], counts[ordered[idx + 1]])
        if neighbor_floor >= 100 and counts[current] < max(10, int(neighbor_floor * 0.35)):
            low_row_dates.add(current)
    suspicious.update(low_row_dates)
    if low_row_dates:
        issues.append(f"abnormally_low_row_dates={len(low_row_dates)}")
    return suspicious, issues


def _historical_bounds(spec: DatasetSpec, args: argparse.Namespace) -> tuple[date, date]:
    configured_start = _parse_date(spec.start_date or "2000-01-01")
    start = configured_start if args.start_date == "earliest" else max(configured_start, _parse_date(args.start_date))
    end = _parse_date(resolve_end_date(args.end_date))
    return start, end


def _tpex_calendar_spec() -> DatasetSpec:
    return next(
        spec
        for spec in HISTORICAL_DAILY_DATASETS
        if spec.name == TPEX_OFFICIAL_CALENDAR_DATASET
    )


def _requires_strict_session_calendar(spec: DatasetSpec, args: argparse.Namespace) -> bool:
    return spec.kind == "historical_json_table" and bool(
        getattr(args, "require_taiex_session_calendar", False)
    )


def _uses_taiex_session_calendar(spec: DatasetSpec, args: argparse.Namespace) -> bool:
    # TPEx feature histories deliberately remain downstream of the audited TPEx
    # OHLCV session set. Every other historical source uses the receipt-bound
    # official TAIEX actual-session archive directly in canonical strict mode.
    return (
        _requires_strict_session_calendar(spec, args)
        and spec.name not in TPEX_SESSION_DEPENDENT_DATASETS
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_file_identity(path: Path) -> tuple[int, int, int, int]:
    """Return mutation-relevant stat fields, deliberately excluding atime."""

    stat = path.stat()
    return (
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
    )


def _validated_taiex_session_dates(
    output_dir: Path,
    start: date,
    end: date,
) -> tuple[set[date], str]:
    """Load a portable, receipt-verified TAIEX session calendar.

    The monthly TAIEX downloader certifies coverage by requested month.  This
    consumer additionally binds that certification to the exact canonical
    parquet bytes before using its dates to decide which historical weekdays may
    be skipped. A copied production tree remains valid because receipt paths are
    deliberately tree-relative/basename based rather than tied to a stage root.
    """

    parquet_path = output_dir / f"{TAIEX_SESSION_CALENDAR_DATASET}.parquet"
    summary_path = output_dir / f"{TAIEX_SESSION_CALENDAR_DATASET}.summary.json"
    if not parquet_path.is_file() or not summary_path.is_file():
        raise RuntimeError(
            "strict historical coverage requires twse_taiex_ohlc.parquet and "
            "twse_taiex_ohlc.summary.json; run the TAIEX monthly stage first"
        )

    parquet_stat = parquet_path.stat()
    summary_stat = summary_path.stat()
    parquet_identity = _stable_file_identity(parquet_path)
    summary_identity = _stable_file_identity(summary_path)
    parquet_sha256 = _file_sha256(parquet_path)
    summary_sha256 = _file_sha256(summary_path)
    if (
        _stable_file_identity(parquet_path) != parquet_identity
        or _stable_file_identity(summary_path) != summary_identity
    ):
        raise RuntimeError("TAIEX session calendar files changed during validation")
    cache_key = (
        str(parquet_path.resolve()),
        int(parquet_stat.st_size),
        int(parquet_stat.st_mtime_ns),
        parquet_sha256,
        int(summary_stat.st_size),
        int(summary_stat.st_mtime_ns),
        summary_sha256,
    )
    with _TAIEX_SESSION_CACHE_LOCK:
        cached = _TAIEX_SESSION_CACHE.get(cache_key)

    if cached is None:
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            raise RuntimeError(
                f"TAIEX session calendar summary is unreadable: {summary_path}"
            ) from exc
        if not isinstance(summary, dict):
            raise RuntimeError("TAIEX session calendar summary must be a JSON object")
        if int(summary.get("schema_version", -1)) != TAIEX_SESSION_CALENDAR_SUMMARY_SCHEMA_VERSION:
            raise RuntimeError("TAIEX session calendar summary schema_version is unsupported")
        if summary.get("dataset") != TAIEX_SESSION_CALENDAR_DATASET:
            raise RuntimeError("TAIEX session calendar summary names the wrong dataset")
        if (
            summary.get("coverage_complete") is not True
            or summary.get("baseline_established") is not True
            or summary.get("replacement_promoted") is not True
            or int(summary.get("unresolved_month_count", -1)) != 0
            or int(summary.get("failed_count", -1)) != 0
        ):
            raise RuntimeError(
                "TAIEX session calendar requires a promoted, coverage-complete "
                "baseline with no unresolved or failed months"
            )
        try:
            coverage_start = _parse_date(str(summary.get("effective_start_date", ""))[:10])
            coverage_end = _parse_date(str(summary.get("effective_end_date", ""))[:10])
        except ValueError as exc:
            raise RuntimeError("TAIEX session calendar summary has invalid coverage bounds") from exc
        if coverage_start > coverage_end:
            raise RuntimeError("TAIEX session calendar summary has reversed coverage bounds")

        declared_canonical = Path(str(summary.get("canonical_path", "")))
        if declared_canonical.name != parquet_path.name:
            raise RuntimeError(
                "TAIEX session calendar canonical_path does not name the expected parquet"
            )
        receipt = summary.get("output_receipt")
        if not isinstance(receipt, dict):
            raise RuntimeError("TAIEX session calendar summary has no output receipt")
        receipt_path = Path(str(receipt.get("path", "")))
        if receipt_path.name != parquet_path.name:
            raise RuntimeError(
                "TAIEX session calendar receipt does not name the expected parquet"
            )
        try:
            receipt_size = int(receipt.get("size", -1))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("TAIEX session calendar receipt has an invalid size") from exc
        receipt_sha256 = str(receipt.get("sha256", "")).lower()
        if not re.fullmatch(r"[0-9a-f]{64}", receipt_sha256):
            raise RuntimeError("TAIEX session calendar receipt has an invalid sha256")
        if receipt_size != int(parquet_stat.st_size):
            raise RuntimeError("TAIEX session calendar parquet size disagrees with its receipt")
        if parquet_sha256 != receipt_sha256:
            raise RuntimeError("TAIEX session calendar parquet sha256 disagrees with its receipt")

        schema = set(pq.read_schema(parquet_path).names)
        missing_columns = [
            column
            for column in TAIEX_SESSION_CALENDAR_REQUIRED_COLUMNS
            if column not in schema
        ]
        if missing_columns:
            raise RuntimeError(
                "TAIEX session calendar parquet is missing required columns: "
                + ", ".join(missing_columns)
            )
        calendar = pl.read_parquet(
            parquet_path,
            columns=list(TAIEX_SESSION_CALENDAR_REQUIRED_COLUMNS),
        ).with_columns(pl.col("date").cast(pl.Date, strict=False))
        if calendar.is_empty():
            raise RuntimeError("TAIEX session calendar parquet has no rows")
        if calendar.get_column("date").null_count():
            raise RuntimeError("TAIEX session calendar parquet has invalid dates")
        if calendar.get_column("date").n_unique() != calendar.height:
            raise RuntimeError("TAIEX session calendar parquet has duplicate dates")
        numeric_columns = [
            pl.col(column).cast(pl.Float64, strict=False).alias(column)
            for column in (
                "opening_index",
                "highest_index",
                "lowest_index",
                "closing_index",
            )
        ]
        calendar = calendar.with_columns(numeric_columns)
        invalid = calendar.filter(
            pl.col("_dataset").is_null()
            | (pl.col("_dataset") != TAIEX_SESSION_CALENDAR_DATASET)
            | pl.col("_source").is_null()
            | (pl.col("_source") != "TWSE")
            | pl.col("_source_product").is_null()
            | (pl.col("_source_product") != "indicesReport/MI_5MINS_HIST")
            | pl.any_horizontal(
                *[
                    pl.col(column).is_null()
                    | ~pl.col(column).is_finite()
                    | (pl.col(column) <= 0.0)
                    for column in (
                        "opening_index",
                        "highest_index",
                        "lowest_index",
                        "closing_index",
                    )
                ]
            )
            | (
                pl.col("highest_index")
                < pl.max_horizontal(
                    "opening_index", "lowest_index", "closing_index"
                )
            )
            | (
                pl.col("lowest_index")
                > pl.min_horizontal(
                    "opening_index", "highest_index", "closing_index"
                )
            )
        )
        if not invalid.is_empty():
            raise RuntimeError("TAIEX session calendar parquet has invalid data or provenance")
        if int(summary.get("output_rows", -1)) != calendar.height:
            raise RuntimeError("TAIEX session calendar row count disagrees with its summary")
        cached = (
            coverage_start,
            coverage_end,
            frozenset(calendar.get_column("date").to_list()),
            receipt_sha256,
        )
        with _TAIEX_SESSION_CACHE_LOCK:
            _TAIEX_SESSION_CACHE.clear()
            _TAIEX_SESSION_CACHE[cache_key] = cached

    coverage_start, coverage_end, sessions, receipt_sha256 = cached
    if coverage_start > start or coverage_end < end:
        raise RuntimeError(
            "TAIEX session calendar does not cover the complete requested range: "
            f"calendar={coverage_start.isoformat()}..{coverage_end.isoformat()} "
            f"requested={start.isoformat()}..{end.isoformat()}"
        )
    return {day for day in sessions if start <= day <= end}, receipt_sha256


def _validated_tpex_session_dates(
    output_dir: Path,
    start: date,
    end: date,
    *,
    expected_taiex_sessions: set[date] | None = None,
    expected_taiex_receipt: str | None = None,
) -> set[date]:
    """Return official TPEx sessions only after the OHLCV baseline passed audit."""

    spec = _tpex_calendar_spec()
    parquet_path = output_dir / f"{spec.name}.parquet"
    state_path = _coverage_state_path(output_dir, spec)
    if not parquet_path.is_file() or not state_path.is_file():
        raise RuntimeError(
            "TPEx feature history requires a completed tpex_daily_ohlcv baseline; "
            "include tpex_daily_ohlcv in this run or rebuild it first"
        )
    parquet_stat = parquet_path.stat()
    state_stat = state_path.stat()
    cache_key = (
        str(parquet_path.resolve()),
        int(parquet_stat.st_size),
        int(parquet_stat.st_mtime_ns),
        int(state_stat.st_size),
        int(state_stat.st_mtime_ns),
    )
    with _TPEX_SESSION_CACHE_LOCK:
        cached = _TPEX_SESSION_CACHE.get(cache_key)
    if cached is None:
        state = _load_coverage_state(state_path, spec)
        if not bool(state.get("baseline_established")) or not bool(
            state.get("coverage_complete")
        ):
            raise RuntimeError(
                "TPEx feature history requires tpex_daily_ohlcv coverage_complete=true"
            )
        try:
            coverage_start = _parse_date(str(state.get("coverage_start", ""))[:10])
            checked_through = _parse_date(str(state.get("checked_through", ""))[:10])
        except ValueError as exc:
            raise RuntimeError(
                "tpex_daily_ohlcv coverage state has invalid calendar bounds"
            ) from exc
        counts, invalid_date_values = _existing_date_counts(parquet_path)
        if invalid_date_values:
            raise RuntimeError(
                "tpex_daily_ohlcv calendar contains invalid date values: "
                f"{len(invalid_date_values)}"
            )
        suspicious, issues = _suspicious_ohlcv_dates(parquet_path, spec, counts)
        if suspicious or issues:
            detail = "; ".join(issues) or f"suspicious_dates={len(suspicious)}"
            raise RuntimeError(
                "tpex_daily_ohlcv cannot be trusted as the TPEx calendar: " + detail
            )
        cached = (
            coverage_start,
            checked_through,
            frozenset(counts),
            (
                str(state.get("coverage_calendar_source"))
                if state.get("coverage_calendar_source") is not None
                else None
            ),
            (
                str(state.get("coverage_calendar_sha256"))
                if state.get("coverage_calendar_sha256") is not None
                else None
            ),
        )
        with _TPEX_SESSION_CACHE_LOCK:
            _TPEX_SESSION_CACHE.clear()
            _TPEX_SESSION_CACHE[cache_key] = cached

    (
        coverage_start,
        checked_through,
        sessions,
        calendar_source,
        calendar_sha256,
    ) = cached
    if coverage_start > start or checked_through < end:
        raise RuntimeError(
            "tpex_daily_ohlcv calendar does not cover requested TPEx feature range: "
            f"calendar={coverage_start.isoformat()}..{checked_through.isoformat()} "
            f"requested={start.isoformat()}..{end.isoformat()}"
        )
    selected = {day for day in sessions if start <= day <= end}
    if expected_taiex_sessions is not None:
        if calendar_source != TAIEX_SESSION_CALENDAR_DATASET:
            raise RuntimeError(
                "strict TPEx feature history requires a tpex_daily_ohlcv baseline "
                "built from the verified TAIEX session calendar"
            )
        if not expected_taiex_receipt or calendar_sha256 != expected_taiex_receipt:
            raise RuntimeError(
                "tpex_daily_ohlcv session-calendar receipt is stale or disagrees "
                "with the current TAIEX archive"
            )
        if selected != expected_taiex_sessions:
            missing = expected_taiex_sessions - selected
            unexpected = selected - expected_taiex_sessions
            raise RuntimeError(
                "audited TPEx OHLCV sessions disagree with the verified TAIEX "
                f"calendar: missing={len(missing)} unexpected={len(unexpected)}"
            )
    return selected


def _plan_historical_download(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
) -> HistoricalDownloadPlan:
    mode = _canonical_mode(args.mode)
    start, end = _historical_bounds(spec, args)
    taiex_calendar_receipt: str | None = None
    if spec.name in TPEX_SESSION_DEPENDENT_DATASETS:
        expected_taiex_sessions: set[date] | None = None
        if _requires_strict_session_calendar(spec, args):
            expected_taiex_sessions, taiex_calendar_receipt = (
                _validated_taiex_session_dates(output_dir, start, end)
            )
        all_weekdays = _validated_tpex_session_dates(
            output_dir,
            start,
            end,
            expected_taiex_sessions=expected_taiex_sessions,
            expected_taiex_receipt=taiex_calendar_receipt,
        )
    elif _uses_taiex_session_calendar(spec, args):
        all_weekdays, taiex_calendar_receipt = _validated_taiex_session_dates(
            output_dir,
            start,
            end,
        )
    else:
        all_weekdays = set(
            _iter_dates(
                start,
                end,
                include_weekends=bool(args.include_weekends),
            )
        )
    parquet_path = output_dir / f"{spec.name}.parquet"
    state_path = _coverage_state_path(output_dir, spec)
    state = _load_coverage_state(state_path, spec)
    if _uses_taiex_session_calendar(spec, args):
        state["coverage_calendar_source"] = TAIEX_SESSION_CALENDAR_DATASET
        state["coverage_calendar_kind"] = "receipt_verified_official_open_sessions"
        state["coverage_calendar_sha256"] = taiex_calendar_receipt
    elif spec.name in TPEX_SESSION_DEPENDENT_DATASETS:
        state["coverage_calendar_source"] = TPEX_OFFICIAL_CALENDAR_DATASET
        state["coverage_calendar_kind"] = "validated_official_open_sessions"
        if taiex_calendar_receipt is not None:
            state["root_coverage_calendar_source"] = TAIEX_SESSION_CALENDAR_DATASET
            state["root_coverage_calendar_sha256"] = taiex_calendar_receipt
    replace_output = mode == "rebuild" or bool(args.refresh)

    try:
        counts, invalid_date_values = _existing_date_counts(parquet_path)
    except Exception as exc:
        if mode == "daily":
            raise RuntimeError(
                f"daily mode requires a readable baseline for {spec.name}; run --mode repair: {exc}"
            ) from exc
        counts, invalid_date_values = {}, [f"unreadable:{type(exc).__name__}:{exc}"]
        replace_output = True
    if _requires_strict_session_calendar(spec, args):
        existing_in_range = {
            day for day in counts if start <= day <= end
        }
        calendar_disagreement = existing_in_range - all_weekdays
        if calendar_disagreement:
            examples = ", ".join(
                day.isoformat() for day in sorted(calendar_disagreement)[:5]
            )
            raise RuntimeError(
                f"{spec.name} canonical dates disagree with its verified "
                f"session calendar: count={len(calendar_disagreement)} "
                f"examples={examples}"
            )
    existing_dates = set(counts) & all_weekdays
    confirmed_empty = _state_date_set(state, "confirmed_empty_dates") & all_weekdays
    if _requires_strict_session_calendar(spec, args):
        # A validated session is expected to contain the requested official
        # report. The only reusable empties are individually receipt-verified
        # dates inside a declared official endpoint archive gap. Empty outcomes
        # from older weekday-based runs are never trusted as holidays.
        confirmed_empty = _validated_source_unavailable_receipt_dates(
            output_dir,
            spec,
            all_weekdays,
        )
    unresolved_failed = _state_failed_date_set(state) & all_weekdays
    try:
        suspicious_dates, suspicious_issues = _suspicious_ohlcv_dates(
            parquet_path, spec, counts
        )
    except Exception as exc:
        if mode == "daily":
            raise RuntimeError(
                f"daily mode could not validate {spec.name}; run --mode repair: {exc}"
            ) from exc
        suspicious_dates = set(counts)
        suspicious_issues = [f"validation_error={type(exc).__name__}:{exc}"]
    suspicious_dates &= all_weekdays
    if invalid_date_values:
        suspicious_issues.append(f"invalid_date_values={len(invalid_date_values)}")

    if replace_output:
        requested = sorted(all_weekdays)
        missing_before = set(all_weekdays)
        confirmed_empty = set()
    elif mode == "repair":
        recheck_days = max(0, int(args.empty_recheck_days))
        recent_cutoff = end - timedelta(days=max(0, recheck_days - 1))
        trusted_empty = (
            set(confirmed_empty)
            if recheck_days == 0
            else {day for day in confirmed_empty if day < recent_cutoff}
        )
        missing_before = (all_weekdays - existing_dates - trusted_empty) | unresolved_failed
        requested = sorted(missing_before | suspicious_dates)
    elif mode == "daily":
        if not parquet_path.exists() or not bool(state.get("baseline_established")):
            raise RuntimeError(
                f"daily mode requires a complete baseline for {spec.name}; run --mode rebuild or --mode repair first"
            )
        try:
            checked_through = _parse_date(str(state.get("checked_through", ""))[:10])
        except ValueError as exc:
            raise RuntimeError(
                f"daily mode found invalid coverage state for {spec.name}; run --mode repair"
            ) from exc
        overlap_days = max(1, int(args.daily_overlap_days))
        overlap_start = max(start, end - timedelta(days=overlap_days - 1))
        gap_start = max(start, checked_through + timedelta(days=1))
        requested_start = min(overlap_start, gap_start)
        requested = sorted(
            {day for day in all_weekdays if day >= requested_start}
            | unresolved_failed
        )
        missing_before = set(requested) - existing_dates - confirmed_empty
    else:
        raise ValueError(f"unsupported download mode: {mode}")

    state["last_plan_issues"] = suspicious_issues
    return HistoricalDownloadPlan(
        start=start,
        end=end,
        dates=requested,
        all_weekdays=all_weekdays,
        existing_dates=existing_dates,
        confirmed_empty_dates=confirmed_empty,
        suspicious_dates=suspicious_dates,
        missing_before=missing_before,
        replace_output=replace_output,
        state=state,
    )


def _roc_date_to_iso(value: str) -> str | None:
    text = value.strip()
    if not ROC_DATE_PATTERN.match(text):
        return None
    parts = [int(part) for part in text.split("/")]
    return date(parts[0] + 1911, parts[1], parts[2]).isoformat()


def _format_date(value: date, fmt: str) -> str:
    return value.strftime(fmt)


def _json_payload_status_error(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw_status = payload.get("stat", payload.get("status", ""))
    status = _strip_html(raw_status).strip()
    if not status or status.lower() == "ok":
        return None
    lowered = status.lower()
    if any(marker in lowered for marker in NO_DATA_STATUS_MARKERS):
        return None
    return status


def _text_explicitly_reports_no_data(value: Any) -> bool:
    lowered = _strip_html(value).strip().lower()
    return bool(lowered) and any(marker in lowered for marker in NO_DATA_STATUS_MARKERS)


def _json_payload_explicitly_reports_no_data(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    raw_status = payload.get("stat", payload.get("status", ""))
    return _text_explicitly_reports_no_data(raw_status)


def _declared_dates_from_text(value: Any) -> set[date]:
    text = _strip_html(value)
    declared: set[date] = set()
    compact = text.strip()
    if re.fullmatch(r"\d{8}", compact):
        try:
            declared.add(datetime.strptime(compact, "%Y%m%d").date())
        except ValueError:
            pass
    separated = re.fullmatch(
        r"(\d{2,4})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{1,2})",
        compact,
    )
    if separated:
        try:
            year = int(separated.group(1))
            if year < 1911:
                year += 1911
            declared.add(
                date(year, int(separated.group(2)), int(separated.group(3)))
            )
        except ValueError:
            pass
    for raw_year, raw_month, raw_day in re.findall(
        r"(?<!\d)(\d{2,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        text,
    ):
        try:
            year = int(raw_year)
            if year < 1911:
                year += 1911
            declared.add(date(year, int(raw_month), int(raw_day)))
        except ValueError:
            continue
    return declared


def _json_payload_declared_dates(payload: Any) -> set[date]:
    if not isinstance(payload, dict):
        return set()
    declared = _declared_dates_from_text(payload.get("date", ""))
    declared.update(_declared_dates_from_text(payload.get("title", "")))
    for table in payload.get("tables") or []:
        if isinstance(table, dict):
            declared.update(_declared_dates_from_text(table.get("title", "")))
    return declared


def _validate_json_historical_response_date(
    payload: Any,
    spec: DatasetSpec,
    requested_day: date,
) -> None:
    relevant_table_dates: set[date] = set()
    if isinstance(payload, dict):
        for table in payload.get("tables") or []:
            if not isinstance(table, dict):
                continue
            title = _strip_html(table.get("title", payload.get("title", "")))
            if _table_matches(spec.table_mode, title):
                relevant_table_dates.update(_declared_dates_from_text(title))
                if spec.name == "tpex_day_trade_eligibility":
                    relevant_table_dates.update(
                        _declared_dates_from_text(table.get("date", ""))
                    )

    strict_selected_table_and_payload_binding = spec.name in {
        "twse_daily_ohlcv",
        "twse_market_index",
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    }
    if strict_selected_table_and_payload_binding:
        if not relevant_table_dates:
            raise HistoricalResponseError(
                f"official response selected table declares no date for {spec.name}"
            )
        _validate_historical_response_date(
            spec,
            requested_day,
            relevant_table_dates,
        )

        # Official endpoints have returned bodies whose selected-table title
        # and top-level payload belonged to different sessions.  Treat both as
        # independent mandatory identity checks, never as hints to overwrite.
        top_level_dates = (
            _declared_dates_from_text(payload.get("date", ""))
            if isinstance(payload, dict)
            else set()
        )
        if not top_level_dates:
            raise HistoricalResponseError(
                f"official response top-level payload.date is missing for {spec.name}"
            )
        _validate_historical_response_date(
            spec,
            requested_day,
            top_level_dates,
        )

        raw_params = payload.get("params") if isinstance(payload, dict) else None
        if isinstance(raw_params, dict) and "date" in raw_params:
            parameter_dates = _declared_dates_from_text(raw_params.get("date", ""))
            if not parameter_dates:
                raise HistoricalResponseError(
                    f"official response params.date is invalid for {spec.name}"
                )
            _validate_historical_response_date(
                spec,
                requested_day,
                parameter_dates,
            )
        return

    if relevant_table_dates:
        # For other historical products, the selected table is the declaration
        # actually parsed into the dataset. Keep their contracts scoped until
        # equivalent independent evidence supports a stricter identity rule.
        _validate_historical_response_date(
            spec,
            requested_day,
            relevant_table_dates,
        )
        return

    _validate_historical_response_date(
        spec,
        requested_day,
        _json_payload_declared_dates(payload),
    )


def _json_payload_has_structured_empty_table(
    payload: Any,
    spec: DatasetSpec,
) -> bool:
    if not isinstance(payload, dict):
        return False
    candidates: list[dict[str, Any]] = [payload]
    candidates.extend(
        table for table in (payload.get("tables") or []) if isinstance(table, dict)
    )
    for table in candidates:
        fields = table.get("fields")
        data = table.get("data")
        title = _strip_html(table.get("title", payload.get("title", "")))
        if not isinstance(fields, list) or not fields:
            continue
        if not isinstance(data, list) or data:
            continue
        if _table_matches(spec.table_mode, title):
            return True
    return False


def _validate_historical_response_date(
    spec: DatasetSpec,
    requested_day: date,
    declared_dates: set[date],
) -> None:
    mismatched = sorted(day for day in declared_dates if day != requested_day)
    if mismatched:
        raise HistoricalResponseError(
            f"official response date mismatch for {spec.name}: "
            f"requested={requested_day.isoformat()} "
            f"declared={','.join(day.isoformat() for day in sorted(declared_dates))}"
        )


def _global_tw_public_rate_limiter() -> SharedRateLimiter:
    global _RATE_LIMITER
    if _RATE_LIMITER is None:
        with _RATE_LIMITER_LOCK:
            if _RATE_LIMITER is None:
                interval = resolve_request_interval("tw_public", None)
                _RATE_LIMITER = SharedRateLimiter(interval, name="tw_public")
    return _RATE_LIMITER


def _configure_tw_public_rate_limiter(requested_interval: float | None) -> float:
    global _RATE_LIMITER
    interval = resolve_request_interval("tw_public", requested_interval)
    with _RATE_LIMITER_LOCK:
        _RATE_LIMITER = SharedRateLimiter(interval, name="tw_public")
    return interval


def _http_get(
    url: str,
    *,
    timeout: int,
    verify_ssl: bool,
    params: dict[str, str] | None = None,
    retries: int = 3,
    retry_backoff: float = 1.0,
    retry_security_blocks: bool = True,
) -> requests.Response:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/csv,text/plain,text/javascript,application/xml,text/xml,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.twse.com.tw/",
        "X-Requested-With": "XMLHttpRequest",
    }
    retry_count = max(0, int(retries))
    # TWSE's edge can return a location-less 307 when its rate guard trips.
    # It is a retryable throttle response, not a successful redirect.
    transient_statuses = {307, 403, 408, 429, 500, 502, 503, 504}
    last_error: requests.exceptions.RequestException | None = None
    network_attempts = 0

    def annotate_attempts(response: requests.Response) -> requests.Response:
        try:
            setattr(response, "_stockagent_response_attempts", network_attempts)
        except Exception:
            pass
        return response

    def close_response(response: requests.Response) -> None:
        close = getattr(response, "close", None)
        if callable(close):
            close()

    def request_once(session: requests.Session, *, verify: bool) -> requests.Response:
        started = time.monotonic()
        response = session.get(
            url,
            params=params,
            headers=headers,
            timeout=(timeout, timeout),
            verify=verify,
            stream=True,
        )
        iter_content = getattr(response, "iter_content", None)
        if not callable(iter_content):
            return response
        try:
            chunks: list[bytes] = []
            for chunk in iter_content(chunk_size=1024 * 1024):
                if chunk:
                    chunks.append(chunk)
                if time.monotonic() - started > timeout:
                    raise requests.exceptions.Timeout(
                        f"response exceeded {timeout}s wall timeout: "
                        f"{getattr(response, 'url', url)}"
                    )
            response._content = b"".join(chunks)
            response._content_consumed = True
            return response
        except BaseException:
            close_response(response)
            raise

    for attempt in range(retry_count + 1):
        try:
            limiter = _global_tw_public_rate_limiter()
            limiter.wait()
            session = _http_session()
            try:
                network_attempts += 1
                response = request_once(session, verify=verify_ssl)
            except requests.exceptions.SSLError:
                if not verify_ssl:
                    raise
                requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
                limiter.wait()
                network_attempts += 1
                response = request_once(session, verify=False)
            security_block = _response_is_tw_public_security_block(response)
            if security_block:
                delay = _retry_delay_seconds(response, attempt, retry_backoff)
                limiter.defer(delay)
                time.sleep(delay)
                if not retry_security_blocks or attempt >= retry_count:
                    return annotate_attempts(response)
                close_response(response)
                continue
            if response.status_code in transient_statuses and attempt < retry_count:
                delay = _retry_delay_seconds(response, attempt, retry_backoff)
                limiter.defer(delay)
                time.sleep(delay)
                close_response(response)
                continue
            response.raise_for_status()
            return annotate_attempts(response)
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= retry_count:
                try:
                    setattr(exc, "_stockagent_response_attempts", network_attempts)
                except Exception:
                    pass
                raise
            delay = _retry_delay_seconds(None, attempt, retry_backoff)
            _global_tw_public_rate_limiter().defer(delay)
            time.sleep(delay)

    if last_error is not None:
        try:
            setattr(last_error, "_stockagent_response_attempts", network_attempts)
        except Exception:
            pass
        raise last_error
    raise RuntimeError(f"HTTP request failed without response: {url}")


def _retry_delay_seconds(response: requests.Response | None, attempt: int, retry_backoff: float) -> float:
    retry_after_seconds: float | None = None
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                retry_after_seconds = max(0.0, float(retry_after))
            except ValueError:
                pass
        if _response_is_tw_public_security_block(response):
            return max(
                TW_PUBLIC_WAF_COOLDOWN_SECONDS,
                retry_after_seconds or 0.0,
                max(0.0, float(retry_backoff)) * (2**attempt),
            )
        if retry_after_seconds is not None:
            return min(60.0, retry_after_seconds)
        if response.status_code == 307 and not response.headers.get("Location"):
            return min(120.0, 30.0 * (2**attempt))
    return max(0.0, float(retry_backoff)) * (2**attempt)


def _response_is_tw_public_security_block(response: requests.Response) -> bool:
    if int(response.status_code) not in {307, 403}:
        return False
    content = bytes(getattr(response, "content", b""))[:4096]
    text = content.decode("utf-8", errors="ignore").lower()
    return (
        "for security reasons" in text
        or "page can not be accessed" in text
        or "安全性考量" in text
    )


def _decode_bytes(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "cp950", "big5", "latin1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def _decode_tpex_archive_html(raw: bytes) -> str:
    # The official archive is Big5/CP950. Mixing an edge-injected UTF-8 script
    # or replacement bytes into the receipt destroys names and numeric cells;
    # fail closed so resume refetches the immutable date instead of persisting
    # mojibake or U+FFFD into a parser-versioned partial.
    try:
        return raw.decode("cp950")
    except UnicodeDecodeError as exc:
        raise HistoricalResponseError(
            "official TPEx archive contains lossy Unicode replacement markers"
        ) from exc


def _decode_tpex_daily_archive_html(raw: bytes) -> tuple[str, bool]:
    """Decode legacy quotes while isolating narrowly evidenced permanent damage."""

    try:
        return raw.decode("cp950"), b"\xef\xbf\xbd" in raw
    except UnicodeDecodeError:
        # A fixed set of official 2004 receipts permanently contains replacement
        # bytes in security names and a few byte-pattern-recoverable labels.
        # Symbols, quote values, and every unknown damaged token are validated
        # separately before acceptance.
        return raw.decode("cp950", errors="replace"), True


def _frame_from_records(records: list[dict[str, Any]]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame()
    normalized: list[dict[str, str]] = []
    keys: list[str] = []
    seen: set[str] = set()
    for row in records:
        clean: dict[str, str] = {}
        for key, value in row.items():
            name = _strip_html(key)
            if name not in seen:
                seen.add(name)
                keys.append(name)
            clean[name] = _strip_html(value)
        normalized.append(clean)
    rows = [{key: row.get(key, "") for key in keys} for row in normalized]
    return pl.DataFrame(rows, schema={key: pl.Utf8 for key in keys})


def _append_common_columns(
    frame: pl.DataFrame,
    spec: DatasetSpec,
    *,
    fetched_at: str,
    url: str,
    as_of_date: str | None = None,
    resource: str | None = None,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    expressions = [
        pl.lit(spec.name).alias("_dataset"),
        pl.lit(spec.source).alias("_source"),
        pl.lit(fetched_at).alias("_downloaded_at_utc"),
        pl.lit(url).alias("_url"),
    ]
    if as_of_date is not None and DATE_COLUMN not in frame.columns:
        expressions.append(pl.lit(as_of_date).alias(DATE_COLUMN))
    elif as_of_date is not None:
        expressions.append(pl.lit(as_of_date).alias("_as_of_date"))
    if resource is not None:
        expressions.append(pl.lit(resource).alias("_resource"))
    return frame.with_columns(expressions)


def _parse_json_table_payload(payload: Any, spec: DatasetSpec, request_date: date) -> pl.DataFrame:
    records: list[dict[str, Any]] = []
    iso_date = request_date.isoformat()
    if isinstance(payload, list):
        for idx, row in enumerate(payload):
            if isinstance(row, dict):
                records.append({**row, DATE_COLUMN: row.get(DATE_COLUMN, iso_date), "_row_index": idx})
        return _frame_from_records(records)

    if not isinstance(payload, dict):
        return pl.DataFrame()

    if str(payload.get("stat", "")).upper() not in {"", "OK"} and str(payload.get("stat", "")).lower() != "ok":
        return pl.DataFrame()

    title = _strip_html(payload.get("title", ""))
    fields = payload.get("fields")
    data = payload.get("data")
    if isinstance(fields, list) and isinstance(data, list):
        normalized_fields = _normalize_historical_json_fields(spec, fields)
        _validate_day_trade_json_table_schema(
            spec=spec,
            request_date=request_date,
            fields=normalized_fields,
            data=data,
            container=payload,
            table_index=0,
        )
        rows = _records_from_fields_data(
            fields=normalized_fields,
            data=data,
            iso_date=iso_date,
            table_title=title,
            table_index=0,
        )
        return _frame_from_records(rows)

    tables = payload.get("tables")
    if not isinstance(tables, list):
        return pl.DataFrame()

    for table_index, table in enumerate(tables):
        if not isinstance(table, dict):
            continue
        table_title = _strip_html(table.get("title", title))
        if not _table_matches(spec.table_mode, table_title):
            continue
        table_fields = table.get("fields")
        table_data = table.get("data")
        if not isinstance(table_fields, list) or not isinstance(table_data, list):
            continue
        normalized_fields = _normalize_historical_json_fields(spec, table_fields)
        _validate_day_trade_json_table_schema(
            spec=spec,
            request_date=request_date,
            fields=normalized_fields,
            data=table_data,
            container=table,
            table_index=table_index,
        )
        records.extend(
            _records_from_fields_data(
                fields=normalized_fields,
                data=table_data,
                iso_date=iso_date,
                table_title=table_title,
                table_index=table_index,
            )
        )
    return _frame_from_records(records)


def _normalize_historical_json_fields(
    spec: DatasetSpec,
    fields: list[Any],
) -> list[Any]:
    stripped = tuple(_strip_html(field) for field in fields)
    if (
        spec.name == "tpex_institutional_trades"
        and stripped == TPEX_INSTITUTIONAL_GROUPED_SOURCE_FIELDS
    ):
        return list(TPEX_INSTITUTIONAL_GROUPED_CANONICAL_FIELDS)
    return fields


def _validate_day_trade_json_table_schema(
    *,
    spec: DatasetSpec,
    request_date: date,
    fields: list[Any],
    data: list[Any],
    container: dict[str, Any],
    table_index: int,
) -> None:
    """Validate the raw selected-table shape before absent cells are materialized.

    Empty strings are legitimate official sell-first markers, so a short list
    row must never be padded with ``""``: doing so would turn a missing field
    into legal permission.  Declared totals are also part of the immutable raw
    receipt contract because a syntactically valid partial response is not a
    complete point-in-time eligibility list.
    """

    if spec.name not in DAY_TRADE_ELIGIBILITY_DATASETS:
        return
    normalized_fields = tuple(_strip_html(field) for field in fields)
    if len(normalized_fields) != len(set(normalized_fields)):
        raise HistoricalResponseError(
            f"official response contains duplicate fields for {spec.name} "
            f"table {table_index}"
        )
    required_fields = set(HISTORICAL_REQUIRED_COLUMNS[spec.name])
    sell_first_regime = request_date >= date(2014, 6, 30)
    if sell_first_regime:
        required_fields.add("暫停現股賣出後現款買進當沖註記")
    missing_fields = sorted(required_fields - set(normalized_fields))
    if missing_fields:
        raise HistoricalResponseError(
            f"official response missing required fields for {spec.name}: "
            + ",".join(missing_fields)
        )

    suspension_field = "暫停現股賣出後現款買進當沖註記"
    suspension_index = (
        normalized_fields.index(suspension_field)
        if suspension_field in normalized_fields
        else None
    )
    expected_width = len(normalized_fields)
    for row_index, row in enumerate(data):
        if isinstance(row, list):
            if len(row) != expected_width:
                raise HistoricalResponseError(
                    f"official response row width mismatch for {spec.name} "
                    f"table={table_index} row={row_index}: "
                    f"expected={expected_width} actual={len(row)}"
                )
            if (
                sell_first_regime
                and suspension_index is not None
                and row[suspension_index] is None
            ):
                raise HistoricalResponseError(
                    f"official response contains a null day-trade sell-first "
                    f"suspension marker for {spec.name} table={table_index} "
                    f"row={row_index}"
                )
            continue
        if isinstance(row, dict):
            normalized_row = {_strip_html(key): value for key, value in row.items()}
            row_fields = set(normalized_row)
            missing_row_fields = sorted(required_fields - row_fields)
            if missing_row_fields:
                raise HistoricalResponseError(
                    f"official response dict row missing required fields for "
                    f"{spec.name} table={table_index} row={row_index}: "
                    + ",".join(missing_row_fields)
                )
            if (
                sell_first_regime
                and suspension_field in normalized_row
                and normalized_row[suspension_field] is None
            ):
                raise HistoricalResponseError(
                    f"official response contains a null day-trade sell-first "
                    f"suspension marker for {spec.name} table={table_index} "
                    f"row={row_index}"
                )
            continue
        raise HistoricalResponseError(
            f"official response contains a non-row value for {spec.name} "
            f"table={table_index} row={row_index}"
        )

    for total_key in ("total", "totalCount"):
        if total_key not in container:
            continue
        raw_total = container[total_key]
        if isinstance(raw_total, bool):
            declared_total = None
        elif isinstance(raw_total, int):
            declared_total = raw_total
        elif isinstance(raw_total, float) and raw_total.is_integer():
            declared_total = int(raw_total)
        elif isinstance(raw_total, str) and re.fullmatch(
            r"[0-9]+", raw_total.strip().replace(",", "")
        ):
            declared_total = int(raw_total.strip().replace(",", ""))
        else:
            declared_total = None
        if declared_total is None or declared_total < 0:
            raise HistoricalResponseError(
                f"official response {total_key} is not a non-negative integer "
                f"for {spec.name} table={table_index}: {raw_total!r}"
            )
        if declared_total != len(data):
            raise HistoricalResponseError(
                f"official response {total_key} does not match data rows for "
                f"{spec.name} table={table_index}: "
                f"declared={declared_total} actual={len(data)}"
            )


def _normalize_tpex_price_cell(value: str) -> str:
    # TPEx decorated prices at the upper/lower limit with several generations
    # of glyphs.  The glyph is metadata, not part of the numeric price.
    return re.sub(r"^[♁☉⊕⊙]\s*", "", str(value).strip())


def _tpex_split_change(direction: str, magnitude: str) -> str:
    direction = str(direction).strip()
    magnitude = str(magnitude).strip()
    if direction in {"▽", "-"}:
        return f"-{magnitude}"
    if direction in {"△", "+"}:
        return f"+{magnitude}"
    return magnitude


def _tpex_quote_row_numeric_cell_count(cells: list[str]) -> int:
    count = 0
    for value in cells:
        normalized = re.sub(r"[\s,]", "", _normalize_tpex_price_cell(value))
        try:
            float(normalized)
        except (TypeError, ValueError):
            continue
        count += 1
    return count


def _tpex_name_has_decode_damage(value: str) -> bool:
    text = str(value)
    return "\ufffd" in text or "ï¿½" in text or "嚙" in text


TPEX_DAMAGED_CHANGE_RECOVERY = {
    # These exact renderings retain distinct surviving CP950 byte patterns.
    # In particular, ASCII 0x76 ("v") uniquely distinguishes 權 in the two
    # affected labels; this is a byte-level recovery, not a price-based guess.
    "嚙踝蕭嚙緞": "除權",
    "嚙踝蕭嚙踝蕭": "除息",
    "嚙踝蕭嚙緞嚙踝蕭": "除權息",
}
TPEX_CHANGE_RECOVERY_STATUS = (
    "official_receipt_change_recovered_from_cp950_byte_pattern"
)


def _parse_tpex_daily_quotes_html(
    raw_html: str,
    request_date: date,
    *,
    lossy_name_receipt: bool = False,
) -> pl.DataFrame:
    records: list[dict[str, str]] = []
    for raw_row in HTML_ROW_START_PATTERN.split(raw_html)[1:]:
        raw_row = raw_row.split("</tr>", 1)[0]
        cells = [
            _strip_html(value)
            for value in HTML_CELL_START_PATTERN.split(raw_row)[1:]
        ]
        width = len(cells)
        expected_code_index = 1 if width == 27 else 0
        supported_width = width in {17, 18, 19, 26, 27}
        looks_like_quote = _tpex_quote_row_numeric_cell_count(cells) >= 5
        if looks_like_quote and not supported_width:
            raise HistoricalResponseError(
                f"legacy TPEx daily quote row has unsupported width={width}"
            )
        if supported_width and looks_like_quote:
            candidate_code = cells[expected_code_index].strip().upper()
            if re.fullmatch(r"[0-9A-Z]{4,6}", candidate_code) is None:
                raise HistoricalResponseError(
                    "legacy TPEx daily quote row has a malformed security code: "
                    f"{candidate_code!r}"
                )

        if width == 27 and re.fullmatch(r"[0-9A-Z]{4,6}", cells[1].upper()):
            code, name = cells[1], cells[3]
            (
                close,
                change,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
            ) = (
                cells[5],
                cells[7],
                cells[9],
                cells[11],
                cells[13],
                cells[15],
                cells[17],
                cells[19],
                cells[21],
                cells[23],
                cells[25],
            )
            shares = reference = limit_up = limit_down = ""
        elif width == 26 and re.fullmatch(
            r"[0-9A-Z]{4,6}", cells[0].upper()
        ):
            # The late Oracle report removed the leading spacer but retained
            # an empty cell after each real value.
            code, name = cells[0], cells[2]
            (
                close,
                change,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
            ) = (
                cells[4],
                cells[6],
                cells[8],
                cells[10],
                cells[12],
                cells[14],
                cells[16],
                cells[18],
                cells[20],
                cells[22],
                cells[24],
            )
            shares = reference = limit_up = limit_down = ""
        elif width == 19:
            code, name = cells[0], cells[1]
            change = _tpex_split_change(cells[4], cells[5])
            (
                close,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
                shares,
                reference,
                limit_up,
                limit_down,
            ) = (
                cells[3],
                cells[6],
                cells[7],
                cells[8],
                cells[9],
                cells[10],
                cells[11],
                cells[12],
                cells[13],
                cells[14],
                cells[15],
                cells[16],
                cells[17],
                cells[18],
            )
        elif width == 18 and cells[2].strip() not in {
            "",
            "⊕",
            "⊙",
            "♁",
            "☉",
        }:
            # In the 2004-10-28 generation, the change direction and magnitude
            # occupy separate cells but the close has no leading spacer.
            code, name = cells[0], cells[1]
            change = _tpex_split_change(cells[3], cells[4])
            (
                close,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
                shares,
                reference,
                limit_up,
                limit_down,
            ) = (
                cells[2],
                cells[5],
                cells[6],
                cells[7],
                cells[8],
                cells[9],
                cells[10],
                cells[11],
                cells[12],
                cells[13],
                cells[14],
                cells[15],
                cells[16],
                cells[17],
            )
        elif width == 18:
            # The following generation restored a status/spacer cell before
            # close while keeping the change in one cell.  A status glyph can
            # be present in that spacer, so branch on the change cell rather
            # than assuming it is empty.
            code, name = cells[0], cells[1]
            (
                close,
                change,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
                shares,
                reference,
                limit_up,
                limit_down,
            ) = (
                cells[3],
                cells[4],
                cells[5],
                cells[6],
                cells[7],
                cells[8],
                cells[9],
                cells[10],
                cells[11],
                cells[12],
                cells[13],
                cells[14],
                cells[15],
                cells[16],
                cells[17],
            )
        elif width == 17:
            code, name = cells[0], cells[1]
            (
                close,
                change,
                open_price,
                high,
                low,
                average,
                volume,
                amount,
                trades,
                last_bid,
                last_ask,
                shares,
                reference,
                limit_up,
                limit_down,
            ) = (
                cells[2],
                cells[3],
                cells[4],
                cells[5],
                cells[6],
                cells[7],
                cells[8],
                cells[9],
                cells[10],
                cells[11],
                cells[12],
                cells[13],
                cells[14],
                cells[15],
                cells[16],
            )
        else:
            continue
        if re.fullmatch(r"[0-9A-Z]{4,6}", code.upper()) is None:
            continue
        close, open_price, high, low, average, last_bid, last_ask, reference, limit_up, limit_down = (
            _normalize_tpex_price_cell(value)
            for value in (
                close,
                open_price,
                high,
                low,
                average,
                last_bid,
                last_ask,
                reference,
                limit_up,
                limit_down,
            )
        )
        change_decode_status = ""
        if lossy_name_receipt and change in TPEX_DAMAGED_CHANGE_RECOVERY:
            change = TPEX_DAMAGED_CHANGE_RECOVERY[change]
            change_decode_status = TPEX_CHANGE_RECOVERY_STATUS
        records.append(
            {
                DATE_COLUMN: request_date.isoformat(),
                "代號": code.upper(),
                "名稱": name,
                "收盤": close,
                "漲跌": change,
                "開盤": open_price,
                "最高": high,
                "最低": low,
                "均價": average,
                "成交股數": volume,
                "成交金額(元)": amount,
                "成交筆數": trades,
                "最後買價": last_bid,
                "最後賣價": last_ask,
                "發行股數": shares,
                "次日參考價": reference,
                "次日漲停價": limit_up,
                "次日跌停價": limit_down,
                "_change_decode_status": change_decode_status,
                "_name_decode_status": (
                    "official_receipt_name_bytes_unrecoverable"
                    if lossy_name_receipt or _tpex_name_has_decode_damage(name)
                    else ""
                ),
                "_table_title": "上櫃股票每日收盤行情",
            }
        )
    return _frame_from_records(records)


def _validate_tpex_daily_numeric_cells(
    frame: pl.DataFrame,
    *,
    lossy_name_receipt: bool = False,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    average_sentinels = {"註", "嚙踝蕭"}
    average_text = pl.col("均價").cast(pl.Utf8, strict=False).str.strip_chars()
    sentinel_rows = frame.filter(average_text.is_in(sorted(average_sentinels)))
    for row in sentinel_rows.iter_rows(named=True):
        average = str(row.get("均價", "")).strip()
        if average != "註" and not lossy_name_receipt:
            raise HistoricalResponseError(
                "official TPEx daily average-note damage is not confined to a lossy receipt"
            )

        def zero(column: str) -> bool:
            try:
                return float(str(row.get(column, "")).replace(",", "").strip()) == 0.0
            except (TypeError, ValueError):
                return False

        if not all(
            zero(column)
            for column in (
                "收盤",
                "開盤",
                "最高",
                "最低",
                "成交股數",
                "成交金額(元)",
                "成交筆數",
                "漲跌",
            )
        ):
            raise HistoricalResponseError(
                "official TPEx daily average-note sentinel violates the exact zero-trade gate"
            )

    if sentinel_rows.height:
        frame = frame.with_columns(
            pl.when(average_text.is_in(sorted(average_sentinels)))
            .then(pl.lit("註"))
            .otherwise(pl.col("均價"))
            .alias("均價")
        )

    # Permanently damaged names and the exact v11 recovered change labels have
    # already been isolated with row provenance. Every remaining parsed cell,
    # including unknown change tokens and symbols, stays fail-closed.
    for column in frame.columns:
        if column in {"名稱", "_name_decode_status"}:
            continue
        damaged = [
            value
            for value in frame.get_column(column).cast(pl.Utf8, strict=False).to_list()
            if _tpex_name_has_decode_damage(value)
        ]
        if damaged:
            raise HistoricalResponseError(
                f"official TPEx daily column {column!r} contains lossy decode damage: "
                f"{damaged[:3]}"
            )

    placeholders = ["", "-", "--", "---", "----", "—", "NA", "N/A"]
    numeric_columns = (
        "收盤",
        "開盤",
        "最高",
        "最低",
        "均價",
        "成交股數",
        "成交金額(元)",
        "成交筆數",
        "最後買價",
        "最後賣價",
        "發行股數",
        "次日參考價",
        "次日漲停價",
        "次日跌停價",
    )
    for column in numeric_columns:
        if column not in frame.columns:
            continue
        text = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
        allowed_placeholders = placeholders + (["註"] if column == "均價" else [])
        invalid = frame.filter(
            ~text.is_in(allowed_placeholders)
            & text.str.replace_all(",", "").cast(pl.Float64, strict=False).is_null()
        )
        if invalid.height:
            examples = invalid.get_column(column).head(3).to_list()
            raise HistoricalResponseError(
                f"official TPEx daily numeric column {column!r} contains "
                f"unparseable values: {examples}"
            )

    change = pl.col("漲跌").cast(pl.Utf8, strict=False).str.strip_chars()
    numeric_change = (
        change.str.replace_all(r"\s+", "")
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
    )
    invalid_change = frame.filter(
        ~change.is_in(["---", "除權", "除息", "除權息"])
        & numeric_change.is_null()
    )
    if invalid_change.height:
        examples = invalid_change.get_column("漲跌").head(3).to_list()
        raise HistoricalResponseError(
            "official TPEx daily change column contains unparseable values: "
            f"{examples}"
        )
    return frame


def _tpex_legacy_html_rows(raw_html: str) -> list[list[str]]:
    """Extract malformed legacy TPEx table rows without requiring valid HTML."""

    row_starts = list(HTML_ROW_CAPTURE_PATTERN.finditer(raw_html))
    rows: list[list[str]] = []
    for row_index, row_match in enumerate(row_starts):
        row_end = (
            row_starts[row_index + 1].start()
            if row_index + 1 < len(row_starts)
            else len(raw_html)
        )
        raw_row = raw_html[row_match.end() : row_end].split("</tr>", 1)[0]
        cell_starts = list(HTML_CELL_CAPTURE_PATTERN.finditer(raw_row))
        if not cell_starts:
            continue
        cells: list[tuple[str, str]] = []
        for cell_index, cell_match in enumerate(cell_starts):
            cell_end = (
                cell_starts[cell_index + 1].start()
                if cell_index + 1 < len(cell_starts)
                else len(raw_row)
            )
            cells.append(
                (
                    cell_match.group("attrs").lower(),
                    _strip_html(raw_row[cell_match.end() : cell_end]),
                )
            )
        row_has_body_class = "table-body" in row_match.group("attrs").lower()
        body_cells = [
            value for attrs, value in cells if "table-body" in attrs
        ]
        width_cells = [
            value
            for attrs, value in cells
            if re.search(r"\bwidth\s*=", attrs, re.IGNORECASE)
        ]
        if row_has_body_class:
            selected = [value for _attrs, value in cells]
        elif width_cells and re.fullmatch(
            r"[0-9A-Z]{4,6}",
            width_cells[0].strip().upper(),
        ):
            selected = width_cells
        elif body_cells:
            selected = body_cells
        elif cells and re.fullmatch(
            r"[0-9A-Z]{4,6}",
            cells[0][1].strip().upper(),
        ):
            # The 2004 margin archive has no row/cell CSS markers and a real
            # trailing note cell that is usually blank. Preserve every cell so
            # the note remains in its canonical position.
            selected = [value for _attrs, value in cells]
        else:
            # Some Oracle Report rows omit all CSS classes. Their spacer cells
            # have no width, while every real data cell has one. Keep width
            # cells even when their value is blank so column positions survive.
            selected = width_cells or [value for _attrs, value in cells if value]
        if selected:
            rows.append(selected)
    return rows


def _parse_roc_compact_date(value: str) -> date:
    compact = re.sub(r"\D", "", value)
    if len(compact) not in {6, 7}:
        raise ValueError(f"invalid ROC compact date: {value!r}")
    year = int(compact[:-4]) + 1911
    return date(year, int(compact[-4:-2]), int(compact[-2:]))


def _tpex_archive_declared_dates(raw_html: str, response_kind: str) -> set[date]:
    text = _strip_html_with_tag_boundaries(raw_html)
    if response_kind == "tpex_institutional_archive_html":
        return _declared_dates_from_text(text)
    declared: set[date] = set()
    for compact in re.findall(
        r"(?:資料日期|交易日期)\s*[:：]?\s*(\d{6,7})(?!\d)",
        text,
    ):
        try:
            declared.add(_parse_roc_compact_date(compact))
        except ValueError:
            continue
    for roc_year, month, day in re.findall(
        r"(?:資料日期|交易日期)\s*[:：]?\s*(\d{2,3})\s*年\s*"
        r"(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        text,
    ):
        try:
            declared.add(date(int(roc_year) + 1911, int(month), int(day)))
        except ValueError:
            continue
    if declared:
        return declared

    if response_kind == "archive_html":
        # A fixed set of 2004 daily-quote receipts has permanently damaged
        # Chinese labels, but repeats the compact ROC date in this exact
        # official report header cell.  Bind all matching cells to one
        # consensus date; the caller still requires it to equal the requested
        # date.  The strict attribute signature prevents six-character stock
        # codes in data rows from being interpreted as dates.
        cell_starts = list(HTML_CELL_CAPTURE_PATTERN.finditer(raw_html))
        for cell_index, cell_match in enumerate(cell_starts):
            attrs = cell_match.group("attrs")

            def attr_value(name: str) -> str | None:
                match = re.search(
                    rf"\b{re.escape(name)}\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s>]+))",
                    attrs,
                    flags=re.IGNORECASE,
                )
                if match is None:
                    return None
                return next(
                    (value for value in match.groups() if value is not None),
                    None,
                )

            css_classes = (attr_value("class") or "").lower().split()
            if not (
                attr_value("width") == "71"
                and attr_value("colspan") == "5"
                and attr_value("rowspan") == "2"
                and "table-body-right" in css_classes
            ):
                continue
            cell_end = (
                cell_starts[cell_index + 1].start()
                if cell_index + 1 < len(cell_starts)
                else len(raw_html)
            )
            body = raw_html[cell_match.end() : cell_end]
            compact_tokens = re.findall(
                r"<tt\b[^>]*>\s*(\d{6,7})\s*</tt>",
                body,
                flags=re.IGNORECASE,
            )
            if len(compact_tokens) != 1:
                raise HistoricalResponseError(
                    "official TPEx daily archive has a malformed styled date header"
                )
            try:
                declared.add(_parse_roc_compact_date(compact_tokens[0]))
            except ValueError as exc:
                raise HistoricalResponseError(
                    "official TPEx daily archive has an invalid styled ROC date"
                ) from exc
        if declared:
            return declared

    # The later margin archive renders its ROC date in a standalone header
    # cell without a preceding label. Limit this fallback to explicitly styled
    # header/date cells so six-character security codes are never dates.
    for cell_match in re.finditer(
        r"<td\b(?P<attrs>[^>]*)>(?P<body>.*?)(?=<td\b|</tr>|<tr\b|$)",
        raw_html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        attrs = cell_match.group("attrs").lower()
        if "table-head" not in attrs and "table-date" not in attrs:
            continue
        compact = _strip_html(cell_match.group("body"))
        if re.fullmatch(r"\d{6,7}", compact) is None:
            continue
        try:
            declared.add(_parse_roc_compact_date(compact))
        except ValueError:
            continue
    if declared:
        return declared

    # The first 16-cell margin report generation writes the compact ROC date
    # in the right-hand cell of its unit/date header. Keep the exact attribute
    # signature so a six-character symbol in a data row can never satisfy this
    # fallback.
    if response_kind == "tpex_margin_archive_html":
        for cell_match in re.finditer(
            r"<td\b(?P<attrs>[^>]*)>(?P<body>.*?)(?=<td\b|</tr>|<tr\b|$)",
            raw_html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            attrs = cell_match.group("attrs")

            def has_exact_attr(name: str, expected: str) -> bool:
                match = re.search(
                    rf"\b{re.escape(name)}\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s>]+))",
                    attrs,
                    flags=re.IGNORECASE,
                )
                if match is None:
                    return False
                value = next(
                    (item for item in match.groups() if item is not None),
                    "",
                )
                return value.strip().lower() == expected

            if not (
                has_exact_attr("align", "right")
                and has_exact_attr("valign", "center")
                and has_exact_attr("colspan", "14")
            ):
                continue
            compact = _strip_html(cell_match.group("body"))
            if re.fullmatch(r"\d{6,7}", compact) is None:
                continue
            try:
                declared.add(_parse_roc_compact_date(compact))
            except ValueError:
                continue
    return declared


def _require_tpex_archive_identity(
    spec: DatasetSpec,
    request_date: date,
    raw_html: str,
    response_kind: str,
    title_markers: tuple[str, ...],
) -> str:
    text = _strip_html_with_tag_boundaries(raw_html)
    matched_title = next((title for title in title_markers if title in text), None)
    if matched_title is None:
        raise HistoricalResponseError(
            f"official TPEx archive title mismatch for {spec.name}"
        )
    declared_dates = _tpex_archive_declared_dates(raw_html, response_kind)
    if not declared_dates:
        raise HistoricalResponseError(
            f"official TPEx archive declares no date for {spec.name}"
        )
    _validate_historical_response_date(spec, request_date, declared_dates)
    return matched_title


def _legacy_records_from_rows(
    rows: list[list[str]],
    *,
    allowed_widths: set[int],
) -> list[tuple[str, list[str]]]:
    output: list[tuple[str, list[str]]] = []
    seen_symbols: set[str] = set()
    for cells in rows:
        if not cells:
            continue
        symbol = cells[0].strip().upper()
        if re.fullmatch(r"[0-9A-Z]{4,6}", symbol) is None:
            continue
        if len(cells) not in allowed_widths:
            raise HistoricalResponseError(
                f"legacy TPEx data row has {len(cells)} cells; "
                f"expected one of {sorted(allowed_widths)} for symbol={symbol}"
            )
        if symbol in seen_symbols:
            raise HistoricalResponseError(
                f"legacy TPEx response contains duplicate symbol={symbol}"
            )
        seen_symbols.add(symbol)
        normalized = list(cells)
        normalized[0] = symbol
        output.append((symbol, normalized))
    return output


TPEX_MARGIN_CANONICAL_COLUMNS = (
    "代號",
    "名稱",
    "前資餘額(張)",
    "資買",
    "資賣",
    "現償",
    "資餘額",
    "資屬證金",
    "資使用率(%)",
    "資限額",
    "前券餘額(張)",
    "券賣",
    "券買",
    "券償",
    "券餘額",
    "券屬證金",
    "券使用率(%)",
    "券限額",
    "資券相抵(張)",
    "備註",
)
TPEX_MARGIN_ARCHIVE_COLUMNS_BY_WIDTH = {
    13: (
        "代號",
        "名稱",
        "前資餘額(張)",
        "資買",
        "資賣",
        "現償",
        "資餘額",
        "資限額",
        "前券餘額(張)",
        "券賣",
        "券買",
        "券償",
        "券餘額",
    ),
    16: (
        "代號",
        "名稱",
        "前資餘額(張)",
        "資買",
        "資賣",
        "現償",
        "資餘額",
        "資屬證金",
        "資限額",
        "前券餘額(張)",
        "券賣",
        "券買",
        "券償",
        "券餘額",
        "券屬證金",
        "備註",
    ),
    17: (
        "代號",
        "名稱",
        "前資餘額(張)",
        "資買",
        "資賣",
        "現償",
        "資餘額",
        "資屬證金",
        "資限額",
        "前券餘額(張)",
        "券賣",
        "券買",
        "券償",
        "券餘額",
        "券屬證金",
        "資券相抵(張)",
        "備註",
    ),
}


def _strip_archive_component_parentheses(value: str) -> str:
    stripped = value.strip()
    match = re.fullmatch(r"\(([^()]*)\)", stripped)
    return match.group(1).strip() if match else stripped


def _parse_tpex_margin_archive_html(
    raw_html: str,
    request_date: date,
    spec: DatasetSpec,
) -> pl.DataFrame:
    title = _require_tpex_archive_identity(
        spec,
        request_date,
        raw_html,
        "tpex_margin_archive_html",
        ("融資融券餘額彙總表", "上櫃股票融資融券餘額"),
    )
    parsed_rows = _legacy_records_from_rows(
        _tpex_legacy_html_rows(raw_html),
        allowed_widths=set(TPEX_MARGIN_ARCHIVE_COLUMNS_BY_WIDTH),
    )
    records: list[dict[str, Any]] = []
    for row_index, (_symbol, cells) in enumerate(parsed_rows):
        source_columns = TPEX_MARGIN_ARCHIVE_COLUMNS_BY_WIDTH[len(cells)]
        record = {column: "" for column in TPEX_MARGIN_CANONICAL_COLUMNS}
        record.update(dict(zip(source_columns, cells, strict=True)))
        for column in ("資屬證金", "券屬證金"):
            record[column] = _strip_archive_component_parentheses(record[column])
        record.update(
            {
                DATE_COLUMN: request_date.isoformat(),
                "_table_title": title,
                "_table_index": 0,
                "_row_index": row_index,
            }
        )
        records.append(record)
    return _frame_from_records(records)


TPEX_INSTITUTIONAL_ARCHIVE_COLUMNS = (
    "代號",
    "名稱",
    "外資及陸資買股數",
    "外資及陸資賣股數",
    "外資及陸資淨買股數",
    "投信買進股數",
    "投信賣股數",
    "投信淨買股數",
    "自營商買股數",
    "自營商賣股數",
    "自營淨買股數",
    "三大法人買賣超股數",
)


def _parse_tpex_institutional_archive_html(
    raw_html: str,
    request_date: date,
    spec: DatasetSpec,
) -> pl.DataFrame:
    title = _require_tpex_archive_identity(
        spec,
        request_date,
        raw_html,
        "tpex_institutional_archive_html",
        ("三大法人日交易資訊",),
    )
    parsed_rows = _legacy_records_from_rows(
        _tpex_legacy_html_rows(raw_html),
        allowed_widths={len(TPEX_INSTITUTIONAL_ARCHIVE_COLUMNS)},
    )
    records: list[dict[str, Any]] = []
    for row_index, (_symbol, cells) in enumerate(parsed_rows):
        record = dict(zip(TPEX_INSTITUTIONAL_ARCHIVE_COLUMNS, cells, strict=True))
        record.update(
            {
                DATE_COLUMN: request_date.isoformat(),
                "_table_title": title,
                "_table_index": 0,
                "_row_index": row_index,
            }
        )
        records.append(record)
    return _frame_from_records(records)


TPEX_VALUATION_CANONICAL_COLUMNS = (
    "股票代號",
    "公司名稱",
    "本益比",
    "每股股利",
    "股利年度",
    "殖利率(%)",
    "股價淨值比",
)


def _parse_tpex_valuation_archive_html(
    raw_html: str,
    request_date: date,
    spec: DatasetSpec,
) -> pl.DataFrame:
    title = _require_tpex_archive_identity(
        spec,
        request_date,
        raw_html,
        "tpex_valuation_archive_html",
        ("上櫃股票個股本益比、股利率、股價淨值比",),
    )
    parsed_rows = _legacy_records_from_rows(
        _tpex_legacy_html_rows(raw_html),
        allowed_widths={5},
    )
    records: list[dict[str, Any]] = []
    for row_index, (_symbol, cells) in enumerate(parsed_rows):
        record = {column: "" for column in TPEX_VALUATION_CANONICAL_COLUMNS}
        record.update(
            {
                "股票代號": cells[0],
                "公司名稱": cells[1],
                "本益比": cells[2],
                "殖利率(%)": cells[3],
                "股價淨值比": cells[4],
                DATE_COLUMN: request_date.isoformat(),
                "_table_title": title,
                "_table_index": 0,
                "_row_index": row_index,
            }
        )
        records.append(record)
    return _frame_from_records(records)


def _tpex_historical_request(day: date, spec: DatasetSpec) -> tuple[str, str]:
    roc_date = f"{day.year - 1911}{day:%m%d}"
    if spec.name == "tpex_daily_ohlcv" and day < date(2007, 1, 1):
        return (
            "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/"
            f"RSTA3104_{roc_date}.HTML",
            "archive_html",
        )
    if spec.name == "tpex_daily_ohlcv" and day < date(2007, 7, 2):
        return (
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis"
            f"?date={day:%Y/%m/%d}&response=json",
            "legacy_json_html",
        )
    if spec.name == "tpex_margin_balance" and day < date(2007, 1, 1):
        return (
            "https://hist.tpex.org.tw/Hist/STOCK/MARGIN_TRADING/MARGIN_BALANCE/"
            f"RSTA3106_{roc_date}.html",
            "tpex_margin_archive_html",
        )
    if spec.name == "tpex_daily_valuation" and day < date(2007, 1, 1):
        return (
            "https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/PERATIO_ANALYSIS/"
            f"RSTA3103_{roc_date}.HTML",
            "tpex_valuation_archive_html",
        )
    if spec.name == "tpex_institutional_trades":
        if day < date(2007, 1, 1):
            return (
                "https://hist.tpex.org.tw/Hist/STOCK/3INSTI/DAILY_TRADE/"
                f"BIGD{roc_date}S_N.html",
                "tpex_institutional_archive_html",
            )
        if day <= date(2007, 4, 20):
            return (
                "https://hist.tpex.org.tw/hist/stock/3insti/daily_trade/"
                f"BIGD_{roc_date}S_N.html",
                "tpex_institutional_archive_html",
            )
        if day < date(2014, 12, 1):
            return (
                "https://www.tpex.org.tw/www/zh-tw/insti/dailyTradeHis"
                f"?date={day:%Y/%m/%d}&type=Daily&cate=EW&response=json",
                "json",
            )
    assert spec.url_template is not None
    return spec.url_template.format(date=_format_date(day, spec.date_format)), "json"


def _normalize_historical_frame(
    spec: DatasetSpec,
    frame: pl.DataFrame,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    if spec.name == "tpex_institutional_trades":
        legacy_net = "自營商淨買股數"
        canonical_net = "自營淨買股數"
        columns = set(frame.columns)
        if legacy_net in columns and canonical_net not in columns:
            frame = frame.rename({legacy_net: canonical_net})
        elif legacy_net in columns and canonical_net in columns:
            frame = frame.with_columns(
                pl.coalesce(
                    pl.col(canonical_net),
                    pl.col(legacy_net),
                ).alias(canonical_net)
            ).drop(legacy_net)
    return frame


def _validate_historical_frame(
    spec: DatasetSpec,
    request_date: date,
    frame: pl.DataFrame,
) -> None:
    if frame.is_empty():
        return
    required = HISTORICAL_REQUIRED_COLUMNS.get(spec.name)
    if not required:
        return
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise HistoricalResponseError(
            f"official response missing required fields for {spec.name}: "
            + ",".join(missing)
        )
    if DATE_COLUMN not in frame.columns:
        raise HistoricalResponseError(
            f"official response missing {DATE_COLUMN!r} for {spec.name}"
        )
    dates = {
        str(value)[:10]
        for value in frame.get_column(DATE_COLUMN).to_list()
        if value is not None
    }
    expected_date = request_date.isoformat()
    if dates != {expected_date}:
        raise HistoricalResponseError(
            f"official parsed rows have wrong dates for {spec.name}: "
            f"requested={expected_date} parsed={sorted(dates)}"
        )
    symbol_column = HISTORICAL_SYMBOL_COLUMNS[spec.name]
    symbols = [
        str(value).strip().upper()
        for value in frame.get_column(symbol_column).to_list()
    ]
    invalid_symbols = [
        symbol
        for symbol in symbols
        if re.fullmatch(r"[0-9A-Z]{4,6}", symbol) is None
    ]
    if invalid_symbols:
        raise HistoricalResponseError(
            f"official response contains invalid symbols for {spec.name}: "
            + ",".join(invalid_symbols[:5])
        )
    if len(symbols) != len(set(symbols)):
        raise HistoricalResponseError(
            f"official response contains duplicate symbols for {spec.name}"
        )
    if spec.name in DAY_TRADE_ELIGIBILITY_DATASETS:
        suspension_column = "暫停現股賣出後現款買進當沖註記"
        # Sell-first day trading started on 2014-06-30.  Before that date the
        # official history legitimately omits this field and every security is
        # treated as buy-first-only by the feature builder.  Once the regime is
        # live, absence of the field is ambiguous and must fail closed.
        if request_date >= date(2014, 6, 30) and suspension_column not in frame.columns:
            raise HistoricalResponseError(
                f"official response missing {suspension_column!r} for {spec.name}"
            )
        if suspension_column in frame.columns:
            markers = {
                str(value or "").strip().upper()
                for value in frame.get_column(suspension_column).to_list()
            }
            allowed_markers = {"", "Y", "*", "＊"}
            unknown_markers = sorted(markers - allowed_markers)
            if unknown_markers:
                raise HistoricalResponseError(
                    "official response contains unknown day-trade sell-first "
                    f"suspension markers for {spec.name}: {unknown_markers[:5]}"
                )


def _table_matches(table_mode: str, title: str) -> bool:
    if not table_mode or table_mode == "all":
        return True
    if table_mode.startswith("title_contains:"):
        needle = table_mode.split(":", 1)[1]
        return needle in title
    return True


def _records_from_fields_data(
    *,
    fields: list[Any],
    data: list[Any],
    iso_date: str,
    table_title: str,
    table_index: int,
) -> list[dict[str, Any]]:
    columns = _make_unique([str(field) for field in fields])
    records: list[dict[str, Any]] = []
    for row_index, row in enumerate(data):
        if isinstance(row, dict):
            record = dict(row)
        elif isinstance(row, list):
            record = {column: row[idx] if idx < len(row) else "" for idx, column in enumerate(columns)}
        else:
            continue
        record[DATE_COLUMN] = iso_date
        record["_table_title"] = table_title
        record["_table_index"] = table_index
        record["_row_index"] = row_index
        records.append(record)
    return records


def _parse_json_bytes(raw: bytes) -> pl.DataFrame:
    text, _encoding = _decode_bytes(raw)
    payload = json.loads(text)
    if isinstance(payload, list):
        return _frame_from_records([row for row in payload if isinstance(row, dict)])
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return _frame_from_records([row for row in payload["data"] if isinstance(row, dict)])
        if isinstance(payload.get("Data"), list):
            return _frame_from_records([row for row in payload["Data"] if isinstance(row, dict)])
        return _frame_from_records([payload])
    return pl.DataFrame()


def _parse_csv_bytes(raw: bytes) -> pl.DataFrame:
    text, _encoding = _decode_bytes(raw)
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        separator = dialect.delimiter
    except csv.Error:
        separator = ","
    try:
        frame = pl.read_csv(
            io.StringIO(text),
            separator=separator,
            infer_schema_length=0,
            ignore_errors=True,
        )
    except Exception:
        rows = list(csv.reader(io.StringIO(text), delimiter=separator))
        if not rows:
            return pl.DataFrame()
        header = _make_unique(rows[0])
        records = [{header[idx]: value for idx, value in enumerate(row[: len(header)])} for row in rows[1:]]
        frame = _frame_from_records(records)
    output_columns = _make_unique([_strip_polars_duplicate_suffix(_strip_html(column)) for column in frame.columns])
    return frame.select(
        [
            pl.col(column).cast(pl.Utf8, strict=False).alias(output_columns[idx])
            for idx, column in enumerate(frame.columns)
        ]
    )


def _strip_polars_duplicate_suffix(column: str) -> str:
    return re.sub(r"_duplicated_\d+$", "", column)


def _parse_xml_bytes(raw: bytes) -> pl.DataFrame:
    text, _encoding = _decode_bytes(raw)
    root = ET.fromstring(text.encode("utf-8"))
    records = _xml_records(root)
    return _frame_from_records(records)


def _xml_records(root: ET.Element) -> list[dict[str, Any]]:
    parent_groups: dict[str, list[ET.Element]] = {}
    for element in root.iter():
        children = [child for child in list(element) if isinstance(child.tag, str)]
        if len(children) < 2:
            continue
        leaf_children = [child for child in children if not list(child)]
        if len(leaf_children) < 2:
            continue
        tag = _local_name(element.tag)
        parent_groups.setdefault(tag, []).append(element)

    candidates = sorted(parent_groups.values(), key=lambda elems: (len(elems), len(list(elems[0]))), reverse=True)
    if candidates:
        rows = [_flatten_xml_record(element) for element in candidates[0]]
        if rows:
            return rows

    rows: list[dict[str, Any]] = []
    for element in root.iter():
        if list(element):
            continue
        text = (element.text or "").strip()
        if text:
            rows.append({"path": _local_name(element.tag), "value": text})
    return rows


def _local_name(tag: Any) -> str:
    text = str(tag)
    if "}" in text:
        return text.rsplit("}", 1)[1]
    return text


def _flatten_xml_record(element: ET.Element) -> dict[str, Any]:
    record: dict[str, Any] = {}
    for child in list(element):
        name = _local_name(child.tag)
        if list(child):
            for key, value in _flatten_xml_record(child).items():
                record[f"{name}.{key}"] = value
        else:
            record[name] = (child.text or "").strip()
    record.update({f"@{_local_name(key)}": value for key, value in element.attrib.items()})
    return record


def _parse_text_bytes(raw: bytes) -> pl.DataFrame:
    text, _encoding = _decode_bytes(raw)
    nonempty = [line for line in text.splitlines() if line.strip()]
    if not nonempty:
        return pl.DataFrame()
    if any("," in line for line in nonempty[:10]):
        try:
            return _parse_csv_bytes(raw)
        except Exception:
            pass
    return pl.DataFrame({"line_number": list(range(1, len(nonempty) + 1)), "line": nonempty})


def _parse_resource_bytes(raw: bytes, *, url: str, resource_format: str | None = None) -> pl.DataFrame:
    fmt = (resource_format or "").strip().lower()
    path = urlparse(url).path.lower()
    if fmt == "json" or path.endswith(".json"):
        return _parse_json_bytes(raw)
    if fmt == "xml" or path.endswith(".xml"):
        return _parse_xml_bytes(raw)
    if fmt == "csv" or path.endswith(".csv"):
        return _parse_csv_bytes(raw)
    if fmt in {"txt", "text"} or path.endswith(".txt"):
        return _parse_text_bytes(raw)
    if fmt in {"zip", "compress file", "壓縮檔"} or path.endswith(".zip"):
        return _parse_zip_bytes(raw)
    content_start = raw[:256].lstrip()
    if content_start.startswith(b"{") or content_start.startswith(b"["):
        return _parse_json_bytes(raw)
    if content_start.startswith(b"<"):
        return _parse_xml_bytes(raw)
    return _parse_text_bytes(raw)


def _parse_zip_bytes(raw: bytes) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        for member in archive.namelist():
            if member.endswith("/"):
                continue
            lower = member.lower()
            if not lower.endswith((".csv", ".json", ".xml", ".txt")):
                continue
            frame = _parse_resource_bytes(archive.read(member), url=member)
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(member).alias("_archive_member")))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _write_raw(raw: bytes, raw_dir: Path, dataset: str, suffix: str, stem: str | None = None) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    base = _safe_name(stem or dataset)
    path = raw_dir / f"{base}{suffix}"
    with tempfile.NamedTemporaryFile(dir=raw_dir, delete=False) as handle:
        handle.write(raw)
        tmp_path = Path(handle.name)
    os.replace(tmp_path, path)
    return path


def _write_immutable_raw(
    raw: bytes,
    raw_dir: Path,
    dataset: str,
    suffix: str,
    stem: str | None = None,
) -> Path:
    """Atomically create a raw receipt without ever replacing prior bytes."""

    raw_dir.mkdir(parents=True, exist_ok=True)
    base = _safe_name(stem or dataset)
    path = raw_dir / f"{base}{suffix}"
    if path.exists():
        if path.read_bytes() != raw:
            raise HistoricalResponseError(
                f"immutable raw receipt changed for {dataset} {base}"
            )
        return path
    with tempfile.NamedTemporaryFile(dir=raw_dir, delete=False) as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
        tmp_path = Path(handle.name)
    try:
        try:
            os.link(tmp_path, path)
        except FileExistsError:
            if path.read_bytes() != raw:
                raise HistoricalResponseError(
                    f"immutable raw receipt changed for {dataset} {base}"
                )
    finally:
        tmp_path.unlink(missing_ok=True)
    return path


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _write_csv_report(path: Path, rows: list[DownloadResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(DownloadResult("", "", 0, None).__dataclass_fields__)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    os.replace(tmp, path)


def _read_existing(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _read_existing_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)
    except Exception:
        return int(_read_existing(path).height)


def _merge_frames(existing: pl.DataFrame, incoming: pl.DataFrame, *, refresh: bool) -> pl.DataFrame:
    if refresh or existing.is_empty():
        return incoming
    if incoming.is_empty():
        return existing
    key_columns = [column for column in (DATE_COLUMN, "_dataset", "_resource", "_table_index", "_row_index") if column in incoming.columns]
    if DATE_COLUMN in incoming.columns and DATE_COLUMN in existing.columns:
        incoming_dates = incoming.select(pl.col(DATE_COLUMN).unique()).to_series().to_list()
        kept = existing.filter(~pl.col(DATE_COLUMN).is_in(incoming_dates))
        return pl.concat([kept, incoming], how="diagonal_relaxed").sort(DATE_COLUMN)
    if key_columns and all(column in existing.columns for column in key_columns):
        incoming_keys = incoming.select(key_columns).unique()
        kept = existing.join(incoming_keys, on=key_columns, how="anti")
        return pl.concat([kept, incoming], how="diagonal_relaxed")
    return incoming


def _write_parquet_merged(path: Path, frame: pl.DataFrame, *, refresh: bool) -> int:
    if frame.is_empty():
        return _read_existing_row_count(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    merged = _merge_frames(_read_existing(path), frame, refresh=refresh)
    tmp = path.with_suffix(path.suffix + ".tmp")
    merged.write_parquet(tmp, compression="snappy", statistics=True)
    os.replace(tmp, path)
    return int(merged.height)


def _copy_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        delete=False,
    ) as handle:
        tmp_path = Path(handle.name)
    try:
        shutil.copyfile(source, tmp_path)
        os.replace(tmp_path, destination)
    finally:
        tmp_path.unlink(missing_ok=True)


def _bootstrap_historical_partial_from_raw(
    spec: DatasetSpec,
    output_dir: Path,
    cache: HistoricalResumeCache,
    allowed_dates: set[date],
) -> None:
    raw_dir = output_dir / "raw" / spec.name
    try:
        existing_counts, _ = _existing_date_counts(cache.partial_path)
    except Exception:
        existing_counts = {}
    existing_dates = set(existing_counts)
    pending_frames: list[pl.DataFrame] = []
    pending_results: list[tuple[HistoricalDateResult, str]] = []

    def flush() -> None:
        nonlocal pending_frames, pending_results
        if not pending_frames:
            return
        incoming = pl.concat(pending_frames, how="diagonal_relaxed")
        if DATE_COLUMN in incoming.columns:
            incoming = incoming.sort(DATE_COLUMN)
        _write_parquet_merged(cache.partial_path, incoming, refresh=False)
        for cached_result, source in pending_results:
            _append_historical_journal_record(
                cache,
                spec,
                cached_result,
                status="data",
                source=source,
            )
        pending_frames = []
        pending_results = []

    if raw_dir.is_dir():
        for raw_path in sorted(raw_dir.iterdir()):
            if raw_path.suffix.lower() not in {".json", ".html"}:
                continue
            try:
                day = _parse_date(raw_path.stem[:10])
            except ValueError:
                continue
            if day not in allowed_dates or day in existing_dates:
                continue
            url, response_kind = _historical_request_info(spec, day)
            try:
                frame, _ = _parse_historical_response_content(
                    spec,
                    day,
                    raw_path.read_bytes(),
                    response_kind,
                )
            except Exception:
                # A corrupt or parser-incompatible receipt is not trusted. The date
                # remains unresolved and will be fetched again from the official source.
                continue
            if frame.is_empty():
                continue
            fetched_at = datetime.fromtimestamp(
                raw_path.stat().st_mtime,
                tz=timezone.utc,
            ).isoformat()
            staged = _append_common_columns(
                frame,
                spec,
                fetched_at=fetched_at,
                url=url,
            )
            result = HistoricalDateResult(
                day=day,
                url=url,
                frame=frame,
                raw_path=str(raw_path),
            )
            pending_frames.append(staged)
            pending_results.append((result, "raw_bootstrap"))
            existing_dates.add(day)
            if len(pending_frames) >= 250:
                flush()
    flush()

    # A parser-contract bump can make a previously rejected HTTP-200 receipt
    # safe to parse. Reuse it only after its append-only journal event, official
    # URL, content-addressed filename, byte length, and full hashes all agree.
    for day, payload, raw_path in _validated_historical_failed_receipts(
        output_dir,
        spec,
        allowed_dates,
    ):
        if day in existing_dates:
            continue
        try:
            content = raw_path.read_bytes()
        except OSError:
            continue
        digest = hashlib.sha256(content).hexdigest()
        if (
            payload.get("raw_sha256") != digest
            or payload.get("body_sha256") != digest
            or int(payload.get("raw_size", -1)) != len(content)
            or int(payload.get("content_length", -1)) != len(content)
        ):
            continue
        url, response_kind = _historical_request_info(spec, day)
        receipt_url = str(payload.get("url") or url)
        try:
            frame, _ = _parse_historical_response_content(
                spec,
                day,
                content,
                response_kind,
            )
            fetched_at = str(payload["recorded_at_utc"])
            datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
            response_attempts = int(payload.get("response_attempts", 0))
        except Exception:
            # The receipt remains a failure under this parser contract and its
            # date stays in the network request plan.
            continue
        if frame.is_empty():
            continue
        staged = _append_common_columns(
            frame,
            spec,
            fetched_at=fetched_at,
            url=receipt_url,
        )
        result = HistoricalDateResult(
            day=day,
            url=receipt_url,
            frame=frame,
            raw_path=str(raw_path),
            http_status=200,
            content_type=payload.get("content_type"),
            content_length=len(content),
            body_sha256=digest,
            body_snippet=payload.get("body_snippet"),
            response_attempts=response_attempts,
        )
        pending_frames.append(staged)
        pending_results.append((result, "raw_failure_reparse"))
        existing_dates.add(day)
        if len(pending_frames) >= 250:
            flush()
    flush()


def _prepare_historical_resume_cache(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
    plan: HistoricalDownloadPlan,
) -> HistoricalResumeCache:
    cache_key = _historical_resume_cache_key(spec)
    resume_enabled = bool(getattr(args, "resume", True))
    if not resume_enabled:
        cache_key = f"{cache_key}-fresh-{os.getpid()}-{time.time_ns()}"
    cache = HistoricalResumeCache(
        cache_key=cache_key,
        journal_path=_historical_journal_path(output_dir, spec),
        partial_path=_historical_partial_path(output_dir, spec, cache_key),
        data_dates=set(),
        empty_dates=set(),
    )
    plan.state.update(
        {
            "resume_enabled": resume_enabled,
            "resume_cache_key": cache.cache_key,
            "journal_path": str(cache.journal_path.relative_to(output_dir)),
            "partial_path": str(cache.partial_path.relative_to(output_dir)),
        }
    )
    if not plan.replace_output:
        if args.max_dates is not None:
            plan.dates = plan.dates[: max(0, int(args.max_dates))]
        return cache

    if not resume_enabled:
        if args.max_dates is not None:
            plan.dates = plan.dates[: max(0, int(args.max_dates))]
        return cache

    _bootstrap_historical_partial_from_raw(
        spec,
        output_dir,
        cache,
        plan.all_weekdays,
    )
    try:
        partial_counts, _ = _existing_date_counts(cache.partial_path)
    except Exception:
        partial_counts = {}
    if _requires_strict_session_calendar(spec, args):
        partial_in_range = {
            day
            for day in partial_counts
            if plan.start <= day <= plan.end
        }
        calendar_disagreement = partial_in_range - plan.all_weekdays
        if calendar_disagreement:
            examples = ", ".join(
                day.isoformat() for day in sorted(calendar_disagreement)[:5]
            )
            raise RuntimeError(
                f"{spec.name} staged partial dates disagree with its verified "
                f"session calendar: count={len(calendar_disagreement)} "
                f"examples={examples}"
            )
    latest = _load_historical_journal_latest(
        cache.journal_path,
        spec,
        cache.cache_key,
    )
    latest_failed = {
        day
        for day, payload in latest.items()
        if payload.get("status") == "failed"
    }
    cached_data = (set(partial_counts) & plan.all_weekdays) - latest_failed
    cached_empty = {
        day
        for day, payload in latest.items()
        if payload.get("status") == "empty" and day in plan.all_weekdays
    }
    if _requires_strict_session_calendar(spec, args):
        cached_empty &= _validated_source_unavailable_receipt_dates(
            output_dir,
            spec,
            plan.all_weekdays,
        )
    cached_empty.difference_update(cached_data)

    try:
        suspicious_cached, _ = _suspicious_ohlcv_dates(
            cache.partial_path,
            spec,
            partial_counts,
        )
    except Exception:
        suspicious_cached = set(partial_counts)
    suspicious_cached &= plan.all_weekdays
    plan.suspicious_dates |= suspicious_cached
    cached_data.difference_update(suspicious_cached)

    cache.data_dates = cached_data
    cache.empty_dates = cached_empty
    plan.state["resumed_data_dates"] = len(cached_data)
    plan.state["resumed_empty_dates"] = len(cached_empty)
    resolved = cached_data | cached_empty
    plan.dates = [
        day
        for day in plan.dates
        if day not in resolved or day in suspicious_cached
    ]
    if args.max_dates is not None:
        plan.dates = plan.dates[: max(0, int(args.max_dates))]
    return cache


def _validate_historical_partial_for_publish(
    spec: DatasetSpec,
    partial_path: Path,
    all_weekdays: set[date],
    confirmed_empty_dates: set[date],
) -> None:
    counts, invalid_date_values = _existing_date_counts(partial_path)
    partial_dates = set(counts)
    expected_data_dates = all_weekdays - confirmed_empty_dates
    missing = expected_data_dates - partial_dates
    unexpected = partial_dates - all_weekdays
    issues: list[str] = []
    if invalid_date_values:
        issues.append(f"invalid_date_values={len(invalid_date_values)}")
    if missing:
        issues.append(f"missing_data_dates={len(missing)}")
    if unexpected:
        issues.append(f"unexpected_dates={len(unexpected)}")
    suspicious, suspicious_issues = _suspicious_ohlcv_dates(
        partial_path,
        spec,
        counts,
    )
    suspicious &= all_weekdays
    if suspicious:
        issues.append(f"suspicious_dates={len(suspicious)}")
    issues.extend(suspicious_issues)
    if issues:
        raise RuntimeError(
            f"staged historical partial failed publish validation for {spec.name}: "
            + "; ".join(issues)
        )


def _latest_existing_date(path: Path) -> date | None:
    if not path.exists():
        return None
    try:
        metadata = pq.read_metadata(path)
        schema = metadata.schema.to_arrow_schema()
        date_idx = schema.get_field_index(DATE_COLUMN)
        if date_idx >= 0:
            latest: date | None = None
            for row_group_idx in range(metadata.num_row_groups):
                stats = metadata.row_group(row_group_idx).column(date_idx).statistics
                if stats is None or not bool(getattr(stats, "has_min_max", False)):
                    continue
                value = stats.max
                if isinstance(value, datetime):
                    parsed = value.date()
                elif isinstance(value, date):
                    parsed = value
                else:
                    if isinstance(value, bytes):
                        value = value.decode("utf-8", errors="ignore")
                    parsed = _parse_date(str(value)[:10])
                latest = parsed if latest is None else max(latest, parsed)
            if latest is not None:
                return latest
    except Exception:
        pass
    try:
        frame = pl.scan_parquet(path).select(pl.col(DATE_COLUMN).max()).collect()
    except Exception:
        return None
    if frame.is_empty():
        return None
    value = frame.item()
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return _parse_date(text)
    except ValueError:
        return None


def _finish_historical_coverage_state(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
    plan: HistoricalDownloadPlan,
    *,
    data_dates: set[date],
    empty_dates: set[date],
    errors: dict[date, str],
    replacement_promoted: bool,
) -> tuple[bool, int]:
    mode = _canonical_mode(args.mode)
    if plan.replace_output:
        # For a failed rebuild, report the durable staged coverage rather than
        # pretending every successful date was lost. Production remains untouched
        # until replacement_promoted is true.
        existing_after = set(data_dates)
        confirmed_empty = set(empty_dates)
    else:
        existing_after = set(plan.existing_dates) | set(data_dates)
        confirmed_empty = set(plan.confirmed_empty_dates)
        confirmed_empty.update(empty_dates)
    confirmed_empty.difference_update(existing_after)
    confirmed_empty.difference_update(errors)
    confirmed_source_unavailable = {
        day
        for day in confirmed_empty
        if _known_source_unavailable_reason(spec, day) is not None
    }
    last_source_unavailable = {
        day
        for day in empty_dates
        if _known_source_unavailable_reason(spec, day) is not None
    }

    missing_after = plan.all_weekdays - existing_after - confirmed_empty
    state = dict(plan.state)
    stored_failed_dates = state.get("failed_dates", {})
    if not isinstance(stored_failed_dates, dict):
        stored_failed_dates = {}
    failed_dates: dict[str, Any] = {}
    pruned_failed_date_keys: list[str] = []
    for raw_day, message in stored_failed_dates.items():
        raw_key = str(raw_day)
        try:
            day = _parse_date(raw_key[:10])
        except ValueError:
            pruned_failed_date_keys.append(raw_key)
            continue
        if day not in plan.all_weekdays:
            pruned_failed_date_keys.append(raw_key)
            continue
        failed_dates[day.isoformat()] = message
    for day in data_dates | empty_dates:
        failed_dates.pop(day.isoformat(), None)
    for day, message in errors.items():
        if day not in plan.all_weekdays:
            pruned_failed_date_keys.append(day.isoformat())
            continue
        failed_dates[day.isoformat()] = message
    unresolved_failed = _state_failed_date_set(
        {"failed_dates": failed_dates}
    ) & plan.all_weekdays
    unresolved_after = missing_after | unresolved_failed
    resume_coverage_complete = not unresolved_after
    full_coverage = resume_coverage_complete and (
        not plan.replace_output or replacement_promoted
    )
    publication_pending = (
        plan.replace_output and not replacement_promoted and not unresolved_after
    )
    previous_baseline = bool(state.get("baseline_established"))
    baseline_established = previous_baseline or (
        mode in {"rebuild", "repair"} and full_coverage
    )
    try:
        prior_pruned_failed_dates = max(
            0, int(state.get("pruned_failed_dates_total", 0))
        )
    except (TypeError, ValueError):
        prior_pruned_failed_dates = 0
    pruned_failed_date_keys = sorted(set(pruned_failed_date_keys))
    state.update(
        {
            "schema_version": COVERAGE_STATE_SCHEMA_VERSION,
            "dataset": spec.name,
            "updated_at_utc": _now_utc(),
            "last_mode": mode,
            "coverage_start": plan.start.isoformat(),
            "coverage_end": plan.end.isoformat(),
            "baseline_established": baseline_established,
            "coverage_complete": full_coverage,
            "resume_coverage_complete": resume_coverage_complete,
            "replacement_promoted": replacement_promoted,
            "confirmed_empty_dates": sorted(day.isoformat() for day in confirmed_empty),
            "confirmed_source_unavailable_dates": sorted(
                day.isoformat() for day in confirmed_source_unavailable
            ),
            "confirmed_empty_date_accounting": {
                "total": len(confirmed_empty),
                "source_unavailable": len(confirmed_source_unavailable),
                "other_confirmed_no_data": len(
                    confirmed_empty - confirmed_source_unavailable
                ),
            },
            "source_unavailable_ranges": (
                _known_source_unavailable_range_summaries(
                    spec,
                    plan.start,
                    plan.end,
                    confirmed_dates=confirmed_source_unavailable,
                    expected_dates=plan.all_weekdays,
                )
            ),
            "failed_dates": dict(sorted(failed_dates.items())),
            "last_pruned_failed_dates": len(pruned_failed_date_keys),
            "last_pruned_failed_date_examples": pruned_failed_date_keys[:10],
            "pruned_failed_dates_total": (
                prior_pruned_failed_dates + len(pruned_failed_date_keys)
            ),
            "last_requested_dates": len(plan.dates),
            "last_fetched_dates": len(data_dates),
            "last_empty_dates": len(empty_dates),
            "last_source_unavailable_dates": len(last_source_unavailable),
            "last_failed_dates": len(errors),
            "missing_dates_after": len(unresolved_after) + int(publication_pending),
            "staged_dates": len(existing_after),
            "publication_pending": publication_pending,
        }
    )
    if full_coverage:
        state["checked_through"] = plan.end.isoformat()
    _write_json(_coverage_state_path(output_dir, spec), state)
    return full_coverage, len(unresolved_after) + int(publication_pending)


def _historical_request_info(spec: DatasetSpec, day: date) -> tuple[str, str]:
    assert spec.url_template is not None
    if spec.name == TPEX_OFFICIAL_CALENDAR_DATASET or spec.name in TPEX_SESSION_DEPENDENT_DATASETS:
        return _tpex_historical_request(day, spec)
    return (
        spec.url_template.format(date=_format_date(day, spec.date_format)),
        "json",
    )


def _historical_response_fallback_url(spec: DatasetSpec, primary_url: str) -> str | None:
    replacements = {
        "twse_daily_ohlcv": (
            "/rwd/zh/afterTrading/MI_INDEX",
            "/exchangeReport/MI_INDEX",
        ),
        "twse_market_index": (
            "/rwd/zh/afterTrading/MI_INDEX",
            "/exchangeReport/MI_INDEX",
        ),
        "twse_daily_valuation": (
            "/rwd/zh/afterTrading/BWIBBU_d",
            "/exchangeReport/BWIBBU_d",
        ),
        "twse_institutional_trades": (
            "/rwd/zh/fund/T86",
            "/fund/T86",
        ),
        "twse_margin_balance": (
            "/rwd/zh/marginTrading/MI_MARGN",
            "/exchangeReport/MI_MARGN",
        ),
    }
    replacement = replacements.get(spec.name)
    if replacement is None:
        return None
    fallback_url = primary_url.replace(*replacement, 1)
    return fallback_url if fallback_url != primary_url else None


def _historical_cache_busted_url(url: str) -> str:
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}_={time.time_ns()}"


def _parse_historical_response_content(
    spec: DatasetSpec,
    day: date,
    content: bytes,
    response_kind: str,
) -> tuple[pl.DataFrame, str]:
    if not content:
        raise HistoricalResponseError("official response body is empty")
    if response_kind in TPEX_LEGACY_HTML_RESPONSE_KINDS:
        decoded = _decode_tpex_archive_html(content)
        if response_kind == "tpex_margin_archive_html":
            frame = _parse_tpex_margin_archive_html(decoded, day, spec)
        elif response_kind == "tpex_institutional_archive_html":
            frame = _parse_tpex_institutional_archive_html(decoded, day, spec)
        else:
            frame = _parse_tpex_valuation_archive_html(decoded, day, spec)
        frame = _normalize_historical_frame(spec, frame)
        if frame.is_empty():
            raise HistoricalResponseError(
                "official TPEx archive produced no rows on a validated open session"
            )
        _validate_historical_frame(spec, day, frame)
        return frame, ".html"
    if response_kind == "archive_html":
        decoded, lossy_name_receipt = _decode_tpex_daily_archive_html(content)
        declared_dates = _declared_dates_from_text(decoded) | _tpex_archive_declared_dates(
            decoded,
            response_kind,
        )
        if not declared_dates:
            raise HistoricalResponseError(
                f"official TPEx archive declares no date for {spec.name}"
            )
        _validate_historical_response_date(
            spec,
            day,
            declared_dates,
        )
        frame = _parse_tpex_daily_quotes_html(
            decoded,
            day,
            lossy_name_receipt=lossy_name_receipt,
        )
        if frame.is_empty() and not _text_explicitly_reports_no_data(decoded):
            raise HistoricalResponseError(
                "official HTML response did not explicitly report no data but parser produced no rows"
            )
        frame = _validate_tpex_daily_numeric_cells(
            frame,
            lossy_name_receipt=lossy_name_receipt,
        )
        return frame, ".html"

    decoded, _ = _decode_bytes(content)
    try:
        payload = json.loads(decoded)
    except (json.JSONDecodeError, TypeError) as exc:
        raise HistoricalResponseError(
            f"official response is not valid JSON: {exc}"
        ) from exc
    _validate_json_historical_response_date(payload, spec, day)
    explicit_no_data = _json_payload_explicitly_reports_no_data(
        payload
    ) or _json_payload_has_structured_empty_table(payload, spec)
    status_error = _json_payload_status_error(payload)
    if status_error is not None:
        raise HistoricalResponseError(
            f"official response status is not OK: {status_error}"
        )
    if response_kind == "legacy_json_html":
        raw_html = str(payload.get("html", ""))
        frame = _parse_tpex_daily_quotes_html(raw_html, day)
        frame = _validate_tpex_daily_numeric_cells(frame)
        explicit_no_data = explicit_no_data or _text_explicitly_reports_no_data(raw_html)
    else:
        frame = _parse_json_table_payload(payload, spec, day)
    frame = _normalize_historical_frame(spec, frame)
    if frame.is_empty() and not explicit_no_data:
        raise HistoricalResponseError(
            "official JSON response did not explicitly report no data but parser produced no rows"
        )
    if frame.is_empty() and spec.name in DAY_TRADE_ELIGIBILITY_DATASETS:
        # Both exchanges have a nonempty eligible universe on every validated
        # cash-equity session from the 2014-01-06 regime start.  Accepting an
        # empty table here would turn a stale/error response into a false
        # complete receipt and later make the strategy silently untradeable.
        raise HistoricalResponseError(
            "official day-trade eligibility report returned no rows on a "
            "validated open session"
        )
    if (
        frame.is_empty()
        and spec.name in TPEX_SESSION_DEPENDENT_DATASETS
        and _known_source_unavailable_reason(spec, day) is None
    ):
        raise HistoricalResponseError(
            "official TPEx report returned no rows on a validated open session"
        )
    _validate_historical_frame(spec, day, frame)
    return frame, ".json"


def _historical_response_audit(
    response: requests.Response,
    *,
    response_attempts: int,
) -> dict[str, Any]:
    content = response.content
    content_type = str(
        response.headers.get("Content-Type", response.headers.get("content-type", ""))
    ).strip()
    decoded, _ = _decode_bytes(content[:1024])
    snippet = re.sub(r"\s+", " ", decoded).strip()[:256] or None
    return {
        "http_status": int(response.status_code),
        "content_type": content_type or None,
        "content_length": len(content),
        "body_sha256": hashlib.sha256(content).hexdigest(),
        "body_snippet": snippet,
        "response_attempts": int(response_attempts),
    }


def _historical_failure_raw_suffix(
    response: requests.Response,
    response_kind: str,
) -> str:
    content_type = str(
        response.headers.get("Content-Type", response.headers.get("content-type", ""))
    ).lower()
    content_start = response.content.lstrip()[:32].lower()
    if (
        response_kind == "archive_html"
        or response_kind in TPEX_LEGACY_HTML_RESPONSE_KINDS
        or "html" in content_type
        or content_start.startswith(b"<")
    ):
        return ".html"
    if "json" in content_type or response_kind in {"json", "legacy_json_html"}:
        return ".json"
    return ".bin"


def _download_historical_date(
    spec: DatasetSpec,
    day: date,
    args: argparse.Namespace,
    output_dir: Path,
) -> HistoricalDateResult:
    primary_url, response_kind = _historical_request_info(spec, day)
    request_urls = [primary_url]
    fallback_url = _historical_response_fallback_url(spec, primary_url)
    if fallback_url is not None:
        request_urls.append(fallback_url)
    last_url = primary_url
    total_response_attempts = 0
    primary_returned_structured_empty = False
    try:
        response_retry_count = max(0, int(args.retries))
        for request_url_index, base_url in enumerate(request_urls):
            for response_attempt in range(response_retry_count + 1):
                url = (
                    base_url
                    if response_attempt == 0
                    else _historical_cache_busted_url(base_url)
                )
                last_url = url
                response = _http_get(
                    url,
                    timeout=args.timeout,
                    verify_ssl=bool(args.verify_ssl),
                    retries=int(args.retries),
                    retry_backoff=float(args.retry_backoff),
                    retry_security_blocks=False,
                )
                total_response_attempts += max(
                    1,
                    int(getattr(response, "_stockagent_response_attempts", 1)),
                )
                audit = _historical_response_audit(
                    response,
                    response_attempts=total_response_attempts,
                )
                try:
                    frame, raw_suffix = _parse_historical_response_content(
                        spec,
                        day,
                        response.content,
                        response_kind,
                    )
                except HistoricalResponseError as exc:
                    # A persistent connection can remain pinned to a poisoned
                    # historical cache/backend.  Do not let one unsafe HTTP
                    # 200 contaminate every later date handled by this worker.
                    _discard_http_session()
                    try_fallback_url = request_url_index < len(request_urls) - 1
                    retry_same_url = (
                        not try_fallback_url
                        and response_attempt < response_retry_count
                    )
                    if retry_same_url or try_fallback_url:
                        # HTTP status/backoff policy belongs to _http_get.  A
                        # syntactically successful HTTP 200 can still be a
                        # cache-poisoned, wrong-date, or lossy receipt; retry it
                        # without deferring the provider-wide schedule.  The
                        # next _http_get still calls the host-global limiter's
                        # wait(), so semantic retries cannot exceed the
                        # configured request rate.  WAF/429/transport cooldowns
                        # remain provider-global inside _http_get.
                        if retry_same_url:
                            continue
                        break
                    raw_path: Path | None = None
                    if not args.skip_raw:
                        raw_path = _write_immutable_raw(
                            response.content,
                            output_dir / "raw_failures" / spec.name,
                            spec.name,
                            _historical_failure_raw_suffix(response, response_kind),
                            stem=(
                                f"{day.isoformat()}."
                                f"{str(audit['body_sha256'])[:16]}"
                            ),
                        )
                    return HistoricalDateResult(
                        day=day,
                        url=url,
                        frame=pl.DataFrame(),
                        raw_path=str(raw_path) if raw_path else None,
                        error=str(exc),
                        **audit,
                    )

                # The newer TWSE rwd/IND route has at least one known historical
                # semantic anomaly (2009-02-02). Treat an otherwise valid empty
                # response conservatively too: confirm it through TWSE's official
                # exchangeReport/IND route before recording a weekday as empty.
                if frame.is_empty() and request_url_index < len(request_urls) - 1:
                    primary_returned_structured_empty = True
                    break

                final_fallback_empty = (
                    frame.is_empty()
                    and len(request_urls) > 1
                    and request_url_index == len(request_urls) - 1
                )
                strict_open_session = _requires_strict_session_calendar(spec, args)
                retry_cross_checked_empty = final_fallback_empty and (
                    primary_returned_structured_empty or strict_open_session
                )
                if (
                    retry_cross_checked_empty
                    and response_attempt < response_retry_count
                ):
                    delay = _retry_delay_seconds(
                        response,
                        response_attempt,
                        float(args.retry_backoff),
                    )
                    _global_tw_public_rate_limiter().defer(delay)
                    time.sleep(delay)
                    continue
                if final_fallback_empty and strict_open_session:
                    raw_path: Path | None = None
                    if not args.skip_raw:
                        raw_path = _write_immutable_raw(
                            response.content,
                            output_dir / "raw_failures" / spec.name,
                            spec.name,
                            _historical_failure_raw_suffix(response, response_kind),
                            stem=(
                                f"{day.isoformat()}."
                                f"{str(audit['body_sha256'])[:16]}"
                            ),
                        )
                    return HistoricalDateResult(
                        day=day,
                        url=url,
                        frame=pl.DataFrame(),
                        raw_path=str(raw_path) if raw_path else None,
                        error=(
                            f"official {spec.name} returned no rows on a "
                            "verified open session"
                        ),
                        **audit,
                    )

                raw_path = None
                source_unavailable_reason = (
                    _known_source_unavailable_reason(spec, day)
                    if frame.is_empty()
                    else None
                )
                if source_unavailable_reason is not None:
                    # Coverage for a known official archive gap is valid only
                    # with one immutable, content-addressed journal receipt per
                    # requested open session. This intentionally overrides
                    # --skip-raw for the narrowly declared source gap.
                    raw_path = _write_immutable_raw(
                        response.content,
                        output_dir / "raw_empty" / spec.name,
                        spec.name,
                        raw_suffix,
                        stem=day.isoformat(),
                    )
                elif not args.skip_raw and not frame.is_empty():
                    raw_path = _write_raw(
                        response.content,
                        output_dir / "raw" / spec.name,
                        spec.name,
                        raw_suffix,
                        stem=day.isoformat(),
                    )
                return HistoricalDateResult(
                    day=day,
                    url=url,
                    frame=frame,
                    raw_path=str(raw_path) if raw_path else None,
                    source_unavailable_reason=source_unavailable_reason,
                    **audit,
                )
        raise RuntimeError(f"historical response retry loop exhausted: {primary_url}")
    except Exception as exc:
        total_response_attempts += max(
            0,
            int(getattr(exc, "_stockagent_response_attempts", 0)),
        )
        return HistoricalDateResult(
            day=day,
            url=last_url,
            frame=pl.DataFrame(),
            error=str(exc),
            response_attempts=total_response_attempts,
        )
    finally:
        if args.sleep:
            time.sleep(max(0.0, float(args.sleep)))


def _download_historical(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
) -> DownloadResult:
    with _historical_dataset_lock(output_dir, spec):
        return _download_historical_unlocked(spec, args, output_dir)


def _download_historical_unlocked(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
) -> DownloadResult:
    assert spec.url_template is not None
    parquet_path = output_dir / f"{spec.name}.parquet"
    plan = _plan_historical_download(spec, args, output_dir)
    cache = _prepare_historical_resume_cache(spec, args, output_dir, plan)
    dates = plan.dates

    working_path = cache.partial_path if plan.replace_output else parquet_path

    fetched_at = _now_utc()
    frames: list[pl.DataFrame] = []
    fetched_dates = 0
    empty_dates: set[date] = set()
    errors: dict[date, str] = {}
    data_dates: set[date] = set()
    new_rows = 0
    raw_path: str | None = None
    rows = _read_existing_row_count(working_path)
    wrote_any = working_path.exists()
    flush_every_dates = max(0, int(getattr(args, "flush_every_dates", 0) or 0))
    date_workers = max(1, int(getattr(args, "date_workers", 1) or 1))
    progress_iter = tqdm(
        total=len(dates),
        desc=f"{spec.name}:dates",
        unit="day",
        leave=False,
        mininterval=0.5,
        disable=not bool(getattr(args, "progress", True)),
    )

    def flush_frames(*, final: bool = False) -> None:
        nonlocal frames, rows, wrote_any
        if not frames:
            return
        incoming = pl.concat(frames, how="diagonal_relaxed")
        if DATE_COLUMN in incoming.columns:
            incoming = incoming.sort(DATE_COLUMN)
        rows = _write_parquet_merged(
            working_path,
            incoming,
            refresh=False,
        )
        frames = []
        wrote_any = True
        if final or bool(getattr(args, "progress", True)):
            progress_iter.set_postfix(
                fetched=fetched_dates,
                empty=len(empty_dates),
                failed=len(errors),
                rows=rows,
                flushed=int(wrote_any),
            )

    def consume(result: HistoricalDateResult) -> None:
        nonlocal fetched_dates, new_rows, raw_path
        if result.error is not None:
            errors[result.day] = result.error
            _append_historical_journal_record(
                cache,
                spec,
                result,
                status="failed",
                source="network",
            )
        elif result.frame.is_empty():
            valid_source_unavailable = (
                _source_unavailable_result_receipt_is_valid(
                    output_dir,
                    spec,
                    result,
                )
            )
            if (
                _requires_strict_session_calendar(spec, args)
                and not valid_source_unavailable
            ):
                message = (
                    f"official {spec.name} returned no rows on a verified open session"
                )
                errors[result.day] = message
                failed_result = HistoricalDateResult(
                    day=result.day,
                    url=result.url,
                    frame=result.frame,
                    raw_path=result.raw_path,
                    error=message,
                    http_status=result.http_status,
                    content_type=result.content_type,
                    content_length=result.content_length,
                    body_sha256=result.body_sha256,
                    body_snippet=result.body_snippet,
                    response_attempts=result.response_attempts,
                    source_unavailable_reason=result.source_unavailable_reason,
                )
                _append_historical_journal_record(
                    cache,
                    spec,
                    failed_result,
                    status="failed",
                    source="network",
                )
            elif result.day in plan.suspicious_dates and not valid_source_unavailable:
                message = "suspicious existing date returned no official rows"
                errors[result.day] = message
                failed_result = HistoricalDateResult(
                    day=result.day,
                    url=result.url,
                    frame=result.frame,
                    raw_path=result.raw_path,
                    error=message,
                )
                _append_historical_journal_record(
                    cache,
                    spec,
                    failed_result,
                    status="failed",
                    source="network",
                )
            else:
                empty_dates.add(result.day)
                _append_historical_journal_record(
                    cache,
                    spec,
                    result,
                    status="empty",
                    source="network",
                )
        else:
            frames.append(_append_common_columns(result.frame, spec, fetched_at=fetched_at, url=result.url))
            fetched_dates += 1
            new_rows += int(result.frame.height)
            data_dates.add(result.day)
            raw_path = result.raw_path or raw_path
            _append_historical_journal_record(
                cache,
                spec,
                result,
                status="data",
                source="network",
            )
            if flush_every_dates and len(frames) >= flush_every_dates:
                flush_frames()
        if bool(getattr(args, "progress", True)):
            progress_iter.set_postfix(
                fetched=fetched_dates,
                empty=len(empty_dates),
                failed=len(errors),
                rows=(rows + new_rows if not wrote_any else rows),
                mode=_canonical_mode(args.mode),
                date_workers=date_workers,
            )

    replacement_promoted = False
    try:
        with ThreadPoolExecutor(max_workers=date_workers) as executor:
            pending_days = iter(dates)
            max_in_flight = max(date_workers, date_workers * 2)
            futures: dict[Any, date] = {}

            def fill_in_flight() -> None:
                while len(futures) < max_in_flight:
                    try:
                        day = next(pending_days)
                    except StopIteration:
                        return
                    future = executor.submit(
                        _download_historical_date,
                        spec,
                        day,
                        args,
                        output_dir,
                    )
                    futures[future] = day

            fill_in_flight()
            while futures:
                completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in completed:
                    day = futures.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = HistoricalDateResult(
                            day=day,
                            url="",
                            frame=pl.DataFrame(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    consume(result)
                    progress_iter.update(1)
                fill_in_flight()
        flush_frames(final=True)
    finally:
        progress_iter.close()

    resolved_data_dates = set(cache.data_dates) | data_dates
    resolved_empty_dates = set(cache.empty_dates) | empty_dates
    resolved_data_dates.difference_update(errors)
    resolved_empty_dates.difference_update(errors)
    confirmed_empty_after = (
        set(resolved_empty_dates)
        if plan.replace_output
        else set(plan.confirmed_empty_dates) | set(empty_dates)
    )
    confirmed_empty_after.difference_update(
        resolved_data_dates | set(plan.existing_dates)
    )
    confirmed_empty_after.difference_update(errors)
    confirmed_source_unavailable_after = {
        day
        for day in confirmed_empty_after
        if _known_source_unavailable_reason(spec, day) is not None
    }
    staged_unresolved = (
        plan.all_weekdays - resolved_data_dates - resolved_empty_dates
    ) | set(errors)

    if (
        plan.replace_output
        and not staged_unresolved
        and wrote_any
        and working_path.exists()
    ):
        _validate_historical_partial_for_publish(
            spec,
            working_path,
            plan.all_weekdays,
            resolved_empty_dates,
        )
        _copy_file_atomic(working_path, parquet_path)
        replacement_promoted = True
        rows = _read_existing_row_count(parquet_path)
    elif not plan.replace_output:
        replacement_promoted = True

    coverage_complete, missing_after = _finish_historical_coverage_state(
        spec,
        args,
        output_dir,
        plan,
        data_dates=resolved_data_dates if plan.replace_output else data_dates,
        empty_dates=resolved_empty_dates if plan.replace_output else empty_dates,
        errors=errors,
        replacement_promoted=replacement_promoted,
    )
    if plan.replace_output and not bool(getattr(args, "resume", True)):
        working_path.unlink(missing_ok=True)

    if errors:
        examples = "; ".join(
            f"{day.isoformat()}={message}" for day, message in sorted(errors.items())[:5]
        )
        status = "failed"
        message = f"{len(errors)} date request(s) failed: {examples}"
    elif not coverage_complete:
        status = "incomplete"
        message = f"{missing_after} expected date(s) remain unchecked"
    elif fetched_dates or empty_dates:
        status = "ok"
        message = None
    else:
        status = "up_to_date"
        message = None

    return DownloadResult(
        spec.name,
        status,
        rows,
        str(parquet_path) if parquet_path.exists() else None,
        message=message,
        raw_path=raw_path,
        requested_dates=len(dates),
        fetched_dates=fetched_dates,
        skipped_dates=len(empty_dates) + len(errors),
        empty_dates=len(empty_dates),
        failed_dates=len(errors),
        missing_dates_before=len(plan.missing_before),
        missing_dates_after=missing_after,
        coverage_complete=coverage_complete,
        source_unavailable_dates=len(confirmed_source_unavailable_after),
    )


def _download_snapshot_url(spec: DatasetSpec, args: argparse.Namespace, output_dir: Path) -> DownloadResult:
    assert spec.url is not None
    fetched_at = _now_utc()
    response = _http_get(
        spec.url,
        timeout=args.timeout,
        verify_ssl=bool(args.verify_ssl),
        retries=int(args.retries),
        retry_backoff=float(args.retry_backoff),
    )
    raw_path: Path | None = None
    if not args.skip_raw:
        suffix = _suffix_from_url(spec.url, response.headers.get("content-type", ""))
        raw_path = _write_raw(response.content, output_dir / "raw" / spec.name, spec.name, suffix)
    frame = _parse_resource_bytes(response.content, url=spec.url)
    if frame.is_empty():
        return DownloadResult(spec.name, "empty", 0, None, raw_path=str(raw_path) if raw_path else None)
    frame = _append_common_columns(frame, spec, fetched_at=fetched_at, url=spec.url, as_of_date=date.today().isoformat())
    parquet_path = output_dir / f"{spec.name}.parquet"
    rows = _write_parquet_merged(
        parquet_path,
        frame,
        refresh=bool(args.refresh) or _canonical_mode(args.mode) == "rebuild",
    )
    return DownloadResult(spec.name, "ok", rows, str(parquet_path), raw_path=str(raw_path) if raw_path else None)


def _parse_roc_delisted_date(value: Any) -> str | None:
    text = _strip_html(value).replace("年", "/").replace("月", "/").replace("日", "").replace("-", "/")
    return _roc_date_to_iso(text)


def _twse_delisted_frame(payload: Any) -> pl.DataFrame:
    if not isinstance(payload, list):
        return pl.DataFrame()
    records: list[dict[str, str]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        delisted_date = _parse_roc_delisted_date(row.get("DelistingDate", ""))
        if delisted_date:
            records.append(
                {
                    DATE_COLUMN: delisted_date,
                    "market": "twse",
                    "symbol": _strip_html(row.get("Code", "")),
                    "company_name": _strip_html(row.get("Company", "")),
                    "delisting_reason": "",
                }
            )
    return pl.DataFrame(records, schema={key: pl.Utf8 for key in records[0]}) if records else pl.DataFrame()


def _tpex_delisted_frame(payload: Any) -> pl.DataFrame:
    if not isinstance(payload, dict) or str(payload.get("stat", "")).lower() != "ok":
        return pl.DataFrame()
    tables = payload.get("tables")
    if not isinstance(tables, list) or not tables or not isinstance(tables[0], dict):
        return pl.DataFrame()
    table = tables[0]
    fields, rows = table.get("fields"), table.get("data")
    if not isinstance(fields, list) or not isinstance(rows, list):
        return pl.DataFrame()
    names = _make_unique([str(value) for value in fields])
    records: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        record = {name: _strip_html(row[idx]) if idx < len(row) else "" for idx, name in enumerate(names)}
        delisted_date = _parse_roc_delisted_date(record.get("終止上櫃日期", ""))
        if delisted_date:
            records.append(
                {
                    DATE_COLUMN: delisted_date,
                    "market": "tpex",
                    "symbol": record.get("股票代號", ""),
                    "company_name": record.get("公司名稱", ""),
                    "delisting_reason": record.get("終止上櫃原因", ""),
                }
            )
    return pl.DataFrame(records, schema={key: pl.Utf8 for key in records[0]}) if records else pl.DataFrame()


def _download_delisted_history(
    spec: DatasetSpec,
    args: argparse.Namespace,
    output_dir: Path,
) -> DownloadResult:
    assert spec.url is not None
    parquet_path = output_dir / f"{spec.name}.parquet"
    configured_start = _parse_date(spec.start_date or "2000-01-01")
    start = configured_start if args.start_date == "earliest" else max(configured_start, _parse_date(args.start_date))
    end = _parse_date(resolve_end_date(args.end_date))
    if _canonical_mode(args.mode) == "daily" and not args.refresh:
        latest = _latest_existing_date(parquet_path)
        if latest is not None:
            start = max(configured_start, latest - timedelta(days=31))
    if start > end:
        return DownloadResult(spec.name, "up_to_date", _read_existing_row_count(parquet_path), str(parquet_path))

    market = "twse" if spec.name.startswith("twse_") else "tpex"
    requests_to_make: list[tuple[str, dict[str, str], str]] = []
    if market == "twse":
        requests_to_make.append((spec.url, {}, "twse_delisted_companies"))
    else:
        for year in range(start.year, end.year + 1):
            requests_to_make.append(
                (spec.url, {"response": "json", "date": str(year), "cate": "1"}, str(year))
            )

    frames: list[pl.DataFrame] = []
    raw_path: Path | None = None
    fetched_at = _now_utc()
    for url, params, raw_stem in requests_to_make:
        response = _http_get(
            url,
            params=params,
            timeout=args.timeout,
            verify_ssl=bool(args.verify_ssl),
            retries=int(args.retries),
            retry_backoff=float(args.retry_backoff),
        )
        if not args.skip_raw:
            raw_path = _write_raw(response.content, output_dir / "raw" / spec.name, spec.name, ".json", stem=raw_stem)
        frame = _twse_delisted_frame(response.json()) if market == "twse" else _tpex_delisted_frame(response.json())
        if not frame.is_empty():
            frame = frame.filter(pl.col(DATE_COLUMN).str.to_date().is_between(start, end))
            if not frame.is_empty():
                frames.append(_append_common_columns(frame, spec, fetched_at=fetched_at, url=response.url))
        if args.sleep:
            time.sleep(max(0.0, float(args.sleep)))

    if not frames:
        return DownloadResult(
            spec.name,
            "no_new_rows",
            _read_existing_row_count(parquet_path),
            str(parquet_path) if parquet_path.exists() else None,
            raw_path=str(raw_path) if raw_path else None,
        )
    incoming = pl.concat(frames, how="diagonal_relaxed")
    rows = _write_parquet_merged(
        parquet_path,
        incoming,
        refresh=bool(args.refresh) or _canonical_mode(args.mode) == "rebuild",
    )
    return DownloadResult(
        spec.name,
        "ok",
        rows,
        str(parquet_path),
        raw_path=str(raw_path) if raw_path else None,
        fetched_dates=len(requests_to_make),
    )


def _suffix_from_url(url: str, content_type: str) -> str:
    suffix = Path(urlparse(url).path).suffix
    if suffix:
        return suffix
    content_type = content_type.lower()
    if "json" in content_type:
        return ".json"
    if "csv" in content_type:
        return ".csv"
    if "xml" in content_type:
        return ".xml"
    if "zip" in content_type:
        return ".zip"
    return ".bin"


def _download_data_gov(spec: DatasetSpec, args: argparse.Namespace, output_dir: Path) -> DownloadResult:
    assert spec.data_gov_id is not None
    fetched_at = _now_utc()
    metadata_url = DATA_GOV_DATASET_API.format(dataset_id=quote(str(spec.data_gov_id)))
    metadata_response = _http_get(
        metadata_url,
        timeout=args.timeout,
        verify_ssl=bool(args.verify_ssl),
        retries=int(args.retries),
        retry_backoff=float(args.retry_backoff),
    )
    metadata = metadata_response.json().get("result", {})
    distributions = metadata.get("distribution") or []
    if not isinstance(distributions, list) or not distributions:
        return DownloadResult(
            spec.name,
            "failed",
            0,
            None,
            message=f"data.gov.tw dataset has no downloadable distribution: {spec.data_gov_id}",
        )

    frames: list[pl.DataFrame] = []
    raw_path: Path | None = None
    messages: list[str] = []
    for idx, distribution in enumerate(distributions):
        if not isinstance(distribution, dict):
            continue
        url = distribution.get("resourceDownloadUrl") or distribution.get("resourceAPIUrl")
        if not url:
            continue
        resource_format = str(distribution.get("resourceFormat") or "")
        resource_name = str(distribution.get("resourceDescription") or f"resource_{idx}")
        try:
            response = _http_get(
                str(url),
                timeout=args.timeout,
                verify_ssl=bool(args.verify_ssl),
                retries=int(args.retries),
                retry_backoff=float(args.retry_backoff),
            )
            if not args.skip_raw:
                raw_path = _write_raw(
                    response.content,
                    output_dir / "raw" / spec.name,
                    spec.name,
                    _suffix_from_url(str(url), response.headers.get("content-type", "")),
                    stem=f"{idx}_{resource_format or 'resource'}",
                )
            frame = _parse_resource_bytes(response.content, url=str(url), resource_format=resource_format)
        except Exception as exc:
            messages.append(f"{idx}:{exc}")
            continue
        if frame.is_empty():
            continue
        frames.append(
            _append_common_columns(
                frame,
                spec,
                fetched_at=fetched_at,
                url=str(url),
                as_of_date=date.today().isoformat(),
                resource=resource_name or f"resource_{idx}",
            ).with_columns(
                pl.lit(spec.data_gov_id).alias("_data_gov_id"),
                pl.lit(str(metadata.get("title") or "")).alias("_data_gov_title"),
            )
        )

    _write_json(output_dir / "metadata" / f"{spec.name}.json", metadata)
    if not frames:
        return DownloadResult(
            spec.name,
            "failed" if messages else "empty",
            _read_existing_row_count(output_dir / f"{spec.name}.parquet"),
            str(output_dir / f"{spec.name}.parquet")
            if (output_dir / f"{spec.name}.parquet").exists()
            else None,
            message="; ".join(messages) if messages else None,
            raw_path=str(raw_path) if raw_path else None,
        )

    if messages:
        parquet_path = output_dir / f"{spec.name}.parquet"
        return DownloadResult(
            spec.name,
            "failed",
            _read_existing_row_count(parquet_path),
            str(parquet_path) if parquet_path.exists() else None,
            message="; ".join(messages),
            raw_path=str(raw_path) if raw_path else None,
        )

    incoming = pl.concat(frames, how="diagonal_relaxed")
    parquet_path = output_dir / f"{spec.name}.parquet"
    rows = _write_parquet_merged(parquet_path, incoming, refresh=True)
    return DownloadResult(
        spec.name,
        "ok",
        rows,
        str(parquet_path),
        message="; ".join(messages) if messages else None,
        raw_path=str(raw_path) if raw_path else None,
    )


def download_dataset(spec: DatasetSpec, args: argparse.Namespace, output_dir: Path) -> DownloadResult:
    try:
        if spec.kind == "historical_json_table":
            return _download_historical(spec, args, output_dir)
        if spec.kind == "delisted_history":
            return _download_delisted_history(spec, args, output_dir)
        if spec.kind == "snapshot_url":
            return _download_snapshot_url(spec, args, output_dir)
        if spec.kind == "data_gov":
            return _download_data_gov(spec, args, output_dir)
        return DownloadResult(spec.name, "unsupported", 0, None, message=f"kind={spec.kind}")
    except Exception as exc:
        return DownloadResult(spec.name, "failed", 0, None, message=str(exc))


def _select_specs(tokens: list[str]) -> list[DatasetSpec]:
    normalized_tokens: set[str] = set()
    for token in tokens:
        for part in str(token).split(","):
            value = part.strip().lower()
            if value:
                normalized_tokens.add(value)
    if not normalized_tokens or "all" in normalized_tokens:
        return list(DEFAULT_DATASETS.values())

    selected: list[DatasetSpec] = []
    for spec in DEFAULT_DATASETS.values():
        labels = {spec.name.lower(), spec.source.lower(), *[tag.lower() for tag in spec.tags]}
        if labels & normalized_tokens:
            selected.append(spec)
    unknown = sorted(token for token in normalized_tokens if not any(token in {spec.name.lower(), spec.source.lower(), *[tag.lower() for tag in spec.tags]} for spec in DEFAULT_DATASETS.values()))
    if unknown:
        raise ValueError(f"Unknown dataset/tag/source: {', '.join(unknown)}")
    return selected


def _print_dataset_list(specs: list[DatasetSpec]) -> None:
    for spec in specs:
        labels = ",".join(spec.tags)
        origin = spec.url or (f"data.gov.tw dataset {spec.data_gov_id}" if spec.data_gov_id else spec.url_template or "")
        print(f"{spec.name}\t{spec.kind}\t{spec.source}\t{labels}\t{origin}")


def _run_selected_downloads(
    specs: list[DatasetSpec],
    args: argparse.Namespace,
    output_dir: Path,
) -> list[DownloadResult]:
    dependent = [
        spec for spec in specs if spec.name in TPEX_SESSION_DEPENDENT_DATASETS
    ]

    def run_batch(batch: list[DatasetSpec], description: str) -> list[DownloadResult]:
        if not batch:
            return []
        return run_parallel_tasks(
            batch,
            lambda spec: download_dataset(spec, args, output_dir),
            max_workers=args.workers,
            desc=description,
            unit="dataset",
            on_error=lambda spec, exc: DownloadResult(
                spec.name,
                "failed",
                0,
                None,
                message=str(exc),
            ),
        )

    if not dependent:
        return run_batch(specs, "download:tw_public")

    # The three TPEx feature histories use validated official OHLCV sessions as
    # their calendar. Complete every independent historical source first so a
    # same-run tpex_daily_ohlcv baseline is fully audited before dependents plan
    # or issue any requests. Snapshot feeds remain parallel in phase two.
    phase_one = [
        spec
        for spec in specs
        if spec.kind == "historical_json_table"
        and spec.name not in TPEX_SESSION_DEPENDENT_DATASETS
    ]
    phase_one_names = {spec.name for spec in phase_one}
    phase_two = [
        spec
        for spec in specs
        if spec.name not in phase_one_names
    ]
    return run_batch(phase_one, "download:tw_public:calendar") + run_batch(
        phase_two,
        "download:tw_public:dependent",
    )


def main() -> None:
    args = parse_args()
    requested_mode = args.mode
    args.mode = _canonical_mode(args.mode)
    if args.daily_overlap_days < 1:
        raise ValueError("--daily-overlap-days must be >= 1")
    if args.empty_recheck_days < 0:
        raise ValueError("--empty-recheck-days must be >= 0")
    request_interval = _configure_tw_public_rate_limiter(args.request_interval)
    print(f"[tw-public] {describe_rate_limit('tw_public', request_interval)}", flush=True)
    specs = _select_specs(args.datasets)
    if args.mode == "list":
        _print_dataset_list(specs)
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "dataset_manifest.json", [asdict(spec) for spec in specs])

    results = _run_selected_downloads(specs, args, output_dir)
    results.sort(key=lambda row: row.dataset)
    _write_csv_report(output_dir / "download_report.csv", results)

    failed_statuses = {"failed", "incomplete", "unsupported"}
    historical_names = {
        spec.name for spec in specs if spec.kind == "historical_json_table"
    }
    historical_results = [row for row in results if row.dataset in historical_names]
    source_unavailable_by_dataset = {
        row.dataset: {
            "confirmed_dates": row.source_unavailable_dates,
            "known_ranges": [
                {
                    "start": range_start.isoformat(),
                    "end": range_end.isoformat(),
                    "reason": reason,
                }
                for range_start, range_end, reason in (
                    TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES.get(row.dataset, ())
                )
            ],
        }
        for row in historical_results
        if row.dataset in TPEX_KNOWN_SOURCE_UNAVAILABLE_RANGES
    }
    publication_lag_candidates = {"twse_margin_balance", "tpex_margin_balance"}
    resolved_end_date = resolve_end_date(args.end_date)
    publication_lag_results = [
        row
        for row in results
        if (
            bool(args.allow_daily_publication_lag)
            and args.mode == "daily"
            and row.dataset in publication_lag_candidates
            and row.status in failed_statuses
            and int(row.failed_dates) == 1
            and int(row.missing_dates_after) == 1
            and resolved_end_date in str(row.message)
        )
    ]
    publication_lag_names = {row.dataset for row in publication_lag_results}
    blocking_failures = [
        row
        for row in results
        if row.status in failed_statuses and row.dataset not in publication_lag_names
    ]
    summary = {
        "schema_version": 3,
        "generated_at_utc": _now_utc(),
        "mode": args.mode,
        "requested_mode": requested_mode,
        "start_date": args.start_date,
        "end_date": resolved_end_date,
        "output_dir": str(output_dir),
        "request_interval_seconds": request_interval,
        "configured_requests_per_second": 1.0 / request_interval if request_interval > 0 else None,
        "rate_limit_scope": "host-global per provider across threads and subprocesses",
        "rate_limit_basis": provider_rate_limit("tw_public").basis,
        "require_taiex_session_calendar": bool(
            getattr(args, "require_taiex_session_calendar", False)
        ),
        "dataset_count": len(results),
        "ok_count": sum(row.status == "ok" for row in results),
        "up_to_date_count": sum(row.status == "up_to_date" for row in results),
        "failed_count": sum(row.status in failed_statuses for row in results),
        "blocking_failed_count": len(blocking_failures),
        "publication_lag_count": len(publication_lag_results),
        "publication_lag_datasets": sorted(publication_lag_names),
        "daily_close_ready": bool(
            args.mode == "daily" and not blocking_failures
        ),
        "incomplete_count": sum(row.status == "incomplete" for row in results),
        "empty_count": sum(row.status in {"empty", "no_new_rows", "no_distribution"} for row in results),
        "historical_dataset_count": len(historical_results),
        "coverage_complete": (
            all(row.coverage_complete is True for row in historical_results)
            if historical_results
            else None
        ),
        "requested_dates": sum(row.requested_dates for row in results),
        "fetched_dates": sum(row.fetched_dates for row in results),
        "confirmed_empty_dates": sum(row.empty_dates for row in results),
        "source_unavailable_dates": sum(
            row.source_unavailable_dates for row in historical_results
        ),
        "source_unavailable_by_dataset": source_unavailable_by_dataset,
        "failed_dates": sum(row.failed_dates for row in results),
        "missing_dates_after": sum(row.missing_dates_after for row in results),
        "rows_total": sum(row.rows for row in results),
    }
    _write_json(output_dir / "download_summary.json", summary)
    print(f"[tw-public] download_report.csv -> {output_dir / 'download_report.csv'}")
    print(f"[tw-public] download_summary.json -> {output_dir / 'download_summary.json'}")
    print(
        f"[tw-public] mode={args.mode} ok={summary['ok_count']} "
        f"up_to_date={summary['up_to_date_count']} failed={summary['failed_count']} "
        f"empty={summary['empty_count']} coverage_complete={summary['coverage_complete']} "
        f"rows={summary['rows_total']}"
    )

    if summary["blocking_failed_count"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
