from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import html
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from typing import Any
from urllib.parse import quote, urlparse
import zipfile
import xml.etree.ElementTree as ET

import polars as pl
import pyarrow.parquet as pq
import requests
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

try:
    from downloader.common import resolve_end_date, run_parallel_tasks
except ImportError:  # pragma: no cover - direct script execution from downloader/
    from common import resolve_end_date, run_parallel_tasks


DATA_GOV_DATASET_API = "https://data.gov.tw/api/v2/rest/dataset/{dataset_id}"
USER_AGENT = "stockAgent-tw-public-data-downloader/1.0"
DATE_COLUMN = "date"
ROC_DATE_PATTERN = re.compile(r"^\d{2,3}/\d{1,2}/\d{1,2}$")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")


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
    fetched_dates: int = 0
    skipped_dates: int = 0


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
            "?date={date}&type=ALLBUT0999&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2004-02-11",
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
        start_date="2004-02-11",
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
        start_date="2007-01-01",
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
        start_date="2007-01-01",
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
        start_date="2007-04-20",
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
        start_date="2007-01-01",
    ),
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
        *SNAPSHOT_OPEN_DATASETS,
        *DATA_GOV_DATASETS,
    )
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Taiwan free public datasets from TWSE, TPEx, MOPS, TDCC, TAIFEX, CBC, DGBAS, and MOF."
    )
    parser.add_argument("--mode", choices=("daily-update", "full", "list"), default="daily-update")
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=["all"],
        help="Dataset names or tags/sources, e.g. all twse tpex macro price.",
    )
    parser.add_argument("--start-date", default="earliest", help="Historical start date or 'earliest'.")
    parser.add_argument("--end-date", default="today", help="Historical end date, today, or now.")
    parser.add_argument("--output-dir", default="data_tw_public", help="Output directory.")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent historical dataset workers.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds.")
    parser.add_argument("--retries", type=int, default=3, help="Transient HTTP retry count per request.")
    parser.add_argument("--retry-backoff", type=float, default=1.0, help="Base seconds for exponential retry backoff.")
    parser.add_argument("--sleep", type=float, default=0.15, help="Delay between historical date requests per dataset.")
    parser.add_argument("--max-dates", type=int, default=None, help="Optional smoke-test cap per historical dataset.")
    parser.add_argument("--refresh", action="store_true", help="Overwrite existing parquet instead of merging.")
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


def _roc_date_to_iso(value: str) -> str | None:
    text = value.strip()
    if not ROC_DATE_PATTERN.match(text):
        return None
    parts = [int(part) for part in text.split("/")]
    return date(parts[0] + 1911, parts[1], parts[2]).isoformat()


def _format_date(value: date, fmt: str) -> str:
    return value.strftime(fmt)


def _http_get(
    url: str,
    *,
    timeout: int,
    verify_ssl: bool,
    params: dict[str, str] | None = None,
    retries: int = 3,
    retry_backoff: float = 1.0,
) -> requests.Response:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/csv,text/plain,application/xml,text/xml,*/*",
    }
    retry_count = max(0, int(retries))
    transient_statuses = {429, 500, 502, 503, 504}
    last_error: requests.exceptions.RequestException | None = None

    for attempt in range(retry_count + 1):
        try:
            try:
                response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=verify_ssl)
            except requests.exceptions.SSLError:
                if not verify_ssl:
                    raise
                requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
                response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)
            if response.status_code in transient_statuses and attempt < retry_count:
                time.sleep(_retry_delay_seconds(response, attempt, retry_backoff))
                continue
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= retry_count:
                raise
            time.sleep(_retry_delay_seconds(None, attempt, retry_backoff))

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"HTTP request failed without response: {url}")


def _retry_delay_seconds(response: requests.Response | None, attempt: int, retry_backoff: float) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(60.0, max(0.0, float(retry_after)))
            except ValueError:
                pass
    return max(0.0, float(retry_backoff)) * (2**attempt)


def _decode_bytes(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "cp950", "big5", "latin1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


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
        rows = _records_from_fields_data(
            fields=fields,
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
        records.extend(
            _records_from_fields_data(
                fields=table_fields,
                data=table_data,
                iso_date=iso_date,
                table_title=table_title,
                table_index=table_index,
            )
        )
    return _frame_from_records(records)


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


def _download_historical(spec: DatasetSpec, args: argparse.Namespace, output_dir: Path) -> DownloadResult:
    assert spec.url_template is not None
    parquet_path = output_dir / f"{spec.name}.parquet"
    configured_start = spec.start_date or "2000-01-01"
    start = _parse_date(configured_start if args.start_date == "earliest" else args.start_date)
    end = _parse_date(resolve_end_date(args.end_date))

    if args.mode == "daily-update" and not args.refresh:
        latest = _latest_existing_date(parquet_path)
        if latest is not None:
            start = max(start, latest + timedelta(days=1))

    dates = _iter_dates(start, end, include_weekends=bool(args.include_weekends))
    if args.max_dates is not None:
        dates = dates[: max(0, int(args.max_dates))]
    if not dates:
        return DownloadResult(spec.name, "up_to_date", _read_existing_row_count(parquet_path), str(parquet_path))

    fetched_at = _now_utc()
    frames: list[pl.DataFrame] = []
    fetched_dates = 0
    skipped_dates = 0
    new_rows = 0
    raw_path: Path | None = None
    last_error: str | None = None
    progress_iter = tqdm(
        dates,
        desc=f"{spec.name}:dates",
        unit="day",
        leave=False,
        mininterval=0.5,
        disable=not bool(getattr(args, "progress", True)),
    )
    for day in progress_iter:
        url = spec.url_template.format(date=_format_date(day, spec.date_format))
        try:
            response = _http_get(
                url,
                timeout=args.timeout,
                verify_ssl=bool(args.verify_ssl),
                retries=int(args.retries),
                retry_backoff=float(args.retry_backoff),
            )
            payload = response.json()
            frame = _parse_json_table_payload(payload, spec, day)
        except Exception as exc:
            last_error = str(exc)
            skipped_dates += 1
            continue
        if frame.is_empty():
            skipped_dates += 1
        else:
            frames.append(_append_common_columns(frame, spec, fetched_at=fetched_at, url=url))
            fetched_dates += 1
            new_rows += int(frame.height)
            if not args.skip_raw:
                raw_path = _write_raw(
                    response.content,
                    output_dir / "raw" / spec.name,
                    spec.name,
                    ".json",
                    stem=day.isoformat(),
                )
        if bool(getattr(args, "progress", True)):
            progress_iter.set_postfix(
                fetched=fetched_dates,
                skipped=skipped_dates,
                rows=new_rows,
                refresh=False,
            )
        if args.sleep:
            time.sleep(max(0.0, float(args.sleep)))

    if not frames:
        rows = _read_existing_row_count(parquet_path)
        return DownloadResult(
            spec.name,
            "no_new_rows",
            rows,
            str(parquet_path) if parquet_path.exists() else None,
            message=last_error,
            raw_path=str(raw_path) if raw_path else None,
            fetched_dates=fetched_dates,
            skipped_dates=skipped_dates,
        )

    incoming = pl.concat(frames, how="diagonal_relaxed")
    rows = _write_parquet_merged(parquet_path, incoming, refresh=bool(args.refresh))
    return DownloadResult(
        spec.name,
        "ok",
        rows,
        str(parquet_path),
        raw_path=str(raw_path) if raw_path else None,
        fetched_dates=fetched_dates,
        skipped_dates=skipped_dates,
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
    rows = _write_parquet_merged(parquet_path, frame, refresh=bool(args.refresh))
    return DownloadResult(spec.name, "ok", rows, str(parquet_path), raw_path=str(raw_path) if raw_path else None)


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
        return DownloadResult(spec.name, "no_distribution", 0, None, message=f"dataset_id={spec.data_gov_id}")

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
            "empty",
            0,
            None,
            message="; ".join(messages) if messages else None,
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


def main() -> None:
    args = parse_args()
    specs = _select_specs(args.datasets)
    if args.mode == "list":
        _print_dataset_list(specs)
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "dataset_manifest.json", [asdict(spec) for spec in specs])

    results = run_parallel_tasks(
        specs,
        lambda spec: download_dataset(spec, args, output_dir),
        max_workers=args.workers,
        desc="download:tw_public",
        unit="dataset",
        on_error=lambda spec, exc: DownloadResult(spec.name, "failed", 0, None, message=str(exc)),
    )
    results.sort(key=lambda row: row.dataset)
    _write_csv_report(output_dir / "download_report.csv", results)

    summary = {
        "generated_at_utc": _now_utc(),
        "mode": args.mode,
        "output_dir": str(output_dir),
        "dataset_count": len(results),
        "ok_count": sum(row.status == "ok" for row in results),
        "failed_count": sum(row.status == "failed" for row in results),
        "empty_count": sum(row.status in {"empty", "no_new_rows", "no_distribution"} for row in results),
        "rows_total": sum(row.rows for row in results),
    }
    _write_json(output_dir / "download_summary.json", summary)
    print(f"[tw-public] download_report.csv -> {output_dir / 'download_report.csv'}")
    print(f"[tw-public] download_summary.json -> {output_dir / 'download_summary.json'}")
    print(
        f"[tw-public] ok={summary['ok_count']} failed={summary['failed_count']} "
        f"empty={summary['empty_count']} rows={summary['rows_total']}"
    )

    if summary["failed_count"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
