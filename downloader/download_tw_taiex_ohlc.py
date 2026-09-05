from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Any

import polars as pl
import requests
from tqdm import tqdm

try:
    from downloader.artifact_io import (
        atomic_write_bytes,
        atomic_write_json,
        atomic_write_parquet,
    )
except ImportError:  # pragma: no cover - direct execution from downloader/
    from artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet

try:
    from downloader.common import (
        SharedRateLimiter,
        describe_rate_limit,
        provider_rate_limit,
        resolve_end_date,
        resolve_request_interval,
    )
except ImportError:  # pragma: no cover - direct execution from downloader/
    from common import (
        SharedRateLimiter,
        describe_rate_limit,
        provider_rate_limit,
        resolve_end_date,
        resolve_request_interval,
    )


DATASET_NAME = "twse_taiex_ohlc"
SOURCE_NAME = "TWSE"
SOURCE_PRODUCT = "indicesReport/MI_5MINS_HIST"
OFFICIAL_START_DATE = date(1999, 1, 5)
ENDPOINT_TEMPLATE = (
    "https://wwwc.twse.com.tw/indicesReport/MI_5MINS_HIST?date={date}&response=json"
)
PARSER_CONTRACT_VERSION = 1
JOURNAL_SCHEMA_VERSION = 1
SUMMARY_SCHEMA_VERSION = 1
WAF_COOLDOWN_SECONDS = 30.0
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/126.0 Safari/537.36 stockAgent/1.0"
)

PRICE_COLUMNS = (
    "date",
    "opening_index",
    "highest_index",
    "lowest_index",
    "closing_index",
)
OUTPUT_COLUMNS = (
    *PRICE_COLUMNS,
    "_dataset",
    "_source",
    "_source_product",
    "_request_month",
    "_downloaded_at_utc",
    "_url",
)
OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date,
    "opening_index": pl.Float64,
    "highest_index": pl.Float64,
    "lowest_index": pl.Float64,
    "closing_index": pl.Float64,
    "_dataset": pl.Utf8,
    "_source": pl.Utf8,
    "_source_product": pl.Utf8,
    "_request_month": pl.Utf8,
    "_downloaded_at_utc": pl.Utf8,
    "_url": pl.Utf8,
}

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("日期", "Date", "date"),
    "opening_index": ("開盤指數", "OpeningIndex", "opening_index"),
    "highest_index": ("最高指數", "HighestIndex", "highest_index"),
    "lowest_index": ("最低指數", "LowestIndex", "lowest_index"),
    "closing_index": ("收盤指數", "ClosingIndex", "closing_index"),
}

_HTTP_LOCAL = threading.local()
_RATE_LIMITER: SharedRateLimiter | None = None
_RATE_LIMITER_LOCK = threading.Lock()
_JOURNAL_LOCK = threading.Lock()


class MonthPayloadError(RuntimeError):
    """An official response that cannot safely represent the requested month."""


@dataclass(slots=True)
class MonthResult:
    month: str
    url: str
    frame: pl.DataFrame
    error: str | None = None
    raw_path: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    content_length: int | None = None
    body_sha256: str | None = None
    body_snippet: str | None = None
    response_attempts: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _empty_output_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def _month_start(month: str) -> date:
    return date.fromisoformat(f"{month}-01")


def _next_month(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def _month_end(month: str) -> date:
    return _next_month(_month_start(month)) - timedelta(days=1)


def _iter_months(start: date, end: date) -> list[str]:
    cursor = date(start.year, start.month, 1)
    finish = date(end.year, end.month, 1)
    months: list[str] = []
    while cursor <= finish:
        months.append(cursor.strftime("%Y-%m"))
        cursor = _next_month(cursor)
    return months


def _month_window(month: str, start: date, end: date) -> tuple[date, date]:
    return max(start, _month_start(month)), min(end, _month_end(month))


def _request_url(month: str) -> str:
    return ENDPOINT_TEMPLATE.format(date=_month_start(month).strftime("%Y%m%d"))


def _resume_cache_key() -> str:
    payload = {
        "dataset": DATASET_NAME,
        "endpoint": ENDPOINT_TEMPLATE,
        "official_start": OFFICIAL_START_DATE.isoformat(),
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "journal_schema_version": JOURNAL_SCHEMA_VERSION,
        "output_columns": list(OUTPUT_COLUMNS),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _journal_path(output_dir: Path) -> Path:
    return output_dir / "state" / "journals" / f"{DATASET_NAME}.jsonl"


def _partial_path(output_dir: Path) -> Path:
    return (
        output_dir
        / "state"
        / "partials"
        / f"{DATASET_NAME}.{_resume_cache_key()}.parquet"
    )


def _summary_path(output_dir: Path) -> Path:
    return output_dir / f"{DATASET_NAME}.summary.json"


def _latest_attempt_summary_path(output_dir: Path) -> Path:
    return output_dir / "state" / f"{DATASET_NAME}.latest_attempt.json"


def _canonical_path(output_dir: Path) -> Path:
    return output_dir / f"{DATASET_NAME}.parquet"


def _raw_path(output_dir: Path, month: str) -> Path:
    return output_dir / "raw" / DATASET_NAME / f"{month}.json"


def _http_session() -> requests.Session:
    session = getattr(_HTTP_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _HTTP_LOCAL.session = session
    return session


def _configure_rate_limiter(requested_interval: float | None) -> float:
    global _RATE_LIMITER
    interval = resolve_request_interval("tw_public", requested_interval)
    with _RATE_LIMITER_LOCK:
        _RATE_LIMITER = SharedRateLimiter(interval, name="tw_public")
    return interval


def _global_rate_limiter() -> SharedRateLimiter:
    global _RATE_LIMITER
    if _RATE_LIMITER is None:
        with _RATE_LIMITER_LOCK:
            if _RATE_LIMITER is None:
                interval = resolve_request_interval("tw_public", None)
                _RATE_LIMITER = SharedRateLimiter(interval, name="tw_public")
    return _RATE_LIMITER


def _response_is_security_block(response: requests.Response) -> bool:
    if int(response.status_code) not in {307, 403}:
        return False
    text = (
        bytes(getattr(response, "content", b""))[:4096]
        .decode("utf-8", errors="ignore")
        .lower()
    )
    return (
        "for security reasons" in text
        or "page can not be accessed" in text
        or "安全性考量" in text
    )


def _retry_delay(
    response: requests.Response | None,
    attempt: int,
    retry_backoff: float,
) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(120.0, max(0.0, float(retry_after)))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return min(
                        120.0,
                        max(
                            0.0, (retry_at - datetime.now(timezone.utc)).total_seconds()
                        ),
                    )
                except (TypeError, ValueError, OverflowError):
                    pass
        if _response_is_security_block(response):
            return max(
                WAF_COOLDOWN_SECONDS,
                max(0.0, float(retry_backoff)) * (2**attempt),
            )
    return max(0.0, float(retry_backoff)) * (2**attempt)


def _request_once(url: str, args: argparse.Namespace) -> requests.Response:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://wwwc.twse.com.tw/zh/indices/taiex/mi-5min-hist.html",
        "X-Requested-With": "XMLHttpRequest",
    }
    _global_rate_limiter().wait()
    return _http_session().get(
        url,
        headers=headers,
        timeout=int(args.timeout),
        verify=bool(args.verify_ssl),
        allow_redirects=False,
    )


def _decode_json(content: bytes) -> Any:
    if not content:
        raise MonthPayloadError("official response body is empty")
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    else:
        raise MonthPayloadError(f"official response cannot be decoded: {last_error}")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise MonthPayloadError(f"official response is not valid JSON: {exc}") from exc


def _normalize_field(value: Any) -> str:
    return re.sub(r"[\s_()（）%％]+", "", str(value)).lower()


def _alias_lookup(columns: list[Any]) -> dict[str, str]:
    normalized = {_normalize_field(column): str(column) for column in columns}
    selected: dict[str, str] = {}
    for canonical, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            match = normalized.get(_normalize_field(alias))
            if match is not None:
                selected[canonical] = match
                break
    missing = [column for column in PRICE_COLUMNS if column not in selected]
    if missing:
        raise MonthPayloadError(
            f"official response schema is missing required TAIEX fields: {missing}"
        )
    return selected


def _declared_months(value: Any) -> set[str]:
    text = str(value or "")
    months: set[str] = set()
    for roc_year, month in re.findall(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月", text):
        months.add(f"{int(roc_year) + 1911:04d}-{int(month):02d}")
    for year, month in re.findall(r"(?<!\d)(20\d{2})[-/]([01]?\d)(?!\d)", text):
        parsed_month = int(month)
        if 1 <= parsed_month <= 12:
            months.add(f"{int(year):04d}-{parsed_month:02d}")
    return months


def _parse_date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("年", "/").replace("月", "/").replace("日", "")
    text = text.replace(".", "/").replace("-", "/")
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    if re.fullmatch(r"\d{6,7}", text):
        year_digits = len(text) - 4
        return date(
            int(text[:year_digits]) + 1911,
            int(text[year_digits : year_digits + 2]),
            int(text[-2:]),
        )
    parts = [part for part in text.split("/") if part]
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        year = int(parts[0])
        if year < 1911:
            year += 1911
        return date(year, int(parts[1]), int(parts[2]))
    raise MonthPayloadError(f"invalid official TAIEX date: {value!r}")


def _parse_number(value: Any, *, field: str) -> float:
    text = str(value).strip().replace(",", "").replace("−", "-").replace("－", "-")
    if text in {"", "--", "---", "N/A", "null", "None"}:
        raise MonthPayloadError(f"missing numeric {field}: {value!r}")
    try:
        number = float(text)
    except ValueError as exc:
        raise MonthPayloadError(f"invalid numeric {field}: {value!r}") from exc
    if not (number > 0.0 and number < float("inf")):
        raise MonthPayloadError(f"non-positive or non-finite {field}: {value!r}")
    return number


def _extract_records(payload: Any, requested_month: str) -> list[dict[str, Any]]:
    title_parts: list[str] = []
    fields: list[Any] | None = None
    rows: list[Any] | None = None

    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        status = str(payload.get("stat", payload.get("status", ""))).strip()
        if status and status.lower() not in {"ok", "success"}:
            raise MonthPayloadError(f"official response status is not OK: {status}")
        title_parts.extend(
            str(payload.get(key, "")) for key in ("title", "date", "reportTitle")
        )
        tables = payload.get("tables")
        if isinstance(tables, list):
            candidates = [
                table
                for table in tables
                if isinstance(table, dict)
                and "發行量加權股價指數歷史資料" in str(table.get("title", ""))
            ]
            if not candidates and len(tables) == 1 and isinstance(tables[0], dict):
                candidates = [tables[0]]
            if len(candidates) != 1:
                raise MonthPayloadError(
                    "official response does not contain exactly one TAIEX history table"
                )
            table = candidates[0]
            title_parts.append(str(table.get("title", "")))
            fields = (
                table.get("fields") if isinstance(table.get("fields"), list) else None
            )
            rows = table.get("data") if isinstance(table.get("data"), list) else None
        else:
            fields = (
                payload.get("fields")
                if isinstance(payload.get("fields"), list)
                else None
            )
            rows = (
                payload.get("data") if isinstance(payload.get("data"), list) else None
            )
    else:
        raise MonthPayloadError(
            f"official response root must be an object or list, got {type(payload).__name__}"
        )

    declared = set().union(*(_declared_months(part) for part in title_parts))
    mismatched = sorted(month for month in declared if month != requested_month)
    if mismatched:
        raise MonthPayloadError(
            f"official response month mismatch: requested={requested_month} "
            f"declared={','.join(sorted(declared))}"
        )
    if rows is None:
        raise MonthPayloadError("official response is missing a data array")
    if not rows:
        # The TAIEX has at least one session in every complete Gregorian month.
        # A bare/structured empty response is therefore not enough to certify
        # coverage and must remain retryable.
        raise MonthPayloadError(
            f"official response contains no TAIEX rows for {requested_month}"
        )

    records: list[dict[str, Any]] = []
    if all(isinstance(row, dict) for row in rows):
        for row in rows:
            assert isinstance(row, dict)
            lookup = _alias_lookup(list(row))
            records.append({name: row[source] for name, source in lookup.items()})
        return records

    if fields is None:
        raise MonthPayloadError("official row-array response is missing fields")
    lookup = _alias_lookup(fields)
    index_by_name = {str(field): idx for idx, field in enumerate(fields)}
    for row in rows:
        if not isinstance(row, list):
            raise MonthPayloadError("official data rows mix incompatible shapes")
        record: dict[str, Any] = {}
        for name, source in lookup.items():
            index = index_by_name[source]
            if index >= len(row):
                raise MonthPayloadError(
                    f"official row is shorter than declared schema for {name}"
                )
            record[name] = row[index]
        records.append(record)
    return records


def _parse_month_payload(
    content: bytes,
    requested_month: str,
    *,
    range_start: date,
    range_end: date,
) -> pl.DataFrame:
    payload = _decode_json(content)
    raw_records = _extract_records(payload, requested_month)
    records: list[dict[str, Any]] = []
    seen_dates: set[date] = set()
    for raw in raw_records:
        row_date = _parse_date_value(raw["date"])
        if row_date.strftime("%Y-%m") != requested_month:
            raise MonthPayloadError(
                f"official row month mismatch: requested={requested_month} row={row_date}"
            )
        if row_date in seen_dates:
            raise MonthPayloadError(f"duplicate official TAIEX date: {row_date}")
        seen_dates.add(row_date)
        opening = _parse_number(raw["opening_index"], field="opening_index")
        highest = _parse_number(raw["highest_index"], field="highest_index")
        lowest = _parse_number(raw["lowest_index"], field="lowest_index")
        closing = _parse_number(raw["closing_index"], field="closing_index")
        if highest + 1e-9 < max(opening, lowest, closing):
            raise MonthPayloadError(
                f"invalid TAIEX OHLC high on {row_date}: "
                f"open={opening} high={highest} low={lowest} close={closing}"
            )
        if lowest - 1e-9 > min(opening, highest, closing):
            raise MonthPayloadError(
                f"invalid TAIEX OHLC low on {row_date}: "
                f"open={opening} high={highest} low={lowest} close={closing}"
            )
        if range_start <= row_date <= range_end:
            records.append(
                {
                    "date": row_date,
                    "opening_index": opening,
                    "highest_index": highest,
                    "lowest_index": lowest,
                    "closing_index": closing,
                }
            )
    if not records:
        raise MonthPayloadError(
            f"official response has no rows inside requested range "
            f"{range_start}..{range_end} for {requested_month}"
        )
    return pl.DataFrame(
        records, schema={name: OUTPUT_SCHEMA[name] for name in PRICE_COLUMNS}
    ).sort("date")


def _attach_provenance(
    frame: pl.DataFrame,
    *,
    month: str,
    url: str,
    fetched_at: str,
) -> pl.DataFrame:
    return frame.with_columns(
        [
            pl.lit(DATASET_NAME).alias("_dataset"),
            pl.lit(SOURCE_NAME).alias("_source"),
            pl.lit(SOURCE_PRODUCT).alias("_source_product"),
            pl.lit(month).alias("_request_month"),
            pl.lit(fetched_at).alias("_downloaded_at_utc"),
            pl.lit(url).alias("_url"),
        ]
    ).select(OUTPUT_COLUMNS)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    atomic_write_bytes(path, content, durable=True)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_json(
        path,
        payload,
        durable=True,
        ensure_ascii=False,
        sort_keys=True,
    )


def _write_parquet_atomic(path: Path, frame: pl.DataFrame) -> None:
    atomic_write_parquet(path, frame, compression="snappy", write_statistics=True)


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _summary_still_certifies_canonical(
    summary: dict[str, Any], canonical_path: Path
) -> bool:
    """Return whether an existing success summary still certifies its parquet.

    A failed refresh attempt is operational evidence, but it must not mutate the
    acceptance receipt of the last atomically promoted canonical dataset. The
    caller records every attempt separately and preserves this summary only
    after re-verifying the exact canonical bytes.
    """

    if (
        int(summary.get("schema_version", -1)) != SUMMARY_SCHEMA_VERSION
        or summary.get("dataset") != DATASET_NAME
        or summary.get("coverage_complete") is not True
        or summary.get("baseline_established") is not True
        or summary.get("replacement_promoted") is not True
        or int(summary.get("unresolved_month_count", -1)) != 0
        or int(summary.get("failed_count", -1)) != 0
        or not canonical_path.is_file()
    ):
        return False
    receipt = summary.get("output_receipt")
    if not isinstance(receipt, dict):
        return False
    try:
        expected_size = int(receipt.get("size", -1))
    except (TypeError, ValueError):
        return False
    expected_sha256 = str(receipt.get("sha256") or "").lower()
    receipt_path = Path(str(receipt.get("path") or ""))
    if (
        receipt_path.name != canonical_path.name
        or expected_size != canonical_path.stat().st_size
        or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
    ):
        return False
    return _file_receipt(canonical_path)["sha256"] == expected_sha256


def _response_audit(response: requests.Response, attempts: int) -> dict[str, Any]:
    content = bytes(response.content)
    snippet = re.sub(
        r"\s+", " ", content[:1024].decode("utf-8", errors="ignore")
    ).strip()[:256]
    return {
        "http_status": int(response.status_code),
        "content_type": str(response.headers.get("Content-Type", "")).strip() or None,
        "content_length": len(content),
        "body_sha256": hashlib.sha256(content).hexdigest(),
        "body_snippet": snippet or None,
        "response_attempts": attempts,
    }


def _download_month(
    month: str,
    args: argparse.Namespace,
    output_dir: Path,
    range_start: date,
    range_end: date,
) -> MonthResult:
    url = _request_url(month)
    response: requests.Response | None = None
    attempts = 0
    last_error = "request did not run"
    audit: dict[str, Any] = {}
    retries = max(0, int(args.retries))
    transient_statuses = {403, 408, 429, 500, 502, 503, 504}

    for attempt in range(retries + 1):
        attempts += 1
        try:
            response = _request_once(url, args)
            audit = _response_audit(response, attempts)
            security_block = _response_is_security_block(response)
            if security_block:
                raise RuntimeError(
                    f"TWSE provider-wide security block HTTP {response.status_code}"
                )
            if int(response.status_code) in transient_statuses:
                raise RuntimeError(f"transient TWSE HTTP {response.status_code}")
            response.raise_for_status()
            parsed = _parse_month_payload(
                bytes(response.content),
                month,
                range_start=range_start,
                range_end=range_end,
            )
            fetched_at = _utc_now()
            frame = _attach_provenance(
                parsed,
                month=month,
                url=url,
                fetched_at=fetched_at,
            )
            raw_path: Path | None = None
            if not bool(args.skip_raw):
                raw_path = _raw_path(output_dir, month)
                _atomic_write_bytes(raw_path, bytes(response.content))
            return MonthResult(
                month=month,
                url=url,
                frame=frame,
                raw_path=str(raw_path) if raw_path else None,
                **audit,
            )
        except (requests.RequestException, MonthPayloadError, RuntimeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt >= retries:
                break
            delay = _retry_delay(response, attempt, float(args.retry_backoff))
            _global_rate_limiter().defer(delay)
            if delay > 0.0:
                time.sleep(delay)

    failure_raw: Path | None = None
    if response is not None and not bool(args.skip_raw):
        digest = str(
            audit.get("body_sha256") or hashlib.sha256(response.content).hexdigest()
        )
        failure_raw = (
            output_dir / "raw_failures" / DATASET_NAME / f"{month}.{digest[:16]}.json"
        )
        _atomic_write_bytes(failure_raw, bytes(response.content))
    return MonthResult(
        month=month,
        url=url,
        frame=_empty_output_frame(),
        error=last_error,
        raw_path=str(failure_raw) if failure_raw else None,
        response_attempts=attempts,
        **{key: value for key, value in audit.items() if key != "response_attempts"},
    )


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


def _load_journal_latest(path: Path) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return latest
    raw_text = path.read_text(encoding="utf-8")
    lines = raw_text.splitlines()
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except (json.JSONDecodeError, TypeError) as exc:
            if index == len(lines) - 1 and not raw_text.endswith("\n"):
                continue
            raise ValueError(
                f"corrupt non-terminal JSONL journal record {index + 1}: {path}"
            ) from exc
        if not isinstance(payload, dict):
            continue
        if payload.get("dataset") != DATASET_NAME:
            continue
        if payload.get("cache_key") != _resume_cache_key():
            continue
        if int(payload.get("schema_version", -1)) != JOURNAL_SCHEMA_VERSION:
            continue
        if payload.get("status") == "reset":
            latest.clear()
            continue
        month = str(payload.get("month", ""))
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            continue
        latest[month] = payload
    return latest


def _load_journal_latest_data(path: Path) -> dict[str, dict[str, Any]]:
    """Load the newest accepted data receipt per month after the last reset."""

    latest_data: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return latest_data
    raw_text = path.read_text(encoding="utf-8")
    lines = raw_text.splitlines()
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except (json.JSONDecodeError, TypeError) as exc:
            if index == len(lines) - 1 and not raw_text.endswith("\n"):
                continue
            raise ValueError(
                f"corrupt non-terminal JSONL journal record {index + 1}: {path}"
            ) from exc
        if not isinstance(payload, dict):
            continue
        if payload.get("dataset") != DATASET_NAME:
            continue
        if payload.get("cache_key") != _resume_cache_key():
            continue
        if int(payload.get("schema_version", -1)) != JOURNAL_SCHEMA_VERSION:
            continue
        if payload.get("status") == "reset":
            latest_data.clear()
            continue
        month = str(payload.get("month", ""))
        if payload.get("status") == "data" and re.fullmatch(r"\d{4}-\d{2}", month):
            latest_data[month] = payload
    return latest_data


def _event_for_result(
    result: MonthResult,
    *,
    status: str,
    source: str,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    raw_receipt: dict[str, Any] | None = None
    if result.raw_path:
        path = Path(result.raw_path)
        if path.is_file():
            raw_receipt = _file_receipt(path)
            if output_dir is not None:
                try:
                    raw_receipt["path"] = str(
                        path.resolve().relative_to(output_dir.resolve())
                    )
                except (OSError, ValueError):
                    pass
    return {
        "schema_version": JOURNAL_SCHEMA_VERSION,
        "cache_key": _resume_cache_key(),
        "dataset": DATASET_NAME,
        "month": result.month,
        "recorded_at_utc": _utc_now(),
        "status": status,
        "source": source,
        "url": result.url,
        "rows": int(result.frame.height),
        "data_sha256": _frame_data_sha256(result.frame),
        "raw_receipt": raw_receipt,
        "http_status": result.http_status,
        "content_type": result.content_type,
        "content_length": result.content_length,
        "body_sha256": result.body_sha256,
        "body_snippet": result.body_snippet,
        "response_attempts": int(result.response_attempts),
        "error": result.error,
    }


def _frame_data_sha256(frame: pl.DataFrame) -> str:
    if frame.is_empty():
        return hashlib.sha256(b"").hexdigest()
    canonical = frame.select(PRICE_COLUMNS).sort("date")
    payload = [
        [
            row[0].isoformat(),
            *[format(float(value), ".17g") for value in row[1:]],
        ]
        for row in canonical.iter_rows()
    ]
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _append_reset_event(path: Path) -> None:
    _append_jsonl(
        path,
        {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "cache_key": _resume_cache_key(),
            "dataset": DATASET_NAME,
            "recorded_at_utc": _utc_now(),
            "status": "reset",
            "reason": "explicit --no-resume",
        },
    )


def _validate_output_frame(
    frame: pl.DataFrame, *, path: Path | None = None
) -> pl.DataFrame:
    missing = [column for column in OUTPUT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(
            f"{path or DATASET_NAME} is missing required columns: {missing}"
        )
    normalized = frame.select(
        [
            pl.col("date").cast(pl.Date, strict=False),
            *[
                pl.col(column).cast(pl.Float64, strict=False).alias(column)
                for column in PRICE_COLUMNS[1:]
            ],
            *[
                pl.col(column).cast(pl.Utf8, strict=False).alias(column)
                for column in OUTPUT_COLUMNS[len(PRICE_COLUMNS) :]
            ],
        ]
    ).drop_nulls(list(PRICE_COLUMNS))
    if normalized.height != frame.height:
        raise ValueError(
            f"{path or DATASET_NAME} contains null or unparseable required values"
        )
    if normalized.get_column("date").n_unique() != normalized.height:
        raise ValueError(f"{path or DATASET_NAME} contains duplicate dates")
    invalid = normalized.filter(
        ~pl.col("opening_index").is_finite()
        | ~pl.col("highest_index").is_finite()
        | ~pl.col("lowest_index").is_finite()
        | ~pl.col("closing_index").is_finite()
        | (pl.col("opening_index") <= 0)
        | (pl.col("highest_index") <= 0)
        | (pl.col("lowest_index") <= 0)
        | (pl.col("closing_index") <= 0)
        | (
            pl.col("highest_index")
            < pl.max_horizontal("opening_index", "lowest_index", "closing_index")
        )
        | (
            pl.col("lowest_index")
            > pl.min_horizontal("opening_index", "highest_index", "closing_index")
        )
        | pl.col("_dataset").is_null()
        | (pl.col("_dataset") != DATASET_NAME)
        | pl.col("_source").is_null()
        | (pl.col("_source") != SOURCE_NAME)
        | pl.col("_source_product").is_null()
        | (pl.col("_source_product") != SOURCE_PRODUCT)
        | pl.col("_request_month").is_null()
        | (pl.col("_request_month") != pl.col("date").dt.strftime("%Y-%m"))
        | pl.col("_downloaded_at_utc").is_null()
        | (pl.col("_downloaded_at_utc").str.len_chars() == 0)
        | pl.col("_url").is_null()
        | (pl.col("_url").str.len_chars() == 0)
    )
    if not invalid.is_empty():
        raise ValueError(
            f"{path or DATASET_NAME} contains invalid OHLC/provenance rows"
        )
    return normalized.sort("date")


def _read_output_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        return _empty_output_frame()
    return _validate_output_frame(pl.read_parquet(path), path=path)


def _overlay_frame(
    base: pl.DataFrame, incoming: pl.DataFrame, start: date, end: date
) -> pl.DataFrame:
    kept = base.filter(~pl.col("date").is_between(start, end, closed="both"))
    merged = pl.concat([kept, incoming], how="diagonal_relaxed")
    return (
        _validate_output_frame(merged)
        if not merged.is_empty()
        else _empty_output_frame()
    )


def _overlay_all(base: pl.DataFrame, incoming: pl.DataFrame) -> pl.DataFrame:
    if incoming.is_empty():
        return base
    incoming_dates = incoming.get_column("date").to_list()
    kept = base.filter(~pl.col("date").is_in(incoming_dates))
    return _validate_output_frame(pl.concat([kept, incoming], how="diagonal_relaxed"))


def _resolved_months(
    months: list[str],
    latest: dict[str, dict[str, Any]],
    frame: pl.DataFrame,
    start: date,
    end: date,
) -> set[str]:
    resolved: set[str] = set()
    for month in months:
        event = latest.get(month)
        if not event or event.get("status") != "data":
            continue
        expected_rows = int(event.get("rows", 0) or 0)
        if expected_rows <= 0:
            continue
        window_start, window_end = _month_window(month, start, end)
        month_frame = frame.filter(
            pl.col("date").is_between(window_start, window_end, closed="both")
        )
        if month_frame.height == expected_rows and str(
            event.get("data_sha256", "")
        ) == _frame_data_sha256(month_frame):
            resolved.add(month)
    return resolved


def _bootstrap_raw_month(
    month: str,
    args: argparse.Namespace,
    output_dir: Path,
    start: date,
    end: date,
) -> MonthResult | None:
    path = _raw_path(output_dir, month)
    if not path.is_file():
        return None
    window_start, window_end = _month_window(month, start, end)
    try:
        parsed = _parse_month_payload(
            path.read_bytes(),
            month,
            range_start=window_start,
            range_end=window_end,
        )
    except (OSError, MonthPayloadError):
        return None
    url = _request_url(month)
    return MonthResult(
        month=month,
        url=url,
        frame=_attach_provenance(
            parsed,
            month=month,
            url=url,
            fetched_at=_utc_now(),
        ),
        raw_path=str(path),
        content_length=int(path.stat().st_size),
        body_sha256=_file_receipt(path)["sha256"],
    )


def _read_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the official monthly TAIEX OHLC archive from "
            "TWSE MI_5MINS_HIST with durable per-month resume state."
        )
    )
    parser.add_argument(
        "--mode", choices=("rebuild", "repair", "daily"), default="daily"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--start-date", default="earliest")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=1.0)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument("--daily-overlap-days", type=int, default=7)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--skip-raw", action="store_true")
    parser.add_argument(
        "--verify-ssl", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--progress", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=None,
        help="Optional smoke-test cap; incomplete coverage remains nonzero.",
    )
    return parser.parse_args(argv)


def _effective_range(args: argparse.Namespace) -> tuple[date, date]:
    end = date.fromisoformat(resolve_end_date(str(args.end_date)))
    if str(args.start_date).strip().lower() == "earliest":
        start = OFFICIAL_START_DATE
    else:
        start = max(OFFICIAL_START_DATE, date.fromisoformat(str(args.start_date)))
    if start > end:
        raise ValueError(f"effective start date {start} is after end date {end}")
    return start, end


def _daily_refresh_months(end: date, overlap_days: int) -> set[str]:
    first = end - timedelta(days=max(1, int(overlap_days)) - 1)
    return set(_iter_months(first, end))


def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    canonical_path = _canonical_path(output_dir)
    partial_path = _partial_path(output_dir)
    journal_path = _journal_path(output_dir)
    summary_path = _summary_path(output_dir)
    previous_summary = _read_summary(summary_path)
    interval = _configure_rate_limiter(args.request_interval)
    print(f"[twse-taiex] {describe_rate_limit('tw_public', interval)}", flush=True)

    start: date | None = None
    end: date | None = None
    months: list[str] = []
    latest: dict[str, dict[str, Any]] = {}
    working = _empty_output_frame()
    requested_network: list[str] = []
    resumed_before = 0
    raw_resumed = 0
    promoted = False
    fatal_error: str | None = None

    try:
        start, end = _effective_range(args)
        months = _iter_months(start, end)
        if args.mode == "daily" and (
            not canonical_path.is_file()
            or previous_summary.get("baseline_established") is not True
        ):
            raise RuntimeError(
                "daily mode requires an established rebuild/repair TAIEX baseline"
            )

        if not bool(args.resume):
            _append_reset_event(journal_path)
            partial_path.unlink(missing_ok=True)
            latest = {}
        else:
            latest = _load_journal_latest(journal_path)

        canonical = (
            _read_output_frame(canonical_path)
            if args.mode in {"repair", "daily"}
            else _empty_output_frame()
        )
        partial = (
            _read_output_frame(partial_path)
            if bool(args.resume)
            else _empty_output_frame()
        )
        working = _overlay_all(canonical, partial)

        forced_refresh = (
            _daily_refresh_months(end, int(args.daily_overlap_days))
            if args.mode == "daily"
            else set()
        )
        resolved = _resolved_months(months, latest, working, start, end)
        resumed_before = len(resolved - forced_refresh)
        unresolved = [
            month
            for month in months
            if month not in resolved and month not in forced_refresh
        ]

        if bool(args.resume):
            for month in unresolved:
                result = _bootstrap_raw_month(month, args, output_dir, start, end)
                if result is None:
                    continue
                window_start, window_end = _month_window(month, start, end)
                working = _overlay_frame(
                    working, result.frame, window_start, window_end
                )
                _write_parquet_atomic(partial_path, working)
                event = _event_for_result(
                    result,
                    status="data",
                    source="raw_resume",
                    output_dir=output_dir,
                )
                _append_jsonl(journal_path, event)
                latest[month] = event
                raw_resumed += 1

        resolved = _resolved_months(months, latest, working, start, end)
        requested_network = [
            month
            for month in months
            if month not in resolved or month in forced_refresh
        ]
        if args.max_months is not None:
            requested_network = requested_network[: max(0, int(args.max_months))]

        def worker(month: str) -> MonthResult:
            window_start, window_end = _month_window(month, start, end)
            return _download_month(
                month,
                args,
                output_dir,
                window_start,
                window_end,
            )

        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            futures = {
                executor.submit(worker, month): month for month in requested_network
            }
            progress = tqdm(
                total=len(futures),
                desc="twse_taiex_ohlc:months",
                unit="month",
                disable=not bool(args.progress),
            )
            try:
                for future in as_completed(futures):
                    result = future.result()
                    if result.error is None:
                        window_start, window_end = _month_window(
                            result.month, start, end
                        )
                        working = _overlay_frame(
                            working,
                            result.frame,
                            window_start,
                            window_end,
                        )
                        _write_parquet_atomic(partial_path, working)
                        event = _event_for_result(
                            result,
                            status="data",
                            source="network",
                            output_dir=output_dir,
                        )
                    else:
                        event = _event_for_result(
                            result,
                            status="failed",
                            source="network",
                            output_dir=output_dir,
                        )
                    _append_jsonl(journal_path, event)
                    latest[result.month] = event
                    progress.update(1)
                    progress.set_postfix_str(
                        f"ok={sum(v.get('status') == 'data' for v in latest.values())} "
                        f"failed={sum(v.get('status') == 'failed' for v in latest.values())}"
                    )
            finally:
                progress.close()

        resolved = _resolved_months(months, latest, working, start, end)
        coverage_complete = len(resolved) == len(months)
        if coverage_complete:
            if args.mode == "rebuild":
                output = working.filter(
                    pl.col("date").is_between(start, end, closed="both")
                )
            else:
                output = working
            output = _validate_output_frame(output)
            _write_parquet_atomic(canonical_path, output)
            promoted = True
    except Exception as exc:
        coverage_complete = False
        fatal_error = f"{type(exc).__name__}: {exc}"

    if start is None or end is None:
        effective_start = None
        effective_end = None
        unresolved_months = months
    else:
        effective_start = start.isoformat()
        effective_end = end.isoformat()
        try:
            resolved = _resolved_months(months, latest, working, start, end)
        except Exception:
            resolved = set()
        unresolved_months = [month for month in months if month not in resolved]
    failed_months = {
        month: str(event.get("error") or "failed")
        for month, event in latest.items()
        if event.get("status") == "failed" and month in months
    }
    baseline_established = bool(previous_summary.get("baseline_established")) or (
        bool(coverage_complete) and args.mode in {"rebuild", "repair"}
    )
    output_receipt = _file_receipt(canonical_path) if promoted else None
    if output_receipt is not None:
        output_receipt["path"] = canonical_path.name
    try:
        output_rows = (
            int(_read_output_frame(canonical_path).height)
            if canonical_path.is_file()
            else 0
        )
    except Exception as exc:
        output_rows = 0
        if fatal_error is None:
            fatal_error = f"{type(exc).__name__}: {exc}"
        coverage_complete = False
        promoted = False
        output_receipt = None
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "dataset": DATASET_NAME,
        "source": SOURCE_NAME,
        "source_product": SOURCE_PRODUCT,
        "official_start_date": OFFICIAL_START_DATE.isoformat(),
        "mode": args.mode,
        "updated_at_utc": _utc_now(),
        "effective_start_date": effective_start,
        "effective_end_date": effective_end,
        "cache_key": _resume_cache_key(),
        "coverage_complete": bool(coverage_complete),
        "baseline_established": baseline_established,
        "replacement_promoted": promoted,
        "month_count": len(months),
        "network_requested_months": requested_network,
        "network_requested_count": len(requested_network),
        "network_succeeded_count": sum(
            latest.get(month, {}).get("status") == "data" for month in requested_network
        ),
        "resumed_month_count": resumed_before,
        "raw_resumed_month_count": raw_resumed,
        "unresolved_months": unresolved_months,
        "unresolved_month_count": len(unresolved_months),
        "failed_months": failed_months,
        "failed_month_count": len(failed_months),
        "failed_count": len(failed_months),
        "fatal_error": fatal_error,
        "canonical_path": str(canonical_path),
        "partial_path": str(partial_path),
        "journal_path": str(journal_path),
        "output_rows": output_rows,
        "output_receipt": output_receipt,
        "request_interval_seconds": interval,
        "requests_per_second": 1.0 / interval,
        "rate_limit_basis": provider_rate_limit("tw_public").basis,
        "rate_limit_source": provider_rate_limit("tw_public").source_url,
    }
    attempt_summary = dict(summary)
    recovered_from_journal = False
    if not coverage_complete and start is not None and end is not None:
        try:
            canonical = _read_output_frame(canonical_path)
            accepted_events = _load_journal_latest_data(journal_path)
            accepted_months = _resolved_months(
                months,
                accepted_events,
                canonical,
                start,
                end,
            )
            recovered_from_journal = len(accepted_months) == len(months)
        except (OSError, TypeError, ValueError):
            recovered_from_journal = False
        if recovered_from_journal:
            receipt = _file_receipt(canonical_path)
            receipt["path"] = canonical_path.name
            latest_refresh_failures = dict(failed_months)
            summary.update(
                {
                    "coverage_complete": True,
                    "baseline_established": True,
                    "replacement_promoted": True,
                    "unresolved_months": [],
                    "unresolved_month_count": 0,
                    "failed_months": {},
                    "failed_month_count": 0,
                    "failed_count": 0,
                    "fatal_error": None,
                    "output_rows": int(canonical.height),
                    "output_receipt": receipt,
                    "canonical_recovered_from_journal": True,
                    "nonblocking_latest_refresh_failed_months": latest_refresh_failures,
                }
            )
            coverage_complete = True
            promoted = True
    preserve_previous = bool(
        not coverage_complete
        and _summary_still_certifies_canonical(previous_summary, canonical_path)
    )
    attempt_summary["preserved_previous_canonical_summary"] = bool(
        preserve_previous or recovered_from_journal
    )
    attempt_summary["canonical_recovered_from_journal"] = recovered_from_journal
    _atomic_write_json(_latest_attempt_summary_path(output_dir), attempt_summary)
    if not preserve_previous:
        _atomic_write_json(summary_path, summary)
    if coverage_complete:
        print(
            f"[twse-taiex] coverage complete months={len(months)} "
            f"rows={summary['output_rows']} output={canonical_path}",
            flush=True,
        )
        return 0
    print(
        f"[twse-taiex] coverage incomplete unresolved={len(unresolved_months)} "
        f"failed={len(failed_months)} error={fatal_error or ''}",
        flush=True,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return _run(args)


if __name__ == "__main__":
    raise SystemExit(main())
