from __future__ import annotations

import argparse
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any
from urllib.parse import parse_qs, urlparse
import xml.etree.ElementTree as ET
from zipfile import BadZipFile, ZipFile

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import describe_rate_limit
from downloader.download_tw_public_data import (
    _configure_tw_public_rate_limiter,
    _http_get,
)


TWSE_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
TPEX_CURRENT_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"
TPEX_HISTORICAL_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQHis"
TPEX_MONTHLY_REPORT_URL = "https://www.tpex.org.tw/www/zh-tw/statistics/monthlyRptMkt"
TPEX_MONTHLY_DOWNLOAD_URL = (
    "https://www.tpex.org.tw/www/zh-tw/statistics/monthlyRptMktDl"
)
TPEX_HISTORICAL_GAP_START = date(2004, 9, 3)
TPEX_HISTORICAL_GAP_END = date(2005, 2, 13)
TPEX_MONTHLY_REPORT_TYPES = {6: "right", 7: "dividend"}
ODS_TABLE_NAMESPACE = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"


@dataclass(slots=True)
class FetchResult:
    market: str
    year: int
    url: str
    rows: list[dict[str, Any]]
    raw_path: str | None
    error: str | None = None
    raw_receipt: dict[str, Any] | None = None


@dataclass(slots=True)
class MonthlyDocumentResult:
    year: int
    month: int
    report_type: int
    query_url: str
    document_url: str | None
    document_id: str | None
    rows: list[dict[str, Any]]
    status: str
    raw_receipts: list[dict[str, Any]]
    error: str | None = None


class MonthlyDocumentIdentityError(ValueError):
    """The official monthly link returned a document for another report."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official TWSE/TPEx ex-right/ex-dividend reference prices by year."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--mode",
        choices=("rebuild", "repair", "daily"),
        default="repair",
        help="rebuild replaces all history; repair merges the requested years; daily refreshes recent years.",
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--daily-overlap-years", type=int, default=2)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help=(
            "Host-global minimum seconds between TW public HTTP requests. "
            "Unspecified endpoints default to the stockAgent 8 req/s policy."
        ),
    )
    parser.add_argument("--skip-raw", action="store_true")
    return parser.parse_args()


def _parse_roc_date(value: Any) -> date | None:
    text = str(value or "").strip().replace("年", "/").replace("月", "/").replace("日", "")
    text = text.replace("-", "/")
    parts = [part for part in text.split("/") if part]
    if len(parts) != 3:
        return None
    try:
        year, month, day = (int(part) for part in parts)
        if year < 1911:
            year += 1911
        return date(year, month, day)
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "--", "---", "N/A", "nan", "null"}:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if result == result and result not in {float("inf"), float("-inf")} else None


def _content_receipt(
    content: bytes,
    *,
    source_url: str,
    path: Path | None = None,
) -> dict[str, Any]:
    return {
        "path": str(path) if path is not None else None,
        "size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "source_url": source_url,
    }


def _write_immutable_raw(
    raw_dir: Path,
    *,
    stem: str,
    suffix: str,
    content: bytes,
    source_url: str,
) -> dict[str, Any]:
    """Write a content-addressed raw response without replacing an earlier receipt."""

    digest = hashlib.sha256(content).hexdigest()
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{stem}-{digest}{suffix}"
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"immutable raw receipt changed unexpectedly: {path}")
        return _content_receipt(content, source_url=source_url, path=path)

    handle = tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=raw_dir, delete=False
    )
    temporary = Path(handle.name)
    try:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise RuntimeError(f"immutable raw receipt collision: {path}")
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)
    return _content_receipt(content, source_url=source_url, path=path)


def _iter_months(start: date, end: date) -> list[tuple[int, int]]:
    if end < start:
        return []
    output: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        output.append((year, month))
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return output


def _monthly_query_document_id(
    payload: Any,
    *,
    year: int,
    month: int,
    report_type: int,
) -> str:
    if report_type not in TPEX_MONTHLY_REPORT_TYPES:
        raise ValueError(f"unsupported TPEx monthly report type: {report_type}")
    if not isinstance(payload, dict):
        raise ValueError("TPEx monthly query response is not a JSON object")
    if str(payload.get("stat", "")).strip().lower() != "ok":
        raise ValueError(f"TPEx monthly query status is not OK: {payload.get('stat')!r}")
    tables = payload.get("tables")
    if not isinstance(tables, list) or len(tables) != 1 or not isinstance(tables[0], dict):
        raise ValueError("TPEx monthly query must contain exactly one table")
    table = tables[0]
    expected_title = "除權交易股票一覽表" if report_type == 6 else "除息交易股票一覽表"
    if str(table.get("title", "")).strip() != expected_title:
        raise ValueError(
            f"TPEx monthly query returned the wrong table title: {table.get('title')!r}"
        )
    fields = table.get("fields")
    data = table.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        raise ValueError("TPEx monthly query has invalid fields/data")
    try:
        period_index = fields.index("日期")
        ods_index = fields.index("下載ODS")
    except ValueError as exc:
        raise ValueError("TPEx monthly query is missing 日期/下載ODS fields") from exc

    expected_period = f"{year - 1911}/{month:02d}"
    document_ids: set[str] = set()
    for values in data:
        if not isinstance(values, list) or max(period_index, ods_index) >= len(values):
            raise ValueError("TPEx monthly query contains a malformed row")
        if str(values[period_index]).strip() != expected_period:
            continue
        link = str(values[ods_index]).strip()
        parsed = urlparse(link)
        if not parsed.path.endswith("/statistics/monthlyRptMktDl"):
            raise ValueError(f"TPEx monthly query returned an unexpected ODS path: {link!r}")
        query = parse_qs(parsed.query)
        document_id = query.get("doc", [""])[0]
        is_ods = query.get("isOds", [""])[0]
        if re.fullmatch(r"[0-9]+", document_id) is None or is_ods.upper() != "Y":
            raise ValueError(f"TPEx monthly query returned an invalid ODS link: {link!r}")
        document_ids.add(document_id)
    if len(document_ids) != 1:
        raise ValueError(
            f"TPEx monthly query expected one document for {expected_period}, "
            f"found {sorted(document_ids)}"
        )
    return next(iter(document_ids))


def _ods_cell_rows(content: bytes, *, max_columns: int = 10) -> list[list[str]]:
    if b"<!DOCTYPE" in content.upper():
        raise ValueError("TPEx monthly ODS must not contain a document type declaration")
    try:
        with ZipFile(BytesIO(content)) as archive:
            names = archive.namelist()
            if names.count("content.xml") != 1:
                raise ValueError("TPEx monthly ODS must contain exactly one content.xml")
            xml_content = archive.read("content.xml")
    except BadZipFile as exc:
        raise ValueError("TPEx monthly response is not a valid ODS archive") from exc
    if b"<!DOCTYPE" in xml_content.upper():
        raise ValueError("TPEx monthly ODS XML must not contain a document type declaration")
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        raise ValueError("TPEx monthly ODS content.xml is malformed") from exc

    output: list[list[str]] = []
    row_tag = f"{{{ODS_TABLE_NAMESPACE}}}table-row"
    cell_tag = f"{{{ODS_TABLE_NAMESPACE}}}table-cell"
    repeat_key = f"{{{ODS_TABLE_NAMESPACE}}}number-columns-repeated"
    for row in root.iter(row_tag):
        values: list[str] = []
        for cell in row.findall(cell_tag):
            value = "".join(cell.itertext()).strip()
            try:
                repeats = int(cell.attrib.get(repeat_key, "1"))
            except ValueError as exc:
                raise ValueError("TPEx monthly ODS has an invalid repeated-cell count") from exc
            if repeats < 1:
                raise ValueError("TPEx monthly ODS has a non-positive repeated-cell count")
            values.extend([value] * min(repeats, max_columns - len(values)))
            if len(values) >= max_columns:
                break
        output.append(values)
    return output


def _compact_cell(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip()


def _parse_tpex_monthly_ods(
    content: bytes,
    *,
    year: int,
    month: int,
    report_type: int,
    document_id: str,
    source_url: str,
) -> list[dict[str, Any]]:
    if report_type not in TPEX_MONTHLY_REPORT_TYPES:
        raise ValueError(f"unsupported TPEx monthly report type: {report_type}")
    rows = _ods_cell_rows(content)
    nonempty = [row for row in rows if row and any(_compact_cell(value) for value in row)]
    expected_event = TPEX_MONTHLY_REPORT_TYPES[report_type]
    expected_title = f"{month}月{'除權' if report_type == 6 else '除息'}交易股票一覽表"
    if not nonempty or _compact_cell(nonempty[0][0]) != expected_title:
        actual = _compact_cell(nonempty[0][0]) if nonempty else ""
        raise MonthlyDocumentIdentityError(
            f"TPEx monthly doc={document_id} expected title {expected_title!r}, got {actual!r}"
        )

    expected_header = (
        ["股票名稱", "日期", "除權前參考價", "除權後參考價"]
        if report_type == 6
        else ["股票名稱", "日期", "除息金額", "除息前參考價", "除息後參考價"]
    )
    header_index: int | None = None
    for index, row in enumerate(rows):
        compact = [_compact_cell(value) for value in row[: len(expected_header)]]
        if compact == expected_header:
            header_index = index
            break
    if header_index is None:
        raise MonthlyDocumentIdentityError(
            f"TPEx monthly doc={document_id} is missing the expected {expected_event} header"
        )

    previous_index, reference_index = ((2, 3) if report_type == 6 else (3, 4))
    parsed: list[dict[str, Any]] = []
    seen: set[tuple[date, str, float, float]] = set()
    for row_index, row in enumerate(rows[header_index + 1 :], start=header_index + 1):
        if len(row) <= reference_index:
            continue
        name = str(row[0]).strip()
        day_value = _number(row[1])
        if not name or day_value is None or not float(day_value).is_integer():
            continue
        day = int(day_value)
        if not 1 <= day <= monthrange(year, month)[1]:
            raise ValueError(
                f"TPEx monthly doc={document_id} row={row_index} has invalid day {row[1]!r}"
            )
        previous = _number(row[previous_index])
        reference = _number(row[reference_index])
        if previous is None or previous <= 0 or reference is None or reference <= 0:
            raise ValueError(
                f"TPEx monthly doc={document_id} row={row_index} has invalid reference prices"
            )
        if len(name) > 64 or "年" in name or "交易" in name:
            raise ValueError(
                f"TPEx monthly doc={document_id} row={row_index} has invalid stock name {name!r}"
            )
        event_date = date(year, month, day)
        key = (event_date, _compact_cell(name), previous, reference)
        if key in seen:
            raise ValueError(
                f"TPEx monthly doc={document_id} contains a duplicate event row: {key}"
            )
        seen.add(key)
        parsed.append(
            {
                "date": event_date,
                "report_type": report_type,
                "event_type": expected_event,
                "report_name": name,
                "previous_close": previous,
                "reference_price": reference,
                "document_id": document_id,
                "document_row": row_index,
                "source_url": source_url,
            }
        )
    return parsed


def _payload_rows(payload: Any, *, market: str, url: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("response is not a JSON object")
    stat = str(payload.get("stat", "OK")).strip().lower()
    if stat not in {"", "ok"}:
        raise ValueError(f"official response status is not OK: {payload.get('stat')!r}")
    if isinstance(payload.get("fields"), list):
        fields = payload["fields"]
        data = payload.get("data", [])
    else:
        tables = payload.get("tables")
        if not isinstance(tables, list) or not tables:
            return []
        fields = tables[0].get("fields", [])
        data = tables[0].get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        raise ValueError("official response has invalid fields/data arrays")
    output: list[dict[str, Any]] = []
    for values in data:
        if not isinstance(values, list):
            continue
        row = {str(field): values[idx] if idx < len(values) else None for idx, field in enumerate(fields)}
        event_date = _parse_roc_date(
            row.get("資料日期")
            or row.get("除權息日期")
            or row.get("除權除息交易日")
        )
        symbol = str(row.get("股票代號") or row.get("代號") or "").strip().upper()
        if event_date is None or re.fullmatch(r"[0-9A-Z]{4,6}", symbol) is None:
            continue
        reference = _number(
            row.get("除權息參考價")
            or row.get("除權參考價")
        )
        opening_reference = _number(
            row.get("開盤競價基準")
            or row.get("開始交易基準價")
            or row.get("開市交易基準價")
        )
        previous_close = _number(row.get("除權息前收盤價") or row.get("除權息前收盤價格"))
        reference = reference if reference is not None and reference > 0 else None
        opening_reference = (
            opening_reference
            if opening_reference is not None and opening_reference > 0
            else None
        )
        if reference is None and opening_reference is None:
            continue
        output.append(
            {
                "date": event_date,
                "symbol": symbol,
                "market": market,
                "reference_price": reference,
                "opening_reference_price": opening_reference,
                "previous_close": previous_close,
                "event_type": str(row.get("權/息") or row.get("除權息") or "").strip(),
                "source_url": url,
            }
        )
    return output


def _year_request(market: str, year: int, end_date: date) -> tuple[str, dict[str, str]]:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), end_date)
    if market == "twse":
        start = max(start, date(2003, 5, 5))
        return TWSE_URL, {
            "response": "json",
            "startDate": start.strftime("%Y%m%d"),
            "endDate": end.strftime("%Y%m%d"),
        }
    if year <= 2007:
        start = max(start, date(2000, 9, 1))
        return TPEX_HISTORICAL_URL, {
            "response": "json",
            "startDate": start.strftime("%Y/%m/%d"),
            "endDate": end.strftime("%Y/%m/%d"),
            "code": "",
        }
    return TPEX_CURRENT_URL, {
        "response": "json",
        "startDate": start.strftime("%Y/%m/%d"),
        "endDate": end.strftime("%Y/%m/%d"),
    }


def _fetch_year(
    market: str,
    year: int,
    end_date: date,
    args: argparse.Namespace,
) -> FetchResult:
    url, params = _year_request(market, year, end_date)
    try:
        response = _http_get(
            url,
            params=params,
            timeout=int(args.timeout),
            verify_ssl=True,
            retries=int(args.retries),
            retry_backoff=1.0,
        )
        payload = response.json()
        rows = _payload_rows(payload, market=market, url=response.url)
        raw_path: Path | None = None
        raw_receipt: dict[str, Any] | None = None
        if not args.skip_raw:
            raw_dir = args.output_dir / "raw" / "tw_corporate_action_reference"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_path = raw_dir / f"{market}_{year}.json"
            temporary = raw_path.with_suffix(".json.tmp")
            temporary.write_bytes(response.content)
            os.replace(temporary, raw_path)
            raw_receipt = _content_receipt(
                response.content,
                source_url=response.url,
                path=raw_path,
            )
        return FetchResult(
            market,
            year,
            response.url,
            rows,
            str(raw_path) if raw_path else None,
            raw_receipt=raw_receipt,
        )
    except Exception as exc:
        return FetchResult(market, year, url, [], None, f"{type(exc).__name__}: {exc}")


def _fetch_tpex_monthly_document(
    year: int,
    month: int,
    report_type: int,
    args: argparse.Namespace,
) -> MonthlyDocumentResult:
    start = date(year, month, 1).strftime("%Y/%m/%d")
    query_params = {
        "type": str(report_type),
        "startDate": start,
        "endDate": start,
        "response": "json",
    }
    query_url = TPEX_MONTHLY_REPORT_URL
    query_receipt: dict[str, Any] | None = None
    document_url: str | None = None
    document_id: str | None = None
    raw_receipts: list[dict[str, Any]] = []
    try:
        query_response = _http_get(
            TPEX_MONTHLY_REPORT_URL,
            params=query_params,
            timeout=int(args.timeout),
            verify_ssl=True,
            retries=int(args.retries),
            retry_backoff=1.0,
        )
        query_url = query_response.url
        query_payload = query_response.json()
        document_id = _monthly_query_document_id(
            query_payload,
            year=year,
            month=month,
            report_type=report_type,
        )
        if not args.skip_raw:
            query_receipt = _write_immutable_raw(
                args.output_dir
                / "raw"
                / "tw_corporate_action_reference"
                / "tpex_monthly"
                / f"{year:04d}-{month:02d}",
                stem=f"type-{report_type}-query",
                suffix=".json",
                content=query_response.content,
                source_url=query_response.url,
            )
        else:
            query_receipt = _content_receipt(
                query_response.content,
                source_url=query_response.url,
            )
        raw_receipts.append(query_receipt)

        document_response = _http_get(
            TPEX_MONTHLY_DOWNLOAD_URL,
            params={"doc": document_id, "isOds": "Y"},
            timeout=int(args.timeout),
            verify_ssl=True,
            retries=int(args.retries),
            retry_backoff=1.0,
        )
        document_url = document_response.url
        if not args.skip_raw:
            document_receipt = _write_immutable_raw(
                args.output_dir
                / "raw"
                / "tw_corporate_action_reference"
                / "tpex_monthly"
                / f"{year:04d}-{month:02d}",
                stem=f"type-{report_type}-doc-{document_id}",
                suffix=".ods",
                content=document_response.content,
                source_url=document_response.url,
            )
        else:
            document_receipt = _content_receipt(
                document_response.content,
                source_url=document_response.url,
            )
        raw_receipts.append(document_receipt)
        try:
            rows = _parse_tpex_monthly_ods(
                document_response.content,
                year=year,
                month=month,
                report_type=report_type,
                document_id=document_id,
                source_url=document_response.url,
            )
        except MonthlyDocumentIdentityError as exc:
            return MonthlyDocumentResult(
                year=year,
                month=month,
                report_type=report_type,
                query_url=query_url,
                document_url=document_url,
                document_id=document_id,
                rows=[],
                status="unusable_document",
                raw_receipts=raw_receipts,
                error=f"{type(exc).__name__}: {exc}",
            )
        return MonthlyDocumentResult(
            year=year,
            month=month,
            report_type=report_type,
            query_url=query_url,
            document_url=document_url,
            document_id=document_id,
            rows=rows,
            status="parsed",
            raw_receipts=raw_receipts,
        )
    except Exception as exc:
        return MonthlyDocumentResult(
            year=year,
            month=month,
            report_type=report_type,
            query_url=query_url,
            document_url=document_url,
            document_id=document_id,
            rows=[],
            status="failed",
            raw_receipts=raw_receipts,
            error=f"{type(exc).__name__}: {exc}",
        )


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "size": int(path.stat().st_size), "sha256": digest.hexdigest()}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    content = (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
    )
    temporary = Path(handle.name)
    try:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary, path)
    finally:
        if not handle.closed:
            handle.close()
        temporary.unlink(missing_ok=True)


def _tpex_event_candidates(
    frame: pl.DataFrame,
    *,
    start: date = TPEX_HISTORICAL_GAP_START,
    end: date = TPEX_HISTORICAL_GAP_END,
) -> list[dict[str, Any]]:
    required = {"date", "代號", "名稱", "收盤", "漲跌"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"TPEx daily OHLCV is missing monthly fallback columns: {missing}")
    next_reference = (
        pl.col("次日參考價")
        .cast(pl.String, strict=False)
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        if "次日參考價" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_url = (
        pl.col("_url").cast(pl.String, strict=False)
        if "_url" in frame.columns
        else pl.lit(None, dtype=pl.String)
    )
    name_status = (
        pl.col("_name_decode_status").cast(pl.String, strict=False)
        if "_name_decode_status" in frame.columns
        else pl.lit(None, dtype=pl.String)
    )
    normalized = (
        frame.select(
            pl.col("date")
            .cast(pl.String, strict=False)
            .str.to_date(strict=False)
            .alias("date"),
            pl.col("代號").cast(pl.String, strict=False).str.strip_chars().alias("symbol"),
            pl.col("名稱").cast(pl.String, strict=False).str.strip_chars().alias("name"),
            pl.col("收盤")
            .cast(pl.String, strict=False)
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .alias("close"),
            pl.col("漲跌").cast(pl.String, strict=False).str.strip_chars().alias("event_type"),
            next_reference.alias("next_reference"),
            source_url.alias("source_url"),
            name_status.alias("name_status"),
        )
        .drop_nulls(["date", "symbol"])
        .sort(["symbol", "date"])
    )
    duplicate = (
        normalized.group_by(["date", "symbol"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if duplicate.height:
        raise ValueError(
            f"TPEx daily OHLCV has duplicate date-symbol keys: {duplicate.head(20)}"
        )
    events = (
        normalized.with_columns(
            pl.col("date").shift(1).over("symbol").alias("previous_date"),
            pl.col("close").shift(1).over("symbol").alias("previous_close"),
            pl.col("next_reference")
            .shift(1)
            .over("symbol")
            .alias("previous_daily_next_reference"),
            pl.col("source_url").shift(1).over("symbol").alias("previous_source_url"),
        )
        .filter(
            pl.col("date").is_between(start, end)
            & pl.col("event_type").is_in(["除權", "除息", "除權息"])
        )
        .sort(["date", "symbol"])
    )
    invalid = events.filter(
        pl.col("previous_date").is_null()
        | pl.col("previous_close").is_null()
        | ~pl.col("previous_close").is_finite()
        | (pl.col("previous_close") <= 0)
    )
    if invalid.height:
        raise ValueError(
            "TPEx monthly fallback event keys lack a positive prior official close: "
            f"{invalid.select('date', 'symbol').head(20)}"
        )
    return events.to_dicts()


def _price_key(value: Any) -> float | None:
    parsed = _number(value)
    if parsed is None or parsed <= 0:
        return None
    return round(parsed, 8)


def _official_report_name_match(daily_name: Any, report_name: Any) -> tuple[bool, str]:
    daily = _compact_cell(daily_name)
    report = _compact_cell(report_name)
    if daily and daily == report:
        return True, "exact"
    # A small number of old fixed-width official monthly cells truncate one
    # trailing character (for example 華鎂光 -> 華鎂). Keep this deliberately
    # narrow; date, prior official close, and the one-to-one row constraint
    # still have to match independently.
    if len(report) >= 2 and len(daily) == len(report) + 1 and daily.startswith(report):
        return True, "one_character_official_truncation"
    return False, "mismatch"


def _corporate_row_from_candidate(
    candidate: dict[str, Any],
    *,
    reference_price: float,
    source_url: str,
) -> dict[str, Any]:
    return {
        "date": candidate["date"],
        "symbol": candidate["symbol"],
        "market": "tpex",
        "reference_price": reference_price,
        "opening_reference_price": None,
        "previous_close": candidate["previous_close"],
        "event_type": candidate["event_type"],
        "source_url": source_url,
    }


def _resolve_tpex_monthly_rows(
    candidates: list[dict[str, Any]],
    monthly_rows: list[dict[str, Any]],
    annual_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidate_by_key: dict[tuple[date, str], dict[str, Any]] = {}
    for candidate in candidates:
        key = (candidate["date"], str(candidate["symbol"]))
        if key in candidate_by_key:
            raise ValueError(f"duplicate TPEx monthly fallback event key: {key}")
        candidate_by_key[key] = candidate

    annual_by_key: dict[tuple[date, str], float] = {}
    for row in annual_rows:
        if str(row.get("market", "")).lower() != "tpex":
            continue
        key = (row.get("date"), str(row.get("symbol", "")))
        if key not in candidate_by_key:
            continue
        reference = _price_key(row.get("reference_price"))
        if reference is None:
            continue
        previous = annual_by_key.get(key)
        if previous is not None and previous != reference:
            raise RuntimeError(f"conflicting TPEx annual archive references for {key}")
        annual_by_key[key] = reference

    rows_by_date_and_previous: dict[tuple[date, float], list[dict[str, Any]]] = {}
    for row in monthly_rows:
        event_date = row.get("date")
        previous = _price_key(row.get("previous_close"))
        reference = _price_key(row.get("reference_price"))
        if not isinstance(event_date, date) or previous is None or reference is None:
            raise ValueError(f"invalid parsed TPEx monthly row: {row}")
        rows_by_date_and_previous.setdefault((event_date, previous), []).append(row)

    matches_by_key: dict[tuple[date, str], list[dict[str, Any]]] = {}
    matched_keys_by_document_row: dict[tuple[str, int, int], list[tuple[date, str]]] = {}
    unrecoverable_name_exception_keys: list[str] = []
    normalized_name_alias_keys: list[str] = []
    for key, candidate in candidate_by_key.items():
        previous = _price_key(candidate.get("previous_close"))
        matches = list(rows_by_date_and_previous.get((candidate["date"], previous), []))
        direct_reference = _price_key(candidate.get("previous_daily_next_reference"))
        needs_monthly = key not in annual_by_key and direct_reference is None
        if needs_monthly:
            label = f"{key[0]}:{key[1]}"
            status = str(candidate.get("name_status") or "").strip()
            if status == "official_receipt_name_bytes_unrecoverable":
                if matches:
                    unrecoverable_name_exception_keys.append(label)
            elif status:
                raise RuntimeError(
                    f"unsupported TPEx daily name provenance status for {label}: {status!r}"
                )
            else:
                name_matches: list[dict[str, Any]] = []
                match_kinds: set[str] = set()
                for row in matches:
                    matched, match_kind = _official_report_name_match(
                        candidate.get("name"), row.get("report_name")
                    )
                    if matched:
                        name_matches.append(row)
                        match_kinds.add(match_kind)
                if matches and not name_matches:
                    report_names = sorted(
                        {_compact_cell(row.get("report_name")) for row in matches}
                    )
                    raise RuntimeError(
                        f"TPEx monthly report name does not match official daily name for "
                        f"{label}: daily={candidate.get('name')!r}, reports={report_names}"
                    )
                matches = name_matches
                if "one_character_official_truncation" in match_kinds:
                    normalized_name_alias_keys.append(label)
        matches_by_key[key] = matches
        if not needs_monthly:
            continue
        for row in matches:
            identity = (
                str(row.get("document_id", "")),
                int(row.get("report_type", -1)),
                int(row.get("document_row", -1)),
            )
            matched_keys_by_document_row.setdefault(identity, []).append(key)
    ambiguous_rows = {
        identity: keys
        for identity, keys in matched_keys_by_document_row.items()
        if len(set(keys)) > 1
    }
    if ambiguous_rows:
        examples = list(ambiguous_rows.items())[:20]
        raise RuntimeError(
            "TPEx monthly report rows match more than one date-symbol key: "
            f"{examples}"
        )

    output: list[dict[str, Any]] = []
    archive_keys: list[str] = []
    direct_keys: list[str] = []
    monthly_keys: list[str] = []
    duplicate_consistent_keys: list[str] = []
    unresolved_keys: list[str] = []
    for key, candidate in sorted(candidate_by_key.items()):
        label = f"{key[0]}:{key[1]}"
        matches = matches_by_key[key]
        references = sorted(
            {
                reference
                for reference in (_price_key(row.get("reference_price")) for row in matches)
                if reference is not None
            }
        )
        names = sorted({_compact_cell(row.get("report_name")) for row in matches})
        if len(references) > 1:
            raise RuntimeError(
                f"conflicting TPEx type=6/7 monthly references for {label}: {references}"
            )
        if len(names) > 1:
            raise RuntimeError(
                f"conflicting TPEx type=6/7 monthly names for {label}: {names}"
            )
        if len(matches) > 1:
            duplicate_consistent_keys.append(label)

        annual_reference = annual_by_key.get(key)
        direct_reference = _price_key(candidate.get("previous_daily_next_reference"))
        if annual_reference is not None:
            if direct_reference is not None and annual_reference != direct_reference:
                raise RuntimeError(
                    f"TPEx annual archive conflicts with prior daily next reference for {label}: "
                    f"{annual_reference} != {direct_reference}"
                )
            archive_keys.append(label)
            continue
        if direct_reference is not None:
            output.append(
                _corporate_row_from_candidate(
                    candidate,
                    reference_price=direct_reference,
                    source_url=str(candidate.get("previous_source_url") or TPEX_HISTORICAL_URL),
                )
            )
            direct_keys.append(label)
            continue
        if len(references) == 1:
            source_urls = sorted({str(row["source_url"]) for row in matches})
            output.append(
                _corporate_row_from_candidate(
                    candidate,
                    reference_price=references[0],
                    source_url=source_urls[0],
                )
            )
            monthly_keys.append(label)
            continue
        unresolved_keys.append(label)

    stats = {
        "candidate_event_keys": len(candidate_by_key),
        "annual_archive_keys": len(archive_keys),
        "previous_daily_next_reference_keys": len(direct_keys),
        "monthly_report_keys": len(monthly_keys),
        "duplicate_consistent_monthly_keys": len(duplicate_consistent_keys),
        "unrecoverable_daily_name_exception_keys": len(
            unrecoverable_name_exception_keys
        ),
        "normalized_name_alias_keys": len(normalized_name_alias_keys),
        "unresolved_keys": len(unresolved_keys),
        "annual_archive_examples": archive_keys[:20],
        "previous_daily_next_reference_examples": direct_keys[:20],
        "monthly_report_examples": monthly_keys[:20],
        "duplicate_consistent_monthly_examples": duplicate_consistent_keys[:20],
        "unrecoverable_daily_name_exception_examples": (
            unrecoverable_name_exception_keys[:20]
        ),
        "normalized_name_alias_examples": normalized_name_alias_keys[:20],
        "unresolved_examples": unresolved_keys[:20],
    }
    return output, stats


def main() -> None:
    args = parse_args()
    request_interval = _configure_tw_public_rate_limiter(args.request_interval)
    print(
        f"[tw-corporate-actions] {describe_rate_limit('tw_public', request_interval)}",
        flush=True,
    )
    end_date = date.fromisoformat(args.end_date)
    if args.daily_overlap_years < 1:
        raise ValueError("--daily-overlap-years must be >= 1")
    output_path = args.output_dir / "tw_corporate_action_reference.parquet"
    summary_path = args.output_dir / "tw_corporate_action_reference.summary.json"
    previous_summary: dict[str, Any] = {}
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            previous_summary = payload if isinstance(payload, dict) else {}
        except (OSError, ValueError):
            previous_summary = {}
    if args.mode == "daily":
        if not output_path.exists() or not bool(previous_summary.get("baseline_established")):
            raise RuntimeError(
                "daily corporate-action update requires a complete baseline; run --mode repair first"
            )
        request_start_year = max(
            int(args.start_year),
            end_date.year - int(args.daily_overlap_years) + 1,
        )
    else:
        request_start_year = int(args.start_year)
    tasks: list[tuple[str, int]] = []
    for market, first_year in (("twse", 2003), ("tpex", 2000)):
        tasks.extend(
            (market, year)
            for year in range(max(request_start_year, first_year), end_date.year + 1)
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results: list[FetchResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(_fetch_year, market, year, end_date, args): (market, year)
            for market, year in tasks
        }
        for future in as_completed(futures):
            results.append(future.result())
    failures = [result for result in results if result.error]
    if failures:
        report = [asdict(result) for result in failures]
        raise RuntimeError(f"corporate-action reference requests failed: {report[:10]}")
    rows = [row for result in results for row in result.rows]
    monthly_results: list[MonthlyDocumentResult] = []
    monthly_stats: dict[str, Any] = {
        "candidate_event_keys": 0,
        "annual_archive_keys": 0,
        "previous_daily_next_reference_keys": 0,
        "monthly_report_keys": 0,
        "duplicate_consistent_monthly_keys": 0,
        "unrecoverable_daily_name_exception_keys": 0,
        "normalized_name_alias_keys": 0,
        "unresolved_keys": 0,
        "annual_archive_examples": [],
        "previous_daily_next_reference_examples": [],
        "monthly_report_examples": [],
        "duplicate_consistent_monthly_examples": [],
        "unrecoverable_daily_name_exception_examples": [],
        "normalized_name_alias_examples": [],
        "unresolved_examples": [],
    }
    tpex_daily_receipt: dict[str, Any] | None = None
    requested_start_date = date(request_start_year, 1, 1)
    monthly_start = max(requested_start_date, TPEX_HISTORICAL_GAP_START)
    monthly_end = min(end_date, TPEX_HISTORICAL_GAP_END)
    if monthly_start <= monthly_end:
        monthly_tasks = [
            (year, month, report_type)
            for year, month in _iter_months(monthly_start, monthly_end)
            for report_type in sorted(TPEX_MONTHLY_REPORT_TYPES)
        ]
        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            futures = {
                executor.submit(
                    _fetch_tpex_monthly_document,
                    year,
                    month,
                    report_type,
                    args,
                ): (year, month, report_type)
                for year, month, report_type in monthly_tasks
            }
            for future in as_completed(futures):
                monthly_results.append(future.result())
        monthly_results.sort(key=lambda result: (result.year, result.month, result.report_type))
        monthly_failures = [result for result in monthly_results if result.status == "failed"]
        if monthly_failures:
            report = [
                {
                    "year": result.year,
                    "month": result.month,
                    "report_type": result.report_type,
                    "query_url": result.query_url,
                    "document_url": result.document_url,
                    "error": result.error,
                }
                for result in monthly_failures
            ]
            raise RuntimeError(f"TPEx monthly corporate-action requests failed: {report[:10]}")
        tpex_daily_path = args.output_dir / "tpex_daily_ohlcv.parquet"
        if not tpex_daily_path.is_file():
            raise FileNotFoundError(
                "TPEx monthly corporate-action fallback requires "
                f"the official daily parquet: {tpex_daily_path}"
            )
        tpex_daily_receipt = _file_receipt(tpex_daily_path)
        candidates = _tpex_event_candidates(
            pl.read_parquet(tpex_daily_path),
            start=monthly_start,
            end=monthly_end,
        )
        monthly_rows = [row for result in monthly_results for row in result.rows]
        fallback_rows, monthly_stats = _resolve_tpex_monthly_rows(
            candidates,
            monthly_rows,
            rows,
        )
        if int(monthly_stats["unresolved_keys"]) != 0:
            raise RuntimeError(
                "TPEx official monthly corporate-action fallback left unresolved event keys: "
                f"{monthly_stats['unresolved_examples']}"
            )
        rows.extend(fallback_rows)
    frame = pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "market": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
            "previous_close": pl.Float64,
            "event_type": pl.String,
            "source_url": pl.String,
        }
    )
    if not frame.is_empty():
        conflicts = (
            frame.group_by(["date", "symbol"])
            .agg(pl.col("reference_price").drop_nulls().n_unique().alias("references"))
            .filter(pl.col("references") > 1)
        )
        if conflicts.height:
            raise RuntimeError(f"conflicting official corporate-action references: {conflicts.head(20)}")
        frame = (
            frame.with_columns(pl.col("reference_price").is_not_null().alias("_has_reference"))
            .sort(["date", "symbol", "_has_reference", "market"], descending=[False, False, True, False])
            .unique(["date", "symbol"], keep="first", maintain_order=True)
            .drop("_has_reference")
        )
    if args.mode != "rebuild" and output_path.exists():
        existing = pl.read_parquet(output_path)
        if not existing.is_empty():
            requested_start = date(request_start_year, 1, 1)
            existing_date = pl.col("date").cast(pl.Date, strict=False)
            existing = existing.filter(
                (existing_date < requested_start) | (existing_date > end_date)
            )
            frame = (
                existing
                if frame.is_empty()
                else pl.concat([existing, frame], how="diagonal_relaxed")
            )
            if not frame.is_empty():
                frame = frame.sort(["date", "symbol"]).unique(
                    ["date", "symbol"], keep="last", maintain_order=True
                )
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent, delete=False
    )
    temporary_path = Path(handle.name)
    handle.close()
    try:
        frame.write_parquet(temporary_path, compression="zstd")
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    previous_baseline = bool(previous_summary.get("baseline_established"))
    baseline_established = previous_baseline or (
        args.mode in {"rebuild", "repair"} and request_start_year <= 2000
    )
    monthly_documents = [
        {
            "year": result.year,
            "month": result.month,
            "report_type": result.report_type,
            "report_kind": TPEX_MONTHLY_REPORT_TYPES[result.report_type],
            "query_url": result.query_url,
            "document_url": result.document_url,
            "document_id": result.document_id,
            "status": result.status,
            "parsed_rows": len(result.rows),
            "error": result.error,
            "raw_receipts": result.raw_receipts,
        }
        for result in monthly_results
    ]
    source_receipts = [
        result.raw_receipt for result in results if result.raw_receipt is not None
    ] + [
        receipt
        for result in monthly_results
        for receipt in result.raw_receipts
    ]
    summary = {
        "schema_version": 3,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "mode": args.mode,
        "requested_start_year": request_start_year,
        "coverage_start_year": (
            min(int(previous_summary.get("coverage_start_year", request_start_year)), request_start_year)
            if args.mode != "rebuild" and previous_summary
            else request_start_year
        ),
        "baseline_established": baseline_established,
        "coverage_complete": baseline_established
        and int(monthly_stats["unresolved_keys"]) == 0,
        "end_date": str(end_date),
        "request_count": len(results),
        "total_http_request_count": len(results) + 2 * len(monthly_results),
        "failure_count": 0,
        "rows": int(frame.height),
        "markets": frame.group_by("market").len().sort("market").to_dicts() if frame.height else [],
        "raw_files": [result.raw_path for result in results if result.raw_path],
        "source_receipts": source_receipts,
        "source_lineage": {
            "annual_reference_endpoints": {
                "twse": TWSE_URL,
                "tpex_historical": TPEX_HISTORICAL_URL,
                "tpex_current": TPEX_CURRENT_URL,
            },
            "tpex_monthly_fallback": {
                "query_endpoint": TPEX_MONTHLY_REPORT_URL,
                "download_endpoint": TPEX_MONTHLY_DOWNLOAD_URL,
                "verified_archive_gap_start": str(TPEX_HISTORICAL_GAP_START),
                "verified_archive_gap_end": str(TPEX_HISTORICAL_GAP_END),
                "matching_contract": (
                    "event_date + prior official TPEx close + verified official name; "
                    "only official_receipt_name_bytes_unrecoverable may use a unique "
                    "date/price/receipt match; type=6/7 duplicates must agree"
                ),
                "requested_start": str(monthly_start) if monthly_start <= monthly_end else None,
                "requested_end": str(monthly_end) if monthly_start <= monthly_end else None,
                "documents": monthly_documents,
                "parsed_document_count": sum(
                    result.status == "parsed" for result in monthly_results
                ),
                "unusable_document_count": sum(
                    result.status == "unusable_document" for result in monthly_results
                ),
                "failed_document_count": 0,
                "tpex_daily_input_receipt": tpex_daily_receipt,
                **monthly_stats,
            },
        },
        "output_receipt": _file_receipt(output_path),
    }
    _write_json_atomic(summary_path, summary)
    print(
        f"[tw-corporate-actions] requests={len(results)} rows={frame.height} "
        f"monthly_documents={len(monthly_results)} "
        f"monthly_keys={monthly_stats['monthly_report_keys']} "
        f"daily_next_reference_keys={monthly_stats['previous_daily_next_reference_keys']} "
        f"output={output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
