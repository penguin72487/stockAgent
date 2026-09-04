"""Build a receipt-backed, exact Taiwan corporate-action entitlement ledger.

The exchange ex-date archive proves that a price-basis transition happened.
For listed/OTC companies, MOPS is the issuer-owned source for the actual cash,
stock, subscription, record, and cash-payment terms.  This downloader joins
those two independent official sources and refuses to call a missing issuer
announcement a zero distribution.

The raw cache is deliberately request-shaped and immutable.  A repair run can
therefore resume tens of thousands of historical issuer queries without
silently accepting a partial archive.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from typing import Any, Iterable

from bs4 import BeautifulSoup
import polars as pl
import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import describe_rate_limit
from downloader.artifact_io import atomic_write_parquet
from downloader.download_tw_public_data import (
    _configure_tw_public_rate_limiter,
    _global_tw_public_rate_limiter,
)


MOPS_BASE = "https://mopsov.twse.com.tw/mops/web"
MOPS_LIST_URL = f"{MOPS_BASE}/ajax_t108sb19"
MOPS_DETAIL_URL = f"{MOPS_BASE}/ajax_t108sb22"
MOPS_BULK_DIVIDEND_URL = f"{MOPS_BASE}/ajax_t108sb27"
SCHEMA_VERSION = 3
PARSER_CONTRACT_VERSION = 3
SYMBOL_PATTERN = re.compile(r"[0-9A-Z]{4,6}")
DETAIL_BUTTON_PATTERN = re.compile(
    r'DATE1\.value="(?P<date>[0-9]{8})";'
    r'document\.t108sb22_fm1\.SEQ_NO\.value="(?P<seq>[0-9]+)";'
    r'document\.t108sb22_fm1\.COMP\.value="(?P<symbol>[0-9A-Z]+)"'
)


_RAW_RECEIPT_REQUESTS: dict[Path, dict[str, Any]] = {}
_RAW_RECEIPT_REQUESTS_LOCK = threading.Lock()


def _reset_raw_receipt_requests() -> None:
    with _RAW_RECEIPT_REQUESTS_LOCK:
        _RAW_RECEIPT_REQUESTS.clear()


@dataclass(frozen=True, slots=True)
class ListingKey:
    market: str
    symbol: str
    roc_year: int


@dataclass(frozen=True, slots=True)
class DetailKey:
    market: str
    symbol: str
    announcement_date: date
    sequence: int


@dataclass(frozen=True, slots=True)
class BulkDividendKey:
    market: str
    roc_year: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download issuer-announced Taiwan corporate-action terms and "
            "cash-payment dates from MOPS."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--reference",
        type=Path,
        default=None,
        help="Defaults to OUTPUT_DIR/tw_corporate_action_reference.parquet.",
    )
    parser.add_argument(
        "--universe-report",
        type=Path,
        default=None,
        help="Defaults to OUTPUT_DIR/stocks/official_symbol_build_report.csv.",
    )
    parser.add_argument("--start-date", default="2005-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument(
        "--mode",
        choices=("rebuild", "repair", "daily"),
        default="repair",
        help=(
            "Controls refresh receipts for mutable MOPS announcements. Past "
            "years use stable immutable receipts; daily refreshes the current "
            "and prior disclosure years at the requested end date."
        ),
    )
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent MOPS requests; a shared limiter still enforces request-interval.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help="Provider-global request interval; default follows project 10 req/s policy.",
    )
    parser.add_argument(
        "--max-list-requests",
        type=int,
        default=0,
        help="Positive values are an explicit incomplete smoke run.",
    )
    parser.add_argument(
        "--max-detail-requests",
        type=int,
        default=0,
        help="Positive values are an explicit incomplete smoke run.",
    )
    return parser.parse_args()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_bytes_atomic(
        path,
        (
            json.dumps(
                payload,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n"
        ).encode("utf-8"),
    )


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    """Replace a canonical parquet only after a complete file is durable."""

    atomic_write_parquet(
        path,
        frame,
        compression="zstd",
        write_statistics=True,
        durable=True,
    )


def _record_raw_receipt_request(
    path: Path,
    *,
    url: str,
    data: dict[str, str],
    content: bytes,
) -> None:
    request = {
        "url": str(url),
        "data": {str(key): str(value) for key, value in sorted(data.items())},
    }
    observed = {
        "request": request,
        "response_size": len(content),
        "response_sha256": _sha256_bytes(content),
    }
    with _RAW_RECEIPT_REQUESTS_LOCK:
        existing = _RAW_RECEIPT_REQUESTS.get(path)
        if existing is not None and existing != observed:
            raise RuntimeError(
                f"raw MOPS receipt request or response changed during build: {path}"
            )
        _RAW_RECEIPT_REQUESTS[path] = observed


def _raw_receipt_manifest_bytes(*, output_dir: Path) -> tuple[bytes, int]:
    """Bind every used POST request to the exact response bytes."""

    with _RAW_RECEIPT_REQUESTS_LOCK:
        requests_by_path = dict(_RAW_RECEIPT_REQUESTS)
    lines: list[bytes] = []
    for path in sorted(requests_by_path, key=lambda value: str(value)):
        try:
            relative_path = path.relative_to(output_dir)
        except ValueError as exc:
            raise ValueError(f"raw MOPS receipt is outside output_dir: {path}") from exc
        receipt = _file_receipt(path)
        observed = requests_by_path[path]
        if (
            receipt["size"] != observed["response_size"]
            or receipt["sha256"] != observed["response_sha256"]
        ):
            raise RuntimeError(
                "raw MOPS response changed after it was parsed: "
                f"{relative_path.as_posix()}"
            )
        request = observed["request"]
        canonical_request = json.dumps(
            request,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        row = {
            "path": relative_path.as_posix(),
            "request": request,
            "request_sha256": _sha256_bytes(canonical_request),
            "response_size": observed["response_size"],
            "response_sha256": observed["response_sha256"],
        }
        lines.append(
            json.dumps(
                row,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
    return b"".join(lines), len(lines)


def _write_content_addressed_receipt_manifest(
    *,
    output_dir: Path,
    raw_root: Path,
) -> dict[str, Any]:
    content, entries = _raw_receipt_manifest_bytes(output_dir=output_dir)
    digest = _sha256_bytes(content)
    path = raw_root / "manifests" / f"{digest}.jsonl"
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"content-addressed manifest collision: {path}")
    else:
        _write_bytes_atomic(path, content)
    receipt = _file_receipt(path)
    receipt.update(
        {
            "relative_path": path.relative_to(output_dir).as_posix(),
            "entries": entries,
        }
    )
    return receipt


def _raw_path(raw_root: Path, kind: str, identity: str) -> Path:
    return raw_root / kind / f"{identity}.html"


def _mops_throttle_response(content: bytes) -> bool:
    """Recognize MOPS's HTTP-200 rate-limit body before it poisons receipts."""

    lowered = content.lower()
    return any(
        marker in lowered
        for marker in (
            b"overrun -",
            b"too many query requests",
            "查詢過於頻繁".encode("utf-8"),
        )
    )


def _cached_or_post(
    path: Path,
    *,
    url: str,
    data: dict[str, str],
    timeout: int,
    retries: int,
) -> bytes:
    if path.exists():
        content = path.read_bytes()
        if not content.strip():
            raise ValueError(f"empty cached MOPS receipt: {path}")
        if _mops_throttle_response(content):
            # MOPS reports throttling as HTTP 200.  Such a body is not an
            # issuer receipt and must never enter the content-addressed
            # manifest.  Atomic response writes make unlinking it safe even
            # when another worker is reading a different receipt.
            path.unlink()
        else:
            _record_raw_receipt_request(path, url=url, data=data, content=content)
            return content
    limiter = _global_tw_public_rate_limiter()
    headers = {
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": f"{MOPS_BASE}/t108sb19_q1",
        "User-Agent": "stockAgent-official-data/1.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    response: requests.Response | None = None
    content: bytes | None = None
    last_error: Exception | None = None
    for attempt in range(max(0, int(retries)) + 1):
        try:
            limiter.wait()
            response = requests.post(
                url,
                data=data,
                headers=headers,
                timeout=(int(timeout), int(timeout)),
            )
            if response.status_code in {403, 408, 429, 500, 502, 503, 504}:
                retry_after = response.headers.get("Retry-After", "").strip()
                delay = (
                    float(retry_after)
                    if retry_after.isdigit()
                    else min(60.0, 2.0**attempt)
                )
                limiter.defer(delay)
                response.close()
                if attempt < int(retries):
                    continue
            response.raise_for_status()
            candidate = response.content
            response.close()
            response = None
            if _mops_throttle_response(candidate):
                last_error = RuntimeError(
                    "MOPS returned an HTTP-200 query-frequency throttle page"
                )
                delay = min(60.0, max(5.0, 2.0**attempt))
                limiter.defer(delay)
                if attempt < int(retries):
                    time.sleep(delay)
                    continue
                raise last_error
            content = candidate
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= int(retries):
                raise
            delay = min(60.0, 2.0**attempt)
            limiter.defer(delay)
            time.sleep(delay)
    if content is None:
        raise RuntimeError(f"MOPS POST failed: {last_error}")
    if not content.strip():
        raise ValueError(f"MOPS returned an empty body for {data}")
    _write_bytes_atomic(path, content)
    _record_raw_receipt_request(path, url=url, data=data, content=content)
    return content


def _roc_date(value: str) -> date | None:
    text = re.sub(r"\s+", "", str(value or ""))
    match = re.search(r"([0-9]{2,3})年([0-9]{1,2})月([0-9]{1,2})日", text)
    if match is None:
        match = re.search(r"([0-9]{2,3})/([0-9]{1,2})/([0-9]{1,2})", text)
    if match is None:
        return None
    try:
        return date(
            int(match.group(1)) + 1911, int(match.group(2)), int(match.group(3))
        )
    except ValueError:
        return None


def _number(value: str) -> float | None:
    text = re.sub(r"[^0-9.+-]", "", str(value or ""))
    if text in {"", "+", "-", "."}:
        return None
    try:
        result = float(text)
    except ValueError:
        return None
    return result if result == result and abs(result) != float("inf") else None


def _market_typek(market: str) -> str:
    if market == "twse":
        return "sii"
    if market == "tpex":
        return "otc"
    raise ValueError(f"unsupported company market: {market!r}")


def parse_mops_listing(content: bytes, *, key: ListingKey) -> list[DetailKey]:
    text = content.decode("utf-8", errors="strict")
    if "公司代號輸入錯誤" in text or "公司代號不可空白" in text:
        raise ValueError(f"MOPS rejected listing key {key}")
    output: list[DetailKey] = []
    seen: set[tuple[str, date, int]] = set()
    for match in DETAIL_BUTTON_PATTERN.finditer(text):
        symbol = match.group("symbol").strip().upper()
        announcement_date = date.fromisoformat(
            f"{match.group('date')[:4]}-{match.group('date')[4:6]}-{match.group('date')[6:]}"
        )
        sequence = int(match.group("seq"))
        identity = (symbol, announcement_date, sequence)
        if identity in seen:
            continue
        seen.add(identity)
        output.append(
            DetailKey(
                market=key.market,
                symbol=symbol,
                announcement_date=announcement_date,
                sequence=sequence,
            )
        )
    if output and any(item.symbol != key.symbol for item in output):
        raise ValueError(f"MOPS listing returned another symbol for {key}")
    return output


def _row_value(rows: Iterable[str], label: str) -> str | None:
    for row in rows:
        compact = re.sub(r"\s+", "", row)
        index = compact.find(label)
        if index >= 0:
            return compact[index + len(label) :]
    return None


def parse_mops_detail(content: bytes, *, key: DetailKey) -> dict[str, Any]:
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text(" ", strip=True)
    if "查無所需資料" in text:
        raise ValueError(f"MOPS detail has no data for {key}")
    rows = [
        " ".join(row.stripped_strings)
        for row in soup.select("tr")
        if row.find("tr") is None
    ]
    if key.symbol not in text[:1000]:
        raise ValueError(f"MOPS detail symbol does not match {key}")

    stop_transfer = _row_value(rows, "四、股票停止過戶起訖日期：") or ""
    stop_dates = re.findall(r"[0-9]{2,3}年[0-9]{1,2}月[0-9]{1,2}日", stop_transfer)
    record_text = _row_value(rows, "（八）權利分派基準日：")
    ex_text = _row_value(rows, "除權/除息交易日：")
    cash_payment_text = _row_value(rows, "＊現金股利發放日：")
    cash_payment_start_text = _row_value(rows, "現金增資繳款開始日：") or ""
    cash_payment_dates = re.findall(
        r"[0-9]{2,3}年[0-9]{1,2}月[0-9]{1,2}日", cash_payment_start_text
    )

    cash_row = next(
        (row for row in rows if "※除息--普通股：每壹股配發現金(股利)" in row), ""
    )
    stock_row = next(
        (row for row in rows if "※除權--普通股：每壹股配發股票(股利)" in row), ""
    )
    cash_match = re.search(r"每壹股配發現金\(股利\)\s*([0-9,.]+)\s*元", cash_row)
    stock_match = re.search(r"每壹股配發股票\(股利\)\s*([0-9,.]+)\s*元", stock_row)
    free_shares = re.findall(r"每壹仟股無償配發[^0-9]*([0-9,.]+)\s*股", stock_row)
    subscription_price = re.search(r"每股新台幣\s*([0-9,.]+)\s*元認購", stock_row)
    subscription_shares = re.search(r"元認購[^0-9]*([0-9,.]+)\s*股", stock_row)

    cash_dividend = _number(cash_match.group(1)) if cash_match else 0.0
    stock_dividend_per_share = _number(stock_match.group(1)) if stock_match else 0.0
    stock_ratio = sum(_number(value) or 0.0 for value in free_shares) / 1000.0
    # Issuers sometimes render the per-share NT$ stock dividend but leave the
    # two thousand-share components blank.  Par value is ordinarily NT$10;
    # do not infer that conversion here because exact mode must prove it.
    stock_terms_complete = not (
        (stock_dividend_per_share or 0.0) > 0.0 and not free_shares
    )
    subscription_ratio = (
        (_number(subscription_shares.group(1)) or 0.0) / 1000.0
        if subscription_shares
        else 0.0
    )
    subscription_price_value = (
        _number(subscription_price.group(1)) if subscription_price else 0.0
    )

    ex_date = _roc_date(ex_text or "")
    if ex_date is None:
        raise ValueError(f"MOPS detail lacks a valid ex-date for {key}")
    cash_payment_date = _roc_date(cash_payment_text or "")

    return {
        "date": ex_date,
        "symbol": key.symbol,
        "market": key.market,
        "announcement_date": key.announcement_date,
        "announcement_sequence": key.sequence,
        "record_date": _roc_date(record_text or ""),
        "stop_transfer_start": _roc_date(stop_dates[0]) if stop_dates else None,
        "stop_transfer_end": _roc_date(stop_dates[1]) if len(stop_dates) > 1 else None,
        "cash_dividend_per_share": float(cash_dividend),
        "cash_payment_date": cash_payment_date,
        "stock_dividend_ratio": float(stock_ratio),
        "stock_terms_complete": bool(stock_terms_complete),
        "subscription_ratio": float(subscription_ratio),
        "subscription_price": float(subscription_price_value or 0.0),
        "subscription_payment_start": (
            _roc_date(cash_payment_dates[0]) if cash_payment_dates else None
        ),
        "subscription_payment_end": (
            _roc_date(cash_payment_dates[1]) if len(cash_payment_dates) > 1 else None
        ),
        "source_url": MOPS_DETAIL_URL,
    }


def _strict_bulk_number(value: str, *, field: str, key: BulkDividendKey) -> float:
    text = re.sub(r"\s+", "", str(value or ""))
    if text in {"", "-", "--", "不適用"}:
        return 0.0
    result = _number(text)
    if result is None:
        raise ValueError(
            f"MOPS bulk dividend has a non-numeric {field} for {key}: {value!r}"
        )
    return float(result)


def _bulk_dividend_layout(cells: list[str], *, key: BulkDividendKey) -> dict[str, int]:
    """Return the evidence-derived column map for each historical MOPS layout."""

    # MOPS changed this report twice.  The old table included employee-bonus
    # columns, the middle table removed those columns, and the current table
    # added participating shares.  Indexing by row width is deterministic
    # because each layout has a distinct width and all three retain the same
    # identifying header/report endpoint.
    layouts = {
        23: {
            "stock_earnings": 4,
            "stock_reserve": 5,
            "ex_rights_date": 6,
            "cash_earnings": 11,
            "cash_reserve": 12,
            "ex_dividend_date": 13,
            "payment_date": 14,
            "subscription_shares": 16,
            "subscription_percent": 17,
            "subscription_price": 18,
            "announcement_date": 20,
            "announcement_time": 21,
        },
        18: {
            "stock_earnings": 4,
            "stock_reserve": 5,
            "ex_rights_date": 6,
            "cash_earnings": 7,
            "cash_reserve": 8,
            "ex_dividend_date": 10,
            "payment_date": 11,
            "subscription_shares": 12,
            "subscription_percent": 13,
            "subscription_price": 14,
            "announcement_date": 15,
            "announcement_time": 16,
        },
        19: {
            "stock_earnings": 4,
            "stock_reserve": 5,
            "ex_rights_date": 6,
            "cash_earnings": 7,
            "cash_reserve": 8,
            "ex_dividend_date": 10,
            "payment_date": 11,
            "subscription_shares": 12,
            "subscription_percent": 13,
            "subscription_price": 14,
            "announcement_date": 16,
            "announcement_time": 17,
        },
    }
    try:
        return layouts[len(cells)]
    except KeyError as exc:
        raise ValueError(
            f"MOPS bulk dividend row width changed for {key}: {len(cells)}"
        ) from exc


def _bulk_announcement_time(
    value: str, *, key: BulkDividendKey
) -> tuple[int, int, int]:
    text = re.sub(r"\s+", "", str(value or ""))
    if not text:
        return (0, 0, 0)
    match = re.fullmatch(r"([0-9]{1,2}):([0-9]{2}):([0-9]{2})", text)
    if match is None:
        raise ValueError(
            f"MOPS bulk dividend has an invalid announcement time for {key}: {value!r}"
        )
    result = tuple(int(match.group(index)) for index in range(1, 4))
    if result[0] > 23 or result[1] > 59 or result[2] > 59:
        raise ValueError(
            f"MOPS bulk dividend has an invalid announcement time for {key}: {value!r}"
        )
    return result


def parse_mops_bulk_dividends(
    content: bytes, *, key: BulkDividendKey
) -> list[dict[str, Any]]:
    """Parse one official market-year dividend allocation table.

    ``ajax_t108sb27`` is already the normalized MOPS report: one request covers
    every issuer in a market-year and includes common cash per share, stock
    terms, ex-dates, record date, cash-payment date, and announcement time.
    A source row can generate separate cash and stock events when their ex-dates
    differ; the later reference join retains only exchange-certified keys.
    """

    if _mops_throttle_response(content):
        raise ValueError(f"MOPS bulk dividend response was throttled for {key}")
    soup = BeautifulSoup(content, "html.parser")
    selected_table = None
    for table in soup.select("table"):
        header = " ".join(table.get_text(" ", strip=True).split())
        if "公司代號" in header and "現金股利發放日" in header:
            selected_table = table
            break
    if selected_table is None:
        text = " ".join(soup.get_text(" ", strip=True).split())
        if "查無資料" in text or "無資料" in text:
            return []
        raise ValueError(f"MOPS bulk dividend table is missing for {key}")

    # Corrections appear as multiple rows with the same entitlement identity
    # and progressively later announcement times.  Select the latest official
    # revision before projecting source actions into ex-date events.  A tied
    # timestamp with different terms is ambiguous and therefore fails closed.
    source_rows: dict[
        tuple[str, date | None, date | None, date | None],
        tuple[tuple[date, tuple[int, int, int]], dict[str, Any]],
    ] = {}
    for row in selected_table.select("tr"):
        cells = [
            " ".join(cell.stripped_strings).strip()
            for cell in row.find_all("td", recursive=False)
        ]
        if not cells:
            continue
        layout = _bulk_dividend_layout(cells, key=key)
        raw_symbol = cells[0].strip().upper()
        security_name = cells[1].strip()
        # The 2005-era report emitted a second pseudo-symbol row such as
        # ``8084* / 特別股*`` beside the common-share issuer row.  It is not the
        # broker-tradable common symbol represented by the exchange reference.
        if "特別股" in security_name or raw_symbol.endswith("*"):
            continue
        symbol = raw_symbol
        if SYMBOL_PATTERN.fullmatch(symbol) is None:
            raise ValueError(
                f"MOPS bulk dividend has an invalid symbol for {key}: {symbol!r}"
            )
        stock_earnings = _strict_bulk_number(
            cells[layout["stock_earnings"]],
            field="earnings stock dividend",
            key=key,
        )
        stock_reserve = _strict_bulk_number(
            cells[layout["stock_reserve"]],
            field="reserve stock dividend",
            key=key,
        )
        cash_earnings = _strict_bulk_number(
            cells[layout["cash_earnings"]],
            field="earnings cash dividend",
            key=key,
        )
        cash_reserve = _strict_bulk_number(
            cells[layout["cash_reserve"]],
            field="reserve cash dividend",
            key=key,
        )
        subscription_percent = _strict_bulk_number(
            cells[layout["subscription_percent"]],
            field="subscription percentage",
            key=key,
        )
        subscription_price = _strict_bulk_number(
            cells[layout["subscription_price"]],
            field="subscription price",
            key=key,
        )
        record_date = _roc_date(cells[3])
        ex_rights_date = _roc_date(cells[layout["ex_rights_date"]])
        ex_dividend_date = _roc_date(cells[layout["ex_dividend_date"]])
        payment_date = _roc_date(cells[layout["payment_date"]])
        announcement_date = _roc_date(cells[layout["announcement_date"]])
        announcement_time = _bulk_announcement_time(
            cells[layout["announcement_time"]], key=key
        )
        source_identity = (
            symbol,
            record_date,
            ex_rights_date,
            ex_dividend_date,
        )
        source_payload = {
            "symbol": symbol,
            "record_date": record_date,
            "ex_rights_date": ex_rights_date,
            "ex_dividend_date": ex_dividend_date,
            "payment_date": payment_date,
            "announcement_date": announcement_date,
            "stock_value_per_share": stock_earnings + stock_reserve,
            "cash_per_share": cash_earnings + cash_reserve,
            "subscription_percent": subscription_percent,
            "subscription_price": subscription_price,
        }
        revision = (announcement_date or date.min, announcement_time)
        existing = source_rows.get(source_identity)
        if existing is not None:
            existing_revision, existing_payload = existing
            if revision < existing_revision:
                continue
            if revision == existing_revision:
                if source_payload != existing_payload:
                    raise ValueError(
                        "MOPS bulk dividend has conflicting rows at the same "
                        f"revision for {key}: {source_identity}"
                    )
                continue
        source_rows[source_identity] = (revision, source_payload)

    output: list[dict[str, Any]] = []
    for _, source_payload in source_rows.values():
        symbol = source_payload["symbol"]
        record_date = source_payload["record_date"]
        ex_rights_date = source_payload["ex_rights_date"]
        ex_dividend_date = source_payload["ex_dividend_date"]
        payment_date = source_payload["payment_date"]
        announcement_date = source_payload["announcement_date"]
        stock_value_per_share = source_payload["stock_value_per_share"]
        cash_per_share = source_payload["cash_per_share"]
        subscription_percent = source_payload["subscription_percent"]
        subscription_price = source_payload["subscription_price"]
        # MOPS expresses stock dividends as NTD per share.  Par is ordinarily
        # NTD 10; only zero/nonzero is required to keep mixed actions off the
        # exact-cash path, so no par-value inference enters a cash amount.
        stock_ratio_marker = stock_value_per_share
        subscription_ratio = subscription_percent / 100.0
        event_dates = {
            event_date
            for event_date in (ex_rights_date, ex_dividend_date)
            if event_date is not None
        }
        for event_date in sorted(event_dates):
            is_cash_date = event_date == ex_dividend_date
            is_stock_date = event_date == ex_rights_date
            output.append(
                {
                    "date": event_date,
                    "symbol": symbol,
                    "market": key.market,
                    "announcement_date": announcement_date,
                    "announcement_sequence": 0,
                    "record_date": record_date,
                    "stop_transfer_start": None,
                    "stop_transfer_end": None,
                    "cash_dividend_per_share": (
                        float(cash_per_share) if is_cash_date else 0.0
                    ),
                    "cash_payment_date": payment_date if is_cash_date else None,
                    "stock_dividend_ratio": (
                        float(stock_ratio_marker) if is_stock_date else 0.0
                    ),
                    "stock_terms_complete": True,
                    "subscription_ratio": (
                        float(subscription_ratio) if is_stock_date else 0.0
                    ),
                    "subscription_price": (
                        float(subscription_price) if is_stock_date else 0.0
                    ),
                    "subscription_payment_start": None,
                    "subscription_payment_end": None,
                    "source_url": MOPS_BULK_DIVIDEND_URL,
                }
            )
    return output


def _collapse_bulk_event_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse cross-year revisions without hiding multiple action kinds."""

    grouped: dict[tuple[date, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["date"], row["symbol"]), []).append(row)

    output: list[dict[str, Any]] = []
    for event_key in sorted(grouped):
        revisions: dict[tuple[date | None, bool, bool, bool], dict[str, Any]] = {}
        for row in grouped[event_key]:
            action_identity = (
                row["record_date"],
                float(row["cash_dividend_per_share"]) > 0.0,
                float(row["stock_dividend_ratio"]) > 0.0,
                float(row["subscription_ratio"]) > 0.0,
            )
            existing = revisions.get(action_identity)
            if existing is None:
                revisions[action_identity] = row
                continue
            existing_date = existing["announcement_date"] or date.min
            candidate_date = row["announcement_date"] or date.min
            if candidate_date > existing_date:
                revisions[action_identity] = row
            elif candidate_date == existing_date and row != existing:
                raise ValueError(
                    "MOPS bulk reports conflict at the same event revision: "
                    f"{event_key}"
                )

        current = list(revisions.values())
        if len(current) == 1:
            output.append(current[0])
            continue

        # Separate issuer actions can share an exchange ex-date.  The current
        # tensor schema has one entitlement/payment slot per date and symbol,
        # so retain their combined risk markers but deliberately make the row
        # ineligible for exact-cash treatment instead of silently choosing one.
        latest = max(
            current,
            key=lambda value: value["announcement_date"] or date.min,
        )
        merged = dict(latest)
        merged["announcement_date"] = max(
            (value["announcement_date"] for value in current),
            key=lambda value: value or date.min,
        )
        record_dates = {value["record_date"] for value in current}
        merged["record_date"] = (
            next(iter(record_dates)) if len(record_dates) == 1 else None
        )
        merged["cash_dividend_per_share"] = sum(
            float(value["cash_dividend_per_share"]) for value in current
        )
        payment_dates = {
            value["cash_payment_date"]
            for value in current
            if float(value["cash_dividend_per_share"]) > 0.0
        }
        merged["cash_payment_date"] = (
            next(iter(payment_dates)) if len(payment_dates) == 1 else None
        )
        merged["stock_dividend_ratio"] = sum(
            float(value["stock_dividend_ratio"]) for value in current
        )
        merged["subscription_ratio"] = sum(
            float(value["subscription_ratio"]) for value in current
        )
        merged["stock_terms_complete"] = False
        output.append(merged)
    return output


def _verified_reference_receipt(reference_path: Path) -> dict[str, Any]:
    summary_path = reference_path.with_suffix(".summary.json")
    if not summary_path.exists():
        raise FileNotFoundError(summary_path)
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"invalid corporate-action reference receipt: {summary_path}"
        ) from exc
    if not isinstance(summary, dict):
        raise ValueError("corporate-action reference receipt must be a JSON object")
    if not bool(summary.get("baseline_established")) or not bool(
        summary.get("coverage_complete")
    ):
        raise ValueError("corporate-action reference is not a complete baseline")
    if int(summary.get("failure_count", -1)) != 0:
        raise ValueError("corporate-action reference receipt contains failures")
    if int(summary.get("schema_version", -1)) < 3:
        raise ValueError("corporate-action reference schema_version must be >= 3")
    expected = summary.get("output_receipt")
    if not isinstance(expected, dict):
        raise ValueError("corporate-action reference output_receipt is missing")
    actual = _file_receipt(reference_path)
    if int(expected.get("size", -1)) != actual["size"]:
        raise ValueError("corporate-action reference size receipt mismatch")
    if str(expected.get("sha256", "")).strip().lower() != actual["sha256"]:
        raise ValueError("corporate-action reference SHA-256 receipt mismatch")
    rows = int(pl.scan_parquet(reference_path).select(pl.len()).collect().item())
    if int(summary.get("rows", -1)) != rows:
        raise ValueError("corporate-action reference row receipt mismatch")
    return {
        **actual,
        "summary_path": str(summary_path),
        "summary_sha256": _file_receipt(summary_path)["sha256"],
        "rows": rows,
    }


def _load_reference(
    args: argparse.Namespace,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any], dict[str, Any]]:
    reference_path = args.reference or (
        args.output_dir / "tw_corporate_action_reference.parquet"
    )
    universe_path = args.universe_report or (
        args.output_dir / "stocks" / "official_symbol_build_report.csv"
    )
    if not reference_path.exists():
        raise FileNotFoundError(reference_path)
    if not universe_path.exists():
        raise FileNotFoundError(universe_path)
    reference_receipt = _verified_reference_receipt(reference_path)
    universe_receipt = _file_receipt(universe_path)
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end < start:
        raise ValueError("end-date must not precede start-date")
    reference = (
        pl.read_parquet(reference_path)
        .with_columns(
            pl.col("symbol").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("market").cast(pl.String).str.to_lowercase(),
        )
        .filter(pl.col("date").is_between(start, end))
        .unique(["date", "symbol"], keep="last")
        .sort(["date", "symbol"])
    )
    universe = (
        pl.read_csv(universe_path)
        .select(
            pl.col("symbol").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("security_type").cast(pl.String).str.to_lowercase(),
            pl.col("market")
            .cast(pl.String)
            .str.to_lowercase()
            .alias("universe_market"),
        )
        .unique("symbol", keep="last")
    )
    return reference, universe, reference_receipt, universe_receipt


def _mutable_receipt_suffix(*, disclosure_year: int, args: argparse.Namespace) -> str:
    """Version request-shaped receipts whose upstream result can still change."""

    end = date.fromisoformat(args.end_date)
    if args.mode == "repair" or (
        args.mode == "daily" and disclosure_year >= end.year - 1
    ):
        return f"-asof-{end:%Y%m%d}"
    return ""


def _requested_listing_keys(company_events: pl.DataFrame) -> list[ListingKey]:
    keys: set[ListingKey] = set()
    for row in company_events.select("market", "symbol", "date").iter_rows(named=True):
        event_year = row["date"].year
        # Query the ex-date's disclosure year.  First-quarter actions may have
        # been announced in the preceding calendar year, so they receive one
        # additional evidence query.  A non-matching event remains on the
        # explicit avoidance path; no missing page is promoted to exact.
        disclosure_years = [event_year]
        if int(row["date"].month) <= 3:
            disclosure_years.insert(0, event_year - 1)
        for gregorian_year in disclosure_years:
            keys.add(
                ListingKey(
                    market=str(row["market"]),
                    symbol=str(row["symbol"]),
                    roc_year=gregorian_year - 1911,
                )
            )
    return sorted(keys, key=lambda value: (value.market, value.symbol, value.roc_year))


def _requested_bulk_dividend_keys(*, start: date, end: date) -> list[BulkDividendKey]:
    return [
        BulkDividendKey(market=market, roc_year=year - 1911)
        for year in range(start.year, end.year + 1)
        for market in ("twse", "tpex")
    ]


def _fetch_bulk_dividends(
    raw_root: Path,
    key: BulkDividendKey,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    disclosure_year = key.roc_year + 1911
    identity = (
        f"{key.market}-{key.roc_year:03d}"
        f"{_mutable_receipt_suffix(disclosure_year=disclosure_year, args=args)}"
        f"-v{PARSER_CONTRACT_VERSION}"
    )
    content = _cached_or_post(
        _raw_path(raw_root, "bulk_dividends", identity),
        url=MOPS_BULK_DIVIDEND_URL,
        data={
            "step": "1",
            "firstin": "1",
            "TYPEK": _market_typek(key.market),
            "year": str(key.roc_year),
            "type": "2",
        },
        timeout=args.timeout,
        retries=args.retries,
    )
    return parse_mops_bulk_dividends(content, key=key)


def _fetch_listing(
    raw_root: Path,
    key: ListingKey,
    args: argparse.Namespace,
) -> list[DetailKey]:
    disclosure_year = key.roc_year + 1911
    identity = (
        f"{key.market}-{key.symbol}-{key.roc_year:03d}"
        f"{_mutable_receipt_suffix(disclosure_year=disclosure_year, args=args)}"
        f"-v{PARSER_CONTRACT_VERSION}"
    )
    content = _cached_or_post(
        _raw_path(raw_root, "lists", identity),
        url=MOPS_LIST_URL,
        data={
            "step": "1",
            "TYPEK": _market_typek(key.market),
            "year": str(key.roc_year),
            "co_id": key.symbol,
            "month": "all",
            "b_date": "",
            "e_date": "",
            "isnew": "false",
            "firstin": "true",
        },
        timeout=args.timeout,
        retries=args.retries,
    )
    return parse_mops_listing(content, key=key)


def _fetch_listing_with_market_fallback(
    raw_root: Path,
    key: ListingKey,
    args: argparse.Namespace,
) -> list[DetailKey]:
    """Resolve historical venue changes without guessing an entitlement.

    MOPS validates the issuer code against the requested current disclosure
    market, while the exchange reference records the venue on the ex-date.
    A company that later moved between TPEx and TWSE can therefore reject the
    historically correct venue.  Query the other listed-company venue using a
    separate immutable receipt; rejection by both is a proven no-MOPS result
    and remains an ``avoid`` classification, not a transport failure.
    """

    try:
        return _fetch_listing(raw_root, key, args)
    except ValueError as primary_exc:
        if "MOPS rejected listing key" not in str(primary_exc):
            raise
    alternate = ListingKey(
        market="twse" if key.market == "tpex" else "tpex",
        symbol=key.symbol,
        roc_year=key.roc_year,
    )
    try:
        return _fetch_listing(raw_root, alternate, args)
    except ValueError as alternate_exc:
        if "MOPS rejected listing key" not in str(alternate_exc):
            raise
        return []


def _fetch_detail(
    raw_root: Path,
    key: DetailKey,
    args: argparse.Namespace,
) -> dict[str, Any]:
    identity = (
        f"{key.market}-{key.symbol}-{key.announcement_date:%Y%m%d}-"
        f"{key.sequence}"
        f"{_mutable_receipt_suffix(disclosure_year=key.announcement_date.year, args=args)}"
        f"-v{PARSER_CONTRACT_VERSION}"
    )
    content = _cached_or_post(
        _raw_path(raw_root, "details", identity),
        url=MOPS_DETAIL_URL,
        data={
            "firstin": "true",
            "TYPEK": _market_typek(key.market),
            "isnew": "false",
            "DATE1": key.announcement_date.strftime("%Y%m%d"),
            "SEQ_NO": str(key.sequence),
            "COMP": key.symbol,
            "kind": "",
            "SKIND": "G",
            "step": "2",
        },
        timeout=args.timeout,
        retries=args.retries,
    )
    return parse_mops_detail(content, key=key)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _reset_raw_receipt_requests()
    request_interval = _configure_tw_public_rate_limiter(args.request_interval)
    print(describe_rate_limit("tw_public", request_interval))

    reference, universe, reference_receipt, universe_receipt = _load_reference(args)
    joined = reference.join(universe, on="symbol", how="left")
    company_events = joined.filter(
        pl.col("security_type").eq("stock") & pl.col("market").is_in(["twse", "tpex"])
    )
    company_cash_events = company_events.filter(
        pl.col("event_type").cast(pl.String).str.strip_chars().is_in(["息", "除息"])
    )
    noncompany_events = joined.filter(~pl.col("security_type").eq("stock"))
    raw_root = args.output_dir / "raw" / "tw_corporate_action_entitlements"

    # The normalized MOPS dividend report is natively market-year bulk data.
    # This is both more complete and orders of magnitude cheaper than issuing
    # one company-year list request followed by one detail request per event.
    # Missing stop-transfer starts remain safe because the panel applies its
    # conservative exchange-ex-date/T+2 Article 76 fallback for margin shorts.
    bulk_keys = _requested_bulk_dividend_keys(
        start=date.fromisoformat(args.start_date),
        end=date.fromisoformat(args.end_date),
    )
    limited = int(args.max_list_requests) > 0
    if limited:
        bulk_keys = bulk_keys[: int(args.max_list_requests)]
    failures: list[dict[str, str]] = []
    parsed_rows: list[dict[str, Any]] = []
    workers = max(1, int(args.workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_keys = {
            executor.submit(_fetch_bulk_dividends, raw_root, key, args): key
            for key in bulk_keys
        }
        for index, future in enumerate(as_completed(future_keys), start=1):
            key = future_keys[future]
            try:
                parsed_rows.extend(future.result())
            except Exception as exc:
                failures.append(
                    {
                        "stage": "bulk_dividend",
                        "key": repr(key),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            if index == 1 or index % 5 == 0 or index == len(bulk_keys):
                print(
                    f"[corporate-action] bulk years {index}/{len(bulk_keys)} "
                    f"events_parsed={len(parsed_rows)} failures={len(failures)}",
                    flush=True,
                )

    detail_limited = int(args.max_detail_requests) > 0
    if detail_limited:
        parsed_rows = parsed_rows[: int(args.max_detail_requests)]

    failures.sort(key=lambda item: (item["stage"], item["key"], item["error"]))

    parsed_rows = _collapse_bulk_event_rows(parsed_rows)
    parsed = (
        pl.from_dicts(parsed_rows, infer_schema_length=None)
        if parsed_rows
        else pl.DataFrame()
    )
    if parsed.height:
        parsed = parsed.sort(
            ["date", "symbol", "announcement_date", "announcement_sequence"]
        )
    reference_keys = company_cash_events.select("date", "symbol").unique()
    matched = (
        reference_keys.join(parsed, on=["date", "symbol"], how="left")
        if parsed.height
        else reference_keys.with_columns(pl.lit(None).alias("market"))
    )
    missing = matched.filter(pl.col("market").is_null())
    output = args.output_dir / "tw_corporate_action_entitlements.parquet"
    summary_path = output.with_suffix(".summary.json")
    attempt_summary_path = output.with_suffix(".attempt.summary.json")
    complete = not limited and not detail_limited and not failures

    exact = (
        parsed.filter(
            (pl.col("cash_dividend_per_share") > 0.0)
            & pl.col("cash_payment_date").is_not_null()
            & pl.col("stock_terms_complete")
            & (pl.col("stock_dividend_ratio") == 0.0)
            & (pl.col("subscription_ratio") == 0.0)
        )
        if parsed.height
        else parsed
    )
    detail_columns = [
        "announcement_date",
        "announcement_sequence",
        "record_date",
        "stop_transfer_start",
        "stop_transfer_end",
        "cash_dividend_per_share",
        "cash_payment_date",
        "stock_dividend_ratio",
        "stock_terms_complete",
        "subscription_ratio",
        "subscription_price",
        "subscription_payment_start",
        "subscription_payment_end",
        "source_url",
    ]
    details_for_join = (
        parsed.select("date", "symbol", *detail_columns)
        .join(
            exact.select("date", "symbol").with_columns(
                pl.lit(True).alias("is_exact_cash")
            ),
            on=["date", "symbol"],
            how="left",
        )
        .rename({"source_url": "mops_source_url"})
        if parsed.height
        else pl.DataFrame(
            schema={
                "date": pl.Date,
                "symbol": pl.String,
                **{
                    name: (
                        pl.Date
                        if name.endswith("_date")
                        or name.endswith("_start")
                        or name.endswith("_end")
                        or name == "announcement_date"
                        else pl.Boolean
                        if name == "stock_terms_complete"
                        else pl.Int64
                        if name == "announcement_sequence"
                        else pl.String
                        if name == "source_url"
                        else pl.Float64
                    )
                    for name in detail_columns
                },
                "is_exact_cash": pl.Boolean,
            }
        ).rename({"source_url": "mops_source_url"})
    )
    ledger = (
        joined.join(details_for_join, on=["date", "symbol"], how="left")
        .with_columns(
            pl.when(pl.col("is_exact_cash").fill_null(False))
            .then(pl.lit("exact_cash"))
            .otherwise(pl.lit("avoid"))
            .alias("handling"),
            pl.when(pl.col("is_exact_cash").fill_null(False))
            .then(pl.lit("mops_exact_cash"))
            .when(
                pl.col("security_type").eq("stock")
                & pl.col("event_type")
                .cast(pl.String)
                .str.strip_chars()
                .is_in(["息", "除息"])
            )
            .then(pl.lit("mops_cash_terms_unavailable_or_complex"))
            .when(pl.col("security_type").eq("stock"))
            .then(pl.lit("stock_or_subscription_action"))
            .otherwise(pl.lit("noncompany_action"))
            .alias("handling_reason"),
        )
        .drop("security_type", "universe_market", "is_exact_cash")
        .sort(["date", "symbol"])
    )
    ledger_exact_cash = ledger.filter(pl.col("handling").eq("exact_cash"))
    ledger_unresolved_company_cash = ledger.filter(
        pl.col("handling_reason").eq("mops_cash_terms_unavailable_or_complex")
    )

    raw_receipt_manifest = _write_content_addressed_receipt_manifest(
        output_dir=args.output_dir,
        raw_root=raw_root,
    )
    # An incomplete smoke/repair never replaces the established production
    # ledger.  Persist its audit summary only.
    if complete:
        _write_parquet_atomic(ledger, output)
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": args.mode,
        "coverage_start": args.start_date,
        "coverage_end": args.end_date,
        "reference_rows": int(reference.height),
        "company_reference_rows": int(company_events.height),
        "company_cash_reference_rows": int(reference_keys.height),
        "noncompany_reference_rows": int(noncompany_events.height),
        "bulk_source": MOPS_BULK_DIVIDEND_URL,
        "requested_bulk_market_years": len(bulk_keys),
        "requested_list_keys": 0,
        "discovered_details": 0,
        "parsed_details": int(parsed.height),
        "parsed_exact_cash_candidates": int(exact.height),
        "exact_cash_events": int(ledger_exact_cash.height),
        "avoided_events": int(ledger.height - ledger_exact_cash.height),
        "cash_events_without_exact_terms": int(ledger_unresolved_company_cash.height),
        "unmatched_mops_cash_events": int(missing.height),
        "missing_examples": missing.head(50).to_dicts(),
        "failure_count": len(failures),
        "failures": failures[:200],
        "coverage_complete": bool(complete),
        "baseline_established": bool(complete and output.exists()),
        "rows": int(ledger.height),
        "output_receipt": _file_receipt(output) if complete else None,
        "raw_receipt_manifest": raw_receipt_manifest,
        "reference_receipt": reference_receipt,
        "universe_receipt": universe_receipt,
    }
    _write_json_atomic(summary_path if complete else attempt_summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    if not complete:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
