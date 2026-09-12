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
import math
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
MOPS_STOCK_DELIVERY_URL = f"{MOPS_BASE}/ajax_t59sb09"
TWSE_ETF_DIVIDEND_URL = "https://www.twse.com.tw/zh/ETFortune/dividendList"
TPEX_ETF_DIVIDEND_URL = "https://info.tpex.org.tw/api/etfExDiv"
SCHEMA_VERSION = 5
PARSER_CONTRACT_VERSION = 4
ETF_PARSER_CONTRACT_VERSION = 1
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


@dataclass(frozen=True, slots=True)
class StockDeliveryDetailKey:
    market: str
    symbol: str
    roc_year: int
    announcement_date: date
    sequence: int
    subject: str


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
    parser.add_argument("--retained-source-dir", type=Path, action="append", default=[],
                        help="Import only hash-verified retained official responses for missing events.")
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
    method: str = "POST",
) -> None:
    request = {
        "url": str(url),
        "data": {str(key): str(value) for key, value in sorted(data.items())},
    }
    if method != "POST":
        request["method"] = method
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


def _public_access_denial_response(content: bytes) -> bool:
    """TWSE can return HTTP 200 containing a WAF page, not market data."""
    lowered = content.lower()
    return any(marker in lowered for marker in (
        b"for security reasons, this page can not be accessed",
        "因為安全性考量，您所執行的頁面無法呈現".encode("utf-8"),
    ))


def _cached_or_post(
    path: Path,
    *,
    url: str,
    data: dict[str, str],
    timeout: int,
    retries: int,
    method: str = "POST",
) -> bytes:
    if method not in {"GET", "POST"}:
        raise ValueError("unsupported public-data request method")
    if path.exists():
        content = path.read_bytes()
        if not content.strip():
            raise ValueError(f"empty cached MOPS receipt: {path}")
        if _mops_throttle_response(content) or _public_access_denial_response(content):
            # Retain the exact failed response as diagnostic evidence, but
            # never register it as a successful issuer/source receipt. Retry
            # the canonical endpoint; do not treat cached denial as permanent
            # data or try to bypass the publisher's access controls.
            rejected = path.parent / "rejected" / f"{_sha256_bytes(content)}.response"
            if not rejected.exists():
                _write_bytes_atomic(rejected, content)
            elif rejected.read_bytes() != content:
                raise RuntimeError(f"rejected-response hash collision: {rejected}")
        else:
            _record_raw_receipt_request(path, url=url, data=data, content=content, method=method)
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
            response = (requests.get(url, params=data, headers=headers, timeout=(int(timeout), int(timeout)))
                        if method == "GET" else requests.post(url, data=data, headers=headers,
                                                             timeout=(int(timeout), int(timeout))))
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
            if _mops_throttle_response(candidate) or _public_access_denial_response(candidate):
                last_error = RuntimeError(
                    "official source returned an HTTP-200 throttle/access-denial page"
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
    _record_raw_receipt_request(path, url=url, data=data, content=content, method=method)
    return content


def parse_etf_distributions(content: bytes, *, key: BulkDividendKey) -> list[dict[str, Any]]:
    """Exact issuer amounts/pay dates from the exchange's ETF distribution view.

    Raw prices and benchmark adjustment factors never determine a cash claim.
    Future announcements may omit an amount; preserve that as unknown, not 0.
    """
    source_url = TWSE_ETF_DIVIDEND_URL if key.market == "twse" else TPEX_ETF_DIVIDEND_URL
    if key.market == "twse":
        soup = BeautifulSoup(content, "html.parser")
        table = soup.select_one("#myTable")
        if table is None or "收益分配發放日" not in table.get_text():
            raise ValueError("TWSE ETF distribution table/schema missing")
        raw_rows = []
        for tr in table.select("tbody > tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td", recursive=False)]
            if len(cells) == 1 and "無資料" in cells[0]:
                continue
            if len(cells) != 8:
                raise ValueError("TWSE ETF distribution row width changed")
            raw_rows.append(dict(zip(("stockNo", "stockName", "divDate", "inBaseDate",
                                      "inDate", "amount", "description", "year"), cells)))
    elif key.market == "tpex":
        raw_rows = json.loads(content)
        if not isinstance(raw_rows, list):
            raise ValueError("TPEx ETF distributions must be an array")
    else:
        raise ValueError(f"unsupported ETF venue: {key.market}")
    result: dict[tuple[date, str], dict[str, Any]] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict) or not {"stockNo", "divDate", "inDate", "amount", "year"} <= raw.keys():
            raise ValueError("ETF distribution schema changed")
        symbol = str(raw["stockNo"]).strip().upper()
        ex_date, payment = _roc_date(str(raw["divDate"])), _roc_date(str(raw["inDate"]))
        if not SYMBOL_PATTERN.fullmatch(symbol) or ex_date is None:
            raise ValueError("ETF distribution has invalid symbol/ex-date")
        if str(raw["year"]).strip() != str(key.roc_year):
            raise ValueError("ETF distribution returned a different disclosure year")
        text = str(raw["amount"] or "").strip().replace(",", "")
        cash = None if text in {"", "-", "--"} else _number(text)
        if text not in {"", "-", "--"} and (cash is None or cash <= 0 or not re.fullmatch(r"\d+(?:\.\d+)?", text)):
            raise ValueError("ETF distribution cash amount is invalid or ambiguous")
        source_issue = None
        if payment is not None and payment < ex_date and cash is not None:
            raise ValueError("ETF distribution payment precedes ex-date")
        if payment is not None and payment < ex_date:
            # Preserve malformed preliminary announcements (e.g. TPEx 00764B
            # reports ROC 190 instead of 109) as unknown, never correct dates
            # by guessing. The raw receipt and explicit issue remain auditable.
            source_issue = "preliminary_payment_precedes_exdate"
            payment = None
        row = dict(date=ex_date, symbol=symbol, market=key.market,
                   announcement_date=None, announcement_sequence=0,
                   record_date=_roc_date(str(raw.get("inBaseDate") or "")),
                   stop_transfer_start=None, stop_transfer_end=None,
                   cash_dividend_per_share=cash, cash_payment_date=payment,
                   stock_dividend_ratio=0.0,
                   stock_dividend_value_per_share=0.0,
                   stock_par_value=None,
                   stock_delivery_date=None,
                   stock_terms_complete=True,
                   subscription_ratio=0.0, subscription_price=0.0,
                   subscription_payment_start=None, subscription_payment_end=None,
                   source_url=source_url, source_issue=source_issue,
                   source_disclosure_year=key.roc_year)
        identity = (ex_date, symbol)
        if identity in result and result[identity] != row:
            previous = result[identity]
            # Exchange pages retain both preliminary (no amount) and final
            # distribution announcements. Only a final, positive amount with
            # a valid payment date supersedes an incomplete preliminary row.
            previous_final = previous["cash_dividend_per_share"] is not None and previous["cash_payment_date"] is not None
            current_final = cash is not None and payment is not None
            if previous_final and cash is None:
                continue
            if not (current_final and previous["cash_dividend_per_share"] is None):
                raise ValueError(f"conflicting ETF distribution terms: {identity}")
        result[identity] = row
    return list(result.values())


def _collapse_etf_event_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One distribution per ex-date, including cross-disclosure-year updates.

    A priced final announcement outranks a price-less preliminary one; among
    finals use the later explicitly reported disclosure year, never file mtime
    or network completion order. Same-year conflicting finals remain an error.
    """
    result: dict[tuple[date, str], dict[str, Any]] = {}
    for row in rows:
        identity = (row["date"], row["symbol"])
        prior = result.get(identity)
        def rank(value: dict[str, Any]) -> tuple[bool, int]:
            return (value["cash_dividend_per_share"] is not None
                    and value["cash_payment_date"] is not None,
                    int(value["source_disclosure_year"]))
        if prior is None or rank(row) > rank(prior):
            result[identity] = row
        elif rank(row) == rank(prior) and row != prior:
            # Some cross-listed distributions occur in both exchange views.
            # Ignore provenance-only differences, never financial differences.
            terms = lambda value: {k: v for k, v in value.items() if k not in {"market", "source_url"}}
            if terms(row) != terms(prior):
                raise ValueError(f"conflicting cross-year ETF distribution: {identity}")
            result[identity] = min((row, prior), key=lambda value: value["source_url"])
    return [result[key] for key in sorted(result)]


def _fetch_etf_distributions(raw_root: Path, key: BulkDividendKey, args: argparse.Namespace) -> list[dict[str, Any]]:
    year = key.roc_year + 1911
    identity = f"{key.market}-{year}-asof-{args.end_date}-etf-v{ETF_PARSER_CONTRACT_VERSION}"
    url = TWSE_ETF_DIVIDEND_URL if key.market == "twse" else TPEX_ETF_DIVIDEND_URL
    data = ({"stkNo": "", "startDate": str(year), "endDate": str(year)} if key.market == "twse"
            else {"stkNo": "", "startDate": f"{year}0101", "endDate": f"{year}1231", "lang": "zh-tw"})
    content = _cached_or_post(_raw_path(raw_root, "etf_distributions", identity), url=url,
                             data=data, timeout=args.timeout, retries=args.retries,
                             method="GET" if key.market == "twse" else "POST")
    return parse_etf_distributions(content, key=key)


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


def parse_mops_stock_delivery_listing(
    content: bytes, *, key: ListingKey
) -> list[StockDeliveryDetailKey]:
    """Discover issuer stock-issue and delivery notices from MOPS t59sb09."""
    if _mops_throttle_response(content):
        raise ValueError(f"MOPS stock-delivery listing was throttled for {key}")
    soup = BeautifulSoup(content, "html.parser")
    text = " ".join(soup.get_text(" ", strip=True).split())
    if "公司代號輸入錯誤" in text or "公司代號不可空白" in text:
        raise ValueError(f"MOPS rejected stock-delivery listing key {key}")
    output: list[StockDeliveryDetailKey] = []
    seen: set[tuple[date, int]] = set()
    for control in soup.select("input[onclick]"):
        onclick = str(control.get("onclick") or "")
        date_match = re.search(r'DATE1\.value="([0-9]{8})"', onclick)
        sequence_match = re.search(r'SKEY\.value="([0-9]+)"', onclick)
        if date_match is None or sequence_match is None:
            continue
        row = control.find_parent("tr")
        subject = " ".join(
            (row.get_text(" ", strip=True) if row is not None else "").split()
        )
        if "發行新股" not in subject:
            continue
        announcement_date = date.fromisoformat(
            f"{date_match.group(1)[:4]}-{date_match.group(1)[4:6]}-"
            f"{date_match.group(1)[6:]}"
        )
        sequence = int(sequence_match.group(1))
        identity = (announcement_date, sequence)
        if identity in seen:
            continue
        seen.add(identity)
        output.append(
            StockDeliveryDetailKey(
                market=key.market,
                symbol=key.symbol,
                roc_year=key.roc_year,
                announcement_date=announcement_date,
                sequence=sequence,
                subject=subject,
            )
        )
    return output


def parse_mops_stock_delivery_detail(
    content: bytes, *, key: StockDeliveryDetailKey
) -> dict[str, Any]:
    """Parse only terms required to bind an ex-date right to delivery."""
    if _mops_throttle_response(content):
        raise ValueError(f"MOPS stock-delivery detail was throttled for {key}")
    soup = BeautifulSoup(content, "html.parser")
    text = " ".join(soup.get_text(" ", strip=True).split())
    compact = re.sub(r"\s+", "", text)
    if key.symbol not in compact[:1000]:
        raise ValueError(f"MOPS stock-delivery detail symbol differs for {key}")
    issued = re.search(r"(?:計)?發行新股([0-9,]+)股", compact)
    ratio = re.search(r"每[仟千]股(?:無償)?(?:派發|配發)([0-9,.]+)股", compact)
    record = re.search(
        r"(?:普通股)?增資配股基準日[：:]?(?:本公司訂於)?"
        r"(?:民國)?([0-9]{2,3}年[0-9]{1,2}月[0-9]{1,2}日)",
        compact,
    )
    delivery = re.search(
        r"訂於([0-9]{2,3}年[0-9]{1,2}月[0-9]{1,2}日)"
        r"[^。]{0,80}?(?:發放|交付)[^。]{0,40}?(?:上市|上櫃)買賣",
        compact,
    )
    return {
        "key": key,
        "issue_shares": (
            None if issued is None else int(issued.group(1).replace(",", ""))
        ),
        "stock_ratio": (
            None if ratio is None else float(ratio.group(1).replace(",", "")) / 1000.0
        ),
        "record_date": _roc_date(record.group(1)) if record else None,
        "delivery_date": _roc_date(delivery.group(1)) if delivery else None,
        "source_url": MOPS_STOCK_DELIVERY_URL,
    }


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
        "stock_dividend_value_per_share": float(
            stock_dividend_per_share or 0.0
        ),
        "stock_par_value": None,
        "stock_delivery_date": None,
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


def _strict_par_value(value: str, *, key: BulkDividendKey) -> float | None:
    """Parse issuer par evidence; never assume the historical NTD 10 norm."""
    text = re.sub(r"\s+", "", str(value or ""))
    if "無面額" in text:
        return None
    match = re.search(r"(?:新台幣|NT\$?)?([0-9][0-9,.]*)元", text, re.I)
    result = _number(match.group(1)) if match else None
    if result is None or result <= 0.0:
        raise ValueError(
            f"MOPS bulk dividend has an invalid par value for {key}: {value!r}"
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
            "par_value": 22,
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
            "par_value": 17,
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
            "par_value": 18,
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
            "par_value": _strict_par_value(
                cells[layout["par_value"]], key=key
            ),
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
        par_value = source_payload["par_value"]
        cash_per_share = source_payload["cash_per_share"]
        subscription_percent = source_payload["subscription_percent"]
        subscription_price = source_payload["subscription_price"]
        stock_terms_complete = stock_value_per_share == 0.0 or par_value is not None
        stock_ratio = (
            0.0
            if stock_value_per_share == 0.0 or par_value is None
            else stock_value_per_share / par_value
        )
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
                        float(stock_ratio) if is_stock_date else 0.0
                    ),
                    "stock_dividend_value_per_share": (
                        float(stock_value_per_share) if is_stock_date else 0.0
                    ),
                    "stock_par_value": (
                        float(par_value)
                        if is_stock_date and par_value is not None
                        else None
                    ),
                    "stock_delivery_date": None,
                    "stock_terms_complete": bool(stock_terms_complete),
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


def _recover_retained_bulk_rows(
    raw_root: Path, *, output_dir: Path, missing: set[tuple[date, str]], end: date,
    target_output_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Recover disappeared historical disclosures from verified source receipts.

    A current bulk listing is not a historical tombstone. Reuse only bytes
    already bound by a content-addressed official request/response manifest;
    never infer a distribution from prices or trust an unreceipted cache file.
    Current returned rows always win, including explicitly incomplete terms.
    """
    if not missing:
        return []
    known = {}
    for manifest in (raw_root / "manifests").glob("*.jsonl"):
        content = manifest.read_bytes()
        if _sha256_bytes(content) != manifest.stem:
            continue
        for line in content.splitlines():
            item = json.loads(line)
            match = re.fullmatch(
                r"(twse|tpex)-(\d+)(?:-asof-(\d{8}))?-v(?:3|4)\.html",
                Path(item["path"]).name,
            )
            if (not match or "/bulk_dividends/" not in item["path"]
                    or (match[3] and match[3] > end.strftime("%Y%m%d"))):
                continue
            key = BulkDividendKey(match[1], int(match[2]))
            if not any(day.year - 1911 in {key.roc_year, key.roc_year + 1} for day, _ in missing):
                continue
            known[item["path"]] = (item, key)
    with _RAW_RECEIPT_REQUESTS_LOCK:
        seen_hashes = {row["response_sha256"] for row in _RAW_RECEIPT_REQUESTS.values()}
    recovered = []
    remaining = set(missing)
    for name, (item, key) in sorted(known.items(), reverse=True):
        if not remaining:
            break
        digest = item["response_sha256"]
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        path = (output_dir / name).resolve()
        if not path.is_relative_to(raw_root.resolve()):
            raise ValueError("retained corporate-action receipt escapes source root")
        if not path.is_file():
            continue
        content = path.read_bytes()
        if len(content) != item["response_size"] or _sha256_bytes(content) != digest:
            raise ValueError(f"retained corporate-action receipt changed: {name}")
        request = item["request"]
        if (request.get("url") != MOPS_BULK_DIVIDEND_URL
                or request.get("data", {}).get("year") != str(key.roc_year)
                or request.get("data", {}).get("TYPEK") != _market_typek(key.market)):
            continue
        canonical = json.dumps(request, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        if _sha256_bytes(canonical) != item["request_sha256"]:
            raise ValueError("retained corporate-action request hash mismatch")
        selected = [row for row in _collapse_bulk_event_rows(parse_mops_bulk_dividends(content, key=key))
                    if (row["date"], row["symbol"]) in remaining]
        if selected:
            if target_output_dir is not None:
                target = target_output_dir / name
                if not target.resolve().is_relative_to(target_output_dir.resolve() / "raw"):
                    raise ValueError("retained corporate-action destination escapes raw root")
                if target.exists() and target.read_bytes() != content:
                    raise ValueError("retained corporate-action destination collision")
                if not target.exists():
                    _write_bytes_atomic(target, content)
                path = target
            _record_raw_receipt_request(path, url=request["url"], data=request["data"], content=content)
            recovered.extend(selected)
            remaining -= {(row["date"], row["symbol"]) for row in selected}
    return recovered


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


def _fetch_stock_delivery_listing(
    raw_root: Path,
    key: ListingKey,
    args: argparse.Namespace,
) -> list[StockDeliveryDetailKey]:
    disclosure_year = key.roc_year + 1911
    identity = (
        f"{key.market}-{key.symbol}-{key.roc_year:03d}"
        f"{_mutable_receipt_suffix(disclosure_year=disclosure_year, args=args)}"
        f"-v{PARSER_CONTRACT_VERSION}"
    )
    content = _cached_or_post(
        _raw_path(raw_root, "stock_delivery_lists", identity),
        url=MOPS_STOCK_DELIVERY_URL,
        data={
            "step": "1",
            "firstin": "1",
            "TYPEK": _market_typek(key.market),
            "co_id": key.symbol,
            "year": str(key.roc_year),
        },
        timeout=args.timeout,
        retries=args.retries,
    )
    return parse_mops_stock_delivery_listing(content, key=key)


def _fetch_stock_delivery_listing_with_market_fallback(
    raw_root: Path,
    key: ListingKey,
    args: argparse.Namespace,
) -> list[StockDeliveryDetailKey]:
    try:
        return _fetch_stock_delivery_listing(raw_root, key, args)
    except ValueError as primary:
        if "rejected stock-delivery listing key" not in str(primary):
            raise
    alternate = ListingKey(
        market="twse" if key.market == "tpex" else "tpex",
        symbol=key.symbol,
        roc_year=key.roc_year,
    )
    try:
        return _fetch_stock_delivery_listing(raw_root, alternate, args)
    except ValueError as secondary:
        if "rejected stock-delivery listing key" not in str(secondary):
            raise
        return []


def _fetch_stock_delivery_detail(
    raw_root: Path,
    key: StockDeliveryDetailKey,
    args: argparse.Namespace,
) -> dict[str, Any]:
    identity = (
        f"{key.market}-{key.symbol}-{key.announcement_date:%Y%m%d}-"
        f"{key.sequence}"
        f"{_mutable_receipt_suffix(disclosure_year=key.announcement_date.year, args=args)}"
        f"-v{PARSER_CONTRACT_VERSION}"
    )
    content = _cached_or_post(
        _raw_path(raw_root, "stock_delivery_details", identity),
        url=MOPS_STOCK_DELIVERY_URL,
        data={
            "step": "2",
            "firstin": "1",
            "TYPEK": _market_typek(key.market),
            "co_id": key.symbol,
            "year": str(key.roc_year),
            "DATE1": key.announcement_date.strftime("%Y%m%d"),
            "SKEY": str(key.sequence),
        },
        timeout=args.timeout,
        retries=args.retries,
    )
    return parse_mops_stock_delivery_detail(content, key=key)


def _attach_exact_stock_delivery_dates(
    rows: list[dict[str, Any]],
    *,
    raw_root: Path,
    args: argparse.Namespace,
    workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], dict[str, int], bool]:
    """Bind bulk stock ratios to an independent issuer delivery disclosure."""
    candidates = [
        row
        for row in rows
        if float(row.get("stock_dividend_ratio") or 0.0) > 0.0
        and bool(row.get("stock_terms_complete"))
        and float(row.get("subscription_ratio") or 0.0) == 0.0
    ]
    for row in rows:
        row.setdefault("stock_delivery_date", None)
        row.setdefault("stock_dividend_value_per_share", 0.0)
        row.setdefault("stock_par_value", None)
    if not candidates:
        return rows, [], {
            "requested_stock_delivery_company_years": 0,
            "discovered_stock_delivery_details": 0,
            "parsed_stock_delivery_details": 0,
            "resolved_stock_delivery_events": 0,
            "unresolved_stock_delivery_events": 0,
        }, False

    listing_keys = sorted(
        {
            ListingKey(
                market=str(row["market"]),
                symbol=str(row["symbol"]),
                roc_year=year - 1911,
            )
            for row in candidates
            for year in (row["date"].year, row["date"].year + 1)
        },
        key=lambda value: (value.market, value.symbol, value.roc_year),
    )
    listing_limited = int(args.max_list_requests) > 0
    if listing_limited:
        listing_keys = listing_keys[: int(args.max_list_requests)]
    failures: list[dict[str, str]] = []
    detail_keys: list[StockDeliveryDetailKey] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _fetch_stock_delivery_listing_with_market_fallback,
                raw_root,
                key,
                args,
            ): key
            for key in listing_keys
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                detail_keys.extend(future.result())
            except Exception as exc:
                failures.append(
                    {
                        "stage": "stock_delivery_listing",
                        "key": repr(key),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    detail_keys = sorted(
        set(detail_keys),
        key=lambda value: (
            value.market,
            value.symbol,
            value.announcement_date,
            value.sequence,
        ),
    )
    detail_limited = int(args.max_detail_requests) > 0
    selected_detail_keys = (
        detail_keys[: int(args.max_detail_requests)]
        if detail_limited
        else detail_keys
    )
    detail_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _fetch_stock_delivery_detail, raw_root, key, args
            ): key
            for key in selected_detail_keys
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                detail_rows.append(future.result())
            except Exception as exc:
                failures.append(
                    {
                        "stage": "stock_delivery_detail",
                        "key": repr(key),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for detail in detail_rows:
        key = detail["key"]
        by_symbol.setdefault(key.symbol, []).append(detail)
    resolved = 0
    for row in candidates:
        symbol_details = by_symbol.get(str(row["symbol"]), [])
        ratio = float(row["stock_dividend_ratio"])
        issues = [
            detail
            for detail in symbol_details
            if detail["record_date"] == row.get("record_date")
            and detail["stock_ratio"] is not None
            and math.isclose(
                float(detail["stock_ratio"]), ratio, rel_tol=0.0, abs_tol=1e-12
            )
            and detail["issue_shares"] is not None
        ]
        issue_sizes = {int(detail["issue_shares"]) for detail in issues}
        delivery_dates = {
            detail["delivery_date"]
            for detail in symbol_details
            if detail["delivery_date"] is not None
            and detail["delivery_date"] >= row["date"]
            and detail["issue_shares"] in issue_sizes
        }
        if len(issue_sizes) == 1 and len(delivery_dates) == 1:
            row["stock_delivery_date"] = next(iter(delivery_dates))
            resolved += 1
    return rows, failures, {
        "requested_stock_delivery_company_years": len(listing_keys),
        "discovered_stock_delivery_details": len(detail_keys),
        "parsed_stock_delivery_details": len(detail_rows),
        "resolved_stock_delivery_events": resolved,
        "unresolved_stock_delivery_events": len(candidates) - resolved,
    }, bool(detail_limited or listing_limited)


def _run(args: argparse.Namespace) -> None:
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

    failures.sort(key=lambda item: (item["stage"], item["key"], item["error"]))

    parsed_rows = _collapse_bulk_event_rows(parsed_rows)
    present_keys = {(row["date"], row["symbol"]) for row in parsed_rows}
    missing_keys = {(row["date"], row["symbol"]) for row in company_cash_events.iter_rows(named=True)} - present_keys
    retained_rows = _recover_retained_bulk_rows(raw_root, output_dir=args.output_dir,
        missing=missing_keys, end=date.fromisoformat(args.end_date))
    for source in getattr(args, "retained_source_dir", []) or []:
        missing_keys -= {(row["date"], row["symbol"]) for row in retained_rows}
        retained_rows.extend(_recover_retained_bulk_rows(
            source / "raw/tw_corporate_action_entitlements", output_dir=source,
            missing=missing_keys, end=date.fromisoformat(args.end_date), target_output_dir=args.output_dir))
    parsed_rows.extend(retained_rows)
    (
        parsed_rows,
        stock_delivery_failures,
        stock_delivery_counts,
        detail_limited,
    ) = _attach_exact_stock_delivery_dates(
        parsed_rows,
        raw_root=raw_root,
        args=args,
        workers=workers,
    )
    failures.extend(stock_delivery_failures)
    failures.sort(key=lambda item: (item["stage"], item["key"], item["error"]))
    etf_events = joined.filter(pl.col("security_type").eq("etf"))
    etf_keys = sorted({BulkDividendKey(str(r["market"]), r["date"].year - 1911)
                       for r in etf_events.iter_rows(named=True)}, key=lambda k: (k.market, k.roc_year))
    etf_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_etf_distributions, raw_root, key, args): key for key in etf_keys}
        for index, future in enumerate(as_completed(futures), start=1):
            key = futures[future]
            try:
                etf_rows.extend(future.result())
            except Exception as exc:
                failures.append({"stage": "etf_distributions", "key": repr(key),
                                 "error": f"{type(exc).__name__}: {exc}"})
            print(f"[corporate-action] ETF years {index}/{len(etf_keys)} events={len(etf_rows)} failures={len(failures)}", flush=True)
    etf_rows = _collapse_etf_event_rows(etf_rows)
    parsed_rows.extend(etf_rows)
    parsed = (
        pl.from_dicts(parsed_rows, infer_schema_length=None)
        if parsed_rows
        else pl.DataFrame()
    )
    if parsed.height:
        if parsed.select(pl.struct("date", "symbol").is_duplicated().any()).item():
            raise ValueError("corporate-action entitlement source has duplicate event keys")
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

    exact_cash = (
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
    exact_inventory = (
        parsed.filter(
            pl.col("stock_terms_complete")
            & (pl.col("stock_dividend_ratio") > 0.0)
            & pl.col("stock_delivery_date").is_not_null()
            & (pl.col("subscription_ratio") == 0.0)
            & (
                (pl.col("cash_dividend_per_share") == 0.0)
                | pl.col("cash_payment_date").is_not_null()
            )
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
        "stock_dividend_value_per_share",
        "stock_par_value",
        "stock_delivery_date",
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
            pl.concat(
                (
                    exact_cash.select("date", "symbol").with_columns(
                        pl.lit("exact_cash").alias("exact_handling")
                    ),
                    exact_inventory.select("date", "symbol").with_columns(
                        pl.lit("exact_inventory").alias("exact_handling")
                    ),
                ),
                how="vertical",
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
                "exact_handling": pl.String,
            }
        ).rename({"source_url": "mops_source_url"})
    )
    ledger = (
        joined.join(details_for_join, on=["date", "symbol"], how="left")
        .with_columns(
            pl.when(pl.col("exact_handling").is_not_null())
            .then(pl.col("exact_handling"))
            .otherwise(pl.lit("avoid"))
            .alias("handling"),
            pl.when(pl.col("exact_handling").eq("exact_cash").fill_null(False))
            .then(
                pl.when(pl.col("security_type").eq("etf"))
                .then(pl.lit("exchange_etf_exact_cash"))
                .otherwise(pl.lit("mops_exact_cash"))
            )
            .when(pl.col("exact_handling").eq("exact_inventory").fill_null(False))
            .then(pl.lit("mops_exact_pending_stock"))
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
            .when(pl.col("security_type").eq("etf"))
            .then(pl.lit("etf_terms_unavailable_or_noncash_action"))
            .otherwise(pl.lit("noncompany_action"))
            .alias("handling_reason"),
        )
        .drop("security_type", "universe_market", "exact_handling")
        .sort(["date", "symbol"])
    )
    ledger_exact_cash = ledger.filter(pl.col("handling").eq("exact_cash"))
    ledger_exact_inventory = ledger.filter(
        pl.col("handling").eq("exact_inventory")
    )
    if ledger.height != reference.height or ledger.select(pl.struct("date", "symbol").is_duplicated().any()).item():
        raise ValueError("corporate-action entitlement join changed reference identity/count")
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
        "stock_delivery_source": MOPS_STOCK_DELIVERY_URL,
        **stock_delivery_counts,
        "etf_sources": [TWSE_ETF_DIVIDEND_URL, TPEX_ETF_DIVIDEND_URL],
        "etf_parser_contract_version": ETF_PARSER_CONTRACT_VERSION,
        "etf_source_issues": [{"date": str(row["date"]), "symbol": row["symbol"],
                              "issue": row["source_issue"]}
                             for row in etf_rows if row.get("source_issue")],
        "requested_etf_market_years": len(etf_keys),
        "etf_reference_rows": etf_events.height,
        "exact_etf_cash_events": ledger.filter(pl.col("handling_reason") == "exchange_etf_exact_cash").height,
        "unresolved_etf_events": ledger.filter(pl.col("handling_reason") == "etf_terms_unavailable_or_noncash_action").height,
        "requested_bulk_market_years": len(bulk_keys),
        "requested_list_keys": 0,
        "discovered_details": 0,
        "parsed_details": int(parsed.height),
        "retained_official_disclosure_rows_recovered": len(retained_rows),
        "parsed_exact_cash_candidates": int(exact_cash.height),
        "parsed_exact_inventory_candidates": int(exact_inventory.height),
        "exact_cash_events": int(ledger_exact_cash.height),
        "exact_inventory_events": int(ledger_exact_inventory.height),
        "avoided_events": int(
            ledger.height
            - ledger_exact_cash.height
            - ledger_exact_inventory.height
        ),
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


def main() -> None:
    import fcntl

    args = parse_args()
    lock_path = args.output_dir / "state" / "locks" / "tw_corporate_action_entitlements.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _run(args)


if __name__ == "__main__":
    main()
