#!/usr/bin/env python3
"""Download the recent TAIFEX TX/TXO trade-by-trade public files.

TAIFEX publishes rolling pages for the latest 30 trading days.  Each official
ZIP contains every futures or options product for one TAIFEX trading date.  We
retain those immutable ZIPs and build filtered Parquet partitions for TX and
TXO without pretending that the source has sub-second timestamps or trade IDs.

Quantity semantics are deliberately explicit:

* futures reports ``成交數量(B+S)`` on each row, so matched contracts are B+S/2;
* options reports ``成交數量(B or S)`` as side rows.  Dividing each side row by
  two gives an additive matched-volume equivalent.  The parser also verifies
  that every date/time/contract/price bucket has an even side-quantity total.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
import fcntl
import html
import io
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import BinaryIO, Final, TextIO
import zipfile
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import (  # noqa: E402
    atomic_write_bytes,
    atomic_write_parquet,
    atomic_write_text,
    sha256_bytes,
    sha256_file,
)
from downloader.common import (  # noqa: E402
    SharedRateLimiter,
    resolve_request_interval,
)
from downloader.http_transport import (  # noqa: E402
    HttpRequestPolicy,
    ResilientHttpTransport,
)


FUTURES_LISTING_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futPrevious30DaysSalesData"
)
OPTIONS_LISTING_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optPrevious30DaysSalesData"
)
USER_AGENT: Final[str] = "stockAgent/taifex-index-derivatives-ticks"
PARSER_CONTRACT_VERSION: Final[int] = 2
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")

FUTURES_URL_RE: Final[re.Pattern[str]] = re.compile(
    r"https://www\.taifex\.com\.tw/file/taifex/Dailydownload/"
    r"DailydownloadCSV/Daily_(\d{4}_\d{2}_\d{2})\.zip"
)
OPTIONS_URL_RE: Final[re.Pattern[str]] = re.compile(
    r"https://www\.taifex\.com\.tw/file/taifex/Dailydownload/"
    r"OptionsDailydownloadCSV/OptionsDaily_(\d{4}_\d{2}_\d{2})\.zip"
)

OPTION_COLUMNS: Final[list[str]] = [
    "trading_date",
    "event_date",
    "event_time",
    "event_ts",
    "session",
    "product",
    "delivery_month_week",
    "strike_price",
    "option_right",
    "price",
    "reported_side_quantity",
    "matched_quantity_equivalent",
    "opening_auction",
    "source_row_number",
    "source_file",
    "source_sha256",
]
FUTURES_COLUMNS: Final[list[str]] = [
    "trading_date",
    "event_date",
    "event_time",
    "event_ts",
    "session",
    "product",
    "delivery_month_week",
    "price",
    "reported_b_plus_s_quantity",
    "matched_quantity",
    "near_month_price",
    "far_month_price",
    "opening_auction",
    "source_row_number",
    "source_file",
    "source_sha256",
]


def _sha256_bytes(payload: bytes) -> str:
    return sha256_bytes(payload)


def _sha256_path(path: Path) -> str:
    return sha256_file(path)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    atomic_write_bytes(path, payload, durable=True)


def _atomic_write_text(path: Path, text: str) -> None:
    atomic_write_text(path, text, durable=True)


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(
        path,
        frame,
        compression="zstd",
        write_statistics=True,
        durable=True,
    )


def _fetch_bytes(
    url: str,
    *,
    attempts: int,
    timeout: float,
    transport: ResilientHttpTransport | None = None,
) -> tuple[bytes, dict[str, str]]:
    client = transport or ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=timeout,
            max_retries=max(0, attempts - 1),
            retry_base_seconds=0.5,
        )
    )
    response = client.request_bytes(url, headers={"User-Agent": USER_AGENT})
    if not response.body:
        raise ValueError(f"empty HTTP response from {url}")
    headers = {key.casefold(): value for key, value in response.headers.items()}
    return response.body, headers


def _extract_downloads(
    page: bytes,
    *,
    pattern: re.Pattern[str],
) -> dict[date, str]:
    text = html.unescape(page.decode("utf-8"))
    downloads: dict[date, str] = {}
    for match in pattern.finditer(text):
        compact = match.group(1)
        trade_date = date.fromisoformat(compact.replace("_", "-"))
        url = match.group(0)
        previous = downloads.setdefault(trade_date, url)
        if previous != url:
            raise ValueError(f"conflicting URLs for {trade_date}: {previous} vs {url}")
    if not downloads:
        raise ValueError("TAIFEX listing contained no matching ZIP URLs")
    return downloads


def _selected_common_dates(
    futures: dict[date, str],
    options: dict[date, str],
    *,
    count: int,
) -> list[date]:
    common = sorted(set(futures) & set(options))
    if len(common) < count:
        raise ValueError(
            f"only {len(common)} common TAIFEX trading dates are available; need {count}"
        )
    return common[-count:]


def _validate_zip_payload(payload: bytes, *, expected_member: str) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            members = [
                name for name in archive.namelist() if name.lower().endswith(".csv")
            ]
            if members != [expected_member]:
                raise ValueError(
                    f"ZIP CSV members {members!r} do not equal expected {expected_member!r}"
                )
            info = archive.getinfo(expected_member)
            if info.file_size <= 100:
                raise ValueError(
                    f"CSV member is implausibly small: {info.file_size} bytes"
                )
            with archive.open(expected_member) as handle:
                header = handle.readline()
            if b"," not in header:
                raise ValueError("CSV member has no comma-delimited header")
    except zipfile.BadZipFile as exc:
        raise ValueError("TAIFEX response is not a valid ZIP") from exc


def _download_zip(
    url: str,
    target: Path,
    *,
    expected_member: str,
    attempts: int,
    timeout: float,
    transport: ResilientHttpTransport | None = None,
) -> dict[str, object]:
    headers: dict[str, str] = {}
    reused = False
    if target.is_file() and target.stat().st_size > 0:
        payload = target.read_bytes()
        try:
            _validate_zip_payload(payload, expected_member=expected_member)
            reused = True
        except ValueError:
            payload, headers = _fetch_bytes(
                url,
                attempts=attempts,
                timeout=timeout,
                transport=transport,
            )
            _validate_zip_payload(payload, expected_member=expected_member)
            _atomic_write_bytes(target, payload)
    else:
        payload, headers = _fetch_bytes(
            url,
            attempts=attempts,
            timeout=timeout,
            transport=transport,
        )
        _validate_zip_payload(payload, expected_member=expected_member)
        _atomic_write_bytes(target, payload)
    return {
        "path": str(target),
        "url": url,
        "bytes": len(payload),
        "sha256": _sha256_bytes(payload),
        "last_modified": headers.get("last-modified"),
        "reused": reused,
    }


def _parse_time(value: str, *, row_number: int) -> int:
    if len(value) != 6 or not value.isdigit():
        raise ValueError(f"row {row_number}: invalid HHMMSS time {value!r}")
    hour, minute, second = int(value[:2]), int(value[2:4]), int(value[4:])
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError(f"row {row_number}: out-of-range HHMMSS time {value!r}")
    return int(value)


def _session_for_time(value: int, *, row_number: int) -> str:
    if value >= 150000 or value < 84500:
        return "night"
    if value <= 134500:
        return "day"
    raise ValueError(f"row {row_number}: event falls outside TAIFEX TX/TXO sessions")


def _parse_float(value: str, *, row_number: int, field: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"row {row_number}: invalid {field} {value!r}") from exc
    if not parsed > 0.0:
        raise ValueError(f"row {row_number}: non-positive {field} {value!r}")
    return parsed


def _parse_finite_float(value: str, *, row_number: int, field: str) -> float:
    """Parse a finite value whose sign is meaningful to the official source."""

    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"row {row_number}: invalid {field} {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"row {row_number}: non-finite {field} {value!r}")
    return parsed


def _optional_float(value: str, *, row_number: int, field: str) -> float | None:
    if value in {"", "-"}:
        return None
    return _parse_float(value, row_number=row_number, field=field)


def _validate_event_date(
    value: str,
    *,
    trading_date: date,
    row_number: int,
) -> None:
    try:
        event_date = date.fromisoformat(f"{value[:4]}-{value[4:6]}-{value[6:8]}")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"row {row_number}: invalid event date {value!r}") from exc
    if event_date > trading_date or (trading_date - event_date).days > 10:
        raise ValueError(
            f"row {row_number}: event date {event_date} is incompatible with "
            f"trading date {trading_date}"
        )


def _base_frame(
    raw: dict[str, list[object]],
    *,
    trading_date: date,
    source_file: str,
    source_sha256: str,
) -> pl.DataFrame:
    if not raw["event_date_raw"]:
        raise ValueError(f"no target rows found in {source_file}")
    return (
        pl.DataFrame(raw)
        .with_columns(
            pl.lit(trading_date).cast(pl.Date).alias("trading_date"),
            pl.col("event_date_raw")
            .str.strptime(pl.Date, format="%Y%m%d", strict=True)
            .alias("event_date"),
            pl.concat_str(["event_date_raw", "event_time"])
            .str.strptime(pl.Datetime("us"), format="%Y%m%d%H%M%S", strict=True)
            .dt.replace_time_zone("Asia/Taipei")
            .alias("event_ts"),
            pl.lit(source_file).alias("source_file"),
            pl.lit(source_sha256).alias("source_sha256"),
        )
        .drop("event_date_raw")
    )


def _parse_options_rows(
    text: TextIO,
    *,
    trading_date: date,
    source_file: str,
    source_sha256: str,
) -> pl.DataFrame:
    raw: dict[str, list[object]] = {
        "event_date_raw": [],
        "event_time": [],
        "session": [],
        "product": [],
        "delivery_month_week": [],
        "strike_price": [],
        "option_right": [],
        "price": [],
        "reported_side_quantity": [],
        "matched_quantity_equivalent": [],
        "opening_auction": [],
        "source_row_number": [],
    }
    for row_number, row in enumerate(csv.reader(text), start=1):
        cells = [cell.strip() for cell in row]
        if len(cells) < 9 or not cells[0].isdigit() or cells[1] != "TXO":
            continue
        if len(cells) > 9 and any(cells[9:]):
            raise ValueError(f"row {row_number}: unexpected nonempty option columns")
        _validate_event_date(cells[0], trading_date=trading_date, row_number=row_number)
        event_time = _parse_time(cells[5], row_number=row_number)
        if cells[4] not in {"C", "P"}:
            raise ValueError(f"row {row_number}: invalid option right {cells[4]!r}")
        try:
            quantity = int(cells[7])
        except ValueError as exc:
            raise ValueError(
                f"row {row_number}: invalid side quantity {cells[7]!r}"
            ) from exc
        if quantity <= 0:
            raise ValueError(f"row {row_number}: non-positive side quantity")
        if cells[8] not in {"", "*"}:
            raise ValueError(f"row {row_number}: invalid auction marker {cells[8]!r}")
        raw["event_date_raw"].append(cells[0])
        raw["event_time"].append(cells[5])
        raw["session"].append(_session_for_time(event_time, row_number=row_number))
        raw["product"].append("TXO")
        raw["delivery_month_week"].append(cells[3])
        raw["strike_price"].append(
            _parse_float(cells[2], row_number=row_number, field="strike price")
        )
        raw["option_right"].append(cells[4])
        raw["price"].append(
            _parse_float(cells[6], row_number=row_number, field="price")
        )
        raw["reported_side_quantity"].append(quantity)
        raw["matched_quantity_equivalent"].append(quantity / 2.0)
        raw["opening_auction"].append(cells[8] == "*")
        raw["source_row_number"].append(row_number)

    frame = _base_frame(
        raw,
        trading_date=trading_date,
        source_file=source_file,
        source_sha256=source_sha256,
    )
    bucket_keys = [
        "event_date",
        "event_time",
        "product",
        "delivery_month_week",
        "strike_price",
        "option_right",
        "price",
    ]
    unpaired = (
        frame.group_by(bucket_keys)
        .agg(pl.col("reported_side_quantity").sum().alias("reported_total"))
        .filter((pl.col("reported_total") % 2) != 0)
    )
    if unpaired.height:
        raise ValueError(
            f"{source_file}: {unpaired.height} TXO second-price buckets have "
            "unpaired B-or-S quantities"
        )
    return frame.select(OPTION_COLUMNS).sort(["event_ts", "source_row_number"])


def _parse_futures_rows(
    text: TextIO,
    *,
    trading_date: date,
    source_file: str,
    source_sha256: str,
    products: tuple[str, ...] = ("TX",),
    outright_contracts_only: bool = False,
) -> pl.DataFrame:
    normalized_products = tuple(str(value).strip().upper() for value in products)
    if not normalized_products or any(not value for value in normalized_products):
        raise ValueError("futures products must contain non-empty product codes")
    if len(set(normalized_products)) != len(normalized_products):
        raise ValueError("futures products must not contain duplicates")
    product_set = frozenset(normalized_products)
    raw: dict[str, list[object]] = {
        "event_date_raw": [],
        "event_time": [],
        "session": [],
        "product": [],
        "delivery_month_week": [],
        "price": [],
        "reported_b_plus_s_quantity": [],
        "matched_quantity": [],
        "near_month_price": [],
        "far_month_price": [],
        "opening_auction": [],
        "source_row_number": [],
    }
    for row_number, row in enumerate(csv.reader(text), start=1):
        cells = [cell.strip() for cell in row]
        if len(cells) < 9 or not cells[0].isdigit() or cells[1] not in product_set:
            continue
        if outright_contracts_only and "/" in cells[2]:
            continue
        if len(cells) > 9 and any(cells[9:]):
            raise ValueError(f"row {row_number}: unexpected nonempty futures columns")
        _validate_event_date(cells[0], trading_date=trading_date, row_number=row_number)
        event_time = _parse_time(cells[3], row_number=row_number)
        try:
            reported_quantity = int(cells[5])
        except ValueError as exc:
            raise ValueError(
                f"row {row_number}: invalid B+S quantity {cells[5]!r}"
            ) from exc
        if reported_quantity <= 0 or reported_quantity % 2:
            raise ValueError(
                f"row {row_number}: B+S quantity must be positive and even, got "
                f"{reported_quantity}"
            )
        if cells[8] not in {"", "*"}:
            raise ValueError(f"row {row_number}: invalid auction marker {cells[8]!r}")
        raw["event_date_raw"].append(cells[0])
        raw["event_time"].append(cells[3])
        raw["session"].append(_session_for_time(event_time, row_number=row_number))
        raw["product"].append(cells[1])
        raw["delivery_month_week"].append(cells[2])
        is_spread = "/" in cells[2]
        raw["price"].append(
            _parse_finite_float(cells[4], row_number=row_number, field="spread price")
            if is_spread
            else _parse_float(cells[4], row_number=row_number, field="price")
        )
        raw["reported_b_plus_s_quantity"].append(reported_quantity)
        raw["matched_quantity"].append(reported_quantity // 2)
        raw["near_month_price"].append(
            _optional_float(cells[6], row_number=row_number, field="near-month price")
        )
        raw["far_month_price"].append(
            _optional_float(cells[7], row_number=row_number, field="far-month price")
        )
        raw["opening_auction"].append(cells[8] == "*")
        raw["source_row_number"].append(row_number)

    frame = _base_frame(
        raw,
        trading_date=trading_date,
        source_file=source_file,
        source_sha256=source_sha256,
    )
    return frame.select(FUTURES_COLUMNS).sort(["event_ts", "source_row_number"])


def _parse_zip(
    path: Path,
    *,
    kind: str,
    trading_date: date,
    source_sha256: str,
    futures_products: tuple[str, ...] = ("TX",),
    futures_outright_contracts_only: bool = False,
) -> pl.DataFrame:
    expected_member = (
        f"OptionsDaily_{trading_date.strftime('%Y_%m_%d')}.csv"
        if kind == "options"
        else f"Daily_{trading_date.strftime('%Y_%m_%d')}.csv"
    )
    with zipfile.ZipFile(path) as archive, archive.open(expected_member) as binary:
        with io.TextIOWrapper(binary, encoding="cp950", newline="") as text:
            if kind == "options":
                return _parse_options_rows(
                    text,
                    trading_date=trading_date,
                    source_file=str(path),
                    source_sha256=source_sha256,
                )
            return _parse_futures_rows(
                text,
                trading_date=trading_date,
                source_file=str(path),
                source_sha256=source_sha256,
                products=futures_products,
                outright_contracts_only=futures_outright_contracts_only,
            )


def _frame_stats(frame: pl.DataFrame, *, kind: str) -> dict[str, object]:
    quantity = (
        "matched_quantity_equivalent" if kind == "options" else "matched_quantity"
    )
    sessions = {
        str(row[0]): {"rows": int(row[1]), "matched_quantity": float(row[2])}
        for row in frame.group_by("session")
        .agg(pl.len().alias("rows"), pl.col(quantity).sum().alias("quantity"))
        .sort("session")
        .iter_rows()
    }
    stats: dict[str, object] = {
        "rows": frame.height,
        "event_ts_min": frame["event_ts"].min().isoformat(),
        "event_ts_max": frame["event_ts"].max().isoformat(),
        "matched_quantity": float(frame[quantity].sum()),
        "sessions": sessions,
    }
    if kind == "options":
        stats["second_price_buckets"] = (
            frame.select(
                [
                    "event_date",
                    "event_time",
                    "delivery_month_week",
                    "strike_price",
                    "option_right",
                    "price",
                ]
            )
            .unique()
            .height
        )
        stats["contracts"] = (
            frame.select(["delivery_month_week", "strike_price", "option_right"])
            .unique()
            .height
        )
    else:
        stats["contracts"] = frame["delivery_month_week"].n_unique()
    return stats


def _partition_paths(root: Path, kind: str, trading_date: date) -> tuple[Path, Path]:
    product = "txo" if kind == "options" else "tx"
    directory = root / product / f"trading_date={trading_date.isoformat()}"
    return directory / "transactions.parquet", directory / "transactions.receipt.json"


def _load_reusable_partition(
    parquet_path: Path,
    receipt_path: Path,
    *,
    source_sha256: str,
    kind: str,
) -> dict[str, object] | None:
    if not parquet_path.is_file() or not receipt_path.is_file():
        return None
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        receipt.get("parser_contract_version") != PARSER_CONTRACT_VERSION
        or receipt.get("source_sha256") != source_sha256
        or receipt.get("output_sha256") != _sha256_path(parquet_path)
        or receipt.get("kind") != kind
    ):
        return None
    return receipt


def _build_partition(
    raw_path: Path,
    *,
    kind: str,
    trading_date: date,
    source_sha256: str,
    root: Path,
) -> dict[str, object]:
    parquet_path, receipt_path = _partition_paths(root, kind, trading_date)
    reusable = _load_reusable_partition(
        parquet_path,
        receipt_path,
        source_sha256=source_sha256,
        kind=kind,
    )
    if reusable is not None:
        return {**reusable, "reused": True}

    frame = _parse_zip(
        raw_path,
        kind=kind,
        trading_date=trading_date,
        source_sha256=source_sha256,
    )
    if frame["trading_date"].unique().to_list() != [trading_date]:
        raise ValueError(f"{raw_path}: normalized trading_date mismatch")
    _atomic_write_parquet(frame, parquet_path)
    stats = _frame_stats(frame, kind=kind)
    receipt: dict[str, object] = {
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "kind": kind,
        "product": "TXO" if kind == "options" else "TX",
        "trading_date": trading_date.isoformat(),
        "source_path": str(raw_path),
        "source_sha256": source_sha256,
        "output_path": str(parquet_path),
        "output_bytes": parquet_path.stat().st_size,
        "output_sha256": _sha256_path(parquet_path),
        "price_contract": (
            "outright TX prices are positive; calendar-spread transaction prices "
            "are finite signed differentials and may be zero or negative"
            if kind == "futures"
            else "TXO transaction and strike prices are positive"
        ),
        **stats,
    }
    _atomic_write_text(
        receipt_path,
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
    )
    return {**receipt, "reused": False}


def _write_daily_report(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "trading_date",
        "tx_rows",
        "tx_matched_quantity",
        "tx_day_rows",
        "tx_night_rows",
        "txo_side_rows",
        "txo_second_price_buckets",
        "txo_matched_quantity",
        "txo_day_rows",
        "txo_night_rows",
        "futures_raw_bytes",
        "options_raw_bytes",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    _atomic_write_text(path, output.getvalue())


def _save_listing_page(root: Path, kind: str, payload: bytes) -> dict[str, object]:
    digest = _sha256_bytes(payload)
    path = root / "raw" / "listing_pages" / f"{kind}_{digest[:16]}.html"
    if not path.exists():
        _atomic_write_bytes(path, payload)
    return {"path": str(path), "bytes": len(payload), "sha256": digest}


def _acquire_lock(path: Path) -> BinaryIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(f"another TAIFEX tick downloader holds {path}") from exc
    return handle


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download matched recent TAIFEX TX and TXO trade files."
    )
    parser.add_argument(
        "--output-dir",
        default="data_tw_index_derivatives_ticks",
        help="Raw ZIP, filtered Parquet, receipt, and manifest root.",
    )
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--request-interval", type=float, default=0.25)
    args = parser.parse_args()
    if args.days < 1 or args.days > 30:
        parser.error("--days must be between 1 and the TAIFEX rolling limit of 30")
    if args.attempts < 1:
        parser.error("--attempts must be positive")
    if args.timeout <= 0.0:
        parser.error("--timeout must be positive")
    if args.request_interval < 0.0:
        parser.error("--request-interval must be non-negative")

    root = Path(args.output_dir).expanduser().resolve()
    request_interval = resolve_request_interval("taifex_public", args.request_interval)
    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=args.timeout,
            max_retries=max(0, args.attempts - 1),
            retry_base_seconds=0.5,
        ),
        limiter=SharedRateLimiter(request_interval, name="taifex_public"),
    )
    lock = _acquire_lock(root / "state" / "download.lock")
    try:
        futures_page, futures_headers = _fetch_bytes(
            FUTURES_LISTING_URL,
            attempts=args.attempts,
            timeout=args.timeout,
            transport=transport,
        )
        options_page, options_headers = _fetch_bytes(
            OPTIONS_LISTING_URL,
            attempts=args.attempts,
            timeout=args.timeout,
            transport=transport,
        )
        futures_urls = _extract_downloads(futures_page, pattern=FUTURES_URL_RE)
        options_urls = _extract_downloads(options_page, pattern=OPTIONS_URL_RE)
        selected_dates = _selected_common_dates(
            futures_urls, options_urls, count=args.days
        )

        page_receipts = {
            "futures": {
                "url": FUTURES_LISTING_URL,
                "last_modified": futures_headers.get("last-modified"),
                **_save_listing_page(root, "futures", futures_page),
            },
            "options": {
                "url": OPTIONS_LISTING_URL,
                "last_modified": options_headers.get("last-modified"),
                **_save_listing_page(root, "options", options_page),
            },
        }

        downloads: list[dict[str, object]] = []
        partitions: list[dict[str, object]] = []
        report_rows: list[dict[str, object]] = []
        for index, trade_date in enumerate(selected_dates, start=1):
            compact = trade_date.strftime("%Y_%m_%d")
            futures_path = root / "raw" / "futures" / f"Daily_{compact}.zip"
            options_path = root / "raw" / "options" / f"OptionsDaily_{compact}.zip"
            futures_download = _download_zip(
                futures_urls[trade_date],
                futures_path,
                expected_member=f"Daily_{compact}.csv",
                attempts=args.attempts,
                timeout=args.timeout,
                transport=transport,
            )
            options_download = _download_zip(
                options_urls[trade_date],
                options_path,
                expected_member=f"OptionsDaily_{compact}.csv",
                attempts=args.attempts,
                timeout=args.timeout,
                transport=transport,
            )
            downloads.extend(
                [
                    {
                        "kind": "futures",
                        "trading_date": trade_date.isoformat(),
                        **futures_download,
                    },
                    {
                        "kind": "options",
                        "trading_date": trade_date.isoformat(),
                        **options_download,
                    },
                ]
            )
            tx = _build_partition(
                futures_path,
                kind="futures",
                trading_date=trade_date,
                source_sha256=str(futures_download["sha256"]),
                root=root,
            )
            txo = _build_partition(
                options_path,
                kind="options",
                trading_date=trade_date,
                source_sha256=str(options_download["sha256"]),
                root=root,
            )
            partitions.extend([tx, txo])
            tx_sessions = dict(tx["sessions"])
            txo_sessions = dict(txo["sessions"])
            report_rows.append(
                {
                    "trading_date": trade_date.isoformat(),
                    "tx_rows": tx["rows"],
                    "tx_matched_quantity": tx["matched_quantity"],
                    "tx_day_rows": dict(tx_sessions.get("day", {})).get("rows", 0),
                    "tx_night_rows": dict(tx_sessions.get("night", {})).get("rows", 0),
                    "txo_side_rows": txo["rows"],
                    "txo_second_price_buckets": txo["second_price_buckets"],
                    "txo_matched_quantity": txo["matched_quantity"],
                    "txo_day_rows": dict(txo_sessions.get("day", {})).get("rows", 0),
                    "txo_night_rows": dict(txo_sessions.get("night", {})).get(
                        "rows", 0
                    ),
                    "futures_raw_bytes": futures_download["bytes"],
                    "options_raw_bytes": options_download["bytes"],
                }
            )
            print(
                f"[{index:02d}/{len(selected_dates):02d}] {trade_date} "
                f"TX rows={int(tx['rows']):,} TXO side_rows={int(txo['rows']):,}",
                flush=True,
            )
        _write_daily_report(root / "daily_report.csv", report_rows)
        partition_dates = {
            kind: sorted(
                str(item["trading_date"]) for item in partitions if item["kind"] == kind
            )
            for kind in ("futures", "options")
        }
        expected_dates = [value.isoformat() for value in selected_dates]
        if any(values != expected_dates for values in partition_dates.values()):
            raise RuntimeError("normalized partition dates do not match selected dates")
        manifest = {
            "dataset": "taifex_recent_tx_txo_trade_by_trade",
            "status": "complete",
            "parser_contract_version": PARSER_CONTRACT_VERSION,
            "generated_at": datetime.now(TAIPEI).isoformat(),
            "requested_days": args.days,
            "date_start": selected_dates[0].isoformat(),
            "date_end": selected_dates[-1].isoformat(),
            "trading_dates": expected_dates,
            "products": ["TX", "TXO"],
            "source_scope": (
                "TAIFEX rolling recent-trading-day files; raw ZIPs contain all "
                "products, normalized Parquet is filtered to TX/TXO"
            ),
            "timestamp_contract": (
                "event_ts is Asia/Taipei at official whole-second resolution; "
                "trading_date is the TAIFEX file date and night events may have "
                "the preceding calendar event_date"
            ),
            "quantity_contract": {
                "TX": "reported B+S quantity divided by 2 into matched_quantity",
                "TXO": (
                    "reported B-or-S side rows retained; each row divided by 2 "
                    "into additive matched_quantity_equivalent; every second-price "
                    "bucket verified to have an even reported total"
                ),
            },
            "price_contract": {
                "TX": (
                    "outright prices are positive; calendar-spread prices are "
                    "finite signed differentials and may be zero or negative"
                ),
                "TXO": "transaction and strike prices are positive",
            },
            "listing_page_dates": {
                "futures": sorted(value.isoformat() for value in futures_urls),
                "options": sorted(value.isoformat() for value in options_urls),
                "futures_only": sorted(
                    value.isoformat() for value in set(futures_urls) - set(options_urls)
                ),
                "options_only": sorted(
                    value.isoformat() for value in set(options_urls) - set(futures_urls)
                ),
            },
            "listing_pages": page_receipts,
            "raw_downloads": downloads,
            "partitions": partitions,
            "totals": {
                "raw_bytes": sum(int(item["bytes"]) for item in downloads),
                "TX_rows": sum(
                    int(item["rows"])
                    for item in partitions
                    if item["kind"] == "futures"
                ),
                "TX_matched_quantity": sum(
                    float(item["matched_quantity"])
                    for item in partitions
                    if item["kind"] == "futures"
                ),
                "TXO_side_rows": sum(
                    int(item["rows"])
                    for item in partitions
                    if item["kind"] == "options"
                ),
                "TXO_second_price_buckets": sum(
                    int(item["second_price_buckets"])
                    for item in partitions
                    if item["kind"] == "options"
                ),
                "TXO_matched_quantity": sum(
                    float(item["matched_quantity"])
                    for item in partitions
                    if item["kind"] == "options"
                ),
            },
        }
        _atomic_write_text(
            root / "manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        print(
            json.dumps(
                {
                    "status": manifest["status"],
                    "date_start": manifest["date_start"],
                    "date_end": manifest["date_end"],
                    **dict(manifest["totals"]),
                    "manifest": str(root / "manifest.json"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    finally:
        lock.close()


if __name__ == "__main__":
    sys.exit(main())
