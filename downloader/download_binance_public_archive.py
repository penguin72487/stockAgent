from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from io import BytesIO, StringIO
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
import time
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import polars as pl

try:
    from downloader.artifact_io import atomic_write_parquet
except ImportError:  # pragma: no cover - direct execution from downloader/
    from artifact_io import atomic_write_parquet

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    PersistentProgress,
    SharedRateLimiter,
    atomic_write_text,
    provider_rate_limit,
    retry_delay_seconds,
)


LIST_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
ARCHIVE_ENDPOINT = "https://data.binance.vision"
MARKET_PREFIXES = {
    "spot": "data/spot",
    "um": "data/futures/um",
    "cm": "data/futures/cm",
}
KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
)
OBJECT_NAME = re.compile(r"-(\d{4}-\d{2}(?:-\d{2})?)\.zip$")
DELIVERY_SUFFIX = re.compile(r"_(\d{6})$")
_STATE_DB_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class ArchiveObject:
    market: str
    symbol: str
    archive_granularity: str
    period: str
    partition: str
    key: str
    etag: str
    compressed_bytes: int
    archive_last_modified_utc: str

    @property
    def source_priority(self) -> int:
        return 2 if self.archive_granularity == "daily" else 1


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _atomic_write_parquet(frame: pl.DataFrame, target: Path) -> None:
    atomic_write_parquet(
        target,
        frame,
        compression="zstd",
        write_statistics=True,
    )


def _parse_listing(
    payload: bytes,
) -> tuple[list[dict[str, object]], list[str], str | None]:
    root = ET.fromstring(payload)
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"
    objects: list[dict[str, object]] = []
    for item in root.findall(f"{namespace}Contents"):
        key = item.findtext(f"{namespace}Key") or ""
        etag = (item.findtext(f"{namespace}ETag") or "").strip('"')
        size = int(item.findtext(f"{namespace}Size") or 0)
        modified = item.findtext(f"{namespace}LastModified") or ""
        objects.append(
            {"key": key, "etag": etag, "size": size, "last_modified": modified}
        )
    prefixes = [
        value
        for node in root.findall(f"{namespace}CommonPrefixes")
        if (value := node.findtext(f"{namespace}Prefix"))
    ]
    token = root.findtext(f"{namespace}NextContinuationToken")
    return objects, prefixes, token


class BinanceArchiveClient:
    def __init__(self, *, retries: int = 5, retry_base: float = 1.0) -> None:
        self.retries = max(0, int(retries))
        self.retry_base = max(0.1, float(retry_base))
        self.limiters = {
            "listing": SharedRateLimiter(
                provider_rate_limit("binance_public_listing").interval_seconds,
                name="binance-public-listing",
            ),
            "archive": SharedRateLimiter(
                provider_rate_limit("binance_public_archive").interval_seconds,
                name="binance-public-archive",
            ),
        }

    def _get(self, url: str, *, bucket: str, missing_ok: bool = False) -> bytes | None:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            self.limiters[bucket].wait()
            request = Request(
                url,
                headers={"Accept": "*/*", "User-Agent": "stockAgent/1"},
            )
            try:
                with urlopen(request, timeout=120) as response:
                    return response.read()
            except HTTPError as exc:
                if missing_ok and exc.code == 404:
                    return None
                last_error = exc
                if exc.code not in {408, 418, 429, 500, 502, 503, 504}:
                    break
                delay = retry_delay_seconds(
                    attempt,
                    base=self.retry_base,
                    cap=60,
                    retry_after=exc.headers.get("Retry-After"),
                )
                self.limiters[bucket].defer(delay)
            except (TimeoutError, URLError, OSError) as exc:
                last_error = exc
                delay = retry_delay_seconds(attempt, base=self.retry_base, cap=60)
            if attempt < self.retries:
                time.sleep(delay)
        raise RuntimeError(f"request failed url={url}: {last_error}")

    def list_objects(
        self,
        prefix: str,
        *,
        delimiter: str | None = None,
        start_after: str | None = None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        objects: list[dict[str, object]] = []
        prefixes: list[str] = []
        token: str | None = None
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if delimiter:
                params["delimiter"] = delimiter
            if start_after and token is None:
                params["start-after"] = start_after
            if token:
                params["continuation-token"] = token
            payload = self._get(
                f"{LIST_ENDPOINT}?{urlencode(params)}", bucket="listing"
            )
            assert payload is not None
            page_objects, page_prefixes, token = _parse_listing(payload)
            objects.extend(page_objects)
            prefixes.extend(page_prefixes)
            if not token:
                break
        return objects, prefixes

    def archive_bytes(self, key: str, *, missing_ok: bool = False) -> bytes | None:
        return self._get(
            f"{ARCHIVE_ENDPOINT}/{quote(key, safe='/')}",
            bucket="archive",
            missing_ok=missing_ok,
        )


def _symbol_from_prefix(prefix: str) -> str:
    parts = prefix.rstrip("/").split("/")
    return parts[-1] if parts else ""


def _is_requested_symbol(market: str, symbol: str) -> bool:
    if market == "spot":
        return bool(symbol)
    return bool(DELIVERY_SUFFIX.search(symbol)) and not symbol.endswith("_PERP")


def _archive_object(
    market: str,
    symbol: str,
    granularity: str,
    item: dict[str, object],
) -> ArchiveObject | None:
    key = str(item["key"])
    if not key.endswith(".zip"):
        return None
    match = OBJECT_NAME.search(key)
    if match is None:
        return None
    period = match.group(1)
    return ArchiveObject(
        market=market,
        symbol=symbol,
        archive_granularity=granularity,
        period=period,
        partition=period[:7],
        key=key,
        etag=str(item["etag"]),
        compressed_bytes=int(item["size"]),
        archive_last_modified_utc=str(item["last_modified"]),
    )


def _month_start(month: str) -> date:
    return datetime.strptime(month, "%Y-%m").date()


def _tail_start(latest_month: str | None, tail_months: int) -> date:
    today = date.today()
    floor = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    for _ in range(max(0, int(tail_months) - 1)):
        floor = (floor - timedelta(days=1)).replace(day=1)
    if latest_month:
        # Include the latest monthly partition again so any daily revision wins.
        floor = min(floor, _month_start(latest_month))
    return floor


def discover_plan(
    client: BinanceArchiveClient,
    *,
    markets: Iterable[str],
    selected_symbols: set[str] | None,
    workers: int,
    tail_months: int,
    correction_dates: tuple[str, ...],
    repair_months: set[tuple[str, str, str]] | None = None,
    progress: PersistentProgress | None = None,
    progress_path: Path | None = None,
) -> tuple[list[ArchiveObject], list[dict[str, object]]]:
    symbols_by_market: dict[str, set[str]] = {}
    for market in markets:
        base = MARKET_PREFIXES[market]
        prefixes: set[str] = set()
        for granularity in ("monthly", "daily"):
            _, found = client.list_objects(
                f"{base}/{granularity}/klines/", delimiter="/"
            )
            prefixes.update(found)
        symbols = {
            symbol
            for value in prefixes
            if _is_requested_symbol(market, (symbol := _symbol_from_prefix(value)))
            and (selected_symbols is None or symbol in selected_symbols)
        }
        symbols_by_market[market] = symbols

    tasks = [
        (market, symbol)
        for market, symbols in symbols_by_market.items()
        for symbol in sorted(symbols)
    ]
    if progress is None and progress_path is not None:
        progress = PersistentProgress(
            progress_path,
            label="Binance public archive discovery",
            total=len(tasks),
            unit="instrument",
            basis="exact venue-symbol count after official S3 prefix discovery",
        )
    discovered: list[ArchiveObject] = []
    lifecycle: list[dict[str, object]] = []
    lock = threading.Lock()

    def one_symbol(
        task: tuple[str, str],
    ) -> tuple[list[ArchiveObject], dict[str, object]]:
        market, symbol = task
        base = MARKET_PREFIXES[market]
        monthly_prefix = f"{base}/monthly/klines/{symbol}/1m/"
        monthly_raw, _ = client.list_objects(monthly_prefix)
        monthly = [
            value
            for item in monthly_raw
            if (value := _archive_object(market, symbol, "monthly", item)) is not None
        ]
        latest_month = max((item.period for item in monthly), default=None)
        tail_start = _tail_start(latest_month, tail_months)
        daily_prefix = f"{base}/daily/klines/{symbol}/1m/"
        start_after = f"{daily_prefix}{symbol}-1m-{tail_start.isoformat()}"
        daily_raw, _ = client.list_objects(daily_prefix, start_after=start_after)
        daily = [
            value
            for item in daily_raw
            if (value := _archive_object(market, symbol, "daily", item)) is not None
        ]
        monthly_periods = {item.period for item in monthly}
        for correction_date in correction_dates:
            if correction_date[:7] not in monthly_periods:
                continue
            exact_prefix = f"{daily_prefix}{symbol}-1m-{correction_date}"
            correction_raw, _ = client.list_objects(exact_prefix)
            daily.extend(
                value
                for item in correction_raw
                if (value := _archive_object(market, symbol, "daily", item)) is not None
            )
        for repair_market, repair_symbol, repair_month in repair_months or set():
            if (repair_market, repair_symbol) != (market, symbol):
                continue
            repair_prefix = f"{daily_prefix}{symbol}-1m-{repair_month}-"
            repair_raw, _ = client.list_objects(repair_prefix)
            daily.extend(
                value
                for item in repair_raw
                if (value := _archive_object(market, symbol, "daily", item)) is not None
            )
        values = {item.key: item for item in [*monthly, *daily]}
        periods = sorted(item.period for item in values.values())
        latest_period = periods[-1] if periods else None
        delivery_date = None
        match = DELIVERY_SUFFIX.search(symbol)
        if match:
            try:
                delivery_date = datetime.strptime(match.group(1), "%y%m%d").date()
            except ValueError:
                delivery_date = None
        state = "active_or_recent"
        if delivery_date is not None and delivery_date < date.today():
            state = "expired"
        elif latest_period:
            latest_day = (
                datetime.strptime(latest_period, "%Y-%m-%d").date()
                if len(latest_period) == 10
                else _month_start(latest_period)
            )
            if latest_day < date.today() - timedelta(days=62):
                state = "archive_inactive_candidate"
        return list(values.values()), {
            "market": market,
            "symbol": symbol,
            "instrument_type": "spot" if market == "spot" else "dated_future",
            "delivery_date": delivery_date.isoformat() if delivery_date else None,
            "lifecycle_state": state,
            "first_archive_period": periods[0] if periods else None,
            "last_archive_period": latest_period,
            "archive_object_count": len(values),
            "archive_compressed_bytes": sum(
                item.compressed_bytes for item in values.values()
            ),
            "observed_at_utc": _utc_now().isoformat(),
            "point_in_time_state": "retrieval_vintage; lifecycle inferred from archive coverage",
        }

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = {pool.submit(one_symbol, task): task for task in tasks}
        for future in as_completed(futures):
            objects, row = future.result()
            with lock:
                discovered.extend(objects)
                lifecycle.append(row)
            if progress is not None:
                progress.update("discover", "planned")
    if progress is not None:
        progress.finish()
    return sorted(discovered, key=lambda item: item.key), sorted(
        lifecycle, key=lambda item: (str(item["market"]), str(item["symbol"]))
    )


def _open_state(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _STATE_DB_LOCK:
        with sqlite3.connect(path, timeout=60) as connection:
            connection.execute("PRAGMA busy_timeout = 60000")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_objects (
                    object_key TEXT PRIMARY KEY,
                    etag TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sha256 TEXT,
                    row_count INTEGER,
                    output_path TEXT,
                    downloaded_at_utc TEXT,
                    error TEXT
                )
                """
            )


def _completed_etags(path: Path) -> dict[str, str]:
    _open_state(path)
    with sqlite3.connect(path) as connection:
        return {
            str(key): str(etag)
            for key, etag in connection.execute(
                """SELECT object_key, etag FROM archive_objects
                   WHERE status IN (
                       'complete',
                       'quarantined_repair_required',
                       'quarantined_source_invalid'
                   )"""
            )
        }


def _promote_monthly_repairs(path: Path) -> set[tuple[str, str, str]]:
    """Quarantine semantically invalid monthly objects and request daily rebuilds.

    A checksum only proves transport integrity. It does not prove that minute
    timestamps or OHLCV semantics are valid, so a failed official monthly object
    remains auditable but must not be coerced into the canonical table.
    """
    _open_state(path)
    repairs: set[tuple[str, str, str]] = set()
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """SELECT object_key, error FROM archive_objects
               WHERE status IN ('failed', 'quarantined_repair_required')"""
        ).fetchall()
        for key_value, error_value in rows:
            key = str(key_value)
            error = str(error_value or "")
            if "/monthly/klines/" not in key or "invalid OHLCV rows" not in error:
                continue
            parts = key.split("/")
            market = "spot" if parts[1:3] == ["spot", "monthly"] else parts[2]
            try:
                symbol = parts[parts.index("klines") + 1]
            except (ValueError, IndexError):
                continue
            match = OBJECT_NAME.search(key)
            if match is None:
                continue
            repairs.add((market, symbol, match.group(1)[:7]))
            connection.execute(
                "UPDATE archive_objects SET status = 'quarantined_repair_required' WHERE object_key = ?",
                (key,),
            )
    return repairs


def _record_state(
    path: Path,
    item: ArchiveObject,
    *,
    status: str,
    digest: str | None = None,
    row_count: int | None = None,
    output_path: str | None = None,
    error: str | None = None,
) -> None:
    # SQLite only has one writer. Archive downloads are parallel, but state
    # publication is tiny and must be serialized so a successful multi-hour
    # download cannot fail at the final receipt write with ``database is locked``.
    with _STATE_DB_LOCK:
        _open_state(path)
        with sqlite3.connect(path, timeout=60) as connection:
            connection.execute("PRAGMA busy_timeout = 60000")
            connection.execute(
                """
                INSERT INTO archive_objects (
                    object_key, etag, status, sha256, row_count, output_path,
                    downloaded_at_utc, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_key) DO UPDATE SET
                    etag=excluded.etag, status=excluded.status, sha256=excluded.sha256,
                    row_count=excluded.row_count, output_path=excluded.output_path,
                    downloaded_at_utc=excluded.downloaded_at_utc, error=excluded.error
                """,
                (
                    item.key,
                    item.etag,
                    status,
                    digest,
                    row_count,
                    output_path,
                    _utc_now().isoformat(),
                    error,
                ),
            )


def _checksum_from_payload(payload: bytes, filename: str) -> str:
    fields = payload.decode("utf-8-sig").strip().split()
    if not fields or not re.fullmatch(r"[0-9a-fA-F]{64}", fields[0]):
        raise ValueError(f"invalid SHA-256 checksum for {filename}")
    if len(fields) > 1 and Path(fields[-1].lstrip("* ")).name != filename:
        raise ValueError(f"checksum filename mismatch for {filename}: {fields[-1]}")
    return fields[0].lower()


def _timestamp_ms(value: str) -> tuple[int, str]:
    number = int(value)
    if number >= 1_000_000_000_000_000:
        return number // 1000, "microseconds"
    if number >= 1_000_000_000_000:
        return number, "milliseconds"
    raise ValueError(f"unsupported timestamp magnitude: {number}")


def _parse_kline_zip(payload: bytes, item: ArchiveObject) -> pl.DataFrame:
    with ZipFile(BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected one CSV in {item.key}, found {members}")
        text = archive.read(members[0]).decode("utf-8-sig")
    reader = csv.reader(StringIO(text))
    rows = list(reader)
    if rows and rows[0] and not rows[0][0].lstrip("-").isdigit():
        rows = rows[1:]
    normalized_by_open: dict[int, tuple[object, ...]] = {}
    exact_duplicate_rows = 0
    units: set[str] = set()
    for row_number, row in enumerate(rows, start=1):
        if len(row) != len(KLINE_COLUMNS):
            raise ValueError(
                f"{item.key} row {row_number}: expected 12 fields, got {len(row)}"
            )
        open_ms, open_unit = _timestamp_ms(row[0])
        close_ms, close_unit = _timestamp_ms(row[6])
        units.update((open_unit, close_unit))
        normalized = (
            open_ms,
            float(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            float(row[5]),
            close_ms,
            float(row[7]),
            int(row[8]),
            float(row[9]),
            float(row[10]),
            row[11],
        )
        previous = normalized_by_open.get(open_ms)
        if previous is not None:
            if previous != normalized:
                raise ValueError(
                    f"{item.key}: conflicting duplicate open timestamp {open_ms}"
                )
            exact_duplicate_rows += 1
            continue
        normalized_by_open[open_ms] = normalized
    frame = pl.DataFrame(
        list(normalized_by_open.values()),
        schema=KLINE_COLUMNS,
        orient="row",
    )
    if frame.is_empty():
        raise ValueError(f"empty archive: {item.key}")
    frame = frame.with_columns(
        pl.lit(item.market).alias("market"),
        pl.lit(item.symbol).alias("symbol"),
        pl.lit("1m").alias("interval"),
        pl.lit(item.archive_granularity).alias("archive_granularity"),
        pl.lit(item.key).alias("archive_key"),
        pl.lit(item.etag).alias("archive_etag"),
        pl.lit(item.archive_last_modified_utc).alias("available_at_utc"),
        pl.lit(item.source_priority).cast(pl.Int8).alias("source_priority"),
        pl.lit(exact_duplicate_rows)
        .cast(pl.Int32)
        .alias("source_exact_duplicate_rows_removed"),
        pl.lit("mixed" if len(units) > 1 else next(iter(units))).alias(
            "source_timestamp_unit"
        ),
    )
    invalid = frame.filter(
        (pl.col("open_time") % 60_000 != 0)
        | (pl.col("close_time") < pl.col("open_time"))
        | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | (pl.col("volume") < 0)
        | (pl.col("quote_volume") < 0)
        | (pl.col("trade_count") < 0)
    )
    if invalid.height:
        raise ValueError(f"{item.key}: {invalid.height} invalid OHLCV rows")
    return frame.sort("open_time")


def _canonical_merge(
    existing: pl.DataFrame | None, incoming: list[pl.DataFrame]
) -> pl.DataFrame:
    frames = (
        [existing] if existing is not None and not existing.is_empty() else []
    ) + incoming
    if not frames:
        return pl.DataFrame()
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .sort(["open_time", "source_priority", "available_at_utc", "archive_key"])
        .unique(
            subset=["market", "symbol", "open_time"], keep="last", maintain_order=True
        )
        .sort("open_time")
    )


def _download_one(
    client: BinanceArchiveClient, item: ArchiveObject
) -> tuple[pl.DataFrame, str]:
    checksum_payload = client.archive_bytes(f"{item.key}.CHECKSUM")
    if checksum_payload is None:
        raise RuntimeError(f"missing checksum: {item.key}.CHECKSUM")
    expected = _checksum_from_payload(checksum_payload, Path(item.key).name)
    payload = client.archive_bytes(item.key)
    if payload is None:
        raise RuntimeError(f"missing archive: {item.key}")
    actual = sha256(payload).hexdigest()
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch for {item.key}: {actual} != {expected}")
    return _parse_kline_zip(payload, item), actual


def _capacity_receipt(
    root: Path,
    pending: list[ArchiveObject],
    *,
    reserve_gib: float,
    max_download_bytes: int,
) -> dict[str, object]:
    usage = shutil.disk_usage(root)
    compressed = sum(item.compressed_bytes for item in pending)
    estimated_peak = int(compressed * 2.5)
    reserve = max(int(reserve_gib * 1024**3), int(usage.total * 0.10))
    usable = max(0, usage.free - reserve)
    reasons: list[str] = []
    if estimated_peak > usable:
        reasons.append("estimated_peak_bytes_exceeds_free_space_after_reserve")
    if max_download_bytes > 0 and compressed > max_download_bytes:
        reasons.append("compressed_bytes_exceeds_operator_ceiling")
    return {
        "generated_at_utc": _utc_now().isoformat(),
        "pending_objects": len(pending),
        "pending_compressed_bytes": compressed,
        "estimated_peak_new_bytes": estimated_peak,
        "filesystem_total_bytes": usage.total,
        "filesystem_free_bytes": usage.free,
        "required_reserve_bytes": reserve,
        "usable_new_bytes": usable,
        "accepted": not reasons,
        "reasons": reasons,
        "estimate_contract": "compressed archive bytes * 2.5 for parquet and atomic-publish headroom; retain max(configured reserve, 10% filesystem)",
    }


def _download_states(
    counts: dict[str, int], durable_counts: dict[str, int]
) -> tuple[str, str]:
    """Separate dataset completeness from the outcome of the current cycle.

    A checksum-valid official object with invalid OHLCV semantics remains
    quarantined and keeps the dataset partial.  Once that durable quarantine is
    recorded, however, a later cycle that encounters no new failure has
    completed its maintenance work successfully.  Conflating those states
    leaves systemd permanently failed even though the source gap is already
    truthfully exposed by the durable receipt.
    """
    cycle_failed = any(
        counts[name]
        for name in (
            "failed",
            "quarantined_repair_required",
            "quarantined_source_invalid",
        )
    )
    dataset_partial = bool(
        counts["failed"]
        or counts["quarantined_repair_required"]
        or durable_counts.get("quarantined_source_invalid", 0)
    )
    return (
        "partial" if dataset_partial else "complete",
        "failed" if cycle_failed else "complete",
    )


def execute_download(
    client: BinanceArchiveClient,
    *,
    root: Path,
    objects: list[ArchiveObject],
    workers: int,
    state_path: Path,
) -> dict[str, object]:
    completed = _completed_etags(state_path)
    pending = [item for item in objects if completed.get(item.key) != item.etag]
    groups: dict[tuple[str, str, str], list[ArchiveObject]] = defaultdict(list)
    for item in pending:
        groups[(item.market, item.symbol, item.partition)].append(item)
    progress = PersistentProgress(
        root / "progress.json",
        label="Binance public archive 1m",
        total=len(pending),
        unit="archive_object",
        basis="official S3 plan; checksum-verified; daily archives override monthly archives",
    )
    counts: dict[str, int] = defaultdict(int)
    error_rows: list[dict[str, str]] = []
    result_lock = threading.Lock()

    def one_partition(group: tuple[str, str, str]) -> None:
        market, symbol, partition = group
        target = root / "partitions" / market / symbol / f"{partition}.parquet"
        existing = pl.read_parquet(target) if target.is_file() else None
        frames: list[pl.DataFrame] = []
        successes: list[tuple[ArchiveObject, str, int]] = []
        for item in sorted(groups[group], key=lambda value: value.source_priority):
            try:
                frame, digest = _download_one(client, item)
                frames.append(frame)
                successes.append((item, digest, frame.height))
                progress.update("download_and_validate", "downloaded")
            except Exception as exc:
                error = str(exc)[:2000]
                semantic_failure = (
                    "invalid OHLCV rows" in error
                    or "conflicting duplicate open timestamp" in error
                )
                if semantic_failure and item.archive_granularity == "monthly":
                    object_status = "quarantined_repair_required"
                elif semantic_failure:
                    object_status = "quarantined_source_invalid"
                else:
                    object_status = "failed"
                _record_state(
                    state_path,
                    item,
                    status=object_status,
                    error=error,
                )
                with result_lock:
                    counts[object_status] += 1
                    error_rows.append(
                        {
                            "archive_key": item.key,
                            "status": object_status,
                            "error": str(exc),
                        }
                    )
                progress.update("download_and_validate", object_status)
        if not frames:
            return
        merged = _canonical_merge(existing, frames)
        _atomic_write_parquet(merged, target)
        for item, digest, rows in successes:
            _record_state(
                state_path,
                item,
                status="complete",
                digest=digest,
                row_count=rows,
                output_path=str(target.relative_to(root)),
            )
        with result_lock:
            counts["complete"] += len(successes)

    try:
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            futures = {pool.submit(one_partition, group): group for group in groups}
            for future in as_completed(futures):
                future.result()
    except Exception:
        progress.finish(failed=True)
        raise
    progress.finish(failed=bool(counts["failed"]))
    if error_rows:
        _atomic_write_parquet(
            pl.DataFrame(error_rows), root / "download_errors.parquet"
        )
    with sqlite3.connect(state_path) as connection:
        durable_counts = {
            str(status): int(count)
            for status, count in connection.execute(
                "SELECT status, COUNT(*) FROM archive_objects GROUP BY status"
            )
        }
    source_invalid_objects = durable_counts.get("quarantined_source_invalid", 0)
    new_repair_objects = counts["quarantined_repair_required"]
    dataset_state, cycle_state = _download_states(counts, durable_counts)
    return {
        "generated_at_utc": _utc_now().isoformat(),
        "state": dataset_state,
        "cycle_state": cycle_state,
        "planned_objects": len(objects),
        "already_complete_objects": len(objects) - len(pending),
        "attempted_objects": len(pending),
        "completed_objects": counts["complete"],
        "failed_objects": counts["failed"],
        "source_invalid_objects": source_invalid_objects,
        "new_quarantined_repair_objects": new_repair_objects,
        "new_quarantined_source_invalid_objects": counts["quarantined_source_invalid"],
        "durable_object_status_counts": durable_counts,
        "quarantined_monthly_objects": durable_counts.get(
            "quarantined_repair_required", 0
        ),
        "partition_root": "partitions",
        "dedup_key": ["market", "symbol", "open_time"],
        "precedence": "daily > monthly; then later S3 LastModified; then archive key",
        "point_in_time": "available_at_utc is S3 object LastModified; retrieval-vintage metadata retained",
    }


def _plan_frame(
    objects: list[ArchiveObject], completed: dict[str, str]
) -> pl.DataFrame:
    rows = []
    for item in objects:
        row = asdict(item)
        row["source_priority"] = item.source_priority
        row["state"] = "complete" if completed.get(item.key) == item.etag else "pending"
        rows.append(row)
    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            schema={
                "market": pl.String,
                "symbol": pl.String,
                "archive_granularity": pl.String,
                "period": pl.String,
                "partition": pl.String,
                "key": pl.String,
                "etag": pl.String,
                "compressed_bytes": pl.Int64,
                "archive_last_modified_utc": pl.String,
                "source_priority": pl.Int8,
                "state": pl.String,
            }
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan and checksum-verify Binance spot/dated-futures 1m archives."
    )
    parser.add_argument(
        "--output-root", type=Path, default=Path("data_binance_archive")
    )
    parser.add_argument("--mode", choices=("plan", "download"), default="plan")
    parser.add_argument(
        "--markets",
        nargs="+",
        choices=tuple(MARKET_PREFIXES),
        default=list(MARKET_PREFIXES),
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional exact symbol allowlist across selected markets.",
    )
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--tail-months", type=int, default=2)
    parser.add_argument(
        "--correction-dates",
        default="2020-12-21,2021-09-29",
        help="Known monthly-vs-daily conflict dates to overlay when the symbol existed.",
    )
    parser.add_argument("--reserve-gib", type=float, default=100.0)
    parser.add_argument("--max-download-bytes", type=int, default=0)
    parser.add_argument("--retries", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    state_path = root / "state/archive_objects.sqlite3"
    repair_months = _promote_monthly_repairs(state_path)
    completed = _completed_etags(state_path)
    selected_symbols = set(args.symbols) if args.symbols else None
    corrections = tuple(
        value.strip() for value in args.correction_dates.split(",") if value.strip()
    )
    client = BinanceArchiveClient(retries=args.retries)
    # The discovery total is initially unknown; the persisted object-level ETA begins
    # after this exact plan is published.
    objects, lifecycle = discover_plan(
        client,
        markets=args.markets,
        selected_symbols=selected_symbols,
        workers=args.workers,
        tail_months=args.tail_months,
        correction_dates=corrections,
        repair_months=repair_months,
        progress_path=root / "progress.json",
    )
    plan = _plan_frame(objects, completed)
    _atomic_write_parquet(plan, root / "archive_plan.parquet")
    _atomic_write_parquet(
        pl.DataFrame(lifecycle), root / "instrument_lifecycle.parquet"
    )
    pending = [item for item in objects if completed.get(item.key) != item.etag]
    capacity = _capacity_receipt(
        root,
        pending,
        reserve_gib=args.reserve_gib,
        max_download_bytes=args.max_download_bytes,
    )
    atomic_write_text(
        root / "capacity_receipt.json",
        json.dumps(capacity, ensure_ascii=False, indent=2) + "\n",
    )
    plan_summary = {
        "generated_at_utc": _utc_now().isoformat(),
        "state": "planned",
        "markets": args.markets,
        "instrument_count": len(lifecycle),
        "archive_object_count": len(objects),
        "pending_object_count": len(pending),
        "compressed_bytes": sum(item.compressed_bytes for item in objects),
        "pending_compressed_bytes": sum(item.compressed_bytes for item in pending),
        "daily_override_dates": list(corrections),
        "quarantined_monthly_repair_partitions": [
            {"market": market, "symbol": symbol, "month": month}
            for market, symbol, month in sorted(repair_months)
        ],
        "tail_months": args.tail_months,
        "capacity_accepted": capacity["accepted"],
        "rate_limit_contract": {
            "listing": asdict(provider_rate_limit("binance_public_listing")),
            "archive": asdict(provider_rate_limit("binance_public_archive")),
            "parallelism": "independent host-global buckets; FIFO within each host",
        },
    }
    atomic_write_text(
        root / "plan_summary.json",
        json.dumps(plan_summary, ensure_ascii=False, indent=2) + "\n",
    )
    print(json.dumps(plan_summary, ensure_ascii=False, indent=2), flush=True)
    if args.mode == "plan":
        return 0
    if not capacity["accepted"]:
        raise SystemExit(
            "capacity gate rejected download: " + ", ".join(capacity["reasons"])
        )
    summary = execute_download(
        client,
        root=root,
        objects=objects,
        workers=args.workers,
        state_path=state_path,
    )
    atomic_write_text(
        root / "download_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if summary["cycle_state"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
