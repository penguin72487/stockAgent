from __future__ import annotations

import argparse
from http.client import IncompleteRead, RemoteDisconnected
import json
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (
    PersistentProgress,
    SharedRateLimiter,
    atomic_write_text,
    describe_rate_limit,
    parquet_temporal_metadata,
    resolve_end_date,
    resolve_incremental_reconcile_start_ms,
    resolve_request_interval,
    retry_delay_seconds,
    run_parallel_tasks,
)
from ohlcv_hot_tail import (
    hot_tail_path,
    read_logical_parquet,
    remove_hot_tail,
)


BASE_URL = "https://api.bybit.com"
INSTRUMENTS_ENDPOINT = "/v5/market/instruments-info"
KLINE_ENDPOINT = "/v5/market/kline"
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
KLINE_INTERVAL = "1"
KLINE_INTERVAL_LABEL = "1m"
CANDLE_INTERVAL_MS = 60 * 1000
BYBIT_MAX_KLINE_LIMIT = "1000"
BYBIT_MAX_CANDLES_PER_REQUEST = int(BYBIT_MAX_KLINE_LIMIT)
BYBIT_WINDOW_SPAN_MS = (BYBIT_MAX_CANDLES_PER_REQUEST - 1) * CANDLE_INTERVAL_MS


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _read_parquet_row_count(path: Path) -> int:
    return int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)


def _stored_parquet_inventory(
    output_dir: Path,
    *,
    current_codes: set[str],
) -> dict[str, Any]:
    """Count current and retained historical files without deleting delistings."""

    stored_rows = 0
    retained_rows = 0
    retained_symbols: list[str] = []
    paths = sorted(output_dir.glob("*_features.parquet"))
    for path in paths:
        code = path.name.removesuffix("_features.parquet")
        rows = _load_logical_existing_candle_info(
            path, require_contiguous=False
        ).rows
        stored_rows += rows
        if code not in current_codes:
            retained_symbols.append(code)
            retained_rows += rows
    return {
        "stored_symbol_count": len(paths),
        "stored_row_count": stored_rows,
        "retained_historical_symbol_count": len(retained_symbols),
        "retained_historical_row_count": retained_rows,
        "retained_historical_symbols": retained_symbols,
    }


def _read_date_column(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path, columns=["date"], memory_map=True))


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), tmp_path, compression="snappy", write_statistics=True
        )
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    bybit_symbol: str
    category: str
    base_coin: str | None
    quote_coin: str | None
    settle_coin: str | None
    contract_type: str | None
    status: str | None
    launch_time: str | None
    symbol_type: str | None
    funding_interval_minutes: int | None
    is_pre_listing: bool
    price_tick_size: float | None
    minimum_order_quantity: float | None
    quantity_step: float | None
    minimum_notional_value: float | None
    maximum_order_quantity: float | None
    maximum_market_order_quantity: float | None
    maximum_leverage: float | None
    unified_margin_trade: bool


@dataclass(slots=True)
class DownloadResult:
    asset_class: str
    code: str
    bybit_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


@dataclass(slots=True)
class ExistingCandleInfo:
    rows: int
    latest_ms: int | None
    interval_ok: bool
    earliest_ms: int | None = None
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bybit perpetual futures one-minute bars to parquet files."
    )
    parser.add_argument("--output-dir", default="data_bybit/1m", help="Output folder.")
    parser.add_argument(
        "--mode",
        choices=["incremental", "daily-update", "full"],
        default="incremental",
        help="incremental: reconcile missing head/tail coverage; daily-update: deprecated alias; full: reconcile requested coverage; --refresh forces rebuild.",
    )
    parser.add_argument(
        "--start-date", default="2019-01-01", help="Inclusive start date YYYY-MM-DD"
    )
    parser.add_argument(
        "--end-date", default="today", help="Inclusive end date YYYY-MM-DD or 'today'"
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        default=["linear", "inverse"],
        help="Bybit categories to fetch: linear inverse (default: both)",
    )
    parser.add_argument(
        "--workers", type=int, default=16, help="Parallel symbol workers"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Optional symbol limit for quick tests"
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional exact Bybit symbols; useful for product-specific updates.",
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Re-download even if parquet exists"
    )
    parser.add_argument(
        "--tail-only",
        action="store_true",
        help=(
            "Fetch only the reconciliation tail. Missing historical heads are "
            "left for a separate full/backfill repair."
        ),
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help="Global minimum seconds between API requests. Default uses official Bybit public REST IP profile.",
    )
    parser.add_argument(
        "--max-retries", type=int, default=8, help="Max retries per HTTP request"
    )
    parser.add_argument(
        "--retry-base",
        type=float,
        default=0.6,
        help="Base seconds for exponential backoff",
    )
    return parser.parse_args()


def _resolve_categories(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        text = value.strip().lower()
        if text == "all":
            normalized.extend(["linear", "inverse"])
            continue
        if text in {"linear", "inverse"}:
            normalized.append(text)

    unique = sorted(set(normalized))
    if not unique:
        raise ValueError("No valid categories. Use: linear inverse or all.")
    return unique


def _date_to_ms(date_str: str, *, end_of_day: bool) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(dt.timestamp() * 1000)


def _normalize_date_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    normalized = frame.with_columns(
        pl.col("date")
        .str.to_datetime(strict=False)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("date")
        if frame.schema.get("date") == pl.String
        else pl.col("date")
        .cast(pl.Datetime("us"), strict=False)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("date")
    )
    return normalized.drop_nulls("date").sort("date")


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _latest_closed_candle_start_ms(now: datetime | None = None) -> int:
    current = (
        now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    )
    current_ms = int(current.timestamp() * 1000)
    return (current_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS - CANDLE_INTERVAL_MS


def _frames_equal(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    common = [column for column in left.columns if column in right.columns]
    if not common or left.height != right.height:
        return False
    return left.select(common).equals(right.select(common))


def _merge_existing_with_fresh(
    existing_df: pl.DataFrame, fresh_df: pl.DataFrame, effective_start_ms: int
) -> tuple[pl.DataFrame, bool]:
    existing = _normalize_date_frame(existing_df)
    cutoff = _ms_to_date_string(effective_start_ms)
    kept_existing = (
        existing.filter(pl.col("date") < cutoff)
        if "date" in existing.columns
        else existing
    )
    combined = (
        pl.concat([kept_existing, fresh_df], how="diagonal_relaxed")
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
        .sort("date")
    )
    return combined, not _frames_equal(existing, combined)


def _frame_matches_1m_interval(frame: pl.DataFrame) -> bool:
    if frame.is_empty() or "date" not in frame.columns:
        return True

    parsed = (
        _normalize_date_frame(frame)
        .select(pl.col("date").str.to_datetime(strict=False).alias("date"))
        .drop_nulls("date")
    )
    if parsed.height == 0:
        return True
    if parsed.select(
        (
            (pl.col("date").dt.second() != 0)
            | (pl.col("date").dt.microsecond() != 0)
        ).any()
    ).item():
        return False
    if parsed.height < 2:
        return True

    deltas = (
        parsed.sort("date")
        .select(pl.col("date").diff().dt.total_seconds().alias("delta"))
        .drop_nulls("delta")
    )
    if deltas.is_empty():
        return True
    return bool(
        deltas.select(
            pl.col("delta").eq(CANDLE_INTERVAL_MS // 1000).all()
        ).item()
    )


def _latest_ms_from_date_frame(frame: pl.DataFrame) -> int | None:
    if frame.is_empty() or "date" not in frame.columns:
        return None
    parsed = _normalize_date_frame(frame)
    if parsed.is_empty():
        return None
    latest = parsed.select(pl.col("date").str.to_datetime(strict=False).max()).item()
    if latest is None:
        return None
    return int(latest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _earliest_ms_from_date_frame(frame: pl.DataFrame) -> int | None:
    if frame.is_empty() or "date" not in frame.columns:
        return None
    parsed = _normalize_date_frame(frame)
    if parsed.is_empty():
        return None
    earliest = parsed.select(pl.col("date").str.to_datetime(strict=False).min()).item()
    if earliest is None:
        return None
    return int(earliest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _load_existing_candle_info(
    path: Path,
    *,
    require_contiguous: bool = True,
) -> ExistingCandleInfo:
    try:
        parquet = pq.ParquetFile(path, memory_map=True)
        row_count, earliest_ms, latest_ms, interval_likely_ok = (
            parquet_temporal_metadata(
                parquet,
                column="date",
                expected_interval_ms=CANDLE_INTERVAL_MS,
            )
        )
        if "date" not in set(parquet.schema_arrow.names):
            return ExistingCandleInfo(
                rows=row_count,
                latest_ms=None,
                interval_ok=False,
                error="missing date column",
            )
        if (
            earliest_ms is not None
            and latest_ms is not None
            and interval_likely_ok is True
            and latest_ms >= earliest_ms
            and (latest_ms - earliest_ms) % CANDLE_INTERVAL_MS == 0
            and (
                not require_contiguous
                or row_count
                == (latest_ms - earliest_ms) // CANDLE_INTERVAL_MS + 1
            )
        ):
            return ExistingCandleInfo(
                rows=row_count,
                latest_ms=latest_ms,
                interval_ok=True,
                earliest_ms=earliest_ms,
            )
        date_frame = _read_date_column(path)
        return ExistingCandleInfo(
            rows=row_count,
            latest_ms=_latest_ms_from_date_frame(date_frame),
            interval_ok=_frame_matches_1m_interval(date_frame),
            earliest_ms=_earliest_ms_from_date_frame(date_frame),
        )
    except Exception as exc:
        return ExistingCandleInfo(
            rows=0,
            latest_ms=None,
            interval_ok=False,
            error=f"{type(exc).__name__}: {exc}",
        )


def _load_logical_existing_candle_info(
    path: Path,
    *,
    require_contiguous: bool = True,
) -> ExistingCandleInfo:
    base = _load_existing_candle_info(path, require_contiguous=require_contiguous)
    tail_path = hot_tail_path(path)
    if not tail_path.is_file() or base.error is not None:
        return base
    tail = _load_existing_candle_info(tail_path)
    if tail.error is not None:
        return ExistingCandleInfo(
            rows=base.rows,
            latest_ms=base.latest_ms,
            interval_ok=False,
            earliest_ms=base.earliest_ms,
            error=f"hot tail: {tail.error}",
        )
    if base.latest_ms is None:
        return tail

    dates = _normalize_date_frame(_read_date_column(tail_path)).select(
        pl.col("date").str.to_datetime(strict=False).alias("__ts")
    )
    base_latest = datetime.fromtimestamp(base.latest_ms / 1000, tz=timezone.utc).replace(
        tzinfo=None
    )
    newer = dates.filter(pl.col("__ts") > base_latest)
    first_new = newer.select(pl.col("__ts").min()).item() if newer.height else None
    contiguous = first_new is None or int(
        first_new.replace(tzinfo=timezone.utc).timestamp() * 1000
    ) <= base.latest_ms + CANDLE_INTERVAL_MS
    earliest_values = [
        value for value in (base.earliest_ms, tail.earliest_ms) if value is not None
    ]
    latest_values = [
        value for value in (base.latest_ms, tail.latest_ms) if value is not None
    ]
    return ExistingCandleInfo(
        rows=base.rows + newer.height,
        latest_ms=max(latest_values) if latest_values else None,
        interval_ok=bool(base.interval_ok and tail.interval_ok and contiguous),
        earliest_ms=min(earliest_values) if earliest_values else None,
    )


def _iter_windows(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    """Return inclusive API windows with one boundary candle of overlap.

    Bybit returns at most 1,000 reverse-sorted rows.  Retaining the previous
    window's last candle as the next window's first candle makes the traversal
    robust to either endpoint being omitted transiently; normalization removes
    the deliberate duplicate.  Never advance by ``chunk_end + interval`` here.
    """

    windows: list[tuple[int, int]] = []
    cursor = start_ms

    while cursor <= end_ms:
        chunk_end = min(cursor + BYBIT_WINDOW_SPAN_MS, end_ms)
        windows.append((cursor, chunk_end))
        if chunk_end >= end_ms:
            break
        cursor = chunk_end

    return windows


class BybitClient:
    def __init__(
        self, request_interval: float | None, max_retries: int, retry_base: float
    ) -> None:
        self.request_interval = resolve_request_interval(
            "bybit_public_rest", request_interval
        )
        self.max_retries = max(0, max_retries)
        self.retry_base = max(0.1, retry_base)
        self.limiter = SharedRateLimiter(
            self.request_interval,
            name="bybit_public_rest",
        )
        print(
            f"[bybit] {describe_rate_limit('bybit_public_rest', self.request_interval)}",
            flush=True,
        )

    def _defer_retry(
        self,
        attempt: int,
        *,
        headers: Any = None,
        minimum_seconds: float = 0.0,
    ) -> None:
        retry_after = headers.get("Retry-After") if headers is not None else None
        delay = retry_delay_seconds(
            attempt,
            base=self.retry_base,
            retry_after=retry_after,
        )
        if headers is not None:
            reset_value = headers.get("X-Bapi-Limit-Reset-Timestamp")
            try:
                reset_delay = float(reset_value) / 1000.0 - time.time()
            except (TypeError, ValueError):
                reset_delay = 0.0
            delay = max(delay, reset_delay)
        self.limiter.defer(max(delay, float(minimum_seconds)))

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            url = f"{BASE_URL}{path}?{urlencode(params)}"
            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                },
            )

            try:
                with urlopen(req, timeout=30) as response:
                    response_headers = response.headers
                    payload = json.load(response)

                if int(payload.get("retCode", -1)) == 0:
                    return payload

                code = str(payload.get("retCode"))
                message = str(payload.get("retMsg") or "")
                retriable_code = {"10006", "429", "10000"}
                if code in retriable_code and attempt < self.max_retries:
                    self._defer_retry(attempt, headers=response_headers)
                    continue
                raise RuntimeError(f"Bybit API error retCode={code} retMsg={message}")

            except HTTPError as exc:
                last_error = exc
                if (
                    exc.code in {403, 429, 500, 502, 503, 504}
                    and attempt < self.max_retries
                ):
                    self._defer_retry(
                        attempt,
                        headers=exc.headers,
                        minimum_seconds=600.0 if exc.code == 403 else 0.0,
                    )
                    continue
                raise
            except (
                URLError,
                TimeoutError,
                IncompleteRead,
                RemoteDisconnected,
                ConnectionError,
            ) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._defer_retry(attempt)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Bybit request failed without explicit error")


def _fetch_perp_symbols(
    client: BybitClient,
    categories: list[str],
    limit: int | None = None,
) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []

    for category in categories:
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "category": category,
                "limit": "1000",
            }
            if cursor:
                params["cursor"] = cursor

            payload = client.get(INSTRUMENTS_ENDPOINT, params)
            result = payload.get("result", {})
            items = result.get("list", [])

            for item in items:
                if item.get("status") != "Trading":
                    continue
                contract_type = str(item.get("contractType") or "")
                if "Perpetual" not in contract_type:
                    continue

                symbol = item.get("symbol")
                if not symbol:
                    continue

                market = f"bybit_{category}_perp"
                price_filter = item.get("priceFilter") or {}
                lot_filter = item.get("lotSizeFilter") or {}
                leverage_filter = item.get("leverageFilter") or {}

                def optional_float(value: object) -> float | None:
                    return (
                        float(value)
                        if value not in {None, ""}
                        else None
                    )

                records.append(
                    SymbolRecord(
                        code=symbol,
                        name=symbol,
                        market=market,
                        bybit_symbol=symbol,
                        category=category,
                        base_coin=item.get("baseCoin"),
                        quote_coin=item.get("quoteCoin"),
                        settle_coin=item.get("settleCoin"),
                        contract_type=item.get("contractType"),
                        status=item.get("status"),
                        launch_time=_ms_to_date_string(int(item["launchTime"]))
                        if item.get("launchTime")
                        else None,
                        symbol_type=str(item.get("symbolType") or "").strip(),
                        funding_interval_minutes=(
                            int(item["fundingInterval"])
                            if item.get("fundingInterval") not in {None, ""}
                            else None
                        ),
                        is_pre_listing=bool(item.get("isPreListing", False)),
                        price_tick_size=optional_float(price_filter.get("tickSize")),
                        minimum_order_quantity=optional_float(
                            lot_filter.get("minOrderQty")
                        ),
                        quantity_step=optional_float(lot_filter.get("qtyStep")),
                        minimum_notional_value=optional_float(
                            lot_filter.get("minNotionalValue")
                        ),
                        maximum_order_quantity=optional_float(
                            lot_filter.get("maxOrderQty")
                        ),
                        maximum_market_order_quantity=optional_float(
                            lot_filter.get("maxMktOrderQty")
                        ),
                        maximum_leverage=optional_float(
                            leverage_filter.get("maxLeverage")
                        ),
                        unified_margin_trade=bool(
                            item.get("unifiedMarginTrade", False)
                        ),
                    )
                )

            cursor = result.get("nextPageCursor")
            if not cursor:
                break

    records.sort(key=lambda x: (x.category, x.bybit_symbol))
    if limit is not None:
        return records[:limit]
    return records


def _normalize_candles(raw_rows: list[list[str]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < 7:
            continue

        ts = int(row[0])
        volume = float(row[5]) if row[5] else 0.0
        turnover = float(row[6]) if row[6] else 0.0
        rows.append(
            {
                "ts": ts,
                "date": _ms_to_date_string(ts),
                "open": float(row[1]),
                "max": float(row[2]),
                "min": float(row[3]),
                "close": float(row[4]),
                "adjclose": float(row[4]),
                "Trading_Volume": volume,
                "bybit_volume": volume,
                "bybit_turnover": turnover,
            }
        )

    if not rows:
        return pl.DataFrame({column: [] for column in OUTPUT_COLUMNS})

    return (
        pl.DataFrame(rows)
        .sort("ts")
        .unique(subset=["date"], keep="last")
        .sort("ts")
        .drop("ts")
    )


def _download_symbol_1m(
    client: BybitClient,
    record: SymbolRecord,
    output_dir: Path,
    start_ms: int,
    end_ms: int,
    mode: str,
    refresh: bool,
    tail_only: bool = False,
    page_progress_callback: Any = None,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing_info: ExistingCandleInfo | None = None
    effective_start_ms = start_ms
    if record.launch_time:
        effective_start_ms = max(
            effective_start_ms,
            int(
                datetime.strptime(record.launch_time, "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            ),
        )

    if output_path.exists() and not refresh:
        existing_info = _load_logical_existing_candle_info(
            output_path,
            require_contiguous=not tail_only,
        )
        if existing_info.error is not None or not existing_info.interval_ok:
            if tail_only:
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="repair_required",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                    message=(
                        "Existing parquet failed the 1m contract; tail-only mode "
                        "will not overwrite historical data. Run a backfill repair."
                    ),
                )
            print(
                f"[bybit] {record.bybit_symbol}: existing parquet does not look like "
                f"{KLINE_INTERVAL_LABEL}; rebuilding from start_date"
            )
            existing_info = None

        if existing_info is not None and existing_info.rows > 0:
            effective_start_ms, _ = resolve_incremental_reconcile_start_ms(
                expected_first_ms=effective_start_ms,
                earliest_existing_ms=existing_info.earliest_ms,
                latest_existing_ms=existing_info.latest_ms,
                overlap_ms=CANDLE_INTERVAL_MS,
                repair_missing_head=not tail_only,
            )
            if effective_start_ms > end_ms:
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                )
    elif tail_only:
        effective_start_ms = max(
            effective_start_ms,
            end_ms - 24 * 60 * CANDLE_INTERVAL_MS,
        )

    all_rows: list[list[str]] = []
    for window_start, window_end in _iter_windows(effective_start_ms, end_ms):
        payload = client.get(
            KLINE_ENDPOINT,
            {
                "category": record.category,
                "symbol": record.bybit_symbol,
                "interval": KLINE_INTERVAL,
                "start": str(window_start),
                "end": str(window_end),
                "limit": BYBIT_MAX_KLINE_LIMIT,
            },
        )
        if page_progress_callback is not None:
            page_progress_callback(record.code)
        chunk = payload.get("result", {}).get("list", [])
        if chunk:
            all_rows.extend(chunk)

    if not all_rows:
        if existing_info is not None and existing_info.rows > 0:
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=existing_info.rows,
                output_path=str(output_path),
            )
        return DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="skipped_no_data",
            rows=0,
            output_path=None,
            message="No candles returned by Bybit.",
        )

    closed_end_ms = min(end_ms, _latest_closed_candle_start_ms())
    filtered_rows = [
        row for row in all_rows if effective_start_ms <= int(row[0]) <= closed_end_ms
    ]
    df = _normalize_candles(filtered_rows)
    if df.is_empty():
        if existing_info is not None and existing_info.rows > 0:
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=existing_info.rows,
                output_path=str(output_path),
            )
        return DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="skipped_no_data",
            rows=0,
            output_path=None,
            message="No rows in requested date range.",
        )

    if existing_info is not None and existing_info.rows > 0:
        if tail_only:
            tail_path = hot_tail_path(output_path)
            fresh_latest = _latest_ms_from_date_frame(df)
            if (
                not tail_path.is_file()
                and fresh_latest is not None
                and existing_info.latest_ms is not None
                and fresh_latest <= existing_info.latest_ms
            ):
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                )
            if tail_path.is_file():
                df, changed = _merge_existing_with_fresh(
                    _read_parquet(tail_path), df, effective_start_ms
                )
            else:
                changed = True
            if not changed:
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                )
            _write_parquet(df, tail_path)
            logical = _load_logical_existing_candle_info(
                output_path, require_contiguous=False
            )
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="updated",
                rows=logical.rows,
                output_path=str(output_path),
            )
        combined, changed = _merge_existing_with_fresh(
            read_logical_parquet(output_path), df, effective_start_ms
        )
        if not changed:
            if not hot_tail_path(output_path).is_file():
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                )
        df = combined

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(df, output_path)
    remove_hot_tail(output_path)

    return DownloadResult(
        asset_class="crypto_bybit_perp",
        code=record.code,
        bybit_symbol=record.bybit_symbol,
        market=record.market,
        status="updated",
        rows=df.height,
        output_path=str(output_path),
    )


def main() -> None:
    started_at = datetime.now(timezone.utc)
    args = parse_args()
    if args.tail_only and (args.refresh or args.mode == "full"):
        raise ValueError("--tail-only cannot be combined with --refresh or --mode full")
    categories = _resolve_categories(args.categories)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (output_dir / ".download.lock").open("a+", encoding="utf-8")
    if fcntl is not None:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
    print(f"[bybit] acquired exclusive dataset lock: {lock_handle.name}", flush=True)

    start_date = args.start_date.strip()
    end_date = resolve_end_date(args.end_date)
    start_ms = _date_to_ms(start_date, end_of_day=False)
    end_ms = _date_to_ms(end_date, end_of_day=True)

    client = BybitClient(
        request_interval=args.request_interval,
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )

    symbols = _fetch_perp_symbols(client, categories=categories)
    requested_symbols = {
        str(value).strip().upper()
        for value in (args.symbols or [])
        if str(value).strip()
    }
    if requested_symbols:
        symbols = [
            item for item in symbols if item.bybit_symbol.upper() in requested_symbols
        ]
        missing_symbols = requested_symbols - {
            item.bybit_symbol.upper() for item in symbols
        }
        if missing_symbols:
            raise ValueError(
                f"requested Bybit perpetual symbols unavailable: {sorted(missing_symbols)}"
            )
    if args.limit is not None:
        symbols = symbols[: max(0, int(args.limit))]
    if not symbols:
        raise RuntimeError("No Bybit perpetual symbols found for selected categories.")

    symbols_path = output_dir / "symbols.csv"
    atomic_write_text(
        symbols_path,
        pl.DataFrame([asdict(s) for s in symbols]).write_csv(),
    )

    total_symbols = len(symbols)
    pipeline_progress = PersistentProgress(
        output_dir / "progress.json",
        label="Bybit 永續合約 1 分鐘 K線",
        total=total_symbols,
        unit="symbol",
        basis=(
            "completed symbols divided by full elapsed time; request pages are "
            "separate telemetry because listing age changes endpoint depth"
        ),
        started_at=started_at,
    )
    print(
        "[bybit] start "
        f"symbols={total_symbols} interval={KLINE_INTERVAL_LABEL} "
        f"workers={args.workers} request_interval={client.request_interval:.6f}s"
    )

    progress_lock = threading.Lock()
    started_symbols = 0
    finished_symbols = 0

    def _worker(record: SymbolRecord) -> DownloadResult:
        nonlocal started_symbols, finished_symbols

        with progress_lock:
            started_symbols += 1
            started_idx = started_symbols

        if started_idx <= 5 or started_idx % 50 == 0:
            print(
                f"[bybit] start symbol {started_idx}/{total_symbols} "
                f"{record.bybit_symbol} ({record.category})"
            )

        result = _download_symbol_1m(
            client,
            record,
            output_dir,
            start_ms,
            end_ms,
            args.mode,
            args.refresh,
            tail_only=args.tail_only,
            page_progress_callback=lambda _code: pipeline_progress.observe(
                "candles", "request_pages"
            ),
        )
        pipeline_progress.update("candles", result.status)

        with progress_lock:
            finished_symbols += 1
            done_idx = finished_symbols

        if done_idx <= 5 or done_idx % 25 == 0:
            print(
                f"[bybit] done symbol {done_idx}/{total_symbols} "
                f"{record.bybit_symbol} status={result.status} rows={result.rows}"
            )

        return result

    def _on_error(record: SymbolRecord, exc: Exception) -> DownloadResult:
        result = DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message=str(exc),
        )
        pipeline_progress.update("candles", result.status)
        return result

    results = run_parallel_tasks(
        symbols,
        _worker,
        max_workers=args.workers,
        desc="download:bybit",
        unit="symbol",
        on_error=_on_error,
    )

    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"

    result_rows = [asdict(r) for r in results]
    result_df = (
        pl.DataFrame(result_rows, infer_schema_length=None).sort(
            ["status", "bybit_symbol"]
        )
        if result_rows
        else pl.DataFrame()
    )
    atomic_write_text(report_path, result_df.write_csv())
    status_counts: dict[str, int] = {}
    row_count = 0
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        row_count += int(result.rows)
    stored_inventory = _stored_parquet_inventory(
        output_dir,
        current_codes={record.code for record in symbols},
    )

    summary = {
        "asset_class": "crypto_bybit_perp",
        "interval": KLINE_INTERVAL_LABEL,
        "symbol_count": len(symbols),
        "row_count": row_count,
        "current_symbol_count": len(symbols),
        "current_row_count": row_count,
        **stored_inventory,
        "status_counts": status_counts,
        "tail_only": args.tail_only,
        "categories": categories,
        "start_date": start_date,
        "end_date": end_date,
        "started_at_utc": started_at.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    atomic_write_text(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    pipeline_progress.finish(
        failed=any(result.status in {"failed", "repair_required"} for result in results),
        require_exact=True,
    )

    print(f"[bybit] symbols.csv -> {symbols_path}")
    print(f"[bybit] download_report.csv -> {report_path}")
    print(f"[bybit] download_summary.json -> {summary_path}")
    print(f"[bybit] done: {json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
