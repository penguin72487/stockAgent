from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    SharedRateLimiter,
    provider_rate_limit,
    resolve_end_date,
    resolve_incremental_reconcile_start_ms,
    retry_delay_seconds,
    run_parallel_tasks,
)


BASE_URL = "https://fapi.binance.com"
EXCHANGE_INFO_ENDPOINT = "/fapi/v1/exchangeInfo"
KLINE_ENDPOINT = "/fapi/v1/klines"
KLINE_INTERVAL = "15m"
CANDLE_INTERVAL_MS = 15 * 60 * 1000

# Binance charges request weight 2 for [100, 500) rows. 499 therefore yields
# 249.5 bars/weight, the best documented kline efficiency tier.
KLINE_LIMIT = 499
KLINE_REQUEST_WEIGHT = 2.0
OUTPUT_COLUMNS = [
    "date",
    "open",
    "max",
    "min",
    "close",
    "adjclose",
    "Trading_Volume",
    "binance_volume_base",
    "binance_volume_quote",
    "binance_trade_count",
    "binance_taker_buy_base_volume",
    "binance_taker_buy_quote_volume",
    "binance_close_time_ms",
]


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    binance_symbol: str
    pair: str | None
    base_asset: str | None
    quote_asset: str | None
    margin_asset: str | None
    contract_type: str | None
    status: str | None
    onboard_time: str | None
    current_exchange_info: bool = True


@dataclass(slots=True)
class ExistingCandleInfo:
    rows: int
    latest_ms: int | None
    interval_ok: bool
    earliest_ms: int | None = None
    error: str | None = None


@dataclass(slots=True)
class DownloadResult:
    asset_class: str
    code: str
    binance_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    first_date: str | None = None
    last_date: str | None = None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download Binance USD-M perpetual 15-minute candles with dynamic "
            "request-weight pacing and atomic parquet publication."
        )
    )
    parser.add_argument("--output-dir", default="data_binance")
    parser.add_argument(
        "--mode",
        choices=["incremental", "daily-update", "full"],
        default="incremental",
        help=(
            "incremental fetches the missing tail; daily-update is an alias; "
            "full reconciles requested head and tail coverage; --refresh forces rebuild."
        ),
    )
    parser.add_argument("--start-date", default="2019-09-08")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--request-weight-per-minute",
        type=float,
        default=None,
        help=(
            "Optional slower IP weight budget. Runtime exchangeInfo is always "
            "authoritative and a higher requested value is clamped."
        ),
    )
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base", type=float, default=0.6)
    return parser.parse_args()


def _date_to_ms(value: str, *, end_of_day: bool) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(parsed.timestamp() * 1000)


def _ms_to_date_string(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _latest_closed_candle_start_ms(now: datetime | None = None) -> int:
    current = (
        now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    )
    current_ms = int(current.timestamp() * 1000)
    return (current_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS - CANDLE_INTERVAL_MS


def _normalize_date_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    expression = (
        pl.col("date").str.to_datetime(strict=False)
        if frame.schema.get("date") == pl.String
        else pl.col("date").cast(pl.Datetime("us"), strict=False)
    )
    return (
        frame.with_columns(expression.dt.strftime("%Y-%m-%d %H:%M:%S").alias("date"))
        .drop_nulls("date")
        .sort("date")
    )


def _latest_ms(frame: pl.DataFrame) -> int | None:
    normalized = _normalize_date_frame(frame)
    if normalized.is_empty() or "date" not in normalized.columns:
        return None
    latest = normalized.select(pl.col("date").str.to_datetime().max()).item()
    if latest is None:
        return None
    return int(latest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _earliest_ms(frame: pl.DataFrame) -> int | None:
    normalized = _normalize_date_frame(frame)
    if normalized.is_empty() or "date" not in normalized.columns:
        return None
    earliest = normalized.select(pl.col("date").str.to_datetime().min()).item()
    if earliest is None:
        return None
    return int(earliest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _frame_matches_15m_interval(frame: pl.DataFrame) -> bool:
    normalized = _normalize_date_frame(frame)
    if normalized.height < 3 or "date" not in normalized.columns:
        return True
    parsed = normalized.select(
        pl.col("date").str.to_datetime(strict=False).alias("date")
    ).drop_nulls("date")
    deltas = (
        parsed.select(pl.col("date").diff().dt.total_seconds().alias("delta"))
        .drop_nulls("delta")
        .filter(pl.col("delta") > 0)
    )
    if deltas.is_empty():
        return True
    median_delta = float(deltas.select(pl.col("delta").median()).item())
    return median_delta <= (CANDLE_INTERVAL_MS / 1000) * 4


def _load_existing_info(path: Path) -> ExistingCandleInfo:
    try:
        parquet = pq.ParquetFile(path, memory_map=True)
        rows = int(parquet.metadata.num_rows)
        if "date" not in set(parquet.schema_arrow.names):
            return ExistingCandleInfo(
                rows=rows,
                latest_ms=None,
                interval_ok=False,
                error="missing date column",
            )
        dates = pl.from_arrow(pq.read_table(path, columns=["date"], memory_map=True))
        return ExistingCandleInfo(
            rows=rows,
            latest_ms=_latest_ms(dates),
            interval_ok=_frame_matches_15m_interval(dates),
            earliest_ms=_earliest_ms(dates),
        )
    except Exception as exc:
        return ExistingCandleInfo(
            rows=0,
            latest_ms=None,
            interval_ok=False,
            error=f"{type(exc).__name__}: {exc}",
        )


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path, memory_map=True))


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(),
            temporary,
            compression="snappy",
            write_statistics=True,
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_csv_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.write_csv(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _merge_existing_with_fresh(
    existing: pl.DataFrame,
    fresh: pl.DataFrame,
    effective_start_ms: int,
) -> tuple[pl.DataFrame, bool]:
    normalized = _normalize_date_frame(existing)
    cutoff = _ms_to_date_string(effective_start_ms)
    kept = normalized.filter(pl.col("date") < cutoff)
    overlap_existing = normalized.filter(pl.col("date") >= cutoff)
    overlap = pl.concat(
        [
            overlap_existing.with_columns(pl.lit(0).alias("__priority")),
            fresh.with_columns(pl.lit(1).alias("__priority")),
        ],
        how="diagonal_relaxed",
    ).sort(["date", "__priority"])
    values = [
        column for column in overlap.columns if column not in {"date", "__priority"}
    ]
    merged_overlap = (
        overlap.group_by("date", maintain_order=True)
        .agg([pl.col(column).drop_nulls().last().alias(column) for column in values])
        .sort("date")
    )
    combined = pl.concat([kept, merged_overlap], how="diagonal_relaxed").sort("date")
    common = [column for column in normalized.columns if column in combined.columns]
    unchanged = (
        normalized.height == combined.height
        and bool(common)
        and normalized.select(common).equals(combined.select(common))
    )
    return combined, not unchanged


def _validate_candles(frame: pl.DataFrame) -> None:
    missing = [column for column in OUTPUT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"normalized Binance candles missing columns: {missing}")
    if frame.select(pl.col("date").is_duplicated().any()).item():
        raise ValueError("normalized Binance candles contain duplicate timestamps")
    invalid = frame.filter(
        (pl.col("open") <= 0)
        | (pl.col("max") <= 0)
        | (pl.col("min") <= 0)
        | (pl.col("close") <= 0)
        | (pl.col("min") > pl.min_horizontal("open", "close", "max"))
        | (pl.col("max") < pl.max_horizontal("open", "close", "min"))
        | (pl.col("Trading_Volume") < 0)
        | (pl.col("binance_volume_quote") < 0)
        | (pl.col("binance_trade_count") < 0)
    )
    if not invalid.is_empty():
        raise ValueError(f"Binance returned {invalid.height} invalid OHLCV rows")


def _normalize_candles(raw_rows: list[list[Any]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < 11:
            continue
        open_time = int(row[0])
        close = float(row[4])
        rows.append(
            {
                "open_time_ms": open_time,
                "date": _ms_to_date_string(open_time),
                "open": float(row[1]),
                "max": float(row[2]),
                "min": float(row[3]),
                "close": close,
                "adjclose": close,
                "Trading_Volume": float(row[5]),
                "binance_volume_base": float(row[5]),
                "binance_volume_quote": float(row[7]),
                "binance_trade_count": int(row[8]),
                "binance_taker_buy_base_volume": float(row[9]),
                "binance_taker_buy_quote_volume": float(row[10]),
                "binance_close_time_ms": int(row[6]),
            }
        )
    if not rows:
        return pl.DataFrame({column: [] for column in OUTPUT_COLUMNS})
    frame = (
        pl.DataFrame(rows)
        .sort("open_time_ms")
        .unique(subset=["date"], keep="last", maintain_order=True)
        .drop("open_time_ms")
        .sort("date")
    )
    _validate_candles(frame)
    return frame


def _safe_symbol(value: str) -> str:
    # Binance explicitly permits non-ASCII symbol names. Preserve the exact
    # exchange identifier and reject only values that are unsafe as one local
    # filename component.
    symbol = str(value).strip()
    if (
        not symbol
        or symbol in {".", ".."}
        or "/" in symbol
        or "\\" in symbol
        or any(ord(character) < 32 for character in symbol)
    ):
        raise ValueError(f"unsafe Binance symbol: {value!r}")
    return symbol


class BinanceClient:
    def __init__(
        self,
        *,
        requested_weight_per_minute: float | None,
        max_retries: int,
        retry_base: float,
    ) -> None:
        fallback = provider_rate_limit("binance_usdm_request_weight")
        requested = (
            fallback.requests
            if requested_weight_per_minute is None
            else max(0.001, float(requested_weight_per_minute))
        )
        self.requested_weight_per_minute = requested
        self.weight_per_minute = min(float(fallback.requests), requested)
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))
        self._limiter_lock = threading.Lock()
        self.limiter = self._new_limiter(self.weight_per_minute)

    @staticmethod
    def _new_limiter(weight_per_minute: float) -> SharedRateLimiter:
        return SharedRateLimiter(
            60.0 / max(0.001, float(weight_per_minute)),
            name="binance_usdm_request_weight",
        )

    def configure_exchange_limits(self, payload: dict[str, Any]) -> None:
        candidates: list[float] = []
        interval_seconds = {
            "SECOND": 1.0,
            "MINUTE": 60.0,
            "HOUR": 3600.0,
            "DAY": 86400.0,
        }
        for row in payload.get("rateLimits", []):
            if row.get("rateLimitType") != "REQUEST_WEIGHT":
                continue
            unit_seconds = interval_seconds.get(str(row.get("interval") or "").upper())
            try:
                seconds = unit_seconds * float(row.get("intervalNum"))
                limit = float(row.get("limit"))
            except (TypeError, ValueError):
                continue
            if seconds and seconds > 0 and limit > 0:
                candidates.append(limit * 60.0 / seconds)
        if not candidates:
            raise RuntimeError(
                "Binance exchangeInfo has no usable REQUEST_WEIGHT limit"
            )
        observed_weight_per_minute = min(candidates)
        effective = min(observed_weight_per_minute, self.requested_weight_per_minute)
        with self._limiter_lock:
            self.weight_per_minute = effective
            self.limiter = self._new_limiter(effective)

    def _defer_retry(
        self,
        limiter: SharedRateLimiter,
        attempt: int,
        *,
        headers: Any = None,
    ) -> None:
        retry_after = headers.get("Retry-After") if headers is not None else None
        limiter.defer(
            retry_delay_seconds(
                attempt,
                base=self.retry_base,
                retry_after=retry_after,
            )
        )

    def get(self, path: str, params: dict[str, Any], *, weight: float) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            with self._limiter_lock:
                limiter = self.limiter
            limiter.wait(cost=weight)
            query = f"?{urlencode(params)}" if params else ""
            request = Request(
                f"{BASE_URL}{path}{query}",
                headers={"Accept": "application/json", "User-Agent": "stockAgent/1"},
            )
            try:
                with urlopen(request, timeout=30) as response:
                    response_headers = response.headers
                    payload = json.load(response)
                if isinstance(payload, dict) and int(payload.get("code", 0)) < 0:
                    code = int(payload["code"])
                    if code in {-1003, -1008} and attempt < self.max_retries:
                        self._defer_retry(limiter, attempt, headers=response_headers)
                        continue
                    raise RuntimeError(
                        f"Binance API error code={code} msg={payload.get('msg')}"
                    )
                return payload
            except HTTPError as exc:
                last_error = exc
                if (
                    exc.code in {418, 429, 500, 502, 503, 504}
                    and attempt < self.max_retries
                ):
                    self._defer_retry(limiter, attempt, headers=exc.headers)
                    continue
                raise
            except URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._defer_retry(limiter, attempt)
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("Binance request failed without explicit error")


def _symbol_from_exchange_info(row: dict[str, Any]) -> SymbolRecord | None:
    if row.get("contractType") != "PERPETUAL" or row.get("status") != "TRADING":
        return None
    symbol = _safe_symbol(str(row.get("symbol") or ""))
    if not symbol:
        return None
    onboard = row.get("onboardDate")
    return SymbolRecord(
        code=symbol,
        name=symbol,
        market="binance_usdm_perp",
        binance_symbol=symbol,
        pair=row.get("pair"),
        base_asset=row.get("baseAsset"),
        quote_asset=row.get("quoteAsset"),
        margin_asset=row.get("marginAsset"),
        contract_type=row.get("contractType"),
        status=row.get("status"),
        onboard_time=_ms_to_date_string(int(onboard)) if onboard else None,
        current_exchange_info=True,
    )


def _load_prior_symbols(path: Path) -> list[SymbolRecord]:
    if not path.exists():
        return []
    try:
        rows = pl.read_csv(path).to_dicts()
    except Exception:
        return []
    records: list[SymbolRecord] = []
    for row in rows:
        try:
            symbol = _safe_symbol(
                str(row.get("binance_symbol") or row.get("code") or "")
            )
        except ValueError:
            continue
        records.append(
            SymbolRecord(
                code=symbol,
                name=str(row.get("name") or symbol),
                market="binance_usdm_perp",
                binance_symbol=symbol,
                pair=row.get("pair"),
                base_asset=row.get("base_asset"),
                quote_asset=row.get("quote_asset"),
                margin_asset=row.get("margin_asset"),
                contract_type=row.get("contract_type"),
                status=row.get("status"),
                onboard_time=row.get("onboard_time"),
                current_exchange_info=False,
            )
        )
    return records


def _fetch_symbols(
    client: BinanceClient,
    symbols_path: Path,
    *,
    limit: int | None,
) -> tuple[list[SymbolRecord], dict[str, Any]]:
    payload = client.get(EXCHANGE_INFO_ENDPOINT, {}, weight=1.0)
    if not isinstance(payload, dict):
        raise RuntimeError("Binance exchangeInfo returned a non-object payload")
    client.configure_exchange_limits(payload)
    by_symbol: dict[str, SymbolRecord] = {
        record.binance_symbol: record for record in _load_prior_symbols(symbols_path)
    }
    for row in payload.get("symbols", []):
        record = _symbol_from_exchange_info(row)
        if record is not None:
            by_symbol[record.binance_symbol] = record
    records = sorted(by_symbol.values(), key=lambda record: record.binance_symbol)
    if limit is not None:
        records = records[: max(0, int(limit))]
    return records, payload


def _download_symbol(
    client: BinanceClient,
    record: SymbolRecord,
    output_dir: Path,
    *,
    start_ms: int,
    end_ms: int,
    mode: str,
    refresh: bool,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing: ExistingCandleInfo | None = None
    effective_start = start_ms
    if record.onboard_time:
        effective_start = max(
            effective_start,
            int(
                datetime.strptime(record.onboard_time, "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            ),
        )

    if output_path.exists() and not refresh:
        existing = _load_existing_info(output_path)
        if existing.error is not None or not existing.interval_ok:
            print(
                f"[binance] {record.binance_symbol}: invalid existing parquet; "
                "rebuilding from requested start",
                flush=True,
            )
            existing = None
        elif existing.rows and existing.latest_ms is not None:
            effective_start, _ = resolve_incremental_reconcile_start_ms(
                expected_first_ms=effective_start,
                earliest_existing_ms=existing.earliest_ms,
                latest_existing_ms=existing.latest_ms,
                overlap_ms=CANDLE_INTERVAL_MS,
            )

    closed_end = min(end_ms, _latest_closed_candle_start_ms())
    if effective_start > closed_end:
        if existing is not None and existing.rows:
            return DownloadResult(
                "crypto_binance_usdm_perp",
                record.code,
                record.binance_symbol,
                record.market,
                "skipped_up_to_date",
                existing.rows,
                str(output_path),
            )
        return DownloadResult(
            "crypto_binance_usdm_perp",
            record.code,
            record.binance_symbol,
            record.market,
            "unavailable_outside_listing_window",
            0,
            None,
            message="Requested range ends before listing or before a completed candle.",
        )

    rows: list[list[Any]] = []
    cursor = effective_start
    while cursor <= closed_end:
        payload = client.get(
            KLINE_ENDPOINT,
            {
                "symbol": record.binance_symbol,
                "interval": KLINE_INTERVAL,
                "startTime": str(cursor),
                "endTime": str(closed_end + CANDLE_INTERVAL_MS - 1),
                "limit": str(KLINE_LIMIT),
            },
            weight=KLINE_REQUEST_WEIGHT,
        )
        if not isinstance(payload, list):
            raise RuntimeError("Binance kline endpoint returned a non-list payload")
        chunk = [row for row in payload if isinstance(row, list) and len(row) >= 11]
        if not chunk:
            break
        rows.extend(chunk)
        last_open = max(int(row[0]) for row in chunk)
        next_cursor = last_open + CANDLE_INTERVAL_MS
        if next_cursor <= cursor:
            raise RuntimeError("Binance kline pagination made no forward progress")
        cursor = next_cursor
        if len(chunk) < KLINE_LIMIT:
            break

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    filtered = [
        row
        for row in rows
        if effective_start <= int(row[0]) <= closed_end and int(row[6]) < now_ms
    ]
    fresh = _normalize_candles(filtered)
    if fresh.is_empty():
        if existing is not None and existing.rows:
            return DownloadResult(
                "crypto_binance_usdm_perp",
                record.code,
                record.binance_symbol,
                record.market,
                "skipped_up_to_date",
                existing.rows,
                str(output_path),
            )
        return DownloadResult(
            "crypto_binance_usdm_perp",
            record.code,
            record.binance_symbol,
            record.market,
            "unavailable_no_data",
            0,
            None,
            message="No completed Binance candles in the requested range.",
        )

    frame = fresh
    if existing is not None and existing.rows:
        frame, changed = _merge_existing_with_fresh(
            _read_parquet(output_path),
            fresh,
            effective_start,
        )
        if not changed:
            return DownloadResult(
                "crypto_binance_usdm_perp",
                record.code,
                record.binance_symbol,
                record.market,
                "skipped_up_to_date",
                existing.rows,
                str(output_path),
            )
    _validate_candles(frame)
    _write_parquet_atomic(frame, output_path)
    return DownloadResult(
        "crypto_binance_usdm_perp",
        record.code,
        record.binance_symbol,
        record.market,
        "updated",
        frame.height,
        str(output_path),
        frame["date"].min(),
        frame["date"].max(),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols_path = output_dir / "symbols.csv"
    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"
    receipt_path = output_dir / "download_receipt.json"
    start_date = args.start_date.strip()
    end_date = resolve_end_date(args.end_date)
    start_ms = _date_to_ms(start_date, end_of_day=False)
    end_ms = _date_to_ms(end_date, end_of_day=True)
    started_at = datetime.now(timezone.utc)

    client = BinanceClient(
        requested_weight_per_minute=args.request_weight_per_minute,
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )
    symbols, exchange_info = _fetch_symbols(
        client,
        symbols_path,
        limit=args.limit,
    )
    if not symbols:
        raise RuntimeError("No Binance USD-M perpetual symbols found")
    _write_csv_atomic(pl.DataFrame([asdict(row) for row in symbols]), symbols_path)

    print(
        "[binance] start "
        f"symbols={len(symbols)} interval={KLINE_INTERVAL} workers={args.workers} "
        f"weight_per_minute={client.weight_per_minute:.2f} "
        f"kline_limit={KLINE_LIMIT} kline_weight={KLINE_REQUEST_WEIGHT:g}",
        flush=True,
    )

    def worker(record: SymbolRecord) -> DownloadResult:
        return _download_symbol(
            client,
            record,
            output_dir,
            start_ms=start_ms,
            end_ms=end_ms,
            mode=args.mode,
            refresh=args.refresh,
        )

    def on_error(record: SymbolRecord, exc: Exception) -> DownloadResult:
        return DownloadResult(
            "crypto_binance_usdm_perp",
            record.code,
            record.binance_symbol,
            record.market,
            "failed",
            0,
            None,
            message=f"{type(exc).__name__}: {exc}",
        )

    results = run_parallel_tasks(
        symbols,
        worker,
        max_workers=args.workers,
        desc="download:binance",
        unit="symbol",
        on_error=on_error,
    )
    result_rows = [asdict(result) for result in results]
    report = pl.DataFrame(result_rows, infer_schema_length=None).sort(
        ["status", "binance_symbol"]
    )
    _write_csv_atomic(report, report_path)

    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    ended_at = datetime.now(timezone.utc)
    limiter_activity = client.limiter.grant_activity()
    summary = {
        "asset_class": "crypto_binance_usdm_perp",
        "interval": KLINE_INTERVAL,
        "symbol_count": len(symbols),
        "status_counts": status_counts,
        "start_date": start_date,
        "end_date": end_date,
        "started_at_utc": started_at.isoformat(),
        "ended_at_utc": ended_at.isoformat(),
        "elapsed_seconds": (ended_at - started_at).total_seconds(),
        "exchange_server_time_ms": exchange_info.get("serverTime"),
        "request_weight_per_minute": client.weight_per_minute,
        "kline_limit": KLINE_LIMIT,
        "kline_request_weight": KLINE_REQUEST_WEIGHT,
        "limiter_activity": limiter_activity,
    }
    _write_text_atomic(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    receipt = {
        "contract_version": 1,
        "source": {
            "provider": "Binance USD-M Futures",
            "base_url": BASE_URL,
            "exchange_info_endpoint": EXCHANGE_INFO_ENDPOINT,
            "kline_endpoint": KLINE_ENDPOINT,
            "official_documentation": (
                "https://developers.binance.com/en/docs/catalog/"
                "core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"
            ),
        },
        "artifacts": {
            path.name: {"bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in (symbols_path, report_path, summary_path)
        },
        "status_counts": status_counts,
        "generated_at_utc": ended_at.isoformat(),
    }
    _write_text_atomic(
        receipt_path,
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
    )

    print(f"[binance] symbols -> {symbols_path}")
    print(f"[binance] report -> {report_path}")
    print(f"[binance] summary -> {summary_path}")
    print(f"[binance] receipt -> {receipt_path}")
    print(f"[binance] done: {json.dumps(summary, ensure_ascii=False)}")
    failed = status_counts.get("failed", 0)
    if failed:
        raise RuntimeError(f"Binance download incomplete: {failed} symbols failed")


if __name__ == "__main__":
    main()
