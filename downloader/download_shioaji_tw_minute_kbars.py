from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import math
import multiprocessing as mp
import os
from pathlib import Path
from queue import Empty
import sys
import time
from typing import Any, Callable

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from downloader.common import SharedRateLimiter
except ModuleNotFoundError:  # direct script execution
    from common import SharedRateLimiter
from stockagent.live.shioaji_traffic_ledger import shioaji_query
from stockagent.live.shioaji_schedule import (
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    STOCK_HISTORY_TRAFFIC_RESERVE_MB,
)

from downloader.download_shioaji_tw_kbars import (  # noqa: E402
    MAX_KBAR_QUERY_DAYS,
    SHIOAJI_STOCK_HISTORY_START,
    SOURCE_NAME,
    SymbolResult,
    TrafficBudgetReached,
    UniverseRow,
    _atomic_write_json,
    _check_traffic_budget,
    _load_universe,
    _payload_dict,
    _positive_volume_dates,
    _read_json,
    _sha256,
    _taiwan_market_hours_now,
    iter_date_chunks,
    normalize_kbars,
)


STORAGE_FREQUENCY = "minute"
RECEIPT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
DEFAULT_CHUNK_DAYS = 29
MINUTE_SESSION_START = 9 * 60 + 1
MINUTE_SESSION_END = 13 * 60 + 30
SHIOAJI_QUOTE_LIMIT_REQUESTS = 50
SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS = 5.0
SHIOAJI_MAX_CONNECTIONS = 5
DEFAULT_WORKERS = SHIOAJI_MAX_CONNECTIONS
DEFAULT_REQUESTS_PER_SECOND = (
    SHIOAJI_QUOTE_LIMIT_REQUESTS / SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS
)


class MarketHoursReached(RuntimeError):
    """Pause a long historical run before it competes with live quote capture."""


class DownloadStopRequested(RuntimeError):
    """Another parallel worker reached a global stop condition."""


class SharedRequestRateLimiter:
    """Process-shared paced limiter with a strict sliding-window ceiling.

    The documented Shioaji quote-query guard is account-wide, so every worker,
    retry, and single-day fallback must acquire from this one limiter. Pacing
    avoids a 50-request burst while the ring buffer also guarantees that no
    sliding five-second window contains more than 50 request starts.
    """

    def __init__(
        self,
        context: Any,
        *,
        requests_per_second: float,
        max_requests: int = SHIOAJI_QUOTE_LIMIT_REQUESTS,
        window_seconds: float = SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        rate = float(requests_per_second)
        maximum = int(max_requests)
        window = float(window_seconds)
        if rate <= 0.0:
            raise ValueError("requests_per_second must be positive")
        if maximum <= 0 or window <= 0.0:
            raise ValueError("rate-limit window must be positive")
        if rate > maximum / window + 1e-12:
            raise ValueError(
                "requests_per_second exceeds the sliding-window ceiling: "
                f"rate={rate} ceiling={maximum / window}"
            )
        self.requests_per_second = rate
        self.max_requests = maximum
        self.window_seconds = window
        self._interval_seconds = 1.0 / rate
        self._lock = context.Lock()
        self._timestamps = context.Array("d", maximum, lock=False)
        self._head = context.Value("i", 0, lock=False)
        self._count = context.Value("i", 0, lock=False)
        self._next_start = context.Value("d", 0.0, lock=False)
        self._total = context.Value("q", 0, lock=False)
        self._first_start = context.Value("d", 0.0, lock=False)
        self._last_start = context.Value("d", 0.0, lock=False)

    def _prune_locked(self, now: float) -> None:
        while self._count.value > 0:
            oldest = self._timestamps[self._head.value]
            if now - oldest < self.window_seconds:
                break
            self._head.value = (self._head.value + 1) % self.max_requests
            self._count.value -= 1

    def acquire(self, stop_event: Any | None = None) -> float:
        with self._lock:
            if stop_event is not None and stop_event.is_set():
                raise DownloadStopRequested("parallel stop requested")
            now = time.monotonic()
            self._prune_locked(now)
            target = max(now, self._next_start.value)
            if self._count.value >= self.max_requests:
                oldest = self._timestamps[self._head.value]
                target = max(target, oldest + self.window_seconds)
            delay = max(0.0, target - now)
            if delay > 0.0:
                if stop_event is not None:
                    if stop_event.wait(delay):
                        raise DownloadStopRequested("parallel stop requested")
                else:
                    time.sleep(delay)
            started = time.monotonic()
            self._prune_locked(started)
            if self._count.value >= self.max_requests:
                # Monotonic-clock rounding can leave the oldest timestamp a few
                # nanoseconds inside the window. Wait it out rather than exceed
                # the documented account-wide boundary.
                oldest = self._timestamps[self._head.value]
                residual = max(0.0, oldest + self.window_seconds - started)
                if residual > 0.0:
                    if stop_event is not None:
                        if stop_event.wait(residual):
                            raise DownloadStopRequested("parallel stop requested")
                    else:
                        time.sleep(residual)
                    started = time.monotonic()
                    self._prune_locked(started)
            slot = (self._head.value + self._count.value) % self.max_requests
            self._timestamps[slot] = started
            self._count.value += 1
            self._next_start.value = started + self._interval_seconds
            self._total.value += 1
            if self._first_start.value <= 0.0:
                self._first_start.value = started
            self._last_start.value = started
            return started

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            now = time.monotonic()
            self._prune_locked(now)
            total = int(self._total.value)
            first = float(self._first_start.value)
            last = float(self._last_start.value)
            elapsed = max(0.0, last - first)
            return {
                "total_requests": total,
                "requests_in_window": int(self._count.value),
                "window_seconds": self.window_seconds,
                "window_rps": float(self._count.value) / self.window_seconds,
                "overall_rps": (
                    float(total - 1) / elapsed if total > 1 and elapsed > 0.0 else 0.0
                ),
            }


class SharedDownloadCounters:
    def __init__(self, context: Any) -> None:
        self._lock = context.Lock()
        self.processed_chunks = context.Value("q", 0, lock=False)
        self.queried_chunks = context.Value("q", 0, lock=False)
        self.skipped_empty_chunks = context.Value("q", 0, lock=False)

    def record_chunk(self, *, query_performed: bool) -> dict[str, int]:
        with self._lock:
            self.processed_chunks.value += 1
            if query_performed:
                self.queried_chunks.value += 1
            else:
                self.skipped_empty_chunks.value += 1
            return self.snapshot_locked()

    def snapshot_locked(self) -> dict[str, int]:
        return {
            "processed_chunks": int(self.processed_chunks.value),
            "queried_chunks": int(self.queried_chunks.value),
            "skipped_empty_chunks": int(self.skipped_empty_chunks.value),
        }

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return self.snapshot_locked()


class SharedTrafficBudgetGuard:
    """Serialize and cache the account-wide traffic check across workers."""

    def __init__(
        self,
        context: Any,
        *,
        max_fraction: float,
        reserve_bytes: int,
        check_interval_seconds: float,
    ) -> None:
        self.max_fraction = float(max_fraction)
        self.reserve_bytes = int(reserve_bytes)
        self.check_interval_seconds = float(check_interval_seconds)
        self._lock = context.Lock()
        self._last_check = context.Value("d", 0.0, lock=False)
        self._used = context.Value("q", -1, lock=False)
        self._limit = context.Value("q", -1, lock=False)

    def check(self, api: Any) -> tuple[int, int] | None:
        with self._lock:
            now = time.monotonic()
            if (
                self._last_check.value > 0.0
                and now - self._last_check.value < self.check_interval_seconds
            ):
                return self.last_usage_locked()
            used, limit = _check_traffic_budget(
                api,
                max_fraction=self.max_fraction,
                reserve_bytes=self.reserve_bytes,
            )
            self._used.value = used
            self._limit.value = limit
            self._last_check.value = now
            return used, limit

    def last_usage_locked(self) -> tuple[int, int] | None:
        if self._used.value < 0 or self._limit.value <= 0:
            return None
        return int(self._used.value), int(self._limit.value)

    def last_usage(self) -> tuple[int, int] | None:
        with self._lock:
            return self.last_usage_locked()


def _emit_worker_log(message: str) -> None:
    """Write one worker line atomically when stdout is a systemd/tee pipe."""

    payload = (str(message).rstrip("\n") + "\n").encode("utf-8", errors="replace")
    try:
        os.write(sys.stdout.fileno(), payload)
    except (AttributeError, OSError, ValueError):
        print(message, flush=True)


def query_minute_chunk(
    api: Any,
    contract: Any,
    row: UniverseRow,
    *,
    contract_unit: float,
    start: date,
    end: date,
    timeout_ms: int,
    retries: int,
    retry_backoff: float,
    expected_dates: set[date],
    request_started: Callable[[], None] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Query one chunk and retain only causal regular-session minute bars.

    Shioaji can include rows outside the public point-in-time lifecycle,
    after-hours 15:00 batches, and negative volume/amount correction rows in a
    historical KBar response. None is executable regular-session liquidity.
    They are removed with explicit receipt counters; every other malformed row
    still fails closed in ``normalize_kbars``.
    """

    def clean_payload(payload: Any) -> tuple[pl.DataFrame, dict[str, int]]:
        values = _payload_dict(payload)
        required = ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
        missing = [name for name in required if name not in values]
        if missing:
            raise ValueError(f"Shioaji Kbars payload is missing fields: {missing}")
        lengths = {name: len(values[name]) for name in required}
        if len(set(lengths.values())) != 1:
            raise ValueError(f"Shioaji Kbars fields have different lengths: {lengths}")
        if not lengths["ts"]:
            return normalize_kbars(
                values,
                symbol=row.symbol,
                market=row.market,
                contract_unit=contract_unit,
            ), {
                "zero_placeholder_rows_dropped": 0,
                "negative_correction_rows_dropped": 0,
                "out_of_session_rows_dropped": 0,
                "outside_reference_date_rows_dropped": 0,
            }

        raw = pl.DataFrame({name: values[name] for name in required}).with_columns(
            pl.col("ts").cast(pl.Datetime("ns"), strict=False),
            *[
                pl.col(name).cast(pl.Float64, strict=False).fill_nan(None).alias(name)
                for name in ("Open", "High", "Low", "Close", "Volume", "Amount")
            ],
        )
        reference_dates = sorted(expected_dates)
        outside_reference_date = (
            pl.col("ts").is_not_null()
            & ~pl.col("ts").cast(pl.Date).is_in(reference_dates)
        ).fill_null(True)
        all_zero_placeholder = pl.all_horizontal(
            *[(pl.col(name) == 0.0) for name in required[1:]]
        ).fill_null(False)
        # A negative trade quantity or amount cannot be executable liquidity.
        # Historical correction rows are not uniform: some have zero/partial
        # prices, and some negate only one of Volume/Amount. Requiring a valid
        # price plus two negative fields leaves those corrections in the alpha
        # dataset and causes an otherwise repairable symbol to fail closed.
        negative_correction = (
            (pl.col("Volume") < 0.0) | (pl.col("Amount") < 0.0)
        ).fill_null(False)
        minute_of_day = pl.col("ts").dt.hour().cast(pl.Int16) * 60 + pl.col(
            "ts"
        ).dt.minute().cast(pl.Int16)
        out_of_session = (
            pl.col("ts").is_not_null()
            & (
                (minute_of_day < MINUTE_SESSION_START)
                | (minute_of_day > MINUTE_SESSION_END)
            )
        ).fill_null(False)
        stats = {
            "outside_reference_date_rows_dropped": raw.filter(
                outside_reference_date
            ).height,
            "zero_placeholder_rows_dropped": raw.filter(
                ~outside_reference_date & all_zero_placeholder
            ).height,
            "negative_correction_rows_dropped": raw.filter(
                ~outside_reference_date & ~all_zero_placeholder & negative_correction
            ).height,
            "out_of_session_rows_dropped": raw.filter(
                ~outside_reference_date
                & ~all_zero_placeholder
                & ~negative_correction
                & out_of_session
            ).height,
        }
        cleaned = raw.filter(
            ~outside_reference_date
            & ~all_zero_placeholder
            & ~negative_correction
            & ~out_of_session
        )
        frame = normalize_kbars(
            cleaned.to_dict(as_series=False),
            symbol=row.symbol,
            market=row.market,
            contract_unit=contract_unit,
        )
        return frame, stats

    last_error: Exception | None = None
    for attempt in range(max(0, retries) + 1):
        try:
            if request_started is not None:
                request_started()
            with shioaji_query(
                api,
                consumer="stock_minute_backfill",
                method="kbars",
                asset_class="stock",
                details={
                    "contract": row.symbol,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                },
            ) as set_ledger_result:
                payload = api.kbars(
                    contract=contract,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    timeout=int(timeout_ms),
                )
                set_ledger_result(payload)
            frame, stats = clean_payload(payload)
            returned_dates = set(frame["date"].to_list()) if frame.height else set()
            missing_dates = sorted(expected_dates - returned_dates)
            if missing_dates:
                fallback_frames: list[pl.DataFrame] = []
                for missing_date in missing_dates:
                    if request_started is not None:
                        request_started()
                    with shioaji_query(
                        api,
                        consumer="stock_minute_gap_recovery",
                        method="kbars",
                        asset_class="stock",
                        details={
                            "contract": row.symbol,
                            "start": missing_date.isoformat(),
                            "end": missing_date.isoformat(),
                        },
                    ) as set_fallback_ledger_result:
                        fallback_payload = api.kbars(
                            contract=contract,
                            start=missing_date.isoformat(),
                            end=missing_date.isoformat(),
                            timeout=int(timeout_ms),
                        )
                        set_fallback_ledger_result(fallback_payload)
                    fallback, fallback_stats = clean_payload(fallback_payload)
                    for key, value in fallback_stats.items():
                        stats[key] += value
                    if fallback.height:
                        fallback_frames.append(fallback)
                stats["single_day_fallback_queries"] = len(missing_dates)
                if fallback_frames:
                    frame = pl.concat(
                        [frame, *fallback_frames], how="vertical_relaxed"
                    ).sort("ts")
                    returned_dates = set(frame["date"].to_list())
            unresolved_dates = sorted(expected_dates - returned_dates)
            stats["source_gap_dates"] = [
                value.isoformat() for value in unresolved_dates
            ]
            return frame, stats
        except DownloadStopRequested:
            raise
        except Exception as exc:
            last_error = exc
            if attempt >= max(0, retries):
                break
            time.sleep(float(retry_backoff) * (2**attempt))
    assert last_error is not None
    raise last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download and retain Shioaji Taiwan stock one-minute Kbars in "
            "receipt-backed, resumable <=29-day chunks. This module is separate "
            "from both daily history and Tick/BidAsk HFT captures."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_minute/shioaji_1m"),
    )
    parser.add_argument("--start-date", default=SHIOAJI_STOCK_HISTORY_START.isoformat())
    parser.add_argument(
        "--end-date", default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols. Required unless --universe-csv or --all-symbols.",
    )
    parser.add_argument(
        "--universe-csv",
        type=Path,
        help="Optional CSV containing a symbol or code column.",
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Explicitly request the entire public stock/ETF universe.",
    )
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Independent logged-in worker processes (maximum 5 per person ID).",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=DEFAULT_REQUESTS_PER_SECOND,
        help="Account-wide KBar request starts per second (maximum 10).",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.0,
        help=(
            "Legacy extra per-worker delay after a primary chunk. Keep 0 when "
            "using the account-wide limiter."
        ),
    )
    parser.add_argument(
        "--traffic-check-interval",
        type=float,
        default=5.0,
        help="Seconds to cache the account-wide traffic-usage check.",
    )
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=3.0)
    parser.add_argument(
        "--max-traffic-fraction",
        type=float,
        default=HISTORICAL_MAX_TRAFFIC_FRACTION,
    )
    parser.add_argument(
        "--traffic-reserve-mb",
        type=float,
        default=STOCK_HISTORY_TRAFFIC_RESERVE_MB,
    )
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def minute_chunk_paths(
    root: Path,
    symbol: str,
    start: date,
    end: date,
) -> tuple[Path, Path]:
    stem = f"{start.isoformat()}_{end.isoformat()}"
    symbol_root = root / "minute_chunks" / symbol
    data_path = symbol_root / f"{stem}.parquet"
    return data_path, data_path.with_suffix(".receipt.json")


def _write_minute_parquet(frame: pl.DataFrame, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(
        temporary,
        compression="zstd",
        compression_level=7,
        statistics=True,
        row_group_size=128_000,
    )
    os.replace(temporary, path)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": _sha256(path),
    }


def validate_minute_kbars(
    frame: pl.DataFrame,
    *,
    symbol: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    if frame.is_empty():
        return {
            "rows": 0,
            "sessions": 0,
            "first_ts": None,
            "last_ts": None,
            "duplicate_timestamps": 0,
            "out_of_session_rows": 0,
            "non_minute_rows": 0,
        }
    minute_of_day = pl.col("ts").dt.hour().cast(pl.Int16) * 60 + pl.col(
        "ts"
    ).dt.minute().cast(pl.Int16)
    duplicate_timestamps = frame.group_by("ts").len().filter(pl.col("len") > 1).height
    out_of_session_rows = frame.filter(
        (minute_of_day < 9 * 60 + 1)
        | (minute_of_day > 13 * 60 + 30)
        | (pl.col("ts").dt.second() != 0)
    ).height
    # Do not infer the integer timestamp unit: Polars may hold an otherwise
    # valid input as us or ns depending on how the frame was constructed.
    non_minute_rows = frame.filter(
        (pl.col("ts").dt.second() != 0) | (pl.col("ts").dt.nanosecond() != 0)
    ).height
    wrong_symbol_rows = frame.filter(pl.col("symbol") != symbol).height
    wrong_date_rows = frame.filter(
        (pl.col("date") < pl.lit(start)) | (pl.col("date") > pl.lit(end))
    ).height
    too_many_bars = frame.group_by("date").len().filter(pl.col("len") > 270).height
    failures = {
        "duplicate_timestamps": duplicate_timestamps,
        "out_of_session_rows": out_of_session_rows,
        "non_minute_rows": non_minute_rows,
        "wrong_symbol_rows": wrong_symbol_rows,
        "wrong_date_rows": wrong_date_rows,
        "sessions_over_270_bars": too_many_bars,
    }
    if any(failures.values()):
        raise RuntimeError(f"invalid minute Kbars for {symbol}: {failures}")
    return {
        "rows": frame.height,
        "sessions": frame["date"].n_unique(),
        "first_ts": str(frame["ts"].min()),
        "last_ts": str(frame["ts"].max()),
        **failures,
    }


def minute_receipt_valid(
    path: Path,
    *,
    symbol: str,
    start: date,
    end: date,
    simulation: bool | None = None,
) -> bool:
    payload = _read_json(path)
    if payload is None or not (
        payload.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and payload.get("source") == SOURCE_NAME
        and payload.get("storage_frequency") == STORAGE_FREQUENCY
        and payload.get("symbol") == symbol
        and payload.get("start_date") == start.isoformat()
        and payload.get("end_date") == end.isoformat()
        and payload.get("status") in {"ok", "empty", "source_gap"}
        and (simulation is None or payload.get("simulation") is bool(simulation))
    ):
        return False
    if payload["status"] == "empty":
        return int(payload.get("rows", -1)) == 0
    if payload["status"] == "source_gap" and not payload.get("source_gap_dates"):
        return False
    if int(payload.get("rows", -1)) == 0:
        return payload["status"] == "source_gap"
    output = payload.get("output_receipt")
    if not isinstance(output, dict):
        return False
    output_path = Path(str(output.get("path", "")))
    try:
        return (
            output_path.is_file()
            and int(output.get("size", -1)) == output_path.stat().st_size
            and str(output.get("sha256", "")) == _sha256(output_path)
        )
    except OSError:
        return False


def _symbols_from_csv(path: Path) -> set[str]:
    if not path.is_file():
        raise FileNotFoundError(f"minute universe CSV does not exist: {path}")
    frame = pl.read_csv(path, infer_schema_length=0)
    column = "symbol" if "symbol" in frame.columns else "code"
    if column not in frame.columns:
        raise ValueError(f"{path} requires a symbol or code column")
    return {
        str(value or "").strip().upper()
        for value in frame[column].to_list()
        if str(value or "").strip()
    }


def select_universe(
    universe: list[UniverseRow],
    *,
    symbols: str,
    universe_csv: Path | None,
    all_symbols: bool,
    max_symbols: int,
) -> list[UniverseRow]:
    modes = sum(
        (
            bool(str(symbols).strip()),
            universe_csv is not None,
            bool(all_symbols),
        )
    )
    if modes != 1:
        raise ValueError(
            "select exactly one of --symbols, --universe-csv, or --all-symbols"
        )
    requested = (
        {item.strip().upper() for item in str(symbols).split(",") if item.strip()}
        if str(symbols).strip()
        else (_symbols_from_csv(universe_csv) if universe_csv is not None else None)
    )
    known = {row.symbol for row in universe}
    if requested is not None:
        unknown = sorted(requested - known)
        if unknown:
            raise ValueError(
                f"requested symbols are absent from the public universe: {unknown}"
            )
        selected = [row for row in universe if row.symbol in requested]
    else:
        selected = list(universe)
    if max_symbols > 0:
        selected = selected[:max_symbols]
    return selected


def stock_contract_map(api: Any) -> dict[str, Any]:
    """Load only Taiwan stock/ETF Base contracts, never derivative families."""

    bases = api.contracts.list("STK", region="TW")
    result = {
        str(getattr(base, "code", "") or "").strip().upper(): base for base in bases
    }
    result.pop("", None)
    if not result:
        raise RuntimeError("Shioaji returned no Taiwan STK Base contracts")
    return result


def contract_for_stock_symbol(
    api: Any,
    row: UniverseRow,
    contracts_by_code: dict[str, Any],
) -> tuple[Any | None, float, str]:
    contract = contracts_by_code.get(row.symbol)
    if contract is None:
        return None, 0.0, "stock_contract_not_found"
    security_type = str(
        getattr(getattr(contract, "security_type", ""), "value", "")
        or getattr(contract, "security_type", "")
    ).upper()
    if security_type not in {"STK", "STOCK"}:
        return None, 0.0, f"unexpected_security_type={security_type}"
    exchange = str(
        getattr(getattr(contract, "exchange", ""), "value", "")
        or getattr(contract, "exchange", "")
    ).upper()
    if exchange not in {"TSE", "OTC"}:
        return None, 0.0, f"unexpected_exchange={exchange}"
    info = api.contracts.info(contract)
    unit = float(getattr(info, "unit", 0.0) or 0.0)
    if not math.isfinite(unit) or unit <= 0.0:
        return None, 0.0, f"invalid_contract_unit={unit}"
    market = "twse" if exchange == "TSE" else "tpex"
    if row.market and row.market not in {market, exchange.lower()}:
        return None, 0.0, f"market_mismatch=public:{row.market},shioaji:{market}"
    return contract, unit, ""


def _write_symbol_manifest(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
    simulation: bool,
) -> SymbolResult:
    entries: list[dict[str, Any]] = []
    total_rows = 0
    dates: set[date] = set()
    source_gap_dates: set[date] = set()
    for chunk_start, chunk_end in chunks:
        data_path, receipt_path = minute_chunk_paths(
            output_dir, row.symbol, chunk_start, chunk_end
        )
        if not minute_receipt_valid(
            receipt_path,
            symbol=row.symbol,
            start=chunk_start,
            end=chunk_end,
            simulation=simulation,
        ):
            raise RuntimeError(f"incomplete minute chunk receipt: {receipt_path}")
        receipt = _read_json(receipt_path)
        assert receipt is not None
        total_rows += int(receipt.get("rows", 0))
        entries.append(
            {
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "status": receipt["status"],
                "rows": int(receipt.get("rows", 0)),
                "sessions": int(receipt.get("sessions", 0)),
                "data_path": (
                    str(data_path)
                    if receipt["status"] in {"ok", "source_gap"}
                    and int(receipt.get("rows", 0)) > 0
                    else None
                ),
                "data_sha256": (
                    receipt["output_receipt"]["sha256"]
                    if receipt["status"] in {"ok", "source_gap"}
                    and int(receipt.get("rows", 0)) > 0
                    else None
                ),
                "receipt_path": str(receipt_path),
                "source_gap_dates": list(receipt.get("source_gap_dates", [])),
            }
        )
        for raw in receipt.get("returned_dates", []):
            dates.add(date.fromisoformat(str(raw)))
        for raw in receipt.get("source_gap_dates", []):
            source_gap_dates.add(date.fromisoformat(str(raw)))
    manifest_path = output_dir / "symbols" / f"{row.symbol}.manifest.json"
    _atomic_write_json(
        manifest_path,
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "simulation": simulation,
            "symbol": row.symbol,
            "name": row.name,
            "market": row.market,
            "security_type": row.security_type,
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "chunks": entries,
            "minute_rows": total_rows,
            "sessions": len(dates),
            "source_gap_sessions": len(source_gap_dates),
            "source_gap_dates": [
                value.isoformat() for value in sorted(source_gap_dates)
            ],
            "first_date": min(dates).isoformat() if dates else None,
            "last_date": max(dates).isoformat() if dates else None,
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    return SymbolResult(
        symbol=row.symbol,
        status=("complete_with_source_gaps" if source_gap_dates else "complete"),
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=total_rows,
        daily_rows=len(dates),
        first_date=min(dates).isoformat() if dates else None,
        last_date=max(dates).isoformat() if dates else None,
        output_path=str(manifest_path),
        message=(
            f"source_gap_sessions={len(source_gap_dates)}" if source_gap_dates else ""
        ),
    )


def completed_symbol_manifest_result(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
    simulation: bool,
) -> SymbolResult | None:
    """Fast restart path for an already sealed symbol.

    Chunk hashes are verified when the manifest is first written and again by
    the research dataset builder. Rehashing every completed symbol on every
    quota-window restart would turn a multi-day full-market backfill into an
    increasingly expensive O(completed data) scan.
    """

    manifest_path = output_dir / "symbols" / f"{row.symbol}.manifest.json"
    payload = _read_json(manifest_path)
    if payload is None or not (
        payload.get("schema_version") == MANIFEST_SCHEMA_VERSION
        and payload.get("source") == SOURCE_NAME
        and payload.get("storage_frequency") == STORAGE_FREQUENCY
        and payload.get("simulation") is simulation
        and payload.get("symbol") == row.symbol
        and payload.get("requested_start") == requested_start.isoformat()
        and payload.get("requested_end") == requested_end.isoformat()
    ):
        return None
    entries = payload.get("chunks")
    if not isinstance(entries, list) or len(entries) != len(chunks):
        return None
    for entry, (chunk_start, chunk_end) in zip(entries, chunks, strict=True):
        if not isinstance(entry, dict) or not (
            entry.get("start_date") == chunk_start.isoformat()
            and entry.get("end_date") == chunk_end.isoformat()
            and entry.get("status") in {"ok", "empty", "source_gap"}
        ):
            return None
        receipt_path = Path(str(entry.get("receipt_path", "")))
        if not receipt_path.is_file():
            return None
        if entry["status"] in {"ok", "source_gap"} and int(entry.get("rows", 0)) > 0:
            data_path = Path(str(entry.get("data_path", "")))
            if not (
                data_path.is_file()
                and int(entry.get("rows", 0)) > 0
                and str(entry.get("data_sha256", ""))
            ):
                return None
    return SymbolResult(
        symbol=row.symbol,
        status=(
            "complete_with_source_gaps"
            if int(payload.get("source_gap_sessions", 0)) > 0
            else "complete"
        ),
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=int(payload.get("minute_rows", 0)),
        daily_rows=int(payload.get("sessions", 0)),
        first_date=payload.get("first_date"),
        last_date=payload.get("last_date"),
        output_path=str(manifest_path),
        message=(
            "resumed_from_sealed_manifest"
            + (
                f";source_gap_sessions={int(payload.get('source_gap_sessions', 0))}"
                if int(payload.get("source_gap_sessions", 0)) > 0
                else ""
            )
        ),
    )


def restore_extended_tail_from_archived_manifest(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
    simulation: bool,
    expected_dates: set[date],
) -> bool:
    """Repack a covered archived tail when a delisted contract disappears.

    Contract V2 is a current contract directory, so a delisted symbol can
    disappear even though its already archived history remains valid. When an
    end-date extension only changes the final chunk boundary and all expected
    public trading dates are covered by the previous sealed tail (or its
    explicit source-gap dates), repack that immutable tail into the new chunk
    instead of relabeling the entire historical symbol as unavailable.
    """

    manifest_path = output_dir / "symbols" / f"{row.symbol}.manifest.json"
    archived = _read_json(manifest_path)
    if archived is None or not (
        archived.get("schema_version") == MANIFEST_SCHEMA_VERSION
        and archived.get("source") == SOURCE_NAME
        and archived.get("storage_frequency") == STORAGE_FREQUENCY
        and archived.get("simulation") is simulation
        and archived.get("symbol") == row.symbol
        and archived.get("requested_start") == requested_start.isoformat()
    ):
        return False
    old_chunks = archived.get("chunks", [])
    if not old_chunks or not chunks:
        return False
    missing = [
        (start, end)
        for start, end in chunks
        if not minute_receipt_valid(
            minute_chunk_paths(output_dir, row.symbol, start, end)[1],
            symbol=row.symbol,
            start=start,
            end=end,
            simulation=simulation,
        )
    ]
    if len(missing) != 1 or missing[0] != chunks[-1]:
        return False
    new_start, new_end = missing[0]
    old_tail = old_chunks[-1]
    if not (
        old_tail.get("start_date") == new_start.isoformat()
        and date.fromisoformat(str(old_tail.get("end_date"))) < new_end
        and old_tail.get("status") in {"ok", "source_gap"}
        and old_tail.get("data_path")
    ):
        return False
    old_path = Path(str(old_tail["data_path"]))
    if not old_path.is_file() or _sha256(old_path) != str(
        old_tail.get("data_sha256", "")
    ):
        return False
    frame = pl.read_parquet(old_path).filter(
        (pl.col("date") >= pl.lit(new_start)) & (pl.col("date") <= pl.lit(new_end))
    )
    returned = set(frame["date"].to_list()) if frame.height else set()
    source_gaps = {
        date.fromisoformat(str(value)) for value in old_tail.get("source_gap_dates", [])
    }
    expected_tail = {value for value in expected_dates if new_start <= value <= new_end}
    if not expected_tail.issubset(returned | source_gaps):
        return False
    units = frame["contract_unit"].unique().to_list() if frame.height else []
    if len(units) != 1 or not math.isfinite(float(units[0])) or float(units[0]) <= 0:
        return False
    unit = float(units[0])
    audit = validate_minute_kbars(
        frame,
        symbol=row.symbol,
        start=new_start,
        end=new_end,
    )
    data_path, receipt_path = minute_chunk_paths(
        output_dir, row.symbol, new_start, new_end
    )
    output_receipt = _write_minute_parquet(frame, data_path) if frame.height else None
    gap_dates = [value.isoformat() for value in sorted(source_gaps & expected_tail)]
    receipt_status = "source_gap" if gap_dates else ("ok" if frame.height else "empty")
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "simulation": simulation,
            "symbol": row.symbol,
            "name": row.name,
            "market": row.market,
            "security_type": row.security_type,
            "contract_unit": unit,
            "start_date": new_start.isoformat(),
            "end_date": new_end.isoformat(),
            "status": receipt_status,
            "rows": frame.height,
            "sessions": audit["sessions"],
            "first_ts": audit["first_ts"],
            "last_ts": audit["last_ts"],
            "expected_positive_volume_sessions": len(expected_tail),
            "returned_dates": [value.isoformat() for value in sorted(returned)],
            "query_performed": False,
            "query_skipped_reason": "archived_delisted_contract_tail_repacked",
            "restored_from": str(old_path),
            "zero_placeholder_rows_dropped": 0,
            "negative_correction_rows_dropped": 0,
            "out_of_session_rows_dropped": 0,
            "outside_reference_date_rows_dropped": 0,
            "single_day_fallback_queries": 0,
            "source_gap_dates": gap_dates,
            "audit": audit,
            "output_receipt": output_receipt,
            "downloaded_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    return True


def _write_run_summary(
    output_dir: Path,
    *,
    args: argparse.Namespace,
    selected: list[UniverseRow],
    results: list[SymbolResult],
    traffic: tuple[int, int] | None,
    stopped_for_traffic: bool,
    stopped_for_market_hours: bool,
    counters: dict[str, int],
    rate: dict[str, float | int],
    fatal_error: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    strict_complete = len(results) == len(selected) and all(
        item.status == "complete" for item in results
    )
    collection_complete = len(results) == len(selected) and all(
        item.status in {"complete", "complete_with_source_gaps", "contract_unavailable"}
        for item in results
    )
    # An interrupted extension is run progress, not a new canonical catalog.
    # Keep the last terminal summary/report published until the full selected
    # universe reaches a terminal state for the new requested end date.
    report_path = output_dir / (
        "download_report.csv" if collection_complete else "latest_run_report.csv"
    )
    if results:
        pl.DataFrame([asdict(item) for item in results]).sort("symbol").write_csv(
            report_path
        )
    else:
        pl.DataFrame(
            schema={name: pl.String for name in SymbolResult.__dataclass_fields__}
        ).write_csv(report_path)
    summary_path = output_dir / (
        "download_summary.json" if collection_complete else "latest_run_summary.json"
    )
    _atomic_write_json(
        summary_path,
        {
            "schema_version": 1,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "start_date": str(args.start_date),
            "end_date": str(args.end_date),
            "chunk_days": int(args.chunk_days),
            "simulation": bool(args.simulation),
            "workers": int(args.workers),
            "requests_per_second_limit": float(args.requests_per_second),
            "quote_limit_requests": SHIOAJI_QUOTE_LIMIT_REQUESTS,
            "quote_limit_window_seconds": SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS,
            "api_requests_started_this_run": int(rate["total_requests"]),
            "observed_request_start_rps": float(rate["overall_rps"]),
            "processed_chunks_this_run": int(counters["processed_chunks"]),
            "queried_chunks_this_run": int(counters["queried_chunks"]),
            "skipped_empty_chunks_this_run": int(counters["skipped_empty_chunks"]),
            "selected_symbols": len(selected),
            "reported_symbols": len(results),
            "complete_symbols": sum(x.status == "complete" for x in results),
            "complete_with_source_gap_symbols": sum(
                x.status == "complete_with_source_gaps" for x in results
            ),
            "contract_unavailable_symbols": sum(
                x.status == "contract_unavailable" for x in results
            ),
            "failed_symbols": sum(x.status == "failed" for x in results),
            "partial_symbols": sum(x.status == "partial" for x in results),
            "selected_coverage_complete": strict_complete,
            "resumable_collection_complete": collection_complete,
            "published_terminal_catalog": collection_complete,
            "stopped_for_traffic": stopped_for_traffic,
            "stopped_for_market_hours": stopped_for_market_hours,
            "fatal_error": fatal_error or None,
            "traffic_used_bytes": traffic[0] if traffic else None,
            "traffic_limit_bytes": traffic[1] if traffic else None,
            "report_path": str(report_path),
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    return summary_path


def _partial_symbol_result(
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    completed: int,
    message: str,
) -> SymbolResult:
    return SymbolResult(
        symbol=row.symbol,
        status="partial",
        chunks_total=len(chunks),
        chunks_complete=completed,
        source_minute_rows=0,
        daily_rows=0,
        first_date=None,
        last_date=None,
        output_path="",
        message=message,
    )


def _download_symbol(
    api: Any,
    contracts_by_code: dict[str, Any],
    row: UniverseRow,
    *,
    symbol_index: int,
    selected_count: int,
    worker_index: int,
    args: argparse.Namespace,
    start: date,
    end: date,
    chunks: list[tuple[date, date]],
    limiter: SharedRequestRateLimiter,
    host_rate_limiter: SharedRateLimiter,
    counters: SharedDownloadCounters,
    traffic_guard: SharedTrafficBudgetGuard,
    stop_event: Any,
    stopped_for_traffic: Any,
    stopped_for_market_hours: Any,
) -> SymbolResult:
    completed = 0
    try:
        sealed_result = completed_symbol_manifest_result(
            args.output_dir,
            row,
            chunks,
            requested_start=start,
            requested_end=end,
            simulation=bool(args.simulation),
        )
        if sealed_result is not None:
            return sealed_result
        completed = sum(
            minute_receipt_valid(
                minute_chunk_paths(args.output_dir, row.symbol, a, b)[1],
                symbol=row.symbol,
                start=a,
                end=b,
                simulation=bool(args.simulation),
            )
            for a, b in chunks
        )
        if completed == len(chunks):
            return _write_symbol_manifest(
                args.output_dir,
                row,
                chunks,
                requested_start=start,
                requested_end=end,
                simulation=bool(args.simulation),
            )
        expected_all = _positive_volume_dates(row.base_path, start, end)
        contract: Any | None = None
        unit = 0.0
        contract_message = ""
        if expected_all:
            contract, unit, contract_message = contract_for_stock_symbol(
                api, row, contracts_by_code
            )
        if expected_all and contract is None:
            restored = restore_extended_tail_from_archived_manifest(
                args.output_dir,
                row,
                chunks,
                requested_start=start,
                requested_end=end,
                simulation=bool(args.simulation),
                expected_dates=expected_all,
            )
            if restored:
                _emit_worker_log(
                    f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
                    f"symbol_index={symbol_index}/{selected_count} "
                    "status=archived_delisted_tail_repacked"
                )
                return _write_symbol_manifest(
                    args.output_dir,
                    row,
                    chunks,
                    requested_start=start,
                    requested_end=end,
                    simulation=bool(args.simulation),
                )
            _emit_worker_log(
                f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
                f"symbol_index={symbol_index}/{selected_count} "
                f"status=contract_unavailable reason={contract_message}"
            )
            return SymbolResult(
                symbol=row.symbol,
                status="contract_unavailable",
                chunks_total=len(chunks),
                chunks_complete=completed,
                source_minute_rows=0,
                daily_rows=0,
                first_date=None,
                last_date=None,
                output_path="",
                message=contract_message,
            )

        def acquire_request_slot() -> None:
            limiter.acquire(stop_event)
            host_rate_limiter.wait()

        for chunk_index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            data_path, receipt_path = minute_chunk_paths(
                args.output_dir, row.symbol, chunk_start, chunk_end
            )
            if minute_receipt_valid(
                receipt_path,
                symbol=row.symbol,
                start=chunk_start,
                end=chunk_end,
                simulation=bool(args.simulation),
            ):
                continue
            if stop_event.is_set():
                raise DownloadStopRequested("another worker requested a global stop")
            if _taiwan_market_hours_now() and not args.allow_market_hours:
                raise MarketHoursReached(
                    "Taiwan market-hours safety window reached; "
                    "resume after 14:30 Asia/Taipei"
                )
            expected_dates = {
                value for value in expected_all if chunk_start <= value <= chunk_end
            }
            query_performed = bool(expected_dates)
            if query_performed:
                traffic_guard.check(api)
                assert contract is not None
                frame, query_audit = query_minute_chunk(
                    api,
                    contract,
                    row,
                    contract_unit=unit,
                    start=chunk_start,
                    end=chunk_end,
                    timeout_ms=int(args.timeout_ms),
                    retries=int(args.retries),
                    retry_backoff=float(args.retry_backoff),
                    expected_dates=expected_dates,
                    request_started=acquire_request_slot,
                )
            else:
                # The public point-in-time panel is the universe and coverage
                # reference. With no positive-volume session, this chunk cannot
                # contribute a tradable minute bar.
                frame = pl.DataFrame()
                query_audit = {
                    "zero_placeholder_rows_dropped": 0,
                    "negative_correction_rows_dropped": 0,
                    "out_of_session_rows_dropped": 0,
                    "outside_reference_date_rows_dropped": 0,
                    "single_day_fallback_queries": 0,
                    "source_gap_dates": [],
                }
            audit = validate_minute_kbars(
                frame,
                symbol=row.symbol,
                start=chunk_start,
                end=chunk_end,
            )
            returned_dates = sorted(
                value.isoformat()
                for value in (set(frame["date"].to_list()) if frame.height else set())
            )
            output_receipt = (
                _write_minute_parquet(frame, data_path) if frame.height else None
            )
            source_gap_dates = list(query_audit["source_gap_dates"])
            receipt_status = (
                "source_gap"
                if source_gap_dates
                else ("ok" if frame.height else "empty")
            )
            _atomic_write_json(
                receipt_path,
                {
                    "schema_version": RECEIPT_SCHEMA_VERSION,
                    "source": SOURCE_NAME,
                    "storage_frequency": STORAGE_FREQUENCY,
                    "simulation": bool(args.simulation),
                    "symbol": row.symbol,
                    "name": row.name,
                    "market": row.market,
                    "security_type": row.security_type,
                    "contract_unit": unit,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "status": receipt_status,
                    "rows": frame.height,
                    "sessions": audit["sessions"],
                    "first_ts": audit["first_ts"],
                    "last_ts": audit["last_ts"],
                    "expected_positive_volume_sessions": len(expected_dates),
                    "returned_dates": returned_dates,
                    "query_performed": query_performed,
                    "query_skipped_reason": (
                        None if query_performed else "no_public_positive_volume_session"
                    ),
                    "zero_placeholder_rows_dropped": int(
                        query_audit["zero_placeholder_rows_dropped"]
                    ),
                    "negative_correction_rows_dropped": int(
                        query_audit["negative_correction_rows_dropped"]
                    ),
                    "out_of_session_rows_dropped": int(
                        query_audit["out_of_session_rows_dropped"]
                    ),
                    "outside_reference_date_rows_dropped": int(
                        query_audit["outside_reference_date_rows_dropped"]
                    ),
                    "single_day_fallback_queries": int(
                        query_audit.get("single_day_fallback_queries", 0)
                    ),
                    "source_gap_dates": source_gap_dates,
                    "audit": audit,
                    "output_receipt": output_receipt,
                    "downloaded_at_utc": datetime.now(timezone.utc)
                    .replace(microsecond=0)
                    .isoformat(),
                },
            )
            completed += 1
            chunk_counts = counters.record_chunk(query_performed=query_performed)
            rate = limiter.snapshot()
            _emit_worker_log(
                f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
                f"symbol_index={symbol_index}/{selected_count} "
                f"chunk={chunk_index}/{len(chunks)} rows={frame.height} "
                f"api_queried={str(query_performed).lower()} "
                f"source_gaps={len(source_gap_dates)} "
                f"queried={chunk_counts['queried_chunks']} "
                f"skipped_empty={chunk_counts['skipped_empty_chunks']} "
                f"api_requests={rate['total_requests']} "
                f"request_rps={float(rate['overall_rps']):.3f} "
                f"window_rps={float(rate['window_rps']):.3f}"
            )
            if query_performed and float(args.request_interval) > 0.0:
                time.sleep(float(args.request_interval))
        return _write_symbol_manifest(
            args.output_dir,
            row,
            chunks,
            requested_start=start,
            requested_end=end,
            simulation=bool(args.simulation),
        )
    except TrafficBudgetReached as exc:
        stopped_for_traffic.value = 1
        stop_event.set()
        _emit_worker_log(
            f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
            f"symbol_index={symbol_index}/{selected_count} "
            f"status=stopped_for_traffic reason={exc}"
        )
        return _partial_symbol_result(
            row, chunks, completed=completed, message=str(exc)
        )
    except MarketHoursReached as exc:
        stopped_for_market_hours.value = 1
        stop_event.set()
        _emit_worker_log(
            f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
            f"symbol_index={symbol_index}/{selected_count} "
            f"status=stopped_for_market_hours reason={exc}"
        )
        return _partial_symbol_result(
            row, chunks, completed=completed, message=str(exc)
        )
    except DownloadStopRequested as exc:
        return _partial_symbol_result(
            row, chunks, completed=completed, message=str(exc)
        )
    except Exception as exc:
        _emit_worker_log(
            f"[shioaji-minute] worker={worker_index} symbol={row.symbol} "
            f"symbol_index={symbol_index}/{selected_count} "
            f"status=failed error={type(exc).__name__}: {exc}"
        )
        return SymbolResult(
            symbol=row.symbol,
            status="failed",
            chunks_total=len(chunks),
            chunks_complete=completed,
            source_minute_rows=0,
            daily_rows=0,
            first_date=None,
            last_date=None,
            output_path="",
            message=f"{type(exc).__name__}: {exc}",
        )


def _parallel_download_worker(
    worker_index: int,
    args: argparse.Namespace,
    start: date,
    end: date,
    chunks: list[tuple[date, date]],
    selected_count: int,
    task_queue: Any,
    result_queue: Any,
    error_queue: Any,
    ready_barrier: Any,
    start_event: Any,
    stop_event: Any,
    init_failures: Any,
    runtime_failures: Any,
    stopped_for_traffic: Any,
    stopped_for_market_hours: Any,
    limiter: SharedRequestRateLimiter,
    counters: SharedDownloadCounters,
    traffic_guard: SharedTrafficBudgetGuard,
) -> None:
    api: Any | None = None
    contracts_by_code: dict[str, Any] = {}
    host_rate_limiter = SharedRateLimiter(
        1.0 / DEFAULT_REQUESTS_PER_SECOND,
        name="shioaji_quote_query",
    )
    init_error = ""
    try:
        import shioaji as sj

        api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
        secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
        api = sj.Shioaji(simulation=bool(args.simulation))
        api.set_event_callback(lambda _code, _event_code, _info, _event: None)
        api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
        contracts_by_code = stock_contract_map(api)
        _emit_worker_log(
            f"[shioaji-minute] worker={worker_index} login=ok "
            f"stock_contracts={len(contracts_by_code)}"
        )
    except Exception as exc:
        init_error = f"worker={worker_index} {type(exc).__name__}: {exc}"
        with init_failures.get_lock():
            init_failures.value += 1
        error_queue.put(("init", init_error))
    try:
        ready_barrier.wait(timeout=120.0)
        start_event.wait()
        if init_error or stop_event.is_set():
            return
        while not stop_event.is_set():
            task = task_queue.get()
            if task is None:
                break
            symbol_index, row = task
            result = _download_symbol(
                api,
                contracts_by_code,
                row,
                symbol_index=int(symbol_index),
                selected_count=selected_count,
                worker_index=worker_index,
                args=args,
                start=start,
                end=end,
                chunks=chunks,
                limiter=limiter,
                host_rate_limiter=host_rate_limiter,
                counters=counters,
                traffic_guard=traffic_guard,
                stop_event=stop_event,
                stopped_for_traffic=stopped_for_traffic,
                stopped_for_market_hours=stopped_for_market_hours,
            )
            result_queue.put((int(symbol_index), result))
    except Exception as exc:
        message = f"worker={worker_index} {type(exc).__name__}: {exc}"
        with runtime_failures.get_lock():
            runtime_failures.value += 1
        error_queue.put(("runtime", message))
        stop_event.set()
    finally:
        if api is not None:
            try:
                api.logout()
            except Exception:
                pass


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(str(args.start_date))
    end = date.fromisoformat(str(args.end_date))
    if start < SHIOAJI_STOCK_HISTORY_START:
        raise ValueError(
            f"Shioaji stock history starts at {SHIOAJI_STOCK_HISTORY_START}; got {start}"
        )
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    if not 1 <= int(args.chunk_days) <= MAX_KBAR_QUERY_DAYS:
        raise ValueError("--chunk-days must be between 1 and 30")
    if not 1 <= int(args.workers) <= SHIOAJI_MAX_CONNECTIONS:
        raise ValueError(f"--workers must be between 1 and {SHIOAJI_MAX_CONNECTIONS}")
    if not 0.0 < float(args.requests_per_second) <= DEFAULT_REQUESTS_PER_SECOND:
        raise ValueError(
            "--requests-per-second must be positive and no greater than "
            f"{DEFAULT_REQUESTS_PER_SECOND:g}"
        )
    if float(args.request_interval) < 0.0:
        raise ValueError("--request-interval must be nonnegative")
    if float(args.traffic_check_interval) <= 0.0:
        raise ValueError("--traffic-check-interval must be positive")
    if not 0 < float(args.max_traffic_fraction) < 1:
        raise ValueError("--max-traffic-fraction must be between 0 and 1")
    if not math.isfinite(float(args.traffic_reserve_mb)) or float(
        args.traffic_reserve_mb
    ) < 0.0:
        raise ValueError("--traffic-reserve-mb must be finite and nonnegative")

    universe = _load_universe(args.base_stock_root)
    selected = select_universe(
        universe,
        symbols=str(args.symbols),
        universe_csv=args.universe_csv,
        all_symbols=bool(args.all_symbols),
        max_symbols=int(args.max_symbols),
    )
    chunks = list(iter_date_chunks(start, end, int(args.chunk_days)))
    if args.dry_run:
        api_query_chunks = 0
        for row in selected:
            expected_dates = _positive_volume_dates(row.base_path, start, end)
            api_query_chunks += len(
                {
                    (value - start).days // int(args.chunk_days)
                    for value in expected_dates
                    if start <= value <= end
                }
            )
        print(
            f"[shioaji-minute] dry_run symbols={len(selected)} "
            f"receipt_chunks={len(selected) * len(chunks)} "
            f"api_query_chunks={api_query_chunks} range={start}..{end} "
            f"workers={int(args.workers)} "
            f"requests_per_second={float(args.requests_per_second):g} "
            f"output={args.output_dir}",
            flush=True,
        )
        return
    if _taiwan_market_hours_now() and not args.allow_market_hours:
        raise RuntimeError(
            "Refusing historical minute backfill during Taiwan market hours "
            "(07:45-14:31 live-priority window)."
        )
    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError(
            "Set SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY locally; credentials "
            "are intentionally not accepted as command-line arguments."
        )

    context = mp.get_context("spawn")
    workers = int(args.workers)
    task_queue = context.Queue()
    result_queue = context.Queue()
    error_queue = context.Queue()
    for symbol_index, row in enumerate(selected, start=1):
        task_queue.put((symbol_index, row))
    for _ in range(workers):
        task_queue.put(None)

    ready_barrier = context.Barrier(workers + 1)
    start_event = context.Event()
    stop_event = context.Event()
    init_failures = context.Value("i", 0)
    runtime_failures = context.Value("i", 0)
    stopped_for_traffic = context.Value("b", 0)
    stopped_for_market_hours = context.Value("b", 0)
    limiter = SharedRequestRateLimiter(
        context,
        requests_per_second=float(args.requests_per_second),
    )
    counters = SharedDownloadCounters(context)
    reserve_bytes = int(float(args.traffic_reserve_mb) * 1024 * 1024)
    traffic_guard = SharedTrafficBudgetGuard(
        context,
        max_fraction=float(args.max_traffic_fraction),
        reserve_bytes=reserve_bytes,
        check_interval_seconds=float(args.traffic_check_interval),
    )
    processes = [
        context.Process(
            target=_parallel_download_worker,
            name=f"shioaji-minute-{worker_index}",
            args=(
                worker_index,
                args,
                start,
                end,
                chunks,
                len(selected),
                task_queue,
                result_queue,
                error_queue,
                ready_barrier,
                start_event,
                stop_event,
                init_failures,
                runtime_failures,
                stopped_for_traffic,
                stopped_for_market_hours,
                limiter,
                counters,
                traffic_guard,
            ),
        )
        for worker_index in range(1, workers + 1)
    ]

    started = time.monotonic()
    result_items: list[tuple[int, SymbolResult]] = []
    error_items: list[tuple[str, str]] = []
    fatal_error = ""
    try:
        for process in processes:
            process.start()
        try:
            ready_barrier.wait(timeout=180.0)
        except Exception as exc:
            fatal_error = f"worker readiness failed: {type(exc).__name__}: {exc}"
            stop_event.set()
        if init_failures.value:
            fatal_error = (
                f"{int(init_failures.value)}/{workers} Shioaji workers "
                "failed login or contract initialization"
            )
            stop_event.set()
        print(
            f"[shioaji-minute] parallel_workers={workers} "
            f"requests_per_second={float(args.requests_per_second):g} "
            f"sliding_window={SHIOAJI_QUOTE_LIMIT_REQUESTS}/"
            f"{SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS:g}s "
            f"start_allowed={str(not stop_event.is_set()).lower()}",
            flush=True,
        )
        start_event.set()

        while any(process.is_alive() for process in processes):
            try:
                result_items.append(result_queue.get(timeout=0.5))
            except Empty:
                pass
        for process in processes:
            process.join(timeout=5.0)
        while True:
            try:
                result_items.append(result_queue.get_nowait())
            except Empty:
                break
        while True:
            try:
                error_items.append(error_queue.get_nowait())
            except Empty:
                break
        bad_exits = [
            f"{process.name}={process.exitcode}"
            for process in processes
            if process.exitcode not in {0, None}
        ]
        if bad_exits and not fatal_error:
            fatal_error = "worker exits: " + ",".join(bad_exits)
        if runtime_failures.value and not fatal_error:
            fatal_error = (
                f"{int(runtime_failures.value)} parallel worker runtime failure(s)"
            )
        if error_items:
            detail = "; ".join(message for _, message in error_items)
            fatal_error = f"{fatal_error}; {detail}".strip("; ")
    finally:
        stop_event.set()
        start_event.set()
        for process in processes:
            if process.is_alive():
                process.join(timeout=10.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5.0)

    # A quota stop can leave thousands of unconsumed tasks in the parent-side
    # multiprocessing feeder buffer.  Do not let Queue's interpreter-exit join
    # turn a completed run into a hung systemd service.
    for queue in (task_queue, result_queue, error_queue):
        try:
            queue.cancel_join_thread()
            queue.close()
        except (AttributeError, OSError, ValueError):
            pass

    deduplicated = {int(symbol_index): result for symbol_index, result in result_items}
    results = [deduplicated[index] for index in sorted(deduplicated)]
    traffic = traffic_guard.last_usage()
    counter_snapshot = counters.snapshot()
    rate_snapshot = limiter.snapshot()
    elapsed_seconds = round(time.monotonic() - started, 3)
    published_summary_path = _write_run_summary(
        args.output_dir,
        args=args,
        selected=selected,
        results=results,
        traffic=traffic,
        stopped_for_traffic=bool(stopped_for_traffic.value),
        stopped_for_market_hours=bool(stopped_for_market_hours.value),
        counters=counter_snapshot,
        rate=rate_snapshot,
        fatal_error=fatal_error,
    )
    state = (
        "failed"
        if fatal_error
        else (
            "stopped_for_traffic"
            if stopped_for_traffic.value
            else (
                "stopped_for_market_hours"
                if stopped_for_market_hours.value
                else "finished"
            )
        )
    )
    _atomic_write_json(
        args.output_dir / "progress.json",
        {
            "schema_version": 2,
            "state": state,
            "parallel_workers": workers,
            "requests_per_second_limit": float(args.requests_per_second),
            "selected_symbols": len(selected),
            "reported_symbols": len(results),
            "processed_chunks_this_run": counter_snapshot["processed_chunks"],
            "queried_chunks_this_run": counter_snapshot["queried_chunks"],
            "skipped_empty_chunks_this_run": counter_snapshot["skipped_empty_chunks"],
            "api_requests_started_this_run": int(rate_snapshot["total_requests"]),
            "observed_request_start_rps": float(rate_snapshot["overall_rps"]),
            "request_window_rps": float(rate_snapshot["window_rps"]),
            "elapsed_seconds": elapsed_seconds,
            "traffic_used_bytes": traffic[0] if traffic else None,
            "traffic_limit_bytes": traffic[1] if traffic else None,
            "fatal_error": fatal_error or None,
            "updated_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    print(
        f"[shioaji-minute] complete={sum(x.status == 'complete' for x in results)} "
        f"complete_with_gaps="
        f"{sum(x.status == 'complete_with_source_gaps' for x in results)} "
        f"failed={sum(x.status == 'failed' for x in results)} "
        f"partial={sum(x.status == 'partial' for x in results)} "
        f"api_requests={int(rate_snapshot['total_requests'])} "
        f"request_rps={float(rate_snapshot['overall_rps']):.3f} "
        f"summary={published_summary_path}",
        flush=True,
    )
    if fatal_error:
        raise RuntimeError(fatal_error)


if __name__ == "__main__":
    main()
