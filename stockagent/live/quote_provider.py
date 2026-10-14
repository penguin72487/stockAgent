from __future__ import annotations

import os
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote
import uuid
from zoneinfo import ZoneInfo

import numpy as np
import requests

from stockagent.live.shioaji_traffic_ledger import (
    QUERY_OBSERVATION_CONTRACT,
    record_avoided_query,
    shioaji_query,
)


_YAHOO_SESSION_LOCK = threading.Lock()
_YAHOO_SESSION: requests.Session | None = None
_YAHOO_CRUMB: str | None = None
_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}
_SHIOAJI_STOCK_LOCK = threading.RLock()
_SHIOAJI_STOCK_API: object | None = None
_SHIOAJI_STOCK_CONTRACTS: dict[str, object | None] = {}
_SHIOAJI_STOCK_CACHE: dict[str, tuple[float, dict[str, float | int | None]]] = {}
_SHIOAJI_STOCK_LOGIN_RETRY_AFTER = 0.0
_SHIOAJI_STOCK_LAST_LOGIN_ERROR: str | None = None
_SHIOAJI_STOCK_LOGIN_FAILURES = 0
_SHIOAJI_HISTORY_REQUEST_LOCK = threading.Lock()
_SHIOAJI_HISTORY_REQUEST_TIMES: deque[float] = deque()
_SHIOAJI_STREAM_LOCK = threading.RLock()
_SHIOAJI_STREAM_API: object | None = None
_SHIOAJI_STREAM_SUBSCRIPTIONS: set[tuple[str, str]] = set()
_SHIOAJI_STREAM_ROWS: dict[str, dict[str, dict[str, Any]]] = {}
_SHIOAJI_STREAM_ROTATION_AT = 0.0
_SHIOAJI_STREAM_CURSOR = 0
_SHIOAJI_STREAM_TARGET: tuple[str, ...] = ()
_SHIOAJI_STREAM_TARGET_READY = False
_SHIOAJI_STOCK_WAKEUP: StockQuoteWakeup | None = None
_SHIOAJI_OPENING_CONDITIONS: set[threading.Condition] = set()
_SHIOAJI_OPENING_REVISION = 0
# Shioaji permits at most 200 subscriptions. QuoteSTKv1 carries both best
# levels and the non-trial flag, so one subscription per stock is sufficient.
_SHIOAJI_STOCK_STREAM_LIMIT = 200
_SHIOAJI_STOCK_STREAM_DWELL_SECONDS = 5.0
_SHIOAJI_STOCK_STREAM_BATCH_LIMIT = 25
_TW_LIMIT_CACHE_LOCK = threading.Lock()
_TW_LIMIT_PREPARE_LOCK = threading.Lock()
_TW_LIMIT_CACHE_KEY: str | None = None
_TW_LIMIT_CACHE: dict[str, tuple[float | None, float | None, float | None]] = {}
_TW_MIS_BOOTSTRAP_LOCK = threading.Lock()
_TW_MIS_BOOTSTRAP_COOKIES: dict[str, str] = {}
_TW_MIS_BOOTSTRAP_AT = 0.0
# A bounded process-wide pool retains per-worker HTTP keep-alive connections.
# Recreating both for every sweep pays DNS/TCP/TLS setup on the critical path.
_TW_MIS_HTTP_WORKERS = 16
_TW_MIS_HTTP_POOL = ThreadPoolExecutor(
    max_workers=_TW_MIS_HTTP_WORKERS, thread_name_prefix="tw-mis-http"
)
_TW_MIS_HTTP_LOCAL = threading.local()
_TW_MIS_INFLIGHT_LOCK = threading.Lock()
_TW_MIS_INFLIGHT: dict[tuple[Any, ...], list[tuple[tuple[str, ...], Future]]] = {}
_TW_MIS_OPENING_LOCK = threading.RLock()
_TW_MIS_OPENING_CACHE_KEY: tuple[str, str] | None = None
_TW_MIS_OPENING_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_TW_MIS_OPENING_DIRTY = False
_TW_MIS_OPENING_REQUESTED: set[str] = set()
_TW_MIS_SESSION_LOCK = threading.Lock()
_TW_MIS_SESSION_CACHE_KEY: str | None = None
_TW_MIS_SESSION_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

_PRICE_SNAPSHOT_ARRAY_FIELDS = (
    "open_prices",
    "high_prices",
    "low_prices",
    "volumes",
    "upper_limit_prices",
    "lower_limit_prices",
    "bid_prices",
    "ask_prices",
    "bid_volumes",
    "ask_volumes",
    "reference_prices",
)


@dataclass(slots=True)
class PriceSnapshot:
    prices: np.ndarray
    source: str
    timestamp: str | None = None
    available_count: int = 0
    requested_count: int = 0
    available_mask: np.ndarray | None = None
    open_prices: np.ndarray | None = None
    high_prices: np.ndarray | None = None
    low_prices: np.ndarray | None = None
    volumes: np.ndarray | None = None
    upper_limit_prices: np.ndarray | None = None
    lower_limit_prices: np.ndarray | None = None
    bid_prices: np.ndarray | None = None
    ask_prices: np.ndarray | None = None
    bid_volumes: np.ndarray | None = None
    ask_volumes: np.ndarray | None = None
    reference_prices: np.ndarray | None = None
    timestamps_ms: np.ndarray | None = None
    # Exchange market-wall-clock time is separate from the local causal
    # observation time.  Shioaji Snapshot.ts is encoded so direct datetime
    # decoding yields Taiwan wall time; it is not a UTC epoch to shift by +8h.
    # timestamps_ms remains the true local response receipt time.
    exchange_timestamps_ms: np.ndarray | None = None
    # A dated MIS observation of zero session volume and absent open is an
    # observed absence, NOT an opening price. Never derive this from receipt
    # time alone, an empty response, or a stale broker snapshot.
    no_open_print_timestamps_ms: np.ndarray | None = None
    # -1 = provider did not expose the flag, 0 = regular trade, 1 = simulated
    # matching.  An explicit tri-state prevents an absent field from being
    # silently interpreted as a real exchange match.
    simtrade_flags: np.ndarray | None = None
    # Local transport measurements are observational metadata only.  They are
    # kept separate from exchange time and from per-symbol callback receipt
    # times so no wall-clock inference can be mistaken for market provenance.
    transport_timing: dict[str, Any] | None = None


def _day_trade_quote_broker_paths(
    state_dir: str | Path | None = None,
) -> tuple[Path, Path]:
    root = Path(
        state_dir
        or os.getenv(
            "TW_DAY_TRADE_STATE_DIR",
            "artifacts/live/tw_day_trade_simulation",
        )
    )
    broker = root / "quote_broker"
    requests_dir = broker / "requests"
    responses_dir = broker / "responses"
    requests_dir.mkdir(parents=True, exist_ok=True)
    responses_dir.mkdir(parents=True, exist_ok=True)
    return requests_dir, responses_dir


def day_trade_quote_broker_request_dir(
    state_dir: str | Path | None = None,
) -> Path:
    """Return the local inbox path so the engine can attach its inotify watch."""

    requests_dir, _responses_dir = _day_trade_quote_broker_paths(state_dir)
    return requests_dir


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _price_snapshot_payload(snapshot: PriceSnapshot) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "prices": np.asarray(snapshot.prices, dtype=np.float64).tolist(),
        "source": str(snapshot.source),
        "timestamp": snapshot.timestamp,
        "available_count": int(snapshot.available_count),
        "requested_count": int(snapshot.requested_count),
    }
    for name in (*_PRICE_SNAPSHOT_ARRAY_FIELDS, "available_mask"):
        values = getattr(snapshot, name)
        if values is not None:
            payload[name] = np.asarray(values).tolist()
    for name in ("timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms"):
        values = getattr(snapshot, name)
        if values is not None:
            payload[name] = np.asarray(values, dtype=np.int64).tolist()
    if snapshot.simtrade_flags is not None:
        payload["simtrade_flags"] = np.asarray(
            snapshot.simtrade_flags, dtype=np.int8
        ).tolist()
    if snapshot.transport_timing is not None:
        payload["transport_timing"] = dict(snapshot.transport_timing)
    return payload


def _price_snapshot_from_payload(payload: dict[str, Any]) -> PriceSnapshot:
    prices = np.asarray(payload.get("prices") or (), dtype=np.float64)
    keyword: dict[str, Any] = {
        "prices": prices,
        "source": str(payload.get("source") or "unknown"),
        "timestamp": payload.get("timestamp"),
        "available_count": int(payload.get("available_count") or 0),
        "requested_count": int(payload.get("requested_count") or len(prices)),
    }
    for name in _PRICE_SNAPSHOT_ARRAY_FIELDS:
        if payload.get(name) is not None:
            keyword[name] = np.asarray(payload[name], dtype=np.float64)
    if payload.get("available_mask") is not None:
        keyword["available_mask"] = np.asarray(
            payload["available_mask"], dtype=bool
        )
    for name in ("timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms"):
        if payload.get(name) is not None:
            keyword[name] = np.asarray(payload[name], dtype=np.int64)
    if payload.get("simtrade_flags") is not None:
        keyword["simtrade_flags"] = np.asarray(
            payload["simtrade_flags"], dtype=np.int8
        )
    if isinstance(payload.get("transport_timing"), dict):
        keyword["transport_timing"] = dict(payload["transport_timing"])
    return PriceSnapshot(**keyword)


def _is_shioaji_session_failure(exc: BaseException) -> bool:
    message = f"{type(exc).__name__}: {exc}".lower()
    return any(
        marker in message
        for marker in (
            "token is expired",
            "sessionnotestablished",
            "session not established",
            "not authenticated",
        )
    )


class _ShioajiSnapshotEmpty(RuntimeError):
    """A submitted non-blocking Snapshot request produced no usable rows."""


def _shioaji_stock_api() -> object:
    """Return one process-local, simulation-only Shioaji quote connection."""

    global _SHIOAJI_STOCK_API
    global _SHIOAJI_STOCK_LOGIN_FAILURES
    global _SHIOAJI_STOCK_LAST_LOGIN_ERROR
    global _SHIOAJI_STOCK_LOGIN_RETRY_AFTER
    with _SHIOAJI_STOCK_LOCK:
        if _SHIOAJI_STOCK_API is not None:
            return _SHIOAJI_STOCK_API
        now_monotonic = time.monotonic()
        if now_monotonic < _SHIOAJI_STOCK_LOGIN_RETRY_AFTER:
            retry_seconds = max(
                1,
                int(_SHIOAJI_STOCK_LOGIN_RETRY_AFTER - now_monotonic + 0.999),
            )
            detail = _SHIOAJI_STOCK_LAST_LOGIN_ERROR or "previous login failed"
            raise RuntimeError(
                "Shioaji stock quote login cooldown is active for "
                f"{retry_seconds}s after: {detail}"
            )
        api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
        secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
        if not api_key or not secret_key:
            raise RuntimeError(
                "Shioaji stock quotes require SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY"
            )
        import shioaji as sj

        # This quote provider must never acquire a production trading session.
        api = sj.Shioaji(simulation=True)
        if hasattr(api, "set_event_callback"):
            api.set_event_callback(lambda *_args: None)
        try:
            api.login(
                api_key=api_key,
                secret_key=secret_key,
                subscribe_trade=False,
                # A long-lived service must not inherit a token with only a
                # few hours left from Shioaji's local token pool.
                force_refresh=True,
            )
        except Exception as exc:
            _SHIOAJI_STOCK_LOGIN_FAILURES += 1
            retry_base = max(
                0.25,
                float(
                    os.environ.get(
                        "STOCKAGENT_SHIOAJI_LOGIN_RETRY_BASE_SECONDS",
                        "2",
                    )
                ),
            )
            retry_cap = max(
                retry_base,
                float(
                    os.environ.get(
                        "STOCKAGENT_SHIOAJI_LOGIN_RETRY_MAX_SECONDS",
                        "30",
                    )
                ),
            )
            retry_delay = min(
                retry_cap,
                retry_base * (2 ** min(_SHIOAJI_STOCK_LOGIN_FAILURES - 1, 10)),
            )
            _SHIOAJI_STOCK_LAST_LOGIN_ERROR = f"{type(exc).__name__}: {exc}"
            _SHIOAJI_STOCK_LOGIN_RETRY_AFTER = time.monotonic() + retry_delay
            try:
                api.logout()
            except Exception:
                pass
            raise
        _SHIOAJI_STOCK_API = api
        _SHIOAJI_STOCK_LOGIN_FAILURES = 0
        _SHIOAJI_STOCK_LAST_LOGIN_ERROR = None
        _SHIOAJI_STOCK_LOGIN_RETRY_AFTER = 0.0
        return api


def warm_shioaji_stock_quote_client(
    symbols: list[str] | tuple[str, ...] | None = None,
) -> dict[str, int | bool | str]:
    """Prove one process-local session and prime requested stock contracts.

    Shioaji clients and their contract objects are process-local.  A warm
    connection owned by the paper engine therefore says nothing about the
    Discord scheduler's 09:00 process.  Resolve the scheduler's active
    universe before the open, but deliberately do not request a Snapshot here:
    a pre-open Snapshot would be stale evidence for the opening auction.
    """

    api = _shioaji_stock_api()
    try:
        api.usage()
    except Exception as exc:
        if not _is_shioaji_session_failure(exc):
            raise
        api = _reconnect_shioaji_stock_quote_client(api)
        api.usage()

    requested = list(
        dict.fromkeys(str(symbol).strip() for symbol in (symbols or ()))
    )
    requested = [symbol for symbol in requested if symbol]
    resolved = 0
    with _SHIOAJI_STOCK_LOCK:
        for symbol in requested:
            # A Contract V2 lookup can transiently return ``None`` while the
            # process-local contract cache is still updating.  ``None`` is not
            # a durable negative result: retry it on every explicit prewarm so
            # the 08:55 final arm can self-heal without restarting the bot.
            if _SHIOAJI_STOCK_CONTRACTS.get(symbol) is None:
                _SHIOAJI_STOCK_CONTRACTS[symbol] = api.contracts.get(symbol)
            if _SHIOAJI_STOCK_CONTRACTS[symbol] is not None:
                resolved += 1
    missing = len(requested) - resolved
    return {
        "ready": not requested or missing == 0,
        "connection_scope": "process",
        "requested_count": len(requested),
        # "primed" is an evidence claim, not an attempt count.  Only a
        # process-local contract object that can be passed to snapshots counts.
        "primed_count": resolved,
        "resolved_count": resolved,
        "missing_count": missing,
        "snapshot_prefetched": False,
    }


def _reconnect_shioaji_stock_quote_client(failed_api: object) -> object:
    """Replace one known-broken cached session and perform one fresh login."""

    global _SHIOAJI_STOCK_API
    with _SHIOAJI_STOCK_LOCK:
        if _SHIOAJI_STOCK_API is not failed_api:
            if _SHIOAJI_STOCK_API is not None:
                return _SHIOAJI_STOCK_API
            return _shioaji_stock_api()
        _SHIOAJI_STOCK_API = None
        _SHIOAJI_STOCK_CONTRACTS.clear()
        _SHIOAJI_STOCK_CACHE.clear()
        try:
            failed_api.logout()
        except Exception:
            pass
        return _shioaji_stock_api()


def close_shioaji_stock_quote_client() -> None:
    """Close the process-local quote connection, primarily for clean shutdowns/tests."""

    global _SHIOAJI_STOCK_API
    global _SHIOAJI_STOCK_LOGIN_FAILURES
    global _SHIOAJI_STOCK_LAST_LOGIN_ERROR
    global _SHIOAJI_STOCK_LOGIN_RETRY_AFTER
    with _SHIOAJI_STOCK_LOCK:
        api = _SHIOAJI_STOCK_API
        _SHIOAJI_STOCK_API = None
        _SHIOAJI_STOCK_LOGIN_FAILURES = 0
        _SHIOAJI_STOCK_LAST_LOGIN_ERROR = None
        _SHIOAJI_STOCK_LOGIN_RETRY_AFTER = 0.0
        _SHIOAJI_STOCK_CONTRACTS.clear()
        _SHIOAJI_STOCK_CACHE.clear()
        if api is not None:
            try:
                api.logout()
            except Exception:
                pass


def fetch_shioaji_historical_stock_entry_books(
    symbols: list[str],
    *,
    trading_date: date,
    time_start: datetime_time = datetime_time(9, 0),
    time_end: datetime_time = datetime_time(9, 0, 59),
    max_traffic_fraction: float = 0.90,
    timeout_ms: int = 30_000,
    progress_every: int = 50,
) -> tuple[dict[str, dict[str, float | int | str | None]], dict[str, Any]]:
    """Fetch the first historical executable stock book for each symbol.

    Shioaji's historical stock Tick payload carries one bid, ask, and displayed
    quantity on every trade event.  Its nanosecond timestamps encode Taiwan
    wall-clock values without timezone metadata, so they are decoded as a
    naive datetime and then labelled Asia/Taipei; adding another eight hours
    would be incorrect.

    This helper is deliberately sequential and quota-aware.  It stops at the
    requested traffic fraction and leaves unresolved symbols absent so callers
    can make an explicit, separately labelled fallback decision.
    """

    if not 0.0 < float(max_traffic_fraction) <= 1.0:
        raise ValueError("max_traffic_fraction must be in (0, 1]")
    if time_start > time_end:
        raise ValueError("time_start must not be after time_end")
    requested = list(dict.fromkeys(str(symbol).strip() for symbol in symbols))
    requested = [symbol for symbol in requested if symbol]
    api = _shioaji_stock_api()
    import shioaji as sj

    def usage() -> dict[str, int | float] | None:
        try:
            current = api.usage()
            used = int(current.bytes)
            limit = int(current.limit_bytes)
        except Exception:
            return None
        if used < 0 or limit <= 0:
            return None
        return {
            "used_bytes": used,
            "limit_bytes": limit,
            "fraction": used / limit,
        }

    usage_before = usage()
    books: dict[str, dict[str, float | int | str | None]] = {}
    error_counts: dict[str, int] = {}
    source_empty = 0
    contract_missing = 0
    queried = 0
    stopped_for_traffic = False
    for index, symbol in enumerate(requested, start=1):
        current_usage = usage()
        if current_usage is not None and float(current_usage["fraction"]) >= float(
            max_traffic_fraction
        ):
            stopped_for_traffic = True
            break
        with _SHIOAJI_STOCK_LOCK:
            if symbol not in _SHIOAJI_STOCK_CONTRACTS:
                _SHIOAJI_STOCK_CONTRACTS[symbol] = api.contracts.get(symbol)
            contract = _SHIOAJI_STOCK_CONTRACTS[symbol]
        if contract is None:
            contract_missing += 1
            continue

        _wait_stock_history_request_slot()
        try:
            with shioaji_query(
                api,
                consumer="tw_day_trade_historical_entry_replay",
                method="ticks",
                asset_class="stock",
                details={
                    "contract": symbol,
                    "date": trading_date.isoformat(),
                    "start": time_start.isoformat(),
                    "end": time_end.isoformat(),
                },
            ) as set_ledger_result:
                ticks = api.ticks(
                    contract=contract,
                    date=trading_date.isoformat(),
                    query_type=sj.TicksQueryType.RangeTime,
                    time_start=time_start.isoformat(),
                    time_end=time_end.isoformat(),
                    timeout=int(timeout_ms),
                )
                set_ledger_result(ticks)
            queried += 1
            fields = {
                name: list(getattr(ticks, name, ()))
                for name in (
                    "ts",
                    "close",
                    "bid_price",
                    "bid_volume",
                    "ask_price",
                    "ask_volume",
                )
            }
            lengths = {name: len(values) for name, values in fields.items()}
            if len(set(lengths.values())) != 1:
                raise ValueError(f"inconsistent historical Tick fields: {lengths}")
            selected: dict[str, float | int | str | None] = {
                "symbol": symbol,
                "bid": None,
                "ask": None,
                "bid_volume": None,
                "ask_volume": None,
                "bid_quote_at": None,
                "ask_quote_at": None,
                "bid_timestamp_ns": None,
                "ask_timestamp_ns": None,
                "bid_source_row_index": None,
                "ask_source_row_index": None,
                "last": None,
                "source": "shioaji:historical_stock_tick_best_quote",
            }
            ordered_indices = sorted(
                range(len(fields["ts"])),
                key=lambda position: (int(fields["ts"][position]), position),
            )
            for position in ordered_indices:
                timestamp_ns = int(fields["ts"][position])
                wall_clock = (
                    np.datetime64(timestamp_ns, "ns")
                    .astype("datetime64[us]")
                    .astype(datetime)
                    .replace(tzinfo=ZoneInfo("Asia/Taipei"))
                )
                bid = _float_or_none(fields["bid_price"][position])
                ask = _float_or_none(fields["ask_price"][position])
                bid_volume = _float_or_none(fields["bid_volume"][position])
                ask_volume = _float_or_none(fields["ask_volume"][position])
                valid_bid = (
                    bid is not None and bid_volume is not None and bid_volume > 0
                )
                valid_ask = (
                    ask is not None and ask_volume is not None and ask_volume > 0
                )
                if not (valid_bid or valid_ask):
                    continue
                if valid_bid and valid_ask and float(bid) > float(ask):
                    continue
                quote_at = wall_clock.isoformat(timespec="microseconds")
                if valid_bid and selected["bid"] is None:
                    selected.update(
                        {
                            "bid": bid,
                            "bid_volume": bid_volume,
                            "bid_quote_at": quote_at,
                            "bid_timestamp_ns": timestamp_ns,
                            "bid_source_row_index": position,
                        }
                    )
                if valid_ask and selected["ask"] is None:
                    selected.update(
                        {
                            "ask": ask,
                            "ask_volume": ask_volume,
                            "ask_quote_at": quote_at,
                            "ask_timestamp_ns": timestamp_ns,
                            "ask_source_row_index": position,
                        }
                    )
                if selected["last"] is None:
                    selected["last"] = _float_or_none(fields["close"][position])
                if selected["bid"] is not None and selected["ask"] is not None:
                    break
            if selected["bid"] is None and selected["ask"] is None:
                source_empty += 1
            else:
                quote_times = [
                    str(value)
                    for value in (
                        selected["bid_quote_at"],
                        selected["ask_quote_at"],
                    )
                    if value
                ]
                selected["quote_at"] = min(quote_times) if quote_times else None
                books[symbol] = selected
        except Exception as exc:
            key = type(exc).__name__
            error_counts[key] = error_counts.get(key, 0) + 1
        if progress_every > 0 and (
            index % progress_every == 0 or index == len(requested)
        ):
            print(
                "[tw-day-trade-entry-book] "
                f"date={trading_date.isoformat()} progress={index}/{len(requested)} "
                f"queried={queried} books={len(books)} fallback={index - len(books)}",
                flush=True,
            )

    usage_after = usage()
    return books, {
        "source": "shioaji:historical_stock_tick_best_quote",
        "trading_date": trading_date.isoformat(),
        "time_start": time_start.isoformat(),
        "time_end": time_end.isoformat(),
        "requested_symbols": len(requested),
        "queried_symbols": queried,
        "resolved_book_symbols": len(books),
        "source_empty_symbols": source_empty,
        "contract_missing_symbols": contract_missing,
        "unqueried_symbols": max(0, len(requested) - queried - contract_missing),
        "error_counts": error_counts,
        "stopped_for_traffic": stopped_for_traffic,
        "max_traffic_fraction": float(max_traffic_fraction),
        "usage_before": usage_before,
        "usage_after": usage_after,
    }


def resolve_observed_minute_execution_price(
    *,
    amount: object = None,
    volume_shares: object = None,
    raw_volume: object = None,
    contract_unit: object = None,
    low: object = None,
    high: object = None,
    close: object = None,
) -> tuple[float | None, str | None, float | None]:
    """Resolve one source-backed completed-minute price without requiring ticks.

    Prefer the bar VWAP when its notional and share volume are internally
    consistent.  A source-provided KBar close is the deterministic fallback
    when VWAP cannot be computed.  This never carries an earlier price or
    manufactures a bar that the source did not publish.
    """

    parsed_amount = _float_or_none(amount)
    parsed_shares = _float_or_none(volume_shares)
    parsed_raw_volume = _float_or_none(raw_volume)
    parsed_low = _float_or_none(low)
    parsed_high = _float_or_none(high)
    parsed_close = _float_or_none(close)
    if parsed_shares is not None:
        parsed_shares = (
            float(round(parsed_shares))
            if (round(parsed_shares) > 0
                and abs(parsed_shares - round(parsed_shares)) <= 1e-6) else None
        )
    if volume_shares is None and parsed_raw_volume is not None:
        multipliers: list[float] = []
        parsed_contract_unit = _float_or_none(contract_unit)
        if parsed_contract_unit is not None:
            multipliers.append(parsed_contract_unit)
        multipliers.extend((1_000.0, 100.0, 10.0, 1.0))
        if (
            parsed_amount is not None
            and parsed_low is not None
            and parsed_high is not None
        ):
            candidates: list[float] = []
            for multiplier in dict.fromkeys(multipliers):
                candidate_shares = parsed_raw_volume * multiplier
                if not np.isfinite(candidate_shares) or candidate_shares <= 0.0:
                    continue
                candidate_price = parsed_amount / candidate_shares
                if (
                    np.isfinite(candidate_price)
                    and candidate_price > 0.0
                    and parsed_low * 0.999 <= candidate_price <= parsed_high * 1.001
                ):
                    candidates.append(candidate_shares)
            # A wide historical bar may admit more than one encoding.  The
            # first multiplier that fits is not proof of its share quantity.
            if (len(candidates) == 1 and round(candidates[0]) > 0
                    and abs(candidates[0] - round(candidates[0])) <= 1e-6):
                parsed_shares = float(round(candidates[0]))
    if parsed_amount is not None and parsed_shares is not None:
        vwap = parsed_amount / parsed_shares
        if np.isfinite(vwap) and vwap > 0.0:
            return float(vwap), "minute_vwap", float(parsed_shares)
    if parsed_close is not None:
        return float(parsed_close), "minute_close", parsed_shares
    return None, None, parsed_shares


def observed_0901_minute_volume_lots(
    price_row: Mapping[str, Any], *, lot_size: int = 1_000
) -> float:
    """Convert a 09:01 price receipt's *observed* volume to board lots.

    Historical Shioaji stock ticks report regular-board volume in lots, while
    local minute bars and the Shioaji KBar fallback are normalized to shares.
    Older receipts lack an explicit unit, so their source identifies the unit.
    Unknown provenance fails closed rather than inventing liquidity.
    """

    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    volume = _float_or_none(price_row.get("tick_volume_units_0901"))
    if volume is None or volume <= 0.0:
        return 0.0
    unit = price_row.get("observed_volume_unit_0901")
    if unit is None:
        source = str(price_row.get("source") or "")
        if source == "shioaji:historical_ticks_0900_090059_vwap_right_label_0901":
            unit = "board_lots"
        elif source.startswith(("shioaji:historical_kbar_0901_", "local_minute_parquet_0901_")):
            unit = "shares"
    if unit == "board_lots":
        return float(volume)
    if unit == "shares":
        return float(volume) / lot_size
    return 0.0


def load_local_stock_0901_vwaps(
    minute_roots: tuple[Path, ...],
    symbols: list[str],
    *,
    trading_date: date,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Resolve right-labelled 09:01 prices from downloaded minute Parquet.

    Local immutable/source-backed data is cheaper and faster than issuing one
    historical broker request per symbol.  Roots retain caller priority;
    unresolved symbols alone flow to later roots and then to the broker.
    """

    import polars as pl

    requested = set(dict.fromkeys(str(symbol).strip() for symbol in symbols))
    requested.discard("")
    resolved: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    price_method_counts: dict[str, int] = {}
    error_counts: dict[str, int] = {}
    invalid_volume_unit_rows = 0

    def accept(frame: Any, source: Path) -> None:
        nonlocal invalid_volume_unit_rows
        if not frame.height:
            return
        names = set(frame.columns)
        if "minutes_from_open" in names:
            frame = frame.filter(pl.col("minutes_from_open") == 1)
        elif "ts" in names:
            frame = frame.with_columns(pl.col("ts").cast(pl.Datetime("ns"))).filter(
                (pl.col("ts").dt.hour() == 9) & (pl.col("ts").dt.minute() == 1)
            )
        if "date" in frame.columns:
            frame = frame.filter(pl.col("date").cast(pl.Date) == trading_date)
        elif "ts" in frame.columns:
            frame = frame.filter(pl.col("ts").dt.date() == trading_date)
        for row in frame.iter_rows(named=True):
            symbol = str(row.get("symbol") or "")
            if symbol not in requested or symbol in resolved:
                continue
            volume_unit_valid = (
                "source_volume_unit_valid" not in row
                or row["source_volume_unit_valid"] is True
            )
            if not volume_unit_valid:
                invalid_volume_unit_rows += 1
            price, price_method, volume_shares = resolve_observed_minute_execution_price(
                amount=row.get("Amount") if volume_unit_valid else None,
                volume_shares=row.get("volume_shares") if volume_unit_valid else None,
                raw_volume=row.get("Volume") if volume_unit_valid else None,
                contract_unit=row.get("contract_unit"),
                low=row.get("Low"),
                high=row.get("High"),
                close=row.get("Close"),
            )
            if price is None or price_method is None:
                continue
            row_source = Path(str(row.get("_source_path") or source))
            source_text = (
                f"local_minute_parquet_0901_{price_method}:{row_source.resolve()}"
            )
            close = _float_or_none(row.get("Close"))
            observed_volume = (
                _float_or_none(
                    row.get("volume_shares")
                    if row.get("volume_shares") is not None
                    else row.get("Volume")
                )
                if volume_unit_valid else None
            )
            resolved[symbol] = {
                "symbol": symbol,
                "execution_price_0901": float(price),
                "valuation_price_0901": (
                    close if observed_volume is not None and observed_volume > 0.0 else None
                ),
                "execution_price_0901_method": price_method,
                "tick_volume_units_0901": float(volume_shares or 0.0),
                "observed_volume_unit_0901": "shares",
                "tick_count_0901": 0,
                "source_window_start": datetime.combine(
                    trading_date,
                    datetime_time(9, 0),
                    tzinfo=ZoneInfo("Asia/Taipei"),
                ).isoformat(timespec="seconds"),
                "source_window_end": datetime.combine(
                    trading_date,
                    datetime_time(9, 0, 59),
                    tzinfo=ZoneInfo("Asia/Taipei"),
                ).isoformat(timespec="seconds"),
                "quote_at": datetime.combine(
                    trading_date,
                    datetime_time(9, 1),
                    tzinfo=ZoneInfo("Asia/Taipei"),
                ).isoformat(timespec="seconds"),
                "source": source_text,
            }
            source_counts[source_text] = source_counts.get(source_text, 0) + 1
            price_method_counts[price_method] = (
                price_method_counts.get(price_method, 0) + 1
            )

    for raw_root in minute_roots:
        root = Path(raw_root)
        unresolved = requested - set(resolved)
        if not unresolved or not root.is_dir():
            continue
        chunk_paths: list[Path] = []
        day_text = trading_date.isoformat()
        for symbol in sorted(unresolved):
            for path in sorted((root / "minute_chunks" / symbol).glob("*.parquet")):
                boundaries = path.stem.split("_")
                if len(boundaries) == 2 and boundaries[0] <= day_text <= boundaries[1]:
                    chunk_paths.append(path)
        schema_groups: dict[tuple[str, ...], list[Path]] = {}
        for path in chunk_paths:
            try:
                schema_groups.setdefault(
                    tuple(pl.read_parquet_schema(path).names()), []
                ).append(path)
            except Exception as exc:
                key = type(exc).__name__
                error_counts[key] = error_counts.get(key, 0) + 1
        for schema_tuple, paths in schema_groups.items():
            schema = set(schema_tuple)
            if "symbol" not in schema or "ts" not in schema:
                error_counts["missing_symbol_or_timestamp"] = (
                    error_counts.get("missing_symbol_or_timestamp", 0) + len(paths)
                )
                continue
            columns = [
                name
                for name in (
                    "symbol", "date", "ts", "Amount", "Volume",
                    "volume_shares", "contract_unit", "Low", "High", "Close",
                )
                if name in schema
            ]
            try:
                lazy = pl.scan_parquet(sorted(paths), include_file_paths="_source_path")
                if "date" in schema:
                    lazy = lazy.filter(pl.col("date").cast(pl.Date) == trading_date)
                else:
                    lazy = lazy.filter(pl.col("ts").dt.date() == trading_date)
                lazy = lazy.filter(
                    (pl.col("ts").dt.hour() == 9)
                    & (pl.col("ts").dt.minute() == 1)
                )
                accept(
                    lazy.select(*columns, "_source_path").collect(engine="streaming"),
                    root,
                )
            except Exception as exc:
                key = type(exc).__name__
                error_counts[key] = error_counts.get(key, 0) + 1
        unresolved = requested - set(resolved)
        partition = root / f"trade_date={day_text}" / "data.parquet"
        if unresolved and partition.is_file():
            try:
                schema = pl.scan_parquet(partition).collect_schema().names()
                columns = [
                    name
                    for name in (
                        "symbol", "date", "ts", "minutes_from_open", "Amount",
                        "Volume", "volume_shares", "contract_unit", "Low", "High",
                        "Close", "source_volume_unit_valid",
                    )
                    if name in schema
                ]
                lazy = pl.scan_parquet(partition).filter(
                    pl.col("symbol").is_in(sorted(unresolved))
                )
                if "minutes_from_open" in schema:
                    lazy = lazy.filter(pl.col("minutes_from_open") == 1)
                elif "ts" in schema:
                    lazy = lazy.filter(
                        (pl.col("ts").dt.hour() == 9)
                        & (pl.col("ts").dt.minute() == 1)
                    )
                accept(lazy.select(columns).collect(), partition)
            except Exception as exc:
                key = type(exc).__name__
                error_counts[key] = error_counts.get(key, 0) + 1
    return resolved, {
        "source": "local_minute_data_0901_price",
        "requested_symbols": len(requested),
        "resolved_symbols": len(resolved),
        "unresolved_symbols": len(requested - set(resolved)),
        "source_counts": source_counts,
        "price_method_counts": price_method_counts,
        "error_counts": error_counts,
        "invalid_volume_unit_rows": invalid_volume_unit_rows,
        "additional_shioaji_requests": 0,
    }


def _wait_stock_history_request_slot() -> None:
    """History retains its 20% reserve within the shared stock client window."""
    _wait_stock_quote_query_slot(history=True)


def _wait_stock_quote_query_slot(*, history: bool = False) -> None:
    """Use the actual account policy without spacing a cold opening burst.

    The canonical provider profile is 50 market-data calls per *10* seconds,
    not the obsolete 50/5s. Snapshots and this client's history now share
    accounting; history reserves 20% for opening repair. Other process/account
    callers are NOT proved by this local window. Native push has no query slot.
    This is not an application/global website limit.
    """

    from downloader.common import provider_rate_limit

    profile = provider_rate_limit("shioaji_quote_query")
    window_seconds = float(profile.seconds)
    budget = max(1, int(profile.requests * (0.8 if history else 1.0)))

    with _SHIOAJI_HISTORY_REQUEST_LOCK:
        now = time.monotonic()
        while _SHIOAJI_HISTORY_REQUEST_TIMES and now - _SHIOAJI_HISTORY_REQUEST_TIMES[0] >= window_seconds:
            _SHIOAJI_HISTORY_REQUEST_TIMES.popleft()
        if len(_SHIOAJI_HISTORY_REQUEST_TIMES) >= budget:
            time.sleep(max(0.0, window_seconds + 0.01 - (now - _SHIOAJI_HISTORY_REQUEST_TIMES[0])))
            now = time.monotonic()
            while _SHIOAJI_HISTORY_REQUEST_TIMES and now - _SHIOAJI_HISTORY_REQUEST_TIMES[0] >= window_seconds:
                _SHIOAJI_HISTORY_REQUEST_TIMES.popleft()
        _SHIOAJI_HISTORY_REQUEST_TIMES.append(time.monotonic())


def _stock_history_result(method, *, timeout_ms: int, nonblocking: bool, **kwargs):
    """Use the callback result, not the timeout=0 placeholder, on live recovery.

    Python's Event wait releases the interpreter to the executor loop. A late
    callback after the bounded wait is discarded; it cannot publish a result
    into another request or into a subsequent trading session.
    """

    if not nonblocking:
        return method(timeout=int(timeout_ms), **kwargs)
    received, closed = threading.Event(), threading.Event()
    values = []

    def callback(value):
        if not closed.is_set():
            values[:] = [value]
            received.set()

    try:
        method(timeout=0, cb=callback, **kwargs)
        if not received.wait(timeout=max(0.001, int(timeout_ms) / 1000.0)):
            raise TimeoutError("Shioaji historical data callback timed out")
        if values[0] is None:
            raise ValueError("Shioaji historical data callback returned no result")
        return values[0]
    finally:
        closed.set()


def fetch_shioaji_historical_stock_0901_vwaps(
    symbols: list[str],
    *,
    trading_date: date,
    max_traffic_fraction: float = 0.90,
    timeout_ms: int = 30_000,
    progress_every: int = 50,
    progress_callback: Callable[[int, int, int, int], None] | None = None,
    nonblocking: bool = False,
    resolved_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[dict[str, dict[str, float | int | str]], dict[str, Any]]:
    """Fetch the observed right-labelled 09:01 minute price per stock.

    The project's minute execution contract labels trades from
    ``09:00:00..09:00:59`` as the completed ``09:01`` bar.  This function
    first computes ``sum(close * volume) / sum(volume)`` from historical
    trades.  If the tick query is empty, it queries the source-published 09:01
    KBar and uses its VWAP when possible, otherwise that same bar's Close.
    Absence of an exact tick is therefore not a no-fill rule.  A symbol remains
    unresolved only when neither source publishes a valid 09:01 minute price;
    callers must not replace that gap with the official open, a carried last
    price, best quote, or an adverse tick.

    The legacy ``tick_volume_units_0901`` field retains source-native units:
    regular-board historical ticks are lots, normalized minute bars are
    shares. Consumers must use ``observed_0901_minute_volume_lots``.
    """

    if not 0.0 < float(max_traffic_fraction) <= 1.0:
        raise ValueError("max_traffic_fraction must be in (0, 1]")
    fetch_started = time.monotonic()
    requested = list(dict.fromkeys(str(symbol).strip() for symbol in symbols))
    requested = [symbol for symbol in requested if symbol]
    api = _shioaji_stock_api()
    import shioaji as sj

    api_ready_ms = (time.monotonic() - fetch_started) * 1000.0
    guard_usage_ms = 0.0
    guard_usage_calls = 0
    contract_resolution_ms = 0.0
    history_admission_ms = 0.0
    resolved_callback_ms = 0.0
    resolved_callback_calls = 0
    query_phases = {name: 0.0 for name in (
        "usage_before_ms", "request_body_ms", "usage_after_ms", "pre_record_ms",
        "ledger_record_ms", "total_context_ms",
    )}
    measured_queries = unmeasured_queries = ledger_record_failures = 0

    def usage() -> dict[str, int | float] | None:
        nonlocal guard_usage_ms, guard_usage_calls
        started = time.monotonic()
        guard_usage_calls += 1
        try:
            current = api.usage()
            used = int(current.bytes)
            limit = int(current.limit_bytes)
        except Exception:
            return None
        finally:
            guard_usage_ms += (time.monotonic() - started) * 1000.0
        if used < 0 or limit <= 0:
            return None
        return {
            "used_bytes": used,
            "limit_bytes": limit,
            "fraction": used / limit,
        }

    @contextmanager
    def history_query(*, consumer: str, method: str, details: dict[str, Any]):
        nonlocal measured_queries, unmeasured_queries, ledger_record_failures
        timing: dict[str, Any] = {}
        try:
            with shioaji_query(
                api, consumer=consumer, method=method, asset_class="stock",
                details=details, timing=timing,
            ) as set_result:
                yield set_result
        finally:
            try:
                measured = (
                    {name: float(timing[name]) for name in query_phases}
                    if timing.get("contract") == QUERY_OBSERVATION_CONTRACT else None
                )
            except (KeyError, TypeError, ValueError):
                measured = None
            if measured is not None:
                measured_queries += 1
                for name in query_phases:
                    query_phases[name] += measured[name]
                ledger_record_failures += int(bool(timing.get("ledger_record_failed")))
            else:
                unmeasured_queries += 1

    usage_before = usage()
    resolved: dict[str, dict[str, float | int | str]] = {}
    error_counts: dict[str, int] = {}
    queried = 0
    source_empty = 0
    kbar_fallback_queries = 0
    kbar_fallback_resolved = 0
    price_method_counts: dict[str, int] = {}
    contract_missing = 0
    stopped_for_traffic = False
    attempted_symbol_codes: list[str] = []
    failed_symbol_codes: list[str] = []
    deferred_kbar_symbol_codes: list[str] = []
    missing_contract_codes: list[str] = []
    window_start = datetime.combine(
        trading_date,
        datetime_time(9, 0),
        tzinfo=ZoneInfo("Asia/Taipei"),
    )
    window_end = datetime.combine(
        trading_date,
        datetime_time(9, 1),
        tzinfo=ZoneInfo("Asia/Taipei"),
    )

    for index, symbol in enumerate(requested, start=1):
        if progress_callback is not None:
            progress_callback(index - 1, len(requested), queried, len(resolved))
        current_usage = usage()
        if current_usage is not None and float(current_usage["fraction"]) >= float(
            max_traffic_fraction
        ):
            stopped_for_traffic = True
            break
        contract_started = time.monotonic()
        with _SHIOAJI_STOCK_LOCK:
            if symbol not in _SHIOAJI_STOCK_CONTRACTS:
                _SHIOAJI_STOCK_CONTRACTS[symbol] = api.contracts.get(symbol)
            contract = _SHIOAJI_STOCK_CONTRACTS[symbol]
        contract_resolution_ms += (time.monotonic() - contract_started) * 1000.0
        if contract is None:
            contract_missing += 1
            missing_contract_codes.append(symbol)
            continue

        admission_started = time.monotonic()
        _wait_stock_history_request_slot()
        history_admission_ms += (time.monotonic() - admission_started) * 1000.0
        attempted_symbol_codes.append(symbol)
        try:
            with history_query(
                consumer="tw_day_trade_missed_open_0901_vwap",
                method="ticks",
                details={
                    "contract": symbol,
                    "date": trading_date.isoformat(),
                    "start": "09:00:00",
                    "end": "09:00:59",
                    "right_label": "09:01:00",
                },
            ) as set_ledger_result:
                ticks = _stock_history_result(
                    api.ticks,
                    contract=contract,
                    date=trading_date.isoformat(),
                    query_type=sj.TicksQueryType.RangeTime,
                    time_start="09:00:00",
                    time_end="09:00:59",
                    timeout_ms=int(timeout_ms), nonblocking=nonblocking,
                )
                set_ledger_result(ticks)
            queried += 1
            timestamps = list(getattr(ticks, "ts", ()))
            closes = list(getattr(ticks, "close", ()))
            volumes = list(getattr(ticks, "volume", ()))
            lengths = {len(timestamps), len(closes), len(volumes)}
            if len(lengths) != 1:
                raise ValueError(
                    "inconsistent historical Tick fields: "
                    f"ts={len(timestamps)} close={len(closes)} volume={len(volumes)}"
                )
            notional = 0.0
            total_volume = 0.0
            accepted = 0
            first_at: datetime | None = None
            first_price: float | None = None
            last_at: datetime | None = None
            for position in sorted(
                range(len(timestamps)),
                key=lambda offset: (int(timestamps[offset]), offset),
            ):
                wall_clock = (
                    np.datetime64(int(timestamps[position]), "ns")
                    .astype("datetime64[us]")
                    .astype(datetime)
                    .replace(tzinfo=ZoneInfo("Asia/Taipei"))
                )
                if wall_clock < window_start or wall_clock >= window_end:
                    continue
                price = _float_or_none(closes[position])
                volume = _float_or_none(volumes[position])
                if price is None or volume is None or volume <= 0.0:
                    continue
                notional += price * volume
                total_volume += volume
                accepted += 1
                if first_at is None:
                    first_at = wall_clock
                    first_price = price
                last_at = wall_clock
            vwap = notional / total_volume if total_volume > 0.0 else float("nan")
            if not np.isfinite(vwap) or vwap <= 0.0 or accepted <= 0:
                current_usage = usage()
                if current_usage is not None and float(
                    current_usage["fraction"]
                ) >= float(max_traffic_fraction):
                    stopped_for_traffic = True
                    # Empty ticks are not an authoritative missing minute bar:
                    # its KBar fallback has not been requested at all.
                    deferred_kbar_symbol_codes.append(symbol)
                    continue
                admission_started = time.monotonic()
                _wait_stock_history_request_slot()
                history_admission_ms += (time.monotonic() - admission_started) * 1000.0
                with history_query(
                    consumer="tw_day_trade_missed_open_0901_kbar_fallback",
                    method="kbars",
                    details={
                        "contract": symbol,
                        "date": trading_date.isoformat(),
                        "right_label": "09:01:00",
                    },
                ) as set_kbar_ledger_result:
                    kbars = _stock_history_result(
                        api.kbars,
                        contract=contract,
                        start=trading_date.isoformat(),
                        end=trading_date.isoformat(),
                        timeout_ms=int(timeout_ms), nonblocking=nonblocking,
                    )
                    set_kbar_ledger_result(kbars)
                kbar_fallback_queries += 1
                kbar_fields = {
                    name: list(getattr(kbars, name, ()))
                    for name in (
                        "ts", "Open", "Close", "Low", "High", "Volume", "Amount"
                    )
                }
                lengths = {len(values) for values in kbar_fields.values()}
                if len(lengths) != 1:
                    raise ValueError(
                        "inconsistent historical KBar fields: "
                        + ", ".join(
                            f"{name}={len(values)}"
                            for name, values in kbar_fields.items()
                        )
                    )
                minute_row: dict[str, object] | None = None
                for position in range(len(kbar_fields["ts"])):
                    wall_clock = (
                        np.datetime64(int(kbar_fields["ts"][position]), "ns")
                        .astype("datetime64[us]")
                        .astype(datetime)
                        .replace(tzinfo=ZoneInfo("Asia/Taipei"))
                    )
                    if wall_clock == window_end:
                        minute_row = {
                            name: values[position]
                            for name, values in kbar_fields.items()
                            if name != "ts"
                        }
                        break
                price, method, normalized_shares = (
                    resolve_observed_minute_execution_price(
                        amount=minute_row.get("Amount"),
                        raw_volume=minute_row.get("Volume"),
                        low=minute_row.get("Low"),
                        high=minute_row.get("High"),
                        close=minute_row.get("Close"),
                    )
                    if minute_row is not None
                    else (None, None, None)
                )
                if price is None or method is None:
                    source_empty += 1
                else:
                    kbar_fallback_resolved += 1
                    price_method_counts[method] = price_method_counts.get(method, 0) + 1
                    resolved[symbol] = {
                        "symbol": symbol,
                        "execution_price_0901": float(price),
                        "execution_price_0901_method": method,
                        "tick_volume_units_0901": float(normalized_shares or 0.0),
                        "observed_volume_unit_0901": "shares",
                        "tick_count_0901": 0,
                        "source_window_start": window_start.isoformat(
                            timespec="seconds"
                        ),
                        "source_window_end": window_end.isoformat(timespec="seconds"),
                        "quote_at": window_end.isoformat(timespec="seconds"),
                        "source": f"shioaji:historical_kbar_0901_{method}",
                        "session_open_price_0900": (
                            float(minute_row["Open"])
                            if normalized_shares is not None
                            and normalized_shares > 0.0
                            and _float_or_none(minute_row.get("Open")) is not None
                            else None
                        ),
                        "session_open_price_source": (
                            "shioaji:historical_kbar_0901_open_first_trade"
                            if normalized_shares is not None
                            and normalized_shares > 0.0
                            and _float_or_none(minute_row.get("Open")) is not None
                            else None
                        ),
                    }
            else:
                price_method_counts["minute_vwap"] = (
                    price_method_counts.get("minute_vwap", 0) + 1
                )
                resolved[symbol] = {
                    "symbol": symbol,
                    "execution_price_0901": float(vwap),
                    "execution_price_0901_method": "minute_vwap",
                    "tick_volume_units_0901": float(total_volume),
                    "observed_volume_unit_0901": "board_lots",
                    "tick_count_0901": int(accepted),
                    "source_window_start": first_at.isoformat(timespec="microseconds"),
                    "source_window_end": last_at.isoformat(timespec="microseconds"),
                    "quote_at": window_end.isoformat(timespec="seconds"),
                    "source": "shioaji:historical_ticks_0900_090059_vwap_right_label_0901",
                    "session_open_price_0900": float(first_price),
                    "session_open_price_source": (
                        "shioaji:historical_first_trade_session_open"
                    ),
                }
        except Exception as exc:
            key = type(exc).__name__
            error_counts[key] = error_counts.get(key, 0) + 1
            failed_symbol_codes.append(symbol)
        if symbol in resolved and resolved_callback is not None:
            callback_started = time.monotonic()
            resolved_callback_calls += 1
            try:
                resolved_callback(symbol, dict(resolved[symbol]))
            finally:
                resolved_callback_ms += (time.monotonic() - callback_started) * 1000.0
        if progress_every > 0 and (
            index % progress_every == 0 or index == len(requested)
        ):
            print(
                "[tw-day-trade-0901-vwap] "
                f"date={trading_date.isoformat()} progress={index}/{len(requested)} "
                f"queried={queried} resolved={len(resolved)}",
                flush=True,
            )
            if progress_callback is not None:
                progress_callback(index, len(requested), queried, len(resolved))

    usage_after = usage()
    total_fetch_ms = (time.monotonic() - fetch_started) * 1000.0
    separately_measured_ms = (
        api_ready_ms + guard_usage_ms + contract_resolution_ms + history_admission_ms
        + resolved_callback_ms + query_phases["total_context_ms"]
    )
    return resolved, {
        "source": "shioaji:historical_0901_minute_price_ticks_then_kbar",
        "trading_date": trading_date.isoformat(),
        "source_window": "09:00:00..09:00:59 Asia/Taipei",
        "right_label": "09:01:00 Asia/Taipei",
        "price_contract": "sum(close*volume)/sum(volume)",
        "requested_symbols": len(requested),
        "queried_symbols": queried,
        "attempted_symbols": attempted_symbol_codes,
        "failed_symbols": failed_symbol_codes,
        "retry_symbols": sorted(set(failed_symbol_codes + deferred_kbar_symbol_codes)),
        "deferred_kbar_symbols": deferred_kbar_symbol_codes,
        "contract_missing_symbol_codes": missing_contract_codes,
        "resolved_symbols": len(resolved),
        "source_empty_symbols": source_empty,
        "kbar_fallback_queries": kbar_fallback_queries,
        "kbar_fallback_resolved_symbols": kbar_fallback_resolved,
        "price_method_counts": price_method_counts,
        "contract_missing_symbols": contract_missing,
        "unqueried_symbols": max(0, len(requested) - len(attempted_symbol_codes) - contract_missing),
        "error_counts": error_counts,
        "stopped_for_traffic": stopped_for_traffic,
        "max_traffic_fraction": float(max_traffic_fraction),
        "usage_before": usage_before,
        "usage_after": usage_after,
        "timing": {
            "contract": "historical-0901-fetch-phases-v1",
            "query_observation_contract": QUERY_OBSERVATION_CONTRACT,
            "clock": "monotonic",
            "scope": "remote fetch including callbacks; excludes local scans, final outer receipt and NAV processing",
            "total_fetch_ms": round(total_fetch_ms, 3),
            "api_ready_ms": round(api_ready_ms, 3),
            "guard_usage_calls": guard_usage_calls,
            "guard_usage_ms": round(guard_usage_ms, 3),
            "contract_resolution_ms": round(contract_resolution_ms, 3),
            "history_admission_ms": round(history_admission_ms, 3),
            "resolved_callback_calls": resolved_callback_calls,
            "resolved_callback_ms": round(resolved_callback_ms, 3),
            "measured_query_contexts": measured_queries,
            "unmeasured_query_contexts": unmeasured_queries,
            "query_phase_totals_ms": {
                name: round(value, 3) if measured_queries else None
                for name, value in query_phases.items()
            },
            "ledger_record_failures": ledger_record_failures,
            "remaining_processing_ms": (
                round(max(0.0, total_fetch_ms - separately_measured_ms), 3)
                if unmeasured_queries == 0 else None
            ),
        },
    }


def fetch_shioaji_current_stock_minute_bars(
    symbols: list[str],
    *,
    trading_date: date,
    completed_through: datetime,
    max_traffic_fraction: float = 0.90,
    timeout_ms: int = 30_000,
    progress_every: int = 50,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read current-session completed stock KBars for an isolated replay.

    This is a quote-only Shioaji query and never submits orders. Returned bars
    retain the source's right-labelled timestamps and normalized share volume;
    zero-volume placeholders are omitted and counted rather than treated as
    executable liquidity.
    """

    if completed_through.tzinfo is None:
        raise ValueError("completed_through must be timezone-aware")
    cutoff = completed_through.astimezone(ZoneInfo("Asia/Taipei"))
    if cutoff.date() != trading_date:
        raise ValueError("completed_through must belong to trading_date")
    if not 0.0 < float(max_traffic_fraction) <= 1.0:
        raise ValueError("max_traffic_fraction must be in (0, 1]")
    requested = [
        symbol
        for symbol in dict.fromkeys(str(value).strip() for value in symbols)
        if symbol
    ]
    api = _shioaji_stock_api()

    def usage() -> dict[str, int | float] | None:
        try:
            current = api.usage()
            used = int(current.bytes)
            limit = int(current.limit_bytes)
        except Exception:
            return None
        if used < 0 or limit <= 0:
            return None
        return {"used_bytes": used, "limit_bytes": limit, "fraction": used / limit}

    rows: list[dict[str, Any]] = []
    attempted: list[str] = []
    error_counts: dict[str, int] = {}
    source_empty = 0
    zero_volume_rows = 0
    contract_missing = 0
    stopped_for_traffic = False
    usage_before = usage()
    for index, symbol in enumerate(requested, start=1):
        current_usage = usage()
        if current_usage is not None and float(current_usage["fraction"]) >= float(
            max_traffic_fraction
        ):
            stopped_for_traffic = True
            break
        with _SHIOAJI_STOCK_LOCK:
            if symbol not in _SHIOAJI_STOCK_CONTRACTS:
                _SHIOAJI_STOCK_CONTRACTS[symbol] = api.contracts.get(symbol)
            contract = _SHIOAJI_STOCK_CONTRACTS[symbol]
        if contract is None:
            contract_missing += 1
            continue
        _wait_stock_history_request_slot()
        try:
            with shioaji_query(
                api,
                consumer="tw_day_trade_current_minute_replay",
                method="kbars",
                asset_class="stock",
                details={
                    "contract": symbol,
                    "start": trading_date.isoformat(),
                    "end": trading_date.isoformat(),
                    "completed_through": cutoff.isoformat(timespec="minutes"),
                },
            ) as set_ledger_result:
                payload = api.kbars(
                    contract=contract,
                    start=trading_date.isoformat(),
                    end=trading_date.isoformat(),
                    timeout=int(timeout_ms),
                )
                set_ledger_result(payload)
            attempted.append(symbol)
            fields = {
                name: list(getattr(payload, name, ()))
                for name in (
                    "ts",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Amount",
                )
            }
            lengths = {len(values) for values in fields.values()}
            if len(lengths) != 1:
                raise ValueError(
                    "inconsistent historical KBar fields: "
                    + ", ".join(
                        f"{name}={len(values)}" for name, values in fields.items()
                    )
                )
            accepted = 0
            for position in range(len(fields["ts"])):
                stamp = (
                    np.datetime64(int(fields["ts"][position]), "ns")
                    .astype("datetime64[us]")
                    .astype(datetime)
                    .replace(tzinfo=ZoneInfo("Asia/Taipei"))
                )
                if (
                    stamp.date() != trading_date
                    or stamp.time() < datetime_time(9, 1)
                    or stamp > cutoff
                ):
                    continue
                raw_volume = _float_or_none(fields["Volume"][position])
                if raw_volume is None or raw_volume <= 0.0:
                    zero_volume_rows += 1
                    continue
                open_price = _float_or_none(fields["Open"][position])
                high = _float_or_none(fields["High"][position])
                low = _float_or_none(fields["Low"][position])
                close = _float_or_none(fields["Close"][position])
                if any(value is None for value in (open_price, high, low, close)):
                    continue
                _price, _method, volume_shares = (
                    resolve_observed_minute_execution_price(
                        amount=fields["Amount"][position],
                        raw_volume=raw_volume,
                        low=low,
                        high=high,
                        close=close,
                    )
                )
                if volume_shares is None or volume_shares <= 0.0:
                    zero_volume_rows += 1
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "date": trading_date,
                        "ts": stamp.replace(tzinfo=None),
                        "Open": float(open_price),
                        "High": float(high),
                        "Low": float(low),
                        "Close": float(close),
                        "volume_shares": float(volume_shares),
                        "Amount": _float_or_none(fields["Amount"][position]),
                        "contract_unit": 1.0,
                        "source": "shioaji:current_session_stock_kbar",
                    }
                )
                accepted += 1
            if not accepted:
                source_empty += 1
        except Exception as exc:
            key = type(exc).__name__
            error_counts[key] = error_counts.get(key, 0) + 1
        if progress_every > 0 and (
            index % progress_every == 0 or index == len(requested)
        ):
            print(
                "[tw-day-trade-current-minute] "
                f"date={trading_date.isoformat()} progress={index}/{len(requested)} "
                f"attempted={len(attempted)} rows={len(rows)}",
                flush=True,
            )

    return rows, {
        "source": "shioaji:current_session_stock_kbar",
        "trading_date": trading_date.isoformat(),
        "completed_through": cutoff.isoformat(timespec="minutes"),
        "requested_symbols": len(requested),
        "attempted_symbols": attempted,
        "resolved_symbols": len({str(row["symbol"]) for row in rows}),
        "rows": len(rows),
        "source_empty_symbols": source_empty,
        "zero_volume_rows_ignored": zero_volume_rows,
        "contract_missing_symbols": contract_missing,
        "unqueried_symbols": max(
            0, len(requested) - len(attempted) - contract_missing
        ),
        "error_counts": error_counts,
        "stopped_for_traffic": stopped_for_traffic,
        "max_traffic_fraction": float(max_traffic_fraction),
        "usage_before": usage_before,
        "usage_after": usage(),
        "simulation_only": True,
        "production_order_possible": False,
    }


def _contract_positive(contract: object, *names: str) -> float | None:
    for name in names:
        value = _float_or_none(getattr(contract, name, None))
        if value is not None:
            return value
    return None


class StockQuoteWakeup:
    """Coalesced nonblocking notification, not a price or execution authority.

    Only currently pending entry symbols and explicit non-trial events wake the
    sole ledger owner. Native callbacks do no serialization, network or ledger
    work; the executor still validates timestamps, prices, limits and quantity.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._symbols: frozenset[str] = frozenset()
        self._pending = False
        self._read_fd: int | None = None
        self._write_fd: int | None = None
        try:
            self._read_fd, self._write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        except (AttributeError, OSError):
            pass  # Ordinary bounded polling remains the portable fallback.

    @property
    def fd(self) -> int | None:
        return self._read_fd

    def arm(self, symbols) -> None:
        with self._lock:
            self._symbols = frozenset(symbols)

    def notify(self, symbol: str, *, simtrade: bool | None) -> None:
        if simtrade is not False:
            return
        with self._lock:
            if symbol not in self._symbols or self._pending or self._write_fd is None:
                return
            try:
                os.write(self._write_fd, b"q")
                self._pending = True
            except (BlockingIOError, OSError):
                pass

    def consume(self) -> bool:
        with self._lock:
            changed = self._pending
            if self._read_fd is not None:
                try:
                    os.read(self._read_fd, 64 * 1024)
                except (BlockingIOError, OSError):
                    pass
            self._pending = False
            return changed

    def close(self) -> None:
        with self._lock:
            self._symbols = frozenset()
            for descriptor in (self._read_fd, self._write_fd):
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
            self._read_fd = self._write_fd = None
            self._pending = False


def set_shioaji_stock_quote_wakeup(wakeup: StockQuoteWakeup | None) -> None:
    global _SHIOAJI_STOCK_WAKEUP
    previous = _SHIOAJI_STOCK_WAKEUP
    _SHIOAJI_STOCK_WAKEUP = wakeup
    if previous is not None and previous is not wakeup:
        previous.close()


def _record_stock_stream_event(kind: str, event: object, *, received: datetime | None = None) -> None:
    """Retain exchange time and trial flags; never relabel a cache read as receipt."""
    global _SHIOAJI_OPENING_REVISION
    if getattr(event, "intraday_odd", False):
        return
    code = str(getattr(event, "code", ""))
    exchange_at = getattr(event, "datetime", None)
    if not code or not isinstance(exchange_at, datetime):
        return
    zone = ZoneInfo("Asia/Taipei")
    exchange_at = exchange_at.replace(tzinfo=zone) if exchange_at.tzinfo is None else exchange_at.astimezone(zone)
    observed = received or datetime.now(zone)
    row: dict[str, Any] = {"exchange_at": exchange_at.isoformat(), "received_at": observed.isoformat(),
                           "simtrade": getattr(event, "simtrade", None)}
    if row["simtrade"] is not None:
        row["simtrade"] = bool(row["simtrade"])
    def number(value: object) -> float | None:
        try:
            result = float(value)
            return result if np.isfinite(result) and result >= 0 else None
        except (ValueError, TypeError):
            return None
    if kind in {"tick", "quote"}:
        for field in ("open", "close", "high", "low", "volume", "total_volume"):
            row[field] = number(getattr(event, field, None))
    if kind in {"book", "quote"}:
        for target, field in (("bid", "bid_price"), ("ask", "ask_price"),
                              ("bid_volume", "bid_volume"), ("ask_volume", "ask_volume")):
            values = getattr(event, field, None)
            row[target] = number(values[0]) if values is not None and len(values) else None
    row["suspend"] = getattr(event, "suspend", None)
    opening_date = observed.astimezone(zone).date().isoformat()
    opening_values = _shioaji_stream_opening_values(row, session_date=opening_date) if kind == "quote" else None
    with _SHIOAJI_STREAM_LOCK:
        previous = _SHIOAJI_STREAM_ROWS.get(code, {}).get(kind, {})
        rows = _SHIOAJI_STREAM_ROWS.setdefault(code, {})
        latest = previous.get("exchange_at", "") <= row["exchange_at"]
        if latest:
            rows[kind] = row
        if (latest and kind == "quote" and row.get("simtrade") is False
                and row.get("volume") is not None and row["volume"] > 0
                and datetime_time(13, 30) <= exchange_at.time() < datetime_time(13, 34)):
            rows["auction"] = row
        # Latest book and a proven opening are different facts. Trial/missing
        # fields, rotation and reconnect must not erase a received formal open.
        # Keep its actual clocks; this does not make its old book executable.
        frozen = rows.get("opening", {})
        previous_opening = _shioaji_stream_opening_values(frozen, session_date=opening_date) if opening_values else None
        opening_changed = opening_values is not None and (
            previous_opening is None or (
                frozen.get("exchange_at", "") <= row["exchange_at"]
                and previous_opening["open"] != opening_values["open"]))
        if opening_changed:
            rows["opening"] = dict(row)
            _SHIOAJI_OPENING_REVISION += 1
        opening_waiters = tuple(_SHIOAJI_OPENING_CONDITIONS) if opening_changed else ()
    # Wake requesting workers only. Parsing, disk writes and model/ledger work
    # must never run on the native delivery thread.
    if opening_changed:
        for condition in opening_waiters:
            with condition:
                condition.notify_all()
    wakeup = _SHIOAJI_STOCK_WAKEUP
    if latest and wakeup is not None:
        wakeup.notify(code, simtrade=row["simtrade"])


def fetch_shioaji_stock_live_quotes(
    symbols: list[str], *, trading_date: date, allow_rotation: bool = True,
) -> dict[str, dict[str, Any]]:
    """Subscribe once on the existing market-data login, then read pushed state.

    This owns callbacks only on the dedicated stock quote client. It never
    registers order callbacks, creates another login, polls snapshots, or
    infers non-trial status from missing fields. Subscriptions are reconciled
    against the current execution universe so completed symbols release quota.
    """
    import shioaji as sj
    global _SHIOAJI_STREAM_API, _SHIOAJI_STREAM_ROTATION_AT, _SHIOAJI_STREAM_CURSOR
    global _SHIOAJI_STREAM_TARGET, _SHIOAJI_STREAM_TARGET_READY
    api = _shioaji_stock_api()
    with _SHIOAJI_STREAM_LOCK:
        if _SHIOAJI_STREAM_API is not api:
            _SHIOAJI_STREAM_SUBSCRIPTIONS.clear()
            # A new connection must receive new books. Opening facts already
            # observed today are immutable input evidence, not live books.
            retained_opens = {
                code: {"opening": dict(rows["opening"])}
                for code, rows in _SHIOAJI_STREAM_ROWS.items()
                if _shioaji_stream_opening_values(rows.get("opening", {}),
                    session_date=trading_date.isoformat()) is not None
            }
            _SHIOAJI_STREAM_ROWS.clear()
            _SHIOAJI_STREAM_ROWS.update(retained_opens)
            _SHIOAJI_STREAM_CURSOR = 0
            _SHIOAJI_STREAM_ROTATION_AT = 0.0
            _SHIOAJI_STREAM_TARGET = ()
            _SHIOAJI_STREAM_TARGET_READY = False
            api.set_on_quote_stk_v1_callback(lambda *args: _record_stock_stream_event("quote", args[-1]))
            _SHIOAJI_STREAM_API = api
    requested = sorted(set(map(str, symbols)))
    if len(requested) <= _SHIOAJI_STOCK_STREAM_LIMIT:
        selected = requested
        _SHIOAJI_STREAM_CURSOR = 0
    else:
        now = time.monotonic()
        if (_SHIOAJI_STREAM_TARGET
                and set(_SHIOAJI_STREAM_TARGET) <= set(requested)
                and (not allow_rotation or not _SHIOAJI_STREAM_TARGET_READY
                     or now < _SHIOAJI_STREAM_ROTATION_AT)):
            selected = list(_SHIOAJI_STREAM_TARGET)
        else:
            start = _SHIOAJI_STREAM_CURSOR % len(requested)
            selected = [requested[(start + idx) % len(requested)] for idx in range(_SHIOAJI_STOCK_STREAM_LIMIT)]
            _SHIOAJI_STREAM_CURSOR = (start + _SHIOAJI_STOCK_STREAM_LIMIT) % len(requested)
    if tuple(selected) != _SHIOAJI_STREAM_TARGET:
        _SHIOAJI_STREAM_TARGET = tuple(selected)
        _SHIOAJI_STREAM_TARGET_READY = False
    desired = {(code, "quote") for code in selected}
    kinds = {"tick": sj.QuoteType.Tick, "book": sj.QuoteType.BidAsk,
             "quote": sj.QuoteType.Quote}
    # API calls stay outside the callback lock (a native subscribe may deliver
    # the first event synchronously). Only this engine thread owns subscriptions.
    for code, kind in sorted(_SHIOAJI_STREAM_SUBSCRIPTIONS - desired)[:_SHIOAJI_STOCK_STREAM_BATCH_LIMIT]:
        contract = api.contracts.get(code)
        if contract is not None:
            api.unsubscribe(contract, quote_type=kinds[kind])
        _SHIOAJI_STREAM_SUBSCRIPTIONS.discard((code, kind))
    free_slots = max(0, _SHIOAJI_STOCK_STREAM_LIMIT - len(_SHIOAJI_STREAM_SUBSCRIPTIONS))
    for code, kind in sorted(desired - _SHIOAJI_STREAM_SUBSCRIPTIONS)[:min(_SHIOAJI_STOCK_STREAM_BATCH_LIMIT, free_slots)]:
        contract = api.contracts.get(code)
        if contract is None:
            continue
        api.subscribe(contract, quote_type=sj.QuoteType.Quote)
        _SHIOAJI_STREAM_SUBSCRIPTIONS.add((code, kind))
    if not _SHIOAJI_STREAM_TARGET_READY:
        unresolved_contracts = {
            (code, "quote") for code in selected if api.contracts.get(code) is None
        }
        if desired - unresolved_contracts <= _SHIOAJI_STREAM_SUBSCRIPTIONS:
            _SHIOAJI_STREAM_TARGET_READY = True
            _SHIOAJI_STREAM_ROTATION_AT = time.monotonic() + _SHIOAJI_STOCK_STREAM_DWELL_SECONDS
    limits, _ = _load_prepared_tw_price_limits(trading_date.isoformat())
    quotes = {}
    selected_set = set(selected)
    requested_set = set(requested)
    with _SHIOAJI_STREAM_LOCK:
        for code in list(_SHIOAJI_STREAM_ROWS):
            if code not in requested_set:
                opening = _SHIOAJI_STREAM_ROWS[code].get("opening", {})
                if _shioaji_stream_opening_values(opening, session_date=trading_date.isoformat()) is not None:
                    _SHIOAJI_STREAM_ROWS[code] = {"opening": opening}
                else:
                    _SHIOAJI_STREAM_ROWS.pop(code, None)
    observed = datetime.now(ZoneInfo("Asia/Taipei"))
    for code in requested:
        with _SHIOAJI_STREAM_LOCK:
            stored = _SHIOAJI_STREAM_ROWS.get(code, {})
            row = dict(stored.get("quote", {}))
            auction = dict(stored.get("auction", {}))
        received = row.get("received_at")
        try:
            age_seconds = (observed - datetime.fromisoformat(received)).total_seconds()
        except (TypeError, ValueError):
            age_seconds = float("inf")
        if (not row.get("exchange_at", "").startswith(trading_date.isoformat())
                or row.get("suspend") is True or not 0 <= age_seconds <= 10):
            row = {}
        auction_received = auction.get("received_at")
        try:
            auction_age = (observed - datetime.fromisoformat(auction_received)).total_seconds()
        except (TypeError, ValueError):
            auction_age = float("inf")
        if (not auction.get("exchange_at", "").startswith(trading_date.isoformat())
                or not 0 <= auction_age <= 10):
            auction = {}
        contract = api.contracts.get(code)
        reference, upper, lower = limits.get(code, (None, None, None))
        upper = upper or _contract_positive(contract, "limit_up")
        lower = lower or _contract_positive(contract, "limit_down")
        reference = reference or _contract_positive(contract, "reference")
        q = {"symbol": code, "source": "shioaji_stock_quote_stream", "available": bool(row),
             "stream_reconciliation_pending": not _SHIOAJI_STREAM_TARGET_READY,
             "stream_subscribed": code in selected_set and (code, "quote") in _SHIOAJI_STREAM_SUBSCRIPTIONS,
             "stream_capacity_limited": code not in selected_set,
             "last": row.get("close"), "open": row.get("open"),
             "high": row.get("high"), "low": row.get("low"),
             "bid": row.get("bid"), "ask": row.get("ask"),
             "bid_volume": row.get("bid_volume"), "ask_volume": row.get("ask_volume"),
             "upper_limit": upper, "lower_limit": lower, "reference_price": reference,
             "cumulative_volume_lots": row.get("total_volume") if row.get("simtrade") is False else None,
             "quote_at": row.get("received_at"), "book_exchange_at": row.get("exchange_at"),
             "exchange_quote_at": row.get("exchange_at"), "simtrade": row.get("simtrade"),
             "trade_simtrade": row.get("simtrade"), "trade_quote_at": row.get("received_at")}
        if auction:
            q["auction_volume_lots"] = auction.get("volume")
            q["auction_volume_source"] = "exchange_non_trial_quote_trade"
            q["last"] = auction.get("close")
            q["trade_simtrade"] = auction.get("simtrade")
            q["trade_quote_at"] = auction.get("received_at")
            q["exchange_quote_at"] = auction.get("exchange_at")
        quotes[code] = q
    return quotes


def _shioaji_stream_opening_values(row: dict, *, session_date: str) -> dict[str, Any] | None:
    """One shared native proof for callback wakeups and requesting workers."""
    if row.get("simtrade") is not False or row.get("suspend") is True:
        return None
    opening = _float_or_none(row.get("open"))
    volume = _quantity_or_none(row.get("total_volume"))
    if opening is None or volume is None or volume <= 0:
        return None
    try:
        market = datetime.fromisoformat(row["exchange_at"])
        receipt = datetime.fromisoformat(row["received_at"])
        if market.tzinfo is None or receipt.tzinfo is None:
            return None
        market_ms, receipt_ms = int(market.timestamp() * 1000), int(receipt.timestamp() * 1000)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return None
    if (not _same_taipei_open_timestamp(market_ms, session_date)
            or not _same_taipei_open_timestamp(receipt_ms, session_date)
            or market_ms > receipt_ms + 1000):
        return None
    values = {name: row.get(name) for name in ("open", "high", "low", "bid", "ask", "bid_volume", "ask_volume")}
    values.update(price=_float_or_none(row.get("close")) or opening,
                  volume=volume, exchange_ms=market_ms, received_ms=receipt_ms,
                  simtrade=0, opening_source="shioaji:stock_quote_stream")
    return values


def _shioaji_stream_opening_rows(
    symbols: list[str], *, session_date: str,
) -> dict[str, tuple[float, dict[str, Any]]]:
    """Detach proven opens, retaining original clocks, without any login/I/O.

    An already-observed open need not become a freshly received executable
    book. Trial, zero-volume, suspended and other-session books stay excluded.
    """
    with _SHIOAJI_STREAM_LOCK:
        candidates = {code: dict(_SHIOAJI_STREAM_ROWS[code].get("opening") or
                                _SHIOAJI_STREAM_ROWS[code].get("quote", {}))
                      for code in symbols if code in _SHIOAJI_STREAM_ROWS}
    result = {}
    for code, row in candidates.items():
        values = _shioaji_stream_opening_values(row, session_date=session_date)
        if values is not None:
            result[code] = (time.monotonic(), values)
    return result


def _shioaji_snapshot_values(
    row: object,
    contract: object,
    *,
    received_ms: int,
) -> dict[str, float | int | None]:
    bid = _float_or_none(getattr(row, "buy_price", None))
    ask = _float_or_none(getattr(row, "sell_price", None))
    close = _float_or_none(getattr(row, "close", None))
    reference = _contract_positive(
        contract, "reference", "reference_price", "ref_price"
    )
    price = close
    if price is None and bid is not None and ask is not None:
        price = (bid + ask) / 2.0
    price = price or bid or ask or reference

    def exchange_timestamp_ms(value: object) -> int | None:
        try:
            raw = int(value)
        except Exception:
            return None
        if raw <= 0:
            return None
        # Shioaji Snapshot.ts is nanoseconds in the current Python binding.
        # Retain compatibility with microsecond/millisecond/second fixtures and
        # older wrappers without changing the local receipt-time boundary.
        if raw >= 100_000_000_000_000_000:
            encoded_ms = raw // 1_000_000
        elif raw >= 100_000_000_000_000:
            encoded_ms = raw // 1_000
        elif raw >= 100_000_000_000:
            encoded_ms = raw
        elif raw >= 1_000_000_000:
            encoded_ms = raw * 1_000
        else:
            return None
        # Native ts encodes Taipei WALL CLOCK, not a UTC epoch. Decode the
        # wall clock and attach Taipei without moving the hour; our normalized
        # PriceSnapshot exchange_timestamps_ms is a genuine UTC epoch.
        try:
            wall = datetime.fromtimestamp(encoded_ms / 1000, timezone.utc).replace(tzinfo=ZoneInfo("Asia/Taipei"))
            return int(wall.timestamp() * 1000)
        except (ValueError, OverflowError, OSError):
            return None

    raw_simtrade = getattr(row, "simtrade", None)
    return {
        "price": price,
        "open": _float_or_none(getattr(row, "open", None)),
        "high": _float_or_none(getattr(row, "high", None)),
        "low": _float_or_none(getattr(row, "low", None)),
        "volume": _quantity_or_none(
            getattr(row, "total_volume", getattr(row, "volume", None))
        ),
        "upper": _contract_positive(
            contract, "limit_up", "limit_up_price", "upper_limit"
        ),
        "lower": _contract_positive(
            contract, "limit_down", "limit_down_price", "lower_limit"
        ),
        "bid": bid,
        "ask": ask,
        "bid_volume": _quantity_or_none(getattr(row, "buy_volume", None)),
        "ask_volume": _quantity_or_none(getattr(row, "sell_volume", None)),
        "reference": reference,
        "exchange_ms": exchange_timestamp_ms(getattr(row, "ts", None)),
        "simtrade": -1 if raw_simtrade is None else int(bool(raw_simtrade)),
        # Causal execution uses the local observation time.  Snapshot ``ts`` is
        # exchange metadata and can predate the request when a symbol is idle.
        "received_ms": int(received_ms),
    }


def _tw_price_limit_snapshot_path(trading_date: str | None = None) -> Path:
    date_text = trading_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    configured = str(os.getenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", "") or "").strip()
    root = Path(configured) if configured else Path("artifacts/live/tw_price_limits")
    return root / f"{date_text}.parquet"


@contextmanager
def _tw_price_limit_prepare_lock(path: Path):
    """Serialize one daily snapshot build across threads and live processes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with _TW_LIMIT_PREPARE_LOCK, lock_path.open("a+b") as lock:
        if os.name == "nt":
            import msvcrt

            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _load_prepared_tw_price_limits(
    trading_date: str | None = None,
) -> tuple[dict[str, tuple[float | None, float | None, float | None]], Path]:
    global _TW_LIMIT_CACHE_KEY, _TW_LIMIT_CACHE
    path = _tw_price_limit_snapshot_path(trading_date)
    if not path.is_file():
        return {}, path
    stat = path.stat()
    key = f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}:{stat.st_ctime_ns}"
    with _TW_LIMIT_CACHE_LOCK:
        if key == _TW_LIMIT_CACHE_KEY:
            return dict(_TW_LIMIT_CACHE), path
    import polars as pl

    try:
        frame = pl.read_parquet(path)
    except (OSError, ValueError, pl.exceptions.PolarsError):
        # This file is a reproducible same-session cache.  A truncated file
        # must behave like a cache miss so the preparation path can repair it;
        # execution still fails closed when neither this cache nor the broker
        # contract supplies authoritative limits.
        return {}, path
    lookup: dict[str, tuple[float | None, float | None, float | None]] = {}
    for row in frame.iter_rows(named=True):
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            continue
        lookup[symbol] = (
            _float_or_none(row.get("reference_price")),
            _float_or_none(row.get("upper_limit_price")),
            _float_or_none(row.get("lower_limit_price")),
        )
    with _TW_LIMIT_CACHE_LOCK:
        _TW_LIMIT_CACHE_KEY = key
        _TW_LIMIT_CACHE = dict(lookup)
    return lookup, path


def _prepare_tw_price_limit_snapshot_locked(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    trading_date: str | None = None,
) -> dict[str, object]:
    """Prepare static TWSE/TPEx reference and limit prices before 09:00.

    Only the non-changing daily limit metadata is retained.  Opening/last and
    Bid/Ask prices remain exclusively sourced from Shioaji on the live path.
    """

    if len(symbols) != len(fallback_prices):
        raise ValueError("symbols and fallback_prices must have equal length")
    date_text = trading_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    existing, path = _load_prepared_tw_price_limits(date_text)
    fallback_by_symbol = {
        str(symbol): float(fallback_prices[idx])
        for idx, symbol in enumerate(symbols)
        if np.isfinite(fallback_prices[idx]) and float(fallback_prices[idx]) > 0.0
    }
    missing = [str(symbol) for symbol in symbols if str(symbol) not in existing]
    if missing:
        fallback = np.asarray(
            [fallback_by_symbol.get(symbol, 1.0) for symbol in missing],
            dtype=np.float64,
        )
        snapshot = fetch_tw_mis_last_prices(
            missing,
            fallback,
            parquet_root=parquet_root,
            chunk_size=80,
        )
        for idx, symbol in enumerate(missing):
            reference = (
                _float_or_none(snapshot.reference_prices[idx])
                if snapshot.reference_prices is not None
                else None
            )
            upper = (
                _float_or_none(snapshot.upper_limit_prices[idx])
                if snapshot.upper_limit_prices is not None
                else None
            )
            lower = (
                _float_or_none(snapshot.lower_limit_prices[idx])
                if snapshot.lower_limit_prices is not None
                else None
            )
            if upper is not None and lower is not None:
                existing[symbol] = (reference, upper, lower)
    if not existing:
        raise RuntimeError("TWSE/TPEx returned no daily price-limit metadata")

    import polars as pl

    prepared_at = datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds")
    rows = [
        {
            "trading_date": date_text,
            "symbol": symbol,
            "reference_price": values[0],
            "upper_limit_price": values[1],
            "lower_limit_price": values[2],
            "prepared_at": prepared_at,
            "source": "twse_tpex:mis_static_limits",
        }
        for symbol, values in sorted(existing.items())
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(
        path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}"
    )
    try:
        pl.DataFrame(rows).write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    # Reload via the stat-keyed cache so readers never retain the pre-replace map.
    loaded, _ = _load_prepared_tw_price_limits(date_text)
    return {
        "path": str(path),
        "trading_date": date_text,
        "requested_count": len(symbols),
        "prepared_count": len(loaded),
        "missing_count": max(0, len(set(symbols)) - len(loaded)),
    }


def prepare_tw_price_limit_snapshot(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    trading_date: str | None = None,
) -> dict[str, object]:
    """Build or extend one valid daily price-limit snapshot exactly once."""

    path = _tw_price_limit_snapshot_path(trading_date)
    with _tw_price_limit_prepare_lock(path):
        return _prepare_tw_price_limit_snapshot_locked(
            symbols,
            fallback_prices,
            parquet_root=parquet_root,
            trading_date=trading_date,
        )


def fetch_shioaji_stock_snapshots(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    cache_ttl_seconds: float = 0.0,
    on_progress: Callable[[list[str], PriceSnapshot], None] | None = None,
    prefer_opening_stream: bool = False,
) -> PriceSnapshot:
    """Fetch full-universe Shioaji stock snapshots in <=500-contract batches.

    The provider is simulation/login only and never submits an order.  Cached
    rows are useful when several models share the same opening observation;
    callers doing one-minute execution marks pass a zero TTL for fresh books.
    """

    for attempt in range(2):
        api = _shioaji_stock_api()
        try:
            return _fetch_shioaji_stock_snapshots_once(
                symbols,
                fallback_prices,
                cache_ttl_seconds=cache_ttl_seconds,
                api=api,
                on_progress=on_progress,
                prefer_opening_stream=prefer_opening_stream,
            )
        except Exception as exc:
            session_failure = _is_shioaji_session_failure(exc)
            if not session_failure and isinstance(exc, _ShioajiSnapshotEmpty):
                # A disconnected non-blocking Snapshot request may only time
                # out its callbacks and surface as an empty batch. Probe the
                # existing session before deciding whether to reconnect; an
                # empty market response or exhausted quota is not proof that
                # another login is appropriate.
                try:
                    api.usage()
                except Exception as probe_exc:
                    session_failure = _is_shioaji_session_failure(probe_exc)
            if attempt > 0 or not session_failure:
                raise
            _reconnect_shioaji_stock_quote_client(api)
    raise AssertionError("unreachable Shioaji stock snapshot retry state")


def fetch_shared_day_trade_stock_snapshots(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    state_dir: str | Path | None = None,
    timeout_seconds: float | None = None,
    purpose: str = "interactive",
    on_progress: Callable[[list[str], PriceSnapshot], None] | None = None,
) -> PriceSnapshot:
    """Ask the long-lived paper engine to use its existing Shioaji session.

    Shioaji logins are a scarce account-level resource.  Discord must not open
    another process-local session merely to refresh an interactive signal while
    the day-trade engine already owns a healthy simulation-only quote session.
    The filesystem request/response boundary is local, atomic, bounded, and
    carries per-row receipt times from the serving process.
    """

    requested = [str(symbol).strip() for symbol in symbols]
    fallback = np.asarray(fallback_prices, dtype=np.float64)
    if len(requested) != len(fallback):
        raise ValueError("symbols and fallback_prices must have equal length")
    if not requested:
        raise ValueError("shared day-trade quote request is empty")
    if len(requested) > 5_000:
        raise ValueError("shared day-trade quote request exceeds 5000 symbols")
    normalized_purpose = str(purpose or "interactive").strip().lower()
    if normalized_purpose not in {"interactive", "latest_quote", "opening_signal"}:
        raise ValueError(f"unsupported shared quote purpose: {purpose!r}")
    timeout = max(
        0.25,
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(
            os.getenv("STOCKAGENT_SHARED_QUOTE_TIMEOUT_SECONDS", "6") or "6"
        ),
    )
    requests_dir, responses_dir = _day_trade_quote_broker_paths(state_dir)
    request_id = uuid.uuid4().hex
    request_path = requests_dir / f"{request_id}.json"
    response_path = responses_dir / f"{request_id}.json"
    client_started = time.perf_counter()
    requested_at = datetime.now(timezone.utc)
    deadline_epoch = time.time() + timeout
    deadline_monotonic = time.monotonic() + timeout
    request_write_started = time.perf_counter()
    _atomic_write_json(
        request_path,
        {
            "schema_version": 1,
            "request_id": request_id,
            "requested_at_utc": requested_at.isoformat(),
            "deadline_epoch": deadline_epoch,
            "symbols": requested,
            "fallback_prices": fallback.tolist(),
            "requester_pid": os.getpid(),
            "purpose": normalized_purpose,
            "incremental": on_progress is not None,
        },
    )
    request_published = time.perf_counter()
    try:
        last_sequence = -1
        last_response_identity = None
        # IPC expiry stays wall-clock based; this local wait must survive
        # NTP/WSL wall-clock corrections and still terminate on its own budget.
        while time.monotonic() < deadline_monotonic:
            response_read_started = time.perf_counter()
            try:
                metadata = response_path.stat()
                identity = (metadata.st_ino, metadata.st_mtime_ns, metadata.st_size)
                if identity == last_response_identity:
                    time.sleep(0.002 if on_progress is not None else 0.01)
                    continue
                payload = json.loads(response_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                time.sleep(0.002 if on_progress is not None else 0.01)
                continue
            except (OSError, json.JSONDecodeError):
                time.sleep(0.01)
                continue
            if str(payload.get("request_id") or "") != request_id:
                raise RuntimeError("shared day-trade quote response id mismatch")
            if payload.get("error"):
                raise RuntimeError(str(payload["error"]))
            last_response_identity = identity
            snapshot_payload = payload.get("snapshot")
            if not isinstance(snapshot_payload, dict):
                raise RuntimeError("shared day-trade quote response has no snapshot")
            snapshot = _price_snapshot_from_payload(snapshot_payload)
            if payload.get("is_final") is False:
                sequence = int(payload.get("sequence") or 0)
                if on_progress is not None and sequence > last_sequence:
                    partial_symbols = [str(value) for value in payload.get("partial_symbols") or ()]
                    if len(partial_symbols) != len(snapshot.prices) or not set(partial_symbols).issubset(requested):
                        raise ValueError("shared partial quote universe mismatch")
                    on_progress(partial_symbols, snapshot)
                    last_sequence = sequence
                time.sleep(0.002)
                continue
            if len(snapshot.prices) != len(requested):
                raise RuntimeError(
                    "shared day-trade quote response size mismatch: "
                    f"received={len(snapshot.prices)} requested={len(requested)}"
                )
            response_observed = time.perf_counter()
            broker_timing = payload.get("broker_timing")
            timing = dict(snapshot.transport_timing or {})
            if isinstance(broker_timing, dict):
                timing.update(broker_timing)
            timing.update(
                {
                    "schema_version": 1,
                    "clock": "monotonic_durations_and_utc_wall_timestamps",
                    "client_requested_at_utc": requested_at.isoformat(),
                    "client_response_observed_at_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "client_request_write_ms": round(
                        (request_published - request_write_started) * 1000.0,
                        3,
                    ),
                    "client_broker_wait_ms": round(
                        (response_read_started - request_published) * 1000.0,
                        3,
                    ),
                    "client_response_read_decode_ms": round(
                        (response_observed - response_read_started) * 1000.0,
                        3,
                    ),
                    "client_round_trip_ms": round(
                        (response_observed - request_published) * 1000.0,
                        3,
                    ),
                    "client_total_ms": round(
                        (response_observed - client_started) * 1000.0,
                        3,
                    ),
                }
            )
            snapshot.transport_timing = timing
            snapshot.source = f"{snapshot.source}+shared_day_trade_engine"
            return snapshot
        raise TimeoutError(
            "shared day-trade quote engine did not respond within "
            f"{timeout:.3f}s"
        )
    finally:
        request_path.unlink(missing_ok=True)
        response_path.unlink(missing_ok=True)


class SharedDayTradeQuoteBroker:
    """One off-loop worker in the existing engine process; no extra login."""

    def __init__(self, state_dir: str | Path):
        self.state_dir = Path(state_dir)
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tw-shared-quote")
        self._future: Future | None = None

    def poll(self, *, allowed_purposes: set[str] | None = None) -> list[dict[str, Any]]:
        results = []
        if self._future is not None:
            if not self._future.done():
                return results
            try:
                results = self._future.result()
            except Exception as exc:
                results = [{"error": type(exc).__name__, "purpose": "broker_worker"}]
            self._future = None
        requests_dir, _ = _day_trade_quote_broker_paths(self.state_dir)
        if any(requests_dir.glob("*.json")):
            self._future = self._pool.submit(serve_shared_day_trade_quote_requests, state_dir=self.state_dir, max_requests=1, allowed_purposes=allowed_purposes)
        return results

    def close(self) -> None:
        self._pool.shutdown(wait=True, cancel_futures=True)


def serve_shared_day_trade_quote_requests(
    *,
    state_dir: str | Path | None = None,
    max_requests: int = 1,
    allowed_purposes: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Serve bounded local quote requests inside the paper-engine process."""

    requests_dir, responses_dir = _day_trade_quote_broker_paths(state_dir)
    results: list[dict[str, Any]] = []
    normalized_allowed = (
        {str(value).strip().lower() for value in allowed_purposes}
        if allowed_purposes is not None
        else None
    )
    request_paths = sorted(
        requests_dir.glob("*.json"),
        key=lambda path: path.stat().st_mtime_ns,
    )
    now_epoch = time.time()
    for request_path in request_paths:
        if len(results) >= max(0, int(max_requests)):
            break
        request_id = request_path.stem
        response_path = responses_dir / f"{request_id}.json"
        server_received = datetime.now(timezone.utc)
        server_started = time.perf_counter()
        response: dict[str, Any] = {
            "schema_version": 1,
            "request_id": request_id,
            "served_at_utc": server_received.isoformat(),
            "server_pid": os.getpid(),
        }
        request_parse_ms = 0.0
        provider_fetch_ms = 0.0
        snapshot_serialize_ms = 0.0
        request_queue_ms: float | None = None
        try:
            parse_started = time.perf_counter()
            request = json.loads(request_path.read_text(encoding="utf-8"))
            request_parse_ms = (time.perf_counter() - parse_started) * 1000.0
            request_purpose = str(
                request.get("purpose") or "interactive"
            ).strip().lower()
            if (
                normalized_allowed is not None
                and request_purpose not in normalized_allowed
            ):
                continue
            if str(request.get("request_id") or "") != request_id:
                raise ValueError("quote request id does not match its filename")
            try:
                requested_at = datetime.fromisoformat(
                    str(request.get("requested_at_utc") or "").replace(
                        "Z", "+00:00"
                    )
                )
                if requested_at.tzinfo is None:
                    requested_at = requested_at.replace(tzinfo=timezone.utc)
                request_queue_ms = max(
                    0.0,
                    (server_received - requested_at.astimezone(timezone.utc))
                    .total_seconds()
                    * 1000.0,
                )
            except (TypeError, ValueError):
                request_queue_ms = None
            deadline_epoch = float(request.get("deadline_epoch") or 0.0)
            if deadline_epoch <= now_epoch:
                raise TimeoutError("shared quote request expired before service")
            progress_deadline = time.monotonic() + max(0.0, deadline_epoch - now_epoch)
            symbols = [str(value).strip() for value in request.get("symbols") or ()]
            fallback = np.asarray(
                request.get("fallback_prices") or (), dtype=np.float64
            )
            if not symbols or len(symbols) != len(fallback):
                raise ValueError("shared quote request symbols/fallback are invalid")
            if len(symbols) > 5_000:
                raise ValueError("shared quote request exceeds 5000 symbols")
            provider_started = time.perf_counter()
            progress_kwargs = {}
            if request_purpose == "opening_signal" and request.get("incremental") is True:
                progress_sequence = [0]

                def publish_progress(partial_symbols, partial):
                    if time.monotonic() >= progress_deadline or not request_path.exists():
                        return
                    progress_sequence[0] += 1
                    # Cumulative full-universe progress avoids losing an early
                    # batch when the next callback arrives before client read.
                    _atomic_write_json(response_path, {
                        **response, "is_final": False, "sequence": progress_sequence[0],
                        "partial_symbols": partial_symbols,
                        "snapshot": _price_snapshot_payload(partial),
                    })

                progress_kwargs["on_progress"] = publish_progress
            snapshot = fetch_shioaji_stock_snapshots(
                symbols,
                fallback,
                cache_ttl_seconds=0.0,
                prefer_opening_stream=request_purpose == "opening_signal",
                **progress_kwargs,
            )
            provider_fetch_ms = (time.perf_counter() - provider_started) * 1000.0
            serialize_started = time.perf_counter()
            response["snapshot"] = _price_snapshot_payload(snapshot)
            snapshot_serialize_ms = (
                time.perf_counter() - serialize_started
            ) * 1000.0
            response["available_count"] = int(snapshot.available_count)
            response["requested_count"] = len(symbols)
            response["purpose"] = request_purpose
            response["is_final"] = True
        except Exception as exc:
            response["error"] = f"{type(exc).__name__}: {exc}"
        response["broker_timing"] = {
            "server_received_at_utc": server_received.isoformat(),
            "server_response_ready_at_utc": datetime.now(timezone.utc).isoformat(),
            "server_request_queue_ms": (
                round(request_queue_ms, 3) if request_queue_ms is not None else None
            ),
            "server_request_parse_ms": round(request_parse_ms, 3),
            "server_provider_fetch_ms": round(provider_fetch_ms, 3),
            "server_snapshot_serialize_ms": round(snapshot_serialize_ms, 3),
            "server_total_before_publish_ms": round(
                (time.perf_counter() - server_started) * 1000.0,
                3,
            ),
        }
        try:
            _atomic_write_json(response_path, response)
        finally:
            request_path.unlink(missing_ok=True)
        results.append(response)

    # A requester may die after publication.  Keep the broker self-cleaning
    # without touching any ledger or signal artifact.
    stale_before = time.time() - 300.0
    for directory in (requests_dir, responses_dir):
        for path in directory.glob("*.json"):
            try:
                if path.stat().st_mtime < stale_before:
                    path.unlink(missing_ok=True)
            except FileNotFoundError:
                pass
    return results


def _fetch_shioaji_stock_snapshots_once(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    cache_ttl_seconds: float,
    api: object,
    on_progress: Callable[[list[str], PriceSnapshot], None] | None = None,
    prefer_opening_stream: bool = False,
) -> PriceSnapshot:
    snapshot_started = time.perf_counter()
    query_timing: dict[str, Any] = {}
    if len(symbols) != len(fallback_prices):
        raise ValueError("symbols and fallback_prices must have equal length")
    ttl = max(0.0, float(cache_ttl_seconds))
    now_monotonic = time.monotonic()
    requested = [str(symbol).strip() for symbol in symbols]
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    stream_rows = _shioaji_stream_opening_rows(requested, session_date=session_date) if prefer_opening_stream else {}
    if stream_rows and on_progress is not None:
        on_progress(requested, _shioaji_opening_progress_snapshot(
            requested, fallback_prices, [], {}, normalized_rows=stream_rows,
            source="shioaji:stock_quote_stream",
        ))
    snapshot_phases: dict[str, float | None] = {}
    lock_requested = time.perf_counter()

    with _SHIOAJI_STOCK_LOCK:
        lock_acquired = time.perf_counter()
        snapshot_phases["lock_queue_ms"] = (lock_acquired - lock_requested) * 1000.0
        missing_codes: list[str] = []
        for code in requested:
            if code in stream_rows:
                continue
            cached = _SHIOAJI_STOCK_CACHE.get(code)
            if ttl > 0.0 and cached is not None and now_monotonic - cached[0] <= ttl:
                continue
            # Retry transient Contract V2 misses instead of pinning ``None``
            # until a process restart.
            if _SHIOAJI_STOCK_CONTRACTS.get(code) is None:
                _SHIOAJI_STOCK_CONTRACTS[code] = api.contracts.get(code)
            if _SHIOAJI_STOCK_CONTRACTS[code] is not None:
                # Once a row exceeds its caller-approved TTL it is no longer
                # evidence for this request.  Evict it before querying so an
                # empty or partial provider response cannot silently revive an
                # older Bid/Ask as a fresh executable quote.
                _SHIOAJI_STOCK_CACHE.pop(code, None)
                missing_codes.append(code)

        available_contracts = sum(
            _SHIOAJI_STOCK_CONTRACTS.get(code) is not None for code in requested
        )
        uncached_batches = (available_contracts + 499) // 500
        queried_batches = (len(missing_codes) + 499) // 500
        avoided_batches = max(0, uncached_batches - queried_batches)
        if avoided_batches:
            record_avoided_query(
                consumer="stock_quote_provider",
                method="snapshots",
                asset_class="stock",
                reason="process_cache_hit",
                count=avoided_batches,
                rows=max(0, available_contracts - len(missing_codes)),
                details={
                    "symbol_count": available_contracts,
                    "cache_scope": "process",
                },
            )

        try:
            configured_snapshot_timeout_ms = int(
                os.environ.get(
                    "STOCKAGENT_SHIOAJI_SNAPSHOT_TIMEOUT_MS",
                    "3000",
                )
            )
        except (TypeError, ValueError):
            configured_snapshot_timeout_ms = 3_000
        snapshot_timeout_ms = max(
            250,
            min(5_000, configured_snapshot_timeout_ms),
        )
        snapshot_batches: list[list[object]] = []
        for start in range(0, len(missing_codes), 500):
            batch_codes = missing_codes[start : start + 500]
            contracts = [
                _SHIOAJI_STOCK_CONTRACTS[code]
                for code in batch_codes
                if _SHIOAJI_STOCK_CONTRACTS[code] is not None
            ]
            if contracts:
                snapshot_batches.append(contracts)

        snapshot_phases["contract_prepare_ms"] = (time.perf_counter() - lock_acquired) * 1000.0

        request_contracts = {code: _SHIOAJI_STOCK_CONTRACTS.get(code) for code in requested}
        request_rows = {code: _SHIOAJI_STOCK_CACHE[code] for code in requested if code in _SHIOAJI_STOCK_CACHE}
        request_rows.update(stream_rows)

    if snapshot_batches:
        # Shioaji accepts at most 500 contracts per Snapshot request. A
        # Taiwan universe therefore needs up to six requests. Waiting for
        # each response before issuing the next turns network latency into
        # a sixfold critical-path cost. ``timeout=0`` queues every request
        # on the same authenticated connection; each batch callback returns
        # one complete List[Snapshot]. The callback only timestamps and
        # hands the immutable list to this waiting thread, where parsing and
        # cache mutation remain serialized.
        condition = threading.Condition()
        callback_rows: list[tuple[list[object], int] | None] = [
            None for _ in snapshot_batches
        ]
        callback_offsets: list[float | None] = [None for _ in snapshot_batches]
        callbacks_open = [True]
        progress_batches: set[int] = set()
        parsed_batches: dict[int, dict[str, tuple[float, dict[str, Any]]]] = {}
        callback_parse_ms = 0.0
        callback_normalized_rows = 0
        stream_signature = tuple((code, row[1]["open"]) for code, row in stream_rows.items())

        def parse_completed_batches() -> None:
            nonlocal callback_parse_ms, callback_normalized_rows
            with condition:
                completed = [(i, result) for i, result in enumerate(callback_rows)
                             if result is not None and i not in parsed_batches]
            parse_started = time.perf_counter()
            for batch_index, (rows, received_ms) in completed:
                values_by_code = {}
                for row in rows:
                    code = str(getattr(row, "code", "") or "").strip()
                    contract = request_contracts.get(code)
                    if not code or contract is None:
                        continue
                    values_by_code[code] = (time.monotonic(),
                        _shioaji_snapshot_values(row, contract, received_ms=received_ms))
                    callback_normalized_rows += 1
                parsed_batches[batch_index] = values_by_code
            callback_parse_ms += (time.perf_counter() - parse_started) * 1000.0

        def normalized_callback_rows():
            # Preserve native batch/row precedence even when callbacks arrive
            # out of order. Raw SDK rows are decoded once, on this worker.
            return {code: values for index in sorted(parsed_batches)
                    for code, values in parsed_batches[index].items()}

        def emit_progress() -> None:
            nonlocal stream_rows, stream_signature
            if on_progress is None:
                return
            # Parse on the requesting worker, never the native delivery
            # callback. Report cumulative rows so the local broker file
            # may be coalesced without dropping earlier opening facts.
            completed_indices = set(parsed_batches)
            latest_stream = _shioaji_stream_opening_rows(requested, session_date=session_date) if prefer_opening_stream else {}
            # Opening facts are immutable within this request. A later trial,
            # missing field or subscription rotation cannot erase an accepted
            # native observation or refresh its original receipt timestamp.
            for code, row in latest_stream.items():
                stream_rows.setdefault(code, row)
            latest_signature = tuple((code, row[1]["open"]) for code, row in stream_rows.items())
            if completed_indices == progress_batches and latest_signature == stream_signature:
                return
            progress_batches.update(completed_indices)
            stream_signature = latest_signature
            on_progress(requested, _shioaji_opening_progress_snapshot(
                requested, fallback_prices, [], request_contracts,
                normalized_rows=stream_rows, snapshot_rows=normalized_callback_rows(),
                response_count=sum(len(callback_rows[i][0]) for i in completed_indices),
            ))

        def make_callback(batch_index: int) -> Callable[[Any], None]:
            def receive(rows: Any) -> None:
                received_ms = int(time.time() * 1000)
                received_elapsed_ms = (time.perf_counter() - request_started) * 1000.0
                try:
                    materialized = list(rows or ())
                except TypeError:
                    materialized = []
                with condition:
                    if (
                        not callbacks_open[0]
                        or callback_rows[batch_index] is not None
                    ):
                        return
                    callback_rows[batch_index] = (materialized, received_ms)
                    callback_offsets[batch_index] = received_elapsed_ms
                    condition.notify_all()

            return receive

        query_details: dict[str, Any] = {
            "contract_count": sum(len(batch) for batch in snapshot_batches),
            "batch_count": len(snapshot_batches),
            "timeout_ms": snapshot_timeout_ms,
            "nonblocking": True,
            "stream_open_rows_reused": len(stream_rows),
        }
        with shioaji_query(
            api,
            consumer="stock_quote_provider",
            method="snapshots",
            asset_class="stock",
            details=query_details,
            request_count=len(snapshot_batches),
            # Account usage is not a dependency of a valid quote. Two
            # synchronous quota reads blocked the 2026-10-05 opening for
            # 8.66 seconds and triggered a second-process fallback. Keep
            # query/row/failure accounting, with unknown byte attribution.
            observe_usage=False,
            timing=query_timing,
        ) as set_ledger_result:
            request_started = time.perf_counter()
            query_admission_ms = 0.0
            for batch_index, contracts in enumerate(snapshot_batches):
                callback = make_callback(batch_index)
                try:
                    admission_started = time.perf_counter()
                    _wait_stock_quote_query_slot()
                    query_admission_ms += (time.perf_counter() - admission_started) * 1000.0
                    immediate = api.snapshots(
                        contracts,
                        timeout=0,
                        cb=callback,
                    )
                except BaseException:
                    # Native callbacks can outlive a failed submission.
                    # Close this request generation before propagating so
                    # late rows cannot mutate even its local aggregation.
                    with condition:
                        callbacks_open[0] = False
                    raise
                # The native API normally returns an empty placeholder in
                # non-blocking mode. Preserve compatibility with a client
                # that can complete synchronously without waiting twice.
                if immediate:
                    callback(immediate)

            submissions_completed = time.perf_counter()
            snapshot_phases["query_admission_ms"] = query_admission_ms
            snapshot_phases["submit_ms"] = (submissions_completed - request_started) * 1000.0
            deadline = time.monotonic() + snapshot_timeout_ms / 1000.0
            if prefer_opening_stream:
                with _SHIOAJI_STREAM_LOCK:
                    _SHIOAJI_OPENING_CONDITIONS.add(condition)
            try:
                while True:
                    stream_revision = _SHIOAJI_OPENING_REVISION
                    with condition:
                        observed_count = sum(result is not None for result in callback_rows)
                    # A callback may already have arrived during submission;
                    # publish it before sleeping for a slower later batch.
                    parse_completed_batches()
                    emit_progress()
                    if observed_count == len(callback_rows) or time.monotonic() >= deadline:
                        break
                    with condition:
                        # Do not lose a notification arriving between the
                        # detached progress callback and entering this wait.
                        if (sum(result is not None for result in callback_rows) != observed_count
                                or (prefer_opening_stream and _SHIOAJI_OPENING_REVISION != stream_revision)):
                            continue
                        remaining = deadline - time.monotonic()
                        if remaining > 0:
                            condition.wait(timeout=remaining)
            finally:
                if prefer_opening_stream:
                    with _SHIOAJI_STREAM_LOCK:
                        _SHIOAJI_OPENING_CONDITIONS.discard(condition)
                with condition:
                    callbacks_open[0] = False

            snapshot_phases["callback_wait_ms"] = (time.perf_counter() - submissions_completed) * 1000.0
            observed_offsets = [value for value in callback_offsets if value is not None]
            snapshot_phases["first_callback_ms"] = min(observed_offsets) if observed_offsets else None
            snapshot_phases["last_callback_ms"] = max(observed_offsets) if observed_offsets else None

            completed_batches = sum(
                result is not None for result in callback_rows
            )
            query_details["completed_batch_count"] = completed_batches
            query_details["timed_out_batch_count"] = (
                len(snapshot_batches) - completed_batches
            )
            rows_with_receipts = [
                (row, result[1])
                for result in callback_rows
                if result is not None
                for row in result[0]
            ]
            set_ledger_result(rows_with_receipts)

        # The causal observation boundary is the callback receipt time,
        # never request submission time nor the exchange wall-clock field.
        # A callback can arrive between the last loop read and deadline fence.
        # Include it, without re-decoding already published cumulative batches.
        parse_completed_batches()
        normalized_rows = normalized_callback_rows()
        request_rows.update(normalized_rows)
        for code, normalized in normalized_rows.items():
            received_ms = int(normalized[1].get("received_ms") or 0)
            with _SHIOAJI_STOCK_LOCK:
                previous = _SHIOAJI_STOCK_CACHE.get(code)
                if _SHIOAJI_STOCK_API is api and (previous is None or int(previous[1].get("received_ms") or 0) <= received_ms):
                    _SHIOAJI_STOCK_CACHE[code] = normalized
        snapshot_phases["callback_parse_ms"] = callback_parse_ms
        snapshot_phases["callback_normalized_rows_count"] = callback_normalized_rows
    if prefer_opening_stream:
        for code, row in _shioaji_stream_opening_rows(requested, session_date=session_date).items():
            stream_rows.setdefault(code, row)
        request_rows.update(stream_rows)

    assemble_started = time.perf_counter()
    size = len(requested)
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    available = np.zeros((size,), dtype=bool)
    arrays = {
        name: np.full((size,), np.nan, dtype=np.float64)
        for name in (
            "open",
            "high",
            "low",
            "volume",
            "upper",
            "lower",
            "bid",
            "ask",
            "bid_volume",
            "ask_volume",
            "reference",
        )
    }
    timestamps_ms = np.zeros((size,), dtype=np.int64)
    exchange_timestamps_ms = np.zeros((size,), dtype=np.int64)
    simtrade_flags = np.full((size,), -1, dtype=np.int8)
    for idx, code in enumerate(requested):
        cached = request_rows.get(code)
        if cached is None:
            continue
        values = cached[1]
        if code in stream_rows:
            # Opening preference must not discard known legal reference/bounds
            # solely because its price came from push instead of Snapshot.
            values = dict(values)
            contract = request_contracts.get(code) or api.contracts.get(code)
            values.update(reference=_contract_positive(contract, "reference", "reference_price", "ref_price"),
                          upper=_contract_positive(contract, "limit_up", "limit_up_price", "upper_limit"),
                          lower=_contract_positive(contract, "limit_down", "limit_down_price", "lower_limit"))
        price = values.get("price")
        if price is not None:
            prices[idx] = float(price)
            available[idx] = True
        for name, target in arrays.items():
            value = values.get(name)
            if value is not None:
                target[idx] = float(value)
        timestamps_ms[idx] = int(values.get("received_ms") or 0)
        exchange_timestamps_ms[idx] = int(values.get("exchange_ms") or 0)
        simtrade_flags[idx] = int(values.get("simtrade", -1))
    snapshot_phases["output_assemble_ms"] = (time.perf_counter() - assemble_started) * 1000.0

    limits_started = time.perf_counter()
    prepared_limits, _limit_path = _load_prepared_tw_price_limits()
    prepared_count = 0
    for idx, code in enumerate(requested):
        values = prepared_limits.get(code)
        if values is None:
            continue
        reference, upper, lower = values
        if reference is not None:
            arrays["reference"][idx] = reference
        if upper is not None:
            arrays["upper"][idx] = upper
        if lower is not None:
            arrays["lower"][idx] = lower
        if upper is not None and lower is not None:
            prepared_count += 1
    # A subset of checkpoint symbols can be absent from the static MIS file
    # (suspensions, legacy listings, or an exchange-side partial response).  For
    # a Shioaji-observed live symbol, derive the remaining legal order bounds
    # only from Shioaji's same-session official auction reference.  Previous
    # close is not interchangeable with that reference around ex-rights,
    # ex-dividend, capital reduction, or other reference-price adjustments.
    # If the official reference is absent, leave both limits missing so the
    # execution layer fails closed; never fabricate them from panel fallback.
    from stockagent.data.tw_price_rules import limit_price_numpy
    from stockagent.data.tw_security import classify_tw_stock_or_etf

    trading_date = np.datetime64(
        datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat(), "D"
    )
    official_reference = arrays["reference"]
    derived_upper = limit_price_numpy(official_reference, 1.10, trading_date)
    derived_lower = limit_price_numpy(official_reference, 0.90, trading_date)
    derived_count = 0
    for idx in range(len(requested)):
        # ETF limits can be leveraged or absent. A stock's 10% formula is not
        # an admissible fallback even when the ETF auction reference exists.
        if not available[idx] or classify_tw_stock_or_etf(requested[idx]) != "stock":
            continue
        if np.isfinite(arrays["upper"][idx]) and np.isfinite(arrays["lower"][idx]):
            continue
        reference = official_reference[idx]
        upper = derived_upper[idx]
        lower = derived_lower[idx]
        if not (
            np.isfinite(reference)
            and reference > 0.0
            and np.isfinite(upper)
            and np.isfinite(lower)
        ):
            continue
        arrays["reference"][idx] = reference
        arrays["upper"][idx] = upper
        arrays["lower"][idx] = lower
        derived_count += 1

    # Shioaji Snapshot can encode a locked limit book with both best prices as
    # zero while still returning the one-sided queue volume.  Restore only the
    # executable side from the already-resolved legal limit: a limit-up queue
    # is an executable bid, and a limit-down queue is an executable ask.  The
    # empty opposite side must stay missing so buys at limit-up and sells at
    # limit-down never become invented fills.
    finite_last = np.isfinite(prices)
    finite_upper = np.isfinite(arrays["upper"])
    finite_lower = np.isfinite(arrays["lower"])
    locked_limit_up = (
        available
        & finite_last
        & finite_upper
        & np.isclose(prices, arrays["upper"], rtol=0.0, atol=1e-8)
        & ~np.isfinite(arrays["bid"])
        & np.isfinite(arrays["bid_volume"])
        & (arrays["bid_volume"] > 0.0)
    )
    locked_limit_down = (
        available
        & finite_last
        & finite_lower
        & np.isclose(prices, arrays["lower"], rtol=0.0, atol=1e-8)
        & ~np.isfinite(arrays["ask"])
        & np.isfinite(arrays["ask_volume"])
        & (arrays["ask_volume"] > 0.0)
    )
    arrays["bid"][locked_limit_up] = arrays["upper"][locked_limit_up]
    arrays["ask"][locked_limit_down] = arrays["lower"][locked_limit_down]
    locked_limit_repair_count = int(locked_limit_up.sum() + locked_limit_down.sum())

    count = int(available.sum())
    if count <= 0:
        error = (
            _ShioajiSnapshotEmpty
            if snapshot_batches
            else RuntimeError
        )
        raise error("Shioaji returned no usable stock snapshots")
    latest_received_ms = int(timestamps_ms.max(initial=0))
    timestamp = (
        datetime.fromtimestamp(latest_received_ms / 1000.0, tz=timezone.utc).isoformat()
        if latest_received_ms > 0
        else None
    )
    source = (
        "shioaji:stock_snapshot+prepared_limits+derived_missing_limits"
        if prepared_count > 0 and derived_count > 0
        else "shioaji:stock_snapshot+prepared_limits"
        if prepared_count > 0
        else "shioaji:stock_snapshot+derived_limits"
        if derived_count > 0
        else "shioaji:stock_snapshot"
    )
    if locked_limit_repair_count > 0:
        source += "+locked_limit_book_repair"
    if snapshot_batches:
        source += "+nonblocking_batch_callbacks"
    if stream_rows:
        source += "+native_opening_stream"
    snapshot_phases["limit_resolve_ms"] = (time.perf_counter() - limits_started) * 1000.0
    return PriceSnapshot(
        prices=prices,
        source=source,
        timestamp=timestamp,
        available_count=count,
        requested_count=len(requested),
        available_mask=available,
        open_prices=arrays["open"],
        high_prices=arrays["high"],
        low_prices=arrays["low"],
        volumes=arrays["volume"],
        upper_limit_prices=arrays["upper"],
        lower_limit_prices=arrays["lower"],
        bid_prices=arrays["bid"],
        ask_prices=arrays["ask"],
        bid_volumes=arrays["bid_volume"],
        ask_volumes=arrays["ask_volume"],
        reference_prices=arrays["reference"],
        timestamps_ms=timestamps_ms,
        exchange_timestamps_ms=exchange_timestamps_ms,
        simtrade_flags=simtrade_flags,
        transport_timing={
            "snapshot_opening_input_policy": "proven_native_open_then_snapshot_gap_v1" if prefer_opening_stream else "snapshot_query_v1",
            "snapshot_normalization_policy": "single_decode_per_callback_row_v1",
            "snapshot_opening_fact_policy": "formal_open_separate_from_latest_book_v1" if prefer_opening_stream else None,
            "snapshot_stream_open_count": len(stream_rows),
            "snapshot_queried_contract_count": len(missing_codes),
            "snapshot_opening_stream_subscription_limit": _SHIOAJI_STOCK_STREAM_LIMIT,
            "snapshot_exchange_clock": "shioaji_taipei_wall_clock_normalized_utc_ms_v1",
            # Nested spans explain the request body; they are not additional
            # sequential costs and must not be summed with snapshot_total_ms.
            **{f"snapshot_{key}": round(value, 3) if value is not None else None
               for key, value in snapshot_phases.items()},
            "snapshot_usage_observation": query_timing.get("usage_observation"),
            **{
                f"snapshot_{key}": query_timing.get(key)
                for key in (
                    "usage_before_ms", "request_body_ms", "usage_after_ms",
                    "ledger_record_ms", "total_context_ms", "ledger_record_failed",
                )
            },
            "snapshot_total_ms": round((time.perf_counter() - snapshot_started) * 1000.0, 3),
        },
    )


def _shioaji_opening_progress_snapshot(symbols, fallback, rows, contracts, *, normalized_rows=None,
                                     snapshot_rows=None, response_count=None, source=None) -> PriceSnapshot:
    """Opening inputs only, from actual callback rows and their receipt clocks."""
    size = len(symbols)
    positions = {symbol: i for i, symbol in enumerate(symbols)}
    prices = np.asarray(fallback, dtype=np.float64).copy()
    opens = np.full(size, np.nan)
    available = np.zeros(size, dtype=bool)
    received, market = np.zeros(size, dtype=np.int64), np.zeros(size, dtype=np.int64)
    flags = np.full(size, -1, dtype=np.int8)
    decoded = dict(snapshot_rows or {})
    for row, received_ms in rows:
        code = str(getattr(row, "code", "") or "")
        if code not in positions or contracts.get(code) is None:
            continue
        values = _shioaji_snapshot_values(row, contracts[code], received_ms=received_ms)
        decoded[code] = (0., values)
    for code, (_at, values) in decoded.items():
        if code not in positions:
            continue
        i = positions[code]
        if values.get("price") is not None:
            prices[i], available[i] = float(values["price"]), True
        opens[i] = values.get("open") or np.nan
        received[i], market[i], flags[i] = int(values.get("received_ms") or 0), int(values.get("exchange_ms") or 0), int(values.get("simtrade", -1))
    for code, (_at, values) in (normalized_rows or {}).items():
        if code not in positions:
            continue
        i = positions[code]
        prices[i], opens[i], available[i] = values["price"], values["open"], True
        received[i], market[i], flags[i] = values["received_ms"], values["exchange_ms"], 0
    source = source or ("shioaji:stock_snapshot+incremental_callback+native_opening_stream" if normalized_rows else "shioaji:stock_snapshot+incremental_callback")
    return PriceSnapshot(prices=prices, source=source, open_prices=opens, available_mask=available, available_count=int(available.sum()), requested_count=size, timestamps_ms=received, exchange_timestamps_ms=market, simtrade_flags=flags, transport_timing={"snapshot_exchange_clock": "shioaji_taipei_wall_clock_normalized_utc_ms_v1", "snapshot_raw_response_count": len(rows) if response_count is None else response_count, "snapshot_stream_open_count": len(normalized_rows or {})})


def fetch_shioaji_futures_snapshot(
    logical_code: str = "TXFR1",
    *,
    additional_contract_codes: tuple[str, ...] = (),
) -> dict[str, object]:
    """Retry one futures observation only after an evidenced lost session."""

    for attempt in range(2):
        api = _shioaji_stock_api()
        try:
            return _fetch_shioaji_futures_snapshot_once(
                logical_code,
                additional_contract_codes=additional_contract_codes,
                api=api,
            )
        except Exception as exc:
            if attempt > 0 or not _is_shioaji_session_failure(exc):
                raise
            _reconnect_shioaji_stock_quote_client(api)
    raise AssertionError("unreachable Shioaji futures snapshot retry state")


def _fetch_shioaji_futures_snapshot_once(
    logical_code: str = "TXFR1",
    *,
    additional_contract_codes: tuple[str, ...] = (),
    api: object,
) -> dict[str, object]:
    """Fetch the current continuous future and optional old roll contract.

    This reuses the process-local simulation-only Shioaji quote connection used
    for stock snapshots.  ``logical_code`` is resolved through Contract V2's
    ``target_code`` on every call so a front-month change is observable.  An
    old concrete code can be requested at the same instant, which lets the
    benchmark require an executable old bid and new ask before recording a
    roll instead of inventing a splice price.
    """

    normalized_logical = str(logical_code or "").strip().upper()
    if not normalized_logical:
        raise ValueError("logical futures code is required")
    with _SHIOAJI_STOCK_LOCK:
        logical = api.contracts.get(normalized_logical)
        if logical is None:
            raise LookupError(
                f"Shioaji futures contract not found: {normalized_logical}"
            )
        target_code = str(getattr(logical, "target_code", "") or "").strip().upper()
        concrete = api.contracts.get(target_code) if target_code else logical
        if concrete is None:
            raise LookupError(
                f"Shioaji futures target contract not found: {target_code or normalized_logical}"
            )
        current_code = str(getattr(concrete, "code", "") or target_code).strip().upper()
        if not current_code:
            raise LookupError(
                f"Shioaji futures target has no concrete code: {normalized_logical}"
            )

        contracts: dict[str, object] = {current_code: concrete}
        for raw_code in additional_contract_codes:
            code = str(raw_code or "").strip().upper()
            if not code or code in contracts:
                continue
            contract = api.contracts.get(code)
            if contract is not None:
                contracts[code] = contract
        requested_contracts = list(contracts.values())
        with shioaji_query(
            api,
            consumer="tw_day_trade_futures_benchmark",
            method="snapshots",
            asset_class="futures",
            observe_usage=False,
            details={
                "logical_code": normalized_logical,
                "contract_count": len(requested_contracts),
                "contracts": list(contracts),
            },
        ) as set_ledger_result:
            rows = list(api.snapshots(requested_contracts))
            received_ms = int(time.time() * 1000)
            set_ledger_result(rows)
        quotes: dict[str, dict[str, float | int | str | None]] = {}
        for row in rows:
            code = str(getattr(row, "code", "") or "").strip().upper()
            contract = contracts.get(code)
            if not code or contract is None:
                continue
            values = _shioaji_snapshot_values(
                row,
                contract,
                received_ms=received_ms,
            )
            delivery_date = getattr(contract, "delivery_date", None)
            last_trading_date = getattr(contract, "last_trading_date", None)
            quotes[code] = {
                "contract_code": code,
                "last": values.get("price"),
                "bid": values.get("bid"),
                "ask": values.get("ask"),
                "bid_volume": values.get("bid_volume"),
                "ask_volume": values.get("ask_volume"),
                "quote_at": datetime.fromtimestamp(
                    received_ms / 1000.0,
                    tz=timezone.utc,
                )
                .astimezone(ZoneInfo("Asia/Taipei"))
                .isoformat(timespec="milliseconds"),
                "source": "shioaji:futures_snapshot",
                "logical_code": normalized_logical if code == current_code else None,
                "delivery_month": str(getattr(contract, "delivery_month", "") or ""),
                "delivery_date": (
                    delivery_date.isoformat()
                    if hasattr(delivery_date, "isoformat")
                    else str(delivery_date or "")
                ),
                "last_trading_date": (
                    last_trading_date.isoformat()
                    if hasattr(last_trading_date, "isoformat")
                    else str(last_trading_date or delivery_date or "")
                ),
            }
        if current_code not in quotes:
            raise RuntimeError(
                f"Shioaji returned no usable current futures snapshot: {current_code}"
            )
        return {
            "logical_code": normalized_logical,
            "current_contract_code": current_code,
            "quotes": quotes,
            "received_at": quotes[current_code]["quote_at"],
            "source": "shioaji:futures_snapshot+contract_v2_target",
        }


def fetch_futures_snapshot_prefer_stream(
    logical_code: str = "TXFR1",
    *,
    additional_contract_codes: tuple[str, ...] = (),
    decision_time: datetime | None = None,
    max_age_seconds: float = 2.0,
    capture_root: str | Path | None = None,
) -> dict[str, object]:
    """Use the causally available local FOP book, with the API as exact fallback."""

    observed = decision_time or datetime.now(ZoneInfo("Asia/Taipei"))
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=ZoneInfo("Asia/Taipei"))
    decision_ns = int(observed.timestamp() * 1_000_000_000)
    normalized_logical = str(logical_code or "").strip().upper()
    logical_root = normalized_logical.removesuffix("R1").removesuffix("R2")
    selected_root = (
        Path(capture_root)
        if capture_root is not None
        else Path(__file__).resolve().parents[2]
        / "data_tw_index_derivatives_ticks/shioaji_fop_captures"
    )
    books: dict[str, dict[str, Any]] = {}
    contract_metadata: dict[str, dict[str, Any]] = {}
    for path in sorted((selected_root / "runtime").glob("worker_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for code, row in (payload.get("books") or {}).items():
            if isinstance(row, dict):
                books[str(code).strip().upper()] = row
        for code, row in (payload.get("contract_metadata") or {}).items():
            if isinstance(row, dict):
                contract_metadata[str(code).strip().upper()] = row
    current_codes = sorted(code for code in books if code.startswith(logical_root))
    current_code = current_codes[0] if len(current_codes) == 1 else ""
    required = {
        current_code,
        *(str(code).strip().upper() for code in additional_contract_codes),
    } - {""}
    quotes: dict[str, dict[str, float | int | str | None]] = {}
    for code in required:
        row = books.get(code)
        if not row:
            break
        receive_ns = int(row.get("book_receive_ts_ns") or 0)
        snapshot_ns = int(row.get("snapshot_ts_ns") or 0)
        age_seconds = (decision_ns - receive_ns) / 1_000_000_000
        bid = float(row.get("bid_price_1") or 0.0)
        ask = float(row.get("ask_price_1") or 0.0)
        if (
            receive_ns <= 0
            or receive_ns > decision_ns
            or snapshot_ns > decision_ns
            or age_seconds < 0.0
            or age_seconds > max(0.0, float(max_age_seconds))
            or bool(row.get("stale"))
            or bool(row.get("suspend"))
            or bool(row.get("simtrade"))
            or bid <= 0.0
            or ask <= 0.0
            or bid > ask
        ):
            break
        quote_at = (
            datetime.fromtimestamp(receive_ns / 1e9, tz=timezone.utc)
            .astimezone(ZoneInfo("Asia/Taipei"))
            .isoformat(timespec="milliseconds")
        )
        quotes[code] = {
            "contract_code": code,
            "last": None,
            "bid": bid,
            "ask": ask,
            "bid_volume": int(row.get("bid_volume_1") or 0),
            "ask_volume": int(row.get("ask_volume_1") or 0),
            "quote_at": quote_at,
            "source": "shioaji:fop_stream_local_book",
            "logical_code": (contract_metadata.get(code) or {}).get("logical_code"),
            "delivery_month": (contract_metadata.get(code) or {}).get("delivery_month"),
            "delivery_date": (contract_metadata.get(code) or {}).get("delivery_date"),
            "last_trading_date": (contract_metadata.get(code) or {}).get(
                "last_trading_date"
            ),
        }
    if current_code and required and set(quotes) == required:
        record_avoided_query(
            consumer="tw_day_trade_futures_benchmark",
            method="snapshots",
            asset_class="futures",
            reason="causal_fop_stream_book",
            details={
                "logical_code": normalized_logical,
                "contracts": sorted(required),
                "contract_count": len(required),
            },
        )
        return {
            "logical_code": normalized_logical,
            "current_contract_code": current_code,
            "quotes": quotes,
            "received_at": quotes[current_code]["quote_at"],
            "source": "shioaji:fop_stream_local_book+api_snapshot_fallback",
        }
    return fetch_shioaji_futures_snapshot(
        normalized_logical,
        additional_contract_codes=additional_contract_codes,
    )


def load_symbol_yahoo_map(parquet_root: str | Path) -> dict[str, str]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.exists():
        return {}
    try:
        import csv

        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = csv.DictReader(handle)
            output: dict[str, str] = {}
            for row in rows:
                code = str(row.get("code", "")).strip()
                if not code:
                    continue
                yahoo_symbol = str(row.get("yahoo_symbol", "")).strip()
                if not yahoo_symbol:
                    # The canonical official symbol builder records venue, not
                    # a provider-specific ticker.  Derive the unambiguous Yahoo
                    # suffix so MIS can issue exactly one exchange channel per
                    # security instead of probing both TWSE and TPEx.
                    venue = str(row.get("market", "")).strip().casefold()
                    if venue == "twse":
                        yahoo_symbol = f"{code}.TW"
                    elif venue == "tpex":
                        yahoo_symbol = f"{code}.TWO"
                if yahoo_symbol:
                    output[code] = yahoo_symbol
            return output
    except Exception:
        return {}


def load_symbol_name_map(parquet_root: str | Path) -> dict[str, str]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.exists():
        return {}
    try:
        import csv

        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = csv.DictReader(handle)
            return {
                str(row.get("code", "")).strip(): str(row.get("name", "")).strip()
                for row in rows
                if str(row.get("code", "")).strip() and str(row.get("name", "")).strip()
            }
    except Exception:
        return {}


def load_prices_csv(
    path: str | Path, symbols: list[str], fallback_prices: np.ndarray
) -> PriceSnapshot:
    import polars as pl

    frame = pl.read_csv(path)
    columns = {name.lower(): name for name in frame.columns}
    symbol_col = columns.get("symbol") or columns.get("code") or columns.get("ticker")
    price_col = (
        columns.get("price")
        or columns.get("close")
        or columns.get("last")
        or columns.get("current_price")
    )
    open_col = columns.get("open_price") or columns.get("open")
    high_col = columns.get("high_price") or columns.get("high")
    low_col = columns.get("low_price") or columns.get("low")
    volume_col = columns.get("volume") or columns.get("trading_volume")
    upper_col = columns.get("upper_limit_price") or columns.get("upper_limit")
    lower_col = columns.get("lower_limit_price") or columns.get("lower_limit")
    bid_col = columns.get("bid_price") or columns.get("bid")
    ask_col = columns.get("ask_price") or columns.get("ask")
    bid_volume_col = columns.get("bid_volume")
    ask_volume_col = columns.get("ask_volume")
    reference_col = columns.get("reference_price") or columns.get("reference")
    # An explicit CSV observation clock is caller-declared research/input
    # provenance. It is never an independently verified exchange print.
    timestamp_col = columns.get("effective_timestamp_ms") or columns.get("timestamp_ms")
    if symbol_col is None or price_col is None:
        raise ValueError(
            "prices CSV must contain symbol/code/ticker and price/close/last/current_price columns"
        )

    value_columns = [
        column
        for column in (
            symbol_col,
            price_col,
            open_col,
            high_col,
            low_col,
            volume_col,
            upper_col,
            lower_col,
            bid_col,
            ask_col,
            bid_volume_col,
            ask_volume_col,
            reference_col,
            timestamp_col,
        )
        if column is not None
    ]
    lookup = {
        str(row[symbol_col]).strip(): row
        for row in frame.select(value_columns).iter_rows(named=True)
        if str(row[symbol_col]).strip()
    }
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    available = np.zeros((len(symbols),), dtype=bool)
    open_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    high_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    low_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    upper_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    lower_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    bid_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    ask_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    bid_volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    ask_volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    reference_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    timestamps_ms = np.zeros((len(symbols),), dtype=np.int64)
    count = 0
    for idx, symbol in enumerate(symbols):
        row = lookup.get(str(symbol))
        if row is None:
            continue
        value = _float_or_none(row.get(price_col))
        if value is None:
            continue
        prices[idx] = value
        available[idx] = True
        count += 1
        if timestamp_col is not None:
            stamp = _float_or_none(row.get(timestamp_col))
            if stamp is not None and stamp.is_integer() and stamp < np.iinfo(np.int64).max:
                timestamps_ms[idx] = int(stamp)
        for column, target in (
            (open_col, open_prices),
            (high_col, high_prices),
            (low_col, low_prices),
            (volume_col, volumes),
            (upper_col, upper_limit_prices),
            (lower_col, lower_limit_prices),
            (bid_col, bid_prices),
            (ask_col, ask_prices),
            (bid_volume_col, bid_volumes),
            (ask_volume_col, ask_volumes),
            (reference_col, reference_prices),
        ):
            if column is None:
                continue
            observed = _float_or_none(row.get(column))
            if observed is not None:
                target[idx] = observed
    return PriceSnapshot(
        prices=prices,
        source=f"csv:{Path(path)}",
        available_count=count,
        available_mask=available,
        open_prices=open_prices,
        high_prices=high_prices,
        low_prices=low_prices,
        volumes=volumes,
        upper_limit_prices=upper_limit_prices,
        lower_limit_prices=lower_limit_prices,
        bid_prices=bid_prices,
        ask_prices=ask_prices,
        bid_volumes=bid_volumes,
        ask_volumes=ask_volumes,
        reference_prices=reference_prices,
        timestamps_ms=timestamps_ms if timestamp_col is not None else None,
    )


def _float_or_none(value: object) -> float | None:
    try:
        if type(value) in (float, int, np.float64):
            # Native Snapshot numbers need neither formatting nor reparsing.
            # float64's shortest string round-trips to the same Python float.
            # Keep text conversion for bool, Decimal, OTHER numpy precisions
            # and custom wrappers (float32's short string has other semantics).
            parsed = float(value)
        else:
            text = str(value).strip()
            if not text or text in {"-", "--", "null", "None"}:
                return None
            parsed = float(text.replace(",", ""))
    except Exception:
        return None
    if not (math.isfinite(parsed) and parsed > 0.0):
        return None
    return parsed


def _quantity_or_none(value: object) -> float | None:
    """Preserve a published zero quantity; unknown and zero are not aliases."""
    if isinstance(value, (bool, np.bool_)):
        return None
    try:
        parsed = float(value) if type(value) in (float, int, np.float64) else float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0.0 else None


def _snapshot_field_or_none(field: str, value: object) -> float | None:
    return _quantity_or_none(value) if field in {"volumes", "bid_volumes", "ask_volumes"} else _float_or_none(value)


def _tw_mis_limit_price(value: object, *, lower: bool) -> float | None:
    """Parse the MIS legal-order band, including its no-limit sentinel.

    TWSE MIS publishes an omitted or zero lower bound together with its large
    system upper bound for securities that have no daily price-movement limit
    (notably eligible foreign-component ETFs). Zero is metadata, not an
    executable price. Normalize only that lower-band sentinel to the exchange's
    minimum positive order price; ordinary quote fields keep rejecting zero.
    """

    try:
        text = str(value).strip()
        if not text or text in {"-", "--", "null", "None"}:
            return None
        parsed = float(text.replace(",", ""))
    except Exception:
        return None
    if not np.isfinite(parsed) or parsed < 0.0:
        return None
    if parsed == 0.0:
        return 0.01 if lower else None
    return parsed


def _first_book_price(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    for part in text.split("_"):
        parsed = _float_or_none(part)
        if parsed is not None:
            return parsed
    return None


def _tw_mis_candidates(symbol: str, yahoo_symbol: str | None) -> list[str]:
    code = str(symbol).strip()
    if not code:
        return []
    raw_yahoo = str(yahoo_symbol or "").strip()
    markets: list[str] = []
    for item in raw_yahoo.split(","):
        ticker = item.strip().upper()
        if ticker.endswith(".TW") and "tse" not in markets:
            markets.append("tse")
        elif ticker.endswith(".TWO") and "otc" not in markets:
            markets.append("otc")
    if not markets:
        markets = ["tse", "otc"]
    return [f"{market}_{code}.tw" for market in markets]


def _tw_mis_price(row: dict) -> float | None:
    for key in ("z", "pz"):
        value = _float_or_none(row.get(key))
        if value is not None:
            return value
    bid = _first_book_price(row.get("b"))
    ask = _first_book_price(row.get("a"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid or ask or _float_or_none(row.get("y"))


def _tw_mis_opening_receipt_path(session_date: str) -> Path:
    configured = str(
        os.getenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", "") or ""
    ).strip()
    root = Path(configured) if configured else Path("artifacts/live/tw_opening_snapshots")
    return root / f"{session_date}.json"


@lru_cache(maxsize=8)
def _opening_receipt_scope_fingerprint(path: str, mtime_ns: int, size: int) -> str | None:
    """Decode receipt metadata once per atomic file generation, not per quote."""
    del mtime_ns, size
    return json.loads(Path(path).read_text()).get("official_trading_universe", {}).get("fingerprint")


@lru_cache(maxsize=32)
def _taipei_session_bounds(session_date: str) -> tuple[int, int]:
    """Cache calendar decoding, not market observations or freshness results."""
    try:
        start = datetime.fromisoformat(session_date + "T00:00:00+08:00")
        if start.date().isoformat() != session_date:
            return (0, 0)
        midnight = int(start.timestamp() * 1000)
        return midnight, midnight + 24 * 60 * 60 * 1000
    except (TypeError, OSError, OverflowError, ValueError):
        return (0, 0)


def _same_taipei_session_timestamp(timestamp_ms: int, session_date: str) -> bool:
    midnight, end = _taipei_session_bounds(session_date)
    return timestamp_ms > 0 and midnight <= timestamp_ms < end


def _same_taipei_open_timestamp(timestamp_ms: int, session_date: str) -> bool:
    midnight, end = _taipei_session_bounds(session_date)
    return timestamp_ms > 0 and midnight + 9 * 60 * 60 * 1000 <= timestamp_ms < end


def _tw_opening_timestamp_mask(stamps: np.ndarray, session_date: str) -> np.ndarray:
    """Same contract as the scalar check, one date decode per full universe."""
    midnight, end = _taipei_session_bounds(session_date)
    return (stamps > 0) & (stamps >= midnight + 9 * 60 * 60 * 1000) & (stamps < end)


def tw_session_open_observed_mask(
    snapshot: PriceSnapshot, *, session_date: str | None = None
) -> np.ndarray:
    """Opening input coverage, not response/book/previous-close coverage.

    A broker receipt time proves delivery, not the session of Snapshot.open.
    Prefer its exchange timestamp; MIS publishes its session time in tlong.
    This mask is for model inputs only, never an executable-price assertion.
    """
    size = len(snapshot.prices)
    opens = np.asarray(snapshot.open_prices, dtype=np.float64) if snapshot.open_prices is not None else np.full(size, np.nan)
    available = np.asarray(snapshot.available_mask, dtype=bool) if snapshot.available_mask is not None else np.zeros(size, dtype=bool)
    timestamps = snapshot.exchange_timestamps_ms
    times = np.asarray(timestamps, dtype=np.int64) if timestamps is not None else np.zeros(size, dtype=np.int64)
    if timestamps is None and not str(snapshot.source).startswith("shioaji:") and snapshot.timestamps_ms is not None:
        times = np.asarray(snapshot.timestamps_ms, dtype=np.int64)
    if any(values.shape != (size,) for values in (opens, available, times)):
        raise ValueError("opening price/availability/timestamp shape mismatch")
    date_text = session_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    observed = available & np.isfinite(opens) & (opens > 0.0) & _tw_opening_timestamp_mask(times, date_text)
    if snapshot.simtrade_flags is not None:
        flags = np.asarray(snapshot.simtrade_flags)
        if flags.shape != (size,):
            raise ValueError("opening simtrade flags must have shape [symbols]")
        observed &= flags != 1
    return observed


def _opening_row_has_open(row: Mapping[str, Any], session_date: str) -> bool:
    price = _float_or_none(row.get("open_prices"))
    # v2 receipts were written after the source router's exchange-time check.
    stamp = int(row.get("open_timestamp_ms") or row.get("timestamp_ms") or 0)
    return price is not None and price > 0.0 and _same_taipei_open_timestamp(stamp, session_date)


def _tw_mis_no_open_print_mask(snapshot: PriceSnapshot, session_date: str) -> np.ndarray:
    """Only this fresh raw MIS row can prove zero volume and no first print.

    Never combine a new missing field with zero volume carried from a previous
    response. A broker snapshot or a shared aggregate is not raw MIS evidence.
    """
    size = len(snapshot.prices)
    source = str(snapshot.source)
    empty = np.zeros(size, dtype=bool)
    if not source.startswith("twse_tpex:mis") or "shared_opening_snapshot" in source:
        return empty
    if snapshot.volumes is None or snapshot.open_prices is None or snapshot.timestamps_ms is None or snapshot.available_mask is None:
        return empty
    volumes = np.asarray(snapshot.volumes, dtype=np.float64)
    opens = np.asarray(snapshot.open_prices, dtype=np.float64)
    received = np.asarray(snapshot.timestamps_ms, dtype=np.int64)
    market = received if snapshot.exchange_timestamps_ms is None else np.asarray(snapshot.exchange_timestamps_ms, dtype=np.int64)
    available = np.asarray(snapshot.available_mask, dtype=bool)
    if any(values.shape != (size,) for values in (volumes, opens, received, market, available)):
        raise ValueError("MIS no-first-print fields must have shape [symbols]")
    observed = (available & (volumes == 0.0) & ~(np.isfinite(opens) & (opens > 0.0))
                & _tw_opening_timestamp_mask(market, session_date)
                & _tw_opening_timestamp_mask(received, session_date))
    if snapshot.simtrade_flags is not None:
        flags = np.asarray(snapshot.simtrade_flags)
        if flags.shape != (size,):
            raise ValueError("MIS no-first-print trial flags must have shape [symbols]")
        observed &= flags != 1
    return observed


def tw_session_no_open_print_mask(
    snapshot: PriceSnapshot, *, session_date: str | None = None,
) -> np.ndarray:
    """Explicit source-published absence, kept distinct from unknown data.

    The open-gap ABI already represents an absent opening print with its zero
    sentinel. This proof permits that input state; it never permits sizing or
    filling an order with a zero/carried/interpolated opening price.
    """
    size = len(snapshot.prices)
    date_text = session_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    stamps = snapshot.no_open_print_timestamps_ms
    if stamps is None:
        return _tw_mis_no_open_print_mask(snapshot, date_text)
    stamps = np.asarray(stamps, dtype=np.int64)
    if stamps.shape != (size,):
        raise ValueError("no-opening-print proof must have shape [symbols]")
    observed = _tw_opening_timestamp_mask(stamps, date_text)
    observed &= ~tw_session_open_observed_mask(snapshot, session_date=date_text)
    if snapshot.available_mask is None:
        observed[:] = False
    else:
        observed &= np.asarray(snapshot.available_mask, dtype=bool)
    return observed


def tw_opening_observation_diagnostics(
    symbols: list[str], snapshot: PriceSnapshot, *, session_date: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Explain the canonical input masks without changing their acceptance ABI.

    Counts and rows describe the decoded observation at this receipt, never a
    claim that an unobserved symbol did not trade. Unknown trial flags stay
    unknown; their admission as model inputs is not executable-price proof.
    """
    size = len(symbols)
    if size != len(snapshot.prices):
        raise ValueError("opening diagnostics symbols/snapshot shape mismatch")
    available = np.asarray(snapshot.available_mask, dtype=bool) if snapshot.available_mask is not None else np.zeros(size, dtype=bool)
    opens = np.asarray(snapshot.open_prices, dtype=np.float64) if snapshot.open_prices is not None else np.full(size, np.nan)
    stamps = snapshot.exchange_timestamps_ms
    if stamps is None and not snapshot.source.startswith("shioaji:"):
        stamps = snapshot.timestamps_ms
    market = np.asarray(stamps, dtype=np.int64) if stamps is not None else np.zeros(size, dtype=np.int64)
    receipt = np.asarray(snapshot.timestamps_ms, dtype=np.int64) if snapshot.timestamps_ms is not None else np.zeros(size, dtype=np.int64)
    flags = np.asarray(snapshot.simtrade_flags, dtype=np.int8) if snapshot.simtrade_flags is not None else np.full(size, -1, dtype=np.int8)
    if any(value.shape != (size,) for value in (available, opens, market, receipt, flags)):
        raise ValueError("opening diagnostic fields must have shape [symbols]")
    valid = tw_session_open_observed_mask(snapshot, session_date=session_date)
    no_print = tw_session_no_open_print_mask(snapshot, session_date=session_date)
    states = np.full(size, "missing_open_price", dtype="<U24")
    states[available & (market <= 0)] = "missing_market_time"
    states[available & (market > 0) & ~_tw_opening_timestamp_mask(market, session_date)] = "stale_or_preopen_time"
    states[available & (flags == 1)] = "trial_matching"
    states[~available] = "no_source_observation"
    states[no_print] = "observed_no_first_print"
    states[valid] = "valid_open"
    labels, counts = np.unique(states, return_counts=True)
    summary = {
        "contract": "tw_opening_observation_diagnostics_v1",
        "requested_count": size, "available_count": int(available.sum()),
        "valid_open_count": int(valid.sum()), "observed_no_print_count": int(no_print.sum()),
        "unresolved_count": int((~(valid | no_print)).sum()),
        "unknown_trial_flag_count": int((available & (flags == -1)).sum()),
        "states": {str(label): int(count) for label, count in zip(labels, counts)},
        "boundary": "decoded_source_observation_not_broker_fill",
    }
    rows = [{
        "symbol": symbol, "state": str(states[i]), "source": snapshot.source,
        "open_price": float(opens[i]) if np.isfinite(opens[i]) else None,
        "available": bool(available[i]), "market_timestamp_ms": int(market[i]),
        "received_timestamp_ms": int(receipt[i]),
        "simtrade": None if flags[i] == -1 else bool(flags[i]),
    } for i, symbol in enumerate(symbols)]
    return summary, rows


def _load_tw_mis_opening_receipt(
    *, parquet_root: str, session_date: str
) -> dict[str, tuple[float, dict[str, Any]]]:
    path = _tw_mis_opening_receipt_path(session_date)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    if (
        not isinstance(payload, dict)
        or payload.get("session_date") != session_date
        or payload.get("parquet_root") != parquet_root
    ):
        return {}
    rows = payload.get("rows")
    if not isinstance(rows, dict):
        return {}
    stored_at = time.monotonic()
    accepted: dict[str, tuple[float, dict[str, Any]]] = {}
    for symbol, raw in rows.items():
        if not isinstance(raw, dict):
            continue
        try:
            timestamp_ms = int(raw.get("timestamp_ms") or 0)
            open_timestamp_ms = int(raw.get("open_timestamp_ms") or timestamp_ms)
            exchange_timestamp_ms = int(raw.get("exchange_timestamp_ms") or 0)
            no_open_print_ms = int(raw.get("no_open_print_observed_ms") or 0)
        except (TypeError, ValueError):
            continue
        if not bool(raw.get("available")) or not _same_taipei_session_timestamp(
            timestamp_ms, session_date
        ):
            continue
        row: dict[str, Any] = {
            "available": bool(raw.get("available")),
            "price": _float_or_none(raw.get("price")),
            "timestamp_ms": timestamp_ms,
            "source": str(raw.get("source") or "twse_tpex:mis"),
            "open_timestamp_ms": open_timestamp_ms,
            "open_source": str(raw.get("open_source") or raw.get("source") or "twse_tpex:mis"),
            "exchange_timestamp_ms": exchange_timestamp_ms,
            "no_open_print_observed_ms": no_open_print_ms,
            "opening_conflicts": list(raw.get("opening_conflicts") or ()),
        }
        for field in _PRICE_SNAPSHOT_ARRAY_FIELDS:
            row[field] = _snapshot_field_or_none(field, raw.get(field))
        if not _opening_row_has_open(row, session_date):
            row["open_prices"] = None
        accepted[str(symbol)] = (stored_at, row)
    return accepted


def tw_mis_opening_receipt_status(
    *, parquet_root: str | Path, session_date: str
) -> dict[str, str | int | bool | None]:
    """Report whether a receipt-backed same-session opening cache is usable."""

    resolved_root = str(Path(parquet_root).resolve())
    path = _tw_mis_opening_receipt_path(session_date)
    rows = _load_tw_mis_opening_receipt(
        parquet_root=resolved_root,
        session_date=session_date,
    )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        requested = set(payload.get("requested_symbols") or rows) if payload.get("parquet_root") == resolved_root else set(rows)
    except (OSError, UnicodeError, ValueError, AttributeError, TypeError):
        requested = set(rows)
    requested_count = len(requested)
    open_count = sum(symbol in rows and _opening_row_has_open(rows[symbol][1], session_date) for symbol in requested)
    return {
        "ready": open_count > 0,
        "complete": requested_count > 0 and open_count == requested_count,
        "session_date": session_date,
        "parquet_root": resolved_root,
        "path": str(path.resolve()),
        "row_count": len(rows),
        "valid_open_count": open_count,
        "requested_count": requested_count,
        "missing_open_count": requested_count - open_count,
    }


def _persist_tw_mis_opening_receipt(
    *,
    parquet_root: str,
    session_date: str,
    rows: dict[str, tuple[float, dict[str, Any]]],
    source_artifact: str | None = None,
    requested_symbols: list[str] | None = None,
) -> Path | None:
    accepted: dict[str, dict[str, Any]] = {}
    for symbol, (_stored_at, raw) in rows.items():
        try:
            timestamp_ms = int(raw.get("timestamp_ms") or 0)
        except (TypeError, ValueError):
            continue
        if not bool(raw.get("available")) or not _same_taipei_session_timestamp(
            timestamp_ms, session_date
        ):
            continue
        accepted[str(symbol)] = {
            "available": bool(raw.get("available")),
            "price": _float_or_none(raw.get("price")),
            "timestamp_ms": timestamp_ms,
            "source": str(raw.get("source") or "twse_tpex:mis"),
            "open_timestamp_ms": int(raw.get("open_timestamp_ms") or timestamp_ms),
            "open_source": str(raw.get("open_source") or raw.get("source") or "twse_tpex:mis"),
            "exchange_timestamp_ms": int(raw.get("exchange_timestamp_ms") or 0),
            "no_open_print_observed_ms": int(raw.get("no_open_print_observed_ms") or 0),
            "opening_conflicts": list(raw.get("opening_conflicts") or ()),
            **{
                field: _snapshot_field_or_none(field, raw.get(field))
                for field in _PRICE_SNAPSHOT_ARRAY_FIELDS
            },
        }
    path = _tw_mis_opening_receipt_path(session_date)
    requested = set(requested_symbols or ()) | set(accepted)
    previous_bytes = None
    previous = {}
    try:
        previous_bytes = path.read_bytes()
        previous = json.loads(previous_bytes)
        if previous.get("session_date") == session_date and previous.get("parquet_root") == parquet_root:
            requested.update(previous.get("requested_symbols") or previous.get("rows") or ())
    except (OSError, UnicodeError, ValueError, AttributeError, TypeError):
        pass
    official_scope = None
    from stockagent.data.tw_current_trading_universe import official_scope_required, opening_scope
    if official_scope_required(parquet_root):
        _mask, official_scope = opening_scope([], session_date)
        requested = set(official_scope["symbols"])
        if previous_bytes and previous.get("official_trading_universe", {}).get("fingerprint") != official_scope["fingerprint"]:
            from downloader.artifact_io import atomic_write_bytes
            digest = hashlib.sha256(previous_bytes).hexdigest()
            atomic_write_bytes(path.parent / "scope_generations" / (session_date + "." + digest + ".json"), previous_bytes, durable=True)
    if not requested:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 4 if official_scope is not None else 3,
        "session_date": session_date,
        "parquet_root": parquet_root,
        "captured_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(
            timespec="seconds"
        ),
        "source": "twse_tpex:mis_opening_snapshot",
        "source_artifact": source_artifact,
        "row_count": len(accepted),
        "requested_symbols": sorted(requested),
        "requested_count": len(requested),
        "valid_open_count": sum(symbol in accepted and _opening_row_has_open(accepted[symbol], session_date) for symbol in requested),
        "missing_open_symbols": sorted(symbol for symbol in requested if symbol not in accepted or not _opening_row_has_open(accepted[symbol], session_date)),
        "rows": accepted,
    }
    if official_scope is not None:
        payload["official_trading_universe"] = official_scope
        payload["out_of_scope_observed_count"] = len(set(accepted) - requested)
    # Atomic rename protects readers, but not a sudden power loss. This is
    # the accepted model-input receipt: sync both file and directory before
    # quorum release, using the canonical writer rather than a second protocol.
    from downloader.artifact_io import atomic_write_bytes
    atomic_write_bytes(path, (json.dumps(payload, ensure_ascii=False,
        sort_keys=True, allow_nan=False) + "\n").encode("utf-8"), durable=True)
    return path


def seed_tw_opening_snapshot_cache(
    symbols: list[str],
    snapshot: PriceSnapshot,
    *,
    parquet_root: str | Path,
    session_date: str | None = None,
    persist: bool = True,
) -> dict[str, int | str]:
    """Share one verified opening observation across all scheduled models.

    The first 09:00 request can be satisfied by MIS, Shioaji, or a truthful
    merge of both. Without this hand-off, each model independently retries the
    same full-universe request while the opening auction is still propagating.
    Cache source-response rows even when an illiquid symbol has not printed an
    open yet; a short retry grace keeps one scheduled batch internally
    consistent while later scheduler retries can still observe new opens.
    """

    requested = [str(symbol).strip() for symbol in symbols]
    size = len(requested)
    if len(snapshot.prices) != size:
        raise ValueError("opening snapshot symbols/prices must have equal length")
    date_text = (
        session_date
        or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    )
    cache_key = (str(Path(parquet_root).resolve()), date_text)
    available = (
        np.asarray(snapshot.available_mask, dtype=bool)
        if snapshot.available_mask is not None
        else np.zeros((size,), dtype=bool)
    )
    timestamps_ms = (
        np.asarray(snapshot.timestamps_ms, dtype=np.int64)
        if snapshot.timestamps_ms is not None
        else np.zeros((size,), dtype=np.int64)
    )
    exchange_timestamps_ms = (
        np.asarray(snapshot.exchange_timestamps_ms, dtype=np.int64)
        if snapshot.exchange_timestamps_ms is not None
        else np.zeros((size,), dtype=np.int64)
    )
    if available.shape != (size,) or timestamps_ms.shape != (size,):
        raise ValueError("opening snapshot availability/timestamp shape mismatch")
    snapshot_arrays = {field: np.asarray(getattr(snapshot, field)) for field in _PRICE_SNAPSHOT_ARRAY_FIELDS if getattr(snapshot, field) is not None}
    if exchange_timestamps_ms.shape != (size,) or any(values.shape != (size,) for values in snapshot_arrays.values()):
        raise ValueError("opening snapshot field arrays must have shape [symbols]")

    source_text = str(snapshot.source or "official_opening_observation")
    valid_opens = tw_session_open_observed_mask(snapshot, session_date=date_text)
    source_no_print = _tw_mis_no_open_print_mask(snapshot, date_text)
    stored_at = time.monotonic()
    accepted = 0
    with _TW_MIS_OPENING_LOCK:
        global _TW_MIS_OPENING_CACHE_KEY
        global _TW_MIS_OPENING_DIRTY
        # A late callback may not roll the process cache back to yesterday.
        if date_text != datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat():
            return {"session_date": date_text, "accepted_count": 0, "cached_count": 0, "source": source_text}
        if _TW_MIS_OPENING_CACHE_KEY != cache_key:
            _TW_MIS_OPENING_CACHE.clear()
            _TW_MIS_OPENING_REQUESTED.clear()
            _TW_MIS_OPENING_CACHE_KEY = cache_key
            _TW_MIS_OPENING_DIRTY = False
            _TW_MIS_OPENING_CACHE.update(
                _load_tw_mis_opening_receipt(
                    parquet_root=cache_key[0],
                    session_date=date_text,
                )
            )
        for idx, symbol in enumerate(requested):
            timestamp_ms = int(timestamps_ms[idx])
            if (
                not symbol
                or not bool(available[idx])
                or not _same_taipei_session_timestamp(timestamp_ms, date_text)
            ):
                continue
            existing = _TW_MIS_OPENING_CACHE.get(symbol)
            row: dict[str, Any] = dict(existing[1]) if existing is not None else {}
            frozen_open = _opening_row_has_open(row, date_text)
            row.update(
                {
                    "available": True,
                    "price": _float_or_none(snapshot.prices[idx]),
                    "timestamp_ms": timestamp_ms,
                    "source": source_text,
                    "exchange_timestamp_ms": int(exchange_timestamps_ms[idx]),
                }
            )
            for field in _PRICE_SNAPSHOT_ARRAY_FIELDS:
                values = snapshot_arrays.get(field)
                value = (
                    _snapshot_field_or_none(field, values[idx])
                    if values is not None
                    else None
                )
                if field == "open_prices":
                    # An accepted opening fact survives later failures, stale
                    # snapshots and TTL expiry. Missing is not a replacement.
                    if frozen_open:
                        if valid_opens[idx] and value is not None and value != row.get(field):
                            evidence = {"price": value, "source": source_text, "exchange_timestamp_ms": int(exchange_timestamps_ms[idx]) or timestamp_ms}
                            conflicts = list(row.get("opening_conflicts") or ())
                            # Preserve both raw observations; never overwrite a
                            # frozen open or inflate the same repeated conflict.
                            if not any(item.get("price") == value and item.get("source") == source_text for item in conflicts):
                                conflicts.append(evidence)
                            row["opening_conflicts"] = conflicts
                        continue
                    if not valid_opens[idx]:
                        value = None
                    else:
                        row["open_timestamp_ms"] = int(exchange_timestamps_ms[idx]) or timestamp_ms
                        row["open_source"] = row["source"]
                if value is not None or field not in row:
                    row[field] = value
            if frozen_open or valid_opens[idx]:
                row["no_open_print_observed_ms"] = 0
            elif source_text.startswith("twse_tpex:mis") and "shared_opening_snapshot" not in source_text:
                # Source-published zero volume and absent open are a waiting
                # first-print observation, not an HTTP/acquisition failure.
                row["no_open_print_observed_ms"] = timestamp_ms if source_no_print[idx] else 0
            if existing is None or row != existing[1]:
                _TW_MIS_OPENING_CACHE[symbol] = (stored_at, row)
                _TW_MIS_OPENING_DIRTY = True
            accepted += 1
        if not set(requested).issubset(_TW_MIS_OPENING_REQUESTED):
            _TW_MIS_OPENING_REQUESTED.update(requested)
            _TW_MIS_OPENING_DIRTY = True
        if persist:
            persist_tw_opening_snapshot_cache(parquet_root=cache_key[0], session_date=date_text, requested_symbols=requested)
        cached_count = len(_TW_MIS_OPENING_CACHE)
    return {
        "session_date": date_text,
        "accepted_count": accepted,
        "cached_count": cached_count,
        "source": source_text,
    }


def persist_tw_opening_snapshot_cache(
    *, parquet_root: str | Path, session_date: str,
    requested_symbols: list[str] | None = None,
) -> Path | None:
    """One durable flush at quorum/final completion, not per arriving chunk."""
    global _TW_MIS_OPENING_DIRTY
    with _TW_MIS_OPENING_LOCK:
        if _TW_MIS_OPENING_CACHE_KEY != (str(Path(parquet_root).resolve()), session_date):
            return None
        if not _TW_MIS_OPENING_DIRTY:
            path = _tw_mis_opening_receipt_path(session_date)
            from stockagent.data.tw_current_trading_universe import official_scope_required, load_current_universe
            if not official_scope_required(parquet_root):
                return path
            authority = load_current_universe(session_date)
            try:
                info = path.stat()
                if _opening_receipt_scope_fingerprint(str(path.resolve()), info.st_mtime_ns, info.st_size) == authority["fingerprint"]:
                    return path
            except (OSError, ValueError):
                pass
        result = _persist_tw_mis_opening_receipt(parquet_root=str(Path(parquet_root).resolve()), session_date=session_date, rows=_TW_MIS_OPENING_CACHE, requested_symbols=sorted(_TW_MIS_OPENING_REQUESTED | set(requested_symbols or ())))
        _TW_MIS_OPENING_DIRTY = False
        return result


def repair_tw_opening_snapshot_receipt(
    *, parquet_root: str | Path, session_date: str,
) -> dict[str, Any]:
    """Continue acquisition after signal publication, without inference/orders.

    The existing opening receipt is the durable pending-work list. Only gaps
    use the same multi-source fan-in; no second broker login or full-market
    refetch. A non-printing/halted symbol remains waiting, never a fake open.
    """
    if session_date != datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat():
        raise ValueError("opening source repair requires the current Taipei session")
    root = str(Path(parquet_root).resolve())
    try:
        payload = json.loads(_tw_mis_opening_receipt_path(session_date).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {"status": "not_started", "requested_count": 0, "missing_open_count": 0}
    if payload.get("parquet_root") != root or payload.get("session_date") != session_date:
        return {"status": "not_started", "requested_count": 0, "missing_open_count": 0}
    rows = _load_tw_mis_opening_receipt(parquet_root=root, session_date=session_date)
    requested = list(payload.get("requested_symbols") or payload.get("rows") or ())
    from stockagent.data.tw_current_trading_universe import official_scope_required, opening_scope
    official_scope = None
    if official_scope_required(root):
        _mask, official_scope = opening_scope([], session_date)
        requested = official_scope["symbols"]
    missing = [symbol for symbol in requested if symbol not in rows or not _opening_row_has_open(rows[symbol][1], session_date)]
    resolved_before = {
        symbol for symbol in requested if symbol in rows and (
            _opening_row_has_open(rows[symbol][1], session_date)
            or _same_taipei_open_timestamp(
                int(rows[symbol][1].get("no_open_print_observed_ms") or 0), session_date,
            )
        )
    }
    if official_scope is not None:
        # A scope-only correction also needs publication when every open is
        # already present; retain the prior receipt as immutable evidence.
        with _TW_MIS_OPENING_LOCK:
            if _TW_MIS_OPENING_CACHE_KEY == (root, session_date):
                persist_tw_opening_snapshot_cache(parquet_root=root, session_date=session_date, requested_symbols=requested)
            else:
                _persist_tw_mis_opening_receipt(parquet_root=root, session_date=session_date,
                                              rows=rows, requested_symbols=requested)
    started = time.perf_counter()
    source_timing = {}
    if missing:
        from stockagent.live.opening_price_acquisition import OPENING_FAN_IN, configured_opening_providers
        fallback = np.asarray([rows.get(symbol, (0.0, {}))[1].get("price") or np.nan for symbol in missing], dtype=np.float64)
        refreshed = OPENING_FAN_IN.fetch(
            missing, fallback, parquet_root=root, providers=configured_opening_providers(root), minimum_coverage=1,
            cache_reader=lambda: fetch_tw_mis_opening_snapshot(missing, fallback, parquet_root=root, allow_network=False),
            resolve_no_prints=False, force_refresh=True,
        )
        source_timing = refreshed.transport_timing or {}
        rows = _load_tw_mis_opening_receipt(parquet_root=root, session_date=session_date)
    remaining = [symbol for symbol in requested if symbol not in rows or not _opening_row_has_open(rows[symbol][1], session_date)]
    no_print = [symbol for symbol in remaining if symbol in rows and _same_taipei_open_timestamp(
        int(rows[symbol][1].get("no_open_print_observed_ms") or 0), session_date,
    )]
    resolved_after = (set(requested) - set(remaining)) | set(no_print)
    return {
        "status": "awaiting_first_print" if remaining and len(no_print) == len(remaining) else "waiting_source" if remaining else "complete",
        "session_date": session_date, "requested_count": len(requested),
        "valid_open_count": len(requested) - len(remaining),
        "missing_open_count": len(remaining), "missing_open_symbols": remaining,
        "reported_no_open_print_count": len(no_print), "reported_no_open_print_symbols": no_print,
        "unresolved_source_count": len(remaining) - len(no_print),
        "newly_requested_count": len(missing), "recovered_count": len(missing) - len(remaining),
        "resolved_input_count": len(resolved_after),
        "recovered_input_count": len(resolved_after - resolved_before),
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "opening_acquisition": source_timing,
        **({"official_trading_universe": official_scope} if official_scope is not None else {}),
        "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="milliseconds"),
    }


def warm_tw_mis_quote_client(
    *, force: bool = False
) -> dict[str, float | int | bool | None]:
    """Warm and prove the actual TWSE MIS quote API before the critical path.

    ``stock/index.jsp`` is only a browser front end.  It has returned 404/502
    while ``api/getStockInfo.jsp`` remained healthy, so its status must not be
    the readiness authority for market data.  Visiting it remains a best-effort
    cookie bootstrap; the small API request below is the fail-closed proof.
    """

    global _TW_MIS_BOOTSTRAP_AT
    global _TW_MIS_BOOTSTRAP_COOKIES
    started = time.perf_counter()
    now_monotonic = time.monotonic()
    with _TW_MIS_BOOTSTRAP_LOCK:
        cache_hit = bool(
            not force
            and _TW_MIS_BOOTSTRAP_AT > 0.0
            and now_monotonic - _TW_MIS_BOOTSTRAP_AT <= 7200.0
        )
        if not cache_hit:
            # A failed forced probe invalidates the earlier transport proof.
            # Empty HTTP-200 responses cannot renew two hours of readiness.
            _TW_MIS_BOOTSTRAP_AT = 0.0
            _TW_MIS_BOOTSTRAP_COOKIES = {}
            session = requests.Session()
            session.headers.update(_YAHOO_HEADERS)
            frontend_http_status: int | None = None
            try:
                frontend = session.get(
                    "https://mis.twse.com.tw/stock/index.jsp",
                    timeout=8,
                )
                frontend_http_status = int(frontend.status_code)
                frontend.raise_for_status()
            except Exception:
                # The browser page is not the data plane.  Preserve its status
                # for diagnostics and continue to the authoritative API probe.
                pass
            try:
                response = session.get(
                    "https://mis.twse.com.tw/stock/api/getStockInfo.jsp",
                    params={
                        "ex_ch": "tse_2330.tw",
                        "json": "1",
                        "delay": "0",
                        "_": str(int(datetime.now().timestamp() * 1000)),
                    },
                    headers={
                        "Referer": "https://mis.twse.com.tw/stock/index.jsp",
                        **_YAHOO_HEADERS,
                    },
                    timeout=8,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(
                    payload.get("msgArray"), list
                ):
                    raise RuntimeError("TWSE MIS quote API returned an invalid payload")
                if not any(isinstance(row, dict) and str(row.get("c")) == "2330"
                           for row in payload["msgArray"]):
                    raise RuntimeError("TWSE MIS quote API returned no requested probe row")
                _TW_MIS_BOOTSTRAP_COOKIES = requests.utils.dict_from_cookiejar(
                    session.cookies
                )
                _TW_MIS_BOOTSTRAP_AT = time.monotonic()
            finally:
                close = getattr(session, "close", None)
                if close is not None:
                    close()
        else:
            frontend_http_status = None
        cookie_count = len(_TW_MIS_BOOTSTRAP_COOKIES)
    return {
        "ready": True,
        "cache_hit": cache_hit,
        "cookie_count": cookie_count,
        "frontend_http_status": frontend_http_status,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def fetch_tw_mis_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    empty_chunk_retry_attempts: int | None = None,
    empty_chunk_retry_delay_seconds: float | None = None,
    max_parallel_requests: int | None = None,
    request_timeout_seconds: float | None = None,
    minimum_response_coverage: float = 1.0,
    on_progress: Callable[[list[str], PriceSnapshot], None] | None = None,
) -> PriceSnapshot:
    """Share only overlapping in-flight rows across different model universes.

    A caller owns the symbols no earlier request is already fetching. Results
    are remapped to its own order and fallback prices. Completed work is removed
    immediately, so a later command always creates a fresh network observation.
    """
    requested = tuple(str(symbol) for symbol in symbols)
    fallback = np.asarray(fallback_prices, dtype=np.float64)
    if fallback.shape != (len(requested),):
        raise ValueError("symbols and fallback_prices must have equal length")
    kwargs = dict(parquet_root=parquet_root, chunk_size=chunk_size,
                  empty_chunk_retry_attempts=empty_chunk_retry_attempts,
                  empty_chunk_retry_delay_seconds=empty_chunk_retry_delay_seconds,
                  max_parallel_requests=max_parallel_requests,
                  request_timeout_seconds=request_timeout_seconds,
                  minimum_response_coverage=minimum_response_coverage)
    if on_progress is not None:
        kwargs["on_progress"] = on_progress
    if not requested or len(set(requested)) != len(requested):
        return _fetch_tw_mis_last_prices(list(requested), fallback, **kwargs)
    policy = (str(Path(parquet_root).resolve()), *tuple(kwargs[key] for key in kwargs if key not in {"parquet_root", "on_progress"}))
    started = time.perf_counter()
    remaining = set(requested)
    borrowed = []
    with _TW_MIS_INFLIGHT_LOCK:
        for batch_symbols, future in _TW_MIS_INFLIGHT.get(policy, []):
            common = remaining.intersection(batch_symbols)
            if common:
                borrowed.append((batch_symbols, future, common))
                remaining.difference_update(common)
        own_symbols = tuple(symbol for symbol in requested if symbol in remaining)
        own_future = Future() if own_symbols else None
        if own_future is not None:
            _TW_MIS_INFLIGHT.setdefault(policy, []).append((own_symbols, own_future))
    own_snapshot = None
    if own_future is not None:
        try:
            positions = {symbol: idx for idx, symbol in enumerate(requested)}
            own_fallback = fallback[[positions[symbol] for symbol in own_symbols]]
            own_snapshot = _fetch_tw_mis_last_prices(list(own_symbols), own_fallback, **kwargs)
            own_future.set_result(own_snapshot)
        except BaseException as exc:
            own_future.set_exception(exc)
            raise
        finally:
            with _TW_MIS_INFLIGHT_LOCK:
                pending = [(values, future) for values, future in _TW_MIS_INFLIGHT.get(policy, []) if future is not own_future]
                if pending:
                    _TW_MIS_INFLIGHT[policy] = pending
                else:
                    _TW_MIS_INFLIGHT.pop(policy, None)
    if not borrowed:
        return own_snapshot

    groups = [(values, future.result(), common) for values, future, common in borrowed]
    if own_snapshot is not None:
        groups.append((own_symbols, own_snapshot, set(own_symbols)))
    size = len(requested)
    prices = fallback.copy()
    available = np.zeros(size, dtype=bool)
    positions = {symbol: idx for idx, symbol in enumerate(requested)}
    fields = {}
    for name in (*_PRICE_SNAPSHOT_ARRAY_FIELDS, "timestamps_ms", "exchange_timestamps_ms"):
        if any(getattr(snapshot, name) is not None for _values, snapshot, _common in groups):
            integer = name.endswith("timestamps_ms")
            fields[name] = np.full(size, 0 if integer else np.nan, dtype=np.int64 if integer else np.float64)
    for values, snapshot, common in groups:
        source_indices = [idx for idx, symbol in enumerate(values) if symbol in common]
        target_indices = [positions[values[idx]] for idx in source_indices]
        valid = np.asarray(snapshot.available_mask, dtype=bool)[source_indices]
        available[target_indices] = valid
        prices[np.asarray(target_indices)[valid]] = np.asarray(snapshot.prices)[np.asarray(source_indices)[valid]]
        for name, target in fields.items():
            source = getattr(snapshot, name)
            if source is not None:
                target[target_indices] = np.asarray(source)[source_indices]
    timestamps = fields.get("timestamps_ms", np.zeros(size, dtype=np.int64))
    latest_ms = int(timestamps.max(initial=0))
    return PriceSnapshot(
        prices=prices, source="twse_tpex:mis+inflight_shared",
        timestamp=datetime.fromtimestamp(latest_ms / 1000.0, tz=timezone.utc).isoformat() if latest_ms else None,
        requested_count=size, available_count=int(available.sum()), available_mask=available,
        transport_timing={
            "mis_inflight_shared_symbols": size - len(own_symbols),
            "mis_newly_requested_symbols": len(own_symbols),
            "mis_total_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "mis_keep_alive_pool": True,
        },
        **fields,
    )


def _tw_mis_transport_error_types(exc: BaseException) -> list[str]:
    """Classify nested transport failures without leaking URLs/cookies/text."""
    pending, seen, names = [exc], set(), set()
    while pending and len(seen) < 16:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        names.add(type(error).__name__)
        pending.extend(value for value in getattr(error, "args", ())
                       if isinstance(value, BaseException))
        pending.extend(value for value in (
            error.__cause__, error.__context__, getattr(error, "reason", None),
        ) if isinstance(value, BaseException))
    return sorted(names)


def _fetch_tw_mis_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    empty_chunk_retry_attempts: int | None = None,
    empty_chunk_retry_delay_seconds: float | None = None,
    max_parallel_requests: int | None = None,
    request_timeout_seconds: float | None = None,
    minimum_response_coverage: float = 1.0,
    on_progress: Callable[[list[str], PriceSnapshot], None] | None = None,
) -> PriceSnapshot:
    """Fetch Taiwan intraday prices from TWSE MIS and align them to panel symbols."""
    request_timeout = max(
        0.25,
        float(request_timeout_seconds)
        if request_timeout_seconds is not None
        else float(os.getenv("STOCKAGENT_TW_MIS_REQUEST_TIMEOUT_SECONDS", "8") or "8"),
    )
    yahoo_map = load_symbol_yahoo_map(parquet_root)
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    filled = np.zeros((len(symbols),), dtype=bool)
    open_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    high_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    low_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    upper_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    lower_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    bid_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    ask_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    bid_volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    ask_volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    reference_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    last_timestamp_ms: int | None = None
    timestamps_ms = np.zeros((len(symbols),), dtype=np.int64)

    ex_channels: list[tuple[int, str]] = []
    for idx, symbol in enumerate(symbols):
        for ex_ch in _tw_mis_candidates(str(symbol), yahoo_map.get(str(symbol))):
            ex_channels.append((idx, ex_ch))
    # A valid single-symbol probe does not prove an 80-channel query fits the
    # opening caller's 1.5s deadline: measured 80-channel requests took 2.69s.
    # Keep larger batches for the ordinary 8s path, but publish smaller batches
    # promptly on the short-budget path. This does not reduce the symbol scope.
    channel_limit = min(20, max(1, int(request_timeout / .075))) if request_timeout <= 2.0 else 80
    chunk_len = max(1, min(int(chunk_size), channel_limit))
    chunks = [
        ex_channels[start : start + chunk_len]
        for start in range(0, len(ex_channels), chunk_len)
    ]
    chunk_failures: list[dict[str, Any]] = []
    chunk_failure_lock = threading.Lock()
    request_observations: list[dict[str, Any]] = []
    stop_dispatch = threading.Event()
    consecutive_endpoint_failures = 0
    submitted_indices: set[int] = set()
    max_parallel = (
        int(max_parallel_requests)
        if max_parallel_requests is not None
        else int(os.getenv("STOCKAGENT_TW_MIS_PARALLEL_REQUESTS", "4") or "4")
    )
    workers = max(1, min(len(chunks) or 1, max_parallel, _TW_MIS_HTTP_WORKERS))
    minimum_coverage = float(minimum_response_coverage)
    if not 0.0 < minimum_coverage <= 1.0:
        raise ValueError("minimum_response_coverage must be in (0, 1]")
    fetch_started = time.perf_counter()
    retry_attempts = max(
        0,
        int(empty_chunk_retry_attempts)
        if empty_chunk_retry_attempts is not None
        else int(os.getenv("STOCKAGENT_TW_MIS_RETRY_ATTEMPTS", "1") or "1"),
    )
    retry_delay_seconds = max(
        0.0,
        float(empty_chunk_retry_delay_seconds)
        if empty_chunk_retry_delay_seconds is not None
        else float(
            os.getenv("STOCKAGENT_TW_MIS_RETRY_DELAY_SECONDS", "0.35") or "0.35"
        ),
    )
    if on_progress is None:
        try:
            warm_tw_mis_quote_client()
        except Exception:
            pass
    # Opening fan-in has an independent pre-open transport owner. Do not
    # serially repeat an optional 8s browser/API bootstrap before its bounded
    # real quote sweep; each actual API response still validates its schema.

    def session() -> requests.Session:
        sess = getattr(_TW_MIS_HTTP_LOCAL, "session", None)
        if sess is None or getattr(_TW_MIS_HTTP_LOCAL, "factory", None) is not requests.Session:
            if sess is not None:
                close = getattr(sess, "close", None)
                if close is not None:
                    close()
            sess = requests.Session()
            sess.headers.update(_YAHOO_HEADERS)
            _TW_MIS_HTTP_LOCAL.session = sess
            _TW_MIS_HTTP_LOCAL.factory = requests.Session
            _TW_MIS_HTTP_LOCAL.bootstrap_at = None
        with _TW_MIS_BOOTSTRAP_LOCK:
            bootstrap_at = _TW_MIS_BOOTSTRAP_AT
            cookies = dict(_TW_MIS_BOOTSTRAP_COOKIES)
        if getattr(_TW_MIS_HTTP_LOCAL, "bootstrap_at", None) != bootstrap_at:
            if cookies:
                sess.cookies.update(cookies)
            _TW_MIS_HTTP_LOCAL.bootstrap_at = bootstrap_at
        # The bootstrap already attempted the browser page once. A failed
        # browser page must not add another eight-second wait in every worker.
        return sess

    def fetch_chunk(
        items: list[tuple[int, str]],
    ) -> list[
        tuple[
            int,
            float,
            int | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
        ]
    ]:
        nonlocal consecutive_endpoint_failures
        if not items:
            return []
        ex_ch = "|".join(ex for _, ex in items)
        code_to_indices: dict[str, list[int]] = {}
        for idx, _ex in items:
            code_to_indices.setdefault(str(symbols[idx]), []).append(idx)
        request_started = time.perf_counter()
        response = None
        try:
            response = session().get(
                "https://mis.twse.com.tw/stock/api/getStockInfo.jsp",
                params={
                    "ex_ch": ex_ch,
                    "json": "1",
                    "delay": "0",
                    "_": str(int(datetime.now().timestamp() * 1000)),
                },
                headers={
                    "Referer": "https://mis.twse.com.tw/stock/index.jsp",
                    **_YAHOO_HEADERS,
                },
                timeout=request_timeout,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("msgArray"), list):
                raise ValueError("MIS response schema is unavailable")
            if payload.get("rtcode") not in (None, "0000"):
                raise ValueError("MIS response reports a source error")
        except Exception as exc:
            # Preserve failed transport/schema evidence without persisting
            # exception text, request headers, cookies or private URLs.
            # Only this HTTP worker discards its broken connection pool. The
            # other providers and accepted opening observations stay intact.
            if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
                broken = getattr(_TW_MIS_HTTP_LOCAL, "session", None)
                _TW_MIS_HTTP_LOCAL.session = None
                close = getattr(broken, "close", None)
                if close is not None:
                    close()
            error_types = _tw_mis_transport_error_types(exc)
            failure = {"error_type": type(exc).__name__, "error_chain_types": error_types}
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is None:
                status = getattr(response, "status_code", None)
            if status is not None:
                failure["http_status"] = int(status)
            with chunk_failure_lock:
                chunk_failures.append(failure)
                endpoint_unavailable = bool(
                    {"RemoteDisconnected", "NameResolutionError", "SSLError", "ConnectTimeout"}
                    .intersection(error_types) or status in {502, 503}
                )
                consecutive_endpoint_failures = (
                    consecutive_endpoint_failures + 1 if endpoint_unavailable else 0
                )
                if status in {401, 402, 403, 429} or consecutive_endpoint_failures >= 4:
                    # Drain already-running requests and preserve their results.
                    # Do not dispatch the rest of a sweep into a refused source;
                    # the existing source owner retries its unresolved delta.
                    stop_dispatch.set()
                request_observations.append({
                    "channel_count": len(items),
                    "response_row_count": 0,
                    "elapsed_ms": round((time.perf_counter() - request_started) * 1000, 3),
                    **failure,
                })
            return []
        elapsed = getattr(response, "elapsed", None)
        headers_ms = (
            round(elapsed.total_seconds() * 1000, 3)
            if hasattr(elapsed, "total_seconds") else None
        )
        with chunk_failure_lock:
            consecutive_endpoint_failures = 0
            request_observations.append({
                "channel_count": len(items),
                "response_row_count": len(payload["msgArray"]),
                "elapsed_ms": round((time.perf_counter() - request_started) * 1000, 3),
                "headers_ms": headers_ms,
                "http_status": getattr(response, "status_code", None),
            })
        rows: list[
            tuple[
                int,
                float,
                int | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ] = []
        for item in payload.get("msgArray") or []:
            if not isinstance(item, dict):
                continue
            code = str(item.get("c") or "").strip()
            if not code:
                continue
            value = _tw_mis_price(item)
            if value is None:
                continue
            try:
                timestamp_ms = int(item.get("tlong") or 0) or None
            except Exception:
                timestamp_ms = None
            upper_limit = _tw_mis_limit_price(item.get("u"), lower=False)
            lower_limit = _tw_mis_limit_price(item.get("w"), lower=True)
            if (
                lower_limit is None
                and upper_limit is not None
                and upper_limit >= 9999.95
            ):
                # MIS omits ``w`` for the exchange's no-daily-limit band while
                # publishing the system ceiling in ``u``. TWSE's executable
                # range remains positive, so retain a 0.01 legal floor.
                lower_limit = 0.01
            for idx in code_to_indices.get(code, []):
                rows.append(
                    (
                        idx,
                        value,
                        timestamp_ms,
                        _float_or_none(item.get("o")),
                        _float_or_none(item.get("h")),
                        _float_or_none(item.get("l")),
                        _quantity_or_none(item.get("v")),
                        upper_limit,
                        lower_limit,
                        _first_book_price(item.get("b")),
                        _first_book_price(item.get("a")),
                        _first_book_price(item.get("g")),
                        _first_book_price(item.get("f")),
                        _float_or_none(item.get("y")),
                    )
                )
        return rows

    chunk_results: list[
        list[
            tuple[
                int,
                float,
                int | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ]
    ] = [[] for _ in chunks]
    chunk_received_ms = [0 for _ in chunks]
    def fetch_indices(indices: list[int]) -> None:
        # Bound each caller's outstanding work as well as total network
        # concurrency. Workers return rows; they never mutate the result after
        # it has been handed to an inference caller.
        remaining = iter(indices)
        pending = {}
        for _ in range(min(workers, len(indices))):
            index = next(remaining)
            submitted_indices.add(index)
            pending[_TW_MIS_HTTP_POOL.submit(fetch_chunk, chunks[index])] = index
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                index = pending.pop(future)
                rows = future.result()
                if rows:
                    chunk_results[index] = rows
                    chunk_received_ms[index] = int(time.time() * 1000)
                    if on_progress is not None:
                        indices = list(dict.fromkeys(row[0] for row in rows))
                        positions = {value: i for i, value in enumerate(indices)}
                        partial_prices = prices[indices].copy()
                        partial_fields = {name: np.full(len(indices), np.nan) for name in ("open_prices", "high_prices", "low_prices", "volumes", "upper_limit_prices", "lower_limit_prices", "bid_prices", "ask_prices", "bid_volumes", "ask_volumes", "reference_prices")}
                        market_ms = np.zeros(len(indices), dtype=np.int64)
                        for row in rows:
                            local = positions[row[0]]
                            partial_prices[local] = row[1]
                            market_ms[local] = int(row[2] or 0)
                            for name, value in zip(partial_fields, row[3:]):
                                if value is not None:
                                    partial_fields[name][local] = value
                        on_progress([symbols[i] for i in indices], PriceSnapshot(
                            prices=partial_prices, source="twse_tpex:mis", available_mask=np.ones(len(indices), dtype=bool), available_count=len(indices), requested_count=len(indices),
                            timestamps_ms=np.full(len(indices), chunk_received_ms[index], dtype=np.int64), exchange_timestamps_ms=market_ms, **partial_fields,
                        ))
                next_index = next(remaining, None)
                if next_index is not None and not stop_dispatch.is_set():
                    submitted_indices.add(next_index)
                    pending[_TW_MIS_HTTP_POOL.submit(fetch_chunk, chunks[next_index])] = next_index

    fetch_indices(list(range(len(chunks))))
    first_pass_ms = (time.perf_counter() - fetch_started) * 1000.0
    retry_rounds = 0

    # MIS intermittently closes connections or returns partial batches. Neither
    # proves a stock has no first trade. Retry only unresolved symbols in bounded
    # parallel rounds; never re-query or overwrite the rows already received.
    for attempt in range(retry_attempts):
        if stop_dispatch.is_set():
            break
        observed_indices = {row[0] for rows in chunk_results for row in rows}
        if len(observed_indices) >= int(np.ceil(len(symbols) * minimum_coverage)):
            break
        missing_items = list(dict.fromkeys(
            item for items in chunks for item in items
            if item[0] not in observed_indices
        ))
        if not missing_items:
            break
        if retry_delay_seconds > 0.0:
            time.sleep(retry_delay_seconds * (attempt + 1))
        retry_rounds += 1
        # Append retry jobs instead of replacing an original partial result:
        # each accepted row must retain its own original receipt clock.
        start = len(chunks)
        retries = [missing_items[i:i + chunk_len]
                   for i in range(0, len(missing_items), chunk_len)]
        chunks.extend(retries)
        chunk_results.extend([] for _ in retries)
        chunk_received_ms.extend(0 for _ in retries)
        missing_indices = list(range(start, len(chunks)))
        fetch_indices(missing_indices)

    exchange_timestamps_ms = np.zeros(len(symbols), dtype=np.int64)
    for chunk_idx, rows in enumerate(chunk_results):
        for (
            idx,
            value,
            timestamp_ms,
            open_px,
            high_px,
            low_px,
            volume,
            upper_px,
            lower_px,
            bid_px,
            ask_px,
            bid_volume,
            ask_volume,
            reference_px,
        ) in rows:
            prices[idx] = value
            filled[idx] = True
            timestamps_ms[idx] = max(int(timestamps_ms[idx]), int(chunk_received_ms[chunk_idx]))
            for target, observed in (
                (open_prices, open_px),
                (high_prices, high_px),
                (low_prices, low_px),
                (volumes, volume),
                (upper_limit_prices, upper_px),
                (lower_limit_prices, lower_px),
                (bid_prices, bid_px),
                (ask_prices, ask_px),
                (bid_volumes, bid_volume),
                (ask_volumes, ask_volume),
                (reference_prices, reference_px),
            ):
                if observed is not None:
                    target[idx] = observed
            if timestamp_ms is not None and (
                last_timestamp_ms is None or timestamp_ms > last_timestamp_ms
            ):
                last_timestamp_ms = timestamp_ms
            if timestamp_ms is not None:
                exchange_timestamps_ms[idx] = max(int(exchange_timestamps_ms[idx]), int(timestamp_ms))

    timestamp = None
    if last_timestamp_ms is not None:
        timestamp = datetime.fromtimestamp(
            last_timestamp_ms / 1000.0, tz=timezone.utc
        ).isoformat()
    return PriceSnapshot(
        prices=prices,
        source="twse_tpex:mis",
        transport_timing={
            "mis_first_pass_ms": round(first_pass_ms, 3),
            "mis_total_ms": round((time.perf_counter() - fetch_started) * 1000.0, 3),
            "mis_retry_rounds": retry_rounds,
            "mis_failed_request_count": len(chunk_failures),
            "mis_failure_types": sorted({item["error_type"] for item in chunk_failures}),
            "mis_http_error_statuses": sorted({item["http_status"] for item in chunk_failures if "http_status" in item}),
            "mis_http_workers": workers,
            "mis_keep_alive_pool": True,
            "mis_minimum_response_coverage": minimum_coverage,
            "mis_request_timeout_seconds": request_timeout,
            "mis_requested_chunk_size": int(chunk_size),
            "mis_effective_chunk_size": chunk_len,
            "mis_batch_policy": "deadline_compatible_channels_v1",
            "mis_request_count": len(request_observations),
            "mis_requests": request_observations,
            "mis_dispatch_stopped": stop_dispatch.is_set(),
            "mis_deferred_channel_count": sum(len(items) for i, items in enumerate(chunks)
                                              if i not in submitted_indices),
            "mis_error_chain_types": sorted({name for item in chunk_failures
                                            for name in item["error_chain_types"]}),
        },
        timestamp=timestamp,
        available_count=int(filled.sum()),
        available_mask=filled,
        open_prices=open_prices,
        high_prices=high_prices,
        low_prices=low_prices,
        volumes=volumes,
        upper_limit_prices=upper_limit_prices,
        lower_limit_prices=lower_limit_prices,
        bid_prices=bid_prices,
        ask_prices=ask_prices,
        bid_volumes=bid_volumes,
        ask_volumes=ask_volumes,
        reference_prices=reference_prices,
        timestamps_ms=timestamps_ms,
        exchange_timestamps_ms=exchange_timestamps_ms,
    )


def fetch_tw_mis_opening_snapshot(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    cache_ttl_seconds: float = 15.0,
    max_parallel_requests: int = 16,
    allow_network: bool = True,
    force_missing_retry: bool = False,
) -> PriceSnapshot:
    """Fetch one shared causal opening observation for all TW day-trade models.

    This session-keyed cache is only for model opening inputs. Execution must call
    Shioaji again for the causally later best Bid/Ask of actual order candidates.
    A complete market observation is shared so three models do not independently
    pay the same full-universe HTTP fan-out at 09:00. ``allow_network=False``
    performs a cache/receipt-only probe for the latency-critical source router.
    """

    global _TW_MIS_OPENING_CACHE_KEY, _TW_MIS_OPENING_DIRTY
    requested = [str(symbol).strip() for symbol in symbols]
    fallback = np.asarray(fallback_prices, dtype=np.float64)
    if len(requested) != len(fallback):
        raise ValueError("symbols and fallback_prices must have equal length")
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    cache_key = (str(Path(parquet_root).resolve()), session_date)
    ttl = max(0.0, float(cache_ttl_seconds))
    no_open_retry_seconds = max(
        0.0,
        float(
            os.getenv("STOCKAGENT_TW_OPENING_NO_OPEN_RETRY_SECONDS", "0.25")
            or "0.25"
        ),
    )

    with _TW_MIS_OPENING_LOCK:
        if _TW_MIS_OPENING_CACHE_KEY != cache_key:
            _TW_MIS_OPENING_CACHE.clear()
            _TW_MIS_OPENING_REQUESTED.clear()
            _TW_MIS_OPENING_CACHE_KEY = cache_key
            _TW_MIS_OPENING_DIRTY = False
            _TW_MIS_OPENING_CACHE.update(
                _load_tw_mis_opening_receipt(
                    parquet_root=cache_key[0],
                    session_date=session_date,
                )
            )
        now_monotonic = time.monotonic()
        missing_indices: list[int] = []
        for idx, symbol in enumerate(requested):
            cached = _TW_MIS_OPENING_CACHE.get(symbol)
            if cached is not None and _opening_row_has_open(cached[1], session_date):
                continue
            if cached is None or now_monotonic - cached[0] > ttl:
                missing_indices.append(idx)
                continue
            # Source-response coverage and the occurrence of an opening trade
            # are different facts.  Keep the causal row as transport evidence,
            # but retry no-open rows on every scheduler pass so a later auction
            # print self-heals without waiting for the long immutable-open TTL.
            if (
                force_missing_retry
                or now_monotonic - cached[0] > no_open_retry_seconds
            ):
                missing_indices.append(idx)
        cache_hits = len(requested) - len(missing_indices)
    # Only protect cache access. An HTTP sweep must not hold up a broker
    # callback, cache-only model read, or independent source repair.
    if missing_indices and allow_network:
        missing_symbols = [requested[idx] for idx in missing_indices]
        fresh = fetch_tw_mis_last_prices(
            missing_symbols,
            fallback[np.asarray(missing_indices, dtype=np.int64)],
            parquet_root=parquet_root,
            chunk_size=chunk_size,
            empty_chunk_retry_attempts=0,
            max_parallel_requests=max_parallel_requests,
            # Bound each HTTP wait, retaining gaps for independent retries.
            request_timeout_seconds=max(
                0.25,
                float(os.getenv("STOCKAGENT_TW_OPENING_REQUEST_TIMEOUT_SECONDS", "1.5") or "1.5"),
            ),
        )
        seed_tw_opening_snapshot_cache(
            missing_symbols, fresh, parquet_root=parquet_root, session_date=session_date,
        )

    with _TW_MIS_OPENING_LOCK:
        if _TW_MIS_OPENING_CACHE_KEY != cache_key:
            raise RuntimeError("opening cache generation changed during acquisition")
        size = len(requested)
        prices = fallback.copy()
        available = np.zeros((size,), dtype=bool)
        timestamps_ms = np.zeros((size,), dtype=np.int64)
        open_timestamps_ms = np.zeros((size,), dtype=np.int64)
        no_open_print_timestamps_ms = np.zeros((size,), dtype=np.int64)
        arrays = {
            field: np.full((size,), np.nan, dtype=np.float64)
            for field in _PRICE_SNAPSHOT_ARRAY_FIELDS
        }
        assembled_at = time.monotonic()
        for idx, symbol in enumerate(requested):
            cached = _TW_MIS_OPENING_CACHE.get(symbol)
            if cached is None:
                continue
            if not _opening_row_has_open(cached[1], session_date) and assembled_at - cached[0] > ttl:
                continue
            row = cached[1]
            available[idx] = bool(row.get("available"))
            price = row.get("price")
            if price is not None:
                prices[idx] = float(price)
            timestamps_ms[idx] = int(row.get("timestamp_ms") or 0)
            open_timestamps_ms[idx] = int(row.get("open_timestamp_ms") or row.get("exchange_timestamp_ms") or 0)
            no_open_print_timestamps_ms[idx] = int(row.get("no_open_print_observed_ms") or 0)
            for field, target in arrays.items():
                value = row.get(field)
                if value is not None:
                    target[idx] = float(value)
        observed_sources = {
            str(cached[1].get("open_source") or cached[1].get("source") or "twse_tpex:mis")
            for symbol in requested
            if (cached := _TW_MIS_OPENING_CACHE.get(symbol)) is not None
        }
        conflict_count = sum(len(row.get("opening_conflicts") or ()) for _at, row in _TW_MIS_OPENING_CACHE.values())

    latest_ms = int(timestamps_ms.max(initial=0))
    timestamp = (
        datetime.fromtimestamp(latest_ms / 1000.0, tz=timezone.utc).isoformat()
        if latest_ms > 0
        else None
    )
    has_shioaji = any("shioaji" in value for value in observed_sources)
    has_mis = any(value.startswith("twse_tpex:") for value in observed_sources)
    source = (
        "twse_tpex:mis+shared_opening_snapshot+shioaji_open_cache"
        if has_shioaji and has_mis
        else "shioaji:stock_snapshot+shared_opening_snapshot"
        if has_shioaji
        else "twse_tpex:mis+shared_opening_snapshot"
    )
    extra_sources = sorted(value for value in observed_sources if not value.startswith(("twse_tpex:", "shioaji:")))
    if extra_sources:
        source = (source + "+" if has_shioaji or has_mis else "") + "+".join(extra_sources) + "+shared_opening_snapshot"
    if cache_hits:
        source += "+cache_hit"
    return PriceSnapshot(
        prices=prices,
        source=source,
        timestamp=timestamp,
        available_count=int(available.sum()),
        requested_count=size,
        available_mask=available,
        open_prices=arrays["open_prices"],
        high_prices=arrays["high_prices"],
        low_prices=arrays["low_prices"],
        volumes=arrays["volumes"],
        upper_limit_prices=arrays["upper_limit_prices"],
        lower_limit_prices=arrays["lower_limit_prices"],
        bid_prices=arrays["bid_prices"],
        ask_prices=arrays["ask_prices"],
        bid_volumes=arrays["bid_volumes"],
        ask_volumes=arrays["ask_volumes"],
        reference_prices=arrays["reference_prices"],
        timestamps_ms=timestamps_ms,
        exchange_timestamps_ms=open_timestamps_ms,
        no_open_print_timestamps_ms=no_open_print_timestamps_ms,
        transport_timing={
            "opening_valid_open_count": int(np.count_nonzero(np.isfinite(arrays["open_prices"]) & (arrays["open_prices"] > 0))),
            "opening_requested_count": size,
            "opening_cache_hits": cache_hits,
            "opening_conflict_count": conflict_count,
            "opening_newly_requested_symbols": len(missing_indices) if allow_network else 0,
            "opening_missing_symbols": [requested[idx] for idx in range(size) if not (np.isfinite(arrays["open_prices"][idx]) and arrays["open_prices"][idx] > 0)],
        },
    )


def fetch_tw_mis_session_snapshot(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    cache_ttl_seconds: float = 120.0,
    max_parallel_requests: int = 16,
    force_refresh: bool = False,
    allow_network: bool = True,
) -> PriceSnapshot:
    """Return one shared, current-session MIS quote snapshot.

    This cache is deliberately separate from the immutable opening-observation
    cache.  At 13:30 the first model forces a bounded full refresh; subsequent
    models reuse those exact response rows and fetch only universe deltas.  It
    is memory-only, so it cannot overwrite the receipt used to prove 09:00
    opening inputs or enter formal portfolio history.
    """

    global _TW_MIS_SESSION_CACHE_KEY
    requested = [str(symbol).strip() for symbol in symbols]
    fallback = np.asarray(fallback_prices, dtype=np.float64)
    if len(requested) != len(fallback):
        raise ValueError("symbols and fallback_prices must have equal length")
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    ttl = max(0.1, float(cache_ttl_seconds))

    # Keep the network request inside the lock.  This is an intentional
    # single-flight boundary: concurrent model jobs wait for one market sweep
    # rather than multiplying HTTP fan-out at the close.
    with _TW_MIS_SESSION_LOCK:
        if _TW_MIS_SESSION_CACHE_KEY != session_date:
            _TW_MIS_SESSION_CACHE.clear()
            _TW_MIS_SESSION_CACHE_KEY = session_date
        if force_refresh:
            # A forced deadline observation may not fall back to an earlier
            # in-session row when the fresh request fails.
            for symbol in requested:
                _TW_MIS_SESSION_CACHE.pop(symbol, None)
        now_monotonic = time.monotonic()
        missing_indices = [
            idx
            for idx, symbol in enumerate(requested)
            if bool(force_refresh)
            or (cached := _TW_MIS_SESSION_CACHE.get(symbol)) is None
            or now_monotonic - cached[0] > ttl
        ]
        cache_hits = 0 if force_refresh else len(requested) - len(missing_indices)
        if missing_indices and allow_network:
            missing_symbols = [requested[idx] for idx in missing_indices]
            fresh = fetch_tw_mis_last_prices(
                missing_symbols,
                fallback[np.asarray(missing_indices, dtype=np.int64)],
                parquet_root=parquet_root,
                chunk_size=chunk_size,
                empty_chunk_retry_attempts=0,
                max_parallel_requests=max_parallel_requests,
                request_timeout_seconds=max(
                    0.25,
                    float(
                        os.getenv(
                            "STOCKAGENT_POSTCLOSE_FAST_REQUEST_TIMEOUT_SECONDS",
                            "1.5",
                        )
                        or "1.5"
                    ),
                ),
            )
            stored_at = time.monotonic()
            fresh_available = (
                np.asarray(fresh.available_mask, dtype=bool)
                if fresh.available_mask is not None
                else np.zeros((len(missing_symbols),), dtype=bool)
            )
            fresh_timestamps = (
                np.asarray(fresh.timestamps_ms, dtype=np.int64)
                if fresh.timestamps_ms is not None
                else np.zeros((len(missing_symbols),), dtype=np.int64)
            )
            for local_idx, symbol in enumerate(missing_symbols):
                timestamp_ms = int(fresh_timestamps[local_idx])
                if not (
                    bool(fresh_available[local_idx])
                    and _same_taipei_session_timestamp(timestamp_ms, session_date)
                ):
                    continue
                row: dict[str, Any] = {
                    "available": True,
                    "price": (
                        float(fresh.prices[local_idx])
                        if np.isfinite(fresh.prices[local_idx])
                        else None
                    ),
                    "timestamp_ms": timestamp_ms,
                    "source": str(fresh.source or "twse_tpex:mis"),
                }
                for field in _PRICE_SNAPSHOT_ARRAY_FIELDS:
                    values = getattr(fresh, field)
                    value = (
                        float(np.asarray(values)[local_idx])
                        if values is not None
                        else float("nan")
                    )
                    row[field] = value if np.isfinite(value) else None
                _TW_MIS_SESSION_CACHE[symbol] = (stored_at, row)

        size = len(requested)
        prices = fallback.copy()
        available = np.zeros((size,), dtype=bool)
        timestamps_ms = np.zeros((size,), dtype=np.int64)
        arrays = {
            field: np.full((size,), np.nan, dtype=np.float64)
            for field in _PRICE_SNAPSHOT_ARRAY_FIELDS
        }
        assembled_at = time.monotonic()
        observed_sources: set[str] = set()
        for idx, symbol in enumerate(requested):
            cached = _TW_MIS_SESSION_CACHE.get(symbol)
            if cached is None or assembled_at - cached[0] > ttl:
                continue
            row = cached[1]
            available[idx] = bool(row.get("available"))
            price = row.get("price")
            if price is not None:
                prices[idx] = float(price)
            timestamps_ms[idx] = int(row.get("timestamp_ms") or 0)
            observed_sources.add(str(row.get("source") or "twse_tpex:mis"))
            for field, target in arrays.items():
                value = row.get(field)
                if value is not None:
                    target[idx] = float(value)

    latest_ms = int(timestamps_ms.max(initial=0))
    timestamp = (
        datetime.fromtimestamp(latest_ms / 1000.0, tz=timezone.utc).isoformat()
        if latest_ms > 0
        else None
    )
    source = "twse_tpex:mis+shared_session_quote_snapshot"
    if observed_sources and all(
        value.startswith("shioaji:") for value in observed_sources
    ):
        source = "shioaji:stock_snapshot+shared_session_quote_snapshot"
    if cache_hits:
        source += "+cache_hit"
    return PriceSnapshot(
        prices=prices,
        source=source,
        timestamp=timestamp,
        available_count=int(available.sum()),
        requested_count=size,
        available_mask=available,
        open_prices=arrays["open_prices"],
        high_prices=arrays["high_prices"],
        low_prices=arrays["low_prices"],
        volumes=arrays["volumes"],
        upper_limit_prices=arrays["upper_limit_prices"],
        lower_limit_prices=arrays["lower_limit_prices"],
        bid_prices=arrays["bid_prices"],
        ask_prices=arrays["ask_prices"],
        bid_volumes=arrays["bid_volumes"],
        ask_volumes=arrays["ask_volumes"],
        reference_prices=arrays["reference_prices"],
        timestamps_ms=timestamps_ms,
    )


def _yahoo_session_and_crumb() -> tuple[requests.Session, str | None]:
    global _YAHOO_SESSION, _YAHOO_CRUMB
    with _YAHOO_SESSION_LOCK:
        if _YAHOO_SESSION is None:
            _YAHOO_SESSION = requests.Session()
        if not _YAHOO_CRUMB:
            try:
                _YAHOO_SESSION.get(
                    "https://fc.yahoo.com", timeout=8, headers=_YAHOO_HEADERS
                )
                response = _YAHOO_SESSION.get(
                    "https://query1.finance.yahoo.com/v1/test/getcrumb",
                    timeout=8,
                    headers=_YAHOO_HEADERS,
                )
                response.raise_for_status()
                crumb = response.text.strip()
                if crumb and "Too Many Requests" not in crumb:
                    _YAHOO_CRUMB = crumb
            except Exception:
                _YAHOO_CRUMB = None
        return _YAHOO_SESSION, _YAHOO_CRUMB


def fetch_yahoo_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    period: str = "1d",
    interval: str = "1m",
) -> PriceSnapshot:
    """Fetch latest Yahoo prices from the quote API and align them to panel symbols."""
    yahoo_map = load_symbol_yahoo_map(parquet_root)
    tickers = [yahoo_map.get(symbol, symbol) for symbol in symbols]
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    filled = np.zeros((len(symbols),), dtype=bool)
    last_timestamp_s: int | None = None
    chunk_len = max(1, int(chunk_size))
    chunks = [
        (start, tickers[start : start + chunk_len])
        for start in range(0, len(tickers), chunk_len)
    ]
    max_parallel = int(os.getenv("STOCKAGENT_YAHOO_PARALLEL_REQUESTS", "32") or "32")
    workers = max(1, min(len(chunks) or 1, max_parallel))

    def fetch_chunk(
        start: int,
        ticker_chunk: list[str],
        *,
        session: requests.Session | None = None,
        crumb: str | None = None,
    ) -> list[tuple[int, float, int | None]]:
        encoded = quote(",".join(str(ticker) for ticker in ticker_chunk), safe=",")
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}"
        params = {"crumb": crumb} if crumb else None
        request_get = session.get if session is not None else requests.get
        try:
            response = request_get(
                url,
                params=params,
                timeout=8,
                headers=_YAHOO_HEADERS,
            )
            response.raise_for_status()
            payload = response.json()
            result_rows = payload.get("quoteResponse", {}).get("result") or []
        except Exception:
            return []

        local_index: dict[str, list[int]] = {}
        for offset, ticker in enumerate(ticker_chunk):
            local_index.setdefault(str(ticker), []).append(start + offset)
        rows: list[tuple[int, float, int | None]] = []
        for item in result_rows:
            ticker = str(item.get("symbol") or "").strip()
            indices = local_index.get(ticker)
            if not indices:
                continue
            raw_value = (
                item.get("regularMarketPrice")
                or item.get("postMarketPrice")
                or item.get("preMarketPrice")
                or item.get("bid")
                or item.get("ask")
            )
            try:
                value = float(raw_value)
            except Exception:
                continue
            if not (np.isfinite(value) and value > 0.0):
                continue
            raw_time = (
                item.get("regularMarketTime")
                or item.get("postMarketTime")
                or item.get("preMarketTime")
            )
            try:
                timestamp_s = int(raw_time)
            except Exception:
                timestamp_s = None
            for index in indices:
                rows.append((index, value, timestamp_s))
        return rows

    def run_quote_pass(
        *, session: requests.Session | None = None, crumb: str | None = None
    ) -> None:
        nonlocal last_timestamp_s
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="yahoo-quote"
        ) as executor:
            futures = [
                executor.submit(
                    fetch_chunk, start, ticker_chunk, session=session, crumb=crumb
                )
                for start, ticker_chunk in chunks
            ]
            for future in as_completed(futures):
                for idx, value, timestamp_s in future.result():
                    prices[idx] = value
                    filled[idx] = True
                    if timestamp_s is not None and (
                        last_timestamp_s is None or timestamp_s > last_timestamp_s
                    ):
                        last_timestamp_s = timestamp_s

    run_quote_pass()
    quote_count = int(filled.sum())
    min_quote_retry_count = min(len(symbols), max(1, int(len(symbols) * 0.8)))
    used_crumb = False
    if quote_count < min_quote_retry_count:
        session, crumb = _yahoo_session_and_crumb()
        if crumb:
            run_quote_pass(session=session, crumb=crumb)
            used_crumb = True

    used_chart = False
    if not str(
        os.getenv("STOCKAGENT_YAHOO_CHART_FALLBACK", "1") or "1"
    ).strip().lower() in {"0", "false", "no", "off"} and not bool(filled.all()):
        missing = [idx for idx, ok in enumerate(filled) if not ok]
        fallback_cap = int(
            os.getenv("STOCKAGENT_YAHOO_CHART_FALLBACK_MAX_SYMBOLS", "200") or "200"
        )
        if fallback_cap >= 0:
            missing = missing[:fallback_cap]

        def fetch_chart(idx: int) -> tuple[int, float, int | None] | None:
            ticker = str(tickers[idx]).strip()
            if not ticker:
                return None
            encoded = quote(ticker, safe="")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={period}&interval={interval}"
            try:
                response = requests.get(
                    url,
                    timeout=8,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
                        )
                    },
                )
                response.raise_for_status()
                payload = response.json()
                result_rows = payload.get("chart", {}).get("result") or []
                if not result_rows:
                    return None
                row = result_rows[0]
                meta = row.get("meta") or {}
                raw_value = meta.get("regularMarketPrice") or meta.get(
                    "chartPreviousClose"
                )
                raw_time = meta.get("regularMarketTime")
                indicators = row.get("indicators", {}).get("quote") or []
                timestamps = row.get("timestamp") or []
                if indicators:
                    closes = indicators[0].get("close") or []
                    for pos in range(len(closes) - 1, -1, -1):
                        try:
                            close_value = float(closes[pos])
                        except Exception:
                            continue
                        if np.isfinite(close_value) and close_value > 0.0:
                            raw_value = close_value
                            if pos < len(timestamps):
                                raw_time = timestamps[pos]
                            break
                value = float(raw_value)
                if not (np.isfinite(value) and value > 0.0):
                    return None
                try:
                    timestamp_s = int(raw_time)
                except Exception:
                    timestamp_s = None
                return idx, value, timestamp_s
            except Exception:
                return None

        chart_workers = max(1, min(len(missing) or 1, max_parallel))
        with ThreadPoolExecutor(
            max_workers=chart_workers, thread_name_prefix="yahoo-chart"
        ) as executor:
            futures = [executor.submit(fetch_chart, idx) for idx in missing]
            for future in as_completed(futures):
                result = future.result()
                if result is None:
                    continue
                idx, value, timestamp_s = result
                prices[idx] = value
                filled[idx] = True
                used_chart = True
                if timestamp_s is not None and (
                    last_timestamp_s is None or timestamp_s > last_timestamp_s
                ):
                    last_timestamp_s = timestamp_s

    last_timestamp = (
        datetime.fromtimestamp(last_timestamp_s, tz=timezone.utc).isoformat()
        if last_timestamp_s is not None
        else None
    )
    source_parts = ["yahoo:quote"]
    if used_crumb:
        source_parts.append("crumb")
    if used_chart:
        source_parts.append("chart")
    source = "+".join(source_parts)
    return PriceSnapshot(
        prices=prices,
        source=source,
        timestamp=last_timestamp,
        available_count=int(filled.sum()),
    )
