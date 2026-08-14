from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import numpy as np
import requests

from stockagent.live.shioaji_traffic_ledger import (
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
_TW_LIMIT_CACHE_LOCK = threading.Lock()
_TW_LIMIT_CACHE_KEY: str | None = None
_TW_LIMIT_CACHE: dict[str, tuple[float | None, float | None, float | None]] = {}


@dataclass(slots=True)
class PriceSnapshot:
    prices: np.ndarray
    source: str
    timestamp: str | None = None
    available_count: int = 0
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


def _shioaji_stock_api() -> object:
    """Return one process-local, simulation-only Shioaji quote connection."""

    global _SHIOAJI_STOCK_API
    with _SHIOAJI_STOCK_LOCK:
        if _SHIOAJI_STOCK_API is not None:
            return _SHIOAJI_STOCK_API
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
        api.login(
            api_key=api_key,
            secret_key=secret_key,
            subscribe_trade=False,
        )
        _SHIOAJI_STOCK_API = api
        return api


def warm_shioaji_stock_quote_client() -> None:
    """Login and load contracts before the opening latency-sensitive path."""

    _shioaji_stock_api()


def close_shioaji_stock_quote_client() -> None:
    """Close the process-local quote connection, primarily for clean shutdowns/tests."""

    global _SHIOAJI_STOCK_API
    with _SHIOAJI_STOCK_LOCK:
        api = _SHIOAJI_STOCK_API
        _SHIOAJI_STOCK_API = None
        _SHIOAJI_STOCK_CONTRACTS.clear()
        _SHIOAJI_STOCK_CACHE.clear()
        if api is not None:
            try:
                api.logout()
            except Exception:
                pass


def _contract_positive(contract: object, *names: str) -> float | None:
    for name in names:
        value = _float_or_none(getattr(contract, name, None))
        if value is not None:
            return value
    return None


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
    return {
        "price": price,
        "open": _float_or_none(getattr(row, "open", None)),
        "high": _float_or_none(getattr(row, "high", None)),
        "low": _float_or_none(getattr(row, "low", None)),
        "volume": _float_or_none(
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
        "bid_volume": _float_or_none(getattr(row, "buy_volume", None)),
        "ask_volume": _float_or_none(getattr(row, "sell_volume", None)),
        "reference": reference,
        # Causal execution uses the local observation time.  Snapshot ``ts`` is
        # exchange metadata and can predate the request when a symbol is idle.
        "received_ms": int(received_ms),
    }


def _tw_price_limit_snapshot_path(trading_date: str | None = None) -> Path:
    date_text = trading_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    configured = str(os.getenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", "") or "").strip()
    root = Path(configured) if configured else Path("artifacts/live/tw_price_limits")
    return root / f"{date_text}.parquet"


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

    frame = pl.read_parquet(path)
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


def prepare_tw_price_limit_snapshot(
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
    temporary = path.with_suffix(".parquet.tmp")
    pl.DataFrame(rows).write_parquet(temporary)
    os.replace(temporary, path)
    # Reload via the stat-keyed cache so readers never retain the pre-replace map.
    loaded, _ = _load_prepared_tw_price_limits(date_text)
    return {
        "path": str(path),
        "trading_date": date_text,
        "requested_count": len(symbols),
        "prepared_count": len(loaded),
        "missing_count": max(0, len(set(symbols)) - len(loaded)),
    }


def fetch_shioaji_stock_snapshots(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    cache_ttl_seconds: float = 0.0,
) -> PriceSnapshot:
    """Fetch full-universe Shioaji stock snapshots in <=500-contract batches.

    The provider is simulation/login only and never submits an order.  Cached
    rows are useful when several models share the same opening observation;
    callers doing one-minute execution marks pass a zero TTL for fresh books.
    """

    if len(symbols) != len(fallback_prices):
        raise ValueError("symbols and fallback_prices must have equal length")
    api = _shioaji_stock_api()
    ttl = max(0.0, float(cache_ttl_seconds))
    now_monotonic = time.monotonic()
    requested = [str(symbol).strip() for symbol in symbols]

    with _SHIOAJI_STOCK_LOCK:
        missing_codes: list[str] = []
        for code in requested:
            cached = _SHIOAJI_STOCK_CACHE.get(code)
            if ttl > 0.0 and cached is not None and now_monotonic - cached[0] <= ttl:
                continue
            if code not in _SHIOAJI_STOCK_CONTRACTS:
                _SHIOAJI_STOCK_CONTRACTS[code] = api.contracts.get(code)
            if _SHIOAJI_STOCK_CONTRACTS[code] is not None:
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

        for start in range(0, len(missing_codes), 500):
            batch_codes = missing_codes[start : start + 500]
            contracts = [
                _SHIOAJI_STOCK_CONTRACTS[code]
                for code in batch_codes
                if _SHIOAJI_STOCK_CONTRACTS[code] is not None
            ]
            if not contracts:
                continue
            with shioaji_query(
                api,
                consumer="stock_quote_provider",
                method="snapshots",
                asset_class="stock",
                details={"contract_count": len(contracts)},
            ) as set_ledger_result:
                rows = list(api.snapshots(contracts))
                set_ledger_result(rows)
            # The causal observation boundary is when the complete response is
            # locally available, never when the request was sent.
            received_ms = int(time.time() * 1000)
            for row in rows:
                code = str(getattr(row, "code", "") or "").strip()
                contract = _SHIOAJI_STOCK_CONTRACTS.get(code)
                if not code or contract is None:
                    continue
                _SHIOAJI_STOCK_CACHE[code] = (
                    time.monotonic(),
                    _shioaji_snapshot_values(
                        row,
                        contract,
                        received_ms=received_ms,
                    ),
                )

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
        for idx, code in enumerate(requested):
            cached = _SHIOAJI_STOCK_CACHE.get(code)
            if cached is None:
                continue
            values = cached[1]
            price = values.get("price")
            if price is not None:
                prices[idx] = float(price)
                available[idx] = True
            for name, target in arrays.items():
                value = values.get(name)
                if value is not None:
                    target[idx] = float(value)
            timestamps_ms[idx] = int(values.get("received_ms") or 0)

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

    trading_date = np.datetime64(
        datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat(), "D"
    )
    official_reference = arrays["reference"]
    derived_upper = limit_price_numpy(official_reference, 1.10, trading_date)
    derived_lower = limit_price_numpy(official_reference, 0.90, trading_date)
    derived_count = 0
    for idx in range(len(requested)):
        if not available[idx]:
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
        raise RuntimeError("Shioaji returned no usable stock snapshots")
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
    return PriceSnapshot(
        prices=prices,
        source=source,
        timestamp=timestamp,
        available_count=count,
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
    )


def fetch_shioaji_futures_snapshot(
    logical_code: str = "TXFR1",
    *,
    additional_contract_codes: tuple[str, ...] = (),
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
    api = _shioaji_stock_api()
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
            details={
                "logical_code": normalized_logical,
                "contract_count": len(requested_contracts),
                "contracts": list(contracts),
            },
        ) as set_ledger_result:
            rows = list(api.snapshots(requested_contracts))
            set_ledger_result(rows)
        received_ms = int(time.time() * 1000)
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
    for path in sorted((selected_root / "runtime").glob("worker_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for code, row in (payload.get("books") or {}).items():
            if isinstance(row, dict):
                books[str(code).strip().upper()] = row
    current_codes = sorted(code for code in books if code.startswith(logical_root))
    current_code = current_codes[0] if len(current_codes) == 1 else ""
    required = {current_code, *(str(code).strip().upper() for code in additional_contract_codes)} - {""}
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
        quote_at = datetime.fromtimestamp(receive_ns / 1e9, tz=timezone.utc).astimezone(
            ZoneInfo("Asia/Taipei")
        ).isoformat(timespec="milliseconds")
        quotes[code] = {
            "contract_code": code,
            "last": None,
            "bid": bid,
            "ask": ask,
            "bid_volume": int(row.get("bid_volume_1") or 0),
            "ask_volume": int(row.get("ask_volume_1") or 0),
            "quote_at": quote_at,
            "source": "shioaji:fop_stream_local_book",
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
            return {
                str(row.get("code", "")).strip(): str(
                    row.get("yahoo_symbol", "")
                ).strip()
                for row in rows
                if str(row.get("code", "")).strip()
                and str(row.get("yahoo_symbol", "")).strip()
            }
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
    )


def _float_or_none(value: object) -> float | None:
    try:
        text = str(value).strip()
        if not text or text in {"-", "--", "null", "None"}:
            return None
        parsed = float(text.replace(",", ""))
    except Exception:
        return None
    if not (np.isfinite(parsed) and parsed > 0.0):
        return None
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


def fetch_tw_mis_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
) -> PriceSnapshot:
    """Fetch Taiwan intraday prices from TWSE MIS and align them to panel symbols."""
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
    # MIS silently returns partial/empty payloads when ex_ch grows too large.
    # Keep the public caller knob for smaller probes, but never exceed the
    # stable endpoint batch used by the full-universe fetcher.
    chunk_len = max(1, min(int(chunk_size), 80))
    chunks = [
        ex_channels[start : start + chunk_len]
        for start in range(0, len(ex_channels), chunk_len)
    ]
    max_parallel = int(os.getenv("STOCKAGENT_TW_MIS_PARALLEL_REQUESTS", "4") or "4")
    workers = max(1, min(len(chunks) or 1, max_parallel))
    retry_attempts = max(
        0,
        int(os.getenv("STOCKAGENT_TW_MIS_RETRY_ATTEMPTS", "3") or "3"),
    )
    retry_delay_seconds = max(
        0.0,
        float(os.getenv("STOCKAGENT_TW_MIS_RETRY_DELAY_SECONDS", "0.35") or "0.35"),
    )
    session_local = threading.local()

    def session() -> requests.Session:
        sess = getattr(session_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update(_YAHOO_HEADERS)
            try:
                sess.get("https://mis.twse.com.tw/stock/index.jsp", timeout=8)
            except Exception:
                pass
            session_local.session = sess
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
        if not items:
            return []
        ex_ch = "|".join(ex for _, ex in items)
        code_to_indices: dict[str, list[int]] = {}
        for idx, _ex in items:
            code_to_indices.setdefault(str(symbols[idx]), []).append(idx)
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
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return []
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
            for idx in code_to_indices.get(code, []):
                rows.append(
                    (
                        idx,
                        value,
                        timestamp_ms,
                        _float_or_none(item.get("o")),
                        _float_or_none(item.get("h")),
                        _float_or_none(item.get("l")),
                        _float_or_none(item.get("v")),
                        _float_or_none(item.get("u")),
                        _float_or_none(item.get("w")),
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
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="tw-mis-quote"
    ) as executor:
        futures = {
            executor.submit(fetch_chunk, chunk): chunk_index
            for chunk_index, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_results[futures[future]] = future.result()

    # MIS intermittently closes full-universe connections. Empty chunks are not
    # usable evidence that no symbol traded, so retry only those chunks in a
    # paced single-thread pass. Never substitute the intraday last price for a
    # missing opening price; callers receive NaN in open_prices and fail closed.
    for chunk_index, rows in enumerate(chunk_results):
        if rows or retry_attempts <= 0:
            continue
        for attempt in range(retry_attempts):
            if retry_delay_seconds > 0.0:
                time.sleep(retry_delay_seconds * (attempt + 1))
            rows = fetch_chunk(chunks[chunk_index])
            if rows:
                chunk_results[chunk_index] = rows
                break

    for rows in chunk_results:
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
                timestamps_ms[idx] = max(int(timestamps_ms[idx]), int(timestamp_ms))

    timestamp = None
    if last_timestamp_ms is not None:
        timestamp = datetime.fromtimestamp(
            last_timestamp_ms / 1000.0, tz=timezone.utc
        ).isoformat()
    return PriceSnapshot(
        prices=prices,
        source="twse_tpex:mis",
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
