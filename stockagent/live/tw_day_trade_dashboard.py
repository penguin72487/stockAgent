"""Read-only, source-backed dashboard snapshot for stock day-trade simulation."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import (
    date as datetime_date,
    datetime,
    time as datetime_time,
    timedelta,
    timezone,
)
from functools import lru_cache
import gzip
import hashlib
import heapq
import io
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
from typing import Any, Final
from zoneinfo import ZoneInfo

from stockagent.data.tw_stock_futures_catalog import load_stock_futures_catalog
from stockagent.live.dashboard_updates import metadata_signature
from stockagent.live.performance_contract import (
    capital_return,
    paper_account_performance,
)
from stockagent.live.market_status import (
    _tw_holiday_schedule_path,
    verified_tw_stock_session_day,
)

from stockagent.live.benchmark_accounting import (
    DAILY_RETURN_BASIS_PREVIOUS_CLOSE,
    TX_FULLY_COLLATERALIZED_CAPITAL_BASIS,
    fully_collateralized_futures_notional,
    previous_close_return,
)
from stockagent.live.benchmark_history_projection import (
    BENCHMARK_HISTORY_INTERIOR_FIELDS as _BENCHMARK_HISTORY_INTERIOR_FIELDS,
    BenchmarkProjection,
    load_benchmark_projection,
    projection_head_path as benchmark_projection_head_path,
)
from stockagent.live.tw_day_trade_service_sync import (
    DISCORD_SERVICE_STATUS_FILENAME,
    age_seconds,
    load_service_sync,
    read_json_object,
)


DASHBOARD_SCHEMA_VERSION: Final[int] = 5
DEFAULT_MAX_SOURCE_AGE_SECONDS: Final[float] = 30.0
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
DEFAULT_CALENDAR_PARQUET_ROOT: Final[Path] = (
    Path(__file__).resolve().parents[2] / "data_tw_public/stocks"
)
BENCHMARK_HISTORY_FILENAME: Final[str] = "benchmark_history.json"
OVERNIGHT_HISTORY_FILENAME: Final[str] = "overnight_history.json"
OVERNIGHT_SIGNAL_HISTORY_FILENAME: Final[str] = "overnight_signal_history.parquet"
OVERNIGHT_EVENT_HISTORY_FILENAME: Final[str] = "overnight_event_history.parquet"
OVERNIGHT_POSITION_HISTORY_DIRNAME: Final[str] = "overnight_position_history"
DEFAULT_OPENING_GATE_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "artifacts/data_refresh/tw_public/preopen_gate/latest.json"
)
DEFAULT_UNATTENDED_GUARDIAN_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "artifacts/operations/tw_day_trade_guardian/latest.json"
)
CHART_RANGE_SECONDS: Final[dict[str, int | None]] = {
    "1h": 60 * 60,
    "1d": 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
    "1mo": 30 * 24 * 60 * 60,
    "1q": 90 * 24 * 60 * 60,
    "1y": 365 * 24 * 60 * 60,
    "all": None,
}
_LINE_COUNT_CACHE: dict[Path, tuple[int, int, int, int, int]] = {}
_LINE_COUNT_LOCK = threading.Lock()
_TAIL_CACHE: dict[
    tuple[Path, int], tuple[int, int, int, int, list[dict[str, Any]]]
] = {}
_TAIL_CACHE_LOCK = threading.Lock()
_TAIL_CACHE_MAX_ENTRIES: Final[int] = 16
_SESSION_TAIL_CACHE: dict[
    tuple[Path, int, str, bool], tuple[int, int, int, int, list[dict[str, Any]]]
] = {}
_SESSION_TAIL_CACHE_LOCK = threading.Lock()
_LATEST_SESSION_BLOCK_CACHE: dict[
    tuple[Path, int, str, bool], tuple[int, int, int, int, list[dict[str, Any]]]
] = {}
_LATEST_SESSION_BLOCK_CACHE_LOCK = threading.Lock()
_SIGNAL_FEATURE_SUMMARY_CACHE: dict[
    tuple[int, int, int, int], list[dict[str, Any]]
] = {}
_SIGNAL_FEATURE_SUMMARY_CACHE_LOCK = threading.Lock()
_AVAILABLE_SESSION_DATES_CACHE: dict[Path, tuple[tuple[Any, ...], list[str]]] = {}
_AVAILABLE_SESSION_DATES_CACHE_LOCK = threading.Lock()
_OBJECT_CACHE: dict[Path, tuple[tuple[int, ...], bytes, dict[str, Any]]] = {}
_OBJECT_CACHE_LOCK = threading.Lock()
_SIGNAL_PAGE_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_SIGNAL_PAGE_CACHE_LOCK = threading.Lock()
_HISTORY_SNAPSHOT_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_HISTORY_SNAPSHOT_CACHE_LOCK = threading.Lock()
_HISTORY_SNAPSHOT_CACHE_MAX_ENTRIES: Final[int] = 4
_HISTORY_PROJECTION_CACHE_SCHEMA_VERSION: Final[int] = 1
_HISTORY_PROJECTION_CACHE_MAX_COMPRESSED_BYTES: Final[int] = 64 * 1024 * 1024
_HISTORY_SESSION_PROJECTION_SCHEMA_VERSION: Final[int] = 2
_HISTORY_SESSION_PROJECTION_MAX_COMPRESSED_BYTES: Final[int] = 4 * 1024 * 1024
_HISTORY_SESSION_REBUILD_LIMIT: Final[int] = 8
_MAX_LEDGER_LINE_BYTES: Final[int] = 8 * 1024 * 1024
_COLUMNAR_LEDGER_MIN_BYTES: Final[int] = 256 * 1024
_COLUMNAR_LEDGER_MAX_BYTES: Final[int] = 256 * 1024 * 1024
_COLUMNAR_LEDGER_WRITE_BUFFER_BYTES: Final[int] = 8 * 1024 * 1024
_SIGNAL_PAGE_COLUMN_TYPES: Final[dict[str, str]] = {
    "action": "string",
    "ask": "float",
    "bid": "float",
    "counterfactual_open_replay": "bool",
    "counterfactual_0901_price_fill": "bool",
    "counterfactual_overnight_replay": "bool",
    "day_trade_eligible": "bool",
    "exchange_quote_at": "string",
    "execution_price": "float",
    "filled_shares": "int",
    "filled_weight": "float",
    "inventory_weight_after": "float",
    "lower_limit": "float",
    "market": "string",
    "model_trained_for_overnight": "bool",
    "name": "string",
    "open_reconstructed_at": "string",
    "order_limit_price": "float",
    "quote_at": "string",
    "raw_score": "float",
    "reason": "string",
    "requested_shares": "int",
    "score": "float",
    "sell_first_allowed": "bool",
    "session_date": "string",
    "side": "string",
    "signal_at": "string",
    "signal_id": "string",
    "simtrade": "bool",
    "simulation_replay": "bool",
    "sizing_capital_twd": "float",
    "sizing_open_price": "float",
    "sizing_price_at_13_25": "float",
    "source_signal_at": "string",
    "status": "string",
    "symbol": "string",
    "target_weight": "float",
    "temporary_day_trade_model_adapter": "bool",
    "top_book_capacity_shares": "int",
    "upper_limit": "float",
}
_EVENT_PAGE_COLUMN_TYPES: Final[dict[str, str]] = {
    "commission_rebate_accrued_twd": "float",
    "depth_assumption": "string",
    "fee_and_tax_twd": "float",
    "fill_at": "string",
    "fill_contract": "string",
    "filled_quantity": "int",
    "gross_fee_and_tax_twd": "float",
    "gross_pnl_twd": "float",
    "market": "string",
    "net_pnl_twd": "float",
    "order_type": "string",
    "price": "float",
    "price_limit_offset_ticks": "int",
    "pricing_rule": "string",
    "purpose": "string",
    "quantity": "int",
    "quote_at": "string",
    "recorded_at": "string",
    "remaining_quantity": "int",
    "replay_basis": "string",
    "requested_quantity": "int",
    "session_date": "string",
    "side": "string",
    "simulation_only": "bool",
    "simulation_replay": "bool",
    "status": "string",
    "symbol": "string",
    "unfilled_quantity": "int",
}
_SESSION_DATE_FIELD_PATTERN: Final[re.Pattern[bytes]] = re.compile(
    rb'"session_date"\s*:\s*"(\d{4}-\d{2}-\d{2})"'
)


@dataclass
class _LedgerSessionIndex:
    device: int
    inode: int
    observed_size: int
    modified_ns: int
    scanned_offset: int
    spans: dict[str, list[tuple[int, int]]]


_LEDGER_SESSION_INDEX_CACHE: dict[tuple[Path, bool], _LedgerSessionIndex] = {}
_LEDGER_SESSION_INDEX_LOCK = threading.Lock()
_LEDGER_SESSION_INDEX_SCHEMA_VERSION: Final[int] = 1
_LEDGER_SESSION_INDEX_CACHE_ENV: Final[str] = "STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"


def _persistent_ledger_index_path(
    source: Path, *, recorded_at_fallback: bool
) -> Path | None:
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    if not cache_root:
        return None
    root = Path(cache_root)
    if not root.is_dir():
        return None
    identity = (
        f"{source.resolve()}\0recorded_at_fallback={int(recorded_at_fallback)}"
    ).encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()
    return root / f"ledger-session-index-v1-{digest}.json"


def _load_persistent_ledger_index(
    source: Path,
    *,
    recorded_at_fallback: bool,
    stat: os.stat_result,
) -> _LedgerSessionIndex | None:
    cache_path = _persistent_ledger_index_path(
        source, recorded_at_fallback=recorded_at_fallback
    )
    if cache_path is None or not cache_path.is_file():
        return None
    try:
        payload = json.loads(cache_path.read_bytes())
        if not isinstance(payload, Mapping):
            return None
        if int(payload.get("schema_version") or 0) != (
            _LEDGER_SESSION_INDEX_SCHEMA_VERSION
        ):
            return None
        if str(payload.get("source") or "") != str(source.resolve()):
            return None
        if bool(payload.get("recorded_at_fallback")) != bool(recorded_at_fallback):
            return None
        device = int(payload.get("device"))
        inode = int(payload.get("inode"))
        observed_size = int(payload.get("observed_size"))
        modified_ns = int(payload.get("modified_ns"))
        scanned_offset = int(payload.get("scanned_offset"))
        if (device, inode) != (stat.st_dev, stat.st_ino):
            return None
        if not 0 <= scanned_offset <= observed_size <= stat.st_size:
            return None
        if stat.st_size == observed_size and stat.st_mtime_ns != modified_ns:
            return None
        raw_spans = payload.get("spans")
        if not isinstance(raw_spans, Mapping):
            return None
        spans: dict[str, list[tuple[int, int]]] = {}
        for raw_date, raw_ranges in raw_spans.items():
            session_date = str(raw_date)
            datetime_date.fromisoformat(session_date)
            if not isinstance(raw_ranges, list):
                return None
            validated: list[tuple[int, int]] = []
            for raw_range in raw_ranges:
                if not isinstance(raw_range, list) or len(raw_range) != 2:
                    return None
                start, end = int(raw_range[0]), int(raw_range[1])
                if not 0 <= start < end <= observed_size:
                    return None
                validated.append((start, end))
            spans[session_date] = validated
        return _LedgerSessionIndex(
            device=device,
            inode=inode,
            observed_size=observed_size,
            modified_ns=modified_ns,
            scanned_offset=scanned_offset,
            spans=spans,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _persist_ledger_index(
    source: Path,
    *,
    recorded_at_fallback: bool,
    index: _LedgerSessionIndex,
) -> None:
    cache_path = _persistent_ledger_index_path(
        source, recorded_at_fallback=recorded_at_fallback
    )
    if cache_path is None:
        return
    payload = {
        "schema_version": _LEDGER_SESSION_INDEX_SCHEMA_VERSION,
        "source": str(source.resolve()),
        "recorded_at_fallback": bool(recorded_at_fallback),
        "device": index.device,
        "inode": index.inode,
        "observed_size": index.observed_size,
        "modified_ns": index.modified_ns,
        "scanned_offset": index.scanned_offset,
        "spans": {
            session_date: [[start, end] for start, end in ranges]
            for session_date, ranges in index.spans.items()
        },
    }
    temporary = cache_path.with_name(
        f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        encoded = (
            json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, cache_path)
    except OSError:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


@dataclass(frozen=True)
class _BenchmarkHistoryIndex:
    device: int
    inode: int
    size: int
    modified_ns: int
    origins: dict[str, Mapping[str, Any]]
    marks: tuple[Mapping[str, Any], ...]
    marks_by_session: dict[str, tuple[Mapping[str, Any], ...]]
    load_error: str | None = None


def _empty_benchmark_history_index() -> _BenchmarkHistoryIndex:
    return _BenchmarkHistoryIndex(
        device=0,
        inode=0,
        size=0,
        modified_ns=0,
        origins={},
        marks=(),
        marks_by_session={},
        load_error="benchmark_history_index_warming",
    )


def _benchmark_index_from_projection(
    source: Path, projection: BenchmarkProjection
) -> _BenchmarkHistoryIndex:
    stat = Path(source).stat()
    marks_by_session = {
        str(session_date): tuple(projection.marks_by_session.get(session_date, ()))
        for session_date in projection.session_entries
    }
    return _BenchmarkHistoryIndex(
        device=stat.st_dev,
        inode=stat.st_ino,
        size=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        origins=projection.origins,
        marks=projection.marks,
        marks_by_session=marks_by_session,
        load_error=None,
    )


_BENCHMARK_HISTORY_INDEX_CACHE: dict[Path, _BenchmarkHistoryIndex] = {}
_BENCHMARK_HISTORY_INDEX_LOCK = threading.Lock()
_BENCHMARK_HISTORY_CACHE_SCHEMA_VERSION: Final[int] = 2
_BENCHMARK_HISTORY_CACHE_MAX_COMPRESSED_BYTES: Final[int] = 64 * 1024 * 1024
@dataclass(frozen=True, slots=True)
class _PositionHistoryEntry:
    identity: str
    source_index: int
    row_index: int
    session_date: str
    market: str
    symbol: str
    name: str
    signed_shares: int
    target_weight: float


@dataclass(frozen=True, slots=True)
class _PositionHistoryIndex:
    source_signature: tuple[tuple[str, int, int, int, int], ...]
    sources: tuple[Path, ...]
    entries: tuple[_PositionHistoryEntry, ...]


_POSITION_HISTORY_INDEX_CACHE: dict[Path, _PositionHistoryIndex] = {}
_POSITION_HISTORY_INDEX_LOCK = threading.Lock()
_POSITION_HISTORY_INDEX_SCHEMA_VERSION: Final[int] = 1


@lru_cache(maxsize=32)
def _session_clock_cached(
    local_date: str,
    after_rollover: bool,
    parquet_root: Path,
    calendar_signature: tuple,
) -> dict[str, Any]:
    """Presentation clock only: a verified session is not a readiness/fill claim."""
    today = datetime_date.fromisoformat(local_date)
    display_date = None
    next_rollover = None
    # Include long exchange holiday breaks, without inventing a weekday calendar.
    for offset in range(40):
        day = today - timedelta(days=offset + (not after_rollover))
        valid, reason = verified_tw_stock_session_day(day, parquet_root=parquet_root)
        if valid:
            display_date = day.isoformat()
            break
        if "missing" in reason or "unverified" in reason:
            break
    for offset in range(40):
        day = today + timedelta(days=offset + after_rollover)
        valid, reason = verified_tw_stock_session_day(day, parquet_root=parquet_root)
        if valid:
            next_rollover = datetime.combine(
                day, datetime_time(8, 30), tzinfo=TAIPEI
            ).isoformat()
            break
        if "missing" in reason or "unverified" in reason:
            break
    return {
        "display_session_date": display_date,
        "next_rollover_at": next_rollover,
        "rollover_local_time": "08:30",
        "timezone": "Asia/Taipei",
        "calendar_verified": display_date is not None and next_rollover is not None,
        "readiness_implied": False,
    }


def dashboard_session_clock(observed: datetime) -> dict[str, Any]:
    parquet_root = DEFAULT_CALENDAR_PARQUET_ROOT
    path = _tw_holiday_schedule_path(parquet_root)
    try:
        stat = path.stat() if path is not None else None
        signature = (
            (str(path.resolve()), stat.st_ino, stat.st_size, stat.st_mtime_ns)
            if stat
            else ()
        )
    except OSError:
        signature = ()
    local = observed.astimezone(TAIPEI)
    return dict(
        _session_clock_cached(
            local.date().isoformat(),
            local.time() >= datetime_time(8, 30),
            parquet_root,
            signature,
        )
    )


def build_dashboard_revision(
    *,
    state_dir: Path,
    discord_service_status_path: Path | None = None,
    discord_markets_field: str = "day_trade_markets",
    discord_engine_revision_field: str = "engine_state_revision",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the tiny cross-service commit/ack state used for fast polling."""

    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    root = Path(state_dir)
    engine = load_service_sync(root)
    if engine is None:
        try:
            status = _object(root / "status.json")
        except (OSError, ValueError, json.JSONDecodeError):
            status = {}
        engine = {
            "state_revision": status.get("state_revision"),
            "content_revision": status.get("content_revision"),
            "engine_run_id": status.get("engine_run_id"),
            "published_at": status.get("updated_at"),
            "enabled_markets": sorted((status.get("modes") or {}).keys()),
            "modes": status.get("modes") or {},
        }

    bot_path = (
        Path(discord_service_status_path)
        if discord_service_status_path is not None
        else None
    )
    try:
        bot = read_json_object(bot_path) if bot_path is not None else None
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        bot = None

    engine_revision = int(engine.get("state_revision") or 0)
    content_revision = int(engine.get("content_revision") or engine_revision)
    bot_revision = int((bot or {}).get(discord_engine_revision_field) or 0)
    engine_markets = sorted(str(item) for item in engine.get("enabled_markets") or ())
    bot_markets = sorted(
        str(item) for item in (bot or {}).get(discord_markets_field) or ()
    )
    engine_age = age_seconds(engine.get("published_at"), now=observed)
    bot_age = age_seconds((bot or {}).get("updated_at"), now=observed)
    bot_fresh = bot_age is not None and bot_age <= 5.0
    bot_connected = bool((bot or {}).get("discord_connected", False))
    synchronized = bool(
        bot is not None
        and bot_fresh
        and bot_connected
        and engine_revision > 0
        and bot_revision == engine_revision
        and bot_markets == engine_markets
    )
    if bot_path is None:
        status_text = "engine_committed"
    elif bot is None or not bot_fresh:
        status_text = "discord_stale"
    elif not bot_connected:
        status_text = "discord_connecting"
    elif bot_revision != engine_revision or bot_markets != engine_markets:
        status_text = "catching_up"
    else:
        status_text = "synchronized"

    preopen_revision = "none"
    if bot_path is not None:
        preopen_path = bot_path.with_name("preopen_readiness.json")
        try:
            preopen_stat = preopen_path.stat()
            preopen_revision = f"{preopen_stat.st_size}:{preopen_stat.st_mtime_ns}"
        except OSError:
            preopen_revision = "missing"

    session_clock = (
        dashboard_session_clock(observed)
        if discord_markets_field == "day_trade_markets"
        else {}
    )
    session_token = session_clock.get("display_session_date") or "unverified"
    return {
        "schema_version": 1,
        "generated_at_utc": observed.isoformat(timespec="milliseconds"),
        "revision_token": f"{content_revision}:{preopen_revision}:{session_token}",
        "session_clock": session_clock,
        "state_revision": engine_revision,
        "content_revision": content_revision,
        "engine_published_at": engine.get("published_at"),
        "engine_age_seconds": (
            round(engine_age, 3) if engine_age is not None else None
        ),
        "enabled_markets": engine_markets,
        "discord": {
            "available": bot is not None,
            "connected": bot_connected,
            "updated_at": (bot or {}).get("updated_at"),
            "age_seconds": round(bot_age, 3) if bot_age is not None else None,
            "engine_state_revision": bot_revision,
            "day_trade_markets": bot_markets,
            "markets": bot_markets,
            "markets_field": discord_markets_field,
        },
        "status": status_text,
        "synchronized": synchronized,
        "revision_lag": max(0, engine_revision - bot_revision),
        "contract": (
            "Discord publishes immutable signals; the paper engine owns the ledger "
            "and publishes this commit revision last; the dashboard is read-only."
        ),
    }


def _object(path: Path, *, use_cache: bool = True) -> dict[str, Any]:
    cache_key = path.resolve()
    signature = metadata_signature(path.stat())
    if use_cache:
        with _OBJECT_CACHE_LOCK:
            cached = _OBJECT_CACHE.get(cache_key)
            if cached is not None and cached[0] == signature:
                return dict(cached[2])
    # Bind bytes to the open descriptor, not a new pathname after replacement.
    with path.open("rb") as stream:
        opened = metadata_signature(os.fstat(stream.fileno()))
        raw = stream.read()
        finished = metadata_signature(os.fstat(stream.fileno()))
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    if (
        use_cache
        and signature == opened == finished == metadata_signature(path.stat())
    ):
        digest = hashlib.blake2b(raw, digest_size=16).digest()
        with _OBJECT_CACHE_LOCK:
            _OBJECT_CACHE[cache_key] = (signature, digest, payload)
            while len(_OBJECT_CACHE) > 512:
                _OBJECT_CACHE.pop(next(iter(_OBJECT_CACHE)))
    return dict(payload)


def _unattended_guardian_status(*, path: Path, observed: datetime) -> dict[str, Any]:
    try:
        receipt = _object(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {
            "status": "missing",
            "ready": False,
            "age_seconds": None,
            "failure_count": 1,
            "warning_count": 0,
            "action_count": 0,
            "components": {},
        }
    receipt_age = age_seconds(receipt.get("observed_at_taipei"), now=observed)
    components = receipt.get("components")
    components = dict(components) if isinstance(components, Mapping) else {}
    time_sync = components.get("time_sync")
    source_events = components.get("source_events")
    runtime_sync = components.get("runtime_sync")
    public_dashboard = components.get("public_dashboard")
    post_close = components.get("post_close_flat")
    disks = components.get("disks")
    disk_rows = dict(disks) if isinstance(disks, Mapping) else {}
    return {
        "status": str(receipt.get("status") or "unknown"),
        "ready": bool(receipt.get("ready") is True),
        "observed_at_taipei": receipt.get("observed_at_taipei"),
        "age_seconds": round(receipt_age, 3) if receipt_age is not None else None,
        "failure_count": len(receipt.get("failures") or ()),
        "warning_count": len(receipt.get("warnings") or ()),
        "action_count": len(receipt.get("actions") or ()),
        "simulation_only": receipt.get("simulation_only") is True,
        "production_order_possible": bool(
            receipt.get("production_order_possible", True)
        ),
        "components": {
            "time_sync": bool(
                isinstance(time_sync, Mapping) and time_sync.get("ready") is True
            ),
            "source_events": bool(
                isinstance(source_events, Mapping)
                and source_events.get("ready") is True
            ),
            "runtime_sync": bool(
                isinstance(runtime_sync, Mapping) and runtime_sync.get("ready") is True
            ),
            "public_dashboard": bool(
                isinstance(public_dashboard, Mapping)
                and public_dashboard.get("ready") is True
            ),
            "post_close_flat": bool(
                isinstance(post_close, Mapping) and post_close.get("ready") is True
            ),
            **(
                {
                    "post_close_accounting": bool(
                        components["post_close_accounting"].get("ready")
                    )
                }
                if isinstance(components.get("post_close_accounting"), Mapping)
                else {}
            ),
            "disk": bool(
                disk_rows
                and all(
                    isinstance(row, Mapping) and row.get("ready") is True
                    for row in disk_rows.values()
                )
            ),
        },
    }


def _as_path(path: Path, value: Any) -> Path | None:
    if value in ("", None):
        return None
    candidate = Path(str(value))
    if not candidate.is_absolute():
        candidate = path / candidate
    return candidate


def _read_signal_feature_drivers(summary_path: Path) -> list[dict[str, Any]]:
    signature = summary_path.stat()
    cache_key = (
        signature.st_dev,
        signature.st_ino,
        signature.st_size,
        signature.st_mtime_ns,
    )
    with _SIGNAL_FEATURE_SUMMARY_CACHE_LOCK:
        cached = _SIGNAL_FEATURE_SUMMARY_CACHE.get(cache_key)
        if cached is not None:
            return list(cached)
    try:
        summary = _object(summary_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    explanation = summary.get("model_explanation")
    raw_drivers = (
        explanation.get("all_feature_drivers")
        if isinstance(explanation, Mapping)
        else None
    )
    if not isinstance(raw_drivers, list):
        raw_drivers = (
            explanation.get("top_feature_drivers")
            if isinstance(explanation, Mapping)
            else None
        )
    if not isinstance(raw_drivers, list):
        return []
    drivers: list[dict[str, Any]] = []
    for item in raw_drivers:
        if not isinstance(item, Mapping):
            continue
        feature = str(item.get("feature") or "")
        if not feature:
            continue

        driver: dict[str, Any] = {"feature": feature}
        for field, raw_value in item.items():
            field_key = str(field)
            if field_key in {"feature", "_internal", "path", "source_path"}:
                continue
            if field_key == "weighted_abs_value":
                value = _finite_float(raw_value)
                driver[field_key] = value if value is not None else 0.0
            else:
                driver[field_key] = raw_value
        if not any(key != "feature" for key in driver):
            # Keep legacy fallback behavior for compatibility with older summaries.
            driver["weighted_abs_value"] = 0.0
        drivers.append(driver)
    with _SIGNAL_FEATURE_SUMMARY_CACHE_LOCK:
        if signature.st_size < 64 * 1024 * 1024:
            _SIGNAL_FEATURE_SUMMARY_CACHE[cache_key] = list(drivers)
    return drivers


def _signal_row_key(row: Mapping[str, Any]) -> str:
    session_date = str(row.get("session_date") or "")
    market = str(row.get("market") or "")
    symbol = str(row.get("symbol") or "")
    signal_at = str(row.get("signal_at") or "")
    if session_date and market and symbol:
        if signal_at:
            return f"{session_date}|{market}|{symbol}|{signal_at}"
        return f"{session_date}|{market}|{symbol}"
    return ""


def _signal_feature_roots(
    state: Mapping[str, Any], state_dir: Path
) -> tuple[dict[str, list[Path]], list[Path]]:
    mode_roots: dict[str, list[Path]] = {}
    fallback_roots: list[Path] = []
    for market, mode in (state.get("modes") or {}).items():
        if not isinstance(mode, Mapping):
            continue
        summary_path = _as_path(state_dir, mode.get("signal_source_path"))
        if (
            not summary_path
            or not summary_path.is_file()
            or summary_path.name != "summary.json"
        ):
            continue
        roots = summary_path.parent.parent.parent
        if not roots:
            continue
        market_key = str(market)
        mode_roots.setdefault(market_key, [])
        if roots not in mode_roots[market_key]:
            mode_roots[market_key].append(roots)
        if roots not in fallback_roots:
            fallback_roots.append(roots)
    return mode_roots, fallback_roots


def _lookup_signal_feature_drivers(
    state_dir: Path,
    state: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    market_roots, fallback_roots = _signal_feature_roots(state, state_dir)
    if not rows:
        return output
    for row in rows:
        signal_id = str(row.get("signal_id") or "")
        if not signal_id or signal_id in output:
            continue
        session_date = str(row.get("session_date") or "")[:10]
        market = str(row.get("market") or "")
        row_key = _signal_row_key(row)
        candidates: list[Path] = []
        direct_path = _as_path(state_dir, row.get("signal_source_path"))
        if direct_path:
            candidates.append(direct_path)
        candidate_roots = []
        candidate_roots.extend(market_roots.get(market, []))
        candidate_roots.extend(fallback_roots)
        for candidate_root in candidate_roots:
            if session_date:
                candidates.append(
                    candidate_root / session_date / signal_id / "summary.json"
                )
        for candidate in candidates:
            if not candidate.is_file() or candidate.name != "summary.json":
                continue
            drivers = _read_signal_feature_drivers(candidate)
            if drivers:
                record = {"drivers": drivers}
                output[signal_id] = record
                if row_key:
                    output.setdefault(row_key, record)
                break
    return output


def _tail(path: Path, maximum_rows: int) -> list[dict[str, Any]]:
    if maximum_rows <= 0 or not path.is_file():
        return []
    stat = path.stat()
    cache_key = (path.resolve(), int(maximum_rows))
    with _TAIL_CACHE_LOCK:
        cached = _TAIL_CACHE.get(cache_key)
        if cached and cached[:4] == (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
        ):
            return list(cached[4])
    rows: deque[dict[str, Any]] = deque(maxlen=maximum_rows)
    with path.open("rb") as handle:
        cursor = handle.seek(0, 2)
        chunks: list[bytes] = []
        newline_count = 0
        while cursor > 0 and newline_count <= maximum_rows:
            chunk_size = min(1 << 20, cursor)
            cursor -= chunk_size
            handle.seek(cursor)
            chunk = handle.read(chunk_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    encoded = b"".join(reversed(chunks))
    for line in encoded.splitlines()[-maximum_rows:]:
        if not line.strip():
            continue
        payload = json.loads(line.decode("utf-8"))
        if isinstance(payload, dict):
            rows.append(payload)
    result = list(rows)
    final_stat = path.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) == (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns):
        with _TAIL_CACHE_LOCK:
            if len(_TAIL_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
                _TAIL_CACHE.pop(next(iter(_TAIL_CACHE)))
            _TAIL_CACHE[cache_key] = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                result,
            )
    return list(result)


def _ledger_row_session_date(
    row: Mapping[str, Any], *, recorded_at_fallback: bool
) -> str:
    explicit = str(row.get("session_date") or "")[:10]
    if explicit:
        try:
            datetime_date.fromisoformat(explicit)
        except ValueError:
            return ""
        return explicit
    fallback_timestamp = row.get("recorded_at") or row.get("minute")
    if not recorded_at_fallback or not fallback_timestamp:
        return ""
    try:
        return _timestamp(fallback_timestamp).astimezone(TAIPEI).date().isoformat()
    except (TypeError, ValueError):
        return ""


def _ledger_line_session_date(line: bytes, *, recorded_at_fallback: bool) -> str:
    """Extract the index key without decoding a complete, often wide row.

    Every canonical execution ledger has a top-level ISO ``session_date``.
    Regex extraction makes index construction proportional to bytes copied,
    rather than to allocation of every nested JSON field.  If nested and
    top-level values ever disagree, or the field is absent, fall back to the
    strict JSON path so unusual/legacy rows retain the original semantics.
    Requested spans are always decoded and validated again before publication.
    """

    matches = {
        match.group(1).decode("ascii")
        for match in _SESSION_DATE_FIELD_PATTERN.finditer(line)
    }
    if len(matches) == 1:
        session_date = next(iter(matches))
        try:
            datetime_date.fromisoformat(session_date)
        except ValueError:
            pass
        else:
            return session_date
    payload = json.loads(line)
    if not isinstance(payload, Mapping):
        return ""
    return _ledger_row_session_date(payload, recorded_at_fallback=recorded_at_fallback)


def _ledger_session_index(
    path: Path, *, recorded_at_fallback: bool = True
) -> _LedgerSessionIndex | None:
    """Incrementally index append-only ledger byte spans by session date.

    Dashboard date discovery needs only a few dates and byte offsets. Keeping
    tens of thousands of decoded signal dictionaries merely to learn those
    dates amplified a 105 MiB ledger into hundreds of MiB of resident Python
    objects. This index parses each complete line once, retains only compact
    contiguous byte spans, and scans only appended bytes on later calls.
    """

    source = Path(path)
    if not source.is_file():
        return None
    cache_key = (source.resolve(), bool(recorded_at_fallback))
    with _LEDGER_SESSION_INDEX_LOCK:
        for _attempt in range(3):
            stat = source.stat()
            cached = _LEDGER_SESSION_INDEX_CACHE.get(cache_key)
            if cached is None:
                cached = _load_persistent_ledger_index(
                    source,
                    recorded_at_fallback=recorded_at_fallback,
                    stat=stat,
                )
                if cached is not None:
                    _LEDGER_SESSION_INDEX_CACHE[cache_key] = cached
            can_extend = bool(
                cached is not None
                and (cached.device, cached.inode) == (stat.st_dev, stat.st_ino)
                and stat.st_size >= cached.observed_size
                and not (
                    stat.st_size == cached.observed_size
                    and stat.st_mtime_ns != cached.modified_ns
                )
            )
            if (
                can_extend
                and cached is not None
                and stat.st_size == cached.observed_size
                and stat.st_mtime_ns == cached.modified_ns
            ):
                return cached

            if can_extend and cached is not None:
                scanned_offset = cached.scanned_offset
                spans = {key: list(value) for key, value in cached.spans.items()}
            else:
                scanned_offset = 0
                spans = {}

            with source.open("rb") as handle:
                handle.seek(scanned_offset)
                cursor = scanned_offset
                while cursor < stat.st_size:
                    line_start = cursor
                    remaining = stat.st_size - cursor
                    line = handle.readline(min(_MAX_LEDGER_LINE_BYTES + 1, remaining))
                    cursor = handle.tell()
                    if not line.endswith(b"\n"):
                        if len(line) > _MAX_LEDGER_LINE_BYTES:
                            raise ValueError(
                                f"dashboard ledger line is too large: {source}"
                            )
                        cursor = line_start
                        break
                    if len(line) > _MAX_LEDGER_LINE_BYTES:
                        raise ValueError(
                            f"dashboard ledger line is too large: {source}"
                        )
                    if not line.strip():
                        scanned_offset = cursor
                        continue
                    session_date = _ledger_line_session_date(
                        line,
                        recorded_at_fallback=recorded_at_fallback,
                    )
                    if session_date:
                        date_spans = spans.setdefault(session_date, [])
                        if date_spans and date_spans[-1][1] == line_start:
                            date_spans[-1] = (date_spans[-1][0], cursor)
                        else:
                            date_spans.append((line_start, cursor))
                    scanned_offset = cursor

            final_stat = source.stat()
            if (
                final_stat.st_dev,
                final_stat.st_ino,
                final_stat.st_size,
                final_stat.st_mtime_ns,
            ) != (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns):
                continue
            result = _LedgerSessionIndex(
                device=stat.st_dev,
                inode=stat.st_ino,
                observed_size=stat.st_size,
                modified_ns=stat.st_mtime_ns,
                scanned_offset=scanned_offset,
                spans=spans,
            )
            _LEDGER_SESSION_INDEX_CACHE[cache_key] = result
            _persist_ledger_index(
                source,
                recorded_at_fallback=recorded_at_fallback,
                index=result,
            )
            return result
    raise OSError(f"dashboard ledger changed repeatedly while indexing: {source}")


def _rows_for_sessions(
    path: Path,
    session_dates: list[str] | tuple[str, ...],
    maximum_rows: int | None,
    *,
    recorded_at_fallback: bool = False,
    projected_schema: Mapping[str, str] | None = None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Read only byte spans belonging to the requested retained sessions."""

    selected_dates = tuple(
        dict.fromkeys(str(value) for value in session_dates if value)
    )
    if (maximum_rows is not None and maximum_rows <= 0) or not selected_dates:
        return {}
    index = _ledger_session_index(
        path,
        recorded_at_fallback=recorded_at_fallback,
    )
    if index is None:
        return {}
    selected_spans = sorted(
        (start, end, session_date)
        for session_date in selected_dates
        for start, end in index.spans.get(session_date, ())
    )
    source = Path(path)
    stat = source.stat()
    if (stat.st_dev, stat.st_ino) != (index.device, index.inode):
        raise OSError(f"dashboard ledger changed before indexed read: {source}")
    columnar = _columnar_ledger_frame(
        source,
        selected_spans=selected_spans,
        maximum_rows=maximum_rows,
        projected_schema=projected_schema,
    )
    if columnar is not None:
        _, frame = columnar
        grouped: dict[str, list[dict[str, Any]]] = {}
        for payload in frame.to_dicts():
            session_date = _ledger_row_session_date(
                payload,
                recorded_at_fallback=recorded_at_fallback,
            )
            if session_date in selected_dates:
                grouped.setdefault(session_date, []).append(payload)
        return {key: tuple(value) for key, value in grouped.items()}

    # The contract retains only the last maximum_rows matching records. Reading
    # and decoding years of discarded prefixes is unnecessary (3 GiB in the
    # live signal ledger). Walk indexed spans backwards and stop at that exact
    # bound, then restore the original source order. Unlimited readers retain
    # their full scan and validation path below.
    if maximum_rows is not None:
        newest_first: list[tuple[str, dict[str, Any]]] = []
        with source.open("rb") as handle:
            for start, end, indexed_date in reversed(selected_spans):
                handle.seek(end - 1)
                if handle.read(1) != b"\n":
                    raise ValueError(f"dashboard ledger span is invalid: {source}")
                cursor, remainder = end, b""
                while cursor > start and len(newest_first) < maximum_rows:
                    size = min(1 << 20, cursor - start)
                    cursor -= size
                    handle.seek(cursor)
                    chunk = handle.read(size)
                    if len(chunk) != size:
                        raise ValueError(f"dashboard ledger span is invalid: {source}")
                    lines = (chunk + remainder).split(b"\n")
                    remainder = lines[0] if cursor > start else b""
                    complete = lines[1:] if cursor > start else lines
                    if len(remainder) > _MAX_LEDGER_LINE_BYTES:
                        raise ValueError(
                            f"dashboard ledger line is too large: {source}"
                        )
                    for line in reversed(complete):
                        if not line.strip():
                            continue
                        if len(line) > _MAX_LEDGER_LINE_BYTES:
                            raise ValueError(
                                f"dashboard ledger line is too large: {source}"
                            )
                        payload = json.loads(line)
                        if not isinstance(payload, dict):
                            continue
                        row_date = _ledger_row_session_date(
                            payload, recorded_at_fallback=recorded_at_fallback
                        )
                        if row_date == indexed_date:
                            newest_first.append((row_date, payload))
                            if len(newest_first) >= maximum_rows:
                                break
                if len(newest_first) >= maximum_rows:
                    break
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row_date, payload in reversed(newest_first):
            grouped.setdefault(row_date, []).append(payload)
        return {key: tuple(value) for key, value in grouped.items()}

    retained: deque[tuple[str, dict[str, Any]]] | list[tuple[str, dict[str, Any]]] = (
        deque(maxlen=maximum_rows) if maximum_rows is not None else []
    )
    with source.open("rb") as handle:
        for start, end, indexed_date in selected_spans:
            handle.seek(start)
            while handle.tell() < end:
                remaining = end - handle.tell()
                line = handle.readline(min(_MAX_LEDGER_LINE_BYTES + 1, remaining))
                if not line.endswith(b"\n") or len(line) > _MAX_LEDGER_LINE_BYTES:
                    raise ValueError(f"dashboard ledger span is invalid: {source}")
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    continue
                session_date = _ledger_row_session_date(
                    payload,
                    recorded_at_fallback=recorded_at_fallback,
                )
                if session_date == indexed_date:
                    retained.append((session_date, payload))
    grouped: dict[str, list[dict[str, Any]]] = {}
    for session_date, row in retained:
        grouped.setdefault(session_date, []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _write_recent_ledger_lines(
    source: Path,
    *,
    selected_spans: list[tuple[int, int, str]],
    maximum_rows: int,
    target: Any,
) -> int:
    """Copy the newest selected physical rows without decoding their payloads.

    The signal ledger is several GiB because each row retains model evidence.
    A public page needs at most ``maximum_rows`` and only a small field
    projection.  Materializing 100,000 full Python dictionaries expanded that
    bounded slice to roughly 2 GiB.  Copying raw lines newest-first into an
    anonymous temporary file lets the native JSON reader project before any
    Python objects exist while preserving the exact append-only source bytes.
    """

    if maximum_rows <= 0:
        return 0
    copied = 0
    pending: list[bytes] = []
    pending_bytes = 0

    def flush() -> None:
        nonlocal pending_bytes
        if pending:
            target.writelines(pending)
            pending.clear()
            pending_bytes = 0

    with Path(source).open("rb") as handle:
        for start, end, _indexed_date in reversed(selected_spans):
            handle.seek(end - 1)
            if handle.read(1) != b"\n":
                raise ValueError(f"dashboard ledger span is invalid: {source}")
            cursor, remainder = end, b""
            while cursor > start and copied < maximum_rows:
                size = min(1 << 20, cursor - start)
                cursor -= size
                handle.seek(cursor)
                chunk = handle.read(size)
                if len(chunk) != size:
                    raise ValueError(f"dashboard ledger span is invalid: {source}")
                lines = (chunk + remainder).split(b"\n")
                remainder = lines[0] if cursor > start else b""
                complete = lines[1:] if cursor > start else lines
                if len(remainder) > _MAX_LEDGER_LINE_BYTES:
                    raise ValueError(f"dashboard ledger line is too large: {source}")
                for line in reversed(complete):
                    if not line.strip():
                        continue
                    if len(line) > _MAX_LEDGER_LINE_BYTES:
                        raise ValueError(
                            f"dashboard ledger line is too large: {source}"
                        )
                    encoded = line + b"\n"
                    pending.append(encoded)
                    pending_bytes += len(encoded)
                    copied += 1
                    if pending_bytes >= _COLUMNAR_LEDGER_WRITE_BUFFER_BYTES:
                        flush()
                    if copied >= maximum_rows:
                        break
            if copied >= maximum_rows:
                break
    flush()
    target.flush()
    target.seek(0)
    return copied


def _columnar_ledger_frame(
    source: Path,
    *,
    selected_spans: list[tuple[int, int, str]],
    maximum_rows: int | None,
    projected_schema: Mapping[str, str] | None = None,
) -> tuple[Any, Any] | None:
    """Decode sufficiently large indexed NDJSON spans into a columnar frame.

    Date filtering first resolves immutable byte spans from the append-only
    ledger index.  Decoding those spans one JSON object at a time dominates a
    cold filter request and temporarily expands every field into Python
    objects.  Polars parses the same bounded bytes in native code and lets
    callers project/filter before materializing Python rows.  Small fixtures,
    unavailable optional runtimes, and unusual schemas retain the strict
    stdlib path below, so this is an optimization rather than a new storage
    authority.
    """

    total_bytes = sum(end - start for start, end, _ in selected_spans)
    if total_bytes < _COLUMNAR_LEDGER_MIN_BYTES:
        return None
    needs_bounded_tail = total_bytes > _COLUMNAR_LEDGER_MAX_BYTES
    if needs_bounded_tail and maximum_rows is None:
        return None
    try:
        import polars as pl
    except ImportError:
        return None

    schema_types = {
        "bool": pl.Boolean,
        "float": pl.Float64,
        "int": pl.Int64,
        "string": pl.String,
    }
    schema = (
        {field: schema_types[kind] for field, kind in projected_schema.items()}
        if projected_schema is not None
        else None
    )

    def read_frame(source_stream: Any) -> Any:
        frame = pl.read_ndjson(
            source_stream,
            schema=schema,
            infer_schema_length=None if schema is not None else 1_000,
            batch_size=4_096,
            low_memory=schema is not None,
            rechunk=False,
        )
        if schema is not None and frame.height:
            null_counts = frame.null_count().row(0)
            frame = frame.drop(
                column
                for column, null_count in zip(frame.columns, null_counts, strict=True)
                if null_count == frame.height
            )
        return frame

    try:
        if needs_bounded_tail:
            cache_root = str(
                os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or ""
            ).strip()
            temporary_root = Path(cache_root) if cache_root else None
            with tempfile.TemporaryFile(dir=temporary_root) as temporary:
                _write_recent_ledger_lines(
                    source,
                    selected_spans=selected_spans,
                    maximum_rows=int(maximum_rows or 0),
                    target=temporary,
                )
                # The temporary stream is newest-first. Restore append order so
                # callers retain the same latest-signal overwrite semantics as
                # the strict JSON implementation.
                frame = read_frame(temporary).reverse()
        else:
            cursor = 0
            covers_complete_file = bool(selected_spans)
            for start, end, _ in selected_spans:
                if start != cursor:
                    covers_complete_file = False
                    break
                cursor = end
            covers_complete_file &= cursor == Path(source).stat().st_size
            if covers_complete_file:
                frame = read_frame(source)
            else:
                chunks: list[bytes] = []
                with Path(source).open("rb") as handle:
                    for start, end, _ in selected_spans:
                        handle.seek(start)
                        chunk = handle.read(end - start)
                        if len(chunk) != end - start or (
                            chunk and not chunk.endswith(b"\n")
                        ):
                            raise ValueError(
                                f"dashboard ledger span is invalid: {source}"
                            )
                        chunks.append(chunk)
                payload = chunks[0] if len(chunks) == 1 else b"".join(chunks)
                frame = read_frame(io.BytesIO(payload))
    except Exception:
        # Mixed legacy rows can defeat bounded schema inference.  The existing
        # line-by-line decoder remains the correctness fallback and will still
        # surface malformed JSON instead of silently dropping it.
        return None
    if maximum_rows is not None and frame.height > maximum_rows:
        frame = frame.tail(maximum_rows)
    return pl, frame


@contextmanager
def _projected_recent_ledger_batches(
    source: Path,
    *,
    selected_spans: list[tuple[int, int, str]],
    maximum_rows: int,
    projected_schema: Mapping[str, str],
):
    """Yield projected newest-first batches through a bounded Arrow stream.

    Unlike a DataFrame conversion, batches are discarded as soon as their rows
    have updated the caller's counters and bounded page heap.  This keeps a
    wide event archive from becoming retained allocator memory in the public
    gateway after each date-filter interaction.
    """

    total_bytes = sum(end - start for start, end, _ in selected_spans)
    if total_bytes < _COLUMNAR_LEDGER_MIN_BYTES:
        yield None
        return
    try:
        import pyarrow as pa
        import pyarrow.json as paj
    except ImportError:
        yield None
        return
    arrow_types = {
        "bool": pa.bool_(),
        "float": pa.float64(),
        "int": pa.int64(),
        "string": pa.string(),
    }
    schema = pa.schema(
        [pa.field(field, arrow_types[kind]) for field, kind in projected_schema.items()]
    )
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    temporary_root = Path(cache_root) if cache_root else None
    with tempfile.TemporaryFile(dir=temporary_root) as temporary:
        _write_recent_ledger_lines(
            source,
            selected_spans=selected_spans,
            maximum_rows=maximum_rows,
            target=temporary,
        )
        with paj.open_json(
            temporary,
            read_options=paj.ReadOptions(block_size=4 << 20, use_threads=True),
            parse_options=paj.ParseOptions(
                explicit_schema=schema,
                unexpected_field_behavior="ignore",
            ),
        ) as reader:

            yield iter(reader)


def _bounded_parquet_history_rows(
    path: Path,
    *,
    session_dates: list[str] | tuple[str, ...],
    maximum_rows: int,
) -> tuple[list[dict[str, Any]], int, tuple[int, int, int, int] | None]:
    """Read a bounded newest slice from an immutable historical detail table."""

    source = Path(path)
    if not source.is_file() or not session_dates or maximum_rows <= 0:
        return [], 0, None
    stat = source.stat()
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover - production runtime requires Polars
        raise RuntimeError(
            "Polars is required for historical dashboard tables"
        ) from exc
    selected = [str(value) for value in session_dates]
    lazy = pl.scan_parquet(source).filter(pl.col("session_date").is_in(selected))
    total = int(lazy.select(pl.len().alias("rows")).collect().item())
    if total == 0:
        return [], 0, signature
    frame = (
        lazy.sort(["session_date", "market", "symbol"])
        .tail(int(maximum_rows))
        .collect()
    )
    final_stat = source.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) != signature:
        raise OSError(f"historical dashboard table changed while reading: {source}")
    return frame.to_dicts(), total, signature


def _tail_for_session(
    path: Path,
    maximum_rows: int,
    session_date: str,
    *,
    recorded_at_fallback: bool = False,
) -> list[dict[str, Any]]:
    """Return the newest matching rows without letting later days crowd them out."""

    if maximum_rows <= 0 or not path.is_file():
        return []
    stat = path.stat()
    cache_key = (
        path.resolve(),
        int(maximum_rows),
        str(session_date),
        bool(recorded_at_fallback),
    )
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    with _SESSION_TAIL_CACHE_LOCK:
        cached = _SESSION_TAIL_CACHE.get(cache_key)
        if cached and cached[:4] == signature:
            return list(cached[4])

    index_cache_key = (path.resolve(), bool(recorded_at_fallback))
    persistent_index_path = _persistent_ledger_index_path(
        path, recorded_at_fallback=recorded_at_fallback
    )
    if (
        index_cache_key in _LEDGER_SESSION_INDEX_CACHE
        or persistent_index_path is not None
        and persistent_index_path.is_file()
    ):
        indexed = list(
            _rows_for_sessions(
                path,
                [session_date],
                maximum_rows,
                recorded_at_fallback=recorded_at_fallback,
            ).get(session_date, ())
        )
        final_stat = path.stat()
        if (
            final_stat.st_dev,
            final_stat.st_ino,
            final_stat.st_size,
            final_stat.st_mtime_ns,
        ) == signature:
            with _SESSION_TAIL_CACHE_LOCK:
                if len(_SESSION_TAIL_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
                    _SESSION_TAIL_CACHE.pop(next(iter(_SESSION_TAIL_CACHE)))
                _SESSION_TAIL_CACHE[cache_key] = (*signature, indexed)
        return indexed

    newest_first: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        cursor = handle.seek(0, 2)
        remainder = b""
        while cursor > 0 and len(newest_first) < maximum_rows:
            chunk_size = min(1 << 20, cursor)
            cursor -= chunk_size
            handle.seek(cursor)
            data = handle.read(chunk_size) + remainder
            lines = data.split(b"\n")
            remainder = lines[0]
            for line in reversed(lines[1:]):
                if not line.strip():
                    continue
                payload = json.loads(line.decode("utf-8"))
                if not isinstance(payload, dict):
                    continue
                explicit = str(payload.get("session_date") or "")
                matches = explicit == session_date
                if recorded_at_fallback and not explicit:
                    matches = _is_taipei_session_date(
                        payload.get("recorded_at"), session_date
                    )
                if matches:
                    newest_first.append(payload)
                    if len(newest_first) >= maximum_rows:
                        break
        if cursor == 0 and remainder.strip() and len(newest_first) < maximum_rows:
            payload = json.loads(remainder.decode("utf-8"))
            if isinstance(payload, dict):
                explicit = str(payload.get("session_date") or "")
                matches = explicit == session_date
                if recorded_at_fallback and not explicit:
                    matches = _is_taipei_session_date(
                        payload.get("recorded_at"), session_date
                    )
                if matches:
                    newest_first.append(payload)
    result = list(reversed(newest_first))
    final_stat = path.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) == signature:
        with _SESSION_TAIL_CACHE_LOCK:
            if len(_SESSION_TAIL_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
                _SESSION_TAIL_CACHE.pop(next(iter(_SESSION_TAIL_CACHE)))
            _SESSION_TAIL_CACHE[cache_key] = (*signature, result)
    return list(result)


def _latest_contiguous_session_rows(
    path: Path,
    session_date: str,
    maximum_rows: int,
    *,
    recorded_at_fallback: bool = False,
) -> list[dict[str, Any]]:
    """Read the newest session block without indexing the complete ledger.

    The active session is append-only and occupies the tail of each canonical
    ledger.  On a cold dashboard process, indexing a multi-GiB signal history
    before reading that tail adds seconds without changing the answer.  This
    fast path is used only when the requested date is also present in current
    engine state; arbitrary historical dates continue through the full byte-
    span index.
    """

    if maximum_rows <= 0 or not path.is_file():
        return []
    stat = path.stat()
    cache_key = (
        path.resolve(),
        int(maximum_rows),
        str(session_date),
        bool(recorded_at_fallback),
    )
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    with _LATEST_SESSION_BLOCK_CACHE_LOCK:
        cached = _LATEST_SESSION_BLOCK_CACHE.get(cache_key)
        if cached and cached[:4] == signature:
            return list(cached[4])

    newest_first: list[dict[str, Any]] = []
    found = False
    finished = False
    with path.open("rb") as handle:
        cursor = handle.seek(0, 2)
        remainder = b""
        while cursor > 0 and not finished and len(newest_first) < maximum_rows:
            chunk_size = min(1 << 20, cursor)
            cursor -= chunk_size
            handle.seek(cursor)
            data = handle.read(chunk_size) + remainder
            lines = data.split(b"\n")
            remainder = lines[0]
            for line in reversed(lines[1:]):
                if not line.strip():
                    continue
                if len(line) > _MAX_LEDGER_LINE_BYTES:
                    raise ValueError(f"dashboard ledger line is too large: {path}")
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    continue
                row_date = _ledger_row_session_date(
                    payload,
                    recorded_at_fallback=recorded_at_fallback,
                )
                if row_date == session_date:
                    found = True
                    newest_first.append(payload)
                    if len(newest_first) >= maximum_rows:
                        break
                elif found:
                    finished = True
                    break
        if (
            cursor == 0
            and not finished
            and remainder.strip()
            and len(newest_first) < maximum_rows
        ):
            payload = json.loads(remainder)
            if isinstance(payload, dict):
                row_date = _ledger_row_session_date(
                    payload,
                    recorded_at_fallback=recorded_at_fallback,
                )
                if row_date == session_date:
                    newest_first.append(payload)

    result = list(reversed(newest_first))
    final_stat = path.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) == signature:
        with _LATEST_SESSION_BLOCK_CACHE_LOCK:
            if len(_LATEST_SESSION_BLOCK_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
                _LATEST_SESSION_BLOCK_CACHE.pop(next(iter(_LATEST_SESSION_BLOCK_CACHE)))
            _LATEST_SESSION_BLOCK_CACHE[cache_key] = (*signature, result)
    return list(result)


def _percentile(values: list[float], quantile: float) -> float | None:
    finite = sorted(value for value in values if math.isfinite(value))
    if not finite:
        return None
    if len(finite) == 1:
        return round(finite[0], 3)
    position = (len(finite) - 1) * min(1.0, max(0.0, quantile))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    value = finite[lower] + (finite[upper] - finite[lower]) * (position - lower)
    return round(value, 3)


def _latency_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [row for row in rows if str(row.get("result") or "") == "registered"]
    values = [
        float(value)
        for row in successful
        if (value := _finite_float(row.get("input_to_ledger_ms"))) is not None
    ]
    latest = successful[-1] if successful else None
    stage_names = sorted(
        {str(name) for row in successful for name in (row.get("stages") or {})}
    )
    stage_p50_ms = {
        name: _percentile(
            [
                float(value)
                for row in successful
                if (value := _finite_float((row.get("stages") or {}).get(name)))
                is not None
            ],
            0.5,
        )
        for name in stage_names
    }
    bottleneck_counts: dict[str, int] = {}
    for row in successful:
        name = str(row.get("bottleneck_stage") or "")
        if name:
            bottleneck_counts[name] = bottleneck_counts.get(name, 0) + 1
    dominant_bottleneck = (
        max(bottleneck_counts, key=lambda name: (bottleneck_counts[name], name))
        if bottleneck_counts
        else None
    )
    return {
        "schema_version": 1,
        "measurement_boundary": "signal_input_to_simulation_ledger_persisted",
        "sample_count": len(values),
        "terminal_sample_count": len(rows),
        "registered_sample_count": len(successful),
        "latest_ms": round(values[-1], 3) if values else None,
        "p50_ms": _percentile(values, 0.5),
        "p95_ms": _percentile(values, 0.95),
        "max_ms": round(max(values), 3) if values else None,
        "latest_market": latest.get("market") if latest else None,
        "latest_recorded_at": latest.get("recorded_at") if latest else None,
        "latest_ready_to_ledger_ms": latest.get("ready_to_ledger_ms")
        if latest
        else None,
        "latest_bottleneck_stage": latest.get("bottleneck_stage") if latest else None,
        "latest_bottleneck_ms": latest.get("bottleneck_ms") if latest else None,
        "dominant_bottleneck_stage": dominant_bottleneck,
        "stage_p50_ms": stage_p50_ms,
        "simulation_only": True,
        "not_external_order_or_venue_rtt": True,
    }


def _opening_signal_latency_summary(
    rows: list[dict[str, Any]],
    *,
    expected_markets: list[str],
    session_date: str,
) -> dict[str, Any]:
    """Summarize source arrival and controllable 09:00 signal stages by day."""

    goal_ms = 1_000.0
    expected = sorted({str(value) for value in expected_markets if str(value)})
    by_session: dict[str, dict[str, dict[str, Any]]] = {}
    failures_by_session: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        market = str(row.get("market") or "")
        if expected and market not in expected:
            continue
        is_attempt = str(row.get("source") or "") == "discord_scheduled_signal"
        status = str(row.get("status") or "")
        if is_attempt:
            row_session = str(row.get("session_date") or "")[:10]
            if status != "ready":
                if row_session:
                    failures_by_session.setdefault(row_session, []).append(
                        {
                            "market": market,
                            "recorded_at": row.get("recorded_at"),
                            "error_type": row.get("error_type"),
                            "error_code": row.get("error_code"),
                        }
                    )
                continue
        elif str(row.get("result") or "") != "registered":
            continue

        try:
            started = _timestamp(row.get("signal_started_at"))
            ready = _timestamp(row.get("signal_ready_at"))
        except (TypeError, ValueError):
            continue
        local_started = started.astimezone(TAIPEI)
        row_session = str(row.get("session_date") or local_started.date().isoformat())
        try:
            gate = (
                _timestamp(row.get("gate_at"))
                if is_attempt
                else datetime.fromisoformat(f"{row_session}T09:00:00+08:00").astimezone(
                    timezone.utc
                )
            )
        except (TypeError, ValueError):
            continue
        queue_ms = (started - gate).total_seconds() * 1000.0
        measured_ready_ms = _finite_float(row.get("ready_from_open_ms"))
        ready_ms = (
            measured_ready_ms
            if is_attempt and measured_ready_ms is not None
            else (ready - gate).total_seconds() * 1000.0
        )
        # Exclude later manual/recovery runs from opening trend while retaining
        # a five-minute window for explicitly recorded scheduled retries.
        if queue_ms < -1_000.0 or queue_ms > 300_000.0 or ready_ms < 0.0:
            continue
        raw_stages = row.get("stages") or {}
        stages = {
            str(name): round(float(value), 3)
            for name, raw_value in raw_stages.items()
            if (value := _finite_float(raw_value)) is not None and value >= 0.0
        }
        bottleneck_stage = (
            max(stages, key=lambda name: (stages[name], name)) if stages else None
        )
        receipt = row.get("price_receipt_timing") or {}
        source_ready_ms = _finite_float(row.get("source_ready_from_open_ms"))
        if source_ready_ms is None and isinstance(receipt, Mapping):
            source_ready_ms = _finite_float(
                receipt.get("coverage_receipt_from_open_ms")
            )
        source_to_signal_ms = _finite_float(row.get("source_ready_to_signal_ms"))
        if source_to_signal_ms is None and isinstance(receipt, Mapping):
            source_to_signal_ms = _finite_float(
                receipt.get("coverage_to_signal_ready_ms")
            )
        published_ms = _finite_float(row.get("published_from_open_ms"))
        candidate = {
            "market": market,
            "signal_id": row.get("signal_id"),
            "started_at": row.get("signal_started_at"),
            "ready_at": row.get("signal_ready_at"),
            "published_at": row.get("artifact_published_at"),
            "consumer_detected_at": row.get("consumer_detected_at"),
            "ledger_persisted_at": row.get("ledger_persisted_at"),
            "queue_ms": round(queue_ms, 3),
            "scheduler_wake_ms": stages.get("scheduler_wake_ms"),
            "compute_ms": round(
                max(0.0, (ready - started).total_seconds() * 1000.0), 3
            ),
            "ready_from_0900_ms": round(ready_ms, 3),
            "published_from_0900_ms": published_ms,
            "input_to_ledger_ms": _finite_float(row.get("input_to_ledger_ms")),
            "ready_to_ledger_ms": _finite_float(row.get("ready_to_ledger_ms")),
            "source_ready_from_0900_ms": source_ready_ms,
            "source_ready_to_signal_ms": source_to_signal_ms,
            "stages": stages,
            "bottleneck_stage": bottleneck_stage,
            "bottleneck_ms": stages.get(bottleneck_stage) if bottleneck_stage else None,
            "price_receipt_timing": receipt if isinstance(receipt, Mapping) else {},
            "quote_transport": row.get("quote_transport")
            if isinstance(row.get("quote_transport"), Mapping)
            else {},
            "previous_signal_history_disabled": row.get(
                "previous_signal_history_disabled"
            ),
            "telemetry_source": "opening_attempt_v2"
            if is_attempt
            else "executor_latency_v1",
        }
        existing = by_session.setdefault(row_session, {}).get(market)
        if (
            existing is not None
            and existing.get("signal_id")
            and existing.get("signal_id") == candidate.get("signal_id")
        ):
            merged_stages = {
                **(existing.get("stages") or {}),
                **candidate["stages"],
            }
            existing["stages"] = merged_stages
            merged_bottleneck = (
                max(
                    merged_stages,
                    key=lambda name: (merged_stages[name], name),
                )
                if merged_stages
                else None
            )
            existing["bottleneck_stage"] = merged_bottleneck
            existing["bottleneck_ms"] = (
                merged_stages.get(merged_bottleneck) if merged_bottleneck else None
            )
            for name in (
                "published_at",
                "consumer_detected_at",
                "ledger_persisted_at",
                "published_from_0900_ms",
                "input_to_ledger_ms",
                "ready_to_ledger_ms",
                "source_ready_from_0900_ms",
                "source_ready_to_signal_ms",
            ):
                if existing.get(name) is None and candidate.get(name) is not None:
                    existing[name] = candidate[name]
            if not existing.get("price_receipt_timing"):
                existing["price_receipt_timing"] = candidate["price_receipt_timing"]
            if not existing.get("quote_transport"):
                existing["quote_transport"] = candidate["quote_transport"]
            if is_attempt:
                existing["previous_signal_history_disabled"] = candidate[
                    "previous_signal_history_disabled"
                ]
            existing["telemetry_source"] = "opening_attempt_v2+executor_latency_v1"
            continue
        if existing is None or float(candidate["ready_from_0900_ms"]) < float(
            existing["ready_from_0900_ms"]
        ):
            by_session[row_session][market] = candidate

    trend: list[dict[str, Any]] = []
    all_sessions = sorted(set(by_session) | set(failures_by_session))
    for day in all_sessions:
        modes_by_market = by_session.get(day, {})
        mode_rows = [modes_by_market[key] for key in sorted(modes_by_market)]
        ready_values = [float(row["ready_from_0900_ms"]) for row in mode_rows]
        source_values = [
            float(value)
            for row in mode_rows
            if (value := _finite_float(row.get("source_ready_from_0900_ms")))
            is not None
        ]
        controllable_values = [
            float(value)
            for row in mode_rows
            if (value := _finite_float(row.get("source_ready_to_signal_ms")))
            is not None
        ]
        observed = sorted(modes_by_market)
        first_ready_ms = round(min(ready_values), 3) if ready_values else None
        final_ready_ms = round(max(ready_values), 3) if ready_values else None
        complete = bool(expected) and observed == expected
        trend.append(
            {
                "session_date": day,
                "observed_mode_count": len(observed),
                "observed_markets": observed,
                "expected_mode_count": len(expected),
                "complete": complete,
                "missing_markets": sorted(set(expected) - set(observed)),
                "failure_count": len(failures_by_session.get(day, ())),
                "failures": failures_by_session.get(day, ()),
                "first_ready_ms": first_ready_ms,
                "final_ready_ms": final_ready_ms,
                "first_source_ready_ms": round(min(source_values), 3)
                if source_values
                else None,
                "final_source_ready_ms": round(max(source_values), 3)
                if source_values
                else None,
                "source_ready_to_signal_p50_ms": _percentile(controllable_values, 0.5),
                "source_ready_to_signal_max_ms": round(max(controllable_values), 3)
                if controllable_values
                else None,
                "first_signal_goal_met": first_ready_ms is not None
                and first_ready_ms <= goal_ms,
                "all_modes_goal_met": complete
                and final_ready_ms is not None
                and final_ready_ms <= goal_ms,
                "modes": mode_rows,
            }
        )
    current = next(
        (dict(row) for row in reversed(trend) if row["session_date"] == session_date),
        {
            "session_date": session_date,
            "observed_mode_count": 0,
            "observed_markets": [],
            "expected_mode_count": len(expected),
            "complete": False,
            "missing_markets": expected,
            "failure_count": 0,
            "failures": [],
            "first_ready_ms": None,
            "final_ready_ms": None,
            "first_source_ready_ms": None,
            "final_source_ready_ms": None,
            "source_ready_to_signal_p50_ms": None,
            "source_ready_to_signal_max_ms": None,
            "first_signal_goal_met": False,
            "all_modes_goal_met": False,
            "modes": [],
        },
    )
    current_markets = current.get("observed_markets") or []
    previous = next(
        (
            row
            for row in reversed(trend)
            if row["session_date"] < session_date
            and row.get("final_ready_ms") is not None
            # Aggregate latency is comparable only when the same modes ran.
            # Otherwise a faster value can merely mean fewer models existed.
            and (row.get("observed_markets") or []) == current_markets
        ),
        None,
    )
    current_final = _finite_float(current.get("final_ready_ms"))
    previous_final = _finite_float((previous or {}).get("final_ready_ms"))
    change_ms = (
        round(current_final - previous_final, 3)
        if current_final is not None and previous_final is not None
        else None
    )
    current.update(
        {
            "schema_version": 2,
            "measurement_boundary": "09:00_gate_to_local_quote_coverage_to_immutable_signal_ready",
            "goal_ms": goal_ms,
            "slo_ms": goal_ms,
            "slo_met": bool(current.get("all_modes_goal_met")),
            "previous_session_date": (previous or {}).get("session_date"),
            "previous_final_ready_ms": previous_final,
            "change_vs_previous_ms": change_ms,
            "improved_vs_previous": change_ms is not None and change_ms < 0.0,
            "trend": trend[-30:],
            "metric_definitions": {
                "total": "09:00 market gate to immutable signal-ready timestamp",
                "source": "09:00 market gate to required local callback-receipt coverage",
                "controllable": "required local quote coverage to immutable signal ready",
                "clock": "timezone-aware wall clock across processes; monotonic clock within each process stage",
            },
            "simulation_only": True,
            "not_external_order_or_venue_rtt": True,
        }
    )
    return current


def _line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    stat = path.stat()
    key = path.resolve()
    with _LINE_COUNT_LOCK:
        cached = _LINE_COUNT_CACHE.get(key)
        if cached is None:
            cache_root = str(
                os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or ""
            ).strip()
            persistent_path = None
            if cache_root and Path(cache_root).is_dir():
                digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
                persistent_path = (
                    Path(cache_root) / f"ledger-line-count-v1-{digest}.json"
                )
                try:
                    payload = json.loads(persistent_path.read_bytes())
                    if (
                        not isinstance(payload, Mapping)
                        or int(payload.get("schema_version") or 0) != 1
                    ):
                        raise ValueError("unsupported persistent line-count cache")
                    candidate = (
                        int(payload.get("device")),
                        int(payload.get("inode")),
                        int(payload.get("size")),
                        int(payload.get("modified_ns")),
                        int(payload.get("count")),
                    )
                    if (
                        str(payload.get("source") or "") == str(key)
                        and (candidate[0], candidate[1]) == (stat.st_dev, stat.st_ino)
                        and 0 <= candidate[2] <= stat.st_size
                        and 0 <= candidate[4]
                        and not (
                            candidate[2] == stat.st_size
                            and candidate[3] != stat.st_mtime_ns
                        )
                    ):
                        cached = candidate
                        _LINE_COUNT_CACHE[key] = candidate
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    pass
        else:
            persistent_path = None
        same_append_only_file = bool(
            cached
            and cached[0] == stat.st_dev
            and cached[1] == stat.st_ino
            and stat.st_size >= cached[2]
            and not (stat.st_size == cached[2] and stat.st_mtime_ns != cached[3])
        )
        start = cached[2] if same_append_only_file and cached else 0
        count = cached[4] if same_append_only_file and cached else 0
        with path.open("rb") as handle:
            handle.seek(start)
            remaining = stat.st_size - start
            while remaining > 0:
                chunk = handle.read(min(1 << 20, remaining))
                if not chunk:
                    break
                count += chunk.count(b"\n")
                remaining -= len(chunk)
        result = (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            count,
        )
        _LINE_COUNT_CACHE[key] = result
        if persistent_path is None:
            cache_root = str(
                os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or ""
            ).strip()
            if cache_root and Path(cache_root).is_dir():
                digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
                persistent_path = (
                    Path(cache_root) / f"ledger-line-count-v1-{digest}.json"
                )
        if persistent_path is not None:
            temporary = persistent_path.with_name(
                f".{persistent_path.name}.{os.getpid()}.tmp"
            )
            try:
                temporary.write_text(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "source": str(key),
                            "device": result[0],
                            "inode": result[1],
                            "size": result[2],
                            "modified_ns": result[3],
                            "count": result[4],
                        },
                        separators=(",", ":"),
                    )
                    + "\n",
                    encoding="utf-8",
                )
                os.chmod(temporary, 0o600)
                os.replace(temporary, persistent_path)
            except OSError:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
        return count


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _capital_return(
    initial_capital: object, total_equity: object
) -> tuple[float | None, float | None]:
    return capital_return(initial_capital, total_equity)


def _load_benchmark_history(root: Path) -> dict[str, Any]:
    """Load the immutable benchmark origin without risking dashboard uptime."""

    path = root / BENCHMARK_HISTORY_FILENAME
    if not path.is_file():
        return {}
    try:
        payload = _object(path)
    except (OSError, json.JSONDecodeError, ValueError):
        return {"load_error": "benchmark_history_unavailable"}
    if int(payload.get("schema_version") or 0) not in {1, 2}:
        return {"load_error": "benchmark_history_schema_unsupported"}
    return payload


def _persistent_benchmark_history_path(source: Path) -> Path | None:
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    if not cache_root:
        return None
    root = Path(cache_root)
    if not root.is_dir():
        return None
    digest = hashlib.sha256(str(source.resolve()).encode("utf-8")).hexdigest()
    return root / f"benchmark-history-index-v1-{digest}.json.gz"


def _persistent_history_projection_path(
    state_dir: Path, *, history_encoding: str
) -> Path | None:
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    if not cache_root:
        return None
    root = Path(cache_root)
    if not root.is_dir():
        return None
    digest = hashlib.sha256(str(Path(state_dir).resolve()).encode("utf-8")).hexdigest()
    encoding_version = {
        "minute_columns_v1": "v1",
        "minute_columns_v2": "v2",
    }.get(history_encoding)
    if encoding_version is None:
        return None
    return root / f"history-projection-all-1m-{encoding_version}-{digest}.json.gz"


def _history_projection_source_fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_persistent_history_projection(
    state_dir: Path,
    *,
    source_fingerprint: str,
    history_encoding: str,
) -> dict[str, Any] | None:
    cache_path = _persistent_history_projection_path(
        state_dir, history_encoding=history_encoding
    )
    if cache_path is None or not cache_path.is_file():
        return None
    try:
        if cache_path.stat().st_size > _HISTORY_PROJECTION_CACHE_MAX_COMPRESSED_BYTES:
            return None
        envelope = json.loads(gzip.decompress(cache_path.read_bytes()))
        if (
            not isinstance(envelope, Mapping)
            or int(envelope.get("cache_schema_version") or 0)
            != _HISTORY_PROJECTION_CACHE_SCHEMA_VERSION
            or envelope.get("source_fingerprint") != source_fingerprint
        ):
            return None
        snapshot = envelope.get("snapshot")
        if (
            not isinstance(snapshot, dict)
            or int(snapshot.get("schema_version") or 0) != DASHBOARD_SCHEMA_VERSION
            or snapshot.get("range") != "all"
            or snapshot.get("history_encoding") != history_encoding
            or snapshot.get("history") != []
            or not isinstance(snapshot.get("minute_series"), list)
        ):
            return None
        returned_points = int(snapshot.get("returned_points") or 0)
        point_field = (
            "points" if history_encoding == "minute_columns_v1" else "minute_indexes"
        )
        observed_points = sum(
            len(series.get(point_field) or ())
            for series in snapshot["minute_series"]
            if isinstance(series, Mapping)
        )
        if history_encoding == "minute_columns_v2" and not isinstance(
            snapshot.get("minute_axis"), list
        ):
            return None
        if returned_points < 0 or observed_points != returned_points:
            return None
        return snapshot
    except (
        EOFError,
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        gzip.BadGzipFile,
    ):
        return None


def _persist_history_projection(
    state_dir: Path,
    *,
    source_fingerprint: str,
    history_encoding: str,
    snapshot: Mapping[str, Any],
) -> None:
    cache_path = _persistent_history_projection_path(
        state_dir, history_encoding=history_encoding
    )
    if cache_path is None:
        return
    temporary = cache_path.with_name(
        f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        encoded = json.dumps(
            {
                "cache_schema_version": _HISTORY_PROJECTION_CACHE_SCHEMA_VERSION,
                "source_fingerprint": source_fingerprint,
                "snapshot": snapshot,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        compressed = gzip.compress(encoded, compresslevel=1)
        if len(compressed) > _HISTORY_PROJECTION_CACHE_MAX_COMPRESSED_BYTES:
            return
        with temporary.open("xb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, cache_path)
    except (OSError, TypeError, ValueError):
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _history_session_projection_root(state_dir: Path) -> Path | None:
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    if not cache_root:
        return None
    root = Path(cache_root)
    if not root.is_dir():
        return None
    digest = hashlib.sha256(str(Path(state_dir).resolve()).encode("utf-8")).hexdigest()
    return root / f"history-session-projection-v2-{digest}"


def _history_session_head(state_dir: Path) -> dict[str, Any] | None:
    root = _history_session_projection_root(state_dir)
    if root is None:
        return None
    path = root / "head.json"
    try:
        if not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
            return None
        payload = json.loads(path.read_bytes())
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if (
        not isinstance(payload, dict)
        or int(payload.get("schema_version") or 0)
        != _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION
        or not isinstance(payload.get("sessions"), Mapping)
        or not isinstance(payload.get("sources"), Mapping)
    ):
        return None
    return payload


def _atomic_private_bytes(path: Path, encoded: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _digest_ledger_sessions(
    path: Path,
    index: _LedgerSessionIndex | None,
    *,
    previous: Mapping[str, Any] | None,
) -> tuple[dict[str, str], dict[str, Any]]:
    if index is None:
        return {}, {"identity": None, "sessions": {}}
    identity = [index.device, index.inode, index.observed_size, index.modified_ns]
    previous_identity = previous.get("identity") if isinstance(previous, Mapping) else None
    previous_sessions = previous.get("sessions") if isinstance(previous, Mapping) else None
    previous_sessions = previous_sessions if isinstance(previous_sessions, Mapping) else {}
    output: dict[str, str] = {}
    metadata: dict[str, dict[str, Any]] = {}
    source = Path(path)
    with source.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (index.device, index.inode):
            raise OSError(f"dashboard ledger changed before session digest: {source}")
        for session_date in sorted(index.spans):
            spans = [list(span) for span in index.spans[session_date]]
            prior = previous_sessions.get(session_date)
            can_reuse = bool(
                isinstance(prior, Mapping)
                and previous_identity is not None
                and list(previous_identity[:2]) == identity[:2]
                and prior.get("spans") == spans
                and len(str(prior.get("sha256") or "")) == 64
            )
            if can_reuse:
                digest = str(prior["sha256"])
            else:
                checksum = hashlib.sha256()
                for start, end in index.spans[session_date]:
                    handle.seek(start)
                    remaining = end - start
                    while remaining > 0:
                        chunk = handle.read(min(1024 * 1024, remaining))
                        if not chunk:
                            raise OSError(f"dashboard ledger truncated: {source}")
                        checksum.update(chunk)
                        remaining -= len(chunk)
                digest = checksum.hexdigest()
            output[session_date] = digest
            metadata[session_date] = {"spans": spans, "sha256": digest}
        finished = os.fstat(handle.fileno())
    if (finished.st_dev, finished.st_ino, finished.st_size, finished.st_mtime_ns) != (
        index.device,
        index.inode,
        index.observed_size,
        index.modified_ns,
    ):
        raise OSError(f"dashboard ledger changed during session digest: {source}")
    return output, {"identity": identity, "sessions": metadata}


def _history_session_source_context(
    *,
    state_dir: Path,
    product: str,
    benchmark_history: _BenchmarkHistoryIndex,
    benchmark_projection: BenchmarkProjection | None,
    marks_path: Path,
    marks_index: _LedgerSessionIndex | None,
    live_benchmark_path: Path,
    live_benchmark_index: _LedgerSessionIndex | None,
    overnight_history_marks: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, str], dict[str, Any], list[str]]:
    previous_head = _history_session_head(state_dir) or {}
    previous_sources = previous_head.get("sources")
    previous_sources = previous_sources if isinstance(previous_sources, Mapping) else {}
    mark_digests, mark_source = _digest_ledger_sessions(
        marks_path,
        marks_index,
        previous=previous_sources.get("marks")
        if isinstance(previous_sources.get("marks"), Mapping)
        else None,
    )
    live_digests, live_source = _digest_ledger_sessions(
        live_benchmark_path,
        live_benchmark_index,
        previous=previous_sources.get("live_benchmark")
        if isinstance(previous_sources.get("live_benchmark"), Mapping)
        else None,
    )
    if benchmark_projection is not None:
        benchmark_digests = {
            str(session_date): str(entry.get("sha256") or "")
            for session_date, entry in benchmark_projection.session_entries.items()
            if isinstance(entry, Mapping) and entry.get("sha256")
        }
        benchmark_source = {
            "sha256": benchmark_projection.source_sha256,
            "size": benchmark_projection.source_size,
            "modified_ns": benchmark_projection.source_modified_ns,
        }
    else:
        benchmark_digests = {
            session_date: _history_projection_source_fingerprint(
                {"marks": list(rows)}
            )
            for session_date, rows in benchmark_history.marks_by_session.items()
        }
        benchmark_source = {
            "identity": [
                benchmark_history.device,
                benchmark_history.inode,
                benchmark_history.size,
                benchmark_history.modified_ns,
            ]
        }
    overnight_grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in overnight_history_marks:
        session_date = str(row.get("session_date") or "")[:10]
        if session_date:
            overnight_grouped.setdefault(session_date, []).append(row)
    overnight_digests = {
        session_date: _history_projection_source_fingerprint({"marks": rows})
        for session_date, rows in overnight_grouped.items()
    }
    origins_digest = _history_projection_source_fingerprint(
        {"origins": benchmark_history.origins}
    )
    session_dates = sorted(
        set(mark_digests)
        | set(live_digests)
        | set(benchmark_digests)
        | set(overnight_digests)
    )
    fingerprints = {
        session_date: _history_projection_source_fingerprint(
            {
                "schema_version": _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION,
                "dashboard_schema_version": DASHBOARD_SCHEMA_VERSION,
                "product": product,
                "marks": mark_digests.get(session_date),
                "benchmark": benchmark_digests.get(session_date),
                "live_benchmark": live_digests.get(session_date),
                "overnight": overnight_digests.get(session_date),
                # Live benchmark marks are rebased through this origin. A
                # changed origin therefore invalidates only projection shards,
                # never the canonical ledgers themselves.
                "benchmark_origins": origins_digest,
            }
        )
        for session_date in session_dates
    }
    return fingerprints, {
        "marks": mark_source,
        "live_benchmark": live_source,
        "benchmark": benchmark_source,
        "benchmark_origins_sha256": origins_digest,
    }, session_dates


def _session_projection_payload(
    *,
    session_date: str,
    source_fingerprint: str,
    grouped: Mapping[str, list[dict[str, Any]]],
    is_overnight: bool,
) -> dict[str, Any]:
    retained = {
        series_id: [
            row for row in rows if str(row.get("session_date") or "") == session_date
        ]
        for series_id, rows in grouped.items()
    }
    retained = {key: value for key, value in retained.items() if value}
    minute_axis = sorted(
        {
            int(row["timestamp_seconds"] // 60)
            for rows in retained.values()
            for row in rows
        }
    )
    minute_index = {minute: index for index, minute in enumerate(minute_axis)}
    series_payload: list[dict[str, Any]] = []
    series_summary: list[dict[str, Any]] = []
    historical_rows = sorted(
        (
            row
            for rows in retained.values()
            for row in rows
            if row["historical_minute_replay"]
        ),
        key=lambda row: (float(row["timestamp_seconds"]), str(row["series_id"])),
    )
    coverage = [
        value
        for row in historical_rows
        if (value := _finite_float(row.get("fresh_trade_notional_coverage_ratio")))
        is not None
    ]
    for series_id, rows in retained.items():
        first = rows[0]
        last = rows[-1]
        points_per_session = (
            2
            if is_overnight and first["series_type"] == "strategy"
            else 270
            if first["series_type"] == "strategy"
            else 300
            if series_id == "benchmark_tx_continuous"
            else 271
        )
        series_payload.append(
            {
                "series_id": series_id,
                "series_type": first["series_type"],
                "minute_indexes": [
                    minute_index[int(row["timestamp_seconds"] // 60)] for row in rows
                ],
                "cumulative_return_fraction": [
                    row["cumulative_return_fraction"] for row in rows
                ],
                "cumulative_return_pct": [
                    row["cumulative_return_pct"] for row in rows
                ],
                "quality_flags": [
                    int(row["valuation_stale"])
                    | (int(row["historical_minute_replay"]) << 1)
                    | (
                        int(int(row.get("missing_price_position_count") or 0) > 0)
                        << 2
                    )
                    for row in rows
                ],
            }
        )
        series_summary.append(
            {
                "series_id": series_id,
                "series_type": first["series_type"],
                "first_minute": first["minute"],
                "last_minute": last["minute"],
                "first_wealth_index": first["_wealth_index"],
                "last_wealth_index": last["_wealth_index"],
                "initial_capital_twd": first["_initial_capital_twd"],
                "start_equity_twd": first["_total_equity_twd"],
                "end_equity_twd": last["_total_equity_twd"],
                "cumulative_return_fraction": last["cumulative_return_fraction"],
                "cumulative_return_pct": last["cumulative_return_pct"],
                "point_count": len(rows),
                "expected_points_per_session": points_per_session,
            }
        )
    return {
        "schema_version": _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION,
        "session_date": session_date,
        "source_fingerprint": source_fingerprint,
        "minute_axis": minute_axis,
        "minute_series": series_payload,
        "series_summary": series_summary,
        "historical": {
            "replay_points": len(historical_rows),
            "carried_price_points": sum(
                int(row.get("last_trade_carried_position_count") or 0) > 0
                for row in historical_rows
            ),
            "missing_price_points": sum(
                int(row.get("missing_price_position_count") or 0) > 0
                for row in historical_rows
            ),
            "fresh_coverage_sum": sum(coverage),
            "fresh_coverage_count": len(coverage),
            "fresh_coverage_values": coverage,
            "fresh_coverage_min": min(coverage) if coverage else None,
            "valuation_contracts": sorted(
                {
                    str(value)
                    for row in historical_rows
                    if (value := row.get("minute_valuation_contract"))
                }
            ),
            "valuation_sources": sorted(
                {
                    str(value)
                    for row in historical_rows
                    if (value := row.get("valuation_source"))
                }
            ),
        },
    }


def _partition_history_rows_by_session(
    grouped: Mapping[str, list[dict[str, Any]]],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    partitioned: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for series_id, rows in grouped.items():
        for row in rows:
            session_date = str(row.get("session_date") or "")
            if session_date:
                partitioned.setdefault(session_date, {}).setdefault(
                    series_id, []
                ).append(row)
    return partitioned


def _persist_history_session_shard(
    state_dir: Path, payload: Mapping[str, Any]
) -> dict[str, Any] | None:
    root = _history_session_projection_root(state_dir)
    if root is None:
        return None
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    session_date = str(payload["session_date"])
    relative = Path("shards") / session_date / f"{digest}.json.gz"
    target = root / relative
    if not target.is_file():
        compressed = gzip.compress(encoded, compresslevel=1, mtime=0)
        if len(compressed) > _HISTORY_SESSION_PROJECTION_MAX_COMPRESSED_BYTES:
            return None
        _atomic_private_bytes(target, compressed)
    return {
        "source_fingerprint": payload["source_fingerprint"],
        "sha256": digest,
        "path": relative.as_posix(),
        "point_count": sum(
            len(series.get("minute_indexes") or ())
            for series in payload.get("minute_series") or ()
            if isinstance(series, Mapping)
        ),
    }


def _load_history_session_shard(
    state_dir: Path,
    *,
    session_date: str,
    entry: Mapping[str, Any],
    source_fingerprint: str,
) -> dict[str, Any] | None:
    root = _history_session_projection_root(state_dir)
    if root is None or entry.get("source_fingerprint") != source_fingerprint:
        return None
    digest = str(entry.get("sha256") or "")
    relative = Path(str(entry.get("path") or ""))
    if (
        len(digest) != 64
        or relative.is_absolute()
        or ".." in relative.parts
        or relative.parts[:2] != ("shards", session_date)
    ):
        return None
    path = root / relative
    try:
        if path.stat().st_size > _HISTORY_SESSION_PROJECTION_MAX_COMPRESSED_BYTES:
            return None
        encoded = gzip.decompress(path.read_bytes())
        if hashlib.sha256(encoded).hexdigest() != digest:
            return None
        payload = json.loads(encoded)
    except (EOFError, OSError, TypeError, ValueError, json.JSONDecodeError, gzip.BadGzipFile):
        return None
    if (
        not isinstance(payload, dict)
        or int(payload.get("schema_version") or 0)
        != _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION
        or payload.get("session_date") != session_date
        or payload.get("source_fingerprint") != source_fingerprint
        or not isinstance(payload.get("minute_axis"), list)
        or not isinstance(payload.get("minute_series"), list)
        or not isinstance(payload.get("series_summary"), list)
    ):
        return None
    observed_points = sum(
        len(series.get("minute_indexes") or ())
        for series in payload["minute_series"]
        if isinstance(series, Mapping)
    )
    if observed_points != int(entry.get("point_count") or -1):
        return None
    return payload


def _persist_history_session_head(
    state_dir: Path,
    *,
    product: str,
    sources: Mapping[str, Any],
    available_session_dates: list[str],
    sessions: Mapping[str, Mapping[str, Any]],
) -> None:
    root = _history_session_projection_root(state_dir)
    if root is None:
        return
    old = _history_session_head(state_dir) or {}
    old_sessions = old.get("sessions")
    old_sessions = old_sessions if isinstance(old_sessions, Mapping) else {}
    changed = [
        {
            "schema_version": _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION,
            "session_date": session_date,
            "previous_sha256": (
                old_sessions.get(session_date, {}).get("sha256")
                if isinstance(old_sessions.get(session_date), Mapping)
                else None
            ),
            "sha256": entry.get("sha256"),
            "source_fingerprint": entry.get("source_fingerprint"),
        }
        for session_date, entry in sessions.items()
        if not isinstance(old_sessions.get(session_date), Mapping)
        or old_sessions[session_date].get("sha256") != entry.get("sha256")
    ]
    removed = sorted(set(old_sessions) - set(sessions))
    changed.extend(
        {
            "schema_version": _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION,
            "session_date": session_date,
            "previous_sha256": old_sessions[session_date].get("sha256"),
            "sha256": None,
            "source_fingerprint": None,
        }
        for session_date in removed
        if isinstance(old_sessions[session_date], Mapping)
    )
    if changed:
        root.mkdir(parents=True, exist_ok=True)
        delta = root / "delta.jsonl"
        descriptor = os.open(
            delta,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC,
            0o600,
        )
        try:
            os.chmod(delta, 0o600)
            for row in changed:
                os.write(
                    descriptor,
                    json.dumps(
                        row,
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n",
                )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    head = {
        "schema_version": _HISTORY_SESSION_PROJECTION_SCHEMA_VERSION,
        "product": product,
        "sources": sources,
        "available_session_dates": available_session_dates,
        "sessions": dict(sessions),
    }
    if old == head:
        return
    _atomic_private_bytes(
        root / "head.json",
        json.dumps(
            head,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n",
    )


def _merge_history_session_shards(
    shards: list[Mapping[str, Any]],
    *,
    available_session_dates: list[str],
    is_overnight: bool,
) -> dict[str, Any]:
    axis: list[int] = []
    series_parts: dict[str, dict[str, Any]] = {}
    summaries: dict[str, list[Mapping[str, Any]]] = {}
    replay_points = carried_points = missing_points = 0
    coverage_values: list[float] = []
    coverage_minima: list[float] = []
    contracts: set[str] = set()
    sources: set[str] = set()
    for shard in sorted(shards, key=lambda item: str(item["session_date"])):
        local_axis = [int(value) for value in shard["minute_axis"]]
        offset = len(axis)
        axis.extend(local_axis)
        for series in shard["minute_series"]:
            series_id = str(series["series_id"])
            target = series_parts.setdefault(
                series_id,
                {
                    "series_id": series_id,
                    "series_type": series["series_type"],
                    "minute_indexes": [],
                    "cumulative_return_fraction": [],
                    "cumulative_return_pct": [],
                    "quality_flags": [],
                    "first_minute": local_axis[int(series["minute_indexes"][0])],
                },
            )
            target["minute_indexes"].extend(
                offset + int(index) for index in series["minute_indexes"]
            )
            target["cumulative_return_fraction"].extend(
                series["cumulative_return_fraction"]
            )
            target["cumulative_return_pct"].extend(series["cumulative_return_pct"])
            target["quality_flags"].extend(series["quality_flags"])
        for summary in shard["series_summary"]:
            summaries.setdefault(str(summary["series_id"]), []).append(summary)
        historical = shard.get("historical") or {}
        replay_points += int(historical.get("replay_points") or 0)
        carried_points += int(historical.get("carried_price_points") or 0)
        missing_points += int(historical.get("missing_price_points") or 0)
        coverage_values.extend(
            float(value) for value in historical.get("fresh_coverage_values") or ()
        )
        if (minimum := _finite_float(historical.get("fresh_coverage_min"))) is not None:
            coverage_minima.append(minimum)
        contracts.update(str(value) for value in historical.get("valuation_contracts") or ())
        sources.update(str(value) for value in historical.get("valuation_sources") or ())
    if axis != sorted(set(axis)):
        raise ValueError("history session projection minute axes overlap or are unsorted")
    ordered_series = sorted(
        series_parts.values(),
        key=lambda item: (int(item["first_minute"]), str(item["series_id"])),
    )
    minute_series: list[dict[str, Any]] = []
    for series in ordered_series:
        fractions = series.pop("cumulative_return_fraction")
        baseline = 1.0 + float(fractions[0])
        returns = [
            ((1.0 + float(value)) / baseline - 1.0) * 100.0
            if baseline > 0.0
            else ((1.0 + float(value)) - baseline) * 100.0
            for value in fractions
        ]
        series.pop("first_minute", None)
        minute_series.append(
            {
                "series_id": series["series_id"],
                "series_type": series["series_type"],
                "minute_indexes": series["minute_indexes"],
                "return_pct": returns,
                "cumulative_return_pct": series["cumulative_return_pct"],
                "quality_flags": series["quality_flags"],
            }
        )
    range_summary: list[dict[str, Any]] = []
    for series_id in sorted(summaries):
        parts = sorted(summaries[series_id], key=lambda item: str(item["first_minute"]))
        first = parts[0]
        last = parts[-1]
        baseline_wealth = float(first["first_wealth_index"])
        end_wealth = float(last["last_wealth_index"])
        initial_capital = _finite_float(first.get("initial_capital_twd"))
        baseline_equity = _finite_float(first.get("start_equity_twd"))
        end_equity = _finite_float(last.get("end_equity_twd"))
        if baseline_equity is None and initial_capital is not None:
            baseline_equity = initial_capital * baseline_wealth
        if end_equity is None and initial_capital is not None:
            end_equity = initial_capital * end_wealth
        period_return = (
            end_wealth / baseline_wealth - 1.0
            if baseline_wealth > 0.0
            else end_wealth - baseline_wealth
        )
        session_counts = {
            str(part["first_minute"])[:10]: int(part["point_count"])
            for part in parts
        }
        expected_per_session = int(first["expected_points_per_session"])
        expected_points = expected_per_session * len(session_counts)
        point_count = sum(session_counts.values())
        range_summary.append(
            {
                "series_id": series_id,
                "series_type": first["series_type"],
                "baseline_kind": "first_visible_mark",
                "baseline_at_utc": first["first_minute"],
                "baseline_equity_twd": baseline_equity,
                "initial_capital_twd": initial_capital,
                "start_at_utc": first["first_minute"],
                "end_at_utc": last["last_minute"],
                "start_equity_twd": _finite_float(first.get("start_equity_twd")),
                "end_equity_twd": end_equity,
                "range_net_pnl_twd": (
                    end_equity - baseline_equity
                    if end_equity is not None and baseline_equity is not None
                    else None
                ),
                "return_fraction": period_return,
                "return_pct": period_return * 100.0,
                "cumulative_net_pnl_twd": (
                    end_equity - initial_capital
                    if end_equity is not None and initial_capital is not None
                    else None
                ),
                "cumulative_return_fraction": last["cumulative_return_fraction"],
                "cumulative_return_pct": last["cumulative_return_pct"],
                "period_return_fraction": period_return,
                "period_return_pct": period_return * 100.0,
                "point_count": point_count,
                "session_point_counts": session_counts,
                "expected_minute_points": expected_points,
                "expected_points_per_session": expected_per_session,
                "minute_coverage_ratio": (
                    point_count / expected_points if expected_points else None
                ),
            }
        )
    returned_points = sum(
        len(series["minute_indexes"]) for series in minute_series
    )
    coverage_start = (
        datetime.fromtimestamp(axis[0] * 60, tz=timezone.utc).isoformat(timespec="minutes")
        if axis
        else None
    )
    coverage_end = (
        datetime.fromtimestamp(axis[-1] * 60, tz=timezone.utc).isoformat(timespec="minutes")
        if axis
        else None
    )
    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "simulation_only": True,
        "production_order_possible": False,
        "range": "all",
        "range_seconds": None,
        "start_date": None,
        "end_date": None,
        "available_start_date": min(available_session_dates)
        if available_session_dates
        else None,
        "available_end_date": max(available_session_dates)
        if available_session_dates
        else None,
        "anchor_at_utc": (
            datetime.fromtimestamp(axis[-1] * 60, tz=timezone.utc).isoformat()
            if axis
            else None
        ),
        "coverage_start_utc": coverage_start,
        "coverage_end_utc": coverage_end,
        "raw_points_in_range": returned_points,
        "returned_points": returned_points,
        "downsampled": False,
        "curve_granularity": "auction_events" if is_overnight else "1m",
        "history_contract": (
            "13:30 official close and next-session 09:00 official open "
            "counterfactual events; no intraminute interpolation or exchange fill claim"
            if is_overnight
            else "right_labelled_one_minute"
        ),
        "expected_right_labelled_session_minute_points": 270,
        "expected_strategy_session_points_from_09_01": 270,
        "expected_stock_benchmark_session_points_including_09_00": 271,
        "expected_tx_day_session_points": 300,
        "expected_overnight_auction_event_points": 2 if is_overnight else None,
        "return_basis": "selected_range_first_visible_mark",
        "cumulative_return_basis": "initial_capital_cumulative_total_equity",
        "period_return_basis": "selected_range_first_visible_mark",
        "range_summary": range_summary,
        "historical_minute_replay_points": replay_points,
        "historical_minute_carried_price_points": carried_points,
        "historical_minute_missing_price_points": missing_points,
        "historical_minute_min_fresh_trade_notional_coverage_ratio": (
            min(coverage_minima) if coverage_minima else None
        ),
        "historical_minute_mean_fresh_trade_notional_coverage_ratio": (
            sum(coverage_values) / len(coverage_values) if coverage_values else None
        ),
        "historical_minute_valuation_contracts": sorted(contracts),
        "historical_minute_valuation_sources": sorted(sources),
        "history": [],
        "history_encoding": "minute_columns_v2",
        "minute_axis": axis,
        "minute_series": minute_series,
    }


def _load_or_rebuild_history_session_projection(
    *,
    state_dir: Path,
    product: str,
    is_overnight: bool,
    source_fingerprints: Mapping[str, str],
    sources: Mapping[str, Any],
    available_session_dates: list[str],
) -> dict[str, Any] | None:
    head = _history_session_head(state_dir)
    if head is None or head.get("product") != product:
        return None
    head_sessions = head.get("sessions")
    if not isinstance(head_sessions, Mapping):
        return None
    loaded: dict[str, dict[str, Any]] = {}
    changed: list[str] = []
    for session_date in available_session_dates:
        entry = head_sessions.get(session_date)
        shard = (
            _load_history_session_shard(
                state_dir,
                session_date=session_date,
                entry=entry,
                source_fingerprint=source_fingerprints[session_date],
            )
            if isinstance(entry, Mapping)
            else None
        )
        if shard is None:
            changed.append(session_date)
        else:
            loaded[session_date] = shard
    if set(head_sessions) - set(available_session_dates):
        # Source history removal is exceptional. A complete rebuild gives the
        # existing correctness path one chance to validate the new scope.
        return None
    if len(changed) > _HISTORY_SESSION_REBUILD_LIMIT:
        return None
    entries = {
        session_date: dict(head_sessions[session_date])
        for session_date in loaded
        if isinstance(head_sessions.get(session_date), Mapping)
    }
    for session_date in changed:
        captured: dict[str, dict[str, Any]] = {
            session_date: {
                "source_fingerprint": source_fingerprints[session_date]
            }
        }
        build_dashboard_history_snapshot(
            state_dir=state_dir,
            range_key="all",
            start_date=session_date,
            end_date=session_date,
            maximum_points_per_series=10_000,
            resolution="1m",
            history_encoding="minute_columns_v2",
            use_memory_cache=False,
            use_persistent_cache=False,
            use_session_projection=False,
            _session_projection_capture=captured,
        )
        shard = captured.get(session_date)
        if not isinstance(shard, dict):
            return None
        entry = _persist_history_session_shard(state_dir, shard)
        if entry is None:
            return None
        loaded[session_date] = shard
        entries[session_date] = entry
    _persist_history_session_head(
        state_dir,
        product=product,
        sources=sources,
        available_session_dates=available_session_dates,
        sessions=entries,
    )
    return _merge_history_session_shards(
        list(loaded.values()),
        available_session_dates=available_session_dates,
        is_overnight=is_overnight,
    )


def _make_benchmark_history_index(
    payload: Mapping[str, Any], *, stat: os.stat_result
) -> _BenchmarkHistoryIndex:
    load_error = str(payload.get("load_error") or "") or None
    raw_origins = payload.get("origins")
    origins = {
        str(key): value
        for key, value in (
            raw_origins.items() if isinstance(raw_origins, Mapping) else ()
        )
        if isinstance(value, Mapping)
    }
    marks = tuple(
        row for row in (payload.get("marks") or ()) if isinstance(row, Mapping)
    )
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in marks:
        session_date = str(row.get("session_date") or "")[:10]
        if not session_date:
            continue
        grouped.setdefault(session_date, []).append(row)
    return _BenchmarkHistoryIndex(
        device=stat.st_dev,
        inode=stat.st_ino,
        size=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        origins=origins,
        marks=marks,
        marks_by_session={key: tuple(value) for key, value in grouped.items()},
        load_error=load_error,
    )


def _load_persistent_benchmark_history_index(
    source: Path, *, stat: os.stat_result
) -> _BenchmarkHistoryIndex | None:
    cache_path = _persistent_benchmark_history_path(source)
    if cache_path is None or not cache_path.is_file():
        return None
    try:
        if cache_path.stat().st_size > _BENCHMARK_HISTORY_CACHE_MAX_COMPRESSED_BYTES:
            return None
        payload = json.loads(gzip.decompress(cache_path.read_bytes()))
        if not isinstance(payload, Mapping):
            return None
        if int(payload.get("schema_version") or 0) != (
            _BENCHMARK_HISTORY_CACHE_SCHEMA_VERSION
        ):
            return None
        if str(payload.get("source") or "") != str(source.resolve()):
            return None
        if (
            int(payload.get("device")),
            int(payload.get("inode")),
            int(payload.get("size")),
            int(payload.get("modified_ns")),
        ) != (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns):
            return None
        index = _make_benchmark_history_index(payload, stat=stat)
        if int(payload.get("mark_count") or -1) != len(index.marks):
            return None
        return index
    except (
        EOFError,
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        gzip.BadGzipFile,
    ):
        return None


def _persist_benchmark_history_index(
    source: Path, *, index: _BenchmarkHistoryIndex
) -> None:
    cache_path = _persistent_benchmark_history_path(source)
    if cache_path is None:
        return
    latest_indices: dict[tuple[str, str], int] = {}
    for row_index, row in enumerate(index.marks):
        latest_indices[
            (
                str(row.get("session_date") or ""),
                str(row.get("benchmark_id") or ""),
            )
        ] = row_index
    compact_marks = [
        dict(row)
        if latest_indices.get(
            (
                str(row.get("session_date") or ""),
                str(row.get("benchmark_id") or ""),
            )
        )
        == row_index
        else {
            key: value
            for key, value in row.items()
            if key in _BENCHMARK_HISTORY_INTERIOR_FIELDS
        }
        for row_index, row in enumerate(index.marks)
    ]
    payload = {
        "schema_version": _BENCHMARK_HISTORY_CACHE_SCHEMA_VERSION,
        "source": str(source.resolve()),
        "device": index.device,
        "inode": index.inode,
        "size": index.size,
        "modified_ns": index.modified_ns,
        "mark_count": len(index.marks),
        "load_error": index.load_error,
        "origins": index.origins,
        "marks": compact_marks,
    }
    temporary = cache_path.with_name(
        f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        compressed = gzip.compress(encoded, compresslevel=1)
        if len(compressed) > _BENCHMARK_HISTORY_CACHE_MAX_COMPRESSED_BYTES:
            return
        with temporary.open("xb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, cache_path)
    except (OSError, TypeError, ValueError):
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _benchmark_history_index(root: Path) -> _BenchmarkHistoryIndex:
    """Index immutable benchmark marks once per source-file generation.

    ``benchmark_history.json`` is hundreds of MiB in production.  Filtering its
    complete in-memory mark list for one session on every status poll made an
    otherwise small request scale with all retained history.  Keep source order
    for the history endpoint and a separate O(1) session lookup for status/date
    views.  The index is invalidated by the same inode/size/mtime contract as
    the existing object cache.
    """

    path = (Path(root) / BENCHMARK_HISTORY_FILENAME).resolve()
    with _BENCHMARK_HISTORY_INDEX_LOCK:
        for _attempt in range(3):
            try:
                stat = path.stat()
            except FileNotFoundError:
                return _BenchmarkHistoryIndex(
                    device=0,
                    inode=0,
                    size=0,
                    modified_ns=0,
                    origins={},
                    marks=(),
                    marks_by_session={},
                    load_error="benchmark_history_unavailable",
                )
            signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
            cached = _BENCHMARK_HISTORY_INDEX_CACHE.get(path)
            if (
                cached is not None
                and (
                    cached.device,
                    cached.inode,
                    cached.size,
                    cached.modified_ns,
                )
                == signature
            ):
                return cached

            projection = load_benchmark_projection(
                state_dir=path.parent,
                source_path=path,
            )
            if projection is not None:
                result = _BenchmarkHistoryIndex(
                    device=stat.st_dev,
                    inode=stat.st_ino,
                    size=stat.st_size,
                    modified_ns=stat.st_mtime_ns,
                    origins=projection.origins,
                    marks=projection.marks,
                    marks_by_session=projection.marks_by_session,
                    load_error=None,
                )
                _BENCHMARK_HISTORY_INDEX_CACHE[path] = result
                return result

            persisted = _load_persistent_benchmark_history_index(path, stat=stat)
            if persisted is not None:
                _BENCHMARK_HISTORY_INDEX_CACHE[path] = persisted
                return persisted

            payload = _load_benchmark_history(path.parent)
            final_stat = path.stat()
            if (
                final_stat.st_dev,
                final_stat.st_ino,
                final_stat.st_size,
                final_stat.st_mtime_ns,
            ) != signature:
                continue
            result = _make_benchmark_history_index(payload, stat=stat)
            _BENCHMARK_HISTORY_INDEX_CACHE[path] = result
            _persist_benchmark_history_index(path, index=result)
            return result
    raise OSError("benchmark history changed repeatedly while indexing")


def _previous_benchmark_close(
    history: _BenchmarkHistoryIndex,
    *,
    benchmark_id: str,
    session_date: str,
    contract_code: str | None = None,
) -> Mapping[str, Any] | None:
    """Find the latest completed retained mark before a selected session."""

    wanted_contract = str(contract_code or "").strip().upper()
    for day in sorted(history.marks_by_session, reverse=True):
        if day >= session_date:
            continue
        rows = history.marks_by_session[day]
        for row in reversed(rows):
            if str(row.get("benchmark_id") or "") != benchmark_id:
                continue
            if (
                wanted_contract
                and str(row.get("contract_code") or "").upper() != wanted_contract
            ):
                continue
            price = _finite_float(row.get("last_mark_price"))
            if price is not None and price > 0.0:
                return row
    return None


def _rebase_live_benchmark(
    source: Mapping[str, Any],
    origin: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Re-anchor a live Buy-and-Hold mark to its retained prior-close origin.

    The live engine remains untouched while positions are open.  This display
    Schema 2 chain-links gross wealth at the live-process boundary. The legacy
    price-offset projection remains below only for schema-1 history files.
    """

    row = dict(source)
    if origin and int(origin.get("benchmark_accounting_contract_version") or 0) >= 2:
        for key in (
            "benchmark_accounting_contract_version",
            "holding_period",
            "daily_return_basis",
            "capital_basis",
            "maximum_leverage",
            "tracking_costs_excluded_from_return",
        ):
            if key in origin:
                row[key] = origin[key]
    if str(row.get("instrument_type") or "").startswith("stock") and str(
        row.get("valuation_source") or ""
    ).startswith("corporate_action_reference_unavailable"):
        row.update(
            {
                "total_equity_twd": None,
                "net_pnl_twd": None,
                "return_fraction": None,
                "return_pct": None,
                "valuation_stale": True,
                "benchmark_origin_error": "total_return_source_incomplete",
            }
        )
        return row
    if not origin or bool(row.get("benchmark_origin_rebased")):
        return row
    expected = origin.get("live_origin") or {}
    expected_at = str(expected.get("entry_at") or "")
    observed_at = str(row.get("origin_entry_at") or row.get("entry_at") or "")
    expected_price = _finite_float(expected.get("entry_price"))
    observed_price = _finite_float(
        row.get("origin_entry_price")
        if row.get("origin_entry_price") is not None
        else row.get("entry_price")
    )
    if observed_price is None and observed_at == expected_at:
        # Schema-3 benchmark marks retained entry_at but not entry_price.  The
        # audited live origin supplies that immutable value after identity is
        # established by the exact timestamp.
        observed_price = expected_price
    if (
        not expected_at
        or observed_at != expected_at
        or expected_price is None
        or observed_price is None
        or not math.isclose(observed_price, expected_price, rel_tol=0.0, abs_tol=1e-9)
    ):
        row.update(
            {
                "total_equity_twd": None,
                "net_pnl_twd": None,
                "return_fraction": None,
                "return_pct": None,
                "valuation_stale": True,
                "valuation_source": "benchmark_origin_mismatch_fail_closed",
                "benchmark_origin_error": "live_entry_no_longer_matches_audited_origin",
            }
        )
        return row

    canonical_entry = _finite_float(origin.get("entry_price"))
    canonical_capital = _finite_float(origin.get("initial_capital_twd"))
    canonical_wealth_at_live = _finite_float(
        origin.get("canonical_buy_hold_wealth_at_live_origin")
    )
    live_wealth = _finite_float(row.get("buy_hold_wealth_index"))
    if (
        canonical_entry is not None
        and canonical_capital is not None
        and canonical_capital > 0.0
        and canonical_wealth_at_live is not None
        and canonical_wealth_at_live > 0.0
        and live_wealth is not None
        and live_wealth > 0.0
    ):
        wealth = canonical_wealth_at_live * live_wealth
        performance_pnl = canonical_capital * (wealth - 1.0)
        row.update(
            {
                "entry_at": origin.get("entry_at"),
                "entry_price": canonical_entry,
                "initial_capital_twd": canonical_capital,
                "buy_hold_wealth_index": wealth,
                "performance_pnl_twd": performance_pnl,
                "gross_pnl_twd": performance_pnl,
                "net_pnl_twd": performance_pnl,
                "total_equity_twd": canonical_capital * wealth,
                "return_fraction": wealth - 1.0,
                "return_pct": (wealth - 1.0) * 100.0,
                "benchmark_origin_rebased": True,
                "benchmark_origin_session_date": origin.get("session_date"),
                "counterfactual_open_replay": False,
                "replay_basis": "cross_session_buy_and_hold_from_previous_close",
                "valuation_source": (
                    "retained_prior_close_buy_hold_history_plus_live_wealth"
                ),
            }
        )
        return row
    raw_net = _finite_float(row.get("net_pnl_twd"))
    gross_multiplier = _finite_float(origin.get("gross_pnl_multiplier"))
    if (
        canonical_entry is None
        or canonical_capital is None
        or canonical_capital <= 0.0
        or raw_net is None
        or gross_multiplier is None
        or gross_multiplier <= 0.0
    ):
        row.update(
            {
                "total_equity_twd": None,
                "net_pnl_twd": None,
                "return_fraction": None,
                "return_pct": None,
                "valuation_stale": True,
                "valuation_source": "benchmark_origin_incomplete_fail_closed",
            }
        )
        return row

    raw_initial_fee = _finite_float(expected.get("initial_fixed_fees_twd")) or 0.0
    canonical_initial_fee = _finite_float(origin.get("initial_fixed_fees_twd")) or 0.0
    raw_initial_tax = _finite_float(expected.get("initial_transaction_tax_twd")) or 0.0
    canonical_initial_tax = (
        _finite_float(origin.get("initial_transaction_tax_twd")) or 0.0
    )
    live_action_factor = _finite_float(row.get("corporate_action_factor"))
    prior_action_factor = _finite_float(
        origin.get("corporate_action_factor_to_live_entry")
    )
    mark_price = _finite_float(row.get("last_mark_price"))
    explicit_net_offset = _finite_float(origin.get("live_net_pnl_offset_twd"))
    is_continuous_future = (
        str(row.get("instrument_type") or "") == "continuous_long_future"
    )
    if is_continuous_future and explicit_net_offset is not None:
        net_pnl = raw_net + explicit_net_offset
        canonical_fee_basis = (
            _finite_float(origin.get("fixed_fees_twd_to_live_origin"))
            or canonical_initial_fee
        )
        canonical_tax_basis = (
            _finite_float(origin.get("transaction_tax_twd_to_live_origin"))
            or canonical_initial_tax
        )
    elif (
        str(row.get("instrument_type") or "").startswith("stock")
        and live_action_factor is not None
        and prior_action_factor is not None
        and mark_price is not None
    ):
        raw_gross = gross_multiplier * (
            live_action_factor * mark_price - expected_price
        )
        canonical_factor = prior_action_factor * live_action_factor
        canonical_gross = gross_multiplier * (
            canonical_factor * mark_price - canonical_entry
        )
        gross_offset = canonical_gross - raw_gross
        row["corporate_action_factor"] = canonical_factor
        net_pnl = (
            raw_net
            + gross_offset
            + raw_initial_fee
            - canonical_initial_fee
            + raw_initial_tax
            - canonical_initial_tax
        )
        canonical_fee_basis = canonical_initial_fee
        canonical_tax_basis = canonical_initial_tax
    else:
        gross_offset = (expected_price - canonical_entry) * gross_multiplier
        net_pnl = (
            raw_net
            + gross_offset
            + raw_initial_fee
            - canonical_initial_fee
            + raw_initial_tax
            - canonical_initial_tax
        )
        canonical_fee_basis = canonical_initial_fee
        canonical_tax_basis = canonical_initial_tax
    current_fixed_fees = _finite_float(row.get("fixed_fees_twd")) or 0.0
    current_transaction_tax = _finite_float(row.get("transaction_tax_twd")) or 0.0
    total_equity = canonical_capital + net_pnl
    return_fraction, return_pct = _capital_return(canonical_capital, total_equity)
    prior_close_origin = (
        int(origin.get("benchmark_accounting_contract_version") or 0) >= 2
    )
    row.update(
        {
            "entry_at": origin.get("entry_at"),
            "entry_price": canonical_entry,
            "initial_capital_twd": canonical_capital,
            "fixed_fees_twd": (
                current_fixed_fees - raw_initial_fee + canonical_fee_basis
            ),
            "transaction_tax_twd": (
                current_transaction_tax - raw_initial_tax + canonical_tax_basis
            ),
            "net_pnl_twd": net_pnl,
            "total_equity_twd": total_equity,
            "return_fraction": return_fraction,
            "return_pct": return_pct,
            "benchmark_origin_rebased": True,
            "benchmark_origin_session_date": origin.get("session_date"),
            "counterfactual_open_replay": not prior_close_origin,
            "replay_basis": (
                "cross_session_buy_and_hold_from_previous_close"
                if prior_close_origin
                else "actual_session_open_to_recorded_executable_marks"
            ),
            "valuation_source": (
                f"{row.get('valuation_source') or 'recorded_executable_mark'}"
                + (
                    "+rebased_to_previous_close_buy_hold_origin"
                    if prior_close_origin
                    else "+rebased_to_actual_session_open"
                )
            ),
        }
    )
    if origin.get("total_return_contract"):
        row["total_return_contract"] = origin.get("total_return_contract")
        row["corporate_action_coverage"] = True
        row["corporate_action_status"] = "official_reference_complete"
        row["corporate_action_coverage_end"] = origin.get(
            "corporate_action_coverage_end"
        )
        row["corporate_action_count"] = int(
            origin.get("corporate_action_count_to_live_entry") or 0
        ) + int(row.get("corporate_action_count") or 0)
        row.setdefault(
            "corporate_action_factor",
            _finite_float(origin.get("corporate_action_factor_to_live_entry")) or 1.0,
        )
    return row


def _ratio(numerator: object, denominator: object) -> float:
    top = _finite_float(numerator)
    bottom = _finite_float(denominator)
    if top is None or bottom is None or bottom <= 0.0:
        return 0.0
    return min(max(top / bottom, 0.0), 1.0)


def _seconds_between(start: object, end: object) -> float | None:
    if not start or not end:
        return None
    try:
        return max(0.0, (_timestamp(end) - _timestamp(start)).total_seconds())
    except (TypeError, ValueError):
        return None


def _is_taipei_session_date(value: object, session_date: str) -> bool:
    if not value or not session_date:
        return False
    try:
        return _timestamp(value).astimezone(TAIPEI).date().isoformat() == session_date
    except (TypeError, ValueError):
        return False


def _attach_execution_records(
    *,
    modes: list[dict[str, Any]],
    events: list[dict[str, Any]],
    observed: datetime,
    session_date: str | None = None,
) -> dict[str, Any]:
    """Attach today's append-only execution fact to every dashboard mode."""

    local = observed.astimezone(TAIPEI)
    selected_date = session_date or local.date().isoformat()
    latest_by_market: dict[str, dict[str, Any]] = {}
    latest_registered_by_market: dict[str, dict[str, Any]] = {}
    tracked = {"signal_registered", "signal_blocked", "flat_session_rearmed"}
    for event in events:
        market = str(event.get("market") or "")
        if (
            market
            and str(event.get("event") or "") in tracked
            and _is_taipei_session_date(event.get("recorded_at"), selected_date)
        ):
            latest_by_market[market] = event
            if str(event.get("event") or "") == "signal_registered":
                latest_registered_by_market[market] = event

    terminal_statuses = {"completed"}
    attempted_statuses = {"completed", "blocked"}
    status_counts: dict[str, int] = {}
    outcome_counts: dict[str, int] = {}
    for mode in modes:
        market = str(mode.get("market") or "")
        event = latest_by_market.get(market)
        if (
            str((event or {}).get("event") or "") == "signal_blocked"
            and str((event or {}).get("reason") or "")
            == "daily_signal_already_consumed"
            and market in latest_registered_by_market
        ):
            event = latest_registered_by_market[market]
        event_name = str((event or {}).get("event") or "")
        reason = (event or {}).get("reason")
        recorded_at = (event or {}).get("recorded_at")
        if event_name == "signal_registered":
            status = "completed"
        elif event_name == "signal_blocked":
            status = "blocked"
        elif event_name == "flat_session_rearmed":
            status = "starting"
        elif _is_taipei_session_date(mode.get("signal_at"), selected_date):
            status = "starting"
            recorded_at = mode.get("signal_at")
        elif selected_date != local.date().isoformat():
            status = "missed"
            reason = "historical_session_has_no_execution_record"
        elif local.weekday() >= 5:
            status = "waiting_trading_day"
        elif local.timetz().replace(tzinfo=None) < datetime_time(9, 0):
            status = "waiting_09_00"
        elif local.timetz().replace(tzinfo=None) < datetime_time(13, 20):
            status = "starting"
            reason = "missed_schedule_immediate_catch_up"
        else:
            status = "missed"
            reason = "entry_window_closed_without_execution_record"
        mode["today_execution_status"] = status
        mode["today_execution_event"] = event_name or None
        mode["today_execution_recorded_at"] = recorded_at
        mode["today_execution_reason"] = reason
        mode["today_execution_terminal"] = status in terminal_statuses
        event_counts = (event or {}).get("counts")
        event_counts = dict(event_counts) if isinstance(event_counts, Mapping) else {}
        fill_count = int(
            (event or {}).get("entry_fill_count")
            or mode.get("entry_fill_count")
            or sum(
                int(event_counts.get(key) or 0)
                for key in (
                    "ready",
                    "partial_depth",
                    "forced_synthetic_fill",
                )
            )
        )
        event_requested = (event or {}).get("entry_requested_shares")
        requested = int(
            mode.get("entry_requested_shares") or 0
            if event_requested is None
            else event_requested
        )
        event_unfilled = (event or {}).get("entry_unfilled_shares")
        unfilled = int(
            mode.get("entry_unfilled_shares") or 0
            if event_unfilled is None
            else event_unfilled
        )
        outcome = str((event or {}).get("entry_fill_outcome") or "")
        if outcome == "no_fill" and requested == 0 and fill_count == 0:
            outcome = "no_order"
        if not outcome:
            if event_name == "signal_registered":
                outcome = (
                    "filled"
                    if fill_count and (not requested or not unfilled)
                    else "partial"
                    if fill_count
                    else "no_order"
                    if requested == 0
                    else "no_fill"
                )
            elif event_name == "signal_blocked":
                outcome = "blocked"
            else:
                outcome = "pending"
        mode["today_execution_outcome"] = outcome
        mode["today_entry_fill_count"] = fill_count
        if event is not None:
            for key in (
                "entry_requested_shares",
                "entry_filled_shares",
                "entry_unfilled_shares",
                "entry_fill_policy",
                "entry_price_offset_ticks",
                "entry_fill_is_synthetic",
                "reason_counts",
            ):
                if event.get(key) is not None:
                    target = "signal_reason_counts" if key == "reason_counts" else key
                    mode[target] = event.get(key)
        status_counts[status] = status_counts.get(status, 0) + 1
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

    executed_count = sum(bool(mode.get("today_execution_terminal")) for mode in modes)
    attempted_count = sum(
        str(mode.get("today_execution_status") or "") in attempted_statuses
        for mode in modes
    )
    return {
        "session_date": selected_date,
        "policy": "missed_schedule_immediate_catch_up_before_13_20",
        "check_interval_seconds": 1,
        "executed_count": executed_count,
        "attempted_count": attempted_count,
        "blocked_count": status_counts.get("blocked", 0),
        "filled_mode_count": outcome_counts.get("filled", 0),
        "partial_mode_count": outcome_counts.get("partial", 0),
        "zero_fill_mode_count": outcome_counts.get("no_fill", 0),
        "no_order_mode_count": outcome_counts.get("no_order", 0),
        "failed_mode_count": outcome_counts.get("blocked", 0),
        "mode_count": len(modes),
        "completion_ratio": _ratio(executed_count, len(modes)),
        "all_executed": bool(modes) and executed_count == len(modes),
        "all_modes_filled": bool(modes)
        and outcome_counts.get("filled", 0) == len(modes),
        "status_counts": status_counts,
        "outcome_counts": outcome_counts,
    }


def _operational_issues(
    *,
    modes: list[dict[str, Any]],
    preopen: Mapping[str, Any],
    observed: datetime,
) -> list[dict[str, Any]]:
    """Build a public-safe, deduplicated list of actionable dashboard issues."""

    issues: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add(
        *,
        severity: str,
        scope: str,
        code: str,
        title: str,
        detail: str,
        market: str | None = None,
        count: int = 1,
        observed_at: object = None,
    ) -> None:
        key = (scope, market or "", code)
        if key in seen:
            return
        seen.add(key)
        issues.append(
            {
                "severity": severity,
                "scope": scope,
                "market": market,
                "code": code,
                "title": title,
                "detail": detail,
                "count": max(1, int(count)),
                "observed_at": observed_at or observed.isoformat(timespec="seconds"),
            }
        )

    for row in preopen.get("markets") or ():
        if not isinstance(row, Mapping) or row.get("status") != "failed":
            continue
        add(
            severity="error",
            scope="preopen_model",
            market=str(row.get("market") or "") or None,
            code=str(row.get("public_error_code") or "preopen_data_update_failed"),
            title="盤前模型資料準備失敗",
            detail=str(
                row.get("public_error_message")
                or "盤前資料或模型準備未完成；此模式不可視為 READY。"
            ),
            observed_at=row.get("completed_at") or preopen.get("updated_at"),
        )
    simulation = preopen.get("simulation")
    simulation = dict(simulation) if isinstance(simulation, Mapping) else {}
    for component, row in (simulation.get("components") or {}).items():
        if not isinstance(row, Mapping) or row.get("status") != "failed":
            continue
        labels = {
            "eligibility": "當日當沖資格驗證失敗",
            "shioaji_quote": "行情連線驗證失敗",
        }
        add(
            severity="error",
            scope="preopen_executor",
            code=f"preopen_{component}_failed",
            title=labels.get(str(component), "模擬執行器盤前驗證失敗"),
            detail=str(
                row.get("public_error_message")
                or "模擬執行器盤前守門未通過；不得將流程狀態當成成交。"
            ),
            observed_at=row.get("checked_at") or simulation.get("updated_at"),
        )

    reason_messages = {
        "official_session_no_trade_print": (
            "warning",
            "官方確認本日無成交價",
            "交易所日資料已有該證券列，但 OHLC 全空；本日沒有可用開盤成交價，已保持空倉。",
        ),
        "official_open_price_unavailable": (
            "error",
            "官方開盤價不可用",
            "歷史回補缺少有效官方開盤價，09:01 反事實計價已停止；不使用 Bid/Ask、最後價或 +1 Tick 替代。",
        ),
        "observed_09_01_minute_vwap_unavailable": (
            "error",
            "09:01 首分鐘成交價不可用",
            "舊版漏跑回補缺少右標記 09:01 首分鐘價格，該股票保持空倉；不以 09:00 開盤價、Bid/Ask、最後價或 +1 Tick 替代。",
        ),
        "observed_09_01_minute_price_unavailable": (
            "error",
            "09:01 首分鐘成交價不可用",
            "漏跑回補連來源 K 棒都沒有有效的右標記 09:01 價格，該股票保持空倉；只有缺 tick 時會改用同一根 K 棒價格。",
        ),
        "synthetic_open_tick_price_unavailable": (
            "error",
            "開盤價 Tick 價格不可用",
            "無法從開盤價與合法 Tick 計算成交價，未建立持倉。",
        ),
        "exact_session_eligibility_missing": (
            "error",
            "當日當沖資格缺漏",
            "缺少該交易日官方資格資料，已 fail-closed。",
        ),
        "price_limit_unavailable": (
            "error",
            "漲跌停價格缺漏",
            "缺少該交易日合法價格界線，未建立持倉。",
        ),
        "no_executable_best_quote": (
            "error",
            "最佳一檔報價不可用",
            "沒有可稽核的最佳買賣價，未建立持倉。",
        ),
        "quote_not_after_signal": (
            "error",
            "報價未晚於訊號",
            "報價不符合因果時間邊界，未建立持倉。",
        ),
        "quote_after_local_observation": (
            "error",
            "報價時間超前本機觀測",
            "報價時間戳不符合本機觀測邊界，未建立持倉。",
        ),
        "marketable_depth_unavailable": (
            "error",
            "可成交深度不可用",
            "沒有足夠的可驗證深度，未宣稱成交。",
        ),
    }
    for mode in modes:
        market = str(mode.get("market") or "") or None
        label = str(mode.get("label") or market or "模式")
        settlement = mode.get("manual_close_settlement")
        if isinstance(settlement, Mapping):
            last_prices = settlement.get("last_traded_prices") or []
            price_detail = "；".join(
                f"{p.get('symbol')}：{float(p.get('price') or 0):.2f} 元（價格日期 {p.get('price_date')}）"
                for p in last_prices
                if isinstance(p, Mapping)
            )
            basis = "收盤價／最後成交價" if last_prices else "官方收盤價"
            add(
                severity="warning",
                scope="mode",
                market=market,
                code="manual_official_close_settlement",
                title=f"{label} {'收盤價／最後成交價' if last_prices else '收盤價'}補登清算",
                detail=f"依使用者要求按{basis}清算 {int(settlement.get('settled_count') or 0)} 筆；這是模擬帳本補登，不是當時的券商或交易所成交。剩餘 {int(settlement.get('remaining_count') or 0)} 筆。"
                + (
                    f"最後成交價：{price_detail}；不是本日收盤成交。"
                    if last_prices
                    else ""
                ),
                observed_at=settlement.get("recorded_at"),
            )
        if mode.get("checkpoint_ready") is False:
            add(
                severity="error",
                scope="mode",
                market=market,
                code="checkpoint_not_ready",
                title=f"{label} 模型權重未就緒",
                detail="checkpoint 未通過載入與契約驗證，模式不可執行。",
                observed_at=mode.get("signal_at"),
            )
        engine_status = str(mode.get("engine_status") or "")
        if engine_status.startswith(("critical", "blocked")):
            execution_details = {
                "critical_day_trade_exit_unresolved": "當沖部位未能在當日沖銷；缺資料、容量不足或程式異常不得當成一般隔夜持倉。",
                "critical_adverse_limit_exception": "不利漲跌停且退出方向無對手量，保留例外證據；下一交易時段優先沖銷，並非券商已核准融資融券。",
                "critical_prior_inventory_liquidation": "前日殘餘部位優先沖銷，清理前不建立新的當沖曝險，不依新訊號留用舊庫存。",
                "critical_legacy_carry_requires_review": "本日帳本按舊規則留下殘餘；新規則已設定，未倒改本日訊號或成交。",
            }
            add(
                severity="error",
                scope="mode",
                market=market,
                code=engine_status,
                title=f"{label} 執行器已阻擋",
                detail=execution_details.get(
                    engine_status,
                    "執行器偵測到安全性或資料契約錯誤，未繼續建立新部位。",
                ),
                observed_at=mode.get("signal_at"),
            )
        open_position_count = int(mode.get("open_position_count") or 0)
        if market and market.startswith("tw_day_trade") and open_position_count:
            stale_position_count = int(mode.get("stale_position_count") or 0)
            force_exit_failures = int(mode.get("force_exit_failures") or 0)
            add(
                severity="error",
                scope="mode",
                market=market,
                code="intraday_residual_open",
                title=f"{label} 仍有當日殘餘部位",
                detail=(
                    f"仍有 {open_position_count} 筆模擬持倉未於當日清倉；"
                    f"其中 {stale_position_count} 筆估值已過期，"
                    f"強制退出失敗 {force_exit_failures} 次。"
                    f"執行器狀態：{engine_status or 'unknown'}。"
                    "這是必須優先沖銷的異常殘餘，不是正常隔夜持倉或券商成交保證。"
                ),
                count=open_position_count,
                observed_at=(
                    mode.get("residual_conversion_completed_at")
                    or mode.get("closing_auction_settled_at")
                    or mode.get("last_mark_at")
                ),
            )
        outcome = str(mode.get("today_execution_outcome") or "")
        if outcome in {"no_fill", "partial", "blocked"}:
            add(
                severity="error" if outcome in {"no_fill", "blocked"} else "warning",
                scope="entry_fill",
                market=market,
                code=f"entry_{outcome}",
                title=f"{label} 未完整成交",
                detail=(
                    "訊號流程已處理，但沒有建立任何成交部位。"
                    if outcome == "no_fill"
                    else "訊號流程已處理，但僅有部分目標股數建立部位。"
                    if outcome == "partial"
                    else "訊號流程遭安全守門阻擋，沒有成交。"
                ),
                observed_at=mode.get("today_execution_recorded_at"),
            )
        reasons = mode.get("signal_reason_counts")
        reasons = dict(reasons) if isinstance(reasons, Mapping) else {}
        # ``below_one_board_lot`` is an expected capital/lot conversion result,
        # not a service incident.  Its complete model row remains visible in
        # the signal table and execution audit instead of occupying the global
        # operational-warning surface.
        for reason, count in reasons.items():
            presentation = reason_messages.get(str(reason))
            if presentation is None or not int(count or 0):
                continue
            severity, title, detail = presentation
            add(
                severity=severity,
                scope="entry_reason",
                market=market,
                code=str(reason),
                title=f"{label}：{title}",
                detail=detail,
                count=int(count or 0),
                observed_at=mode.get("today_execution_recorded_at"),
            )
    severity_order = {"error": 0, "warning": 1, "info": 2}
    issues.sort(
        key=lambda row: (
            severity_order.get(str(row.get("severity")), 9),
            str(row.get("market") or ""),
            str(row.get("code") or ""),
        )
    )
    return issues


def _available_session_dates(
    *,
    root: Path,
    state: Mapping[str, Any],
    observed: datetime,
    include_ledger_dates: bool = True,
    include_benchmark_history_dates: bool = True,
    include_preopen_session: bool = False,
    ledger_filenames: tuple[str, ...] | None = None,
) -> list[str]:
    root = Path(root)
    display_session = (
        dashboard_session_clock(observed).get("display_session_date")
        if include_preopen_session
        else None
    )
    mode_dates = tuple(
        sorted(
            {
                str(raw_mode["session_date"])[:10]
                for raw_mode in (state.get("modes") or {}).values()
                if isinstance(raw_mode, Mapping) and raw_mode.get("session_date")
            }
        )
    )
    tracked_ledgers = (
        (
            ledger_filenames
            if ledger_filenames is not None
            else (
                "marks.jsonl",
                "signals.jsonl",
                "orders.jsonl",
                "fills.jsonl",
                "benchmark_marks.jsonl",
                "events.jsonl",
            )
        )
        if include_ledger_dates
        else ()
    )
    tracked_filenames = (
        *tracked_ledgers,
        *((BENCHMARK_HISTORY_FILENAME,) if include_benchmark_history_dates else ()),
    )

    def signature(path: Path) -> tuple[int, int, int, int] | None:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns

    position_history_dates = tuple(
        sorted(
            {path.parent.name for path in (root / "position_history").glob("*/*.json")}
        )
    )
    overnight_position_history_dates = tuple(
        sorted(
            {
                path.parent.name
                for path in (root / OVERNIGHT_POSITION_HISTORY_DIRNAME).glob("*/*.json")
            }
        )
    )
    overnight_history_path = root / OVERNIGHT_HISTORY_FILENAME
    cache_signature: tuple[Any, ...] = (
        mode_dates,
        tuple((filename, signature(root / filename)) for filename in tracked_filenames),
        position_history_dates,
        overnight_position_history_dates,
        signature(overnight_history_path),
        observed.astimezone(TAIPEI).date().isoformat(),
        display_session,
    )
    resolved_root = root.resolve()
    with _AVAILABLE_SESSION_DATES_CACHE_LOCK:
        cached = _AVAILABLE_SESSION_DATES_CACHE.get(resolved_root)
        if cached is not None and cached[0] == cache_signature:
            return list(cached[1])

    dates: set[str] = set()
    for raw_mode in (state.get("modes") or {}).values():
        if isinstance(raw_mode, Mapping) and raw_mode.get("session_date"):
            dates.add(str(raw_mode["session_date"])[:10])
    if include_ledger_dates:
        for filename in tracked_ledgers:
            # Core execution ledgers carry an explicit session_date and their
            # detail readers use that exact contract. Reuse the same compact
            # index instead of building a duplicate fallback index over every
            # row. events.jsonl is the sole retained legacy stream whose date
            # is derived from recorded_at in Taipei time.
            index = _ledger_session_index(
                root / filename,
                recorded_at_fallback=filename == "events.jsonl",
            )
            if index is not None:
                dates.update(index.spans)
    if include_benchmark_history_dates:
        benchmark_history = _benchmark_history_index(root)
        dates.update(benchmark_history.marks_by_session)
    for raw_date in position_history_dates:
        try:
            datetime_date.fromisoformat(raw_date)
        except ValueError:
            continue
        dates.add(raw_date)
    for raw_date in overnight_position_history_dates:
        try:
            datetime_date.fromisoformat(raw_date)
        except ValueError:
            continue
        dates.add(raw_date)
    if overnight_history_path.is_file():
        overnight_history = _object(overnight_history_path)
        if (
            overnight_history.get("product") == "tw_overnight"
            and overnight_history.get("simulation_only") is True
            and overnight_history.get("production_order_possible") is False
        ):
            for row in overnight_history.get("marks") or ():
                if not isinstance(row, Mapping):
                    continue
                raw_date = str(row.get("session_date") or "")[:10]
                try:
                    datetime_date.fromisoformat(raw_date)
                except ValueError:
                    continue
                dates.add(raw_date)
    if display_session:
        dates.add(display_session)
    elif not dates:
        # Empty ledgers still need a dated waiting view. Require the same
        # verified calendar rather than accepting a bare weekday guess.
        verified_session = dashboard_session_clock(observed).get("display_session_date")
        if verified_session:
            dates.add(verified_session)
    result = sorted(dates, reverse=True)
    with _AVAILABLE_SESSION_DATES_CACHE_LOCK:
        if len(_AVAILABLE_SESSION_DATES_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
            _AVAILABLE_SESSION_DATES_CACHE.pop(
                next(iter(_AVAILABLE_SESSION_DATES_CACHE))
            )
        _AVAILABLE_SESSION_DATES_CACHE[resolved_root] = (cache_signature, result)
    return list(result)


def _select_session_date(requested: str | None, available: list[str]) -> str:
    text = str(requested or "").strip()
    if text:
        try:
            datetime_date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid dashboard session date: {text}") from exc
        if text not in available:
            raise ValueError(f"dashboard session date is unavailable: {text}")
        return text
    if not available:
        raise ValueError("dashboard has no available session dates")
    return available[0]


def _select_session_range(
    *,
    start_date: str | None,
    end_date: str | None,
    session_date: str | None,
    available: list[str],
) -> tuple[str, str, list[str]]:
    """Resolve calendar boundaries over the actual retained trading sessions."""

    if not available:
        raise ValueError("dashboard has no available session dates")
    if session_date and not start_date and not end_date:
        selected_end = _select_session_date(session_date, available)
        selected_start = selected_end
    else:
        selected_end = str(end_date or session_date or start_date or available[0])
        selected_start = str(start_date or session_date or selected_end)
        try:
            datetime_date.fromisoformat(selected_start)
            datetime_date.fromisoformat(selected_end)
        except ValueError as exc:
            raise ValueError("invalid detail date range") from exc
    if selected_start > selected_end:
        raise ValueError("detail start_date must not be after end_date")
    selected_dates = sorted(
        value for value in available if selected_start <= value <= selected_end
    )
    return selected_start, selected_end, selected_dates


def _all_json_objects(path: Path):
    if not path.is_file():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                yield payload


def _native_ledger_frame(path: Path) -> tuple[Any, Any] | None:
    """Read a complete bounded ledger into a native columnar frame when safe."""

    source = Path(path)
    try:
        size = source.stat().st_size
    except FileNotFoundError:
        return None
    if not _COLUMNAR_LEDGER_MIN_BYTES <= size <= _COLUMNAR_LEDGER_MAX_BYTES:
        return None
    try:
        import polars as pl

        try:
            frame = pl.read_ndjson(
                source,
                infer_schema_length=1_000,
                rechunk=False,
            )
        except Exception:
            # A short-lived field may be null throughout the bounded inference
            # prefix and become typed later in the ledger.  One native
            # full-schema retry is still much cheaper than hundreds of
            # thousands of Python ``json.loads`` calls.
            frame = pl.read_ndjson(
                source,
                infer_schema_length=None,
                rechunk=False,
            )
    except Exception:
        return None
    return pl, frame


def _all_ledger_objects(path: Path):
    """Stream a complete production ledger through the native JSON reader.

    The all-history view is the one legitimate caller that needs every row.
    Decoding hundreds of MiB one Python ``json.loads`` call at a time leaves
    the GIL on the critical path.  Reuse the same bounded columnar regime as
    date-selected reads, but iterate the native frame instead of materializing
    a second ``list[dict]`` copy.  Small fixtures and schema variants retain the
    strict line decoder, so this changes neither source authority nor failure
    semantics.
    """

    source = Path(path)
    native = _native_ledger_frame(source)
    if native is not None:
        _, frame = native
        yield from frame.iter_rows(named=True)
        return
    # Optional native acceleration must not make a legacy mixed-schema ledger
    # less readable than the canonical stdlib decoder.
    yield from _all_json_objects(source)


def _downsample_chart_series(
    rows: list[dict[str, Any]], *, maximum_points: int
) -> list[dict[str, Any]]:
    """Preserve endpoints and local return extrema in a bounded curve."""

    rows.sort(key=lambda row: float(row["timestamp_seconds"]))
    if len(rows) <= maximum_points:
        return rows
    bucket_count = max(1, (maximum_points - 2) // 2)
    interior = rows[1:-1]
    output = [rows[0]]
    for bucket_index in range(bucket_count):
        start = len(interior) * bucket_index // bucket_count
        stop = len(interior) * (bucket_index + 1) // bucket_count
        bucket = interior[start:stop]
        if not bucket:
            continue
        extrema = {
            min(
                range(len(bucket)),
                key=lambda index: float(bucket[index].get("return_pct") or 0.0),
            ),
            max(
                range(len(bucket)),
                key=lambda index: float(bucket[index].get("return_pct") or 0.0),
            ),
        }
        output.extend(bucket[index] for index in sorted(extrema))
    output.append(rows[-1])
    return output[:maximum_points]


def build_dashboard_history_snapshot(
    *,
    state_dir: Path,
    range_key: str = "1d",
    start_date: str | datetime_date | None = None,
    end_date: str | datetime_date | None = None,
    maximum_points_per_series: int = 2_000,
    resolution: str = "sampled",
    history_encoding: str = "minute_columns_v1",
    use_memory_cache: bool = True,
    use_persistent_cache: bool = True,
    use_session_projection: bool = True,
    _session_projection_capture: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return cross-session strategy and total-return benchmark curves.

    Time windows are anchored to the newest retained observation rather than
    wall-clock time, so historical/replay ledgers remain inspectable.  Longer
    windows scan only the mark ledgers. Legacy sampled responses preserve
    extrema; resolution=1m transports every minute in compact numeric columns.
    """

    normalized_range = str(range_key or "1d").strip().lower()
    if normalized_range not in CHART_RANGE_SECONDS:
        raise ValueError(f"unsupported chart range: {range_key}")
    if not 100 <= int(maximum_points_per_series) <= 10_000:
        raise ValueError("maximum_points_per_series must be between 100 and 10000")
    if resolution not in {"sampled", "1m"}:
        raise ValueError("unsupported history resolution")
    if history_encoding not in {"minute_columns_v1", "minute_columns_v2"}:
        raise ValueError("unsupported history encoding")
    selected_start = (
        start_date
        if isinstance(start_date, datetime_date)
        else datetime_date.fromisoformat(str(start_date))
        if start_date
        else None
    )
    selected_end = (
        end_date
        if isinstance(end_date, datetime_date)
        else datetime_date.fromisoformat(str(end_date))
        if end_date
        else None
    )
    if selected_start is not None and selected_end is not None:
        if selected_start > selected_end:
            raise ValueError("history start_date must not be after end_date")
    explicit_dates = selected_start is not None or selected_end is not None
    root = Path(state_dir)
    try:
        state = _object(root / "state.json")
    except (OSError, ValueError, json.JSONDecodeError):
        state = {}
    is_overnight = str(state.get("product") or "") == "tw_overnight"
    product = str(state.get("product") or "tw_day_trade")
    session_projection_eligible = bool(
        use_session_projection
        and normalized_range == "all"
        and resolution == "1m"
        and history_encoding == "minute_columns_v2"
        and selected_start is None
        and selected_end is None
        and _history_session_projection_root(root) is not None
    )
    benchmark_source = root / BENCHMARK_HISTORY_FILENAME
    benchmark_projection = load_benchmark_projection(
        state_dir=root,
        source_path=benchmark_source,
        selected_sessions=(),
    )
    if benchmark_projection is not None and explicit_dates:
        selected_benchmark_sessions = [
            session_date
            for session_date in benchmark_projection.session_entries
            if (
                selected_start is None
                or session_date >= selected_start.isoformat()
            )
            and (selected_end is None or session_date <= selected_end.isoformat())
        ]
        selected_projection = load_benchmark_projection(
            state_dir=root,
            source_path=benchmark_source,
            selected_sessions=selected_benchmark_sessions,
        )
        benchmark_history = (
            _benchmark_index_from_projection(benchmark_source, selected_projection)
            if selected_projection is not None
            else _benchmark_history_index(root)
        )
    elif benchmark_projection is not None and session_projection_eligible:
        # The source head provides exact session fingerprints. Do not decode
        # 115k benchmark rows merely to decide that private chart shards remain
        # reusable after a canonical append.
        benchmark_history = _benchmark_index_from_projection(
            benchmark_source, benchmark_projection
        )
    else:
        benchmark_history = _benchmark_history_index(root)
    benchmark_origins = benchmark_history.origins
    deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
    canonical_benchmark_keys: set[tuple[str, str]] = set()
    marks_path = root / "marks.jsonl"
    live_benchmark_path = root / "benchmark_marks.jsonl"
    overnight_history_path = root / OVERNIGHT_HISTORY_FILENAME
    overnight_history = (
        _object(overnight_history_path)
        if is_overnight and overnight_history_path.is_file()
        else {}
    )
    overnight_history_valid = bool(
        overnight_history.get("product") == "tw_overnight"
        and overnight_history.get("simulation_only") is True
        and overnight_history.get("production_order_possible") is False
        and int(overnight_history.get("schema_version") or 0) == 1
    )
    overnight_history_marks = tuple(
        row
        for row in (overnight_history.get("marks") or ())
        if overnight_history_valid and isinstance(row, Mapping)
    )
    try:
        overnight_history_stat = overnight_history_path.stat()
        overnight_history_signature = (
            overnight_history_stat.st_dev,
            overnight_history_stat.st_ino,
            overnight_history_stat.st_size,
            overnight_history_stat.st_mtime_ns,
        )
    except FileNotFoundError:
        overnight_history_signature = None
    marks_recorded_at_fallback = False
    live_benchmark_recorded_at_fallback = False
    marks_index = None
    live_benchmark_index = None
    if explicit_dates or session_projection_eligible:
        marks_index = _ledger_session_index(
            marks_path, recorded_at_fallback=marks_recorded_at_fallback
        )
        if (
            marks_index is not None
            and not marks_index.spans
            and marks_path.stat().st_size > 0
        ):
            # Legacy fixtures/ledgers may omit session_date but retain a timestamp.
            # Production marks carry session_date, so its already-warm compact
            # index is reused without a second full-file scan.
            marks_recorded_at_fallback = True
            marks_index = _ledger_session_index(
                marks_path, recorded_at_fallback=marks_recorded_at_fallback
            )
        live_benchmark_index = _ledger_session_index(
            live_benchmark_path,
            recorded_at_fallback=live_benchmark_recorded_at_fallback,
        )
        if (
            live_benchmark_index is not None
            and not live_benchmark_index.spans
            and live_benchmark_path.stat().st_size > 0
        ):
            live_benchmark_recorded_at_fallback = True
            live_benchmark_index = _ledger_session_index(
                live_benchmark_path,
                recorded_at_fallback=live_benchmark_recorded_at_fallback,
            )
    available_session_dates = {
        *(marks_index.spans if marks_index is not None else ()),
        *benchmark_history.marks_by_session,
        *(live_benchmark_index.spans if live_benchmark_index is not None else ()),
        *(
            str(row.get("session_date") or "")[:10]
            for row in overnight_history_marks
            if row.get("session_date")
        ),
    }

    selected_sessions = (
        sorted(
            session_date
            for session_date in available_session_dates
            if (selected_start is None or session_date >= selected_start.isoformat())
            and (selected_end is None or session_date <= selected_end.isoformat())
        )
        if explicit_dates
        else []
    )

    def selected_span_signature(
        index: _LedgerSessionIndex | None, source: Path
    ) -> tuple[Any, ...]:
        if not explicit_dates:
            try:
                stat = source.stat()
            except FileNotFoundError:
                return ("missing",)
            return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        if index is None:
            return ("missing",)
        signed_sessions = set(selected_sessions)
        # Canonical ledgers are append-only between atomic replacements.  An
        # older selected session therefore keeps the same byte spans while the
        # current session grows.  Keying only its visible spans avoids reparsing
        # an old 270-point curve every minute; inode changes still invalidate
        # the cache after an atomic historical promotion.
        return (
            index.device,
            index.inode,
            tuple(
                (session_date, tuple(index.spans.get(session_date, ())))
                for session_date in sorted(signed_sessions)
            ),
        )

    history_cache_key = (
        root.resolve(),
        normalized_range,
        selected_start.isoformat() if selected_start else None,
        selected_end.isoformat() if selected_end else None,
        int(maximum_points_per_series),
        resolution,
        history_encoding,
        selected_span_signature(marks_index, marks_path),
        (
            benchmark_history.device,
            benchmark_history.inode,
            benchmark_history.size,
            benchmark_history.modified_ns,
        ),
        overnight_history_signature,
        selected_span_signature(live_benchmark_index, live_benchmark_path),
    )
    persistent_projection_eligible = bool(
        use_persistent_cache
        and normalized_range == "all"
        and resolution == "1m"
        and selected_start is None
        and selected_end is None
    )
    projection_source_fingerprint = _history_projection_source_fingerprint(
        {
            "dashboard_schema_version": DASHBOARD_SCHEMA_VERSION,
            "product": str(state.get("product") or "tw_day_trade"),
            "marks": selected_span_signature(marks_index, marks_path),
            "benchmark_history": (
                benchmark_history.device,
                benchmark_history.inode,
                benchmark_history.size,
                benchmark_history.modified_ns,
            ),
            "overnight_history": overnight_history_signature,
            "live_benchmark": selected_span_signature(
                live_benchmark_index, live_benchmark_path
            ),
        }
    )
    if use_memory_cache:
        with _HISTORY_SNAPSHOT_CACHE_LOCK:
            cached_history = _HISTORY_SNAPSHOT_CACHE.get(history_cache_key)
            if cached_history is not None:
                return dict(cached_history)
    if persistent_projection_eligible:
        persisted_history = _load_persistent_history_projection(
            root,
            source_fingerprint=projection_source_fingerprint,
            history_encoding=history_encoding,
        )
        if persisted_history is not None:
            if use_memory_cache:
                with _HISTORY_SNAPSHOT_CACHE_LOCK:
                    if (
                        len(_HISTORY_SNAPSHOT_CACHE)
                        >= _HISTORY_SNAPSHOT_CACHE_MAX_ENTRIES
                    ):
                        _HISTORY_SNAPSHOT_CACHE.pop(next(iter(_HISTORY_SNAPSHOT_CACHE)))
                    _HISTORY_SNAPSHOT_CACHE[history_cache_key] = persisted_history
            return dict(persisted_history)

    session_source_fingerprints: dict[str, str] | None = None
    session_source_context: dict[str, Any] | None = None
    session_source_dates: list[str] | None = None
    if session_projection_eligible:
        (
            session_source_fingerprints,
            session_source_context,
            session_source_dates,
        ) = _history_session_source_context(
            state_dir=root,
            product=product,
            benchmark_history=benchmark_history,
            benchmark_projection=benchmark_projection,
            marks_path=marks_path,
            marks_index=marks_index,
            live_benchmark_path=live_benchmark_path,
            live_benchmark_index=live_benchmark_index,
            overnight_history_marks=overnight_history_marks,
        )
        sharded = _load_or_rebuild_history_session_projection(
            state_dir=root,
            product=product,
            is_overnight=is_overnight,
            source_fingerprints=session_source_fingerprints,
            sources=session_source_context,
            available_session_dates=session_source_dates,
        )
        if sharded is not None:
            if persistent_projection_eligible:
                _persist_history_projection(
                    root,
                    source_fingerprint=projection_source_fingerprint,
                    history_encoding=history_encoding,
                    snapshot=sharded,
                )
            if use_memory_cache:
                with _HISTORY_SNAPSHOT_CACHE_LOCK:
                    if (
                        len(_HISTORY_SNAPSHOT_CACHE)
                        >= _HISTORY_SNAPSHOT_CACHE_MAX_ENTRIES
                    ):
                        _HISTORY_SNAPSHOT_CACHE.pop(
                            next(iter(_HISTORY_SNAPSHOT_CACHE))
                        )
                    _HISTORY_SNAPSHOT_CACHE[history_cache_key] = sharded
            return dict(sharded)
        if benchmark_projection is not None and benchmark_projection.session_entries:
            # No reusable private chart head exists yet (or too many sessions
            # changed). Bootstrap once from the complete verified source
            # projection, then publish all private session shards below.
            benchmark_history = _benchmark_history_index(root)
            benchmark_origins = benchmark_history.origins

    # Several accounts and all benchmarks share the same observed minute.
    # Convert its clock once; do not infer missing minutes or alter timezone rules.
    @lru_cache(maxsize=131_072)
    def observed_clock(raw_timestamp: str):
        # The same retained minute appears across every strategy and benchmark.
        # Parse its ISO timestamp once, rather than once per series and then
        # converting the same epoch back into a datetime for presentation.
        utc = _timestamp(raw_timestamp)
        timestamp_seconds = utc.timestamp()
        local = utc.astimezone(TAIPEI)
        return (
            timestamp_seconds,
            local.timetz().replace(tzinfo=None),
            utc.isoformat(timespec="minutes"),
            local.date().isoformat(),
        )

    def add_values(
        series_type: str,
        canonical: bool,
        market: object,
        benchmark_id: object,
        raw_minute: object,
        raw_recorded_at: object,
        initial_capital_value: object,
        total_equity_value: object,
        return_pct_value: object,
        return_fraction_value: object,
        valuation_stale: object,
        historical_minute_replay: object,
        historical_counterfactual_replay: object,
        minute_valuation_contract: object,
        valuation_source: object,
        valuation_executable: object,
        fresh_trade_position_count: object,
        last_trade_carried_position_count: object,
        missing_price_position_count: object,
        fresh_trade_notional_coverage_ratio: object,
    ) -> None:
        """Project only chart fields, independent of the source row container."""

        series_id = str(market if series_type == "strategy" else benchmark_id or "")
        if not series_id:
            return
        chart_clock = None
        for raw_timestamp in (raw_minute, raw_recorded_at):
            if not raw_timestamp:
                continue
            try:
                chart_clock = observed_clock(str(raw_timestamp))
                break
            except (TypeError, ValueError):
                continue
        if chart_clock is None:
            return
        timestamp_seconds, local_clock, minute, session_date = chart_clock
        # Full-history requests deliberately skip the separate byte-span index;
        # the rows already being projected are the cheapest exact authority for
        # their available trading dates.
        available_session_dates.add(session_date)
        if series_type == "strategy" and not (
            (
                local_clock.hour == 9
                and local_clock.minute == 0
                or local_clock.hour == 13
                and local_clock.minute == 30
            )
            if is_overnight
            else datetime_time(9, 1) <= local_clock <= datetime_time(13, 30)
        ):
            # Signal publication can happen during 09:00, but the canonical
            # right-labelled strategy curve is exactly 09:01..13:30.  Keeping
            # a 09:00 point for only the live session makes the latest day use
            # a different grain from every completed replay.
            return
        return_fraction, return_pct = _capital_return(
            initial_capital_value, total_equity_value
        )
        if return_pct is None:
            return_pct = _finite_float(return_pct_value)
            return_fraction = _finite_float(return_fraction_value)
        if return_pct is None:
            return
        if return_fraction is None:
            return_fraction = float(return_pct) / 100.0
        initial_capital = _finite_float(initial_capital_value)
        total_equity = _finite_float(total_equity_value)
        wealth_index = 1.0 + float(return_fraction)
        # A leveraged reference can truthfully cross zero. Dropping those rows
        # creates a false hole in the minute curve; only logarithmic display is
        # undefined, not the observed equity/PnL itself.
        if not math.isfinite(wealth_index):
            return
        if total_equity is None and initial_capital is not None:
            total_equity = initial_capital * wealth_index
        point_key = (series_id, minute)
        if not canonical and point_key in canonical_benchmark_keys:
            # A historical Close/reference mark and an old live Bid/Ask mark
            # are different valuation contracts. Do not weave both into one
            # completed curve according to incidental file-read order.
            return
        if canonical:
            canonical_benchmark_keys.add(point_key)
        deduplicated[(series_id, minute)] = {
            "series_id": series_id,
            "series_type": series_type,
            "market": market if series_type == "strategy" else None,
            "benchmark_id": benchmark_id if series_type == "benchmark" else None,
            "minute": minute,
            "session_date": session_date,
            "timestamp_seconds": timestamp_seconds,
            "return_fraction": return_fraction,
            "return_pct": return_pct,
            "cumulative_return_fraction": return_fraction,
            "cumulative_return_pct": return_pct,
            "_initial_capital_twd": initial_capital,
            "_total_equity_twd": total_equity,
            "_wealth_index": wealth_index,
            "valuation_stale": bool(valuation_stale),
            "historical_minute_replay": bool(
                historical_minute_replay or historical_counterfactual_replay
            ),
            "minute_valuation_contract": minute_valuation_contract,
            "valuation_source": valuation_source,
            "valuation_executable": valuation_executable,
            "fresh_trade_position_count": fresh_trade_position_count,
            "last_trade_carried_position_count": last_trade_carried_position_count,
            "missing_price_position_count": missing_price_position_count,
            "fresh_trade_notional_coverage_ratio": fresh_trade_notional_coverage_ratio,
        }

    projected_ledger_fields = (
        "market",
        "benchmark_id",
        "minute",
        "recorded_at",
        "initial_capital_twd",
        "total_equity_twd",
        "return_pct",
        "return_fraction",
        "valuation_stale",
        "historical_minute_replay",
        "historical_counterfactual_replay",
        "minute_valuation_contract",
        "valuation_source",
        "valuation_executable",
        "fresh_trade_position_count",
        "last_trade_carried_position_count",
        "missing_price_position_count",
        "fresh_trade_notional_coverage_ratio",
    )

    def add(
        source: Mapping[str, Any], *, series_type: str, canonical: bool = False
    ) -> None:
        # Explicit projection keeps the semantic path identical for canonical
        # history, live marks, legacy rows and small test fixtures.
        add_values(
            series_type,
            canonical,
            source.get("market"),
            source.get("benchmark_id"),
            source.get("minute"),
            source.get("recorded_at"),
            source.get("initial_capital_twd"),
            source.get("total_equity_twd"),
            source.get("return_pct"),
            source.get("return_fraction"),
            source.get("valuation_stale", False),
            source.get("historical_minute_replay", False),
            source.get("historical_counterfactual_replay", False),
            source.get("minute_valuation_contract"),
            source.get("valuation_source"),
            source.get("valuation_executable"),
            source.get("fresh_trade_position_count"),
            source.get("last_trade_carried_position_count"),
            source.get("missing_price_position_count"),
            source.get("fresh_trade_notional_coverage_ratio"),
        )

    def add_complete_ledger(path: Path, *, series_type: str) -> None:
        """Project a full ledger positionally before entering Python."""

        native = _native_ledger_frame(path)
        if native is None:
            for source in _all_json_objects(path) or ():
                add(source, series_type=series_type)
            return
        pl, frame = native
        expressions = [
            pl.col(field) if field in frame.columns else pl.lit(None).alias(field)
            for field in projected_ledger_fields
        ]
        for values in frame.select(expressions).iter_rows(named=False):
            add_values(series_type, False, *values)

    if explicit_dates:
        selected_date_set = set(selected_sessions)
        for row in overnight_history_marks:
            if str(row.get("session_date") or "")[:10] in selected_date_set:
                add(row, series_type="strategy")
        strategy_rows = _rows_for_sessions(
            marks_path,
            selected_sessions,
            None,
            recorded_at_fallback=marks_recorded_at_fallback,
        )
        for session_date in selected_sessions:
            for row in strategy_rows.get(session_date, ()):
                add(row, series_type="strategy")
        for session_date in selected_sessions:
            for row in benchmark_history.marks_by_session.get(session_date, ()):
                add(row, series_type="benchmark", canonical=True)
        live_benchmark_rows = _rows_for_sessions(
            live_benchmark_path,
            selected_sessions,
            None,
            recorded_at_fallback=live_benchmark_recorded_at_fallback,
        )
        for session_date in selected_sessions:
            for source in live_benchmark_rows.get(session_date, ()):
                benchmark_id = str(source.get("benchmark_id") or "")
                add(
                    _rebase_live_benchmark(source, benchmark_origins.get(benchmark_id)),
                    series_type="benchmark",
                )

    else:
        for row in overnight_history_marks:
            add(row, series_type="strategy")
        add_complete_ledger(marks_path, series_type="strategy")
        for row in benchmark_history.marks:
            add(row, series_type="benchmark", canonical=True)
        for source in _all_ledger_objects(live_benchmark_path) or ():
            benchmark_id = str(source.get("benchmark_id") or "")
            add(
                _rebase_live_benchmark(source, benchmark_origins.get(benchmark_id)),
                series_type="benchmark",
            )

    rows = sorted(
        deduplicated.values(),
        key=lambda row: (float(row["timestamp_seconds"]), str(row["series_id"])),
    )
    available_dates = [
        datetime_date.fromisoformat(session_date)
        for session_date in available_session_dates
    ]
    if selected_start is not None or selected_end is not None:
        rows = [
            row
            for row in rows
            if (
                selected_start is None
                or datetime.fromtimestamp(
                    float(row["timestamp_seconds"]), tz=timezone.utc
                )
                .astimezone(TAIPEI)
                .date()
                >= selected_start
            )
            and (
                selected_end is None
                or datetime.fromtimestamp(
                    float(row["timestamp_seconds"]), tz=timezone.utc
                )
                .astimezone(TAIPEI)
                .date()
                <= selected_end
            )
        ]
    anchor = max((float(row["timestamp_seconds"]) for row in rows), default=None)
    duration = CHART_RANGE_SECONDS[normalized_range]
    cutoff = (
        None
        if selected_start is not None
        or selected_end is not None
        or anchor is None
        or duration is None
        else anchor - duration
    )
    if cutoff is not None:
        rows = [row for row in rows if float(row["timestamp_seconds"]) >= cutoff]

    historical_rows = [row for row in rows if row["historical_minute_replay"]]
    fresh_coverage = [
        value
        for row in historical_rows
        if (value := _finite_float(row.get("fresh_trade_notional_coverage_ratio")))
        is not None
    ]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["series_id"]), []).append(row)
    range_summary: list[dict[str, Any]] = []
    for series_id, series_rows in sorted(grouped.items()):
        # Grouping preserves the already sorted global chronology.
        first = series_rows[0]
        last = series_rows[-1]
        baseline_wealth = float(first["_wealth_index"])
        initial_capital = _finite_float(first.get("_initial_capital_twd"))
        baseline_equity = _finite_float(first.get("_total_equity_twd"))
        end_equity = _finite_float(last.get("_total_equity_twd"))
        if baseline_equity is None and initial_capital is not None:
            baseline_equity = initial_capital * baseline_wealth
        if end_equity is None and initial_capital is not None:
            end_equity = initial_capital * float(last["_wealth_index"])
        cumulative_net_pnl = (
            end_equity - initial_capital
            if end_equity is not None and initial_capital is not None
            else None
        )
        # The chart is a normalized view over the visible selection: each
        # series starts at exactly 0%.  Its underlying cumulative equity and
        # P&L remain untouched and continue from the original account capital.
        # A reference that has crossed zero cannot use a return ratio, so keep
        # the existing finite wealth-index difference fallback in that case.
        for row in series_rows:
            wealth = float(row["_wealth_index"])
            selected_return_fraction = (
                wealth / baseline_wealth - 1.0
                if baseline_wealth > 0.0
                else wealth - baseline_wealth
            )
            row["return_fraction"] = selected_return_fraction
            row["return_pct"] = selected_return_fraction * 100.0
        period_return_fraction = float(last["return_fraction"])
        period_return_pct = float(period_return_fraction) * 100.0
        session_point_counts: dict[str, int] = {}
        for row in series_rows:
            session = str(row["session_date"])
            session_point_counts[session] = session_point_counts.get(session, 0) + 1
        # The strategy is 09:01..13:30 because there is deliberately no
        # fabricated 09:00 position. Cash benchmarks retain the 09:00 official
        # open, while the right-labelled TX day session covers 08:46..13:45.
        points_per_session = (
            2
            if is_overnight and first["series_type"] == "strategy"
            else 270
            if first["series_type"] == "strategy"
            else 300
            if series_id == "benchmark_tx_continuous"
            else 271
        )
        expected_minute_points = points_per_session * len(session_point_counts)
        range_summary.append(
            {
                "series_id": series_id,
                "series_type": first["series_type"],
                "baseline_kind": "first_visible_mark",
                "baseline_at_utc": first["minute"],
                "baseline_equity_twd": baseline_equity,
                "initial_capital_twd": initial_capital,
                "start_at_utc": first["minute"],
                "end_at_utc": last["minute"],
                "start_equity_twd": _finite_float(first.get("_total_equity_twd")),
                "end_equity_twd": end_equity,
                "range_net_pnl_twd": (
                    end_equity - baseline_equity
                    if end_equity is not None and baseline_equity is not None
                    else None
                ),
                "return_fraction": last["return_fraction"],
                "return_pct": last["return_pct"],
                "cumulative_net_pnl_twd": cumulative_net_pnl,
                "cumulative_return_fraction": last["cumulative_return_fraction"],
                "cumulative_return_pct": last["cumulative_return_pct"],
                "period_return_fraction": period_return_fraction,
                "period_return_pct": period_return_pct,
                "point_count": len(series_rows),
                "session_point_counts": session_point_counts,
                "expected_minute_points": expected_minute_points,
                "expected_points_per_session": points_per_session,
                "minute_coverage_ratio": (
                    len(series_rows) / expected_minute_points
                    if expected_minute_points
                    else None
                ),
            }
        )
    sampled = (
        rows
        if resolution == "1m"
        else [
            row
            for series_rows in grouped.values()
            for row in _downsample_chart_series(
                series_rows, maximum_points=int(maximum_points_per_series)
            )
        ]
    )
    if resolution != "1m":
        sampled.sort(
            key=lambda row: (float(row["timestamp_seconds"]), row["series_id"])
        )
        for row in sampled:
            for internal_key in (
                "timestamp_seconds",
                "_initial_capital_twd",
                "_total_equity_twd",
                "_wealth_index",
            ):
                row.pop(internal_key, None)
    coverage_start = sampled[0]["minute"] if sampled else None
    coverage_end = sampled[-1]["minute"] if sampled else None
    payload = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "simulation_only": True,
        "production_order_possible": False,
        "range": normalized_range,
        "range_seconds": duration,
        "start_date": selected_start.isoformat() if selected_start else None,
        "end_date": selected_end.isoformat() if selected_end else None,
        "available_start_date": min(available_dates).isoformat()
        if available_dates
        else None,
        "available_end_date": max(available_dates).isoformat()
        if available_dates
        else None,
        "anchor_at_utc": (
            datetime.fromtimestamp(anchor, tz=timezone.utc).isoformat()
            if anchor is not None
            else None
        ),
        "coverage_start_utc": coverage_start,
        "coverage_end_utc": coverage_end,
        "raw_points_in_range": len(rows),
        "returned_points": len(sampled),
        "downsampled": len(sampled) < len(rows),
        "curve_granularity": "auction_events" if is_overnight else "1m",
        "history_contract": (
            "13:30 official close and next-session 09:00 official open "
            "counterfactual events; no intraminute interpolation or exchange fill claim"
            if is_overnight
            else "right_labelled_one_minute"
        ),
        "expected_right_labelled_session_minute_points": 270,
        "expected_strategy_session_points_from_09_01": 270,
        "expected_stock_benchmark_session_points_including_09_00": 271,
        "expected_tx_day_session_points": 300,
        "expected_overnight_auction_event_points": 2 if is_overnight else None,
        "return_basis": "selected_range_first_visible_mark",
        "cumulative_return_basis": "initial_capital_cumulative_total_equity",
        "period_return_basis": "selected_range_first_visible_mark",
        "range_summary": range_summary,
        "historical_minute_replay_points": len(historical_rows),
        "historical_minute_carried_price_points": sum(
            int(row.get("last_trade_carried_position_count") or 0) > 0
            for row in historical_rows
        ),
        "historical_minute_missing_price_points": sum(
            int(row.get("missing_price_position_count") or 0) > 0
            for row in historical_rows
        ),
        "historical_minute_min_fresh_trade_notional_coverage_ratio": (
            min(fresh_coverage) if fresh_coverage else None
        ),
        "historical_minute_mean_fresh_trade_notional_coverage_ratio": (
            sum(fresh_coverage) / len(fresh_coverage) if fresh_coverage else None
        ),
        "historical_minute_valuation_contracts": sorted(
            {
                str(value)
                for row in historical_rows
                if (value := row.get("minute_valuation_contract"))
            }
        ),
        "historical_minute_valuation_sources": sorted(
            {
                str(value)
                for row in historical_rows
                if (value := row.get("valuation_source"))
            }
        ),
        "history": sampled,
    }
    if resolution == "1m" and history_encoding == "minute_columns_v1":
        # Lossless columnar transport avoids repeating IDs, timestamps and
        # field names hundreds of thousands of times. Each row is an actual
        # retained minute; the browser must not reconstruct interpolated rows.
        payload["history_encoding"] = "minute_columns_v1"
        payload["minute_series"] = [
            {
                "series_id": series_id,
                "series_type": values[0]["series_type"],
                "points": [
                    [
                        int(row["timestamp_seconds"] // 60),
                        row["return_pct"],
                        row["cumulative_return_pct"],
                        int(row["valuation_stale"])
                        | (int(row["historical_minute_replay"]) << 1)
                        | (
                            int(int(row.get("missing_price_position_count") or 0) > 0)
                            << 2
                        ),
                    ]
                    for row in values
                ],
            }
            for series_id, values in grouped.items()
        ]
        payload["history"] = []
    elif resolution == "1m":
        # V2 stores each observed epoch-minute once, then uses per-series
        # indexes and parallel numeric columns. It remains lossless while the
        # browser can feed the values directly to a Canvas renderer.
        minute_axis = sorted(
            {int(row["timestamp_seconds"] // 60) for row in sampled}
        )
        minute_index = {minute: index for index, minute in enumerate(minute_axis)}
        payload["history_encoding"] = "minute_columns_v2"
        payload["minute_axis"] = minute_axis
        payload["minute_series"] = [
            {
                "series_id": series_id,
                "series_type": values[0]["series_type"],
                "minute_indexes": [
                    minute_index[int(row["timestamp_seconds"] // 60)]
                    for row in values
                ],
                "return_pct": [row["return_pct"] for row in values],
                "cumulative_return_pct": [
                    row["cumulative_return_pct"] for row in values
                ],
                "quality_flags": [
                    int(row["valuation_stale"])
                    | (int(row["historical_minute_replay"]) << 1)
                    | (
                        int(
                            int(row.get("missing_price_position_count") or 0) > 0
                        )
                        << 2
                    )
                    for row in values
                ],
            }
            for series_id, values in grouped.items()
        ]
        payload["history"] = []
    if _session_projection_capture is not None:
        partitioned = _partition_history_rows_by_session(grouped)
        for session_date, capture in list(_session_projection_capture.items()):
            source_fingerprint = str(capture.get("source_fingerprint") or "")
            _session_projection_capture[session_date] = _session_projection_payload(
                session_date=session_date,
                source_fingerprint=source_fingerprint,
                grouped=partitioned.get(session_date, {}),
                is_overnight=is_overnight,
            )
    if (
        session_projection_eligible
        and session_source_fingerprints is not None
        and session_source_context is not None
        and session_source_dates is not None
    ):
        partitioned = _partition_history_rows_by_session(grouped)
        shards = [
            _session_projection_payload(
                session_date=session_date,
                source_fingerprint=session_source_fingerprints[session_date],
                grouped=partitioned.get(session_date, {}),
                is_overnight=is_overnight,
            )
            for session_date in session_source_dates
        ]
        session_entries: dict[str, Mapping[str, Any]] = {}
        # First-time materialization is independent by content-addressed path.
        # Bounded parallel compression/fsync removes a serial 137-file latency
        # staircase without changing the read-only request or source ledgers.
        with ThreadPoolExecutor(
            max_workers=min(8, max(1, len(shards))),
            thread_name_prefix="history-shard",
        ) as executor:
            entries = list(
                executor.map(
                    lambda shard: _persist_history_session_shard(root, shard),
                    shards,
                )
            )
        for shard, entry in zip(shards, entries, strict=True):
            if entry is None:
                session_entries = {}
                break
            session_entries[str(shard["session_date"])] = entry
        if len(session_entries) == len(session_source_dates):
            _persist_history_session_head(
                root,
                product=product,
                sources=session_source_context,
                available_session_dates=session_source_dates,
                sessions=session_entries,
            )
    if persistent_projection_eligible:
        _persist_history_projection(
            root,
            source_fingerprint=projection_source_fingerprint,
            history_encoding=history_encoding,
            snapshot=payload,
        )
    if use_memory_cache:
        with _HISTORY_SNAPSHOT_CACHE_LOCK:
            if len(_HISTORY_SNAPSHOT_CACHE) >= _HISTORY_SNAPSHOT_CACHE_MAX_ENTRIES:
                _HISTORY_SNAPSHOT_CACHE.pop(next(iter(_HISTORY_SNAPSHOT_CACHE)))
            _HISTORY_SNAPSHOT_CACHE[history_cache_key] = payload
    return dict(payload)


def _preopen_progress(
    *,
    path: Path | None,
    modes: list[dict[str, Any]],
    observed: datetime,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if path is not None and Path(path).is_file():
        try:
            payload = _object(Path(path))
        except (OSError, ValueError, json.JSONDecodeError):
            payload = {}
    updated_at = payload.get("updated_at")
    source_age_seconds: float | None = None
    same_trading_date = False
    if updated_at:
        try:
            updated = _timestamp(updated_at)
            source_age_seconds = max(0.0, (observed - updated).total_seconds())
            same_trading_date = (
                updated.astimezone(TAIPEI).date() == observed.astimezone(TAIPEI).date()
            )
        except (TypeError, ValueError):
            pass

    raw_markets = payload.get("markets") if same_trading_date else {}
    if not isinstance(raw_markets, Mapping):
        raw_markets = {}
    rows: list[dict[str, Any]] = []
    session_date = observed.astimezone(TAIPEI).date().isoformat()
    local_time = observed.astimezone(TAIPEI).time()
    final_arm_current_process_required = (
        datetime_time(8, 55) <= local_time < datetime_time(9, 0)
    )
    process_run_id = str(payload.get("run_id") or "")
    for mode in modes:
        market = str(mode.get("market") or "")
        signal_market = str(mode.get("signal_market") or market)
        reuses_signal = signal_market != market
        raw = raw_markets.get(signal_market)
        item = dict(raw) if isinstance(raw, Mapping) else {}
        status = str(item.get("status") or "pending")
        step = max(0, int(item.get("step") or 0))
        total = max(0, int(item.get("total") or 0))
        if status == "ready":
            progress_ratio = 1.0
        elif status == "failed":
            progress_ratio = 1.0
        elif total:
            progress_ratio = _ratio(step, total)
        else:
            progress_ratio = 0.0
        elapsed_seconds = _finite_float(item.get("elapsed_seconds"))
        if elapsed_seconds is None and item.get("started_at"):
            end = (
                item.get("completed_at") if status in {"ready", "failed"} else observed
            )
            elapsed_seconds = _seconds_between(item.get("started_at"), end)
        symbol_count = int(item.get("symbol_count") or 0)
        latency = item.get("live_latency")
        latency = dict(latency) if isinstance(latency, Mapping) else {}
        final_arm = item.get("final_arm")
        final_arm = dict(final_arm) if isinstance(final_arm, Mapping) else {}
        final_arm_latency = final_arm.get("live_latency")
        final_arm_latency = (
            dict(final_arm_latency) if isinstance(final_arm_latency, Mapping) else {}
        )
        final_arm_quote_prewarm = final_arm.get("quote_prewarm")
        final_arm_quote_prewarm = (
            dict(final_arm_quote_prewarm)
            if isinstance(final_arm_quote_prewarm, Mapping)
            else {}
        )
        final_arm_requested = int(final_arm_quote_prewarm.get("requested_count") or 0)
        final_arm_run_id = str(final_arm.get("run_id") or "")
        final_arm_contract_ready = bool(
            final_arm.get("status") == "ready"
            and final_arm_run_id
            and _is_taipei_session_date(final_arm.get("completed_at"), session_date)
            and final_arm_latency.get("panel_cache_hit") is True
            and final_arm_latency.get("checkpoint_cache_hit") is True
            and final_arm_latency.get("model_cache_hit") is True
            and final_arm_quote_prewarm.get("ready") is True
            and final_arm_quote_prewarm.get("run_id") == final_arm_run_id
            and final_arm_quote_prewarm.get("connection_scope") == "process"
            and final_arm_requested > 0
            and int(final_arm_quote_prewarm.get("primed_count") or 0)
            == final_arm_requested
            and int(final_arm_quote_prewarm.get("resolved_count") or 0)
            == final_arm_requested
            and int(final_arm_quote_prewarm.get("missing_count") or 0) == 0
        )
        final_arm_hot_ready = bool(
            final_arm_contract_ready
            and process_run_id
            and final_arm_run_id == process_run_id
        )
        preparation_status = status
        recovered_signal_id = str(mode.get("signal_id") or "")
        recovered_at = mode.get("entry_completed_at")
        recovered_late = bool(
            status in {"failed", "pending"}
            and str(mode.get("engine_status") or "") in {"active", "completed"}
            and str(mode.get("session_date") or "") == session_date
            and recovered_signal_id
            and _is_taipei_session_date(recovered_at, session_date)
        )
        if recovered_late:
            # Preserve the failed/pending preparation stage while reflecting
            # the newer durable engine fact. A mode deployed after the morning
            # receipt is necessarily pending there, but its same-session
            # signal/entry is still a truthful late recovery. The separate
            # opening gate keeps the missed 09:00 SLO visible.
            status = "recovered_late"
        if (
            final_arm_current_process_required
            and status == "ready"
            and not final_arm_hot_ready
        ):
            status = "pending"
        inference_ms = _finite_float(latency.get("model_inference_ms"))
        price_limits = item.get("preopen_price_limits")
        price_limits = dict(price_limits) if isinstance(price_limits, Mapping) else {}
        same_session = item.get("same_session_eligibility")
        same_session = dict(same_session) if isinstance(same_session, Mapping) else {}
        rule_venues = same_session.get("venues")
        rule_venues = dict(rule_venues) if isinstance(rule_venues, Mapping) else {}
        requested = int(price_limits.get("requested_count") or 0)
        prepared = int(price_limits.get("prepared_count") or 0)
        rate = (
            symbol_count / elapsed_seconds
            if symbol_count and elapsed_seconds and elapsed_seconds > 0.0
            else None
        )
        inference_rate = (
            symbol_count / (inference_ms / 1000.0)
            if symbol_count and inference_ms and inference_ms > 0.0
            else None
        )
        rows.append(
            {
                "market": market,
                "label": mode.get("label") or market,
                "signal_market": signal_market,
                "reuses_signal": reuses_signal,
                "status": status,
                "preparation_status": preparation_status,
                "recovered_late": recovered_late,
                "recovered_signal_id": (
                    recovered_signal_id if recovered_late else None
                ),
                "recovered_at": recovered_at if recovered_late else None,
                "progress_ratio": round(progress_ratio, 6),
                "step": step or None,
                "total": total or None,
                "message": (
                    f"重用 {signal_market} 已準備的訊號與模型，不重複推論"
                    if reuses_signal and status == "ready"
                    else item.get("message")
                ),
                "started_at": item.get("started_at"),
                "completed_at": item.get("completed_at"),
                "elapsed_seconds": (
                    round(elapsed_seconds, 3) if elapsed_seconds is not None else None
                ),
                "panel_date": item.get("panel_date"),
                "symbol_count": symbol_count or None,
                "symbols_per_second": round(rate, 3) if rate is not None else None,
                "model_inference_ms": inference_ms,
                "model_symbols_per_second": (
                    round(inference_rate, 3) if inference_rate is not None else None
                ),
                "compute_before_publish_ms": _finite_float(
                    latency.get("compute_before_publish_ms")
                ),
                "checkpoint_cache_hit": latency.get("checkpoint_cache_hit"),
                "model_cache_hit": latency.get("model_cache_hit"),
                "panel_cache_hit": latency.get("panel_cache_hit"),
                "final_arm_status": final_arm.get("status"),
                "final_arm_contract_ready": final_arm_contract_ready,
                "final_arm_current_process_required": (
                    final_arm_current_process_required
                ),
                "final_arm_hot_ready": final_arm_hot_ready,
                "final_arm_completed_at": final_arm.get("completed_at"),
                "final_arm_elapsed_seconds": _finite_float(
                    final_arm.get("elapsed_seconds")
                ),
                "final_arm_attempts": int(final_arm.get("attempts") or 0),
                "final_arm_panel_cache_hit": final_arm_latency.get("panel_cache_hit"),
                "final_arm_checkpoint_cache_hit": final_arm_latency.get(
                    "checkpoint_cache_hit"
                ),
                "final_arm_model_cache_hit": final_arm_latency.get("model_cache_hit"),
                "final_arm_quote_ready": final_arm_quote_prewarm.get("ready"),
                "final_arm_quote_connection_scope": final_arm_quote_prewarm.get(
                    "connection_scope"
                ),
                "final_arm_quote_requested": int(
                    final_arm_quote_prewarm.get("requested_count") or 0
                ),
                "final_arm_quote_primed": int(
                    final_arm_quote_prewarm.get("primed_count") or 0
                ),
                "final_arm_quote_resolved": int(
                    final_arm_quote_prewarm.get("resolved_count") or 0
                ),
                "final_arm_quote_missing": int(
                    final_arm_quote_prewarm.get("missing_count") or 0
                ),
                "final_arm_quote_snapshot_prefetched": final_arm_quote_prewarm.get(
                    "snapshot_prefetched"
                ),
                "final_arm_mis_fallback_ready": (
                    dict(final_arm.get("tw_mis_fallback_prewarm") or {}).get("ready")
                    if isinstance(final_arm.get("tw_mis_fallback_prewarm"), Mapping)
                    else None
                ),
                "final_arm_compute_ms": _finite_float(
                    final_arm_latency.get("compute_before_publish_ms")
                ),
                "final_arm_error": final_arm.get("error"),
                "final_arm_public_error_code": (
                    "final_arm_failed" if final_arm.get("error") else None
                ),
                "final_arm_public_error_message": (
                    "08:55 最後武裝驗證失敗；快取或模型尚未達到 HOT READY。"
                    if final_arm.get("error")
                    else None
                ),
                "price_limit_prepared": prepared,
                "price_limit_requested": requested,
                "price_limit_coverage_ratio": _ratio(prepared, requested),
                "price_limit_missing": int(price_limits.get("missing_count") or 0),
                "eligibility_target_date": same_session.get("target_date"),
                "eligibility_coverage": rule_venues,
                "eligibility_ready": bool(rule_venues)
                and all(
                    bool(dict(value).get("covered"))
                    for value in rule_venues.values()
                    if isinstance(value, Mapping)
                ),
                "preparation_error": item.get("error"),
                "error": None if recovered_late else item.get("error"),
                "public_error_code": (
                    "preopen_recovered_late"
                    if recovered_late
                    else "preopen_data_update_failed"
                    if status == "failed"
                    else None
                ),
                "public_error_message": (
                    "盤前準備曾失敗，但今日訊號與模擬帳本已耐久提交；"
                    "09:00 準時性事故仍保留於 opening gate。"
                    if recovered_late
                    else "盤前公開資料、特徵或模型準備失敗；請查看此模式並等待重新驗證。"
                    if status == "failed"
                    else None
                ),
            }
        )

    ready_count = sum(row["status"] in {"ready", "recovered_late"} for row in rows)
    recovered_count = sum(row["status"] == "recovered_late" for row in rows)
    failed_count = sum(row["status"] == "failed" for row in rows)
    terminal_count = ready_count + failed_count
    running_count = sum(row["status"] == "running" for row in rows)
    starts = [row["started_at"] for row in rows if row.get("started_at")]
    ends = [row["completed_at"] for row in rows if row.get("completed_at")]
    wall_elapsed = None
    if starts and ends:
        wall_elapsed = _seconds_between(min(starts), max(ends))
    overall_status = (
        "failed"
        if failed_count
        else "recovered_late"
        if recovered_count
        else "ready"
        if rows and ready_count == len(rows)
        else "running"
        if running_count or (same_trading_date and terminal_count)
        else "pending"
    )
    return {
        "status": overall_status,
        "updated_at": updated_at if same_trading_date else None,
        "source_age_seconds": (
            round(source_age_seconds, 3)
            if same_trading_date and source_age_seconds is not None
            else None
        ),
        "ready_count": ready_count,
        "recovered_count": recovered_count,
        "failed_count": failed_count,
        "running_count": running_count,
        "completed_count": terminal_count,
        "total_count": len(rows),
        "progress_ratio": _ratio(terminal_count, len(rows)),
        "wall_elapsed_seconds": round(wall_elapsed, 3)
        if wall_elapsed is not None
        else None,
        "modes_per_minute": (
            round(terminal_count * 60.0 / wall_elapsed, 3)
            if terminal_count and wall_elapsed and wall_elapsed > 0.0
            else None
        ),
        "markets": rows,
        "source_path": str(path) if path is not None else None,
    }


def _simulation_preopen_progress(*, path: Path, observed: datetime) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if path.is_file():
        try:
            payload = _object(path)
        except (OSError, ValueError, json.JSONDecodeError):
            payload = {}
    session_date = observed.astimezone(TAIPEI).date().isoformat()
    if payload.get("session_date") != session_date:
        return {
            "status": "pending",
            "ready": False,
            "session_date": session_date,
            "updated_at": None,
            "components": {},
            "source_path": str(path),
        }
    components = payload.get("components")
    components = dict(components) if isinstance(components, Mapping) else {}
    safe_components: dict[str, dict[str, Any]] = {}
    for name in ("eligibility", "shioaji_quote"):
        raw = components.get(name)
        row = dict(raw) if isinstance(raw, Mapping) else {}
        details = row.get("details")
        details = dict(details) if isinstance(details, Mapping) else {}
        safe_components[name] = {
            "status": row.get("status") or "pending",
            "checked_at": row.get("checked_at"),
            "elapsed_ms": _finite_float(row.get("elapsed_ms")),
            "proof": details.get("proof"),
            "symbol_count": int(details.get("symbol_count") or 0) or None,
            "error": row.get("error"),
            "public_error_code": (
                f"preopen_{name}_failed" if row.get("status") == "failed" else None
            ),
            "public_error_message": (
                "盤前執行器驗證失敗；此守門項目尚未就緒。"
                if row.get("status") == "failed"
                else None
            ),
        }
    ready = bool(
        payload.get("status") == "ready"
        and all(row.get("status") == "ready" for row in safe_components.values())
    )
    return {
        "status": payload.get("status") or "pending",
        "ready": ready,
        "session_date": session_date,
        "updated_at": payload.get("updated_at"),
        "components": safe_components,
        "source_path": str(path),
    }


def _session_progress(
    *,
    observed: datetime,
    mode_count: int,
    modes: list[dict[str, Any]],
    marks: list[dict[str, Any]],
) -> dict[str, Any]:
    local = observed.astimezone(TAIPEI)
    day = local.date()

    def at(hour: int, minute: int) -> datetime:
        return datetime.combine(day, datetime_time(hour, minute), tzinfo=TAIPEI)

    preopen_at = at(8, 15)
    signal_at = at(9, 0)
    exit_limit_at = at(13, 20)
    force_exit_at = at(13, 24)
    closing_auction_at = at(13, 25)
    session_end_at = at(13, 30)
    if local < preopen_at:
        phase = "waiting_prewarm"
        label = "等待 08:15 預熱"
        phase_start, phase_end = at(0, 0), preopen_at
        next_label, next_at = "開始預熱", preopen_at
    elif local < signal_at:
        phase = "preopen"
        label = "盤前預熱"
        phase_start, phase_end = preopen_at, signal_at
        next_label, next_at = "09:00 訊號閘門", signal_at
    elif local < exit_limit_at:
        phase = "active"
        label = "盤中每分鐘估值"
        phase_start, phase_end = signal_at, exit_limit_at
        next_label, next_at = "13:20 限價退出", exit_limit_at
    elif local < force_exit_at:
        phase = "exit_limit"
        label = "13:20 限價退出"
        phase_start, phase_end = exit_limit_at, force_exit_at
        next_label, next_at = "13:24 市價強平", force_exit_at
    elif local < closing_auction_at:
        phase = "force_exit"
        label = "13:24 市價強平重試"
        phase_start, phase_end = force_exit_at, closing_auction_at
        next_label, next_at = "13:25 收盤集合競價", closing_auction_at
    elif local < session_end_at:
        phase = "closing_auction"
        label = "13:25 收盤集合競價"
        phase_start, phase_end = closing_auction_at, session_end_at
        next_label, next_at = "13:30 撮合／帳務完成", session_end_at
    else:
        phase = "complete"
        label = "本日流程結束"
        phase_start, phase_end = signal_at, session_end_at
        next_label, next_at = "已完成", session_end_at

    signal_completed = sum(bool(mode.get("today_execution_terminal")) for mode in modes)
    entry_completed = signal_completed
    exit_started = sum(
        _is_taipei_session_date(mode.get("exit_limit_submitted_at"), day.isoformat())
        or _is_taipei_session_date(mode.get("force_exit_started_at"), day.isoformat())
        or _is_taipei_session_date(
            mode.get("closing_auction_submitted_at"), day.isoformat()
        )
        for mode in modes
    )
    unique_mode_minutes = {
        (str(row.get("market")), str(row.get("minute")))
        for row in marks
        if row.get("market") and row.get("minute")
    }
    elapsed_active_minutes = 0
    if local >= signal_at:
        elapsed_active_minutes = max(
            0,
            min(
                int((min(local, force_exit_at) - signal_at).total_seconds() // 60) + 1,
                int((force_exit_at - signal_at).total_seconds() // 60) + 1,
            ),
        )
    expected_mode_marks = elapsed_active_minutes * max(0, mode_count)
    mark_tracking_completed_modes = sum(
        str(mode.get("session_date") or "") == day.isoformat()
        and int(mode.get("open_position_count") or 0) == 0
        and _is_taipei_session_date(
            mode.get("residual_conversion_completed_at"), day.isoformat()
        )
        for mode in modes
    )
    mark_tracking_complete = bool(
        local >= session_end_at
        and mode_count > 0
        and mark_tracking_completed_modes == mode_count
    )
    # Once every mode is durably flat, later flat-forward rows carry no new
    # valuation information.  Do not compare the observed curve against a
    # fictitious 265-minute requirement after an earlier valid exit.
    if mark_tracking_complete:
        expected_mode_marks = len(unique_mode_minutes)
    return {
        "phase": phase,
        "label": label,
        "phase_progress_ratio": _ratio(
            (min(max(local, phase_start), phase_end) - phase_start).total_seconds(),
            (phase_end - phase_start).total_seconds(),
        ),
        "session_progress_ratio": _ratio(
            (min(max(local, signal_at), session_end_at) - signal_at).total_seconds(),
            (session_end_at - signal_at).total_seconds(),
        ),
        "next_milestone_label": next_label,
        "next_milestone_at": next_at.isoformat(timespec="seconds"),
        "seconds_to_next_milestone": max(0.0, (next_at - local).total_seconds()),
        "decision_interval_seconds": 60,
        "signal_completed_modes": signal_completed,
        "entry_completed_modes": entry_completed,
        "exit_started_modes": exit_started,
        "mode_count": mode_count,
        "signal_progress_ratio": _ratio(signal_completed, mode_count),
        "entry_progress_ratio": _ratio(entry_completed, mode_count),
        "exit_progress_ratio": _ratio(exit_started, mode_count),
        "observed_mode_minutes": len(unique_mode_minutes),
        "expected_mode_minutes": expected_mode_marks,
        "mark_tracking_completed_modes": mark_tracking_completed_modes,
        "mark_tracking_complete": mark_tracking_complete,
        "mark_progress_ratio": _ratio(len(unique_mode_minutes), expected_mode_marks),
        "mark_rows_per_minute": (
            round(len(unique_mode_minutes) / elapsed_active_minutes, 3)
            if elapsed_active_minutes
            else 0.0
        ),
    }


def _safe_position(position: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "position_id",
        "market",
        "signal_market",
        "session_date",
        "signal_id",
        "signal_at",
        "source_signal_at",
        "open_reconstructed_at",
        "symbol",
        "name",
        "side",
        "target_weight",
        "requested_shares",
        "filled_shares",
        "signed_shares",
        "lot_size",
        "entry_at",
        "entry_quote_at",
        "entry_price",
        "inventory_basis_price",
        "odd_lot_execution_policy",
        "share_replacement_contract",
        "share_replacement_halted_until",
        "sizing_open_price",
        "entry_fee_twd",
        "remaining_entry_fee_twd",
        "entry_gross_fee_and_tax_twd",
        "entry_commission_rebate_accrued_twd",
        "upper_limit",
        "lower_limit",
        "take_profit_price",
        "stop_trigger_price",
        "price_limit_offset_ticks",
        "bracket_price_policy",
        "fill_guaranteed",
        "take_profit_order_status",
        "stop_order_status",
        "eod_limit_price",
        "eod_limit_submitted_at",
        "eod_limit_order_status",
        "eod_limit_liquidity_status",
        "closing_auction_limit_price",
        "closing_auction_order_status",
        "status",
        "last_mark_at",
        "last_quote_at",
        "last_mark_price",
        "last_complete_net_pnl_twd",
        "total_net_pnl_twd",
        "realized_net_pnl_twd",
        "valuation_stale",
        "last_exit_at",
        "last_exit_quote_at",
        "last_exit_price",
        "last_exit_quantity",
        "exit_at",
        "exit_quote_at",
        "exit_price",
        "gross_pnl_twd",
        "net_pnl_twd",
        "exit_reason",
        "simulation_replay",
        "replay_basis",
        "replay_source",
        "counterfactual_open_replay",
        "carry_type",
        "margin_carry_contract",
        "margin_converted_at",
        "margin_exception_evidence",
        "manual_close_settlement",
        "mandatory_exit_pending",
        "stop_triggered_at",
        "exit_quote_status",
        "margin_cost_accrued_through",
        "margin_cost_twd",
        "current_target_signal_id",
        "inventory_session_date",
    )
    row = {key: position.get(key) for key in allowed if key in position}
    if bool(position.get("counterfactual_open_replay")):
        row["open_reconstructed_at"] = (
            position.get("open_reconstructed_at")
            or position.get("entry_at")
            or position.get("signal_at")
        )
    signed_shares = int(position.get("signed_shares") or 0)
    realized = _finite_float(position.get("realized_net_pnl_twd"))
    if realized is None and signed_shares == 0:
        realized = _finite_float(position.get("net_pnl_twd"))
    unrealized = (
        0.0
        if signed_shares == 0
        else _finite_float(position.get("last_complete_net_pnl_twd"))
    )
    if realized is not None:
        row["realized_net_pnl_twd"] = realized
    row["unrealized_net_pnl_twd"] = unrealized
    if realized is not None and unrealized is not None:
        reconciled_total = realized + unrealized
        row["reconciled_total_net_pnl_twd"] = reconciled_total
        raw_total = _finite_float(position.get("total_net_pnl_twd"))
        row["pnl_reconciliation_difference_twd"] = (
            None if raw_total is None else raw_total - reconciled_total
        )
    return row


def _historical_positions(root: Path, session_date: str) -> list[dict[str, Any]]:
    """Load deterministic per-mode position snapshots for one prior session."""

    rows: list[dict[str, Any]] = []
    for directory in ("position_history", OVERNIGHT_POSITION_HISTORY_DIRNAME):
        session_root = root / directory / session_date
        if not session_root.is_dir():
            continue
        for path in sorted(session_root.glob("*.json")):
            # A full date-range query can touch hundreds of immutable snapshots.
            # Retaining every decoded graph in the generic object cache turns a
            # 203 MiB source tree into multiple GiB of long-lived Python objects.
            # Page responses and the compact position index own their caches;
            # these source documents are intentionally decoded one at a time.
            payload = _object(path, use_cache=False)
            if str(payload.get("session_date") or "") != session_date:
                continue
            for position in payload.get("positions") or ():
                if isinstance(position, Mapping):
                    rows.append(
                        _safe_position(
                            dict(position) | {"inventory_session_date": session_date}
                        )
                    )
    return rows


def _persistent_position_history_index_path(root: Path) -> Path | None:
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    if not cache_root or not Path(cache_root).is_dir():
        return None
    digest = hashlib.sha256(str(Path(root).resolve()).encode("utf-8")).hexdigest()
    return Path(cache_root) / f"position-history-index-v1-{digest}.json.gz"


def _position_history_source_signature(
    root: Path,
) -> tuple[tuple[Path, tuple[str, int, int, int, int]], ...]:
    sources = sorted(
        path
        for directory in ("position_history", OVERNIGHT_POSITION_HISTORY_DIRNAME)
        for path in (Path(root) / directory).glob("*/*.json")
    )
    output: list[tuple[Path, tuple[str, int, int, int, int]]] = []
    for path in sources:
        stat = path.stat()
        output.append(
            (
                path,
                (
                    str(path.relative_to(root)),
                    stat.st_dev,
                    stat.st_ino,
                    stat.st_size,
                    stat.st_mtime_ns,
                ),
            )
        )
    return tuple(output)


def _position_history_index(root: Path) -> _PositionHistoryIndex:
    """Return a compact, immutable locator index for archived positions.

    The UI sorts and filters on nine scalar fields; retaining all 45k wide
    position dictionaries wastes hundreds of MiB.  This index keeps only those
    scalars plus an exact file/row locator.  The selected page is rehydrated
    from at most the files that actually contribute visible rows.
    """

    resolved_root = Path(root).resolve()
    with _POSITION_HISTORY_INDEX_LOCK:
        inventory = _position_history_source_signature(resolved_root)
        signature = tuple(item[1] for item in inventory)
        cached = _POSITION_HISTORY_INDEX_CACHE.get(resolved_root)
        if cached is not None and cached.source_signature == signature:
            return cached

        cache_path = _persistent_position_history_index_path(resolved_root)
        if cache_path is not None and cache_path.is_file():
            try:
                payload = json.loads(gzip.decompress(cache_path.read_bytes()))
                if (
                    isinstance(payload, Mapping)
                    and int(payload.get("schema_version") or 0)
                    == _POSITION_HISTORY_INDEX_SCHEMA_VERSION
                    and str(payload.get("root") or "") == str(resolved_root)
                    and tuple(tuple(item) for item in payload.get("source_signature") or ())
                    == signature
                ):
                    expected_sources = tuple(item[0] for item in inventory)
                    if tuple(payload.get("sources") or ()) != tuple(
                        str(path.relative_to(resolved_root)) for path in expected_sources
                    ):
                        raise ValueError("position history source list mismatch")
                    sources = expected_sources
                    entries = tuple(
                        _PositionHistoryEntry(
                            identity=str(values[0]),
                            source_index=int(values[1]),
                            row_index=int(values[2]),
                            session_date=sys.intern(str(values[3])),
                            market=sys.intern(str(values[4])),
                            symbol=sys.intern(str(values[5])),
                            name=str(values[6]),
                            signed_shares=int(values[7]),
                            target_weight=float(values[8]),
                        )
                        for values in (payload.get("entries") or ())
                    )
                    if any(
                        entry.source_index < 0
                        or entry.source_index >= len(sources)
                        or entry.row_index < 0
                        for entry in entries
                    ):
                        raise ValueError("position history locator is outside source")
                    result = _PositionHistoryIndex(signature, sources, entries)
                    _POSITION_HISTORY_INDEX_CACHE[resolved_root] = result
                    return result
            except (
                EOFError,
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
                gzip.BadGzipFile,
            ):
                pass

        sources = tuple(item[0] for item in inventory)
        deduplicated: dict[str, _PositionHistoryEntry] = {}
        for source_index, path in enumerate(sources):
            session_date = path.parent.name
            payload = _object(path, use_cache=False)
            if str(payload.get("session_date") or "") != session_date:
                continue
            for row_index, position in enumerate(payload.get("positions") or ()):
                if not isinstance(position, Mapping):
                    continue
                market = str(position.get("market") or "")
                symbol = str(position.get("symbol") or "")
                identity = str(
                    position.get("position_id")
                    or f"{session_date}:{market}:{symbol}"
                )
                deduplicated[identity] = _PositionHistoryEntry(
                    identity=identity,
                    source_index=source_index,
                    row_index=row_index,
                    session_date=sys.intern(session_date),
                    market=sys.intern(market),
                    symbol=sys.intern(symbol),
                    name=str(position.get("name") or ""),
                    signed_shares=int(position.get("signed_shares") or 0),
                    target_weight=_finite_float(position.get("target_weight")) or 0.0,
                )
        result = _PositionHistoryIndex(signature, sources, tuple(deduplicated.values()))
        _POSITION_HISTORY_INDEX_CACHE[resolved_root] = result

        if cache_path is not None:
            temporary = cache_path.with_name(
                f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            try:
                encoded = json.dumps(
                    {
                        "schema_version": _POSITION_HISTORY_INDEX_SCHEMA_VERSION,
                        "root": str(resolved_root),
                        "source_signature": signature,
                        "sources": [str(path.relative_to(resolved_root)) for path in sources],
                        "entries": [
                            [
                                entry.identity,
                                entry.source_index,
                                entry.row_index,
                                entry.session_date,
                                entry.market,
                                entry.symbol,
                                entry.name,
                                entry.signed_shares,
                                entry.target_weight,
                            ]
                            for entry in result.entries
                        ],
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                compressed = gzip.compress(encoded, compresslevel=1)
                with temporary.open("xb") as handle:
                    handle.write(compressed)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(temporary, 0o600)
                os.replace(temporary, cache_path)
            except (OSError, TypeError, ValueError):
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
        return result


def _rehydrate_position_entries(
    index: _PositionHistoryIndex,
    entries: list[_PositionHistoryEntry],
) -> dict[str, dict[str, Any]]:
    requested: dict[int, dict[int, str]] = {}
    for entry in entries:
        requested.setdefault(entry.source_index, {})[entry.row_index] = entry.identity
    output: dict[str, dict[str, Any]] = {}
    for source_index, rows in requested.items():
        path = index.sources[source_index]
        payload = _object(path, use_cache=False)
        positions = payload.get("positions") or ()
        for row_index, identity in rows.items():
            if row_index >= len(positions) or not isinstance(
                positions[row_index], Mapping
            ):
                raise OSError(f"position history index changed while reading: {path}")
            source = positions[row_index]
            observed_identity = str(
                source.get("position_id")
                or f"{path.parent.name}:{source.get('market') or ''}:{source.get('symbol') or ''}"
            )
            if observed_identity != identity:
                raise OSError(f"position history index changed while reading: {path}")
            output[identity] = _safe_position(
                dict(source) | {"inventory_session_date": path.parent.name}
            )
    return output


def _historical_position_count(root: Path) -> int:
    """Count archived positions while decoding only new or changed snapshots."""

    history_root = Path(root) / "position_history"
    paths = sorted(history_root.glob("*/*.json"))
    cache_root = str(os.environ.get(_LEDGER_SESSION_INDEX_CACHE_ENV) or "").strip()
    cache_path: Path | None = None
    retained: Mapping[str, Any] = {}
    if cache_root and Path(cache_root).is_dir():
        digest = hashlib.sha256(str(history_root.resolve()).encode("utf-8")).hexdigest()
        cache_path = Path(cache_root) / f"historical-position-count-v1-{digest}.json"
        try:
            payload = json.loads(cache_path.read_bytes())
            if (
                isinstance(payload, Mapping)
                and int(payload.get("schema_version") or 0) == 1
                and str(payload.get("root") or "") == str(history_root.resolve())
            ):
                raw_entries = payload.get("entries")
                if isinstance(raw_entries, Mapping):
                    retained = raw_entries
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            pass

    entries: dict[str, dict[str, int]] = {}
    total = 0
    changed = len(retained) != len(paths)
    for path in paths:
        stat = path.stat()
        relative = str(path.relative_to(history_root))
        cached = retained.get(relative)
        try:
            cached_matches = bool(
                isinstance(cached, Mapping)
                and int(cached.get("device") or -1) == stat.st_dev
                and int(cached.get("inode") or -1) == stat.st_ino
                and int(cached.get("size") or -1) == stat.st_size
                and int(cached.get("modified_ns") or -1) == stat.st_mtime_ns
                and int(cached.get("count") or -1) >= 0
            )
        except (TypeError, ValueError):
            cached_matches = False
        if cached_matches and isinstance(cached, Mapping):
            count = int(cached["count"])
        else:
            count = len((_object(path).get("positions") or ()))
            changed = True
        entries[relative] = {
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "size": stat.st_size,
            "modified_ns": stat.st_mtime_ns,
            "count": count,
        }
        total += count

    if cache_path is not None and changed:
        temporary = cache_path.with_name(
            f".{cache_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            temporary.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "root": str(history_root.resolve()),
                        "entries": entries,
                        "count": total,
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            os.replace(temporary, cache_path)
        except OSError:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
    return total


def build_dashboard_snapshot(
    *,
    state_dir: Path,
    preopen_readiness_path: Path | None = None,
    discord_service_status_path: Path | None = None,
    session_date: str | None = None,
    now: datetime | None = None,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    maximum_signal_rows: int = 0,
    maximum_event_rows: int = 2_000,
    maximum_mark_rows: int = 4_000,
    include_position_rows: bool = True,
    include_ledger_session_dates: bool = True,
    unattended_guardian_path: Path = DEFAULT_UNATTENDED_GUARDIAN_PATH,
    discord_markets_field: str = "day_trade_markets",
    discord_engine_revision_field: str = "engine_state_revision",
) -> dict[str, Any]:
    root = Path(state_dir)
    state = _object(root / "state.json")
    status = _object(root / "status.json")
    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    resolved_discord_status_path = (
        Path(discord_service_status_path)
        if discord_service_status_path is not None
        else (
            Path(preopen_readiness_path).with_name(DISCORD_SERVICE_STATUS_FILENAME)
            if preopen_readiness_path is not None
            else None
        )
    )
    service_sync = build_dashboard_revision(
        state_dir=root,
        discord_service_status_path=resolved_discord_status_path,
        discord_markets_field=discord_markets_field,
        discord_engine_revision_field=discord_engine_revision_field,
        now=observed,
    )
    unattended_guardian = _unattended_guardian_status(
        path=Path(unattended_guardian_path), observed=observed
    )
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=observed,
        include_ledger_dates=include_ledger_session_dates,
        include_benchmark_history_dates=include_ledger_session_dates,
        include_preopen_session=discord_markets_field == "day_trade_markets",
    )
    clock_session = service_sync.get("session_clock", {}).get("display_session_date")
    selected_session_date = _select_session_date(
        session_date
        or (clock_session if clock_session in available_session_dates else None),
        available_session_dates,
    )
    local_observed = observed.astimezone(TAIPEI)
    current_view = selected_session_date == local_observed.date().isoformat()
    selected_observed = (
        observed
        if current_view
        else datetime.combine(
            datetime_date.fromisoformat(selected_session_date),
            datetime_time(13, 30),
            tzinfo=TAIPEI,
        )
    )
    source_updated = _timestamp(status.get("updated_at"))
    source_age = max(0.0, (observed - source_updated).total_seconds())
    health = str(status.get("health") or "unknown")
    if source_age > float(max_source_age_seconds):
        health = "stale"
    benchmark_history = (
        _benchmark_history_index(root)
        if include_ledger_session_dates
        else _empty_benchmark_history_index()
    )
    benchmark_origins = benchmark_history.origins
    benchmark_history_marks = [
        dict(row)
        for row in benchmark_history.marks_by_session.get(selected_session_date, ())
    ]

    modes: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    configured_markets = state.get("enabled_markets")
    has_enabled_market_contract = isinstance(configured_markets, list)
    enabled_markets = {str(market) for market in (configured_markets or ())}
    for market, raw_mode in (state.get("modes") or {}).items():
        if has_enabled_market_contract and str(market) not in enabled_markets:
            continue
        mode = dict(raw_mode) if isinstance(raw_mode, Mapping) else {}
        return_fraction, return_pct = _capital_return(
            mode.get("initial_capital_twd"), mode.get("total_equity_twd")
        )
        mode_positions = [
            _safe_position(item)
            for item in (mode.get("positions") or {}).values()
            if isinstance(item, Mapping)
        ]
        position_requested_shares = sum(
            int(item.get("requested_shares") or item.get("filled_shares") or 0)
            for item in mode_positions
        )
        position_filled_shares = sum(
            int(item.get("filled_shares") or 0) for item in mode_positions
        )
        recorded_requested_shares = mode.get("entry_requested_shares")
        recorded_filled_shares = mode.get("entry_filled_shares")
        recorded_unfilled_shares = mode.get("entry_unfilled_shares")
        entry_requested_shares = int(
            position_requested_shares
            if recorded_requested_shares is None
            else recorded_requested_shares
        )
        entry_filled_shares = int(
            position_filled_shares
            if recorded_filled_shares is None
            else recorded_filled_shares
        )
        entry_unfilled_shares = int(
            max(0, entry_requested_shares - entry_filled_shares)
            if recorded_unfilled_shares is None
            else recorded_unfilled_shares
        )
        entry_fill_outcome = mode.get("entry_fill_outcome") or (
            "partial"
            if entry_filled_shares and entry_unfilled_shares
            else "filled"
            if entry_filled_shares
            else "pending"
        )
        positions.extend(mode_positions)
        modes.append(
            {
                "market": market,
                "label": mode.get("label"),
                "signal_market": mode.get("signal_market") or market,
                "price_limit_offset_ticks": mode.get("price_limit_offset_ticks", 0),
                "bracket_price_policy": mode.get("bracket_price_policy"),
                "fill_guaranteed": bool(mode.get("fill_guaranteed", False)),
                # Preserve the currently deployed execution contract before a
                # historical session view below restores the contract recorded
                # by that session's immutable signal event.  The dashboard must
                # show both facts instead of making a legacy replay look like
                # the active policy (or rewriting it as though it had used the
                # active policy).
                "configured_entry_fill_policy": mode.get("entry_fill_policy"),
                "configured_entry_price_offset_ticks": mode.get(
                    "entry_price_offset_ticks", 0
                ),
                "configured_entry_fill_is_synthetic": bool(
                    mode.get("entry_fill_is_synthetic", False)
                ),
                "entry_fill_policy": mode.get("entry_fill_policy"),
                "entry_price_offset_ticks": mode.get("entry_price_offset_ticks", 0),
                "entry_fill_is_synthetic": bool(
                    mode.get("entry_fill_is_synthetic", False)
                ),
                "entry_best_quote_fill_count": int(
                    mode.get("entry_best_quote_fill_count") or 0
                ),
                "entry_synthetic_fallback_fill_count": int(
                    mode.get("entry_synthetic_fallback_fill_count") or 0
                ),
                "entry_0901_vwap_fill_count": int(
                    mode.get("entry_0901_vwap_fill_count") or 0
                ),
                "entry_0901_close_fill_count": int(
                    mode.get("entry_0901_close_fill_count") or 0
                ),
                "entry_0901_minute_price_fill_count": int(
                    mode.get("entry_0901_minute_price_fill_count")
                    or mode.get("entry_0901_vwap_fill_count")
                    or 0
                ),
                "engine_status": mode.get("engine_status"),
                "checkpoint_ready": mode.get("checkpoint_ready"),
                "readiness_error": mode.get("readiness_error"),
                "checkpoint_path": mode.get("checkpoint_path"),
                "checkpoint_fingerprint": mode.get("checkpoint_fingerprint"),
                "config_path": mode.get("config_path"),
                "config_fingerprint": mode.get("config_fingerprint"),
                "live_output_dir": mode.get("live_output_dir"),
                "target_weights_path": mode.get("target_weights_path"),
                "target_positions_path": mode.get("target_positions_path"),
                "executed_positions_path": mode.get("executed_positions_path"),
                "target_symbol_count": mode.get("target_symbol_count"),
                "target_risk": mode.get("target_risk") or {},
                "session_date": mode.get("session_date"),
                "signal_id": mode.get("signal_id"),
                "signal_at": mode.get("signal_at"),
                "source_signal_at": mode.get("source_signal_at"),
                "open_reconstructed_at": (
                    (mode.get("open_reconstructed_at") or mode.get("signal_at"))
                    if bool(mode.get("counterfactual_open_replay", False))
                    else None
                ),
                "feature_cutoff_date": mode.get("feature_cutoff_date"),
                "signal_counts": mode.get("signal_counts") or {},
                "signal_reason_counts": mode.get("signal_reason_counts") or {},
                "entry_fill_count": int(
                    len(mode_positions)
                    if mode.get("entry_fill_count") is None
                    else mode.get("entry_fill_count")
                ),
                "entry_requested_shares": entry_requested_shares,
                "entry_filled_shares": entry_filled_shares,
                "entry_unfilled_shares": entry_unfilled_shares,
                "intraday_contract": mode.get("intraday_contract"),
                "configured_intraday_contract": mode.get(
                    "configured_intraday_contract"
                ),
                "pending_entry_shares": mode.get("pending_entry_shares", 0),
                "manual_close_settlement": mode.get("manual_close_settlement"),
                "closing_auction_pending_count": mode.get(
                    "closing_auction_pending_count", 0
                ),
                "margin_exception_count": mode.get("margin_exception_count", 0),
                "unresolved_exit_count": mode.get("unresolved_exit_count", 0),
                "entry_fill_outcome": entry_fill_outcome,
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "total_equity_twd": mode.get("total_equity_twd"),
                "last_mark_at": mode.get("last_mark_at"),
                "valuation_stale": mode.get("valuation_stale", False),
                "return_fraction": return_fraction,
                "return_pct": return_pct,
                "cumulative_realized_net_pnl_twd": mode.get(
                    "cumulative_realized_net_pnl_twd"
                ),
                "cumulative_commission_rebate_accrued_twd": mode.get(
                    "cumulative_commission_rebate_accrued_twd"
                ),
                "open_net_liquidation_pnl_twd": mode.get(
                    "open_net_liquidation_pnl_twd"
                ),
                "open_position_count": mode.get("open_position_count", 0),
                "stale_position_count": mode.get("stale_position_count", 0),
                "entry_completed_at": mode.get("entry_completed_at"),
                "exit_limit_submitted_at": mode.get("exit_limit_submitted_at"),
                "force_exit_started_at": mode.get("force_exit_started_at"),
                "closing_auction_submitted_at": mode.get(
                    "closing_auction_submitted_at"
                ),
                "closing_auction_settled_at": mode.get("closing_auction_settled_at"),
                "residual_conversion_completed_at": mode.get(
                    "residual_conversion_completed_at"
                ),
                "force_exit_failures": mode.get("force_exit_failures", 0),
                "terminal_flatten_count": mode.get("terminal_flatten_count", 0),
                "terminal_flatten_degraded_count": mode.get(
                    "terminal_flatten_degraded_count", 0
                ),
                "eligibility_coverage": mode.get("eligibility_coverage") or {},
                "current_eligibility_coverage": mode.get("current_eligibility_coverage")
                or {},
                "simulation_replay": bool(mode.get("simulation_replay", False)),
                "replay_basis": mode.get("replay_basis"),
                "replay_source": mode.get("replay_source"),
                "counterfactual_open_replay": bool(
                    mode.get("counterfactual_open_replay", False)
                ),
                "entry_fill_contract": mode.get("entry_fill_contract"),
                "entry_liquidity_assumption": mode.get("entry_liquidity_assumption"),
                "position_count": len(mode_positions),
            }
        )

    benchmarks: list[dict[str, Any]] = []
    for benchmark_id, raw_benchmark in (state.get("benchmarks") or {}).items():
        if not isinstance(raw_benchmark, Mapping):
            continue
        benchmark_origin = benchmark_origins.get(str(benchmark_id))
        benchmark = _rebase_live_benchmark(
            raw_benchmark,
            benchmark_origin,
        )
        if _finite_float(benchmark.get("daily_return_pct")) is None:
            is_tx = str(benchmark.get("instrument_type") or "") == (
                "continuous_long_future"
            )
            retained_close = _previous_benchmark_close(
                benchmark_history,
                benchmark_id=str(benchmark_id),
                session_date=selected_session_date,
                contract_code=(
                    str(benchmark.get("contract_code") or "") if is_tx else None
                ),
            )
            if is_tx:
                origin_contract = str(
                    (benchmark_origin or {}).get("latest_completed_contract_code") or ""
                ).upper()
                current_contract = str(benchmark.get("contract_code") or "").upper()
                origin_close_date = str(
                    (benchmark_origin or {}).get("latest_completed_close_date") or ""
                )
                use_origin_close = (
                    origin_contract == current_contract
                    and bool(origin_close_date)
                    and origin_close_date < selected_session_date
                )
                daily_reference = (
                    _finite_float(
                        (benchmark_origin or {}).get("latest_completed_close")
                    )
                    if use_origin_close
                    else _finite_float((retained_close or {}).get("last_mark_price"))
                )
                previous_close_source = str(
                    (benchmark_origin or {}).get("latest_completed_close_source")
                    if use_origin_close
                    else "retained_receipt_backed_same_contract_close"
                )
            else:
                daily_reference = _finite_float(
                    benchmark.get("current_session_reference_price")
                ) or _finite_float((retained_close or {}).get("last_mark_price"))
                previous_close_source = str(
                    benchmark.get("previous_official_close_source")
                    or "retained_official_total_return_close"
                )
            daily_fraction, daily_pct = previous_close_return(
                benchmark.get("last_mark_price"),
                daily_reference,
            )
            benchmark.update(
                {
                    "daily_return_fraction": daily_fraction,
                    "daily_return_pct": daily_pct,
                    "daily_return_basis": DAILY_RETURN_BASIS_PREVIOUS_CLOSE,
                    "daily_return_reference_price": daily_reference,
                    "daily_return_previous_close": daily_reference,
                    "daily_return_previous_close_date": (
                        (
                            origin_close_date
                            if is_tx and use_origin_close
                            else (retained_close or {}).get("session_date")
                        )
                        or benchmark.get("previous_official_close_date")
                    ),
                    "daily_return_previous_close_source": previous_close_source,
                }
            )
        if str(benchmark.get("instrument_type") or "") == "continuous_long_future":
            current_notional = fully_collateralized_futures_notional(
                benchmark.get("last_mark_price"),
                benchmark.get("multiplier_twd_per_point") or 200.0,
            )
            benchmark.update(
                {
                    "capital_basis": TX_FULLY_COLLATERALIZED_CAPITAL_BASIS,
                    "current_contract_notional_twd": current_notional,
                    "fully_collateralized_account_equity_twd": current_notional,
                    "collateral_coverage_ratio": (
                        1.0 if current_notional is not None else None
                    ),
                    "maximum_leverage": 1.0,
                    "tracking_costs_excluded_from_return": True,
                }
            )
        return_fraction, return_pct = _capital_return(
            benchmark.get("initial_capital_twd"), benchmark.get("total_equity_twd")
        )
        benchmark["benchmark_id"] = str(benchmark.get("benchmark_id") or benchmark_id)
        benchmark["return_fraction"] = return_fraction
        benchmark["return_pct"] = return_pct
        benchmarks.append(benchmark)

    def current(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if not row.get("session_date")
            or str(row.get("session_date")) == selected_session_date
        ]

    def current_event(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if str(row.get("session_date") or "") == selected_session_date
            or _is_taipei_session_date(row.get("recorded_at"), selected_session_date)
        ]

    def observed_on_selected_date(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if _is_taipei_session_date(row.get("recorded_at"), selected_session_date)
            or (
                not row.get("recorded_at")
                and str(row.get("session_date") or "") == selected_session_date
            )
        ]

    state_session_dates = {
        str(raw_mode.get("session_date") or "")[:10]
        for raw_mode in (state.get("modes") or {}).values()
        if isinstance(raw_mode, Mapping) and raw_mode.get("session_date")
    }
    use_contiguous_tail = (
        not include_ledger_session_dates
        and selected_session_date in state_session_dates
    )

    def selected_session_rows(
        filename: str,
        maximum_rows: int,
        *,
        recorded_at_fallback: bool = False,
    ) -> list[dict[str, Any]]:
        path = root / filename
        if use_contiguous_tail:
            return _latest_contiguous_session_rows(
                path,
                selected_session_date,
                maximum_rows,
                recorded_at_fallback=recorded_at_fallback,
            )
        return _tail_for_session(
            path,
            maximum_rows,
            selected_session_date,
            recorded_at_fallback=recorded_at_fallback,
        )

    signals = current(_tail(root / "signals.jsonl", maximum_signal_rows))
    orders = selected_session_rows("orders.jsonl", maximum_event_rows)
    fills = selected_session_rows("fills.jsonl", maximum_event_rows)
    raw_marks = selected_session_rows(
        "marks.jsonl",
        maximum_mark_rows,
        recorded_at_fallback=True,
    )
    marks_by_mode_minute: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in raw_marks:
        row = dict(source_row)
        return_fraction, return_pct = _capital_return(
            row.get("initial_capital_twd"), row.get("total_equity_twd")
        )
        row["return_fraction"] = return_fraction
        row["return_pct"] = return_pct
        marks_by_mode_minute[(str(row.get("market")), str(row.get("minute")))] = row
    marks = list(marks_by_mode_minute.values())
    raw_benchmark_marks = selected_session_rows(
        "benchmark_marks.jsonl",
        maximum_mark_rows,
        recorded_at_fallback=True,
    )
    benchmark_marks_by_id_minute: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in [*benchmark_history_marks, *raw_benchmark_marks]:
        benchmark_id = str(source_row.get("benchmark_id") or "")
        row = _rebase_live_benchmark(
            source_row,
            benchmark_origins.get(benchmark_id),
        )
        return_fraction, return_pct = _capital_return(
            row.get("initial_capital_twd"), row.get("total_equity_twd")
        )
        row["return_fraction"] = return_fraction
        row["return_pct"] = return_pct
        benchmark_marks_by_id_minute[
            (str(row.get("benchmark_id")), str(row.get("minute")))
        ] = row
    benchmark_marks = list(benchmark_marks_by_id_minute.values())
    events = selected_session_rows(
        "events.jsonl",
        min(maximum_event_rows, 2_000),
        recorded_at_fallback=True,
    )
    all_latency_rows = _tail(root / "latency.jsonl", 2_000)
    opening_attempt_rows = _tail(
        root / "opening_signal_latency.jsonl",
        10_000,
    )
    latency_rows = selected_session_rows(
        "latency.jsonl",
        min(maximum_event_rows, 2_000),
        recorded_at_fallback=True,
    )
    latency = _latency_summary(latency_rows)
    today_session_date = local_observed.date().isoformat()
    today_latency_rows = (
        latency_rows
        if selected_session_date == today_session_date
        else _tail_for_session(
            root / "latency.jsonl",
            min(maximum_event_rows, 2_000),
            today_session_date,
            recorded_at_fallback=True,
        )
    )
    today_latency = _latency_summary(today_latency_rows)
    today_latency["session_date"] = today_session_date
    opening_signal_latency = _opening_signal_latency_summary(
        [*opening_attempt_rows, *all_latency_rows],
        expected_markets=[str(row.get("market") or "") for row in modes],
        session_date=selected_session_date,
    )

    pending_session_markets = {
        str(mode.get("market"))
        for mode in modes
        if str(mode.get("session_date") or "") != selected_session_date
    }
    if not current_view or pending_session_markets:
        latest_marks = {
            str(row.get("market") or ""): row for row in marks if row.get("market")
        }
        events_by_market: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            market = str(event.get("market") or "")
            if market:
                events_by_market.setdefault(market, []).append(event)
        selected_positions_by_market: dict[str, int] = {}
        archived_positions = _historical_positions(root, selected_session_date)
        current_state_positions = [
            position
            for position in positions
            if str(position.get("session_date") or "") == selected_session_date
            or (current_view and int(position.get("signed_shares") or 0) != 0)
        ]
        positions_by_id = {
            str(position.get("position_id") or ""): position
            for position in [*archived_positions, *current_state_positions]
            if position.get("position_id")
        }
        positions = list(positions_by_id.values())
        for position in positions:
            market = str(position.get("market") or "")
            selected_positions_by_market[market] = (
                selected_positions_by_market.get(market, 0) + 1
            )
        for mode in modes:
            market = str(mode.get("market") or "")
            if current_view and market not in pending_session_markets:
                continue
            market_events = events_by_market.get(market, [])
            signal_event = next(
                (
                    event
                    for event in reversed(market_events)
                    if str(event.get("event") or "")
                    in {"signal_registered", "signal_blocked"}
                ),
                None,
            )
            last_mark = latest_marks.get(market)
            mode["session_date"] = selected_session_date
            mode["signal_id"] = (signal_event or {}).get("signal_id")
            mode["signal_at"] = (signal_event or {}).get("recorded_at")
            mode["source_signal_at"] = (signal_event or {}).get("source_signal_at")
            mode["entry_completed_at"] = (
                (signal_event or {}).get("recorded_at")
                if (signal_event or {}).get("event") == "signal_registered"
                else None
            )
            mode["signal_counts"] = (signal_event or {}).get("counts") or {}
            mode["signal_reason_counts"] = (signal_event or {}).get(
                "reason_counts"
            ) or {}
            mode["entry_fill_count"] = int(
                (signal_event or {}).get("entry_fill_count") or 0
            )
            mode["entry_requested_shares"] = int(
                (signal_event or {}).get("entry_requested_shares") or 0
            )
            mode["entry_filled_shares"] = int(
                (signal_event or {}).get("entry_filled_shares") or 0
            )
            mode["entry_unfilled_shares"] = int(
                (signal_event or {}).get("entry_unfilled_shares") or 0
            )
            mode["entry_fill_outcome"] = (signal_event or {}).get(
                "entry_fill_outcome"
            ) or "pending"
            mode["entry_fill_policy"] = (signal_event or {}).get(
                "entry_fill_policy"
            ) or mode.get("entry_fill_policy")
            mode["entry_price_offset_ticks"] = int(
                (signal_event or {}).get("entry_price_offset_ticks")
                or mode.get("entry_price_offset_ticks")
                or 0
            )
            mode["entry_fill_is_synthetic"] = bool(
                (signal_event or {}).get("entry_fill_is_synthetic")
                or mode.get("entry_fill_is_synthetic", False)
            )
            mode["entry_best_quote_fill_count"] = int(
                (signal_event or {}).get("entry_best_quote_fill_count") or 0
            )
            mode["entry_synthetic_fallback_fill_count"] = int(
                (signal_event or {}).get("entry_synthetic_fallback_fill_count") or 0
            )
            mode["entry_0901_vwap_fill_count"] = int(
                (signal_event or {}).get("entry_0901_vwap_fill_count") or 0
            )
            mode["entry_0901_close_fill_count"] = int(
                (signal_event or {}).get("entry_0901_close_fill_count") or 0
            )
            mode["entry_0901_minute_price_fill_count"] = int(
                (signal_event or {}).get("entry_0901_minute_price_fill_count")
                or (signal_event or {}).get("entry_0901_vwap_fill_count")
                or 0
            )
            mode["simulation_replay"] = bool(
                (signal_event or {}).get("simulation_replay", False)
            )
            mode["replay_basis"] = (signal_event or {}).get("replay_basis")
            mode["counterfactual_open_replay"] = bool(
                (signal_event or {}).get("counterfactual_open_replay", False)
            )
            mode["open_reconstructed_at"] = (
                (
                    (signal_event or {}).get("open_reconstructed_at")
                    or (signal_event or {}).get("recorded_at")
                )
                if mode["counterfactual_open_replay"]
                else None
            )
            mode["position_count"] = selected_positions_by_market.get(market, 0)
            mode["exit_limit_submitted_at"] = next(
                (
                    event.get("recorded_at")
                    for event in reversed(market_events)
                    if event.get("event") == "exit_limits_submitted"
                ),
                None,
            )
            mode["force_exit_started_at"] = next(
                (
                    event.get("recorded_at")
                    for event in reversed(market_events)
                    if event.get("event") == "force_exit_started"
                ),
                None,
            )
            if last_mark is not None:
                for key in (
                    "initial_capital_twd",
                    "total_equity_twd",
                    "cumulative_realized_net_pnl_twd",
                    "open_net_liquidation_pnl_twd",
                    "open_position_count",
                    "stale_position_count",
                ):
                    mode[key] = last_mark.get(key)
                mode["return_fraction"] = last_mark.get("return_fraction")
                mode["return_pct"] = last_mark.get("return_pct")
                mode["last_mark_at"] = last_mark.get("minute") or last_mark.get(
                    "recorded_at"
                )
                mode["valuation_stale"] = bool(
                    last_mark.get("valuation_stale")
                    or last_mark.get("stale_position_count")
                )
            elif current_view:
                # Carry the real account forward, never yesterday's signal or
                # fill counters. Do not manufacture a current-session mark.
                mode["return_fraction"] = None
                mode["return_pct"] = None
                mode["last_mark_at"] = None
                mode["closing_auction_settled_at"] = None
            else:
                mode["total_equity_twd"] = None
                mode["return_fraction"] = None
                mode["return_pct"] = None
                mode["open_position_count"] = 0
                mode["stale_position_count"] = 0
                mode["last_mark_at"] = None
                mode["closing_auction_settled_at"] = None
                mode["valuation_stale"] = False
            if (signal_event or {}).get("event") == "signal_blocked":
                mode["engine_status"] = "historical_signal_blocked"
            elif (signal_event or {}).get("event") == "signal_registered":
                mode["engine_status"] = (
                    "historical_session_closed_with_residual"
                    if int(mode.get("open_position_count") or 0)
                    else "historical_session_complete"
                )
            elif current_view:
                mode["engine_status"] = (
                    "waiting_open"
                    if local_observed.time() < datetime_time(9)
                    else "waiting_signal"
                )
            else:
                mode["engine_status"] = "historical_session_missed"

        latest_benchmark_marks = {
            str(row.get("benchmark_id") or ""): row
            for row in benchmark_marks
            if row.get("benchmark_id")
        }
        for benchmark in benchmarks:
            historical = latest_benchmark_marks.get(
                str(benchmark.get("benchmark_id") or "")
            )
            if historical is not None:
                benchmark.update(historical)

    execution_records = _attach_execution_records(
        modes=modes,
        events=events,
        observed=selected_observed,
        session_date=selected_session_date,
    )
    for mode in modes:
        account_source = (
            (state.get("modes") or {}).get(mode["market"], mode)
            if current_view
            else mode
        )
        mode["account_performance"] = paper_account_performance(
            account_source,
            revision=state.get("state_revision") if current_view else None,
        )
        mode["signal_product"] = "scheduled_execution"
    modes.sort(key=lambda row: str(row.get("market")))
    benchmarks.sort(key=lambda row: str(row.get("benchmark_id")))
    positions.sort(
        key=lambda row: (
            str(row.get("market")),
            0 if int(row.get("signed_shares") or 0) else 1,
            str(row.get("symbol")),
        )
    )
    # The default dashboard is an operational view.  Before today's first
    # signal the latest ledger session is still yesterday, but today's preopen
    # receipts must remain visible.  Only an explicitly selected historical
    # session should evaluate preopen state at that historical timestamp.
    operational_view = session_date is None or current_view
    preopen_observed = observed if operational_view else selected_observed
    preopen = _preopen_progress(
        path=preopen_readiness_path,
        modes=modes,
        observed=preopen_observed,
    )
    simulation_preopen = _simulation_preopen_progress(
        path=root / "preopen_readiness.json",
        observed=preopen_observed,
    )
    preopen["simulation"] = simulation_preopen
    if operational_view and not simulation_preopen["ready"]:
        preopen["status"] = (
            "failed" if simulation_preopen["status"] == "failed" else "pending"
        )
    operational_issues = _operational_issues(
        modes=modes,
        preopen=preopen,
        observed=preopen_observed,
    )
    if operational_view:
        try:
            raw_opening_gate = _object(DEFAULT_OPENING_GATE_PATH)
        except (OSError, ValueError, json.JSONDecodeError):
            # The opening gate is an optional operational receipt.  A missing or
            # partially replaced file must degrade the snapshot, not make the
            # read-only dashboard endpoint fail.
            raw_opening_gate = {}
    else:
        raw_opening_gate = {}
    opening_gate = (
        {
            key: raw_opening_gate.get(key)
            for key in (
                "schema_version",
                "status",
                "ready",
                "strict",
                "session_date",
                "observed_at_taipei",
                "deadline_taipei",
                "failures",
                "engine_runtime",
                "runtime_sync",
                "opening_execution",
            )
        }
        if str(raw_opening_gate.get("session_date") or "")
        == local_observed.date().isoformat()
        else {}
    )
    if opening_gate.get("status") == "failed":
        operational_issues.append(
            {
                "severity": "error",
                "scope": "opening_gate",
                "market": None,
                "code": "opening_acceptance_failed",
                "title": "09:00 啟動驗收失敗",
                "detail": "；".join(
                    str(value) for value in (opening_gate.get("failures") or ())
                )
                or "三個模式尚未完成同日訊號與 ledger 提交。",
                "count": 1,
                "observed_at": opening_gate.get("observed_at_taipei"),
            }
        )
    if health not in {"stale", "critical"} and any(
        issue.get("severity") in {"error", "warning"} for issue in operational_issues
    ):
        health = "degraded"
    session_progress = _session_progress(
        observed=selected_observed,
        mode_count=len(modes),
        modes=modes,
        marks=marks,
    )
    count_or_pending = (
        (lambda filename: _line_count(root / filename))
        if include_ledger_session_dates
        else (lambda _filename: None)
    )
    record_counts = {
        "signals": count_or_pending("signals.jsonl"),
        "orders": count_or_pending("orders.jsonl"),
        "fills": count_or_pending("fills.jsonl"),
        "marks": count_or_pending("marks.jsonl"),
        "benchmark_marks": count_or_pending("benchmark_marks.jsonl"),
        "benchmark_history_marks": (
            len(benchmark_history.marks) if include_ledger_session_dates else None
        ),
        "events": count_or_pending("events.jsonl"),
        "latency_samples": count_or_pending("latency.jsonl"),
        "opening_signal_latency_samples": count_or_pending(
            "opening_signal_latency.jsonl"
        ),
        "historical_positions": (
            _historical_position_count(root) if include_ledger_session_dates else None
        ),
    }

    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "generated_at_utc": observed.isoformat(timespec="seconds"),
        "health": health,
        "source_updated_at": status.get("updated_at"),
        "source_age_seconds": round(source_age, 3),
        "service_sync": service_sync,
        "unattended_guardian": unattended_guardian,
        "ledger_integrity": status.get("ledger_integrity") or {},
        "simulation_only": True,
        "production_order_possible": False,
        "session_date": selected_session_date,
        "available_session_dates": available_session_dates,
        "schedule": status.get("schedule") or {},
        "preopen": preopen,
        "opening_gate": opening_gate,
        "operational_issues": operational_issues,
        "execution_records": execution_records,
        "latency": latency,
        "today_latency": today_latency,
        "opening_signal_latency": opening_signal_latency,
        "session_progress": session_progress,
        "modes": modes,
        "benchmarks": benchmarks,
        "positions": positions if include_position_rows else [],
        "signals": signals,
        "orders": orders,
        "fills": fills,
        "marks": marks,
        "benchmark_marks": benchmark_marks,
        "events": events,
        "record_counts": record_counts,
        "record_counts_ready": bool(include_ledger_session_dates),
        "payload_window": {
            "positions": len(positions) if include_position_rows else 0,
            "signals": len(signals),
            "orders": len(orders),
            "fills": len(fills),
            "marks": len(marks),
            "benchmark_marks": len(benchmark_marks),
            "events": len(events),
            "latency_samples": len(latency_rows),
        },
        "source_contract": {
            "preopen": "artifacts/discord_bot/preopen_readiness.json; only same-day recorded stages are shown and missing intermediate states are not estimated",
            "execution_record": "today's append-only signal_registered or signal_blocked event per mode; stale prior-session timestamps never count",
            "missed_start": "between 09:00 and 13:20, Linux inotify wakes the executor when the atomic latest-signal pointer is published; a 0.1-second timeout remains only as a portable catch-up fallback and the public dashboard remains read-only",
            "signal": "Discord live target_weights.parquet after observed opening quote",
            "replay": "simulation_replay=true is recorded at 09:01: inference and whole-lot sizing use the official 09:00 session open, while execution uses the source-backed right-labelled 09:01 minute price (VWAP, otherwise that KBar's Close). It is explicitly counterfactual and is not a live quote or real order fill",
            "entry_fill": "live execution starts at 09:00: after the immutable signal pointer is published, buy/cover consumes the first strictly later best Ask and sell/short consumes the first strictly later best Bid. An uncommitted opening after the 09:00:15 durability deadline uses the source-backed 09:01 minute price; missing ticks alone do not block, but a missing minute bar is blocked without open-price fill, carried last-price, or adverse-tick substitution",
            "latency": "opening telemetry separates 09:00 scheduler wake, local quote callback coverage, quote-service queue/provider/serialization, feature preparation, model lock/inference, atomic signal publication, consumer discovery, and ledger persistence; local callback receipt is not exchange matching time, order acknowledgement, or venue round-trip latency",
            "service_sync": "Discord, the paper engine, and the dashboard share one compact engine commit revision; Discord acknowledges that revision without reparsing the full ledger and the dashboard fetches heavy state only when the revision changes",
            "unattended_guardian": "the weekday guardian verifies the schedule clock, all 156 source events, exact-session eligibility, 08:30 acceptance, the three engine/Discord revisions, post-close flatness, public endpoints, and disk headroom; it re-arms existing systemd units but never invents data, signals, or fills",
            "mark": "best bid liquidates long; best ask covers short",
            "missing_mark": "carry only the same open position's last complete liquidation value and flag stale",
            "eligibility": "exact-session TWSE and TPEx official day-trade membership; missing venue/date blocks",
            "fees": "gross commission and sell tax are charged first; earned commission rebate is recorded separately in economic NAV",
            "pnl_split": "realized net PnL uses simulated executable exits plus any explicitly tagged 13:30 terminal ledger flatten, with allocated entry and exit costs; unrealized net liquidation PnL values remaining shares at executable bid or ask after remaining costs; total net PnL is their reconciled sum",
            "comparison": "strategies retain their own execution-capital returns; all three market benchmarks are separate gross cross-session Buy-and-Hold comparators. Their displayed daily return is current value divided by the preceding session close. TX holds one fully collateralized contract with cash equal to 100% of notional, so leverage never exceeds 1x",
            "benchmarks": "0050/2330 use a one-board-lot total-return Buy-and-Hold curve from the preceding official close; official ex-date factors are applied exactly once. TXFR1 chain-links one front-month TX contract, marks each day against the same physical contract's preceding close, and treats the price-level difference at a roll as an external cash top-up or withdrawal rather than return. Minute history uses receipt-backed observations and explicitly carries only a prior observation when a minute has no trade, without interpolation. Fees and tax are disclosed as estimated tracking costs but excluded from gross benchmark return",
            "benchmark_history": (
                "audited prior-close Buy-and-Hold benchmark history is merged read-only with later live marks"
                if benchmark_history.origins
                else benchmark_history.load_error
                or "live benchmark marks only; no historical origin file"
            ),
            "depth_limit": "live entry quantity is bounded by independently verified eligibility, whole lots, price limits, displayed level-one depth, and after 09:01 completed-minute participation. Missed-opening replay uses the official open only for sizing and the source-backed 09:01 minute price for execution; v3 caps quantity at 50 percent of observed minute volume and the session NAV risk budget. Historical prices remain proxies, never proof of queue priority, broker buying power, or a guaranteed exchange fill",
            "bracket_fill": "each mode moves TP and the local SL trigger one legal dated TW tick inward; this improves fill probability but does not guarantee a fill without a trigger and executable counterparty volume",
            "exit_schedule": "from 13:20 through 13:23 each unfilled exit is checked for a real cross and otherwise cancel-repriced once per new minute to the current passive best ask for a sell or best bid for a buy-to-cover; at 13:24 it is replaced by a marketable exit attempt",
            "terminal_flatten": "v3 preserves unfilled delivery obligations after the sourced 13:30 auction and blocks new exposure; only legacy ledgers contain explicitly tagged synthetic terminal valuation, never an exchange fill",
        },
    }


def build_dashboard_signal_page(
    *,
    state_dir: Path,
    session_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    mode: str = "",
    symbol: str = "",
    status: str = "all",
    offset: int = 0,
    limit: int = 250,
    maximum_scan_rows: int = 100_000,
) -> dict[str, Any]:
    """Return a bounded, server-filtered page from the append-only signal ledger."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    root = Path(state_dir)
    state = _object(root / "state.json")
    state_signal_signature = tuple(
        sorted(
            (
                str(market),
                str(raw_mode.get("session_date") or ""),
                str(raw_mode.get("initial_capital_twd") or ""),
                str(raw_mode.get("signal_source_path") or ""),
            )
            for market, raw_mode in (state.get("modes") or {}).items()
            if isinstance(raw_mode, Mapping)
        )
    )
    current_state_dates = sorted(
        {
            str(raw_mode.get("session_date") or "")[:10]
            for raw_mode in (state.get("modes") or {}).values()
            if isinstance(raw_mode, Mapping) and raw_mode.get("session_date")
        }
    )
    latest_state_date = current_state_dates[-1] if current_state_dates else None
    requested_single_date = (
        str(start_date)
        if start_date and end_date and str(start_date) == str(end_date)
        else str(session_date)
        if session_date
        else None
    )
    use_latest_session_fast_path = bool(
        requested_single_date
        and latest_state_date
        and requested_single_date == latest_state_date
    )
    observed = datetime.now(timezone.utc)
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=observed,
        include_ledger_dates=not use_latest_session_fast_path,
        # Signal/position archives already enumerate execution sessions. The
        # benchmark curve must not be decoded just to populate a detail filter.
        include_benchmark_history_dates=False,
        include_preopen_session=bool(start_date or end_date or session_date),
        ledger_filenames=("signals.jsonl",),
    )
    selected_start_date, selected_end_date, selected_session_dates = (
        _select_session_range(
            start_date=start_date,
            end_date=end_date,
            session_date=session_date,
            available=available_session_dates,
        )
    )
    normalized_mode = str(mode or "").strip()
    normalized_symbol = str(symbol or "").strip().casefold()
    normalized_status = str(status or "all").strip().casefold()
    signal_path = root / "signals.jsonl"
    signal_stat = signal_path.stat()
    formal_signal_path = root / OVERNIGHT_SIGNAL_HISTORY_FILENAME
    try:
        formal_signal_stat = formal_signal_path.stat()
        formal_signal_signature = (
            formal_signal_stat.st_dev,
            formal_signal_stat.st_ino,
            formal_signal_stat.st_size,
            formal_signal_stat.st_mtime_ns,
        )
    except FileNotFoundError:
        formal_signal_signature = None
    formal_signal_record_count = 0
    overnight_history_path = root / OVERNIGHT_HISTORY_FILENAME
    if overnight_history_path.is_file():
        overnight_history = _object(overnight_history_path)
        if overnight_history.get("product") == "tw_overnight":
            formal_signal_record_count = int(overnight_history.get("signal_count") or 0)
    futures_catalog = load_stock_futures_catalog()
    cache_key = (
        root.resolve(),
        signal_stat.st_dev,
        signal_stat.st_ino,
        signal_stat.st_size,
        signal_stat.st_mtime_ns,
        formal_signal_signature,
        tuple(selected_session_dates),
        normalized_mode,
        normalized_symbol,
        normalized_status,
        int(offset),
        int(limit),
        int(maximum_scan_rows),
        state_signal_signature,
        futures_catalog.revision,
    )
    with _SIGNAL_PAGE_CACHE_LOCK:
        cached_page = _SIGNAL_PAGE_CACHE.get(cache_key)
        if cached_page is not None:
            return dict(cached_page)
    signal_frame: Any | None = None
    polars_module: Any | None = None
    if use_latest_session_fast_path and selected_session_dates == [
        requested_single_date
    ]:
        rows_by_session = {
            requested_single_date: tuple(
                _latest_contiguous_session_rows(
                    signal_path,
                    requested_single_date,
                    maximum_scan_rows,
                )
            )
        }
    else:
        signal_index = _ledger_session_index(
            signal_path,
            recorded_at_fallback=False,
        )
        selected_spans = (
            sorted(
                (start, end, session_date)
                for session_date in selected_session_dates
                for start, end in signal_index.spans.get(session_date, ())
            )
            if signal_index is not None
            else []
        )
        columnar = _columnar_ledger_frame(
            signal_path,
            selected_spans=selected_spans,
            maximum_rows=maximum_scan_rows,
            projected_schema=_SIGNAL_PAGE_COLUMN_TYPES,
        )
        if columnar is not None:
            polars_module, candidate_frame = columnar
            required_columns = {
                "ask",
                "bid",
                "execution_price",
                "filled_shares",
                "filled_weight",
                "market",
                "reason",
                "requested_shares",
                "session_date",
                "signal_id",
                "sizing_open_price",
                "status",
                "symbol",
                "target_weight",
            }
            if required_columns.issubset(candidate_frame.columns):
                signal_frame = candidate_frame
        if signal_frame is None:
            rows_by_session = _rows_for_sessions(
                signal_path,
                selected_session_dates,
                maximum_scan_rows,
            )

    current_rows: list[dict[str, Any]] = []
    if signal_frame is None:
        current_rows = [
            row
            for selected_date in selected_session_dates
            for row in rows_by_session.get(selected_date, ())
        ]
    formal_rows, formal_signal_total, _ = _bounded_parquet_history_rows(
        formal_signal_path,
        session_dates=selected_session_dates,
        maximum_rows=maximum_scan_rows,
    )
    if formal_rows:
        if signal_frame is not None:
            current_rows = signal_frame.to_dicts()
            signal_frame = None
            polars_module = None
        # A current real-time row supersedes the counterfactual row for the
        # same signal identity.  Historical data never overwrite live proof.
        combined = formal_rows + current_rows
        deduplicated_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in combined:
            identity = (
                str(row.get("session_date") or "")[:10],
                str(row.get("market") or ""),
                str(row.get("symbol") or ""),
            )
            deduplicated_rows[identity] = row
        current_rows = list(deduplicated_rows.values())
        if len(current_rows) > maximum_scan_rows:
            current_rows.sort(
                key=lambda row: (
                    str(row.get("session_date") or ""),
                    str(row.get("market") or ""),
                    str(row.get("symbol") or ""),
                )
            )
            current_rows = current_rows[-maximum_scan_rows:]

    def included(row: Mapping[str, Any]) -> bool:
        if normalized_mode and normalized_mode != "all":
            if str(row.get("market") or "") != normalized_mode:
                return False
        if normalized_symbol:
            haystack = f"{row.get('symbol') or ''} {row.get('name') or ''}".casefold()
            if normalized_symbol not in haystack:
                return False
        if normalized_status == "blocked":
            return str(row.get("status") or "") not in {
                "ready",
                "partial_depth",
                "hold",
            }
        return True

    def sort_key(row: Mapping[str, Any]) -> tuple[float, int, float, str, str]:
        weight = _finite_float(row.get("target_weight"))
        resolved = weight if weight is not None else 0.0
        return (
            -abs(resolved),
            -int(str(row.get("session_date") or "0000-00-00").replace("-", "")),
            -resolved,
            str(row.get("market") or ""),
            str(row.get("symbol") or ""),
        )

    filtered: list[dict[str, Any]] = []
    filtered_frame: Any | None = None
    if signal_frame is None:
        filtered = [row for row in current_rows if included(row)]
        filtered.sort(key=sort_key)
        source_rows_scanned = len(current_rows)
        filtered_total = len(filtered)
    else:
        pl = polars_module
        predicate = pl.lit(True)
        if normalized_mode and normalized_mode != "all":
            predicate &= pl.col("market").fill_null("") == normalized_mode
        if normalized_symbol:
            # Keep Python's Unicode casefold semantics for textual searches.
            # Decode the already bounded frame once; date-only filtering takes
            # the native columnar path below without Python row materialization.
            decoded_rows_by_session: dict[str, list[dict[str, Any]]] = {}
            for row in signal_frame.to_dicts():
                decoded_rows_by_session.setdefault(
                    str(row.get("session_date") or "")[:10], []
                ).append(row)
            signal_frame = None
            current_rows = [
                row
                for selected_date in selected_session_dates
                for row in decoded_rows_by_session.get(selected_date, ())
            ]
            filtered = [row for row in current_rows if included(row)]
            filtered.sort(key=sort_key)
            source_rows_scanned = len(current_rows)
            filtered_total = len(filtered)
        else:
            if normalized_status == "blocked":
                predicate &= ~pl.col("status").fill_null("").is_in(
                    ["ready", "partial_depth", "hold"]
                )
            resolved_weight = (
                pl.col("target_weight").cast(pl.Float64, strict=False).fill_null(0.0)
            )
            filtered_frame = (
                signal_frame.filter(predicate)
                .with_columns(
                    resolved_weight.alias("__resolved_weight"),
                    resolved_weight.abs().alias("__absolute_weight"),
                )
                .sort(
                    [
                        "__absolute_weight",
                        "session_date",
                        "__resolved_weight",
                        "market",
                        "symbol",
                    ],
                    descending=[True, True, True, False, False],
                    nulls_last=True,
                )
            )
            source_rows_scanned = signal_frame.height
            filtered_total = filtered_frame.height
    capitals = {
        str(market): _finite_float(raw_mode.get("initial_capital_twd"))
        for market, raw_mode in (state.get("modes") or {}).items()
        if isinstance(raw_mode, Mapping)
    }
    current_signal_ids: dict[tuple[str, str], str] = {}
    if signal_frame is not None:
        for row_date, market, signal_id in signal_frame.select(
            "session_date", "market", "signal_id"
        ).iter_rows():
            row_date = str(row_date or "")[:10]
            market = str(market or "")
            signal_id = str(signal_id or "")
            if row_date and market and signal_id:
                current_signal_ids[(row_date, market)] = signal_id
    else:
        for row in current_rows:
            row_date = str(row.get("session_date") or "")[:10]
            market = str(row.get("market") or "")
            signal_id = str(row.get("signal_id") or "")
            if row_date and market and signal_id:
                current_signal_ids[(row_date, market)] = signal_id
    direction_summary: dict[str, dict[str, float | int]] = {
        stage: {
            "long_count": 0,
            "short_count": 0,
            "long_gross": 0.0,
            "short_gross": 0.0,
        }
        for stage in ("target", "actual")
    }
    summary_columns = [
        "session_date",
        "market",
        "signal_id",
        "target_weight",
        "filled_weight",
        "filled_shares",
        "ask",
        "bid",
        "sizing_open_price",
        "execution_price",
        "requested_shares",
        "reason",
        "status",
        "symbol",
    ]
    if (
        filtered_frame is not None
        and "inventory_weight_after" in filtered_frame.columns
    ):
        summary_columns.append("inventory_weight_after")

    def summary_rows():
        candidates = (
            filtered_frame.select(summary_columns).iter_rows(named=True)
            if filtered_frame is not None
            else iter(filtered)
        )
        for row in candidates:
            identity = (
                str(row.get("session_date") or "")[:10],
                str(row.get("market") or ""),
            )
            current_signal_id = current_signal_ids.get(identity)
            if (
                not current_signal_id
                or str(row.get("signal_id") or "") == current_signal_id
            ):
                yield row

    opening_execution_audit: dict[str, dict[str, Any]] = {}
    for row in summary_rows():
        target = _finite_float(row.get("target_weight")) or 0.0
        market = str(row.get("market") or "unknown")
        capital = _finite_float(row.get("sizing_capital_twd")) or capitals.get(market)
        entry_price = _finite_float(row.get("ask") if target > 0.0 else row.get("bid"))

        def executed_weight(explicit_key: str, shares_key: str) -> float:
            explicit = _finite_float(row.get(explicit_key))
            if explicit is not None:
                return explicit
            shares = _finite_float(row.get(shares_key)) or 0.0
            if not capital or not entry_price or target == 0.0:
                return 0.0
            return math.copysign(shares * entry_price / capital, target)

        values = {
            "target": target,
            "actual": (
                _finite_float(row.get("inventory_weight_after"))
                if row.get("inventory_weight_after") is not None
                else executed_weight("filled_weight", "filled_shares")
            ),
        }
        for stage, value in values.items():
            if value > 0.0:
                direction_summary[stage]["long_count"] += 1
                direction_summary[stage]["long_gross"] += value
            elif value < 0.0:
                direction_summary[stage]["short_count"] += 1
                direction_summary[stage]["short_gross"] += -value
        audit = opening_execution_audit.setdefault(
            market,
            {
                "model_signal_row_count": 0,
                "zero_target_row_count": 0,
                "nonzero_signal_count": 0,
                "opening_price_covered_count": 0,
                "opening_price_missing_count": 0,
                "execution_price_covered_count": 0,
                "requested_signal_count": 0,
                "filled_signal_count": 0,
                "unfilled_signal_count": 0,
                "below_one_board_lot_count": 0,
                "missing_open_symbols": [],
                "unfilled_reason_counts": {},
            },
        )
        audit["model_signal_row_count"] += 1
        if target == 0.0:
            audit["zero_target_row_count"] += 1
            continue
        audit["nonzero_signal_count"] += 1
        sizing_open = _finite_float(row.get("sizing_open_price"))
        if sizing_open is not None and sizing_open > 0.0:
            audit["opening_price_covered_count"] += 1
        else:
            audit["opening_price_missing_count"] += 1
            missing_symbols = audit["missing_open_symbols"]
            if len(missing_symbols) < 50:
                missing_symbols.append(str(row.get("symbol") or ""))
        execution_price = _finite_float(row.get("execution_price"))
        if execution_price is not None and execution_price > 0.0:
            audit["execution_price_covered_count"] += 1
        if int(row.get("requested_shares") or 0) > 0:
            audit["requested_signal_count"] += 1
        if int(row.get("filled_shares") or 0) > 0:
            audit["filled_signal_count"] += 1
        else:
            audit["unfilled_signal_count"] += 1
            reason = str(row.get("reason") or row.get("status") or "unknown")
            if reason == "below_one_board_lot":
                audit["below_one_board_lot_count"] += 1
            reason_counts = audit["unfilled_reason_counts"]
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

    single_selected_date = (
        selected_session_dates[0] if len(selected_session_dates) == 1 else None
    )
    for market, audit in opening_execution_audit.items():
        raw_mode = (state.get("modes") or {}).get(market)
        expected_rows = None
        if (
            single_selected_date
            and isinstance(raw_mode, Mapping)
            and str(raw_mode.get("session_date") or "") == single_selected_date
        ):
            expected_rows = int(raw_mode.get("target_symbol_count") or 0) or None
        audit["expected_model_signal_row_count"] = expected_rows
        audit["model_signal_rows_complete"] = (
            int(audit["model_signal_row_count"]) == expected_rows
            if expected_rows is not None
            else None
        )
        audit["unfilled_reason_counts"] = dict(
            sorted(
                audit["unfilled_reason_counts"].items(),
                key=lambda item: (-int(item[1]), str(item[0])),
            )
        )

    source_page = (
        filtered_frame.slice(offset, limit)
        .drop("__resolved_weight", "__absolute_weight")
        .to_dicts()
        if filtered_frame is not None
        else filtered[offset : offset + limit]
    )
    page: list[dict[str, Any]] = []
    for source_row in source_page:
        row = dict(source_row)
        row["stock_futures"] = futures_catalog.membership(str(row.get("symbol") or ""))
        if bool(row.get("counterfactual_open_replay")):
            row["open_reconstructed_at"] = row.get("open_reconstructed_at") or row.get(
                "signal_at"
            )
        page.append(row)
    feature_drivers_by_signal = _lookup_signal_feature_drivers(
        state_dir=root,
        state=state,
        rows=page,
    )
    payload = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "simulation_only": True,
        "production_order_possible": False,
        "session_date": selected_end_date,
        "start_date": selected_start_date,
        "end_date": selected_end_date,
        "session_dates": selected_session_dates,
        "available_session_dates": available_session_dates,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": filtered_total,
        "has_more": offset + len(page) < filtered_total,
        "source_rows_scanned": source_rows_scanned,
        "scan_limit": maximum_scan_rows,
        "scan_limit_reached": (
            source_rows_scanned >= maximum_scan_rows
            or formal_signal_total > len(formal_rows)
        ),
        "record_count": (
            _line_count(root / "signals.jsonl")
            + max(formal_signal_record_count, formal_signal_total)
        ),
        "direction_summary_scope": "current_signal_id_per_mode",
        "direction_summary": direction_summary,
        "opening_execution_audit_scope": "bounded_recent_signal_rows_per_mode"
        if source_rows_scanned >= maximum_scan_rows
        else "complete_current_signal_rows_per_mode",
        "opening_execution_audit": opening_execution_audit,
        "feature_drivers_scope": "all_feature_drivers_if_available_else_top_feature_drivers",
        "feature_drivers_by_signal": feature_drivers_by_signal,
        "rows": page,
    }
    with _SIGNAL_PAGE_CACHE_LOCK:
        if len(_SIGNAL_PAGE_CACHE) >= 128:
            _SIGNAL_PAGE_CACHE.pop(next(iter(_SIGNAL_PAGE_CACHE)))
        _SIGNAL_PAGE_CACHE[cache_key] = payload
    return dict(payload)


def build_dashboard_position_page(
    *,
    state_dir: Path,
    session_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    mode: str = "",
    symbol: str = "",
    status: str = "all",
    offset: int = 0,
    limit: int = 250,
) -> dict[str, Any]:
    """Return bounded position snapshots for an inclusive session range."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    root = Path(state_dir)
    state = _object(root / "state.json")
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=datetime.now(timezone.utc),
        include_ledger_dates=not bool(start_date or end_date or session_date),
        include_benchmark_history_dates=False,
        include_preopen_session=bool(start_date or end_date or session_date),
    )
    selected_start_date, selected_end_date, selected_session_dates = (
        _select_session_range(
            start_date=start_date,
            end_date=end_date,
            session_date=session_date,
            available=available_session_dates,
        )
    )
    selected_date_set = set(selected_session_dates)
    history_index = _position_history_index(root)
    deduplicated: dict[str, _PositionHistoryEntry | dict[str, Any]] = {
        entry.identity: entry
        for entry in history_index.entries
        if entry.session_date in selected_date_set
    }
    for raw_mode in (state.get("modes") or {}).values():
        if not isinstance(raw_mode, Mapping):
            continue
        for position in (raw_mode.get("positions") or {}).values():
            if not isinstance(position, Mapping):
                continue
            safe = _safe_position(position)
            if str(safe.get("session_date") or "")[:10] in selected_date_set:
                identity = str(
                    safe.get("position_id")
                    or f"{safe.get('session_date')}:{safe.get('market')}:{safe.get('symbol')}"
                )
                deduplicated[identity] = safe
    normalized_mode = str(mode or "").strip()
    normalized_symbol = str(symbol or "").strip().casefold()
    normalized_status = str(status or "all").strip().casefold()

    def field(row: _PositionHistoryEntry | Mapping[str, Any], key: str) -> Any:
        return getattr(row, key) if isinstance(row, _PositionHistoryEntry) else row.get(key)

    def included(row: _PositionHistoryEntry | Mapping[str, Any]) -> bool:
        if normalized_mode and normalized_mode != "all":
            if str(field(row, "market") or "") != normalized_mode:
                return False
        if normalized_symbol:
            haystack = (
                f"{field(row, 'symbol') or ''} {field(row, 'name') or ''}".casefold()
            )
            if normalized_symbol not in haystack:
                return False
        signed_shares = int(field(row, "signed_shares") or 0)
        if normalized_status == "open":
            return signed_shares != 0
        if normalized_status == "closed":
            return signed_shares == 0
        if normalized_status == "blocked":
            return False
        return True

    def sort_key(row: _PositionHistoryEntry | Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            -int(str(field(row, "session_date") or "0000-00-00").replace("-", "")),
            str(field(row, "market") or ""),
            -abs(_finite_float(field(row, "target_weight")) or 0.0),
            str(field(row, "symbol") or ""),
        )

    filtered = (row for row in deduplicated.values() if included(row))
    # Keep only the requested prefix. For the common first page this holds 100
    # compact locators instead of 45k wide position dictionaries.
    requested_prefix = heapq.nsmallest(offset + limit, filtered, key=sort_key)
    selected = requested_prefix[offset : offset + limit]
    selected_history = [
        row for row in selected if isinstance(row, _PositionHistoryEntry)
    ]
    hydrated = _rehydrate_position_entries(history_index, selected_history)
    page = [
        hydrated[row.identity] if isinstance(row, _PositionHistoryEntry) else row
        for row in selected
    ]
    filtered_total = sum(included(row) for row in deduplicated.values())
    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "simulation_only": True,
        "production_order_possible": False,
        "session_date": selected_end_date,
        "start_date": selected_start_date,
        "end_date": selected_end_date,
        "session_dates": selected_session_dates,
        "available_session_dates": available_session_dates,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": filtered_total,
        "has_more": offset + len(page) < filtered_total,
        "record_count": len(deduplicated),
        "rows": page,
    }


def build_dashboard_event_page(
    *,
    state_dir: Path,
    session_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    mode: str = "",
    symbol: str = "",
    offset: int = 0,
    limit: int = 250,
    maximum_scan_rows: int = 100_000,
) -> dict[str, Any]:
    """Return a bounded page from the selected day's order and fill ledgers."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    root = Path(state_dir)
    state = _object(root / "state.json")
    observed = datetime.now(timezone.utc)
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=observed,
        include_preopen_session=bool(start_date or end_date or session_date),
        include_benchmark_history_dates=False,
        ledger_filenames=("orders.jsonl", "fills.jsonl"),
    )
    selected_start_date, selected_end_date, selected_session_dates = (
        _select_session_range(
            start_date=start_date,
            end_date=end_date,
            session_date=session_date,
            available=available_session_dates,
        )
    )
    normalized_mode = str(mode or "").strip()
    normalized_symbol = str(symbol or "").strip().casefold()

    allowed_fields = (
        "recorded_at",
        "fill_at",
        "quote_at",
        "session_date",
        "market",
        "symbol",
        "side",
        "purpose",
        "order_type",
        "price",
        "quantity",
        "requested_quantity",
        "remaining_quantity",
        "filled_quantity",
        "unfilled_quantity",
        "status",
        "price_limit_offset_ticks",
        "pricing_rule",
        "gross_pnl_twd",
        "net_pnl_twd",
        "fee_and_tax_twd",
        "gross_fee_and_tax_twd",
        "commission_rebate_accrued_twd",
        "simulation_only",
        "simulation_replay",
        "replay_basis",
        "fill_contract",
        "depth_assumption",
    )

    def safe_event(row: Mapping[str, Any], event_kind: str) -> dict[str, Any]:
        event = {key: row.get(key) for key in allowed_fields if key in row}
        event["event_kind"] = event_kind
        if event_kind == "fill":
            event["order_type"] = "FILL"
            event["status"] = "filled"
        return event

    def included(row: Mapping[str, Any]) -> bool:
        if normalized_mode and normalized_mode != "all":
            if str(row.get("market") or "") != normalized_mode:
                return False
        if normalized_symbol:
            haystack = str(row.get("symbol") or "").casefold()
            if normalized_symbol not in haystack:
                return False
        return True

    requested_prefix_size = offset + limit
    retained: list[tuple[tuple[str, str, str, str], int, dict[str, Any]]] = []
    seen: set[tuple[str, ...]] = set()
    sequence = 0
    total_rows = 0
    order_total = 0
    fill_total = 0

    def consider(event: dict[str, Any], event_kind: str) -> None:
        nonlocal sequence, total_rows, order_total, fill_total
        if not included(event):
            return
        identity = (
            str(event.get("event_kind") or ""),
            str(event.get("recorded_at") or ""),
            str(event.get("fill_at") or ""),
            str(event.get("market") or ""),
            str(event.get("symbol") or ""),
            str(event.get("purpose") or ""),
            str(event.get("status") or ""),
            str(event.get("quantity") or ""),
            str(event.get("price") or ""),
        )
        if identity in seen:
            return
        seen.add(identity)
        total_rows += 1
        if event_kind == "fill":
            fill_total += 1
        else:
            order_total += 1
        sort_key = (
            str(event.get("fill_at") or event.get("recorded_at") or ""),
            str(event.get("event_kind") or ""),
            str(event.get("market") or ""),
            str(event.get("symbol") or ""),
        )
        item = (sort_key, sequence, event)
        sequence += 1
        if len(retained) < requested_prefix_size:
            heapq.heappush(retained, item)
        elif requested_prefix_size and item[0] > retained[0][0]:
            heapq.heapreplace(retained, item)

    def consume(source_rows: Any, fixed_event_kind: str | None = None) -> None:
        for source in source_rows:
            event_kind = fixed_event_kind or str(source.get("event_kind") or "order")
            consider(safe_event(source, event_kind), event_kind)

    def consume_batches(batches: Any, event_kind: str) -> None:
        """Aggregate Arrow scalars and materialize only heap candidates."""

        nonlocal sequence, total_rows, order_total, fill_total
        identity_names = (
            "recorded_at",
            "fill_at",
            "market",
            "symbol",
            "purpose",
            "status",
            "quantity",
            "price",
        )
        for batch in batches:
            available = set(batch.schema.names)
            identity_columns = {
                name: (
                    batch.column(batch.schema.get_field_index(name)).to_pylist()
                    if name in available
                    else [None] * batch.num_rows
                )
                for name in identity_names
            }
            for row_index in range(batch.num_rows):
                market_value = identity_columns["market"][row_index]
                symbol_value = identity_columns["symbol"][row_index]
                if normalized_mode and normalized_mode != "all":
                    if str(market_value or "") != normalized_mode:
                        continue
                if normalized_symbol and normalized_symbol not in str(
                    symbol_value or ""
                ).casefold():
                    continue
                status_value = (
                    "filled"
                    if event_kind == "fill"
                    else identity_columns["status"][row_index]
                )
                identity = (
                    event_kind,
                    str(identity_columns["recorded_at"][row_index] or ""),
                    str(identity_columns["fill_at"][row_index] or ""),
                    str(market_value or ""),
                    str(symbol_value or ""),
                    str(identity_columns["purpose"][row_index] or ""),
                    str(status_value or ""),
                    str(identity_columns["quantity"][row_index] or ""),
                    str(identity_columns["price"][row_index] or ""),
                )
                if identity in seen:
                    continue
                seen.add(identity)
                total_rows += 1
                if event_kind == "fill":
                    fill_total += 1
                else:
                    order_total += 1
                sort_key = (
                    str(
                        identity_columns["fill_at"][row_index]
                        or identity_columns["recorded_at"][row_index]
                        or ""
                    ),
                    event_kind,
                    str(market_value or ""),
                    str(symbol_value or ""),
                )
                should_retain = len(retained) < requested_prefix_size or (
                    requested_prefix_size and sort_key > retained[0][0]
                )
                if should_retain:
                    source = {}
                    for column_index, name in enumerate(batch.schema.names):
                        value = batch.column(column_index)[row_index].as_py()
                        if value is not None:
                            source[name] = value
                    event = safe_event(source, event_kind)
                    item = (sort_key, sequence, event)
                    if len(retained) < requested_prefix_size:
                        heapq.heappush(retained, item)
                    else:
                        heapq.heapreplace(retained, item)
                sequence += 1

    for filename, event_kind in (("orders.jsonl", "order"), ("fills.jsonl", "fill")):
        path = root / filename
        index = _ledger_session_index(path, recorded_at_fallback=False)
        selected_spans = (
            sorted(
                (start, end, session_date)
                for session_date in selected_session_dates
                for start, end in index.spans.get(session_date, ())
            )
            if index is not None
            else []
        )
        with _projected_recent_ledger_batches(
            path,
            selected_spans=selected_spans,
            maximum_rows=maximum_scan_rows,
            projected_schema=_EVENT_PAGE_COLUMN_TYPES,
        ) as projected_batches:
            if projected_batches is not None:
                consume_batches(projected_batches, event_kind)
                continue
        rows_by_session = _rows_for_sessions(
            path,
            selected_session_dates,
            maximum_scan_rows,
            projected_schema=_EVENT_PAGE_COLUMN_TYPES,
        )
        consume(
            (
                row
                for selected_date in reversed(selected_session_dates)
                for row in reversed(rows_by_session.get(selected_date, ()))
            ),
            event_kind,
        )
    formal_rows, formal_event_total, _ = _bounded_parquet_history_rows(
        root / OVERNIGHT_EVENT_HISTORY_FILENAME,
        session_dates=selected_session_dates,
        maximum_rows=maximum_scan_rows,
    )
    consume(reversed(formal_rows))
    retained.sort(key=lambda item: (item[0], item[1]), reverse=True)
    page = [item[2] for item in retained[offset : offset + limit]]
    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "simulation_only": True,
        "production_order_possible": False,
        "session_date": selected_end_date,
        "start_date": selected_start_date,
        "end_date": selected_end_date,
        "session_dates": selected_session_dates,
        "available_session_dates": available_session_dates,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": total_rows,
        "order_total": order_total,
        "fill_total": fill_total,
        "has_more": offset + len(page) < total_rows,
        "record_counts": {
            "orders": _line_count(root / "orders.jsonl")
            + sum(row.get("event_kind") == "order" for row in formal_rows),
            "fills": _line_count(root / "fills.jsonl")
            + sum(row.get("event_kind") == "fill" for row in formal_rows),
            "formal_selected_rows": formal_event_total,
        },
        "rows": page,
    }


def build_dashboard_summary(
    *,
    state_dir: Path,
    preopen_readiness_path: Path | None = None,
    session_date: str | None = None,
    now: datetime | None = None,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    include_ledger_session_dates: bool = True,
) -> dict[str, Any]:
    """Return the frequently refreshed operational subset of the dashboard."""

    snapshot = build_dashboard_snapshot(
        state_dir=state_dir,
        preopen_readiness_path=preopen_readiness_path,
        session_date=session_date,
        now=now,
        max_source_age_seconds=max_source_age_seconds,
        maximum_signal_rows=0,
        maximum_event_rows=500,
        maximum_mark_rows=4_000,
        include_position_rows=False,
        include_ledger_session_dates=include_ledger_session_dates,
    )
    keys = (
        "schema_version",
        "generated_at_utc",
        "health",
        "source_updated_at",
        "source_age_seconds",
        "service_sync",
        "unattended_guardian",
        "session_date",
        "available_session_dates",
        "preopen",
        "operational_issues",
        "execution_records",
        "latency",
        "today_latency",
        "opening_signal_latency",
        "session_progress",
        "modes",
        "record_counts",
        "record_counts_ready",
    )
    return {key: snapshot.get(key) for key in keys}


def warm_dashboard_session_indexes(*, state_dir: Path) -> dict[str, int]:
    """Persist compact byte indexes for reboot-fast historical date reads."""

    root = Path(state_dir)
    benchmark_history = _benchmark_history_index(root)
    sources = (
        ("signals.jsonl", False),
        ("orders.jsonl", False),
        ("fills.jsonl", False),
        ("marks.jsonl", False),
        ("marks.jsonl", True),
        ("benchmark_marks.jsonl", False),
        ("benchmark_marks.jsonl", True),
        ("events.jsonl", True),
        ("latency.jsonl", True),
    )
    indexed: dict[str, int] = {}
    indexed[BENCHMARK_HISTORY_FILENAME] = len(benchmark_history.marks_by_session)
    indexed["position_history"] = len(_position_history_index(root).entries)
    for filename, recorded_at_fallback in sources:
        index = _ledger_session_index(
            root / filename,
            recorded_at_fallback=recorded_at_fallback,
        )
        if index is not None:
            suffix = ":recorded_at_fallback" if recorded_at_fallback else ""
            indexed[f"{filename}{suffix}"] = len(index.spans)
    for filename in {
        "signals.jsonl",
        "orders.jsonl",
        "fills.jsonl",
        "marks.jsonl",
        "benchmark_marks.jsonl",
        "events.jsonl",
        "latency.jsonl",
        "opening_signal_latency.jsonl",
    }:
        indexed[f"{filename}:line_count"] = _line_count(root / filename)
    indexed["historical_positions"] = _historical_position_count(root)
    return indexed


__all__ = [
    "build_dashboard_event_page",
    "build_dashboard_history_snapshot",
    "build_dashboard_position_page",
    "build_dashboard_revision",
    "build_dashboard_signal_page",
    "build_dashboard_snapshot",
    "build_dashboard_summary",
    "warm_dashboard_session_indexes",
]
