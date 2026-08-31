"""Read-only, source-backed dashboard snapshot for stock day-trade simulation."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date as datetime_date, datetime, time as datetime_time, timezone
import json
import math
from pathlib import Path
import threading
from typing import Any, Final
from zoneinfo import ZoneInfo

from stockagent.live.tw_day_trade_service_sync import (
    DISCORD_SERVICE_STATUS_FILENAME,
    age_seconds,
    load_service_sync,
    read_json_object,
)


DASHBOARD_SCHEMA_VERSION: Final[int] = 5
DEFAULT_MAX_SOURCE_AGE_SECONDS: Final[float] = 30.0
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
BENCHMARK_HISTORY_FILENAME: Final[str] = "benchmark_history.json"
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
_LINE_COUNT_CACHE: dict[Path, tuple[int, int, int, int]] = {}
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
_SIGNAL_FEATURE_SUMMARY_CACHE: dict[
    tuple[int, int, int, int], list[dict[str, Any]]
] = {}
_SIGNAL_FEATURE_SUMMARY_CACHE_LOCK = threading.Lock()
_AVAILABLE_SESSION_DATES_CACHE: dict[Path, tuple[tuple[Any, ...], list[str]]] = {}
_AVAILABLE_SESSION_DATES_CACHE_LOCK = threading.Lock()
_OBJECT_CACHE: dict[Path, tuple[int, int, int, int, dict[str, Any]]] = {}
_OBJECT_CACHE_LOCK = threading.Lock()
_SIGNAL_PAGE_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_SIGNAL_PAGE_CACHE_LOCK = threading.Lock()
_MAX_LEDGER_LINE_BYTES: Final[int] = 8 * 1024 * 1024


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


def build_dashboard_revision(
    *,
    state_dir: Path,
    discord_service_status_path: Path | None = None,
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
    bot_revision = int((bot or {}).get("engine_state_revision") or 0)
    engine_markets = sorted(str(item) for item in engine.get("enabled_markets") or ())
    bot_markets = sorted(
        str(item) for item in (bot or {}).get("day_trade_markets") or ()
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

    return {
        "schema_version": 1,
        "generated_at_utc": observed.isoformat(timespec="milliseconds"),
        "revision_token": f"{content_revision}:{preopen_revision}",
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
        },
        "status": status_text,
        "synchronized": synchronized,
        "revision_lag": max(0, engine_revision - bot_revision),
        "contract": (
            "Discord publishes immutable signals; the paper engine owns the ledger "
            "and publishes this commit revision last; the dashboard is read-only."
        ),
    }


def _object(path: Path) -> dict[str, Any]:
    cache_key = path.resolve()
    stat = path.stat()
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    with _OBJECT_CACHE_LOCK:
        cached = _OBJECT_CACHE.get(cache_key)
        if cached is not None and cached[:4] == signature:
            return dict(cached[4])
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    final_stat = path.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) == signature:
        with _OBJECT_CACHE_LOCK:
            _OBJECT_CACHE[cache_key] = (*signature, payload)
    return dict(payload)


def _unattended_guardian_status(
    *, path: Path, observed: datetime
) -> dict[str, Any]:
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
                isinstance(runtime_sync, Mapping)
                and runtime_sync.get("ready") is True
            ),
            "public_dashboard": bool(
                isinstance(public_dashboard, Mapping)
                and public_dashboard.get("ready") is True
            ),
            "post_close_flat": bool(
                isinstance(post_close, Mapping)
                and post_close.get("ready") is True
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
    if not recorded_at_fallback or not row.get("recorded_at"):
        return ""
    try:
        return _timestamp(row["recorded_at"]).astimezone(TAIPEI).date().isoformat()
    except (TypeError, ValueError):
        return ""


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
                    payload = json.loads(line)
                    if not isinstance(payload, dict):
                        scanned_offset = cursor
                        continue
                    session_date = _ledger_row_session_date(
                        payload,
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
            return result
    raise OSError(f"dashboard ledger changed repeatedly while indexing: {source}")


def _rows_for_sessions(
    path: Path,
    session_dates: list[str] | tuple[str, ...],
    maximum_rows: int,
    *,
    recorded_at_fallback: bool = False,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Read only byte spans belonging to the requested retained sessions."""

    selected_dates = tuple(
        dict.fromkeys(str(value) for value in session_dates if value)
    )
    if maximum_rows <= 0 or not selected_dates:
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
    retained: deque[tuple[str, dict[str, Any]]] = deque(maxlen=maximum_rows)
    source = Path(path)
    stat = source.stat()
    if (stat.st_dev, stat.st_ino) != (index.device, index.inode):
        raise OSError(f"dashboard ledger changed before indexed read: {source}")
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


def _line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    stat = path.stat()
    key = path.resolve()
    with _LINE_COUNT_LOCK:
        cached = _LINE_COUNT_CACHE.get(key)
        same_append_only_file = bool(
            cached
            and cached[0] == stat.st_dev
            and cached[1] == stat.st_ino
            and stat.st_size >= cached[2]
        )
        start = cached[2] if same_append_only_file and cached else 0
        count = cached[3] if same_append_only_file and cached else 0
        with path.open("rb") as handle:
            handle.seek(start)
            remaining = stat.st_size - start
            while remaining > 0:
                chunk = handle.read(min(1 << 20, remaining))
                if not chunk:
                    break
                count += chunk.count(b"\n")
                remaining -= len(chunk)
        _LINE_COUNT_CACHE[key] = (stat.st_dev, stat.st_ino, stat.st_size, count)
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
    initial = _finite_float(initial_capital)
    equity = _finite_float(total_equity)
    if initial is None or initial <= 0.0 or equity is None:
        return None, None
    return equity / initial - 1.0, (equity / initial - 1.0) * 100.0


def _load_benchmark_history(root: Path) -> dict[str, Any]:
    """Load the immutable benchmark origin without risking dashboard uptime."""

    path = root / BENCHMARK_HISTORY_FILENAME
    if not path.is_file():
        return {}
    try:
        payload = _object(path)
    except (OSError, json.JSONDecodeError, ValueError):
        return {"load_error": "benchmark_history_unavailable"}
    if int(payload.get("schema_version") or 0) != 1:
        return {"load_error": "benchmark_history_schema_unsupported"}
    return payload


def _rebase_live_benchmark(
    source: Mapping[str, Any],
    origin: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Re-anchor a live benchmark mark to the retained actual-open origin.

    The live engine remains untouched while positions are open.  This display
    projection adds the gross price move that happened before the live
    benchmark process started and replaces only its initial entry costs.  Roll
    PnL and all later fees/taxes stay exactly as recorded by the live ledger.
    """

    row = dict(source)
    if (
        str(row.get("instrument_type") or "").startswith("stock")
        and str(row.get("valuation_source") or "").startswith(
            "corporate_action_reference_unavailable"
        )
    ):
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
            "counterfactual_open_replay": True,
            "replay_basis": "actual_session_open_to_recorded_executable_marks",
            "valuation_source": (
                f"{row.get('valuation_source') or 'recorded_executable_mark'}"
                "+rebased_to_actual_session_open"
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
        "below_one_board_lot": (
            "warning",
            "訊號不足一張",
            "整股當沖最小單位為一張；目標股數不足時保持空倉。",
        ),
    }
    for mode in modes:
        market = str(mode.get("market") or "") or None
        label = str(mode.get("label") or market or "模式")
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
            add(
                severity="error",
                scope="mode",
                market=market,
                code=engine_status,
                title=f"{label} 執行器已阻擋",
                detail="執行器偵測到安全性或資料契約錯誤，未繼續建立新部位。",
                observed_at=mode.get("signal_at"),
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
    *, root: Path, state: Mapping[str, Any], observed: datetime
) -> list[str]:
    root = Path(root)
    mode_dates = tuple(
        sorted(
            {
                str(raw_mode["session_date"])[:10]
                for raw_mode in (state.get("modes") or {}).values()
                if isinstance(raw_mode, Mapping) and raw_mode.get("session_date")
            }
        )
    )
    tracked_filenames = (
        "marks.jsonl",
        "signals.jsonl",
        "orders.jsonl",
        "fills.jsonl",
        "benchmark_marks.jsonl",
        "events.jsonl",
        BENCHMARK_HISTORY_FILENAME,
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
    cache_signature: tuple[Any, ...] = (
        mode_dates,
        tuple((filename, signature(root / filename)) for filename in tracked_filenames),
        position_history_dates,
        observed.astimezone(TAIPEI).date().isoformat(),
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
    for filename in (
        "marks.jsonl",
        "signals.jsonl",
        "orders.jsonl",
        "fills.jsonl",
        "benchmark_marks.jsonl",
        "events.jsonl",
    ):
        # Core execution ledgers carry an explicit session_date and their
        # detail readers use that exact contract. Reuse the same compact index
        # instead of building a duplicate fallback index over every row.
        # events.jsonl is the sole retained legacy stream whose date is derived
        # from recorded_at in Taipei time.
        index = _ledger_session_index(
            root / filename,
            recorded_at_fallback=filename == "events.jsonl",
        )
        if index is not None:
            dates.update(index.spans)
    benchmark_history = _load_benchmark_history(root)
    for row in benchmark_history.get("marks") or ():
        if isinstance(row, Mapping) and row.get("session_date"):
            dates.add(str(row["session_date"])[:10])
    for raw_date in position_history_dates:
        try:
            datetime_date.fromisoformat(raw_date)
        except ValueError:
            continue
        dates.add(raw_date)
    local = observed.astimezone(TAIPEI)
    if not dates and local.weekday() < 5:
        dates.add(local.date().isoformat())
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


def _chart_timestamp(row: Mapping[str, Any]) -> float | None:
    for field in ("minute", "recorded_at"):
        value = row.get(field)
        if not value:
            continue
        try:
            return _timestamp(value).timestamp()
        except (TypeError, ValueError):
            continue
    return None


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
) -> dict[str, Any]:
    """Return cross-session strategy and total-return benchmark curves.

    Time windows are anchored to the newest retained observation rather than
    wall-clock time, so historical/replay ledgers remain inspectable.  Longer
    windows scan only the two append-only mark ledgers and are then bounded by
    extrema-preserving downsampling at the API boundary.
    """

    normalized_range = str(range_key or "1d").strip().lower()
    if normalized_range not in CHART_RANGE_SECONDS:
        raise ValueError(f"unsupported chart range: {range_key}")
    if not 100 <= int(maximum_points_per_series) <= 10_000:
        raise ValueError("maximum_points_per_series must be between 100 and 10000")
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
    root = Path(state_dir)
    benchmark_history = _load_benchmark_history(root)
    benchmark_origins = benchmark_history.get("origins") or {}
    deduplicated: dict[tuple[str, str], dict[str, Any]] = {}

    def add(source: Mapping[str, Any], *, series_type: str) -> None:
        row = dict(source)
        series_id = str(
            row.get("market")
            if series_type == "strategy"
            else row.get("benchmark_id") or ""
        )
        timestamp_seconds = _chart_timestamp(row)
        if not series_id or timestamp_seconds is None:
            return
        return_fraction, return_pct = _capital_return(
            row.get("initial_capital_twd"), row.get("total_equity_twd")
        )
        if return_pct is None:
            return_pct = _finite_float(row.get("return_pct"))
            return_fraction = _finite_float(row.get("return_fraction"))
        if return_pct is None:
            return
        if return_fraction is None:
            return_fraction = float(return_pct) / 100.0
        initial_capital = _finite_float(row.get("initial_capital_twd"))
        total_equity = _finite_float(row.get("total_equity_twd"))
        wealth_index = 1.0 + float(return_fraction)
        if not math.isfinite(wealth_index) or wealth_index <= 0.0:
            return
        if total_equity is None and initial_capital is not None:
            total_equity = initial_capital * wealth_index
        minute = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat(
            timespec="minutes"
        )
        session_date = (
            datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
            .astimezone(TAIPEI)
            .date()
            .isoformat()
        )
        deduplicated[(series_id, minute)] = {
            "series_id": series_id,
            "series_type": series_type,
            "market": row.get("market") if series_type == "strategy" else None,
            "benchmark_id": (
                row.get("benchmark_id") if series_type == "benchmark" else None
            ),
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
            "valuation_stale": bool(row.get("valuation_stale", False)),
            "historical_minute_replay": bool(
                row.get("historical_minute_replay", False)
            ),
            "minute_valuation_contract": row.get("minute_valuation_contract"),
            "valuation_source": row.get("valuation_source"),
            "valuation_executable": row.get("valuation_executable"),
            "fresh_trade_position_count": row.get("fresh_trade_position_count"),
            "last_trade_carried_position_count": row.get(
                "last_trade_carried_position_count"
            ),
            "missing_price_position_count": row.get("missing_price_position_count"),
            "fresh_trade_notional_coverage_ratio": row.get(
                "fresh_trade_notional_coverage_ratio"
            ),
        }

    for row in _all_json_objects(root / "marks.jsonl") or ():
        add(row, series_type="strategy")
    for row in benchmark_history.get("marks") or ():
        if isinstance(row, Mapping):
            add(row, series_type="benchmark")
    for source in _all_json_objects(root / "benchmark_marks.jsonl") or ():
        benchmark_id = str(source.get("benchmark_id") or "")
        add(
            _rebase_live_benchmark(source, benchmark_origins.get(benchmark_id)),
            series_type="benchmark",
        )

    all_rows = sorted(
        deduplicated.values(),
        key=lambda row: (float(row["timestamp_seconds"]), str(row["series_id"])),
    )
    rows = list(all_rows)
    available_dates = [
        datetime.fromtimestamp(float(row["timestamp_seconds"]), tz=timezone.utc)
        .astimezone(TAIPEI)
        .date()
        for row in rows
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

    # A detail-date selection is a period-return question.  Rebase every series
    # to its last retained equity before the selected start date; if the ledger
    # begins inside the requested range, its explicit initial capital is the
    # only valid fallback.  This keeps cards, legend values, and chart points on
    # exactly the same denominator without manufacturing an opening zero point.
    baseline_by_series: dict[str, dict[str, Any]] = {}
    if selected_start is not None:
        for row in all_rows:
            if datetime_date.fromisoformat(str(row["session_date"])) >= selected_start:
                continue
            baseline_by_series[str(row["series_id"])] = row
        for row in rows:
            baseline = baseline_by_series.get(str(row["series_id"]))
            baseline_wealth = (
                float(baseline["_wealth_index"]) if baseline is not None else 1.0
            )
            row_wealth = float(row["_wealth_index"])
            range_return = row_wealth / baseline_wealth - 1.0
            row["return_fraction"] = range_return
            row["return_pct"] = range_return * 100.0

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
        series_rows.sort(key=lambda row: float(row["timestamp_seconds"]))
        first = series_rows[0]
        last = series_rows[-1]
        baseline = baseline_by_series.get(series_id)
        baseline_wealth = (
            float(baseline["_wealth_index"])
            if selected_start is not None and baseline is not None
            else 1.0
        )
        initial_capital = _finite_float(first.get("_initial_capital_twd"))
        baseline_equity = (
            _finite_float(baseline.get("_total_equity_twd"))
            if baseline is not None
            else initial_capital
        )
        end_equity = _finite_float(last.get("_total_equity_twd"))
        if baseline_equity is None and initial_capital is not None:
            baseline_equity = initial_capital * baseline_wealth
        if end_equity is None and initial_capital is not None:
            end_equity = initial_capital * float(last["_wealth_index"])
        session_point_counts: dict[str, int] = {}
        for row in series_rows:
            session = str(row["session_date"])
            session_point_counts[session] = session_point_counts.get(session, 0) + 1
        # The strategy is 09:01..13:30 because there is deliberately no
        # fabricated 09:00 position. Cash benchmarks retain the 09:00 official
        # open, while the right-labelled TX day session covers 08:46..13:45.
        points_per_session = (
            270
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
                "baseline_kind": (
                    "previous_retained_mark"
                    if selected_start is not None and baseline is not None
                    else "initial_capital"
                ),
                "baseline_at_utc": baseline.get("minute") if baseline else None,
                "baseline_equity_twd": baseline_equity,
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
    sampled = [
        row
        for series_rows in grouped.values()
        for row in _downsample_chart_series(
            series_rows, maximum_points=int(maximum_points_per_series)
        )
    ]
    sampled.sort(key=lambda row: (float(row["timestamp_seconds"]), row["series_id"]))
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
    return {
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
        "curve_granularity": "1m",
        "expected_right_labelled_session_minute_points": 270,
        "expected_strategy_session_points_from_09_01": 270,
        "expected_stock_benchmark_session_points_including_09_00": 271,
        "expected_tx_day_session_points": 300,
        "return_basis": "previous_retained_mark_before_start_else_initial_capital",
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
        "history": sampled,
    }


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
        final_arm_requested = int(
            final_arm_quote_prewarm.get("requested_count") or 0
        )
        final_arm_run_id = str(final_arm.get("run_id") or "")
        final_arm_contract_ready = bool(
            final_arm.get("status") == "ready"
            and final_arm_run_id
            and _is_taipei_session_date(
                final_arm.get("completed_at"), session_date
            )
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
            status == "failed"
            and str(mode.get("engine_status") or "") in {"active", "completed"}
            and str(mode.get("session_date") or "") == session_date
            and recovered_signal_id
            and _is_taipei_session_date(recovered_at, session_date)
        )
        if recovered_late:
            # Preserve the failed preparation stage while reflecting the newer
            # durable engine fact.  The separate opening gate still records a
            # missed 09:00 SLO, so this never rewrites a late recovery as an
            # on-time preopen success.
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
                    dict(final_arm.get("tw_mis_fallback_prewarm") or {}).get(
                        "ready"
                    )
                    if isinstance(
                        final_arm.get("tw_mis_fallback_prewarm"), Mapping
                    )
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
                    else
                    "盤前公開資料、特徵或模型準備失敗；請查看此模式並等待重新驗證。"
                    if status == "failed"
                    else None
                ),
            }
        )

    ready_count = sum(
        row["status"] in {"ready", "recovered_late"} for row in rows
    )
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
    )
    row = {key: position.get(key) for key in allowed if key in position}
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
    session_root = root / "position_history" / session_date
    if not session_root.is_dir():
        return rows
    for path in sorted(session_root.glob("*.json")):
        payload = _object(path)
        if str(payload.get("session_date") or "") != session_date:
            continue
        for position in payload.get("positions") or ():
            if isinstance(position, Mapping):
                rows.append(_safe_position(position))
    return rows


def build_dashboard_snapshot(
    *,
    state_dir: Path,
    preopen_readiness_path: Path | None = None,
    session_date: str | None = None,
    now: datetime | None = None,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    maximum_signal_rows: int = 0,
    maximum_event_rows: int = 2_000,
    maximum_mark_rows: int = 4_000,
    include_position_rows: bool = True,
    unattended_guardian_path: Path = DEFAULT_UNATTENDED_GUARDIAN_PATH,
) -> dict[str, Any]:
    root = Path(state_dir)
    state = _object(root / "state.json")
    status = _object(root / "status.json")
    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    discord_service_status_path = (
        Path(preopen_readiness_path).with_name(DISCORD_SERVICE_STATUS_FILENAME)
        if preopen_readiness_path is not None
        else None
    )
    service_sync = build_dashboard_revision(
        state_dir=root,
        discord_service_status_path=discord_service_status_path,
        now=observed,
    )
    unattended_guardian = _unattended_guardian_status(
        path=Path(unattended_guardian_path), observed=observed
    )
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=observed,
    )
    selected_session_date = _select_session_date(
        session_date,
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
    benchmark_history = _load_benchmark_history(root)
    benchmark_origins = benchmark_history.get("origins") or {}
    benchmark_history_marks = [
        dict(row)
        for row in (benchmark_history.get("marks") or ())
        if isinstance(row, Mapping)
        and str(row.get("session_date") or "") == selected_session_date
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
                "entry_fill_outcome": entry_fill_outcome,
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "total_equity_twd": mode.get("total_equity_twd"),
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
        benchmark = _rebase_live_benchmark(
            raw_benchmark,
            benchmark_origins.get(str(benchmark_id)),
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

    signals = current(_tail(root / "signals.jsonl", maximum_signal_rows))
    orders = _tail_for_session(
        root / "orders.jsonl", maximum_event_rows, selected_session_date
    )
    fills = _tail_for_session(
        root / "fills.jsonl", maximum_event_rows, selected_session_date
    )
    raw_marks = _tail_for_session(
        root / "marks.jsonl",
        maximum_mark_rows,
        selected_session_date,
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
    raw_benchmark_marks = _tail_for_session(
        root / "benchmark_marks.jsonl",
        maximum_mark_rows,
        selected_session_date,
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
    events = _tail_for_session(
        root / "events.jsonl",
        min(maximum_event_rows, 2_000),
        selected_session_date,
        recorded_at_fallback=True,
    )
    latency_rows = _tail_for_session(
        root / "latency.jsonl",
        min(maximum_event_rows, 2_000),
        selected_session_date,
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

    if not current_view:
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
            mode["simulation_replay"] = bool(
                (signal_event or {}).get("simulation_replay", False)
            )
            mode["replay_basis"] = (signal_event or {}).get("replay_basis")
            mode["counterfactual_open_replay"] = bool(
                (signal_event or {}).get("counterfactual_open_replay", False)
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
            else:
                mode["total_equity_twd"] = None
                mode["return_fraction"] = None
                mode["return_pct"] = None
                mode["open_position_count"] = 0
                mode["stale_position_count"] = 0
            if (signal_event or {}).get("event") == "signal_blocked":
                mode["engine_status"] = "historical_signal_blocked"
            elif (signal_event or {}).get("event") == "signal_registered":
                mode["engine_status"] = (
                    "historical_session_closed_with_residual"
                    if int(mode.get("open_position_count") or 0)
                    else "historical_session_complete"
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
    raw_opening_gate = _object(DEFAULT_OPENING_GATE_PATH) if operational_view else {}
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
    record_counts = {
        "signals": _line_count(root / "signals.jsonl"),
        "orders": _line_count(root / "orders.jsonl"),
        "fills": _line_count(root / "fills.jsonl"),
        "marks": _line_count(root / "marks.jsonl"),
        "benchmark_marks": _line_count(root / "benchmark_marks.jsonl"),
        "benchmark_history_marks": len(benchmark_history.get("marks") or ()),
        "events": _line_count(root / "events.jsonl"),
        "latency_samples": _line_count(root / "latency.jsonl"),
        "historical_positions": sum(
            len((_object(path).get("positions") or ()))
            for path in (root / "position_history").glob("*/*.json")
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
            "replay": "simulation_replay=true is recorded at 09:01 and retrospectively values the historical order at the observed official session open; it is explicitly counterfactual and is not a causally executable quote or real order fill",
            "entry_fill": "live execution starts at 09:00: after the immutable signal pointer is published, buy/cover consumes the first strictly later best Ask and sell/short consumes the first strictly later best Bid; missing causal quotes are blocked without last-price, 09:01, or adverse-tick substitution. Historical replay alone uses the 09:01 official open",
            "latency": "measured 09:00 trigger through model, atomic artifact publication, consumer discovery, first causally later best quote, and durable simulation-ledger persistence on this host; it is not an external order acknowledgement or venue round-trip measurement",
            "service_sync": "Discord, the paper engine, and the dashboard share one compact engine commit revision; Discord acknowledges that revision without reparsing the full ledger and the dashboard fetches heavy state only when the revision changes",
            "unattended_guardian": "the weekday guardian verifies the schedule clock, all 156 source events, exact-session eligibility, 08:30 acceptance, the three engine/Discord revisions, post-close flatness, public endpoints, and disk headroom; it re-arms existing systemd units but never invents data, signals, or fills",
            "mark": "best bid liquidates long; best ask covers short",
            "missing_mark": "carry only the same open position's last complete liquidation value and flag stale",
            "eligibility": "exact-session TWSE and TPEx official day-trade membership; missing venue/date blocks",
            "fees": "gross commission and sell tax are charged first; earned commission rebate is recorded separately in economic NAV",
            "pnl_split": "realized net PnL uses simulated executable exits plus any explicitly tagged 13:30 terminal ledger flatten, with allocated entry and exit costs; unrealized net liquidation PnL values remaining shares at executable bid or ask after remaining costs; total net PnL is their reconciled sum",
            "comparison": "all strategies and benchmarks are compared as cumulative net return divided by their own capital basis; TX uses one-contract official initial margin, while 0050/2330 use one-board-lot entry notional",
            "benchmarks": "0050/2330 are total-return benchmarks anchored to the retained actual session open: the completed-session official corporate-action archive is combined with the current session's official previous close and Shioaji/MIS reference price, so cash dividends and ETF distributions are reinvested and stock dividends or splits are adjusted exactly once without waiting until after close. Adjusted units are then marked at executable bid after tw_cash costs. TXFR1 has no cash distribution; it holds one real TX front-month contract across sessions. Before expiry it rolls only when the old bid and new ask coexist; after expiry it cash-settles the old month only at the official TAIFEX final settlement price and opens the new month at ask. The two bases stay separate, so the calendar spread is never booked as return; fees and statutory futures tax remain explicit",
            "benchmark_history": (
                "audited actual-open benchmark history is merged read-only with later live executable marks"
                if benchmark_history.get("origins")
                else benchmark_history.get("load_error")
                or "live benchmark marks only; no historical origin file"
            ),
            "depth_limit": "live entry quantity is bounded by independently verified eligibility, whole lots, price limits, displayed level-one depth, and after 09:01 completed-minute participation; historical 09:01 official-open replay is a separate counterfactual convention and never claims exchange depth, queue priority, or a guaranteed real-market fill",
            "bracket_fill": "each mode moves TP and the local SL trigger one legal dated TW tick inward; this improves fill probability but does not guarantee a fill without a trigger and executable counterparty volume",
            "exit_schedule": "from 13:20 through 13:23 each unfilled exit is checked for a real cross and otherwise cancel-repriced once per new minute to the current passive best ask for a sell or best bid for a buy-to-cover; at 13:24 it is replaced by a marketable exit attempt",
            "terminal_flatten": "after the 13:30 auction simulation, every residual is closed in a simulation-only terminal ledger pass so a day-trade mode never carries overnight; this is explicitly tagged and is not claimed as an exchange fill",
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
    observed = datetime.now(timezone.utc)
    available_session_dates = _available_session_dates(
        root=root,
        state=state,
        observed=observed,
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
    cache_key = (
        root.resolve(),
        signal_stat.st_dev,
        signal_stat.st_ino,
        signal_stat.st_size,
        signal_stat.st_mtime_ns,
        tuple(selected_session_dates),
        normalized_mode,
        normalized_symbol,
        normalized_status,
        int(offset),
        int(limit),
        state_signal_signature,
    )
    with _SIGNAL_PAGE_CACHE_LOCK:
        cached_page = _SIGNAL_PAGE_CACHE.get(cache_key)
        if cached_page is not None:
            return dict(cached_page)
    rows_by_session = _rows_for_sessions(
        signal_path,
        selected_session_dates,
        maximum_scan_rows,
    )
    current_rows = [
        row
        for selected_date in selected_session_dates
        for row in rows_by_session.get(selected_date, ())
    ]

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

    filtered = [row for row in current_rows if included(row)]

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

    filtered.sort(key=sort_key)
    capitals = {
        str(market): _finite_float(raw_mode.get("initial_capital_twd"))
        for market, raw_mode in (state.get("modes") or {}).items()
        if isinstance(raw_mode, Mapping)
    }
    current_signal_ids: dict[tuple[str, str], str] = {}
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
    summary_rows = [
        row
        for row in filtered
        if not current_signal_ids.get(
            (
                str(row.get("session_date") or "")[:10],
                str(row.get("market") or ""),
            )
        )
        or str(row.get("signal_id") or "")
        == current_signal_ids[
            (
                str(row.get("session_date") or "")[:10],
                str(row.get("market") or ""),
            )
        ]
    ]
    for row in summary_rows:
        target = _finite_float(row.get("target_weight")) or 0.0
        capital = capitals.get(str(row.get("market") or ""))
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
            "actual": executed_weight("filled_weight", "filled_shares"),
        }
        for stage, value in values.items():
            if value > 0.0:
                direction_summary[stage]["long_count"] += 1
                direction_summary[stage]["long_gross"] += value
            elif value < 0.0:
                direction_summary[stage]["short_count"] += 1
                direction_summary[stage]["short_gross"] += -value

    opening_execution_audit: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        target = _finite_float(row.get("target_weight")) or 0.0
        if target == 0.0:
            continue
        market = str(row.get("market") or "unknown")
        audit = opening_execution_audit.setdefault(
            market,
            {
                "nonzero_signal_count": 0,
                "opening_price_covered_count": 0,
                "opening_price_missing_count": 0,
                "execution_price_covered_count": 0,
                "requested_signal_count": 0,
                "filled_signal_count": 0,
                "unfilled_signal_count": 0,
                "missing_open_symbols": [],
                "unfilled_reason_counts": {},
            },
        )
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
            reason_counts = audit["unfilled_reason_counts"]
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

    for audit in opening_execution_audit.values():
        audit["unfilled_reason_counts"] = dict(
            sorted(
                audit["unfilled_reason_counts"].items(),
                key=lambda item: (-int(item[1]), str(item[0])),
            )
        )

    page = filtered[offset : offset + limit]
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
        "total": len(filtered),
        "has_more": offset + len(page) < len(filtered),
        "source_rows_scanned": len(current_rows),
        "record_count": _line_count(root / "signals.jsonl"),
        "direction_summary_scope": "current_signal_id_per_mode",
        "direction_summary": direction_summary,
        "opening_execution_audit_scope": "nonzero_target_rows_in_current_signal_id_per_mode",
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
    rows = [
        position
        for selected_date in selected_session_dates
        for position in _historical_positions(root, selected_date)
    ]
    for raw_mode in (state.get("modes") or {}).values():
        if not isinstance(raw_mode, Mapping):
            continue
        for position in (raw_mode.get("positions") or {}).values():
            if not isinstance(position, Mapping):
                continue
            safe = _safe_position(position)
            if str(safe.get("session_date") or "")[:10] in selected_date_set:
                rows.append(safe)
    deduplicated = {
        str(
            row.get("position_id")
            or f"{row.get('session_date')}:{row.get('market')}:{row.get('symbol')}"
        ): row
        for row in rows
    }
    normalized_mode = str(mode or "").strip()
    normalized_symbol = str(symbol or "").strip().casefold()
    normalized_status = str(status or "all").strip().casefold()

    def included(row: Mapping[str, Any]) -> bool:
        if normalized_mode and normalized_mode != "all":
            if str(row.get("market") or "") != normalized_mode:
                return False
        if normalized_symbol:
            haystack = f"{row.get('symbol') or ''} {row.get('name') or ''}".casefold()
            if normalized_symbol not in haystack:
                return False
        signed_shares = int(row.get("signed_shares") or 0)
        if normalized_status == "open":
            return signed_shares != 0
        if normalized_status == "closed":
            return signed_shares == 0
        if normalized_status == "blocked":
            return False
        return True

    filtered = [row for row in deduplicated.values() if included(row)]
    filtered.sort(
        key=lambda row: (
            -int(str(row.get("session_date") or "0000-00-00").replace("-", "")),
            str(row.get("market") or ""),
            -abs(_finite_float(row.get("target_weight")) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    page = filtered[offset : offset + limit]
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
        "total": len(filtered),
        "has_more": offset + len(page) < len(filtered),
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

    order_rows = _rows_for_sessions(
        root / "orders.jsonl",
        selected_session_dates,
        maximum_scan_rows,
    )
    fill_rows = _rows_for_sessions(
        root / "fills.jsonl",
        selected_session_dates,
        maximum_scan_rows,
    )
    orders = [
        safe_event(row, "order")
        for selected_date in selected_session_dates
        for row in order_rows.get(selected_date, ())
        if included(row)
    ]
    fills = [
        safe_event(row, "fill")
        for selected_date in selected_session_dates
        for row in fill_rows.get(selected_date, ())
        if included(row)
    ]
    rows = orders + fills
    rows.sort(
        key=lambda row: (
            str(row.get("fill_at") or row.get("recorded_at") or ""),
            str(row.get("event_kind") or ""),
            str(row.get("market") or ""),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )
    page = rows[offset : offset + limit]
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
        "total": len(rows),
        "order_total": len(orders),
        "fill_total": len(fills),
        "has_more": offset + len(page) < len(rows),
        "record_counts": {
            "orders": _line_count(root / "orders.jsonl"),
            "fills": _line_count(root / "fills.jsonl"),
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
        "session_progress",
        "modes",
        "record_counts",
    )
    return {key: snapshot.get(key) for key in keys}


__all__ = [
    "build_dashboard_event_page",
    "build_dashboard_history_snapshot",
    "build_dashboard_position_page",
    "build_dashboard_revision",
    "build_dashboard_signal_page",
    "build_dashboard_snapshot",
    "build_dashboard_summary",
]
