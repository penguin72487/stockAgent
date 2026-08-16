"""Process-safe, public-safe accounting for Shioaji market-data queries.

The ledger is deliberately local: reading it never logs in to Shioaji.  Usage
observations are best effort and must never turn a successful quote query into
a failed trading/simulation observation.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Iterator, TypeVar
import uuid
from zoneinfo import ZoneInfo


T = TypeVar("T")
LEDGER_SCHEMA_VERSION = 2
TAIPEI = ZoneInfo("Asia/Taipei")


def quota_window_date(observed: datetime) -> str:
    """Return the local observation date, not an inferred broker quota reset.

    The name is retained for compatibility with existing callers.  A quota
    epoch can only be established by ordered ``api.usage()`` observations; a
    wall-clock boundary must never manufacture a broker reset.
    """

    selected = observed if observed.tzinfo is not None else observed.replace(tzinfo=UTC)
    return selected.astimezone(TAIPEI).date().isoformat()


def traffic_ledger_root() -> Path:
    configured = os.getenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "artifacts/live/shioaji_traffic"


def _usage(api: Any) -> dict[str, Any] | None:
    try:
        value = api.usage()
        used = int(getattr(value, "bytes"))
        limit = int(getattr(value, "limit_bytes"))
    except Exception:
        return None
    if used < 0 or limit <= 0:
        return None
    return {
        "used_bytes": used,
        "limit_bytes": limit,
        "observed_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def _safe_details(details: dict[str, Any] | None) -> dict[str, Any]:
    allowed = {
        "contract",
        "contracts",
        "contract_count",
        "symbol_count",
        "start",
        "end",
        "date",
        "logical_code",
        "target_code",
        "cache_scope",
        "reason",
        "worker_index",
        "session",
        "trade_date",
    }
    output: dict[str, Any] = {}
    for key, value in (details or {}).items():
        if key not in allowed or value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            output[key] = value
        elif isinstance(value, (list, tuple)):
            output[key] = [str(item) for item in value[:20]]
    return output


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _increment(bucket: dict[str, Any], event: dict[str, Any]) -> None:
    bucket["events"] = int(bucket.get("events") or 0) + 1
    bucket["queries"] = int(bucket.get("queries") or 0) + int(
        event.get("request_count") or 0
    )
    bucket["avoided_queries"] = int(bucket.get("avoided_queries") or 0) + int(
        event.get("avoided_request_count") or 0
    )
    bucket["rows"] = int(bucket.get("rows") or 0) + int(event.get("rows") or 0)
    bucket["failures"] = int(bucket.get("failures") or 0) + int(
        event.get("status") == "failed"
    )
    delta = event.get("usage_delta_bytes")
    if isinstance(delta, int):
        bucket["observed_usage_delta_bytes"] = int(
            bucket.get("observed_usage_delta_bytes") or 0
        ) + max(0, delta)
    for key in (
        "stream_tick_events",
        "stream_book_events",
        "stream_snapshot_rows",
        "stream_dropped_events",
        "stream_stored_bytes",
    ):
        bucket[key] = int(bucket.get(key) or 0) + max(0, int(event.get(key) or 0))
    bucket["stream_observations"] = int(bucket.get("stream_observations") or 0) + int(
        event.get("operation") == "stream"
    )


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _usage_observations(event: dict[str, Any]) -> list[dict[str, Any]]:
    fallback_at = str(event.get("observed_at_utc") or "")
    observations: list[dict[str, Any]] = []
    for phase_rank, phase in enumerate(("usage_before", "usage_after")):
        raw = event.get(phase)
        if not isinstance(raw, dict):
            continue
        try:
            used = int(raw.get("used_bytes"))
            limit = int(raw.get("limit_bytes"))
        except (TypeError, ValueError):
            continue
        if used < 0 or limit <= 0:
            continue
        observed_at = str(raw.get("observed_at_utc") or fallback_at)
        if _parse_utc(observed_at) is None:
            continue
        observations.append(
            {
                "used_bytes": used,
                "limit_bytes": limit,
                "observed_at_utc": observed_at,
                "event_id": str(event.get("event_id") or ""),
                "phase": phase,
                "phase_rank": phase_rank,
            }
        )
    return sorted(
        observations,
        key=lambda item: (
            _parse_utc(item["observed_at_utc"]) or datetime.min.replace(tzinfo=UTC),
            int(item["phase_rank"]),
        ),
    )


def _blank_summary(
    *,
    observed_at_utc: str,
    boundary_kind: str,
    boundary: dict[str, Any] | None = None,
    previous_reset: dict[str, Any] | None = None,
) -> dict[str, Any]:
    observed = _parse_utc(observed_at_utc) or datetime.now(UTC)
    observed_text = observed.isoformat().replace("+00:00", "Z")
    local_date = quota_window_date(observed)
    reset = boundary if boundary_kind == "observed_counter_drop" else previous_reset
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "ledger_date": local_date,
        "observation_date": local_date,
        "quota_epoch": {
            "id": f"{boundary_kind}:{observed_text}",
            "started_at_utc": observed_text,
            "boundary_kind": boundary_kind,
            "reset_observed": boundary_kind == "observed_counter_drop",
        },
        "latest_boundary": boundary,
        "latest_reset": reset,
        "totals": {},
        "by_consumer": {},
        "by_method": {},
        "by_asset_class": {},
    }


def _upgrade_summary(summary: dict[str, Any], *, observed_at_utc: str) -> dict[str, Any]:
    if int(summary.get("schema_version") or 0) >= LEDGER_SCHEMA_VERSION:
        return summary
    observed = _parse_utc(observed_at_utc) or datetime.now(UTC)
    local_date = quota_window_date(observed)
    summary["schema_version"] = LEDGER_SCHEMA_VERSION
    summary["observation_date"] = local_date
    summary.setdefault("ledger_date", local_date)
    summary.setdefault(
        "quota_epoch",
        {
            "id": f"legacy_import:{summary.get('ledger_date') or local_date}",
            "started_at_utc": None,
            "boundary_kind": "legacy_import_without_reset_evidence",
            "reset_observed": False,
        },
    )
    summary.setdefault("latest_boundary", None)
    summary.setdefault("latest_reset", None)
    return summary


def _observation_is_newer(
    observation: dict[str, Any],
    latest: dict[str, Any] | None,
) -> bool:
    if not isinstance(latest, dict):
        return True
    current_at = _parse_utc(observation.get("observed_at_utc"))
    latest_at = _parse_utc(latest.get("observed_at_utc"))
    if current_at is None:
        return False
    if latest_at is None or current_at > latest_at:
        return True
    if current_at < latest_at:
        return False
    return bool(
        observation.get("event_id")
        and observation.get("event_id") == latest.get("event_id")
        and int(observation.get("phase_rank") or 0)
        > int(latest.get("phase_rank") or 0)
    )


def _apply_event_to_summary(
    summary: dict[str, Any],
    event: dict[str, Any],
    *,
    recent_ledger: str,
) -> dict[str, Any]:
    event_at = str(event.get("observed_at_utc") or "")
    if _parse_utc(event_at) is None:
        return summary
    if not summary:
        summary = _blank_summary(
            observed_at_utc=event_at,
            boundary_kind="initial_local_event",
        )
    else:
        summary = _upgrade_summary(summary, observed_at_utc=event_at)

    for observation in _usage_observations(event):
        latest = summary.get("latest_usage")
        if not _observation_is_newer(observation, latest):
            continue
        boundary_kind: str | None = None
        if isinstance(latest, dict):
            prior_used = int(latest.get("used_bytes") or 0)
            prior_limit = int(latest.get("limit_bytes") or 0)
            if int(observation["limit_bytes"]) != prior_limit:
                boundary_kind = "observed_limit_change"
            elif int(observation["used_bytes"]) < prior_used:
                boundary_kind = "observed_counter_drop"
        if boundary_kind is not None:
            boundary = {
                "kind": boundary_kind,
                "observed_at_utc": observation["observed_at_utc"],
                "previous_used_bytes": int(latest.get("used_bytes") or 0),
                "new_used_bytes": int(observation["used_bytes"]),
                "previous_limit_bytes": int(latest.get("limit_bytes") or 0),
                "new_limit_bytes": int(observation["limit_bytes"]),
                "consumer": event.get("consumer"),
                "method": event.get("method"),
                "event_id": event.get("event_id"),
            }
            summary = _blank_summary(
                observed_at_utc=observation["observed_at_utc"],
                boundary_kind=boundary_kind,
                boundary=boundary,
                previous_reset=summary.get("latest_reset"),
            )
        summary["latest_usage"] = {
            "used_bytes": int(observation["used_bytes"]),
            "limit_bytes": int(observation["limit_bytes"]),
            "observed_at_utc": observation["observed_at_utc"],
            "event_id": event.get("event_id"),
            "phase": observation["phase"],
            "phase_rank": int(observation["phase_rank"]),
            "consumer": event.get("consumer"),
            "method": event.get("method"),
        }

    _increment(summary.setdefault("totals", {}), event)
    for field in ("consumer", "method", "asset_class"):
        key = str(event.get(field) or "unknown")
        _increment(summary.setdefault(f"by_{field}", {}).setdefault(key, {}), event)
    observed = _parse_utc(event_at) or datetime.now(UTC)
    summary["ledger_date"] = quota_window_date(observed)
    summary["observation_date"] = quota_window_date(observed)
    summary["updated_at_utc"] = event_at
    summary["recent_ledger"] = recent_ledger
    return summary


def record_traffic_event(event: dict[str, Any], *, root: Path | None = None) -> None:
    if root is None and "PYTEST_CURRENT_TEST" in os.environ and not os.getenv(
        "STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT"
    ):
        return
    selected = Path(root) if root is not None else traffic_ledger_root()
    selected.mkdir(parents=True, exist_ok=True)
    observed = datetime.now(UTC)
    observation_date = quota_window_date(observed)
    payload = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "event_id": uuid.uuid4().hex,
        "observed_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "pid": os.getpid(),
        **event,
    }
    day_path = selected / "daily" / f"{observation_date}.jsonl"
    summary_path = selected / "summary.json"
    lock_path = selected / "ledger.lock"
    day_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with day_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            summary = {}
        summary = _apply_event_to_summary(
            summary if isinstance(summary, dict) else {},
            payload,
            recent_ledger=str(day_path.relative_to(selected)),
        )
        _atomic_json(summary_path, summary)


def rebuild_traffic_summary(*, root: Path | None = None) -> dict[str, Any]:
    """Rebuild the current observed quota epoch from append-only raw events."""

    selected = Path(root) if root is not None else traffic_ledger_root()
    selected.mkdir(parents=True, exist_ok=True)
    summary_path = selected / "summary.json"
    lock_path = selected / "ledger.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        events: list[tuple[datetime, str, str, dict[str, Any]]] = []
        for path in sorted((selected / "daily").glob("*.jsonl")):
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(event, dict):
                    continue
                observed = _parse_utc(event.get("observed_at_utc"))
                if observed is None:
                    continue
                events.append(
                    (
                        observed,
                        str(event.get("event_id") or ""),
                        str(path.relative_to(selected)),
                        event,
                    )
                )
        events.sort(key=lambda item: (item[0], item[1]))
        summary: dict[str, Any] = {}
        for _observed, _event_id, relative_path, event in events:
            summary = _apply_event_to_summary(
                summary,
                event,
                recent_ledger=relative_path,
            )
        if not summary:
            return {}
        _atomic_json(summary_path, summary)
    return summary


def _best_effort_record(event: dict[str, Any]) -> None:
    try:
        record_traffic_event(event)
    except Exception:
        # Accounting must never alter quote/data availability.
        return


@contextmanager
def shioaji_query(
    api: Any,
    *,
    consumer: str,
    method: str,
    asset_class: str,
    details: dict[str, Any] | None = None,
) -> Iterator[Callable[[Any], None]]:
    """Record one billed request without changing its error behavior."""

    before = _usage(api)
    started = time.monotonic()
    rows = 0

    def set_result(value: Any) -> None:
        nonlocal rows
        try:
            rows = len(value)
        except (TypeError, AttributeError):
            for name in ("ts", "close"):
                try:
                    rows = len(getattr(value, name))
                    break
                except (TypeError, AttributeError):
                    continue

    try:
        yield set_result
    except BaseException as exc:
        _best_effort_record(
            {
                "consumer": consumer,
                "method": method,
                "asset_class": asset_class,
                "operation": "query",
                "status": "failed",
                "error_type": type(exc).__name__,
                "request_count": 1,
                "avoided_request_count": 0,
                "rows": rows,
                "duration_ms": round((time.monotonic() - started) * 1000.0, 3),
                "usage_before": before,
                "usage_after": _usage(api),
                "details": _safe_details(details),
            }
        )
        raise
    else:
        after = _usage(api)
        delta = None
        if before is not None and after is not None:
            candidate = after["used_bytes"] - before["used_bytes"]
            delta = candidate if candidate >= 0 else None
        _best_effort_record(
            {
                "consumer": consumer,
                "method": method,
                "asset_class": asset_class,
                "operation": "query",
                "status": "success",
                "request_count": 1,
                "avoided_request_count": 0,
                "rows": rows,
                "duration_ms": round((time.monotonic() - started) * 1000.0, 3),
                "usage_before": before,
                "usage_after": after,
                "usage_delta_bytes": delta,
                "details": _safe_details(details),
            }
        )


def record_avoided_query(
    *,
    consumer: str,
    method: str,
    asset_class: str,
    reason: str,
    count: int = 1,
    rows: int = 0,
    details: dict[str, Any] | None = None,
) -> None:
    _best_effort_record(
        {
            "consumer": consumer,
            "method": method,
            "asset_class": asset_class,
            "operation": "avoided",
            "status": "success",
            "request_count": 0,
            "avoided_request_count": max(0, int(count)),
            "rows": max(0, int(rows)),
            "duration_ms": 0.0,
            "usage_before": None,
            "usage_after": None,
            "usage_delta_bytes": None,
            "details": _safe_details({**(details or {}), "reason": reason}),
        }
    )


def record_streaming_observation(
    *,
    consumer: str,
    asset_class: str,
    tick_events: int = 0,
    book_events: int = 0,
    snapshot_rows: int = 0,
    dropped_events: int = 0,
    stored_bytes: int = 0,
    details: dict[str, Any] | None = None,
) -> None:
    """Record quota-exempt push data without consulting the traffic guard."""

    _best_effort_record(
        {
            "consumer": consumer,
            "method": "subscribe",
            "asset_class": asset_class,
            "operation": "stream",
            "status": "success",
            "quota_exempt": True,
            "request_count": 0,
            "avoided_request_count": 0,
            "rows": max(0, int(tick_events)) + max(0, int(book_events)),
            "stream_tick_events": max(0, int(tick_events)),
            "stream_book_events": max(0, int(book_events)),
            "stream_snapshot_rows": max(0, int(snapshot_rows)),
            "stream_dropped_events": max(0, int(dropped_events)),
            "stream_stored_bytes": max(0, int(stored_bytes)),
            "duration_ms": 0.0,
            "usage_before": None,
            "usage_after": None,
            "usage_delta_bytes": 0,
            "details": _safe_details(details),
        }
    )


class StreamingLedgerRecorder:
    """Convert cumulative capture counters into non-overlapping ledger deltas."""

    def __init__(
        self,
        *,
        consumer: str,
        asset_class: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.consumer = consumer
        self.asset_class = asset_class
        self.details = details
        self.previous = (0, 0, 0, 0, 0)

    def observe(
        self,
        *,
        tick_events: int,
        book_events: int,
        snapshot_rows: int,
        dropped_events: int,
        stored_bytes: int,
    ) -> None:
        current = tuple(
            max(0, int(value))
            for value in (
                tick_events,
                book_events,
                snapshot_rows,
                dropped_events,
                stored_bytes,
            )
        )
        delta = tuple(max(0, value - old) for value, old in zip(current, self.previous))
        self.previous = current
        if not any(delta):
            return
        record_streaming_observation(
            consumer=self.consumer,
            asset_class=self.asset_class,
            tick_events=delta[0],
            book_events=delta[1],
            snapshot_rows=delta[2],
            dropped_events=delta[3],
            stored_bytes=delta[4],
            details=self.details,
        )


__all__ = [
    "LEDGER_SCHEMA_VERSION",
    "rebuild_traffic_summary",
    "record_avoided_query",
    "record_streaming_observation",
    "StreamingLedgerRecorder",
    "record_traffic_event",
    "quota_window_date",
    "shioaji_query",
    "traffic_ledger_root",
]
