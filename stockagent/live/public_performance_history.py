"""Bounded append-only history for anonymous public-dashboard timing buckets.

The request path only serializes and enqueues one aggregate per completed
minute.  A daemon writer owns disk I/O, so telemetry cannot add fsync latency to
reader responses.  Files contain allowlisted route names and aggregate timing
histograms only; no IP, header, cookie, query string, or account data is stored.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
import json
import math
import os
from pathlib import Path
import queue
import re
import threading
from typing import Any, Collection, Mapping


_DAY_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.jsonl$")
_PROCESS_KEY = re.compile(r"^[a-f0-9]{16}$")
_PHASES = frozenset({"cache_wait", "build", "write", "other"})
_HISTOGRAM_BUCKETS = 18
_COUNT_FIELDS = (
    "requests",
    "response_body_bytes",
    "errors",
    "api_requests",
    "page_requests",
    "asset_requests",
)


def _normalized_latency(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    try:
        samples = max(0, int(value.get("samples") or 0))
        latency_sum_ms = max(0.0, float(value.get("latency_sum_ms") or 0.0))
        latency_max_ms = max(0.0, float(value.get("latency_max_ms") or 0.0))
        histogram = [
            max(0, int(item)) for item in value.get("latency_histogram", [])
        ]
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        len(histogram) != _HISTOGRAM_BUCKETS
        or samples != sum(histogram)
        or not math.isfinite(latency_sum_ms)
        or not math.isfinite(latency_max_ms)
        or (samples == 0 and (latency_sum_ms or latency_max_ms))
        or (samples > 0 and latency_sum_ms < latency_max_ms)
    ):
        return None
    return {
        "samples": samples,
        "latency_sum_ms": latency_sum_ms,
        "latency_max_ms": latency_max_ms,
        "latency_histogram": histogram,
    }


def _normalized_aggregate(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    try:
        counters = {
            field: max(0, int(value.get(field) or 0)) for field in _COUNT_FIELDS
        }
        latency_sum_ms = max(0.0, float(value.get("latency_sum_ms") or 0.0))
        latency_max_ms = max(0.0, float(value.get("latency_max_ms") or 0.0))
        histogram = [
            max(0, int(item)) for item in value.get("latency_histogram", [])
        ]
    except (TypeError, ValueError, OverflowError):
        return None
    requests = counters["requests"]
    if (
        len(histogram) != _HISTOGRAM_BUCKETS
        or requests != sum(histogram)
        or not math.isfinite(latency_sum_ms)
        or not math.isfinite(latency_max_ms)
        or counters["errors"] > requests
        or counters["api_requests"]
        + counters["page_requests"]
        + counters["asset_requests"]
        > requests
        or (requests == 0 and (latency_sum_ms or latency_max_ms))
        or (requests > 0 and latency_sum_ms < latency_max_ms)
    ):
        return None
    phases = value.get("phases") or {}
    if not isinstance(phases, Mapping):
        return None
    normalized_phases: dict[str, dict[str, Any]] = {}
    for phase, timing in phases.items():
        if str(phase) not in _PHASES:
            continue
        normalized = _normalized_latency(timing)
        if normalized is None:
            return None
        normalized_phases[str(phase)] = normalized
    return {
        **counters,
        "latency_sum_ms": latency_sum_ms,
        "latency_max_ms": latency_max_ms,
        "latency_histogram": histogram,
        "phases": normalized_phases,
    }


def _merge_latency(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    target["samples"] += int(source["samples"])
    target["latency_sum_ms"] += float(source["latency_sum_ms"])
    target["latency_max_ms"] = max(
        float(target["latency_max_ms"]), float(source["latency_max_ms"])
    )
    for index, count in enumerate(source["latency_histogram"]):
        target["latency_histogram"][index] += int(count)


def _merge_aggregate(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for field in _COUNT_FIELDS:
        target[field] += int(source[field])
    target["latency_sum_ms"] += float(source["latency_sum_ms"])
    target["latency_max_ms"] = max(
        float(target["latency_max_ms"]), float(source["latency_max_ms"])
    )
    for index, count in enumerate(source["latency_histogram"]):
        target["latency_histogram"][index] += int(count)
    for phase, timing in source["phases"].items():
        if phase not in target["phases"]:
            target["phases"][phase] = deepcopy(timing)
        else:
            _merge_latency(target["phases"][phase], timing)


class PublicPerformanceHistoryStore:
    """Persist validated minute aggregates without blocking HTTP requests."""

    schema_version = 1

    def __init__(
        self,
        root: Path,
        *,
        retention_days: int = 90,
        maximum_rows: int = 200_000,
        queue_size: int = 2_048,
        allowed_routes: Collection[str] = (),
        allowed_cache_outcomes: Collection[str] = (),
    ) -> None:
        self.root = Path(root)
        self.retention_days = min(366, max(1, int(retention_days)))
        self.maximum_rows = min(50_000, max(2_880, int(maximum_rows)))
        self._allowed_routes = frozenset(str(value) for value in allowed_routes)
        self._allowed_cache_outcomes = frozenset(
            str(value) for value in allowed_cache_outcomes
        )
        self._lock = threading.Lock()
        self._recent_seconds = 2 * 86_400
        self._rows: deque[dict[str, Any]] = deque(maxlen=self.maximum_rows)
        self._hour_rows: dict[int, dict[str, Any]] = {}
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(
            maxsize=max(16, int(queue_size))
        )
        self._enabled = False
        self._loaded_rows = 0
        self._invalid_rows = 0
        self._dropped_rows = 0
        self._write_errors = 0
        self._last_error: str | None = None
        self._writer = threading.Thread(
            target=self._writer_loop,
            name="public-performance-history-writer",
            daemon=True,
        )
        try:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.root, 0o700)
            self._load_existing()
            self._prune_expired_files()
            self._enabled = True
        except OSError as error:
            self._last_error = type(error).__name__
        self._writer.start()

    def _validated_row(self, value: object) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        try:
            schema_version = int(value.get("schema_version") or 0)
            minute_epoch = int(value.get("minute_epoch") or 0)
            process_key = str(value.get("process_key") or "")
        except (TypeError, ValueError, OverflowError):
            return None
        if (
            schema_version != PublicPerformanceHistoryStore.schema_version
            or minute_epoch < 946_684_800
            or minute_epoch % 60
            or not _PROCESS_KEY.fullmatch(process_key)
            or not isinstance(value.get("aggregate"), Mapping)
            or not isinstance(value.get("routes"), Mapping)
            or not isinstance(value.get("cache"), Mapping)
        ):
            return None
        aggregate = _normalized_aggregate(value["aggregate"])
        if aggregate is None:
            return None
        routes: dict[str, dict[str, Any]] = {}
        for key, row in value["routes"].items():
            route = str(key)
            if route not in self._allowed_routes:
                continue
            normalized = _normalized_aggregate(row)
            if normalized is None:
                return None
            routes[route] = normalized
        try:
            cache = {
                str(key): max(0, int(count))
                for key, count in value["cache"].items()
                if isinstance(count, int)
                and count >= 0
                and str(key) in self._allowed_cache_outcomes
            }
        except (TypeError, ValueError, OverflowError):
            return None
        return {
            "schema_version": schema_version,
            "minute_epoch": minute_epoch,
            "minute_utc": datetime.fromtimestamp(minute_epoch, UTC).isoformat(),
            "process_key": process_key,
            "aggregate": aggregate,
            "routes": routes,
            "cache": cache,
        }

    def _index_row(self, row: dict[str, Any], *, now_epoch: int) -> None:
        minute = int(row["minute_epoch"])
        if minute >= now_epoch - self._recent_seconds:
            self._rows.append(row)
        hour = minute - minute % 3_600
        rollup = self._hour_rows.get(hour)
        if rollup is None:
            rollup = {
                "schema_version": self.schema_version,
                "minute_epoch": hour,
                "minute_utc": datetime.fromtimestamp(hour, UTC).isoformat(),
                "process_key": row["process_key"],
                "process_keys": {row["process_key"]},
                "observed_minute_mask": 1 << ((minute - hour) // 60),
                "rollup_seconds": 3_600,
                "aggregate": deepcopy(row["aggregate"]),
                "routes": deepcopy(row["routes"]),
                "cache": dict(row["cache"]),
            }
            self._hour_rows[hour] = rollup
            return
        rollup["process_keys"].add(row["process_key"])
        rollup["observed_minute_mask"] |= 1 << ((minute - hour) // 60)
        _merge_aggregate(rollup["aggregate"], row["aggregate"])
        for route, aggregate in row["routes"].items():
            if route not in rollup["routes"]:
                rollup["routes"][route] = deepcopy(aggregate)
            else:
                _merge_aggregate(rollup["routes"][route], aggregate)
        for key, count in row["cache"].items():
            rollup["cache"][key] = rollup["cache"].get(key, 0) + int(count)

    def _prune_resident(self, *, now_epoch: int) -> None:
        recent_cutoff = now_epoch - self._recent_seconds
        while self._rows and int(self._rows[0]["minute_epoch"]) < recent_cutoff:
            self._rows.popleft()
        retention_cutoff = now_epoch - self.retention_days * 86_400
        for hour in tuple(self._hour_rows):
            if hour + 3_600 < retention_cutoff:
                self._hour_rows.pop(hour, None)

    def _load_existing(self) -> None:
        cutoff = datetime.now(UTC).date() - timedelta(days=self.retention_days - 1)
        now_epoch = int(datetime.now(UTC).timestamp())
        for path in sorted(self.root.glob("*.jsonl")):
            match = _DAY_FILE.fullmatch(path.name)
            if match is None:
                continue
            try:
                if date.fromisoformat(match.group(1)) < cutoff:
                    continue
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if len(line) > 1_000_000:
                            self._invalid_rows += 1
                            continue
                        try:
                            decoded = json.loads(line)
                        except (ValueError, UnicodeError):
                            # A crash can leave one partial final append.  Keep
                            # all earlier complete rows and expose the gap.
                            self._invalid_rows += 1
                            continue
                        row = self._validated_row(decoded)
                        if row is None:
                            self._invalid_rows += 1
                            continue
                        self._index_row(row, now_epoch=now_epoch)
                        self._loaded_rows += 1
            except OSError as error:
                self._last_error = type(error).__name__
        self._prune_resident(now_epoch=now_epoch)

    def _prune_expired_files(self) -> None:
        cutoff = datetime.now(UTC).date() - timedelta(days=self.retention_days - 1)
        for path in self.root.glob("*.jsonl"):
            match = _DAY_FILE.fullmatch(path.name)
            if match is None:
                continue
            try:
                expired = date.fromisoformat(match.group(1)) < cutoff
            except ValueError:
                continue
            if expired:
                path.unlink(missing_ok=True)

    def enqueue(self, row: Mapping[str, Any]) -> bool:
        validated = self._validated_row(row)
        if validated is None:
            with self._lock:
                self._invalid_rows += 1
            return False
        with self._lock:
            self._index_row(validated, now_epoch=int(datetime.now(UTC).timestamp()))
            self._prune_resident(now_epoch=int(datetime.now(UTC).timestamp()))
        if not self._enabled:
            with self._lock:
                self._dropped_rows += 1
            return False
        try:
            # JSON encoding and all filesystem work stay on the writer thread.
            # The request-facing minute rollover only copies a bounded aggregate
            # and performs a non-blocking queue operation.
            self._queue.put_nowait(validated)
        except queue.Full:
            with self._lock:
                self._dropped_rows += 1
            return False
        return True

    def _writer_loop(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                try:
                    target = self.root / f"{item['minute_utc'][:10]}.jsonl"
                    serialized = json.dumps(
                        item,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        allow_nan=False,
                    ) + "\n"
                    with target.open("a", encoding="utf-8") as handle:
                        os.chmod(target, 0o600)
                        handle.write(serialized)
                        handle.flush()
                        os.fsync(handle.fileno())
                except (OSError, TypeError, ValueError) as error:
                    with self._lock:
                        self._write_errors += 1
                        self._last_error = type(error).__name__
            finally:
                self._queue.task_done()

    def _raw_rows_between(self, start_epoch: int, end_epoch: int) -> list[dict[str, Any]]:
        if end_epoch <= start_epoch:
            return []
        dates: set[date] = set()
        cursor = datetime.fromtimestamp(start_epoch, UTC).date()
        last = datetime.fromtimestamp(end_epoch - 1, UTC).date()
        while cursor <= last:
            dates.add(cursor)
            cursor += timedelta(days=1)
        result: list[dict[str, Any]] = []
        for day in sorted(dates):
            path = self.root / f"{day.isoformat()}.jsonl"
            try:
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if len(line) > 1_000_000:
                            continue
                        try:
                            row = self._validated_row(json.loads(line))
                        except (ValueError, UnicodeError):
                            continue
                        if row is not None and start_epoch <= row["minute_epoch"] < end_epoch:
                            result.append(row)
            except FileNotFoundError:
                continue
            except OSError:
                continue
        return result

    @staticmethod
    def _export_rollup(row: Mapping[str, Any]) -> dict[str, Any]:
        exported = deepcopy(dict(row))
        exported["process_keys"] = sorted(row.get("process_keys") or ())
        return exported

    def rows_since(self, since_epoch: int) -> list[dict[str, Any]]:
        since_epoch = int(since_epoch)
        with self._lock:
            if since_epoch >= int(datetime.now(UTC).timestamp()) - self._recent_seconds:
                return [
                    deepcopy(row)
                    for row in self._rows
                    if int(row["minute_epoch"]) >= since_epoch
                ]
            first_full_hour = ((since_epoch + 3_599) // 3_600) * 3_600
            rollups = [
                self._export_rollup(row)
                for hour, row in sorted(self._hour_rows.items())
                if hour >= first_full_hour
            ]
        boundary = self._raw_rows_between(since_epoch, first_full_hour)
        return sorted(boundary + rollups, key=lambda row: int(row["minute_epoch"]))

    def rows_since_readonly(self, since_epoch: int) -> list[dict[str, Any]]:
        """Internal read-only view of recent immutable rows; callers must not mutate.

        Published rows are never changed after indexing. Returning references
        avoids deep-copying every nested route histogram for each history
        request. Older ranges keep the normal copied/disk-backed contract.
        """
        since_epoch = int(since_epoch)
        with self._lock:
            if since_epoch >= int(datetime.now(UTC).timestamp()) - self._recent_seconds:
                return [
                    row for row in self._rows
                    if int(row["minute_epoch"]) >= since_epoch
                ]
        return self.rows_since(since_epoch)

    def status(self) -> dict[str, Any]:
        with self._lock:
            hour_first = None
            hour_latest = None
            if self._hour_rows:
                first_hour = min(self._hour_rows)
                first_mask = int(
                    self._hour_rows[first_hour].get("observed_minute_mask") or 0
                )
                last_hour = max(self._hour_rows)
                last_mask = int(
                    self._hour_rows[last_hour].get("observed_minute_mask") or 0
                )
                if first_mask:
                    first_offset = (first_mask & -first_mask).bit_length() - 1
                    hour_first = first_hour + first_offset * 60
                if last_mask:
                    last_offset = last_mask.bit_length() - 1
                    hour_latest = last_hour + last_offset * 60
            first_candidates: list[int] = []
            latest_candidates: list[int] = []
            if self._rows:
                first_candidates.append(int(self._rows[0]["minute_epoch"]))
                latest_candidates.append(int(self._rows[-1]["minute_epoch"]))
            if hour_first is not None:
                first_candidates.append(hour_first)
            if hour_latest is not None:
                latest_candidates.append(hour_latest)
            first = (
                datetime.fromtimestamp(min(first_candidates), UTC).isoformat()
                if first_candidates
                else None
            )
            latest = (
                datetime.fromtimestamp(max(latest_candidates), UTC).isoformat()
                if latest_candidates
                else None
            )
            health = (
                "disabled"
                if not self._enabled
                else "degraded"
                if self._dropped_rows or self._write_errors or self._last_error
                else "ready"
            )
            return {
                "health": health,
                "enabled": self._enabled,
                "retention_days": self.retention_days,
                "resident_rows": len(self._rows),
                "resident_hour_rollups": len(self._hour_rows),
                "resident_resolution": "minute_2d_plus_hour_90d",
                "maximum_resident_rows": self.maximum_rows,
                "loaded_rows": self._loaded_rows,
                "invalid_rows": self._invalid_rows,
                "dropped_rows": self._dropped_rows,
                "write_errors": self._write_errors,
                "last_error_type": self._last_error,
                "first_minute_utc": first,
                "latest_minute_utc": latest,
                "queued_writes": self._queue.qsize(),
            }

    def close(self) -> None:
        try:
            self._queue.put(None, timeout=1.0)
        except queue.Full:
            with self._lock:
                self._dropped_rows += 1
            return
        self._writer.join(timeout=5.0)
