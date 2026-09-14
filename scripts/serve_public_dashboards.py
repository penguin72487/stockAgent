#!/usr/bin/env python3
"""Serve sanitized, cached public views of both localhost dashboards."""

from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import gzip
import hashlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
import time
from typing import Any, Callable, Final, Mapping
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import ProxyHandler, build_opener
from weakref import WeakValueDictionary
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.public_dashboards import (  # noqa: E402
    UnsafePublicDashboardPayload,
    sanitize_taifex_history,
    sanitize_taifex_status,
    sanitize_tw_events,
    sanitize_tw_history,
    sanitize_tw_positions,
    sanitize_tw_signals,
    sanitize_tw_status,
)
from stockagent.live.dashboard_updates import (
    DashboardUpdateHub,
    file_signature,
    metadata_signature,
)  # noqa: E402
from stockagent.live.benchmark_history_projection import (  # noqa: E402
    projection_head_path as benchmark_projection_head_path,
)
from stockagent.live.shioaji_api_dashboard import (  # noqa: E402
    build_shioaji_public_status,
)
from stockagent.live.openbb_archive_dashboard import (  # noqa: E402
    build_openbb_public_history,
    build_openbb_public_status,
)
from stockagent.live.public_performance_history import (  # noqa: E402
    PublicPerformanceHistoryStore,
)
from stockagent.live.data_monitor_dashboard import (  # noqa: E402
    build_data_monitor_public_status,
    build_tw_public_monitor_status,
)
from stockagent.live.tw_day_trade_dashboard import (  # noqa: E402
    DEFAULT_MAX_SOURCE_AGE_SECONDS,
    DEFAULT_OPENING_GATE_PATH,
    build_dashboard_event_page,
    build_dashboard_history_snapshot,
    build_dashboard_position_page,
    build_dashboard_revision,
    build_dashboard_signal_page,
    build_dashboard_snapshot,
    dashboard_session_clock,
    warm_dashboard_session_indexes,
)


MAX_UPSTREAM_BYTES: Final[int] = 8 * 1024 * 1024
MAX_REQUEST_TARGET_BYTES: Final[int] = 2_048
PUBLIC_SIGNAL_LIMIT: Final[int] = 250
PUBLIC_EVENT_LIMIT: Final[int] = 250
MAX_CACHE_ENTRIES: Final[int] = 512
MAX_CACHE_BYTES: Final[int] = 128 * 1024 * 1024
MONITOR_STATUS_STALE_GRACE_SECONDS: Final[float] = 30.0
OVERVIEW_STALE_GRACE_SECONDS: Final[float] = 5 * 60.0
HISTORY_STALE_GRACE_SECONDS: Final[float] = 15 * 60.0
IMMUTABLE_ASSET_CACHE_CONTROL: Final[str] = "public, max-age=31536000, immutable"
_OPENER = build_opener(ProxyHandler({}))
_LATENCY_BUCKETS_MS: Final[tuple[float, ...]] = (
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
    64.0,
    125.0,
    250.0,
    500.0,
    1_000.0,
    2_000.0,
    5_000.0,
    10_000.0,
    30_000.0,
)
_SERVER_PHASES: Final[tuple[str, ...]] = (
    "cache_wait",
    "build",
    "write",
    "other",
)
_CACHE_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "background_build",
        "build",
        "coalesced_hit",
        "fresh_hit",
        "revision_stale_hit",
        "stale_hit",
        "static_build",
        "static_hit",
    }
)
_TRAFFIC_HISTORY_RANGES: Final[dict[str, tuple[int, int]]] = {
    "1h": (3_600, 60),
    "24h": (86_400, 900),
    "7d": (7 * 86_400, 3_600),
    "30d": (30 * 86_400, 6 * 3_600),
    "90d": (90 * 86_400, 86_400),
}
_PUBLIC_API_ROUTES: Final[frozenset[str]] = frozenset(
    {
        "/api/overview",
        "/taifex/api/status",
        "/taifex/api/history",
        "/tw-day-trade/api/status",
        "/tw-day-trade/api/history",
        "/tw-day-trade/api/positions",
        "/tw-day-trade/api/public-data-status",
        "/tw-day-trade/api/revision",
        "/tw-day-trade/api/updates",
        "/tw-day-trade/api/summary",
        "/tw-day-trade/api/signals",
        "/tw-day-trade/api/events",
        "/tw-overnight/api/status",
        "/tw-overnight/api/history",
        "/tw-overnight/api/positions",
        "/tw-overnight/api/public-data-status",
        "/tw-overnight/api/revision",
        "/tw-overnight/api/updates",
        "/tw-overnight/api/summary",
        "/tw-overnight/api/signals",
        "/tw-overnight/api/events",
        "/shioaji/api/status",
        "/openbb/api/status",
        "/openbb/api/history",
        "/data-monitor/api/status",
        "/data-monitor/api/summary",
        "/data-monitor/api/details",
        "/traffic/api/status",
        "/traffic/api/history",
    }
)
_PUBLIC_PAGE_ROUTES: Final[frozenset[str]] = frozenset(
    {
        "/",
        "/taifex/",
        "/tw-day-trade/",
        "/tw-overnight/",
        "/shioaji/",
        "/openbb/",
        "/data-monitor/",
        "/traffic/",
    }
)
_QUERY_API_ROUTES: Final[frozenset[str]] = frozenset(
    {
        "/taifex/api/history",
        "/tw-day-trade/api/status",
        "/tw-day-trade/api/history",
        "/tw-day-trade/api/summary",
        "/tw-day-trade/api/positions",
        "/tw-day-trade/api/signals",
        "/tw-day-trade/api/events",
        "/openbb/api/history",
        "/tw-overnight/api/status",
        "/tw-overnight/api/history",
        "/tw-overnight/api/summary",
        "/tw-overnight/api/positions",
        "/tw-overnight/api/signals",
        "/tw-overnight/api/events",
        "/traffic/api/history",
    }
)


class InvalidPublicRequest(ValueError):
    """A caller-controlled query error that is safe to report as HTTP 400."""


class PublicRouteNotFound(KeyError):
    """An unknown public route, distinct from an internal mapping failure."""


@dataclass(frozen=True)
class PreparedResponse:
    body: bytes
    gzip_body: bytes
    content_type: str
    etag: str
    gzip_etag: str
    cache_control: str

    @property
    def resident_bytes(self) -> int:
        """Bytes retained by the in-process raw and gzip response variants."""

        return len(self.body) + (
            0 if self.gzip_body is self.body else len(self.gzip_body)
        )


@dataclass
class CacheEntry:
    expires_at: float
    stale_until: float
    response: PreparedResponse
    last_accessed_at: float


@dataclass
class StaticCacheEntry:
    signature: tuple[int, ...]
    response: PreparedResponse


@dataclass
class LatencyAggregate:
    samples: int = 0
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_histogram: list[int] = field(
        default_factory=lambda: [0] * (len(_LATENCY_BUCKETS_MS) + 1)
    )

    def record(self, latency_ms: float) -> None:
        value = max(0.0, float(latency_ms))
        self.samples += 1
        self.latency_sum_ms += value
        self.latency_max_ms = max(self.latency_max_ms, value)
        bucket_index = bisect_left(_LATENCY_BUCKETS_MS, value)
        self.latency_histogram[bucket_index] += 1

    def merge(self, other: LatencyAggregate) -> None:
        self.samples += other.samples
        self.latency_sum_ms += other.latency_sum_ms
        self.latency_max_ms = max(self.latency_max_ms, other.latency_max_ms)
        for index, value in enumerate(other.latency_histogram):
            self.latency_histogram[index] += value

    def to_payload(self) -> dict[str, Any]:
        return {
            "samples": self.samples,
            "latency_sum_ms": round(self.latency_sum_ms, 6),
            "latency_max_ms": round(self.latency_max_ms, 6),
            "latency_histogram": list(self.latency_histogram),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> LatencyAggregate:
        result = cls()
        try:
            histogram = [max(0, int(value)) for value in payload.get("latency_histogram", [])]
            if len(histogram) != len(_LATENCY_BUCKETS_MS) + 1:
                return result
            result.samples = max(0, int(payload.get("samples") or 0))
            result.latency_sum_ms = max(0.0, float(payload.get("latency_sum_ms") or 0.0))
            result.latency_max_ms = max(0.0, float(payload.get("latency_max_ms") or 0.0))
            if (
                not math.isfinite(result.latency_sum_ms)
                or not math.isfinite(result.latency_max_ms)
                or result.samples != sum(histogram)
                or (result.samples == 0 and (result.latency_sum_ms or result.latency_max_ms))
                or (result.samples > 0 and result.latency_sum_ms < result.latency_max_ms)
            ):
                return cls()
            result.latency_histogram = histogram
        except (TypeError, ValueError, OverflowError):
            return cls()
        return result


@dataclass
class TrafficAggregate:
    requests: int = 0
    response_body_bytes: int = 0
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0
    errors: int = 0
    api_requests: int = 0
    page_requests: int = 0
    asset_requests: int = 0
    latency_histogram: list[int] = field(
        default_factory=lambda: [0] * (len(_LATENCY_BUCKETS_MS) + 1)
    )
    phases: dict[str, LatencyAggregate] = field(default_factory=dict)

    def record(
        self,
        *,
        latency_ms: float,
        response_body_bytes: int,
        status: int,
        route_kind: str,
        phase_durations_ms: Mapping[str, float] | None = None,
    ) -> None:
        self.requests += 1
        self.response_body_bytes += max(0, int(response_body_bytes))
        latency_value = max(0.0, float(latency_ms))
        self.latency_sum_ms += latency_value
        self.latency_max_ms = max(self.latency_max_ms, latency_value)
        self.errors += int(status >= 400)
        self.api_requests += int(route_kind == "api")
        self.page_requests += int(route_kind == "page")
        self.asset_requests += int(route_kind == "asset")
        bucket_index = bisect_left(_LATENCY_BUCKETS_MS, latency_value)
        self.latency_histogram[bucket_index] += 1
        for phase, phase_latency in (phase_durations_ms or {}).items():
            if phase not in _SERVER_PHASES:
                continue
            self.phases.setdefault(phase, LatencyAggregate()).record(phase_latency)

    def merge(self, other: TrafficAggregate) -> None:
        self.requests += other.requests
        self.response_body_bytes += other.response_body_bytes
        self.latency_sum_ms += other.latency_sum_ms
        self.latency_max_ms = max(self.latency_max_ms, other.latency_max_ms)
        self.errors += other.errors
        self.api_requests += other.api_requests
        self.page_requests += other.page_requests
        self.asset_requests += other.asset_requests
        for index, value in enumerate(other.latency_histogram):
            self.latency_histogram[index] += value
        for phase, aggregate in other.phases.items():
            self.phases.setdefault(phase, LatencyAggregate()).merge(aggregate)

    def to_payload(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "response_body_bytes": self.response_body_bytes,
            "latency_sum_ms": round(self.latency_sum_ms, 6),
            "latency_max_ms": round(self.latency_max_ms, 6),
            "errors": self.errors,
            "api_requests": self.api_requests,
            "page_requests": self.page_requests,
            "asset_requests": self.asset_requests,
            "latency_histogram": list(self.latency_histogram),
            "phases": {
                phase: aggregate.to_payload()
                for phase, aggregate in sorted(self.phases.items())
            },
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> TrafficAggregate:
        result = cls()
        try:
            histogram = [max(0, int(value)) for value in payload.get("latency_histogram", [])]
            if len(histogram) != len(_LATENCY_BUCKETS_MS) + 1:
                return result
            result.requests = max(0, int(payload.get("requests") or 0))
            result.response_body_bytes = max(0, int(payload.get("response_body_bytes") or 0))
            result.latency_sum_ms = max(0.0, float(payload.get("latency_sum_ms") or 0.0))
            result.latency_max_ms = max(0.0, float(payload.get("latency_max_ms") or 0.0))
            result.errors = max(0, int(payload.get("errors") or 0))
            result.api_requests = max(0, int(payload.get("api_requests") or 0))
            result.page_requests = max(0, int(payload.get("page_requests") or 0))
            result.asset_requests = max(0, int(payload.get("asset_requests") or 0))
            if (
                not math.isfinite(result.latency_sum_ms)
                or not math.isfinite(result.latency_max_ms)
                or result.requests != sum(histogram)
                or (result.requests == 0 and (result.latency_sum_ms or result.latency_max_ms))
                or (result.requests > 0 and result.latency_sum_ms < result.latency_max_ms)
                or result.errors > result.requests
                or result.api_requests + result.page_requests + result.asset_requests
                > result.requests
            ):
                return cls()
            result.latency_histogram = histogram
            phases = payload.get("phases") or {}
            if isinstance(phases, Mapping):
                result.phases = {
                    str(phase): LatencyAggregate.from_payload(row)
                    for phase, row in phases.items()
                    if phase in _SERVER_PHASES and isinstance(row, Mapping)
                }
        except (TypeError, ValueError, OverflowError):
            return cls()
        return result


@dataclass
class TrafficSecondBucket:
    epoch_second: int
    aggregate: TrafficAggregate = field(default_factory=TrafficAggregate)
    routes: dict[str, TrafficAggregate] = field(default_factory=dict)
    cache: dict[str, int] = field(default_factory=dict)


class PublicTrafficObserver:
    """Bounded, anonymous request telemetry for the public reader dashboard."""

    def __init__(self, *, history_root: Path | None = None) -> None:
        self._lock = threading.Lock()
        self._started_monotonic = time.monotonic()
        self._started_at_utc = datetime.now(UTC)
        self._process_key = hashlib.sha256(
            f"{time.time_ns()}:{id(self)}".encode("ascii")
        ).hexdigest()[:16]
        self._seconds: deque[TrafficSecondBucket] = deque(maxlen=3_600)
        self._lifetime = TrafficAggregate()
        self._cache_lifetime: dict[str, int] = {}
        self._in_flight = 0
        self._peak_in_flight = 0
        current_minute = int(time.time()) // 60 * 60
        self._current_minute = TrafficSecondBucket(epoch_second=current_minute)
        self._history_store = (
            PublicPerformanceHistoryStore(
                history_root,
                allowed_routes={
                    *_PUBLIC_API_ROUTES,
                    *_PUBLIC_PAGE_ROUTES,
                    "靜態資源",
                    "其他／未命中",
                },
                allowed_cache_outcomes=_CACHE_OUTCOMES,
            )
            if history_root is not None
            else None
        )
        self._rollover_stop = threading.Event()
        self._rollover_thread: threading.Thread | None = None
        self._closed = False
        if self._history_store is not None:
            self._rollover_thread = threading.Thread(
                target=self._rollover_loop,
                name="public-traffic-minute-rollover",
                daemon=True,
            )
            self._rollover_thread.start()

    @staticmethod
    def _route(path: str) -> tuple[str, str]:
        if path in _PUBLIC_API_ROUTES:
            return path, "api"
        if path in _PUBLIC_PAGE_ROUTES:
            return path, "page"
        if path.endswith((".css", ".js", ".ico", ".txt")):
            return "靜態資源", "asset"
        return "其他／未命中", "other"

    def _bucket_locked(self, epoch_second: int) -> TrafficSecondBucket:
        if self._seconds and self._seconds[-1].epoch_second == epoch_second:
            return self._seconds[-1]
        bucket = TrafficSecondBucket(epoch_second=epoch_second)
        self._seconds.append(bucket)
        return bucket

    def _minute_payload_locked(
        self, bucket: TrafficSecondBucket
    ) -> dict[str, Any]:
        aggregate = TrafficAggregate()
        routes: dict[str, TrafficAggregate] = {}
        cache: dict[str, int] = {}
        for second in reversed(self._seconds):
            if second.epoch_second < bucket.epoch_second:
                break
            if second.epoch_second >= bucket.epoch_second + 60:
                continue
            aggregate.merge(second.aggregate)
            for route, timing in second.routes.items():
                routes.setdefault(route, TrafficAggregate()).merge(timing)
            for outcome, count in second.cache.items():
                cache[outcome] = cache.get(outcome, 0) + count
        return {
            "schema_version": 1,
            "minute_epoch": bucket.epoch_second,
            "minute_utc": datetime.fromtimestamp(bucket.epoch_second, UTC).isoformat(),
            "process_key": self._process_key,
            "aggregate": aggregate.to_payload(),
            "routes": {
                route: aggregate.to_payload()
                for route, aggregate in sorted(routes.items())
            },
            "cache": dict(sorted(cache.items())),
        }

    def _roll_minute_locked(self, epoch_second: int) -> None:
        minute = int(epoch_second) // 60 * 60
        if minute == self._current_minute.epoch_second:
            return
        if self._history_store is not None:
            self._history_store.enqueue(
                self._minute_payload_locked(self._current_minute)
            )
        self._current_minute = TrafficSecondBucket(epoch_second=minute)

    def _rollover_loop(self) -> None:
        while not self._rollover_stop.wait(1.0):
            with self._lock:
                self._roll_minute_locked(int(time.time()))

    def request_started(self, path: str) -> bool:
        if path == "/healthz":
            return False
        with self._lock:
            self._in_flight += 1
            self._peak_in_flight = max(self._peak_in_flight, self._in_flight)
        return True

    def request_finished(
        self,
        *,
        observed: bool,
        path: str,
        status: int,
        latency_ms: float,
        response_body_bytes: int,
        phase_durations_ms: Mapping[str, float] | None = None,
    ) -> None:
        if not observed:
            return
        route, route_kind = self._route(path)
        with self._lock:
            self._roll_minute_locked(int(time.time()))
            self._in_flight = max(0, self._in_flight - 1)
            bucket = self._bucket_locked(int(time.time()))
            bucket.aggregate.record(
                latency_ms=latency_ms,
                response_body_bytes=response_body_bytes,
                status=status,
                route_kind=route_kind,
                phase_durations_ms=phase_durations_ms,
            )
            route_aggregate = bucket.routes.setdefault(route, TrafficAggregate())
            route_aggregate.record(
                latency_ms=latency_ms,
                response_body_bytes=response_body_bytes,
                status=status,
                route_kind=route_kind,
                phase_durations_ms=phase_durations_ms,
            )
            self._lifetime.record(
                latency_ms=latency_ms,
                response_body_bytes=response_body_bytes,
                status=status,
                route_kind=route_kind,
                phase_durations_ms=phase_durations_ms,
            )

    def record_cache(self, outcome: str) -> None:
        with self._lock:
            self._roll_minute_locked(int(time.time()))
            bucket = self._bucket_locked(int(time.time()))
            bucket.cache[outcome] = bucket.cache.get(outcome, 0) + 1
            self._cache_lifetime[outcome] = self._cache_lifetime.get(outcome, 0) + 1

    @staticmethod
    def _percentile(aggregate: TrafficAggregate, quantile: float) -> float | None:
        if aggregate.requests <= 0:
            return None
        target = max(1, int(aggregate.requests * quantile + 0.999999))
        cumulative = 0
        for index, count in enumerate(aggregate.latency_histogram):
            cumulative += count
            if cumulative < target:
                continue
            if index < len(_LATENCY_BUCKETS_MS):
                return round(
                    min(_LATENCY_BUCKETS_MS[index], aggregate.latency_max_ms),
                    3,
                )
            return round(aggregate.latency_max_ms, 3)
        return round(aggregate.latency_max_ms, 3)

    @staticmethod
    def _phase_percentile(
        aggregate: LatencyAggregate, quantile: float
    ) -> float | None:
        if aggregate.samples <= 0:
            return None
        target = max(1, int(aggregate.samples * quantile + 0.999999))
        cumulative = 0
        for index, count in enumerate(aggregate.latency_histogram):
            cumulative += count
            if cumulative < target:
                continue
            if index < len(_LATENCY_BUCKETS_MS):
                return round(
                    min(_LATENCY_BUCKETS_MS[index], aggregate.latency_max_ms),
                    3,
                )
            return round(aggregate.latency_max_ms, 3)
        return round(aggregate.latency_max_ms, 3)

    def _phase_payload(self, aggregate: TrafficAggregate) -> list[dict[str, Any]]:
        labels = {
            "cache_wait": "等待相同快取鍵",
            "build": "來源讀取、資料整理與序列化",
            "write": "回應 body 寫入",
            "other": "路由、headers 與其他框架成本",
        }
        rows: list[dict[str, Any]] = []
        for phase in _SERVER_PHASES:
            timing = aggregate.phases.get(phase, LatencyAggregate())
            rows.append(
                {
                    "phase": phase,
                    "label": labels[phase],
                    "samples": timing.samples,
                    "latency_average_ms": (
                        round(timing.latency_sum_ms / timing.samples, 3)
                        if timing.samples
                        else None
                    ),
                    "latency_p50_ms": self._phase_percentile(timing, 0.50),
                    "latency_p95_ms": self._phase_percentile(timing, 0.95),
                    "latency_p99_ms": self._phase_percentile(timing, 0.99),
                    "latency_max_ms": (
                        round(timing.latency_max_ms, 3)
                        if timing.samples
                        else None
                    ),
                }
            )
        return rows

    def _window_payload(
        self,
        aggregate: TrafficAggregate,
        *,
        seconds: int,
        uptime_seconds: float,
    ) -> dict[str, Any]:
        denominator = max(0.001, min(float(seconds), uptime_seconds))
        requests = aggregate.requests
        return {
            "window_seconds": seconds,
            "requests": requests,
            "requests_per_second": round(requests / denominator, 3),
            "response_body_bytes": aggregate.response_body_bytes,
            "response_body_bytes_per_second": round(
                aggregate.response_body_bytes / denominator, 3
            ),
            "latency_average_ms": (
                round(aggregate.latency_sum_ms / requests, 3) if requests else None
            ),
            "latency_p50_ms": self._percentile(aggregate, 0.50),
            "latency_p95_ms": self._percentile(aggregate, 0.95),
            "latency_p99_ms": self._percentile(aggregate, 0.99),
            "latency_max_ms": (
                round(aggregate.latency_max_ms, 3) if requests else None
            ),
            "errors": aggregate.errors,
            "error_ratio": round(aggregate.errors / requests, 6) if requests else 0.0,
            "api_requests": aggregate.api_requests,
            "page_requests": aggregate.page_requests,
            "asset_requests": aggregate.asset_requests,
        }

    def _route_payload(
        self,
        routes: Mapping[str, TrafficAggregate],
        *,
        total_requests: int,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for route, aggregate in sorted(
            routes.items(), key=lambda item: item[1].requests, reverse=True
        ):
            requests = aggregate.requests
            result.append(
                {
                    "route": route,
                    "requests": requests,
                    "share": round(requests / total_requests, 6)
                    if total_requests
                    else 0.0,
                    "latency_average_ms": (
                        round(aggregate.latency_sum_ms / requests, 3)
                        if requests
                        else None
                    ),
                    "latency_p50_ms": self._percentile(aggregate, 0.50),
                    "latency_p95_ms": self._percentile(aggregate, 0.95),
                    "latency_p99_ms": self._percentile(aggregate, 0.99),
                    "latency_max_ms": (
                        round(aggregate.latency_max_ms, 3)
                        if requests
                        else None
                    ),
                    "response_body_bytes": aggregate.response_body_bytes,
                    "errors": aggregate.errors,
                    "error_ratio": round(aggregate.errors / requests, 6)
                    if requests
                    else 0.0,
                    "phases": self._phase_payload(aggregate),
                }
            )
        return result

    @staticmethod
    def _cache_payload(outcomes: Mapping[str, int]) -> dict[str, Any]:
        hits = sum(
            int(outcomes.get(key, 0))
            for key in ("fresh_hit", "stale_hit", "coalesced_hit", "static_hit")
        )
        builds = sum(
            int(outcomes.get(key, 0))
            for key in ("build", "background_build", "static_build")
        )
        total = hits + builds
        return {
            "lookups": total,
            "hits": hits,
            "builds": builds,
            "hit_ratio": round(hits / total, 6) if total else None,
            "outcomes": dict(sorted(outcomes.items())),
        }

    def snapshot(self, *, exclude_current_request: bool = False) -> dict[str, Any]:
        now_epoch = int(time.time())
        uptime_seconds = max(0.001, time.monotonic() - self._started_monotonic)
        with self._lock:
            buckets = list(self._seconds)
            lifetime = TrafficAggregate()
            lifetime.merge(self._lifetime)
            cache_lifetime = dict(self._cache_lifetime)
            in_flight = max(
                0,
                self._in_flight - int(exclude_current_request),
            )
            peak_in_flight = self._peak_in_flight

        windows = {
            60: TrafficAggregate(),
            300: TrafficAggregate(),
            3_600: TrafficAggregate(),
        }
        route_hour: dict[str, TrafficAggregate] = {}
        cache_minute: dict[str, int] = {}
        minute_totals: dict[int, TrafficAggregate] = {}
        for bucket in buckets:
            age = now_epoch - bucket.epoch_second
            if age < 0 or age >= 3_600:
                continue
            windows[3_600].merge(bucket.aggregate)
            if age < 300:
                windows[300].merge(bucket.aggregate)
            if age < 60:
                windows[60].merge(bucket.aggregate)
                for key, value in bucket.cache.items():
                    cache_minute[key] = cache_minute.get(key, 0) + value
            for route, aggregate in bucket.routes.items():
                route_hour.setdefault(route, TrafficAggregate()).merge(aggregate)
            minute = bucket.epoch_second - bucket.epoch_second % 60
            minute_totals.setdefault(minute, TrafficAggregate()).merge(bucket.aggregate)

        trend: list[dict[str, Any]] = []
        current_minute = now_epoch - now_epoch % 60
        for offset in range(59, -1, -1):
            minute = current_minute - offset * 60
            aggregate = minute_totals.get(minute, TrafficAggregate())
            elapsed = max(1, now_epoch - minute + 1) if offset == 0 else 60
            trend.append(
                {
                    "minute_utc": datetime.fromtimestamp(minute, UTC).isoformat(),
                    "requests": aggregate.requests,
                    "requests_per_second": round(aggregate.requests / elapsed, 3),
                    "latency_p95_ms": self._percentile(aggregate, 0.95),
                    "response_body_bytes": aggregate.response_body_bytes,
                    "errors": aggregate.errors,
                }
            )

        routes = self._route_payload(
            route_hour,
            total_requests=windows[3_600].requests,
        )
        cache_payload = self._cache_payload(cache_lifetime)
        cache_payload["last_1m_outcomes"] = cache_minute
        return {
            "schema_version": 2,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "process_started_at_utc": self._started_at_utc.isoformat(),
            "uptime_seconds": round(uptime_seconds, 3),
            "read_only": True,
            "production_control_possible": False,
            "limits": {
                "global_rate_limit_enabled": False,
                "application_concurrency_limit_enabled": False,
                "request_queue_size": 1_024,
            },
            "connections": {
                "in_flight": in_flight,
                "peak_in_flight": peak_in_flight,
            },
            "lifetime": self._window_payload(
                lifetime,
                seconds=max(1, int(uptime_seconds) + 1),
                uptime_seconds=uptime_seconds,
            ),
            "windows": {
                "1m": self._window_payload(
                    windows[60], seconds=60, uptime_seconds=uptime_seconds
                ),
                "5m": self._window_payload(
                    windows[300], seconds=300, uptime_seconds=uptime_seconds
                ),
                "1h": self._window_payload(
                    windows[3_600], seconds=3_600, uptime_seconds=uptime_seconds
                ),
            },
            "cache": cache_payload,
            "route_window": "1h",
            "routes": routes,
            "server_phases": self._phase_payload(windows[3_600]),
            "history_storage": (
                self._history_store.status()
                if self._history_store is not None
                else {"enabled": False, "retention_days": 0}
            ),
            "trend": trend,
            "definitions": {
                "request": "公開閘道完成的一次 GET 或 HEAD；Caddy 健康檢查不列入。",
                "response_body_bytes": "閘道實際寫出的壓縮或未壓縮回應 body；不含 HTTP headers。",
                "latency": "公開閘道從收到請求到完成 body 寫出的 wall time。",
                "percentile": "固定延遲 histogram 的保守上界估計；最大值保留實測值。",
                "error_ratio": "HTTP 4xx 與 5xx 回應數除以完成請求數。",
                "visitor": "此記憶體計量不依 IP 或 User-Agent 分群，也不公開訪客識別；request 不等於人數。",
                "cache_hit_ratio": "伺服器 JSON 與靜態資源快取查找命中數除以命中加建置數。",
                "server_phases": "cache_wait、build、write 與 other 互不重疊；其總和近似閘道 wall time，histogram 分位數不可逐項相加。",
                "retention": "一秒桶保留最近一小時；每分鐘匿名聚合由背景執行緒非同步保存 90 天，服務停止的分鐘保留為未觀測而不是零流量。",
            },
        }

    def history_snapshot(self, range_key: str) -> dict[str, Any]:
        if range_key not in _TRAFFIC_HISTORY_RANGES:
            raise InvalidPublicRequest("unsupported traffic history range")
        range_seconds, bin_seconds = _TRAFFIC_HISTORY_RANGES[range_key]
        now_epoch = int(time.time())
        expected_minutes = max(1, range_seconds // 60)
        current_minute = now_epoch - now_epoch % 60
        since_epoch = current_minute - (expected_minutes - 1) * 60
        rows = (
            self._history_store.rows_since(since_epoch)
            if self._history_store is not None
            else []
        )
        with self._lock:
            if self._current_minute.epoch_second >= since_epoch:
                rows.append(self._minute_payload_locked(self._current_minute))

        aggregate = TrafficAggregate()
        routes: dict[str, TrafficAggregate] = {}
        cache_outcomes: dict[str, int] = {}
        bins: dict[int, TrafficAggregate] = {}
        bin_observed_minutes: dict[int, set[int]] = {}
        observed_minutes: set[int] = set()
        process_keys: set[str] = set()
        for row in rows:
            minute = int(row.get("minute_epoch") or 0)
            if minute < since_epoch:
                continue
            row_aggregate = TrafficAggregate.from_payload(row.get("aggregate") or {})
            aggregate.merge(row_aggregate)
            minute_mask = max(0, int(row.get("observed_minute_mask") or 0))
            row_minutes = (
                {
                    minute + offset * 60
                    for offset in range(60)
                    if minute_mask & (1 << offset)
                }
                if int(row.get("rollup_seconds") or 0) == 3_600
                else {minute}
            )
            observed_minutes.update(row_minutes)
            persisted_process_keys = row.get("process_keys") or ()
            if isinstance(persisted_process_keys, (list, tuple, set)) and persisted_process_keys:
                process_keys.update(str(value) for value in persisted_process_keys)
            else:
                process_keys.add(str(row.get("process_key") or ""))
            for route, payload in (row.get("routes") or {}).items():
                if not isinstance(payload, Mapping):
                    continue
                routes.setdefault(str(route), TrafficAggregate()).merge(
                    TrafficAggregate.from_payload(payload)
                )
            for outcome, count in (row.get("cache") or {}).items():
                try:
                    cache_outcomes[str(outcome)] = (
                        cache_outcomes.get(str(outcome), 0) + max(0, int(count))
                    )
                except (TypeError, ValueError, OverflowError):
                    continue
            bin_epoch = minute - minute % bin_seconds
            bins.setdefault(bin_epoch, TrafficAggregate()).merge(row_aggregate)
            bin_observed_minutes.setdefault(bin_epoch, set()).update(row_minutes)

        first_bin = (since_epoch // bin_seconds) * bin_seconds
        last_bin = (now_epoch // bin_seconds) * bin_seconds
        trend: list[dict[str, Any]] = []
        for bin_epoch in range(first_bin, last_bin + 1, bin_seconds):
            row_aggregate = bins.get(bin_epoch, TrafficAggregate())
            trend.append(
                {
                    "bucket_start_utc": datetime.fromtimestamp(
                        bin_epoch, UTC
                    ).isoformat(),
                    "bucket_seconds": bin_seconds,
                    "observed_minutes": len(
                        bin_observed_minutes.get(bin_epoch, set())
                    ),
                    "requests": row_aggregate.requests,
                    "requests_per_second": (
                        round(
                            row_aggregate.requests
                            / (
                                len(bin_observed_minutes.get(bin_epoch, set()))
                                * 60
                            ),
                            6,
                        )
                        if bin_observed_minutes.get(bin_epoch)
                        else None
                    ),
                    "latency_p50_ms": self._percentile(row_aggregate, 0.50),
                    "latency_p95_ms": self._percentile(row_aggregate, 0.95),
                    "latency_p99_ms": self._percentile(row_aggregate, 0.99),
                    "response_body_bytes": row_aggregate.response_body_bytes,
                    "errors": row_aggregate.errors,
                }
            )

        return {
            "schema_version": 1,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "range": range_key,
            "range_seconds": range_seconds,
            "bucket_seconds": bin_seconds,
            "read_only": True,
            "production_control_possible": False,
            "summary": self._window_payload(
                aggregate,
                seconds=range_seconds,
                uptime_seconds=float(range_seconds),
            ),
            "coverage": {
                "observed_minutes": len(observed_minutes),
                "expected_minutes": expected_minutes,
                "ratio": round(len(observed_minutes) / expected_minutes, 6),
                "first_observed_minute_utc": (
                    datetime.fromtimestamp(min(observed_minutes), UTC).isoformat()
                    if observed_minutes
                    else None
                ),
                "last_observed_minute_utc": (
                    datetime.fromtimestamp(max(observed_minutes), UTC).isoformat()
                    if observed_minutes
                    else None
                ),
                "process_segments": len(process_keys - {""}),
                "meaning": "有 gateway 心跳聚合的分鐘；未觀測不等於零請求。",
            },
            "cache": self._cache_payload(cache_outcomes),
            "server_phases": self._phase_payload(aggregate),
            "routes": self._route_payload(
                routes,
                total_requests=aggregate.requests,
            ),
            "trend": trend,
            "history_storage": (
                self._history_store.status()
                if self._history_store is not None
                else {"enabled": False, "retention_days": 0}
            ),
            "definitions": {
                "source": "公開 gateway 的實際完成請求與每分鐘存活心跳；不含虛構或補零樣本。",
                "latency": "從 Python gateway 開始處理至 body 寫入完成；Caddy、WAN 與瀏覽器繪製另由瀏覽器測速觀察。",
                "percentile": "由固定 histogram 對實際樣本估計的保守上界；不同階段的分位數不可相加。",
                "coverage": "服務有寫出分鐘聚合的時間比例，不是網站可用率 SLA。",
                "privacy": "只保存 allowlist 路由與聚合數值；不保存 query、IP、User-Agent、Cookie、帳號或輸入內容。",
            },
        }

    def close(self) -> None:
        if self._history_store is None or self._closed:
            return
        self._closed = True
        self._rollover_stop.set()
        if self._rollover_thread is not None:
            self._rollover_thread.join(timeout=2.0)
        with self._lock:
            self._history_store.enqueue(
                self._minute_payload_locked(self._current_minute)
            )
        self._history_store.close()


def _prepared(
    body: bytes,
    *,
    content_type: str,
    cache_control: str,
) -> PreparedResponse:
    digest = hashlib.sha256(body).hexdigest()
    compressed = (
        gzip.compress(body, compresslevel=5, mtime=0) if len(body) >= 1_024 else body
    )
    return PreparedResponse(
        body=body,
        gzip_body=compressed,
        content_type=content_type,
        etag=f'"sha256-{digest}"',
        gzip_etag=f'"sha256-{hashlib.sha256(compressed).hexdigest()}"',
        cache_control=cache_control,
    )


def _preferred_encoding(header: str | None, *, gzip_available: bool) -> str | None:
    """Negotiate our two representations; explicit q=0 always excludes a coding."""

    if not header:
        return "identity"
    qualities: dict[str, float] = {}
    for item in header.lower().split(","):
        coding, *parameters = item.strip().split(";")
        coding = coding.strip()
        quality = 1.0
        seen_quality = False
        for parameter in parameters:
            name, separator, value = parameter.strip().partition("=")
            if (
                name.strip() != "q"
                or not separator
                or seen_quality
                or re.fullmatch(r"(?:0(?:\.\d{0,3})?|1(?:\.0{0,3})?)", value.strip())
                is None
            ):
                quality = 0.0
                break
            seen_quality = True
            quality = float(value)
        # Repeated contradictory values are malformed; never override q=0.
        qualities[coding] = min(quality, qualities.get(coding, quality))
    gzip_quality = (
        qualities.get("gzip", qualities.get("*", 0.0)) if gzip_available else 0.0
    )
    identity_quality = qualities.get(
        "identity", 0.0 if qualities.get("*") == 0.0 else 1.0
    )
    if gzip_quality > 0 and (
        "identity" not in qualities or gzip_quality >= identity_quality
    ):
        return "gzip"
    return "identity" if identity_quality > 0 else None


def _etag_matches(header: str | None, etag: str) -> bool:
    """GET/HEAD If-None-Match uses weak comparison, including lists and *."""

    if not header:
        return False
    if header.strip() == "*":
        return True
    return any(
        token.removeprefix("W/") == etag
        for token in re.findall(r'(?:W/)?"[^"\r\n]*"', header)
    )


def _response_json(response: PreparedResponse) -> dict[str, Any]:
    payload = json.loads(response.body)
    if not isinstance(payload, dict):
        raise ValueError("cached JSON root is not an object")
    return payload


def _open_position_summary(payload: Mapping[str, Any]) -> tuple[int, int]:
    open_count = 0
    stale_count = 0
    positions = payload.get("positions")
    if isinstance(positions, list) and positions:
        for row in positions:
            if not isinstance(row, Mapping) or not row.get("signed_shares"):
                continue
            open_count += 1
            if row.get("valuation_stale"):
                stale_count += 1
    else:
        for row in payload.get("modes") or ():
            if not isinstance(row, Mapping):
                continue
            open_count += int(row.get("open_position_count") or 0)
            stale_count += int(row.get("stale_position_count") or 0)
    return open_count, stale_count


def summarize_tw_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Project the frequently polled TW fields without its large ledgers."""

    open_count, stale_count = _open_position_summary(payload)
    allowed = (
        "schema_version",
        "dashboard_schema_version",
        "generated_at_utc",
        "health",
        "source_age_seconds",
        "source_updated_at",
        "service_sync",
        "session_date",
        "available_session_dates",
        "simulation_only",
        "production_order_possible",
        "current_market_phase",
        "modes",
        "benchmarks",
        "record_counts",
        "execution_records",
        "session_progress",
        "preopen",
    )
    summary = {key: payload[key] for key in allowed if key in payload}
    summary["open_position_count"] = open_count
    summary["stale_position_count"] = stale_count
    return summary


def build_compact_tw_overview_status(
    state_dir: Path,
    *,
    opening_gate_path: Path = DEFAULT_OPENING_GATE_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read only the atomic engine receipt needed by the landing card.

    The complete TW status intentionally joins historical ledgers and benchmark
    curves for the detail page. Making the landing card depend on that scan
    caused a cold gateway to exceed the reverse-proxy timeout even though the
    engine already maintained a small atomic status receipt.
    """

    status_path = Path(state_dir) / "status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    if not isinstance(status, Mapping):
        raise ValueError("TW status receipt root is not an object")
    if status.get("simulation_only") is not True:
        raise UnsafePublicDashboardPayload("simulation_only must be true")
    if status.get("production_order_possible") is not False:
        raise UnsafePublicDashboardPayload("production_order_possible must be false")

    observed = (now or datetime.now(UTC)).astimezone(UTC)
    source_updated_at = status.get("updated_at")
    source_age_seconds: float | None = None
    if source_updated_at:
        parsed = datetime.fromisoformat(str(source_updated_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("TW status updated_at has no timezone")
        source_age_seconds = max(
            0.0,
            (observed - parsed.astimezone(UTC)).total_seconds(),
        )

    health = str(status.get("health") or "unknown")
    if (
        source_age_seconds is None
        or source_age_seconds > DEFAULT_MAX_SOURCE_AGE_SECONDS
    ):
        health = "stale"

    local_session_date = observed.astimezone(ZoneInfo("Asia/Taipei")).date().isoformat()
    try:
        opening_gate = json.loads(Path(opening_gate_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        opening_gate = {}
    if not isinstance(opening_gate, Mapping):
        raise ValueError("TW opening-gate receipt root is not an object")
    if (
        str(opening_gate.get("session_date") or "") == local_session_date
        and opening_gate.get("status") == "failed"
        and health not in {"stale", "critical"}
    ):
        health = "degraded"

    modes: list[dict[str, Any]] = []
    compact_operational_issue_count = 0
    raw_modes = status.get("modes")
    if isinstance(raw_modes, Mapping):
        for market, raw_mode in raw_modes.items():
            mode = raw_mode if isinstance(raw_mode, Mapping) else {}
            open_position_count = int(mode.get("open_position_count") or 0)
            stale_position_count = int(mode.get("stale_position_count") or 0)
            force_exit_failures = int(mode.get("force_exit_failures") or 0)
            unresolved_exit_count = int(mode.get("unresolved_exit_count") or 0)
            product = str(mode.get("product") or "tw_day_trade")
            requested_shares = int(mode.get("entry_requested_shares") or 0)
            filled_shares = int(mode.get("entry_filled_shares") or 0)
            entry_incomplete = (
                product != "tw_overnight"
                and requested_shares > filled_shares
                and str(mode.get("entry_fill_outcome") or "") in {"partial", "no_fill"}
            )
            if (
                stale_position_count
                or force_exit_failures
                or unresolved_exit_count
                or entry_incomplete
            ):
                compact_operational_issue_count += 1
            modes.append(
                {
                    "market": str(market),
                    "open_position_count": open_position_count,
                    "stale_position_count": stale_position_count,
                }
            )

    # ``status.json`` describes the engine's current schedule state, so a
    # closed market can legitimately be ``waiting`` while its latest execution
    # still contains a partial entry or an unflattened stale position.  The
    # detail endpoint already degrades for those facts; keep the inexpensive
    # landing-card projection consistent without loading the large ledgers.
    if compact_operational_issue_count and health not in {"stale", "critical"}:
        health = "degraded"

    return {
        "schema_version": 1,
        "generated_at_utc": observed.isoformat(timespec="seconds"),
        "health": health,
        "source_updated_at": source_updated_at,
        "source_age_seconds": (
            round(source_age_seconds, 3) if source_age_seconds is not None else None
        ),
        "simulation_only": True,
        "production_order_possible": False,
        "modes": modes,
        "operational_issue_modes": compact_operational_issue_count,
    }


def build_public_overview(
    taifex: Mapping[str, Any],
    tw: Mapping[str, Any],
    shioaji: Mapping[str, Any],
    openbb: Mapping[str, Any] | None = None,
    data_monitor: Mapping[str, Any] | None = None,
    traffic: Mapping[str, Any] | None = None,
    overnight: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return only the fields required by the public landing cards."""

    tw_open, _ = _open_position_summary(tw)
    overnight = overnight if isinstance(overnight, Mapping) else {}
    overnight_open, _ = _open_position_summary(overnight)
    taifex_strategies = taifex.get("strategies")
    tw_modes = tw.get("modes")
    overnight_modes = overnight.get("modes")
    shioaji_traffic = shioaji.get("traffic")
    backfill = shioaji.get("backfill")
    pipeline_summary = shioaji.get("pipeline_summary")
    market = taifex.get("market")
    strategy_counts = taifex.get("strategy_counts")
    openbb = openbb if isinstance(openbb, Mapping) else {}
    openbb_archive = openbb.get("archive")
    openbb_archive = openbb_archive if isinstance(openbb_archive, Mapping) else {}
    data_monitor = data_monitor if isinstance(data_monitor, Mapping) else {}
    data_summary = data_monitor.get("summary")
    data_summary = data_summary if isinstance(data_summary, Mapping) else {}
    shioaji_traffic = shioaji_traffic if isinstance(shioaji_traffic, Mapping) else {}
    traffic = traffic if isinstance(traffic, Mapping) else {}
    traffic_windows = traffic.get("windows")
    traffic_windows = traffic_windows if isinstance(traffic_windows, Mapping) else {}
    traffic_minute = traffic_windows.get("1m")
    traffic_minute = traffic_minute if isinstance(traffic_minute, Mapping) else {}
    traffic_connections = traffic.get("connections")
    traffic_connections = (
        traffic_connections if isinstance(traffic_connections, Mapping) else {}
    )
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "taifex": {
            "health": taifex.get("health"),
            "source_age_seconds": taifex.get("source_age_seconds"),
            "live_strategies": (
                strategy_counts.get("live_ideal")
                if isinstance(strategy_counts, Mapping)
                else len(taifex_strategies)
                if isinstance(taifex_strategies, list)
                else 0
            ),
            "book_coverage_ratio": (
                market.get("book_coverage_ratio")
                if isinstance(market, Mapping)
                else None
            ),
        },
        "tw": {
            "health": tw.get("health"),
            "source_age_seconds": tw.get("source_age_seconds"),
            "modes": len(tw_modes) if isinstance(tw_modes, list) else 0,
            "open_positions": tw_open,
        },
        "overnight": {
            "health": overnight.get("health"),
            "source_age_seconds": overnight.get("source_age_seconds"),
            "modes": (len(overnight_modes) if isinstance(overnight_modes, list) else 0),
            "open_positions": overnight_open,
        },
        "shioaji": {
            "health": shioaji.get("health"),
            "source_age_seconds": shioaji.get("source_age_seconds"),
            "traffic_used_ratio": (shioaji_traffic.get("used_ratio")),
            "safe_remaining_bytes": (shioaji_traffic.get("safe_remaining_bytes")),
            "completed_contracts": (
                backfill.get("completed_contracts")
                if isinstance(backfill, Mapping)
                else 0
            ),
            "inventory_contracts": (
                backfill.get("inventory_contracts")
                if isinstance(backfill, Mapping)
                else 0
            ),
            "progress_ratio": (
                backfill.get("progress_ratio")
                if isinstance(backfill, Mapping)
                else None
            ),
            "pipeline_total": (
                pipeline_summary.get("total")
                if isinstance(pipeline_summary, Mapping)
                else 0
            ),
        },
        "openbb": {
            "health": openbb.get("health"),
            "snapshot_state": openbb.get("snapshot_state"),
            "source_age_seconds": openbb.get("source_age_seconds"),
            "completion_percent": openbb_archive.get("completion_percent"),
            "accepted_tasks": openbb_archive.get("accepted_tasks", 0),
            "total_tasks": openbb_archive.get("total_tasks", 0),
            "success_rows": openbb_archive.get("success_rows", 0),
        },
        "data_monitor": {
            "health": data_monitor.get("health"),
            "registered_items": data_summary.get("registered_items", 0),
            "healthy_or_progressing": data_summary.get("healthy_or_progressing", 0),
            "attention_required": data_summary.get("attention_required", 0),
            "source_level_ratio": data_summary.get("source_level_ratio"),
        },
        "traffic": {
            "requests_1m": traffic_minute.get("requests", 0),
            "requests_per_second_1m": traffic_minute.get("requests_per_second", 0),
            "latency_p95_ms_1m": traffic_minute.get("latency_p95_ms"),
            "in_flight": traffic_connections.get("in_flight", 0),
            "peak_in_flight": traffic_connections.get("peak_in_flight", 0),
        },
    }


class PublicDashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1_024
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        public_static_root: Path,
        taifex_static_root: Path,
        tw_static_root: Path,
        shioaji_static_root: Path,
        openbb_static_root: Path,
        data_monitor_static_root: Path,
        traffic_static_root: Path,
        repo_root: Path,
        taifex_upstream: str,
        tw_upstream: str,
        overnight_static_root: Path | None = None,
        traffic_history_root: Path | None = None,
    ) -> None:
        super().__init__(address, PublicDashboardHandler)
        self.public_static_root = Path(public_static_root)
        self.taifex_static_root = Path(taifex_static_root)
        self.tw_static_root = Path(tw_static_root)
        self.overnight_static_root = Path(overnight_static_root or tw_static_root)
        self.shioaji_static_root = Path(shioaji_static_root)
        self.openbb_static_root = Path(openbb_static_root)
        self.data_monitor_static_root = Path(data_monitor_static_root)
        self.traffic_static_root = Path(traffic_static_root)
        self.repo_root = Path(repo_root)
        self.taifex_upstream = str(taifex_upstream).rstrip("/")
        self.tw_upstream = str(tw_upstream).rstrip("/")
        self.traffic_observer = PublicTrafficObserver(
            history_root=traffic_history_root
        )
        self.request_metrics = threading.local()
        self._cache: dict[str, CacheEntry] = {}
        self._cache_bytes = 0
        self._cache_lock = threading.Lock()
        # Builders and waiters hold strong references. A lock disappears only
        # after its last user, independently of success, failure or LRU eviction.
        self._cache_key_locks: WeakValueDictionary[str, threading.Lock] = (
            WeakValueDictionary()
        )
        self._refreshing: set[str] = set()
        self._static_cache: dict[Path, StaticCacheEntry] = {}
        self._static_cache_lock = threading.Lock()
        bot = self.repo_root / "artifacts/discord_bot"
        self.update_hub = DashboardUpdateHub(
            {
                topic: (
                    self.repo_root / f"artifacts/live/{directory}/service_sync.json",
                    self.repo_root / f"artifacts/live/{directory}/status.json",
                    benchmark_projection_head_path(
                        self.repo_root / f"artifacts/live/{directory}"
                    ),
                    bot / "service_status.json",
                    bot / "preopen_readiness.json",
                )
                for topic, directory in (
                    ("tw", "tw_day_trade_simulation"),
                    ("overnight", "tw_overnight_simulation"),
                )
            }
        )
        overnight_root = self.repo_root / "artifacts/live/tw_overnight_simulation"
        self.update_hub.paths["overnight"] = (
            *self.update_hub.paths["overnight"],
            overnight_root / "overnight_history.json",
            overnight_root / "overnight_signal_history.parquet",
            overnight_root / "overnight_event_history.parquet",
        )

    def server_close(self) -> None:
        self.update_hub.close()
        self.traffic_observer.close()
        super().server_close()

    def _cache_observation(self, kind: str) -> None:
        self.traffic_observer.record_cache(kind)
        metrics = getattr(self.request_metrics, "current", None)
        if metrics is not None:
            metrics["cache"].add(kind)

    def _measured_build(
        self, builder: Callable[[], PreparedResponse]
    ) -> PreparedResponse:
        metrics = getattr(self.request_metrics, "current", None)
        if metrics is None:
            return builder()
        outer = metrics["depth"] == 0
        metrics["depth"] += 1
        started = time.perf_counter()
        try:
            return builder()
        finally:
            metrics["depth"] -= 1
            if outer:
                metrics["build_ms"] += (time.perf_counter() - started) * 1000

    def content_token(self, topic: str = "tw") -> str:
        response = self.tw_revision() if topic == "tw" else self.overnight_revision()
        revision = str(_response_json(response).get("revision_token") or "missing")
        directory = (
            "tw_day_trade_simulation" if topic == "tw" else "tw_overnight_simulation"
        )
        projection_head = benchmark_projection_head_path(
            self.repo_root / f"artifacts/live/{directory}"
        )
        try:
            stat = projection_head.stat()
            projection_revision = f"{stat.st_size}:{stat.st_mtime_ns}"
        except FileNotFoundError:
            projection_revision = "none"
        return f"{revision}:{projection_revision}"

    def cached_static(
        self,
        target: Path,
        *,
        content_type: str,
        cache_control: str,
    ) -> PreparedResponse:
        """Reuse immutable bytes until metadata changes, including atomic swaps."""

        signature = metadata_signature(target.stat())
        with self._static_cache_lock:
            cached = self._static_cache.get(target)
            if cached is not None and cached.signature == signature:
                self._cache_observation("static_hit")
                return cached.response
        response = _prepared(
            target.read_bytes(),
            content_type=content_type,
            cache_control=cache_control,
        )
        if metadata_signature(target.stat()) == signature:
            with self._static_cache_lock:
                self._static_cache[target] = StaticCacheEntry(
                    signature=signature, response=response
                )
        self._cache_observation("static_build")
        return response

    def _store_cached_response(
        self,
        cache_key: str,
        response: PreparedResponse,
        ttl_seconds: float,
        stale_grace_seconds: float | None = None,
    ) -> None:
        observed = time.monotonic()
        stale_grace = (
            max(15.0, float(ttl_seconds) * 3.0)
            if stale_grace_seconds is None
            else max(0.0, float(stale_grace_seconds))
        )
        with self._cache_lock:
            previous = self._cache.pop(cache_key, None)
            if previous is not None:
                self._cache_bytes -= previous.response.resident_bytes
            if response.resident_bytes > MAX_CACHE_BYTES or MAX_CACHE_ENTRIES < 1:
                return  # Still return the response to its caller, without retaining it.
            self._cache[cache_key] = CacheEntry(
                expires_at=observed + float(ttl_seconds),
                stale_until=observed + float(ttl_seconds) + stale_grace,
                response=response,
                last_accessed_at=observed,
            )
            self._cache_bytes += response.resident_bytes
            removable = sorted(
                (
                    (entry.last_accessed_at, key)
                    for key, entry in self._cache.items()
                    if key != cache_key
                )
            )
            for _, key in removable:
                if (
                    len(self._cache) <= MAX_CACHE_ENTRIES
                    and self._cache_bytes <= MAX_CACHE_BYTES
                ):
                    break
                removed = self._cache.pop(key, None)
                if removed is not None:
                    self._cache_bytes -= removed.response.resident_bytes
        # A new content revision may temporarily return the prior verified view.
        # Notify again when its replacement is committed; otherwise viewers can
        # consume the revision notification but keep that stale view for a minute.
        for topic, prefix in (("tw", "tw-status:"), ("overnight", "overnight-status:")):
            if cache_key.startswith(prefix):
                self.update_hub.publish(topic)

    def cache_residency(self) -> dict[str, int]:
        """Return bounded, non-sensitive cache capacity and occupancy metrics."""

        with self._cache_lock:
            return {
                "resident_entries": len(self._cache),
                "resident_bytes": max(0, self._cache_bytes),
                "maximum_entries": MAX_CACHE_ENTRIES,
                "maximum_resident_bytes": MAX_CACHE_BYTES,
            }

    def _background_refresh(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        stale_grace_seconds: float | None,
        builder: Callable[[], PreparedResponse],
        key_lock: threading.Lock,
    ) -> None:
        try:
            with key_lock:
                self.traffic_observer.record_cache("background_build")
                response = builder()
                self._store_cached_response(
                    cache_key,
                    response,
                    ttl_seconds,
                    stale_grace_seconds,
                )
        except Exception as error:  # keep the last verified response on refresh errors
            with self._cache_lock:
                cached = self._cache.get(cache_key)
                if cached is not None:
                    cached.expires_at = min(
                        cached.stale_until,
                        time.monotonic() + min(2.0, max(0.5, float(ttl_seconds))),
                    )
            sys.stderr.write(
                f"public-dashboard background_refresh_failed key={cache_key} "
                f"error={type(error).__name__}\n"
            )
        finally:
            with self._cache_lock:
                self._refreshing.discard(cache_key)

    def _cached_response(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        builder: Callable[[], PreparedResponse],
        stale_grace_seconds: float | None = None,
    ) -> PreparedResponse:
        """Per-key single-flight cache with stale-while-refresh behavior."""

        start_background = False
        with self._cache_lock:
            observed = time.monotonic()
            cached = self._cache.get(cache_key)
            if cached is not None:
                cached.last_accessed_at = observed
                if cached.expires_at > observed:
                    self._cache_observation("fresh_hit")
                    return cached.response
                key_lock = self._cache_key_locks.setdefault(cache_key, threading.Lock())
                if cached.stale_until > observed:
                    if cache_key not in self._refreshing:
                        self._refreshing.add(cache_key)
                        start_background = True
                    stale_response = cached.response
                else:
                    stale_response = None
            else:
                key_lock = self._cache_key_locks.setdefault(cache_key, threading.Lock())
                stale_response = None

        if stale_response is not None:
            self._cache_observation("stale_hit")
            if start_background:
                threading.Thread(
                    target=self._background_refresh,
                    kwargs={
                        "cache_key": cache_key,
                        "ttl_seconds": ttl_seconds,
                        "stale_grace_seconds": stale_grace_seconds,
                        "builder": builder,
                        "key_lock": key_lock,
                    },
                    name=f"public-cache-{cache_key[:48]}",
                    daemon=True,
                ).start()
            return stale_response

        lock_started = time.perf_counter()
        with key_lock:
            metrics = getattr(self.request_metrics, "current", None)
            if metrics is not None:
                metrics["cache_wait_ms"] += (time.perf_counter() - lock_started) * 1000
            with self._cache_lock:
                observed = time.monotonic()
                cached = self._cache.get(cache_key)
                if cached is not None and cached.expires_at > observed:
                    cached.last_accessed_at = observed
                    self._cache_observation("coalesced_hit")
                    return cached.response
            self._cache_observation("build")
            response = self._measured_build(builder)
            self._store_cached_response(
                cache_key,
                response,
                ttl_seconds,
                stale_grace_seconds,
            )
            return response

    def cached_local_json(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        cache_control: str,
        builder: Callable[[], Mapping[str, Any]],
        stale_grace_seconds: float | None = None,
    ) -> PreparedResponse:
        """Build and cache a local allowlisted status payload."""

        def build() -> PreparedResponse:
            encoded = (
                json.dumps(
                    dict(builder()),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            response = _prepared(
                encoded,
                content_type="application/json; charset=utf-8",
                cache_control=cache_control,
            )
            return response

        return self._cached_response(
            cache_key=cache_key,
            ttl_seconds=ttl_seconds,
            stale_grace_seconds=stale_grace_seconds,
            builder=build,
        )

    def cached_json(
        self,
        *,
        cache_key: str,
        upstream_url: str,
        ttl_seconds: float,
        cache_control: str,
        sanitizer: Callable[[Mapping[str, Any]], dict[str, Any]],
        stale_grace_seconds: float | None = None,
    ) -> PreparedResponse:
        def build() -> PreparedResponse:
            request = _OPENER.open(upstream_url, timeout=15.0)
            try:
                raw = request.read(MAX_UPSTREAM_BYTES + 1)
            finally:
                request.close()
            if len(raw) > MAX_UPSTREAM_BYTES:
                raise ValueError("upstream payload exceeds public size limit")
            payload = json.loads(raw)
            if not isinstance(payload, Mapping):
                raise ValueError("upstream JSON root is not an object")
            public_payload = sanitizer(payload)
            encoded = (
                json.dumps(
                    public_payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            response = _prepared(
                encoded,
                content_type="application/json; charset=utf-8",
                cache_control=cache_control,
            )
            return response

        return self._cached_response(
            cache_key=cache_key,
            ttl_seconds=ttl_seconds,
            stale_grace_seconds=stale_grace_seconds,
            builder=build,
        )

    def _revision_stale_or_build(
        self,
        *,
        cache_prefix: str,
        cache_key: str,
        revision_token: str,
        builder: Callable[[], PreparedResponse],
    ) -> PreparedResponse:
        """Serve the last verified revision while one replacement is built.

        Revision-bearing keys preserve immediate source invalidation, but a
        large immutable projection should not turn that invalidation into a
        blank page for every viewer.  The prior response is reused only for the
        exact query prefix and only inside its bounded stale window.
        """

        start_refresh = False
        fallback: PreparedResponse | None = None
        with self._cache_lock:
            if cache_key not in self._cache:
                observed = time.monotonic()
                candidates = [
                    entry
                    for key, entry in self._cache.items()
                    if key.startswith(cache_prefix) and entry.stale_until > observed
                ]
                if candidates:
                    latest = max(candidates, key=lambda entry: entry.last_accessed_at)
                    latest.last_accessed_at = observed
                    fallback = latest.response
                    if cache_key not in self._refreshing:
                        self._refreshing.add(cache_key)
                        start_refresh = True
        if fallback is None:
            return builder()

        self._cache_observation("revision_stale_hit")
        if start_refresh:

            def refresh_revision() -> None:
                try:
                    builder()
                except Exception as error:
                    sys.stderr.write(
                        "public-dashboard revision_refresh_failed "
                        f"key={cache_key} error={type(error).__name__}\n"
                    )
                finally:
                    with self._cache_lock:
                        self._refreshing.discard(cache_key)

            threading.Thread(
                target=refresh_revision,
                name=f"public-revision-{revision_token[:32]}",
                daemon=True,
            ).start()
        return fallback

    def tw_status(self, session_date: str | None = None) -> PreparedResponse:
        normalized_date = str(session_date or "").strip()
        revision = _response_json(self.tw_revision())
        revision_token = str(
            revision.get("revision_token")
            or revision.get("state_revision")
            or "missing"
        )
        # Price/valuation facts advance through the durable content revision.
        # Keep time out of the key: adding a wall-clock bucket can start a
        # duplicate hundreds-of-MiB history scan at a minute boundary.  Once a
        # verified response exists, stale-while-refresh lets the first reader
        # after TTL return immediately while one background thread refreshes
        # source ages.  A new signal/mark/pre-open revision still gets a new
        # key and starts a rebuild immediately; an existing timestamped,
        # verified response remains visible only during that bounded rebuild.
        display_session = (revision.get("session_clock") or {}).get(
            "display_session_date"
        )
        date_scope = normalized_date or (
            f"latest@{display_session}" if display_session else "latest"
        )
        cache_prefix = f"tw-status:{date_scope}:"
        cache_key = f"{cache_prefix}{revision_token}"

        def build_response() -> PreparedResponse:
            return self.cached_local_json(
                cache_key=cache_key,
                ttl_seconds=55.0,
                cache_control="no-store",
                stale_grace_seconds=120.0,
                builder=lambda: sanitize_tw_status(
                    build_dashboard_snapshot(
                        state_dir=self.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        preopen_readiness_path=self.repo_root
                        / "artifacts/discord_bot/preopen_readiness.json",
                        session_date=normalized_date or None,
                        maximum_event_rows=500,
                        maximum_mark_rows=32,
                        include_position_rows=False,
                        # Completed-session position snapshots and immutable
                        # benchmark history already enumerate the public date
                        # selector. Avoid indexing multi-GB append-only ledgers on
                        # a cold read merely to rediscover the same dates.
                        include_ledger_session_dates=False,
                    )
                ),
            )

        # An atomic history promotion can change the content revision and the
        # 358 MiB benchmark inode together.  Never use the fallback on the first
        # cold build, after its bounded stale window, or across dates.
        return self._revision_stale_or_build(
            cache_prefix=cache_prefix,
            cache_key=cache_key,
            revision_token=revision_token,
            builder=build_response,
        )

    def tw_public_data_status(self) -> PreparedResponse:
        """Return only official TW source progress used by the day-trade page."""

        return self.cached_local_json(
            cache_key=f"tw-public-data-status:{int(time.time() // 25)}",
            ttl_seconds=30.0,
            cache_control="no-store",
            stale_grace_seconds=60.0,
            builder=lambda: build_tw_public_monitor_status(self.repo_root),
        )

    def tw_revision(self) -> PreparedResponse:
        signature = tuple(file_signature(path) for path in self.update_hub.paths["tw"])
        session_date = dashboard_session_clock(datetime.now(UTC)).get(
            "display_session_date"
        )
        return self.cached_local_json(
            cache_key=f"tw-revision:{signature}:{session_date}",
            ttl_seconds=0.05,
            cache_control="no-store",
            stale_grace_seconds=0.0,
            builder=lambda: build_dashboard_revision(
                state_dir=self.repo_root / "artifacts/live/tw_day_trade_simulation",
                discord_service_status_path=self.repo_root
                / "artifacts/discord_bot/service_status.json",
            ),
        )

    def tw_history(
        self,
        range_key: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        resolution: str = "sampled",
        history_encoding: str = "minute_columns_v1",
    ) -> PreparedResponse:
        date_key = (
            f"{start_date or ''}:{end_date or ''}:{resolution}:{history_encoding}"
        )
        revision_token = self.content_token()
        cache_prefix = f"tw-history:{range_key}:{date_key}:"
        cache_key = f"{cache_prefix}{revision_token}"

        def build_response() -> PreparedResponse:
            return self.cached_local_json(
                cache_key=cache_key,
                ttl_seconds=55.0,
                cache_control="no-cache",
                stale_grace_seconds=HISTORY_STALE_GRACE_SECONDS,
                builder=lambda: sanitize_tw_history(
                    build_dashboard_history_snapshot(
                        state_dir=self.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        range_key=range_key,
                        start_date=start_date,
                        end_date=end_date,
                        resolution=resolution,
                        history_encoding=history_encoding,
                        # The gateway already retains the serialized and compressed
                        # response. Keeping the decoded graph too doubles the
                        # largest dashboard allocation without reducing latency.
                        use_memory_cache=False,
                    )
                ),
            )

        # History is revision-addressed and drives the visible equity curve.
        # Returning the prior revision here can leave a tab permanently one
        # commit behind: the SSE notification has already been consumed and a
        # completed background history build does not create a new engine
        # revision. Coalesce concurrent readers on the new key, but make the
        # first reader wait for that exact immutable revision.
        return build_response()

    def overnight_revision(self) -> PreparedResponse:
        signature = tuple(
            file_signature(path) for path in self.update_hub.paths["overnight"]
        )

        def build() -> Mapping[str, Any]:
            payload = build_dashboard_revision(
                state_dir=self.repo_root / "artifacts/live/tw_overnight_simulation",
                discord_service_status_path=self.repo_root
                / "artifacts/discord_bot/service_status.json",
                discord_markets_field="overnight_markets",
                discord_engine_revision_field="overnight_engine_state_revision",
            )
            history_revision = hashlib.sha256(
                repr(signature[4:]).encode("utf-8")
            ).hexdigest()[:16]
            payload["history_revision"] = history_revision
            payload["revision_token"] = (
                f"{payload.get('revision_token') or 'missing'}:{history_revision}"
            )
            return payload

        return self.cached_local_json(
            cache_key=f"overnight-revision:{signature}",
            ttl_seconds=0.05,
            cache_control="no-store",
            stale_grace_seconds=0.0,
            builder=build,
        )

    def overnight_status(self, session_date: str | None = None) -> PreparedResponse:
        normalized_date = str(session_date or "").strip()
        revision = _response_json(self.overnight_revision())
        revision_token = str(
            revision.get("revision_token")
            or revision.get("state_revision")
            or "missing"
        )

        def build() -> Mapping[str, Any]:
            payload = sanitize_tw_status(
                build_dashboard_snapshot(
                    state_dir=self.repo_root / "artifacts/live/tw_overnight_simulation",
                    discord_service_status_path=self.repo_root
                    / "artifacts/discord_bot/service_status.json",
                    session_date=normalized_date or None,
                    maximum_event_rows=500,
                    maximum_mark_rows=32,
                    include_position_rows=False,
                    include_ledger_session_dates=False,
                    discord_markets_field="overnight_markets",
                    discord_engine_revision_field="overnight_engine_state_revision",
                )
            )
            payload["product"] = "tw_overnight"
            payload["model_adapter_notice"] = (
                "13:20 target weights temporarily reuse the day-trade checkpoint; "
                "the model has not been trained for overnight risk"
            )
            history_path = (
                self.repo_root
                / "artifacts/live/tw_overnight_simulation/overnight_history.json"
            )
            try:
                history = json.loads(history_path.read_text())
            except (OSError, ValueError, json.JSONDecodeError):
                history = {}
            payload["historical_replay"] = {
                key: history.get(key)
                for key in (
                    "status",
                    "start_date",
                    "end_date",
                    "session_count",
                    "mark_count",
                    "signal_count",
                    "event_count",
                    "position_count",
                    "generated_at",
                    "missing_1325_count",
                    "close_fallback_count",
                    "valuation_stale_market_count",
                )
            }
            service_sync = dict(payload.get("service_sync") or {})
            service_sync["revision_token"] = revision_token
            service_sync["history_revision"] = revision.get("history_revision")
            payload["service_sync"] = service_sync
            payload["source_contract"] = {
                "switch": "the dashboard and executor roll to the current verified session at 13:00 Asia/Taipei, then wait",
                "signal": "calculation starts at 13:20 using the current quote and temporary day-trade model weights",
                "replay": "history before this live clock change retains prior-feature 13:25 targets, official close/open counterfactual prices, and a disclosed same-close fallback when 13:25 data are absent; it is not an exchange fill claim",
                "entry_fill": "legal-limit LMT_ROD is submitted after the 13:20 calculation and before 13:30; fill uses the actual close auction print",
                "fees": "ordinary cash-stock commission and ordinary stock or ETF transaction tax",
                "comparison": "history sizes each new cohort from pre-entry account equity; absolute equity remains cumulative and the selected-period percentage resets to zero",
                "benchmarks": "this adapter has no day-trade benchmark ledger",
                "benchmark_history": "the overnight curve contains two auction events per completed session and does not interpolate minute prices",
                "eligibility": "buy permission and ordinary next-day short-open inventory are evaluated separately from day-trade eligibility",
                "depth_limit": "full requested quantity is a paper-auction assumption; level-one data cannot prove queue allocation",
                "bracket_fill": "orders use same-session legal price limits and are settled only by an actual auction print",
                "terminal_flatten": "no same-day terminal flatten; a proven close fill must remain open overnight",
                "exit_schedule": "next-session legal-limit LMT_ROD from 08:30; fill uses the actual opening auction print at or after 09:00",
                "latency": "13:20 calculation start, signal publication, consumer discovery and ledger timestamps are recorded separately",
                "simtrade": "indicative matching is displayed as waiting and is never a fill",
                "queue": "paper quantity assumes full auction allocation and makes no exchange fill claim",
            }
            return payload

        return self.cached_local_json(
            cache_key=(
                f"overnight-status:{normalized_date or 'latest'}:{revision_token}"
            ),
            ttl_seconds=55.0,
            cache_control="no-store",
            stale_grace_seconds=120.0,
            builder=build,
        )

    def overnight_history(
        self,
        range_key: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        resolution: str = "sampled",
        history_encoding: str = "minute_columns_v1",
    ) -> PreparedResponse:
        date_key = (
            f"{start_date or ''}:{end_date or ''}:{resolution}:{history_encoding}"
        )
        return self.cached_local_json(
            cache_key=f"overnight-history:{range_key}:{date_key}:{self.content_token('overnight')}",
            ttl_seconds=55.0,
            cache_control="no-cache",
            stale_grace_seconds=HISTORY_STALE_GRACE_SECONDS,
            builder=lambda: sanitize_tw_history(
                build_dashboard_history_snapshot(
                    state_dir=self.repo_root / "artifacts/live/tw_overnight_simulation",
                    range_key=range_key,
                    start_date=start_date,
                    end_date=end_date,
                    resolution=resolution,
                    history_encoding=history_encoding,
                    use_memory_cache=False,
                )
            ),
        )

    def openbb_status(self) -> PreparedResponse:
        return self.cached_local_json(
            cache_key="openbb-status",
            ttl_seconds=8.0,
            cache_control="no-store",
            stale_grace_seconds=0.0,
            builder=lambda: build_openbb_public_status(self.repo_root),
        )

    def openbb_history(self, range_key: str) -> PreparedResponse:
        return self.cached_local_json(
            cache_key=f"openbb-history:{range_key}",
            ttl_seconds=55.0,
            cache_control="no-cache",
            stale_grace_seconds=HISTORY_STALE_GRACE_SECONDS,
            builder=lambda: build_openbb_public_history(self.repo_root, range_key),
        )

    def data_monitor_status(
        self,
        *,
        shioaji_status: Mapping[str, Any] | None = None,
        openbb_status: Mapping[str, Any] | None = None,
    ) -> PreparedResponse:
        snapshot_path = (
            self.repo_root / "artifacts/live/data_monitor/public_status.json"
        )
        try:
            stat = snapshot_path.stat()
        except FileNotFoundError:
            stat = None
        if stat is not None:
            signature = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
            )

            def read_snapshot() -> Mapping[str, Any]:
                payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
                if not isinstance(payload, Mapping):
                    raise ValueError("data-monitor public snapshot is not an object")
                if payload.get("read_only") is not True:
                    raise ValueError("data-monitor public snapshot is not read-only")
                if payload.get("production_control_possible") is not False:
                    raise ValueError(
                        "data-monitor public snapshot exposes production control"
                    )
                return payload

            return self.cached_local_json(
                cache_key=f"data-monitor-status-snapshot:{signature}",
                ttl_seconds=45.0,
                cache_control="no-store",
                stale_grace_seconds=60.0,
                builder=read_snapshot,
            )
        if shioaji_status is None:
            shioaji_status = _response_json(
                self.cached_local_json(
                    cache_key="shioaji-status",
                    ttl_seconds=8.0,
                    cache_control="no-store",
                    stale_grace_seconds=MONITOR_STATUS_STALE_GRACE_SECONDS,
                    builder=lambda: build_shioaji_public_status(self.repo_root),
                )
            )
        if openbb_status is None:
            openbb_status = _response_json(self.openbb_status())
        return self.cached_local_json(
            cache_key="data-monitor-status",
            ttl_seconds=8.0,
            cache_control="no-store",
            stale_grace_seconds=MONITOR_STATUS_STALE_GRACE_SECONDS,
            builder=lambda: build_data_monitor_public_status(
                self.repo_root,
                shioaji_status=shioaji_status,
                openbb_status=openbb_status,
            ),
        )

    def data_monitor_summary(self) -> PreparedResponse:
        def build() -> Mapping[str, Any]:
            payload = _response_json(self.data_monitor_status())
            return {
                key: payload[key]
                for key in (
                    "schema_version",
                    "generated_at_utc",
                    "health",
                    "read_only",
                    "production_control_possible",
                    "summary",
                    "endpoint_inventory",
                    "provider_summaries",
                    "integrity_checks",
                    "definitions",
                    # Physical groups are the overview immediately above the
                    # registry. They are small enough for first paint; the
                    # per-source records remain on the deferred detail route.
                    "groups",
                )
                if key in payload
            }

        return self.cached_local_json(
            cache_key="data-monitor-summary",
            ttl_seconds=8.0,
            cache_control="no-store",
            stale_grace_seconds=MONITOR_STATUS_STALE_GRACE_SECONDS,
            builder=build,
        )

    def data_monitor_details(self) -> PreparedResponse:
        """Return the deferred source registry without repeating the overview."""

        def build() -> Mapping[str, Any]:
            payload = _response_json(self.data_monitor_status())
            return {
                key: payload[key]
                for key in (
                    "schema_version",
                    "generated_at_utc",
                    "read_only",
                    "production_control_possible",
                    "sources",
                )
                if key in payload
            }

        return self.cached_local_json(
            cache_key="data-monitor-details",
            ttl_seconds=8.0,
            cache_control="no-store",
            stale_grace_seconds=MONITOR_STATUS_STALE_GRACE_SECONDS,
            builder=build,
        )

    def public_overview(self) -> Mapping[str, Any]:
        # These sources are independent.  Build their verified snapshots on
        # the critical path concurrently, then reuse Shioaji/OpenBB in the
        # dependent all-data projection instead of reading them twice.
        with ThreadPoolExecutor(
            max_workers=5,
            thread_name_prefix="public-overview",
        ) as executor:
            taifex_future = executor.submit(
                self.cached_json,
                cache_key="taifex-status",
                upstream_url=f"{self.taifex_upstream}/api/status",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                sanitizer=sanitize_taifex_status,
            )
            tw_future = executor.submit(
                self.cached_local_json,
                cache_key="tw-overview-status",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=OVERVIEW_STALE_GRACE_SECONDS,
                builder=lambda: build_compact_tw_overview_status(
                    self.repo_root / "artifacts/live/tw_day_trade_simulation"
                ),
            )
            overnight_future = executor.submit(
                self.cached_local_json,
                cache_key="overnight-overview-status",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=OVERVIEW_STALE_GRACE_SECONDS,
                builder=lambda: build_compact_tw_overview_status(
                    self.repo_root / "artifacts/live/tw_overnight_simulation",
                    opening_gate_path=self.repo_root
                    / "artifacts/live/tw_overnight_simulation/preopen_readiness.json",
                ),
            )
            shioaji_future = executor.submit(
                self.cached_local_json,
                cache_key="shioaji-status",
                ttl_seconds=8.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: build_shioaji_public_status(self.repo_root),
            )
            openbb_future = executor.submit(self.openbb_status)
            taifex = _response_json(taifex_future.result())
            tw = _response_json(tw_future.result())
            overnight = _response_json(overnight_future.result())
            shioaji = _response_json(shioaji_future.result())
            openbb = _response_json(openbb_future.result())
        data_monitor = _response_json(
            self.data_monitor_status(
                shioaji_status=shioaji,
                openbb_status=openbb,
            )
        )
        return build_public_overview(
            taifex,
            tw,
            shioaji,
            openbb,
            data_monitor,
            self.traffic_observer.snapshot(),
            overnight=overnight,
        )

    def prewarm_overview(self) -> None:
        try:
            self.cached_local_json(
                cache_key="public-overview",
                ttl_seconds=55.0,
                cache_control="no-store",
                stale_grace_seconds=OVERVIEW_STALE_GRACE_SECONDS,
                builder=self.public_overview,
            )
        except Exception as error:
            sys.stderr.write(
                f"public-dashboard prewarm_failed error={type(error).__name__}\n"
            )

    def prewarm_default_views(self) -> None:
        """Warm every first-view cache without delaying the listener."""

        def taifex_history_builder() -> PreparedResponse:
            return self.cached_json(
                cache_key="taifex-history:1d",
                upstream_url=f"{self.taifex_upstream}/api/history?range=1d",
                ttl_seconds=55.0,
                cache_control="no-cache",
                stale_grace_seconds=HISTORY_STALE_GRACE_SECONDS,
                sanitizer=sanitize_taifex_history,
            )

        # The landing page is the public readiness surface and aggregates the
        # monitor snapshots.  Warm it before the large history scans so a cold
        # gateway cannot make the first /api/overview request compete for disk
        # and exceed the reverse proxy's timeout.
        try:
            self.prewarm_overview()
        except Exception as error:
            sys.stderr.write(
                "public-dashboard critical_overview_prewarm_failed "
                f"error={type(error).__name__}\n"
            )
        # The detailed TW page still receives the complete source-backed
        # snapshot. Warm it after the compact landing card is ready, so its
        # historical joins cannot hold the public readiness surface hostage.
        current_tw_session_date: str | None = None
        try:
            warmed_tw_status = self.tw_status()
            current_tw_session_date = (
                str(_response_json(warmed_tw_status).get("session_date") or "") or None
            )
        except Exception as error:
            sys.stderr.write(
                "public-dashboard critical_tw_status_prewarm_failed "
                f"error={type(error).__name__}\n"
            )
        # The day-trade page asks for an explicit selected-date curve as soon
        # as status paints.  Warm that exact response before lower-priority
        # cross-dashboard histories so its request reuses the status-built
        # benchmark index and never queues behind an unrelated TAIFEX scan.
        if current_tw_session_date:
            try:
                self.tw_history(
                    "all",
                    start_date=current_tw_session_date,
                    end_date=current_tw_session_date,
                    resolution="1m",
                    history_encoding="minute_columns_v2",
                )
            except Exception as error:
                sys.stderr.write(
                    "public-dashboard critical_tw_history_prewarm_failed "
                    f"error={type(error).__name__}\n"
                )
        # Build the largest detail payload next.  An immediate request joins
        # this cache key's single flight, while the remaining detail views wait
        # instead of competing for CPU and disk bandwidth.
        try:
            taifex_history_builder()
        except Exception as error:
            sys.stderr.write(
                "public-dashboard critical_view_prewarm_failed "
                f"error={type(error).__name__}\n"
            )
        # Complete history is the largest optional TW view.  Build it on the
        # existing asynchronous startup worker after critical first-paint data
        # is ready, so a browser opening the full range does not become the
        # process that pays the one-time source reconstruction cost.
        try:
            self.tw_history(
                "all",
                resolution="1m",
                history_encoding="minute_columns_v2",
            )
        except Exception as error:
            sys.stderr.write(
                "public-dashboard full_history_prewarm_failed "
                f"error={type(error).__name__}\n"
            )
        # The overview already indexes the TW ledger dates. Prebuilding raw
        # position and event pages here used to decode and retain every ledger
        # row without storing a public response, inflating a fresh gateway to
        # multiple GiB. The default signal page now retains only its bounded
        # 100-row result, so it is safe and useful to warm the laptop's first
        # visible detail table after the compact date index exists.
        builders: tuple[Callable[[], object], ...] = (
            lambda: self.openbb_history("1d"),
            lambda: build_dashboard_signal_page(
                state_dir=self.repo_root / "artifacts/live/tw_day_trade_simulation",
                start_date=current_tw_session_date,
                end_date=current_tw_session_date,
                mode="all",
                status="all",
                limit=100,
            ),
        )
        with ThreadPoolExecutor(
            max_workers=len(builders),
            thread_name_prefix="public-prewarm",
        ) as executor:
            futures = [executor.submit(builder) for builder in builders]
            for future in futures:
                try:
                    future.result()
                except Exception as error:
                    sys.stderr.write(
                        "public-dashboard view_prewarm_failed "
                        f"error={type(error).__name__}\n"
                    )
        try:
            warm_dashboard_session_indexes(
                state_dir=self.repo_root / "artifacts/live/tw_day_trade_simulation"
            )
        except Exception as error:
            sys.stderr.write(
                "public-dashboard session_index_prewarm_failed "
                f"error={type(error).__name__}\n"
            )


class PublicDashboardHandler(BaseHTTPRequestHandler):
    server: PublicDashboardServer
    server_version = "StockAgentPublicGateway"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True

    def handle(self) -> None:
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            # A browser can cancel a keep-alive request while navigating or
            # refreshing.  The response writer already handles this case; the
            # request-line reader needs the same narrow treatment so normal
            # client disconnects do not become server tracebacks.
            return

    def _security_headers(self) -> None:
        if self.close_connection:
            self.send_header("Connection", "close")
        request_started_ns = getattr(self, "_request_started_ns", None)
        if isinstance(request_started_ns, int):
            elapsed_ms = (time.perf_counter_ns() - request_started_ns) / 1_000_000
            fields = [f"app;dur={elapsed_ms:.3f}"]
            metrics = getattr(self.server.request_metrics, "current", None)
            if metrics is not None:
                fields.extend(
                    (
                        f"cache_wait;dur={metrics['cache_wait_ms']:.3f}",
                        f"build;dur={metrics['build_ms']:.3f}",
                    )
                )
                if metrics["cache"]:
                    fields.append(
                        'cache;desc="' + "+".join(sorted(metrics["cache"])) + '"'
                    )
            self.send_header("Server-Timing", ", ".join(fields))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-XSS-Protection", "0")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Origin-Agent-Cluster", "?1")
        self.send_header("X-Permitted-Cross-Domain-Policies", "none")
        self.send_header(
            "Permissions-Policy",
            "accelerometer=(), autoplay=(), camera=(), display-capture=(), "
            "encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), "
            "magnetometer=(), microphone=(), payment=(), picture-in-picture=(), "
            "publickey-credentials-get=(), screen-wake-lock=(), usb=(), "
            "web-share=()",
        )
        self.send_header("X-Robots-Tag", "noindex, nofollow, noarchive")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "font-src 'self'; media-src 'none'; frame-src 'none'; "
            "worker-src 'none'; manifest-src 'none'; base-uri 'none'; "
            "form-action 'none'; frame-ancestors 'none'; "
            "script-src-attr 'none'; style-src-attr 'none'",
        )

    def _write_body(self, body: bytes) -> None:
        started = time.perf_counter()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Browser navigation and aborted fetches are normal client events.
            return
        finally:
            metrics = getattr(self.server.request_metrics, "current", None)
            if metrics is not None:
                metrics["write_ms"] += (time.perf_counter() - started) * 1000
        self._response_body_bytes += len(body)

    def send_response(self, code: int, message: str | None = None) -> None:
        self._response_status = int(code)
        super().send_response(code, message)

    def _send_prepared(
        self,
        status: HTTPStatus,
        response: PreparedResponse,
        *,
        head_only: bool,
    ) -> None:
        encoding = _preferred_encoding(
            ",".join(self.headers.get_all("Accept-Encoding", [])),
            gzip_available=response.gzip_body is not response.body,
        )
        if encoding is None:
            status = HTTPStatus.NOT_ACCEPTABLE
            response = _prepared(
                b'{"error":"no_acceptable_content_encoding"}\n',
                content_type="application/json; charset=utf-8",
                cache_control="no-store",
            )
        use_gzip = encoding == "gzip"
        body = response.gzip_body if use_gzip else response.body
        etag = response.gzip_etag if use_gzip else response.etag
        if status == HTTPStatus.OK and _etag_matches(
            self.headers.get("If-None-Match"), etag
        ):
            self.send_response(HTTPStatus.NOT_MODIFIED)
            self.send_header("ETag", etag)
            self.send_header("Cache-Control", response.cache_control)
            self.send_header("Vary", "Accept-Encoding")
            self._security_headers()
            self.end_headers()
            return
        self.send_response(status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", response.cache_control)
        self.send_header("ETag", etag)
        self.send_header("Vary", "Accept-Encoding")
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
        self._security_headers()
        self.end_headers()
        if not head_only:
            self._write_body(body)

    def _send_json(
        self,
        status: HTTPStatus,
        payload: Mapping[str, Any],
        *,
        head_only: bool,
        cache_control: str = "no-store",
    ) -> None:
        body = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        self._send_prepared(
            status,
            _prepared(
                body,
                content_type="application/json; charset=utf-8",
                cache_control=cache_control,
            ),
            head_only=head_only,
        )

    def _redirect(self, location: str, *, head_only: bool) -> None:
        body = b"redirecting\n"
        self.send_response(HTTPStatus.PERMANENT_REDIRECT)
        self.send_header("Location", location)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "public, max-age=300")
        self._security_headers()
        self.end_headers()
        if not head_only:
            self._write_body(body)

    def _static_response(self, path: str) -> PreparedResponse | None:
        routes: dict[str, tuple[Path, str, str]] = {
            "/": (
                self.server.public_static_root / "index.html",
                "text/html; charset=utf-8",
                "public, max-age=60",
            ),
            "/public.css": (
                self.server.public_static_root / "public.css",
                "text/css; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/public.js": (
                self.server.public_static_root / "public.js",
                "text/javascript; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/dashboard-core.css": (
                self.server.public_static_root / "dashboard-core.css",
                "text/css; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/dashboard-core.js": (
                self.server.public_static_root / "dashboard-core.js",
                "text/javascript; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/dashboard-responsive.css": (
                self.server.public_static_root / "dashboard-responsive.css",
                "text/css; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/time-axis.js": (
                self.server.public_static_root / "time-axis.js",
                "text/javascript; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/vendor/uplot/uPlot.iife.min.js": (
                self.server.public_static_root / "vendor/uplot/uPlot.iife.min.js",
                "text/javascript; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/vendor/uplot/uPlot.min.css": (
                self.server.public_static_root / "vendor/uplot/uPlot.min.css",
                "text/css; charset=utf-8",
                IMMUTABLE_ASSET_CACHE_CONTROL,
            ),
            "/robots.txt": (
                self.server.public_static_root / "robots.txt",
                "text/plain; charset=utf-8",
                "public, max-age=3600",
            ),
        }
        for prefix, root in (
            ("/taifex/", self.server.taifex_static_root),
            ("/tw-day-trade/", self.server.tw_static_root),
            ("/tw-overnight/", self.server.overnight_static_root),
            ("/shioaji/", self.server.shioaji_static_root),
            ("/openbb/", self.server.openbb_static_root),
            ("/data-monitor/", self.server.data_monitor_static_root),
            ("/traffic/", self.server.traffic_static_root),
        ):
            suffix = path.removeprefix(prefix) if path.startswith(prefix) else None
            if suffix in {"", "index.html"}:
                routes[path] = (
                    root / "index.html",
                    "text/html; charset=utf-8",
                    "public, max-age=60",
                )
            elif suffix == "app.js" or (
                prefix in {"/tw-day-trade/", "/tw-overnight/"}
                and suffix
                in {
                    "presentation.js",
                    "detail-components.js",
                    "chart-renderer.js",
                }
            ):
                routes[path] = (
                    root / suffix,
                    "text/javascript; charset=utf-8",
                    IMMUTABLE_ASSET_CACHE_CONTROL,
                )
            elif suffix == "styles.css" or (
                prefix == "/traffic/" and suffix == "performance.css"
            ):
                routes[path] = (
                    root / suffix,
                    "text/css; charset=utf-8",
                    IMMUTABLE_ASSET_CACHE_CONTROL,
                )
        selected = routes.get(path)
        if selected is None:
            return None
        target, content_type, cache_control = selected
        return self.server.cached_static(
            target,
            content_type=content_type,
            cache_control=cache_control,
        )

    @staticmethod
    def _date_query(raw_query: str) -> str | None:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=1,
            )
            if set(query) - {"date"} or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            value = str(query.get("date", [""])[0]).strip()
            if not value:
                return None
            date.fromisoformat(value)
            return value
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid date query") from error

    @staticmethod
    def _signal_query(raw_query: str) -> str:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=8,
            )
            allowed = {
                "date",
                "start_date",
                "end_date",
                "mode",
                "symbol",
                "status",
                "offset",
                "limit",
            }
            if set(query) - allowed or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            mode = str(query.get("mode", [""])[0])[:64]
            symbol = str(query.get("symbol", [""])[0])[:32]
            status = str(query.get("status", ["all"])[0])[:32]
            session_date = str(query.get("date", [""])[0]).strip()
            start_date = str(query.get("start_date", [""])[0]).strip()
            end_date = str(query.get("end_date", [""])[0]).strip()
            if session_date:
                date.fromisoformat(session_date)
            if start_date:
                date.fromisoformat(start_date)
            if end_date:
                date.fromisoformat(end_date)
            if start_date and end_date and start_date > end_date:
                raise InvalidPublicRequest("start_date must not be after end_date")
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0])
            if offset < 0 or offset > 100_000:
                raise InvalidPublicRequest("offset is outside the public range")
            if limit < 1:
                raise InvalidPublicRequest("limit must be positive")
            limit = min(limit, PUBLIC_SIGNAL_LIMIT)
            normalized = {"date": session_date}
            if start_date:
                normalized["start_date"] = start_date
            if end_date:
                normalized["end_date"] = end_date
            normalized.update(
                {
                    "mode": mode,
                    "symbol": symbol,
                    "status": status,
                    "offset": offset,
                    "limit": limit,
                }
            )
            return urlencode(normalized)
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid signal query") from error

    @staticmethod
    def _history_range_query(raw_query: str) -> str:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=1,
            )
            if set(query) - {"range"} or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            value = str(query.get("range", ["1d"])[0]).strip().lower() or "1d"
            if value not in {"1h", "1d", "1w", "1mo", "1q", "1y", "all"}:
                raise InvalidPublicRequest("unsupported chart range")
            return value
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid history query") from error

    @staticmethod
    def _traffic_history_query(raw_query: str) -> str:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=1,
            )
            if set(query) - {"range"} or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            value = str(query.get("range", ["24h"])[0]).strip().lower() or "24h"
            if value not in _TRAFFIC_HISTORY_RANGES:
                raise InvalidPublicRequest("unsupported traffic history range")
            return value
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid traffic history query") from error

    @staticmethod
    def _tw_history_query(raw_query: str) -> dict[str, str | None]:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=5,
            )
            if set(query) - {
                "range", "start_date", "end_date", "resolution", "encoding"
            } or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            range_key = str(query.get("range", ["1d"])[0]).strip().lower() or "1d"
            resolution = str(query.get("resolution", ["sampled"])[0])
            encoding_key = str(query.get("encoding", ["v1"])[0]).strip().lower()
            history_encoding = {
                "v1": "minute_columns_v1",
                "v2": "minute_columns_v2",
            }.get(encoding_key)
            if resolution not in {"sampled", "1m"}:
                raise InvalidPublicRequest("unsupported history resolution")
            if history_encoding is None:
                raise InvalidPublicRequest("unsupported history encoding")
            if resolution != "1m" and "encoding" in query:
                raise InvalidPublicRequest("history encoding requires 1m resolution")
            if range_key not in {"1h", "1d", "1w", "1mo", "1q", "1y", "all"}:
                raise InvalidPublicRequest("unsupported chart range")
            start_date = str(query.get("start_date", [""])[0]).strip() or None
            end_date = str(query.get("end_date", [""])[0]).strip() or None
            if start_date is not None:
                date.fromisoformat(start_date)
            if end_date is not None:
                date.fromisoformat(end_date)
            if (
                start_date is not None
                and end_date is not None
                and start_date > end_date
            ):
                raise InvalidPublicRequest(
                    "history start_date must not be after end_date"
                )
            return {
                "range_key": range_key,
                "start_date": start_date,
                "end_date": end_date,
                "resolution": resolution,
                "history_encoding": history_encoding,
            }
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid TW history query") from error

    @staticmethod
    def _event_query(raw_query: str) -> str:
        try:
            query = parse_qs(
                raw_query,
                keep_blank_values=True,
                strict_parsing=False,
                max_num_fields=7,
            )
            allowed = {
                "date",
                "start_date",
                "end_date",
                "mode",
                "symbol",
                "offset",
                "limit",
            }
            if set(query) - allowed or any(
                len(values) != 1 for values in query.values()
            ):
                raise InvalidPublicRequest("unsupported or repeated query field")
            mode = str(query.get("mode", [""])[0])[:64]
            symbol = str(query.get("symbol", [""])[0])[:32]
            session_date = str(query.get("date", [""])[0]).strip()
            start_date = str(query.get("start_date", [""])[0]).strip()
            end_date = str(query.get("end_date", [""])[0]).strip()
            if session_date:
                date.fromisoformat(session_date)
            if start_date:
                date.fromisoformat(start_date)
            if end_date:
                date.fromisoformat(end_date)
            if start_date and end_date and start_date > end_date:
                raise InvalidPublicRequest("start_date must not be after end_date")
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(PUBLIC_EVENT_LIMIT)])[0])
            if offset < 0 or offset > 100_000:
                raise InvalidPublicRequest("offset is outside the public range")
            if limit < 1:
                raise InvalidPublicRequest("limit must be positive")
            limit = min(limit, PUBLIC_EVENT_LIMIT)
            normalized = {"date": session_date}
            if start_date:
                normalized["start_date"] = start_date
            if end_date:
                normalized["end_date"] = end_date
            normalized.update(
                {
                    "mode": mode,
                    "symbol": symbol,
                    "offset": offset,
                    "limit": limit,
                }
            )
            return urlencode(normalized)
        except InvalidPublicRequest:
            raise
        except (ValueError, OverflowError) as error:
            raise InvalidPublicRequest("invalid event query") from error

    def _api_response(self, path: str, raw_query: str) -> PreparedResponse:
        if raw_query and path in _PUBLIC_API_ROUTES and path not in _QUERY_API_ROUTES:
            raise InvalidPublicRequest("endpoint does not accept query fields")
        if path == "/api/overview":
            return self.server.cached_local_json(
                cache_key="public-overview",
                ttl_seconds=55.0,
                cache_control="no-store",
                stale_grace_seconds=OVERVIEW_STALE_GRACE_SECONDS,
                builder=self.server.public_overview,
            )
        if path == "/taifex/api/status":
            return self.server.cached_json(
                cache_key="taifex-status",
                upstream_url=f"{self.server.taifex_upstream}/api/status",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                sanitizer=sanitize_taifex_status,
            )
        if path == "/taifex/api/history":
            range_key = self._history_range_query(raw_query)
            return self.server.cached_json(
                cache_key=f"taifex-history:{range_key}",
                upstream_url=(
                    f"{self.server.taifex_upstream}/api/history?"
                    f"{urlencode({'range': range_key})}"
                ),
                ttl_seconds=55.0,
                cache_control="no-cache",
                stale_grace_seconds=HISTORY_STALE_GRACE_SECONDS,
                sanitizer=sanitize_taifex_history,
            )
        if path == "/tw-day-trade/api/status":
            return self.server.tw_status(self._date_query(raw_query))
        if path == "/tw-day-trade/api/revision":
            if raw_query:
                raise InvalidPublicRequest("revision does not accept query fields")
            return self.server.tw_revision()
        if path == "/tw-day-trade/api/history":
            history_query = self._tw_history_query(raw_query)
            return self.server.tw_history(
                str(history_query["range_key"]),
                start_date=history_query["start_date"],
                end_date=history_query["end_date"],
                resolution=history_query["resolution"],
                history_encoding=history_query["history_encoding"],
            )
        if path == "/tw-day-trade/api/public-data-status":
            if raw_query:
                raise InvalidPublicRequest(
                    "public data status does not accept query fields"
                )
            return self.server.tw_public_data_status()
        if path == "/tw-day-trade/api/summary":
            session_date = self._date_query(raw_query)
            return self.server.cached_local_json(
                cache_key=f"tw-summary:{session_date or 'latest'}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: summarize_tw_status(
                    _response_json(self.server.tw_status(session_date))
                ),
            )
        if path == "/tw-day-trade/api/signals":
            normalized = self._signal_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            return self.server.cached_local_json(
                cache_key=f"tw-signals:{normalized}:{self.server.content_token()}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_signals(
                    build_dashboard_signal_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0]),
                    )
                ),
            )
        if path == "/tw-day-trade/api/positions":
            normalized = self._signal_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            return self.server.cached_local_json(
                cache_key=f"tw-positions:{normalized}:{self.server.content_token()}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_positions(
                    build_dashboard_position_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0]),
                    )
                ),
            )
        if path == "/tw-day-trade/api/events":
            normalized = self._event_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            session_date = str(query.get("date", [""])[0]) or None
            start_date = str(query.get("start_date", [""])[0]) or None
            end_date = str(query.get("end_date", [""])[0]) or None
            mode = str(query.get("mode", [""])[0])
            symbol = str(query.get("symbol", [""])[0])
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(PUBLIC_EVENT_LIMIT)])[0])
            return self.server.cached_local_json(
                cache_key=f"tw-events:{normalized}:{self.server.content_token()}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_events(
                    build_dashboard_event_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        session_date=session_date,
                        start_date=start_date,
                        end_date=end_date,
                        mode=mode,
                        symbol=symbol,
                        offset=offset,
                        limit=limit,
                    )
                ),
            )
        if path == "/tw-overnight/api/status":
            return self.server.overnight_status(self._date_query(raw_query))
        if path == "/tw-overnight/api/revision":
            if raw_query:
                raise InvalidPublicRequest("revision does not accept query fields")
            return self.server.overnight_revision()
        if path == "/tw-overnight/api/history":
            history_query = self._tw_history_query(raw_query)
            return self.server.overnight_history(
                str(history_query["range_key"]),
                start_date=history_query["start_date"],
                end_date=history_query["end_date"],
                resolution=history_query["resolution"],
                history_encoding=history_query["history_encoding"],
            )
        if path == "/tw-overnight/api/public-data-status":
            if raw_query:
                raise InvalidPublicRequest(
                    "public data status does not accept query fields"
                )
            return self.server.tw_public_data_status()
        if path == "/tw-overnight/api/summary":
            session_date = self._date_query(raw_query)
            return self.server.cached_local_json(
                cache_key=f"overnight-summary:{session_date or 'latest'}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: summarize_tw_status(
                    _response_json(self.server.overnight_status(session_date))
                ),
            )
        if path == "/tw-overnight/api/signals":
            normalized = self._signal_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            return self.server.cached_local_json(
                cache_key=f"overnight-signals:{normalized}:{self.server.content_token('overnight')}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_signals(
                    build_dashboard_signal_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_overnight_simulation",
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0]),
                    )
                ),
            )
        if path == "/tw-overnight/api/positions":
            normalized = self._signal_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            return self.server.cached_local_json(
                cache_key=f"overnight-positions:{normalized}:{self.server.content_token('overnight')}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_positions(
                    build_dashboard_position_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_overnight_simulation",
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0]),
                    )
                ),
            )
        if path == "/tw-overnight/api/events":
            normalized = self._event_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            return self.server.cached_local_json(
                cache_key=f"overnight-events:{normalized}:{self.server.content_token('overnight')}",
                ttl_seconds=2.0,
                cache_control="no-store",
                stale_grace_seconds=0.0,
                builder=lambda: sanitize_tw_events(
                    build_dashboard_event_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_overnight_simulation",
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", [str(PUBLIC_EVENT_LIMIT)])[0]),
                    )
                ),
            )
        if path == "/shioaji/api/status":
            return self.server.cached_local_json(
                cache_key="shioaji-status",
                ttl_seconds=8.0,
                cache_control="no-store",
                stale_grace_seconds=MONITOR_STATUS_STALE_GRACE_SECONDS,
                builder=lambda: build_shioaji_public_status(self.server.repo_root),
            )
        if path == "/openbb/api/status":
            return self.server.openbb_status()
        if path == "/openbb/api/history":
            return self.server.openbb_history(self._history_range_query(raw_query))
        if path == "/data-monitor/api/status":
            return self.server.data_monitor_status()
        if path == "/data-monitor/api/summary":
            return self.server.data_monitor_summary()
        if path == "/data-monitor/api/details":
            return self.server.data_monitor_details()
        if path == "/traffic/api/status":
            payload = self.server.traffic_observer.snapshot(
                exclude_current_request=True
            )
            payload["cache"].update(self.server.cache_residency())
            payload["definitions"]["cache_resident_bytes"] = (
                "程序內 JSON 回應快取目前保留的原始與 gzip body；不含靜態資源、"
                "Python 物件與作業系統頁面快取。"
            )
            payload["definitions"]["cache_capacity"] = (
                "JSON 回應快取同時受筆數與 body bytes 約束；先到任一上限即按"
                "最近最少使用順序淘汰。"
            )
            body = (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            return _prepared(
                body,
                content_type="application/json; charset=utf-8",
                cache_control="no-store",
            )
        if path == "/traffic/api/history":
            range_key = self._traffic_history_query(raw_query)
            return self.server.cached_local_json(
                cache_key=f"traffic-history:{range_key}",
                ttl_seconds=55.0,
                cache_control="no-store",
                stale_grace_seconds=120.0,
                builder=lambda: self.server.traffic_observer.history_snapshot(
                    range_key
                ),
            )
        raise PublicRouteNotFound(path)

    def _stream_updates(self, topic: str, *, head_only: bool) -> None:
        # SSE uses identity with immediate flush, not a buffered gzip response.
        if (
            _preferred_encoding(
                ",".join(self.headers.get_all("Accept-Encoding", [])),
                gzip_available=False,
            )
            is None
        ):
            self._send_json(
                HTTPStatus.NOT_ACCEPTABLE,
                {"error": "no_acceptable_content_encoding"},
                head_only=head_only,
            )
            return
        hub = self.server.update_hub
        if not head_only and not hub.acquire(topic):
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "update_stream_capacity"},
                head_only=False,
            )
            return
        self.close_connection = True
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-transform")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Vary", "Accept-Encoding")
            self._security_headers()
            self.end_headers()
            if head_only:
                return
            self.connection.settimeout(5.0)  # slow consumers cannot pin writers
            version = -1
            while not hub.closed:
                current = hub.wait(topic, version)
                if hub.closed:
                    break
                if current != version:
                    revision = (
                        self.server.tw_revision()
                        if topic == "tw"
                        else self.server.overnight_revision()
                    )
                    payload = _response_json(revision)
                    payload["view_generation"] = current
                    frame = (
                        b"event: revision\ndata: "
                        + json.dumps(
                            payload, ensure_ascii=False, separators=(",", ":")
                        ).encode()
                        + b"\n\n"
                    )
                    version = current
                else:
                    frame = b": heartbeat\n\n"
                self.wfile.write(frame)
                self.wfile.flush()
                if not hasattr(self, "_stream_first_frame_ms"):
                    self._stream_first_frame_ms = (
                        time.perf_counter_ns() - self._request_started_ns
                    ) / 1_000_000
                self._response_body_bytes += len(frame)
        except (OSError, ValueError):
            # Disconnects/timeouts and transient invalid source receipts close
            # the stream; the browser reconnects and keeps its polling fallback.
            pass
        finally:
            if not head_only:
                hub.release()

    def _handle(self, *, head_only: bool) -> None:
        # Public GET/HEAD routes have no request-body contract. Reject bodies
        # and ambiguous framing without consuming or reinterpreting their bytes.
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get("Transfer-Encoding") is not None or (
            lengths
            and (len(lengths) != 1 or re.fullmatch(r"0+", lengths[0].strip()) is None)
        ):
            self.close_connection = True
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "request_body_not_supported"},
                head_only=head_only,
            )
            return
        if len(self.path.encode("utf-8", errors="ignore")) > MAX_REQUEST_TARGET_BYTES:
            self._send_json(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                {"error": "request_target_too_long"},
                head_only=head_only,
            )
            return
        try:
            parsed = urlparse(self.path)
        except ValueError:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"error": "invalid_request"},
                head_only=head_only,
            )
            return
        path = parsed.path
        if path in {
            "/taifex",
            "/tw-day-trade",
            "/tw-overnight",
            "/shioaji",
            "/openbb",
            "/data-monitor",
            "/traffic",
        }:
            self._redirect(f"{path}/", head_only=head_only)
            return
        if path == "/healthz":
            self._send_json(
                HTTPStatus.OK,
                {"health": "ok"},
                head_only=head_only,
                cache_control="no-store",
            )
            return
        if path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.send_header("Cache-Control", "public, max-age=86400")
            self._security_headers()
            self.end_headers()
            return

        is_api = "/api/" in path
        if path in {"/tw-day-trade/api/updates", "/tw-overnight/api/updates"}:
            if parsed.query:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_request"},
                    head_only=head_only,
                )
            else:
                self._stream_updates(
                    "tw" if path.startswith("/tw-day-trade/") else "overnight",
                    head_only=head_only,
                )
            return
        if is_api:
            try:
                response = self._api_response(path, parsed.query)
            except PublicRouteNotFound:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": "not_found"},
                    head_only=head_only,
                )
                return
            except InvalidPublicRequest:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_request"},
                    head_only=head_only,
                )
                return
            except Exception as error:
                # Public payload builders must fail closed.  Log only the
                # exception class so paths, records, and upstream text never
                # become an accidental public-service side channel.
                sys.stderr.write(
                    "public-dashboard api_failure "
                    f"path={path} error={type(error).__name__}\n"
                )
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": "temporarily_unavailable"},
                    head_only=head_only,
                )
                return
            self._send_prepared(HTTPStatus.OK, response, head_only=head_only)
            return

        try:
            response = self._static_response(path)
        except OSError:
            response = None
        if response is None:
            self._send_json(
                HTTPStatus.NOT_FOUND,
                {"error": "not_found"},
                head_only=head_only,
            )
            return
        self._send_prepared(HTTPStatus.OK, response, head_only=head_only)

    def _observe_request(self, callback: Callable[[], None]) -> None:
        try:
            path = urlparse(self.path).path
        except ValueError:
            path = (
                "<invalid>"  # _handle returns a sanitized 400; keep telemetry bounded.
            )
        self._request_started_ns = time.perf_counter_ns()
        self.server.request_metrics.current = {
            "cache": set(),
            "cache_wait_ms": 0.0,
            "build_ms": 0.0,
            "write_ms": 0.0,
            "depth": 0,
        }
        self.__dict__.pop("_stream_first_frame_ms", None)
        self._response_status = int(HTTPStatus.INTERNAL_SERVER_ERROR)
        self._response_body_bytes = 0
        observed = self.server.traffic_observer.request_started(path)
        try:
            callback()
        finally:
            # An SSE connection's lifetime is not an HTTP response latency.
            elapsed_ms = getattr(self, "_stream_first_frame_ms", None)
            if elapsed_ms is None:
                elapsed_ms = (
                    time.perf_counter_ns() - self._request_started_ns
                ) / 1_000_000
            metrics = getattr(self.server.request_metrics, "current", None) or {}
            cache_wait_ms = max(0.0, float(metrics.get("cache_wait_ms") or 0.0))
            build_ms = max(0.0, float(metrics.get("build_ms") or 0.0))
            write_ms = max(0.0, float(metrics.get("write_ms") or 0.0))
            phase_durations_ms = {
                "cache_wait": cache_wait_ms,
                "build": build_ms,
                "write": write_ms,
                "other": max(
                    0.0,
                    float(elapsed_ms) - cache_wait_ms - build_ms - write_ms,
                ),
            }
            self.server.request_metrics.current = None
            self.server.traffic_observer.request_finished(
                observed=observed,
                path=path,
                status=self._response_status,
                latency_ms=elapsed_ms,
                response_body_bytes=self._response_body_bytes,
                phase_durations_ms=phase_durations_ms,
            )

    def do_GET(self) -> None:  # noqa: N802
        self._observe_request(lambda: self._handle(head_only=False))

    def do_HEAD(self) -> None:  # noqa: N802
        self._observe_request(lambda: self._handle(head_only=True))

    def _method_not_allowed(self) -> None:
        # Do not parse an unread POST/chunked body as a new keep-alive request.
        self.close_connection = True
        body = b'{"error":"method_not_allowed"}\n'
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._security_headers()
        self.end_headers()
        self._write_body(body)

    def _observe_method_not_allowed(self) -> None:
        self._observe_request(self._method_not_allowed)

    do_POST = _observe_method_not_allowed
    do_PUT = _observe_method_not_allowed
    do_PATCH = _observe_method_not_allowed
    do_DELETE = _observe_method_not_allowed
    do_OPTIONS = _observe_method_not_allowed
    do_TRACE = _observe_method_not_allowed

    def log_message(self, format: str, *args: object) -> None:
        # Caddy owns the durable access log.  Avoid a second synchronous log
        # write on the latency-critical Python response path.
        return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument(
        "--public-static-root",
        type=Path,
        default=Path("services/public_dashboards"),
    )
    parser.add_argument(
        "--taifex-static-root", type=Path, default=Path("services/taifex_dashboard")
    )
    parser.add_argument(
        "--tw-static-root",
        type=Path,
        default=Path("services/tw_day_trade_dashboard"),
    )
    parser.add_argument(
        "--overnight-static-root",
        type=Path,
        default=Path("services/tw_day_trade_dashboard"),
    )
    parser.add_argument(
        "--shioaji-static-root",
        type=Path,
        default=Path("services/shioaji_api_dashboard"),
    )
    parser.add_argument(
        "--openbb-static-root",
        type=Path,
        default=Path("services/openbb_archive_dashboard"),
    )
    parser.add_argument(
        "--data-monitor-static-root",
        type=Path,
        default=Path("services/data_monitor_dashboard"),
    )
    parser.add_argument(
        "--traffic-static-root",
        type=Path,
        default=Path("services/traffic_dashboard"),
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--taifex-upstream", default="http://127.0.0.1:8765")
    parser.add_argument("--tw-upstream", default="http://127.0.0.1:8766")
    parser.add_argument(
        "--traffic-history-root",
        type=Path,
        default=Path(
            os.environ.get(
                "STOCKAGENT_DASHBOARD_PERFORMANCE_DIR",
                "/var/lib/stockagent-public-dashboards/performance",
            )
        ),
        help="private append-only directory for anonymous minute timing aggregates",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= int(args.port) <= 65_535:
        raise ValueError("port must be between 1 and 65535")
    server = PublicDashboardServer(
        (str(args.host), int(args.port)),
        public_static_root=Path(args.public_static_root),
        taifex_static_root=Path(args.taifex_static_root),
        tw_static_root=Path(args.tw_static_root),
        overnight_static_root=Path(args.overnight_static_root),
        shioaji_static_root=Path(args.shioaji_static_root),
        openbb_static_root=Path(args.openbb_static_root),
        data_monitor_static_root=Path(args.data_monitor_static_root),
        traffic_static_root=Path(args.traffic_static_root),
        repo_root=Path(args.repo_root),
        taifex_upstream=str(args.taifex_upstream),
        tw_upstream=str(args.tw_upstream),
        traffic_history_root=Path(args.traffic_history_root),
    )
    threading.Thread(
        target=server.prewarm_default_views,
        name="public-default-views-prewarm",
        daemon=True,
    ).start()
    print(
        f"[public-dashboards] listening=http://{args.host}:{args.port} "
        "upstreams=localhost-only",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
