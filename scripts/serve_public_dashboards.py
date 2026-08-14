#!/usr/bin/env python3
"""Serve sanitized, cached public views of both localhost dashboards."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime
import gzip
import hashlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable, Final, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import ProxyHandler, build_opener

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.public_dashboards import (  # noqa: E402
    TokenBucketRateLimiter,
    UnsafePublicDashboardPayload,
    sanitize_taifex_history,
    sanitize_taifex_status,
    sanitize_tw_events,
    sanitize_tw_signals,
    sanitize_tw_status,
)
from stockagent.live.shioaji_api_dashboard import (  # noqa: E402
    build_shioaji_public_status,
)
from stockagent.live.tw_day_trade_dashboard import (  # noqa: E402
    build_dashboard_event_page,
    build_dashboard_snapshot,
)


MAX_UPSTREAM_BYTES: Final[int] = 8 * 1024 * 1024
MAX_REQUEST_TARGET_BYTES: Final[int] = 2_048
PUBLIC_SIGNAL_LIMIT: Final[int] = 250
PUBLIC_EVENT_LIMIT: Final[int] = 250
API_CONCURRENCY: Final[int] = 32
MAX_CACHE_ENTRIES: Final[int] = 512
_OPENER = build_opener(ProxyHandler({}))


@dataclass(frozen=True)
class PreparedResponse:
    body: bytes
    gzip_body: bytes
    content_type: str
    etag: str
    cache_control: str


@dataclass
class CacheEntry:
    expires_at: float
    stale_until: float
    response: PreparedResponse
    last_accessed_at: float


def _prepared(
    body: bytes,
    *,
    content_type: str,
    cache_control: str,
) -> PreparedResponse:
    digest = hashlib.sha256(body).hexdigest()
    compressed = gzip.compress(body, compresslevel=5) if len(body) >= 1_024 else body
    return PreparedResponse(
        body=body,
        gzip_body=compressed,
        content_type=content_type,
        etag=f'"sha256-{digest}"',
        cache_control=cache_control,
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
    if not isinstance(positions, list):
        return open_count, stale_count
    for row in positions:
        if not isinstance(row, Mapping) or not row.get("signed_shares"):
            continue
        open_count += 1
        if row.get("valuation_stale"):
            stale_count += 1
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


def build_public_overview(
    taifex: Mapping[str, Any],
    tw: Mapping[str, Any],
    shioaji: Mapping[str, Any],
) -> dict[str, Any]:
    """Return only the fields required by the public landing cards."""

    tw_open, _ = _open_position_summary(tw)
    taifex_strategies = taifex.get("strategies")
    tw_modes = tw.get("modes")
    traffic = shioaji.get("traffic")
    backfill = shioaji.get("backfill")
    pipeline_summary = shioaji.get("pipeline_summary")
    market = taifex.get("market")
    strategy_counts = taifex.get("strategy_counts")
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
        "shioaji": {
            "health": shioaji.get("health"),
            "source_age_seconds": shioaji.get("source_age_seconds"),
            "traffic_used_ratio": (
                traffic.get("used_ratio") if isinstance(traffic, Mapping) else None
            ),
            "safe_remaining_bytes": (
                traffic.get("safe_remaining_bytes")
                if isinstance(traffic, Mapping)
                else None
            ),
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
    }


class PublicDashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        public_static_root: Path,
        taifex_static_root: Path,
        tw_static_root: Path,
        shioaji_static_root: Path,
        repo_root: Path,
        taifex_upstream: str,
        tw_upstream: str,
    ) -> None:
        super().__init__(address, PublicDashboardHandler)
        self.public_static_root = Path(public_static_root)
        self.taifex_static_root = Path(taifex_static_root)
        self.tw_static_root = Path(tw_static_root)
        self.shioaji_static_root = Path(shioaji_static_root)
        self.repo_root = Path(repo_root)
        self.taifex_upstream = str(taifex_upstream).rstrip("/")
        self.tw_upstream = str(tw_upstream).rstrip("/")
        self.rate_limiter = TokenBucketRateLimiter()
        self.api_slots = threading.BoundedSemaphore(API_CONCURRENCY)
        self._cache: dict[str, CacheEntry] = {}
        self._cache_lock = threading.Lock()
        self._cache_key_locks: dict[str, threading.Lock] = {}
        self._refreshing: set[str] = set()

    def _store_cached_response(
        self,
        cache_key: str,
        response: PreparedResponse,
        ttl_seconds: float,
    ) -> None:
        observed = time.monotonic()
        stale_grace_seconds = max(15.0, float(ttl_seconds) * 3.0)
        with self._cache_lock:
            self._cache[cache_key] = CacheEntry(
                expires_at=observed + float(ttl_seconds),
                stale_until=observed + float(ttl_seconds) + stale_grace_seconds,
                response=response,
                last_accessed_at=observed,
            )
            if len(self._cache) <= MAX_CACHE_ENTRIES:
                return
            removable = sorted(
                (
                    (entry.last_accessed_at, key)
                    for key, entry in self._cache.items()
                    if key != cache_key
                    and key not in self._refreshing
                    and not self._cache_key_locks.get(key, threading.Lock()).locked()
                )
            )
            for _, key in removable[: len(self._cache) - MAX_CACHE_ENTRIES]:
                self._cache.pop(key, None)
                self._cache_key_locks.pop(key, None)

    def _background_refresh(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        builder: Callable[[], PreparedResponse],
        key_lock: threading.Lock,
    ) -> None:
        try:
            with key_lock:
                response = builder()
                self._store_cached_response(cache_key, response, ttl_seconds)
        except Exception as error:  # keep the last verified response on refresh errors
            with self._cache_lock:
                cached = self._cache.get(cache_key)
                if cached is not None:
                    cached.expires_at = min(
                        cached.stale_until,
                        time.monotonic()
                        + min(2.0, max(0.5, float(ttl_seconds))),
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
    ) -> PreparedResponse:
        """Per-key single-flight cache with stale-while-refresh behavior."""

        start_background = False
        with self._cache_lock:
            observed = time.monotonic()
            cached = self._cache.get(cache_key)
            if cached is not None:
                cached.last_accessed_at = observed
                if cached.expires_at > observed:
                    return cached.response
                key_lock = self._cache_key_locks.setdefault(
                    cache_key, threading.Lock()
                )
                if cached.stale_until > observed:
                    if cache_key not in self._refreshing:
                        self._refreshing.add(cache_key)
                        start_background = True
                    stale_response = cached.response
                else:
                    stale_response = None
            else:
                key_lock = self._cache_key_locks.setdefault(
                    cache_key, threading.Lock()
                )
                stale_response = None

        if stale_response is not None:
            if start_background:
                threading.Thread(
                    target=self._background_refresh,
                    kwargs={
                        "cache_key": cache_key,
                        "ttl_seconds": ttl_seconds,
                        "builder": builder,
                        "key_lock": key_lock,
                    },
                    name=f"public-cache-{cache_key[:48]}",
                    daemon=True,
                ).start()
            return stale_response

        with key_lock:
            with self._cache_lock:
                observed = time.monotonic()
                cached = self._cache.get(cache_key)
                if cached is not None and cached.expires_at > observed:
                    cached.last_accessed_at = observed
                    return cached.response
            response = builder()
            self._store_cached_response(cache_key, response, ttl_seconds)
            return response

    def cached_local_json(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        cache_control: str,
        builder: Callable[[], Mapping[str, Any]],
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
            builder=build,
        )

    def tw_status(self, session_date: str | None = None) -> PreparedResponse:
        normalized_date = str(session_date or "").strip()
        return self.cached_local_json(
            cache_key=f"tw-status:{normalized_date or 'latest'}",
            ttl_seconds=2.0,
            cache_control="public, max-age=2, stale-while-revalidate=8",
            builder=lambda: sanitize_tw_status(
                build_dashboard_snapshot(
                    state_dir=self.repo_root
                    / "artifacts/live/tw_day_trade_simulation",
                    preopen_readiness_path=self.repo_root
                    / "artifacts/discord_bot/preopen_readiness.json",
                    session_date=normalized_date or None,
                )
            ),
        )

    def public_overview(self) -> Mapping[str, Any]:
        taifex = _response_json(
            self.cached_json(
                cache_key="taifex-status",
                upstream_url=f"{self.taifex_upstream}/api/status",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                sanitizer=sanitize_taifex_status,
            )
        )
        tw = _response_json(self.tw_status())
        shioaji = _response_json(
            self.cached_local_json(
                cache_key="shioaji-status",
                ttl_seconds=8.0,
                cache_control="public, max-age=5, stale-while-revalidate=15",
                builder=lambda: build_shioaji_public_status(self.repo_root),
            )
        )
        return build_public_overview(taifex, tw, shioaji)


class PublicDashboardHandler(BaseHTTPRequestHandler):
    server: PublicDashboardServer
    server_version = "StockAgentPublicGateway"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def _security_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header(
            "Permissions-Policy",
            "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
        )
        self.send_header("X-Robots-Tag", "noindex, nofollow, noarchive")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        )

    def _client_key(self) -> str:
        peer = str(self.client_address[0])
        try:
            peer_address = ipaddress.ip_address(peer)
        except ValueError:
            return peer
        if not peer_address.is_loopback:
            return peer
        forwarded = str(self.headers.get("X-Forwarded-For") or "").split(",", 1)[0]
        try:
            return str(ipaddress.ip_address(forwarded.strip()))
        except ValueError:
            return peer

    def _send_prepared(
        self,
        status: HTTPStatus,
        response: PreparedResponse,
        *,
        head_only: bool,
    ) -> None:
        if self.headers.get("If-None-Match") == response.etag:
            self.send_response(HTTPStatus.NOT_MODIFIED)
            self.send_header("ETag", response.etag)
            self.send_header("Cache-Control", response.cache_control)
            self.send_header("Content-Length", "0")
            self._security_headers()
            self.end_headers()
            return
        accepts_gzip = "gzip" in str(self.headers.get("Accept-Encoding") or "").lower()
        use_gzip = accepts_gzip and response.gzip_body is not response.body
        body = response.gzip_body if use_gzip else response.body
        self.send_response(status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", response.cache_control)
        self.send_header("ETag", response.etag)
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
        self._security_headers()
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

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
            self.wfile.write(body)

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
                "public, max-age=300",
            ),
            "/public.js": (
                self.server.public_static_root / "public.js",
                "text/javascript; charset=utf-8",
                "public, max-age=300",
            ),
            "/dashboard-core.css": (
                self.server.public_static_root / "dashboard-core.css",
                "text/css; charset=utf-8",
                "public, max-age=300",
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
            ("/shioaji/", self.server.shioaji_static_root),
        ):
            suffix = path.removeprefix(prefix) if path.startswith(prefix) else None
            if suffix in {"", "index.html"}:
                routes[path] = (
                    root / "index.html",
                    "text/html; charset=utf-8",
                    "public, max-age=60",
                )
            elif suffix == "app.js":
                routes[path] = (
                    root / "app.js",
                    "text/javascript; charset=utf-8",
                    "public, max-age=300",
                )
            elif suffix == "styles.css":
                routes[path] = (
                    root / "styles.css",
                    "text/css; charset=utf-8",
                    "public, max-age=300",
                )
        selected = routes.get(path)
        if selected is None:
            return None
        target, content_type, cache_control = selected
        return _prepared(
            target.read_bytes(),
            content_type=content_type,
            cache_control=cache_control,
        )

    @staticmethod
    def _date_query(raw_query: str) -> str | None:
        query = parse_qs(
            raw_query,
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=1,
        )
        if set(query) - {"date"} or any(
            len(values) != 1 for values in query.values()
        ):
            raise ValueError("unsupported or repeated query field")
        value = str(query.get("date", [""])[0]).strip()
        if not value:
            return None
        date.fromisoformat(value)
        return value

    @staticmethod
    def _signal_query(raw_query: str) -> str:
        query = parse_qs(
            raw_query,
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=6,
        )
        allowed = {"date", "mode", "symbol", "status", "offset", "limit"}
        if set(query) - allowed or any(len(values) != 1 for values in query.values()):
            raise ValueError("unsupported or repeated query field")
        mode = str(query.get("mode", [""])[0])[:64]
        symbol = str(query.get("symbol", [""])[0])[:32]
        status = str(query.get("status", ["all"])[0])[:32]
        session_date = str(query.get("date", [""])[0]).strip()
        if session_date:
            date.fromisoformat(session_date)
        offset = int(query.get("offset", ["0"])[0])
        limit = int(query.get("limit", [str(PUBLIC_SIGNAL_LIMIT)])[0])
        if offset < 0 or offset > 100_000:
            raise ValueError("offset is outside the public range")
        if limit < 1:
            raise ValueError("limit must be positive")
        limit = min(limit, PUBLIC_SIGNAL_LIMIT)
        return urlencode(
            {
                "date": session_date,
                "mode": mode,
                "symbol": symbol,
                "status": status,
                "offset": offset,
                "limit": limit,
            }
        )

    @staticmethod
    def _event_query(raw_query: str) -> str:
        query = parse_qs(
            raw_query,
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=5,
        )
        allowed = {"date", "mode", "symbol", "offset", "limit"}
        if set(query) - allowed or any(len(values) != 1 for values in query.values()):
            raise ValueError("unsupported or repeated query field")
        mode = str(query.get("mode", [""])[0])[:64]
        symbol = str(query.get("symbol", [""])[0])[:32]
        session_date = str(query.get("date", [""])[0]).strip()
        if session_date:
            date.fromisoformat(session_date)
        offset = int(query.get("offset", ["0"])[0])
        limit = int(query.get("limit", [str(PUBLIC_EVENT_LIMIT)])[0])
        if offset < 0 or offset > 100_000:
            raise ValueError("offset is outside the public range")
        if limit < 1:
            raise ValueError("limit must be positive")
        limit = min(limit, PUBLIC_EVENT_LIMIT)
        return urlencode(
            {
                "date": session_date,
                "mode": mode,
                "symbol": symbol,
                "offset": offset,
                "limit": limit,
            }
        )

    def _api_response(self, path: str, raw_query: str) -> PreparedResponse:
        if path == "/api/overview":
            return self.server.cached_local_json(
                cache_key="public-overview",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                builder=self.server.public_overview,
            )
        if path == "/taifex/api/status":
            return self.server.cached_json(
                cache_key="taifex-status",
                upstream_url=f"{self.server.taifex_upstream}/api/status",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                sanitizer=sanitize_taifex_status,
            )
        if path == "/taifex/api/history":
            return self.server.cached_json(
                cache_key="taifex-history",
                upstream_url=f"{self.server.taifex_upstream}/api/history",
                ttl_seconds=55.0,
                cache_control="public, max-age=30, stale-while-revalidate=30",
                sanitizer=sanitize_taifex_history,
            )
        if path == "/tw-day-trade/api/status":
            return self.server.tw_status(self._date_query(raw_query))
        if path == "/tw-day-trade/api/summary":
            session_date = self._date_query(raw_query)
            return self.server.cached_local_json(
                cache_key=f"tw-summary:{session_date or 'latest'}",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                builder=lambda: summarize_tw_status(
                    _response_json(self.server.tw_status(session_date))
                ),
            )
        if path == "/tw-day-trade/api/signals":
            normalized = self._signal_query(raw_query)
            return self.server.cached_json(
                cache_key=f"tw-signals:{normalized}",
                upstream_url=f"{self.server.tw_upstream}/api/signals?{normalized}",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                sanitizer=sanitize_tw_signals,
            )
        if path == "/tw-day-trade/api/events":
            normalized = self._event_query(raw_query)
            query = parse_qs(normalized, keep_blank_values=True)
            session_date = str(query.get("date", [""])[0]) or None
            mode = str(query.get("mode", [""])[0])
            symbol = str(query.get("symbol", [""])[0])
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(PUBLIC_EVENT_LIMIT)])[0])
            return self.server.cached_local_json(
                cache_key=f"tw-events:{normalized}",
                ttl_seconds=2.0,
                cache_control="public, max-age=2, stale-while-revalidate=8",
                builder=lambda: sanitize_tw_events(
                    build_dashboard_event_page(
                        state_dir=self.server.repo_root
                        / "artifacts/live/tw_day_trade_simulation",
                        session_date=session_date,
                        mode=mode,
                        symbol=symbol,
                        offset=offset,
                        limit=limit,
                    )
                ),
            )
        if path == "/shioaji/api/status":
            return self.server.cached_local_json(
                cache_key="shioaji-status",
                ttl_seconds=8.0,
                cache_control="public, max-age=5, stale-while-revalidate=15",
                builder=lambda: build_shioaji_public_status(self.server.repo_root),
            )
        raise KeyError(path)

    def _handle(self, *, head_only: bool) -> None:
        if len(self.path.encode("utf-8", errors="ignore")) > MAX_REQUEST_TARGET_BYTES:
            self._send_json(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                {"error": "request_target_too_long"},
                head_only=head_only,
            )
            return
        parsed = urlparse(self.path)
        path = parsed.path
        if path in {"/taifex", "/tw-day-trade", "/shioaji"}:
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
        if is_api and not self.server.rate_limiter.allow(
            self._client_key(), cost=5.0 if path.endswith("/api/history") else 1.0
        ):
            self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
            self.send_header("Retry-After", "5")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            body = b'{"error":"rate_limited"}\n'
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self._security_headers()
            self.end_headers()
            if not head_only:
                self.wfile.write(body)
            return

        if is_api:
            if not self.server.api_slots.acquire(blocking=False):
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"error": "busy"},
                    head_only=head_only,
                )
                return
            try:
                response = self._api_response(path, parsed.query)
            except KeyError:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": "not_found"},
                    head_only=head_only,
                )
                return
            except ValueError:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_request"},
                    head_only=head_only,
                )
                return
            except (
                HTTPError,
                URLError,
                OSError,
                json.JSONDecodeError,
                UnsafePublicDashboardPayload,
            ):
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": "temporarily_unavailable"},
                    head_only=head_only,
                )
                return
            finally:
                self.server.api_slots.release()
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

    def do_GET(self) -> None:  # noqa: N802
        self._handle(head_only=False)

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(head_only=True)

    def _method_not_allowed(self) -> None:
        body = b'{"error":"method_not_allowed"}\n'
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    do_POST = _method_not_allowed
    do_PUT = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_DELETE = _method_not_allowed
    do_OPTIONS = _method_not_allowed
    do_TRACE = _method_not_allowed

    def log_message(self, format: str, *args: object) -> None:
        path = urlparse(self.path).path
        sys.stderr.write(
            f"public-dashboard peer={self._client_key()} path={path} "
            f"message={format % args}\n"
        )


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
        "--shioaji-static-root",
        type=Path,
        default=Path("services/shioaji_api_dashboard"),
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--taifex-upstream", default="http://127.0.0.1:8765")
    parser.add_argument("--tw-upstream", default="http://127.0.0.1:8766")
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
        shioaji_static_root=Path(args.shioaji_static_root),
        repo_root=Path(args.repo_root),
        taifex_upstream=str(args.taifex_upstream),
        tw_upstream=str(args.tw_upstream),
    )
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
