"""Bounded HTTP/robots transport. One publisher shares one process-global bucket."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import errno
import fcntl
import hashlib
import json
import math
from pathlib import Path
import re
import ssl
import threading
import time
from typing import Callable
from urllib.parse import urljoin, urlsplit

import requests
from protego import Protego

from downloader.artifact_io import atomic_write_json
from downloader.common import RateLimitWaitCancelled, SharedRateLimiter, parse_retry_after_seconds
from downloader.news_fulltext_connections import OriginConnectionSession
from downloader.news_fulltext_metrics import (NewsMetrics, ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION,
    ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION, TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, normalize_url, public_address_check
from downloader.news_fulltext_robots import (MAX_ROBOTS_ORIGINS_PER_PROVIDER, next_robots_policy_retry,
    policy_origin, robots_policy_retry, robots_representation)


@dataclass(frozen=True)
class RedirectHop:
    """Actual source response, not an inferred permalink or fetched body."""
    from_url: str
    status: int
    location: str
    to_url: str
    observed_at_utc: str
    response_sha256: str
    response_bytes: int


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    redirects: tuple[RedirectHop, ...] = ()


ACCESS_DIAGNOSTIC_HEADERS = frozenset({
    "server", "content-type", "content-length", "content-encoding", "location",
    "retry-after", "cache-control", "via", "x-cache", "x-amz-cf-pop",
    "cf-mitigated", "x-robots-tag", "allow", "etag", "last-modified",
})

# This rare repair is status-only and runs only after overflow plus a real
# successful policy check. Never let a corrupt cache tree create an unbounded
# scan in a normal status heartbeat.
MAX_ROBOTS_RECHECK_SCAN_FILES = 65536


class _RequestSlotCancellation:
    """Cancel a queued slot without confusing client waiting with an HTTP error."""

    def __init__(self, http, provider, url):
        self.http, self.provider, self.url = http, provider, url
        self.deadline = time.monotonic() + http.config['response_deadline_seconds']
        self.error = None

    def is_set(self):
        # SharedRateLimiter removes a cancelled ticket itself. Never raise from
        # this predicate: doing so would strand the ticket in its FIFO queue.
        try:
            self.http._check_request_gates(self.provider, self.url)
        except NewsError as error:
            self.error = error
            return True
        return time.monotonic() >= self.deadline


class NewsHTTP:
    def __init__(self, registry: PolicyRegistry, config: dict, root: Path, *,
                 session_factory: Callable = OriginConnectionSession,
                 address_check: Callable = public_address_check,
                 limiter_factory: Callable = SharedRateLimiter,
                 metrics: NewsMetrics | None = None,
                 access_observer: Callable[[str, Response], None] | None = None):
        self.registry, self.config, self.root = registry, config, root
        self.metrics = metrics or NewsMetrics()
        # Opt-in, bounded diagnostics use the very same request path. Normal
        # collection neither parses/copies extra bodies nor writes trace files.
        self.access_observer = access_observer
        self.http_owner_lock = None
        if config.get("exclusive_http_owner", False):
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.http_owner_lock = (root / "http-owner.lock").open("a")
            try:
                fcntl.flock(self.http_owner_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.http_owner_lock.close()
                self.http_owner_lock = None
                raise NewsError("news_http_owner_already_running") from None
        self.session_factory, self.address_check = session_factory, address_check
        self.local = threading.local()
        self.lock = threading.RLock()
        from downloader.news_fulltext_wake import OwnerStopEvent
        self.stop = OwnerStopEvent()
        self.limiters = {p.provider: limiter_factory(p.interval_seconds, name="news-fulltext:" + p.provider)
                         for p in registry.sources.values() if p.request_enabled}
        self.send_locks = {provider: threading.Lock() for provider in self.limiters}
        self.robots_locks: dict[str, threading.Lock] = {}
        self.robots_parsers: dict[str, tuple[float, Protego]] = {}
        self.robot_delays: dict[str, float] = {}
        self.robots_policy_pauses: dict[str, dict[str, float]] = {}
        # Negative REP evidence remains unresolved after its retry deadline.
        # This is status evidence only: it must never extend a scheduling gate.
        self.robots_policy_rechecks: dict[str, dict[str, float | None]] = {}
        self.robots_policy_rechecks_overflow: set[str] = set()
        self.robots_recheck_rescan_needed: set[str] = set()
        self.robots_recheck_rescan_not_before: dict[str, float] = {}
        self.robots_recheck_revisions: dict[str, int] = {}
        self.robots_recheck_writers: dict[str, int] = {}
        self.robots_recheck_unpersisted: dict[str, set[str]] = {}
        self.robots_recheck_unpersisted_overflow: set[str] = set()
        self.robots_recheck_scan_lock = threading.Lock()
        self.last_calls: dict[str, float] = {}
        self.provider_not_before: dict[str, float] = {}
        # Actual shared-slot deadlines are separate from source refusals. The
        # capacity controller must not mistake our own pacing for a new 403/429.
        self.pacing_not_before: dict[str, float] = {}  # Monotonic; epoch only at the display boundary.
        self.transport_failures: dict[str, int] = {}
        self.rate_refusal_intervals: dict[str, float] = {}
        self.articles_since_sitemap: dict[str, int] = {}
        self.bulk_since_article: dict[str, int] = {}
        entries = {entry["id"]: entry for entry in config["providers"]}
        self.client_intervals = {p: registry.sources[p].interval_seconds for p in self.limiters}
        self.parallelism = {p: registry.sources[p].parallelism for p in self.limiters}
        self.maximum_parallelism = {p: entries[p].get("capacity_max_parallelism", registry.sources[p].parallelism)
                                    for p in self.limiters}
        self.minimum_permitted_intervals: dict[str, float] = {}
        self.capacity_controller = None
        from downloader.news_fulltext_host_circuit import GatewayHostCircuit
        self.host_circuits = GatewayHostCircuit(root, registry, threshold=config.get('transport_breaker_failures', 3))
        for provider in self.limiters:
            try:
                checkpoint = json.loads((root / "cooldowns" / (provider + ".json")).read_text())
                deadline = float(checkpoint["not_before_epoch"])
                if checkpoint["provider"] == provider and deadline > time.time():
                    self.provider_not_before[provider] = deadline
                learned = float(checkpoint.get("rate_refusal_interval_seconds", 0))
                if checkpoint["provider"] == provider and math.isfinite(learned) and 0 < learned <= 3600:
                    self.rate_refusal_intervals[provider] = learned
                    self.limiters[provider].interval_seconds = max(self.limiters[provider].interval_seconds, learned)
            except (OSError, ValueError, KeyError, TypeError):
                pass
            self._refresh_pacing_deadline(provider)
        self.requests_by_provider: dict[str, int] = {}
        self.response_bytes = 0
        self._sessions: list = []
        self._restore_robots_policy_pauses()

    def _restore_robots_policy_pauses(self):
        """Restore durable negative REP state before the first scheduler snapshot.

        A checkpoint only postpones requests; it never restores a positive
        robots policy or changes the authorization for an origin. Validate its
        file identity and current provider as well as the existing retry
        contract. The normal request path still performs the actual REP check.
        """
        now = time.time()
        max_cache_bytes = 6 * self.config['max_response_bytes'] + 65536
        for path in (self.root / 'robots').glob('*.json'):
            if not re.fullmatch(r'[0-9a-f]{64}\.json', path.name):
                continue
            try:
                if path.stat().st_size > max_cache_bytes:
                    continue
                cache = json.loads(path.read_bytes())
                origin = cache.get('origin') if isinstance(cache, dict) else None
                if (not isinstance(origin, str) or policy_origin(origin) != origin
                        or path.name != hashlib.sha256(origin.encode()).hexdigest() + '.json'):
                    continue
                source = self.registry.match(origin + '/')
                if source is None or source.provider not in self.limiters:
                    continue
                retry = robots_policy_retry(cache, origin, now)
                if isinstance(cache, dict) and 'policy_retry' in cache:
                    # A malformed negative checkpoint cannot prove recovery.
                    self._remember_robots_policy_recheck(
                        source, origin, retry['not_before_epoch'] if retry else None)
                if retry and retry['not_before_epoch'] > now:
                    try:
                        self._remember_robots_policy_pause(source, origin, retry['not_before_epoch'])
                    except NewsError as error:
                        if error.code != 'robots_origin_policy_capacity_exhausted':
                            raise
                        # The existing bounded fallback defers this provider.
                    else:
                        self.metrics.count(source.provider, 'robots_origin_policy_pauses_restored')
            except (OSError, ValueError, TypeError, NewsError):
                # Bad local state never grants permission; _robots still needs
                # a fresh policy. One malformed file must not block startup.
                continue

    def hard_interval_floor(self, provider):
        return max(self.registry.sources[provider].official_min_interval_seconds,
                   self.robot_delays.get(provider, 0), self.rate_refusal_intervals.get(provider, 0))

    def effective_interval(self, provider):
        return max(self.client_intervals[provider], self.hard_interval_floor(provider))

    @staticmethod
    def endpoint_kind(source, url):
        """Bounded endpoint families, never article IDs/query tokens in metrics."""
        parts = urlsplit(url)
        base = parts.scheme + "://" + parts.netloc + parts.path
        if parts.path == "/robots.txt":
            return "robots"
        if url in source.metadata_feeds:
            return 'metadata_feed'
        if base in source.cnyes_apis:
            return "cnyes_" + parts.path.rsplit("/", 1)[-1]
        if base in source.wordpress_apis:
            return "wordpress_posts"
        if base in source.fulltext_feeds:
            return "rss_fulltext"
        if base in source.feeds:
            return "feed"
        if base in source.sitemaps or parts.path.endswith((".xml", ".xml.gz")):
            return "xml_discovery"
        if source.index_path_pattern and re.search(source.index_path_pattern, parts.path):
            return "index"
        if source.article_path_pattern and re.search(source.article_path_pattern, parts.path):
            return "article_pdf" if parts.path.lower().endswith(".pdf") else "article_html"
        return "other_metadata"

    def set_client_capacity(self, provider, interval, slots):
        if (provider not in self.limiters or type(interval) not in {int, float} or not math.isfinite(interval)
                or not 0.01 <= interval <= 3600 or type(slots) is not int or not 1 <= slots <= self.maximum_parallelism[provider]):
            raise ValueError("invalid bounded news client capacity")
        with self.lock:
            self.client_intervals[provider] = float(interval)
            self.parallelism[provider] = slots
            effective = self.effective_interval(provider)
            interval_changed = self.limiters[provider].interval_seconds != effective
            self.limiters[provider].interval_seconds = effective
        if interval_changed and effective > self.config['response_deadline_seconds']:
            # Calibration restores learned capacity after construction. Re-read
            # with that interval's restart/stale-state interpretation as well.
            self._refresh_pacing_deadline(provider)

    def traffic_snapshot(self):
        """Current owner's sent requests and received bytes; no I/O or requests."""
        with self.lock:
            return dict(self.requests_by_provider), self.response_bytes

    def cooldown_snapshot(self):
        """Scheduler readiness, including long client waits; no slot is claimed."""
        with self.lock:
            deadlines = dict(self.provider_not_before)
            for provider in self.limiters:
                deadline = self._pacing_schedule_deadline(provider)
                if deadline:
                    deadlines[provider] = max(deadlines.get(provider, 0), deadline)
            return deadlines

    def _refresh_pacing_deadline(self, provider):
        # Read at startup, source deferral, slow grants or cancelled waits, not
        # on every scheduler round/request for normally paced publishers.
        readiness = getattr(self.limiters[provider], 'remaining_delay_seconds', None)
        delay = readiness() if readiness else 0
        if delay > self.config['response_deadline_seconds']:
            with self.lock:
                self.pacing_not_before[provider] = max(
                    self.pacing_not_before.get(provider, 0), time.monotonic() + delay)

    def _local_pacing_remaining(self, provider):
        last = self.last_calls.get(provider)
        return max(0, last + self.effective_interval(provider) - time.monotonic()) if last is not None else 0

    def _pacing_schedule_deadline(self, provider):
        delay = max(0, self.pacing_not_before.get(provider, 0) - time.monotonic())
        if self.effective_interval(provider) > self.config['response_deadline_seconds']:
            delay = max(delay, self._local_pacing_remaining(provider))
        return time.time() + delay if delay > 0 else 0

    def _check_pacing(self, provider, *, shared=True):
        with self.lock:
            delay = self._local_pacing_remaining(provider)
            if shared:
                delay = max(delay, self.pacing_not_before.get(provider, 0) - time.monotonic())
            if delay > self.config['response_deadline_seconds']:
                raise NewsError('provider_pacing_cooldown', retry=True, delay=delay, request_deferred=True)

    def _check_request_gates(self, provider, url, *, shared_pacing=True):
        if self.stop.is_set():
            raise NewsError('stopping', retry=True, delay=1, request_deferred=True)
        self._check_cooldown(provider)
        self._check_host_cooldown(provider, url)
        self._check_robots_origin_cooldown(provider, url)
        self._check_pacing(provider, shared=shared_pacing)

    def capacity_snapshot(self):
        # Usual snapshots do no disk I/O. An overflow is reconciled only after
        # a real recovery, in one background reader without publisher calls.
        self._schedule_robots_recheck_refresh()
        with self.lock:
            controls = {p: {"client_interval_seconds": self.client_intervals[p],
                            "effective_interval_seconds": self.effective_interval(p),
                            "parallelism": self.parallelism[p], "maximum_parallelism": self.maximum_parallelism[p],
                            "hard_interval_floor_seconds": self.hard_interval_floor(p),
                            "request_pacing_not_before_epoch": self._pacing_schedule_deadline(p) or None,
                            "minimum_permitted_interval_this_owner": self.minimum_permitted_intervals.get(p)}
                        for p in self.limiters}
            for provider, control in controls.items():
                control['temporarily_paused_gateway_hosts'] = self.host_circuits.blocked(provider)
                control['temporarily_paused_robots_origins'] = self.blocked_robots_origins(provider)
                # Unlike the scheduler's blocked maps these include expired
                # failures until a real response clears their evidence.
                control['unverified_gateway_hosts'] = dict(self.host_circuits.paused.get(provider, {}))
                control['unverified_robots_origins'] = dict(self.robots_policy_rechecks.get(provider, {}))
                control['unverified_robots_origins_overflow'] = (provider in self.robots_policy_rechecks_overflow
                    or provider in self.robots_recheck_unpersisted_overflow)
        return {"providers": controls, "request_pacing_wait_contract_version": 1,
                "finite_robots_redirect_contract_version": 1,
                "calibration": self.capacity_controller.snapshot(for_status=True) if self.capacity_controller else None}

    def _session(self):
        if not hasattr(self.local, "session"):
            self.local.session = self.session_factory()
            # No implicit .netrc credentials, environment proxies or browser cookies.
            self.local.session.trust_env = False
            with self.lock:
                self._sessions.append(self.local.session)
        return self.local.session

    def close(self):
        self.stop.set()
        for session in self._sessions:
            session.close()
        if self.http_owner_lock is not None:
            fcntl.flock(self.http_owner_lock, fcntl.LOCK_UN)
            self.http_owner_lock.close()
            self.http_owner_lock = None

    def _reset_failed_connection(self, provider, url):
        session = getattr(self.local, "session", None)
        if session is None:
            return
        try:
            if isinstance(session, OriginConnectionSession):
                if session.reset_origin(url):
                    self.metrics.count(provider, "failed_origin_pools_reset")
                return
            # Injected/legacy transports have no verified per-origin pools.
            # Keep their original recovery rather than claiming isolation.
            del self.local.session
            with self.lock:
                self._sessions.remove(session)
            session.close()
            self.metrics.count(provider, "failed_sessions_reset")
        except (OSError, requests.RequestException):
            # The failed pool has already been removed. A cleanup error must
            # not replace the original source exception or destroy healthy pools.
            self.metrics.count(provider, "connection_pool_reset_errors")

    def defer(self, provider: str, seconds: float):
        self.limiters[provider].defer(seconds)
        with self.lock:
            self.provider_not_before[provider] = max(self.provider_not_before.get(provider, 0), time.time() + seconds)
            # Timers create a fresh process each cycle. Persist source-wide
            # refusals so the next owner skips this publisher before leasing
            # articles, instead of sleeping inside its shared limiter and
            # delaying an otherwise finished cycle for 15 minutes.
            atomic_write_json(self.root / "cooldowns" / (provider + ".json"), {
                "provider": provider, "not_before_epoch": self.provider_not_before[provider],
                "rate_refusal_interval_seconds": self.rate_refusal_intervals.get(provider, 0),
            })
        self._refresh_pacing_deadline(provider)

    def _check_cooldown(self, provider):
        with self.lock:
            if self.provider_not_before.get(provider, 0) > time.time():
                raise NewsError("provider_cooldown", retry=True,
                                delay=self.provider_not_before[provider] - time.time(), request_deferred=True)

    def blocked_gateway_hosts(self, provider):
        with self.lock:
            return tuple(self.host_circuits.blocked(provider))

    def _check_host_cooldown(self, provider, url):
        with self.lock:
            delay = self.host_circuits.remaining(provider, urlsplit(url).hostname)
            if delay:
                raise NewsError('host_gateway_cooldown', retry=True, delay=delay, request_deferred=True)

    def blocked_robots_origins(self, provider):
        with self.lock:
            now = time.time()
            return {origin: deadline for origin, deadline in self.robots_policy_pauses.get(provider, {}).items() if deadline > now}

    def request_scope_cooldown_snapshot(self):
        """Bounded local scheduling state; no network, policy reload or quota."""
        with self.lock:
            return {provider: {'hosts': self.host_circuits.blocked(provider),
                               'origins': self.blocked_robots_origins(provider)} for provider in self.limiters}

    def _check_robots_origin_cooldown(self, provider, url):
        with self.lock:
            delay = self.robots_policy_pauses.get(provider, {}).get(policy_origin(url), 0) - time.time()
            if delay > 0:
                error = NewsError('robots_origin_policy_cooldown', retry=True, delay=delay, request_deferred=True)
                error.robots_origin_isolation = True
                raise error

    def _require(self, url, metadata_feed=None):
        if metadata_feed is None:
            source = self.registry.require(url)
            if source.finite_article_urls and url not in source.finite_article_urls:
                parts = urlsplit(url)
                if (parts.scheme != 'https' or parts.hostname not in source.domains
                        or parts.path != '/robots.txt' or parts.query or parts.fragment):
                    raise NewsError('article_rights_or_scope_excluded')
            return source
        from downloader.news_fulltext_metadata_feeds import require_request
        return require_request(self.registry, url, metadata_feed)

    def _require_robots_request(self, url, origin, metadata_feed=None):
        """Validate the policy role without granting another article URL.

        The REP loop supplies only the original /robots.txt and actual Location
        hops. Finite articles may need a same-authority localized policy path;
        ordinary get(), metadata-feed capabilities and other authorities keep
        their existing exact scopes. Each hop still checks authorization, DNS,
        pacing, status and the final policy representation at the initial origin.
        """
        if policy_origin(origin + '/') != origin:
            raise NewsError('robots_origin_context_invalid')
        initial = self._require(origin + '/robots.txt', metadata_feed)
        if metadata_feed is None and initial.finite_article_urls:
            if policy_origin(url) != origin:
                raise NewsError('robots_redirect_refused', retry=True, delay=300)
            source = self.registry.require(url)
        else:
            source = self._require(url, metadata_feed)
        if source.provider != initial.provider:
            raise NewsError('robots_redirect_refused', retry=True, delay=300)
        return source

    def _reserve_request(self, source, url, *, endpoint=None, metadata_feed=None, robots_origin=None):
        # Lock only the send reservation, never the network response. Concurrent
        # responses may overlap, but dispatch commitments remain evenly paced.
        provider = source.provider
        cancellation = _RequestSlotCancellation(self, provider, url)
        send_lock = self.send_locks[provider]
        with self.metrics.measure("request_slot_wait", provider):
            while not send_lock.acquire(timeout=0.05):
                if cancellation.is_set():
                    self._raise_slot_cancellation(cancellation)
            try:
                self._check_request_gates(provider, url)
                try:
                    self.limiters[provider].wait(cancel_event=cancellation)
                except RateLimitWaitCancelled:
                    self._raise_slot_cancellation(cancellation)
                # A granted slot's future deadline is for subsequent requests,
                # not grounds for cancelling the current legitimate request.
                if self.effective_interval(provider) > self.config['response_deadline_seconds']:
                    self._refresh_pacing_deadline(provider)
                while True:
                    with self.lock:
                        self._check_request_gates(provider, url, shared_pacing=False)
                        delay = self.effective_interval(provider)
                        remaining = self._local_pacing_remaining(provider)
                        if remaining <= 0:
                            if metadata_feed is not None or source.authorization.get("valid_until"):
                                # Permission may expire during the pacing wait;
                                # a redirected policy retains its policy role.
                                if robots_origin is not None:
                                    self._require_robots_request(url, robots_origin, metadata_feed)
                                else:
                                    self._require(url, metadata_feed)
                            # One clock event, after telemetry-lock waiting,
                            # owns both the pacing deadline and dispatch gap.
                            self.last_calls[provider] = self.metrics.request_started(
                                provider, endpoint or self.endpoint_kind(source, url))
                            self.requests_by_provider[provider] = self.requests_by_provider.get(provider, 0) + 1
                            self.minimum_permitted_intervals[provider] = min(
                                self.minimum_permitted_intervals.get(provider, math.inf), delay)
                            return
                    if time.monotonic() >= cancellation.deadline:
                        self._raise_slot_cancellation(cancellation)
                    # A new source refusal/floor or stop must interrupt even a
                    # previously permitted short local wait.
                    self.stop.wait(min(0.05, remaining))
            finally:
                send_lock.release()

    def _raise_slot_cancellation(self, cancellation):
        if self.stop.is_set():
            raise NewsError('stopping', retry=True, delay=1, request_deferred=True)
        if cancellation.error is not None:
            raise cancellation.error
        self._refresh_pacing_deadline(cancellation.provider)
        self._check_request_gates(cancellation.provider, cancellation.url)
        with self.lock:
            remaining = max(self._local_pacing_remaining(cancellation.provider),
                self.pacing_not_before.get(cancellation.provider, 0) - time.monotonic(), 1)
        raise NewsError('provider_pacing_cooldown', retry=True, delay=remaining, request_deferred=True)

    def _response_status(self, provider: str, status: int, headers: dict, *, host=None, robots_origin=None):
        """Honor source signals before reading a possibly stalled error body.

        A lone gateway error is an article retry, not proof all publisher URLs
        failed. Gateway observations distinguish corroborated, provisional and
        correlated/unknown scope; explicit overload, refusal and Retry-After
        retain publisher-wide protection. A host pause is not proof of health.
        """
        retry_after = parse_retry_after_seconds(headers.get("retry-after"))
        has_retry_after = 'retry-after' in headers
        challenged = headers.get('cf-mitigated', '').lower() == 'challenge'
        if challenged:
            # Official explicit access signal, even on 200/502. Never treat
            # this as a gateway-only outage or read/solve the challenge body.
            self.metrics.count(provider, 'content_access_refusals')
            self.metrics.count(provider, 'header_access_refusals')
        if status in {401, 403, 451} or challenged and status != 429:
            delay = max(900, retry_after or 0)
            self.defer(provider, delay)
            raise NewsError("source_access_denied", retry=True, delay=delay)
        if status in {301, 302, 303, 307, 308} and retry_after is not None:
            delay = max(1, retry_after)
            self.defer(provider, delay)
            raise NewsError("redirect_retry_after", retry=True, delay=delay)
        if status == 429 or status >= 500:
            delay = max(30, retry_after or 0, 900 if challenged else 0)
            if robots_origin is not None and status in {502, 504} and not has_retry_after and not challenged:
                # REP failure denies THIS initial authority, not a different
                # registered origin with its own REP. Only recent, actually
                # completed other-host responses corroborate pressure isolation;
                # unknown/correlated health retains adaptive error pressure.
                with self.lock:
                    self.host_circuits.failure(provider, host)
                    proof = self.host_circuits.corroboration(provider, host,
                        excluded_hosts=(urlsplit(robots_origin).hostname,))
                healthy = proof['corroborating_hosts']
                self.metrics.count(provider, 'http_transient_errors')
                self.metrics.count(provider, 'robots_origin_unreachable_headers')
                error = NewsError('http_' + str(status), retry=True, delay=delay)
                error.robots_origin_isolation = True
                error.robots_pressure_corroboration = {
                    'contract_version': ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION, 'isolated': bool(healthy),
                    'basis': 'recent_completed_other_registered_host; not_permission_or_body_acceptance',
                    'corroborating_hosts': healthy,
                    'success_ages_seconds': {h: proof['other_success_ages_seconds'][h] for h in healthy}}
                raise error
            with self.lock:
                try:
                    gateway_decision = self.host_circuits.gateway_failure(provider, host, status,
                        retry_after if not has_retry_after or retry_after is not None else 0)
                except OSError:
                    self.metrics.count(provider, 'gateway_host_checkpoint_write_errors')
                    gateway_decision = None  # Preserve conservative publisher fallback.
                if gateway_decision and gateway_decision['scope'] in {'host', 'provisional_host'}:
                    provisional = gateway_decision['scope'] == 'provisional_host'
                    self.metrics.count(provider, 'provisional_gateway_host_errors' if provisional
                                       else 'isolated_gateway_host_errors')
                    if gateway_decision['pause_open']:
                        self.metrics.count(provider, 'provisional_gateway_host_circuit_opened' if provisional
                                           else 'isolated_gateway_host_circuit_opened')
                    host_deadline = gateway_decision['host_pause_not_before_epoch']
                    error = NewsError('http_' + str(status), retry=True,
                        delay=max(delay, host_deadline - time.time() if host_deadline is not None else 0))
                    error.isolated_gateway_host = not provisional
                    error.provisional_gateway_host = provisional
                    error.gateway_recovery_observation = gateway_decision
                    raise error
                if status == 429:
                    # A reset/restart must not resume the same refused rate.
                    # Back off the actually refused cadence, not an old config
                    # default. The persisted floor cannot be undone by tuning,
                    # robots refresh, or an owner restart.
                    learned = min(3600.0, 2 * self.effective_interval(provider))
                    self.rate_refusal_intervals[provider] = max(self.rate_refusal_intervals.get(provider, 0), learned)
                    self.limiters[provider].interval_seconds = max(self.limiters[provider].interval_seconds, learned)
                    self.metrics.count(provider, "rate_refusals_with_slower_pacing")
                failures = self.transport_failures.get(provider, 0) + 1
                self.transport_failures[provider] = failures
            self.metrics.count(provider, "http_transient_errors")
            if (status in {429, 503} or has_retry_after
                    or failures >= self.config.get("transport_breaker_failures", 3)):
                self.defer(provider, delay)
                self.metrics.count(provider, "http_circuit_opened")
            else:
                self.metrics.count(provider, "isolated_http_errors")
            error = NewsError("http_" + str(status), retry=True, delay=delay)
            if gateway_decision:
                error.gateway_recovery_observation = gateway_decision
            raise error

    def _record_access_problem(self, response: Response, code: str, *, body_read: bool,
                               recovery_observation=None, robots_origin=None, robots_pressure=None, metadata_feed=None,
                               request_robots_origin=None):
        """Preserve the refusal layer, not just a retry timestamp.

        Only failed access/policy requests take this path. Ordinary successful
        articles incur no new hash, parse or file write. These are private
        diagnostic events, not accepted-body receipts or public status fields.
        """
        provider = (self._require_robots_request(response.url, request_robots_origin, metadata_feed)
                    if request_robots_origin is not None else self._require(response.url, metadata_feed)).provider
        observation = {
            "contract_version": 2, "owner_id": self.metrics.owner_id,
            "provider": provider, "requested_url": response.url,
            "status": response.status, "error_code": code, "observed_at_epoch": time.time(),
            "headers": {k: v[:2048] for k, v in response.headers.items() if k in ACCESS_DIAGNOSTIC_HEADERS},
            "body_read": body_read, "diagnostic_only": True, "accepted_as_article": False,
            "publish": False,
        }
        if body_read:
            observation.update(response_sha256=hashlib.sha256(response.body).hexdigest(), response_bytes=len(response.body))
        if recovery_observation is not None:
            # Same failure receipt and durable write; no extra source request,
            # error-body read or successful-request hot-path instrumentation.
            observation['gateway_recovery'] = recovery_observation
        if robots_origin is not None:
            observation['robots_origin_policy'] = {"contract_version": ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION,
                "initial_origin": robots_origin, "scope": "initial_origin_policy", "permission_granted": False,
                "source_data_fingerprint": self.registry.sources[provider].data_fingerprint,
                "authorization_fingerprint": self.registry.sources[provider].authorization_fingerprint}
            if robots_pressure is not None:
                observation['robots_origin_policy']['capacity_pressure'] = robots_pressure
        self._persist_problem('access', provider, observation)

    def _persist_problem(self, kind, provider, observation, *, day=None):
        """Failure-only, private durable events; never publisher text or a writer."""
        digest = hashlib.sha256(json.dumps(observation, sort_keys=True).encode()).hexdigest()
        try:
            folder = self.root / (kind + '_problems')
            folder.mkdir(parents=True, exist_ok=True, mode=0o700)
            folder /= provider
            folder.mkdir(exist_ok=True, mode=0o700)
            if day is not None:
                folder /= day
                folder.mkdir(exist_ok=True, mode=0o700)
            target = folder / (str(time.time_ns()) + '-' + digest + '.json')
            atomic_write_json(target, observation, durable=True)
            target.chmod(0o600)
            if kind == 'transport':
                self.metrics.count(provider, 'transport_failure_observations_retained')
        except OSError:
            # Preserve the original source outcome if diagnostic storage is
            # unavailable; never turn it into success or stop healthy sources.
            self.metrics.count(provider, kind + '_diagnostic_write_errors')

    def _record_transport_problem(self, source, url, endpoint, phase, status, wall, headers_wall, size, error_types,
                                  *, robots_origin=None, connection_observation=None, robots_pressure=None):
        now = datetime.now(timezone.utc)
        observation = {
            'contract_version': TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION,
            'owner_id': self.metrics.owner_id,
            'provider': source.provider, 'requested_url': url, 'host': urlsplit(url).hostname,
            'endpoint': endpoint, 'phase': phase, 'http_status': status,
            'observed_at_utc': now.isoformat(), 'request_elapsed_seconds': wall,
            'request_to_headers_seconds': headers_wall, 'response_bytes_seen': size,
            'error_types': error_types, 'error_message_retained': False,
            'source_data_fingerprint': source.data_fingerprint,
            'authorization_fingerprint': source.authorization_fingerprint,
            'diagnostic_only': True, 'accepted_as_article': False, 'body_stored': False, 'publish': False,
            'clock_basis': 'local request wrapper, excluding limiter/address wait; before diagnostic persistence/cleanup; not packet or source publication clocks',
        }
        if connection_observation is not None:
            observation['connection_observation'] = connection_observation
        if robots_origin is not None:
            observation['robots_origin_policy'] = {
                'contract_version': ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION,
                'initial_origin': robots_origin, 'scope': 'initial_origin_policy', 'permission_granted': False,
                'source_data_fingerprint': source.data_fingerprint,
                'authorization_fingerprint': source.authorization_fingerprint}
            if robots_pressure is not None:
                observation['robots_origin_policy']['capacity_pressure'] = robots_pressure
        self._persist_problem('transport', source.provider, observation, day=now.strftime('%Y%m%d'))

    def _raw(self, url: str, headers: dict | None = None, *, robots_origin=None, metadata_feed=None) -> Response:
        try:
            source = (self._require_robots_request(url, robots_origin, metadata_feed)
                      if robots_origin is not None else self._require(url, metadata_feed))
            self._check_request_gates(source.provider, url)
            try:
                with self.metrics.measure("address_validation", source.provider):
                    self.address_check(url)
            except NewsError as error:
                if error.code == 'non_public_address' and robots_origin is not None:
                    # No request was sent. Revalidate this authority's REP later,
                    # instead of permanently failing every article behind it.
                    # The same all-address/redirect checks still run on retry.
                    self.metrics.count(source.provider, 'robots_pretransport_address_rejections')
                    raise NewsError('robots_non_public_address', retry=True, delay=30) from error
                if error.code == "dns_error":
                    self.defer(source.provider, max(30, error.delay))
                raise
            self._reserve_request(source, url, endpoint='robots' if robots_origin is not None else None,
                                  metadata_feed=metadata_feed, robots_origin=robots_origin)
        except NewsError as error:
            if error.request_deferred:
                # Only this pre-transport catch can establish that _raw has
                # not dispatched. An observer/transport error after send may
                # itself carry a gate-shaped error and is not such evidence.
                error.raw_request_not_sent = True
            raise
        started = time.monotonic()
        started_cpu = time.thread_time()
        endpoint = 'robots' if robots_origin is not None else self.endpoint_kind(source, url)
        status, outcome, error_type = None, "internal_error", None
        phase, headers_wall, size = 'awaiting_headers', None, 0
        connection_observation, session = None, None
        idle_expirations_before = 0
        isolated_robots_origin = False
        host = urlsplit(url).hostname
        request_headers = {"User-Agent": self.config["user_agent"],
                           "Accept": "text/html,application/xhtml+xml,application/xml,text/plain;q=0.9"}
        if endpoint in {"feed", "rss_fulltext", "metadata_feed"}:
            # application/xml does not match the distinct RSS/Atom media
            # types. Declare our existing feed parsers only on registered
            # endpoints; this changes neither access nor retry permission.
            request_headers["Accept"] = ("application/rss+xml,application/atom+xml,text/xml,"
                                         + request_headers["Accept"])
        registered_pdf = bool(source.pdf_path_pattern and re.fullmatch(source.pdf_path_pattern, urlsplit(url).path))
        if registered_pdf:
            # HTTP 406 is content negotiation, not missing source data. Ask for
            # the registered representation; never impersonate a browser or
            # retry through another identity to evade an access refusal.
            request_headers["Accept"] = "application/pdf"
        request_headers.update(headers or {})
        try:
            session = self._session()
            if isinstance(session, OriginConnectionSession):
                idle_expirations_before = session.idle_connection_expirations
            with session.get(url, headers=request_headers, allow_redirects=False, stream=True,
                                     timeout=(10, self.config["request_timeout_seconds"])) as response:
                headers_wall = time.monotonic() - started
                phase = 'response_policy'
                response_headers = {key.lower(): value for key, value in response.headers.items()}
                status = response.status_code
                self.metrics.response_received(source.provider, endpoint, status)
                if self.access_observer is not None:
                    self.access_observer("headers", Response(url, response.status_code, response_headers, b""))
                try:
                    self._response_status(source.provider, response.status_code, response_headers, host=host, robots_origin=robots_origin)
                except NewsError as error:
                    self._record_access_problem(Response(url, response.status_code, response_headers, b""), error.code,
                        body_read=False, recovery_observation=getattr(error, 'gateway_recovery_observation', None),
                        robots_origin=robots_origin if getattr(error, 'robots_origin_isolation', False) else None,
                        robots_pressure=getattr(error, 'robots_pressure_corroboration', None), metadata_feed=metadata_feed,
                        request_robots_origin=robots_origin)
                    raise
                maximum = max(self.config["max_response_bytes"], source.pdf_max_response_bytes) if registered_pdf else self.config["max_response_bytes"]
                try:
                    if int(response.headers.get("Content-Length", "0")) > maximum:
                        raise NewsError("response_too_large")
                except ValueError:
                    pass
                parts = []
                phase = 'reading_body'
                for chunk in response.iter_content(chunk_size=32768):
                    if time.monotonic() - started > self.config["response_deadline_seconds"]:
                        raise NewsError("response_deadline", retry=True, delay=30)
                    size += len(chunk)
                    if size > maximum:
                        raise NewsError("response_too_large")
                    parts.append(chunk)
                phase = 'assembling_body'
                with self.lock:
                    self.response_bytes += size
                result = Response(url, response.status_code, response_headers, b"".join(parts))
                self.metrics.count(source.provider, "http_response_bytes", size)
                if self.access_observer is not None:
                    self.access_observer("complete", result)
                outcome = "complete"
                phase = 'complete'
        except NewsError as error:
            if error.request_deferred:
                # Even a reused gate-shaped exception from a callback cannot
                # attest that this already-dispatched request was unsent.
                error.raw_request_not_sent = False
            isolated_robots_origin = bool(getattr(error, 'robots_pressure_corroboration', {}).get('isolated'))
            outcome = ('robots_origin_policy_unreachable' if getattr(error, 'robots_origin_isolation', False) else
                       'isolated_gateway_host_error' if getattr(error, 'isolated_gateway_host', False) else
                       'provisional_gateway_host_error' if getattr(error, 'provisional_gateway_host', False) else
                       error.code if error.code in {"response_deadline", "response_too_large"}
                       else "source_status_rejected" if status is not None else "request_rejected")
            raise
        except requests.RequestException as exc:
            outcome, error_type = "transport_error", type(exc).__name__
            failure_wall = time.monotonic() - started
            if isinstance(session, OriginConnectionSession):
                # Observe before recovery discards this failed origin's pool.
                connection_observation = session.finish_connection_observation(url)
            # One origin's bad certificate denies its REP, not permission to
            # disable TLS, reuse old Allow, replace its host or request a body.
            # Only fresh completed OTHER-host proof isolates provider pressure.
            # Unknown TLS/routes and all ordinary article failures retain the
            # conservative publisher breaker.
            chain, seen, route_failure, error_types = exc, set(), isinstance(exc, requests.exceptions.SSLError), []
            certificate_failure, network_route_failure = False, False
            while chain is not None and id(chain) not in seen:
                seen.add(id(chain))
                if len(error_types) < 8:
                    error_types.append(type(chain).__name__[:64])
                certificate_failure |= isinstance(chain, ssl.SSLCertVerificationError)
                network_route_failure |= getattr(chain, "errno", None) in {errno.ENETUNREACH, errno.EHOSTUNREACH}
                route_failure |= network_route_failure
                chain = chain.__cause__ or chain.__context__
            rep_certificate = (robots_origin is not None and certificate_failure and not network_route_failure
                               and status is None and phase == 'awaiting_headers')
            robots_pressure = None
            with self.lock:
                self.host_circuits.failure(source.provider, host)
                if rep_certificate:
                    proof = self.host_circuits.corroboration(source.provider, host,
                        excluded_hosts=(urlsplit(robots_origin).hostname,))
                    healthy = proof['corroborating_hosts']
                    isolated_robots_origin = bool(healthy)
                    robots_pressure = {
                        'contract_version': ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION, 'isolated': bool(healthy),
                        'basis': 'recent_completed_other_registered_host; not_permission_or_body_acceptance',
                        'corroborating_hosts': healthy,
                        'success_ages_seconds': {h: proof['other_success_ages_seconds'][h] for h in healthy}}
                failures = self.transport_failures.get(source.provider, 0)
                if not isolated_robots_origin:
                    failures += 1
                    self.transport_failures[source.provider] = failures
            self._record_transport_problem(source, url, endpoint, phase, status, failure_wall, headers_wall, size, error_types,
                robots_origin=robots_origin, connection_observation=connection_observation, robots_pressure=robots_pressure)
            if not isolated_robots_origin and (route_failure or failures >= self.config.get("transport_breaker_failures", 3)):
                self.defer(source.provider, 30)
                self.metrics.count(source.provider, "transport_circuit_opened")
            self._reset_failed_connection(source.provider, url)
            self.metrics.count(source.provider, "transport_errors")
            self.metrics.count(source.provider, "transport_error_" + type(exc).__name__)
            error = NewsError("transport_error", retry=True, delay=30)
            if rep_certificate:
                # Persist the actual initial authority's negative policy and
                # bounded retry generation, even if pressure scope is unknown.
                # _robots owns that checkpoint; expiry is a probe, not recovery.
                error.robots_origin_isolation = True
                error.robots_pressure_corroboration = robots_pressure
            raise error from exc
        finally:
            if connection_observation is None and isinstance(session, OriginConnectionSession):
                connection_observation = session.finish_connection_observation(url)
            if isinstance(session, OriginConnectionSession):
                expirations = session.idle_connection_expirations - idle_expirations_before
                if expirations:
                    self.metrics.count(source.provider, 'expired_idle_connections', expirations)
            self.metrics.request_finished(source.provider, endpoint, status, outcome,
                                          time.monotonic() - started, time.thread_time() - started_cpu,
                                          error_type=error_type, host=host, phase=phase,
                                          headers_wall=headers_wall, response_bytes=size,
                                          connection_observation=connection_observation,
                                          isolated_robots_origin=isolated_robots_origin)
        with self.lock:
            self.transport_failures[source.provider] = 0
            if result.status < 400:
                try:
                    if self.host_circuits.success(source.provider, host):
                        self.metrics.count(source.provider, 'gateway_host_recoveries')
                except OSError:
                    # The completed response is still real. Keep checkpoint
                    # failure observable; do not turn a valid body into another
                    # source request. An old disk pause remains conservative
                    # across restart, and every deadline is bounded.
                    self.metrics.count(source.provider, 'gateway_host_checkpoint_write_errors')
        return result

    @contextmanager
    def _robots_checkpoint_write(self, provider, origin, *, recovery=False):
        """Fence a status-only rescan against in-flight checkpoint writes."""
        with self.lock:
            self.robots_recheck_writers[provider] = self.robots_recheck_writers.get(provider, 0) + 1
            self.robots_recheck_revisions[provider] = self.robots_recheck_revisions.get(provider, 0) + 1
        complete = False
        try:
            yield
            complete = True
        finally:
            with self.lock:
                uncertain = self.robots_recheck_unpersisted.setdefault(provider, set())
                if complete:
                    uncertain.discard(origin)
                elif origin in uncertain or len(uncertain) < MAX_ROBOTS_ORIGINS_PER_PROVIDER:
                    uncertain.add(origin)
                else:
                    # An unrecorded failed write is not evidence of recovery.
                    self.robots_recheck_unpersisted_overflow.add(provider)
                self.robots_recheck_writers[provider] -= 1
                self.robots_recheck_revisions[provider] = self.robots_recheck_revisions.get(provider, 0) + 1
                if complete and recovery and provider in self.robots_policy_rechecks_overflow:
                    self.robots_recheck_rescan_needed.add(provider)
                    self.robots_recheck_rescan_not_before.pop(provider, None)

    def _schedule_robots_recheck_refresh(self):
        """Schedule at most one nonblocking status-only checkpoint scan."""
        now = time.monotonic()
        with self.lock:
            targets = {provider: self.robots_recheck_revisions.get(provider, 0)
                for provider in self.robots_recheck_rescan_needed
                if provider in self.robots_policy_rechecks_overflow
                and not self.robots_recheck_writers.get(provider, 0)
                and not self.robots_recheck_unpersisted.get(provider)
                and provider not in self.robots_recheck_unpersisted_overflow
                and self.robots_recheck_rescan_not_before.get(provider, 0) <= now}
        if self.stop.is_set() or not targets or not self.robots_recheck_scan_lock.acquire(blocking=False):
            return
        try:
            threading.Thread(target=self._refresh_robots_recheck_overflows, args=(targets,),
                             name='news-robots-recheck-scan', daemon=True).start()
        except RuntimeError:
            self.robots_recheck_scan_lock.release()
            with self.lock:
                for provider in targets:
                    self.robots_recheck_rescan_not_before[provider] = time.monotonic() + 30

    def _refresh_robots_recheck_overflows(self, targets):
        """Recover a bounded status index after real policy recovery.

        The persisted per-origin checkpoints remain authoritative. This never
        changes an HTTP gate or sends a request. A concurrent or unpersisted
        checkpoint, unreadable file, changed directory or scan limit leaves
        the result unknown; another snapshot may retry the same work.
        """
        try:
            if self.stop.is_set():
                return
            rows: dict[str, dict[str, float | None]] = {provider: {} for provider in targets}
            overflow = set()
            directory = self.root / 'robots'
            before = directory.stat()
            if not directory.is_dir():
                raise OSError('robots checkpoint directory unavailable')
            max_cache_bytes = 6 * self.config['max_response_bytes'] + 65536
            now_epoch = time.time()
            scanned = 0
            for path in directory.glob('*.json'):
                if self.stop.is_set():
                    return
                if not re.fullmatch(r'[0-9a-f]{64}\.json', path.name):
                    continue
                scanned += 1
                if scanned > MAX_ROBOTS_RECHECK_SCAN_FILES:
                    raise OSError('robots checkpoint scan limit exceeded')
                stat = path.stat()
                if stat.st_size > max_cache_bytes:
                    raise OSError('robots checkpoint exceeds scan limit')
                payload = path.read_bytes()
                after_file = path.stat()
                if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != (
                        after_file.st_dev, after_file.st_ino, after_file.st_size,
                        after_file.st_mtime_ns, after_file.st_ctime_ns):
                    return  # Do not infer recovery from a concurrently changed checkpoint.
                cache = json.loads(payload)
                if not isinstance(cache, dict):
                    raise ValueError('invalid robots checkpoint')
                if 'policy_retry' not in cache:
                    continue
                origin = cache.get('origin')
                if (not isinstance(origin, str) or policy_origin(origin) != origin
                        or path.name != hashlib.sha256(origin.encode()).hexdigest() + '.json'):
                    raise ValueError('invalid robots checkpoint origin')
                source = self.registry.match(origin + '/')
                if source is None or source.provider not in targets:
                    continue
                retry = robots_policy_retry(cache, origin, now_epoch)
                entries = rows[source.provider]
                if len(entries) < MAX_ROBOTS_ORIGINS_PER_PROVIDER:
                    entries[origin] = retry['not_before_epoch'] if retry else None
                else:
                    overflow.add(source.provider)
            after = directory.stat()
            if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns):
                return  # A concurrent replacement requires a fresh scan.
            with self.lock:
                if self.stop.is_set():
                    return
                for provider, revision in targets.items():
                    if (revision != self.robots_recheck_revisions.get(provider, 0)
                            or self.robots_recheck_writers.get(provider, 0)
                            or self.robots_recheck_unpersisted.get(provider)
                            or provider in self.robots_recheck_unpersisted_overflow):
                        continue
                    self.robots_policy_rechecks[provider] = rows[provider]
                    if provider in overflow:
                        self.robots_policy_rechecks_overflow.add(provider)
                    else:
                        self.robots_policy_rechecks_overflow.discard(provider)
                    self.robots_recheck_rescan_needed.discard(provider)
                    self.robots_recheck_rescan_not_before.pop(provider, None)
        except (OSError, ValueError, TypeError, NewsError):
            # A broken scan must not spin on every status call or imply that
            # unexamined origins have recovered.
            with self.lock:
                retry_at = time.monotonic() + 30
                for provider in targets:
                    self.robots_recheck_rescan_not_before[provider] = retry_at
        finally:
            self.robots_recheck_scan_lock.release()

    def _remember_robots_policy_recheck(self, source, origin, deadline):
        """Bound unresolved evidence without changing the request deadline."""
        with self.lock:
            self.robots_recheck_revisions[source.provider] = self.robots_recheck_revisions.get(source.provider, 0) + 1
            rows = self.robots_policy_rechecks.setdefault(source.provider, {})
            if origin in rows or len(rows) < MAX_ROBOTS_ORIGINS_PER_PROVIDER:
                rows[origin] = deadline
            else:
                # Do not silently present a partial list as complete.
                self.robots_policy_rechecks_overflow.add(source.provider)

    def _remember_robots_policy_pause(self, source, origin, deadline):
        with self.lock:
            self._remember_robots_policy_recheck(source, origin, deadline)
            now = time.time()
            rows = self.robots_policy_pauses.setdefault(source.provider, {})
            for expired in [key for key, until in rows.items() if until <= now]:
                rows.pop(expired)
            if origin not in rows and len(rows) >= MAX_ROBOTS_ORIGINS_PER_PROVIDER:
                self.defer(source.provider, 300)  # Conservative bounded-capacity fallback.
                raise NewsError('robots_origin_policy_capacity_exhausted', retry=True, delay=300)
            rows[origin] = deadline

    def _pause_robots_policy(self, source, origin, cache_path, cache, previous, error):
        state = next_robots_policy_retry(source, error, previous, time.time())
        capacity_error = None
        try:
            with self._robots_checkpoint_write(source.provider, origin):
                try:
                    self._remember_robots_policy_pause(source, origin, state['not_before_epoch'])
                except NewsError as failure:
                    if failure.code != 'robots_origin_policy_capacity_exhausted':
                        raise
                    # The existing provider fallback stays in force, but the
                    # failed origin still needs its own durable checkpoint.
                    capacity_error = failure
                self.robots_parsers.pop(origin, None)
                # Keep previous validators as evidence, never as permission.
                atomic_write_json(cache_path, {**(cache or {}), 'origin': origin, 'policy_retry': state}, durable=True)
        except OSError:
            self.metrics.count(source.provider, 'robots_policy_checkpoint_write_errors')
            try:
                self.defer(source.provider, 300)
            except OSError:
                pass  # The in-memory provider deadline and unknown evidence remain.
            raise NewsError('robots_policy_checkpoint_failure', retry=True, delay=300) from None
        self.metrics.count(source.provider, 'robots_origin_policy_circuit_opened')
        if capacity_error is not None:
            raise capacity_error
        error.delay = state['delay_seconds']
        error.robots_origin_isolation = True

    def _robots(self, url: str, *, metadata_feed=None) -> Protego:
        source = self._require(url, metadata_feed)
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        cache_path = self.root / "robots" / (hashlib.sha256(origin.encode()).hexdigest() + ".json")
        with self.lock:
            host_lock = self.robots_locks.setdefault(origin, threading.Lock())
        with host_lock:
            self._check_robots_origin_cooldown(source.provider, url)
            cached_parser = self.robots_parsers.get(origin)
            if cached_parser and cached_parser[0] + self.config["robots_ttl_seconds"] > time.time():
                return cached_parser[1]
            cache = None
            try:
                cache = json.loads(cache_path.read_text())
            except (OSError, ValueError):
                pass
            previous = robots_policy_retry(cache, origin, time.time())
            had_failure = isinstance(cache, dict) and 'policy_retry' in cache
            if previous and previous['not_before_epoch'] > time.time():
                self._remember_robots_policy_pause(source, origin, previous['not_before_epoch'])
                self._check_robots_origin_cooldown(source.provider, url)
            if cache is not None and (not isinstance(cache, dict) or cache.get("origin") != origin
                    or not isinstance(cache.get("body"), str) or type(cache.get("checked_at")) not in {int, float}
                    or not math.isfinite(cache["checked_at"]) or cache["checked_at"] > time.time() + 5
                    or cache.get("status") not in {200, 404, 410}):
                cache = None  # Re-fetch bad local state; never infer permission from it.
            if cache is not None and cache["status"] == 200 and cache["body"].lstrip("\ufeff \t\r\n").startswith("<"):
                cache = None  # Old raw HTML must pass the current representation classifier.
            if (cache is not None and cache.get("representation_kind") == "registered_home_without_rep_groups"
                    and cache.get("home_scope_fingerprint") != source.data_fingerprint):
                cache = None  # A revoked/changed reviewed home scope is not an empty policy.
            if had_failure or cache is None or cache["checked_at"] + self.config["robots_ttl_seconds"] <= time.time():
                headers = {}
                if cache:
                    if cache.get("etag"):
                        headers["If-None-Match"] = cache["etag"]
                    if cache.get("last_modified"):
                        headers["If-Modified-Since"] = cache["last_modified"]
                target = origin + "/robots.txt"
                previous_policy_cache = cache
                try:
                    for _ in range(6):
                        result = self._raw(target, headers, robots_origin=origin, **({'metadata_feed': metadata_feed} if metadata_feed is not None else {}))
                        if result.status not in {301, 302, 303, 307, 308}:
                            break
                        location = result.headers.get('location')
                        if not location:
                            raise NewsError('robots_redirect_missing_location', retry=True, delay=300)
                        target = normalize_url(urljoin(target, location))
                        # Redirect rules still belong to the initial authority.
                        self._require_robots_request(target, origin, metadata_feed)
                        headers = {}
                    else:
                        raise NewsError("robots_redirect_loop")
                    if result.status == 304 and cache:
                        cache["checked_at"] = time.time()
                    elif result.status in {200, 404, 410}:
                        cache = {"origin": origin, "checked_at": time.time(), "status": result.status,
                                 "body": "", "etag": result.headers.get("etag"),
                                 "last_modified": result.headers.get("last-modified")}
                        if result.status == 200:
                            try:
                                cache.update(robots_representation(result, source, origin))
                            except NewsError as error:
                                self._record_access_problem(result, error.code, body_read=True, metadata_feed=metadata_feed,
                                                            request_robots_origin=origin)
                                raise
                            if cache["representation_kind"] != "plain_utf8":
                                self.metrics.count(source.provider, "robots_" + cache["representation_kind"])
                    else:
                        raise NewsError("robots_unavailable", retry=True, delay=300)
                except NewsError as error:
                    if (getattr(error, 'robots_origin_isolation', False)
                            or error.code.startswith('robots_') and error.code != 'robots_access_challenge'):
                        self._pause_robots_policy(source, origin, cache_path, previous_policy_cache, previous, error)
                    raise
                cache.pop('policy_retry', None)
                with self.lock:
                    recovery = (had_failure or origin in self.robots_recheck_unpersisted.get(source.provider, set()))
                with self._robots_checkpoint_write(source.provider, origin, recovery=recovery):
                    atomic_write_json(cache_path, cache)
                    with self.lock:
                        self.robots_policy_pauses.get(source.provider, {}).pop(origin, None)
                        self.robots_policy_rechecks.get(source.provider, {}).pop(origin, None)
                if had_failure:
                    self.metrics.count(source.provider, 'robots_origin_policy_recoveries')
            parser = Protego.parse(cache["body"].lstrip("\ufeff"))
            token = self.config["user_agent"].split("/", 1)[0]
            delay = parser.crawl_delay(token) or 0
            rate = parser.request_rate(token)
            if rate and rate.requests:
                delay = max(delay, rate.seconds / rate.requests)
            with self.lock:
                self.robot_delays[source.provider] = max(self.robot_delays.get(source.provider, 0), float(delay))
                self.limiters[source.provider].interval_seconds = self.effective_interval(source.provider)
            self.robots_parsers[origin] = (cache["checked_at"], parser)
            return parser

    def get(self, url: str, *, headers: dict | None = None, follow_redirects: bool = True,
            metadata_feed=None, conditional_url: str | None = None) -> Response:
        """Check every requested origin; optionally inspect a redirect only.

        Link diagnosis may read a public redirect's location without following
        it into another publisher. It keeps the same robots, pacing, DNS and
        refusal checks and grants no permission to fetch the destination.
        Explicitly bound cache validators are sent only at their original
        representation URL, reached through the actual checked redirect chain.
        Other headers never follow redirects; the default behavior is unchanged.
        """
        if type(follow_redirects) is not bool:
            raise ValueError("follow_redirects must be a boolean")
        url = normalize_url(url)
        bound_url = normalize_url(conditional_url) if conditional_url is not None else None
        validators = {key: value for key, value in (headers or {}).items()
                      if key.lower() in {'if-none-match', 'if-modified-since'}} if bound_url else {}
        initial_headers = {key: value for key, value in (headers or {}).items()
                           if key.lower() not in {'if-none-match', 'if-modified-since'}} if bound_url else headers
        provider = self._require(url, metadata_feed).provider
        redirects = []
        for _ in range(6):
            # Check source rights and robots at every hop, not after auto-follow.
            if self._require(url, metadata_feed).provider != provider:
                raise NewsError("redirect_outside_authorized_provider")
            try:
                parser = self._robots(url, **({'metadata_feed': metadata_feed} if metadata_feed is not None else {}))
            except NewsError as error:
                if error.request_deferred:
                    # This flag is scoped to this get() call's initial URL,
                    # not all HTTP activity: REP requests may have occurred.
                    # Once a redirect was received, the initial URL was sent.
                    error.initial_request_not_sent = not redirects
                if getattr(error, 'robots_origin_isolation', False):
                    raise  # No extension of an intact origin deadline, no publisher-wide pause.
                if error.code.startswith("robots_"):
                    # A bad origin policy must not be fetched once for every
                    # pending article. Pause only this source; after the bounded
                    # cooldown, fetch/revalidate its policy again automatically.
                    delay = max(300, error.delay)
                    self.defer(provider, delay)
                    self.metrics.count(provider, "robots_policy_circuit_opened")
                    raise NewsError(error.code, retry=True, delay=delay) from error
                raise
            if not parser.can_fetch(url, self.config["user_agent"].split("/", 1)[0]):
                raise NewsError("robots_disallowed")
            request_headers = ({**(initial_headers or {}), **(validators if url == bound_url else {})}
                               if bound_url else headers)
            try:
                response = self._raw(url, request_headers, **({'metadata_feed': metadata_feed} if metadata_feed is not None else {}))
            except NewsError as error:
                if error.request_deferred:
                    # Only _raw's pre-transport catch proves this hop unsent.
                    # Earlier initial redirects still count as a send.
                    error.initial_request_not_sent = (not redirects and
                        getattr(error, 'raw_request_not_sent', False) is True)
                raise
            if response.status not in {301, 302, 303, 307, 308} or not follow_redirects:
                return replace(response, redirects=tuple(redirects)) if redirects else response
            location = response.headers.get("location")
            if not location:
                raise NewsError("invalid_redirect")
            target = normalize_url(urljoin(url, location))
            redirects.append(RedirectHop(url, response.status, location, target,
                datetime.now(timezone.utc).isoformat(), hashlib.sha256(response.body).hexdigest(), len(response.body)))
            url = target
            headers = None  # Conditional identifiers belong to the original URL.
            initial_headers = None
        raise NewsError("redirect_loop")
