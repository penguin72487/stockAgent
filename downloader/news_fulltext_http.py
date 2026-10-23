"""Bounded HTTP/robots transport. One publisher shares one process-global bucket."""

from __future__ import annotations

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
        self.last_calls: dict[str, float] = {}
        self.provider_not_before: dict[str, float] = {}
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
            readiness = getattr(self.limiters[provider], "remaining_delay_seconds", None)
            if readiness:
                delay = readiness()
                if delay > max(config["response_deadline_seconds"], registry.sources[provider].interval_seconds):
                    self.provider_not_before[provider] = max(self.provider_not_before.get(provider, 0), time.time() + delay)
        self.requests_by_provider: dict[str, int] = {}
        self.response_bytes = 0
        self._sessions: list = []

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
        if base in source.sitemaps or parts.path.endswith(".xml"):
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
            self.limiters[provider].interval_seconds = self.effective_interval(provider)

    def cooldown_snapshot(self):
        with self.lock:
            return dict(self.provider_not_before)

    def capacity_snapshot(self):
        with self.lock:
            controls = {p: {"client_interval_seconds": self.client_intervals[p],
                            "effective_interval_seconds": self.effective_interval(p),
                            "parallelism": self.parallelism[p], "maximum_parallelism": self.maximum_parallelism[p],
                            "hard_interval_floor_seconds": self.hard_interval_floor(p),
                            "minimum_permitted_interval_this_owner": self.minimum_permitted_intervals.get(p)}
                        for p in self.limiters}
            for provider, control in controls.items():
                control['temporarily_paused_gateway_hosts'] = self.host_circuits.blocked(provider)
                control['temporarily_paused_robots_origins'] = self.blocked_robots_origins(provider)
        return {"providers": controls,
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

    def _reserve_request(self, source, url, *, endpoint=None, metadata_feed=None):
        # Lock only the send reservation, never the network response. Concurrent
        # responses may overlap, but dispatch commitments remain evenly paced.
        with self.metrics.measure("request_slot_wait", source.provider), self.send_locks[source.provider]:
            self._check_cooldown(source.provider)
            self._check_host_cooldown(source.provider, url)
            self._check_robots_origin_cooldown(source.provider, url)
            try:
                self.limiters[source.provider].wait(cancel_event=self.stop)
            except RateLimitWaitCancelled:
                raise NewsError("stopping", retry=True, delay=1, request_deferred=True) from None
            while True:
                with self.lock:
                    self._check_cooldown(source.provider)
                    self._check_host_cooldown(source.provider, url)
                    self._check_robots_origin_cooldown(source.provider, url)
                    delay = self.effective_interval(source.provider)
                    remaining = self.last_calls.get(source.provider, 0) + delay - time.monotonic()
                    if remaining <= 0:
                        if self.stop.is_set():
                            raise NewsError("stopping", retry=True, delay=1, request_deferred=True)
                        if metadata_feed is not None or source.authorization.get("valid_until"):
                            self._require(url, metadata_feed)  # Permission may expire during the pacing wait.
                        # One clock event, after telemetry-lock waiting, owns
                        # both the next deadline and measured dispatch gap.
                        # Refusal/floor checks and this commitment share the
                        # HTTP lock, so another response cannot change policy
                        # between the final check and this reservation.
                        self.last_calls[source.provider] = self.metrics.request_started(
                            source.provider, endpoint or self.endpoint_kind(source, url))
                        self.requests_by_provider[source.provider] = self.requests_by_provider.get(source.provider, 0) + 1
                        self.minimum_permitted_intervals[source.provider] = min(
                            self.minimum_permitted_intervals.get(source.provider, math.inf), delay)
                        return
                if self.stop.is_set() or remaining > 0 and self.stop.wait(remaining):
                    raise NewsError("stopping", retry=True, delay=1, request_deferred=True)
                # Re-read pacing after sleep: another response/robots parse can
                # impose a stricter publisher floor while this slot is waiting.

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
                               recovery_observation=None, robots_origin=None, robots_pressure=None, metadata_feed=None):
        """Preserve the refusal layer, not just a retry timestamp.

        Only failed access/policy requests take this path. Ordinary successful
        articles incur no new hash, parse or file write. These are private
        diagnostic events, not accepted-body receipts or public status fields.
        """
        provider = self._require(response.url, metadata_feed).provider
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
        source = self._require(url, metadata_feed)
        if robots_origin is not None and (policy_origin(robots_origin + '/') != robots_origin
                # Validate the initial REP authority with its policy URL, not
                # an unrelated home page outside an exact finite article scope.
                # This does not fetch the home page or widen redirect access.
                or self._require(robots_origin + '/robots.txt', metadata_feed).provider != source.provider):
            raise NewsError('robots_origin_context_invalid')
        if self.stop.is_set():
            raise NewsError("stopping", retry=True, delay=1, request_deferred=True)
        self._check_cooldown(source.provider)
        self._check_host_cooldown(source.provider, url)
        self._check_robots_origin_cooldown(source.provider, url)
        try:
            with self.metrics.measure("address_validation", source.provider):
                self.address_check(url)
        except NewsError as error:
            if error.code == "dns_error":
                self.defer(source.provider, max(30, error.delay))
            raise
        self._reserve_request(source, url, endpoint='robots' if robots_origin is not None else None, metadata_feed=metadata_feed)
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
                        robots_pressure=getattr(error, 'robots_pressure_corroboration', None), metadata_feed=metadata_feed)
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

    def _remember_robots_policy_pause(self, source, origin, deadline):
        with self.lock:
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
        self._remember_robots_policy_pause(source, origin, state['not_before_epoch'])
        self.robots_parsers.pop(origin, None)
        try:
            # Keep a previous policy/validators as evidence, NOT permission.
            # Its Allow lines cannot be reused after a failed policy probe.
            atomic_write_json(cache_path, {**(cache or {}), 'origin': origin, 'policy_retry': state}, durable=True)
        except OSError:
            self.metrics.count(source.provider, 'robots_policy_checkpoint_write_errors')
            self.defer(source.provider, 300)
            raise NewsError('robots_policy_checkpoint_failure', retry=True, delay=300) from None
        self.metrics.count(source.provider, 'robots_origin_policy_circuit_opened')
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
                        if self._require(target, metadata_feed).provider != source.provider:
                            raise NewsError("robots_redirect_refused")
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
                                self._record_access_problem(result, error.code, body_read=True, metadata_feed=metadata_feed)
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
                atomic_write_json(cache_path, cache)
                with self.lock:
                    self.robots_policy_pauses.get(source.provider, {}).pop(origin, None)
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
            response = self._raw(url, request_headers, **({'metadata_feed': metadata_feed} if metadata_feed is not None else {}))
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
