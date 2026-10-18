"""Bounded owner-lifetime timings; no URLs, credentials or article text."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import threading
import time

from downloader.news_fulltext_connection_observation import (
    CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION, connection_observation_bucket,
)

REQUEST_DISPATCH_CLOCK_CONTRACT_VERSION = 2
HTML_SELECTOR_CANDIDATE_CONTRACT_VERSION = 2
CONNECTION_ORIGIN_ISOLATION_CONTRACT_VERSION = 1
HTTP_HOST_PHASE_CONTRACT_VERSION = 1
TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION = 1
GATEWAY_HOST_ISOLATION_CONTRACT_VERSION = 3
GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION = 4
ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION = 1
ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION = 1
ROBOTS_ORIGIN_TRANSPORT_PRESSURE_CONTRACT_VERSION = 1
MAX_REQUEST_HOSTS_PER_PROVIDER = 16
METRICS_SNAPSHOT_CLOCK_CONTRACT_VERSION = 1
CNYES_NATIVE_BODY_FAILURE_OBSERVATION_CONTRACT_VERSION = 1
CNYES_NATIVE_BODY_FAILURE_STAGES = frozenset({
    'missing_body_representation', 'visible_current_mismatch',
    'schema_mismatch_without_verified_figure_view', 'schema_comparison_mismatch',
    'table_grid_mismatch',
})
ETTODAY_ATTRIBUTION_FAILURE_OBSERVATION_CONTRACT_VERSION = 1
ETTODAY_ATTRIBUTION_FAILURE_STAGES = frozenset({
    'bare_reporter_creator_mismatch', 'image_credit_structure',
    'image_caption_structure', 'image_caption_ambiguous_credit',
    'image_credit_missing_following_prose', 'caption_credit_structure',
    'caption_credit_boundaries', 'caption_credit_reporter',
    'caption_credit_duplicate_reporters', 'caption_credit_missing_following_prose',
    'newsroom_credit_markup', 'duplicate_or_late_credit', 'duplicate_reporters',
    'late_caption_credit', 'unrecognized_precredit_paragraph', 'missing_visible_credit',
})
TRANSPORT_PHASES = frozenset({'awaiting_headers', 'response_policy', 'reading_body', 'assembling_body', 'complete'})


def metrics_snapshot_clock(status):
    """Counter cutoff, not the later heartbeat-building/publication clock.

    Legacy snapshots remain identifiable, never silently upgraded to the new
    locked counter clock. A present but malformed contract fails closed.
    """
    published = datetime.fromisoformat(status['updated_at_utc'])
    performance = status.get('performance', {})
    if not isinstance(performance, dict):
        raise ValueError('invalid_metrics_snapshot_payload')
    marker = performance.get('snapshot_clock_contract_version')
    value = performance.get('snapshot_at_utc')
    if 'snapshot_clock_contract_version' not in performance and 'snapshot_at_utc' not in performance:
        return published, False
    if type(marker) is not int or marker != METRICS_SNAPSHOT_CLOCK_CONTRACT_VERSION or type(value) is not str:
        raise ValueError('incompatible_metrics_snapshot_clock')
    snapshot = datetime.fromisoformat(value)
    if (snapshot.utcoffset() != timezone.utc.utcoffset(snapshot)
            or published.tzinfo is None or snapshot > published):
        raise ValueError('invalid_metrics_snapshot_clock')
    return snapshot, True


def bulk_body_page_classification(records, errors, *, cnyes=False, rss_metadata_only=False):
    """Classify parsed source rows, not pending heads or error count alone."""
    if rss_metadata_only:
        return 'rss_xml_controls_metadata_only_pages'
    if cnyes and errors:
        if not records and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors):
            return 'bulk_metadata_only_html_fallback_pages'
        if records:
            return 'bulk_valid_body_pages_with_item_errors'
    return None


def endpoint_workload(endpoint):
    """Small comparable work classes; categories share one publisher bucket."""
    if endpoint.startswith("cnyes_"):
        return "cnyes_bulk"
    if endpoint in {"rss_fulltext", "wordpress_posts", "article_pdf", "article_html"}:
        return endpoint
    return "metadata"


def record_workload(record):
    if record.get('acquisition_transport') == 'retained_raw_reparse':
        return 'cached_source_reparse'  # Never inflate online HTTP capacity evidence.
    method = record.get("extraction_method", "")
    return {"cnyes_api_content_v1": "cnyes_bulk", "rss_content_encoded_v1": "rss_fulltext",
            "wordpress_api_content_rendered": "wordpress_posts", "fed_pdf_original_text_v1": "article_pdf"}.get(method, "article_html")


class NewsMetrics:
    def __init__(self):
        self.owner_id = None  # Bound only after the canonical queue acquires its lock.
        self.started = time.monotonic()
        self.started_cpu = time.process_time()
        self.lock = threading.Lock()
        self.stages = {}
        self.providers = {}

    @contextmanager
    def measure(self, stage, provider=None):
        wall, cpu = time.monotonic(), time.thread_time()
        try:
            yield
        finally:
            self.observe(stage, time.monotonic() - wall, time.thread_time() - cpu, provider)

    def observe(self, stage, wall, cpu=0.0, provider=None):
        with self.lock:
            stages = self.stages if provider is None else self.providers.setdefault(provider, {"stages": {}, "counts": Counter()})["stages"]
            self._observe_stage(stages, stage, wall, cpu)

    @staticmethod
    def _observe_stage(stages, stage, wall, cpu):
        item = stages.setdefault(stage, {"count": 0, "wall_seconds": 0.0, "cpu_seconds": 0.0, "max_wall_seconds": 0.0})
        item["count"] += 1
        item["wall_seconds"] += wall
        item["cpu_seconds"] += cpu
        item["max_wall_seconds"] = max(item["max_wall_seconds"], wall)

    @staticmethod
    def _observe_wall_stage(stages, stage, wall):
        """Wall-only boundaries must not report an unmeasured CPU time as zero."""
        item = stages.setdefault(stage, {"count": 0, "wall_seconds": 0.0, "max_wall_seconds": 0.0})
        item["count"] += 1
        item["wall_seconds"] += wall
        item["max_wall_seconds"] = max(item["max_wall_seconds"], wall)

    def count(self, provider, key, value=1):
        with self.lock:
            self.providers.setdefault(provider, {"stages": {}, "counts": Counter()})["counts"][key] += value

    def request_started(self, provider, endpoint=None) -> float:
        """Commit one dispatch event and return its exact monotonic timestamp.

        The publisher pacer uses THIS timestamp for its next reservation.
        A separate pre-metric clock can under-report the next gap when a
        thread pauses or waits for telemetry. This is not a socket/WAN clock.
        """
        with self.lock:
            now = time.monotonic()
            item = self.providers.setdefault(provider, {"stages": {}, "counts": Counter()})
            previous = item.get("last_request_started_monotonic")
            if previous is not None:
                gap = now - previous
                item["minimum_request_start_gap_seconds"] = min(item.get("minimum_request_start_gap_seconds", gap), gap)
            item["last_request_started_monotonic"] = now
            counts = item["counts"]
            counts["http_requests_started"] += 1
            counts["http_inflight"] += 1
            counts["peak_http_inflight"] = max(counts["peak_http_inflight"], counts["http_inflight"])
            if endpoint is not None:
                counts["endpoint_" + endpoint + "_requests"] += 1
            return now

    def response_received(self, provider, endpoint, status):
        """Headers are observable before a body completes or an error is raised."""
        with self.lock:
            counts = self.providers.setdefault(provider, {"stages": {}, "counts": Counter()})["counts"]
            counts["http_responses_received"] += 1
            counts["http_status_" + str(status)] += 1
            counts["http_endpoint_" + endpoint + "_responses_received"] += 1
            counts["http_endpoint_" + endpoint + "_status_" + str(status)] += 1

    def request_finished(self, provider, endpoint, status, outcome, wall, cpu, *, error_type=None,
                         host=None, phase=None, headers_wall=None, response_bytes=0,
                         connection_observation=None, isolated_robots_origin=False):
        """One terminal observation per reserved request, including failures.

        The endpoint comes from the bounded registered classifier, never a URL
        or article ID. Terminal counts keep late responses/errors attached to
        their real endpoint, not the dominant request-start mix of a new window.
        Headers with an unfinished body cannot masquerade as successful work.
        Coalesce timing/count updates under one lock instead of extra hot-path
        locks for each field. Actual source pacing is unchanged.
        """
        with self.lock:
            item = self.providers.setdefault(provider, {"stages": {}, "counts": Counter()})
            counts = item["counts"]
            prefix = "http_endpoint_" + endpoint + "_"
            counts["http_inflight"] -= 1
            counts["http_terminal_requests"] += 1
            counts[prefix + "terminal_requests"] += 1
            counts[prefix + "outcome_" + outcome] += 1
            connection_bucket = connection_observation_bucket(connection_observation)
            counts['connection_checkout_' + connection_bucket] += 1
            if status is not None:
                counts["http_terminal_responses"] += 1
                counts["http_terminal_status_" + str(status)] += 1
                counts[prefix + "terminal_status_" + str(status)] += 1
                if 500 <= status < 600:
                    counts["http_terminal_server_errors"] += 1
            if outcome == "transport_error":
                counts["http_terminal_transport_errors"] += 1
                counts[prefix + "transport_error_" + error_type] += 1
                if endpoint == 'robots' and status is None and isolated_robots_origin is True:
                    # Proven REP certificate failure has NO HTTP response.
                    # Keep the raw error/request; only its pressure subset is
                    # isolated. Never subtract a nonexistent response or body.
                    counts['http_terminal_isolated_robots_origin_transport_errors'] += 1
            elif outcome == 'isolated_gateway_host_error':
                counts['http_terminal_isolated_gateway_host_errors'] += 1
            elif outcome == 'provisional_gateway_host_error':
                counts['http_terminal_provisional_gateway_host_errors'] += 1
            elif (outcome == 'robots_origin_policy_unreachable' and endpoint == 'robots'
                    and status in {502, 504} and isolated_robots_origin is True):
                # Still included in raw requests/responses/server errors. This
                # terminal subset requires live other-host corroboration from
                # NewsHTTP; it is neither a success nor an access permission.
                counts['http_terminal_isolated_robots_origin_errors'] += 1
            elif outcome == "response_deadline":
                counts["http_terminal_deadline_errors"] += 1
                counts["response_deadline_errors"] += 1
            elif outcome in {"response_too_large", "internal_error", "request_rejected"}:
                counts["http_terminal_unusable_bodies"] += 1
            self._observe_stage(item["stages"], "transport", wall, cpu)
            self._observe_stage(item["stages"], "transport_endpoint_" + endpoint, wall, cpu)
            if host is not None:
                hosts = item.setdefault('request_hosts', {})
                # Labels contain only hostname, never path/query/account. A
                # wildcard source cannot create unbounded telemetry cardinality.
                if len(host) > 253 or host not in hosts and len(hosts) >= MAX_REQUEST_HOSTS_PER_PROVIDER:
                    host = '(other_hosts)'
                observed = hosts.setdefault(host, {'counts': Counter(), 'stages': {}})
                observed['counts']['terminal_requests'] += 1
                observed['counts']['outcome_' + outcome] += 1
                observed['counts']['response_bytes_seen'] += response_bytes
                connection_key = 'connection_checkout_' + connection_bucket
                observed['counts'][connection_key] += 1
                observed['counts'][connection_key + '_outcome_' + outcome] += 1
                if headers_wall is not None:
                    self._observe_wall_stage(observed['stages'], 'request_to_headers_' + connection_key, headers_wall)
                if connection_observation is not None:
                    self._observe_wall_stage(observed['stages'], 'connect_hook', connection_observation['connect_wall_seconds'])
                if status is not None:
                    observed['counts']['status_' + str(status)] += 1
                phase = phase if phase in TRANSPORT_PHASES else 'unclassified'
                if outcome == 'transport_error':
                    observed['counts']['transport_error_' + phase] += 1
                self._observe_stage(observed['stages'], 'transport', wall, cpu)
                if headers_wall is not None:
                    self._observe_wall_stage(observed['stages'], 'request_to_headers', headers_wall)
                    self._observe_wall_stage(observed['stages'], 'headers_to_terminal', max(0.0, wall - headers_wall))
                elif phase == 'awaiting_headers':
                    self._observe_stage(observed['stages'], 'failed_before_headers', wall, cpu)

    def snapshot(self):
        with self.lock:
            providers = {provider: {"counts": dict(item["counts"]),
                                    "stages": {key: dict(value) for key, value in item["stages"].items()},
                                    "request_hosts": {host: {'counts': dict(value['counts']),
                                        'stages': {key: dict(stage) for key, stage in value['stages'].items()}}
                                        for host, value in item.get('request_hosts', {}).items()},
                                    "minimum_request_start_gap_seconds": item.get("minimum_request_start_gap_seconds")}
                         for provider, item in self.providers.items()}
            stages = {key: dict(value) for key, value in self.stages.items()}
            # Copying counts and recording their clock share the same lock.
            # Status SQL/serialization and failure observations may follow;
            # none of those later events belongs to this counter generation.
            snapshot_at = datetime.now(timezone.utc).isoformat()
            elapsed = time.monotonic() - self.started
            cpu = time.process_time() - self.started_cpu
        return {"version": 1, "owner_id": self.owner_id, "cnyes_bulk_page_classification_version": 2,
                "html_selector_candidate_contract_version": HTML_SELECTOR_CANDIDATE_CONTRACT_VERSION,
                "cnyes_native_body_failure_observation_contract_version": CNYES_NATIVE_BODY_FAILURE_OBSERVATION_CONTRACT_VERSION,
                "ettoday_attribution_failure_observation_contract_version": ETTODAY_ATTRIBUTION_FAILURE_OBSERVATION_CONTRACT_VERSION,
                "snapshot_clock_contract_version": METRICS_SNAPSHOT_CLOCK_CONTRACT_VERSION,
                "snapshot_at_utc": snapshot_at,
                "http_terminal_observation_contract_version": 1,
                "http_host_phase_observation_contract_version": HTTP_HOST_PHASE_CONTRACT_VERSION,
                "gateway_host_isolation_contract_version": GATEWAY_HOST_ISOLATION_CONTRACT_VERSION,
                "gateway_host_checkpoint_contract_version": GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION,
                "robots_origin_policy_contract_version": ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION,
                "robots_origin_pressure_contract_version": ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION,
                "robots_origin_transport_pressure_contract_version": ROBOTS_ORIGIN_TRANSPORT_PRESSURE_CONTRACT_VERSION,
                "transport_failure_diagnostic_contract_version": TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION,
                "connection_origin_isolation_contract_version": CONNECTION_ORIGIN_ISOLATION_CONTRACT_VERSION,
                "connection_reuse_observation_contract_version": CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION,
                "connection_reuse_observation_basis": "native urllib3 checkout after dropped-socket check; open socket and idle since this connection object's prior terminal; connect() hooks include DNS/TCP/TLS together, not source server or packet timing; injected/unobserved transports explicitly unknown",
                "http_host_phase_time_basis": "local request wrapper to headers, then headers to terminal including cleanup; phase CPU unmeasured and omitted; not DNS/connect/TLS/server or kernel/WAN separation; decoded response bytes, not wire bytes",
                "request_dispatch_clock_contract_version": REQUEST_DISPATCH_CLOCK_CONTRACT_VERSION,
                "request_start_clock_basis": "publisher-serialized dispatch commitment; exact timestamp shared by pacer and metrics, not socket/kernel/WAN arrival",
                "owner_elapsed_seconds": elapsed,
                "owner_cpu_seconds": cpu,
                "stages": stages, "providers": providers,
                "stage_wall_time_basis": "summed calls, including waits/failures; parallel stage times may overlap",
                "throughput_basis": "durably committed records / timed owner wall from queue initialization, including inventory, waits and idle; interpreter/module startup is in process benchmark wall"}
