"""Isolate evidenced gateway failures without bypassing publisher-wide limits.

Only 502/504 without Retry-After qualify. A different recently successful host
of the SAME publisher corroborates isolation. Older, not subsequently failed
host observations permit only PROVISIONAL isolation; their errors remain in
the publisher's capacity pressure. Unknown/correlated failures, refusals,
overload and route failures retain publisher-wide protection. Pauses are bounded,
durable and automatically expire. Repeated failed probes back off only this
host; a completed successful response immediately clears its pause. None of
this accepts an article body.
The canonical HTTP owner calls this under its existing lock; no queue writer.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_metrics import (
    GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION, GATEWAY_HOST_ISOLATION_CONTRACT_VERSION,
    MAX_REQUEST_HOSTS_PER_PROVIDER,
)
from downloader.news_fulltext_policy import NewsError, normalize_url


CONTRACT_VERSION = GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION
NEGATIVE_EVIDENCE_SCOPE = 'provider_exact_host_gateway_failure'
CORROBORATION_SECONDS = 120
PAUSE_SECONDS = 30
MAX_PAUSE_SECONDS = 300
MAX_PAUSE_GENERATION = 5


def pause_seconds(generation):
    """A failed host's retry floor, never a healthy publisher's request pace."""
    if type(generation) is not int or not 1 <= generation <= MAX_PAUSE_GENERATION:
        raise ValueError('invalid_gateway_pause_generation')
    return min(MAX_PAUSE_SECONDS, PAUSE_SECONDS * (1 << (generation - 1)))


class GatewayHostCircuit:
    def __init__(self, root: Path, registry, *, threshold=3):
        self.registry, self.threshold = registry, threshold
        self.path = root / 'gateway_host_cooldowns.json'
        self.successes, self.failures, self.paused, self.pause_generations = {}, {}, {}, {}
        # Originating negative evidence, NOT current body/parser identity.
        # Another host's checkpoint or a parser update must not relabel it.
        self.pause_evidence_fingerprints = {}
        try:
            if self.path.stat().st_size > 1 << 20:
                return
            saved = json.loads(self.path.read_text())
            version = saved.get('contract_version')
            # Preserve v1/v2's exact unexpired deadline. They did not retain a
            # retry generation; do not invent their earlier failed probes.
            if type(version) is not int or version not in {1, 2, 3, CONTRACT_VERSION}:
                return
            now = time.time()
            for provider, rows in saved['providers'].items():
                source = registry.sources.get(provider)
                if not source or not source.allowed or not isinstance(rows, dict) or len(rows) > MAX_REQUEST_HOSTS_PER_PROVIDER:
                    continue
                for host, row in rows.items():
                    try:
                        restored = self._restore_row(provider, host, row, version, now)
                    except (ValueError, KeyError, TypeError, AttributeError, NewsError):
                        # One corrupt host must not erase other valid deadlines.
                        continue
                    if restored is None:
                        continue
                    until, generation, failures, fingerprint = restored
                    self.paused.setdefault(provider, {})[host] = until
                    self.pause_generations.setdefault(provider, {})[host] = generation
                    self.failures.setdefault(provider, {})[host] = failures
                    self.pause_evidence_fingerprints.setdefault(provider, {})[host] = fingerprint
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            pass

    def _restore_row(self, provider, host, row, version, now):
        if not isinstance(host, str) or not 1 <= len(host) <= 253 or not isinstance(row, dict):
            return None
        url = 'https://' + host + '/'
        # Keep EXACT observed hostname; no userinfo, port, path/query, case or
        # Unicode alias can transfer a technical pause to another authority.
        if normalize_url(url) != url or urlsplit(url).netloc != host:
            return None
        matched = self.registry.match(url)
        until, fingerprint = row['not_before_epoch'], row['source_data_fingerprint']
        if (not matched or matched.provider != provider
                or not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint)
                or type(until) not in (int, float) or not math.isfinite(until)):
            return None
        if version == CONTRACT_VERSION and row.get('evidence_scope') != NEGATIVE_EVIDENCE_SCOPE:
            return None
        # These failures concern this provider/host, not the body parser.
        # Current permission, robots and shared limits are still independently
        # enforced by NewsHTTP BEFORE every real request. No success is restored.
        if version in {1, 2}:
            # Legacy state proves only an unexpired bounded pause, not an old
            # failed-probe generation or exact historical error count.
            return ((until, 1, self.threshold, fingerprint)
                    if now < until <= now + PAUSE_SECONDS + 5 else None)
        generation, failures = row.get('pause_generation'), row.get('failure_count')
        if (type(generation) is not int or not 1 <= generation <= MAX_PAUSE_GENERATION
                or type(failures) is not int or failures < self.threshold
                or not 0 <= until <= now + pause_seconds(generation) + 5):
            return None
        # Expiry permits a real probe; it is not host recovery and must not
        # reset the evidenced failure streak, including across parser updates.
        return until, generation, failures, fingerprint

    def blocked(self, provider):
        now = time.time()
        return {host: deadline for host, deadline in self.paused.get(provider, {}).items() if deadline > now}

    def remaining(self, provider, host):
        return max(0.0, self.paused.get(provider, {}).get(host, 0) - time.time())

    def _checkpoint(self):
        atomic_write_json(self.path, {'contract_version': CONTRACT_VERSION, 'providers': {
            provider: {host: {'not_before_epoch': deadline,
                'source_data_fingerprint': self.pause_evidence_fingerprints[provider][host],
                'evidence_scope': NEGATIVE_EVIDENCE_SCOPE,
                'pause_generation': self.pause_generations.get(provider, {}).get(host, 1),
                'failure_count': max(self.threshold, self.failures.get(provider, {}).get(host, 0))}
                for host, deadline in rows.items()}
            for provider, rows in self.paused.items() if rows}}, durable=True)

    def success(self, provider, host):
        if host is None:
            return False
        self.failures.setdefault(provider, {}).pop(host, None)
        # A later completed 2xx/3xx response is actual host recovery evidence.
        # It need not wait for a previously opened pause to expire; global
        # refusals/robots/provider limits remain enforced independently.
        self.pause_generations.get(provider, {}).pop(host, None)
        recovered = self.paused.get(provider, {}).pop(host, None) is not None
        rows = self.successes.setdefault(provider, {})
        if host in rows or len(rows) < MAX_REQUEST_HOSTS_PER_PROVIDER:
            rows[host] = time.monotonic()
        if recovered:
            self.pause_evidence_fingerprints.get(provider, {}).pop(host, None)
            self._checkpoint()
        return recovered

    def failure(self, provider, host):
        """A later timeout/gateway failure invalidates old healthy-host proof.

        Called only for failed requests under the canonical HTTP lock. No I/O,
        success-path work, or change to route/TLS/publisher breaker semantics.
        """
        if host is None:
            return None
        rows = self.failures.setdefault(provider, {})
        if host not in rows and len(rows) >= MAX_REQUEST_HOSTS_PER_PROVIDER:
            return None
        rows[host] = rows.get(host, 0) + 1
        return rows[host]

    def corroboration(self, provider, host, *, excluded_hosts=()):
        """Read bounded live evidence, not restored health or permission.

        Called under the HTTP lock only on a failure. A redirected REP probe
        excludes both its initial authority and the actual failed-response host.
        Completion, age and unrecovered-failure checks are shared with ordinary
        gateway isolation; headers, elapsed cooldowns and old successes do not
        establish another host's current health.
        """
        now = time.monotonic()
        rows = self.failures.get(provider, {})
        active = self.blocked(provider)
        excluded = {host, *excluded_hosts}
        other = {h: now - checked for h, checked in self.successes.get(provider, {}).items()
                 if h not in excluded and now >= checked and not rows.get(h) and h not in active}
        current = sorted(h for h, age in other.items() if age <= CORROBORATION_SECONDS)
        unresolved = sorted(h for h in set(rows) | set(active)
                            if h not in excluded and (rows.get(h) or h in active))
        return {'corroborating_hosts': current, 'other_success_ages_seconds': other,
                'unrecovered_other_failed_hosts': unresolved}

    def gateway_failure(self, provider, host, status, retry_after):
        if host is None or status not in {502, 504} or retry_after is not None:
            return None
        rows = self.failures.setdefault(provider, {})
        failure_count = self.failure(provider, host)
        if failure_count is None:
            return None
        active = self.blocked(provider)
        # A previous success is NOT corroboration after that host itself failed
        # or entered a durable pause. success() clears its failure streak only
        # after another successful HTTP response, not after a timer expires.
        evidence = self.corroboration(provider, host)
        other, current, unresolved = (evidence[key] for key in (
            'other_success_ages_seconds', 'corroborating_hosts', 'unrecovered_other_failed_hosts'))
        if current:
            scope, basis = 'host', 'different_recent_successful_host'
        elif other and not unresolved:
            # A quiet host's old success is not proof of current health, nor
            # proof it has failed. Pause the evidenced failed host, retaining
            # ALL these errors in adaptive pressure and allowing no health or
            # throughput claim from this classification.
            scope, basis = 'provisional_host', 'older_success_no_observed_other_gateway_failure'
        else:
            scope, basis = 'publisher', ('other_unrecovered_host_failure'
                                         if unresolved else 'no_other_success_observation')
        observation = {
            'contract_version': GATEWAY_HOST_ISOLATION_CONTRACT_VERSION, 'scope': scope, 'basis': basis,
            'source_data_fingerprint': self.registry.sources[provider].data_fingerprint,
            'authorization_fingerprint': self.registry.sources[provider].authorization_fingerprint,
            'failed_host': host, 'failure_count': rows[host],
            'corroborating_hosts': sorted(current), 'other_success_ages_seconds': other,
            'unrecovered_other_failed_hosts': unresolved, 'pause_open': False,
            'host_pause_not_before_epoch': active.get(host),
            'host_pause_generation': self.pause_generations.get(provider, {}).get(host, 0),
            'host_pause_seconds': (pause_seconds(self.pause_generations.get(provider, {}).get(host, 1))
                                   if host in active else 0),
            'host_pause_started_at_epoch': (active[host] - pause_seconds(
                self.pause_generations.get(provider, {}).get(host, 1)) if host in active else None),
        }
        if scope != 'publisher' and rows[host] >= self.threshold:
            if host not in active and len(active) >= MAX_REQUEST_HOSTS_PER_PROVIDER:
                observation.update(scope='publisher', basis='bounded_host_capacity_exhausted')
                return observation
            generations = self.pause_generations.setdefault(provider, {})
            # Late responses from the already in-flight cohort do not restart
            # the timer or count as another failed recovery probe.
            if host not in active:
                generation = min(MAX_PAUSE_GENERATION, generations.get(host, 0) + 1)
                generations[host] = generation
                self.paused.setdefault(provider, {})[host] = time.time() + pause_seconds(generation)
                self.pause_evidence_fingerprints.setdefault(provider, {})[host] = self.registry.sources[provider].data_fingerprint
                self._checkpoint()
                observation['pause_open'] = True
            observation.update(host_pause_not_before_epoch=self.paused[provider][host],
                host_pause_generation=generations.get(host, 1),
                host_pause_seconds=pause_seconds(generations.get(host, 1)),
                host_pause_started_at_epoch=self.paused[provider][host] - pause_seconds(generations.get(host, 1)))
        return observation


def host_filtered_query(statement, parameters, excluded_hosts, *, excluded_origins=()):
    """Filter before LIMIT in every article lane; retain its original index.

    No active host pause means byte-identical SQL and no extra lookup. Numbered
    binds support the existing UNION undated lane without duplicated arguments.
    Host strings are bound, never SQL interpolation. No observations are changed.
    """
    if not excluded_hosts and not excluded_origins:
        return statement, parameters
    if len(excluded_hosts) > MAX_REQUEST_HOSTS_PER_PROVIDER or any(type(h) is not str for h in excluded_hosts):
        raise ValueError('bounded_excluded_hosts_required')
    from downloader.news_fulltext_robots import MAX_ROBOTS_ORIGINS_PER_PROVIDER, policy_origin
    if (len(excluded_origins) > MAX_ROBOTS_ORIGINS_PER_PROVIDER
            or any(type(o) is not str or policy_origin(o + '/') != o for o in excluded_origins)):
        raise ValueError('bounded_exact_excluded_origins_required')
    number = iter(range(1, len(parameters) + 1))
    numbered = re.sub(r'\?(?![0-9])', lambda match: '?' + str(next(number)), statement)
    values, filters = list(parameters), []
    if excluded_hosts:
        binds = ','.join('?' + str(len(values) + i + 1) for i in range(len(excluded_hosts)))
        values.extend(excluded_hosts)
        filters.append('host NOT IN (' + binds + ')')
    if excluded_origins:
        checks = ['instr(url,?' + str(len(values) + i + 1) + ')=1' for i in range(len(excluded_origins))]
        values.extend(origin + '/' for origin in excluded_origins)
        filters.append('NOT (' + ' OR '.join(checks) + ')')
    return numbered.replace('WHERE ', 'WHERE ' + ' AND '.join(filters) + ' AND '), tuple(values)
