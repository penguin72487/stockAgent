"""Read-only bounded HTTP phase evidence; no publisher requests or body export.

Headers are a wrapper boundary, not separate DNS, TCP, TLS or server timings.
Immutable private failures survive a successful retry; sampled events are not
proof of all-history coverage or a source's official capacity.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_connection_observation import (
    CONNECTION_BUCKETS, CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION,
    connection_observation_bucket, valid_connection_observation,
)
from downloader.news_fulltext_http import ACCESS_DIAGNOSTIC_HEADERS, NewsHTTP
from downloader.news_fulltext_host_circuit import CORROBORATION_SECONDS, MAX_PAUSE_GENERATION, pause_seconds
from downloader.news_fulltext_metrics import (
    CONNECTION_ORIGIN_ISOLATION_CONTRACT_VERSION,
    GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION, GATEWAY_HOST_ISOLATION_CONTRACT_VERSION,
    HTTP_HOST_PHASE_CONTRACT_VERSION,
    MAX_REQUEST_HOSTS_PER_PROVIDER, ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION,
    ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION,
    TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION, TRANSPORT_PHASES,
    metrics_snapshot_clock,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_robots import policy_origin


EVENT_FIELDS = frozenset({
    'contract_version', 'owner_id', 'provider', 'requested_url', 'host', 'endpoint', 'phase', 'http_status',
    'observed_at_utc', 'request_elapsed_seconds', 'request_to_headers_seconds', 'response_bytes_seen',
    'error_types', 'error_message_retained', 'source_data_fingerprint', 'authorization_fingerprint',
    'diagnostic_only', 'accepted_as_article', 'body_stored', 'publish', 'clock_basis',
})
GATEWAY_EVENT_FIELDS = frozenset({
    'contract_version', 'owner_id', 'provider', 'requested_url', 'status', 'error_code',
    'observed_at_epoch', 'headers', 'body_read', 'diagnostic_only', 'accepted_as_article',
    'publish', 'gateway_recovery',
})
GATEWAY_DECISION_FIELDS = frozenset({
    'contract_version', 'scope', 'basis', 'source_data_fingerprint', 'authorization_fingerprint',
    'failed_host', 'failure_count', 'corroborating_hosts', 'other_success_ages_seconds',
    'unrecovered_other_failed_hosts', 'pause_open', 'host_pause_not_before_epoch',
    'host_pause_generation', 'host_pause_seconds',
    'host_pause_started_at_epoch',
})
ROBOTS_EVENT_FIELDS = (GATEWAY_EVENT_FIELDS - {'gateway_recovery'}) | {'robots_origin_policy'}


def _verify_robots_context(event, provider, registry):
    """Initial policy authority survives redirects; no URL/body permission."""
    context = event['robots_origin_policy']
    origin = context['initial_origin']
    expected = {'contract_version', 'initial_origin', 'scope', 'permission_granted',
                'source_data_fingerprint', 'authorization_fingerprint'}
    if 'capacity_pressure' in context:
        expected.add('capacity_pressure')
    if (set(context) != expected
            or type(context['contract_version']) is not int
            or context['contract_version'] != ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION
            or type(origin) is not str or policy_origin(origin + '/') != origin
            or registry.require(origin + '/').provider != provider
            or context['source_data_fingerprint'] != registry.sources[provider].data_fingerprint
            or context['authorization_fingerprint'] != registry.sources[provider].authorization_fingerprint
            or context['scope'] != 'initial_origin_policy' or context['permission_granted'] is not False):
        raise ValueError('invalid_robots_origin_context')
    if 'capacity_pressure' in context:
        proof = context['capacity_pressure']
        excluded = {urlsplit(origin).hostname, urlsplit(event['requested_url']).hostname}
        if (not isinstance(proof, dict) or set(proof) != {'contract_version', 'isolated', 'basis',
                'corroborating_hosts', 'success_ages_seconds'}
                or type(proof['contract_version']) is not int
                or proof['contract_version'] != ROBOTS_ORIGIN_PRESSURE_CONTRACT_VERSION
                or type(proof['isolated']) is not bool
                or proof['basis'] != 'recent_completed_other_registered_host; not_permission_or_body_acceptance'
                or type(proof['corroborating_hosts']) is not list
                or type(proof['success_ages_seconds']) is not dict):
            raise ValueError('invalid_robots_pressure_context')
        hosts, ages = proof['corroborating_hosts'], proof['success_ages_seconds']
        if (len(hosts) > MAX_REQUEST_HOSTS_PER_PROVIDER or any(type(h) is not str for h in hosts)
                or hosts != sorted(set(hosts)) or set(ages) != set(hosts)
                or proof['isolated'] != bool(hosts)
                or any(h in excluded or urlsplit('https://' + h + '/').hostname != h
                       or registry.require('https://' + h + '/').provider != provider
                       or not _finite_nonnegative(ages[h]) or ages[h] > CORROBORATION_SECONDS for h in hosts)):
            raise ValueError('invalid_robots_pressure_corroboration')
    return origin


def _verify_robots_event(path, event, provider, registry):
    origin = _verify_robots_context(event, provider, registry)
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    if not (set(event) == ROBOTS_EVENT_FIELDS and event['contract_version'] == 2
            and path.name.endswith('-' + digest + '.json')
            and stat.S_IMODE(path.stat().st_mode) == 0o600
            and stat.S_IMODE(path.parent.stat().st_mode) == 0o700
            and registry.require(event['requested_url']).provider == provider and event['provider'] == provider
            and type(event['status']) is int and event['status'] in {502, 504}
            and event['error_code'] == 'http_' + str(event['status'])
            and _finite_nonnegative(event['observed_at_epoch'])
            and type(event['headers']) is dict and set(event['headers']) <= ACCESS_DIAGNOSTIC_HEADERS
            and 'retry-after' not in event['headers'] and event['headers'].get('cf-mitigated', '').lower() != 'challenge'
            and all(type(v) is str and len(v) <= 2048 for v in event['headers'].values())
            and event['body_read'] is False and event['diagnostic_only'] is True
            and event['accepted_as_article'] is False and event['publish'] is False):
        raise ValueError('invalid_robots_origin_decision')
    return {'file_sha256': sha256_file(path), 'object_sha256': digest,
        'initial_origin': origin, 'response_host': urlsplit(event['requested_url']).hostname,
        'scope': 'initial_origin_policy', 'status': event['status'],
        'observed_at_epoch': event['observed_at_epoch'], 'body_accepted': False, 'permission_granted': False,
        'capacity_pressure': event['robots_origin_policy'].get('capacity_pressure')}


def _recent_files(folder, days, maximum):
    """Bound decoded content and memory; enumerate only two exact UTC buckets."""
    def entries():
        for day in days:
            bucket = folder / day
            if not bucket.exists():
                continue
            with os.scandir(bucket) as listing:
                for entry in listing:
                    if entry.name.endswith('.json'):
                        yield Path(entry.path)
    return heapq.nlargest(maximum, entries(), key=lambda path: path.name)


def _finite_nonnegative(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _recent_access_files(folder, maximum):
    """One existing private failure folder; decoded records/memory are bounded.

    Access v1 was not date-bucketed. Keep its evidence and location, reporting
    the bounded current-owner sample rather than rewriting historical events.
    """
    if not folder.exists():
        return []
    with os.scandir(folder) as entries:
        return heapq.nlargest(maximum, (Path(e.path) for e in entries if e.name.endswith('.json')),
                             key=lambda path: path.name)


def _verify_gateway_event(path, event, provider, source, registry):
    """Verify a decision, not health/body acceptance; never export URL/headers."""
    decision = event['gateway_recovery']
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    scope, host = decision['scope'], urlsplit(event['requested_url']).hostname
    ages, current, unresolved = (decision['other_success_ages_seconds'], decision['corroborating_hosts'],
                                 decision['unrecovered_other_failed_hosts'])
    current_from_ages = sorted(h for h, age in ages.items() if age <= CORROBORATION_SECONDS)
    generation, duration, started = (decision['host_pause_generation'], decision['host_pause_seconds'],
                                     decision['host_pause_started_at_epoch'])
    pause_valid = (type(generation) is int and 0 <= generation <= MAX_PAUSE_GENERATION
        and type(duration) is int and (duration == 0 or generation > 0 and duration == pause_seconds(generation))
        and (duration == 0 and started is None or duration > 0 and _finite_nonnegative(started)
             and _finite_nonnegative(decision['host_pause_not_before_epoch'])
             and started <= event['observed_at_epoch'] + 1
             and abs(decision['host_pause_not_before_epoch'] - started - duration) <= 1e-6)
        and (not decision['pause_open'] or duration > 0))
    valid = (set(event) == GATEWAY_EVENT_FIELDS and set(decision) == GATEWAY_DECISION_FIELDS
        and event['contract_version'] == 2 and decision['contract_version'] == GATEWAY_HOST_ISOLATION_CONTRACT_VERSION
        and path.name.endswith('-' + digest + '.json') and stat.S_IMODE(path.stat().st_mode) == 0o600
        and stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        and registry.require(event['requested_url']).provider == provider and event['provider'] == provider
        and type(event['status']) is int and event['status'] in {502, 504}
        and event['error_code'] == 'http_' + str(event['status'])
        and _finite_nonnegative(event['observed_at_epoch'])
        and set(event['headers']) <= ACCESS_DIAGNOSTIC_HEADERS and 'retry-after' not in event['headers']
        and all(type(v) is str and len(v) <= 2048 for v in event['headers'].values())
        and event['body_read'] is False and event['diagnostic_only'] is True
        and event['accepted_as_article'] is False and event['publish'] is False
        and decision['source_data_fingerprint'] == source.data_fingerprint
        and decision['authorization_fingerprint'] == source.authorization_fingerprint
        and decision['failed_host'] == host and scope in {'host', 'provisional_host', 'publisher'}
        and type(decision['failure_count']) is int and decision['failure_count'] > 0
        and type(decision['pause_open']) is bool
        and pause_valid
        and (decision['host_pause_not_before_epoch'] is None or _finite_nonnegative(decision['host_pause_not_before_epoch']))
        and type(ages) is dict and len(ages) <= MAX_REQUEST_HOSTS_PER_PROVIDER
        and all(_finite_nonnegative(age) for age in ages.values())
        and type(current) is list and current == current_from_ages
        and type(unresolved) is list and len(unresolved) <= MAX_REQUEST_HOSTS_PER_PROVIDER
        and len(unresolved) == len(set(unresolved)) and not set(ages) & set(unresolved)
        and all(type(h) is str and h != host and registry.require('https://' + h + '/').provider == provider
                for h in set(ages) | set(unresolved))
        and (scope != 'host' or bool(current) and decision['basis'] == 'different_recent_successful_host')
        and (scope != 'provisional_host' or bool(ages) and not current and not unresolved
             and decision['basis'] == 'older_success_no_observed_other_gateway_failure')
        and (scope != 'publisher' or decision['basis'] in {
            'other_unrecovered_host_failure', 'no_other_success_observation', 'bounded_host_capacity_exhausted'})
        and (decision['basis'] != 'other_unrecovered_host_failure' or bool(unresolved) and not current)
        and (decision['basis'] != 'no_other_success_observation' or not ages and not unresolved))
    if not valid:
        raise ValueError('invalid_gateway_decision')
    return {'file_sha256': sha256_file(path), 'object_sha256': digest, 'host': host, 'scope': scope,
        'basis': decision['basis'], 'observed_at_epoch': event['observed_at_epoch'], 'status': event['status'],
        'failure_count': decision['failure_count'], 'pause_open': decision['pause_open'],
        'host_pause_generation': generation, 'host_pause_seconds': duration,
        'host_pause_started_at_epoch': started,
        'corroborating_hosts': current, 'other_success_ages_seconds': ages,
        'unrecovered_other_failed_hosts': unresolved, 'body_accepted': False}


def _verify_event(path, event, provider, source, registry):
    """Never propagate URL, body, headers or exception messages to the report."""
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    stamp = datetime.fromisoformat(event['observed_at_utc'])
    elapsed, headers = event['request_elapsed_seconds'], event['request_to_headers_seconds']
    phase, status = event['phase'], event['http_status']
    host = urlsplit(event['requested_url']).hostname
    robots = 'robots_origin_policy' in event
    connection = 'connection_observation' in event
    if robots:
        _verify_robots_context(event, provider, registry)
    expected_fields = EVENT_FIELDS | ({'robots_origin_policy'} if robots else set())
    if connection:
        expected_fields |= {'connection_observation'}
        if not valid_connection_observation(event['connection_observation'], request_wall=elapsed):
            raise ValueError('invalid_connection_observation')
    valid = (set(event) == expected_fields
        and path.name.endswith('-' + digest + '.json')
        and stat.S_IMODE(path.stat().st_mode) == 0o600
        and stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        and stamp.tzinfo is not None and stamp.strftime('%Y%m%d') == path.parent.name
        and event['contract_version'] == TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION
        and event['provider'] == provider and event['host'] == host
        and event['endpoint'] == ('robots' if robots else NewsHTTP.endpoint_kind(source, event['requested_url']))
        and source.data_fingerprint == event['source_data_fingerprint']
        and source.authorization_fingerprint == event['authorization_fingerprint']
        and phase in TRANSPORT_PHASES and _finite_nonnegative(elapsed)
        and (headers is None and status is None and phase == 'awaiting_headers'
             or _finite_nonnegative(headers) and headers <= elapsed and type(status) is int and 100 <= status <= 599)
        and type(event['response_bytes_seen']) is int and event['response_bytes_seen'] >= 0
        and type(event['error_types']) is list and 1 <= len(event['error_types']) <= 8
        and all(type(t) is str and 1 <= len(t) <= 64 and t.isidentifier() for t in event['error_types'])
        and event['diagnostic_only'] is True and event['accepted_as_article'] is False
        and event['body_stored'] is False and event['publish'] is False and event['error_message_retained'] is False)
    if not valid:
        raise ValueError('invalid_failure_observation')
    return {'file_sha256': sha256_file(path), 'object_sha256': digest, 'host': host,
        'phase': phase, 'endpoint': event['endpoint'], 'http_status': status,
        'observed_at_utc': event['observed_at_utc'], 'request_elapsed_seconds': elapsed,
        'request_to_headers_seconds': headers, 'response_bytes_seen': event['response_bytes_seen'],
        'error_types': event['error_types'],
        'connection_observation': event.get('connection_observation'),
        'connection_bucket': connection_observation_bucket(event.get('connection_observation'))}


def _connection_counts_reconciled(counts, hosts, terminal):
    """Every terminal gets one bounded bucket; unknown is not a warm socket."""
    allowed = {'connection_checkout_' + bucket for bucket in CONNECTION_BUCKETS}
    totals = {key: value for key, value in counts.items() if key.startswith('connection_checkout_')}
    if (set(totals) - allowed or any(type(v) is not int or v < 0 for v in totals.values())
            or sum(totals.values()) != terminal):
        return False
    for host in hosts.values():
        values = host['counts']
        selected = {key: values.get(key, 0) for key in allowed}
        if (any(type(v) is not int or v < 0 for v in selected.values())
                or sum(selected.values()) != values['terminal_requests']):
            return False
    return all(sum(host['counts'].get(key, 0) for host in hosts.values()) == value
               for key, value in totals.items())


def audit(root, registry, providers, *, maximum_events=128):
    if not 1 <= len(providers) <= 32 or len(set(providers)) != len(providers):
        raise ValueError('bounded_distinct_registered_providers_required')
    if type(maximum_events) is not int or not 1 <= maximum_events <= 256:
        raise ValueError('bounded_event_sample_required')
    payload = (root / 'status.json').read_bytes()
    status = json.loads(payload)
    now = datetime.now(timezone.utc)
    published = datetime.fromisoformat(status['updated_at_utc'])
    snapshot, clock_verified = metrics_snapshot_clock(status)
    days = [(snapshot - timedelta(days=i)).strftime('%Y%m%d') for i in (0, 1)]
    with sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True) as db:
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
    performance = status.get('performance', {})
    coherent = bool(owner and owner[0] == status.get('owner_id') and status.get('activity') not in {'error', 'stopped'}
        and 0 <= (now - published).total_seconds() <= 120)
    contract_ok = (performance.get('http_host_phase_observation_contract_version') == HTTP_HOST_PHASE_CONTRACT_VERSION
        and performance.get('transport_failure_diagnostic_contract_version') == TRANSPORT_FAILURE_DIAGNOSTIC_CONTRACT_VERSION)
    gateway_contract_ok = performance.get('gateway_host_isolation_contract_version') == GATEWAY_HOST_ISOLATION_CONTRACT_VERSION
    gateway_checkpoint = performance.get('gateway_host_checkpoint_contract_version')
    gateway_checkpoint_ok = (type(gateway_checkpoint) is int
                             and gateway_checkpoint == GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION)
    robots_contract = performance.get('robots_origin_policy_contract_version')
    robots_contract_ok = type(robots_contract) is int and robots_contract == ROBOTS_ORIGIN_POLICY_CONTRACT_VERSION
    connection_contract = performance.get('connection_origin_isolation_contract_version')
    connection_contract_ok = (type(connection_contract) is int
                              and connection_contract == CONNECTION_ORIGIN_ISOLATION_CONTRACT_VERSION)
    reuse_contract = performance.get('connection_reuse_observation_contract_version')
    reuse_absent = 'connection_reuse_observation_contract_version' not in performance
    reuse_contract_ok = (type(reuse_contract) is int
                         and reuse_contract == CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION)
    rows, errors = {}, []
    for provider in providers:
        source = registry.sources[provider]
        if not source.allowed:
            raise ValueError('enabled_registered_provider_required')
        metrics = performance.get('providers', {}).get(provider, {})
        counts, hosts = metrics.get('counts', {}), metrics.get('request_hosts', {})
        terminal = counts.get('http_terminal_requests', 0)
        reconciled = sum(h['counts']['terminal_requests'] for h in hosts.values()) == terminal
        reuse_reconciled = _connection_counts_reconciled(counts, hosts, terminal) if reuse_contract_ok else None
        events, other_owner, later = [], 0, 0
        folder = root / 'transport_problems' / provider
        for path in _recent_files(folder, days, maximum_events):
            try:
                if path.is_symlink() or path.stat().st_size > 65536:
                    raise ValueError('unsafe_diagnostic_file')
                event = json.loads(path.read_bytes())
                if event.get('owner_id') != status.get('owner_id'):
                    other_owner += 1
                    continue
                if datetime.fromisoformat(event['observed_at_utc']) > snapshot:
                    later += 1
                    continue
                if registry.require(event['requested_url']).provider != provider:
                    raise ValueError('wrong_failure_provider')
                if 'robots_origin_policy' in event and not robots_contract_ok:
                    raise ValueError('robots_context_without_loaded_contract')
                if 'connection_observation' in event and not reuse_contract_ok:
                    raise ValueError('connection_context_without_loaded_contract')
                events.append(_verify_event(path, event, provider, source, registry))
            except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, NewsError):
                errors.append({'provider': provider, 'code': 'invalid_private_failure_observation'})
        gateways, robots_events, legacy_access, later_access = [], [], 0, 0
        for path in _recent_access_files(root / 'access_problems' / provider, maximum_events):
            try:
                if path.is_symlink() or path.stat().st_size > 65536:
                    raise ValueError('unsafe_access_diagnostic_file')
                event = json.loads(path.read_bytes())
                if event.get('owner_id') != status.get('owner_id'):
                    legacy_access += 1
                    continue
                if not {'gateway_recovery', 'robots_origin_policy'} & event.keys():
                    continue  # Refusals/other statuses are not gateway-scope evidence.
                if event['observed_at_epoch'] > snapshot.timestamp():
                    later_access += 1
                    continue
                if 'robots_origin_policy' in event:
                    if not robots_contract_ok:
                        raise ValueError('robots_context_without_loaded_contract')
                    robots_events.append(_verify_robots_event(path, event, provider, registry))
                else:
                    gateways.append(_verify_gateway_event(path, event, provider, source, registry))
            except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, NewsError):
                errors.append({'provider': provider, 'code': 'invalid_private_gateway_decision'})
        rows[provider] = {'host_terminal_counts_reconciled': reconciled, 'terminal_requests': terminal,
            'connection_checkout_counts_reconciled': reuse_reconciled,
            'connection_checkout_counts': {k: v for k, v in counts.items() if k.startswith('connection_checkout_')},
            'hosts': hosts, 'recovery_counts': {k: counts.get(k, 0) for k in (
                'transport_errors', 'failed_sessions_reset', 'failed_origin_pools_reset',
                'connection_pool_reset_errors', 'transport_circuit_opened',
                'transport_failure_observations_retained', 'transport_diagnostic_write_errors',
                'isolated_gateway_host_errors', 'isolated_gateway_host_circuit_opened',
                'provisional_gateway_host_errors', 'provisional_gateway_host_circuit_opened',
                'gateway_host_recoveries',
                'gateway_host_checkpoint_write_errors', 'access_diagnostic_write_errors', 'http_circuit_opened',
                'robots_origin_unreachable_headers', 'robots_origin_policy_circuit_opened',
                'http_terminal_isolated_robots_origin_errors',
                'robots_origin_policy_recoveries', 'robots_policy_checkpoint_write_errors')},
            'verified_robots_origin_decisions': robots_events, 'observed_robots_origin_decision_count': len(robots_events),
            'verified_gateway_decisions': gateways, 'observed_gateway_decision_count': len(gateways),
            'gateway_scope_counts': dict(Counter(e['scope'] for e in gateways)),
            'gateway_basis_counts': dict(Counter(e['basis'] for e in gateways)),
            'legacy_or_other_owner_access_files_excluded': legacy_access,
            'after_snapshot_access_files_excluded': later_access,
            'verified_failure_events': events,
            'observed_failure_count': len(events), 'older_owner_files_excluded': other_owner,
            'after_snapshot_files_excluded': later,
            'failure_phases': dict(Counter(e['phase'] for e in events)),
            'failure_hosts': dict(Counter(e['host'] for e in events)),
            'failure_connection_buckets': dict(Counter(e['connection_bucket'] for e in events)),
            'sample_may_exclude_older_events': len(events) + other_owner + later == maximum_events}
    return {'contract_version': 8, 'observed_at_utc': now.isoformat(), 'status_snapshot_at_utc': published.isoformat(),
        'connection_reuse_observation_contract_matches': reuse_contract_ok,
        'connection_reuse_observation_contract_absent_legacy': reuse_absent,
        'gateway_host_checkpoint_contract_matches': gateway_checkpoint_ok,
        'gateway_host_checkpoint_contract_absent_legacy': gateway_checkpoint is None,
        'connection_origin_isolation_contract_matches': connection_contract_ok,
        'connection_origin_isolation_contract_absent_legacy': connection_contract is None,
        'robots_origin_policy_contract_matches': robots_contract_ok,
        'robots_origin_policy_contract_absent_legacy': robots_contract is None,
        'metrics_snapshot_at_utc': snapshot.isoformat(), 'metrics_snapshot_clock_verified': clock_verified,
        'owner_id': status.get('owner_id'), 'status_database_owner_coherent': coherent,
        'phase_contract_matches': contract_ok, 'input_status_sha256': hashlib.sha256(payload).hexdigest(),
        'gateway_scope_contract_matches': gateway_contract_ok,
        'providers': rows, 'errors': errors,
        'accepted': coherent and contract_ok and gateway_contract_ok
                    and (gateway_checkpoint is None or gateway_checkpoint_ok)
                    and (robots_contract is None or robots_contract_ok) and not errors
                    and (reuse_absent or reuse_contract_ok)
                    and (reuse_absent or all(r['connection_checkout_counts_reconciled'] for r in rows.values()))
                    and all(r['host_terminal_counts_reconciled'] for r in rows.values()),
        'sample_scope': 'latest bounded files in two UTC date buckets, current owner, at or before the locked metrics snapshot when verified; legacy uses publication clock; not exhaustive failures or one-to-one terminal event reconciliation',
        'timing_scope': performance.get('http_host_phase_time_basis'),
        'publisher_requests': 0, 'source_states_mutated': False, 'body_receipts_verified': False,
        'all_history_complete': False, 'official_limit_verified': False, 'publish': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--provider', action='append')
    parser.add_argument('--maximum-events', type=int, default=128)
    parser.add_argument('--require-failure-provider', action='append', default=[])
    parser.add_argument('--require-gateway-failure-provider', action='append', default=[])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    providers = args.provider or [p for p, source in registry.sources.items() if source.allowed]
    result = audit(repo / config['root'], registry, providers, maximum_events=args.maximum_events)
    result['required_failures_observed'] = all(result['providers'].get(p, {}).get('observed_failure_count', 0) > 0
                                             for p in args.require_failure_provider)
    result['accepted'] &= result['required_failures_observed']
    result['required_gateway_failures_observed'] = all(
        result['providers'].get(p, {}).get('observed_gateway_decision_count', 0) > 0
        for p in args.require_gateway_failure_provider)
    result['accepted'] &= result['required_gateway_failures_observed']
    result['implementation_sha256'] = sha256_file(Path(__file__))
    result['configuration_sha256'] = sha256_file(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({'accepted': result['accepted'], 'owner_coherent': result['status_database_owner_coherent'],
        'errors': result['errors'], 'providers': {p: {'terminal_requests': r['terminal_requests'],
            'failure_hosts': r['failure_hosts'], 'failure_phases': r['failure_phases'],
            'gateway_scope_counts': r['gateway_scope_counts'], 'recovery': r['recovery_counts']}
            for p, r in result['providers'].items()}}, ensure_ascii=False))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
