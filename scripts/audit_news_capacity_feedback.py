"""Read-only source-work and capacity feedback evidence; never publisher HTTP.

Keep transport recovery separate from body throughput/workload comparisons.
This bounded evidence does not verify bodies, source limits or full history.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_metrics import endpoint_workload
from downloader.news_fulltext_queue import NewsQueue, utc_now


def audit(root, registry, providers):
    if not 1 <= len(providers) <= 32 or len(set(providers)) != len(providers):
        raise ValueError('bounded_distinct_registered_provider_ids_required')
    payload = (root / 'status.json').read_bytes()
    status = json.loads(payload)
    rows = {}
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        for provider in providers:
            source = registry.sources[provider]
            if not source.allowed:
                raise ValueError('enabled_registered_provider_required')
            # No source-state mutation: reuse the exact declared dispatch scope
            # and current endpoint circuits, not legacy queued endpoints.
            circuit = db.execute('SELECT not_before,data_fingerprint FROM bulk_adapter_cooldowns WHERE provider=?',
                                 (provider,)).fetchone()
            allowed = bool(source.cnyes_apis) or not circuit or circuit['data_fingerprint'] != source.data_fingerprint or circuit['not_before'] <= time.time()
            clause, params = NewsQueue.discovery_dispatch_filter(source, allowed)
            ready = [dict(r) for r in db.execute(
                'SELECT kind,count(*) jobs FROM discovery WHERE provider=? AND due<=? '
                'AND superseded_by_windows_json IS NULL AND ' + clause + ' GROUP BY kind',
                (provider, time.time(), *params))]
            deadline = db.execute('SELECT min(due) FROM discovery WHERE provider=? '
                'AND superseded_by_windows_json IS NULL AND ' + clause, (provider, *params)).fetchone()[0]
            published_calibration = status.get('http_capacity', {}).get('calibration') or {}
            calibration = published_calibration.get('providers', {}).get(provider, {})
            history = calibration.get('history', [])[-128:]
            windows, streak = [], 0
            for sample in reversed(history):
                if sample.get('healthy') is not True:
                    break
                streak += 1
            for sample in history:
                families = Counter()
                for endpoint, count in sample.get('endpoint_request_counts', {}).items():
                    families[endpoint_workload(endpoint)] += count
                completed_families = Counter()
                for endpoint, count in sample.get('endpoint_completed_request_counts', {}).items():
                    completed_families[endpoint_workload(endpoint)] += count
                windows.append({k: sample.get(k) for k in (
                    'ended_at_epoch', 'wall_seconds', 'interval_seconds', 'parallelism', 'requests', 'responses',
                    'started_requests', 'request_measurement_basis',
                    'new_stored_url_heads', 'transport_or_server_errors', 'unusable_http_responses',
                    'isolated_gateway_host_errors', 'isolated_robots_origin_errors', 'provisional_gateway_host_errors', 'pressure_transport_or_server_errors',
                    'pressure_request_results', 'pressure_response_results', 'all_observed_hosts_healthy', 'health_scope',
                    'workload_family', 'healthy', 'opening_http_inflight', 'closing_http_inflight')}
                    | {'request_workload_counts': dict(families), 'terminal_workload_counts': dict(completed_families)})
            counts = status.get('performance', {}).get('providers', {}).get(provider, {}).get('counts', {})
            rows[provider] = {'ready_declared_discovery_jobs': ready,
                'next_declared_discovery_at_utc': datetime.fromtimestamp(deadline, timezone.utc).isoformat() if deadline is not None else None,
                'capacity': status.get('http_capacity', {}).get('providers', {}).get(provider, {}),
                'calibration': {k: calibration.get(k) for k in ('state', 'reason', 'workload_family', 'stable_samples', 'transport_stable_samples', 'transport_recovery_active')},
                'measurement_contract_version': published_calibration.get('measurement_contract_version'),
                'consecutive_healthy_transport_windows_ignoring_workload_switch': streak,
                'bounded_windows': windows,
                'endpoint_outcome_counts': {k: v for k, v in counts.items() if k.startswith('http_endpoint_')},
                'aggregate_error_counts': {k: counts.get(k, 0) for k in (
                    'transport_errors', 'http_status_502', 'http_status_503', 'http_status_403', 'http_status_429', 'response_deadline_errors',
                    'http_terminal_isolated_robots_origin_errors',
                    'transient_recovery_pressure_reduced_once', 'transient_recovery_pressure_held')}}
        coherent = bool(owner and owner[0] == status.get('owner_id'))
    finally:
        db.close()
    return {'contract_version': 3, 'observed_at_utc': utc_now(), 'status_snapshot_at_utc': status.get('updated_at_utc'),
            'owner_id': status.get('owner_id'), 'status_database_owner_coherent': coherent,
            'input_status_sha256': hashlib.sha256(payload).hexdigest(), 'providers': rows,
            'publisher_requests': 0, 'source_states_mutated': False, 'body_receipts_verified': False,
            'history_complete': False, 'official_limit_verified': False, 'publish': False,
            'scope': 'bounded existing owner feedback and declared due discovery jobs; not body/history acceptance'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--provider', action='append', help='Default: all enabled registered providers (bounded at 32).')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    providers = args.provider or [p for p, source in registry.sources.items() if source.allowed]
    result = audit(repo / config['root'], registry, providers)
    result['configuration_sha256'] = sha256_file(args.config)
    result['implementation_sha256'] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({'owner_coherent': result['status_database_owner_coherent'], 'providers': {
        p: {'ready_jobs': r['ready_declared_discovery_jobs'], 'calibration': r['calibration'],
            'consecutive_healthy_transport_windows': r['consecutive_healthy_transport_windows_ignoring_workload_switch']}
        for p, r in result['providers'].items()}}, ensure_ascii=False))
    return int(not result['status_database_owner_coherent'])


if __name__ == '__main__':
    raise SystemExit(main())
