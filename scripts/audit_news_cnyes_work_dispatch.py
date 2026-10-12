"""Pinned read-only measurements of Cnyes candidate scheduling, not deduplication.

Measure the production ready partial index and owner telemetry. No publisher
requests, queue ownership, source mutation, body export or unique-story claim.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import json
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_work import CONTRACT_VERSION, LOOKUP_DISCOVERY_VERSION, PROGRESS_VERSION, STATES, canonical_candidate_id, progress, statement
from downloader.news_fulltext_host_circuit import host_filtered_query
from downloader.news_fulltext_queue import utc_now


def desktop_inventory(db):
    """One pinned backlog audit, not a heartbeat scan or an alias/body proof."""
    counts = {'supported_variant_urls': 0, 'matching_candidate_ids': 0,
              'unclassified_supported_variant_urls': 0, 'available_primary_body_hints': 0,
              'availability_hint_mismatches': 0}
    for row in db.execute("SELECT a.url,a.cnyes_canonical_candidate_id,a.cnyes_known_body_candidate,p.body_sha256 AS primary_body "
        "FROM articles a INDEXED BY dispatch_cnyes_body_value LEFT JOIN articles p "
        "ON p.id=a.cnyes_canonical_candidate_id AND p.provider='cnyes' "
        "WHERE a.provider='cnyes' AND a.host='news.cnyes.com' AND a.body_sha256 IS NULL "
        "AND a.state IN ('pending','retry','retry_cooldown')"):
        expected = canonical_candidate_id(row['url'])
        if expected is None:
            continue
        counts['supported_variant_urls'] += 1
        if row['cnyes_canonical_candidate_id'] != expected:
            counts['unclassified_supported_variant_urls'] += 1
            continue
        counts['matching_candidate_ids'] += 1
        known = bool(row['primary_body'])
        counts['available_primary_body_hints'] += known
        counts['availability_hint_mismatches'] += row['cnyes_known_body_candidate'] != known
    return {**counts, 'classification_complete_for_current_frontier': counts['unclassified_supported_variant_urls'] == 0,
            'scope': 'desktop variants with active unleased missing-body work; not full history, equivalence or acceptance',
            'classification_is_source_acceptance': False}


def audit(root, *, repetitions=64):
    if type(repetitions) is not int or not 1 <= repetitions <= 256:
        raise ValueError('bounded_index_lookup_repetitions_required')
    status = json.loads((root / 'status.json').read_text())
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0]
        contracts = dict(db.execute("SELECT key,value FROM queue_settings WHERE key IN "
            "('cnyes_body_value_dispatch_contract','cnyes_native_lookup_discovery_contract','cnyes_body_work_metrics_version')"))
        loaded_contracts_match = (contracts.get('cnyes_body_value_dispatch_contract') == str(CONTRACT_VERSION)
                                 and contracts.get('cnyes_native_lookup_discovery_contract') == str(LOOKUP_DISCOVERY_VERSION)
                                 and contracts.get('cnyes_body_work_metrics_version') == str(PROGRESS_VERSION))
        native_groups = []
        if loaded_contracts_match:
            native_groups = [dict(row) for row in db.execute("SELECT a.state,count(*) candidates,"
                "sum(a.body_sha256 IS NOT NULL) stored_native_heads,sum(a.metadata_date_raw!='') nonempty_raw_metadata_dates "
                "FROM cnyes_native_lookup_candidates l JOIN articles a ON a.id=l.candidate_article_id GROUP BY a.state")]
        observed = utc_now()
        started, cpu = time.monotonic(), time.process_time()
        groups = [dict(r) for r in db.execute("SELECT state,cnyes_known_body_candidate AS candidate,count(*) AS urls "
            "FROM articles INDEXED BY dispatch_cnyes_body_value WHERE provider='cnyes' AND body_sha256 IS NULL "
            "AND state IN ('pending','retry','retry_cooldown') GROUP BY state,cnyes_known_body_candidate")]
        ready_cost = {'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu}
        active_groups = groups + [dict(row) for row in db.execute("SELECT state,cnyes_known_body_candidate AS candidate,count(*) AS urls "
            "FROM articles INDEXED BY dispatch WHERE provider='cnyes' AND state='inflight' AND body_sha256 IS NULL "
            "GROUP BY state,cnyes_known_body_candidate")]
        work_progress = progress(db) if loaded_contracts_match else None
        reconciled = (work_progress is not None and work_progress['states'] ==
                      sorted(active_groups, key=lambda row: (row['state'], row['candidate'])))
        started, cpu = time.monotonic(), time.process_time()
        if loaded_contracts_match:
            for _ in range(repetitions):
                progress(db)
        aggregate_cost = {'lookup_repetitions': repetitions,
            'seconds_per_lookup': (time.monotonic() - started) / repetitions,
            'cpu_seconds_per_lookup': (time.process_time() - cpu) / repetitions,
            'source_table_scanned_per_read': False}
        desktop = desktop_inventory(db)
        measurements = []
        now = time.time()
        paused = (status.get('http_capacity') or {}).get('providers', {}).get('cnyes', {}).get('temporarily_paused_gateway_hosts', {})
        excluded = tuple(host for host, deadline in paused.items() if deadline > now)
        for state in STATES:
            for known in (0, 1):
                query, args = host_filtered_query(statement(), ('cnyes', state, now, known), excluded)
                plan = [r['detail'] for r in db.execute('EXPLAIN QUERY PLAN ' + query, args)]
                started, cpu = time.monotonic(), time.process_time()
                for _ in range(repetitions):
                    db.execute(query, args).fetchone()
                measurements.append({'state': state, 'existing_body_candidate': bool(known), 'query_plan': plan,
                    'indexed_without_full_scan_or_sort': any('SEARCH articles USING INDEX dispatch_cnyes_body_value' in r for r in plan)
                        and not any('TEMP B-TREE' in r or 'SCAN articles' in r for r in plan),
                    'lookup_repetitions': repetitions,
                    'seconds_per_lookup': (time.monotonic() - started) / repetitions,
                    'cpu_seconds_per_lookup': (time.process_time() - cpu) / repetitions})
        perf = status.get('performance', {})
        counts = perf.get('providers', {}).get('cnyes', {}).get('counts', {})
        primary = counts.get('frontier_no_known_body_candidate_dispatches', 0)
        candidate = counts.get('frontier_existing_body_candidate_dispatches', 0)
        coherent = owner == status.get('owner_id') and status.get('activity') not in {'stopped', 'error'}
        return {'contract_version': 4, 'work_dispatch_contract_version': CONTRACT_VERSION,
            'native_lookup_discovery_contract_version': LOOKUP_DISCOVERY_VERSION,
            'loaded_work_contracts_match': loaded_contracts_match,
            'native_lookup_work_by_state': native_groups,
            'lookup_targets_require_independent_source_acceptance': True,
            'database_snapshot_at_utc': observed, 'queue_owner_id': owner, 'status_owner_id': status.get('owner_id'),
            'status_updated_at_utc': status.get('updated_at_utc'), 'owner_status_coherent': coherent,
            'missing_body_work_by_candidate': groups, 'ready_inventory_cost': ready_cost,
            'body_work_progress': work_progress, 'body_work_progress_reconciled': reconciled,
            'progress_read_cost': aggregate_cost,
            'desktop_variant_inventory': desktop,
            'lookup_measurements': measurements,
            'owner_html_dispatches': {'no_known_body_candidate': primary, 'existing_body_candidate': candidate},
            'temporarily_excluded_gateway_hosts': excluded,
            'owner_classification_stage': perf.get('stages', {}).get('cnyes_body_candidate_backfill'),
            'classification_counters': {k: v for k, v in status.get('counters', {}).items()
                if k.startswith(('cnyes_candidate_', 'cnyes_existing_body_', 'cnyes_native_lookup_'))},
            'publisher_requests': 0, 'source_states_mutated': False, 'publish': False,
            'historical_completeness_verified': False, 'unique_story_inventory_verified': False,
            'candidate_body_equivalence_verified': False,
            'scope': 'known missing-body URL work, candidate hints and current-owner telemetry; not unique stories or source completeness',
            'accepted': coherent and loaded_contracts_match and reconciled and primary + candidate > 0
                and desktop['availability_hint_mismatches'] == 0
                and all(m['indexed_without_full_scan_or_sort'] for m in measurements)}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, _, repo = load_config(args.config)
    result = audit(repo / config['root'])
    result['implementation_sha256'] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print({k: result[k] for k in ('accepted', 'missing_body_work_by_candidate', 'ready_inventory_cost', 'owner_html_dispatches', 'owner_classification_stage')})
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
