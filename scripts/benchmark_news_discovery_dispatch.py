"""Read-only paired discovery dispatch on one fixed real WAL snapshot.

The reference is the previous priority-index query without the exact readiness
probe. Every bound, scope filter, result row and order is identical. This is not
a publisher-capacity, cold-boot or whole-history completeness measurement.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import time
from urllib.parse import quote

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import (
    DISCOVERY_READINESS_PROBE_CONTRACT_VERSION, DISCOVERY_READINESS_PROBE_SQL,
    NewsQueue, discovery_selection_sql, utc_now,
)


def benchmark(root, registry, providers, *, cycles=2, limit=17):
    if type(cycles) is not int or not 1 <= cycles <= 3:
        raise ValueError('bounded_ab_ba_cycles_required')
    if type(limit) is not int or not 1 <= limit <= 129:
        raise ValueError('bounded_dispatch_limit_required')
    if (not 1 <= len(providers) <= 32 or len(set(providers)) != len(providers)
            or any(p not in registry.sources or not registry.sources[p].allowed for p in providers)):
        raise ValueError('bounded_distinct_enabled_provider_selection_required')
    paths = (Path(__file__), Path('downloader/news_fulltext_queue.py'))
    implementation = {str(path): sha256_file(path) for path in paths}
    db = sqlite3.connect('file:' + quote(str(root.resolve() / 'queue.sqlite3'), safe='/') + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        index = db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='discovery_due_frontier'").fetchone()
        if not index:
            raise ValueError('canonical_discovery_due_frontier_index_required')
        changes, rows = db.total_changes, {}
        for provider in providers:
            source = registry.sources[provider]
            sql, binds = NewsQueue.discovery_candidates_query(source, limit)
            expected = [dict(r) for r in db.execute(sql, binds)]
            cohort_sha = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
            total, raw_due = db.execute('SELECT count(*),coalesce(sum(due<=?),0) FROM discovery '
                'WHERE provider=? AND superseded_by_windows_json IS NULL', (binds[1], provider)).fetchone()
            plans = {name: [r[-1] for r in db.execute('EXPLAIN QUERY PLAN ' + statement, parameters)]
                for name, statement, parameters in (
                    ('reference_ordered_selection', sql, binds),
                    ('candidate_readiness_probe', DISCOVERY_READINESS_PROBE_SQL, binds[:2]))}
            results = []
            for cycle in range(cycles):
                for mode in ('reference', 'candidate', 'candidate', 'reference'):
                    callbacks = 0
                    def progress():
                        nonlocal callbacks
                        callbacks += 1
                        return 0
                    db.set_progress_handler(progress, 1000)
                    cpu, wall = time.process_time(), time.perf_counter()
                    readiness = (None if mode == 'reference' else
                        len(db.execute(DISCOVERY_READINESS_PROBE_SQL, binds[:2]).fetchall()))
                    ready = mode == 'reference' or bool(readiness)
                    selection = sql if mode == 'reference' else discovery_selection_sql(sql, readiness)
                    observed = [dict(r) for r in db.execute(selection, binds)] if ready else []
                    elapsed, consumed = time.perf_counter() - wall, time.process_time() - cpu
                    db.set_progress_handler(None, 0)
                    if observed != expected:
                        raise ValueError('same_snapshot_full_row_order_parity_required')
                    results.append({'cycle': cycle, 'mode': mode, 'wall_seconds': elapsed, 'cpu_seconds': consumed,
                        'result_rows': len(observed), 'readiness_empty': mode == 'candidate' and not ready,
                        'bounded_readiness_rows': readiness,
                        'selection_index': ('discovery_due_frontier' if ready and mode == 'candidate'
                            and ' INDEXED BY discovery_due_frontier ' in selection else
                            'discovery_ready_order' if ready and ' INDEXED BY discovery_ready_order ' in selection else None),
                        'sqlite_vm_steps_lower_bound': callbacks * 1000,
                        'sqlite_vm_steps_upper_bound': (callbacks + 1) * 1000 - 1})
            medians = {mode: {metric: statistics.median(r[metric] for r in results if r['mode'] == mode)
                for metric in ('wall_seconds', 'cpu_seconds', 'sqlite_vm_steps_lower_bound', 'sqlite_vm_steps_upper_bound')}
                for mode in ('reference', 'candidate')}
            rows[provider] = {'active_discovery_rows': total, 'raw_due_rows': raw_due, 'selected_rows': len(expected),
                'selected_full_rows_sha256': cohort_sha, 'query_plans': plans, 'measurements': results, 'medians': medians,
                'selection_query': sql, 'selection_cutoff_epoch': binds[1], 'exact_row_order_parity': True,
                'dispatch_wall_speedup': medians['reference']['wall_seconds'] / medians['candidate']['wall_seconds']}
        unchanged = implementation == {str(path): sha256_file(path) for path in paths}
        accepted = unchanged and db.total_changes == changes and all(
            'discovery_due_frontier' in ' '.join(row['query_plans']['candidate_readiness_probe']) for row in rows.values())
        return {'contract_version': 1, 'accepted': accepted, 'observed_at_utc': utc_now(),
            'discovery_readiness_probe_contract_version': DISCOVERY_READINESS_PROBE_CONTRACT_VERSION,
            'fixed_single_wal_snapshot': True, 'reference_basis': 'same original priority-index selection; no readiness probe',
            'providers': rows, 'index_sql': index[0], 'publisher_requests': 0, 'production_database_written': False,
            'implementation_sha256': implementation, 'implementation_unchanged': unchanged,
            'all_history_complete': False, 'official_maximum_verified': False, 'cold_boot_verified': False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--provider', action='append')
    parser.add_argument('--cycles', type=int, default=2)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    providers = args.provider or [p for p, source in registry.sources.items() if source.allowed]
    result = benchmark(repo / config['root'], registry, providers, cycles=args.cycles, limit=config['workers'] + 1)
    result['configuration_sha256'] = sha256_file(args.config)
    atomic_write_json(args.output, result)
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
