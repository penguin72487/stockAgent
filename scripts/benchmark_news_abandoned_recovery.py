"""Read-only paired scan/index benchmark on one real news WAL snapshot.

This measures finding abandoned leases, not receipt/body validation, source
throughput, full-history completeness, or cold-boot recovery. No NewsQueue or
HTTP owner is constructed and no production data/index is changed.
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
from downloader.news_fulltext_queue import ABANDONED_WORK_RECOVERY_SQL, utc_now


def benchmark(root: Path, *, cycles: int = 2):
    if type(cycles) is not int or not 1 <= cycles <= 3:
        raise ValueError('bounded_ab_ba_cycles_required')
    root = root.resolve(strict=True)
    paths = (Path(__file__), Path('downloader/news_fulltext_queue.py'))
    implementation = {str(path): sha256_file(path) for path in paths}
    db = sqlite3.connect('file:' + quote(str(root / 'queue.sqlite3'), safe='/') + '?mode=ro', uri=True)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        index = db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='abandoned_work'").fetchone()
        if not index:
            raise ValueError('canonical_abandoned_work_index_required')
        counts = dict(db.execute('SELECT state,sum(articles) FROM state_metrics GROUP BY state'))
        rows = list(db.execute("SELECT id FROM articles INDEXED BY abandoned_work WHERE state='inflight' LIMIT 4097"))
        if len(rows) > 4096:
            raise ValueError('bounded_inflight_cohort_required')
        expected = sorted(row[0].hex() for row in rows)
        identity = hashlib.sha256(json.dumps(expected).encode()).hexdigest()
        queries = {
            'reference_full_scan': "SELECT id FROM articles NOT INDEXED WHERE state='inflight'",
            'candidate_partial_index': "SELECT id FROM articles INDEXED BY abandoned_work WHERE state='inflight'",
        }
        plans = {name: [row[-1] for row in db.execute('EXPLAIN QUERY PLAN ' + sql)]
                 for name, sql in queries.items()}
        update_plan = [row[-1] for row in db.execute('EXPLAIN QUERY PLAN ' + ABANDONED_WORK_RECOVERY_SQL)]
        changes, results = db.total_changes, []
        for cycle in range(cycles):
            for name in ('reference_full_scan', 'candidate_partial_index',
                         'candidate_partial_index', 'reference_full_scan'):
                callbacks = 0
                def progress():
                    nonlocal callbacks
                    callbacks += 1
                    return 0
                db.set_progress_handler(progress, 1000)
                cpu, wall = time.process_time(), time.perf_counter()
                observed = list(db.execute(queries[name]))
                elapsed, consumed = time.perf_counter() - wall, time.process_time() - cpu
                db.set_progress_handler(None, 0)
                if sorted(row[0].hex() for row in observed) != expected:
                    raise ValueError('same_snapshot_full_cohort_parity_required')
                results.append({'cycle': cycle, 'mode': name, 'wall_seconds': elapsed,
                    'cpu_seconds': consumed, 'rows': len(observed),
                    'sqlite_vm_steps_lower_bound': callbacks * 1000,
                    'sqlite_vm_steps_upper_bound': (callbacks + 1) * 1000 - 1})
        medians = {name: {metric: statistics.median(row[metric] for row in results if row['mode'] == name)
                         for metric in ('wall_seconds', 'cpu_seconds', 'sqlite_vm_steps_lower_bound',
                                        'sqlite_vm_steps_upper_bound')} for name in queries}
        unchanged = implementation == {str(path): sha256_file(path) for path in paths}
        accepted = (unchanged and db.total_changes == changes and
                    all('abandoned_work' in ' '.join(plan) for plan in (plans['candidate_partial_index'], update_plan)))
        return {'contract_version': 1, 'accepted': accepted, 'observed_at_utc': utc_now(),
            'fixed_single_wal_snapshot': True, 'state_summary_articles': sum(counts.values()),
            'inflight_rows': len(expected), 'inflight_ids_sha256': identity, 'queries': queries,
            'query_plans': plans, 'canonical_recovery_update_plan': update_plan, 'index_sql': index[0],
            'results': results, 'medians': medians,
            'selection_wall_speedup': medians['reference_full_scan']['wall_seconds'] /
                                      medians['candidate_partial_index']['wall_seconds'],
            'production_database_written': False, 'publisher_requests': 0,
            'implementation_unchanged': unchanged, 'implementation_sha256': implementation,
            'all_history_complete': False, 'boot_recovery_verified': False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--cycles', type=int, default=2)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = benchmark(args.root, cycles=args.cycles)
    atomic_write_json(args.output, result)
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
