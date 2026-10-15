"""Pinned read-only AB/BA comparison of every configured Cnyes planned range.

Measure local scope reconstruction, not publisher capacity or whole history.
The original scheduled URLs, refused jobs and superseded parents are included.
Never instantiate a queue writer or issue a publisher request.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import time
from urllib.parse import parse_qs, urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_unseeded_intervals
from downloader.news_fulltext_cnyes_seed import CONTRACT_VERSION, PLANNED_QUERY, planned_intervals


def legacy_intervals(db, base, stats):
    covered = []
    for row in db.execute("SELECT url FROM discovery WHERE url>=? AND url<? AND kind='cnyes_news'",
                          (base + '?', base + '@')):
        query = parse_qs(urlsplit(row[0]).query)
        stats['queries_parsed'] = stats.get('queries_parsed', 0) + 1
        if 'startAt' in query and 'endAt' in query:
            covered.append((int(query['startAt'][0]), int(query['endAt'][0])))
    return sorted(set(covered))


def benchmark(root, source, *, repetitions=2, max_seconds=120):
    if type(repetitions) is not int or not 1 <= repetitions <= 4 or not 10 <= max_seconds <= 300:
        raise ValueError('bounded_scope_maintenance_benchmark_required')
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.execute('PRAGMA query_only=ON')
    deadline = time.monotonic() + max_seconds
    db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
    low = int(datetime.strptime(source.cnyes_history_start, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
    high = int(datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()) - 1
    samples, parity, first = [], True, None
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        total = db.execute("SELECT count(*) FROM discovery WHERE provider=? AND kind='cnyes_news'", (source.provider,)).fetchone()[0]
        for repetition in range(repetitions):
            for method in (('legacy', 'optimized') if repetition % 2 == 0 else ('optimized', 'legacy')):
                wall, cpu, stats, scope_digest, roots_digest, ranges, roots = time.perf_counter(), time.process_time(), {}, hashlib.sha256(), hashlib.sha256(), 0, 0
                for base in source.cnyes_apis:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('scope_maintenance_benchmark_time_budget')
                    covered = legacy_intervals(db, base, stats) if method == 'legacy' else planned_intervals(db, base, statistics=stats)
                    unplanned = cnyes_unseeded_intervals(low, high, covered)
                    scope_digest.update(json.dumps([base, covered], separators=(',', ':')).encode())
                    roots_digest.update(json.dumps([base, unplanned], separators=(',', ':')).encode())
                    ranges += len(covered)
                    roots += len(unplanned)
                identity = (scope_digest.hexdigest(), roots_digest.hexdigest(), ranges, roots)
                if first is None:
                    first = identity
                parity &= first == identity
                samples.append({'method': method, 'wall_seconds': time.perf_counter() - wall,
                                'cpu_seconds': time.process_time() - cpu, 'queries_parsed': stats.get('queries_parsed', 0),
                                'planned_range_count': ranges, 'unplanned_roots': roots,
                                'planned_ranges_sha256': identity[0], 'unplanned_roots_sha256': identity[1]})
        prefix = source.cnyes_apis[0] + '?limit=30&page='
        plans = [row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + PLANNED_QUERY,
                 {'prefix': prefix, 'prefix_length': len(prefix), 'lower': source.cnyes_apis[0] + '?', 'upper': source.cnyes_apis[0] + '@'})]
        medians = {method: {key: statistics.median(r[key] for r in samples if r['method'] == method)
                            for key in ('wall_seconds', 'cpu_seconds', 'queries_parsed')}
                   for method in ('legacy', 'optimized')}
        return {'accepted': parity, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'owner_id_at_snapshot': owner[0] if owner else None, 'maintenance_contract_version': CONTRACT_VERSION,
                'endpoint_count': len(source.cnyes_apis), 'scheduled_discovery_urls': total, 'samples': samples,
                'medians': medians, 'optimized_query_plan': plans, 'exact_planned_ranges_and_new_roots_equal': parity,
                'publisher_requests': 0, 'source_states_mutated': False, 'history_completeness_verified': False,
                'matched_network_throughput_benchmark': False,
                'basis': 'AB/BA local planned-range reconstruction on one pinned native WAL snapshot; DISTINCT is a cold-maintenance operation, not the hot dispatch path'}
    finally:
        db.rollback()
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = benchmark(repo / config['root'], registry.sources['cnyes'])
    result['implementation_sha256'] = sha256_file(Path(__file__))
    result['configuration_sha256'] = sha256_file(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'endpoint_count', 'scheduled_discovery_urls', 'medians')}, ensure_ascii=False))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
