"""Replay complete resident discovery seeding on one read-only WAL snapshot.

Only TEMP copies of discovery/settings/counters are writable. The original
collector, HTTP owner, article heads and source clocks remain untouched.
Includes every configured provider, not only the measured slow SQL statement.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import sys
import time
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config, seed_discovery
from downloader.news_fulltext_cnyes_seed import (
    PLANNED_QUERY, PLANNED_SCOPE_INDEX_NAME, PLANNED_SCOPE_INDEX_SQL,
)
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_queue import DISCOVERY_PARSER_CONTRACT_INDEX_SQL, NewsQueue


class _ScratchTransaction:
    """Keep canonical nested transactions inside the benchmark savepoint."""
    def __init__(self, db):
        self.connection = db

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _digest(db):
    digest = hashlib.sha256()
    for table, order in (('discovery', 'url'), ('queue_settings', 'key'), ('counters', 'key')):
        digest.update(table.encode())
        for row in db.execute(f'SELECT * FROM temp.{table} ORDER BY {order}'):
            digest.update(json.dumps(tuple(row), ensure_ascii=False, separators=(',', ':')).encode())
            digest.update(b'\n')
    return digest.hexdigest()


def benchmark(root, registry, *, repetitions=2, max_seconds=180, index_axis='parser_contract'):
    if (type(repetitions) is not int or not 1 <= repetitions <= 3
            or type(max_seconds) is not int or not 10 <= max_seconds <= 300):
        raise ValueError('bounded_discovery_maintenance_benchmark_required')
    if index_axis not in {'parser_contract', 'cnyes_planned_scope'}:
        raise ValueError('unknown_discovery_maintenance_index_axis')
    candidate_name, candidate_sql = (
        ('discovery_parser_contract', DISCOVERY_PARSER_CONTRACT_INDEX_SQL)
        if index_axis == 'parser_contract' else (PLANNED_SCOPE_INDEX_NAME, PLANNED_SCOPE_INDEX_SQL))
    deadline = time.monotonic() + max_seconds
    db = sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    # TEMP copies are bounded before allocation; do not copy the article lake.
    db.execute('PRAGMA temp_store=MEMORY')
    db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
    started_wall, started_cpu = time.perf_counter(), time.process_time()
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM main.queue_settings WHERE key='current_owner_id'").fetchone()
        rows, approximate_bytes = db.execute('''SELECT count(*),coalesce(sum(length(url)+length(provider)
            +coalesce(length(etag),0)+coalesce(length(last_modified),0)+coalesce(length(error_code),0)
            +coalesce(length(response_sha256),0)+coalesce(length(parser_fingerprint),0)
            +coalesce(length(last_checked_at_utc),0)+coalesce(length(superseded_by_windows_json),0)+128),0)
            FROM main.discovery''').fetchone()
        if rows > 500000 or approximate_bytes > 256 << 20:
            raise ValueError('discovery_scratch_memory_budget_exceeded')
        for table in ('discovery', 'queue_settings', 'counters'):
            ddl = db.execute("SELECT sql FROM main.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
            db.execute(ddl.replace('CREATE TABLE', 'CREATE TEMP TABLE', 1))
            db.execute(f'INSERT INTO temp.{table} SELECT * FROM main.{table}')
        # Preserve all preexisting physical indexes except the candidate.
        for row in db.execute("SELECT name,sql FROM main.sqlite_master WHERE type='index' AND tbl_name='discovery' AND sql IS NOT NULL").fetchall():
            if row['name'] == candidate_name:
                continue
            db.execute(row['sql'].replace('CREATE INDEX ' + row['name'], 'CREATE INDEX temp.' + row['name'], 1))

        def prohibit_source_writes(action, arg1, arg2, database, trigger):
            if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE) and database != 'temp':
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(prohibit_source_writes)
        seed_wall = time.perf_counter() - started_wall
        seed_cpu = time.process_time() - started_cpu
        observed = datetime.now(timezone.utc)
        if index_axis == 'parser_contract':
            query = ("SELECT url FROM discovery WHERE provider=? AND kind='index' AND error_code IS NULL "
                     "AND parser_fingerprint IS NOT NULL AND parser_fingerprint!=?")
            query_parameters = ('cnyes', 'invalid')
        else:
            source = registry.sources.get('cnyes')
            if source is None or not source.cnyes_apis:
                raise ValueError('cnyes_planned_scope_requires_configured_endpoints')
            base = source.cnyes_apis[0]
            prefix = base + '?limit=30&page='
            query, query_parameters = PLANNED_QUERY, {
                'prefix': prefix, 'prefix_length': len(prefix),
                'lower': base + '?', 'upper': base + '@'}
        plans, samples, digests = {}, [], set()
        baseline_digest = _digest(db)
        index_bytes = index_wall = index_cpu = None
        for repetition in range(repetitions):
            for method in (('legacy', 'indexed') if repetition % 2 == 0 else ('indexed', 'legacy')):
                db.execute('SAVEPOINT replay')
                try:
                    if method == 'indexed':
                        before_bytes = db.execute('PRAGMA temp.page_count').fetchone()[0] * db.execute('PRAGMA temp.page_size').fetchone()[0]
                        wall, cpu = time.perf_counter(), time.process_time()
                        db.execute(candidate_sql.replace('EXISTS ' + candidate_name,
                                                         'EXISTS temp.' + candidate_name))
                        index_wall = max(index_wall or 0, time.perf_counter()-wall)
                        index_cpu = max(index_cpu or 0, time.process_time()-cpu)
                        index_bytes = max(index_bytes or 0, db.execute('PRAGMA temp.page_count').fetchone()[0]
                                          * db.execute('PRAGMA temp.page_size').fetchone()[0] - before_bytes)
                    plans[method] = [row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + query, query_parameters)]
                    view = NewsQueue.__new__(NewsQueue)
                    view.root, view.db, view.registry = root, _ScratchTransaction(db), registry
                    view.metrics = NewsMetrics()
                    # These immutable cohorts are already checked by a resident
                    # owner's initializer; this is maintenance, not cold recovery.
                    view._validated_finite_supplements = True
                    view._validated_finite_cohorts = set(registry.sources)
                    with patch('downloader.news_fulltext_queue.utc_now', return_value=observed.isoformat()):
                        wall, cpu = time.perf_counter(), time.process_time()
                        seed_discovery(view)
                        elapsed, used_cpu = time.perf_counter()-wall, time.process_time()-cpu
                    digest = _digest(db)
                    digests.add(digest)
                    samples.append({'method': method, 'wall_seconds': elapsed, 'cpu_seconds': used_cpu,
                                    'complete_discovery_settings_counters_sha256': digest})
                finally:
                    db.execute('ROLLBACK TO replay')
                    db.execute('RELEASE replay')
                if _digest(db) != baseline_digest:
                    raise ValueError('scratch_replay_did_not_restore_pinned_input')
        medians = {method: {key: statistics.median(row[key] for row in samples if row['method'] == method)
                            for key in ('wall_seconds', 'cpu_seconds')} for method in ('legacy', 'indexed')}
        return {'accepted': len(digests) == 1, 'observed_at_utc': observed.isoformat(),
                'index_axis': index_axis, 'candidate_index_name': candidate_name,
                'owner_id_at_snapshot': owner[0] if owner else None, 'discovery_rows': rows,
                'estimated_discovery_row_bytes': approximate_bytes, 'configured_providers': len(registry.sources),
                'scratch_preparation_wall_seconds': seed_wall, 'scratch_preparation_cpu_seconds': seed_cpu,
                'index_build_wall_seconds': index_wall, 'index_build_cpu_seconds': index_cpu,
                'index_allocated_bytes': index_bytes, 'query_plans': plans, 'samples': samples, 'medians': medians,
                'exact_complete_maintenance_results_equal': len(digests) == 1,
                'publisher_requests': 0, 'source_states_mutated': False, 'source_schema_mutated': False,
                'all_history_complete': False,
                'scope': 'AB/BA complete resident seed_discovery on all configured providers; TEMP discovery/settings/counters on one native WAL snapshot; original index predicates, refusals, priority migration and clocks retained; excludes cold cohort recovery, HTTP dispatch, metadata ingest, heartbeat and actual main-file index commit'}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--index-axis', choices=('parser_contract', 'cnyes_planned_scope'),
                        default='parser_contract')
    args = parser.parse_args(argv)
    implementation_sha256 = sha256_file(Path(__file__))
    configuration_sha256 = sha256_file(args.config)
    config, registry, repo = load_config(args.config)
    result = benchmark(repo / config['root'], registry, index_axis=args.index_axis)
    if (sha256_file(Path(__file__)) != implementation_sha256
            or sha256_file(args.config) != configuration_sha256):
        raise ValueError('benchmark_implementation_or_configuration_changed_during_replay')
    result['implementation_sha256'] = implementation_sha256
    result['configuration_sha256'] = configuration_sha256
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('accepted', 'discovery_rows', 'configured_providers', 'index_allocated_bytes', 'medians')}, ensure_ascii=False))
    return int(not result['accepted'])


if __name__ == '__main__':
    raise SystemExit(main())
