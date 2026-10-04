"""Compare historical request plans on one disposable SQLite backup.

The baseline is the pre-optimization ID x calendar-date candidate formula,
not a fabricated download benchmark. Mutations occur on the backup only;
nonempty source tasks/receipts must remain byte-for-byte identical there.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import tempfile
import time

from downloader import download_finmind_complement as worker
from downloader import finmind_supplemental as supplemental
from downloader.artifact_io import atomic_write_json
from downloader.download_finmind_sponsor import _official_session_calendar


def nonempty_task_digest(connection: sqlite3.Connection) -> dict:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute('SELECT * FROM tasks WHERE rows>0 ORDER BY dataset,data_id,partition'):
        digest.update((json.dumps(tuple(row), separators=(',', ':')) + '\n').encode())
        count += 1
    return {'task_count': count, 'sha256': digest.hexdigest()}


def queued(connection: sqlite3.Connection) -> dict[str, int]:
    return dict(connection.execute(
        "SELECT dataset,COUNT(*) FROM tasks WHERE state IN ('pending','failed','inflight') GROUP BY dataset"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=5)
    args = parser.parse_args()
    if not 3 <= args.repetitions <= 20:
        parser.error('repetitions must be 3..20')
    now = datetime.now(UTC)
    proof = _official_session_calendar()
    if proof is None:
        raise ValueError('receipt_verified_calendar_unavailable')
    source_path = args.root / 'complement' / 'queue.sqlite3'
    sources = supplemental.SOURCES
    with tempfile.TemporaryDirectory(prefix='finmind-history-plan-') as folder:
        path = Path(folder) / 'queue.sqlite3'
        with sqlite3.connect(source_path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2) as live:
            with sqlite3.connect(path) as copy:
                live.backup(copy)
        with worker._db(path) as connection:
            original_tasks = connection.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]
            original_nonempty = nonempty_task_digest(connection)
            universes: dict[str, set[str]] = {}
            for dataset, identifier in connection.execute('SELECT dataset,data_id FROM finmind_source_frontiers'):
                spec = sources.get(dataset)
                if spec and identifier and spec.universe != 'market':
                    universes.setdefault(spec.universe, set()).add(identifier)
            # The old shape enumerated per-ID SpreadTick frontiers. Preserve
            # those archived IDs but do not count the replacement market ID.
            connection.execute("DELETE FROM finmind_source_frontiers WHERE dataset='TaiwanFuturesSpreadTick' AND data_id=''")
            legacy_sources = {**sources, 'TaiwanFuturesSpreadTick': replace(
                sources['TaiwanFuturesSpreadTick'], universe='futures')}
            try:
                supplemental.SOURCES = legacy_sources
                before = supplemental.frontier_status(connection, now)
            finally:
                supplemental.SOURCES = sources
            before_queued = queued(connection)
            if connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_query_shape_migrations'").fetchone():
                # A post-deployment replay reconstructs the old shape from its
                # preserved task evidence, not from its retired state label.
                legacy_rows = connection.execute(
                    'SELECT prior_task_json FROM finmind_query_shape_migrations WHERE version=? AND dataset=?',
                    (supplemental.CONTRACT_VERSION, 'TaiwanFuturesSpreadTick')).fetchall()
                if legacy_rows:
                    before_queued['TaiwanFuturesSpreadTick'] = sum(
                        json.loads(row[0])['state'] in {'pending', 'failed', 'inflight'} for row in legacy_rows)
            connection.execute("DELETE FROM finmind_frontier_universes WHERE dataset='TaiwanFuturesSpreadTick'")
            started, cpu_started = time.perf_counter(), time.process_time()
            after = supplemental.seed(connection, {key: sorted(value) for key, value in universes.items()},
                                      now, official_sessions=proof)
            seed_wall, seed_cpu = time.perf_counter() - started, time.process_time() - cpu_started
            if after.get('TaiwanFuturesSpreadTick', {}).get('known_identifiers') != 1:
                raise ValueError('replacement_market_frontier_not_reconstructed')
            after_queued = queued(connection)
            wall, cpu = [], []
            for _ in range(args.repetitions):
                started, cpu_started = time.perf_counter(), time.process_time()
                supplemental.frontier_status(connection, now)
                wall.append(time.perf_counter() - started)
                cpu.append(time.process_time() - cpu_started)
            preserved_nonempty = nonempty_task_digest(connection)
            if preserved_nonempty != original_nonempty:
                raise ValueError('nonempty_task_receipts_changed')
            datasets = []
            for dataset in sorted(before.keys() | after.keys()):
                old, new = before.get(dataset, {}), after.get(dataset, {})
                old_requests = old.get('raw_unseeded_calendar_candidates', 0) + before_queued.get(dataset, 0)
                new_requests = new.get('unseeded_partition_candidates', 0) + after_queued.get(dataset, 0)
                datasets.append({
                    'dataset': dataset, 'before_candidate_requests': old_requests,
                    'after_candidate_requests': new_requests, 'avoided_candidate_requests': old_requests - new_requests,
                    'after_excluded_cash_calendar_candidates': new.get('excluded_calendar_candidates', 0),
                    'after_already_materialized_candidates': new.get('already_materialized_candidates', 0),
                    'before_known_ids': old.get('known_identifiers'), 'after_known_ids': new.get('known_identifiers'),
                })
            old_total = sum(row['before_candidate_requests'] for row in datasets)
            new_total = sum(row['after_candidate_requests'] for row in datasets)
            report = {
                'schema_version': 1, 'observed_at_utc': now.isoformat(), 'scope': 'fixed_supplemental_day_plan',
                'baseline_definition': 'pre_change_id_times_calendar_dates_plus_physical_unfinished_tasks',
                'task_count': original_tasks, 'datasets': datasets,
                'before_candidate_requests': old_total, 'after_candidate_requests': new_total,
                'avoided_candidate_requests': old_total - new_total,
                'candidate_request_reduction_fraction': 1 - new_total / old_total if old_total else None,
                'calendar_candidates_removed': sum(row['after_excluded_cash_calendar_candidates'] for row in datasets),
                'already_materialized_estimate_only_removed': sum(
                    row['after_already_materialized_candidates'] for row in datasets),
                'savings_boundary': 'calendar_queue_materialization_and_estimate_corrections_are_not_measured_API_call_savings',
                'calendar_receipt_sha256': proof.receipt_sha256,
                'calendar_first': proof.first.isoformat(), 'calendar_last': proof.last.isoformat(),
                'nonempty_task_integrity': preserved_nonempty, 'nonempty_receipts_preserved': True,
                'seed_wall_seconds': seed_wall, 'seed_cpu_seconds': seed_cpu,
                'status_median_wall_seconds': statistics.median(wall),
                'status_median_cpu_seconds': statistics.median(cpu),
                'status_wall_samples': wall, 'production_queue_mutations': 0, 'provider_calls': 0,
                'download_throughput_improvement_proven': False,
                'limitations': 'candidate_plan_not_verified_lifetimes_not_record_count_not_full_source_history',
            }
    atomic_write_json(args.output, report)
    print({'before': old_total, 'after': new_total, 'avoided': old_total - new_total,
           'status_median_seconds': report['status_median_wall_seconds'], 'output': str(args.output)})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
