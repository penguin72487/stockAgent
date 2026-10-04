"""Read-only FinMind throughput windows and a reproducible queue baseline.

Request starts, monitored responses, and finalized partitions are distinct.
Never calls a provider or scans the historical Parquet tree.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import inspect
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import tempfile
import time
from unittest.mock import patch

import pyarrow.parquet as pq

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.finmind_scheduling import incremental_reservation
from downloader import finmind_retry_cohorts as cohorts


def read_db(path: Path):
    return closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2))


def audit(root: Path, now: datetime, minutes: float) -> dict:
    start = now - timedelta(minutes=minutes)
    with read_db(root / 'request_traffic.sqlite3') as conn:
        calls = conn.execute('SELECT started_at_utc,dataset FROM requests '
                             'WHERE started_at_utc>=? AND started_at_utc<=? ORDER BY started_at_utc',
                             (start.isoformat(), now.isoformat())).fetchall()
    times = [datetime.fromisoformat(stamp) for stamp, _ in calls]
    gaps = sorted((b - a).total_seconds() for a, b in zip(times, times[1:]))
    datasets = {}
    for _, dataset in calls:
        datasets[dataset] = datasets.get(dataset, 0) + 1
    owners = {}
    for owner in ('complement', 'sponsor'):
        with read_db(root / owner / 'queue.sqlite3') as conn:
            totals = conn.execute('SELECT dataset,state,count(*),sum(rows) FROM tasks GROUP BY 1,2').fetchall()
            attempts = conn.execute('SELECT dataset,state,count(*),sum(rows) FROM tasks '
                                    'WHERE last_attempt_at_utc>=? AND last_attempt_at_utc<=? GROUP BY 1,2',
                                    (start.isoformat(), now.isoformat())).fetchall()
        with read_db(root / owner / 'update_observations.sqlite3') as conn:
            checks = conn.execute('SELECT dataset,kind,metadata_json FROM checks '
                                  'WHERE checked_at_utc>=? AND checked_at_utc<=?',
                                  (start.isoformat(), now.isoformat())).fetchall()
        outcomes = {}
        for dataset, kind, metadata in checks:
            value = json.loads(metadata)
            item = outcomes.setdefault(dataset + ':' + kind, {'checks': 0, 'response_rows': 0})
            item['checks'] += 1
            item['response_rows'] += value.get('response_rows') or 0
        owners[owner] = {'queue_totals': totals, 'last_attempt_partition_counts': attempts,
                         'monitored_outcomes': outcomes}
    return {'schema_version': 1, 'observed_at_utc': now.isoformat(), 'window_start_at_utc': start.isoformat(),
            'window_minutes': minutes, 'request_starts': len(calls), 'requests_by_dataset': datasets,
            'request_gap_median_seconds': statistics.median(gaps) if gaps else None,
            'request_gap_p95_seconds': gaps[int(.95 * (len(gaps) - 1))] if gaps else None,
            'gaps_over_2s': sum(gap > 2 for gap in gaps),
            'seconds_in_gaps_over_2s': sum(gap for gap in gaps if gap > 2), 'owners': owners,
            'provider_calls': 0, 'queue_writes': 0, 'parquet_history_scans': 0,
            'caveats': ['Request starts do not prove successful responses.',
                        'Latest queue attempts can collapse repeated attempts of one key.',
                        'Only refresh/recent partitions have update-observation histories.',
                        'Finalized partition rows are not necessarily net-new historical rows.']}


def retained_history(root: Path, baseline: dict | None = None) -> dict:
    """Check small immutable receipt heads, not the historical data tree."""
    owner = root / 'complement'
    records = []
    with read_db(owner / 'queue.sqlite3') as conn:
        if baseline is None:
            selected = conn.execute("SELECT dataset,data_id,partition FROM tasks WHERE kind='id_history' "
                                    "AND state='failed' AND error_code=? AND rows>0 ORDER BY dataset,data_id",
                                    (cohorts.ERROR,)).fetchall()
        else:
            selected = [(row['dataset'], row['data_id'], row['partition']) for row in baseline['records']]
        if not 0 < len(selected) <= 1000:
            raise ValueError('expected 1..1000 retained failed histories')
        for dataset, data_id, partition in selected:
            state, error, rows, relative = conn.execute(
                'SELECT state,error_code,rows,receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                (dataset, data_id, partition)).fetchone()
            path = (owner / relative).resolve()
            if not path.is_relative_to(owner.resolve()) or path.stat().st_size > 1024**2:
                raise ValueError('unsafe or oversized receipt')
            body = path.read_bytes()
            receipt = json.loads(body)
            if baseline is not None:
                parquet = (owner / receipt['parquet_path']).resolve()
                assert parquet.is_relative_to(owner.resolve())
                assert worker._sha256(parquet) == receipt['sha256']
                assert pq.read_metadata(parquet).num_rows == receipt['rows']
            records.append({'dataset': dataset, 'data_id': data_id, 'partition': partition,
                            'state': state, 'error_code': error, 'rows': rows,
                            'receipt_body_sha256': hashlib.sha256(body).hexdigest(),
                            'parquet_sha256': receipt.get('sha256'), 'receipt_rows': receipt.get('rows')})
        gates = cohorts.summary(conn)
    if baseline is not None:
        for old, new in zip(baseline['records'], records):
            if new['state'] in {'failed', 'retry_exhausted'}:
                assert new['rows'] == old['rows']
                assert new['receipt_body_sha256'] == old['receipt_body_sha256']
                assert new['parquet_sha256'] == old['parquet_sha256']
            else:
                assert new['state'] == 'complete' and new['rows'] > 0, 'failed history disappeared without recovery'
    return {'records': records, 'retained_keys': len(records), 'retry_cohorts': gates,
            'still_failed': sum(row['state'] == 'failed' for row in records),
            'retry_exhausted': sum(row['state'] == 'retry_exhausted' for row in records),
            'receipt_preservation_verified': baseline is not None,
            'parquet_sha_and_footer_verified': baseline is not None,
            'all_history_complete_claim': False, 'provider_calls': 0}


def new_minute_partitions(root: Path, backup: Path, since: datetime, until: datetime) -> dict:
    """Before/after key comparison, not inference from service/HTTP health."""
    with read_db(root / 'complement' / 'queue.sqlite3') as conn:
        conn.execute('ATTACH DATABASE ? AS baseline', (backup.resolve().as_uri() + '?mode=ro',))
        conn.execute('BEGIN')
        row = conn.execute(
            "SELECT count(*),coalesce(sum(t.rows),0) FROM tasks t LEFT JOIN baseline.tasks b "
            "ON b.dataset=t.dataset AND b.data_id=t.data_id AND b.partition=t.partition "
            "WHERE t.dataset='TaiwanStockKBar' AND t.state='complete' AND t.rows>0 "
            "AND t.last_attempt_at_utc BETWEEN ? AND ? AND (b.state IS NULL OR b.state!='complete')",
            (since.astimezone(UTC).isoformat(), until.isoformat())).fetchone()
    return {'dataset': 'TaiwanStockKBar', 'new_nonempty_partitions': row[0], 'new_rows': row[1],
            'since_utc': since.isoformat(), 'until_utc': until.isoformat(),
            'basis': 'completed_key_not_nonempty_in_fixed_baseline_and_finalized_after_deployment',
            'whole_history_completeness_proven': False}


def benchmark(root: Path, backup: Path, baseline_source: str, now: datetime, repetitions: int,
              delegated: tuple[str, ...] | None = None) -> dict:
    namespace = dict(vars(worker))
    exec(compile(baseline_source, '<captured-deployed-selector>', 'exec'), namespace)
    # Freeze ownership too. Reading a new live status at an older snapshot's
    # clock makes a healthy Sponsor appear future-dated, changing the benchmark.
    delegated = frozenset(delegated if delegated is not None else
                          worker._sponsor_delegated(root / 'complement', datetime.now(UTC)))
    measurements = {}
    signature = worker._sha256(backup)
    with tempfile.TemporaryDirectory(prefix='finmind-selector-planner-') as folder:
        target = Path(folder) / 'queue.sqlite3'
        with read_db(backup) as source, closing(sqlite3.connect(target)) as output:
            source.backup(output)
        began = time.perf_counter()
        candidate_connection = worker._db(target)
        measurements['planner_initialization_wall_seconds'] = time.perf_counter() - began
        with read_db(backup) as baseline_connection, closing(candidate_connection) as candidate_connection:
            pairs = (('baseline', namespace['_next_task'], baseline_connection),
                     ('candidate', worker._next_task, candidate_connection))
            for label, fn, conn in pairs:
                elapsed = []
                cpu = []
                selected = []
                for _ in range(repetitions):
                    started = time.perf_counter()
                    cpu_started = time.process_time()
                    task = fn(conn, now, delegated=delegated)
                    cpu.append(time.process_time() - cpu_started)
                    elapsed.append(time.perf_counter() - started)
                    selected.append(vars(task) if task else None)
                measurements[label] = {'wall_seconds': elapsed, 'median_wall_seconds': statistics.median(elapsed),
                                        'cpu_seconds': cpu, 'median_cpu_seconds': statistics.median(cpu),
                                        'selected': selected}
            with patch.object(worker, 'atomic_write_json', lambda *args, **kwargs: None):
                began = time.perf_counter()
                worker._status(candidate_connection, root / 'complement', state='diagnostic_no_write', delegated=delegated)
                measurements['status_wall_seconds'] = time.perf_counter() - began
    assert signature == worker._sha256(backup), 'fixed_baseline_was_modified'
    began = time.perf_counter()
    plan = incremental_reservation(root, now)
    measurements['reservation'] = {'wall_seconds': time.perf_counter() - began,
                                   'reserve': plan['reserve_requests']}
    measurements.update(schema_version=2, baseline_preserved=True,
                        provider_calls=0, production_queue_writes=0,
                        delegated_datasets=sorted(delegated),
                        selector_parity=measurements['baseline']['selected'] == measurements['candidate']['selected'])
    return measurements


def benchmark_local_cycle(root: Path, baseline: dict, repetitions: int = 3, *,
                          planner_only: bool = False) -> dict:
    """Same queue/clock; retain all seeding, validation and published summaries.

    Compare the captured selector and the discarded seed-summary calculation
    with the optimized selector and summary-free seeding. Both sides use the
    same new publication clock, isolating CPU changes from scheduling policy.
    This is the full local dispatch cycle, NOT HTTP/storage/provider throughput.
    """
    from downloader import finmind_supplemental
    now = datetime.fromisoformat(baseline['observed_at_utc'])
    namespace = dict(vars(worker))
    exec(compile(baseline['selector_source'], '<captured-selector>', 'exec'), namespace)
    selectors = {'baseline': namespace['_next_task'], 'candidate': worker._next_task}
    delegated = frozenset(baseline['benchmark']['delegated_datasets'])
    seed = finmind_supplemental.seed
    measurements = {label: {'wall_seconds': [], 'cpu_seconds': [], 'output_hashes': []}
                    for label in selectors}
    initial_signature = worker._sha256(Path(baseline['queue_backup']))
    with tempfile.TemporaryDirectory(prefix='finmind-local-cycle-') as temporary:
        for repeat in range(repetitions):
            # Balance ordering to avoid calling every baseline cold and every
            # candidate warm. Every side starts from an independent exact DB.
            for label in (('baseline', 'candidate') if repeat % 2 == 0 else ('candidate', 'baseline')):
                target = Path(temporary) / f'{repeat}-{label}.sqlite3'
                with read_db(Path(baseline['queue_backup'])) as source, closing(sqlite3.connect(target)) as output:
                    source.backup(output)
                def seeded(*args, **kwargs):
                    kwargs['include_status'] = label == 'baseline' and not planner_only
                    return seed(*args, **kwargs)
                wall, cpu = time.perf_counter(), time.process_time()
                # Do not initialize the baseline with today's planner fix: it
                # would erase the very missing-statistics cost being measured.
                conn = (sqlite3.connect(target, timeout=10) if label == 'baseline' else worker._db(target))
                with closing(conn) as conn, \
                        patch.object(worker, '_now', lambda: now), \
                        patch.object(worker, '_next_task', selectors[label]), \
                        patch.object(worker, 'atomic_write_json', lambda *a, **k: None), \
                        patch.object(finmind_supplemental, 'seed', seeded):
                    if label == 'baseline':
                        conn.execute('PRAGMA journal_mode=WAL')
                        conn.execute('PRAGMA busy_timeout=10000')
                    worker._populate(conn, root / 'complement', today=now.astimezone(worker.TAIPEI).date())
                    result = worker._status(conn, root / 'complement', state='diagnostic_no_write', last={}, delegated=delegated)
                    selected = []
                    # A real normal batch admits 120 requests, with status
                    # refreshes retained at its existing approximate cadence.
                    for index in range(120):
                        task = worker._next_task(conn, now, delegated=delegated)
                        selected.append(vars(task) if task else None)
                        if index % 30 == 29:
                            result = worker._status(conn, root / 'complement', state='diagnostic_no_write', last={}, delegated=delegated)
                    measurements[label]['cpu_seconds'].append(time.process_time() - cpu)
                    measurements[label]['wall_seconds'].append(time.perf_counter() - wall)
                    # The selector doesn't mutate this queue; compare its full
                    # seeded state AND every published frontier/count/clock.
                    digest = hashlib.sha256(json.dumps({'status': result, 'selected': selected},
                                                       sort_keys=True, separators=(',', ':')).encode())
                    # Stream the exact rows; full-queue parity must not require
                    # a second hundreds-of-MiB Python list/JSON materialization.
                    for query in ('SELECT * FROM tasks ORDER BY dataset,data_id,partition',
                                  'SELECT * FROM finmind_source_frontiers ORDER BY dataset,data_id'):
                        digest.update(query.encode())
                        for row in conn.execute(query):
                            digest.update(json.dumps(row, separators=(',', ':')).encode() + b'\n')
                    measurements[label]['output_hashes'].append(digest.hexdigest())
    for item in measurements.values():
        item.update(median_wall_seconds=statistics.median(item['wall_seconds']),
                    median_cpu_seconds=statistics.median(item['cpu_seconds']))
    assert initial_signature == worker._sha256(Path(baseline['queue_backup']))
    parity = measurements['baseline']['output_hashes'] == measurements['candidate']['output_hashes']
    return {'schema_version': 2, 'scope': 'queue_open_local_seed_120_selectors_five_full_status_publications_not_network_or_data_io',
            'planner_only': planner_only, 'baseline_preserved': True,
            'same_publication_policy': True, 'output_parity': parity, 'measurements': measurements,
            'provider_calls': 0, 'production_queue_writes': 0, 'production_receipt_writes': 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--minutes', type=float, default=60)
    parser.add_argument('--since', type=datetime.fromisoformat)
    parser.add_argument('--capture', action='store_true')
    parser.add_argument('--benchmark-baseline', type=Path)
    parser.add_argument('--benchmark-local-cycle', type=Path)
    parser.add_argument('--benchmark-planner', type=Path,
                        help='Compare only planner maintenance, preserving the captured old schema/statistics')
    parser.add_argument('--capture-retained-history', action='store_true')
    parser.add_argument('--verify-retained-baseline', type=Path)
    parser.add_argument('--compare-minute-backup', type=Path)
    args = parser.parse_args()
    if not 0 < args.minutes <= 1440:
        parser.error('minutes must be positive and at most 1440')
    now = datetime.now(UTC)
    if args.since is not None and (args.since.tzinfo is None or args.since >= now):
        parser.error('since must be a past timezone-aware timestamp')
    minutes = (now - args.since).total_seconds() / 60 if args.since else args.minutes
    if not 0 < minutes <= 1440:
        parser.error('window must be positive and at most 1440 minutes')
    report = audit(args.root, now, minutes)
    if args.capture_retained_history or args.verify_retained_baseline:
        baseline = (json.loads(args.verify_retained_baseline.read_bytes())['retained_history']
                    if args.verify_retained_baseline else None)
        report['retained_history'] = retained_history(args.root, baseline)
    if args.compare_minute_backup:
        if args.since is None:
            parser.error('minute backup comparison requires --since')
        report['new_minute_partitions'] = new_minute_partitions(args.root, args.compare_minute_backup, args.since, now)
    if args.capture:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        backup = args.output.parent / 'complement_baseline.sqlite3'
        if backup.exists():
            parser.error('baseline already exists; use another output folder')
        with read_db(args.root / 'complement' / 'queue.sqlite3') as source, closing(sqlite3.connect(backup)) as target:
            source.backup(target)
        report['selector_source'] = inspect.getsource(worker._next_task)
        report['queue_backup'] = str(backup.resolve())
        report['benchmark'] = benchmark(args.root, backup, report['selector_source'], now, 5)
    if args.benchmark_baseline:
        old = json.loads(args.benchmark_baseline.read_bytes())
        report['benchmark'] = benchmark(args.root, Path(old['queue_backup']), old['selector_source'],
                                        datetime.fromisoformat(old['observed_at_utc']), 7,
                                        old.get('benchmark', {}).get('delegated_datasets'))
    if args.benchmark_local_cycle:
        old = json.loads(args.benchmark_local_cycle.read_bytes())
        report['local_cycle_benchmark'] = benchmark_local_cycle(args.root, old)
    if args.benchmark_planner:
        if args.benchmark_local_cycle:
            parser.error('choose either benchmark-local-cycle or benchmark-planner')
        old = json.loads(args.benchmark_planner.read_bytes())
        report['local_cycle_benchmark'] = benchmark_local_cycle(args.root, old, planner_only=True)
    atomic_write_json(args.output, report)
    if report.get('local_cycle_benchmark', {}).get('output_parity') is False:
        raise ValueError('local_cycle_output_parity_failed')
    print({'output': str(args.output), 'request_starts': report['request_starts'],
           'observed_at_utc': now.isoformat(), 'provider_calls': 0})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
