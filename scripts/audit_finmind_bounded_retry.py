"""Inspect bounded retry evidence, or simulate migration on a private backup.

Does not call a provider. Simulation never writes to a live queue. The normal
audit exports every exhausted key and its retained-data metadata, not raw values.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import time

from downloader.artifact_io import atomic_write_json
from downloader import finmind_retry_cohorts as cohorts
from downloader import finmind_retry_policy as policy
from downloader import finmind_eta_work as work
from scripts.audit_finmind_throughput import read_db


def exhausted_keys(conn: sqlite3.Connection) -> list[dict]:
    cursor = conn.execute('SELECT t.dataset,t.data_id,t.partition,t.state,t.error_code,'
                          'r.failures AS consecutive_failures,r.first_failure_at_utc,r.last_failure_at_utc,'
                          'r.exhausted_at_utc,t.rows,t.bytes,t.first_data_date,t.last_data_date,t.receipt_path '
                          'FROM tasks t JOIN finmind_task_retries r USING(dataset,data_id,partition) '
                          'WHERE t.state=? ORDER BY t.dataset,t.data_id,t.partition', (policy.EXHAUSTED,))
    names = [field[0] for field in cursor.description]
    return [dict(zip(names, values), max_consecutive_failures=policy.failure_limit()) for values in cursor]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--simulate-baseline', type=Path)
    parser.add_argument('--verify-retained-baseline', type=Path,
                        help='Verify live exhausted keys against the original captured queue metadata')
    args = parser.parse_args()
    if args.simulate_baseline and args.verify_retained_baseline:
        parser.error('simulation and live verification are separate audit modes')
    now = datetime.now(UTC)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {'observed_at_utc': now.isoformat(), 'provider_calls': 0,
              'live_queue_writes': 0, 'parquet_scans': 0, 'all_history_complete_claim': False}
    if args.simulate_baseline:
        baseline = json.loads(args.simulate_baseline.read_bytes())
        original = Path(baseline['queue_backup'])
        with tempfile.TemporaryDirectory(prefix='finmind-bounded-retry-') as folder:
            clone = Path(folder) / 'queue.sqlite3'
            before, evidence = work._queue(original, 'complement', now)
            assert evidence['state'] == 'observed'
            with read_db(original) as source, closing(sqlite3.connect(clone)) as conn:
                source.backup(conn)
                began, cpu = time.perf_counter(), time.process_time()
                policy.migrate(conn, args.root / 'complement', now)
                cohorts.ensure_schema(conn)
                cohorts.reconcile(conn, now)
                conn.commit()
                report.update(migration_wall_seconds=time.perf_counter() - began,
                              migration_cpu_seconds=time.process_time() - cpu)
                keys = exhausted_keys(conn)
                conn.execute('ATTACH DATABASE ? AS baseline', (original.resolve().as_uri() + '?mode=ro',))
                preserved = 'dataset,data_id,partition,rows,bytes,first_data_date,last_data_date,receipt_path,last_attempt_at_utc,error_code'
                changed = conn.execute(f'SELECT count(*) FROM (SELECT {preserved} FROM tasks '
                                       f'EXCEPT SELECT {preserved} FROM baseline.tasks)').fetchone()[0]
                assert changed == 0
                expected = {(row['dataset'], row['data_id'], row['partition'])
                            for row in baseline['retained_history']['records']}
                assert expected <= {(row['dataset'], row['data_id'], row['partition']) for row in keys}
                report.update(retained_data_and_attempt_metadata_unchanged=True,
                              expected_retained_keys=len(expected), exhausted_tasks=len(keys),
                              retry_cohorts=cohorts.summary(conn))
            after, evidence = work._queue(clone, 'complement', now)
            assert evidence['state'] == 'observed'
            rows_before = {ds: work._observed_row(ds, 'complement', {'query_shape': 'audit'}, value, now)
                           for ds, value in before.items()}
            rows_after = {ds: work._observed_row(ds, 'complement', {'query_shape': 'audit'}, value, now)
                          for ds, value in after.items()}
            report['workload_delta'] = {field: sum(row[field] for row in rows_after.values()) -
                                       sum(row[field] for row in rows_before.values()) for field in (
                                           'current_plan_requests', 'completed_tasks', 'retry_tasks', 'retry_exhausted_tasks')}
            assert report['workload_delta']['completed_tasks'] == 0
    else:
        keys = []
        for owner in ('complement', 'sponsor'):
            with read_db(args.root / owner / 'queue.sqlite3') as conn:
                rows = exhausted_keys(conn)
                keys.extend(dict(row, owner=owner) for row in rows)
                report[owner] = {'retry_policy': policy.summary(conn), 'exhausted_tasks': len(rows)}
        report['exhausted_tasks'] = len(keys)
        if args.verify_retained_baseline:
            baseline = json.loads(args.verify_retained_baseline.read_bytes())
            preserved = 'rows,bytes,first_data_date,last_data_date,receipt_path,last_attempt_at_utc,error_code'
            expected = baseline['retained_history']['records']
            with read_db(Path(baseline['queue_backup'])) as original, read_db(args.root / 'complement/queue.sqlite3') as live:
                live.execute('BEGIN')
                for row in expected:
                    key = (row['dataset'], row['data_id'], row['partition'])
                    query = f'SELECT {preserved} FROM tasks WHERE dataset=? AND data_id=? AND partition=?'
                    old, current = original.execute(query, key).fetchone(), live.execute(query, key).fetchone()
                    assert old is not None and current == old, 'retained_data_or_attempt_metadata_changed'
                    assert live.execute('SELECT state FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                                        key).fetchone()[0] == policy.EXHAUSTED
            report.update(expected_retained_keys=len(expected), retained_data_and_attempt_metadata_unchanged=True)
    if keys:
        report.update(retained_rows=sum(row['rows'] for row in keys),
                      retained_bytes=sum(row['bytes'] for row in keys),
                      min_consecutive_failures=min(row['consecutive_failures'] for row in keys),
                      max_consecutive_failures=max(row['consecutive_failures'] for row in keys),
                      recorded_failures=sum(row['consecutive_failures'] for row in keys),
                      first_data_date=min((row['first_data_date'] for row in keys if row['first_data_date']), default=None),
                      last_data_date=max((row['last_data_date'] for row in keys if row['last_data_date']), default=None))
    if keys:
        with (args.output_dir / 'retry_exhausted_tasks.csv').open('w', newline='', encoding='utf-8-sig') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(keys[0]))
            writer.writeheader()
            writer.writerows(keys)
    report['passed'] = True
    atomic_write_json(args.output_dir / 'audit.json', report)
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
