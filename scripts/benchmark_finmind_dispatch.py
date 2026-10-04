"""Compare queue selectors on one SQLite backup; no provider calls/queue writes."""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import statistics
import subprocess
import tempfile
import time

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json


def measure(selector, connection, now, delegated, repetitions):
    elapsed, cpu = [], []
    statements = []
    for _ in range(repetitions):
        started, cpu_started = time.perf_counter(), time.process_time()
        task = selector(connection, now, delegated=delegated)
        cpu.append(time.process_time() - cpu_started)
        elapsed.append(time.perf_counter() - started)
    connection.set_trace_callback(statements.append)
    selector(connection, now, delegated=delegated)
    connection.set_trace_callback(None)
    plans = []
    for query in statements:
        if ('LIMIT 1' in query and 'sqlite_master' not in query
                and (query.startswith('SELECT') or query.startswith('WITH'))):
            plans.append([row[3] for row in connection.execute('EXPLAIN QUERY PLAN ' + query)])
    return {'median_wall_seconds': statistics.median(elapsed), 'median_cpu_seconds': statistics.median(cpu),
            'samples_wall_seconds': elapsed, 'query_plans': plans,
            'selected_dataset': task.dataset if task else None,
            'selected_priority': task.priority if task else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=9)
    parser.add_argument('--baseline-ref', default='HEAD')
    args = parser.parse_args()
    if not 3 <= args.repetitions <= 100:
        parser.error('repetitions must be 3..100')
    repo = Path(__file__).resolve().parents[1]
    text = subprocess.run(['git', 'show', f'{args.baseline_ref}:downloader/download_finmind_complement.py'],
                          cwd=repo, capture_output=True, text=True, check=True).stdout
    tree = ast.parse(text)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_next_task')
    namespace = dict(vars(worker))
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<baseline-selector>', 'exec'), namespace)
    now = datetime.now(UTC)
    delegated = worker._sponsor_delegated(args.root / 'complement', now)
    source = args.root / 'complement' / 'queue.sqlite3'
    with tempfile.TemporaryDirectory(prefix='finmind-dispatch-benchmark-') as folder:
        copy = Path(folder) / 'queue.sqlite3'
        with sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True, timeout=2) as live:
            with sqlite3.connect(copy) as connection:
                live.backup(connection)
        with sqlite3.connect(copy) as connection:
            # A later run may copy an already deployed schema. Reconstruct the
            # pre-change selector's index surface on this disposable copy only.
            connection.execute('DROP INDEX IF EXISTS idx_finmind_outstanding_dispatch')
            connection.execute('DROP INDEX IF EXISTS idx_finmind_due_refresh')
            connection.commit()
            task_count = connection.execute('SELECT count(*) FROM tasks').fetchone()[0]
            old = measure(namespace['_next_task'], connection, now, delegated, args.repetitions)
        with worker._db(copy) as connection:
            indexed_old = measure(namespace['_next_task'], connection, now, delegated, args.repetitions)
            new = measure(worker._next_task, connection, now, delegated, args.repetitions)
        result = {'schema_version': 1, 'observed_at_utc': now.isoformat(), 'scope': 'fixed_queue_snapshot_dispatch_only',
                  'task_count': task_count, 'baseline_ref': args.baseline_ref, 'baseline': old,
                  'baseline_with_new_indexes': indexed_old, 'candidate': new,
                  'wall_reduction_fraction': 1 - new['median_wall_seconds'] / old['median_wall_seconds'],
                  'cpu_reduction_fraction': 1 - new['median_cpu_seconds'] / old['median_cpu_seconds'],
                  'query_only_wall_reduction_fraction': 1 - new['median_wall_seconds'] / indexed_old['median_wall_seconds'],
                  'production_queue_mutations': 0, 'provider_calls': 0,
                  'download_throughput_improvement_proven': False}
    atomic_write_json(args.output, result)
    print(f"Tasks={task_count}; dispatch median {old['median_wall_seconds']:.4f}s -> "
          f"{new['median_wall_seconds']:.4f}s; wall reduction={result['wall_reduction_fraction']:.2%}; "
          f"CPU reduction={result['cpu_reduction_fraction']:.2%}; provider calls=0; output={args.output}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
