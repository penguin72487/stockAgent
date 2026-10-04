"""Read-only live acceptance of the ordered history queue and public projection.

Provider requests remain owned by the regular worker; this verifier reads only
its SQLite/status/receipts and the metadata-only gateway. No queue is modified.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq
import requests

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.finmind_history_order import HISTORY_STAGES, STAGES, first_unfinished_dataset, metadata
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--since', type=datetime.fromisoformat, required=True)
    parser.add_argument('--base-url', default='http://127.0.0.1:8770')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.since.tzinfo is None:
        parser.error('--since requires timezone')
    now = datetime.now(UTC)
    root = args.root.resolve()
    complement = root / 'complement'
    status = json.loads((complement / 'status.json').read_bytes())
    assert status['dispatch_contract_version'] == worker.DISPATCH_CONTRACT_VERSION and status['history_order'] == metadata()
    assert datetime.fromisoformat(status['observed_at_utc']) >= args.since
    delegated = worker._sponsor_delegated(complement, now)
    with sqlite3.connect((complement / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True, timeout=3) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        first = first_unfinished_dataset(conn, now, delegated=delegated)
        background = worker._next_task(conn, now, delegated=delegated, background_only=True)
        assert first == HISTORY_STAGES[0].dataset, 'unexpected_history_frontier_for_this_acceptance'
        # Normal core repairs or previous-stage due renewals can also preempt.
        # On this unfinished-first-stage queue, later history may NOT win.
        if background and background.dataset in {stage.dataset for stage in HISTORY_STAGES}:
            assert background.dataset == first
        task = conn.execute("SELECT dataset,data_id,partition,rows,receipt_path,last_attempt_at_utc "
                            "FROM tasks WHERE state='complete' AND rows>0 AND last_attempt_at_utc>=? "
                            'ORDER BY last_attempt_at_utc DESC LIMIT 1',
                            (args.since.astimezone(UTC).isoformat(),)).fetchone()
        assert task is not None, 'no_new_nonempty_worker_receipt'
    receipt_path = (complement / task[4]).resolve()
    assert receipt_path.is_relative_to(complement)
    receipt = json.loads(receipt_path.read_bytes())
    parquet = (complement / receipt['parquet_path']).resolve()
    assert parquet.is_relative_to(complement)
    assert worker._sha256(parquet) == receipt['sha256']
    assert receipt['rows'] == task[3] == pq.read_metadata(parquet).num_rows
    response = requests.get(args.base_url.rstrip('/') + '/finmind/api/status', timeout=20)
    response.raise_for_status()
    public = response.json()
    acquisition = public['acquisition']
    assert public['read_only'] and not public['production_control_possible']
    assert acquisition['history_order'] == {**metadata(), 'applied': True}
    ranked = [row for row in public['datasets'] if row.get('history_rank')]
    assert [row['id'] for row in ranked] == [stage.dataset for stage in HISTORY_STAGES]
    assert [row['history_rank'] for row in ranked] == list(range(1, 10))
    assert ranked[-1]['id'] == 'USStockPriceMinute'
    assert acquisition['materialized_tasks'] + acquisition['unseeded_candidate_tasks'] == acquisition['total_tasks']
    eta = json.loads((root / 'eta_status.json').read_bytes())
    assert eta['snapshot_contract_version'] == SNAPSHOT_CONTRACT_VERSION
    assert eta['estimate']['schema_version'] == acquisition['completion_estimate']['schema_version'] == SNAPSHOT_CONTRACT_VERSION
    assert [stage['key'] for stage in acquisition['completion_estimate']['stages']] == [key for key, _ in STAGES]
    assert sum(stage['workload']['planned_requests'] for stage in eta['estimate']['stages']) == eta['workload']['summary']['current_plan_requests']
    assert acquisition['completion_estimate']['state'] not in {'unavailable', 'stale'}
    for row in ranked:
        assert row['checked_partitions'] <= row['target_partitions']
        assert row['complete_partitions'] <= row['checked_partitions']
        assert row['unseeded_partition_candidates'] >= 0
    books = []
    for relative in ('data_tw_microstructure/captures/book_events',
                     'data_tw_index_derivatives_ticks/shioaji_fop_captures/book_events'):
        capture = root.parent / relative
        sample = next(capture.rglob('*.parquet'), None)
        entry = {'root': relative, 'sample_file': str(sample.relative_to(root.parent)) if sample else None,
                 'sample_rows': pq.read_metadata(sample).num_rows if sample else None,
                 'five_level_schema_verified': False, 'whole_market_history_complete': False}
        if sample:
            columns = {f'{side}_{field}_{level}' for side in ('bid', 'ask')
                       for field in ('price', 'volume') for level in range(1, 6)}
            entry['five_level_schema_verified'] = columns <= set(pq.ParquetFile(sample).schema_arrow.names)
        books.append(entry)
    report = {'schema_version': 1, 'observed_at_utc': now.isoformat(), 'passed': True,
              'deployment_since_utc': args.since.astimezone(UTC).isoformat(),
              'dispatch_contract_version': worker.DISPATCH_CONTRACT_VERSION, 'history_order': metadata(),
              'first_unfinished_history': first, 'background_preview': background.dataset if background else None,
              'new_nonempty_receipt': {'dataset': task[0], 'data_id': task[1], 'partition': task[2],
                                       'rows': task[3], 'attempt_at_utc': task[5], 'sha256': receipt['sha256']},
              'eta_contract_version': SNAPSHOT_CONTRACT_VERSION, 'public_health': public['health'],
              'public_generated_at_utc': public['generated_at_utc'],
              'history_progress_rows': [{key: row.get(key) for key in (
                  'id', 'history_rank', 'target_partitions', 'checked_partitions',
                  'complete_partitions', 'unseeded_partition_candidates', 'rows', 'first_data_date', 'last_data_date')}
                  for row in ranked],
              'local_five_level_samples': books,
              'provider_calls': 0, 'queue_writes': 0, 'all_history_complete_claim': False}
    atomic_write_json(args.output, report)
    print({'passed': True, 'first_history': first, 'last_history': ranked[-1]['id'],
           'new_receipt': task[0], 'rows': task[3], 'output': str(args.output)})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
