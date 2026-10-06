"""Finite acceptance probe using the canonical owner, transport and receipts.

Run only while Complement is stopped. At most one already-pending, eligible
day per selected registered dataset is fetched. This is not a new collector or
a change to the background history order; due-refresh quota remains reserved.
No credentials, signed URLs or raw observations enter the acceptance report.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import fcntl
import os
from pathlib import Path
import shutil

import requests

from downloader import download_finmind_complement as worker
from downloader import finmind_storage_objects as objects
from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import incremental_reservation, protected_stock_opening
from downloader.finmind_supplemental import SOURCES, _eligible_anchor


def verify(root: Path, datasets: tuple[str, ...], output: Path) -> dict:
    if not 0 < len(datasets) <= len(objects.OBJECT_FIRST) or set(datasets) - objects.OBJECT_FIRST.keys():
        raise ValueError('finite_registered_object_datasets_required')
    root = root.resolve()
    result = {'schema_version': 1, 'purpose': 'finite_source_transport_acceptance_not_history_completeness',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'items': [],
              'state': 'running', 'maximum_data_api_calls': len(datasets)}
    load_env_file(Path(__file__).resolve().parents[1] / '.env', allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    with (root / 'worker.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(worker._db(root / 'queue.sqlite3')) as conn, conn, requests.Session() as session:
            if not set(datasets) <= objects.active_datasets(conn):
                raise ValueError('canonical_object_plan_must_be_activated_first')
            for dataset in datasets:
                now = datetime.now(UTC)
                account = verified_account(session, token, root.parent)
                if account['tier'] != 'SponsorPro':
                    raise ValueError('verified_sponsor_pro_required')
                reservation = incremental_reservation(root.parent, now)
                budget = backfill_budget(account, root.parent,
                                         fixed_incremental_requests=reservation['reserve_requests'], now=now)
                if not budget['allowed'] or reservation['queue_errors']:
                    result['state'] = 'waiting_quota'
                    break
                if protected_stock_opening(now) or shutil.disk_usage(root).free < worker.MIN_FREE_BYTES:
                    result['state'] = 'resource_guard'
                    break
                anchor = _eligible_anchor(SOURCES[dataset], now).isoformat()
                row = conn.execute('SELECT dataset,data_id,partition,kind,priority,state FROM tasks '
                                   "WHERE dataset=? AND data_id='' AND state IN ('pending','failed') "
                                   'AND partition<=? AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?) '
                                   'ORDER BY partition DESC LIMIT 1', (dataset, anchor, now.isoformat())).fetchone()
                if not row:
                    result['items'].append({'dataset': dataset, 'state': 'no_eligible_pending_object'})
                    continue
                task = worker.Task(*row)
                worker._status(conn, root, state='running', active=task)
                try:
                    value, metadata = objects.fetch(session, rate_limiter(account), root, task, token,
                                                   heartbeat=lambda: worker._status(conn, root, state='running', active=task))
                    receipt = worker._store(root, task, value, datetime.now(UTC), request_metadata=metadata)
                    worker._save_result(conn, task, receipt, datetime.now(UTC))
                    last = {'dataset': dataset, 'data_id': '', 'partition': task.partition,
                            'status': receipt['status'], 'rows': receipt['rows'],
                            'observed_at_utc': datetime.now(UTC).isoformat()}
                    worker._status(conn, root, state='running', last=last)
                    result['items'].append({**last, 'bytes': receipt.get('parquet_size_bytes', 0),
                                            'sha256': receipt.get('sha256'),
                                            'receipt_path': receipt['receipt_path'],
                                            'data_api_calls': metadata['request_count'],
                                            'signed_file_gets': metadata['signed_transfer_requests'],
                                            'transfer_validation_store_seconds': metadata.get('transfer_and_validation_seconds')})
                except worker.SourceError as error:
                    worker._save_failure(conn, task, error, datetime.now(UTC))
                    result['items'].append({'dataset': dataset, 'partition': task.partition,
                                            'status': 'failed', 'error_code': error.code})
                atomic_write_json(output, result)
            if result['state'] == 'running':
                result['state'] = ('accepted' if len(result['items']) == len(datasets)
                                   and all(item.get('status') == 'complete' for item in result['items']) else 'partial')
            result['completed_at_utc'] = datetime.now(UTC).isoformat()
            atomic_write_json(output, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--dataset', action='append', choices=sorted(objects.OBJECT_FIRST), required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.root, tuple(dict.fromkeys(args.dataset)), args.output)
    print({'state': result['state'], 'datasets': len(result['items']), 'output': str(args.output)})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
