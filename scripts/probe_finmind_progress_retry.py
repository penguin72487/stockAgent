"""Bounded source diagnostic using the canonical shared FinMind quota lane.

Reads one failed price identity and its verified baseline; never changes its
queue, prices, receipts or failure state. Persist metadata, not raw responses.
"""
from datetime import UTC, date, datetime, timedelta
import argparse
import json
import os
from pathlib import Path
import sqlite3

import requests

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.finmind_account import rate_limiter, verified_account
from downloader.finmind_history_refresh import read_baseline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    root = repo / 'data_finmind'
    now = datetime.now(UTC)
    with sqlite3.connect((root / 'complement' / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True) as conn:
        found = conn.execute("SELECT data_id FROM tasks WHERE dataset='UKStockPrice' AND state='failed' "
                             "AND error_code='unexpected_empty_after_nonempty' ORDER BY data_id LIMIT 1").fetchone()
    if not found:
        parser.error('no_matching_retry_task')
    task = worker.Task('UKStockPrice', found[0], 'history', 'id_history', 0, 'failed')
    baseline, old = read_baseline(root / 'complement', task)
    load_env_file(repo / '.env', allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '')
    results = []
    with requests.Session() as session:
        account = verified_account(session, token, root)
        limiter = rate_limiter(account)
        for label, start in (
                ('configured_full', '1900-01-01'),
                ('baseline_floor', baseline['source_first_date'][:10]),
                ('tail', (date.fromisoformat(baseline['source_last_date'][:10]) - timedelta(days=7)).isoformat())):
            params = {'dataset': task.dataset, 'data_id': task.data_id, 'start_date': start,
                      'end_date': now.astimezone(worker.TAIPEI).date().isoformat()}
            try:
                rows = worker._fetch_rows(session, limiter, root, task.dataset, token, params,
                                          max_response_bytes=worker.BULK_MAX_RESPONSE_BYTES)
            except worker.SourceError as error:
                results.append({'shape': label, 'parameters': params, 'error_code': error.code})
                break  # No retries or repeated invalid requests in diagnostics.
            dates = sorted(str(row.get('date', '')) for row in rows)
            results.append({'shape': label, 'parameters': params, 'rows': len(rows),
                            'first_date': dates[0] if dates else None, 'last_date': dates[-1] if dates else None,
                            'identity_matches': all(str(row.get('stock_id', '')) == task.data_id for row in rows)})
    report = {'schema_version': 1, 'observed_at_utc': now.isoformat(),
              'dataset': task.dataset, 'data_id': task.data_id,
              'baseline_rows': len(old), 'baseline_first_date': baseline.get('source_first_date'),
              'baseline_last_date': baseline.get('source_last_date'), 'probes': results,
              'data_provider_calls': len(results), 'production_queue_writes': 0,
              'source_unavailable_proven': False}
    atomic_write_json(args.output, report)
    print(json.dumps(report))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
