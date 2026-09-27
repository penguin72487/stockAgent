"""Bounded, quota-accounted FinMind query verification; never mutate queues."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import _fetch_rows, _sha256, SourceError
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_observation_dates import is_observation_date
from downloader.finmind_scheduling import fixed_incremental_demand


def _row_counts(rows, keys):
    # FinMind JSON and Parquet can represent the same number as int or float.
    # Python numeric equality preserves that equivalence without rounding.
    return Counter(tuple(row.get(key) for key in keys) for row in rows)


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    root = repo / 'data_finmind'
    load_env_file(repo / '.env', allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    report = {'observed_at_utc': datetime.now(UTC).isoformat(),
              'max_data_requests': 3, 'queue_writes': 0, 'probes': []}
    requests_to_check = [
        {'dataset': 'TaiwanStockFinancialStatements', 'data_id': '2330',
         'start_date': '1990-03-01', 'end_date': '2013-12-31'},
        {'dataset': 'TaiwanBusinessIndicator', 'start_date': '2014-01-01', 'end_date': '2015-12-31'},
        {'dataset': 'CnnFearGreedIndex', 'start_date': '2014-01-01', 'end_date': '2015-12-31'},
    ]
    with requests.Session() as session:
        account = verified_account(session, token, root)
        limiter = rate_limiter(account)
        for params in requests_to_check:
            now = datetime.now(UTC)
            budget = backfill_budget(account, root, fixed_incremental_requests=fixed_incremental_demand(root, now), now=now)
            if not budget['allowed']:
                report['halted'] = budget['basis']
                break
            dataset = params['dataset']
            record = {'params': params, 'started_at_utc': now.isoformat()}
            try:
                rows = _fetch_rows(session, limiter, root, dataset, token, params)
                stamps = sorted({row['date'][:10] for row in rows})
                record.update({'rows': len(rows), 'distinct_dates': len(stamps),
                               'first_date': min(stamps, default=None), 'last_date': max(stamps, default=None),
                               'outside_request_dates': [s for s in stamps if not params['start_date'] <= s <= params['end_date']],
                               'response_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()})
                if params.get('data_id'):
                    record['nonstandard_observation_dates'] = [s for s in stamps if not is_observation_date(dataset, date.fromisoformat(s))]
                    record['scope'] = 'one_symbol_old_history_not_whole_market_proof'
                else:
                    original = []
                    for partition in ('2014-01-01', '2015-01-01'):
                        receipt = json.loads((root / 'sponsor/receipts' / dataset / 'all' / f'{partition}.json').read_text())
                        parquet = root / 'sponsor' / receipt['parquet_path']
                        if _sha256(parquet) != receipt['sha256']:
                            raise ValueError('local proof mismatch')
                        original.extend(pq.read_table(parquet).to_pylist())
                    keys = sorted({key for row in rows for key in row})
                    record['rows_per_year'] = dict(Counter(row['date'][:4] for row in rows))
                    record['local_verified_rows'] = len(original)
                    record['exact_local_union_parity'] = bool(rows) and _row_counts(rows, keys) == _row_counts(original, keys)
                    record['logical_partitions_per_request'] = 2
                record['completed_at_utc'] = datetime.now(UTC).isoformat()
            except SourceError as error:
                record['error_code'] = error.code
                report['probes'].append(record)
                break
            except (OSError, ValueError, KeyError, TypeError) as error:
                record['verification_error_type'] = type(error).__name__
            report['probes'].append(record)
    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    path = repo / 'artifacts/data_quality' / f'finmind_efficiency_probe_{stamp}.json'
    atomic_write_json(path, report)
    print(json.dumps({'report': str(path), **report}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
