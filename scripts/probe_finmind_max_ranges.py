"""One bounded full-span verification per range source; no production writes."""

from collections import Counter
from datetime import UTC, date, datetime
import json
import os
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import SourceError, _fetch_rows, _sha256
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import SOURCES, fixed_incremental_demand

DATASETS = (
    'TaiwanBusinessIndicator', 'CnnFearGreedIndex', 'TaiwanOptionVix',
    'TaiwanTotalExchangeMarginMaintenance', 'TaiwanStockCapitalReductionReferencePrice',
    'TaiwanStockSuspended', 'TaiwanStockConvertibleBondPutProvision',
    'TaiwanStockInfoWithWarrantSummary',
)


def main():
    repo = Path(__file__).resolve().parents[1]
    root = repo / 'data_finmind'
    load_env_file(repo / '.env', allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    specs = {s.dataset: s for s in SOURCES}
    now = datetime.now(UTC)
    report = {'observed_at_utc': now.isoformat(), 'production_queue_writes': 0,
              'max_data_requests': len(DATASETS), 'probes': []}
    with requests.Session() as session, sqlite3.connect(f'file:{root}/sponsor/queue.sqlite3?mode=ro', uri=True) as conn:
        account = verified_account(session, token, root)
        limiter = rate_limiter(account)
        for dataset in DATASETS:
            now = datetime.now(UTC)
            budget = backfill_budget(account, root, fixed_incremental_requests=fixed_incremental_demand(root, now), now=now)
            if not budget['allowed']:
                report['halted'] = budget['basis']
                break
            # Closed periods avoid current-period publication changing during a
            # parity check. Future runs calculate this boundary, not a fixed year.
            spec = specs[dataset]
            today = now.date()
            end = (date(today.year, today.month, 1).toordinal() - 1 if spec.grain == 'month'
                   else date(today.year, 1, 1).toordinal() - 1)
            end_day = date.fromordinal(end)
            params = {'dataset': dataset, 'start_date': str(spec.first_date), 'end_date': str(end_day)}
            item = {'params': params, 'started_at_utc': now.isoformat()}
            try:
                rows = _fetch_rows(session, limiter, root, dataset, token, params,
                                   max_response_bytes=64 * 1024 * 1024)
                dates = [date.fromisoformat(row['date'][:10]) for row in rows]
                keys = sorted({key for row in rows for key in row})
                by_partition = {}
                for row in rows:
                    d = date.fromisoformat(row['date'][:10])
                    nominal = date(d.year, d.month if spec.grain == 'month' else 1, 1)
                    partition = str(max(nominal, spec.first_date))
                    by_partition.setdefault(partition, []).append(row)
                compared = matched = compared_rows = 0
                mismatched = []
                for part, receipt_path in conn.execute(
                    "SELECT partition,receipt_path FROM tasks WHERE dataset=? AND state='complete' "
                    "AND partition<=? AND priority!=0 ORDER BY partition", (dataset, str(end_day)),
                ):
                    receipt = json.loads((root / 'sponsor' / receipt_path).read_text())
                    parquet = root / 'sponsor' / receipt['parquet_path']
                    if _sha256(parquet) != receipt['sha256']:
                        raise ValueError('local proof mismatch')
                    original = pq.read_table(parquet).to_pylist()
                    actual = by_partition.get(part, [])
                    equal = Counter(tuple(row.get(k) for k in keys) for row in original) == Counter(tuple(row.get(k) for k in keys) for row in actual)
                    compared += 1
                    matched += equal
                    compared_rows += len(original)
                    if not equal:
                        mismatched.append(part)
                item.update({'rows': len(rows), 'response_first_date': str(min(dates)) if dates else None,
                             'response_last_date': str(max(dates)) if dates else None,
                             'returned_partitions': len(by_partition), 'verified_local_partitions': compared,
                             'matched_local_partitions': matched, 'verified_local_rows': compared_rows,
                             'mismatched_partitions': mismatched,
                             'outside_request_rows': sum(d < spec.first_date or d > end_day for d in dates),
                             'oldest_requested_year_rows': sum(d.year == spec.first_date.year for d in dates),
                             'exact_available_local_overlap': compared > 0 and matched == compared,
                             'upstream_total_rows_provided': False})
            except SourceError as exc:
                item['error_code'] = exc.code
                if exc.code in {'rate_limited','not_entitled','invalid_token','ip_banned'}:
                    report['halted'] = exc.code
            except (OSError, ValueError, TypeError, KeyError) as exc:
                item['verification_error_type'] = type(exc).__name__
            item['completed_at_utc'] = datetime.now(UTC).isoformat()
            report['probes'].append(item)
            if report.get('halted'):
                break
    target = repo / 'artifacts/data_quality' / f"finmind_max_ranges_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    atomic_write_json(target, report)
    print(json.dumps({'report': str(target), **report}, ensure_ascii=False))


if __name__ == '__main__':
    main()
