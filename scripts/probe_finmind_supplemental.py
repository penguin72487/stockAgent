"""Bounded authenticated contract probes through the shared FinMind limiter.

Only sanitized schema/count/range evidence is emitted. No queue is mutated and
no token or raw response body is included in diagnostic artifacts.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import BULK_MAX_RESPONSE_BYTES, SourceError, _fetch_rows
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_supplemental import SOURCES, request_contract, validate_response


PROBE_IDS = {
    'TaiwanFuturesSpreadTrading': 'TX', 'TaiwanStockConvertibleBondMonthlyAnalysis': '13166',
    'TaiwanAssetSwapFixedIncomeDaily': '17172', 'TaiwanAssetSwapOptionDaily': '17172',
    'TaiwanStockTradingDailyReportSecIdAgg': '2330', 'TaiwanStockKBar': '2330',
    'TaiwanFuturesKBar': 'TX', 'TaiwanStockTradingDailyReport': '2330',
    'TaiwanStockWarrantTradingDailyReport': '5920', 'USStockPriceMinute': '^DJI',
    'TaiwanStockPriceTick': '2330', 'TaiwanFuturesTick': 'MTX',
    'TaiwanOptionTick': 'OCO', 'TaiwanFuturesSpreadTick': 'CAF',
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--dataset', choices=sorted(SOURCES), action='append')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    load_env_file(Path('.env'), allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    if not token:
        raise SystemExit('FINMIND_TOKEN is required')
    report = {'observed_at_utc': datetime.now(UTC).isoformat(), 'probes': [], 'queue_writes': 0}
    with requests.Session() as session:
        account = verified_account(session, token, args.root)
        limiter = rate_limiter(account)
        for dataset in args.dataset or SOURCES:
            if SOURCES[dataset].grain == 'derived':
                report['probes'].append({'dataset': dataset, 'status': 'derived_no_api_parent_probe_required'})
                continue
            if not backfill_budget(account, args.root, fixed_incremental_requests=32, now=datetime.now(UTC))['allowed']:
                report['stop_reason'] = 'quota_reserved_or_account_sample_stale'
                break
            spec = SOURCES[dataset]
            partition = ('history' if spec.grain == 'history' else
                         args.date.replace(day=1).isoformat() if spec.grain == 'month' else args.date.isoformat())
            identifier = PROBE_IDS[dataset]
            endpoint, params, metadata = request_contract(dataset, identifier, partition, args.date)
            item = {'dataset': dataset, 'data_id': identifier, 'request': metadata}
            try:
                rows = _fetch_rows(session, limiter, args.root, dataset, token, params,
                                   endpoint=endpoint, max_response_bytes=BULK_MAX_RESPONSE_BYTES)
                validate_response(dataset, identifier, partition, args.date, rows)
                dates = sorted(str(row.get('date')) for row in rows if row.get('date'))
                item.update(status='nonempty_validated' if rows else 'observed_empty_not_complete', rows=len(rows),
                            first_data_date=dates[0] if dates else None, last_data_date=dates[-1] if dates else None,
                            columns=sorted({key for row in rows for key in row}))
            except SourceError as error:
                item.update(status='request_error', error_code=error.code)
            except ValueError:
                item.update(status='validation_error')
            report['probes'].append(item)
            atomic_write_json(args.output, report)
            print({key: item[key] for key in ('dataset', 'status', 'rows', 'error_code') if key in item}, flush=True)
            if item.get('error_code') in {'invalid_token', 'ip_banned', 'rate_limited'}:
                break
    atomic_write_json(args.output, report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
