"""Bounded, cached query-shape evidence using the canonical shared limiter.

Diagnostic responses stay private. The report contains hashes/counts only;
running the same probe again reuses its response instead of spending quota.
No production task/receipt is changed by this tool.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path

import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import _fetch_rows, SourceError
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import fixed_incremental_demand


PROBES = {
    'bond_month_boundary': ('data', {'dataset': 'TaiwanStockConvertibleBondMonthlyAnalysis',
                                    'start_date': '2026-05-01', 'end_date': '2026-07-01'}),
    'spread_all_range': ('data', {'dataset': 'TaiwanFuturesSpreadTrading',
                                 'start_date': '2026-09-23', 'end_date': '2026-09-24'}),
    'bond_month_all': ('data', {'dataset': 'TaiwanStockConvertibleBondMonthlyAnalysis',
                               'start_date': '2026-05-01', 'end_date': '2026-09-01'}),
    'bond_month_one': ('data', {'dataset': 'TaiwanStockConvertibleBondMonthlyAnalysis', 'data_id': '13166',
                               'start_date': '2026-05-01', 'end_date': '2026-09-01'}),
    'swap_fixed_all': ('data', {'dataset': 'TaiwanAssetSwapFixedIncomeDaily', 'start_date': '2026-09-23'}),
    'swap_option_all': ('data', {'dataset': 'TaiwanAssetSwapOptionDaily', 'start_date': '2026-09-23'}),
    'broker_all_stocks': ('taiwan_stock_trading_daily_report',
                          {'securities_trader_id': '1020', 'date': '2026-09-24'}),
    'stock_all_brokers': ('taiwan_stock_trading_daily_report',
                          {'data_id': '2330', 'date': '2026-09-24'}),
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', choices=PROBES, action='append', required=True)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    load_env_file(Path('.env'), allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    report = {'observed_at_utc': datetime.now(UTC).isoformat(), 'queue_writes': 0, 'probes': []}
    cache = args.root / 'diagnostics' / 'call_efficiency'
    with requests.Session() as session:
        account = verified_account(session, token, args.root)
        limiter = rate_limiter(account)
        for name in args.probe:
            endpoint, params = PROBES[name]
            key = hashlib.sha256(json.dumps([endpoint, params], sort_keys=True).encode()).hexdigest()
            result_path = cache / (key + '.json')
            if result_path.exists():
                item = json.loads(result_path.read_bytes())
                item = {**item, 'reused_cached_probe': True}
            else:
                now = datetime.now(UTC)
                budget = backfill_budget(account, args.root,
                    fixed_incremental_requests=fixed_incremental_demand(args.root, now), now=now)
                if not budget['allowed']:
                    report['halted'] = budget['basis']
                    break
                item = {'name': name, 'endpoint': endpoint, 'params': params,
                        'observed_at_utc': now.isoformat(), 'reused_cached_probe': False}
                try:
                    rows = _fetch_rows(session, limiter, args.root, params.get('dataset', endpoint), token, params,
                                       endpoint='https://api.finmindtrade.com/api/v4/' + endpoint,
                                       max_response_bytes=64 * 1024**2)
                    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
                    digest = hashlib.sha256(payload).hexdigest()
                    payload_path = cache / (digest + '.rows.json')
                    atomic_write_bytes(payload_path, payload, durable=True)
                    days = Counter(str(row.get('date', ''))[:10] for row in rows)
                    item.update(state='nonempty' if rows else 'observed_empty', rows=len(rows),
                                dates=dict(sorted(days.items())), columns=sorted({k for row in rows for k in row}),
                                payload_path=str(payload_path.relative_to(args.root)), payload_sha256=digest,
                                known_ids={field: len({r[field] for r in rows if field in r})
                                           for field in ('stock_id', 'cb_id', 'securities_trader_id')})
                except SourceError as error:
                    item.update(state='request_error', error_code=error.code)
                atomic_write_json(result_path, item)
            report['probes'].append(item)
            atomic_write_json(args.output, report)
            print({key: item[key] for key in ('name', 'state', 'rows', 'error_code', 'reused_cached_probe') if key in item}, flush=True)
            if item.get('error_code') in {'invalid_token', 'ip_banned', 'rate_limited'}:
                break
    atomic_write_json(args.output, report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
