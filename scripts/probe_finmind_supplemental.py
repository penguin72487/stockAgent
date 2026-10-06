"""Bounded authenticated contract probes through the shared FinMind limiter.

Only sanitized schema/count/range evidence is emitted. No queue is mutated and
no token or raw response body is included in diagnostic artifacts.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import os
import hashlib
import json
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


def _fingerprint(rows):
    """Compare bags, including duplicate multiplicity, without logging values."""
    canonical = sorted(json.dumps(row, sort_keys=True, separators=(',', ':')) for row in rows)
    return hashlib.sha256(json.dumps(canonical).encode()).hexdigest()


def probe_us_date_range(fetch, start: date, days: int) -> dict:
    """Finite diagnostic only; an ignored end_date must not become a new ABI.

    Compare every requested physical date with its independent single-day
    response. Empty references alone cannot prove a range is supported.
    """
    if not 2 <= days <= 7:
        raise ValueError('range_probe_requires_two_to_seven_days')
    dataset, identifier = 'USStockPriceMinute', PROBE_IDS['USStockPriceMinute']
    end = start + timedelta(days=days - 1)
    endpoint, params, metadata = request_contract(dataset, identifier, start.isoformat(), end)
    params['end_date'] = end.isoformat()
    rows = fetch(endpoint, params)
    buckets = {}
    for row in rows:
        observed = date.fromisoformat(str(row.get('date', ''))[:10])
        if not start <= observed <= end:
            raise ValueError('range_probe_response_outside_range')
        validate_response(dataset, identifier, observed.isoformat(), end, [row])
        buckets.setdefault(observed, []).append(row)
    comparisons = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        reference_params = {key: value for key, value in params.items() if key != 'end_date'}
        reference_params['start_date'] = day.isoformat()
        reference = fetch(endpoint, reference_params)
        validate_response(dataset, identifier, day.isoformat(), day, reference)
        subset = buckets.get(day, [])
        comparisons.append({'date': day.isoformat(), 'range_rows': len(subset),
                            'single_day_rows': len(reference),
                            'equal': _fingerprint(subset) == _fingerprint(reference),
                            'range_sha256': _fingerprint(subset), 'single_day_sha256': _fingerprint(reference)})
    verified = (all(item['equal'] for item in comparisons)
                and sum(item['single_day_rows'] > 0 for item in comparisons) >= 2)
    return {'dataset': dataset, 'data_id': identifier, 'request_start_date': start.isoformat(),
            'request_end_date': end.isoformat(), 'range_rows': len(rows), 'days': comparisons,
            'provider_data_fetch_invocations': days + 1, 'queue_writes': 0,
            'date_range_parity_verified': verified,
            'status': 'range_parity_verified_not_admitted' if verified else 'range_not_proven_or_mismatch'}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--dataset', choices=sorted(SOURCES), action='append')
    parser.add_argument('--probe-market-shape', action='store_true',
                        help='Bounded whole-market/per-ID parity probe; does not admit the shape into the worker.')
    parser.add_argument('--probe-us-range-days', type=int, choices=range(2, 8),
                        help='Finite US minute range-versus-each-day probe. No queue changes or shape admission.')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    market_probe_sources = {'TaiwanFuturesSpreadTick', 'TaiwanAssetSwapFixedIncomeDaily', 'TaiwanAssetSwapOptionDaily'}
    if args.probe_market_shape and (not args.dataset or not set(args.dataset) <= market_probe_sources):
        parser.error('--probe-market-shape requires explicit allowlisted sparse sources, never known per-ID-only tick/KBar')
    if args.probe_us_range_days and (args.probe_market_shape or args.dataset != ['USStockPriceMinute']):
        parser.error('--probe-us-range-days requires exactly --dataset USStockPriceMinute')
    load_env_file(Path('.env'), allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    if not token:
        raise SystemExit('FINMIND_TOKEN is required')
    report = {'schema_version': 2, 'observed_at_utc': datetime.now(UTC).isoformat(), 'probes': [], 'queue_writes': 0}
    with requests.Session() as session:
        account = verified_account(session, token, args.root)
        limiter = rate_limiter(account)
        if args.probe_us_range_days:
            def fetch(endpoint, params):
                if not backfill_budget(account, args.root, fixed_incremental_requests=32,
                                       now=datetime.now(UTC))['allowed']:
                    raise SourceError('probe_quota_not_available', retry_after=60)
                return _fetch_rows(session, limiter, args.root, 'USStockPriceMinute', token, params,
                                   endpoint=endpoint, max_response_bytes=BULK_MAX_RESPONSE_BYTES)
            try:
                report['probes'].append(probe_us_date_range(fetch, args.date, args.probe_us_range_days))
            except SourceError as error:
                report['probes'].append({'dataset': 'USStockPriceMinute', 'status': 'request_error',
                                         'error_code': error.code})
            except ValueError:
                report['probes'].append({'dataset': 'USStockPriceMinute', 'status': 'validation_error'})
            atomic_write_json(args.output, report)
            print({'status': report['probes'][0]['status']}, flush=True)
            return 0
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
            identifier = '' if spec.universe == 'market' else PROBE_IDS[dataset]
            endpoint, params, metadata = request_contract(dataset, identifier, partition, args.date)
            if args.probe_market_shape:
                params.pop('data_id', None)
                metadata = {**metadata, 'query_shape': 'diagnostic_whole_market_not_yet_admitted', 'data_id': None}
            item = {'dataset': dataset, 'data_id': identifier, 'request': metadata,
                    'provider_data_fetch_invocations': 0}
            try:
                item['provider_data_fetch_invocations'] += 1
                rows = _fetch_rows(session, limiter, args.root, dataset, token, params,
                                   endpoint=endpoint, max_response_bytes=BULK_MAX_RESPONSE_BYTES)
                if args.probe_market_shape:
                    for row in rows:
                        observed_id = str(row.get(spec.identity_field) or '')
                        if not observed_id:
                            raise ValueError('missing_market_identity')
                        validate_response(dataset, '' if spec.universe == 'market' else observed_id,
                                          partition, args.date, [row])
                    ids = sorted({str(row[spec.identity_field]) for row in rows})
                    item['returned_identifiers'] = ids
                    if ids:
                        selected_id = ids[0]
                        reference_params = {**params, 'data_id': selected_id}
                        if not backfill_budget(account, args.root, fixed_incremental_requests=32,
                                               now=datetime.now(UTC))['allowed']:
                            raise ValueError('parity_probe_quota_not_available')
                        item['provider_data_fetch_invocations'] += 1
                        reference = _fetch_rows(session, limiter, args.root, dataset, token, reference_params,
                                                endpoint=endpoint, max_response_bytes=BULK_MAX_RESPONSE_BYTES)
                        validate_response(dataset, '' if spec.universe == 'market' else selected_id,
                                          partition, args.date, reference)
                        if any(str(row[spec.identity_field]) != selected_id for row in reference):
                            raise ValueError('per_id_reference_identity_mismatch')
                        subset = [row for row in rows if str(row[spec.identity_field]) == selected_id]
                        item['parity'] = {'data_id': selected_id, 'market_subset_rows': len(subset),
                                          'per_id_rows': len(reference), 'equal': _fingerprint(subset) == _fingerprint(reference),
                                          'market_subset_sha256': _fingerprint(subset), 'per_id_sha256': _fingerprint(reference)}
                    item['market_shape_parity_verified'] = bool(item.get('parity', {}).get('equal'))
                else:
                    validate_response(dataset, identifier, partition, args.date, rows)
                dates = sorted(str(row.get('date')) for row in rows if row.get('date'))
                item.update(status='nonempty_validated' if rows else 'observed_empty_not_complete', rows=len(rows),
                            first_data_date=dates[0] if dates else None, last_data_date=dates[-1] if dates else None,
                            columns=sorted({key for row in rows for key in row}))
                if args.probe_market_shape and rows and not item['market_shape_parity_verified']:
                    item['status'] = 'market_parity_mismatch_shape_rejected'
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
