#!/usr/bin/env python3
"""Independently check indexed two-clock candidates against native tick rows.

The oracle has its own sequential selection algorithm but reads the same
retained vendor files; it is not a second market data source or execution proof.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_reference_oracle import decimal_text, update
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import CONTRACT


def audit(manifest_path: Path, day: date) -> dict:
    manifest_path = manifest_path.resolve()
    manifest_sha = sha256_file(manifest_path)
    doc = json.loads(manifest_path.read_bytes())
    policy = Path(doc['policy_path'])
    payload = Path(doc['data_path'])
    if (doc.get('contract') != CONTRACT or doc.get('training_ready') is not False
            or sha256_file(policy) != doc['policy_sha256']
            or payload.resolve().parent != manifest_path.parent
            or sha256_file(payload) != doc['data_sha256']):
        raise ValueError('event index or policy identity changed')
    windows, _ = candidate_policy(policy)
    clocks = {'08:45': day.strftime('%Y%m%d') + '084500',
              '09:00': day.strftime('%Y%m%d') + '090000'}
    latest = {clock: {} for clock in clocks}
    opened = {clock: {} for clock in clocks}
    counts = {'native_valid_rows': 0, 'observed_rows_by_clock': {clock: 0 for clock in clocks}}
    sources = doc['source_pins']
    if not sources or day.isoformat() not in [s['day'] for s in sources]:
        raise ValueError('decision date needs its own retained source')
    for source in sources:
        source_day = date.fromisoformat(source['day'])
        path, receipt = Path(source['path']), Path(source['receipt_path'])
        if (source_day > day or sha256_file(path) != source['sha256']
                or sha256_file(receipt) != source['receipt_sha256']):
            raise ValueError('source is after the requested day or changed')
        raw, stats = candidate_rows(pl.read_parquet(path), day=source_day,
                                    source_sha256=source['sha256'], candidate_products=set(windows))
        if stats['excluded_rows']:
            raise ValueError('oracle requires all source rows to be classified and valid')
        for stamp, product, series, strike, right, price, digest in raw.iter_rows():
            counts['native_valid_rows'] += 1
            encoded = stamp.strftime('%Y%m%d%H%M%S')
            key = (product, series, decimal_text(strike), right)
            price = decimal_text(price)
            minute = stamp.hour * 60 + stamp.minute
            start, end = windows[product]
            in_session = stamp.date() == day and start <= minute < end
            for clock, cutoff in clocks.items():
                if encoded >= cutoff:
                    continue
                counts['observed_rows_by_clock'][clock] += 1
                update(latest[clock], key, encoded, price, digest, first=False)
                if in_session:
                    update(opened[clock], key, encoded, price, digest, first=True)
    index = pl.read_parquet(payload)
    compared = {}
    for clock in clocks:
        expected = {}
        for key, last in latest[clock].items():
            is_open = key in opened[clock]
            stamp, prices, hashes = opened[clock].get(key, last)
            kind = ('observed_session_open_candidate' if len(prices) == 1 else 'ambiguous_first_second') \
                   if is_open else ('last_observed_trade_candidate' if len(prices) == 1 else 'ambiguous_last_second')
            expected[key] = (kind, next(iter(prices)) if len(prices) == 1 else None,
                             stamp, tuple(sorted(hashes)))
        actual = {}
        for row in reference_candidates(index, trading_date=day, decision_time=clock,
                                        session_windows=windows).iter_rows(named=True):
            key = tuple(row[name] for name in ('product_raw', 'series_raw', 'strike_decimal', 'right_raw'))
            actual[key] = (row['reference_kind'], row['reference_price_decimal'],
                           row['trade_ts'].strftime('%Y%m%d%H%M%S'), tuple(row['source_hashes']))
        missing, extra = set(expected) - set(actual), set(actual) - set(expected)
        mismatch = [key for key in expected.keys() & actual.keys() if expected[key] != actual[key]]
        if missing or extra or mismatch:
            raise ValueError(f'{clock}: missing={len(missing)} extra={len(extra)} mismatched={len(mismatch)}')
        compared[clock] = {'contract_keys_compared': len(expected), 'missing': 0, 'extra': 0,
                           'reference_kind_price_second_or_provenance_mismatch': 0}
    if (sha256_file(manifest_path) != manifest_sha or sha256_file(payload) != doc['data_sha256']
            or any(sha256_file(Path(s['path'])) != s['sha256']
                   or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256'] for s in sources)):
        raise ValueError('source or index changed during independent oracle')
    return {'contract': 'finmind_option_event_index_native_oracle_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'day': day.isoformat(), 'manifest_path': str(manifest_path), 'manifest_sha256': manifest_sha,
            'implementation_sha256': sha256_file(Path(__file__)), 'native_counts': counts,
            'clock_readback': compared, 'known_missing_source_days': doc['known_missing_source_days'],
            'training_ready': False,
            'limitations': ['Both algorithms use the same vendor source files.',
                            'Absent history and known source gaps may hide later trades.',
                            'PIT, expiry, listing, execution and option accounting remain unverified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve the prior oracle output')
    result = audit(args.manifest, args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback']}))


if __name__ == '__main__':
    main()
