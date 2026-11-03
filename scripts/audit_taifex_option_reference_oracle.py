#!/usr/bin/env python3
"""Independently compare native CSV reference reduction with the Arrow path.

This validates the observed source reduction only. Neither path establishes
dated listing, complete tapes, actionable prices, or option account rules.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import lru_cache
import hashlib
import io
import json
from pathlib import Path
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import reference_candidates


@lru_cache(maxsize=65536)
def decimal_text(raw: str) -> str:
    value = Decimal(raw)
    if not value.is_finite() or value <= 0:
        raise ValueError('invalid observed price or strike')
    return format(value.normalize(), 'f')


def update(table: dict, key: tuple, stamp: str, price: str, source_hash: str, *, first: bool) -> None:
    old = table.get(key)
    if old is None or (stamp < old[0] if first else stamp > old[0]):
        table[key] = (stamp, {price}, {source_hash})
    elif stamp == old[0]:
        old[1].add(price)
        old[2].add(source_hash)


def native_reference(sources: list[dict], day: date, windows: dict) -> tuple[dict, dict]:
    clocks = {'08:45': day.strftime('%Y%m%d') + '084500',
              '09:00': day.strftime('%Y%m%d') + '090000'}
    latest = {clock: {} for clock in clocks}
    opening = {clock: {} for clock in clocks}
    counts = {'raw_side_rows': 0, 'observed_rows_by_clock': {clock: 0 for clock in clocks}}
    for source in sources:
        path = Path(source['path'])
        if sha256_file(path) != source['sha256']:
            raise ValueError('official source changed before native oracle')
        member = f"OptionsDaily_{date.fromisoformat(source['date']).strftime('%Y_%m_%d')}.csv"
        with zipfile.ZipFile(path) as archive:
            if archive.namelist() != [member]:
                raise ValueError('unexpected official source member')
            with archive.open(member) as binary:
                reader = csv.reader(io.TextIOWrapper(binary, encoding='cp950', newline=''))
                header = tuple(value.strip() for value in next(reader))
                if header != ('成交日期', '商品代號', '履約價格', '到期月份(週別)', '買賣權別',
                              '成交時間', '成交價格', '成交數量(B or S)', '開盤集合競價'):
                    raise ValueError('unexpected native official header')
                if not next(reader)[0].strip().startswith('-'):
                    raise ValueError('unexpected native official separator')
                for row in reader:
                    counts['raw_side_rows'] += 1
                    if len(row) != 9:
                        raise ValueError('malformed native CSV row')
                    native_day, product, strike, series, right, native_time, price = (
                        value.strip() for value in row[:7])
                    if product not in windows:
                        continue
                    stamp = native_day + native_time
                    if len(stamp) != 14 or not stamp.isdigit():
                        raise ValueError('invalid native event timestamp')
                    active_clocks = [clock for clock, cutoff in clocks.items() if stamp < cutoff]
                    if not active_clocks:
                        continue
                    key = (product, series, decimal_text(strike), right)
                    value = decimal_text(price)
                    minute = int(native_time[:2]) * 60 + int(native_time[2:4])
                    start, end = windows[product]
                    in_session = native_day == day.strftime('%Y%m%d') and start <= minute < end
                    for clock in active_clocks:
                        counts['observed_rows_by_clock'][clock] += 1
                        update(latest[clock], key, stamp, value, source['sha256'], first=False)
                        if in_session:
                            update(opening[clock], key, stamp, value, source['sha256'], first=True)
        if sha256_file(path) != source['sha256'] or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']:
            raise ValueError('official source changed during native oracle')
    expected = {}
    for clock in clocks:
        output = {}
        for key, last in latest[clock].items():
            has_open = key in opening[clock]
            stamp, prices, hashes = opening[clock].get(key, last)
            if has_open:
                kind = 'observed_session_open_candidate' if len(prices) == 1 else 'ambiguous_first_second'
            else:
                kind = 'last_observed_trade_candidate' if len(prices) == 1 else 'ambiguous_last_second'
            output[key] = (kind, next(iter(prices)) if len(prices) == 1 else None, stamp, tuple(sorted(hashes)))
        expected[clock] = output
    return expected, counts


def audit(day: date, previous: date, policy_path: Path, manifest: Path) -> dict:
    if previous >= day:
        raise ValueError('previous source must precede the decision')
    policy_sha = sha256_file(policy_path)
    policy = json.loads(policy_path.read_bytes())
    if policy.get('trading_date') != day.isoformat() or policy.get('research_candidate_only') is not True:
        raise ValueError('dated research-only candidate policy required')
    windows = {key: tuple(value['day_minutes']) for key, value in policy['products'].items()}
    sources = pin_official_option_tick_inventory(manifest, [previous, day])
    oracle, counts = native_reference(sources, day, windows)
    frames = []
    for source in sources:
        for frame in iter_official_option_trade_batches(source):
            frames.append(frame.select('event_ts', 'product_raw', 'series_raw', 'strike_raw',
                                       'right_raw', 'price_raw', 'source_sha256'))
    rows = pl.concat(frames)
    compared = {}
    for clock in ('08:45', '09:00'):
        actual = reference_candidates(rows, trading_date=day, decision_time=clock, session_windows=windows)
        observed = {}
        for row in actual.iter_rows(named=True):
            key = tuple(row[name] for name in ('product_raw', 'series_raw', 'strike_decimal', 'right_raw'))
            observed[key] = (row['reference_kind'], row['reference_price_decimal'],
                             row['trade_ts'].strftime('%Y%m%d%H%M%S'), tuple(row['source_hashes']))
        expected = oracle[clock]
        missing = set(expected) - set(observed)
        extra = set(observed) - set(expected)
        mismatched = [key for key in expected.keys() & observed.keys() if expected[key] != observed[key]]
        if missing or extra or mismatched:
            raise ValueError(f'{clock}: missing={len(missing)} extra={len(extra)} mismatched={len(mismatched)}')
        compared[clock] = {'contract_keys_compared': len(expected), 'missing': 0, 'extra': 0,
                           'reference_kind_price_second_or_provenance_mismatch': 0}
    if sha256_file(policy_path) != policy_sha:
        raise ValueError('candidate session policy changed during oracle')
    paths = [Path(__file__).resolve(), Path('stockagent/data/tw_option_product_ticks.py').resolve(),
             Path('stockagent/data/tw_option_reference_candidates.py').resolve()]
    return {'contract': 'taifex_option_reference_native_csv_oracle_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'day': day.isoformat(), 'previous_source_date': previous.isoformat(),
            'source_pins': sources, 'policy_path': str(policy_path.resolve()), 'policy_sha256': policy_sha,
            'implementation_sha256': {str(path): sha256_file(path) for path in paths},
            'independent_native_csv_counts': counts, 'clock_readback': compared,
            'training_ready': False,
            'limitations': ['Observed keys are not dated listing, expiry, fill or account evidence.',
                            'Independent algorithm reads the same original files, not an independent venue.',
                            'Incomplete historical input cannot prove a true session open or last trade.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--previous-date', type=date.fromisoformat, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the earlier oracle readback')
    result = audit(args.date, args.previous_date, args.policy, args.manifest)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback']}))


if __name__ == '__main__':
    main()
