#!/usr/bin/env python3
"""Audit retained whole-market TX prints for candidate entries and expiry rolls.

This compares authentic contract-day tape against the independently retained
exchange daily book. Its time windows are observational probes, not certified
fill times, contemporaneous vendor delivery, or a broker clearing rule.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import re
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts.audit_tw_futures_session_envelope import read_market_object  # noqa: E402
from stockagent.data.tw_futures_finmind_ticks import pin_bulk_tick_inventory  # noqa: E402
from stockagent.data.tw_stock_futures_repair import apply_block_evidence  # noqa: E402
from stockagent.data.tw_price_rules import (  # noqa: E402
    price_on_taifex_futures_tick_grid_numpy, taifex_futures_day_session_minutes,
)


def clock_probe(ticks: pl.DataFrame, *, day: str, after: str) -> dict:
    """Summarize all prices at the first strictly later source timestamp.

    Second resolution and duplicate records do not certify within-second
    sequence. Do not choose an individual price when more than one exists.
    """
    later = ticks.filter(pl.col('date') > f'{day} {after}')
    if not later.height:
        return {'after': after, 'observed': False}
    stamp = later['date'].min()
    first = later.filter(pl.col('date') == stamp)
    prices = sorted({float(x) for x in first['price'].to_list()})
    return {'after': after, 'observed': True, 'timestamp': stamp,
            'source_rows_at_second': first.height, 'distinct_prices': prices,
            'matched_contracts_at_second': int(first['volume'].sum() // 2),
            'same_second_price_unambiguous': len(prices) == 1}


def audit_day(ticks: pl.DataFrame, *, day: date, contract: str, official: dict,
              role: str) -> dict:
    if role not in {'year_entry', 'expiry_reopen'} or not re.fullmatch(r'\d{6}', contract):
        raise ValueError('invalid candidate role or physical month')
    if (str(official.get('date')) != day.isoformat() or official.get('product') != 'TX'
            or official.get('contract') != contract):
        raise ValueError('official contract-day identity mismatch')
    if not official.get('official_row_observed') or not official.get('source_row_observed'):
        raise ValueError('official contract-day was not observed')
    expiry = official.get('official_expiry') == day
    start, end = taifex_futures_day_session_minutes(day, product_code='TX',
                                                   asset_class='index_future',
                                                   is_last_trading_day=expiry)
    start_clock = f'{start//60:02d}:{start%60:02d}:00'
    end_clock = f'{end//60:02d}:{end%60:02d}:00'
    d = day.isoformat()
    selected = (ticks.with_row_index('source_row_index')
                .filter((pl.col('futures_id') == 'TX') & (pl.col('contract_date') == contract)
                        & (pl.col('date') >= f'{d} {start_clock}')
                        & (pl.col('date') <= f'{d} {end_clock}'))
                .select('date', 'price', 'volume', 'source_row_index')
                .sort('date', 'source_row_index'))
    row = {'date': d, 'product': 'TX', 'contract_month': contract, 'role': role,
           'session_envelope': [start_clock, end_clock],
           'official_expiry': str(official['official_expiry']) if official['official_expiry'] else None,
           'expiry_date_known': official['official_expiry'] is not None,
           'official_source_sha256': official['official_source_sha256'],
           'official_regular_outright_volume': official['outright_volume'],
           'official_block_volume': official['block_volume'],
           'observed_source_rows': selected.height}
    if not selected.height:
        return {**row, 'valid_tick_values': False, 'full_book_matches': False,
                'entry_probes': [], 'failure': 'no_selected_session_prints'}
    stamps = selected['date'].str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=True)
    values_ok = (stamps.dt.date().eq(day).all() and selected['price'].is_finite().all()
                 and (selected['price'] > 0).all() and selected['volume'].is_not_null().all()
                 and (selected['volume'] > 0).all() and (selected['volume'] % 2 == 0).all())
    if not values_ok:
        return {**row, 'valid_tick_values': False, 'full_book_matches': False,
                'entry_probes': [], 'failure': 'invalid_timestamp_price_or_volume'}
    prices = selected['price'].to_numpy()
    if not price_on_taifex_futures_tick_grid_numpy(
            prices, np.datetime64(day), product_codes='TX', asset_classes='index_future').all():
        return {**row, 'valid_tick_values': False, 'full_book_matches': False,
                'entry_probes': [], 'failure': 'off_dated_tick_grid'}
    first_stamp = selected['date'][0]
    last_stamp = selected['date'][-1]
    first_prices = selected.filter(pl.col('date') == first_stamp)['price'].unique().to_list()
    last_prices = selected.filter(pl.col('date') == last_stamp)['price'].unique().to_list()
    expected = {}
    for field in ('open', 'high', 'low', 'close'):
        try:
            expected[field] = np.float32(float(official[f'official_{field}']))
        except (TypeError, ValueError):
            expected[field] = np.float32(np.nan)
    matched = int(selected['volume'].sum() // 2)
    price_match = bool(np.float32(selected['price'].max()) == expected['high']
                   and np.float32(selected['price'].min()) == expected['low']
                   and np.float32(selected['price'][0]) == expected['open']
                   and np.float32(selected['price'][-1]) == expected['close'])
    volume_match = matched == official['outright_volume']
    probes = ['08:45:00', '09:00:00'] if role == 'year_entry' else ['13:31:00']
    return {**row, 'valid_tick_values': True, 'observed_matched_contracts': matched,
            'volume_matches_official_outright': volume_match, 'ohlc_matches_official_in_source_order': price_match,
            'full_book_matches': bool(volume_match and price_match),
            'first_timestamp': first_stamp, 'last_timestamp': last_stamp,
            'first_second_distinct_prices': sorted(float(p) for p in first_prices),
            'last_second_distinct_prices': sorted(float(p) for p in last_prices),
            'entry_probes': [clock_probe(selected, day=d, after=t) for t in probes]}


def audit(plan_path: Path, reference_path: Path, projection: Path, manifest: Path,
          block_manifest: Path, provider_root: Path) -> dict:
    inputs = [plan_path, reference_path, projection, manifest, block_manifest, Path(__file__)]
    pins = {str(p): sha256_file(p) for p in inputs}
    doc = json.loads(plan_path.read_text())
    meta = json.loads(manifest.read_text())
    if (doc.get('contract') != 'provider_priority_query_v1' or doc.get('provider') != 'finlab'
            or meta.get('contract') != 'tx_contract_day_research_audit_projection_v1'
            or meta.get('projection_sha256') != pins[str(projection)]):
        raise ValueError('plan or pinned official projection differs from its manifest')
    ref = pl.read_parquet(reference_path).sort('date').to_dicts()
    by_day = {str(item['date']): (index, item) for index, item in enumerate(ref)}
    if len(by_day) != len(ref):
        raise ValueError('duplicate reference sessions')
    # The daily source's outright volume still includes block transactions.
    # The canonical reader authenticates each official annual block response
    # and deducts only its dated contract quantity from the regular book.
    official_frame, _ = apply_block_evidence(pl.read_parquet(projection), block_manifest)
    official_rows = official_frame.to_dicts()
    by_key = {(str(r['date']), r['contract']): r for r in official_rows}
    if len(by_key) != len(official_rows):
        raise ValueError('duplicate daily physical identity')
    targets = []
    for item in doc['items']:
        match = re.fullmatch(r'tw_futures_minute:TX_(\d{6})', item['dataset'])
        day = item['partition']
        if not match or day not in by_day:
            raise ValueError('target is not a reference TX contract-day')
        month = match.group(1)
        idx, refrow = by_day[day]
        first_year_day = idx == 0 or ref[idx - 1]['date'].year != refrow['date'].year
        if first_year_day and month == refrow['contract']:
            role = 'year_entry'
        elif (refrow['cash_settlement_event'] and idx + 1 < len(ref)
              and ref[idx + 1]['front_month_roll'] and month == ref[idx + 1]['contract']):
            role = 'expiry_reopen'
        else:
            raise ValueError('target does not match a first-entry or observed expiry transition')
        if (day, month) not in by_key:
            raise ValueError('daily projection lacks the requested physical contract')
        targets.append((day, month, role))
    if not targets or len(set(targets)) != len(targets) or len({x[0] for x in targets}) != len(targets):
        raise ValueError('empty, duplicate or multiple targets per day')
    pinned = {item['date']: item for item in pin_bulk_tick_inventory(
        provider_root, [date.fromisoformat(x[0]) for x in targets])}
    results, objects = [], []
    for day, month, role in sorted(targets):
        raw, proof = read_market_object(provider_root, 'TaiwanFuturesTick', day)
        item = pinned[day]
        if (proof['payload_sha256'] != item['sha256'] or proof['receipt_sha256'] != item['receipt_sha256']
                or raw.height != item['rows']):
            raise ValueError('loaded market object differs from pinned collector inventory')
        result = audit_day(raw, day=date.fromisoformat(day), contract=month,
                           official=by_key[(day, month)], role=role)
        result['tick_sha256'] = item['sha256']
        results.append(result)
        objects.append({'date': day, 'path': item['path'], 'sha256': item['sha256'],
                        'receipt_path': item['receipt_path'], 'receipt_sha256': item['receipt_sha256'],
                        'rows': item['rows'], 'historical_point_in_time': item['historical_point_in_time']})
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('plan or daily projection changed during audit')
    all_probes = [p for r in results for p in r['entry_probes']]
    return {'contract': 'tx_retained_finmind_contract_day_clock_audit_v2',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'inputs_sha256': pins,
            'counts': {'target_contract_days': len(results),
                       'year_entry_days': sum(r['role'] == 'year_entry' for r in results),
                       'expiry_reopen_days': sum(r['role'] == 'expiry_reopen' for r in results),
                       'valid_tick_days': sum(r['valid_tick_values'] for r in results),
                       'days_with_official_block_volume': sum(r['official_block_volume'] > 0 for r in results),
                       'official_full_book_matches': sum(r['full_book_matches'] for r in results),
                       'clock_probes': len(all_probes),
                       'observed_clock_probes': sum(p['observed'] for p in all_probes),
                       'same_second_multi_price_probes': sum(p['observed'] and not p['same_second_price_unambiguous'] for p in all_probes)},
            'source_objects': objects, 'rows': results,
            'provider_calls': 0, 'timezone_interpretation': 'Asia/Taipei',
            'historical_source_delivery_clock_verified': False, 'broker_fill_verified': False,
            'historical_settlement_or_margin_release_timing_verified': False,
            'benchmark_admitted': False, 'training_admitted': False,
            'limitations': ['Ticks are recorded market prints, not proof that our order could fill.',
                            'Full-day agreement is retrospective and does not prove historical delivery time.',
                            'Official annual block records are excluded from the regular-order volume comparison.',
                            'Probes use strict-after clocks and do not choose a price inside a multi-price second.',
                            'The 13:31 probe is a candidate only, not a historical clearing or margin-release rule.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--projection', type=Path, required=True)
    parser.add_argument('--projection-manifest', type=Path, required=True)
    parser.add_argument('--block-manifest', type=Path, required=True)
    parser.add_argument('--provider-root', type=Path, default=ROOT / 'data_finmind/complement')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or not args.output.resolve().is_relative_to(ROOT / 'artifacts/markets'):
        raise ValueError('choose a new versioned market artifact')
    result = audit(args.plan, args.reference, args.projection, args.projection_manifest,
                   args.block_manifest, args.provider_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'counts': result['counts']}))


if __name__ == '__main__':
    main()
