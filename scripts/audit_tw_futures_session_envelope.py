#!/usr/bin/env python3
"""Diagnose missing common-session prints against retained original sources.

This is a retrospective audit, not a source admission or an order/fill.  In
particular, matching a wider envelope does not establish the contract's final
trading day, historical availability at a decision, or a financial reference.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_tw_futures_initial_reference_evidence import source_rows


def read_market_object(root: Path, dataset: str, day: str) -> tuple[pl.DataFrame, dict]:
    """Read both the retained collector receipt and its immutable payload."""
    receipt = root / 'receipts' / dataset / 'all' / f'{day}.json'
    receipt_hash = sha256_file(receipt)
    info = json.loads(receipt.read_text())
    if (info.get('dataset'), info.get('data_id'), info.get('partition'), info.get('status')) != (
            dataset, '', day, 'complete'):
        raise ValueError(f'unexpected provider receipt: {dataset} {day}')
    payload = (root / info['parquet_path']).resolve()
    if (not payload.is_relative_to(root.resolve()) or payload.stat().st_size != info['parquet_size_bytes']
            or sha256_file(payload) != info['sha256']):
        raise ValueError(f'provider payload is not receipt-bound: {dataset} {day}')
    frame = pl.read_parquet(payload)
    if (frame.height != info['rows'] or sha256_file(receipt) != receipt_hash
            or sha256_file(payload) != info['sha256']):
        raise ValueError('provider object changed or its count differs')
    return frame, {'receipt': str(receipt), 'receipt_sha256': receipt_hash,
                   'payload': str(payload), 'payload_sha256': info['sha256'],
                   'historical_point_in_time': info.get('historical_point_in_time')}


def f32_equal(actual: object, official: object) -> bool:
    """Compare an original Float32 encoding, not an arbitrarily rounded quote."""
    return np.float32(float(actual)) == np.float32(float(str(official).strip()))


def audit(plan: Path, omissions: Path, official: list[Path], root: Path) -> dict:
    watched = [plan, omissions, *official, Path(__file__).resolve()]
    pinned = {str(p.resolve()): sha256_file(p) for p in watched}
    doc = json.loads(plan.read_text())
    if doc.get('contract') != 'provider_priority_query_v1' or doc.get('provider') != 'finlab':
        raise ValueError('an existing finite futures-minute priority plan is required')
    wanted = set()
    for item in doc['items']:
        match = re.fullmatch(r'tw_futures_minute:([A-Z0-9]+)_(\d{6}(?:W[1-5])?)', item['dataset'])
        if match is None:
            raise ValueError('priority item is not an exact futures delivery contract')
        wanted.add((item['partition'], match.group(1), match.group(2)))
    if len(wanted) != len(doc['items']) or not wanted:
        raise ValueError('priority target coordinates must be nonempty and unique')
    with omissions.open(newline='') as stream:
        candidates = {}
        for row in csv.DictReader(stream):
            key = row['date'], row['product'], row['contract']
            if key in wanted:
                if key in candidates:
                    raise ValueError('multiple omitted physical generations match one target')
                candidates[key] = row
    if set(candidates) != wanted:
        raise ValueError('priority targets do not match the omitted contract-day audit')
    found = {}
    for path in official:
        for row in source_rows(path):
            if (row.get('交易時段') or '').strip() != '一般':
                continue
            key = ((row.get('交易日期') or '').strip().replace('/', '-'),
                   (row.get('契約') or '').strip(), (row.get('到期月份(週別)') or '').strip())
            if key in wanted:
                if key in found:
                    raise ValueError('multiple official daily rows match one contract-day')
                found[key] = row
    if set(found) != wanted:
        raise ValueError('official source does not cover every exact contract-day')
    objects = {}
    for day in sorted({key[0] for key in wanted}):
        for dataset in ('TaiwanFuturesTick', 'TaiwanFuturesKBar'):
            objects[(day, dataset)] = read_market_object(root, dataset, day)
    results = []
    for day, product, contract in sorted(wanted):
        key = day, product, contract
        daily, missing = found[key], candidates[key]
        if int(daily['成交量'].strip()) != int(missing['official_volume']):
            raise ValueError('original daily volume disagrees with omission audit')
        ticks = objects[(day, 'TaiwanFuturesTick')][0].filter(
            (pl.col('futures_id') == product) & (pl.col('contract_date') == contract)
            & (pl.col('date').str.slice(0, 10) == day)
            & pl.col('date').str.slice(11, 8).is_between(pl.lit('08:45:00'), pl.lit('13:45:00'))).sort('date')
        bars = objects[(day, 'TaiwanFuturesKBar')][0].filter(
            (pl.col('futures_id') == product) & (pl.col('contract_date') == contract)
            & (pl.col('date') == day) & (pl.col('minute') >= '08:45:00')
            & (pl.col('minute') < '13:45:00')).sort('minute')
        if not ticks.height or ticks.filter((pl.col('volume') <= 0) | (pl.col('volume') % 2 != 0)).height:
            raise ValueError('exact contract source must have positive even native tick volumes')
        short = ticks.filter(pl.col('date').str.slice(11, 8) <= '13:30:00')
        after = ticks.filter(pl.col('date').str.slice(11, 8) > '13:30:00')
        ticks_by_minute = (ticks.with_columns((pl.col('date').str.slice(11, 5) + ':00').alias('minute'))
            .group_by('minute', maintain_order=True).agg(pl.col('volume').sum(),
                pl.col('price').first().alias('open'), pl.col('price').max().alias('high'),
                pl.col('price').min().alias('low'), pl.col('price').last().alias('close')))
        # Multisets retain duplicates and source order within each timestamp.
        cols = ['minute', 'open', 'high', 'low', 'close', 'volume']
        def native_minute_multiset(frame: pl.DataFrame) -> Counter:
            return Counter((minute, *(np.float32(value).view(np.uint32).item() for value in prices), volume)
                for minute, *prices, volume in frame.select(cols).iter_rows())
        tick_minutes = native_minute_multiset(ticks_by_minute)
        kbar_minutes = native_minute_multiset(bars)
        high, low, close = ticks['price'].max(), ticks['price'].min(), ticks['price'][-1]
        results.append(dict(date=day, product=product, contract=contract,
            physical_instance=missing['physical_instance'], official_volume=int(missing['official_volume']),
            official_outright_volume=int(missing['outright_volume']),
            common_envelope_contracts=int(short['volume'].sum() // 2),
            regular_envelope_contracts=int(ticks['volume'].sum() // 2),
            post_1330_contracts=int(after['volume'].sum() // 2),
            regular_exact_outright_volume=(ticks['volume'].sum() == 2 * int(missing['outright_volume'])),
            official_hlc_float32_match=all(f32_equal(a, daily[k]) for a, k in
                ((high, '最高價'), (low, '最低價'), (close, '收盤價'))),
            kbar_tick_minute_multisets_equal=tick_minutes == kbar_minutes,
            tick_minutes_only=sum((tick_minutes-kbar_minutes).values()),
            kbar_minutes_only=sum((kbar_minutes-tick_minutes).values()),
            last_tick_timestamp=ticks['date'][-1]))
    if any(sha256_file(Path(path)) != digest for path, digest in pinned.items()):
        raise ValueError('official daily source or target plan changed during readback')
    for _, proof in objects.values():
        if (sha256_file(Path(proof['receipt'])) != proof['receipt_sha256']
                or sha256_file(Path(proof['payload'])) != proof['payload_sha256']):
            raise ValueError('provider source changed during audit')
    return dict(contract='retrospective_futures_session_envelope_evidence_v1',
        completed_at_utc=datetime.now(UTC).isoformat(), input_sha256=pinned,
        source_objects=[proof for _, proof in objects.values()], rows=results,
        counts=dict(targets=len(results),
            volume_matches=sum(r['regular_exact_outright_volume'] for r in results),
            hlc_matches=sum(r['official_hlc_float32_match'] for r in results),
            kbar_tick_minute_matches=sum(r['kbar_tick_minute_multisets_equal'] for r in results)),
        interpretation='wider real-print envelope matched or mismatched the official daily book; not a session proof',
        market_kbar_and_tick_may_share_upstream_source=True,
        historical_source_delivery_clock_verified=False, final_trading_day_proven=False,
        first_session_accounting_proven=False, execution_price_or_fill=False,
        training_or_financial_admitted=False)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--omissions', type=Path, required=True)
    p.add_argument('--official', type=Path, action='append', required=True)
    p.add_argument('--provider-root', type=Path, default=ROOT / 'data_finmind/complement')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / 'artifacts/markets') or output.exists() or output.is_symlink():
        raise ValueError('choose a new versioned output under artifacts/markets')
    result = audit(args.plan, args.omissions, args.official, args.provider_root)
    atomic_write_json(output, result)
    print(json.dumps({'output': str(output), 'counts': result['counts']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
