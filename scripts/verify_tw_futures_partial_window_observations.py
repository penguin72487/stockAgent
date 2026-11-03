#!/usr/bin/env python3
"""Independently recompute positive candidate windows from pinned native ticks.

This readback uses the same vendor originals, not an independent provider.
It certifies arithmetic and identity only, never completeness or order fills.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sys

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file


def independent_window(rows, *, day: str, decision_time: str, window_minutes: int,
                       tick_kind: str | None = None, tick_size: float | None = None,
                       session_start: str | None = None, session_end: str | None = None):
    """Recalculate a fixed post-decision window without minute bar code."""
    start = datetime.fromisoformat(day + 'T' + decision_time) + timedelta(minutes=1)
    end = start + timedelta(minutes=window_minutes)
    selected = []
    start_session = (datetime.fromisoformat(day + 'T' + session_start)
                     if session_start is not None else None)
    end_session = (datetime.fromisoformat(day + 'T' + session_end)
                   if session_end is not None else None)
    for timestamp, price, raw_volume in rows:
        when = datetime.fromisoformat(timestamp)
        if (not start <= when < end or (start_session is not None and when < start_session)
                or (end_session is not None and when > end_session)):
            continue
        if raw_volume <= 0 or raw_volume % 2:
            raise ValueError('noninteger price or invalid double-sided quantity')
        selected.append((float(price), raw_volume // 2))
    if not selected:
        return None
    prices = np.asarray([item[0] for item in selected], dtype=np.float64)
    if tick_kind is None:
        if not np.isfinite(prices).all() or np.any(prices != np.rint(prices)):
            raise ValueError('noninteger price or invalid double-sided quantity')
        decoded = prices
    else:
        # Apply the bound exchange grid, but independently accumulate native
        # prints here rather than using the producer's minute-bar reducer.
        # Confirm exact Float32 round trips and reject ambiguous neighbors.
        if not np.isfinite(prices).all() or np.any(prices <= 0):
            raise ValueError('invalid native price')
        if tick_kind == 'fixed':
            if tick_size is None or not math.isfinite(tick_size) or tick_size <= 0:
                raise ValueError('fixed product lacks a dated tick size')
            ticks = np.full(prices.shape, tick_size, dtype=np.float64)
        elif tick_kind in {'stock_future', 'etf_future'}:
            from stockagent.data.tw_price_rules import tick_size_numpy
            ticks = tick_size_numpy(prices, np.datetime64(day), security_types=tick_kind)
        else:
            raise ValueError('unsupported dated tick kind')
        encoded = prices.astype(np.float32)
        decoded = np.round(np.rint(prices / ticks) * ticks, decimals=12)
        if (not np.array_equal(prices, encoded.astype(np.float64))
                or not np.array_equal(decoded.astype(np.float32), encoded)
                or np.any((decoded + ticks).astype(np.float32) == encoded)
                or np.any((decoded - ticks).astype(np.float32) == encoded)):
            raise ValueError('native price has no unique dated legal tick')
    units = [item[1] for item in selected]
    volume = sum(units)
    return volume, math.fsum(float(price) * quantity for price, quantity in zip(decoded, units)) / volume


def _bound_metadata(candidates: list[dict], daily: Path, specifications: Path, diagnostic: dict) -> dict:
    """Resolve each candidate's original dated contract rules before readback."""
    from stockagent.data.tw_futures_decision_clock import bind_entry_specifications
    from stockagent.data.tw_futures_margin_release import read_bound_output

    if (sha256_file(daily) != diagnostic['sources']['daily_sha256']
            or sha256_file(specifications) != diagnostic['sources']['specifications_sha256']):
        raise ValueError('dated verification inputs differ from the diagnostic')
    specs, proof = read_bound_output(specifications)
    if proof.get('dataset') != 'taifex_dated_product_specifications':
        raise ValueError('dated verification specification differs')
    if not candidates:
        return {}
    identities = pl.DataFrame([{'date': row['date'], 'physical_contract': row['physical_contract']}
                               for row in candidates]).unique()
    cols = ['date', 'physical_contract', 'product', 'contract', 'asset_class', 'official_expiry']
    daily_rows = (pl.scan_parquet(daily).select(cols).join(identities.lazy(),
        on=['date', 'physical_contract'], how='semi').collect())
    if daily_rows.height != identities.height or daily_rows.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('dated verification contract identity differs')
    bound = bind_entry_specifications(daily_rows, specs)
    metadata = {}
    for row in bound.to_dicts():
        final = row['official_expiry']
        policy = row['spec_final_cash_accounting_policy']
        if policy not in {'expiry_event_time', 'official_final_day_end_v1'}:
            raise ValueError('unknown dated final cash accounting policy')
        regular, expiry = row['spec_settlement_time'], row['spec_expiry_time']
        if final is None or policy == 'official_final_day_end_v1':
            end = min(regular, expiry)
        elif row['date'] == final:
            end = expiry
        elif row['date'] < final:
            end = regular
        else:
            raise ValueError('candidate occurs after recorded final settlement')
        metadata[(str(row['date']), row['physical_contract'])] = {
            'product': row['product'], 'contract': row['contract'],
            'tick_kind': row['spec_tick_kind'], 'tick_size': row['spec_tick_size'],
            'session_start': row['spec_opening_time'], 'session_end': end}
    for row in candidates:
        identity = metadata[(str(row['date']), row['physical_contract'])]
        if row['product'] != identity['product'] or row['contract'] != identity['contract']:
            raise ValueError('candidate differs from its dated physical identity')
    return metadata


def verify(*, diagnostic_manifest: Path, checkpoint_manifest: Path, report: Path,
           daily: Path | None = None, specifications: Path | None = None) -> dict:
    diagnostic_sha = sha256_file(diagnostic_manifest)
    diagnostic = json.loads(diagnostic_manifest.read_text())
    if (diagnostic.get('contract') != 'tw_futures_partial_window_observations_diagnostic_v1'
            or diagnostic.get('research_only') is not True
            or diagnostic.get('training_ready') is not False):
        raise ValueError('candidate diagnostic identity differs')
    if sha256_file(checkpoint_manifest) != diagnostic['sources']['checkpoint_manifest_sha256']:
        raise ValueError('checkpoint manifest changed')
    checkpoint = json.loads(checkpoint_manifest.read_text())
    data = Path(diagnostic['candidate_file']['path'])
    if sha256_file(data) != diagnostic['candidate_file']['sha256']:
        raise ValueError('candidate bytes changed')
    candidates = pq.read_table(data).to_pylist()
    if len(candidates) != diagnostic['candidate_rows']:
        raise ValueError('candidate row count differs')
    if (daily is None) != (specifications is None):
        raise ValueError('all-product verification requires both daily and specifications')
    metadata = _bound_metadata(candidates, daily, specifications, diagnostic) if daily is not None else None
    by_day = defaultdict(list)
    for row in candidates:
        if metadata is None and row['product'] not in {'I5F', 'TX'}:
            raise ValueError('independent integer tick assumption is limited to I5F and TX')
        by_day[str(row['date'])].append(row)
    objects = {item['date']: item for item in checkpoint['identity']['objects']
               if item['date'] in by_day}
    if set(objects) != set(by_day):
        raise ValueError('missing pinned daily original')
    checked = 0
    max_difference = 0.0
    for day, rows in sorted(by_day.items()):
        item = objects[day]
        original, receipt = Path(item['path']), Path(item['receipt_path'])
        if sha256_file(original) != item['sha256'] or sha256_file(receipt) != item['receipt_sha256']:
            raise ValueError('original or receipt SHA changed')
        table = pq.read_table(original, columns=['date', 'futures_id', 'contract_date', 'price', 'volume'])
        if table.num_rows != item['rows']:
            raise ValueError('original row count changed')
        products = sorted({row['product'] for row in rows})
        table = table.filter(pc.is_in(table['futures_id'], value_set=pa.array(products)))
        events = defaultdict(list)
        for raw in table.to_pylist():
            events[(raw['futures_id'], raw['contract_date'])].append(
                (raw['date'], raw['price'], raw['volume']))
        for row in rows:
            if row['source_file_sha256'] != item['sha256']:
                raise ValueError('candidate and native source differ')
            terms = metadata[(day, row['physical_contract'])] if metadata is not None else {}
            options = ({name: terms[name] for name in ('tick_kind', 'tick_size', 'session_start', 'session_end')}
                       if metadata is not None else {})
            actual = independent_window(events[(row['product'], row['contract'])],
                day=day, decision_time=row['decision_time'], window_minutes=row['window_minutes'], **options)
            if actual is None or actual[0] != row['entry_volume']:
                raise ValueError('independent observed volume differs')
            difference = abs(actual[1] - row['entry_quote'])
            max_difference = max(max_difference, difference)
            if difference > 1e-8 + 1e-12 * abs(row['entry_quote']):
                raise ValueError('independent observed VWAP differs')
            checked += 1
        if sha256_file(original) != item['sha256'] or sha256_file(receipt) != item['receipt_sha256']:
            raise ValueError('original changed during readback')
    if (checked != len(candidates) or sha256_file(data) != diagnostic['candidate_file']['sha256']
            or sha256_file(diagnostic_manifest) != diagnostic_sha
            or sha256_file(checkpoint_manifest) != diagnostic['sources']['checkpoint_manifest_sha256']
            or (metadata is not None and (sha256_file(daily) != diagnostic['sources']['daily_sha256']
                or sha256_file(specifications) != diagnostic['sources']['specifications_sha256']))):
        raise ValueError('candidate set changed during readback')
    result = dict(contract='tw_futures_partial_window_independent_arithmetic_v1',
        completed_at_utc=datetime.now(timezone.utc).isoformat(), rows_checked=checked,
        source_days=len(objects), max_absolute_vwap_difference=max_difference,
        independent_implementation=True, independent_market_source=False,
        historical_point_in_time_verified=False, full_window_completeness_verified=False,
        broker_fill_verified=False, training_admitted=False,
        candidate_sha256=diagnostic['candidate_file']['sha256'],
        diagnostic_manifest_sha256=diagnostic_sha,
        checkpoint_manifest_sha256=diagnostic['sources']['checkpoint_manifest_sha256'],
        dated_all_product_rules_verified=metadata is not None)
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--diagnostic-manifest', required=True, type=Path)
    parser.add_argument('--checkpoint-manifest', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--daily', type=Path)
    parser.add_argument('--specifications', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(diagnostic_manifest=args.diagnostic_manifest,
        checkpoint_manifest=args.checkpoint_manifest, report=args.report, daily=args.daily,
        specifications=args.specifications)), flush=True)


if __name__ == '__main__':
    main()
