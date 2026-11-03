#!/usr/bin/env python3
"""Inspect two-clock reference candidates from retained all-option originals.

Candidate session windows and prior source files are supplied explicitly.
This report is source research, not a model feature or execution dataset.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import json

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import CONTRACT, reference_candidates


def audit(day: date, previous: date, policy_path: Path, manifest_path: Path) -> dict:
    if previous >= day:
        raise ValueError('the previous retained trading-day source must precede the decision date')
    policy_sha = sha256_file(policy_path)
    policy = json.loads(policy_path.read_bytes())
    if policy.get('trading_date') != day.isoformat() or policy.get('research_candidate_only') is not True:
        raise ValueError('a matching research-only session policy is required')
    windows = {key: tuple(row['day_minutes']) for key, row in policy['products'].items()}
    if not windows:
        raise ValueError('no candidate session products')
    sources = pin_official_option_tick_inventory(manifest_path, [previous, day])
    cutoff = datetime(day.year, day.month, day.day, 9, 0, tzinfo=ZoneInfo('Asia/Taipei'))
    batches, input_rows = [], 0
    for item in sources:
        for frame in iter_official_option_trade_batches(item):
            input_rows += frame.height
            prefix = frame.filter(pl.col('event_ts') + pl.duration(seconds=1) <= cutoff)
            if prefix.height:
                batches.append(prefix.select('event_ts', 'product_raw', 'series_raw', 'strike_raw',
                                             'right_raw', 'price_raw', 'source_sha256'))
    if not batches:
        raise ValueError('no authenticated observed prefix rows')
    rows = pl.concat(batches)
    products = set(rows['product_raw'].unique())
    results = {}
    for decision in ('08:45', '09:00'):
        frame = reference_candidates(rows, trading_date=day, decision_time=decision, session_windows=windows)
        kinds = Counter(frame['reference_kind'].to_list())
        examples = {}
        for kind in sorted(kinds):
            sample = frame.filter(pl.col('reference_kind') == kind).head(3)
            examples[kind] = [{key: str(value) if isinstance(value, datetime) else value
                               for key, value in row.items()} for row in sample.to_dicts()]
        results[decision] = {'observed_contract_keys': frame.height, 'reference_kind_counts': dict(kinds),
                             'examples': examples}
    if sha256_file(policy_path) != policy_sha:
        raise ValueError('candidate session policy changed during readback')
    implementation = [Path(__file__).resolve(), Path('stockagent/data/tw_option_product_ticks.py').resolve(),
                      Path('stockagent/data/tw_option_reference_candidates.py').resolve()]
    return {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'decision_date': day.isoformat(), 'previous_source_date': previous.isoformat(),
            'sources': sources, 'policy_path': str(policy_path.resolve()), 'policy_sha256': policy_sha,
            'implementation_sha256': {str(path): sha256_file(path) for path in implementation},
            'raw_side_rows_scanned': input_rows, 'observed_prefix_side_rows': rows.height,
            'products_observed_by_0900': sorted(products),
            'products_without_candidate_session': sorted(products - set(windows)),
            'decisions': results, 'training_ready': False,
            'limitations': [
                'Candidate session policy is not dated listing, expiry, currency or accounting evidence.',
                'Observed contract keys may be expired, not yet listed, or otherwise ineligible at the decision clock.',
                'Two retained sources do not prove complete historical ticks or that a source-first print is the true open.',
                'The contract universe contains only prints observed by the clock; unobserved contracts are not counted.',
                'Only one prior retained source is included; intervening sessions or older last trades may exist.',
                'Whole-second ordering is unknown; multiple prices in the selected second remain ambiguous.',
                'No results in this report are broker fills or are injected into training.',
            ]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--previous-date', type=date.fromisoformat, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the earlier readback')
    result = audit(args.date, args.previous_date, args.policy, args.manifest)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'decisions': result['decisions'],
                      'training_ready': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
