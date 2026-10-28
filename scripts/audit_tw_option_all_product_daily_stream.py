#!/usr/bin/env python3
"""Reconcile an all-product option source stream to a pinned full inventory."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches


def audit(summary_path: Path, first: int, last: int) -> dict:
    if first != 2011 or last != 2025:
        raise ValueError('the pinned comparison inventory is exactly 2011 through 2025')
    summary_bytes = summary_path.read_bytes()
    reference = json.loads(summary_bytes)
    inventory_path = Path(reference['product_inventory'])
    if (reference.get('contract') != 'taifex_option_archive_product_day_inventory_v1'
            or sha256_file(inventory_path) != reference['product_inventory_sha256']):
        raise ValueError('pinned annual inventory differs')
    with inventory_path.open(encoding='utf-8', newline='') as source:
        expected = {row['product']: int(row['source_rows']) for row in csv.DictReader(source)}
    counts: Counter[str] = Counter()
    years = []
    sessions: Counter[str] = Counter()
    series: Counter[str] = Counter()
    for year in range(first, last + 1):
        sources = [row for row in reference['annual_inputs'] if Path(row['path']).name == f'{year}_opt.zip']
        if len(sources) != 1:
            raise ValueError(f'missing or ambiguous annual inventory source: {year}')
        source = sources[0]
        began = time.monotonic()
        rows = 0
        for batch in iter_option_product_daily_batches(source['path'], expected_sha256=source['sha256']):
            rows += batch.height
            for product, count in batch.group_by('product').agg(pl.len()).iter_rows():
                counts[product] += count
            for value, count in batch.group_by('session').agg(pl.len()).iter_rows():
                sessions[value] += count
            for value, count in batch.group_by('series_scope').agg(pl.len()).iter_rows():
                series[value] += count
        record = {'year': year, 'source_path': source['path'], 'source_sha256': source['sha256'],
                  'raw_rows': rows, 'elapsed_seconds': time.monotonic() - began}
        years.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
    if summary_path.read_bytes() != summary_bytes or sha256_file(inventory_path) != reference['product_inventory_sha256']:
        raise ValueError('comparison inventory changed during readback')
    differences = {product: {'observed': counts[product], 'expected': expected.get(product, 0)}
                   for product in sorted(set(counts) | set(expected))
                   if counts[product] != expected.get(product, 0)}
    if differences:
        raise ValueError(f'all-product source row counts disagree: {str(differences)[:500]}')
    implementation = Path(__file__).resolve().parents[1] / 'stockagent/data/tw_option_product_daily.py'
    return {'contract': 'tw_option_all_product_daily_full_inventory_readback_v1',
            'completed_at_utc': datetime.now(timezone.utc).isoformat(),
            'reference_summary_path': str(summary_path.resolve()),
            'reference_summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
            'reference_inventory_sha256': reference['product_inventory_sha256'],
            'implementation_sha256': sha256_file(implementation), 'years': years,
            'source_rows': sum(counts.values()), 'product_count': len(counts),
            'sessions': dict(sorted(sessions.items())), 'series_scopes': dict(sorted(series.items())),
            'per_product_counts_equal': True,
            'limitations': ['Source row counts do not establish contract life, prices, PIT access or execution.',
                            'Overlapping archive members and unreported sessions are preserved.',
                            'All-product source iteration is not a completed training dataset.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--first-year', type=int, default=2011)
    parser.add_argument('--last-year', type=int, default=2025)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.summary, args.first_year, args.last_year)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write('\n')
    print(json.dumps({'output': str(args.output), 'source_rows': result['source_rows'],
                      'product_count': result['product_count']}), flush=True)


if __name__ == '__main__':
    main()
