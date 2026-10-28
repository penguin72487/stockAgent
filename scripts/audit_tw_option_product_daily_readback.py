#!/usr/bin/env python3
"""Read selected real dates from pinned all-product annual option archives."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches


def audit(root: Path, manifest_path: Path, dates: list[date]) -> dict:
    if not dates or len(dates) != len(set(dates)):
        raise ValueError('readback requires distinct dates')
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    output = []
    for year in sorted({day.year for day in dates}):
        source = root / f'{year}_opt.zip'
        receipts = [item for item in manifest['receipts']
                    if Path(item['path']).name == source.name and '/annual/' in item['path']]
        if len(receipts) != 1:
            raise ValueError('missing or ambiguous official annual receipt')
        start = time.monotonic()
        requested = {day for day in dates if day.year == year}
        frames = list(iter_option_product_daily_batches(source, expected_sha256=receipts[0]['sha256'],
                                                        trading_dates=requested))
        elapsed = time.monotonic() - start
        if not frames:
            raise ValueError('no matching rows for requested annual dates')
        frame = pl.concat(frames)
        for day in sorted(requested):
            part = frame.filter(pl.col('date') == day)
            if part.is_empty():
                raise ValueError(f'requested date has no source rows: {day}')
            counts = Counter(part['product'].to_list())
            output.append({'date': day.isoformat(), 'source_path': str(source.resolve()),
                           'source_sha256': receipts[0]['sha256'], 'raw_rows': part.height,
                           'product_count': len(counts), 'rows_per_product': dict(sorted(counts.items())),
                           'sessions': dict(sorted(Counter(part['session'].to_list()).items())),
                           'series_scopes': dict(sorted(Counter(part['series_scope'].to_list()).items())),
                           'year_source_read_elapsed_seconds': elapsed})
        print(json.dumps({'year': year, 'dates': len(requested), 'raw_rows': frame.height,
                          'elapsed_seconds': elapsed}), flush=True)
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError('official daily manifest changed during readback')
    implementation = Path(__file__).resolve().parents[1] / 'stockagent/data/tw_option_product_daily.py'
    return {'contract': 'tw_option_all_product_daily_source_readback_v1',
            'completed_at_utc': datetime.now(timezone.utc).isoformat(),
            'source_manifest': str(manifest_path.resolve()),
            'source_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
            'implementation_sha256': sha256_file(implementation), 'dates': output,
            'limitations': ['Daily prices are first/last prints and not decision-clock execution prices.',
                            'Daily source availability, lifecycle, currency and accounting are not admitted.',
                            'Archive member rows may overlap; raw counts retain duplicates.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_tw_index_options_daily/raw/annual'))
    parser.add_argument('--manifest', type=Path, default=Path('data_tw_index_options_daily/manifest.json'))
    parser.add_argument('--date', type=date.fromisoformat, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.root, args.manifest, args.date)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write('\n')
    print(json.dumps({'output': str(args.output), 'dates': len(result['dates'])}), flush=True)


if __name__ == '__main__':
    main()
