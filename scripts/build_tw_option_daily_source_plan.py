#!/usr/bin/env python3
"""Bind verified annual/monthly official sources into a source-only reader plan."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.taifex_daily_download_common import month_ranges


def build(annual_path: Path, monthly_path: Path) -> dict:
    annual_bytes, monthly_bytes = annual_path.read_bytes(), monthly_path.read_bytes()
    annual, monthly = json.loads(annual_bytes), json.loads(monthly_bytes)
    if (annual.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1'
            or monthly.get('contract') != 'tw_option_all_product_monthly_range_readback_v1'):
        raise ValueError('unsupported option source evidence')
    if sha256_file(Path(annual['source_manifest'])) != annual['source_manifest_sha256']:
        raise ValueError('official annual manifest changed')
    if monthly.get('reader_sha256') != sha256_file(Path('stockagent/data/tw_option_product_daily.py')):
        raise ValueError('monthly source reader changed after readback')
    sources = []
    for source in annual['annual_sources']:
        path = Path(source['path'])
        if path.is_symlink() or not path.is_absolute() or not path.name.endswith('_opt.zip'):
            raise ValueError('invalid annual source path')
        year = int(path.name.removesuffix('_opt.zip'))
        if sha256_file(path) != source['sha256']:
            raise ValueError('annual option source changed')
        sources.append({'path': str(path), 'sha256': source['sha256'], 'start': f'{year}-01-01',
                        'end': f'{year}-12-31', 'kind': 'official_annual_archive'})
    if sorted(item['start'] for item in sources) != [f'{year}-01-01' for year in range(2011, 2026)]:
        raise ValueError('annual source selection is not exactly 2011-2025')
    start, end = date.fromisoformat(monthly['start']), date.fromisoformat(monthly['end'])
    ranges = [(a.isoformat(), b.isoformat()) for a, b in month_ranges(start, end)]
    if (start != date(2026, 1, 1) or monthly['months_verified'] != len(monthly['sources'])
            or [(x['start'], x['end']) for x in monthly['sources']] != ranges):
        raise ValueError('monthly source selection is incomplete')
    for source in monthly['sources']:
        path, receipt_path = Path(source['path']), Path(source['receipt'])
        if path.is_symlink() or receipt_path.is_symlink() or not path.is_absolute():
            raise ValueError('invalid monthly source or receipt path')
        receipt_bytes = receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
        if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                or receipt.get('range_start') != source['start'] or receipt.get('range_end') != source['end']
                or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                or receipt.get('sha256') != source['sha256'] or receipt.get('bytes') != source['bytes']
                or path.stat().st_size != source['bytes'] or sha256_file(path) != source['sha256']
                or receipt_path.read_bytes() != receipt_bytes):
            raise ValueError('monthly source evidence changed')
        sources.append({'path': str(path), 'sha256': source['sha256'], 'start': source['start'],
                        'end': source['end'], 'kind': 'official_monthly_range'})
    if annual_path.read_bytes() != annual_bytes or monthly_path.read_bytes() != monthly_bytes:
        raise ValueError('source evidence changed during plan build')
    return {'contract': 'tw_option_all_product_daily_source_plan_v1',
            'created_at_utc': datetime.now(timezone.utc).isoformat(),
            'annual_evidence': str(annual_path.resolve()), 'annual_evidence_sha256': hashlib.sha256(annual_bytes).hexdigest(),
            'monthly_evidence': str(monthly_path.resolve()), 'monthly_evidence_sha256': hashlib.sha256(monthly_bytes).hexdigest(),
            'reader_sha256': sha256_file(Path('stockagent/data/tw_option_product_daily.py')),
            'start': '2011-01-01', 'end': end.isoformat(), 'sources': sources,
            'limitations': ['Source-selection bounds are not trading-calendar completeness or per-contract eligibility.',
                            'Raw day prices are not decision-clock trades, fills, historical PIT or account rules.',
                            'Overlapping records inside original files remain unchanged.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--annual-evidence', type=Path, required=True)
    parser.add_argument('--monthly-evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.annual_evidence, args.monthly_evidence)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'sha256': sha256_file(args.output),
                      'sources': len(result['sources']), 'end': result['end']}))


if __name__ == '__main__':
    main()
