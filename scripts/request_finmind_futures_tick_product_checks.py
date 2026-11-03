#!/usr/bin/env python3
"""Register evidenced per-product futures tick checks through the existing owner."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import re

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.finmind_futures_tick_conflicts import BATCHES, load_futures_tick_conflicts
from scripts.request_finmind_option_tick_product_checks import register_evidenced

DATASET = 'TaiwanFuturesTick'
CONTRACT = 'finmind_futures_tick_product_source_check_v1'


def evidence(root: Path, selections: list[tuple[str, str]], now: datetime) -> list[dict]:
    root = root.resolve()
    if now.tzinfo is None or not selections or len(set(selections)) != len(selections):
        raise ValueError('unique dated product selections and an aware clock are required')
    wanted = set(selections)
    for day, product in wanted:
        if (date.fromisoformat(day).isoformat() != day or date.fromisoformat(day) > now.date()
                or not re.fullmatch(r'[A-Z0-9]{2,12}', product)):
            raise ValueError('invalid product or physical source date')
    by_day = {}
    for path in sorted((root / BATCHES).glob('*.json')):
        raw = path.read_bytes()
        metadata = json.loads(raw)
        if not ({day for day, _ in wanted} & set(metadata.get('dates', []))):
            continue
        loaded = load_futures_tick_conflicts(root, now, sidecar=path)
        if not loaded:
            raise ValueError('source conflict evidence expired')
        expiry = datetime.fromisoformat(metadata['expires_at_utc'].replace('Z', '+00:00'))
        for item in loaded[0]['requests']:
            if item['start_date'] in by_day:
                raise ValueError('source date occurs in multiple conflict plans')
            by_day[item['start_date']] = (item, path, sha256_file(path), expiry)
        if path.read_bytes() != raw:
            raise ValueError('source conflict plan changed during verification')
    checked = []
    current_by_day = {}
    for day, product in sorted(wanted):
        if day not in by_day:
            raise ValueError('no active official-source conflict for requested date')
        item, sidecar, sidecar_sha, expiry = by_day[day]
        expected = [key for key in item['expected_source_futures_contracts'] if key[0] == product]
        if not expected or 'original_parquet_path' not in item:
            raise ValueError('requested product lacks a pinned prior source and audited missing contract')
        baseline = (root / item['original_parquet_path']).resolve()
        if day not in current_by_day:
            receipt = root / 'receipts' / DATASET / 'all' / f'{day}.json'
            receipt_bytes = receipt.read_bytes()
            source = json.loads(receipt_bytes)
            if (source.get('dataset') != DATASET or source.get('data_id') != ''
                    or source.get('partition') != day or source.get('status') != 'complete'):
                raise ValueError('current whole-market source receipt does not match')
            parquet = (root / source['parquet_path']).resolve()
            if (not parquet.is_relative_to(root) or not baseline.is_relative_to(root)
                    or sha256_file(parquet) != source['sha256']
                    or sha256_file(baseline) != item['original_parquet_sha256']
                    or pq.ParquetFile(parquet).metadata.num_rows != source['rows']):
                raise ValueError('current or prior whole-market payload does not match its receipt')
            actual = (pl.scan_parquet(parquet).filter(pl.col('volume') > 0).select(
                pl.col('futures_id').cast(pl.String).str.strip_chars().str.to_uppercase(),
                pl.col('contract_date').cast(pl.String).str.strip_chars().str.to_uppercase(),
            ).unique().collect(engine='streaming'))
            if receipt.read_bytes() != receipt_bytes or sha256_file(parquet) != source['sha256']:
                raise ValueError('current source changed during verification')
            current_by_day[day] = (receipt, parquet, source, set(actual.iter_rows()))
        receipt, parquet, source, found = current_by_day[day]
        missing = [key for key in expected if tuple(key) not in found]
        if not missing:
            raise ValueError('the current whole-market response already has the audited product contracts')
        checked.append({'dataset': DATASET, 'data_id': product, 'partition': day,
                        'audited_missing_contracts': missing,
                        'original_receipt': str(receipt), 'original_receipt_sha256': sha256_file(receipt),
                        'original_parquet': str(parquet), 'original_parquet_sha256': source['sha256'],
                        'baseline_parquet': str(baseline), 'baseline_parquet_sha256': item['original_parquet_sha256'],
                        'conflict_plan': str(sidecar), 'conflict_plan_sha256': sidecar_sha,
                        'conflict_plan_expires_at_utc': expiry.isoformat()})
    return checked


def register_checks(root: Path, selections: list[tuple[str, str]], now: datetime, output: Path) -> dict:
    return register_evidenced(root, evidence(root, selections, now), now, output, dataset=DATASET,
                             contract=CONTRACT, reason='Compare audited missing futures contract-days with the documented per-product source, preserving both whole-market versions.')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--product-day', nargs=2, metavar=('DATE', 'PRODUCT'), action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(register_checks(args.root, [tuple(x) for x in args.product_day], datetime.now(UTC), args.output),
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
