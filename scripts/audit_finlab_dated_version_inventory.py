#!/usr/bin/env python3
"""Index dated FinLab source snapshots without merging or rewriting revisions.

This read-only inventory locates dates which have rolled out of the current
head. Every listed version is bound to its original receipt and Parquet hash.
Publication timing, record identity, training eligibility and any merge across
different snapshots are deliberately outside this source-level contract.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


def source_version(root: Path, receipt: Path, dataset: str) -> dict:
    receipt_hash = sha256_file(receipt)
    record = json.loads(receipt.read_text(encoding='utf-8'))
    if record.get('dataset') != dataset:
        raise ValueError('dataset mismatch')
    relative = Path(record['parquet_path'])
    if relative.is_absolute() or '..' in relative.parts or relative.parts[:1] != ('datasets',):
        raise ValueError('unsafe object path')
    obj = root / relative
    digest = record.get('sha256')
    if not isinstance(digest, str) or len(digest) != 64 or sha256_file(obj) != digest:
        raise ValueError('object SHA mismatch')
    metadata = pq.ParquetFile(obj).metadata
    if metadata.num_rows != record.get('rows'):
        raise ValueError('source row count mismatch')
    if 'date' not in metadata.schema.names:
        raise ValueError('source has no explicit date column')
    counts = (pl.scan_parquet(obj)
              .select(pl.col('date').cast(pl.Date, strict=False).alias('date'))
              .group_by('date').agg(pl.len().alias('rows'))
              .sort('date').collect())
    days, null_rows = [], 0
    for day, count in counts.iter_rows():
        if day is None:
            null_rows = count
        else:
            days.append({'date': day.isoformat(), 'rows': count})
    if sha256_file(receipt) != receipt_hash or sha256_file(obj) != digest:
        raise ValueError('source changed during read')
    return {
        'receipt': str(receipt.resolve()), 'receipt_sha256': receipt_hash,
        'object': str(obj.resolve()), 'object_sha256': digest,
        'rows': metadata.num_rows, 'null_or_unparsed_date_rows': null_rows,
        'fetched_at_utc': record.get('fetched_at_utc'),
        'acquisition_completed_at_utc': record.get('acquisition_completed_at_utc'),
        'source_checked_at_utc': record.get('source_checked_at_utc'),
        'publication_time_status': record.get('publication_time_status'),
        'days': days,
    }


def inventory(root: Path, current_receipt: Path, dataset: str) -> dict:
    if current_receipt.parent.resolve() != (root / 'receipts').resolve():
        raise ValueError('current receipt must be under the native receipts directory')
    archives = sorted((root / 'versions' / current_receipt.stem).glob('*.json'))
    paths = [current_receipt, *archives]
    versions, failures = [], []
    for index, receipt in enumerate(paths):
        try:
            version = source_version(root, receipt, dataset)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            failures.append({'receipt': str(receipt.resolve()), 'reason': str(exc)})
            continue
        version['is_current_head'] = index == 0
        versions.append(version)
    current = next((x for x in versions if x['is_current_head']), None)
    current_days = {x['date'] for x in current['days']} if current else set()
    all_days = {day['date'] for v in versions for day in v['days']}
    return {
        'contract': 'finlab_dated_native_version_inventory_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'dataset': dataset, 'native_root': str(root.resolve()),
        'current_head_verified': current is not None,
        'verified_versions': len(versions), 'failed_versions': failures,
        'verified_event_dates_any_version': len(all_days),
        'event_dates_absent_from_current_but_in_verified_versions': sorted(all_days-current_days)
        if current is not None else None,
        'first_verified_source_date': min(all_days) if all_days else None,
        'last_verified_source_date': max(all_days) if all_days else None,
        'versions': versions,
        'limitations': [
            'Source date presence is not publication-time, revision-history or completeness proof.',
            'Snapshots remain separate; no latest-value or point-in-time merge has been selected.',
            'An absent acquisition completion timestamp cannot establish historical availability.',
            'The inventory does not change the native head or admit data to training.',
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finlab'))
    parser.add_argument('--current-receipt', type=Path, required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve earlier inventory; use a new output')
    result = inventory(args.root, args.current_receipt, args.dataset)
    result['audit_sha256'] = sha256_file(Path(__file__))
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('contract', 'dataset', 'verified_versions',
          'failed_versions', 'verified_event_dates_any_version',
          'event_dates_absent_from_current_but_in_verified_versions',
          'first_verified_source_date', 'last_verified_source_date')}, ensure_ascii=False))
    return 0 if result['current_head_verified'] and not result['failed_versions'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
