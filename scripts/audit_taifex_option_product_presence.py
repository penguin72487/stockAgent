#!/usr/bin/env python3
"""Check official positive-volume option products against retained day objects.

This is a product-presence audit, not a contract, executable-price, source-PIT
or full-history proof. Day-session absences and unreported-session candidates
are explicitly separate because trade dates and natural dates can differ.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import csv
import hashlib
import io
import json
from pathlib import Path
import sys
import time

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file


def _set(value: str) -> set[str]:
    return set(filter(None, value.split('|')))


def _signature(path: Path) -> tuple[int, ...]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def audit_day(row: dict, root: Path) -> dict:
    day = row['date']
    expected_day = _set(row['day_positive_products'])
    expected_unreported = _set(row['unreported_session_positive_products'])
    result = {'date': day, 'source_status': row['finmind_status'],
              'official_day_products': len(expected_day),
              'official_unreported_products': len(expected_unreported),
              'finmind_products': '', 'missing_day_products': '', 'missing_unreported_products': ''}
    if row['finmind_status'] != 'complete_payload_sha_verified':
        return result
    try:
        receipt = Path(row['finmind_receipt_path']).resolve()
        parquet = Path(row['finmind_parquet_path']).resolve()
        if not receipt.is_relative_to(root) or not parquet.is_relative_to(root):
            raise ValueError('source path outside FinMind root')
        receipt_bytes = receipt.read_bytes()
        if hashlib.sha256(receipt_bytes).hexdigest() != row['finmind_receipt_sha256']:
            raise ValueError('receipt changed after date inventory')
        source = json.loads(receipt_bytes)
        if (source.get('dataset') != 'TaiwanOptionTick' or source.get('data_id') != ''
                or source.get('partition') != day or source.get('status') != 'complete'
                or (root / source['parquet_path']).resolve() != parquet
                or source.get('sha256') != row['finmind_parquet_sha256']):
            raise ValueError('receipt source identity mismatch')
        before = _signature(parquet)
        if (sha256_file(parquet) != source['sha256'] or before[2] != source['parquet_size_bytes']
                or pq.ParquetFile(parquet).metadata.num_rows != int(source['rows'])):
            raise ValueError('parquet byte or row proof failed')
        values = pl.scan_parquet(parquet).select(pl.col('option_id').unique()).collect(engine='streaming')
        found = {str(x).strip() for x in values['option_id'].to_list() if x is not None and str(x).strip()}
        if _signature(parquet) != before or receipt.read_bytes() != receipt_bytes:
            raise ValueError('source changed during product audit')
        result.update(source_status='product_readback_verified', finmind_products='|'.join(sorted(found)),
                      missing_day_products='|'.join(sorted(expected_day - found)),
                      missing_unreported_products='|'.join(sorted(expected_unreported - found)))
    except (OSError, KeyError, ValueError, TypeError, pl.exceptions.PolarsError, pa.ArrowException) as error:
        result.update(source_status='product_readback_failed', error=str(error)[:256])
    return result


def audit(summary_path: Path, workers: int) -> tuple[list[dict], dict]:
    if not 1 <= workers <= 16:
        raise ValueError('workers must be in 1..16')
    started = time.monotonic()
    summary_bytes = summary_path.read_bytes()
    summary = json.loads(summary_bytes)
    if summary.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1':
        raise ValueError('incompatible coverage summary')
    inventory = Path(summary['day_inventory'])
    inventory_bytes = inventory.read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != summary['day_inventory_sha256']:
        raise ValueError('date inventory changed')
    manifest = Path(summary['source_manifest'])
    if sha256_file(manifest) != summary['source_manifest_sha256']:
        raise ValueError('official manifest changed')
    for source in summary['annual_sources']:
        if sha256_file(Path(source['path'])) != source['sha256']:
            raise ValueError('official annual source changed')
    rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    root = Path(summary['finmind_root']).resolve()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(lambda row: audit_day(row, root), rows):
            results.append(result)
            if len(results) % 250 == 0:
                print(f'checked {len(results)}/{len(rows)} dates', file=sys.stderr, flush=True)
    if summary_path.read_bytes() != summary_bytes or inventory.read_bytes() != inventory_bytes:
        raise ValueError('coverage evidence changed during audit')
    stats = []
    for year in sorted({r['date'][:4] for r in results}):
        group = [r for r in results if r['date'].startswith(year)]
        verified = [r for r in group if r['source_status'] == 'product_readback_verified']
        stats.append({'year': int(year), 'official_positive_dates': len(group),
                      'source_states': dict(sorted(Counter(r['source_status'] for r in group).items())),
                      'day_missing_product_dates': sum(bool(r['missing_day_products']) for r in verified),
                      'day_missing_product_pairs': sum(len(_set(r['missing_day_products'])) for r in verified),
                      'unreported_missing_product_dates': sum(bool(r['missing_unreported_products']) for r in verified),
                      'unreported_missing_product_pairs': sum(len(_set(r['missing_unreported_products'])) for r in verified)})
    return results, {
        'contract': 'taifex_option_daily_vs_finmind_product_presence_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'elapsed_seconds': time.monotonic() - started,
        'workers': workers, 'coverage_summary': str(summary_path.resolve()),
        'coverage_summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
        'inventory_sha256': hashlib.sha256(inventory_bytes).hexdigest(),
        'audit_script_sha256': sha256_file(Path(__file__)), 'by_year': stats,
        'limitations': [
            'A day-session volume/product mismatch is a candidate only: daily volume may include negotiated block trades absent from tick objects.',
            'An unreported-session absence is only a candidate mismatch because trade-date to natural-day mapping is not established here.',
            'Presence of a product does not establish every expiry, strike, right, event, decision clock, point-in-time or executable quote.',
            'Official day evidence and FinMind may derive from the same market source and are not independent trades.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    rows, summary = audit(args.coverage_summary, args.workers)
    if not rows:
        raise ValueError('empty source date inventory')
    buffer = io.StringIO(newline='')
    fields = list(dict.fromkeys(key for row in rows for key in row))
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    data = buffer.getvalue().encode()
    out = args.output_dir / 'option_product_presence_by_day.csv'
    atomic_write_bytes(out, data)
    summary.update(day_inventory=str(out.resolve()), day_inventory_sha256=hashlib.sha256(data).hexdigest())
    atomic_write_json(args.output_dir / 'option_product_presence.summary.json', summary)
    print(json.dumps({'output': str(out), 'by_year': summary['by_year']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
