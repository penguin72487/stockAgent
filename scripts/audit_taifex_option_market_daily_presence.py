#!/usr/bin/env python3
"""Compare official positive option product-days to retained FinMind daily data.

The FinMind input is its original all-product day partition, not the separately
downloaded tick dataset or a product-history request.  A matching daily row
is source presence, not a historical clock, fill, PIT, or accounting proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import UTC, date, datetime
import hashlib
import io
import json
from pathlib import Path
import sys
import time

import polars as pl

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from downloader.parquet_integrity import parquet_receipt_error, signature
from scripts.audit_taifex_option_product_presence import _set
from scripts.taifex_daily_download_common import month_ranges


GROUPS = ('day', 'unreported_session', 'night', 'unknown_session')


def audit_day(row: dict, root: Path) -> dict:
    day = row['date']
    expected = {group: _set(row[f'{group}_positive_products']) for group in GROUPS}
    result = {'date': day, 'source_status': 'missing_receipt', 'source_kind': '', 'receipt_sha256': '',
              'parquet_sha256': '', 'source_rows': '', 'vendor_products': '',
              'vendor_positive_volume_products': '', 'vendor_trading_sessions': '',
              'error': ''}
    for group, products in expected.items():
        result[f'official_{group}_positive_products'] = '|'.join(sorted(products))
        result[f'missing_{group}_products'] = ''
        result[f'missing_{group}_positive_volume_products'] = ''
    try:
        receipt_path = root / 'receipts' / 'TaiwanOptionDaily' / 'all' / f'{day}.json'
        if not receipt_path.exists():
            return result
        if receipt_path.is_symlink() or not receipt_path.resolve().is_relative_to(root.resolve()):
            raise ValueError('unsafe receipt path')
        receipt_bytes = receipt_path.read_bytes()
        result['receipt_sha256'] = hashlib.sha256(receipt_bytes).hexdigest()
        receipt = json.loads(receipt_bytes)
        if any(receipt.get(k) != v for k, v in
               {'dataset': 'TaiwanOptionDaily', 'data_id': '', 'partition': day}.items()):
            raise ValueError('receipt identity mismatch')
        kind = receipt.get('kind')
        # The native owner migrated old start-only month queries to day tasks
        # without rewriting their source receipts.  Accept only the actual
        # observed first day, retaining its original query kind; a month
        # receipt is never evidence for the rest of the month.
        if kind not in ('day', 'month') or (kind == 'month' and
                (not day.endswith('-01') or receipt.get('source_first_date') != day
                 or receipt.get('source_last_date') != day)):
            raise ValueError('unsupported source query shape')
        result['source_kind'] = kind
        if receipt.get('status') == 'observed_empty':
            if receipt.get('rows') != 0:
                raise ValueError('empty receipt has nonzero rows')
            if receipt_path.read_bytes() != receipt_bytes:
                raise ValueError('receipt changed during read')
            result['source_status'] = 'observed_empty'
            return result
        if receipt.get('status') != 'complete':
            raise ValueError('unexpected receipt status')
        error = parquet_receipt_error(root, receipt, verify_hash=True)
        if error:
            raise ValueError(error)
        parquet = root / receipt['parquet_path']
        before = signature(parquet)
        frame = pl.scan_parquet(parquet).select(
            pl.col('date').unique().sort().implode().alias('dates'),
            pl.col('option_id').str.strip_chars().str.to_uppercase().drop_nulls().unique().sort().implode().alias('products'),
            pl.col('option_id').filter(pl.col('volume') > 0).str.strip_chars().str.to_uppercase()
              .drop_nulls().unique().sort().implode().alias('positive_products'),
            pl.col('trading_session').drop_nulls().unique().sort().implode().alias('sessions'),
            pl.col('date').null_count().alias('null_dates'),
            (pl.col('option_id').is_null() | (pl.col('option_id').str.strip_chars() == '')).sum().alias('bad_products'),
        ).collect(engine='streaming').row(0, named=True)
        if frame['dates'] != [day] or frame['null_dates'] or frame['bad_products']:
            raise ValueError('invalid row date or product identity')
        if signature(parquet) != before or receipt_path.read_bytes() != receipt_bytes:
            raise ValueError('source changed during product audit')
        products = set(frame['products'])
        positive = set(frame['positive_products'])
        result.update(source_status='daily_readback_verified', parquet_sha256=receipt['sha256'],
                      source_rows=receipt['rows'], vendor_products='|'.join(sorted(products)),
                      vendor_positive_volume_products='|'.join(sorted(positive)),
                      vendor_trading_sessions='|'.join(frame['sessions']))
        for group, values in expected.items():
            result[f'missing_{group}_products'] = '|'.join(sorted(values - products))
            result[f'missing_{group}_positive_volume_products'] = '|'.join(sorted(values - positive))
    except (OSError, ValueError, KeyError, TypeError, pl.exceptions.PolarsError) as error:
        result.update(source_status='daily_readback_failed', error=str(error)[:256])
    return result


def audit(summary_path: Path, root: Path, workers: int) -> tuple[list[dict], dict]:
    if not 1 <= workers <= 16:
        raise ValueError('workers must be in 1..16')
    started = time.monotonic()
    summary_bytes = summary_path.read_bytes()
    coverage = json.loads(summary_bytes)
    if coverage.get('contract') == 'taifex_option_annual_positive_days_finmind_receipts_v1':
        inventory = Path(coverage['day_inventory'])
        inventory_bytes = inventory.read_bytes()
        if hashlib.sha256(inventory_bytes).hexdigest() != coverage['day_inventory_sha256']:
            raise ValueError('official date inventory changed')
        manifest = Path(coverage['source_manifest'])
        if sha256_file(manifest) != coverage['source_manifest_sha256']:
            raise ValueError('official manifest changed')
        for source in coverage['annual_sources']:
            if sha256_file(Path(source['path'])) != source['sha256']:
                raise ValueError('official annual source changed')
        rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    elif coverage.get('contract') == 'tw_option_all_product_monthly_range_readback_v1':
        inventory = summary_path
        inventory_bytes = summary_bytes
        if (coverage.get('audit_sha256') != sha256_file(Path('scripts/audit_tw_option_daily_ranges.py'))
                or coverage.get('reader_sha256') != sha256_file(Path('stockagent/data/tw_option_product_daily.py'))):
            raise ValueError('monthly inventory implementation changed')
        start, end = date.fromisoformat(coverage['start']), date.fromisoformat(coverage['end'])
        sources = coverage['sources']
        ranges = [(first.isoformat(), last.isoformat()) for first, last in month_ranges(start, end)]
        if (coverage['months_verified'] != len(sources)
                or [(source['start'], source['end']) for source in sources] != ranges):
            raise ValueError('monthly source ranges changed')
        for source in sources:
            path, receipt_path = Path(source['path']), Path(source['receipt'])
            if path.is_symlink() or receipt_path.is_symlink():
                raise ValueError('redirected monthly source or receipt')
            receipt = json.loads(receipt_path.read_bytes())
            if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                    or receipt.get('range_start') != source['start'] or receipt.get('range_end') != source['end']
                    or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                    or receipt.get('sha256') != source['sha256'] or receipt.get('bytes') != source['bytes']
                    or path.stat().st_size != source['bytes'] or sha256_file(path) != source['sha256']):
                raise ValueError('monthly source or receipt differs from verified inventory')
        rows = coverage['daily_positive_products']
        if any(not start <= date.fromisoformat(row['date']) <= end for row in rows):
            raise ValueError('official daily inventory outside monthly source range')
    else:
        raise ValueError('incompatible official coverage summary')
    if not rows or len({r['date'] for r in rows}) != len(rows):
        raise ValueError('empty or duplicate official day inventory')
    root = root.resolve()
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(lambda row: audit_day(row, root), rows):
            results.append(result)
            if len(results) % 250 == 0:
                print(f'checked {len(results)}/{len(rows)} dates', file=sys.stderr, flush=True)
    if summary_path.read_bytes() != summary_bytes or inventory.read_bytes() != inventory_bytes:
        raise ValueError('official coverage evidence changed during audit')
    stats = []
    for year in sorted({r['date'][:4] for r in results}):
        group = [r for r in results if r['date'].startswith(year)]
        verified = [r for r in group if r['source_status'] == 'daily_readback_verified']
        totals = {'year': int(year), 'official_positive_dates': len(group),
                  'source_states': dict(sorted(Counter(r['source_status'] for r in group).items()))}
        for session in GROUPS:
            for suffix in ('products', 'positive_volume_products'):
                key = f'missing_{session}_{suffix}'
                totals[f'{key}_dates'] = sum(bool(r[key]) for r in verified)
                totals[f'{key}_pairs'] = sum(len(_set(r[key])) for r in verified)
        stats.append(totals)
    return results, {
        'contract': 'taifex_option_vs_finmind_all_market_daily_presence_v2',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'elapsed_seconds': time.monotonic() - started,
        'workers': workers, 'finmind_daily_root': str(root),
        'official_coverage_summary': str(summary_path.resolve()),
        'official_coverage_summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
        'official_inventory_sha256': hashlib.sha256(inventory_bytes).hexdigest(),
        'audit_script_sha256': sha256_file(Path(__file__)), 'by_year': stats,
        'limitations': [
            'A verified daily partition is an observed response, not vendor completeness or historical PIT.',
            'Legacy monthly source receipts are accepted only for their observed partition date and retain their original kind.',
            'Official session classes and vendor session labels are retained separately; no assumed session mapping is applied.',
            'Positive-volume product discrepancies are candidates; date/session attribution and negotiated trades can differ.',
            'A product row is not contract, expiration, strike, right, decision-clock, fill, margin or accounting evidence.',
            'Official and FinMind records may have a common upstream source and are not independent trades.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--finmind-daily-root', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    rows, summary = audit(args.coverage_summary, args.finmind_daily_root, args.workers)
    buffer = io.StringIO(newline='')
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    data = buffer.getvalue().encode()
    out = args.output_dir / 'official_vs_finmind_daily_products_by_day.csv'
    atomic_write_bytes(out, data)
    summary.update(day_results=str(out.resolve()), day_results_sha256=hashlib.sha256(data).hexdigest())
    atomic_write_json(args.output_dir / 'official_vs_finmind_daily_products.summary.json', summary)
    print(json.dumps({'output': str(out), 'by_year': summary['by_year']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
