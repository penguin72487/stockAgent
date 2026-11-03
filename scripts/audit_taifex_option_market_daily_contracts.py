#!/usr/bin/env python3
"""Read back option contract identities in receipted all-market daily data.

Compares only official positive-volume/open outright month/week keys from day
or unreported sessions. It does not establish decision-clock prices, fills,
complete tick history, historical availability, or option-account readiness.
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
from downloader.finmind_option_contract_keys import vendor_option_series
from downloader.parquet_integrity import parquet_receipt_error, signature
from scripts.audit_taifex_option_contract_presence import KEYS, official_year
from scripts.taifex_daily_download_common import month_ranges


def vendor_keys(vendor: pl.DataFrame, *, positive: bool = False) -> pl.DataFrame:
    """Normalize labels only; strike comparison follows the earlier float32 audit."""
    if positive:
        vendor = vendor.filter(pl.col('volume') > 0)
    cp = pl.col('call_put').str.strip_chars().str.to_uppercase()
    return vendor.select(
        pl.col('option_id').str.strip_chars().str.to_uppercase().alias('product'),
        vendor_option_series(pl.col('contract_date')).alias('series'),
        pl.col('strike_price').cast(pl.Float32, strict=False).alias('strike'),
        pl.when(cp.is_in(['CALL', 'C'])).then(pl.lit('C'))
          .when(cp.is_in(['PUT', 'P'])).then(pl.lit('P'))
          .otherwise(pl.lit(None, dtype=pl.String)).alias('right'),
    ).filter(pl.col('product').is_not_null() & (pl.col('product') != '')
             & pl.col('series').str.contains(r'^\d{6}([WF][1-5])?$')
             & pl.col('strike').is_finite() & (pl.col('strike') > 0)
             & pl.col('right').is_not_null()).unique()


def audit_day(row: dict, root: Path, official: pl.DataFrame | None) -> tuple[dict, list[dict]]:
    day = row['date']
    result = {'date': day, 'source_status': row['source_status'], 'official_keys': 0,
              'missing_keys': 0, 'missing_positive_volume_keys': 0,
              'missing_day_keys': 0, 'missing_unreported_keys': 0, 'error': ''}
    if official is None or not official.height:
        result['source_status'] = 'no_supported_official_open'
        return result, []
    expected = official.select(KEYS).unique()
    result['official_keys'] = expected.height
    if row['source_status'] != 'daily_readback_verified':
        return result, []
    try:
        receipt_path = root / 'receipts' / 'TaiwanOptionDaily' / 'all' / f'{day}.json'
        if receipt_path.is_symlink() or not receipt_path.resolve().is_relative_to(root.resolve()):
            raise ValueError('unsafe receipt path')
        receipt_bytes = receipt_path.read_bytes()
        if hashlib.sha256(receipt_bytes).hexdigest() != row['receipt_sha256']:
            raise ValueError('receipt changed after product audit')
        receipt = json.loads(receipt_bytes)
        if (receipt.get('dataset') != 'TaiwanOptionDaily' or receipt.get('data_id') != ''
                or receipt.get('partition') != day or receipt.get('kind') != row['source_kind']
                or receipt.get('status') != 'complete' or receipt.get('sha256') != row['parquet_sha256']):
            raise ValueError('source identity mismatch')
        error = parquet_receipt_error(root, receipt, verify_hash=True)
        if error:
            raise ValueError(error)
        parquet = root / receipt['parquet_path']
        before = signature(parquet)
        vendor = pl.scan_parquet(parquet).select(
            ['option_id', 'contract_date', 'strike_price', 'call_put', 'volume']).collect(engine='streaming')
        missing = expected.join(vendor_keys(vendor), on=KEYS, how='anti')
        missing_positive = expected.join(vendor_keys(vendor, positive=True), on=KEYS, how='anti')
        if signature(parquet) != before or receipt_path.read_bytes() != receipt_bytes:
            raise ValueError('source changed during contract audit')
        evidence = official.join(missing_positive, on=KEYS, how='inner').select([*KEYS, 'session']).unique()
        existence = missing.with_columns(pl.lit(True).alias('missing_entirely'))
        evidence = (evidence.join(existence, on=KEYS, how='left')
                    .with_columns(pl.col('missing_entirely').fill_null(False)))
        result.update(source_status='daily_contract_readback_verified', missing_keys=missing.height,
                      missing_positive_volume_keys=missing_positive.height,
                      missing_day_keys=evidence.filter(pl.col('session') == 'day').height,
                      missing_unreported_keys=evidence.filter(pl.col('session') == 'unreported').height)
        return result, [{'date': day, **item} for item in evidence.sort([*KEYS, 'session']).to_dicts()]
    except (OSError, KeyError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        result.update(source_status='daily_contract_readback_failed', error=str(exc)[:256])
        return result, []


def audit(product_summary: Path, coverage_path: Path, years: list[int], workers: int) -> tuple[list[dict], list[dict], dict]:
    if not 1 <= workers <= 16 or not years or len(set(years)) != len(years):
        raise ValueError('invalid worker count or years')
    started = time.monotonic()
    product_bytes, coverage_bytes = product_summary.read_bytes(), coverage_path.read_bytes()
    product, coverage = json.loads(product_bytes), json.loads(coverage_bytes)
    coverage_kind = coverage.get('contract')
    if (product.get('contract') != 'taifex_option_vs_finmind_all_market_daily_presence_v2'
            or product.get('official_coverage_summary_sha256') != hashlib.sha256(coverage_bytes).hexdigest()
            or coverage_kind not in {'taifex_option_annual_positive_days_finmind_receipts_v1',
                                     'tw_option_all_product_monthly_range_readback_v1'}):
        raise ValueError('product or official coverage identity mismatch')
    inventory = Path(product['day_results'])
    inventory_bytes = inventory.read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != product['day_results_sha256']:
        raise ValueError('product inventory changed')
    if coverage_kind == 'taifex_option_annual_positive_days_finmind_receipts_v1':
        if sha256_file(Path(coverage['source_manifest'])) != coverage['source_manifest_sha256']:
            raise ValueError('official source manifest changed')
    else:
        start, end = date.fromisoformat(coverage['start']), date.fromisoformat(coverage['end'])
        ranges = [(first.isoformat(), last.isoformat()) for first, last in month_ranges(start, end)]
        if (years != [start.year] or end.year != start.year or coverage['months_verified'] != len(coverage['sources'])
                or [(s['start'], s['end']) for s in coverage['sources']] != ranges):
            raise ValueError('monthly source scope differs from requested year')
    all_rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    root = Path(product['finmind_daily_root']).resolve()
    results, missing, year_stats = [], [], []
    for year in sorted(years):
        if coverage_kind == 'taifex_option_annual_positive_days_finmind_receipts_v1':
            sources = [x for x in coverage['annual_sources'] if Path(x['path']).name == f'{year}_opt.zip']
            if len(sources) != 1:
                raise ValueError(f'missing official annual source: {year}')
            daily, official_stats = official_year(Path(sources[0]['path']), sources[0]['sha256'], year)
        else:
            daily, official_stats = {}, {'source_rows_with_ordinary_or_unreported_open': 0, 'unsupported_rows': 0}
            for source in coverage['sources']:
                path, receipt_path = Path(source['path']), Path(source['receipt'])
                if path.is_symlink() or receipt_path.is_symlink():
                    raise ValueError('redirected monthly source or receipt')
                receipt_bytes = receipt_path.read_bytes()
                receipt = json.loads(receipt_bytes)
                if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                        or receipt.get('range_start') != source['start'] or receipt.get('range_end') != source['end']
                        or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                        or receipt.get('sha256') != source['sha256'] or receipt.get('bytes') != source['bytes']
                        or path.stat().st_size != source['bytes']):
                    raise ValueError('monthly receipt differs from source inventory')
                days, stats = official_year(path, source['sha256'], year)
                if (receipt_path.read_bytes() != receipt_bytes
                        or any(not source['start'] <= day <= source['end'] or day in daily for day in days)):
                    raise ValueError('monthly source changed or overlapped')
                daily.update(days)
                for name in official_stats:
                    official_stats[name] += stats[name]
        selected = [row for row in all_rows if row['date'].startswith(str(year))]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for day_result, details in pool.map(lambda row: audit_day(row, root, daily.get(row['date'])), selected):
                results.append(day_result)
                missing.extend(details)
        group = [row for row in results if row['date'].startswith(str(year))]
        stats = {'year': year, **official_stats, 'audit_dates': len(group),
                 'source_states': dict(sorted(Counter(row['source_status'] for row in group).items())),
                 'verified_dates_with_missing_keys': sum(row['missing_keys'] > 0 for row in group),
                 'verified_missing_keys': sum(row['missing_keys'] for row in group),
                 'verified_dates_with_missing_positive_volume_keys': sum(row['missing_positive_volume_keys'] > 0 for row in group),
                 'verified_missing_positive_volume_keys': sum(row['missing_positive_volume_keys'] for row in group)}
        year_stats.append(stats)
        print(json.dumps(stats), file=sys.stderr, flush=True)
    if (coverage_path.read_bytes() != coverage_bytes or product_summary.read_bytes() != product_bytes
            or inventory.read_bytes() != inventory_bytes):
        raise ValueError('source coverage evidence changed during audit')
    return results, missing, {
        'contract': 'taifex_option_ordinary_open_vs_finmind_market_daily_contract_v2',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'elapsed_seconds': time.monotonic() - started,
        'product_summary': str(product_summary.resolve()), 'product_summary_sha256': hashlib.sha256(product_bytes).hexdigest(),
        'official_coverage_sha256': hashlib.sha256(coverage_bytes).hexdigest(),
        'official_parser_sha256': sha256_file(Path('scripts/audit_taifex_option_contract_presence.py')),
        'series_normalizer_sha256': sha256_file(Path('downloader/finmind_option_contract_keys.py')),
        'audit_script_sha256': sha256_file(Path(__file__)), 'workers': workers, 'by_year': year_stats,
        'limitations': [
            'Only official positive opening and volume for day or unreported session and supported outright month/week identities are compared.',
            'Unsupported official rows are counted, not included; unreported session is not automatically day session.',
            'Vendor natural-day records may combine sessions; presence is not an opening event or decision-clock fill.',
            'Strike identities use float32 precision; distinct original decimal strikes could collide.',
            'Vendor monthly YYYYMM.0 is compared as YYYYMM; weekly series and other decimals are not converted.',
            'A source mismatch is not proof of its cause, completeness, PIT availability, or accounting eligibility.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--product-summary', type=Path, required=True)
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--years', type=int, nargs='+', required=True)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    rows, missing, summary = audit(args.product_summary, args.coverage_summary, args.years, args.workers)

    def write_csv(path: Path, items: list[dict], fields: list[str]) -> str:
        buf = io.StringIO(newline='')
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        writer.writerows(items)
        content = buf.getvalue().encode()
        atomic_write_bytes(path, content)
        return hashlib.sha256(content).hexdigest()

    days = args.output_dir / 'option_daily_contracts_by_day.csv'
    misses = args.output_dir / 'option_daily_missing_contracts.csv'
    summary['day_results'], summary['missing_results'] = str(days.resolve()), str(misses.resolve())
    summary['day_results_sha256'] = write_csv(days, rows, list(rows[0]))
    summary['missing_results_sha256'] = write_csv(misses, missing, ['date', *KEYS, 'session', 'missing_entirely'])
    atomic_write_json(args.output_dir / 'option_daily_contracts.summary.json', summary)
    print(json.dumps({'output': str(args.output_dir), 'by_year': summary['by_year']}), flush=True)


if __name__ == '__main__':
    main()
