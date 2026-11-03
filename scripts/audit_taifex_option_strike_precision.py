#!/usr/bin/env python3
"""Audit retained TAIFEX decimal strikes before comparison at Float32 precision.

This is a source identity audit, not a tick-completeness or execution audit.
It never rewrites vendor data or promotes a contract to training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_source_universe import _csv_column_names
from scripts.taifex_daily_download_common import month_ranges
from stockagent.data.tw_index_futures import TAIFEX_DAY_SESSION_ALIASES, iter_taifex_daily_csv_streams


def exact_strike(expr: pl.Expr) -> pl.Expr:
    """Normalize decimal spelling without rounding or converting to float."""
    return (expr.str.replace_all(',', '').str.replace(r'^\+', '')
            .str.replace(r'^0+([0-9])', '${1}')
            .str.replace(r'(\.[0-9]*?)0+$', '${1}').str.replace(r'\.$', ''))


def collision_groups(frame: pl.DataFrame) -> pl.DataFrame:
    """Find distinct official decimal identities collapsed by one Float32 bin."""
    keys = ['date', 'product', 'series', 'right', 'strike_float32']
    return (frame.select([*keys, 'strike_decimal']).unique()
            .group_by(keys).agg(pl.col('strike_decimal').sort().alias('official_decimal_strikes'))
            .filter(pl.col('official_decimal_strikes').list.len() > 1).sort(keys))


def audit_source(path: Path, digest: str, start: date, end: date) -> tuple[dict, list[dict]]:
    columns = ['交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '開盤價', '成交量', '交易時段']
    frames: list[pl.DataFrame] = []
    positive_rows = unsupported_rows = 0
    for stream, member, source_sha in iter_taifex_daily_csv_streams(path):
        if source_sha != digest:
            raise ValueError('official source digest mismatch')
        names, _ = _csv_column_names(stream.buffer, member)
        reader = arrow_csv.open_csv(
            stream.buffer,
            read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=names, skip_rows=1,
                                               block_size=2 * 1024 * 1024),
            convert_options=arrow_csv.ConvertOptions(include_columns=columns, include_missing_columns=True,
                                                     column_types={name: pa.string() for name in columns}),
        )
        try:
            for batch in reader:
                raw = pl.from_arrow(batch).select(pl.all().fill_null('').str.strip_chars())
                observed = raw.filter(
                    pl.col('交易時段').is_in([*TAIFEX_DAY_SESSION_ALIASES, ''])
                    & (pl.col('開盤價').str.replace_all(',', '').cast(pl.Float64, strict=False) > 0)
                    & (pl.col('成交量').str.replace_all(',', '').cast(pl.Int64, strict=False) > 0))
                positive_rows += observed.height
                selected = observed.select(
                    pl.col('交易日期').str.strptime(pl.Date, '%Y/%m/%d', strict=False).alias('date'),
                    pl.col('契約').str.to_uppercase().alias('product'),
                    pl.col('到期月份(週別)').str.to_uppercase().alias('series'),
                    exact_strike(pl.col('履約價')).alias('strike_decimal'),
                    pl.when(pl.col('買賣權').is_in(['買權', 'C'])).then(pl.lit('C'))
                      .when(pl.col('買賣權').is_in(['賣權', 'P'])).then(pl.lit('P'))
                      .otherwise(pl.lit(None, dtype=pl.String)).alias('right'),
                ).with_columns(pl.col('strike_decimal').cast(pl.Float32, strict=False).alias('strike_float32'))
                supported = selected.filter(
                    pl.col('date').is_between(start, end, closed='both')
                    & (pl.col('product') != '') & pl.col('series').str.contains(r'^\d{6}([WF][1-5])?$')
                    & pl.col('strike_decimal').str.contains(r'^\d+(\.\d+)?$')
                    & pl.col('strike_float32').is_finite() & (pl.col('strike_float32') > 0)
                    & pl.col('right').is_not_null())
                unsupported_rows += selected.height - supported.height
                if supported.height:
                    frames.append(supported.unique())
        finally:
            reader.close()
    if sha256_file(path) != digest:
        raise ValueError('official source changed during audit')
    if not frames:
        return {'positive_open_rows': positive_rows, 'unsupported_rows': unsupported_rows,
                'official_decimal_contract_day_keys': 0, 'official_float32_contract_day_keys': 0,
                'supported_dates': 0, 'collision_groups': 0, 'collision_decimal_keys': 0}, []
    combined = pl.concat(frames).unique()
    groups = collision_groups(combined)
    rows = groups.to_dicts()
    for row in rows:
        row['date'] = row['date'].isoformat()
    return {
        'positive_open_rows': positive_rows, 'unsupported_rows': unsupported_rows,
        'official_decimal_contract_day_keys': combined.select(
            ['date', 'product', 'series', 'right', 'strike_decimal']).unique().height,
        'official_float32_contract_day_keys': combined.select(
            ['date', 'product', 'series', 'right', 'strike_float32']).unique().height,
        'supported_dates': combined['date'].n_unique(), 'collision_groups': groups.height,
        'collision_decimal_keys': sum(len(row['official_decimal_strikes']) for row in rows),
    }, rows


def read_inputs(annual_path: Path, monthly_path: Path, years: list[int]) -> tuple[list[dict], dict]:
    annual_bytes = annual_path.read_bytes()
    annual = json.loads(annual_bytes)
    if annual.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1':
        raise ValueError('annual source coverage contract mismatch')
    if sha256_file(Path(annual['source_manifest'])) != annual['source_manifest_sha256']:
        raise ValueError('annual source manifest changed')
    if not years or len(years) != len(set(years)):
        raise ValueError('years must be nonempty and unique')
    sources = []
    for year in sorted(years):
        matches = [item for item in annual['annual_sources'] if Path(item['path']).name == f'{year}_opt.zip']
        if len(matches) != 1:
            raise ValueError(f'missing or duplicate annual archive: {year}')
        sources.append({'path': matches[0]['path'], 'sha256': matches[0]['sha256'],
                        'start': f'{year}-01-01', 'end': f'{year}-12-31', 'kind': 'annual'})

    monthly_bytes = monthly_path.read_bytes()
    monthly = json.loads(monthly_bytes)
    if monthly.get('contract') != 'tw_option_all_product_monthly_range_readback_v1':
        raise ValueError('monthly source readback contract mismatch')
    first, last = date.fromisoformat(monthly['start']), date.fromisoformat(monthly['end'])
    if first.year != last.year or first.year in years or first > last:
        raise ValueError('monthly and annual sources overlap or have invalid ranges')
    ranges = [(a.isoformat(), b.isoformat()) for a, b in month_ranges(first, last)]
    if monthly.get('months_verified') != len(ranges) or [
        (x['start'], x['end']) for x in monthly['sources']
    ] != ranges:
        raise ValueError('monthly source ranges are incomplete or overlap')
    for item in monthly['sources']:
        path, receipt_path = Path(item['path']), Path(item['receipt'])
        if any(p.is_symlink() or not p.is_file() for p in (path, receipt_path)):
            raise ValueError('monthly source or receipt missing or redirected')
        receipt_bytes = receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
        if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                or receipt.get('range_start') != item['start'] or receipt.get('range_end') != item['end']
                or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                or receipt.get('sha256') != item['sha256'] or receipt.get('bytes') != item['bytes']
                or path.stat().st_size != item['bytes']):
            raise ValueError('monthly receipt identity mismatch')
        sources.append({**{k: item[k] for k in ('path', 'sha256', 'start', 'end')}, 'kind': 'monthly',
                        'receipt': str(receipt_path),
                        'receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest()})
    return sources, {'annual_coverage': str(annual_path.resolve()),
                     'annual_coverage_sha256': hashlib.sha256(annual_bytes).hexdigest(),
                     'annual_source_manifest': annual['source_manifest'],
                     'annual_source_manifest_sha256': annual['source_manifest_sha256'],
                     'monthly_readback': str(monthly_path.resolve()),
                     'monthly_readback_sha256': hashlib.sha256(monthly_bytes).hexdigest()}


def audit(annual_path: Path, monthly_path: Path, years: list[int]) -> dict:
    sources, pins = read_inputs(annual_path, monthly_path, years)
    stats, collisions = [], []
    for source in sources:
        result, groups = audit_source(Path(source['path']), source['sha256'],
                                      date.fromisoformat(source['start']), date.fromisoformat(source['end']))
        if source.get('receipt') and sha256_file(Path(source['receipt'])) != source['receipt_sha256']:
            raise ValueError('monthly receipt changed during audit')
        stats.append({**source, **result})
        collisions.extend(groups)
        print(json.dumps({'start': source['start'], 'end': source['end'], **result}), flush=True)
    for key in ('annual_coverage', 'annual_source_manifest', 'monthly_readback'):
        if sha256_file(Path(pins[key])) != pins[f'{key}_sha256']:
            raise ValueError(f'{key} changed during audit')
    return {'contract': 'taifex_option_positive_open_strike_precision_audit_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'input_pins': pins,
            'audit_script_sha256': sha256_file(Path(__file__)), 'sources': stats,
            'totals': {key: sum(item[key] for item in stats) for key in (
                'positive_open_rows', 'unsupported_rows', 'official_decimal_contract_day_keys',
                'official_float32_contract_day_keys', 'supported_dates', 'collision_groups',
                'collision_decimal_keys')},
            'collisions': collisions,
            'limitations': [
                'Only outright month/week option rows with positive daily open and volume in day or unreported session are covered.',
                'Float32 equality is still not proof of the original vendor decimal, source event, execution price, time or completeness.',
                'An unreported source session is not proven day-session; night-only and non-trading contracts are outside scope.',
                'A source collision is ambiguous, not a missing contract or an executable trade.',
                'This audit does not rewrite or admit any source or training data.',
            ], 'financial_or_training_adopted': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--annual-coverage', type=Path, required=True)
    parser.add_argument('--monthly-readback', type=Path, required=True)
    parser.add_argument('--years', nargs='+', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.annual_coverage, args.monthly_readback, args.years)
    if args.output.exists():
        raise FileExistsError('refusing to overwrite an audit receipt')
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'totals': result['totals']}), flush=True)


if __name__ == '__main__':
    main()
