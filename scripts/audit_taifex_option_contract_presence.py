#!/usr/bin/env python3
"""Read back retained option contract identities against official daily opens.

An official ordinary open corroborates at least one trade, but is neither a
decision-clock price nor a fill. This audit compares natural-day objects at
their stored float32 strike precision; it does not establish complete ticks.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
import sys
import time

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from scripts.audit_taifex_option_product_presence import _signature
from scripts.audit_taifex_option_source_universe import _csv_column_names
from stockagent.data.tw_index_futures import TAIFEX_DAY_SESSION_ALIASES, iter_taifex_daily_csv_streams

KEYS = ['product', 'series', 'strike', 'right']


def compare_contracts(official: pl.DataFrame, vendor: pl.DataFrame) -> pl.DataFrame:
    """Conservatively count simple contract keys absent in the whole-day object."""
    expected = official.select(KEYS).unique()
    observed = vendor.select(
        pl.col('option_id').str.strip_chars().str.to_uppercase().alias('product'),
        pl.col('contract_date').str.strip_chars().str.to_uppercase().alias('series'),
        pl.col('ExercisePrice').cast(pl.Float32).alias('strike'),
        pl.col('PutCall').str.strip_chars().str.to_uppercase().alias('right'),
    ).unique()
    return expected.join(observed, on=KEYS, how='anti').sort(KEYS)


def official_year(path: Path, digest: str, year: int) -> tuple[dict[str, pl.DataFrame], dict]:
    cols = ['交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '開盤價', '成交量', '交易時段']
    frames, unsupported, opens = [], 0, 0
    for stream, member, source_sha in iter_taifex_daily_csv_streams(path):
        if source_sha != digest:
            raise ValueError('official annual source digest mismatch')
        names, _ = _csv_column_names(stream.buffer, member)
        reader = arrow_csv.open_csv(
            stream.buffer,
            read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=names, skip_rows=1,
                                               block_size=2 * 1024 * 1024),
            convert_options=arrow_csv.ConvertOptions(include_columns=cols, include_missing_columns=True,
                                                     column_types={name: pa.string() for name in cols}),
        )
        try:
            for batch in reader:
                raw = pl.from_arrow(batch).select(pl.all().fill_null('').str.strip_chars())
                observations = raw.filter(
                    pl.col('交易時段').is_in([*TAIFEX_DAY_SESSION_ALIASES, ''])
                    & (pl.col('開盤價').str.replace_all(',', '').cast(pl.Float64, strict=False) > 0)
                    & (pl.col('成交量').str.replace_all(',', '').cast(pl.Int64, strict=False) > 0))
                opens += observations.height
                frame = observations.select(
                    pl.col('交易日期').str.strptime(pl.Date, '%Y/%m/%d', strict=False).alias('date'),
                    pl.col('契約').str.to_uppercase().alias('product'),
                    pl.col('到期月份(週別)').str.to_uppercase().alias('series'),
                    pl.col('履約價').str.replace_all(',', '').cast(pl.Float32, strict=False).alias('strike'),
                    pl.when(pl.col('買賣權').is_in(['買權', 'C'])).then(pl.lit('C'))
                      .when(pl.col('買賣權').is_in(['賣權', 'P'])).then(pl.lit('P'))
                      .otherwise(pl.lit(None, dtype=pl.String)).alias('right'),
                    pl.when(pl.col('交易時段') == '').then(pl.lit('unreported'))
                      .otherwise(pl.lit('day')).alias('session'),
                )
                supported = frame.filter(
                    pl.col('date').is_not_null() & (pl.col('date').dt.year() == year)
                    & (pl.col('product') != '') & pl.col('series').str.contains(r'^\d{6}([WF][1-5])?$')
                    & pl.col('strike').is_finite() & (pl.col('strike') > 0)
                    & pl.col('right').is_not_null())
                unsupported += frame.height - supported.height
                if supported.height:
                    frames.append(supported.unique())
        finally:
            reader.close()
    if sha256_file(path) != digest:
        raise ValueError('official annual source changed during read')
    if not frames:
        return {}, {'source_rows_with_ordinary_or_unreported_open': opens, 'unsupported_rows': unsupported}
    combined = pl.concat(frames).unique()
    days = {key[0].isoformat(): frame.drop('date') for key, frame in combined.partition_by('date', as_dict=True).items()}
    return days, {'source_rows_with_ordinary_or_unreported_open': opens, 'unsupported_rows': unsupported}


def audit_day(row: dict, root: Path, official: pl.DataFrame | None) -> tuple[dict, list[dict]]:
    day = row['date']
    result = {'date': day, 'source_status': row['finmind_status'], 'official_contracts': 0,
              'missing_contracts': 0, 'missing_day_session_contracts': 0,
              'missing_unreported_session_contracts': 0, 'unreported_session_contracts': 0}
    if official is None or not official.height:
        result['source_status'] = 'no_supported_official_open'
        return result, []
    result['official_contracts'] = official.select(KEYS).unique().height
    result['unreported_session_contracts'] = official.filter(pl.col('session') == 'unreported').select(KEYS).unique().height
    if row['finmind_status'] != 'complete_payload_sha_verified':
        return result, []
    try:
        receipt_path = Path(row['finmind_receipt_path']).resolve()
        parquet = Path(row['finmind_parquet_path']).resolve()
        if not receipt_path.is_relative_to(root) or not parquet.is_relative_to(root):
            raise ValueError('source path outside FinMind root')
        receipt_bytes = receipt_path.read_bytes()
        if hashlib.sha256(receipt_bytes).hexdigest() != row['finmind_receipt_sha256']:
            raise ValueError('receipt changed after date inventory')
        receipt = json.loads(receipt_bytes)
        if (receipt.get('dataset') != 'TaiwanOptionTick' or receipt.get('data_id') != ''
                or receipt.get('partition') != day or receipt.get('status') != 'complete'
                or (root / receipt['parquet_path']).resolve() != parquet
                or receipt.get('sha256') != row['finmind_parquet_sha256']):
            raise ValueError('receipt identity mismatch')
        signature = _signature(parquet)
        if (sha256_file(parquet) != receipt['sha256'] or signature[2] != receipt['parquet_size_bytes']
                or pq.ParquetFile(parquet).metadata.num_rows != int(receipt['rows'])):
            raise ValueError('parquet byte or row proof failed')
        vendor = pl.scan_parquet(parquet).select(['option_id', 'contract_date', 'ExercisePrice', 'PutCall']).unique().collect(engine='streaming')
        missing = compare_contracts(official, vendor)
        if _signature(parquet) != signature or receipt_path.read_bytes() != receipt_bytes:
            raise ValueError('source changed during contract audit')
        evidence = official.join(missing, on=KEYS, how='inner').select([*KEYS, 'session']).unique()
        result.update(source_status='contract_key_readback_verified', missing_contracts=missing.height,
                      missing_day_session_contracts=evidence.filter(pl.col('session') == 'day').height,
                      missing_unreported_session_contracts=evidence.filter(pl.col('session') == 'unreported').height)
        details = [{'date': day, **item} for item in evidence.sort([*KEYS, 'session']).to_dicts()]
        return result, details
    except (OSError, KeyError, ValueError, TypeError, pl.exceptions.PolarsError, pa.ArrowException) as exc:
        result.update(source_status='contract_key_readback_failed', error=str(exc)[:256])
        return result, []


def audit(coverage_path: Path, years: list[int], workers: int) -> tuple[list[dict], list[dict], dict]:
    if not 1 <= workers <= 16 or not years or len(set(years)) != len(years):
        raise ValueError('invalid worker count or years')
    started = time.monotonic()
    coverage_bytes = coverage_path.read_bytes()
    coverage = json.loads(coverage_bytes)
    if coverage.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1':
        raise ValueError('incompatible coverage audit')
    inventory = Path(coverage['day_inventory'])
    inventory_bytes = inventory.read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != coverage['day_inventory_sha256']:
        raise ValueError('coverage inventory changed')
    if sha256_file(Path(coverage['source_manifest'])) != coverage['source_manifest_sha256']:
        raise ValueError('official source manifest changed')
    all_rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    root = Path(coverage['finmind_root']).resolve()
    results, missing, year_stats = [], [], []
    for year in sorted(years):
        sources = [x for x in coverage['annual_sources'] if Path(x['path']).name == f'{year}_opt.zip']
        if len(sources) != 1:
            raise ValueError(f'missing official annual source: {year}')
        source = sources[0]
        daily, official_stats = official_year(Path(source['path']), source['sha256'], year)
        selected = [row for row in all_rows if row['date'].startswith(str(year))]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for day_result, details in pool.map(lambda row: audit_day(row, root, daily.get(row['date'])), selected):
                results.append(day_result)
                missing.extend(details)
        year_rows = [row for row in results if row['date'].startswith(str(year))]
        stats = {'year': year, **official_stats, 'audit_dates': len(year_rows),
                 'source_states': dict(sorted(Counter(row['source_status'] for row in year_rows).items())),
                 'verified_dates_with_missing_contracts': sum(row['missing_contracts'] > 0 for row in year_rows),
                 'verified_missing_contract_day_keys': sum(row['missing_contracts'] for row in year_rows)}
        year_stats.append(stats)
        print(json.dumps(stats), file=sys.stderr, flush=True)
    if coverage_path.read_bytes() != coverage_bytes or inventory.read_bytes() != inventory_bytes:
        raise ValueError('coverage evidence changed during contract audit')
    return results, missing, {
        'contract': 'taifex_option_ordinary_open_contract_vs_finmind_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'elapsed_seconds': time.monotonic() - started,
        'coverage_summary': str(coverage_path.resolve()), 'coverage_summary_sha256': hashlib.sha256(coverage_bytes).hexdigest(),
        'audit_script_sha256': sha256_file(Path(__file__)), 'workers': workers, 'by_year': year_stats,
        'limitations': [
            'Only official positive opening price and volume in day or unreported session, and simple month/week outright keys, are compared.',
            'Unsupported rows are counted but never reported as covered; unreported sessions are not automatically classified as day.',
            'Natural-day vendor keys may include night events and can hide a day-session absence.',
            'Keys are compared after float32 strike conversion, which can merge distinct original decimal strikes.',
            'A key presence does not prove matching opening event, event completeness, decision-clock fill or historical point-in-time access.',
            'A missing key is a source discrepancy, not proof of its cause or that the vendor can supply it.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--years', nargs='+', type=int, required=True)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    rows, missing, summary = audit(args.coverage_summary, args.years, args.workers)
    if not rows:
        raise ValueError('empty date inventory')
    def write_csv(path, items, fields):
        buffer = io.StringIO(newline='')
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(items)
        body = buffer.getvalue().encode()
        atomic_write_bytes(path, body)
        return hashlib.sha256(body).hexdigest()
    days = args.output_dir / 'option_contract_presence_by_day.csv'
    misses = args.output_dir / 'option_missing_contracts.csv'
    summary['day_inventory'] = str(days.resolve())
    summary['day_inventory_sha256'] = write_csv(days, rows, list(dict.fromkeys(key for row in rows for key in row)))
    summary['missing_inventory'] = str(misses.resolve())
    summary['missing_inventory_sha256'] = write_csv(misses, missing, ['date', *KEYS, 'session'])
    atomic_write_json(args.output_dir / 'option_contract_presence.summary.json', summary)
    print(json.dumps({'output': str(days), 'by_year': summary['by_year']}))


if __name__ == '__main__':
    main()
