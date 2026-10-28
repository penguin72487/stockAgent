#!/usr/bin/env python3
"""Read back exact receipted TAIFEX all-product option daily ranges.

This audits retained daily source records only. It does not attest historical
availability, lifecycle, executable prices, or option-account training readiness.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from scripts.download_taifex_option_daily_history import TAIFEX_OPTION_DOWNLOAD_URL
from scripts.taifex_daily_download_common import month_ranges
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches


def audit(root: Path, start: date, end: date, *, include_daily_products: bool = False) -> dict:
    if start.year != end.year or start > end:
        raise ValueError('ranges must be ordered within one calendar year')
    root = root.resolve()
    all_products: set[str] = set()
    positive_products: set[str] = set()
    sessions: Counter[str] = Counter()
    series: Counter[str] = Counter()
    daily_products: dict[tuple[str, str], set[str]] = {}
    sources = []
    for first, last in month_ranges(start, end):
        source = root / f'{first.isoformat()}_{last.isoformat()}_all.csv'
        intent_path = source.with_suffix('.intent.json')
        receipt_path = source.with_suffix('.receipt.json')
        for path in (source, intent_path, receipt_path):
            if path.is_symlink() or not path.is_file():
                raise ValueError(f'missing or redirected source evidence: {path}')
        intent_bytes = intent_path.read_bytes()
        receipt_bytes = receipt_path.read_bytes()
        identity = {
            'contract': 'taifex_option_all_products_daily_raw_v1',
            'url': TAIFEX_OPTION_DOWNLOAD_URL,
            'request': {'down_type': '1', 'queryStartDate': first.strftime('%Y/%m/%d'),
                        'queryEndDate': last.strftime('%Y/%m/%d'),
                        'commodity_id': 'all', 'commodity_id2': ''},
            'range_start': first.isoformat(), 'range_end': last.isoformat(),
            'point_in_time_snapshot': False,
        }
        intent, receipt = json.loads(intent_bytes), json.loads(receipt_bytes)
        if (intent != identity or any(receipt.get(k) != v for k, v in identity.items())
                or receipt.get('path') != str(source)
                or receipt.get('bytes') != source.stat().st_size):
            raise ValueError(f'source receipt identity or size mismatch: {source}')
        expected_sha = receipt.get('sha256')
        month_products: set[str] = set()
        month_positive: set[str] = set()
        rows = 0
        unknown_volume_rows = 0
        for batch in iter_option_product_daily_batches(source, expected_sha256=expected_sha):
            lower, upper = batch['date'].min(), batch['date'].max()
            if lower < first or upper > last:
                raise ValueError(f'source row outside receipted range: {source}')
            rows += batch.height
            month_products.update(batch['product'].unique().to_list())
            volumes = batch.with_columns(
                pl.col('volume_raw').str.strip_chars().str.replace_all(',', '')
                .cast(pl.Int64, strict=False).alias('_volume')
            )
            unknown_volume_rows += volumes['_volume'].null_count()
            month_positive.update(volumes.filter(pl.col('_volume') > 0)['product'].unique().to_list())
            if include_daily_products:
                grouped = (volumes.filter(pl.col('_volume') > 0)
                           .group_by('date', 'session').agg(pl.col('product').unique()))
                for day, session, products in grouped.iter_rows():
                    daily_products.setdefault((day.isoformat(), session), set()).update(products)
            for value, count in batch.group_by('session').agg(pl.len()).iter_rows():
                sessions[value] += count
            for value, count in batch.group_by('series_scope').agg(pl.len()).iter_rows():
                series[value] += count
        if not rows:
            raise ValueError(f'no source rows in monthly range: {source}')
        if intent_path.read_bytes() != intent_bytes or receipt_path.read_bytes() != receipt_bytes:
            raise ValueError(f'source evidence changed during readback: {source}')
        all_products.update(month_products)
        positive_products.update(month_positive)
        record = {'start': first.isoformat(), 'end': last.isoformat(), 'path': str(source),
                  'receipt': str(receipt_path), 'sha256': expected_sha, 'bytes': receipt['bytes'],
                  'rows': rows, 'product_count': len(month_products),
                  'positive_volume_product_count': len(month_positive),
                  'unknown_volume_rows': unknown_volume_rows}
        sources.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
    implementation = Path(__file__).resolve().parents[1] / 'stockagent/data/tw_option_product_daily.py'
    result = {'contract': 'tw_option_all_product_monthly_range_readback_v1',
            'completed_at_utc': datetime.now(timezone.utc).isoformat(),
            'start': start.isoformat(), 'end': end.isoformat(),
            'reader_sha256': sha256_file(implementation),
            'audit_sha256': sha256_file(Path(__file__)),
            'months_verified': len(sources), 'raw_rows': sum(item['rows'] for item in sources),
            'product_union_count': len(all_products),
            'observed_products': sorted(all_products),
            'positive_volume_product_union_count': len(positive_products),
            'positive_volume_products': sorted(positive_products),
            'unknown_volume_rows': sum(item['unknown_volume_rows'] for item in sources),
            'sessions': dict(sorted(sessions.items())), 'series_scopes': dict(sorted(series.items())),
            'sources': sources,
            'limitations': ['Daily source rows may overlap and include zero-volume records.',
                            'Range and digest verification do not prove trading-calendar completeness.',
                            'Daily source is not a PIT snapshot, execution tape or training dataset.']}
    if include_daily_products:
        labels = {'day': 'day', 'unreported': 'unreported_session',
                  'night': 'night', 'unknown': 'unknown_session'}
        dates = sorted({day for day, _ in daily_products})
        result['daily_positive_products'] = [
            {'date': day, **{f'{label}_positive_products': '|'.join(sorted(daily_products.get((day, session), set())))
                             for session, label in labels.items()}}
            for day in dates]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_tw_index_options_daily/raw/all_products/ranges'))
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--include-daily-products', action='store_true')
    args = parser.parse_args()
    result = audit(args.root, args.start, args.end, include_daily_products=args.include_daily_products)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write('\n')
    print(json.dumps({'output': str(args.output), 'months_verified': result['months_verified'],
                      'raw_rows': result['raw_rows']}), flush=True)


if __name__ == '__main__':
    main()
