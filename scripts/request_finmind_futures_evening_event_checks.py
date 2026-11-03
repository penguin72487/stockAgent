#!/usr/bin/env python3
"""Register exact futures product-days for a source-backed evening event check.

The official and vendor captures can differ by publication time. A missing
event in an old capture is not a claim that its contract is missing, or that a
later vendor response must contain it. No training data is modified.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time, timedelta
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from scripts.request_finmind_option_tick_product_checks import register_evidenced
from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory


DATASET = 'TaiwanFuturesTick'
CONTRACT = 'finmind_futures_evening_event_source_check_v1'
KEYS = ['product', 'contract', 'event_ts', 'price_f32']


def compare(official: pl.DataFrame, vendor: pl.DataFrame, day: date, products: tuple[str, ...]) -> dict:
    start, end = datetime.combine(day, time(15)), datetime.combine(day + timedelta(days=1), time())
    o = (official.filter(pl.col('product').is_in(products)
            & (pl.col('event_ts') >= start) & (pl.col('event_ts') < end) & (pl.col('price') > 0))
        .select('product', 'contract', 'event_ts', pl.col('price').cast(pl.Float32).alias('price_f32'))
        .unique())
    v = (vendor.with_columns(pl.col('date').str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False).alias('event_ts'))
        .filter(pl.col('futures_id').is_in(products) & (pl.col('event_ts') >= start)
            & (pl.col('event_ts') < end) & (pl.col('price') > 0) & (pl.col('volume') > 0))
        .select(pl.col('futures_id').alias('product'),
            pl.col('contract_date').cast(pl.String).str.replace(r'\.0$', '').alias('contract'),
            'event_ts', pl.col('price').cast(pl.Float32).alias('price_f32')).unique())
    missing = o.join(v, on=KEYS, how='anti')
    by_product = []
    for product in products:
        ex, ven, absent = (x.filter(pl.col('product') == product) for x in (o, v, missing))
        by_product.append({'product': product, 'official_event_keys': ex.height,
                           'vendor_event_keys': ven.height, 'official_keys_absent_in_vendor_capture': absent.height,
                           'examples': (absent.sort('event_ts', 'contract', 'price_f32').head(5)
                               .with_columns(pl.col('event_ts').dt.strftime('%Y-%m-%dT%H:%M:%S'))
                               .to_dicts())})
    return {'official_event_keys': o.height, 'vendor_event_keys': v.height,
            'official_keys_absent_in_vendor_capture': missing.height, 'by_product': by_product}


def register(root: Path, official_manifest: Path, day: date, products: tuple[str, ...],
             evidence: Path, output: Path, now: datetime) -> dict:
    root, official_manifest, evidence, output = (p.resolve() for p in (root, official_manifest, evidence, output))
    workspace = root.parents[1]
    if (not products or len(set(products)) != len(products) or now.tzinfo is None or day >= now.date()
            or any(not p.isascii() or not p.isalnum() or p != p.upper() for p in products)
            or not evidence.is_relative_to(workspace / 'artifacts/markets')
            or not output.is_relative_to(workspace / 'artifacts/markets')
            or evidence.exists() or output.exists()):
        raise ValueError('unique products and new in-workspace evidence are required')
    receipt = root / 'receipts' / DATASET / 'all' / f'{day.isoformat()}.json'
    receipt_sha = sha256_file(receipt)
    doc = json.loads(receipt.read_bytes())
    if (doc.get('dataset') != DATASET or doc.get('data_id') != '' or doc.get('partition') != day.isoformat()
            or doc.get('status') != 'complete' or doc.get('source_bytes_preserved') is not True):
        raise ValueError('exact retained whole-market source receipt required')
    vendor_path = (root / doc['parquet_path']).resolve()
    if (not vendor_path.is_relative_to(root) or sha256_file(vendor_path) != doc['sha256']
            or pq.ParquetFile(vendor_path).metadata.num_rows != doc['rows']):
        raise ValueError('retained whole-market payload differs from receipt')
    exchange = pin_official_tick_inventory(official_manifest, [day + timedelta(days=1)])[0]
    exchange_path = Path(exchange['path'])
    if sha256_file(exchange_path) != exchange['sha256'] or exchange_path.stat().st_size != exchange['bytes']:
        raise ValueError('official archive differs from its receipt')
    parsed = _parse_zip(exchange_path, kind='futures', trading_date=day + timedelta(days=1),
        source_sha256=exchange['sha256'], futures_products=products, futures_outright_contracts_only=True,
        futures_day_sessions={product: (0, 1440) for product in products})
    official = parsed.select('product', pl.col('delivery_month_week').alias('contract'),
                             pl.col('event_ts').dt.replace_time_zone(None).alias('event_ts'), 'price')
    comparison = compare(official, pl.read_parquet(vendor_path), day, products)
    if any(row['official_keys_absent_in_vendor_capture'] == 0 for row in comparison['by_product']):
        raise ValueError('each requested product needs an observed missing evening event')
    if sha256_file(receipt) != receipt_sha or sha256_file(vendor_path) != doc['sha256']:
        raise ValueError('vendor source changed during comparison')
    evidence_doc = {'contract': 'taifex_finmind_futures_evening_event_source_comparison_v1',
                    'generated_at_utc': now.isoformat(), 'source_day': day.isoformat(),
                    'official_archive': exchange, 'vendor_receipt': str(receipt),
                    'vendor_receipt_sha256': receipt_sha, 'vendor_payload': str(vendor_path),
                    'vendor_payload_sha256': doc['sha256'], 'vendor_fetched_at_utc': doc.get('fetched_at_utc'),
                    'comparison': comparison, 'price_comparison': 'Float32 event key; volume not compared',
                    'training_ready': False, 'limitations': [
                        'Observed capture difference, not proof of permanent provider omission.',
                        'Archive event time does not establish historic source delivery.',
                        'No claims about order fills, eligibility or accounting.']}
    atomic_write_json(evidence, evidence_doc)
    expiry = now + timedelta(days=6)
    checks = [{'dataset': DATASET, 'data_id': row['product'], 'partition': day.isoformat(),
               'audited_absent_event_keys_count': row['official_keys_absent_in_vendor_capture'],
               'original_receipt': str(receipt), 'original_receipt_sha256': receipt_sha,
               'original_parquet': str(vendor_path), 'original_parquet_sha256': doc['sha256'],
               'conflict_plan': str(evidence), 'conflict_plan_sha256': sha256_file(evidence),
               'conflict_plan_expires_at_utc': expiry.isoformat()} for row in comparison['by_product']]
    return register_evidenced(root, checks, now, output, dataset=DATASET, contract=CONTRACT,
        reason='Check evening event coverage for two futures product-days against the retained whole-market capture.',
        additional_evidence_pins={str(exchange_path): exchange['sha256'],
                                  str(Path(exchange['receipt_path'])): exchange['receipt_sha256'],
                                  str(Path(__file__).resolve()): sha256_file(Path(__file__))})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--day', type=date.fromisoformat, required=True)
    parser.add_argument('--products', nargs='+', required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = register(args.root, args.official_manifest, args.day, tuple(sorted(args.products)),
                      args.evidence, args.output, datetime.now(UTC))
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
