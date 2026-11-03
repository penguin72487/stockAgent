#!/usr/bin/env python3
"""Compare carried option-reference candidates with official settlement rows.

This is a retrospective source comparison. A product and series can have
multiple physical generations; future settlement dates are never features.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_finmind_option_reference_boundary import KEYS
from scripts.download_taifex_stock_option_final_settlement import parse as parse_stock
from scripts.publish_taifex_gold_option_settlements import parse as parse_gold
from scripts.publish_taifex_index_option_settlements import parse as parse_index
from scripts.run_finmind_option_reference_boundaries import verify


def classify(references: pl.DataFrame, settlements: pl.DataFrame) -> dict:
    identities = defaultdict(list)
    for product, series, day in settlements.filter(pl.col('positive_numeric_price')).select(
            'product_raw', 'series_raw', 'settlement_date').iter_rows():
        identities[(product, series)].append(day)
    counts = Counter()
    for row in references.iter_rows(named=True):
        values = identities.get((row['product_raw'], row['series_raw']), [])
        if not values:
            state = 'no_source_settlement_key'
        elif len(values) > 1:
            state = 'multiple_source_rows_generation_unknown'
        elif values[0] < row['decision_date']:
            state = 'single_source_settlement_before_decision_day'
        elif values[0] == row['decision_date']:
            state = 'single_source_settlement_on_decision_day'
        else:
            state = 'single_source_settlement_after_decision_day'
        counts[(row['decision_date'].isoformat(), row['decision_time'], row['product_raw'],
                row['series_raw'], state)] += 1
    return {'rows': references.height,
            'by_day_clock_product_series_state': [
                {'date': day, 'clock': clock, 'product': product, 'series': series,
                 'state': state, 'rows': count}
                for (day, clock, product, series, state), count in sorted(counts.items())]}


def audit(boundary: Path, history_plan: Path, root: Path, index_manifest: Path,
          stock_receipts: list[Path], gold_manifest: Path | None = None) -> dict:
    implementation = [Path(__file__).resolve(), Path('scripts/download_taifex_stock_option_final_settlement.py').resolve(),
                      Path('scripts/publish_taifex_index_option_settlements.py').resolve(),
                      Path('scripts/publish_taifex_gold_option_settlements.py').resolve()]
    implementation_pins = {str(path): sha256_file(path) for path in implementation}
    doc = json.loads((boundary / 'manifest.json').read_bytes())
    plan = json.loads(history_plan.read_bytes())
    start = doc['parents'][1]['window'][0]
    target = next(window for window in plan['windows'] if window['start'] == start)
    verified = verify(boundary, history_plan, target, root)
    seeded = pl.read_parquet(doc['data_path'])
    baseline = pl.read_parquet(Path(doc['parents'][1]['manifest']).parent / 'observed_references.parquet')
    added = seeded.join(baseline.select(KEYS), on=KEYS, how='anti')
    if added.height != doc['comparison']['new_candidate_keys']:
        raise ValueError('candidate key difference does not match receipt')
    index_sha = sha256_file(index_manifest)
    index = json.loads(index_manifest.read_bytes())
    if index.get('contract') != 'taifex_index_option_final_settlement_source_v1' or index.get('training_ready') is not False:
        raise ValueError('source-only official index settlement manifest required')
    index_raw, index_data = Path(index['raw_path']), Path(index['data_path'])
    if sha256_file(index_raw) != index['raw_sha256'] or sha256_file(index_data) != index['data_sha256']:
        raise ValueError('official index settlement content changed')
    index_frame = pl.read_parquet(index_data)
    if not index_frame.equals(parse_index(index_raw.read_bytes(), date.fromisoformat(index['requested_start']),
                                         date.fromisoformat(index['requested_end']))):
        raise ValueError('official index settlement independent parse differs')
    stocks, receipt_pins = [], []
    for receipt in stock_receipts:
        receipt_sha = sha256_file(receipt)
        stock = json.loads(receipt.read_bytes())
        if stock.get('contract') != 'taifex_stock_option_final_settlement_month_v1' or stock.get('training_ready') is not False:
            raise ValueError('source-only official stock settlement receipt required')
        raw, data = Path(stock['raw_path']), Path(stock['payload_path'])
        if sha256_file(raw) != stock['raw_sha256'] or sha256_file(data) != stock['payload_sha256']:
            raise ValueError('official stock settlement content changed')
        frame = pl.read_parquet(data)
        year, month = map(int, stock['query_month'].split('-'))
        if not frame.equals(parse_stock(raw.read_bytes(), year, month)):
            raise ValueError('official stock settlement independent parse differs')
        stocks.append(frame)
        receipt_pins.append({'path': str(receipt), 'sha256': receipt_sha, 'raw_sha256': stock['raw_sha256'],
                             'payload_sha256': stock['payload_sha256']})
    columns = ['product_raw', 'series_raw', 'settlement_date', 'positive_numeric_price']
    frames = [index_frame.select(columns)] + [frame.select(columns) for frame in stocks]
    gold_pin = None
    if gold_manifest is not None:
        gold_sha = sha256_file(gold_manifest)
        gold = json.loads(gold_manifest.read_bytes())
        if gold.get('contract') != 'taifex_gold_option_final_settlement_source_v1' or gold.get('training_ready') is not False:
            raise ValueError('source-only official gold settlement manifest required')
        gold_raw, gold_data = Path(gold['raw_path']), Path(gold['data_path'])
        if sha256_file(gold_raw) != gold['raw_sha256'] or sha256_file(gold_data) != gold['data_sha256']:
            raise ValueError('official gold settlement content changed')
        gold_frame = pl.read_parquet(gold_data)
        if not gold_frame.equals(parse_gold(gold_raw.read_bytes(), date.fromisoformat(gold['requested_start']),
                                            date.fromisoformat(gold['requested_end']))):
            raise ValueError('official gold settlement independent parse differs')
        frames.append(gold_frame.select(columns))
        gold_pin = {'path': str(gold_manifest), 'sha256': gold_sha,
                    'raw_path': str(gold_raw), 'raw_sha256': gold['raw_sha256'],
                    'data_path': str(gold_data), 'data_sha256': gold['data_sha256']}
    settlements = pl.concat(frames)
    all_result = classify(added, settlements)
    first_day = classify(added.filter(pl.col('decision_date') == date.fromisoformat(start)), settlements)
    if (verify(boundary, history_plan, target, root) != verified or sha256_file(index_manifest) != index_sha
            or sha256_file(index_raw) != index['raw_sha256'] or sha256_file(index_data) != index['data_sha256']
            or any(sha256_file(Path(pin['path'])) != pin['sha256'] for pin in receipt_pins)
            or (gold_pin is not None and (sha256_file(gold_manifest) != gold_pin['sha256']
                or sha256_file(Path(gold_pin['raw_path'])) != gold_pin['raw_sha256']
                or sha256_file(Path(gold_pin['data_path'])) != gold_pin['data_sha256']))
            or any(sha256_file(Path(path)) != digest for path, digest in implementation_pins.items())):
        raise ValueError('one of the source records changed during comparison')
    return {'contract': 'finmind_boundary_official_final_settlement_diagnostic_v1',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'boundary': verified, 'index_manifest': str(index_manifest), 'index_manifest_sha256': index_sha,
            'stock_receipts': receipt_pins, 'gold_manifest': gold_pin,
            'implementation_sha256': implementation_pins, 'added': all_result, 'added_first_day': first_day,
            'training_ready': False, 'limitations': [
                'Product/month matches do not prove the identical physical generation after contract adjustments.',
                'Multiple settlement records remain ambiguous; no source record does not prove no expiration.',
                'Settlement after the decision uses future data and cannot become a feature or admission rule.',
                'A same-day final settlement does not mean the contract was unavailable at the opening.',
                'Snapshots do not prove historical notice availability, quotes, fills, or account eligibility.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--boundary', type=Path, required=True)
    parser.add_argument('--history-plan', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--index-manifest', type=Path, required=True)
    parser.add_argument('--stock-receipt', type=Path, action='append', required=True)
    parser.add_argument('--gold-manifest', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve previous diagnostic')
    result = audit(args.boundary.resolve(), args.history_plan.resolve(), args.root.resolve(),
                   args.index_manifest.resolve(), [p.resolve() for p in args.stock_receipt],
                   args.gold_manifest.resolve() if args.gold_manifest else None)
    atomic_write_json(args.output, result)
    summary = Counter()
    for row in result['added_first_day']['by_day_clock_product_series_state']:
        summary[(row['clock'], row['state'])] += row['rows']
    print(json.dumps({'output': str(args.output), 'first_day': [
        {'clock': clock, 'state': state, 'rows': count} for (clock, state), count in sorted(summary.items())]},
        ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
