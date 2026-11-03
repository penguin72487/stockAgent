#!/usr/bin/env python3
"""Compare retained per-product and whole-market futures minute source objects.

This read-only audit preserves duplicate rows and does not infer missing trades,
execution prices, source independence, or financial/training admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


REPO = Path(__file__).resolve().parents[1]
DATASET = 'TaiwanFuturesKBar'
COLUMNS = ['date', 'futures_id', 'contract_date', 'minute', 'open', 'high', 'low', 'close', 'volume']


def compare_rows(product: pl.DataFrame, whole: pl.DataFrame) -> tuple[int, int]:
    """Compare source row multisets, including identical repeated observations."""
    if not set(COLUMNS) <= set(product.columns) or not set(COLUMNS) <= set(whole.columns):
        raise ValueError('futures kbar source schema incomplete')
    a = Counter(product.select(COLUMNS).iter_rows())
    b = Counter(whole.select(COLUMNS).iter_rows())
    return sum((a - b).values()), sum((b - a).values())


def _load(root: Path, path: Path, product: str, day: str) -> tuple[dict, pl.DataFrame | None]:
    before = path.read_bytes()
    info = json.loads(before)
    if ((info.get('dataset'), info.get('data_id'), info.get('partition')) != (DATASET, product, day)
            or info.get('kind') != 'id_day'):
        raise ValueError('native receipt identity differs')
    item = {'receipt_path': str(path), 'receipt_sha256': hashlib.sha256(before).hexdigest(),
            'status': info.get('status'), 'rows': info.get('rows')}
    if info.get('status') == 'observed_empty' and info.get('rows') == 0:
        if path.read_bytes() != before:
            raise ValueError('native receipt changed during readback')
        return item, None
    if info.get('status') != 'complete' or not isinstance(info.get('rows'), int) or info['rows'] <= 0:
        raise ValueError('receipt is neither nonempty complete nor observed empty')
    payload = (root / info['parquet_path']).resolve()
    if (not payload.is_relative_to(root) or payload.stat().st_size != info['parquet_size_bytes']
            or sha256_file(payload) != info['sha256']):
        raise ValueError('native payload path, bytes or digest differs')
    frame = pl.read_parquet(payload)
    if frame.height != info['rows'] or not set(COLUMNS) <= set(frame.columns):
        raise ValueError('native payload row count or schema differs')
    if frame.filter(pl.col('date').cast(pl.String) != day).height:
        raise ValueError('native partition contains another calendar date')
    if product and frame.filter(pl.col('futures_id') != product).height:
        raise ValueError('per-product payload contains another product')
    if sha256_file(payload) != info['sha256'] or path.read_bytes() != before:
        raise ValueError('native source changed during readback')
    item.update(parquet_path=str(payload), parquet_sha256=info['sha256'])
    return item, frame


def audit(root: Path, product: str, start: date, end: date) -> dict:
    root = root.resolve()
    if not re.fullmatch(r'[A-Z0-9]{2,12}', product) or start > end:
        raise ValueError('invalid product or date range')
    identifier = hashlib.sha256(product.encode()).hexdigest()[:12]
    individual_dir = root / 'receipts' / DATASET / identifier
    market_dir = root / 'receipts' / DATASET / 'all'
    if not individual_dir.is_dir() or not market_dir.is_dir():
        raise ValueError('retained product and whole-market receipt directories required')
    rows = []
    for path in sorted(individual_dir.glob('*.json')):
        day = date.fromisoformat(path.stem)
        if not start <= day <= end:
            continue
        item, product_frame = _load(root, path.resolve(), product, day.isoformat())
        row = {'date': day.isoformat(), 'product_source': item}
        market_path = market_dir / path.name
        if not market_path.is_file():
            row['comparison'] = 'whole_market_receipt_unavailable'
        else:
            market, market_frame = _load(root, market_path.resolve(), '', day.isoformat())
            row['whole_market_source'] = market
            if product_frame is None or market_frame is None:
                row['comparison'] = 'source_observed_empty_not_no_trade_proof'
            else:
                market_product = market_frame.filter(pl.col('futures_id') == product)
                left, right = compare_rows(product_frame, market_product)
                row.update(comparison='identical_multiset' if not left and not right else 'source_row_difference',
                           product_rows=product_frame.height, market_product_rows=market_product.height,
                           rows_only_in_product=left, rows_only_in_market=right)
        rows.append(row)
    if not rows:
        raise ValueError('no product receipts in the selected range')
    statuses = Counter(x['comparison'] for x in rows)
    return {'contract': 'finmind_futures_kbar_query_shape_source_audit_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'product': product,
            'date_start': start.isoformat(), 'date_end': end.isoformat(),
            'comparison_states': dict(sorted(statuses.items())),
            'rows_only_in_product': sum(x.get('rows_only_in_product', 0) for x in rows),
            'rows_only_in_market': sum(x.get('rows_only_in_market', 0) for x in rows),
            'days': rows, 'audit_sha256': sha256_file(Path(__file__)), 'provider_calls': 0,
            'financial_or_training_adopted': False,
            'limitations': ['Only retained product receipts are inspected; unrecorded dates are not a no-trade proof.',
                            'Source responses may share an upstream origin; matching is not independent market corroboration.',
                            'A minute bar is not an exact trade event or decision-clock executable quote.',
                            'No retained source is changed or admitted to financial/training inputs.']}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_finmind/complement')
    parser.add_argument('--product', required=True)
    parser.add_argument('--start', type=date.fromisoformat, default=date(2011, 1, 1))
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(REPO / 'artifacts/markets') or output.exists() or output.is_symlink():
        raise ValueError('new versioned output must be inside artifacts/markets')
    result = audit(args.root, args.product, args.start, args.end)
    atomic_write_json(output, result)
    print(json.dumps({'output': str(output), 'comparison_states': result['comparison_states'],
                      'rows_only_in_product': result['rows_only_in_product'],
                      'rows_only_in_market': result['rows_only_in_market']}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
