#!/usr/bin/env python3
"""Independently compare retained option whole-market and product tick sources.

This checks actual retained source observations, not market completeness,
execution, source independence, point-in-time availability or training use.
The original all-market prices are float32, so comparison at that precision is
only a candidate equivalence and never a replacement for source decimals.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import UTC, datetime
import glob
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.finmind_option_contract_keys import vendor_option_series
from scripts.audit_taifex_option_contract_presence import compare_contracts


REPO = Path(__file__).resolve().parents[1]
COLUMNS = ['date', 'option_id', 'ExercisePrice', 'contract_date', 'PutCall', 'price', 'volume']
NONPRICE = [column for column in COLUMNS if column != 'price']


def normalize(frame: pl.DataFrame) -> pl.DataFrame:
    if not set(COLUMNS) <= set(frame.columns):
        raise ValueError('option tick source schema incomplete')
    result = frame.select(COLUMNS).with_columns(
        pl.col('ExercisePrice').cast(pl.Float32), pl.col('price').cast(pl.Float32),
        pl.col('volume').cast(pl.Int64), vendor_option_series(pl.col('contract_date')).alias('contract_date'))
    if result.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError('null source comparison field')
    return result


def multiset_difference(left: pl.DataFrame, right: pl.DataFrame, columns: list[str]) -> tuple[int, int]:
    a, b = (left.group_by(columns).len(name='n'), right.group_by(columns).len(name='n'))
    joined = a.join(b, on=columns, how='full', coalesce=True, suffix='_right').with_columns(
        pl.col('n').fill_null(0).cast(pl.Int64), pl.col('n_right').fill_null(0).cast(pl.Int64))
    return (joined.select((pl.col('n') - pl.col('n_right')).clip(lower_bound=0).sum()).item() or 0,
            joined.select((pl.col('n_right') - pl.col('n')).clip(lower_bound=0).sum()).item() or 0)


def compare_frames(market: pl.DataFrame, product: pl.DataFrame) -> dict:
    a, b = normalize(market), normalize(product)
    left, right = multiset_difference(a, b, COLUMNS)
    left_base, right_base = multiset_difference(a, b, NONPRICE)
    # This lower bound does not impose a within-second ordering: it only counts
    # product nonpositive prices that cannot be paired with a nonpositive price
    # in the corresponding multiset of other fields.
    def nonpositive(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.group_by(NONPRICE).agg(pl.len().alias('n'),
                                            (pl.col('price') <= 0).sum().alias('nonpositive'))
    groups = nonpositive(a).join(nonpositive(b), on=NONPRICE, how='full', coalesce=True,
                                 suffix='_product').with_columns(
        pl.col('n').fill_null(0), pl.col('n_product').fill_null(0),
        pl.col('nonpositive').fill_null(0).cast(pl.Int64),
        pl.col('nonpositive_product').fill_null(0).cast(pl.Int64))
    lower = groups.filter(pl.col('n') == pl.col('n_product')).select(
        (pl.col('nonpositive_product') - pl.col('nonpositive')).clip(lower_bound=0).sum()).item() or 0
    return {'market_rows': a.height, 'product_rows': b.height,
            'price_inclusive_rows_only_in_market': left, 'price_inclusive_rows_only_in_product': right,
            'nonprice_rows_only_in_market': left_base, 'nonprice_rows_only_in_product': right_base,
            'product_nonpositive_vs_market_positive_rows_lower_bound': lower,
            'comparison': 'identical_at_float32' if not left and not right else 'source_row_difference'}


def audit(root: Path, readbacks: list[Path]) -> dict:
    root = root.resolve()
    pins: dict[str, str] = {}
    work = defaultdict(list)

    def pin(path: Path, expected: str | None = None) -> bytes:
        path = path.resolve()
        if not path.is_relative_to(REPO):
            raise ValueError('source or evidence outside workspace')
        digest = sha256_file(path)
        if expected is not None and digest != expected:
            raise ValueError('source evidence digest mismatch')
        if str(path) in pins and pins[str(path)] != digest:
            raise ValueError('source changed during audit')
        pins[str(path)] = digest
        return path.read_bytes()

    for path in sorted({p.resolve() for p in readbacks}):
        report = json.loads(pin(path))
        if report.get('contract') != 'finmind_option_tick_product_source_readback_v2':
            raise ValueError('incompatible option product source readback')
        plan_path = Path(report['source_plan']).resolve()
        plan = json.loads(pin(plan_path, report['source_plan_sha256']))
        checks = {(r['partition'], r['data_id']): r for r in plan['checks']}
        if len(checks) != len(plan['checks']) or len(checks) != len(report['results']):
            raise ValueError('product plan and results differ')
        for row in report['results']:
            key = (row['date'], row['data_id'])
            if key not in checks or row['dataset'] != 'TaiwanOptionTick':
                raise ValueError('product result identity differs')
            item = checks.pop(key)
            work[(row['date'], item['original_receipt'], item['original_receipt_sha256'],
                  item['original_parquet'], item['original_parquet_sha256'])].append((row, item))
        if checks:
            raise ValueError('product source readback is incomplete')
    if not work:
        raise ValueError('at least one completed product readback is required')
    results = []
    for (day, receipt_path, receipt_sha, market_path, market_sha), entries in sorted(work.items()):
        receipt = json.loads(pin(Path(receipt_path), receipt_sha))
        if ((receipt.get('dataset'), receipt.get('data_id'), receipt.get('partition'), receipt.get('status'))
                != ('TaiwanOptionTick', '', day, 'complete')
                or (root / receipt['parquet_path']).resolve() != Path(market_path).resolve()
                or receipt.get('sha256') != market_sha):
            raise ValueError('whole-market receipt identity differs')
        pin(Path(market_path), market_sha)
        market = pl.read_parquet(market_path)
        if market.height != receipt.get('rows'):
            raise ValueError('whole-market source row count differs')
        for row, item in entries:
            key = (day, row['data_id'])
            product_receipt = json.loads(pin(Path(row['receipt_path']), row['receipt_sha256']))
            if tuple(product_receipt.get(k) for k in ('dataset', 'partition', 'data_id')) != (
                    'TaiwanOptionTick', *key):
                raise ValueError('product receipt identity differs')
            selected = market.filter(pl.col('option_id') == key[1])
            result = {'date': day, 'product': key[1], 'status': row['status'],
                      'market_rows_for_product': selected.height,
                      'expected_missing_contracts': row['expected_contracts']}
            if row['status'] == 'provider_observed_empty_receipt_verified':
                if product_receipt.get('status') != 'observed_empty' or product_receipt.get('rows') != 0:
                    raise ValueError('product empty receipt differs')
                result['comparison'] = 'product_observed_empty'
            elif row['status'] in {'missing_keys_remain', 'candidate_keys_present'}:
                if (product_receipt.get('status') != 'complete'
                        or product_receipt.get('sha256') != row['parquet_sha256']
                        or (root / product_receipt['parquet_path']).resolve() != Path(row['parquet_path']).resolve()):
                    raise ValueError('product payload identity differs')
                pin(Path(row['parquet_path']), row['parquet_sha256'])
                product = pl.read_parquet(row['parquet_path'])
                if product.height != product_receipt.get('rows'):
                    raise ValueError('product payload row count differs')
                if product.filter((pl.col('option_id') != key[1]) | ~pl.col('date').str.starts_with(day)).height:
                    raise ValueError('product payload contains another product or date')
                result.update(compare_frames(selected, product))
                expected = pl.DataFrame(item['audited_missing_contracts'],
                    schema={'product': pl.String, 'series': pl.String, 'strike': pl.Float32, 'right': pl.String},
                    orient='row')
                if expected.unique().height != row['expected_contracts']:
                    raise ValueError('expected contract count differs')
                missing = compare_contracts(expected, product.filter((pl.col('price') > 0) & (pl.col('volume') > 0)))
                missing_after_unpriced = compare_contracts(missing, product.filter(
                    (pl.col('price') <= 0) & (pl.col('volume') > 0)))
                result['expected_keys_seen_only_with_nonpositive_price'] = missing.height - missing_after_unpriced.height
            else:
                raise ValueError('product readback is not terminal')
            results.append(result)
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source evidence changed during independent audit')
    states = Counter(row['comparison'] for row in results)
    return {'contract': 'finmind_option_tick_query_shape_source_audit_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'source_readbacks': len(readbacks),
            'product_days': len(results), 'market_days': len(work), 'comparison_states': dict(states),
            'product_nonpositive_vs_market_positive_rows_lower_bound': sum(
                row.get('product_nonpositive_vs_market_positive_rows_lower_bound', 0) for row in results),
            'expected_keys_seen_only_with_nonpositive_price': sum(
                row.get('expected_keys_seen_only_with_nonpositive_price', 0) for row in results),
            'days': results, 'input_pins': pins, 'audit_sha256': sha256_file(Path(__file__)),
            'provider_calls': 0, 'financial_or_training_adopted': False,
            'limitations': ['Float32 comparison cannot prove original decimal precision.',
                'Same timestamp and fields do not establish ordering or independent upstream sources.',
                'Empty or nonpositive response is not proof of no market transaction.',
                'The retained per-product JSON response bytes were not independently archived; the cause of differences is unknown.',
                'No source, eligibility, account, execution or training data are modified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_finmind/complement')
    parser.add_argument('--readback', type=Path, action='append', default=[])
    parser.add_argument('--readback-glob', action='append', default=[])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    paths = args.readback + [Path(name) for pattern in args.readback_glob for name in glob.glob(pattern)]
    output = args.output.resolve()
    if not output.is_relative_to(REPO / 'artifacts/markets') or output.exists() or output.is_symlink():
        raise ValueError('new versioned output must be inside artifacts/markets')
    result = audit(args.root, paths)
    atomic_write_json(output, result)
    print(json.dumps({'output': str(output), 'product_days': result['product_days'],
                      'comparison_states': result['comparison_states'],
                      'product_nonpositive_vs_market_positive_rows_lower_bound':
                          result['product_nonpositive_vs_market_positive_rows_lower_bound'],
                      'expected_keys_seen_only_with_nonpositive_price':
                          result['expected_keys_seen_only_with_nonpositive_price']}))


if __name__ == '__main__':
    main()
