#!/usr/bin/env python3
"""Audit FinLab option trade date labels against retained TAIFEX side rows.

This is a source comparison, not a converter or execution dataset. Duplicate
side rows and source dates remain untouched. A key is called date-resolvable
only if its official matches have one physical date and sufficient side rows.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches


KEY = ['product', 'series', 'strike', 'right', 'time', 'price', 'quantity', 'auction']


def vendor_keys(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        pl.col('symbol').cast(pl.String).alias('product'),
        pl.col('到期月份(週別)').cast(pl.String).str.strip_chars().alias('series'),
        pl.col('履約價格').cast(pl.Float64, strict=False).alias('strike'),
        pl.when(pl.col('買賣權') == '買權').then(pl.lit('C'))
          .when(pl.col('買賣權') == '賣權').then(pl.lit('P'))
          .otherwise(None).alias('right'),
        pl.col('成交時間').cast(pl.String).str.strip_chars().alias('time'),
        pl.col('成交價格').cast(pl.Float64, strict=False).alias('price'),
        pl.col('成交數量').cast(pl.Float64, strict=False).alias('quantity'),
        pl.col('開盤集合競價').cast(pl.String).fill_null('').str.strip_chars().alias('auction'),
    )


def official_keys(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        pl.col('product_raw').alias('product'), pl.col('series_raw').alias('series'),
        pl.col('strike_raw').cast(pl.Float64, strict=False).alias('strike'),
        pl.col('right_raw').alias('right'), pl.col('event_time_raw').alias('time'),
        pl.col('price_raw').cast(pl.Float64, strict=False).alias('price'),
        pl.col('side_quantity_raw').cast(pl.Float64, strict=False).alias('quantity'),
        pl.col('auction_raw').alias('auction'), 'event_date',
    )


def compare(vendor: pl.DataFrame, official: pl.DataFrame) -> dict:
    """Compare exact normalized row multisets without assigning source rows.

    Prices/strikes compare as Float64 because the vendor object is Float64;
    this cannot prove equality to the exchange's original decimal text.
    """
    valid = (pl.all_horizontal([pl.col(k).is_not_null() for k in KEY])
             & pl.col('strike').is_finite() & pl.col('price').is_finite()
             & pl.col('quantity').is_finite() & (pl.col('quantity') > 0))
    vendor_valid = vendor.filter(valid)
    official_valid = official.filter(valid & pl.col('event_date').is_not_null())
    vg = vendor_valid.group_by(KEY).agg(pl.len().cast(pl.UInt64).alias('vendor_rows'))
    by_date = official_valid.group_by(KEY + ['event_date']).agg(pl.len().cast(pl.UInt64).alias('official_rows'))
    og = by_date.group_by(KEY).agg(
        pl.col('official_rows').sum().alias('official_rows'),
        pl.col('event_date').n_unique().alias('official_date_count'),
        pl.col('event_date').min().alias('only_or_first_date'),
    )
    matched = vg.join(og, on=KEY, how='left').with_columns(
        pl.col('official_rows').fill_null(0), pl.col('official_date_count').fill_null(0),
    ).with_columns(
        pl.min_horizontal('vendor_rows', 'official_rows').alias('matched_side_rows_upper_bound'),
    )
    resolvable = matched.filter((pl.col('official_date_count') == 1)
                                 & (pl.col('official_rows') >= pl.col('vendor_rows')))
    ambiguous = matched.filter(pl.col('official_date_count') > 1)
    insufficient = matched.filter((pl.col('official_date_count') > 0)
                                   & (pl.col('official_rows') < pl.col('vendor_rows')))
    missing = matched.filter(pl.col('official_date_count') == 0)

    def rows(f: pl.DataFrame) -> int:
        return int(f['vendor_rows'].sum() or 0)

    dates = resolvable.group_by('only_or_first_date').agg(
        pl.col('vendor_rows').sum().alias('vendor_side_rows')
    ).sort('only_or_first_date').to_dicts()
    for item in dates:
        item['official_event_date'] = item.pop('only_or_first_date').isoformat()
    return {
        'vendor_side_rows': vendor.height, 'invalid_vendor_side_rows': vendor.height-vendor_valid.height,
        'official_side_rows': official.height, 'invalid_official_side_rows': official.height-official_valid.height,
        'vendor_distinct_normalized_keys': vg.height,
        'matched_side_rows_upper_bound': int(matched['matched_side_rows_upper_bound'].sum() or 0),
        'unique_physical_date_and_sufficient_count_vendor_side_rows': rows(resolvable),
        'ambiguous_multiple_physical_dates_vendor_side_rows': rows(ambiguous),
        'insufficient_official_multiplicity_vendor_side_rows': rows(insufficient),
        'no_official_key_vendor_side_rows': rows(missing),
        'unique_physical_date_by_vendor_side_rows': dates,
    }


def load_version(root: Path, receipt_path: Path) -> tuple[dict, pl.DataFrame, dict]:
    receipt_hash = sha256_file(receipt_path)
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    if receipt.get('dataset') != 'tw_option_recent_trades':
        raise ValueError('not an option recent trades receipt')
    rel = Path(receipt['parquet_path'])
    if rel.is_absolute() or '..' in rel.parts or rel.parts[:1] != ('datasets',):
        raise ValueError('unsafe FinLab source path')
    obj = root / rel
    if sha256_file(obj) != receipt.get('sha256'):
        raise ValueError('FinLab object does not match receipt')
    frame = pl.read_parquet(obj)
    if frame.height != receipt.get('rows') or sha256_file(obj) != receipt['sha256']:
        raise ValueError('FinLab object changed or row count disagrees')
    if sha256_file(receipt_path) != receipt_hash:
        raise ValueError('FinLab receipt changed during read')
    evidence = {'receipt': str(receipt_path.resolve()), 'receipt_sha256': receipt_hash,
                'object': str(obj.resolve()), 'object_sha256': receipt['sha256'],
                'fetched_at_utc': receipt.get('fetched_at_utc'),
                'source_checked_at_utc': receipt.get('source_checked_at_utc'),
                'source_event_labels': sorted(str(x) for x in frame['date'].unique().to_list())}
    return receipt, frame, evidence


def audit(root: Path, older: Path, newer: Path, manifest: Path, trading_day: date) -> dict:
    script_hash = sha256_file(Path(__file__))
    reader_path = Path('stockagent/data/tw_option_product_ticks.py')
    reader_hash = sha256_file(reader_path)
    _, first, first_evidence = load_version(root, older)
    _, second, second_evidence = load_version(root, newer)
    source = pin_official_option_tick_inventory(manifest, [trading_day])[0]
    official_frames = [official_keys(f) for f in iter_official_option_trade_batches(source)]
    if not official_frames:
        raise ValueError('official archive has no trade rows')
    result = compare(vendor_keys(second), pl.concat(official_frames, rechunk=False))
    if (sha256_file(Path(__file__)) != script_hash or sha256_file(reader_path) != reader_hash
            or sha256_file(older) != first_evidence['receipt_sha256']
            or sha256_file(newer) != second_evidence['receipt_sha256']
            or sha256_file(Path(first_evidence['object'])) != first_evidence['object_sha256']
            or sha256_file(Path(second_evidence['object'])) != second_evidence['object_sha256']):
        raise ValueError('audit source changed before completion')
    return {
        'contract': 'finlab_option_trade_date_axis_source_audit_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'audit_sha256': script_hash, 'official_reader_sha256': reader_hash,
        'older_vendor': first_evidence, 'newer_vendor': second_evidence,
        'vendor_objects_equal_except_date_in_source_order': first.drop('date').equals(second.drop('date')),
        'official_exchange_trading_date': trading_day.isoformat(), 'official_source': source,
        'comparison': result,
        'limitations': [
            'FinLab date is a vendor label; official event date and exchange trading date are distinct.',
            'Float64 comparison is an audit, not proof of original decimal representation.',
            'Identical whole-second side rows are not independent fills or unique exchange event IDs; '
            'a date is unique only among rows of the compared official archive.',
            'No source row is corrected, admitted to training, or used as an executable quote.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finlab'))
    parser.add_argument('--older-receipt', type=Path, required=True)
    parser.add_argument('--newer-receipt', type=Path, required=True)
    parser.add_argument('--official-manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--official-trading-date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve previous audit; choose a new output')
    result = audit(args.root, args.older_receipt, args.newer_receipt,
                   args.official_manifest, args.official_trading_date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'comparison': result['comparison']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
