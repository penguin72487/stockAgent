#!/usr/bin/env python3
"""Classify unusable official price rows before requesting a source recheck.

Uses the existing official converters and stock/ETF symbol scope. No prices,
source files, feature ABI or execution masks are rewritten. Zero-quantity
no-trade records are not invented missing executions.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, UTC
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.source_anomalies import file_binding
from scripts import build_tw_official_symbol_parquets as native
from scripts.audit_source_anomalies import csv_file


def classify(frame: pl.DataFrame) -> pl.DataFrame:
    positive = pl.all_horizontal(*(
        (pl.col(c).is_finite() & (pl.col(c) > 0)).fill_null(False)
        for c in ('open', 'max', 'min', 'close')))
    geometry = ((pl.col('max') >= pl.max_horizontal('open', 'min', 'close'))
                & (pl.col('min') <= pl.min_horizontal('open', 'max', 'close'))).fill_null(False)
    sentinel = ((pl.col('open') == 0) & (pl.col('max') == 0) & (pl.col('min') == 0)
                & pl.col('close').is_finite() & (pl.col('close') > 0)).fill_null(False)
    no_regular_price = pl.all_horizontal(*(
        (pl.col(c).is_null() | (pl.col(c) == 0)).fill_null(False) for c in ('open', 'max', 'min', 'close')))
    volume = pl.col('Trading_Volume')
    return frame.with_columns(
        pl.when(~volume.is_finite().fill_null(False) | (volume < 0).fill_null(False))
        .then(pl.lit('invalid_or_unknown_quantity'))
        .when(positive & geometry).then(pl.lit('usable_native_price'))
        .when(sentinel).then(pl.lit('normal_native_positive_close_sentinel'))
        .when(volume == 0).then(pl.lit('no_trade_unpriceable_not_missing_execution'))
        .when(no_regular_price).then(pl.lit('quantity_without_regular_OHLC_trade_scope_unverified'))
        .otherwise(pl.lit('positive_quantity_invalid_price_requires_source_recheck'))
        .alias('classification'),
        (~(positive & geometry | sentinel)).alias('native_builder_unusable'),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--end-date', required=True)
    args = parser.parse_args()
    public, out = args.root / 'data_tw_public', args.output_dir
    sources, priorities, unresolved_scope = [], [], []
    cutoff = date.fromisoformat(args.end_date)
    for name, converter in zip(native.OFFICIAL_CORE_SOURCE_FILENAMES[:2],
                               (native._twse_frame, native._tpex_frame), strict=True):
        path = public / name
        before = file_binding(path)
        source_sha = sha256_file(path)
        frame = converter(path).filter(
            (pl.col('date') <= pl.lit(cutoff)) & (pl.col('date') >= pl.lit(date(2000, 1, 1)))
            & (pl.col('symbol').str.contains(native.TW_STOCK_SYMBOL_PATTERN)
               | pl.col('symbol').str.contains(native.TW_ETF_SYMBOL_PATTERN)).fill_null(False))
        classified = classify(frame)
        active_bad = classified.filter(pl.col('classification').is_in([
            'positive_quantity_invalid_price_requires_source_recheck', 'invalid_or_unknown_quantity']))
        rows = active_bad.select('date', 'symbol', 'market', 'Trading_Volume',
                                 'open', 'max', 'min', 'close', 'classification').to_dicts()
        priorities.extend({**r, 'source_file': name, 'source_sha256': source_sha,
                           'priority': 1, 'source_empty_is_not_gap_fill': True} for r in rows)
        unresolved_scope.extend({**r, 'source_file': name, 'source_sha256': source_sha,
                                 'requires_regular_vs_odd_lot_scope_evidence': True,
                                 'automatic_price_gap_repair_authorized_by_evidence': False}
            for r in classified.filter(pl.col('classification') ==
                'quantity_without_regular_OHLC_trade_scope_unverified').select(
                    'date', 'symbol', 'market', 'Trading_Volume', 'open', 'max', 'min',
                    'close', 'classification').to_dicts())
        csv_file(out / f'{path.stem}_active_price_rechecks.csv', rows)
        sources.append({
            'source': name, 'scoped_rows': frame.height, 'source_sha256': source_sha,
            'classification': dict(classified.group_by('classification').len().iter_rows()),
            'native_builder_unusable_rows': int(classified['native_builder_unusable'].sum()),
            'genuine_source_recheck_candidate_rows': len(rows),
            'source_binding_unchanged': file_binding(path) == before,
        })
        if not sources[-1]['source_binding_unchanged']:
            raise RuntimeError('official source changed during classification; do not dispatch')
        print(f'[tw-raw-quality] source={name} active_rechecks={len(rows)}', flush=True)
    csv_file(out / 'priority_worklist.csv', priorities)
    csv_file(out / 'unverified_trade_scope.csv', unresolved_scope)
    atomic_write_json(out / 'summary.json', {
        'contract': 'native_tw_raw_price_classification_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'TEJ_excluded': True,
        'network_calls': 0, 'source_files_rewritten': 0, 'sources': sources,
        'source_recheck_candidate_rows': len(priorities),
        'unverified_trade_scope_rows': len(unresolved_scope),
        'unverified_trade_scope_sha256': sha256_file(out / 'unverified_trade_scope.csv'),
        'source_recheck_is_not_proof_of_recoverable_prices': True,
        'no_trade_rows_are_not_missing_execution_prices': True,
        'quantity_scope_rule': 'Odd-lot prices do not define ordinary-session OHLC; positive total quantity alone is not proof of missing ordinary executions',
        'official_scope_reference': 'https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW',
        'worklist_sha256': sha256_file(out / 'priority_worklist.csv'),
    })


if __name__ == '__main__':
    main()
