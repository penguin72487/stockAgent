#!/usr/bin/env python3
"""Compare observed option reference keys with later official settlements.

This report is retrospective only.  It never supplies a trading mask or a
model feature: a product/series may be reused by another physical generation,
and the source's eventual settlement date was not observed at decision time.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_finmind_option_boundary_settlements import classify
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.download_taifex_stock_option_final_settlement import parse as parse_stock
from scripts.publish_taifex_gold_option_settlements import parse as parse_gold
from scripts.publish_taifex_index_option_settlements import parse as parse_index


KEYS = ['product_raw', 'series_raw', 'strike_decimal', 'right_raw', 'decision_date', 'decision_time']
SETTLEMENT_COLUMNS = ['product_raw', 'series_raw', 'settlement_date', 'positive_numeric_price']
CONTRACT = 'tw_option_preclock_retrospective_settlement_diagnostic_v1'


def annotate(references: pl.DataFrame, settlements: pl.DataFrame) -> pl.DataFrame:
    """Record source relationships without deciding listing or generation."""
    required = set(KEYS) | {'trade_ts'}
    if (not required <= set(references.columns) or references.is_empty()
            or references.select(KEYS).is_duplicated().any()
            or references.select(list(required)).null_count().to_numpy().sum() != 0
            or not set(SETTLEMENT_COLUMNS) <= set(settlements.columns)):
        raise ValueError('invalid or duplicate reference identity or settlement schema')
    eligible = settlements.filter(pl.col('positive_numeric_price'))
    if eligible.select(SETTLEMENT_COLUMNS).null_count().to_numpy().sum() != 0:
        raise ValueError('positive settlement rows require an explicit identity and date')
    grouped = (eligible.group_by('product_raw', 'series_raw').agg(
        pl.len().alias('matching_source_rows'),
        pl.col('settlement_date').sort().alias('source_settlement_dates')))
    result = references.join(grouped, on=['product_raw', 'series_raw'], how='left', validate='m:1')
    count = pl.col('matching_source_rows').fill_null(0)
    one_date = pl.col('source_settlement_dates').list.first()
    decision = pl.col('decision_date')
    state = (pl.when(count == 0).then(pl.lit('no_source_settlement_key'))
        .when(count > 1).then(pl.lit('multiple_source_rows_generation_unknown'))
        .when(one_date < decision).then(pl.lit('single_source_settlement_before_decision_day'))
        .when(one_date == decision).then(pl.lit('single_source_settlement_on_decision_day'))
        .otherwise(pl.lit('single_source_settlement_after_decision_day')))
    trade_relation = (pl.when((count == 1) & (one_date < decision))
        .then(pl.when(pl.col('trade_ts').dt.date() > one_date)
              .then(pl.lit('reference_trade_after_source_settlement_generation_unknown'))
              .otherwise(pl.lit('reference_trade_on_or_before_source_settlement')))
        .otherwise(pl.lit('not_applicable')))
    result = result.with_columns(count.alias('matching_source_rows'),
                                 state.alias('settlement_relation'),
                                 trade_relation.alias('reference_trade_relation'))
    # Reuse the scalar source diagnostic as a second implementation.  It is a
    # count check, not proof that one source key identifies a physical contract.
    scalar = classify(references, settlements)['by_day_clock_product_series_state']
    expected = {(r['date'], r['clock'], r['product'], r['series'], r['state']): r['rows']
                for r in scalar}
    actual = {(str(d), c, p, s, state): n for d, c, p, s, state, n in result.group_by(
        'decision_date', 'decision_time', 'product_raw', 'series_raw', 'settlement_relation')
        .len().iter_rows()}
    if actual != expected:
        raise ValueError('vectorized settlement relationships differ from scalar diagnostic')
    return result.sort(KEYS)


def _manifest_source(path: Path, expected: str, parser, pins: dict[str, str]) -> pl.DataFrame:
    path = path.resolve()
    doc = json.loads(path.read_bytes())
    if doc.get('contract') != expected or doc.get('training_ready') is not False:
        raise ValueError('source-only official settlement manifest required')
    pins[str(path)] = sha256_file(path)
    raw, data = Path(doc['raw_path']), Path(doc['data_path'])
    for source, digest in ((raw, doc['raw_sha256']), (data, doc['data_sha256'])):
        if source.is_symlink() or sha256_file(source) != digest:
            raise ValueError('official source changed')
        pins[str(source)] = digest
    frame = pl.read_parquet(data)
    if not frame.equals(parser(raw.read_bytes(), date.fromisoformat(doc['requested_start']),
                               date.fromisoformat(doc['requested_end']))):
        raise ValueError('official raw settlement reparse differs')
    return frame.select(SETTLEMENT_COLUMNS)


def _stock_source(path: Path, pins: dict[str, str]) -> pl.DataFrame:
    path = path.resolve()
    doc = json.loads(path.read_bytes())
    if (doc.get('contract') != 'taifex_stock_option_final_settlement_month_v1'
            or doc.get('training_ready') is not False):
        raise ValueError('source-only official stock settlement receipt required')
    pins[str(path)] = sha256_file(path)
    raw, data = Path(doc['raw_path']), Path(doc['payload_path'])
    for source, digest in ((raw, doc['raw_sha256']), (data, doc['payload_sha256'])):
        if source.is_symlink() or sha256_file(source) != digest:
            raise ValueError('official stock settlement source changed')
        pins[str(source)] = digest
    year, month = map(int, doc['query_month'].split('-'))
    frame = pl.read_parquet(data)
    if not frame.equals(parse_stock(raw.read_bytes(), year, month)):
        raise ValueError('official stock settlement raw reparse differs')
    return frame.select(SETTLEMENT_COLUMNS)


def build(window_manifest: Path, window_readback: Path, index_manifest: Path,
          gold_manifest: Path, stock_receipts: list[Path], output: Path, report: Path) -> dict:
    workspace = Path.cwd().resolve()
    window_manifest, window_readback, index_manifest, gold_manifest, output, report = (
        p.resolve() for p in (window_manifest, window_readback, index_manifest, gold_manifest, output, report))
    if (not output.is_relative_to(workspace / 'data_finmind/research')
            or not report.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or report.exists() or not stock_receipts):
        raise ValueError('new research and report paths and stock receipts are required')
    window, readback = (json.loads(p.read_bytes()) for p in (window_manifest, window_readback))
    if (window != readback or window.get('contract') != 'tw_option_preclock_window_union_v1'
            or window.get('training_ready') is not False):
        raise ValueError('matching research window and readback required')
    pins = {str(p): sha256_file(p) for p in (window_manifest, window_readback, Path(__file__).resolve(),
        Path('scripts/audit_finmind_option_boundary_settlements.py').resolve())}
    payload = Path(window['data_path'])
    if payload.is_symlink() or sha256_file(payload) != window['data_sha256']:
        raise ValueError('reference source payload changed')
    pins[str(payload)] = window['data_sha256']
    references = pl.read_parquet(payload)
    if references.height != window['rows']:
        raise ValueError('reference source row count differs')
    index = _manifest_source(index_manifest, 'taifex_index_option_final_settlement_source_v1', parse_index, pins)
    gold = _manifest_source(gold_manifest, 'taifex_gold_option_final_settlement_source_v1', parse_gold, pins)
    stock = [_stock_source(p, pins) for p in stock_receipts]
    settlements = pl.concat([index, gold, *stock])
    capacity = capacity_check(output.parent)
    annotated = annotate(references, settlements)
    if any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
        raise ValueError('input changed during diagnostic')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    result_path = output / 'retrospective_diagnostic.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.diagnostic-', suffix='.tmp', delete=False) as f:
        temporary = Path(f.name)
    try:
        annotated.write_parquet(temporary, compression='zstd')
        if not annotated.equals(pl.read_parquet(temporary)):
            raise ValueError('diagnostic publication readback differs')
        durable_replace(temporary, result_path)
    finally:
        temporary.unlink(missing_ok=True)
    arrow_rows = pq.read_table(result_path).to_pylist()
    readback = Counter((r['decision_time'], r['settlement_relation'], r['reference_trade_relation'])
                       for r in arrow_rows)
    counts = Counter((r['decision_time'], r['settlement_relation'], r['reference_trade_relation'])
                     for r in annotated.iter_rows(named=True))
    if readback != counts or len(arrow_rows) != annotated.height:
        raise ValueError('independent Arrow readback differs')
    if any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
        raise ValueError('input changed during publication')
    result = {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'decision_date': window['decision_date'], 'source_window': window['official_window'],
              'data_path': str(result_path), 'data_sha256': sha256_file(result_path),
              'rows': annotated.height, 'input_pins': pins, 'capacity_at_start': capacity,
              'by_clock_and_source_relationship': [
                  {'clock': c, 'settlement_relation': s, 'reference_trade_relation': t, 'rows': n}
                  for (c, s, t), n in sorted(counts.items())],
              'independent_arrow_rows_checked': len(arrow_rows),
              'scalar_count_mismatches': 0, 'training_ready': False,
              'limitations': [
                  'Retrospective final settlement dates are not decision-time features or admission.',
                  'Product/series match does not prove the same physical generation.',
                  'Multiple matching source rows remain ambiguous, including identical duplicates.',
                  'Unmatched keys may be missing sources or not yet settled; they are not listed proof.',
                  'Same-day settlement does not establish whether a contract was available at the clock.',
                  'No source result proves historical delivery, executable price, account or training readiness.']}
    atomic_write_json(output / 'manifest.json', result)
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('window-manifest', 'window-readback', 'index-manifest', 'gold-manifest', 'output', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--stock-receipt', type=Path, action='append', required=True)
    args = parser.parse_args()
    result = build(args.window_manifest, args.window_readback, args.index_manifest, args.gold_manifest,
                   args.stock_receipt, args.output, args.report)
    print(json.dumps({'report': str(args.report), 'rows': result['rows'],
                      'by_clock_and_source_relationship': result['by_clock_and_source_relationship'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
