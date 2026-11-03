#!/usr/bin/env python3
"""Independently re-read a retrospective preclock settlement diagnostic.

Index and stock records are extracted from retained official HTML with the
stdlib parser. Gold uses the existing independent shared wide-table parser.
This does not turn later settlement information into a trading decision.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import unicodedata

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_stock_option_settlement_collection import NativeHTML, compare_rows, native_rows
from scripts.download_taifex_futures_final_settlement_history import parse_index_futures_final_settlement_html


def _positive(value: str) -> bool:
    try:
        number = Decimal(value.replace(',', ''))
        return number.is_finite() and number > 0
    except InvalidOperation:
        return False


def native_index(body: bytes, normalized: list[dict]) -> list[tuple[str, str, date]]:
    """Compare every index cell, including Friday series, against native HTML."""
    parser = NativeHTML()
    parser.feed(body.decode('utf-8'))
    matches = [t for t in parser.tables if len(t['headers']) >= 3
               and '最後' in t['headers'][0] and '契約' in t['headers'][1]]
    if len(matches) != 1:
        raise ValueError('one official index settlement table is required')
    table = matches[0]
    codes = []
    for header in table['headers'][2:]:
        matches = re.findall(r'\(([A-Z][A-Z0-9]{1,5})\)', unicodedata.normalize('NFKC', header))
        if len(matches) != 1:
            raise ValueError('unsupported official index product header')
        codes.append(matches[0])
    if len(set(codes)) != len(codes):
        raise ValueError('duplicate official index product headers')
    expected = []
    for ordinal, cells in enumerate(table['rows'], 1):
        if len(cells) != len(codes) + 2:
            raise ValueError('invalid official index source row')
        day = datetime.strptime(cells[0], '%Y/%m/%d').date()
        for product, raw in zip(codes, cells[2:], strict=True):
            expected.append((ordinal, day, cells[1], product, raw, _positive(raw)))
    actual = [(r['source_row'], r['settlement_date'], r['series_raw'], r['product_raw'],
               r['settlement_price_raw'], r['positive_numeric_price']) for r in normalized]
    if actual != expected:
        raise ValueError('native index HTML and normalized cells differ')
    return [(product, series, day) for _, day, series, product, _, positive in expected if positive]


def relationship(row: dict, matches: list[date]) -> tuple[str, str]:
    if not matches:
        return 'no_source_settlement_key', 'not_applicable'
    if len(matches) > 1:
        return 'multiple_source_rows_generation_unknown', 'not_applicable'
    settled, decision = matches[0], row['decision_date']
    if settled < decision:
        trade = ('reference_trade_after_source_settlement_generation_unknown'
                 if row['trade_ts'].date() > settled
                 else 'reference_trade_on_or_before_source_settlement')
        return 'single_source_settlement_before_decision_day', trade
    if settled == decision:
        return 'single_source_settlement_on_decision_day', 'not_applicable'
    return 'single_source_settlement_after_decision_day', 'not_applicable'


def audit(parent: Path, index_manifest: Path, gold_manifest: Path, stock_receipts: list[Path]) -> dict:
    paths = [parent.resolve(), index_manifest.resolve(), gold_manifest.resolve(),
             *[p.resolve() for p in stock_receipts]]
    parent, index_manifest, gold_manifest, *stock_receipts = paths
    doc = json.loads(parent.read_bytes())
    if (doc.get('contract') != 'tw_option_preclock_retrospective_settlement_diagnostic_v1'
            or doc.get('training_ready') is not False):
        raise ValueError('research diagnostic receipt required')
    pins = {str(p): sha256_file(p) for p in paths}
    pins[str(Path(__file__).resolve())] = sha256_file(Path(__file__))
    for source in paths[1:]:
        if doc['input_pins'].get(str(source)) != pins[str(source)]:
            raise ValueError('settlement receipt differs from diagnostic source')
    index, gold = (json.loads(p.read_bytes()) for p in (index_manifest, gold_manifest))
    entries = []
    for src in [index, gold, *[json.loads(p.read_bytes()) for p in stock_receipts]]:
        raw = Path(src['raw_path'])
        data = Path(src.get('payload_path', src.get('data_path')))
        for p, digest in ((raw, src['raw_sha256']), (data, src.get('payload_sha256', src.get('data_sha256')))):
            if p.is_symlink() or sha256_file(p) != digest or doc['input_pins'].get(str(p)) != digest:
                raise ValueError('raw or normalized source does not match diagnostic receipt')
            pins[str(p)] = digest
    index_rows = pq.read_table(index['data_path']).to_pylist()
    entries.extend(native_index(Path(index['raw_path']).read_bytes(), index_rows))
    gold_frame = parse_index_futures_final_settlement_html(
        Path(gold['raw_path']).read_bytes(), start_date=date.fromisoformat(gold['requested_start']),
        end_date=date.fromisoformat(gold['requested_end']), source_file=gold['raw_path'],
        source_sha256=gold['raw_sha256'], source_url=gold['url'],
        source_kind='official_gold_option_html', allowed_products=frozenset({'TGO'}))
    gold_rows = pq.read_table(gold['data_path']).to_pylist()
    independent_gold = [(r['product'], r['contract'], r['settlement_date']) for r in gold_frame.to_dicts()]
    normalized_gold = [(r['product_raw'], r['series_raw'], r['settlement_date']) for r in gold_rows
                       if r['positive_numeric_price']]
    if sorted(independent_gold) != sorted(normalized_gold):
        raise ValueError('independent gold source identities differ')
    entries.extend(independent_gold)
    stock_count = 0
    for receipt in stock_receipts:
        item = json.loads(receipt.read_bytes())
        raw_rows = native_rows(Path(item['raw_path']).read_bytes(), item['query_month'])
        normalized = pl.read_parquet(item['payload_path'])
        compare_rows(raw_rows, normalized)
        stock_count += len(raw_rows)
        for r in raw_rows:
            if _positive(r[5]):
                entries.append((r[1], r[4], datetime.strptime(r[3], '%Y/%m/%d').date()))
    source = defaultdict(list)
    for product, series, day in entries:
        source[(product, series)].append(day)
    output = Path(doc['data_path'])
    if output.is_symlink() or sha256_file(output) != doc['data_sha256']:
        raise ValueError('diagnostic payload changed')
    pins[str(output)] = doc['data_sha256']
    counts = Counter()
    rows = pq.read_table(output).to_pylist()
    for row in rows:
        dates = sorted(source[(row['product_raw'], row['series_raw'])])
        state, trade = relationship(row, dates)
        if (row['matching_source_rows'] != len(dates) or (row['source_settlement_dates'] or []) != dates
                or row['settlement_relation'] != state or row['reference_trade_relation'] != trade):
            raise ValueError('native settlement diagnostic mismatch')
        counts[(row['decision_time'], state, trade)] += 1
    expected = {(r['clock'], r['settlement_relation'], r['reference_trade_relation']): r['rows']
                for r in doc['by_clock_and_source_relationship']}
    if counts != expected or len(rows) != doc['rows']:
        raise ValueError('native settlement diagnostic totals differ')
    if any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('source changed during native readback')
    return {'contract': 'tw_option_preclock_retrospective_settlement_native_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'input_pins': pins,
            'index_cells_checked': len(index_rows), 'stock_source_rows_checked': stock_count,
            'gold_positive_source_rows_checked': len(independent_gold),
            'diagnostic_rows_checked': len(rows), 'row_mismatches': 0,
            'friday_index_cells_checked': sum('F' in r['series_raw'] for r in index_rows),
            'training_ready': False, 'limitations': [
                'Independent parsers read the same retained exchange pages, not independent market feeds.',
                'Final settlement is retrospective and cannot be used as a preclock model input.',
                'Product/series match does not establish generation, actual execution or training eligibility.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('parent', 'index-manifest', 'gold-manifest', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--stock-receipt', type=Path, action='append', required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve existing native readback')
    result = audit(args.parent, args.index_manifest, args.gold_manifest, args.stock_receipt)
    atomic_write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k not in {'input_pins', 'limitations'}},
                     ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
