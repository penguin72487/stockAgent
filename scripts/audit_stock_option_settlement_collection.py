#!/usr/bin/env python3
"""Independently read back retained stock/ETF option settlement month pages.

This audits raw source rows and artifact hashes only. Settlement information
is retrospective, is not an option premium, and does not admit training.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import parse_qs, urlparse

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


HEADERS = ['商品名稱', '商品代號', '標的證券代號', '到期日', '契約月份', '最後結算價']
FIELDS = ['product_name_raw', 'product_raw', 'underlying_raw', 'settlement_date',
          'series_raw', 'settlement_price_raw', 'contract_value_raw']


class NativeHTML(HTMLParser):
    """Extract HTML table cells with stdlib, independently of the producer parser."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables = []
        self.stack = []
        self.select = None
        self.selected = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == 'select':
            self.select = attributes.get('name')
        elif tag == 'option' and self.select and 'selected' in attributes:
            self.selected.setdefault(self.select, []).append(attributes.get('value', '').strip())
        elif tag == 'table':
            self.stack.append({'headers': [], 'rows': [], 'tbody': False, 'row': None, 'cell': None})
        elif self.stack:
            table = self.stack[-1]
            if tag == 'tbody':
                table['tbody'] = True
            elif tag == 'tr' and table['tbody']:
                table['row'] = []
            elif tag in {'td', 'th'}:
                table['cell'] = {'kind': tag, 'text': []}

    def handle_data(self, data):
        if self.stack and self.stack[-1]['cell'] is not None and data.strip():
            self.stack[-1]['cell']['text'].append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'select':
            self.select = None
        elif tag == 'table' and self.stack:
            self.tables.append(self.stack.pop())
        elif self.stack:
            table = self.stack[-1]
            if tag in {'td', 'th'} and table['cell'] is not None and table['cell']['kind'] == tag:
                text = ' '.join(table['cell']['text'])
                if tag == 'th':
                    table['headers'].append(text)
                elif table['row'] is not None:
                    table['row'].append(text)
                table['cell'] = None
            elif tag == 'tr' and table['tbody'] and table['row'] is not None:
                table['rows'].append(table['row'])
                table['row'] = None
            elif tag == 'tbody':
                table['tbody'] = False


def native_rows(body: bytes, month: str) -> list[list]:
    parser = NativeHTML()
    parser.feed(body.decode('utf-8'))
    year, part = month.split('-')
    if parser.selected.get('queryYear') != [year] or parser.selected.get('queryMonth') != [part]:
        raise ValueError('raw page selection differs from requested month')
    tables = [table for table in parser.tables if table['headers'][:6] == HEADERS]
    if len(tables) != 1:
        raise ValueError('expected exactly one source settlement table')
    table = tables[0]
    if (len(table['headers']) not in {6, 7}
            or (len(table['headers']) == 7 and table['headers'][6] != '約定標的物價值')):
        raise ValueError('unexpected raw settlement headers')
    rows = table['rows']
    if rows == [['查無資料']]:
        return []
    if any(len(row) != len(table['headers']) for row in rows):
        raise ValueError('unexpected raw settlement row shape')
    return [row + ([None] if len(row) == 6 else []) for row in rows]


def compare_rows(rows: list[list], frame: pl.DataFrame) -> dict:
    if len(rows) != frame.height or not set(FIELDS + ['source_row', 'supported_key', 'positive_numeric_price']) <= set(frame.columns):
        raise ValueError('source and normalized row counts or schema differ')
    counts = Counter()
    for ordinal, (raw, item) in enumerate(zip(rows, frame.to_dicts(), strict=True), 1):
        date_value = datetime.strptime(raw[3], '%Y/%m/%d').date()
        expected = [raw[0], raw[1], raw[2], date_value, raw[4], raw[5], raw[6]]
        if item['source_row'] != ordinal or [item[name] for name in FIELDS] != expected:
            raise ValueError(f'normalized source value mismatch at row {ordinal}')
        supported = bool(re.fullmatch(r'[A-Z][A-Z0-9]{1,5}', raw[1])
                         and re.fullmatch(r'\d{6}(?:[WF][1-5])?', raw[4]))
        try:
            value = Decimal(raw[5].replace(',', ''))
            positive = value.is_finite() and value > 0
        except InvalidOperation:
            positive = False
        if item['supported_key'] != supported or item['positive_numeric_price'] != positive:
            raise ValueError(f'normalized key or positive-price flag mismatch at row {ordinal}')
        counts['positive_price_rows'] += positive
        counts['unsupported_key_rows'] += not supported
        counts['underlying_leading_zero_rows'] += raw[2].startswith('0')
        if supported and date_value.strftime('%Y%m') < raw[4][:6]:
            counts['settled_before_contract_month_rows'] += 1
    return dict(counts)


def audit(collection: Path) -> dict:
    collection = collection.resolve()
    summary_sha = sha256_file(collection)
    summary = json.loads(collection.read_bytes())
    if (summary.get('contract') != 'taifex_stock_option_final_settlement_collection_v1'
            or summary.get('training_ready') is not False or summary.get('capacity_deferred') is not False
            or summary.get('planned_months') != summary.get('attempted_months')
            or len(summary.get('results', [])) != summary['planned_months']):
        raise ValueError('a complete source collection summary is required')
    start = datetime.strptime(summary['start'], '%Y-%m-%d').date()
    end = datetime.strptime(summary['end'], '%Y-%m-%d').date()
    year, month = start.year, start.month
    expected_months = []
    while (year, month) <= (end.year, end.month):
        expected_months.append(f'{year:04d}-{month:02d}')
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    items, totals, products = [], Counter(), set()
    for result in summary['results']:
        if result['state'] not in {'built_verified', 'reused_verified'}:
            raise ValueError('collection contains an unverified month')
        receipt = Path(result['receipt']).resolve()
        if sha256_file(receipt) != result['receipt_sha256']:
            raise ValueError('source month receipt changed')
        doc = json.loads(receipt.read_bytes())
        if (doc.get('contract') != 'taifex_stock_option_final_settlement_month_v1'
                or doc.get('query_month') != result['query_month'] or doc.get('training_ready') is not False):
            raise ValueError('source receipt identity differs')
        parsed_url = urlparse(doc['url'])
        query = parse_qs(parsed_url.query)
        year, month = result['query_month'].split('-')
        if (parsed_url.scheme != 'https' or parsed_url.netloc != 'www.taifex.com.tw'
                or parsed_url.path != '/cht/5/sSOFSP' or query != {
                    'down_type': ['1'], 'queryYear': [year], 'queryMonth': [month]}):
            raise ValueError('source receipt URL does not identify the official requested month')
        raw, payload = Path(doc['raw_path']).resolve(), Path(doc['payload_path']).resolve()
        if (sha256_file(raw) != doc['raw_sha256'] or sha256_file(payload) != doc['payload_sha256']):
            raise ValueError('source raw or normalized object differs from receipt')
        native = native_rows(raw.read_bytes(), result['query_month'])
        frame = pl.read_parquet(payload)
        if frame.height != doc['rows'] or frame.height != result['rows']:
            raise ValueError('source receipt row count differs')
        stats = compare_rows(native, frame)
        products.update(frame['product_raw'].to_list())
        totals.update(stats)
        totals['rows'] += len(native)
        totals['empty_months'] += not native
        items.append({'query_month': result['query_month'], 'rows': len(native),
                      'receipt_sha256': result['receipt_sha256'], 'raw_sha256': doc['raw_sha256'],
                      'payload_sha256': doc['payload_sha256'], **stats})
        if (sha256_file(receipt) != result['receipt_sha256'] or sha256_file(raw) != doc['raw_sha256']
                or sha256_file(payload) != doc['payload_sha256']):
            raise ValueError('source changed during independent readback')
    if [x['query_month'] for x in items] != expected_months or sha256_file(collection) != summary_sha:
        raise ValueError('missing, duplicate or unordered months, or changed collection summary')
    return {'contract': 'taifex_stock_option_settlement_native_html_readback_v1',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'collection': str(collection), 'collection_sha256': summary_sha,
            'implementation_sha256': sha256_file(Path(__file__)), 'months': len(items),
            'distinct_observed_product_codes': len(products), 'totals': dict(totals),
            'month_readbacks': items, 'training_ready': False,
            'limitations': ['Both parsers use the same retained exchange pages; no independent exchange feed.',
                            'A successful month page does not prove all historical contracts were captured.',
                            'Final underlying settlement values are not option premiums or executable quotes.',
                            'Historical delivery time, contract generation and account rules are unverified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collection', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve existing source readback')
    result = audit(args.collection)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'months': result['months'], 'totals': result['totals'],
                      'distinct_observed_product_codes': result['distinct_observed_product_codes'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
