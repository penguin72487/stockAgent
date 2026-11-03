#!/usr/bin/env python3
"""Independently read back the official daily option contract-date column.

The exchange calls this the originally scheduled last trading day at listing.
A post-session daily report is not a decision-time listing or expiry receipt.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import csv
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_option_product_daily import ALL_PRODUCT_DAILY_SOURCE_CONTRACT, iter_option_product_daily_batches


def audit(day: date, source: Path, source_sha: str) -> dict:
    if source.suffix.lower() != '.csv' or sha256_file(source) != source_sha:
        raise ValueError('one explicitly pinned native CSV is required')
    implementation = Path('stockagent/data/tw_option_product_daily.py').resolve()
    implementation_sha = sha256_file(implementation)
    reader_hash = hashlib.sha256()
    reader_rows = 0
    classes: Counter[str] = Counter()
    by_series: dict[tuple[str, str], set[str]] = defaultdict(set)
    products = set()

    def hash_row(digest, row):
        digest.update(json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n')

    for frame in iter_option_product_daily_batches(source, expected_sha256=source_sha, trading_dates={day}):
        if frame.filter(frame['source_contract'] != ALL_PRODUCT_DAILY_SOURCE_CONTRACT).height:
            raise ValueError('option daily source contract changed')
        columns = ('source_record_ordinal', 'date_raw', 'product_raw', 'series_raw',
                   'strike_raw', 'right_raw', 'contract_expiry_raw')
        for row in frame.select(*columns).iter_rows():
            hash_row(reader_hash, row)
            reader_rows += 1
            _, _, product, series, _, _, expiry = row
            products.add(product.strip())
            if expiry is None:
                classes['column_absent'] += 1
            elif expiry == '':
                classes['empty_field'] += 1
            elif re.fullmatch(r'\d{8}', expiry):
                try:
                    datetime.strptime(expiry, '%Y%m%d')
                except ValueError:
                    classes['invalid_date_text'] += 1
                else:
                    classes['valid_date_text'] += 1
                    by_series[(product.strip(), series.strip())].add(expiry)
            else:
                classes['other_native_text'] += 1
    native_hash = hashlib.sha256()
    native_rows = 0
    with source.open(encoding='cp950', newline='') as stream:
        rows = csv.DictReader(stream)
        names = ('交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '契約到期日')
        if rows.fieldnames is None or any(name not in rows.fieldnames for name in names):
            raise ValueError('official daily report does not contain the contract-date column')
        for ordinal, row in enumerate(rows, start=1):
            if row['交易日期'].strip() == day.strftime('%Y/%m/%d'):
                hash_row(native_hash, (ordinal, *(row[name] for name in names)))
                native_rows += 1
    if (reader_rows == 0 or reader_rows != native_rows or reader_hash.digest() != native_hash.digest()
            or sha256_file(source) != source_sha or sha256_file(implementation) != implementation_sha):
        raise ValueError('native expiry source values, order or source identity differ')
    multiple = [{'product': key[0], 'series': key[1], 'native_date_texts': sorted(values)}
                for key, values in sorted(by_series.items()) if len(values) > 1]
    return {'contract': 'taifex_option_daily_contract_date_native_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'date': day.isoformat(),
            'source_path': str(source.resolve()), 'source_sha256': source_sha,
            'reader_contract': ALL_PRODUCT_DAILY_SOURCE_CONTRACT, 'reader_sha256': implementation_sha,
            'script_sha256': sha256_file(Path(__file__)), 'source_rows': reader_rows,
            'native_csv_rows': native_rows, 'native_and_reader_rows_sha256': reader_hash.hexdigest(),
            'expiry_text_states': dict(classes), 'observed_product_codes': sorted(products),
            'product_series_with_date_text': len(by_series), 'product_series_with_multiple_date_texts': multiple,
            'training_ready': False,
            'limitations': ['Native field is the originally scheduled last trading day; changes require separate evidence.',
                            'A daily report acquired later does not prove decision-time knowledge or listing.',
                            'No generation identity, executable quote, position, account or training admission is implied.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', required=True, type=date.fromisoformat)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve earlier contract-date audit')
    result = audit(args.date, args.source.resolve(), args.source_sha256)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'rows': result['source_rows'],
                      'expiry_text_states': result['expiry_text_states'],
                      'multi_dates': len(result['product_series_with_multiple_date_texts'])}, ensure_ascii=False))


if __name__ == '__main__':
    main()
