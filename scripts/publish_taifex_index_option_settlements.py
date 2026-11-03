#!/usr/bin/env python3
"""Normalize a retained official multi-product index-option settlement page.

This publishes a research source snapshot, not a live historical eligibility
or settlement-accounting contract. Blank cells remain explicit source values.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import unicodedata

from bs4 import BeautifulSoup
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.download_taifex_futures_final_settlement_history import parse_index_futures_final_settlement_html


PRODUCTS = {'2': 'TXO', '8': 'TEO', '9': 'TFO', '11': 'MSO', '14': 'XIO', '16': 'GTO'}
SCHEMA = {'source_row': pl.Int64, 'settlement_date': pl.Date, 'series_raw': pl.String,
          'product_raw': pl.String, 'source_column_raw': pl.String,
          'settlement_price_raw': pl.String, 'positive_numeric_price': pl.Boolean}


def parse(body: bytes, start: date, end: date) -> pl.DataFrame:
    soup = BeautifulSoup(body, 'html.parser')
    for name, expected in (('start_year', f'{start.year:04d}'), ('start_month', f'{start.month:02d}'),
                           ('end_year', f'{end.year:04d}'), ('end_month', f'{end.month:02d}')):
        select = soup.find('select', attrs={'name': name})
        selected = select.find_all('option', selected=True) if select else []
        if len(selected) != 1 or selected[0].get('value', '').strip() != expected:
            raise ValueError('official response did not echo requested date range')
    checked = {item.get('value') for item in soup.find_all('input', attrs={'name': 'commodityIds'})
               if item.has_attr('checked')}
    if checked != set(PRODUCTS):
        raise ValueError('official response did not echo the six requested products')
    matches = []
    for table in soup.find_all('table'):
        headers = [cell.get_text(' ', strip=True) for cell in table.find_all('th')]
        if len(headers) >= 2 and '最後' in headers[0] and '契約' in headers[1]:
            matches.append((table, headers))
    if len(matches) != 1:
        raise ValueError('expected exactly one index-option final-settlement table')
    table, headers = matches[0]
    codes = []
    for header in headers[2:]:
        found = re.findall(r'\(([A-Z][A-Z0-9]{1,5})\)', unicodedata.normalize('NFKC', header))
        if len(found) != 1:
            raise ValueError('unknown product column in official result')
        codes.append(found[0])
    if len(codes) != len(set(codes)) or set(codes) != set(PRODUCTS.values()):
        raise ValueError('official product columns differ from requested products')
    body_tag = table.find('tbody')
    if body_tag is None:
        raise ValueError('official result table has no body')
    rows = []
    for ordinal, row in enumerate(body_tag.find_all('tr'), 1):
        cells = [cell.get_text(' ', strip=True) for cell in row.find_all('td')]
        if len(cells) != len(headers):
            raise ValueError(f'unexpected official result row {ordinal}')
        try:
            settlement_day = datetime.strptime(cells[0], '%Y/%m/%d').date()
        except ValueError as error:
            raise ValueError(f'malformed settlement date at row {ordinal}') from error
        series = cells[1]
        if not start <= settlement_day <= end or not re.fullmatch(r'\d{6}(?:[WF][1-5])?', series):
            raise ValueError(f'unsupported date or series at row {ordinal}')
        for product, header, value in zip(codes, headers[2:], cells[2:], strict=True):
            try:
                number = Decimal(value.replace(',', ''))
                positive = number.is_finite() and number > 0
            except InvalidOperation:
                positive = False
            rows.append({'source_row': ordinal, 'settlement_date': settlement_day, 'series_raw': series,
                         'product_raw': product, 'source_column_raw': header, 'settlement_price_raw': value,
                         'positive_numeric_price': positive})
    return pl.from_dicts(rows, schema=SCHEMA, strict=True)


def publish(raw: Path, output: Path, start: date, end: date, source_url: str) -> dict:
    repo = Path(__file__).resolve().parents[1]
    raw, output = raw.resolve(), output.resolve()
    if (not raw.is_file() or raw.is_symlink() or not output.is_relative_to(repo / 'data_tw_index_options_daily' / 'research')
            or (output / 'manifest.json').exists() or (output / 'index_option_final_settlements.parquet').exists()):
        raise ValueError('a retained raw source and new research publication are required')
    capacity_check(output.parent)
    implementation = [Path(__file__).resolve(), repo / 'scripts/download_taifex_futures_final_settlement_history.py']
    pins = {str(p): sha256_file(p) for p in implementation}
    raw_sha = sha256_file(raw)
    data = parse(raw.read_bytes(), start, end)
    # Existing shared wide-table parser supplies an independent numeric check
    # for monthly and Wednesday series; it does not support Friday series.
    other = parse_index_futures_final_settlement_html(raw.read_bytes(), start_date=start, end_date=end,
        source_file=str(raw), source_sha256=raw_sha, source_url=source_url,
        source_kind='official_index_options_html', allowed_products=frozenset(PRODUCTS.values()))
    observed = (data.filter(pl.col('positive_numeric_price') & ~pl.col('series_raw').str.contains('F'))
                .select('settlement_date', pl.col('product_raw').alias('product'),
                        pl.col('series_raw').alias('contract'),
                        pl.col('settlement_price_raw').str.replace_all(',', '').cast(pl.Float64).alias('price'))
                .sort('settlement_date', 'product', 'contract', 'price'))
    check = (other.select('settlement_date', 'product', 'contract',
                          pl.col('final_settlement_price').alias('price'))
             .sort('settlement_date', 'product', 'contract', 'price'))
    if not observed.equals(check):
        raise ValueError('independent shared parser differs for monthly or Wednesday contracts')
    if (sha256_file(raw) != raw_sha or any(sha256_file(Path(p)) != sha for p, sha in pins.items())):
        raise ValueError('raw source or parser changed during publication')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=True)
    payload = output / 'index_option_final_settlements.parquet'
    atomic_write_parquet(payload, data, compression='zstd', durable=True)
    if not data.equals(pl.read_parquet(payload)):
        raise ValueError('independent index settlement payload readback differs')
    duplicates = (data.filter(pl.col('positive_numeric_price'))
                  .group_by('product_raw', 'series_raw').agg(pl.len().alias('rows'))
                  .filter(pl.col('rows') > 1).height)
    receipt = {'contract': 'taifex_index_option_final_settlement_source_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'requested_start': str(start), 'requested_end': str(end), 'url': source_url,
               'raw_file_mtime_utc': datetime.fromtimestamp(raw.stat().st_mtime, timezone.utc).isoformat(),
               'raw_path': str(raw), 'raw_sha256': raw_sha, 'data_path': str(payload),
               'data_sha256': sha256_file(payload), 'rows': data.height,
               'positive_numeric_rows': data.filter(pl.col('positive_numeric_price')).height,
               'independent_shared_parser_rows_checked': check.height,
               'product_series_with_multiple_positive_rows': duplicates,
               'implementation_sha256': pins, 'training_ready': False,
               'limitations': ['Snapshot acquisition time is not historical publication time.',
                   'Absent or nonnumeric cells are retained; they are not settlement zeroes.',
                   'Repeated product/series keys may require generation and corporate-action evidence.',
                   'An underlying settlement value is not a premium, quote, fill, or training admission.',
                   'Currencies and actual execution/account rules must be checked by product and date.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--source-url', required=True)
    args = parser.parse_args()
    result = publish(args.raw, args.output, args.start, args.end, args.source_url)
    print(json.dumps({key: result[key] for key in ('rows', 'positive_numeric_rows',
          'independent_shared_parser_rows_checked', 'product_series_with_multiple_positive_rows')},
          ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
