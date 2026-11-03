#!/usr/bin/env python3
"""Publish retained TAIFEX FX-option settlement source evidence for research.

This is not an option premium, executable quote, or an FX-account contract.
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


PRODUCTS = {'22': 'RHO', '23': 'RTO'}
SCHEMA = {'source_row': pl.Int64, 'settlement_date': pl.Date, 'series_raw': pl.String,
          'product_raw': pl.String, 'settlement_price_raw': pl.String,
          'positive_numeric_price': pl.Boolean}


def parse_raw_cells(body: bytes, start: date, end: date) -> pl.DataFrame:
    """Keep exact cell strings, while refusing a mismatched source response."""
    soup = BeautifulSoup(body, 'html.parser')
    for name, expected in (('start_year', f'{start.year:04d}'), ('start_month', f'{start.month:02d}'),
                           ('end_year', f'{end.year:04d}'), ('end_month', f'{end.month:02d}')):
        select = soup.find('select', attrs={'name': name})
        choices = select.find_all('option', selected=True) if select else []
        if len(choices) != 1 or choices[0].get('value', '').strip() != expected:
            raise ValueError('official response did not echo requested date range')
    selected = {item.get('value') for item in soup.find_all('input', attrs={'name': 'commodityIds'})
                if item.has_attr('checked')}
    if selected != set(PRODUCTS):
        raise ValueError('official response did not echo FX option selection')
    matches = []
    for table in soup.find_all('table'):
        headers = [cell.get_text(' ', strip=True) for cell in table.find_all('th')]
        if len(headers) >= 2 and '最後' in headers[0] and '契約' in headers[1]:
            matches.append((table, headers))
    if len(matches) != 1:
        raise ValueError('expected exactly one FX-option settlement table')
    table, headers = matches[0]
    codes = []
    for header in headers[2:]:
        found = re.findall(r'\(([A-Z][A-Z0-9]{1,5})\)', unicodedata.normalize('NFKC', header))
        if len(found) != 1:
            raise ValueError('unknown FX option product column')
        codes.append(found[0])
    if len(codes) != len(set(codes)) or set(codes) != set(PRODUCTS.values()):
        raise ValueError('FX option columns differ from requested products')
    body_tag = table.find('tbody')
    if body_tag is None:
        raise ValueError('FX option settlement table has no body')
    rows = []
    for ordinal, row in enumerate(body_tag.find_all('tr'), 1):
        # Some official tables have an unclosed row; avoid gathering nested cells.
        cells = [cell.get_text(' ', strip=True) for cell in row.find_all('td', recursive=False)]
        if len(cells) != len(headers):
            raise ValueError(f'unexpected FX option result row {ordinal}')
        try:
            day = datetime.strptime(cells[0], '%Y/%m/%d').date()
        except ValueError as error:
            raise ValueError('malformed FX option settlement date') from error
        if not start <= day <= end or not re.fullmatch(r'\d{6}(?:[WF][1-5])?', cells[1]):
            raise ValueError('unsupported FX option settlement date or series')
        for product, value in zip(codes, cells[2:], strict=True):
            try:
                number = Decimal(value.replace(',', ''))
                positive = number.is_finite() and number > 0
            except InvalidOperation:
                positive = False
            rows.append({'source_row': ordinal, 'settlement_date': day, 'series_raw': cells[1],
                         'product_raw': product, 'settlement_price_raw': value,
                         'positive_numeric_price': positive})
    return pl.from_dicts(rows, schema=SCHEMA, strict=True)


def publish(raw: Path, output: Path, start: date, end: date, url: str) -> dict:
    repo = Path(__file__).resolve().parents[1]
    raw, output = raw.resolve(), output.resolve()
    if (not raw.is_file() or raw.is_symlink() or not output.is_relative_to(repo / 'data_tw_index_options_daily' / 'research')
            or (output / 'manifest.json').exists() or (output / 'fx_option_final_settlements.parquet').exists()):
        raise ValueError('a retained raw source and new research publication are required')
    capacity_check(output.parent)
    implementation = [Path(__file__).resolve(), repo / 'scripts/download_taifex_futures_final_settlement_history.py']
    pins = {str(path): sha256_file(path) for path in implementation}
    raw_sha = sha256_file(raw)
    body = raw.read_bytes()
    frame = parse_raw_cells(body, start, end)
    # Reuse the canonical wide-table parser; compare against the original cells.
    other = parse_index_futures_final_settlement_html(body, start_date=start, end_date=end,
        source_file=str(raw), source_sha256=raw_sha, source_url=url,
        source_kind='official_fx_options_html', allowed_products=frozenset(PRODUCTS.values()))
    cells = (frame.filter(pl.col('positive_numeric_price') & ~pl.col('series_raw').str.contains('F'))
        .select('settlement_date', pl.col('product_raw').alias('product'), pl.col('series_raw').alias('contract'),
                pl.col('settlement_price_raw').str.replace_all(',', '').cast(pl.Float64).alias('price'))
        .sort('settlement_date', 'product', 'contract', 'price'))
    checked = (other.select('settlement_date', 'product', 'contract', pl.col('final_settlement_price').alias('price'))
               .sort('settlement_date', 'product', 'contract', 'price'))
    if not cells.equals(checked):
        raise ValueError('canonical wide-table parser differs from original source cells')
    if sha256_file(raw) != raw_sha or any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
        raise ValueError('source or implementation changed')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=True)
    data = output / 'fx_option_final_settlements.parquet'
    atomic_write_parquet(data, frame, compression='zstd', durable=True)
    if not frame.equals(pl.read_parquet(data)):
        raise ValueError('FX option settlement readback differs')
    receipt = {'contract': 'taifex_fx_option_final_settlement_source_v1',
        'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'requested_start': str(start),
        'requested_end': str(end), 'url': url, 'raw_path': str(raw), 'raw_sha256': raw_sha,
        'raw_file_mtime_utc': datetime.fromtimestamp(raw.stat().st_mtime, timezone.utc).isoformat(),
        'data_path': str(data), 'data_sha256': sha256_file(data), 'rows': frame.height,
        'positive_numeric_rows': frame.filter(pl.col('positive_numeric_price')).height,
        'canonical_parser_rows_checked': checked.height, 'implementation_sha256': pins,
        'currency_scope': 'foreign_currency_separate', 'training_ready': False,
        'limitations': ['A retrospective snapshot does not establish historical notice availability.',
            'Products sharing a contract month can have different actual settlement dates.',
            'Blank cells are not zeroes; settlement is not a premium, quote, fill, or training admission.',
            'Separate currency accounting, contractual terms, data-use rights, and history remain to be verified.']}
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
          'canonical_parser_rows_checked')}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
