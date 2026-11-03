#!/usr/bin/env python3
"""Archive official stock/ETF option final settlements by contract month.

The native HTML remains authoritative. This collector uses the existing
TAIFEX transport and shared provider limiter; it does not create a price,
fill, historical publication timestamp, or training eligibility.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import json
from pathlib import Path
import re
from urllib.parse import urlencode

from bs4 import BeautifulSoup
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.common import SharedRateLimiter, resolve_request_interval
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport
from scripts.build_taifex_option_finmind_reference_window import CapacityNotAdmitted, capacity_check
from scripts.download_taifex_futures_final_settlement_history import _download_html, _months


PAGE = 'https://www.taifex.com.tw/cht/5/sSOFSP'
HEADERS = ['商品名稱', '商品代號', '標的證券代號', '到期日', '契約月份', '最後結算價']
SCHEMA = {'source_row': pl.Int64, 'product_name_raw': pl.String, 'product_raw': pl.String,
          'underlying_raw': pl.String, 'settlement_date': pl.Date, 'series_raw': pl.String,
          'settlement_price_raw': pl.String, 'contract_value_raw': pl.String,
          'supported_key': pl.Boolean, 'positive_numeric_price': pl.Boolean}


def url_for(year: int, month: int) -> str:
    return PAGE + '?' + urlencode({'down_type': '1', 'queryYear': str(year), 'queryMonth': f'{month:02d}'})


def parse(body: bytes, year: int, month: int) -> pl.DataFrame:
    soup = BeautifulSoup(body, 'html.parser')
    for name, expected in (('queryYear', str(year)), ('queryMonth', f'{month:02d}')):
        select = soup.find('select', attrs={'name': name})
        selected = select.find_all('option', selected=True) if select else []
        if len(selected) != 1 or selected[0].get('value', '').strip() != expected:
            raise ValueError('official response did not echo requested contract month')
    tables = []
    for table in soup.find_all('table'):
        headers = [x.get_text(' ', strip=True) for x in table.find_all('th')]
        if headers[:len(HEADERS)] == HEADERS:
            tables.append((table, headers))
    if len(tables) != 1:
        raise ValueError('expected exactly one stock option settlement results table')
    table, headers = tables[0]
    if len(headers) not in {6, 7} or (len(headers) == 7 and headers[6] != '約定標的物價值'):
        raise ValueError('unexpected official settlement columns')
    body_tag = table.find('tbody')
    if body_tag is None:
        raise ValueError('official result table has no body')
    records = []
    for index, row in enumerate(body_tag.find_all('tr'), 1):
        cells = [cell.get_text(' ', strip=True) for cell in row.find_all('td')]
        if len(cells) == 1 and '查無資料' in cells[0]:
            if len(body_tag.find_all('tr')) != 1:
                raise ValueError('empty marker mixed with settlement records')
            continue
        if len(cells) != len(headers):
            raise ValueError(f'unexpected official result row {index}')
        name, product, underlying, raw_date, series, price = cells[:6]
        try:
            settlement_day = datetime.strptime(raw_date, '%Y/%m/%d').date()
        except ValueError as error:
            raise ValueError(f'malformed official settlement date at row {index}') from error
        try:
            value = Decimal(price.replace(',', ''))
            positive_price = value.is_finite() and value > 0
        except InvalidOperation:
            positive_price = False
        records.append({'source_row': index, 'product_name_raw': name, 'product_raw': product,
            'underlying_raw': underlying, 'settlement_date': settlement_day, 'series_raw': series,
            'settlement_price_raw': price, 'contract_value_raw': cells[6] if len(cells) == 7 else None,
            'supported_key': bool(re.fullmatch(r'[A-Z][A-Z0-9]{1,5}', product)
                                  and re.fullmatch(r'\d{6}(?:[WF][1-5])?', series)),
            'positive_numeric_price': positive_price})
    return pl.from_dicts(records, schema=SCHEMA, strict=True)


def implementation_compatible(previous: dict, current: dict, snapshot: Path) -> bool:
    """Accept the measured smoke producer only with its preserved exact bytes."""
    script = str(Path(__file__).resolve())
    if previous == current:
        return True
    return (set(previous) == set(current) and snapshot.is_file()
            and sha256_file(snapshot) == previous.get(script)
            and all(previous[path] == digest for path, digest in current.items() if path != script))


def run(start: date, end: date, output: Path, summary: Path) -> dict:
    output, summary = output.resolve(), summary.resolve()
    repo = Path(__file__).resolve().parents[1]
    if (end < start or not output.is_relative_to(repo / 'data_tw_index_options_daily' / 'research')
            or not summary.is_relative_to(repo / 'artifacts' / 'markets')
            or summary.exists() or summary.is_symlink()):
        raise ValueError('ordered dates, research data output and a new market summary are required')
    implementation = [Path(__file__).resolve(),
                      repo / 'scripts/download_taifex_futures_final_settlement_history.py',
                      repo / 'downloader/http_transport.py']
    pins = {str(p): sha256_file(p) for p in implementation}
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=True)
    transport = ResilientHttpTransport(
        HttpRequestPolicy(provider='taifex_public', timeout_seconds=120, max_retries=2, retry_base_seconds=0.5),
        limiter=SharedRateLimiter(resolve_request_interval('taifex_public', 0.05), name='taifex_public'))
    results = []
    deferred = False
    smoke_snapshot = repo / 'artifacts/markets/tw_derivative_scopes_options_20261028_v1/stock_option_settlement_smoke_implementation_v1.py'
    with (output / '.writer.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for year, month in _months(start, end):
            label = f'{year:04d}-{month:02d}'
            raw = output / 'raw' / f'{label}.html'
            payload = output / 'partitions' / f'{label}.parquet'
            receipt = output / 'receipts' / f'{label}.json'
            try:
                capacity_check(output.parent)
                if any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
                    raise ValueError('collector implementation changed during collection')
                if receipt.exists():
                    item = json.loads(receipt.read_bytes())
                    if (item.get('contract') != 'taifex_stock_option_final_settlement_month_v1'
                            or item.get('query_month') != label or item.get('url') != url_for(year, month)
                            or not implementation_compatible(item.get('implementation_sha256', {}), pins, smoke_snapshot)
                            or sha256_file(raw) != item['raw_sha256']
                            or sha256_file(payload) != item['payload_sha256']):
                        raise ValueError('existing month receipt or source changed')
                    normalized = parse(raw.read_bytes(), year, month)
                    if not normalized.equals(pl.read_parquet(payload)):
                        raise ValueError('independent normalized readback differs')
                    state = 'reused_verified'
                else:
                    if payload.exists():
                        raise ValueError('unreceipted normalized output requires inspection')
                    raw = _download_html(url_for(year, month), raw, refresh=False, transport=transport)
                    digest = sha256_file(raw)
                    normalized = parse(raw.read_bytes(), year, month)
                    capacity_check(output.parent)
                    atomic_write_parquet(payload, normalized, compression='zstd', durable=True)
                    if sha256_file(raw) != digest or not normalized.equals(pl.read_parquet(payload)):
                        raise ValueError('source or normalized readback differs')
                    item = {'contract': 'taifex_stock_option_final_settlement_month_v1', 'query_month': label,
                            'url': url_for(year, month), 'processed_at_utc': datetime.now(timezone.utc).isoformat(),
                            'raw_file_mtime_utc': datetime.fromtimestamp(raw.stat().st_mtime, timezone.utc).isoformat(),
                            'raw_path': str(raw), 'raw_sha256': digest, 'payload_path': str(payload),
                            'payload_sha256': sha256_file(payload), 'rows': normalized.height,
                            'unsupported_key_rows': normalized.filter(~pl.col('supported_key')).height,
                            'nonpositive_or_invalid_price_rows': normalized.filter(~pl.col('positive_numeric_price')).height,
                            'implementation_sha256': pins, 'training_ready': False}
                    atomic_write_json(receipt, item)
                    state = 'built_verified'
                result = {'query_month': label, 'state': state, 'rows': item['rows'],
                          'receipt': str(receipt), 'receipt_sha256': sha256_file(receipt)}
            except CapacityNotAdmitted as error:
                result = {'query_month': label, 'state': 'capacity_deferred', 'capacity': error.status}
                deferred = True
            except Exception as error:
                result = {'query_month': label, 'state': 'failed', 'error_type': type(error).__name__,
                          'error': str(error)}
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if deferred:
                break
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    summary_doc = {'contract': 'taifex_stock_option_final_settlement_collection_v1',
                   'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                   'start': str(start), 'end': str(end), 'planned_months': len(list(_months(start, end))),
                   'attempted_months': len(results), 'capacity_deferred': deferred, 'results': results,
                   'implementation_sha256': pins, 'training_ready': False,
                   'limitations': ['Query scope is contract month; adjusted contracts can settle early.',
                       'Retained records are retrospective snapshots, not historical publication-time proof.',
                       'Empty responses and complete month queries do not prove exchange-wide completeness.',
                       'Contracts with reused codes or adjustments require generation and entitlement evidence.',
                       'Underlying final settlement is not an option premium, quote or execution price.']}
    atomic_write_json(summary, summary_doc)
    return summary_doc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    args = parser.parse_args()
    result = run(args.start, args.end, args.output, args.summary)
    if result['capacity_deferred']:
        raise SystemExit(75)
    if any(row['state'] == 'failed' for row in result['results']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
