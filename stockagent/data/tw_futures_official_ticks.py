"""Pinned TAIFEX recent trade archives for the dated futures clock builder.

The canonical collector retains each all-product exchange ZIP even though its
published normalized partitions cover only TX/TXO.  This adapter authenticates
that original receipt and applies the same all-TWD full-book certification as
the FinMind adapter.  It does not establish historical provider delivery time.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re
import zipfile

import polars as pl

from downloader.artifact_io import sha256_file

SOURCE = 'taifex_recent_original_all_product_futures_tick_v1'
OPTION_SOURCE = 'taifex_recent_original_all_product_option_tick_v1'
DATASET = 'taifex_recent_tx_txo_trade_by_trade'


def _pin_official_tick_inventory(manifest_path: Path, dates: list[date], *, kind: str) -> list[dict]:
    if kind not in {'futures', 'options'}:
        raise ValueError('unsupported TAIFEX recent archive kind')
    manifest = Path(manifest_path).resolve()
    before = sha256_file(manifest)
    document = json.loads(manifest.read_bytes())
    if document.get('dataset') != DATASET or document.get('status') != 'complete':
        raise ValueError('invalid canonical TAIFEX recent archive receipt')
    wanted = {str(day) for day in dates}
    objects = {}
    for record in document.get('raw_downloads', []):
        if record.get('kind') != kind or record.get('trading_date') not in wanted:
            continue
        day = date.fromisoformat(record['trading_date'])
        prefix = 'Daily' if kind == 'futures' else 'OptionsDaily'
        directory = 'DailydownloadCSV' if kind == 'futures' else 'OptionsDailydownloadCSV'
        source_kind = SOURCE if kind == 'futures' else OPTION_SOURCE
        name = f"{prefix}_{day.strftime('%Y_%m_%d')}.zip"
        path = Path(record['path'])
        url = 'https://www.taifex.com.tw/file/taifex/Dailydownload/' + directory + '/' + name
        if (str(day) in objects or path.name != name or not path.is_absolute()
                or path.is_symlink() or record.get('url') != url
                or not isinstance(record.get('sha256'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', record['sha256'])
                or not isinstance(record.get('bytes'), int) or record['bytes'] <= 0):
            raise ValueError('TAIFEX archive identity, URL, or receipt differs')
        objects[str(day)] = dict(date=str(day), source_kind=source_kind, path=str(path.resolve()),
            sha256=record['sha256'], bytes=record['bytes'], receipt_path=str(manifest),
            receipt_sha256=before, url=url, historical_point_in_time=False,
            price_representation='native_decimal')
    if set(objects) != wanted:
        raise ValueError('TAIFEX recent archive receipt lacks a selected date')
    if sha256_file(manifest) != before:
        raise ValueError('TAIFEX archive receipt changed during pinning')
    return [objects[day] for day in sorted(objects)]


def pin_official_tick_inventory(manifest_path: Path, dates: list[date]) -> list[dict]:
    """Pin retained all-product futures originals without changing the existing ABI."""
    return _pin_official_tick_inventory(manifest_path, dates, kind='futures')


def pin_official_option_tick_inventory(manifest_path: Path, dates: list[date]) -> list[dict]:
    """Pin retained all-product option originals using the same collector receipt."""
    return _pin_official_tick_inventory(manifest_path, dates, kind='options')


def read_official_tick_day(item: dict, expected: pl.DataFrame, *, asset_scope='all_twd'):
    if item.get('source_kind') != SOURCE or asset_scope != 'all_twd':
        raise ValueError('TAIFEX recent fallback requires dated all-TWD rules')
    from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
    from stockagent.data.tw_futures_finmind_ticks import normalize_bulk_stock_ticks

    archive, receipt = Path(item['path']), Path(item['receipt_path'])
    before = archive.stat()
    if (before.st_size != item['bytes'] or sha256_file(archive) != item['sha256']
            or sha256_file(receipt) != item['receipt_sha256']):
        raise ValueError('TAIFEX recent original or receipt SHA mismatch')
    day = date.fromisoformat(item['date'])
    products = tuple(sorted(expected['product'].unique().to_list()))
    if not products or expected.filter(pl.col('date') != day).height:
        raise ValueError('TAIFEX archive expected contract-day differs')
    with zipfile.ZipFile(archive) as zipped:
        members = zipped.infolist()
        if (len(members) != 1 or members[0].filename != f"Daily_{day.strftime('%Y_%m_%d')}.csv"
                or members[0].is_dir() or members[0].flag_bits & 1
                or members[0].file_size > 128 * 1024 * 1024):
            raise ValueError('unexpected TAIFEX original archive member')
    # Admit raw timestamps through the parser without guessing product hours;
    # the normalizer applies the actual bound session per physical contract.
    try:
        parsed = _parse_zip(archive, kind='futures', trading_date=day,
            source_sha256=item['sha256'], futures_products=products,
            futures_outright_contracts_only=True,
            futures_day_sessions={product: (0, 1440) for product in products})
    except ValueError as exc:
        # The canonical parser scans the entire archive before this exact
        # no-target error. Keep a genuine empty print set: only independently
        # evidenced zero official volume can then be marked verified.
        if str(exc) != f'no target rows found in {archive}':
            raise
        raw = pl.DataFrame(schema={'date': pl.String, 'futures_id': pl.String,
            'contract_date': pl.String, 'price': pl.Float64, 'volume': pl.Int64})
    else:
        raw = parsed.filter(pl.col('event_date') == day).select(
            pl.col('event_ts').dt.strftime('%Y-%m-%d %H:%M:%S').alias('date'),
            pl.col('product').alias('futures_id'),
            pl.col('delivery_month_week').alias('contract_date'),
            'price', pl.col('reported_b_plus_s_quantity').alias('volume'))
    bars, coverage, events = normalize_bulk_stock_ticks(raw, day=day, expected=expected,
        source_sha256=item['sha256'], asset_scope=asset_scope, price_representation='native_decimal')
    # First preserve every already-certified common-hours book. If it is
    # incomplete, the immutable all-product original permits a wider session
    # candidate, still admitted only on a complete independent day-book match.
    # This prevents a later non-day print from invalidating an already complete
    # shorter session, and never adds a print merely because it is observed.
    pending_keys = coverage.filter(~pl.col('verified')).select('date', 'physical_contract')
    if pending_keys.height:
        pending = expected.join(pending_keys, on=['date', 'physical_contract'], how='semi')
        extra_bars, extra_coverage, extra_events = normalize_bulk_stock_ticks(
            raw, day=day, expected=pending, source_sha256=item['sha256'],
            asset_scope=asset_scope, price_representation='native_decimal',
            unknown_final_mode='union_for_full_book_verification')
        coverage = pl.concat([coverage.join(pending_keys, on=['date', 'physical_contract'], how='anti'),
                              extra_coverage], how='vertical')
        bars = pl.concat([bars, extra_bars], how='vertical')
        events = pl.concat([events, extra_events], how='vertical')
    after = archive.stat()
    if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or sha256_file(archive) != item['sha256']
            or sha256_file(receipt) != item['receipt_sha256']):
        raise ValueError('TAIFEX original or receipt changed during readback')
    return (bars.sort('date', 'physical_contract', 'minute'),
            coverage.sort('date', 'physical_contract'),
            events.sort('trade_date', 'physical_contract', 'trade_ts', 'is_session_open'))
