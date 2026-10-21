"""Receipt-bound FinMind outright prints for the canonical futures minute tape.

FinMind reports both sides of each outright match. Preserve repeated prints and
source order; divide each reported quantity by two exactly, never infer a VWAP
from OHLC or a current continuous-contract identity.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re

import polars as pl
import numpy as np

from downloader.artifact_io import sha256_file

SOURCE = 'finmind_futures_tick_contract_day_v1'
SCHEMA_VERSION = 1
BULK_SOURCE = 'finmind_whole_market_stock_prints_v1'
BULK_PRICE_POLICY = 'unique_dated_legal_quote_with_identical_float32_encoding_raw_retained_v1'


def pin_bulk_tick_inventory(root: Path, dates: list[date]) -> list[dict]:
    """Pin the collector's active daily objects, never glob historical versions."""
    import sqlite3
    root = Path(root).resolve()
    wanted = {str(d) for d in dates}
    with sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
        tasks = {d: (state, path) for d, state, path in db.execute(
            "SELECT partition,state,receipt_path FROM tasks "
            "WHERE dataset='TaiwanFuturesTick' AND data_id='' ORDER BY partition") if d in wanted}
    if set(tasks) != wanted or any(state != 'complete' for state, _ in tasks.values()):
        raise ValueError('global tick inventory lacks a complete selected date')
    sources = []
    for day in sorted(tasks):
        receipt_path = (root / tasks[day][1]).resolve()
        if not receipt_path.is_relative_to(root):
            raise ValueError('unsafe bulk receipt path')
        payload = receipt_path.read_bytes()
        import hashlib
        receipt = json.loads(payload)
        if (receipt.get('dataset') != 'TaiwanFuturesTick' or receipt.get('data_id') != ''
                or receipt.get('partition') != day or receipt.get('status') != 'complete'):
            raise ValueError('bulk tick receipt identity differs from active inventory')
        path = (root / receipt['parquet_path']).resolve()
        if not path.is_relative_to(root):
            raise ValueError('unsafe bulk tick member path')
        sources.append(dict(date=day, path=str(path), sha256=receipt['sha256'],
            receipt_path=str(receipt_path), receipt_sha256=hashlib.sha256(payload).hexdigest(),
            rows=receipt['rows'], bytes=receipt['parquet_size_bytes'],
            fetched_at_utc=receipt.get('fetched_at_utc'),
            historical_point_in_time=receipt.get('historical_point_in_time'),
            source_order=receipt.get('request', {}).get('source_order')))
    return sources


def decode_bulk_stock_prices(prices: np.ndarray, *, day: date, products: np.ndarray) -> np.ndarray:
    """Recover a unique legal decimal quote from the provider's F32 encoding.

    This is a declared source representation conversion, not arbitrary price
    rounding. Reject any value whose legal candidate does not re-encode to the
    exact retained bits, or for which a neighboring legal tick has those bits.
    Raw objects and hashes are never rewritten. VWAPs are never rounded.
    """
    from stockagent.data.tw_price_rules import (
        taifex_futures_tick_size_numpy, price_on_taifex_futures_tick_grid_numpy,
    )
    values = np.asarray(prices, dtype=np.float64)
    raw = values.astype(np.float32)
    if not np.array_equal(values, raw.astype(np.float64)):
        raise ValueError('bulk price is not the declared Float32 source encoding')
    ticks = taifex_futures_tick_size_numpy(values, np.datetime64(day),
        product_codes=products, asset_classes='stock_future')
    with np.errstate(invalid='ignore', divide='ignore'):
        decoded = np.rint(np.rint(values / ticks) * ticks * 100.) / 100.
    valid = price_on_taifex_futures_tick_grid_numpy(decoded, np.datetime64(day),
        product_codes=products, asset_classes='stock_future')
    if (not valid.all() or not np.array_equal(decoded.astype(np.float32), raw)
            or np.any((decoded + ticks).astype(np.float32) == raw)
            or np.any((decoded - ticks).astype(np.float32) == raw)):
        raise ValueError('bulk price has no unique dated legal Float32 quote')
    return decoded


def stock_print_views(prints: pl.DataFrame, *, day: date, source_sha256: str):
    """One sorted, physical-identity-bound tape feeds minutes and reference events.

    The caller authenticates quantities, physical identity and dated price grid.
    Minute completion bounds are conservative historical availability; exchange
    timestamps and source order are retained without claiming receive times.
    """
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    keys = ['date', 'physical_contract']
    ordered = prints.sort('event_ts', 'source_row_index').with_columns(
        pl.lit(day).alias('date'),
        (pl.col('event_ts').dt.hour().cast(pl.Int32) * 60
         + pl.col('event_ts').dt.minute().cast(pl.Int32) + 1).alias('minute'))
    stats = ordered.group_by(keys).agg(
        pl.col('price').max().alias('tick_high'), pl.col('price').min().alias('tick_low'),
        pl.col('price').last().alias('tick_close'), pl.col('volume').sum().alias('tick_volume'),
        pl.len().alias('tick_rows'))
    bars = ordered.group_by(*keys, 'minute').agg(
        ((pl.col('price') * pl.col('volume')).sum() / pl.col('volume').sum()).alias('vwap'),
        pl.col('price').max().alias('high'), pl.col('price').min().alias('low'),
        pl.col('price').last().alias('close'), pl.col('volume').sum().cast(pl.Float64),
    ).with_columns(pl.lit(source_sha256).alias('source_file_sha256')).select(*MINUTE_SCHEMA).cast(MINUTE_SCHEMA)
    # Only three observations per contract-day are needed for the two selected
    # decisions: actual first session print, actual last prefix print and actual
    # last full-session print. No O(minutes * decisions) reference expansion.
    first = ordered.group_by(keys).first().with_columns(pl.lit(True).alias('is_session_open'))
    last = ordered.group_by(keys).last().with_columns(pl.lit(False).alias('is_session_open'))
    prefix = ordered.filter(pl.col('minute') <= 540).group_by(keys).last().with_columns(
        pl.lit(False).alias('is_session_open'))
    events = pl.concat([first, last, prefix], how='vertical_relaxed').select(
        'physical_contract', 'price', pl.col('date').alias('trade_date'),
        pl.col('event_ts').cast(pl.Datetime('us')).alias('trade_ts'),
        (pl.col('date').cast(pl.Datetime('us')) + pl.duration(minutes=pl.col('minute'))).alias('available_by'),
        pl.lit(source_sha256).alias('source_sha256'), 'is_session_open').unique()
    return bars, stats, events


def normalize_bulk_stock_ticks(frame: pl.DataFrame, *, day: date, expected: pl.DataFrame,
                               source_sha256: str):
    """Columnar daily adapter for all selected delivery months, including zeros."""
    from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes
    keys = ['date', 'physical_contract']
    mapping = expected.select('product', 'contract', 'physical_contract').unique()
    if (expected.select(keys).is_duplicated().any() or mapping.select('product', 'contract').is_duplicated().any()
            or expected.filter((pl.col('date') != day) | (pl.col('asset_class') != 'stock_future')).height):
        raise ValueError('bulk reader needs unique dated stock physical identities')
    required = {'date', 'futures_id', 'contract_date', 'price', 'volume'}
    if not required <= set(frame.columns):
        raise ValueError('incomplete global tick schema')
    # Resolve dated sessions once per selected product, not once per tick.
    sessions = [dict(product=p, _start=a*60, _end=b*60) for p in mapping['product'].unique()
                for a, b in [taifex_futures_day_session_minutes(day, product_code=p, asset_class='stock_future')]]
    raw = frame.with_row_index('source_row_index').filter(
        pl.col('futures_id').is_in(mapping['product'].unique().implode())
        & pl.col('contract_date').str.contains(r'^\d{6}$')).rename(
            {'futures_id': 'product', 'contract_date': 'contract'}).join(mapping,
                on=['product', 'contract'], how='inner', validate='m:1').with_columns(
                    pl.col('date').str.to_datetime(time_unit='us', strict=True).alias('event_ts'))
    if raw.filter(pl.col('event_ts').is_null() | (pl.col('event_ts').dt.date() != day)).height:
        raise ValueError('selected bulk ticks lie outside receipt calendar day')
    raw = raw.join(pl.DataFrame(sessions), on='product', how='left').with_columns(
        (pl.col('event_ts').dt.hour().cast(pl.Int32)*3600 + pl.col('event_ts').dt.minute().cast(pl.Int32)*60
         + pl.col('event_ts').dt.second().cast(pl.Int32)).alias('_second'))
    raw = raw.filter(pl.col('_second').is_between(pl.col('_start'), pl.col('_end')))
    if raw.filter(pl.col('price').is_null() | pl.col('volume').is_null() | (pl.col('volume') <= 0)
                  | (pl.col('volume') % 2 != 0) | (pl.col('volume') != pl.col('volume').floor())).height:
        raise ValueError('bulk outright volume must be positive even double-sided contracts')
    decoded = decode_bulk_stock_prices(raw['price'].to_numpy(), day=day, products=raw['product'].to_numpy())
    prints = raw.with_columns(pl.Series('price', decoded), (pl.col('volume').cast(pl.Int64)//2).alias('volume'))
    bars, stats, events = stock_print_views(prints, day=day, source_sha256=source_sha256)
    coverage = expected.select(*keys, 'product', 'contract', 'high', 'low', 'close', 'outright_volume', 'block_volume').join(
        stats, on=keys, how='left', validate='1:1').with_columns(pl.col('tick_volume').fill_null(0))
    coverage = coverage.with_columns(((pl.col('tick_volume') == pl.col('outright_volume'))
        & ((pl.col('outright_volume') == 0)
           | ((pl.col('tick_high')-pl.col('high')).abs().le(1e-6)
              & (pl.col('tick_low')-pl.col('low')).abs().le(1e-6)
              & (pl.col('tick_close')-pl.col('close')).abs().le(1e-6)))).fill_null(False).alias('verified'),
        pl.lit(source_sha256).alias('source_file_sha256'))
    verified = coverage.filter(pl.col('verified')).select(keys)
    bars = bars.join(verified, on=keys, how='semi')
    events = events.join(verified.rename({'date': 'trade_date'}), on=['trade_date', 'physical_contract'], how='semi')
    return bars, coverage, events


def read_bulk_stock_tick_day(item: dict, expected: pl.DataFrame):
    """Independent SHA/row/stability readback of a pinned active source object."""
    path, receipt_path = Path(item['path']), Path(item['receipt_path'])
    before = path.stat()
    if sha256_file(receipt_path) != item['receipt_sha256'] or sha256_file(path) != item['sha256']:
        raise ValueError('pinned global tick SHA mismatch')
    frame = pl.read_parquet(path)
    if frame.height != item['rows'] or before.st_size != item['bytes']:
        raise ValueError('pinned global tick row/byte count mismatch')
    result = normalize_bulk_stock_ticks(frame, day=date.fromisoformat(item['date']),
        expected=expected, source_sha256=item['sha256'])
    after = path.stat()
    if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or sha256_file(receipt_path) != item['receipt_sha256']):
        raise ValueError('pinned global tick changed during readback')
    return result


def source_paths(root: Path, product: str, day: date) -> tuple[Path, Path]:
    if not re.fullmatch(r'[A-Z0-9]{2,3}', product):
        raise ValueError('invalid futures product identity')
    folder = Path(root) / 'TaiwanFuturesTick' / product / str(day)
    return folder / 'data.parquet', folder / 'receipt.json'


def normalize_finmind_futures_ticks(frame: pl.DataFrame, *, day: date,
                                    product: str, physical_contract: str,
                                    source_sha256: str, asset_class: str) -> tuple[pl.DataFrame, dict]:
    """Adapt one product/calendar-day response without changing execution clocks."""
    from stockagent.data.tw_stock_futures_history import normalize_continuous_ticks
    if not re.fullmatch(re.escape(product) + r':\d{6}(?:W[1-5])?', physical_contract):
        raise ValueError('FinMind physical contract differs from requested product/month')
    required = {'date', 'futures_id', 'contract_date', 'price', 'volume'}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError('missing FinMind futures tick schema or empty response')
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in ('date','futures_id','contract_date')).any()).item():
        raise ValueError('null FinMind futures tick identity')
    raw = frame.with_row_index('source_row_index').with_columns(
        pl.col('date').str.to_datetime(time_unit='ns', strict=True).alias('event_ts'),
        pl.col('contract_date').str.strip_chars(),
    )
    if raw.filter((pl.col('futures_id') != product) | (pl.col('event_ts').dt.date() != day)).height:
        raise ValueError('FinMind response lies outside requested product/calendar day')
    # Other delivery months and spread prints remain preserved in the raw file,
    # but have no capacity in the selected physical outright contract.
    raw = raw.filter(pl.col('contract_date') == physical_contract.split(':')[1])
    if raw.is_empty():
        raise ValueError('no FinMind prints for requested physical contract')
    if raw.select((pl.col('price').is_null() | pl.col('volume').is_null()).any()).item():
        raise ValueError('null FinMind selected outright price/quantity')
    raw = raw.with_columns(pl.col('price').cast(pl.Float64), pl.col('volume').cast(pl.Float64))
    if raw.filter(~pl.col('volume').is_finite() | (pl.col('volume') <= 0)
                  | (pl.col('volume') != pl.col('volume').floor())
                  | (pl.col('volume') % 2 != 0)).height:
        raise ValueError('FinMind outright volume must be positive even double-sided contracts')
    alias = f'FinMind:{physical_contract}'
    canonical = raw.with_columns(
        pl.col('event_ts').cast(pl.Int64).alias('ts'),
        pl.col('price').alias('close'),
        (pl.col('volume') / 2).cast(pl.Int64).alias('volume'),
        pl.lit(day).alias('trading_date'), pl.lit(alias).alias('query_contract'),
    )
    bars, stats = normalize_continuous_ticks(canonical, day=day, alias=alias,
                                             physical=physical_contract, digest=source_sha256,
                                             product=product, asset_class=asset_class)
    return bars, stats


def read_finmind_contract_day(root: Path, row: dict,
                             official_fact: dict | None) -> tuple[pl.DataFrame, dict]:
    """Read and validate one receipt; failures remain explicit source evidence."""
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    from stockagent.data.tw_stock_futures_repair import _check_price_bounds
    day, product, physical = row['date'], row['product'], row['physical_contract']
    data_path, receipt_path = source_paths(Path(root), product, day)
    empty = pl.DataFrame(schema=MINUTE_SCHEMA)
    evidence = dict(date=day, physical_contract=physical, status='missing_receipt',
                    alias=f'FinMind:{physical}', source_file_sha256='', receipt_sha256='',
                    official_volume=row.get('volume'), tick_volume=None, tick_rows=None,
                    detail='', source_row_observed=bool(row.get('source_row_observed')))
    if not receipt_path.is_file():
        return empty, evidence
    try:
        receipt_sha = sha256_file(receipt_path)
        receipt = json.loads(receipt_path.read_text())
        if (receipt.get('source') != SOURCE or receipt.get('schema_version') != SCHEMA_VERSION
                or receipt.get('product') != product or receipt.get('date') != str(day)):
            raise ValueError('FinMind receipt identity mismatch')
        evidence['receipt_sha256'] = receipt_sha
        if receipt.get('status') != 'complete':
            evidence.update(status='source_empty_unresolved' if receipt.get('status') == 'source_empty'
                            else 'incomplete_receipt', detail='FinMind source receipt has no verified prints')
            return empty, evidence
        if official_fact is None or official_fact.get('outright_volume') is None:
            raise ValueError('FinMind tape requires SHA-verified dated official volume/price evidence')
        if (official_fact.get('date') != day or official_fact.get('physical_contract') != physical):
            raise ValueError('FinMind official fact identity mismatch')
        digest = sha256_file(data_path)
        if digest != receipt.get('sha256'):
            raise ValueError('FinMind tick data SHA mismatch')
        raw = pl.read_parquet(data_path)
        if raw.height != receipt.get('rows'):
            raise ValueError('FinMind tick row count mismatch')
        bars, stats = normalize_finmind_futures_ticks(raw, day=day, product=product,
                                                     physical_contract=physical, source_sha256=digest,
                                                     asset_class=row['asset_class'])
        if stats['tick_volume'] > official_fact['outright_volume']:
            raise ValueError('FinMind observed outright volume exceeds official upper bound')
        bars = _check_price_bounds(bars, official_fact, evidence)
        if sha256_file(data_path) != digest or sha256_file(receipt_path) != receipt_sha:
            raise ValueError('FinMind tick inputs changed during read')
        evidence.update(status='minute_verified', source_file_sha256=digest,
                        official_reason=official_fact.get('official_reason'),
                        outright_volume=official_fact['outright_volume'],
                        tick_volume=int(bars['volume'].sum()), tick_rows=stats['tick_rows'],
                        detail='exact_month_FinMind_outright_volume_divided_by_two_observed_prints_only',
                        repair_kind='finmind_ticks', repair_data_path=str(data_path),
                        repair_receipt_path=str(receipt_path))
        return bars, evidence
    except (OSError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        evidence.update(status='invalid_source', detail=str(exc)[:300])
        return empty, evidence
