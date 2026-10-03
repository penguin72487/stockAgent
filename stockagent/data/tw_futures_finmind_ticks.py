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

from downloader.artifact_io import sha256_file

SOURCE = 'finmind_futures_tick_contract_day_v1'
SCHEMA_VERSION = 1


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
