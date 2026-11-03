"""Convert retained FinMind option trades into source-only reference inputs.

Original Float32 prices and strikes remain Float32-quality observations even
when represented as strings for the shared decimal-aware reducer. Unknown
products, invalid rows and other partitions are counted rather than guessed.
This adapter does not establish sessions, listings, executions or PIT access.
"""
from __future__ import annotations

from datetime import date
import re

import polars as pl

from downloader.finmind_option_contract_keys import vendor_option_series


CONTRACT = 'finmind_option_reference_source_adapter_v1'
COLUMNS = ('event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw', 'price_raw', 'source_sha256')


def candidate_rows(frame: pl.DataFrame, *, day: date, source_sha256: str,
                   candidate_products: set[str]) -> tuple[pl.DataFrame, dict]:
    """Keep valid natural-day source rows with explicit source identity."""
    required = {'date', 'option_id', 'contract_date', 'ExercisePrice', 'PutCall', 'price', 'volume'}
    if required - set(frame.columns) or not re.fullmatch(r'[0-9a-f]{64}', source_sha256):
        raise ValueError('FinMind option source identity or schema missing')
    if (frame.schema['ExercisePrice'] != pl.Float32 or frame.schema['price'] != pl.Float32
            or frame.schema['date'] != pl.String or frame.schema['option_id'] != pl.String
            or frame.schema['contract_date'] != pl.String or frame.schema['PutCall'] != pl.String
            or not frame.schema['volume'].is_integer()):
        raise ValueError('FinMind stored precision or source schema differs')
    if not candidate_products or any(not isinstance(p, str) or not p or p != p.upper()
                                     for p in candidate_products):
        raise ValueError('an explicit candidate product scope is required')
    rows = (frame.select(
        pl.col('date').str.strptime(pl.Datetime('us'), '%Y-%m-%d %H:%M:%S', strict=False)
          .dt.replace_time_zone('Asia/Taipei').alias('event_ts'),
        pl.col('option_id').str.strip_chars().str.to_uppercase().alias('product_raw'),
        vendor_option_series(pl.col('contract_date')).alias('series_raw'),
        pl.col('ExercisePrice'),
        pl.col('PutCall').str.strip_chars().str.to_uppercase().alias('right_raw'),
        pl.col('price'), pl.col('volume')))
    valid_time = (pl.col('event_ts').is_not_null() & (pl.col('event_ts').dt.date() == day)).fill_null(False)
    valid_contract = (pl.col('series_raw').str.contains(r'^\d{4}(0[1-9]|1[0-2])([WF][1-5])?$')
                      & pl.col('right_raw').is_in(['C', 'P'])).fill_null(False)
    valid_numbers = (pl.col('price').is_finite() & (pl.col('price') > 0)
                     & pl.col('ExercisePrice').is_finite() & (pl.col('ExercisePrice') > 0)
                     & (pl.col('volume') > 0)).fill_null(False)
    known_product = pl.col('product_raw').is_in(sorted(candidate_products)).fill_null(False)
    flags = rows.select(valid_time.alias('valid_time'), valid_contract.alias('valid_contract'),
                        valid_numbers.alias('valid_numbers'), known_product.alias('known_product'))
    valid = flags.select(pl.all_horizontal(pl.all()).alias('valid')).to_series()
    selected = rows.filter(valid).select(
        'event_ts', 'product_raw', 'series_raw',
        pl.col('ExercisePrice').cast(pl.String).alias('strike_raw'), 'right_raw',
        pl.col('price').cast(pl.String).alias('price_raw'),
        pl.lit(source_sha256).alias('source_sha256'))
    unknown = (rows.filter(~flags['known_product'])['product_raw'].drop_nulls().unique().sort().to_list())
    return selected, {'source_rows': frame.height, 'candidate_rows': selected.height,
                      'excluded_rows': frame.height - selected.height,
                      'invalid_or_other_day_timestamp_rows': flags.filter(~pl.col('valid_time')).height,
                      'unsupported_contract_rows': flags.filter(~pl.col('valid_contract')).height,
                      'nonpositive_or_invalid_numeric_rows': flags.filter(~pl.col('valid_numbers')).height,
                      'unclassified_product_rows': flags.filter(~pl.col('known_product')).height,
                      'unclassified_products': unknown}
