"""Source-only option opening-reference candidates at a completed-second clock.

The caller supplies a dated *candidate* session policy and authenticated raw
rows.  Results are not listing, source-completeness, price-grid, fill, or
training admission proofs.  No within-second ordering is inferred.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import polars as pl


CONTRACT = 'tw_option_observed_reference_candidate_v1'
KEYS = ['product_raw', 'series_raw', 'strike_decimal', 'right_raw']


def _normalize(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    mapping = {}
    for raw in frame[column].unique().to_list():
        try:
            value = Decimal(raw)
        except (InvalidOperation, TypeError):
            raise ValueError(f'invalid native {column}') from None
        if not value.is_finite() or value <= 0:
            raise ValueError(f'invalid native {column}')
        mapping[raw] = format(value.normalize(), 'f')
    return frame.with_columns(pl.col(column).replace_strict(mapping, return_dtype=pl.String)
                              .alias(column.replace('_raw', '_decimal')))


def compact_last_observations(rows: pl.DataFrame) -> pl.DataFrame:
    """Retain all distinct price/hash alternatives in each key's last second.

    This is a derived in-memory state for later source comparisons; native
    duplicate side rows remain untouched in the original archives. It is not a
    price, position, contract-eligibility or training admission. The caller must
    pass only source rows whose source dates precede the next decision date.
    """
    required = {'event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw',
                'price_raw', 'source_sha256'}
    if required - set(rows.columns) or rows.is_empty():
        raise ValueError('a nonempty explicit source state is required')
    if rows.filter(pl.col('event_ts').is_null() | pl.col('product_raw').is_null()
                   | pl.col('series_raw').is_null() | pl.col('source_sha256').is_null()
                   | (pl.col('source_sha256').str.len_chars() != 64)
                   | ~pl.col('right_raw').is_in(['C', 'P'])).height:
        raise ValueError('invalid reference candidate source identity')
    normalized = _normalize(_normalize(rows, 'strike_raw'), 'price_raw')
    last = normalized.group_by(KEYS).agg(pl.col('event_ts').max().alias('event_ts'))
    selected = normalized.join(last, on=[*KEYS, 'event_ts'], how='inner', validate='m:1')
    return (selected.select('event_ts', 'product_raw', 'series_raw',
                            pl.col('strike_decimal').alias('strike_raw'), 'right_raw',
                            pl.col('price_decimal').alias('price_raw'), 'source_sha256')
            .unique(maintain_order=False).sort('product_raw', 'series_raw', 'strike_raw',
                                               'right_raw', 'price_raw', 'source_sha256'))


def reference_candidates(rows: pl.DataFrame, *, trading_date: date, decision_time: str,
                         session_windows: dict[str, tuple[int, int]]) -> pl.DataFrame:
    """Reduce only observed prints known by the decision, preserving ambiguity."""
    required = {'event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw',
                'price_raw', 'source_sha256'}
    if (required - set(rows.columns) or decision_time not in {'08:45', '09:00'}
            or not session_windows or any(not isinstance(window, tuple) or len(window) != 2
                                         or not 0 <= window[0] < window[1] <= 1440
                                         for window in session_windows.values())):
        raise ValueError('explicit source rows, decision and candidate sessions are required')
    hour, minute = map(int, decision_time.split(':'))
    cutoff = datetime(trading_date.year, trading_date.month, trading_date.day, hour, minute,
                      tzinfo=ZoneInfo('Asia/Taipei'))
    source = rows.filter(pl.col('product_raw').is_in(sorted(session_windows)))
    if (source.filter(pl.col('event_ts').is_null() | pl.col('series_raw').is_null()
                      | pl.col('source_sha256').is_null() | (pl.col('source_sha256').str.len_chars() != 64)
                      | ~pl.col('right_raw').is_in(['C', 'P'])).height):
        raise ValueError('invalid reference candidate source identity')
    source = source.filter(pl.col('event_ts') + pl.duration(seconds=1) <= cutoff)
    if source.is_empty():
        return pl.DataFrame(schema={key: pl.String for key in KEYS} | {
            'reference_kind': pl.String, 'reference_price_decimal': pl.String,
            'trade_ts': pl.Datetime('us', 'Asia/Taipei'),
            'available_by': pl.Datetime('us', 'Asia/Taipei'), 'source_hashes': pl.List(pl.String)})
    source = _normalize(_normalize(source, 'strike_raw'), 'price_raw')
    windows = pl.DataFrame({'product_raw': list(session_windows),
                            'session_start': [v[0] for v in session_windows.values()],
                            'session_end': [v[1] for v in session_windows.values()]})
    seconds = (source.group_by(*KEYS, 'event_ts').agg(
        pl.col('price_decimal').n_unique().alias('distinct_prices'),
        pl.col('price_decimal').first().alias('candidate_price'),
        pl.col('source_sha256').unique().sort().alias('source_hashes'))
        .join(windows, on='product_raw', how='left', validate='m:1')
        .with_columns((pl.col('event_ts').dt.hour().cast(pl.Int32) * 60
                       + pl.col('event_ts').dt.minute().cast(pl.Int32))
                      .alias('_event_minute')))
    ordered = seconds.sort(*KEYS, 'event_ts')
    latest = ordered.group_by(KEYS, maintain_order=True).last()
    first = (ordered.filter((pl.col('event_ts').dt.date() == trading_date)
                            & (pl.col('_event_minute') >= pl.col('session_start'))
                            & (pl.col('_event_minute') < pl.col('session_end')))
             .group_by(KEYS, maintain_order=True).first())
    combined = latest.join(first.select(*KEYS, 'event_ts', 'distinct_prices', 'candidate_price', 'source_hashes'),
                           on=KEYS, how='left', suffix='_open', validate='1:1')
    opened = pl.col('event_ts_open').is_not_null()
    selected_prices = pl.when(opened).then(pl.col('distinct_prices_open')).otherwise(pl.col('distinct_prices'))
    selected_ts = pl.when(opened).then(pl.col('event_ts_open')).otherwise(pl.col('event_ts'))
    return combined.select(*KEYS,
        pl.when(opened & (selected_prices == 1)).then(pl.lit('observed_session_open_candidate'))
          .when(opened).then(pl.lit('ambiguous_first_second'))
          .when(selected_prices == 1).then(pl.lit('last_observed_trade_candidate'))
          .otherwise(pl.lit('ambiguous_last_second')).alias('reference_kind'),
        pl.when(selected_prices == 1)
          .then(pl.when(opened).then(pl.col('candidate_price_open')).otherwise(pl.col('candidate_price')))
          .otherwise(None).alias('reference_price_decimal'),
        selected_ts.alias('trade_ts'), (selected_ts + pl.duration(seconds=1)).alias('available_by'),
        pl.when(opened).then(pl.col('source_hashes_open')).otherwise(pl.col('source_hashes'))
          .alias('source_hashes')).sort(KEYS)
