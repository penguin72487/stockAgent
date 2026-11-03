"""Compact source trades for the existing two-clock option reference reducer.

The index is a lossless projection for 08:45/09:00 references and end-of-day
observed state, not a listing, historical availability, or execution proof.
Native source receipts must be validated by the caller. Whole-second ties are
retained; no execution order is inferred.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.data.tw_option_reference_candidates import KEYS, _normalize


CONTRACT = 'tw_option_two_clock_event_index_v1'
SOURCE_COLUMNS = ('event_ts', 'product_raw', 'series_raw', 'strike_raw', 'right_raw',
                  'price_raw', 'source_sha256')


def compact_decision_events(rows: pl.DataFrame, *, day: date,
                            session_windows: dict[str, tuple[int, int]]) -> pl.DataFrame:
    """Keep exactly the observed seconds needed by the shared reducer.

    Per contract, retain the latest pre-decision second for each clock, the
    first session second, and the last natural-day second. All distinct native
    price/source alternatives at a retained second remain in the output.
    The reducer, not this function, applies the decision-time information gate.
    """
    if set(SOURCE_COLUMNS) - set(rows.columns) or rows.is_empty() or not session_windows:
        raise ValueError('nonempty source events and explicit session windows required')
    if any(not isinstance(k, str) or not k or not isinstance(v, tuple) or len(v) != 2
           or not all(isinstance(n, int) for n in v) or not 0 <= v[0] < v[1] <= 1440
           for k, v in session_windows.items()):
        raise ValueError('invalid candidate session windows')
    rows = rows.select(*SOURCE_COLUMNS)
    if (rows.filter(pl.col('event_ts').is_null() | (pl.col('event_ts').dt.date() != day)
                    | pl.col('product_raw').is_null() | pl.col('series_raw').is_null()
                    | pl.col('source_sha256').is_null()
                    | (pl.col('source_sha256').str.len_chars() != 64)
                    | ~pl.col('right_raw').is_in(['C', 'P']).fill_null(False)).height
            or set(rows['product_raw'].unique().to_list()) - set(session_windows)):
        raise ValueError('source day, product, contract or source identity differs')
    rows = _normalize(_normalize(rows, 'strike_raw'), 'price_raw')
    windows = pl.DataFrame({'product_raw': list(session_windows),
                            '_start': [v[0] for v in session_windows.values()],
                            '_end': [v[1] for v in session_windows.values()]})
    frame = (rows.join(windows, on='product_raw', how='left', validate='m:1')
             .with_columns((pl.col('event_ts').dt.hour().cast(pl.Int32) * 60
                            + pl.col('event_ts').dt.minute().cast(pl.Int32)).alias('_minute')))
    cutoffs = [datetime(day.year, day.month, day.day, hour, minute,
                        tzinfo=ZoneInfo('Asia/Taipei'))
               for hour, minute in ((8, 45), (9, 0))]
    summary = frame.group_by(KEYS).agg(
        *(pl.when(pl.col('event_ts') + pl.duration(seconds=1) <= cutoff)
          .then(pl.col('event_ts')).max().alias(f'_last_{i}')
          for i, cutoff in enumerate(cutoffs)),
        pl.when((pl.col('_minute') >= pl.col('_start')) & (pl.col('_minute') < pl.col('_end')))
          .then(pl.col('event_ts')).min().alias('_first_session'),
        pl.col('event_ts').max().alias('_last_day'))
    selected = (frame.join(summary, on=KEYS, how='inner', validate='m:1')
                .filter(pl.any_horizontal(*(pl.col('event_ts') == pl.col(name)
                                            for name in ('_last_0', '_last_1',
                                                         '_first_session', '_last_day'))))
                .select('event_ts', 'product_raw', 'series_raw',
                        pl.col('strike_decimal').alias('strike_raw'), 'right_raw',
                        pl.col('price_decimal').alias('price_raw'), 'source_sha256')
                .unique().sort('product_raw', 'series_raw', 'strike_raw', 'right_raw',
                               'event_ts', 'price_raw', 'source_sha256'))
    if selected.is_empty():
        raise ValueError('event compaction unexpectedly removed every source row')
    return selected
