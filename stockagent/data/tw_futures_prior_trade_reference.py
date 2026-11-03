"""Reduce authenticated official pre-session prints to causal reference events.

Callers authenticate raw files and dated identities.  These events are prices
for model inputs only; they never certify an execution or a complete session.
"""
from __future__ import annotations

from datetime import date, datetime, time
import re

import polars as pl


def prior_trade_events(prints: pl.DataFrame, sessions: pl.DataFrame, *, day: date,
                       source_sha256: str) -> tuple[pl.DataFrame, dict]:
    """Keep only provable same-generation pre-session events and their ties.

    Sessions contain dated physical identity plus the established regular
    session envelope in seconds. An absent natural-date mapping is unknown,
    except for a source-archive overnight print through 05:00 when the previous
    natural day and target trading day prove the identical generation. The
    exchange describes after-hours as the next trading session and does not
    list new delivery months overnight. Two observations per contract suffice
    for the midnight denominator and 08:45 decision; neither changes the
    day-session execution tape or claims complete source coverage.
    """
    required = {'date', 'product', 'contract', 'physical_contract', '_start', '_end'}
    if required - set(sessions.columns) or not re.fullmatch(r'[0-9a-f]{64}', source_sha256):
        raise ValueError('dated session identities and official source hash are required')
    if sessions.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('dated physical identity must be unique')
    if {'product', 'contract', 'event_ts', 'price'} - set(prints.columns):
        raise ValueError('official prints are missing identity, time or price')
    target = sessions.filter(pl.col('date') == day).select(
        'product', 'contract', 'physical_contract', pl.col('_start').alias('_target_start'))
    if not target.height:
        raise ValueError('no target-day dated session identities')
    if prints.filter(pl.col('event_ts').is_null() | pl.col('price').is_null()
                     | ~pl.col('price').is_finite() | (pl.col('price') <= 0)).height:
        raise ValueError('official prior observations must be timed positive real prices')
    history = sessions.select(pl.col('date').alias('_event_date'), 'product', 'contract',
                              pl.col('physical_contract').alias('_history_physical'),
                              pl.col('_end').alias('_history_end'))
    previous_natural = sessions.select(
        (pl.col('date') + pl.duration(days=1)).alias('_event_date'), 'product', 'contract',
        pl.col('physical_contract').alias('_previous_natural_physical'))
    candidates = (prints.join(target, on=['product', 'contract'], how='inner', validate='m:1')
                  .with_columns(pl.col('event_ts').dt.date().alias('_event_date'),
                                (pl.col('event_ts').dt.hour().cast(pl.Int32) * 3600
                                 + pl.col('event_ts').dt.minute().cast(pl.Int32) * 60
                                 + pl.col('event_ts').dt.second().cast(pl.Int32)).alias('_second'),
                                (pl.col('event_ts').dt.truncate('1m') + pl.duration(minutes=1)).alias('available_by'))
                  .filter(pl.col('available_by') <= datetime.combine(day, time(8, 45))))
    mapped = (candidates.join(history, on=['_event_date', 'product', 'contract'], how='left', validate='m:1')
              .join(previous_natural, on=['_event_date', 'product', 'contract'], how='left', validate='m:1'))
    missing_identity = mapped.filter(pl.col('_history_physical').is_null()).height
    changed_identity = mapped.filter(pl.col('_history_physical').is_not_null()
                                     & (pl.col('_history_physical') != pl.col('physical_contract'))).height
    bridge = (pl.col('_history_physical').is_null() & (pl.col('_event_date') < day)
              & (pl.col('_second') <= 5 * 3600)
              & (pl.col('_previous_natural_physical') == pl.col('physical_contract'))).fill_null(False)
    same_generation_bridge = mapped.filter(bridge).height
    eligible = mapped.filter(((pl.col('_history_physical') == pl.col('physical_contract'))
        & (((pl.col('_event_date') == day) & (pl.col('_second') < pl.col('_target_start')))
           | ((pl.col('_event_date') < day) & (pl.col('_second') > pl.col('_history_end'))))) | bridge)
    midnight = datetime.combine(day, time())

    def last(frame: pl.DataFrame) -> pl.DataFrame:
        keys = ['physical_contract']
        last_ts = frame.group_by(keys).agg(pl.col('event_ts').max().alias('_last_ts'))
        return (frame.join(last_ts, on=keys, how='inner').filter(pl.col('event_ts') == pl.col('_last_ts'))
                .group_by(keys).agg(pl.col('event_ts').first(), pl.col('available_by').first(),
                                    pl.col('price').min().alias('price'),
                                    (pl.col('price').n_unique() > 1).alias('fallback_ambiguous')))

    selected = pl.concat([last(eligible.filter(pl.col('available_by') < midnight)), last(eligible)],
                         how='vertical_relaxed').unique()
    # A placeholder in an ambiguous second is an observed price, but v2 masks
    # it; it must never be passed to the legacy v1 reducer as a definite price.
    events = selected.select('physical_contract', 'price', pl.col('event_ts').dt.date().alias('trade_date'),
        pl.col('event_ts').alias('trade_ts'), 'available_by', pl.lit(source_sha256).alias('source_sha256'),
        pl.lit(False).alias('is_session_open'), pl.lit(False).alias('opening_ambiguous'), 'fallback_ambiguous')
    return events.sort('physical_contract', 'available_by'), {
        'target_contracts': target.height, 'predecision_source_rows': candidates.height,
        'missing_natural_date_identity_rows': missing_identity,
        'same_generation_overnight_bridge_rows': same_generation_bridge,
        'unresolved_natural_date_identity_rows': missing_identity - same_generation_bridge,
        'different_generation_identity_rows': changed_identity,
        'eligible_rows': eligible.height, 'reference_events': events.height,
        'ambiguous_last_second_events': events.filter(pl.col('fallback_ambiguous')).height,
    }
