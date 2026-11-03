"""Conservatively mark known missing days in observed option references.

This guard takes a source-bound list of known gaps; absence of a listed gap is
not proof of a complete feed, point-in-time delivery, a live contract or an
executable price. It does not alter the observed candidate or invent a price.
"""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl


def mark_known_missing_days(references: pl.DataFrame, *, missing_days: list[date],
                            trading_date: date, decision_time: str) -> pl.DataFrame:
    """Flag candidates whose selected observation precedes a known gap.

    A whole missing day has unknown intraday order, so a gap on the selected
    observation's date is conservatively considered after it. The caller is
    responsible for deriving and verifying the gap list from its source plan.
    """
    if (decision_time not in ('08:45', '09:00')
            or not isinstance(trading_date, date) or isinstance(trading_date, datetime)
            or any(not isinstance(day, date) or isinstance(day, datetime) for day in missing_days)
            or missing_days != sorted(set(missing_days))
            or {'trade_ts', 'available_by'} - set(references.columns)):
        raise ValueError('ordered dated gaps and explicit reference clocks are required')
    hour, minute = map(int, decision_time.split(':'))
    cutoff = datetime(trading_date.year, trading_date.month, trading_date.day, hour, minute,
                      tzinfo=ZoneInfo('Asia/Taipei'))
    if references.filter(pl.col('trade_ts').is_null() | pl.col('available_by').is_null()
                         | (pl.col('available_by') > cutoff)
                         | (pl.col('trade_ts') + pl.duration(seconds=1) != pl.col('available_by'))).height:
        raise ValueError('reference observation is invalid or unavailable at decision time')
    eligible = [day for day in missing_days if day <= trading_date]
    latest = eligible[-1] if eligible else None
    return references.with_columns(
        pl.lit(latest, dtype=pl.Date).alias('latest_known_missing_source_day'),
        (pl.lit(False) if latest is None else pl.col('trade_ts').dt.date() <= latest)
        .alias('known_missing_day_since_reference'))


def require_no_known_missing_days(references: pl.DataFrame) -> None:
    """Reject a selected set with unknown or recorded intervening gaps.

    This is only one necessary admission check. An unflagged reference still
    needs independent lifecycle, completeness, pricing and accounting checks.
    """
    name = 'known_missing_day_since_reference'
    if (name not in references.columns or references.schema[name] != pl.Boolean
            or references.select(pl.col(name).is_null().any() | pl.col(name).any()).item()):
        raise ValueError('selected reference has an unknown or known missing source day')
