"""Shared scheduling policy for quota-consuming Shioaji history queries."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
import math
from pathlib import Path
from zoneinfo import ZoneInfo

from stockagent.live.market_status import is_trading_day


TAIPEI = ZoneInfo("Asia/Taipei")
HISTORICAL_QUERY_CUTOFF = time(7, 45)
HISTORICAL_QUERY_RESUME = time(14, 31)
HISTORICAL_MAX_TRAFFIC_FRACTION = 0.90


def _taipei_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(TAIPEI)
    if value.tzinfo is None:
        return value.replace(tzinfo=TAIPEI)
    return value.astimezone(TAIPEI)


def historical_query_pause_seconds(value: datetime | None = None) -> int:
    """Return the weekday live-priority delay for historical API queries.

    Historical queries stop before the broker's observed approximately 08:00
    quota reset.  This prevents a reset from immediately funding a pre-open
    backfill that leaves too little quota for the live trading day.
    """

    local = _taipei_datetime(value)
    if local.weekday() >= 5:
        return 0
    if not HISTORICAL_QUERY_CUTOFF <= local.time() < HISTORICAL_QUERY_RESUME:
        return 0
    resume = datetime.combine(local.date(), HISTORICAL_QUERY_RESUME, tzinfo=TAIPEI)
    return max(1, math.ceil((resume - local).total_seconds()))


def historical_query_is_protected(value: datetime | None = None) -> bool:
    return historical_query_pause_seconds(value) > 0


def previous_tw_stock_session(
    value: datetime | None = None,
    *,
    parquet_root: Path | None = None,
) -> date:
    """Return the latest completed prior TW stock session.

    Historical minute downloads intentionally exclude the current calendar
    day. Weekend and official-holiday calendar days are not valid completion
    targets and would otherwise leave a truthful Friday dataset mislabeled as
    incomplete throughout the weekend.
    """

    candidate = _taipei_datetime(value).date() - timedelta(days=1)
    for _ in range(32):
        if is_trading_day("tw", candidate, parquet_root=parquet_root):
            return candidate
        candidate -= timedelta(days=1)
    raise RuntimeError("cannot resolve the previous Taiwan stock session")


__all__ = [
    "HISTORICAL_MAX_TRAFFIC_FRACTION",
    "HISTORICAL_QUERY_CUTOFF",
    "HISTORICAL_QUERY_RESUME",
    "historical_query_is_protected",
    "historical_query_pause_seconds",
    "previous_tw_stock_session",
]
