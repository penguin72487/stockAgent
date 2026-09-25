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
MINUTE_PRE_NIGHT_YIELD = time(14, 45)
HISTORICAL_MAX_TRAFFIC_FRACTION = 0.90
SHIOAJI_PERSON_CONNECTION_LIMIT = 5


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


def reserved_fop_connection_slots(
    value: datetime | None = None, *, active_workers: int = 0
) -> int:
    """Reserve three FOP logins before night capture, or actual workers if more."""

    if active_workers < 0:
        raise ValueError("active_workers must be nonnegative")
    local = _taipei_datetime(value)
    night_reserved = (local.weekday() < 5 and local.time() >= time(14, 31)) or (
        local.weekday() in (1, 2, 3, 4, 5) and local.time() < time(5)
    )
    return max(active_workers, 3 if night_reserved else 0)


def minute_pre_night_deadline(value: datetime | None = None) -> datetime | None:
    """Bound the post-close stock download before the 14:50 FOP pre-open.

    The stock session is complete at 14:31.  Giving its incremental frontier
    the intervening window costs no live FOP connection, provided every minute
    worker has a hard, resumable stop before the next FOP login begins.
    """

    local = _taipei_datetime(value)
    if (
        local.weekday() < 5
        and HISTORICAL_QUERY_RESUME <= local.time() < MINUTE_PRE_NIGHT_YIELD
    ):
        return datetime.combine(local.date(), MINUTE_PRE_NIGHT_YIELD, tzinfo=TAIPEI)
    return None


def minute_connection_plan(
    value: datetime | None = None,
    *,
    active_fop_workers: int,
    active_history_workers: int,
    reserved_stock_quotes: int,
    configured_workers: int,
) -> tuple[int, int, int, datetime | None]:
    """Return minute workers, FOP/history reservations and a safe stop time.

    Historical runners already pause their logins from 14:31 through the night
    capture.  Reserving an extra *idle* history login during that same interval
    would strand capacity.  Actual running workers are always counted.
    """

    if (
        active_fop_workers < 0
        or active_history_workers < 0
        or not 0 <= reserved_stock_quotes < SHIOAJI_PERSON_CONNECTION_LIMIT
        or not 1 <= configured_workers <= SHIOAJI_PERSON_CONNECTION_LIMIT
    ):
        raise ValueError("invalid Shioaji minute connection plan")
    local = _taipei_datetime(value)
    deadline = minute_pre_night_deadline(local)
    reserved_fop = (
        active_fop_workers
        if deadline is not None
        else reserved_fop_connection_slots(local, active_workers=active_fop_workers)
    )
    history_paused = historical_login_pause_seconds(
        local,
        fop_workers=active_fop_workers,
        reserved_stock_quotes=reserved_stock_quotes,
    ) > 0
    reserved_history = max(active_history_workers, 0 if history_paused else 1)
    available = max(
        0,
        SHIOAJI_PERSON_CONNECTION_LIMIT
        - reserved_fop
        - reserved_history
        - reserved_stock_quotes,
    )
    return min(configured_workers, available), reserved_fop, reserved_history, deadline


def next_postreset_historical_window(
    value: datetime | None = None,
    *,
    parquet_root: Path | None = None,
) -> datetime:
    """First permitted history window after a traffic-ceiling stop.

    Shioaji resets traffic at 08:00 on a stock trading day; the next legal
    historical session on that day begins at 14:31. This is for an *observed*
    traffic stop, not a generic retry after a transient error.
    """

    local = _taipei_datetime(value)
    target = datetime.combine(local.date(), HISTORICAL_QUERY_RESUME, tzinfo=TAIPEI)
    if target <= local:
        target += timedelta(days=1)
    while not is_trading_day("tw", target.date(), parquet_root=parquet_root):
        target += timedelta(days=1)
    return target


def historical_login_pause_seconds(
    value: datetime | None = None,
    *,
    fop_workers: int = 0,
    reserved_stock_quotes: int = 2,
) -> int:
    """Keep a history login out of the reserved live-quote connection slots.

    The two history runners serialize their *logged-in* batches separately.
    The minute runner reserves one history slot whenever it starts workers.
    This function protects the three future/option workers from 14:31 until
    05:00, including the pre-capture interval, so a long history batch cannot
    prevent the night collector from logging in.
    """

    if fop_workers < 0 or not 0 <= reserved_stock_quotes < SHIOAJI_PERSON_CONNECTION_LIMIT:
        raise ValueError("invalid Shioaji connection reservation")
    local = _taipei_datetime(value)
    night_reserved = reserved_fop_connection_slots(local) == 3
    if night_reserved:
        wake = datetime.combine(local.date(), time(5, 0, 10), tzinfo=TAIPEI)
        if wake <= local:
            wake += timedelta(days=1)
        return max(1, math.ceil((wake - local).total_seconds()))
    if fop_workers + reserved_stock_quotes + 1 > SHIOAJI_PERSON_CONNECTION_LIMIT:
        return 60
    return 0


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


def latest_completed_tw_stock_session(
    value: datetime | None = None,
    *,
    parquet_root: Path | None = None,
) -> date:
    """Return today's session after the historical-query resume boundary.

    Before 14:31 the current session is mutable and the latest safe history
    target is the previous trading session.  At and after 14:31, today's
    trading session is complete and may be queried.  Downloaders still retain
    their source/receipt gates, so an upstream source that has not published
    yet remains visible as empty/partial instead of being fabricated.
    """

    local = _taipei_datetime(value)
    if (
        local.time() >= HISTORICAL_QUERY_RESUME
        and is_trading_day("tw", local.date(), parquet_root=parquet_root)
    ):
        return local.date()
    return previous_tw_stock_session(local, parquet_root=parquet_root)


__all__ = [
    "HISTORICAL_MAX_TRAFFIC_FRACTION",
    "HISTORICAL_QUERY_CUTOFF",
    "HISTORICAL_QUERY_RESUME",
    "MINUTE_PRE_NIGHT_YIELD",
    "SHIOAJI_PERSON_CONNECTION_LIMIT",
    "historical_login_pause_seconds",
    "minute_connection_plan",
    "minute_pre_night_deadline",
    "next_postreset_historical_window",
    "reserved_fop_connection_slots",
    "historical_query_is_protected",
    "historical_query_pause_seconds",
    "latest_completed_tw_stock_session",
    "previous_tw_stock_session",
]
