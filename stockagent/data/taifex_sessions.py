"""Canonical TAIFEX day/night session and trading-date mapping.

TAIFEX assigns the 15:00--05:00 after-hours session to the following regular
trading session.  Friday night therefore belongs to Monday (absent an exchange
holiday calendar); the weekday mapping here is intentionally limited to the
normal weekly schedule used by live callback timestamps.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
DAY_OPEN = time(8, 45)
DAY_CLOSE = time(13, 45)
NIGHT_PREOPEN = time(14, 50)
NIGHT_OPEN = time(15, 0)
NIGHT_CLOSE = time(5, 0)


def _localize(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=TAIPEI)
    return value.astimezone(TAIPEI)


def _next_weekday(value: date) -> date:
    candidate = value
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def taifex_trading_date(value: datetime) -> date:
    """Map a TAIFEX callback timestamp to its exchange trading date."""

    local = _localize(value)
    if local.time() >= NIGHT_PREOPEN:
        return _next_weekday(local.date() + timedelta(days=1))
    if local.time() < NIGHT_CLOSE and local.weekday() == 5:
        return _next_weekday(local.date())
    return local.date()


def taifex_session_kind(value: datetime, *, include_preopen: bool = False) -> str:
    """Return ``day``, ``night``, or ``closed`` for the normal weekly schedule."""

    local = _localize(value)
    weekday = local.weekday()
    clock = local.time()
    if weekday < 5 and DAY_OPEN <= clock < DAY_CLOSE:
        return "day"
    night_start = NIGHT_PREOPEN if include_preopen else NIGHT_OPEN
    if (weekday < 5 and clock >= night_start) or (
        0 < weekday <= 5 and clock < NIGHT_CLOSE
    ):
        return "night"
    return "closed"


def taifex_continuous_session_open(value: datetime) -> bool:
    return taifex_session_kind(value) in {"day", "night"}


__all__ = [
    "TAIPEI",
    "taifex_continuous_session_open",
    "taifex_session_kind",
    "taifex_trading_date",
]
