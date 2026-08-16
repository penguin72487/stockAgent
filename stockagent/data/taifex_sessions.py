"""Canonical TAIFEX day/night session and trading-date mapping.

TAIFEX assigns the 15:00--05:00 after-hours session to the following regular
trading session.  Friday night therefore belongs to Monday (absent an exchange
holiday calendar); the weekday mapping here is intentionally limited to the
normal weekly schedule used by live callback timestamps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import math
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
DAY_PREOPEN = time(8, 30)
DAY_OPEN = time(8, 45)
DAY_CLOSE = time(13, 45)
NIGHT_PREOPEN = time(14, 50)
NIGHT_OPEN = time(15, 0)
NIGHT_CLOSE = time(5, 0)
DAY_CAPTURE_OPEN = DAY_PREOPEN
DAY_CAPTURE_CLOSE = time(13, 45, 5)
NIGHT_CAPTURE_OPEN = NIGHT_PREOPEN
NIGHT_CAPTURE_CLOSE = time(5, 0, 5)


@dataclass(frozen=True)
class TaifexCaptureWindow:
    """One prospective quote-capture window on the normal weekly calendar."""

    session: str
    trading_date: date
    starts_at: datetime
    stops_at: datetime

    def delay_seconds(self, value: datetime) -> int:
        return max(0, math.ceil((self.starts_at - _localize(value)).total_seconds()))


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
    # The collector intentionally remains connected for five seconds after the
    # 05:00 night close so final exchange book messages can drain.  Those
    # closing-grace callbacks still belong to Friday night's following trading
    # date, not to Saturday's calendar date.
    if local.time() <= NIGHT_CAPTURE_CLOSE and local.weekday() == 5:
        return _next_weekday(local.date())
    return local.date()


def taifex_session_kind(value: datetime, *, include_preopen: bool = False) -> str:
    """Return ``day``, ``night``, or ``closed`` for the normal weekly schedule."""

    local = _localize(value)
    weekday = local.weekday()
    clock = local.time()
    day_start = DAY_PREOPEN if include_preopen else DAY_OPEN
    if weekday < 5 and day_start <= clock < DAY_CLOSE:
        return "day"
    night_start = NIGHT_PREOPEN if include_preopen else NIGHT_OPEN
    if (weekday < 5 and clock >= night_start) or (
        0 < weekday <= 5 and clock < NIGHT_CLOSE
    ):
        return "night"
    return "closed"


def taifex_market_phase(value: datetime) -> str:
    """Return the exchange phase without treating auction books as fills."""

    local = _localize(value)
    weekday = local.weekday()
    clock = local.time()
    if weekday < 5 and DAY_PREOPEN <= clock < DAY_OPEN:
        return "day_preopen"
    if weekday < 5 and DAY_OPEN <= clock < DAY_CLOSE:
        return "day_continuous"
    if weekday < 5 and DAY_CLOSE <= clock < NIGHT_PREOPEN:
        return "day_close_to_night_preopen"
    if weekday < 5 and NIGHT_PREOPEN <= clock < NIGHT_OPEN:
        return "night_preopen"
    if (weekday < 5 and clock >= NIGHT_OPEN) or (
        0 < weekday <= 5 and clock < NIGHT_CLOSE
    ):
        return "night_continuous"
    return "closed_monitoring"


def taifex_continuous_session_open(value: datetime) -> bool:
    return taifex_session_kind(value) in {"day", "night"}


def next_taifex_capture_window(value: datetime) -> TaifexCaptureWindow:
    """Return the active or next day/night capture window.

    This deliberately models the normal weekly schedule only.  Exchange
    holidays remain an audit-time absence rather than being projected from an
    unversioned live holiday calendar.
    """

    local = _localize(value)
    candidates: list[TaifexCaptureWindow] = []
    for offset in range(-1, 10):
        session_date = local.date() + timedelta(days=offset)
        if session_date.weekday() >= 5:
            continue
        day_start = datetime.combine(session_date, DAY_CAPTURE_OPEN, tzinfo=TAIPEI)
        day_stop = datetime.combine(session_date, DAY_CAPTURE_CLOSE, tzinfo=TAIPEI)
        candidates.append(
            TaifexCaptureWindow(
                session="day",
                trading_date=session_date,
                starts_at=day_start,
                stops_at=day_stop,
            )
        )
        night_start = datetime.combine(session_date, NIGHT_CAPTURE_OPEN, tzinfo=TAIPEI)
        night_stop = datetime.combine(
            session_date + timedelta(days=1),
            NIGHT_CAPTURE_CLOSE,
            tzinfo=TAIPEI,
        )
        candidates.append(
            TaifexCaptureWindow(
                session="night",
                trading_date=_next_weekday(session_date + timedelta(days=1)),
                starts_at=night_start,
                stops_at=night_stop,
            )
        )
    future = sorted(
        (candidate for candidate in candidates if candidate.stops_at > local),
        key=lambda candidate: candidate.starts_at,
    )
    if not future:
        raise RuntimeError("could not resolve the next TAIFEX capture window")
    return future[0]


__all__ = [
    "DAY_PREOPEN",
    "TAIPEI",
    "TaifexCaptureWindow",
    "next_taifex_capture_window",
    "taifex_continuous_session_open",
    "taifex_market_phase",
    "taifex_session_kind",
    "taifex_trading_date",
]
