"""Bounded Windows/WSL operation clocks, never market/publication timestamps.

Native source observations and Python commits use different wall clocks. Exact
attempt identity, immutable code, local file order and a finite host deadline
remain mandatory; a small remote clock offset is not an unknown source action.
No clock is rewritten, and this helper never grants a query or extends a lease.
"""
from datetime import UTC, datetime, timedelta

CONTRACT = "bounded_cross_clock_operation_window_v1"
MAX_CLOCK_SKEW_SECONDS = 5


class OperationClockError(ValueError):
    """A safe reason code, without private source/session details."""


def source_operation_clock(value: str, started: datetime, finished: datetime) -> dict:
    """Validate a remote observation against a fixed local operation window."""
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise OperationClockError("source_operation_clock_invalid") from exc
    if (any(not isinstance(t, datetime) or t.tzinfo is None or t.utcoffset() is None
            for t in (started, finished, observed)) or finished < started):
        raise OperationClockError("source_operation_clock_invalid")
    margin = timedelta(seconds=MAX_CLOCK_SKEW_SECONDS)
    if (not started - margin <= observed <= finished + margin
            or observed > datetime.now(UTC) + margin):
        raise OperationClockError("source_clock_outside_operation_window")
    outside = ((observed - started).total_seconds() if observed < started
               else (observed - finished).total_seconds() if observed > finished else 0.0)
    return {"contract": CONTRACT, "max_clock_skew_seconds": MAX_CLOCK_SKEW_SECONDS,
            "source_observed_at_utc": value,
            "local_window_started_at_utc": started.isoformat(),
            "local_window_finished_at_utc": finished.isoformat(),
            "outside_local_window_seconds": outside}
