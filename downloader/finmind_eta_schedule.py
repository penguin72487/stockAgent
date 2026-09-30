"""Bounded event-driven capacity projection for the existing FinMind stages."""
from datetime import UTC, datetime, timedelta, time
import heapq
import math

from downloader.finmind_scheduling import TAIPEI


def release_aware_finish(start, requests, rate, events, *, day_is_protected):
    """Drain required work and released priority work through ONE service lane.

    No capacity is charged before release. Releases arriving during the opening
    pause wait in the queue. Each later stage starts from its predecessor's
    completion, so a recurring request is never charged in two stages.
    """
    if rate <= 0 or not math.isfinite(rate) or requests < 0:
        raise ValueError('positive finite capacity and nonnegative work required')
    cursor = start.astimezone(UTC)
    horizon = cursor + timedelta(days=3660)
    heap = []
    for index, row in enumerate(events):
        first = datetime.fromisoformat(row['first_at_utc']).astimezone(UTC)
        period = row['interval_seconds']
        if first < cursor and period:
            first += timedelta(seconds=math.ceil((cursor - first).total_seconds() / period) * period)
        if first >= cursor:
            heapq.heappush(heap, (first, index))
    remaining = float(requests)
    refresh = 0
    pause = 0.0
    while remaining > 1e-8:
        if cursor > horizon:
            raise ValueError('timed_projection_horizon')
        while heap and heap[0][0] <= cursor:
            stamp, index = heapq.heappop(heap)
            row = events[index]
            if not row['session_only'] or day_is_protected(stamp.astimezone(TAIPEI).date()):
                remaining += row['requests']
                refresh += row['requests']
            period = row['interval_seconds']
            if period:
                heapq.heappush(heap, (stamp + timedelta(seconds=period), index))
        local = cursor.astimezone(TAIPEI)
        day = local.date()
        protected = day_is_protected(day)
        opening = datetime.combine(day, time(8, 20), TAIPEI).astimezone(UTC)
        resume = datetime.combine(day, time(9, 10), TAIPEI).astimezone(UTC)
        paused = protected and opening <= cursor < resume
        midnight = datetime.combine(day + timedelta(days=1), time(), TAIPEI).astimezone(UTC)
        boundary = resume if paused else opening if protected and cursor < opening else midnight
        if heap:
            boundary = min(boundary, heap[0][0])
        elapsed = (boundary - cursor).total_seconds()
        if paused:
            pause += elapsed
        else:
            capacity = elapsed * rate / 3600
            if remaining <= capacity:
                cursor += timedelta(seconds=remaining * 3600 / rate)
                remaining = 0
                break
            remaining -= capacity
        cursor = boundary
    return cursor, math.ceil(pause), refresh
