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

    def publishing_boundary(stamp, row):
        weekdays = row.get('publishing_weekdays')
        if weekdays is None:
            return stamp
        if not weekdays or any(type(day) is not int or not 0 <= day <= 6 for day in weekdays):
            raise ValueError('invalid_publishing_weekdays')
        local = stamp.astimezone(TAIPEI)
        while local.weekday() not in weekdays:
            local = (local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return local.astimezone(UTC)

    for index, row in enumerate(events):
        first = datetime.fromisoformat(row['first_at_utc']).astimezone(UTC)
        period = row['interval_seconds']
        first = publishing_boundary(first, row)
        if first < cursor and period:
            # A short publishing interval resets to midnight after a closed
            # block, exactly as the worker's next successful check does. A
            # later stage must keep that reset phase instead of extrapolating
            # the original Friday hour through a weekend (and shifting ETA).
            weekdays = row.get('publishing_weekdays')
            if weekdays is not None and period <= 86400:
                local = cursor.astimezone(TAIPEI)
                for offset in range(7):
                    boundary = (local - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)
                    if (boundary.weekday() in weekdays and (boundary - timedelta(days=1)).weekday() not in weekdays
                            and first <= boundary.astimezone(UTC) <= cursor):
                        first = boundary.astimezone(UTC)
                        break
            first += timedelta(seconds=math.ceil((cursor - first).total_seconds() / period) * period)
        first = publishing_boundary(first, row)
        if first >= cursor:
            heapq.heappush(heap, (first, index))
    remaining = float(requests)
    refresh = 0
    pause = 0.0
    calendar = {}
    midnight = cursor

    def protected_day(day):
        if day not in calendar:
            calendar[day] = day_is_protected(day)
        return calendar[day]

    while remaining > 1e-8:
        if cursor > horizon:
            raise ValueError('timed_projection_horizon')
        while heap and heap[0][0] <= cursor:
            stamp, index = heapq.heappop(heap)
            row = events[index]
            if not row['session_only'] or protected_day(stamp.astimezone(TAIPEI).date()):
                remaining += row['requests']
                refresh += row['requests']
            period = row['interval_seconds']
            if period:
                heapq.heappush(heap, (publishing_boundary(stamp + timedelta(seconds=period), row), index))
        # Many release events share a session. Its immutable calendar decision
        # and UTC boundaries cost O(days), not O(release events). Event order,
        # capacity arithmetic and refresh counts stay exactly unchanged.
        if cursor >= midnight:
            day = cursor.astimezone(TAIPEI).date()
            protected = protected_day(day)
            opening = datetime.combine(day, time(8, 20), TAIPEI).astimezone(UTC)
            resume = datetime.combine(day, time(9, 10), TAIPEI).astimezone(UTC)
            midnight = datetime.combine(day + timedelta(days=1), time(), TAIPEI).astimezone(UTC)
        paused = protected and opening <= cursor < resume
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
