from datetime import datetime
import pytest

from stockagent.data_sync.archive_schedule import allows_start, next_due, validate_window

WINDOW = {'timezone':'Asia/Taipei', 'start_hour':3, 'end_hour':7}


def stamp(value):
    return datetime.fromisoformat(value+'+08:00').timestamp()


@pytest.mark.parametrize('now,allowed,due', [
    ('2026-11-10T02:59:59',False,'2026-11-10T03:00:00'),
    ('2026-11-10T03:00:00',True,'2026-11-10T03:00:00'),
    ('2026-11-10T06:59:59',True,'2026-11-10T03:00:00'),
    ('2026-11-10T07:00:00',False,'2026-11-11T03:00:00'),
    ('2026-11-10T23:59:59',False,'2026-11-11T03:00:00'),
])
def test_daily_window_has_exact_boundaries(now, allowed, due):
    assert allows_start(stamp(now),WINDOW) is allowed
    assert next_due(None,stamp(now),WINDOW)==stamp(due)


def test_a_late_finish_does_not_shift_the_next_day_or_repeat_the_same_day():
    assert next_due(stamp('2026-11-09T06:50:00'),stamp('2026-11-10T03:00:00'),WINDOW)==stamp('2026-11-10T03:00:00')
    assert next_due(stamp('2026-11-10T03:40:00'),stamp('2026-11-10T06:00:00'),WINDOW)==stamp('2026-11-11T03:00:00')
    assert allows_start(stamp('2026-11-10T12:00:00'),None)


@pytest.mark.parametrize('window', [dict(WINDOW,start_hour=7),dict(WINDOW,end_hour=25),
    dict(WINDOW,timezone=None),dict(WINDOW,unexpected=True)])
def test_invalid_timing_is_rejected(window):
    with pytest.raises((ValueError,TypeError)):
        validate_window(window)
