from datetime import UTC, datetime, timedelta
import pytest

from downloader.finmind_eta_schedule import release_aware_finish

NOW = datetime(2026, 9, 30, 9, 0, tzinfo=UTC)


def event(at, calls, period=86400, session=False):
    return {'first_at_utc': at.isoformat(), 'requests': calls,
            'interval_seconds': period, 'session_only': session}


def finish(count, events, start=NOW, rate=100, protected=lambda _: False):
    return release_aware_finish(start, count, rate, events, day_is_protected=protected)


def test_no_capacity_cost_before_release():
    done, pause, calls = finish(100, [event(NOW + timedelta(hours=2), 2000)])
    assert done == NOW + timedelta(hours=1)
    assert (pause, calls) == (0, 0)


def test_due_burst_uses_one_shared_lane_then_returns_to_history():
    done, _, calls = finish(300, [event(NOW + timedelta(hours=2), 200)])
    assert done == NOW + timedelta(hours=5)
    assert calls == 200


def test_later_stages_do_not_recount_predecessor_releases():
    events = [event(NOW + timedelta(minutes=30), 50, 3600)]
    done, _, one = finish(100, events)
    end, _, two = finish(100, events, start=done)
    combined, _, total = finish(200, events)
    assert end == combined and total == one + two


def test_known_closed_day_skips_stock_release_not_news():
    events = [event(NOW, 100, session=True), event(NOW, 20)]
    done, _, calls = finish(100, events)
    assert calls == 20 and done == NOW + timedelta(minutes=72)


def test_releases_during_opening_pause_are_not_lost():
    start = datetime(2026, 9, 30, 0, 10, tzinfo=UTC)
    done, pause, calls = finish(100, [event(start + timedelta(minutes=20), 50)], start=start,
                               protected=lambda _: True)
    assert pause == 3000 and calls == 50
    assert done == start + timedelta(minutes=140)


def test_zero_work_does_not_wait_for_tomorrow_and_invalid_rate_rejected():
    assert finish(0, [event(NOW, 100)])[0] == NOW
    with pytest.raises(ValueError):
        finish(100, [], rate=0)


def test_intraday_calendar_decision_is_cached_per_day_not_per_release():
    checks = []

    def protected(day):
        checks.append(day)
        return False

    events = [event(NOW + timedelta(minutes=i), 1, period=0, session=True) for i in range(20)]
    done, pause, calls = finish(100, events, rate=100, protected=protected)
    assert done == NOW + timedelta(hours=1)
    assert (pause, calls) == (0, 0)
    assert checks == [NOW.date()]


def test_cached_boundaries_change_at_taipei_midnight_and_session_closure():
    start = datetime(2026, 9, 30, 15, 50, tzinfo=UTC)  # 23:50 Taipei
    checks = []

    def protected(day):
        checks.append(day)
        return day.day == 1

    events = [event(start + timedelta(minutes=5 * i), 1, period=0, session=True) for i in range(12)]
    done, pause, calls = finish(100, events, start=start, rate=10, protected=protected)
    # Two closed-day events are skipped; ten Oct 1 events consume capacity.
    assert calls == 10 and pause == 3000
    assert done == start + timedelta(hours=11, minutes=50)
    assert len(checks) == len(set(checks)) == 2


def test_periodic_publication_weekend_is_not_a_cash_session_filter():
    sunday = datetime(2026, 10, 3, 17, tzinfo=UTC)
    row = event(sunday + timedelta(hours=1), 50, period=10800)
    row['publishing_weekdays'] = list(range(5))
    done, _, calls = finish(100, [row], start=sunday, rate=100)
    assert done == sunday + timedelta(hours=1) and calls == 0
    monday = datetime(2026, 10, 4, 16, tzinfo=UTC)
    done, _, calls = finish(100, [row], start=monday, rate=100)
    assert done == monday + timedelta(minutes=90) and calls == 50


def test_later_stage_keeps_midnight_reset_phase_after_publishing_weekend():
    friday = datetime(2026, 10, 2, 14, tzinfo=UTC)  # Fri 22:00 Taipei.
    row = event(friday + timedelta(hours=1), 10, period=10800)
    row['publishing_weekdays'] = list(range(5))
    # The first Friday phase is 23:00; after the weekend it is Monday 00:00,
    # 03:00, ... not the original modulo-3 phase of Monday 02:00, 05:00, ...
    done, _, first = finish(5350, [row], start=friday, rate=100)
    end, _, second = finish(400, [row], start=done, rate=100)
    combined, _, total = finish(5750, [row], start=friday, rate=100)
    assert end == combined and total == first + second


@pytest.mark.parametrize('weekdays', [[], [7], [-1], [True], ['Monday']])
def test_invalid_periodic_publication_clock_does_not_fake_eta(weekdays):
    row = event(NOW, 1, period=10800)
    row['publishing_weekdays'] = weekdays
    with pytest.raises(ValueError, match='invalid_publishing_weekdays'):
        finish(100, [row])
