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
