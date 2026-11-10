from datetime import datetime, date

import pytest

from scripts import run_bounded_storage_maintenance as bounded
from scripts.run_bounded_storage_maintenance import TAIPEI, allowed_seconds


def local(value):
    return datetime.fromisoformat(value).replace(tzinfo=TAIPEI)


def test_weekend_work_uses_eight_hours_without_artificial_two_hour_restart():
    assert allowed_seconds(local('2026-11-07T13:00:00'), session_on=lambda day: day.weekday() < 5) == 8*3600


def test_overnight_work_stops_before_the_next_trading_session():
    assert allowed_seconds(local('2026-11-09T05:30:00'), session_on=lambda day: day.weekday() < 5) == 9000
    assert allowed_seconds(local('2026-11-09T07:59:50'), session_on=lambda day: day.weekday() < 5) == 10
    assert allowed_seconds(local('2026-11-09T08:00:00'), session_on=lambda day: day.weekday() < 5) == 0
    assert allowed_seconds(local('2026-11-09T13:30:00'), session_on=lambda day: day.weekday() < 5) == 8*3600


def test_verified_holiday_can_continue_and_next_session_is_bounded():
    session = lambda day: day.weekday() < 5 and day != date(2026, 11, 9)
    assert allowed_seconds(local('2026-11-09T08:30:00'), session_on=session) == 8*3600
    assert allowed_seconds(local('2026-11-09T23:00:00'), session_on=session) == 8*3600


def test_naive_clock_and_invalid_bound_fail_closed():
    with pytest.raises(ValueError):
        allowed_seconds(datetime(2026, 11, 9, 8))
    with pytest.raises(ValueError):
        allowed_seconds(local('2026-11-09T07:00:00'), maximum=0)


def test_default_session_uses_official_calendar_guard(monkeypatch):
    from types import SimpleNamespace
    from stockagent.live import market_status
    calls = []
    def decision(day, **options):
        calls.append((day, options))
        return SimpleNamespace(status='scheduled_open')
    monkeypatch.setattr(market_status, 'tw_stock_day_decision', decision)
    assert allowed_seconds(local('2026-11-09T07:59:00')) == 60
    assert calls[0][1]['parquet_root'] == bounded.ROOT/'data_tw_public'


def test_wrapper_executes_only_the_existing_owner_with_a_bounded_process_group(monkeypatch):
    seen = []
    monkeypatch.setenv(bounded.GUARD_ENV, '0')
    monkeypatch.setattr(bounded.sys, 'argv', ['wrapper', 'scripts/run_source_local_repack_queue.py'])
    monkeypatch.setattr(bounded, 'allowed_seconds', lambda _now: 123)
    def capture(path, args):
        seen.append((path, args))
        raise RuntimeError('captured exec')
    monkeypatch.setattr(bounded.os, 'execv', capture)
    with pytest.raises(RuntimeError, match='captured exec'):
        bounded.main()
    assert seen[0][0] == '/usr/bin/timeout'
    assert seen[0][1][-1] == 'scripts/run_source_local_repack_queue.py'
    assert '--kill-after=30s' in seen[0][1] and '123s' in seen[0][1]
    assert bounded.os.environ[bounded.GUARD_ENV] == '1'


def test_unenrolled_owner_is_rejected(monkeypatch):
    monkeypatch.setattr(bounded.sys, 'argv', ['wrapper', 'scripts/other.py'])
    with pytest.raises(ValueError, match='not enrolled'):
        bounded.main()


def test_placement_owner_allows_only_its_reviewed_apply_argument(monkeypatch):
    monkeypatch.setattr(bounded, 'allowed_seconds', lambda _now: 123)
    monkeypatch.setattr(bounded.sys, 'argv', ['wrapper', 'scripts/run_packed_object_placement.py', '--apply'])
    def capture(_path, args):
        assert args[-2:] == ['scripts/run_packed_object_placement.py', '--apply']
        raise RuntimeError('captured placement exec')
    monkeypatch.setattr(bounded.os, 'execv', capture)
    with pytest.raises(RuntimeError, match='captured placement'):
        bounded.main()
    monkeypatch.setattr(bounded.sys, 'argv', ['wrapper', 'scripts/run_packed_object_placement.py', '--dataset', 'other'])
    with pytest.raises(ValueError):
        bounded.main()


def test_running_worker_yields_after_host_resume_in_a_trading_window(monkeypatch):
    monkeypatch.setenv(bounded.GUARD_ENV, '1')
    monkeypatch.setattr(bounded, 'session_requires_protection', lambda day: day.weekday() < 5)
    guard = bounded.TradingPriorityGuard()
    guard.check(local('2026-11-09T07:59:59'))
    with pytest.raises(bounded.SnapshotError, match='day-trade priority window'):
        guard.check(local('2026-11-09T08:00:00'))
    guard.check(local('2026-11-09T13:30:00'))
    guard.check(local('2026-11-07T09:00:00'))


def test_holiday_continues_but_unavailable_calendar_fails_closed(monkeypatch):
    from types import SimpleNamespace
    from stockagent.live import market_status
    monkeypatch.setenv(bounded.GUARD_ENV, '1')
    monkeypatch.setattr(market_status, 'tw_stock_day_decision', lambda *_args, **_kwargs: SimpleNamespace(status='closed'))
    bounded.TradingPriorityGuard().check(local('2026-11-09T09:00:00'))
    def unavailable(*_args, **_kwargs):
        raise OSError('calendar unavailable')
    monkeypatch.setattr(market_status, 'tw_stock_day_decision', unavailable)
    with pytest.raises(bounded.SnapshotError, match='day-trade priority window'):
        bounded.TradingPriorityGuard().check(local('2026-11-09T09:00:00'))


def test_exceptional_weekend_session_is_protected(monkeypatch):
    monkeypatch.setenv(bounded.GUARD_ENV, '1')
    monkeypatch.setattr(bounded, 'session_requires_protection', lambda _day: True)
    with pytest.raises(bounded.SnapshotError, match='day-trade priority window'):
        bounded.TradingPriorityGuard().check(local('2026-11-07T09:00:00'))
    assert allowed_seconds(local('2026-11-07T07:55:00'), session_on=lambda _day: True) == 300
