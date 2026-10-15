from datetime import UTC, date, datetime, timedelta
import json

import pytest

from downloader import finmind_runtime as runtime
from downloader.finmind_scheduling import stock_opening_protection_deadline
from stockagent.live.market_status import TwStockDayDecision

START = datetime(2026, 10, 15, 0, 25, tzinfo=UTC)
END = START.replace(hour=1, minute=10)


@pytest.mark.parametrize('stamp,decision,expected', [
    (START, 'actual_open', END), (START, 'unknown', END), (START, 'closed', None),
    (START.replace(minute=19), 'actual_open', None), (START.replace(minute=20), 'actual_open', END),
    (END - timedelta(microseconds=1), 'actual_open', END), (END, 'actual_open', None),
    (START + timedelta(days=2), 'unknown', None),
    (START + timedelta(days=2), 'scheduled_open', END + timedelta(days=2)),
])
def test_deadline_preserves_exact_existing_calendar_gate(stamp, decision, expected):
    assert stock_opening_protection_deadline(stamp,
        day_decision=lambda *_a, **_k: TwStockDayDecision(decision, 'fixture')) == expected


def simulate(tmp_path, monkeypatch, *, deadline=None, release_at=None, seconds=600):
    ticks, heartbeats = [0.0], []
    status = {'state': 'protected_opening', 'observed_at_utc': START.isoformat(),
              'last_task': {'observed_at_utc': '2026-10-14T23:00:00+00:00'}}
    original = json.dumps(status)
    (tmp_path / 'status.json').write_text(original)
    monkeypatch.setattr(runtime, 'next_cycle_delay', lambda *_a, **_k: pytest.fail('large idle queue scan'))
    monkeypatch.setattr(runtime, '_backfill_ready', lambda *_a: pytest.fail('opening quota polling'))
    monkeypatch.setattr(runtime, 'stock_opening_protection_deadline', deadline or (
        lambda now: END if release_at is None or ticks[0] < release_at else None))

    def sleep(delay):
        value = json.loads((tmp_path / 'worker_status.json').read_bytes())
        assert runtime.idle_heartbeat(tmp_path, status, START + timedelta(seconds=ticks[0]))['alive']
        heartbeats.append(value)
        ticks[0] += delay

    runtime.wait_for_next_cycle(tmp_path, status, seconds,
                               clock=lambda: START + timedelta(seconds=ticks[0]),
                               sleep=sleep, monotonic=lambda: ticks[0])
    assert (tmp_path / 'status.json').read_text() == original
    assert json.loads((tmp_path / 'worker_status.json').read_bytes())['phase'] == 'running'
    return ticks[0], heartbeats


def test_keep_heartbeat_until_gate_ends_without_reinitializing_queue(tmp_path, monkeypatch):
    elapsed, beats = simulate(tmp_path, monkeypatch)
    assert elapsed == 2700  # 08:25 -> 09:10, not repeated 600s seed cycles.
    assert len(beats) == 90
    assert {beat['next_check_at_utc'] for beat in beats} == {END.isoformat()}
    assert {beat['cycle_observed_at_utc'] for beat in beats} == {START.isoformat()}


def test_emergency_closure_releases_at_next_existing_heartbeat(tmp_path, monkeypatch):
    elapsed, _ = simulate(tmp_path, monkeypatch, release_at=45)
    assert elapsed == 60


def test_stale_protected_result_does_not_extend_wait(tmp_path, monkeypatch):
    assert simulate(tmp_path, monkeypatch, deadline=lambda now: None) == (0, [])


def test_free_calendar_refresh_keeps_its_independent_deadline(tmp_path, monkeypatch):
    calendar = {'observed_at_utc': (START - timedelta(minutes=55)).isoformat(),
                'dates': [date(2026, 10, 15).isoformat()]}
    (tmp_path / 'calendar.json').write_text(json.dumps(calendar))
    elapsed, beats = simulate(tmp_path, monkeypatch)
    assert elapsed == 300
    assert beats[-1]['next_check_at_utc'] == (START + timedelta(seconds=300)).isoformat()


def test_calendar_failure_retry_is_not_turned_into_busy_loop(tmp_path, monkeypatch):
    (tmp_path / 'calendar.json').write_text(json.dumps({
        'observed_at_utc': (START - timedelta(hours=2)).isoformat()}))
    (tmp_path / 'calendar_refresh.json').write_text(json.dumps({
        'observed_at_utc': START.isoformat(), 'retry_at_utc': (START + timedelta(minutes=2)).isoformat()}))
    assert simulate(tmp_path, monkeypatch)[0] == 120


def test_zero_delay_still_means_no_wait(tmp_path, monkeypatch):
    assert simulate(tmp_path, monkeypatch, seconds=0) == (0, [])
