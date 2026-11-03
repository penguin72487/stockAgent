"""Separate blocked HTTP scheduling from live local inventory and heartbeats."""
from contextlib import nullcontext
from types import SimpleNamespace
import threading

import pytest

from downloader.news_fulltext import _coalesce_inventory_heartbeats, run_service


def service(monkeypatch, *, duration=62, stop_at_metadata=None, inventory_complete_at=None,
            metadata_pending=True, capacity_ready_at=None, service_idle_seconds=60):
    clock, cycles, metadata, statuses, waits, status_arguments, metadata_flags = [0.0], [], [], [], [], [], []
    stop = threading.Event()
    queue = SimpleNamespace(metrics=SimpleNamespace(measure=lambda *args: nullcontext()),
                            metadata_inventory_pending=metadata_pending)
    http = SimpleNamespace(stop=stop, capacity_snapshot=lambda: {}, cooldown_snapshot=lambda: {},
                           scheduling_cooldown_snapshot=lambda: {},
                           request_scope_cooldown_snapshot=lambda: {}, requests_by_provider={}, response_bytes=0,
                           traffic_snapshot=lambda: ({}, 0))

    def status(**kwargs):
        statuses.append((clock[0], kwargs['activity']))
        status_arguments.append(kwargs)
        return {'activity': kwargs['activity']}

    queue.status = status

    class Mailbox:
        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def has_pending(self):
            pytest.fail('blocked disk must not pump the durable mailbox')

        def wait(self, seconds, *, enabled=True):
            assert seconds > 0  # No zero-time busy spin at a due local tick.
            waits.append((clock[0], seconds, enabled))
            clock[0] += seconds
            return False

    def cycle(*args, **kwargs):
        cycles.append(clock[0])
        if capacity_ready_at is not None and clock[0] >= capacity_ready_at:
            stop.set()  # Next original cycle can dispatch immediately.
        return {'activity': 'waiting_disk', 'cycle': {'wall_seconds': 2,
                'article_attempts_started': 4, 'durably_accepted_versions': 3,
                'http_requests': {'publisher': 4}}}

    def refresh():
        metadata.append(clock[0])
        metadata_flags.append(getattr(queue, '_coalesce_inventory_heartbeats', False))
        if stop_at_metadata is not None and clock[0] >= stop_at_metadata:
            stop.set()  # Includes changed configuration/expired authorization.
        if inventory_complete_at is not None and clock[0] >= inventory_complete_at:
            queue.metadata_inventory_pending = False

    monkeypatch.setattr('downloader.news_fulltext.time.monotonic', lambda: clock[0])
    monkeypatch.setattr('downloader.news_fulltext.run_cycle', cycle)
    monkeypatch.setattr('downloader.news_fulltext_wake.OwnerMailboxWake', Mailbox)
    result = run_service(queue, http, {'workers': 1, 'service_idle_max_seconds': service_idle_seconds},
        max_articles=1, max_discovery_requests=1, duration_seconds=duration, refresh_metadata=refresh)
    return SimpleNamespace(result=result, cycles=cycles, metadata=metadata, statuses=statuses, waits=waits,
                           clock=clock[0], status_arguments=status_arguments, metadata_flags=metadata_flags,
                           queue=queue)


def test_pending_metadata_keeps_slices_without_restarting_blocked_cycle(monkeypatch):
    actual = service(monkeypatch)
    assert actual.cycles == [0, 60]
    assert actual.metadata == list(range(0, 62))
    assert actual.statuses == [(0, 'waiting_disk'), (60, 'waiting_disk'), (62, 'service_duration_complete')]
    assert actual.clock == 62
    assert all(not enabled for _, _, enabled in actual.waits)
    assert actual.metadata_flags[0] is False and actual.metadata_flags[60] is False
    assert all(actual.metadata_flags[1:60]) and actual.metadata_flags[61] is True
    assert not hasattr(actual.queue, '_coalesce_inventory_heartbeats')


def test_inventory_heartbeat_scope_restores_outer_state_after_exception():
    queue = SimpleNamespace()
    with pytest.raises(RuntimeError):
        with _coalesce_inventory_heartbeats(queue):
            assert queue._coalesce_inventory_heartbeats is True
            with _coalesce_inventory_heartbeats(queue):
                raise RuntimeError('inventory failure')
    assert not hasattr(queue, '_coalesce_inventory_heartbeats')
    queue._coalesce_inventory_heartbeats = False
    with _coalesce_inventory_heartbeats(queue):
        assert queue._coalesce_inventory_heartbeats is True
    assert queue._coalesce_inventory_heartbeats is False


def test_disk_wait_heartbeats_keep_completed_cycle_without_reported_active_rate(monkeypatch):
    actual = service(monkeypatch, duration=61, metadata_pending=False, service_idle_seconds=120)
    waiting = [args for args in actual.status_arguments if args['activity'] == 'waiting_disk']
    assert len(waiting) == 2  # Initial transition plus one minute heartbeat.
    assert all(args['cycle'] == waiting[0]['cycle'] for args in waiting)
    assert waiting[0]['run_seconds'] == 2 and waiting[0]['accepted_this_run'] == 3
    assert all('run_seconds' not in args and 'accepted_this_run' not in args for args in waiting[1:])
    assert actual.cycles == [0] and all(not enabled for _, _, enabled in actual.waits)


def test_metadata_stop_interrupts_before_any_extra_cycle_or_source_dispatch(monkeypatch):
    actual = service(monkeypatch, stop_at_metadata=4)
    assert actual.cycles == [0] and actual.metadata == [0, 1, 2, 3, 4]
    assert actual.clock == 4 and actual.result['activity'] == 'stopped'


def test_completed_inventory_stops_slices_and_keeps_disk_and_heartbeat_wakes(monkeypatch):
    actual = service(monkeypatch, inventory_complete_at=4)
    assert actual.metadata == [0, 1, 2, 3, 4, 60]
    assert actual.cycles == [0, 60]
    assert [when for when, activity in actual.statuses if activity == 'waiting_disk'] == [0, 60]


def test_capacity_recovery_reenters_original_scheduler_at_configured_boundary(monkeypatch):
    actual = service(monkeypatch, capacity_ready_at=60)
    assert actual.cycles == [0, 60] and actual.clock == 60
    assert actual.result['activity'] == 'stopped'


def test_no_pending_inventory_has_no_extra_local_tick_or_mailbox_pump(monkeypatch):
    actual = service(monkeypatch, metadata_pending=False)
    assert actual.metadata == [0, 60]
    assert [seconds for when, seconds, enabled in actual.waits] == [60, 2]


def test_disk_wait_heartbeat_remains_minutely_with_longer_capacity_delay(monkeypatch):
    actual = service(monkeypatch, duration=125, metadata_pending=False, service_idle_seconds=120)
    assert actual.cycles == [0, 120]
    assert [when for when, activity in actual.statuses if activity == 'waiting_disk'] == [0, 60, 120]
    assert [seconds for _, seconds, _ in actual.waits] == [60, 60, 5]
    assert all(not enabled for _, _, enabled in actual.waits)


@pytest.mark.parametrize('blocked_at', [None, 30])
def test_idle_heartbeats_refresh_capacity_and_return_to_disk_wait(monkeypatch, blocked_at):
    """An idle heartbeat must not make an old capacity observation current."""
    clock, checks, cycles, statuses, waits = [0.0], [], [], [], []
    stop = threading.Event()
    queue = SimpleNamespace(metrics=SimpleNamespace(measure=lambda *args: nullcontext()),
                            metadata_inventory_pending=False, reviewed_repair_pending=False,
                            next_wake_delay=lambda *args, **kwargs: 60)
    http = SimpleNamespace(stop=stop, capacity_snapshot=lambda: {}, cooldown_snapshot=lambda: {},
                           scheduling_cooldown_snapshot=lambda: {},
                           request_scope_cooldown_snapshot=lambda: {}, requests_by_provider={}, response_bytes=0,
                           traffic_snapshot=lambda: ({}, 0))
    queue.status = lambda **kwargs: statuses.append((clock[0], kwargs['activity'])) or {'activity': kwargs['activity']}

    class Mailbox:
        def __init__(self, *args): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def has_pending(self): return False
        def wait(self, seconds, *, enabled=True):
            assert seconds > 0
            waits.append((clock[0], enabled))
            clock[0] += seconds
            return False

    def cycle(*args, **kwargs):
        cycles.append(clock[0])
        return {'activity': 'waiting_disk' if blocked_at is not None and clock[0] >= blocked_at else 'cycle_complete'}

    def refresh(*args):
        checks.append(clock[0])
        return blocked_at is not None and clock[0] >= blocked_at

    monkeypatch.setattr('downloader.news_fulltext.time.monotonic', lambda: clock[0])
    monkeypatch.setattr('downloader.news_fulltext.run_cycle', cycle)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', refresh)
    monkeypatch.setattr('downloader.news_fulltext_wake.OwnerMailboxWake', Mailbox)
    run_service(queue, http, {'workers': 1, 'service_idle_max_seconds': 60},
                max_articles=1, max_discovery_requests=1, duration_seconds=61)
    if blocked_at is None:
        assert checks == [0, 15, 30, 45, 60]
        assert cycles == [0, 60]
    else:
        assert checks == [0, 15, 30] and cycles == [0, 30]
        assert (30, 'waiting_disk') in statuses
        assert all(not enabled for when, enabled in waits if when >= 30)
