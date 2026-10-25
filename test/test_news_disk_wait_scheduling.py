"""Separate blocked HTTP scheduling from live local inventory and heartbeats."""
from contextlib import nullcontext
from types import SimpleNamespace
import threading

import pytest

from downloader.news_fulltext import run_service


def service(monkeypatch, *, duration=62, stop_at_metadata=None, inventory_complete_at=None,
            metadata_pending=True, capacity_ready_at=None):
    clock, cycles, metadata, statuses, waits = [0.0], [], [], [], []
    stop = threading.Event()
    queue = SimpleNamespace(metrics=SimpleNamespace(measure=lambda *args: nullcontext()),
                            metadata_inventory_pending=metadata_pending)
    http = SimpleNamespace(stop=stop, capacity_snapshot=lambda: {}, cooldown_snapshot=lambda: {},
                           request_scope_cooldown_snapshot=lambda: {}, requests_by_provider={}, response_bytes=0)

    def status(**kwargs):
        statuses.append((clock[0], kwargs['activity']))
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
        return {'activity': 'waiting_disk'}

    def refresh():
        metadata.append(clock[0])
        if stop_at_metadata is not None and clock[0] >= stop_at_metadata:
            stop.set()  # Includes changed configuration/expired authorization.
        if inventory_complete_at is not None and clock[0] >= inventory_complete_at:
            queue.metadata_inventory_pending = False

    monkeypatch.setattr('downloader.news_fulltext.time.monotonic', lambda: clock[0])
    monkeypatch.setattr('downloader.news_fulltext.run_cycle', cycle)
    monkeypatch.setattr('downloader.news_fulltext_wake.OwnerMailboxWake', Mailbox)
    result = run_service(queue, http, {'workers': 1, 'service_idle_max_seconds': 60},
        max_articles=1, max_discovery_requests=1, duration_seconds=duration, refresh_metadata=refresh)
    return SimpleNamespace(result=result, cycles=cycles, metadata=metadata, statuses=statuses, waits=waits, clock=clock[0])


def test_pending_metadata_keeps_slices_without_restarting_blocked_cycle(monkeypatch):
    actual = service(monkeypatch)
    assert actual.cycles == [0, 60]
    assert actual.metadata == list(range(0, 62))
    assert actual.statuses == [(15, 'waiting_disk'), (30, 'waiting_disk'), (45, 'waiting_disk'),
                               (62, 'service_duration_complete')]
    assert actual.clock == 62
    assert all(not enabled for _, _, enabled in actual.waits)


def test_metadata_stop_interrupts_before_any_extra_cycle_or_source_dispatch(monkeypatch):
    actual = service(monkeypatch, stop_at_metadata=4)
    assert actual.cycles == [0] and actual.metadata == [0, 1, 2, 3, 4]
    assert actual.clock == 4 and actual.result['activity'] == 'stopped'


def test_completed_inventory_stops_slices_and_keeps_disk_and_heartbeat_wakes(monkeypatch):
    actual = service(monkeypatch, inventory_complete_at=4)
    assert actual.metadata == [0, 1, 2, 3, 4, 60]
    assert actual.cycles == [0, 60]
    assert [when for when, activity in actual.statuses if activity == 'waiting_disk'] == [15, 30, 45]


def test_capacity_recovery_reenters_original_scheduler_at_configured_boundary(monkeypatch):
    actual = service(monkeypatch, capacity_ready_at=60)
    assert actual.cycles == [0, 60] and actual.clock == 60
    assert actual.result['activity'] == 'stopped'


def test_no_pending_inventory_has_no_extra_local_tick_or_mailbox_pump(monkeypatch):
    actual = service(monkeypatch, metadata_pending=False)
    assert actual.metadata == [0, 60]
    assert [seconds for when, seconds, enabled in actual.waits] == [15, 15, 15, 15, 2]
