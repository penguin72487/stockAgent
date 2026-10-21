"""First readiness, recurring observation age and old owners are distinct."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data import news_worker_lifecycle as module
from stockagent.live.news_dashboard import build_news_public_status
from test_news_worker_lifecycle import seed, _save


def read(root, *, now=None):
    return module.read_news_worker_lifecycle(root, snapshot_owner_id='b' * 32,
        snapshot_at=None, now=now or datetime.now(UTC))


def test_first_snapshot_is_measured_once_with_monotonic_not_observer_age(tmp_path, monkeypatch):
    tick = [100.]
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda: tick[0]))
    lifecycle = module.NewsWorkerLifecycle(tmp_path)
    lifecycle.started('b' * 32)
    assert read(tmp_path)['initialization_wall_seconds'] is None
    tick[0] = 144.5
    lifecycle.snapshot_written()
    original = read(tmp_path)
    assert original['initialization_wall_seconds'] == 44.5
    assert original['first_snapshot_at_utc'] == lifecycle.record['first_snapshot_at_utc']
    tick[0] = 400.
    lifecycle.snapshot_written()
    later = read(tmp_path, now=datetime.now(UTC) + timedelta(hours=2))
    assert later['initialization_wall_seconds'] == 44.5
    assert later['first_snapshot_at_utc'] == original['first_snapshot_at_utc']
    assert later['elapsed_seconds'] > 7199
    lifecycle.finish()
    assert read(tmp_path)['initialization_wall_seconds'] == 44.5


def test_reusing_observer_for_new_owner_resets_first_snapshot_not_previous_duration(tmp_path, monkeypatch):
    tick = [1.]
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda: tick[0]))
    lifecycle = module.NewsWorkerLifecycle(tmp_path)
    lifecycle.started('a' * 32); tick[0] = 10.; lifecycle.snapshot_written()
    lifecycle.started('b' * 32)
    assert read(tmp_path)['initialization_wall_seconds'] is None
    tick[0] = 12.; lifecycle.snapshot_written()
    assert read(tmp_path)['initialization_wall_seconds'] == 2.


def test_old_owner_does_not_acquire_first_snapshot_duration_from_later_ready(tmp_path):
    lifecycle = module.NewsWorkerLifecycle(tmp_path)
    lifecycle.started('b' * 32); lifecycle.snapshot_written()
    old = {key: value for key, value in lifecycle.record.items() if key not in {
        'first_snapshot_timing_contract_version', 'first_snapshot_at_utc', 'initialization_wall_seconds'}}
    atomic_write_json(lifecycle.path, old)
    value = read(tmp_path, now=datetime.now(UTC) + timedelta(hours=12))
    assert value['available'] and value['process_alive'] and value['state'] == 'ready'
    assert value['first_snapshot_at_utc'] is value['initialization_wall_seconds'] is None


@pytest.mark.parametrize('change', [
    {'first_snapshot_timing_contract_version': True},
    {'first_snapshot_timing_contract_version': 2},
    {'initialization_wall_seconds': True}, {'initialization_wall_seconds': -1},
    {'initialization_wall_seconds': float('nan')}, {'initialization_wall_seconds': float('inf')},
    {'initialization_wall_seconds': 'PRIVATE_TIMING'},
    {'first_snapshot_at_utc': 'PRIVATE_CLOCK'},
    {'first_snapshot_at_utc': '2026-10-01T01:00:00'},
    {'first_snapshot_at_utc': '2000-01-01T01:00:00+00:00'},
    {'first_snapshot_at_utc': '9999-01-01T01:00:00+00:00'},
    {'first_snapshot_at_utc': None},
    {'state': 'initializing', 'phase': 'acceptance_recovery'},
])
def test_malformed_timing_cannot_publish_invented_startup_metrics(tmp_path, change):
    lifecycle = module.NewsWorkerLifecycle(tmp_path)
    lifecycle.started('b' * 32); lifecycle.snapshot_written()
    atomic_write_json(lifecycle.path, {**lifecycle.record, **change})
    result = read(tmp_path)
    assert result['available'] and result['process_alive']
    assert result['first_snapshot_at_utc'] is result['initialization_wall_seconds'] is None
    assert not {'owner_id', 'pid', 'boot_id', 'process_owner'} & result.keys()


def test_telemetry_write_error_does_not_move_first_ready_to_successful_retry(tmp_path, monkeypatch):
    tick = [100.]
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda: tick[0]))
    lifecycle = module.NewsWorkerLifecycle(tmp_path); lifecycle.started('b' * 32)
    original = atomic_write_json
    def failed(*args, **kwargs):
        raise OSError('telemetry only')
    monkeypatch.setattr('downloader.artifact_io.atomic_write_json', failed)
    tick[0] = 120.; lifecycle.snapshot_written()
    first = lifecycle.record['first_snapshot_at_utc']
    assert lifecycle.write_errors == 1
    monkeypatch.setattr('downloader.artifact_io.atomic_write_json', original)
    tick[0] = 180.; lifecycle.snapshot_written()
    result = read(tmp_path)
    assert result['initialization_wall_seconds'] == 20.
    assert result['first_snapshot_at_utc'] == first


def test_public_projection_adds_validated_timing_without_promoting_old_snapshot(tmp_path):
    old, lifecycle = seed(tmp_path)
    lifecycle.snapshot_written()
    result = build_news_public_status(tmp_path)
    assert result['worker_lifecycle_projection_contract_version'] == 2
    assert result['health'] == 'initializing' and not result['worker']['fresh']
    old.update(owner_id='b' * 32, activity='running', updated_at_utc=datetime.now(UTC).isoformat())
    _save(tmp_path, old)
    result = build_news_public_status(tmp_path)
    assert result['health'] == 'running' and result['worker']['fresh']
    assert result['worker']['lifecycle']['first_snapshot_at_utc']
    assert result['worker']['lifecycle']['initialization_wall_seconds'] >= 0
