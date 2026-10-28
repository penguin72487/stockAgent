"""An unavailable process observation is not proof of a stalled worker."""
import io
import json
import signal

import pytest

from downloader import news_fulltext_large_replay as manager


class FakeProcess:
    pid = 42

    def __init__(self):
        self.stdin = io.BytesIO()
        self.status = None
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.status

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.status = -signal.SIGKILL

    def wait(self, timeout=None):
        self.status = self.status if self.status is not None else -signal.SIGTERM
        return self.status


@pytest.fixture
def running_job(monkeypatch):
    clock = [0.0]
    process = FakeProcess()
    monkeypatch.setattr(manager.subprocess, 'Popen', lambda *args, **kwargs: process)
    job = manager.LargeReplayJob({'max_output_bytes': 1024}, clock=lambda: clock[0])
    try:
        yield job, process, clock
    finally:
        job.close()


def test_unknown_progress_does_not_kill_a_worker_that_completes(running_job, monkeypatch):
    job, process, clock = running_job
    monkeypatch.setattr(manager, '_progress', lambda pid: None)
    assert job.poll() is None
    clock[0] = manager.STALL_SECONDS + 1
    assert job.poll() is None
    assert not process.terminated
    process.status = 0
    job.output.write(json.dumps({'version': 1, 'state': 'ok'}).encode())
    assert job.poll() == {'version': 1, 'state': 'ok'}
    assert not process.killed


def test_observed_unchanged_counters_trigger_watchdog_after_a_gap(running_job, monkeypatch):
    job, process, clock = running_job
    observation = [(1, 1, 1, 1)]
    monkeypatch.setattr(manager, '_progress', lambda pid: observation[0])
    assert job.poll() is None
    clock[0] = manager.STALL_SECONDS / 2
    observation[0] = None
    assert job.poll() is None and not process.terminated
    clock[0] = manager.STALL_SECONDS + 1
    observation[0] = (1, 1, 1, 1)
    assert job.poll() is None and process.terminated
    clock[0] += manager.TERMINATE_GRACE_SECONDS + 1
    assert job.poll() is None and process.killed
    assert job.poll() == {'version': 1, 'state': 'local_io_retry', 'error': 'worker_progress_unchanged_timeout'}


def test_progress_after_unobserved_gap_resets_the_stall_clock(running_job, monkeypatch):
    job, process, clock = running_job
    observation = [(1, 1, 1, 1)]
    monkeypatch.setattr(manager, '_progress', lambda pid: observation[0])
    assert job.poll() is None
    clock[0] = manager.STALL_SECONDS + 1
    observation[0] = None
    assert job.poll() is None and not process.terminated
    clock[0] += 1
    observation[0] = (2, 1, 1, 1)
    assert job.poll() is None and not process.terminated
    clock[0] += manager.STALL_SECONDS - 1
    assert job.poll() is None and not process.terminated
    clock[0] += 2
    assert job.poll() is None and process.terminated


def test_permanently_unknown_progress_is_a_distinct_bounded_retry(running_job, monkeypatch):
    job, process, clock = running_job
    monkeypatch.setattr(manager, '_progress', lambda pid: None)
    assert job.poll() is None
    clock[0] = 900 + 1
    assert job.poll() is None and process.terminated
    clock[0] += manager.TERMINATE_GRACE_SECONDS + 1
    assert job.poll() is None and process.killed
    assert job.poll() == {
        'version': 1, 'state': 'local_io_retry', 'error': 'worker_progress_unknown_timeout'}


def test_progress_observation_includes_storage_io(monkeypatch):
    stat = ['0'] * 13
    stat[11], stat[12] = '7', '8'

    class FixturePath:
        def __init__(self, path):
            self.path = path

        def read_text(self):
            if self.path.endswith('/stat'):
                return '42 (reader) ' + ' '.join(stat)
            return 'rchar: 101\nwchar: 202\nread_bytes: 303\nwrite_bytes: 404\n'

    monkeypatch.setattr(manager, 'Path', FixturePath)
    assert manager._progress(42) == (7, 8, 101, 202, 303, 404)


def test_only_physical_read_progress_resets_the_stall_clock(running_job, monkeypatch):
    job, process, clock = running_job
    observation = [(1, 1, 1, 1, 100, 0)]
    monkeypatch.setattr(manager, '_progress', lambda pid: observation[0])
    assert job.poll() is None
    clock[0] = manager.STALL_SECONDS + 1
    observation[0] = (1, 1, 1, 1, 200, 0)
    assert job.poll() is None and not process.terminated
    clock[0] += manager.STALL_SECONDS + 1
    assert job.poll() is None and process.terminated


@pytest.mark.parametrize('error,attempts,expected', [
    (None, None, 900),
    ('worker_progress_unchanged_timeout', 100, 900),
    ('worker_progress_unknown_timeout', 1, 1800),
    ('worker_progress_unknown_timeout', 2, 3600),
    ('worker_progress_unknown_timeout', 100, 28800),
    ('worker_progress_unknown_timeout', 'bad', 900),
])
def test_unknown_timeout_grows_from_durable_retry_with_a_bound(error, attempts, expected):
    from downloader.news_fulltext_large_replay_owner import _unknown_progress_timeout

    assert _unknown_progress_timeout(error, attempts) == expected
