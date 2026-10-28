"""Real backing space must gate news without blocking the owner."""
from contextlib import nullcontext
import subprocess
import sys
from threading import Event
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import _wait_for_disk_capacity, refresh_disk_admission
from downloader.news_fulltext_disk import BackingDiskSampler, _IsolatedBackingProbe


def test_native_filesystem_needs_no_external_probe(tmp_path):
    def unexpected(_):
        raise AssertionError('native capacity must not launch a Windows query')
    sampler = BackingDiskSampler(tmp_path, wsl=False, probe=unexpected)
    assert sampler.sample(300) == dict(state='not_required', free_bytes=300,
        filesystem_free_bytes=300, backing_free_bytes=None, inflight=False)


def test_one_nonblocking_probe_clamps_virtual_free_and_does_not_duplicate(tmp_path, monkeypatch):
    started, release = Event(), Event()
    calls = []
    def probe(root):
        calls.append(root)
        started.set()
        assert release.wait(3)
        return 18
    sampler = BackingDiskSampler(tmp_path, wsl=True, probe=probe)
    queue = SimpleNamespace(root=tmp_path, _backing_disk_sampler=sampler)
    monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda _: SimpleNamespace(free=300))
    config = {'min_free_disk_bytes': 25, 'service_idle_max_seconds': 60}
    try:
        assert refresh_disk_admission(queue, config)
        assert started.wait(2) and queue.disk_admission['free_bytes'] is None
        assert queue.disk_admission['physical_backing_state'] == 'pending'
        for _ in range(5):
            assert refresh_disk_admission(queue, config)
        assert len(calls) == 1
    finally:
        release.set()
    assert sampler.wait(2)
    assert refresh_disk_admission(queue, config)
    assert queue.disk_admission['free_bytes'] == 18
    assert queue.disk_admission['filesystem_free_bytes'] == 300
    assert queue.disk_admission['shortfall_bytes'] == 7
    assert not queue.disk_admission['fetch_allowed']


def test_unavailable_probe_fails_closed_then_recovers_on_retry(tmp_path):
    clock = [0.0]
    attempts = []
    def probe(_):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError('unavailable')
        return 40
    sampler = BackingDiskSampler(tmp_path, wsl=True, probe=probe, clock=lambda: clock[0])
    assert sampler.sample(100)['state'] == 'pending'
    assert sampler.wait(2)
    failed = sampler.sample(100)
    assert failed['state'] == 'unavailable' and failed['free_bytes'] is None
    assert len(attempts) == 1
    clock[0] = 61
    assert sampler.sample(100)['inflight']
    assert sampler.wait(2)
    assert sampler.sample(100)['free_bytes'] == 40
    assert len(attempts) == 2


def test_thread_start_failure_does_not_grant_or_spin(tmp_path, monkeypatch):
    attempts = []
    def fail(*args, **kwargs):
        attempts.append(1)
        raise RuntimeError('no thread available')
    monkeypatch.setattr('downloader.news_fulltext_disk.threading.Thread', fail)
    sampler = BackingDiskSampler(tmp_path, wsl=True, clock=lambda: 0.0)
    first = sampler.sample(100)
    assert first['state'] == 'unavailable' and first['free_bytes'] is None and not first['inflight']
    assert sampler.wait(0)
    assert sampler.sample(100)['state'] == 'unavailable' and len(attempts) == 1


def test_expired_success_cannot_authorize_while_recheck_hangs(tmp_path):
    clock = [0.0]
    release = Event()
    attempts = []
    def probe(_):
        attempts.append(1)
        if len(attempts) == 2:
            assert release.wait(3)
        return 80
    sampler = BackingDiskSampler(tmp_path, wsl=True, probe=probe, clock=lambda: clock[0])
    sampler.sample(100)
    assert sampler.wait(2) and sampler.sample(100)['free_bytes'] == 80
    clock[0] = 91
    try:
        stale = sampler.sample(100)
        assert stale['state'] == 'stale' and stale['free_bytes'] is None and stale['inflight']
    finally:
        release.set()
    assert sampler.wait(2) and sampler.sample(100)['free_bytes'] == 80


def test_pending_backing_wakes_same_owner_without_dispatch():
    sampler = SimpleNamespace(wait=lambda _: True)
    queue = SimpleNamespace(_backing_disk_sampler=sampler,
        disk_admission={'physical_probe_inflight': True},
        metrics=SimpleNamespace(measure=lambda _: nullcontext()))
    http = SimpleNamespace(stop=Event())
    mailbox = SimpleNamespace(wait=lambda *a, **k: (_ for _ in ()).throw(AssertionError('no mailbox dispatch')))
    _wait_for_disk_capacity(queue, http, mailbox, 60, None)


def test_timeout_cannot_launch_another_probe_until_the_first_child_exits(tmp_path, monkeypatch):
    class UnreapableProcess:
        pid = 123456789
        stdout = None
        returncode = None
        exited = False

        def communicate(self, timeout):
            raise subprocess.TimeoutExpired('probe', timeout)

        def kill(self):
            raise PermissionError('not reaped')

        def poll(self):
            return 1 if self.exited else None

    stuck = UnreapableProcess()
    launched = []
    def start(*args, **kwargs):
        launched.append(1)
        return stuck
    def cannot_kill(*args):
        raise PermissionError('not reaped')
    monkeypatch.setattr('downloader.news_fulltext_disk.subprocess.Popen', start)
    monkeypatch.setattr('downloader.news_fulltext_disk.os.killpg', cannot_kill)
    probe = _IsolatedBackingProbe(timeout_seconds=0.1, termination_grace_seconds=0.1)
    with pytest.raises(TimeoutError, match='deadline'):
        probe(tmp_path)
    with pytest.raises(TimeoutError, match='has not exited'):
        probe(tmp_path)
    assert len(launched) == 1


@pytest.mark.parametrize('response', ['not-a-number', '-1', '9223372036854775808'])
def test_isolated_probe_rejects_bad_child_output(tmp_path, monkeypatch, response):
    real_popen = subprocess.Popen
    def start(_args, **kwargs):
        return real_popen([sys.executable, '-c', 'import sys; print(sys.argv[1])', response], **kwargs)
    monkeypatch.setattr('downloader.news_fulltext_disk.subprocess.Popen', start)
    with pytest.raises(ValueError):
        _IsolatedBackingProbe(timeout_seconds=5)(tmp_path)
