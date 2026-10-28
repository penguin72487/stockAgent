"""Capacity admission keeps every retained byte and descriptor fence."""
import os
from types import SimpleNamespace
import threading

import pytest

from stockagent.data_sync import transport_capacity as candidate
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_deep_independent_subtrees_execute_concurrently(tmp_path, monkeypatch):
    for a in range(65):
        p = tmp_path / 'one-large-top-branch' / f'branch-{a}' / 'narrow' / 'deep'
        p.mkdir(parents=True)
        (p / 'raw').write_bytes(b'original')
    actual = candidate.os.scandir
    gate = threading.Barrier(2, timeout=5)
    entered = []
    lock = threading.Lock()

    def scan(fd):
        first = False
        if isinstance(fd, int) and os.readlink(f'/proc/self/fd/{fd}').endswith('/deep'):
            with lock:
                if len(entered) < 2:
                    entered.append(threading.get_ident())
                    first = True
        if first:
            gate.wait()
        return actual(fd)

    monkeypatch.setattr(candidate.os, 'scandir', scan)
    before = set(os.listdir('/proc/self/fd'))
    assert candidate.transport_usage(tmp_path) == {
        'retained_transport_bytes': 65 * len(b'original'), 'retained_staging_bytes': 0}
    assert len(set(entered)) == 2
    assert set(os.listdir('/proc/self/fd')) == before


def test_frontier_depth_exceeding_window_completes_without_leaks(tmp_path, monkeypatch):
    monkeypatch.setattr(candidate, 'MAX_OPEN_DIRECTORY_FRONTIER', 3)
    for a in range(8):
        p = tmp_path / f'branch-{a}'
        for depth in range(12):
            p = p / f'narrow-{depth}'
            p.mkdir(parents=True)
        (p / 'raw').write_bytes(b'original')
    before = set(os.listdir('/proc/self/fd'))
    assert candidate.transport_usage(tmp_path) == {
        'retained_transport_bytes': 8 * len(b'original'), 'retained_staging_bytes': 0}
    assert set(os.listdir('/proc/self/fd')) == before


def test_failed_subtree_joins_other_live_subtrees_before_closing_ancestors(tmp_path, monkeypatch):
    for name in ['good', 'bad']:
        (tmp_path / name).mkdir()
    (tmp_path / 'good' / 'blocked').write_bytes(b'original')
    (tmp_path / 'bad' / 'unsafe').symlink_to('/outside-do-not-follow')
    actual = candidate.os.stat
    blocked, failed, release, finished = [threading.Event() for _ in range(4)]
    held = []
    result = {}

    def observe(name, *args, **kwargs):
        if name == 'blocked' and kwargs.get('dir_fd') is not None:
            held.append(kwargs['dir_fd'])
            blocked.set()
            assert release.wait(5)
        elif name == 'unsafe' and kwargs.get('dir_fd') is not None:
            assert blocked.wait(5)
            failed.set()
        return actual(name, *args, **kwargs)

    def run():
        try:
            candidate.transport_usage(tmp_path)
        except BaseException as error:
            result['error'] = error
        finally:
            finished.set()

    monkeypatch.setattr(candidate.os, 'stat', observe)
    before = set(os.listdir('/proc/self/fd'))
    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert failed.wait(5)
        assert not finished.wait(0.05)
        assert os.fstat(held[0]).st_ino == (tmp_path / 'good').stat().st_ino
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert isinstance(result.get('error'), SnapshotError)
    assert set(os.listdir('/proc/self/fd')) == before
    assert (tmp_path / 'good' / 'blocked').read_bytes() == b'original'


def test_separate_directory_and_stat_worker_windows_are_bounded(tmp_path, monkeypatch):
    for i in range(260):
        branch = tmp_path / f'branch-{i}'
        branch.mkdir()
        (branch / 'raw').write_bytes(b'original')
    original_pool = candidate.ThreadPoolExecutor
    pools = []

    class ObservedPool(original_pool):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.pending = set()
            self.peak = 0
            self.guard = threading.Lock()
            self.kind = kwargs['thread_name_prefix']
            pools.append(self)

        def submit(self, *args, **kwargs):
            with self.guard:
                future = super().submit(*args, **kwargs)
                self.pending = {f for f in self.pending if not f.done()}
                if not future.done():
                    self.pending.add(future)
                self.peak = max(self.peak, len(self.pending))
            return future

    monkeypatch.setattr(candidate, 'ThreadPoolExecutor', ObservedPool)
    assert candidate.transport_usage(tmp_path) == {
        'retained_transport_bytes': 260 * len(b'original'), 'retained_staging_bytes': 0}
    assert len(pools) == 2
    for pool in pools:
        assert all(f.done() for f in pool.pending)
        assert pool._max_workers == candidate.WORKERS
        assert pool.peak <= (candidate.BATCH_REFERENCES if pool.kind.endswith('stat') else candidate.WORKERS)


def test_capacity_counts_unknown_failed_and_nested_staging(tmp_path):
    members = {
        "unknown/raw": b"u" * 11,
        ".staging/failed/partial": b"f" * 23,
        ".staging/orphan": b"o" * 7,
        "closed/.staging/raw": b"c" * 17,
    }
    for name, raw in members.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    assert candidate.transport_usage(tmp_path) == {
        "retained_transport_bytes": 58, "retained_staging_bytes": 30,
    }
    assert {p.relative_to(tmp_path).as_posix(): p.read_bytes()
            for p in tmp_path.rglob("*") if p.is_file()} == members


def test_capacity_rejects_another_mounted_volume_without_mutation(tmp_path, monkeypatch):
    branch = tmp_path / "mounted"
    branch.mkdir()
    raw = branch / "raw"
    raw.write_bytes(b"original")
    actual_stat = candidate.os.stat

    def another_device(name, *args, **kwargs):
        info = actual_stat(name, *args, **kwargs)
        if name == "mounted" and kwargs.get("dir_fd") is not None:
            return SimpleNamespace(st_dev=info.st_dev + 1, st_ino=info.st_ino,
                                   st_mode=info.st_mode, st_size=info.st_size)
        return info

    monkeypatch.setattr(candidate.os, "stat", another_device)
    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(SnapshotError, match="another mounted volume"):
        candidate.transport_usage(tmp_path)
    assert raw.read_bytes() == b"original"
    assert set(os.listdir("/proc/self/fd")) == before

