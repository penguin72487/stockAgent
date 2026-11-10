from __future__ import annotations

import os
from pathlib import Path
import sqlite3

import pytest

from stockagent.data_sync import source_mutation_fence as fence_module
from stockagent.data_sync.source_mutation_fence import SourceMutationFence
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def scope(tmp_path):
    roots = [tmp_path/'source', tmp_path/'destination']
    for root in roots:
        (root/'nested').mkdir(parents=True)
        (root/'nested'/'member').write_bytes(b'fixed')
        (root/'.download.lock').write_bytes(b'')
    index = tmp_path/'files.sqlite'
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE files(path TEXT PRIMARY KEY,kind TEXT)')
        db.execute("INSERT INTO files VALUES ('nested','directory')")
    return roots, index


def test_ignores_own_directory_and_empty_lock_reads_but_detects_payload_access(scope):
    (source, dest), index = scope
    with SourceMutationFence(source,dest,index) as fence:
        with os.scandir(source/'nested') as entries:
            list(entries)
        with (source/'.download.lock').open('rb'):
            pass
        fence.check()
        with (dest/'nested'/'member').open('rb') as handle:
            assert handle.read() == b'fixed'
        with pytest.raises(BlockingIOError, match='namespace changed'):
            fence.check()


def test_detects_writer_that_had_opened_payload_before_fence(scope):
    (source, dest), index = scope
    with (source/'nested'/'member').open('r+b', buffering=0) as writer:
        with SourceMutationFence(source,dest,index) as fence:
            writer.write(b'x')
            with pytest.raises(BlockingIOError, match='namespace changed'):
                fence.check()


def test_detects_unindexed_new_directory(scope):
    (source, dest), index = scope
    with SourceMutationFence(source,dest,index) as fence:
        (source/'nested'/'unexpected').mkdir()
        with pytest.raises(BlockingIOError, match='namespace changed'):
            fence.check()


def test_only_source_lock_hardlink_attribute_and_source_root_move_are_expected(scope, tmp_path):
    (source, dest), index = scope
    with SourceMutationFence(source,dest,index) as fence:
        os.link(source/'.download.lock', tmp_path/'placeholder-lock')
        fence.check(allow_lock_attrib=True)
        os.rename(source, tmp_path/'renamed-source')
        fence.check(allow_root_move=True)
    with SourceMutationFence(tmp_path/'renamed-source',dest,index) as fence:
        os.link(dest/'.download.lock', tmp_path/'other-lock')
        with pytest.raises(BlockingIOError, match='coordination lock changed'):
            fence.check(allow_lock_attrib=True)


def test_event_queue_overflow_is_never_treated_as_a_clean_fence(scope, monkeypatch):
    (source, dest), index = scope
    with SourceMutationFence(source,dest,index) as fence:
        raw = fence_module._EVENT.pack(-1, fence_module._Q_OVERFLOW, 0, 0)
        monkeypatch.setattr(fence_module.os, 'read', lambda *_: raw)
        with pytest.raises(BlockingIOError, match='lost event coverage'):
            fence.check()


def test_reviewed_directory_limit_is_explicit_and_bounded(scope):
    (source, dest), index = scope
    with SourceMutationFence(source, dest, index, max_directories=1) as fence:
        fence.check()
    with pytest.raises(SnapshotError, match='limit is invalid'):
        SourceMutationFence(source, dest, index, max_directories=200001)
    with pytest.raises(SnapshotError, match='limit is invalid'):
        SourceMutationFence(source, dest, index, max_directories=True)


def test_reports_fence_install_without_changing_mutation_detection(scope):
    (source, dest), index = scope
    progress = []
    with SourceMutationFence(source, dest, index, progress=lambda done, total: progress.append((done, total))) as fence:
        assert progress == [(0, 1), (1, 1)]
        (source/'nested'/'member').write_bytes(b'changed')
        with pytest.raises(BlockingIOError, match='namespace changed'):
            fence.check()


def test_reviewed_large_scope_does_not_truncate_index(tmp_path, monkeypatch):
    source, dest = tmp_path/'source', tmp_path/'dest'
    source.mkdir(); dest.mkdir()
    (dest/'.download.lock').touch()
    index = tmp_path/'index.sqlite'
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE files(path TEXT PRIMARY KEY,kind TEXT)')
        db.executemany('INSERT INTO files VALUES (?,?)',
                       ((f'directory-{n}', 'directory') for n in range(20001)))
    # Count every intended watch without allocating 40k kernel watches in the
    # unit test. A separate isolated mount test exercises the real fence.
    watched = []
    monkeypatch.setattr(SourceMutationFence, '_watch', lambda self, path, **kw: watched.append(path))
    monkeypatch.setattr(SourceMutationFence, 'check', lambda self, **kw: None)
    with pytest.raises(SnapshotError, match='directory limit exceeded'):
        with SourceMutationFence(source, dest, index):
            pass
    watched.clear()
    with SourceMutationFence(source, dest, index, max_directories=20001):
        pass
    assert len(watched) == 2 + 2*20001


def test_rejects_missing_or_redirected_indexed_directory(scope):
    (source, dest), index = scope
    (source/'nested'/'member').unlink()
    (source/'nested').rmdir()
    (source/'nested').symlink_to(dest/'nested', target_is_directory=True)
    with pytest.raises(SnapshotError, match='missing or redirected'):
        with SourceMutationFence(source,dest,index):
            pass
