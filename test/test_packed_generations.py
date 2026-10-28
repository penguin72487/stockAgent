"""Concurrency, ordered failures and non-verification metadata progress."""
import os
from pathlib import Path
import threading

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('count', [0, 1, 127, 128, 385])
def test_object_generations_capture_complete_ordered_reference_set_once(tmp_path, monkeypatch, count):
    paths = [tmp_path / f'object-{i:04d}' for i in range(count)]
    for path in paths:
        path.write_bytes(b'original')
    original, seen, lock = Path.lstat, [], threading.Lock()

    def observe(path):
        with lock:
            seen.append(path)
        return original(path)

    monkeypatch.setattr(Path, 'lstat', observe)
    progress = []
    rows = packed._collect_object_signatures(paths, progress=progress.append)
    assert list(rows) == paths
    assert sorted(seen) == paths
    assert all(info.st_size == 8 for info in rows.values())
    assert progress[0]['completed_items'] == 0
    assert progress[-1]['completed_items'] == progress[-1]['total_items'] == count
    assert all(row['metadata_only'] is True and row['completed_bytes'] == row['total_bytes'] == 0
               and row['stage'] == 'capture-cold-object-generations' for row in progress)


def test_object_generations_bound_pending_work_preserve_first_error_and_join_threads(tmp_path, monkeypatch):
    paths = [tmp_path / f'object-{i:04d}' for i in range(385)]
    native = Path.lstat
    first, second, lock = threading.Event(), threading.Event(), threading.Lock()
    active = peak = 0
    seen = []

    def observe(path):
        nonlocal active, peak
        index = int(path.name.removeprefix('object-'))
        with lock:
            active += 1
            peak = max(peak, active)
            seen.append(index)
        try:
            if index == 0:
                first.set()
                assert second.wait(10), 'generation probes were serialized'
                raise FileNotFoundError('first missing object')
            if index == 1:
                assert first.wait(10)
                second.set()
                raise PermissionError('second unreadable object')
            return native(tmp_path)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(Path, 'lstat', observe)
    events = []
    with pytest.raises(FileNotFoundError, match='first missing object'):
        packed._collect_object_signatures(paths, progress=events.append)
    assert 2 <= peak <= packed.OBJECT_PRESENCE_WORKERS
    assert max(seen) < packed.OBJECT_PRESENCE_BATCH_REFERENCES
    assert active == 0
    assert all(row['completed_items'] != row['total_items'] for row in events)


def test_object_generations_keep_lstat_identity_of_redirected_leaf(tmp_path):
    target = tmp_path / 'original'
    target.write_bytes(b'original')
    redirect = tmp_path / 'redirect'
    redirect.symlink_to(target)
    rows = packed._collect_object_signatures([redirect])
    assert rows[redirect].st_ino == redirect.lstat().st_ino
    assert rows[redirect].st_ino != target.stat().st_ino


@pytest.mark.parametrize('fault', [None, 'replace-identical', 'hardlink', 'mode', 'mtime', 'missing'])
def test_complete_chunk_recovery_keeps_before_after_generations_and_content_gates(tmp_path, monkeypatch, fault):
    source, cold = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    payload = b''.join(bytes([i]) * 16 for i in range(129))
    (source / 'original.bin').write_bytes(payload)
    release = packed.publish_packed_snapshot(cold, 'provider', source, node_id='test',
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2)
    events = []
    original = packed._iter_chunked_file
    target = cold / release.manifest['archive']['objects'][0]['relpath']

    def observe(*args, **options):
        captures = [row for row in events if row['stage'] == 'capture-cold-chunk-generations']
        assert captures[-1]['completed_items'] == captures[-1]['total_items'] == 129
        assert all(row['metadata_only'] for row in captures)
        yield from original(*args, **options)
        if fault == 'replace-identical':
            replacement = target.with_suffix('.replacement')
            replacement.write_bytes(target.read_bytes())
            os.replace(replacement, target)
        elif fault == 'hardlink':
            os.link(target, tmp_path / 'additional-name')
        elif fault == 'mode':
            target.chmod(0o400 if target.stat().st_mode & 0o077 else 0o444)
        elif fault == 'mtime':
            info = target.stat()
            os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
        elif fault == 'missing':
            target.unlink()

    monkeypatch.setattr(packed, '_iter_chunked_file', observe)
    if fault:
        with pytest.raises((SnapshotError, OSError)):
            packed.verify_packed_snapshot(cold, release, reconstruct_all=True, progress=events.append)
        assert not any(row['stage'] == 'verify-cold-objects'
            and row['completed_items'] == row['total_items'] for row in events)
    else:
        proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True, progress=events.append)
        assert proof['independently_reconstructed_files'] == 1
        assert proof['independently_reconstructed_bytes'] == len(payload)
        assert proof['objects'] == 129
        rechecks = [row for row in events if row['stage'] == 'recheck-cold-chunk-generations']
        assert rechecks[-1]['completed_items'] == rechecks[-1]['total_items'] == 129
        assert events[-1]['stage'] == 'verify-cold-objects'
        assert events[-1]['completed_items'] == events[-1]['total_items'] == 129
