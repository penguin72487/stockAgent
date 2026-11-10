from __future__ import annotations

import fcntl
import os
from pathlib import Path

import pytest

from stockagent.data_sync import taifex_workspace_guard as guard, workspace_volume
from stockagent.live import taifex_snapshot_cache as cache


@pytest.fixture
def source(tmp_path, monkeypatch):
    root = tmp_path/'data_taifex_public_history'
    root.mkdir()
    (root/'manifest.json').write_text('{"generation":1,"nested":[3]}')
    monkeypatch.setattr(guard, 'SOURCE', root)
    monkeypatch.setattr(guard, 'WORKSPACE_ROOT', tmp_path/'E')
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path/'E')})
    return root/'manifest.json'


def test_unchanged_source_survives_other_processes_without_opening_payload(source, monkeypatch):
    cache.prime(source.parent.parent)
    original = Path.open
    def refuse_source(path, *args, **kwargs):
        if path == source:
            raise AssertionError('cached source payload was reopened')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', refuse_source)
    one = cache.read(source)
    one['nested'].append(4)
    assert cache.read(source) == {'generation': 1, 'nested': [3]}


def test_changed_source_never_reuses_cache_while_handoff_holds_lock(source):
    cache.prime(source.parent.parent)
    lock = source.parent/'.download.lock'
    with lock.open('r+') as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert cache.read(source) == {'generation': 1, 'nested': [3]}
        source.write_text('{"generation":2,"nested":[5]}')
        assert cache.read(source, 'unavailable') == 'unavailable'
    cache.prime(source.parent.parent)
    assert cache.read(source) == {'generation': 2, 'nested': [5]}


def test_same_mtime_rewrite_and_malformed_json_do_not_resurrect_old_snapshot(source):
    cache.prime(source.parent.parent)
    before = source.stat()
    source.write_text('{"generation":2,"nested":[3]}')
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert cache.read(source) == {'generation': 2, 'nested': [3]}
    source.write_text('{"generation":')
    assert cache.read(source, 'invalid') == 'invalid'


def test_corrupt_cache_falls_back_to_current_source_and_read_does_not_write(source):
    cache.prime(source.parent.parent)
    path = cache._entry_path(source.parent.parent, 'manifest.json')
    path.write_text('{"contract":"corrupt"}')
    assert cache.read(source) == {'generation': 1, 'nested': [3]}
    assert path.read_text() == '{"contract":"corrupt"}'
    cache.prime(source.parent.parent)
    assert cache.read(source) == {'generation': 1, 'nested': [3]}


def test_large_source_remains_readable_without_entering_the_small_cache(source, monkeypatch):
    monkeypatch.setattr(cache, 'MAX_BYTES', 2)
    assert cache.read(source) == {'generation': 1, 'nested': [3]}


def test_unrelated_same_named_directory_is_not_locked_as_the_registered_source(source, tmp_path):
    other = tmp_path/'other'/'data_taifex_public_history'
    other.mkdir(parents=True)
    manifest = other/'manifest.json'
    manifest.write_text('{"other": true}')
    assert cache._scope(manifest) is None
    assert cache.read(manifest) == {'other': True}


def test_snapshot_uses_read_only_shared_lock_and_respects_handoff(source, monkeypatch):
    with guard.writer_guard(source, timeout_seconds=0):
        pass
    real_open = os.open
    def read_only_open(path, flags, *args, **kwargs):
        if Path(path) == source.parent/'.download.lock':
            assert flags & os.O_ACCMODE == os.O_RDONLY
            assert not flags & os.O_CREAT
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', read_only_open)
    with cache._snapshot_guard(source):
        with (source.parent/'.download.lock').open('r') as handoff:
            with pytest.raises(BlockingIOError):
                fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (source.parent/'.download.lock').open('r+') as handoff:
        fcntl.flock(handoff.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(cache.DatasetLockTimeout):
            with cache._snapshot_guard(source):
                pass
