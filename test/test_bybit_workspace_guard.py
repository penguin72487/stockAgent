from contextlib import contextmanager
import fcntl

import pytest

from stockagent.data_sync import bybit_workspace_guard as guard, workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_unenrolled_source_and_custom_root_are_unchanged(tmp_path, monkeypatch):
    source, destination, custom = tmp_path / 'source', tmp_path / 'E', tmp_path / 'custom'
    source.mkdir()
    alias = tmp_path / 'data_bybit'
    alias.symlink_to(source, target_is_directory=True)
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    with guard.writer_guard(alias / '1m'), guard.writer_guard(custom):
        pass
    assert not (source / '.download.lock').exists() and not custom.exists()
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination / '1m'):
            pass


def test_registered_alias_shares_handoff_lock_and_releases_on_error(tmp_path, monkeypatch):
    source, destination = tmp_path / 'source', tmp_path / 'E'
    source.mkdir()
    alias = tmp_path / 'data_bybit'
    alias.symlink_to(source, target_is_directory=True)
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.BYBIT_NAME,))
    monkeypatch.setattr(handoff, 'guard_path', lambda _path: None)
    with pytest.raises(RuntimeError, match='test error'):
        with guard.writer_guard(alias / 'funding'):
            with (source / '.download.lock').open('r+b') as other, pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            raise RuntimeError('test error')
    with (source / '.download.lock').open('r+b') as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_registered_redirected_canonical_alias_is_not_treated_as_a_custom_path(tmp_path, monkeypatch):
    source, destination, other = tmp_path / 'source', tmp_path / 'E', tmp_path / 'other'
    source.mkdir(); other.mkdir()
    alias = tmp_path / 'data_bybit'
    alias.symlink_to(other, target_is_directory=True)
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(guard, 'ALIASES', (alias,))
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.BYBIT_NAME,))
    with pytest.raises(SnapshotError, match='alias no longer identifies'):
        with guard.writer_guard(alias / '1m'):
            pass
    alias.unlink()
    with pytest.raises(SnapshotError, match='alias is unavailable'):
        with guard.writer_guard(alias / '1m'):
            pass


def test_alias_changed_while_waiting_for_the_lock_is_rejected(tmp_path, monkeypatch):
    source, destination, other = tmp_path / 'source', tmp_path / 'E', tmp_path / 'other'
    source.mkdir(); other.mkdir()
    alias = tmp_path / 'data_bybit'
    alias.symlink_to(source, target_is_directory=True)
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(guard, 'ALIASES', (alias,))
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.BYBIT_NAME,))
    @contextmanager
    def waited(_source, _timeout):
        alias.unlink()
        alias.symlink_to(other, target_is_directory=True)
        yield
    monkeypatch.setattr(guard, '_writer_guard', waited)
    with pytest.raises(SnapshotError, match='alias no longer identifies'):
        with guard.writer_guard(alias / '1m'):
            pass


def test_direct_e_path_requires_the_active_exact_mount(tmp_path, monkeypatch):
    source, destination = tmp_path / 'source', tmp_path / 'E'
    source.mkdir()
    destination.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.BYBIT_NAME,))
    monkeypatch.setattr(handoff, 'guard_path', lambda _path: None)
    monkeypatch.setattr(handoff, 'selected_source', lambda _name: __import__('contextlib').nullcontext())
    monkeypatch.setattr(handoff, 'read_journal', lambda: None)
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination / 'daily'):
            pass
    monkeypatch.setattr(handoff, 'read_journal', lambda: {'state': 'active', 'destination': str(destination)})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: False)
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination / 'daily'):
            pass
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: True)
    with guard.writer_guard(destination / 'daily'):
        pass


@pytest.mark.parametrize('timeout', [-1, float('inf'), float('nan')])
def test_invalid_timeout_is_rejected(timeout):
    with pytest.raises(ValueError, match='timeout'):
        with guard.writer_guard('/custom', timeout_seconds=timeout):
            pass
