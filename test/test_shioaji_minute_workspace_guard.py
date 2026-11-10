import fcntl

import pytest

from stockagent.data_sync import shioaji_minute_workspace_guard as guard, workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_unenrolled_and_day_trade_roots_remain_unmodified(tmp_path, monkeypatch):
    source, destination, custom = tmp_path/'source', tmp_path/'E', tmp_path/'day-trade'
    source.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    with guard.writer_guard(source), guard.writer_guard(custom):
        pass
    assert not (source/'.download.lock').exists() and not custom.exists()
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination):
            pass


def test_registered_raw_writer_shares_the_handoff_lock(tmp_path, monkeypatch):
    source, destination = tmp_path/'source', tmp_path/'E'
    source.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.MINUTE_RAW_NAME,))
    monkeypatch.setattr(handoff, 'guard_path', lambda _path: None)
    with guard.writer_guard(source):
        with (source/'.download.lock').open('r+b') as other, pytest.raises(BlockingIOError):
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (source/'.download.lock').open('r+b') as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_direct_e_path_requires_the_active_exact_alias(tmp_path, monkeypatch):
    source, destination = tmp_path/'source', tmp_path/'E'
    source.mkdir()
    destination.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.MINUTE_RAW_NAME,))
    monkeypatch.setattr(handoff, 'guard_path', lambda _path: None)
    monkeypatch.setattr(handoff, 'read_journal', lambda: None)
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination):
            pass
    monkeypatch.setattr(handoff, 'read_journal', lambda: {'state': 'active', 'destination': str(destination)})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: True)
    with guard.writer_guard(destination):
        pass
