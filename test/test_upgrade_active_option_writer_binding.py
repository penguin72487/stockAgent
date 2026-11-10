from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import upgrade_active_option_writer_binding as upgrade
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


@pytest.fixture
def active_binding(tmp_path, monkeypatch):
    repo, control, destination = tmp_path/'repo', tmp_path/'control', tmp_path/'E'
    repo.mkdir(); control.mkdir(); destination.mkdir()
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    name = handoff.OPTIONS_RAW_NAME
    spec = {**handoff.RETAINED_LOCK_SOURCES[name], 'guard_files': (upgrade.WRITER, 'other.py')}
    monkeypatch.setattr(handoff, 'RETAINED_LOCK_SOURCES', {name: spec})
    source, backup = repo/spec['directory'], repo/'.data_tw_index_options_daily_raw.pre-e-20261108-v1'
    source.mkdir(parents=True); backup.mkdir()
    external = repo/spec['external_native_locks'][0]
    external.parent.mkdir(parents=True, exist_ok=True); external.touch()
    (control/'owner.lock').touch()
    writer = repo/upgrade.WRITER
    writer.parent.mkdir(parents=True); writer.write_text('old reviewed writer')
    (repo/'other.py').write_text('unchanged guard')
    previous = handoff.retained_enrollment_value(name)
    marker = control/(spec['label']+'-enrollment.json')
    atomic_write_json(marker, previous)
    old = previous['writer_guard_sha256'][upgrade.WRITER]
    writer.write_text('new reviewed writer')
    new = hashlib.sha256(writer.read_bytes()).hexdigest()
    monkeypatch.setattr(handoff, 'read_journal', lambda: {'state': 'active'})
    monkeypatch.setattr(handoff, '_assert_retained', lambda _v: destination)
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: True)
    monkeypatch.setattr(handoff, 'binance_handoff_window_allowed', lambda **_: True)
    monkeypatch.setattr(handoff, 'options_raw_handoff_window_allowed', lambda **_: True)
    return repo, control, marker, previous, old, new


def test_active_writer_upgrade_preserves_prior_and_is_idempotent(active_binding):
    _repo, _control, marker, previous, old, new = active_binding
    identity = previous['identity_sha256']
    assert upgrade.upgrade(identity, old, new)['state'] == 'ready'
    assert json.loads(marker.read_text()) == previous
    assert upgrade.upgrade(identity, old, new, apply=True)['state'] == 'upgraded'
    assert json.loads(marker.with_name(marker.name+'.prior-'+identity+'.json').read_text()) == previous
    handoff._validate_retained_enrollment(handoff.OPTIONS_RAW_NAME)
    assert upgrade.upgrade(identity, old, new, apply=True)['state'] == 'already_upgraded'


def test_active_writer_upgrade_rejects_other_code_or_wrong_identity(active_binding):
    repo, _control, marker, previous, old, new = active_binding
    with pytest.raises(SnapshotError, match='reviewed change'):
        upgrade.upgrade('0'*64, old, new, apply=True)
    (repo/'other.py').write_text('unexpected change')
    with pytest.raises(SnapshotError, match='reviewed change'):
        upgrade.upgrade(previous['identity_sha256'], old, new, apply=True)
    assert json.loads(marker.read_text()) == previous


def test_active_writer_upgrade_defers_for_busy_owner_and_missing_mount(active_binding, monkeypatch):
    _repo, control, marker, previous, old, new = active_binding
    with handoff.lock_file(control/'owner.lock'):
        with pytest.raises(BlockingIOError):
            upgrade.upgrade(previous['identity_sha256'], old, new, apply=True)
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: False)
    with pytest.raises(SnapshotError, match='mount or trading window'):
        upgrade.upgrade(previous['identity_sha256'], old, new, apply=True)
    assert json.loads(marker.read_text()) == previous


def test_active_writer_upgrade_rejects_unavailable_trading_window(active_binding, monkeypatch):
    _repo, _control, marker, previous, old, new = active_binding
    monkeypatch.setattr(handoff, 'options_raw_handoff_window_allowed', lambda **_: False)
    with pytest.raises(BlockingIOError, match='trading priority'):
        upgrade.upgrade(previous['identity_sha256'], old, new, apply=True)
    assert json.loads(marker.read_text()) == previous
