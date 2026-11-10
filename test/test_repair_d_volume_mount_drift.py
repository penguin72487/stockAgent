from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from scripts import repair_d_volume_mount_drift as repair
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def state(monkeypatch, tmp_path):
    current = {'mounted': True, 'healthy': False, 'busy': False, 'calls': [], 'mount_failures': 0}
    def run(*command, **_kwargs):
        current['calls'].append(command)
        if command[:2] == ('bash', str(repair.CHECKER)):
            return SimpleNamespace(returncode=0 if current['healthy'] else 2, stdout='', stderr='')
        if command[0] == 'fuser':
            return SimpleNamespace(returncode=0 if current['busy'] else 1,
                                   stdout='1234' if current['busy'] else '', stderr='')
        if command[0] == 'umount':
            current['mounted'] = False
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        if command[0] == 'mount':
            if current['mount_failures']:
                current['mount_failures'] -= 1
                return SimpleNamespace(returncode=1, stdout='', stderr='mount failed')
            current['mounted'] = current['healthy'] = True
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        raise AssertionError(command)
    monkeypatch.setattr(repair, '_run', run)
    monkeypatch.setattr(repair, '_field', lambda path, field: str(repair.VOLUME) if field == 'TARGET' else 'rw,msize=65536')
    monkeypatch.setattr(repair, '_canonical_identity', lambda: None)
    monkeypatch.setattr(repair, '_volume_identity', lambda: None)
    monkeypatch.setattr(repair, 'allowed_seconds', lambda *_a: 1000)
    monkeypatch.setattr(repair.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(repair.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(repair.packed, '_try_publication_lock', lambda *_a, **_kwargs: nullcontext())
    monkeypatch.setattr(repair.repack_retirement, '_boundary', lambda *_a, **_kwargs: nullcontext())
    monkeypatch.setattr(repair.os.path, 'ismount', lambda *_a: current['mounted'])
    return current


def test_repair_only_remounts_d_root_without_stopping_canonical_bind(state):
    assert repair.repair()['state'] == 'repairable_mount_drift'
    assert not any(command[0] in ('mount', 'umount') for command in state['calls'])
    result = repair.repair(apply=True)
    assert result['state'] == 'repaired' and result['canonical_bind_retained'] is True
    assert [command[0] for command in state['calls'] if command[0] in ('mount', 'umount')] == ['umount', 'mount']
    assert state['mounted'] and result['online_trim_performed'] is False


def test_busy_mount_and_trading_window_defer_without_detaching(state, monkeypatch):
    state['busy'] = True
    assert repair.repair(apply=True)['state'] == 'waiting_D_mount_users'
    assert not any(command[0] == 'umount' for command in state['calls'])
    monkeypatch.setattr(repair, 'allowed_seconds', lambda *_a: 0)
    assert repair.repair(apply=True)['state'] == 'waiting_trading_priority_window'
    assert state['mounted']


def test_failed_mount_is_retried_before_releasing_owner_locks(state):
    state['mount_failures'] = 1
    with pytest.raises(SnapshotError, match='remount failed'):
        repair.repair(apply=True)
    assert state['mounted'] and state['healthy']
    assert sum(command[0] == 'mount' for command in state['calls']) == 2
    assert repair.repair(apply=True)['state'] == 'healthy'


def test_wrong_canonical_authority_is_never_unmounted(state, monkeypatch):
    def reject():
        raise SnapshotError('wrong volume')
    monkeypatch.setattr(repair, '_canonical_identity', reject)
    with pytest.raises(SnapshotError, match='wrong volume'):
        repair.repair(apply=True)
    assert not any(command[0] == 'umount' for command in state['calls'])
