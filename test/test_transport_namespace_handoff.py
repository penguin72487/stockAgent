import json
from pathlib import Path
import subprocess

import pytest

from stockagent.data_sync import transport_namespace_handoff as handoff


OLD = ('21 1 0:171 /stockagent-backup-ingress-lab203.relocated-to-E-v1 '
       '/srv/stockagent-backup-ingress-lab203 ro,nosuid,noatime shared:22 - 9p D: rw\n')
ROOT = '1 0 8:64 / / ro,nosuid,relatime shared:11 - ext4 /dev/sde rw\n'
NEW = ('31 1 8:80 /workspace/transport/lab203 /srv/stockagent-backup-ingress-lab203 '
       'ro,nosuid,noatime - ext4 /dev/sdf rw\n')


def observation(tmp_path, raw=ROOT + OLD):
    process = tmp_path / '22'
    process.mkdir(exist_ok=True)
    return {'mnt:[22]': (process, handoff.mounts(raw))}


def test_only_exact_unreachable_alias_is_eligible(tmp_path):
    observed = observation(tmp_path)
    assert handoff.eligible('mnt:[22]', observed)['id'] == '21'
    (tmp_path/'22/root/srv/stockagent-backup-ingress-lab203').mkdir(parents=True)
    with pytest.raises(BlockingIOError, match='reachable'):
        handoff.eligible('mnt:[22]', observed)


@pytest.mark.parametrize('which', ['parent_peer', 'target_peer', 'slave', 'submount', 'extra_alias'])
def test_mount_dependencies_defer_without_mutation(tmp_path, which):
    raw = ROOT + OLD
    if which == 'slave':
        raw = raw.replace('shared:22', 'shared:22 master:9')
    if which == 'submount':
        raw += '41 21 8:80 / /srv/stockagent-backup-ingress-lab203/child rw - ext4 /dev/sdf rw\n'
    if which == 'extra_alias':
        raw += OLD.replace('21 1 ', '42 1 ').replace('/srv/stockagent-backup-ingress-lab203', '/another')
    observed = observation(tmp_path, raw)
    if which in {'parent_peer', 'target_peer'}:
        observed['mnt:[23]'] = (tmp_path/'23', handoff.mounts(ROOT if which == 'parent_peer' else OLD))
    with pytest.raises(BlockingIOError):
        handoff.eligible('mnt:[22]', observed)


def prepare_helper(tmp_path, monkeypatch):
    expected = handoff.mounts(OLD)[0]
    plan_path = tmp_path/'plan.json'
    plan_path.write_text(json.dumps({'contract': handoff.CONTRACT, 'namespace': 'mnt:[22]',
        'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'canonical': str(handoff.placement.CANONICAL), 'destination': '/enrolled/E',
        'destination_key': [8, 70], 'mount': expected, 'phase': 'planned'}))
    monkeypatch.setattr(handoff.placement, 'backing', lambda **kw: Path('/enrolled/E'))
    monkeypatch.setattr(handoff.os, 'readlink', lambda p: 'mnt:[1]' if str(p) == '/proc/1/ns/mnt' else 'mnt:[22]')
    monkeypatch.setattr(handoff, 'key', lambda p: [8, 70])
    monkeypatch.setattr(handoff, 'tree_key', lambda fd: [8, 70])
    state = {'mounts': ROOT + OLD, 'commands': []}
    original_read = Path.read_text
    def read(path, *args, **kwargs):
        if str(path) == '/proc/self/mountinfo':
            return state['mounts']
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(handoff, 'namespaces', lambda: {'fresh': True})
    monkeypatch.setattr(handoff, 'check_resume_parent', lambda *args: None)
    def check(identity, observed):
        assert identity == 'mnt:[22]' and observed == {'fresh': True}
        return expected
    monkeypatch.setattr(handoff, 'eligible', check)
    def execute(argv):
        state['commands'].append(argv)
        if argv[0] == 'umount':
            assert argv == ['umount', '--internal-only', '--no-canonicalize', str(handoff.placement.CANONICAL)]
            state['mounts'] = ROOT
        elif '--bind' in argv:
            state['mounts'] = ROOT + NEW
    monkeypatch.setattr(handoff, 'execute', execute)
    def attach(fd, target):
        assert fd == 99 and target == handoff.placement.CANONICAL
        state['commands'].append(['attach_prepared_tree', str(fd)])
        state['mounts'] = ROOT + NEW
    monkeypatch.setattr(handoff, 'attach_tree', attach)
    return plan_path, state, execute


def test_exact_handoff_preserves_permissions_and_never_forces_or_restarts(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    assert handoff.handoff_one(plan, 99) == {'namespace': 'mnt:[22]', 'state': 'alias_verified'}
    assert json.loads(plan.read_bytes())['phase'] == 'alias_verified'
    assert all('-l' not in command and '-f' not in command and 'systemctl' not in command for command in state['commands'])
    assert ['attach_prepared_tree', '99'] in state['commands']
    assert state['mounts'] == ROOT + NEW


def test_busy_umount_retains_old_mount_and_prepared_intent(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    def busy(argv):
        raise subprocess.CalledProcessError(32, argv, stderr='target is busy')
    monkeypatch.setattr(handoff, 'execute', busy)
    with pytest.raises(subprocess.CalledProcessError):
        handoff.handoff_one(plan, 99)
    assert state['mounts'] == ROOT + OLD
    assert json.loads(plan.read_bytes())['phase'] == 'prepared'


@pytest.mark.parametrize('failure', ['before_bind', 'before_shared'])
def test_interrupted_handoff_resumes_without_unmounting_E(tmp_path, monkeypatch, failure):
    plan, state, original = prepare_helper(tmp_path, monkeypatch)
    def fail(argv):
        if failure == 'before_shared' and '--make-shared' in argv:
            raise subprocess.CalledProcessError(1, argv)
        original(argv)
    monkeypatch.setattr(handoff, 'execute', fail)
    attach = handoff.attach_tree
    if failure == 'before_bind':
        def broken_attach(*args):
            raise OSError('attach interrupted')
        monkeypatch.setattr(handoff, 'attach_tree', broken_attach)
    with pytest.raises((subprocess.CalledProcessError, OSError)):
        handoff.handoff_one(plan, 99)
    assert json.loads(plan.read_bytes())['phase'] == ('old_mount_removed' if failure == 'before_bind' else 'E_bound')
    count = sum(command[0] == 'umount' for command in state['commands'])
    monkeypatch.setattr(handoff, 'execute', original)
    monkeypatch.setattr(handoff, 'attach_tree', attach)
    assert handoff.handoff_one(plan, 99)['state'] == 'alias_verified'
    assert sum(command[0] == 'umount' for command in state['commands']) == count == 1


def test_unplanned_absence_and_changed_namespace_do_not_bind(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    state['mounts'] = ROOT
    with pytest.raises(BlockingIOError, match='prepared'):
        handoff.handoff_one(plan, 99)
    monkeypatch.setattr(handoff.os, 'readlink', lambda _: 'mnt:[1]')
    with pytest.raises(ValueError, match='identity'):
        handoff.handoff_one(plan, 99)
    assert state['commands'] == []


def test_wrong_E_identity_or_changed_mount_never_unmounts(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    state['mounts'] = ROOT + OLD.replace('21 1 ', '22 1 ')
    with pytest.raises(BlockingIOError, match='generation'):
        handoff.handoff_one(plan, 99)
    monkeypatch.setattr(handoff, 'key', lambda _: [8, 71])
    with pytest.raises(ValueError, match='identity'):
        handoff.handoff_one(plan, 99)
    assert state['commands'] == []


def test_old_boot_intent_cannot_apply_to_reused_namespace_id(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    value = json.loads(plan.read_bytes())
    value['boot_id'] = 'previous-boot'
    plan.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='identity'):
        handoff.handoff_one(plan, 99)
    assert state['commands'] == []


def test_detached_tree_permissions_are_applied_before_attachment_and_fd_is_closed(monkeypatch):
    calls = []
    def api(name, args, types):
        calls.append(name)
        if name == 'mount_setattr':
            attrs = args[3]._obj
            assert attrs.attr_set == 1 | 2 | 16
            assert attrs.attr_clr == 0x70 and attrs.propagation == 1 << 18
        return 99 if name == 'open_tree' else 0
    monkeypatch.setattr(handoff, 'mount_api', api)
    closed = []
    monkeypatch.setattr(handoff.os, 'close', closed.append)
    with pytest.raises(RuntimeError):
        with handoff.prepared_tree(Path('/E'), ['ro', 'nosuid', 'noatime']) as fd:
            assert fd == 99 and calls == ['open_tree', 'mount_setattr']
            handoff.attach_tree(fd, Path('/target'))
            assert calls == ['open_tree', 'mount_setattr', 'move_mount']
            raise RuntimeError('child failed')
    assert closed == [99]


def test_wrong_tree_fd_cannot_remove_the_old_mount(tmp_path, monkeypatch):
    plan, state, _ = prepare_helper(tmp_path, monkeypatch)
    monkeypatch.setattr(handoff, 'tree_key', lambda _: [8, 71])
    with pytest.raises(ValueError, match='identity'):
        handoff.handoff_one(plan, 99)
    assert state['commands'] == []


def test_resume_rejects_a_replaced_parent_or_a_new_shared_peer(tmp_path):
    expected = handoff.mounts(OLD)[0]
    observed = observation(tmp_path, ROOT)
    handoff.check_resume_parent('mnt:[22]', expected, observed)
    observed['mnt:[23]'] = (tmp_path/'23', handoff.mounts(ROOT))
    with pytest.raises(BlockingIOError, match='parent'):
        handoff.check_resume_parent('mnt:[22]', expected, observed)
    observed = observation(tmp_path, ROOT.replace('1 0 ', '4 0 '))
    with pytest.raises(BlockingIOError, match='parent'):
        handoff.check_resume_parent('mnt:[22]', expected, observed)
