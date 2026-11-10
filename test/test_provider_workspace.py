import json
import os
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import provider_workspace as placement
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import cache_capacity as capacity
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_unenrolled_provider_retains_existing_capacity_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(placement, 'REGISTRY', tmp_path/'absent')
    monkeypatch.setattr(placement, 'ROOT', tmp_path)
    monkeypatch.setattr(placement.volume, 'configuration', lambda: pytest.fail('no enrollment, no E probe'))
    assert placement.check_path(tmp_path/'data_finmind') is None


def test_retained_registry_without_control_bind_defers_before_provider_write(tmp_path, monkeypatch):
    monkeypatch.setattr(placement, 'ROOT', tmp_path)
    monkeypatch.setattr(placement, 'REGISTRY', tmp_path/'legacy/workspace-volume/provider-placement.json')
    retained = tmp_path/'service_data/stockagent/workspace-volume/provider-placement.json'
    retained.parent.mkdir(parents=True)
    retained.write_text('{"contract":"temporary_provider_e_workspace_v1"}')
    monkeypatch.setattr(placement.volume, 'configuration', lambda: pytest.fail('no identity or source call'))
    with pytest.raises(SnapshotError, match='canonical control root'):
        placement.check_path(tmp_path/'data_finmind/complement')


def test_dangling_registry_symlink_is_not_an_unenrolled_provider(tmp_path, monkeypatch):
    monkeypatch.setattr(placement, 'ROOT', tmp_path)
    registry = tmp_path/'registry'
    registry.symlink_to(tmp_path/'absent')
    monkeypatch.setattr(placement, 'REGISTRY', registry)
    with pytest.raises(SnapshotError, match='canonical control root'):
        placement.check_path(tmp_path/'data_tej')


def test_all_provider_dispatch_waits_for_complete_migration_handoff(tmp_path, monkeypatch):
    monkeypatch.setattr(placement, 'ROOT', tmp_path)
    fence = tmp_path/'fence'; fence.write_text('owned migration')
    monkeypatch.setattr(placement, 'MIGRATION_FENCE', fence)
    monkeypatch.setattr(placement, 'load', lambda: {'providers': {'tej': {'state': 'active'}}})
    with pytest.raises(SnapshotError, match='complete E migration handoff'):
        placement.check_path(tmp_path/'data_finmind/complement')


def test_namespace_bridge_rejects_occupied_placeholder_before_mount(tmp_path):
    from scripts import provider_workspace_namespace as bridge
    source = tmp_path/'source'; source.mkdir()
    repo = tmp_path/'repo'; repo.mkdir()
    target = repo/'data_finlab'; target.mkdir()
    (target/'original').write_bytes(b'preserve')
    identity = lambda p: [p.stat().st_dev, p.stat().st_ino]
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(RuntimeError, match='refuse to hide'):
            bridge.run({'repository_root': str(repo), 'repository_identity': identity(repo),
                        'provider': 'finlab', 'root': {'fd': fd, 'identity': identity(source)},
                        'placeholder_identity': identity(target), 'children': []})
        assert (target/'original').read_bytes() == b'preserve'
    finally:
        os.close(fd)


def test_namespace_bridge_preserves_mount_errno_and_identifies_target(tmp_path, monkeypatch):
    import errno
    from scripts import provider_workspace_namespace as bridge
    source = tmp_path/'source'; source.mkdir()
    target = tmp_path/'target'; target.mkdir()
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    monkeypatch.setattr(bridge, 'mounts', lambda: set())
    monkeypatch.setattr(bridge.os, 'statvfs', lambda path: type('VFS', (), {'f_flag': 0})())
    def fail(*args):
        raise OSError(errno.ENOENT, 'move_mount')
    monkeypatch.setattr(bridge, 'attach_tree', fail, raising=False)
    try:
        info = os.fstat(fd)
        with pytest.raises(OSError) as caught:
            bridge.attach(target, {'fd': fd, 'identity': [info.st_dev, info.st_ino]})
        assert caught.value.errno == errno.ENOENT
        assert str(target) in str(caught.value)
    finally:
        os.close(fd)


def test_provider_worker_failure_is_reported_while_other_lane_finishes(capsys):
    from concurrent.futures import Future
    from scripts import relocate_provider_workspace as mover
    failed, completed = Future(), Future()
    failed.set_exception(RuntimeError('preserve exact diagnostic'))
    completed.set_result('verified')
    with pytest.raises(RuntimeError, match='retained original and fence: finmind'):
        mover.collect_provider_jobs({failed: 'finmind', completed: 'tej'})
    observed = capsys.readouterr().out
    assert 'provider_worker_failed' in observed and 'preserve exact diagnostic' in observed


def test_only_vanished_namespace_may_skip_identified_missing_mount(monkeypatch):
    from scripts import relocate_provider_workspace as mover
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    monkeypatch.setattr(mover, 'namespaces', lambda: {42: read_fd})
    error = RuntimeError('FileNotFoundError: [Errno 2] provider alias attach failed: /data_finmind')
    assert mover.vanished_namespace_after_missing_attach(41, error)
    with pytest.raises(OSError):
        os.fstat(read_fd)  # The fresh census is not leaked.
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    monkeypatch.setattr(mover, 'namespaces', lambda: {42: read_fd})
    assert not mover.vanished_namespace_after_missing_attach(42, error)
    monkeypatch.setattr(mover, 'namespaces', lambda: pytest.fail('unrelated errors cannot be skipped'))
    assert not mover.vanished_namespace_after_missing_attach(41, RuntimeError('permission denied'))


def test_namespace_program_uses_canonical_detached_mount_primitives():
    import inspect
    from scripts.relocate_provider_workspace import namespace_program
    from stockagent.data_sync.transport_namespace_handoff import attach_tree, mount_api
    program = namespace_program()
    assert inspect.getsource(attach_tree) in program
    assert inspect.getsource(mount_api) in program
    assert 'from stockagent' not in program


def test_continuation_reuses_exact_existing_run_and_rejects_conflicting_argv(tmp_path, monkeypatch):
    from scripts import relocate_provider_workspace as mover
    state = tmp_path/'state'; state.mkdir()
    name = 'provider-finite-test'
    argv = ['run_fintech_python', mover.FINITE_SCRIPTS[0], '--request-id', 'original']
    job = {'role': 'finlab', 'name': name, 'task': 'original-task', 'argv': argv}
    atomic_write_json(state/'continuation.json', {'contract': 'provider_e_finite_continuation_v1', 'jobs': [job]})
    record = tmp_path/'artifacts/operations/agent-workflow/runs'/(name+'-original')/'run.json'
    record.parent.mkdir(parents=True)
    row = {'task_id': job['task'], 'argv': argv, 'run_id': name+'-original'}
    atomic_write_json(record, row)
    monkeypatch.setattr(mover, 'STATE', state)
    monkeypatch.setattr(mover, 'ROOT', tmp_path)
    monkeypatch.setattr(mover, 'command', lambda *a, **k: pytest.fail('must not duplicate an existing finite owner'))
    assert mover.resume_finite() == {'finlab': name+'-original'}
    row['argv'] = [*argv, '--unrelated']
    atomic_write_json(record, row)
    with pytest.raises(RuntimeError, match='incompatible previous argv'):
        mover.resume_finite()


def test_original_windows_tej_recovery_waits_before_any_source_write(tmp_path, monkeypatch, capsys):
    from scripts import recover_tej_startup as recovery
    import sys
    fence = tmp_path/'fence'; fence.write_text('owned migration')
    monkeypatch.setattr(placement, 'MIGRATION_FENCE', fence)
    root = tmp_path/'data_tej'
    monkeypatch.setattr(sys, 'argv', ['recover_tej_startup.py', '--root', str(root),
                                    '--watch', '--windows-session-id', '1'])
    monkeypatch.setattr(recovery, 'bootstrap', lambda *a, **k: pytest.fail('must not reach Windows or provider'))
    assert recovery.main() == 75
    assert not root.exists()
    assert 'provider_workspace_handoff_wait' in capsys.readouterr().out


def test_tej_pending_placement_stays_fenced_after_runtime_marker_disappears(tmp_path, monkeypatch):
    from scripts import recover_tej_startup as recovery
    import sys
    monkeypatch.setattr(placement, 'MIGRATION_FENCE', tmp_path/'absent-runtime-marker')
    def pending(path):
        raise SnapshotError('persistent cutover is incomplete after reboot')
    monkeypatch.setattr(placement, 'check_path', pending)
    root = tmp_path/'data_tej'
    monkeypatch.setattr(sys, 'argv', ['recover_tej_startup.py', '--root', str(root), '--watch',
                                    '--windows-session-id', '1'])
    monkeypatch.setattr(recovery, 'bootstrap', lambda *a, **k: pytest.fail('pending placement must not bootstrap'))
    assert recovery.main() == 75 and not root.exists()


def test_tej_interactive_relay_enters_service_mount_namespace_without_changing_arguments(monkeypatch):
    import sys
    from scripts import recover_tej_startup as recovery
    reads = {'/proc/self/ns/mnt': 'mnt:[old]', '/proc/1/ns/mnt': 'mnt:[service]'}
    monkeypatch.setattr(recovery.os, 'readlink', lambda path: reads[path])
    monkeypatch.setattr(sys, 'argv', ['recover_tej_startup.py', '--watch', '--windows-session-id', '1'])
    monkeypatch.setenv('WSL_INTEROP', '/run/WSL/123_interop')
    calls = []
    def execvp(name, argv):
        calls.append((name, argv, os.environ['WSL_INTEROP']))
        raise RuntimeError('test exec handoff')
    monkeypatch.setattr(recovery.os, 'execvp', execvp)
    with pytest.raises(RuntimeError, match='test exec handoff'):
        recovery.enter_service_mount_namespace()
    name, argv, interop = calls[0]
    assert name == 'nsenter'
    assert argv[:3] == ['nsenter', '--mount=/proc/1/ns/mnt', '--']
    assert argv[3] == sys.executable and argv[-3:] == ['--watch', '--windows-session-id', '1']
    assert interop == '/run/WSL/123_interop'
    reads['/proc/self/ns/mnt'] = reads['/proc/1/ns/mnt']
    recovery.enter_service_mount_namespace()
    assert len(calls) == 1


def test_same_device_bind_requires_real_mount_and_same_inode(tmp_path, monkeypatch):
    source = tmp_path/'source'; source.mkdir()
    alias = tmp_path/'alias'; alias.mkdir()
    monkeypatch.setattr(placement.volume, '_mount_info', lambda p: {'target': str(p)})
    assert placement.exact_mount(alias, source) is False
    original = Path.samefile
    monkeypatch.setattr(Path, 'samefile', lambda p, other: True if (p, other) == (alias, source) else original(p, other))
    assert alias.is_mount() is False
    assert placement.exact_mount(alias, source) is True
    monkeypatch.setattr(placement.volume, '_mount_info', lambda p: {'target': '/'})
    assert placement.exact_mount(alias, source) is False


def test_incomplete_copy_and_missing_mount_cannot_fall_back_to_c(tmp_path, monkeypatch):
    entry = {'state': 'copying', 'alias': str(tmp_path/'alias'), 'destination': str(tmp_path/'e')}
    with pytest.raises(SnapshotError, match='has not completed'):
        placement.check_entry('finmind', entry)
    entry['state'] = 'active'
    monkeypatch.setattr(placement.volume, 'check', lambda: {})
    with pytest.raises(SnapshotError, match='C fallback refused'):
        placement.check_entry('finmind', entry)


def test_copy_proof_and_external_subtrees_are_part_of_acceptance(tmp_path, monkeypatch):
    proof = tmp_path/'copy-result.json'
    atomic_write_json(proof, {'state': 'verified', 'files': 7})
    entry = {'state': 'active', 'alias': str(tmp_path/'alias'), 'destination': str(tmp_path/'e'),
             'evidence': str(tmp_path), 'copy_result_sha256': relocation.digest(proof),
             'external_subtrees': [{'relative': 'unused', 'source': str(tmp_path/'old/unused'), 'readonly': True}]}
    monkeypatch.setattr(placement.volume, 'check', lambda: {})
    monkeypatch.setattr(placement, 'exact_mount', lambda *a: True)
    monkeypatch.setattr(os, 'statvfs', lambda p: type('VFS', (), {'f_flag': 0})())
    with pytest.raises(SnapshotError, match='must remain read-only'):
        placement.check_entry('finmind', entry)
    monkeypatch.setattr(os, 'statvfs', lambda p: type('VFS', (), {'f_flag': os.ST_RDONLY})())
    assert placement.check_entry('finmind', entry)['physical_volume'] == 'E:'
    proof.write_text('changed')
    with pytest.raises(SnapshotError, match='proof changed'):
        placement.check_entry('finmind', entry)


def test_dispatch_defers_registered_provider_before_backing_or_source_call(tmp_path, monkeypatch):
    monkeypatch.setattr(capacity, 'load_capacity_policy', lambda: {'nodes': [capacity.socket.gethostname()]})
    def unavailable(path):
        raise SnapshotError('E unmounted')
    monkeypatch.setattr(placement, 'check_path', unavailable)
    monkeypatch.setattr(capacity, '_admit_backing_volume', lambda *a: pytest.fail('must defer before allocation'))
    result = capacity.acquisition_dispatch_status(tmp_path)
    assert result['state'] == 'deferred' and result['reason'] == 'provider_workspace_unavailable'


def test_interrupted_pause_keeps_original_service_restore_states(tmp_path, monkeypatch):
    from scripts import relocate_provider_workspace as mover
    state = tmp_path/'state'; state.mkdir()
    fence = tmp_path/'fence'; fence.write_text('migration')
    original = {unit: ('active' if unit in mover.TIMERS else 'inactive')
                for unit in (*mover.TIMERS, *mover.SERVICES)}
    atomic_write_json(state/'pause.json', {'unit_states': original, 'state': 'fenced',
        'finite_units_stopped': [], 'interactive_recovery': []})
    monkeypatch.setattr(mover, 'STATE', state)
    monkeypatch.setattr(mover, 'FENCE', fence)
    monkeypatch.setattr(mover, 'SYSTEMD_RUNTIME_ROOT', tmp_path/'units')
    monkeypatch.setattr(mover, 'process_rows', lambda: iter(()))
    calls = []
    monkeypatch.setattr(mover, 'command', lambda argv, **kw: calls.append(argv) or 'inactive')
    result = mover.pause()
    assert result['state'] == 'paused' and result['unit_states'] == original
    assert not any(argv[:2] == ['systemctl', 'show'] for argv in calls)
    for unit in (*mover.TIMERS, *mover.SERVICES):
        path = tmp_path/'units'/(unit+'.d')/mover.DROPIN
        assert path.read_text() == '[Unit]\nConditionPathExists=!'+str(fence)+'\n'


def test_tej_relocation_includes_the_native_api_lock(tmp_path, monkeypatch):
    from scripts import relocate_provider_workspace as mover
    monkeypatch.setattr(mover, 'ROOT', tmp_path)
    (tmp_path/'data_tej/api_trial_v1').mkdir(parents=True)
    assert tmp_path/'data_tej/api_trial_v1/.api.lock' in mover.lock_paths('tej')


@pytest.mark.parametrize('name', ['tej', 'finlab'])
def test_runner_checks_actual_provider_root_before_launch(tmp_path, name):
    import shutil
    import subprocess
    scripts = tmp_path/'scripts'; scripts.mkdir()
    script = 'run_tej_history.sh' if name == 'tej' else 'run_finlab_refresh.sh'
    shutil.copy(Path(__file__).resolve().parents[1]/'scripts'/script, scripts/script)
    (scripts/'runtime_env.sh').write_text('''
run_fintech_python() { printf '%s\\n' "$*"; return 75; }
sleep() { exit 81; }
resolve_fintech_python() { echo unexpected; return 97; }
''')
    result = subprocess.run(['bash', str(scripts/script)], cwd=tmp_path, capture_output=True, text=True,
                            env={**os.environ, 'FINLAB_REPO_ROOT': str(tmp_path)}, timeout=10)
    assert result.returncode in {75, 81}
    assert '--path '+str(tmp_path/('data_'+name)) in result.stdout
    assert 'unexpected' not in result.stdout
