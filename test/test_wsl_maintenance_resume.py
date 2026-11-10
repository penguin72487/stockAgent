"""An interrupted finite request must resume once, through its original owner."""
from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import agent_workflow
from scripts import wsl_maintenance_resume as resume


@pytest.fixture
def scene(tmp_path, monkeypatch):
    state = tmp_path / 'state'
    runs = tmp_path / 'runs'
    runs.mkdir()
    monkeypatch.setattr(resume, 'STATE', state)
    monkeypatch.setattr(resume, 'RUNS', runs)
    a, b = 'a' * 64, 'b' * 64
    run_id = 'finlab-original-20261104-aaaaaa'
    unit = 'stockagent-agent-' + run_id + '.service'
    identity = {'pid': 12345, 'start_ticks': '60', 'boot_id': 'original-boot'}
    argv = ['run_fintech_python', 'scripts/run_finlab_finite_priority_handoff.py',
            '--request-id', a, '--request-id', b, '--preferred-futures-request-id', b,
            '--predecessor-pid', '20', '--predecessor-start-ticks', '30',
            '--progress', 'artifacts/operations/original.json', '--futures-workspace-only']
    record = {'run_id': run_id, 'unit': unit, 'task_id': 'existing-futures-task',
              'argv': argv, 'process': identity, 'state': 'running'}
    directory = runs / run_id
    directory.mkdir()
    path = directory / 'run.json'
    record['log'] = str(directory / 'run.log')
    path.write_text(json.dumps(record))
    monkeypatch.setattr(resume, 'active_agent_units', lambda: [unit])
    monkeypatch.setattr(resume, 'live_finlab_requests', lambda: {a, b})
    monkeypatch.setattr(resume, 'verify_finlab_workspace', lambda: None)
    monkeypatch.setattr(agent_workflow, 'process_identity', lambda pid: identity if pid == 12345 else None)
    return {'state': state, 'runs': runs, 'a': a, 'b': b, 'unit': unit,
            'record': record, 'record_path': path, 'identity': identity}


def captured(scene):
    return resume.capture('wsl-test-20261104.json')


def test_capture_pins_running_original_scope_without_provider_calls(scene):
    result = captured(scene)
    plan = json.loads(Path(result['plan']).read_text())
    assert result['run_count'] == 1 and result['provider_calls'] == 0
    assert plan['runs'][0]['argv'] == scene['record']['argv']
    assert plan['identity_sha256'] == result['identity_sha256']
    with pytest.raises(FileExistsError):
        captured(scene)


@pytest.mark.parametrize('change', ['unknown_script', 'bad_request', 'duplicate_request', 'outside_progress', 'no_workspace'])
def test_capture_refuses_unrecoverable_or_untrusted_work(scene, change):
    argv = scene['record']['argv']
    if change == 'unknown_script':
        argv[1] = 'scripts/unreviewed.py'
    elif change == 'bad_request':
        argv[3] = 'not-a-registered-id'
    elif change == 'duplicate_request':
        argv[5] = argv[3]
    elif change == 'outside_progress':
        argv[argv.index('--progress') + 1] = '../../other.json'
    else:
        argv.remove('--futures-workspace-only')
    scene['record_path'].write_text(json.dumps(scene['record']))
    with pytest.raises(ValueError):
        captured(scene)
    assert not (scene['state'] / 'wsl-test-20261104.json').exists()


def test_still_running_original_owner_is_never_relaunched(scene):
    plan = captured(scene)
    with pytest.raises(BlockingIOError, match='still active'):
        resume.restore('wsl-test-20261104.json', plan['identity_sha256'])
    assert not list(scene['runs'].glob('fl-wsl-resume-*'))


def test_unmanaged_finite_owner_prevents_shutdown_capture(scene, monkeypatch):
    monkeypatch.setattr(resume, 'live_finlab_requests', lambda: {scene['a'], scene['b'], 'c' * 64})
    with pytest.raises(ValueError, match='unmanaged'):
        captured(scene)
    assert not (scene['state'] / 'wsl-test-20261104.json').exists()


def test_cancelled_owner_requeues_once_and_keeps_exact_request_scope(scene, monkeypatch):
    plan = captured(scene)
    scene['record']['state'] = 'cancelled'
    scene['record_path'].write_text(json.dumps(scene['record']))
    monkeypatch.setattr(agent_workflow, 'process_identity', lambda _pid: None)
    monkeypatch.setattr(resume, 'active_agent_units', lambda: [])
    monkeypatch.setattr(resume, 'live_finlab_requests', lambda: set())
    calls = []

    def launch(command, **_kwargs):
        calls.append(command)
        name = command[command.index('--name') + 1]
        argv = command[command.index('--') + 1:]
        run_id = name + '-20261104-123456'
        path = scene['runs'] / run_id / 'run.json'
        path.parent.mkdir()
        path.write_text(json.dumps({'run_id': run_id, 'name': name, 'task_id': 'existing-futures-task',
                                    'argv': argv, 'state': 'queued',
                                    'log': str(path.parent / 'run.log'),
                                    'queued_at_utc': datetime.now(UTC).isoformat()}))
        return SimpleNamespace(stdout=json.dumps({'run_id': run_id}))

    monkeypatch.setattr(resume.subprocess, 'run', launch)
    first = resume.restore('wsl-test-20261104.json', plan['identity_sha256'])
    second = resume.restore('wsl-test-20261104.json', plan['identity_sha256'])
    assert len(calls) == 1 and first['runs'][0]['state'] == 'requeued'
    assert second['runs'][0]['state'] == 'existing_recovery_owner'
    args = calls[0][calls[0].index('--') + 1:]
    assert resume.parse_finlab(args) == ([scene['a'], scene['b']], [scene['b']])
    assert args[args.index('--progress') + 1] != 'artifacts/operations/original.json'


def test_partial_live_owner_excludes_only_its_requests(scene, monkeypatch):
    plan = captured(scene)
    scene['record']['state'] = 'cancelled'
    scene['record_path'].write_text(json.dumps(scene['record']))
    monkeypatch.setattr(agent_workflow, 'process_identity', lambda _pid: None)
    monkeypatch.setattr(resume, 'active_agent_units', lambda: [])
    monkeypatch.setattr(resume, 'live_finlab_requests', lambda: {scene['a']})
    launched = []
    def launch(command, **_kwargs):
        launched.append(command)
        return SimpleNamespace(stdout=json.dumps({'run_id': 'new-run'}))
    monkeypatch.setattr(resume.subprocess, 'run', launch)
    resume.restore('wsl-test-20261104.json', plan['identity_sha256'])
    argv = launched[0][launched[0].index('--') + 1:]
    assert resume.parse_finlab(argv) == ([scene['b']], [scene['b']])


def test_changed_plan_or_unexplained_failure_is_not_replayed(scene, monkeypatch):
    plan = captured(scene)
    path = Path(plan['plan'])
    altered = json.loads(path.read_text())
    altered['runs'][0]['task_id'] = 'different-task'
    path.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match='identity'):
        resume.restore('wsl-test-20261104.json', plan['identity_sha256'])


def test_unexplained_original_failure_is_not_replayed(scene, monkeypatch):
    plan = captured(scene)
    scene['record']['state'] = 'failed'
    scene['record_path'].write_text(json.dumps(scene['record']))
    monkeypatch.setattr(agent_workflow, 'process_identity', lambda _pid: None)
    monkeypatch.setattr(resume, 'active_agent_units', lambda: [])
    monkeypatch.setattr(resume, 'live_finlab_requests', lambda: set())
    with pytest.raises(ValueError, match='requires review'):
        resume.restore('wsl-test-20261104.json', plan['identity_sha256'])

