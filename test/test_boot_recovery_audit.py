"""Boot readiness needs matching owner scope and all infrastructure gates."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from scripts import audit_boot_recovery as audit


@pytest.fixture
def scene(tmp_path, monkeypatch):
    report = {'startup': {'gateway_health_http': 200,
                         'windows_wsl_runtime': {'holders': [{'pid': 7}]},
                         'wsl_boot_id': 'same-kernel', 'wsl_userspace_at_utc': '2026-10-04T17:41:05+00:00'},
              'timers': {'a.timer': {'UnitFileState': 'enabled', 'ActiveState': 'active'}},
              'timer_schedule_findings': [], 'product_probes': {'health': 'blocked'}}
    units = {unit: {'ActiveState': 'active'} for unit in audit.PROFILES}
    failures = set()
    monkeypatch.setattr(audit, 'build_report', lambda **_: deepcopy(report))
    monkeypatch.setattr(audit, 'snapshot', lambda: deepcopy(units))
    monkeypatch.setattr(audit, 'control_verification_readiness', lambda _: {'required': False, 'ready': True})
    monkeypatch.setattr(audit, 'lakehouse_readiness', lambda **_: {'required': False, 'ready': True})
    monkeypatch.setattr(audit.os, 'readlink', lambda _: 'owner-namespace')

    def run(args, **_):
        if args[0] == 'runuser':
            return SimpleNamespace(returncode=0, stdout='2' if 'database' in failures else str(audit.SCHEMA_VERSION))
        return SimpleNamespace(returncode=2 if 'mount' in failures else 0)

    monkeypatch.setattr(audit.subprocess, 'run', run)
    monkeypatch.setattr(audit.sys, 'argv', ['audit', '--state-root', str(tmp_path)])
    return report, units, failures, tmp_path


def execute(scene):
    with pytest.raises(SystemExit) as exit:
        audit.main()
    return exit.value.code, json.loads((scene[3] / 'latest.json').read_text())


def test_closed_market_or_blocked_settlement_does_not_hide_infrastructure(scene):
    code, result = execute(scene)
    assert code == 0
    assert result['boot_recovery']['local_infrastructure_ready'] is True
    assert result['product_probes']['health'] == 'blocked'


@pytest.mark.parametrize('gate', ['mount', 'database', 'holder', 'http', 'owner', 'timer', 'timer_schedule', 'namespace', 'control', 'lakehouse'])
def test_http_200_alone_is_not_boot_acceptance(scene, monkeypatch, gate):
    report, units, failures, _ = scene
    if gate in {'mount', 'database'}:
        failures.add(gate)
    elif gate == 'holder':
        report['startup']['windows_wsl_runtime']['holders'] = []
    elif gate == 'http':
        report['startup']['gateway_health_http'] = 503
    elif gate == 'owner':
        units['syncthing@root.service']['ActiveState'] = 'failed'
    elif gate == 'timer':
        report['timers']['a.timer']['ActiveState'] = 'inactive'
    elif gate == 'timer_schedule':
        report['timer_schedule_findings'] = [{'finding': 'no_next_trigger'}]
    elif gate == 'control':
        monkeypatch.setattr(audit, 'control_verification_readiness', lambda _: {'required': True, 'ready': False})
    elif gate == 'lakehouse':
        monkeypatch.setattr(audit, 'lakehouse_readiness', lambda **_: {'required': True, 'ready': False})
    else:
        monkeypatch.setattr(audit.os, 'readlink', lambda path: 'interactive' if '/self/' in path else 'owner')
    code, result = execute(scene)
    assert code == 75
    assert result['boot_recovery']['local_infrastructure_ready'] is False


def test_first_observation_is_preserved_per_userspace_lifetime(scene):
    execute(scene)
    first = next(scene[3].glob('first-*.json'))
    original = first.read_bytes()
    scene[2].add('mount')
    assert execute(scene)[0] == 75
    assert first.read_bytes() == original
    scene[0]['startup']['wsl_userspace_at_utc'] = '2026-10-04T17:42:05+00:00'
    execute(scene)
    assert len(list(scene[3].glob('first-*.json'))) == 2
    assert not list(scene[3].glob('*.tmp'))


@pytest.mark.parametrize('failure', [None, 'stale', 'future', 'degraded', 'no_registration', 'incomplete', 'disabled', 'public_policy', 'last_failed', 'last_deferred'])
def test_selected_control_role_requires_fresh_success(tmp_path, failure):
    now = datetime.now(UTC)
    policy = tmp_path / 'policy.json'
    policy.write_text('{}')
    policy.chmod(0o644 if failure == 'public_policy' else 0o600)
    status = tmp_path / 'status.json'
    value = {'state': 'ready', 'observed_at_utc': now.isoformat(),
             'registered_release_count': 1, 'verified_release_count': 1, 'errors': []}
    if failure == 'stale':
        value['observed_at_utc'] = (now - timedelta(minutes=6)).isoformat()
    elif failure == 'future':
        value['observed_at_utc'] = (now + timedelta(minutes=1)).isoformat()
    elif failure == 'degraded':
        value['state'] = 'degraded'
    elif failure == 'no_registration':
        value.update(registered_release_count=0, verified_release_count=0)
    elif failure == 'incomplete':
        value['verified_release_count'] = 0
    status.write_text(json.dumps(value))
    timer = {'UnitFileState': 'disabled' if failure == 'disabled' else 'enabled', 'ActiveState': 'active'}
    service = {'LoadState': 'loaded', 'Result': 'success', 'ExecMainStatus': '0'}
    if failure == 'last_failed':
        service.update(Result='exit-code', ExecMainStatus='1')
    elif failure == 'last_deferred':
        service['ExecMainStatus'] = '75'
    result = audit.control_verification_readiness(
        {'stockagent-control-release-verification.timer': timer}, policy=policy, status=status, now=now, service=service)
    assert result['required'] is True and result['ready'] is (failure is None)


def test_unselected_control_role_does_not_become_a_boot_requirement(tmp_path):
    assert audit.control_verification_readiness({}, policy=tmp_path / 'absent') == {'required': False, 'ready': True}
