"""A rollout must retain the existing owner and the physical capacity guard."""
import json
from pathlib import Path

import pytest

from scripts import run_tej_finite_retry_rollout_wait as rollout


def unit(pid='10', active='active', kill='mixed', timeout='31min 40s'):
    return {'MainPID': pid, 'ActiveState': active, 'SubState': 'running',
            'KillMode': kill, 'TimeoutStopUSec': timeout}


def halt(_):
    raise StopIteration


def test_low_physical_capacity_does_not_restart_the_existing_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(rollout, 'unit_state', lambda: unit())
    monkeypatch.setattr(rollout, 'checked_capacity', lambda _: {'state': 'deferred', 'reason': 'physical_backing_reserve'})
    monkeypatch.setattr(rollout.time, 'sleep', halt)
    monkeypatch.setattr(rollout.subprocess, 'run', lambda *_a, **_k: pytest.fail('must not restart'))
    with pytest.raises(StopIteration):
        rollout.wait(tmp_path, 'table', tmp_path/'receipt.json', 1, interval=1)
    assert not (tmp_path/'receipt.json').exists()


@pytest.mark.parametrize('state', [unit(active='inactive'), unit(kill='control-group'), unit(timeout='1s')])
def test_changed_or_intentionally_stopped_unit_is_not_restarted(tmp_path, monkeypatch, state):
    monkeypatch.setattr(rollout, 'unit_state', lambda: state)
    monkeypatch.setattr(rollout, 'checked_capacity', lambda _: {'state': 'admitted'})
    monkeypatch.setattr(rollout.time, 'sleep', halt)
    monkeypatch.setattr(rollout.subprocess, 'run', lambda *_a, **_k: pytest.fail('must not restart'))
    with pytest.raises(StopIteration):
        rollout.wait(tmp_path, 'table', tmp_path/'receipt.json', 1, interval=1)


def test_capacity_is_rechecked_after_exact_target_before_handoff(tmp_path, monkeypatch):
    checks = iter([{'state': 'admitted'}, {'state': 'deferred', 'reason': 'physical_backing_reserve'}])
    monkeypatch.setattr(rollout, 'unit_state', lambda: unit())
    monkeypatch.setattr(rollout, 'checked_capacity', lambda _: next(checks))
    monkeypatch.setattr(rollout, 'verify_target', lambda *_: 'exact-task')
    monkeypatch.setattr(rollout.time, 'sleep', halt)
    monkeypatch.setattr(rollout.subprocess, 'run', lambda *_a, **_k: pytest.fail('must not restart'))
    with pytest.raises(StopIteration):
        rollout.wait(tmp_path, 'table', tmp_path/'receipt.json', 1, interval=1)


def test_owner_already_transitioning_is_observed_without_another_restart(tmp_path, monkeypatch):
    units = iter([unit(), unit('11')])
    monkeypatch.setattr(rollout, 'unit_state', lambda: next(units))
    monkeypatch.setattr(rollout, 'checked_capacity', lambda _: {'state': 'admitted'})
    monkeypatch.setattr(rollout.time, 'sleep', halt)
    monkeypatch.setattr(rollout.subprocess, 'run', lambda *_a, **_k: pytest.fail('must not restart'))
    with pytest.raises(StopIteration):
        rollout.wait(tmp_path, 'table', tmp_path/'receipt.json', 1, interval=1)


def test_only_the_existing_graceful_owner_is_restarted_and_read_back(tmp_path, monkeypatch):
    (tmp_path/'scheduler_status.json').write_text(json.dumps({'owner_pid': 11}))
    units = iter([unit(), unit(), unit('11')])
    monkeypatch.setattr(rollout, 'unit_state', lambda: next(units))
    monkeypatch.setattr(rollout, 'checked_capacity', lambda _: {'state': 'admitted'})
    monkeypatch.setattr(rollout, 'verify_target', lambda *_: 'exact-task')
    monkeypatch.setattr(rollout.time, 'sleep', lambda _: None)
    calls = []
    monkeypatch.setattr(rollout.subprocess, 'run', lambda command, **kwargs: calls.append(command))
    output = tmp_path/'receipt.json'
    assert rollout.wait(tmp_path, 'table', output, 1, interval=1) == 0
    assert calls == [['systemctl', 'restart', '--no-block', rollout.UNIT]]
    receipt = json.loads(output.read_text())
    assert receipt['state'] == 'new_canonical_owner_observed' and receipt['owner_pid'] == '11'
    assert receipt['provider_queries'] == 0 and receipt['source_receipts_still_require_readback'] is True
