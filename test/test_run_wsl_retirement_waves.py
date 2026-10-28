import fcntl
import hashlib
import json
from pathlib import Path
from threading import Barrier, Lock
from types import SimpleNamespace

import pytest

from scripts import retire_local_offline_artifacts as owner
from scripts import run_wsl_retirement_waves as runner
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_owner_rejects_changed_pinned_inventory_before_receipt_or_apply(tmp_path, monkeypatch):
    inventory, receipt = tmp_path / 'inventory.json', tmp_path / 'receipt'
    inventory.write_bytes(b'original inventory')
    reached = []
    monkeypatch.setattr(owner, '_apply', lambda *args: reached.append(args) or {'all_selected_retired': True})
    with pytest.raises(SnapshotError, match='pinned SHA-256'):
        owner.apply(inventory, receipt, expected_inventory_sha256='0' * 64)
    assert not receipt.exists()
    assert not reached
    exact = hashlib.sha256(inventory.read_bytes()).hexdigest()
    assert owner.apply(inventory, receipt, expected_inventory_sha256=exact)['all_selected_retired']
    assert len(reached) == 1


def test_fixed_d_owner_requires_both_inventory_and_release_pins(tmp_path, monkeypatch):
    inventory, receipt = tmp_path / 'inventory.json', tmp_path / 'receipt'
    inventory.write_bytes(b'fixed')
    digest = hashlib.sha256(inventory.read_bytes()).hexdigest()
    reached = []
    monkeypatch.setattr(owner, '_apply', lambda *a, **kw: reached.append(kw) or {'all_selected_retired': True})
    with pytest.raises(SnapshotError, match='pinned cold manifest'):
        owner.apply(inventory, receipt, fixed_d_stage_only=True, expected_fixed_manifest_sha256='a' * 64)
    with pytest.raises(SnapshotError, match='pinned cold manifest'):
        owner.apply(inventory, receipt, expected_inventory_sha256=digest,
                    expected_fixed_manifest_sha256='a' * 64)
    assert not receipt.exists() and not reached
    owner.apply(inventory, receipt, expected_inventory_sha256=digest, fixed_d_stage_only=True,
                expected_fixed_manifest_sha256='a' * 64)
    assert reached == [{'fixed_d_stage_only': True, 'expected_fixed_manifest_sha256': 'a' * 64}]


def test_new_d_recovery_owner_requires_exact_inventory_and_does_not_change_fixed_mode(tmp_path, monkeypatch):
    inventory, receipt = tmp_path / 'inventory.json', tmp_path / 'receipt'
    inventory.write_bytes(b'guarded D fixture')
    digest = hashlib.sha256(inventory.read_bytes()).hexdigest()
    reached = []
    monkeypatch.setattr(owner, '_apply', lambda *a, **kw: reached.append(kw) or {'all_selected_retired': True})
    with pytest.raises(SnapshotError, match='pinned inventory'):
        owner.apply(inventory, receipt, d_stage_recovery_only=True)
    with pytest.raises(SnapshotError, match='single resource mode'):
        owner.apply(inventory, receipt, expected_inventory_sha256=digest, fixed_d_stage_only=True,
                    expected_fixed_manifest_sha256='a' * 64, d_stage_recovery_only=True)
    assert not receipt.exists() and not reached
    assert owner.apply(inventory, receipt, expected_inventory_sha256=digest,
                       d_stage_recovery_only=True)['all_selected_retired']
    assert reached == [{'d_stage_recovery_only': True}]


def _plan_for(pairs, parallelism, *, deferred=False, capacity=False):
    cohorts = [{'inventory': str(inv), 'receipt_directory': str(receipt),
                'inventory_sha256': inv.name.ljust(64, '0')[:64],
                'preflight_deferrals': ['busy'] if deferred else []}
               for inv, receipt in pairs]
    indexes = [] if deferred else list(range(len(cohorts)))
    waves = [{'cohorts': indexes[start:start + parallelism],
              'capacity_deferrals': ['windows_c'] if capacity else []}
             for start in range(0, len(indexes), parallelism)]
    return {'cohorts': cohorts, 'overlap_conflicts': [], 'waves': waves}


def _setup_executor(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'SCHEDULER_LOCK', tmp_path / 'waves.lock')
    monkeypatch.setattr(runner, '_inspect', _plan_for)
    return [(tmp_path / str(i), tmp_path / f'receipt-{i}') for i in range(3)]


def test_serial_waits_for_each_accepted_owner(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)
    observed = []
    monkeypatch.setattr(runner, '_run_one', lambda cohort: observed.append(cohort['inventory'])
                        or {'state': 'accepted'})
    assert runner.execute(pairs, 1) == 0
    assert observed == [str(pair[0]) for pair in pairs]


def test_disjoint_wave_uses_parallel_owners_then_waits_for_both(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)
    barrier, lock, events = Barrier(2), Lock(), []

    def fake_owner(cohort):
        name = Path(cohort['inventory']).name
        with lock:
            events.append(('start', name))
        if name in ('0', '1'):
            barrier.wait(timeout=3)
        with lock:
            events.append(('finish', name))
        return {'state': 'accepted'}

    monkeypatch.setattr(runner, '_run_one', fake_owner)
    assert runner.execute(pairs, 2) == 0
    assert {event for event in events[:2]} == {('start', '0'), ('start', '1')}
    assert events.index(('start', '2')) > events.index(('finish', '0'))
    assert events.index(('start', '2')) > events.index(('finish', '1'))


def test_incomplete_owner_stops_later_waves(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)
    called = []
    monkeypatch.setattr(runner, '_run_one', lambda cohort: called.append(cohort['inventory'])
                        or {'state': 'owner_incomplete'})
    assert runner.execute(pairs, 1) == 75
    assert called == [str(pairs[0][0])]


def test_independent_waves_continue_after_failure_but_do_not_claim_completion(tmp_path, monkeypatch, capsys):
    pairs = _setup_executor(tmp_path, monkeypatch)
    called = []

    def run(cohort):
        called.append(cohort['inventory'])
        return {'state': 'owner_incomplete' if len(called) == 1 else 'accepted'}

    monkeypatch.setattr(runner, '_run_one', run)
    assert runner.execute(pairs, 1, continue_independent_waves=True) == 75
    assert called == [str(pair[0]) for pair in pairs]
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert events[-1]['event'] == 'incomplete'
    assert events[-1]['deferred_waves'] == [0]
    assert all(event['event'] != 'complete' for event in events)


@pytest.mark.parametrize('changed', ['inventory', 'capacity', 'owner'])
def test_independent_continuation_still_checks_each_wave(tmp_path, monkeypatch, changed):
    pairs = _setup_executor(tmp_path, monkeypatch)
    called = []

    def inspect(selected, parallelism):
        result = _plan_for(selected, parallelism)
        if selected == [pairs[1]]:
            if changed == 'inventory':
                result['cohorts'][0]['inventory_sha256'] = 'f' * 64
            elif changed == 'capacity':
                result['waves'][0]['capacity_deferrals'] = ['windows_c']
            else:
                result['cohorts'][0]['preflight_deferrals'] = ['busy']
        return result

    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, '_run_one', lambda c: called.append(c['inventory']) or {'state': 'accepted'})
    assert runner.execute(pairs, 1, continue_independent_waves=True) == 75
    assert called == [str(pairs[0][0]), str(pairs[2][0])]


def test_independent_continuation_never_accepts_overlapping_inputs(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)

    def inspect(selected, parallelism):
        result = _plan_for(selected, parallelism)
        result['overlap_conflicts'] = [{'cohorts': [0, 1]}]
        return result

    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, '_run_one', lambda c: pytest.fail('overlapping inputs must not run'))
    assert runner.execute(pairs, 1, continue_independent_waves=True) == 75


@pytest.mark.parametrize('problem', ['busy', 'global_visibility', 'valid_receipt', 'invalid_receipt'])
def test_independent_restart_distinguishes_prior_acceptance_from_missing_source(
        tmp_path, monkeypatch, problem):
    pairs = _setup_executor(tmp_path, monkeypatch)
    called = []

    def inspect(selected, parallelism):
        result = _plan_for(selected, parallelism)
        if selected == pairs:
            cohort = result['cohorts'][0]
            cohort['preflight_deferrals'] = [
                'active_agent_supervisors_unobservable' if problem == 'global_visibility' else
                'receipt_owner_busy_or_unobservable' if problem == 'busy' else
                'one_or_more_sources_absent_or_already_retired']
            cohort['historical_progress'] = 'retired_and_source_absent'
            result['waves'] = [{'cohorts': [1], 'capacity_deferrals': []},
                               {'cohorts': [2], 'capacity_deferrals': []}]
        return result

    def accepted(cohort):
        if problem == 'invalid_receipt':
            raise ValueError('source absence has no exact accepted receipt')
        return {'reclaimed_allocated_bytes': 123}

    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, '_accepted', accepted)
    monkeypatch.setattr(runner, '_run_one', lambda c: called.append(c['inventory']) or {'state': 'accepted'})
    code = runner.wait_and_execute(pairs, 1, continue_independent_waves=True)
    assert code == (0 if problem == 'valid_receipt' else 75)
    assert called == ([] if problem == 'global_visibility' else [str(p[0]) for p in pairs[1:]])


@pytest.mark.parametrize('blocker', ['cohort', 'capacity'])
def test_preflight_or_capacity_deferral_never_starts_owner(tmp_path, monkeypatch, blocker):
    pairs = _setup_executor(tmp_path, monkeypatch)
    monkeypatch.setattr(runner, '_inspect', lambda pairs, parallelism:
                        _plan_for(pairs, parallelism, deferred=blocker == 'cohort',
                                  capacity=blocker == 'capacity'))
    monkeypatch.setattr(runner, '_run_one', lambda cohort: pytest.fail('owner must not start'))
    assert runner.execute(pairs, 2) == 75


def test_capacity_wait_rechecks_without_holding_owner_lock_and_then_admits(tmp_path, monkeypatch):
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    observations = []
    monkeypatch.setattr(runner, '_inspect', lambda pairs, parallelism:
                        _plan_for(pairs, parallelism, capacity=len(observations) < 2))

    def wait(seconds):
        assert not runner.SCHEDULER_LOCK.exists()
        observations.append(seconds)

    monkeypatch.setattr(runner.time, 'sleep', wait)
    monkeypatch.setattr(runner, 'SCHEDULER_LOCK', tmp_path / 'unheld.lock')
    assert runner.wait_for_capacity(pairs, 1, poll_seconds=1) == 0
    assert observations == [1, 1]


@pytest.mark.parametrize('change', ['inventory', 'cohort'])
def test_capacity_wait_stops_on_inventory_or_owner_change(tmp_path, monkeypatch, change):
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    observed = []

    def inspect(pairs, parallelism):
        plan = _plan_for(pairs, parallelism, deferred=bool(observed) and change == 'cohort', capacity=True)
        if observed and change == 'inventory':
            plan['cohorts'][0]['inventory_sha256'] = 'f' * 64
        return plan

    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner.time, 'sleep', lambda seconds: observed.append(seconds))
    assert runner.wait_for_capacity(pairs, 1, poll_seconds=1) == 75
    assert observed == [1]


def test_fixed_capacity_wait_stops_when_publication_changes(tmp_path, monkeypatch):
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    observed = []
    def inspect(pairs, parallelism, **kwargs):
        plan = _plan_for(pairs, parallelism, capacity=True)
        plan['cohorts'][0]['fixed_d_publication'] = {'manifest_sha256': ('b' if observed else 'a') * 64}
        return plan
    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner.time, 'sleep', lambda seconds: observed.append(seconds))
    assert runner.wait_for_capacity(pairs, 1, poll_seconds=1, fixed_d_stage_only=True) == 75
    assert observed == [1]


def test_fixed_execution_rechecks_publication_before_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'SCHEDULER_LOCK', tmp_path / 'waves.lock')
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    calls = []
    def inspect(pairs, parallelism, **kwargs):
        calls.append(1)
        plan = _plan_for(pairs, parallelism)
        plan['cohorts'][0]['fixed_d_publication'] = {'manifest_sha256': ('a' if len(calls) == 1 else 'b') * 64}
        return plan
    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, '_run_one', lambda cohort: pytest.fail('changed publication must not launch'))
    assert runner.execute(pairs, 1, fixed_d_stage_only=True) == 75


def test_capacity_wait_identity_is_pinned_through_scheduler_handoff(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'SCHEDULER_LOCK', tmp_path / 'waves.lock')
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    state = {'manifest': 'a' * 64}
    def inspect(pairs, parallelism, **kwargs):
        plan = _plan_for(pairs, parallelism)
        plan['cohorts'][0]['fixed_d_publication'] = {'manifest_sha256': state['manifest']}
        return plan
    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, '_run_one', lambda cohort: pytest.fail('changed publication must not launch'))
    pinned = []
    assert runner.wait_for_capacity(pairs, 1, fixed_d_stage_only=True, handoff_identity=pinned) == 0
    assert pinned[0][1]['manifest_sha256'] == 'a' * 64
    state['manifest'] = 'b' * 64
    assert runner.execute(pairs, 1, fixed_d_stage_only=True, expected_handoff_identity=pinned) == 75


def test_queued_wave_rechecks_capacity_and_identity_after_scheduler_contention(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)[:1]
    attempts, waits, inspected = [], [], []
    original_inspect = runner._inspect
    def inspect(*args, **kwargs):
        inspected.append(1)
        return original_inspect(*args, **kwargs)
    def execute(*args, **kwargs):
        attempts.append(kwargs['expected_handoff_identity'])
        if len(attempts) == 1:
            raise runner.SchedulerBusy()
        return 0
    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, 'execute', execute)
    monkeypatch.setattr(runner.time, 'sleep', lambda seconds: waits.append(seconds))
    assert runner.wait_and_execute(pairs, 1, wait_for_scheduler=True, poll_seconds=1) == 0
    assert waits == [1] and len(inspected) == 2
    assert len(attempts) == 2 and attempts[0] == attempts[1]


def test_queued_wave_defers_if_inventory_changes_during_scheduler_wait(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)[:1]
    waits, owner_attempts = [], []
    def inspect(pairs, parallelism):
        plan = _plan_for(pairs, parallelism)
        if waits:
            plan['cohorts'][0]['inventory_sha256'] = 'f' * 64
        return plan
    def execute(*args, **kwargs):
        owner_attempts.append(1)
        raise runner.SchedulerBusy()
    monkeypatch.setattr(runner, '_inspect', inspect)
    monkeypatch.setattr(runner, 'execute', execute)
    monkeypatch.setattr(runner.time, 'sleep', lambda seconds: waits.append(seconds))
    assert runner.wait_and_execute(pairs, 1, wait_for_scheduler=True, poll_seconds=1) == 75
    assert waits == [1] and len(owner_attempts) == 1


def test_scheduler_busy_without_queue_fails_before_owner(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)[:1]
    runner.SCHEDULER_LOCK.touch()
    with runner.SCHEDULER_LOCK.open('r+') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monkeypatch.setattr(runner, '_run_one', lambda _: pytest.fail('owner must not start'))
        with pytest.raises(runner.SchedulerBusy):
            runner.execute(pairs, 1)


def _predecessor_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    run_id = 'old-frozen-run'
    inventory, receipt = tmp_path / 'inventory', tmp_path / 'receipt'
    inventory.write_bytes(b'frozen')
    receipt.mkdir()
    run_path = tmp_path / 'artifacts/operations/agent-workflow/runs' / run_id / 'run.json'
    run_path.parent.mkdir(parents=True)
    run = {'run_id': run_id, 'repo_root': str(tmp_path), 'host': 'penguin',
           'unit': f'stockagent-agent-{run_id}.service',
           'argv': ['/bin/bash', 'scripts/run_authority_storage_operation.sh',
                    'scripts/retire_local_offline_artifacts.py', 'apply', '--inventory',
                    str(inventory), '--receipt-dir', str(receipt), '--wait-for-owner'],
           'queued_at_utc': '2026-10-27T00:00:00+00:00',
           'started_at_utc': '2026-10-27T00:00:00+00:00',
           'process': {'pid': 1}, 'child_process': {'pid': 2}, 'state': 'failed', 'exit_code': 75}
    run_path.write_text(json.dumps(run))
    digest = hashlib.sha256(inventory.read_bytes()).hexdigest()
    pinned = {'inventory_sha256': digest, 'fixed_d_publication': {'manifest_sha256': 'a'*64},
              'sources': [{'relative_root': 'benchmarks/fixture', 'present': True}]}
    progress = {'contract': owner.MANUAL_WSL_CAPTURE_CONTRACT, 'inventory_sha256': digest,
                'all_selected_retired': False, 'checked_at_epoch': 1793059201,
                'items': [{'relative_root': 'benchmarks/fixture', 'retired': False,
                           'error': 'owned legacy full verification expired or its generation changed'}]}
    (receipt / 'progress.json').write_text(json.dumps(progress))
    monkeypatch.setattr(runner.planner, 'inspect_cohort', lambda *a, **kw: pinned)
    monkeypatch.setattr(runner, '_predecessor_unit_state', lambda unit: 'inactive')
    return run_id, inventory, receipt, run_path, run, pinned, progress


def test_predecessor_only_retries_fresh_known_failure_with_fixed_identity(tmp_path, monkeypatch):
    run_id, inventory, receipt, _, _, pinned, _ = _predecessor_fixture(tmp_path, monkeypatch)
    outcome, identity = runner.wait_for_predecessor((inventory, receipt), run_id, poll_seconds=1)
    assert outcome == 'retry'
    assert identity == [(pinned['inventory_sha256'], pinned['fixed_d_publication'])]


def test_predecessor_waits_without_a_scheduler_lock_then_handles_known_failure(tmp_path, monkeypatch):
    run_id, inventory, receipt, _, _, _, _ = _predecessor_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(runner, 'SCHEDULER_LOCK', tmp_path / 'not-held.lock')
    states, slept = iter(['active', 'inactive']), []
    monkeypatch.setattr(runner, '_predecessor_unit_state', lambda unit: next(states))
    def wait(seconds):
        assert not runner.SCHEDULER_LOCK.exists()
        slept.append(seconds)
    monkeypatch.setattr(runner.time, 'sleep', wait)
    assert runner.wait_for_predecessor((inventory, receipt), run_id, poll_seconds=1)[0] == 'retry'
    assert slept == [1]


def test_capacity_wait_rejects_predecessor_identity_change_before_first_poll(tmp_path, monkeypatch):
    pairs = [(tmp_path / 'inventory', tmp_path / 'receipt')]
    def inspect(pairs, parallelism, **kwargs):
        plan = _plan_for(pairs, parallelism)
        plan['cohorts'][0]['fixed_d_publication'] = {'manifest_sha256': 'b' * 64}
        return plan
    monkeypatch.setattr(runner, '_inspect', inspect)
    original = [(pairs[0][0].name.ljust(64, '0')[:64], {'manifest_sha256': 'a' * 64})]
    assert runner.wait_for_capacity(pairs, 1, fixed_d_stage_only=True, expected_identity=original) == 75


@pytest.mark.parametrize('failure', ['stale', 'other-error', 'other-owner', 'release-changed'])
def test_predecessor_does_not_retry_unknown_or_changed_evidence(tmp_path, monkeypatch, failure):
    run_id, inventory, receipt, run_path, run, pinned, progress = _predecessor_fixture(tmp_path, monkeypatch)
    if failure == 'stale':
        progress['checked_at_epoch'] = 0
    elif failure == 'other-error':
        progress['items'][0]['error'] = 'unexpected source data'
    elif failure == 'other-owner':
        run['argv'][5] = str(tmp_path / 'other-inventory')
        run_path.write_text(json.dumps(run))
    else:
        observations = []
        def inspect(*args, **kwargs):
            observations.append(1)
            return {**pinned, 'fixed_d_publication': {'manifest_sha256': ('a' if len(observations) == 1 else 'b')*64}}
        monkeypatch.setattr(runner.planner, 'inspect_cohort', inspect)
    (receipt / 'progress.json').write_text(json.dumps(progress))
    with pytest.raises(ValueError):
        runner.wait_for_predecessor((inventory, receipt), run_id, poll_seconds=1)


def test_predecessor_success_requires_independent_acceptance(tmp_path, monkeypatch):
    run_id, inventory, receipt, run_path, run, _, _ = _predecessor_fixture(tmp_path, monkeypatch)
    run.update(state='succeeded', exit_code=0)
    run_path.write_text(json.dumps(run))
    monkeypatch.setattr(runner, '_accepted', lambda cohort: {'items': 1})
    assert runner.wait_for_predecessor((inventory, receipt), run_id, poll_seconds=1)[0] == 'accepted'
    monkeypatch.setattr(runner, '_accepted', lambda cohort: (_ for _ in ()).throw(ValueError('no receipt')))
    with pytest.raises(ValueError, match='no receipt'):
        runner.wait_for_predecessor((inventory, receipt), run_id, poll_seconds=1)


@pytest.mark.parametrize('poll_seconds', [True, 0, 61])
def test_capacity_wait_bounds_poll_interval(tmp_path, poll_seconds):
    with pytest.raises(ValueError, match='poll interval'):
        runner.wait_for_capacity([(tmp_path / 'inventory', tmp_path / 'receipt')], 1,
                                 poll_seconds=poll_seconds)


def _completed_fixture(tmp_path):
    inventory = tmp_path / 'inventory.json'
    inventory.write_bytes(b'fixed inventory')
    digest = hashlib.sha256(inventory.read_bytes()).hexdigest()
    receipt, source = tmp_path / 'receipt', tmp_path / 'retired-source'
    accepted = {'dataset': 'legacy-one', 'relative_root': 'benchmarks/one', 'retired': True,
                'snapshot_id': 'snapshot-one', 'manifest_sha256': 'a' * 64,
                'reclaimed_allocated_bytes': 4096}
    (receipt / accepted['dataset']).mkdir(parents=True)
    (receipt / accepted['dataset'] / 'acceptance.json').write_text(json.dumps(accepted))
    progress = {'contract': owner.MANUAL_WSL_CAPTURE_CONTRACT, 'inventory_sha256': digest,
                'all_selected_retired': True, 'items': [accepted], 'reclaimed_allocated_bytes': 4096}
    (receipt / 'progress.json').write_text(json.dumps(progress))
    cohort = {'inventory': str(inventory), 'receipt_directory': str(receipt),
              'inventory_sha256': digest, 'sources': [{'relative_root': 'benchmarks/one',
                                                       'path': str(source)}]}
    return cohort, progress, source


def test_completion_requires_bound_owner_acceptance_and_source_absence(tmp_path):
    cohort, progress, source = _completed_fixture(tmp_path)
    assert runner._accepted(cohort)['reclaimed_allocated_bytes'] == 4096
    source.mkdir()
    with pytest.raises(ValueError, match='source is present'):
        runner._accepted(cohort)
    source.rmdir()
    progress['items'][0]['manifest_sha256'] = 'b' * 64
    Path(cohort['receipt_directory'], 'progress.json').write_text(json.dumps(progress))
    with pytest.raises(ValueError, match='acceptance differ'):
        runner._accepted(cohort)


def test_completion_rejects_inventory_change_and_redirected_acceptance(tmp_path):
    cohort, _progress, _source = _completed_fixture(tmp_path)
    inventory = Path(cohort['inventory'])
    inventory.write_bytes(b'changed')
    with pytest.raises(ValueError, match='inventory changed'):
        runner._accepted(cohort)
    inventory.write_bytes(b'fixed inventory')
    acceptance = Path(cohort['receipt_directory'], 'legacy-one', 'acceptance.json')
    target = tmp_path / 'elsewhere'
    acceptance.rename(target)
    acceptance.symlink_to(target)
    with pytest.raises(ValueError, match='redirected'):
        runner._accepted(cohort)


def test_owner_command_pins_inventory_and_zero_exit_alone_is_not_acceptance(tmp_path, monkeypatch):
    cohort, _progress, _source = _completed_fixture(tmp_path)
    cohort['canonical_argv'] = ['/bin/bash', 'existing-owner', 'apply']
    commands = []
    monkeypatch.setattr(runner.subprocess, 'run', lambda command, **kwargs:
                        commands.append(command) or SimpleNamespace(returncode=0))
    assert runner._run_one(cohort)['state'] == 'accepted'
    assert commands[0][-2:] == ['--expected-inventory-sha256', cohort['inventory_sha256']]
    Path(cohort['receipt_directory'], 'legacy-one', 'acceptance.json').unlink()
    assert runner._run_one(cohort)['state'] == 'not_accepted'


def test_execution_rejects_a_different_mount_namespace_before_launch(tmp_path, monkeypatch):
    pairs = _setup_executor(tmp_path, monkeypatch)
    monkeypatch.setattr(runner.os, 'readlink', lambda path: 'other' if '/self/' in path else 'authority')
    with pytest.raises(ValueError, match='authority mount namespace'):
        runner.execute(pairs, 1)
    assert not runner.SCHEDULER_LOCK.exists()


def test_only_actual_own_agent_cgroup_is_excluded(monkeypatch):
    pairs = [(Path('/inventory'), Path('/receipt'))]
    monkeypatch.setattr(runner, '_own_agent_run_id', lambda: 'ours')
    monkeypatch.setattr(runner.planner, 'observe_active_supervisors', lambda: [
        {'run_id': 'ours', 'argv': []}, {'run_id': 'other', 'argv': []}])
    seen = []
    monkeypatch.setattr(runner.planner, 'inspect_cohort',
                        lambda inventory, receipt, active: seen.extend(active) or {})
    monkeypatch.setattr(runner.planner, 'capacity_observation', lambda: {})
    monkeypatch.setattr(runner.planner, 'build_plan', lambda cohorts, parallelism, free: cohorts)
    runner._inspect(pairs, 1)
    assert [run['run_id'] for run in seen] == ['other']
