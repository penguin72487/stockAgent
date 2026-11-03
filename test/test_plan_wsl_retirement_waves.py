import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import plan_wsl_retirement_waves as planner


def cohort(tmp_path, name, source, *, old_blockers=(), lock='not_busy_at_observation',
           historical='not_retired_or_interrupted'):
    return {'receipt_directory': str(tmp_path / 'receipts' / name),
            'sources': [{'resolved_path': str(source), 'present': True}],
            'logical_bytes_frozen': 1000, 'allocated_unique_file_bytes_frozen': 2000,
            'files_frozen': 2, 'old_inventory_blockers': list(old_blockers),
            'observed_receipt_owner': lock, 'historical_progress': historical,
            'observed_active_agent_runs': []}


def test_disjoint_cohorts_can_be_planned_in_serial_or_parallel(tmp_path):
    rows = [cohort(tmp_path, 'one', tmp_path / 'source' / 'one'),
            cohort(tmp_path, 'two', tmp_path / 'source' / 'two')]
    free = dict.fromkeys(('wsl', 'windows_c', 'primary_d'), 100 * 1024**3)
    serial = planner.build_plan(rows, 1, free)
    parallel = planner.build_plan(rows, 2, free)
    assert [w['cohorts'] for w in serial['waves']] == [[0], [1]]
    assert parallel['waves'][0]['cohorts'] == [0, 1]
    assert parallel['waves'][0]['capacity_deferrals'] == []
    assert parallel['waves'][0]['capacity_is_reserved'] is False
    assert parallel['authorizes_launch_or_retirement'] is False


@pytest.mark.parametrize('mode', ['fixed_d_stage_only', 'd_stage_recovery_only'])
def test_backup_pool_is_spendable_only_by_explicit_guarded_d_maintenance(tmp_path, mode):
    row = cohort(tmp_path, 'one', tmp_path / 'one')
    row[mode] = True
    if mode == 'fixed_d_stage_only':
        row['fixed_d_publication'] = {'max_temporary_object_bytes': 4096}
    free = dict.fromkeys(('wsl', 'windows_c', 'primary_d'), 1024**3)
    assert planner.build_plan([row], 1, free)['waves'][0]['capacity_deferrals']
    plan = planner.build_plan([row], 1, free, use_backup_reserve=True)
    wave = plan['waves'][0]
    assert wave['capacity_deferrals'] == []
    assert wave['required_bytes_heuristic'] == wave['workspace_bytes_heuristic']
    assert wave['retained_reserve_bytes'] == 0
    assert plan['backup_reserve_pool_bytes'] == planner.RESERVE
    assert plan['authorizes_launch_or_retirement'] is False
    for volume in ('wsl', 'windows_c', 'primary_d'):
        for available in (None, wave['workspace_bytes_heuristic'][volume] - 1):
            limited = {**free, volume: available}
            blocked = planner.build_plan([row], 1, limited, use_backup_reserve=True)
            assert volume in blocked['waves'][0]['capacity_deferrals']


def test_backup_pool_never_changes_unscoped_source_sized_work(tmp_path):
    row = cohort(tmp_path, 'one', tmp_path / 'one')
    with pytest.raises(ValueError, match='guarded-D'):
        planner.build_plan([row], 1, {}, use_backup_reserve=True)


def test_overlapping_sources_receipts_and_old_blockers_defer(tmp_path):
    one = cohort(tmp_path, 'one', tmp_path / 'source')
    two = cohort(tmp_path, 'one/child', tmp_path / 'source' / 'child', old_blockers=['old'])
    result = planner.build_plan([one, two], 2, {})
    assert {c['kind'] for c in result['overlap_conflicts']} == {
        'source_roots_overlap', 'receipt_directories_overlap'}
    assert result['waves'] == []
    assert 'frozen_inventory_had_blockers_reinventory_first' in two['preflight_deferrals']


def test_capacity_is_sum_per_wave_and_unknown_mount_fails_closed(tmp_path):
    rows = [cohort(tmp_path, 'one', tmp_path / 'one'), cohort(tmp_path, 'two', tmp_path / 'two')]
    serial = planner.build_plan(rows, 1, {'wsl': 100 * 1024**3, 'windows_c': None,
                                          'primary_d': 100 * 1024**3})
    parallel = planner.build_plan(rows, 2, {'wsl': 100 * 1024**3, 'windows_c': None,
                                            'primary_d': 100 * 1024**3})
    assert serial['waves'][0]['capacity_deferrals'] == ['windows_c']
    assert parallel['waves'][0]['required_bytes_heuristic']['primary_d'] > serial['waves'][0]['required_bytes_heuristic']['primary_d']


def test_fixed_d_only_does_not_budget_source_copy_on_c_and_keeps_floor(tmp_path):
    full = cohort(tmp_path, 'full', tmp_path / 'source')
    full.update(logical_bytes_frozen=14 * 1024**3, files_frozen=78000)
    fixed = dict(full, fixed_d_stage_only=True,
                 fixed_d_publication={'manifest_sha256': 'a' * 64, 'max_temporary_object_bytes': 123456})
    free = {'wsl': 100 * 1024**3, 'windows_c': 40 * 1024**3, 'primary_d': 100 * 1024**3}
    normal = planner.build_plan([full], 1, free)['waves'][0]
    resumed = planner.build_plan([fixed], 1, free)['waves'][0]
    assert normal['capacity_deferrals'] == ['windows_c']
    assert resumed['capacity_deferrals'] == []
    assert resumed['required_bytes_heuristic']['windows_c'] == planner.RESERVE + 78000 * planner.PER_FILE_FORECAST + 123456
    assert resumed['required_bytes_heuristic']['primary_d'] == normal['required_bytes_heuristic']['primary_d']
    assert resumed['local_forecast_mode'] == 'fixed_d_stage_control_only'
    assert not resumed['capacity_is_reserved']


def test_fixed_d_only_without_observed_publication_cannot_launch(tmp_path):
    row = cohort(tmp_path, 'fixed', tmp_path / 'source')
    row.update(fixed_d_stage_only=True, fixed_d_publication=None)
    result = planner.build_plan([row], 1, dict.fromkeys(('wsl', 'windows_c', 'primary_d'), 100 * 1024**3))
    assert result['waves'] == []
    assert 'fixed_d_stage_or_publication_unavailable' in row['preflight_deferrals']


def test_completed_fixed_stage_inspection_does_not_read_retired_temporary_files(tmp_path, monkeypatch):
    artifact, receipt = tmp_path/'artifacts', tmp_path/'receipts'
    receipt.mkdir()
    inventory = tmp_path/'inventory.json'
    inventory.write_text(json.dumps({'schema_version': 1, 'contract': planner.owner.MANUAL_WSL_CAPTURE_CONTRACT,
        'authority_node_id': 'penguin', 'items': [{'relative_root': 'benchmarks/one', 'logical_bytes': 100,
            'allocated_unique_file_bytes': 128, 'files': 1, 'blockers': []}]}))
    (receipt/'progress.json').write_text(json.dumps({'inventory_sha256': hashlib.sha256(inventory.read_bytes()).hexdigest(),
        'all_selected_retired': True}))
    monkeypatch.setattr(planner.owner, 'source_layout', lambda origin: (artifact, None, None, None))
    monkeypatch.setattr(planner.owner, 'stage_profile', lambda data: (tmp_path/'stage', True))
    monkeypatch.setattr(planner.owner, 'require_existing_fixed_d_stage',
        lambda *a, **kw: pytest.fail('completed cohort must not read a retired stage'))
    observed = planner.inspect_cohort(inventory, receipt, [], fixed_d_stage_only=True)
    assert observed['historical_progress'] == 'retired_and_source_absent'
    plan = planner.build_plan([observed], 1, {})
    assert observed['preflight_deferrals'] == ['one_or_more_sources_absent_or_already_retired']
    assert plan['waves'] == []  # The canonical acceptance still has to verify the fixed release.


def test_guarded_d_recovery_budget_omits_only_the_disallowed_local_payload(tmp_path):
    row = cohort(tmp_path, 'guarded', tmp_path / 'source')
    row.update(logical_bytes_frozen=14 * 1024**3, files_frozen=78000)
    free = {'wsl': 100 * 1024**3, 'windows_c': 40 * 1024**3, 'primary_d': 100 * 1024**3}
    original = planner.build_plan([dict(row)], 1, free)['waves'][0]
    current = planner.build_plan([dict(row, d_stage_recovery_only=True)], 1, free)['waves'][0]
    assert original['capacity_deferrals'] == ['windows_c']
    assert current['capacity_deferrals'] == []
    assert current['required_bytes_heuristic']['windows_c'] == planner.RESERVE + 78000 * planner.PER_FILE_FORECAST
    assert current['required_bytes_heuristic']['primary_d'] == original['required_bytes_heuristic']['primary_d']
    assert current['local_forecast_mode'] == 'guarded_d_encoding_recovery_only'
    assert current['capacity_is_reserved'] is False


def test_guarded_d_recovery_rejects_non_d_scope_before_any_owner(tmp_path, monkeypatch):
    path = tmp_path / 'inventory.json'
    path.write_text(json.dumps({'schema_version': 1, 'contract': planner.owner.MANUAL_WSL_CAPTURE_CONTRACT,
        'authority_node_id': 'penguin', 'items': [{'relative_root': 'benchmarks/one'}]}))
    monkeypatch.setattr(planner.owner, 'source_layout', lambda *args: (tmp_path, None, None, None))
    monkeypatch.setattr(planner.owner, 'stage_profile', lambda data: (tmp_path / 'not-d', True))
    with pytest.raises(ValueError, match='guarded-D'):
        planner.inspect_cohort(path, tmp_path / 'receipts', [], d_stage_recovery_only=True)


def test_busy_or_inconsistent_receipt_never_yields_a_wave(tmp_path):
    rows = [cohort(tmp_path, 'one', tmp_path / 'one', lock='busy'),
            cohort(tmp_path, 'two', tmp_path / 'two', historical='retired_but_source_present_reconcile')]
    assert planner.build_plan(rows, 2, {})['waves'] == []


def test_waiting_supervisor_blocks_launch_even_without_receipt_lock(tmp_path):
    row = cohort(tmp_path, 'one', tmp_path / 'one')
    row['observed_active_agent_runs'] = ['waiting-for-prior-cohort']
    result = planner.build_plan([row], 1, {})
    assert result['waves'] == []
    assert 'active_or_waiting_agent_run_already_mentions_cohort' in row['preflight_deferrals']


def test_active_supervisor_observes_mixed_case_agent_run_id(tmp_path, monkeypatch):
    run_id = 'waiting-20261027T130311-abc123'
    run_dir = tmp_path / 'artifacts/operations/agent-workflow/runs' / run_id
    run_dir.mkdir(parents=True)
    (run_dir / 'run.json').write_text(json.dumps({'run_id': run_id, 'argv': ['waiting']}))
    monkeypatch.setattr(planner, 'ROOT', tmp_path)
    monkeypatch.setattr(planner.subprocess, 'run', lambda *a, **kw: SimpleNamespace(
        stdout=f'stockagent-agent-{run_id}.service loaded active running\n'))
    assert planner.observe_active_supervisors() == [{'run_id': run_id, 'argv': ['waiting']}]


def test_invalid_frozen_inventory_is_rejected(tmp_path):
    path = tmp_path / 'inventory.json'
    path.write_text(json.dumps({'schema_version': 1, 'items': []}))
    with pytest.raises(ValueError, match='invalid frozen inventory'):
        planner.inspect_cohort(path, tmp_path / 'receipt')


@pytest.mark.parametrize('broken', ['no-policy', 'no-links', 'existing-dataset', 'wrong-scope'])
def test_symlink_inventory_is_bound_to_exact_explicit_benchmark_policy(tmp_path, broken):
    item = {'relative_root': 'benchmarks/suite/case', 'symlink_policy': planner.owner.OPAQUE_SYMLINK_POLICY,
            'symlinks': [], 'logical_bytes': 100, 'allocated_unique_file_bytes': 128,
            'files': 1, 'blockers': []}
    inventory = {'schema_version': 1, 'contract': planner.owner.MANUAL_WSL_CAPTURE_CONTRACT,
                 'authority_node_id': 'penguin', 'symlink_policy': planner.owner.OPAQUE_SYMLINK_POLICY,
                 'items': [item]}
    if broken == 'no-policy':
        inventory.pop('symlink_policy')
    elif broken == 'no-links':
        item.pop('symlinks')
    elif broken == 'existing-dataset':
        item['existing_dataset'] = 'other'
    else:
        item['relative_root'] = 'cache/suite/case'
    path = tmp_path / 'inventory.json'
    path.write_text(json.dumps(inventory))
    with pytest.raises(ValueError, match='symlink'):
        planner.inspect_cohort(path, tmp_path / 'receipt')


def test_inspection_finds_waiter_and_preserves_existing_owner(tmp_path, monkeypatch):
    artifact = tmp_path / 'artifacts'
    source = artifact / 'benchmarks' / 'one'
    source.mkdir(parents=True)
    inventory = tmp_path / 'inventory.json'
    inventory.write_text(json.dumps({
        'schema_version': 1, 'contract': planner.owner.MANUAL_WSL_CAPTURE_CONTRACT,
        'authority_node_id': 'penguin', 'items': [{
            'relative_root': 'benchmarks/one', 'logical_bytes': 100,
            'allocated_unique_file_bytes': 128, 'files': 1, 'blockers': []}]}))
    monkeypatch.setattr(planner.owner, 'source_layout', lambda origin: (artifact, None, None, None))
    monkeypatch.setattr(planner.owner, 'stage_profile', lambda data: (tmp_path / 'stage', True))
    observed = planner.inspect_cohort(inventory, tmp_path / 'receipt', [
        {'run_id': 'waiting', 'argv': ['python -c', f'future apply {inventory}']}])
    assert observed['observed_active_agent_runs'] == ['waiting']
    assert observed['sources'][0]['present'] is True
    assert observed['canonical_argv'][2:4] == ['scripts/retire_local_offline_artifacts.py', 'apply']
    assert planner.build_plan([observed], 1, {})['waves'] == []


@pytest.mark.parametrize('defect', ['none', 'manifest', 'unexpected-entry', 'redirected', 'missing-plan'])
def test_only_bound_interrupted_quarantine_is_a_resume_candidate(tmp_path, monkeypatch, defect):
    artifact, hot, states = (tmp_path / name for name in ('artifacts', 'hot', 'states'))
    for path in (artifact, hot, states):
        path.mkdir()
    monkeypatch.setattr(planner.owner, 'source_layout', lambda origin: (artifact, hot, states, None))
    monkeypatch.setattr(planner.owner, 'stage_profile', lambda data: (tmp_path / 'stage', True))
    relative, fingerprint = 'benchmarks/suite/one', 'a' * 64
    dataset = planner.owner.dataset_prefix(None) + hashlib.sha256(
        (relative + fingerprint).encode()).hexdigest()[:24]
    inventory = tmp_path / 'inventory.json'
    inventory.write_text(json.dumps({'schema_version': 1,
        'contract': planner.owner.MANUAL_WSL_CAPTURE_CONTRACT, 'authority_node_id': 'penguin',
        'items': [{'relative_root': relative, 'fingerprint': fingerprint,
                   'logical_bytes': 10, 'allocated_unique_file_bytes': 16,
                   'files': 1, 'blockers': []}]}))
    receipt = tmp_path / 'receipt'
    plan_path = receipt / dataset / 'retirement-plan.json'
    plan_path.parent.mkdir(parents=True)
    parent = states / 'retirements/quarantine' / dataset
    quarantine = parent / 'owned'
    (quarantine / 'source').mkdir(parents=True)
    plan = {'dataset': dataset, 'relative_root': relative,
            'source': str(artifact / relative), 'hot_tree': str(hot / relative),
            'manual_immediate': True, 'manual_capture': True, 'plan_fingerprint': 'f' * 64,
            'snapshot_id': 'fixed-snapshot', 'manifest_sha256': 'c' * 64,
            'full_verification': {'binding': {'dataset': dataset, 'hot': 'absent'}}}
    state = {key: plan[key] for key in ('dataset', 'relative_root', 'plan_fingerprint',
                                         'snapshot_id', 'manifest_sha256')}
    state.update(state='retiring', quarantine=str(quarantine))
    state_path = states / 'retirements' / (dataset + '.json')
    if defect == 'manifest':
        state['manifest_sha256'] = 'd' * 64
    elif defect == 'unexpected-entry':
        (quarantine / 'unknown').touch()
    elif defect == 'redirected':
        (quarantine / 'source').rmdir()
        (quarantine / 'source').symlink_to(artifact, target_is_directory=True)
    state_path.write_text(json.dumps(state))
    if defect != 'missing-plan':
        plan_path.write_text(json.dumps(plan))
    observed = planner.inspect_cohort(inventory, receipt, [])
    plan_result = planner.build_plan([observed], 1,
        dict.fromkeys(('wsl', 'windows_c', 'primary_d'), 100 * 1024**3))
    candidate = observed['sources'][0]['quarantine_resume_candidate']
    if defect == 'none':
        assert candidate['dataset'] == dataset and candidate['requires_fresh_owner_proofs']
        assert plan_result['waves'][0]['cohorts'] == [0]
        assert plan_result['authorizes_launch_or_retirement'] is False
    else:
        assert candidate is None and plan_result['waves'] == []
        assert 'one_or_more_sources_absent_or_already_retired' in observed['preflight_deferrals']
