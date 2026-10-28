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
