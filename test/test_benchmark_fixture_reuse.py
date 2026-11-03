import fcntl
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from stockagent.data_sync import benchmark_fixtures as fixtures
from stockagent.data_sync.benchmark_fixtures import ImmutableFixtureInputs, RECEIPT


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _workflow_round_case(parent, number=7):
    source = parent / f'artifacts/benchmarks/workflow_speed_optimization_20261021/round{number}'
    source.mkdir(parents=True)
    acceptance = {'contract': fixtures.WORKFLOW_ROUND_CONTRACTS[number],
        'state': 'verified_scoped_engineering', 'observed_at_utc': '2026-10-22T13:24:21+00:00',
        'scope': 'original fixed-input measurements; no all-source or live-service claim',
        'evidence': [{'path': 'scripts/producer.py', 'bytes': 12, 'sha256': 'a' * 64},
                     {'path': str(source.relative_to(parent) / 'trial.json'), 'bytes': 13, 'sha256': 'b' * 64}]}
    boundaries = {'all_workflows_measured': False, 'hardware_limit_proven': False}
    if number in (8, 9, 10, 11, 12):
        acceptance.update(boundaries)
    else:
        acceptance['remaining_boundaries'] = boundaries
    readback = {'state': 'verified', 'observed_at_utc': '2026-10-22T13:24:29+00:00',
        'collector_phases_verified': 13, **boundaries,
        ('evidence_files_verified' if number == 6 else 'sealed_files_verified'): 2,
        ('performance_samples_verified' if number in (9, 10, 11) else
         'optimization_samples_verified' if number == 8 else 'raw_samples_verified'): 4}
    objects = dict(zip(fixtures.WORKFLOW_ROUND_RECEIPTS,
        (acceptance, readback, {'state': 'prepared' if number in (8, 9, 10, 11) else 'reviewed'}), strict=True))
    if number == 8:
        acceptance.update(optimization_samples=4, confirmation_samples=2,
                          instrumentation_comparison_samples=2, instrumented_diagnostic_samples=1)
        readback.update(confirmation_samples_verified=2, instrumentation_comparison_samples_verified=2,
                        diagnostic_counter_coverage_verified=True, remaining_registry_counts_verified=True,
                        mean_checks=16, speed_samples_verified=48, summary_groups_verified=8)
        for value in objects.values():
            value.update(selected_variant='fifo_batch256', selected_batch_mib=256,
                         performance_candidate_adopted=True)
    if number == 9:
        acceptance.update(performance_samples=4, diagnostic_samples=1,
                          selected_source='baseline_with_tests', selected_batch_mib=256,
                          selected_read_workers=1, performance_candidate_adopted=False)
        readback.update(diagnostic_samples_verified=1, serialization_operations_verified=5,
                        remaining_registry_counts_verified=True, mean_checks=16,
                        speed_samples_verified=48, summary_groups_verified=8,
                        performance_candidate_adopted=False)
        objects['source_disposition.json'].update(selected_source='baseline_with_tests',
            selected_batch_mib=256, performance_candidate_adopted=False)
    if number == 10:
        acceptance.update(performance_samples=4, encoding_samples=6, encoding_diagnostics=1,
                          selected_source='baseline_source', selected_batch_mib=256,
                          selected_read_workers=1, performance_candidate_adopted=False)
        readback.update(encoding_samples_verified=6, encoding_diagnostic_operations_verified=5,
                        remaining_registry_counts_verified=True, mean_checks=16,
                        speed_samples_verified=48, summary_groups_verified=8,
                        performance_candidate_adopted=False)
        objects['source_disposition.json'].update(selected_source='baseline_source',
            selected_batch_mib=256, performance_candidate_adopted=False,
            source_data_deleted=False, services_restarted=False, new_training_jobs=0)
    if number == 12:
        acceptance.update(performance_samples=8, fresh_confirmation_samples=4,
            selected_source='optimized_source', performance_candidate_adopted=True,
            all_six_outputs_equal=True, original_memory_gate_rejection_retained=True,
            remote_strict_environment_passed=False)
        readback.pop('raw_samples_verified')
        readback.pop('collector_phases_verified')
        readback.update(raw_complete_samples_verified=8, full_collector_phases=13,
            mean_checks=36, all_six_outputs_equal=True, selected_source='optimized_source')
        objects['source_disposition.json'].update(state='selected_source_verified',
            selected='optimized_source', production_services_restarted=0)
    def write(*, bind=True):
        raw = json.dumps(acceptance).encode()
        (source / 'optimization_acceptance.json').write_bytes(raw)
        if bind:
            readback['acceptance_sha256' if number == 6 else 'seal_sha256'] = hashlib.sha256(raw).hexdigest()
        for name in fixtures.WORKFLOW_ROUND_RECEIPTS[1:]:
            (source / name).write_text(json.dumps(objects[name]))
    write()
    return source, objects, write


@pytest.mark.parametrize('number', [6, 7, 8, 9, 10, 11, 12])
def test_workflow_round_retains_small_original_receipts_and_preserves_scoped_claims(tmp_path, number):
    source, objects, _ = _workflow_round_case(tmp_path, number)
    # Real rounds include giant JSON measurement payloads. They belong to the
    # full exact archive, never the locally retained control-metadata parser.
    payload = source / 'encoding.manifest.json'
    with payload.open('wb') as handle:
        handle.truncate(16 * 1024**2)
    contract = fixtures.WORKFLOW_ROUND_CONTRACTS[number]
    assert fixtures.external_benchmark_contract(source) == contract
    kept = tmp_path / 'retained-evidence'
    result = fixtures.completed_benchmark_evidence(source, destination=kept)
    assert {row['path'] for row in result['files']} == set(fixtures.WORKFLOW_ROUND_RECEIPTS)
    assert result['producer_state'] == 'verified_scoped_engineering'
    assert result['scope'] == 'producer_declared_completion_not_benchmark_reexecution'
    assert payload.is_file() and result['bytes'] < 8192
    for name in fixtures.WORKFLOW_ROUND_RECEIPTS:
        assert (kept / name).read_bytes() == (source / name).read_bytes()
    source.rename(source.with_name(source.name + '-still-preserved'))
    assert fixtures.completed_benchmark_evidence(kept, original_source=source,
        required_contract=contract) == result
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(kept, required_contract=contract)


@pytest.mark.parametrize('problem', [
    'sample_mismatch', 'bool_samples', 'phases', 'outputs', 'selected', 'disposition',
    'lost_rejection', 'remote_claim', 'service_restart', 'changed_seal', 'unreviewed_round',
])
def test_workflow_round12_requires_original_reader_abi_and_preserved_failure(tmp_path, problem):
    source, objects, write = _workflow_round_case(tmp_path, 12)
    accepted, readback, disposition = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    if problem == 'sample_mismatch': readback['raw_complete_samples_verified'] = 7
    elif problem == 'bool_samples': readback['raw_complete_samples_verified'] = True
    elif problem == 'phases': readback['full_collector_phases'] = 12
    elif problem == 'outputs': readback['all_six_outputs_equal'] = False
    elif problem == 'selected': readback['selected_source'] = 'different_source'
    elif problem == 'disposition': disposition['state'] = 'prepared'
    elif problem == 'lost_rejection': accepted['original_memory_gate_rejection_retained'] = False
    elif problem == 'remote_claim': accepted['remote_strict_environment_passed'] = True
    elif problem == 'service_restart': disposition['production_services_restarted'] = 1
    elif problem == 'changed_seal': accepted['scope'] += ' modified'
    elif problem == 'unreviewed_round':
        target = source.with_name('round13')
        source.rename(target)
        source = target
    if problem != 'unreviewed_round': write(bind=problem != 'changed_seal')
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(source)


def _workflow_round15_case(parent):
    root = parent / 'artifacts/benchmarks/workflow_speed_optimization_20261021/round15'
    root.mkdir(parents=True)
    prefix = 'artifacts/benchmarks/workflow_speed_optimization_20261021/round15/'
    accepted = {'contract': fixtures.WORKFLOW_ROUND_CONTRACTS[15], 'state': 'verified',
        'observed_at_utc': '2026-10-23T09:11:06+00:00',
        'source_archive_verified_at_seal': True,
        'files': [{'path': prefix + role + '/source.py', 'sha256': 'a' * 64, 'bytes': 1}
                  for role in ('baseline_source', 'candidate_source')]}
    readback = {'state': 'verified', 'observed_at_utc': '2026-10-23T09:11:18+00:00',
        'sealed_files_verified': 2, 'complete_fixed_input_trials_verified': 8, 'means_recomputed': 32}
    for receipt in (accepted, readback):
        receipt.update(dict.fromkeys(('candidate_admitted', 'full_collector_verified',
            'full_collector_speedup_verified', 'all_workflows_measured', 'hardware_limit_proven'), False))
    def write(bind=True):
        body = json.dumps(accepted).encode()
        if bind:
            readback['seal_sha256'] = hashlib.sha256(body).hexdigest()
        (root / fixtures.WORKFLOW_ROUND_RECEIPTS[0]).write_bytes(body)
        (root / fixtures.WORKFLOW_ROUND_RECEIPTS[1]).write_text(json.dumps(readback))
    write()
    return root, accepted, readback, write


def _workflow_round18_case(parent):
    root = parent / 'artifacts/benchmarks/workflow_speed_optimization_20261021/round18'
    root.mkdir(parents=True)
    prefix = root.relative_to(parent).as_posix() + '/'
    terminal = [{'run_id': 'good', 'state': 'succeeded', 'exit_code': 0},
                {'run_id': 'failed', 'state': 'failed', 'exit_code': 127},
                {'run_id': 'interrupted', 'state': 'interrupted', 'exit_code': None}]
    accepted = {'state': 'verified_scoped_progress', 'observed_at_utc': '2026-10-23T13:16:08+00:00',
        'files': [{'path': prefix + role + '/source.py', 'sha256': 'a' * 64, 'bytes': 12}
                  for role in ('candidate_source', 'canonical_source')],
        'terminal_runs': terminal, 'raw_current_registry_entries': 2144, 'raw_current_stage_bound': 1511,
        'stage_bound': 1646, 'remaining_stage_unbound': 753}
    readback = {'state': 'verified', 'observed_at_utc': '2026-10-23T13:16:45+00:00',
        'sealed_files_verified': 2, 'terminal_command_receipts_verified': 3,
        'raw_current_registry_entries': 2144, 'raw_current_stage_bound': 1511,
        'stage_bound_recounted': 1646, 'remaining_unbound_recounted': 753,
        'complete_fixed_trials_verified': 8, 'full_single_invocation_gap_free': True,
        'full_collector_speedup_verified': False, 'failed_commands_retained': ['failed'],
        'interrupted_commands_retained': ['interrupted']}
    for row in (accepted, readback):
        row.update(dict.fromkeys(('candidate_accepted', 'single_invocation_registry_scope_complete',
                                 'all_workflows_measured', 'hardware_limit_proven'), False))
        row['original_registry_scope_reconciled'] = True
    disposition = {'state': 'selected_source_verified', 'selected_source': 'baseline', 'provider_calls': 0,
        **dict.fromkeys(('cache_candidate_accepted', 'default_reuse_enabled', 'source_or_schema_writes',
                        'service_restarts', 'new_gpu_jobs'), False)}
    def write(bind=True):
        body = json.dumps(accepted).encode()
        if bind:
            readback['seal_sha256'] = hashlib.sha256(body).hexdigest()
        for name, value in zip(fixtures.WORKFLOW_ROUND_RECEIPTS,
                               (accepted, readback, disposition), strict=True):
            (root / name).write_text(json.dumps(value))
    write()
    return root, accepted, readback, disposition, write


def test_round18_preserves_rejected_candidate_and_terminal_failures_in_retained_evidence(tmp_path):
    root, accepted, _, _, _ = _workflow_round18_case(tmp_path)
    retained = tmp_path / 'retained-evidence'
    proof = fixtures.completed_benchmark_evidence(root, destination=retained)
    assert proof['producer_contract'] == fixtures.WORKFLOW_ROUND_CONTRACTS[18]
    assert len(proof['files']) == 3
    assert json.loads((retained / fixtures.WORKFLOW_ROUND_RECEIPTS[0]).read_text()) == accepted
    assert fixtures.completed_benchmark_evidence(retained, original_source=root,
        required_contract=fixtures.WORKFLOW_ROUND_CONTRACTS[18]) == proof


@pytest.mark.parametrize('defect', ['seal', 'count', 'bool-count', 'trials', 'failure-removed',
    'failure-renamed', 'duplicate-run', 'failed-zero', 'unbound-count', 'candidate', 'claim',
    'clock', 'source', 'scope', 'state', 'provider-call', 'rejected-baseline'])
def test_round18_rejects_unbound_or_rewritten_original_evidence(tmp_path, defect):
    root, accepted, readback, disposition, write = _workflow_round18_case(tmp_path)
    if defect == 'seal': accepted['files'][0]['bytes'] += 1
    elif defect == 'count': readback['sealed_files_verified'] = 1
    elif defect == 'bool-count': readback['terminal_command_receipts_verified'] = True
    elif defect == 'trials': readback['complete_fixed_trials_verified'] = 7
    elif defect == 'failure-removed': readback['failed_commands_retained'] = []
    elif defect == 'failure-renamed': readback['failed_commands_retained'] = ['other']
    elif defect == 'duplicate-run': accepted['terminal_runs'][1]['run_id'] = 'good'
    elif defect == 'failed-zero': accepted['terminal_runs'][1]['exit_code'] = 0
    elif defect == 'unbound-count': readback['remaining_unbound_recounted'] = 0
    elif defect == 'candidate': disposition['default_reuse_enabled'] = True
    elif defect == 'claim': readback['hardware_limit_proven'] = True
    elif defect == 'clock': readback['observed_at_utc'] = '2026-10-23T13:00:00+00:00'
    elif defect == 'source': accepted['files'][0]['path'] = 'unrelated/source.py'
    elif defect == 'scope': accepted['files'][0]['path'] = '../outside.py'
    elif defect == 'state': accepted['state'] = 'running'
    elif defect == 'provider-call': disposition['provider_calls'] = True
    elif defect == 'rejected-baseline': disposition['selected_source'] = 'candidate'
    write(bind=defect != 'seal')
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(root)


def test_round15_preserves_original_negative_two_receipt_abi_and_quarantine(tmp_path):
    root, accepted, _, _ = _workflow_round15_case(tmp_path)
    (root / 'unbounded_measurements.json').write_text('not completion metadata')
    result = fixtures.completed_benchmark_evidence(root, destination=tmp_path / 'retained-evidence')
    assert result['producer_contract'] == fixtures.WORKFLOW_ROUND_CONTRACTS[15]
    assert len(result['files']) == 2
    retained = tmp_path / 'retained-evidence'
    assert json.loads((retained / fixtures.WORKFLOW_ROUND_RECEIPTS[0]).read_text()) == accepted
    states = tmp_path / 'states/retirements'
    quarantine = states / 'quarantine/round15-fixture' / ('123-' + 'a' * 32)
    quarantine.mkdir(parents=True)
    moved = quarantine / 'source'
    root.rename(moved)
    (states / 'round15-fixture.json').write_text(json.dumps({
        'schema_version': 1, 'dataset': 'round15-fixture',
        'relative_root': 'benchmarks/workflow_speed_optimization_20261021/round15',
        'state': 'retiring', 'quarantine': str(quarantine),
        'snapshot_id': 'fixed-release', 'manifest_sha256': 'c' * 64}))
    assert fixtures.completed_benchmark_evidence(moved, original_source=root,
        required_contract=fixtures.WORKFLOW_ROUND_CONTRACTS[15]) == result


def test_round15_historical_external_references_are_evidence_only(tmp_path):
    root, accepted, readback, write = _workflow_round15_case(tmp_path)
    for name in ('artifacts/benchmarks/workflow_speed_optimization_20261021/round12/confirmation_worker.py',
            'artifacts/operations/agent-workflow/runs/workflow-r15-complete-abba-20261023T083653-example/run.json',
            'docs/workflow_speed_optimization_2026-10-23_scalar_prefilter.md'):
        accepted['files'].append({'path': name, 'bytes': 0, 'sha256': 'b' * 64})
    readback['sealed_files_verified'] = len(accepted['files'])
    write()
    assert len(fixtures.completed_benchmark_evidence(root)['files']) == 2


@pytest.mark.parametrize('defect', ['seal', 'count', 'bool-count', 'trials', 'means', 'claim',
    'source', 'path', 'duplicate', 'clock', 'source-proof', 'state'])
def test_round15_rejects_unbound_incomplete_or_misrepresented_experiment(tmp_path, defect):
    root, accepted, readback, write = _workflow_round15_case(tmp_path)
    if defect == 'seal': accepted['files'][0]['bytes'] = 2
    elif defect == 'count': readback['sealed_files_verified'] = 1
    elif defect == 'bool-count': readback['sealed_files_verified'] = True
    elif defect == 'trials': readback['complete_fixed_input_trials_verified'] = 7
    elif defect == 'means': readback['means_recomputed'] = 31
    elif defect == 'claim': readback['candidate_admitted'] = True
    elif defect == 'source': accepted['files'].pop()
    elif defect == 'path': accepted['files'][0]['path'] = 'unrelated/source.py'
    elif defect == 'duplicate': accepted['files'][1] = accepted['files'][0].copy()
    elif defect == 'clock': readback['observed_at_utc'] = '2026-10-23T01:00:00+00:00'
    elif defect == 'source-proof': accepted['source_archive_verified_at_seal'] = False
    elif defect == 'state': accepted['state'] = 'running'
    write(bind=defect != 'seal')
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(root)


def _workflow_round13_case(parent):
    root = parent / 'artifacts/benchmarks/workflow_speed_optimization_20261021/round13'
    root.mkdir(parents=True)
    sources = [{'path': 'scripts/producer.py', 'sha256': 'a' * 64}]
    accepted = {'contract': fixtures.WORKFLOW_ROUND_CONTRACTS[13], 'state': 'verified',
        'observed_at_utc': '2026-10-23T08:14:55+00:00',
        'files': [{'path': 'canonical_source/scripts/producer.py', 'bytes': 12, 'sha256': 'a' * 64}],
        'recomputed_means': [{'mean': 1.5}], 'current_source_identities_verified_at_seal': sources,
        'required_run_states': {'workflow-r13-final-collection-example': 'failed',
                                'workflow-r13-readback-repair-example': 'succeeded'},
        'source_archive': 'canonical_source', 'complete_original_sample_rows': 12,
        'selected_phase': 'path-only four-process static source inventory',
        'prior_negative_results_retained': True, 'canonical_full_collector_verified': True,
        'full_collector_speedup_verified': False, 'all_workflows_measured': False, 'hardware_limit_proven': False}
    readback = {'state': 'verified', 'observed_at_utc': '2026-10-23T08:16:41+00:00',
        'sealed_files_verified': 1, 'all_original_sample_rows': 12, 'recomputed_mean_checks': 1,
        'archive_source_files': 1, 'full_collector_speedup_verified': False,
        'all_workflows_measured': False, 'hardware_limit_proven': False}
    disposition = {'state': 'selected_source_verified', 'observed_at_utc': '2026-10-23T07:59:37+00:00',
        'sources': sources, 'code_inventory_workers': 4, 'default_code_inventory_workers': 0}
    def write(bind=True):
        body = json.dumps(accepted).encode()
        if bind:
            readback['seal_sha256'] = hashlib.sha256(body).hexdigest()
        for name, value in zip(fixtures.WORKFLOW_ROUND_RECEIPTS, (accepted, readback, disposition), strict=True):
            (root / name).write_text(json.dumps(value))
    write()
    return root, accepted, readback, disposition, write


def test_original_round13_seal_keeps_failed_run_and_original_state(tmp_path):
    root, accepted, _, _, _ = _workflow_round13_case(tmp_path)
    (root / 'unrelated_measurement.json').write_text('[1,2,3]')
    result = fixtures.completed_benchmark_evidence(root, destination=tmp_path / 'retained-evidence')
    assert result['producer_state'] == 'verified'
    assert result['producer_contract'] == fixtures.WORKFLOW_ROUND_CONTRACTS[13]
    assert len(result['files']) == 3
    assert json.loads((tmp_path / 'retained-evidence/optimization_acceptance.json').read_text()) == accepted


@pytest.mark.parametrize('defect', ['seal', 'count', 'samples', 'means', 'source-count',
    'source-sha', 'failure', 'repair', 'claim', 'phase', 'clock', 'path', 'state', 'workers'])
def test_round13_rejects_changed_or_incomplete_original_static_phase_abi(tmp_path, defect):
    root, accepted, readback, disposition, write = _workflow_round13_case(tmp_path)
    if defect == 'seal': accepted['complete_original_sample_rows'] += 1
    elif defect == 'count': readback['sealed_files_verified'] = 2
    elif defect == 'samples': readback['all_original_sample_rows'] = True
    elif defect == 'means': readback['recomputed_mean_checks'] = 0
    elif defect == 'source-count': readback['archive_source_files'] = 2
    elif defect == 'source-sha': disposition['sources'][0]['sha256'] = 'unknown'
    elif defect == 'failure': accepted['required_run_states']['workflow-r13-final-collection-example'] = 'succeeded'
    elif defect == 'repair': accepted['required_run_states'].pop('workflow-r13-readback-repair-example')
    elif defect == 'claim': accepted['hardware_limit_proven'] = True
    elif defect == 'phase': accepted['selected_phase'] = 'all workflows finished'
    elif defect == 'clock': readback['observed_at_utc'] = '2026-10-23T01:00:00+00:00'
    elif defect == 'path': accepted['files'][0]['path'] = '../escape'
    elif defect == 'state': accepted['state'] = 'verified_scoped_engineering'
    elif defect == 'workers': disposition['code_inventory_workers'] = True
    write(bind=defect != 'seal')
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(root)


def test_workflow_round12_retains_original_large_seal_without_admitting_measurement_payloads(tmp_path):
    source, objects, write = _workflow_round_case(tmp_path, 12)
    accepted, readback, _ = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    accepted['evidence'] = [
        {'path': f'artifacts/benchmarks/workflow_speed_optimization_20261021/round12/trial_{i}/'
                 + 'original_member_' * 8, 'bytes': 1, 'sha256': 'a' * 64}
        for i in range(40574)]
    readback['sealed_files_verified'] = len(accepted['evidence'])
    write()
    seal = source / 'optimization_acceptance.json'
    assert 8 * 1024**2 < seal.stat().st_size <= 16 * 1024**2
    (source / 'measurement.json').write_text('x' * (17 * 1024**2))
    result = fixtures.completed_benchmark_evidence(source)
    assert len(result['files']) == 3
    assert result['producer_contract'] == fixtures.WORKFLOW_ROUND_CONTRACTS[12]
    # A single original seal over its explicit bound still fails closed.
    accepted['oversized'] = 'x' * (16 * 1024**2)
    write()
    with pytest.raises(fixtures.SnapshotError, match='bounded regular metadata'):
        fixtures.completed_benchmark_evidence(source)


@pytest.mark.parametrize('problem', ['state', 'contract', 'scope', 'missing', 'seal', 'count',
    'bool_count', 'samples', 'phases', 'partial_readback', 'duplicate', 'escape', 'absolute',
    'noncanonical', 'backslash', 'null', 'dot', 'hash', 'negative_size', 'bool_size',
    'naive_time', 'reverse_time', 'all_workflows', 'hardware', 'disposition', 'wrong_round', 'bad_day'])
@pytest.mark.parametrize('number', [7, 8])
def test_workflow_round_refuses_partial_unbound_or_malformed_original_receipts(tmp_path, problem, number):
    source, objects, write = _workflow_round_case(tmp_path, number)
    accepted, readback, disposition = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    row = accepted['evidence'][0]
    if problem == 'state': accepted['state'] = 'running'
    elif problem == 'contract': accepted['contract'] = fixtures.WORKFLOW_ROUND_CONTRACTS[6]
    elif problem == 'scope': accepted['scope'] = ''
    elif problem == 'seal': readback['seal_sha256'] = '0' * 64
    elif problem == 'count': readback['sealed_files_verified'] = 1
    elif problem == 'bool_count': readback['sealed_files_verified'] = True
    elif problem == 'samples': readback['optimization_samples_verified' if number == 8 else 'raw_samples_verified'] = 0
    elif problem == 'phases': readback['collector_phases_verified'] = 12
    elif problem == 'partial_readback': readback['state'] = 'partial'
    elif problem == 'duplicate': accepted['evidence'][1] = dict(row)
    elif problem == 'escape': row['path'] = '../outside'
    elif problem == 'absolute': row['path'] = '/outside'
    elif problem == 'noncanonical': row['path'] = 'scripts//producer.py'
    elif problem == 'backslash': row['path'] = 'scripts\\producer.py'
    elif problem == 'null': row['path'] = 'scripts/producer\x00.py'
    elif problem == 'dot': row['path'] = '.'
    elif problem == 'hash': row['sha256'] = 'x' * 64
    elif problem == 'negative_size': row['bytes'] = -1
    elif problem == 'bool_size': row['bytes'] = False
    elif problem == 'naive_time': accepted['observed_at_utc'] = '2026-10-22T13:24:21'
    elif problem == 'reverse_time': readback['observed_at_utc'] = '2026-10-21T13:24:21+00:00'
    elif problem == 'all_workflows':
        (accepted if number == 8 else accepted['remaining_boundaries'])['all_workflows_measured'] = True
    elif problem == 'hardware': readback['hardware_limit_proven'] = True
    elif problem == 'disposition': disposition['state'] = 'running'
    write(bind=problem != 'seal')
    if problem == 'missing': (source / 'optimization_acceptance_readback.json').unlink()
    elif problem == 'wrong_round':
        target = source.with_name('round9')
        source.rename(target)
        source = target
    elif problem == 'bad_day':
        target = source.parent.with_name('workflow_speed_optimization_20261301')
        source.parent.rename(target)
        source = target / source.name
    with pytest.raises((fixtures.SnapshotError, OSError)):
        fixtures.completed_benchmark_evidence(source)


@pytest.mark.parametrize('number', [6, 7, 8, 9, 10, 11])
def test_workflow_round_original_producer_survives_canonical_quarantine_admission(tmp_path, number):
    import time
    from stockagent.data_sync.legacy_artifact_archive import (
        LegacyArchiveSpec, MANUAL_WSL_CAPTURE_CONTRACT, source_plan,
    )
    source, _, _ = _workflow_round_case(tmp_path, number)
    (source / 'raw-measurement.bin').write_bytes(b'original failed and successful trial bodies')
    stamp = time.time() - 2 * 86400
    for path in source.iterdir():
        os.utime(path, (stamp, stamp))
    relative = source.relative_to(tmp_path / 'artifacts').as_posix()
    spec = LegacyArchiveSpec('workflow-round-fixture', relative, 7, tmp_path / 'stage',
        manual_capture_min_stable_hours=12, capture_contract=MANUAL_WSL_CAPTURE_CONTRACT)
    before = source_plan(source, spec, manual_capture=True)
    assert len(before) == 4 and 'raw-measurement.bin' in {row['path'] for row in before}
    state_root = tmp_path / 'states/retirements'
    quarantine = state_root / 'quarantine' / spec.dataset / ('123-' + 'a' * 32)
    quarantine.mkdir(parents=True)
    original = source
    source.rename(quarantine / 'source')
    (state_root / (spec.dataset + '.json')).write_text(json.dumps({
        'schema_version': 1, 'dataset': spec.dataset, 'relative_root': relative,
        'state': 'retiring', 'quarantine': str(quarantine),
        'snapshot_id': 'fixed-release', 'manifest_sha256': 'c' * 64}))
    after = source_plan(quarantine / 'source', spec, manual_capture=True, original_source=original)
    assert after == before
    original.mkdir()
    with pytest.raises(fixtures.SnapshotError, match='original retirement quarantine'):
        source_plan(quarantine / 'source', spec, manual_capture=True, original_source=original)


@pytest.mark.parametrize('problem', [
    'optimization_count', 'confirmation_count', 'instrumentation_count', 'bool_samples',
    'diagnostic_count', 'diagnostic_coverage', 'registry_coverage', 'mean_checks',
    'speed_samples', 'summary_groups', 'empty_variant', 'bool_batch', 'nonpositive_batch',
    'nonboolean_adoption', 'readback_variant', 'disposition_variant', 'readback_batch',
    'disposition_batch', 'readback_adoption', 'disposition_adoption', 'missing_selection',
])
def test_workflow_round8_refuses_partial_family_counts_or_inconsistent_original_selection(tmp_path, problem):
    source, objects, write = _workflow_round_case(tmp_path, 8)
    accepted, readback, disposition = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    if problem == 'optimization_count': accepted['optimization_samples'] = 8
    elif problem == 'confirmation_count': readback['confirmation_samples_verified'] = 1
    elif problem == 'instrumentation_count': readback.pop('instrumentation_comparison_samples_verified')
    elif problem == 'bool_samples': accepted['confirmation_samples'] = True
    elif problem == 'diagnostic_count': accepted['instrumented_diagnostic_samples'] = 0
    elif problem == 'diagnostic_coverage': readback['diagnostic_counter_coverage_verified'] = False
    elif problem == 'registry_coverage': readback.pop('remaining_registry_counts_verified')
    elif problem == 'mean_checks': readback['mean_checks'] = False
    elif problem == 'speed_samples': readback['speed_samples_verified'] = 0
    elif problem == 'summary_groups': readback['summary_groups_verified'] = -1
    elif problem == 'empty_variant': accepted['selected_variant'] = ''
    elif problem == 'bool_batch': accepted['selected_batch_mib'] = True
    elif problem == 'nonpositive_batch': accepted['selected_batch_mib'] = 0
    elif problem == 'nonboolean_adoption': accepted['performance_candidate_adopted'] = 1
    elif problem == 'readback_variant': readback['selected_variant'] = 'unsealed'
    elif problem == 'disposition_variant': disposition['selected_variant'] = 'unsealed'
    elif problem == 'readback_batch': readback['selected_batch_mib'] = 512
    elif problem == 'disposition_batch': disposition['selected_batch_mib'] = 512
    elif problem == 'readback_adoption': readback['performance_candidate_adopted'] = False
    elif problem == 'disposition_adoption': disposition['performance_candidate_adopted'] = False
    elif problem == 'missing_selection': disposition.pop('selected_variant')
    write()
    with pytest.raises(fixtures.SnapshotError, match='completed'):
        fixtures.completed_benchmark_evidence(source)


@pytest.mark.parametrize('problem', [
    'performance_count', 'diagnostic_count', 'bool_samples', 'serialization_count',
    'registry_coverage', 'mean_checks', 'speed_samples', 'summary_groups',
    'empty_selected_source', 'different_selected_source', 'bool_batch', 'different_batch',
    'bool_workers', 'zero_workers', 'nonboolean_adoption', 'readback_adoption',
    'disposition_adoption',
])
def test_workflow_round9_refuses_partial_readback_or_inconsistent_original_selection(tmp_path, problem):
    source, objects, write = _workflow_round_case(tmp_path, 9)
    accepted, readback, disposition = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    if problem == 'performance_count': accepted['performance_samples'] = 8
    elif problem == 'diagnostic_count': readback['diagnostic_samples_verified'] = 0
    elif problem == 'bool_samples': accepted['diagnostic_samples'] = True
    elif problem == 'serialization_count': readback['serialization_operations_verified'] = 4
    elif problem == 'registry_coverage': readback.pop('remaining_registry_counts_verified')
    elif problem == 'mean_checks': readback['mean_checks'] = False
    elif problem == 'speed_samples': readback['speed_samples_verified'] = 0
    elif problem == 'summary_groups': readback['summary_groups_verified'] = -1
    elif problem == 'empty_selected_source': accepted['selected_source'] = ''
    elif problem == 'different_selected_source': disposition['selected_source'] = 'unsealed'
    elif problem == 'bool_batch': accepted['selected_batch_mib'] = True
    elif problem == 'different_batch': disposition['selected_batch_mib'] = 512
    elif problem == 'bool_workers': accepted['selected_read_workers'] = True
    elif problem == 'zero_workers': accepted['selected_read_workers'] = 0
    elif problem == 'nonboolean_adoption': accepted['performance_candidate_adopted'] = 0
    elif problem == 'readback_adoption': readback['performance_candidate_adopted'] = True
    elif problem == 'disposition_adoption': disposition['performance_candidate_adopted'] = True
    write()
    with pytest.raises(fixtures.SnapshotError, match='completed'):
        fixtures.completed_benchmark_evidence(source)


@pytest.mark.parametrize('problem', [
    'performance_count', 'encoding_count', 'bool_encoding_count', 'diagnostics',
    'diagnostic_operations', 'registry_coverage', 'mean_checks', 'speed_samples',
    'summary_groups', 'different_selected_source', 'different_batch', 'readback_adoption',
    'source_deleted', 'services_restarted', 'training_jobs', 'boolean_jobs',
])
def test_workflow_round10_refuses_partial_or_conflicting_original_receipts(tmp_path, problem):
    source, objects, write = _workflow_round_case(tmp_path, 10)
    accepted, readback, disposition = (objects[name] for name in fixtures.WORKFLOW_ROUND_RECEIPTS)
    if problem == 'performance_count': accepted['performance_samples'] = 8
    elif problem == 'encoding_count': readback['encoding_samples_verified'] = 5
    elif problem == 'bool_encoding_count': accepted['encoding_samples'] = True
    elif problem == 'diagnostics': accepted['encoding_diagnostics'] = 0
    elif problem == 'diagnostic_operations': readback['encoding_diagnostic_operations_verified'] = 0
    elif problem == 'registry_coverage': readback.pop('remaining_registry_counts_verified')
    elif problem == 'mean_checks': readback['mean_checks'] = False
    elif problem == 'speed_samples': readback['speed_samples_verified'] = 0
    elif problem == 'summary_groups': readback['summary_groups_verified'] = -1
    elif problem == 'different_selected_source': disposition['selected_source'] = 'unsealed'
    elif problem == 'different_batch': disposition['selected_batch_mib'] = 512
    elif problem == 'readback_adoption': readback['performance_candidate_adopted'] = True
    elif problem == 'source_deleted': disposition['source_data_deleted'] = True
    elif problem == 'services_restarted': disposition['services_restarted'] = True
    elif problem == 'training_jobs': disposition['new_training_jobs'] = 1
    elif problem == 'boolean_jobs': disposition['new_training_jobs'] = False
    write()
    with pytest.raises(fixtures.SnapshotError, match='completed'):
        fixtures.completed_benchmark_evidence(source)


def _taifex_proof_memo_case(parent):
    source = parent / 'artifacts/benchmarks/running_services_20261013/taifex_proof_memo_20261015'
    source.mkdir(parents=True)
    outputs = dict.fromkeys(('1h', '1d', '1w', '1mo', '1q', '1y', 'all'), 'a' * 64)
    order = ['baseline', 'candidate', 'candidate', 'baseline'] * 2
    def trial(phase, index, variant):
        return {'phase': phase, 'trial': index, 'variant': variant,
            'output_sha256': {'1d': 'b' * 64} if phase == 'setup' else outputs.copy(),
            'durable_complete_readback': True, 'warm_exact_output_parity': True,
            'provider_calls': 0, 'production_writes': False, 'model_forward_calls': 0,
            'wall_seconds': 1.0, 'cpu_seconds': 0.5, 'warm_wall_seconds': 0.1, 'warm_cpu_seconds': 0.1}
    report = {'contract': fixtures.TAIFEX_PROOF_MEMO_CONTRACT, 'state': 'measurement_complete',
        'scope': 'complete_seven_range_real_append_curve_durable_readback_and_warm_not_broker_or_whole_host',
        'fixture': str(source / 'fixture'), 'trial_order': order,
        'balanced_abba': True, 'code_stable': True, 'source_unchanged': True, 'exact_output_parity': True,
        'provider_calls': 0, 'model_forward_calls': 0, 'production_writes': False,
        'completed_at_utc': '2026-10-15T02:00:00+00:00',
        'source_sha256': {str(source / 'fixture/state' / name): 'c' * 64 for name in (
            'state.json', 'status.json', 'marks.jsonl',
            'backfills/all_strategies_bidask_recapitalizing_v3/marks.jsonl',
            'backfills/all_strategies_bidask_recapitalizing_v3/receipt.json')},
        'code_sha256': {str(source / 'original_column_cache.py'): 'd' * 64,
            str(parent / 'scripts/benchmark_taifex_source_proof_memo.py'): 'e' * 64},
        'independent_raw_1d': {'path': str(source / 'comparison-v2/independent-raw-1d.json'),
            'sha256': 'f' * 64, 'output_sha256': outputs['1d']},
        'base_setup': [trial('setup', i, v) for i, v in enumerate(order)],
        'trials': [trial('update', i, v) for i, v in enumerate(order)]}
    objects = {'comparison-v2.json': report, 'original-code-proof.json': {
        'source': 'stockagent/live/taifex_history_column_cache.py', 'prefix_bytes': 123,
        'sha256': 'd' * 64}, 'benchmark-started.json': {'at_utc': '2026-10-15T01:00:00+00:00'}}
    for name, value in objects.items():
        (source / name).write_text(json.dumps(value))
    return source, objects


def test_taifex_proof_memo_retains_original_evidence_after_source_retirement(tmp_path):
    source, objects = _taifex_proof_memo_case(tmp_path)
    saved = tmp_path / 'retained-evidence'
    assert fixtures.external_benchmark_contract(source) == fixtures.TAIFEX_PROOF_MEMO_CONTRACT
    result = fixtures.completed_benchmark_evidence(source, destination=saved)
    assert result['producer_contract'] == fixtures.TAIFEX_PROOF_MEMO_CONTRACT
    assert {row['path'] for row in result['files']} == set(objects)
    assert all((saved / name).read_bytes() == (source / name).read_bytes() for name in objects)
    source.rename(source.with_name(source.name + '-preserved'))
    assert fixtures.completed_benchmark_evidence(saved, original_source=source,
        required_contract=fixtures.TAIFEX_PROOF_MEMO_CONTRACT) == result


@pytest.mark.parametrize('problem', ['failed', 'range_missing', 'different_values', 'wrong_order',
    'missing_setup', 'missing_trial', 'duplicate_index', 'unknown_phase', 'writes', 'calls',
    'forward', 'bool_calls', 'nondurable', 'warm_mismatch', 'source_missing', 'source_escape',
    'original_code', 'raw_mismatch', 'raw_escape', 'nan_time', 'negative_time',
    'naive_clock', 'reverse_clock', 'unknown_metadata', 'other_case'])
def test_taifex_proof_memo_refuses_incomplete_or_unbound_completions(tmp_path, problem):
    source, objects = _taifex_proof_memo_case(tmp_path)
    report = objects['comparison-v2.json']
    trial = report['trials'][1]
    if problem == 'failed': report['state'] = 'failed'
    elif problem == 'range_missing': del trial['output_sha256']['all']
    elif problem == 'different_values': trial['output_sha256']['1d'] = '0' * 64
    elif problem == 'wrong_order': report['trial_order'] = report['trial_order'][1:] + report['trial_order'][:1]
    elif problem == 'missing_setup': report['base_setup'].pop()
    elif problem == 'missing_trial': report['trials'].pop()
    elif problem == 'duplicate_index': trial['trial'] = 0
    elif problem == 'unknown_phase': trial['phase'] = 'setup'
    elif problem == 'writes': trial['production_writes'] = True
    elif problem == 'calls': report['provider_calls'] = 1
    elif problem == 'forward': trial['model_forward_calls'] = 1
    elif problem == 'bool_calls': trial['provider_calls'] = False
    elif problem == 'nondurable': trial['durable_complete_readback'] = False
    elif problem == 'warm_mismatch': trial['warm_exact_output_parity'] = False
    elif problem == 'source_missing': report['source_sha256'].popitem()
    elif problem == 'source_escape': report['source_sha256'][str(source / '../other')] = 'c' * 64
    elif problem == 'original_code': objects['original-code-proof.json']['sha256'] = '0' * 64
    elif problem == 'raw_mismatch': report['independent_raw_1d']['output_sha256'] = '0' * 64
    elif problem == 'raw_escape': report['independent_raw_1d']['path'] = '/another/independent-raw-1d.json'
    elif problem == 'nan_time': trial['wall_seconds'] = float('nan')
    elif problem == 'negative_time': trial['cpu_seconds'] = -1
    elif problem == 'naive_clock': report['completed_at_utc'] = '2026-10-15T02:00:00'
    elif problem == 'reverse_clock': report['completed_at_utc'] = '2026-10-14T02:00:00+00:00'
    elif problem == 'unknown_metadata': objects['unknown.json'] = {}
    elif problem == 'other_case':
        target = source.with_name('not-taifex')
        source.rename(target)
        source = target
    for name, value in objects.items():
        (source / name).write_text(json.dumps(value))
    with pytest.raises(fixtures.SnapshotError, match='supported completed producer'):
        fixtures.completed_benchmark_evidence(source)


def _public_builder_case(parent):
    source = parent / 'artifacts/benchmarks/running_services_20261013/public-feature-assembly-20261015c'
    (source / 'source').mkdir(parents=True)
    original = source / 'source/data.parquet'
    original.write_bytes(b'complete literal source')
    fixture = {'contract': 'tw_public_private_full_builder_fixture_v1',
        'original_code_sha256': 'c' * 64, 'symbol_universe': {'names': ['0050', '2330']},
        'source_files': [{'name': original.name, 'size': original.stat().st_size, 'sha256': _sha(original)}]}
    fixture_raw = json.dumps(fixture).encode()
    (source / 'fixture.json').write_bytes(fixture_raw)
    rows, measurements = [], []
    for name, variant in [('a0', 'original'), ('a1', 'original'), ('b1', 'streaming'),
                          ('b2', 'streaming'), ('a2', 'original')]:
        trial = source / name
        trial.mkdir()
        output = trial / 'features.parquet'
        output.write_bytes(b'complete byte-identical values\0')
        descriptor = {'name': output.name, 'size': output.stat().st_size, 'sha256': _sha(output)}
        receipt = {'state': 'completed_trial', 'variant': variant, 'build_mode': 'full',
            'original_code_sha256': fixture['original_code_sha256'],
            'symbol_universe_receipt': fixture['symbol_universe'],
            'source_receipts': fixture['source_files'], 'rows': 3, 'stock_rows': 3, 'market_rows': 0,
            'features': 1, 'pool_profile': {'threads': 2}, 'polars_threads': 2, 'arrow_threads': 2,
            'harness_code_sha256': ('a' if name == 'a0' else 'b') * 64,
            'provider_calls': 0, 'production_writes': False, 'model_runtime_imported': False,
            'output_receipt': descriptor}
        raw = json.dumps(receipt)
        (trial / 'receipt.json').write_text(raw)
        sha = hashlib.sha256(raw.encode()).hexdigest()
        rows.append({'trial': name, 'receipt_raw': raw, 'receipt_sha256': sha, 'verified_output': descriptor})
        if name != 'a0':
            measurements.append({'trial': name, 'variant': variant, 'receipt_sha256': sha, 'output': descriptor})
    paths = fixtures._public_builder_receipts(source)
    proof = {'contract': fixtures.PUBLIC_BUILDER_CONTRACT, 'state': 'accepted_exact_output',
        'fixture': str(source), 'fixture_sha256': _sha(source / 'fixture.json'),
        'source_files': 1, 'rows': 3, 'feature_count': 1, 'measurements': measurements,
        'proof_method': 'complete_parquet_bytes', 'exact_output_parity': True,
        'repeated_per_variant': True, 'provider_calls': 0, 'production_writes': False}
    paths[0].write_text(json.dumps(proof))
    evidence = {'contract': 'tw_public_builder_frozen_retirement_evidence_v1',
        'source': str(source), 'fixture_sha256': proof['fixture_sha256'],
        'verification_sha256': _sha(paths[0]), 'source_files': fixture['source_files'],
        'trials': rows, 'provider_calls': 0, 'production_writes': False}
    paths[1].write_text(json.dumps(evidence))
    return source, paths, fixture, proof, evidence


def test_public_builder_preserves_all_original_receipts_and_separate_harness_scope(tmp_path):
    source, paths, fixture, proof, evidence = _public_builder_case(tmp_path)
    saved = tmp_path / 'retained-evidence'
    result = fixtures.completed_benchmark_evidence(source, destination=saved)
    assert result['producer_contract'] == fixtures.PUBLIC_BUILDER_CONTRACT
    assert {row['path'] for row in result['files']} == {'fixture.json', *(p.name for p in paths)}
    assert json.loads((saved / paths[1].name).read_bytes())['trials'] == evidence['trials']
    # The archived original can be gone; retained exact bytes bind the same
    # source path without borrowing another source's current filesystem state.
    source.rename(source.with_name(source.name + '-kept-for-test'))
    assert fixtures.completed_benchmark_evidence(saved, original_source=source,
        required_contract=fixtures.PUBLIC_BUILDER_CONTRACT) == result


@pytest.mark.parametrize('problem', ['proof_bytes', 'fixture_bytes', 'receipt_bytes', 'output',
    'unknown_state', 'missing_trial', 'duplicate_trial', 'mixed_measured_harness', 'single_repeat',
    'source_scope', 'writes', 'calls', 'wrong_source', 'unknown_metadata'])
def test_public_builder_rejects_changed_incomplete_or_mixed_evidence(tmp_path, problem):
    source, paths, fixture, proof, evidence = _public_builder_case(tmp_path)
    if problem == 'proof_bytes':
        proof['added'] = True
        paths[0].write_text(json.dumps(proof))
    elif problem == 'fixture_bytes':
        (source / 'fixture.json').write_text(json.dumps(fixture) + '\n')
    elif problem == 'missing_trial':
        evidence['trials'].pop()
    elif problem == 'duplicate_trial':
        evidence['trials'].append(evidence['trials'][0])
    elif problem == 'single_repeat':
        proof['measurements'].pop()
        paths[0].write_text(json.dumps(proof))
        evidence['verification_sha256'] = _sha(paths[0])
    elif problem == 'wrong_source':
        evidence['source'] = str(source.parent / 'another')
    elif problem == 'unknown_metadata':
        (source / 'unknown.json').write_text('{}')
    elif problem in {'calls', 'writes'}:
        evidence['provider_calls' if problem == 'calls' else 'production_writes'] = 1
    else:
        row = evidence['trials'][1]
        receipt = json.loads(row['receipt_raw'])
        if problem == 'receipt_bytes':
            row['receipt_raw'] += '\n'  # Keep the original digest to expose drift.
        elif problem == 'output':
            row['verified_output']['sha256'] = 'f' * 64
        else:
            if problem == 'unknown_state':
                receipt['state'] = 'failed'
            elif problem == 'mixed_measured_harness':
                receipt['harness_code_sha256'] = 'a' * 64
            else:
                receipt['source_receipts'] = []
            row['receipt_raw'] = json.dumps(receipt)
            row['receipt_sha256'] = hashlib.sha256(row['receipt_raw'].encode()).hexdigest()
            proof['measurements'][0]['receipt_sha256'] = row['receipt_sha256']
            paths[0].write_text(json.dumps(proof))
            evidence['verification_sha256'] = _sha(paths[0])
    paths[1].write_text(json.dumps(evidence))
    with pytest.raises(fixtures.SnapshotError, match='supported completed producer'):
        fixtures.completed_benchmark_evidence(source)


def _capture(parent, name, sources):
    root = parent / name
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        for label, source in sources.items():
            inputs.copy(source, label)
        receipt = inputs.commit()
    return root, receipt


def test_complete_sources_shared_but_mutable_original_never_hardlinked(tmp_path):
    source = tmp_path / "live.parquet"
    source.write_bytes(b"all rows, raw nulls and float bits\0" * 500)
    first, cold = _capture(tmp_path, "a", {"source/data.parquet": source})
    second, warm = _capture(tmp_path, "b", {"source/data.parquet": source})
    a, b = first / "source/data.parquet", second / "source/data.parquet"
    assert cold["copied_files"] == 1 and cold["reused_files"] == 0
    assert warm["copied_bytes"] == 0 and warm["reused_bytes"] == source.stat().st_size
    assert a.stat().st_ino == b.stat().st_ino != source.stat().st_ino
    assert a.stat().st_mode & 0o222 == 0
    original = b.read_bytes()
    source.write_bytes(b"new current source")
    assert a.read_bytes() == b.read_bytes() == original
    a.unlink()  # A donor name retiring does not strand the surviving experiment.
    assert b.read_bytes() == original


def test_update_copies_only_changed_source_and_preserves_full_manifest(tmp_path):
    source = tmp_path / "current"
    source.mkdir()
    a, b = source / "a", source / "b"
    a.write_bytes(b"unchanged history")
    b.write_bytes(b"old tail")
    old, _ = _capture(tmp_path, "old", {"a": a, "b": b})
    b.write_bytes(b"corrected tail of different length")
    new, result = _capture(tmp_path, "new", {"a": a, "b": b})
    assert result["reused_files"] == result["copied_files"] == 1
    assert result["copied_bytes"] == b.stat().st_size
    assert (old / "a").stat().st_ino == (new / "a").stat().st_ino
    assert (old / "b").read_bytes() == b"old tail"
    assert (new / "b").read_bytes() == b.read_bytes()
    assert {item["name"] for item in result["files"]} == {"a", "b"}


@pytest.mark.parametrize("defect", ["writable", "wrong_hash", "symlink", "missing", "busy", "corrupt_receipt"])
def test_unsafe_or_busy_donor_falls_back_to_independent_verified_copy(tmp_path, defect):
    source = tmp_path / "live"
    source.write_bytes(b"exact source")
    old, _ = _capture(tmp_path, "old", {"data": source})
    donor = old / "data"
    lock = None
    if defect == "writable":
        donor.chmod(0o600)
    elif defect == "wrong_hash":
        donor.chmod(0o600)
        donor.write_bytes(b"wrong source")
        donor.chmod(0o400)
    elif defect == "symlink":
        donor.unlink()
        donor.symlink_to(source)
    elif defect == "missing":
        donor.unlink()
    elif defect == "busy":
        lock = (old / ".immutable-inputs.lock").open("rb")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        (old / RECEIPT).write_text("[]")
    try:
        new, result = _capture(tmp_path, "new", {"data": source})
    finally:
        if lock:
            lock.close()
    assert result["reused_files"] == 0 and result["copied_files"] == 1
    assert (new / "data").read_bytes() == source.read_bytes()
    assert (new / "data").stat().st_ino != source.stat().st_ino


def test_existing_sealed_public_fixture_reused_without_editing_its_receipt(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"full retained history")
    old = tmp_path / "old"
    (old / "source").mkdir(parents=True)
    frozen = old / "source/input.parquet"
    frozen.write_bytes(source.read_bytes())
    frozen.chmod(0o400)
    receipt = {"contract": "tw_public_private_full_builder_fixture_v1", "source_files": [
        {"name": "input.parquet", "size": source.stat().st_size, "sha256": _sha(source)}]}
    (old / "fixture.json").write_text(json.dumps(receipt))
    before = (old / "fixture.json").read_bytes()
    new, result = _capture(tmp_path, "new", {"source/input.parquet": source})
    assert result["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "source/input.parquet").stat().st_ino
    assert (old / "fixture.json").read_bytes() == before
    assert not (old / RECEIPT).exists()


def test_incomplete_or_failed_capture_cannot_seed_reuse(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    partial = tmp_path / "partial"
    partial.mkdir()
    with pytest.raises(RuntimeError), ImmutableFixtureInputs(partial) as inputs:
        inputs.copy(source, "data")
        raise RuntimeError("interrupted before coherent source fence")
    assert not (partial / RECEIPT).exists()
    new, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0
    assert (new / "data").stat().st_ino != (partial / "data").stat().st_ino


@pytest.mark.parametrize("name", ["../outside", "/outside", "a/../outside", "./data", "a//b", ""])
def test_member_cannot_escape_or_alias_experiment(tmp_path, name):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, name)


def test_rejects_wrong_source_hash_and_existing_destination(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        with pytest.raises(ValueError, match="source differs"):
            inputs.copy(source, "data", expected_sha256="0" * 64)
        assert not (root / "data").exists()
        (root / "occupied").write_bytes(b"keep")
        with pytest.raises(ValueError, match="already exists"):
            inputs.copy(source, "occupied")
        assert (root / "occupied").read_bytes() == b"keep"


def test_mutation_during_freeze_prevents_commit(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    copyfileobj = fixtures.shutil.copyfileobj
    def changing_copy(reader, writer, length):
        copyfileobj(reader, writer, length)
        source.write_bytes(b"other")
    monkeypatch.setattr(fixtures.shutil, "copyfileobj", changing_copy)
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, "data")
    assert not (root / RECEIPT).exists()
    assert not list(root.glob("*.partial"))


def test_fifo_metadata_is_not_read_or_blocked(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    donor = tmp_path / "untrusted"
    donor.mkdir()
    os.mkfifo(donor / RECEIPT)
    _, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0


def test_commit_rechecks_full_payload_and_readonly_contract(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        (root / "data").chmod(0o600)
        (root / "data").write_bytes(b"other")
        (root / "data").chmod(0o400)
        with pytest.raises(ValueError, match="changed before commit"):
            inputs.commit()
    assert not (root / RECEIPT).exists()


def test_frozen_donor_can_be_the_selected_source_without_false_mutation(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    old, _ = _capture(tmp_path, "old", {"data": source})
    frozen = old / "data"
    new, receipt = _capture(tmp_path, "new", {"data": frozen})
    assert receipt["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "data").stat().st_ino


def test_commit_seals_member_set_and_cannot_be_overwritten(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        inputs.commit()
        with pytest.raises(RuntimeError, match="already committed"):
            inputs.copy(source, "more")
        with pytest.raises(ValueError, match="already committed"):
            inputs.commit()
    with pytest.raises(ValueError, match="fresh experiment"):
        ImmutableFixtureInputs(root)


def test_public_capture_uses_shared_inputs_and_keeps_full_source_and_code_proof(tmp_path, monkeypatch):
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    repo = tmp_path / "repo"
    (repo / "stockagent/data").mkdir(parents=True)
    (repo / "stockagent/data/tw_public_features.py").write_bytes(b"# fixed builder\n")
    benchmarks = repo / "artifacts/benchmarks"
    benchmarks.mkdir(parents=True)
    source = tmp_path / "source"
    (source / "stocks").mkdir(parents=True)
    (source / "stocks/2330_features.parquet").write_bytes(b"symbol names only")
    (source / "prices.parquet").write_bytes(b"full price history")
    (source / "features.parquet").write_bytes(b"full features")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    first, second = benchmarks / "a", benchmarks / "b"
    benchmark.capture(first, source, source / "stocks")
    benchmark.capture(second, source, source / "stocks")
    receipt = benchmark._verified_fixture(second)
    assert receipt["storage_build"]["reused_files"] == 2
    assert receipt["storage_build"]["copied_bytes"] == 0
    assert len(receipt["source_files"]) == 2
    assert receipt["production_writes"] is False and receipt["providers_called"] == 0
    assert (first / "source/prices.parquet").stat().st_ino == (second / "source/prices.parquet").stat().st_ino


def test_openbb_capture_shares_readonly_data_but_never_mutable_sqlite(tmp_path, monkeypatch):
    from scripts import benchmark_openbb_metadata_audit as benchmark
    source = tmp_path / "source"
    (source / "_state").mkdir(parents=True)
    payload = source / "data.parquet"
    payload.write_bytes(b"full source table")
    with sqlite3.connect(source / "_state/openbb_archive.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(task_id TEXT,active INTEGER,plan_token TEXT,status TEXT,output_path TEXT,rows INTEGER,endpoint TEXT)")
        conn.execute("CREATE TABLE archive_meta(key TEXT,value TEXT)")
        conn.execute("INSERT INTO archive_meta VALUES('active_plan_token','test')")
        conn.execute("INSERT INTO tasks VALUES('one',1,'test','success',?,2,'macro')", (str(payload),))
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **kw: type("Git", (), {"stdout": b"# baseline\n"})())
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    a, b = tmp_path / "a", tmp_path / "b"
    benchmark.prepare(source, a, "test")
    receipt = benchmark.prepare(source, b, "test")
    assert receipt["storage_build"]["reused_files"] == 1
    assert (a / "data/one.parquet").stat().st_ino == (b / "data/one.parquet").stat().st_ino
    db_a, db_b = a / "_state/openbb_archive.sqlite3", b / "_state/openbb_archive.sqlite3"
    assert db_a.stat().st_ino != db_b.stat().st_ino
    with sqlite3.connect(db_b) as conn:
        conn.execute("UPDATE tasks SET rows=999")
    with sqlite3.connect(db_a) as conn:
        assert conn.execute("SELECT rows FROM tasks").fetchone()[0] == 2


def test_target_publication_io_error_propagates_without_copied_fallback(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    _capture(tmp_path, "old", {"data": source})
    def disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "publication disk full")
    monkeypatch.setattr(fixtures, "link_immutable_payload", disk_full)
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(OSError) as error:
        inputs.copy(source, "data")
    assert error.value.errno == errno.ENOSPC
    assert not (root / "data").exists() and not (root / RECEIPT).exists()


def test_busy_public_source_rejects_before_creating_empty_generation(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    from stockagent.live import tw_public_source_guard as source_guard
    repo = tmp_path / "repo"
    parent = repo / "artifacts/benchmarks"
    parent.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "prices.parquet").write_bytes(b"source")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    @contextmanager
    def busy(_root):
        raise source_guard.TWPublicSourceUpdatePending(source, source / "writer.lock")
        yield
    monkeypatch.setattr(source_guard, "live_tw_public_source_read_guard", busy)
    root = parent / "new"
    with pytest.raises(source_guard.TWPublicSourceUpdatePending):
        benchmark.capture(root, source, source)
    assert not root.exists()


def _finmind_group_case(tmp_path):
    import copy

    source = tmp_path / 'artifacts/benchmarks/suite/finmind-group-status-abba-v2-20261015-fixture'
    source.mkdir(parents=True)
    paths = fixtures._finmind_group_receipts(source)
    code = ['benchmark_finmind_group_status.py', 'download_finmind_complement.py',
            'finmind_updates.py', 'artifact_io.py', 'finmind_status_aggregates.py']
    base = {'schema_version': 1, 'state': 'measurement_complete',
        'fixture': str(source), 'source_queue': '/repo/data_finmind/complement/queue.sqlite3',
        'task_count': 100, 'tasks_per_status': 2, 'rows_published_per_trial': 5,
        'dataset': 'UKStockPrice', 'snapshot_sha256': 'a' * 64,
        'preparation_wall_seconds': 1.0, 'exact_output_parity': True,
        'all_task_values_equal': True, 'source_files_stable': True, 'code_stable': True,
        'provider_calls': 0, 'production_queue_writes': 0, 'production_deployed': False,
        'code_sha256': {'/repo/' + name: 'b' * 64 for name in code},
        'initial_full_status': {name: {'status_sha256': 'c' * 64, 'wall_seconds': 1.,
            'cpu_seconds': 1.} for name in ('baseline', 'candidate')},
        'source_receipts': [{'rows': n, 'sha256': 'd' * 64, 'parquet_sha256': 'e' * 64}
                            for n in (2, 3)],
        'trials': [{'label': name, 'ordinal': ordinal, 'wall_seconds': 1., 'cpu_seconds': 1.,
            'whole_status_sha256': str(ordinal) * 64, 'all_receipt_sha256': 'f' * 64,
            'parquet_bytes_sha256': ['a' * 64, 'b' * 64]}
            for name, ordinal in [('baseline', 0), ('candidate', 0), ('candidate', 1), ('baseline', 1)]]}
    objects = {p.name: copy.deepcopy(base) for p in paths}
    objects[paths[1].name].update(prepared_from=str(paths[0]), resource_limits_stable=True)
    for p in paths:
        p.write_text(json.dumps(objects[p.name]))
    return source, paths, objects


def test_original_adjacent_finmind_group_receipts_are_retained_unchanged(tmp_path):
    source, paths, objects = _finmind_group_case(tmp_path)
    originals = {p.name: p.read_bytes() for p in paths}
    result = fixtures.completed_benchmark_evidence(source, destination=tmp_path / 'retained')
    assert result['producer_contract'] == fixtures.FINMIND_GROUP_STATUS_CONTRACT
    assert result['producer_state'] == 'measurement_complete'
    assert {r['path'] for r in result['files']} == set(objects)
    assert list(source.iterdir()) == []
    for p in paths:
        assert p.read_bytes() == originals[p.name]
        assert (tmp_path / 'retained' / p.name).read_bytes() == originals[p.name]
    # The final retained-evidence reader requires the original frozen scope;
    # never infer a different fixture from the producer's own claim.
    retained = tmp_path / 'retained-evidence'
    (tmp_path / 'retained').rename(retained)
    source.rmdir()
    assert fixtures.completed_benchmark_evidence(retained,
        required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT, original_source=source) == result
    with pytest.raises(fixtures.SnapshotError, match='supported completed'):
        fixtures.completed_benchmark_evidence(retained, required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT)
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(retained, original_source=source)


@pytest.mark.parametrize('defect', [None, 'unowned-path', 'nonce', 'schema', 'state',
    'dataset', 'quarantine', 'relative', 'snapshot', 'manifest', 'writable', 'symlink', 'reappeared'])
def test_adjacent_receipts_require_exact_owned_quarantine_identity(tmp_path, defect):
    import uuid

    source, _, _ = _finmind_group_case(tmp_path)
    expected = fixtures.completed_benchmark_evidence(source)
    dataset = 'legacy-wsl-group'
    state_root = tmp_path / 'retirements'
    nonce = '12-' + uuid.uuid4().hex if defect != 'nonce' else 'arbitrary'
    quarantine = state_root / 'quarantine' / dataset / nonce
    if defect == 'unowned-path':
        quarantine = tmp_path / 'arbitrary'
    quarantine.mkdir(parents=True)
    source.rename(quarantine / 'source')
    relative = 'benchmarks/suite/' + source.name
    state = dict(schema_version=1, dataset=dataset, relative_root=relative, state='retiring',
        quarantine=str(quarantine), snapshot_id='fixed-release', manifest_sha256='a' * 64)
    if defect in {'schema', 'state', 'dataset', 'quarantine', 'snapshot', 'manifest'}:
        key = 'schema_version' if defect == 'schema' else ('snapshot_id' if defect == 'snapshot'
              else 'manifest_sha256' if defect == 'manifest' else defect)
        state[key] = False if defect == 'schema' else ''
    elif defect == 'relative':
        state['relative_root'] = relative.replace('20261015', '20261016')
    if defect == 'reappeared':
        source.mkdir()
    state_root.mkdir(exist_ok=True)
    path = state_root / (dataset + '.json')
    path.write_text(json.dumps(state))
    if defect == 'writable':
        path.chmod(0o666)
    elif defect == 'symlink':
        path.rename(path.with_suffix('.saved'))
        path.symlink_to(path.with_suffix('.saved'))
    read = lambda: fixtures.completed_benchmark_evidence(quarantine / 'source',
        original_source=source, required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT)
    if defect:
        with pytest.raises((fixtures.SnapshotError, OSError)):
            read()
    else:
        assert read() == expected
    assert (quarantine / 'source').is_dir()


@pytest.mark.parametrize('change', [
    'missing_trial', 'duplicate_ordinal', 'status_hash', 'receipt_hash', 'parquet_hash',
    'false_parity', 'task_values', 'source_stability', 'code_stability', 'provider_call',
    'queue_write', 'boolean_calls', 'deployed', 'fixture', 'prepared_from', 'resources',
    'snapshot', 'initial', 'source_rows', 'source_sha', 'code_sha', 'nan_timing'])
def test_adjacent_finmind_completion_rejects_incomplete_or_changed_comparison(tmp_path, change):
    from stockagent.data_sync.desync_snapshots import SnapshotError

    source, paths, objects = _finmind_group_case(tmp_path)
    row = objects[paths[1].name]
    if change == 'missing_trial': row['trials'].pop()
    elif change == 'duplicate_ordinal': row['trials'][2]['ordinal'] = 0
    elif change == 'status_hash': row['trials'][1]['whole_status_sha256'] = 'e' * 64
    elif change == 'receipt_hash': row['trials'][1]['all_receipt_sha256'] = 'e' * 64
    elif change == 'parquet_hash': row['trials'][1]['parquet_bytes_sha256'][0] = 'e' * 64
    elif change == 'false_parity': row['exact_output_parity'] = False
    elif change == 'task_values': row['all_task_values_equal'] = False
    elif change == 'source_stability': row['source_files_stable'] = False
    elif change == 'code_stability': row['code_stable'] = False
    elif change == 'provider_call': row['provider_calls'] = 1
    elif change == 'queue_write': row['production_queue_writes'] = 1
    elif change == 'boolean_calls': row['provider_calls'] = False
    elif change == 'deployed': row['production_deployed'] = True
    elif change == 'fixture': row['fixture'] += '-other'
    elif change == 'prepared_from': row['prepared_from'] += '.other'
    elif change == 'resources': row['resource_limits_stable'] = False
    elif change == 'snapshot': row['snapshot_sha256'] = 'unknown'
    elif change == 'initial': row['initial_full_status']['candidate']['status_sha256'] = 'e' * 64
    elif change == 'source_rows': row['source_receipts'][0]['rows'] = 999
    elif change == 'source_sha': row['source_receipts'][0]['sha256'] = 'unknown'
    elif change == 'code_sha': row['code_sha256']['/repo/artifact_io.py'] = 'unknown'
    elif change == 'nan_timing': row['trials'][0]['wall_seconds'] = float('nan')
    paths[1].write_text(json.dumps(row))
    with pytest.raises(SnapshotError, match='supported completed'):
        fixtures.completed_benchmark_evidence(source)


def test_adjacent_comparison_never_falls_back_to_missing_v3_or_embedded_alias(tmp_path):
    source, paths, objects = _finmind_group_case(tmp_path)
    paths[1].unlink()
    with pytest.raises(FileNotFoundError):
        fixtures.completed_benchmark_evidence(source)
    paths[1].write_text(json.dumps(objects[paths[1].name]))
    (source / paths[0].name).write_bytes(paths[0].read_bytes())
    with pytest.raises(fixtures.SnapshotError, match='collides'):
        fixtures.completed_benchmark_evidence(source)
