from dataclasses import replace
import json
import os
from pathlib import Path
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    MANUAL_WSL_CAPTURE_CONTRACT, MANUAL_OFFLINE_SIMULATION_CONTRACT,
    MANUAL_TRANSFER_QUARANTINE_CONTRACT,
    MANUAL_MINUTE_DERIVED_VIEW_CONTRACT,
    load_legacy_specs, prepare_archive,
    publish_archive, source_plan, verify_cold_archive,
)


def example(tmp_path):
    root = tmp_path / 'repo/artifacts'
    source = root / 'replays/old-result'
    source.mkdir(parents=True)
    file = source / 'unique-result.bin'
    file.write_bytes(b'unique offline research output')
    stamp = time.time() - 2 * 86400
    os.utime(file, (stamp, stamp))
    row = {'dataset': 'legacy-wsl-example', 'relative_root': 'replays/old-result',
           'minimum_stable_days': 7, 'manual_capture_min_stable_hours': 12,
           'archive_only': True, 'compression': 'gzip-1-csv-over-8m',
           'stage_root': str(tmp_path / 'stage'), 'capture_contract': MANUAL_WSL_CAPTURE_CONTRACT}
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    return root, source, row, catalog


@pytest.mark.parametrize('problem', ['', 'changed-receipt', 'different-source'])
def test_adjacent_finmind_reports_bind_before_canonical_retirement(tmp_path, monkeypatch, problem):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    from test_benchmark_fixture_reuse import _finmind_group_case

    source, paths, _ = _finmind_group_case(tmp_path)
    target = tmp_path / 'receipt'
    target.mkdir()
    saved = cleanup.benchmark_evidence(source, destination=target / 'retained-evidence')
    (target / 'retained-benchmark-evidence.json').write_text(json.dumps(saved))
    artifact_root = tmp_path / 'artifacts'
    relative = source.relative_to(artifact_root).as_posix()
    if problem == 'different-source':
        relative = relative.replace('20261015', '20261016')
    if problem == 'changed-receipt':
        path = target / 'retained-evidence' / paths[0].name
        changed = json.loads(path.read_bytes())
        changed['additional_observation'] = True
        path.write_text(json.dumps(changed))
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *a: {'ok': True})
    calls = []

    def apply(spec, **options):
        assert source.is_dir()
        assert options['owned_verified_plan'] is plan
        calls.append(True)
        return {'deleted': True}

    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', apply)
    plan = {'blockers': [], 'plan_fingerprint': 'a' * 64,
            'full_verification': {'verified_at_epoch': time.time()}}
    call = lambda: cleanup.retire_after_convergence(
        SimpleNamespace(relative_root=relative), None, target, plan, {'artifact_root': artifact_root})
    if problem:
        with pytest.raises(SnapshotError):
            call()
        assert not calls
    else:
        assert call() == {'deleted': True} and calls == [True]
    assert source.is_dir() and all(path.is_file() for path in paths)


def test_adjacent_finmind_reports_bind_after_source_removal_in_final_readback(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    from test_benchmark_fixture_reuse import _finmind_group_case

    source, paths, _ = _finmind_group_case(tmp_path)
    target = tmp_path / 'receipt'
    target.mkdir()
    saved = cleanup.benchmark_evidence(source, destination=target / 'retained-evidence')
    (target / 'retained-benchmark-evidence.json').write_text(json.dumps(saved))
    (target / 'stage-prune-plan.json').write_text(json.dumps({'reclaimable_allocated_file_bytes': 0}))
    (target / 'stage-pruned.json').write_text(json.dumps({'pruned': True}))
    artifact_root = tmp_path / 'artifacts'
    relative = source.relative_to(artifact_root).as_posix()
    source.rmdir()
    monkeypatch.setattr(cleanup, 'source_layout', lambda origin: (
        artifact_root, tmp_path / 'mirror', tmp_path / 'states', tmp_path / 'activations'))
    recovery_calls = []

    def verify(*args):
        recovery_calls.append(True)
        return {'snapshot_id': 'fixed-version', 'manifest_sha256': 'b' * 64}

    monkeypatch.setattr(cleanup, 'verify_cold_archive', verify)
    spec = SimpleNamespace(relative_root=relative, dataset='legacy-test', stage_root=tmp_path / 'stage')
    accepted = cleanup.finish_retirement(spec, SimpleNamespace(sync_root=tmp_path / 'cold'), target, {
        'deleted': True, 'snapshot_id': 'fixed-version', 'manifest_sha256': 'b' * 64,
        'reclaimed_allocated_file_bytes': 0})
    assert accepted['retired'] is True and recovery_calls == [True]
    assert accepted['local_benchmark_evidence_bytes'] == saved['bytes']
    assert all(path.is_file() for path in paths) and not source.exists()


def test_manual_offline_capture_preserves_unique_bytes_and_cannot_run_automatically(tmp_path):
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(source, spec)
    packed = tmp_path / 'packed'
    release = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['snapshot_id'] == release['snapshot_id']
    assert proof['decoded_originals_verified'] and proof['manifest']['capture_contract'] == MANUAL_WSL_CAPTURE_CONTRACT
    assert proof['manifest']['files'][0]['original_sha256']
    assert (source / 'unique-result.bin').read_bytes() == b'unique offline research output'


def _completed_benchmark(source):
    fixture = {'contract': 'private_native_test_v1', 'state': 'private_workload_prepared'}
    accepted = {'contract': fixture['contract'], 'state': 'accepted_bounded_comparison',
                'trials': [{'state': 'completed_trial'}]}
    (source / 'fixture.json').write_text(json.dumps(fixture))
    (source / 'abba-receipt.json').write_text(json.dumps(accepted))
    for path in source.glob('*.json'):
        old = time.time() - 2 * 86400
        os.utime(path, (old, old))


def _completed_finmind_reservation(source, *, automatic=False, reused=False):
    queue = '/observed/data_finmind/complement/queue.sqlite3'
    fixture = str(source / 'queues')
    variants = ('baseline', 'automatic', 'candidate') if automatic else ('baseline', 'candidate')
    snapshot = ({f'{v}/{owner}': {'path': f'{fixture}/{v}/{owner}/queue.sqlite3',
                                'bytes': 42, 'sha256_before_setup': 'a' * 64}
                 for v in ('baseline', 'candidate') for owner in ('sponsor', 'complement')}
                if reused else {f'{owner}/queue.sqlite3': {
                    'source_path': f'/observed/data_finmind/{owner}/queue.sqlite3',
                    'bytes': 42, 'sha256': 'a' * 64} for owner in ('sponsor', 'complement')})
    value = {'schema_version': 1, 'state': 'measurement_complete',
        'scope': 'fixed_real_two_owner_queues_full_reservation_status_and_durable_claim_release',
        'code_stable': True, 'exact_output_parity': True, 'source_workflow_unchanged': True,
        'provider_calls': 0, 'production_queue_mutations': 0, 'source_queue': queue,
        'fixture': fixture, 'whole_output_sha256': 'b' * 64, 'snapshots': snapshot,
        'code_sha256': {f'/observed/{name}': 'c' * 64 for name in (
            'benchmark_finmind_reservation.py', 'download_finmind_complement.py',
            'finmind_scheduling.py', 'finmind_queue_indexes.py')},
        'index_setup': {owner: {'wall_seconds': 1., 'cpu_seconds': .5,
                               'allocated_index_bytes': 4096, 'indexed_rows': 3}
                        for owner in ('sponsor', 'complement')},
        'status_plans': {v: {owner: ['SCAN tasks'] for owner in ('sponsor', 'complement')}
                         for v in ('baseline', 'candidate')},
        'reservation_plans': {v: {owner: ['SEARCH tasks'] for owner in ('sponsor', 'complement')}
                              for v in variants},
        'trials': [{'variant': v, 'repeat': repeat, 'wall_seconds': 1., 'cpu_seconds': .8,
                    'reservation_wall_seconds': .2, 'reservation_cpu_seconds': .1,
                    'queue_changes': 0, 'whole_output_sha256': 'b' * 64}
                   for repeat in range(2) for v in (*variants, *reversed(variants))],
        'claim_release_trials': [{'variant': v, 'repeat': repeat, 'group': group,
                                 'tasks': 3, 'transactions': 6, 'exact_task_rows_restored': True,
                                 'wall_seconds': 1., 'cpu_seconds': .1}
                                for repeat in range(2)
                                for v in (('baseline', 'candidate') if repeat % 2 == 0 else ('candidate', 'baseline'))
                                for group in ('history', 'release')]}
    (source / 'comparison.json').write_text(json.dumps(value))
    return value


def _finmind_throughput_case(root):
    import hashlib
    from stockagent.data_sync.benchmark_fixtures import FINMIND_THROUGHPUT_REPORTS

    source = root / 'artifacts/benchmarks/finlab-finmind-throughput-20261018/reservation'
    external = root / 'artifacts/data_quality/finlab_finmind_throughput_2026-10-18'
    source.mkdir(parents=True)
    external.mkdir(parents=True)
    for stage, name in enumerate(FINMIND_THROUGHPUT_REPORTS):
        value = _completed_finmind_reservation(source, automatic=stage == 0, reused=stage > 0)
        value['fixture'] = str(source)
        if stage:
            for key, row in value['snapshots'].items():
                row['path'] = str(source / key / 'queue.sqlite3')
            value['prepared_from'] = str(external / FINMIND_THROUGHPUT_REPORTS[stage-1])
        output = {'reservation': {'queue_errors': []}, 'status': {'complete': 3, 'missing': None}}
        selections = list(zip(
                ('includes_us_publication_recheck', 'includes_periodic_preparation', 'includes_correction_candidates'),
                ('publication_rechecks', 'periodic_refreshes', 'correction_candidates')))
        for position in ((), (0,), (0, 1), (2,))[stage]:
            flag, collection = selections[position]
            value[flag] = True
            output[collection] = [{'raw_value': -3.5, 'symbol': '0050'}]
        digest = hashlib.sha256(json.dumps(output, sort_keys=True, separators=(',', ':'),
                                           ensure_ascii=False).encode()).hexdigest()
        value['whole_output_sha256'] = digest
        stem = name.removesuffix('.json')
        context_name = stem + '-context.json'
        value['captured_context'] = str(source / context_name)
        context = {'fixed_clock': '2026-10-18T00:00:00+00:00', 'captured_live_status': {'complete': 3},
                   'current_stock_ids': ['0050'], 'official_delisted_ids': ['1000']}
        if stage == 3:
            context.update(correction_plan_sha256='a'*64, correction_requests=[{
                'owner': 'complement', 'request_id': 'fixed-request', 'dataset': 'TaiwanStockKBar',
                'start_date': '2021-03-05', 'end_date': '2021-04-14'}])
        (source / context_name).write_text(json.dumps(context))
        for index, trial in enumerate(value['trials']):
            trial['whole_output_sha256'] = digest
            file = source / f"{stem}-{trial['variant']}-output-{trial['repeat']}-{index}.json"
            file.write_text(json.dumps(output))
        (external / name).write_text(json.dumps(value))
    (source / 'comparison.json').unlink()  # Test builder's uncommitted intermediate only.
    return source, external


def test_throughput_companion_reports_preserve_original_full_outputs_and_completion(tmp_path):
    from stockagent.data_sync.benchmark_fixtures import (
        completed_benchmark_evidence, FINMIND_THROUGHPUT_RESERVATION_CONTRACT,
    )
    source, external = _finmind_throughput_case(tmp_path)
    before = {file: file.read_bytes() for parent in (source, external) for file in parent.glob('*.json')}
    retained = tmp_path / 'retained-evidence'
    proof = completed_benchmark_evidence(source, destination=retained)
    assert proof['producer_contract'] == FINMIND_THROUGHPUT_RESERVATION_CONTRACT
    assert len(proof['files']) == 44
    assert all(file.read_bytes() == raw for file, raw in before.items())
    replay = completed_benchmark_evidence(retained, original_source=source,
        required_contract=FINMIND_THROUGHPUT_RESERVATION_CONTRACT)
    assert replay == proof


def test_throughput_completion_survives_capture_quarantine_and_final_retained_readback(tmp_path):
    import uuid
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, verify_archive_directory
    from stockagent.data_sync.benchmark_fixtures import FINMIND_THROUGHPUT_RESERVATION_CONTRACT

    source, external = _finmind_throughput_case(tmp_path)
    artifact_root = tmp_path / 'artifacts'
    relative = source.relative_to(artifact_root).as_posix()
    receipt = tmp_path / 'retirement-receipt'
    receipt.mkdir()
    retained = cleanup.benchmark_evidence(source, destination=receipt / 'retained-evidence')
    (receipt / 'retained-benchmark-evidence.json').write_text(json.dumps(retained))
    assert cleanup.verified_retained_benchmark(receipt, source) == retained
    for path in source.rglob('*'):
        stamp = time.time() - 2 * 86400
        os.utime(path, (stamp, stamp))
    spec = LegacyArchiveSpec('legacy-wsl-throughput', relative, 7, tmp_path / 'stage',
                            12, False, 'gzip-1-csv-over-8m', MANUAL_WSL_CAPTURE_CONTRACT)
    packed = tmp_path / 'cold'
    published = publish_archive(spec, artifact_root, packed,
                                repo_root=tmp_path, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['snapshot_id'] == published['snapshot_id']
    assert proof['decoded_originals_verified'] and proof['files'] == 40
    state_root = tmp_path / 'retirements'
    quarantine = state_root / 'quarantine' / spec.dataset / ('12-' + uuid.uuid4().hex)
    quarantine.mkdir(parents=True)
    source.rename(quarantine / 'source')
    (state_root / (spec.dataset + '.json')).write_text(json.dumps({
        'schema_version': 1, 'state': 'retiring', 'dataset': spec.dataset,
        'relative_root': relative, 'quarantine': str(quarantine),
        'snapshot_id': published['snapshot_id'], 'manifest_sha256': published['manifest_sha256']}))
    assert source_plan(quarantine / 'source', spec, manual_capture=True, original_source=source)
    # Capture verification follows the original path supplied by the archive
    # spec, then validates the durable quarantine; producer JSON cannot pick it.
    verify_archive_directory(spec.stage_root / spec.dataset / 'archive',
        spec=spec, source=quarantine / 'source', artifact_root=artifact_root, manual_capture=True)
    assert cleanup.verified_retained_benchmark(receipt, source) == retained
    assert retained['producer_contract'] == FINMIND_THROUGHPUT_RESERVATION_CONTRACT


@pytest.mark.parametrize('defect', ['missing-report', 'wrong-previous', 'wrong-fixture', 'partial-output',
                                    'corrupt-output', 'extra-attempt', 'wrong-clock', 'missing-stage'])
def test_throughput_companion_reports_reject_incomplete_or_unbound_work(tmp_path, defect):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence

    source, external = _finmind_throughput_case(tmp_path)
    file = external / 'periodic_query_control.json'
    value = json.loads(file.read_bytes())
    if defect == 'missing-report':
        file.unlink()
    elif defect in {'wrong-previous', 'wrong-fixture', 'missing-stage'}:
        key = {'wrong-previous': 'prepared_from', 'wrong-fixture': 'fixture',
               'missing-stage': 'includes_periodic_preparation'}[defect]
        value[key] = False if defect == 'missing-stage' else '/different/original'
        file.write_text(json.dumps(value))
    elif defect == 'extra-attempt':
        (source / 'unknown-attempt.json').write_text('{}')
    elif defect == 'wrong-clock':
        path = source / 'periodic_query_control-context.json'
        context = json.loads(path.read_bytes())
        context['fixed_clock'] = '2026-10-18T00:00:00'
        path.write_text(json.dumps(context))
    else:
        path = source / 'periodic_query_control-baseline-output-0-0.json'
        if defect == 'partial-output':
            path.unlink()
        else:
            path.write_text('{"reservation": {}, "status": {}}')
    with pytest.raises((SnapshotError, FileNotFoundError), match='|'.join((
            'supported completed', 'No such file'))):
        completed_benchmark_evidence(source)


@pytest.mark.parametrize('automatic,reused', [(False, False), (True, False), (True, True)])
def test_finmind_reservation_preserves_native_completion_without_rewriting_receipts(tmp_path, automatic, reused):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence, FINMIND_RESERVATION_CONTRACT
    source = tmp_path / 'case'
    source.mkdir()
    _completed_finmind_reservation(source, automatic=automatic, reused=reused)
    original = (source / 'comparison.json').read_bytes()
    retained = tmp_path / 'retained'
    proof = completed_benchmark_evidence(source, destination=retained)
    assert proof['producer_contract'] == FINMIND_RESERVATION_CONTRACT
    assert proof['producer_state'] == 'measurement_complete'
    assert completed_benchmark_evidence(retained) == proof
    assert (retained / 'comparison.json').read_bytes() == original
    assert (source / 'comparison.json').read_bytes() == original
    assert not (source / 'fixture.json').exists()
    assert not (retained / 'abba-receipt.json').exists()


@pytest.mark.parametrize('defect', [
    'incomplete', 'different_scope', 'code_drift', 'missing_code', 'call', 'mutation',
    'different_output', 'missing_trial', 'reordered_trial', 'boolean_repeat', 'not_restored',
    'missing_write', 'transaction_count', 'different_write_tasks', 'missing_owner',
    'snapshot_escape', 'wrong_snapshot_hash', 'setup_nan', 'plan_missing_owner',
    'source_escape', 'prepared_without_prior', 'foreign_comparison', 'second_incomplete',
])
def test_finmind_reservation_rejects_partial_or_contradictory_completion(tmp_path, defect):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    value = _completed_finmind_reservation(tmp_path)
    if defect == 'incomplete': value['state'] = 'running'
    elif defect == 'different_scope': value['scope'] = 'status_only'
    elif defect == 'code_drift': value['code_stable'] = False
    elif defect == 'missing_code': value['code_sha256'].pop('/observed/finmind_scheduling.py')
    elif defect == 'call': value['provider_calls'] = 1
    elif defect == 'mutation': value['production_queue_mutations'] = 1
    elif defect == 'different_output': value['trials'][-1]['whole_output_sha256'] = 'd' * 64
    elif defect == 'missing_trial': value['trials'].pop()
    elif defect == 'reordered_trial': value['trials'][0], value['trials'][1] = value['trials'][1], value['trials'][0]
    elif defect == 'boolean_repeat': value['trials'][0]['repeat'] = False
    elif defect == 'not_restored': value['claim_release_trials'][0]['exact_task_rows_restored'] = False
    elif defect == 'missing_write': value['claim_release_trials'].pop()
    elif defect == 'transaction_count': value['claim_release_trials'][0]['transactions'] = 4
    elif defect == 'different_write_tasks':
        value['claim_release_trials'][0].update(tasks=2, transactions=4)
    elif defect == 'missing_owner': value['snapshots'].pop('sponsor/queue.sqlite3')
    elif defect == 'snapshot_escape': value['snapshots']['sponsor/queue.sqlite3']['source_path'] = '/other/queue.sqlite3'
    elif defect == 'wrong_snapshot_hash': value['snapshots']['sponsor/queue.sqlite3']['sha256'] = 'unknown'
    elif defect == 'setup_nan': value['index_setup']['sponsor']['wall_seconds'] = float('nan')
    elif defect == 'plan_missing_owner': value['reservation_plans']['baseline'].pop('complement')
    elif defect == 'source_escape': value['source_queue'] = '/observed/../data_finmind/complement/queue.sqlite3'
    elif defect == 'prepared_without_prior': value['prepared_from'] = str(tmp_path / 'missing.json')
    elif defect == 'foreign_comparison': (tmp_path / 'comparison-unknown.json').write_text('{}')
    elif defect == 'second_incomplete': (tmp_path / 'comparison-v2.json').write_text(json.dumps({**value, 'state': 'running'}))
    (tmp_path / 'comparison.json').write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='no supported completed producer'):
        completed_benchmark_evidence(tmp_path)


def test_finmind_reservation_reused_fixture_requires_original_bound_receipt(tmp_path):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    original = _completed_finmind_reservation(tmp_path)
    current = _completed_finmind_reservation(tmp_path, automatic=True, reused=True)
    current['prepared_from'] = str(tmp_path / 'comparison.json')
    (tmp_path / 'comparison.json').write_text(json.dumps(original))
    (tmp_path / 'comparison-three-plans.json').write_text(json.dumps(current))
    assert completed_benchmark_evidence(tmp_path)['producer_state'] == 'measurement_complete'
    current['fixture'] = str(tmp_path / 'other-fixture')
    (tmp_path / 'comparison-three-plans.json').write_text(json.dumps(current))
    with pytest.raises(SnapshotError, match='no supported completed producer'):
        completed_benchmark_evidence(tmp_path)


def test_one_explicit_completed_benchmark_case_uses_existing_exact_archive_owner(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    relative = 'benchmarks/service-suite/completed-case'
    target = root / relative
    target.parent.mkdir(parents=True)
    source.rename(target)
    _completed_benchmark(target)
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(target, spec)
    observed = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    assert not observed['blockers']
    local = tmp_path / 'retirement/retained-evidence'
    evidence = cleanup.benchmark_evidence(target, destination=local)
    assert evidence == observed['benchmark_evidence'] == cleanup.benchmark_evidence(local)
    assert {path.name for path in local.iterdir()} == {'fixture.json', 'abba-receipt.json'}
    assert (target / 'unique-result.bin').read_bytes() == b'unique offline research output'
    packed = tmp_path / 'packed'
    published = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['decoded_originals_verified'] and proof['snapshot_id'] == published['snapshot_id']
    assert proof['files'] == 3 and target.is_dir()  # Publication never implies deletion.


def _completed_official_daily(source):
    import hashlib
    names = ('twse_daily_ohlcv.parquet', 'tpex_daily_ohlcv.parquet',
             'tw_corporate_action_reference.parquet', 'twse_delisted_company.parquet',
             'tpex_delisted_company.parquet', 'twse_listed_company_basic.parquet',
             'tpex_basic_company.parquet', 'twse_api_company_newlisting.parquet',
             'twse_taiex_ohlc.parquet', 'twse_taiex_ohlc.summary.json')
    captured = source / 'captured'
    captured.mkdir()
    proofs = []
    for name in names:
        path = captured / name
        path.write_bytes(name.encode())
        proofs.append({'path': str(path), 'original': '/original/' + name,
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    stages = {name: {'wall_seconds': 1., 'cpu_seconds': .5} for name in (
        'capture_fixed_sources', 'prior_fixture', 'prior_full_merge', 'restore_actual_latest_sources',
        'new_source_session_merge', 'daily_complete_typed_oracle', 'independent_full_merge',
        'full_complete_typed_oracle', 'unchanged_merge', 'unchanged_complete_typed_oracle')}
    code = {key: 'a' * 64 for key in ('builder', 'typed_proof', 'parser_shared',
                                     'group_projection', 'merge_projection', 'partitioned_parquet')}
    code.update(polars='1.0', numpy='2.0', arrow='20.0')
    work = {}
    for name, mode, reused, rebuilt in (('previous', 'full', 0, 20),
                                      ('daily', 'suffix', 20, 1), ('unchanged', 'unchanged', 21, 0)):
        work[name] = {'contract': 'canonical_row_local_parquet_group_projection_v1',
            'merge_projection': {'contract': 'official_ohlcv_exact_session_merge_v1',
                'full_source_receipts_required': True, 'mode': mode,
                'reused_rows': reused, 'rebuilt_rows': rebuilt, 'old_generations_removed': 0}}
    receipt = {'status': 'accepted_real_source_merge_daily_not_full_base_pit_or_live_readiness',
        'retrospective_fixture_not_historical_pit': True, 'benchmark_sha256': 'b' * 64,
        'production_source_writes': 0, 'service_restarts': 0, 'ledger_writes': 0,
        'prior': '2026-10-19', 'latest': '2026-10-20', 'code': code, 'stages': stages,
        'previous_shape': [20, 3], 'daily_shape': [21, 3], 'work': work, 'source_capture': proofs,
        'oracle': {'values_sha256': 'c' * 64, 'receipts': [[], [], []], 'statistics': {}}}
    (source / 'receipt.json').write_text(json.dumps(receipt))
    (source / 'progress.json').write_text(json.dumps({'stages': stages, 'code': code}))
    for path in source.rglob('*'):
        if path.is_file():
            os.utime(path, (time.time() - 2 * 86400,) * 2)
    return receipt


def test_standalone_official_daily_requires_exact_producer_and_full_cold_recovery(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.benchmark_fixtures import OFFICIAL_DAILY_CONTRACT
    root, source, row, catalog = example(tmp_path)
    relative = 'benchmarks/official-real-daily-20261020-v87'
    target = root / relative
    target.parent.mkdir(parents=True)
    source.rename(target)
    receipt = _completed_official_daily(target)
    original = (target / 'receipt.json').read_bytes()
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    observed = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    assert observed['blockers'] == []
    retained = tmp_path / 'retained'
    evidence = cleanup.benchmark_evidence(target, destination=retained, required_contract=OFFICIAL_DAILY_CONTRACT)
    assert evidence == cleanup.benchmark_evidence(retained)
    assert evidence['producer_state'] == receipt['status']
    published = publish_archive(spec, root, tmp_path / 'packed', repo_root=root.parent, manual_capture=True)
    recovered = verify_cold_archive(spec, tmp_path / 'packed')
    assert recovered['decoded_originals_verified'] and recovered['snapshot_id'] == published['snapshot_id']
    assert recovered['files'] == 13 and (target / 'receipt.json').read_bytes() == original
    assert cleanup.stage_profile({'staging_profile': cleanup.D_BENCHMARK_STAGE_PROFILE,
                                 'items': [{'relative_root': relative}]}) == (cleanup.D_BENCHMARK_STAGE_ROOT, True)
    with pytest.raises(SnapshotError, match='standalone case'):
        cleanup.benchmark_evidence(target, required_contract='complete_panel_storage_comparison_v1')
    assert not cleanup.reviewed_wsl_artifact_root('benchmarks/official-real-daily-20261020')
    assert not cleanup.reviewed_wsl_artifact_root(relative + '/nested/case')


@pytest.mark.parametrize('defect', [
    'unfinished', 'missing_progress', 'progress_changed', 'missing_stage', 'nonfinite_time',
    'boolean_time', 'code_hash', 'source_digest', 'source_missing', 'source_duplicate',
    'relative_source', 'oracle_hash', 'shape_bool', 'clock_order', 'production_write',
    'boolean_zero', 'mode', 'lost_rows', 'old_generation_removed', 'rebuilt_unchanged',
    'source_proof_disabled', 'reference',
])
def test_official_daily_rejects_incomplete_or_malformed_completion(tmp_path, defect):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    value = _completed_official_daily(tmp_path)
    if defect == 'unfinished': value['status'] = 'running'
    elif defect == 'missing_stage': del value['stages']['independent_full_merge']
    elif defect == 'nonfinite_time': value['stages']['unchanged_merge']['wall_seconds'] = float('nan')
    elif defect == 'boolean_time': value['stages']['unchanged_merge']['wall_seconds'] = True
    elif defect == 'code_hash': value['code']['builder'] = 'unknown'
    elif defect == 'source_digest': value['source_capture'][0]['sha256'] = 'unknown'
    elif defect == 'source_missing': value['source_capture'].pop()
    elif defect == 'source_duplicate': value['source_capture'][1] = value['source_capture'][0]
    elif defect == 'relative_source': value['source_capture'][0]['path'] = 'captured/source.parquet'
    elif defect == 'oracle_hash': value['oracle']['values_sha256'] = 'unknown'
    elif defect == 'shape_bool': value['daily_shape'][0] = True
    elif defect == 'clock_order': value['prior'] = value['latest']
    elif defect == 'production_write': value['production_source_writes'] = 1
    elif defect == 'boolean_zero': value['ledger_writes'] = False
    elif defect == 'mode': value['work']['daily']['merge_projection']['mode'] = 'full'
    elif defect == 'lost_rows': value['work']['daily']['merge_projection']['rebuilt_rows'] = 0
    elif defect == 'old_generation_removed': value['work']['daily']['merge_projection']['old_generations_removed'] = 1
    elif defect == 'rebuilt_unchanged':
        value['work']['unchanged']['merge_projection'].update(reused_rows=20, rebuilt_rows=1)
    elif defect == 'source_proof_disabled': value['work']['daily']['merge_projection']['full_source_receipts_required'] = False
    elif defect == 'reference': value['fixed_pre_change_reference'] = {'sha256': 'unknown'}
    (tmp_path / 'receipt.json').write_text(json.dumps(value))
    (tmp_path / 'progress.json').write_text(json.dumps({'stages': value['stages'], 'code': value['code']}))
    if defect == 'missing_progress': (tmp_path / 'progress.json').unlink()
    elif defect == 'progress_changed': (tmp_path / 'progress.json').write_text('{}')
    with pytest.raises(SnapshotError, match='completed producer'):
        completed_benchmark_evidence(tmp_path)
    assert (tmp_path / 'receipt.json').is_file()


def _completed_openbb_selector(source):
    fixture = {'contract': 'openbb_private_full_manifest_source_selector_v1',
               'manifest_sha256': 'a' * 64, 'selector_sha256': 'b' * 64,
               'module_sha256': 'c' * 64, 'tasks': 123,
               'provider_calls': 0, 'production_writes': False}
    names = ('01-original.json', '02-candidate.json', '03-candidate.json', '04-original.json')
    trials = []
    for name, variant in zip(names, ('original', 'candidate', 'candidate', 'original'), strict=True):
        row = {'state': 'completed_trial', 'variant': variant, 'limit': 32768,
               'fixture_manifest_sha256': fixture['manifest_sha256'],
               'shared_code_sha256': fixture['module_sha256'], 'selected_files': 0,
               'full_ordered_source_contract_sha256': 'd' * 64,
               'provider_calls': 0, 'production_writes': False, 'model_runtime_imported': False}
        (source / name).write_text(json.dumps(row))
        trials.append(dict(name=name, **row))
    accepted = {'contract': 'openbb_full_manifest_unassigned_source_abba_v1',
                'state': 'accepted_bounded_phase_comparison', 'tasks': fixture['tasks'],
                'fixture_manifest_sha256': fixture['manifest_sha256'],
                'exact_full_ordered_source_parity': True, 'trials': trials,
                'provider_calls': 0, 'production_writes': False}
    (source / 'fixture.json').write_text(json.dumps(fixture))
    (source / 'abba-receipt.json').write_text(json.dumps(accepted))
    return fixture, accepted


def test_openbb_distinct_fixture_and_comparison_abis_retain_exact_completion_evidence(tmp_path):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    source = tmp_path / 'case'
    source.mkdir()
    _completed_openbb_selector(source)
    retained = tmp_path / 'retained'
    proof = completed_benchmark_evidence(source, destination=retained)
    assert proof == completed_benchmark_evidence(retained)
    assert proof['producer_contract'] == 'openbb_private_full_manifest_source_selector_v1'
    assert proof['producer_state'] == 'accepted_bounded_phase_comparison'
    assert len(proof['files']) == 6
    assert proof['scope'] == 'producer_declared_completion_not_benchmark_reexecution'


@pytest.mark.parametrize('problem', [
    'foreign_pair', 'same_contract_shortcut', 'missing_trial', 'sidecar_changed',
    'unfinished', 'manifest', 'module', 'order', 'limit', 'nonzero_calls',
    'writes', 'model_import', 'parity', 'selected_bool', 'calls_bool',
    'tasks_bool', 'malformed_digest', 'parity_claim',
])
def test_openbb_completion_bridge_rejects_unbound_or_incomplete_evidence(tmp_path, problem):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    fixture, accepted = _completed_openbb_selector(tmp_path)
    row = accepted['trials'][0]
    if problem == 'foreign_pair':
        accepted['contract'] = 'unknown_comparison'
    elif problem == 'same_contract_shortcut':
        accepted['contract'] = fixture['contract']
    elif problem == 'missing_trial':
        (tmp_path / row['name']).unlink()
    elif problem == 'sidecar_changed':
        (tmp_path / row['name']).write_text('{}')
    elif problem == 'unfinished':
        row['state'] = 'running'
    elif problem == 'manifest':
        accepted['fixture_manifest_sha256'] = 'e' * 64
    elif problem == 'module':
        row['shared_code_sha256'] = 'e' * 64
    elif problem == 'order':
        accepted['trials'].reverse()
    elif problem == 'limit':
        row['limit'] = 8192
    elif problem == 'nonzero_calls':
        row['provider_calls'] = 1
    elif problem == 'writes':
        accepted['production_writes'] = True
    elif problem == 'model_import':
        row['model_runtime_imported'] = True
    elif problem == 'parity':
        row['full_ordered_source_contract_sha256'] = 'e' * 64
    elif problem == 'selected_bool':
        row['selected_files'] = False
    elif problem == 'calls_bool':
        row['provider_calls'] = False
    elif problem == 'tasks_bool':
        fixture['tasks'] = accepted['tasks'] = True
    elif problem == 'malformed_digest':
        fixture['selector_sha256'] = 'not-a-sha256'
    else:
        accepted['exact_full_ordered_source_parity'] = False
    # Rebind edited trial sidecars except in the explicit missing/tamper cases:
    # rejection must not depend only on an incidental receipt mismatch.
    if problem not in {'missing_trial', 'sidecar_changed'}:
        for trial in accepted['trials']:
            (tmp_path / trial['name']).write_text(json.dumps({
                key: value for key, value in trial.items() if key != 'name'}))
    (tmp_path / 'fixture.json').write_text(json.dumps(fixture))
    (tmp_path / 'abba-receipt.json').write_text(json.dumps(accepted))
    with pytest.raises(SnapshotError, match='completed producer'):
        completed_benchmark_evidence(tmp_path)


def test_temporary_stage_selection_is_fixed_and_bound_to_one_completed_benchmark_scope():
    from scripts import retire_local_offline_artifacts as cleanup
    assert cleanup.stage_profile({}) == (cleanup.C_TEMP_STAGE_ROOT, False)
    inventory = {'staging_profile': cleanup.D_BENCHMARK_STAGE_PROFILE,
                 'items': [{'relative_root': 'benchmarks/suite/completed-case'}]}
    assert cleanup.stage_profile(inventory) == (cleanup.D_BENCHMARK_STAGE_ROOT, True)


def test_native_ssd_profile_keeps_the_existing_fixed_root_and_durable_staging():
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.NATIVE_SSD_STAGE_PROFILE,
                 'items': [{'relative_root': 'benchmarks/suite/completed-case'}]}
    assert cleanup.stage_profile(inventory) == (cleanup.C_TEMP_STAGE_ROOT, True)
    assert cleanup.stage_profile({}) == (cleanup.C_TEMP_STAGE_ROOT, False)
    for other in ('guarded_d_scratch', 'guarded_d_offline_scratch'):
        with pytest.raises(SnapshotError, match='select only one'):
            cleanup.inventory([], None, native_ssd_scratch=True, **{other: True})


@pytest.mark.parametrize('problem', ['multiple', 'existing', 'origin', 'catalog', 'scope', 'parent', 'arbitrary'])
def test_native_ssd_profile_cannot_relocate_frozen_or_unknown_source_roles(problem):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.NATIVE_SSD_STAGE_PROFILE,
                 'items': [{'relative_root': 'benchmarks/suite/completed-case'}]}
    if problem == 'multiple':
        inventory['items'].append({'relative_root': 'benchmarks/suite/other'})
    elif problem == 'existing':
        inventory['items'][0]['existing_dataset'] = 'retained-source'
    elif problem == 'origin':
        inventory['source_origin'] = cleanup.HOT_MIRROR_ORIGIN
    elif problem == 'catalog':
        inventory['existing_catalog_sha256'] = 'a' * 64
    elif problem == 'scope':
        inventory['items'][0]['relative_root'] = 'data_finlab/raw'
    elif problem == 'parent':
        inventory['items'][0]['relative_root'] = 'benchmarks/suite'
    else:
        inventory['staging_profile'] = '/arbitrary/fast/drive'
    with pytest.raises(SnapshotError, match='staging profile'):
        cleanup.stage_profile(inventory)


def _completed_research_staging(source):
    from stockagent.data_sync.benchmark_fixtures import RESEARCH_STAGING_CONTRACT
    value = {'contract': RESEARCH_STAGING_CONTRACT,
             'status': 'accepted_real_byte_staging_not_full_pipeline',
             'source': {'path': '/private/original.parquet', 'bytes': 32, 'sha256': 'a' * 64},
             'code_sha256': {f'/code/{name}': 'b' * 64 for name in (
                 'benchmark_research_stage_copy.py', 'stage_tw_public_research_release.py',
                 'immutable_cache_files.py')},
             'baseline_ast_sha256': 'c' * 64,
             'baseline_has_no_explicit_fsync': True, 'candidate_new_writes_fsync': True,
             'provider_calls': 0, 'production_writes': False, 'service_restarts': 0,
             'samples': [{'variant': variant, 'wall_seconds': .5, 'cpu_seconds': .2,
                          'written_payload_bytes': 32 if variant == 'baseline' else 0}
                         for variant in ('baseline', 'candidate', 'candidate', 'baseline')],
             'baseline_seed': {'wall_seconds': .5, 'cpu_seconds': .2},
             'candidate_seed': {'wall_seconds': .5, 'cpu_seconds': .2}}
    (source / 'receipt.json').write_text(json.dumps(value))
    old = time.time() - 2 * 86400
    os.utime(source / 'receipt.json', (old, old))
    return value


def _completed_panel_storage(source):
    import copy
    records = []
    for kind in ('whole', 'chunks', 'chunks', 'whole'):
        build = dict(contract='panel_time_chunk_reuse_v2' if kind == 'chunks' else 'panel_array_reuse_v1',
                     written_arrays=1, written_array_bytes=32, reused_arrays=0, avoided_array_bytes=0,
                     written_chunks=0, written_chunk_bytes=0, reused_chunks=0, avoided_chunk_bytes=0)
        records.append({'kind': kind, 'complete_axes_equal': True,
            'complete_array_hashes': {'features': 'a' * 64, 'close_prices': 'b' * 64},
            'load_seconds': .1, 'full_readback_verification_seconds': .2,
            'stages': {name: {'wall_seconds': .2, 'cpu_seconds': .1, 'storage_build': copy.deepcopy(build)}
                       for name in ('initial', 'append')}})
    value = {'state': 'accepted_complete_panel_storage_workflow', 'order': 'ABBA',
        'original_values_receipts_preserved': True, 'reader_rollout_required_before_writer_opt_in': True,
        'provider_calls': 0, 'production_cache_mutations': 0, 'dates': 3, 'symbols': 2, 'features': 1,
        'chunk_bytes': 8388608, 'minimum_chunked_bytes': 8388608,
        'source_manifest': '/private/panel/manifest.json',
        'source_proofs': [{'path': '/private/panel/manifest.json', 'sha256': 'c' * 64, 'metadata': {'size': 32}}],
        'source_code': {name: 'd' * 64 for name in ('scripts/benchmark_panel_chunk_storage.py',
            'stockagent/data/panel_cache.py', 'stockagent/data/panel_chunks.py')}, 'records': records}
    (source / 'acceptance.json').write_text(json.dumps(value))
    (source / 'progress.json').write_text(json.dumps({'records': records}))
    return value


def _original_panel_storage(source):
    """The actual first producer shape, with unknown code/policy left absent."""
    value = _completed_panel_storage(source)
    for key in ('source_code', 'chunk_bytes', 'minimum_chunked_bytes'):
        del value[key]
    value['observed_at_utc'] = '2026-10-15T18:15:34.520547+00:00'
    for record in value['records']:
        record.update(features_memmap=True, peak_process_rss_bytes=65536,
                      post_measurement_exact_links=0)
    (source / 'acceptance.json').write_text(json.dumps(value))
    (source / 'progress.json').write_text(json.dumps({'records': value['records']}))
    return value


@pytest.mark.parametrize('original_format', [False, True])
@pytest.mark.parametrize('suffix', ['final', 'v1', '8m_v1'])
def test_completed_panel_storage_case_keeps_original_bytes_and_uses_full_cold_restore(
        tmp_path, suffix, original_format):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.benchmark_fixtures import PANEL_STORAGE_CONTRACT
    root, source, row, catalog = example(tmp_path)
    relative = 'benchmarks/panel_chunk_storage_20261016_' + suffix
    target = root / relative
    target.parent.mkdir(parents=True)
    source.rename(target)
    (_original_panel_storage if original_format else _completed_panel_storage)(target)
    for path in target.glob('*.json'):
        old = time.time() - 2 * 86400
        os.utime(path, (old, old))
    before = {p.name: p.read_bytes() for p in target.iterdir()}
    observed = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    assert observed['blockers'] == []
    proof = cleanup.benchmark_evidence(target, destination=tmp_path / 'retained')
    assert proof['producer_contract'] == PANEL_STORAGE_CONTRACT
    if original_format:
        assert proof['producer_receipt_format'] == 'panel_original_abba_without_policy_v1'
        assert proof['source_code_identity'] == proof['chunk_policy'] == 'not_recorded_by_original_producer'
    assert proof == cleanup.benchmark_evidence(tmp_path / 'retained')
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(target, spec)
    published = publish_archive(spec, root, tmp_path / 'packed', repo_root=root.parent, manual_capture=True)
    recovered = verify_cold_archive(spec, tmp_path / 'packed')
    assert recovered['snapshot_id'] == published['snapshot_id'] and recovered['decoded_originals_verified']
    assert recovered['files'] == 3
    assert {p.name: p.read_bytes() for p in target.iterdir()} == before


@pytest.mark.parametrize('defect', ['missing_progress', 'different_progress', 'wrong_state', 'order',
    'partial', 'unequal_hash', 'empty_hashes', 'missing_append', 'nonfinite_time', 'missing_code',
    'relative_input', 'repeated_input', 'missing_manifest_proof', 'provider_call', 'production_write',
    'missing_counter', 'wrong_storage_contract', 'boolean_counter', 'lost_values'])
def test_panel_storage_completion_rejects_partial_changed_and_unbound_evidence(tmp_path, defect):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    value = _completed_panel_storage(tmp_path)
    if defect == 'wrong_state': value['state'] = 'running'
    elif defect == 'order': value['order'] = 'ABAB'
    elif defect == 'partial': value['records'].pop()
    elif defect == 'unequal_hash': value['records'][1]['complete_array_hashes']['features'] = 'e' * 64
    elif defect == 'empty_hashes':
        for row in value['records']: row['complete_array_hashes'] = {}
    elif defect == 'missing_append': del value['records'][0]['stages']['append']
    elif defect == 'nonfinite_time': value['records'][0]['stages']['append']['wall_seconds'] = float('nan')
    elif defect == 'missing_code': value['source_code'].pop('stockagent/data/panel_chunks.py')
    elif defect == 'relative_input': value['source_proofs'][0]['path'] = 'manifest.json'
    elif defect == 'repeated_input': value['source_proofs'].append(value['source_proofs'][0])
    elif defect == 'missing_manifest_proof': value['source_manifest'] = '/unknown/manifest.json'
    elif defect == 'provider_call': value['provider_calls'] = 1
    elif defect == 'production_write': value['production_cache_mutations'] = True
    elif defect == 'missing_counter': del value['records'][0]['stages']['append']['storage_build']['written_arrays']
    elif defect == 'wrong_storage_contract': value['records'][0]['stages']['append']['storage_build']['contract'] = 'unknown'
    elif defect == 'boolean_counter': value['records'][0]['stages']['append']['storage_build']['written_arrays'] = True
    elif defect == 'lost_values': value['original_values_receipts_preserved'] = False
    (tmp_path / 'acceptance.json').write_text(json.dumps(value))
    (tmp_path / 'progress.json').write_text(json.dumps({'records': value['records']}))
    if defect == 'missing_progress': (tmp_path / 'progress.json').unlink()
    elif defect == 'different_progress': (tmp_path / 'progress.json').write_text('{}')
    with pytest.raises(SnapshotError, match='completed producer'):
        completed_benchmark_evidence(tmp_path)
    assert (tmp_path / 'acceptance.json').exists()  # Rejection never retires evidence.


@pytest.mark.parametrize('defect', ['partial_modern_policy', 'unknown_header', 'missing_clock',
    'naive_clock', 'invalid_clock', 'incomplete_record', 'bool_rss', 'negative_links',
    'unequal_hash', 'different_progress', 'provider_call', 'nonfinite_time'])
def test_original_panel_receipt_rejects_hybrid_and_incomplete_evidence(tmp_path, defect):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    value = _original_panel_storage(tmp_path)
    if defect == 'partial_modern_policy': value['chunk_bytes'] = 8388608
    elif defect == 'unknown_header': value['new_success_claim'] = True
    elif defect == 'missing_clock': del value['observed_at_utc']
    elif defect == 'naive_clock': value['observed_at_utc'] = '2026-10-15T18:15:34'
    elif defect == 'invalid_clock': value['observed_at_utc'] = '2026-99-15T18:15:34Z'
    elif defect == 'incomplete_record': del value['records'][0]['features_memmap']
    elif defect == 'bool_rss': value['records'][0]['peak_process_rss_bytes'] = True
    elif defect == 'negative_links': value['records'][0]['post_measurement_exact_links'] = -1
    elif defect == 'unequal_hash': value['records'][1]['complete_array_hashes']['features'] = 'f' * 64
    elif defect == 'provider_call': value['provider_calls'] = 1
    elif defect == 'nonfinite_time': value['records'][0]['stages']['append']['cpu_seconds'] = float('inf')
    (tmp_path / 'acceptance.json').write_text(json.dumps(value))
    (tmp_path / 'progress.json').write_text(json.dumps({'records': value['records']}))
    if defect == 'different_progress': (tmp_path / 'progress.json').write_text('{}')
    before = (tmp_path / 'acceptance.json').read_bytes()
    with pytest.raises(SnapshotError, match='completed producer'):
        completed_benchmark_evidence(tmp_path)
    assert (tmp_path / 'acceptance.json').read_bytes() == before


def test_standalone_panel_cannot_borrow_other_producer_or_expand_to_suite(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence, RESEARCH_STAGING_CONTRACT
    _completed_panel_storage(tmp_path)
    with pytest.raises(SnapshotError, match='standalone case'):
        completed_benchmark_evidence(tmp_path, required_contract=RESEARCH_STAGING_CONTRACT)
    assert not cleanup.reviewed_wsl_artifact_root('benchmarks/panel_chunk_storage_20261016')
    assert not cleanup.reviewed_wsl_artifact_root('benchmarks/panel_chunk_storage_20261016_final/nested/case')


def test_standalone_research_staging_preserves_original_receipt_and_exact_cold_recovery(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    relative = 'benchmarks/research-stage-copy-20261021-v94'
    target = root / relative
    target.parent.mkdir(parents=True)
    source.rename(target)
    terminal = _completed_research_staging(target)
    original = (target / 'receipt.json').read_bytes()
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    observed = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    assert observed['blockers'] == []
    retained = tmp_path / 'retained-evidence'
    proof = cleanup.benchmark_evidence(target, destination=retained)
    assert proof == cleanup.benchmark_evidence(retained)
    assert proof['producer_state'] == terminal['status']
    assert (retained / 'receipt.json').read_bytes() == original
    assert cleanup.stage_profile({'staging_profile': cleanup.D_BENCHMARK_STAGE_PROFILE,
                                 'items': [{'relative_root': relative}]}) == (cleanup.D_BENCHMARK_STAGE_ROOT, True)
    published = publish_archive(spec, root, tmp_path / 'packed', repo_root=root.parent, manual_capture=True)
    recovered = verify_cold_archive(spec, tmp_path / 'packed')
    assert recovered['decoded_originals_verified'] and recovered['snapshot_id'] == published['snapshot_id']
    assert recovered['files'] == 2 and (target / 'receipt.json').read_bytes() == original


@pytest.mark.parametrize('problem', [
    'unfinished', 'order', 'truncated', 'rewritten_candidate', 'source_digest',
    'source_bool', 'code_digest', 'calls_bool', 'writes', 'restart', 'nan_time', 'seed',
])
def test_standalone_staging_rejects_incomplete_or_foreign_completion_without_deletion(tmp_path, problem):
    from stockagent.data_sync.benchmark_fixtures import completed_benchmark_evidence
    value = _completed_research_staging(tmp_path)
    if problem == 'unfinished':
        value['status'] = 'running'
    elif problem == 'order':
        value['samples'][0], value['samples'][1] = value['samples'][1], value['samples'][0]
    elif problem == 'truncated':
        value['samples'].pop()
    elif problem == 'rewritten_candidate':
        value['samples'][1]['written_payload_bytes'] = 32
    elif problem == 'source_digest':
        value['source']['sha256'] = 'unknown'
    elif problem == 'source_bool':
        value['source']['bytes'] = True
    elif problem == 'code_digest':
        value['code_sha256']['/code/immutable_cache_files.py'] = 'unknown'
    elif problem == 'calls_bool':
        value['provider_calls'] = False
    elif problem == 'writes':
        value['production_writes'] = True
    elif problem == 'restart':
        value['service_restarts'] = 1
    elif problem == 'nan_time':
        value['samples'][0]['wall_seconds'] = float('nan')
    else:
        value.pop('baseline_seed')
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps(value))
    before = receipt.read_bytes()
    with pytest.raises(SnapshotError, match='completed producer'):
        completed_benchmark_evidence(tmp_path)
    assert receipt.read_bytes() == before


def test_standalone_case_cannot_reuse_an_unrelated_suite_completion(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    relative = 'benchmarks/research-stage-copy-20261021-v94'
    target = root / relative
    target.parent.mkdir(parents=True)
    source.rename(target)
    _completed_benchmark(target)
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='standalone case'):
        source_plan(target, spec, manual_capture=True)
    observed = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    assert any('standalone case' in reason for reason in observed['blockers'])


def test_offline_stage_selection_uses_its_own_fixed_d_root():
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.D_OFFLINE_STAGE_PROFILE,
                 'items': [{'relative_root': 'operations/closed-audit/remote'}]}
    assert cleanup.stage_profile(inventory) == (cleanup.D_OFFLINE_STAGE_ROOT, True)
    with pytest.raises(SnapshotError, match='select only one'):
        cleanup.inventory([], None, guarded_d_scratch=True, guarded_d_offline_scratch=True)


@pytest.mark.parametrize('relative', ['operations', 'data_finlab', 'data_finlab/parquet',
                                      'benchmarks/suite/case', 'markets/experiment',
                                      'live/active', 'operations/../data_news'])
def test_offline_d_stage_cannot_capture_raw_sources_or_other_roles(relative):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.D_OFFLINE_STAGE_PROFILE,
                 'items': [{'relative_root': relative}]}
    with pytest.raises(SnapshotError):
        cleanup.stage_profile(inventory)


@pytest.mark.parametrize('change', ['multiple', 'existing', 'origin', 'catalog'])
def test_offline_d_stage_never_overrides_frozen_source_or_archive_roles(change):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.D_OFFLINE_STAGE_PROFILE,
                 'items': [{'relative_root': 'operations/closed-audit'}]}
    if change == 'multiple':
        inventory['items'] *= 2
    elif change == 'existing':
        inventory['items'][0]['existing_dataset'] = 'old-archive'
    elif change == 'origin':
        inventory['source_origin'] = 'retained-hot-mirror-preservation-v1'
    else:
        inventory['existing_catalog_sha256'] = 'a' * 64
    with pytest.raises(SnapshotError, match='staging profile'):
        cleanup.stage_profile(inventory)


@pytest.mark.parametrize('problem', ['foreign', 'suite-root', 'raw-source', 'multiple',
                                   'existing', 'physical-source', 'catalog', 'malformed'])
def test_d_temporary_stage_cannot_override_other_sources_or_existing_archives(problem):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = {'staging_profile': cleanup.D_BENCHMARK_STAGE_PROFILE,
                 'items': [{'relative_root': 'benchmarks/suite/completed-case'}]}
    if problem == 'foreign':
        inventory['staging_profile'] = '/arbitrary/volume'
    elif problem == 'suite-root':
        inventory['items'][0]['relative_root'] = 'benchmarks/suite'
    elif problem == 'raw-source':
        inventory['items'][0]['relative_root'] = 'data_tw_minute/shioaji_1m'
    elif problem == 'multiple':
        inventory['items'] *= 2
    elif problem == 'existing':
        inventory['items'][0]['existing_dataset'] = 'old-archive'
    elif problem == 'physical-source':
        inventory['source_origin'] = 'retained-hot-mirror-preservation-v1'
    elif problem == 'catalog':
        inventory['existing_catalog_sha256'] = 'a' * 64
    else:
        inventory['items'] = None
    with pytest.raises(SnapshotError, match='staging profile'):
        cleanup.stage_profile(inventory)


@pytest.mark.parametrize('problem', ['missing', 'failed', 'unfinished', 'foreign_contract', 'contract_type'])
def test_shared_capture_owner_also_rejects_unfinished_benchmarks_without_outer_cli(tmp_path, problem):
    root, source, row, catalog = example(tmp_path)
    target = root / 'benchmarks/suite/case'
    target.parent.mkdir(parents=True)
    source.rename(target)
    _completed_benchmark(target)
    row['relative_root'] = 'benchmarks/suite/case'
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    receipt = target / 'abba-receipt.json'
    if problem == 'missing':
        receipt.unlink()
    else:
        value = json.loads(receipt.read_bytes())
        if problem == 'failed':
            value['state'] = 'failed'
        elif problem == 'unfinished':
            value['trials'][0]['state'] = 'running'
        elif problem == 'foreign_contract':
            value['contract'] = 'foreign'
        else:
            value['contract'] = {'contract': 'not-a-string'}
        receipt.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='completed producer'):
        source_plan(target, spec, manual_capture=True)
    assert (target / 'unique-result.bin').is_file()


def test_publication_wait_phase_ends_before_commit_runs_under_the_existing_owner(tmp_path, monkeypatch):
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    target = tmp_path / 'receipts'
    target.mkdir()
    owner = tmp_path / 'existing-common-owner.lock'
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', owner)
    with cleanup.publication_owner(target):
        value = json.loads((target / 'phase.json').read_bytes())
        assert value['phase'] == 'publish-under-shared-owner' and value['state'] == 'running'
        with owner.open('a') as competing:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
    value = json.loads((target / 'phase.json').read_bytes())
    assert value['phase'] == 'publish-under-shared-owner' and value['state'] == 'completed'


@pytest.mark.parametrize('relative', ['benchmarks', 'benchmarks/service-suite',
    'benchmarks/service-suite/completed-case/nested', 'data_tw_public/source'])
def test_manual_benchmark_scope_cannot_expand_to_a_whole_suite_or_data_root(tmp_path, relative):
    from scripts import retire_local_offline_artifacts as cleanup
    root, _, row, catalog = example(tmp_path)
    row['relative_root'] = relative
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    with pytest.raises(SnapshotError):
        load_legacy_specs(catalog)
    with pytest.raises(SnapshotError):
        cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])


@pytest.mark.parametrize('problem', ['missing', 'failed', 'wrong_contract', 'unfinished', 'oversized',
                                    'symlink', 'fifo', 'redirected_destination', 'redirected_member', 'duplicate_json'])
def test_benchmark_evidence_preserves_unknown_or_unfinished_work_without_deletion(tmp_path, problem):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, _, _ = example(tmp_path)
    _completed_benchmark(source)
    receipt = source / 'abba-receipt.json'
    destination = tmp_path / 'retained'
    original = (source / 'unique-result.bin').read_bytes()
    if problem == 'missing':
        receipt.unlink()
    elif problem in {'failed', 'wrong_contract', 'unfinished'}:
        value = json.loads(receipt.read_bytes())
        if problem == 'failed':
            value['state'] = 'failed'
        elif problem == 'wrong_contract':
            value['contract'] = 'foreign'
        else:
            value['trials'][0]['state'] = 'running'
        receipt.write_text(json.dumps(value))
    elif problem == 'duplicate_json':
        receipt.write_text('{"state":"failed","state":"accepted_hidden"}')
    elif problem == 'oversized':
        with receipt.open('wb') as handle:
            handle.truncate(8 * 1024**2 + 1)
    elif problem == 'symlink':
        retained = source / 'receipt.original'
        receipt.rename(retained)
        receipt.symlink_to(retained)
    elif problem == 'fifo':
        receipt.unlink()
        os.mkfifo(receipt)
    elif problem == 'redirected_destination':
        outside = tmp_path / 'outside'
        outside.mkdir()
        destination.symlink_to(outside, target_is_directory=True)
    else:
        destination.mkdir()
        (destination / 'abba-receipt.json').symlink_to(receipt)
    with pytest.raises((SnapshotError, OSError)):
        cleanup.benchmark_evidence(source, destination=destination)
    assert (source / 'unique-result.bin').read_bytes() == original


def test_retired_transport_preserves_original_namespace_and_has_a_distinct_physical_role(tmp_path):
    from scripts.retire_local_offline_artifacts import source_layout,dataset_prefix,TRANSFER_QUARANTINE_ORIGIN
    root,source,row,catalog=example(tmp_path)
    relative='stockagent-transfer-quarantine/2026-08-20_ssh_transport'
    target=root/relative;target.parent.mkdir();source.rename(target)
    row.update(relative_root=relative,capture_contract=MANUAL_TRANSFER_QUARANTINE_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    spec=load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError,match='explicit manual capture'):
        source_plan(target,spec)
    packed=tmp_path/'packed'
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    proof=verify_cold_archive(spec,packed)
    assert proof['decoded_originals_verified'] and proof['snapshot_id']==published['snapshot_id']
    assert proof['manifest']['relative_root']==relative
    assert proof['manifest']['capture_contract']==MANUAL_TRANSFER_QUARANTINE_CONTRACT
    assert (target/'unique-result.bin').read_bytes()==b'unique offline research output'
    assert str(source_layout(TRANSFER_QUARANTINE_ORIGIN)[0])=='/srv'
    assert dataset_prefix(TRANSFER_QUARANTINE_ORIGIN)=='legacy-wsl-retired-transfer-'


@pytest.mark.parametrize('relative',[
    'stockagent-live/data_tw_public','stockagent-packed/objects',
    'stockagent-transfer-quarantine','stockagent-transfer-quarantine/arbitrary',
    'stockagent-transfer-quarantine/2026-08-20_ssh_transport/child'])
def test_retired_transport_contract_rejects_other_service_source_roots(tmp_path,relative):
    root,source,row,catalog=example(tmp_path)
    row.update(relative_root=relative,capture_contract=MANUAL_TRANSFER_QUARANTINE_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    with pytest.raises(SnapshotError):load_legacy_specs(catalog)


@pytest.mark.parametrize('name', ['research_dataset_developing_v5', 'research_dataset_schema2_volume_bug_20260807'])
def test_retired_minute_view_is_separate_exact_preservation_not_source_promotion(tmp_path, name):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    relative = 'data_tw_minute/'+name
    target = root.parent/relative
    target.parent.mkdir()
    source.rename(target)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(target, spec)
    packed = tmp_path/'packed'
    published = publish_archive(spec, root.parent, packed, repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['decoded_originals_verified'] and proof['snapshot_id']==published['snapshot_id']
    assert proof['manifest']['relative_root']==relative
    assert proof['manifest']['capture_contract']==MANUAL_MINUTE_DERIVED_VIEW_CONTRACT
    assert published['source_files']==1 and target.is_dir()
    assert cleanup.dataset_prefix(cleanup.MINUTE_DERIVED_VIEW_ORIGIN)=='legacy-wsl-minute-derived-'


@pytest.mark.parametrize('relative', ['data_tw_minute/research_dataset', 'data_tw_minute/shioaji_1m',
    'data_tw_minute', 'data_openBB/history', 'data_tw_minute/research_dataset_developing_v5/child'])
def test_retired_minute_view_cannot_include_current_raw_or_arbitrary_data_roots(tmp_path, relative):
    root, source, row, catalog = example(tmp_path)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    with pytest.raises(SnapshotError):
        load_legacy_specs(catalog)


@pytest.mark.parametrize('reference,protected', [('workspace', False), ('data-scope', True), ('selected-view', True)])
def test_minute_view_process_domain_keeps_real_data_ancestors_not_generic_workspace(tmp_path, monkeypatch, reference, protected):
    import subprocess
    import sys
    from datetime import datetime, timezone
    import stockagent.data_sync.artifact_retirement as common
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    import stockagent.data_sync.remote_legacy_return as remote
    from stockagent.data_sync.legacy_artifact_archive import legacy_process_scope
    from stockagent.data_sync.artifact_maintenance import artifact_process_references
    root, original, row, catalog = example(tmp_path)
    relative = 'data_tw_minute/research_dataset_developing_v5'
    source = root.parent/relative
    source.parent.mkdir()
    original.rename(source)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    argument = {'workspace': root.parent, 'data-scope': source.parent, 'selected-view': source}[reference]
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)',
                              '--scan-root', str(argument)], cwd=root.parent)
    try:
        scope = legacy_process_scope(spec, root.parent)
        assert scope == source.parent
        refs = artifact_process_references(source, scope)
        assert any(f'pid={child.pid}:' in ref for ref in refs) is protected
        # The same domain must survive rename and interrupted-transaction
        # resume. Generic workspace parents do not become data consumers.
        quarantine = tmp_path/'quarantine'
        quarantine.mkdir()
        source.rename(quarantine/'source')
        plan = {'source': str(source), 'hot_tree': str(tmp_path/'hot/view'),
                'snapshot_id': 'fixed-release'}
        options = {'artifact_root': root.parent, 'repo_root': root.parent,
                   'materialized_root': tmp_path/'materialized', 'hot_root': tmp_path/'hot',
                   'bridge_inactive': True, 'peer_proof': {'ok': True,
                       'checked_at': datetime.now(timezone.utc).isoformat(), 'peers': []}}
        monkeypatch.setattr(common, 'artifact_service_references', lambda paths, *args:
                            {str(path): [] for path in paths})
        monkeypatch.setattr(remote, 'active_configuration_references', lambda *args: [])
        if protected:
            with pytest.raises(SnapshotError, match='post-rename process references'):
                retirement._assert_legacy_unlink_gates(spec, plan, quarantine, **options)
        else:
            assert retirement._assert_legacy_unlink_gates(spec, plan, quarantine, **options)['ok']
        assert (quarantine/'source/unique-result.bin').is_file()
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_minute_view_named_cli_selection_uses_only_fixed_paths_without_source_argv(tmp_path, monkeypatch):
    import sys
    from scripts import retire_local_offline_artifacts as cleanup
    seen=[]
    monkeypatch.setattr(cleanup.socket, 'gethostname', lambda: 'penguin')
    def fake_inventory(roots, output, existing, **options):
        seen.append((roots, options))
        return {'items': []}
    monkeypatch.setattr(cleanup, 'inventory', fake_inventory)
    monkeypatch.setattr(sys, 'argv', ['retire_local_offline_artifacts.py','inventory','--minute-derived-view',
                                    '--minute-view-name','developing-v5','--inventory',str(tmp_path/'inventory.json')])
    assert cleanup.main()==0
    assert seen==[(['data_tw_minute/research_dataset_developing_v5'],
                   {'hot_mirror':False,'transfer_quarantine':False,'minute_derived_view':True})]


@pytest.mark.parametrize('reference', ['fd', 'mmap', 'cwd'])
def test_minute_view_final_gate_still_protects_real_quarantine_users(tmp_path, reference):
    import select
    import subprocess
    import sys
    from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec
    from stockagent.data_sync.legacy_artifact_retirement import _assert_legacy_unlink_gates
    repo = tmp_path/'repo'
    relative = 'data_tw_minute/research_dataset_developing_v5'
    source = repo/relative
    source.mkdir(parents=True)
    (source/'original.bin').write_bytes(b'exact preserved original')
    quarantine = tmp_path/'quarantine'
    quarantine.mkdir()
    source.rename(quarantine/'source')
    spec = LegacyArchiveSpec('legacy-minute', relative, 7, tmp_path/'stage',
                             capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    code = ('import mmap, os, sys, time; '
            'kind, name = sys.argv[1:]; '
            'handle = open(name, "rb") if kind != "cwd" else None; '
            'mapping = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) if kind == "mmap" else None; '
            'handle.close() if kind == "mmap" else None; '
            'os.chdir(os.path.dirname(name)) if kind == "cwd" else None; '
            'print("ready", flush=True); time.sleep(30)')
    child = subprocess.Popen([sys.executable, '-c', code, reference,
                              str(quarantine/'source/original.bin')], stdout=subprocess.PIPE)
    try:
        assert select.select([child.stdout], [], [], 5)[0], 'reference holder did not become ready'
        assert child.stdout.readline() == b'ready\n'
        plan = {'source': str(source), 'hot_tree': str(tmp_path/'hot'/relative),
                'snapshot_id': 'fixed-release'}
        with pytest.raises(SnapshotError, match='post-rename process references'):
            _assert_legacy_unlink_gates(spec, plan, quarantine, artifact_root=repo)
        assert (quarantine/'source/original.bin').read_bytes() == b'exact preserved original'
    finally:
        child.terminate()
        child.wait(timeout=5)
        child.stdout.close()


@pytest.mark.parametrize('changed', [False, True])
def test_publication_owner_excludes_complete_c_and_d_reads_but_keeps_source_guard(tmp_path, monkeypatch, changed):
    from contextlib import contextmanager
    import fcntl
    import stockagent.data_sync.legacy_artifact_archive as archive
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    prepare_archive(spec, root, manual_capture=True)
    owner_path = tmp_path/'common.lock';events=[]
    def held():
        with owner_path.open('a') as probe:
            try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return True
            fcntl.flock(probe,fcntl.LOCK_UN)
            return False
    @contextmanager
    def owner():
        with owner_path.open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if changed:
                path=source/'unique-result.bin';before=path.stat()
                path.write_bytes(b'newly changed source')
                os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns))
            yield
    real_c, real_publish, real_d = archive.verify_archive_directory, archive.publish_packed_snapshot, archive.verify_packed_snapshot
    def check_c(*a, **kw):
        events.append(('original-read',held()));return real_c(*a,**kw)
    def publish(*a, **kw):
        events.append(('publish',held()));return real_publish(*a,**kw)
    def check_d(*a, **kw):
        events.append(('cold-read',held()));return real_d(*a,**kw)
    monkeypatch.setattr(archive,'verify_archive_directory',check_c)
    monkeypatch.setattr(archive,'publish_packed_snapshot',publish)
    monkeypatch.setattr(archive,'verify_packed_snapshot',check_d)
    cold=tmp_path/'packed'
    if changed:
        with pytest.raises(SnapshotError,match='source changed before cold commit'):
            publish_archive(spec,root,cold,repo_root=root.parent,manual_capture=True,publication_owner=owner)
        assert not list((cold/'heads').rglob('*.json'))
        assert source.exists()
    else:
        published=publish_archive(spec,root,cold,repo_root=root.parent,manual_capture=True,publication_owner=owner)
        assert events==[('original-read',False),('publish',True),('cold-read',False),('original-read',False)]
        assert verify_cold_archive(spec,cold)['manifest_sha256']==published['manifest_sha256']


@pytest.mark.parametrize('scope', ['live', 'runtime', '..', 'data_tw_public'])
def test_manual_contract_does_not_expand_to_live_ledgers_or_source_roots(tmp_path, scope):
    root, source, row, catalog = example(tmp_path)
    row['relative_root'] = scope + '/example'
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    with pytest.raises(SnapshotError):
        load_legacy_specs(catalog)


def test_manual_capture_checks_independent_configuration_consumers_before_encoding(tmp_path, monkeypatch):
    root, source, row, catalog = example(tmp_path)
    import stockagent.data_sync.remote_legacy_return as consumers
    monkeypatch.setattr(consumers, 'active_configuration_references', lambda *a: ['independent-training-config'])
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='in use'):
        prepare_archive(spec, root, manual_capture=True)
    assert not (spec.stage_root / spec.dataset).exists()


@pytest.mark.parametrize('operation', ['prepare', 'verify', 'publish'])
def test_separate_physical_source_always_uses_explicit_repository_consumers(tmp_path, monkeypatch, operation):
    import stockagent.data_sync.remote_legacy_return as consumers
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    repository = tmp_path/'actual-repository';repository.mkdir()
    if operation == 'verify':
        prepare_archive(spec, root, manual_capture=True)
    seen=[]
    def references(_source, repo):
        seen.append(repo)
        return ['configured-web-consumer'] if repo == repository else []
    monkeypatch.setattr(consumers, 'active_configuration_references', references)
    with pytest.raises(SnapshotError, match='in use'):
        if operation == 'prepare':
            prepare_archive(spec, root, manual_capture=True, repo_root=repository)
        elif operation == 'verify':
            verify_archive_directory(spec.stage_root/spec.dataset/'archive', source,
                spec=spec, manual_capture=True, artifact_root=root, repo_root=repository)
        else:
            publish_archive(spec, root, tmp_path/'packed', manual_capture=True, repo_root=repository)
    assert seen and all(repo == repository for repo in seen)
    assert source.is_dir()
    assert not (tmp_path/'packed/heads'/spec.dataset).exists()


@pytest.mark.parametrize('origin, physical', [('unknown', '/tmp/arbitrary'),
    ('retained-hot-mirror-preservation-v1', '/tmp/arbitrary')])
def test_hot_mirror_inventory_cannot_redirect_the_cleanup_source(tmp_path, origin, physical):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = tmp_path/'inventory.json';receipts=tmp_path/'receipts';receipts.mkdir()
    value={'schema_version':1,'contract':MANUAL_WSL_CAPTURE_CONTRACT,'authority_node_id':'penguin',
           'source_origin':origin,'artifact_root':physical,'items':[{'relative_root':'replays/example'}]}
    inventory.write_text(json.dumps(value))
    with pytest.raises(SnapshotError,match='source origin|physical source'):
        cleanup._apply(inventory, inventory.read_bytes(), receipts)
    assert not list(receipts.iterdir())


@pytest.mark.parametrize('origin', ['retained-transfer-quarantine-preservation-v1',
                                  'retained-minute-derived-view-preservation-v1'])
def test_separate_source_frozen_inventory_rejects_other_reviewed_artifact_scopes(tmp_path, origin):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = tmp_path/'inventory.json'
    receipts = tmp_path/'receipts'
    receipts.mkdir()
    value = {'schema_version':1, 'contract':MANUAL_WSL_CAPTURE_CONTRACT,
             'authority_node_id':'penguin', 'source_origin':origin,
             'artifact_root':str(cleanup.source_layout(origin)[0]),
             'items':[{'relative_root':'markets/unrelated-source'}]}
    inventory.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='unreviewed physical root'):
        cleanup._apply(inventory, inventory.read_bytes(), receipts)
    assert not list(receipts.iterdir())


def test_manifest_contract_change_cannot_be_used_as_recovery_proof(tmp_path):
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    prepare_archive(spec, root, manual_capture=True)
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    with pytest.raises(SnapshotError, match='identity differs'):
        verify_archive_directory(spec.stage_root / spec.dataset / 'archive', spec=replace(spec, capture_contract=None))


def test_explicit_offline_simulation_has_exact_recovery_and_remains_manual(tmp_path):
    root, source, row, catalog = example(tmp_path)
    relative = 'live/official-close-settlement-old-experiment'
    (root / 'live').mkdir()
    source.rename(root / relative)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        prepare_archive(spec, root)
    release = publish_archive(spec, root, tmp_path / 'packed', repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, tmp_path / 'packed')
    assert proof['snapshot_id'] == release['snapshot_id'] and proof['decoded_originals_verified']
    assert proof['manifest']['capture_contract'] == MANUAL_OFFLINE_SIMULATION_CONTRACT


@pytest.mark.parametrize('relative', ['live/tw_day_trade_simulation', 'live/data_monitor',
    'live/shioaji_taifex_volatility_simulation', 'live/official-close-settlement-',
    'live/official-close-settlement-old/nested', 'markets/old-result'])
def test_offline_simulation_marker_never_admits_canonical_or_arbitrary_live_roots(tmp_path, relative):
    root, source, row, catalog = example(tmp_path)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    with pytest.raises(SnapshotError, match='excludes canonical'):
        load_legacy_specs(catalog)


def test_promoted_offline_simulation_stays_protected_when_a_service_uses_it(tmp_path, monkeypatch):
    root, source, row, catalog = example(tmp_path)
    relative = 'live/official-close-settlement-promoted'
    (root / 'live').mkdir()
    source.rename(root / relative)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    import stockagent.data_sync.artifact_consumers as consumers
    monkeypatch.setattr(consumers, 'artifact_service_references',
                        lambda roots, repo: {str(p): ['promoted-live-state'] for p in roots})
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='in use'):
        prepare_archive(spec, root, manual_capture=True)
    assert (root / relative / 'unique-result.bin').is_file()


def test_existing_archive_retirement_binds_catalog_and_rejects_later_drift(tmp_path, monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup
    import hashlib
    root, source, row, catalog = example(tmp_path)
    row['stage_root'] = '/var/lib/stockagent-legacy-archive-stage'
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    monkeypatch.setattr(cleanup, 'ROOT', root.parent)
    monkeypatch.setattr(cleanup, 'EXISTING_CATALOG', catalog)
    result = cleanup.inventory([], tmp_path / 'fixed-inventory.json', [row['dataset']])
    assert result['items'][0]['existing_dataset'] == row['dataset']
    assert result['existing_catalog_sha256'] == hashlib.sha256(catalog.read_bytes()).hexdigest()
    row['relative_root'] = 'replays/different-root'
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    with pytest.raises(SnapshotError, match='catalog changed'):
        cleanup._apply(tmp_path / 'fixed-inventory.json', json.dumps(result).encode(), tmp_path / 'receipts')
    assert (source / 'unique-result.bin').is_file()


def test_phase_status_reports_failure_without_a_retirement_claim(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    with pytest.raises(SnapshotError):
        with phase(tmp_path, 'independent-D-original-recovery'):
            assert json.loads((tmp_path / 'phase.json').read_text())['state'] == 'running'
            raise SnapshotError('cold original corrupted')
    result = json.loads((tmp_path / 'phase.json').read_text())
    assert result['state'] == 'failed' and result['error_type'] == 'SnapshotError'
    assert result['elapsed_seconds'] >= 0
    assert 'retired' not in result and not (tmp_path / 'acceptance.json').exists()


def test_failed_retirement_records_error_location_without_frame_locals():
    import errno
    from scripts import retire_local_offline_artifacts as cleanup
    def failing_stage():
        local_secret = 'not part of the diagnostic'
        raise FileNotFoundError(errno.ENOENT, 'missing', '/example/encoded.raw')
    try:
        failing_stage()
    except FileNotFoundError as error:
        diagnostic = cleanup._failure_diagnostics(error)
    assert diagnostic['error_type'] == 'FileNotFoundError'
    assert diagnostic['errno'] == errno.ENOENT
    assert diagnostic['traceback_frames'][-1]['function'] == 'failing_stage'
    assert 'local_secret' not in json.dumps(diagnostic)


@pytest.mark.parametrize('current_service', [False, True])
def test_previously_retired_root_recovered_for_service_is_retained(tmp_path, monkeypatch, current_service):
    import hashlib
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    monkeypatch.setattr(cleanup, 'ROOT', root.parent)
    cfg = cleanup.RetentionConfig(tmp_path/'cold', tmp_path/'archive', tmp_path/'cache',
                                  tmp_path/'backup.json', tmp_path/'state', 'packed', ())
    monkeypatch.setattr(cleanup.RetentionConfig, 'load', lambda *a, **k: cfg)
    monkeypatch.setattr(cleanup, 'load_retirement_peer_names', lambda *a, **k: ())
    monkeypatch.setattr(cleanup, 'artifact_service_references',
                        lambda roots, repo: {str(p): ['execution-replay.source_files'] if current_service else [] for p in roots})
    relative = 'replays/old-result'
    fingerprint = 'fixed-observation'
    dataset = 'legacy-wsl-offline-' + hashlib.sha256((relative+fingerprint).encode()).hexdigest()[:24]
    receipts = tmp_path/'receipts'
    target = receipts/dataset
    target.mkdir(parents=True)
    prior = target/'acceptance.json'
    prior.write_text(json.dumps({'retired': True, 'dataset': dataset, 'relative_root': relative}))
    original_receipt = prior.read_bytes()
    inventory = tmp_path/'inventory.json'
    raw = json.dumps({'schema_version': 1, 'contract': MANUAL_WSL_CAPTURE_CONTRACT,
                      'authority_node_id': 'penguin', 'items': [{'relative_root': relative, 'fingerprint': fingerprint}]}).encode()
    inventory.write_bytes(raw)
    if current_service:
        result = cleanup._apply(inventory, raw, receipts)
        assert result['all_selected_retired'] is False
        assert result['items'][0]['retained_for_current_service'] is True
        assert result['items'][0]['service_references'] == ['execution-replay.source_files']
    else:
        with pytest.raises(SnapshotError, match='requires reconciliation'):
            cleanup._apply(inventory, raw, receipts)
    assert prior.read_bytes() == original_receipt
    assert (source/'unique-result.bin').read_bytes() == b'unique offline research output'
    assert not (target/'publication.json').exists()


@pytest.mark.parametrize('changed',[False,True])
def test_reuse_completed_publication_requires_fixed_identity_and_claims_no_recovery(tmp_path,changed):
    from scripts import retire_local_offline_artifacts as cleanup
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    packed=tmp_path/'packed';target=tmp_path/'receipt';target.mkdir()
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    assert cleanup.reusable_publication(target,spec,packed) is None
    if changed:published['manifest_sha256']='0'*64
    (target/'publication.json').write_text(json.dumps(published))
    if changed:
        with pytest.raises(SnapshotError,match='fixed D release'):
            cleanup.reusable_publication(target,spec,packed)
    else:
        reused=cleanup.reusable_publication(target,spec,packed)
        assert reused==published and 'cold_verified' not in reused
    assert source.is_dir()


def test_preparation_progress_is_written_by_the_canonical_publication_owner(tmp_path, monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync import legacy_artifact_archive as archives
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed, target = tmp_path / 'packed', tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'publication.lock')
    monkeypatch.setattr(cleanup, '_check_d_primary_mount', lambda path: None)
    monkeypatch.setattr(archives, '_manual_capture_usage_gate', lambda *a, **kw: None)
    monkeypatch.setattr(archives, 'artifact_process_references', lambda *a, **kw: [])
    published = cleanup.prepare_or_resume_publication(spec, root, packed, target, repo_root=root.parent)
    progress = json.loads((target / 'preparation-progress.json').read_bytes())
    assert progress['contract'] == 'manual-retirement-preparation-progress-v1'
    assert progress['stage'] == 'encode-and-check-receipts'
    assert progress['files_completed'] == progress['files_total'] == 1
    assert progress['source_bytes_completed'] == progress['source_bytes_total'] == (source / 'unique-result.bin').stat().st_size
    assert progress['acceptance'] is False
    assert verify_cold_archive(spec, packed)['snapshot_id'] == published['snapshot_id']
    assert source.is_dir() and not (target / 'acceptance.json').exists()


@pytest.mark.parametrize('defect', [None, 'source', 'encoding'])
@pytest.mark.parametrize('d_primary', [False, True])
def test_complete_uncommitted_encoding_resume_keeps_canonical_full_audits(tmp_path, monkeypatch, defect, d_primary):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync import legacy_artifact_archive as archives
    from stockagent.data_sync import syncthing_scan as scan
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed, target = tmp_path/'packed', tmp_path/'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path/'publication.lock')
    checked_roots = []
    monkeypatch.setattr(cleanup, '_check_d_primary_mount', lambda path: checked_roots.append(path))
    if d_primary:
        packed.mkdir()
        (packed / scan.D_PRIMARY_MARKER).write_text('{}')
        monkeypatch.setattr(cleanup, 'D_PRIMARY_ROOT', packed)
        monkeypatch.setattr(scan, 'CANONICAL_ROOT', packed)
    monkeypatch.setattr(scan, '_scan_pending', lambda *a, **kw: pytest.fail('publication must not wait for a network scan'))
    manifest = prepare_archive(spec, root, manual_capture=True, repo_root=root.parent)
    archive = spec.stage_root/spec.dataset/'archive'
    if defect == 'source':
        (source/'unique-result.bin').write_bytes(b'new source generation')
    elif defect == 'encoding':
        (archive/manifest['files'][0]['encoded_path']).write_bytes(b'broken encoded bytes')
    def forbidden(*args, **kwargs):
        pytest.fail('complete encodings use the canonical publisher rather than another outer full scan')
    for name in ('prepare_archive', 'verify_archive_directory'):
        monkeypatch.setattr(cleanup, name, forbidden)
    calls = []
    verifier = archives.verify_archive_directory
    def checked(*args, **kwargs):
        calls.append('canonical-complete-verification')
        return verifier(*args, **kwargs)
    monkeypatch.setattr(archives, 'verify_archive_directory', checked)
    if defect:
        with pytest.raises(SnapshotError):
            cleanup.prepare_or_resume_publication(spec, root, packed, target, repo_root=root.parent)
        assert calls == ['canonical-complete-verification']
        assert not (target/'publication.json').exists()
        assert not (packed/'heads'/spec.dataset).exists()
    else:
        published = cleanup.prepare_or_resume_publication(spec, root, packed, target, repo_root=root.parent)
        assert calls == ['canonical-complete-verification'] * 2
        assert json.loads((target/'publication.json').read_bytes()) == published
        progress = json.loads((target/'publication-progress.json').read_bytes())
        assert progress['contract'] == 'manual-retirement-publication-progress-v1'
        # The final event is the post-publication complete tree readback,
        # including directories. It must not masquerade as a file-only count.
        assert progress['acceptance'] is False and progress['acceptance_proof'] is False
        assert progress['stage'] == 'post-publication-verify-materialized-tree'
        assert progress['item_scope'] == 'filesystem_entries_observed'
        assert progress['completed_items'] == progress['total_items'] > 0
        assert progress['fingerprint_matched'] is True
        proof = verify_cold_archive(spec, packed)
        assert proof['decoded_originals_verified'] and proof['snapshot_id'] == published['snapshot_id']
        pending = packed / '.local-state/scan-pending' / (spec.dataset + '.json')
        assert pending.exists() is d_primary
        if d_primary:
            intent = json.loads(pending.read_bytes())
            assert intent['dataset'] == spec.dataset and intent['new_object_paths']
            assert intent['generation']
    assert checked_roots == [packed]
    assert source.is_dir()


def test_manual_d_publication_requires_durable_scan_intent(tmp_path, monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync import syncthing_scan as scan

    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed, target = tmp_path/'packed', tmp_path/'receipt'
    packed.mkdir()
    target.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text('{}')
    monkeypatch.setattr(cleanup, 'D_PRIMARY_ROOT', packed)
    monkeypatch.setattr(scan, 'CANONICAL_ROOT', packed)
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path/'publication.lock')
    monkeypatch.setattr(cleanup, '_check_d_primary_mount', lambda path: None)
    monkeypatch.setattr(scan, 'queue_after_publish', lambda *a, **kw: False)
    monkeypatch.setattr(scan, '_scan_pending', lambda *a, **kw: pytest.fail('must not send another scan'))
    with pytest.raises(SnapshotError, match='durable scan intent'):
        cleanup.prepare_or_resume_publication(spec, root, packed, target, repo_root=root.parent)
    assert not (target/'publication.json').exists()
    assert source.is_dir() and (source/'unique-result.bin').is_file()


@pytest.mark.parametrize('defect', [None, 'source', 'encoding'])
def test_fixed_publication_resume_avoids_preparation_but_keeps_all_byte_audits(tmp_path, monkeypatch, defect):
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed, target = tmp_path/'packed', tmp_path/'receipt'
    target.mkdir()
    published = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    (target/'publication.json').write_text(json.dumps(published))
    archive = spec.stage_root/spec.dataset/'archive'
    if defect == 'source':
        (source/'unique-result.bin').write_bytes(b'new source generation')
    elif defect == 'encoding':
        member = next(p for p in archive.rglob('*') if p.is_file() and p.name != 'legacy_archive_manifest.json')
        member.write_bytes(b'broken encoded bytes')
    def forbidden(*args, **kwargs):
        pytest.fail('a fixed publication resumes at mandatory D recovery, not preparation/republication')
    for name in ('prepare_archive', 'verify_archive_directory', 'publish_archive'):
        monkeypatch.setattr(cleanup, name, forbidden)
    resumed = cleanup.prepare_or_resume_publication(spec, root, packed, target, repo_root=root.parent)
    assert resumed == published and 'cold_verified' not in resumed
    if defect == 'encoding':
        with pytest.raises((SnapshotError, OSError, ValueError)):
            cleanup.verify_for_retirement(spec, packed, target)
    else:
        proof = cleanup.verify_for_retirement(spec, packed, target)
        assert proof['decoded_originals_verified'] and proof['snapshot_id'] == resumed['snapshot_id']
        if defect == 'source':
            with pytest.raises(SnapshotError):
                verify_archive_directory(archive, source, spec=spec, artifact_root=root,
                                         repo_root=root.parent, manual_capture=True)
        else:
            assert verify_archive_directory(archive, source, spec=spec, artifact_root=root,
                repo_root=root.parent, manual_capture=True)['files'] == 1
    assert source.exists()  # Neither resumption nor recovery is deletion permission.


@pytest.mark.parametrize('defect', [None, 'cold-object', 'encoding-after-decode'])
def test_parallel_d_recovery_keeps_independent_cold_decode_and_post_decode_checks(
        tmp_path, monkeypatch, defect):
    import threading
    import stockagent.data_sync.legacy_artifact_archive as legacy
    from stockagent.data_sync.packed_snapshots import resolve_latest_packed

    root, source, row, catalog = example(tmp_path)
    stamp = time.time() - 2 * 86400
    for index in range(75):
        path = source / f'extra-{index:03d}'
        path.write_bytes(f'source-{index:03d}'.encode())
        os.utime(path, (stamp, stamp))
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed = tmp_path / 'packed'
    published = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    archive = spec.stage_root / spec.dataset / 'archive'
    monkeypatch.setattr(legacy, 'D_BENCHMARK_STAGE_ROOT', spec.stage_root)

    if defect == 'cold-object':
        resolved = resolve_latest_packed(packed, spec.dataset)
        object_path = packed / resolved.manifest['archive']['objects'][0]['relpath']
        raw = object_path.read_bytes()
        object_path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])

    original = legacy._encoded_original_hashes
    lock = threading.Lock()
    concurrent = threading.Event()
    active = peak = 0

    def tracked_hash(*args):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if peak >= 4:
                concurrent.set()
        if not concurrent.wait(timeout=5):
            raise AssertionError('four decoding workers did not start')
        try:
            return original(*args)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(legacy, '_encoded_original_hashes', tracked_hash)
    events = []
    changed = False

    def progress(event):
        nonlocal changed
        events.append(event)
        if (defect == 'encoding-after-decode' and not changed
                and event.get('recovery_step') == 'decode-originals'
                and event.get('stage') == 'decode-originals'
                and event.get('completed_items') == event.get('total_items')):
            member = next(p for p in archive.rglob('*') if p.is_file()
                          and p.name != 'legacy_archive_manifest.json')
            data = member.read_bytes()
            member.write_bytes(bytes([data[0] ^ 1]) + data[1:])
            changed = True

    if defect:
        with pytest.raises((SnapshotError, OSError)):
            verify_cold_archive(spec, packed, reuse_encoded_archive=archive, progress=progress)
        if defect == 'encoding-after-decode':
            assert changed and peak == 4
    else:
        proof = verify_cold_archive(spec, packed, reuse_encoded_archive=archive, progress=progress)
        assert proof['decoded_originals_verified']
        assert proof['manifest_sha256'] == published['manifest_sha256']
        assert proof['cold_reconstruction']['independently_reconstructed_files'] == 77
        assert peak == 4
    assert source.is_dir() and (source / 'unique-result.bin').is_file()


def test_parallel_legacy_verification_does_not_extend_to_other_stages(tmp_path):
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='restricted'):
        verify_archive_directory(spec.stage_root / spec.dataset / 'archive', source,
                                 spec=spec, file_workers=4)


@pytest.mark.parametrize('changed', [False, True])
def test_parallel_legacy_source_audit_keeps_full_source_bytes(tmp_path, monkeypatch, changed):
    import stockagent.data_sync.legacy_artifact_archive as legacy
    root, source, row, catalog = example(tmp_path)
    stamp = time.time() - 2 * 86400
    for index in range(6):
        path = source / f'extra-{index:03d}'
        path.write_bytes(f'source-{index:03d}'.encode())
        os.utime(path, (stamp, stamp))
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed = tmp_path / 'packed'
    publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    archive = spec.stage_root / spec.dataset / 'archive'
    monkeypatch.setattr(legacy, 'D_BENCHMARK_STAGE_ROOT', spec.stage_root)
    if changed:
        path = source / 'extra-001'
        original = path.stat()
        path.write_bytes(b'x' * original.st_size)
        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    if changed:
        with pytest.raises(SnapshotError, match='source differs'):
            legacy.verify_archive_directory(archive, source, spec=spec, manual_capture=True,
                artifact_root=root, repo_root=root.parent, file_workers=4)
    else:
        proof = legacy.verify_archive_directory(archive, source, spec=spec, manual_capture=True,
            artifact_root=root, repo_root=root.parent, file_workers=4)
        assert proof['files'] == 7
    assert source.exists()


def test_queued_cleanup_cannot_enter_the_existing_cohort_transaction(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    receipts=tmp_path/'receipts';receipts.mkdir()
    fixed=tmp_path/'inventory.json';fixed.write_text('{}')
    entered=[]
    monkeypatch.setattr(cleanup,'_apply',lambda *args:entered.append(True) or {'all_selected_retired':True})
    with (receipts/'owner.lock').open('a') as owner, ThreadPoolExecutor(max_workers=1) as pool:
        fcntl.flock(owner,fcntl.LOCK_EX)
        with pytest.raises(BlockingIOError):
            cleanup.apply(fixed,receipts)
        queued=pool.submit(cleanup.apply,fixed,receipts,wait_for_owner=True)
        time.sleep(0.05)
        assert not queued.done() and not entered
        fcntl.flock(owner,fcntl.LOCK_UN)
        assert queued.result(timeout=5)=={'all_selected_retired':True}
    assert entered==[True]


@pytest.mark.parametrize('ram_available',[False,True])
def test_recovery_scratch_selection_keeps_exact_decode_and_physical_capacity_gate(
        tmp_path,monkeypatch,ram_available):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.training_return as admission
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    packed=tmp_path/'packed'
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    target=tmp_path/'receipts';target.mkdir()
    monkeypatch.setattr(cleanup.shutil,'disk_usage',lambda path:SimpleNamespace(free=(64*1024**3 if ram_available else 0)))
    admitted=[]
    def reject(path,required):
        admitted.append((path,required))
        raise SnapshotError('actual physical backing drive lacks capacity')
    monkeypatch.setattr(admission,'admit_workspace',reject)
    proof=cleanup.verify_for_retirement(spec,packed,target)
    assert proof['decoded_originals_verified'] and proof['manifest_sha256']==published['manifest_sha256']
    assert not admitted
    if not ram_available:
        assert proof['verification_scratch_created'] is False
        assert proof['cold_reconstruction']['independently_reconstructed_files'] > 0
    assert source.is_dir()
    assert not list(target.glob('verification-scratch/stockagent-legacy-verify-*'))
    progress = json.loads((target / 'recovery-progress.json').read_bytes())
    assert progress['snapshot_id'] == published['snapshot_id']
    assert progress['manifest_sha256'] == published['manifest_sha256']
    assert progress['acceptance_proof'] is False
    assert progress['state'] == 'verified'
    assert not (target / 'retirement.json').exists()


def test_recovery_interrupt_keeps_partial_telemetry_and_never_accepts_retirement(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed = tmp_path / 'packed'
    published = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    target = tmp_path / 'receipts'
    target.mkdir()
    monkeypatch.setattr(cleanup.shutil, 'disk_usage', lambda path: SimpleNamespace(free=64*1024**3))
    def interrupted(spec, cold, *, progress, **options):
        progress({'stage': 'hash-cold-blobs', 'recovery_step': 'fetch-encoded-archive',
                  'completed_items': 5, 'total_items': 10, 'completed_bytes': 1024, 'total_bytes': 2048})
        raise KeyboardInterrupt()
    monkeypatch.setattr(cleanup, 'verify_cold_archive', interrupted)
    with pytest.raises(KeyboardInterrupt):
        cleanup.verify_for_retirement(spec, packed, target)
    progress = json.loads((target / 'recovery-progress.json').read_bytes())
    assert progress['state'] == 'interrupted' and progress['error_type'] == 'KeyboardInterrupt'
    assert progress['completed_items'] == 5 and progress['completed_bytes'] == 1024
    assert progress['snapshot_id'] == published['snapshot_id']
    assert progress['acceptance_proof'] is False
    assert source.is_dir() and not (target / 'retirement.json').exists()


@pytest.mark.parametrize('failure', [False, True])
def test_quarantine_audit_progress_is_observation_only_and_preserves_failure(tmp_path, monkeypatch, failure):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    target = tmp_path / 'receipts'
    target.mkdir()
    spec = SimpleNamespace(dataset='exact-dataset')
    prior = {'snapshot_id': 'fixed-snapshot', 'manifest_sha256': 'fixed-manifest'}

    def audit(actual_spec, actual_prior, *, progress, **options):
        assert actual_spec is spec and actual_prior is prior and options == {'guard': True}
        progress({'audit_step': 'verify-encoded-and-quarantined-originals',
                  'stage': 'decode-originals', 'completed_items': 5, 'total_items': 10})
        if failure:
            raise SnapshotError('retain quarantine')
        return {'full_verification': {'original_files': 10}}

    monkeypatch.setattr(cleanup, 'plan_legacy_quarantine_resume', audit)
    if failure:
        with pytest.raises(SnapshotError, match='retain quarantine'):
            cleanup.audit_quarantine_resume(spec, prior, {'guard': True}, target, 2)
    else:
        assert cleanup.audit_quarantine_resume(spec, prior, {'guard': True}, target, 2)['full_verification']['original_files'] == 10
    progress = json.loads((target / 'quarantine-audit-progress.json').read_bytes())
    assert progress['state'] == ('failed' if failure else 'verified')
    assert progress['attempt'] == 2 and progress['manifest_sha256'] == 'fixed-manifest'
    assert progress['acceptance_proof'] is False
    if failure:
        assert progress['completed_items'] == 5 and progress['error_type'] == 'SnapshotError'
    assert not (target / 'retirement.json').exists()


def test_post_decode_corruption_persists_failure_without_retirement(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.legacy_artifact_archive as archive_module

    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    packed = tmp_path / 'packed'
    published = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    target = tmp_path / 'receipts'
    target.mkdir()
    monkeypatch.setattr(cleanup.shutil, 'disk_usage', lambda path: SimpleNamespace(free=0))
    verify_originals = archive_module.verify_archive_directory

    def change_encoding_after_decode(*args, **kwargs):
        proof = verify_originals(*args, **kwargs)
        encoded = next((spec.stage_root / spec.dataset / 'archive' / 'payload').rglob('*.raw'))
        contents = encoded.read_bytes()
        assert contents
        encoded.write_bytes(bytes([contents[0] ^ 1]) + contents[1:])
        return proof

    monkeypatch.setattr(archive_module, 'verify_archive_directory', change_encoding_after_decode)
    with pytest.raises(SnapshotError, match='checksum mismatch'):
        cleanup.verify_for_retirement(spec, packed, target)
    progress = json.loads((target / 'recovery-progress.json').read_bytes())
    assert progress['state'] == 'failed' and progress['error_type'] == 'SnapshotError'
    assert progress['snapshot_id'] == published['snapshot_id']
    assert progress['manifest_sha256'] == published['manifest_sha256']
    assert progress['recovery_step'] == 'decode-originals'
    assert progress['stage'] == 'verify-materialized-post-decode-files'
    assert progress['acceptance_proof'] is False
    assert source.is_dir() and not (target / 'retirement.json').exists()


def test_inventory_drift_only_admits_removed_hardlinks(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    root,source,row,catalog=example(tmp_path)
    neighbor=tmp_path/'other-hardlink'
    os.link(source/'unique-result.bin',neighbor)
    before=cleanup.observe(root,'replays/old-result', service_refs=[], configuration_refs=[], process_refs=[])
    neighbor.unlink()
    observed=cleanup.observe(root,'replays/old-result', service_refs=[], configuration_refs=[], process_refs=[])
    first_ctime=before['observation_rows'][0]['signature'][4]
    observed_ctime=observed['observation_rows'][0]['signature'][4]
    expected=[{'path':'unique-result.bin','before_links':2,'current_links':1}]
    # Actual WSL CLOCK_REALTIME can step backwards, including during unlink.
    # The real observation must stay denied in that case. Use explicit fixture
    # clocks for the separate pure admission tests, never amend a real receipt.
    assert cleanup.shared_link_observation_drift(before,observed)==(expected if observed_ctime>=first_ctime else None)
    after=json.loads(json.dumps(observed))
    after['observation_rows'][0]['signature'][4]=first_ctime+1
    assert cleanup.shared_link_observation_drift(before,after)==[
        {'path':'unique-result.bin','before_links':2,'current_links':1}]
    for position in (0,1,2,3,5):
        changed=json.loads(json.dumps(after));changed['observation_rows'][0]['signature'][position]+=1
        assert cleanup.shared_link_observation_drift(before,changed) is None
    no_link_change=json.loads(json.dumps(after));no_link_change['observation_rows'][0]['signature'][6]=2
    assert cleanup.shared_link_observation_drift(before,no_link_change) is None
    clock_backwards=json.loads(json.dumps(after));clock_backwards['observation_rows'][0]['signature'][4]=first_ctime-1
    assert cleanup.shared_link_observation_drift(before,clock_backwards) is None


def _renumber_observations(cleanup, root, relative):
    current = cleanup.observe(root, relative, service_refs=[], configuration_refs=[], process_refs=[])
    original = json.loads(json.dumps(current))
    for row in original['observation_rows']:
        row['signature'][0] += 1
    return original, current


def test_uniform_device_renumber_preserves_all_other_observations(tmp_path):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, _row, _catalog = example(tmp_path)
    (source / 'empty-directory').mkdir()
    original, current = _renumber_observations(cleanup, root, 'replays/old-result')
    result = cleanup.uniform_device_renumber_drift(original, current)
    assert result['original_device'] == source.stat().st_dev + 1
    assert result['current_device'] == source.stat().st_dev
    assert result['unchanged_other_observation_rows'] == 2
    assert result['source_bytes_verified'] is False
    assert result['full_original_and_cold_verification_required'] is True


@pytest.mark.parametrize('position', [1, 2, 3, 4, 5, 6])
def test_device_renumber_rejects_any_other_stat_change(tmp_path, position):
    from scripts import retire_local_offline_artifacts as cleanup
    root, _source, _row, _catalog = example(tmp_path)
    original, current = _renumber_observations(cleanup, root, 'replays/old-result')
    current['observation_rows'][0]['signature'][position] += 1
    assert cleanup.uniform_device_renumber_drift(original, current) is None


@pytest.mark.parametrize('defect', ['partial-device', 'different-devices', 'missing-row',
                                  'path', 'kind', 'cross-filesystem', 'old-inventory'])
def test_device_renumber_requires_one_complete_unchanged_tree(tmp_path, defect):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, _row, _catalog = example(tmp_path)
    (source / 'directory').mkdir()
    original, current = _renumber_observations(cleanup, root, 'replays/old-result')
    row = current['observation_rows'][0]
    if defect == 'partial-device':
        row['signature'][0] = original['observation_rows'][0]['signature'][0]
    elif defect == 'different-devices':
        row['signature'][0] += 2
    elif defect == 'missing-row':
        current['observation_rows'].pop()
    elif defect == 'path':
        row['path'] = 'other'
    elif defect == 'kind':
        row['kind'] = 'unsupported'
    elif defect == 'cross-filesystem':
        row['cross_filesystem'] = True
    else:
        original.pop('observation_rows')
    assert cleanup.uniform_device_renumber_drift(original, current) is None


@pytest.mark.parametrize('corrupt', [False, True])
def test_renumbered_source_reuses_original_capture_only_with_complete_sha(tmp_path, monkeypatch, corrupt):
    import stockagent.data_sync.legacy_artifact_archive as canonical
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    signature = canonical._source_signature
    with monkeypatch.context() as capture:
        capture.setattr(canonical, '_source_signature',
                        lambda info: {**signature(info), 'device': info.st_dev + 1})
        prepare_archive(spec, root, manual_capture=True)
    manifest_path = spec.stage_root / spec.dataset / 'archive/legacy_archive_manifest.json'
    original_manifest = manifest_path.read_bytes()
    if corrupt:
        path = source / 'unique-result.bin'
        info = path.stat()
        path.write_bytes(b'X' * info.st_size)
        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
    monkeypatch.setattr(canonical, 'prepare_archive',
                        lambda *a, **kw: pytest.fail('retain the original complete capture'))
    if corrupt:
        with pytest.raises(SnapshotError, match='source differs'):
            publish_archive(spec, root, tmp_path / 'cold', repo_root=root.parent, manual_capture=True)
        assert not (tmp_path / 'cold/heads').exists()
    else:
        published = publish_archive(spec, root, tmp_path / 'cold', repo_root=root.parent, manual_capture=True)
        recovered = verify_cold_archive(spec, tmp_path / 'cold')
        assert recovered['snapshot_id'] == published['snapshot_id'] and recovered['decoded_originals_verified']
    assert manifest_path.read_bytes() == original_manifest


@pytest.mark.parametrize('scenario,expected_timeouts', [
    ('idle-only', [0]), ('healthy', [0]), ('peer-down', [0, 300]),
    ('local-error', [0, 300]), ('normal-path', [300]),
])
def test_quarantine_options_overlap_only_healthy_scan(tmp_path, monkeypatch, scenario, expected_timeouts):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup

    checks = {key: True for key in ('folder_idle', 'need_bytes_zero', 'need_items_zero',
              'need_deletes_zero', 'errors_zero', 'pull_errors_zero', 'watch_error_empty',
              'folder_errors_empty', 'system_errors_empty')}
    checks['folder_idle'] = scenario in ('healthy', 'normal-path')
    if scenario == 'local-error':
        checks['errors_zero'] = False
    peer = {'ok': scenario != 'peer-down'}
    initial = {'ok': scenario in ('healthy', 'normal-path'), 'local_checks': checks,
               'peers': [peer], 'checked_at': datetime.now(timezone.utc).isoformat()}
    ready = {'ok': True, 'local_checks': {**checks, 'folder_idle': True, 'errors_zero': True},
             'peers': [{'ok': True}], 'checked_at': initial['checked_at']}
    calls = []
    def wait(_cfg, timeout):
        calls.append(timeout)
        return initial if timeout == 0 else ready
    cfg = SimpleNamespace(sync_root=tmp_path/'cold', materialized_root=tmp_path/'materialized',
                          backup_config=tmp_path/'backup.json')
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(cleanup, '_syncthing', lambda _cfg: ready)
    monkeypatch.setattr(cleanup, '_bridge_inactive', lambda _path: True)
    options = cleanup.retirement_options(cfg, tmp_path/'src', tmp_path/'hot', tmp_path/'state',
        tmp_path/'activations', allow_audit_during_idle_scan=scenario != 'normal-path')
    assert calls == expected_timeouts
    assert options['peer_proof'] == (initial if calls == [0] else ready)
    assert options['peer_probe']() == ready


@pytest.mark.parametrize('outcome', ['expired-before', 'expired-during', 'changed-and-expired', 'consumer'])
def test_quarantine_expiry_requests_full_reaudit_only_before_unlink(tmp_path, monkeypatch, outcome):
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    from test_legacy_artifact_retirement import _fixture

    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    with pytest.raises(retirement.LegacyQuarantineAuditRequired):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
            owned_verified_plan=plan, defer_owned_post_rename_audit=True, **options)
    resumed = retirement.plan_legacy_quarantine_resume(spec, plan, **options)
    state_path = retirement._state_path(options['state_root'], spec.dataset)
    state_bytes = state_path.read_bytes()
    quarantine = Path(json.loads(state_bytes)['quarantine'])
    original = (quarantine/'source/checkpoint.pt').read_bytes()
    verified_at = resumed['full_verification']['verified_at_epoch']
    clock = {'now': verified_at}
    monkeypatch.setattr(retirement.time, 'time', lambda: clock['now'])
    if outcome == 'expired-before':
        clock['now'] += 1501
        with pytest.raises(retirement.LegacyQuarantineProofExpired):
            retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                owned_verified_plan=resumed, defer_quarantine_transport=True, **options)
    else:
        target = tmp_path/'receipt'
        target.mkdir()
        monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path/'common.lock')
        monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *a: {'ok': True})
        real_gate = retirement._assert_legacy_unlink_gates
        def gate(*args, **kwargs):
            if outcome == 'consumer':
                raise SnapshotError('new process reference; quarantine retained')
            peer = real_gate(*args, **kwargs)
            if outcome == 'changed-and-expired':
                (quarantine/'source/checkpoint.pt').write_bytes(b'changed')
            clock['now'] += 1501
            return peer
        monkeypatch.setattr(retirement, '_assert_legacy_unlink_gates', gate)
        if outcome == 'expired-during':
            assert cleanup.retire_after_convergence(spec, None, target, resumed, options) is None
            receipts = list(target.glob('quarantine-audit-expired-*.json'))
            assert len(receipts) == 1
            receipt = json.loads(receipts[0].read_bytes())
            assert receipt['proof_time_extended'] is False and receipt['unlink_started'] is False
            assert receipt['original_verified_at_epoch'] == verified_at
        else:
            with pytest.raises(SnapshotError) as caught:
                cleanup.retire_after_convergence(spec, None, target, resumed, options)
            assert not isinstance(caught.value, retirement.LegacyQuarantineProofExpired)
            assert not list(target.glob('quarantine-audit-expired-*.json'))
    assert resumed['full_verification']['verified_at_epoch'] == verified_at
    assert state_path.read_bytes() == state_bytes
    assert (quarantine/'source/checkpoint.pt').exists() and (quarantine/'hot').is_dir()
    if outcome != 'changed-and-expired':
        assert (quarantine/'source/checkpoint.pt').read_bytes() == original


def test_quarantine_audit_overlaps_scan_but_unlink_requires_convergence(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.artifact_retirement as common
    import stockagent.data_sync.legacy_artifact_archive as archive
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    from test_legacy_artifact_retirement import _fixture

    # This test's fixture lives only in tmpfs; production consumer checks are
    # independently covered by the existing pin/process/FD gate regressions.
    monkeypatch.setattr(archive, 'artifact_process_references', lambda *args: [])
    monkeypatch.setattr(common, 'artifact_process_references', lambda *args: [])
    monkeypatch.setattr(common, 'process_references', lambda *args: [])
    spec, source, hot, old_options = _fixture(tmp_path, monkeypatch)
    old_options['manual_immediate'] = True
    original = (source/'checkpoint.pt').read_bytes()
    plan = retirement.plan_legacy_retirement(spec, **old_options)
    original_gate = retirement._assert_unlink_gates
    monkeypatch.setattr(retirement, '_assert_unlink_gates', lambda *a, **kw:
                        (_ for _ in ()).throw(SnapshotError('controlled interruption')))
    with pytest.raises(SnapshotError, match='controlled interruption'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                          owned_verified_plan=plan, **old_options)
    monkeypatch.setattr(retirement, '_assert_unlink_gates', original_gate)
    state_path = retirement._state_path(old_options['state_root'], spec.dataset)
    quarantine = Path(json.loads(state_path.read_bytes())['quarantine'])
    checks = {key: True for key in ('need_bytes_zero', 'need_items_zero', 'need_deletes_zero',
              'errors_zero', 'pull_errors_zero', 'watch_error_empty', 'folder_errors_empty',
              'system_errors_empty')}
    scanning = {'ok': False, 'checked_at': datetime.now(timezone.utc).isoformat(),
                'local_checks': {**checks, 'folder_idle': False},
                'peers': [{'name': 'vastai1T', 'ok': True}]}
    healthy = {**scanning, 'ok': True, 'local_checks': {**checks, 'folder_idle': True}}
    current = {'proof': scanning}
    cfg = SimpleNamespace(sync_root=old_options['sync_root'],
        materialized_root=old_options['materialized_root'], backup_config=old_options['backup_config'])
    timeouts = []
    def wait(_cfg, timeout):
        timeouts.append(timeout)
        return current['proof']
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(cleanup, '_syncthing', lambda _cfg: current['proof'])
    monkeypatch.setattr(cleanup, '_bridge_inactive', lambda _path: True)
    options = cleanup.retirement_options(cfg, old_options['artifact_root'], old_options['hot_root'],
        old_options['state_root'], old_options['activation_root'], allow_audit_during_idle_scan=True)
    options.update(repo_root=old_options['repo_root'], manual_capture=False)
    assert timeouts == [0]
    resumed = retirement.plan_legacy_quarantine_resume(spec, plan, **options)
    assert 'cold-peer-not-converged' in resumed['blockers']
    with pytest.raises(retirement.LegacyQuarantineTransportPending):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
            owned_verified_plan=resumed, defer_quarantine_transport=True, **options)
    assert (quarantine/'source/checkpoint.pt').read_bytes() == original
    assert json.loads(state_path.read_text())['state'] == 'retiring'
    current['proof'] = healthy
    result = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
        owned_verified_plan=resumed, defer_quarantine_transport=True, **options)
    assert result['deleted'] and result['resumed_quarantine']
    assert not source.exists() and not hot.exists() and not quarantine.exists()


@pytest.mark.parametrize('problem', [None, 'wait-failed', 'transport-changed',
                                   'expired-before', 'expired-during', 'consumer', 'source'])
def test_scan_wait_keeps_original_full_audit_and_all_final_gates(tmp_path, monkeypatch, problem):
    from datetime import datetime, timezone
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    from test_legacy_artifact_retirement import _fixture
    import fcntl
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    current = {'ok': False, 'checked_at': datetime.now(timezone.utc).isoformat()}
    options['peer_probe'] = lambda: dict(current)
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan['blockers'] == ['cold-peer-not-converged']
    verified_at = plan['full_verification']['verified_at_epoch']
    if problem == 'expired-before':
        plan['full_verification']['verified_at_epoch'] = time.time() - 1501
    if problem == 'consumer':
        plan['blockers'].append('enabled-service-references-artifact')
    target = tmp_path/'receipt';target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path/'common.lock')
    waits = []
    def wait(cfg, seconds):
        with cleanup.COMMON_OWNER.open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert 0 <= seconds <= 60
        waits.append(True)
        if problem in ('expired-during', 'wait-failed'):
            monkeypatch.setattr(cleanup.time, 'time', lambda: verified_at + 1501)
        if problem == 'source':
            file = source/'checkpoint.pt';info=file.stat()
            file.write_bytes(b'X'*info.st_size)
            os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
        current['ok'] = problem not in ('wait-failed', 'transport-changed')
        returned = dict(current)
        if problem == 'transport-changed':
            returned['ok'] = True
        return returned
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(retirement, 'verify_packed_snapshot',
                        lambda *a, **kw: pytest.fail('full audit repeated while holding mutation owner'))
    if problem in ('transport-changed', 'consumer', 'source'):
        with pytest.raises(SnapshotError):
            cleanup.retire_after_convergence(spec, None, target, plan, options)
        assert source.exists() and hot.exists()
    else:
        result = cleanup.retire_after_convergence(spec, None, target, plan, options)
        if problem:
            assert result is None and source.exists() and hot.exists()
        else:
            assert result['deleted'] is True and not source.exists() and not hot.exists()
            assert plan['full_verification']['verified_at_epoch'] == verified_at
    assert bool(waits) is (problem not in ('consumer', 'expired-before'))


@pytest.mark.parametrize('outcome', ['cleared', 'persistent', 'changed-source', 'other-blocker'])
def test_transient_reader_wait_rechecks_full_owned_retirement_gates(tmp_path, monkeypatch, outcome):
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    from test_legacy_artifact_retirement import _fixture

    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    plan['blockers'].append('artifact-has-process-references')
    if outcome == 'other-blocker':
        plan['blockers'].append('artifact-release-is-pinned')
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *args: {'ok': True})
    waited = []

    def wait(spec_arg, options_arg):
        with cleanup.COMMON_OWNER.open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert spec_arg is spec and options_arg is options
        waited.append(True)
        if outcome == 'changed-source':
            (source / 'checkpoint.pt').write_bytes(b'changed while the reader was leaving')
        return outcome != 'persistent'

    monkeypatch.setattr(cleanup, '_wait_for_artifact_process_references', wait)
    if outcome == 'cleared':
        result = cleanup.retire_after_convergence(spec, None, target, plan, options)
        assert result['deleted'] and not source.exists() and not hot.exists()
    else:
        with pytest.raises(SnapshotError):
            cleanup.retire_after_convergence(spec, None, target, plan, options)
        assert source.exists() and hot.exists()
    assert bool(waited) is (outcome != 'other-blocker')
    if waited:
        receipt = json.loads((target / 'retirement-process-wait.json').read_bytes())
        assert receipt['references_cleared'] is (outcome != 'persistent')
        assert receipt['proof_time_extended'] is False


def test_process_reference_wait_checks_source_and_hot_and_is_bounded(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    root = tmp_path / 'artifacts'
    source = root / 'markets/example'
    source.mkdir(parents=True)
    hot_root = tmp_path / 'hot'
    hot = hot_root / 'markets/example'
    hot.mkdir(parents=True)
    clock = {'value': 0.0}
    monkeypatch.setattr(cleanup.time, 'monotonic', lambda: clock['value'])
    monkeypatch.setattr(cleanup.time, 'sleep', lambda seconds: clock.update(value=clock['value'] + seconds))
    monkeypatch.setattr(cleanup, 'artifact_process_references', lambda src, scope: [])
    monkeypatch.setattr(cleanup, 'process_references', lambda path: ['pid=7:fd'] if path == hot else [])
    spec = SimpleNamespace(relative_root='markets/example', capture_contract=None)
    options = {'artifact_root': root, 'hot_root': hot_root}
    assert not cleanup._wait_for_artifact_process_references(spec, options, seconds=5)
    assert clock['value'] == 5
    monkeypatch.setattr(cleanup, 'process_references', lambda path: [])
    assert cleanup._wait_for_artifact_process_references(spec, options, seconds=5)


@pytest.mark.parametrize('combined', [False, True])
def test_reader_appearing_in_final_preflight_retries_only_before_mutation(
        tmp_path, monkeypatch, combined):
    from types import SimpleNamespace
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    verified_at = time.time()
    plan = {'blockers': [], 'plan_fingerprint': 'a' * 64,
            'full_verification': {'verified_at_epoch': verified_at}}
    monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *args: {'ok': True})
    applies, waits = [], []

    def apply(spec, **options):
        applies.append(True)
        if len(applies) == 1:
            suffix = ', artifact-release-is-pinned' if combined else ''
            raise SnapshotError('legacy retirement blocked: artifact-has-process-references' + suffix)
        return {'deleted': True}

    def wait(spec, options):
        with cleanup.COMMON_OWNER.open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        waits.append(True)
        return True

    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', apply)
    monkeypatch.setattr(cleanup, '_wait_for_artifact_process_references', wait)
    call = lambda: cleanup.retire_after_convergence(
        SimpleNamespace(relative_root='markets/example'), None, target, plan, {})
    if combined:
        with pytest.raises(SnapshotError, match='artifact-release-is-pinned'):
            call()
        assert len(applies) == 1 and not waits
    else:
        assert call() == {'deleted': True}
        assert len(applies) == 2 and waits == [True]
        journal = list(target.glob('transport-preflight-retry-*.json'))
        assert len(journal) == 1
        assert json.loads(journal[0].read_bytes())['state'] == 'retry_process_preflight_without_mutation'


@pytest.mark.parametrize('outcome', ['success', 'exhausted', 'expired', 'unlink', 'combined', 'source'])
def test_transport_preflight_retry_is_bounded_and_never_replays_mutation(tmp_path, monkeypatch, outcome):
    from types import SimpleNamespace
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    marker = tmp_path / 'untouched-source'
    marker.write_bytes(b'original unique observation')
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    verified_at = time.time()
    clock = {'now': verified_at}
    monkeypatch.setattr(cleanup.time, 'time', lambda: clock['now'])
    spec = SimpleNamespace(relative_root='replays/example')
    plan = {'blockers': [], 'plan_fingerprint': 'a' * 64,
            'full_verification': {'verified_at_epoch': verified_at}}
    waits, applies = [], []
    def wait(cfg, seconds):
        assert 0 <= seconds <= 60
        with cleanup.COMMON_OWNER.open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        waits.append(seconds)
        return {'ok': True}
    def apply(value, **options):
        assert value is spec and options['owned_verified_plan'] is plan
        assert options['expected_fingerprint'] == plan['plan_fingerprint']
        assert options['manual_immediate'] is True
        with cleanup.COMMON_OWNER.open('a') as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        applies.append(True)
        if outcome == 'success' and len(applies) == 2:
            return {'deleted': True}
        if outcome == 'expired':
            clock['now'] = verified_at + 1501
        message = {
            'unlink': 'legacy retirement unlink blocked: cold-peer-not-converged',
            'combined': 'legacy retirement blocked: cold-peer-not-converged, artifact-release-is-pinned',
            'source': 'legacy retirement plan changed; rerun dry run',
        }.get(outcome, 'legacy retirement blocked: cold-peer-not-converged')
        raise SnapshotError(message)
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', apply)
    call = lambda: cleanup.retire_after_convergence(spec, None, target, plan, {'manual_immediate': True})
    if outcome == 'success':
        assert call() == {'deleted': True}
    elif outcome == 'expired':
        assert call() is None
    else:
        with pytest.raises(SnapshotError):
            call()
    expected = 2 if outcome == 'success' else 3 if outcome == 'exhausted' else 1
    assert len(waits) == len(applies) == expected
    journal = [json.loads(path.read_bytes()) for path in target.glob('transport-preflight-retry-*.json')]
    assert len(journal) == (1 if outcome in ('success', 'expired') else 2 if outcome == 'exhausted' else 0)
    assert all(row['proof_time_extended'] is False and row['original_verified_at_epoch'] == verified_at
               for row in journal)
    assert plan['full_verification']['verified_at_epoch'] == verified_at
    assert marker.read_bytes() == b'original unique observation'


@pytest.mark.parametrize('scan_seconds', [360, 900])
def test_long_healthy_scan_waits_without_repeating_or_extending_full_audit(tmp_path, monkeypatch, scan_seconds):
    from types import SimpleNamespace
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    target = tmp_path / 'receipt'; target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'owner.lock')
    verified_at = time.time()
    clock = {'now': verified_at}
    monkeypatch.setattr(cleanup.time, 'time', lambda: clock['now'])
    plan = {'blockers': ['cold-peer-not-converged'], 'plan_fingerprint': 'b' * 64,
            'full_verification': {'verified_at_epoch': verified_at}}
    waits, applies = [], []
    def wait(cfg, seconds):
        assert 0 < seconds <= 60
        with cleanup.COMMON_OWNER.open('a') as other_owner:
            fcntl.flock(other_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        clock['now'] += seconds
        waits.append(seconds)
        return {'ok': clock['now'] - verified_at >= scan_seconds}
    def apply(spec, **options):
        with cleanup.COMMON_OWNER.open('a') as other_owner:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert options['owned_verified_plan'] is plan
        assert plan['full_verification']['verified_at_epoch'] == verified_at
        applies.append(True)
        return {'deleted': True}
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', apply)
    monkeypatch.setattr(cleanup, 'plan_legacy_retirement',
                        lambda *a, **kw: pytest.fail('fresh full audit repeated for a healthy scan'))
    assert cleanup.retire_after_convergence(SimpleNamespace(relative_root='replays/example'), None,
                                           target, plan, {}) == {'deleted': True}
    assert sum(waits) == scan_seconds and applies == [True]
    saved = json.loads((target / 'retirement-peer-wait.json').read_bytes())
    assert saved['original_verified_at_epoch'] == verified_at and saved['proof_time_extended'] is False


def test_batched_active_configuration_keeps_independent_jobs_and_unreadable_configs_protected(tmp_path):
    import subprocess,sys
    from stockagent.data_sync.remote_legacy_return import active_configuration_references_many
    roots=[tmp_path/'artifacts/replays/first',tmp_path/'artifacts/replays/second']
    for root in roots:root.mkdir(parents=True)
    config=tmp_path/'independent.yaml'
    config.write_text('checkpoint: artifacts/replays/first/model.pt\n')
    process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)','--config',str(config)],cwd=tmp_path)
    try:
        result=active_configuration_references_many(roots,tmp_path)
        label='pid='+str(process.pid)+':active-config:'
        assert any(r.startswith(label) for r in result[str(roots[0])])
        assert not any(r.startswith(label) for r in result[str(roots[1])])
        config.write_text('checkpoint: [broken\n')
        result=active_configuration_references_many(roots,tmp_path)
        assert all(any(r.startswith('pid='+str(process.pid)+':active-config-unreadable:')
                       for r in result[str(root)]) for root in roots)
    finally:
        process.terminate();process.wait(timeout=5)


@pytest.mark.parametrize('corrupt',[False,True])
def test_complete_archive_publication_keeps_sha_gates_after_shared_link_removal(tmp_path,monkeypatch,corrupt):
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    neighbor=tmp_path/'other-hardlink';os.link(source/'unique-result.bin',neighbor)
    prepare_archive(spec,root,manual_capture=True)
    manifest_path=spec.stage_root/spec.dataset/'archive/legacy_archive_manifest.json'
    preserved=manifest_path.read_bytes()
    neighbor.unlink()
    if corrupt:
        file=source/'unique-result.bin';info=file.stat();file.write_bytes(b'X'*info.st_size)
        os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    import stockagent.data_sync.legacy_artifact_archive as canonical
    monkeypatch.setattr(canonical,'prepare_archive',lambda *a,**kw:pytest.fail('complete stage must keep its provenance'))
    if corrupt:
        with pytest.raises(SnapshotError,match='source differs'):
            publish_archive(spec,root,tmp_path/'cold',repo_root=root.parent,manual_capture=True)
        assert not (tmp_path/'cold/heads').exists()
    else:
        result=publish_archive(spec,root,tmp_path/'cold',repo_root=root.parent,manual_capture=True)
        assert verify_cold_archive(spec,tmp_path/'cold')['snapshot_id']==result['snapshot_id']
    assert manifest_path.read_bytes()==preserved


def test_nested_phase_progress_restores_the_running_parent_clock(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    path = tmp_path / 'phase.json'
    with phase(tmp_path, 'publish-to-D'):
        parent = json.loads(path.read_text())
        with phase(tmp_path, 'publish-under-shared-owner'):
            child = json.loads(path.read_text())
            assert child['phase_stack'] == ['publish-to-D', 'publish-under-shared-owner']
            assert child['phase_depth'] == 1 and child['state'] == 'running'
        resumed = json.loads(path.read_text())
        assert resumed['phase'] == 'publish-to-D' and resumed['state'] == 'running'
        assert resumed['started_at_epoch'] == parent['started_at_epoch']
        assert resumed['last_subphase']['state'] == 'completed'
        assert 'finished_at_epoch' not in resumed
    completed = json.loads(path.read_text())
    assert completed['contract'] == 'manual-retirement-phase-progress-v2'
    assert completed['phase'] == 'publish-to-D' and completed['state'] == 'completed'


@pytest.mark.parametrize('handled', [False, True])
def test_nested_phase_failure_keeps_parent_active_until_its_own_outcome(tmp_path, handled):
    from scripts.retire_local_offline_artifacts import phase
    path = tmp_path / 'phase.json'
    def run():
        with phase(tmp_path, 'outer-audit'):
            try:
                with phase(tmp_path, 'child-audit'):
                    raise ValueError('failed child')
            except ValueError:
                current = json.loads(path.read_text())
                assert current['phase'] == 'outer-audit' and current['state'] == 'running'
                assert current['last_subphase']['state'] == 'failed'
                assert current['last_subphase']['error_type'] == 'ValueError'
                if not handled:
                    raise
    if handled:
        run()
    else:
        with pytest.raises(ValueError, match='failed child'):
            run()
    result = json.loads(path.read_text())
    assert result['phase'] == 'outer-audit'
    assert result['state'] == ('completed' if handled else 'failed')


def test_nested_phase_targets_never_overwrite_each_others_progress(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    left, right = tmp_path / 'left', tmp_path / 'right'
    left.mkdir(); right.mkdir()
    with phase(left, 'left-audit'):
        with phase(right, 'right-audit'):
            assert json.loads((right / 'phase.json').read_text())['phase_stack'] == ['right-audit']
        assert json.loads((right / 'phase.json').read_text())['state'] == 'completed'
        assert json.loads((left / 'phase.json').read_text())['state'] == 'running'
    assert json.loads((right / 'phase.json').read_text())['phase'] == 'right-audit'


def test_phase_progress_does_not_resurrect_an_interrupted_disk_phase(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    path = tmp_path / 'phase.json'
    path.write_text(json.dumps({'phase': 'interrupted-old-attempt', 'state': 'running'}))
    with phase(tmp_path, 'new-attempt'):
        assert json.loads(path.read_text())['phase_stack'] == ['new-attempt']
    result = json.loads(path.read_text())
    assert result['phase'] == 'new-attempt' and result['state'] == 'completed'
    assert result['phase_depth'] == 0


def test_phase_history_retains_nested_completed_and_failed_timing_without_double_counting(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    with pytest.raises(ValueError):
        with phase(tmp_path, 'parent'):
            with phase(tmp_path, 'child'):
                pass
            with phase(tmp_path, 'bad-child'):
                raise ValueError('injected failure')
    history = json.loads((tmp_path / 'phase-history.json').read_bytes())
    assert history['contract'] == 'manual-retirement-phase-history-v1'
    assert [(e['phase'], e['state'], e['phase_depth']) for e in history['events']] == [
        ('child', 'completed', 1), ('bad-child', 'failed', 1), ('parent', 'failed', 0)]
    assert all(e['elapsed_seconds'] >= 0 and e['owner_pid'] == os.getpid() for e in history['events'])


def test_fixed_d_only_requires_exact_existing_stage_and_pinned_publication(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup

    stage = tmp_path / 'enrolled-d-stage'
    archive = stage / 'dataset' / 'archive'
    archive.mkdir(parents=True)
    manifest = archive / 'legacy_archive_manifest.json'
    manifest.write_text('{}')
    spec = SimpleNamespace(stage_root=stage, dataset='dataset')
    target = tmp_path / 'receipt'
    target.mkdir()
    published = {'dataset': 'dataset', 'snapshot_id': 'fixed-release', 'manifest_sha256': 'a' * 64}
    monkeypatch.setattr(cleanup, 'D_BENCHMARK_STAGE_ROOT', stage)
    monkeypatch.setattr(cleanup, '_check_d_primary_mount', lambda path: None)
    monkeypatch.setattr(cleanup, 'reusable_publication', lambda *a: published)
    assert cleanup.require_existing_fixed_d_stage(spec, tmp_path / 'cold', target,
        expected_fixed_manifest_sha256='a' * 64) == published
    with pytest.raises(SnapshotError, match='changed since'):
        cleanup.require_existing_fixed_d_stage(spec, tmp_path / 'cold', target,
            expected_fixed_manifest_sha256='b' * 64)
    manifest.unlink()
    monkeypatch.setattr(cleanup, 'prepare_archive', lambda *a, **kw: pytest.fail('must not create a replacement'))
    with pytest.raises(SnapshotError, match='existing exact D archive'):
        cleanup.prepare_or_resume_publication(spec, tmp_path, tmp_path / 'cold', target,
            repo_root=tmp_path, fixed_d_stage_only=True, expected_fixed_manifest_sha256='a' * 64)
    assert not manifest.exists()


def test_fixed_d_only_never_falls_back_to_new_workspace_if_stage_disappears(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup

    spec = SimpleNamespace(dataset='dataset', stage_root=tmp_path / 'stage')
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'require_existing_fixed_d_stage', lambda *a, **kw: {})
    monkeypatch.setattr(cleanup, 'resolve_latest_packed', lambda *a: SimpleNamespace(
        manifest={'source': {'logical_bytes': 100}, 'snapshot_id': 'fixed'}, manifest_sha256='a' * 64))
    observed = []
    def fail_without_fallback(*args, **kwargs):
        observed.append(kwargs)
        raise SnapshotError('stage vanished')
    monkeypatch.setattr(cleanup, 'verify_cold_archive', fail_without_fallback)
    with pytest.raises(SnapshotError, match='stage vanished'):
        cleanup.verify_for_retirement(spec, tmp_path / 'cold', target,
            fixed_d_stage_only=True, expected_fixed_manifest_sha256='a' * 64)
    assert observed[0]['reuse_encoded_archive'] == spec.stage_root / spec.dataset / 'archive'
    assert 'verification_root' not in observed[0]
    assert json.loads((target / 'recovery-progress.json').read_text())['state'] == 'failed'


def test_fixed_d_only_rejects_release_change_after_preflight(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup

    spec = SimpleNamespace(dataset='dataset', stage_root=tmp_path / 'stage')
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'require_existing_fixed_d_stage', lambda *a, **kw: {})
    monkeypatch.setattr(cleanup, 'resolve_latest_packed', lambda *a: SimpleNamespace(
        manifest={'source': {'logical_bytes': 100}, 'snapshot_id': 'other'}, manifest_sha256='b' * 64))
    monkeypatch.setattr(cleanup, 'verify_cold_archive', lambda *a, **kw: pytest.fail('must not read changed release'))
    with pytest.raises(SnapshotError, match='changed before independent recovery'):
        cleanup.verify_for_retirement(spec, tmp_path / 'cold', target,
            fixed_d_stage_only=True, expected_fixed_manifest_sha256='a' * 64)


@pytest.mark.parametrize('damage', [None, 'missing-encoding', 'corrupt-cold'])
def test_new_guarded_d_recovery_retains_complete_cold_decode_proof_without_local_tree(
        tmp_path, monkeypatch, damage):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    from stockagent.data_sync.packed_snapshots import resolve_latest_packed
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    cold = tmp_path / 'packed'
    published = publish_archive(spec, root, cold, repo_root=root.parent, manual_capture=True)
    archive = spec.stage_root / spec.dataset / 'archive'
    monkeypatch.setattr(cleanup, 'D_BENCHMARK_STAGE_ROOT', spec.stage_root)
    monkeypatch.setattr(cleanup, '_check_d_primary_mount', lambda path: None)
    monkeypatch.setattr(cleanup.shutil, 'disk_usage', lambda path: SimpleNamespace(free=0))
    monkeypatch.setattr(cleanup.tempfile, 'mkdtemp',
                        lambda *a, **kw: pytest.fail('D recovery must not allocate a C restore tree'))
    target = tmp_path / 'receipts'
    target.mkdir()
    if damage == 'missing-encoding':
        (archive / 'legacy_archive_manifest.json').unlink()
    if damage == 'corrupt-cold':
        resolved = resolve_latest_packed(cold, spec.dataset)
        member = cold / resolved.manifest['archive']['objects'][0]['relpath']
        raw = member.read_bytes()
        member.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    if damage:
        with pytest.raises(SnapshotError):
            cleanup.verify_for_retirement(spec, cold, target, d_stage_recovery_only=True)
    else:
        proof = cleanup.verify_for_retirement(spec, cold, target, d_stage_recovery_only=True)
        assert proof['cold_verified'] and proof['decoded_originals_verified']
        assert proof['manifest_sha256'] == published['manifest_sha256']
        assert proof['verification_scratch_created'] is False
    assert source.is_dir() and not (target / 'retirement.json').exists()
