"""Portable public receipts exercise the canonical importer without private inputs."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from scripts.audit_data_credentials import _presence_row

REPO = Path(os.environ.get('R135_REPOSITORY_ROOT', str(Path(__file__).resolve().parents[1])))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False))


@pytest.fixture
def profile():
    from scripts import profile_workflow_speeds
    return profile_workflow_speeds


@pytest.fixture
def bundle(tmp_path, monkeypatch, profile):
    root, directory = tmp_path / 'logical', tmp_path / 'audit'
    directory.mkdir()
    monkeypatch.setattr(profile, 'ROOT', root)
    actual_read = Path.read_bytes
    private_root = (REPO / 'artifacts/benchmarks').resolve()
    fixture_root = tmp_path.resolve()

    def public_or_fixture_read(path):
        resolved = path.resolve()
        assert (not resolved.is_relative_to(private_root)
                or resolved.is_relative_to(fixture_root)), 'private_benchmark_source_read'
        return actual_read(path)

    monkeypatch.setattr(Path, 'read_bytes', public_or_fixture_read)
    roles = ['scripts/audit_data_credentials.py', 'downloader/operation_timing.py', 'configs/data_api_credentials.json',
             'artifacts/benchmarks/credential_fixture/observer.py']
    sources = []
    for i, name in enumerate(roles):
        original = root / name
        original.parent.mkdir(parents=True, exist_ok=True)
        body = (b'"""Synthetic public observer identity; never executed."""\n'
                if i == 3 else (REPO / name).read_bytes())
        original.write_bytes(body)
        archive = directory / (str(i) + '-' + original.name)
        archive.write_bytes(original.read_bytes())
        sources.append(dict(path=str(original), archive=str(archive), sha256=sha(archive)))
    providers = json.loads((root / roles[2]).read_bytes())['providers']
    assert len(providers) == 45
    public_providers = [_presence_row(row, configured=lambda _: False, source=row['location']) for row in providers]
    baseline = dict(generated_at_utc='2026-10-24T00:00:00Z', secret_values_included=False,
        validation_scope='local_presence_only; no_authentication_or_entitlement_check',
        providers=public_providers)
    status = json.loads(json.dumps(baseline))
    status['generated_at_utc'] = '2026-10-24T00:00:01Z'
    stages = [dict(stage='credential_presence_predicate', provider='credential:' + row['id'],
                   seconds=(1001 + i) / 1e9, process_cpu_nanoseconds=501 + i, outcome='returned')
              for i, row in enumerate(public_providers)]
    stages += [dict(stage=name, seconds=seconds, process_cpu_nanoseconds=cpu, outcome='returned', **extra)
        for name, seconds, cpu, extra in [
            ('registry_validation', .0001, 10000, {}),
            ('finlab_local_session_probe', .002, 100000, {}),
            ('finlab_presence_resolution', .003, 110000, dict(provider='credential:finlab'))]]
    timing = dict(contract='source_operation_monotonic_v1', clock='perf_counter_ns',
        scope='after_complete_canonical_status_publication', seconds=.5, invalid_clock=False, stages=stages,
        diagnostic_instrumentation_adds_overhead=True, nested_and_inclusive_ranges_overlap=True,
        process_cpu_excludes_isolated_sdk_subprocess=True, authentication_or_entitlement_verified=False,
        external_acquisition_measured=False, aggregation='inclusive stages overlap total; do not sum')
    protocol = dict(state='prepared_canonical_local_credential_audit_observation',
        expected_declared_providers=45, driver_sha256=sources[3]['sha256'], all_45_original_presence_predicates_required=True,
        baseline_and_instrumented_public_receipt_semantic_parity_required=True,
        no_secret_files_or_secret_values_archived=True, diagnostic_instrumentation_adds_overhead=True,
        instrumented_clocks_not_speed_comparison=True, production_service_environment_or_loaded_identity_verified=False,
        source_registry_stage_bindings_recomputed=False, sources=sources,
        original_audit_source_sha256=sources[0]['sha256'], original_timing_helper_sha256=sources[1]['sha256'],
        environment_scope='benchmark inherited process environment plus canonical literal-presence file reader',
        new_provider_calls=0, new_gpu_jobs=0, all_workflows_measured=False, hardware_limit_proven=False)
    receipts = [dict(state='completed_canonical_local_presence_audit', mode=mode,
        original_audit_source_sha256=sources[0]['sha256'], original_timing_helper_sha256=sources[1]['sha256'],
        status_mode='0o600', original_provider_count=45, no_secret_files_archived=True,
        production_service_loaded_identity_verified=False) for mode in ['baseline', 'instrumented']]
    readback = dict(state='verified_all_45_canonical_presence_predicates_and_receipt_parity',
        complete_public_status_semantic_parity=True, original_presence_predicates=45, all_clock_values_revalidated=True,
        authentication_or_provider_access_verified=False, full_external_acquisition_verified=False,
        source_stage_bindings_recomputed=False, new_provider_calls=0, new_gpu_jobs=0,
        all_workflows_measured=False, hardware_limit_proven=False)
    completion = dict(state='verified_complete_canonical_credential_audit_diagnostic',
        counts=dict(tests=55, failures=0, errors=0, skipped=0), controlled_speedup_verified=False,
        canonical_sources_modified=False, source_stage_bindings_recomputed=False,
        new_provider_calls=0, new_gpu_jobs=0, all_workflows_measured=False, hardware_limit_proven=False)
    data = [timing, protocol, baseline, status, *receipts, readback, completion]
    paths = [directory / name for name in profile.LOCAL_CREDENTIAL_AUDIT_FILES]

    def persist():
        for i in range(4):
            save(paths[i], data[i])
        for i in range(2):
            receipts[i]['status_sha256'] = sha(paths[2 + i])
            save(paths[4 + i], receipts[i])
        readback.update(protocol_sha256=sha(paths[1]), operation_timings_sha256=sha(paths[0]))
        save(paths[6], readback)
        completion['readback_sha256'] = sha(paths[6])
        save(paths[7], completion)

    persist()
    return SimpleNamespace(root=root, directory=directory, data=data, paths=paths,
                           sources=sources, persist=persist, expected={r['id'] for r in public_providers})


def collect(profile, bundle, tmp_path, directories=None):
    output = tmp_path / 'observation'
    output.mkdir()
    evidence = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=8 << 20)
    ledger = profile.Ledger()
    ledger.inventory.extend(dict(kind='source_endpoint', id='credential:' + value) for value in bundle.expected)
    ledger.inventory += [dict(kind='source_endpoint', id='credential:unrelated'),
                         dict(kind='source_endpoint', id='economic:shioaji')]
    count = profile.collect_local_credential_audits(ledger, evidence, directories or [bundle.directory])
    return ledger, evidence, count


def test_complete_public_parity_exact_clocks_and_only_45_gate_bindings(profile, bundle, tmp_path):
    ledger, evidence, count = collect(profile, bundle, tmp_path)
    assert count == len(ledger.samples) == 97 and not evidence.issues
    assert profile.bind_source_stages(ledger) == 45
    assert sum('source_endpoint_id' in row for row in ledger.samples) == 90
    assert all('timed_stages' not in row for row in ledger.inventory[45:47])
    for i, row in enumerate(bundle.data[0]['stages']):
        wall, cpu = ledger.samples[2 * i:2 * i + 2]
        assert wall['seconds'].hex() == row['seconds'].hex()
        assert cpu['seconds'].hex() == (row['process_cpu_nanoseconds'] / 1e9).hex()
        assert cpu['context']['native_nanoseconds'] == row['process_cpu_nanoseconds']
        assert wall['context']['authentication_or_entitlement_verified'] is False
        assert wall['context']['process_cpu_excludes_isolated_sdk_subprocess'] is True
        assert wall['context']['nested_and_inclusive_ranges_overlap'] is True
        assert wall['context']['observer_source_sha256'] == bundle.data[1]['driver_sha256']
        assert wall['context']['registry_source_sha256'] == bundle.sources[2]['sha256']
    assert all('source_endpoint_id' not in row for row in ledger.samples[90:])
    evidence.close_snapshot_store()


def test_same_completed_bundle_is_not_double_counted(profile, bundle, tmp_path):
    ledger, evidence, count = collect(profile, bundle, tmp_path, [bundle.directory, bundle.directory])
    assert count == len(ledger.samples) == 97
    assert sum(row['kind'] == 'local_credential_audit_report' for row in ledger.inventory) == 1
    evidence.close_snapshot_store()


@pytest.mark.parametrize('failure', [
    'missing_catalog', 'duplicate_catalog', 'catalog_as_public', 'duplicate_public',
    'invalid_clock', 'nonmonotonic_clock', 'claimed_auth', 'claimed_env', 'provider_calls',
    'status_parity', 'missing_predicate', 'duplicate_predicate', 'unknown_predicate',
    'bad_wall_negative', 'bad_wall_bool', 'bad_wall_nan', 'bad_cpu_negative', 'bad_cpu_bool',
    'bad_cpu_float', 'bad_cpu_overflow', 'wrong_public_source_archive', 'wrong_readback',
    'wrong_receipt', 'failed_tests',
    'missing_observer', 'changed_observer', 'ambiguous_observer', 'observer_without_SHA',
    'missing_environment_scope', 'wrong_aggregation',
])
def test_invalid_bundle_never_emits_partial_clocks(profile, bundle, tmp_path, failure):
    timing, protocol, baseline, status, base_receipt, receipt, readback, completion = bundle.data
    if failure in {'missing_catalog', 'duplicate_catalog', 'catalog_as_public', 'duplicate_public'}:
        for public in (baseline, status):
            if failure == 'missing_catalog':
                public['providers'][0].pop('catalog_id')
            elif failure == 'duplicate_catalog':
                public['providers'][1]['catalog_id'] = public['providers'][0]['catalog_id']
            elif failure == 'catalog_as_public':
                row = next(row for row in public['providers'] if row['id'].startswith('openbb:'))
                row['id'] = row['catalog_id']
            else:
                public['providers'][1]['id'] = public['providers'][0]['id']
    elif failure == 'invalid_clock': timing['invalid_clock'] = True
    elif failure == 'nonmonotonic_clock': timing['clock'] = 'utc'
    elif failure == 'claimed_auth': timing['authentication_or_entitlement_verified'] = True
    elif failure == 'claimed_env': protocol['production_service_environment_or_loaded_identity_verified'] = True
    elif failure == 'provider_calls': protocol['new_provider_calls'] = 1
    elif failure == 'status_parity': status['providers'][-1]['state'] = 'configured'
    elif failure == 'missing_predicate': timing['stages'].pop(44)
    elif failure == 'duplicate_predicate': timing['stages'][44] = dict(timing['stages'][0])
    elif failure == 'unknown_predicate': timing['stages'][44]['provider'] = 'credential:unknown'
    elif failure == 'bad_wall_negative': timing['stages'][-1]['seconds'] = -1
    elif failure == 'bad_wall_bool': timing['stages'][-1]['seconds'] = True
    elif failure == 'bad_wall_nan': timing['stages'][-1]['seconds'] = float('nan')
    elif failure == 'bad_cpu_negative': timing['stages'][-1]['process_cpu_nanoseconds'] = -1
    elif failure == 'bad_cpu_bool': timing['stages'][-1]['process_cpu_nanoseconds'] = True
    elif failure == 'bad_cpu_float': timing['stages'][-1]['process_cpu_nanoseconds'] = 1.5
    elif failure == 'bad_cpu_overflow': timing['stages'][-1]['process_cpu_nanoseconds'] = 10**400
    elif failure == 'wrong_public_source_archive': Path(bundle.sources[0]['archive']).write_bytes(b'changed public code')
    elif failure == 'wrong_receipt': receipt['original_audit_source_sha256'] = '0' * 64
    elif failure == 'failed_tests': completion['counts']['failures'] = 1
    elif failure == 'missing_observer': protocol['sources'].pop()
    elif failure == 'changed_observer': Path(bundle.sources[3]['archive']).write_bytes(b'changed public observer')
    elif failure == 'ambiguous_observer':
        extra = dict(bundle.sources[3], path=str(bundle.root / 'artifacts/benchmarks/other_observer.py'))
        protocol['sources'].append(extra)
    elif failure == 'observer_without_SHA': protocol.pop('driver_sha256')
    elif failure == 'missing_environment_scope': protocol.pop('environment_scope')
    elif failure == 'wrong_aggregation': timing['aggregation'] = 'sum all stages'
    bundle.persist()
    if failure == 'wrong_readback':
        readback['operation_timings_sha256'] = '0' * 64
        save(bundle.paths[6], readback)
        completion['readback_sha256'] = sha(bundle.paths[6])
        save(bundle.paths[7], completion)
    ledger, evidence, count = collect(profile, bundle, tmp_path)
    assert count == 0 and not ledger.samples and evidence.issues, 'credential_observer_source_missing'
    assert not any(row['kind'] == 'local_credential_audit_report' for row in ledger.inventory)
    assert profile.bind_source_stages(ledger) == 0
    evidence.close_snapshot_store()


@pytest.mark.parametrize('layout', ['individual_gzip', 'packed_gzip_v1'])
def test_full_13_phase_collection_and_future_worktree_changes(profile, bundle, tmp_path, layout):
    root = bundle.root
    subprocess.run(['git', 'init', '--quiet', str(root)], check=True)
    source = root / 'scripts/profile_workflow_speeds.py'
    source.write_text('raise AssertionError("source must not execute")\n')
    monitor = root / 'monitor.json'
    save(monitor, dict(sources=[dict(id='credential:' + value) for value in sorted(bundle.expected)]))
    output = tmp_path / 'complete'
    assert profile.main(['--output-dir', str(output), '--metric-backend', 'python',
        '--monitor-snapshot', str(monitor), '--training-root', str(root / 'empty'),
        '--local-credential-audit-report', str(bundle.directory), '--evidence-storage-layout', layout,
        '--max-file-mib', '1', '--max-total-mib', '8']) == 0
    report = json.loads((output / 'collection.json').read_bytes())
    assert report['local_credential_audit_ingestion_contract'] == 'observed_local_credential_audit_v1'
    inventory = json.loads((output / 'inventory.json').read_bytes())['rows']
    sources = [row for row in inventory if row['kind'] == 'source_endpoint']
    assert len(sources) == 45 and all(row.get('speed_sample_count') == 2 for row in sources)
    measurement_sources = json.loads((output / 'measurement_sources.json').read_bytes())['files']
    assert any(row['path'] == 'scripts/audit_data_credentials.py' for row in measurement_sources)
    (root / 'scripts/audit_data_credentials.py').write_bytes(b'future version')
    for path in bundle.paths:
        path.write_bytes(b'future public receipt')
    verified = profile.verify_collection(output)
    assert verified['state'] == 'verified' and verified['collector_phases_verified'] == 13


def test_missing_fixed_receipt_is_retained_as_an_issue(profile, bundle, tmp_path):
    bundle.paths[7].rename(bundle.directory / 'incomplete.json')
    ledger, evidence, count = collect(profile, bundle, tmp_path)
    assert count == 0 and not ledger.samples and evidence.issues
    evidence.close_snapshot_store()


def test_portable_fixture_denies_private_benchmark_source_reads(bundle):
    with pytest.raises(AssertionError, match='private_benchmark_source_read'):
        (REPO / 'artifacts/benchmarks/private-source-must-not-be-read.py').read_bytes()
