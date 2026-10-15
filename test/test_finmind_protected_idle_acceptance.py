from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import seal_finmind_protected_idle as seal
from scripts.seal_finmind_protected_idle import window_proof


def fixtures():
    common = {'state': 'observed', 'native_owner_unchanged': True, 'last_actual_acquisition_unchanged': True,
              'sampled_prior_source_files_unchanged': True, 'preserved_source_files': [{'sha256': 'source'}]}
    baseline = {**common, 'sample_seconds': 180, 'cpu_seconds': 18,
                'observed_cycle_clocks': ['2026-10-15T00:24:00+00:00', '2026-10-15T00:35:00+00:00']}
    reuse = {**common, 'sample_seconds': 690, 'cpu_seconds': .08,
             'observed_cycle_clocks': ['2026-10-15T00:38:00+00:00'],
             'started_at_utc': '2026-10-15T00:39:00+00:00', 'observed_at_utc': '2026-10-15T00:50:30+00:00',
             'samples': [{'phase': 'waiting', 'reason': 'protected_opening', 'active_task': None,
                          'acquisition_state': 'protected_opening', 'cycle_observed_at_utc': '2026-10-15T00:38:00+00:00'}]}
    return deepcopy((baseline, reuse))


def test_requires_real_old_boundary_not_just_sleeping_flag():
    baseline, reuse = fixtures()
    assert window_proof(baseline, reuse) == (True, '2026-10-15T00:48:00+00:00')
    reuse['observed_at_utc'] = '2026-10-15T00:47:59+00:00'
    assert not window_proof(baseline, reuse)[0]


@pytest.mark.parametrize('field,value', [
    ('native_owner_unchanged', False), ('last_actual_acquisition_unchanged', False),
    ('sampled_prior_source_files_unchanged', False), ('preserved_source_files', []),
    ('sample_seconds', 30), ('cpu_seconds', 1000), ('samples', []),
    ('observed_cycle_clocks', ['2026-10-15T00:38:00+00:00', '2026-10-15T00:49:00+00:00']),
])
def test_changed_sources_owners_cycles_and_short_windows_rejected(field, value):
    baseline, reuse = fixtures()
    reuse[field] = value
    assert not window_proof(baseline, reuse)[0]


def test_active_acquisition_cannot_be_presented_as_idle_savings():
    baseline, reuse = fixtures()
    reuse['samples'][0]['active_task'] = {'dataset': 'actual_work'}
    assert not window_proof(baseline, reuse)[0]


@pytest.mark.parametrize('wrong_code', [False, True])
def test_seal_binds_native_code_test_scope_and_current_owner(tmp_path, monkeypatch, wrong_code):
    monkeypatch.chdir(tmp_path)
    before, after = fixtures()
    old, new = {'InvocationID': 'old'}, {'InvocationID': 'new'}
    before['native_owner'], after['native_owner'] = old, new
    after['samples'][0]['next_check_at_utc'] = '2026-10-15T01:10:00+00:00'
    runtime = tmp_path / 'downloader/finmind_runtime.py'
    runtime.parent.mkdir()
    runtime.write_bytes(b'fixed runtime code')
    (runtime.parent / 'finmind_scheduling.py').write_bytes(b'fixed scheduler')
    control = {'state': 'canonical_owner_schema_and_status_verified', 'service_before': old,
               'service_after': new, 'restart_at_utc': '2026-10-15T00:38:00+00:00'}
    sql = {'state': 'accepted_bounded_finmind_status_cache', 'native_owner': new,
           'native_complete_status_equals_uncached_canonical_sql': True,
           'native_generation_and_status_stable': True, 'protected_services_unchanged': True,
           'gpu_device_handles': [], 'unreadable_sampled_pids': []}
    argv = []
    for name, value in (('baseline', before), ('reuse', after), ('reload', control), ('sql-readback', sql)):
        path = tmp_path / (name + '.json')
        path.write_text(json.dumps(value))
        argv.extend(['--' + name, str(path)])
    run_id = 'controlled-tests'
    run_path = Path('artifacts/operations/agent-workflow/runs') / run_id / 'run.json'
    run_path.parent.mkdir(parents=True)
    log_path = run_path.with_name('run.log')
    log_path.write_text('1121 passed in 67.73s\n')
    run_path.write_text(json.dumps({'task_id': 'running-services-optimization-20261013',
        'state': 'succeeded', 'exit_code': 0, 'log': str(log_path),
        'argv': ['test_(finmind|reload_finmind|summarize_finmind)', 'test/test_runtime_env.py',
                 'test/test_agent_workflow.py']}))
    native = {'_SYSTEMD_INVOCATION_ID': 'new', 'MESSAGE': json.dumps({'finmind_protected_idle': {
        'contract_version': 1, 'loaded_runtime_sha256': 'wrong' if wrong_code else hashlib.sha256(runtime.read_bytes()).hexdigest(),
        'calendar_recheck_seconds': 30, 'until_utc': '2026-10-15T01:10:00+00:00'}})}
    monkeypatch.setattr(seal, 'identity', lambda unit: new)
    monkeypatch.setattr(seal.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout=json.dumps(native)))
    output = tmp_path / 'sealed.json'
    argv.extend(['--regression-run-id', run_id, '--output', str(output)])
    if wrong_code:
        with pytest.raises(ValueError, match='not_proven'):
            seal.main(argv)
        assert json.loads(output.read_bytes())['state'] == 'protected_idle_not_accepted'
    else:
        assert seal.main(argv) == 0
        report = json.loads(output.read_bytes())
        assert report['state'] == 'accepted_bounded_protected_idle'
        assert report['regression_passed_count'] == 1121
