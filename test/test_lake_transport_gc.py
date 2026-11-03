"""Deferred exact recovery must not monopolize the bounded GC owner."""
import json
import math

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import retire_lake_transport_cache as module


def timing_sample(**changes):
    return {'delivery_identity_sha256': 'a'*64, 'validation_contract': module.VALIDATION_CONTRACT,
            'kind': 'source', 'items': 10, 'bytes': 1000, 'seconds': 100,
            'admission_seconds': 10, **changes}


def test_eta_models_file_and_byte_cost_without_reusing_other_kind_or_failed_gate():
    samples = [timing_sample(), timing_sample(kind='catalog', seconds=100000),
               timing_sample(validation_contract='obsolete', seconds=100000)]
    model = module.duration_model(samples, 'source', {'items': 20, 'bytes': 500})
    assert model['typical_seconds'] == 190 and model['sample_count'] == 1
    assert model['fastest_seconds'] <= 190 <= model['slowest_seconds']
    assert model['confidence'] == 'low'
    assert module.duration_model(samples, 'source', None) is None
    assert module.duration_model([timing_sample(seconds=math.nan)], 'source', {'items': 2, 'bytes': 20}) is None


def test_eta_serial_queue_includes_original_timer_and_cannot_skip_unknown_work(tmp_path):
    writer = module.ProgressWriter(tmp_path, {'state': 'retiring_exact_accepted_transport',
        'delivery_identity_sha256': 'a', 'retry_wait_count': 2}, [('a', 'source'), ('b', 'source')],
        {'a': {'items': 10, 'bytes': 1000}, 'b': {'items': 20, 'bytes': 500}}, started_at=1000)
    writer.samples = [timing_sample()]
    forecast = writer.forecasts(1020)
    assert forecast['completion_forecast']['completion_at_utc'].startswith('1970-01-01T00:18:20')
    assert forecast['queue_forecast']['completion_at_utc'].startswith('1970-01-01T00:22:00')  # 100+190+30 seconds
    assert forecast['queue_forecast']['retry_jobs'] == 2
    writer.candidates.append(('unknown', 'catalog'))
    partial = writer.forecasts(1020)['queue_forecast']
    assert partial['state'] == 'partial' and partial['unknown_jobs'] == 1
    assert 'completion_at_utc' not in partial


def test_eta_never_moves_missed_deadline_forward_on_metadata_refresh(tmp_path):
    writer = module.ProgressWriter(tmp_path, {'state': 'retiring_exact_accepted_transport',
        'delivery_identity_sha256': 'a'}, [('a', 'source')],
        {'a': {'items': 10, 'bytes': 1000}}, started_at=1000)
    writer.samples = [timing_sample()]
    first = writer.forecasts(1020)['completion_forecast']
    later = writer.forecasts(1200)['completion_forecast']
    assert first['state'] == 'estimated' and later['state'] == 'overdue'
    assert first['completion_at_utc'] == later['completion_at_utc']


def test_owner_counter_writes_are_throttled_and_keep_phase_generation(tmp_path, monkeypatch):
    from stockagent.data_sync.work_progress import observe_work, report_work, work_pass
    writes = []
    monkeypatch.setattr(module, 'atomic_write_json', lambda path, value, **kwargs: writes.append(value))
    monkeypatch.setattr(module.time, 'perf_counter', lambda: 1000)
    writer = module.ProgressWriter(tmp_path, {}, [], {}, started_at=1000)
    with observe_work(writer), work_pass('dry_run'):
        for done in range(101):
            report_work('inspect_transport_members', done=done, total=100)
        with work_pass('apply'):
            report_work('inspect_transport_members', total=100)
    report_work('outside_owner', total=100)  # Context is no longer installed.
    assert len(writes) == 3  # Phase start, completion, new pass; not 101 writes.
    assert writes[1]['work_progress']['completed_items'] == 100
    assert writes[2]['phase_iteration'] == 2 and writes[2]['work_pass'] == 'apply'
    assert writes[2]['work_progress']['completed_items'] == 0


def test_only_actual_successful_workflows_enter_timing_history(tmp_path):
    writer = module.ProgressWriter(tmp_path, {}, [], {}, started_at=1000)
    work = {'items': 10, 'bytes': 1000}
    writer.remember({'state': 'retirement_deferred', 'delivery_identity_sha256': 'a',
                     'complete_workflow_seconds': 12}, work)
    assert not (tmp_path/'transport-gc-timing.json').exists()
    success = {'state': 'retired', 'delivery_kind': 'source', 'delivery_identity_sha256': 'a',
               'complete_workflow_seconds': 100}
    writer.remember(success, work)
    writer.remember(success, work)
    assert len(json.loads((tmp_path/'transport-gc-timing.json').read_bytes())['samples']) == 1


@pytest.fixture
def queue(tmp_path, monkeypatch):
    a, b = 'a' * 64, 'b' * 64
    atomic_write_json(tmp_path / 'source-replication-ledger.json', {'deliveries': {
        key: {'nas_acceptance': {'state': 'nas_archive_file_recovery_verified'}} for key in (a, b)}})
    monkeypatch.setattr(module.time, 'time', lambda: 1000)
    monkeypatch.setattr(module, 'catalog_enrollment', lambda *args: None)
    return tmp_path, a, b


@pytest.mark.parametrize('acceptance_failure', [False, True])
def test_failed_wave_keeps_evidence_and_next_invocation_retires_another(queue, monkeypatch, acceptance_failure):
    root, bad, good = queue
    evidence = root / 'failed-evidence'
    evidence.write_bytes(b'preserve missing-source evidence')
    calls = []
    def accept(c, identity):
        if acceptance_failure and identity == bad:
            raise FileNotFoundError(2, 'authority original absent', '/cold/original')
        return {'state': 'nas_archive_file_recovery_verified'}
    def retire(c, identity, proof):
        calls.append(identity)
        if identity == bad:
            return {'state': 'retirement_deferred', 'error_type': 'FileNotFoundError'}
        atomic_write_json(root / ('cache-retirement-' + identity + '.json'), {'state': 'retired'})
        return {'state': 'retired', 'primary_source_files_deleted': 0, 'nas_archive_files_deleted': 0}
    monkeypatch.setattr(module, 'acceptance', accept)
    monkeypatch.setattr(module, 'retire_accepted_transport', retire)
    first = module.retire_one({}, root)
    assert first['delivery_identity_sha256'] == bad and first['state'] == 'retirement_deferred'
    second = module.retire_one({}, root)
    assert second['delivery_identity_sha256'] == good and second['state'] == 'retired'
    assert second['retry_wait_count'] == 1 and evidence.read_bytes() == b'preserve missing-source evidence'
    ledger = json.loads((root / 'source-replication-ledger.json').read_bytes())
    assert set(ledger['deliveries']) == {bad, good}
    assert calls == ([good] if acceptance_failure else [bad, good])


def test_interrupted_attempt_is_persisted_and_yields_to_an_untouched_wave(queue, monkeypatch):
    root, interrupted, good = queue
    def accept(c, identity):
        if identity == interrupted:
            raise KeyboardInterrupt('interrupted expensive recovery gate')
        return {'state': 'nas_archive_file_recovery_verified'}
    monkeypatch.setattr(module, 'acceptance', accept)
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {'state': 'retired'})
    with pytest.raises(KeyboardInterrupt):
        module.retire_one({}, root)
    persisted = json.loads((root / 'transport-gc-schedule.json').read_bytes())
    assert persisted['attempts'][interrupted]['state'] == 'checking'
    assert module.retire_one({}, root)['delivery_identity_sha256'] == good


def test_all_deferred_candidates_wait_without_reverifying_or_claiming_retirement(queue, monkeypatch):
    root, a, b = queue
    atomic_write_json(root / 'transport-gc-schedule.json', {
        'contract': 'immutable_transport_gc_schedule_v1',
        'attempts': {key: {'retry_at_epoch': 2000, 'last_attempt_epoch': 1000,
                          'validation_contract': module.VALIDATION_CONTRACT} for key in (a, b)}})
    monkeypatch.setattr(module, 'acceptance', lambda *args: pytest.fail('cooldown was bypassed'))
    result = module.retire_one({}, root)
    assert result['state'] == 'waiting_transport_retry' and result['retry_wait_count'] == 2
    assert result['next_retry_at_epoch'] == 2000 and not list(root.glob('cache-retirement-*.json'))
    assert result['contract'] == 'stockagent.lake_transport_gc_status.v1'
    assert result['reason_code'] == 'retirement_retry'


def test_failed_retirement_records_specific_reason_and_private_detail_without_changing_retry(queue, monkeypatch):
    root, _, _ = queue
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    def reject(*args):
        raise ValueError('primary source changed since the full SHA recovery gate /private/original')
    monkeypatch.setattr(module, 'retire_accepted_transport', reject)
    result = module.retire_one({}, root)
    assert result['reason_code'] == 'retirement_source_changed'
    assert result['retirement']['detail'].endswith('/private/original')
    assert result['next_retry_at_epoch'] == 1600
    assert result['ready_candidate_count'] == 1 and result['retry_wait_count'] == 1
    assert not list(root.glob('cache-retirement-*.json'))


def test_minute_status_does_not_copy_the_complete_retirement_journal(queue, monkeypatch):
    root, identity, _ = queue
    original = {'state': 'retired', 'delivery_identity_sha256': identity, 'reclaimed_bytes': 123,
                'primary_source_files_deleted': 0, 'nas_archive_files_deleted': 0,
                'files': {'private-member': 'x'*1024**2}, 'primary_signatures': {'private-source': [1]*5}}
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: original)
    result = module.retire_one({}, root)
    assert result['state'] == 'retired' and result['retirement']['reclaimed_bytes'] == 123
    assert len(json.dumps(result)) < 4096 and 'private-member' not in json.dumps(result)
    assert original['files']['private-member'] == 'x'*1024**2


def test_waiting_minute_poll_keeps_the_previous_failed_gate_reason(queue, monkeypatch):
    root, a, b = queue
    atomic_write_json(root/'transport-gc-schedule.json', {
        'contract': 'immutable_transport_gc_schedule_v1', 'attempts': {
            a: {'retry_at_epoch': 2000, 'validation_contract': module.VALIDATION_CONTRACT,
                'error': {'reason_code': 'retirement_source_changed'}},
            b: {'retry_at_epoch': 2100, 'validation_contract': module.VALIDATION_CONTRACT,
                'error': {'reason_code': 'retirement_transport_changed'}}}})
    monkeypatch.setattr(module, 'acceptance', lambda *args: pytest.fail('cooldown was bypassed'))
    result = module.retire_one({}, root)
    assert result['next_retry_at_epoch'] == 2000 and result['reason_code'] == 'retirement_source_changed'
    assert result['phase'] == 'waiting_retirement_retry'


@pytest.mark.parametrize('old_contract', [None, 'obsolete-validator-v1'])
def test_repaired_validator_gets_one_attempt_then_keeps_normal_cooldown_and_old_failure(queue, monkeypatch, old_contract):
    root, identity, _ = queue
    prior_error = {'state': 'retirement_deferred', 'reason_code': 'retirement_source_changed'}
    atomic_write_json(root/'source-replication-ledger.json', {'deliveries': {
        identity: {'nas_acceptance': {'state': 'nas_archive_file_recovery_verified'}}}})
    atomic_write_json(root/'transport-gc-schedule.json', {
        'contract': 'immutable_transport_gc_schedule_v1', 'attempts': {identity: {
            'validation_contract': old_contract, 'attempt_count': 5, 'retry_at_epoch': 9999,
            'error': prior_error}}})
    calls = []
    def accept(*args):
        calls.append('original-NAS-gate')
        return {'state': 'nas_archive_file_recovery_verified'}
    monkeypatch.setattr(module, 'acceptance', accept)
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {
        'state': 'retirement_deferred', 'reason_code': 'retirement_source_changed'})
    first = module.retire_one({}, root)
    assert first['repaired_validator_retry'] is True and calls == ['original-NAS-gate']
    saved = json.loads((root/'transport-gc-schedule.json').read_bytes())['attempts'][identity]
    assert saved['validator_transitions'][0]['previous_error'] == prior_error
    assert saved['validator_transitions'][0]['previous_retry_at_epoch'] == 9999
    assert saved['validation_contract'] == module.VALIDATION_CONTRACT
    assert saved['attempt_count'] == 6 and saved['retry_at_epoch'] == 4600
    assert module.retire_one({}, root)['state'] == 'waiting_transport_retry'
    assert calls == ['original-NAS-gate']


def test_one_repaired_gate_is_not_starved_by_new_untouched_waves(queue, monkeypatch):
    root, repaired, untouched = queue
    atomic_write_json(root/'transport-gc-schedule.json', {
        'contract': 'immutable_transport_gc_schedule_v1', 'attempts': {repaired: {
            'validation_contract': 'old-version', 'attempt_count': 5,
            'last_attempt_epoch': 900, 'retry_at_epoch': 9999}}})
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {'state': 'retired'})
    assert module.retire_one({}, root)['delivery_identity_sha256'] == repaired


def test_oldest_attempt_wins_after_cooldown_even_when_first_wave_keeps_failing(queue, monkeypatch):
    root, a, b = queue
    atomic_write_json(root / 'transport-gc-schedule.json', {
        'contract': 'immutable_transport_gc_schedule_v1',
        'attempts': {a: {'retry_at_epoch': 900, 'last_attempt_epoch': 800, 'attempt_count': 2},
                     b: {'retry_at_epoch': 900, 'last_attempt_epoch': 100, 'attempt_count': 1}}})
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {'state': 'retirement_deferred'})
    assert module.retire_one({}, root)['delivery_identity_sha256'] == b


def test_redirected_schedule_is_preserved(queue):
    root, _, _ = queue
    unrelated = root / 'unrelated'
    unrelated.write_text('{}')
    (root / 'transport-gc-schedule.json').symlink_to(unrelated)
    with pytest.raises(ValueError, match='redirected'):
        module.retire_one({}, root)
    assert unrelated.read_text() == '{}'


def test_pressure_prefers_larger_raw_wave_then_yields_failed_wave_to_another(queue, monkeypatch):
    root, bad, good = queue
    catalog = 'c' * 64
    ledger = json.loads((root / 'source-replication-ledger.json').read_bytes())
    ledger['deliveries'][bad]['bytes'] = 90
    ledger['deliveries'][good]['bytes'] = 80
    atomic_write_json(root / 'source-replication-ledger.json', ledger)
    atomic_write_json(root / ('catalog-acceptance-' + catalog + '.json'), {
        'delivery_identity_sha256': catalog, 'state': 'nas_archive_file_recovery_verified'})
    atomic_write_json(root / 'transport-gc-status.json', {'delivery_kind': 'source'})
    atomic_write_json(root / 'source-replication-status.json', {'retained_transport_bytes': 95})
    policy = {'source_replication': {'maximum_retained_transport_bytes': 100, 'maximum_wave_bytes': 10}}
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    def retire(c, identity, proof):
        return {'state': 'retirement_deferred' if identity == bad else 'retired'}
    monkeypatch.setattr(module, 'retire_accepted_transport', retire)
    first = module.retire_one(policy, root)
    assert first['source_capacity_pressure'] and first['delivery_identity_sha256'] == bad
    second = module.retire_one(policy, root)
    assert second['delivery_identity_sha256'] == good and second['retry_wait_count'] == 1
    assert (root / ('catalog-acceptance-' + catalog + '.json')).is_file()


def test_normal_capacity_preserves_catalog_kind_alternation(queue, monkeypatch):
    root, _, _ = queue
    catalog = 'c' * 64
    atomic_write_json(root / ('catalog-acceptance-' + catalog + '.json'), {
        'delivery_identity_sha256': catalog, 'state': 'nas_archive_file_recovery_verified'})
    atomic_write_json(root / 'transport-gc-status.json', {'delivery_kind': 'source'})
    atomic_write_json(root / 'source-replication-status.json', {'retained_transport_bytes': 10})
    policy = {'source_replication': {'maximum_retained_transport_bytes': 100, 'maximum_wave_bytes': 10}}
    monkeypatch.setattr(module, 'acceptance', lambda *args: {'state': 'nas_archive_file_recovery_verified'})
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {'state': 'retired'})
    result = module.retire_one(policy, root)
    assert not result['source_capacity_pressure'] and result['delivery_identity_sha256'] == catalog


def test_source_gc_admits_original_enrollment_then_runs_full_retirement(queue, monkeypatch):
    root, identity, _ = queue
    ledger = json.loads((root / 'source-replication-ledger.json').read_bytes())
    enrolled = ledger['deliveries'][identity]
    enrolled.update(file_keys=['objects/a@' + 'f' * 64], bytes=17)
    atomic_write_json(root / 'source-replication-ledger.json', ledger)
    calls = []
    def admit(c, key, **options):
        assert key == identity and options == {'enrolled_source': enrolled}
        calls.append('paired_ack')
        return {'state': 'nas_archive_file_recovery_verified'}
    def retire(c, key, proof):
        calls.append('full_local_retirement')
        status = json.loads((root / 'transport-gc-status.json').read_bytes())
        assert status['phase'] == 'verify_and_retire_transport'
        assert status['ready_candidate_count'] == 1  # The active wave is no longer queued.
        assert status['nas_admission_seconds'] >= 0
        return {'state': 'retired'}
    monkeypatch.setattr(module, 'acceptance', admit)
    monkeypatch.setattr(module, 'retire_accepted_transport', retire)
    assert module.retire_one({}, root)['state'] == 'retired'
    assert calls == ['paired_ack', 'full_local_retirement']


@pytest.mark.parametrize('has_enrollment', [False, True])
def test_catalog_gc_uses_only_exact_published_enrollment(queue, monkeypatch, has_enrollment):
    root, _, _ = queue
    identity = 'c' * 64
    atomic_write_json(root / 'source-replication-ledger.json', {'deliveries': {}})
    atomic_write_json(root / ('catalog-acceptance-' + identity + '.json'), {
        'delivery_identity_sha256': identity, 'state': 'nas_archive_file_recovery_verified'})
    enrolled = {'delivery_identity_sha256': identity, 'state': 'published'}
    monkeypatch.setattr(module, 'catalog_enrollment', lambda c, key: enrolled if has_enrollment else None)
    def admit(c, key, **options):
        assert key == identity
        assert options == ({'enrolled_catalog': enrolled} if has_enrollment else {})
        return {'state': 'nas_archive_file_recovery_verified'}
    monkeypatch.setattr(module, 'acceptance', admit)
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: {'state': 'retired'})
    assert module.retire_one({}, root)['state'] == 'retired'


def test_changed_enrollment_failure_preserves_transport_and_never_enters_retirement(queue, monkeypatch):
    root, identity, _ = queue
    ledger = json.loads((root / 'source-replication-ledger.json').read_bytes())
    ledger['deliveries'][identity]['file_keys'] = ['incorrect enrollment']
    atomic_write_json(root / 'source-replication-ledger.json', ledger)
    def reject(c, key, **options):
        raise ValueError('NAS receipt differs from enrolled source publication')
    monkeypatch.setattr(module, 'acceptance', reject)
    monkeypatch.setattr(module, 'retire_accepted_transport', lambda *args: pytest.fail('invalid enrollment reached retirement'))
    assert module.retire_one({}, root)['state'] == 'retirement_deferred'
    assert not list(root.glob('cache-retirement-*.json'))
