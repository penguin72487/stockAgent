"""Deferred exact recovery must not monopolize the bounded GC owner."""
import json

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import retire_lake_transport_cache as module


@pytest.fixture
def queue(tmp_path, monkeypatch):
    a, b = 'a' * 64, 'b' * 64
    atomic_write_json(tmp_path / 'source-replication-ledger.json', {'deliveries': {
        key: {'nas_acceptance': {'state': 'nas_archive_file_recovery_verified'}} for key in (a, b)}})
    monkeypatch.setattr(module.time, 'time', lambda: 1000)
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
        'attempts': {key: {'retry_at_epoch': 2000, 'last_attempt_epoch': 1000} for key in (a, b)}})
    monkeypatch.setattr(module, 'acceptance', lambda *args: pytest.fail('cooldown was bypassed'))
    result = module.retire_one({}, root)
    assert result['state'] == 'waiting_transport_retry' and result['retry_wait_count'] == 2
    assert result['next_retry_at_epoch'] == 2000 and not list(root.glob('cache-retirement-*.json'))


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
