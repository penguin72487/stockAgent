"""The performance comparator must preserve the complete native source cycle."""
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

pytest.importorskip('temporalio', reason='use the locked lakehouse control role')
from downloader.artifact_io import atomic_write_json
from scripts import benchmark_lakehouse_source_wave as benchmark
from stockagent.control import lakehouse
from stockagent.data_sync import nas_coverage


def test_comparator_rejects_changes_outside_selected_cycle():
    original = 'def other(): return 1\ndef stage_source_wave(c): return 1\n'
    benchmark.validate_code_scope(original, original.replace('wave(c): return 1', 'wave(c): return 2'))
    with pytest.raises(ValueError, match='unrelated source'):
        benchmark.validate_code_scope(original, original.replace('other(): return 1', 'other(): return 2'))
    with pytest.raises(ValueError, match='one canonical'):
        benchmark.validate_code_scope(original, 'def other(): return 1\n')


def test_resource_matching_never_targets_native_owner(monkeypatch):
    monkeypatch.setattr(Path, 'read_text', lambda *args, **kw: '0::/system.slice/stockagent-storage-lifecycle.service\n')
    monkeypatch.setattr(benchmark.subprocess, 'run', lambda *args, **kw: pytest.fail('must not modify a production cgroup'))
    with pytest.raises(ValueError, match='dedicated supervised'):
        benchmark.match_native_limits()


@pytest.mark.parametrize('change', ['expired', 'old_source_status', 'pending', 'new_objects', 'processing', 'none', 'waiting_nas'])
def test_native_window_requires_real_fresh_completed_source_cycle(change):
    now = datetime(2026, 10, 15, 3, 30, tzinfo=UTC)
    refresh = {'state': 'registered', 'observed_at_utc': '2026-10-15T03:29:00+00:00',
               'retry_at_epoch': now.timestamp() + 500}
    status = {'state': 'monitoring_increments', 'observed_at_utc': now.isoformat(),
              'pending_delivery_ids': [], 'unpublished_source_object_count': 0}
    if change == 'expired': refresh['retry_at_epoch'] = now.timestamp() + 30
    elif change == 'old_source_status': status['observed_at_utc'] = '2026-10-15T03:28:00+00:00'
    elif change == 'pending': status['pending_delivery_ids'] = ['fixed-wave']
    elif change == 'new_objects': status['unpublished_source_object_count'] = 1
    elif change == 'processing': status['state'] = 'processing_immutable_increments'
    elif change == 'waiting_nas':
        status.update(state='waiting_nas_archive_acceptance', pending_delivery_ids=['fixed-wave'])
    assert benchmark.monitoring_window_ready(refresh, status, now) == (change in ('none', 'waiting_nas'))


def test_pending_fixture_preserves_absence_and_refuses_arriving_or_unverified_proof(tmp_path):
    ledger = {'deliveries': {'fixed-wave': {'file_keys': [], 'bytes': 27}}}
    path = tmp_path / 'lake-fixed-wave.json'
    assert benchmark.pending_receipt_absences(ledger, tmp_path) == [{'path': str(path), 'exists': False}]
    path.write_text('{}')
    with pytest.raises(ValueError, match='consume its arriving'):
        benchmark.pending_receipt_absences(ledger, tmp_path)
    ledger['deliveries']['fixed-wave']['nas_acceptance'] = {'state': 'not-verified'}
    with pytest.raises(ValueError, match='unverified proof'):
        benchmark.pending_receipt_absences(ledger, tmp_path)
    ledger['deliveries']['fixed-wave']['nas_acceptance'] = {'state': 'nas_archive_file_recovery_verified'}
    assert benchmark.pending_receipt_absences(ledger, tmp_path) == []


@pytest.mark.parametrize('registry_changed', [False, True])
@pytest.mark.parametrize('pending', [False, True])
@pytest.mark.parametrize('retirement', [False, True])
def test_complete_replay_preserves_ledger_and_uses_full_readonly_rows(
        tmp_path, monkeypatch, registry_changed, pending, retirement):
    template = tmp_path / 'template'
    template.mkdir()
    relative, digest = 'objects/packs/fixed.pack', 'a' * 64
    rows = [(relative, digest, 27)]
    ledger = {'deliveries': {'fixed': {'file_keys': [relative + '@' + digest], 'bytes': 27,
        'nas_acceptance': {'state': 'nas_archive_file_recovery_verified', 'delivery_identity_sha256': 'fixed'}}}}
    if pending:
        del ledger['deliveries']['fixed']['nas_acceptance']
    (template / 'receipts').mkdir()
    if retirement:
        atomic_write_json(template / 'cache-retirement-fixed.json', {
            'state': 'retired', 'reclaimed_bytes': 27,
            'primary_source_files_deleted': 0, 'nas_archive_files_deleted': 0})
    atomic_write_json(template / 'source-replication-ledger.json', ledger)
    active = benchmark.module(Path(lakehouse.__file__).read_text(), 'unit_source_wave')
    monkeypatch.setattr(active, 'guard', lambda c: None)
    calls = []

    class Connection:
        def execute(self, sql):
            assert sql == benchmark.SOURCE_SQL
            calls.append(sql)
            return self

        def fetchall(self):
            return [] if registry_changed else rows

        def close(self):
            calls.append('closed')

    def readonly(c, **kwargs):
        assert kwargs == {'read_only': True}
        return Connection()

    monkeypatch.setattr(lakehouse, 'connect', readonly)
    monkeypatch.setattr(nas_coverage, 'read_ledger', lambda path: pytest.fail('no priority demand'))
    c = {'state_root': 'not-the-private-replay', 'receipt_root': str(tmp_path / 'receipts'),
         'source_replication': {'prioritize_uncovered_nas': True, 'decouple_nas_acceptance': True}}
    options = active, c, tmp_path / 'trial', template, tmp_path / 'restic', rows, datetime.now(UTC)
    if registry_changed:
        with pytest.raises(ValueError, match='registry changed'):
            benchmark.run_trial(*options)
    else:
        result = benchmark.run_trial(*options)
        assert result['restic_reads'] == 0 and result['readonly_connections'] == 1
        assert result['pinned_nonledger_inputs_preserved']
        assert result['ledger_advanced_by_original_retirement_receipts'] == (retirement and not pending)
        assert result['complete_output']['unpublished_source_object_count'] == 0
        assert result['complete_output']['pending_delivery_ids'] == (['fixed'] if pending else [])
        assert result['complete_output']['state'] == ('waiting_nas_archive_acceptance' if pending else 'monitoring_increments')
        assert json.loads((tmp_path / 'trial/durable-readback.json').read_bytes()) == result['complete_output']
    assert calls == [benchmark.SOURCE_SQL, 'closed']


@pytest.mark.parametrize('field', ['bytes', 'nas_acceptance', 'file_keys', 'transport_retirement'])
def test_ledger_oracle_rejects_unreceipted_or_source_mutation(tmp_path, field):
    original = {'deliveries': {'fixed': {'file_keys': ['object@sha'], 'bytes': 27,
        'nas_acceptance': {'state': 'nas_archive_file_recovery_verified'}}}}
    modified = json.loads(json.dumps(original))
    modified['deliveries']['fixed'][field] = None
    with pytest.raises(ValueError, match='membership, NAS proofs'):
        benchmark.validate_ledger_transition(tmp_path, original, modified)
