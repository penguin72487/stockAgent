import json
import fcntl
from datetime import UTC, datetime, timedelta

import pytest

from scripts import dataset_migration_status as projection
from stockagent.data_sync.dataset_migration import migration_process_identity


@pytest.fixture
def journal(tmp_path):
    operation = tmp_path / 'operations' / 'current-cycle'
    operation.mkdir(parents=True)
    current = tmp_path / 'current.json'
    current.write_text(json.dumps({'contract': 'all_catalog_incremental_migration_v1',
        'operation_directory': str(operation), 'plan_sha256': 'plan'}))
    (operation / 'plan.json').write_text(json.dumps({'plan_sha256': 'plan', 'catalog': [
        {'dataset': 'one', 'publish': True}, {'dataset': 'two', 'publish': False}]}))
    (operation / 'progress.json').write_text(json.dumps({'plan_sha256': 'plan', 'state': 'processing',
        'runtime_owner': {**migration_process_identity(), 'activity_id': 'activity',
                          'active_dataset': 'two', 'active_attempt_id': 'same'},
        'datasets': {'one': {'state': 'verified'}, 'two': {'state': 'recovery',
                    'attempt_id': 'same', 'owner_activity_id': 'activity'}}}))
    with (operation / 'owner.lock').open('w') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield current, operation


def test_status_reads_only_complete_owner_journals(journal):
    current, operation = journal
    (operation / 'recovery').mkdir()
    (operation / 'recovery/two.progress.json').write_text(json.dumps({'stage': 'verify-cold-objects',
        'completed_bytes': 3, 'total_bytes': 7, 'acceptance': False, 'attempt_id': 'same'}))
    result = projection.status(current)
    assert result['source_count'] == 2 and result['verified_sources'] == 1
    assert result['states'] == {'verified': 1, 'recovery': 1}
    assert result['active_sources'] == 1
    assert result['domains'] == {'shared': 1, 'host_private': 1}
    assert result['datasets'][1]['recovery_progress']['completed_bytes'] == 3
    assert result['global_byte_percent'] is None
    assert result['source_payload_reads'] == result['provider_calls'] == 0
    assert result['NAS_acceptance_claim'] is False
    assert all(not row['native_workspace_deletion_authorized'] for row in result['datasets'])
    fds = result['runtime_owner']['file_descriptors']
    assert fds['contract'] == 'process_fd_budget_observation_v1'
    assert fds['state'] == 'sampled' and fds['open_descriptors'] > 0
    assert fds['sample_is_atomic'] is False


def test_fresh_work_is_not_mistaken_for_an_old_transition_journal(journal):
    current, operation = journal
    path = operation / 'progress.json'
    document = json.loads(path.read_text())
    document['updated_at_utc'] = '2000-01-01T00:00:00+00:00'
    path.write_text(json.dumps(document))
    stamp = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    (operation / 'recovery').mkdir()
    (operation / 'recovery/two.progress.json').write_text(json.dumps({
        'stage': 'reading_cold_objects', 'attempt_id': 'same', 'observed_at_utc': stamp}))
    result = projection.status(current)
    assert result['transition_journal_updated_at_utc'] == document['updated_at_utc']
    assert result['latest_active_work_progress_at_utc'] == stamp
    observation = result['datasets'][1]['activity_progress_observation']
    assert observation['state'] == 'recent'
    assert 5 <= observation['age_seconds'] < 30
    assert observation['acceptance'] is False


@pytest.mark.parametrize('case', ['old', 'future', 'naive', 'malformed', 'missing', 'inactive'])
def test_progress_sample_does_not_fabricate_liveness_or_failure(case):
    now = datetime(2026, 10, 18, tzinfo=UTC)
    stamps = {'old': now - timedelta(seconds=301), 'future': now + timedelta(seconds=1),
              'naive': now.replace(tzinfo=None), 'malformed': 'not-a-clock',
              'missing': None, 'inactive': now}
    stamp = stamps[case]
    document = {'observed_at_utc': stamp.isoformat() if isinstance(stamp, datetime) else stamp}
    item = {'runtime_active': case != 'inactive', 'state': 'processing',
            'publication_progress': document}
    result = projection.activity_progress_observation(item, now)
    expected = {'old': 'stale_sample', 'future': 'invalid_timestamp', 'naive': 'invalid_timestamp',
                'malformed': 'invalid_timestamp', 'missing': 'missing_timestamp', 'inactive': 'inactive'}
    assert result['state'] == expected[case]
    assert result['stale_is_failure_proof'] is result['acceptance'] is False
    if case != 'old':
        assert result['age_seconds'] is None


@pytest.mark.parametrize('fault', ['plan', 'omission', 'outside_owner', 'redirected_journal'])
def test_status_rejects_ambiguous_or_incomplete_scope(journal, tmp_path, fault):
    current, operation = journal
    if fault == 'outside_owner':
        document = json.loads(current.read_text())
        document['operation_directory'] = str(tmp_path)
        current.write_text(json.dumps(document))
    elif fault == 'redirected_journal':
        original = operation / 'progress.json'
        original.rename(operation / 'elsewhere.json')
        original.symlink_to(operation / 'elsewhere.json')
    else:
        original = operation / 'progress.json'
        document = json.loads(original.read_text())
        if fault == 'plan':
            document['plan_sha256'] = 'different-generation'
        else:
            del document['datasets']['two']
        original.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        projection.status(current)


@pytest.mark.parametrize('legacy', [False, True])
def test_retry_never_reports_previous_attempt_as_current_progress(journal, legacy):
    current, operation = journal
    path = operation / 'progress.json'
    document = json.loads(path.read_text())
    row = document['datasets']['two']
    row.update(state='processing', started_at_utc='2026-10-17T03:00:00+00:00')
    if not legacy:
        row['attempt_id'] = 'new-attempt'
    else:
        row.pop('attempt_id', None)
    document['runtime_owner']['active_attempt_id'] = row.get('attempt_id')
    path.write_text(json.dumps(document))
    (operation / 'publication').mkdir()
    (operation / 'publication/two.json').write_text(json.dumps({
        'attempt_id': 'old-attempt', 'observed_at_utc': '2026-10-17T02:00:00+00:00',
        'files_completed': 99, 'files_total': 100}))
    result = projection.status(current)['datasets'][1]
    assert result['publication_progress'] is None
    assert result['previous_attempt_publication_progress']['files_completed'] == 99


def test_progress_is_bound_to_current_attempt_and_recovery_release(journal):
    current, operation = journal
    path = operation / 'progress.json'
    document = json.loads(path.read_text())
    document['datasets']['two'].update(attempt_id='same', snapshot_id='new-snapshot')
    path.write_text(json.dumps(document))
    (operation / 'publication').mkdir()
    (operation / 'recovery').mkdir()
    (operation / 'publication/two.json').write_text(json.dumps({'attempt_id': 'same', 'files_completed': 7}))
    (operation / 'recovery/two.progress.json').write_text(json.dumps({
        'attempt_id': 'same', 'snapshot_id': 'old-snapshot', 'completed_bytes': 99}))
    result = projection.status(current)['datasets'][1]
    assert result['publication_progress']['files_completed'] == 7
    assert result['recovery_progress'] is None


@pytest.mark.parametrize('fault', ['boot', 'pid_generation', 'activity', 'dataset', 'attempt'])
def test_old_activity_never_appears_live(journal, fault):
    current, operation = journal
    path = operation / 'progress.json'
    document = json.loads(path.read_text())
    owner = document['runtime_owner']
    key, value = {'boot': ('boot_id', 'previous-boot'),
        'pid_generation': ('start_ticks', owner['start_ticks'] - 1),
        'activity': ('activity_id', 'previous-activity'),
        'dataset': ('active_dataset', 'one'),
        'attempt': ('active_attempt_id', 'previous-attempt')}[fault]
    owner[key] = value
    path.write_text(json.dumps(document))
    (operation / 'publication').mkdir()
    (operation / 'publication/two.json').write_text(json.dumps({'attempt_id': 'same', 'files_completed': 99}))
    result = projection.status(current)
    row = result['datasets'][1]
    assert result['active_sources'] == 0
    assert row['state'] == 'interrupted' and row['journal_state'] == 'recovery'
    assert row['publication_progress'] is None
    assert row['unverified_publication_progress']['files_completed'] == 99
    assert 'previous_attempt_publication_progress' not in row


def test_legacy_inflight_state_without_process_evidence_is_not_claimed_live(journal):
    current, operation = journal
    path = operation / 'progress.json'
    document = json.loads(path.read_text())
    document.pop('runtime_owner')
    path.write_text(json.dumps(document))
    result = projection.status(current)
    assert result['active_sources'] == 0
    assert result['datasets'][1]['state'] == 'inflight_unverified'


def test_process_alive_without_exclusive_operation_lock_is_not_active(journal):
    current, operation = journal
    # The live PID alone is insufficient once this activity releases its lock.
    owner = json.loads((operation / 'progress.json').read_text())['runtime_owner']
    other = operation / 'inactive'
    other.mkdir()
    (other / 'owner.lock').touch()
    assert projection.migration_owner_observation(other, owner) == {
        'lock_held': False, 'process_matches': True}


@pytest.fixture
def descriptor_process(tmp_path, monkeypatch):
    owner = {'pid': 42, 'start_ticks': 100, 'boot_id': 'current'}
    process = tmp_path / '42'
    (process / 'fd').mkdir(parents=True)
    for index in range(7):
        (process / 'fd' / str(index)).touch()
    (process / 'limits').write_text('Max open files            64                  1024                 files\n')
    monkeypatch.setattr(projection, 'migration_process_identity', lambda pid: dict(owner))
    return owner, process


def test_fd_pressure_is_source_free_and_fenced(descriptor_process):
    owner, process = descriptor_process
    result = projection.file_descriptor_status(owner, {'process_matches': True}, proc_root=process.parent)
    assert result['open_descriptors'] == 7 and result['soft_limit'] == 64
    assert result['remaining_descriptors'] == 57 and result['fraction_used'] == 7 / 64
    assert projection.file_descriptor_status(owner, {'process_matches': False})['state'] == 'unverified_owner'


@pytest.mark.parametrize('failure', ['unreadable', 'malformed', 'zero', 'unlimited', 'generation'])
def test_fd_metrics_never_make_an_unknown_process_look_healthy(descriptor_process, monkeypatch, failure):
    owner, process = descriptor_process
    if failure == 'unreadable':
        (process / 'limits').unlink()
    elif failure == 'malformed':
        (process / 'limits').write_text('not a process limits file')
    elif failure in ('zero', 'unlimited'):
        (process / 'limits').write_text('Max open files            ' + ('0' if failure == 'zero' else 'unlimited') + ' 1024 files\n')
    else:
        observations = iter((dict(owner), {**owner, 'start_ticks': 101}))
        monkeypatch.setattr(projection, 'migration_process_identity', lambda pid: next(observations))
    result = projection.file_descriptor_status(owner, {'process_matches': True}, proc_root=process.parent)
    if failure == 'unlimited':
        assert result['state'] == 'sampled' and result['open_descriptors'] == 7
    else:
        assert result['state'] in ('unavailable', 'unverified_owner')
        assert result['open_descriptors'] is None
    assert result['fraction_used'] is None
