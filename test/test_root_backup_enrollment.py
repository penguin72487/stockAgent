"""Only verified moved backups enter the existing private source owner."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import publish_data_releases as publisher
from scripts import rollout_lakehouse_source_policy as rollout


@pytest.fixture
def moved_backups(tmp_path, monkeypatch):
    repo = tmp_path / 'repo'
    state = repo / rollout.ROOT_BACKUP_ROOT
    source = state / 'data'
    source.mkdir(parents=True, mode=0o700)
    state.chmod(0o700)
    for name in rollout.ROOT_BACKUP_NAMES:
        target = source / name
        target.mkdir()
        count = 0
        if name != 'StockAgent':
            (target / 'original').write_bytes(('private ' + name).encode())
            count = 1
        info = target.stat()
        atomic_write_json(state / (name + '.receipt.json'), {
            'contract': 'root-backup-atomic-relocation-v1', 'state': 'verified',
            'payload_deleted': False, 'file_count': count,
            'move': {'target': str(target), 'device': info.st_dev, 'directory_inode': info.st_ino},
        })
    catalog = repo / rollout.CATALOG_RELATIVE
    catalog.parent.mkdir(parents=True)
    atomic_write_json(catalog, {'schema_version': 1, 'datasets': [
        {'dataset': 'other', 'source': 'other', 'publish': False, 'role': 'test',
         'pack_buckets': 1, 'loose_threshold_mib': 8}], 'untouched': {'keep': True}})
    monkeypatch.setattr(publisher, 'REPO_ROOT', repo)
    return repo, source, catalog


def test_enrollment_preserves_private_originals_and_grants_no_recovery_or_retirement(moved_backups):
    repo, source, catalog = moved_backups
    old = catalog.read_bytes()
    payloads = {path: path.read_bytes() for path in source.rglob('original')}
    prepared = rollout.prepare_root_backup_enrollment(repo)
    assert catalog.read_bytes() == old
    result = rollout.commit_root_backup_enrollment(prepared, repo / 'intent.json')
    assert result['state'] == 'catalog_enrolled_not_yet_cold_or_NAS_verified'
    assert result['source_files_deleted'] == 0
    assert result['cold_reconstruction_verified'] is result['NAS_restore_ACK_verified'] is False
    document = json.loads(catalog.read_bytes())
    assert document['untouched'] == {'keep': True}
    assert document['datasets'][0] == json.loads(old)['datasets'][0]
    assert document['datasets'][-1]['publish'] is False
    assert document['datasets'][-1]['dataset'] == rollout.ROOT_BACKUP_DATASET
    assert {path: path.read_bytes() for path in payloads} == payloads


@pytest.mark.parametrize('fault', ['extra', 'link', 'receipt', 'not-private'])
def test_bad_moved_tree_is_never_enrolled(moved_backups, fault):
    repo, source, catalog = moved_backups
    if fault == 'extra':
        (source / 'unreviewed').mkdir()
    elif fault == 'link':
        (source / 'StockAgent' / 'link').symlink_to('.')
    elif fault == 'receipt':
        path = source.parent / 'StockAgent.receipt.json'
        value = json.loads(path.read_bytes())
        value['payload_deleted'] = True
        atomic_write_json(path, value)
    else:
        source.chmod(0o755)
    original = catalog.read_bytes()
    with pytest.raises(ValueError):
        rollout.prepare_root_backup_enrollment(repo)
    assert catalog.read_bytes() == original


@pytest.mark.parametrize('fault', ['catalog', 'source', 'receipt', 'intent'])
def test_intervening_changes_abort_before_catalog_commit(moved_backups, fault):
    repo, source, catalog = moved_backups
    prepared = rollout.prepare_root_backup_enrollment(repo)
    intent = repo / 'intent.json'
    if fault == 'catalog':
        value = json.loads(catalog.read_bytes())
        value['other_user_change'] = True
        atomic_write_json(catalog, value)
    elif fault == 'source':
        (source / 'stockagent-sync-backups' / 'original').write_bytes(b'changed original')
    elif fault == 'receipt':
        path = source.parent / 'StockAgent.receipt.json'
        value = json.loads(path.read_bytes())
        value['new_observation'] = True
        atomic_write_json(path, value)
    else:
        atomic_write_json(intent, {'existing': True})
    original = catalog.read_bytes()
    with pytest.raises(ValueError):
        rollout.commit_root_backup_enrollment(prepared, intent)
    assert catalog.read_bytes() == original


def test_enrolled_private_tree_reconstructs_every_original_without_source_deletion(moved_backups):
    from stockagent.data_sync import dataset_migration as migration
    from stockagent.data_sync import packed_snapshots as packed

    repo, source, _ = moved_backups
    prepared = rollout.prepare_root_backup_enrollment(repo)
    rollout.commit_root_backup_enrollment(prepared, repo / 'intent.json')
    config = json.loads((Path(rollout.__file__).resolve().parents[1] / 'configs/columnar_storage.json').read_bytes())
    config['source_versioning']['roots'] = {}
    config['dataset_version_lifecycle']['pending_source_priority'] = []
    (repo / 'configs/columnar_storage.json').write_text(json.dumps(config))
    cold = repo.parent / 'cold'
    packed.initialize_packed_layout(cold, node_id='test')
    args = SimpleNamespace(sync_root=cold, migration_state=repo.parent / 'journal', node_id='test',
        retry_seconds=1, maximum_passes=1, watch=False, recover_missing_base_objects=False,
        source_lock_fd=None)
    result = migration.migrate_catalog(repo, args, [prepared['entry']])
    row = result['datasets'][rollout.ROOT_BACKUP_DATASET]
    assert row['state'] == 'verified' and result['state'] == 'verified'
    authority = migration.private_history_root(cold, config['source_versioning'], initialize=False)
    fixed = packed.resolve_packed_snapshot_id(authority, rollout.ROOT_BACKUP_DATASET, row['snapshot_id'])
    restored = packed.fetch_packed_snapshot(authority, repo.parent / 'restored', fixed)
    originals = {path.relative_to(source): path.read_bytes() for path in source.rglob('original')}
    assert originals
    assert {name: (restored / name).read_bytes() for name in originals} == originals
    assert {path.relative_to(source): path.read_bytes() for path in source.rglob('original')} == originals
    assert fixed.manifest['metadata']['shared_export'] == 'false'
    assert fixed.manifest['metadata']['nas_export'] == 'false'


def test_full_handoff_waits_for_original_owners_and_loads_exact_private_catalog(moved_backups, monkeypatch):
    import fcntl
    import hashlib
    import subprocess

    repo, source, catalog = moved_backups
    state = repo / 'state'
    (state / 'dataset-versions').mkdir(parents=True)
    c = {'state_root': str(state), 'producer_device_id': 'producer', 'receiver_device_id': 'receiver',
         'source_replication': {}}
    ready, private = repo / 'readiness.json', repo / 'private.json'
    atomic_write_json(ready, {'producer_device_id': 'producer', 'receiver_device_id': 'receiver',
                             'ingress_free_bytes': 1})
    private.write_bytes(b'{"private_policy":"unchanged"}')
    monkeypatch.setattr(rollout, 'ROOT', repo)
    monkeypatch.setattr(rollout, 'RECEIVER_READINESS', ready)
    monkeypatch.setattr(rollout, 'PRIVATE_CONFIGURATION', private)
    monkeypatch.setattr(rollout, 'configuration', lambda: c)
    monkeypatch.setattr(rollout, 'guard', lambda *_: None)
    monkeypatch.setattr(rollout, 'source_wave_policy', lambda *_: None)

    def identity(**options):
        raw = options.get('source_overrides', {}).get(rollout.CATALOG_RELATIVE, catalog.read_bytes())
        return hashlib.sha256(raw).hexdigest()
    monkeypatch.setattr(rollout, 'worker_code_identity', identity)
    processes, events = {'worker': '100'}, []
    monkeypatch.setattr(rollout, 'pid', lambda unit: processes['worker'] if unit == rollout.OWNER else unit)
    async def workflows(_):
        return {name: {'status': 'RUNNING', 'first_run_id': name, 'run_id': name}
                for name in (*rollout.IDS, *rollout.OPTIONAL_IDS)}
    monkeypatch.setattr(rollout, 'workflows', workflows)
    locks = (state / 'dataset-versions/owner.lock', state / 'source-replication-owner.lock',
             state / 'catalog-owner.lock')
    def run(command, **_options):
        events.append(command[1])
        if command[1] == 'stop':
            for lock in locks:
                with lock.open('a') as other:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            processes['worker'] = '0'
        else:
            processes['worker'] = '200'
            atomic_write_json(state / 'worker-ready.json', {'code_identity_sha256': identity()})
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(rollout.subprocess, 'run', run)
    output = repo / 'handoff.json'
    rollout.main(['--output', str(output), '--reload-code-only', '--enroll-private-root-backups'])
    result = json.loads(output.read_bytes())
    assert result['state'] == 'accepted' and not result['authorized_active_owner_restart']
    assert result['root_backup_enrollment']['state'] == 'catalog_enrolled_not_yet_cold_or_NAS_verified'
    assert not result['root_backup_enrollment']['cold_reconstruction_verified']
    assert result['loaded_code_identity_sha256'] == result['candidate_code_identity_sha256'] == identity()
    assert result['workflow_chains_preserved'] and result['postgres_and_temporal_server_not_restarted']
    assert private.read_bytes() == b'{"private_policy":"unchanged"}'
    assert events == ['stop', 'start', 'start']
    assert (source / 'stockagent-sync-backups/original').is_file()


@pytest.mark.parametrize('options', [[], ['--reload-code-only', '--restart-active-owner'],
                                      ['--reload-code-only', '--retained-source-candidate', 'other']])
def test_private_root_enrollment_refuses_unreviewed_handoff_modes(tmp_path, monkeypatch, options):
    monkeypatch.setattr(rollout, 'configuration', lambda: pytest.fail('invalid mode reached service access'))
    with pytest.raises(SystemExit) as error:
        rollout.main(['--output', str(tmp_path / 'receipt'), '--enroll-private-root-backups', *options])
    assert error.value.code == 2
