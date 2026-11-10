import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from scripts import prepare_offline_repair_archive as worker, retire_offline_repair_source as retire
from scripts import publish_handoff_index_archive as publisher, run_handoff_index_d_queue as d_queue
from stockagent.data_sync import offline_repair_handoff, packed_snapshots, source_stream_staging, storage_progress


@pytest.fixture
def prepared_source(tmp_path, monkeypatch):
    repo, workspace = tmp_path/'repo', tmp_path/'workspace'
    item = {'name': worker.queue.OFFLINE_REPAIR_SCOPE[0],
            'source': worker.queue.OFFLINE_REPAIR_SCOPE[1],
            'marker': worker.queue.OFFLINE_REPAIR_SCOPE[2],
            'budget_bytes': 4 * 1024**3, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    source = repo/item['source']
    source.mkdir(parents=True)
    (source/'promotion_receipt.json').write_text('{"generation":1}')
    (source/'raw.jsonl').write_bytes(b'{"raw":-1}\n'*1000)
    for name in ('downloads', 'migration', 'verify'):
        (workspace/name).mkdir(parents=True)
    monkeypatch.setattr(worker.queue, 'ROOT', repo)
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: [item])
    monkeypatch.setattr(worker.queue, 'check_offline_repair_provenance', lambda _item: None)
    source, destination, evidence, report = worker.queue.paths(item, workspace)
    destination.parent.mkdir(parents=True)
    result = source_stream_staging.copy(source, destination, evidence)
    copy_path = evidence/'copy-result.json'
    report.write_text(json.dumps({'state': 'source_stream_staged_without_destination_readback',
        'source': str(source), 'e_destination': str(destination), 'relocation_evidence': str(copy_path),
        'relocation_evidence_sha256': hashlib.sha256(copy_path.read_bytes()).hexdigest(),
        'files': result['files'], 'logical_bytes': result['logical_bytes'], 'source_files_deleted': 0,
        'source_alias_changed': False, 'destination_independent_readback': False}))
    monkeypatch.setattr(worker.workspace_volume, 'configuration', lambda: {
        'mount_point': str(workspace), 'minimum_free_bytes': 0})
    monkeypatch.setattr(worker.workspace_volume, 'check', lambda **_kwargs: {})
    monkeypatch.setattr(worker.workspace_volume, 'path', lambda name, **_kwargs: workspace/name)
    monkeypatch.setattr(worker.workspace_volume, 'admit', lambda *_args, **_kwargs: {})
    monkeypatch.setattr(worker.handoff, 'binance_handoff_window_allowed', lambda **_kwargs: True)
    monkeypatch.setattr(worker, 'process_references_many', lambda _paths: [])
    @contextmanager
    def reserve(*_args, **_kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(worker, 'reserve_capacity', reserve)
    return source, destination, workspace


def test_prepare_seals_fixed_source_without_deleting_or_repointing(prepared_source):
    source, destination, workspace = prepared_source
    worker.main()
    status_path = workspace/'migration/handoff-index-archive-v1'/(worker.DATASET+'.status.json')
    status = json.loads(status_path.read_bytes())
    assert status['state'] == 'E_index_source_hashed_prepared'
    assert status['files'] == 2 and status['C_payload_deleted'] is False
    assert status['D_archive_committed'] is False and status['destination_independent_readback'] is False
    assert (source/'raw.jsonl').read_bytes() == (destination/'raw.jsonl').read_bytes()
    assert (source/'.download.lock').stat().st_size == 0
    stage = status_path.parent/worker.DATASET/status['source_identity']
    assert json.loads((stage/'prepared.json').read_bytes())['identity_sha256'] == status['prepared_sha256']
    row, = storage_progress.offline_repair_archive_tasks(workspace, workspace_checked=True)
    assert row['state'] == 'completed' and row['complete_evidence'] is True
    assert row['subject'] == 'tw-daytrade-offline-repair'
    assert row['completed_bytes'] == row['total_bytes'] == status['logical_bytes']
    assert (source/'raw.jsonl').exists()  # Completion is packaging, not C retirement.
    worker.main()  # The completed generation and its durable checkpoint are reused.
    assert json.loads(status_path.read_bytes())['source_identity'] == status['source_identity']


def test_prepare_refuses_a_changed_source_before_creating_an_archive(prepared_source):
    source, destination, _workspace = prepared_source
    (source/'raw.jsonl').write_bytes(b'changed')
    with pytest.raises(Exception, match='source generation changed'):
        worker.main()
    assert (source/'raw.jsonl').read_bytes() == b'changed'
    assert (destination/'raw.jsonl').read_bytes() != b'changed'


def test_prepare_waits_for_an_active_source_reference(prepared_source, monkeypatch):
    source, _destination, _workspace = prepared_source
    monkeypatch.setattr(worker, 'process_references_many', lambda _paths: ['active'])
    with pytest.raises(BlockingIOError, match='active process'):
        worker.main()
    assert not (source/'.download.lock').exists()


def test_observer_exposes_source_bytes_only_while_packing(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    status = {'contract': worker.CONTRACT, 'dataset': worker.DATASET,
              'state': 'running', 'source_identity': 'a'*64,
              'C_payload_deleted': False, 'D_archive_committed': False,
              'files_total': 8, 'logical_bytes_total': 100,
              'progress': {'stage': 'streaming_source_into_E_packs',
                           'files_completed': 2, 'logical_bytes_processed': 30}}
    (root/(worker.DATASET+'.status.json')).write_text(json.dumps(status))
    row, = storage_progress.offline_repair_archive_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['completed_bytes'] == 30 and row['total_bytes'] == 100
    assert row['complete_evidence'] is False
    status['progress'] = {'stage': 'writing_E_inventory', 'inventory_items_completed': 9,
                          'inventory_items_total': 15}
    (root/(worker.DATASET+'.status.json')).write_text(json.dumps(status))
    row, = storage_progress.offline_repair_archive_tasks(tmp_path, workspace_checked=True)
    assert row['completed_bytes'] is None and row['total_bytes'] is None
    assert row['completed_items'] == 9 and row['total_items'] == 15


def test_offline_handoff_recovers_an_interrupted_atomic_exchange(prepared_source):
    source, destination, workspace = prepared_source
    original_inode = source.stat().st_ino
    worker.main()
    archive_root = workspace/'migration/handoff-index-archive-v1'
    status = json.loads((archive_root/(worker.DATASET+'.status.json')).read_bytes())
    identity = status['source_identity']
    stage = archive_root/worker.DATASET/identity
    index = workspace/'migration'/worker.queue.OFFLINE_REPAIR_SCOPE[0]/'files.sqlite'
    lock_inode = (source/'.download.lock').stat().st_ino
    kwargs = dict(original_inode=original_inode, lock_inode=lock_inode, process_check=lambda _paths: [])
    def crash():
        raise OSError('crash immediately after exchange')
    with pytest.raises(OSError, match='after exchange'):
        offline_repair_handoff.handoff(source, destination, index, stage, worker.DATASET,
                                       identity, after_exchange=crash, **kwargs)
    assert source.is_symlink() and (source/'raw.jsonl').read_bytes() == (destination/'raw.jsonl').read_bytes()
    result = offline_repair_handoff.handoff(source, destination, index, stage, worker.DATASET, identity, **kwargs)
    assert result['state'] == 'C_payload_retired' and result['C_payload_deleted'] is True
    assert result['C_retirement']['files_removed'] == 2
    assert result['destination_independent_readback'] is False and result['NAS_accepted'] is False
    assert source.is_symlink() and (source/'raw.jsonl').is_file()
    assert not (Path(result['preserved'])/'raw.jsonl').exists()
    assert offline_repair_handoff.handoff(source, destination, index, stage, worker.DATASET,
                                          identity, **kwargs)['state'] == 'C_payload_retired'


def test_offline_handoff_rejects_changed_e_generation_before_exchange(prepared_source):
    source, destination, workspace = prepared_source
    original_inode = source.stat().st_ino
    worker.main()
    archive_root = workspace/'migration/handoff-index-archive-v1'
    status = json.loads((archive_root/(worker.DATASET+'.status.json')).read_bytes())
    identity = status['source_identity']
    index = workspace/'migration'/worker.queue.OFFLINE_REPAIR_SCOPE[0]/'files.sqlite'
    (destination/'raw.jsonl').write_bytes(b'changed')
    with pytest.raises(Exception, match='E file generation differs'):
        offline_repair_handoff.handoff(source, destination, index, archive_root/worker.DATASET/identity,
            worker.DATASET, identity, original_inode=original_inode,
            lock_inode=(source/'.download.lock').stat().st_ino, process_check=lambda _paths: [])
    assert source.is_dir() and not source.is_symlink() and (source/'raw.jsonl').is_file()


def test_scheduled_retirement_keeps_old_path_readable_on_e(prepared_source, tmp_path, monkeypatch):
    source, destination, workspace = prepared_source
    worker.main()
    owner = tmp_path/'owner.lock'
    owner.touch(mode=0o600)
    monkeypatch.setattr(retire, 'COMMON_OWNER', owner)
    monkeypatch.setattr(retire, 'ROOT', worker.queue.ROOT)
    monkeypatch.setattr(retire, 'artifact_process_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'active_configuration_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'artifact_service_references', lambda *_args: {str(source): []})
    retire.main()
    status_path = workspace/'migration/handoff-index-archive-v1'/(worker.DATASET+'.status.json')
    status = json.loads(status_path.read_bytes())
    assert status['state'] == 'C_payload_retired' and status['C_payload_deleted'] is True
    assert source.is_symlink() and source.resolve() == destination
    assert (source/'raw.jsonl').read_bytes() == b'{"raw":-1}\n'*1000
    assert status['D_archive_committed'] is False and status['NAS_accepted'] is False
    handoff, drow = storage_progress.offline_repair_followup_tasks(workspace, workspace_checked=True)
    assert handoff['state'] == 'completed' and handoff['complete_evidence'] is True
    assert drow['state'] == 'waiting' and drow['complete_evidence'] is False
    worker.main()  # Future archive timer runs do not rewrite the retired generation.
    retire.main()  # Handoff itself is idempotent.


def test_second_offline_dataset_uses_same_restartable_handoff(prepared_source, tmp_path, monkeypatch):
    source, destination, workspace = prepared_source
    dataset = worker.ROLLBACK_DATASET
    # The independent provenance/scope test covers the real rollback receipt;
    # this tiny source exercises the shared archive and retirement owner.
    monkeypatch.setitem(worker.DATASET_SCOPES, dataset, worker.queue.OFFLINE_REPAIR_SCOPE)
    worker.main(dataset)
    owner = tmp_path/'owner.lock'
    owner.touch(mode=0o600)
    monkeypatch.setattr(retire, 'COMMON_OWNER', owner)
    monkeypatch.setattr(retire, 'ROOT', worker.queue.ROOT)
    monkeypatch.setattr(retire, 'artifact_process_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'active_configuration_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'artifact_service_references', lambda *_args: {str(source): []})
    retire.main(dataset)
    status = json.loads((workspace/'migration/handoff-index-archive-v1'/(dataset+'.status.json')).read_bytes())
    assert retire.retired_status_matches(status, dataset)
    assert source.is_symlink() and source.resolve() == destination
    assert (source/'raw.jsonl').read_bytes() == b'{"raw":-1}\n'*1000
    assert status['D_archive_committed'] is False and status['NAS_accepted'] is False
    worker.main(dataset)
    retire.main(dataset)  # Handoff itself is idempotent.


@pytest.mark.parametrize('owner', [worker, retire])
def test_offline_queue_attempts_other_dataset_after_a_failure(tmp_path, monkeypatch, owner):
    scopes = worker.DATASET_SCOPES
    items = [{'name': scope[0], 'source': scope[1], 'marker': scope[2]} for scope in scopes.values()]
    monkeypatch.setattr(worker.queue, 'load_sources', lambda: items)
    monkeypatch.setattr(worker.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(worker.workspace_volume, 'check', lambda **_kwargs: {})
    root = tmp_path/'migration/handoff-index-archive-v1'
    root.mkdir(parents=True)
    for dataset in scopes:
        (root/(dataset+'.status.json')).write_text(json.dumps({'state': 'E_index_source_hashed_prepared'}))
    report = tmp_path/'report.json'; report.write_text('{}')
    monkeypatch.setattr(worker.queue, 'paths', lambda *_args: (None, None, None, report))
    seen = []
    def run(dataset):
        seen.append(dataset)
        if dataset == worker.DATASET:
            raise BlockingIOError('owner busy')
    monkeypatch.setattr(owner, 'main', run)
    with pytest.raises(Exception, match='needs retry'):
        owner.run_enrolled()
    assert seen == [worker.DATASET, worker.ROLLBACK_DATASET]


@pytest.mark.parametrize('dataset', [worker.DATASET, worker.ROLLBACK_DATASET])
def test_retired_candidate_uses_canonical_private_d_publication(prepared_source, tmp_path, monkeypatch, dataset):
    from types import SimpleNamespace
    source, destination, workspace = prepared_source
    if dataset == worker.ROLLBACK_DATASET:
        monkeypatch.setitem(worker.DATASET_SCOPES, dataset, worker.queue.OFFLINE_REPAIR_SCOPE)
    worker.main(dataset)
    owner = tmp_path/'owner.lock'
    owner.touch(mode=0o600)
    monkeypatch.setattr(retire, 'COMMON_OWNER', owner)
    monkeypatch.setattr(retire, 'ROOT', worker.queue.ROOT)
    monkeypatch.setattr(retire, 'artifact_process_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'active_configuration_references', lambda *_args: [])
    monkeypatch.setattr(retire, 'artifact_service_references', lambda *_args: {str(source): []})
    retire.main(dataset)
    cold, private = tmp_path/'cold', tmp_path/'private'
    cold.mkdir()
    packed_snapshots.initialize_packed_layout(private, node_id='test')
    monkeypatch.setattr(publisher, 'COLD', cold)
    monkeypatch.setattr(publisher, '_check_d_primary_mount', lambda *_args: None)
    monkeypatch.setattr(publisher, 'private_history_root', lambda *_args, **_kwargs: private)
    monkeypatch.setattr(publisher, 'version_policy', lambda *_args: {})
    monkeypatch.setattr(publisher.shutil, 'disk_usage', lambda *_args: SimpleNamespace(
        free=publisher.D_INSTALL_RESERVE+100_000_000))
    result = publisher.publish(dataset)
    assert d_queue._done(result, dataset)
    assert result['D_archive_committed'] and result['D_E_stage_cleanup_complete']
    assert result['D_publication']['NAS_accepted'] is False
    handoff, drow = storage_progress.offline_repair_followup_tasks(workspace, workspace_checked=True, dataset=dataset)
    assert handoff['state'] == drow['state'] == 'completed'
    assert drow['complete_evidence'] is True
    assert storage_progress.offline_repair_archive_tasks(workspace, workspace_checked=True, dataset=dataset)[0]['state'] == 'completed'
    assert source.is_symlink() and source.resolve() == destination
    release = packed_snapshots.resolve_latest_packed(private, dataset)
    assert packed_snapshots.verify_packed_snapshot(private, release, reconstruct_all=True)['independently_reconstructed_files'] == 2
    assert publisher.publish(dataset)['D_archive_committed']
