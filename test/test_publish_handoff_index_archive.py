import json
import zipfile
from types import SimpleNamespace

import pytest

from scripts import publish_handoff_index_archive as cli
from stockagent.data_sync import handoff_index_archive, handoff_source_repack, index_archive_existing, packed_snapshots, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_handoff_source_repack import fixture


def setup_cli(tmp_path, monkeypatch, *, supplemental=False):
    source, public, _stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    workspace = tmp_path/'workspace'
    migration = workspace/'migration'
    evidence = tmp_path/'evidence'
    evidence.mkdir()
    (evidence/'files.sqlite').write_bytes(index.read_bytes())
    index = evidence/'files.sqlite'
    stage = migration/'handoff-index-archive-v1/fixture'/('a'*64)
    prepared = handoff_index_archive.prepare(source, stage, 'fixture', 'a'*64, index,
                                              **kwargs, preserved_paths=['.download.lock'], supplemental_root_lock=supplemental)
    retired = handoff_source_repack.retire(source, stage, 'fixture', 'a'*64, index,
                                            **kwargs, process_check=lambda _paths: [])
    status = stage.parent.parent/'fixture.status.json'
    status.write_text(json.dumps({'contract': cli.source_cli.CONTRACT, 'dataset': 'fixture',
        'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
        'source_identity': 'a'*64, 'prepared_sha256': prepared['identity_sha256'],
        'C_retirement': retired, 'D_archive_committed': False}))
    private = tmp_path/'private-D'
    packed_snapshots.initialize_packed_layout(private, node_id='test')
    config = {'mount_point': str(workspace)}
    monkeypatch.setattr(cli.source_cli, 'ENROLLED', {'fixture-source': ('fixture', ['.download.lock'], supplemental)})
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cli.workspace_volume, 'path', lambda *_a, **_kw: migration)
    monkeypatch.setattr(cli, 'COLD', public)
    monkeypatch.setattr(cli, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(cli, 'private_history_root', lambda *_a, **_kw: private)
    monkeypatch.setattr(cli, 'version_policy', lambda *_a: {})
    monkeypatch.setattr(cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=cli.D_INSTALL_RESERVE+100_000_000))
    from contextlib import contextmanager
    @contextmanager
    def selected(*_a):
        yield
    monkeypatch.setattr(cli.handoff, 'selected_source', selected)
    monkeypatch.setattr(cli.handoff, 'read_journal', lambda: {'state': 'active', 'evidence': str(evidence)})
    monkeypatch.setattr(cli.handoff, '_assert_retained', lambda *_a: None)
    monkeypatch.setattr(cli.handoff, '_scope', lambda: SimpleNamespace(source=tmp_path/'data_fixture'))
    return public, private, stage, status, old


def test_index_source_moves_to_private_D_without_changing_public_dataset(tmp_path, monkeypatch):
    public, private, stage, status, old = setup_cli(tmp_path, monkeypatch)
    result = cli.publish('fixture')
    assert result['D_archive_committed'] and result['D_E_stage_cleanup_complete']
    assert not result['D_publication']['public_dataset_merged'] and not result['D_publication']['NAS_accepted']
    assert packed_snapshots.resolve_latest_packed(public, 'fixture').manifest_sha256 == old.manifest_sha256
    assert packed_snapshots.resolve_latest_packed(private, 'fixture').manifest_sha256 == result['D_publication']['new_manifest_sha256']
    assert not list(stage.glob('pack-*.zip')) and json.loads(status.read_text())['D_state'] == 'completed'


def test_index_source_D_capacity_failure_preserves_E_then_resumes(tmp_path, monkeypatch):
    _public, _private, stage, _status, _old = setup_cli(tmp_path, monkeypatch)
    monkeypatch.setattr(cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=cli.D_INSTALL_RESERVE))
    with pytest.raises(SnapshotError, match='below source-index install'):
        cli.publish('fixture')
    assert stream_repack._read_prepared(stage, 'fixture', 'a'*64)
    monkeypatch.setattr(cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=cli.D_INSTALL_RESERVE+100_000_000))
    assert cli.publish('fixture')['D_E_stage_cleanup_complete']


def test_index_source_D_head_commit_and_cleanup_can_resume(tmp_path, monkeypatch):
    _public, private, _stage, status, _old = setup_cli(tmp_path, monkeypatch)
    original = stream_repack.cleanup_stage
    monkeypatch.setattr(stream_repack, 'cleanup_stage', lambda *_a, **_k: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        cli.publish('fixture')
    saved = json.loads(status.read_text())
    assert saved['D_archive_committed'] and saved['D_state'] == 'head_committed'
    monkeypatch.setattr(stream_repack, 'cleanup_stage', original)
    assert cli.publish('fixture')['D_E_stage_cleanup_complete']
    assert packed_snapshots.resolve_latest_packed(private, 'fixture').manifest_sha256 == saved['D_publication']['new_manifest_sha256']


def test_index_source_reuses_existing_D_head_and_resumes_cleanup(tmp_path, monkeypatch):
    _public, private, stage, status, _old = setup_cli(tmp_path, monkeypatch, supplemental=True)
    source = tmp_path/'data_fixture'
    source.mkdir()
    for package in stage.glob('pack-*.zip'):
        with zipfile.ZipFile(package) as archive:
            archive.extractall(source)
    head = packed_snapshots.publish_packed_snapshot(private, 'fixture', source, node_id='test',
                                                    excluded_subtrees=['.download.lock'])
    monkeypatch.setattr(index_archive_existing, 'REVIEWED', {'fixture': ('data_fixture', ('.download.lock',))})
    cleanup = index_archive_existing.cleanup
    monkeypatch.setattr(index_archive_existing, 'cleanup', lambda *_a, **_k: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        cli.publish('fixture')
    saved = json.loads(status.read_text())
    assert saved['D_archive_committed'] and saved['D_state'] == 'existing_head_matched'
    assert saved['D_publication']['new_manifest_sha256'] == head.manifest_sha256
    assert cli.queue._done(saved, 'fixture')
    from scripts import run_handoff_index_d_queue
    assert not run_handoff_index_d_queue._done(saved, 'fixture')  # E cleanup has not completed.
    monkeypatch.setattr(index_archive_existing, 'cleanup', cleanup)
    result = cli.publish('fixture')
    assert result['D_E_stage_cleanup_complete'] and not list(stage.glob('pack-*.zip'))
    assert run_handoff_index_d_queue._done(result, 'fixture')
    result['D_publication']['existing_match']['matched_items'] += 1
    assert not run_handoff_index_d_queue._done(result, 'fixture')
    assert packed_snapshots.resolve_latest_packed(private, 'fixture').manifest_sha256 == head.manifest_sha256


def test_index_source_reuses_pinned_parent_without_writing_duplicate_objects(tmp_path, monkeypatch):
    _public, private, stage, _status, _old = setup_cli(tmp_path, monkeypatch, supplemental=True)
    parent = tmp_path/'parent-source'
    bodies = parent/'bodies'
    bodies.mkdir(parents=True)
    for package in stage.glob('pack-*.zip'):
        with zipfile.ZipFile(package) as archive:
            archive.extractall(bodies)
    (parent/'other.txt').write_text('other parent data')
    head = packed_snapshots.publish_packed_snapshot(private, 'parent', parent, node_id='test')
    pins = tmp_path/'pins'
    pins.mkdir()
    monkeypatch.setattr(index_archive_existing, 'REVIEWED', {'fixture': ('data_fixture', ('.download.lock',))})
    monkeypatch.setattr(index_archive_existing, 'SUBTREE_COVERAGES', {'fixture': ('parent', parent.name, 'bodies/')})
    monkeypatch.setattr(index_archive_existing, '_materialized_root', lambda: pins)
    monkeypatch.setattr(cli.index_archive_publish, 'publish',
                        lambda *_a, **_k: pytest.fail('covered payload must not be written again'))
    result = cli.publish('fixture')
    from scripts import run_handoff_index_d_queue
    assert run_handoff_index_d_queue._done(result, 'fixture')
    assert result['D_publication']['existing_match']['covering_dataset'] == 'parent'
    assert result['D_publication']['new_manifest_sha256'] == head.manifest_sha256
    assert not list(stage.glob('pack-*.zip')) and not (private/'heads/fixture').exists()
    pin = index_archive_existing._coverage_pin(result['D_publication']['existing_match'], stage)
    pin.unlink()
    assert not run_handoff_index_d_queue._done(result, 'fixture')
    resumed = cli.publish('fixture')
    assert pin.is_file() and run_handoff_index_d_queue._done(resumed, 'fixture')


def test_index_source_reuses_public_cover_without_private_duplicate(tmp_path, monkeypatch):
    public, private, stage, _status, _old = setup_cli(tmp_path, monkeypatch, supplemental=True)
    source = tmp_path/'data_fixture'
    source.mkdir()
    for package in stage.glob('pack-*.zip'):
        with zipfile.ZipFile(package) as archive:
            archive.extractall(source)
    head = packed_snapshots.publish_packed_snapshot(public, 'cover', source, node_id='test',
                                                    excluded_subtrees=['.download.lock'])
    pins = tmp_path/'pins'; pins.mkdir()
    monkeypatch.setattr(index_archive_existing, 'REVIEWED', {'fixture': ('data_fixture', ('.download.lock',))})
    monkeypatch.setattr(index_archive_existing, 'SUBTREE_COVERAGES', {'fixture': ('cover', source.name, '')})
    monkeypatch.setattr(index_archive_existing, 'PUBLIC_COVERAGE_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(index_archive_existing, '_materialized_root', lambda: pins)
    monkeypatch.setattr(index_archive_existing, '_covering_root', lambda _cold, _dataset: public)
    monkeypatch.setattr(cli.index_archive_publish, 'publish',
                        lambda *_a, **_k: pytest.fail('covered public payload must not be copied'))
    result = cli.publish('fixture')
    from scripts import run_handoff_index_d_queue
    assert run_handoff_index_d_queue._done(result, 'fixture')
    assert result['D_publication']['existing_match']['covering_authority'] == 'public'
    assert result['D_publication']['new_manifest_sha256'] == head.manifest_sha256
    assert not (private/'heads/fixture').exists() and not list(stage.glob('pack-*.zip'))
