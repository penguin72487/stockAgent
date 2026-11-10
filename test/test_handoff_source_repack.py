import hashlib
import json
import sqlite3
import zipfile
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from stockagent.data_sync import handoff_source_repack as handoff, packed_snapshots as packed
from stockagent.data_sync import stream_repack, workspace_relocation
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import prepare_handoff_source_repack as cli


def fixture(tmp_path, monkeypatch):
    source, cold, stage = tmp_path/'C-source', tmp_path/'D-private', tmp_path/'E-stage'
    (source/'nested').mkdir(parents=True)
    (source/'.download.lock').write_bytes(b'')
    (source/'nested/a.parquet').write_bytes(b'first-value'*4)
    (source/'nested/b.bin').write_bytes(b'second-value'*4)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=8)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    index = tmp_path/'files.sqlite'
    db = sqlite3.connect(index)
    db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)')
    for name, kind, signature in workspace_relocation.walk(source):
        data = (source/name).read_bytes() if kind == 'file' else b''
        digest = hashlib.sha256(data).hexdigest() if kind == 'file' else None
        db.execute('INSERT INTO files VALUES (?,?,?,?,?,?)',
                   (name, kind, json.dumps(signature), digest, len(data), 'verified' if digest else 'pending'))
    db.commit()
    db.close()
    kwargs = {'original_inode': source.stat().st_ino,
              'original_lock_inode': (source/'.download.lock').stat().st_ino}
    return source, cold, stage, index, old, kwargs


def _simulate_c_device_renumber(source, index, monkeypatch):
    from stockagent.data_sync import workspace_source_handoff
    root_signature = workspace_relocation.signature(source)
    prior_device = root_signature[0] + 1000000
    old_root = [prior_device, *root_signature[1:]]
    (index.parent/'intent.json').write_text(json.dumps({
        'contract': 'workspace_exact_relocation_v1', 'source_root_signature': old_root}))
    with sqlite3.connect(index) as db:
        for name, encoded in db.execute('SELECT path,sig FROM files').fetchall():
            signature = json.loads(encoded)
            signature[0] = prior_device
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(signature), name))
    monkeypatch.setattr(workspace_source_handoff, '_canonical_c_source',
                        lambda path, dev: path == source and dev == source.stat().st_dev)


def test_fixed_source_repack_and_retirement_survive_c_device_renumber(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    prepared = handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    assert prepared['source_member_sha_streamed']
    result = handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and not (source/'nested/a.parquet').exists()


def test_device_renumber_never_accepts_a_different_source_inode(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    file = source/'nested/a.parquet'
    copy = source/'nested/replacement'
    copy.write_bytes(file.read_bytes())
    copy.replace(file)
    with pytest.raises(SnapshotError, match='generation changed'):
        handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)


def test_interrupted_retirement_recovers_after_another_device_renumber(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    _simulate_c_device_renumber(source, index, monkeypatch)
    handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    unlink = type(source).unlink
    def interrupted(path, *args, **options):
        result = unlink(path, *args, **options)
        if path == source/'nested/a.parquet':
            raise OSError('interrupted after unlink')
        return result
    monkeypatch.setattr(type(source), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: [])
    monkeypatch.setattr(type(source), 'unlink', unlink)
    with sqlite3.connect(stage/'C-payload-retirement.sqlite') as db:
        db.execute('UPDATE meta SET value=? WHERE key=?', (json.dumps(source.stat().st_dev + 2000000), 'source_device'))
    result = handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and not (source/'nested/b.bin').exists()


def test_handoff_source_packs_exact_fixed_private_release_on_E(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    assert prepared['source_mode'] == 'retained_handoff_C_original'
    assert prepared['source_member_sha_streamed'] and not prepared['destination_independent_readback']
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256) == prepared
    with zipfile.ZipFile(stage/prepared['packages'][0]['name']) as archive:
        assert archive.read('nested/a.parquet') == (source/'nested/a.parquet').read_bytes()


def test_handoff_source_rejects_changed_C_member_before_preparation(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    (source/'nested/a.parquet').write_bytes(b'changed-value')
    with pytest.raises(SnapshotError, match='generation changed'):
        handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    assert not (stage/'prepared.json').exists()
    assert (source/'nested/b.bin').is_file()


def test_handoff_source_resumes_sealed_E_packs(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    consume = stream_repack._PackBuilder.consume
    def interrupted(self, stream, row):
        if self.checkpoint.packages:
            raise OSError('interrupted after first sealed pack')
        return consume(self, stream, row)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupted)
    with pytest.raises(OSError, match='interrupted after first sealed pack'):
        handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', consume)
    result = handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    assert result['files'] == old.manifest['source']['files']
    assert len(result['packages']) >= 2


def test_handoff_completed_E_resumes_without_reopening_changed_D_head(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    def changed_head(*_a, **_kw):
        raise AssertionError('an already sealed E generation must not reopen the D head')
    monkeypatch.setattr(packed, 'resolve_latest_packed', changed_head)
    (source/'nested/a.parquet').unlink()  # interrupted C retirement
    assert handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs) == prepared


def test_handoff_retirement_keeps_source_root_lock_and_E_packages(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    result = handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['state'] == 'C_payload_retired_root_and_lock_preserved'
    assert result['files_removed'] == 2 and result['estimated_allocated_bytes_reclaimed'] > 0
    assert source.is_dir() and (source/'.download.lock').is_file()
    assert not (source/'nested/a.parquet').exists() and not (source/'nested/b.bin').exists()
    assert stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_handoff_retirement_resumes_exact_pending_batch_after_unlink(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    original = type(source).unlink
    interrupted = False
    def after_unlink(path, *args, **kw):
        nonlocal interrupted
        result = original(path, *args, **kw)
        if path == source/'nested/a.parquet' and not interrupted:
            interrupted = True
            raise OSError('after C unlink before journal completion')
        return result
    monkeypatch.setattr(type(source), 'unlink', after_unlink)
    with pytest.raises(OSError, match='after C unlink'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: [])
    assert not (source/'nested/a.parquet').exists()
    monkeypatch.setattr(type(source), 'unlink', original)
    result = handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                            **kwargs, process_check=lambda _paths: [])
    assert result['files_removed'] == 2 and not (source/'nested/b.bin').exists()
    assert (source/'.download.lock').is_file()


def test_handoff_retirement_rejects_changed_source_or_live_reader(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    with pytest.raises(SnapshotError, match='running process'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: ['reader'])
    (source/'nested/a.parquet').write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: [])
    assert (source/'nested/b.bin').is_file()


def test_handoff_retirement_rejects_substituted_equal_size_journal_member(tmp_path, monkeypatch):
    source, cold, stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    handoff.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    with pytest.raises(OSError, match='interrupted'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index, **kwargs,
                       process_check=lambda _paths: [], guard=lambda: (_ for _ in ()).throw(OSError('interrupted')))
    # Interrupt after the journal was created, before unlinks.
    calls = 0
    def guard():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('interrupted')
    with pytest.raises(OSError, match='interrupted'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: [], guard=guard)
    with sqlite3.connect(stage/'C-payload-retirement.sqlite') as db:
        db.execute("UPDATE files SET path='nested/x.parquet' WHERE path='nested/a.parquet'")
    with pytest.raises(SnapshotError, match='plan differs'):
        handoff.retire(source, stage, 'fixture', old.manifest_sha256, index,
                       **kwargs, process_check=lambda _paths: [])
    assert (source/'nested/a.parquet').exists() and (source/'nested/b.bin').exists()


@pytest.mark.parametrize('retire', [False, True])
def test_handoff_cli_preserves_private_D_and_controls_C_retirement(tmp_path, monkeypatch, retire):
    source, cold, _stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    workspace = tmp_path/'workspace'
    migration = workspace/'migration'
    migration.mkdir(parents=True)
    evidence = tmp_path/'evidence'
    evidence.mkdir()
    (evidence/'files.sqlite').write_bytes(index.read_bytes())
    config = {'mount_point': str(workspace), 'minimum_free_bytes': 0}
    monkeypatch.setattr(cli, 'ENROLLED', {'fixture-source': 'fixture'})
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cli.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(cli.workspace_volume, 'path', lambda *_a, **_kw: migration)
    monkeypatch.setattr(cli, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(cli, 'private_history_root', lambda *_a, **_kw: cold)
    monkeypatch.setattr(cli, 'version_policy', lambda *_a: {})
    monkeypatch.setattr(cli.provider_workspace, 'exact_mount', lambda *_a: True)
    monkeypatch.setattr(cli.handoff, 'read_journal', lambda: {
        'state': 'active', 'destination': str(tmp_path/'destination'), 'evidence': str(evidence),
        'files': old.manifest['source']['files'], 'logical_bytes': old.manifest['source']['logical_bytes'],
        **kwargs})
    monkeypatch.setattr(cli.handoff, '_assert_retained', lambda *_a: None)
    monkeypatch.setattr(cli.handoff, '_scope', lambda: SimpleNamespace(source=tmp_path/'alias', backup=source))
    @contextmanager
    def context(*_a, **_kw):
        yield
    @contextmanager
    def reserve(*_a, **_kw):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(cli.handoff, 'selected_source', context)
    monkeypatch.setattr(cli.handoff, 'lock_file', context)
    monkeypatch.setattr(cli.handoff, '_lock_writer_roots', lambda *_a: None)
    monkeypatch.setattr(cli, 'reserve_capacity', reserve)
    monkeypatch.setattr(cli.sys, 'argv', ['prepare', '--source', 'fixture-source'] + (['--retire'] if retire else []))
    assert cli.main() == 0
    status = json.loads((migration/'handoff-source-repack-v1/fixture.status.json').read_text())
    assert status['state'] == ('C_payload_retired_root_and_lock_preserved' if retire else 'E_source_hashed_prepared')
    assert status['C_payload_deleted'] is retire
    assert not status['D_new_head_committed'] and not status['NAS_accepted']
    assert (source/'nested/a.parquet').is_file() is (not retire)
    assert (source/'.download.lock').is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    if not retire:
        def changed_head(*_a, **_kw):
            raise AssertionError('completed E stage must not need the latest D head')
        monkeypatch.setattr(packed, 'resolve_latest_packed', changed_head)
        monkeypatch.setattr(cli.sys, 'argv', ['prepare', '--source', 'fixture-source', '--retire'])
        assert cli.main() == 0
        status = json.loads((migration/'handoff-source-repack-v1/fixture.status.json').read_text())
        assert status['state'] == 'C_payload_retired_root_and_lock_preserved'
        assert status['original_manifest_sha256'] == old.manifest_sha256
