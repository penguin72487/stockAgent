import os
import json
import sqlite3
from pathlib import Path

import pytest

from stockagent.data_sync import handoff_index_archive, index_archive_publish, offline_replay_pairs as pairs
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import run_offline_replay_pair_migration as runner


def fixture(tmp_path):
    source, mirror = tmp_path/'C/source', tmp_path/'C/mirror'
    for root in (source, mirror):
        (root/'nested').mkdir(parents=True)
    (source/'.download.lock').touch()
    for name, data in [('nested/a', b'a'*123), ('nested/b', b'b'*456)]:
        (source/name).write_bytes(data)
        os.link(source/name, mirror/name)
    work = tmp_path/'E'
    work.mkdir()
    evidence, destination, stage = work/'evidence', work/'empty-destination', work/'stage'
    assert pairs.check_pair(source, mirror)['files'] == 2
    index = pairs.hash_index(source, destination, evidence)
    prepared = handoff_index_archive.prepare(source, stage, 'replay-fixture', 'a'*64, index,
        original_inode=source.stat().st_ino, original_lock_inode=(source/'.download.lock').stat().st_ino,
        preserved_paths=['.download.lock'])
    private = tmp_path/'D'
    packed.initialize_packed_layout(private, node_id='test')
    result = index_archive_publish.publish(private, stage, 'replay-fixture', 'a'*64, index, 'source')
    binding = {'source_root': [source.stat().st_dev, source.stat().st_ino],
               'mirror_root': [mirror.stat().st_dev, mirror.stat().st_ino],
               'prepared_sha256': prepared['identity_sha256'], 'manifest_sha256': result['new_manifest_sha256']}
    def guard():
        if packed.resolve_latest_packed(private, 'replay-fixture').manifest_sha256 != binding['manifest_sha256']:
            raise SnapshotError('D head changed')
    return source, mirror, index, stage, private, binding, guard


def test_paired_retirement_keeps_a_real_dataset_archive(tmp_path):
    source, mirror, index, stage, private, binding, guard = fixture(tmp_path)
    result = pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    assert result['state'] == 'paired_payload_retired' and result['files_removed'] == 2
    assert not (source/'nested/a').exists() and not (mirror/'nested/a').exists()
    assert (source/'.download.lock').exists()
    current = packed.resolve_latest_packed(private, 'replay-fixture')
    restored = packed.fetch_packed_snapshot(private, tmp_path/'restored', current)
    assert (restored/'nested/a').read_bytes() == b'a'*123
    assert (restored/'nested/b').read_bytes() == b'b'*456
    assert pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard) == result


def test_retirement_publication_guard_rejects_changed_installed_D_generation(tmp_path):
    _source, _mirror, _index, stage, private, binding, _guard = fixture(tmp_path)
    prepared = runner.stream_repack._read_prepared(stage, 'replay-fixture', 'a'*64)
    runner._fixed_publication(private, stage, 'replay-fixture', 'a'*64, prepared, binding['manifest_sha256'])
    current = packed.resolve_latest_packed(private, 'replay-fixture')
    target = private/current.manifest['archive']['objects'][0]['relpath']
    data = target.read_bytes()
    target.write_bytes(bytes([data[0] ^ 1])+data[1:])
    with pytest.raises(SnapshotError, match='object generation changed'):
        runner._fixed_publication(private, stage, 'replay-fixture', 'a'*64, prepared, binding['manifest_sha256'])


@pytest.mark.parametrize('side', ['source', 'mirror'])
def test_paired_retirement_resumes_after_each_unlink(tmp_path, monkeypatch, side):
    source, mirror, index, stage, _private, binding, guard = fixture(tmp_path)
    target = (source if side == 'source' else mirror)/'nested/a'
    original = Path.unlink
    interrupted = False
    def unlink(path, *args, **kwargs):
        nonlocal interrupted
        result = original(path, *args, **kwargs)
        if path == target and not interrupted:
            interrupted = True
            raise OSError('power loss after unlink')
        return result
    monkeypatch.setattr(Path, 'unlink', unlink)
    with pytest.raises(OSError, match='power loss'):
        pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    monkeypatch.setattr(Path, 'unlink', original)
    result = pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    assert result['files_removed'] == 2 and not (source/'nested/b').exists()
    assert not (mirror/'nested/b').exists()


def test_rejects_external_hardlink_before_archiving(tmp_path):
    source, mirror = tmp_path/'s', tmp_path/'m'
    source.mkdir(); mirror.mkdir()
    (source/'.download.lock').touch()
    (source/'a').write_bytes(b'data')
    os.link(source/'a', mirror/'a')
    os.link(source/'a', tmp_path/'third-link')
    with pytest.raises(SnapshotError, match='paired hardlinks'):
        pairs.check_pair(source, mirror)
    assert (source/'a').exists() and (mirror/'a').exists()


def test_interrupted_inventory_is_replanned_without_touching_C(tmp_path, monkeypatch):
    source, mirror = tmp_path/'s', tmp_path/'m'
    source.mkdir(); mirror.mkdir()
    (source/'.download.lock').touch()
    (source/'a').write_bytes(b'data')
    os.link(source/'a', mirror/'a')
    original = pairs.relocation.walk
    def interrupted(root, **kwargs):
        for item in original(root, **kwargs):
            yield item
            raise OSError('inventory interrupted')
    monkeypatch.setattr(pairs.relocation, 'walk', interrupted)
    with pytest.raises(OSError, match='inventory interrupted'):
        pairs.hash_index(source, tmp_path/'E/destination', tmp_path/'E/evidence')
    monkeypatch.setattr(pairs.relocation, 'walk', original)
    assert pairs.hash_index(source, tmp_path/'E/destination', tmp_path/'E/evidence').is_file()
    assert (source/'a').read_bytes() == b'data' and (mirror/'a').read_bytes() == b'data'


def test_hash_progress_updates_within_a_large_member(tmp_path):
    source, mirror = tmp_path/'s', tmp_path/'m'
    source.mkdir(); mirror.mkdir()
    (source/'.download.lock').touch()
    (source/'a').write_bytes(b'x'*(9*1024**2))
    os.link(source/'a', mirror/'a')
    events = []
    pairs.hash_index(source, tmp_path/'E/destination', tmp_path/'E/evidence', progress=events.append)
    assert any(0 < row['logical_bytes_processed'] < row['logical_bytes_total'] for row in events)


def test_fixed_index_resumes_only_trusted_C_device_renumbering(tmp_path, monkeypatch):
    source, mirror = tmp_path/'s', tmp_path/'m'
    source.mkdir(); mirror.mkdir()
    (source/'.download.lock').touch()
    (source/'a').write_bytes(b'data')
    os.link(source/'a', mirror/'a')
    evidence, destination = tmp_path/'E/evidence', tmp_path/'E/destination'
    index = pairs.hash_index(source, destination, evidence)
    old_device = source.stat().st_dev + 1000000
    intent = evidence/'intent.json'
    body = json.loads(intent.read_text())
    body['source_root_signature'][0] = old_device
    intent.write_text(json.dumps(body))
    with sqlite3.connect(index) as db:
        for name, encoded in db.execute('SELECT path,sig FROM files').fetchall():
            sig = json.loads(encoded); sig[0] = old_device
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(sig), name))
    with pytest.raises(ValueError, match='source identity changed'):
        pairs.hash_index(source, destination, evidence)
    monkeypatch.setattr(pairs.handoff, '_canonical_c_source', lambda path, dev: path == source and dev == source.stat().st_dev)
    assert pairs.hash_index(source, destination, evidence) == index


def test_paired_retirement_recognizes_trusted_C_device_renumbering(tmp_path, monkeypatch):
    source, mirror, index, stage, _private, binding, guard = fixture(tmp_path)
    old_device = source.stat().st_dev + 1000000
    binding['source_root'][0] = binding['mirror_root'][0] = old_device
    with sqlite3.connect(index) as db:
        for name, encoded in db.execute('SELECT path,sig FROM files').fetchall():
            sig = json.loads(encoded); sig[0] = old_device
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(sig), name))
    with pytest.raises(SnapshotError, match='root identity changed'):
        pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    monkeypatch.setattr(pairs.handoff, '_canonical_c_source', lambda path, dev: path == source and dev == source.stat().st_dev)
    result = pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    assert result['files_removed'] == 2


def test_changed_survivor_and_active_consumer_block_retirement(tmp_path, monkeypatch):
    source, mirror, index, stage, _private, binding, guard = fixture(tmp_path)
    with pytest.raises(BlockingIOError):
        pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard,
                          consumer_guard=lambda: (_ for _ in ()).throw(BlockingIOError('active reader')))
    assert (source/'nested/a').exists()
    original = Path.unlink
    def interrupted(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == source/'nested/a':
            raise OSError('interrupt')
        return result
    monkeypatch.setattr(Path, 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupt'):
        pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    monkeypatch.setattr(Path, 'unlink', original)
    (mirror/'nested/a').write_bytes(b'z'*123)
    with pytest.raises(SnapshotError, match='survivor changed'):
        pairs.retire_pair(source, mirror, index, stage/'retire.sqlite', binding, guard=guard)
    assert (mirror/'nested/a').exists() and (source/'nested/b').exists()


def test_automatic_pipeline_resumes_after_interrupted_E_cleanup(tmp_path, monkeypatch):
    source_root, mirror_root = tmp_path/'C/live', tmp_path/'hot/live'
    for root in (source_root/'old-replay', mirror_root/'old-replay'):
        root.mkdir(parents=True)
    (source_root/'old-replay/file').write_bytes(b'historic replay')
    os.link(source_root/'old-replay/file', mirror_root/'old-replay/file')
    workspace = tmp_path/'E'
    workspace.mkdir()
    private = tmp_path/'D'
    packed.initialize_packed_layout(private, node_id='test')
    owner = tmp_path/'owner.lock'
    owner.touch(mode=0o600)
    monkeypatch.setattr(runner, 'SOURCE_ROOT', source_root)
    monkeypatch.setattr(runner, 'MIRROR_ROOT', mirror_root)
    monkeypatch.setattr(runner, 'ENROLLED', {'fixture-replay': 'old-replay'})
    monkeypatch.setattr(runner, 'COLD', private)
    monkeypatch.setattr(runner, 'COMMON_OWNER', owner)
    monkeypatch.setattr(runner, '_consumer_guard', lambda *_args: None)
    monkeypatch.setattr(runner, '_check_d_primary_mount', lambda *_args: None)
    monkeypatch.setattr(runner, 'private_history_root', lambda *_args, **_kwargs: private)
    monkeypatch.setattr(runner, 'version_policy', lambda *_args: {})
    monkeypatch.setattr(runner.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(runner.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    config = {'minimum_free_bytes': 0}
    original = runner.stream_repack.cleanup_stage
    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError('after E cleanup')
    monkeypatch.setattr(runner.stream_repack, 'cleanup_stage', interrupted)
    with pytest.raises(OSError, match='after E cleanup'):
        runner.migrate('fixture-replay', workspace, private, config)
    assert not (source_root/'old-replay/file').exists()
    monkeypatch.setattr(runner.stream_repack, 'cleanup_stage', original)
    result = runner.migrate('fixture-replay', workspace, private, config)
    assert result['state'] == 'completed' and result['C_payload_retired'] is True
    snapshot = packed.resolve_latest_packed(private, 'fixture-replay')
    restored = packed.fetch_packed_snapshot(private, tmp_path/'restored', snapshot)
    assert (restored/'file').read_bytes() == b'historic replay'


def test_queue_continues_after_one_dataset_is_blocked(tmp_path, monkeypatch):
    workspace, private = tmp_path/'E', tmp_path/'D'
    workspace.mkdir(); private.mkdir()
    monkeypatch.setattr(runner, 'ENROLLED', {'first': 'first', 'second': 'second'})
    monkeypatch.setattr(runner.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(runner.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(runner, '_check_d_primary_mount', lambda *_args: None)
    monkeypatch.setattr(runner, 'private_history_root', lambda *_args, **_kwargs: private)
    monkeypatch.setattr(runner, 'version_policy', lambda *_args: {})
    attempted = []
    def migrate(dataset, *_args):
        attempted.append(dataset)
        if dataset == 'first':
            raise BlockingIOError('active process consumer')
        return {'state': 'completed'}
    monkeypatch.setattr(runner, 'migrate', migrate)
    assert runner.main() == 1 and attempted == ['first', 'second']
    status = runner._load(workspace/'migration/offline-replay-pairs-v1/first/status.json')
    assert status['state'] == 'waiting_retry' and 'active process consumer' in status['error']


def test_completed_queue_does_not_probe_or_rewrite_D_on_each_timer_tick(tmp_path, monkeypatch):
    workspace = tmp_path/'E'
    queue = workspace/'migration/offline-replay-pairs-v1'
    for name in ('first', 'second'):
        path = queue/name/'status.json'
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'contract': runner.CONTRACT, 'dataset': name, 'state': 'completed',
            'D_archive_committed': True, 'D_manifest_sha256': 'a'*64, 'C_payload_retired': True,
            'C_retirement': {'state': 'paired_payload_retired'}, 'E_archive_cleanup': {'state': 'completed'}}))
    monkeypatch.setattr(runner, 'ENROLLED', {'first': 'first', 'second': 'second'})
    monkeypatch.setattr(runner.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(runner.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(runner, '_check_d_primary_mount', lambda *_args: pytest.fail('unneeded D probe'))
    monkeypatch.setattr(runner, 'migrate', lambda *_args: pytest.fail('completed work was repeated'))
    assert runner.main() == 0


def test_timer_hint_only_skips_a_fully_completed_cohort(tmp_path, monkeypatch):
    from scripts import check_offline_replay_pair_pending as pending
    monkeypatch.setattr(pending, 'ENROLLED', {'first': 'first'})
    path = tmp_path/'first/status.json'
    assert pending.needs_work(tmp_path)
    path.parent.mkdir()
    value = {'contract': runner.CONTRACT, 'dataset': 'first', 'state': 'completed',
             'D_archive_committed': True, 'D_manifest_sha256': 'a'*64, 'C_payload_retired': True,
             'C_retirement': {'state': 'paired_payload_retired'}, 'E_archive_cleanup': {'state': 'completed'}}
    path.write_text(json.dumps(value))
    assert not pending.needs_work(tmp_path)
    value['C_payload_retired'] = False
    path.write_text(json.dumps(value))
    assert pending.needs_work(tmp_path)
