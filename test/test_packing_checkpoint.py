"""An abandoned pack is a routing hint, never a source/recovery acceptance."""
import hashlib
import json
from pathlib import Path

import pytest

from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packing_checkpoint import SealedPackHints


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    source, primary = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    for index in range(13):
        (source / f'{index:03}.txt').write_bytes(f'{index},-2.5,NULL,股\n'.encode())
    packed.initialize_packed_layout(primary, node_id='test')
    policy = {'private_history': {'enabled': True, 'relative_root': '.local-state/private-history',
              'shared_export': False, 'nas_export': False, 'source_retirement': False}}
    cold = migration.private_history_root(primary, policy)
    root = tmp_path / 'journal/sealed-pack-checkpoints/example'
    monkeypatch.setattr(packed, 'MAX_PACK_MEMBERS', 4)
    def hints():
        return SealedPackHints(root, cold, source, 'example')
    def publish(index):
        return packed.publish_packed_snapshot(cold, 'example', source, node_id='test',
            pack_buckets=1, file_chunk_bytes=1024, private_observation_cohort=True,
            sealed_pack_hints=index, batch_directory_fsync=True)
    return source, cold, root, hints, publish


def abort_after_first_pack(fixture, monkeypatch):
    source, cold, root, hints, publish = fixture
    original = packed._write_pack
    calls = []
    def crash(*args, **options):
        if calls:
            raise SystemExit('simulated process interruption')
        calls.append(True)
        return original(*args, **options)
    with monkeypatch.context() as patch:
        patch.setattr(packed, '_write_pack', crash)
        with pytest.raises(SystemExit, match='process interruption'):
            publish(hints())
    records = list(root.glob('*.json'))
    assert len(records) == 1
    assert not list((cold / 'heads/example').glob('*.json'))
    return records[0]


@pytest.mark.parametrize('change', [None, 'revise', 'delete', 'add', 'mode'])
def test_retry_rehashes_originals_reuses_only_exact_sealed_members_and_restores_all(
        fixture, monkeypatch, tmp_path, change):
    source, cold, root, hints, publish = fixture
    record = abort_after_first_pack(fixture, monkeypatch)
    document = json.loads(record.read_bytes())
    first = source / document['files'][0]['path']
    if change == 'revise':
        first.write_bytes(b'revised source value, not the cached value')
    elif change == 'delete':
        first.unlink()
    elif change == 'add':
        (source / 'new.txt').write_bytes(b'new observation')
    elif change == 'mode':
        first.chmod(0o640)
    hashed, written = [], []
    original_hash, original_write = packed._hash_source_entry, packed._write_pack
    def full_hash(root, entry, **options):
        hashed.append(entry.path)
        return original_hash(root, entry, **options)
    def writer(root, entries, target, **options):
        written.extend(row.path for row in entries)
        return original_write(root, entries, target, **options)
    monkeypatch.setattr(packed, '_hash_source_entry', full_hash)
    monkeypatch.setattr(packed, '_write_pack', writer)
    index = hints()
    selected = publish(index)
    expected = {row['path'] for row in document['files'] if (source / row['path']).exists()}
    assert set(hashed) == expected  # Including a same-size/mode/stale hint.
    reused = expected - ({first.name} if change == 'revise' else set())
    assert not (set(written) & reused)
    assert selected.manifest['archive']['reused_files'] == len(reused)
    proof = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == len(list(source.iterdir()))
    restored = packed.fetch_packed_snapshot(cold, tmp_path / 'restored', selected)
    assert {p.name: p.read_bytes() for p in source.iterdir()} == {
        p.name: p.read_bytes() for p in restored.iterdir()}
    assert first.exists() is (change != 'delete')
    if change == 'mode':
        assert (restored / first.name).stat().st_mode & 0o777 == 0o640


@pytest.mark.parametrize('problem', ['corrupt-json', 'deep-json', 'wrong-binding', 'missing-object', 'fifo'])
def test_bad_or_missing_hint_is_preserved_but_never_blocks_fresh_raw_publication(
        fixture, monkeypatch, problem):
    import os
    source, cold, root, hints, publish = fixture
    record = abort_after_first_pack(fixture, monkeypatch)
    document = json.loads(record.read_bytes())
    if problem == 'corrupt-json':
        record.write_bytes(b'broken JSON retained as evidence')
    elif problem == 'deep-json':
        record.unlink()
        raw = b'[' * 2000 + b'0' + b']' * 2000
        record = root / (hashlib.sha256(raw).hexdigest() + '.json')
        record.write_bytes(raw)
    elif problem == 'wrong-binding':
        document['binding']['source'] = '/another/source'
        record.unlink()  # Only generated fixture metadata, never production.
        raw = packed._canonical_json_bytes(document)
        record = root / (hashlib.sha256(raw).hexdigest() + '.json')
        record.write_bytes(raw)
    elif problem == 'missing-object':
        (cold / document['object']['relpath']).unlink()
    else:
        record.unlink()
        os.mkfifo(record)
    index = hints()
    selected = publish(index)
    assert record.exists()
    assert index.statistics['missing_objects' if problem == 'missing-object' else 'rejected'] > 0
    assert index.statistics['objects_loaded'] == 0
    assert selected.manifest['archive']['reused_files'] == 0
    assert packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)['independently_reconstructed_files'] == 13


def test_corrupt_cas_object_is_not_blessed_by_hint_or_current_source_hash(fixture, monkeypatch):
    source, cold, root, hints, publish = fixture
    record = abort_after_first_pack(fixture, monkeypatch)
    item = json.loads(record.read_bytes())['object']
    path = cold / item['relpath']
    path.write_bytes(b'X' * path.stat().st_size)
    selected = publish(hints())
    with pytest.raises(SnapshotError, match='checksum'):
        packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
    assert len(list(source.iterdir())) == 13


def test_hint_cannot_be_enabled_for_public_or_other_source_authority(fixture, tmp_path):
    source, cold, root, hints, publish = fixture
    with pytest.raises(SnapshotError, match='original private source/authority'):
        packed.publish_packed_snapshot(cold, 'example', source, sealed_pack_hints=hints())
    wrong = SealedPackHints(tmp_path / 'other-journal', cold, tmp_path / 'another-source', 'example')
    with pytest.raises(SnapshotError, match='original private source/authority'):
        publish(wrong)


def test_hint_root_cannot_redirect_or_become_shared(fixture, tmp_path):
    source, cold, root, hints, publish = fixture
    index = hints()
    root.chmod(0o755)
    with pytest.raises(SnapshotError, match='authority changed'):
        index.load_into(None, {})
    with pytest.raises(SnapshotError, match='owner/permissions'):
        hints()
    alias = tmp_path / 'alias'
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(SnapshotError, match='real private journal'):
        SealedPackHints(alias, cold, source, 'example')


def test_committed_inventory_has_priority_over_abandoned_hints(fixture, monkeypatch):
    source, cold, root, hints, publish = fixture
    abort_after_first_pack(fixture, monkeypatch)
    selected = publish(hints())
    next_index = hints()
    assert publish(next_index).manifest_sha256 == selected.manifest_sha256
    assert next_index.statistics['files_loaded'] == 0
    assert next_index.receipts


@pytest.mark.parametrize('referenced', [False, True])
def test_owned_hint_retirement_scans_processes_once_and_holds_whole_referenced_batch(
        fixture, monkeypatch, referenced):
    from stockagent.data_sync import materialized_cache
    source, cold, root, hints, publish = fixture
    index = hints()
    selected = publish(index)
    packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
    calls = []
    def references(paths):
        calls.append(paths)
        return ['pid=123:fd=9:owned-hint'] if referenced else []
    monkeypatch.setattr(materialized_cache, 'process_references_many', references)
    result = migration._retire_owned_scratch(root.parents[1], 'example', {
        'sealed_pack_hint_files': list(index.receipts.values())})
    assert len(calls) == 1 and len(calls[0]) == 4
    assert result['state'] == ('waiting' if referenced else 'retired')
    assert all(Path(name).exists() is referenced for name in index.receipts)
    assert len(list(source.iterdir())) == 13
    assert packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)['independently_reconstructed_files'] == 13


def test_owned_hint_retirement_rejects_cross_operation_before_any_deletion(fixture, tmp_path):
    source, cold, root, hints, publish = fixture
    index = hints()
    publish(index)
    with pytest.raises(SnapshotError, match='original operation'):
        migration._retire_owned_scratch(tmp_path / 'other-operation', 'example', {
            'sealed_pack_hint_files': list(index.receipts.values())})
    assert all(Path(name).exists() for name in index.receipts)


def test_owned_hint_retirement_rechecks_after_fresh_process_scan(fixture, monkeypatch):
    from stockagent.data_sync import materialized_cache
    source, cold, root, hints, publish = fixture
    index = hints()
    publish(index)
    first = Path(next(iter(index.receipts)))
    def race(paths):
        first.write_bytes(b'changed after the full checksum')
        return []
    monkeypatch.setattr(materialized_cache, 'process_references_many', race)
    with pytest.raises(SnapshotError, match='final retirement check'):
        migration._retire_owned_scratch(root.parents[1], 'example', {
            'sealed_pack_hint_files': list(index.receipts.values())})
    assert all(Path(name).exists() for name in index.receipts)


def test_owned_hint_retirement_reconciles_interruption_without_counting_unproved_reclaim(
        fixture, monkeypatch):
    from stockagent.data_sync import materialized_cache
    source, cold, root, hints, publish = fixture
    index = hints()
    selected = publish(index)
    packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    original_unlink = Path.unlink
    attempted = []
    def crash(path, *args, **options):
        if path.parent == root:
            attempted.append(path)
            if len(attempted) == 2:
                raise OSError('injected interrupted owned metadata unlink')
        return original_unlink(path, *args, **options)
    publication = {'sealed_pack_hint_files': list(index.receipts.values())}
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', crash)
        with pytest.raises(OSError, match='interrupted owned metadata'):
            migration._retire_owned_scratch(root.parents[1], 'example', publication)
    journal = root.parents[1] / 'scratch-retirement/example.json'
    assert all(row['state'] == 'unlink_planned' for row in json.loads(journal.read_bytes())['files'].values())
    result = migration._retire_owned_scratch(root.parents[1], 'example', publication)
    assert result['state'] == 'retired'
    assert result['files'][str(attempted[0])]['state'] == 'already_absent'
    assert result['files'][str(attempted[0])]['allocated_bytes_reclaimed'] == 0
    assert not any(Path(name).exists() for name in index.receipts)
    assert packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)['independently_reconstructed_files'] == 13
