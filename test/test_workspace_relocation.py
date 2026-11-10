import hashlib
import os
from pathlib import Path
import tempfile
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import packed_snapshots as packed


def test_exact_copy_preserves_payload_modes_empty_dirs_and_source(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source/'empty').mkdir()
    p = source/'nested'/'bytes'
    p.parent.mkdir()
    p.write_bytes(bytes(range(256))*1024)
    p.chmod(0o640)
    os.utime(p, ns=(1234567890123456789,1234567890123456789))
    destination = tmp_path/'dest'
    result = relocation.copy(source, destination, tmp_path/'evidence', workers=2)
    assert result['source_files_deleted'] == 0
    assert result['files'] == 1
    assert (destination/'empty').is_dir()
    q = destination/'nested'/'bytes'
    assert q.read_bytes() == p.read_bytes()
    assert q.stat().st_mtime_ns == p.stat().st_mtime_ns
    assert q.stat().st_mode == p.stat().st_mode
    q.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='verified E member changed'):
        relocation.copy(source, destination, tmp_path/'evidence')
    assert p.exists()
    q.write_bytes(p.read_bytes())
    os.utime(q, ns=(p.stat().st_mtime_ns, p.stat().st_mtime_ns))
    q.chmod(0o600)
    with pytest.raises(ValueError, match='verified E member changed'):
        relocation.copy(source, destination, tmp_path/'evidence')
    q.chmod(0o640)
    resumed = relocation.copy(source, destination, tmp_path/'evidence')
    assert resumed['state'] == 'exact_destination_readback_verified'


def test_source_changes_fail_without_deleting_anything(tmp_path):
    source = tmp_path/'source';source.mkdir()
    p = source/'file';p.write_bytes(b'original')
    destination = tmp_path/'dest'
    evidence = tmp_path/'evidence'
    db = relocation.index(source,destination,evidence);db.close()
    p.write_bytes(b'changed')
    with pytest.raises(ValueError,match='changed after inventory'):
        relocation.copy(source,destination,evidence)
    assert p.read_bytes() == b'changed'


def test_slow_file_does_not_block_later_files_or_exact_readback(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    for i in range(12):
        (source / f'{i:02d}').write_bytes(f'source-{i}'.encode())
    release_slow, later_started = Event(), Event()
    real_copy = relocation._copy

    def controlled_copy(row, *args, **kwargs):
        if row[0] == '00':
            assert release_slow.wait(10)
        if row[0] == '08':
            later_started.set()
        return real_copy(row, *args, **kwargs)

    monkeypatch.setattr(relocation, '_copy', controlled_copy)
    destination = tmp_path / 'destination'
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(relocation.copy, source, destination, tmp_path / 'evidence', workers=2)
        try:
            # Eight later files must be dispatched before the first is released;
            # the old fixed-batch pool.map waits for that first file instead.
            assert later_started.wait(8)
        finally:
            release_slow.set()
        receipt = result.result(timeout=15)
    assert receipt['state'] == 'exact_destination_readback_verified'
    assert receipt['files'] == 12 and receipt['source_files_deleted'] == 0
    assert all((destination / f'{i:02d}').read_bytes() == (source / f'{i:02d}').read_bytes()
               for i in range(12))


def test_resume_rechecks_all_files_without_one_slow_hash_blocking_others(tmp_path, monkeypatch):
    source = tmp_path/'source'; source.mkdir()
    for i in range(140):
        (source/f'{i:03d}').write_bytes(f'original-{i}'.encode())
    destination, evidence = tmp_path/'destination', tmp_path/'evidence'
    relocation.copy(source, destination, evidence, workers=2)
    with sqlite3.connect(evidence/'files.sqlite') as db:
        names = [row[0] for row in db.execute("SELECT path FROM files WHERE kind='file' AND state='verified'")]
    blocked_name, later_name = names[0], names[64]
    slow, later = Event(), Event()
    original = relocation.digest
    def controlled(path, **kwargs):
        if path == destination/blocked_name:
            assert slow.wait(10)
        if path == destination/later_name:
            later.set()
        return original(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', controlled)
    with ThreadPoolExecutor(max_workers=1) as pool:
        job = pool.submit(relocation.copy, source, destination, evidence, workers=2, verify_workers=2)
        try:
            assert later.wait(8)
        finally:
            slow.set()
        assert job.result(timeout=15)['state'] == 'exact_destination_readback_verified'


def test_relocation_rejects_zero_workers_without_creating_destination(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    with pytest.raises(ValueError, match='at least one worker'):
        relocation.copy(source, tmp_path / 'destination', tmp_path / 'evidence', workers=0)
    assert not (tmp_path / 'destination').exists()


def test_copy_reports_namespace_verification_as_distinct_phases(tmp_path, monkeypatch):
    source = tmp_path/'source'; source.mkdir()
    (source/'directory').mkdir()
    (source/'value').write_bytes(b'original')
    states, directory_progress, write = [], [], relocation.atomic_write_json
    def capture(path, value, *args, **kwargs):
        if Path(path).name == 'progress.json':
            states.append(value.get('state'))
            if value.get('state') == 'applying_destination_directory_metadata':
                directory_progress.append(value)
        return write(path, value, *args, **kwargs)
    monkeypatch.setattr(relocation, 'atomic_write_json', capture)
    relocation.copy(source, tmp_path/'dest', tmp_path/'proof')
    assert 'verifying_source_namespace' in states
    assert directory_progress[-1]['entries_completed'] == directory_progress[-1]['entries_total'] == 1
    assert directory_progress[-1]['source_files_deleted'] == 0
    assert 'verifying_destination_namespace' in states
    assert states[-1] == 'exact_destination_readback_verified'


def test_namespace_batch_obeys_reduced_sqlite_variable_limit(tmp_path, monkeypatch):
    source = tmp_path/'source'; source.mkdir()
    for i in range(11):
        (source/f'{i:02d}').write_bytes(bytes([i]))
    index = relocation.index
    def limited_index(*args, **kwargs):
        db = index(*args, **kwargs)
        db.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 4)
        return db
    monkeypatch.setattr(relocation, 'index', limited_index)
    result = relocation.copy(source, tmp_path/'dest', tmp_path/'proof', workers=2)
    assert result['files'] == 11 and result['source_namespace_unchanged'] is True


def test_shared_inode_proof_preserves_full_sha_and_rechecks_each_generation(tmp_path, monkeypatch):
    import os
    import shutil
    source = tmp_path/'source'; source.mkdir()
    (source/'first').write_bytes(b'original shared financial evidence')
    os.link(source/'first', source/'second')
    destination, evidence = tmp_path/'destination', tmp_path/'proof'
    db = relocation.index(source, destination, evidence)
    db.close()
    shutil.copy2(source/'first', destination/'first')
    os.link(destination/'first', destination/'second')
    original_digest, calls = relocation.digest, []
    def counted(path, **kwargs):
        calls.append(path)
        return original_digest(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', counted)
    result = relocation.copy(source, destination, evidence, workers=1,
                             deduplicate_hardlink_verification=True)
    assert result['hardlink_generation_proof_cache_hits'] == 1
    assert len(calls) == 2  # One full source SHA and one independent E SHA.
    assert (destination/'first').samefile(destination/'second')
    calls.clear()
    result = relocation.copy(source, destination, evidence, workers=1, verify_workers=1,
                             deduplicate_hardlink_verification=True)
    assert result['hardlink_generation_proof_cache_hits'] == 1
    assert len(calls) == 1  # Resume still checks the retained inode's full SHA.
    (destination/'second').write_bytes(b'changed')
    with pytest.raises(ValueError, match='previously verified E member changed'):
        relocation.copy(source, destination, evidence, workers=1,
                        deduplicate_hardlink_verification=True)


def test_out_of_order_copy_failure_can_resume_without_losing_sources(tmp_path, monkeypatch):
    source, destination, evidence = tmp_path / 'source', tmp_path / 'destination', tmp_path / 'evidence'
    source.mkdir()
    for i in range(12):
        (source / f'{i:02d}').write_bytes(f'payload-{i}'.encode())
    real_copy = relocation._copy

    def fail_one(row, *args, **kwargs):
        if row[0] == '05':
            raise OSError('simulated interrupted worker')
        return real_copy(row, *args, **kwargs)

    monkeypatch.setattr(relocation, '_copy', fail_one)
    with pytest.raises(OSError, match='simulated interrupted worker'):
        relocation.copy(source, destination, evidence, workers=2)
    assert len(list(source.iterdir())) == 12
    monkeypatch.setattr(relocation, '_copy', real_copy)
    result = relocation.copy(source, destination, evidence, workers=2)
    assert result['files'] == 12 and result['source_files_deleted'] == 0
    assert all((destination / f'{i:02d}').read_bytes() == (source / f'{i:02d}').read_bytes()
               for i in range(12))


def test_redirected_source_is_never_followed(tmp_path):
    source=tmp_path/'source';source.mkdir()
    (source/'link').symlink_to('/etc/passwd')
    with pytest.raises(ValueError,match='redirected/special'):
        relocation.copy(source,tmp_path/'dest',tmp_path/'proof')
    assert (source/'link').is_symlink()


def test_scoped_copy_preserves_link_text_without_reading_excluded_payload(tmp_path):
    source = tmp_path/'source'; source.mkdir()
    (source/'unused').mkdir()
    (source/'unused'/'not-a-file').symlink_to('/does/not/exist')
    (source/'payload').write_bytes(b'needed futures source')
    (source/'alias').symlink_to('payload')
    dest, evidence = tmp_path/'dest', tmp_path/'proof'
    result = relocation.copy(source, dest, evidence, excluded_subtrees=('unused',),
                             preserve_symlinks=True)
    assert (dest/'payload').read_bytes() == b'needed futures source'
    assert os.readlink(dest/'alias') == 'payload'
    assert list((dest/'unused').iterdir()) == []
    assert result['selected_source_sha256_and_destination_readback_verified'] is True
    assert result['all_source_sha256_and_destination_readback_verified'] is False
    with pytest.raises(ValueError, match='scope changed'):
        relocation.copy(source, dest, evidence, preserve_symlinks=True)
    relocation.copy(source, dest, evidence, excluded_subtrees=('unused',), preserve_symlinks=True)


@pytest.mark.parametrize('exclusions', [('absent',), ('payload',), ('../escape',), ('a', 'a/b')])
def test_scoped_copy_rejects_unproven_exclusion(tmp_path, exclusions):
    source = tmp_path/'source'; source.mkdir()
    (source/'payload').write_bytes(b'preserve')
    with pytest.raises(ValueError):
        relocation.copy(source, tmp_path/'dest', tmp_path/'proof', excluded_subtrees=exclusions)
    assert (source/'payload').read_bytes() == b'preserve'


def test_batched_sync_failure_commits_no_unflushed_completion_and_can_resume(tmp_path, monkeypatch):
    source = tmp_path/'source'; source.mkdir()
    for i in range(15):
        (source/str(i)).write_bytes(('source-'+str(i)).encode())
    dest, proof = tmp_path/'dest', tmp_path/'proof'
    real_sync = relocation.sync_filesystem
    def failed_sync(path):
        raise OSError('simulated E flush failure')
    monkeypatch.setattr(relocation, 'sync_filesystem', failed_sync)
    with pytest.raises(OSError, match='flush failure'):
        relocation.copy(source, dest, proof, workers=2, sync_batch_files=8)
    with sqlite3.connect(proof/'files.sqlite') as db:
        assert db.execute("SELECT count(*) FROM files WHERE state='verified'").fetchone() == (0,)
    monkeypatch.setattr(relocation, 'sync_filesystem', real_sync)
    result = relocation.copy(source, dest, proof, workers=2, sync_batch_files=8)
    assert result['files'] == 15 and result['source_files_deleted'] == 0
    assert result['durability'] == 'filesystem_barrier_before_verified_commit'
    assert all((source/str(i)).read_bytes() == (dest/str(i)).read_bytes() for i in range(15))


def test_pending_rows_release_sqlite_snapshot_before_completion_commits(tmp_path):
    db = sqlite3.connect(tmp_path/'proof.sqlite')
    try:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE files(path TEXT, sig TEXT, bytes INTEGER, kind TEXT, state TEXT)')
        db.executemany('INSERT INTO files VALUES(?,?,?,?,?)',
                       [(str(i), '[]', 1, 'file', 'pending') for i in range(17)])
        db.commit()
        rows = relocation.pending_rows(db, size=3)
        seen = []
        for row in rows:
            seen.append(row[0])
            db.execute("UPDATE files SET state='verified' WHERE path=?", (row[0],))
            db.commit()
            # An open full-inventory cursor keeps this checkpoint locked.
            assert db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        assert seen == [str(i) for i in range(17)]
    finally:
        db.close()


@pytest.mark.parametrize('corrupt', [False,True])
def test_cross_volume_cas_never_replaces_winner_or_loses_original(tmp_path,monkeypatch,corrupt):
    if not Path('/dev/shm').is_dir() or Path('/dev/shm').stat().st_dev == tmp_path.stat().st_dev:
        pytest.skip('requires two temporary filesystems')
    cold=tmp_path/'cold';packed.initialize_packed_layout(cold,node_id='test')
    with tempfile.TemporaryDirectory(dir='/dev/shm',prefix='stockagent-crossfs-test-') as td:
        temporary=Path(td)/'closed';temporary.write_bytes(b'exact sealed pack')
        target=cold/'objects/blobs/aa'/('f'*64)
        target.parent.mkdir()
        expected=hashlib.sha256(temporary.read_bytes()).hexdigest()
        if corrupt:
            monkeypatch.setattr(packed,'_copy_and_hash',lambda *a:'0'*64)
            with pytest.raises(packed.SnapshotError,match='copy differs'):
                packed._install_immutable_object(cold,temporary,target,expected_sha256=expected)
            assert temporary.exists() and not target.exists()
        else:
            assert packed._install_immutable_object(cold,temporary,target,expected_sha256=expected) is False
            assert target.read_bytes() == b'exact sealed pack' and not temporary.exists()
