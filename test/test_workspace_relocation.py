import hashlib
import os
from pathlib import Path
import tempfile
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


def test_relocation_rejects_zero_workers_without_creating_destination(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    with pytest.raises(ValueError, match='at least one worker'):
        relocation.copy(source, tmp_path / 'destination', tmp_path / 'evidence', workers=0)
    assert not (tmp_path / 'destination').exists()


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
