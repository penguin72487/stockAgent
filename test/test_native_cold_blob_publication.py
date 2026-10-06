import hashlib

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('problem', [None, 'corrupt', 'changed-during-read'])
def test_bounded_small_pack_keeps_complete_sha_and_final_generation(tmp_path, monkeypatch, problem):
    import os
    path=tmp_path/'pack.zip';payload=b'small exact encoded members'*100
    path.write_bytes(payload)
    item={'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'file_count':2}
    if problem == 'corrupt':
        path.write_bytes(b'X'*len(payload))
    monkeypatch.setattr(native,'binary_reader',lambda *a:pytest.fail('bounded small pack launched native reader'))
    if problem:
        with pytest.raises(SnapshotError,match='checksum|changed during'):
            with packed._native_pack_source(path,item,enabled=True) as source:
                assert source.read()==payload
                before=path.stat();path.write_bytes(b'X'*len(payload))
                os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns))
    else:
        with packed._native_pack_source(path,item,enabled=True) as source:
            assert source.read()==payload


@pytest.mark.parametrize('bytes_count,members,stored', [
    (512*1024+1,2,None), (1024,3,None), (1024,2,500)])
def test_large_many_member_and_subset_packs_keep_native_admission(tmp_path, monkeypatch, bytes_count, members, stored):
    from contextlib import contextmanager
    from types import SimpleNamespace
    import stockagent.data_sync.training_return as admission
    path=tmp_path/'pack.zip';payload=b'X'*bytes_count;path.write_bytes(payload)
    item={'bytes':bytes_count,'sha256':hashlib.sha256(payload).hexdigest(),'file_count':members}
    if stored is not None:item['stored_file_count']=stored
    calls=[]
    @contextmanager
    def reader(path):
        calls.append('native')
        with path.open('rb') as source:yield source
    monkeypatch.setattr(native,'binary_reader',reader)
    monkeypatch.setattr(packed.shutil,'disk_usage',lambda path:SimpleNamespace(free=64*1024**3))
    monkeypatch.setattr(admission,'admit_workspace',lambda *a:calls.append('physical-admission'))
    with packed._native_pack_source(path,item,enabled=True) as source:
        assert source.read()==payload
    assert calls==['physical-admission','native']


@pytest.mark.parametrize('corrupt', [False, True])
def test_native_durable_copy_requires_both_full_checksums_and_closes_writer(tmp_path, monkeypatch, corrupt):
    source = tmp_path / 'unique-original'
    payload = b'original research output' * 1000
    source.write_bytes(payload)
    destination = tmp_path / 'exact-private-copy'
    writers = []

    class Writer:
        def __init__(self, path):
            self.stream = path.open('xb')
            self.digest = hashlib.sha256()
            writers.append(self)

        def write(self, block):
            self.stream.write(block)
            self.digest.update(block)

        def finish(self):
            self.stream.flush()
            return {'sha256': '0' * 64 if corrupt else self.digest.hexdigest()}

        def close(self):
            self.stream.close()

    monkeypatch.setattr(native, 'BinaryWriter', Writer)
    if corrupt:
        with pytest.raises(SnapshotError, match='checksum differs'):
            packed._native_copy_and_hash(source, destination)
    else:
        assert packed._native_copy_and_hash(source, destination) == hashlib.sha256(payload).hexdigest()
        assert destination.read_bytes() == payload
    assert source.read_bytes() == payload and writers[0].stream.closed


@pytest.mark.parametrize('flag', [True, 1, 'true', None])
def test_native_write_cannot_use_an_unenrolled_cold_namespace(tmp_path, flag):
    with pytest.raises(SnapshotError, match='native D blob writes|explicit boolean'):
        packed.publish_packed_snapshot(tmp_path / 'cold', 'example', tmp_path / 'source',
                                       d_primary_native_blob_writes=flag)


def test_four_bucket_publication_keeps_every_original_recoverable(tmp_path):
    import json
    import os
    import time
    from stockagent.data_sync.legacy_artifact_archive import (
        LegacyArchiveSpec, MANUAL_WSL_CAPTURE_CONTRACT, publish_archive, verify_cold_archive,
    )
    artifacts = tmp_path / 'repo/artifacts'
    source = artifacts / 'replays/example'
    source.mkdir(parents=True)
    for n in range(40):
        file = source / f'output-{n}.json'
        file.write_text(json.dumps({'value': n}))
        stamp = time.time() - 2 * 86400
        os.utime(file, (stamp, stamp))
    spec = LegacyArchiveSpec('legacy-four-pack-example', 'replays/example', 7, tmp_path / 'stage',
                             12, False, capture_contract=MANUAL_WSL_CAPTURE_CONTRACT)
    release = publish_archive(spec, artifacts, tmp_path / 'cold', repo_root=artifacts.parent,
                              manual_capture=True, pack_buckets=4)
    proof = verify_cold_archive(spec, tmp_path / 'cold')
    assert proof['snapshot_id'] == release['snapshot_id'] and proof['files'] == 40
    assert proof['cold_verified'] and proof['decoded_originals_verified']


@pytest.mark.parametrize('failure',[None,'hash','mutation','byte-bound','oversized-corrupt'])
def test_native_batch_verification_keeps_all_blob_integrity_and_generation_gates(tmp_path,monkeypatch,failure):
    source=tmp_path/'source';source.mkdir()
    for index in range(19):
        (source/f'blob-{index}').write_bytes(str(index).encode()*300)
    cold=tmp_path/'cold'
    resolved=packed.publish_packed_snapshot(cold,'batch-example',source,loose_file_threshold_bytes=1)
    root_resolve=packed.Path.resolve
    monkeypatch.setattr(packed.Path,'resolve',lambda path,*a,**kw:
        packed.Path('/srv/stockagent-packed') if path==cold else root_resolve(path,*a,**kw))
    original_path_under=packed._path_under
    monkeypatch.setattr(packed,'_path_under',lambda root,*a,**kw:
        original_path_under(cold if str(root)=='/srv/stockagent-packed' else root,*a,**kw))
    original_inventory=packed._load_inventory
    monkeypatch.setattr(packed,'_load_inventory',lambda root,*a:
        original_inventory(cold if str(root)=='/srv/stockagent-packed' else root,*a))
    monkeypatch.setattr(packed,'_validate_object_presence',lambda *a:None)
    if failure in {'byte-bound','oversized-corrupt'}:
        monkeypatch.setattr(packed,'MAX_NATIVE_HASH_MEMBER_BYTES',400)
        monkeypatch.setattr(packed,'MAX_NATIVE_HASH_BATCH_BYTES',1000)
    if failure=='oversized-corrupt':
        member=next(item for item in resolved.manifest['archive']['objects'] if item['bytes']>400)
        (cold/member['relpath']).write_bytes(b'X'*member['bytes'])
    batches=[]
    def full_hash(paths):
        batches.append(list(paths))
        if failure in {'byte-bound','oversized-corrupt'}:
            assert sum(path.stat().st_size for path in paths)<=1000
            assert all(path.stat().st_size<=400 for path in paths)
        value={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        if failure=='hash': value[paths[0]]='0'*64
        if failure=='mutation' and len(batches)==2:
            first=batches[0][0];info=first.stat();first.write_bytes(b'X'*info.st_size)
        return value
    monkeypatch.setattr(native,'hash_many',full_hash)
    if failure and failure!='byte-bound':
        with pytest.raises(SnapshotError,match='checksum mismatch|changed after full batch'):
            packed.verify_packed_snapshot(cold,resolved,d_primary_native_blob_reads=True)
    else:
        result=packed.verify_packed_snapshot(cold,resolved,d_primary_native_blob_reads=True)
        assert result['objects']==19
        if failure=='byte-bound':
            assert sum(map(len,batches))==10  # larger members still get a full streaming SHA
        else:
            assert sorted(map(len,batches))==[3,16]
