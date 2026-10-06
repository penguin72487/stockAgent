from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import time

import pytest

from stockagent.data_sync import bulk_archive as archive
from stockagent.data_sync import bulk_archive_retirement as retirement
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def partition_budget(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(archive.os, 'statvfs', lambda _: SimpleNamespace(f_bavail=2 * 1024**4, f_frsize=1))


def tar_bytes(entries):
    out = BytesIO()
    with tarfile.open(fileobj=out, mode="w", format=tarfile.PAX_FORMAT) as writer:
        for name, kind, data in entries:
            info = tarfile.TarInfo(name)
            info.mode = 0o640
            info.pax_headers["mtime"] = "1234567890.123456789"
            if kind == "dir":
                info.type = tarfile.DIRTYPE
                writer.addfile(info)
            elif kind == "link":
                info.type = tarfile.LNKTYPE
                info.linkname = data
                writer.addfile(info)
            elif kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = data
                writer.addfile(info)
            else:
                info.size = len(data)
                writer.addfile(info, BytesIO(data))
    return out.getvalue()


def test_index_preserves_originals_and_portable_metadata():
    data = tar_bytes([("markets/one", "dir", None), ("markets/one/empty", "dir", None),
                      ("markets/one/a.npy", "file", b"original"),
                      ("markets/one/b.npy", "link", "markets/one/a.npy")])
    index = archive.index_tar_stream(BytesIO(data), scopes={"markets"})
    assert index["files"] == 2 and index["logical_bytes"] == 16
    files = [r for r in index["rows"] if r["kind"] == "file"]
    assert files[0]["sha256"] == files[1]["sha256"] == hashlib.sha256(b"original").hexdigest()
    assert files[0]["mtime_ns"] == 1234567890123456789
    assert archive.root_records(index)[0]["unsupported"] is False


@pytest.mark.parametrize("name", ["/markets/a", "markets/../secret", "markets/./a", "markets//a", "cache/a"])
def test_archive_paths_fail_closed(name):
    with pytest.raises(SnapshotError):
        archive.index_tar_stream(BytesIO(tar_bytes([(name, "file", b"x")])), scopes={"markets"})


def test_duplicate_or_unresolved_member_rejected():
    for members in ([('markets/a', 'file', b'x'), ('markets/a', 'file', b'y')],
                    [('markets/a', 'link', 'markets/missing')]):
        with pytest.raises(SnapshotError):
            archive.index_tar_stream(BytesIO(tar_bytes(members)), scopes={"markets"})


def test_symlink_preserved_but_never_retirable():
    index = archive.index_tar_stream(BytesIO(tar_bytes([
        ('markets/a', 'dir', None), ('markets/a/source', 'symlink', '/unique/original')
    ])), scopes={"markets"})
    assert archive.root_records(index)[0]["unsupported"] is True


def test_trailing_non_tar_payload_rejected():
    data = tar_bytes([('markets/a', 'file', b'x')]) + b'not padding'
    with pytest.raises(SnapshotError):
        archive.index_tar_stream(BytesIO(data), scopes={"markets"})


def test_full_zstd_frame_checksum_and_hash(tmp_path):
    raw = tar_bytes([('markets/a', 'file', b'x' * 50000)])
    encoded = subprocess.check_output(['zstd', '-1', '-q', '-c'], input=raw)
    payload = tmp_path / 'p.tar.zst'
    payload.write_bytes(encoded)
    proof = archive.index_zstd(payload, expected_sha256=hashlib.sha256(encoded).hexdigest(), scopes={"markets"})
    assert proof["logical_bytes"] == 50000
    broken = encoded[:-3]
    payload.write_bytes(broken)
    with pytest.raises((SnapshotError, tarfile.ReadError)):
        archive.index_zstd(payload, expected_sha256=hashlib.sha256(broken).hexdigest(), scopes={"markets"})


def make_ack(repo, roots):
    rows = []
    for root in roots:
        base = repo / 'artifacts' / root
        for path in [base, *sorted(base.rglob('*'))]:
            s = path.lstat()
            row = {"path": path.relative_to(repo / 'artifacts').as_posix(),
                   "kind": "directory" if path.is_dir() else "file",
                   "mode": stat.S_IMODE(s.st_mode), "uid": s.st_uid, "gid": s.st_gid,
                   "mtime_ns": s.st_mtime_ns}
            if path.is_file():
                row.update(size=s.st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            rows.append(row)
    index = {"rows": rows}
    records = archive.root_records(index)
    ack = {"contract": "d_verified_bulk_archive_return_v1", "preservation_contract": archive.CONTRACT,
           "origin_node_id": "vastai1T", "authority_node_id": "penguin",
           "dataset": "legacy-vast-bulk-cache-test", "snapshot_id": "exact-release",
           "manifest_sha256": 'a' * 64, "compressed_sha256": 'b' * 64,
           "member_inventory_sha256": 'c' * 64, "source_scope": "cache",
           "cold_verified": True, "decoded_originals_verified": True,
           "verified_at_epoch": time.time(), "manual_immediate": True,
           "minimum_stable_hours": 12, "root": records[0], "approved_roots": roots,
           "cohort_file_names": [r['path'] for r in rows if r['kind'] == 'file']}
    ack['identity_sha256'] = archive.digest(ack)
    return ack, records


def source(tmp_path, monkeypatch):
    root = tmp_path / 'artifacts/cache/one'
    root.mkdir(parents=True)
    data = root / 'data.npy'
    data.write_bytes(b'preserved original' * 100)
    data.chmod(0o644)
    os.utime(data, ns=(1, 1))
    (root / 'empty').mkdir()
    monkeypatch.setattr(retirement, 'consumers', lambda *_: [])
    monkeypatch.setattr(retirement, 'transport', lambda *_: {'ok': True})
    ack, _ = make_ack(tmp_path, ['cache/one'])
    return root, data, ack


def test_dry_run_then_exact_cold_backed_retirement(tmp_path, monkeypatch):
    root, data, ack = source(tmp_path, monkeypatch)
    state = tmp_path / 'private-state'
    plan = retirement.retire(tmp_path, ack, state_root=state)
    assert plan['state'] == 'would-retire' and data.is_file()
    result = retirement.retire(tmp_path, ack, apply=True, state_root=state)
    assert result['deleted'] and result['reclaimed_allocated_bytes'] > 0
    assert not root.exists() and result['cold_deleted'] is False


@pytest.mark.parametrize('reason', ['absent', 'consumer', 'changed'])
def test_candidate_observation_skips_unusable_roots_without_hash_or_delete(tmp_path, monkeypatch, reason):
    root, data, ack = source(tmp_path, monkeypatch)
    if reason == 'absent':
        data.unlink()
        (root / 'empty').rmdir()
        root.rmdir()
    elif reason == 'consumer':
        monkeypatch.setattr(retirement, 'consumers', lambda *_: ['active-training'])
    else:
        data.write_bytes(b'new generation')
    monkeypatch.setattr(retirement, 'sha256_file', lambda *_: pytest.fail('metadata observation read payload'))
    result = retirement.observe_preserved_roots(tmp_path, [ack['root']])['cache/one']
    assert result['deleted'] is False and result['candidate_only'] is True
    assert result['state'] == {'absent': 'source-absent-without-new-retirement',
                               'consumer': 'source-protected',
                               'changed': 'source-changed-from-preservation'}[reason]
    assert data.exists() == (reason != 'absent')


def test_candidate_metadata_is_not_original_byte_proof(tmp_path, monkeypatch):
    root, data, ack = source(tmp_path, monkeypatch)
    original_time = data.stat().st_mtime_ns
    original = data.read_bytes()
    data.write_bytes(bytes(len(original)))
    os.utime(data, ns=(original_time, original_time))
    result = retirement.observe_preserved_roots(tmp_path, [ack['root']])['cache/one']
    assert result['state'] == 'metadata-candidate-requires-exact-proof'
    with pytest.raises(SnapshotError, match='original bytes'):
        retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / 'state')
    assert root.is_dir() and data.is_file()


def test_batched_training_consumer_observation_protects_only_referenced_roots(tmp_path, monkeypatch):
    from stockagent.data_sync import node_roles
    paths = [tmp_path / 'artifacts/cache' / name for name in ('active', 'offline')]
    monkeypatch.setattr(node_roles, 'training_only_node', lambda: True)
    calls = []
    def configs(sources, repo):
        calls.append(tuple(sources))
        return {str(p.resolve()): ['pid=12:active-config:closed-input'] if p == paths[0] else [] for p in sources}
    monkeypatch.setattr(retirement, 'active_configuration_references_many', configs)
    monkeypatch.setattr(retirement, 'artifact_service_references', lambda sources, _: {str(p): [] for p in sources})
    monkeypatch.setattr(retirement, 'artifact_process_references_many', lambda *_: [f'pid=12:fd=9:{paths[0]}/array.npy'])
    result = retirement._candidate_consumers_many(paths, tmp_path)
    assert result[str(paths[0])] and result[str(paths[1])] == []
    assert calls == [tuple(paths)]
    monkeypatch.setattr(retirement, 'artifact_process_references_many', lambda *_: [f'pid=13:cmdline:{tmp_path}/artifacts/cache'])
    result = retirement._candidate_consumers_many(paths, tmp_path)
    assert all(any('pid=13:cmdline:' in ref for ref in refs) for refs in result.values())


@pytest.mark.parametrize('failure', ['stale', 'hash', 'extra', 'metadata', 'root_metadata', 'consumer', 'transport', 'recent', 'shared'])
def test_retirement_preserves_source_on_failed_proof(tmp_path, monkeypatch, failure):
    root, data, ack = source(tmp_path, monkeypatch)
    if failure == 'stale':
        ack['verified_at_epoch'] -= 1801
        ack['identity_sha256'] = archive.digest({k: v for k, v in ack.items() if k != 'identity_sha256'})
    elif failure == 'hash':
        original_time = data.stat().st_mtime_ns
        data.write_bytes(b'other content')
        os.utime(data, ns=(original_time, original_time))
    elif failure == 'extra':
        (root / 'unique-original').write_bytes(b'new')
    elif failure == 'metadata':
        data.chmod(0o600)
    elif failure == 'root_metadata':
        root.chmod(0o711)
    elif failure == 'consumer':
        monkeypatch.setattr(retirement, 'consumers', lambda *_: ['active-training'])
    elif failure == 'transport':
        monkeypatch.setattr(retirement, 'transport', lambda *_: (_ for _ in ()).throw(SnapshotError('not converged')))
    elif failure == 'recent':
        os.utime(data, None)
    else:
        os.link(data, tmp_path / 'unknown-alias')
    if failure == 'consumer':
        result = retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / 'state')
        assert result['state'] == 'source-protected'
    else:
        with pytest.raises(SnapshotError):
            retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / 'state')
    assert data.is_file()


def test_known_cache_shared_names_reclaim_once(tmp_path, monkeypatch):
    root, data, _ = source(tmp_path, monkeypatch)
    second = tmp_path / 'artifacts/cache/two'
    second.mkdir()
    os.link(data, second / 'data.npy')
    blocks = data.stat().st_blocks * 512
    ack, records = make_ack(tmp_path, ['cache/one', 'cache/two'])
    first = retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / 'state')
    assert first['reclaimed_allocated_bytes'] == 0
    ack['root'] = records[1]
    ack['identity_sha256'] = archive.digest({k: v for k, v in ack.items() if k != 'identity_sha256'})
    second_result = retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / 'state')
    assert second_result['reclaimed_allocated_bytes'] == blocks


def shared_name_ack(ack):
    ack = {**ack, "contract": "d_verified_bulk_archive_return_v2",
           "shared_file_policy": "unlink_preserved_names_only"}
    ack["identity_sha256"] = archive.digest({k: v for k, v in ack.items() if k != "identity_sha256"})
    return ack


def test_v2_removes_only_fully_preserved_names_and_keeps_external_alias(tmp_path, monkeypatch):
    root, data, ack = source(tmp_path, monkeypatch)
    outside = tmp_path / "external-original.npy"
    os.link(data, outside)
    original = outside.read_bytes()
    result = retirement.retire(tmp_path, shared_name_ack(ack), apply=True, state_root=tmp_path / "state")
    assert result["deleted"] and not root.exists()
    assert outside.read_bytes() == original and outside.stat().st_nlink == 1
    assert result["reclaimed_allocated_bytes"] == 0
    assert result["external_shared_names_deleted"] is False


@pytest.mark.parametrize("mapping", [False, True])
def test_shared_external_fd_or_mmap_blocks_v2_retirement(tmp_path, monkeypatch, mapping):
    root, data, ack = source(tmp_path, monkeypatch)
    outside = tmp_path / "active-alias.npy"
    os.link(data, outside)
    program = """import mmap,os,sys,time
f=open(sys.argv[1],'rb')
if sys.argv[2]=='mapped':
    key=(os.fstat(f.fileno()).st_dev,os.fstat(f.fileno()).st_ino)
    view=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ)
    for name in os.listdir('/proc/self/fd'):
        try:
            fd=int(name); info=os.fstat(fd)
            if (info.st_dev,info.st_ino)==key: os.close(fd)
        except OSError: pass
print('ready',flush=True)
time.sleep(60)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(outside),
                                "mapped" if mapping else "open"], stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "ready"
        result = retirement.retire(tmp_path, shared_name_ack(ack), apply=True, state_root=tmp_path / "state")
        assert result["state"] == "source-protected" and "shared-inode-in-use" in result["blockers"]
        assert root.is_dir() and outside.is_file()
    finally:
        process.terminate()
        process.wait(timeout=5)
        process.stdout.close()


def test_v2_internal_aliases_reclaim_allocated_bytes_once(tmp_path, monkeypatch):
    root, data, _ = source(tmp_path, monkeypatch)
    blocks = data.stat().st_blocks * 512
    os.link(data, root / "another.npy")
    ack, _ = make_ack(tmp_path, ["cache/one"])
    result = retirement.retire(tmp_path, shared_name_ack(ack), apply=True, state_root=tmp_path / "state")
    assert result["reclaimed_allocated_bytes"] == blocks and not root.exists()


def test_shared_original_inode_is_hashed_once_per_verification_pass(tmp_path, monkeypatch):
    root, data, _ = source(tmp_path, monkeypatch)
    os.link(data, root / 'another.npy')
    ack, _ = make_ack(tmp_path, ['cache/one'])
    calls = []
    original = retirement.sha256_file
    def counted(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(retirement, 'sha256_file', counted)
    result = retirement.retire(tmp_path, shared_name_ack(ack), apply=True, state_root=tmp_path / 'state')
    assert result['deleted'] and not root.exists()
    # Source plan, repeated plan and quarantine each read the inode afresh.
    assert len(calls) == 3


def test_shared_hash_reuse_still_checks_each_archived_name_digest(tmp_path, monkeypatch):
    root, data, _ = source(tmp_path, monkeypatch)
    os.link(data, root / 'another.npy')
    ack, _ = make_ack(tmp_path, ['cache/one'])
    row = next(r for r in ack['root']['rows'] if r['path'].endswith('/data.npy'))
    row['sha256'] = 'f' * 64
    ack['root']['root_fingerprint_sha256'] = archive.digest(ack['root']['rows'])
    with pytest.raises(SnapshotError, match='original bytes'):
        retirement.retire(tmp_path, shared_name_ack(ack), apply=True, state_root=tmp_path / 'state')
    assert root.is_dir() and data.is_file()


@pytest.mark.parametrize("contract,policy", [
    ("d_verified_bulk_archive_return_v1", "unlink_preserved_names_only"),
    ("d_verified_bulk_archive_return_v2", "delete_all_aliases"),
    ("d_verified_bulk_archive_return_v2", None),
])
def test_shared_policy_requires_its_exact_ack_version(tmp_path, monkeypatch, contract, policy):
    _, data, ack = source(tmp_path, monkeypatch)
    ack["contract"] = contract
    if policy is not None:
        ack["shared_file_policy"] = policy
    ack["identity_sha256"] = archive.digest({k: v for k, v in ack.items() if k != "identity_sha256"})
    with pytest.raises(SnapshotError, match="acknowledgement version"):
        retirement.retire(tmp_path, ack, apply=True, state_root=tmp_path / "state")
    assert data.is_file()


def test_quarantine_mutation_never_unlinks(tmp_path, monkeypatch):
    root, data, ack = source(tmp_path, monkeypatch)
    def mutate(path, _):
        if path.name.startswith('quarantine-'):
            (path / 'data.npy').write_bytes(b'unexpected unique change')
        return []
    monkeypatch.setattr(retirement, 'consumers', mutate)
    state = tmp_path / 'state'
    with pytest.raises(SnapshotError):
        retirement.retire(tmp_path, ack, apply=True, state_root=state)
    quarantines = list(state.glob('quarantine-*'))
    assert len(quarantines) == 1 and (quarantines[0] / 'data.npy').read_bytes() == b'unexpected unique change'


def test_explicit_restore_preserves_bytes_mode_mtime_and_empty_directories(tmp_path, monkeypatch):
    from stockagent.data_sync import training_return
    monkeypatch.setattr(training_return,'admit_workspace',lambda path,needed: {'required_bytes':needed})
    raw = tar_bytes([('cache/one', 'dir', None), ('cache/one/empty', 'dir', None),
                     ('cache/one/a.npy', 'file', b'original'),
                     ('cache/one/b.npy', 'link', 'cache/one/a.npy')])
    encoded = subprocess.check_output(['zstd', '-1', '-q', '-c'], input=raw)
    payload = tmp_path / 'cold.tar.zst'
    payload.write_bytes(encoded)
    index = archive.index_zstd(payload, expected_sha256=hashlib.sha256(encoded).hexdigest(), scopes={'cache'})
    destination = tmp_path / 'restored'
    result = archive.restore_original_root(payload, index, 'cache/one', destination)
    assert result['state'] == 'original_root_restored_verified'
    assert (destination / 'empty').is_dir()
    first, second = destination / 'a.npy', destination / 'b.npy'
    assert first.read_bytes() == second.read_bytes() == b'original'
    assert first.samefile(second)
    assert stat.S_IMODE(first.stat().st_mode) == 0o640
    assert first.stat().st_mtime_ns == 1234567890123456789
    assert payload.is_file() and result['deployable'] is False
    with pytest.raises(SnapshotError):
        archive.restore_original_root(payload, index, 'cache/one', destination)


def test_explicit_restore_external_hardlink_dependency_is_not_a_second_hot_copy(tmp_path, monkeypatch):
    from stockagent.data_sync import training_return
    monkeypatch.setattr(training_return,'admit_workspace',lambda path,needed: {'required_bytes':needed})
    raw = tar_bytes([('cache/first', 'dir', None), ('cache/first/a.npy', 'file', b'original'),
                     ('cache/second', 'dir', None), ('cache/second/b.npy', 'link', 'cache/first/a.npy')])
    encoded = subprocess.check_output(['zstd', '-1', '-q', '-c'], input=raw)
    payload = tmp_path / 'cold.tar.zst'
    payload.write_bytes(encoded)
    index = archive.index_zstd(payload, expected_sha256=hashlib.sha256(encoded).hexdigest(), scopes={'cache'})
    result = archive.restore_original_root(payload, index, 'cache/second', tmp_path / 'only-second')
    assert result['files'] == 1
    assert (tmp_path / 'only-second/b.npy').read_bytes() == b'original'
    assert not list(tmp_path.glob('.stockagent-bulk-dependencies-*'))
    assert not list(tmp_path.glob('.stockagent-bulk-restore-*'))


def test_closed_compressed_preservation_publishes_two_real_blobs_and_decodes_again(tmp_path, monkeypatch):
    from stockagent.data_sync import cold_primary, packed_snapshots, syncthing_scan
    cold=tmp_path/'cold';cold.mkdir()
    incoming=tmp_path/'incoming';batch=incoming/'exact-batch';batch.mkdir(parents=True)
    raw=tar_bytes([('cache/one','dir',None),('cache/one/empty','dir',None),
                   ('cache/one/weights.bin','file',b'unique old original')])
    encoded=subprocess.check_output(['zstd','-1','-q','-c'],input=raw)
    payload=batch/'cache.tar.zst';payload.write_bytes(encoded)
    expected=hashlib.sha256(encoded).hexdigest()
    index=archive.index_zstd(payload,expected_sha256=expected,scopes={'cache'})
    monkeypatch.setattr(archive,'SYNC_ROOT',cold)
    monkeypatch.setattr(archive,'NATIVE_COLD',cold)
    monkeypatch.setattr(archive,'INCOMING',incoming)
    monkeypatch.setattr(archive,'native_guard',lambda:None)
    monkeypatch.setattr(cold_primary,'d_primary_read_alias',lambda path:path)
    original_publish=packed_snapshots.publish_packed_snapshot
    original_verify=packed_snapshots.verify_packed_snapshot
    def publish(*args,**kwargs):
        assert kwargs['loose_file_threshold_bytes']>=1
        assert kwargs.pop('d_primary_native_blob_reads') is True
        return original_publish(*args,**kwargs)
    def verify(*args,**kwargs):
        assert kwargs.pop('d_primary_native_blob_reads') is True
        return original_verify(*args,**kwargs)
    monkeypatch.setattr(packed_snapshots,'publish_packed_snapshot',publish)
    monkeypatch.setattr(packed_snapshots,'verify_packed_snapshot',verify)
    monkeypatch.setattr(syncthing_scan,'scan_after_publish',lambda *_args,**_kwargs:None)
    monkeypatch.setattr(syncthing_scan,'queue_after_publish',lambda *_args,**_kwargs:True)
    proof=archive.publish_preservation(batch,{'scope':'cache','payload':str(payload),'producer_exit_code':0},index,repo_root=tmp_path)
    assert proof['cold_verified'] is True and proof['decoded_originals_verified'] is True
    resolved=packed_snapshots.resolve_packed_snapshot_id(cold,proof['dataset'],proof['snapshot_id'])
    assert len(resolved.manifest['archive']['objects'])==2
    assert {o['kind'] for o in resolved.manifest['archive']['objects']}=={'blob'}
    assert payload.samefile(archive.canonical_blobs(cold,resolved)['payload.tar.zst'])


def test_explicit_restore_requires_real_native_workspace_budget_before_scratch(tmp_path,monkeypatch):
    from stockagent.data_sync import training_return
    raw=tar_bytes([('cache/one','dir',None),('cache/one/a','file',b'original')])
    encoded=subprocess.check_output(['zstd','-1','-q','-c'],input=raw)
    payload=tmp_path/'cold.tar.zst';payload.write_bytes(encoded)
    index=archive.index_zstd(payload,expected_sha256=hashlib.sha256(encoded).hexdigest(),scopes={'cache'})
    monkeypatch.setattr(training_return,'admit_workspace',lambda *_: (_ for _ in ()).throw(SnapshotError('physical budget failed')))
    with pytest.raises(SnapshotError,match='physical budget'):
        archive.restore_original_root(payload,index,'cache/one',tmp_path/'restored')
    assert not (tmp_path/'restored').exists() and not list(tmp_path.glob('.stockagent-bulk-restore-*'))


def test_partition_reuses_verified_parts_and_retains_failed_partial(tmp_path, monkeypatch, partition_budget):
    from contextlib import contextmanager
    payload = tmp_path / 'received.zst'
    payload.write_bytes(bytes(range(256)))
    index = {'compressed_bytes': 256, 'compressed_sha256': hashlib.sha256(payload.read_bytes()).hexdigest()}
    stage = tmp_path / 'parts'
    original_reader = archive.binary_reader
    @contextmanager
    def interrupted(path):
        with original_reader(path) as stream:
            class Reader:
                count = 0
                def read(self, size):
                    self.count += 1
                    if self.count == 3:
                        raise OSError('controlled interruption')
                    return stream.read(size)
            yield Reader()
    monkeypatch.setattr(archive, 'binary_reader', interrupted)
    with pytest.raises(OSError, match='controlled interruption'):
        archive.stage_compressed_parts(payload, stage, index, part_bytes=32)
    complete = stage / 'payload.part-00000.zst'
    before = complete.stat().st_ino
    partials = list((tmp_path / 'parts-partial').glob('*.partial'))
    assert partials and not (stage / 'compressed_parts.json').exists()
    monkeypatch.setattr(archive, 'binary_reader', original_reader)
    plan = archive.stage_compressed_parts(payload, stage, index, part_bytes=32)
    assert complete.stat().st_ino == before
    assert all(p.exists() for p in partials)
    assert archive.payload_hash(tuple(stage / p['path'] for p in plan['parts'])) == index['compressed_sha256']
    assert payload.read_bytes() == bytes(range(256))


def test_partition_rejects_corrupt_retained_part_without_overwrite(tmp_path, partition_budget):
    payload = tmp_path / 'received.zst'
    payload.write_bytes(b'a' * 100)
    index = {'compressed_bytes': 100, 'compressed_sha256': hashlib.sha256(payload.read_bytes()).hexdigest()}
    stage = tmp_path / 'parts'
    archive.stage_compressed_parts(payload, stage, index, part_bytes=32)
    corrupt = stage / 'payload.part-00001.zst'
    corrupt.write_bytes(b'b' * 32)
    with pytest.raises(SnapshotError, match='readback differs'):
        archive.stage_compressed_parts(payload, stage, index, part_bytes=32)
    assert corrupt.read_bytes() == b'b' * 32 and payload.read_bytes() == b'a' * 100


def test_partition_rejects_same_byte_source_mutation(tmp_path, monkeypatch, partition_budget):
    payload = tmp_path / 'received.zst'
    payload.write_bytes(b'a' * 100)
    index = {'compressed_bytes': 100, 'compressed_sha256': hashlib.sha256(payload.read_bytes()).hexdigest()}
    original_fsync = archive._fsync_directory
    def mutate(directory):
        original_fsync(directory)
        info = payload.stat()
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns + 1000000))
    monkeypatch.setattr(archive, '_fsync_directory', mutate)
    with pytest.raises(SnapshotError, match='differs from the received frame'):
        archive.stage_compressed_parts(payload, tmp_path / 'parts', index, part_bytes=32)
    assert not (tmp_path / 'parts/compressed_parts.json').exists()


def test_chunked_canonical_preservation_decodes_and_restores_originals(tmp_path, monkeypatch, partition_budget):
    from stockagent.data_sync import cold_primary, packed_snapshots, syncthing_scan, training_return
    cold = tmp_path / 'cold'; cold.mkdir()
    incoming = tmp_path / 'incoming'; batch = incoming / 'exact'; batch.mkdir(parents=True)
    raw = tar_bytes([('cache/one', 'dir', None), ('cache/one/empty', 'dir', None),
                     ('cache/one/a.bin', 'file', bytes(range(256)) * 100),
                     ('cache/one/b.bin', 'link', 'cache/one/a.bin')])
    encoded = subprocess.check_output(['zstd', '-1', '-q', '-c'], input=raw)
    payload = batch / 'cache.tar.zst'; payload.write_bytes(encoded)
    index = archive.index_zstd(payload, expected_sha256=hashlib.sha256(encoded).hexdigest(), scopes={'cache'})
    monkeypatch.setattr(archive, 'SYNC_ROOT', cold)
    monkeypatch.setattr(archive, 'NATIVE_COLD', cold)
    monkeypatch.setattr(archive, 'INCOMING', incoming)
    monkeypatch.setattr(archive, 'PART_BYTES', 64)
    monkeypatch.setattr(archive, 'native_guard', lambda: None)
    monkeypatch.setattr(cold_primary, 'd_primary_read_alias', lambda path: path)
    monkeypatch.setattr(training_return, 'admit_workspace', lambda *_: None)
    original_publish, original_verify = packed_snapshots.publish_packed_snapshot, packed_snapshots.verify_packed_snapshot
    def publish(*args, **kwargs):
        assert kwargs.pop('d_primary_native_blob_reads') is True
        return original_publish(*args, **kwargs)
    def verify(*args, **kwargs):
        assert kwargs.pop('d_primary_native_blob_reads') is True
        return original_verify(*args, **kwargs)
    monkeypatch.setattr(packed_snapshots, 'publish_packed_snapshot', publish)
    monkeypatch.setattr(packed_snapshots, 'verify_packed_snapshot', verify)
    monkeypatch.setattr(syncthing_scan, 'scan_after_publish', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(syncthing_scan, 'queue_after_publish', lambda *_args, **_kwargs: True)
    proof = archive.publish_preservation(batch, {'scope': 'cache', 'payload': str(payload), 'producer_exit_code': 0}, index, repo_root=tmp_path)
    assert proof['preservation_contract'] == archive.PARTS_CONTRACT
    assert proof['cold_verified'] is True and proof['decoded_originals_verified'] is True
    resolved = packed_snapshots.resolve_packed_snapshot_id(cold, proof['dataset'], proof['snapshot_id'])
    files = archive.canonical_blobs(cold, resolved)
    assert archive.verify_retained_preservation(proof) == files
    parts = archive.preservation_payload(files)
    assert len(parts) > 1 and all(p.stat().st_size <= 64 for p in parts)
    assert archive.index_zstd(parts, expected_sha256=index['compressed_sha256'], scopes={'cache'})['rows'] == index['rows']
    restored = tmp_path / 'restored'
    result = archive.restore_original_root(parts, index, 'cache/one', restored)
    assert result['state'] == 'original_root_restored_verified'
    assert (restored / 'a.bin').samefile(restored / 'b.bin') and (restored / 'empty').is_dir()
    assert (restored / 'a.bin').read_bytes() == bytes(range(256)) * 100
    assert (restored / 'a.bin').stat().st_mtime_ns == 1234567890123456789
    with pytest.raises((SnapshotError, tarfile.ReadError, BrokenPipeError)):
        archive.index_zstd(tuple(reversed(parts)), expected_sha256=index['compressed_sha256'], scopes={'cache'})
    plan = json.loads(files['compressed_parts.json'].read_text())
    plan['parts'].reverse()
    with pytest.raises(SnapshotError, match='order'):
        archive.validate_parts_plan(plan)
    assert payload.read_bytes() == encoded
    parts[0].write_bytes(b'x' * parts[0].stat().st_size)
    with pytest.raises(SnapshotError, match='changed after independent decode'):
        archive.verify_retained_preservation(proof)


def test_native_blob_link_targets_the_physical_mount_alias(tmp_path, monkeypatch):
    from stockagent.data_sync import cold_primary
    physical = tmp_path / 'physical'; physical.mkdir()
    canonical = tmp_path / 'canonical'; canonical.symlink_to(physical, target_is_directory=True)
    payload = tmp_path / 'incoming'; payload.write_bytes(b'exact')
    monkeypatch.setattr(archive, 'SYNC_ROOT', canonical)
    monkeypatch.setattr(archive, 'NATIVE_COLD', physical)
    monkeypatch.setattr(archive, 'native_guard', lambda: None)
    monkeypatch.setattr(cold_primary, 'd_primary_read_alias', lambda path: path)
    original_link, called = os.link, []
    def same_mount(source, destination, **kwargs):
        assert destination.is_relative_to(physical)
        called.append(destination)
        return original_link(source, destination, **kwargs)
    monkeypatch.setattr(archive.os, 'link', same_mount)
    result = archive.install_native_blob(payload, hashlib.sha256(b'exact').hexdigest())
    assert called and result.is_relative_to(canonical) and result.samefile(payload)


@pytest.mark.parametrize('partitioned', [False, True])
def test_publication_rejects_changed_carrier_against_received_proof_before_cas_admission(
        tmp_path, monkeypatch, partition_budget, partitioned):
    cold = tmp_path / 'cold'; cold.mkdir()
    incoming = tmp_path / 'incoming'; batch = incoming / 'exact'; batch.mkdir(parents=True)
    payload = batch / 'cache.tar.zst'; payload.write_bytes(b'original' * 8)
    index = {'compressed_bytes': payload.stat().st_size,
             'compressed_sha256': hashlib.sha256(payload.read_bytes()).hexdigest()}
    monkeypatch.setattr(archive, 'SYNC_ROOT', cold)
    monkeypatch.setattr(archive, 'NATIVE_COLD', cold)
    monkeypatch.setattr(archive, 'INCOMING', incoming)
    monkeypatch.setattr(archive, 'native_guard', lambda: None)
    if partitioned:
        monkeypatch.setattr(archive, 'PART_BYTES', 16)
        original_stage = archive.stage_compressed_parts
        def changed_part(*args, **kwargs):
            plan = original_stage(*args, **kwargs)
            part = args[1] / plan['parts'][0]['path']
            part.write_bytes(b'X' * part.stat().st_size)
            return plan
        monkeypatch.setattr(archive, 'stage_compressed_parts', changed_part)
    else:
        payload.write_bytes(b'X' * payload.stat().st_size)
    with pytest.raises(SnapshotError, match='incoming blob differs from proof'):
        archive.publish_preservation(batch, {'scope': 'cache', 'payload': str(payload),
                                            'producer_exit_code': 0}, index, repo_root=tmp_path)
    assert not list(cold.rglob('*.blob')) and not list(cold.rglob('*.manifest.json'))


def test_ordered_carrier_can_reuse_one_deduplicated_cas_blob(tmp_path):
    part = tmp_path / 'blob'; part.write_bytes(b'original carrier bytes')
    paths = (part, part, part)
    assert archive.payload_hash(paths) == hashlib.sha256(part.read_bytes() * 3).hexdigest()
    assert len(archive.payload_signatures(paths)) == 3
