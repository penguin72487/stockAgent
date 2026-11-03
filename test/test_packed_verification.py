"""Coverage, fault isolation and real overlap for cooperative pack recovery."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time
from types import SimpleNamespace
import uuid
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import packed_verification as lanes


def pack_bytes(files):
    source = io.BytesIO()
    with zipfile.ZipFile(source, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, value in sorted(files.items()):
            archive.writestr(name, value)
    body = source.getvalue()
    item = {'kind': 'pack', 'sha256': hashlib.sha256(body).hexdigest(),
            'bytes': len(body), 'file_count': len(files)}
    rows = [[name, len(value), hashlib.sha256(value).hexdigest()]
            for name, value in sorted(files.items())]
    return body, item, rows


def test_exact_original_commitment_includes_zero_length_unicode_and_raw_values():
    body, item, rows = pack_bytes({'zero': b'', '數值': b'-10,1.0,NaN\n'})
    proof = lanes.inspect_pack(io.BytesIO(body), item, rows)
    assert all(proof[k] == v for k, v in lanes.expected_result(item, rows).items())
    assert proof['stored_members_crc_verified'] == 2
    wrong = [row[:] for row in rows]
    wrong[1][2] = '0'*64
    with pytest.raises(ValueError, match='original pack SHA'):
        lanes.inspect_pack(io.BytesIO(body), item, wrong)


def test_subset_still_checks_crc_of_unselected_original():
    body, item, rows = pack_bytes({'a': b'first', 'b': b'original unselected bytes'})
    item.update(member_selection='subset', stored_file_count=2, file_count=1)
    proof = lanes.inspect_pack(io.BytesIO(body), item, rows[:1])
    assert proof['files'] == 1 and proof['stored_members_crc_verified'] == 2
    broken = body.replace(b'original unselected bytes', b'corrupt! unselected bytes')
    assert len(broken) == len(body)
    item.update(sha256=hashlib.sha256(broken).hexdigest())
    with pytest.raises(zipfile.BadZipFile, match='CRC'):
        lanes.inspect_pack(io.BytesIO(broken), item, rows[:1])


@pytest.mark.parametrize('defect', ['size', 'hash', 'member_order', 'missing_member'])
def test_pack_cannot_attest_incomplete_or_different_inventory(defect):
    body, item, rows = pack_bytes({'a': b'1', 'b': b'2'})
    if defect == 'size':
        item['bytes'] += 1
    elif defect == 'hash':
        item['sha256'] = '0'*64
    elif defect == 'member_order':
        rows.reverse()
    else:
        rows.pop()
    with pytest.raises(ValueError):
        lanes.inspect_pack(io.BytesIO(body), item, rows)


def test_persistent_worker_checks_two_exact_jobs_without_creating_a_payload_file(tmp_path):
    body, item, rows = pack_bytes({'a': b'original'})
    worker = lanes.StreamSession([sys.executable, lanes.__file__], 5)
    request = {'mode': 'verify', 'manifest_sha256': 'a'*64, 'item': item, 'rows': rows,
               'boot_id': worker.identity['boot_id'], 'worker_sha256': lanes.SOURCE_SHA256, 'timeout_seconds': 5}
    try:
        for _ in range(2):
            proof = worker.request({**request, 'job_id': uuid.uuid4().hex}, io.BytesIO(body))
            assert proof['originals_sha256'] == lanes.commitment(rows)
            assert proof['payload_storage'] == 'anonymous_memory'
    finally:
        assert worker.close()['state'] == 'memory_worker_closed'
    assert worker.process.returncode == 0
    assert not list(tmp_path.iterdir())


def test_pipe_timeout_releases_its_exported_payload_even_when_traceback_is_retained():
    read, write = os.pipe()
    os.set_blocking(write, False)
    payload = io.BytesIO(b'x'*1024**2)
    error = None
    try:
        with payload.getbuffer() as view:
            try:
                lanes._pipe_io(write, data=view, deadline=time.monotonic()+0.02)
            except TimeoutError as exc:
                error = exc
        assert error is not None
        payload.close()  # A traceback must not leave a live memoryview export.
    finally:
        os.close(read); os.close(write)


def test_worker_rejects_a_different_boot_before_accepting_any_raw_bytes():
    worker = lanes.StreamSession([sys.executable, lanes.__file__], 5)
    try:
        with pytest.raises(EOFError):
            worker.request({'mode': 'verify', 'boot_id': 'wrong', 'worker_sha256': lanes.SOURCE_SHA256})
    finally:
        worker.abort()


def fixture_release(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    for n in range(12):
        (source/f'file-{n:02d}').write_bytes(bytes([n])*1024)
    packed.initialize_packed_layout(cold)
    result = packed.publish_packed_snapshot(cold, 'pool-test', source, pack_buckets=8)
    return cold, result


def pool_options(tmp_path):
    return {'local_workers': 1, 'remote_workers': 1,
            'initial_local_bytes_per_second': 1, 'initial_remote_bytes_per_second': 10**12,
            'evidence_directory': str(tmp_path/'proofs')}


def test_shared_pool_overlaps_disjoint_local_remote_and_keeps_sqlite_on_owner_thread(tmp_path, monkeypatch):
    cold, release = fixture_release(tmp_path)
    original_inspect = lanes.inspect_pack
    both = threading.Barrier(2)
    first = threading.local()
    reads, nodes = [], []

    @contextlib.contextmanager
    def native(path, item, **kwargs):
        reads.append(item['sha256'])
        with io.BytesIO(path.read_bytes()) as stream:
            yield stream

    def inspect(source, item, rows):
        if not getattr(first, 'arrived', False):
            first.arrived = True
            both.wait(timeout=10)
        return original_inspect(source, item, rows)

    class Remote:
        def __init__(self, *args):
            self.disabled = threading.Event()
            self.root, self.boot_id = '/owned/probe', 'fixed-boot'
        def verify(self, source, item, rows):
            nodes.append('vast')
            return inspect(source, item, rows)
        def close(self):
            return {'state': 'owned_session_removed'}

    monkeypatch.setattr(packed, '_native_pack_source', native)
    monkeypatch.setattr(lanes, 'inspect_pack', inspect)
    monkeypatch.setattr(lanes, 'RemotePacks', Remote)
    monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', 1)
    updates = []
    proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True,
                                          pack_verification=pool_options(tmp_path), progress=updates.append)
    assert len(reads) == len(set(reads)) == len(release.manifest['archive']['objects'])
    assert proof['independently_reconstructed_files'] == 12
    assert proof['independently_reconstructed_bytes'] == 12*1024
    assert proof['pack_reconstruction_execution']['nodes']['vast'] > 0
    assert proof['pack_reconstruction_execution']['nodes']['local'] > 0
    assert nodes and any(p['stage'] == 'parallel-pack-reconstruction' for p in updates)
    assert all(p.get('acceptance') is not True for p in updates)


def test_remote_timeout_falls_back_to_exact_buffer_once_and_does_not_block_later_jobs(tmp_path, monkeypatch):
    cold, release = fixture_release(tmp_path)
    reads, attempts = [], []
    @contextlib.contextmanager
    def native(path, item, **kwargs):
        reads.append(item['sha256'])
        yield io.BytesIO(path.read_bytes())
    class Remote:
        def __init__(self, *args):
            self.disabled = threading.Event()
            self.root, self.boot_id = '/owned/probe', 'fixed-boot'
        def verify(self, source, item, rows):
            attempts.append(item['sha256'])
            raise TimeoutError('offline')
        def close(self):
            return {'state': 'retained_incomplete_scratch'}
    monkeypatch.setattr(packed, '_native_pack_source', native)
    monkeypatch.setattr(lanes, 'RemotePacks', Remote)
    proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True,
                                          pack_verification=pool_options(tmp_path))
    assert proof['independently_reconstructed_files'] == 12
    assert len(attempts) == 1
    assert len(reads) == len(set(reads))
    receipts = [json.loads(p.read_bytes()) for p in (tmp_path/'proofs').glob('*.json')]
    assert any(p.get('remote_failure') == 'TimeoutError' for p in receipts)


def test_changed_source_cannot_be_accepted_after_valid_remote_commitment(tmp_path, monkeypatch):
    cold, release = fixture_release(tmp_path)
    class Remote:
        def __init__(self, *args):
            self.disabled = threading.Event()
            self.root, self.boot_id = '/owned/probe', 'fixed-boot'
        def verify(self, source, item, rows):
            result = lanes.inspect_pack(source, item, rows)
            (cold/item['relpath']).touch()
            return result
        def close(self):
            return {}
    @contextlib.contextmanager
    def native(path, item, **kwargs):
        yield io.BytesIO(path.read_bytes())
    monkeypatch.setattr(packed, '_native_pack_source', native)
    monkeypatch.setattr(lanes, 'RemotePacks', Remote)
    with pytest.raises(packed.SnapshotError, match='generation changed'):
        packed.verify_packed_snapshot(cold, release, reconstruct_all=True,
                                     pack_verification=pool_options(tmp_path))
    assert not (tmp_path/'proofs/summary.json').exists()


def test_pool_is_not_allowed_to_attest_a_partial_reconstruction(tmp_path):
    cold, release = fixture_release(tmp_path)
    with pytest.raises(packed.SnapshotError, match='complete original'):
        packed.verify_packed_snapshot(cold, release, pack_verification=pool_options(tmp_path))


def test_policy_requires_private_file_and_explicit_dataset_enrollment(tmp_path):
    path = tmp_path/'policy.json'
    data = json.loads(Path('configs/data_sync/pack_verification.example.json').read_bytes())
    data['enabled'] = True
    path.write_text(json.dumps(data))
    path.chmod(0o600)
    assert lanes.configuration('other-source', path) is None
    assert lanes.configuration('local-openbb', path)['policy_sha256']
    path.chmod(0o644)
    with pytest.raises(ValueError, match='private'):
        lanes.configuration('local-openbb', path)


def test_wrong_remote_original_commitment_aborts_only_its_worker():
    body, item, rows = pack_bytes({'a': b'exact original'})
    class Session:
        aborted = False
        def request(self, request, source):
            return {**lanes.expected_result(item, rows), **request,
                    'originals_sha256': '0'*64, 'stored_members_crc_verified': 1}
        def abort(self):
            self.aborted = True
    remote = lanes.RemotePacks.__new__(lanes.RemotePacks)
    remote.options = {'timeout_seconds': 1}
    remote.boot_id, remote.manifest_sha256 = 'fixed-boot', 'a'*64
    remote.available = queue.Queue()
    session = Session()
    remote.available.put(session)
    with pytest.raises(ValueError, match='commitment differs'):
        remote.verify(io.BytesIO(body), item, rows)
    assert session.aborted and remote.available.get_nowait() is session


def test_canonical_recovery_enrolls_pool_but_preserves_accepted_proof_without_repeating_work(tmp_path, monkeypatch):
    from stockagent.data_sync import dataset_migration as migration
    cold = Path('/srv/stockagent-packed/.local-state/private-history')
    selected = SimpleNamespace(manifest_sha256='b'*64, manifest={'source': {'files': 2}})
    seen = []
    def resolve(*args, **kwargs):
        seen.append(kwargs['require_objects'])
        return selected
    monkeypatch.setattr(migration, 'resolve_packed_snapshot_id', resolve)
    monkeypatch.setattr(lanes, 'configuration', lambda dataset: {'policy_sha256': 'c'*64})
    calls = []
    def verify(*args, **kwargs):
        calls.append(kwargs)
        return {'independently_reconstructed_files': 2}
    monkeypatch.setattr(migration, 'verify_packed_snapshot', verify)
    receipt, progress = tmp_path/'recovery.json', tmp_path/'progress.json'
    proof = migration._recover(cold, 'test-dataset', 'fixed-version', receipt, progress)
    assert calls[0]['reconstruct_all'] is True
    assert calls[0]['pack_verification']['policy_sha256'] == 'c'*64
    assert calls[0]['pack_verification']['evidence_directory'].startswith(str(tmp_path/'pack-verification/test-dataset'))
    assert proof['proof_scope'] == 'fixed_D_release_not_NAS_or_source_retirement'
    original = receipt.read_bytes()
    monkeypatch.setattr(lanes, 'configuration', lambda *_: pytest.fail('accepted proof must not start remote work'))
    assert migration._recover(cold, 'test-dataset', 'fixed-version', receipt, progress) == proof
    assert receipt.read_bytes() == original and len(calls) == 1
    assert seen == [False, True]
