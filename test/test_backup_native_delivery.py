"""Native read admission preserves exact bytes and rejects partial/corrupt data."""
from contextlib import contextmanager
import hashlib
import json

import pytest

from scripts.verify_backup_delivery import CONTRACT, identity, verify
from stockagent.data_sync.offhost_backup import copy_verified_bytes
from stockagent.data_sync import windows_cold_io
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('native', [False, True])
def test_large_closed_delivery_rejects_same_size_corruption(tmp_path, native):
    payload = bytes(range(256)) * (9 * 1024**2 // 256)
    member = tmp_path / 'source.bin'
    member.write_bytes(payload)
    body = {'contract':CONTRACT, 'source_plan_identity_sha256':'a' * 64,
            'files':[{'relative':'source.bin', 'sha256':hashlib.sha256(payload).hexdigest(), 'bytes':len(payload)}]}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body,'identity_sha256':key}))
    (tmp_path / 'READY').write_text(key+'\n')
    result = verify(tmp_path, key, native_windows_io=native)
    assert result['bytes_verified'] == len(payload)
    with member.open('r+b') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='content differs'):
        verify(tmp_path, key, native_windows_io=native)


def test_native_restore_reader_failure_retains_source_and_partial(tmp_path, monkeypatch):
    source = tmp_path / 'source.bin'
    payload = b'original observation\n' * 1024
    source.write_bytes(payload)
    destination = tmp_path / 'restore.bin'

    class BrokenReader:
        reads = 0
        def read(self, size):
            self.reads += 1
            if self.reads == 1:
                return payload[:128]
            raise OSError('actual bridge read failed')

    @contextmanager
    def fail_reader(path):
        assert path == source
        yield BrokenReader()

    monkeypatch.setattr(windows_cold_io, 'binary_reader', fail_reader)
    with pytest.raises(OSError, match='bridge read failed'):
        copy_verified_bytes(source, destination, expected_sha256=hashlib.sha256(payload).hexdigest(),
                            expected_bytes=len(payload), native_windows_io=True)
    assert source.read_bytes() == payload
    assert destination.read_bytes() == payload[:128]
    with pytest.raises(FileExistsError):
        copy_verified_bytes(source, destination, expected_sha256=hashlib.sha256(payload).hexdigest(),
                            expected_bytes=len(payload), native_windows_io=True)


def test_native_restore_cannot_accept_wrong_expected_hash(tmp_path):
    source = tmp_path / 'source.bin'
    source.write_bytes(b'authoritative original')
    with pytest.raises(SnapshotError, match='canonical selection'):
        copy_verified_bytes(source, tmp_path / 'restored.bin', expected_sha256='f' * 64,
                            expected_bytes=source.stat().st_size, native_windows_io=True)
    assert source.read_bytes() == b'authoritative original'
