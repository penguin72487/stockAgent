"""Native read admission preserves exact bytes and rejects partial/corrupt data."""
from contextlib import contextmanager
import hashlib
import json
import os
import threading
from types import SimpleNamespace

import pytest

from scripts.verify_backup_delivery import CONTRACT, identity, verify
from scripts import verify_backup_delivery
from stockagent.data_sync.offhost_backup import copy_verified_bytes
from stockagent.data_sync import windows_cold_io
from stockagent.data_sync import cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_windows_path_and_fd_identity_can_have_different_ctime():
    path_stat = SimpleNamespace(st_dev=1, st_ino=2, st_size=3, st_mtime_ns=4,
                                st_ctime_ns=5, st_mode=0o100600, st_nlink=1)
    opened = SimpleNamespace(**vars(path_stat))
    opened.st_ctime_ns = 6
    assert verify_backup_delivery._descriptor_identity(path_stat, windows=True) == (
        verify_backup_delivery._descriptor_identity(opened, windows=True))
    assert verify_backup_delivery._descriptor_identity(path_stat, windows=False) != (
        verify_backup_delivery._descriptor_identity(opened, windows=False))
    opened.st_ino += 1
    assert verify_backup_delivery._descriptor_identity(path_stat, windows=True) != (
        verify_backup_delivery._descriptor_identity(opened, windows=True))


def test_native_hash_groups_respect_member_and_total_size_bounds():
    groups = verify_backup_delivery._native_hash_groups([{'bytes': 8 * 1024**3}] * 3)
    assert [len(group) for group in groups] == [2, 1]
    groups = verify_backup_delivery._native_hash_groups([{'bytes': 1}] * 17)
    assert [len(group) for group in groups] == [16, 1]


def test_native_small_members_use_bounded_full_hash_batches(tmp_path, monkeypatch):
    # Exercise batching independently of the production size-routing threshold.
    monkeypatch.setattr(verify_backup_delivery, 'PORTABLE_HASH_MAX_BYTES', 0)
    rows = []
    for number in range(35):
        raw = f'original body {number}'.encode()
        name = f'{number}.bin'
        (tmp_path / name).write_bytes(raw)
        rows.append({'relative': name, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)})
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64, 'files': rows}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    calls = []
    monkeypatch.setattr(windows_cold_io, 'windows_path', lambda path: 'D:\\test\\' + path.name)
    def hashes(paths, *, cancel_event=None):
        calls.append(len(paths))
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(windows_cold_io, 'hash_many', hashes)
    proof = verify(tmp_path, key, workers=2, native_windows_io=True)
    assert sorted(calls) == [3, 16, 16]
    assert proof['files_verified'] == 35
    (tmp_path / '5.bin').write_bytes(b'corrupted body 5')
    with pytest.raises(ValueError, match='content differs'):
        verify(tmp_path, key, workers=2, native_windows_io=True)


def test_native_hash_batch_cannot_hide_a_generation_change(tmp_path, monkeypatch):
    monkeypatch.setattr(verify_backup_delivery, 'PORTABLE_HASH_MAX_BYTES', 0)
    raw = b'original body'
    path = tmp_path / 'body.bin'
    path.write_bytes(raw)
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64,
            'files': [{'relative': path.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}]}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    monkeypatch.setattr(windows_cold_io, 'windows_path', lambda path: 'D:\\test\\' + path.name)
    def changed(paths):
        hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
        path.write_bytes(b'a different version')
        return hashes
    monkeypatch.setattr(windows_cold_io, 'hash_many', changed)
    with pytest.raises(ValueError, match='member changed'):
        verify(tmp_path, key, native_windows_io=True)


def test_small_and_large_members_route_without_skipping_byte_verification(tmp_path, monkeypatch):
    rows = []
    for name, size in [('small.bin', 1024**2), ('large.bin', 1024**2 + 1)]:
        raw = b'a' * size
        (tmp_path / name).write_bytes(raw)
        rows.append({'relative': name, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': size})
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64, 'files': rows}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    calls = []
    monkeypatch.setattr(windows_cold_io, 'windows_path', lambda path: 'D:\\test\\' + path.name)
    def hashes(paths, *, cancel_event=None):
        calls.extend(path.name for path in paths)
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(windows_cold_io, 'hash_many', hashes)
    proof = verify(tmp_path, key, workers=2, native_windows_io=True, include_file_signatures=True)
    assert calls == ['large.bin']
    assert proof['bytes_verified'] == sum(row['bytes'] for row in rows)
    assert all(len(value) == 5 for value in proof['file_signatures'].values())
    with (tmp_path / 'small.bin').open('r+b') as stream:
        stream.write(b'b')
    with pytest.raises(ValueError, match='content differs'):
        verify(tmp_path, key, workers=2, native_windows_io=True)


def test_portable_hash_can_overlap_the_last_native_batch(tmp_path, monkeypatch):
    monkeypatch.setattr(verify_backup_delivery, 'PORTABLE_HASH_MAX_BYTES', 1)
    rows = []
    for index in range(17):
        name = f'native-{index}.bin'
        (tmp_path / name).write_bytes(b'aa')
        rows.append({'relative': name, 'sha256': hashlib.sha256(b'aa').hexdigest(), 'bytes': 2})
    member = tmp_path / 'portable.bin'
    member.write_bytes(b'b')
    rows.append({'relative': member.name, 'sha256': hashlib.sha256(b'b').hexdigest(), 'bytes': 1})
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64, 'files': rows}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    portable_opened = threading.Event()
    monkeypatch.setattr(windows_cold_io, 'windows_path', lambda path: 'D:\\test\\' + path.name)
    def hashes(paths, *, cancel_event=None):
        if len(paths) == 16 and not portable_opened.wait(5):
            raise TimeoutError('portable work was not scheduled alongside the final native batch')
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(windows_cold_io, 'hash_many', hashes)
    real_open = os.open
    def observe_open(path, flags, *args, **kwargs):
        if os.fspath(path) == os.fspath(member):
            portable_opened.set()
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', observe_open)
    proof = verify(tmp_path, key, workers=2, native_windows_io=True)
    assert portable_opened.is_set()
    assert proof['files_verified'] == 18


def test_failed_native_group_cancels_waiting_peer_and_preserves_original_error(tmp_path, monkeypatch):
    monkeypatch.setattr(verify_backup_delivery, 'PORTABLE_HASH_MAX_BYTES', 0)
    rows = []
    for index in range(17):
        path = tmp_path / f'{index:02}.bin'
        path.write_bytes(b'original')
        rows.append({'relative': path.name, 'sha256': hashlib.sha256(b'original').hexdigest(),
                     'bytes': len(b'original')})
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64, 'files': rows}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    peer_started = threading.Event()
    peer_cancelled = threading.Event()
    monkeypatch.setattr(windows_cold_io, 'windows_path', lambda path: 'D:\\test\\' + path.name)
    def hashes(paths, *, cancel_event):
        if len(paths) == 16:
            peer_started.set()
            if not cancel_event.wait(5):
                raise TimeoutError('failed peer did not signal cancellation')
            peer_cancelled.set()
            raise SnapshotError('peer cancelled')
        assert peer_started.wait(5)
        raise ValueError('original verifier failure')
    monkeypatch.setattr(windows_cold_io, 'hash_many', hashes)
    with pytest.raises(ValueError, match='original verifier failure'):
        verify(tmp_path, key, workers=2, native_windows_io=True)
    assert peer_cancelled.is_set()
    assert all((tmp_path / row['relative']).read_bytes() == b'original' for row in rows)


def test_direct_d_hash_checks_registered_volume_before_and_after(tmp_path, monkeypatch):
    member = tmp_path / 'small.bin'
    member.write_bytes(b'a')
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64,
            'files': [{'relative': member.name, 'sha256': hashlib.sha256(b'a').hexdigest(), 'bytes': 1}]}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    monkeypatch.setattr(windows_cold_io, 'ALIASES', ((tmp_path, 'test'),))
    checks = []
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda path: checks.append('mount'))
    monkeypatch.setattr(windows_cold_io, '_configuration_on_checked_mount',
                        lambda *args, **kwargs: checks.append('ingress'))
    assert verify(tmp_path, key, native_windows_io=True)['files_verified'] == 1
    assert checks == ['mount', 'ingress', 'mount', 'ingress']
    def failed_after_initial_mount(path):
        checks.append('mount')
        if len(checks) > 2:
            raise SnapshotError('D backing changed')
    checks.clear()
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', failed_after_initial_mount)
    with pytest.raises(SnapshotError, match='D backing changed'):
        verify(tmp_path, key, native_windows_io=True)


@pytest.mark.skipif(not hasattr(os, 'O_NOFOLLOW'), reason='requires no-follow file opens')
def test_small_member_cannot_be_swapped_for_symlink_before_open(tmp_path, monkeypatch):
    member = tmp_path / 'small.bin'
    raw = b'original'
    member.write_bytes(raw)
    outside = tmp_path.parent / (tmp_path.name + '-outside')
    outside.write_bytes(raw)
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64,
            'files': [{'relative': member.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}]}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    real_open = os.open
    def swap_then_open(path, flags, *args, **kwargs):
        if os.fspath(path) == os.fspath(member):
            member.unlink()
            member.symlink_to(outside)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', swap_then_open)
    with pytest.raises((ValueError, OSError)):
        verify(tmp_path, key, native_windows_io=True)


@pytest.mark.parametrize('native', [False, True])
def test_small_member_cannot_be_replaced_with_identical_bytes_before_open(tmp_path, monkeypatch, native):
    raw = b'identical bytes'
    member = tmp_path / 'small.bin'
    member.write_bytes(raw)
    replacement = tmp_path.parent / (tmp_path.name + '-replacement')
    replacement.write_bytes(raw)
    body = {'contract': CONTRACT, 'source_plan_identity_sha256': 'a' * 64,
            'files': [{'relative': member.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}]}
    key = identity(body)
    (tmp_path / 'backup-envelope.json').write_text(json.dumps({**body, 'identity_sha256': key}))
    (tmp_path / 'READY').write_text(key + '\n')
    real_open = os.open
    def replace_then_open(path, flags, *args, **kwargs):
        if os.fspath(path) == os.fspath(member):
            os.replace(replacement, member)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', replace_then_open)
    with pytest.raises(ValueError, match='member changed'):
        verify(tmp_path, key, native_windows_io=native)


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
