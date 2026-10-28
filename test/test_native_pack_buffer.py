"""Native pack buffers preserve the cold-byte and resource-admission fences."""
from contextlib import contextmanager
import hashlib
import io
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent import remote_build


@pytest.fixture
def original(tmp_path, monkeypatch):
    payload = b'original immutable pack\n' * 60_000
    path = tmp_path / 'original.zip'
    path.write_bytes(payload)
    item = {'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest(), 'file_count': 405}
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed', lambda *_: True)
    monkeypatch.setattr(packed.shutil, 'disk_usage', lambda *_: pytest.fail('buffer allocated C scratch'))
    return path, item, payload


def reader_for(payload, after=None):
    @contextmanager
    def reader(path, *, expected_bytes):
        assert expected_bytes > 1024**2
        yield io.BytesIO(payload)
        if after:
            after(path)
    return reader


def test_complete_native_exit_precedes_exposing_buffer_and_buffer_is_not_retained(original, monkeypatch):
    path, item, payload = original
    finished = []
    monkeypatch.setattr(native, 'binary_reader', reader_for(payload, lambda _: finished.append(True)))
    with packed._native_pack_source(path, item, enabled=True) as buffer:
        assert finished == [True]
        assert buffer.read() == payload
    assert buffer.closed


@pytest.mark.parametrize('failure', ['truncated', 'excess', 'checksum', 'native_exit', 'source_changed'])
def test_native_failure_never_exposes_unverified_bytes(original, monkeypatch, failure):
    path, item, payload = original
    data = payload[:-1] if failure == 'truncated' else payload + b'x' if failure == 'excess' else payload
    if failure == 'checksum':
        item = {**item, 'sha256': '0' * 64}

    def finish(selected):
        if failure == 'native_exit':
            raise SnapshotError('native child failed')
        if failure == 'source_changed':
            info = selected.stat()
            os.utime(selected, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))

    monkeypatch.setattr(native, 'binary_reader', reader_for(data, finish))
    with pytest.raises(SnapshotError):
        with packed._native_pack_source(path, item, enabled=True):
            pytest.fail('consumer saw bytes before the native transaction was accepted')


def test_buffer_still_rejects_original_change_during_consumer_use(original, monkeypatch):
    path, item, payload = original
    monkeypatch.setattr(native, 'binary_reader', reader_for(payload))
    with pytest.raises(SnapshotError, match='during reconstruction'):
        with packed._native_pack_source(path, item, enabled=True) as buffer:
            assert buffer.read(1) == payload[:1]
            info = path.stat()
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
    assert buffer.closed


@pytest.mark.parametrize('available,allowed', [
    (0, False), (4 * 8 * 1024**2 + 512 * 1024**2 - 1, False),
    (4 * 8 * 1024**2 + 512 * 1024**2, True), ('unverified', False), (True, False),
])
def test_admission_uses_observed_effective_headroom(monkeypatch, available, allowed):
    monkeypatch.setattr(remote_build, 'observe_node', lambda *_: {'limits': {'memory_headroom_bytes': available}})
    assert packed._native_pack_buffer_allowed(Path('/fixed/pack'), 8 * 1024**2) is allowed


@pytest.mark.parametrize('count', [134_390_589, 198_594_344])
def test_original_financial_packs_above_128_mib_still_need_exact_headroom(monkeypatch, count):
    # The real 3,541-member pack and the largest declared financial source
    # pack previously fell back to portable seeks despite sufficient RAM.
    required = 4 * count + 512 * 1024**2
    monkeypatch.setattr(remote_build, 'observe_node',
                        lambda *_: {'limits': {'memory_headroom_bytes': required - 1}})
    assert not packed._native_pack_buffer_allowed(Path('/fixed/pack'), count)
    monkeypatch.setattr(remote_build, 'observe_node',
                        lambda *_: {'limits': {'memory_headroom_bytes': required}})
    assert packed._native_pack_buffer_allowed(Path('/fixed/pack'), count)


@pytest.mark.parametrize('failure', [PermissionError(), ValueError(), KeyError(), ImportError()])
def test_unknown_capacity_keeps_existing_reader(monkeypatch, failure):
    def observe(*_):
        raise failure
    monkeypatch.setattr(remote_build, 'observe_node', observe)
    assert packed._native_pack_buffer_allowed(Path('/fixed/pack'), 8 * 1024**2) is False


@pytest.mark.parametrize('count', [1024**2, packed.MAX_NATIVE_PACK_BUFFER_BYTES + 1])
def test_pack_bound_does_not_probe_the_host(monkeypatch, count):
    monkeypatch.setattr(remote_build, 'observe_node', lambda *_: pytest.fail('out-of-bound pack probed capacity'))
    assert packed._native_pack_buffer_allowed(Path('/fixed/pack'), count) is False


def test_denied_buffer_uses_existing_verified_streaming_fallback(tmp_path, monkeypatch):
    path = tmp_path / 'original.zip'
    path.write_bytes(b'x' * (1024**2 + 1))
    item = {'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed', lambda *_: False)
    monkeypatch.setattr(packed.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=0))
    monkeypatch.setattr(native, 'hash_file', lambda selected: hashlib.sha256(selected.read_bytes()).hexdigest())
    monkeypatch.setattr(native, 'binary_reader', lambda *_a, **_k: pytest.fail('denied buffer used native RAM'))
    with packed._native_pack_source(path, item, enabled=True) as selected:
        assert selected == path


@pytest.fixture
def physically_full_pack(tmp_path, monkeypatch):
    path = tmp_path / 'original.zip'
    payload = bytes(range(256)) * 8192
    with zipfile.ZipFile(path, 'x', compression=zipfile.ZIP_STORED) as archive:
        archive.writestr('raw/original', payload)
    item = {'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'file_count': 1}
    monkeypatch.setattr(packed.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=293 * 1024**3))
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed',
                        lambda *_: pytest.fail('explicit no-memory read used a RAM buffer'))
    monkeypatch.setattr(packed.tempfile, 'TemporaryFile',
                        lambda **_: pytest.fail('physical-capacity refusal allocated C scratch'))
    def reject_scratch(selected, required):
        assert selected == Path('/var/lib') and required == item['bytes'] + 32 * 1024**3
        raise native.WorkspaceCapacityDeferred('training return workspace lacks real physical free space')
    monkeypatch.setattr(native, 'admit_workspace', reject_scratch)
    return path, item, payload


def test_large_virtual_disk_with_full_backing_uses_full_sha_then_portable_zip(physically_full_pack, monkeypatch):
    path, item, payload = physically_full_pack
    actual_hash, calls = native.hash_file, []
    def complete_hash(selected):
        result = actual_hash(selected)
        calls.append((selected, result))
        return result
    monkeypatch.setattr(native, 'hash_file', complete_hash)
    before = path.stat()
    with packed._native_pack_source(path, item, enabled=True, prefer_memory=False) as selected:
        assert selected == path and calls == [(path, item['sha256'])]
        with zipfile.ZipFile(selected) as archive:
            assert archive.namelist() == ['raw/original']
            assert archive.read('raw/original') == payload  # Includes ZIP CRC.
    after = path.stat()
    assert (before.st_ino, before.st_ctime_ns, before.st_mtime_ns, before.st_size) == (
        after.st_ino, after.st_ctime_ns, after.st_mtime_ns, after.st_size)


@pytest.mark.parametrize('failure', [SnapshotError('unverified backing'),
                                    ValueError('capacity probe failed'), PermissionError('authority denied')])
def test_unverified_admission_failure_never_selects_portable_fallback(physically_full_pack, monkeypatch, failure):
    path, item, _ = physically_full_pack
    def reject(*_):
        raise failure
    monkeypatch.setattr(native, 'admit_workspace', reject)
    monkeypatch.setattr(native, 'hash_file', lambda *_: pytest.fail('unknown backing admitted a source'))
    with pytest.raises(type(failure)) as caught:
        with packed._native_pack_source(path, item, enabled=True, prefer_memory=False):
            pytest.fail('unknown backing exposed a source')
    assert caught.value is failure


@pytest.mark.parametrize('failure', ['checksum', 'changed-before-use', 'changed-during-use', 'zip-crc'])
def test_physical_capacity_fallback_keeps_sha_generation_and_zip_checks(physically_full_pack, monkeypatch, failure):
    path, item, _ = physically_full_pack
    if failure == 'checksum':
        item = {**item, 'sha256': '0' * 64}
    if failure == 'zip-crc':
        # The whole-object ID matches these damaged ZIP bytes. The consumer
        # must still reject its member CRC; object SHA alone is insufficient.
        with path.open('r+b') as stream:
            stream.seek(100); stream.write(b'changed')
        item = {**item, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    actual_hash = native.hash_file
    def hash_then_change(selected):
        result = actual_hash(selected)
        if failure == 'changed-before-use':
            info = selected.stat()
            os.utime(selected, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
        return result
    monkeypatch.setattr(native, 'hash_file', hash_then_change)
    expected_error = zipfile.BadZipFile if failure == 'zip-crc' else SnapshotError
    with pytest.raises(expected_error):
        with packed._native_pack_source(path, item, enabled=True, prefer_memory=False) as selected:
            assert failure in {'changed-during-use', 'zip-crc'}
            with zipfile.ZipFile(selected) as archive:
                archive.read('raw/original')
            if failure == 'changed-during-use':
                info = path.stat()
                os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
