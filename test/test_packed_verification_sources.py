import contextlib
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as windows
from stockagent.data_sync.packed_verification_sources import PackSources


@pytest.fixture(params=[1024**2 + 1, 9*1024**2 + 1])
def packs(tmp_path, monkeypatch, request):
    items = []
    for number in range(3):
        value = bytes([number]) * request.param
        path = tmp_path / str(number)
        path.write_bytes(value)
        items.append({'sha256': hashlib.sha256(value).hexdigest(), 'bytes': len(value),
                      'file_count': 1, 'relpath': path.name})
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed', lambda *_: True)
    return tmp_path, items


def test_native_startup_is_shared_and_no_buffer_is_exposed_before_exit_fences(packs, monkeypatch):
    root, items = packs
    observed = []
    @contextlib.contextmanager
    def reader(members):
        observed.append(len(members))
        yield io.BytesIO(b''.join(path.read_bytes() for path, _ in members))
        observed.append('child-exit-verified')
    monkeypatch.setattr(windows, 'binary_reader_many', reader)
    sources = PackSources(root, items, native=True)
    try:
        for item in items:
            with sources.open(item) as buffer:
                assert observed == [3, 'child-exit-verified']
                assert hashlib.sha256(buffer.read()).hexdigest() == item['sha256']
        assert sources.summary()['native_read_batches'] == 1
        assert not sources.buffers
    finally:
        sources.close()


@pytest.mark.parametrize('fault', ['child-exit', 'content'])
def test_failed_native_batch_never_exposes_any_prefetched_pack(packs, monkeypatch, fault):
    root, items = packs
    @contextlib.contextmanager
    def reader(members):
        body = b''.join(path.read_bytes() for path, _ in members)
        if fault == 'content':
            body = b'x' + body[1:]
        yield io.BytesIO(body)
        if fault == 'child-exit':
            raise packed.SnapshotError('injected native child exit failure')
    monkeypatch.setattr(windows, 'binary_reader_many', reader)
    sources = PackSources(root, items, native=True)
    try:
        with pytest.raises(packed.SnapshotError):
            with sources.open(items[0]):
                pytest.fail('must not expose a partly accepted native batch')
        assert not sources.buffers
        assert sources.summary()['native_read_batches'] == 0
        assert all((root/item['relpath']).exists() for item in items)
    finally:
        sources.close()


def test_generation_change_while_waiting_in_prefetch_is_rejected(packs, monkeypatch):
    root, items = packs
    @contextlib.contextmanager
    def reader(members):
        yield io.BytesIO(b''.join(path.read_bytes() for path, _ in members))
    monkeypatch.setattr(windows, 'binary_reader_many', reader)
    sources = PackSources(root, items, native=True)
    try:
        with sources.open(items[0]):
            pass
        (root/items[1]['relpath']).touch()
        with pytest.raises(packed.SnapshotError, match='before delegation'):
            with sources.open(items[1]):
                pytest.fail('stale generation must not be delegated')
    finally:
        sources.close()


def test_missing_ram_keeps_original_native_reader_without_batching(packs, monkeypatch):
    root, items = packs
    calls = []
    @contextlib.contextmanager
    def reader(path, item, **options):
        calls.append(path)
        yield io.BytesIO(path.read_bytes())
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed', lambda *_: False)
    monkeypatch.setattr(packed, '_native_pack_source', reader)
    monkeypatch.setattr(windows, 'binary_reader_many', lambda *_: pytest.fail('RAM admission refused'))
    sources = PackSources(root, items, native=True)
    try:
        for item in items:
            with sources.open(item) as stream:
                assert stream.read() == (root/item['relpath']).read_bytes()
        assert len(calls) == len(items)
        assert sources.summary()['native_read_batches'] == 0
    finally:
        sources.close()


def test_large_pack_batch_keeps_member_byte_and_single_object_bounds(tmp_path, monkeypatch):
    items = [{'sha256': str(i), 'bytes': 60*1024**2, 'file_count': 1, 'relpath': str(i)}
             for i in range(5)]
    admitted, fills = [], []
    monkeypatch.setattr(packed, '_native_pack_buffer_allowed',
        lambda path, size: admitted.append(size) or True)
    monkeypatch.setattr(PackSources, '_signature', lambda self, item: (item['sha256'], item['bytes']))
    def fill(self, batch):
        fills.append(batch)
        for item in batch:
            self.pending.pop(item['sha256'], None)
            self.buffers[item['sha256']] = io.BytesIO(), self._signature(item)
    monkeypatch.setattr(PackSources, '_fill', fill)
    sources = PackSources(tmp_path, items, native=True)
    try:
        with sources.open(items[0]):
            assert len(fills) == 1 and len(fills[0]) == 2
            assert admitted == [120*1024**2]
        assert sources.summary()['maximum_batch_bytes'] == 128*1024**2
        assert not sources._candidate({**items[0], 'bytes': 64*1024**2 + 1})
        assert sources._candidate({**items[0], 'bytes': 64*1024**2})
    finally:
        sources.close()


def test_anonymous_scratch_exposes_only_a_verified_pack_and_detects_changes(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume
    source = tmp_path / 'pack.zip'
    with zipfile.ZipFile(source, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('one.json', b'one')
        archive.writestr('two.bin', b'two' * 100)
    item = {'bytes': source.stat().st_size, 'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
    scratch = tmp_path / 'e-verify'
    scratch.mkdir()
    enrollment = tmp_path / 'enrolled'
    enrollment.touch()
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', enrollment)
    monkeypatch.setattr(workspace_volume, 'path', lambda *_a, **_k: scratch)
    monkeypatch.setattr(packed.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=2**50))
    monkeypatch.setattr(windows, 'admit_workspace', lambda *_: None)
    monkeypatch.setattr(windows, 'binary_reader', lambda path: path.open('rb'))

    with packed._native_pack_source(source, item, enabled=True, prefer_memory=False) as decoded:
        with zipfile.ZipFile(decoded) as archive:
            assert archive.read('one.json') == b'one'
            assert archive.read('two.bin') == b'two' * 100
    assert list(scratch.iterdir()) == []

    with pytest.raises(packed.SnapshotError, match='checksum'):
        with packed._native_pack_source(source, {**item, 'sha256': '0' * 64},
                                        enabled=True, prefer_memory=False):
            pytest.fail('an unverified pack must never be exposed')
    with pytest.raises(packed.SnapshotError, match='changed during reconstruction'):
        with packed._native_pack_source(source, item, enabled=True, prefer_memory=False):
            with source.open('ab') as stream:
                stream.write(b'changed')
