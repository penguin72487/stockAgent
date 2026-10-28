"""Ordered D-stage read-ahead must preserve the original publication contract."""
import contextlib
import hashlib
import threading
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


def _entries(source):
    entries, _ = packed._collect_entries(source)
    return [entry for entry in entries if entry.kind == 'file']


def test_prefetch_pack_has_identical_bytes_order_and_full_reconstruction(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    payloads = {'a.json': b'{"x":-1}', 'b.empty': b'', 'c.parquet': bytes(range(256)),
                'd.data': b'large-member-falls-back-to-streaming'}
    for name, value in payloads.items():
        (source / name).write_bytes(value)
    monkeypatch.setattr(packed, '_PACK_PREFETCH_MAX_MEMBER_BYTES', 16)
    sequential, prefetch = tmp_path / 'serial.zip', tmp_path / 'parallel.zip'
    serial_entries, parallel_entries = _entries(source), _entries(source)
    serial_order, parallel_order = [], []
    first = packed._write_pack(source, serial_entries, sequential, compression_level=6,
                               on_entry=lambda e: serial_order.append(e.path))
    second = packed._write_pack(source, parallel_entries, prefetch, compression_level=6,
                                on_entry=lambda e: parallel_order.append(e.path), source_workers=4)
    assert sequential.read_bytes() == prefetch.read_bytes()
    assert first == second == hashlib.sha256(prefetch.read_bytes()).hexdigest()
    assert serial_order == parallel_order
    assert [e.sha256 for e in serial_entries] == [e.sha256 for e in parallel_entries]
    with zipfile.ZipFile(prefetch) as archive:
        assert archive.testzip() is None
        assert {name: archive.read(name) for name in archive.namelist()} == payloads

    cold = tmp_path / 'cold'
    release = packed.publish_packed_snapshot(cold, 'prefetch', source, node_id='test',
                                             pack_buckets=1, pack_source_workers=4)
    proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == len(payloads)


def test_prefetch_rechecks_source_after_archive_write(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    path = source / 'value'
    path.write_bytes(b'original')
    entries = _entries(source)
    original_info = packed._zip_info

    def change_after_read(entry):
        path.write_bytes(b'changed after prefetch')
        return original_info(entry)

    monkeypatch.setattr(packed, '_zip_info', change_after_read)
    with pytest.raises(SnapshotError, match='source file changed while packing'):
        packed._write_pack(source, entries, tmp_path / 'interrupted.zip', compression_level=6,
                           source_workers=4)
    assert path.read_bytes() == b'changed after prefetch'


def test_prefetch_publisher_recovers_when_zip_digest_needs_full_rehash(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'stored.parquet').write_bytes(bytes(range(256)) * 8)
    native = packed._SeekablePackBuffer
    monkeypatch.setattr(packed, '_SeekablePackBuffer', lambda stream: native(stream, capacity=32))
    cold = tmp_path / 'cold'
    release = packed.publish_packed_snapshot(cold, 'prefetch-fallback', source, node_id='test',
                                             pack_buckets=1, pack_source_workers=4)
    proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 1


def test_prefetch_really_overlaps_source_reads(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    for number in range(4):
        (source / str(number)).write_bytes(str(number).encode())
    original = packed._open_source_entry
    barrier = threading.Barrier(4, timeout=5)

    @contextlib.contextmanager
    def concurrent_open(root, entry):
        with original(root, entry) as stream:
            barrier.wait()
            yield stream

    monkeypatch.setattr(packed, '_open_source_entry', concurrent_open)
    packed._write_pack(source, _entries(source), tmp_path / 'parallel.zip', compression_level=6,
                       source_workers=4)


@pytest.mark.parametrize('workers', [0, 17, True, 1.5])
def test_prefetch_rejects_invalid_worker_count_before_creating_output(tmp_path, workers):
    target = tmp_path / 'must-not-exist.zip'
    with pytest.raises(ValueError, match='pack source workers'):
        packed._write_pack(tmp_path, [], target, compression_level=6, source_workers=workers)
    assert not target.exists()
