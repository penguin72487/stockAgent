"""Complete source hashes remain authoritative across native batching/failures."""
from contextlib import contextmanager
import hashlib
import io
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


class PreviousFiles:
    def __init__(self, entries, *, present=True):
        self.rows = {entry.path: {"kind": "file", "size": entry.size,
            "sha256": "0" * 64, "storage": {"kind": "blob"}}
            for entry in entries if present}
        self.consumed = []

    def take_file_hint(self, path):
        self.consumed.append(path)
        return self.rows.pop(path, None)


def files(tmp_path, count=5):
    root = tmp_path / "source"
    root.mkdir()
    for index in range(count):
        (root / f"{index:02}.bin").write_bytes(bytes([index]) * (3 + index))
    entries, _ = packed._collect_entries(root)
    return root, [entry for entry in entries if entry.kind == "file"]


def mock_native(monkeypatch, root, *, finish=None, suffix=b"", truncated=False):
    calls, descriptors = [], []
    original_open = os.open

    def opened(path, flags, *args, **kwargs):
        descriptor = original_open(path, flags, *args, **kwargs)
        if isinstance(path, (str, Path)) and Path(path).parent == root:
            assert flags & os.O_NOFOLLOW and flags & os.O_NONBLOCK
            descriptors.append(descriptor)
        return descriptor

    class ShortReads(io.BytesIO):
        def read(self, count=-1):
            return super().read(min(count, 3) if count > 0 else count)

    @contextmanager
    def reader(members):
        live = []
        for descriptor in descriptors:
            try:
                live.append(os.fstat(descriptor))
            except OSError:
                pass
        assert all(any((info.st_dev, info.st_ino) == (path.stat().st_dev, path.stat().st_ino)
                       for info in live) for path, _ in members)
        calls.append([(Path(path), size) for path, size in members])
        payload = b"".join(path.read_bytes() for path, _ in members)
        with ShortReads((payload[:-1] if truncated else payload) + suffix) as stream:
            yield stream
            if finish:
                finish(members)

    monkeypatch.setattr(os, "open", opened)
    monkeypatch.setattr(native, "windows_path", lambda path: "D:\\reviewed\\" + Path(path).name)
    monkeypatch.setattr(native, "binary_reader_many", reader)
    monkeypatch.setattr(native, "hash_file", lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest())
    return calls, descriptors


def routes(root, entries, previous, **options):
    return list(packed._publication_file_routes(root, entries, previous,
        previous=None, file_chunk_bytes=None, file_chunking="fixed-v1",
        loose_file_threshold_bytes=1024, native_d_io=options.pop("native_d_io", True), **options))


def test_native_batch_hashes_every_complete_original_and_closes_held_fds(tmp_path, monkeypatch):
    root, entries = files(tmp_path)
    previous = PreviousFiles(entries)
    calls, descriptors = mock_native(monkeypatch, root)
    result = routes(root, entries, previous)
    assert previous.consumed == [entry.path for entry in entries] and not previous.rows
    assert len(calls) == 1 and len(calls[0]) == len(entries)
    assert all(row[4] and not row[2] and not row[3] for row in result)
    assert [entry.sha256 for entry in entries] == [hashlib.sha256((root / entry.path).read_bytes()).hexdigest()
                                                 for entry in entries]
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("failure", ["truncated", "extra", "exit", "replace", "append", "mode", "symlink"])
def test_failed_native_batch_accepts_no_partial_sha_or_reuse_proof(tmp_path, monkeypatch, failure):
    root, entries = files(tmp_path, 2)
    def finish(members):
        path = members[0][0]
        if failure == "exit":
            raise SnapshotError("native child rejected its final exit")
        if failure in {"replace", "symlink"}:
            substitute = tmp_path / "replacement"
            substitute.write_bytes(path.read_bytes())
            if failure == "replace":
                os.replace(substitute, path)
            else:
                path.unlink()
                path.symlink_to(substitute)
        elif failure == "append":
            with path.open("ab") as stream:
                stream.write(b"new")
        elif failure == "mode":
            path.chmod(0o600 if path.stat().st_mode & 0o777 != 0o600 else 0o640)
    _, descriptors = mock_native(monkeypatch, root, finish=finish,
        truncated=failure == "truncated", suffix=b"excess" if failure == "extra" else b"")
    with pytest.raises(SnapshotError):
        packed._hash_source_batch(root, entries)
    assert all(entry.sha256 is None for entry in entries)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("kind", ["fifo", "symlink", "changed_size"])
def test_source_replaced_before_native_open_never_reaches_child(tmp_path, monkeypatch, kind):
    root, entries = files(tmp_path, 2)
    path = root / entries[-1].path
    path.unlink()
    if kind == "fifo":
        os.mkfifo(path)
    elif kind == "symlink":
        path.symlink_to(root / entries[0].path)
    else:
        path.write_bytes(b"different size")
    calls, descriptors = mock_native(monkeypatch, root)
    with pytest.raises(SnapshotError):
        packed._hash_source_batch(root, entries)
    assert not calls and all(entry.sha256 is None for entry in entries)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("policy", ["portable", "not_enrolled", "new_small", "atomic", "oversized", "one_pass"])
def test_unselected_source_routes_keep_the_original_reader_and_one_pass_rules(tmp_path, monkeypatch, policy):
    root, entries = files(tmp_path, 2)
    previous = PreviousFiles(entries, present=policy != "new_small")
    mock_native(monkeypatch, root)
    if policy == "not_enrolled":
        monkeypatch.setattr(native, "windows_path", lambda path: None)
    elif policy == "atomic":
        for entry in entries:
            entry.atomic_json_control = True
    elif policy == "oversized":
        monkeypatch.setattr(packed, "MAX_NATIVE_HASH_MEMBER_BYTES", 1)
    elif policy == "one_pass":
        for row in previous.rows.values():
            row["storage"] = {"kind": "chunks"}
    monkeypatch.setattr(packed, "_hash_source_batch", lambda *args: pytest.fail("unselected native batch"))
    if policy == "one_pass":
        result = list(packed._publication_file_routes(root, entries, previous,
            previous=SimpleNamespace(manifest={"archive": {"file_chunk_bytes": 1, "file_chunking": "fixed-v1"}}),
            file_chunk_bytes=1, file_chunking="fixed-v1", loose_file_threshold_bytes=1, native_d_io=True))
        assert all(row[3] for row in result)
    else:
        result = routes(root, entries, previous, native_d_io=policy != "portable")
    assert all(not row[4] and row[0].sha256 is None for row in result)
    assert previous.consumed == [entry.path for entry in entries]


@pytest.mark.parametrize("bound", ["members", "bytes"])
def test_native_source_batch_respects_both_bounds_and_keeps_route_order(tmp_path, monkeypatch, bound):
    root, entries = files(tmp_path, 5)
    calls, _ = mock_native(monkeypatch, root)
    if bound == "members":
        monkeypatch.setattr(packed, "MAX_NATIVE_SOURCE_HASH_FILES", 2)
    else:
        monkeypatch.setattr(packed, "MAX_NATIVE_HASH_BATCH_BYTES", 9)
    result = routes(root, entries, PreviousFiles(entries))
    assert [row[0].path for row in result] == [entry.path for entry in entries]
    assert all(row[4] for row in result)
    if bound == "members":
        assert all(len(batch) <= 2 for batch in calls)
    else:
        assert all(sum(size for _, size in batch) <= 9 for batch in calls)


def test_unselected_middle_source_flushes_prior_batch_before_later_entries(tmp_path, monkeypatch):
    root, entries = files(tmp_path)
    calls, _ = mock_native(monkeypatch, root)
    monkeypatch.setattr(native, "windows_path", lambda path: None if Path(path).name == "02.bin" else "D:\\enrolled")
    result = routes(root, entries, PreviousFiles(entries))
    assert [[path.name for path, _ in batch] for batch in calls] == [["00.bin", "01.bin"], ["03.bin", "04.bin"]]
    assert [row[4] for row in result] == [True, True, False, True, True]
    assert [row[0].path for row in result] == [entry.path for entry in entries]


def test_complete_publisher_refresh_and_recovery_preserve_inventory_after_native_hash_batching(tmp_path, monkeypatch):
    root, _ = files(tmp_path, 20)
    cold = tmp_path / "cold"
    first = packed.publish_packed_snapshot(cold, "source-batch", root, node_id="test-node")
    calls, _ = mock_native(monkeypatch, root)
    original_routes = packed._publication_file_routes
    def selected(*args, **kwargs):
        return original_routes(*args, **{**kwargs, "native_d_io": True})
    monkeypatch.setattr(packed, "_publication_file_routes", selected)
    same = packed.publish_packed_snapshot(cold, "source-batch", root, node_id="test-node")
    assert same.manifest_sha256 == first.manifest_sha256
    assert [len(batch) for batch in calls] == [16, 4]
    (root / "00.bin").write_bytes(b"new")
    changed = packed.publish_packed_snapshot(cold, "source-batch", root, node_id="test-node")
    assert changed.manifest_sha256 != first.manifest_sha256
    proof = packed.verify_packed_snapshot(cold, changed, reconstruct_all=True)
    assert proof["independently_reconstructed_files"] == 20
    restored = packed.fetch_packed_snapshot(cold, tmp_path / "restored", changed)
    assert {path.name: path.read_bytes() for path in restored.iterdir() if path.suffix == ".bin"} == {
        path.name: path.read_bytes() for path in root.iterdir()}
    packed.verify_packed_snapshot(cold, first, reconstruct_all=True)
