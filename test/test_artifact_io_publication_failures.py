"""Publication and failure semantics shared by downloader artifact writers."""
import os
from pathlib import Path
import stat

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import artifact_io


@pytest.mark.parametrize("mode", [None, 0o600])
@pytest.mark.parametrize("durable", [False, True])
def test_nested_bytes_publication_preserves_permissions_and_storage_barriers(tmp_path, monkeypatch, mode, durable):
    target = tmp_path / "new" / "nested" / "raw.bin"
    replace, fsync = os.replace, os.fsync
    events = []

    def inspect(source, destination):
        source = Path(source)
        assert source.parent == target.parent and source.read_bytes() == b"complete\x00payload"
        if mode is not None:
            assert source.stat().st_mode & 0o777 == mode
        events.append("replace")
        replace(source, destination)

    def synchronize(fd):
        events.append("directory_sync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file_sync")
        fsync(fd)

    monkeypatch.setattr(artifact_io.os, "replace", inspect)
    monkeypatch.setattr(artifact_io.os, "fsync", synchronize)
    artifact_io.atomic_write_bytes(target, b"complete\x00payload", mode=mode, durable=durable)
    assert target.read_bytes() == b"complete\x00payload"
    assert events == (["file_sync", "replace", "directory_sync"] if durable else ["replace"])
    assert not list(target.parent.glob(".*.tmp"))


@pytest.mark.parametrize("mode", [None, 0o600])
@pytest.mark.parametrize("durable", [False, True])
def test_bytes_rename_failure_keeps_old_destination_and_cleans_owned_sibling(tmp_path, monkeypatch, mode, durable):
    target = tmp_path / "raw.bin"
    target.write_bytes(b"previous")

    def fail(source, destination):
        assert Path(source).read_bytes() == b"replacement"
        raise OSError("injected rename failure")

    monkeypatch.setattr(artifact_io.os, "replace", fail)
    with pytest.raises(OSError, match="injected rename failure"):
        artifact_io.atomic_write_bytes(target, b"replacement", mode=mode, durable=durable)
    assert target.read_bytes() == b"previous"
    assert not artifact_io._temporary_path(target).exists()


@pytest.mark.parametrize("mode", [None, 0o600])
def test_bytes_file_sync_failure_prevents_publication(tmp_path, monkeypatch, mode):
    target = tmp_path / "raw.bin"
    target.write_bytes(b"previous")

    def fail(fd):
        raise OSError("injected file sync failure")

    monkeypatch.setattr(artifact_io.os, "fsync", fail)
    with pytest.raises(OSError, match="injected file sync failure"):
        artifact_io.atomic_write_bytes(target, b"replacement", mode=mode, durable=True)
    assert target.read_bytes() == b"previous"
    assert not artifact_io._temporary_path(target).exists()


@pytest.mark.parametrize("mode", [None, 0o600])
def test_partial_bytes_write_cannot_publish_and_cleans_owned_sibling(tmp_path, monkeypatch, mode):
    target = tmp_path / "artifact"
    target.write_bytes(b"previous")
    path_open, fdopen = Path.open, os.fdopen

    class PartialWrite:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def write(self, body):
            self.stream.write(body[:3])
            raise OSError("injected partial write")

    def wrapped_open(path, *args, **kwargs):
        stream = path_open(path, *args, **kwargs)
        return PartialWrite(stream) if path == artifact_io._temporary_path(target) else stream

    monkeypatch.setattr(Path, "open", wrapped_open)
    monkeypatch.setattr(artifact_io.os, "fdopen", lambda *a, **kw: PartialWrite(fdopen(*a, **kw)))
    with pytest.raises(OSError, match="injected partial write"):
        artifact_io.atomic_write_bytes(target, b"replacement", mode=mode)
    assert target.read_bytes() == b"previous"
    assert not artifact_io._temporary_path(target).exists()


def test_private_sibling_symlink_cannot_redirect_bytes_or_be_removed(tmp_path):
    target, victim = tmp_path / "private", tmp_path / "victim"
    target.write_bytes(b"previous")
    victim.write_bytes(b"preserve")
    temporary = artifact_io._temporary_path(target)
    temporary.symlink_to(victim)
    with pytest.raises(FileExistsError):
        artifact_io.atomic_write_bytes(target, b"replacement", mode=0o600)
    assert target.read_bytes() == b"previous" and victim.read_bytes() == b"preserve"
    assert temporary.is_symlink()


@pytest.mark.parametrize("kind", ["bytes", "parquet"])
def test_parent_sync_failure_is_reported_after_complete_publication(tmp_path, monkeypatch, kind):
    target = tmp_path / "artifact"
    target.write_bytes(b"previous")

    def fail(path):
        assert Path(path) == target
        raise OSError("injected parent sync failure")

    monkeypatch.setattr(artifact_io, "_sync_parent", fail)
    with pytest.raises(OSError, match="injected parent sync failure"):
        if kind == "bytes":
            artifact_io.atomic_write_bytes(target, b"complete", durable=True)
        else:
            artifact_io.atomic_write_parquet(target, pa.table({"value": [1.0, None, -0.0]}), durable=True)
    if kind == "bytes":
        assert target.read_bytes() == b"complete"
    else:
        assert pq.read_table(target).equals(pa.table({"value": [1.0, None, -0.0]}))
    assert not artifact_io._temporary_path(target).exists()


@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize("failure", ["writer", "callback", "rename"])
def test_parquet_failure_keeps_original_and_cleans_partial_sibling(tmp_path, monkeypatch, durable, failure):
    target = tmp_path / "artifact.parquet"
    target.write_bytes(b"previous")
    callback = None

    def reject(*args, **kwargs):
        raise OSError("injected " + failure + " failure")

    if failure == "writer":
        def partial_writer(table, path, **kwargs):
            Path(path).write_bytes(b"incomplete parquet")
            reject()
        monkeypatch.setattr(pq, "write_table", partial_writer)
    elif failure == "callback":
        callback = reject
    else:
        monkeypatch.setattr(artifact_io.os, "replace", reject)
    with pytest.raises(OSError, match="injected " + failure + " failure"):
        artifact_io.atomic_write_parquet(target, pa.table({"value": [1, None, 3]}),
                                        durable=durable, before_replace=callback)
    assert target.read_bytes() == b"previous"
    assert not artifact_io._temporary_path(target).exists()


@pytest.mark.parametrize("mode", [None, 0o600])
def test_next_publication_recreates_removed_parent_without_stale_directory_cache(tmp_path, mode):
    target = tmp_path / "mutable" / "artifact"
    artifact_io.atomic_write_bytes(target, b"first", mode=mode)
    target.unlink()
    target.parent.rmdir()
    artifact_io.atomic_write_bytes(target, b"second", mode=mode)
    assert target.read_bytes() == b"second"


@pytest.mark.parametrize("mode", [None, 0o600])
def test_parent_file_remains_intact_when_publication_cannot_create_directory(tmp_path, mode):
    parent = tmp_path / "regular-file"
    parent.write_bytes(b"preserve")
    with pytest.raises(OSError):
        artifact_io.atomic_write_bytes(parent / "artifact", b"cannot publish", mode=mode)
    assert parent.read_bytes() == b"preserve"
