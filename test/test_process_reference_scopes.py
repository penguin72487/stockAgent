import errno
import os
from pathlib import Path

import pytest

from stockagent.data_sync import materialized_cache
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references, process_references_many
from stockagent.data_sync.artifact_maintenance import artifact_process_references_many


def _proc_fixture(tmp_path, monkeypatch):
    process = tmp_path / "proc/123"
    (process / "fd").mkdir(parents=True)
    (process / "maps").write_text("")
    (process / "cmdline").write_bytes(b"python\x00")
    original = Path.glob
    original_iterdir = Path.iterdir
    def glob(path, pattern):
        if path == Path("/proc") and pattern == "[0-9]*":
            return iter((process,))
        return original(path, pattern)
    monkeypatch.setattr(Path, "glob", glob)
    def iterdir(path):
        if path == Path("/proc"):
            return iter((process,))
        return original_iterdir(path)
    monkeypatch.setattr(Path, "iterdir", iterdir)
    old = tmp_path / "artifacts/old-v1"
    new = tmp_path / "artifacts/new-v120"
    old.mkdir(parents=True); new.mkdir()
    (old / "source").write_bytes(b"old")
    (new / "build.log").write_bytes(b"new")
    (process / "fd/1").symlink_to(new / "build.log")
    return process, old, new


def test_selected_roots_ignore_sibling_writer_but_detect_selected_fd(tmp_path, monkeypatch):
    process, old, new = _proc_fixture(tmp_path, monkeypatch)
    assert process_references_many([old]) == []
    assert process_references(old) == []
    assert process_references(new)
    (process / "fd/2").symlink_to(old / "source")
    assert any("fd=2" in row for row in process_references_many([old]))


def test_selected_maps_and_ancestor_orchestrator_remain_protected(tmp_path, monkeypatch):
    process, old, new = _proc_fixture(tmp_path, monkeypatch)
    (process / "maps").write_text(f"0-1 r--p 0 0 0 {old}/source\n")
    assert any(":maps:" in row for row in process_references_many([old]))
    (process / "maps").write_text("")
    (process / "cmdline").write_bytes(b"python\x00" + str(old.parent).encode() + b"\x00")
    assert any(":cmdline:" in row for row in artifact_process_references_many([old], old.parent))
    (process / "cmdline").write_bytes(b"python\x00" + str(new).encode() + b"\x00")
    assert artifact_process_references_many([old], old.parent) == []


@pytest.mark.parametrize("name", ["cwd", "root", "exe", "fd/1"])
@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EPERM, errno.EIO])
def test_unreadable_reference_link_is_not_a_clean_empty_scan(tmp_path, monkeypatch, name, error_number):
    process, old, _ = _proc_fixture(tmp_path, monkeypatch)
    original = os.readlink

    def readlink(path, *args, **kwargs):
        if Path(path) == process / name:
            raise OSError(error_number, "injected reference visibility error")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(materialized_cache.os, "readlink", readlink)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        process_references_many([old])


@pytest.mark.parametrize("component", ["processes", "fd", "maps", "inode"])
def test_unreadable_reference_component_is_not_a_clean_empty_scan(tmp_path, monkeypatch, component):
    process, old, _ = _proc_fixture(tmp_path, monkeypatch)
    original_iterdir, original_text, original_stat = Path.iterdir, Path.read_text, Path.stat

    def iterdir(path):
        if (component == "processes" and path == Path("/proc")) or (component == "fd" and path == process / "fd"):
            raise PermissionError(errno.EACCES, "injected reference visibility error")
        return original_iterdir(path)

    def read_text(path, *args, **kwargs):
        if component == "maps" and path == process / "maps":
            raise PermissionError(errno.EACCES, "injected reference visibility error")
        return original_text(path, *args, **kwargs)

    def metadata(path, *args, **kwargs):
        if component == "inode" and path == process / "fd/1":
            raise PermissionError(errno.EACCES, "injected reference visibility error")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "iterdir", iterdir)
    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "stat", metadata)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        process_references_many([old], inode_keys={(1, 2)})


def test_unreadable_orchestrator_command_is_not_a_clean_empty_scan(tmp_path, monkeypatch):
    process, old, _ = _proc_fixture(tmp_path, monkeypatch)
    original = Path.read_bytes

    def read_bytes(path):
        if path == process / "cmdline":
            raise PermissionError(errno.EACCES, "injected orchestrator visibility error")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        artifact_process_references_many([old], old.parent)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_reference_limit_must_be_a_positive_integer(tmp_path, monkeypatch, limit):
    _, old, _ = _proc_fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="positive integer"):
        process_references_many([old], limit=limit)


@pytest.mark.parametrize("error_number", [errno.ENOENT, errno.ESRCH])
@pytest.mark.parametrize("component", ["link", "fd", "maps", "inode", "command"])
def test_exited_process_or_closed_fd_is_not_a_visibility_failure(tmp_path, monkeypatch, component, error_number):
    process, old, _ = _proc_fixture(tmp_path, monkeypatch)
    original_link, original_iterdir = os.readlink, Path.iterdir
    original_text, original_stat, original_bytes = Path.read_text, Path.stat, Path.read_bytes

    def fail():
        raise OSError(error_number, "injected ordinary process/descriptor exit")

    def readlink(path, *args, **kwargs):
        if component == "link" and Path(path) == process / "exe":
            fail()
        return original_link(path, *args, **kwargs)

    def iterdir(path):
        if component == "fd" and path == process / "fd":
            fail()
        return original_iterdir(path)

    def read_text(path, *args, **kwargs):
        if component == "maps" and path == process / "maps":
            fail()
        return original_text(path, *args, **kwargs)

    def metadata(path, *args, **kwargs):
        if component == "inode" and path == process / "fd/1":
            fail()
        return original_stat(path, *args, **kwargs)

    def read_bytes(path):
        if component == "command" and path == process / "cmdline":
            fail()
        return original_bytes(path)

    monkeypatch.setattr(os, "readlink", readlink)
    monkeypatch.setattr(Path, "iterdir", iterdir)
    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "stat", metadata)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    assert process_references_many([old], inode_keys={(1, 2)}) == []
    assert artifact_process_references_many([old], old.parent) == []


@pytest.mark.parametrize("error_number", [errno.ENOENT, errno.ESRCH])
def test_missing_proc_namespace_is_not_an_exited_individual_process(tmp_path, monkeypatch, error_number):
    _, old, _ = _proc_fixture(tmp_path, monkeypatch)
    original = Path.iterdir

    def iterdir(path):
        if path == Path("/proc"):
            raise OSError(error_number, "injected missing proc namespace")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", iterdir)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan at /proc"):
        process_references_many([old])


def test_hardlink_inode_and_deleted_mapping_remain_references(tmp_path, monkeypatch):
    process, old, new = _proc_fixture(tmp_path, monkeypatch)
    alias = new / "alias"
    os.link(old / "source", alias)
    (process / "fd/2").symlink_to(alias)
    info = (old / "source").stat()
    key = info.st_dev, info.st_ino
    assert any("fd=2" in row for row in process_references_many([old], inode_keys={key}))
    (process / "fd/2").unlink()
    device = f"{os.major(key[0]):02x}:{os.minor(key[0]):02x}"
    (process / "maps").write_text(f"0-1 r--p 0 {device} {key[1]} {alias} (deleted)\n")
    assert any(":maps-inode:" in row for row in process_references_many([old], inode_keys={key}))


def test_unreadable_orchestrator_cwd_is_not_a_nonpath_argument(tmp_path, monkeypatch):
    process, old, _ = _proc_fixture(tmp_path, monkeypatch)
    (process / "cmdline").write_bytes(b"python\x00relative-suite\x00")
    original = Path.resolve

    def resolve(path, *args, **kwargs):
        if path == process / "cwd":
            raise PermissionError(errno.EACCES, "injected orchestrator cwd error")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        artifact_process_references_many([old], old.parent)
