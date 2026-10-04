from pathlib import Path

from stockagent.data_sync.materialized_cache import process_references, process_references_many
from stockagent.data_sync.artifact_maintenance import artifact_process_references_many


def _proc_fixture(tmp_path, monkeypatch):
    process = tmp_path / "proc/123"
    (process / "fd").mkdir(parents=True)
    (process / "maps").write_text("")
    (process / "cmdline").write_bytes(b"python\x00")
    original = Path.glob
    def glob(path, pattern):
        if path == Path("/proc") and pattern == "[0-9]*":
            return iter((process,))
        return original(path, pattern)
    monkeypatch.setattr(Path, "glob", glob)
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
