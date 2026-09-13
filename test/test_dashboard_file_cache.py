"""Read-only invalidation must be cheap without hiding same-size replacements."""
import os
from pathlib import Path

from stockagent.live import dashboard_updates as updates
from stockagent.live import tw_day_trade_dashboard as dashboard


def test_unchanged_signature_and_json_do_not_read_again(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    path.write_text('{"revision":1}')
    signature = updates.file_signature(path)
    assert dashboard._object(path) == {"revision": 1}
    def unexpected_read(*args, **kwargs):
        raise AssertionError("unchanged hot path read source bytes")
    monkeypatch.setattr(Path, "open", unexpected_read)
    assert updates.file_signature(path) == signature
    assert dashboard._object(path) == {"revision": 1}


def test_restored_mtime_same_size_write_invalidates_both_caches(tmp_path):
    path = tmp_path / "status.json"
    path.write_text('{"revision":1}')
    stat = path.stat()
    before = updates.file_signature(path)
    assert dashboard._object(path)["revision"] == 1
    path.write_text('{"revision":2}')
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert path.stat().st_size == stat.st_size
    assert path.stat().st_mtime_ns == stat.st_mtime_ns
    assert updates.file_signature(path) != before
    assert dashboard._object(path)["revision"] == 2


def test_atomic_replacement_and_deleted_file_invalidate(tmp_path):
    path = tmp_path / "status.json"
    path.write_text('{"revision":1}')
    before = updates.file_signature(path)
    dashboard._object(path)
    staged = tmp_path / "staged.json"
    staged.write_text('{"revision":2}')
    staged.replace(path)
    assert updates.file_signature(path) != before
    assert dashboard._object(path)["revision"] == 2
    path.unlink()
    assert updates.file_signature(path) is None


def test_signature_cache_is_bounded_and_directory_is_not_read(tmp_path, monkeypatch):
    monkeypatch.setattr(updates, "_MAX_SIGNATURES", 2)
    for n in range(4):
        path = tmp_path / str(n)
        path.write_text(str(n))
        updates.file_signature(path)
    assert len(updates._SIGNATURE_CACHE) <= 2
    assert updates.file_signature(tmp_path)[-1] == b""


def test_replacement_during_json_read_does_not_cache_wrong_revision(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    path.write_text('{"revision":1}')
    staged = tmp_path / "staged.json"
    staged.write_text('{"revision":2}')
    original_open = Path.open
    def swapped_open(target, *args, **kwargs):
        stream = original_open(target, *args, **kwargs)
        if target == path and staged.exists():
            staged.replace(path)
        return stream
    monkeypatch.setattr(Path, "open", swapped_open)
    # A coherent already-open old snapshot is valid, but may not be cached as new.
    assert dashboard._object(path) == {"revision": 1}
    assert dashboard._object(path) == {"revision": 2}
