import hashlib
from pathlib import Path

import pytest

from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_non_d_read_and_hash_use_exact_regular_bytes(tmp_path):
    path = tmp_path / 'bytes.bin'
    value = bytes(range(256)) * 4097
    path.write_bytes(value)
    assert native.windows_path(path) is None
    assert native.hash_file(path) == hashlib.sha256(value).hexdigest()


def test_d_mapping_cannot_silently_normalize_parent_traversal(monkeypatch):
    monkeypatch.setattr(Path, 'resolve', lambda self: self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: False)
    assert native.windows_path(Path('/mnt/d/stockagent-cold-primary/packed/objects/a.blob')) == (
        'D:\\stockagent-cold-primary\\packed\\objects\\a.blob')
    with pytest.raises(SnapshotError, match='Unsafe'):
        native.windows_path(Path('/mnt/d/stockagent-cold-primary/packed/../outside.bin'))


def test_d_redirect_rejected_before_native_process_launch(monkeypatch):
    monkeypatch.setattr(Path, 'resolve', lambda self: Path('/redirected'))
    with pytest.raises(SnapshotError, match='Redirected'):
        native.windows_path(Path('/srv/stockagent-packed/objects/a.blob'))


def test_partial_append_needs_exact_sha_before_native_process():
    with pytest.raises(SnapshotError, match='prefix'):
        native.BinaryWriter(Path('/mnt/d/stockagent-cold-primary/incoming/a'), offset=1)


def test_native_io_uses_binary_filestream_not_9p_copy():
    code = native.SCRIPT.read_text()
    assert 'OpenStandardInput' in code and 'OpenStandardOutput' in code
    assert '[IO.FileMode]::CreateNew' in code and '$stream.Flush($true)' in code
    assert 'Retained prefix SHA differs' in code and 'AvailableFreeSpace' in code
    assert 'Copy-Item' not in code and 'Set-Content' not in code
    # Prefixes exceed 2 GiB: avoid PowerShell choosing Math.Min(Int32,Int32).
    assert '[Math]::Min([long]$buffer.Length, [long]($offset - $total))' in code
