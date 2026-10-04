from __future__ import annotations

from pathlib import Path

import pytest

import stockagent.data_sync.cold_primary as cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import _archive_d_primary_head


def test_d_primary_archives_exact_head_bytes_before_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "packed"
    head = root / "heads/example/penguin.json"
    head.parent.mkdir(parents=True)
    head.write_bytes(b'{"old":"head"}\n')
    (root / ".stockagent-d-primary").write_text("{}")
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda path: None)
    relative = _archive_d_primary_head(root, head)
    assert relative is not None
    archived = root / relative
    assert archived.read_bytes() == head.read_bytes()
    assert _archive_d_primary_head(root, head) == relative
    archived.write_bytes(b"corrupt history")
    with pytest.raises(SnapshotError, match="history checksum collision"):
        _archive_d_primary_head(root, head)
