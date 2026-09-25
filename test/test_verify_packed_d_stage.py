from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3

import pytest

from scripts.verify_packed_d_stage import verify_one
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_verifier_hashes_once_and_rechecks_changed_inode(tmp_path: Path) -> None:
    data = b"cold object"
    relative = "objects/blobs/ab/payload.blob"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(data)
    item = {
        "relative": relative,
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
    }
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE verified (path TEXT PRIMARY KEY, digest TEXT NOT NULL, "
        "signature TEXT NOT NULL, checked_at REAL NOT NULL)"
    )

    assert verify_one(connection, tmp_path, item) == (True, len(data))
    assert verify_one(connection, tmp_path, item) == (False, len(data))
    replacement = path.with_suffix(".new")
    replacement.write_bytes(data)
    replacement.replace(path)
    assert verify_one(connection, tmp_path, item) == (True, len(data))

    path.write_bytes(b"bad payload")
    with pytest.raises(SnapshotError, match="SHA-256"):
        verify_one(connection, tmp_path, item)
