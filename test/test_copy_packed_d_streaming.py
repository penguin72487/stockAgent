from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3

import pytest

from scripts.copy_packed_d_streaming import copy_one
from stockagent.data_sync.desync_snapshots import SnapshotError


def _database() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE verified (path TEXT PRIMARY KEY, digest TEXT NOT NULL, "
        "signature TEXT NOT NULL, checked_at REAL NOT NULL)"
    )
    return connection


def test_streaming_copy_is_atomic_and_reuses_verified_object(tmp_path: Path) -> None:
    source = tmp_path / "source"
    stage = tmp_path / "stage"
    payload = b"streaming immutable cold object" * 100
    digest = hashlib.sha256(payload).hexdigest()
    relative = f"objects/blobs/{digest[:2]}/{digest}.blob"
    path = source / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    item = {"relative": relative, "sha256": digest, "bytes": len(payload)}
    connection = _database()

    assert copy_one(connection, source, stage, item) == (True, len(payload))
    assert (stage / relative).read_bytes() == payload
    assert copy_one(connection, source, stage, item) == (False, len(payload))
    assert not list((stage / relative).parent.glob(".stockagent-copy-*.tmp"))


def test_bad_source_fails_closed_and_bad_stage_is_preserved_before_repair(tmp_path: Path) -> None:
    source = tmp_path / "source"
    stage = tmp_path / "stage"
    payload = b"good"
    digest = hashlib.sha256(payload).hexdigest()
    relative = f"objects/blobs/{digest[:2]}/{digest}.blob"
    path = source / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(b"bad!")
    item = {"relative": relative, "sha256": digest, "bytes": len(payload)}
    connection = _database()

    with pytest.raises(SnapshotError, match="source SHA-256"):
        copy_one(connection, source, stage, item)
    assert not (stage / relative).exists()

    (stage / relative).parent.mkdir(parents=True, exist_ok=True)
    (stage / relative).write_bytes(b"bad!")
    with pytest.raises(SnapshotError, match="D raw source is invalid"):
        copy_one(connection, source, stage, item)
    assert (stage / relative).read_bytes() == b"bad!"

    path.write_bytes(payload)
    assert copy_one(connection, source, stage, item) == (True, len(payload))
    assert (stage / relative).read_bytes() == payload
    quarantined = list((stage / ".migration-quarantine").rglob("*.corrupt"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"bad!"
    assert quarantined[0].with_suffix(".corrupt.json").is_file()
