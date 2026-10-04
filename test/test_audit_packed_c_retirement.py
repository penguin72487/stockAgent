from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

import pytest

import scripts.audit_packed_c_retirement as audit_module
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    former = tmp_path / "former-c"
    primary = tmp_path / "d-primary"
    state = tmp_path / "state"
    backup_state = tmp_path / "backup-state"
    for root in (former, primary):
        for name in ("blobs", "packs", "inventories"):
            (root / "objects" / name).mkdir(parents=True)
        (root / "heads/example").mkdir(parents=True)
        (root / "heads/example/penguin.json").write_text("{}")
        (root / "manifests/example").mkdir(parents=True)
        (root / "manifests/example/release.json").write_text("{}")
        (root / ".local-state").mkdir()
        (root / ".local-state/node-id").write_text("penguin\n")
    payload = b"verified old cold object"
    digest = hashlib.sha256(payload).hexdigest()
    relative = f"objects/blobs/{digest[:2]}/{digest}.blob"
    for root in (former, primary):
        path = root / relative
        path.parent.mkdir()
        path.write_bytes(payload)
    state.mkdir()
    backup_state.mkdir()
    (state / "direct-verify-status.json").write_text(
        json.dumps({"state": "verified_all_present_objects"})
    )
    checked = time.time()
    with sqlite3.connect(state / "direct-verified.sqlite3") as connection:
        connection.execute(
            "CREATE TABLE verified (path TEXT PRIMARY KEY, digest TEXT NOT NULL, "
            "signature TEXT NOT NULL, checked_at REAL NOT NULL)"
        )
        connection.execute(
            "INSERT INTO verified VALUES (?,?,?,?)",
            (relative, digest, json.dumps(signature(primary / relative)), checked),
        )
    with sqlite3.connect(backup_state / "verified.sqlite3") as connection:
        connection.execute(
            "CREATE TABLE verified (path TEXT PRIMARY KEY, digest TEXT NOT NULL, "
            "source_sig TEXT NOT NULL, target_sig TEXT NOT NULL, checked REAL NOT NULL)"
        )
        connection.execute(
            "INSERT INTO verified VALUES (?,?,?,?,?)",
            (
                relative,
                digest,
                json.dumps(signature(former / relative)),
                json.dumps(signature(primary / relative)),
                checked,
            ),
        )
    monkeypatch.setattr(audit_module, "C_ROOT", former)
    monkeypatch.setattr(audit_module, "D_ROOT", primary)
    monkeypatch.setattr(audit_module, "STATE_DIR", state)
    monkeypatch.setattr(audit_module, "_check_d_primary", lambda: None)
    monkeypatch.setattr(audit_module, "_distinct_filesystems", lambda a, b: True)
    monkeypatch.setattr(
        audit_module.BackupConfig,
        "load",
        lambda path: SimpleNamespace(state_dir=backup_state, checksum_recheck_days=30),
    )
    return former, primary


def test_c_retirement_audit_proves_exact_existing_object_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _former, _primary = _fixture(tmp_path, monkeypatch)
    result = audit_module.run()
    assert result["state"] == "verified_c_subset_of_d"
    assert result["checked_objects"] == 1
    assert result["c_receipt_trusted_objects"] == 1
    assert result["heads_verified"] == 1


def test_c_retirement_audit_rejects_changed_d_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _former, primary = _fixture(tmp_path, monkeypatch)
    next((primary / "objects/blobs").rglob("*.blob")).write_bytes(b"wrong content")
    with pytest.raises(SnapshotError, match="D primary lacks the verified C object"):
        audit_module.run()


def test_c_retirement_audit_accepts_archived_replaced_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    former, primary = _fixture(tmp_path, monkeypatch)
    old = (former / "heads/example/penguin.json").read_bytes()
    (primary / "heads/example/penguin.json").write_text('{"new":true}')
    digest = hashlib.sha256(old).hexdigest()
    archived = primary / "head-history/heads/example/penguin" / f"{digest}.json"
    archived.parent.mkdir(parents=True)
    archived.write_bytes(old)
    assert audit_module.run()["state"] == "verified_c_subset_of_d"
