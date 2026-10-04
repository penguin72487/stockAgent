"""Frozen dirty working trees, privacy exclusions and consistent control bytes."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

import pytest

from stockagent.data_sync.backup_auxiliary import (
    export_auxiliary, verified_control, verified_working_tree, working_tree_inventory,
)
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.runtime_identity import identity_sha256
from scripts.verify_backup_delivery import verify


@pytest.fixture
def auxiliary(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    for parent in ("stockagent", "configs", "docs", "test"):
        (root / parent).mkdir()
    (root / "stockagent/__init__.py").write_text('"""public source"""\n')
    (root / "README.md").write_text("source and restore contract\n")
    (root / "docs/audit.md").write_text("evidence boundary\n")
    (root / "test/test_contract.py").write_text("def test_contract(): assert True\n")
    (root / ".gitignore").write_text(".env\ndata_*\n")
    (root / "configs/public.json").write_text('{"batch_size": 16}')
    (root / "configs/private.json").write_text('{"secret": "synthetic-private"}')
    (root / "configs/auth.json").write_text('{"api_key": "synthetic-unit-token"}')
    (root / ".env").write_text("SYNTHETIC_SECRET=do-not-send\n")
    (root / "data_restricted").mkdir()
    (root / "data_restricted/source.csv").write_text("local only\n")
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=backup-test", "-c", "user.email=backup-test@localhost",
                    "commit", "-qm", "synthetic fixture"], cwd=root, check=True)
    # Preserve dirty/untracked work and a deliberate tracked deletion.
    (root / "README.md").write_text("new uncommitted revision\n")
    (root / "configs/new.json").write_text('{"new_contract": true}')
    (root / "docs/audit.md").unlink()
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    archive = control / "fake-test.backup"
    archive.write_bytes(b"synthetic opaque dump; never claims logical restore")
    archive.chmod(0o600)
    body = {"contract": "all_user_table_columns_sorted_jsonb_rows_utc_v1", "tables": []}
    state = control / "state.json"
    state.write_text(json.dumps({**body, "identity_sha256": identity_sha256(body), "table_count": 0, "row_count": 0}))
    state.chmod(0o600)
    receipt = control / "latest.json"
    receipt.write_text(json.dumps({"state": "backup_written", "same_mvcc_snapshot_as_dump": True,
        "archive": str(archive), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "logical_state_file": str(state), "logical_state_file_sha256": hashlib.sha256(state.read_bytes()).hexdigest(),
        "logical_state_identity_sha256": identity_sha256(body), "observed_at_utc": datetime.now(timezone.utc).isoformat()}))
    receipt.chmod(0o600)
    return root, receipt, archive


def test_freeze_includes_uncommitted_untracked_tests_and_deletions(auxiliary, tmp_path):
    root, receipt, _ = auxiliary
    destination = tmp_path / "delivery"
    result = export_auxiliary(root, destination, control_receipt=receipt)
    verify(destination, result["envelope_identity_sha256"])
    with zipfile.ZipFile(destination / "code/working-tree.zip") as archive:
        assert archive.read("README.md") == b"new uncommitted revision\n"
        assert "configs/new.json" in archive.namelist()
        assert "test/test_contract.py" in archive.namelist()
        assert not any(n.startswith((".env", "data_", ".git/")) for n in archive.namelist())
        assert "configs/private.json" not in archive.namelist()
        assert "configs/auth.json" not in archive.namelist()
    tree = json.loads((destination / "code/receipt.json").read_bytes())
    assert tree["deleted_tracked_paths"] == ["docs/audit.md"]
    assert verified_working_tree(destination / "code")["files_verified"] == len(tree["files"])
    assert result["control_database_restore_verified"] is False
    assert result["durable_off_host_backup_verified"] is False


def test_restored_working_tree_cannot_accept_extra_or_corrupt_inner_files(auxiliary, tmp_path):
    root, receipt, _ = auxiliary
    destination = tmp_path / "delivery"
    export_auxiliary(root, destination, control_receipt=receipt)
    with zipfile.ZipFile(destination / "code/working-tree.zip", "a") as archive:
        archive.writestr("unrecorded-secret.txt", "unexpected member")
    with pytest.raises(SnapshotError, match="set differs"):
        verified_working_tree(destination / "code")


@pytest.mark.parametrize("damage", ["archive", "logical_state", "same_mvcc", "public_archive"])
def test_invalid_control_receipt_cannot_enter_a_closed_delivery(auxiliary, tmp_path, damage):
    root, receipt, archive = auxiliary
    body = json.loads(receipt.read_bytes())
    if damage == "archive":
        archive.write_bytes(b"altered dump")
    elif damage == "logical_state":
        Path(body["logical_state_file"]).write_text("{}")
    elif damage == "same_mvcc":
        body["same_mvcc_snapshot_as_dump"] = False
        receipt.write_text(json.dumps(body))
    else:
        archive.chmod(0o644)
    destination = tmp_path / "bad-delivery"
    with pytest.raises(SnapshotError):
        export_auxiliary(root, destination, control_receipt=receipt)
    assert not (destination / "READY").exists()


def test_portable_control_uses_relative_objects_and_retains_exact_logical_identity(auxiliary, tmp_path):
    root, receipt, _ = auxiliary
    destination = tmp_path / "delivery"
    export_auxiliary(root, destination, control_receipt=receipt)
    portable, archive, state = verified_control(destination / "control")
    assert portable["archive"] == "archive.backup"
    assert portable["source_receipt_sha256"] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert state["identity_sha256"] == portable["logical_state_identity_sha256"]
    assert not str(archive).startswith(str(receipt.parent))


def test_private_runtime_and_ignored_licensed_sources_do_not_affect_public_code_identity(auxiliary):
    root, _, _ = auxiliary
    before = working_tree_inventory(root, documentation=True, configs=True)
    (root / ".env").write_text("another synthetic credential\n")
    (root / "data_restricted/source.csv").write_text("another unpublished vintage\n")
    after = working_tree_inventory(root, documentation=True, configs=True)
    assert before == after


def test_public_credentials_documentation_tests_and_examples_are_recoverable(auxiliary):
    root, _, _ = auxiliary
    public = {"docs/data_credentials.md": "Use the local credential store; do not paste passwords.\n",
        "test/test_private_json.py": "def test_private_contract(): assert True\n",
        "configs/credentials.example.json": '{"api_key": "${PROVIDER_API_KEY}"}',
        "configs/authorization-policy.json": '{"authorization": "explicit user requested public data"}'}
    for name, text in public.items():
        (root / name).write_text(text)
    # The machine's global ignore deliberately excludes credential-named
    # documentation; this fixture declares the public document tracked.
    subprocess.run(["git", "add", "-f", *public], cwd=root, check=True)
    (root / "configs/credentials-real.example.json").write_text('{"api_key": "synthetic-real-secret"}')
    (root / "configs/header.json").write_text('{"authorization": "Bearer synthetic-auth"}')
    inventory = working_tree_inventory(root, documentation=True, configs=True)
    assert set(public) <= set(inventory["files"])
    assert "configs/credentials-real.example.json" in inventory["excluded_private_paths"]
    assert "configs/header.json" in inventory["excluded_private_paths"]


def test_deliberate_core_code_deletion_is_recorded_without_weakening_canonical_identity(auxiliary, tmp_path):
    from stockagent.runtime_identity import source_identity
    root, receipt, _ = auxiliary
    (root / "README.md").unlink()
    with pytest.raises((OSError, ValueError)):
        source_identity(root)
    destination = tmp_path / "deleted-code-delivery"
    export_auxiliary(root, destination, control_receipt=receipt)
    inventory = json.loads((destination / "code/receipt.json").read_bytes())
    assert "README.md" in inventory["deleted_tracked_paths"]
    assert "README.md" not in inventory["files"]
    verified_working_tree(destination / "code")
