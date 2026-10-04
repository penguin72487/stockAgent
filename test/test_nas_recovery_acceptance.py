"""Pinned NAS semantic proof admission and canonical reconstruction boundaries."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
from pathlib import Path

import pytest

from scripts.backup_delivery_receipt import signed
from scripts.verify_backup_delivery import verify
from stockagent.data_sync.nas_recovery_acceptance import (
    ACCEPTANCE, PLAN, recovery_status, run_acceptance, validate_acceptance, validate_plan, verify_restored_job,
)
from stockagent.data_sync.offhost_backup import ResticBackup, capture_plan, export_delivery
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.runtime_identity import runtime_identity
from test_backup_stream import stream  # noqa: F401


@pytest.fixture
def recovery_plan():
    return signed({"contract": PLAN, "producer_device_id": "source-test", "receiver_device_id": "receiver-test",
        "repository_id": "a" * 64, "minimum_free_bytes": 1024, "maximum_restore_bytes": 1024**2,
        "full_history_backup_verified": False, "jobs": [{"job_id": "prices", "kind": "packed",
        "nas_snapshot_id": "b" * 64, "envelope_identity_sha256": "c" * 64, "envelope_file_sha256": "d" * 64,
        "complete_files": 4, "complete_bytes": 100, "dataset": "prices", "source_snapshot_id": "prices-fixed",
        "manifest_sha256": "e" * 64, "source_fingerprint_sha256": "f" * 64},
        {"job_id": "control", "kind": "control", "nas_snapshot_id": "1" * 64,
        "envelope_identity_sha256": "2" * 64, "envelope_file_sha256": "3" * 64,
        "complete_files": 7, "complete_bytes": 200, "logical_state_identity_sha256": "4" * 64,
        "table_count": 50, "row_count": 60}]})


def acceptance(plan):
    jobs = deepcopy(plan["jobs"])
    for job in jobs:
        job["all_files_sha256_verified"] = True
        if job["kind"] == "packed":
            job.update(canonical_reconstruction_verified=True, copied_cold_files=4)
        else:
            job.update(control_database_restore_verified=True, no_tcp_listener=True, production_database_modified=False)
    return signed({"contract": ACCEPTANCE, "plan_identity_sha256": plan["identity_sha256"],
        **{k: plan[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")}, "jobs": jobs,
        "nas_mount_guard_verified": True, "single_owner_verified": True, "runtime_lock_verified": True,
        "runtime_identity_sha256": "5" * 64, "repository_check_verified": True,
        "restic_command_exit_codes": [0] * 8, "complete_workflow_seconds": 10.5,
        "accepted_at_utc": datetime.now(timezone.utc).isoformat(), "private_evidence_sha256": "6" * 64,
        "full_history_backup_verified": False, "scope": "named_fixed_nas_snapshots_only"})


@pytest.mark.parametrize("damage", ["latest", "duplicate", "unknown", "budget", "path", "tamper"])
def test_plan_rejects_unpinned_or_unsafe_scope(recovery_plan, damage):
    plan = deepcopy(recovery_plan)
    if damage == "latest": plan["jobs"][0]["nas_snapshot_id"] = "latest"
    elif damage == "duplicate": plan["jobs"][1]["job_id"] = plan["jobs"][0]["job_id"]
    elif damage == "unknown": plan["jobs"][0]["password"] = "never permitted"
    elif damage == "budget": plan["jobs"][0]["complete_bytes"] = plan["maximum_restore_bytes"] + 1
    elif damage == "path": plan["jobs"][0]["source_snapshot_id"] = "../latest"
    else: plan["repository_id"] = "9" * 64
    if damage != "tamper": plan = signed({k: v for k, v in plan.items() if k != "identity_sha256"})
    with pytest.raises((ValueError, SnapshotError)): validate_plan(plan)


@pytest.mark.parametrize("damage", ["snapshot", "fingerprint", "columns", "tcp", "missing_job", "exit75", "scope", "secret"])
def test_semantic_acceptance_requires_all_independent_proofs(recovery_plan, damage):
    proof = acceptance(recovery_plan)
    if damage == "snapshot": proof["jobs"][0]["nas_snapshot_id"] = "7" * 64
    elif damage == "fingerprint": proof["jobs"][0]["source_fingerprint_sha256"] = "8" * 64
    elif damage == "columns": proof["jobs"][1]["logical_state_identity_sha256"] = "9" * 64
    elif damage == "tcp": proof["jobs"][1]["no_tcp_listener"] = False
    elif damage == "missing_job": proof["jobs"].pop()
    elif damage == "exit75": proof["restic_command_exit_codes"][-1] = 75
    elif damage == "scope": proof["full_history_backup_verified"] = True
    else: proof["private_password_file"] = "/must/not/return"
    proof = signed({k: v for k, v in proof.items() if k != "identity_sha256"})
    with pytest.raises(ValueError): validate_acceptance(proof, recovery_plan)


def test_custody_confirmation_is_separate_from_nas_proof(tmp_path, recovery_plan):
    plan = tmp_path / "plan.json"; plan.write_text(json.dumps(recovery_plan))
    custody = tmp_path / "custody.json"
    custody.write_text(json.dumps({"state": "user_confirmed_controlled_usb_custody", "evidence_origin": "explicit_user_confirmation"}))
    config = {**{k: recovery_plan[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
        "nas_recovery_plan": str(plan), "key_custody_confirmation": str(custody)}
    initial = recovery_status(config, tmp_path)
    assert initial["usb_key_custody_user_confirmed"] and not initial["control_logical_restore_verified"]
    assert initial["state"] == "awaiting_lab203_fixed_snapshot_execution"
    proof = acceptance(recovery_plan)
    (tmp_path / ("recovery-acceptance-" + recovery_plan["identity_sha256"] + ".json")).write_text(json.dumps(proof))
    result = recovery_status(config, tmp_path)
    assert result["control_logical_restore_verified"]
    assert len(result["packed_reconstructed_releases"]) == 1
    assert result["state"] == "named_nas_semantic_recovery_verified"


def test_real_closed_delivery_reconstructs_exact_source_then_rejects_wrong_pin(stream, tmp_path):
    queue, cold, source, _, selected = stream
    snapshot_id = selected.manifest["snapshot_id"]
    plan = capture_plan(cold, selectors=[("prices", snapshot_id)])
    delivery = tmp_path / "delivery"
    exported = export_delivery(plan, delivery)
    raw = (delivery / "backup-envelope.json").read_bytes()
    verified = verify(delivery, exported["envelope_identity_sha256"])
    job = {"job_id": "prices", "kind": "packed", "nas_snapshot_id": "a" * 64,
        "envelope_identity_sha256": exported["envelope_identity_sha256"], "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
        "complete_files": verified["files_verified"] + 2,
        "complete_bytes": verified["bytes_verified"] + len(raw) + (delivery / "READY").stat().st_size,
        "dataset": "prices", "source_snapshot_id": snapshot_id, "manifest_sha256": selected.manifest_sha256,
        "source_fingerprint_sha256": selected.manifest["source"]["portable_fingerprint_sha256"]}
    result = verify_restored_job(job, delivery, tmp_path / "independent")
    assert result["canonical_reconstruction_verified"]
    actual = next((tmp_path / "independent/source/prices").rglob("價格.txt"))
    assert actual.read_bytes() == (source / "價格.txt").read_bytes()
    bad = {**job, "manifest_sha256": "f" * 64}
    with pytest.raises(SnapshotError, match="manifest is absent"):
        verify_restored_job(bad, delivery, tmp_path / "wrong-pin")


@pytest.fixture
def local_admission(tmp_path):
    mount = tmp_path / "nas"
    (mount / "user/repository").mkdir(parents=True)
    profile = tmp_path / "nas-profile.json"
    profile.write_text(json.dumps({"schema_version": 1, "automatic_pruning": False, "reserve_bytes": 1024,
        "nas": {"host": "140.127.208.143", "share": "Lab203", "mount_point": str(mount),
                "relative_directory": "user", "repository_directory": "repository"}}))
    lock = tmp_path / "owner.lock"; lock.touch()
    runtime = tmp_path / "runtime-lock.json"; runtime.write_text(json.dumps(runtime_identity()))
    password = tmp_path / "password"; password.write_text("test fixture only"); password.chmod(0o600)
    return {"nas_configuration": profile, "restic": Path("/unused/no-command-permitted"),
        "password_file": password, "runtime_lock": runtime, "owner_lock": lock,
        "output": tmp_path / "recovery", "receipt_root": tmp_path / "receipts"}


def test_local_directory_cannot_be_promoted_to_nas_provenance(local_admission, recovery_plan):
    with pytest.raises(SnapshotError): run_acceptance(recovery_plan, **local_admission)
    assert not local_admission["output"].exists()
    assert not local_admission["receipt_root"].exists()


def test_active_original_owner_prevents_parallel_recovery(local_admission, recovery_plan):
    with local_admission["owner_lock"].open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError): run_acceptance(recovery_plan, **local_admission)
    assert not local_admission["output"].exists()


def test_bad_recovery_runtime_never_touches_nas_or_receipts(local_admission, recovery_plan):
    local_admission["runtime_lock"].write_text("{}")
    with pytest.raises(ValueError, match="runtime|role"):
        run_acceptance(recovery_plan, **local_admission)
    assert not local_admission["output"].exists()


def test_invalid_semantic_evidence_does_not_stop_ordinary_stream(stream, tmp_path):
    queue, *_ = stream
    bad = tmp_path / "bad-custody.json"; bad.write_text("{}")
    queue.config["key_custody_confirmation"] = str(bad)
    status = queue.cycle(publish=False)
    assert status["independent_recovery"]["state"] == "invalid_recovery_evidence"
    assert status["receipt_errors"] == []
    assert status["state"] == "inspected"


def test_complete_runner_with_real_restic_and_simulated_mount_admission(stream, local_admission, tmp_path, monkeypatch):
    # The mount is a unit-test simulation; no actual NAS proof is claimed by this test.
    binary = os.environ.get("STOCKAGENT_TEST_RESTIC_BINARY")
    if not binary: pytest.skip("real Restic binary not supplied")
    from stockagent.data_sync import nas_target
    monkeypatch.setattr(nas_target, "mounted_volume", lambda _: ("unit-test", "cifs", "//140.127.208.143/Lab203"))
    _, cold, _, _, selected = stream
    snapshot_id = selected.manifest["snapshot_id"]
    delivery = tmp_path / "real-delivery"
    exported = export_delivery(capture_plan(cold, selectors=[("prices", snapshot_id)]), delivery)
    local_admission["restic"] = Path(binary)
    repository = tmp_path / "nas/user/repository"
    client = ResticBackup(Path(binary), str(repository), local_admission["password_file"], cache=tmp_path / "test-cache")
    repo = client.initialize()
    backup = client.run(["backup", "--json", str(delivery)])
    summary = next(json.loads(line) for line in backup.stdout.splitlines() if json.loads(line).get("message_type") == "summary")
    raw = (delivery / "backup-envelope.json").read_bytes()
    transport = verify(delivery, exported["envelope_identity_sha256"])
    job = {"job_id": "prices", "kind": "packed", "nas_snapshot_id": summary["snapshot_id"],
        "envelope_identity_sha256": exported["envelope_identity_sha256"], "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
        "complete_files": transport["files_verified"] + 2,
        "complete_bytes": transport["bytes_verified"] + len(raw) + (delivery / "READY").stat().st_size,
        "dataset": "prices", "source_snapshot_id": snapshot_id, "manifest_sha256": selected.manifest_sha256,
        "source_fingerprint_sha256": selected.manifest["source"]["portable_fingerprint_sha256"]}
    plan = signed({"contract": PLAN, "producer_device_id": "source-test", "receiver_device_id": "receiver-test",
        "repository_id": repo["id"], "jobs": [job], "minimum_free_bytes": 1024,
        "maximum_restore_bytes": 1024**2, "full_history_backup_verified": False})
    result = run_acceptance(plan, **local_admission)
    validate_acceptance(result, plan)
    assert result["jobs"][0]["canonical_reconstruction_verified"]
    assert str(tmp_path) not in json.dumps(result)
    again = run_acceptance(plan, **{**local_admission, "output": tmp_path / "unused-repeat"})
    assert again == result and not (tmp_path / "unused-repeat").exists()
