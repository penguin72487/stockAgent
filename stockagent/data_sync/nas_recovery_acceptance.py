"""Semantic recovery from fixed NAS snapshots, under the existing relay owner.

The file ACK contract stays unchanged. These separately pinned receipts prove
only the named releases and control snapshot, never the full backup inventory.
"""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import time

from scripts.backup_delivery_receipt import HASH, atomic_public, read_json, signed, valid_signature
from scripts.verify_backup_delivery import verify
from stockagent.control.recovery import restore_control
from stockagent.data_sync.backup_auxiliary import verified_control, verified_working_tree
from stockagent.data_sync.backup_recovery import assemble_release
from stockagent.data_sync.desync_snapshots import scan_tree, validate_slug
from stockagent.data_sync.nas_target import NasTarget
from stockagent.data_sync.offhost_backup import ResticBackup, private_json, _regular
from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, resolve_packed_snapshot_id, verify_packed_snapshot
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock

PLAN = "fixed_nas_semantic_recovery_plan_v1"
ACCEPTANCE = "fixed_nas_semantic_recovery_acceptance_v1"
COMMON = {"job_id", "kind", "nas_snapshot_id", "envelope_identity_sha256", "envelope_file_sha256",
          "complete_files", "complete_bytes"}
PACKED = {"dataset", "source_snapshot_id", "manifest_sha256", "source_fingerprint_sha256"}
CONTROL = {"logical_state_identity_sha256", "table_count", "row_count"}
ACCEPTANCE_FIELDS = {"contract", "plan_identity_sha256", "producer_device_id", "receiver_device_id",
    "repository_id", "jobs", "nas_mount_guard_verified", "single_owner_verified", "runtime_lock_verified",
    "runtime_identity_sha256", "repository_check_verified", "restic_command_exit_codes",
    "complete_workflow_seconds", "accepted_at_utc", "private_evidence_sha256", "full_history_backup_verified",
    "scope", "identity_sha256"}


def validate_plan(plan: dict) -> None:
    valid_signature(plan, PLAN, fields={"contract", "producer_device_id", "receiver_device_id", "repository_id",
        "jobs", "minimum_free_bytes", "maximum_restore_bytes", "full_history_backup_verified", "identity_sha256"})
    if not HASH.fullmatch(plan["repository_id"]) or plan["full_history_backup_verified"] is not False:
        raise ValueError("recovery plan must pin one repository and bounded scope")
    if not all(isinstance(plan[k], str) and plan[k] for k in ("producer_device_id", "receiver_device_id")):
        raise ValueError("recovery plan needs paired identities")
    for key in ("minimum_free_bytes", "maximum_restore_bytes"):
        if type(plan[key]) is not int or plan[key] <= 0:
            raise ValueError("recovery capacity must be positive")
    if not isinstance(plan["jobs"], list) or not plan["jobs"] or len(plan["jobs"]) > 16:
        raise ValueError("recovery plan needs a bounded job list")
    names = set()
    for job in plan["jobs"]:
        expected = COMMON | (PACKED if job.get("kind") == "packed" else CONTROL if job.get("kind") == "control" else set())
        if job.get("kind") not in {"packed", "control"} or set(job) != expected:
            raise ValueError("recovery job has an unknown kind or field set")
        name = validate_slug(job["job_id"], "job_id")
        if name in names:
            raise ValueError("recovery job identities must be unique")
        names.add(name)
        for key in ("nas_snapshot_id", "envelope_identity_sha256", "envelope_file_sha256"):
            if not HASH.fullmatch(job[key]):
                raise ValueError("recovery requires full immutable hashes, never latest")
        for key in ("complete_files", "complete_bytes"):
            if type(job[key]) is not int or job[key] < 2:
                raise ValueError("recovery requires positive complete delivery counts")
        if job["complete_bytes"] > plan["maximum_restore_bytes"]:
            raise ValueError("recovery job exceeds its restore budget")
        if job["kind"] == "packed":
            validate_slug(job["dataset"], "dataset")
            validate_slug(job["source_snapshot_id"], "snapshot_id")
            if not all(HASH.fullmatch(job[k]) for k in ("manifest_sha256", "source_fingerprint_sha256")):
                raise ValueError("recovery must pin original source provenance")
        elif (not HASH.fullmatch(job["logical_state_identity_sha256"])
              or any(type(job[k]) is not int or job[k] < 0 for k in ("table_count", "row_count"))):
            raise ValueError("control recovery must pin same-MVCC logical state")


def verify_restored_job(job: dict, delivery: Path, output: Path, *, pg_bin: Path | None = None) -> dict:
    """Canonical semantic checks; the caller separately proves NAS provenance."""
    raw = _regular(delivery / "backup-envelope.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != job["envelope_file_sha256"]:
        raise ValueError("restored envelope differs from the source-pinned raw file")
    transport = verify(delivery, job["envelope_identity_sha256"])
    size = transport["bytes_verified"] + len(raw) + _regular(delivery / "READY").stat().st_size
    if transport["files_verified"] + 2 != job["complete_files"] or size != job["complete_bytes"]:
        raise ValueError("restored complete file set differs from the pinned job")
    result = {**job, "all_files_sha256_verified": True}
    if job["kind"] == "packed":
        cold = output / "cold"
        union = assemble_release([{"root": str(delivery), **{k: job[k] for k in
            ("envelope_identity_sha256", "envelope_file_sha256")}}], cold,
            dataset=job["dataset"], snapshot_id=job["source_snapshot_id"], manifest_sha256=job["manifest_sha256"])
        selected = resolve_packed_snapshot_id(cold, job["dataset"], job["source_snapshot_id"])
        materialized = fetch_packed_snapshot(cold, output / "source", selected)
        verify_packed_snapshot(cold, selected, materialized_path=materialized)
        fingerprint = scan_tree(materialized)["portable_fingerprint_sha256"]
        if fingerprint != job["source_fingerprint_sha256"] or fingerprint != union["portable_source_fingerprint_sha256"]:
            raise ValueError("NAS reconstructed source fingerprint differs from source pin")
        result.update(canonical_reconstruction_verified=True, copied_cold_files=union["copied_files"])
    else:
        verified_working_tree(delivery / "code")
        receipt, _, _ = verified_control(delivery / "control")
        if receipt["logical_state_identity_sha256"] != job["logical_state_identity_sha256"]:
            raise ValueError("NAS control logical identity differs from source pin")
        control = restore_control(delivery / "control", output / "postgresql", pg_bin=pg_bin)
        if any(control[k] != job[k] for k in CONTROL):
            raise ValueError("NAS restored control state differs from the pinned same-MVCC snapshot")
        result.update(control_database_restore_verified=True, no_tcp_listener=control["no_tcp_listener"],
                      production_database_modified=control["production_database_modified"])
    return result


def validate_acceptance(receipt: dict, plan: dict) -> None:
    validate_plan(plan)
    valid_signature(receipt, ACCEPTANCE, fields=ACCEPTANCE_FIELDS)
    if receipt["plan_identity_sha256"] != plan["identity_sha256"] or any(
            receipt[k] != plan[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")):
        raise ValueError("semantic acceptance differs from source-pinned plan")
    if receipt["scope"] != "named_fixed_nas_snapshots_only" or receipt["full_history_backup_verified"] is not False:
        raise ValueError("semantic acceptance cannot claim full history")
    for key in ("nas_mount_guard_verified", "single_owner_verified", "runtime_lock_verified", "repository_check_verified"):
        if receipt[key] is not True:
            raise ValueError("semantic acceptance has an incomplete operational proof")
    for key in ("private_evidence_sha256", "runtime_identity_sha256"):
        if not HASH.fullmatch(receipt[key]):
            raise ValueError("semantic acceptance needs evidence and runtime identity")
    exits = receipt["restic_command_exit_codes"]
    if not isinstance(exits, list) or len(exits) < 2 + 2 * len(plan["jobs"]) or any(type(x) is not int or x != 0 for x in exits):
        raise ValueError("semantic recovery commands must all exit zero")
    seconds = receipt["complete_workflow_seconds"]
    if type(seconds) not in {int, float} or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("semantic recovery needs complete measured wall time")
    accepted = datetime.fromisoformat(receipt["accepted_at_utc"])
    if accepted.tzinfo is None or (accepted - datetime.now(timezone.utc)).total_seconds() > 300:
        raise ValueError("semantic acceptance time is invalid")
    if not isinstance(receipt["jobs"], list) or len(receipt["jobs"]) != len(plan["jobs"]):
        raise ValueError("semantic acceptance lacks planned jobs")
    for actual, expected in zip(receipt["jobs"], plan["jobs"], strict=True):
        extra = {"all_files_sha256_verified"} | ({"canonical_reconstruction_verified", "copied_cold_files"}
            if expected["kind"] == "packed" else {"control_database_restore_verified", "no_tcp_listener", "production_database_modified"})
        if set(actual) != set(expected) | extra or any(type(actual[k]) is not type(v) or actual[k] != v for k, v in expected.items()):
            raise ValueError("semantic job identity or exact field set differs")
        if actual["all_files_sha256_verified"] is not True:
            raise ValueError("semantic recovery lacks complete file hashes")
        if expected["kind"] == "packed":
            if actual["canonical_reconstruction_verified"] is not True or type(actual["copied_cold_files"]) is not int or actual["copied_cold_files"] < 2:
                raise ValueError("canonical source recovery is incomplete")
        elif (actual["control_database_restore_verified"] is not True or actual["no_tcp_listener"] is not True
              or actual["production_database_modified"] is not False):
            raise ValueError("isolated logical restore proof is incomplete")


def run_acceptance(plan: dict, *, nas_configuration: Path, restic: Path, password_file: Path,
                   runtime_lock: Path, owner_lock: Path, output: Path, receipt_root: Path,
                   pg_bin: Path | None = None, wait_for_owner: bool = False, owner_handle=None) -> dict:
    validate_plan(plan)
    output = output.absolute()
    if output.exists() or any(p.is_symlink() for p in (output, *output.parents)):
        raise ValueError("keep a fresh independent semantic recovery output")
    _regular(owner_lock)
    identity = runtime_identity()
    if validate_runtime_lock(read_json(runtime_lock), identity):
        raise ValueError("recovery role differs from its accepted Miniforge/Mamba lock")
    previous = receipt_root / ("recovery-acceptance-" + plan["identity_sha256"] + ".json")
    if previous.exists():
        acceptance = read_json(previous)
        validate_acceptance(acceptance, plan)
        return acceptance  # Preserve the first completed fixed-snapshot proof.
    target = NasTarget(nas_configuration)
    if output.is_relative_to(target.mount_point) or output.is_relative_to(receipt_root.absolute()):
        raise ValueError("private recovery output must be outside NAS and Syncthing receipts")
    old = os.umask(0o077)
    created = False
    try:
        with (nullcontext(owner_handle) if owner_handle is not None else owner_lock.open("a")) as owner:
            before_lock = owner_lock.stat()
            opened = os.fstat(owner.fileno())
            if (opened.st_ino, opened.st_dev) != (before_lock.st_ino, before_lock.st_dev):
                raise ValueError("recovery must reuse the exact existing relay owner")
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            if (owner_lock.stat().st_ino, owner_lock.stat().st_dev) != (before_lock.st_ino, before_lock.st_dev):
                raise ValueError("existing relay owner lock changed")
            mount = target.check()
            if shutil.disk_usage(output.parent).free < plan["minimum_free_bytes"] + plan["maximum_restore_bytes"]:
                raise ValueError("independent recovery scratch capacity is insufficient")
            started = time.perf_counter()
            output.mkdir(mode=0o755)
            created = True
            output.chmod(0o755)  # Only this new parent; nobody needs to reach its fresh PG child.
            client = ResticBackup(restic, str(target.repository), password_file, cache=output / "cache")
            exits = []
            def run(arguments):
                target.unchanged(mount)
                result = client.run(arguments)
                exits.append(result.returncode)
                target.unchanged(mount)
                return result
            if json.loads(run(["cat", "config"]).stdout)["id"] != plan["repository_id"]:
                raise ValueError("mounted NAS repository identity differs")
            run(["check"])
            proofs = []
            for job in plan["jobs"]:
                if shutil.disk_usage(output).free < plan["minimum_free_bytes"] + plan["maximum_restore_bytes"]:
                    raise ValueError("remaining independent recovery scratch capacity is insufficient")
                snapshots = json.loads(run(["snapshots", "--json", job["nas_snapshot_id"]]).stdout)
                if len(snapshots) != 1 or snapshots[0]["id"] != job["nas_snapshot_id"]:
                    raise ValueError("NAS restore snapshot identity differs")
                listing = run(["ls", "--json", job["nas_snapshot_id"]])
                nodes = [json.loads(line) for line in listing.stdout.splitlines() if line.strip()]
                bytes_to_restore = sum(n.get("size", 0) for n in nodes if n.get("type") == "file")
                if bytes_to_restore > plan["maximum_restore_bytes"]:
                    raise ValueError("fixed snapshot exceeds local restore budget")
                root = output / job["job_id"]
                root.mkdir(mode=0o755)
                root.chmod(0o755)
                restored = root / "restored"
                target.unchanged(mount)
                client.restore(job["nas_snapshot_id"], restored)
                exits.append(0)  # restore() admits only a completed zero-exit Restic command.
                target.unchanged(mount)
                matches = [p.parent for p in restored.rglob("backup-envelope.json") if
                    hashlib.sha256(_regular(p).read_bytes()).hexdigest() == job["envelope_file_sha256"]]
                if len(matches) != 1:
                    raise ValueError("fixed snapshot must contain exactly one pinned closed delivery")
                proofs.append(verify_restored_job(job, matches[0], root, pg_bin=pg_bin))
            target.unchanged(mount)
            if validate_runtime_lock(read_json(runtime_lock), runtime_identity()):
                raise ValueError("recovery runtime changed during acceptance")
            body = {"contract": ACCEPTANCE, "plan_identity_sha256": plan["identity_sha256"],
                **{k: plan[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
                "jobs": proofs, "nas_mount_guard_verified": True, "single_owner_verified": True,
                "runtime_lock_verified": True, "runtime_identity_sha256": identity["sha256"],
                "repository_check_verified": True, "restic_command_exit_codes": exits,
                "complete_workflow_seconds": time.perf_counter() - started,
                "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
                "full_history_backup_verified": False, "scope": "named_fixed_nas_snapshots_only"}
            private = output / "acceptance.private.json"
            private_json(private, {**body, "mount": mount, "private_output": str(output)})
            acceptance = signed({**body, "private_evidence_sha256": hashlib.sha256(private.read_bytes()).hexdigest()})
            validate_acceptance(acceptance, plan)
            atomic_public(receipt_root / ("recovery-acceptance-" + plan["identity_sha256"] + ".json"), acceptance)
            return acceptance
    except Exception as error:
        if created:
            private_json(output / "failure.private.json", {"state": "failed", "error_type": type(error).__name__,
                "plan_identity_sha256": plan["identity_sha256"], "observed_at_utc": datetime.now(timezone.utc).isoformat()})
        raise
    finally:
        os.umask(old)


def recovery_status(configuration: dict, receipt_root: Path) -> dict:
    result = {"state": "not_configured", "usb_key_custody_user_confirmed": False,
              "packed_reconstructed_releases": [], "control_logical_restore_verified": False}
    custody = configuration.get("key_custody_confirmation")
    if custody:
        value = read_json(Path(custody))
        if value.get("state") != "user_confirmed_controlled_usb_custody" or value.get("evidence_origin") != "explicit_user_confirmation":
            raise ValueError("USB custody needs explicit user evidence")
        result["usb_key_custody_user_confirmed"] = True
    if configuration.get("nas_recovery_plan"):
        plan = read_json(Path(configuration["nas_recovery_plan"]))
        validate_plan(plan)
        if any(plan[k] != configuration[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")):
            raise ValueError("recovery plan does not belong to this relay")
        result.update(state="awaiting_lab203_fixed_snapshot_execution", plan_identity_sha256=plan["identity_sha256"])
        path = receipt_root / ("recovery-acceptance-" + plan["identity_sha256"] + ".json")
        if path.exists():
            acceptance = read_json(path)
            validate_acceptance(acceptance, plan)
            result.update(state="named_nas_semantic_recovery_verified", acceptance_identity_sha256=acceptance["identity_sha256"],
                packed_reconstructed_releases=[{k: j[k] for k in ("dataset", "source_snapshot_id", "manifest_sha256")}
                    for j in acceptance["jobs"] if j["kind"] == "packed"],
                control_logical_restore_verified=any(j["kind"] == "control" for j in acceptance["jobs"]),
                accepted_at_utc=acceptance["accepted_at_utc"], complete_workflow_seconds=acceptance["complete_workflow_seconds"])
    return result
