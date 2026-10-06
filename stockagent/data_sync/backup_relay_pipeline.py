"""One existing relay owner, bounded concurrent Restic jobs and durable retry.

Backup B can overlap fixed-snapshot restore A. The repository structural check
is an exclusive barrier, after all Restic jobs finish; it is never bypassed.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time
import uuid

from scripts.backup_delivery_receipt import (
    HASH, atomic_public, publish, publish_readiness, read_json, signed,
    valid_signature, validate_ack, validate_dispatch, verify_dispatch_delivery,
)
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.nas_target import NasTarget
from stockagent.data_sync.offhost_backup import ResticBackup, private_file, private_json
from stockagent.data_sync.packed_backup import signature
from stockagent.data_sync.recovery_queue import physical_free_bytes
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock

STATUS = "parallel_nas_backup_relay_status_v1"
PAIRED = ("producer_device_id", "receiver_device_id", "repository_id")
PIPELINE_HANDOFF_VERSIONS = ("v8", "v9", "v10")


def receiver_status(configuration, transport, receipts):
    """Source recognizes only the fixed deployed handoff and public fields."""
    path = Path(receipts) / "pipeline-status.json"
    if not path.exists():
        return {"state": "waiting_locally_installed_parallel_receiver"}
    value = read_json(path)
    valid_signature(value, STATUS, fields={"contract", *PAIRED, "installed_handoff_identity_sha256", "parallelism",
        "jobs", "runtime_lock_verified", "single_controller_owner_verified", "full_history_backup_verified",
        "observed_at_utc", "complete_cycle_seconds", "identity_sha256"})
    if any(value[k] != configuration[k] for k in PAIRED):
        raise ValueError("pipeline status belongs to another paired repository")
    available = {}
    for version in PIPELINE_HANDOFF_VERSIONS:
        path = Path(transport) / ("tools/continuous-backup-20261004-"+version+"/handoff-manifest.json")
        if path.is_file():
            manifest = read_json(path)
            valid_signature(manifest, "frozen_backup_receiver_handoff_v1")
            available[version] = manifest["identity_sha256"]
    installed = next((version for version,digest in available.items()
        if digest == value["installed_handoff_identity_sha256"]),None)
    if installed is None:
        raise ValueError("parallel receiver differs from its frozen local-install handoff")
    if (value["runtime_lock_verified"] is not True or value["single_controller_owner_verified"] is not True
            or value["full_history_backup_verified"] is not False):
        raise ValueError("parallel receiver has not proved its owner/runtime boundary")
    parallelism = value["parallelism"]
    if set(parallelism) != {"job_workers", "backup_workers", "restore_workers", "verify_workers"} or any(
        type(n) is not int or not 1 <= n <= 16 for n in parallelism.values()):
        raise ValueError("invalid receiver parallelism")
    for item in value["jobs"]:
        if set(item) - {"envelope_identity_sha256", "state", "failed_stage", "error_type", "dispatch_name"}:
            raise ValueError("pipeline status contains nonpublic job fields")
        if item["state"] not in {"accepted", "queued", "retry_wait", "rejected"}:
            raise ValueError("invalid receiver job state")
    observed = datetime.fromisoformat(value["observed_at_utc"])
    if observed.tzinfo is None:
        raise ValueError("parallel receiver status needs a timezone")
    age = (datetime.now(timezone.utc)-observed).total_seconds()
    if age < -300:
        raise ValueError("parallel receiver status comes from the future")
    return {"state": "parallel_receiver_active" if age <= configuration["readiness_max_age_seconds"] else "parallel_receiver_status_stale",
        "installed_package_version":installed,"available_package_version":next(reversed(available)),
        "upgrade_pending":installed != next(reversed(available)),
        "parallelism": parallelism, "observed_at_utc": value["observed_at_utc"],
        "last_cycle_seconds": value["complete_cycle_seconds"], "jobs": value["jobs"]}


def validate_configuration(c):
    fields = {"schema_version", *PAIRED, "ingress_root", "receipt_root", "state_root", "scratch_root",
        "owner_lock", "nas_configuration", "password_file", "restic", "runtime_lock",
        "minimum_free_bytes", "maximum_batch_bytes", "maximum_scratch_bytes", "maximum_jobs_per_cycle", "readiness_max_age_seconds",
        "job_workers", "backup_workers", "restore_workers", "verify_workers", "retry_delays_seconds",
        "windows_distribution", "powershell", "installed_handoff_identity_sha256"}
    if set(c) != fields or c["schema_version"] != 1:
        raise ValueError("parallel relay needs its exact local configuration")
    for key in ("repository_id", "installed_handoff_identity_sha256"):
        if not HASH.fullmatch(c[key]):
            raise ValueError("parallel relay needs full fixed identities")
    for key in ("minimum_free_bytes", "maximum_batch_bytes", "maximum_scratch_bytes", "maximum_jobs_per_cycle", "readiness_max_age_seconds",
                "job_workers", "backup_workers", "restore_workers", "verify_workers"):
        if type(c[key]) is not int or c[key] <= 0:
            raise ValueError("parallel relay budgets must be positive integers")
    if any(c[key] > 16 for key in ("maximum_jobs_per_cycle", "job_workers", "backup_workers", "restore_workers", "verify_workers")):
        raise ValueError("parallel relay exceeds its installed worker ceiling")
    if c["backup_workers"] > c["job_workers"] or c["restore_workers"] > c["job_workers"]:
        raise ValueError("stage workers cannot exceed the total admitted jobs")
    for key in ("ingress_root", "receipt_root", "state_root", "scratch_root", "owner_lock",
                "nas_configuration", "password_file", "restic", "runtime_lock", "powershell"):
        p = Path(c[key])
        if not p.is_absolute() or any(x.is_symlink() for x in (p, *p.parents)):
            raise ValueError("parallel relay path is relative or redirected")
    roots = [Path(c[k]) for k in ("ingress_root", "receipt_root", "state_root", "scratch_root")]
    if any(a.is_relative_to(b) or b.is_relative_to(a) for i, a in enumerate(roots) for b in roots[i+1:]):
        raise ValueError("parallel relay authorities must be separate")
    if not isinstance(c["retry_delays_seconds"], list) or not c["retry_delays_seconds"] or any(
        type(n) is not int or not 1 <= n <= 86400 for n in c["retry_delays_seconds"]):
        raise ValueError("parallel relay retry delays must be bounded")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", c["windows_distribution"] or "linux"):
        raise ValueError("invalid WSL distribution identity")


def remove_verified_restore(output, recovered, dispatch, verified):
    """Only this completed job's exact restore, with retained private journal."""
    if output.is_symlink() or recovered.is_symlink() or not recovered.is_relative_to(output):
        raise ValueError("restore cleanup must stay inside its private job scratch")
    prefix = recovered.relative_to(output)
    envelope = read_json(recovered / "backup-envelope.json")
    expected = {prefix / name for name in ("READY", "backup-envelope.json")}
    expected.update(prefix / row["relative"] for row in envelope["files"])
    expected_directories = {Path(".")}
    for path in expected:
        expected_directories.update(path.parents)
    observed, directories = {}, []
    for root, dirs, files in os.walk(output, followlinks=False):
        directory = Path(root)
        if directory.is_symlink() or directory.stat().st_dev != output.stat().st_dev:
            raise ValueError("restore cleanup rejects another mount or redirect")
        if directory.relative_to(output) not in expected_directories:
            raise ValueError("restore cleanup preserves unknown directories")
        directories.append(directory)
        for name in dirs:
            if (directory / name).is_symlink():
                raise ValueError("restore cleanup rejects symlink directories")
        for name in files:
            p = directory / name
            if p.is_symlink() or p.stat().st_nlink != 1:
                raise ValueError("restore cleanup rejects shared or redirected files")
            observed[p.relative_to(output)] = signature(p)
    if set(observed) != expected:
        raise ValueError("restore cleanup preserves unknown files")
    for relative, pinned in verified["file_signatures"].items():
        if list(observed[prefix / relative]) != pinned:
            raise ValueError("restored payload changed after its SHA verification")
    if process_references_many([output]):
        raise ValueError("restore scratch is still used by a process")
    for relative, pinned in observed.items():
        p = output / relative
        if signature(p) != pinned:
            raise ValueError("restore changed immediately before cleanup")
        p.unlink()
    for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
        directory.rmdir()


def run_cycle(c):
    validate_configuration(c)
    started = time.perf_counter()
    state_root, scratch = Path(c["state_root"]), Path(c["scratch_root"])
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_file(Path(c["password_file"]))
    private_file(Path(c["nas_configuration"]))
    if validate_runtime_lock(read_json(Path(c["runtime_lock"])), runtime_identity()):
        raise ValueError("parallel relay runtime differs from its accepted lock")
    lock = Path(c["owner_lock"])
    if not lock.is_file():
        raise ValueError("existing backup owner is absent")
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        nas = NasTarget(Path(c["nas_configuration"]))
        mount = nas.check()
        class GuardedRestic(ResticBackup):
            def run(self, arguments, *, timeout=None):
                nas.unchanged(mount)
                result = super().run(arguments, timeout=timeout or 3600)
                nas.unchanged(mount)
                return result

        factory = lambda key: GuardedRestic(Path(c["restic"]), str(nas.repository), Path(c["password_file"]),
            cache=state_root / "cache" / key)
        admission = factory("admission")
        repo = json.loads(admission.run(["cat", "config"]).stdout)
        nas.unchanged(mount)
        if repo["id"] != c["repository_id"]:
            raise ValueError("parallel relay repository differs from its paired identity")
        free = min(shutil.disk_usage(scratch).free, physical_free_bytes(c))
        readiness_proof = state_root / "readiness.private.json"
        private_json(readiness_proof, {"repository_id": repo["id"],
            "ingress_free_bytes": min(shutil.disk_usage(c["ingress_root"]).free, free),
            "nas_free_bytes": mount["free_bytes"], "reserve_bytes": c["minimum_free_bytes"],
            "maximum_batch_bytes": c["maximum_batch_bytes"],
            "repository_check_mode": "structural_plus_fixed_snapshot_full_restore",
            "single_owner_verified": True, "nas_mount_guard_verified": True, "runtime_lock_verified": True,
            "automatic_pruning": False, "automatic_batch_deletion": False})
        publish_readiness(c, readiness_proof, Path(c["receipt_root"]))
        jobs, status, reserved = [], [], 0
        candidates = []
        for path in sorted((Path(c["ingress_root"]) / "tools/dispatch").glob("*.json")):
            try:
                dispatch = read_json(path)
                validate_dispatch(dispatch)
                if any(dispatch[k] != c[k] for k in PAIRED) or path.stem != dispatch["envelope_identity_sha256"]:
                    raise ValueError("dispatch differs from this paired source")
                key = dispatch["envelope_identity_sha256"]
                journal = state_root / (key + ".json")
                item = read_json(journal) if journal.exists() else {"attempts": 0, "last_attempt_epoch": 0}
                ack = Path(c["receipt_root"]) / ("acceptance-" + key + ".json")
                if ack.exists():
                    validate_ack(read_json(ack), dispatch)
                    if item.get("state") == "accepted" and not item.get("success_scratch_removed") and item.get("output"):
                        # Resume only this exact, journal-owned verified restore;
                        # an ACK alone never authorizes deleting arbitrary scratch.
                        output = Path(item["output"])
                        if (output.parent == scratch and re.fullmatch(key + "-[0-9a-f]{32}", output.name)
                                and output.is_dir() and not output.is_symlink()
                                and item.get("snapshot_id") == read_json(ack)["snapshot_id"]):
                            try:
                                recovered = output / str(Path(c["ingress_root"]) / dispatch["batch_relative"]).lstrip("/")
                                verify_dispatch_delivery(recovered, dispatch, workers=c["verify_workers"])
                                from scripts.verify_backup_delivery import verify
                                verified = verify(recovered, key, workers=c["verify_workers"], include_file_signatures=True)
                                remove_verified_restore(output, recovered, dispatch, verified)
                                item["success_scratch_removed"] = True
                                item.pop("cleanup_error_type", None)
                            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                                item["cleanup_error_type"] = type(error).__name__
                            private_json(journal, item)
                    continue  # Accepted batches need not remain in ingress.
                if not (Path(c["ingress_root"]) / dispatch["batch_relative"] / "READY").is_file():
                    continue  # Syncthing may deliver dispatch/READY before all data.
                batch = Path(c["ingress_root"]) / dispatch["batch_relative"]
                envelope_file = batch / "backup-envelope.json"
                if not envelope_file.is_file():
                    continue
                envelope = read_json(envelope_file)
                if hashlib.sha256(envelope_file.read_bytes()).hexdigest() != dispatch["envelope_file_sha256"]:
                    raise ValueError("received envelope differs from the source dispatch")
                from scripts.verify_backup_delivery import regular
                try:
                    if any(regular(batch, row["relative"]).stat().st_size != row["bytes"] for row in envelope["files"]):
                        continue
                except FileNotFoundError:
                    continue  # Receiving files are not failed NAS attempts.
                candidates.append((item.get("last_attempt_epoch", 0), key, dispatch, item, journal))
            except (OSError, ValueError, KeyError, TypeError) as error:
                status.append({"dispatch_name": path.name, "state": "rejected", "error_type": type(error).__name__})
        retained_scratch = 0
        for p in scratch.rglob("*"):
            if p.is_symlink() or p.stat().st_dev != scratch.stat().st_dev or not (p.is_file() or p.is_dir()):
                raise ValueError("retained scratch contains a redirect, another mount or special file")
            if p.is_file():
                retained_scratch += p.stat().st_size
        for _, key, dispatch, item, journal in sorted(candidates):
            if item.get("next_attempt_epoch", 0) > time.time():
                status.append({"envelope_identity_sha256": key, "state": "retry_wait"})
                continue
            if dispatch["complete_bytes"] > c["maximum_batch_bytes"]:
                status.append({"envelope_identity_sha256": key, "state": "rejected", "error_type": "CapacityError"})
                continue
            old_output = Path(item.get("output", "/nonexistent"))
            reuse_restore = (item.get("restore_verified_snapshot_id") == item.get("snapshot_id")
                and item.get("snapshot_id") is not None and old_output.parent == scratch and old_output.is_dir())
            required_scratch = 0 if reuse_restore else dispatch["complete_bytes"]
            if (len(jobs) >= c["maximum_jobs_per_cycle"] or reserved + required_scratch > min(
                    free - c["minimum_free_bytes"], c["maximum_scratch_bytes"] - retained_scratch)):
                status.append({"envelope_identity_sha256": key, "state": "queued"})
                continue
            reserved += required_scratch
            jobs.append((key, dispatch, item, journal))
        backup_slots = threading.BoundedSemaphore(c["backup_workers"])
        restore_slots = threading.BoundedSemaphore(c["restore_workers"])

        def execute(job):
            key, dispatch, item, journal = job
            stage = "input_verification"
            item.update(attempts=item["attempts"] + 1, last_attempt_epoch=time.time(), state=stage)
            private_json(journal, item)
            attempt_started = time.perf_counter()
            backend = factory(key)
            batch = Path(c["ingress_root"]) / dispatch["batch_relative"]
            command_exits = item.setdefault("successful_command_exit_codes", {})
            try:
                verify_dispatch_delivery(batch, dispatch, workers=c["verify_workers"])
                if item.get("snapshot_id"):
                    if not HASH.fullmatch(item["snapshot_id"]) or item.get("dispatch_identity_sha256") != dispatch["identity_sha256"]:
                        raise ValueError("resume snapshot differs from its closed dispatch")
                else:
                    stage = "backup"
                    item.update(state=stage, dispatch_identity_sha256=dispatch["identity_sha256"])
                    private_json(journal, item)
                    with backup_slots:
                        nas.unchanged(mount)
                        if item["attempts"] > 1:
                            # Recover a snapshot completed just before SIGKILL
                            # prevented its journal commit. Every candidate is
                            # still subjected to a fresh full restore below.
                            previous = json.loads(backend.run(["snapshots", "--json", "--tag", key,
                                "--tag", dispatch["identity_sha256"]]).stdout)
                            command_exits["snapshot_query"] = 0
                            previous = [p for p in previous if key in p.get("tags", [])
                                and dispatch["identity_sha256"] in p.get("tags", []) and HASH.fullmatch(p.get("id", ""))
                                and p["id"] not in item.get("rejected_snapshot_ids", [])]
                            if previous:
                                item["snapshot_id"] = min(previous, key=lambda p:p["time"])["id"]
                        if item.get("snapshot_id"):
                            item["state"] = "snapshot_written"
                            private_json(journal, item)
                        else:
                            result = backend.run(["backup", "--json", "--tag", key,
                                "--tag", dispatch["identity_sha256"], str(batch)])
                            command_exits["backup"] = result.returncode
                            summaries = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
                            summaries = [value for value in summaries if value.get("message_type") == "summary"]
                            if len(summaries) != 1 or not HASH.fullmatch(summaries[0].get("snapshot_id", "")):
                                raise ValueError("backup lacks a fixed complete snapshot")
                            item.update(snapshot_id=summaries[0]["snapshot_id"], state="snapshot_written")
                            private_json(journal, item)
                        nas.unchanged(mount)
                stage = "restore"
                previous_output = Path(item.get("output", "/nonexistent"))
                reuse_restore = (item.get("restore_verified_snapshot_id") == item["snapshot_id"]
                    and previous_output.parent == scratch and previous_output.is_dir()
                    and not previous_output.is_symlink()
                    and re.fullmatch(key + "-[0-9a-f]{32}", previous_output.name))
                output = previous_output if reuse_restore else scratch / (key + "-" + uuid.uuid4().hex)
                item.update(output=str(output), state=stage)
                private_json(journal, item)  # Durable creation intent precedes the restore.
                if not reuse_restore:
                    with restore_slots:
                        nas.unchanged(mount)
                        backend.restore(item["snapshot_id"], output)
                        command_exits["restore"] = 0
                        nas.unchanged(mount)
                stage = "restored_sha256"
                recovered = output / str(batch).lstrip("/")
                verify_dispatch_delivery(recovered, dispatch, workers=c["verify_workers"])
                from scripts.verify_backup_delivery import verify
                verified = verify(recovered, key, workers=c["verify_workers"], include_file_signatures=True)
                item.update(state="restore_verified", next_attempt_epoch=0,
                    restore_verified_snapshot_id=item["snapshot_id"],
                    current_attempt_seconds=time.perf_counter() - attempt_started)
                private_json(journal, item)
                return {"key": key, "dispatch": dispatch, "item": item, "journal": journal,
                    "output": output, "recovered": recovered, "verified": verified,
                    "started": attempt_started}
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
                delays = c["retry_delays_seconds"]
                if stage == "restored_sha256" and item.get("snapshot_id"):
                    # A completed Restic command is not a complete source
                    # delivery. Do not permanently retry a partial tagged
                    # snapshot left by a killed/incomplete previous backup.
                    item.setdefault("rejected_snapshot_ids", []).append(item.pop("snapshot_id"))
                    item.pop("restore_verified_snapshot_id", None)
                    if item.get("output"):
                        item.setdefault("retained_failed_outputs", []).append(item.pop("output"))
                item.update(state="retry_wait", failed_stage=stage, error_type=type(error).__name__, private_error=str(error)[:2000],
                    next_attempt_epoch=time.time() + delays[min(item["attempts"] - 1, len(delays) - 1)])
                private_json(journal, item)
                return {"key": key, "error_type": type(error).__name__, "stage": stage}

        completed = []
        with ThreadPoolExecutor(max_workers=c["job_workers"], thread_name_prefix="nas-delivery") as pool:
            futures = [pool.submit(execute, job) for job in jobs]
            for future in as_completed(futures):
                result = future.result()
                if "error_type" in result:
                    status.append({"envelope_identity_sha256": result["key"], "state": "retry_wait",
                        "failed_stage": result["stage"], "error_type": result["error_type"]})
                else:
                    completed.append(result)
        # `restic check` requires an exclusive repository lock. One post-write
        # barrier checks the whole completed cohort, without disabling locks.
        if completed:
            nas.unchanged(mount)
            try:
                checked = admission.run(["check"], timeout=3600)
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
                for result in completed:
                    item = result["item"]
                    delays = c["retry_delays_seconds"]
                    item.update(state="retry_wait", failed_stage="repository_check", error_type=type(error).__name__,
                        private_error=str(error)[:2000],
                        next_attempt_epoch=time.time() + delays[min(item["attempts"] - 1, len(delays) - 1)])
                    private_json(result["journal"], item)
                    status.append({"envelope_identity_sha256": result["key"], "state": "retry_wait",
                        "failed_stage": "repository_check", "error_type": type(error).__name__})
                completed = []
            nas.unchanged(mount)
            if completed:
                check_receipt = state_root / ("check-" + uuid.uuid4().hex + ".json")
                private_json(check_receipt, {"exit_code": checked.returncode,
                    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                    "completed_snapshot_ids": [r["item"]["snapshot_id"] for r in completed]})
            for result in completed:
                dispatch, item = result["dispatch"], result["item"]
                proof = {"repository_id": c["repository_id"], "snapshot_id": item["snapshot_id"],
                    **{k: dispatch[k] for k in ("envelope_identity_sha256", "envelope_file_sha256", "complete_files", "complete_bytes")},
                    "repository_check_mode": "structural_plus_fixed_snapshot_full_restore",
                    "repository_check_verified": True, "independent_restore_verified": True,
                    "source_verifier_verified": True, "all_files_sha256_verified": True,
                    "command_exit_codes": [*item["successful_command_exit_codes"].values(), checked.returncode],
                    "complete_workflow_seconds": time.perf_counter()-result["started"],
                    "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
                    "repository_check_receipt": str(check_receipt),
                    "repository_check_receipt_sha256": hashlib.sha256(check_receipt.read_bytes()).hexdigest()}
                proof_path = state_root / (result["key"] + ".proof.json")
                private_json(proof_path, proof)
                try:
                    publish(Path(c["ingress_root"]) / dispatch["batch_relative"], dispatch, proof_path,
                        Path(c["receipt_root"]), workers=c["verify_workers"])
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    delays = c["retry_delays_seconds"]
                    item.update(state="retry_wait", failed_stage="receipt_publication", error_type=type(error).__name__,
                        private_error=str(error)[:2000],
                        next_attempt_epoch=time.time() + delays[min(item["attempts"] - 1, len(delays) - 1)])
                    private_json(result["journal"], item)
                    status.append({"envelope_identity_sha256": result["key"], "state": "retry_wait",
                        "failed_stage": "receipt_publication", "error_type": type(error).__name__})
                    continue
                item.update(state="accepted", next_attempt_epoch=0)
                private_json(result["journal"], item)
                try:
                    remove_verified_restore(result["output"], result["recovered"], dispatch, result["verified"])
                    item["success_scratch_removed"] = True
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    item["cleanup_error_type"] = type(error).__name__
                private_json(result["journal"], item)
                status.append({"envelope_identity_sha256": result["key"], "state": "accepted"})
        result = signed({"contract": STATUS, **{k: c[k] for k in PAIRED},
            "installed_handoff_identity_sha256": c["installed_handoff_identity_sha256"],
            "parallelism": {k:c[k] for k in ("job_workers", "backup_workers", "restore_workers", "verify_workers")},
            "jobs": status, "runtime_lock_verified": True, "single_controller_owner_verified": True,
            "full_history_backup_verified": False, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "complete_cycle_seconds": time.perf_counter()-started})
        atomic_public(Path(c["receipt_root"]) / "pipeline-status.json", result, replace=True)
        return result
