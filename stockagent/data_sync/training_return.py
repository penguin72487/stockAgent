"""Durable completed-run return proofs and narrowly scoped compute-node cleanup.

The existing penguin ingress owns transfers/publication. A private SSH control
call carries this fixed code plus an exact content acknowledgement to the
compute node. Synced files are never commands. Source inputs and caches are
outside this completed-artifact cleanup contract.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import time
import uuid
from datetime import datetime

from stockagent.data_sync.artifact_maintenance import artifact_process_references, automatic_dataset_name
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes
from stockagent.data_sync.cold_artifacts import ColdArtifactSpec, validate_cold_artifact_source
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, atomic_write_json
from stockagent.data_sync.packed_snapshots import _load_inventory, verify_packed_snapshot
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = "durable_completed_training_return_v2"
ACK_TTL_SECONDS = 1800
ACK_FUTURE_SKEW_SECONDS = 60
POLICY_FIELDS_V2 = {"schema_version", "authority_node_id", "origin_node_id", "scopes", "stable_hours",
                 "maximum_run_bytes", "maximum_run_files", "reserve_bytes", "retire_verified_source",
                 "shared_file_policy"}
POLICY_FIELDS = POLICY_FIELDS_V2 | {"minimum_hot_retention_hours"}
MINIMUM_HOT_RETENTION_HOURS = 7 * 24


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def load_policy(value: dict | Path) -> dict:
    c = json.loads(value.read_text()) if isinstance(value, Path) else dict(value)
    if c.get("schema_version") == 2 and set(c) == POLICY_FIELDS_V2:
        # Existing owners may still carry a v2 policy in memory. Their next
        # authenticated control call must enforce the new retention floor too.
        c = {**c, "schema_version": 3, "minimum_hot_retention_hours": MINIMUM_HOT_RETENTION_HOURS}
    if set(c) != POLICY_FIELDS or c["schema_version"] != 3:
        raise SnapshotError("unsupported completed training return policy")
    if c["authority_node_id"] != "penguin" or c["origin_node_id"] != "vastai1T":
        raise SnapshotError("completed training return needs the enrolled two roles")
    if not isinstance(c["scopes"], list) or not c["scopes"] or len(set(c["scopes"])) != len(c["scopes"]):
        raise SnapshotError("completed training scopes must be a nonempty unique list")
    if any(s not in {"markets", "ablations"} for s in c["scopes"]):
        raise SnapshotError("only canonical markets/ablations completed runs are enrolled")
    for key in ("maximum_run_bytes", "maximum_run_files", "reserve_bytes"):
        if type(c[key]) is not int or c[key] < 1:
            raise SnapshotError("completed training return capacity must be positive")
    if type(c["stable_hours"]) not in (float, int) or not 0 <= c["stable_hours"] <= 168:
        raise SnapshotError("completed training stability must be bounded")
    if type(c["retire_verified_source"]) is not bool:
        raise SnapshotError("completed training source retirement must be explicit")
    if c["shared_file_policy"] != "unlink_returned_names_only":
        raise SnapshotError("shared returned files must preserve every external inode name")
    hours = c["minimum_hot_retention_hours"]
    if (type(hours) not in (int, float) or not math.isfinite(hours)
            or not MINIMUM_HOT_RETENTION_HOURS <= hours <= 24 * 365):
        raise SnapshotError("formal training hot retention must be at least seven days")
    return c


def training_hot_retention(source: Path, rows: list[dict], *,
                           minimum_hours: float = MINIMUM_HOT_RETENTION_HOURS,
                           required: bool = False) -> dict:
    """Retain formal results after completion AND the last source write.

    Publication remains independent. Legacy/bulk parents containing a recent
    lifecycle inherit this same floor; ordinary panel/cache trees do not.
    Static evidence enters the plan fingerprint, never a changing age value.
    """
    if (type(minimum_hours) not in (int, float) or not math.isfinite(minimum_hours)
            or minimum_hours < MINIMUM_HOT_RETENTION_HOURS):
        raise SnapshotError("formal training hot retention cannot be bypassed")
    files = {row["path"]: row for row in rows if row["kind"] == "file"}
    markers = {"run_manifest.json", "progress.json", "epoch_curve.jsonl",
               "checkpoint_best.pt", "checkpoint_last.pt"}
    applies = required or any(PurePosixPath(name).name in markers for name in files)
    if not applies:
        return {"applies": False, "minimum_hours": minimum_hours, "blockers": []}
    if not files:
        raise SnapshotError("formal training retention has no source evidence")
    newest = max(int(row["signature"][3]) for row in files.values())
    completions = []
    for name in sorted(files):
        if PurePosixPath(name).name != "run_manifest.json":
            continue
        path = source / str(PurePosixPath(name).parent) / "progress.json"
        try:
            progress = json.loads(path.read_text())
            if progress.get("state") != "complete":
                if required:
                    raise SnapshotError("formal training retention requires completed lifecycle")
                continue
            stamp = datetime.fromisoformat(progress["updated_at"].replace("Z", "+00:00"))
            if stamp.tzinfo is None or not math.isfinite(stamp.timestamp()):
                raise ValueError("completion timestamp lacks timezone")
            completions.append({"progress_relative": path.relative_to(source).as_posix(),
                                "completed_at": stamp.isoformat(), "epoch": stamp.timestamp()})
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            raise SnapshotError("formal training retention evidence is unreadable") from error
    if required and not completions:
        raise SnapshotError("formal training retention lacks canonical completion timestamp")
    anchor = max([newest / 1_000_000_000, *(r["epoch"] for r in completions)])
    until = anchor + minimum_hours * 3600
    return {"applies": True, "minimum_hours": minimum_hours,
            "newest_source_mtime_ns": newest, "completions": completions,
            "retain_until_epoch": until,
            "blockers": ["training-hot-retention-not-expired"] if time.time() < until else []}


def recovered_source_references(source: Path, repo_root: Path) -> list[str]:
    # Lazy import avoids the existing shared-inode helper's import cycle.
    from stockagent.data_sync.remote_legacy_return import recovery_hold_references
    return recovery_hold_references(source, repo_root)


def admitted(relative: str, policy: dict) -> bool:
    path = _safe_relative_path(relative, "completed training root")
    return (len(path.parts) >= 2 and path.parts[0] in policy["scopes"]
            and all(not part.startswith(".") for part in path.parts))


def admit_workspace(path: Path, required_bytes: int) -> dict:
    """Mutable ingress/DB state uses a native local filesystem and real space."""
    path = _real(path)
    if not path.is_dir():
        raise SnapshotError("training return workspace must already exist")
    result = subprocess.run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(path)],
                            capture_output=True, text=True, check=True, timeout=10)
    filesystem = result.stdout.strip()
    if filesystem not in {"ext4", "xfs", "btrfs", "zfs"}:
        raise SnapshotError("authority mutable return state needs a native local filesystem")
    fs = os.statvfs(path)
    free = fs.f_bavail * fs.f_frsize
    if "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower():
        from stockagent.data_sync.recovery_queue import physical_free_bytes
        config = {"windows_distribution": os.environ.get("WSL_DISTRO_NAME", ""),
                  "powershell": "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"}
        free = min(free, physical_free_bytes(config))
    if free < required_bytes:
        raise SnapshotError("training return workspace lacks real physical free space")
    return {"filesystem": filesystem, "physical_free_bytes": free, "required_bytes": required_bytes}


def make_ack(sync_root: Path, resolved, *, relative_root: str, origin: str) -> dict:
    """Prove every decoded packed member before remote deletion is possible."""
    if resolved.manifest.get("metadata", {}).get("artifact_relative_root") != relative_root:
        raise SnapshotError("returned release belongs to a different artifact root")
    entries = _load_inventory(sync_root, resolved.manifest)
    if any(row["kind"] not in {"file", "directory"} for row in entries):
        raise SnapshotError("automatic returned-source retirement does not accept links")
    files = [row["path"] for row in entries if row["kind"] == "file"]
    proof = verify_packed_snapshot(sync_root, resolved, reconstruct_paths=files)
    if proof.get("independently_reconstructed_files") != len(files):
        raise SnapshotError("not every returned file was independently reconstructed")
    inventory = [{k: row[k] for k in (("path", "kind", "mode", "size", "sha256")
                                      if row["kind"] == "file" else ("path", "kind", "mode"))}
                 for row in entries]
    body = {"contract": CONTRACT, "authority_node_id": "penguin", "origin_node_id": origin,
            "relative_root": relative_root, "dataset": resolved.manifest["dataset"],
            "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256,
            "inventory": inventory, "independently_reconstructed_files": len(files),
            "logical_bytes": sum(r["size"] for r in inventory if r["kind"] == "file"),
            "recorded_at_epoch": time.time(), "cold_reconstruction_verified": True}
    return {**body, "identity_sha256": identity(body)}


def validate_ack(ack: dict, policy: dict) -> None:
    body = {k: v for k, v in ack.items() if k != "identity_sha256"}
    expected = {"contract", "authority_node_id", "origin_node_id", "relative_root", "dataset", "snapshot_id",
                "manifest_sha256", "inventory", "independently_reconstructed_files", "logical_bytes",
                "recorded_at_epoch", "cold_reconstruction_verified", "identity_sha256"}
    if (set(ack) != expected or ack["contract"] != CONTRACT or identity(body) != ack["identity_sha256"]
            or ack["cold_reconstruction_verified"] is not True
            or any(ack[k] != policy[k] for k in ("authority_node_id", "origin_node_id"))
            or not admitted(ack["relative_root"], policy)
            or ack["dataset"] != automatic_dataset_name(ack["relative_root"])
            or not re.fullmatch(r"[0-9a-f]{64}", ack["manifest_sha256"])):
        raise SnapshotError("invalid exact durable training return acknowledgement")
    stamp = ack["recorded_at_epoch"]
    if (type(stamp) not in (int, float) or not math.isfinite(stamp)
            or not -ACK_FUTURE_SKEW_SECONDS <= time.time() - stamp <= ACK_TTL_SECONDS):
        raise SnapshotError("durable training return acknowledgement is expired or not fresh")
    if not isinstance(ack["inventory"], list):
        raise SnapshotError("invalid returned file inventory")
    paths = set()
    files = total = 0
    for row in ack["inventory"]:
        path = _safe_relative_path(row["path"], "returned member").as_posix()
        if path in paths:
            raise SnapshotError("duplicate returned file path")
        paths.add(path)
        if row.get("kind") == "file":
            if (set(row) != {"path", "kind", "mode", "size", "sha256"} or type(row["size"]) is not int
                    or row["size"] < 0 or not re.fullmatch(r"[0-9a-f]{64}", row["sha256"])):
                raise SnapshotError("invalid returned file descriptor")
            files += 1
            total += row["size"]
        elif row.get("kind") != "directory" or set(row) != {"path", "kind", "mode"}:
            raise SnapshotError("returned file inventory has an unsupported kind")
        if type(row["mode"]) is not int or not 0 <= row["mode"] <= 0o7777:
            raise SnapshotError("returned mode is invalid")
    if (not 1 <= files <= policy["maximum_run_files"] or total > policy["maximum_run_bytes"]
            or files != ack["independently_reconstructed_files"] or total != ack["logical_bytes"]):
        raise SnapshotError("returned inventory exceeds policy or differs from recovery proof")


def _signature(info) -> tuple:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_nlink


def _real(path: Path) -> Path:
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("returned artifact path is redirected")
    return path


def inventory(root: Path) -> dict:
    """Hash exact names; reject redirects/mounts and count only freed inodes."""
    root = _real(root)
    device = root.stat().st_dev
    rows = []
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in sorted(dirs + names):
            path = Path(directory) / name
            before = path.lstat()
            relative = path.relative_to(root).as_posix()
            if before.st_dev != device or path.is_symlink():
                raise SnapshotError("returned tree contains a link or another filesystem")
            if stat.S_ISDIR(before.st_mode):
                row = {"path": relative, "kind": "directory", "mode": stat.S_IMODE(before.st_mode)}
            elif stat.S_ISREG(before.st_mode) and before.st_nlink >= 1:
                digest = hashlib.sha256()
                with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
                    if _signature(os.fstat(stream.fileno())) != _signature(before):
                        raise SnapshotError("returned source changed before hashing")
                    for chunk in iter(lambda: stream.read(8*1024**2), b""):
                        digest.update(chunk)
                    if _signature(os.fstat(stream.fileno())) != _signature(before):
                        raise SnapshotError("returned source changed while hashing")
                row = {"path": relative, "kind": "file", "mode": stat.S_IMODE(before.st_mode),
                       "size": before.st_size, "sha256": digest.hexdigest()}
            else:
                raise SnapshotError("returned tree contains unsupported files")
            if _signature(path.lstat()) != _signature(before):
                raise SnapshotError("returned source changed during capture")
            rows.append({**row, "signature": list(_signature(before))})
    return {"rows": sorted(rows, key=lambda r: r["path"]), "allocated_bytes": _reclaimable_file_bytes(root),
            "shared_file_names": sum(r["kind"] == "file" and r["signature"][6] > 1 for r in rows)}


def shared_inode_references(observed: dict) -> list[str]:
    keys = {(r["signature"][0], r["signature"][1]) for r in observed["rows"]
            if r["kind"] == "file" and r["signature"][6] > 1}
    return process_references_many((), inode_keys=keys)


def _matches(observed: dict, ack: dict) -> None:
    rows = [{k: v for k, v in row.items() if k != "signature"} for row in observed["rows"]]
    if rows != sorted(ack["inventory"], key=lambda r: r["path"]):
        raise SnapshotError("remote source differs from the independently recovered return")


def plan_retirement(artifact_root: Path, state_root: Path, repo_root: Path, ack: dict, policy: dict) -> dict:
    policy = load_policy(policy)
    validate_ack(ack, policy)
    artifact_root, state_root = _real(artifact_root), _real(state_root)
    source = _real(artifact_root / ack["relative_root"])
    if (not source.is_dir() or state_root.is_relative_to(artifact_root)
            or artifact_root.is_relative_to(state_root) or source.stat().st_dev != state_root.stat().st_dev):
        raise SnapshotError("returned source and private quarantine must be independent on one filesystem")
    spec = ColdArtifactSpec(ack["dataset"], ack["relative_root"], None, 8*1024**2, 32, policy["stable_hours"],
                            "training-lifecycle-v1")
    validate_cold_artifact_source(artifact_root, spec)
    refs = artifact_process_references(source, artifact_root / PurePosixPath(ack["relative_root"]).parts[0])
    dependency_error = None
    try:
        service_refs = artifact_service_references([source], repo_root).get(str(source), [])
    except (SyntaxError, ImportError, SnapshotError, OSError, ValueError) as error:
        # A broken/conflicted service parser is unknown dependency evidence,
        # never an empty consumer list. Keep exact returned source protected.
        service_refs = []
        dependency_error = {"error_type": type(error).__name__}
        if isinstance(error, SyntaxError):
            dependency_error.update(file_name=Path(error.filename or "unknown").name, line=error.lineno)
    observed = inventory(source)
    _matches(observed, ack)
    retention = training_hot_retention(source, observed["rows"],
                                      minimum_hours=policy["minimum_hot_retention_hours"], required=True)
    recovery_refs = recovered_source_references(source, repo_root)
    inode_refs = shared_inode_references(observed)
    blockers = (["retirement-disabled"] if not policy["retire_verified_source"] else [])
    blockers += retention["blockers"]
    if recovery_refs:
        blockers.append("source-recovery-hold")
    if refs:
        blockers.append("source-in-use")
    if service_refs:
        blockers.append("source-service-dependency")
    if dependency_error:
        blockers.append("source-service-dependencies-unverifiable")
    if inode_refs:
        blockers.append("source-shared-inode-in-use")
    body = {"contract": "exact_returned_source_retirement_plan_v2", "ack_identity_sha256": ack["identity_sha256"],
            "policy_identity_sha256": identity(policy), "relative_root": ack["relative_root"],
            "source": str(source), "rows": observed["rows"], "allocated_bytes": observed["allocated_bytes"],
            "process_references": refs, "service_references": service_refs,
            "service_dependency_error": dependency_error, "blockers": blockers,
            "hot_retention": retention, "recovery_references": recovery_refs}
    body.update(shared_file_names=observed["shared_file_names"], shared_inode_references=inode_refs)
    return {**body, "plan_fingerprint": identity(body)}


def apply_retirement(artifact_root: Path, state_root: Path, repo_root: Path, ack: dict, policy: dict,
                     expected_fingerprint: str) -> dict:
    policy = load_policy(policy)
    validate_ack(ack, policy)
    state_root = _real(state_root)
    lock_path = state_root / (ack["dataset"] + ".lock")
    _real(lock_path)
    with lock_path.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = plan_retirement(artifact_root, state_root, repo_root, ack, policy)
        if plan["plan_fingerprint"] != expected_fingerprint or plan["blockers"]:
            raise SnapshotError("returned source retirement changed or remains blocked")
        transaction = uuid.uuid4().hex
        quarantine = state_root / "quarantine" / transaction
        _real(quarantine.parent).mkdir(mode=0o700, parents=True, exist_ok=True)
        journal = state_root / f"retirement-{transaction}.json"
        private = {"state": "prepared", "plan": plan, "ack": ack, "quarantine": str(quarantine)}
        atomic_write_json(journal, private)
        source = Path(plan["source"])
        source.rename(quarantine)
        private["state"] = "quarantined"
        atomic_write_json(journal, private)
        observed = inventory(quarantine)
        _matches(observed, ack)
        if observed["rows"] != plan["rows"]:
            raise SnapshotError("quarantined returned source changed; retained for inspection")
        if artifact_process_references(source, artifact_root) or artifact_process_references(quarantine, state_root):
            raise SnapshotError("returned source became active; retained quarantine")
        if shared_inode_references(observed):
            raise SnapshotError("shared returned inode became active; retained quarantine")
        if artifact_service_references([source], repo_root).get(str(source)):
            raise SnapshotError("returned source acquired a service dependency; retained quarantine")
        if (recovered_source_references(source, repo_root)
                or training_hot_retention(quarantine, observed["rows"],
                    minimum_hours=policy["minimum_hot_retention_hours"], required=True)["blockers"]):
            raise SnapshotError("returned source retention/hold changed; retained quarantine")
        validate_ack(ack, policy)
        inode_states = {}
        for row in observed["rows"]:
            path = quarantine / row["path"]
            if row["kind"] == "file":
                key = tuple(row["signature"][:2])
                expected = inode_states.get(key, row["signature"])
                if list(_signature(path.lstat())) != expected:
                    raise SnapshotError("returned file changed before unlink; retained quarantine")
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    if list(_signature(os.fstat(descriptor))) != expected:
                        raise SnapshotError("returned file changed before unlink; retained quarantine")
                    path.unlink()
                    after = list(_signature(os.fstat(descriptor)))
                    if (after[:4] != expected[:4] or after[5] != expected[5]
                            or after[6] != expected[6]-1):
                        raise SnapshotError("returned inode changed while unlinking; retained quarantine")
                    inode_states[key] = after
                finally:
                    os.close(descriptor)
        for row in sorted(observed["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
            if row["kind"] == "directory":
                (quarantine / row["path"]).rmdir()
        quarantine.rmdir()
        result = {"state": "verified_returned_source_retired", "deleted": True,
                  "relative_root": ack["relative_root"], "snapshot_id": ack["snapshot_id"],
                  "manifest_sha256": ack["manifest_sha256"], "ack_identity_sha256": ack["identity_sha256"],
                  "plan_fingerprint": plan["plan_fingerprint"], "reclaimed_allocated_bytes": plan["allocated_bytes"],
                  "source_inputs_deleted": False, "canonical_cold_deleted": False}
        result.update(shared_file_names=plan["shared_file_names"], external_shared_names_deleted=False)
        atomic_write_json(journal, {**private, "state": "retired", "result": result})
        return result
