#!/usr/bin/env python3
"""Public, credential-free NAS acknowledgements for a paired backup relay.

The existing lab203 worker owns backup/restore and its owner lock. Call publish
only after those operations finish, using their private acceptance proof. This
module neither backs up files nor turns a service exit code into recovery proof.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile

try:
    from scripts.verify_backup_delivery import identity, regular, verify, is_redirected
except ModuleNotFoundError:  # Files delivered together to the WSL backup role.
    from verify_backup_delivery import identity, regular, verify, is_redirected

HASH = re.compile(r"[0-9a-f]{64}\Z")
DISPATCH = "nas_backup_dispatch_v1"
ACK = "nas_backup_delivery_acceptance_v1"
READY = "nas_backup_relay_readiness_v1"
CHECK_MODES = {"full_read_data", "structural_plus_fixed_snapshot_full_restore"}
ACK_FIELDS = {
    "contract", "producer_device_id", "receiver_device_id", "envelope_identity_sha256",
    "envelope_file_sha256", "dispatch_identity_sha256", "repository_id", "snapshot_id",
    "complete_files", "complete_bytes", "repository_check_mode", "repository_check_verified",
    "independent_restore_verified", "source_verifier_verified", "all_files_sha256_verified",
    "command_exit_codes", "complete_workflow_seconds", "accepted_at_utc",
    "private_evidence_sha256", "scope", "canonical_reconstruction_verified",
    "control_database_restore_verified", "independent_key_custody_verified", "identity_sha256",
}
READY_FIELDS = {
    "contract", "producer_device_id", "receiver_device_id", "repository_id",
    "ingress_free_bytes", "nas_free_bytes", "reserve_bytes", "maximum_batch_bytes",
    "repository_check_mode", "single_owner_verified", "nas_mount_guard_verified",
    "runtime_lock_verified", "automatic_pruning", "automatic_batch_deletion",
    "observed_at_utc", "identity_sha256",
}


def read_json(path: Path) -> dict:
    if any(is_redirected(p) for p in (path, *path.parents)) or not path.is_file():
        raise ValueError("receipt path is redirected or not a file")
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("public receipt exceeds its size limit")
    result = json.loads(path.read_bytes())
    if not isinstance(result, dict):
        raise ValueError("receipt must be a JSON object")
    return result


def signed(body: dict) -> dict:
    return {**body, "identity_sha256": identity(body)}


def valid_signature(value: dict, contract: str, *, fields: set | None = None) -> None:
    if (value.get("contract") != contract
            or not isinstance(value.get("identity_sha256"), str)
            or not HASH.fullmatch(value["identity_sha256"])
            or identity({k: v for k, v in value.items() if k != "identity_sha256"}) != value["identity_sha256"]
            or (fields is not None and set(value) != fields)):
        raise ValueError("public receipt contract, exact field set or identity differs")


def validate_dispatch(value: dict) -> None:
    valid_signature(value, DISPATCH)
    for name in ("envelope_identity_sha256", "envelope_file_sha256", "repository_id"):
        if not isinstance(value.get(name), str) or not HASH.fullmatch(value[name]):
            raise ValueError("dispatch needs pinned full hashes")
    if value.get("batch_relative") != "batch-" + value["envelope_identity_sha256"]:
        raise ValueError("dispatch batch path differs from its immutable identity")
    for name in ("complete_files", "complete_bytes"):
        if type(value.get(name)) is not int or value[name] < 2:
            raise ValueError("dispatch needs positive exact file/byte counts")
    if not value.get("producer_device_id") or not value.get("receiver_device_id"):
        raise ValueError("dispatch needs both paired device identities")


def validate_ack(value: dict, dispatch: dict) -> None:
    validate_dispatch(dispatch)
    valid_signature(value, ACK, fields=ACK_FIELDS)
    for field in ("producer_device_id", "receiver_device_id", "envelope_identity_sha256",
                  "envelope_file_sha256", "repository_id", "complete_files", "complete_bytes"):
        if type(value.get(field)) is not type(dispatch[field]) or value[field] != dispatch[field]:
            raise ValueError("NAS acknowledgement differs from the source dispatch")
    if (value["dispatch_identity_sha256"] != dispatch["identity_sha256"]
            or not isinstance(value["snapshot_id"], str) or not HASH.fullmatch(value["snapshot_id"])
            or not isinstance(value["private_evidence_sha256"], str)
            or not HASH.fullmatch(value["private_evidence_sha256"])
            or value["repository_check_mode"] not in CHECK_MODES):
        raise ValueError("NAS acknowledgement lacks fixed repository/snapshot/evidence identity")
    for field in ("repository_check_verified", "independent_restore_verified",
                  "source_verifier_verified", "all_files_sha256_verified"):
        if value[field] is not True:
            raise ValueError("NAS acknowledgement has an incomplete acceptance step")
    exits = value["command_exit_codes"]
    if not isinstance(exits, list) or not exits or any(type(x) is not int or x != 0 for x in exits):
        raise ValueError("all acceptance commands must exit zero; 75 is not success")
    seconds = value["complete_workflow_seconds"]
    if type(seconds) not in {int, float} or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("NAS acknowledgement needs complete measured workflow time")
    if (value["scope"] != "this_closed_delivery_off_host_file_backup_only"
            or any(value[x] is not False for x in ("canonical_reconstruction_verified",
                "control_database_restore_verified", "independent_key_custody_verified"))):
        raise ValueError("file transport acknowledgements cannot promote other recovery proofs")
    accepted = datetime.fromisoformat(value["accepted_at_utc"])
    if accepted.tzinfo is None or (accepted - datetime.now(timezone.utc)).total_seconds() > 300:
        raise ValueError("NAS acceptance needs a plausible timezone-aware timestamp")


def validate_readiness(value: dict, configuration: dict, *, now: datetime | None = None) -> None:
    valid_signature(value, READY, fields=READY_FIELDS)
    for field in ("producer_device_id", "receiver_device_id", "repository_id"):
        if value[field] != configuration[field]:
            raise ValueError("relay readiness has another paired identity/repository")
    for field in ("single_owner_verified", "nas_mount_guard_verified", "runtime_lock_verified"):
        if value[field] is not True:
            raise ValueError("relay readiness has not verified its owner, mount and runtime")
    for field in ("automatic_pruning", "automatic_batch_deletion"):
        if value[field] is not False:
            raise ValueError("this rollout preserves pruning and automatic deletion disabled")
    for field in ("ingress_free_bytes", "nas_free_bytes", "reserve_bytes", "maximum_batch_bytes"):
        if type(value[field]) is not int or value[field] < 0:
            raise ValueError("invalid relay capacity")
    if value["repository_check_mode"] not in CHECK_MODES:
        raise ValueError("unknown repository acceptance mode")
    observed = datetime.fromisoformat(value["observed_at_utc"])
    if observed.tzinfo is None:
        raise ValueError("relay readiness needs a timezone")
    age = ((now or datetime.now(timezone.utc)) - observed).total_seconds()
    if age < -300 or age > configuration["readiness_max_age_seconds"]:
        raise ValueError("relay readiness is stale or in the future")


def atomic_public(path: Path, value: dict, *, replace: bool = False) -> None:
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("public receipt must not traverse a symlink")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists() and not replace:
        if read_json(path) == value:
            return
        raise ValueError("preserve the previous immutable acknowledgement")
    fd, name = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "posix":
            temporary.chmod(0o600)
        os.replace(temporary, path)
        if os.name == "posix":
            parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
    finally:
        temporary.unlink(missing_ok=True)


def verify_dispatch_delivery(delivery: Path, dispatch: dict, *, workers: int = 1) -> dict:
    """Shared pre-backup gate; a dispatch may arrive before its file set."""
    validate_dispatch(dispatch)
    if delivery.name != dispatch["batch_relative"]:
        raise ValueError("delivery path differs from the source dispatch")
    if hashlib.sha256(regular(delivery, "backup-envelope.json").read_bytes()).hexdigest() != dispatch["envelope_file_sha256"]:
        raise ValueError("raw envelope differs from the source-pinned file hash")
    verification = verify(delivery, dispatch["envelope_identity_sha256"], workers=workers)
    if verification["files_verified"] + 2 != dispatch["complete_files"]:
        raise ValueError("complete file count differs")
    complete_bytes = verification["bytes_verified"] + sum(regular(delivery, x).stat().st_size for x in ("READY", "backup-envelope.json"))
    if complete_bytes != dispatch["complete_bytes"]:
        raise ValueError("complete file bytes differ")
    return verification


def publish(delivery: Path, dispatch: dict, proof_file: Path, output_root: Path, *, workers: int = 1) -> dict:
    """Adapter called by the existing worker after its fixed-snapshot proof."""
    verify_dispatch_delivery(delivery, dispatch, workers=workers)
    proof = read_json(proof_file)
    # Select known fields; private configuration, raw log output and keys can
    # never be copied into the reverse Syncthing folder.
    required = ("snapshot_id", "repository_check_mode", "repository_check_verified",
                "independent_restore_verified", "source_verifier_verified", "all_files_sha256_verified",
                "command_exit_codes", "complete_workflow_seconds", "accepted_at_utc")
    if any(proof.get(k) != dispatch[k] for k in ("repository_id", "envelope_identity_sha256", "envelope_file_sha256", "complete_files", "complete_bytes")):
        raise ValueError("private acceptance proof differs from this dispatch")
    body = {"contract": ACK, **{k: dispatch[k] for k in ("producer_device_id", "receiver_device_id",
        "envelope_identity_sha256", "envelope_file_sha256", "repository_id", "complete_files", "complete_bytes")},
        "dispatch_identity_sha256": dispatch["identity_sha256"], **{k: proof[k] for k in required},
        "private_evidence_sha256": hashlib.sha256(proof_file.read_bytes()).hexdigest(),
        "scope": "this_closed_delivery_off_host_file_backup_only", "canonical_reconstruction_verified": False,
        "control_database_restore_verified": False, "independent_key_custody_verified": False}
    ack = signed(body)
    validate_ack(ack, dispatch)
    output = output_root / ("acceptance-" + dispatch["envelope_identity_sha256"] + ".json")
    if output.exists():
        old = read_json(output)
        validate_ack(old, dispatch)
        if old["snapshot_id"] != ack["snapshot_id"]:
            raise ValueError("preserve the originally accepted full snapshot")
        return old
    atomic_public(output, ack)
    return ack


def publish_readiness(configuration: dict, private_proof_file: Path, output_root: Path) -> dict:
    """Called under the existing relay owner after its real mount/runtime checks.

    The worker supplies actual disk_usage capacities and the repository ID read
    from that guarded NAS repository. Only the public allowlist is returned.
    """
    proof = read_json(private_proof_file)
    if proof["repository_id"] != configuration["repository_id"]:
        raise ValueError("relay is attached to another NAS repository")
    body = {"contract": READY, **{k: configuration[k] for k in (
        "producer_device_id", "receiver_device_id", "repository_id")},
        **{k: proof[k] for k in ("ingress_free_bytes", "nas_free_bytes", "reserve_bytes", "maximum_batch_bytes",
            "repository_check_mode", "single_owner_verified", "nas_mount_guard_verified", "runtime_lock_verified",
            "automatic_pruning", "automatic_batch_deletion")}, "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    value = signed(body)
    validate_readiness(value, configuration)
    atomic_public(output_root / "readiness.json", value, replace=True)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery", type=Path, required=True)
    parser.add_argument("--dispatch", type=Path, required=True)
    parser.add_argument("--private-proof", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = publish(args.delivery, read_json(args.dispatch), args.private_proof, args.output_root)
    print(json.dumps({k: result[k] for k in ("contract", "envelope_identity_sha256", "snapshot_id", "identity_sha256")}))


if __name__ == "__main__":
    main()
