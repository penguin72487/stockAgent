"""Back up an already-published, pinned receiver handoff through BackupStream.

The original tools package stays immutable.  This adapter only creates the
same closed envelope as a normal paired backup delivery; receipt, restoration,
and transport retirement remain with their existing owners.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from scripts.backup_delivery_receipt import (
    read_json, valid_signature, validate_ack, validate_dispatch, validate_readiness,
    verify_dispatch_delivery,
)
from scripts.verify_backup_delivery import HASH, regular, verify
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes

KIND = "frozen_receiver_handoff_backup"
MAX_PACKAGE_BYTES = 2 * 1024**2


def inspect_handoff(queue, name: str, envelope_id: str) -> dict:
    """Check both the full envelope and its signed, paired source contract."""
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}", name):
        raise ValueError("handoff name must be a single safe component")
    if not isinstance(envelope_id, str) or not HASH.fullmatch(envelope_id):
        raise ValueError("handoff needs a pinned full envelope identity")
    root = queue.transport / "tools" / name
    proof = verify(root, envelope_id)
    envelope_raw = regular(root, "backup-envelope.json").read_bytes()
    envelope = json.loads(envelope_raw)
    manifest = json.loads(regular(root, "handoff-manifest.json").read_bytes())
    valid_signature(manifest, "frozen_backup_receiver_handoff_v1")
    if any(manifest.get(key) != queue.config[key] for key in
           ("producer_device_id", "receiver_device_id", "repository_id")):
        raise ValueError("handoff is not bound to the configured receiver and repository")
    if (envelope.get("selection") != "frozen_backup_receiver_handoff"
            or envelope.get("source_plan_identity_sha256") != manifest["identity_sha256"]
            or any(manifest.get(key) is not False for key in
                   ("replace_existing_worker", "automatic_execution_from_syncthing", "private_credentials_included"))):
        raise ValueError("handoff is not a sealed, non-executable paired control package")
    by_name = {row["relative"]: row for row in envelope["files"]}
    listed = manifest.get("files")
    if (not isinstance(listed, list) or len(listed) + 1 != len(by_name)
            or {row["relative"] for row in listed} != set(by_name) - {"handoff-manifest.json"}
            or any(by_name[row["relative"]] != row for row in listed)):
        raise ValueError("handoff manifest and sealed members differ")
    total = proof["bytes_verified"] + len(envelope_raw) + regular(root, "READY").stat().st_size
    if total > MAX_PACKAGE_BYTES:
        raise ValueError("large handoffs need their own explicitly enrolled backup scope")
    return {"name": name, "identity": envelope_id, "root": root, "envelope": envelope,
            "envelope_file_sha256": hashlib.sha256(envelope_raw).hexdigest(),
            "catalog_identity": manifest["identity_sha256"], "complete_bytes": total,
            "complete_files": proof["files_verified"] + 2}


def _stage(queue, source: dict) -> Path:
    parent = queue.transport / ".staging"
    if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
        raise SnapshotError("backup staging root is redirected or is not a directory")
    staging = parent / ("frozen-handoff-" + uuid.uuid4().hex)
    staging.mkdir(mode=0o700, parents=True)
    # Source packages are size bounded.  Hashes and the pinned identity are
    # checked before and after copying; verification of the stage is separate.
    for relative in [row["relative"] for row in source["envelope"]["files"]] + ["backup-envelope.json", "READY"]:
        path = regular(source["root"], relative)
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_PACKAGE_BYTES + 1)
        if len(raw) > MAX_PACKAGE_BYTES:
            raise ValueError("handoff member grew beyond the bounded package size")
        atomic_write_bytes(staging / relative, raw, mode=0o600)
    verify(staging, source["identity"])
    rechecked = inspect_handoff(queue, source["name"], source["identity"])
    if rechecked["envelope_file_sha256"] != source["envelope_file_sha256"]:
        raise ValueError("handoff envelope changed during backup preparation")
    return staging


def publish_handoff_backups(queue, requested: list[tuple[str, str]], *, wait_for_owner: bool = False) -> dict:
    """Admit and journal fixed packages under the existing publication owner.

    A completed delivery still needs lab203's independently validated NAS ACK.
    A failed or interrupted stage is retained for the canonical recovery path.
    """
    if not requested or len(set(requested)) != len(requested):
        raise ValueError("provide distinct pinned handoffs")
    if len({identity for _, identity in requested}) != len(requested):
        raise ValueError("a pinned envelope cannot name more than one handoff")
    if type(wait_for_owner) is not bool:
        raise ValueError("owner wait must be an explicit boolean")
    queue.storage_guard()
    queue.paired_transport()
    sources = [inspect_handoff(queue, name, identity) for name, identity in requested]
    lock = queue.state / "owner.lock"
    if lock.is_symlink():
        raise SnapshotError("backup owner lock is redirected")
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
        queue.storage_guard()
        ledger = queue.load_ledger()
        queue.recover_publications(ledger)
        errors = queue.ingest(ledger)
        queue._save_ledger(ledger)
        if errors:
            raise SnapshotError("paired NAS receipts need reconciliation before publication")
        completed, pending = [], []
        for source in sources:
            key = source["identity"]
            delivery = ledger["deliveries"].get(key)
            batch = queue.transport / ("batch-" + key)
            dispatch_path = queue.transport / "tools" / "dispatch" / (key + ".json")
            if delivery is None:
                if batch.exists() or dispatch_path.exists():
                    raise SnapshotError("unregistered handoff batch requires original-owner recovery")
                pending.append(source)
                continue
            dispatch = delivery["dispatch"]
            validate_dispatch(dispatch)
            if (dispatch.get("delivery_kind") != KIND
                    or dispatch["envelope_file_sha256"] != source["envelope_file_sha256"]
                    or any(dispatch[field] != queue.config[field] for field in
                           ("producer_device_id", "receiver_device_id", "repository_id"))):
                raise SnapshotError("existing handoff delivery differs from this fixed source")
            if dispatch_path.exists() and read_json(dispatch_path) != dispatch:
                raise SnapshotError("existing handoff dispatch differs from the owner ledger")
            if batch.exists():
                verify_dispatch_delivery(batch, dispatch)
            elif not delivery.get("acceptance"):
                raise SnapshotError("unacknowledged handoff batch is missing")
            if delivery.get("acceptance"):
                validate_ack(delivery["acceptance"], dispatch)
            completed.append({"name": source["name"], "identity": key,
                              "state": delivery["state"], "nas_ack_verified": bool(delivery.get("acceptance"))})
        if pending:
            ready = read_json(queue.receipts / "readiness.json")
            independent = not queue.config.get("publication_requires_receiver_ready", True)
            validate_readiness(ready, queue.config, admission_only=independent)
            reserve = max(queue.config["reserve_bytes"], ready["reserve_bytes"])
            pending_bytes = sum(row["dispatch"]["complete_bytes"] for row in ledger["deliveries"].values()
                                if not row.get("acceptance"))
            pending_count = sum(not row.get("acceptance") for row in ledger["deliveries"].values())
            largest = max(item["complete_bytes"] for item in pending)
            if any(item["complete_files"] > queue.config["maximum_batch_files"] for item in pending):
                raise SnapshotError("handoff exceeds the configured per-batch file limit")
            if largest > min(queue.config["maximum_batch_bytes"], ready["maximum_batch_bytes"]):
                raise SnapshotError("handoff exceeds the configured per-batch byte limit")
            if not independent:
                if (pending_count + len(pending) > queue.config["maximum_pending_deliveries"]
                        or sum(item["complete_bytes"] for item in pending) > min(
                            queue.config["maximum_pending_bytes"] - pending_bytes,
                            ready["ingress_free_bytes"] - reserve - pending_bytes,
                            ready["nas_free_bytes"] - reserve - pending_bytes)):
                    raise SnapshotError("receiver or pending delivery budget is exhausted")
            # Count all three bounded allocations once, under the same owner.
            # Extra room covers the atomic temporary member and dispatch files.
            required = sum(item["complete_bytes"] for item in pending) + MAX_PACKAGE_BYTES
            if queue.staging_room(queue.transport_usage()) < required:
                raise SnapshotError("transport or physical reserve cannot admit these handoffs")
            for source in pending:
                queue.storage_guard()
                stage = _stage(queue, source)
                queue.publish_closed(ledger, [], stage, source["identity"], source["catalog_identity"], KIND)
                queue._save_ledger(ledger)
                completed.append({"name": source["name"], "identity": source["identity"],
                                  "state": "published_waiting_nas_receipt", "nas_ack_verified": False})
    return {"state": "paired_handoffs_published_or_existing", "deliveries": completed,
            "all_nas_acks_verified": all(item["nas_ack_verified"] for item in completed),
            "source_deleted": False, "full_history_backup_verified": False}
