"""Retire only machine-ACKed cold transport copies with an unchanged source.

NAS snapshots, source objects, code/SQL deliveries and relayed pilot baselines
are outside this cache. The source owns Syncthing deletion propagation; the
receiver continues using its existing backup worker and owner.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat

from scripts.backup_delivery_receipt import validate_ack
from scripts.verify_backup_delivery import regular
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.offhost_backup import _regular, private_json
from stockagent.data_sync.packed_backup import PinnedObjectSignatures, safe_path, signature, signature_from_stat
from stockagent.runtime_identity import identity_sha256

CONTRACT = "verified_reconstructible_cold_transport_cache_retirement_v1"


def process_roots(transport: Path, batch: Path, *, mountinfo: str | None = None) -> tuple[Path, ...]:
    """Include physical/bind aliases of this exact batch, not sibling batches."""
    decode = lambda s: re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), s)
    mounts = []
    for line in (mountinfo if mountinfo is not None else Path('/proc/self/mountinfo').read_text()).splitlines():
        fields = line.split()
        separator = fields.index('-')
        mounts.append((Path(decode(fields[3])), Path(decode(fields[4])), decode(fields[separator + 2])))
    enrolled = next((m for m in mounts if m[1] == transport), None)
    roots = {batch}
    if enrolled is not None:
        filesystem_path = enrolled[0] / batch.relative_to(transport)
        for filesystem_root, mountpoint, source in mounts:
            if source == enrolled[2] and filesystem_path.is_relative_to(filesystem_root):
                roots.add(mountpoint / filesystem_path.relative_to(filesystem_root))
    return tuple(sorted(roots))


def same_ntfs_signature(current, recorded) -> bool:
    # Device numbers change after remount; enrolled volume + NTFS inode/time do not.
    return len(recorded) == 5 and tuple(current)[1:] == tuple(recorded)[1:]


def source_path(queue, row: dict, *, retain_head: bool = False, pinned=None) -> Path | None:
    if pinned is not None and row["relative"].startswith("objects/"):
        if not same_ntfs_signature(pinned.signature(row["relative"]),row["signature"]):
            raise SnapshotError("original cold source changed; preserve its transport copy")
        return queue.cold / row["relative"]  # Descriptor and every parent are pinned/rechecked above.
    path = safe_path(queue.cold, row["relative"])
    if row["relative"].startswith("heads/"):
        raw = row.get("captured_bytes_utf8")
        if raw is not None:
            data = raw.encode("utf-8")
            if len(data) != row["bytes"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
                raise SnapshotError("captured original head metadata changed")
            parts = Path(row["relative"]).parts
            archived = queue.cold / "head-history" / Path(*parts).with_suffix("") / (row["sha256"] + ".json")
            if archived.exists() and hashlib.sha256(_regular(archived).read_bytes()).hexdigest() == row["sha256"]:
                return archived
            if path.exists() and hashlib.sha256(_regular(path).read_bytes()).hexdigest() == row["sha256"]:
                return path
            if not retain_head:
                return None  # Original observed bytes remain in the private journal.
            # Preserve the small, genuinely observed head bytes from the journal.
            audit = queue.state / "retained-captured-heads" / (row["sha256"] + ".json")
            if audit.exists():
                if _regular(audit).read_bytes() != data:
                    raise SnapshotError("retained captured head differs")
            else:
                atomic_write_bytes(audit, data, mode=0o600)
            return audit
    path = _regular(path)
    if not same_ntfs_signature(signature(path), row["signature"]):
        raise SnapshotError("original cold source changed; preserve its transport copy")
    return path


def build_plan(queue, delivery: dict, *, root: Path | None = None, resume: bool = False) -> dict:
    dispatch = delivery["dispatch"]
    if dispatch["delivery_kind"] != "incremental_cold_objects" or not delivery.get("acceptance"):
        raise SnapshotError("only machine-ACKed cold-object copies are cache candidates")
    validate_ack(delivery["acceptance"], dispatch)
    key = dispatch["envelope_identity_sha256"]
    if dispatch["batch_relative"] != "batch-" + key:
        raise SnapshotError("cold cache batch is outside its pinned scope")
    batch = root or queue.transport / dispatch["batch_relative"]
    envelope_path = batch / "backup-envelope.json"
    raw = _regular(envelope_path).read_bytes() if envelope_path.exists() else None
    # The journal pins metadata even after a crash during exact-file unlink.
    retirement = delivery.get("cache_retirement", {})
    envelope = json.loads(raw) if raw is not None else retirement.get("envelope")
    if envelope is None or (raw is not None and hashlib.sha256(raw).hexdigest() != dispatch["envelope_file_sha256"]):
        raise SnapshotError("cold cache envelope is missing or changed")
    if raw is None and not resume:
        raise SnapshotError("cold cache envelope is missing")
    files = {r["relative"]: r for r in envelope["files"]}
    required = {"cold/" + r["relative"] for r in delivery["files"]}
    if set(files) != required or set(delivery.get("transport_signatures", {})) != required:
        raise SnapshotError("cold cache lacks its full-SHA signature proof")
    source_proofs = []
    with PinnedObjectSignatures(queue.cold) as pinned:
        for row in delivery["files"]:
            member = files["cold/" + row["relative"]]
            if (member["sha256"], member["bytes"]) != (row["sha256"], row["bytes"]):
                raise SnapshotError("cold cache member differs from its source journal")
            original = source_path(queue,row,pinned=pinned)
            observed = (pinned.signature(row["relative"]) if row["relative"].startswith("objects/")
                        else signature(original) if original else None)
            if observed is not None and not row["relative"].startswith("heads/") and not same_ntfs_signature(observed,row["signature"]):
                raise SnapshotError("original cold source changed during its pinned proof")
            source_proofs.append({"relative":row["relative"],"sha256":row["sha256"],
                "source_signature":list(observed) if observed is not None else None,
                "captured_head_retained_in_journal":original is None})
        pinned.recheck()
    expected = {*required, "backup-envelope.json", "READY"}
    observed = set()
    current_signatures = {}
    if any(p.is_symlink() for p in (batch,*batch.parents)):
        raise SnapshotError("cold cache capacity root is redirected")
    root_identity=batch.stat()
    def walk_error(error):
        raise error
    for parent,dirs,names,directory_fd in os.fwalk(batch,follow_symlinks=False,onerror=walk_error):
        directory_info=os.fstat(directory_fd)
        if directory_info.st_dev != root_identity.st_dev or (Path(parent) == batch and directory_info.st_ino != root_identity.st_ino):
            raise SnapshotError("cold cache root/mount changed during inventory")
        for name in dirs:
            item=os.stat(name,dir_fd=directory_fd,follow_symlinks=False)
            if not stat.S_ISDIR(item.st_mode) or item.st_dev != root_identity.st_dev:
                raise SnapshotError("cold cache has a redirected directory")
        for name in names:
            path = Path(parent) / name
            relative = path.relative_to(batch).as_posix()
            item=os.stat(name,dir_fd=directory_fd,follow_symlinks=False)
            if relative not in expected or not stat.S_ISREG(item.st_mode) or item.st_nlink != 1 or item.st_dev != root_identity.st_dev:
                raise SnapshotError("cold cache has an unknown or shared file")
            observed.add(relative)
            before = signature_from_stat(item)
            if relative in required:
                pinned = delivery["transport_signatures"][relative]
                if not same_ntfs_signature(before,pinned):
                    # After interruption/remount, re-read instead of guessing.
                    if not resume:
                        raise SnapshotError("cold transport member changed since complete SHA verification")
                    digest = hashlib.sha256()
                    with path.open("rb") as reader:
                        for chunk in iter(lambda: reader.read(4 * 1024**2), b""):
                            digest.update(chunk)
                    if digest.hexdigest() != files[relative]["sha256"] or signature(path) != before:
                        raise SnapshotError("interrupted cold cache member changed")
            current_signatures[relative] = list(before)
    after=batch.stat()
    if (root_identity.st_dev,root_identity.st_ino) != (after.st_dev,after.st_ino):
        raise SnapshotError("cold cache authority changed during inventory")
    if (not resume and observed != expected) or not observed <= expected:
        raise SnapshotError("cold cache exact file set differs")
    ready = batch / "READY"
    if ready.exists() and ready.read_bytes() != (key + "\n").encode():
        raise SnapshotError("cold cache READY differs")
    references = process_references_many(process_roots(queue.transport, batch))
    if references:
        raise SnapshotError("cold cache has active process references")
    body = {"contract": CONTRACT, "envelope_identity_sha256": key,
            "nas_snapshot_id": delivery["acceptance"]["snapshot_id"], "source_proofs": source_proofs,
            "files": sorted(expected), "complete_bytes": dispatch["complete_bytes"],
            "source_objects_deleted": 0, "NAS_snapshots_deleted": 0}
    return {**body, "identity_sha256": identity_sha256(body), "envelope": envelope,
            "verified_transport_signatures": current_signatures}


def retire(queue, ledger: dict, key: str) -> dict:
    delivery = ledger["deliveries"][key]
    prior = delivery.get("cache_retirement", {})
    if prior.get("state") == "retired":
        return prior
    queue.storage_guard()
    batch = queue.transport / delivery["dispatch"]["batch_relative"]
    stage = queue.transport / ".staging" / ("retired-cache-" + key)
    if stage.exists() and batch.exists():
        raise SnapshotError("duplicate cache retirement roots")
    if not stage.exists() and not batch.exists() and prior.get("state") in {"prepared", "renamed"}:
        # Directory removal finished before the terminal journal write.
        delivery["cache_retirement"] = {**prior, "state": "retired"}
        private_json(queue.ledger_path, ledger)
        return delivery["cache_retirement"]
    plan = build_plan(queue, delivery, root=stage if stage.exists() else batch, resume=stage.exists())
    if not stage.exists():
        with PinnedObjectSignatures(queue.cold) as pinned:
            for row in delivery["files"]:
                source_path(queue,row,retain_head=True,pinned=pinned)
            pinned.recheck()
        delivery["cache_retirement"] = {**plan, "state": "prepared"}
        private_json(queue.ledger_path, ledger)
        # Recheck all sources and process references immediately before rename.
        second = build_plan(queue, delivery)
        if second["identity_sha256"] != plan["identity_sha256"]:
            raise SnapshotError("cold cache retirement plan changed")
        queue.storage_guard()
        os.rename(batch, stage)
        root_fd = os.open(queue.transport, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
        delivery["cache_retirement"]["state"] = "renamed"
        private_json(queue.ledger_path, ledger)
    plan = build_plan(queue, delivery, root=stage, resume=prior.get("state") in {"prepared", "renamed"})
    # Exact files only. Unexpected contents survive and make rmdir fail closed.
    for relative in plan["files"]:
        path = stage / relative
        if path.exists():
            path = regular(stage, relative)
            if not same_ntfs_signature(signature(path), plan["verified_transport_signatures"][relative]):
                raise SnapshotError("cold cache member changed immediately before unlink")
            path.unlink()
    directories = [Path(parent) for parent, _, _ in os.walk(stage, followlinks=False)]
    for path in sorted(directories, key=lambda p: len(p.parts), reverse=True):
        path.rmdir()
    delivery["cache_retirement"] = {**plan, "state": "retired"}
    private_json(queue.ledger_path, ledger)
    return delivery["cache_retirement"]
