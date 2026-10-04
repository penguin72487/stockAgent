"""Assemble an exact release from independently restored immutable waves.

The caller owns fixed NAS snapshot restoration/provenance. No producer identity,
mutable head, live source or new release is created by this recovery helper.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil

from scripts.verify_backup_delivery import HASH, verify
from stockagent.data_sync.desync_snapshots import SnapshotError, validate_slug
from stockagent.data_sync.offhost_backup import _regular, _sha
from stockagent.data_sync.packed_backup import object_descriptor, safe_path, signature
from stockagent.data_sync.packed_snapshots import _validate_manifest, resolve_packed_snapshot_id, verify_packed_snapshot


def assemble_release(inputs: list[dict], destination: Path, *, dataset: str,
                     snapshot_id: str, manifest_sha256: str) -> dict:
    dataset = validate_slug(dataset, "dataset")
    snapshot_id = validate_slug(snapshot_id, "snapshot_id")
    if not HASH.fullmatch(manifest_sha256) or not inputs:
        raise SnapshotError("recovery needs fixed source manifest and restored delivery identities")
    destination = destination.absolute()
    if destination.exists() or any(p.is_symlink() for p in (destination, *destination.parents)):
        raise SnapshotError("use a fresh independent recovery directory")
    available = {}
    transports = []
    roots = []
    for item in inputs:
        root = Path(item["root"]).absolute()
        if root == destination or destination.is_relative_to(root) or root.is_relative_to(destination):
            raise SnapshotError("recovery output overlaps a restored delivery")
        raw = _regular(root / "backup-envelope.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["envelope_file_sha256"]:
            raise SnapshotError("restored raw envelope differs from its source-pinned hash")
        transports.append(verify(root, item["envelope_identity_sha256"]))
        roots.append(root)
        envelope = json.loads(raw)
        for row in envelope["files"]:
            if not row["relative"].startswith("cold/"):
                continue
            relative = row["relative"][5:]
            if relative.startswith("objects/") and object_descriptor(relative) != row["sha256"]:
                raise SnapshotError("restored immutable object descriptor differs")
            key = (relative, row["sha256"])
            if key in available and available[key][0]["bytes"] != row["bytes"]:
                raise SnapshotError("restored waves disagree on immutable object size")
            available[key] = (row, safe_path(root, row["relative"]))
    relative = f"manifests/{dataset}/{snapshot_id}.json"
    selected = available.get((relative, manifest_sha256))
    if selected is None:
        raise SnapshotError("fixed source manifest is absent from the restored waves")
    manifest = json.loads(_regular(selected[1]).read_bytes())
    _validate_manifest(manifest)
    if manifest["dataset"] != dataset or manifest["snapshot_id"] != snapshot_id:
        raise SnapshotError("restored manifest identity differs")
    required = [{"relpath": relative, "sha256": manifest_sha256, "bytes": selected[0]["bytes"]},
                manifest["archive"]["inventory"], *manifest["archive"]["objects"]]
    if any((r["relpath"], r["sha256"]) not in available for r in required):
        raise SnapshotError("exact release closure is incomplete; more fixed snapshot restores are required")
    destination.mkdir(mode=0o700, parents=True)
    for row in required:
        _, source = available[(row["relpath"], row["sha256"])]
        before = signature(source)
        target = safe_path(destination, row["relpath"])
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(0o600)
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
        if before != signature(source) or target.stat().st_size != row["bytes"] or _sha(target) != row["sha256"]:
            raise SnapshotError("restored source changed or independently copied recovery bytes differ")
    resolved = resolve_packed_snapshot_id(destination, dataset, snapshot_id)
    proof = verify_packed_snapshot(destination, resolved)
    return {"state": "exact_packed_release_union_verified", "dataset": dataset, "snapshot_id": snapshot_id,
        "manifest_sha256": manifest_sha256, "portable_source_fingerprint_sha256": manifest["source"]["portable_fingerprint_sha256"],
        "restored_delivery_count": len(roots), "copied_files": len(required), "canonical_archive": proof,
        "transports": transports, "mutable_heads_copied": False, "producer_identity_copied": False,
        "nas_provenance_verified_by_this_tool": False, "full_history_backup_verified": False}
