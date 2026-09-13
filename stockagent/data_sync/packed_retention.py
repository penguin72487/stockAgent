"""D-backed rolling retention for the authoritative packed cold store.

Release manifests provide atomicity; they are not bulk copies. This module
keeps the current/protected object graph on C while allowing expired history
to live only in the independently verified, additive D archive.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping

from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    _fsync_directory,
    atomic_write_json,
    sha256_file,
)
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.packed_backup import BackupConfig, VolumeGuard, safe_path, signature
from stockagent.data_sync.packed_snapshots import (
    _validate_manifest,
    resolve_packed_snapshot_id,
)

PLAN_SCHEMA = 1


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RetentionConfig:
    sync_root: Path
    archive_root: Path
    materialized_root: Path
    backup_config: Path
    state_dir: Path
    folder_id: str
    required_peer_names: tuple[str, ...]
    authority_node_id: str = "penguin"
    grace_hours: float = 24.0
    peer_proof_max_age_seconds: int = 300
    post_apply_convergence_timeout_seconds: int = 900

    @classmethod
    def load(cls, path: Path, *, repo_root: Path) -> "RetentionConfig":
        raw = json.loads(path.read_text())
        if raw.pop("schema_version", None) != 1:
            raise SnapshotError("unsupported packed retention configuration")
        for key in ("sync_root", "archive_root", "materialized_root", "state_dir"):
            raw[key] = Path(raw[key])
        backup = Path(raw["backup_config"])
        raw["backup_config"] = backup if backup.is_absolute() else repo_root / backup
        raw["required_peer_names"] = tuple(raw["required_peer_names"])
        cfg = cls(**raw)
        if (
            cfg.grace_hours < 0
            or cfg.peer_proof_max_age_seconds <= 0
            or cfg.post_apply_convergence_timeout_seconds < 0
        ):
            raise SnapshotError("invalid rolling retention limits")
        if len(set(cfg.required_peer_names)) != len(cfg.required_peer_names):
            raise SnapshotError("retention peers must be unique")
        return cfg


def _manifest_refs(manifest: Mapping[str, Any]) -> set[str]:
    archive = manifest["archive"]
    return {str(item["relpath"]) for item in [archive["inventory"], *archive["objects"]]}


def _manifest_identity(sync_root: Path, path: Path, manifest: Mapping[str, Any]) -> str:
    _validate_manifest(manifest)
    if path.parent.name != manifest["dataset"] or path.stem != manifest["snapshot_id"]:
        raise SnapshotError(f"manifest path/identity mismatch: {path}")
    return path.relative_to(sync_root).as_posix()


def _local_protected_snapshot_ids(
    materialized_root: Path, known: set[str]
) -> tuple[set[str], list[str], list[str]]:
    protected: set[str] = set()
    evidence: list[str] = []
    errors: list[str] = []
    candidates = [
        path
        for path in materialized_root.rglob("*.pin.json")
        if ".cache-state" not in path.parts
    ]
    candidates += list((materialized_root / ".cache-state/leases").glob("*/*.json"))
    candidates += list(materialized_root.glob("*/*.READY.json"))
    for path in candidates:
        try:
            value = json.loads(path.read_text())
            manifest = value.get("manifest", {}) if isinstance(value, dict) else {}
            snapshot_id = str(value.get("snapshot_id") or manifest.get("snapshot_id") or "")
            target_value = value.get("target") if isinstance(value, dict) else None
            target_exists = bool(target_value) and Path(str(target_value)).exists()
            active = path.name.endswith(".pin.json") or ".READY." in path.name
            active = active or (value.get("state") == "hot" and target_exists)
            if active:
                if snapshot_id in known:
                    protected.add(snapshot_id)
                    evidence.append(f"{snapshot_id}:{path}")
                else:
                    errors.append(f"protected metadata names missing release {snapshot_id!r}: {path}")
        except (OSError, ValueError, TypeError):
            errors.append(f"unreadable protected metadata: {path}")
    # A failed materialization can be the only convenient recovery evidence.
    # Retain its release until the quarantine itself is handled separately.
    quarantine = materialized_root / ".cache-state/quarantine"
    if quarantine.is_dir():
        for path in quarantine.glob("*/*"):
            for snapshot_id in known:
                if snapshot_id in path.name:
                    protected.add(snapshot_id)
                    evidence.append(f"{snapshot_id}:{path}")
    return protected, evidence, errors


def _backup_receipt_proves(
    connection: sqlite3.Connection,
    backup_cfg: BackupConfig,
    relative: str,
    digest: str,
    source: Path,
    archive: Path,
) -> tuple[bool, str]:
    row = connection.execute(
        "SELECT digest,source_sig,target_sig,checked FROM verified WHERE path=?",
        (relative,),
    ).fetchone()
    if row is None or row[0] != digest:
        return False, "missing-backup-checksum-receipt"
    if time.time() - float(row[3]) > backup_cfg.checksum_recheck_days * 86400:
        return False, "expired-backup-checksum-receipt"
    try:
        source_sig = list(signature(source))
        target_sig = list(signature(archive))
        if json.loads(row[1]) != source_sig or json.loads(row[2]) != target_sig:
            return False, "backup-signature-changed"
    except (OSError, ValueError, SnapshotError):
        return False, "backup-proof-unreadable"
    return True, "verified-sha256-receipt"


def _validate_peer_proof(config: RetentionConfig, proof: Mapping[str, Any]) -> None:
    """Reject deletion unless the caller supplies a fresh full-fleet proof."""
    if proof.get("ok") is not True:
        raise SnapshotError("Syncthing peer convergence proof is not complete")
    observed = {str(item.get("name")): item for item in proof.get("peers", [])}
    if any(observed.get(name, {}).get("ok") is not True for name in config.required_peer_names):
        raise SnapshotError("Syncthing proof does not cover every required peer")
    try:
        checked = datetime.fromisoformat(str(proof["checked_at"]))
        if checked.tzinfo is None:
            raise ValueError("timezone missing")
        age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotError("Syncthing proof has no valid checked_at timestamp") from exc
    if age < -5 or age > config.peer_proof_max_age_seconds:
        raise SnapshotError(f"Syncthing convergence proof is stale ({age:.1f}s)")


def build_plan(config: RetentionConfig, *, now_ns: int | None = None) -> dict[str, Any]:
    root = config.sync_root.resolve()
    archive_root = config.archive_root.resolve()
    if (root / ".local-state/node-id").read_text().strip() != config.authority_node_id:
        raise SnapshotError("rolling retention may run only on the configured authority")
    backup_cfg = BackupConfig.load(config.backup_config)
    if backup_cfg.source.resolve() != root or backup_cfg.destination.resolve() != archive_root:
        raise SnapshotError("retention/archive configuration mismatch")
    VolumeGuard(backup_cfg).check()

    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    cutoff_ns = current_ns - int(config.grace_hours * 3600 * 1_000_000_000)
    blockers: list[str] = []
    manifests: dict[str, tuple[Path, dict[str, Any], str]] = {}
    invalid_manifests: list[str] = []
    conflicts = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*.sync-conflict-*")
        if path.is_file() or path.is_symlink()
    )
    if conflicts:
        blockers.append("Syncthing conflict files require audit")
    for path in sorted((root / "manifests").glob("*/*.json")):
        try:
            raw = path.read_bytes()
            value = json.loads(raw)
            relative = _manifest_identity(root, path, value)
            manifests[str(value["snapshot_id"])] = (path, value, relative)
        except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
            invalid_manifests.append(f"{path}:{exc}")
    if invalid_manifests:
        blockers.append("invalid manifests require audit")

    protected_ids: set[str] = set()
    head_evidence: list[str] = []
    for head_path in sorted((root / "heads").glob("*/*.json")):
        try:
            head = json.loads(head_path.read_text())
            resolved = resolve_packed_snapshot_id(
                root, str(head["dataset"]), str(head["snapshot_id"]), require_objects=True
            )
            if resolved.manifest_sha256 != head["manifest_sha256"]:
                raise SnapshotError("head manifest checksum mismatch")
            protected_ids.add(str(head["snapshot_id"]))
            head_evidence.append(head_path.relative_to(root).as_posix())
        except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
            blockers.append(f"incomplete current head: {head_path}: {exc}")

    local_ids, local_evidence, local_errors = _local_protected_snapshot_ids(
        config.materialized_root, set(manifests)
    )
    if local_errors:
        blockers.append("local retention protection metadata requires audit")
    protected_ids.update(local_ids)
    retained_ids = set(protected_ids)
    for snapshot_id, (path, _manifest, _relative) in manifests.items():
        if path.stat().st_mtime_ns > cutoff_ns:
            retained_ids.add(snapshot_id)

    retained_refs: set[str] = set()
    for snapshot_id in retained_ids:
        item = manifests.get(snapshot_id)
        if item is None:
            blockers.append(f"protected release manifest missing: {snapshot_id}")
            continue
        retained_refs.update(_manifest_refs(item[1]))

    stored: dict[str, Path] = {}
    for path in sorted((root / "objects").glob("*/*/*")):
        if path.is_file() and not path.is_symlink():
            stored[path.relative_to(root).as_posix()] = path
    object_candidates = {
        relative: path for relative, path in stored.items()
        if relative not in retained_refs and path.stat().st_mtime_ns <= cutoff_ns
    }
    manifest_candidates = {
        relative: path
        for snapshot_id, (path, _manifest, relative) in manifests.items()
        if snapshot_id not in retained_ids
    }

    db_path = backup_cfg.state_dir / "verified.sqlite3"
    archive_failures: list[str] = []
    entries: list[dict[str, Any]] = []
    if not db_path.is_file():
        raise SnapshotError(f"backup checksum database is missing: {db_path}")
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        for relative, path in sorted(object_candidates.items()):
            digest = path.name.split(".", 1)[0]
            target = safe_path(archive_root, relative)
            proven, reason = _backup_receipt_proves(
                connection, backup_cfg, relative, digest, path, target
            )
            if not proven:
                archive_failures.append(f"{relative}:{reason}")
            info = signature(path)
            entries.append({"kind": "object", "path": relative, "sha256": digest,
                            "bytes": info[2], "allocated_bytes": path.stat().st_blocks * 512,
                            "source_signature": list(info), "archive_proven": proven})
    for relative, path in sorted(manifest_candidates.items()):
        target = safe_path(archive_root, relative)
        digest = sha256_file(path)
        proven = target.is_file() and not target.is_symlink() and sha256_file(target) == digest
        if not proven:
            archive_failures.append(f"{relative}:manifest-not-identical-on-archive")
        info = signature(path)
        entries.append({"kind": "manifest", "path": relative, "sha256": digest,
                        "bytes": info[2], "allocated_bytes": path.stat().st_blocks * 512,
                        "source_signature": list(info), "archive_proven": proven})
    if archive_failures:
        blockers.append("D archive proof incomplete")

    fingerprint_rows = [
        [item["kind"], item["path"], item["sha256"], item["bytes"], item["source_signature"]]
        for item in entries
    ]
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_rows, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    all_manifest_occurrences = sum(
        sum(int(ref.get("bytes", 0)) for ref in [m[1]["archive"]["inventory"], *m[1]["archive"]["objects"]])
        for m in manifests.values()
    )
    unique_stored_bytes = sum(path.stat().st_size for path in stored.values())
    return {
        "schema_version": PLAN_SCHEMA,
        "created_at": _utc_now(),
        "mode": "rolling-current-c-with-additive-d-archive",
        "authority_node_id": config.authority_node_id,
        "sync_root": str(root),
        "archive_root": str(archive_root),
        "grace_hours": config.grace_hours,
        "manifest_count": len(manifests),
        "current_head_count": len(head_evidence),
        "protected_release_count": len(protected_ids),
        "retained_release_count": len(retained_ids),
        "snapshot_reference_occurrence_bytes": all_manifest_occurrences,
        "unique_stored_object_bytes": unique_stored_bytes,
        "content_address_reuse_bytes": max(
            0, all_manifest_occurrences - unique_stored_bytes
        ),
        "candidate_objects": len(object_candidates),
        "candidate_manifests": len(manifest_candidates),
        "reclaimable_bytes": sum(item["bytes"] for item in entries if item["kind"] == "object"),
        "reclaimable_allocated_bytes": sum(item["allocated_bytes"] for item in entries if item["kind"] == "object"),
        "archive_proven_objects": sum(item["kind"] == "object" and item["archive_proven"] for item in entries),
        "archive_failure_count": len(archive_failures),
        "archive_failures": archive_failures[:100],
        "invalid_manifests": invalid_manifests[:100],
        "conflict_files": conflicts[:100],
        "head_evidence": head_evidence,
        "local_protection_evidence": local_evidence,
        "local_protection_errors": local_errors[:100],
        "blockers": blockers,
        "plan_fingerprint": fingerprint,
        "entries": entries,
        "deletes_archive": False,
        "deletes_heads": False,
        "deletes_materialized": False,
    }


def apply_plan(
    config: RetentionConfig,
    expected_fingerprint: str,
    *,
    peer_proof: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_peer_proof(config, peer_proof)
    lock_path = config.sync_root / ".local-state/locks/publish-retention-global.lock"
    with _exclusive_lock(lock_path):
        _validate_peer_proof(config, peer_proof)
        references = process_references(config.sync_root)
        if references:
            raise SnapshotError(f"packed store has live process references: {references}")
        plan = build_plan(config)
        if plan["plan_fingerprint"] != expected_fingerprint:
            raise SnapshotError("retention plan changed; inspect a fresh dry run")
        if plan["blockers"]:
            raise SnapshotError(f"retention blockers: {plan['blockers']}")
        if not all(item["archive_proven"] for item in plan["entries"]):
            raise SnapshotError("not every deletion target has an independent D proof")
        config.state_dir.mkdir(parents=True, exist_ok=True)
        intent = config.state_dir / "apply-intent.json"
        atomic_write_json(intent, plan)
        removed = []
        touched_dirs: set[Path] = set()
        # Remove historical descriptors first. An interrupted pass can leave
        # harmless extra objects, never a visible manifest with newly missing data.
        for kind in ("manifest", "object"):
            for item in (entry for entry in plan["entries"] if entry["kind"] == kind):
                path = safe_path(config.sync_root, item["path"])
                if list(signature(path)) != item["source_signature"] or sha256_file(path) != item["sha256"]:
                    raise SnapshotError(f"target changed after plan: {item['path']}")
                archive = safe_path(config.archive_root, item["path"])
                if not archive.is_file():
                    raise SnapshotError(f"D archive disappeared: {item['path']}")
                path.unlink()
                removed.append(item)
                touched_dirs.add(path.parent)
        for directory in touched_dirs:
            _fsync_directory(directory)
        result = {
            "schema_version": PLAN_SCHEMA,
            "applied_at": _utc_now(),
            "plan_fingerprint": expected_fingerprint,
            "removed_objects": sum(item["kind"] == "object" for item in removed),
            "removed_manifests": sum(item["kind"] == "manifest" for item in removed),
            "recovered_allocated_bytes": sum(item["allocated_bytes"] for item in removed if item["kind"] == "object"),
            "archive_root": str(config.archive_root),
            "recovery_possible_from_d": True,
            "pre_delete_peer_convergence": dict(peer_proof),
            "removed_paths": [item["path"] for item in removed],
        }
        atomic_write_json(config.state_dir / "last-apply.json", result)
        intent.unlink(missing_ok=True)
        _fsync_directory(config.state_dir)
        return result
