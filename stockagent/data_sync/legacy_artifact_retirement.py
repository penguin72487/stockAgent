"""Fail-closed seven-day retirement of an exact legacy quarantine archive."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_retirement import _assert_unlink_gates, _hot_mirror, _reclaimable_file_bytes
from stockagent.data_sync.cold_artifacts import COLD_ACTIVATION_SCHEMA_VERSION, rebuild_cold_ignore
from stockagent.data_sync.cold_primary import verify_cold_resilience
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    _utc_iso_from_ns,
    atomic_write_json,
    sha256_file,
)
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec,
    source_plan,
    verify_archive_directory,
)
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import (
    resolve_latest_packed,
    verify_packed_snapshot,
)


def _active_service_references(source: Path, repo_root: Path) -> list[str]:
    return artifact_service_references((source,), repo_root)[str(source.absolute())]


def _state_path(state_root: Path, dataset: str) -> Path:
    return state_root / "retirements" / f"{dataset}.json"


def _read_state(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("schema_version") != 1:
        raise SnapshotError(f"legacy retirement state schema mismatch: {path}")
    return state


def renew_legacy_lease(
    spec: LegacyArchiveSpec,
    *,
    artifact_root: Path,
    state_root: Path,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Explicit pre-use renewal for jobs shorter than a process scan."""

    source = artifact_root.resolve() / spec.relative_root
    if source.is_symlink() or not source.is_dir() or source.resolve() != source:
        raise SnapshotError("legacy hot source is not available for lease renewal")
    state_path = _state_path(state_root, spec.dataset)
    lock_path = state_root / "retirements" / f"{spec.dataset}.lock"
    with _exclusive_lock(lock_path):
        state = _read_state(state_path)
        if (
            state is None
            or state.get("dataset") != spec.dataset
            or state.get("relative_root") != spec.relative_root
            or state.get("state") != "hot-enrolled"
        ):
            raise SnapshotError("legacy source has no active hot-use lease")
        observed_ns = time.time_ns() if now_ns is None else int(now_ns)
        renewed_ns = max(observed_ns, int(state["last_used_ns"]))
        state.update(last_used_ns=renewed_ns, last_used_at=_utc_iso_from_ns(renewed_ns))
        atomic_write_json(state_path, state)
        return {
            "dataset": spec.dataset,
            "source": str(source),
            "last_used_at": state["last_used_at"],
            "lease_expires_at": _utc_iso_from_ns(renewed_ns + 7 * 86_400_000_000_000),
        }


def enroll_legacy_lease(
    spec: LegacyArchiveSpec, *, artifact_root: Path, state_root: Path
) -> dict[str, Any]:
    """Start observation without asserting preservation or deleting any bytes.

    Full source/cold/mirror/peer verification remains mandatory at retirement.
    Enrollment is deliberately cheap so awaiting cold publication does not
    postpone the beginning of the seven-day observation period.
    """

    artifact_root = artifact_root.resolve()
    source = artifact_root / spec.relative_root
    inventory = source_plan(source, spec)
    if artifact_process_references(source, artifact_root / "markets"):
        raise SnapshotError("legacy lease enrollment source is in use")
    state_path = _state_path(state_root, spec.dataset)
    with _exclusive_lock(state_root / "retirements" / f"{spec.dataset}.lock"):
        state = _read_state(state_path)
        if state is not None:
            if (state.get("dataset") != spec.dataset
                or state.get("relative_root") != spec.relative_root
                or state.get("state") != "hot-enrolled"):
                raise SnapshotError("legacy lease enrollment state identity mismatch")
            action = "already-enrolled"
        else:
            now_ns = time.time_ns()
            state = {"schema_version": 1, "dataset": spec.dataset,
                     "relative_root": spec.relative_root, "state": "hot-enrolled",
                     "last_used_ns": now_ns, "last_used_at": _utc_iso_from_ns(now_ns)}
            atomic_write_json(state_path, state)
            action = "enrolled"
    return {**state, "action": action, "deleted": False,
            "files": len(inventory), "cold_verified": False,
            "lease_expires_at": _utc_iso_from_ns(int(state["last_used_ns"]) + 7 * 86_400_000_000_000)}


def plan_legacy_retirement(
    spec: LegacyArchiveSpec,
    *,
    repo_root: Path,
    artifact_root: Path,
    hot_root: Path,
    sync_root: Path,
    materialized_root: Path,
    state_root: Path,
    activation_root: Path,
    backup_config: Path,
    peer_proof: Mapping[str, Any],
    peer_probe: Callable[[], Mapping[str, Any]] | None = None,
    bridge_inactive: bool,
    manual_immediate: bool = False,
    now_ns: int | None = None,
) -> dict[str, Any]:
    if type(manual_immediate) is not bool:
        raise SnapshotError("manual immediate retirement must be an explicit boolean")
    now_ns = time.time_ns() if now_ns is None else int(now_ns)
    repo_root = repo_root.resolve()
    artifact_root = artifact_root.resolve()
    hot_root = hot_root.resolve()
    sync_root = sync_root.resolve()
    source = artifact_root / spec.relative_root
    hot_tree = hot_root / spec.relative_root
    archive = spec.stage_root / spec.dataset / "archive"
    if (
        not source.is_dir()
        or source.is_symlink()
        or source.resolve() != source
        or hot_tree.resolve(strict=False) != hot_tree
    ):
        raise SnapshotError("legacy retirement source/hot path is missing or redirected")
    if source.stat().st_dev != state_root.parent.stat().st_dev:
        raise SnapshotError("legacy retirement quarantine must share source filesystem")
    if hot_tree.exists() and hot_tree.stat().st_dev != source.stat().st_dev:
        raise SnapshotError("legacy retirement hot mirror must share source filesystem")
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if (
        metadata.get("transport_role") != "legacy-quarantine-archive"
        or metadata.get("deployable") != "false"
        or metadata.get("source_relative_root") != spec.relative_root
        or metadata.get("legacy_manifest_sha256")
        != sha256_file(archive / "legacy_archive_manifest.json")
    ):
        raise SnapshotError("cold release does not match legacy quarantine source")
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive)
    proof = verify_archive_directory(archive, source)
    cold_proof = verify_cold_resilience(sync_root, resolved, backup_config)
    original_inventory = [
        {"kind": "file", "path": row["path"], "size": row["source"]["size"], "sha256": row["original_sha256"]}
        for row in proof["manifest"]["files"]
    ]
    mirror = _hot_mirror(source, hot_tree, original_inventory)
    state = _read_state(_state_path(state_root, spec.dataset))
    if state is not None and (
        state.get("dataset") != spec.dataset
        or state.get("relative_root") != spec.relative_root
    ):
        raise SnapshotError("legacy retirement state identity mismatch")
    last_used_ns = int(state["last_used_ns"]) if state else 0
    expires_ns = (last_used_ns or now_ns) + 7 * 86_400_000_000_000
    process_refs = artifact_process_references(source, artifact_root / "markets")
    if hot_tree.is_dir():
        process_refs.extend(process_references(hot_tree))
    service_refs = _active_service_references(source, repo_root)
    blockers = []
    if state is None:
        if not manual_immediate:
            blockers.append("seven-day-use-lease-not-enrolled")
    elif state.get("state") == "retiring":
        blockers.append("unfinished-retirement-quarantine")
    elif state.get("state") != "hot-enrolled":
        blockers.append("retirement-state-is-not-hot-enrolled")
    elif now_ns < expires_ns and not manual_immediate:
        blockers.append("seven-day-use-lease-active")
    if process_refs:
        blockers.append("artifact-has-process-references")
    if service_refs:
        blockers.append("enabled-service-references-artifact")
    if str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(materialized_root):
        blockers.append("artifact-release-is-pinned")
    # A full C + D + original-source audit can outlive a five-minute transport
    # receipt. Observe Syncthing again after those expensive reads, immediately
    # before deciding whether a hot source may be retired.
    current_peer_proof = peer_probe() if peer_probe is not None else peer_proof
    if current_peer_proof.get("ok") is not True:
        blockers.append("cold-peer-not-converged")
    try:
        checked = datetime.fromisoformat(str(current_peer_proof["checked_at"]))
        if checked.tzinfo is None:
            raise ValueError("peer proof timestamp has no timezone")
        age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
        if age < -5 or age > 300:
            blockers.append("peer-proof-stale")
    except (KeyError, TypeError, ValueError):
        blockers.append("peer-proof-missing-timestamp")
    if not bridge_inactive:
        blockers.append("hot-bridge-must-be-stopped")
    quarantine = state_root / "retirements" / "quarantine" / spec.dataset
    if quarantine.exists() and any(quarantine.iterdir()):
        blockers.append("unfinished-retirement-quarantine")
    identity = {
        "dataset": spec.dataset,
        "relative_root": spec.relative_root,
        "snapshot_id": resolved.manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "source_fingerprint": hashlib.sha256(
            json.dumps(source_plan(source, spec), sort_keys=True).encode()
        ).hexdigest(),
        "hot_fingerprint": mirror["fingerprint"],
        "last_used_ns": last_used_ns,
        "manual_immediate": manual_immediate,
    }
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return {
        **identity,
        "plan_fingerprint": fingerprint,
        "source": str(source),
        "hot_tree": str(hot_tree),
        "original_files": proof["files"],
        "original_bytes": proof["original_bytes"],
        "source_metadata_drift": proof.get("source_metadata_drift", []),
        "cold_bytes": resolved.manifest["archive"]["stored_bytes"],
        "hot_mirror": mirror,
        "process_references": process_refs,
        "service_references": service_refs,
        "peer_proof": dict(current_peer_proof),
        **cold_proof,
        "lease_expires_at": _utc_iso_from_ns(expires_ns),
        "blockers": blockers,
        "apply_ready": not blockers,
    }


def apply_legacy_retirement(
    spec: LegacyArchiveSpec, *, expected_fingerprint: str, **options: Any
) -> dict[str, Any]:
    state_root = Path(options["state_root"])
    state_path = _state_path(state_root, spec.dataset)
    lock_path = state_root / "retirements" / f"{spec.dataset}.lock"
    with _exclusive_lock(lock_path):
        plan = plan_legacy_retirement(spec, **options)
        if plan["plan_fingerprint"] != expected_fingerprint:
            raise SnapshotError("legacy retirement plan changed; rerun dry run")
        now_ns = int(options.get("now_ns") or time.time_ns())
        if "seven-day-use-lease-not-enrolled" in plan["blockers"]:
            state = {
                "schema_version": 1,
                "dataset": spec.dataset,
                "relative_root": spec.relative_root,
                "state": "hot-enrolled",
                "last_used_ns": now_ns,
                "last_used_at": _utc_iso_from_ns(now_ns),
            }
            atomic_write_json(state_path, state)
            return {**plan, "action": "enrolled", "deleted": False}
        if plan["process_references"] and not plan["manual_immediate"]:
            state = _read_state(state_path)
            assert state is not None
            state.update(last_used_ns=now_ns, last_used_at=_utc_iso_from_ns(now_ns))
            atomic_write_json(state_path, state)
            return {**plan, "action": "renewed-in-use", "deleted": False}
        if plan["blockers"]:
            raise SnapshotError("legacy retirement blocked: " + ", ".join(plan["blockers"]))
        state = _read_state(state_path)
        if state is None:
            state = {
                "schema_version": 1,
                "dataset": spec.dataset,
                "relative_root": spec.relative_root,
                "state": "hot-enrolled",
                "last_used_ns": now_ns,
                "last_used_at": _utc_iso_from_ns(now_ns),
            }
        state.update(manual_immediate=plan["manual_immediate"],
                     plan_fingerprint=plan["plan_fingerprint"])
        atomic_write_json(state_path, state)
        source = Path(plan["source"])
        hot_tree = Path(plan["hot_tree"])
        activation_root = Path(options["activation_root"])
        atomic_write_json(
            activation_root / f"{spec.dataset}.json",
            {
                "schema_version": COLD_ACTIVATION_SCHEMA_VERSION,
                "dataset": spec.dataset,
                "snapshot_id": plan["snapshot_id"],
                "manifest_sha256": plan["manifest_sha256"],
                "artifact_relative_root": spec.relative_root,
                "ignored_file_paths": [spec.relative_root],
                "retired_at": _utc_iso_from_ns(now_ns),
            },
        )
        rebuild_cold_ignore(Path(options["hot_root"]), activation_root)
        quarantine = (
            state_root / "retirements" / "quarantine" / spec.dataset / f"{now_ns}-{uuid.uuid4().hex}"
        )
        quarantine.mkdir(parents=True, exist_ok=False)
        state = _read_state(state_path)
        assert state is not None
        state.update(state="retiring", quarantine=str(quarantine), snapshot_id=plan["snapshot_id"])
        atomic_write_json(state_path, state)
        if hot_tree.is_dir():
            os.replace(hot_tree, quarantine / "hot")
        os.replace(source, quarantine / "source")
        archive = spec.stage_root / spec.dataset / "archive"
        verify_archive_directory(archive, quarantine / "source")
        if (quarantine / "hot").is_dir():
            proof = json.loads((archive / "legacy_archive_manifest.json").read_text())
            inventory = [
                {"kind": "file", "path": row["path"], "size": row["source"]["size"], "sha256": row["original_sha256"]}
                for row in proof["files"]
            ]
            _hot_mirror(quarantine / "source", quarantine / "hot", inventory)
        resolved = resolve_latest_packed(Path(options["sync_root"]), spec.dataset)
        if (resolved.manifest["snapshot_id"] != plan["snapshot_id"]
            or resolved.manifest_sha256 != plan["manifest_sha256"]):
            raise SnapshotError("legacy cold head changed after rename; quarantine retained")
        verify_cold_resilience(Path(options["sync_root"]), resolved, Path(options["backup_config"]))
        unlink_peer_proof = _assert_unlink_gates(plan, quarantine, **options)
        reclaimed_bytes = _reclaimable_file_bytes(quarantine / "source", quarantine / "hot")
        if (quarantine / "hot").is_dir():
            shutil.rmtree(quarantine / "hot")
        shutil.rmtree(quarantine / "source")
        quarantine.rmdir()
        state.update(state="cold-only", retired_at=_utc_iso_from_ns(time.time_ns()),
                     reclaimed_allocated_file_bytes=reclaimed_bytes)
        state.pop("quarantine", None)
        atomic_write_json(state_path, state)
        return {**plan, "action": "retired", "deleted": True,
                "reclaimed_allocated_file_bytes": reclaimed_bytes, "unlink_peer_proof": unlink_peer_proof}
