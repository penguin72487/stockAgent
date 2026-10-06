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
    legacy_process_scope,
    _native_read_options,
    source_plan,
    verify_archive_directory,
)
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import (
    resolve_latest_packed,
    verify_packed_snapshot,
)


def _active_service_references(source: Path, repo_root: Path) -> list[str]:
    from stockagent.data_sync.remote_legacy_return import active_configuration_references

    return (artifact_service_references((source,), repo_root)[str(source.absolute())]
            + active_configuration_references(source, repo_root))


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
    if artifact_process_references(source, legacy_process_scope(spec, artifact_root)):
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


def _full_verification_binding(spec: LegacyArchiveSpec, source: Path, hot_tree: Path,
                               archive: Path, sync_root: Path) -> dict[str, Any]:
    """Bind a full byte audit to every observed mutable and cold generation."""
    from stockagent.data_sync.remote_legacy_return import metadata_tree, real, signature

    resolved = resolve_latest_packed(sync_root, spec.dataset)
    refs = [resolved.manifest["archive"]["inventory"], *resolved.manifest["archive"]["objects"]]
    objects = []
    for member in refs:
        path = real(sync_root / member["relpath"])
        info = path.lstat()
        if not path.is_file() or info.st_size != member["bytes"]:
            raise SnapshotError("legacy full verification cold generation is unavailable")
        objects.append({"relative": member["relpath"], "signature": signature(info)})
    return {"dataset": spec.dataset, "relative_root": spec.relative_root,
            "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256, "objects": objects,
            "source": metadata_tree(source)["fingerprint"],
            "hot": metadata_tree(hot_tree)["fingerprint"] if hot_tree.exists() else "absent",
            "archive": metadata_tree(archive)["fingerprint"]}


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
    manual_capture: bool = False,
    now_ns: int | None = None,
    _owned_full_verification: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if type(manual_immediate) is not bool:
        raise SnapshotError("manual immediate retirement must be an explicit boolean")
    if manual_capture and not manual_immediate:
        raise SnapshotError("manual capture retirement requires manual immediate authorization")
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
    binding = (_full_verification_binding(spec, source, hot_tree, archive, sync_root)
               if manual_immediate else None)
    if _owned_full_verification is None:
        verify_packed_snapshot(sync_root, resolved, materialized_path=archive, **_native_read_options(sync_root))
        proof = verify_archive_directory(archive, source, spec=spec, manual_capture=manual_capture,
                                         artifact_root=artifact_root, repo_root=repo_root)
    else:
        if (not manual_immediate
                or _owned_full_verification.get("contract") != "owned-legacy-full-verification-v1"
                or not 0 <= time.time() - _owned_full_verification["verified_at_epoch"] <= 1500
                or _owned_full_verification["binding"] != binding):
            raise SnapshotError("owned legacy full verification expired or its generation changed")
        proof = {"manifest": json.loads((archive / "legacy_archive_manifest.json").read_text()),
                 "files": _owned_full_verification["original_files"],
                 "original_bytes": _owned_full_verification["original_bytes"],
                 "source_metadata_drift": _owned_full_verification["source_metadata_drift"]}
    cold_proof = verify_cold_resilience(sync_root, resolved, backup_config)
    original_inventory = [
        {"kind": "file", "path": row["path"], "size": row["source"]["size"], "sha256": row["original_sha256"]}
        for row in proof["manifest"]["files"]
    ]
    mirror = _hot_mirror(source, hot_tree, original_inventory)
    full_verification = None
    if binding is not None:
        if _full_verification_binding(spec, source, hot_tree, archive, sync_root) != binding:
            raise SnapshotError("legacy source/mirror/cold generation changed during full verification")
        full_verification = (_owned_full_verification if _owned_full_verification is not None else {
            "contract": "owned-legacy-full-verification-v1", "binding": binding,
            "verified_at_epoch": time.time(), "original_files": proof["files"],
            "original_bytes": proof["original_bytes"],
            "source_metadata_drift": proof.get("source_metadata_drift", []),
        })
    state = _read_state(_state_path(state_root, spec.dataset))
    if state is not None and (
        state.get("dataset") != spec.dataset
        or state.get("relative_root") != spec.relative_root
    ):
        raise SnapshotError("legacy retirement state identity mismatch")
    last_used_ns = int(state["last_used_ns"]) if state else 0
    expires_ns = (last_used_ns or now_ns) + 7 * 86_400_000_000_000
    process_refs = artifact_process_references(source, legacy_process_scope(spec, artifact_root))
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
            json.dumps(source_plan(source, spec, manual_capture=manual_capture), sort_keys=True).encode()
        ).hexdigest(),
        "hot_fingerprint": mirror["fingerprint"],
        "last_used_ns": last_used_ns,
        "manual_immediate": manual_immediate,
        "manual_capture": manual_capture,
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
        **({"full_verification": dict(full_verification)} if full_verification is not None else {}),
    }


def apply_legacy_retirement(
    spec: LegacyArchiveSpec, *, expected_fingerprint: str,
    owned_verified_plan: Mapping[str, Any] | None = None, **options: Any
) -> dict[str, Any]:
    if owned_verified_plan is not None and owned_verified_plan.get("quarantine_resume") is True:
        return _apply_quarantine_resume(spec, expected_fingerprint=expected_fingerprint,
                                       owned_verified_plan=owned_verified_plan, **options)
    state_root = Path(options["state_root"])
    state_path = _state_path(state_root, spec.dataset)
    lock_path = state_root / "retirements" / f"{spec.dataset}.lock"
    with _exclusive_lock(lock_path):
        if owned_verified_plan is not None:
            if (options.get("manual_immediate") is not True
                    or owned_verified_plan.get("plan_fingerprint") != expected_fingerprint
                    or owned_verified_plan.get("dataset") != spec.dataset
                    or not owned_verified_plan.get("full_verification")):
                raise SnapshotError("owned legacy retirement requires its exact manual full plan")
            plan = plan_legacy_retirement(spec, **options,
                _owned_full_verification=owned_verified_plan["full_verification"])
        else:
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
                     manual_capture=plan["manual_capture"],
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
        state.update(state="retiring", quarantine=str(quarantine), snapshot_id=plan["snapshot_id"],
                     manifest_sha256=plan["manifest_sha256"])
        atomic_write_json(state_path, state)
        if hot_tree.is_dir():
            os.replace(hot_tree, quarantine / "hot")
        os.replace(source, quarantine / "source")
        archive = spec.stage_root / spec.dataset / "archive"
        verify_archive_directory(archive, quarantine / "source", spec=spec,
                                 manual_capture=plan["manual_capture"],
                                 artifact_root=Path(options["artifact_root"]),
                                 repo_root=Path(options["repo_root"]))
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
        result = {**plan, "action": "retired", "deleted": True,
                  "reclaimed_allocated_file_bytes": reclaimed_bytes, "unlink_peer_proof": unlink_peer_proof}
        state.update(state="cold-only", retired_at=_utc_iso_from_ns(time.time_ns()),
                     reclaimed_allocated_file_bytes=reclaimed_bytes, retirement_receipt=result)
        state.pop("quarantine", None)
        atomic_write_json(state_path, state)
        return result


def _quarantine_resume_binding(spec: LegacyArchiveSpec, plan: Mapping[str, Any],
                               **options: Any) -> tuple[Path, dict[str, Any]]:
    """Only the original, complete interrupted transaction may be resumed."""
    state_root = Path(options["state_root"]).absolute()
    state = _read_state(_state_path(state_root, spec.dataset))
    source = Path(options["artifact_root"]).resolve() / spec.relative_root
    hot = Path(options["hot_root"]).resolve() / spec.relative_root
    if (options.get("manual_immediate") is not True or plan.get("manual_immediate") is not True
            or plan.get("manual_capture", False) != options.get("manual_capture", False)
            or plan.get("dataset") != spec.dataset or plan.get("relative_root") != spec.relative_root
            or plan.get("source") != str(source) or plan.get("hot_tree") != str(hot)
            or state is None or state.get("state") != "retiring"
            or state.get("dataset") != spec.dataset or state.get("relative_root") != spec.relative_root
            or state.get("plan_fingerprint") != plan.get("plan_fingerprint")
            or state.get("snapshot_id") != plan.get("snapshot_id")
            or state.get("manifest_sha256", plan.get("manifest_sha256")) != plan.get("manifest_sha256")
            or source.exists() or source.is_symlink() or hot.exists() or hot.is_symlink()
            or source.resolve(strict=False) != source or hot.resolve(strict=False) != hot):
        raise SnapshotError("interrupted legacy retirement identity or original paths changed")
    quarantine = Path(str(state.get("quarantine", "")))
    parent = state_root / "retirements" / "quarantine" / spec.dataset
    original_binding = plan.get("full_verification", {}).get("binding", {})
    if original_binding.get("dataset") != spec.dataset or "hot" not in original_binding:
        raise SnapshotError("interrupted legacy retirement lacks its original full binding")
    expected_names = {"source"} | ({"hot"} if original_binding["hot"] != "absent" else set())
    if (quarantine.parent != parent or quarantine.resolve(strict=False) != quarantine
            or not quarantine.is_dir() or quarantine.is_symlink()
            or {p.name for p in parent.iterdir()} != {quarantine.name}
            or {p.name for p in quarantine.iterdir()} != expected_names
            or any(not (quarantine/name).is_dir() or (quarantine/name).is_symlink()
                   or (quarantine/name).resolve() != quarantine/name for name in expected_names)
            or any((quarantine/name).stat().st_dev != state_root.stat().st_dev for name in expected_names)):
        raise SnapshotError("interrupted legacy quarantine is incomplete, redirected or has unknown names")
    archive = spec.stage_root / spec.dataset / "archive"
    resolved = resolve_latest_packed(Path(options["sync_root"]), spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if (resolved.manifest["snapshot_id"] != plan["snapshot_id"]
            or resolved.manifest_sha256 != plan["manifest_sha256"]
            or metadata.get("transport_role") != "legacy-quarantine-archive"
            or metadata.get("deployable") != "false"
            or metadata.get("source_relative_root") != spec.relative_root
            or metadata.get("legacy_manifest_sha256") != sha256_file(archive/"legacy_archive_manifest.json")):
        raise SnapshotError("interrupted legacy retirement fixed cold release changed")
    binding = _full_verification_binding(spec, quarantine/"source", quarantine/"hot",
                                         archive, Path(options["sync_root"]))
    binding.update(quarantine=str(quarantine), retirement_state=state)
    return quarantine, binding


def plan_legacy_quarantine_resume(spec: LegacyArchiveSpec, prior_plan: Mapping[str, Any],
                                  **options: Any) -> dict[str, Any]:
    """Fresh full audit outside the common mutation owner; no rename or unlink."""
    quarantine, binding = _quarantine_resume_binding(spec, prior_plan, **options)
    _assert_unlink_gates(prior_plan, quarantine, **options)
    sync_root = Path(options["sync_root"])
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    archive = spec.stage_root/spec.dataset/"archive"
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive, **_native_read_options(sync_root))
    proof = verify_archive_directory(archive, quarantine/"source", spec=spec,
        manual_capture=prior_plan.get("manual_capture", False),
        artifact_root=Path(options["artifact_root"]), repo_root=Path(options["repo_root"]))
    inventory = [{"kind":"file", "path":r["path"], "size":r["source"]["size"],
                  "sha256":r["original_sha256"]} for r in proof["manifest"]["files"]]
    _hot_mirror(quarantine/"source", quarantine/"hot", inventory)
    cold = verify_cold_resilience(sync_root, resolved, Path(options["backup_config"]))
    peer = _assert_unlink_gates(prior_plan, quarantine, **options)
    if _quarantine_resume_binding(spec, prior_plan, **options)[1] != binding:
        raise SnapshotError("interrupted legacy retirement changed during full audit")
    return {**prior_plan, **cold, "quarantine_resume":True, "peer_proof":peer,
            "full_verification":{"contract":"owned-legacy-quarantine-resume-v1",
                "binding":binding, "verified_at_epoch":time.time()}}


def _apply_quarantine_resume(spec: LegacyArchiveSpec, *, expected_fingerprint: str,
                              owned_verified_plan: Mapping[str, Any], **options: Any) -> dict[str, Any]:
    state_root = Path(options["state_root"])
    with _exclusive_lock(state_root/"retirements"/f"{spec.dataset}.lock"):
        plan = owned_verified_plan
        full = plan.get("full_verification", {})
        # The original binding is needed for the exact expected source/hot set.
        original = dict(plan)
        original["full_verification"] = {"binding":{
            "dataset":spec.dataset, "hot":full.get("binding", {}).get("hot")}}
        if (plan.get("plan_fingerprint") != expected_fingerprint
                or full.get("contract") != "owned-legacy-quarantine-resume-v1"
                or not 0 <= time.time()-float(full.get("verified_at_epoch", 0)) <= 1500):
            raise SnapshotError("interrupted legacy retirement full audit expired or differs")
        quarantine, binding = _quarantine_resume_binding(spec, original, **options)
        if binding != full["binding"]:
            raise SnapshotError("interrupted legacy retirement generation changed before unlink")
        archive = spec.stage_root/spec.dataset/"archive"
        proof = verify_archive_directory(archive, quarantine/"source", spec=spec,
            manual_capture=plan.get("manual_capture", False),
            artifact_root=Path(options["artifact_root"]), repo_root=Path(options["repo_root"]))
        inventory = [{"kind":"file", "path":r["path"], "size":r["source"]["size"],
                      "sha256":r["original_sha256"]} for r in proof["manifest"]["files"]]
        _hot_mirror(quarantine/"source", quarantine/"hot", inventory)
        resolved = resolve_latest_packed(Path(options["sync_root"]), spec.dataset)
        verify_cold_resilience(Path(options["sync_root"]), resolved, Path(options["backup_config"]))
        peer = _assert_unlink_gates(plan, quarantine, **options)
        if (_quarantine_resume_binding(spec, original, **options)[1] != binding
                or not 0 <= time.time()-float(full["verified_at_epoch"]) <= 1500):
            raise SnapshotError("interrupted legacy retirement changed or expired after quarantine audit")
        reclaimed = _reclaimable_file_bytes(quarantine/"source", quarantine/"hot")
        if (quarantine/"hot").is_dir():
            shutil.rmtree(quarantine/"hot")
        shutil.rmtree(quarantine/"source")
        quarantine.rmdir()
        result = {**plan, "action":"retired", "deleted":True, "resumed_quarantine":True,
                  "reclaimed_allocated_file_bytes":reclaimed, "unlink_peer_proof":peer}
        state = _read_state(_state_path(state_root, spec.dataset))
        assert state is not None
        state.update(state="cold-only", retired_at=_utc_iso_from_ns(time.time_ns()),
                     reclaimed_allocated_file_bytes=reclaimed, retirement_receipt=result)
        state.pop("quarantine", None)
        atomic_write_json(_state_path(state_root, spec.dataset), state)
        return result
