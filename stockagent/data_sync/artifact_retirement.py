"""Fail-closed transition of one complete artifact run from hot to cold-only.

The packed release and independent backup are never modified here.  A run is
retired only after an explicit seven-day lease (or a one-shot manual age bypass), exact source/mirror comparison,
independent D verification, and the configured local/peer transport proof.
Dataset materialization is handled separately by
the existing seven-day materialized-cache controller.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.cold_artifacts import (
    COLD_ACTIVATION_SCHEMA_VERSION,
    ColdArtifactSpec,
    rebuild_cold_ignore,
)
from stockagent.data_sync.cold_primary import verify_cold_resilience
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    _utc_iso_from_ns,
    atomic_write_json,
    sha256_file,
)
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import (
    _load_inventory,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    scan_tree,
    verify_packed_snapshot,
)


RETIREMENT_SCHEMA_VERSION = 1


def load_retirement_peer_names(path: Path, *, authority_node_id: str) -> tuple[str, ...]:
    """Keep local hot retirement independent of C cold-object retention policy."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1 or raw.get("authority_node_id") != authority_node_id:
        raise SnapshotError("artifact retirement policy authority/schema mismatch")
    names = raw.get("required_peer_names")
    if not isinstance(names, list) or any(
        not isinstance(name, str) or not name.strip() for name in names
    ) or len(names) != len(set(names)):
        raise SnapshotError("invalid artifact retirement peer names")
    return tuple(names)


def _state_path(state_root: Path, dataset: str) -> Path:
    return state_root / "retirements" / f"{dataset}.json"


def _read_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != RETIREMENT_SCHEMA_VERSION:
        raise SnapshotError(f"unsupported artifact retirement state: {path}")
    return value


def _validate_roots(artifact_root: Path, hot_root: Path, sync_root: Path) -> None:
    roots = (artifact_root.resolve(), hot_root.resolve(), sync_root.resolve())
    if len(set(roots)) != 3 or any(
        first in second.parents or second in first.parents
        for index, first in enumerate(roots)
        for second in roots[index + 1 :]
    ):
        raise SnapshotError("artifact, hot transport, and packed roots must be disjoint")


def _running_hot_bridge(hot_root: Path) -> bool:
    root_arg = os.fsencode(str(hot_root.resolve()))
    for cmdline in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            arguments = cmdline.read_bytes().split(b"\0")
        except OSError:
            continue
        if any(item.endswith(b"/run_live_artifact_sync.py") for item in arguments):
            if root_arg in arguments:
                return True
    return False


def _hot_mirror(
    source: Path, hot_tree: Path, inventory: list[dict[str, Any]]
) -> dict[str, Any]:
    expected = {row["path"]: row for row in inventory if row["kind"] == "file"}
    if hot_tree.is_symlink():
        raise SnapshotError(f"hot artifact root is a symlink: {hot_tree}")
    if not hot_tree.exists():
        return {"files": 0, "logical_bytes": 0, "fingerprint": "absent"}
    if not hot_tree.is_dir():
        raise SnapshotError(f"hot artifact root is not a directory: {hot_tree}")
    digest = hashlib.sha256()
    files = logical_bytes = 0
    for directory, dirnames, filenames in os.walk(hot_tree, followlinks=False):
        dirnames.sort()
        filenames.sort()
        for name in dirnames:
            if (Path(directory) / name).is_symlink():
                raise SnapshotError("hot artifact mirror contains a symlink directory")
        for name in filenames:
            hot_file = Path(directory) / name
            relative = hot_file.relative_to(hot_tree).as_posix()
            row = expected.get(relative)
            info = hot_file.lstat()
            if row is None or not stat.S_ISREG(info.st_mode):
                raise SnapshotError(f"hot mirror has unverified content: {hot_file}")
            source_file = source / relative
            source_info = source_file.stat(follow_symlinks=False)
            if info.st_size != int(row["size"]):
                raise SnapshotError(f"hot mirror size differs: {hot_file}")
            if (info.st_dev, info.st_ino) != (source_info.st_dev, source_info.st_ino):
                if sha256_file(hot_file) != row["sha256"]:
                    raise SnapshotError(f"hot mirror checksum differs: {hot_file}")
            digest.update(
                f"{relative}\0{info.st_dev}:{info.st_ino}:{info.st_size}:"
                f"{info.st_mtime_ns}:{info.st_ctime_ns}\n".encode("utf-8")
            )
            files += 1
            logical_bytes += info.st_size
    return {
        "files": files,
        "logical_bytes": logical_bytes,
        "fingerprint": digest.hexdigest(),
    }


def _reclaimable_file_bytes(*trees: Path) -> int:
    """Count allocated file blocks only when all inode names will be removed.

    Source and the old transport often share inodes. External hard links must
    not be counted as recovered space; later retirement of their last name may
    count those blocks instead. Directory blocks and filesystem overhead are
    deliberately excluded.
    """

    inodes: dict[tuple[int, int], list[int]] = {}
    for tree in trees:
        if not tree.exists():
            continue
        for directory, _dirnames, filenames in os.walk(tree, followlinks=False):
            for name in filenames:
                info = (Path(directory) / name).lstat()
                if not stat.S_ISREG(info.st_mode):
                    raise SnapshotError("retirement space audit found a non-regular file")
                key = (info.st_dev, info.st_ino)
                record = inodes.setdefault(key, [info.st_nlink, info.st_blocks * 512, 0])
                if record[:2] != [info.st_nlink, info.st_blocks * 512]:
                    raise SnapshotError("retirement inode changed during space audit")
                record[2] += 1
    return sum(allocated for links, allocated, removed in inodes.values() if links == removed)


def _assert_unlink_gates(plan: Mapping[str, Any], quarantine: Path, **options: Any) -> dict[str, Any]:
    """Refresh usage and transport after the potentially long rename audit.

    Failure leaves the exact hot bytes in quarantine for review, never unlinks
    them or an immutable cold object. Original service paths are checked even
    though the verified files now live under quarantine.
    """

    source = Path(plan["source"])
    artifact_root = Path(options["artifact_root"])
    refs = artifact_process_references(source, artifact_root)
    for path in (Path(plan["hot_tree"]), quarantine):
        refs.extend(process_references(path))
    if refs:
        raise SnapshotError("retirement post-rename process references; quarantine retained")
    services = artifact_service_references((source,), options.get("repo_root", artifact_root.parent))
    if services[str(source)]:
        raise SnapshotError("retirement post-rename service reference; quarantine retained")
    from stockagent.data_sync.remote_legacy_return import active_configuration_references

    if active_configuration_references(source, options.get("repo_root", artifact_root.parent)):
        raise SnapshotError("retirement post-rename active configuration reference; quarantine retained")
    if str(plan["snapshot_id"]) in _pinned_snapshot_ids(Path(options["materialized_root"])):
        raise SnapshotError("retirement post-rename release pinned; quarantine retained")
    if not options["bridge_inactive"] or _running_hot_bridge(Path(options["hot_root"])):
        raise SnapshotError("retirement post-rename hot bridge active; quarantine retained")
    probe = options.get("peer_probe")
    proof = probe() if probe is not None else options["peer_proof"]
    peers = {row.get("name"): row for row in proof.get("peers", []) if isinstance(row, Mapping)}
    if proof.get("ok") is not True or any(
        peers.get(name, {}).get("ok") is not True for name in options.get("required_peer_names", ())
    ):
        raise SnapshotError("retirement post-rename transport not converged; quarantine retained")
    try:
        checked = datetime.fromisoformat(str(proof["checked_at"]))
        if checked.tzinfo is None:
            raise ValueError("missing timezone")
        age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
        if not -5 <= age <= 300:
            raise ValueError("stale transport proof")
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotError("retirement post-rename transport proof stale; quarantine retained") from exc
    return dict(proof)


def plan_artifact_retirement(
    spec: ColdArtifactSpec,
    *,
    artifact_root: Path,
    hot_root: Path,
    sync_root: Path,
    materialized_root: Path,
    state_root: Path,
    backup_config: Path,
    peer_proof: Mapping[str, Any],
    required_peer_names: tuple[str, ...],
    bridge_inactive: bool,
    retention_days: float = 7.0,
    manual_immediate: bool = False,
    peer_probe: Callable[[], Mapping[str, Any]] | None = None,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Return a read-only, fingerprinted plan for one registered *full* run."""

    if spec.maximum_file_bytes is not None:
        raise SnapshotError("partial cold artifact releases cannot retire a whole run")
    if retention_days <= 0:
        raise SnapshotError("artifact retirement lease must be positive")
    if type(manual_immediate) is not bool:
        raise SnapshotError("manual immediate retirement must be an explicit boolean")
    _validate_roots(artifact_root, hot_root, sync_root)
    artifact_root = artifact_root.resolve()
    hot_root = hot_root.resolve()
    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    state_root = state_root.resolve()
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    relative = PurePosixPath(spec.relative_root)
    source = artifact_root.joinpath(*relative.parts)
    hot_tree = hot_root.joinpath(*relative.parts)
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError(f"artifact source is not a real directory: {source}")
    if source.resolve() != source or hot_tree.resolve(strict=False) != hot_tree:
        raise SnapshotError("artifact retirement path is redirected by a symlink")
    quarantine_device = (
        state_root if state_root.exists() else state_root.parent
    ).stat().st_dev
    if source.stat().st_dev != quarantine_device:
        raise SnapshotError("artifact source and retirement quarantine cross filesystems")
    if hot_tree.exists() and hot_tree.stat().st_dev != quarantine_device:
        raise SnapshotError("hot mirror and retirement quarantine cross filesystems")
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if (
        metadata.get("artifact_relative_root") != spec.relative_root
        or metadata.get("transport_role") != "cold-full-run"
        or metadata.get("source_lifecycle_validated") != "true"
        or metadata.get("completion_contract") != spec.completion_contract
    ):
        raise SnapshotError("latest cold release does not cover this complete artifact root")
    verify_packed_snapshot(sync_root, resolved, materialized_path=source)
    source_tree = scan_tree(source)
    inventory = _load_inventory(sync_root, resolved.manifest)
    mirror = _hot_mirror(source, hot_tree, inventory)

    cold_proof = verify_cold_resilience(sync_root, resolved, backup_config)

    state = _read_state(_state_path(state_root, spec.dataset))
    if state is not None and (
        state.get("dataset") != spec.dataset
        or state.get("artifact_relative_root") != spec.relative_root
    ):
        raise SnapshotError("retirement state identity mismatch")
    last_used_ns = int(state["last_used_ns"]) if state else 0
    expiry_ns = (last_used_ns or current_ns) + int(
        retention_days * 86_400 * 1_000_000_000
    )
    references = artifact_process_references(source, artifact_root / relative.parts[0])
    service_references = artifact_service_references((source,), artifact_root.parent)[str(source)]
    if hot_tree.is_dir():
        references.extend(process_references(hot_tree))
    blockers: list[str] = []
    if state is None:
        if not manual_immediate:
            blockers.append("seven-day-use-lease-not-enrolled")
    elif state.get("state") == "retiring":
        blockers.append("unfinished-retirement-state-requires-audit")
    elif state.get("state") != "hot-enrolled":
        blockers.append("retirement-state-is-not-hot-enrolled")
    elif current_ns < expiry_ns and not manual_immediate:
        blockers.append("seven-day-use-lease-active")
    if references:
        blockers.append("artifact-has-process-references")
    if service_references:
        blockers.append("enabled-service-references-artifact")
    if str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(materialized_root):
        blockers.append("artifact-release-is-pinned")
    current_peer_proof = peer_probe() if peer_probe is not None else peer_proof
    observed_peers = {
        str(row.get("name")): row
        for row in current_peer_proof.get("peers", [])
        if isinstance(row, Mapping)
    }
    if current_peer_proof.get("ok") is not True or any(
        observed_peers.get(name, {}).get("ok") is not True
        for name in required_peer_names
    ):
        blockers.append("intended-cold-peers-not-converged")
    try:
        checked = datetime.fromisoformat(str(current_peer_proof["checked_at"]))
        if checked.tzinfo is None:
            raise ValueError("peer proof timestamp has no timezone")
        age = (datetime.now(timezone.utc) - checked.astimezone(timezone.utc)).total_seconds()
        if age < -5 or age > 300:
            blockers.append("peer-proof-stale")
    except (KeyError, TypeError, ValueError):
        blockers.append("peer-proof-missing-timestamp")
    if not bridge_inactive or _running_hot_bridge(hot_root):
        blockers.append("hot-bridge-must-be-stopped-for-retirement")
    quarantine_parent = state_root / "retirements" / "quarantine" / spec.dataset
    if quarantine_parent.exists() and any(quarantine_parent.iterdir()):
        blockers.append("unfinished-retirement-quarantine-requires-audit")

    identity = {
        "dataset": spec.dataset,
        "artifact_relative_root": spec.relative_root,
        "required_peer_names": list(required_peer_names),
        "snapshot_id": resolved.manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "source_stability_sha256": source_tree["stability_fingerprint_sha256"],
        "hot_fingerprint": mirror["fingerprint"],
        "last_used_ns": last_used_ns,
        "manual_immediate": manual_immediate,
        "retention_days": retention_days,
    }
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        **identity,
        "plan_fingerprint": fingerprint,
        "source": str(source),
        "hot_tree": str(hot_tree),
        "source_logical_bytes": resolved.manifest["source"]["logical_bytes"],
        "hot_mirror": mirror,
        "lease_expires_at": _utc_iso_from_ns(expiry_ns),
        "process_references": references,
        "service_references": service_references,
        **cold_proof,
        "peer_proof": dict(current_peer_proof),
        "blockers": blockers,
        "apply_ready": not blockers,
    }


def apply_artifact_retirement(
    spec: ColdArtifactSpec,
    *,
    expected_fingerprint: str,
    **plan_options: Any,
) -> dict[str, Any]:
    """Recheck under a per-dataset lock, then unlink only this exact hot run."""

    state_root = Path(plan_options["state_root"])
    state_path = _state_path(state_root, spec.dataset)
    lock_path = state_root / "retirements" / f"{spec.dataset}.lock"
    with _exclusive_lock(lock_path):
        plan = plan_artifact_retirement(spec, **plan_options)
        if plan["plan_fingerprint"] != expected_fingerprint:
            raise SnapshotError("artifact retirement plan changed; run dry-run again")
        if "seven-day-use-lease-not-enrolled" in plan["blockers"]:
            now_ns = int(plan_options.get("now_ns") or time.time_ns())
            state = {
                "schema_version": RETIREMENT_SCHEMA_VERSION,
                "dataset": spec.dataset,
                "artifact_relative_root": spec.relative_root,
                "state": "hot-enrolled",
                "last_used_ns": now_ns,
                "last_used_at": _utc_iso_from_ns(now_ns),
                "enrolled_at": _utc_iso_from_ns(now_ns),
            }
            atomic_write_json(state_path, state)
            return {**plan, "action": "enrolled", "deleted": False}
        if plan["process_references"] and not plan["manual_immediate"]:
            state = _read_state(state_path)
            assert state is not None
            now_ns = int(plan_options.get("now_ns") or time.time_ns())
            state.update(last_used_ns=now_ns, last_used_at=_utc_iso_from_ns(now_ns))
            atomic_write_json(state_path, state)
            return {**plan, "action": "renewed-in-use", "deleted": False}
        if plan["blockers"]:
            raise SnapshotError("artifact retirement blocked: " + ", ".join(plan["blockers"]))

        # A manual age bypass never fabricates an expired lease. Preserve an
        # existing lease or record the actual enrollment time for the audit.
        state = _read_state(state_path)
        if state is None:
            now_ns = int(plan_options.get("now_ns") or time.time_ns())
            state = {
                "schema_version": RETIREMENT_SCHEMA_VERSION,
                "dataset": spec.dataset,
                "artifact_relative_root": spec.relative_root,
                "state": "hot-enrolled",
                "last_used_ns": now_ns,
                "last_used_at": _utc_iso_from_ns(now_ns),
                "enrolled_at": _utc_iso_from_ns(now_ns),
            }
        state.update(manual_immediate=plan["manual_immediate"],
                     plan_fingerprint=plan["plan_fingerprint"])
        atomic_write_json(state_path, state)

        # The directory-level tombstone is local to this node. Syncthing's
        # (?d)/path ignores the directory and descendants; the hot bridge uses
        # the same prefix to prevent the old transport from rehydrating it.
        activation_root = state_root / "activations"
        atomic_write_json(
            activation_root / f"{spec.dataset}.json",
            {
                "schema_version": COLD_ACTIVATION_SCHEMA_VERSION,
                "dataset": spec.dataset,
                "snapshot_id": plan["snapshot_id"],
                "manifest_sha256": plan["manifest_sha256"],
                "artifact_relative_root": spec.relative_root,
                "ignored_file_paths": [spec.relative_root],
                "retired_at": _utc_iso_from_ns(time.time_ns()),
            },
        )
        rebuild_cold_ignore(Path(plan_options["hot_root"]), activation_root)
        hot_tree = Path(plan["hot_tree"])
        source = Path(plan["source"])
        quarantine = (
            state_root
            / "retirements"
            / "quarantine"
            / spec.dataset
            / f"{time.time_ns()}-{uuid.uuid4().hex}"
        )
        quarantine.mkdir(parents=True, exist_ok=False)
        state = _read_state(state_path)
        assert state is not None
        state.update(
            state="retiring",
            snapshot_id=plan["snapshot_id"],
            manifest_sha256=plan["manifest_sha256"],
            quarantine=str(quarantine),
        )
        atomic_write_json(state_path, state)
        if hot_tree.is_dir():
            os.replace(hot_tree, quarantine / "hot")
        os.replace(source, quarantine / "source")
        resolved = resolve_packed_snapshot_id(
            Path(plan_options["sync_root"]), spec.dataset, str(plan["snapshot_id"])
        )
        verify_packed_snapshot(
            Path(plan_options["sync_root"]),
            resolved,
            materialized_path=quarantine / "source",
        )
        if (quarantine / "hot").exists():
            _hot_mirror(
                quarantine / "source",
                quarantine / "hot",
                _load_inventory(Path(plan_options["sync_root"]), resolved.manifest),
            )
        verify_cold_resilience(Path(plan_options["sync_root"]), resolved, Path(plan_options["backup_config"]))
        unlink_peer_proof = _assert_unlink_gates(plan, quarantine, **plan_options)
        reclaimed_bytes = _reclaimable_file_bytes(quarantine / "source", quarantine / "hot")
        if (quarantine / "hot").exists():
            shutil.rmtree(quarantine / "hot")
        shutil.rmtree(quarantine / "source")
        quarantine.rmdir()
        state.update(
            state="cold-only",
            snapshot_id=plan["snapshot_id"],
            manifest_sha256=plan["manifest_sha256"],
            retired_at=_utc_iso_from_ns(time.time_ns()),
            reclaimed_allocated_file_bytes=reclaimed_bytes,
        )
        state.pop("quarantine", None)
        atomic_write_json(state_path, state)
        return {**plan, "action": "retired", "deleted": True,
                "reclaimed_allocated_file_bytes": reclaimed_bytes, "unlink_peer_proof": unlink_peer_proof}
