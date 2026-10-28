"""Fail-closed transition of one complete artifact run from hot to cold-only.

The packed release and independent backup are never modified here.  A run is
retired only after an explicit seven-day lease (or a one-shot manual age bypass), exact source/mirror comparison,
independent D verification, and the configured local/peer transport proof.
Dataset materialization is handled separately by
the existing seven-day materialized-cache controller.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import resource
import shutil
import stat
import time
import uuid
from dataclasses import dataclass
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
    return _retirement_peer_names(raw, authority_node_id=authority_node_id)


def _retirement_peer_names(raw, *, authority_node_id: str) -> tuple[str, ...]:
    if not isinstance(raw, dict):
        raise SnapshotError("artifact retirement policy must be an object")
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


def _reclaimable_file_bytes(*trees: Path, allow_symlinks: bool = False) -> int:
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
                if stat.S_ISLNK(info.st_mode) and allow_symlinks:
                    # The approved archive preserves link bytes separately.
                    # Do not count a link's target or its own inode blocks.
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise SnapshotError("retirement space audit found a non-regular file")
                key = (info.st_dev, info.st_ino)
                record = inodes.setdefault(key, [info.st_nlink, info.st_blocks * 512, 0])
                if record[:2] != [info.st_nlink, info.st_blocks * 512]:
                    raise SnapshotError("retirement inode changed during space audit")
                record[2] += 1
    return sum(allocated for links, allocated, removed in inodes.values() if links == removed)


def _assert_unlink_gates(
    plan: Mapping[str, Any], quarantine: Path, *,
    _process_scope_root: Path | None = None, **options: Any,
) -> dict[str, Any]:
    """Refresh usage and transport after the potentially long rename audit.

    Failure leaves the exact hot bytes in quarantine for review, never unlinks
    them or an immutable cold object. Original service paths are checked even
    though the verified files now live under quarantine.
    """

    source = Path(plan["source"])
    artifact_root = Path(options["artifact_root"])
    # A reviewed legacy profile may have a narrower ancestor-consumer domain
    # than its physical capture root. Keep that domain identical before and
    # after rename; never narrow the exact source/hot/quarantine FD/mmap scans.
    process_scope = (artifact_root if _process_scope_root is None else _process_scope_root).resolve()
    if (not process_scope.is_relative_to(artifact_root.resolve())
            or process_scope not in source.resolve().parents):
        raise SnapshotError("retirement process scope is outside its source ancestry")
    refs = artifact_process_references(source, process_scope)
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


def _closed_raw_usage(source, members, materialized_root, resolved, repo_root, *, quarantine=None,
                      _dataset=None, _expected_pin=None, _quarantine_names=None):
    """Reuse the existing process/config/service and managed-cache owners."""
    from stockagent.data_sync.artifact_maintenance import _ancestor_process_references
    from stockagent.data_sync.materialized_cache import process_references_many, _lease_path, _target_path
    from stockagent.data_sync.remote_legacy_return import active_configuration_references_many
    from stockagent.data_sync import closed_source as closed

    originals = [source / name for name in members]
    targets = [source / "raw", *originals]
    dataset = closed.DATASET if _dataset is None else _dataset
    actuals = originals if quarantine is None else [
        quarantine / (_quarantine_names[name] if _quarantine_names is not None else Path(name).name)
        for name in members]
    if quarantine is not None:
        targets.append(quarantine)
    inodes = {(info.st_dev, info.st_ino) for path in actuals if path.exists()
              for info in (path.lstat(),)}
    processes = _ancestor_process_references(targets, source,
        process_references_many(targets, inode_keys=inodes))
    services = artifact_service_references(originals, repo_root)
    configurations = active_configuration_references_many(originals, repo_root)
    pins = []
    # Validate all pin schemas through the canonical owner first. The one
    # closed-source pin protects D bytes; it does not pin the local raw names.
    protected = _pinned_snapshot_ids(materialized_root)
    if str(resolved.manifest["snapshot_id"]) in protected:
        expected = closed._pin_path(materialized_root) if _expected_pin is None else _expected_pin
        for path in materialized_root.rglob("*.pin.json"):
            if ".cache-state" in path.parts or path == expected:
                continue
            value = json.loads(closed._read(path, limit=16 * 1024**2))
            if value.get("manifest", {}).get("dataset") == dataset:
                pins.append(str(path))
    leases = []
    lease_dir = _lease_path(materialized_root, dataset,
                            resolved.manifest["snapshot_id"]).parent
    if lease_dir.resolve() != lease_dir:
        raise SnapshotError("closed source managed-use lease directory is redirected")
    for path in lease_dir.glob("*.json"):
        value = closed._json(path)
        if (value.get("schema_version") not in {1, 2} or value.get("dataset") != dataset
                or type(value.get("expires_ns")) is not int or value["expires_ns"] < 1
                or value.get("snapshot_id") != path.stem or value.get("state") not in {"hot", "cold"}
                or value.get("target") != str(_target_path(materialized_root, dataset, path.stem))):
            raise SnapshotError("closed source has an unknown managed-use lease")
        if value["expires_ns"] > time.time_ns():
            leases.append(str(path))
    return {"process_references": processes, "service_references": services,
            "configuration_references": configurations, "additional_pins": pins,
            "active_leases": leases}


def _closed_raw_usage_blockers(usage):
    return [name for name, value in usage.items()
            if (any(value.values()) if isinstance(value, dict) else bool(value))]


def _closed_raw_policy(path: Path, authority: str, dataset: str) -> tuple[str, tuple[str, ...]]:
    from stockagent.data_sync import closed_source as closed
    content = closed._read(path, limit=64 * 1024)
    value = json.loads(content)
    names = _retirement_peer_names(value, authority_node_id=authority)
    datasets = value.get("closed_source_raw_datasets")
    if (not isinstance(datasets, list) or any(not isinstance(name, str) for name in datasets)
            or len(set(datasets)) != len(datasets)
            or datasets != [closed.DATASET] or dataset not in datasets):
        raise SnapshotError("closed source raw retirement is not enrolled in the existing policy")
    return hashlib.sha256(content).hexdigest(), names


def _closed_raw_code() -> dict[str, str]:
    root = Path(__file__).resolve().parents[2]
    names = ("artifact_retirement", "closed_source", "packed_snapshots", "cold_primary",
             "packed_retention", "materialized_cache", "artifact_maintenance",
             "artifact_consumers", "remote_legacy_return", "desync_snapshots")
    return {"stockagent/data_sync/" + name + ".py": sha256_file(
        root / "stockagent/data_sync" / (name + ".py")) for name in names}


@dataclass(frozen=True)
class _RawRetirementContract:
    dataset: str
    contract: str
    receipt_path: str
    resolve: Callable
    owner: Callable
    policy: Callable
    implementation: Callable
    usage: Callable
    minimum_members: int
    maximum_members: int
    requires_runtime_admission: bool = False


_RETAINED_RAW_RETIREMENT = "retained_source_raw_retirement_v1"
_RETAINED_RAW_RECEIPT = "state/retained-raw-retirement.json"


class _RawRetirementCheckpoint:
    """Commit verified name changes after their directory entries are durable.

    The complete original plan is durable before the first mutation. A crash
    between checkpoints leaves a recoverable original plan, never a completed
    retirement: reconciliation verifies the actual names and the fixed D bytes.
    Do not flush partial/unverified operations on exception.
    """

    def __init__(self, path, journal, members):
        self.path, self.journal, self.members = path, journal, members
        self.pending = 0
        self.directories = set()

    def advance(self, *directories):
        self.directories.update(directories)
        self.pending += 1
        if self.pending >= self.members:
            self.flush()

    def flush(self):
        if not self.pending:
            return
        from stockagent.data_sync.desync_snapshots import _fsync_directory
        for directory in sorted(self.directories):
            _fsync_directory(directory)
        atomic_write_json(self.path, self.journal)
        self.pending = 0
        self.directories.clear()


def _retained_raw_policy(path, authority, dataset):
    from stockagent.data_sync import closed_source as closed
    content = closed._read(path, limit=64 * 1024)
    value = json.loads(content)
    peers = _retirement_peer_names(value, authority_node_id=authority)
    if dataset != "fred-crypto-macro" or value.get("retained_source_raw_datasets") != [dataset]:
        raise SnapshotError("retained raw retirement is not enrolled in the original policy")
    return hashlib.sha256(content).hexdigest(), peers


def _retained_raw_code():
    root = Path(__file__).resolve().parents[2]
    code = _closed_raw_code()
    names = ("stockagent/data_sync/retained_source.py", "stockagent/data_sync/dataset_migration.py",
        "stockagent/data_sync/cache_capacity.py", "stockagent/data_sync/completed_forward_capacity.py",
        "downloader/download_fred_crypto_macro_vintages.py", "downloader/fred_projection.py",
        "scripts/manage_lakehouse.py", "scripts/rollout_lakehouse_source_policy.py",
        "scripts/publish_data_releases.py", "scripts/deliver_vast_futures_preparation.py")
    code.update({name: sha256_file(root / name) for name in names})
    return code


def _retained_runtime_admitted(proof, source, dataset, release, implementation):
    """A binding alone is never admission of live readers and publishers.

    The canonical source owner supplies a fresh live probe with stable identity
    fields. No wall-clock sampling field belongs in the dry-run fingerprint.
    """
    if not isinstance(proof, dict):
        return False
    worker = proof.get("worker")
    if (proof.get("contract") != "retained_raw_runtime_admission_v1"
            or proof.get("state") != "accepted" or proof.get("source") != str(source)
            or proof.get("dataset") != dataset
            or proof.get("manifest_sha256") != release.manifest_sha256
            or proof.get("implementation_sha256") != implementation
            or proof.get("canonical_retained_reader_writer_verified") is not True
            or proof.get("old_publisher_fenced") is not True or not isinstance(worker, dict)
            or type(worker.get("pid")) is not int or worker["pid"] <= 1
            or type(worker.get("start_ticks")) is not int or worker["start_ticks"] < 1
            or not isinstance(worker.get("boot_id"), str) or len(worker["boot_id"]) != 36):
        return False
    from stockagent.data_sync.packed_snapshots import _validate_hash
    try:
        _validate_hash(proof.get("loaded_code_identity_sha256"), "retained loaded code identity")
        _validate_hash(proof.get("handoff_receipt_sha256"), "retained owner handoff")
    except (SnapshotError, TypeError):
        return False
    return True


def _retained_raw_resolution(source, config, dataset):
    from stockagent.data_sync import closed_source as closed
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync import packed_snapshots as packed
    bound = retained.load_retained_source(source, dataset, cold=config.sync_root,
        pin_root=config.materialized_root, read_originals=False)
    if bound is None:
        raise SnapshotError("source lacks its fixed retained-raw binding")
    release = bound.request.release
    with packed._open_inventory(config.sync_root, release.manifest) as inventory:
        packed._validate_inventory(release.manifest, inventory, retain_object_members=False)
        files = {}
        for row in inventory:
            if row["kind"] == "file":
                if len(files) >= 32768:
                    raise SnapshotError("retained raw release exceeds its complete inventory admission")
                files[row["path"]] = {"bytes": row["size"], "sha256": row["sha256"]}
    members = {name: files[name] for name in bound.request.names}
    present = any((source / name).exists() or (source / name).is_symlink() for name in members)
    complete = False
    path = source / _RETAINED_RAW_RECEIPT
    if path.exists() or path.is_symlink():
        receipt = json.loads(closed._read(path, limit=16 * 1024**2))
        if not isinstance(receipt, dict):
            raise SnapshotError("retained raw retirement receipt must be an object")
        recovery = receipt.get("post_unlink_original_recovery", {})
        evidence = receipt.get("unlinked_member_evidence", {})
        if (receipt.get("contract") != _RETAINED_RAW_RETIREMENT or receipt.get("state") != "cold-raw-retired"
                or receipt.get("source") != str(source) or receipt.get("snapshot_id") != release.manifest["snapshot_id"]
                or receipt.get("manifest_sha256") != release.manifest_sha256
                or receipt.get("binding_sha256") != bound.binding_sha256
                or receipt.get("retired_members") != members or receipt.get("deleted_names") != len(members)
                or not isinstance(recovery, dict) or recovery.get("manifest_sha256") != release.manifest_sha256
                or recovery.get("snapshot_id") != release.manifest["snapshot_id"]
                or recovery.get("inventory_sha256") != release.manifest["archive"]["inventory"]["sha256"]
                or recovery.get("independently_reconstructed_files") != len(files)
                or recovery.get("independently_reconstructed_bytes") != sum(row["bytes"] for row in files.values())
                or not isinstance(evidence, dict) or set(evidence) != set(members)
                or any(not isinstance(row, dict) or type(row.get("device")) is not int or row["device"] < 1
                    or type(row.get("inode")) is not int or row["inode"] < 1
                    or type(row.get("allocated_bytes")) is not int or row["allocated_bytes"] < 0
                    or row.get("links_before") != 1 or row.get("links_after") != 0
                    or row.get("descriptor_closed") is not True for row in evidence.values())
                or receipt.get("reclaimed_allocated_file_bytes") != sum(row["allocated_bytes"] for row in evidence.values())
                or not isinstance(receipt.get("retired_at_utc"), str)):
            raise SnapshotError("retained raw receipt lacks exact unlink and complete recovery evidence")
        try:
            if datetime.fromisoformat(receipt["retired_at_utc"]).utcoffset() is None:
                raise ValueError("missing retirement timezone")
        except ValueError as error:
            raise SnapshotError("retained raw retirement timestamp is invalid") from error
        complete = not present
    bound.check_binding_generation()
    return release, files, members, {"raw_retirement_complete": complete,
        "binding_sha256": bound.binding_sha256}


def _retained_raw_usage(source, members, materialized_root, resolved, repo_root, *, quarantine=None):
    from stockagent.data_sync import retained_source as retained
    dataset = resolved.manifest["dataset"]
    return _closed_raw_usage(source, members, materialized_root, resolved, repo_root,
        quarantine=quarantine, _dataset=dataset,
        _expected_pin=retained._pin_path(materialized_root, dataset, resolved.manifest_sha256),
        _quarantine_names={name: hashlib.sha256(name.encode()).hexdigest() for name in members})


def _retained_raw_contract(dataset):
    from stockagent.data_sync import retained_source as retained
    if dataset != "fred-crypto-macro":
        raise SnapshotError("retained raw retirement requires the explicitly qualified FRED source")
    return _RawRetirementContract(dataset, _RETAINED_RAW_RETIREMENT, _RETAINED_RAW_RECEIPT,
        lambda source, config: _retained_raw_resolution(source, config, dataset),
        retained.require_collector_owner, _retained_raw_policy, _retained_raw_code,
        _retained_raw_usage, 1, 4096, requires_runtime_admission=True)


def plan_retained_source_raw_retirement(*, dataset: str, runtime_probe=None, **options):
    """Audit only fixed bound originals, preserving all unbound C increments."""
    return _plan_source_raw_retirement(_retained_raw_contract(dataset), runtime_probe=runtime_probe, **options)


def apply_retained_source_raw_retirement(*, dataset: str, expected_fingerprint: str,
        runtime_probe=None,
        _ingress_owner: Path = Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock"), **options):
    contract = _retained_raw_contract(dataset)
    return _apply_source_raw_retirement(contract,
        plan_factory=lambda **kwargs: plan_retained_source_raw_retirement(dataset=dataset, **kwargs),
        runtime_probe=runtime_probe, expected_fingerprint=expected_fingerprint,
        _ingress_owner=_ingress_owner, **options)


def _closed_raw_contract():
    from stockagent.data_sync import closed_source as closed

    def resolve(source, config):
        bound = closed.resolve_closed_source(source, config.sync_root, config.materialized_root)
        if bound is None:
            raise SnapshotError("source has not adopted a fully recovered fixed closed release")
        resolved, storage = bound
        files = closed._files(config.sync_root, resolved)
        members = {name: row for name, row in files.items() if closed._RAW.fullmatch(name)}
        return resolved, files, members, storage

    return _RawRetirementContract(closed.DATASET, "closed_source_raw_retirement_v1",
        closed.RETIREMENT_RECEIPT, resolve, closed._source_owner, _closed_raw_policy,
        _closed_raw_code, _closed_raw_usage, 2, 2)


def _source_raw_cold_proof(contract, config, release):
    """Private history keeps its original D authority and never claims a backup."""
    from stockagent.data_sync import packed_snapshots as packed
    cold = config.sync_root
    if (contract.requires_runtime_admission
            and cold == Path("/srv/stockagent-packed/.local-state/private-history")):
        packed._check_packed_read_authority(cold)
        proof = verify_cold_resilience(Path("/srv/stockagent-packed"), release, config.backup_config)
        if proof.get("cold_primary_verified") is not True or proof.get("backup_verified") is not False:
            raise SnapshotError("private retained history requires its enrolled single-D authority")
        return {**proof, "private_history_guard_verified": True}
    return verify_cold_resilience(cold, release, config.backup_config)


def _source_raw_native_reads(cold):
    native = cold == Path("/srv/stockagent-packed")
    private = cold == Path("/srv/stockagent-packed/.local-state/private-history")
    return {"d_primary_native_blob_reads": native,
        "d_primary_native_pack_reads": native or private}


def plan_closed_source_raw_retirement(*, source: Path, config, policy_path: Path,
        state_root: Path, source_lock_fd: int, repo_root: Path,
        peer_probe: Callable[[], Mapping[str, Any]]) -> dict[str, Any]:
    """Audit the exact enrolled raw members; never retire a whole source tree."""
    return _plan_source_raw_retirement(_closed_raw_contract(), source=source, config=config,
        policy_path=policy_path, state_root=state_root, source_lock_fd=source_lock_fd,
        repo_root=repo_root, peer_probe=peer_probe)


def _plan_source_raw_retirement(contract: _RawRetirementContract, *, source: Path, config,
        policy_path: Path, state_root: Path, source_lock_fd: int, repo_root: Path,
        peer_probe: Callable[[], Mapping[str, Any]], runtime_probe=None) -> dict[str, Any]:
    """One raw retirement inventory and every original recovery/consumer gate."""
    from stockagent.data_sync import closed_source as closed
    from stockagent.data_sync.packed_retention import _validate_peer_proof

    source = closed._root(source)
    contract.owner(source, source_lock_fd)
    implementation = contract.implementation()
    state_root = state_root.absolute()
    if (state_root.resolve() != state_root or state_root.is_relative_to(source)
            or state_root.is_relative_to(config.sync_root)):
        raise SnapshotError("closed source retirement state must be separate and unredirected")
    policy_sha, peers = contract.policy(policy_path, config.authority_node_id, contract.dataset)
    if config.required_peer_names != peers:
        raise SnapshotError("closed source peer selection differs from the original retirement policy")
    resolved, files, members, storage = contract.resolve(source, config)
    if not contract.minimum_members <= len(members) <= contract.maximum_members:
        raise SnapshotError("source raw retirement member count is outside its enrolled scope")
    runtime = runtime_probe() if runtime_probe is not None else None
    identities = {}
    for name in members:
        path = source / name
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if path.resolve(strict=True) != path.absolute() or not stat.S_ISREG(info.st_mode):
                raise SnapshotError("source raw retirement member is redirected or non-regular")
            if (info.st_size != members[name]["bytes"]
                    or closed._hash(path, members[name]["bytes"]) != members[name]["sha256"]):
                raise SnapshotError("source raw changed from its fixed original; preserve the increment")
            identities[name] = {"signature": list(closed._signature(info)),
                                "allocated_bytes": info.st_blocks * 512}
            if closed._signature(path.lstat()) != closed._signature(info):
                raise SnapshotError("source raw changed during retirement inventory")
    state_path = state_root / "source-raw-retirements" / f"{contract.dataset}.json"
    state = (json.loads(closed._read(state_path, limit=16 * 1024**2)) if contract.requires_runtime_admission
             else closed._json(state_path)) if state_path.exists() or state_path.is_symlink() else None
    if state is not None and not isinstance(state, dict):
        raise SnapshotError("source raw retirement journal must be an object")
    blockers = []
    if contract.requires_runtime_admission and not _retained_runtime_admitted(
            runtime, source, contract.dataset, resolved, implementation):
        blockers.append("retained-reader-writer-runtime-not-admitted")
    if state and state.get("state") in {"retiring", "restoring-raw-names"}:
        blockers.append("unfinished-retirement-quarantine-requires-audit")
    allowed_states = ({"retiring", "restoring-raw-names", "rolled-back", "cold-raw-retired"}
        if contract.requires_runtime_admission else {"retiring", "cold-raw-retired"})
    if state and (state.get("contract") != contract.contract
                  or state.get("source") != str(source)
                  or state.get("manifest_sha256") != resolved.manifest_sha256
                  or state.get("state") not in allowed_states):
        raise SnapshotError("unknown or incompatible closed raw retirement journal")
    if state and state.get("state") == "cold-raw-retired" and identities:
        blockers.append("raw-names-reappeared-after-retirement")
    if len(identities) != len(members):
        if not (not identities and storage["raw_retirement_complete"] and state
                and state.get("state") == "cold-raw-retired"
                and state.get("manifest_sha256") == resolved.manifest_sha256):
            blockers.append("raw-name-set-differs-without-complete-retirement-receipt")
    if any(row["signature"][3] != 1 for row in identities.values()):
        blockers.append("raw-inode-has-unreviewed-hardlinks")
    fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    if fd_limit != resource.RLIM_INFINITY and len(identities) + len(list(Path("/proc/self/fd").iterdir())) + 32 > fd_limit:
        blockers.append("raw-retirement-file-descriptor-budget")
    if identities and state_root.exists() and source.stat().st_dev != state_root.stat().st_dev:
        blockers.append("raw-quarantine-crosses-filesystems")
    usage = contract.usage(source, members, config.materialized_root, resolved, repo_root)
    blockers.extend(_closed_raw_usage_blockers(usage))
    proof = _source_raw_cold_proof(contract, config, resolved)
    recovered = verify_packed_snapshot(config.sync_root, resolved, reconstruct_all=True,
        **_source_raw_native_reads(config.sync_root))
    if (recovered["independently_reconstructed_files"] != len(files)
            or recovered["independently_reconstructed_bytes"] != sum(row["bytes"] for row in files.values())):
        raise SnapshotError("closed source fixed release was not completely reconstructed")
    peer = peer_probe()
    try:
        _validate_peer_proof(config, peer)
    except SnapshotError as error:
        blockers.append(str(error))
    # A source or pin change during the potentially long cold decode is not
    # accepted from an earlier hash/census observation.
    current = contract.resolve(source, config)
    for name, record in identities.items():
        path = source / name
        if list(closed._signature(path.lstat())) != record["signature"]:
            raise SnapshotError("closed source raw changed during retirement audit")
    if current[0].manifest_sha256 != resolved.manifest_sha256:
        raise SnapshotError("closed source binding changed during retirement audit")
    if current[1:3] != (files, members) or runtime_probe is not None and runtime_probe() != runtime:
        raise SnapshotError("source raw scope or runtime changed during retirement audit")
    stable = {"contract": contract.contract, "source": str(source),
        "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256,
        "members": members, "source_identities": identities, "policy_sha256": policy_sha,
        "materialized_root": str(config.materialized_root), "state_root": str(state_root),
        "implementation_sha256": implementation}
    if contract.requires_runtime_admission:
        stable["runtime_admission"] = runtime
        stable["binding_sha256"] = storage["binding_sha256"]
        stable["journal_checkpoint_members"] = 32 if len(members) >= 32 else 1
    if contract.implementation() != implementation:
        raise SnapshotError("closed raw implementation changed during retirement audit")
    return {**stable, "plan_fingerprint": hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest(),
        "blockers": sorted(set(blockers)), "usage": usage, "cold_proof": proof,
        "complete_original_recovery": recovered, "peer_proof": peer,
        "raw_retirement_complete": storage["raw_retirement_complete"],
        "reclaimable_allocated_bytes": sum(row["allocated_bytes"] for row in identities.values()),
        "deleted_names": 0}


def apply_closed_source_raw_retirement(*, expected_fingerprint: str,
        _ingress_owner: Path = Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock"),
        **options) -> dict[str, Any]:
    """Original ingress owner, exact quarantine, final-link unlink and D readback."""
    return _apply_source_raw_retirement(_closed_raw_contract(),
        plan_factory=plan_closed_source_raw_retirement, expected_fingerprint=expected_fingerprint,
        _ingress_owner=_ingress_owner, **options)


def _raw_quarantine_name(contract, name):
    return (hashlib.sha256(name.encode()).hexdigest() if contract.requires_runtime_admission
            else Path(name).name)


def _apply_source_raw_retirement(contract: _RawRetirementContract, *, plan_factory,
        expected_fingerprint: str, _ingress_owner: Path, **options) -> dict[str, Any]:
    """Shared exact-name quarantine/unlink transaction, never an alternate owner."""
    from stockagent.data_sync import closed_source as closed
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.packed_retention import _validate_peer_proof
    source, config, state_root = options["source"], options["config"], options["state_root"].absolute()
    contract.owner(source, options["source_lock_fd"])
    packed._check_packed_read_authority(config.sync_root)
    # Shared D retention protection does not stop unrelated archive publishers.
    with packed._try_publication_lock(_ingress_owner), packed._try_publication_lock(
            config.sync_root / ".local-state/locks/publish-retention-global.lock", shared=True):
        plan = plan_factory(**options)
        if plan["plan_fingerprint"] != expected_fingerprint:
            raise SnapshotError("closed raw plan changed; repeat the original dry run")
        if plan["blockers"]:
            raise SnapshotError("closed raw retirement blocked: " + ", ".join(plan["blockers"]))
        if plan["raw_retirement_complete"]:
            return {**plan, "action": "already-retired", "deleted_names": 0}
        state_root.mkdir(parents=True, exist_ok=True)
        if source.stat().st_dev != state_root.stat().st_dev:
            raise SnapshotError("closed raw quarantine crosses filesystems")
        state_dir = state_root / "source-raw-retirements"
        state_dir.mkdir(parents=True, exist_ok=True)
        if state_dir.resolve() != state_dir:
            raise SnapshotError("closed raw retirement journal is redirected")
        state_path = state_dir / f"{contract.dataset}.json"
        quarantine = state_dir / (contract.dataset + ".quarantine-" + uuid.uuid4().hex)
        quarantine_names = {name: _raw_quarantine_name(contract, name) for name in plan["members"]}
        if len(set(quarantine_names.values())) != len(quarantine_names):
            raise SnapshotError("raw quarantine names collide; preserve every original")
        quarantine.mkdir(mode=0o700)
        journal = {**plan, "state": "retiring", "quarantine": str(quarantine),
                   "quarantined_members": [], "unlinked_members": [], "unlinked_member_evidence": {}}
        if contract.requires_runtime_admission:
            journal["quarantine_member_names"] = quarantine_names
        atomic_write_json(state_path, journal)
        checkpoint = _RawRetirementCheckpoint(state_path, journal,
            plan.get("journal_checkpoint_members", 1))
        descriptors = {}
        try:
            for name, record in plan["source_identities"].items():
                path = source / name
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                descriptors[name] = fd
                if list(closed._signature(os.fstat(fd))) != record["signature"]:
                    raise SnapshotError("closed raw identity changed before quarantine")
                destination = quarantine / quarantine_names[name]
                os.rename(path, destination)
                from stockagent.data_sync.desync_snapshots import _fsync_directory
                moved = destination.lstat()
                if list(closed._signature(moved))[:-1] != record["signature"][:-1]:
                    raise SnapshotError("closed raw identity changed during quarantine")
                journal["quarantined_members"].append(name)
                checkpoint.advance(path.parent, quarantine)
            checkpoint.flush()
            resolved, _, _, storage = contract.resolve(source, config)
            members = plan["members"]
            quarantined_signatures = {}
            for name, row in members.items():
                path = quarantine / quarantine_names[name]
                before = closed._signature(path.lstat())
                if (closed._hash(path, row["bytes"]) != row["sha256"]
                        or closed._signature(path.lstat()) != before):
                    raise SnapshotError("quarantined raw full SHA differs")
                quarantined_signatures[name] = before
            usage = contract.usage(source, members, config.materialized_root, resolved,
                options["repo_root"], quarantine=quarantine)
            if _closed_raw_usage_blockers(usage):
                raise SnapshotError("closed raw has post-quarantine use; keep quarantine")
            peer = options["peer_probe"]()
            _validate_peer_proof(config, peer)
            contract.owner(source, options["source_lock_fd"])
            policy_sha, _ = contract.policy(options["policy_path"], config.authority_node_id, contract.dataset)
            if policy_sha != plan["policy_sha256"]:
                raise SnapshotError("closed raw policy changed after quarantine")
            if contract.implementation() != plan["implementation_sha256"]:
                raise SnapshotError("closed raw implementation changed after quarantine")
            if contract.requires_runtime_admission and (
                    options.get("runtime_probe") is None
                    or options["runtime_probe"]() != plan["runtime_admission"]
                    or storage["binding_sha256"] != plan["binding_sha256"]):
                raise SnapshotError("retained runtime or binding changed after quarantine; preserve originals")
            reclaimed = 0
            for name, row in members.items():
                path, fd = quarantine / quarantine_names[name], descriptors[name]
                before = path.lstat()
                if (closed._signature(before) != closed._signature(os.fstat(fd)) or before.st_nlink != 1
                        or closed._signature(before) != quarantined_signatures[name]
                        or list(closed._signature(before))[:-1] != plan["source_identities"][name]["signature"][:-1]
                        or before.st_blocks * 512 != plan["source_identities"][name]["allocated_bytes"]):
                    raise SnapshotError("quarantined raw changed before exact unlink")
                path.unlink()
                after = os.fstat(fd)
                if after.st_nlink != 0 or path.exists() or path.is_symlink():
                    raise SnapshotError("closed raw final-link unlink was not verified")
                os.close(descriptors.pop(name))
                reclaimed += before.st_blocks * 512
                journal["unlinked_members"].append(name)
                journal["unlinked_member_evidence"][name] = {
                    "device": before.st_dev, "inode": before.st_ino,
                    "allocated_bytes": before.st_blocks * 512,
                    "links_before": before.st_nlink, "links_after": after.st_nlink,
                    "descriptor_closed": True}
                checkpoint.advance(quarantine)
            checkpoint.flush()
            _fsync_directory(quarantine)
            quarantine.rmdir()
            _fsync_directory(state_dir)
            # A final independent cold decode verifies that the exact originals
            # are still recoverable after removal, not just before it.
            recovery = verify_packed_snapshot(config.sync_root, resolved, reconstruct_all=True,
                **_source_raw_native_reads(config.sync_root))
            contract.resolve(source, config)
            receipt = {"contract": contract.contract, "state": "cold-raw-retired",
                "source": str(source), "snapshot_id": resolved.manifest["snapshot_id"],
                "manifest_sha256": resolved.manifest_sha256, "retired_members": members,
                "deleted_names": len(members), "reclaimed_allocated_file_bytes": reclaimed,
                "unlinked_member_evidence": journal["unlinked_member_evidence"],
                "post_unlink_original_recovery": recovery, "unlink_peer_proof": peer,
                "retired_at_utc": datetime.now(timezone.utc).isoformat(),
                "cold_objects_deleted": 0, "service_views_deleted": 0,
                "NAS_backup_verified": False, "plan_fingerprint": expected_fingerprint}
            if contract.requires_runtime_admission:
                receipt["binding_sha256"] = plan["binding_sha256"]
                receipt["runtime_admission"] = plan["runtime_admission"]
            atomic_write_json(source / contract.receipt_path, receipt)
            final = contract.resolve(source, config)[3]
            if not final["raw_retirement_complete"]:
                raise SnapshotError("closed raw final native receipt readback failed")
            atomic_write_json(state_path, {**receipt, "action": "retired", "final_storage": final})
            return {**receipt, "action": "retired", "final_storage": final}
        finally:
            for fd in descriptors.values():
                os.close(fd)


def reconcile_retained_source_raw_retirement(*, dataset: str, expected_fingerprint: str,
        source: Path, config, state_root: Path, source_lock_fd: int,
        _ingress_owner: Path = Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock")):
    """Restore an uncommitted transaction, or finalize its already valid receipt.

    Every original comes from this exact quarantine or the fully recovered fixed
    D release. Never overwrite a current correction or discard failed evidence.
    A rollback is permission to repeat the dry run, never permission to unlink.
    """
    from scripts.deliver_vast_futures_preparation import rename_no_replace
    from stockagent.data_sync import closed_source as closed
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync.cache_capacity import load_capacity_policy, reserve_cache_write
    from stockagent.data_sync.desync_snapshots import _fsync_directory
    from downloader.artifact_io import atomic_write_bytes

    contract = _retained_raw_contract(dataset)
    source = closed._root(source)
    contract.owner(source, source_lock_fd)
    state_root = state_root.absolute()
    if state_root.resolve() != state_root or state_root.is_relative_to(source) or state_root.is_relative_to(config.sync_root):
        raise SnapshotError("retained recovery journal must remain outside source and cold authority")
    state_dir = state_root / "source-raw-retirements"
    path = state_dir / (dataset + ".json")
    with packed._try_publication_lock(_ingress_owner), packed._try_publication_lock(
            config.sync_root / ".local-state/locks/publish-retention-global.lock", shared=True):
        raw = closed._read(path, limit=16 * 1024**2)
        journal = json.loads(raw)
        keys = ("contract", "source", "snapshot_id", "manifest_sha256", "members", "source_identities",
            "policy_sha256", "materialized_root", "state_root", "implementation_sha256",
            "runtime_admission", "binding_sha256")
        if isinstance(journal, dict) and "journal_checkpoint_members" in journal:
            if type(journal["journal_checkpoint_members"]) is not int or journal["journal_checkpoint_members"] not in (1, 32):
                raise SnapshotError("retained recovery checkpoint scope is invalid")
            keys += ("journal_checkpoint_members",)
        if (not isinstance(journal, dict) or any(key not in journal for key in keys)
                or journal.get("plan_fingerprint") != expected_fingerprint
                or hashlib.sha256(json.dumps({key: journal[key] for key in keys}, sort_keys=True).encode()).hexdigest() != expected_fingerprint
                or journal["contract"] != contract.contract or journal["source"] != str(source)
                or journal["state_root"] != str(state_root)
                or journal["materialized_root"] != str(config.materialized_root)
                or journal.get("state") not in {"retiring", "restoring-raw-names", "rolled-back", "cold-raw-retired"}):
            raise SnapshotError("retained recovery requires its exact original interrupted journal")
        release, files, members, storage = contract.resolve(source, config)
        if (journal["manifest_sha256"] != release.manifest_sha256 or journal["members"] != members
                or journal["snapshot_id"] != release.manifest["snapshot_id"]
                or journal["binding_sha256"] != storage["binding_sha256"]
                or set(journal["source_identities"]) != set(members)):
            raise SnapshotError("retained recovery binding or complete original scope changed")
        if ((source / contract.receipt_path).exists() and not storage["raw_retirement_complete"]):
            raise SnapshotError("committed raw names reappeared; preserve the receipt and current originals")
        quarantine = Path(journal["quarantine"])
        names = {name: _raw_quarantine_name(contract, name) for name in members}
        if (quarantine.parent != state_dir or not quarantine.name.startswith(dataset + ".quarantine-")
                or quarantine.resolve() != quarantine or journal.get("quarantine_member_names") != names):
            raise SnapshotError("retained recovery quarantine is redirected or has an unknown name map")
        if quarantine.exists() and (not quarantine.is_dir()
                or set(p.name for p in quarantine.iterdir()) - set(names.values())):
            raise SnapshotError("retained recovery quarantine has unknown members")
        _source_raw_cold_proof(contract, config, release)
        recovery = verify_packed_snapshot(config.sync_root, release, reconstruct_all=True,
            **_source_raw_native_reads(config.sync_root))
        actions, absent = {}, []
        for name, row in members.items():
            signature = journal["source_identities"][name]["signature"]
            if (not isinstance(signature, list) or len(signature) != 7
                    or any(type(value) is not int for value in signature)
                    or not stat.S_ISREG(signature[2]) or signature[3] != 1 or signature[4] != row["bytes"]):
                raise SnapshotError("retained recovery original metadata is not valid")
            current, saved = source / name, quarantine / names[name]
            for original in (current, saved):
                if original.exists() or original.is_symlink():
                    if (original.resolve(strict=True) != original or not original.is_file()
                            or original.lstat().st_nlink != 1
                            or original.stat().st_size != row["bytes"]
                            or closed._hash(original, row["bytes"]) != row["sha256"]):
                        raise SnapshotError("retained recovery found a changed original; preserve it")
            actions[name] = "already-present" if current.exists() else "quarantine" if saved.exists() else "fixed-D"
            if actions[name] == "fixed-D":
                absent.append(name)
        if journal["state"] == "rolled-back":
            if any(action != "already-present" for action in actions.values()):
                raise SnapshotError("rolled-back raw names changed; repeat an exact recovery audit")
            return {"action": "already-rolled-back", "source": str(source), "deleted_names": 0,
                "raw_retirement_complete": False, "complete_original_recovery": recovery}
        resuming = journal.get("state") == "restoring-raw-names"
        interrupted_sha = journal.get("rollback_original_journal_sha256") if resuming else hashlib.sha256(raw).hexdigest()
        if resuming:
            packed._validate_hash(interrupted_sha, "original interrupted journal SHA")
        operation = (Path(journal["rollback_operation"]) if resuming
            else state_dir / "recoveries" / uuid.uuid4().hex)
        if operation.parent != state_dir / "recoveries" or operation.resolve() != operation:
            raise SnapshotError("retained recovery operation directory is redirected")
        growth = ((0 if storage["raw_retirement_complete"] else sum(members[name]["bytes"] for name in absent))
            + 2 * len(raw) + 4 * 1024**2)
        with reserve_cache_write(operation, key="retained-raw-rollback", maximum_growth_bytes=growth,
                family="materialized", policy=load_capacity_policy()):
            operation.mkdir(mode=0o700, parents=True, exist_ok=resuming)
            failed = operation / "interrupted-journal.private.json"
            if failed.exists() or failed.is_symlink():
                if hashlib.sha256(closed._read(failed, limit=16 * 1024**2)).hexdigest() != interrupted_sha:
                    raise SnapshotError("retained interrupted journal archive changed; preserve recovery state")
            elif resuming:
                raise SnapshotError("retained interrupted journal archive changed or is missing")
            else:
                packed._copy_member_and_verify(io.BytesIO(raw), failed, expected_size=len(raw),
                    expected_sha256=interrupted_sha, mode=0o600)
                _fsync_directory(operation)
            if storage["raw_retirement_complete"]:
                receipt = json.loads(closed._read(source / contract.receipt_path, limit=16 * 1024**2))
                if receipt["plan_fingerprint"] != expected_fingerprint:
                    raise SnapshotError("committed raw receipt belongs to another original transaction")
                result = {**receipt, "action": "retired", "reconciled_after_receipt": True,
                    "interrupted_journal_archive": str(failed), "fresh_original_recovery": recovery}
                atomic_write_json(operation / "recovery.private.json", result)
                atomic_write_json(path, result)
                return result
            journal.update(state="restoring-raw-names", rollback_operation=str(operation),
                rollback_original_journal_sha256=interrupted_sha)
            atomic_write_json(path, journal)
            payloads = packed.read_packed_files(config.sync_root, release, names=absent,
                maximum_bytes=retained.MAX_ORIGINAL_BYTES,
                maximum_object_bytes=retained.MAX_OBJECT_BYTES) if absent else {}
            if contract.resolve(source, config)[3]["binding_sha256"] != storage["binding_sha256"]:
                raise SnapshotError("retained source binding changed before exact restoration")
            restored, restored_from_D = [], 0
            checkpoint = _RawRetirementCheckpoint(path, journal,
                journal.get("journal_checkpoint_members", 1))
            for name, action in actions.items():
                destination = source / name
                if destination.parent.resolve() != destination.parent:
                    raise SnapshotError("retained restore destination parent is redirected")
                if action == "already-present":
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                saved = quarantine / names[name]
                if action == "quarantine":
                    if closed._hash(saved, members[name]["bytes"]) != members[name]["sha256"]:
                        raise SnapshotError("retained quarantine changed during recovery")
                    rename_no_replace(saved, destination)
                    changed_directory = quarantine
                else:
                    stage = operation / (names[name] + ".restore")
                    if not stage.exists():
                        atomic_write_bytes(stage, payloads[name])
                    if closed._hash(stage, members[name]["bytes"]) != members[name]["sha256"]:
                        raise SnapshotError("retained restoration staging differs from the fixed original")
                    signature = journal["source_identities"][name]["signature"]
                    stage.chmod(stat.S_IMODE(signature[2]))
                    os.utime(stage, ns=(signature[5], signature[5]))
                    with stage.open("rb") as stream:
                        os.fsync(stream.fileno())
                    rename_no_replace(stage, destination)
                    changed_directory = operation
                    restored_from_D += members[name]["bytes"]
                restored.append(name)
                journal["restored_names"] = restored
                checkpoint.advance(destination.parent, changed_directory)
            checkpoint.flush()
            for name, row in members.items():
                original = source / name
                if (original.resolve(strict=True) != original or original.lstat().st_nlink != 1
                        or closed._hash(original, row["bytes"]) != row["sha256"]):
                    raise SnapshotError("restored source original failed its complete readback")
            leftovers = sorted(p.name for p in quarantine.iterdir()) if quarantine.exists() else []
            if not leftovers and quarantine.exists():
                quarantine.rmdir()
                _fsync_directory(state_dir)
            recovery = verify_packed_snapshot(config.sync_root, release, reconstruct_all=True,
                **_source_raw_native_reads(config.sync_root))
            if contract.resolve(source, config)[3]["binding_sha256"] != storage["binding_sha256"]:
                raise SnapshotError("retained binding changed after restoring its fixed originals")
            contract.owner(source, source_lock_fd)
            if hashlib.sha256(closed._read(failed, limit=16 * 1024**2)).hexdigest() != interrupted_sha:
                raise SnapshotError("retained interrupted journal archive changed during recovery")
            result = {"action": "restored-for-fresh-audit", "source": str(source),
                "snapshot_id": release.manifest["snapshot_id"], "manifest_sha256": release.manifest_sha256,
                "interrupted_plan_fingerprint": expected_fingerprint, "interrupted_journal_archive": str(failed),
                "original_journal_sha256": interrupted_sha,
                "restored_names": restored, "restored_from_D_original_bytes": restored_from_D,
                "source_originals_verified": len(members), "complete_original_recovery": recovery,
                "quarantine_preserved_members": leftovers, "raw_retirement_complete": False,
                "deleted_names": 0, "cold_objects_deleted": 0, "NAS_backup_verified": False,
                "verified_at_utc": datetime.now(timezone.utc).isoformat()}
            receipt = operation / "recovery.private.json"
            atomic_write_json(receipt, result)
            atomic_write_json(path, {**journal, "state": "rolled-back", "rollback_receipt": str(receipt)})
            return result
