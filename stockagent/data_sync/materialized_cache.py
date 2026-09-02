"""Lease-based hot cache for immutable packed dataset releases.

The packed Syncthing tree is the durable cold copy.  Materialized trees are
verified, replaceable caches: ``use`` renews a lease, while periodic ``gc``
automatically renews leases referenced by a running process and removes only
expired, manifest-backed trees that are not pinned or in use.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

from stockagent.data_sync.desync_snapshots import (
    ResolvedSnapshot,
    SnapshotError,
    _exclusive_lock,
    _paths_overlap,
    _utc_iso_from_ns,
    atomic_write_json,
    validate_slug,
)
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)


CACHE_LEASE_SCHEMA_VERSION = 1
DEFAULT_CACHE_TTL_DAYS = 7.0
_STATE_DIRECTORY = ".cache-state"


def _absolute_link_path(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return expanded.parent.resolve() / expanded.name


def _state_root(materialized_root: Path) -> Path:
    return materialized_root / _STATE_DIRECTORY


def _lease_path(
    materialized_root: Path, dataset: str, snapshot_id: str
) -> Path:
    return (
        _state_root(materialized_root)
        / "leases"
        / dataset
        / f"{snapshot_id}.json"
    )


def _lock_path(materialized_root: Path, dataset: str) -> Path:
    return _state_root(materialized_root) / "locks" / f"{dataset}.lock"


def _target_path(
    materialized_root: Path, dataset: str, snapshot_id: str
) -> Path:
    return materialized_root / dataset / snapshot_id


def _ready_path(
    materialized_root: Path, dataset: str, snapshot_id: str
) -> Path:
    return materialized_root / dataset / f".{snapshot_id}.READY.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read cache metadata {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SnapshotError(f"cache metadata must be an object: {path}")
    return value


def _ready_matches(
    materialized_root: Path, resolved: ResolvedSnapshot
) -> bool:
    manifest = resolved.manifest
    dataset = validate_slug(str(manifest["dataset"]), "dataset")
    snapshot_id = validate_slug(str(manifest["snapshot_id"]), "snapshot_id")
    target = _target_path(materialized_root, dataset, snapshot_id)
    if not target.is_dir() or target.is_symlink():
        return False
    try:
        ready = _read_json(_ready_path(materialized_root, dataset, snapshot_id))
    except SnapshotError:
        return False
    return (
        ready.get("snapshot_id") == snapshot_id
        and ready.get("manifest_sha256") == resolved.manifest_sha256
    )


def _equivalent_ready_snapshot(
    sync_root: Path,
    materialized_root: Path,
    resolved: ResolvedSnapshot,
) -> ResolvedSnapshot | None:
    """Find a fully verified local tree with the same immutable inventory."""

    dataset = validate_slug(str(resolved.manifest["dataset"]), "dataset")
    inventory_sha256 = str(
        resolved.manifest["archive"]["inventory"]["sha256"]
    )
    dataset_root = materialized_root / dataset
    for ready_path in sorted(dataset_root.glob(".*.READY.json"), reverse=True):
        try:
            ready = _read_json(ready_path)
            if ready.get("inventory_sha256") != inventory_sha256:
                continue
            candidate = resolve_packed_snapshot_id(
                sync_root,
                dataset,
                str(ready.get("snapshot_id") or ""),
            )
        except (OSError, SnapshotError, ValueError):
            continue
        if _ready_matches(materialized_root, candidate):
            return candidate
    return None


def _atomic_symlink(link: Path, target: Path) -> None:
    link = _absolute_link_path(link)
    target = target.resolve()
    link.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(link) and not link.is_symlink():
        raise SnapshotError(f"refusing to replace a non-symlink cache link: {link}")
    temporary = link.parent / f".{link.name}.cache-link.{uuid.uuid4().hex}"
    try:
        temporary.symlink_to(target)
        os.replace(temporary, link)
    finally:
        temporary.unlink(missing_ok=True)


def _link_points_to(link: Path, target: Path) -> bool:
    if not link.is_symlink():
        return False
    try:
        return link.resolve(strict=False) == target.resolve(strict=False)
    except OSError:
        return False


def _quarantine_corrupt_materialization(
    materialized_root: Path,
    dataset: str,
    snapshot_id: str,
) -> dict[str, str]:
    """Atomically isolate a corrupt hot cache before reconstructing it."""

    target = _target_path(materialized_root, dataset, snapshot_id)
    ready = _ready_path(materialized_root, dataset, snapshot_id)
    quarantine = (
        _state_root(materialized_root)
        / "quarantine"
        / dataset
        / f"{snapshot_id}.{time.time_ns()}.{uuid.uuid4().hex}"
    )
    quarantine.mkdir(parents=True, exist_ok=False)
    result: dict[str, str] = {"quarantine_root": str(quarantine)}
    if target.exists():
        destination = quarantine / "tree"
        os.replace(target, destination)
        result["tree"] = str(destination)
    if ready.exists():
        destination = quarantine / "READY.json"
        os.replace(ready, destination)
        result["ready"] = str(destination)
    return result


def use_materialized_snapshot(
    sync_root: Path,
    materialized_root: Path,
    dataset: str,
    *,
    snapshot_id: str | None = None,
    ttl_days: float = DEFAULT_CACHE_TTL_DAYS,
    links: Iterable[Path] = (),
    verify_existing: bool = False,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Materialize a packed release and renew its local hot-cache lease."""

    if not math.isfinite(ttl_days) or ttl_days <= 0:
        raise SnapshotError("cache ttl_days must be positive")
    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError("materialized root must be outside the packed sync root")
    dataset = validate_slug(dataset, "dataset")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    requested_snapshot_id = snapshot_id
    resolved = (
        resolve_packed_snapshot_id(sync_root, dataset, snapshot_id)
        if snapshot_id
        else resolve_latest_packed(sync_root, dataset, now_ns=current_ns)
    )
    if requested_snapshot_id is None and not _ready_matches(
        materialized_root, resolved
    ):
        equivalent = _equivalent_ready_snapshot(
            sync_root, materialized_root, resolved
        )
        if equivalent is not None:
            resolved = equivalent
    snapshot_id = validate_slug(
        str(resolved.manifest["snapshot_id"]), "snapshot_id"
    )
    target = _target_path(materialized_root, dataset, snapshot_id)
    default_link = materialized_root / "current" / dataset
    requested_links = [default_link, *links]

    with _exclusive_lock(_lock_path(materialized_root, dataset)):
        ready_reused = _ready_matches(materialized_root, resolved)
        recovered_corruption: dict[str, str] | None = None
        if verify_existing and ready_reused:
            try:
                verify_packed_snapshot(
                    sync_root, resolved, materialized_path=target
                )
            except SnapshotError:
                recovered_corruption = _quarantine_corrupt_materialization(
                    materialized_root, dataset, snapshot_id
                )
                target = fetch_packed_snapshot(
                    sync_root, materialized_root, resolved
                )
            verification = "full"
        elif ready_reused:
            verification = "ready-marker"
        else:
            target = fetch_packed_snapshot(sync_root, materialized_root, resolved)
            verification = "full"

        installed_links: list[str] = []
        for raw_link in requested_links:
            link = _absolute_link_path(Path(raw_link))
            if link == target or target in link.parents:
                raise SnapshotError(f"cache link must be outside its target: {link}")
            _atomic_symlink(link, target)
            installed_links.append(str(link))

        lease_path = _lease_path(materialized_root, dataset, snapshot_id)
        previous_links: list[str] = []
        if lease_path.exists():
            try:
                previous = _read_json(lease_path)
                previous_links = [
                    str(item) for item in previous.get("link_paths", [])
                ]
            except SnapshotError:
                previous_links = []
        all_links = sorted(set(previous_links + installed_links))
        expires_ns = current_ns + int(ttl_days * 86_400 * 1_000_000_000)
        lease = {
            "schema_version": CACHE_LEASE_SCHEMA_VERSION,
            "state": "hot",
            "dataset": dataset,
            "snapshot_id": snapshot_id,
            "manifest_sha256": resolved.manifest_sha256,
            "inventory_sha256": resolved.manifest["archive"]["inventory"]["sha256"],
            "sync_root": str(sync_root),
            "materialized_root": str(materialized_root),
            "target": str(target),
            "link_paths": all_links,
            "last_used_ns": current_ns,
            "last_used_at": _utc_iso_from_ns(current_ns),
            "expires_ns": expires_ns,
            "expires_at": _utc_iso_from_ns(expires_ns),
            "ttl_days": float(ttl_days),
            "source_files": int(resolved.manifest["source"]["files"]),
            "source_logical_bytes": int(
                resolved.manifest["source"]["logical_bytes"]
            ),
            "cold_stored_bytes": int(
                resolved.manifest["archive"]["stored_bytes"]
            ),
            "verification": verification,
        }
        if recovered_corruption is not None:
            lease["recovered_corrupt_materialization"] = recovered_corruption
        atomic_write_json(lease_path, lease)
        return lease


def _load_leases(materialized_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    leases: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(
        (_state_root(materialized_root) / "leases").glob("*/*.json")
    ):
        try:
            lease = _read_json(path)
        except SnapshotError:
            continue
        leases.append((path, lease))
    return leases


def _pinned_snapshot_ids(materialized_root: Path) -> set[str]:
    pinned: set[str] = set()
    for path in materialized_root.rglob("*.pin.json"):
        if _STATE_DIRECTORY in path.parts:
            continue
        try:
            value = _read_json(path)
            manifest = value.get("manifest", {})
            if isinstance(manifest, Mapping):
                snapshot_id = manifest.get("snapshot_id")
                if snapshot_id:
                    pinned.add(str(snapshot_id))
        except SnapshotError:
            continue
    return pinned


def _path_is_under(value: str, target: Path) -> bool:
    cleaned = value.removesuffix(" (deleted)")
    target_text = str(target)
    return cleaned == target_text or cleaned.startswith(target_text + os.sep)


def process_references(target: Path, *, limit: int = 20) -> list[str]:
    """Return bounded evidence that a running process still uses ``target``."""

    target = target.resolve()
    references: list[str] = []
    own_pid = os.getpid()
    for process in sorted(Path("/proc").glob("[0-9]*")):
        try:
            pid = int(process.name)
        except ValueError:
            continue
        if pid == own_pid:
            continue
        for name in ("cwd", "root", "exe"):
            try:
                value = os.readlink(process / name)
            except OSError:
                continue
            if _path_is_under(value, target):
                references.append(f"pid={pid}:{name}:{value}")
                if len(references) >= limit:
                    return references
        try:
            descriptors = list((process / "fd").iterdir())
        except OSError:
            descriptors = []
        for descriptor in descriptors:
            try:
                value = os.readlink(descriptor)
            except OSError:
                continue
            if _path_is_under(value, target):
                references.append(f"pid={pid}:fd={descriptor.name}:{value}")
                if len(references) >= limit:
                    return references
        try:
            maps = (process / "maps").read_text(
                encoding="utf-8", errors="replace"
            )
        except OSError:
            maps = ""
        target_text = str(target)
        if f" {target_text}/" in maps or any(
            line.endswith(f" {target_text}") for line in maps.splitlines()
        ):
            references.append(f"pid={pid}:maps:{target_text}")
            if len(references) >= limit:
                return references
    return references


def _validated_lease_identity(
    materialized_root: Path, lease: Mapping[str, Any]
) -> tuple[str, str, Path, Path]:
    if int(lease.get("schema_version", -1)) != CACHE_LEASE_SCHEMA_VERSION:
        raise SnapshotError("unsupported materialized cache lease schema")
    dataset = validate_slug(str(lease.get("dataset", "")), "lease dataset")
    snapshot_id = validate_slug(
        str(lease.get("snapshot_id", "")), "lease snapshot_id"
    )
    expected = _target_path(materialized_root, dataset, snapshot_id)
    recorded = Path(str(lease.get("target", ""))).resolve(strict=False)
    if recorded != expected:
        raise SnapshotError(
            f"cache lease target escapes its dataset root: {recorded}"
        )
    ready_path = _ready_path(materialized_root, dataset, snapshot_id)
    return dataset, snapshot_id, expected, ready_path


def _unlink_registered_links(lease: Mapping[str, Any], target: Path) -> list[str]:
    removed: list[str] = []
    for value in lease.get("link_paths", []):
        link = _absolute_link_path(Path(str(value)))
        if _link_points_to(link, target):
            link.unlink()
            removed.append(str(link))
    return removed


def _auto_renew_active_lease(
    lease_path: Path,
    lease: dict[str, Any],
    *,
    now_ns: int,
    dry_run: bool,
    references: list[str],
) -> dict[str, Any]:
    """Extend a managed hot lease after observing a live process reference."""

    try:
        ttl_days = float(lease.get("ttl_days", DEFAULT_CACHE_TTL_DAYS))
    except (TypeError, ValueError):
        ttl_days = 0.0
    if not math.isfinite(ttl_days) or ttl_days <= 0:
        return {
            "action": "keep",
            "reason": "in-use-invalid-lease-ttl",
            "process_references": references,
        }

    previous_expires_ns = int(lease.get("expires_ns", 0))
    renewed_expires_ns = max(
        previous_expires_ns,
        now_ns + int(ttl_days * 86_400 * 1_000_000_000),
    )
    result: dict[str, Any] = {
        "action": "would-renew" if dry_run else "renewed",
        "reason": "in-use-auto-renewed",
        "process_references": references,
        "previous_expires_at": lease.get("expires_at"),
        "expires_at": _utc_iso_from_ns(renewed_expires_ns),
    }
    if dry_run:
        return result

    lease.update(
        {
            "state": "hot",
            "last_used_ns": now_ns,
            "last_used_at": _utc_iso_from_ns(now_ns),
            "expires_ns": renewed_expires_ns,
            "expires_at": _utc_iso_from_ns(renewed_expires_ns),
            "auto_renewed_ns": now_ns,
            "auto_renewed_at": _utc_iso_from_ns(now_ns),
            "auto_renewal_count": int(lease.get("auto_renewal_count", 0)) + 1,
            "auto_renewal_evidence": references,
        }
    )
    atomic_write_json(lease_path, lease)
    return result


def _evict_one(
    sync_root: Path,
    materialized_root: Path,
    lease_path: Path,
    lease: dict[str, Any],
    *,
    now_ns: int,
    force: bool,
    dry_run: bool,
    pinned: set[str],
) -> dict[str, Any]:
    dataset, snapshot_id, target, ready_path = _validated_lease_identity(
        materialized_root, lease
    )
    result: dict[str, Any] = {
        "dataset": dataset,
        "snapshot_id": snapshot_id,
        "target": str(target),
        "action": "keep",
        "reason": "lease-active",
        "expires_at": lease.get("expires_at"),
    }
    if snapshot_id in pinned:
        result["reason"] = "pinned"
        return result

    # Size/presence validation is deliberately performed before touching the
    # hot copy.  This proves the cold release is still locally reconstructable.
    try:
        resolved = resolve_packed_snapshot_id(sync_root, dataset, snapshot_id)
    except (OSError, SnapshotError) as exc:
        result["reason"] = f"cold-release-incomplete: {exc}"
        return result
    if resolved.manifest_sha256 != lease.get("manifest_sha256"):
        result["reason"] = "manifest-hash-mismatch"
        return result

    if not target.exists():
        result["action"] = "would-mark-cold" if dry_run else "marked-cold"
        result["reason"] = "already-absent"
        if not dry_run:
            removed_links = _unlink_registered_links(lease, target)
            lease.update(
                {
                    "state": "cold-only",
                    "evicted_ns": now_ns,
                    "evicted_at": _utc_iso_from_ns(now_ns),
                    "eviction_reason": "already-absent",
                    "removed_links": removed_links,
                }
            )
            atomic_write_json(lease_path, lease)
        return result
    if not target.is_dir() or target.is_symlink():
        result["reason"] = "target-is-not-a-real-directory"
        return result
    try:
        ready = _read_json(ready_path)
    except SnapshotError as exc:
        result["reason"] = f"missing-ready-proof: {exc}"
        return result
    if (
        ready.get("snapshot_id") != snapshot_id
        or ready.get("manifest_sha256") != resolved.manifest_sha256
    ):
        result["reason"] = "ready-proof-mismatch"
        return result
    references = process_references(target)
    if references:
        if not force:
            result.update(
                _auto_renew_active_lease(
                    lease_path,
                    lease,
                    now_ns=now_ns,
                    dry_run=dry_run,
                    references=references,
                )
            )
        else:
            result["reason"] = "in-use"
            result["process_references"] = references
        return result
    if not force and int(lease.get("expires_ns", 0)) > now_ns:
        return result
    result["action"] = "would-evict" if dry_run else "evicted"
    result["reason"] = "forced" if force else "lease-expired"
    if dry_run:
        return result

    removed_links = _unlink_registered_links(lease, target)
    shutil.rmtree(target)
    ready_path.unlink(missing_ok=True)
    lease.update(
        {
            "state": "cold-only",
            "evicted_ns": now_ns,
            "evicted_at": _utc_iso_from_ns(now_ns),
            "eviction_reason": result["reason"],
            "removed_links": removed_links,
        }
    )
    atomic_write_json(lease_path, lease)
    result["removed_links"] = removed_links
    return result


def evict_materialized_snapshots(
    sync_root: Path,
    materialized_root: Path,
    *,
    dataset: str | None = None,
    snapshot_id: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Evict expired leases, or one explicitly selected lease with ``force``."""

    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError("materialized root must be outside the packed sync root")
    if dataset is not None:
        dataset = validate_slug(dataset, "dataset")
    if snapshot_id is not None:
        snapshot_id = validate_slug(snapshot_id, "snapshot_id")
    if snapshot_id is not None and dataset is None:
        raise SnapshotError("snapshot_id requires dataset")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    pinned = _pinned_snapshot_ids(materialized_root)
    candidates = [
        (path, lease)
        for path, lease in _load_leases(materialized_root)
        if (dataset is None or lease.get("dataset") == dataset)
        and (snapshot_id is None or lease.get("snapshot_id") == snapshot_id)
        and lease.get("state") != "cold-only"
    ]
    if force and dataset is not None and not candidates:
        raise SnapshotError(f"no hot cache lease found for dataset {dataset}")
    results: list[dict[str, Any]] = []
    for lease_path, lease in candidates:
        lease_dataset = validate_slug(str(lease.get("dataset", "")), "dataset")
        with _exclusive_lock(_lock_path(materialized_root, lease_dataset)):
            latest_lease = _read_json(lease_path)
            results.append(
                _evict_one(
                    sync_root,
                    materialized_root,
                    lease_path,
                    latest_lease,
                    now_ns=current_ns,
                    force=force,
                    dry_run=dry_run,
                    pinned=pinned,
                )
            )
    return {
        "schema_version": 1,
        "checked_at": _utc_iso_from_ns(current_ns),
        "dry_run": dry_run,
        "force": force,
        "checked": len(results),
        "evicted": sum(item["action"] == "evicted" for item in results),
        "would_evict": sum(item["action"] == "would-evict" for item in results),
        "renewed": sum(item["action"] == "renewed" for item in results),
        "would_renew": sum(
            item["action"] == "would-renew" for item in results
        ),
        "kept": sum(item["action"] == "keep" for item in results),
        "results": results,
    }


def materialized_cache_status(
    sync_root: Path,
    materialized_root: Path,
    *,
    dataset: str | None = None,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Return cheap cold/hot state without rehashing materialized files."""

    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if dataset is not None:
        dataset = validate_slug(dataset, "dataset")
        datasets = [dataset]
    else:
        manifest_root = sync_root / "manifests"
        datasets = sorted(
            path.name for path in manifest_root.iterdir() if path.is_dir()
        ) if manifest_root.is_dir() else []
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    pinned = _pinned_snapshot_ids(materialized_root)
    lease_by_target = {
        str(lease.get("target")): lease
        for _, lease in _load_leases(materialized_root)
    }
    rows: list[dict[str, Any]] = []
    for name in datasets:
        try:
            latest = resolve_latest_packed(sync_root, name, now_ns=current_ns)
            latest_id = str(latest.manifest["snapshot_id"])
            cold = {
                "available": True,
                "snapshot_id": latest_id,
                "source_files": int(latest.manifest["source"]["files"]),
                "source_logical_bytes": int(
                    latest.manifest["source"]["logical_bytes"]
                ),
                "stored_bytes": int(latest.manifest["archive"]["stored_bytes"]),
                "objects": int(latest.manifest["archive"]["object_count"]) + 1,
            }
        except (OSError, SnapshotError) as exc:
            latest_id = None
            cold = {"available": False, "error": str(exc)}
        current_link = materialized_root / "current" / name
        hot: dict[str, Any] = {
            "state": "cold-only",
            "current_link": str(current_link),
            "target": None,
        }
        dataset_root = materialized_root / name
        materializations: list[dict[str, Any]] = []
        if dataset_root.is_dir():
            for target in sorted(dataset_root.iterdir()):
                if (
                    not target.is_dir()
                    or target.is_symlink()
                    or target.name.startswith(".")
                ):
                    continue
                snapshot = target.name
                lease = lease_by_target.get(str(target))
                try:
                    historical = resolve_packed_snapshot_id(
                        sync_root, name, snapshot
                    )
                    source_logical_bytes = int(
                        historical.manifest["source"]["logical_bytes"]
                    )
                except (OSError, SnapshotError):
                    source_logical_bytes = (
                        lease.get("source_logical_bytes") if lease else None
                    )
                materializations.append(
                    {
                        "snapshot_id": snapshot,
                        "target": str(target),
                        "managed": lease is not None,
                        "lease_state": lease.get("state") if lease else None,
                        "last_used_at": lease.get("last_used_at") if lease else None,
                        "expires_at": lease.get("expires_at") if lease else None,
                        "ready": _ready_path(
                            materialized_root, name, snapshot
                        ).is_file(),
                        "pinned": snapshot in pinned,
                        "source_logical_bytes": source_logical_bytes,
                    }
                )
        hot["materializations"] = materializations
        hot["materialized_count"] = len(materializations)
        if materializations:
            hot["state"] = "hot-unmanaged"
        if os.path.lexists(current_link):
            if not current_link.is_symlink():
                hot["state"] = "invalid-link"
            else:
                target = current_link.resolve(strict=False)
                lease = lease_by_target.get(str(target))
                snapshot = target.name
                hot.update(
                    {
                        "target": str(target),
                        "snapshot_id": snapshot,
                        "ready": target.is_dir()
                        and _ready_path(
                            materialized_root, name, snapshot
                        ).is_file(),
                        "pinned": snapshot in pinned,
                        "in_use": bool(process_references(target))
                        if target.is_dir()
                        else False,
                        "last_used_at": lease.get("last_used_at") if lease else None,
                        "expires_at": lease.get("expires_at") if lease else None,
                        "source_logical_bytes": lease.get("source_logical_bytes")
                        if lease
                        else None,
                    }
                )
                if not target.is_dir():
                    hot["state"] = "broken-link"
                elif lease is None:
                    hot["state"] = "hot-unmanaged"
                elif int(lease.get("expires_ns", 0)) <= current_ns:
                    hot["state"] = "hot-expired"
                elif snapshot == latest_id:
                    hot["state"] = "hot-current"
                else:
                    hot["state"] = "hot-outdated"
        rows.append({"dataset": name, "cold": cold, "hot": hot})
    return {
        "schema_version": 1,
        "checked_at": _utc_iso_from_ns(current_ns),
        "sync_root": str(sync_root),
        "materialized_root": str(materialized_root),
        "datasets": rows,
    }
