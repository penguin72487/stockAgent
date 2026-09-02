"""Verified cold-artifact packing and direct materialization.

Cold artifact releases remove stable small files from Syncthing's live index
without changing their logical paths under ``artifacts``.  Packed objects are
verified before files are installed, and ignore rules are activated only after
the local materialization has passed its completion contract.
"""

from __future__ import annotations

import dataclasses
import json
import os
import shutil
import stat
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Mapping

from stockagent.data_sync.desync_snapshots import (
    ResolvedSnapshot,
    SnapshotError,
    _safe_relative_path,
    _utc_iso_from_ns,
    atomic_write_bytes,
    atomic_write_json,
    sha256_file,
    validate_slug,
)
from stockagent.data_sync.packed_snapshots import (
    _load_inventory,
    _validate_inventory,
    fetch_packed_snapshot,
    publish_packed_snapshot,
    resolve_latest_packed,
    verify_packed_snapshot,
)
from stockagent.training.lifecycle import validate_completed_training_artifacts


COLD_ARTIFACT_SCHEMA_VERSION = 1
COLD_ACTIVATION_SCHEMA_VERSION = 1
COLD_IGNORE_INCLUDE = ".stignore-cold-local"
COLD_IGNORE_DIRECTIVE = f"#include {COLD_IGNORE_INCLUDE}"
ConflictPolicy = Literal["fail", "local-wins", "packed-wins"]


@dataclasses.dataclass(frozen=True)
class ColdArtifactSpec:
    dataset: str
    relative_root: str
    maximum_file_bytes: int | None
    loose_file_threshold_bytes: int
    pack_buckets: int
    min_stable_hours: float
    completion_contract: str

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ColdArtifactSpec":
        dataset = validate_slug(str(value.get("dataset", "")), "cold dataset")
        relative = _safe_relative_path(
            str(value.get("relative_root", "")), "cold artifact relative_root"
        ).as_posix()
        raw_maximum = value.get("maximum_file_bytes", -1)
        maximum = None if raw_maximum is None else int(raw_maximum)
        threshold = int(value.get("loose_file_threshold_bytes", -1))
        buckets = int(value.get("pack_buckets", 0))
        stable_hours = float(value.get("min_stable_hours", -1))
        contract = str(value.get("completion_contract", "")).strip()
        if maximum is not None and maximum < 0:
            raise SnapshotError("cold maximum_file_bytes must be non-negative")
        if maximum is not None and threshold <= maximum:
            raise SnapshotError(
                "loose_file_threshold_bytes must exceed maximum_file_bytes so "
                "every selected cold file is packed"
            )
        if not 1 <= buckets <= 4096:
            raise SnapshotError("cold pack_buckets must be between 1 and 4096")
        if stable_hours < 0:
            raise SnapshotError("cold min_stable_hours must be non-negative")
        if contract != "training-lifecycle-v1":
            raise SnapshotError(f"unsupported cold completion contract: {contract}")
        return cls(
            dataset=dataset,
            relative_root=relative,
            maximum_file_bytes=maximum,
            loose_file_threshold_bytes=threshold,
            pack_buckets=buckets,
            min_stable_hours=stable_hours,
            completion_contract=contract,
        )


def load_cold_artifact_registry(path: Path) -> dict[str, ColdArtifactSpec]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read cold artifact registry {path}: {exc}") from exc
    if not isinstance(payload, Mapping) or int(payload.get("schema_version", -1)) != 1:
        raise SnapshotError("unsupported cold artifact registry schema")
    rows = payload.get("artifacts")
    if not isinstance(rows, list):
        raise SnapshotError("cold artifact registry artifacts must be a list")
    result: dict[str, ColdArtifactSpec] = {}
    roots: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise SnapshotError("cold artifact registry row must be an object")
        spec = ColdArtifactSpec.from_dict(row)
        if spec.dataset in result:
            raise SnapshotError(f"duplicate cold artifact dataset: {spec.dataset}")
        if spec.relative_root in roots:
            raise SnapshotError(f"duplicate cold artifact root: {spec.relative_root}")
        result[spec.dataset] = spec
        roots.add(spec.relative_root)
    return result


def _newest_regular_mtime_ns(root: Path) -> tuple[int, int, int]:
    newest = 0
    files = 0
    selected_bytes = 0
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            path = Path(directory) / name
            try:
                info = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            files += 1
            selected_bytes += info.st_size
            newest = max(newest, info.st_mtime_ns)
    return newest, files, selected_bytes


def validate_cold_artifact_source(
    artifact_root: Path, spec: ColdArtifactSpec
) -> dict[str, Any]:
    artifact_root = artifact_root.resolve()
    source = artifact_root.joinpath(*PurePosixPath(spec.relative_root).parts)
    if not source.is_dir() or source.is_symlink():
        raise SnapshotError(f"cold artifact source is not a real directory: {source}")
    progress_path = source / "progress.json"
    manifest_path = source / "run_manifest.json"
    try:
        progress = json.loads(progress_path.read_text(encoding="utf-8"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cold lifecycle envelope is unreadable: {exc}") from exc
    if progress.get("state") != "complete" or progress.get("phase") != "complete":
        raise SnapshotError(f"cold source lifecycle is not complete: {source}")
    fold_ids = [int(value) for value in manifest.get("selected_fold_ids", [])]
    group_names = sorted(
        path.name
        for path in source.iterdir()
        if path.is_dir() and not path.is_symlink() and path.name.startswith("train_")
    )
    if not fold_ids or not group_names:
        raise SnapshotError(f"cold lifecycle has no fold/group contract: {source}")
    conformance = validate_completed_training_artifacts(
        source,
        fold_ids=fold_ids,
        group_names=group_names,
    )
    try:
        conformance.require()
    except RuntimeError as exc:
        raise SnapshotError(str(exc)) from exc
    newest_ns, files, logical_bytes = _newest_regular_mtime_ns(source)
    stable_before_ns = time.time_ns() - int(
        spec.min_stable_hours * 3600 * 1_000_000_000
    )
    if newest_ns > stable_before_ns:
        age_hours = (time.time_ns() - newest_ns) / 3_600_000_000_000
        raise SnapshotError(
            f"cold source is only {age_hours:.2f} hours stable; "
            f"requires {spec.min_stable_hours:.2f} hours"
        )
    return {
        "dataset": spec.dataset,
        "relative_root": spec.relative_root,
        "source": str(source),
        "files": files,
        "logical_bytes": logical_bytes,
        "newest_mtime_ns": newest_ns,
        "stable_hours": (time.time_ns() - newest_ns) / 3_600_000_000_000,
        "fold_ids": fold_ids,
        "group_names": group_names,
        "completion_contract": spec.completion_contract,
        "contract_ok": True,
    }


def publish_cold_artifact(
    sync_root: Path,
    artifact_root: Path,
    spec: ColdArtifactSpec,
    *,
    node_id: str | None = None,
    repo_root: Path | None = None,
) -> ResolvedSnapshot:
    status = validate_cold_artifact_source(artifact_root, spec)
    source = Path(str(status["source"]))
    return publish_packed_snapshot(
        sync_root,
        spec.dataset,
        source,
        node_id=node_id,
        loose_file_threshold_bytes=spec.loose_file_threshold_bytes,
        pack_buckets=spec.pack_buckets,
        maximum_file_bytes=spec.maximum_file_bytes,
        metadata={
            "artifact_relative_root": spec.relative_root,
            "completion_contract": spec.completion_contract,
            "source_lifecycle_validated": "true",
            "transport_role": (
                "cold-full-run"
                if spec.maximum_file_bytes is None
                else "cold-small-files"
            ),
        },
        repo_root=repo_root,
    )


def _safe_destination(root: Path, relative: str) -> Path:
    relpath = _safe_relative_path(relative, "cold materialization path")
    current = root
    for component in relpath.parts[:-1]:
        current = current / component
        if current.is_symlink():
            raise SnapshotError(f"cold destination parent is a symlink: {current}")
        if current.exists() and not current.is_dir():
            raise SnapshotError(f"cold destination parent is not a directory: {current}")
    return root.joinpath(*relpath.parts)


def _same_file_payload(path: Path, row: Mapping[str, Any]) -> bool:
    try:
        info = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_size == int(row["size"])
        and stat.S_IMODE(info.st_mode) == int(row["mode"])
        and sha256_file(path) == str(row["sha256"])
    )


def _atomic_install_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / (
        f".stockagent-cold.{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        try:
            os.link(source, temporary, follow_symlinks=False)
        except OSError:
            shutil.copy2(source, temporary, follow_symlinks=False)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _materialized_rows(sync_root: Path, resolved: ResolvedSnapshot) -> list[dict[str, Any]]:
    verify_packed_snapshot(sync_root, resolved)
    rows = _load_inventory(sync_root.resolve(), resolved.manifest)
    _validate_inventory(resolved.manifest, rows)
    return rows


def activate_cold_artifact(
    sync_root: Path,
    artifact_root: Path,
    live_sync_root: Path,
    state_root: Path,
    spec: ColdArtifactSpec,
    *,
    conflict_policy: ConflictPolicy = "fail",
) -> dict[str, Any]:
    if conflict_policy not in {"fail", "local-wins", "packed-wins"}:
        raise ValueError(f"unsupported conflict policy: {conflict_policy}")
    sync_root = sync_root.resolve()
    artifact_root = artifact_root.resolve()
    live_sync_root = live_sync_root.resolve()
    state_root = state_root.resolve()
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if metadata.get("artifact_relative_root") != spec.relative_root:
        raise SnapshotError("packed artifact root disagrees with the local registry")
    rows = _materialized_rows(sync_root, resolved)
    destination_root = artifact_root.joinpath(
        *PurePosixPath(spec.relative_root).parts
    )
    destination_root.mkdir(parents=True, exist_ok=True)
    staging_parent = state_root / "staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    added = replaced = already_present = local_conflicts = 0
    ignored_paths: list[str] = []
    with tempfile.TemporaryDirectory(
        prefix=f"{spec.dataset}.", dir=staging_parent
    ) as temporary:
        materialized = fetch_packed_snapshot(
            sync_root, Path(temporary), resolved
        )
        conflicts: list[str] = []
        for row in rows:
            if row["kind"] != "file":
                continue
            destination = _safe_destination(destination_root, str(row["path"]))
            if os.path.lexists(destination) and not _same_file_payload(destination, row):
                conflicts.append(str(row["path"]))
        if conflicts and conflict_policy == "fail":
            raise SnapshotError(
                "cold materialization has conflicting local files: "
                + ", ".join(conflicts[:10])
            )
        for row in rows:
            relative = str(row["path"])
            destination = _safe_destination(destination_root, relative)
            source = materialized.joinpath(*PurePosixPath(relative).parts)
            full_relative = (
                PurePosixPath(spec.relative_root) / PurePosixPath(relative)
            ).as_posix()
            if row["kind"] == "directory":
                destination.mkdir(parents=True, exist_ok=True)
                continue
            if row["kind"] == "symlink":
                if destination.is_symlink() and os.readlink(destination) == row["target"]:
                    ignored_paths.append(full_relative)
                    continue
                if os.path.lexists(destination):
                    if conflict_policy == "local-wins":
                        local_conflicts += 1
                        continue
                    if conflict_policy != "packed-wins":
                        raise SnapshotError(f"cold symlink conflict: {destination}")
                    destination.unlink()
                    replaced += 1
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.symlink_to(str(row["target"]))
                added += 1
                ignored_paths.append(full_relative)
                continue
            if _same_file_payload(destination, row):
                already_present += 1
                ignored_paths.append(full_relative)
                continue
            if os.path.lexists(destination):
                if conflict_policy == "local-wins":
                    local_conflicts += 1
                    continue
                replaced += 1
            else:
                added += 1
            _atomic_install_file(source, destination)
            if not _same_file_payload(destination, row):
                raise SnapshotError(f"installed cold file failed verification: {destination}")
            ignored_paths.append(full_relative)
    receipt = {
        "schema_version": COLD_ACTIVATION_SCHEMA_VERSION,
        "dataset": spec.dataset,
        "snapshot_id": resolved.manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "artifact_relative_root": spec.relative_root,
        "conflict_policy": conflict_policy,
        "added": added,
        "replaced": replaced,
        "already_present": already_present,
        "conflicts_detected": len(conflicts),
        "local_conflicts": local_conflicts,
        "ignored_file_paths": sorted(ignored_paths),
        "activated_at": _utc_iso_from_ns(time.time_ns()),
    }
    activation_dir = state_root / "activations"
    activation_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(activation_dir / f"{spec.dataset}.json", receipt)
    rebuild_cold_ignore(live_sync_root, activation_dir)
    return receipt


def _syncthing_literal(path: str) -> str:
    if any(character in path for character in "*?[]\\"):
        raise SnapshotError(f"cold path requires unsupported ignore escaping: {path}")
    return "/" + path


def rebuild_cold_ignore(live_sync_root: Path, activation_dir: Path) -> Path:
    live_sync_root = live_sync_root.resolve()
    include_path = live_sync_root / COLD_IGNORE_INCLUDE
    patterns: set[str] = set()
    receipts: list[tuple[str, str]] = []
    for path in sorted(activation_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"cannot read cold activation {path}: {exc}") from exc
        if int(payload.get("schema_version", -1)) != COLD_ACTIVATION_SCHEMA_VERSION:
            raise SnapshotError(f"unsupported cold activation receipt: {path}")
        dataset = validate_slug(str(payload.get("dataset", "")), "cold dataset")
        snapshot_id = validate_slug(
            str(payload.get("snapshot_id", "")), "cold snapshot_id"
        )
        receipts.append((dataset, snapshot_id))
        for relative in payload.get("ignored_file_paths", []):
            patterns.add(_syncthing_literal(str(relative)))
    lines = [
        "// Generated locally after verified cold-artifact materialization.",
        "// Do not copy this file between nodes; each node activates independently.",
    ]
    for dataset, snapshot_id in receipts:
        lines.append(f"// active {dataset} {snapshot_id}")
    lines.extend(f"(?d){pattern}" for pattern in sorted(patterns))
    atomic_write_bytes(include_path, ("\n".join(lines) + "\n").encode("utf-8"))
    ignore_path = live_sync_root / ".stignore"
    existing = ignore_path.read_text(encoding="utf-8") if ignore_path.exists() else ""
    if COLD_IGNORE_DIRECTIVE not in existing.splitlines():
        updated = existing.rstrip("\n")
        if updated:
            updated += "\n"
        updated += COLD_IGNORE_DIRECTIVE + "\n"
        atomic_write_bytes(ignore_path, updated.encode("utf-8"))
    return include_path


__all__ = [
    "COLD_IGNORE_DIRECTIVE",
    "COLD_IGNORE_INCLUDE",
    "ColdArtifactSpec",
    "activate_cold_artifact",
    "load_cold_artifact_registry",
    "publish_cold_artifact",
    "rebuild_cold_ignore",
    "validate_cold_artifact_source",
]
