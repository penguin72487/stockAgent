"""Automatic lifecycle for completed training artifacts.

Mutable runs stay outside Syncthing. Complete immutable runs enter the packed
store, and an old local source is removed only after exact cold verification
and convergence of the intended Syncthing peer.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import time
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping

from stockagent.data_sync.cold_artifacts import (
    ColdArtifactSpec,
    publish_cold_artifact,
    validate_cold_artifact_source,
)
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _utc_iso_from_ns,
    atomic_write_json,
)
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.packed_snapshots import (
    resolve_latest_packed,
    verify_packed_snapshot,
)


AUTO_ARTIFACT_STATE_SCHEMA_VERSION = 1


def automatic_dataset_name(relative_root: str) -> str:
    """Return a stable, bounded dataset name for one artifact root."""

    digest = hashlib.sha256(relative_root.encode("utf-8")).hexdigest()[:16]
    basename = PurePosixPath(relative_root).name.lower()
    readable = "".join(c if c.isalnum() else "-" for c in basename)
    readable = "-".join(part for part in readable.split("-") if part)[:40]
    return f"artifact-auto-{readable or 'run'}-{digest}"


def discover_completed_runs(artifact_root: Path, scope: str) -> list[Path]:
    """Discover real directories with a canonical complete lifecycle envelope."""

    artifact_root = artifact_root.resolve()
    relative_scope = PurePosixPath(scope)
    if relative_scope.is_absolute() or ".." in relative_scope.parts:
        raise SnapshotError(f"unsafe automatic artifact scope: {scope}")
    scope_root = artifact_root.joinpath(*relative_scope.parts)
    if not scope_root.is_dir() or scope_root.is_symlink():
        raise SnapshotError(f"automatic artifact scope is not a real directory: {scope_root}")
    result: list[Path] = []
    for progress_path in sorted(scope_root.rglob("progress.json")):
        source = progress_path.parent
        if source.is_symlink() or not source.is_dir():
            continue
        try:
            progress = json.loads(progress_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if progress.get("state") == "complete" and progress.get("phase") == "complete":
            result.append(source)
    return result


def newest_activity_ns(root: Path) -> int:
    """Return newest content-change time among regular files.

    Filesystem atime is deliberately excluded: validation, deduplication, and
    packing read the files and would otherwise look like user activity.
    """

    newest = 0
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            path = Path(directory) / name
            try:
                info = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode):
                newest = max(newest, info.st_mtime_ns)
    return newest


def artifact_process_references(source: Path, scope_root: Path) -> list[str]:
    """Find direct references plus orchestrators naming an ancestor suite."""

    references = process_references(source)
    source = source.resolve()
    scope_root = scope_root.resolve()
    own_pid = os.getpid()
    for process in sorted(Path("/proc").glob("[0-9]*")):
        try:
            pid = int(process.name)
            if pid == own_pid:
                continue
            raw = (process / "cmdline").read_bytes()
        except (OSError, ValueError):
            continue
        for raw_arg in raw.split(b"\0"):
            if not raw_arg.startswith(b"/"):
                continue
            try:
                argument = Path(os.fsdecode(raw_arg)).resolve(strict=False)
                argument.relative_to(scope_root)
                source.relative_to(argument)
            except (OSError, ValueError):
                continue
            evidence = f"pid={pid}:cmdline:{argument}"
            if evidence not in references:
                references.append(evidence)
            if len(references) >= 20:
                return references
    return references


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read automatic artifact state {path}: {exc}") from exc
    if int(value.get("schema_version", -1)) != AUTO_ARTIFACT_STATE_SCHEMA_VERSION:
        raise SnapshotError(f"unsupported automatic artifact state: {path}")
    return value


def _new_state(dataset: str, relative_root: str, activity_ns: int) -> dict[str, Any]:
    return {
        "schema_version": AUTO_ARTIFACT_STATE_SCHEMA_VERSION,
        "dataset": dataset,
        "artifact_relative_root": relative_root,
        "state": "observed-complete",
        "last_used_ns": activity_ns,
        "last_used_at": _utc_iso_from_ns(activity_ns),
        "observed_at": _utc_iso_from_ns(time.time_ns()),
    }


def _spec(dataset: str, relative_root: str, stable_hours: float) -> ColdArtifactSpec:
    return ColdArtifactSpec(
        dataset=dataset,
        relative_root=relative_root,
        maximum_file_bytes=None,
        loose_file_threshold_bytes=8 * 1024 * 1024,
        pack_buckets=32,
        min_stable_hours=stable_hours,
        completion_contract="training-lifecycle-v1",
    )


def _release_matches_source(sync_root: Path, source: Path, spec: ColdArtifactSpec):
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if metadata.get("artifact_relative_root") != spec.relative_root:
        raise SnapshotError("packed artifact root disagrees with automatic state")
    verify_packed_snapshot(sync_root, resolved, materialized_path=source)
    return resolved


def maintain_completed_artifacts(
    artifact_root: Path,
    sync_root: Path,
    state_root: Path,
    *,
    scope: str = "ablations",
    stable_hours: float = 24.0,
    retention_days: float = 7.0,
    max_publish: int = 1,
    apply: bool = False,
    peer_converged: Callable[[], Mapping[str, Any]] | None = None,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Publish complete runs and safely evict expired local sources.

    Publishing and deletion are separated across invocations. A run published
    during this call cannot be deleted until a later call independently
    re-resolves and verifies its release.
    """

    artifact_root = artifact_root.resolve()
    sync_root = sync_root.resolve()
    state_root = state_root.resolve()
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    expiry_ns = int(retention_days * 86_400 * 1_000_000_000)
    state_dir = state_root / "automatic"
    if apply:
        state_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    publish_slots = max(0, int(max_publish))
    convergence: Mapping[str, Any] | None = None

    # Keep at most one peer-unconfirmed publication wave in flight. This
    # bounds packed-store growth when the peer or network is slow.
    has_published_source = False
    for state_path in sorted(state_dir.glob("*.json")):
        state = _read_state(state_path)
        if state is None or state.get("state") != "published":
            continue
        relative = PurePosixPath(str(state.get("artifact_relative_root", "")))
        if relative.is_absolute() or ".." in relative.parts:
            raise SnapshotError(f"unsafe root in automatic artifact state: {state_path}")
        if artifact_root.joinpath(*relative.parts).is_dir():
            has_published_source = True
            break
    publication_blocked = False
    if has_published_source:
        if peer_converged is None:
            publication_blocked = True
        else:
            convergence = peer_converged()
            publication_blocked = not bool(convergence.get("ok"))

    candidates = discover_completed_runs(artifact_root, scope)
    candidates.sort(key=newest_activity_ns)
    for source in candidates:
        relative_root = source.relative_to(artifact_root).as_posix()
        dataset = automatic_dataset_name(relative_root)
        state_path = state_dir / f"{dataset}.json"
        state = _read_state(state_path)
        premaintenance_activity_ns = newest_activity_ns(source)
        if state is None:
            state = _new_state(dataset, relative_root, premaintenance_activity_ns)
        elif state.get("artifact_relative_root") != relative_root:
            raise SnapshotError(f"automatic artifact state root mismatch: {state_path}")

        references = artifact_process_references(
            source, artifact_root.joinpath(*PurePosixPath(scope).parts)
        )
        if references:
            state["last_used_ns"] = current_ns
            state["last_used_at"] = _utc_iso_from_ns(current_ns)
            state["last_use_evidence"] = references
        else:
            # Do not consult atime again after enrollment: verification and
            # packing read every source file and must not renew their own lease.
            state["last_used_at"] = _utc_iso_from_ns(int(state["last_used_ns"]))

        row: dict[str, Any] = {
            "dataset": dataset,
            "relative_root": relative_root,
            "action": "keep",
            "reason": "not-yet-published",
            "last_used_at": state["last_used_at"],
            "process_references": references,
        }
        spec = _spec(dataset, relative_root, stable_hours)
        previously_published = bool(state.get("snapshot_id"))
        resolved = None
        try:
            resolved = _release_matches_source(sync_root, source, spec)
        except (OSError, SnapshotError):
            if not references and publish_slots > 0 and not publication_blocked:
                try:
                    validate_cold_artifact_source(artifact_root, spec)
                    if apply:
                        resolved = publish_cold_artifact(
                            sync_root, artifact_root, spec, repo_root=artifact_root.parent
                        )
                        verify_packed_snapshot(sync_root, resolved, materialized_path=source)
                        publish_slots -= 1
                        state.update(
                            {
                                "state": "published",
                                "snapshot_id": resolved.manifest["snapshot_id"],
                                "manifest_sha256": resolved.manifest_sha256,
                                "published_ns": current_ns,
                                "published_at": _utc_iso_from_ns(current_ns),
                            }
                        )
                        row.update(action="published", reason="verified-packed-release")
                    else:
                        row.update(action="would-publish", reason="eligible-complete-run")
                        publish_slots -= 1
                except (OSError, SnapshotError) as exc:
                    row["reason"] = f"publish-blocked: {exc}"
            elif references:
                row["reason"] = "in-use"
            elif publication_blocked:
                row["reason"] = "awaiting-peer-before-next-publish"
            else:
                row["reason"] = "publish-limit"

        if resolved is not None:
            state.update(
                {
                    "state": "published",
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest_sha256": resolved.manifest_sha256,
                }
            )
            expired = current_ns - int(state["last_used_ns"]) >= expiry_ns
            published_this_call = not previously_published and row["action"] == "published"
            if references:
                row.update(action="keep", reason="in-use")
            elif not expired:
                row.update(action="keep", reason="retention-active")
            elif published_this_call:
                row["reason"] = "awaiting-independent-peer-check"
            elif peer_converged is None:
                row["reason"] = "peer-check-unavailable"
            else:
                if convergence is None:
                    convergence = peer_converged()
                if not bool(convergence.get("ok")):
                    row["reason"] = "peer-not-converged"
                    row["peer"] = dict(convergence)
                else:
                    _release_matches_source(sync_root, source, spec)
                    late_references = artifact_process_references(
                        source,
                        artifact_root.joinpath(*PurePosixPath(scope).parts),
                    )
                    postverification_activity_ns = newest_activity_ns(source)
                    if late_references:
                        state["last_used_ns"] = current_ns
                        state["last_used_at"] = _utc_iso_from_ns(current_ns)
                        state["last_use_evidence"] = late_references
                        row.update(
                            action="keep",
                            reason="in-use-after-verification",
                            process_references=late_references,
                        )
                    elif postverification_activity_ns != premaintenance_activity_ns:
                        state["last_used_ns"] = max(
                            int(state["last_used_ns"]),
                            postverification_activity_ns,
                        )
                        state["last_used_at"] = _utc_iso_from_ns(
                            int(state["last_used_ns"])
                        )
                        row.update(action="keep", reason="source-changed-during-verification")
                    elif apply:
                        shutil.rmtree(source)
                        state.update(
                            {
                                "state": "cold-only",
                                "evicted_ns": current_ns,
                                "evicted_at": _utc_iso_from_ns(current_ns),
                                "eviction_reason": "retention-expired-peer-converged",
                                "peer_evidence": dict(convergence),
                            }
                        )
                        row.update(action="evicted", reason="verified-cold-and-peer-converged")
                    else:
                        row.update(action="would-evict", reason="verified-cold-and-peer-converged")

        if apply:
            atomic_write_json(state_path, state)
        rows.append(row)

    return {
        "schema_version": AUTO_ARTIFACT_STATE_SCHEMA_VERSION,
        "scope": scope,
        "apply": apply,
        "candidates": len(candidates),
        "published": sum(row["action"] == "published" for row in rows),
        "evicted": sum(row["action"] == "evicted" for row in rows),
        "would_publish": sum(row["action"] == "would-publish" for row in rows),
        "would_evict": sum(row["action"] == "would-evict" for row in rows),
        "rows": rows,
    }
