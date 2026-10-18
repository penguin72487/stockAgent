"""Lossless, lock-coherent sharing of completed v12 physical minute sessions.

This is not artifact retirement or cold GC. Every logical file and every
manifest/READY byte remains. Canonical cache writers replace files atomically
under the same per-generation flock; they never write a shared payload in place.
No separate object pool is needed: generation names themselves own all links.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid
from typing import Any, Callable

from stockagent.data_sync.desync_snapshots import atomic_write_json, _fsync_directory
from stockagent.data_sync.immutable_cache_files import (
    payload_info as _info, stable_payload_hash as _stable_hash,
)
from stockagent.data_sync.immutable_cache_compaction import (
    allocated_bytes as _allocated, duplicate_payload_groups as _groups,
    inode_key as _inode, share_locked_payload_group,
)
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.physical_cache_reuse import (
    SOURCE_ABI as SOURCE_ABI,
    SESSION_ENCODING as SESSION_ENCODING,
    _GENERATION, _generation_inventory, _metadata, _real_directory, _writer_lock,
)

CONTRACT = "physical_minute_session_sharing_v1"
_CACHE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


def _json_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def policy_roots(path: Path, repo: Path, *, contract: str = CONTRACT) -> list[Path]:
    """Explicit local allowlist, not a synchronized deletion policy."""
    path = path.absolute()
    before = _info(path)
    if (os.geteuid() != 0 or path.resolve() != path or before["uid"] != 0
            or before["mode"] & 0o022 or before["size"] > 4096):
        raise ValueError("service cache policy must be a bounded root-owned local file")
    policy, _digest = _metadata(path)
    if _info(path) != before or path.resolve() != path:
        raise ValueError("service cache policy changed during validation")
    if set(policy) != {"schema_version", "contract", "cache_roots"} or (
            type(policy["schema_version"]) is not int or policy["schema_version"] != 1
            or policy["contract"] != contract):
        raise ValueError("unknown service cache compaction policy")
    roots = policy["cache_roots"]
    if (not isinstance(roots, list) or not 1 <= len(roots) <= 8
            or any(type(value) is not str for value in roots) or len(set(roots)) != len(roots)):
        raise ValueError("explicit bounded unique cache roots required")
    result = []
    for name in roots:
        relative = Path(name)
        if (relative.parent != Path("artifacts/cache") or not _CACHE_NAME.fullmatch(relative.name)
                or relative.as_posix() != name):
            raise ValueError("policy may name only direct artifacts/cache children")
        root = repo.absolute() / relative
        if root.exists() or root.is_symlink():
            result.append(_real_directory(root))
    return result


def _inventory(root: Path, generations: list[Path]) -> tuple[list[dict], list[dict], list[dict]]:
    files, proofs, skipped = [], [], []
    # Inventory only holds one writer lock at a time. Full payload SHA runs
    # optimistically without writer locks, then requires an unchanged inventory.
    for generation in generations:
        members, committed, retained = _generation_inventory(root, generation)
        files.extend(members)
        proofs.extend(committed)
        skipped.extend(retained)
    return files, proofs, skipped


def _share_group(root: Path, group: list[list[dict]], proofs: dict[str, dict],
                 touched: set[Path], report: dict,
                 retired_handles: ExitStack) -> set[tuple[int, int]]:
    """Short nonblocking lock scope, not a whole-cache maintenance outage."""
    generations = sorted({Path(item["path"]).parts[0] for names in group for item in names})
    with ExitStack() as locks:
        for generation in generations:
            locks.enter_context(_writer_lock(root / f".{generation}.lock"))
        for generation in generations:
            proof = proofs[generation]
            if (_info(root / generation / "manifest.json") != proof["manifest_identity"]
                    or _info(root / generation / "READY.json") != proof["ready_identity"]):
                raise ValueError("physical generation changed before sharing")
        return share_locked_payload_group(
            root, group, touched=touched, report=report,
            retired_handles=retired_handles, temporary_tag="session-share")


def compact_physical_minute_cache(
    root: Path, receipt_dir: Path, *, apply: bool = False,
    expected_fingerprint: str | None = None,
    full_audit: bool = True,
    reference_probe: Callable = process_references_many,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    """Audit/apply one exact cache; busy or referenced caches remain untouched.

    SHA every selected session inode, retain all logical names, and replace only
    supported byte-identical payloads. Readonly permissions protect shared data;
    the ABI's lock+atomic-replace writer still rebuilds normally. A crash leaves
    old or new names with identical bytes, never a missing unique object.
    """
    if type(full_audit) is not bool:
        raise ValueError("full_audit must be an explicit boolean")
    root = _real_directory(root)
    receipt_dir = receipt_dir.absolute()
    if receipt_dir.resolve(strict=False) != receipt_dir or (
            receipt_dir == root or root in receipt_dir.parents or receipt_dir in root.parents):
        raise ValueError("compaction receipts must be outside the cache")
    receipt_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    result_path = receipt_dir / f"{stamp}.result.json"
    report = {"contract": CONTRACT, "attempt_id": stamp, "root": str(root), "apply": apply,
              "state": "running", "started_at": datetime.now(UTC).isoformat(),
              "logical_paths_removed": 0, "cold_objects_removed": 0,
              "source_or_manifest_bytes_changed": False, "replaced_paths": 0,
              "retired_allocated_bytes": 0, "result_path": str(result_path)}
    touched: set[Path] = set()
    started, last_progress = time.monotonic(), [0.0]
    def emit(stage, **fields):
        value = {"contract": CONTRACT, "attempt_id": stamp, "stage": stage, "root": str(root),
                 "elapsed_seconds": time.monotonic() - started, **fields}
        atomic_write_json(receipt_dir / "progress.json", value)
        if progress:
            progress(value)
    try:
        with ExitStack() as locks:
            locks.enter_context(_writer_lock(root / ".session-compaction.lock"))
            generations = sorted(path for path in root.iterdir() if _GENERATION.fullmatch(path.name))
            files, proofs, skipped = _inventory(root, generations)
            inode_keys = {_inode(item) for item in files}
            references = reference_probe([root], inode_keys=inode_keys)
            if references:
                report.update(state="deferred", reason="process_references", references=references)
            else:
                groups, external = _groups(files)
                selected_files = files if full_audit else [
                    item for group in groups for aliases in group for item in aliases]
                verified, verified_bytes = set(), 0
                total_bytes = sum({_inode(item): item["size"] for item in selected_files}.values())
                emit("hash-sessions", completed_bytes=0, total_bytes=total_bytes)
                for item in selected_files:
                    key = _inode(item)
                    if key not in verified:
                        if _stable_hash(root / item["path"], _info(root / item["path"])) != item["sha256"]:
                            raise ValueError(f"physical session SHA mismatch: {item['path']}")
                        verified.add(key)
                        verified_bytes += item["size"]
                    if time.monotonic() - last_progress[0] >= 10:
                        emit("hash-sessions", completed_bytes=verified_bytes, total_bytes=total_bytes)
                        last_progress[0] = time.monotonic()
                current_files, current_proofs, _ = _inventory(root, generations)
                if current_files != files or current_proofs != proofs:
                    raise ValueError("physical cache changed during full SHA audit")
                fingerprint = _json_digest({"contract": CONTRACT, "root": str(root),
                                            "files": files, "proofs": proofs})
                if expected_fingerprint is not None and fingerprint != expected_fingerprint:
                    raise ValueError("physical cache dry-run fingerprint changed")
                plan_path = receipt_dir / f"{stamp}.plan.json"
                plan = {"contract": CONTRACT, "root": str(root), "fingerprint": fingerprint,
                        "files": files, "proofs": proofs,
                        "groups": [[[row["path"] for row in names] for names in group] for group in groups]}
                atomic_write_json(plan_path, plan)
                report.update(plan_path=str(plan_path), plan_fingerprint=fingerprint,
                              verified_session_inodes=len(verified), verified_session_bytes=verified_bytes,
                              hash_scope="all_supported_payloads" if full_audit else "sharing_candidates_only",
                              metadata_only_payload_inodes=len(inode_keys - verified),
                              supported_generations=len(proofs), skipped_generations=skipped,
                              external_link_inodes_retained=external, duplicate_groups=len(groups),
                              candidate_allocated_bytes=sum(names[0]["allocated"] for group in groups for names in group[1:]),
                              allocated_before=_allocated(files))
                if not apply:
                    report["state"] = "audited"
                else:
                    references = reference_probe([root], inode_keys=inode_keys)
                    if references:
                        report.update(state="deferred", reason="process_references", references=references)
                    else:
                        retired_keys = set()
                        busy_groups = 0
                        proof_map = {proof["generation"]: proof for proof in proofs}
                        retired_proven_bytes, references = 0, []
                        allocations = {_inode(item): item["allocated"] for item in files}
                        with ExitStack() as retired_handles:
                            def flush_retired():
                                nonlocal retired_proven_bytes
                                if retired_keys:
                                    retained = reference_probe([], inode_keys=set(retired_keys))
                                    references.extend(retained)
                                    if not retained:
                                        retired_proven_bytes += sum(allocations[key] for key in retired_keys)
                                retired_handles.close()
                                retired_keys.clear()
                            for index, group in enumerate(groups):
                                try:
                                    retired_keys.update(_share_group(
                                        root, group, proof_map, touched, report, retired_handles))
                                except BlockingIOError:
                                    busy_groups += 1
                                    continue
                                if len(retired_keys) >= 128:
                                    flush_retired()
                                if time.monotonic() - last_progress[0] >= 10:
                                    emit("share-sessions", completed_groups=index + 1, total_groups=len(groups),
                                         replaced_paths=report["replaced_paths"])
                                    last_progress[0] = time.monotonic()
                            flush_retired()
                        after, after_proofs, _ = _inventory(root, generations)
                        if after_proofs != proofs or [(row["path"], row["sha256"], row["size"]) for row in after] != [
                                (row["path"], row["sha256"], row["size"]) for row in files]:
                            raise ValueError("physical manifest or logical source changed")
                        reduction = _allocated(files) - _allocated(after)
                        report.update(state="partially_compacted" if busy_groups else "compacted",
                                      busy_groups_retained=busy_groups, allocated_after=_allocated(after),
                                      allocated_namespace_reduction_bytes=reduction,
                                      retired_allocated_bytes=min(reduction, retired_proven_bytes),
                                      retired_inode_checks="descriptor_pinned_bounded_batches_v1",
                                      retained_open_inode_references=references)
    except BlockingIOError:
        report.update(state="deferred", reason="writer_lock_busy")
    except (KeyboardInterrupt, SystemExit) as error:
        report.update(state="interrupted", error_type=type(error).__name__)
        raise
    except Exception as error:
        report.update(state="failed", error_type=type(error).__name__, error=str(error))
    finally:
        for directory in sorted(touched):
            _fsync_directory(directory)
        report.update(completed_at=datetime.now(UTC).isoformat(), elapsed_seconds=time.monotonic() - started)
        atomic_write_json(result_path, report)
        atomic_write_json(receipt_dir / "latest.json", report)
    return report
