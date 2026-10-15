"""Explicitly unwanted testing artifacts, separate from recoverable retirement.

Only a content-pinned, individually reviewed user disposition authorizes this
path. It waives preservation, never live consumers, pins, holds or path checks.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import time
import uuid

from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, atomic_write_json
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.remote_legacy_return import (
    active_configuration_references, identity, metadata_tree, real, signature,
    unlink_preserved_file_names,
)
from stockagent.data_sync.training_return import shared_inode_references

from stockagent.data_sync.testing_disposition import (
    CONTRACT, DEFAULT_POLICY, load_policy, artifact_roots, discarded_root, manifest_discarded,
)


def cold_discard_plan(cold_root: Path, policy: dict, *, materialized_root: Path) -> dict:
    """Reference graph for test-only CAS members; retain all manifest receipts.

    Restricted publications and historical manifests count as references too.
    Current use/pins override the user's discard decision.
    """
    from stockagent.data_sync.packed_snapshots import _validate_manifest
    from stockagent.data_sync.packed_backup import object_descriptor
    cold_root = real(cold_root)
    pins = _pinned_snapshot_ids(materialized_root)
    retained, unwanted, generation, releases, blockers = set(), {}, [], [], []
    paths = sorted((cold_root / "manifests").glob("*/*.json"))
    # An interrupted prepared publication can hold CAS members before its head.
    for p in sorted((cold_root / ".staging").rglob("*.json")):
        value = json.loads(real(p).read_bytes())
        if isinstance(value, dict) and "archive" in value and "dataset" in value:
            paths.append(p)
    for p in paths:
        before = signature(real(p).lstat())
        raw = p.read_bytes(); m = json.loads(raw)
        _validate_manifest(m)
        if signature(p.lstat()) != before:
            raise SnapshotError("cold reference manifest changed during discard plan")
        digest = hashlib.sha256(raw).hexdigest()
        generation.append({"relative": p.relative_to(cold_root).as_posix(), "sha256": digest, "signature": before})
        refs = [m["archive"]["inventory"], *m["archive"]["objects"]]
        if p.is_relative_to(cold_root / ".staging") or not manifest_discarded(m, policy):
            retained.update(r["relpath"] for r in refs)
            continue
        releases.append({"dataset": m["dataset"], "snapshot_id": m["snapshot_id"],
                         "manifest_relative": p.relative_to(cold_root).as_posix(), "manifest_sha256": digest})
        if m["snapshot_id"] in pins:
            blockers.append("pinned-testing-release:" + m["snapshot_id"])
        for r in refs:
            if r["relpath"] in unwanted and any(unwanted[r["relpath"]][k] != r[k]
                                                for k in ("relpath", "sha256", "bytes")):
                raise SnapshotError("cold manifests disagree on a testing object")
            unwanted[r["relpath"]] = r
    candidates, absent = [], []
    for relative in sorted(set(unwanted) - retained):
        ref = unwanted[relative]
        if object_descriptor(relative) != ref["sha256"]:
            raise SnapshotError("testing object is outside canonical CAS")
        p = real(cold_root / _safe_relative_path(relative, "testing cold object"))
        if not p.exists():
            absent.append(relative)
            continue
        info = p.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != ref["bytes"]:
            raise SnapshotError("testing cold object is redirected, shared or changed")
        if process_references(p):
            blockers.append("active-cold-object:" + relative)
        candidates.append({"relative": relative, "sha256": ref["sha256"], "signature": signature(info),
                           "allocated_bytes": info.st_blocks * 512})
    result = {"contract": CONTRACT, "policy_identity_sha256": policy["identity_sha256"],
              "releases": releases, "manifest_generation": generation, "objects": candidates,
              "already_absent_objects": absent, "retained_shared_objects": sorted(set(unwanted) & retained),
              "blockers": sorted(set(blockers)), "reclaimable_allocated_bytes": sum(r["allocated_bytes"] for r in candidates),
              "manifest_receipts_deleted": False, "nas_deletion_claimed": False}
    result["identity_sha256"] = identity(result)
    return result


def _recheck_cold_graph(cold_root: Path, plan: dict, *, materialized_root: Path) -> None:
    paths = sorted((cold_root / "manifests").glob("*/*.json"))
    expected = {r["relative"]: r for r in plan["manifest_generation"]}
    for p in (cold_root / ".staging").rglob("*.json"):
        # Existing staging members were fully parsed outside the mutation
        # owner. Any new staging metadata invalidates reuse conservatively.
        relative = p.relative_to(cold_root).as_posix()
        if relative not in expected:
            raise SnapshotError("cold staging changed after testing discard audit")
        paths.append(p)
    if {p.relative_to(cold_root).as_posix() for p in paths} != set(expected):
        raise SnapshotError("cold manifest generation changed after testing discard audit")
    for parent in {p.parent for p in paths}:
        real(parent)
    for p in paths:
        if signature(p.lstat()) != expected[p.relative_to(cold_root).as_posix()]["signature"]:
            raise SnapshotError("cold manifest identity changed after testing discard audit")
    pins = _pinned_snapshot_ids(materialized_root)
    if any(r["snapshot_id"] in pins for r in plan["releases"]):
        raise SnapshotError("testing release became pinned")
    for r in plan["objects"]:
        p = real(cold_root / r["relative"])
        if signature(p.lstat()) != r["signature"] or process_references(p):
            raise SnapshotError("testing object changed or became active")


def apply_cold_discard(cold_root: Path, policy: dict, plan: dict, *, materialized_root: Path, receipt: Path,
                       _audit_started_monotonic: float | None = None) -> dict:
    """Caller holds the canonical publication owner for this complete operation."""
    if receipt.exists():
        raise SnapshotError("retain the previous cold testing-discard receipt")
    if _audit_started_monotonic is None:
        current = cold_discard_plan(cold_root, policy, materialized_root=materialized_root)
        if current != plan:
            raise SnapshotError("cold testing-discard graph changed or live protection applies")
    else:
        age = time.monotonic() - _audit_started_monotonic
        if not 0 <= age <= 1500 or plan["policy_identity_sha256"] != policy["identity_sha256"]:
            raise SnapshotError("same-process testing discard graph expired or changed")
        _recheck_cold_graph(cold_root, plan, materialized_root=materialized_root)
    if plan["blockers"]:
        raise SnapshotError("cold testing-discard graph changed or live protection applies")
    atomic_write_json(receipt, {"state": "prepared", "plan": plan, "deleted_objects": []})
    deleted = []
    for row in plan["objects"]:
        p = real(cold_root / row["relative"])
        if signature(p.lstat()) != row["signature"] or process_references(p):
            raise SnapshotError("testing object changed or became active; remainder retained")
        # Final-link helper rechecks opened inode identity through unlink.
        reclaimed = unlink_preserved_file_names(p.parent, [{"path": p.name, "kind": "file", "signature": row["signature"]}])
        deleted.append({"relative": row["relative"], "sha256": row["sha256"], "reclaimed_allocated_bytes": reclaimed})
        atomic_write_json(receipt, {"state": "deleting", "plan": plan, "deleted_objects": deleted})
    result = {"state": "testing-only-cold-payload-discarded", "policy_identity_sha256": policy["identity_sha256"],
              "deleted_objects": deleted, "reclaimed_allocated_bytes": sum(r["reclaimed_allocated_bytes"] for r in deleted),
              "retained_shared_objects": plan["retained_shared_objects"], "manifest_receipts_deleted": False,
              "nas_deletion_claimed": False, "recovery_claimed": False}
    atomic_write_json(receipt, {"state": "discarded", "plan": plan, "result": result})
    return result


def _observed(source: Path, kind: str) -> dict:
    source = real(source)
    info = source.lstat()
    if kind == "directory":
        if not stat.S_ISDIR(info.st_mode):
            raise SnapshotError("testing directory target changed type")
        result = metadata_tree(source)
    else:
        if not stat.S_ISREG(info.st_mode):
            raise SnapshotError("testing file target changed type")
        result = {"rows": [{"path": source.name, "kind": "file", "signature": signature(info), "cross_filesystem": False}],
                  "logical_bytes": info.st_size, "files": 1}
        result["fingerprint"] = identity(result)
    return {**result, "root_signature": signature(info)}


def _same_root_before_after_rename(before: list, after: list, kind: str) -> bool:
    # Overlay/NTFS may change directory allocation/size on rename. Its exact
    # child inventory establishes content; file size remains mandatory.
    return (before[:2] == after[:2] and before[3] == after[3] and before[5:] == after[5:]
            and (kind == "directory" or before[2] == after[2]))


def restore_unchanged_discard_quarantine(policy: dict, target: dict, *, state_root: Path,
                                        repo_root: Path, engineering_root: Path,
                                        materialized_root: Path, cold_root: Path) -> bool:
    """Resume only this owner's unchanged, exact journal; preserve unknown data."""
    if target not in policy["targets"]:
        raise SnapshotError("quarantine resume is outside the user disposition")
    state_root = real(state_root)
    key = identity(target)
    journal = real(state_root / (key + ".json"))
    if not journal.exists():
        return False
    with real(state_root / (key + ".lock")).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        value = json.loads(journal.read_bytes())
        if value.get("state") != "quarantined":
            return False
        plan = value["plan"]
        base = repo_root / "artifacts" if target["base"] == "artifact" else engineering_root
        source = real(base / target["relative"])
        q = real(Path(value["quarantine"]))
        if (plan["policy_identity_sha256"] != policy["identity_sha256"] or plan["target"] != target
                or plan["source"] != str(source) or source.exists() or q.parent.parent != state_root
                or not q.parent.name.startswith("quarantine-") or q.name != source.name):
            raise SnapshotError("testing quarantine/source identity changed")
        current = _observed(q, target["kind"])
        old = plan["observed"]
        if (current["rows"] != old["rows"] or not _same_root_before_after_rename(
                old["root_signature"], current["root_signature"], target["kind"])
                or process_references(q) or _gates(source, current, repo_root, materialized_root, cold_root)):
            raise SnapshotError("interrupted testing quarantine changed or became active")
        q.rename(source)
        q.parent.rmdir()
        atomic_write_json(journal, {**value, "state": "rolled-back-for-fresh-discard", "resumed_at_epoch": time.time()})
        return True


def _gates(source: Path, observed: dict, repo_root: Path, materialized_root: Path, cold_root: Path) -> list[str]:
    blockers = []
    if any(r["kind"] == "unsupported" or r["cross_filesystem"] for r in observed["rows"]):
        blockers.append("unsupported-or-cross-filesystem-entry")
    scope = repo_root / "artifacts" if source.is_relative_to(repo_root / "artifacts") else source.parent
    if (artifact_process_references(source, scope) or active_configuration_references(source, repo_root)
            or shared_inode_references(observed)):
        blockers.append("active-process-config-hold-or-shared-inode")
    if artifact_service_references([source], repo_root).get(str(source)):
        blockers.append("service-dependency")
    pins = _pinned_snapshot_ids(materialized_root)
    if pins:
        for p in (cold_root / "manifests").glob("*/*.json"):
            if p.stem not in pins:
                continue
            m = json.loads(real(p).read_bytes()); meta = m.get("metadata", {})
            relative = meta.get("source_relative_root", meta.get("artifact_relative_root"))
            if isinstance(relative, str):
                protected = repo_root / "artifacts" / _safe_relative_path(relative, "pinned artifact")
                if source == protected or source in protected.parents or protected in source.parents:
                    blockers.append("pinned-release")
    return sorted(set(blockers))


def discard_target(policy: dict, target: dict, *, repo_root: Path, engineering_root: Path,
                   state_root: Path, materialized_root: Path, cold_root: Path,
                   apply: bool = False, expected_plan_identity: str | None = None) -> dict:
    if target not in policy["targets"] or type(apply) is not bool:
        raise SnapshotError("testing deletion requires one exact approved target")
    base = repo_root / "artifacts" if target["base"] == "artifact" else engineering_root
    source = real(base / target["relative"])
    state_root = real(state_root)
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if state_root.is_relative_to(source) or source.is_relative_to(state_root):
        raise SnapshotError("discard evidence and target overlap")
    key = identity(target)
    with real(state_root / (key + ".lock")).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = state_root / (key + ".json")
        prior = json.loads(real(journal).read_bytes()) if journal.exists() else {}
        if prior.get("state") in {"prepared", "quarantined"}:
            raise SnapshotError("interrupted discard quarantine needs exact recovery")
        if not source.exists():
            return {"state": "already-absent", "target": target, "deleted": False,
                    "reclaimed_allocated_bytes": 0}
        observed = _observed(source, target["kind"])
        blockers = _gates(source, observed, repo_root, materialized_root, cold_root)
        plan = {"contract": CONTRACT, "policy_identity_sha256": policy["identity_sha256"],
                "target": target, "source": str(source), "observed": observed, "blockers": blockers}
        plan["identity_sha256"] = identity(plan)
        if not apply:
            return {**plan, "state": "discard-dry-run", "deleted": False}
        if blockers or plan["identity_sha256"] != expected_plan_identity:
            raise SnapshotError("testing discard dry run changed or live protection applies: "
                                + json.dumps({"blockers": blockers, "generation_changed": plan["identity_sha256"] != expected_plan_identity}))
        if _observed(source, target["kind"]) != observed or _gates(source, observed, repo_root, materialized_root, cold_root):
            raise SnapshotError("testing discard changed before quarantine")
        quarantine_parent = state_root / ("quarantine-" + uuid.uuid4().hex)
        quarantine_parent.mkdir(mode=0o700)
        if quarantine_parent.stat().st_dev != source.stat().st_dev:
            quarantine_parent.rmdir()
            raise SnapshotError("testing quarantine crosses filesystem")
        quarantine = quarantine_parent / source.name
        atomic_write_json(journal, {"state": "prepared", "plan": plan, "quarantine": str(quarantine)})
        source.rename(quarantine)
        atomic_write_json(journal, {"state": "quarantined", "plan": plan, "quarantine": str(quarantine)})
        current = _observed(quarantine, target["kind"])
        # Rename may change the root inode's ctime, never file members/content.
        if (current["rows"] != observed["rows"] and target["kind"] == "directory"
                or current["files"] != observed["files"] or current["logical_bytes"] != observed["logical_bytes"]
                or not _same_root_before_after_rename(observed["root_signature"], current["root_signature"], target["kind"])
                or process_references(quarantine) or _gates(source, current, repo_root, materialized_root, cold_root)):
            raise SnapshotError("quarantined testing artifact changed or became active; retained")
        if target["kind"] == "file":
            old, new = observed["rows"][0]["signature"], current["rows"][0]["signature"]
            if old[:4] != new[:4] or old[5:] != new[5:]:
                raise SnapshotError("testing file changed during quarantine")
        unlink_root = quarantine if target["kind"] == "directory" else quarantine_parent
        reclaimed = unlink_preserved_file_names(unlink_root, current["rows"])
        if target["kind"] == "directory":
            for r in sorted(current["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
                if r["kind"] == "directory":
                    (quarantine / r["path"]).rmdir()
            quarantine.rmdir()
        quarantine_parent.rmdir()
        result = {"state": "discarded-without-preservation", "deleted": True, "target": target,
                  "policy_identity_sha256": policy["identity_sha256"], "files": current["files"],
                  "logical_bytes": current["logical_bytes"], "reclaimed_allocated_bytes": reclaimed,
                  "recovery_claimed": False, "external_shared_names_deleted": False, "at_epoch": time.time()}
        atomic_write_json(journal, {"state": "discarded", "plan": plan, "result": result})
        return result
