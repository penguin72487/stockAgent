"""Explicitly unwanted testing artifacts, separate from recoverable retirement.

Only a content-pinned, individually reviewed user disposition authorizes this
path. It waives preservation, never live consumers, pins, holds or path checks.
"""
from __future__ import annotations

import fcntl
import hashlib
from itertools import islice
import json
import os
from pathlib import Path, PurePosixPath
import stat
import time
import uuid

from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, atomic_write_bytes
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references, process_references_many
from stockagent.data_sync.remote_legacy_return import (
    active_configuration_references, identity, metadata_tree, real, signature,
    unlink_preserved_file_names,
)
from stockagent.data_sync.training_return import shared_inode_references

from stockagent.data_sync.testing_disposition import (
    CONTRACT, DEFAULT_POLICY, load_policy, artifact_roots, discarded_root, manifest_discarded,
    target_path, LOCAL_TESTING_CONTRACT, LOCAL_COPY_CONTRACTS, D_WORKFLOW_TESTING_CONTRACT,
)


def write_discard_receipt(path: Path, value) -> None:
    """Persist exact POSIX names, including undecodable test-fixture bytes.

    JSON escapes preserve surrogate-escaped filenames through a readback.
    This encoding is for mutable discard evidence only; packed/source manifest
    serialization and the identities of existing plans remain unchanged.
    """
    content = (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':')) + '\n').encode('ascii')
    atomic_write_bytes(path, content)


def cold_discard_plan(cold_root: Path, policy: dict, *, materialized_root: Path) -> dict:
    """Reference graph for test-only CAS members; retain all manifest receipts.

    Restricted publications and historical manifests count as references too.
    Current use/pins override the user's discard decision.
    """
    if policy.get('contract') in LOCAL_COPY_CONTRACTS:
        raise SnapshotError('local performance-copy disposal cannot discard cold objects')
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
    write_discard_receipt(receipt, {"state": "prepared", "plan": plan, "deleted_objects": []})
    deleted = []
    for row in plan["objects"]:
        p = real(cold_root / row["relative"])
        if signature(p.lstat()) != row["signature"] or process_references(p):
            raise SnapshotError("testing object changed or became active; remainder retained")
        # Final-link helper rechecks opened inode identity through unlink.
        reclaimed = unlink_preserved_file_names(p.parent, [{"path": p.name, "kind": "file", "signature": row["signature"]}])
        deleted.append({"relative": row["relative"], "sha256": row["sha256"], "reclaimed_allocated_bytes": reclaimed})
        write_discard_receipt(receipt, {"state": "deleting", "plan": plan, "deleted_objects": deleted})
    result = {"state": "testing-only-cold-payload-discarded", "policy_identity_sha256": policy["identity_sha256"],
              "deleted_objects": deleted, "reclaimed_allocated_bytes": sum(r["reclaimed_allocated_bytes"] for r in deleted),
              "retained_shared_objects": plan["retained_shared_objects"], "manifest_receipts_deleted": False,
              "nas_deletion_claimed": False, "recovery_claimed": False}
    write_discard_receipt(receipt, {"state": "discarded", "plan": plan, "result": result})
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
    retained_evidence = _guard_d_workflow_copy(policy, cold_root, target)
    state_root = real(state_root)
    key = identity(target)
    journal = real(state_root / (key + ".json"))
    if not journal.exists():
        return False
    with real(state_root / (key + ".lock")).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        value = json.loads(journal.read_bytes())
        if value.get("state") not in {"prepared", "quarantined", "unlink_started", "unlink_finished"}:
            return False
        plan = value["plan"]
        if policy.get('contract') == D_WORKFLOW_TESTING_CONTRACT and plan.get('retained_evidence') != retained_evidence:
            raise SnapshotError('D benchmark comparison evidence changed; quarantine retained')
        source = target_path(policy, target, repo_root=repo_root, engineering_root=engineering_root)
        q = real(Path(value["quarantine"]))
        old_location = q.parent.parent == state_root and q.parent.name.startswith('quarantine-')
        local_location = (policy['contract'] in LOCAL_COPY_CONTRACTS
                          and q.parent.parent == source.parent
                          and q.parent.name.startswith('.stockagent-testing-quarantine-'))
        if (plan["policy_identity_sha256"] != policy["identity_sha256"] or plan["target"] != target
                or plan["source"] != str(source) or not (old_location or local_location)
                or q.name != source.name):
            raise SnapshotError("testing quarantine/source identity changed")
        if value['state'] == 'unlink_started' and not os.path.lexists(source) and not os.path.lexists(q):
            # The process may have died immediately after its one exact unlink
            # and before the result receipt.  A durable pre-unlink intent binds
            # this name; do not invent an allocated-byte reclamation amount.
            if (policy['contract'] not in LOCAL_COPY_CONTRACTS or target['kind'] != 'file'
                    or plan.get('identity_sha256') != identity({k: v for k, v in plan.items()
                                                                if k != 'identity_sha256'})):
                raise SnapshotError('interrupted testing file intent differs')
            if q.parent.exists():
                info = q.parent.lstat()
                if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                        or stat.S_IMODE(info.st_mode) != 0o700 or any(q.parent.iterdir())
                        or process_references(q.parent)):
                    raise SnapshotError('interrupted testing file quarantine is not empty or is active')
                q.parent.rmdir()
            result = {'state': 'discarded-without-preservation', 'deleted': True, 'target': target,
                'policy_identity_sha256': policy['identity_sha256'], 'files': 1,
                'logical_bytes': plan['observed']['logical_bytes'], 'reclaimed_allocated_bytes': 0,
                'reclaimed_allocation_unknown': True, 'reconciled_absent_after_unlink_intent': True,
                'recovery_claimed': False, 'external_shared_names_deleted': False, 'at_epoch': time.time()}
            write_discard_receipt(journal, {'state': 'discarded', 'plan': plan, 'result': result})
            return True
        if value['state'] == 'unlink_finished':
            result = value.get('result')
            if (policy['contract'] != D_WORKFLOW_TESTING_CONTRACT or target['kind'] != 'directory'
                    or plan.get('identity_sha256') != identity({k:v for k,v in plan.items() if k != 'identity_sha256'})
                    or not isinstance(result, dict) or result.get('target') != target
                    or result.get('policy_identity_sha256') != policy['identity_sha256']
                    or result.get('deleted') is not True or type(result.get('reclaimed_allocated_bytes')) is not int
                    or result['reclaimed_allocated_bytes'] < 0 or source.exists() or source.is_symlink()):
                raise SnapshotError('completed D benchmark quarantine identity changed')
            if q.exists():
                if (not q.is_dir() or any(q.iterdir())
                        or signature(q.lstat())[:2] != plan['observed']['root_signature'][:2]):
                    raise SnapshotError('completed D benchmark quarantine is not empty')
                empty = _observed(q, target['kind'])
                if (process_references(q)
                        or _gates(source, empty, repo_root, materialized_root, cold_root, allow_opaque_symlinks=True)
                        or _gates(q, empty, repo_root, materialized_root, cold_root, allow_opaque_symlinks=True)):
                    raise SnapshotError('completed D benchmark quarantine is in use')
                q.rmdir()
            if q.parent.exists():
                if not q.parent.is_dir() or any(q.parent.iterdir()):
                    raise SnapshotError('completed D benchmark quarantine parent is not empty')
                q.parent.rmdir()
            result = {**result, 'resumed_partial': True}
            write_discard_receipt(journal, {'state': 'discarded', 'plan': plan, 'result': result})
            return True
        if value['state'] == 'prepared' and source.exists() and not q.exists():
            # EXDEV/pre-rename interruption left the source untouched. Only
            # this exact unchanged generation and empty owned directory may
            # roll back; never infer that a prepared source was deleted.
            if (_observed(source, target['kind']) != plan['observed']
                    or not q.parent.is_dir() or any(q.parent.iterdir())
                    or q.parent.stat().st_uid != os.geteuid()
                    or stat.S_IMODE(q.parent.stat().st_mode) != 0o700):
                raise SnapshotError('prepared testing quarantine changed; retain source')
            q.parent.rmdir()
            write_discard_receipt(journal, {**value, 'state': 'rolled-back-for-fresh-discard',
                                       'resumed_at_epoch': time.time()})
            return True
        if source.exists() or not q.exists():
            raise SnapshotError('testing quarantine/source identity changed')
        current = _observed(q, target["kind"])
        old = plan["observed"]
        if (policy['contract'] == D_WORKFLOW_TESTING_CONTRACT and target['kind'] == 'directory'
                and current['rows'] != old['rows']):
            return _resume_partial_d_workflow_discard(policy, target, value, current, q, source, journal,
                repo_root, materialized_root, cold_root)
        rows_changed = current['rows'] != old['rows']
        if target['kind'] == 'file' and len(current['rows']) == len(old['rows']) == 1:
            before, after = old['rows'][0], current['rows'][0]
            rows_changed = (before['path'] != after['path'] or before['kind'] != after['kind']
                or before['signature'][:4] != after['signature'][:4]
                or before['signature'][5:] != after['signature'][5:])
        if (rows_changed or not _same_root_before_after_rename(
                old["root_signature"], current["root_signature"], target["kind"])
                or process_references(q) or _gates(source, current, repo_root, materialized_root, cold_root,
                    **({'allow_opaque_symlinks': True} if policy['contract'] in LOCAL_COPY_CONTRACTS else {}))):
            raise SnapshotError("interrupted testing quarantine changed or became active")
        q.rename(source)
        q.parent.rmdir()
        write_discard_receipt(journal, {**value, "state": "rolled-back-for-fresh-discard", "resumed_at_epoch": time.time()})
        return True


def _resume_partial_d_workflow_discard(policy, target, journal_value, current, quarantine, source, journal,
                                       repo_root, materialized_root, cold_root):
    """Finish a reviewed disposable trial after an interrupted partial unlink.

    A partial deletion cannot be rolled back. Continue only if every remaining
    name/inode came from the fixed plan, and no current consumer or pin applies.
    Earlier freed blocks are unknowable here and are never counted a second time.
    """
    plan = journal_value['plan']
    if plan.get('identity_sha256') != identity({k: v for k, v in plan.items() if k != 'identity_sha256'}):
        raise SnapshotError('partial D benchmark journal identity changed')
    original = plan['observed']
    before_root, after_root = original['root_signature'], current['root_signature']
    if before_root[:2] != after_root[:2] or before_root[5] != after_root[5]:
        raise SnapshotError('partial D benchmark quarantine root changed')
    planned = {row['path']: row for row in original['rows']}
    remaining = {row['path']: row for row in current['rows']}
    if len(planned) != len(original['rows']) or len(remaining) != len(current['rows']) or not remaining.keys() <= planned.keys():
        raise SnapshotError('partial D benchmark has unplanned names')
    removed_links = {}
    for name, row in planned.items():
        if name not in remaining and row['kind'] == 'file':
            key = tuple(row['signature'][:2])
            removed_links[key] = removed_links.get(key, 0) + 1
    for name, row in remaining.items():
        prior = planned[name]
        old, new = prior['signature'], row['signature']
        if (row['kind'] != prior['kind'] or row['cross_filesystem'] or prior['cross_filesystem']
                or old[:2] != new[:2] or old[5] != new[5]):
            raise SnapshotError('partial D benchmark member identity changed')
        if row['kind'] == 'file':
            removed = removed_links.get(tuple(old[:2]), 0)
            if (old[2:4] != new[2:4] or new[6] != old[6] - removed
                    or (not removed and old != new)):
                raise SnapshotError('partial D benchmark file changed')
        elif row['kind'] == 'unsupported' and old != new:
            raise SnapshotError('partial D benchmark opaque name changed')
    options = {'allow_opaque_symlinks': True}
    if (process_references(quarantine) or _gates(source, current, repo_root, materialized_root, cold_root, **options)
            or _gates(quarantine, current, repo_root, materialized_root, cold_root, **options)
            or _observed(quarantine, target['kind']) != current
            or _guard_d_workflow_copy(policy, cold_root, target) != plan.get('retained_evidence')):
        raise SnapshotError('partial D benchmark is active or changed; quarantine retained')
    write_discard_receipt(journal, {**journal_value, 'partial_resume': {
        'prior_names_absent': len(planned) - len(remaining), 'at_epoch': time.time()}})
    reclaimed = unlink_preserved_file_names(quarantine, current['rows'])
    reclaimed += _unlink_opaque_links(quarantine, current['rows'])
    for row in sorted(current['rows'], key=lambda r: len(PurePosixPath(r['path']).parts), reverse=True):
        if row['kind'] == 'directory':
            (quarantine / row['path']).rmdir()
    result = {'state': 'discarded-without-preservation', 'deleted': True, 'target': target,
              'policy_identity_sha256': policy['identity_sha256'], 'files': current['files'],
              'logical_bytes': current['logical_bytes'], 'reclaimed_allocated_bytes': reclaimed,
              'prior_reclaimed_allocated_bytes_unknown': True, 'resumed_partial': True,
              'recovery_claimed': False, 'external_shared_names_deleted': False, 'at_epoch': time.time()}
    write_discard_receipt(journal, {'state': 'unlink_finished', 'plan': plan,
                                    'quarantine': str(quarantine), 'result': result})
    quarantine.rmdir()
    quarantine.parent.rmdir()
    write_discard_receipt(journal, {'state': 'discarded', 'plan': plan, 'result': result})
    return True


def _gates(source: Path, observed: dict, repo_root: Path, materialized_root: Path, cold_root: Path,
           *, allow_opaque_symlinks: bool = False) -> list[str]:
    blockers = []
    if any((r["kind"] == "unsupported" and not (
                allow_opaque_symlinks and (stat.S_ISLNK(r['signature'][5])
                                           or stat.S_ISFIFO(r['signature'][5]))))
           or r["cross_filesystem"] for r in observed["rows"]):
        blockers.append("unsupported-or-cross-filesystem-entry")
    scope = repo_root / "artifacts" if source.is_relative_to(repo_root / "artifacts") else source.parent
    if (artifact_process_references(source, scope) or active_configuration_references(source, repo_root)
            or shared_inode_references(observed)):
        blockers.append("active-process-config-hold-or-shared-inode")
    if allow_opaque_symlinks:
        # Bind mounts have different names but the same inode, even at nlink=1.
        # Include directories for cwd/dir-FD consumers and every file for mmap.
        inodes = {tuple(row['signature'][:2]) for row in observed['rows']}
        inodes.add(tuple(observed['root_signature'][:2]))
        if process_references_many((), inode_keys=inodes):
            blockers.append('active-inode-through-mount-alias')
    if artifact_service_references([source], repo_root).get(str(source)):
        blockers.append("service-dependency")
    pins = _pinned_snapshot_ids(materialized_root)
    if pins:
        for p in _pinned_manifest_paths(cold_root, pins):
            m = json.loads(real(p).read_bytes()); meta = m.get("metadata", {})
            relative = meta.get("source_relative_root", meta.get("artifact_relative_root"))
            if isinstance(relative, str):
                relative_path = _safe_relative_path(relative, "pinned artifact")
                base = repo_root if relative_path.parts[0].startswith("data_") else repo_root / "artifacts"
                protected = base / relative_path
                if source == protected or source in protected.parents or protected in source.parents:
                    blockers.append("pinned-release")
    return sorted(set(blockers))


def _pinned_manifest_paths(cold_root: Path, pins):
    """Read exact pin filenames across datasets, not every historical version.

    Enumerate small directories once; probe exact names after a bounded prefix
    of a large directory. No historical file count or persistent cache is used.
    Duplicate basenames in every dataset and literal metacharacters are kept;
    selected redirected paths still reach real() and fail the original gate.
    """
    names = set()
    for snapshot in pins:
        if (not isinstance(snapshot, str) or not snapshot
                or Path(snapshot).name != snapshot or snapshot in ('.', '..')):
            raise SnapshotError('pinned snapshot is not an exact manifest filename')
        names.add(snapshot + '.json')
    if not names:
        return
    directory = cold_root / 'manifests'
    if not directory.exists():
        return
    with os.scandir(directory) as datasets:
        for dataset in datasets:
            if not dataset.is_dir():
                continue
            limit = 2 * len(names)
            with os.scandir(dataset.path) as versions:
                prefix = list(islice(versions, limit + 1))
            observed = {entry.name: Path(entry.path) for entry in prefix if entry.name in names}
            yield from observed.values()
            if len(prefix) <= limit:
                continue
            for name in sorted(names - observed.keys()):
                path = Path(dataset.path) / name
                try:
                    path.lstat()
                except FileNotFoundError:
                    continue
                yield path


def _unlink_opaque_links(root: Path, rows: list[dict]) -> int:
    """Unlink exact symlinks/FIFOs in disposable quarantine, without reading them.

    A FIFO created by a finished test is a name, not an archived byte stream.
    O_PATH inspects its inode without waiting for a reader/writer. The caller
    still checks every inode consumer; devices and sockets remain unsupported.
    """
    reclaimed = 0
    for row in rows:
        if row['kind'] != 'unsupported' or not (
                stat.S_ISLNK(row['signature'][5]) or stat.S_ISFIFO(row['signature'][5])):
            continue
        path = root / _safe_relative_path(row['path'], 'testing symlink')
        parent = real(path.parent)
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            descriptor = os.open(path.name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 dir_fd=directory)
            try:
                before = os.fstat(descriptor)
                if (signature(before) != row['signature']
                        or not (stat.S_ISLNK(before.st_mode) or stat.S_ISFIFO(before.st_mode))
                        or before.st_nlink != 1
                        or signature(os.stat(path.name, dir_fd=directory, follow_symlinks=False)) != row['signature']):
                    raise SnapshotError('testing symlink changed before unlink; quarantine retained')
                os.unlink(path.name, dir_fd=directory)
                after = signature(os.fstat(descriptor))
                original = row['signature']
                if after[:4] != original[:4] or after[5] != original[5] or after[6] != 0:
                    raise SnapshotError('testing symlink inode changed during unlink; remainder retained')
                reclaimed += before.st_blocks * 512
            finally:
                os.close(descriptor)
        finally:
            os.close(directory)
    return reclaimed


def _guard_d_workflow_copy(policy: dict, cold_root: Path, target: dict):
    if policy.get('contract') == D_WORKFLOW_TESTING_CONTRACT:
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        from stockagent.data_sync import testing_disposition
        _check_d_primary_mount(cold_root)
        if target['kind'] == 'directory':
            round_name = PurePosixPath(target['relative']).parts[1]
            name = 'comparison_result.json' if round_name in {'round25', 'round27'} else 'comparison.json'
            path = real(testing_disposition.D_WORKFLOW_TESTING_ROOT / 'benchmarks' / round_name / name)
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            with os.fdopen(fd, 'rb') as handle:
                before = signature(os.fstat(handle.fileno()))
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode) or before[2] > 1024**2:
                    raise SnapshotError('D benchmark comparison evidence is not a bounded regular file')
                raw = handle.read(1024**2 + 1)
                if len(raw) != before[2] or signature(os.fstat(handle.fileno())) != before or signature(path.lstat()) != before:
                    raise SnapshotError('D benchmark comparison evidence changed during read')
            summary = json.loads(raw)
            if not isinstance(summary, dict) or not summary:
                raise SnapshotError('D benchmark comparison evidence is empty')
            return {'sha256': hashlib.sha256(raw).hexdigest(), 'signature': before, 'name': name}
    return None


def discard_target(policy: dict, target: dict, *, repo_root: Path, engineering_root: Path,
                   state_root: Path, materialized_root: Path, cold_root: Path,
                   apply: bool = False, expected_plan_identity: str | None = None) -> dict:
    if target not in policy["targets"] or type(apply) is not bool:
        raise SnapshotError("testing deletion requires one exact approved target")
    retained_evidence = _guard_d_workflow_copy(policy, cold_root, target)
    source = target_path(policy, target, repo_root=repo_root, engineering_root=engineering_root)
    state_root = real(state_root)
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    if state_root.is_relative_to(source) or source.is_relative_to(state_root):
        raise SnapshotError("discard evidence and target overlap")
    key = identity(target)
    with real(state_root / (key + ".lock")).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = state_root / (key + ".json")
        prior = json.loads(real(journal).read_bytes()) if journal.exists() else {}
        if prior.get("state") in {"prepared", "quarantined", "unlink_started", "unlink_finished"}:
            raise SnapshotError("interrupted discard quarantine needs exact recovery")
        if not source.exists():
            return {"state": "already-absent", "target": target, "deleted": False,
                    "reclaimed_allocated_bytes": 0}
        observed = _observed(source, target["kind"])
        gate_options = {'allow_opaque_symlinks': True} if policy['contract'] in LOCAL_COPY_CONTRACTS else {}
        blockers = _gates(source, observed, repo_root, materialized_root, cold_root, **gate_options)
        plan = {"contract": policy["contract"], "policy_identity_sha256": policy["identity_sha256"],
                "target": target, "source": str(source), "observed": observed, "blockers": blockers}
        if policy['contract'] == D_WORKFLOW_TESTING_CONTRACT:
            plan['retained_evidence'] = retained_evidence
        plan["identity_sha256"] = identity(plan)
        if not apply:
            return {**plan, "state": "discard-dry-run", "deleted": False}
        if blockers or plan["identity_sha256"] != expected_plan_identity:
            raise SnapshotError("testing discard dry run changed or live protection applies: "
                                + json.dumps({"blockers": blockers, "generation_changed": plan["identity_sha256"] != expected_plan_identity}))
        if _observed(source, target["kind"]) != observed or _gates(source, observed, repo_root, materialized_root, cold_root, **gate_options):
            raise SnapshotError("testing discard changed before quarantine")
        if _guard_d_workflow_copy(policy, cold_root, target) != retained_evidence:
            raise SnapshotError('D benchmark comparison evidence changed before quarantine')
        # Equal st_dev does not imply rename works across distinct bind
        # mounts. A local work copy uses a private sibling on its own mount;
        # its durable journal remains under the canonical service state.
        quarantine_parent = (source.parent / ('.stockagent-testing-quarantine-' + uuid.uuid4().hex)
                             if policy['contract'] in LOCAL_COPY_CONTRACTS else
                             state_root / ("quarantine-" + uuid.uuid4().hex))
        quarantine_parent.mkdir(mode=0o700)
        if quarantine_parent.stat().st_dev != source.stat().st_dev:
            quarantine_parent.rmdir()
            raise SnapshotError("testing quarantine crosses filesystem")
        quarantine = quarantine_parent / source.name
        write_discard_receipt(journal, {"state": "prepared", "plan": plan, "quarantine": str(quarantine)})
        source.rename(quarantine)
        write_discard_receipt(journal, {"state": "quarantined", "plan": plan, "quarantine": str(quarantine)})
        current = _observed(quarantine, target["kind"])
        # Rename may change the root inode's ctime, never file members/content.
        if (current["rows"] != observed["rows"] and target["kind"] == "directory"
                or current["files"] != observed["files"] or current["logical_bytes"] != observed["logical_bytes"]
                or not _same_root_before_after_rename(observed["root_signature"], current["root_signature"], target["kind"])
                or process_references(quarantine) or _gates(source, current, repo_root, materialized_root, cold_root, **gate_options)):
            raise SnapshotError("quarantined testing artifact changed or became active; retained")
        if target["kind"] == "file":
            old, new = observed["rows"][0]["signature"], current["rows"][0]["signature"]
            if old[:4] != new[:4] or old[5:] != new[5:]:
                raise SnapshotError("testing file changed during quarantine")
        unlink_root = quarantine if target["kind"] == "directory" else quarantine_parent
        if policy['contract'] in LOCAL_COPY_CONTRACTS and target['kind'] == 'file':
            write_discard_receipt(journal, {"state": "unlink_started", "plan": plan,
                                            "quarantine": str(quarantine)})
        reclaimed = unlink_preserved_file_names(unlink_root, current["rows"])
        if policy['contract'] in LOCAL_COPY_CONTRACTS:
            reclaimed += _unlink_opaque_links(unlink_root, current['rows'])
        if target["kind"] == "directory":
            for r in sorted(current["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
                if r["kind"] == "directory":
                    (quarantine / r["path"]).rmdir()
        result = {"state": "discarded-without-preservation", "deleted": True, "target": target,
                  "policy_identity_sha256": policy["identity_sha256"], "files": current["files"],
                  "logical_bytes": current["logical_bytes"], "reclaimed_allocated_bytes": reclaimed,
                  "recovery_claimed": False, "external_shared_names_deleted": False, "at_epoch": time.time()}
        if policy['contract'] == D_WORKFLOW_TESTING_CONTRACT and target['kind'] == 'directory':
            write_discard_receipt(journal, {"state": "unlink_finished", "plan": plan,
                                            "quarantine": str(quarantine), "result": result})
        if target["kind"] == "directory":
            quarantine.rmdir()
        quarantine_parent.rmdir()
        write_discard_receipt(journal, {"state": "discarded", "plan": plan, "result": result})
        return result
