"""Fresh exact-content retirement for one-shot compressed artifact returns.

Only private SSH acknowledgements from penguin can enter this path. Reuse the
existing source inventory, complete service/config/process/pin and convergence
gates. Unknown shared names, unsupported records and mutation fail closed.
"""
from __future__ import annotations
from collections import defaultdict
from contextlib import ExitStack
import fcntl
import os
from pathlib import Path, PurePosixPath
import stat
import time
import uuid

from stockagent.data_sync.bulk_archive import PRESERVATION_CONTRACTS, ROLE, digest, safe_member
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.remote_legacy_return import (
    acknowledgement_age, active_configuration_references, artifact_process_references,
    metadata_tree, real, signature,
)
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids, process_references
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, _load_inventory, _validate_inventory
from stockagent.data_sync.training_return import shared_inode_references


def consumers(source, repo):
    from scripts.deduplicate_inactive_panel_caches import current_service_blockers
    refs = (artifact_process_references(source, repo / "artifacts")
            + active_configuration_references(source, repo)
            + process_references(source))
    # Output/model consumers and data/cache consumers are different views.
    refs += artifact_service_references([source], repo)[str(source)]
    refs += current_service_blockers([source], repo)
    return sorted(set(refs))


def transport(ack):
    from scripts.configure_artifact_ingress_syncthing import credentials
    from scripts.manage_packed_edge import _convergence
    if ack["snapshot_id"] in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized")):
        raise SnapshotError("compressed archive release is pinned")
    resolved = resolve_packed_snapshot_id(Path("/srv/stockagent-packed"), ack["dataset"],
                                          ack["snapshot_id"], require_objects=False)
    if resolved.manifest_sha256 != ack["manifest_sha256"]:
        raise SnapshotError("origin did not receive the exact cold release")
    metadata = resolved.manifest.get("metadata", {})
    for name in ("preservation_contract", "compressed_sha256", "member_inventory_sha256", "source_scope"):
        if metadata.get(name) != ack[name]:
            raise SnapshotError("cold archive acknowledgement metadata differs")
    if metadata.get("transport_role") != ROLE or metadata.get("deployable") != "false":
        raise SnapshotError("not an approved non-deployable preservation release")
    _validate_inventory(resolved.manifest, _load_inventory(Path("/srv/stockagent-packed"), resolved.manifest))
    base, key = credentials()
    convergence = _convergence(base, key, "stockagent-packed", "penguin")
    if convergence.get("ok") is not True:
        raise SnapshotError("origin and penguin are not currently converged")
    return convergence


def validate_ack(ack):
    body = {k: v for k, v in ack.items() if k != "identity_sha256"}
    version = ack.get("contract")
    if (version not in {"d_verified_bulk_archive_return_v1", "d_verified_bulk_archive_return_v2"}
        or ack.get("preservation_contract") not in PRESERVATION_CONTRACTS or digest(body) != ack.get("identity_sha256")
        or ack.get("origin_node_id") != "vastai1T" or ack.get("authority_node_id") != "penguin"
        or ack.get("cold_verified") is not True or ack.get("decoded_originals_verified") is not True
        or ack.get("manual_immediate") is not True or ack.get("minimum_stable_hours") != 12):
        raise SnapshotError("invalid, stale or unapproved D preservation acknowledgement")
    if (version == "d_verified_bulk_archive_return_v2"
            and ack.get("shared_file_policy") != "unlink_preserved_names_only"
            or version == "d_verified_bulk_archive_return_v1" and "shared_file_policy" in ack):
        raise SnapshotError("shared-name policy does not match the acknowledgement version")
    acknowledgement_age(ack.get("verified_at_epoch", 0), 1800)
    relative = safe_member(ack["root"]["relative_root"])
    if len(PurePosixPath(relative).parts) != 2 or relative.split("/")[0] not in {"markets", "ablations", "cache"}:
        raise SnapshotError("compressed return root outside authorized artifact scopes")
    if relative not in ack["approved_roots"] or relative.split("/")[0] != ack["source_scope"]:
        raise SnapshotError("root not in the exact preservation cohort")
    record = ack["root"]
    if (digest(record["rows"]) != record["root_fingerprint_sha256"]
        or record.get("directory_root") is not True or record.get("unsupported") is not False):
        raise SnapshotError("not a complete recoverable regular directory root")


def original_rows(ack):
    relative = ack["root"]["relative_root"]
    prefix = relative + "/"
    expected = {}
    for row in ack["root"]["rows"]:
        if row["path"] == relative:
            if row["kind"] != "directory":
                raise SnapshotError("original root is not a directory")
            continue
        if not row["path"].startswith(prefix):
            raise SnapshotError("original member escapes its root")
        local = safe_member(row["path"][len(prefix):])
        if local in expected or row["kind"] not in {"file", "directory"}:
            raise SnapshotError("duplicate or unsupported original member")
        expected[local] = row
    return expected


def shared_names_owned(repo, row, ack):
    """Prove ALL current inode names for unused-cache-only shared payloads.

    Artifact runs retain the older conservative shared-inode gate. For caches
    every counted name must be a regular, exact indexed member in this same
    approved inactive cohort, not a mount, redirected name or unknown alias.
    """
    if row["signature"][6] == 1:
        return True
    if ack["contract"] == "d_verified_bulk_archive_return_v2":
        # Full original-byte recovery authorizes only this indexed name. All
        # outside aliases remain, and inode-based FD/mmap gates are mandatory.
        return True
    if ack["source_scope"] != "cache":
        return False
    count = 0
    for name in ack.get("cohort_file_names", []):
        path = real(repo / "artifacts" / safe_member(name))
        if "/".join(PurePosixPath(name).parts[:2]) not in ack["approved_roots"]:
            raise SnapshotError("cache shared-name proof escapes approved roots")
        try:
            s = path.lstat()
        except FileNotFoundError:
            continue  # A prior verified cohort retirement may have removed it.
        if stat.S_ISREG(s.st_mode) and [s.st_dev, s.st_ino] == row["signature"][:2]:
            count += 1
    return count == row["signature"][6]


def source_plan(repo, ack):
    validate_ack(ack)
    source = real(repo / "artifacts" / ack["root"]["relative_root"])
    if not source.is_dir():
        raise SnapshotError("current source directory absent")
    refs = consumers(source, repo)
    if refs:
        return {"source": str(source), "blockers": ["current-consumer"], "references": refs}
    observed = metadata_tree(source)
    inode_refs = shared_inode_references(observed)
    if inode_refs:
        return {"source": str(source), "blockers": ["shared-inode-in-use"], "references": inode_refs}
    expected = original_rows(ack)
    root_row = next(row for row in ack['root']['rows'] if row['path'] == ack['root']['relative_root'])
    top = source.lstat()
    if (top.st_mtime_ns != root_row['mtime_ns'] or stat.S_IMODE(top.st_mode) != root_row['mode']
        or top.st_uid != root_row['uid'] or top.st_gid != root_row['gid']):
        raise SnapshotError('Original root portable metadata differs')
    if {r["path"] for r in observed["rows"]} != set(expected):
        raise SnapshotError("current source has extra or missing preserved members")
    for row in observed["rows"]:
        if row["cross_filesystem"] or row["kind"] not in {"file", "directory"}:
            raise SnapshotError("mounted or unsupported current source")
        path = source / row["path"]
        record = expected[row["path"]]
        info = path.lstat()
        if (row["kind"] != record["kind"] or info.st_mtime_ns != record["mtime_ns"]
            or stat.S_IMODE(info.st_mode) != record["mode"]
            or info.st_uid != record["uid"] or info.st_gid != record["gid"]):
            raise SnapshotError("original portable metadata differs")
        if row["kind"] == "file":
            if time.time_ns() - info.st_mtime_ns < 12 * 3600 * 1000000000:
                raise SnapshotError("original is not stable for at least twelve hours")
            if not shared_names_owned(repo, row, ack):
                raise SnapshotError("unknown or protected shared inode names")
            if (info.st_size != record["size"] or sha256_file(path) != record["sha256"]
                or signature(path.lstat()) != row["signature"]):
                raise SnapshotError("original bytes changed or differ from verified D decode")
    if metadata_tree(source) != observed or consumers(source, repo) or shared_inode_references(observed):
        raise SnapshotError("source changed or became active during exact comparison")
    convergence = transport(ack)
    result = {"source": str(source), "relative_root": ack["root"]["relative_root"],
              "rows": observed["rows"], "blockers": [], "references": [],
              "ack_identity_sha256": ack["identity_sha256"],
              "shared_file_policy": ack.get("shared_file_policy", "reject_unknown_names"),
              "manual_immediate": True, "cold_deleted": False}
    result["fingerprint"] = digest(result)
    result["convergence"] = convergence
    return result


def retire(repo, ack, *, apply=False, state_root=Path("/var/lib/stockagent-legacy-return/bulk")):
    """Dry run, repeat, quarantine, rehash, recheck, then unlink exact names."""
    validate_ack(ack)
    original_rows(ack)
    state_root = real(state_root)
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    source = real(repo / "artifacts" / ack["root"]["relative_root"])
    key = digest(ack["root"]["relative_root"])[:24]
    with ExitStack() as locks:
        owner = locks.enter_context((state_root / (key + ".lock")).open("a+b"))
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Known cache generation writers honor these exact canonical locks.
        for row in ack["root"]["rows"]:
            if row["kind"] == "file" and row["path"].endswith("/panel_cache_v2/.write.lock"):
                path = real(repo / "artifacts" / row["path"])
                lock = locks.enter_context(path.open("r+b"))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = source_plan(repo, ack)
        atomic_write_json(state_root / (key + "-plan.json"), plan)
        if not apply or plan["blockers"]:
            return {"state": "source-protected" if plan["blockers"] else "would-retire",
                    "deleted": False, "plan_fingerprint": plan.get("fingerprint"),
                    "blockers": plan["blockers"], "references": plan["references"]}
        repeated = source_plan(repo, ack)
        if repeated.get("fingerprint") != plan["fingerprint"] or repeated["blockers"]:
            raise SnapshotError("current dry-run fingerprint differs; source retained")
        quarantine = real(state_root / ("quarantine-" + uuid.uuid4().hex))
        if source.stat().st_dev != state_root.stat().st_dev:
            raise SnapshotError("quarantine crosses filesystems")
        journal = state_root / (key + "-retirement.json")
        atomic_write_json(journal, {"state": "prepared", "plan": plan, "ack": ack, "quarantine": str(quarantine)})
        source.rename(quarantine)
        atomic_write_json(journal, {"state": "quarantined", "plan": plan, "ack": ack, "quarantine": str(quarantine)})
        current = metadata_tree(quarantine)
        if (current["rows"] != plan["rows"] or consumers(source, repo) or consumers(quarantine, repo)
                or shared_inode_references(current)):
            raise SnapshotError("quarantine changed or source is active; retained")
        expected = original_rows(ack)
        # Revalidate every original after quarantine, before any unlink.
        for row in current["rows"]:
            if row["kind"] == "file":
                path = quarantine / row["path"]
                if signature(path.lstat()) != row["signature"] or sha256_file(path) != expected[row["path"]]["sha256"]:
                    raise SnapshotError("quarantined bytes differ; no unlink authorized")
        validate_ack(ack)
        transport(ack)
        if (metadata_tree(quarantine)["rows"] != plan["rows"]
            or consumers(source, repo) or consumers(quarantine, repo) or shared_inode_references(current)):
            raise SnapshotError("final quarantine/reference gate failed; retained")
        inode_names = defaultdict(list)
        for row in current["rows"]:
            if row["kind"] == "file":
                inode_names[tuple(row["signature"][:2])].append(row)
        reclaimed = 0
        for rows in inode_names.values():
            first = quarantine / rows[0]["path"]
            info = first.lstat()
            if signature(info) != rows[0]["signature"]:
                raise SnapshotError("inode changed before unlink; remaining quarantine retained")
            blocks = info.st_blocks * 512
            with first.open("rb") as handle:
                latest = signature(os.fstat(handle.fileno()))
                for row in rows:
                    path = quarantine / row["path"]
                    if signature(path.lstat()) != latest:
                        raise SnapshotError("inode mutated during retirement; remaining names retained")
                    path.unlink()
                    # Our own hard-link unlink legitimately changes ctime/nlink.
                    after = signature(os.fstat(handle.fileno()))
                    if after[:4] != latest[:4] or after[5] != latest[5] or after[6] != latest[6] - 1:
                        raise SnapshotError("inode changed during unlink; remaining names retained")
                    latest = after
                freed = latest[6] == 0
            if freed:
                reclaimed += blocks
        for row in sorted(current["rows"], key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
            if row["kind"] == "directory":
                (quarantine / row["path"]).rmdir()
        quarantine.rmdir()
        result = {"state": "exact-d-bulk-backed-source-retired", "deleted": True,
                  "reclaimed_allocated_bytes": reclaimed, "cold_deleted": False,
                  "shared_file_policy": plan["shared_file_policy"], "external_shared_names_deleted": False,
                  "snapshot_id": ack["snapshot_id"], "manifest_sha256": ack["manifest_sha256"]}
        atomic_write_json(journal, {"state": "retired", "plan": plan, "ack": ack, "result": result})
        return result
