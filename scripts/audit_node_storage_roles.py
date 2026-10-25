#!/usr/bin/env python3
"""Inventory node-local project data against the four-node role contract.

Metadata only: no hash, deletion, source API, materialization or model startup.
Inventory is not recovery proof or permission to delete an unknown directory.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import atomic_write_json, resolve_node_id
from stockagent.data_sync.materialized_cache import process_references_many, _path_is_under
from stockagent.remote_build import observe_node


def filesystem_inventory(paths: list[Path], *, run=subprocess.run) -> dict:
    """Query mounts/capacity without walking or reading project payloads.

    Paths may share a filesystem; their capacities must not be added. DrvFS
    reports 9p here; the Windows backing volume format is a separate probe.
    """
    rows = []
    for path in dict.fromkeys(p.absolute() for p in paths):
        row = {"path": str(path)}
        try:
            before = path.stat()
            space = os.statvfs(path)
            result = run(["findmnt", "--json", "--target", str(path), "--output", "TARGET,FSTYPE"],
                         capture_output=True, text=True, timeout=10, check=True)
            mounts = json.loads(result.stdout)["filesystems"]
            if (not isinstance(mounts, list) or len(mounts) != 1
                    or not isinstance(mounts[0], dict)
                    or not all(isinstance(mounts[0].get(key), str) and mounts[0][key]
                               for key in ("target", "fstype"))):
                raise ValueError("mount probe omitted its filesystem identity")
            if path.stat().st_dev != before.st_dev:
                raise ValueError("mount changed during observation")
            row.update(mount=mounts[0], device=before.st_dev,
                       block_bytes=space.f_frsize, total_bytes=space.f_blocks * space.f_frsize,
                       free_bytes=space.f_bfree * space.f_frsize,
                       available_bytes=space.f_bavail * space.f_frsize,
                       reserved_free_bytes=(space.f_bfree - space.f_bavail) * space.f_frsize,
                       complete=True)
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
            row.update(complete=False, error_type=type(error).__name__)
        rows.append(row)
    return {"contract": "stockagent.filesystem_observation.v1", "observed_at_epoch": time.time(),
            "filesystems": rows, "inventory_complete": all(row["complete"] for row in rows),
            "source_payload_reads": 0, "source_payload_writes": 0, "deletion_authorized": False,
            "scope": "mounts and statvfs only; repeated filesystems are not summed; not recovery or filesystem repair proof"}


def inventory(path: Path, *, max_files: int = 5_000_000) -> dict:
    result = {"files": 0, "directories": 0, "symlinks": 0, "logical_bytes": 0,
              "allocated_unique_file_bytes": 0, "shared_inodes": 0,
              "newest_mtime_ns": 0, "errors": [], "complete": True}
    inodes = set()
    device = path.stat().st_dev
    def error(value):
        result["complete"] = False
        if len(result["errors"]) < 20:
            result["errors"].append(str(value))
    for parent, dirs, files in os.walk(path, followlinks=False, onerror=error):
        result["directories"] += 1
        for name in list(dirs):
            entry = Path(parent) / name
            try:
                info = entry.lstat()
                if stat.S_ISLNK(info.st_mode):
                    result["symlinks"] += 1
                    dirs.remove(name)
                elif info.st_dev != device:
                    error("different_filesystem:" + str(entry.relative_to(path)))
                    dirs.remove(name)
            except OSError as exc:
                error(type(exc).__name__ + ":" + str(entry.relative_to(path)))
                dirs.remove(name)
        for name in files:
            entry = Path(parent) / name
            try:
                info = entry.lstat()
                if stat.S_ISLNK(info.st_mode):
                    result["symlinks"] += 1
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_dev != device:
                    error("unsupported:" + str(entry.relative_to(path)))
                    continue
                result["files"] += 1
                result["logical_bytes"] += info.st_size
                result["newest_mtime_ns"] = max(result["newest_mtime_ns"], info.st_mtime_ns)
                inode = (info.st_dev, info.st_ino)
                if inode not in inodes:
                    inodes.add(inode)
                    result["allocated_unique_file_bytes"] += info.st_blocks * 512
                    result["shared_inodes"] += info.st_nlink > 1
                if result["files"] >= max_files:
                    error("inventory_limit_reached")
                    return result
            except OSError as exc:
                error(type(exc).__name__ + ":" + str(entry.relative_to(path)))
    return result


def project_roots(repo: Path) -> list[tuple[Path, str]]:
    rows = [(p, "source_or_materialized_input") for p in repo.glob("data_*")]
    artifacts = repo / "artifacts"
    for parent, role in (("cache", "derived_cache"), ("markets", "training_or_legacy_output"),
                         ("ablations", "training_or_legacy_output"), ("runtime", "pinned_runtime")):
        base = artifacts / parent
        if base.is_dir() and not base.is_symlink():
            rows.extend((p, role) for p in base.iterdir())
    if artifacts.is_dir():
        rows.extend((p, "operational_or_unclassified") for p in artifacts.iterdir()
                    if p.name not in {"cache", "markets", "ablations", "runtime"})
    return sorted(rows, key=lambda row: str(row[0]))


def audit(repo: Path, node_id: str, policy: dict) -> dict:
    node = policy["nodes"][node_id]
    roots = project_roots(repo)
    ordinary = [p for p, _ in roots if p.is_dir() and not p.is_symlink()]
    references = process_references_many(ordinary, limit=100_000)
    rows = []
    for path, role in roots:
        row = {"path": str(path), "relative": path.relative_to(repo).as_posix(), "role": role,
               "process_references": [ref for ref in references
                                      if _path_is_under(ref.split(":", 2)[-1], path)],
               "deletion_authorized": False}
        if path.is_symlink():
            row.update(kind="symlink_interface", target=str(path.resolve(strict=False)),
                       complete=True, logical_bytes=0, allocated_unique_file_bytes=0)
        elif path.is_dir():
            row.update(kind="directory", **inventory(path))
        elif path.is_file():
            info = path.stat()
            row.update(kind="file", files=1, complete=True, logical_bytes=info.st_size,
                       allocated_unique_file_bytes=info.st_blocks * 512)
        else:
            row.update(kind="unsupported", complete=False, errors=["missing_or_unsupported"])
        rows.append(row)
    return {"schema_version": 1, "node_id": node_id, "observed_at_epoch": time.time(),
            "node_policy": node, "authority_node_id": policy["authority_node_id"],
            "node_observation": observe_node(repo, repo), "roots": rows,
            "root_count": len(rows), "inventory_complete": all(r["complete"] for r in rows),
            "scope": "project data/artifacts; symlinks are interfaces, shared bytes are not summed across roots",
            "counts_by_role": dict(Counter(r["role"] for r in rows)),
            "metadata_inventory_is_not_deletion_or_recovery_proof": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=ROOT / "configs/data_sync/node_storage_roles.json")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--node-id", choices=("penguin", "vastai1T"))
    parser.add_argument("--filesystems-only", action="store_true",
                        help="bounded mount/capacity observation; no source or artifact tree scan")
    parser.add_argument("--filesystem-path", action="append", type=Path,
                        help="explicit path for --filesystems-only; repeat to inspect multiple mount boundaries")
    args = parser.parse_args()
    if args.filesystem_path and not args.filesystems_only:
        parser.error("explicit filesystem paths require --filesystems-only")
    policy = json.loads(args.policy.read_text())
    if policy.get("schema_version") != 1 or policy.get("authority_node_id") != "penguin":
        parser.error("unsupported node role authority/schema")
    actual = resolve_node_id(Path("/srv/stockagent-packed"))
    if actual not in {"penguin", "vastai1T"} or args.node_id and actual != args.node_id:
        parser.error("requested inventory node differs from the local cold identity")
    os.umask(0o077)
    if args.filesystems_only:
        value = filesystem_inventory(args.filesystem_path or [ROOT, Path(policy["nodes"][actual]["cold_root"])])
        value["node_id"] = actual
        atomic_write_json(args.output, value)
        print(json.dumps(value, ensure_ascii=False, indent=2))
        return 0 if value["inventory_complete"] else 75
    value = audit(ROOT, actual, policy)
    atomic_write_json(args.output, value)
    print(json.dumps({k: value[k] for k in ("node_id", "root_count", "inventory_complete", "counts_by_role")}))
    return 0 if value["inventory_complete"] else 75


if __name__ == "__main__":
    raise SystemExit(main())
