"""Inventory every local market-artifact root before cold-only cleanup.

Read-only with respect to artifacts, services and cold storage. Writes only an
audit report; capacity/age/progress observations never authorize deletion.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.legacy_artifact_archive import load_legacy_specs
from stockagent.data_sync.cold_artifacts import load_cold_artifact_registry
from stockagent.data_sync.packed_snapshots import resolve_latest_packed


def inventory(repo: Path, output: Path, *, selected_roots: list[str] | None = None) -> dict:
    repo = repo.resolve()
    parent = repo / "artifacts/markets"
    if parent.resolve() != parent or not parent.is_dir():
        raise ValueError("market artifact parent must be a real directory")
    if selected_roots is not None:
        if not selected_roots or len(selected_roots) != len(set(selected_roots)):
            raise ValueError("selected market roots must be a non-empty unique list")
        if any(Path(name).name != name or name in {".", ".."} for name in selected_roots):
            raise ValueError("selected market roots must be direct child names")
        roots = sorted(parent / name for name in selected_roots)
        if any(not os.path.lexists(root) for root in roots):
            raise ValueError("selected market root is missing")
    else:
        roots = sorted(parent.iterdir())
    service_refs = artifact_service_references(roots, repo)
    cold = load_cold_artifact_registry(repo / "configs/data_sync/cold_artifacts.json")
    legacy = load_legacy_specs(repo / "configs/data_sync/legacy_artifact_archives.json")
    now = time.time_ns()
    rows = []
    all_inodes = set()
    total_unique = 0
    for root in roots:
        row = {
            "root": str(root.relative_to(repo)),
            "resolved_path": str(root.resolve()),
            "files": 0,
            "logical_bytes": 0,
            "unique_allocated_bytes_within_root": 0,
            "shared_inode_files": 0,
            "newest_file_mtime_ns": 0,
            "service_references": service_refs[str(root)],
            "process_references": artifact_process_references(root, parent),
            "non_regular": [],
            "eviction_authorized": False,
        }
        inodes = set()
        if root.is_symlink() or not root.is_dir():
            row["non_regular"].append("root-is-symlink-or-non-directory")
        else:
            for directory, dirs, files in os.walk(root, followlinks=False):
                for name in dirs + files:
                    path = Path(directory) / name
                    info = path.lstat()
                    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                        row["non_regular"].append(str(path.relative_to(root)))
                    if not stat.S_ISREG(info.st_mode):
                        continue
                    row["files"] += 1
                    row["logical_bytes"] += info.st_size
                    row["newest_file_mtime_ns"] = max(
                        row["newest_file_mtime_ns"], info.st_mtime_ns
                    )
                    row["shared_inode_files"] += int(info.st_nlink > 1)
                    inode = info.st_dev, info.st_ino
                    if inode not in inodes:
                        row["unique_allocated_bytes_within_root"] += (
                            info.st_blocks * 512
                        )
                        inodes.add(inode)
                    if inode not in all_inodes:
                        total_unique += info.st_blocks * 512
                        all_inodes.add(inode)
        row["stable_days"] = (
            (now - row["newest_file_mtime_ns"]) / 86400e9
            if row["newest_file_mtime_ns"]
            else None
        )
        row["changed_during_inventory"] = row["newest_file_mtime_ns"] > now
        progress = root / "progress.json"
        row["progress_state"] = None
        if progress.is_file():
            try:
                content = json.loads(progress.read_text())
                row["progress_state"] = content.get("state")
                row["progress_phase"] = content.get("phase")
            except (ValueError, OSError) as exc:
                row["progress_error"] = str(exc)
        relative = str(root.relative_to(repo / "artifacts"))
        row["registered_cold"] = []
        for spec in [*cold.values(), *legacy.values()]:
            if spec.relative_root != relative:
                continue
            identity = {
                "dataset": spec.dataset,
                "role": "completed" if spec.dataset in cold else "legacy-quarantine",
            }
            try:
                resolved = resolve_latest_packed(
                    Path("/srv/stockagent-packed"), spec.dataset
                )
                identity.update(
                    snapshot_id=resolved.manifest["snapshot_id"],
                    stored_bytes=resolved.manifest["archive"]["stored_bytes"],
                    checksum_and_decode_verified_in_this_inventory=False,
                )
            except (OSError, ValueError, SnapshotError) as exc:
                identity["error"] = str(exc)
            row["registered_cold"].append(identity)
        if row["service_references"] or row["process_references"]:
            row["classification"] = "keep-in-use"
        elif row["non_regular"]:
            row["classification"] = "keep-non-regular-or-managed-link"
        elif not row["files"]:
            row["classification"] = "empty-root-review"
        elif row["stable_days"] < 7:
            row["classification"] = "recent-output-review"
        elif row["registered_cold"]:
            row["classification"] = "verify-registered-cold-then-retire"
        else:
            row["classification"] = "archive-before-retirement"
        rows.append(row)
        print(
            json.dumps(
                {
                    k: row[k]
                    for k in (
                        "root",
                        "files",
                        "unique_allocated_bytes_within_root",
                        "stable_days",
                        "classification",
                    )
                }
            ),
            flush=True,
        )
    summary = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "roots": rows,
        "root_count": len(rows),
        "selected_roots": selected_roots,
        "unique_file_allocated_bytes": total_unique,
        "eviction_authorized_by_inventory": False,
        "limitations": [
            "No source or cold checksum proof; paths/age are not deletion proof",
            "Per-root bytes are not additive when inodes are shared",
            "Freeing a hard-linked source alone may reclaim no payload",
            "Service config gate resolves Discord and overnight consumers; other pipelines require process and dependency review",
            "Live processes may start after this observation; recheck before mutation",
        ],
    }
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output / "inventory.json", summary)
    with (output / "roots.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        fields = (
            "root",
            "files",
            "logical_bytes",
            "unique_allocated_bytes_within_root",
            "shared_inode_files",
            "stable_days",
            "classification",
            "service_references",
            "process_references",
            "registered_cold",
        )
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            {
                k: json.dumps(row[k], ensure_ascii=False)
                if isinstance(row[k], list)
                else row[k]
                for k in fields
            }
            for row in rows
        )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--root", action="append", help="inventory only this direct markets child; repeatable")
    args = parser.parse_args()
    result = inventory(REPO, args.output_dir, selected_roots=args.root)
    print(
        json.dumps(
            {
                "root_count": result["root_count"],
                "unique_file_allocated_bytes": result["unique_file_allocated_bytes"],
                "output_dir": str(args.output_dir),
            }
        )
    )
