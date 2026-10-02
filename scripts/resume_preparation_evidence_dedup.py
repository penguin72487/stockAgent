#!/usr/bin/env python3
"""Resume a hash-audited, source-only TAIFEX preparation deduplication wave."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import stat
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.artifact_dedup import ArtifactFile, DuplicateGroup, apply_duplicate_groups
from stockagent.data_sync.artifact_maintenance import artifact_process_references_many
from stockagent.data_sync.desync_snapshots import atomic_write_json


def resume(report: Path, *, apply: bool = False) -> dict:
    plan = json.loads((report / "preparation_dedup_dry_run.json").read_text())
    root = REPO_ROOT / "artifacts/markets/tw_futures_v8_margin_preparation"
    if root.is_symlink() or root.resolve() != root or Path(plan["root"]) != root:
        raise ValueError("dedup audit is not for the canonical preparation root")
    selected = set(plan["selected_roots"])
    if any(not name.startswith("all_products_rule_facts_native_v")
           or Path(name).name != name for name in selected):
        raise ValueError("invalid preparation version allowlist")
    targets = [root / name for name in sorted(selected)]
    if any(target.is_symlink() or target.resolve() != target or not target.is_dir()
           for target in targets):
        raise ValueError("selected preparation directory is redirected or missing")
    rows = []
    for group in plan["groups"]:
        paths = [group["canonical"], *group["duplicates"]]
        if all(len(Path(x).parts) >= 3 and Path(x).parts[0] in selected
               and Path(x).parts[1] == "sources" and ".." not in Path(x).parts
               for x in paths):
            rows.append(group)
    status_path = report / "preparation_dedup_apply_status.json"
    status = json.loads(status_path.read_text())
    completed = int(status.get("completed_groups", 0))
    if not 0 <= completed <= len(rows) or int(status["groups"]) != len(rows):
        raise ValueError("checkpoint disagrees with the audited group count")
    if completed % 250 and completed != len(rows):
        raise ValueError("resume checkpoint is not a complete batch boundary")
    # A partial last batch has no durable receipt. Reprocess it: existing
    # aliases are safe no-ops, never removals of unique bytes.
    for index in range((completed + 249) // 250):
        receipt = json.loads((report / "dedup_batches" / f"{index:04d}.json").read_text())
        if receipt.get("same_inode_verified") is not True:
            raise ValueError("completed batch lacks post-link verification")
    if completed < len(rows) and (report / "dedup_batches" / f"{completed // 250:04d}.json").exists():
        raise ValueError("uncheckpointed receipt requires accounting reconciliation")
    if not apply:
        return {"mode": "dry-run", "completed_groups": completed,
                "remaining_groups": len(rows) - completed, "status": status}
    refs = artifact_process_references_many(targets, root.parent)
    if refs:
        raise RuntimeError(f"selected source roots are in use: {refs[:3]}")
    status.update(phase="applying", resumed_ns=time.time_ns())
    atomic_write_json(status_path, status)
    for start in range(completed, len(rows), 250):
        refs = artifact_process_references_many(targets, root.parent)
        if refs:
            raise RuntimeError(f"selected source roots became active: {refs[:3]}")
        groups = []
        allocated = {}
        extra_links = 0
        recent_files = 0
        cutoff = int(status["started_ns"]) - 24 * 3600 * 10**9
        for row in rows[start:start + 250]:
            canonical_path = root / row["canonical"]
            if canonical_path.resolve() != canonical_path or not stat.S_ISREG(canonical_path.lstat().st_mode):
                raise ValueError("canonical source path is redirected or non-regular")
            canonical = ArtifactFile.from_path(root, canonical_path)
            if canonical.mtime_ns > cutoff:
                recent_files += len(row["duplicates"])
                continue
            duplicates = []
            for relative in row["duplicates"]:
                path = root / relative
                info = path.lstat()
                if path.resolve() != path or not stat.S_ISREG(info.st_mode):
                    raise ValueError("duplicate source path is redirected or non-regular")
                if info.st_nlink != 1:
                    extra_links += 1
                    continue
                if max(info.st_mtime_ns, info.st_ctime_ns) > cutoff:
                    recent_files += 1
                    continue
                item = ArtifactFile.from_path(root, path)
                if (item.mode, item.uid, item.gid, item.xattrs) != (canonical.mode, canonical.uid, canonical.gid, canonical.xattrs):
                    raise ValueError("duplicate source metadata no longer matches")
                duplicates.append(item)
                allocated[relative] = item.blocks * 512
            if duplicates:
                groups.append(DuplicateGroup(row["sha256"], row["size"], canonical, tuple(duplicates)))
        replaced, skipped = apply_duplicate_groups(root, groups)
        for item in replaced:
            first, second = (root / item["path"]).stat(), (root / item["canonical"]).stat()
            if not os.path.samestat(first, second) or first.st_size != item["size"]:
                raise RuntimeError("post-link identity mismatch")
        recovered = sum(allocated[item["path"]] for item in replaced)
        atomic_write_json(report / "dedup_batches" / f"{start // 250:04d}.json",
                          {"groups": len(groups), "replaced": replaced, "skipped": skipped,
                           "existing_or_external_links": extra_links,
                           "recent_files": recent_files,
                           "reclaimed_allocated_bytes": recovered, "same_inode_verified": True})
        status.update(completed_groups=min(start + 250, len(rows)),
                      replaced_files=int(status["replaced_files"]) + len(replaced),
                      reclaimed_allocated_bytes=int(status["reclaimed_allocated_bytes"]) + recovered,
                      reclaimed_logical_bytes=int(status["reclaimed_logical_bytes"]) + sum(item["size"] for item in replaced),
                      skipped_files=int(status["skipped_files"]) + len(skipped),
                      recent_files=int(status.get("recent_files", 0)) + recent_files,
                      uncounted_existing_or_external_links=int(status.get("uncounted_existing_or_external_links", 0)) + extra_links)
        atomic_write_json(status_path, status)
        if start // 250 % 10 == 0:
            print(json.dumps(status), flush=True)
    filesystem = os.statvfs("/")
    status.update(phase="complete", completed_ns=time.time_ns(),
                  after_free_bytes=filesystem.f_bfree * filesystem.f_frsize,
                  after_available_bytes=filesystem.f_bavail * filesystem.f_frsize,
                  all_original_paths_retained=True, unique_source_bytes_deleted=False)
    atomic_write_json(report / "preparation_dedup_apply.json", status)
    atomic_write_json(status_path, status)
    return status


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    report = args.report_dir.resolve(strict=True)
    if args.apply:
        with (report / ".dedup-resume.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print(json.dumps(resume(report, apply=True)), flush=True)
    else:
        print(json.dumps(resume(report)), flush=True)
