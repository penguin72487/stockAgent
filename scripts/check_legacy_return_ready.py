#!/usr/bin/env python3
"""Read-only systemd condition for resuming an existing legacy return cohort."""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path


def readiness(state_root: Path) -> tuple[bool, str]:
    if not state_root.is_dir() or state_root.is_symlink():
        return False, "cohort_not_initialized"
    paths = {name: state_root / name for name in (
        "inventory.json", "progress.json", "archive-catalog.json", "cohort-owner.lock"
    )}
    if any(not path.is_file() or path.is_symlink() for path in paths.values()):
        return False, "cohort_receipts_missing_or_redirected"
    with paths["cohort-owner.lock"].open("rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False, "existing_cohort_owner_active"
        path = paths["progress.json"]
        before = path.stat()
        if before.st_size > 16 * 1024 * 1024:
            return False, "progress_exceeds_receipt_bound"
        try:
            progress = json.loads(path.read_bytes())
        except (OSError, ValueError):
            return False, "progress_invalid"
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_ino, after.st_size, after.st_mtime_ns
        ):
            return False, "progress_changed_during_read"
        items = progress.get("items") if isinstance(progress, dict) else None
        if not isinstance(items, list):
            return False, "progress_items_invalid"
        for row in items:
            if not isinstance(row, dict) or not isinstance(row.get("state"), str):
                return False, "progress_items_invalid"
            # The canonical inventory retains loose files (logs, locks,
            # receipts) without a directory's recursive file count. They are
            # protected and the worker skips them; they must not block retries
            # of valid directory entries in the same retained cohort.
            if "files" not in row and row["state"] == "non-directory-protected":
                continue
            if type(row.get("files")) is not int or row["files"] < 0:
                return False, "progress_items_invalid"
        retained = {"remote-source-retired", "source-protected", "non-directory-protected",
                    "empty-directory-protected"}
        if any(row.get("files", 0) > 0 and row["state"] not in retained for row in items):
            return True, "retained_cohort_has_retryable_items"
        return False, "no_retryable_items_in_retained_cohort"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_root", type=Path)
    args = parser.parse_args()
    ready, reason = readiness(args.state_root)
    print(json.dumps({"ready": ready, "reason": reason}))
    # ExecCondition skips on 1. A running cohort is not a successful backup.
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
