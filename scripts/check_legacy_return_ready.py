#!/usr/bin/env python3
"""Read-only systemd condition for resuming an existing legacy return cohort."""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import time


def readiness(state_root: Path, *, selection_path: Path | None = None) -> tuple[bool, str]:
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
        selected = None
        if selection_path is not None:
            from scripts.return_remote_legacy_archives import load_cohort_selection
            try:
                selected = load_cohort_selection(selection_path, state_root)
            except Exception:
                return False, "fixed_cohort_selection_invalid"
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
        for row in items:
            if selected is not None and row.get("relative_root") not in selected:
                continue
            if row.get("files", 0) > 0 and row["state"] not in retained:
                return True, "retained_cohort_has_retryable_items"
            if (row["state"] == "source-protected" and row.get("blockers") == ["source-not-twelve-hour-stable"]
                    and type(row.get("newest_mtime_ns")) is int and row["newest_mtime_ns"] > 0
                    and time.time_ns() - row["newest_mtime_ns"] >= 12 * 3_600_000_000_000):
                return True, "selected_source_stability_floor_elapsed"
        return False, "no_retryable_items_in_retained_cohort"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_root", type=Path)
    parser.add_argument("--selection", type=Path)
    args = parser.parse_args()
    ready, reason = readiness(args.state_root, selection_path=args.selection)
    print(json.dumps({"ready": ready, "reason": reason}))
    # ExecCondition skips on 1. A running cohort is not a successful backup.
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
