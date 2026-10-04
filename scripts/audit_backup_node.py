#!/usr/bin/env python3
"""Read-only backup-node inventory. Never infer that a listed path can be deleted."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess


def inventory_tree(root: Path) -> dict:
    info = root.lstat()
    redirected = lambda p: p.is_symlink() or (hasattr(p, "is_junction") and p.is_junction())
    if not stat.S_ISDIR(info.st_mode) or any(redirected(p) for p in (root, *root.parents)):
        return {"path": str(root), "state": "not_a_regular_directory", "deletion_authorized": False}
    files = symlinks = logical = allocated = 0
    seen: set[tuple[int, int]] = set()
    errors = []
    for directory, subdirs, names in os.walk(root, followlinks=False, onerror=lambda e: errors.append(type(e).__name__)):
        for name in list(subdirs):
            if redirected(Path(directory) / name):
                symlinks += 1; subdirs.remove(name)
        for name in names:
            try:
                item = (Path(directory) / name).lstat()
                if stat.S_ISLNK(item.st_mode) or bool(getattr(item, "st_file_attributes", 0) & 0x400):
                    symlinks += 1
                elif stat.S_ISREG(item.st_mode):
                    files += 1; logical += item.st_size
                    identity = (item.st_dev, item.st_ino)
                    if identity not in seen:
                        allocated += getattr(item, "st_blocks", 0) * 512
                        seen.add(identity)
            except OSError as e:
                errors.append(type(e).__name__)
    usage = shutil.disk_usage(root)
    return {"path": str(root.absolute()), "files": files, "symlinks": symlinks,
            "logical_bytes": logical, "allocated_bytes": allocated if os.name == "posix" else None,
            "volume_free_bytes": usage.free, "errors": errors,
            "recovery_proof": None, "process_references_checked": False, "deletion_authorized": False}


def processes() -> list[dict]:
    if os.name != "posix" or not Path("/proc").is_dir():
        # Names only; command lines can carry passwords and require private inspection.
        r = subprocess.run(["tasklist", "/FO", "CSV", "/NH"], capture_output=True, text=True, check=False)
        return [{"native_tasklist": r.stdout, "returncode": r.returncode}]
    result = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            result.append({"pid": int(entry.name), "name": (entry / "comm").read_text().strip(),
                           "cwd": str((entry / "cwd").resolve(strict=True))})
        except OSError:
            continue
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve the previous inventory receipt")
    os.umask(0o077)
    result = {"schema_version": 1, "host": platform.node(), "system": platform.system(),
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "trees": [inventory_tree(root) for root in args.root], "processes": processes(),
              "state": "read_only_inventory", "deletion_authorized": False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
    args.output.chmod(0o600)
    print(json.dumps({"state": result["state"], "trees": result["trees"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
