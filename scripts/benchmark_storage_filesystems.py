#!/usr/bin/env python3
"""Measure a closed write/rename/read/SHA workflow on explicitly selected roots.

Only newly created private benchmark directories are touched. Results describe
the observed filesystem path, hardware and warm-cache workflow, not raw-device
performance. No system cache, mount option or source file is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def mount_info(path: Path) -> dict:
    result = subprocess.run(
        ["findmnt", "--json", "--target", str(path), "--output", "TARGET,FSTYPE"],
        capture_output=True, text=True, check=True, timeout=10,
    )
    return json.loads(result.stdout)["filesystems"][0]


def measure(root: Path, *, files: int, file_bytes: int) -> dict:
    root = root.absolute()
    if not root.is_dir() or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("benchmark root must be an existing real directory")
    if os.statvfs(root).f_bavail * os.statvfs(root).f_frsize < 2 * files * file_bytes + 1024**3:
        raise ValueError("benchmark root lacks workspace and a 1 GiB reserve")
    payload = os.urandom(min(file_bytes, 1024**2))
    expected = hashlib.sha256()
    remaining = file_bytes
    while remaining:
        chunk = payload[:min(remaining, len(payload))]
        expected.update(chunk)
        remaining -= len(chunk)
    durations = {}
    with tempfile.TemporaryDirectory(prefix=".stockagent-fs-probe-", dir=root) as temporary:
        scratch = Path(temporary)
        before = time.perf_counter()
        for i in range(files):
            path = scratch / f"{i:06d}.partial"
            with path.open("xb") as stream:
                remaining = file_bytes
                while remaining:
                    chunk = payload[:min(remaining, len(payload))]
                    stream.write(chunk)
                    remaining -= len(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            path.rename(path.with_suffix(".closed"))
        durations["write_fsync_rename_seconds"] = time.perf_counter() - before
        before = time.perf_counter()
        for path in sorted(scratch.glob("*.closed")):
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024**2), b""):
                    digest.update(chunk)
            if path.stat().st_size != file_bytes or digest.digest() != expected.digest():
                raise RuntimeError("filesystem workflow read-back differs")
        durations["enumerate_read_sha_seconds"] = time.perf_counter() - before
        durations["complete_workflow_seconds"] = sum(durations.values())
    return {"root": str(root), "mount": mount_info(root), "files": files,
            "logical_bytes": files * file_bytes, "all_sha256_verified": True,
            "own_scratch_removed": True, **durations}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, action="append", required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 5 or args.output.exists():
        parser.error("use 1-5 rounds and a fresh output receipt")
    rows = []
    started = time.perf_counter()
    for turn in range(args.rounds):
        # Alternate order to reduce systematic first/last-position effects.
        roots = args.root if turn % 2 == 0 else list(reversed(args.root))
        for root in roots:
            for workload, files, size in (("small_files", 128, 64*1024), ("packed_files", 2, 4*1024**2)):
                rows.append({"round": turn, "workload": workload,
                             **measure(root, files=files, file_bytes=size)})
    summaries = []
    for root in args.root:
        for workload in ("small_files", "packed_files"):
            values = [r["complete_workflow_seconds"] for r in rows
                      if r["root"] == str(root.absolute()) and r["workload"] == workload]
            summaries.append({"root": str(root.absolute()), "workload": workload,
                              "median_complete_seconds": statistics.median(values),
                              "complete_seconds": values})
    result = {"contract": "filesystem_closed_workflow_measurement_v2", "host": os.uname().nodename,
              "logical_bytes_per_workload": 8*1024**2, "equal_payload_sizes": True,
              "observed_at_epoch": time.time(), "cache_scope": "fresh paths; OS caches were not cleared",
              "scope": "observed paths and drives; filesystem algorithm alone is not isolated",
              "source_files_changed": False, "mounts_changed": False,
              "complete_measurement_seconds": time.perf_counter()-started,
              "runs": rows, "summaries": summaries}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"output": str(args.output), "summaries": summaries}))


if __name__ == "__main__":
    main()
