#!/usr/bin/env python3
"""Compare guarded D blob recovery through DrvFs and native Windows pipes.

Real immutable objects only; includes full SHA, copy, fsync and independent
restored SHA. Does not publish, change mounts, flush OS caches or remove D data.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.training_return import admit_workspace
from stockagent.data_sync.windows_cold_io import binary_reader


def signature(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode, s.st_nlink]


def cpu_seconds():
    return sum(resource.getrusage(w).ru_utime + resource.getrusage(w).ru_stime
               for w in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.rounds <= 5:
        parser.error("fresh receipt and 1-5 rounds required")
    os.umask(0o077)
    cold = Path("/srv/stockagent-packed")
    _check_d_primary_mount(cold)
    samples = []
    for parent in sorted((cold / "objects/blobs").iterdir()):
        for path in sorted(parent.iterdir()):
            if path.is_symlink() or path.suffix != ".blob":
                continue
            info = path.stat()
            if 8 * 1024**2 <= info.st_size <= 32 * 1024**2:
                samples.append({"path": path, "signature": signature(path), "sha256": path.stem})
            if len(samples) == 3:
                break
        if len(samples) == 3:
            break
    if len(samples) != 3:
        raise SnapshotError("three bounded real cold objects were not found")
    total = sum(row["signature"][2] for row in samples)
    admit_workspace(Path("/var/lib"), 32 * 1024**3 + 2 * total)
    private = Path(tempfile.mkdtemp(prefix="stockagent-native-cold-read-", dir="/var/lib"))
    rows = []
    for turn in range(args.rounds):
        for profile in (("drvfs", "native") if turn % 2 == 0 else ("native", "drvfs")):
            started, cpu = time.monotonic(), cpu_seconds()
            for index, sample in enumerate(samples):
                path = sample["path"]
                if signature(path) != sample["signature"]:
                    raise SnapshotError("immutable D sample changed; scratch retained")
                target = private / f"{turn}-{profile}-{index}.blob"
                digest, count = hashlib.sha256(), 0
                reader = binary_reader(path) if profile == "native" else path.open("rb")
                with reader as source, target.open("xb") as output:
                    while block := source.read(4 * 1024**2):
                        output.write(block)
                        digest.update(block)
                        count += len(block)
                    output.flush()
                    os.fsync(output.fileno())
                if (count != sample["signature"][2] or digest.hexdigest() != sample["sha256"]
                        or sha256_file(target) != sample["sha256"] or signature(path) != sample["signature"]):
                    raise SnapshotError("D recovery full SHA/signature differs; scratch retained")
                target.unlink()
            duration = time.monotonic() - started
            row = {"profile": profile, "round": turn + 1, "logical_bytes": total,
                   "complete_workflow_seconds": duration, "cpu_seconds": cpu_seconds() - cpu,
                   "verified_MiB_per_second": total / duration / 1024**2,
                   "full_original_and_restored_sha_verified": True}
            rows.append(row)
            print(json.dumps(row), flush=True)
    if any(private.iterdir()):
        raise SnapshotError("unknown measurement scratch retained")
    private.rmdir()
    medians = {p: statistics.median(r["complete_workflow_seconds"] for r in rows if r["profile"] == p)
               for p in ("drvfs", "native")}
    value = {"state": "real_d_blob_recovery_measured", "samples": [
        {**r, "path": str(r["path"])} for r in samples], "runs": rows, "medians": medians,
        "native_speedup": medians["drvfs"] / medians["native"],
        "scope": "bounded blob copy/fsync/SHA; not a full release or NAS workflow",
        "os_cache_flushed": False, "authoritative_cold_changed": False, "scratch_removed": True}
    atomic_write_json(args.output, value)
    print(json.dumps({"medians": medians, "native_speedup": value["native_speedup"]}))


if __name__ == "__main__":
    main()
