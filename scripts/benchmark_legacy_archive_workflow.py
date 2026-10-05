#!/usr/bin/env python3
"""Measure real byte-preserving legacy publish/recovery on native C and D.

Fresh private benchmark paths only. Does not notify Syncthing, alter source,
touch authoritative objects/heads, skip decode, clear caches or remount a drive.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file, SnapshotError, _safe_relative_path
from stockagent.data_sync.legacy_artifact_archive import (
    ADAPTIVE_COMPRESSION_PROFILE, DEFAULT_COMPRESSION_PROFILE, COMPRESSION_PROFILES, LegacyArchiveSpec,
    publish_archive, restore_archive, verify_cold_archive,
)
from stockagent.data_sync.remote_legacy_return import metadata_tree, real
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.training_return import admit_workspace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--file", required=True, action="append")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--profile", action="append", choices=sorted(COMPRESSION_PROFILES),
                        help="Compare explicitly selected codecs with identical source and recovery work")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if not 1 <= args.rounds <= 3 or args.output.exists():
        parser.error("use 1-3 rounds and a fresh output receipt")
    _check_d_primary_mount(Path("/srv/stockagent-packed"))
    source = real(args.source)
    names = [_safe_relative_path(name, "benchmark source").as_posix() for name in args.file]
    expected = {}
    for name in names:
        path = real(source / name)
        before = path.stat()
        expected[name] = {"sha256": sha256_file(path), "size": before.st_size,
                          "mode": before.st_mode & 0o7777, "mtime_ns": before.st_mtime_ns}
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise SnapshotError("real sample changed before benchmark")
    size = sum(row["size"] for row in expected.values())
    admit_workspace(Path("/var/lib"), 64 * 1024**3 + 5 * size)
    native = Path(tempfile.mkdtemp(prefix="stockagent-archive-benchmark-", dir="/var/lib"))
    original = native / "artifacts/markets/measurement"
    original.mkdir(parents=True)
    for name in names:
        target = original / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, target)
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
    baseline = metadata_tree(original)
    rows = []
    profiles = [("baseline", DEFAULT_COMPRESSION_PROFILE, False),
                ("adaptive", ADAPTIVE_COMPRESSION_PROFILE, False),
                ("adaptive-batched", ADAPTIVE_COMPRESSION_PROFILE, True)]
    if args.profile:
        profiles = [(name, name, False) for name in dict.fromkeys(args.profile)]
    for turn in range(args.rounds):
        for name, codec, batched in (profiles if turn % 2 == 0 else list(reversed(profiles))):
            _check_d_primary_mount(Path("/srv/stockagent-packed"))
            d_scratch = Path(tempfile.mkdtemp(prefix=".stockagent-archive-benchmark-", dir="/srv/stockagent-d-volume"))
            stage = native / f"stage-{turn}-{name}"
            spec = LegacyArchiveSpec("legacy-io-measurement", "markets/measurement", 7, stage,
                                     durable_staging=False, compression_profile=codec)
            # These stable, old samples meet ordinary seven-day capture. Manual
            # mode is needed only for non-fsynced private encoding scratch;
            # retain fsynced staging for this ordinary canonical capture.
            spec = replace(spec, durable_staging=True)
            started = time.monotonic()
            result = publish_archive(spec, native / "artifacts", d_scratch, repo_root=ROOT,
                                     batch_directory_fsync=batched)
            published = time.monotonic()
            proof = verify_cold_archive(spec, d_scratch)
            verified = time.monotonic()
            destination = native / f"restored-{turn}-{name}"
            restore_archive(spec, d_scratch, destination, materialized_root=native / f"cache-{turn}-{name}")
            if metadata_tree(original)["rows"] != baseline["rows"]:
                raise SnapshotError("isolated benchmark original changed; scratch retained")
            actual = {p.relative_to(destination).as_posix() for p in destination.rglob("*") if p.is_file()}
            if actual != set(names) | {".LEGACY_RESTORED.json"}:
                raise SnapshotError("benchmark restored file set differs; scratch retained")
            for member, raw in expected.items():
                path = destination / member
                if (sha256_file(path) != raw["sha256"] or path.stat().st_size != raw["size"]
                    or path.stat().st_mode & 0o7777 != raw["mode"] or path.stat().st_mtime_ns != raw["mtime_ns"]):
                    raise SnapshotError("benchmark restore differs; scratch retained")
            finished = time.monotonic()
            row = {"profile": name, "round": turn + 1, "logical_bytes": size,
                   "cold_bytes": result["cold_bytes"], "files": len(names),
                   "publish_seconds": published - started, "cold_decode_verify_seconds": verified - published,
                   "restore_and_original_sha_seconds": finished - verified,
                   "complete_workflow_seconds": finished - started,
                   "all_original_sha_mode_mtime_verified": True, "cold_verified": proof["cold_verified"]}
            print(json.dumps(row), flush=True)
            rows.append(row)
            # Fresh, unshared private measurement directories, fully recovered
            # above. No authoritative cold path or existing source is selected.
            d_tree = metadata_tree(d_scratch)
            if (process_references(d_scratch) or any(r["kind"] == "unsupported" or r["cross_filesystem"]
                or r["kind"] == "file" and r["signature"][6] != 1 for r in d_tree["rows"])):
                raise SnapshotError("private benchmark scratch is referenced/linked; retained")
            _check_d_primary_mount(Path("/srv/stockagent-packed"))
            shutil.rmtree(d_scratch)
    for member, raw in expected.items():
        path = real(source / member)
        if (sha256_file(path) != raw["sha256"] or path.stat().st_size != raw["size"]
            or path.stat().st_mode & 0o7777 != raw["mode"] or path.stat().st_mtime_ns != raw["mtime_ns"]):
            raise SnapshotError("real source changed across comparison; native scratch retained")
    summaries = [{"profile": name, "median_complete_workflow_seconds": statistics.median(
                  row["complete_workflow_seconds"] for row in rows if row["profile"] == name)}
                 for name, _, _ in profiles]
    if process_references(native):
        raise SnapshotError("private native benchmark scratch is referenced; retained")
    shutil.rmtree(native)
    payload = {"state": "real_source_cold_workflow_verified", "source": str(source), "samples": expected,
               "interleaved": True, "os_cache_flushed": False, "authoritative_cold_changed": False,
               "source_changed": False, "private_scratch_removed": True, "runs": rows, "summaries": summaries}
    atomic_write_json(args.output, payload)
    print(json.dumps({"output": str(args.output), "summaries": summaries}), flush=True)


if __name__ == "__main__":
    main()
