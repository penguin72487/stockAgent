#!/usr/bin/env python3
"""Compare a fixed real cold-original request with full hot materialization."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.benchmark_legacy_archive_workflow import cpu_seconds, retire_private_scratch
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.training_return import admit_workspace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cold-root", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--rounds", type=int, choices=(1, 2, 3), default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists():
        parser.error("preserve the original output receipt")
    source, cold = args.source.absolute(), args.cold_root.absolute()
    if source.resolve(strict=True) != source:
        parser.error("source must be a real original directory")
    prefix = packed._safe_relative_path(args.prefix, "benchmark prefix").as_posix() + "/"
    packed._check_packed_read_authority(cold)
    release = packed.resolve_packed_snapshot_id(cold, args.dataset, args.snapshot_id)
    implementation = [Path(__file__), ROOT / "stockagent/data_sync/packed_snapshots.py",
        ROOT / "stockagent/data_sync/packed_metadata.py", ROOT / "stockagent/data_sync/desync_snapshots.py",
        ROOT / "scripts/benchmark_legacy_archive_workflow.py"]
    identities = {path.relative_to(ROOT).as_posix(): sha256_file(path) for path in implementation}
    with packed._existing_packed_publication_owner(cold, shared=True):
        with packed._open_inventory(cold, release.manifest) as entries:
            packed._validate_inventory(release.manifest, entries, retain_object_members=False)
            selected = [row for row in entries if row["kind"] == "file" and row["path"].startswith(prefix)]
        size = sum(int(row["size"]) for row in selected)
        if not selected or len(selected) > 4096 or not 0 < size <= 64 * 1024**2:
            raise SnapshotError("entire requested source prefix exceeds bounded memory-read scope")
        expected, observed = {}, {}
        for row in selected:
            path = source / row["path"]
            before = path.lstat()
            if path.resolve(strict=True) != path:
                raise SnapshotError("original benchmark source is redirected")
            with path.open("rb") as handle:
                raw = handle.read(int(row["size"]) + 1)
            if (len(raw) != row["size"] or hashlib.sha256(raw).hexdigest() != row["sha256"]
                    or packed._source_stat(path.lstat()) != packed._source_stat(before)):
                raise SnapshotError("fixed cold original differs from the current independent source")
            expected[row["path"]] = {"size": row["size"], "sha256": row["sha256"]}
            observed[path] = packed._source_stat(before)
        # Full materialization is only the comparator for this selected request;
        # its extra unrequested files and complete-release validation are named
        # below and are never claimed as the candidate's acceptance scope.
        admit_workspace(Path("/var/lib"), 64 * 1024**3 + 4 * int(release.manifest["source"]["logical_bytes"]))
        scratch = Path(tempfile.mkdtemp(prefix="stockagent-packed-read-benchmark-", dir="/var/lib"))
        rows = []
        for turn in range(args.rounds):
            profiles = ["full-materialization", "bounded-original-read"]
            for profile in profiles if turn % 2 == 0 else reversed(profiles):
                trial = scratch / f"{turn}-{profile}"
                started, cpu_before = time.monotonic(), cpu_seconds()
                materialized_files = materialized_bytes = 0
                if profile == "full-materialization":
                    target = packed.fetch_packed_snapshot(cold, trial, release)
                    values = {name: (target / name).read_bytes() for name in expected}
                    for path in trial.rglob("*"):
                        if path.is_file():
                            materialized_files += 1
                            materialized_bytes += path.lstat().st_blocks * 512
                else:
                    values = packed.read_packed_files(cold, release, names=expected,
                        maximum_bytes=size, maximum_object_bytes=512 * 1024**2)
                if set(values) != set(expected) or any(len(raw) != expected[name]["size"]
                        or hashlib.sha256(raw).hexdigest() != expected[name]["sha256"]
                        for name, raw in values.items()):
                    raise SnapshotError("complete selected originals differ from the independent oracle")
                del values
                if trial.exists():
                    retire_private_scratch(trial)
                rows.append({"profile": profile, "round": turn + 1,
                    "complete_wall_seconds_including_owned_scratch_retirement": time.monotonic() - started,
                    "complete_cpu_seconds_including_owned_scratch_retirement": cpu_seconds() - cpu_before,
                    "requested_files": len(expected), "requested_original_bytes": size,
                    "all_requested_original_SHA_verified": True,
                    "hot_materialized_files": materialized_files,
                    "hot_materialized_allocated_bytes": materialized_bytes,
                    "owned_trial_scratch_removed": not trial.exists()})
                print(json.dumps(rows[-1]), flush=True)
        if any(packed._source_stat(path.lstat()) != signature or sha256_file(path) != expected[
                path.relative_to(source).as_posix()]["sha256"] for path, signature in observed.items()):
            raise SnapshotError("original source changed across the complete comparison")
        if any(sha256_file(ROOT / name) != sha for name, sha in identities.items()):
            raise SnapshotError("measured implementation changed during comparison")
        if sha256_file(release.manifest_path) != release.manifest_sha256:
            raise SnapshotError("fixed release changed during comparison")
        retire_private_scratch(scratch)
    summaries = [{"profile": profile,
        "median_complete_wall_seconds": statistics.median(row[
            "complete_wall_seconds_including_owned_scratch_retirement"] for row in rows if row["profile"] == profile),
        "median_complete_cpu_seconds": statistics.median(row[
            "complete_cpu_seconds_including_owned_scratch_retirement"] for row in rows if row["profile"] == profile)}
        for profile in ("full-materialization", "bounded-original-read")]
    report = {"state": "fixed_real_source_original_request_verified",
        "dataset": args.dataset, "snapshot_id": args.snapshot_id,
        "manifest_sha256": release.manifest_sha256, "source": str(source), "prefix": args.prefix,
        "expected_originals": expected, "requested_files": len(expected), "requested_original_bytes": size,
        "full_release_files": release.manifest["source"]["files"],
        "full_release_logical_bytes": release.manifest["source"]["logical_bytes"],
        "code_sha256": identities, "runs": rows, "summaries": summaries,
        "original_source_changed": False, "fixed_release_changed": False,
        "provider_API_calls": 0, "original_source_deletions": 0, "new_materialization_owner": False,
        "candidate_scope": "requested originals only; full-release unselected-object availability is not claimed",
        "native_source_migration_claimed": False, "goal_complete": False}
    atomic_write_json(args.output, report)
    print(json.dumps({"output": str(args.output), "summaries": summaries}), flush=True)


if __name__ == "__main__":
    main()
