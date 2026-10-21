#!/usr/bin/env python3
"""Measure real byte-preserving legacy publish/recovery on native C and D.

Fresh private benchmark paths only. Does not notify Syncthing, alter source,
touch authoritative objects/heads, skip decode, clear caches or remount a drive.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

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
import stockagent.data_sync.legacy_artifact_archive as archive_module
from stockagent.remote_build import observe_node


@contextmanager
def decoder_variant(variant):
    """The retained oracle performs both original complete input reads."""
    if variant is None or variant == "single-pass":
        yield
        return
    if variant != "two-pass":
        raise ValueError("unknown archive decoder comparison")

    def two_pass(path, codec):
        encoded_digest = sha256_file(path)
        encoded_size = path.stat().st_size
        decoded_digest, decoded_size = archive_module._decode_hash(path, codec)
        return encoded_digest, encoded_size, decoded_digest, decoded_size

    with patch.object(archive_module, "_encoded_original_hashes", two_pass):
        yield


def cpu_seconds():
    own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


def complete_trial(spec, artifact_root, cold, destination, cache, expected, *, variant=None,
                   batch_directory_fsync=False):
    """Keep publication, independent full cold decode, restore and readback."""
    original = artifact_root / spec.relative_root
    baseline = metadata_tree(original)
    started, cpu_before = time.monotonic(), cpu_seconds()
    with decoder_variant(variant):
        result = publish_archive(spec, artifact_root, cold, repo_root=ROOT,
                                 batch_directory_fsync=batch_directory_fsync)
        published = time.monotonic()
        proof = verify_cold_archive(spec, cold)
        verified = time.monotonic()
        restore_archive(spec, cold, destination, materialized_root=cache)
        if metadata_tree(original)["rows"] != baseline["rows"]:
            raise SnapshotError("isolated benchmark original changed; scratch retained")
        actual = {p.relative_to(destination).as_posix() for p in destination.rglob("*") if p.is_file()}
        if actual != set(expected) | {".LEGACY_RESTORED.json"}:
            raise SnapshotError("benchmark restored file set differs; scratch retained")
        for member, raw in expected.items():
            path = destination / member
            if (sha256_file(path) != raw["sha256"] or path.stat().st_size != raw["size"]
                or path.stat().st_mode & 0o7777 != raw["mode"] or path.stat().st_mtime_ns != raw["mtime_ns"]):
                raise SnapshotError("benchmark restore differs; scratch retained")
    finished = time.monotonic()
    return {"logical_bytes": sum(row["size"] for row in expected.values()),
            "cold_bytes": result["cold_bytes"], "files": len(expected),
            "publish_seconds": published - started, "cold_decode_verify_seconds": verified - published,
            "restore_and_original_sha_seconds": finished - verified,
            "complete_workflow_seconds": finished - started,
            "complete_workflow_cpu_seconds_including_children": cpu_seconds() - cpu_before,
            "all_original_sha_mode_mtime_verified": True, "cold_verified": proof["cold_verified"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--file", required=True, action="append")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--profile", action="append", choices=sorted(COMPRESSION_PROFILES),
                        help="Compare explicitly selected codecs with identical source and recovery work")
    parser.add_argument("--single-pass-decode", action="store_true",
                        help="Compare original two-read and fused single-read decode, in ABBA order")
    parser.add_argument("--encoding-on-d", action="store_true",
                        help="Place only this fresh benchmark's encoding scratch on D as well as its cold copy")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if not 1 <= args.rounds <= 3 or args.output.exists():
        parser.error("use 1-3 rounds and a fresh output receipt")
    if args.single_pass_decode and args.profile and len(set(args.profile)) != 1:
        parser.error("single-pass comparison requires one fixed encoding profile")
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
    # D is the guarded NTFS authority, not a native-Linux mutable return-state
    # directory. Keep native admission for C and budget this private D fixture
    # directly after the existing D mount/marker guard.
    if shutil.disk_usage(Path("/srv/stockagent-d-volume")).free < 32 * 1024**3 + 4 * size:
        raise SnapshotError("private D benchmark lacks fixture bytes plus 32 GiB reserve")
    node_before = observe_node(source, Path("/srv/stockagent-d-volume"))
    identity_paths = (Path(__file__), ROOT / "stockagent/data_sync/legacy_artifact_archive.py")
    code_identity = {str(path.relative_to(ROOT)): sha256_file(path) for path in identity_paths}
    native = Path(tempfile.mkdtemp(prefix="stockagent-archive-benchmark-", dir="/var/lib"))
    original = native / "artifacts/markets/measurement"
    original.mkdir(parents=True)
    for name in names:
        target = original / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, target)
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
    rows = []
    profiles = [("baseline", DEFAULT_COMPRESSION_PROFILE, False),
                ("adaptive", ADAPTIVE_COMPRESSION_PROFILE, False),
                ("adaptive-batched", ADAPTIVE_COMPRESSION_PROFILE, True)]
    if args.profile:
        profiles = [(name, name, False) for name in dict.fromkeys(args.profile)]
    if args.single_pass_decode:
        codec = args.profile[0] if args.profile else DEFAULT_COMPRESSION_PROFILE
        profiles = [(name, codec, False) for name in ("two-pass", "single-pass")]
    for turn in range(args.rounds):
        for name, codec, batched in (profiles if turn % 2 == 0 else list(reversed(profiles))):
            _check_d_primary_mount(Path("/srv/stockagent-packed"))
            d_scratch = Path(tempfile.mkdtemp(prefix=".stockagent-archive-benchmark-", dir="/srv/stockagent-d-volume"))
            stage = ((d_scratch / "encoding-stage") if args.encoding_on_d
                     else native / f"stage-{turn}-{name}")
            cold = d_scratch / "cold"
            spec = LegacyArchiveSpec("legacy-io-measurement", "markets/measurement", 7, stage,
                                     durable_staging=False, compression_profile=codec)
            # These stable, old samples meet ordinary seven-day capture. Manual
            # mode is needed only for non-fsynced private encoding scratch;
            # retain fsynced staging for this ordinary canonical capture.
            spec = replace(spec, durable_staging=True)
            started, trial_cpu_before = time.monotonic(), cpu_seconds()
            destination = native / f"restored-{turn}-{name}"
            row = {"profile": name, "round": turn + 1,
                   **complete_trial(spec, native / "artifacts", cold, destination,
                       native / f"cache-{turn}-{name}", expected,
                       variant=name if args.single_pass_decode else None,
                       batch_directory_fsync=batched)}
            # Fresh, unshared private measurement directories, fully recovered
            # above. No authoritative cold path or existing source is selected.
            d_tree = metadata_tree(d_scratch)
            if (process_references(d_scratch) or any(r["kind"] == "unsupported" or r["cross_filesystem"]
                or r["kind"] == "file" and r["signature"][6] != 1 for r in d_tree["rows"])):
                raise SnapshotError("private benchmark scratch is referenced/linked; retained")
            _check_d_primary_mount(Path("/srv/stockagent-packed"))
            shutil.rmtree(d_scratch)
            row["complete_workflow_including_d_scratch_retirement_seconds"] = time.monotonic() - started
            row["complete_workflow_including_d_scratch_retirement_cpu_seconds"] = cpu_seconds() - trial_cpu_before
            row["owned_d_scratch_removed"] = True
            print(json.dumps(row), flush=True)
            rows.append(row)
    for member, raw in expected.items():
        path = real(source / member)
        if (sha256_file(path) != raw["sha256"] or path.stat().st_size != raw["size"]
            or path.stat().st_mode & 0o7777 != raw["mode"] or path.stat().st_mtime_ns != raw["mtime_ns"]):
            raise SnapshotError("real source changed across comparison; native scratch retained")
    summaries = [{"profile": name, "median_complete_workflow_seconds": statistics.median(
                  row["complete_workflow_seconds"] for row in rows if row["profile"] == name),
                  "median_including_d_scratch_retirement_seconds": statistics.median(
                  row["complete_workflow_including_d_scratch_retirement_seconds"]
                  for row in rows if row["profile"] == name)}
                 for name, _, _ in profiles]
    if process_references(native):
        raise SnapshotError("private native benchmark scratch is referenced; retained")
    if any(sha256_file(path) != code_identity[str(path.relative_to(ROOT))] for path in identity_paths):
        raise SnapshotError("archive comparison code changed; native scratch retained")
    shutil.rmtree(native)
    payload = {"state": "real_source_cold_workflow_verified", "source": str(source), "samples": expected,
               "comparison": "encoded-original-sha-single-read-v1" if args.single_pass_decode else "encoding-profiles",
               "encoding_on_d": args.encoding_on_d, "code_identity": code_identity, "node_before": node_before,
               "interleaved": True, "os_cache_flushed": False, "authoritative_cold_changed": False,
               "source_changed": False, "private_scratch_removed": True, "runs": rows, "summaries": summaries}
    atomic_write_json(args.output, payload)
    print(json.dumps({"output": str(args.output), "summaries": summaries}), flush=True)


if __name__ == "__main__":
    main()
