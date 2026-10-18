"""Read-only real-input benchmark of complete incremental-version round trips.

Run as a module with runtime_env.sh. Payloads are copied into a private temporary
fixture; the production sources, cold authority and services are never changed.
The reference emulates the former pre-hash + chunk-write source passes. Both
variants retain full cold object/member/original SHA checks and restoration.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import shutil
import statistics
import tempfile
import time
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import sha256_file


def signature(path):
    info = path.lstat()
    return tuple(getattr(info, field) for field in
                 ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"))


def source_proof(path):
    before = signature(path)
    if path.is_symlink() or not path.is_file() or not 8 * 1024**2 < before[2] <= 256 * 1024**2:
        raise ValueError("use a real closed 8..256 MiB source file")
    digest = sha256_file(path)
    if signature(path) != before:
        raise ValueError("benchmark input changed")
    return {"path": str(path), "sha256": digest, "bytes": before[2], "signature": list(before)}


@contextmanager
def former_source_passes(enabled, previous, counts):
    original = packed._store_chunked_source
    def two_pass(root, source, entry, **options):
        if entry.sha256 is None:
            entry.sha256 = packed._hash_source_entry(source, entry)
            counts["source_pre_hash_passes"] += 1
            counts["source_pre_hash_bytes"] += entry.size
            prior = previous.get(entry.path)
            if prior and prior["sha256"] == entry.sha256 and prior["size"] == entry.size:
                entry.storage = dict(prior["storage"])
                return
        original(root, source, entry, **options)
    with patch.object(packed, "_store_chunked_source", two_pass if enabled else original):
        yield


def run_trial(inputs, proofs, *, reference):
    started = time.perf_counter()
    cpu_started = time.process_time()
    phases, previous, counts = [], {}, {"source_pre_hash_passes": 0, "source_pre_hash_bytes": 0}
    with tempfile.TemporaryDirectory(prefix="stockagent-version-benchmark-") as temporary:
        base = Path(temporary)
        source, cold = base / "source", base / "cold"
        source.mkdir()
        (source / "units.json").write_text(json.dumps({"code": "0050", "value": -7, "missing": None,
                                                        "volume_unit": "shares"}))
        selected = []
        with former_source_passes(reference, previous, counts):
            for index, original in enumerate(inputs):
                shutil.copyfile(original, source / "source.parquet")
                if sha256_file(source / "source.parquet") != proofs[index]["sha256"]:
                    raise ValueError("private fixture copy differs from its source")
                stamp = time.perf_counter()
                release = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="test",
                    loose_file_threshold_bytes=8 * 1024**2, file_chunk_bytes=8 * 1024**2,
                    pack_buckets=2, batch_directory_fsync=True)
                publish_seconds = time.perf_counter() - stamp
                stamp = time.perf_counter()
                proof = packed.verify_packed_snapshot(cold, release,
                                                      reconstruct_paths=["source.parquet", "units.json"])
                verify_seconds = time.perf_counter() - stamp
                stamp = time.perf_counter()
                restored = packed.fetch_packed_snapshot(cold, base / "restored", release)
                if (sha256_file(restored / "source.parquet") != proofs[index]["sha256"]
                        or json.loads((restored / "units.json").read_text())["missing"] is not None
                        or proof["independently_reconstructed_files"] != 2):
                    raise ValueError("complete cold restoration failed")
                phases.append({"stage": "baseline" if index == 0 else "incremental",
                    "publish_seconds": publish_seconds, "full_verify_seconds": verify_seconds,
                    "restore_and_readback_seconds": time.perf_counter() - stamp,
                    "new_stored_bytes": release.manifest["archive"]["new_stored_bytes"],
                    "logical_bytes": release.manifest["source"]["logical_bytes"]})
                previous.clear()
                previous.update({row["path"]: row for row in packed._load_inventory(cold, release.manifest)
                                 if row["kind"] == "file"})
                selected.append((release, proofs[index]["sha256"]))
            stamp = time.perf_counter()
            noop = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="test",
                loose_file_threshold_bytes=8 * 1024**2, file_chunk_bytes=8 * 1024**2,
                pack_buckets=2, batch_directory_fsync=True)
            if noop.manifest_sha256 != selected[-1][0].manifest_sha256:
                raise ValueError("unchanged version advanced its semantic head")
            phases.append({"stage": "semantic_noop", "seconds": time.perf_counter() - stamp})
        # The old snapshot is still independently recoverable after updates.
        first, expected = selected[0]
        old = packed.fetch_packed_snapshot(cold, base / "old-restored", first)
        if sha256_file(old / "source.parquet") != expected:
            raise ValueError("incremental update damaged an earlier version")
    return {"variant": "former_two_pass" if reference else "single_pass",
            "complete_wall_seconds": time.perf_counter() - started,
            "complete_cpu_seconds": time.process_time() - cpu_started,
            "phases": phases, **counts, "full_roundtrip_verified": True}


def evaluate_cdc(inputs):
    results = []
    for chunking, maximum in (("fixed-v1", 8 * 1024**2), ("fastcdc-v1", 8 * 1024**2),
                              ("fastcdc-v1", 1024**2)):
        started, objects, total, chunks = time.perf_counter(), {}, 0, 0
        for path in inputs:
            with path.open("rb") as stream:
                for block in packed._source_blocks(stream, maximum, chunking):
                    total += len(block)
                    chunks += 1
                    objects[hashlib.sha256(block).hexdigest()] = len(block)
        results.append({"policy": chunking, "maximum_bytes": maximum, "logical_bytes": total,
            "unique_chunk_bytes": sum(objects.values()), "reused_bytes": total - sum(objects.values()),
            "chunks": chunks, "seconds": time.perf_counter() - started})
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--evaluate-cdc", action="store_true")
    args = parser.parse_args(argv)
    if not 2 <= len(args.source) <= 4 or args.output.exists() or args.output.is_symlink():
        parser.error("use 2..4 closed real input revisions and a new output receipt")
    inputs = [path.absolute() for path in args.source]
    proofs = [source_proof(path) for path in inputs]
    trials = [run_trial(inputs, proofs, reference=variant) for variant in (True, False, False, True)]
    cdc = evaluate_cdc(inputs) if args.evaluate_cdc else None
    for path, proof in zip(inputs, proofs, strict=True):
        if list(signature(path)) != proof["signature"] or sha256_file(path) != proof["sha256"]:
            raise ValueError("production input changed during benchmark; retain unaccepted results")
    reference = statistics.mean(t["complete_wall_seconds"] for t in trials if t["variant"] == "former_two_pass")
    candidate = statistics.mean(t["complete_wall_seconds"] for t in trials if t["variant"] == "single_pass")
    result = {"contract": "dataset_version_complete_pipeline_benchmark_v1", "state": "verified",
        "observed_at_utc": datetime.now(UTC).isoformat(), "sources": proofs, "trials": trials,
        "mean_complete_seconds": {"former_two_pass": reference, "single_pass": candidate},
        "complete_speed_ratio": reference / candidate, "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "cdc": cdc, "source_payload_writes": 0, "production_cold_writes": 0, "provider_calls": 0,
        "caveats": ["Warm local SSD fixture, not an end-to-end D/NAS throughput claim.",
                    "Four A/B/B/A trials retain copying, hashing, publication, complete verification, old-version restore and cleanup.",
                    "Raw Parquet chunk reuse is measured, not inferred from synthetic insertion tests."]}
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({key: result[key] for key in ("state", "mean_complete_seconds", "complete_speed_ratio", "cdc")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
