#!/usr/bin/env python3
"""Compare complete D ZIP-pack reconstruction and legacy original decoding."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file, atomic_write_json
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, _load_inventory, _validate_inventory, _native_pack_source
from stockagent.data_sync.legacy_artifact_archive import _decode_hash
from stockagent.data_sync.bulk_archive import signature
from stockagent.data_sync.training_return import admit_workspace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--legacy-manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("fresh measurement receipt required")
    os.umask(0o077)
    cold = Path("/srv/stockagent-packed")
    _check_d_primary_mount(cold)
    resolved = resolve_packed_snapshot_id(cold, args.dataset, args.snapshot_id)
    if sha256_file(args.legacy_manifest) != resolved.manifest["metadata"].get("legacy_manifest_sha256"):
        raise SnapshotError("benchmark original hashes are not bound to this exact cold release")
    legacy = json.loads(args.legacy_manifest.read_text())
    originals = {r["encoded_path"]: r for r in legacy["files"]}
    entries = _load_inventory(cold, resolved.manifest)
    _validate_inventory(resolved.manifest, entries)
    items = [r for r in resolved.manifest["archive"]["objects"] if r["kind"] == "pack" and r["bytes"] <= 64 * 1024**2]
    if not items:
        raise SnapshotError("no bounded complete real ZIP pack to compare")
    item = max(items, key=lambda r: r["file_count"])
    path = cold / item["relpath"]
    expected = [r for r in entries if r["kind"] == "file" and r["storage"]["object_sha256"] == item["sha256"]]
    if not all(r["path"] in originals for r in expected):
        raise SnapshotError("sample includes a non-legacy-payload member")
    total_original = sum(originals[r["path"]]["source"]["size"] for r in expected)
    admit_workspace(Path("/var/lib"), 32 * 1024**3 + 2 * item["logical_bytes"])
    before = signature(path)
    records = []
    for turn in range(3):
        for profile in (["drvfs", "native_ssd_pack"] if turn % 2 == 0 else ["native_ssd_pack", "drvfs"]):
            _check_d_primary_mount(cold)
            scratch = Path(tempfile.mkdtemp(prefix="stockagent-pack-recovery-benchmark-", dir="/var/lib"))
            started = time.perf_counter()
            if profile == "drvfs" and sha256_file(path) != item["sha256"]:
                raise SnapshotError("immutable real D pack differs; scratch retained")
            with _native_pack_source(path, item, enabled=profile == "native_ssd_pack") as source, zipfile.ZipFile(source) as archive:
                if archive.namelist() != [r["storage"]["member"] for r in expected] or archive.testzip() is not None:
                    raise SnapshotError("pack membership or CRC differs; scratch retained")
                for number, row in enumerate(expected):
                    target = scratch / f"{number:06d}.encoded"
                    size, digest = 0, hashlib.sha256()
                    with archive.open(row["storage"]["member"]) as reader, target.open("xb") as output:
                        while block := reader.read(4 * 1024**2):
                            output.write(block); digest.update(block); size += len(block)
                        output.flush(); os.fsync(output.fileno())
                    raw = originals[row["path"]]
                    if size != row["size"] or digest.hexdigest() != row["sha256"]:
                        raise SnapshotError("encoded member reconstruction differs; scratch retained")
                    decoded_sha, decoded_size = _decode_hash(target, raw["codec"])
                    if (decoded_sha != raw["original_sha256"] or decoded_size != raw["source"]["size"]
                            or sha256_file(target) != raw["encoded_sha256"]):
                        raise SnapshotError("original legacy decode differs; scratch retained")
                    target.unlink()
            if signature(path) != before or list(scratch.iterdir()):
                raise SnapshotError("cold pack changed or unknown scratch remains")
            scratch.rmdir()
            record = {"profile": profile, "round": turn + 1, "complete_workflow_seconds": time.perf_counter() - started,
                      "encoded_pack_bytes": item["bytes"], "decoded_original_bytes": total_original,
                      "files": len(expected), "full_pack_sha_crc_verified": True,
                      "all_original_and_encoded_sha_verified": True, "own_scratch_removed": True}
            records.append(record); print(json.dumps(record), flush=True)
            atomic_write_json(args.output.with_suffix(".progress.json"), {"state": "running", "runs": records})
    medians = {p: statistics.median(r["complete_workflow_seconds"] for r in records if r["profile"] == p)
               for p in ["drvfs", "native_ssd_pack"]}
    result = {"state": "real_pack_original_recovery_measured", "dataset": args.dataset,
              "snapshot_id": args.snapshot_id, "manifest_sha256": resolved.manifest_sha256,
              "pack_sha256": item["sha256"], "pack_signature": before, "runs": records, "medians": medians,
              "native_speedup": medians["drvfs"] / medians["native_ssd_pack"], "os_caches_flushed": False,
              "authoritative_cold_changed": False, "source_changed": False,
              "scope": "one complete actual pack: object SHA, ZIP CRC, per-member fsync/encoded SHA, original decode/SHA, scratch removal; not whole-release completion"}
    atomic_write_json(args.output, result)
    print(json.dumps({"medians": medians, "native_speedup": result["native_speedup"]}))


if __name__ == "__main__":
    main()
