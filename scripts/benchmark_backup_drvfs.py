#!/usr/bin/env python3
"""Measure complete closed delivery creation and verification on D aliases."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.data_sync.offhost_backup import export_incremental_delivery, private_json  # noqa: E402
from stockagent.data_sync.packed_backup import signature  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--candidate-volume", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("retain a fresh benchmark receipt")
    cold = Path("/srv/stockagent-packed")
    transport = Path("/srv/stockagent-d-volume/stockagent-backup-ingress-lab203")
    candidate_cold = args.candidate_volume / "stockagent-cold-primary/packed"
    candidate_transport = args.candidate_volume / "stockagent-backup-ingress-lab203"
    marker = "stockagent-cold-primary/volume.json"
    if ((args.candidate_volume / marker).read_bytes()
            != Path("/srv/stockagent-d-volume", marker).read_bytes()
            or (candidate_cold / ".stockagent-d-primary").read_bytes() != (cold / ".stockagent-d-primary").read_bytes()
            or (candidate_transport / ".transport-owner.json").read_bytes() != (transport / ".transport-owner.json").read_bytes()):
        parser.error("candidate is not the enrolled D namespace")
    catalog = json.loads(args.catalog.read_bytes())
    blob = next(r for r in catalog["files"] if r["relative"].startswith("objects/blobs/") and 60 * 1024**2 < r["bytes"] < 68 * 1024**2)
    rows = [*([r for r in catalog["files"] if r["role"] == "cold_metadata" and r["bytes"] < 16384][:12]), blob]
    for row in rows:
        row["signature"] = list(signature(cold / row["relative"]))
    results = []
    sequence = [("candidate", candidate_cold, candidate_transport), ("baseline", cold, transport)] * 2
    for label, reader, destination_root in sequence:
        stage = destination_root / ".staging" / ("drvfs-benchmark-" + uuid.uuid4().hex)
        started = time.perf_counter()
        result = export_incremental_delivery(cold, rows, stage,
                                            catalog_identity=catalog["identity_sha256"], read_root=reader)
        # The source queue checks a closed batch again before journaling and publishing.
        checked = verify(stage, result["envelope_identity_sha256"])
        elapsed = time.perf_counter() - started
        row = {"backend": label, "seconds": elapsed, "files": len(rows),
               "bytes": checked["bytes_verified"], "MiB_per_second": checked["bytes_verified"] / 1024**2 / elapsed,
               "closed_envelope_and_READY_verified": True, "every_file_sha256_verified": True,
               "complete_file_set_verified": True, "staging_path": str(stage),
               "envelope_identity_sha256": result["envelope_identity_sha256"]}
        results.append(row)
        print(json.dumps(row), flush=True)
    result = {"contract": "source_complete_closed_delivery_drvfs_comparison_v1", "trials": results,
              "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "same_real_members_interleaved": True, "operating_system_cache_cleared": False,
              "primary_mount_or_data_changed": False, "transport_or_NAS_backup_claimed": False,
              "probe_files_retained_in_ignored_staging": True}
    for label, root in (("baseline", Path("/srv/stockagent-d-volume")), ("candidate", args.candidate_volume)):
        options = subprocess.check_output(["findmnt", "-n", "-o", "OPTIONS", "-T", str(root)], text=True)
        result[label + "_request_bytes"] = next(int(part.split("=", 1)[1]) for part in options.strip().split(",") if part.startswith("msize="))
    private_json(args.output, result)


if __name__ == "__main__":
    main()
