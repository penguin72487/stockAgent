#!/usr/bin/env python3
"""Audit and exactly repair retained missing objects from authorized hot sources."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.repair_packed_source_objects import original_members, repair  # noqa: E402
from stockagent.data_sync.backup_stream import capture_catalog  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402


def heads_identity(cold: Path) -> dict:
    return {p.relative_to(cold).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (cold / "heads").glob("*/*.json")}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve prior per-release recovery receipts")
    args.output.mkdir(mode=0o700, parents=True)
    config = json.loads(args.config.read_bytes())
    cold = Path(config["cold_root"])
    started = time.perf_counter()
    catalog = capture_catalog(cold, Path(config["publication_catalog"]))
    heads_before = heads_identity(cold)
    affected = [r for r in catalog["releases"] if r["missing_objects"]]
    caches = {}
    for dataset in sorted({r["dataset"] for r in affected}):
        required = {Path(p).name.split(".", 1)[0] for r in affected if r["dataset"] == dataset for p in r["missing_objects"]}
        caches[dataset] = original_members(cold, dataset, required)
    results = []
    for i, release in enumerate(affected):
        try:
            dry = repair(release["dataset"], cold, apply=False, receipt=args.output / f"{i:03d}-dry.json",
                         snapshot_id=release["snapshot_id"], original_member_cache=caches[release["dataset"]])
            row = {"dataset": release["dataset"], "snapshot_id": release["snapshot_id"], "dry": dry}
            if args.apply and dry["recoverable"]:
                row["apply"] = repair(release["dataset"], cold, apply=True, receipt=args.output / f"{i:03d}-apply.json",
                    snapshot_id=release["snapshot_id"], original_member_cache=caches[release["dataset"]])
            results.append(row)
        except (OSError, ValueError, RuntimeError) as error:
            results.append({"dataset": release["dataset"], "snapshot_id": release["snapshot_id"], "error": type(error).__name__})
        print(json.dumps({"release": i + 1, "total": len(affected), "recoverable_source_matches": len(results[-1].get("dry", {}).get("recoverable", [])),
                          "installed_exact_objects": len(results[-1].get("apply", {}).get("restored", [])), "error": results[-1].get("error")}), flush=True)
    after = capture_catalog(cold, Path(config["publication_catalog"]))
    restored = {r["relpath"]: r for entry in results for r in entry.get("apply", {}).get("restored", [])}
    reasons = Counter(r["reason"] for entry in results for r in entry.get("dry", {}).get("unresolved", []))
    result = {"state": "retained_exact_source_recovery_completed", "apply": args.apply,
        "missing_before": len(catalog["missing_objects"]), "missing_after": len(after["missing_objects"]),
        "restored_unique_objects": len(restored), "restored_bytes": sum(r["bytes"] for r in restored.values()),
        "heads_observed_unchanged": heads_identity(cold) == heads_before,
        "source_files_modified": False, "source_or_cold_data_deleted": False,
        "releases": results, "unresolved_source_reasons": dict(reasons),
        "release_error_count": sum("error" in r for r in results), "complete_workflow_seconds": time.perf_counter() - started,
        "nas_recovery_verified": False}
    private_json(args.output / "result.json", result)
    private_json(args.output / "catalog-after.json", after)
    print(json.dumps({k: v for k, v in result.items() if k not in {"releases"}}, ensure_ascii=False))
    if result["release_error_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
