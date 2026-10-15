"""Read back a complete native snapshot, not service liveness or all-history.

Reuse canonical generation/receipt verification and the complete identity scan.
No provider request, source write, service control, model or CUDA initialization.
An advancing source/cache is inconclusive, never permission to bypass a fence.
"""

from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.benchmark_data_monitor_feature_delta import _read_generation  # noqa: E402
from stockagent.live import data_monitor_inventory as inventory  # noqa: E402


UNIT = "stockagent-data-refresh-status-snapshot.service"
CODE = ("stockagent/live/data_monitor_inventory.py", "stockagent/live/data_monitor_record_codec.py",
        "scripts/snapshot_data_refresh_services.py")


def code_identity():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in CODE}


def accept_event(rows, cache):
    for row in reversed(rows):
        event = row["event"]
        if (event.get("event") == "data_monitor_timing"
                and event.get("inventory_mapping_validation_types") == ["dict", "Mapping"]
                and event.get("inventory_source_observation_root") == cache["source_observation_root"]
                and event.get("inventory_feature_revision") == cache["feature_revision"]
                and event.get("cached_files") == len(cache["files"])
                and event.get("feature_source_observation", {}).get("matches_record") is True
                and event.get("feature_source_observation", {}).get("inventory_matches_cache") is True):
            return row
    raise ValueError("no complete native event bound to this generation and actual runtime guards")


def verify(comparison: Path):
    before = code_identity()
    report = json.loads(comparison.read_bytes())
    if not (report.get("mode") == "mapping" and report.get("state") == "verified"
            and all(report.get(key) is True for key in (
                "code_stable", "parity", "cache_bytes_parity", "feature_bytes_parity", "all_source_identities_checked"))
            and report["code_after"][CODE[0]] == before[CODE[0]]
            and report["code_after"][CODE[1]] == before[CODE[1]]):
        raise ValueError("full benchmark parity or current measured implementation differs")
    tree = ast.parse((ROOT / CODE[0]).read_text())
    if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "isinstance"
           and len(node.args) > 1 and isinstance(node.args[1], ast.Name) and node.args[1].id == "Mapping"
           for node in ast.walk(tree)):
        raise ValueError("native Mapping validation still has unmeasured direct ABC calls")
    cache, feature, revision = _read_generation(ROOT)
    selected = inventory._selected_files(ROOT)
    if inventory._bound_dataset_memberships(cache) != inventory._dataset_memberships(selected):
        raise ValueError("source membership advanced beyond the completed generation")
    scan = inventory.FeatureInventoryScan(ROOT, snapshot=inventory.InventorySnapshot(ROOT, cache, selected))
    owners = 0
    for dataset in scan:
        owners += bool(dataset.by_schema)
    source = scan.summary(datasets_with_schema=owners)
    if (source["source_observation_root"] != cache["source_observation_root"]
            or source["source_observation_matches_cache"] is not True):
        raise ValueError("source identities advanced during full native readback")
    raw = subprocess.run(["journalctl", "-u", UNIT, "--since", "15 minutes ago", "-o", "json", "--no-pager"],
                         check=True, capture_output=True, text=True, timeout=10).stdout
    rows = []
    for line in raw.splitlines():
        row = json.loads(line)
        try:
            event = json.loads(row.get("MESSAGE", ""))
        except (ValueError, TypeError):
            continue
        if isinstance(event, dict):
            rows.append({"invocation_id": row.get("_SYSTEMD_INVOCATION_ID"),
                         "timestamp_us": row.get("__REALTIME_TIMESTAMP"), "event": event})
    event = accept_event(rows, cache)
    if (not event["invocation_id"] or int(event["timestamp_us"]) * 1000 < max((ROOT / name).stat().st_mtime_ns for name in CODE)
            or before != code_identity() or "torch" in sys.modules):
        raise ValueError("native owner/code admission or CUDA-cold verifier closure differs")
    return {"contract": "data_monitor_dict_first_native_readback_v1", "state": "accepted_bounded_optimization",
            "observed_at_utc": datetime.now(UTC).isoformat(), "comparison": str(comparison),
            "comparison_sha256": hashlib.sha256(comparison.read_bytes()).hexdigest(), "code_sha256": before,
            "native_event": event, "feature_revision": revision, "features": len(feature["rows"]),
            "cached_files": len(cache["files"]), "source_identity_scan": source,
            "full_generation_receipt_and_feature_bytes_verified": True,
            "provider_calls": 0, "source_writes": False, "services_changed": False,
            "all_history_complete_claim": False, "whole_snapshot_speedup_claim": False,
            "per_variant_ram_reduction_claim": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink() or not args.output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        parser.error("new private operational evidence required")
    result = verify(args.comparison)
    atomic_write_json(args.output, result)
    print(json.dumps({name: result[name] for name in ("state", "features", "cached_files")}))


if __name__ == "__main__":
    main()
