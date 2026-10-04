#!/usr/bin/env python3
"""Full-namespace, real-footer frozen replay of the canonical feature publisher.

Capture two actual cache generations and real stat/absence observations. Replay
those fixed observations for matched ABBA trials in a temporary workspace. This
measures field verification/reduction, public projection, complete JSON and
receipts; it is NOT live/cold source I/O, discovery, record refresh or browser.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.snapshot_data_refresh_services import publish_feature_inventory_snapshot  # noqa: E402
from stockagent.live import data_monitor_inventory as inventory  # noqa: E402
from stockagent.live.data_monitor_dashboard import feature_source_metadata_sha256  # noqa: E402
from stockagent.live.data_monitor_feature_shards import ProjectionStore, _read_private, projection_abi  # noqa: E402


def capture(root: Path) -> dict:
    started = time.perf_counter()
    cache_path = root / "artifacts/live/data_monitor/record_inventory_cache.json"
    before = inventory._cache_signature(cache_path)
    body = _read_private(cache_path, max_bytes=512 * 1024 * 1024)
    payload = json.loads(body)
    if before is None or before != inventory._cache_signature(cache_path):
        raise ValueError("cache advanced while capturing")
    selected = inventory._selected_files(root)
    if inventory._bound_dataset_memberships(payload) != inventory._dataset_memberships(selected):
        raise ValueError("membership advanced while capturing")
    source_stats = {}
    for paths in selected.values():
        for path in paths:
            if str(path) in source_stats:
                continue
            try:
                source_stats[str(path)] = path.stat()
            except OSError:
                source_stats[str(path)] = None
    status_path = root / "artifacts/live/data_monitor/public_status.json"
    status = json.loads(_read_private(status_path, max_bytes=64 * 1024 * 1024))
    if not isinstance(status, dict) or not isinstance(status.get("sources"), list):
        raise ValueError("invalid public label snapshot")
    return {"payload": payload, "selected": selected, "source_stats": source_stats,
            "signature": before, "sha256": hashlib.sha256(body).hexdigest(), "status": status,
            "capture_ms": round((time.perf_counter() - started) * 1_000, 3)}


def _run(root: Path, generation: dict, output: Path, *, prefer_shards: bool,
         previous_signature: list[int] | None, changed: set[str] | None) -> dict:
    frame = inventory.InventorySnapshot(
        root, generation["payload"], generation["selected"], previous_signature,
        generation["signature"], frozenset(changed) if changed is not None else None,
    )
    original_stat = Path.stat
    stat_calls = 0

    def frozen_stat(path, *args, **kwargs):
        nonlocal stat_calls
        key = str(path)
        if key not in generation["source_stats"]:
            return original_stat(path, *args, **kwargs)
        stat_calls += 1
        value = generation["source_stats"][key]
        if value is None:
            raise FileNotFoundError("captured source absence")
        return value

    metadata_hash = feature_source_metadata_sha256(generation["status"], generation["payload"]["datasets"])
    started = time.perf_counter()
    with patch.object(Path, "stat", frozen_stat):
        result = publish_feature_inventory_snapshot(
            root, output, snapshot=frame, public_status=generation["status"],
            feature_revision=generation["payload"].get("feature_revision"),
            source_metadata_sha256=metadata_hash, prefer_shards=prefer_shards,
        )
    elapsed = (time.perf_counter() - started) * 1_000
    return {
        "variant": "sharded" if prefer_shards else "full", "wall_ms": round(elapsed, 3),
        "source_stat_calls": stat_calls, "fields": result["fields"],
        "source_observation_root": result["source_observation_root"],
        "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "output_bytes": output.stat().st_size, "stages_ms": result["stages_ms"],
        "inventory_timing_ms": result["inventory_timing_ms"],
        "projection_cache": result["projection_cache"],
    }


def benchmark(root: Path, *, timeout_seconds: float = 60.0) -> dict:
    abi = projection_abi()
    first = capture(root)
    deadline = time.monotonic() + timeout_seconds
    second = None
    while time.monotonic() < deadline:
        cache_path = root / "artifacts/live/data_monitor/record_inventory_cache.json"
        if inventory._cache_signature(cache_path) != first["signature"]:
            try:
                candidate = capture(root)
            except (OSError, ValueError):
                time.sleep(0.25)
                continue
            if candidate["signature"] != first["signature"]:
                second = candidate
                break
        time.sleep(0.25)
    if second is None:
        return {"state": "no_new_generation", "production_writes": False}
    changed = inventory.inventory_dataset_delta(first["payload"], second["payload"], second["selected"])
    if changed is None:
        return {"state": "inconclusive_delta", "production_writes": False}
    trials = []
    seed_trials = []
    with tempfile.TemporaryDirectory(prefix="data-monitor-pipeline-abba-") as directory:
        for index, variant in enumerate(("full", "sharded", "sharded", "full")):
            output = Path(directory) / f"trial-{index}" / "feature_inventory.json"
            if variant == "sharded":
                seed_trials.append(_run(root, first, output, prefer_shards=True,
                                        previous_signature=None, changed=None))
                store = ProjectionStore(output)
                if store.manifest() is None:
                    raise ValueError("cold seed did not build a valid cache")
            trials.append(_run(root, second, output, prefer_shards=variant == "sharded",
                               previous_signature=first["signature"], changed=changed))
    parity = len({(row["output_sha256"], row["output_bytes"], row["source_observation_root"], row["fields"]) for row in trials}) == 1
    expected_stats = sum(len(paths) for paths in second["selected"].values())
    all_sources_checked = all(row["source_stat_calls"] == expected_stats for row in trials)
    stable_code = projection_abi() == abi
    medians = {variant: statistics.median(row["wall_ms"] for row in trials if row["variant"] == variant)
               for variant in ("full", "sharded")}
    return {
        "schema_version": 1, "state": "verified" if parity and all_sources_checked and stable_code else "mismatch",
        "scope": "full real-footer namespace; frozen actual stat/absence replay; field scan, math, public JSON and receipt",
        "excludes": ["imports", "cache_decode", "directory_discovery", "record_refresh", "live_source_stat_io",
                     "OS-cold cache control", "provider_calls", "network", "browser_paint"],
        "production_writes": False, "source_deletion": False, "promotion": False,
        "first_generation": {key: first[key] for key in ("signature", "sha256", "capture_ms")},
        "second_generation": {key: second[key] for key in ("signature", "sha256", "capture_ms")},
        "changed_dataset_ids": sorted(changed), "selected_file_references": expected_stats,
        "unique_selected_paths": len(second["source_stats"]), "complete_bytes_parity": parity,
        "all_source_identities_checked": all_sources_checked, "code_abi_stable": stable_code,
        "code_abi": abi, "cold_seed_trials": seed_trials, "trials": trials, "median_ms": medians,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("benchmark output must be a new evidence file")
    try:
        report = benchmark(args.repo_root, timeout_seconds=args.timeout_seconds)
    except (OSError, ValueError) as exc:
        report = {"state": "inconclusive_capture", "error_type": type(exc).__name__,
                  "reason": str(exc), "production_writes": False}
    atomic_write_json(args.output, report)
    print(json.dumps({"state": report["state"], "output": str(args.output), "median_ms": report.get("median_ms")}), flush=True)
    return 0 if report["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
