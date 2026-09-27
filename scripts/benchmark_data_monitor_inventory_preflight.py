#!/usr/bin/env python3
"""Reproduce REJECTED directory hints; production was rolled back.

No Parquet data/footer reads, discovery, service action or production writes.
The source cache is copied into memory with a five-part stat fence. A private
index covers at most 4096 receipt-bound Yahoo paths. The stale-directory case
is fault injection into that private index, not a measured production change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import sys
import tempfile
import time
from types import ModuleType
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REJECTED_CANDIDATE = REPO_ROOT / "artifacts/benchmarks/data-monitor-directory-hints-rejected-candidate-20260927.py"
REJECTED_CANDIDATE_SHA256 = "b3967f5d0aac13033f5cf1bb0bc1f9817141cf97d8515c9d83ac6b09a9cbd600"


def load_rejected_candidate() -> ModuleType:
    """Load only the exact saved local experiment, never alter production code."""
    body = REJECTED_CANDIDATE.read_bytes()
    if hashlib.sha256(body).hexdigest() != REJECTED_CANDIDATE_SHA256:
        raise ValueError("rejected candidate source SHA changed")
    module = ModuleType("_rejected_directory_hint_inventory")
    module.__file__ = str(REJECTED_CANDIDATE)
    sys.modules[module.__name__] = module
    exec(compile(body, str(REJECTED_CANDIDATE), "exec"), module.__dict__)
    return module


inventory = load_rejected_candidate()


def _stable_bytes(path: Path) -> bytes:
    before = inventory._file_identity(path.stat())
    with path.open("rb") as stream:
        if inventory._file_identity(os.fstat(stream.fileno())) != before:
            raise ValueError("metadata source changed before read")
        body = stream.read()
        if inventory._file_identity(os.fstat(stream.fileno())) != before:
            raise ValueError("metadata source changed during read")
    if inventory._file_identity(path.stat()) != before:
        raise ValueError("metadata source changed after read")
    return body


def benchmark(cache_path: Path, *, path_prefix: str, limit: int) -> dict:
    if not 1 <= limit <= 4096:
        raise ValueError("limit must be between 1 and 4096")
    code_paths = (Path(__file__).resolve(), REJECTED_CANDIDATE)
    code_before = {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    body = _stable_bytes(cache_path)
    frozen = json.loads(body)
    if frozen.get("version") != inventory.INVENTORY_VERSION:
        raise ValueError("unexpected inventory metadata version")
    hint_build_trials = []
    for _ in range(4):
        wall_started, cpu_started = time.perf_counter_ns(), time.process_time_ns()
        all_hints = inventory._directory_preflight_hints(frozen["files"])
        hint_build_trials.append({
            "wall_ms": (time.perf_counter_ns() - wall_started) / 1e6,
            "cpu_ms": (time.process_time_ns() - cpu_started) / 1e6,
            "cache_files": len(frozen["files"]), "directory_hints": len(all_hints),
        })
    candidates = sorted(key for key in frozen["files"] if key.startswith(path_prefix))
    if len(candidates) < limit:
        raise ValueError("not enough source paths for the requested fixed sample")
    keys = sorted(random.Random(20260927).sample(candidates, limit))
    cache = {key: frozen["files"][key] for key in keys}
    selected = {"bounded_metadata_probe": [Path(key) for key in keys]}
    before = {key: inventory._file_identity(Path(key).stat()) for key in keys}
    if any(cache[key].get("file_identity") != before[key] for key in keys):
        raise ValueError("sample no longer matches the source metadata receipt")
    contract = hashlib.sha256(json.dumps(
        [[key, before[key]] for key in keys], separators=(",", ":"),
    ).encode()).hexdigest()
    hints = inventory._directory_preflight_hints(cache)
    if not hints:
        raise ValueError("sample has no ordinary directory hints")
    fingerprint = inventory._selection_fingerprint(selected, cache=cache)
    original_fingerprint = inventory._selection_fingerprint
    trials = []
    with tempfile.TemporaryDirectory(prefix="inventory-preflight-") as temporary:
        private_cache = Path(temporary) / "record_inventory_cache.json"
        # The probe checks this metadata file's identity, not its contents.
        private_cache.write_text("{}\n", encoding="utf-8")
        index_path = inventory._quick_index_path(private_cache)
        index = {
            "version": inventory.FAST_INDEX_VERSION,
            "inventory_version": inventory.INVENTORY_VERSION,
            "cache_signature": inventory._cache_signature(private_cache),
            "datasets": {}, "cached_files": limit, "identity_unbound_files": 0,
            "feature_revision": frozen["feature_revision"],
            "selection_fingerprint": fingerprint,
            "preflight_file_hints": [
                [key, before[key]] for key in random.Random(0).sample(keys, min(512, limit))
            ],
        }
        for scenario in ("unchanged", "injected_stale_directory"):
            scenario_outputs = []
            for variant in ("legacy", "directory_hints", "directory_hints", "legacy"):
                trial_index = dict(index)
                directory_hints = [[directory, list(identity)] for directory, identity in hints]
                if scenario == "injected_stale_directory":
                    directory_hints[0][1][3] -= 1
                    trial_index["selection_fingerprint"] = "0" * 32
                if variant == "directory_hints":
                    trial_index["preflight_directory_hints"] = directory_hints
                trial_index["checksum"] = inventory._quick_index_checksum(trial_index)
                index_path.write_text(json.dumps(trial_index), encoding="utf-8")
                full_scans = 0

                def counted_fingerprint(paths, *, cache=None):
                    nonlocal full_scans
                    if cache is None:
                        full_scans += 1
                    return original_fingerprint(paths, cache=cache)

                with patch.object(inventory, "_selected_files", return_value=selected), patch.object(
                    inventory, "_selection_fingerprint", side_effect=counted_fingerprint,
                ):
                    wall_started, cpu_started = time.perf_counter_ns(), time.process_time_ns()
                    result = inventory._read_quick_index(Path(temporary), private_cache, snapshot=None)
                    cpu_ns = time.process_time_ns() - cpu_started
                    wall_ns = time.perf_counter_ns() - wall_started
                projection = None if result is None else {
                    key: value for key, value in result.items() if key != "timing_ms"
                }
                scenario_outputs.append(projection)
                expected_scans = int(scenario == "unchanged" or variant == "legacy")
                if full_scans != expected_scans:
                    raise ValueError("early-reject/full-scan contract changed")
                trials.append({
                    "scenario": scenario, "variant": variant,
                    "wall_ms": wall_ns / 1e6, "cpu_ms": cpu_ns / 1e6,
                    "full_file_scans": full_scans,
                    "full_scan_paths": full_scans * limit,
                    "result_sha256": hashlib.sha256(json.dumps(
                        projection, sort_keys=True, separators=(",", ":"),
                    ).encode()).hexdigest(),
                })
            if any(value != scenario_outputs[0] for value in scenario_outputs[1:]):
                raise ValueError("ABBA probe output parity failed")
    after = {key: inventory._file_identity(Path(key).stat()) for key in keys}
    if after != before or inventory._directory_preflight_hints(cache) != hints:
        raise ValueError("selected source or parent directory changed during trials")
    if code_before != {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}:
        raise ValueError("benchmark or candidate code changed during trials")
    return {
        "schema_version": 1, "state": "verified", "read_only": True,
        "candidate_status": "rejected_rolled_back", "production_enabled": False,
        "scope": "bounded real file-metadata probe with private-index fault injection",
        "production_change_latency_claim": False,
        "source_cache": str(cache_path), "source_cache_sha256": hashlib.sha256(body).hexdigest(),
        "source_revision": frozen["feature_revision"], "sample_paths": keys,
        "path_prefix": path_prefix, "sample_count": limit, "sample_seed": 20260927,
        "sample_contract_sha256": contract, "source_identity_unchanged": True,
        "directory_hints": len(hints), "trials": trials,
        "hint_build_trials": hint_build_trials,
        "summary": {
            scenario: {variant: statistics.median(
                trial["wall_ms"] for trial in trials
                if trial["scenario"] == scenario and trial["variant"] == variant
            ) for variant in ("legacy", "directory_hints")}
            for scenario in ("unchanged", "injected_stale_directory")
        },
        "limitations": [
            "No complete producer run or full inventory/feature rows were timed.",
            "Fault injection changes only the private index; no live data is mutated.",
            "Discovery is supplied from the fixed sample and excluded from timing.",
            "OS cache is uncontrolled; trials are one process and not source-cold.",
            "The producer may publish a later source-cache generation after our stable read.",
        ],
        "source_sha256": code_before,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=REPO_ROOT / "artifacts/live/data_monitor/record_inventory_cache.json")
    parser.add_argument("--path-prefix", default=str(REPO_ROOT / "data_yahoo") + "/")
    parser.add_argument("--limit", type=int, default=4096)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(REPO_ROOT / "artifacts/benchmarks"):
        raise ValueError("receipt output must be under artifacts/benchmarks")
    result = benchmark(args.cache, path_prefix=args.path_prefix, limit=args.limit)
    result["command"] = [sys.executable, *sys.argv]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"state": result["state"], "summary": result["summary"], "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
