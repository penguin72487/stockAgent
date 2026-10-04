#!/usr/bin/env python3
"""ABBA replay of the FULL frozen cache's metadata namespace, not producer wall.

No Parquet/provider/service action or production writes. File identities are
replayed in memory; mutations affect only that private replay. Every unhinted
path remains subject to the complete canonical probe. This proves decisions
and redundant metadata work, not cold I/O, source freshness or end-to-end gain.
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
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live import data_monitor_inventory as inventory  # noqa: E402


def _frozen_cache(path: Path) -> tuple[bytes, dict]:
    before = inventory._file_identity(path.stat())
    with path.open("rb") as stream:
        if inventory._file_identity(os.fstat(stream.fileno())) != before:
            raise ValueError("cache changed before read")
        body = stream.read()
        if inventory._file_identity(os.fstat(stream.fileno())) != before:
            raise ValueError("cache changed during read")
    if inventory._file_identity(path.stat()) != before:
        raise ValueError("cache changed after read")

    def reject_nonfinite(value: str) -> None:
        raise ValueError(f"non-finite inventory JSON: {value}")

    cache = json.loads(body, parse_constant=reject_nonfinite)
    if (
        not isinstance(cache, dict) or cache.get("version") != inventory.INVENTORY_VERSION
        or not isinstance(cache.get("files"), dict)
        or not isinstance(cache.get("feature_revision"), str)
        or len(cache["feature_revision"]) != 32
        or any(
            not isinstance(key, str) or not isinstance(entry, dict)
            or not isinstance(entry.get("file_identity"), list)
            or len(entry["file_identity"]) != 5
            or any(type(value) is not int or not 0 <= value < 2**64 for value in entry["file_identity"])
            for key, entry in cache["files"].items()
        )
    ):
        raise ValueError("cache lacks a complete native five-part identity namespace")
    return body, cache


def benchmark(cache_path: Path) -> dict:
    code_paths = (Path(__file__).resolve(), Path(inventory.__file__).resolve())
    code_sha256 = {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in code_paths}
    body, frozen = _frozen_cache(cache_path)
    files = frozen["files"]
    identities = {key: entry["file_identity"] for key, entry in files.items()}
    selected = {"frozen-full-cache-namespace": [Path(key) for key in files]}
    fingerprint = inventory._selection_fingerprint(selected, cache=files)
    if not files or fingerprint is None:
        raise ValueError("empty or invalid full metadata namespace")
    real_fingerprint = inventory._selection_fingerprint
    real_stat = Path.stat
    hint_keys = {}
    hint_build_trials = []
    recent_pool = frozen.get("recently_refreshed_files", [])
    if not isinstance(recent_pool, list) or any(not isinstance(key, str) for key in recent_pool):
        raise ValueError("invalid producer refreshed-file pool")
    recent_pool = [key for key in recent_pool if key in files]
    for variant in ("uniform", "refreshed", "refreshed", "uniform"):
        started, cpu_started = time.perf_counter(), time.process_time()
        keys = (
            random.Random(frozen["feature_revision"]).sample(
                tuple(files), min(inventory.FAST_INDEX_PREFLIGHT_HINTS, len(files)),
            ) if variant == "uniform" else inventory._preflight_keys(
                files, recently_refreshed=recent_pool, seed=frozen["feature_revision"],
            )
        )
        hint_build_trials.append({
            "variant": variant, "wall_ms": (time.perf_counter() - started) * 1_000,
            "cpu_ms": (time.process_time() - cpu_started) * 1_000,
        })
        hint_keys[variant] = keys
    uniform_set, recent_set = set(hint_keys["uniform"]), set(hint_keys["refreshed"])
    pool_set = set(recent_pool)
    recent_only = [key for key in hint_keys["refreshed"] if key not in uniform_set and key in pool_set]
    all_hints = recent_set | uniform_set
    unhinted = [key for key in files if key not in all_hints]
    scenarios = {"unchanged": None}
    if recent_only:
        scenarios["injected_refreshed_path_change"] = recent_only[0]
    if unhinted:
        scenarios["injected_unhinted_path_change"] = unhinted[0]
    trials = []
    with tempfile.TemporaryDirectory(prefix="inventory-hint-policy-") as temporary:
        directory = Path(temporary)
        private_cache = directory / "record_inventory_cache.json"
        private_cache.write_text("{}\n", encoding="utf-8")
        index_path = inventory._quick_index_path(private_cache)
        for scenario, changed_key in scenarios.items():
            outputs = []
            observed = dict(identities)
            if changed_key is not None:
                changed_identity = list(observed[changed_key])
                changed_identity[4] += 1
                observed[changed_key] = changed_identity
            stats = {
                key: SimpleNamespace(st_dev=value[0], st_ino=value[1], st_size=value[2],
                                     st_mtime_ns=value[3], st_ctime_ns=value[4])
                for key, value in observed.items()
            }
            for variant in ("uniform", "refreshed", "refreshed", "uniform"):
                index = {
                    "version": inventory.FAST_INDEX_VERSION,
                    "inventory_version": inventory.INVENTORY_VERSION,
                    "cache_signature": inventory._cache_signature(private_cache),
                    "selection_fingerprint": fingerprint, "datasets": {},
                    "cached_files": len(files), "identity_unbound_files": 0,
                    "feature_revision": frozen["feature_revision"],
                    # This is a frozen metadata replay, not a production
                    # source-observation proof or permission to skip stats.
                    "source_observation_root": fingerprint[:32],
                    "source_observation_matches_cache": True,
                    "preflight_file_hints": [[key, identities[key]] for key in hint_keys[variant]],
                }
                index["checksum"] = inventory._quick_index_checksum(index)
                index_path.write_text(json.dumps(index), encoding="utf-8")
                index_path.chmod(0o600)
                calls = scans = 0

                def frozen_stat(path: Path, *args, **kwargs):
                    nonlocal calls
                    result = stats.get(str(path))
                    if result is not None:
                        calls += 1
                        return result
                    return real_stat(path, *args, **kwargs)

                def counted_fingerprint(paths, *, cache=None):
                    nonlocal scans
                    if cache is None:
                        scans += 1
                    return real_fingerprint(paths, cache=cache)

                with patch.object(Path, "stat", frozen_stat), patch.object(
                    inventory, "_selected_files", return_value=selected,
                ), patch.object(inventory, "_selection_fingerprint", counted_fingerprint):
                    started, cpu_started = time.perf_counter(), time.process_time()
                    result = inventory._read_quick_index(directory, private_cache, snapshot=None)
                    wall_ms = (time.perf_counter() - started) * 1_000
                    cpu_ms = (time.process_time() - cpu_started) * 1_000
                projection = (
                    {key: value for key, value in result.items() if key != "timing_ms"}
                    if result is not None else None
                )
                outputs.append(projection)
                expected_scans = int(changed_key is None or changed_key not in hint_keys[variant])
                if scans != expected_scans or (result is None) != (changed_key is not None):
                    raise ValueError("canonical full-scan/reject decision changed")
                trials.append({
                    "scenario": scenario, "variant": variant, "wall_ms": wall_ms, "cpu_ms": cpu_ms,
                    "full_scans": scans, "metadata_source_calls": calls,
                    "result_sha256": hashlib.sha256(json.dumps(
                        projection, sort_keys=True, separators=(",", ":"),
                    ).encode()).hexdigest(),
                })
            if any(output != outputs[0] for output in outputs):
                raise ValueError("ABBA output parity failed")
    if code_sha256 != {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in code_paths}:
        raise ValueError("measured code changed during trials")
    return {
        "schema_version": 2, "state": "verified", "source_sha256": hashlib.sha256(body).hexdigest(),
        "source_revision": frozen["feature_revision"], "full_cache_paths": len(files),
        "hints": len(hint_keys["refreshed"]), "code_sha256": code_sha256,
        "producer_recent_pool_paths": len(set(recent_pool)),
        "candidate_policy": "refreshed-priority-uniform-v1",
        "read_only": True, "production_writes": False, "provider_calls": False,
        "scope": "full frozen five-part metadata namespace; private in-memory fault injection",
        "complete_producer_wall_claim": False, "live_source_freshness_claim": False,
        "hint_build_trials": hint_build_trials, "trials": trials,
        "summary": {
            scenario: {
                variant: {
                    "wall_ms": statistics.median(t["wall_ms"] for t in trials
                                                if t["scenario"] == scenario and t["variant"] == variant),
                    "metadata_source_calls": statistics.median(t["metadata_source_calls"] for t in trials
                                                               if t["scenario"] == scenario and t["variant"] == variant),
                } for variant in hint_keys
            } for scenario in scenarios
        },
        "limitations": [
            "All cache paths are replayed; no directory discovery, footer read, provider or source-cold I/O.",
            "The refreshed-change case explicitly chooses a producer-refreshed path absent from the uniform sample.",
            "Unhinted changes still trigger a complete source-identity probe in both policies.",
            "Every successful refresh still performs its canonical scan/footer/aggregate/publication workflow.",
            "A frozen metadata speedup is not the complete producer or website speedup.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=REPO_ROOT / "artifacts/live/data_monitor/record_inventory_cache.json")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = benchmark(args.cache)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"state": result["state"], "paths": result["full_cache_paths"],
                      "summary": result["summary"]}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
