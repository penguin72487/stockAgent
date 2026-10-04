#!/usr/bin/env python3
"""Matched full-namespace record-cache replay, including validation and writes.

Real source stat/absence observations are frozen. No provider request, source
write, Parquet scan, service restart or promotion occurs. Complete record
decode, source identity scan, aggregation, cache persistence and quick-index
publication are included. Import/discovery/cold source I/O and UI are excluded.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time
from unittest.mock import patch
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.benchmark_data_monitor_projection_pipeline import capture  # noqa: E402
from stockagent import private_json  # noqa: E402
from stockagent.live import data_monitor_inventory as inventory  # noqa: E402
from stockagent.runtime_identity import identity_sha256  # noqa: E402


def benchmark(root: Path, *, repeats: int = 2) -> dict:
    generation = capture(root)
    original_stat = Path.stat
    body = json.dumps(generation["payload"], separators=(",", ":")).encode()
    native_loads, native_dumps = private_json.loads, private_json.dumps
    trials = []
    outputs = []
    output_bytes_hashes = []
    with tempfile.TemporaryDirectory(prefix="stockagent-record-codec-") as directory:
        for repeat in range(repeats):
            for index, variant in enumerate(("stdlib", "native", "native", "stdlib")):
                trial_root = Path(directory) / f"{repeat}-{index}"
                cache = trial_root / "artifacts/live/data_monitor/record_inventory_cache.json"
                cache.parent.mkdir(parents=True)
                cache.write_bytes(body)
                stat_calls = 0

                def frozen_stat(path, *args, **kwargs):
                    nonlocal stat_calls
                    key = str(path)
                    if key in generation["source_stats"]:
                        stat_calls += 1
                        value = generation["source_stats"][key]
                        if value is None:
                            raise FileNotFoundError(key)
                        return value
                    return original_stat(path, *args, **kwargs)

                loads = json.loads if variant == "stdlib" else native_loads
                dumps = (lambda value: json.dumps(value, separators=(",", ":")).encode()) if variant == "stdlib" else native_dumps
                gc.collect()
                frame = inventory.InventorySnapshot(trial_root)
                with patch.object(Path, "stat", frozen_stat), \
                     patch.object(inventory, "_selected_files", return_value=generation["selected"]), \
                     patch.object(inventory, "uuid4", return_value=UUID(int=1)), \
                     patch.object(private_json, "loads", loads), \
                     patch.object(private_json, "dumps", dumps):
                    started = time.perf_counter()
                    cpu_started = time.process_time()
                    result = inventory.build_record_inventory(
                        trial_root, refresh=True, max_refresh_files=0, snapshot=frame,
                    )
                    wall_ms = (time.perf_counter()-started)*1000
                    cpu_ms = (time.process_time()-cpu_started)*1000
                persisted = cache.read_bytes()
                output = json.loads(persisted)
                output_bytes_hashes.append(hashlib.sha256(persisted).hexdigest())
                outputs.append(identity_sha256(output))
                trials.append({"variant": variant, "wall_ms": round(wall_ms, 3),
                               "cpu_ms": round(cpu_ms, 3),
                               "source_stat_calls": stat_calls, "cached_files": result["cached_files"],
                               "source_observation_root": result["source_observation_root"],
                               "source_observation_matches_cache": result["source_observation_matches_cache"],
                               "output_bytes": cache.stat().st_size, "output_semantic_sha256": outputs[-1],
                               "timing_ms": result["timing_ms"]})
                del frame, output, result
    parity = len(set(outputs)) == 1
    bytes_parity = len(set(output_bytes_hashes)) == 1
    medians = {name: statistics.median(t["wall_ms"] for t in trials if t["variant"] == name)
               for name in ("stdlib", "native")}
    return {"schema_version": 1, "state": "verified" if parity and bytes_parity else "mismatch",
            "scope": "full record cache decode, frozen-source identity checks, aggregation and atomic cache/index writes",
            "excludes": ["imports", "directory_discovery", "live_or_cold_source_io", "Parquet_footer_refresh",
                         "feature_publisher", "provider_calls", "network", "browser"],
            "input_sha256": generation["sha256"], "input_bytes": len(body),
            "native_available": private_json.native_available(),
            "native_backend": "msgspec-decode_stdlib-encode", "native_version": private_json._native.__version__,
            "production_source_writes": 0,
            "parity": parity, "cache_bytes_parity": bytes_parity, "median_ms": medians,
            "speedup": round(medians["stdlib"] / medians["native"], 4),
            "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "rss_scope": "whole ABBA process high-water mark; not per-variant peak",
            "trials": trials}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-package-path", type=Path)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    if args.native_package_path:
        sys.path.insert(0, str(args.native_package_path.resolve()))
        import msgspec
        private_json._native = msgspec
    if not private_json.native_available():
        parser.error("native candidate must be available; install or pass --native-package-path")
    result = benchmark(REPO_ROOT, repeats=args.repeats)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("state", "parity", "input_bytes", "median_ms", "speedup")}))
    return 0 if result["state"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
