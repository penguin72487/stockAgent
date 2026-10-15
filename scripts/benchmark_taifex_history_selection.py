"""Complete seven-range curve workflow: canonical windows vs grouped keys.

Use an already frozen, SHA-verified engineering fixture, never mutable ledgers.
Both variants reuse the same verified physical column cache. No source writes,
broker/model calls, API requests, publication or source removal occur here.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
from contextlib import nullcontext
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_selection as selection
from stockagent.live import taifex_volatility_dashboard as dashboard


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fixture_proof(fixture):
    meta = json.loads((fixture / "fixture.json").read_bytes())
    proofs = {}
    for row in meta["proofs"]:
        path = fixture / "state" / Path(row["source"]).relative_to(meta["source"])
        with path.open("rb") as stream:
            if path.name == "marks.jsonl" and path.parent == fixture / "state":
                descriptor = stream.fileno()
                size = row["prefix_bytes"]
                if cache._prefix_digest(descriptor, size) != row["sha256"]:
                    raise ValueError("frozen real source prefix mismatch")
                stream.seek(size)
                tail = stream.read()
                if (len(tail) != meta["captured_tail_bytes"]
                        or hashlib.sha256(tail).hexdigest() != meta["captured_tail_sha256"]):
                    raise ValueError("frozen real captured tail mismatch")
            elif sha(path) != row["sha256"]:
                raise ValueError("frozen source fixture changed")
        proofs[str(path)] = sha(path)
    return proofs


def worker(fixture, variant):
    meta = json.loads((fixture / "fixture.json").read_bytes())
    os.environ[cache.CACHE_ENV] = str(fixture / "columns")
    os.environ[cache.ENABLE_ENV] = "1"
    start, cpu = time.perf_counter(), time.process_time()
    hashes, timings = {}, []
    for range_key in dashboard.HISTORY_RANGE_SECONDS:
        began, began_cpu = time.perf_counter(), time.process_time()
        # The production selector stays canonical. Only this SHA-verified,
        # frozen fixture substitutes the rejected experimental selector.
        context = patch.object(selection, "select_window_rows", selection.select_grouped_rows) \
            if variant == "grouped" else nullcontext()
        with context:
            result = dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state",
                now=datetime.fromisoformat(meta["clock"]), range_key=range_key)
        wall, cpu_seconds = time.perf_counter()-began, time.process_time()-began_cpu
        hashes[range_key] = hashlib.sha256(json.dumps(result, sort_keys=True, allow_nan=False,
            separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        timings.append({"range": range_key, "wall_seconds": wall, "cpu_seconds": cpu_seconds,
                        "rows": len(result["history"])})
    return {"variant": variant, "wall_seconds": time.perf_counter()-start,
        "cpu_seconds": time.process_time()-cpu, "ranges": timings, "output_sha256": hashes,
        "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        "provider_calls": 0, "production_writes": False}


def benchmark(fixture, output, repeats, control=None):
    if output.exists():
        raise FileExistsError("use a new evidence path")
    paths = [Path(__file__), Path(cache.__file__), Path(selection.__file__), Path(dashboard.__file__)]
    code = {str(path): sha(path) for path in paths}
    before = fixture_proof(fixture)
    prior = json.loads(control.read_bytes()) if control is not None else None
    if prior is not None and Path(prior["source_fixture"]).resolve() != fixture.resolve():
        raise ValueError("independent raw control must use this exact frozen fixture")
    trials = []
    for _ in range(repeats):
        for variant in ("window", "grouped", "grouped", "window"):
            run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", variant,
                "--fixture", str(fixture)], text=True, capture_output=True, check=True)
            row = json.loads(run.stdout.splitlines()[-1])
            trials.append(row)
            print(json.dumps(row), flush=True)
    if any(row["output_sha256"] != trials[0]["output_sha256"] for row in trials):
        raise RuntimeError("complete seven-range outputs differ")
    if prior is not None and any(row["output_sha256"]["1d"] != prior["trials"][0]["output_sha256"]
                                 for row in trials):
        raise RuntimeError("refactored selector differs from independent original raw control")
    if before != fixture_proof(fixture) or code != {str(path): sha(path) for path in paths}:
        raise RuntimeError("source or implementation changed during measurement")
    medians = {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
        for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
        for variant in ("window", "grouped")}
    result = {"contract": "taifex_seven_range_complete_selection_comparison_v1",
        "state": "measurement_complete", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "all_seven_complete_curves_in_one_resident_worker_not_host_or_broker",
        "exact_output_parity": True, "code_stable": True, "source_unchanged": True,
        "fixture": str(fixture), "source_sha256": before, "code_sha256": code,
        "medians": medians, "trials": trials, "provider_calls": 0, "production_writes": False}
    if control is not None:
        result["independent_raw_control"] = {"path": str(control), "sha256": sha(control),
                                             "same_fixture_1d_parity": True}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeats", type=int, default=2, choices=range(1, 4))
    parser.add_argument("--worker", choices=("window", "grouped"))
    parser.add_argument("--control", type=Path, help="Preserved independent original raw 1d control")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
    elif args.output:
        print(json.dumps(benchmark(args.fixture, args.output, args.repeats, args.control)["medians"]))
    else:
        parser.error("--output is required")


if __name__ == "__main__":
    main()
