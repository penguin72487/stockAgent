"""Measure complete seven-range resident workflows in fresh thread pools.

No live source, source/ledger mutations, model calls or API requests. Supply an
explicit ordered trial sequence; an ABBA sequence is recorded, not inferred
from a convenient sorted median. Never change global acquisition/training pools.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json
from scripts import benchmark_taifex_history_selection as workflow


def benchmark(fixture, control, threads, output):
    if output.exists():
        raise FileExistsError("never overwrite prior measurements")
    prior = json.loads(control.read_bytes())
    if (prior.get("exact_output_parity") is not True
            or Path(prior["fixture"]).resolve() != fixture.resolve()):
        raise ValueError("independent seven-range control must use this frozen source")
    paths = [Path(__file__), Path(workflow.__file__), Path(workflow.cache.__file__),
             Path(workflow.selection.__file__), Path(workflow.dashboard.__file__)]
    code = {str(path): workflow.sha(path) for path in paths}
    before = workflow.fixture_proof(fixture)
    expected = prior["trials"][0]["output_sha256"]
    rows = []
    for count in threads:
        env = {**os.environ, "POLARS_MAX_THREADS": str(count)}
        run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker",
            "--fixture", str(fixture)], env=env, capture_output=True, text=True, check=True)
        row = json.loads(run.stdout.splitlines()[-1])
        if row["threads"] != count or row["output_sha256"] != expected:
            raise RuntimeError("actual thread pool or complete control output mismatch")
        rows.append(row)
        print(json.dumps(row), flush=True)
    if before != workflow.fixture_proof(fixture) or code != {
        str(path): workflow.sha(path) for path in paths
    }:
        raise RuntimeError("frozen source or benchmark implementation changed")
    medians = {str(count): {key: statistics.median(row[key] for row in rows if row["threads"] == count)
        for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
        for count in dict.fromkeys(threads)}
    balanced = len(threads) >= 8 and len(threads) % 4 == 0 and all(
        threads[i] == threads[i+3] and threads[i+1] == threads[i+2]
        and threads[i] != threads[i+1] for i in range(0, len(threads), 4)
    ) and all(threads[i:i+4] == threads[:4] for i in range(0, len(threads), 4))
    result = {"contract": "taifex_complete_seven_curve_thread_comparison_v1",
        "state": "measurement_complete", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "complete_seven_range_resident_workflow_not_api_host_capture_or_training",
        "trial_order": threads, "balanced_abba": balanced, "medians": medians, "trials": rows,
        "fixture": str(fixture), "source_sha256": before, "code_sha256": code,
        "source_unchanged": True, "code_stable": True, "exact_output_parity": True,
        "independent_control": {"path": str(control), "sha256": workflow.sha(control)},
        "provider_calls": 0, "model_forward_calls": 0, "production_writes": False}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--threads", nargs="+", type=int, choices=range(1, 65))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    if args.worker:
        result = workflow.worker(args.fixture, "window")
        result["threads"] = pl.thread_pool_size()
        print(json.dumps(result))
    elif args.control and args.threads and args.output:
        result = benchmark(args.fixture, args.control, args.threads, args.output)
        print(json.dumps({"medians": result["medians"], "balanced_abba": result["balanced_abba"]}))
    else:
        parser.error("--control --threads --output are required")


if __name__ == "__main__":
    main()
