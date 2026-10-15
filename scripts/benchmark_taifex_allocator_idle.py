"""Complete curve computation, real idle RSS, then an exact warm readback.

Only private SHA-verified fixtures and disposable physical projection caches;
no source, broker/model, public API, global allocator or service mutations.
Thread count stays at the accepted role-specific four for both variants.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json
from scripts import benchmark_taifex_history_selection as workflow

DECAY_CONF = "muzzy_decay_ms:1000"


def current_rss():
    with Path("/proc/self/statm").open() as stream:
        return int(stream.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def worker(fixture, variant):
    result = workflow.worker(fixture, "window")
    result.update(allocator_variant=variant, threads=pl.thread_pool_size(),
                  allocator_conf=os.environ.get("_RJEM_MALLOC_CONF"), rss_after_compute=current_rss())
    began, cpu = time.perf_counter(), time.process_time()
    gc.collect()  # Same bounded collection in both variants, included in idle cost.
    idle = [{"elapsed_seconds": time.perf_counter()-began, "rss_bytes": current_rss()}]
    for target in (1., 3., 10.):
        time.sleep(max(0., target - (time.perf_counter()-began)))
        idle.append({"elapsed_seconds": time.perf_counter()-began, "rss_bytes": current_rss()})
    result.update(idle_samples=idle, idle_cpu_seconds=time.process_time()-cpu,
                  idle_rss_bytes=idle[-1]["rss_bytes"])
    meta = json.loads((fixture / "fixture.json").read_bytes())
    began, cpu = time.perf_counter(), time.process_time()
    snapshot = workflow.dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state",
        now=datetime.fromisoformat(meta["clock"]), range_key="1d")
    warm_wall, warm_cpu = time.perf_counter()-began, time.process_time()-cpu
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False,
        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if digest != result["output_sha256"]["1d"]:
        raise RuntimeError("idle reclamation changed the complete subsequent curve")
    result.update(warm_read_wall_seconds=warm_wall, warm_read_cpu_seconds=warm_cpu,
                  warm_read_exact_output_parity=True, rss_after_warm_read=current_rss())
    return result


def benchmark(fixture, control, output):
    if output.exists():
        raise FileExistsError("never overwrite prior evidence")
    prior = json.loads(control.read_bytes())
    if (not prior.get("exact_output_parity")
            or Path(prior["fixture"]).resolve() != fixture.resolve()):
        raise ValueError("independent control uses another fixture")
    expected = prior["trials"][0]["output_sha256"]
    paths = [Path(__file__), Path(workflow.__file__), Path(workflow.cache.__file__),
             Path(workflow.dashboard.__file__), Path(workflow.selection.__file__), Path(pl.__file__)]
    code = {str(path): workflow.sha(path) for path in paths}
    source = workflow.fixture_proof(fixture)
    inherited = os.environ.get("_RJEM_MALLOC_CONF", "")
    trials = []
    for variant in ("baseline", "decay", "decay", "baseline") * 2:
        env = {**os.environ, "POLARS_MAX_THREADS": "4"}
        env.pop("_RJEM_MALLOC_CONF", None)
        if variant == "decay":
            env["_RJEM_MALLOC_CONF"] = DECAY_CONF
        run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", variant,
            "--fixture", str(fixture)], env=env, check=True, text=True, capture_output=True)
        row = json.loads(run.stdout.splitlines()[-1])
        if (row["threads"] != 4 or row["output_sha256"] != expected
                or row["warm_read_exact_output_parity"] is not True
                or variant == "decay" and not row["allocator_conf"].endswith(DECAY_CONF)):
            raise RuntimeError("allocator, thread pool or complete output mismatch")
        trials.append(row)
        print(json.dumps(row), flush=True)
    if source != workflow.fixture_proof(fixture) or code != {
        str(path): workflow.sha(path) for path in paths
    }:
        raise RuntimeError("frozen source or code changed")
    metrics = ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "idle_rss_bytes",
               "idle_cpu_seconds", "warm_read_wall_seconds", "warm_read_cpu_seconds")
    medians = {variant: {key: statistics.median(row[key] for row in trials
        if row["allocator_variant"] == variant) for key in metrics}
        for variant in ("baseline", "decay")}
    result = {"contract": "taifex_complete_curve_allocator_idle_comparison_v1",
        "state": "measurement_complete", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "seven_complete_curves_then_ten_seconds_idle_and_exact_warm_1d_read_not_host_or_training",
        "fixture": str(fixture), "source_sha256": source, "code_sha256": code,
        "polars_version": pl.__version__, "decay_conf": DECAY_CONF,
        "controller_inherited_allocator_override_present": bool(inherited),
        "trial_order": [row["allocator_variant"] for row in trials], "balanced_abba": True,
        "medians": medians, "trials": trials, "exact_output_parity": True, "source_unchanged": True,
        "code_stable": True, "provider_calls": 0, "model_forward_calls": 0, "production_writes": False,
        "independent_control": {"path": str(control), "sha256": workflow.sha(control)}}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", choices=("baseline", "decay"))
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
    elif args.control and args.output:
        print(json.dumps(benchmark(args.fixture, args.control, args.output)["medians"]))
    else:
        parser.error("--control --output are required")


if __name__ == "__main__":
    main()
