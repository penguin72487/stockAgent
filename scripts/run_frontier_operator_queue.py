#!/usr/bin/env python3
"""Run isolated operator trials sequentially under the canonical GPU lease."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.manage_gpu_jobs import _gpu_lease_command
from scripts.benchmark_tw_day_trade_batch_sizes import _wait_for_idle_gpus
from scripts.benchmark_frontier_kernels import CASES
from stockagent.runtime_identity import runtime_identity, verify_source_release, verify_release_bundles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--after-exit", type=Path)
    parser.add_argument("--cases", help="comma-separated targeted cases; defaults to the full inventory")
    for dimension in ("rows", "width", "output-width", "query-steps", "key-steps"):
        parser.add_argument(f"--{dimension}", type=int)
    parser.add_argument("--leased-worker", action="store_true")
    args = parser.parse_args()
    if not args.leased_worker:
        if args.after_exit:
            while not args.after_exit.exists():
                time.sleep(10)
        _wait_for_idle_gpus(wait=True, poll_s=2.0)
        command = [sys.executable, str(Path(__file__).resolve()), "--output", str(args.output),
            "--leased-worker"]
        if args.cases:
            command += ["--cases", args.cases]
        for dimension in ("rows", "width", "output_width", "query_steps", "key_steps"):
            if getattr(args, dimension) is not None:
                command += ["--" + dimension.replace("_", "-"), str(getattr(args, dimension))]
        return subprocess.run(_gpu_lease_command(command, [0, 1], sharing=False),
            cwd=ROOT, env=os.environ.copy()).returncode
    args.output.mkdir(parents=True, exist_ok=False)
    preflight = subprocess.run([sys.executable, str(ROOT / "scripts/check_environment.py"),
        "--require-cuda", "--minimum-cuda-devices", "2", "--strict"],
        cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (args.output / "cuda-preflight.log").write_text(preflight.stdout)
    if preflight.returncode:
        raise RuntimeError("strict dual-GPU preflight failed")
    atomic_write_json(args.output / "runtime.json", runtime_identity())
    receipt = ROOT.parent / "release.json"
    if receipt.exists():
        verify_release_bundles(receipt)
        atomic_write_json(args.output / "source.json", verify_source_release(receipt, ROOT))
    trials = []
    selected_cases = args.cases.split(",") if args.cases else CASES
    if any(case not in CASES for case in selected_cases):
        raise ValueError("unknown operator case")
    for case in selected_cases:
        for dtype in (["bf16"] if case.startswith("adamw") else ["fp32", "bf16"]):
            name = f"{case}-{dtype}"
            result_path = args.output / f"{name}.json"
            command = [sys.executable, str(ROOT / "scripts/benchmark_frontier_kernels.py"),
                "--case", case, "--dtype", dtype, "--output", str(result_path)]
            if case.startswith("attention"):
                # The selected model has narrow 8-wide heads and short/last
                # query attention, rather than long-sequence LLM attention.
                command += ["--rows", "32", "--width", "8"]
            elif case.startswith("swiglu"):
                command += ["--width", "128"]
            for dimension in ("rows", "width", "output_width", "query_steps", "key_steps"):
                if getattr(args, dimension) is not None:
                    command += ["--" + dimension.replace("_", "-"), str(getattr(args, dimension))]
            started = time.perf_counter()
            with (args.output / f"{name}.log").open("w") as log:
                try:
                    result = subprocess.run(command, cwd=ROOT, stdout=log,
                        stderr=subprocess.STDOUT, timeout=240)
                    returncode = result.returncode
                except subprocess.TimeoutExpired:
                    returncode = 124
                    atomic_write_json(result_path, {"status": "rejected", "case": case,
                        "dtype": dtype, "error": "isolated trial exceeded 240 seconds"})
            trials.append({"name": name, "returncode": returncode,
                "wall_s": time.perf_counter() - started, "result": str(result_path)})
            atomic_write_json(args.output / "queue-progress.json", trials)
            print(json.dumps(trials[-1]), flush=True)
    atomic_write_json(args.output / "queue-exit.json", {"completed": len(trials),
        "measured": sum(row["returncode"] == 0 for row in trials), "trials": trials})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
