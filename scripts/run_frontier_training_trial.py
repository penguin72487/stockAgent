#!/usr/bin/env python3
"""Receipt-bound frontier trials through the canonical complete-fold benchmark.

Each trial gets a fresh output root. Precision differences are research results;
they never authorize changing the execution/accounting contract or resuming a
formal optimizer. GPU admission and lifecycle acceptance remain shared owners.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

_bootstrap = argparse.ArgumentParser(add_help=False)
_bootstrap.add_argument("--code-root", type=Path)
_selection, _ = _bootstrap.parse_known_args()
ROOT = (_selection.code_root or Path(__file__).resolve().parents[1]).resolve()
sys.path.insert(0, str(ROOT))

import yaml
from downloader.artifact_io import atomic_write_json
from scripts.manage_gpu_jobs import _gpu_lease_command
from scripts.benchmark_tw_day_trade_batch_sizes import _wait_for_idle_gpus
from scripts.run_tw_scale_cash_annual_ablations import _environment
from stockagent.runtime_identity import runtime_identity, verify_source_release, verify_release_bundles


def merge_overrides(base: dict, overrides: dict) -> dict:
    """Merge declared experiment overrides without dropping inherited settings."""
    import copy
    out = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = merge_overrides(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--source-receipt", type=Path, required=True)
    parser.add_argument("--baseline-config", type=Path, required=True)
    parser.add_argument("--overrides", type=Path)
    parser.add_argument("--runtime-overrides", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--fold", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--compile-threads", type=int, default=16)
    parser.add_argument("--cpu-threads", type=int, default=8)
    parser.add_argument("--timeout-s", type=float, default=3600)
    parser.add_argument("--min-vram-headroom-gib", type=float, default=1.5)
    parser.add_argument("--max-peak-vram-fraction", type=float, default=0.96)
    parser.add_argument("--profile-timing", action="store_true", help="diagnostic CUDA stage events; compare equally when scoring")
    parser.add_argument("--nsys-trace", action="store_true", help="bounded diagnostic timeline; excluded from throughput ranking")
    args = parser.parse_args()
    if args.epochs < 4:
        parser.error("at least four complete epochs are required (steady epochs 3+)")
    if args.cpu_threads < 1 or args.compile_threads < 1:
        parser.error("CPU and compiler thread counts must be positive")
    if not 0 <= args.min_vram_headroom_gib < float("inf"):
        parser.error("minimum VRAM headroom must be finite and non-negative")
    if not 0 < args.max_peak_vram_fraction <= 1:
        parser.error("maximum peak VRAM fraction must be in (0, 1]")
    code = args.code_root.resolve()
    proof = verify_source_release(args.source_receipt.resolve(), code)
    verify_release_bundles(args.source_receipt.resolve())
    base = yaml.safe_load(args.baseline_config.read_text())
    overrides = yaml.safe_load(args.overrides.read_text()) if args.overrides else {}
    candidate = merge_overrides(base, overrides or {})
    # One workload and reporting policy; only the declared overrides vary.
    candidate["experiment_name"] = args.name
    candidate["runner"]["resume"] = False
    candidate["runner"]["output_dir"] = str(args.output.resolve() / "config-only")
    args.output.mkdir(parents=True, exist_ok=False)
    config = args.output.resolve() / "candidate.yaml"
    config.write_text(yaml.safe_dump(candidate, sort_keys=False))
    launch_path = args.baseline_config.parent / "runtime-launch.json"
    launch = json.loads(launch_path.read_text())
    acceptance = json.loads(Path(launch["acceptance_receipt"]).read_text())
    # Config alone is insufficient: use the canonical accepted launch's
    # rematerialization, certified-flat, transport and compiler environment.
    env = _environment({**launch, "source_receipt": str(args.source_receipt.resolve())}, acceptance, code)
    env["PYTHONUNBUFFERED"] = "1"
    runtime_overrides = yaml.safe_load(args.runtime_overrides.read_text()) if args.runtime_overrides else {}
    for key, value in (runtime_overrides or {}).items():
        if not key.startswith("STOCKAGENT_"):
            raise ValueError(f"unsupported trial environment variable {key!r}")
        env[key] = str(value)
    command = [sys.executable, str(code / "scripts/benchmark_tw_day_trade_batch_sizes.py"),
        "--python", sys.executable,
        "--project-root", str(code), "--config", str(config),
        "--output-root", str(args.output.resolve() / "full-fold"),
        "--batch-sizes", str(args.batch_size), "--batch-size-eval", "16",
        "--start-fold", str(args.fold), "--epochs", str(args.epochs),
        "--skip-epochs", "2", "--minimum-steady-epochs", str(args.epochs - 2),
        "--multi-gpu-strategy", "distributed_data_parallel",
        "--cuda-visible-devices", "0,1", "--cpu-threads", str(args.cpu_threads),
        "--compile-threads", str(args.compile_threads), "--polars-threads", "1",
        "--min-host-headroom-gib", "16", "--min-vram-headroom-gib", str(args.min_vram_headroom_gib),
        "--max-peak-vram-fraction", str(args.max_peak_vram_fraction), "--timeout-s", str(args.timeout_s),
        "--stop-after-oom"]
    if args.batch_size & (args.batch_size - 1):
        command.append("--allow-non-power-of-two")
    if args.profile_timing:
        command.append("--profile-timing")
    if args.nsys_trace:
        # The profiler must let the complete workflow finish after capture.
        # No CPU/perf sampling, privileged counters or process attachment.
        command = ["nsys", "profile", "--trace=cuda,nvtx,osrt", "--sample=none",
            "--cpuctxsw=none", "--delay=300", "--duration=120", "--kill=none",
            "--wait=all", "--force-overwrite=false", "--output", str(args.output.resolve() / "timeline"),
            *command]
    spec = {"contract": "frontier_complete_fold_trial_v1", "name": args.name,
        "source": proof, "runtime": runtime_identity(), "overrides": overrides,
        "baseline_config": str(args.baseline_config.resolve()), "command": command,
        "runtime_overrides": runtime_overrides or {},
        "runtime_environment": {key: value for key, value in env.items()
            if key.startswith("STOCKAGENT_DAY_TRADE_") or key in {
                "STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR", "STOCKAGENT_FEATURE_VERIFY_THREADS",
                "STOCKAGENT_DDP_CPU_AFFINITY"}},
        "numeric_policy": "numeric_and_gradient_differences_authorized_v1",
        "mechanism_policy": "canonical_causal_execution_accounting_and_lifecycle",
        "optimizer_donated": False, "formal_training_modified": False,
        "profile_timing": args.profile_timing, "nsys_trace": args.nsys_trace}
    spec["host_thread_counts"] = {"cpu": args.cpu_threads, "compiler": args.compile_threads, "polars": 1}
    spec["vram_admission_policy"] = {"min_headroom_gib": args.min_vram_headroom_gib,
        "max_peak_fraction": args.max_peak_vram_fraction}
    spec["package_environment"] = {key: env[key] for key in
        ("STOCKAGENT_FRONTIER_CUDNN_OVERLAY", "LD_PRELOAD", "LD_LIBRARY_PATH", "STOCKAGENT_CUDA_ROOT", "CUDA_HOME", "CXX", "MAX_JOBS") if key in env}
    atomic_write_json(args.output / "run-spec.json", spec)
    _wait_for_idle_gpus(wait=True, poll_s=2.0)
    with (args.output / "driver.log").open("w") as log:
        preflight = subprocess.run([sys.executable, str(code / "scripts/check_environment.py"),
            "--require-cuda", "--minimum-cuda-devices", "2", "--strict"],
            cwd=code, env=env, stdout=log, stderr=subprocess.STDOUT)
        if preflight.returncode:
            atomic_write_json(args.output / "exit.json", {"preflight_returncode": preflight.returncode})
            return preflight.returncode
        result = subprocess.run(_gpu_lease_command(command, [0, 1], sharing=False),
            cwd=code, env=env, stdout=log, stderr=subprocess.STDOUT)
    # A bounded Nsight capture may return while the un-killed target is still
    # completing its fold. Admission for the following trial must wait for the
    # actual CUDA owners, rather than treating the profiler's exit as completion.
    _wait_for_idle_gpus(wait=True, poll_s=2.0)
    if args.nsys_trace and result.returncode == 0:
        summary = args.output / "full-fold" / "summary.json"
        if not summary.exists():
            raise RuntimeError("profiler exited without a complete-fold receipt")
    atomic_write_json(args.output / "exit.json", {"returncode": result.returncode})
    print(json.dumps({"trial": args.name, "returncode": result.returncode,
        "output": str(args.output.resolve())}), flush=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
