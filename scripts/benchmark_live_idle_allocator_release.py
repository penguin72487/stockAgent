"""Fresh-process ABBA of actual idle release, not a financial-model benchmark.

The saved original release function is restored mechanically. Each worker uses
256 MiB of inactive pinned staging memory, 64 MiB of device scratch, a small
model, and a still-active pinned sentinel. No provider, source data, signals,
orders or service state are changed. CUDA is required and compute-verified.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack
from datetime import UTC, datetime
import gc
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.canonical_live_tail_cache_oracle import (
    _function_namespace, original_callers_identity,
)

MIB = 1024**2
SOURCES = (Path(__file__), ROOT / "stockagent/live/signal_engine.py",
           ROOT / "scripts/canonical_live_tail_cache_oracle.py",
           ROOT / "scripts/check_environment.py")


def sources():
    return {str(path): sha256_file(path) for path in SOURCES}


def tensor_digest(value):
    return hashlib.sha256(value.detach().cpu().numpy().tobytes()).hexdigest()


def sample(torch):
    stats = torch.cuda.host_memory_stats()
    return {"rss_bytes": int(Path("/proc/self/statm").read_text().split()[1])
            * os.sysconf("SC_PAGE_SIZE"),
            "host_allocated_bytes": stats["allocated_bytes.current"],
            "host_active_bytes": stats["active_bytes.current"],
            "host_num_alloc": stats["num_host_alloc"],
            "host_num_free": stats["num_host_free"],
            "device_allocated_bytes": torch.cuda.memory_allocated(),
            "device_reserved_bytes": torch.cuda.memory_reserved()}


def worker(case, original):
    import torch
    from scripts.check_environment import _torch_cuda_report
    from stockagent.live import signal_engine as engine

    if torch.cuda.is_initialized():
        raise RuntimeError("worker must start CUDA cold")
    cuda, errors = _torch_cuda_report(torch, require_cuda=True)
    if errors or not cuda["cuda_compute_verified"]:
        raise RuntimeError(f"strict CUDA compute preflight failed: {errors}")
    identity = original_callers_identity(original)
    saved = Path(identity["proofs"][str(ROOT / "stockagent/live/signal_engine.py")]["path"])
    release = (engine.release_idle_live_cuda_cache if case == "optimized" else
               _function_namespace(saved, vars(engine), ("release_idle_live_cuda_cache",))[
                   "release_idle_live_cuda_cache"])
    baseline_ast = next(node for node in ast.parse(saved.read_bytes()).body
                        if isinstance(node, ast.FunctionDef)
                        and node.name == "release_idle_live_cuda_cache")
    baseline_digest = hashlib.sha256(ast.dump(baseline_ast).encode()).hexdigest()
    torch.manual_seed(19)
    model = torch.nn.Linear(32, 32).cuda().eval()
    with torch.inference_mode():
        output = model(torch.arange(32, dtype=torch.float32, device="cuda").unsqueeze(0)).cpu()
    engine._LIVE_MODEL_CACHE["isolated-allocator-probe"] = model
    del model
    sentinel = torch.full((MIB,), 23, dtype=torch.uint8, pin_memory=True)
    sentinel_hash = tensor_digest(sentinel)
    staging = torch.full((256 * MIB,), 37, dtype=torch.uint8, pin_memory=True)
    scratch = torch.empty((64 * MIB,), dtype=torch.uint8, device="cuda")
    scratch.copy_(staging[:scratch.numel()], non_blocking=True)
    torch.cuda.synchronize()
    if int(scratch[0]) != 37 or int(scratch[-1]) != 37:
        raise AssertionError("asynchronous staging copy was not correct")
    del staging, scratch
    gc.collect()
    torch.cuda.synchronize()
    # Synchronize completes the copy, but the host allocator may not retire
    # its queued events until the next same-size allocation. Exercise that
    # normal allocator path; do not mislabel queued DMA buffers as live leaks.
    after_copy = sample(torch)
    reuse = torch.empty((256 * MIB,), dtype=torch.uint8, pin_memory=True)
    del reuse
    gc.collect()
    before = sample(torch)
    print(json.dumps({"case": case, "after_copy": after_copy, "before_release": before}), flush=True)
    if before["host_allocated_bytes"] - before["host_active_bytes"] < 256 * MIB:
        raise AssertionError("probe did not reproduce an inactive pinned allocation")
    began, cpu = time.perf_counter(), time.process_time()
    if release() is not True:
        raise AssertionError("real cache release did not execute")
    wall, consumed = time.perf_counter() - began, time.process_time() - cpu
    after = sample(torch)
    if tensor_digest(sentinel) != sentinel_hash:
        raise AssertionError("release damaged a still-active pinned tensor")
    if after["host_active_bytes"] != before["host_active_bytes"]:
        raise AssertionError("release changed checked-out pinned memory")
    if after["device_allocated_bytes"] != 0:
        raise AssertionError("model cache still retains live device allocations")
    def forbidden(*_args, **_kwargs):
        raise AssertionError("second idle must not probe/synchronize/collect")
    with ExitStack() as stack:
        for name in ("is_initialized", "is_available", "init", "synchronize", "empty_cache"):
            stack.enter_context(patch.object(torch.cuda, name, forbidden))
        stack.enter_context(patch.object(torch.accelerator, "empty_host_cache", forbidden))
        stack.enter_context(patch.object(gc, "collect", forbidden))
        for _ in range(5):
            if release() is not False:
                raise AssertionError("empty idle should have no further work")
    return {"case": case, "cuda_preflight": cuda, "after_copy": after_copy,
            "before": before, "after": after,
            "release_wall_seconds": wall, "release_cpu_seconds": consumed,
            "model_output_sha256": tensor_digest(output), "sentinel_sha256": sentinel_hash,
            "saved_original_release_ast_sha256": baseline_digest,
            "repeat_idle_calls": 5, "repeat_cuda_calls": 0, "repeat_gc_calls": 0,
            "scope": "bounded real allocator mechanism, not full financial inference"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--original-callers", type=Path, required=True)
    parser.add_argument("--worker", choices=("baseline", "optimized"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    output = args.output.absolute()
    if (output.exists() or output.is_symlink()
            or not output.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve())):
        parser.error("new private benchmark receipt required")
    before = sources()
    original = original_callers_identity(args.original_callers)
    if args.worker:
        result = worker(args.worker, args.original_callers)
    else:
        folder = output.with_suffix("")
        folder.mkdir(parents=True, exist_ok=False)
        trials = []
        for index, case in enumerate(("baseline", "optimized", "optimized", "baseline")):
            trial = folder / f"{index}-{case}.json"
            subprocess.run([sys.executable, str(Path(__file__)), "--output", str(trial),
                            "--original-callers", str(args.original_callers), "--worker", case],
                           check=True, timeout=120)
            row = json.loads(trial.read_text())
            if row["sources"] != before or row["original_callers"] != original:
                raise RuntimeError("worker implementation or baseline snapshot changed")
            trials.append({**row, "receipt": str(trial), "receipt_sha256": sha256_file(trial)})
        if len({row["model_output_sha256"] for row in trials}) != 1:
            raise AssertionError("model outputs differ across the ABBA workers")
        for row in trials:
            inactive = row["after"]["host_allocated_bytes"] - row["after"]["host_active_bytes"]
            if row["case"] == "optimized" and inactive != 0:
                raise AssertionError("optimized release retained inactive pinned memory")
            if row["case"] == "baseline" and inactive < 256 * MIB:
                raise AssertionError("baseline did not retain the reproduced pinned working set")
        result = {"status": "accepted_bounded_allocator_mechanism", "trials": trials,
                  "financial_model_or_native_service_acceptance": False}
    if sources() != before or original_callers_identity(args.original_callers) != original:
        raise RuntimeError("source changed during benchmark; keep partial evidence, reject acceptance")
    atomic_write_json(output, {**result, "sources": before, "original_callers": original,
                              "observed_at_utc": datetime.now(UTC).isoformat(),
                              "provider_calls": 0, "services_changed": False,
                              "production_outputs_modified": False})
    print(json.dumps({"output": str(output), "status": result.get("status", args.worker)}), flush=True)


if __name__ == "__main__":
    main()
