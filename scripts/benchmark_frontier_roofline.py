#!/usr/bin/env python3
"""Measure usable device ceilings; these are bounds, not training speedups."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import torch
from downloader.artifact_io import atomic_write_json
from scripts.manage_gpu_jobs import _busy_gpu_indices, _gpu_lease_command


def event_ms(function, repeats=12):
    for _ in range(3):
        function()
    torch.cuda.synchronize()
    pairs = [(torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True))
        for _ in range(repeats)]
    for start, end in pairs:
        start.record(); function(); end.record()
    torch.cuda.synchronize()
    return statistics.median(start.elapsed_time(end) for start, end in pairs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--leased-worker", action="store_true")
    parser.add_argument("--affinity-map", help="semicolon-separated per-device CPU sets, matching the canonical DDP syntax")
    args = parser.parse_args()
    if not args.leased_worker:
        if _busy_gpu_indices():
            raise RuntimeError("another GPU owner is active")
        command = [sys.executable, str(Path(__file__).resolve()), "--output", str(args.output), "--leased-worker"]
        if args.affinity_map:
            command += ["--affinity-map", args.affinity_map]
        return subprocess.run(_gpu_lease_command(command, [0, 1], sharing=False), cwd=ROOT).returncode
    preflight = subprocess.run([sys.executable, str(ROOT / "scripts/check_environment.py"),
        "--require-cuda", "--minimum-cuda-devices", "2", "--strict"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if preflight.returncode:
        raise RuntimeError(preflight.stdout)
    torch.set_num_threads(4)
    record = {"contract": "measured_device_roofline_v1", "torch": torch.__version__,
        "cuda": torch.version.cuda, "gpus": [], "claims_epoch_optimum": False,
        "peer_access": [[bool(torch.cuda.can_device_access_peer(a, b)) if a != b else True
            for b in range(2)] for a in range(2)]}
    for device in range(2):
        if args.affinity_map:
            from train import _parse_cpu_affinity
            specs = args.affinity_map.split(";")
            if len(specs) != 2:
                raise ValueError("the dual-device probe requires two CPU sets")
            os.sched_setaffinity(0, _parse_cpu_affinity(specs[device]))
        torch.cuda.set_device(device)
        count = 256 * 1024**2
        source = torch.empty(count, device="cuda", dtype=torch.float32).fill_(1.)
        destination = torch.empty_like(source)
        copy_ms = event_ms(lambda dst=destination, src=source: dst.copy_(src))
        host = torch.empty(count, pin_memory=True).fill_(1.)
        h2d_ms = event_ms(lambda dst=destination, src=host: dst.copy_(src, non_blocking=True), repeats=6)
        measurements = {"index": device, "name": torch.cuda.get_device_name(device),
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "capability": list(torch.cuda.get_device_capability(device)),
            "copy_bytes": source.numel() * source.element_size(),
            "device_copy_ms": copy_ms, "device_read_write_gb_s": 2 * source.numel() * 4 / copy_ms / 1e6,
            "pinned_h2d_ms": h2d_ms, "pinned_h2d_gb_s": source.numel() * 4 / h2d_ms / 1e6,
            "gemm": {}}
        del source, destination, host
        for dtype, n in ((torch.bfloat16, 8192), (torch.float32, 4096), (torch.float64, 1024)):
            torch.backends.cuda.matmul.allow_tf32 = dtype == torch.float32
            a = torch.randn(n, n, device="cuda", dtype=dtype)
            b = torch.randn_like(a); c = torch.empty_like(a)
            ms = event_ms(lambda left=a, right=b, out=c: torch.mm(left, right, out=out))
            measurements["gemm"][str(dtype)] = {"n": n, "ms": ms,
                "tflop_s": 2 * n**3 / ms / 1e9, "tf32_allowed": dtype == torch.float32}
            del a, b, c
        record["gpus"].append(measurements)
        torch.cuda.empty_cache()
    atomic_write_json(args.output, record)
    print(json.dumps(record), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
