"""Measure canonical live inference without quotes, publication or orders.

The first call has cold process/model caches, but may reuse persistent panels
and the OS page cache. Subsequent calls are warm. Neither proves live opening
latency. Full inputs, model auxiliaries and financial formatting are retained.
"""
from __future__ import annotations

import argparse
import cProfile
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
import platform
from pathlib import Path
import resource
import statistics
import sys
import time
import warnings
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import file_sha256
from stockagent.live import signal_engine
from stockagent.live.market_config import load_market_config
from stockagent.config import load_config
import torch


def contract_digest(result) -> str:
    """Use complete outputs, not a top-N sample or tolerance-based oracle."""
    value = {"weights": result.weights_rows, "decisions": result.decision_rows,
             "explanation": result.summary["model_explanation"]}
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False).encode()).hexdigest()


def forward_tensor_inventory(value, path: str = "output") -> list[dict]:
    """Logical owned-cache bytes, without reading tensor values or changing math."""
    if isinstance(value, torch.Tensor):
        return [{"path": path, "shape": list(value.shape), "dtype": str(value.dtype),
                 "bytes": value.numel() * value.element_size()}]
    if isinstance(value, dict):
        return [row for key, item in value.items()
                for row in forward_tensor_inventory(item, f"{path}.{key}")]
    if isinstance(value, (list, tuple)):
        return [row for index, item in enumerate(value)
                for row in forward_tensor_inventory(item, f"{path}[{index}]")]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-config", type=Path, required=True)
    parser.add_argument("--panel-date", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--compare-to", type=Path)
    parser.add_argument("--cpu-threads", type=int)
    parser.add_argument("--factorized-stream", choices=("0", "1"))
    parser.add_argument("--config", type=Path, help="Optional isolated runtime-only config candidate")
    parser.add_argument("--completed-session-cache", action="store_true")
    parser.add_argument("--cache-root", type=Path, help="Explicit private cache for offline acceptance")
    parser.add_argument("--cache-hit-only", action="store_true",
                        help="Fail if any model/CUDA work is attempted in this fresh process")
    parser.add_argument("--release-after-first", action="store_true")
    parser.add_argument("--forward-inventory", action="store_true",
                        help="Record full output shapes and logical cache size, never tensor values")
    args = parser.parse_args()
    if args.repeats < 1 or (args.cpu_threads is not None and args.cpu_threads < 1):
        parser.error("repeats and cpu-threads must be positive")
    if args.completed_session_cache and args.cache_root is None:
        parser.error("offline completed cache benchmark requires --cache-root")
    if (args.cache_hit_only or args.release_after_first) and not args.completed_session_cache:
        parser.error("cache-hit-only/release-after-first require --completed-session-cache")
    cfg = load_market_config(args.market_config)
    config_path = args.config or cfg.config_path
    config = load_config(config_path)
    if args.factorized_stream is not None:
        os.environ["STOCKAGENT_LIVE_FACTORIZED_STREAM"] = args.factorized_stream
    if args.cpu_threads is not None:
        torch.set_num_threads(args.cpu_threads)
    if args.cache_root:
        os.environ["STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT"] = str(args.cache_root.resolve())
    kwargs = cfg.signal_kwargs(write=False, publish_latest=False,
        ensure_previous_signal=False, previous_signal_backfill_limit=0,
        panel_date=args.panel_date, asof_date=args.panel_date + " 13:30:00",
        price_source="panel", use_completed_session_cache=args.completed_session_cache)
    kwargs["config_path"] = config_path
    started_at = datetime.now(timezone.utc).isoformat()
    samples = []
    forward_inventory = []
    profiler = cProfile.Profile() if args.profile else None
    expected = (json.loads(args.compare_to.read_text())["output_sha256"]
                if args.compare_to else None)
    with ExitStack() as stack:
        if args.cache_hit_only:
            if torch.cuda.is_initialized():
                raise RuntimeError("cache-hit-only must start without initialized CUDA")
            def forbid_gpu(*a, **kw):
                raise AssertionError("completed cache hit attempted model or CUDA work")
            for name in ("resolve_device", "_cached_live_model", "call_model"):
                stack.enter_context(patch.object(signal_engine, name, forbid_gpu))
            for name in ("is_available", "init", "synchronize", "empty_cache"):
                stack.enter_context(patch.object(torch.cuda, name, forbid_gpu))
        for name in ("fetch_shared_day_trade_stock_snapshots", "fetch_shioaji_stock_snapshots",
                     "fetch_tw_mis_last_prices", "fetch_tw_mis_opening_snapshot",
                     "fetch_tw_mis_session_snapshot", "fetch_yahoo_last_prices"):
            stack.enter_context(patch.object(signal_engine, name,
                side_effect=AssertionError("offline inference benchmark forbids network")))
        nested = {}
        input_shape = None

        def timed(name, fn):
            def call(*a, **kw):
                nonlocal input_shape
                if name == "call_model" and torch.cuda.is_initialized():
                    torch.cuda.synchronize()
                began = time.perf_counter()
                result = fn(*a, **kw)
                if args.forward_inventory and name == "call_model":
                    rows = forward_tensor_inventory(result)
                    forward_inventory.append({"logical_bytes": sum(row["bytes"] for row in rows),
                                              "tensors": rows})
                if name == "_cached_normalized_model_window":
                    input_shape = list(result.shape)
                if name == "call_model" and torch.cuda.is_initialized():
                    torch.cuda.synchronize()
                nested[name] = nested.get(name, 0.0) + (time.perf_counter()-began)*1000
                return result
            return call

        for name in ("_cached_normalized_model_window", "_cached_live_model",
                     "_day_trade_live_model_window", "call_model"):
            stack.enter_context(patch.object(signal_engine, name,
                timed(name, getattr(signal_engine, name))))
        for index in range(args.repeats + 1):
            nested.clear()
            if profiler is not None and index == 0:
                profiler.enable()
            started = time.perf_counter()
            result = signal_engine.generate_live_signal(**kwargs)
            elapsed_ms = (time.perf_counter()-started)*1000
            if profiler is not None and index == 0:
                profiler.disable()
            digest = contract_digest(result)
            if expected is None:
                expected = digest
            if expected != digest:
                raise RuntimeError("complete live inference output differs from control")
            sample = {"cache_state": "cold_process" if index == 0 else "warm_process",
                "elapsed_ms": elapsed_ms, "stages": result.summary["live_latency"],
                "nested_stage_ms_not_additive": dict(nested),
                "torch_threads": torch.get_num_threads(),
                "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                "gpu_peak_allocated_bytes": torch.cuda.max_memory_allocated()
                    if torch.cuda.is_initialized() else 0,
                "gpu_peak_reserved_bytes": torch.cuda.max_memory_reserved()
                    if torch.cuda.is_initialized() else 0}
            samples.append(sample)
            if index == 0 and args.release_after_first:
                sample["released_inference_models"] = signal_engine.release_idle_live_cuda_cache()
                sample["gpu_allocated_after_release_bytes"] = (
                    torch.cuda.memory_allocated() if torch.cuda.is_initialized() else 0)
                sample["gpu_reserved_after_release_bytes"] = (
                    torch.cuda.memory_reserved() if torch.cuda.is_initialized() else 0)
                if sample["gpu_allocated_after_release_bytes"] or sample["gpu_reserved_after_release_bytes"]:
                    import gc
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        objects = gc.get_objects()
                        tensors = [{"shape": list(value.shape), "dtype": str(value.dtype),
                                    "bytes": value.numel()*value.element_size(),
                                    "type": type(value).__name__}
                                   for value in objects if isinstance(value, torch.Tensor) and value.is_cuda]
                    del objects
                    raise RuntimeError("completed cache retained CUDA allocations: " + json.dumps({
                        "allocated": sample["gpu_allocated_after_release_bytes"],
                        "reserved": sample["gpu_reserved_after_release_bytes"],
                        "tensors": tensors}))
            if args.cache_hit_only and torch.cuda.is_initialized():
                raise RuntimeError("cache hit initialized CUDA")
            print(json.dumps(sample), flush=True)
        if profiler:
            args.profile.parent.mkdir(parents=True, exist_ok=True)
            profiler.dump_stats(str(args.profile))
    payload = {"contract": "canonical_live_inference_benchmark_v1",
        "scope": "complete_offline_signal_not_opening_or_broker_latency",
        "market": cfg.market, "config_path": str(config_path),
        "config_sha256": hashlib.sha256(Path(config_path).read_bytes()).hexdigest(),
        "checkpoint_fingerprint": result.summary["checkpoint_fingerprint"],
        "panel_date": args.panel_date, "symbol_count": len(result.weights_rows),
        "input_shape": input_shape,
        "feature_count": input_shape[-1] if input_shape else None,
        "output_sha256": expected, "exact_output_parity": True,
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "profiled_first_call": profiler is not None,
        "precision_contract": "unchanged_checkpoint_bf16_autocast_fp32_sensitive_blocks",
        "host": platform.node(), "torch_version": torch.__version__,
        "cuda_version": torch.version.cuda,
        "gpu_name": torch.cuda.get_device_name() if torch.cuda.is_initialized() else None,
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "requested_cpu_threads": args.cpu_threads,
        "factorized_stream_override": args.factorized_stream,
        "factorized_host_cache_bytes": config.data.factorized_host_cache_bytes,
        "factorized_manifest_sha256": (file_sha256(Path(config.data.factorized_feature_manifest))
            if config.data.factorized_feature_manifest else None),
        "network_requests": 0, "production_writes": False,
        "completed_session_cache": args.completed_session_cache,
        "cache_root": str(args.cache_root) if args.cache_root else None,
        "cache_hit_only": args.cache_hit_only,
        "cuda_initialized": torch.cuda.is_initialized(),
        "cuda_matmul_allow_tf32": torch.backends.cuda.matmul.allow_tf32,
        "configured_cpu_threads": config.environment.cpu_threads,
        "cold_process_ms": samples[0]["elapsed_ms"],
        "warm_median_ms": statistics.median(x["elapsed_ms"] for x in samples[1:]),
        "samples": samples}
    if args.forward_inventory:
        payload["forward_tensor_inventory"] = forward_inventory
    atomic_write_json(args.output, payload)


if __name__ == "__main__":
    main()
