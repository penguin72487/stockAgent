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
from stockagent.live import completed_inference_cache as forward_cache
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


class _ForwardIdentityCaptured(Exception):
    """Stop the offline observer before reading/writing a forward payload."""

    def __init__(self, identity: dict):
        self.identity = json.loads(json.dumps(identity, sort_keys=True, allow_nan=False))


def capture_forward_identity(kwargs: dict) -> dict:
    """Observe the canonical CPU input boundary, not a forward acceptance.

    Do not reimplement window/mask selection, read an old tensor as a new
    generation, create a cache lease or publish a signal. Canonical preparation
    may use its existing CPU panel caches; all model, CUDA and HTTP work fails
    closed. The source reader lease and final identity fence remain in place.
    """
    if not kwargs.get("use_completed_session_cache"):
        raise ValueError("identity observation requires the completed-session cache path")
    if kwargs.get("write", True) or kwargs.get("publish_latest", True):
        raise ValueError("identity observation forbids signal publication")
    if kwargs.get("ensure_previous_signal", True):
        raise ValueError("identity observation forbids historical signal generation")
    if kwargs.get("price_source") != "panel":
        raise ValueError("identity observation requires offline panel prices")
    if torch.cuda.is_initialized():
        raise RuntimeError("identity observation requires a fresh CUDA-cold process")

    source_generations = []
    original_forward = signal_engine._live_forward_outputs

    def record_source(**forward_kwargs):
        source_generations.append(forward_kwargs["source_fingerprint"])
        return original_forward(**forward_kwargs)

    def capture(root, identity, compute, *, validate_before_commit=None):
        if validate_before_commit is None:
            raise RuntimeError("canonical forward identity fence is missing")
        validate_before_commit()
        raise _ForwardIdentityCaptured(identity)

    def forbid_work(*a, **kw):
        raise AssertionError("identity observation attempted model, CUDA or HTTP work")

    with ExitStack() as stack:
        import requests
        stack.enter_context(patch.object(forward_cache, "cached_forward", capture))
        stack.enter_context(patch.object(signal_engine, "_live_forward_outputs", record_source))
        for name in ("resolve_device", "_cached_live_model", "call_model"):
            stack.enter_context(patch.object(signal_engine, name, forbid_work))
        for name in ("is_available", "init", "synchronize", "empty_cache"):
            stack.enter_context(patch.object(torch.cuda, name, forbid_work))
        stack.enter_context(patch.object(requests.sessions.Session, "request", forbid_work))
        started = time.perf_counter()
        try:
            signal_engine.generate_live_signal(**kwargs)
        except _ForwardIdentityCaptured as observed:
            if len(source_generations) != 1 or not source_generations[0]:
                raise RuntimeError("identity observation did not retain the canonical source fence")
            if torch.cuda.is_initialized():
                raise RuntimeError("identity observation initialized CUDA")
            return {
                "contract": "completed_forward_identity_observer_v1",
                "scope": "canonical_input_identity_not_forward_or_artifact_acceptance",
                "key": forward_cache.identity_digest(observed.identity),
                "identity": observed.identity,
                "source_generation": source_generations[0],
                "elapsed_ms": (time.perf_counter() - started) * 1000,
                "model_calls": 0, "network_requests": 0, "cuda_initialized": False,
                "signal_publication": False, "forward_cache_reads": 0,
                "forward_cache_writes": 0,
            }
    raise RuntimeError("identity observation never reached the canonical forward boundary")


def require_equal_forward_outputs(control, candidate, path: str = "output") -> None:
    """Exact full-tree oracle, including dtype, NaN bits, signs and auxiliaries."""
    if isinstance(control, torch.Tensor) and isinstance(candidate, torch.Tensor):
        if control.shape != candidate.shape or control.dtype != candidate.dtype:
            raise RuntimeError(f"{path}: tensor shape/dtype differs from forward control")
        a = control.detach().cpu().contiguous().reshape(-1).view(torch.uint8)
        b = candidate.detach().cpu().contiguous().reshape(-1).view(torch.uint8)
        if not torch.equal(a, b):
            raise RuntimeError(f"{path}: full tensor bytes differ from forward control")
    elif type(control) is not type(candidate):
        raise RuntimeError(f"{path}: output type differs from forward control")
    elif isinstance(control, dict):
        if control.keys() != candidate.keys():
            raise RuntimeError(f"{path}: auxiliary keys differ from forward control")
        for key in control:
            require_equal_forward_outputs(control[key], candidate[key], f"{path}.{key}")
    elif isinstance(control, (tuple, list)):
        if len(control) != len(candidate):
            raise RuntimeError(f"{path}: output length differs from forward control")
        for index, (a, b) in enumerate(zip(control, candidate)):
            require_equal_forward_outputs(a, b, f"{path}[{index}]")
    elif control != candidate:
        raise RuntimeError(f"{path}: output value differs from forward control")


def require_equal_metadata_only_candle_outputs(control, candidate, fields):
    """Only declared diagnostic values may be None; every other byte is exact."""
    if (not isinstance(control, tuple) or len(control) != 3
            or not isinstance(control[2], dict) or not fields
            or not set(fields).issubset(control[2])):
        raise RuntimeError("metadata-only candle oracle requires a complete tuple/field inventory")
    expected = (*control[:2], {key: None if key in fields else value
                              for key, value in control[2].items()})
    require_equal_forward_outputs(expected, candidate)


def measured_model_call(compute):
    """Bounded oracle telemetry, separate from production end-to-end timing."""
    cuda = torch.cuda.is_initialized()
    if cuda:
        torch.cuda.synchronize()
        torch.cuda.reset_peak_memory_stats()
        baseline = torch.cuda.memory_allocated()
    start = time.perf_counter()
    value = compute()
    if cuda:
        torch.cuda.synchronize()
    return value, {"wall_seconds": time.perf_counter() - start,
                   "peak_allocated_bytes": torch.cuda.max_memory_allocated() if cuda else None,
                   "incremental_peak_bytes": torch.cuda.max_memory_allocated() - baseline if cuda else None}


def forward_control_wrapper(control_path: Path, evidence: list, *, diagnostic_pairs: int = 0):
    """Qualify one new-code candidate against a verified old-generation tensor.

    Only code and the explicitly versioned projection/diagnostic policies may
    differ. Metadata-only candidates additionally compute the new DEFAULT full
    model as an independent oracle, then compare every non-omitted output and
    the complete signal projection. Oracle work is reported separately; it is
    not a production model call or a production first-miss latency measurement.
    """
    with control_path.open("rb") as handle:
        raw = handle.read(1024**2 + 1)
    if len(raw) > 1024**2:
        raise ValueError("forward control metadata exceeds bounded size")
    control = json.loads(raw)
    control_key = control["key"]
    if (control.get("contract") != forward_cache.CONTRACT
            or forward_cache.identity_digest(control["identity"]) != control_key
            or control_path.name != f"{control_key}.receipt.json"):
        raise ValueError("forward control contract/key/identity mismatch")
    original = forward_cache.cached_forward
    value = None

    def compare(root, identity, compute, *, validate_before_commit=None):
        nonlocal value
        if Path(root).resolve() == control_path.parent.resolve():
            raise ValueError("new-code candidate must use a private forward cache root")
        identity = json.loads(json.dumps(identity, sort_keys=True, allow_nan=False))
        differences = sorted(key for key in identity.keys() | control["identity"].keys()
                             if identity.get(key) != control["identity"].get(key))
        projection_changed = (
            identity.get("output_profile") == signal_engine._COMPLETED_SIGNAL_PROJECTION
            and "output_profile" not in control["identity"])
        allowed = {"code", "output_profile"} if projection_changed else {"code"}
        candle_policy_changed = (
            identity.get("candle_aux_policy") == signal_engine._COMPLETED_CANDLE_AUX_POLICY
            and "candle_aux_policy" not in control["identity"])
        if candle_policy_changed:
            if not projection_changed:
                raise RuntimeError("metadata-only qualification requires the original full model control")
            allowed.add("candle_aux_policy")
        changed_inputs = sorted(set(differences) - allowed)
        if changed_inputs:
            raise RuntimeError("forward control has different canonical inputs: " + ", ".join(changed_inputs))
        if validate_before_commit is None:
            raise RuntimeError("forward control requires canonical identity validation")
        validate_before_commit()
        if value is None:
            value = forward_cache._read(
                control_path.parent / f"{control_key}.forward", control_key, control["identity"])
            if value is forward_cache._CACHE_MISS:
                raise RuntimeError("verified old-generation forward control is unavailable")
            with control_path.open("rb") as handle:
                if handle.read(1024**2 + 1) != raw:
                    raise RuntimeError("forward control receipt changed during verification")
        expected = signal_engine._completed_signal_projection(value) if projection_changed else value
        model_calls = 0
        full_model_checks = 0
        default_model_checks = 0
        candle_fields = ()
        model_timings = {}

        def verified_compute():
            nonlocal model_calls, full_model_checks, default_model_checks, candle_fields
            model_calls += 1
            if projection_changed:
                project = signal_engine._completed_signal_projection
                call_model = signal_engine.call_model

                def verify_model(model, *args, **kwargs):
                    nonlocal default_model_checks, candle_fields
                    if kwargs.get("materialize_candle_aux") is not False:
                        raise RuntimeError("metadata-only candidate did not request diagnostic omission")
                    from stockagent.training.runtime import unwrap_model
                    target = unwrap_model(model)
                    inventory = getattr(target, "candle_diagnostic_aux_fields", None)
                    if not callable(inventory):
                        raise RuntimeError("metadata-only candidate has no explicit diagnostic inventory")
                    candle_fields = inventory()
                    full, model_timings["full_default"] = measured_model_call(lambda:
                        call_model(model, *args, **{**kwargs, "materialize_candle_aux": True}))
                    require_equal_forward_outputs(value, full)
                    default_model_checks += 1
                    del full
                    lean, model_timings["metadata_only"] = measured_model_call(lambda:
                        call_model(model, *args, **kwargs))
                    require_equal_metadata_only_candle_outputs(value, lean, candle_fields)
                    paired = []
                    for pair in range(diagnostic_pairs):
                        # AB/BA avoids crediting only the second, warmed path.
                        order = (True, False) if pair % 2 == 0 else (False, True)
                        for materialize in order:
                            sample, timing = measured_model_call(lambda:
                                call_model(model, *args,
                                           **{**kwargs, "materialize_candle_aux": materialize}))
                            if materialize:
                                require_equal_forward_outputs(value, sample)
                            else:
                                require_equal_metadata_only_candle_outputs(value, sample, candle_fields)
                            paired.append({"pair": pair,
                                           "mode": "full_default" if materialize else "metadata_only",
                                           **timing})
                            del sample
                    model_timings["balanced_pairs"] = paired
                    return lean

                def verify_then_project(full_output):
                    nonlocal full_model_checks
                    if candle_policy_changed:
                        require_equal_metadata_only_candle_outputs(value, full_output, candle_fields)
                    else:
                        require_equal_forward_outputs(value, full_output)
                    full_model_checks += 1
                    return project(full_output)

                with ExitStack() as stack:
                    stack.enter_context(patch.object(signal_engine, "_completed_signal_projection", verify_then_project))
                    if candle_policy_changed:
                        stack.enter_context(patch.object(signal_engine, "call_model", verify_model))
                    computed = compute()
                if full_model_checks != 1:
                    raise RuntimeError("projection bypassed its complete new-model output oracle")
                if candle_policy_changed and default_model_checks != 1:
                    raise RuntimeError("metadata-only projection bypassed its default full-model oracle")
            else:
                computed = compute()
            require_equal_forward_outputs(expected, computed)
            return computed

        candidate, receipt = original(root, identity, verified_compute,
                                      validate_before_commit=validate_before_commit)
        require_equal_forward_outputs(expected, candidate)
        evidence.append({"control_receipt": str(control_path), "control_key": control_key,
                         "control_receipt_sha256": hashlib.sha256(raw).hexdigest(),
                         "control_payload_sha256": control["payload_sha256"],
                         "candidate_key": receipt["key"], "different_components": differences,
                         "whole_output_bitwise_equal": not projection_changed,
                         "model_calls": model_calls,
                         **({"projection_contract": signal_engine._COMPLETED_SIGNAL_PROJECTION,
                             "complete_signal_projection_bitwise_equal": True,
                             "full_model_output_bitwise_equal":
                                 True if full_model_checks and not candle_policy_changed else None}
                            if projection_changed else {})})
        if candle_policy_changed:
            evidence[-1].update(
                candle_aux_policy=signal_engine._COMPLETED_CANDLE_AUX_POLICY,
                oracle_model_calls=default_model_checks,
                full_default_model_output_bitwise_equal=True if default_model_checks else None,
                non_candle_output_bitwise_equal=True if full_model_checks else None,
                omitted_candle_fields=list(candle_fields),
                model_timings=model_timings,
                diagnostic_benchmark_model_calls=2 * diagnostic_pairs if default_model_checks else 0,
                first_sample_includes_additional_full_model_oracle=bool(default_model_checks),
            )
        return candidate, receipt
    return compare


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-config", type=Path, required=True)
    parser.add_argument("--panel-date", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--profile-cache-state", choices=("cold", "warm"), default="cold",
                        help="Profile the first cold call or the first warm call; retain both in timing output")
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
    parser.add_argument("--capture-forward-identity", action="store_true",
                        help="Observe exact canonical CPU input identity; forbid model/CUDA/HTTP")
    parser.add_argument("--forward-control", type=Path,
                        help="Verify every tensor against an existing receipt; only code may differ")
    parser.add_argument("--candle-aux-pairs", type=int, default=0,
                        help="Additional balanced full/metadata-only model pairs; not production timing")
    args = parser.parse_args()
    if args.repeats < 1 or (args.cpu_threads is not None and args.cpu_threads < 1):
        parser.error("repeats and cpu-threads must be positive")
    if args.completed_session_cache and args.cache_root is None:
        parser.error("offline completed cache benchmark requires --cache-root")
    if (args.cache_hit_only or args.release_after_first) and not args.completed_session_cache:
        parser.error("cache-hit-only/release-after-first require --completed-session-cache")
    if args.capture_forward_identity:
        if not args.completed_session_cache:
            parser.error("capture-forward-identity requires --completed-session-cache")
        if (args.cache_hit_only or args.release_after_first or args.compare_to or args.forward_control
                or args.profile or args.forward_inventory):
            parser.error("identity observation is not a forward benchmark or output comparison")
    if args.forward_control and not args.completed_session_cache:
        parser.error("forward-control requires --completed-session-cache")
    if args.candle_aux_pairs < 0 or args.candle_aux_pairs > 10:
        parser.error("candle-aux-pairs must be between 0 and 10")
    if args.candle_aux_pairs and (not args.forward_control or args.cache_hit_only):
        parser.error("candle-aux-pairs requires a new-model forward-control benchmark")
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
    if args.capture_forward_identity:
        payload = capture_forward_identity(kwargs)
        payload.update(market=cfg.market, config_path=str(config_path),
                       panel_date=args.panel_date, started_at_utc=started_at,
                       finished_at_utc=datetime.now(timezone.utc).isoformat(),
                       host=platform.node())
        atomic_write_json(args.output, payload)
        print(json.dumps({key: value for key, value in payload.items() if key != "identity"}),
              flush=True)
        return
    samples = []
    forward_inventory = []
    forward_controls = []
    profiler = cProfile.Profile() if args.profile else None
    profile_index = 0 if args.profile_cache_state == "cold" else 1
    expected = (json.loads(args.compare_to.read_text())["output_sha256"]
                if args.compare_to else None)
    with ExitStack() as stack:
        import requests

        # Guard the transport too: enumerating today's quote fetchers is not
        # proof that a future input/helper cannot unexpectedly issue HTTP.
        stack.enter_context(patch.object(requests.sessions.Session, "request",
            side_effect=AssertionError("offline inference benchmark forbids network")))
        if args.forward_control:
            stack.enter_context(patch.object(forward_cache, "cached_forward",
                forward_control_wrapper(args.forward_control, forward_controls,
                                        diagnostic_pairs=args.candle_aux_pairs)))
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
            if profiler is not None and index == profile_index:
                profiler.enable()
            started = time.perf_counter()
            result = signal_engine.generate_live_signal(**kwargs)
            elapsed_ms = (time.perf_counter()-started)*1000
            if profiler is not None and index == profile_index:
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
        "profiled_first_call": profiler is not None and profile_index == 0,
        "profiled_cache_state": args.profile_cache_state if profiler is not None else None,
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
        "network_requests": 0, "network_guard": "requests_session_and_quote_fetchers",
        "production_writes": False,
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
    if args.forward_control:
        payload["full_forward_controls"] = forward_controls
    atomic_write_json(args.output, payload)


if __name__ == "__main__":
    main()
