"""Profile the canonical wide-panel model, using real fold-owned observations.

Run with torchrun on both GPUs after the ordinary strict environment check.
This is a model-only diagnostic: it neither substitutes for the recurrent loss
nor advances an optimizer, and is not complete-fold performance acceptance.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import timedelta
import gc
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--global-row", type=int, default=2048)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--cpu-threads", type=int, default=24)
    parser.add_argument("--transfer-mode", choices=("dense_cpu", "compact_cuda", "compact_cuda_packed", "compact_cuda_cached"), default=None)
    parser.add_argument("--compare-transport", action="store_true",
        help="Bit-check every actual raw chunk and compare native control/transport gradients, no optimizer.")
    args = parser.parse_args()
    sys.path.insert(0, str(args.code_root.resolve(strict=True)))
    import numpy as np
    import torch
    from downloader.artifact_io import atomic_write_json
    from stockagent.config import load_config
    from stockagent.data.factorized_panel import (
        FactorizedPanelFeatures, FactorizedPanelSlab, attach_factorized_features, file_sha256,
    )
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.models.factory import build_model
    from stockagent.models.temporal_basis_fit import temporal_basis_overrides_from_state_dict
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.runtime import load_checkpoint, load_model_state_dict
    from stockagent.training.trainer import (
        _PanelSlabForwardWrapper, _wrap_distributed_data_parallel_model,
        _dynamo_compile_counter_snapshot,
    )
    from stockagent.training.windowed import dataset_to_windowed_tensors
    from train import _build_panel_kwargs

    rank = int(os.environ["LOCAL_RANK"])
    assert torch.cuda.is_available() and torch.cuda.device_count() == 2
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group("nccl", timeout=timedelta(minutes=15))
    assert torch.distributed.get_world_size() == 2
    assert args.warmup >= 2 and args.repeats >= 3
    torch.set_num_threads(args.cpu_threads)
    torch.set_float32_matmul_precision("highest")
    device = torch.device("cuda", rank)
    output = args.output / f"rank_{rank}"
    output.mkdir(parents=True, exist_ok=False)
    config = load_config(args.config)
    assert config.training.batch_size_train == 32
    assert config.training.multi_gpu_strategy == "distributed_data_parallel"
    assert config.environment.amp_dtype == "bf16" and config.runner.require_cuda
    assert not config.training.day_trade_sparse_events
    panel = attach_factorized_features(
        build_panel(config.data.parquet_root, **_build_panel_kwargs(config)),
        config.data.factorized_feature_manifest,
        transfer_mode=args.transfer_mode or getattr(config.data,"factorized_transfer_mode","dense_cpu"),
    )
    fold = next(f for f in build_expanding_year_folds(
        panel.dates, min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    ) if f.fold_id == 11)
    dataset = CrossSectionalDataset(
        panel, fold.train_indices, config.training.lookback, include_volume_notional=True,
        execution_mode=config.trading.execution_mode,
        lookback_context=config.walk_forward.lookback_context,
        short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
        day_trade_unlimited_margin_conversion=config.trading.tw_day_trade_unlimited_margin_conversion,
        tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
        tw_commission_rebate_timing=config.trading.tw_commission_rebate_timing,
    )
    split = dataset_to_windowed_tensors(dataset)
    # Only model source chunks need pinning in this diagnostic. Expanded
    # settlement metadata has aliasing strides and is not model input; do not
    # materialize/pin every unrelated tensor just to measure source transfer.
    split.features = split.features.pin_memory()
    start = args.global_row + rank * 16
    assert args.global_row >= 0 and start + 16 <= len(split)
    batch = split.panel_slab_batch_by_rows(start, start + 16, device=device, non_blocking=True)
    assert batch is not None and batch["feature_slab"].size(0) == 47
    checkpoint = load_checkpoint(args.checkpoint)
    state = checkpoint["model_state_dict"]
    model = build_model(
        config=config, lookback=config.training.lookback,
        num_features=len(panel.feature_names), num_symbols=panel.num_symbols,
        feature_names=panel.feature_names,
        temporal_basis_overrides=temporal_basis_overrides_from_state_dict(state),
    )
    load_model_state_dict(model, state, strict_no_fallback=True)
    model.factorized_input_compile = True
    model = model.to(device).train()
    del state, checkpoint
    gc.collect()
    wrapped = _wrap_distributed_data_parallel_model(
        _PanelSlabForwardWrapper(model), config=config, device=device,
    )
    coefficients = torch.linspace(-.01, .01, 16 * panel.num_symbols, device=device).reshape(16, -1)
    metrics = {}
    last_output = None

    def timed(name, operation):
        def call(*a, **k):
            begin = time.perf_counter()
            with torch.profiler.record_function("factorized." + name):
                value = operation(*a, **k)
            row = metrics.setdefault(name, {"calls": 0, "wall_s": 0.})
            row["calls"] += 1
            row["wall_s"] += time.perf_counter() - begin
            return value
        return call

    # Instrument the actual shared source methods without changing their math.
    # Totals are nested CPU intervals; they must not be added to GPU event time.
    FactorizedPanelFeatures.__getitem__ = timed("dense_expand_cpu", FactorizedPanelFeatures.__getitem__)
    FactorizedPanelSlab.stock_chunk = timed("read_pin_h2d_enqueue", FactorizedPanelSlab.stock_chunk)
    cache = panel.features._cache
    cache.get = timed("compact_cache_get", cache.get)
    FactorizedPanelFeatures.compact_statistics_rows = timed("compact_expand_cpu", FactorizedPanelFeatures.compact_statistics_rows)

    def run_once(trace=False):
        nonlocal last_output
        wrapped.zero_grad(set_to_none=True)
        metrics.clear()
        torch.cuda.synchronize()
        torch.distributed.barrier()
        torch.cuda.reset_peak_memory_stats()
        cache_before = {k: getattr(cache, k) for k in ("hits", "misses", "bytes_read")}
        transfer_before = dict(panel.features.transfer_stats) if hasattr(panel.features,"transfer_stats") else {}
        graphs_before = _dynamo_compile_counter_snapshot()
        began = time.perf_counter()
        events = [torch.cuda.Event(enable_timing=True) for _ in range(3)]
        profiler_context = torch.profiler.profile(
            activities=[torch.profiler.ProfilerActivity.CPU, torch.profiler.ProfilerActivity.CUDA],
            record_shapes=True, profile_memory=False, with_stack=False,
        ) if trace else nullcontext()
        with profiler_context as profiler:
            events[0].record()
            with torch.autocast("cuda", dtype=torch.bfloat16):
                actual = wrapped(batch["feature_slab"], batch["tradable_mask"])
            events[1].record()
            (actual.float() * coefficients).sum().backward()
            events[2].record()
            events[2].synchronize()
        elapsed = time.perf_counter() - began
        maximum = torch.tensor(elapsed, dtype=torch.float64, device=device)
        torch.distributed.all_reduce(maximum, op=torch.distributed.ReduceOp.MAX)
        record = {"wall_s": elapsed, "maximum_rank_wall_s": float(maximum),
            "forward_cuda_interval_ms": events[0].elapsed_time(events[1]),
            "backward_cuda_interval_ms": events[1].elapsed_time(events[2]),
            "nested_host_intervals": dict(metrics),
            "cache_delta": {k: getattr(cache, k) - cache_before[k] for k in cache_before},
            "transfer_delta": {k: panel.features.transfer_stats[k]-v for k,v in transfer_before.items()},
            "compile_before": graphs_before, "compile_after": _dynamo_compile_counter_snapshot(),
            "peak_allocated_bytes": torch.cuda.max_memory_allocated(),
            "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
            "finite_output": bool(torch.isfinite(actual).all()),
            "output_sha256": hashlib.sha256(actual.detach().float().cpu().numpy().tobytes()).hexdigest(),
            "gradient_l1": float(sum(p.grad.detach().abs().sum() for p in model.parameters() if p.grad is not None))}
        assert record["finite_output"] and np.isfinite(record["gradient_l1"])
        last_output=actual.detach().clone()
        if trace:
            profiler.export_chrome_trace(str(output / "trace.json"))
            (output / "cuda_top.txt").write_text(profiler.key_averages().table(
                sort_by="self_device_time_total", row_limit=35))
            (output / "cpu_top.txt").write_text(profiler.key_averages().table(
                sort_by="self_cpu_time_total", row_limit=35))
        print(json.dumps({"rank": rank, "trace": trace, **record}), flush=True)
        return record

    warm = [run_once() for _ in range(args.warmup)]
    measured = [run_once() for _ in range(args.repeats)]
    traced = run_once(trace=True) if args.trace else None
    gradients=hashlib.sha256()
    for name,parameter in model.named_parameters():
        gradients.update(name.encode())
        if parameter.grad is None:
            gradients.update(b"absent");continue
        flat=parameter.grad.detach().reshape(-1)
        for offset in range(0,flat.numel(),8*1024**2):
            gradients.update(flat[offset:offset+8*1024**2].cpu().numpy().tobytes())
    comparison=None
    if args.compare_transport:
        source=panel.features;slab=batch["feature_slab"]
        selected_mode=source.transfer_mode
        chunk=max(1,min(128,int(source.manifest.get("stream_chunk_bytes",512*1024**2))//
            (3*slab.size(0)*slab.size(2)*4)))
        raw_chunks=0
        for begin in range(0,slab.size(1),chunk):
            end=min(slab.size(1),begin+chunk)
            source.transfer_mode="dense_cpu";expected=slab.stock_chunk(begin,end)
            source.transfer_mode=selected_mode;actual=slab.stock_chunk(begin,end)
            assert torch.equal(expected.view(torch.int32),actual.view(torch.int32)), "raw input transport bits differ"
            raw_chunks+=1
            del expected,actual
        source.transfer_mode="dense_cpu"
        run_once()
        expected_output=last_output
        expected_gradients={name:None if p.grad is None else p.grad.detach().clone()
            for name,p in model.named_parameters()}
        def compare(name):
            rows=[]
            torch.testing.assert_close(last_output,expected_output,rtol=3e-3,atol=2e-4)
            for key,p in model.named_parameters():
                target=expected_gradients[key]
                assert (p.grad is None)==(target is None),key
                if target is None:continue
                difference=p.grad-target
                row={"parameter":key,"max_abs_error":float(difference.abs().max()),
                    "relative_l2_error":float(torch.linalg.vector_norm(difference)/
                        torch.linalg.vector_norm(target).clamp_min(1e-12))}
                torch.testing.assert_close(p.grad,target,rtol=3e-2,atol=5e-4,msg=lambda d:key+": "+d)
                rows.append(row)
            return {"comparison":name,"output_max_abs_error":float((last_output-expected_output).abs().max()),
                "parameters":rows,"all_gradients_pass_existing_bf16_oracle":True}
        run_once();control=compare("dense_cpu_repeat_control")
        source.transfer_mode=selected_mode;run_once();candidate=compare(selected_mode+"_vs_same_dense_model")
        source.transfer_mode=selected_mode
        comparison={"all_actual_raw_chunk_bits_equal":True,"raw_chunks":raw_chunks,
            "raw_logical_values_checked":slab.size(0)*slab.size(1)*slab.size(2),
            "native_repeat_control":control,"candidate":candidate,
            "output_rtol":3e-3,"output_atol":2e-4,"gradient_rtol":3e-2,"gradient_atol":5e-4,
            "tolerances":"unchanged existing test_factorized_cuda_ddp BF16 oracle",
            "optimizer_updates":0}
        del expected_gradients,expected_output
    proof = {"state": "profiled_canonical_real_panel_model_only", "rank": rank,
        "world_size": 2, "selected_fold": 11, "global_batch": 32,
        "features": len(panel.feature_names), "symbols": panel.num_symbols,
        "lookback": config.training.lookback, "global_row": args.global_row,
        "config_sha256": file_sha256(args.config),
        "feature_manifest_sha256": file_sha256(Path(config.data.factorized_feature_manifest)),
        "checkpoint_sha256": file_sha256(args.checkpoint), "cpu_threads": args.cpu_threads,
        "transfer_mode": getattr(panel.features,"transfer_mode","dense_cpu"),
        "parameter_grad_sha256":gradients.hexdigest(),
        "paired_comparison":comparison,
        "process_affinity": sorted(os.sched_getaffinity(0)), "warmup": warm,
        "measured": measured, "profiled": traced, "optimizer_updates": 0,
        "formal_training_started": False, "full_lifecycle_verified": False,
        "scope": "real fold11 model/DDP/source only; linear gradient probe, not recurrent financial loss"}
    atomic_write_json(output / "profile.json", proof)
    torch.distributed.barrier()
    torch.distributed.destroy_process_group()


if __name__ == "__main__":
    try:
        main()
    finally:
        import torch
        if torch.distributed.is_initialized():
            torch.distributed.destroy_process_group()
