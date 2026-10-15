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
import runpy
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
    parser.add_argument("--start-fold", type=int, default=None,
        help="Selected diagnostic fold; default is the config's canonical start_fold, not a fixed historical fold.")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=None,
        help="Global diagnostic batch; default is the selected config, not a historical fixed batch.")
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--fresh-packets", action="store_true",
        help="Create a new raw slab per measurement, as real epoch batches do; do not time repeated GPU packet reuse as first-use transport.")
    parser.add_argument("--cpu-threads", type=int, default=24)
    parser.add_argument("--transfer-mode", choices=("dense_cpu", "compact_cuda", "compact_cuda_packed", "compact_cuda_cached"), default=None)
    parser.add_argument("--compare-transport", action="store_true",
        help="Bit-check every actual raw chunk and compare native control/transport gradients, no optimizer.")
    parser.add_argument("--compare-compact-projection", action="store_true",
        help="Compare the opt-in full-affine compact stem to the identical dense stem, no optimizer.")
    parser.add_argument("--compare-sparse-projection", action="store_true",
        help="Compare resident CSR/narrow temporal execution to the selected canonical compact baseline, no optimizer.")
    parser.add_argument("--numeric-experiment", action="store_true",
        help="Explicitly allow a changed numerical trajectory; retain every strict oracle rejection. Native-repeat, finite-output and nonzero-gradient gates remain mandatory.")
    parser.add_argument("--attention-control-backend", choices=("auto", "manual", "sdpa"), default=None,
        help="Compare the selected attention backend to this canonical backend with identical observations/parameters; no optimizer or relaxed tolerances.")
    parser.add_argument("--compact-stream-control-bytes", type=int, default=None,
        help="Compare the selected stream budget to this control (0 is the original 1 GiB/128-stock policy), same parameters and tolerances.")
    args = parser.parse_args()
    runpy.run_path(str(args.code_root.resolve(strict=True) / "stockagent/storage_layout.py"))["admit_output"](
        args.output, "benchmark"
    )
    sys.path.insert(0, str(args.code_root.resolve(strict=True)))
    import numpy as np
    import torch
    from downloader.artifact_io import atomic_write_json
    from stockagent.config import load_config
    from stockagent.data.factorized_panel import (
        FactorizedPanelFeatures, FactorizedPanelSlab, attach_factorized_features, file_sha256,
    )
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds, year_boundary_offset_sessions
    from stockagent.models.factory import build_model
    from stockagent.models.temporal_basis_fit import temporal_basis_overrides_from_state_dict
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.runtime import load_checkpoint, load_model_state_dict
    from stockagent.training.trainer import (
        _PanelSlabForwardWrapper, _wrap_distributed_data_parallel_model,
        _dynamo_compile_counter_snapshot,
    )
    from stockagent.training.windowed import dataset_to_windowed_tensors
    from train import _build_panel_kwargs, _configure_cpu_parallelism, _configure_cuda_runtime

    rank = int(os.environ["LOCAL_RANK"])
    assert torch.cuda.is_available() and torch.cuda.device_count() == 2
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group("nccl", timeout=timedelta(minutes=15))
    assert torch.distributed.get_world_size() == 2
    assert args.warmup >= 2 and args.repeats >= 3
    _configure_cpu_parallelism(cpu_threads=args.cpu_threads, compile_threads=8, local_world_size=1)
    device = torch.device("cuda", rank)
    output = args.output / f"rank_{rank}"
    output.mkdir(parents=True, exist_ok=False)
    config = load_config(args.config)
    selected_fold = args.start_fold if args.start_fold is not None else config.runner.start_fold
    if selected_fold < 1:
        raise ValueError("diagnostic fold must be positive")
    global_batch = args.batch_size or config.training.batch_size_train
    if global_batch <= 0 or global_batch % 2:
        raise ValueError("diagnostic global batch must be positive and divisible by the actual two ranks")
    local_batch = global_batch // 2
    _configure_cuda_runtime(cudnn_benchmark=config.environment.cudnn_benchmark,
        use_tensor_cores=config.environment.use_tensor_cores)
    assert config.training.multi_gpu_strategy == "distributed_data_parallel"
    assert config.environment.amp_dtype == "bf16" and config.runner.require_cuda
    assert not config.training.day_trade_sparse_events
    panel = attach_factorized_features(
        build_panel(config.data.parquet_root, **_build_panel_kwargs(config)),
        config.data.factorized_feature_manifest,
        transfer_mode=args.transfer_mode or getattr(config.data,"factorized_transfer_mode","dense_cpu"),
        host_cache_bytes=getattr(config.data,"factorized_host_cache_bytes",None),
    )
    fold = next(f for f in build_expanding_year_folds(
        panel.dates, min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
        year_boundary_offset_sessions=year_boundary_offset_sessions(config),
    ) if f.fold_id == selected_fold)
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
    start = args.global_row + rank * local_batch
    assert args.global_row >= 0 and start + local_batch <= len(split)
    batch = split.panel_slab_batch_by_rows(start, start + local_batch, device=device, non_blocking=True)
    assert batch is not None and batch["feature_slab"].size(0) == local_batch + config.training.lookback - 1
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
    model.factorized_encoder_checkpoint = config.training.factorized_encoder_checkpoint
    model.factorized_encoder_vram_safety_margin_bytes = int(
        config.training.vram_safety_margin_gb * 1024**3)
    if args.compare_compact_projection and not model.factorized_compact_projection:
        raise ValueError("compact comparison requires the selected opt-in contraction config")
    model = model.to(device).train()
    del state, checkpoint
    gc.collect()
    wrapped = _wrap_distributed_data_parallel_model(
        _PanelSlabForwardWrapper(model), config=config, device=device,
    )
    coefficients = torch.linspace(-.01, .01, local_batch * panel.num_symbols,
        device=device).reshape(local_batch, -1)
    assert torch.get_num_threads() == args.cpu_threads
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
    if hasattr(FactorizedPanelSlab, "compact_stock_chunk"):
        FactorizedPanelSlab.compact_stock_chunk = timed("compact_read_pin_h2d_enqueue", FactorizedPanelSlab.compact_stock_chunk)
    cache = panel.features._cache
    cache.get = timed("compact_cache_get", cache.get)
    FactorizedPanelFeatures.compact_statistics_rows = timed("compact_expand_cpu", FactorizedPanelFeatures.compact_statistics_rows)
    FactorizedPanelSlab._compact_column_packet = timed("compact_rectangle_packet",FactorizedPanelSlab._compact_column_packet)

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
        current_batch = (split.panel_slab_batch_by_rows(start, start+local_batch,
            device=device,non_blocking=True) if args.fresh_packets else batch)
        events = [torch.cuda.Event(enable_timing=True) for _ in range(3)]
        profiler_context = torch.profiler.profile(
            activities=[torch.profiler.ProfilerActivity.CPU, torch.profiler.ProfilerActivity.CUDA],
            record_shapes=True, profile_memory=False, with_stack=False,
        ) if trace else nullcontext()
        with profiler_context as profiler:
            events[0].record()
            with torch.autocast("cuda", dtype=torch.bfloat16):
                actual = wrapped(current_batch["feature_slab"], current_batch["tradable_mask"])
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
            "nested_host_intervals": {key: dict(value) for key, value in metrics.items()},
            "cache_delta": {k: getattr(cache, k) - cache_before[k] for k in cache_before},
            "transfer_delta": {k: panel.features.transfer_stats[k]-v for k,v in transfer_before.items()},
            "compile_before": graphs_before, "compile_after": _dynamo_compile_counter_snapshot(),
            "peak_allocated_bytes": torch.cuda.max_memory_allocated(),
            "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
            "finite_output": bool(torch.isfinite(actual).all()),
            "output_sha256": hashlib.sha256(actual.detach().float().cpu().numpy().tobytes()).hexdigest(),
            "gradient_l1": float(sum(p.grad.detach().abs().sum() for p in model.parameters() if p.grad is not None))}
        assert record["finite_output"] and np.isfinite(record["gradient_l1"]) and record["gradient_l1"]>0
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
    comparison_failures=[]
    if args.compare_transport or args.compare_compact_projection or args.compare_sparse_projection or args.attention_control_backend or args.compact_stream_control_bytes is not None:
        source=panel.features;slab=batch["feature_slab"]
        selected_mode=source.transfer_mode
        selected_checkpoint=model.factorized_encoder_checkpoint
        selected_projection=bool(getattr(model,"factorized_compact_projection",False))
        selected_sparse=bool(getattr(model,"factorized_sparse_projection",False))
        if args.compare_sparse_projection and not selected_sparse:
            raise ValueError("sparse comparison requires the selected resident CSR experiment")
        selected_stream_bytes=int(getattr(model,"factorized_compact_stream_chunk_bytes",0))
        attention_modules = [(m, m.compiled_cross_attention_backend) for m in model.modules()
            if hasattr(m, "compiled_cross_attention_backend")]
        def set_attention_control(enabled):
            if args.attention_control_backend:
                for module, selected_backend in attention_modules:
                    module.compiled_cross_attention_backend = (args.attention_control_backend
                        if enabled else selected_backend)
            if args.compact_stream_control_bytes is not None:
                model.factorized_compact_stream_chunk_bytes=(args.compact_stream_control_bytes if enabled else selected_stream_bytes)
            if args.attention_control_backend or args.compact_stream_control_bytes is not None:
                model.__dict__.pop("_factorized_encode_fn", None)
                model.__dict__.pop("_factorized_head_fn", None)
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
        model.factorized_encoder_checkpoint=True
        if args.compare_sparse_projection:
            model.factorized_sparse_projection=False
            model.__dict__.pop("_factorized_encode_fn",None)
        set_attention_control(True)
        if args.compare_compact_projection:
            model.factorized_compact_projection=False
            # The compiled encoder closure owns the selected contraction.
            # Invalidate it when explicitly toggling only this oracle branch;
            # production never mutates the selected policy within a model.
            model.__dict__.pop("_factorized_encode_fn",None)
        run_once()
        expected_output=last_output
        expected_gradients={name:None if p.grad is None else p.grad.detach().clone()
            for name,p in model.named_parameters()}
        def compare(name):
            rows=[]
            output_pass=True;gradients_pass=True
            try:
                torch.testing.assert_close(last_output,expected_output,rtol=3e-3,atol=2e-4)
            except AssertionError as error:
                output_pass=False
                comparison_failures.append({"comparison":name,"phase":"output","detail":str(error)})
            for key,p in model.named_parameters():
                target=expected_gradients[key]
                assert (p.grad is None)==(target is None),key
                if target is None:continue
                difference=p.grad-target
                row={"parameter":key,"max_abs_error":float(difference.abs().max()),
                    "relative_l2_error":float(torch.linalg.vector_norm(difference)/
                        torch.linalg.vector_norm(target).clamp_min(1e-12))}
                try:
                    torch.testing.assert_close(p.grad,target,rtol=3e-2,atol=5e-4,msg=lambda d:key+": "+d)
                except AssertionError as error:
                    gradients_pass=False
                    comparison_failures.append({"comparison":name,"phase":"gradient",
                        "parameter":key,"detail":str(error)})
                rows.append(row)
            return {"comparison":name,"output_max_abs_error":float((last_output-expected_output).abs().max()),
                "parameters":rows,"output_pass_existing_bf16_oracle":output_pass,
                "all_gradients_pass_existing_bf16_oracle":gradients_pass}
        run_once();control=compare("dense_cpu_repeat_control")
        source.transfer_mode=selected_mode
        model.factorized_encoder_checkpoint=selected_checkpoint
        if args.compare_sparse_projection:
            model.factorized_sparse_projection=selected_sparse
            model.__dict__.pop("_factorized_encode_fn",None)
        set_attention_control(False)
        if args.compare_compact_projection:
            model.factorized_compact_projection=selected_projection
            model.__dict__.pop("_factorized_encode_fn",None)
        comparison_name = ("resident_csr_vs_canonical_compact_model" if args.compare_sparse_projection else
            "selected_stream_budget_vs_control"
            if args.compact_stream_control_bytes is not None else
            ("selected_attention_vs_" + args.attention_control_backend
             if args.attention_control_backend else selected_mode + "_vs_same_dense_model"))
        run_once();candidate=compare(comparison_name)
        source.transfer_mode=selected_mode
        comparison={"all_actual_raw_chunk_bits_equal":True,"raw_chunks":raw_chunks,
            "raw_logical_values_checked":slab.size(0)*slab.size(1)*slab.size(2),
            "native_repeat_control":control,"candidate":candidate,
            "output_rtol":3e-3,"output_atol":2e-4,"gradient_rtol":3e-2,"gradient_atol":5e-4,
            "tolerances":"unchanged existing test_factorized_cuda_ddp BF16 oracle",
            "failures":comparison_failures,"optimizer_updates":0}
        comparison["attention_control_backend"] = args.attention_control_backend
        comparison["compact_stream_control_bytes"] = args.compact_stream_control_bytes
        comparison["selected_compact_stream_bytes"] = selected_stream_bytes
        del expected_gradients,expected_output
    proof = {"state": "profiled_canonical_real_panel_model_only", "rank": rank,
        "world_size": 2, "selected_fold": fold.fold_id, "global_batch": global_batch,
        "local_batch": local_batch,
        "matmul_precision": torch.get_float32_matmul_precision(),
        "tf32": bool(torch.backends.cuda.matmul.allow_tf32),
        "features": len(panel.feature_names), "symbols": panel.num_symbols,
        "lookback": config.training.lookback, "global_row": args.global_row,
        "year_boundary_offset_sessions": year_boundary_offset_sessions(config),
        "train_start_date": str(panel.dates[fold.train_indices[0]]),
        "train_end_date": str(panel.dates[fold.train_indices[-1]]),
        "config_sha256": file_sha256(args.config),
        "feature_manifest_sha256": file_sha256(Path(config.data.factorized_feature_manifest)),
        "checkpoint_sha256": file_sha256(args.checkpoint), "cpu_threads": args.cpu_threads,
        "actual_cpu_threads": torch.get_num_threads(),
        "transfer_mode": getattr(panel.features,"transfer_mode","dense_cpu"),
        "factorized_encoder_checkpoint": config.training.factorized_encoder_checkpoint,
        "factorized_compact_projection": bool(getattr(model,"factorized_compact_projection",False)),
        "factorized_sparse_projection": bool(getattr(model,"factorized_sparse_projection",False)),
        "factorized_compact_stream_chunk_bytes": int(getattr(model,"factorized_compact_stream_chunk_bytes",0)),
        "packet_reuse_scope": "new_raw_slab_each_measurement" if args.fresh_packets else "same_raw_slab_repeated",
        "parameter_grad_sha256":gradients.hexdigest(),
        "numeric_experiment_selected":args.numeric_experiment,
        "strict_compatibility_passed":not comparison_failures,
        "paired_comparison":comparison,
        "process_affinity": sorted(os.sched_getaffinity(0)), "warmup": warm,
        "measured": measured, "profiled": traced, "optimizer_updates": 0,
        "formal_training_started": False, "full_lifecycle_verified": False,
        "scope": "real fold11 model/DDP/source only; linear gradient probe, not recurrent financial loss"}
    atomic_write_json(output / "profile.json", proof)
    # Numerical rejection is a shared phase boundary. Publish both ranks'
    # diagnostics before either destroys NCCL; otherwise a locally failing
    # rank can hang the peer's final barrier for the full process-group TTL.
    native_failed=(comparison is not None and (
        not comparison["native_repeat_control"]["output_pass_existing_bf16_oracle"]
        or not comparison["native_repeat_control"]["all_gradients_pass_existing_bf16_oracle"]))
    failed=torch.tensor([int(bool(comparison_failures)),int(native_failed)],device=device)
    torch.distributed.all_reduce(failed,op=torch.distributed.ReduceOp.MAX)
    if int(failed[1]):
        raise AssertionError("native repeated control rejected; not attributable to the selected experiment")
    if int(failed[0]) and not args.numeric_experiment:
        raise AssertionError("real-panel paired numerical oracle rejected; both rank diagnostics retained")
    torch.distributed.barrier()
    torch.distributed.destroy_process_group()


if __name__ == "__main__":
    try:
        main()
    finally:
        import torch
        if torch.distributed.is_initialized():
            torch.distributed.destroy_process_group()
