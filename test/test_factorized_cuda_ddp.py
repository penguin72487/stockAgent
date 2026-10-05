"""Opt-in CUDA engineering oracle; not a substitute for full-fold acceptance."""
from __future__ import annotations

import copy
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import MethodType

import numpy as np
import pytest
import torch


def _partition_precision_diagnostics(model, raw, mask):
    """Isolate stock-shape rounding without changing the acceptance oracle.

    Exposing intermediates changes compiler fusion; this is a diagnostic,
    never an alternative numerical reference or a production execution path.
    """
    from stockagent.models.factorized_input import _call_partition, _partition_compile_options
    from stockagent.models.transformer_base_portfolio import _safe_attention_mask

    def stages(source, stock_mask, stock_ids):
        embedded = model._embed_windowed_from_panel_slab(source, stock_ids)
        temporal = model._apply_temporal_blocks(embedded, keep_all_steps=False)
        pooled = model._pool_temporal(temporal, stock_mask)
        basis = model._raw_temporal_basis_windows_from_panel_slab(source)
        fused = pooled if model.temporal_basis_feature_encoder is None else (
            model.temporal_basis_feature_encoder._fused_projection(basis, pooled, lag_batched=True))
        return embedded, temporal, pooled, fused.masked_fill(~stock_mask.unsqueeze(-1), 0.)

    compiled = torch.compile(stages, dynamic=False, options=_partition_compile_options(model))
    ids = torch.arange(raw.size(1), device=raw.device)
    safe = _safe_attention_mask(mask)
    with torch.no_grad():
        full = _call_partition(model, compiled, raw, safe, ids)
        parts = [_call_partition(model, compiled, raw[:, i:i+1].contiguous(),
            safe[:, i:i+1], ids[i:i+1]) for i in range(raw.size(1))]
        records = []
        for i, name in enumerate(("candle_embedding", "temporal", "pooled", "basis_fusion")):
            split = torch.cat([part[i] for part in parts], dim=2 if i < 2 else 1)
            expected = full[i]
            delta = split.float() - expected.float()
            records.append({"stage": name, "dtype": str(expected.dtype),
                "max_abs_error": float(delta.abs().max()),
                "relative_l2_error": float(torch.linalg.vector_norm(delta) /
                    torch.linalg.vector_norm(expected.float()).clamp_min(1e-12)),
                "different_cells": int(torch.count_nonzero(delta))})
        head = model.__dict__["_factorized_head_fn"]
        full_weights = _call_partition(model, head, full[-1], mask)
        split_weights = _call_partition(model, head,
            torch.cat([part[-1] for part in parts], dim=1), mask)
        records.append({"stage": "same_partition_head", "dtype": str(full_weights.dtype),
            "max_abs_error": float((full_weights - split_weights).abs().max())})
    return {"scope": "instrumented same-math stock-width diagnostic, not acceptance",
        "stages": records}


@pytest.mark.skipif(os.environ.get("STOCKAGENT_FACTOR_CUDA_DDP_TEST") != "1",
    reason="explicit dual-CUDA engineering acceptance only")
def test_compiled_factorized_model_dual_gpu_oracle(tmp_path):
    assert torch.cuda.device_count() >= 2
    subprocess.run([sys.executable, "-m", "torch.distributed.run", "--standalone",
        "--nproc_per_node=2", str(Path(__file__).resolve()), str(tmp_path)],
        env={**os.environ, "STOCKAGENT_FACTOR_CUDA_CODE_ROOT": str(Path.cwd())},
        check=True, timeout=900)


def worker(work: Path):
    code = Path(os.environ.get("STOCKAGENT_FACTOR_CUDA_CODE_ROOT", Path.cwd()))
    sys.path.insert(0, str(code))
    from stockagent.config import load_config
    from stockagent.data.factorized_panel import FactorizedPanelFeatures, write_array_block
    from stockagent.models.financial_transformer import FinancialTransformerModel
    from stockagent.training.trainer import _PanelSlabForwardWrapper, _wrap_distributed_data_parallel_model
    from stockagent.training.trainer import _dynamo_compile_counter_snapshot

    rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group("nccl", timeout=timedelta(minutes=10))
    assert torch.distributed.get_world_size() == 2
    root = work / f"rank_{rank}"
    root.mkdir(parents=True, exist_ok=False)
    config = load_config(code / os.environ.get("STOCKAGENT_FACTOR_ORACLE_CONFIG",
        "configs/deployments/tw_day_trade_factorized_native_20261004_v1.yaml"))
    selected_shape = os.environ.get("STOCKAGENT_FACTOR_SELECTED_SHAPE") == "1"
    torch.manual_seed(7)
    precision = os.environ.get("STOCKAGENT_FACTOR_FLOAT32_PRECISION",
        "high" if config.environment.use_tensor_cores else "highest")
    torch.set_float32_matmul_precision(precision)
    rng = np.random.default_rng(7)
    value_only = os.environ.get("STOCKAGENT_FACTOR_VALUES_ONLY") == "1"
    batch_rows=16;lookback=config.training.lookback if selected_shape else 4;rows=2*batch_rows+lookback
    symbols = int(os.environ.get("STOCKAGENT_FACTOR_ORACLE_SYMBOLS", "3"))
    individual_features = int(os.environ.get("STOCKAGENT_FACTOR_ORACLE_INDIVIDUAL_FEATURES", "1" if value_only else "4"))
    chunk_stocks = int(os.environ.get("STOCKAGENT_FACTOR_ORACLE_CHUNK_STOCKS", "1"))
    assert symbols >= 3 and individual_features >= 1 and chunk_stocks >= 1
    base = rng.normal(size=(rows, symbols, 2)).astype(np.float32)
    individual = rng.normal(size=(rows, symbols, individual_features)).astype(np.float32)
    transfer_mode=os.environ.get("STOCKAGENT_FACTOR_TRANSFER_MODE","dense_cpu")
    zero_stress=os.environ.get("STOCKAGENT_FACTOR_TRANSPORT_ZERO_STRESS")=="1"
    if zero_stress:
        individual[...,::2]=0.
        individual[1,2,0]=-0.0
    if not value_only:
        individual[..., -1] = 0.
    shared = rng.normal(size=(rows, 4)).astype(np.float32)
    if value_only:
        shared = np.ascontiguousarray(shared[:, :1])
    features = base.shape[-1] + individual.shape[-1] + shared.shape[-1]
    np.save(root / "common.npy", shared, allow_pickle=False)
    blocks = []
    for begin in range(0, rows, 2):
        proof = write_array_block(root / f"{begin}.npy.zst", individual[begin:begin+2], omit_zero_columns=True)
        blocks.append({**proof, "start": begin})
    source = FactorizedPanelFeatures(root, {"common": {"path": "common.npy"}, "blocks": blocks,
        "individual_channels": [f"i_{i}" for i in range(individual.shape[-1])],
        "common_channels": [f"m_{i}" for i in range(shared.shape[-1])],
        "stream_chunk_bytes": 3*(batch_rows+lookback-1)*features*4*chunk_stocks}, base,
        cache_bytes=individual.nbytes,transfer_mode=transfer_mode)
    dense = np.concatenate([base, individual, np.broadcast_to(shared[:, None, :], (rows, symbols, shared.shape[-1]))], axis=-1)
    device = torch.device("cuda", rank)
    kwargs = dict(lookback=4, num_features=features, num_symbols=symbols, d_model=8,
        attention_mode="market_token", temporal_layers=1, temporal_heads=2,
        cross_heads=2, latent_layers=1, num_latent_factors=2, num_market_tokens=1,
        market_layers=1, head_hidden_dim=8, head_layers=1, dropout=0., input_dropout=0.,
        candle_dropout=0., temporal_pooling="last", temporal_query_mode="last_only",
        norm_type="layernorm", causal_feature_rms_normalization=True,
        temporal_basis_families=["haar", "learned"], temporal_basis_components=1,
        temporal_basis_input="raw_features", portfolio_mode="long_short",
        portfolio_output_mode="projection_l1", return_aux=False, factorized_input_compile=True,
        temporal_basis_fp32_contraction=True)
    portfolio_mode = config.training.financial_transformer.portfolio_mode
    if portfolio_mode in {"", "auto"}:
        portfolio_mode = "long_only" if config.trading.long_only else "long_short"
    kwargs.update(portfolio_output_mode=config.training.financial_transformer.portfolio_output_mode,
        portfolio_mode=portfolio_mode)
    if selected_shape:
        from stockagent.models.factory import build_model
        from stockagent.models.temporal_basis_fit import fit_training_only_pca_klt
        # Fit the engineering fixture's own training windows, not a placeholder
        # PCA bank or any production validation/test dates.
        overrides={}
        if "pca_klt" in config.training.financial_transformer.temporal_basis_families:
            fitted = fit_training_only_pca_klt(dense,np.arange(lookback,rows),
                lookback=lookback,feature_lag=1,components=lookback-1)
            overrides["pca_klt"]=fitted.basis
        model = build_model(config=config,lookback=lookback,num_features=features,num_symbols=symbols,
            feature_names=[f"feature_{i}" for i in range(features)],
            temporal_basis_overrides=overrides)
        model.factorized_input_compile=True
    else:
        model = FinancialTransformerModel(**kwargs)
    model = model.to(device).train()
    rms = np.sqrt(np.square(dense.astype(np.float64)).mean(axis=(0, 1))).astype(np.float32)
    model.set_causal_feature_rms_normalizer(torch.tensor(np.where(rms > 1e-6, rms, 1.), device=device),
        torch.tensor(rms > 1e-6, device=device))
    if model.candle_encoder.feature_svd_components:
        from stockagent.models.feature_svd import fit_training_feature_svd
        fitted = fit_training_feature_svd(dense, np.ones(dense.shape[:2], dtype=bool),
            np.arange(lookback, rows), lookback=lookback, feature_lag=1,
            scale=np.where(rms > 1e-6, rms, 1.), active_mask=rms > 1e-6,
            components=model.candle_encoder.feature_svd_components,
            analysis_components=features, oversampling=0, power_iterations=1)
        model.candle_encoder.set_feature_svd_projection(fitted.directions)
    reduction_diagnostic = os.environ.get("STOCKAGENT_FACTOR_REDUCTION_DIAGNOSTIC", "")
    if reduction_diagnostic not in {"", "factorized_einsum", "dense_lag"}:
        raise ValueError("unknown isolated reduction diagnostic")
    if reduction_diagnostic == "factorized_einsum":
        # Fixture only: the original contraction may replicate raw rolling
        # rows; passing this probe never admits it for the wide production
        # panel or its bounded-memory contract.
        def original_contraction(self, kernel, source):
            return torch.einsum("olf,blsf->bso", kernel, source)
        model.temporal_basis_feature_encoder._lag_batched_projection = MethodType(
            original_contraction, model.temporal_basis_feature_encoder)
    oracle = copy.deepcopy(model)
    if reduction_diagnostic == "dense_lag":
        encoder_type = type(oracle.temporal_basis_feature_encoder)
        def lag_reference(self, temporal_source, z_base, effective_kernel=None, *, lag_batched=False):
            return encoder_type._fused_projection(self, temporal_source, z_base,
                effective_kernel, lag_batched=True)
        oracle.temporal_basis_feature_encoder._fused_projection = MethodType(
            lag_reference, oracle.temporal_basis_feature_encoder)
    wrapped = _wrap_distributed_data_parallel_model(_PanelSlabForwardWrapper(model), config=config, device=device)
    reference = _PanelSlabForwardWrapper(oracle)
    compiled_reference = torch.compile(reference, dynamic=False,
        options={"triton.cudagraphs": False, "emulate_precision_casts": True})
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4, weight_decay=.01)
    oracle_optimizer = torch.optim.AdamW(oracle.parameters(), lr=1e-4, weight_decay=.01)
    mask = torch.ones((batch_rows, symbols), device=device, dtype=torch.bool)
    mask[0, 2] = False
    coefficients = torch.tensor(rng.normal(size=(batch_rows,symbols)).astype(np.float32),device=device)
    start=1+rank*batch_rows;length=batch_rows+lookback-1
    slab = source.as_torch().slab(start,length)
    tensor = torch.from_numpy(dense[start:start+length]).to(device)
    if transfer_mode in {"compact_cuda","compact_cuda_packed","compact_cuda_cached"}:
        reconstructed=slab.to(device).stock_chunk(0,symbols)
        assert torch.equal(reconstructed.view(torch.int32),tensor.view(torch.int32)), "raw transport must be bit exact"
        if zero_stress:
            assert source.transfer_stats["host_payload_bytes"]<source.transfer_stats["logical_dense_bytes"]
        del reconstructed
    timings = []
    gradient_errors = []
    failures = []
    amp_modes = (False,) if config.environment.amp_dtype == "none" else (False, True)
    for amp in amp_modes:
        for step in range(3):
            optimizer.zero_grad(set_to_none=True)
            oracle_optimizer.zero_grad(set_to_none=True)
            torch.cuda.synchronize()
            started = time.perf_counter()
            with torch.autocast("cuda", dtype=torch.bfloat16, enabled=amp):
                # The selected experiment already compiles its dense model.
                # FP32 also checks the uncompiled oracle; BF16 compares the
                # original compiled arithmetic, not a different fusion mode.
                import torch._functorch.config as aot_config
                with aot_config.patch(backward_pass_autocast="off"):
                    expected = (compiled_reference if amp else reference)(tensor, mask)
                actual = wrapped(slab, mask)
            try:
                torch.testing.assert_close(actual, expected, rtol=3e-3 if amp else 2e-5, atol=2e-4 if amp else 2e-6)
            except AssertionError as error:
                failures.append({"phase": "output", "amp": amp, "step": step, "detail": str(error),
                    "actual": actual.detach().float().cpu().tolist(),
                    "expected": expected.detach().float().cpu().tolist()})
            (actual.float()*coefficients).sum().backward()
            (expected.float()*coefficients).sum().backward()
            nonzero = 0
            maximum = 0.
            per_parameter = []
            for name, parameter in model.named_parameters():
                target = dict(oracle.named_parameters())[name]
                assert (parameter.grad is None) == (target.grad is None), name
                if parameter.grad is not None:
                    # Distinct chronological rows per rank; the eager oracle
                    # must reproduce DDP's mean gradient, not a local gradient.
                    torch.distributed.all_reduce(target.grad)
                    target.grad.div_(torch.distributed.get_world_size())
                    maximum = max(maximum, float((parameter.grad-target.grad).abs().max()))
                    nonzero += int(bool(parameter.grad.abs().max() > 0))
                    per_parameter.append({"name": name, "max_abs_error": float((parameter.grad-target.grad).abs().max()),
                        "oracle_max_abs": float(target.grad.abs().max()),
                        "relative_l2_error": float(torch.linalg.vector_norm(parameter.grad-target.grad) /
                            torch.linalg.vector_norm(target.grad).clamp_min(1e-12))})
                    try:
                        torch.testing.assert_close(parameter.grad, target.grad, rtol=3e-2 if amp else 3e-4,
                            atol=5e-4 if amp else 3e-6, msg=lambda detail: name+": "+detail)
                    except AssertionError as error:
                        failures.append({"phase": "gradient", "parameter": name,
                            "amp": amp, "step": step, "detail": str(error)})
            assert nonzero > 0, "the oracle must exercise real parameter gradients"
            failed_rank = torch.tensor(int(bool(failures)), device=device)
            torch.distributed.all_reduce(failed_rank, op=torch.distributed.ReduceOp.MAX)
            if int(failed_rank):
                # Both ranks leave this exact optimizer boundary together.
                # One rank must not destroy NCCL while the other begins the
                # next step's collectives after its locally passing oracle.
                if not failures:
                    failures.append({"phase": "peer_rejected", "amp": amp, "step": step})
                result = {"state": "rejected_engineering_oracle", "rank": rank, "failures": failures,
                    "per_parameter": per_parameter, "timings_before_failure": timings,
                    "reduction_diagnostic": reduction_diagnostic,
                    "emulate_precision_casts": os.environ.get("TORCHINDUCTOR_EMULATE_PRECISION_CASTS", "0")}
                if os.environ.get("STOCKAGENT_FACTOR_FORWARD_DIAGNOSTIC") == "1":
                    with torch.autocast("cuda", dtype=torch.bfloat16, enabled=amp):
                        result["partition_precision"] = _partition_precision_diagnostics(model, tensor, mask)
                from downloader.artifact_io import atomic_write_json
                atomic_write_json(root / "diagnostics.json", result)
                print(json.dumps({"state": result["state"], "rank": rank,
                    "failures": len(failures), "diagnostics": str(root / "diagnostics.json")}), flush=True)
                raise AssertionError("precision diagnostics retained; numerical acceptance failed")
            optimizer.step()
            oracle_optimizer.step()
            torch.cuda.synchronize()
            timings.append({"amp": amp, "step": step, "wall_s": time.perf_counter()-started,
                "compile_counters": _dynamo_compile_counter_snapshot()})
            gradient_errors.append(maximum)
    compiled_keys = ["_factorized_encode_fn", "_factorized_head_fn"]
    if model._raw_temporal_basis_enabled():
        compiled_keys.append("_factorized_effective_kernel_fn")
    assert all(key in model.__dict__ for key in compiled_keys)
    assert _dynamo_compile_counter_snapshot()["unique_graphs"] >= len(compiled_keys)
    for name, parameter in model.named_parameters():
        assert torch.isfinite(parameter).all(), name
    if transfer_mode == "compact_cuda_cached":
        assert source.transfer_stats["payload_cache_hits"] > 0
        assert source.transfer_stats["payload_cache_peak_bytes"] <= source.max_slab_bytes
    result = {"rank": rank, "world_size": 2, "state": "accepted_engineering_oracle_only",
        "global_batch_rows":2*batch_rows,"distinct_rank_date_rows":True,
        "portfolio_output_mode":kwargs["portfolio_output_mode"], "float32_matmul_precision": precision,
        "selected_factory_architecture":selected_shape,"lookback":lookback,
        "model_dim":model.d_model,"source_features":features,"value_only_fixture":value_only,
        "source_symbols":symbols,"stock_chunk_width":chunk_stocks,
        "ragged_final_stock_chunk":bool(symbols % chunk_stocks),
        "largest_parameter_bytes":max(p.numel()*p.element_size() for p in model.parameters()),
        "ddp_bucket_bytes":int(config.training.ddp_bucket_cap_mb)*1024**2,
        "compiler_operation_dtype_policy":"emulate_precision_casts_explicit_options_v2",
        "reduction_diagnostic":reduction_diagnostic,
        "production_reduction_verified":not bool(reduction_diagnostic),
        "full_fold_verified": False, "timings": timings, "max_abs_grad_error": max(gradient_errors),
        "amp_reference": "compiled_original_dense_panel_slab", "fp32_reference": "eager_original_dense_panel_slab",
        "backward_pass_autocast": "off_scoped_to_compiled_regions",
        "temporal_basis_fp32_contraction": bool(getattr(model.temporal_basis_feature_encoder,"fp32_contraction",False)),
        "temporal_basis_families":list(model.temporal_basis_families),
        "feature_svd_components":model.candle_encoder.feature_svd_components,
        "temporal_blocks_fp32": bool(getattr(model, "temporal_blocks_fp32", False)),
        "portfolio_blocks_fp32": bool(getattr(model, "portfolio_blocks_fp32", False)),
        "candle_projection_fp32": bool(model.candle_encoder.projection_fp32),
        "resolved_attention_mode": model.attention_mode,
        "configured_amp_dtype": config.environment.amp_dtype,
        "transfer_mode":transfer_mode,"transfer_stats":dict(source.transfer_stats),
        "zero_stress_fixture":zero_stress,
        "optimizer_steps": len(timings),
        "max_allocated_bytes": torch.cuda.max_memory_allocated(), "cache_misses": source._cache.misses}
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(root / "acceptance.json", result)
    print(json.dumps(result), flush=True)
    torch.distributed.barrier()
    torch.distributed.destroy_process_group()


if __name__ == "__main__":
    try:
        worker(Path(sys.argv[1]))
    finally:
        if torch.distributed.is_initialized():
            torch.distributed.destroy_process_group()
