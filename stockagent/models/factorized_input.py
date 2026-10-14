"""Bounded host I/O and exact separable encoder scheduling for wide panels.

Only individual stock/time encoding is chunked. The ordinary shared model's
market attention, score allocation, recurrent loss and DDP batch remain intact.
No feature bottleneck, pruning, new attention architecture or sparse-event
execution is introduced. Raw observations remain in their source packages.
"""
from __future__ import annotations
import torch
from torch import nn
from torch.utils.checkpoint import checkpoint
from stockagent.models.transformer_base_portfolio import _safe_attention_mask


def factorized_head_backend_contract(model) -> str:
    """Version the narrow FP32 head's compiler policy, not its mathematics."""
    if (int(getattr(model, "d_model", 32)) < 32
            and bool(getattr(model, "portfolio_blocks_fp32", False))):
        return "sm12_narrow_fp32_head_aligned_inductor_v1"
    return "inductor_v1"


def _compact_encoder_static_abi_budget(features: int) -> int:
    # Power-of-two column buckets plus the full-width native stem, at most
    # full/tail stock chunks in train/eval. Static width avoids Inductor's
    # symbolic reduction-tiling failure. This is a finite graph inventory,
    # not an unbounded global recompile allowance.
    if features < 1:
        raise ValueError("compact encoder requires a positive logical width")
    return 4 * (int(features).bit_length() + 1)


def _use_encoder_checkpoint(model, slab, device):
    """Keep the historical policy unless direct encoding safely fits on CUDA.

    The admitted no-basis path projects each unique source row before rolling
    the narrow embeddings, rather than retaining overlapping wide windows.
    Reserve two full-width FP32 slabs, narrow temporal activations and the
    configured VRAM margin. Basis/window-normalization paths are not admitted.
    """
    if bool(getattr(model, "factorized_encoder_force_retention", False)):
        candle = model.candle_encoder
        if (device.type != "cuda" or model.temporal_basis_feature_encoder is not None
                or model._input_basis_enabled()
                or bool(getattr(candle, "feature_svd_components", 0))
                or bool(getattr(candle, "causal_feature_window_rms_normalization", False))):
            raise ValueError("forced activation retention requires the no-basis CUDA stream")
        # An explicit capacity experiment. The complete-fold launcher's VRAM
        # sampling/OOM gate admits it; do not silently revert to recomputation.
        return False
    if bool(getattr(model, "factorized_encoder_checkpoint", True)):
        return True
    candle = model.candle_encoder
    if (device.type != "cuda" or model.temporal_basis_feature_encoder is not None
            or model._input_basis_enabled()
            or bool(getattr(candle, "feature_svd_components", 0))
            or bool(getattr(candle, "causal_feature_window_rms_normalization", False))):
        return True
    rows, stocks, features = slab.shape
    wide_bytes = rows * stocks * features * 4
    narrow_bytes = (max(1, rows - model.lookback + 1) * model.lookback
                    * stocks * model.d_model * 4)
    required = (2 * wide_bytes + 16 * narrow_bytes
                + max(0, int(getattr(model, "factorized_encoder_vram_safety_margin_bytes",
                                    1536 * 1024**2))))
    free, _ = torch.cuda.mem_get_info(device)
    # Freed activations stay in PyTorch's allocator; those inactive blocks
    # remain reusable. Driver-free bytes alone would incorrectly switch back
    # to recomputation after the first warm batch.
    reusable = max(0, torch.cuda.memory_reserved(device) - torch.cuda.memory_allocated(device))
    return required > free + reusable


def _partition_compile_options(model, *, stage="encoder"):
    graphs = (bool(getattr(model, "factorized_cuda_graphs", False)) or
        (stage == "head" and bool(getattr(model, "factorized_portfolio_cuda_graphs", False))))
    options = {"triton.cudagraphs": graphs}
    if bool(getattr(model, "factorized_compile_autotune", False)):
        options["max_autotune"] = True
    if (bool(getattr(model.temporal_basis_feature_encoder, "fp32_contraction", False))
            or bool(getattr(getattr(model, "candle_encoder", None), "feature_svd_components", 0))
            or bool(getattr(getattr(model, "candle_encoder", None), "projection_fp32", False))
            or (model.temporal_basis_feature_encoder is None and (
                bool(getattr(model, "temporal_blocks_fp32", False))
                or bool(getattr(model, "portfolio_blocks_fp32", False))))):
        # Fusion must retain each operation's declared BF16/FP32 rounding.
        # Otherwise a graph cut can change allocation weights even with the
        # same parameters, FP32 basis, and correct backward autocast contract.
        if not bool(getattr(model, "factorized_relaxed_precision_casts", False)):
            options["emulate_precision_casts"] = True
    return options


def _call_partition(model,fn,*args,**kwargs):
    if model.factorized_input_compile and model.candle_encoder.candle_query.device.type=="cuda":
        import torch._functorch.config as aot_config
        import torch._dynamo.config as dynamo_config
        if not hasattr(aot_config,"backward_pass_autocast"):
            raise RuntimeError("factorized AMP compilation requires an explicit AOT backward autocast contract")
        # The canonical executor calls backward outside forward/loss autocast.
        # Include lazy compilation AND invocation in the patch, not just the
        # torch.compile constructor. Do not mutate other workers' defaults.
        # These are already bounded, independent Tensor partitions. DDP still
        # owns gradients/all-reduce around the ordinary shared model. Letting
        # Dynamo split an inner partition by the outer DDP bucket during
        # forward, but not during checkpoint recomputation (outside the DDP
        # forward context), changes AOT saved-tensor cardinality. Keep the same
        # compiled graph in both contexts; do not disable DDP or checkpoint
        # validation, change the model, or retain every raw GPU slab.
        compile_patch = {"optimize_ddp": False}
        static_budget = getattr(fn, "_stockagent_static_abi_budget", None)
        if static_budget is not None:
            compile_patch["recompile_limit"] = max(dynamo_config.recompile_limit, static_budget)
        with dynamo_config.patch(**compile_patch), aot_config.patch(backward_pass_autocast="off"):
            return fn(*args,**kwargs)
    return fn(*args,**kwargs)


def _factorized_stock_chunk_size(model, slab, mask, *, raw_basis=False):
    """Bound source workspace without tying large batches to a tiny stock cap."""
    compact = bool(getattr(model, "factorized_compact_projection", False))
    budget = int(slab.source.manifest.get("stream_chunk_bytes", 512 * 1024**2))
    rows = (1 if compact else 3) * slab.size(0)
    cap = 128
    if compact:
        selected = int(getattr(model, "factorized_compact_stream_chunk_bytes", 0))
        if selected:
            budget = min(selected, slab.source.max_slab_bytes)
            cap = slab.size(1)
        else:
            budget = max(budget, min(slab.source.max_slab_bytes, 1024**3))
    if raw_basis and not model.temporal_basis_feature_encoder.fp32_contraction:
        rows += mask.size(0) * model.lookback
    return max(1, min(cap, budget // max(1, rows * slab.size(2) * 4)))


def forward_factorized_live_window(model, window, mask, *, return_aux=None,
                                   symbol_indices=None, portfolio_context=None):
    """Lossless bounded transport, full-shape original math, private workspace."""
    if model.training or not torch.is_inference_mode_enabled():
        raise RuntimeError("factorized live windows require eval and inference mode")
    if (model._input_basis_enabled() or model._raw_temporal_basis_enabled()
            or model.temporal_basis_feature_encoder is not None
            or getattr(model, "daily_context_encoder", None) is not None
            or model.candle_encoder.causal_feature_window_rms_normalization
            or not model.candle_encoder.causal_feature_rms_normalization
            or model.candle_encoder.causal_feature_compression != "none"
            or model.candle_encoder.feature_svd_components
            or model.candle_encoder.feature_bottleneck_dim):
        raise ValueError("factorized live windows require the admitted no-basis model")
    if any(isinstance(m, nn.Dropout) and m.p != 0 for m in model.modules()):
        raise ValueError("factorized live encoding requires zero dropout")
    if tuple(window.shape) != (model.lookback, mask.size(1), model.num_features):
        raise ValueError("factorized live window does not match the model input schema")
    if not model.allow_dynamic_symbols and symbol_indices is None and mask.size(1) != model.num_symbols:
        raise ValueError("factorized live window does not match the checkpoint symbol axis")
    model._check_symbol_indices(symbol_indices, mask.size(1))
    collect = return_aux is True or (return_aux is None and model.return_aux and model.return_aux_details)
    # Preserve the ORIGINAL full shape/GEMM/reduction schedule. Chunking the
    # Candle projection itself is not bit-equivalent on all CUDA kernels.
    # Only storage transport is chunked. This freshly reconstructed allocation
    # is owned by this call, so RMS can normalize in place without a 5 GiB clone.
    raw = window.materialize(model.candle_encoder.candle_query.device)
    h, token_aux = model.candle_encoder(raw, return_aux=collect, owned_inference=True)
    del raw
    h = model._add_window_positions(h, int(mask.size(1)), symbol_indices)
    output = model._forward_embedded(h, mask, return_aux=return_aux,
                                    portfolio_context=portfolio_context)
    return model._attach_candle_aux(output, token_aux, return_aux)


@torch.compiler.disable
def forward_factorized_slab(model,slab,mask,*,temperature=None,return_aux=None,
                            symbol_indices=None,portfolio_context=None):
    """Eager source boundary around compiled, ordinary Tensor computations."""
    if model.attention_mode in {"full","axial"}:
        raise ValueError("wide factorized streaming requires the selected separable compact attention model")
    if return_aux is True or (return_aux is None and model.return_aux and model.return_aux_details):
        raise ValueError("detailed raw feature aux must use an explicitly bounded ordinary tensor")
    if getattr(model,"daily_context_encoder",None) is not None:
        raise ValueError("factorized daily-context streaming has not been admitted")
    if any(isinstance(m,nn.Dropout) and m.p!=0 for m in model.modules()):
        raise ValueError("chunked encoding is only admitted for zero-dropout experiments")
    device=model.candle_encoder.candle_query.device
    slab=slab.to(device=device)
    checkpoint_encoder = (model.training and torch.is_grad_enabled()
                          and _use_encoder_checkpoint(model, slab, device))
    if (not bool(getattr(model, "factorized_encoder_checkpoint", True))
            or bool(getattr(model, "factorized_encoder_force_retention", False))):
        key = (model.training, torch.is_grad_enabled(), tuple(slab.shape), checkpoint_encoder)
        observed = model.__dict__.setdefault("_encoder_storage_receipts", set())
        if key not in observed:
            observed.add(key)
            print(f"[encoder-storage] shape={tuple(slab.shape)} training={model.training} "
                  f"checkpoint={checkpoint_encoder} force_retention="
                  f"{bool(getattr(model, 'factorized_encoder_force_retention', False))}", flush=True)
    # Retained activations need no source reread on backward. Do not also keep
    # redundant compact GPU packets in that case (or during no-grad evaluation).
    slab._retain_gpu_packets = (checkpoint_encoder
        and bool(getattr(model, "factorized_encoder_packet_cache", True)))
    mask=mask.to(device=device,dtype=torch.bool)
    safe_mask=_safe_attention_mask(mask)
    if symbol_indices is None:
        symbol_indices=torch.arange(slab.size(1),device=device)
    else:symbol_indices=symbol_indices.to(device=device)
    encoder=model.temporal_basis_feature_encoder
    raw_basis=model._raw_temporal_basis_enabled()
    compact_projection=bool(getattr(model,"factorized_compact_projection",False))
    sparse_projection=bool(getattr(model,"factorized_sparse_projection",False))
    sparse_embeddings=None
    if sparse_projection:
        from stockagent.models.factorized_sparse_projection import project_factorized_sparse_rows
        slab._retain_gpu_packets=False
        sparse_embeddings=project_factorized_sparse_rows(model,slab)
    if encoder is not None and not raw_basis:
        raise ValueError("wide streaming currently admits the selected raw-feature basis ABI only")
    if raw_basis and not encoder.fuse_projection:
        raise ValueError("wide raw basis requires the canonical fused projection")
    kernels=None
    if raw_basis:
        kernel_fn=model.__dict__.get("_factorized_effective_kernel_fn")
        if kernel_fn is None:
            kernel_fn=encoder.fused_effective_kernel
            if device.type=="cuda" and model.factorized_input_compile:
                kernel_fn=torch.compile(kernel_fn,dynamic=False,options=_partition_compile_options(model))
            object.__setattr__(model,"_factorized_effective_kernel_fn",kernel_fn)
        # _basis only uses the source's dtype/device, not its observations.
        kernels=_call_partition(model,kernel_fn,encoder.feature_projection.weight.new_empty(()))
    def encode(raw,stock_mask,stock_ids,kernel,columns=None):
        if compact_projection:
            projected=(raw if sparse_projection else
                model.candle_encoder.forward_compact_columns(raw,columns))
            h=projected.unfold(0,model.lookback,1).permute(0,3,1,2).contiguous()
            h=model._add_window_positions(h,int(raw.size(1)),stock_ids)
            basis=None
        else:
            h=model._embed_windowed_from_panel_slab(raw,stock_ids)
            basis=model._raw_temporal_basis_windows_from_panel_slab(raw)
        temporal=model._apply_temporal_blocks(h,keep_all_steps=False)
        pooled=model._pool_temporal(temporal,stock_mask)
        if raw_basis:
            pooled=encoder._fused_projection(basis,pooled,effective_kernel=kernel,
                lag_batched=encoder.fp32_contraction)
            pooled=pooled.masked_fill(~stock_mask.unsqueeze(-1),0.)
        return pooled
    encode_fn=model.__dict__.get("_factorized_encode_fn")
    if encode_fn is None:
        encode_fn=encode
        if device.type=="cuda" and model.factorized_input_compile:
            encode_fn=torch.compile(encode_fn,dynamic=False,
                fullgraph=compact_projection,options=_partition_compile_options(model))
            if compact_projection:
                # Train/eval must share all static bucket ABIs without silently
                # falling back at Dynamo's eight-variant default. Fullgraph
                # raises if this finite budget is exceeded; steady epochs must
                # still report zero new graphs in the full-fold acceptance.
                encode_fn._stockagent_static_abi_budget = _compact_encoder_static_abi_budget(slab.size(2))
        object.__setattr__(model,"_factorized_encode_fn",encode_fn)
    chunk = _factorized_stock_chunk_size(model, slab, mask, raw_basis=raw_basis)
    if sparse_projection:
        # Bound temporal workspace by its narrow input, rather than F-wide
        # source storage. All stocks/dates/model dimensions remain selected.
        per_stock=max(1,mask.size(0)*model.lookback*model.d_model*4*8)
        chunk=max(1,min(512,(2*1024**3)//per_stock))
    outputs=[]
    for begin in range(0,slab.size(1),chunk):
        end=min(slab.size(1),begin+chunk)
        stock_mask=safe_mask[:,begin:end];stock_ids=symbol_indices[begin:end]
        # Bind loop values: backward recomputation must never use the last
        # chunk's rows, security IDs or masks for an earlier chunk.
        def read_and_encode(m,ids,k,narrow=None,begin=begin,end=end):
            if sparse_projection:
                return _call_partition(model,encode_fn,narrow,m,ids,k)
            if compact_projection:
                values,columns=slab.compact_stock_chunk(begin,end)
                key = (values.size(1), columns.numel())
                observed = model.__dict__.setdefault("_compact_bucket_receipts", set())
                if key not in observed:
                    observed.add(key)
                    dense_bucket = columns.numel() == slab.size(2)
                    compute_dtype = ("fp32" if dense_bucket else
                        getattr(model.candle_encoder, "compact_projection_compute_dtype", "fp32"))
                    print(f"[compact-execution] rows={values.size(0)} stocks={values.size(1)} "
                          f"bucket={columns.numel()} logical_features={slab.size(2)} "
                          f"dense_bucket={dense_bucket} compute_dtype={compute_dtype} "
                          f"packet_cache={slab._retain_gpu_packets}", flush=True)
                return _call_partition(model,encode_fn,values,m,ids,k,columns)
            return _call_partition(model,encode_fn,slab.stock_chunk(begin,end),m,ids,k)
        if sparse_projection:
            narrow=sparse_embeddings[:,begin:end]
            output=(checkpoint(read_and_encode,stock_mask,stock_ids,kernels,narrow,use_reentrant=False)
                if checkpoint_encoder else read_and_encode(stock_mask,stock_ids,kernels,narrow))
        elif checkpoint_encoder:
            output=checkpoint(read_and_encode,stock_mask,stock_ids,kernels,use_reentrant=False)
        else:output=read_and_encode(stock_mask,stock_ids,kernels)
        outputs.append(output)
    embeddings=torch.cat(outputs,dim=1)
    head_fn=model.__dict__.get("_factorized_head_fn")
    if head_fn is None:
        head_fn=model._forward_stock_embeddings
        if device.type=="cuda" and model.factorized_input_compile:
            narrow = factorized_head_backend_contract(model) != "inductor_v1"
            head_fn=torch.compile(head_fn,dynamic=False,fullgraph=narrow,
                                  options=_partition_compile_options(model, stage="head"))
            object.__setattr__(model, "_factorized_head_compile_backend", "inductor")
            if narrow:
                print("[model-compile] narrow FP32 head: Inductor fullgraph; "
                      "logical attention retained with aligned SDPA tiles", flush=True)
        object.__setattr__(model,"_factorized_head_fn",head_fn)
    result = _call_partition(model,head_fn,embeddings,mask,temperature=temperature,return_aux=return_aux,portfolio_context=portfolio_context)
    if bool(getattr(model, "factorized_portfolio_cuda_graphs", False)):
        # Annual training retains several forwards before a single backward.
        # Preserve caller-owned outputs across subsequent graph replays.
        from torch.utils._pytree import tree_map
        result = tree_map(lambda value: value.clone()
            if isinstance(value, torch.Tensor) and value.is_cuda else value, result)
    return result
