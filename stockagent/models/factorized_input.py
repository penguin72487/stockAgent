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


def _use_encoder_checkpoint(model, slab, device):
    """Keep the historical policy unless direct encoding safely fits on CUDA.

    The admitted no-basis path projects each unique source row before rolling
    the narrow embeddings, rather than retaining overlapping wide windows.
    Reserve two full-width FP32 slabs, narrow temporal activations and the
    configured VRAM margin. Basis/window-normalization paths are not admitted.
    """
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


def _partition_compile_options(model):
    options = {"triton.cudagraphs": False}
    if (bool(getattr(model.temporal_basis_feature_encoder, "fp32_contraction", False))
            or bool(getattr(getattr(model, "candle_encoder", None), "feature_svd_components", 0))
            or bool(getattr(getattr(model, "candle_encoder", None), "projection_fp32", False))
            or (model.temporal_basis_feature_encoder is None and (
                bool(getattr(model, "temporal_blocks_fp32", False))
                or bool(getattr(model, "portfolio_blocks_fp32", False))))):
        # Fusion must retain each operation's declared BF16/FP32 rounding.
        # Otherwise a graph cut can change allocation weights even with the
        # same parameters, FP32 basis, and correct backward autocast contract.
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
        with dynamo_config.patch(optimize_ddp=False), aot_config.patch(backward_pass_autocast="off"):
            return fn(*args,**kwargs)
    return fn(*args,**kwargs)


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
    # Retained activations need no source reread on backward. Do not also keep
    # redundant compact GPU packets in that case (or during no-grad evaluation).
    slab._retain_gpu_packets = checkpoint_encoder
    mask=mask.to(device=device,dtype=torch.bool)
    safe_mask=_safe_attention_mask(mask)
    if symbol_indices is None:
        symbol_indices=torch.arange(slab.size(1),device=device)
    else:symbol_indices=symbol_indices.to(device=device)
    encoder=model.temporal_basis_feature_encoder
    raw_basis=model._raw_temporal_basis_enabled()
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
    def encode(raw,stock_mask,stock_ids,kernel):
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
            encode_fn=torch.compile(encode_fn,dynamic=False,options=_partition_compile_options(model))
        object.__setattr__(model,"_factorized_encode_fn",encode_fn)
    budget=int(slab.source.manifest.get("stream_chunk_bytes",512*1024**2))
    rows=3*slab.size(0)
    if raw_basis and not encoder.fp32_contraction:
        # Original einsum can materialize B*L overlapping raw rows. Do not
        # size its workspace by unique slab rows alone.
        rows+=mask.size(0)*model.lookback
    per_stock=rows*slab.size(2)*4
    chunk=max(1,min(128,budget//max(1,per_stock)))
    outputs=[]
    for begin in range(0,slab.size(1),chunk):
        end=min(slab.size(1),begin+chunk)
        stock_mask=safe_mask[:,begin:end];stock_ids=symbol_indices[begin:end]
        # Bind loop values: backward recomputation must never use the last
        # chunk's rows, security IDs or masks for an earlier chunk.
        def read_and_encode(m,ids,k,begin=begin,end=end):
            return _call_partition(model,encode_fn,slab.stock_chunk(begin,end),m,ids,k)
        if checkpoint_encoder:
            output=checkpoint(read_and_encode,stock_mask,stock_ids,kernels,use_reentrant=False)
        else:output=read_and_encode(stock_mask,stock_ids,kernels)
        outputs.append(output)
    embeddings=torch.cat(outputs,dim=1)
    head_fn=model.__dict__.get("_factorized_head_fn")
    if head_fn is None:
        head_fn=model._forward_stock_embeddings
        if device.type=="cuda" and model.factorized_input_compile:
            head_fn=torch.compile(head_fn,dynamic=False,options=_partition_compile_options(model))
        object.__setattr__(model,"_factorized_head_fn",head_fn)
    return _call_partition(model,head_fn,embeddings,mask,temperature=temperature,return_aux=return_aux,portfolio_context=portfolio_context)
