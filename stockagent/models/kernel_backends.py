"""Explicit optional kernels with original parameters and state-dict names.

These kernels change compute/rounding, never eligibility or financial accounting.
Import and execution failures stay visible; there is no automatic CPU fallback.
"""
from __future__ import annotations

import importlib.metadata
from pathlib import Path
import torch
from torch import nn
import torch.nn.functional as F
from stockagent.models.transformer_base_portfolio import PortfolioRMSNorm


def liger_layernorm_function():
    from liger_kernel.ops.layer_norm import LigerLayerNormFunction
    def norm(x, weight, bias, eps):
        return LigerLayerNormFunction.apply(x, weight, bias, eps)
    return norm


def liger_swiglu_function():
    from liger_kernel.ops.swiglu import LigerSiLUMulFunction
    def swiglu(gate, value):
        return LigerSiLUMulFunction.apply(gate, value)
    return swiglu


class LigerLayerNorm(nn.LayerNorm):
    def forward(self, x):
        if not x.is_cuda:
            raise RuntimeError("Liger normalization trial requires CUDA")
        return self._kernel(x.contiguous(), self.weight, self.bias, self.eps)


class QuackRMSNorm(PortfolioRMSNorm):
    def forward(self, x):
        if not x.is_cuda:
            raise RuntimeError("QuACK normalization trial requires CUDA")
        return self._kernel(x, self.weight, eps=self.eps)


class QuackLayerNorm(nn.LayerNorm):
    def forward(self, x):
        if not x.is_cuda:
            raise RuntimeError("QuACK normalization trial requires CUDA")
        return self._kernel(x, self.weight, self.bias, self.eps)


def quack_layernorm_function():
    from quack.rmsnorm import layernorm_fwd, layernorm_bwd

    class LayerNorm(torch.autograd.Function):
        @staticmethod
        def forward(ctx, x, weight, bias, eps):
            out, rstd, mean = layernorm_fwd(x, weight, bias, eps,
                return_rstd=True, return_mean=True)
            ctx.save_for_backward(x, weight, rstd, mean)
            ctx.has_bias = bias is not None
            return out

        @staticmethod
        def backward(ctx, dout):
            x, weight, rstd, mean = ctx.saved_tensors
            dx, dw, db = layernorm_bwd(x, weight, dout.contiguous(), rstd, mean,
                has_bias=ctx.has_bias)
            return dx, dw, db, None

    def layernorm(x, weight, bias, eps):
        # Flatten before .apply(), following QuACK's RMSNorm wrapper so the
        # autograd function keeps a fixed rank across compiled batch ABIs.
        return LayerNorm.apply(x.reshape(-1, x.shape[-1]).contiguous(),
            weight, bias, eps).reshape_as(x)
    return layernorm


class QuackLinear(nn.Linear):
    def forward(self, x):
        if not x.is_cuda:
            raise RuntimeError("QuACK linear trial requires CUDA")
        return self._kernel(x, self.weight, self.bias, tuned=False)


class QuantizedTrainingLinear(nn.Linear):
    def forward(self, x):
        if not x.is_cuda:
            raise RuntimeError("the selected low-precision training adapter requires CUDA")
        return self._kernel(x, self.weight, self.bias)


def torchao_fp8_function():
    """Use TorchAO 0.18's own dynamic-scale autograd with nonleaf stem weights.

    A functional call keeps index_select's credit path to every FP32 master
    column. No temporary Parameter or stored quantized weight detaches it.
    The vendor pads inner dimensions only. Explicit neutral M/N/K padding
    also keeps tail shapes valid for all three forward/backward GEMMs.
    """
    from torchao.float8 import Float8LinearConfig
    from torchao.float8.float8_linear import (
        LinearMMConfig, ScaledMMConfig, matmul_with_hp_or_float8_args,
    )
    config = Float8LinearConfig(pad_inner_dim=True)
    mm_config = LinearMMConfig(*[
        ScaledMMConfig(config.emulate, recipe.use_fast_accum, False, config.pad_inner_dim)
        for recipe in (config.gemm_config_output, config.gemm_config_grad_input, config.gemm_config_grad_weight)
    ])

    def linear(x, weight, bias=None):
        if torch.is_autocast_enabled():
            x = x.to(torch.get_autocast_dtype("cuda"))
        shape = x.shape[:-1]
        x = x.reshape(-1, x.shape[-1])
        rows, width, outputs = x.shape[0], x.shape[1], weight.shape[0]
        row_pad, width_pad, output_pad = (-rows) % 16, (-width) % 16, (-outputs) % 16
        if row_pad or width_pad:
            x = F.pad(x, (0, width_pad, 0, row_pad))
        if output_pad or width_pad:
            weight = F.pad(weight, (0, width_pad, 0, output_pad))
        output = matmul_with_hp_or_float8_args.apply(x, weight.t(), mm_config, config)
        output = output[:rows, :outputs].reshape(*shape, outputs)
        return output if bias is None else output + bias.to(output.dtype)
    return linear


_DEEPGEMM_MM = None


def _deepgemm_mm_operator():
    global _DEEPGEMM_MM
    if _DEEPGEMM_MM is None:
        @torch.library.custom_op("stockagent_frontier::deepgemm_bf16_mm", mutates_args=())
        def mm(a: torch.Tensor, weight: torch.Tensor, fp32_output: bool) -> torch.Tensor:
            import deep_gemm
            out = torch.empty((a.shape[0], weight.shape[0]), device=a.device,
                dtype=torch.float32 if fp32_output else torch.bfloat16)
            deep_gemm.bf16_gemm_nt(a, weight, out)
            return out

        @mm.register_fake
        def fake(a, weight, fp32_output):
            return a.new_empty((a.shape[0], weight.shape[0]),
                dtype=torch.float32 if fp32_output else torch.bfloat16)

        _DEEPGEMM_MM = mm
    return _DEEPGEMM_MM


def deepgemm_bf16_function():
    """Pinned DeepGEMM BF16 API; SM120 plain-store dispatch uses cuBLASLt."""
    import deep_gemm  # noqa: F401 - fail package admission before model execution
    # The pinned API is a PyBind entrypoint. A fake-registered custom op lets
    # fullgraph/AOT preserve that call without tracing into the vendor binding.
    mm = _deepgemm_mm_operator()

    class Linear(torch.autograd.Function):
        @staticmethod
        def forward(ctx, x, weight, bias):
            ctx.shape = x.shape
            ctx.dtypes = x.dtype, weight.dtype, bias.dtype if bias is not None else None
            a = x.reshape(-1, x.shape[-1]).to(torch.bfloat16).contiguous()
            w = weight.to(torch.bfloat16).contiguous()
            ctx.save_for_backward(a, w)
            out = mm(a, w, False)
            out = out.to(x.dtype).reshape(*x.shape[:-1], w.shape[0])
            return out if bias is None else out + bias.to(x.dtype)

        @staticmethod
        def backward(ctx, gradient):
            a, w = ctx.saved_tensors
            flat = gradient.reshape(-1, w.shape[0])
            g = flat.to(torch.bfloat16).contiguous()
            dx = mm(g, w.t().contiguous(), True)
            dw = mm(g.t().contiguous(), a.t().contiguous(), True)
            db = flat.float().sum(0).to(ctx.dtypes[2]) if ctx.dtypes[2] is not None else None
            return dx.to(ctx.dtypes[0]).reshape(ctx.shape), dw.to(ctx.dtypes[1]), db
    def linear(x, weight, bias=None):
        # Keep the module attribute a Python function. A bound Function.apply
        # descriptor makes Dynamo trace its setup_context comparison instead
        # of recognizing the autograd function at compact stem boundaries.
        return Linear.apply(x, weight, bias)
    return linear


def transformer_engine_linear_class(kind):
    flash4_function()  # Resolve the pinned FA4 namespace before TE's import.
    import transformer_engine.pytorch as te
    from transformer_engine.common.recipe import Float8CurrentScaling, NVFP4BlockScaling
    recipe = (Float8CurrentScaling() if kind == "te_fp8" else
        NVFP4BlockScaling(disable_rht=True, disable_stochastic_rounding=True))

    class TrainingLinear(te.Linear):
        def forward(self, x):
            if not x.is_cuda:
                raise RuntimeError("Transformer Engine training requires CUDA")
            rows = x.numel() // x.shape[-1]
            flat = x.reshape(rows, x.shape[-1])
            # Zero rows change neither tensorwise/block maxima nor master
            # weight credit. Crop before downstream normalization/accounting.
            padded = F.pad(flat, (0, 0, 0, (-rows) % 16))
            with torch.autocast("cuda", dtype=torch.bfloat16), te.autocast(recipe=recipe):
                result = super().forward(padded)
            return result[:rows].to(x.dtype).reshape(*x.shape[:-1], result.shape[-1])
    return TrainingLinear


def _adopt(module, cls, kernel):
    """Preserve parameter identities without initialization or RNG draws."""
    replacement = cls.__new__(cls)
    replacement.__dict__ = module.__dict__.copy()
    replacement._kernel = kernel
    return replacement


_FLASH4_UNMASKED_OPS = None


def _flash4_unmasked_operator():
    """Keep the pinned vendor's actual forward/backward opaque to Dynamo."""
    global _FLASH4_UNMASKED_OPS
    if _FLASH4_UNMASKED_OPS is None:
        from flash_attn.cute.interface import _flash_attn_fwd, _flash_attn_bwd

        @torch.library.custom_op("stockagent_frontier::flash4_unmasked_backward", mutates_args=())
        def backward(q: torch.Tensor, k: torch.Tensor, v: torch.Tensor,
                out: torch.Tensor, dout: torch.Tensor, lse: torch.Tensor,
                scale: float) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            dq, dk, dv = _flash_attn_bwd(q, k, v, out, dout.contiguous(), lse,
                softmax_scale=scale, causal=False)
            return dq, dk, dv

        @backward.register_fake
        def fake_backward(q, k, v, out, dout, lse, scale):
            return torch.empty_like(q), torch.empty_like(k), torch.empty_like(v)

        @torch.library.custom_op("stockagent_frontier::flash4_unmasked_forward", mutates_args=())
        def forward(q: torch.Tensor, k: torch.Tensor, v: torch.Tensor,
                scale: float) -> tuple[torch.Tensor, torch.Tensor]:
            out, lse, _, _, _ = _flash_attn_fwd(q, k, v,
                softmax_scale=scale, causal=False, return_lse=True)
            return out, lse

        @forward.register_fake
        def fake_forward(q, k, v, scale):
            return (q.new_empty((*q.shape[:-1], v.shape[-1])),
                q.new_empty((q.shape[0], q.shape[2], q.shape[1]), dtype=torch.float32))

        def setup_context(ctx, inputs, output):
            q, k, v, scale = inputs
            out, lse = output
            ctx.save_for_backward(q, k, v, out, lse)
            ctx.scale = scale
            # The public wrapper uses output credit only (return_lse=False).
            ctx.mark_non_differentiable(lse)

        def autograd_backward(ctx, dout, dlse):
            q, k, v, out, lse = ctx.saved_tensors
            dq, dk, dv = backward(q, k, v, out, dout, lse, ctx.scale)
            return dq, dk, dv, None

        forward.register_autograd(autograd_backward, setup_context=setup_context)
        _FLASH4_UNMASKED_OPS = (forward, backward)
    return _FLASH4_UNMASKED_OPS[0]


def flash4_function():
    # FA2 owns a regular flash_attn package; the official FA4 wheel installs a
    # separate flash_attn.cute namespace. Resolve both without uninstalling FA2.
    import flash_attn
    location = Path(importlib.metadata.distribution("flash-attn-4").locate_file("flash_attn"))
    # FA2 wheels can also bundle an older cute/ subtree. The selected FA4
    # distribution must precede that subtree, rather than merely be appended.
    if str(location) in flash_attn.__path__:
        flash_attn.__path__.remove(str(location))
    flash_attn.__path__.insert(0, str(location))
    from flash_attn.cute.interface import flash_attn_func
    unmasked = _flash4_unmasked_operator()
    import cutlass
    import cutlass.cute as cute

    @cute.jit
    def keep_key(batch, head, query_idx, key_idx, seqlen_info, aux_tensors):
        # FA4 calls mask_mod with TensorSSA vectors, including one-element
        # batch vectors. Load every key lane, then return a Boolean vector.
        kept = cute.make_rmem_tensor(key_idx.shape, cutlass.Boolean)
        for lane in cutlass.range_constexpr(cute.size(key_idx.shape)):
            kept[lane] = aux_tensors[0][batch[0], key_idx[lane]]
        return kept.load()

    def attention(q, k, v, attn_mask=None, dropout_p=0.0, scale=None):
        if dropout_p != 0.0:
            raise ValueError("the selected FA4 interface does not implement attention dropout")
        if q.dtype not in (torch.float16, torch.bfloat16):
            raise ValueError("FA4 training requires the explicit FP16/BF16 model trial")
        kwargs = {}
        if attn_mask is not None:
            if torch.cuda.get_device_capability(q.device)[0] == 12:
                raise ValueError("the selected FA4 SM120 backward does not support mask_mod; select the unmasked temporal trial")
            if (attn_mask.dtype != torch.bool or attn_mask.ndim != 4
                    or attn_mask.shape[1:3] != (1, 1)):
                raise ValueError("FA4 adapter admits only the canonical broadcast key mask")
            key_mask = attn_mask[:, 0, 0, :]
            # The callback can visit a padded tile; its padded keys stay false.
            pad = (-key_mask.shape[-1]) % 128
            kwargs = {"mask_mod": keep_key,
                "aux_tensors": [F.pad(key_mask, (0, pad)).contiguous()]}
        q, k, v = (t.transpose(1, 2).contiguous() for t in (q, k, v))
        if attn_mask is None:
            out = unmasked(q, k, v, q.shape[-1] ** -.5 if scale is None else scale)
        else:
            out = flash_attn_func(q, k, v, softmax_scale=scale, causal=False, **kwargs)
        # The selected FA4 interface returns (output, softmax_lse).
        if isinstance(out, tuple):
            out = out[0]
        return out.transpose(1, 2)
    return attention


def configure_model_kernels(model, settings):
    from stockagent.models.transformer_base_portfolio import (
        FlashSDPAAttention, PortfolioRMSNorm, SwiGLUFeedForward,
    )
    selected = {"norm": settings.kernel_norm_backend,
        "linear": settings.kernel_linear_backend,
        "swiglu": settings.kernel_swiglu_backend,
        "attention": settings.kernel_attention_backend,
        "softmax": settings.kernel_softmax_backend}
    allowed = {"norm": {"torch", "liger", "quack", "quack_layernorm"},
        "linear": {"torch", "quack", "quack_compact", "cublaslt", "torchao_fp8", "deepgemm_bf16", "te_fp8", "te_nvfp4"}, "swiglu": {"torch", "liger"},
        "attention": {"torch", "flash4", "flash4_temporal"}, "softmax": {"torch", "quack"}}
    for key, value in selected.items():
        if value not in allowed[key]:
            raise ValueError(f"unknown {key} kernel backend {value!r}")
    if settings.compact_projection_compute_dtype not in {"fp32", "bf16"}:
        raise ValueError("compact projection dtype must be fp32 or bf16")
    if settings.compact_projection_compute_dtype != "fp32" and not model.factorized_compact_projection:
        raise ValueError("compact projection precision requires the selected compact stem")
    if selected["linear"] == "quack_compact" and (not model.factorized_compact_projection
            or settings.compact_projection_compute_dtype != "bf16"):
        raise ValueError("QuACK compact GEMM requires the explicit compact BF16 projection trial")
    functions = {}
    if selected["linear"] == "cublaslt":
        torch.backends.cuda.preferred_blas_library("cublaslt")
    if selected["norm"] == "liger":
        functions["norm"] = liger_layernorm_function()
    if selected["norm"] == "quack":
        from quack import rmsnorm
        functions["norm"] = rmsnorm
    if selected["norm"] == "quack_layernorm":
        functions["norm"] = quack_layernorm_function()
    if selected["linear"] in {"quack", "quack_compact"}:
        from functools import partial
        from quack.linear import linear_func
        functions["linear"] = partial(linear_func, tuned=selected["linear"] == "quack_compact")
    if selected["linear"] == "torchao_fp8":
        functions["linear"] = torchao_fp8_function()
    if selected["linear"] == "deepgemm_bf16":
        functions["linear"] = deepgemm_bf16_function()
    if selected["linear"] in {"te_fp8", "te_nvfp4"}:
        functions["linear_class"] = transformer_engine_linear_class(selected["linear"])
    if selected["swiglu"] == "liger":
        functions["swiglu"] = liger_swiglu_function()
    if selected["attention"] in {"flash4", "flash4_temporal"}:
        functions["attention"] = flash4_function()
    if selected["softmax"] == "quack":
        from quack import softmax
        def last_dim_softmax(x):
            width = x.shape[-1]
            flat = x.reshape(-1, width)
            # QuACK vectorizes 8 BF16 / 4 FP32 values. Negative infinity
            # padding adds exactly zero attention mass, including its adjoint.
            padded = F.pad(flat, (0, (-width) % 8), value=-float("inf"))
            return softmax(padded.contiguous())[:, :width].reshape_as(x)
        functions["softmax"] = last_dim_softmax
    applied = {key: [] for key in selected}
    native_linear = []
    for name, module in list(model.named_modules()):
        replacement = None
        if selected["norm"] == "liger" and isinstance(module, nn.LayerNorm):
            replacement = _adopt(module, LigerLayerNorm, functions["norm"])
        elif selected["norm"] == "quack" and isinstance(module, PortfolioRMSNorm):
            replacement = _adopt(module, QuackRMSNorm, functions["norm"])
        elif selected["norm"] == "quack_layernorm" and isinstance(module, nn.LayerNorm):
            replacement = _adopt(module, QuackLayerNorm, functions["norm"])
        if replacement is not None:
            parent, _, leaf = name.rpartition(".")
            model.get_submodule(parent)._modules[leaf] = replacement
            applied["norm"].append(name)
        if selected["linear"] == "quack" and isinstance(module, nn.Linear):
            if module.in_features % 8 == 0 and module.out_features % 8 == 0:
                parent, _, leaf = name.rpartition(".")
                model.get_submodule(parent)._modules[leaf] = _adopt(module, QuackLinear, functions["linear"])
                applied["linear"].append(name)
            else:
                native_linear.append(name)
        if selected["linear"] in {"torchao_fp8", "deepgemm_bf16"} and isinstance(module, nn.Linear):
            if module.in_features >= 16 and module.out_features >= 16 and module.out_features % 8 == 0:
                parent, _, leaf = name.rpartition(".")
                model.get_submodule(parent)._modules[leaf] = _adopt(module, QuantizedTrainingLinear, functions["linear"])
                applied["linear"].append(name)
            else:
                native_linear.append(name)
        if selected["linear"] in {"te_fp8", "te_nvfp4"} and isinstance(module, nn.Linear):
            if module.in_features % 16 == 0 and module.out_features % 16 == 0:
                devices = [module.weight.device.index] if module.weight.is_cuda else []
                with torch.random.fork_rng(devices=devices):
                    replacement = functions["linear_class"](
                        module.in_features, module.out_features, bias=module.bias is not None,
                        params_dtype=module.weight.dtype, device=module.weight.device)
                replacement.weight = module.weight
                replacement.bias = module.bias
                replacement.train(module.training)
                parent, _, leaf = name.rpartition(".")
                model.get_submodule(parent)._modules[leaf] = replacement
                applied["linear"].append(name)
            else:
                native_linear.append(name)
        if selected["swiglu"] != "torch" and isinstance(module, SwiGLUFeedForward):
            module._kernel_swiglu = functions["swiglu"]
            applied["swiglu"].append(name)
        if isinstance(module, SwiGLUFeedForward):
            module.fused_gate_value_projection = settings.fused_swiglu_projection
        if (selected["attention"] != "torch" and isinstance(module, FlashSDPAAttention)
                and (selected["attention"] != "flash4_temporal" or name.startswith("temporal_blocks."))):
            module._kernel_attention = functions["attention"]
            applied["attention"].append(name)
        if selected["softmax"] != "torch" and isinstance(module, FlashSDPAAttention):
            module._kernel_softmax = functions["softmax"]
            applied["softmax"].append(name)
    if selected["norm"] != "torch" and not applied["norm"]:
        raise ValueError(f"{selected['norm']} has no compatible normalization sites in this architecture")
    for attr in ("factorized_compile_autotune", "factorized_cuda_graphs", "factorized_portfolio_cuda_graphs", "factorized_relaxed_precision_casts",
                 "factorized_encoder_force_retention", "factorized_encoder_packet_cache"):
        setattr(model, attr, getattr(settings, attr))
    model.candle_encoder.compact_projection_compute_dtype = settings.compact_projection_compute_dtype
    if selected["linear"] in {"quack", "quack_compact", "torchao_fp8", "deepgemm_bf16"}:
        model.candle_encoder._compact_linear = functions["linear"]
        applied["linear"].append("candle_encoder.compact_projection")
    receipt = {"contract": "explicit_training_kernel_dispatch_v1", "selected": selected,
        "applied_sites": applied, "native_unaligned_linear_sites": native_linear,
        "compact_projection_compute_dtype": settings.compact_projection_compute_dtype,
        "fused_swiglu_projection": settings.fused_swiglu_projection,
        "accounting_changed": False}
    if selected["linear"] in {"torchao_fp8", "deepgemm_bf16", "te_fp8", "te_nvfp4"}:
        package = {"torchao_fp8": "torchao", "deepgemm_bf16": "deep-gemm",
            "te_fp8": "transformer-engine", "te_nvfp4": "transformer-engine"}[selected["linear"]]
        receipt["low_precision_package_version"] = importlib.metadata.version(package)
    model.kernel_backend_receipt = receipt
    if (any(value != "torch" for value in selected.values())
            or settings.compact_projection_compute_dtype != "fp32" or settings.fused_swiglu_projection):
        import json
        print("[kernel-dispatch] " + json.dumps(receipt, sort_keys=True), flush=True)
    return receipt
