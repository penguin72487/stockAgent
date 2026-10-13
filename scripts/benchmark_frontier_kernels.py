#!/usr/bin/env python3
"""Isolated CUDA operator/adjoint trials, diagnostic rather than fold acceptance."""
from __future__ import annotations

import argparse
from contextlib import nullcontext
import importlib.metadata
import io
import json
import math
from pathlib import Path
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import torch
from torch import nn
import torch.nn.functional as F
from downloader.artifact_io import atomic_write_json


CASES = ["layernorm_torch", "layernorm_compile", "layernorm_liger", "layernorm_liger_compile",
    "layernorm_quack", "layernorm_quack_compile",
    "rmsnorm_torch", "rmsnorm_compile", "rmsnorm_quack",
    "softmax_torch", "softmax_compile", "softmax_quack", "softmax_quack_padded",
    "swiglu_torch", "swiglu_compile", "swiglu_liger", "swiglu_liger_compile", "swiglu_triton",
    "swiglu_gluon", "swiglu_tilelang", "swiglu_helion", "swiglu_thunderkittens",
    "ffn_torch", "ffn_compile", "ffn_packed_compile",
    "gemm_torch", "gemm_cublaslt", "gemm_compile", "gemm_autotune", "gemm_quack", "gemm_quack_tuned",
    "gemm_torchao_fp8", "gemm_torchao_fp8_compile", "gemm_cusparselt", "gemm_cublasdx",
    "gemm_te_fp8", "gemm_te_mxfp8", "gemm_te_nvfp4", "gemm_deepgemm",
    "gemm_torchao_functional_fp8", "gemm_torchao_functional_fp8_compile",
    "gemm_deepgemm_adapter", "gemm_deepgemm_adapter_compile",
    "gemm_te_adapter_fp8", "gemm_te_adapter_fp8_compile",
    "gemm_te_adapter_nvfp4", "gemm_te_adapter_nvfp4_compile",
    "attention_math", "attention_flash", "attention_cudnn", "attention_compile", "attention_reduce_compile",
    "attention_flash4", "attention_flash4_nomask", "attention_flash4_nomask_compile", "attention_flex", "attention_flex_padded",
    "attention_flash_nomask", "adamw_fused", "adamw_foreach"]


def _operator(args):
    case = args.case
    device = torch.device("cuda", args.device)
    dtype = torch.float32 if args.dtype == "fp32" else torch.bfloat16
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.set_float32_matmul_precision("high")
    if case.startswith(("layernorm", "rmsnorm")):
        x = torch.randn(args.rows, args.width, device=device, dtype=dtype, requires_grad=True)
        w = torch.randn(args.width, device=device, dtype=torch.float32, requires_grad=True)
        b = torch.randn_like(w, requires_grad=True)
        if case.startswith("layernorm"):
            reference = lambda x, w, b: F.layer_norm(x.float(), (args.width,), w, b, 1e-5).to(x.dtype)
            inputs = (x, w, b)
            if case.startswith("layernorm_liger"):
                from stockagent.models.kernel_backends import LigerLayerNorm, liger_layernorm_function, _adopt
                layer = _adopt(nn.LayerNorm(args.width).to(device), LigerLayerNorm, liger_layernorm_function())
                with torch.no_grad():
                    layer.weight.copy_(w); layer.bias.copy_(b)
                inputs = (x, layer.weight, layer.bias)
                operation = lambda x, w, b: layer(x)
            elif case.startswith("layernorm_quack"):
                from stockagent.models.kernel_backends import quack_layernorm_function
                norm = quack_layernorm_function()
                operation = lambda x, w, b: norm(x, w, b, 1e-5)
            else:
                operation = reference
        else:
            reference = lambda x, w: (x.float() * torch.rsqrt(x.float().square().mean(-1, keepdim=True) + 1e-6) * w).to(x.dtype)
            inputs = (x, w)
            if case.endswith("quack"):
                from quack import rmsnorm
                operation = lambda x, w: rmsnorm(x, w, eps=1e-6)
            else:
                operation = reference
    elif case.startswith("softmax"):
        inputs = (torch.randn(args.rows, args.width, device=device, dtype=dtype, requires_grad=True),)
        reference = lambda x: torch.softmax(x, dim=-1)
        if case in {"softmax_quack", "softmax_quack_padded"}:
            from quack import softmax
            if case.endswith("padded"):
                operation = lambda x: softmax(F.pad(x, (0, (-args.width) % 8), value=-float("inf")))[:, :args.width]
            else:
                operation = softmax
        else:
            operation = reference
    elif case.startswith("swiglu"):
        inputs = tuple(torch.randn(args.rows, args.width, device=device, dtype=dtype,
            requires_grad=True) for _ in range(2))
        reference = lambda a, b: F.silu(a) * b
        if case.startswith("swiglu_liger"):
            from stockagent.models.kernel_backends import liger_swiglu_function
            operation = liger_swiglu_function()
        elif case.endswith("thunderkittens"):
            from scripts.frontier_cpp_operators import cpp_operator
            operation = cpp_operator("thunderkittens")
        elif case.rsplit("_", 1)[-1] in {"triton", "gluon", "tilelang", "helion"}:
            from scripts.frontier_dsl_operators import swiglu_operator
            operation = swiglu_operator(case.rsplit("_", 1)[-1])
        else:
            operation = reference
    elif case.startswith("ffn"):
        from stockagent.models.transformer_base_portfolio import SwiGLUFeedForward
        hidden = 8 * math.ceil((args.width * 4 * 2 / 3) / 8)
        layer = SwiGLUFeedForward(args.width, hidden, 0.).to(device=device)
        x = torch.randn(args.rows, args.width, device=device, dtype=dtype, requires_grad=True)
        inputs = (x, *layer.parameters())
        layer.fused_gate_value_projection = "packed" in case
        def operation(x, *parameters):
            with torch.autocast("cuda", enabled=dtype == torch.bfloat16, dtype=torch.bfloat16):
                return layer(x)
        def reference(x, gw, gb, vw, vb, ow, ob):
            with torch.autocast("cuda", enabled=dtype == torch.bfloat16, dtype=torch.bfloat16):
                return F.linear(F.silu(F.linear(x, gw, gb)) * F.linear(x, vw, vb), ow, ob)
    elif case.startswith("gemm"):
        if case.endswith("cublaslt"):
            torch.backends.cuda.preferred_blas_library("cublaslt")
        x = torch.randn(args.rows, args.width, device=device, dtype=dtype, requires_grad=True)
        w = torch.randn(args.output_width, args.width, device=device, dtype=dtype, requires_grad=True) / math.sqrt(args.width)
        w = w.detach().requires_grad_()
        b = torch.randn(args.output_width, device=device, dtype=dtype, requires_grad=True)
        inputs = (x, w, b)
        reference = F.linear
        operation = reference
        if case.endswith("cublasdx"):
            if dtype != torch.bfloat16:
                raise ValueError("this cuBLASDx tile diagnostic selects BF16 GEMM")
            from scripts.frontier_cpp_operators import cpp_operator
            operation = cpp_operator("cublasdx")
        if case in {"gemm_quack", "gemm_quack_tuned"}:
            from quack.linear import linear_func
            operation = lambda x, w, b: linear_func(x, w, b, tuned=case.endswith("tuned"))
        if case in {"gemm_torchao_fp8", "gemm_torchao_fp8_compile"}:
            from torchao.float8 import convert_to_float8_training, Float8LinearConfig
            layer = nn.Linear(args.width, args.output_width, device=device, dtype=torch.float32)
            # Return gradients for this operator's actual FP32 master weights.
            layer = convert_to_float8_training(layer, config=Float8LinearConfig(pad_inner_dim=True))
            with torch.no_grad():
                layer.weight.copy_(w.float()); layer.bias.copy_(b.float())
            inputs = (x, layer.weight, layer.bias)
            def operation(x, w, b):
                with torch.autocast("cuda", dtype=torch.bfloat16):
                    return layer(x)
            def reference(x, w, b):
                with torch.autocast("cuda", dtype=torch.bfloat16):
                    return F.linear(x, w, b)
        if case.endswith("cusparselt"):
            # An explicit 2:4 parameter constraint, measured against its own
            # dense mathematical oracle. This never prunes source features.
            if dtype != torch.bfloat16 or args.width % 4:
                raise ValueError("the sparse training diagnostic requires BF16 and K divisible by four")
            keep = (torch.arange(args.width, device=device) % 4 < 2).expand_as(w)
            reference = lambda x, w, b: F.linear(x, w * keep, b)
            class SparseLinear(torch.autograd.Function):
                @staticmethod
                def forward(ctx, x, w, b):
                    from torch.sparse import to_sparse_semi_structured
                    weight = w * keep
                    ctx.save_for_backward(x, weight)
                    padded_k = (-args.width) % 128
                    padded_n = (-args.output_width) % 128
                    sparse = to_sparse_semi_structured(F.pad(weight, (0, padded_k, 0, padded_n)).contiguous())
                    out = torch.mm(sparse, F.pad(x, (0, padded_k)).T).T[:, :args.output_width]
                    return out + b
                @staticmethod
                def backward(ctx, g):
                    x, weight = ctx.saved_tensors
                    return g @ weight, (g.T @ x) * keep, g.sum(0)
            operation = SparseLinear.apply
        if case in {"gemm_te_fp8", "gemm_te_mxfp8", "gemm_te_nvfp4"}:
            # TE imports FA4 even for its Linear module. Resolve the official
            # namespace before the inherited FA2 wheel's older cute subtree.
            from stockagent.models.kernel_backends import flash4_function
            flash4_function()
            import transformer_engine.pytorch as te
            from transformer_engine.common.recipe import Float8CurrentScaling, MXFP8BlockScaling, NVFP4BlockScaling
            recipes = {"gemm_te_fp8": Float8CurrentScaling(),
                "gemm_te_mxfp8": MXFP8BlockScaling(),
                "gemm_te_nvfp4": NVFP4BlockScaling(disable_rht=True, disable_stochastic_rounding=True)}
            recipe = recipes[case]
            layer = te.Linear(args.width, args.output_width, params_dtype=torch.float32, device=device)
            with torch.no_grad():
                layer.weight.copy_(w.float()); layer.bias.copy_(b.float())
            inputs = (x, layer.weight, layer.bias)
            def operation(x, w, b):
                with torch.autocast("cuda", dtype=torch.bfloat16), te.autocast(recipe=recipe):
                    return layer(x)
            def reference(x, w, b):
                with torch.autocast("cuda", dtype=torch.bfloat16):
                    return F.linear(x, w, b)
        if case.startswith(("gemm_te_adapter_fp8", "gemm_te_adapter_nvfp4")):
            from stockagent.models.kernel_backends import transformer_engine_linear_class
            kind = "te_nvfp4" if "nvfp4" in case else "te_fp8"
            layer = transformer_engine_linear_class(kind)(args.width, args.output_width,
                params_dtype=torch.float32, device=device)
            with torch.no_grad():
                layer.weight.copy_(w.float()); layer.bias.copy_(b.float())
            inputs = (x, layer.weight, layer.bias)
            operation = lambda x, w, b: layer(x)
            operation._checkpoint_module = layer
            # The adapter retains FP32 masters even when its input is BF16.
            # Compare the same input compute dtype without demoting storage.
            reference = lambda x, w, b: F.linear(x, w.to(x.dtype), b.to(x.dtype))
        if case.endswith("deepgemm"):
            import deep_gemm
            class DeepGemm(torch.autograd.Function):
                @staticmethod
                def forward(ctx, x, w, b):
                    a, weight = x.to(torch.bfloat16).contiguous(), w.to(torch.bfloat16).contiguous()
                    ctx.save_for_backward(a, weight)
                    ctx.dtypes = x.dtype, w.dtype, b.dtype
                    out = torch.empty(a.shape[0], weight.shape[0], device=device, dtype=torch.bfloat16)
                    deep_gemm.bf16_gemm_nt(a, weight, out)
                    return (out + b).to(x.dtype)
                @staticmethod
                def backward(ctx, g):
                    a, w = ctx.saved_tensors
                    g = g.to(torch.bfloat16).contiguous()
                    dx, dw = torch.empty_like(a), torch.empty_like(w)
                    deep_gemm.bf16_gemm_nt(g, w.T.contiguous(), dx)
                    deep_gemm.bf16_gemm_nt(g.T.contiguous(), a.T.contiguous(), dw)
                    return dx.to(ctx.dtypes[0]), dw.to(ctx.dtypes[1]), g.sum(0).to(ctx.dtypes[2])
            operation = DeepGemm.apply
        if case.startswith("gemm_torchao_functional_fp8"):
            from stockagent.models.kernel_backends import torchao_fp8_function
            operation = torchao_fp8_function()
        if case.startswith("gemm_deepgemm_adapter"):
            from stockagent.models.kernel_backends import deepgemm_bf16_function
            operation = deepgemm_bf16_function()
    elif case.startswith("attention"):
        shape_q = (args.rows, 4, args.query_steps, args.width)
        shape_k = (args.rows, 4, args.key_steps, args.width)
        inputs = tuple(torch.randn(shape, device=device, dtype=dtype, requires_grad=True)
            for shape in (shape_q, shape_k, shape_k))
        mask = torch.rand(args.rows, 1, 1, args.key_steps, device=device) > .2
        mask[..., 0] = True
        mask[0] = False  # Completely masked rows must remain finite and zero.
        if "nomask" in case:
            mask = None
        scale = args.width ** -.5
        reference = lambda q, k, v: F.scaled_dot_product_attention(q, k, v, attn_mask=mask, scale=scale)
        operation = reference
        if case == "attention_reduce_compile":
            if args.query_steps != 1:
                raise ValueError("the exact last-query reduction requires Q=1")
            def operation(q, k, v):
                scores = (q * k).sum(-1) * scale
                if mask is not None:
                    scores = scores.masked_fill(~mask.squeeze(-2), torch.finfo(scores.dtype).min)
                probabilities = torch.softmax(scores, -1)
                if mask is not None:
                    probabilities = probabilities * mask.squeeze(-2).any(-1, keepdim=True)
                return (probabilities.unsqueeze(-1) * v).sum(-2, keepdim=True)
        if case in {"attention_flash4", "attention_flash4_nomask", "attention_flash4_nomask_compile"}:
            from stockagent.models.kernel_backends import flash4_function
            backend = flash4_function()
            operation = lambda q, k, v: backend(q, k, v, attn_mask=mask, scale=scale)
        if case in {"attention_flex", "attention_flex_padded"}:
            from torch.nn.attention.flex_attention import flex_attention, create_block_mask
            def mask_mod(b, h, q, k):
                return (k < args.key_steps) & mask[b, 0, 0, k.clamp_max(args.key_steps - 1)]
            block = create_block_mask(mask_mod, args.rows, 4, args.query_steps,
                args.key_steps, device=device, _compile=True)
            compiled_flex = torch.compile(flex_attention, dynamic=False)
            if case.endswith("padded"):
                def operation(q, k, v):
                    padding = max(0, 16 - q.shape[-1])
                    return compiled_flex(F.pad(q, (0, padding)), F.pad(k, (0, padding)),
                        F.pad(v, (0, padding)), block_mask=block, scale=scale)[..., :args.width]
            else:
                operation = lambda q, k, v: compiled_flex(q, k, v, block_mask=block, scale=scale)
    else:
        raise ValueError(case)
    if case.endswith(("compile", "autotune")):
        checkpoint_module = getattr(operation, "_checkpoint_module", None)
        options = {"triton.cudagraphs": False}
        if case.endswith("autotune"):
            options["max_autotune"] = True
        operation = torch.compile(operation, fullgraph=True, dynamic=False, options=options)
        if checkpoint_module is not None:
            operation._checkpoint_module = checkpoint_module
    return operation, reference, inputs, mask if case.startswith("attention") else None


def _errors(actual, expected):
    values = actual.float().flatten()
    difference = values - expected.float().flatten()
    reference = expected.float().flatten()
    denominator = reference.norm().clamp_min(1e-12)
    return {"max_abs": difference.abs().max().item(),
        "relative_l2": (difference.norm() / denominator).item(),
        "actual_norm": values.norm().item(), "reference_norm": reference.norm().item(),
        "cosine": F.cosine_similarity(values, reference, dim=0).item(),
        "finite": bool(torch.isfinite(actual).all())}


def _measure(function, repeats):
    for _ in range(3):
        function()
    torch.cuda.synchronize()
    start = time.perf_counter()
    for _ in range(repeats):
        function()
    torch.cuda.synchronize()
    eager_ms = (time.perf_counter() - start) * 1000 / repeats
    # Replay avoids measuring a starved GPU stream as kernel execution. Keep
    # eager wall timing too: graph-only speed does not prove epoch throughput.
    graph_ms, graph_error = None, None
    try:
        graph = torch.cuda.CUDAGraph()
        stream = torch.cuda.Stream()
        stream.wait_stream(torch.cuda.current_stream())
        with torch.cuda.stream(stream):
            for _ in range(3):
                function()
        torch.cuda.current_stream().wait_stream(stream)
        with torch.cuda.graph(graph):
            result = function()
        start_event, end_event = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
        start_event.record()
        for _ in range(max(100, repeats)):
            graph.replay()
        end_event.record(); end_event.synchronize()
        graph_ms = start_event.elapsed_time(end_event) / max(100, repeats)
        del result
    except Exception as exc:
        graph_error = f"{type(exc).__name__}: {exc}"
    return {"eager_forward_backward_ms": eager_ms,
        "graph_forward_backward_ms": graph_ms, "graph_error": graph_error}


def run(args):
    torch.cuda.set_device(args.device)
    torch.set_num_threads(4)
    torch.manual_seed(151)
    if args.case.startswith("adamw"):
        params = [nn.Parameter(torch.randn(1024, 512, device="cuda")) for _ in range(2)]
        for p in params:
            p.grad = torch.randn_like(p)
        fused = args.case.endswith("fused")
        optimizer = torch.optim.AdamW(params, lr=1e-4, fused=fused, foreach=not fused)
        for _ in range(3):
            optimizer.step()
        torch.cuda.synchronize()
        start = time.perf_counter()
        for _ in range(args.repeats):
            optimizer.step()
        torch.cuda.synchronize()
        return {"status": "measured", "optimizer_step_ms": (time.perf_counter() - start) * 1000 / args.repeats,
            "finite_parameters": all(bool(torch.isfinite(p).all()) for p in params),
            "parameter_count": sum(p.numel() for p in params)}
    operation, reference, inputs, key_mask = _operator(args)
    reference_inputs = tuple(x.detach().clone().requires_grad_() for x in inputs)
    from torch.nn.attention import sdpa_kernel, SDPBackend
    ref_context = sdpa_kernel(SDPBackend.MATH) if args.case.startswith("attention") else nullcontext()
    with ref_context:
        expected = reference(*reference_inputs)
        coefficients = torch.randn_like(expected)
        expected_grads = torch.autograd.grad(expected, reference_inputs, coefficients)
    backend = {"attention_math": SDPBackend.MATH, "attention_flash": SDPBackend.FLASH_ATTENTION,
        "attention_cudnn": SDPBackend.CUDNN_ATTENTION,
        "attention_flash_nomask": SDPBackend.FLASH_ATTENTION}.get(args.case)
    context = sdpa_kernel(backend) if backend is not None else nullcontext()
    with context:
        started = time.perf_counter()
        actual = operation(*inputs)
        gradients = torch.autograd.grad(actual, inputs, coefficients)
        torch.cuda.synchronize()
        compile_and_first_call_s = time.perf_counter() - started
        errors = {"output": _errors(actual, expected), "gradients": [
            _errors(a, b) for a, b in zip(gradients, expected_grads, strict=True)]}
        if not all(e["finite"] for e in [errors["output"], *errors["gradients"]]):
            raise FloatingPointError(f"nonfinite operator or gradient: {errors}")
        if any(e["reference_norm"] > 1e-12 and e["actual_norm"] <= 1e-12 for e in errors["gradients"]):
            raise ValueError(f"the operator lost a nonzero reference credit path: {errors}")
        # Loose numeric tolerance is not a permission to lose the derivative.
        approximate_training = "torchao" in args.case or "gemm_te_" in args.case
        numeric_review_required = any(e["relative_l2"] > .2 for e in errors["gradients"])
        if numeric_review_required and not approximate_training:
            raise ValueError(f"large adjoint discrepancy requires review: {errors}")
        if key_mask is not None:
            masked = ~key_mask.transpose(-2, -1).expand_as(inputs[1])
            for gradient in gradients[1:]:
                if gradient[masked].abs().max().item() > 1e-8:
                    raise ValueError(f"masked keys/values received an invalid derivative: max_abs={gradient[masked].abs().max().item()}")
            if actual[0].abs().max().item() > 1e-8:
                raise ValueError("fully masked attention row must have zero output")
        nonzero_gradient_inputs = [bool(g.abs().sum() > 0) for g in gradients]
        # Release the first autograd graph before switching capture streams.
        # Otherwise its AccumulateGrad nodes retain the legacy stream in 2.11.
        del actual, expected, gradients, expected_grads
        def function():
            output = operation(*inputs)
            return torch.autograd.grad(output, inputs, coefficients)
        timings = _measure(function, args.repeats)
    checkpoint = None
    layer = getattr(operation, "_checkpoint_module", None)
    if layer is not None:
        buffer = io.BytesIO()
        state = layer.state_dict()
        torch.save(state, buffer)
        checkpoint_bytes = buffer.tell()
        buffer.seek(0)
        restored = torch.load(buffer, map_location=inputs[0].device, weights_only=True)
        with torch.random.fork_rng(devices=[inputs[0].device]):
            replacement = type(layer)(args.width, args.output_width,
                params_dtype=torch.float32, device=inputs[0].device)
        replacement.load_state_dict(restored, strict=True)
        for name in ("weight", "bias"):
            torch.testing.assert_close(getattr(replacement, name), getattr(layer, name), rtol=0, atol=0)
        roundtrip = replacement(inputs[0])
        if not bool(torch.isfinite(roundtrip).all()):
            raise FloatingPointError("restored quantized checkpoint produced nonfinite output")
        checkpoint = {"strict_roundtrip": True, "fp32_master_parameters_exact": True,
            "state_keys": list(state), "serialized_bytes": checkpoint_bytes}
    return {"status": "measured", "compile_and_first_call_s": compile_and_first_call_s,
        "numeric_review_required": numeric_review_required,
        "shapes": [list(x.shape) for x in inputs], "errors": errors, **timings,
        "nonzero_gradient_inputs": nonzero_gradient_inputs, "checkpoint": checkpoint}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dtype", choices=["fp32", "bf16"], default="bf16")
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--width", type=int, default=32)
    parser.add_argument("--output-width", type=int, default=96)
    parser.add_argument("--query-steps", type=int, default=1)
    parser.add_argument("--key-steps", type=int, default=32)
    parser.add_argument("--device", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    record = {"contract": "frontier_operator_adjoint_probe_v1", "case": args.case,
        "dtype": args.dtype, "scope": "synthetic_operator_diagnostic_not_complete_fold",
        "torch": torch.__version__, "cuda_build": torch.version.cuda,
        "arguments": vars(args) | {"output": str(args.output)}}
    try:
        record.update(run(args))
    except Exception as exc:
        record.update(status="rejected", error=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc())
    atomic_write_json(args.output, record)
    print(json.dumps({k: v for k, v in record.items() if k not in {"traceback", "errors"}}), flush=True)
    return 0 if record["status"] == "measured" else 2


if __name__ == "__main__":
    raise SystemExit(main())
