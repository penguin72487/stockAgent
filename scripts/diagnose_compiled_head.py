"""Isolate an Inductor head adjoint with the actual two-rank captured inputs.

This is a read-only component experiment, not full-fold or performance
acceptance. It uses the frozen canonical model and makes no optimizer update.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
import time


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", default="full,context,allocation")
    parser.add_argument("--options", default="{}")
    parser.add_argument("--matmul-precision", choices=("high", "highest"), default="high")
    args = parser.parse_args()
    if int(os.environ.get("WORLD_SIZE", "1")) != 2:
        parser.error("requires two torchrun ranks; component proof does not replace canonical DDP")
    if "training_diagnostics" not in args.output.resolve().parts:
        parser.error("output must be isolated under training_diagnostics")
    sys.path.insert(0, str(args.source_root.resolve()))
    import torch
    import torch.distributed as dist
    import torch._dynamo.config as dynamo_config
    import torch._functorch.config as aot_config
    from stockagent.config import load_config
    from stockagent.models.factory import build_model
    from stockagent.models.factorized_input import _partition_compile_options
    from stockagent.models.transformer_base_portfolio import _safe_attention_mask
    from train import _configure_cuda_runtime

    rank = int(os.environ["RANK"])
    torch.cuda.set_device(rank)
    torch.set_num_threads(4)
    dist.init_process_group("nccl", device_id=torch.device("cuda", rank))
    if args.output.exists():
        raise FileExistsError(args.output)
    cfg = load_config(args.config)
    _configure_cuda_runtime(use_tensor_cores=cfg.environment.use_tensor_cores)
    torch.set_float32_matmul_precision(args.matmul_precision)
    torch.backends.cuda.matmul.allow_tf32 = args.matmul_precision != "highest"
    snapshot = torch.load(args.snapshot_root / f"head-snapshot-rank{rank}.pt", weights_only=True)
    embeddings = snapshot["embeddings"].cuda()
    mask = snapshot["mask"].cuda()
    upstream = snapshot["upstream_gradient"].cuda()
    model = build_model(config=cfg, lookback=cfg.training.lookback,
                        num_features=14726, num_symbols=int(embeddings.size(1))).cuda().train()
    model.load_state_dict(snapshot["model_state"], strict=True)

    @contextmanager
    def compilation_contract():
        with dynamo_config.patch(optimize_ddp=False), aot_config.patch(backward_pass_autocast="off"):
            yield

    def context(x, legal):
        safe = _safe_attention_mask(legal)
        return model._forward_latent_or_market_from_stock_embeddings(
            x, safe, use_latent=True, use_market=True, collect_aux=False)[0]

    def allocation(z, legal):
        return model._portfolio_outputs_from_stock_embeddings(
            z, legal, {}, temperature=None, return_aux=False,
            return_scores=False, portfolio_context=None)

    def run(fn):
        model.zero_grad(set_to_none=True)
        x = embeddings.detach().clone().requires_grad_(True)
        torch.cuda.synchronize()
        begin = time.perf_counter()
        with compilation_contract(), torch.autocast("cuda", dtype=torch.bfloat16):
            output = fn(x, mask)
        output.backward(upstream)
        torch.cuda.synchronize()
        wall = time.perf_counter() - begin
        return (output.detach(), x.grad.detach(),
                {name: p.grad.detach().clone() for name, p in model.named_parameters() if p.grad is not None},
                wall)

    attention_capture = {}
    def capture_attention(module, inputs, keywords, output):
        attention_capture.update(query=inputs[0].detach().clone(),
                                 context=inputs[1].detach().clone(),
                                 mask=keywords["key_mask"].detach().clone())
        output.register_hook(lambda grad: attention_capture.update(upstream=grad.detach().clone()))
    first_attention = model.latent_blocks[0].attn
    capture_hook = first_attention.register_forward_hook(capture_attention, with_kwargs=True)
    expected, expected_dx, expected_dp, _ = run(model._forward_stock_embeddings)
    capture_hook.remove()
    options = {**_partition_compile_options(model), **json.loads(args.options)}
    records = []
    for case in args.cases.split(","):
        if case.startswith("attention"):
            from stockagent.models.transformer_base_portfolio import _rms_normalize_last_dim
            import torch.nn.functional as F
            def contiguous_attention(query, context, key_mask):
                q, k, v = first_attention._project_cross(query, context)
                if first_attention.qk_norm:
                    q = _rms_normalize_last_dim(q)
                    k = _rms_normalize_last_dim(k)
                if case in {"attention_sequence", "attention_tile"}:
                    extra_keys = (-k.size(-2)) % 128
                    k = F.pad(k, (0,0,0,extra_keys))
                    v = F.pad(v, (0,0,0,extra_keys))
                    key_mask = F.pad(key_mask, (0,extra_keys), value=False)
                if case in {"attention_tile", "attention_channels"}:
                    extra_channels = (-q.size(-1)) % 16
                    q = F.pad(q, (0,extra_channels))
                    k = F.pad(k, (0,extra_channels))
                    v = F.pad(v, (0,extra_channels))
                q = q.contiguous(); k = k.contiguous(); v = v.contiguous()
                y = F.scaled_dot_product_attention(q, k, v, attn_mask=key_mask[:,None,None,:],
                                                  dropout_p=0., scale=first_attention.scale)
                y = y[..., :first_attention.head_dim]
                y = y.transpose(1,2).contiguous().reshape(query.size(0), query.size(1), first_attention.dim)
                return first_attention.out_proj(y)
            def attention_run(fn):
                first_attention.zero_grad(set_to_none=True)
                q = attention_capture["query"].detach().clone().requires_grad_(True)
                k = attention_capture["context"].detach().clone().requires_grad_(True)
                begin = time.perf_counter()
                with compilation_contract(), torch.autocast("cuda", enabled=False):
                    out = fn(q,k,attention_capture["mask"])
                out.backward(attention_capture["upstream"])
                torch.cuda.synchronize()
                return out.detach(),q.grad,k.grad,{n:p.grad.detach().clone() for n,p in first_attention.named_parameters()},time.perf_counter()-begin
            oracle = attention_run(first_attention)
            fn = first_attention if case == "attention" else contiguous_attention
            compiled = torch.compile(fn, dynamic=False, fullgraph=True, options=options)
            output, dq, dk, dp, wall = attention_run(compiled)
            record={"rank":rank,"case":case,"forward_max_abs":float((output-oracle[0]).abs().max()),
                    "query_grad_max_abs":float((dq-oracle[1]).abs().max()),
                    "context_grad_max_abs":float((dk-oracle[2]).abs().max()),
                    "parameter_grad_max_abs": {n:float((dp[n]-p).abs().max()) for n,p in oracle[3].items()},
                    "first_call_s":wall,"acceptance_scope":"first_latent_attention_only"}
            print(json.dumps(record,allow_nan=False),flush=True); records.append(record)
            continue
        if case == "full":
            fn = torch.compile(model._forward_stock_embeddings, dynamic=False, fullgraph=True, options=options)
        elif case == "full_tiles":
            import torch.nn.functional as F
            native_sdpa = F.scaled_dot_product_attention
            def aligned_sdpa(q,k,v,attn_mask=None,dropout_p=0.,is_causal=False,scale=None,**kwargs):
                if q.dtype == torch.float32 and q.size(-1) < 8:
                    channels = q.size(-1)
                    extra_keys = (-k.size(-2)) % 128
                    key_count = k.size(-2)
                    q = F.pad(q, (0,16-channels))
                    k = F.pad(k, (0,16-channels,0,extra_keys))
                    v = F.pad(v, (0,16-channels,0,extra_keys))
                    if attn_mask is None:
                        attn_mask = torch.arange(key_count+extra_keys,device=q.device) < key_count
                    else:
                        attn_mask = F.pad(attn_mask,(0,extra_keys),value=False)
                    return native_sdpa(q.contiguous(),k.contiguous(),v.contiguous(),attn_mask=attn_mask,
                                       dropout_p=dropout_p,is_causal=is_causal,
                                       scale=channels**-.5 if scale is None else scale,**kwargs)[..., :channels]
                return native_sdpa(q,k,v,attn_mask=attn_mask,dropout_p=dropout_p,is_causal=is_causal,scale=scale,**kwargs)
            F.scaled_dot_product_attention = aligned_sdpa
            fn = torch.compile(model._forward_stock_embeddings, dynamic=False, fullgraph=True, options=options)
        elif case == "context":
            part = torch.compile(context, dynamic=False, fullgraph=True, options=options)
            def fn(x, legal, part=part):
                return allocation(part(x, legal), legal)
        elif case == "allocation":
            part = torch.compile(allocation, dynamic=False, fullgraph=True, options=options)
            def fn(x, legal, part=part):
                return part(context(x, legal), legal)
        elif case == "split":
            c = torch.compile(context, dynamic=False, fullgraph=True, options=options)
            a = torch.compile(allocation, dynamic=False, fullgraph=True, options=options)
            def fn(x, legal, c=c, a=a):
                return a(c(x, legal), legal)
        else:
            raise ValueError(case)
        actual, dx, dp, wall = run(fn)
        errors = [(name, float((dp[name]-grad).abs().max().item()))
                  for name, grad in expected_dp.items()]
        delta = torch.stack([torch.linalg.vector_norm((dp[name]-grad).double())
                             for name, grad in expected_dp.items()]).norm().item()
        norm = torch.stack([torch.linalg.vector_norm(grad.double())
                            for grad in expected_dp.values()]).norm().item()
        dx_delta = (dx-expected_dx).double().norm().item()
        dx_norm = expected_dx.double().norm().item()
        finite = bool(torch.isfinite(dx).all().item()) and all(bool(torch.isfinite(g).all().item()) for g in dp.values())
        record = {"rank": rank, "case": case, "compile_options": options,
                  "forward_max_abs": float((actual-expected).abs().max().item()),
                  "input_grad_relative_l2": dx_delta / max(dx_norm, 1e-30) if finite else None,
                  "parameter_grad_relative_l2": delta / max(norm, 1e-30) if finite else None,
                  "largest_parameter_errors": sorted(errors, key=lambda v: v[1], reverse=True)[:8] if finite else [],
                  "oracle_parameter_grad_norm": norm, "first_call_s": wall,
                  "finite": finite,
                  "acceptance_scope": "captured_head_component_only"}
        print(json.dumps(record, allow_nan=False), flush=True)
        records.append(record)
    gathered = [None, None]
    dist.all_gather_object(gathered, records)
    if rank == 0:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as handle:
            handle.write(json.dumps({"world_size": 2, "ranks": gathered}, allow_nan=False, indent=2)+"\n")
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
