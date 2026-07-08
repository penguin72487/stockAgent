#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

import torch
import torch.distributed as dist
import torch.distributed.nn.functional as dist_nn_f
try:
    import torch.distributed._functional_collectives as dist_fc
except Exception:  # pragma: no cover - older torch fallback
    dist_fc = None
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP


@dataclass(frozen=True)
class ProbeResult:
    scale: float
    max_abs_diff: float
    max_rel_diff: float
    ddp_grad_norm: float
    baseline_grad_norm: float


class TinyRankModel(nn.Module):
    def __init__(self, in_dim: int, out_dim: int) -> None:
        super().__init__()
        self.proj = nn.Linear(in_dim, out_dim, bias=False)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.proj(x).tanh()


def _env_rank() -> tuple[int, int, int]:
    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    local_rank = int(os.environ.get("LOCAL_RANK", str(rank)))
    return rank, world_size, local_rank


def _device(local_rank: int, prefer_cuda: bool) -> torch.device:
    if prefer_cuda and torch.cuda.is_available():
        torch.cuda.set_device(local_rank)
        return torch.device("cuda", local_rank)
    return torch.device("cpu")


def _init_process_group(device: torch.device) -> None:
    backend = "nccl" if device.type == "cuda" else "gloo"
    dist.init_process_group(backend=backend)


def _sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)
        try:
            dist.barrier(device_ids=[device.index if device.index is not None else torch.cuda.current_device()])
            return
        except TypeError:
            pass
    dist.barrier()


def _all_gather_autograd(x: torch.Tensor) -> torch.Tensor:
    if dist_fc is not None and hasattr(dist_fc, "all_gather_tensor"):
        return dist_fc.all_gather_tensor(x, 0, group=dist.group.WORLD)
    parts = dist_nn_f.all_gather(x)
    return torch.cat(tuple(parts), dim=0)


@torch.no_grad()
def _broadcast_baseline_grad(
    grad: torch.Tensor,
    device: torch.device,
    src: int = 0,
) -> torch.Tensor:
    grad = grad.to(device=device)
    dist.broadcast(grad, src=src)
    return grad


def _baseline_grad(
    weight: torch.Tensor,
    full_x: torch.Tensor,
    full_target: torch.Tensor,
    *,
    scale: float,
) -> torch.Tensor:
    model = TinyRankModel(full_x.size(-1), full_target.size(-1)).to(device=full_x.device)
    with torch.no_grad():
        model.proj.weight.copy_(weight)
    output = model(full_x)
    loss = (output - full_target).square().mean() * float(scale)
    loss.backward()
    return model.proj.weight.grad.detach().clone()


def run_probe(*, device: torch.device, scale: float, rows_per_rank: int, in_dim: int, out_dim: int) -> ProbeResult:
    rank, world_size, _ = _env_rank()
    torch.manual_seed(20260707)
    model = TinyRankModel(in_dim, out_dim).to(device)
    ddp_model = DDP(
        model,
        device_ids=[device.index] if device.type == "cuda" else None,
        output_device=device.index if device.type == "cuda" else None,
        bucket_cap_mb=1,
        find_unused_parameters=False,
    )

    total_rows = rows_per_rank * world_size
    full_x = torch.randn(total_rows, in_dim, device=device)
    full_target = torch.randn(total_rows, out_dim, device=device)
    local_start = rank * rows_per_rank
    local_end = local_start + rows_per_rank
    local_x = full_x[local_start:local_end].contiguous()

    _sync(device)
    output_local = ddp_model(local_x)
    output_full = _all_gather_autograd(output_local)
    loss = (output_full - full_target).square().mean() * float(scale)
    loss.backward()
    _sync(device)

    ddp_grad = model.proj.weight.grad.detach().clone()
    if rank == 0:
        base_grad = _baseline_grad(
            model.proj.weight.detach().clone(),
            full_x.detach(),
            full_target.detach(),
            scale=1.0,
        )
    else:
        base_grad = torch.empty_like(ddp_grad)
    base_grad = _broadcast_baseline_grad(base_grad, device=device, src=0)
    diff = ddp_grad - base_grad
    max_abs = float(diff.abs().max().detach().cpu().item())
    denom = torch.maximum(base_grad.abs(), torch.full_like(base_grad, 1e-12))
    max_rel = float((diff.abs() / denom).max().detach().cpu().item())
    return ProbeResult(
        scale=float(scale),
        max_abs_diff=max_abs,
        max_rel_diff=max_rel,
        ddp_grad_norm=float(ddp_grad.norm().detach().cpu().item()),
        baseline_grad_norm=float(base_grad.norm().detach().cpu().item()),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Probe DDP + autograd all_gather gradient scaling for the stockAgent async-backprop design."
        )
    )
    parser.add_argument("--cpu", action="store_true", help="Use CPU/gloo even when CUDA is available.")
    parser.add_argument("--rows-per-rank", type=int, default=4)
    parser.add_argument("--in-dim", type=int, default=7)
    parser.add_argument("--out-dim", type=int, default=5)
    args = parser.parse_args()

    rank, world_size, local_rank = _env_rank()
    if world_size < 2:
        raise RuntimeError("Launch with torchrun, for example: torchrun --standalone --nproc_per_node=2 ...")

    device = _device(local_rank, prefer_cuda=not args.cpu)
    _init_process_group(device)
    try:
        candidate_scales = (1.0 / float(world_size), 1.0, float(world_size))
        results = [
            run_probe(
                device=device,
                scale=scale,
                rows_per_rank=max(1, int(args.rows_per_rank)),
                in_dim=max(1, int(args.in_dim)),
                out_dim=max(1, int(args.out_dim)),
            )
            for scale in candidate_scales
        ]
        if rank == 0:
            print("[ddp-probe] DDP + autograd all_gather gradient scaling")
            print(f"[ddp-probe] world_size={world_size} device={device.type}")
            for result in results:
                print(
                    "[ddp-probe] "
                    f"loss_scale={result.scale:g} "
                    f"max_abs_diff={result.max_abs_diff:.6e} "
                    f"max_rel_diff={result.max_rel_diff:.6e} "
                    f"ddp_grad_norm={result.ddp_grad_norm:.6e} "
                    f"baseline_grad_norm={result.baseline_grad_norm:.6e}"
                )
            best = min(results, key=lambda item: item.max_abs_diff)
            print(f"[ddp-probe] recommended_loss_scale={best.scale:g}")
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
