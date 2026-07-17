#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

import torch
import torch.distributed as dist
import torch.distributed.nn.functional as dist_nn_f
try:
    import torch.distributed._functional_collectives as dist_fc
except Exception:  # pragma: no cover - older torch fallback
    dist_fc = None
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _PanelSlabForwardWrapper
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel


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


class TinyPanelSlabModel(nn.Module):
    def __init__(self, lookback: int, features: int) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.proj = nn.Linear(int(features), 1, bias=False)

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool = False,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        del return_aux, symbol_indices
        windows = feature_slab.unfold(0, self.lookback, 1).permute(0, 3, 1, 2)
        scores = self.proj(windows.mean(dim=1)).squeeze(-1)
        return scores.masked_fill(~mask, 0.0)


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


def run_panel_slab_probe(*, device: torch.device, rows_per_rank: int) -> tuple[float, float]:
    rank, world_size, _ = _env_rank()
    lookback, symbols, features = 4, 7, 3
    total_rows = int(rows_per_rank) * world_size
    torch.manual_seed(20260708)
    model = TinyPanelSlabModel(lookback, features).to(device)
    slab_model: nn.Module = _PanelSlabForwardWrapper(model)
    if device.type == "cuda":
        slab_model = torch.compile(
            slab_model,
            fullgraph=True,
            dynamic=False,
            options={"triton.cudagraphs": False},
        )
    ddp_model = DDP(
        slab_model,
        device_ids=[device.index] if device.type == "cuda" else None,
        output_device=device.index if device.type == "cuda" else None,
        bucket_cap_mb=1,
        find_unused_parameters=False,
        static_graph=True,
    )

    full_panel = torch.randn(total_rows + lookback - 1, symbols, features, device=device)
    future_returns = torch.randn(total_rows, symbols, device=device) * 0.01
    tradable = torch.ones(total_rows, symbols, dtype=torch.bool, device=device)
    can_short_open = tradable.clone()
    can_short_open[:, -1] = False
    force_cover = torch.zeros_like(tradable)
    force_cover[total_rows // 2, 1] = True
    force_exit = torch.zeros_like(tradable)
    volume_limit = torch.full((total_rows, symbols), 0.20, device=device)
    benchmark = future_returns.mean(dim=1)
    local_start = rank * int(rows_per_rank)
    local_end = local_start + int(rows_per_rank)
    local_slab = full_panel[local_start : local_end + lookback - 1].contiguous()
    local_mask = tradable[local_start:local_end].contiguous()

    losses: list[float] = []
    for _ in range(2):
        ddp_model.zero_grad(set_to_none=True)
        local_weights = ddp_model(local_slab, local_mask, None)
        weights = _all_gather_autograd(local_weights.contiguous())
        aux = {"initial_weights": torch.zeros(symbols, device=device)}
        loss = risk_aware_loss(
            weights,
            future_returns,
            tradable,
            benchmark_returns=benchmark,
            can_buy_mask=tradable,
            can_sell_mask=tradable,
            can_short_open_mask=can_short_open,
            force_short_cover_mask=force_cover,
            force_exit_mask=force_exit,
            volume_limit_weights=volume_limit,
            long_only=False,
            buy_fee_rate=0.001425,
            sell_fee_rate=0.004425,
            objective="log_utility",
            aux_outputs=aux,
        )
        loss.backward()
        _sync(device)
        if model.proj.weight.grad is None or not bool(torch.isfinite(model.proj.weight.grad).all()):
            raise RuntimeError("DDP panel-slab fullgraph produced a missing or non-finite gradient")
        losses.append(float(loss.detach().cpu()))
    return losses[0], losses[1]


def run_transformer_panel_slab_probe(*, device: torch.device, rows_per_rank: int) -> tuple[float, float, float, int]:
    """Exercise the actual market-token Transformer compile-before-DDP path."""
    rank, world_size, _ = _env_rank()
    lookback, symbols, features = 4, 16, 6
    total_rows = int(rows_per_rank) * world_size
    torch.manual_seed(20260709)
    model = TransformerBasePortfolioModel(
        lookback=lookback,
        num_features=features,
        num_symbols=symbols,
        d_model=16,
        attention_mode="market_token",
        use_flash_attention=True,
        use_time_pos=True,
        use_symbol_pos=True,
        input_dropout=0.0,
        sdpa_batch_limit=4096,
        norm_type="rmsnorm",
        ffn_type="swiglu",
        qk_norm=False,
        rope_temporal=True,
        temporal_layers=1,
        temporal_heads=4,
        temporal_ffn_mult=2,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        cross_layers=0,
        joint_layers=0,
        latent_layers=0,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=16,
        head_layers=1,
        dropout=0.0,
        portfolio_mode="long_short",
        portfolio_activation="identity",
        portfolio_output_mode="logits",
        checkpoint_blocks=False,
        return_aux=False,
        return_aux_details=False,
        runtime_shape_check=False,
        allow_dynamic_symbols=False,
    ).to(device)
    slab_model: nn.Module = _PanelSlabForwardWrapper(model)
    if device.type == "cuda":
        slab_model = torch.compile(
            slab_model,
            fullgraph=True,
            dynamic=False,
            options={"triton.cudagraphs": False},
        )
    ddp_model = DDP(
        slab_model,
        device_ids=[device.index] if device.type == "cuda" else None,
        output_device=device.index if device.type == "cuda" else None,
        bucket_cap_mb=1,
        find_unused_parameters=False,
        static_graph=True,
    )

    full_panel = torch.randn(total_rows + lookback - 1, symbols, features, device=device)
    future_returns = torch.randn(total_rows, symbols, device=device) * 0.01
    tradable = torch.ones(total_rows, symbols, dtype=torch.bool, device=device)
    tradable[:, -2:] = False
    can_buy = tradable.clone()
    can_sell = tradable.clone()
    can_short_open = tradable.clone()
    can_short_open[:, 0] = False
    force_cover = torch.zeros_like(tradable)
    force_cover[total_rows // 2, 1] = True
    force_exit = torch.zeros_like(tradable)
    volume_limit = torch.full((total_rows, symbols), 0.15, device=device)
    benchmark = future_returns.masked_fill(~tradable, 0.0).sum(dim=1) / tradable.sum(dim=1).clamp_min(1)
    local_start = rank * int(rows_per_rank)
    local_end = local_start + int(rows_per_rank)
    local_slab = full_panel[local_start : local_end + lookback - 1].contiguous()
    local_mask = tradable[local_start:local_end].contiguous()
    def amp_context():
        return (
            torch.autocast(device_type="cuda", dtype=torch.bfloat16)
            if device.type == "cuda"
            else nullcontext()
        )

    losses: list[float] = []
    grad_norm = 0.0
    missing_grad_count = 0
    for _ in range(2):
        ddp_model.zero_grad(set_to_none=True)
        with amp_context():
            local_weights = ddp_model(local_slab, local_mask, None)
            weights = _all_gather_autograd(local_weights.contiguous())
            aux = {"initial_weights": torch.zeros(symbols, device=device)}
            loss = risk_aware_loss(
                weights,
                future_returns,
                tradable,
                benchmark_returns=benchmark,
                can_buy_mask=can_buy,
                can_sell_mask=can_sell,
                can_short_open_mask=can_short_open,
                force_short_cover_mask=force_cover,
                force_exit_mask=force_exit,
                volume_limit_weights=volume_limit,
                long_only=False,
                buy_fee_rate=0.001425,
                sell_fee_rate=0.004425,
                objective="log_utility",
                aux_outputs=aux,
                direction_weight=0.0,
                volatility_regime_weight=0.0,
                concentration_weight=0.0,
            )
        loss.backward()
        _sync(device)
        trainable = [parameter for parameter in model.parameters() if parameter.requires_grad]
        grads = [parameter.grad for parameter in trainable if parameter.grad is not None]
        missing_grad_count = len(trainable) - len(grads)
        if not grads or any(not bool(torch.isfinite(grad).all()) for grad in grads):
            raise RuntimeError("Transformer DDP panel-slab produced no usable gradient or a non-finite gradient")
        grad_norm = float(torch.sqrt(sum(grad.float().pow(2).sum() for grad in grads)).detach().cpu())
        losses.append(float(loss.detach().cpu()))
    return losses[0], losses[1], grad_norm, missing_grad_count


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
    parser.add_argument(
        "--panel-slab",
        action="store_true",
        help="Also run two canonical-loss backwards through compile-before-DDP panel-slab forward.",
    )
    parser.add_argument(
        "--transformer-panel-slab",
        action="store_true",
        help=(
            "Also run two BF16 canonical-loss backwards through the real market-token "
            "TransformerBasePortfolioModel compile-before-DDP panel-slab path."
        ),
    )
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
        if args.panel_slab:
            panel_losses = run_panel_slab_probe(
                device=device,
                rows_per_rank=max(1, int(args.rows_per_rank)),
            )
            if rank == 0:
                print(
                    "[ddp-probe] panel_slab_fullgraph=ok "
                    f"replay_losses=({panel_losses[0]:.8f},{panel_losses[1]:.8f})"
                )
        if args.transformer_panel_slab:
            transformer_result = run_transformer_panel_slab_probe(
                device=device,
                rows_per_rank=max(1, int(args.rows_per_rank)),
            )
            if rank == 0:
                print(
                    "[ddp-probe] transformer_market_token_panel_slab_fullgraph=ok "
                    f"replay_losses=({transformer_result[0]:.8f},{transformer_result[1]:.8f}) "
                    f"grad_norm={transformer_result[2]:.8f} "
                    f"structurally_unused_parameters={transformer_result[3]}"
                )
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
