#!/usr/bin/env python3
"""Benchmark multi-stock TCN throughput on a synthetic batch."""

from __future__ import annotations

import argparse
import time

import torch

from stockagent.models.multi_stock_tcn import CrossSectionalMultiStockTCN
from stockagent.training.loss import risk_aware_loss


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--lookback", type=int, default=16)
    parser.add_argument("--symbols", type=int, default=351)
    parser.add_argument("--features", type=int, default=21)
    parser.add_argument("--hidden-channels", type=int, default=32)
    parser.add_argument("--embedding-dim", type=int, default=32)
    parser.add_argument("--tcn-blocks", type=int, default=4)
    parser.add_argument("--kernel-size", type=int, default=3)
    parser.add_argument("--head-hidden-dim", type=int, default=32)
    parser.add_argument("--head-layers", type=int, default=1)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--tcn-conv-mode", default="separable")
    parser.add_argument("--conv-layers-per-block", type=int, default=1)
    parser.add_argument("--norm-type", default="none")
    parser.add_argument("--sanitize-inputs", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--compile-model", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--compile-loss", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--long-only", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--steps", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required for a useful throughput benchmark.")

    torch.backends.cudnn.benchmark = True
    device = torch.device("cuda")
    model = CrossSectionalMultiStockTCN(
        lookback=args.lookback,
        num_features=args.features,
        num_symbols=args.symbols,
        hidden_channels=args.hidden_channels,
        embedding_dim=args.embedding_dim,
        tcn_blocks=args.tcn_blocks,
        tcn_kernel_size=args.kernel_size,
        head_hidden_dim=args.head_hidden_dim,
        head_layers=args.head_layers,
        dropout=args.dropout,
        tcn_conv_mode=args.tcn_conv_mode,
        conv_layers_per_block=args.conv_layers_per_block,
        norm_type=args.norm_type,
        sanitize_inputs=args.sanitize_inputs,
        long_only=args.long_only,
    ).to(device)
    loss_fn = risk_aware_loss
    if args.compile_model:
        model = torch.compile(model, mode="reduce-overhead", dynamic=False)
    if args.compile_loss:
        loss_fn = torch.compile(risk_aware_loss, mode="reduce-overhead", dynamic=False)
    x = torch.randn(args.batch_size, args.lookback, args.symbols, args.features, device=device)
    tradable_mask = torch.ones(args.batch_size, args.symbols, dtype=torch.bool, device=device)
    future_log_returns = torch.randn(args.batch_size, args.symbols, device=device) * 0.01
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4)

    for _ in range(args.warmup):
        optimizer.zero_grad(set_to_none=True)
        weights = model(x, tradable_mask)
        loss = loss_fn(
            weights,
            future_log_returns,
            tradable_mask,
            objective="sharpe",
            long_only=args.long_only,
        )
        loss.backward()
        optimizer.step()
    torch.cuda.synchronize(device)

    forward_ms = 0.0
    backward_ms = 0.0
    full_ms = 0.0
    for _ in range(args.steps):
        optimizer.zero_grad(set_to_none=True)
        torch.cuda.synchronize(device)
        start = time.perf_counter()
        weights = model(x, tradable_mask)
        loss = loss_fn(
            weights,
            future_log_returns,
            tradable_mask,
            objective="sharpe",
            long_only=args.long_only,
        )
        torch.cuda.synchronize(device)
        mid = time.perf_counter()
        loss.backward()
        optimizer.step()
        torch.cuda.synchronize(device)
        end = time.perf_counter()
        forward_ms += (mid - start) * 1000.0
        backward_ms += (end - mid) * 1000.0
        full_ms += (end - start) * 1000.0

    denom = float(max(1, args.steps))
    mean_full_ms = full_ms / denom
    print(
        {
            "batch_size": args.batch_size,
            "lookback": args.lookback,
            "symbols": args.symbols,
            "features": args.features,
            "hidden_channels": args.hidden_channels,
            "embedding_dim": args.embedding_dim,
            "tcn_blocks": args.tcn_blocks,
            "tcn_conv_mode": args.tcn_conv_mode,
            "conv_layers_per_block": args.conv_layers_per_block,
            "norm_type": args.norm_type,
            "sanitize_inputs": args.sanitize_inputs,
            "compile_model": args.compile_model,
            "compile_loss": args.compile_loss,
            "forward_loss_ms": forward_ms / denom,
            "backward_step_ms": backward_ms / denom,
            "full_ms": mean_full_ms,
            "samples_per_s": args.batch_size / mean_full_ms * 1000.0,
        }
    )


if __name__ == "__main__":
    main()
