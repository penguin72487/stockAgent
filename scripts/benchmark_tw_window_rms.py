#!/usr/bin/env python3
"""Bounded model-path comparison; never loads the full TW panel or trains folds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import sys
import time

import torch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config  # noqa: E402
from stockagent.models.factory import build_model  # noqa: E402
CONTROLS = {
    "train_only_rms": "tw_public_preopen_all_observed_multibasis_rms_2014_v1.yaml",
    "window_rms": "tw_public_preopen_all_observed_multibasis_window_rms_2014_v1.yaml",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", type=int, default=64)
    parser.add_argument("--batch", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--feature-names", type=Path, required=True)
    args = parser.parse_args()
    if args.symbols < 1 or args.batch < 1 or args.repeat < 1:
        parser.error("symbols, batch and repeat must be positive")
    names = json.loads(args.feature_names.read_text())
    if len(names) != 428 or len(set(names)) != len(names):
        parser.error("feature-names must be the exact 428-column v3 model ABI")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    torch.manual_seed(42)
    rows = args.batch + 32 - 1
    slab = torch.randn(rows, args.symbols, len(names), device=device)
    for index, name in enumerate(names):
        if name.endswith("__available"):
            slab[..., index] = (torch.rand(rows, args.symbols, device=device) > 0.25).float()
    for index, name in enumerate(names):
        if f"{name}__available" in names:
            flag_index = names.index(f"{name}__available")
            slab[..., index] *= slab[..., flag_index]
    mask = torch.ones(args.batch, args.symbols, dtype=torch.bool, device=device)
    target = torch.randn(args.batch, args.symbols, device=device)
    results = {}
    for label, config_name in CONTROLS.items():
        config = load_config(ROOT / "configs" / "markets" / config_name)
        model = build_model(
            config=config,
            lookback=32,
            num_features=len(names),
            num_symbols=args.symbols,
            feature_names=names,
        ).to(device).train()
        times = []
        if device.type == "cuda":
            torch.cuda.reset_peak_memory_stats(device)
        for step in range(args.repeat + 2):
            model.zero_grad(set_to_none=True)
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            started = time.perf_counter()
            with torch.autocast(device_type=device.type, dtype=torch.bfloat16, enabled=device.type == "cuda"):
                output = model.forward_from_panel_slab(slab, mask, return_aux=False)
                loss = (output.float() * target).sum()
            loss.backward()
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            if step >= 2:
                times.append(time.perf_counter() - started)
        results[label] = {
            "median_forward_backward_s": statistics.median(times),
            "min_forward_backward_s": min(times),
            "peak_allocated_mib": (
                torch.cuda.max_memory_allocated(device) / (1024 * 1024)
                if device.type == "cuda" else None
            ),
        }
        del model
        if device.type == "cuda":
            torch.cuda.empty_cache()
    print(json.dumps({
        "device": str(device), "batch": args.batch, "symbols": args.symbols,
        "lookback": 32, "features": len(names), "repeat": args.repeat,
        "measurement": "slab model forward plus backward; excludes data I/O and executor",
        "results": results,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
