#!/usr/bin/env python3
"""Verify lossless blocked physical staging on the selected two CUDA ranks.

This isolated transport oracle does not run a model, advance an optimizer, or
replace complete-fold DDP acceptance. The dense 270-minute source ABI stays
identical; only the temporary padded-packet live set is varied.
"""
from __future__ import annotations

import argparse
from dataclasses import fields
from datetime import timedelta
import gc
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--code-root", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--rows", type=int, default=48)
    p.add_argument("--symbols", type=int, default=2757)
    p.add_argument("--event-minutes", type=int, default=120)
    p.add_argument("--block-rows", type=int, default=16)
    a = p.parse_args()
    sys.path.insert(0, str(a.code_root.resolve(strict=True)))
    import torch
    from downloader.artifact_io import atomic_write_json
    from stockagent.storage_layout import admit_output
    from stockagent.training.day_trade_carry_bridge import (
        PackedDayTradeCarrySession, PreparedDayTradeCarryBatch,
    )

    admit_output(a.output, "benchmark")
    if min(a.rows, a.symbols, a.block_rows) <= 0 or not 1 <= a.event_minutes <= 270:
        raise ValueError("positive bounded rows/symbols/block and 1..270 event minutes required")
    rank = int(os.environ["LOCAL_RANK"])
    if torch.cuda.device_count() != 2:
        raise RuntimeError("this oracle requires the selected two CUDA devices")
    torch.cuda.set_device(rank)
    torch.set_num_threads(4)
    torch.distributed.init_process_group("nccl", timeout=timedelta(minutes=10))
    if torch.distributed.get_world_size() != 2:
        raise RuntimeError("this oracle requires both ranks")
    device = torch.device("cuda", rank)
    output = a.output / f"rank_{rank}"
    output.mkdir(parents=True, exist_ok=False)
    sources = []
    vector = torch.linspace(10, 100, a.symbols, dtype=torch.float64)
    vector[0] = -0.0
    vector[-1] = float("nan")
    marks = torch.arange(a.symbols * 270, dtype=torch.float64).reshape(a.symbols, 270)
    marks[0, 0] = -0.0
    marks[-1, -1] = float("nan")
    optional = {"day", "marks", "exit_flat", "exit_price", "exit_capacity",
                "terminal_liquidation_price", "entry_path", "stop_hits"}
    for i in range(a.rows):
        # Empty days and differing event lengths exercise the sentinel padding.
        events = 0 if i == 0 else a.symbols * a.event_minutes * (1 + i % 2)
        flat = torch.arange(events, dtype=torch.int64)
        price = torch.arange(events, dtype=torch.float64)
        if events:
            price[0] = -0.0
            price[-1] = float("nan")
        values = {f.name: vector for f in fields(PackedDayTradeCarrySession) if f.name not in optional}
        sources.append(PackedDayTradeCarrySession(
            day=730000 + i, **values, marks=marks, exit_flat=flat,
            exit_price=price, exit_capacity=price,
            terminal_liquidation_price=vector,
        ))
    staged = PreparedDayTradeCarryBatch.from_packed_sessions(
        tuple(sources), a.rows, event_compression=False,
    )
    del sources
    gc.collect()

    def digest(sessions):
        h = hashlib.sha256()
        for session in sessions:
            h.update(str(session.day).encode())
            for f in fields(session):
                value = getattr(session, f.name)
                h.update(f.name.encode())
                if isinstance(value, torch.Tensor):
                    h.update(str((tuple(value.shape), str(value.dtype))).encode())
                    h.update(value.detach().cpu().contiguous().numpy().tobytes())
                else:
                    h.update(str(value).encode())
        return h.hexdigest()

    def source_digest():
        h = hashlib.sha256()
        for name, value in sorted(staged.tensors.items()):
            h.update(name.encode())
            h.update(value.contiguous().numpy().tobytes())
        return h.hexdigest()

    source_before = source_digest()
    result = {"scope": "two_rank_transport_only_no_optimizer", "rows": a.rows,
              "symbols": a.symbols, "event_minutes": a.event_minutes,
              "cuda_device": str(device), "variants": []}
    for block in (0, a.block_rows):
        torch.cuda.empty_cache()
        torch.cuda.synchronize(device)
        torch.cuda.reset_peak_memory_stats(device)
        begin = time.perf_counter()
        sessions = staged.sessions(device, transport_block_rows=block)
        torch.cuda.synchronize(device)
        wall = time.perf_counter() - begin
        peak = torch.cuda.max_memory_allocated(device)
        result["variants"].append({"block_rows": block, "wall_s": wall,
                                   "peak_allocated_bytes": peak, "source_bits_sha256": digest(sessions)})
        del sessions
        gc.collect()
    result["raw_source_unchanged"] = source_before == source_digest()
    result["accepted"] = (result["raw_source_unchanged"] and
                          result["variants"][0]["source_bits_sha256"] == result["variants"][1]["source_bits_sha256"])
    atomic_write_json(output / "oracle.json", result)
    accepted = torch.tensor(int(result["accepted"]), device=device)
    torch.distributed.all_reduce(accepted, op=torch.distributed.ReduceOp.MIN)
    if rank == 0:
        print(json.dumps(result), flush=True)
    torch.distributed.destroy_process_group()
    if not int(accepted):
        raise RuntimeError("blocked physical staging changed authoritative source bits")


if __name__ == "__main__":
    main()
