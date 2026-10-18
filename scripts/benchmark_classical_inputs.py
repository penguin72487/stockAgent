"""Measure the full-information tabular representation before model admission."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import numpy as np


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--cache-root", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--prepare-all", action="store_true")
    a = p.parse_args()
    from stockagent.config import load_config
    from stockagent.data.panel import build_panel
    from stockagent.training.classical_data import PhysicalCSRStore
    from train import _build_panel_rank_coordinated
    from downloader.artifact_io import atomic_write_json
    began = time.perf_counter()
    config = load_config(a.config)
    panel = _build_panel_rank_coordinated(build_panel, config, "none")
    store = PhysicalCSRStore(panel.features, a.cache_root, cache_bytes=2 * 1024**3)
    records = []
    total = (store.shape[0] + store.block_rows - 1) // store.block_rows
    for bid in range(total if a.prepare_all else min(total, 4)):
        started = time.perf_counter()
        x = store.block(bid)
        records.append(dict(block=bid, rows=x.shape[0], nnz=int(x.nnz),
            bytes=x.data.nbytes + x.indices.nbytes + x.indptr.nbytes,
            wall_s=time.perf_counter() - started))
        if bid % 8 == 0 or bid + 1 == total:
            print(json.dumps(dict(phase="physical_csr", completed=bid + 1,
                total=total, nnz=int(sum(r["nnz"] for r in records)),
                wall_s=time.perf_counter() - began)), flush=True)
            atomic_write_json(a.output, dict(complete=bid + 1 == total,
                source=store.identity, shape=store.shape, cache=str(store.root),
                blocks=records, wall_s=time.perf_counter() - began))
    print(json.dumps(dict(output=str(a.output), complete=a.prepare_all)), flush=True)


if __name__ == "__main__":
    main()
