#!/usr/bin/env python3
"""Compare bounded projection rows with hash-verified original trained shards."""
from __future__ import annotations
import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import FactorizedPanelFeatures, file_sha256


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--parent-manifest", type=Path, required=True)
    p.add_argument("--projection-manifest", type=Path, required=True)
    p.add_argument("--source", required=True, help="Enrolled SSH source:path of fixed prepared members")
    p.add_argument("--original-dir", type=Path, help="Reuse previously retained exact original members")
    p.add_argument("--local-only", action="store_true", help="Verify retained SHA-bound members without another remote request")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--date", action="append", required=True)
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    original = json.loads(args.parent_manifest.read_text())
    projection = json.loads(args.projection_manifest.read_text())
    if (original["individual_channels"] != projection["individual_channels"]
            or original["common_channels"] != projection["common_channels"]
            or original["symbols"] != projection["symbols"]):
        raise ValueError("feature/schema identity differs")
    originals = args.original_dir or args.output_dir / "original_shards"
    originals.mkdir(exist_ok=True)
    rows = [original["dates"].index(day) for day in args.date]
    selected = [b for b in original["blocks"] if any(b["start"] <= i < b["start"]+b["shape"][0] for i in rows)]
    members = [original["common"], *(part for block in selected for part in block["symbol_blocks"])]
    # Exact member list only, never an unbounded remote workspace mirror.
    host, remote = args.source.split(":", 1)
    paths = [f"{host}:{remote.rstrip('/')}/{member['path']}" for member in members]
    if not args.local_only:
        subprocess.run(["rsync", "-a", "--ignore-existing", "-e", "ssh -F /etc/stockagent/agent-ssh.conf",
                        *paths, str(originals)+"/"], check=True)
    for proof in members:
        if file_sha256(originals / proof["path"]) != proof["sha256"]:
            raise ValueError(f"fixed original shard differs: {proof['path']}")
    bounded = deepcopy(original)
    bounded["blocks"] = selected
    base = np.zeros((len(original["dates"]), len(original["symbols"]), 6), dtype=np.float32)
    original_features = FactorizedPanelFeatures(originals, bounded, base, max_slab_bytes=1024**3)
    live_features = FactorizedPanelFeatures(args.projection_manifest.resolve().parent, projection,
        np.zeros((len(projection["dates"]), len(projection["symbols"]), 6), dtype=np.float32), max_slab_bytes=1024**3)
    result = []
    for day, row in zip(args.date, rows):
        expected = original_features[row][..., 6:]
        actual = live_features[projection["dates"].index(day)][..., 6:]
        different = actual.view(np.uint32) != expected.view(np.uint32)
        result.append({"date": day, "compared_float32_cells": int(expected.size),
            "different_cells": int(different.sum()), "exact_bitwise_parity": not different.any(),
            "different_features": [projection["individual_channels"][i] if i < len(projection["individual_channels"])
                else projection["common_channels"][i-len(projection["individual_channels"]) ]
                for i in np.flatnonzero(different.any(axis=0))[:25]]})
    passed = all(row["exact_bitwise_parity"] for row in result)
    proof = {"status": "verified" if passed else "failed", "scope": "bounded_original_external_feature_rows",
        "original_manifest_sha256": file_sha256(args.parent_manifest),
        "projection_manifest_sha256": file_sha256(args.projection_manifest.resolve()),
        "source_members": members, "rows": result, "base_prices_compared": False}
    atomic_write_json(args.output_dir / "parity_receipt.json", proof)
    print(json.dumps({k:v for k,v in proof.items() if k != "source_members"}, ensure_ascii=False))
    raise SystemExit(0 if passed else 2)


if __name__ == "__main__":
    main()
