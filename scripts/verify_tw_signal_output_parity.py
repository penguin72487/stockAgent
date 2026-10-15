#!/usr/bin/env python3
"""Bitwise financial-output check for two isolated TW signal runs.

This is bounded engineering evidence, not live latency or broker-fill proof.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from scripts.promote_tw_day_trade_replay import _atomic_json, _sha256


def verify(reference: Path, candidate: Path) -> dict:
    left = {p.relative_to(reference): p for p in reference.glob("*/**/target_weights.parquet")}
    right = {p.relative_to(candidate): p for p in candidate.glob("*/**/target_weights.parquet")}
    if not left or left.keys() != right.keys():
        raise ValueError("signal-session membership differs or is empty")
    rows = []
    for relative in sorted(left):
        old, new = pd.read_parquet(left[relative]), pd.read_parquet(right[relative])
        pd.testing.assert_frame_equal(old, new, check_exact=True, check_dtype=True)
        floating_cells = 0
        for column in old.columns:
            a, b = old[column].to_numpy(), new[column].to_numpy()
            if a.dtype.kind == "f":
                # Includes NaN representation and signed zero, unlike a tolerance check.
                if a.tobytes() != b.tobytes():
                    raise ValueError(f"floating bits differ: {relative} {column}")
                floating_cells += len(a)
        old_summary = json.loads(left[relative].with_name("summary.json").read_text())
        new_summary = json.loads(right[relative].with_name("summary.json").read_text())
        for key in ("target_gross", "target_net", "model_explanation", "source_publication_contract"):
            if old_summary.get(key) != new_summary.get(key):
                raise ValueError(f"derived financial/publication summary differs: {relative} {key}")
        rows.append({"session": str(relative.parts[0])[:10], "symbols": len(old),
                     "floating_cells": floating_cells,
                     "reference_weights_sha256": _sha256(left[relative]),
                     "candidate_weights_sha256": _sha256(right[relative]),
                     "reference_latency": old_summary.get("live_latency"),
                     "candidate_latency": new_summary.get("live_latency")})
    return {"contract": "bounded_exact_signal_output_parity_v1", "status": "exact",
            "reference_root": str(reference.resolve()), "candidate_root": str(candidate.resolve()),
            "sessions": rows, "session_count": len(rows),
            "symbol_rows": sum(r["symbols"] for r in rows),
            "floating_cells": sum(r["floating_cells"] for r in rows),
            "live_opening_latency_proven": False, "broker_execution_proven": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.reference, args.candidate)
    _atomic_json(args.receipt, result)
    print(json.dumps({k: v for k, v in result.items() if k != "sessions"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
