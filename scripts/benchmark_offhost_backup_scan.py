#!/usr/bin/env python3
"""Compare complete metadata passes on exact current-node cold objects."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.offhost_backup import _regular, private_json, validate_plan  # noqa: E402
from stockagent.data_sync.packed_backup import PinnedObjectSignatures, safe_path, signature  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--objects", type=int, default=128)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists() or not 1 <= args.objects <= 2048:
        parser.error("use a fresh receipt and a bounded 1..2048 object sample")
    plan = json.loads(args.plan.read_bytes())
    validate_plan(plan)
    source = Path(plan["cold_root"])
    candidates = [r for r in plan["files"] if r["role"] == "packed_object"]
    count = min(len(candidates), args.objects)
    selected = [candidates[i * len(candidates) // count] for i in range(count)]
    samples = []
    for number, order in enumerate((("paths", "pinned"), ("pinned", "paths"), ("paths", "pinned"))):
        for method in order:
            started = time.perf_counter()
            if method == "paths":
                values = [signature(_regular(safe_path(source, row["relative"]))) for row in selected]
            else:
                with PinnedObjectSignatures(source) as pinned:
                    values = [pinned.signature(row["relative"]) for row in selected]
                    pinned.recheck()
            seconds = time.perf_counter() - started
            if [list(v) for v in values] != [row["signature"] for row in selected]:
                raise ValueError("the exact selected source changed; timing comparison rejected")
            samples.append({"round": number, "method": method, "objects": count, "complete_seconds": seconds})
    means = {method: statistics.mean(v["complete_seconds"] for v in samples if v["method"] == method)
             for method in ("paths", "pinned")}
    result = {"state": "accepted", "plan_identity_sha256": plan["identity_sha256"],
              "samples": samples, "mean_seconds": means, "selected_method": min(means, key=means.get),
              "pinned_speedup": means["paths"] / means["pinned"], "parity": "exact source signatures",
              "scope": "three interleaved complete metadata passes per method on this node; no checksum, transfer or full-backup timing claim"}
    private_json(args.output, result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
