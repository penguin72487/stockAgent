#!/usr/bin/env python3
"""Summarize a bounded Nsight SQLite capture without treating it as throughput."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json


def summarize(path: Path) -> dict:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    devices = []
    for (device,) in connection.execute("SELECT DISTINCT deviceId FROM CUPTI_ACTIVITY_KIND_KERNEL"):
        first = last = begin = end = None
        count = union = duration_sum = 0
        for a, b in connection.execute(
            "SELECT start,end FROM CUPTI_ACTIVITY_KIND_KERNEL WHERE deviceId=? ORDER BY start", (device,)
        ):
            count += 1
            duration_sum += b - a
            first = a if first is None else first
            last = max(last or b, b)
            if begin is None:
                begin, end = a, b
            elif a > end:
                union += end - begin
                begin, end = a, b
            else:
                end = max(end, b)
        if begin is not None:
            union += end - begin
        devices.append({"device": device, "kernel_count": count,
            "capture_kernel_span_s": (last - first) / 1e9,
            "kernel_union_s": union / 1e9, "summed_kernel_s": duration_sum / 1e9,
            "kernel_union_fraction": union / (last - first)})
    transfers = [dict(zip(("device", "copy_kind", "calls", "bytes", "summed_duration_ns", "calls_le_16bytes"), row, strict=True))
        for row in connection.execute("SELECT deviceId,copyKind,count(*),sum(bytes),sum(end-start),"
            "sum(CASE WHEN bytes<=16 THEN 1 ELSE 0 END) FROM CUPTI_ACTIVITY_KIND_MEMCPY GROUP BY deviceId,copyKind")]
    connection.close()
    return {"contract": "bounded_nsight_diagnostic_v1", "sqlite": str(path.resolve()),
        "devices": devices, "transfers": transfers,
        "limits": "Profiler overhead included. Kernel union includes collective waits; summed durations do not prove critical-path cost or physical peak."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.sqlite)
    atomic_write_json(args.output, result)
    print(json.dumps({"devices": result["devices"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
