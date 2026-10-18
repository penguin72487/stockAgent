"""Measure complete metadata-census parity on a bounded, closed source subtree.

Read-only against the source and cold authority. This isolates inventory memory;
it does not measure hashing, publication, recovery or NAS throughput.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import gc
import json
from pathlib import Path
import statistics
import time
import tracemalloc

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.packed_snapshots import _collect_entries


def measure(source: Path, *, maximum_files: int) -> dict:
    if source.is_symlink() or source.resolve() != source or not source.is_dir():
        raise ValueError("use a real, nonredirected closed source subtree")
    _, expected = _collect_entries(source, retain_entries=False)
    if not 1 <= expected["files"] <= maximum_files:
        raise ValueError("source census exceeds the admitted nonempty file bound")
    trials = []
    for retain in (True, False, False, True):
        gc.collect()
        tracemalloc.start()
        started, cpu_started = time.perf_counter(), time.process_time()
        try:
            entries, summary = _collect_entries(source, retain_entries=retain)
            seconds = time.perf_counter() - started
            cpu_seconds = time.process_time() - cpu_started
            _, peak_bytes = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        if summary != expected or (not retain and entries):
            raise ValueError("source changed or summary-only census differs")
        trials.append({"variant": "retained_inventory" if retain else "summary_only",
                       "census_seconds": seconds, "cpu_seconds": cpu_seconds,
                       "peak_python_bytes": peak_bytes, "retained_records": len(entries)})
        del entries
    _, after = _collect_entries(source, retain_entries=False)
    if after != expected:
        raise ValueError("source changed during the bounded comparison")
    means = {variant: {
        field: statistics.mean(trial[field] for trial in trials if trial["variant"] == variant)
        for field in ("census_seconds", "cpu_seconds", "peak_python_bytes")
    } for variant in ("retained_inventory", "summary_only")}
    return {"contract": "packed_source_census_memory_benchmark_v1",
            "state": "verified_metadata_census_parity",
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "source": str(source), "summary": expected, "trials": trials, "means": means,
            "source_payload_writes": 0, "production_cold_writes": 0, "provider_calls": 0,
            "caveats": ["Warm bounded source metadata walks with tracemalloc enabled.",
                        "Python allocation peak, not total RSS, swap or complete migration speed.",
                        "No content hashing or cold recovery acceptance is claimed."]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--maximum-files", type=int, default=50_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.maximum_files <= 100_000 or args.output.exists() or args.output.is_symlink():
        parser.error("use a 1..100000 file bound and a new output receipt")
    result = measure(args.source.absolute(), maximum_files=args.maximum_files)
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({"state": result["state"], "files": result["summary"]["files"],
                      "means": result["means"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
