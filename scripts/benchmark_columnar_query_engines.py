#!/usr/bin/env python3
"""Measure existing offline engines on the same frozen, real daily source.

No provider request, panel backend, source overwrite or GPU job is involved.
Includes complete selected-result ordering, persistence and readback validation;
reports query cost separately. Source hashing/snapshot setup is outside timings.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file

COLUMNS = ["date", "symbol", "twpub_official_trading_volume_raw", "twpub_official_trading_value_raw"]


def memory_high_water() -> int | None:
    # Linux getrusage.ru_maxrss can retain the fork parent's high-water mark
    # across exec. VmHWM belongs to this process's current memory image.
    path = Path("/proc/self/status")
    if path.is_file():
        for line in path.read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    return None


def child(config: dict) -> dict:
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq
    from stockagent.data.columnar_lake import read_daily_projection

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    source, output = Path(config["source"]), Path(config["output"])
    symbols = config["symbols"]
    start_date = date.fromisoformat(config["start_date"])
    cpu_before = time.process_time()
    started = time.perf_counter()
    if config["engine"] == "eager_projected_reference":
        # Same required columns and filters; not an inflated all-column oracle.
        frame = pl.from_arrow(pq.read_table(source, columns=COLUMNS)).filter(
            (pl.col("date") >= start_date) & pl.col("symbol").is_in(symbols)
        )
    else:
        table = read_daily_projection(
            source, columns=COLUMNS, symbols=symbols, start_date=start_date,
            engine=config["engine"], duckdb_threads=2, duckdb_memory_limit="1GB",
        )
        frame = pl.from_arrow(table)
    query_seconds = time.perf_counter() - started
    frame = frame.sort(["date", "symbol"], maintain_order=True)
    atomic_write_parquet(output, frame)
    decoded = pl.read_parquet(output)
    if not decoded.equals(frame):
        raise ValueError("persisted result changed values, identity, NULLs or ordering")
    return {
        "query_seconds": query_seconds,
        "workflow_seconds": time.perf_counter() - started,
        "cpu_seconds": time.process_time() - cpu_before,
        "peak_rss_bytes": memory_high_water(),
        "ru_maxrss_bytes_diagnostic_only": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "rows": len(frame), "output": str(output), "output_bytes": output.stat().st_size,
        "sha256": sha256_file(output), "engine": config["engine"], "query": config["query"],
    }


def benchmark(args) -> dict:
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq
    import duckdb

    work = args.output.parent / ("query-work-" + datetime.now(UTC).strftime("%H%M%S%f"))
    work.mkdir(parents=True)
    work = work.resolve()
    source = args.source.resolve(strict=True)
    before = sha256_file(source)
    snapshot = work / "daily-source.parquet"
    shutil.copy2(source, snapshot)
    if sha256_file(snapshot) != before or sha256_file(source) != before:
        raise ValueError("source changed during frozen snapshot creation")
    universe = sorted(pl.scan_parquet(snapshot).select("symbol").unique().collect()["symbol"].to_list())
    queries = {"four_symbols_2014_onward": ["2330", "2317", "0050", "6488"],
               "all_observed_identities_2014_onward": universe}
    report = {
        "schema_version": 1, "state": "running", "provider_calls": 0, "production_writes": False,
        "source": {"path": str(source), "snapshot": str(snapshot), "sha256": before,
                   "bytes": source.stat().st_size, "rows": pq.ParquetFile(snapshot).metadata.num_rows},
        "columns": COLUMNS, "start_date": "2014-01-01", "observed_identities": len(universe),
        "runtime": {"python": sys.version, "polars": pl.__version__, "pyarrow": pa.__version__,
                    "duckdb": duckdb.__version__, "threads_per_engine": 2},
        "implementation_sha256": sha256_file(Path(__file__)),
        "query_implementation_sha256": sha256_file(ROOT / "stockagent/data/columnar_lake.py"),
        "trials": [], "parity": {},
        "scope": "existing offline query/persistence path; not neural panel backend or a whole training fold",
        "timing_excludes": ["source SHA and snapshot creation", "process imports (reported separately)",
                            "independent cross-engine output comparison", "HTTP", "GPU", "OS-cold cache control"],
        "memory_measurement": "current exec memory-image /proc/self/status VmHWM; includes imports and result readback",
        "host_exclusivity": False,
    }
    atomic_write_json(args.output, report)
    modes = ["eager_projected_reference", "arrow", "polars", "duckdb"]
    for query, symbols in queries.items():
        expected = None
        for repeat in range(args.repeat):
            for mode in modes if repeat % 2 == 0 else reversed(modes):
                output = work / f"{query}-{mode}-{repeat}.parquet"
                config = {"source": str(snapshot), "output": str(output), "symbols": symbols,
                          "start_date": "2014-01-01", "engine": mode, "query": query}
                started = time.perf_counter()
                completed = subprocess.run(
                    [sys.executable, __file__, "--child", json.dumps(config)], cwd=ROOT,
                    env={**os.environ, "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
                         "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"},
                    capture_output=True, text=True, timeout=180,
                )
                if completed.returncode:
                    raise RuntimeError(f"{query}/{mode} failed: {completed.stderr[-3000:]}")
                trial = json.loads(completed.stdout)
                trial.update(process_wall_seconds=time.perf_counter()-started, host_load=list(os.getloadavg()))
                report["trials"].append(trial)
                atomic_write_json(args.output, report)
                if expected is None:
                    expected = output
                else:
                    left, right = pl.read_parquet(expected), pl.read_parquet(output)
                    if left.schema != right.schema or not left.equals(right):
                        raise ValueError(f"complete cross-engine output mismatch: {query}/{mode}")
                print(json.dumps({key: trial[key] for key in ("query", "engine", "rows", "workflow_seconds", "peak_rss_bytes")}), flush=True)
        report["parity"][query] = "all selected rows, dtypes, identity, NULL/NaN and stable key order match"
    if sha256_file(snapshot) != before:
        raise ValueError("frozen source changed during benchmark")
    if sha256_file(Path(__file__)) != report["implementation_sha256"] or sha256_file(ROOT / "stockagent/data/columnar_lake.py") != report["query_implementation_sha256"]:
        raise ValueError("query implementation changed during benchmark")
    report["medians"] = {
        query: {mode: {field: statistics.median(row[field] for row in report["trials"]
                       if row["query"] == query and row["engine"] == mode)
                       for field in ("query_seconds", "workflow_seconds", "cpu_seconds", "process_wall_seconds", "peak_rss_bytes")}
                for mode in modes} for query in queries
    }
    report.update(state="accepted", observed_at_utc=datetime.now(UTC).isoformat(), source_unchanged=True,
                  all_project_optimal_claim=False, training_default_changed=False)
    atomic_write_json(args.output, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data_tw_public/features/tw_public_stock_daily.parquet")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeat", type=int, choices=range(1, 8), default=3)
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
    else:
        if args.output is None or args.output.exists():
            parser.error("a NEW output path is required")
        try:
            print(json.dumps(benchmark(args)["medians"], ensure_ascii=False))
        except Exception as exc:
            if args.output.exists():
                report = json.loads(args.output.read_bytes())
                report.update(state="failed", error_type=type(exc).__name__, reason=str(exc))
                atomic_write_json(args.output, report)
            raise
