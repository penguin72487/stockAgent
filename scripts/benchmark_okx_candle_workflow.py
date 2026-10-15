#!/usr/bin/env python3
"""Offline complete OKX-history parser/buffer/persistence paired acceptance.

Uses every acquired row's original candle columns, rendered to documented
string-array shape. Not original HTTP payload bytes, network throughput, or a
claim about the complete market. Both paths retain all three volume dimensions.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.benchmark_project_data_processing import load_baseline, measured

FIELDS = ["date", "open", "max", "min", "close", "okx_volume_contract",
          "okx_volume_base", "okx_volume_quote", "okx_confirm"]


def child(config):
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq
    from downloader import download_okx_perp_daily as current
    from downloader.candle_frame_buffer import CandleFrameBuffer
    from downloader.ohlcv_hot_tail import invalid_candle_values

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    baseline = Path(config["baseline"])
    module = (load_baseline(baseline / "download_okx_perp_daily.py")
              if config["mode"] == "baseline" else current)
    buffer_class = (load_baseline(baseline / "candle_frame_buffer.py").CandleFrameBuffer
                    if config["mode"] == "baseline" else CandleFrameBuffer)
    source, output = Path(config["source"]), Path(config["output"])
    def execute():
        buffer = buffer_class(module._normalize_candles)
        for batch in pq.ParquetFile(source).iter_batches(batch_size=50_000, columns=FIELDS, use_threads=False):
            frame = pl.from_arrow(batch).with_columns(
                pl.col("date").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=True).dt.epoch("ms")
            )
            buffer.extend([[str(value) for value in row] for row in frame.iter_rows()])
        result = buffer.finish()
        if result["date"].n_unique() != result.height or result.filter(invalid_candle_values()).height:
            raise ValueError("complete candle result failed basic OHLCV validation")
        atomic_write_parquet(output, result)
        if not pl.read_parquet(output).equals(result):
            raise ValueError("persisted candles do not match complete result")
        return {"rows": result.height, "output": str(output), "sha256": sha256_file(output),
                "spilled_parts": getattr(buffer, "spilled_parts", 0),
                "peak_staging_bytes": getattr(buffer, "peak_staging_bytes", None)}
    return measured(execute)


def benchmark(args):
    import polars as pl
    import pyarrow.parquet as pq
    work = args.output.parent / ("okx-work-" + datetime.now(UTC).strftime("%H%M%S%f"))
    work.mkdir(parents=True)
    source = args.source.resolve(strict=True)
    identity = sha256_file(source)
    implementations = [Path(__file__), ROOT / "downloader/download_okx_perp_daily.py",
                       ROOT / "downloader/candle_columns.py", ROOT / "downloader/candle_frame_buffer.py",
                       ROOT / "scripts/benchmark_project_data_processing.py",
                       ROOT / "scripts/benchmark_columnar_query_engines.py"]
    report = {"schema_version": 1, "state": "running", "source": str(source), "source_sha256": identity,
              "source_rows": pq.ParquetFile(source).metadata.num_rows, "source_columns": FIELDS,
              "provider_calls": 0, "production_writes": False, "trials": [],
              "implementation_sha256": {str(path): sha256_file(path) for path in implementations},
              "baseline_sha256": {name: sha256_file(args.baseline / name) for name in
                                  ("download_okx_perp_daily.py", "candle_frame_buffer.py")},
              "scope": "whole acquired BTCUSDT OKX candle history, parsing + buffer + validation + write/readback",
              "cache_state": "uncontrolled OS cache; live nonexclusive host; alternating order",
              "memory_measurement": "current exec memory-image VmHWM; imports and result readback included",
              "threads": 2, "all_project_optimal_claim": False, "official_api_maximum_claim": False}
    atomic_write_json(args.output, report)
    expected = None
    for repeat in range(args.repeat):
        for mode in (("baseline", "columnar") if repeat % 2 == 0 else ("columnar", "baseline")):
            output = (work / f"{mode}-{repeat}.parquet").resolve()
            config = {"mode": mode, "baseline": str(args.baseline.resolve()),
                      "source": str(source), "output": str(output)}
            started = time.perf_counter()
            process = subprocess.run([sys.executable, __file__, "--child", json.dumps(config)],
                cwd=ROOT, capture_output=True, text=True, timeout=300,
                env={**os.environ, "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
                     "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"})
            if process.returncode:
                raise RuntimeError(f"{mode} failed: {process.stderr[-3000:]}")
            trial = json.loads(process.stdout)
            trial.update(mode=mode, process_wall_seconds=time.perf_counter()-started, host_load=list(os.getloadavg()))
            report["trials"].append(trial)
            atomic_write_json(args.output, report)
            if expected is None:
                expected = output
            elif not pl.read_parquet(expected).equals(pl.read_parquet(output)):
                raise ValueError("complete values, dtypes, timestamps, order or volume dimensions differ")
            print(json.dumps({key: trial[key] for key in ("mode", "workflow_seconds", "peak_rss_bytes")}), flush=True)
    if sha256_file(source) != identity or any(sha256_file(path) != report["implementation_sha256"][str(path)]
                                            for path in implementations):
        raise ValueError("source or implementation changed during benchmark")
    report["medians"] = {mode: {field: statistics.median(t[field] for t in report["trials"] if t["mode"] == mode)
                        for field in ("workflow_seconds", "process_wall_seconds", "cpu_seconds", "peak_rss_bytes")}
                        for mode in ("baseline", "columnar")}
    report.update(state="accepted", parity="all rows/dtypes/order/NULLs/volume dimensions match; source unchanged",
                  observed_at_utc=datetime.now(UTC).isoformat())
    atomic_write_json(args.output, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--source", type=Path, default=ROOT / "data_okx/1m/BTCUSDTSWAP_features.parquet")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeat", type=int, choices=range(1, 8), default=3)
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
    else:
        if args.output is None or args.baseline is None or args.output.exists():
            parser.error("a baseline and NEW output path are required")
        try:
            print(json.dumps(benchmark(args)["medians"]))
        except Exception as exc:
            if args.output.exists():
                report = json.loads(args.output.read_bytes())
                report.update(state="failed", error_type=type(exc).__name__, reason=str(exc))
                atomic_write_json(args.output, report)
            raise
