"""Offline A/B/B/A of complete acquired feature history processing.

Exact baseline functions come from the named Git revision. Each fresh process
reads the same complete Parquet, plans latest nonnull observations, applies ten
feature overlays, derives features, audits coverage, atomically writes Parquet,
hashes it and independently reads every value back. No provider call or source
write occurs. This is local processing evidence, not API throughput acceptance.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.benchmark_project_data_processing import measured


def child(config: dict) -> dict:
    import polars as pl
    import pyarrow as pa
    from downloader import binance_historical_features as current
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    implementation = {name: getattr(current, name) for name in (
        "_overlay_feature_frame", "_latest_non_null_ms", "_coverage_summary")}
    if config["mode"] == "baseline":
        source = subprocess.run(["git", "show", f'{config["baseline_ref"]}:downloader/binance_historical_features.py'],
                                cwd=ROOT, capture_output=True, text=True, check=True).stdout
        names = {*implementation, "_datetime_ms_expr"}
        selected = [ast.get_source_segment(source, node) for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name in names]
        if len(selected) != len(names):
            raise ValueError("baseline functions differ from the requested comparison")
        namespace = {"pl": pl, "CANDLE_INTERVAL_MS": current.CANDLE_INTERVAL_MS,
                     "FAMILY_PRIMARY_COLUMN": current.FAMILY_PRIMARY_COLUMN}
        exec(compile("from __future__ import annotations\n" + "\n\n".join(selected),
                     "exact-git-baseline", "exec"), namespace)
        implementation = {name: namespace[name] for name in implementation}

    def execute():
        frame = pl.read_parquet(config["source"])
        original = frame
        for column in current.FAMILY_PRIMARY_COLUMN.values():
            implementation["_latest_non_null_ms"](frame, column, 0)
            # Use actual acquired values and nulls, not synthetic replacements.
            fresh = original.tail(2880).select(["date", column])
            frame = implementation["_overlay_feature_frame"](frame, fresh, (column,))
        frame = current._add_derived_features(frame)
        coverage = implementation["_coverage_summary"](frame)
        output = Path(config["output"])
        atomic_write_parquet(output, frame, compression="snappy")
        digest = sha256_file(output)
        if not pl.read_parquet(output).equals(frame):
            raise AssertionError("independent whole-Parquet readback differs")
        return {"rows": frame.height, "columns": len(frame.columns),
                "sha256": digest, "coverage": coverage}
    return measured(execute)


def benchmark(args) -> dict:
    source = args.source.resolve(strict=True)
    identity = sha256_file(source)
    revision = subprocess.run(["git", "rev-parse", args.baseline_ref], cwd=ROOT,
                              check=True, text=True, capture_output=True).stdout.strip()
    implementations = [Path(__file__), ROOT / "downloader/binance_historical_features.py",
                       ROOT / "downloader/feature_overlay.py", ROOT / "downloader/artifact_io.py"]
    fingerprints = {str(path): sha256_file(path) for path in implementations}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"state": "running", "source": str(source), "source_sha256": identity,
              "baseline_ref": revision, "implementation_sha256": fingerprints,
              "provider_calls": 0, "production_writes": False, "trials": [],
              "scope": __doc__, "cache_state": "uncontrolled OS cache; live shared host; alternating order"}
    atomic_write_json(args.output, report)
    with tempfile.TemporaryDirectory(prefix="feature-overlay-", dir=args.output.parent) as scratch:
        reference = None
        for mode in ("baseline", "narrow", "narrow", "baseline"):
            config = {"mode": mode, "baseline_ref": revision, "source": str(source),
                      "output": str(Path(scratch) / "output.parquet")}
            started = time.perf_counter()
            process = subprocess.run([sys.executable, __file__, "--child", json.dumps(config)],
                cwd=ROOT, capture_output=True, text=True, timeout=600,
                env={**os.environ, "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
                     "OPENBLAS_NUM_THREADS": "2", "CUDA_VISIBLE_DEVICES": ""})
            if process.returncode:
                raise RuntimeError(f"{mode} failed: {process.stderr[-2000:]}")
            trial = json.loads(process.stdout)
            if reference is not None and trial["result"] != reference:
                raise AssertionError("full bytes, row counts or coverage differ from exact baseline")
            reference = trial["result"]
            trial.update(mode=mode, process_wall_seconds=time.perf_counter() - started)
            report["trials"].append(trial)
            atomic_write_json(args.output, report)
            print(json.dumps({k: trial[k] for k in ("mode", "workflow_seconds", "peak_rss_bytes")}), flush=True)
    if sha256_file(source) != identity or any(sha256_file(p) != fingerprints[str(p)] for p in implementations):
        raise RuntimeError("source or implementation changed during measurement")
    report["medians"] = {mode: {field: statistics.median(t[field] for t in report["trials"] if t["mode"] == mode)
                               for field in ("workflow_seconds", "cpu_seconds", "peak_rss_bytes", "process_wall_seconds")}
                         for mode in ("baseline", "narrow")}
    report.update(state="accepted", byte_and_value_parity=True, generated_at_utc=datetime.now(timezone.utc).isoformat())
    atomic_write_json(args.output, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data_binance/1m/BTCUSDT_features.parquet")
    parser.add_argument("--baseline-ref", default="HEAD")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
    elif args.output is None or args.output.exists():
        parser.error("a NEW output path is required")
    else:
        print(json.dumps(benchmark(args)["medians"]))
