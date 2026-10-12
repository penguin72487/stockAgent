#!/usr/bin/env python3
"""Paired, offline full-source merge/buffer/normalizer acceptance.

Baseline modules are exact pre-edit copies, not reimplemented fast/slow oracles.
All outputs go to a new engineering directory. No API, production writer or GPU
is involved. Each case has its own process for interpretable peak RSS; imports
and independent output-parity checking are reported separately from workflow.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import hashlib
import importlib.util
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

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from downloader.verified_merge import sha256_file
from scripts.benchmark_columnar_query_engines import memory_high_water


def load_baseline(path: Path):
    name = "processing_baseline_" + path.stem
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def measured(call):
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    result = call()
    elapsed = time.perf_counter() - started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {"workflow_seconds": elapsed, "cpu_seconds": (
        usage.ru_utime + usage.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime),
        "peak_rss_bytes": memory_high_water(),
        "ru_maxrss_bytes_diagnostic_only": usage.ru_maxrss * 1024, "result": result,
        "block_reads": usage.ru_inblock - cpu_before.ru_inblock,
        "block_writes": usage.ru_oublock - cpu_before.ru_oublock}


def child(config: dict) -> dict:
    import numpy as np
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq
    pa.set_cpu_count(int(os.environ.get("POLARS_MAX_THREADS", "2")))
    case, mode = config["case"], config["mode"]
    baseline = Path(config["baseline"])
    if case == "taifex":
        from scripts import download_taifex_public_history as current
        collector = (load_baseline(baseline / "download_taifex_public_history.py")
                     if mode == "baseline" else current)
        root = Path(config["root"])
        sessions, _ = collector._load_sessions(Path(config["calendar"]), date.max)
        mapping = collector._next_session_map(sessions)
        return measured(lambda: [collector._merge_dataset(root, name, mapping)
                                 for name in config["datasets"]])
    if case == "crypto":
        from downloader.candle_frame_buffer import CandleFrameBuffer
        from downloader import download_binance_perp_15m as binance
        buffer_class = (load_baseline(baseline / "candle_frame_buffer.py").CandleFrameBuffer
                        if mode == "baseline" else CandleFrameBuffer)
        normalizer = (load_baseline(baseline / "download_binance_perp_15m.py")._normalize_candles
                      if mode == "baseline" else binance._normalize_candles)
        path, output = Path(config["source"]), Path(config["output"])
        def execute():
            kwargs = {} if mode == "baseline" else {"scratch_dir": output.parent,
                "max_resident_bytes": (64 << 20) if mode == "bounded" else (1 << 40)}
            buffer = buffer_class(normalizer, **kwargs)
            fields = ["date", "open", "max", "min", "close", "binance_volume_base",
                      "binance_close_time_ms", "binance_volume_quote", "binance_trade_count",
                      "binance_taker_buy_base_volume", "binance_taker_buy_quote_volume"]
            source = pq.ParquetFile(path)
            for batch in source.iter_batches(batch_size=50_000, columns=fields, use_threads=False):
                frame = pl.from_arrow(batch).with_columns(
                    pl.col("date").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=True)
                    .dt.epoch("ms").alias("date"))
                buffer.extend([list(row) for row in frame.iter_rows()])
            result = buffer.finish()
            binance._validate_candles(result)
            atomic_write_parquet(output, result)
            return {"rows": len(result), "output": str(output), "sha256": sha256_file(output),
                    "spilled_parts": getattr(buffer, "spilled_parts", 0),
                    "peak_staging_bytes": getattr(buffer, "peak_staging_bytes", None),
                    "disjoint_fast_path": getattr(buffer, "used_disjoint_fast_path", False)}
        return measured(execute)
    if case == "tick":
        from stockagent.data import tw_index_derivatives_tick as current
        module = (load_baseline(baseline / "tw_index_derivatives_tick.py")
                  if mode == "baseline" else current)
        dataset = module.load_index_derivatives_tick_dataset(Path(config["source"]))
        def execute():
            for path, receipt in zip(dataset.partition_paths, dataset.manifest["partitions"], strict=True):
                if sha256_file(path) != receipt["output_sha256"]:
                    raise ValueError("real tick partition changed or does not match receipt")
            result = []
            for option_mode in (False, True):
                norm = dataset.fit_normalizer(range(len(dataset.partition_paths)), option_mode=option_mode)
                digest = hashlib.sha256()
                for value in (norm.mean, norm.scale, norm.counts):
                    digest.update(np.ascontiguousarray(value).tobytes())
                result.append({"option_mode": option_mode, "sha256": digest.hexdigest(),
                               "features": len(norm.mean), "counts": norm.counts.tolist()})
            return result
        return measured(execute)
    raise ValueError(case)


def run_child(config: dict) -> dict:
    started = time.perf_counter()
    env = {**os.environ, "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
           "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"}
    process = subprocess.run([sys.executable, __file__, "--child", json.dumps(config)],
                             cwd=ROOT, env=env, text=True, capture_output=True, timeout=600)
    if process.returncode:
        raise RuntimeError(f"{config['case']}/{config['mode']} failed: {process.stderr[-4000:]}")
    result = json.loads(process.stdout)
    result.update(case=config["case"], mode=config["mode"],
                  process_wall_seconds=time.perf_counter() - started,
                  host_load=list(os.getloadavg()))
    print(json.dumps({key: result[key] for key in ("case", "mode", "workflow_seconds", "peak_rss_bytes")}), flush=True)
    return result


def benchmark(args) -> dict:
    import polars as pl
    import pyarrow.parquet as pq
    work = (args.output.parent / ("work-" + datetime.now(UTC).strftime("%H%M%S%f"))).resolve()
    work.mkdir(parents=True)
    baseline = args.baseline.resolve()
    report = {"schema_version": 1, "state": "running", "provider_calls": 0,
              "production_writes": False, "baseline": str(baseline), "trials": [],
              "parity": {}, "source_identities": {},
              "scope": "offline full selected datasets; not API throughput or all training folds",
              "cache_state": "OS cache uncontrolled, balanced order, live nonexclusive host",
              "memory_measurement": "current exec memory-image VmHWM; ru_maxrss retained only as diagnostic",
              "baseline_sha256": {path.name: sha256_file(path) for path in baseline.glob("*.py")}}
    implementations = [Path(__file__).resolve(), ROOT / "downloader/verified_merge.py",
                       ROOT / "downloader/artifact_io.py", ROOT / "downloader/candle_frame_buffer.py",
                       ROOT / "scripts/download_taifex_public_history.py",
                       ROOT / "stockagent/data/tw_index_derivatives_tick.py",
                       ROOT / "downloader/download_binance_perp_15m.py",
                       ROOT / "downloader/candle_columns.py",
                       ROOT / "scripts/benchmark_columnar_query_engines.py"]
    report["implementation_sha256"] = {str(path.relative_to(ROOT)): sha256_file(path) for path in implementations}
    report["runtime"] = {"python": sys.version, "polars": pl.__version__, "pyarrow": __import__("pyarrow").__version__,
                         "polars_threads": 2, "arrow_threads": 2}
    atomic_write_json(args.output, report)
    if "taifex" in args.cases:
        source = ROOT / "data_taifex_public_history"
        datasets = sorted(path.stem for path in (source / "normalized").glob("*.parquet")
                          if (source / "receipts" / path.stem).is_dir())
        frozen = work / "receipt-snapshot"
        frozen.mkdir()
        for name in datasets:
            shutil.copytree(source / "receipts" / name, frozen / name)
        report["source_identities"]["taifex"] = {
            "datasets": datasets, "receipts": {
                str(path.relative_to(frozen)): sha256_file(path) for path in frozen.rglob("*.json")}}
        def make_root(label):
            root = work / label
            root.mkdir()
            for name in ("raw", "shards"):
                (root / name).symlink_to(source / name, target_is_directory=True)
            (root / "receipts").symlink_to(frozen, target_is_directory=True)
            return root
        current_root = make_root("taifex-current")
        calendar = work / "calendar.parquet"
        shutil.copy2(ROOT / "data_tw_index_futures/day_session_contracts.parquet", calendar)
        report["source_identities"]["taifex"]["calendar_sha256"] = sha256_file(calendar)
        common = {"case": "taifex", "baseline": str(baseline), "datasets": datasets,
                  "calendar": str(calendar)}
        seed = run_child({**common, "mode": "rebuild", "root": str(current_root)})
        report["trials"].append(seed)
        atomic_write_json(args.output, report)
        for repeat in range(args.repeat):
            order = ("baseline", "reuse") if repeat % 2 == 0 else ("reuse", "baseline")
            for mode in order:
                root = make_root(f"taifex-old-{repeat}") if mode == "baseline" else current_root
                trial = run_child({**common, "mode": mode, "root": str(root)})
                report["trials"].append(trial)
                atomic_write_json(args.output, report)
                if mode == "baseline":
                    for name in datasets:
                        old = pq.read_table(root / "normalized" / f"{name}.parquet")
                        new = pq.read_table(current_root / "normalized" / f"{name}.parquet")
                        if not old.equals(new, check_metadata=True):
                            raise ValueError(f"TAIFEX complete value/schema/row-order mismatch: {name}")
                elif not all(row["merge_reused"] for row in trial["result"]):
                    raise ValueError("unchanged dataset unexpectedly rebuilt")
                atomic_write_json(args.output, report)
        report["parity"]["taifex"] = "all rows, schema metadata, physical order and NULLs identical"
    if "crypto" in args.cases:
        source = ROOT / "data_binance/1m/BTCUSDT_features.parquet"
        before = sha256_file(source)
        report["source_identities"]["crypto"] = {"path": str(source), "sha256": before,
            "rows": pq.ParquetFile(source).metadata.num_rows, "bytes": source.stat().st_size,
            "input": "all raw candle fields projected from full acquired BTC history, not HTTP response bytes"}
        expected = None
        for repeat in range(args.repeat):
            modes = ("baseline", "bounded", "in_memory") if repeat % 2 == 0 else ("in_memory", "bounded", "baseline")
            for mode in modes:
                output = work / f"crypto-{mode}-{repeat}.parquet"
                trial = run_child({"case": "crypto", "mode": mode, "baseline": str(baseline),
                                   "source": str(source), "output": str(output)})
                report["trials"].append(trial)
                atomic_write_json(args.output, report)
                if expected is None:
                    expected = output
                elif not pl.read_parquet(expected).equals(pl.read_parquet(output)):
                    raise ValueError("full crypto value/dtype/order/NULL mismatch")
                atomic_write_json(args.output, report)
        if sha256_file(source) != before:
            raise ValueError("crypto source changed during benchmark")
        report["parity"]["crypto"] = "all rows, dtypes, order and NULLs identical; source unchanged"
    if "tick" in args.cases:
        source = ROOT / "data_tw_index_derivatives_ticks/strategy_dataset"
        if args.rebuild_tick_fixture:
            from stockagent.data import tw_index_derivatives_tick as current
            old = json.loads((source / "manifest.json").read_bytes())
            old_rejected = False
            try:
                current.load_index_derivatives_tick_dataset(source)
            except ValueError as exc:
                if str(exc) != "unsupported tick strategy dataset schema":
                    raise
                old_rejected = True
            original = source
            source = work / "tick-current-schema"
            started = time.perf_counter()
            manifest = current.build_index_derivatives_tick_dataset(
                ROOT / "data_tw_index_derivatives_ticks", source,
                **old["parameters"],
            )
            report["tick_preparation"] = {
                "seconds": time.perf_counter()-started, "old_manifest": str(original / "manifest.json"),
                "old_schema": old["schema_version"], "old_schema_rejected_not_bypassed": old_rejected,
                "canonical_builder": "build_index_derivatives_tick_dataset",
                "new_schema": manifest["schema_version"], "source_manifest_sha256": manifest["source_manifest_sha256"],
                "summary": manifest["summary"], "production_overwrite": False,
            }
            atomic_write_json(args.output, report)
        before = sha256_file(source / "manifest.json")
        report["source_identities"]["tick"] = {"manifest": str(source / "manifest.json"), "sha256": before,
            "scope": "every day in the chosen complete research strategy dataset, both TX and TXO modes"}
        expected = None
        for repeat in range(args.repeat):
            for mode in (("baseline", "projected") if repeat % 2 == 0 else ("projected", "baseline")):
                trial = run_child({"case": "tick", "mode": mode, "baseline": str(baseline), "source": str(source)})
                report["trials"].append(trial)
                atomic_write_json(args.output, report)
                if expected is None:
                    expected = trial["result"]
                elif trial["result"] != expected:
                    raise ValueError("normalizer bitwise mean/scale/count mismatch")
                atomic_write_json(args.output, report)
        if sha256_file(source / "manifest.json") != before:
            raise ValueError("tick manifest changed during benchmark")
        report["parity"]["tick"] = "bitwise mean/scale/count parity in both modes; receipts verified"
    report["medians"] = {}
    for case in args.cases:
        trials = [row for row in report["trials"] if row["case"] == case]
        report["medians"][case] = {
            mode: {field: statistics.median(row[field] for row in trials if row["mode"] == mode)
                   for field in ("workflow_seconds", "process_wall_seconds", "cpu_seconds", "peak_rss_bytes")}
            for mode in sorted({row["mode"] for row in trials})}
    if any(sha256_file(path) != report["implementation_sha256"][str(path.relative_to(ROOT))]
           for path in implementations):
        raise ValueError("processing implementation changed during paired benchmark")
    report["implementation_unchanged"] = True
    report.update(state="accepted", observed_at_utc=datetime.now(UTC).isoformat(),
                  all_project_optimal_claim=False, official_api_maximum_claim=False,
                  excludes=["HTTP/publisher throughput", "formal GPU training", "browser rendering"])
    atomic_write_json(args.output, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cases", type=lambda value: value.split(","), default=["taifex", "crypto", "tick"])
    parser.add_argument("--repeat", type=int, choices=range(1, 8), default=3)
    parser.add_argument("--rebuild-tick-fixture", action="store_true",
                        help="Rebuild in private benchmark scratch; never overwrite an old production schema")
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
    else:
        if args.output is None or args.baseline is None or args.output.exists():
            parser.error("a baseline directory and a NEW output are required")
        try:
            print(json.dumps(benchmark(args)["medians"], ensure_ascii=False))
        except Exception as exc:
            if args.output.exists():
                report = json.loads(args.output.read_bytes())
                report.update(state="failed", error_type=type(exc).__name__, reason=str(exc))
                atomic_write_json(args.output, report)
            raise
