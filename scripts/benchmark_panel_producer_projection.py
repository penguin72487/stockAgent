#!/usr/bin/env python3
"""Fixed-input ABBA of complete Panel build, durable cache and validated reuse.

Reuse an existing bounded external fixture; capture at most nine small OHLCV
files once, under the source owner's lease. No provider/model/GPU calls, live
cache writes, full database copies, source retirement or implicit cleanup.
The control is an explicitly preserved pre-change panel.py, not a second
implementation of its transformations.
"""
from __future__ import annotations

import argparse
from dataclasses import fields
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from scripts.audit_tw_public_data_layer import _panel_kwargs
from scripts.benchmark_tw_public_audit_projection import _array, _generation, _sha
from stockagent.config import load_config
from stockagent.data import panel as current_panel
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

CONTRACT = "shared_panel_producer_complete_bounded_abba_v1"


def _private_path(path: Path, role: str) -> Path:
    path = path.absolute()
    allowed = ROOT / "artifacts" / role
    if path.resolve() != path or not path.is_relative_to(allowed) or path == allowed:
        raise ValueError(f"requires a non-redirected private artifacts/{role} child")
    return path


def _proofs(paths):
    result = {}
    for path in paths:
        before = _generation(path)
        digest = _sha(path)
        if before != _generation(path):
            raise ValueError(f"input changed while hashing: {path}")
        result[str(path)] = {"generation": before, "sha256": digest}
    return result


def _panel_hash(panel):
    digest = hashlib.sha256()
    for field in fields(panel):
        value = getattr(panel, field.name)
        if isinstance(value, np.ndarray):
            _array(digest, field.name, value)
        else:
            digest.update(json.dumps([field.name, value], sort_keys=True, default=str).encode())
    return digest.hexdigest()


def _cache_inventory(cache):
    return _proofs(sorted(path for path in cache.rglob("*") if path.is_file()))


def capture(*, config, external, parquet_root, baseline, output, baseline_cache=None):
    output.mkdir(parents=True, mode=0o700)
    stocks = output / "stocks"
    stocks.mkdir(mode=0o700)
    # This is the existing retained capture, not a new read of the wide live
    # source. Its original receipt binds the complete Parquet bytes.
    original = json.loads((external.parent / "fixture.json").read_bytes())
    if (original.get("fixture_sha256") != _sha(external)
            or not 1 <= pq.ParquetFile(external).metadata.num_rows <= 131072
            or external.stat().st_size > 32 * 1024**2):
        raise ValueError("external capture is not the original bounded fixture")
    symbols = pq.read_table(external, columns=["symbol"])["symbol"].unique().to_pylist()
    # The configured benchmark owns the calendar even when it is absent from
    # the bounded public batch. Do not change benchmark semantics for a sample.
    benchmark_name = load_config(config).data.benchmark_name
    symbols = list(dict.fromkeys([benchmark_name, *symbols]))
    source_proofs, captured = {}, []
    with live_tw_public_source_read_guard(parquet_root):
        total = 0
        for symbol in symbols:
            if not isinstance(symbol, str) or not symbol.isalnum():
                continue
            source = parquet_root / f"{symbol}_features.parquet"
            if not source.is_file():
                continue
            if source.stat().st_size > 16 * 1024**2:
                raise ValueError("OHLCV fixture member exceeds the fixed byte bound")
            parquet = pq.ParquetFile(source)
            if not 1 <= parquet.metadata.num_rows <= 16384:
                raise ValueError("OHLCV fixture exceeds the fixed row bound")
            proof = _proofs([source])
            table = parquet.read(use_threads=False)
            total += table.nbytes
            if total > 128 * 1024**2:
                raise ValueError("OHLCV capture exceeds the total byte bound")
            target = stocks / source.name
            pq.write_table(table, target, compression="zstd")
            if proof != _proofs([source]):
                raise ValueError("source changed during bounded capture")
            captured.append(target)
            source_proofs.update(proof)
            if len(captured) == 9:
                break
        if not captured or source_proofs != _proofs([Path(p) for p in source_proofs]):
            raise ValueError("empty or changed source generation")
    metadata = {
        "contract": CONTRACT, "external": str(external), "baseline_panel": str(baseline),
        "config": str(config), "captured_sources": source_proofs,
        "input_proofs": _proofs([external, baseline, config, *captured]),
        "stock_files": len(captured), "bounded_external_rows": original["bounded_rows"],
        "no_live_cache_or_source_writes": True,
    }
    if baseline_cache is not None:
        metadata["baseline_cache"] = str(baseline_cache)
        metadata["input_proofs"].update(_proofs([baseline_cache]))
    atomic_write_json(output / "fixture.json", metadata)
    return metadata


def worker(output, variant, threads, trial):
    metadata = json.loads((output / "fixture.json").read_bytes())
    if metadata["contract"] != CONTRACT:
        raise ValueError("unsupported fixture")
    paths = [Path(path) for path in metadata["input_proofs"]]
    if metadata["input_proofs"] != _proofs(paths):
        raise ValueError("fixed inputs changed")
    pa.set_cpu_count(threads)
    if pl.thread_pool_size() != threads:
        raise ValueError("worker thread pool differs from the admitted budget")
    module = current_panel
    if variant == "baseline":
        name = "stockagent.data._private_panel_baseline"
        spec = importlib.util.spec_from_file_location(name, metadata["baseline_panel"])
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        if metadata.get("baseline_cache"):
            cache_name = "stockagent.data._private_panel_cache_baseline"
            cache_spec = importlib.util.spec_from_file_location(cache_name, metadata["baseline_cache"])
            control_cache = importlib.util.module_from_spec(cache_spec)
            sys.modules[cache_name] = control_cache
            cache_spec.loader.exec_module(control_cache)
            module.save_panel_cache_v2 = control_cache.save_panel_cache_v2
    config = load_config(metadata["config"])
    kwargs = _panel_kwargs(config, Path(metadata["external"]), resolve_projection=False)
    kwargs["panel_load_workers"] = threads
    cache = trial / "cache"
    reads, writes = [], []
    original_read, original_save = module._load_external_feature_arrays, module._save_panel_cache

    def read(*args, **kw):
        value = original_read(*args, **kw)
        reads.append({"model_columns": len(value.feature_names), "rules": list(value.rule_names)})
        return value

    def save(*args, **kw):
        writes.append(True)
        return original_save(*args, **kw)

    module._load_external_feature_arrays, module._save_panel_cache = read, save
    started, cpu = time.perf_counter(), time.process_time()
    panel = module.build_panel(output / "stocks", panel_cache_root=cache, **kwargs)
    expected_rules = ([name for name in pq.read_schema(metadata["external"]).names if name.startswith("_twpub_")]
                      if kwargs["external_include_rules"] else [])
    if len(reads) != 1 or reads[0]["rules"] != expected_rules:
        raise ValueError("complete independent execution-rule columns changed")
    cold_seconds = time.perf_counter() - started
    digest = _panel_hash(panel)
    before = _cache_inventory(cache)
    warm_started = time.perf_counter()
    reused = module.build_panel(output / "stocks", panel_cache_root=cache, **kwargs)
    reuse_seconds = time.perf_counter() - warm_started
    readonly = current_panel.load_cached_panel(output / "stocks", panel_cache_root=cache, **kwargs)
    if (readonly is None or digest != _panel_hash(readonly) or digest != _panel_hash(reused)
            or before != _cache_inventory(cache) or len(writes) != 1 or len(reads) != 1):
        raise ValueError("complete cache readback changed values, rewrote data or decoded sources")
    if metadata["input_proofs"] != _proofs(paths):
        raise ValueError("inputs changed during complete workflow")
    inodes = {}
    for path in cache.rglob("*"):
        if path.is_file():
            info = path.stat()
            inodes[(info.st_dev, info.st_ino)] = info.st_blocks * 512
    return {
        "state": "completed_trial", "variant": variant, "output_sha256": digest,
        "wall_seconds": time.perf_counter() - started, "cpu_seconds": time.process_time() - cpu,
        "cold_build_seconds": cold_seconds, "validated_reuse_seconds": reuse_seconds,
        "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "cache_allocated_bytes": sum(inodes.values()), "cache_files": len(before),
        "panel_shape": list(panel.features.shape), "feature_names": panel.feature_names,
        "source_reads": reads, "cache_writes": len(writes), "cache_hit_writes": 0,
        "current_readonly_loader_reuses_original_key": True,
        "polars_threads": pl.thread_pool_size(), "arrow_threads": pa.cpu_count(),
    }


def benchmark(*, config, external, parquet_root, baseline, output, threads, reuse_fixture, baseline_cache=None):
    output = _private_path(output, "benchmarks")
    baseline = _private_path(baseline, "operations")
    if baseline.suffix != ".py" or baseline.stat().st_size > 1024**2:
        raise ValueError("control must be a bounded preserved panel.py")
    if baseline_cache is not None:
        baseline_cache = _private_path(baseline_cache, "operations")
        if baseline_cache.suffix != ".py" or baseline_cache.stat().st_size > 1024**2:
            raise ValueError("writer control must be a bounded preserved panel_cache.py")
    if not 1 <= threads <= 8 or pl.thread_pool_size() != threads:
        raise ValueError("set POLARS_MAX_THREADS to the selected [1,8] thread budget")
    if output.exists() != reuse_fixture:
        raise ValueError("use a fresh output or explicitly --reuse-fixture")
    metadata = (json.loads((output / "fixture.json").read_bytes()) if reuse_fixture else capture(
        config=config, external=external, parquet_root=parquet_root, baseline=baseline, output=output,
        baseline_cache=baseline_cache))
    if (metadata["contract"] != CONTRACT or metadata["config"] != str(config)
            or metadata["baseline_panel"] != str(baseline) or metadata["external"] != str(external)
            or metadata.get("baseline_cache") != (str(baseline_cache) if baseline_cache is not None else None)):
        raise ValueError("fixed fixture contract changed")
    code = _proofs([Path(__file__), Path(current_panel.__file__),
                    ROOT / "scripts/audit_tw_public_data_layer.py", ROOT / "stockagent/data/panel_cache.py",
                    ROOT / "stockagent/data/panel_chunks.py", ROOT / "scripts/benchmark_tw_public_audit_projection.py"])
    attempt = output / f"attempt-{uuid.uuid4().hex}"
    attempt.mkdir(mode=0o700)
    trials = []
    for index, variant in enumerate(("baseline", "projected", "projected", "baseline")):
        trial = attempt / f"{index}-{variant}"
        trial.mkdir(mode=0o700)
        child = subprocess.run([sys.executable, str(Path(__file__)), "--worker", variant,
                                "--output", str(output), "--trial", str(trial), "--threads", str(threads)],
                               capture_output=True, text=True, timeout=240, check=False)
        atomic_write_json(trial / "process.json", {"exit_code": child.returncode, "stdout": child.stdout, "stderr": child.stderr})
        if child.returncode:
            raise RuntimeError(f"{variant} failed: {child.stderr[-3000:]}")
        row = json.loads(child.stdout.splitlines()[-1])
        trials.append(row)
        atomic_write_json(trial / "receipt.json", row)
        print(json.dumps(row), flush=True)
    if code != _proofs([Path(p) for p in code]) or len({row["output_sha256"] for row in trials}) != 1:
        raise ValueError("code changed or complete Panel bits/metadata differ")
    result = {
        "contract": CONTRACT, "state": "accepted_complete_bounded_builder",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "trials": trials,
        "fixture": metadata, "code": code, "all_panel_fields_bit_equal": True,
        "means": {variant: {key: statistics.mean(row[key] for row in trials if row["variant"] == variant)
                              for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "cache_allocated_bytes")}
                  for variant in ("baseline", "projected")},
        "scope": "complete cold builder/cache/full readback/reuse on bounded real inputs; not full native audit",
        "production_restarts": 0, "production_cache_writes": 0, "provider_calls": 0, "gpu_calls": 0,
    }
    atomic_write_json(attempt / "abba-receipt.json", result)
    print(json.dumps({"attempt": str(attempt), "means": result["means"]}))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--external-fixture", type=Path)
    parser.add_argument("--parquet-root", type=Path)
    parser.add_argument("--baseline-panel", type=Path)
    parser.add_argument("--baseline-cache", type=Path, help="also compare the preserved original durable writer")
    parser.add_argument("--threads", default=2, type=int)
    parser.add_argument("--reuse-fixture", action="store_true")
    parser.add_argument("--worker", choices=("baseline", "projected"))
    parser.add_argument("--trial", type=Path)
    args = parser.parse_args()
    if args.worker:
        output = _private_path(args.output, "benchmarks")
        if args.trial is None or not args.trial.absolute().is_relative_to(output) or args.trial.resolve() != args.trial.absolute():
            parser.error("worker requires a real private trial below its fixture")
        print(json.dumps(worker(output, args.worker, args.threads, args.trial)))
    else:
        if any(value is None for value in (args.config, args.external_fixture, args.parquet_root, args.baseline_panel)):
            parser.error("--config, --external-fixture, --parquet-root and --baseline-panel required")
        benchmark(config=args.config.resolve(strict=True), external=args.external_fixture.resolve(strict=True),
                  parquet_root=args.parquet_root.resolve(strict=True), baseline=args.baseline_panel.absolute(),
                  output=args.output.absolute(), threads=args.threads, reuse_fixture=args.reuse_fixture,
                  baseline_cache=args.baseline_cache)


if __name__ == "__main__":
    main()
