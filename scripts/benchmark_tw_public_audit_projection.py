"""Bounded real-source A/B/B/A of the canonical audit's external-column read.

This is not a full-history audit benchmark. It retains one fixed, bounded
source batch; both variants use the original reader and all execution rules.
It never builds a production panel, calls providers, or initializes a GPU.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
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
from stockagent.config import load_config
from stockagent.data import panel as panel_module
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

CONTRACT = "tw_public_audit_bounded_external_projection_v1"


def _sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _generation(path):
    info = path.stat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _array(digest, name, value):
    value = np.ascontiguousarray(value)
    if value.dtype.hasobject:
        raise ValueError("object arrays do not supply a raw-bit identity")
    header = json.dumps([name, value.dtype.str, list(value.shape)], ensure_ascii=False).encode()
    digest.update(len(header).to_bytes(8, "big"))
    digest.update(header)
    # datetime64 has a valid byte representation, but does not expose a Python
    # buffer format. View the same contiguous bits, including empty arrays.
    digest.update(memoryview(value.view(np.uint8).reshape(-1)))


def worker(fixture, variant, threads):
    pa.set_cpu_count(threads)
    metadata = json.loads((fixture / "fixture.json").read_bytes())
    path = fixture / "bounded_features.parquet"
    if metadata["contract"] != CONTRACT or _sha(path) != metadata["fixture_sha256"]:
        raise ValueError("bounded fixture identity changed")
    selected = metadata["selected_model_columns"]
    started, cpu = time.perf_counter(), time.process_time()
    arrays = panel_module._load_external_feature_arrays(
        path, market_symbol=metadata["market_symbol"],
        include_features=True, include_rules=metadata["include_rules"],
        selected_feature_names=tuple(selected) if variant == "projected" else None,
    )
    indices = [arrays.feature_names.index(name) for name in selected]
    digest = hashlib.sha256()
    _array(digest, "market_dates", arrays.market_dates)
    _array(digest, "market_values", arrays.market_values[:, indices])
    _array(digest, "market_rules", arrays.market_rule_values)
    _array(digest, "official_session_dates", arrays.official_session_dates)
    for symbol, (days, values) in sorted(arrays.by_symbol.items()):
        _array(digest, f"{symbol}/dates", days)
        _array(digest, f"{symbol}/values", values[:, indices])
    for symbol, (days, values) in sorted(arrays.by_symbol_rules.items()):
        _array(digest, f"{symbol}/rule_dates", days)
        _array(digest, f"{symbol}/rules", values)
    rule_names = list(arrays.rule_names)
    if rule_names != metadata["rule_columns"]:
        raise ValueError("execution rule columns were pruned or reordered")
    digest.update(json.dumps([selected, rule_names], ensure_ascii=False).encode())
    return {"variant": variant, "wall_seconds": time.perf_counter() - started,
            "cpu_seconds": time.process_time() - cpu,
            "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "output_sha256": digest.hexdigest(), "selected_features": len(selected),
            "read_model_columns": len(arrays.feature_names), "rule_columns": len(rule_names),
            "symbols": len(arrays.by_symbol), "polars_threads": pl.thread_pool_size(),
            "arrow_threads": pa.cpu_count()}


def benchmark(*, config_path, feature_path, parquet_root, output, max_rows, threads,
              reuse_fixture=False):
    if not 1 <= max_rows <= 131072 or not 1 <= threads <= 8:
        raise ValueError("bounded rows must be [1,131072], threads [1,8]")
    if output.is_symlink() or output.absolute() != output.resolve() or not output.is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("benchmark requires a real, private artifacts/benchmarks directory")
    if output.exists() != reuse_fixture:
        raise ValueError("use a fresh directory, or explicitly reuse the fixed fixture")
    if pl.thread_pool_size() != threads:
        raise ValueError("set POLARS_MAX_THREADS to the selected bounded thread count")
    cfg = load_config(config_path)
    resolved_config = hashlib.sha256(json.dumps(asdict(cfg), sort_keys=True, default=str).encode()).hexdigest()
    target = output / "bounded_features.parquet"
    if reuse_fixture:
        # Retry the same exact bounded capture, not another copy/read of live
        # history. A failed attempt's supervised log remains independent.
        metadata = json.loads((output / "fixture.json").read_bytes())
        if (metadata["contract"] != CONTRACT or metadata["resolved_config_sha256"] != resolved_config
                or not 1 <= metadata["bounded_rows"] <= max_rows
                or not 1 <= metadata["batch_bytes"] <= 128 * 1024**2
                or target.is_symlink() or _sha(target) != metadata["fixture_sha256"]
                or target.stat().st_size != metadata["fixture_bytes"]):
            raise ValueError("existing bounded fixture/config changed or exceeds the selected budget")
    else:
        metadata = capture(cfg, feature_path, parquet_root, output, target, max_rows, resolved_config)
    code_paths = [Path(__file__), Path(panel_module.__file__), ROOT / "scripts/audit_tw_public_data_layer.py",
                  ROOT / "stockagent/config.py", output / "fixture.json", target]
    code = {str(path): _sha(path) for path in code_paths}
    attempt = output / f"attempt-{uuid.uuid4().hex}"
    attempt.mkdir(mode=0o700)
    trials = []
    for index, variant in enumerate(("baseline", "projected", "projected", "baseline")):
        child = subprocess.run([sys.executable, str(Path(__file__)), "--worker", variant,
                                "--output-dir", str(output), "--threads", str(threads)],
                               text=True, capture_output=True, check=False, timeout=120)
        if child.returncode:
            atomic_write_json(attempt / f"{index}-{variant}-failed.json", {
                "state": "failed_trial", "variant": variant, "exit_code": child.returncode,
                "stdout": child.stdout, "stderr": child.stderr,
            })
            raise RuntimeError(f"{variant} trial failed ({child.returncode}): {child.stderr[-4000:]}")
        row = json.loads(child.stdout.splitlines()[-1])
        if row["polars_threads"] != threads or row["arrow_threads"] != threads:
            raise ValueError("actual worker pools differ from the selected budget")
        row["state"] = "completed_trial"
        trials.append(row)
        atomic_write_json(attempt / f"{index}-{variant}.json", row)
        print(json.dumps(row), flush=True)
    if code != {str(path): _sha(path) for path in code_paths} or len({r["output_sha256"] for r in trials}) != 1:
        raise ValueError("code/fixture changed or complete retained values/rules differ")
    result = {"contract": CONTRACT, "state": "accepted_for_bounded_external_read",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(), "fixture": metadata,
              "input_and_code_sha256": code, "trials": trials, "attempt": str(attempt),
              "reused_existing_bounded_fixture": reuse_fixture,
              "means": {variant: {metric: statistics.mean(r[metric] for r in trials if r["variant"] == variant)
                         for metric in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
                        for variant in ("baseline", "projected")},
              "complete_selected_values_and_all_rules_bit_equal": True,
              "scope": "bounded external read/normalization/arrays/full retained hash; not full audit or full-source memory",
              "production_panel_builds": 0, "production_restarts": 0, "provider_calls": 0,
              "gpu_calls": 0, "source_writes": False}
    atomic_write_json(attempt / "abba-receipt.json", result)
    # Each code generation owns a fresh result receipt, but the same fixed
    # input is reused. Preserve the first accepted receipt as well as retries.
    if not (output / "abba-receipt.json").exists():
        atomic_write_json(output / "abba-receipt.json", result)
    return result


def capture(cfg, feature_path, parquet_root, output, target, max_rows, resolved_config):
    with live_tw_public_source_read_guard(parquet_root):
        before = _generation(feature_path)
        kwargs = _panel_kwargs(cfg, feature_path)
        if not kwargs["external_include_features"] or kwargs["feature_exclude"]:
            raise ValueError("this audit config does not supply the resolved feature projection")
        # One bounded batch, not a whole-file read followed by head(). No extra
        # provider source tree or complete benchmark database is constructed.
        batch = next(pq.ParquetFile(feature_path).iter_batches(batch_size=max_rows, use_threads=False))
        if batch.nbytes > 128 * 1024**2:
            raise ValueError("bounded input exceeds the 128 MiB engineering guard")
        schema = batch.schema.names
        selected = [name for name in kwargs["feature_include"] if name in schema]
        rules = [name for name in schema if name.startswith("_twpub_")] if kwargs["external_include_rules"] else []
        output.mkdir(parents=True, mode=0o700)
        pq.write_table(pa.Table.from_batches([batch]), target, compression="zstd")
        if _generation(feature_path) != before:
            raise ValueError("source changed during bounded capture; no acceptance")
        metadata = {"contract": CONTRACT, "source": str(feature_path), "source_generation": before,
                    "source_total_rows": pq.ParquetFile(feature_path).metadata.num_rows,
                    "source_model_columns": len([n for n in schema if n not in {"date", "symbol"} and not n.startswith("_")]),
                    "fixture_sha256": _sha(target), "fixture_bytes": target.stat().st_size,
                    "bounded_rows": batch.num_rows, "batch_bytes": batch.nbytes,
                    "selected_model_columns": selected, "rule_columns": rules,
                    "market_symbol": kwargs["external_market_symbol"],
                    "include_rules": kwargs["external_include_rules"],
                    "resolved_config_sha256": resolved_config,
                    "source_acceptance": "bounded stable-generation capture, not full-file or full-history proof"}
        atomic_write_json(output / "fixture.json", metadata)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--feature-path", type=Path)
    parser.add_argument("--parquet-root", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-rows", type=int, default=65536)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--reuse-fixture", action="store_true",
                        help="retry the existing fixed capture without reading/copying live history")
    parser.add_argument("--worker", choices=("baseline", "projected"))
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.output_dir, args.worker, args.threads)))
    else:
        if args.config is None:
            parser.error("--config required")
        cfg = load_config(args.config)
        result = benchmark(config_path=args.config.resolve(),
                           feature_path=(args.feature_path or Path(cfg.data.tw_public_feature_path)).resolve(),
                           parquet_root=(args.parquet_root or Path(cfg.data.parquet_root)).resolve(),
                           output=args.output_dir.absolute(), max_rows=args.max_rows, threads=args.threads,
                           reuse_fixture=args.reuse_fixture)
        print(json.dumps(result["means"]))


if __name__ == "__main__":
    main()
