"""Compare complete seven-range base/delta updates on an existing real fixture.

Reuse the previously verified source fixture; never copy another multi-GB ledger.
A prefix-only Parquet seed is derived from its verified existing projection. Its
cache generation metadata describes an engineering prefix, not a historical
writer receipt. Only real captured tail rows are added. Per-trial disposable
cache directories are temporary; source, seed, code and measurement receipts stay.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import re
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from scripts import benchmark_taifex_history_selection as workflow
from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_segments as segments
from stockagent.live import taifex_volatility_dashboard as dashboard


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def code_proof():
    return {str(p): sha(p) for p in (Path(__file__), Path(cache.__file__),
        Path(segments.__file__), Path(dashboard.__file__), Path(workflow.__file__),
        Path(workflow.selection.__file__))}


def prepare(fixture: Path, work: Path):
    if work.exists():
        raise FileExistsError("use a new evidence directory; preserve earlier trials")
    before = workflow.fixture_proof(fixture)
    work.mkdir(mode=0o700, parents=True)
    # The cache helper was clean before the layout change. Preserve the actual
    # HEAD baseline instead of comparing two copies of the candidate function.
    relative = Path(cache.__file__).resolve().relative_to(ROOT)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    original = subprocess.check_output(["git", "show", f"{head}:{relative}"], cwd=ROOT)
    atomic_write_bytes(work / "original_column_cache.py", original, mode=0o600)
    meta = json.loads((fixture / "fixture.json").read_bytes())
    tail = (fixture / "captured-tail.jsonl").read_bytes()
    if len(tail) != meta["captured_tail_bytes"] or hashlib.sha256(tail).hexdigest() != meta["captured_tail_sha256"]:
        raise ValueError("captured append tail mismatch")
    tail_rows = sum(bool(row.strip()) for row in tail.splitlines())
    seed = work / "seed"
    seed.mkdir(mode=0o700)
    receipts = []
    for source_receipt in sorted((fixture / "columns/history-columns-v1").glob("*.json")):
        value = json.loads(source_receipt.read_bytes())
        source = Path(value["source"])
        if source not in [Path(path) for path in before]:
            raise ValueError("existing cache does not belong to the verified fixture")
        payload = source_receipt.with_suffix(".parquet")
        if sha(payload) != value["payload_sha256"]:
            raise ValueError("existing projection checksum mismatch")
        destination = seed / payload.name
        if source == fixture / "state/marks.jsonl":
            pl.scan_parquet(payload).slice(0, value["rows"] - tail_rows).sink_parquet(
                destination, compression="zstd", compression_level=3,
                row_group_size=8192, maintain_order=True, engine="streaming")
            os.chmod(destination, 0o600)
            prefix_proof = next(row for row in meta["proofs"] if row["source"] == meta["source"] + "/marks.jsonl")
            value["source_generation"][2] = meta["base_bytes"]
            value["source_sha256"] = prefix_proof["sha256"]
            value["payload_sha256"] = sha(destination)
            value["rows"] -= tail_rows
        else:
            os.link(payload, destination)
        atomic_write_json(seed / source_receipt.name, value)
        receipts.append({"name": source_receipt.name, "receipt_sha256": sha(seed / source_receipt.name),
                         "payload_sha256": sha(destination), "rows": value["rows"]})
    if len(receipts) != 2:
        raise ValueError("expected exact original live and replay fixture projections")
    value = {"contract": "taifex_history_base_delta_benchmark_seed_v1", "fixture": str(fixture),
             "source_sha256": before, "seed_receipts": receipts, "code_sha256": code_proof(),
             "original_code_sha256": sha(work / "original_column_cache.py"), "original_git_head": head,
             "prefix_seed_is_engineering_metadata": True, "new_source_copies": 0,
             "provider_calls": 0, "production_writes": False}
    atomic_write_json(work / "seed.json", value)
    return value


def io_counts():
    return {key: int(value) for key, value in
            (line.split(":", 1) for line in Path("/proc/self/io").read_text().splitlines())}


def worker(fixture, work, columns, variant):
    if variant == "baseline":
        import importlib.util

        spec = importlib.util.spec_from_file_location(cache.__name__, work / "original_column_cache.py")
        original = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(original)
        sys.modules[cache.__name__] = original
    os.environ[cache.CACHE_ENV] = str(columns)
    os.environ[cache.ENABLE_ENV] = "1"
    os.environ[segments.LAYOUT_ENV] = "base_delta_v2" if variant == "candidate" else "v1"
    meta = json.loads((fixture / "fixture.json").read_bytes())
    outputs, written, ranges = {}, [], []
    raw_sink = pl.LazyFrame.sink_parquet

    def sink(frame, path, *args, **kwargs):
        value = raw_sink(frame, path, *args, **kwargs)
        written.append(Path(path).stat().st_size)
        return value

    pl.LazyFrame.sink_parquet = sink
    start_io = io_counts()
    start, cpu = time.perf_counter(), time.process_time()
    for key in dashboard.HISTORY_RANGE_SECONDS:
        began, began_cpu = time.perf_counter(), time.process_time()
        result = dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state",
            now=datetime.fromisoformat(meta["clock"]), range_key=key)
        encoded = json.dumps(result, sort_keys=True, ensure_ascii=False,
            allow_nan=False, separators=(",", ":")).encode()
        outputs[key] = hashlib.sha256(encoded).hexdigest()
        ranges.append({"range": key, "rows": len(result["history"]), "json_bytes": len(encoded),
                       "wall_seconds": time.perf_counter() - began,
                       "cpu_seconds": time.process_time() - began_cpu})
    wall, cpu_seconds = time.perf_counter() - start, time.process_time() - cpu
    end_io = io_counts()
    update_writes, update_sinks = sum(written), len(written)
    start, cpu = time.perf_counter(), time.process_time()
    for key in dashboard.HISTORY_RANGE_SECONDS:
        result = dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state",
            now=datetime.fromisoformat(meta["clock"]), range_key=key)
        encoded = json.dumps(result, sort_keys=True, ensure_ascii=False,
            allow_nan=False, separators=(",", ":")).encode()
        if hashlib.sha256(encoded).hexdigest() != outputs[key]:
            raise ValueError("same-process complete warm output differs")
    return {"variant": variant, "wall_seconds": wall, "cpu_seconds": cpu_seconds,
            "threads": pl.thread_pool_size(), "allocator_conf": os.environ.get("_RJEM_MALLOC_CONF", ""),
            "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "io_delta": {key: end_io[key] - start_io[key] for key in end_io},
            "parquet_written_bytes": update_writes, "sink_calls": update_sinks, "ranges": ranges,
            "warm_wall_seconds": time.perf_counter() - start, "warm_cpu_seconds": time.process_time() - cpu,
            "warm_parquet_written_bytes": sum(written) - update_writes,
            "warm_sink_calls": len(written) - update_sinks, "warm_exact_output_parity": True,
            "output_sha256": outputs, "provider_calls": 0, "model_forward_calls": 0,
            "production_writes": False}


def benchmark(fixture, work, control, repeats, measurement="measurement"):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", measurement):
        raise ValueError("invalid measurement name")
    output = work / f"{measurement}.json"
    if output.exists():
        raise FileExistsError("never replace an earlier measurement")
    seed = json.loads((work / "seed.json").read_bytes())
    prior = json.loads(control.read_bytes())
    if (prior.get("exact_output_parity") is not True
            or Path(prior["fixture"]).resolve() != fixture
            or seed["fixture"] != str(fixture)):
        raise ValueError("independent seven-range control must use this same frozen source")
    expected = prior["trials"][0]["output_sha256"]
    before, code = workflow.fixture_proof(fixture), code_proof()
    if before != seed["source_sha256"] or sha(work / "original_column_cache.py") != seed["original_code_sha256"]:
        raise ValueError("prepared source or original comparator changed")
    for item in seed["seed_receipts"]:
        receipt = work / "seed" / item["name"]
        if (sha(receipt) != item["receipt_sha256"]
                or sha(receipt.with_suffix(".parquet")) != item["payload_sha256"]):
            raise ValueError("verified prefix seed changed")
    rows = []
    for variant in ("baseline", "candidate", "candidate", "baseline") * repeats:
        with tempfile.TemporaryDirectory(prefix=f"disposable-{measurement}-", dir=work) as directory:
            columns = Path(directory)
            legacy = columns / "history-columns-v1"
            legacy.mkdir(mode=0o700)
            for item in (work / "seed").iterdir():
                os.link(item, legacy / item.name)
            run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", variant,
                "--fixture", str(fixture), "--work", str(work), "--columns", str(columns)],
                env={**os.environ, "POLARS_MAX_THREADS": "4"}, capture_output=True, text=True, check=True)
            row = json.loads(run.stdout.splitlines()[-1])
            if row["output_sha256"] != expected:
                raise RuntimeError("complete output differs from independent control")
            rows.append(row)
            print(json.dumps(row), flush=True)
    if before != workflow.fixture_proof(fixture) or code_proof() != code:
        raise RuntimeError("source/code changed during comparison")
    medians = {variant: {key: statistics.median(row[key] for row in rows if row["variant"] == variant)
        for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "parquet_written_bytes",
                    "warm_wall_seconds", "warm_cpu_seconds")}
        for variant in ("baseline", "candidate")}
    value = {"contract": "taifex_complete_seven_curve_base_delta_comparison_v1", "state": "measurement_complete",
             "observed_at_utc": datetime.now(timezone.utc).isoformat(), "fixture": str(fixture),
             "scope": "complete_seven_range_real_tail_update_not_live_capture_or_whole_host",
             "independent_control": {"path": str(control), "sha256": sha(control)},
             "medians": medians, "trials": rows, "balanced_abba": True,
             "source_unchanged": True, "code_stable": True, "exact_output_parity": True,
             "source_sha256": before, "code_sha256": code, "seed": seed,
             "provider_calls": 0, "model_forward_calls": 0, "production_writes": False}
    atomic_write_json(output, value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--columns", type=Path)
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=1)
    parser.add_argument("--measurement", default="measurement")
    args = parser.parse_args()
    fixture, work = args.fixture.resolve(strict=True), args.work.resolve()
    if args.prepare_only:
        prepare(fixture, work)
        print(json.dumps({"work": str(work), "state": "prepared"}))
    elif args.worker and args.columns:
        print(json.dumps(worker(fixture, work, args.columns, args.worker)))
    elif args.control:
        result = benchmark(fixture, work, args.control.resolve(strict=True), args.repeats, args.measurement)
        print(json.dumps({"state": result["state"], "medians": result["medians"]}))
    else:
        parser.error("--prepare-only, --worker/--columns or --control is required")


if __name__ == "__main__":
    main()
