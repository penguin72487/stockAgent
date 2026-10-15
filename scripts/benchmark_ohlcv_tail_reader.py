#!/usr/bin/env python3
"""Offline, paired complete bounded OHLCV read/merge/parity acceptance.

Freeze the actual base and hot tail before editing the reader. Each trial uses
a fresh CPU process and reads every requested value; no API or source writes.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.benchmark_project_data_processing import load_baseline, measured

CONTRACT = "bounded_ohlcv_reader_full_output_parity_v1"
READER = ROOT / "downloader/ohlcv_hot_tail.py"
SOURCES = {
    "binance": ROOT / "data_binance/1m/BTCUSDT_features.parquet",
    "okx": ROOT / "data_okx/1m/BTCUSDTSWAP_features.parquet",
}


def _generation(path: Path) -> list[int]:
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def select_tw_sources(root: Path, count: int) -> dict[str, Path]:
    """A deterministic size-stratified sample, explicitly not all-market proof."""
    paths = sorted(root.glob("*_features.parquet"), key=lambda path: (path.stat().st_size, path.name))
    if count < 2 or len(paths) < count:
        raise ValueError("TW sample requires at least two files and sufficient sources")
    return {f"tw_{paths[index].stem}": paths[index]
            for index in (round(position * (len(paths) - 1) / (count - 1))
                          for position in range(count))}


def threshold_candidate(module, threshold: int) -> None:
    """Change only the existing reader's one cutoff in this isolated process."""
    if type(threshold) is not int or threshold < 0:
        raise ValueError("threshold must be a nonnegative integer")
    tree = ast.parse(inspect.getsource(module._read_tail_row_groups))
    found = [node for node in ast.walk(tree)
             if isinstance(node, ast.Constant) and type(node.value) is int and node.value == 8192]
    if len(found) != 1:
        raise ValueError("requires the single measured 8192-row cutoff")
    found[0].value = threshold
    exec(compile(tree, "<isolated-tail-cutoff-candidate>", "exec"), module.__dict__)


def prepare(fixture: Path, *, sources: dict[str, Path] | None = None) -> dict:
    from downloader.ohlcv_hot_tail import hot_tail_path
    if fixture.exists():
        raise FileExistsError("Use a new fixture, preserving the original baseline")
    if shutil.disk_usage(fixture.parent).free < 30 * 1024**3:
        raise RuntimeError("Keep at least 30 GiB free before freezing sources")
    fixture.mkdir(mode=0o700)
    reader_before = _generation(READER)
    shutil.copyfile(READER, fixture / "baseline.py")
    metadata = {"contract": CONTRACT, "observed_at_utc": datetime.now(UTC).isoformat(),
                "baseline_sha256": sha256_file(fixture / "baseline.py"), "sources": []}
    for name, source in (SOURCES if sources is None else sources).items():
        target = fixture / name / source.name
        target.parent.mkdir(mode=0o700)
        paths = [(source, target)]
        tail = hot_tail_path(source)
        if tail.is_file():
            hot_tail_path(target).parent.mkdir(mode=0o700)
            paths.append((tail, hot_tail_path(target)))
        before = {str(path): _generation(path) for path, _ in paths}
        members = []
        for path, copied in paths:
            shutil.copyfile(path, copied)
            digest = sha256_file(copied)
            if digest != sha256_file(path):
                raise RuntimeError(f"Source changed during complete copy: {path.name}")
            copied.chmod(0o400)
            members.append({"source": str(path), "copy": str(copied.resolve()),
                            "generation": before[str(path)], "sha256": digest,
                            "bytes": copied.stat().st_size})
        if before != {str(path): _generation(path) for path, _ in paths}:
            raise RuntimeError("Source generation changed; failed fixture is retained")
        metadata["sources"].append({"name": name, "base": str(target.resolve()),
                                    "members": members})
    if reader_before != _generation(READER):
        raise RuntimeError("Reader changed while freezing the baseline")
    (fixture / "baseline.py").chmod(0o400)
    atomic_write_json(fixture / "fixture.json", metadata)
    return metadata


def _frame_identity(frame) -> str:
    """Ordered schema, NULL mask and every non-NULL numeric bit (incl. NaN/-0)."""
    import polars as pl
    digest = hashlib.sha256()
    digest.update(json.dumps({"rows": frame.height, "schema": [
        (name, str(dtype)) for name, dtype in frame.schema.items()]},
        ensure_ascii=False, separators=(",", ":")).encode())
    for series in frame:
        valid = series.is_not_null().to_numpy()
        digest.update(valid.tobytes())
        physical = series.to_physical()
        if physical.dtype.is_numeric() or physical.dtype == pl.Boolean:
            digest.update(physical.to_numpy()[valid].tobytes())
        else:
            digest.update(json.dumps(series.to_list(), ensure_ascii=False,
                                     separators=(",", ":")).encode())
    return digest.hexdigest()


def child(config: dict) -> dict:
    import polars as pl
    import pyarrow as pa
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    from downloader import ohlcv_hot_tail as current
    baseline = load_baseline(Path(config["fixture"]) / "baseline.py")
    module = current if config["mode"] == "current" else baseline
    if config["mode"] == "cutoff_candidate":
        threshold_candidate(module, config["tail_threshold"])
    if config["mode"] == "lazy_candidate":
        def candidate(path, rows, *, columns=None):
            query = pl.scan_parquet(path, glob=False)
            if columns is not None:
                query = query.select(list(columns))
            return query.tail(max(1, int(rows))).collect().to_arrow()
        module._read_tail_row_groups = candidate
    case = config["case"]
    columns = (["date", "open", "max", "min", "close", "Trading_Volume"]
               if case == "ohlcv32" else None)
    count = {"all32": 32, "all48": 48, "all4096": 4096, "ohlcv32": 32}[case]

    def execute():
        frame = module.read_logical_parquet(Path(config["base"]), columns=columns,
                                            tail_rows=count)
        return {"rows": frame.height, "columns": frame.width,
                "output_sha256": _frame_identity(frame)}
    return measured(execute)


def benchmark(args) -> dict:
    fixture = args.fixture.resolve(strict=True)
    metadata = json.loads((fixture / "fixture.json").read_bytes())
    if metadata["contract"] != CONTRACT:
        raise ValueError("Incompatible source fixture")
    paths = [fixture / "baseline.py", READER, Path(__file__),
             ROOT / "scripts/benchmark_project_data_processing.py"]
    paths += [Path(member["copy"]) for source in metadata["sources"]
              for member in source["members"]]
    identities = {str(path): sha256_file(path) for path in paths}
    if identities[str(fixture / "baseline.py")] != metadata["baseline_sha256"]:
        raise ValueError("Frozen baseline differs")
    for source in metadata["sources"]:
        for member in source["members"]:
            if identities[member["copy"]] != member["sha256"]:
                raise ValueError("Frozen complete source differs")
    threshold = getattr(args, "tail_threshold", None)
    other_mode = ("cutoff_candidate" if threshold is not None else
                  "current" if args.current else "lazy_candidate")
    cases = getattr(args, "cases", None) or ("all32", "all4096", "ohlcv32")
    report = {"contract": CONTRACT, "state": "running", "fixture": str(fixture),
              "sources": metadata["sources"], "implementation_sha256": identities,
              "provider_calls": 0, "production_writes": False,
              "threads": 2, "gpu_calls": 0, "trials": [],
              "all_project_optimal_claim": False,
              "scope": "complete requested bounded read + base/hot-tail merge + every output bit hash",
              "cache_state": "uncontrolled OS cache; live host; alternating order"}
    report.update(tail_threshold=threshold, cases=list(cases),
                  source_scope="explicit frozen source sample, not full live panel or all-market acceptance")
    atomic_write_json(args.output, report)
    expected = {}
    for source in metadata["sources"]:
        for case in cases:
            for repeat in range(args.repeat):
                order = (("baseline", other_mode) if repeat % 2 == 0
                         else (other_mode, "baseline"))
                for mode in order:
                    config = {"fixture": str(fixture), "mode": mode,
                              "base": source["base"], "case": case, "tail_threshold": threshold}
                    started = time.perf_counter()
                    result = subprocess.run([sys.executable, __file__, "--child", json.dumps(config)],
                        cwd=ROOT, capture_output=True, text=True, timeout=120,
                        env={**os.environ, "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
                             "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"})
                    if result.returncode:
                        raise RuntimeError(f"{source['name']}/{case}/{mode}: {result.stderr[-3000:]}")
                    trial = json.loads(result.stdout)
                    trial.update(provider=source["name"], case=case, mode=mode, repeat=repeat,
                                 process_wall_seconds=time.perf_counter() - started)
                    key = (source["name"], case)
                    if trial["result"] != expected.setdefault(key, trial["result"]):
                        raise ValueError("Complete requested values/dtypes/order/NULL/float bits differ")
                    report["trials"].append(trial)
                    atomic_write_json(args.output, report)
                    print(json.dumps({"provider": source["name"], "case": case, "mode": mode,
                                      "workflow_seconds": trial["workflow_seconds"],
                                      "peak_rss_bytes": trial["peak_rss_bytes"]}), flush=True)
    if identities != {str(path): sha256_file(path) for path in paths}:
        raise ValueError("Fixture or implementation changed during benchmark")
    report["medians"] = [
        {"provider": name, "case": case, "mode": mode, **{
            field: statistics.median(trial[field] for trial in report["trials"]
                if (trial["provider"], trial["case"], trial["mode"]) == (name, case, mode))
            for field in ("workflow_seconds", "process_wall_seconds", "cpu_seconds", "peak_rss_bytes")}}
        for name in (source["name"] for source in metadata["sources"]) for case in cases
        for mode in ("baseline", other_mode)]
    report.update(state="accepted", observed_at_utc=datetime.now(UTC).isoformat(),
                  parity="all requested numeric bits/schema/order/NULLs identical; fixed sources unchanged")
    atomic_write_json(args.output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--current", action="store_true", help="Verify maintained reader instead of prototype")
    parser.add_argument("--tail-threshold", type=int,
                        help="Isolated cutoff candidate; does not edit or deploy the maintained reader")
    parser.add_argument("--tw-sample", type=int, choices=[0, *range(2, 33)], default=0)
    parser.add_argument("--tw-root", type=Path, default=ROOT / "data_tw_public/stocks")
    parser.add_argument("--cases", nargs="+", choices=("all32", "all48", "all4096", "ohlcv32"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeat", type=int, choices=range(1, 8), default=3)
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
        return
    if args.fixture is None:
        parser.error("An explicit fixture is required")
    if args.tail_threshold is not None and (args.current or args.tail_threshold < 0):
        parser.error("A nonnegative cutoff candidate cannot be combined with --current")
    if args.prepare_only:
        sources = select_tw_sources(args.tw_root, args.tw_sample) if args.tw_sample else None
        prepare(args.fixture, sources=sources)
        print(json.dumps({"fixture": str(args.fixture), "state": "prepared"}))
        return
    if args.output is None or args.output.exists():
        parser.error("A NEW output is required; preserve previous evidence")
    try:
        print(json.dumps(benchmark(args)["medians"]))
    except Exception as exc:
        if args.output.exists():
            report = json.loads(args.output.read_bytes())
            report.update(state="failed", error_type=type(exc).__name__, reason=str(exc))
            atomic_write_json(args.output, report)
        raise


if __name__ == "__main__":
    main()
