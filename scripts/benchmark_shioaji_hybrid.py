"""Full-market, zero-API A/B/B/A of the canonical hybrid builder and strict audit.

Borrows the existing minute-backfill owner lock. A is the unchanged merge
oracle; B reuses exact SHA-verified inputs/output. Every trial includes the
complete checksum audit and independent value readback. The temporary baseline
copy is bounded and removed; only measurements and audit receipts are retained.
This is not a downloader, publisher, or evidence of full source availability.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import resource
import shutil
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import build_tw_shioaji_dataset as builder
from scripts import audit_tw_shioaji_dataset as auditor


@contextmanager
def owner_lock(path: Path, wait_seconds: float):
    started = time.monotonic()
    with path.open("a+") as handle:
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() - started >= wait_seconds:
                    raise RuntimeError("canonical minute owner is busy; no benchmark writes started")
                time.sleep(min(0.2, wait_seconds - (time.monotonic() - started)))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_benchmark(output: Path, *, wait_seconds: float = 0, audit_only: bool = False) -> dict:
    dataset = ROOT / "data_tw_public/shioaji/stocks"
    lock = ROOT / "artifacts/data_repair/shioaji_minute_full/runner.lock"
    output.mkdir(parents=True, exist_ok=False)
    trials = []
    code_paths = (Path(__file__), Path(builder.__file__), Path(auditor.__file__))
    code_before = {str(path.relative_to(ROOT)): sha256_file(path) for path in code_paths}
    reference_audit = None
    modes = ("scalar", "columnar", "columnar", "scalar") if audit_only else ("force", "reuse", "reuse", "force")
    with owner_lock(lock, wait_seconds), tempfile.TemporaryDirectory(
        prefix="hybrid-value-oracle-", dir=output,
    ) as scratch:
        snapshot = Path(scratch)
        for mode in modes:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            cpu_start = usage.ru_utime + usage.ru_stime
            started = time.perf_counter()
            if not audit_only:
                with patch.object(sys, "argv", ["build_tw_shioaji_dataset"] + (
                    ["--force-rebuild"] if mode == "force" else []
                )):
                    builder.main()
            built = time.perf_counter()
            comparator = auditor._allclose_reference if mode == "scalar" else auditor._allclose
            with patch.object(auditor, "_allclose", comparator):
                result = auditor.audit(
                    base_stock_root=ROOT / "data_tw_public/stocks",
                    shioaji_root=ROOT / "data_tw_public/shioaji", dataset_root=dataset,
                    verify_chunk_checksums=True, calendar_root=ROOT / "data_tw_public",
                )
            audited = time.perf_counter()
            stable_result = {key: value for key, value in result.items() if key != "written_at_utc"}
            if reference_audit is None:
                reference_audit = stable_result
            elif stable_result != reference_audit:
                raise AssertionError("strict audit results differ between trials")
            paths = sorted(dataset.glob("*_features.parquet"))
            if not trials:
                size = sum(path.stat().st_size for path in paths)
                if size > 2 * 1024**3 or shutil.disk_usage(output).free < size + 5 * 1024**3:
                    raise RuntimeError("bounded value-oracle workspace capacity is insufficient")
                for path in paths:
                    shutil.copy2(path, snapshot / path.name)
                # Do not count the one-time oracle setup in A's workflow time.
                setup_seconds = time.perf_counter() - audited
            else:
                setup_seconds = 0.0
            reading = time.perf_counter()
            if {path.name for path in paths} != {p.name for p in snapshot.iterdir()}:
                raise AssertionError("output symbol scope differs from the cold oracle")
            for path in paths:
                if not pl.read_parquet(path).equals(pl.read_parquet(snapshot / path.name)):
                    raise AssertionError(f"raw values/nulls/units differ from oracle: {path.name}")
            ended = time.perf_counter()
            summary = json.loads((dataset / "shioaji_dataset_summary.json").read_text())
            usage = resource.getrusage(resource.RUSAGE_SELF)
            item = {
                "mode": mode, "symbols": len(paths), "build_seconds": built - started,
                "strict_audit_seconds": audited - built, "readback_seconds": ended - reading,
                "workflow_seconds": ended - started - setup_seconds,
                "cpu_seconds_including_oracle_setup": usage.ru_utime + usage.ru_stime - cpu_start,
                "incremental_build": summary["incremental_build"], "value_parity": True,
            }
            atomic_write_json(output / f"audit-{len(trials)}-{mode}.json", result)
            trials.append(item)
            atomic_write_json(output / "trials.json", trials)
            print(json.dumps(item), flush=True)
        medians = {mode: statistics.median(t["workflow_seconds"] for t in trials if t["mode"] == mode)
                   for mode in (modes[0], modes[1])}
        if code_before != {str(path.relative_to(ROOT)): sha256_file(path) for path in code_paths}:
            raise RuntimeError("benchmark/audit/builder code changed during measurement")
        result = {
            "scope": ("full-market source checksum audit + exact independent readback" if audit_only
                      else "full-market canonical build + full source checksum audit + exact independent readback"),
            "api_requests": 0, "trials": trials, "median_workflow_seconds": medians,
            "workflow_speedup": medians[modes[0]] / medians[modes[1]],
            "all_source_download_complete": False, "value_parity": True, "audit_parity": True,
            "audit_only": audit_only, "code_sha256": code_before,
        }
        atomic_write_json(output / "result.json", result)
        # Same canonical audit receipt, refreshed only after all trials pass.
        atomic_write_json(ROOT / "artifacts/data_quality/tw_shioaji_audit.json",
                          json.loads((output / f"audit-3-{modes[3]}.json").read_text()))
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--lock-wait-seconds", type=float, default=0)
    parser.add_argument("--audit-only", action="store_true",
                        help="Compare original scalar vs columnar strict audit; do not rebuild datasets.")
    args = parser.parse_args()
    if not 0 <= args.lock_wait_seconds <= 1800:
        parser.error("lock wait must be between 0 and 1800 seconds")
    print(json.dumps(run_benchmark(args.output_dir, wait_seconds=args.lock_wait_seconds,
                                   audit_only=args.audit_only)))


if __name__ == "__main__":
    main()
