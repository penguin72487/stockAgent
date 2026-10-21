"""Offline real-source daily merge acceptance; no source promotion or trading.

Capture the exact source bytes under the canonical reader lease, construct an
explicitly retrospective prior-day fixture, restore latest bytes, then compare
the entire daily output and diagnostics to the original full owner. Timings
include source proofs, merge, cache writes and complete result assembly.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    # The same bounded native pool for both full and daily measurements, not
    # hundreds of threads in each independent decoder. No service setting.
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    source_profile = configure_cpu_source_runtime(2)
    import pyarrow as pa
    pa.set_cpu_count(source_profile["effective_threads"])

import polars as pl  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts import build_tw_official_symbol_parquets as owner  # noqa: E402
from stockagent.data.parquet_group_projection import ParquetGroupProjection  # noqa: E402
from stockagent.data.tw_public_features import _date_column_expr  # noqa: E402
from stockagent.data.tw_public_incremental import table_content_sha256  # noqa: E402
from stockagent.data_sync.cache_capacity import (  # noqa: E402
    GenerationWriteBudget, load_capacity_policy, reserve_cache_write,
)
from stockagent.data_sync.factorized_projection_reuse import seal_new_file  # noqa: E402
from stockagent.data_sync.immutable_cache_files import payload_info  # noqa: E402
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard  # noqa: E402


def measure(function):
    wall, cpu = time.perf_counter(), time.process_time()
    value = function()
    return value, {"wall_seconds": time.perf_counter() - wall,
                   "cpu_seconds": time.process_time() - cpu}


def capture(root, public):
    with live_tw_public_source_read_guard(public / "stocks"):
        paths = [*owner._official_symbol_source_paths(public),
                 public / "twse_taiex_ohlc.parquet", public / "twse_taiex_ohlc.summary.json"]
        before = {str(path): {"generation": payload_info(path), "sha256": sha256_file(path)} for path in paths}
        growth = sum(row["generation"]["size"] for row in before.values()) * 2 + 16 * 1024**2
        captured = []
        with reserve_cache_write(root, key="official-merge-source-capture", maximum_growth_bytes=growth,
                family="prepared", policy=load_capacity_policy()), GenerationWriteBudget(growth) as budget:
            for path in paths:
                target = root / path.name
                with path.open("rb") as source, io.BufferedWriter(budget.open(target), 1024**2) as output:
                    for block in iter(lambda: source.read(8 * 1024**2), b""):
                        output.write(block)
                seal_new_file(target)
                if sha256_file(target) != before[str(path)]["sha256"]:
                    raise ValueError("captured source bytes changed")
                captured.append({"original": str(path), "path": str(target), "sha256": sha256_file(target)})
        if before != {str(path): {"generation": payload_info(path), "sha256": sha256_file(path)} for path in paths}:
            raise ValueError("original source changed during capture")
    return captured


def fixture(captured, working, cutoff=None):
    working.mkdir(exist_ok=True)
    growth = sum(path.stat().st_size for path in captured.iterdir()) * 4 + 64 * 1024**2
    with reserve_cache_write(working, key="official-merge-cutoff-fixture", maximum_growth_bytes=growth,
            family="prepared", policy=load_capacity_policy()), GenerationWriteBudget(growth) as budget:
        for path in captured.iterdir():
            target = working / path.name
            if cutoff is not None and path.name in (*owner.OFFICIAL_CORE_SOURCE_FILENAMES, "twse_taiex_ohlc.parquet"):
                parquet = pq.ParquetFile(path)
                with io.BufferedWriter(budget.open(target), 1024**2) as stream, pq.ParquetWriter(
                        stream, parquet.schema_arrow, compression="snappy") as writer:
                    for batch in parquet.iter_batches(batch_size=128_000, use_threads=False):
                        table = pl.from_arrow(batch).filter(_date_column_expr("date") <= pl.lit(cutoff)).to_arrow()
                        if table.num_rows:
                            writer.write_table(table.cast(parquet.schema_arrow))
                seal_new_file(target)
            else:
                if target.exists():
                    # Unlink this tool's exact disposable fixture link, never
                    # a source or retained capture. Every original is retained.
                    target.unlink()
                os.link(path, target)
        if cutoff is not None:
            path = working / "twse_taiex_ohlc.parquet"
            summary = json.loads((captured / "twse_taiex_ohlc.summary.json").read_bytes())
            summary.update(effective_end_date=str(cutoff), output_rows=pq.ParquetFile(path).metadata.num_rows,
                output_receipt={"path": str(path), "name": path.name, "size": path.stat().st_size, "sha256": sha256_file(path)},
                diagnostic_fixture={"retrospective_subset_not_historical_pit": True})
            # The working summary may be a hardlink to the sealed capture.
            atomic_write_json(working / "twse_taiex_ohlc.summary.json", summary)


def read_reference(path):
    with path.open("rb") as stream:
        payload = stream.read(32 * 1024**2 + 1)
    if len(payload) > 32 * 1024**2:
        raise ValueError("reference receipt exceeds control-file limit")
    reference = json.loads(payload)
    if reference.get("status") != "accepted_real_source_merge_daily_not_full_base_pit_or_live_readiness":
        raise ValueError("reference is not an accepted complete merge receipt")
    return reference, {"path": str(path), "size": len(payload),
                       "sha256": hashlib.sha256(payload).hexdigest()}


def source_bytes_by_name(proofs):
    result = {Path(proof["path"]).name: proof["sha256"] for proof in proofs}
    if len(result) != len(proofs):
        raise ValueError("duplicate source names in fixed-source receipt")
    return result


def run(public, output, *, reference_receipt=None):
    implementation = owner._symbol_code_identity()
    self_sha = sha256_file(Path(__file__))
    reference, reference_proof = (read_reference(reference_receipt)
                                 if reference_receipt is not None else (None, None))
    output.mkdir(parents=True)
    captured = output / "captured"
    captured.mkdir()
    stages = {}

    def stage(name, function):
        print(json.dumps({"stage": name, "state": "started"}), flush=True)
        value, timing = measure(function)
        stages[name] = timing
        atomic_write_json(output / "progress.json", {"stages": stages, "code": implementation})
        return value

    source_proofs = stage("capture_fixed_sources", lambda: capture(captured, public))
    if reference is not None and source_bytes_by_name(source_proofs) != source_bytes_by_name(reference["source_capture"]):
        raise ValueError("reference original source bytes differ from captured sources")
    calendar, _, _ = owner._load_verified_taiex_session_calendar(captured)
    dates = calendar["date"].sort().to_list()
    quote_ends = [pl.scan_parquet(captured / name).select(_date_column_expr("date").max()).collect().item()
                  for name in owner.OFFICIAL_CORE_SOURCE_FILENAMES[:2]]
    latest = min(dates[-1], *quote_ends)
    sessions = [day for day in dates if day <= latest]
    if len(sessions) < 2:
        raise ValueError("no two real completed source sessions")
    prior = sessions[-2]
    working, cache = output / "working", output / "source_cache"
    stage("prior_fixture", lambda: fixture(captured, working, prior))

    def owner_run(cutoff, cached):
        projection = ParquetGroupProjection(cache, implementation, capacity_policy=load_capacity_policy()) if cached else None
        result = owner._official_frame(working, end_date=cutoff, source_cache=projection,
                                       incremental_merge=cached)
        return result, projection.stats() if projection else {"enabled": False}

    (previous, previous_work) = stage("prior_full_merge", lambda: owner_run(prior, True))
    previous_shape = [previous[0].height, previous[0].width]
    del previous
    stage("restore_actual_latest_sources", lambda: fixture(captured, working))
    (daily, daily_work) = stage("new_source_session_merge", lambda: owner_run(latest, True))
    daily_shape = [daily[0].height, daily[0].width]
    daily_proof = stage("daily_complete_typed_oracle", lambda daily=daily: {
        "values_sha256": table_content_sha256(daily[0].to_arrow()),
        "receipts": daily[1:4], "statistics": asdict(daily[-1])})
    del daily
    (full, _) = stage("independent_full_merge", lambda: owner_run(latest, False))
    full_proof = stage("full_complete_typed_oracle", lambda full=full: {
        "values_sha256": table_content_sha256(full[0].to_arrow()),
        "receipts": full[1:4], "statistics": asdict(full[-1])})
    del full
    if daily_proof != full_proof:
        raise ValueError("daily merged values, order, receipts or diagnostics differ from complete original owner")
    if reference is not None and (json.dumps(full_proof, sort_keys=True) !=
                                  json.dumps(reference["oracle"], sort_keys=True)):
        raise ValueError("complete output, receipts or diagnostics differ from fixed pre-change oracle")
    (unchanged, unchanged_work) = stage("unchanged_merge", lambda: owner_run(latest, True))
    unchanged_proof = stage("unchanged_complete_typed_oracle", lambda: {
        "values_sha256": table_content_sha256(unchanged[0].to_arrow()),
        "receipts": unchanged[1:4], "statistics": asdict(unchanged[-1])})
    if unchanged_proof != full_proof:
        raise ValueError("unchanged full logical output differs")
    if implementation != owner._symbol_code_identity() or self_sha != sha256_file(Path(__file__)):
        raise ValueError("benchmark implementation changed during measurement")
    for proof in source_proofs:
        if sha256_file(Path(proof["path"])) != proof["sha256"]:
            raise ValueError("sealed original capture changed")
    if reference_proof is not None and read_reference(reference_receipt)[1] != reference_proof:
        raise ValueError("fixed reference receipt changed during measurement")
    result = {"status": "accepted_real_source_merge_daily_not_full_base_pit_or_live_readiness",
        "scope": "entire official merge, complete original history/columns/diagnostics; no legacy/Yahoo arguments",
        "prior": str(prior), "latest": str(latest), "retrospective_fixture_not_historical_pit": True,
        "stages": stages, "previous_shape": previous_shape, "daily_shape": daily_shape,
        "work": {"previous": previous_work, "daily": daily_work, "unchanged": unchanged_work},
        "oracle": daily_proof, "source_capture": source_proofs, "code": implementation,
        "fixed_pre_change_reference": reference_proof,
        "benchmark_sha256": self_sha, "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "source_runtime": {"polars_threads": pl.thread_pool_size(),
            "environment": {key: os.environ.get(key) for key in (
                "POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")}},
        "affinity_cpus": len(os.sched_getaffinity(0)), "load_average": list(os.getloadavg()),
        "production_source_writes": 0, "service_restarts": 0, "ledger_writes": 0}
    atomic_write_json(output / "receipt.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--reference-receipt", type=Path,
                        help="Accepted pre-change complete oracle for exactly identical original bytes")
    args = parser.parse_args()
    output = args.output.absolute()
    if not output.is_relative_to(ROOT / "artifacts/benchmarks") or output.exists() or output.resolve() != output:
        raise ValueError("requires a fresh non-redirected benchmark output root")
    result = run(args.source_root.resolve(strict=True), output,
                 reference_receipt=args.reference_receipt.resolve(strict=True) if args.reference_receipt else None)
    print(json.dumps({"status": result["status"], "stages": result["stages"]}), flush=True)


if __name__ == "__main__":
    main()
