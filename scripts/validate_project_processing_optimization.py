#!/usr/bin/env python3
"""Seal bounded optimization evidence; never promote a global-optimum claim."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file


def read_json(path: Path) -> dict:
    before = path.stat()
    value = json.loads(path.read_bytes())
    after = path.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
    ):
        raise ValueError(f"evidence changed while reading: {path}")
    return value


def verify_candle_outputs(reference: Path, variants: list[tuple[Path, str]]) -> dict:
    import numpy as np
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = pq.ParquetFile(reference).metadata.num_rows
    accepted = []
    for variant, expected_sha in variants:
        if sha256_file(variant) != expected_sha:
            raise ValueError(f"persisted benchmark output no longer matches receipt: {variant}")
        left, right = pq.ParquetFile(reference), pq.ParquetFile(variant)
        if not left.schema_arrow.equals(right.schema_arrow, check_metadata=True):
            raise ValueError("candle Arrow types or metadata differ")
        seen = 0
        for a, b in zip(left.iter_batches(batch_size=65536, use_threads=False),
                        right.iter_batches(batch_size=65536, use_threads=False), strict=True):
            if not pl.from_arrow(a).equals(pl.from_arrow(b)):
                raise ValueError("candle values/NULLs/NaNs/identity/order differ")
            for index, field in enumerate(a.schema):
                if pa.types.is_floating(field.type):
                    av = np.ascontiguousarray(a.column(index).to_numpy(zero_copy_only=False)).tobytes()
                    bv = np.ascontiguousarray(b.column(index).to_numpy(zero_copy_only=False)).tobytes()
                    if av != bv:
                        raise ValueError(f"floating-point bit pattern differs: {field.name}")
            seen += a.num_rows
        if seen != rows:
            raise ValueError("candle readback row count differs")
        accepted.append({"path": str(variant), "sha256": expected_sha, "rows": seen})
    return {"reference": str(reference), "reference_sha256": sha256_file(reference),
            "rows": rows, "verified_variants": accepted,
            "parity": "all rows, Arrow schema/metadata, order, NULL/NaN and Float32/Float64 bit patterns"}


def require_accepted_evidence(name: str, report: dict) -> None:
    if name == "dashboard_projection.json":
        # The existing dashboard owner calls its successful state "verified",
        # unlike collector benchmarks. Require actual acceptance proofs too.
        accepted = report.get("state") == "verified" and all(report.get(key) is True for key in (
            "complete_bytes_parity", "all_source_identities_checked", "code_abi_stable"
        ))
    else:
        accepted = (report.get("state") in ("accepted", "accepted_static_inventory")
                    or report.get("accepted") is True)
    if not accepted:
        raise ValueError(f"benchmark or inventory not accepted: {name}")


def validate(args):
    benchmark_root = args.benchmark_root.resolve()
    names = ["optimized_workflows.json", "okx_workflows.json", "query_engines.json", "finlab_ticks.json",
             "finmind_brokers_paired.json", "news_status.json", "dashboard_projection.json", "method_inventory_final.json"]
    evidence = {name: read_json(benchmark_root / name) for name in names}
    for name, report in evidence.items():
        require_accepted_evidence(name, report)
    processing, okx = evidence["optimized_workflows.json"], evidence["okx_workflows.json"]
    for name in ("optimized_workflows.json", "okx_workflows.json"):
        for relative, expected in evidence[name]["implementation_sha256"].items():
            if sha256_file(ROOT / relative) != expected:
                raise ValueError(f"implementation drifted after benchmark: {relative}")
    run = json.loads(subprocess.check_output(["stockagent-agent", "run-status", args.regression_run], text=True))
    if run["state"] != "succeeded" or run["exit_code"] != 0:
        raise ValueError("current broad regression did not succeed")
    log = Path(run["log"])
    match = re.findall(r"(\d+) passed in ([0-9.]+)s", log.read_text())
    if not match:
        raise ValueError("regression lacks an explicit passed-count receipt")
    count, seconds = match[-1]
    crypto_trials = [trial for trial in processing["trials"] if trial["case"] == "crypto"]
    reference = Path(next(t for t in crypto_trials if t["mode"] == "baseline")["result"]["output"])
    parity = {"binance": verify_candle_outputs(reference, [
        (Path(t["result"]["output"]), t["result"]["sha256"]) for t in crypto_trials
    ])}
    reference = Path(next(t for t in okx["trials"] if t["mode"] == "baseline")["result"]["output"])
    parity["okx"] = verify_candle_outputs(reference, [
        (Path(t["result"]["output"]), t["result"]["sha256"]) for t in okx["trials"]
    ])
    metrics = []
    for name, case, mode in (("taifex_verified_unchanged_merge", "taifex", "reuse"),
                             ("binance_complete_candles", "crypto", "bounded"),
                             ("tick_normalizer_both_modes", "tick", "projected")):
        before, after = processing["medians"][case]["baseline"], processing["medians"][case][mode]
        metrics.append({"name": name, "before": before, "after": after,
                        "workflow_speed_ratio": before["workflow_seconds"] / after["workflow_seconds"],
                        "peak_rss_reduction_fraction": 1 - after["peak_rss_bytes"] / before["peak_rss_bytes"]})
    before, after = okx["medians"]["baseline"], okx["medians"]["columnar"]
    metrics.append({"name": "okx_complete_candles", "before": before, "after": after,
                    "workflow_speed_ratio": before["workflow_seconds"] / after["workflow_seconds"],
                    "peak_rss_reduction_fraction": 1 - after["peak_rss_bytes"] / before["peak_rss_bytes"]})
    ownership = {
        "newly_implemented": ["verified no-op merges with source/calendar/code/runtime/output identity",
                              "TAIFEX columnar merges with timestamp metadata compatibility",
                              "Binance/OKX/Bybit shared native decoding and scalar error compatibility",
                              "bounded IPC staging and ordered disjoint-concat proof",
                              "normalizer column projection preserving reduction order",
                              "supported Polars floor aligned to tested 1.38.1 runtime",
                              "paired benchmark RSS measurement corrected to current exec VmHWM"],
        "existing_paths_remeasured_not_new_implementation": ["FinLab tick validation/aggregation", "exact FinMind broker aggregation",
            "news status projection", "sharded acquisition-dashboard projection", "offline Arrow/Polars/DuckDB projection"],
    }
    remaining = [
        {"id": "changed_taifex_history", "state": "partial", "reason": "no-op and columnar merge implemented; changed source still safely rebuilds full canonical projection",
         "next": "measure segmented incremental publication with compatible consumer/receipt/version contracts"},
        {"id": "bybit_full_source_profile", "state": "source_unavailable", "reason": "data_bybit symlink target absent locally; all parser/buffer semantic tests pass but no same-full-source rate claim",
         "next": "restore existing source through canonical storage owner, then repeat complete same-source measurement"},
        {"id": "stateful_full_day_execution_books", "state": "pending_profile", "reason": "normalizer projection complete; full-day stateful book assembly not optimized or benchmarked",
         "next": "measure captured complete day and preserve event order, carrying quote/book state across bounded batches"},
        {"id": "remaining_html_xls_and_research_adapters", "state": "static_inventory_only", "reason": "14 owned direct pandas imports remain, predominantly parsers/research reports; SDK boundaries intentionally retained",
         "next": "measure independent whole-source fixtures before replacing parser/research math semantics"},
        {"id": "end_to_end_live_provider_and_training", "state": "not_claimed", "reason": "no provider calls, service restarts, formal GPU folds, browser deployment, or fleet migration performed",
         "next": "apply measured processor changes through existing owners and validate source receipts/full selected workflows"},
        {"id": "minimum_role_environment_ci", "state": "tested_current_floor_not_all_roles", "reason": "runtime 1.38.1 tested; no reinstall or fresh minimal-role/CUDA environment sweep",
         "next": "validate clean role-lock installs and retain platform-specific runtime identity"},
    ]
    report = {
        "schema_version": 1, "state": "accepted_bounded_optimization", "task_id": "project-data-processing-optimization-20261011",
        "observed_at_utc": datetime.now(UTC).isoformat(), "metrics": metrics, "ownership": ownership,
        "regression": {"run_id": args.regression_run, "tests_passed": int(count), "seconds": float(seconds),
                       "log": str(log), "log_sha256": sha256_file(log)},
        "independent_complete_candle_readback": parity,
        "benchmark_receipts": {name: {"path": str(benchmark_root / name), "sha256": sha256_file(benchmark_root / name)}
                               for name in names},
        "full_inventory": {key: evidence["method_inventory_final.json"][key] for key in ("files", "by_kind", "source_engine_counts", "csv_path")},
        "preserved_contracts": ["raw values and units", "source clocks/availability and causal masks", "last-source-wins revisions",
                                "original receipts and source files", "FP64 normalizer reductions and feature/checkpoint ABI"],
        "remaining_worklist": remaining, "provider_calls": 0, "production_service_restarts": 0, "formal_gpu_jobs": 0,
        "all_project_optimal_claim": False, "all_provider_data_completeness_claim": False,
        "official_api_maximum_claim": False, "all_formal_training_workflows_verified": False,
        "limits": ["2 threads per benchmark, uncontrolled OS cache and nonexclusive live host",
                   "full returned candle DataFrame still materializes at finish; 64MiB limits staging, not total process RSS",
                   "bounded Binance path is slower than unlimited in-memory candidate but keeps fetch residency bounded",
                   "changed TAIFEX seed rebuild not proven faster; no-op improvement requires unchanged verified identities",
                   "earlier paired_workflows_final.json/tick_normalizer.json ru_maxrss metrics are diagnostic only due fork-parent inheritance",
                   "cold FinMind first-use path slower; steady repeated advantage does not prove provider-wide benefit"],
    }
    atomic_write_json(args.output, report)
    return {"state": report["state"], "tests_passed": int(count), "metrics": metrics,
            "remaining_work_items": len(remaining), "acceptance": str(args.output)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-root", type=Path, required=True)
    parser.add_argument("--regression-run", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("a NEW acceptance output is required")
    print(json.dumps(validate(args), ensure_ascii=False))
