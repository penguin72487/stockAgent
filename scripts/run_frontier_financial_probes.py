#!/usr/bin/env python3
"""Admit compiler recipes through existing CUDA financial and adjoint oracles."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.manage_gpu_jobs import _gpu_lease_command
from scripts.benchmark_tw_day_trade_batch_sizes import _terminate_process_group, _wait_for_idle_gpus
from stockagent.runtime_identity import verify_source_release


CASES = (
    "test_cuda_compiled_pending_stock_delivery_matches_cpu_and_gradient",
    "test_power_of_two_compiled_fifo_path_matches_eager_forward_state_and_gradient",
    "test_compiled_sparse_event_stream_matches_dense_endpoint_and_gradient",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--test-source-root", type=Path, required=True)
    parser.add_argument("--calendar-recipes", action="store_true")
    parser.add_argument("--graph-recipe", action="store_true")
    parser.add_argument("--prepare-recipe", action="store_true")
    parser.add_argument("--portfolio-graph-recipe", action="store_true")
    parser.add_argument("--sdpa-recipe", action="store_true")
    parser.add_argument("--sdpa-padded-recipe", action="store_true")
    parser.add_argument("--sdpa-runtime-rejection-recipe", action="store_true")
    parser.add_argument("--fp32-last-recipe", action="store_true")
    parser.add_argument("--baseline-config", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = verify_source_release(ROOT.parent / "release.json", ROOT)
    fixture = args.test_source_root.resolve() / "test_tw_day_trade_carry.py"
    cases = (*CASES, "test_cuda_calendar_cache_has_independent_state_and_compiled_adjoints") if args.calendar_recipes else CASES
    recipes = ([{"name": "calendar_cache", "fusion_size": 8, "calendar_cache": True, "calendar_compile": False},
        {"name": "calendar_compiled", "fusion_size": 8, "calendar_cache": True, "calendar_compile": True}]
        if args.calendar_recipes else [{"name": f"fusion_{size}", "fusion_size": size} for size in (16, 32)])
    if args.graph_recipe:
        recipes = [{"name": "financial_cuda_graphs", "fusion_size": 8, "cuda_graphs": True}]
        cases = (*CASES, "test_cuda_financial_graphs_capture_real_ledger_and_preserve_repeated_credit")
    if args.prepare_recipe:
        recipes = [{"name": "funding_preparation", "fusion_size": 8, "prepare_compile": True}]
        cases = (*CASES, "test_cuda_source_failure_stays_nan_without_poisoning_context_or_financial_default",
            "test_cuda_compiled_funding_uses_one_graph_for_different_gregorian_dates",
            "test_cuda_funding_claim_bucket_preserves_history_payment_and_adjoints",
            "test_cuda_funding_cohort_bucket_preserves_logical_zero_rows_and_adjoints")
    if args.portfolio_graph_recipe:
        if args.baseline_config is None:
            parser.error("--portfolio-graph-recipe requires --baseline-config")
        fixture = args.test_source_root.resolve() / "test_factorized_activation_policy.py"
        recipes = [{"name": "portfolio_only_cuda_graphs", "fusion_size": 8}]
        cases = ("test_portfolio_graph_scope_preserves_encoder_checkpoint_and_native_fingerprint",
            "test_portfolio_graph_multiple_forwards_preserve_outputs_and_full_gradients")
    if args.sdpa_recipe:
        fixture = args.test_source_root.resolve() / "test_compiled_fp32_last_attention.py"
        recipes = [{"name": "sdpa_explicit", "fusion_size": 8}]
        cases = tuple("test_cuda_explicit_sdpa_keeps_mask_and_full_parameter_credit[" + dtype + "]"
            for dtype in ("fp32", "bf16"))
    if args.fp32_last_recipe:
        fixture = args.test_source_root.resolve() / "test_compiled_fp32_last_attention.py"
        recipes = [{"name": "fp32_last_query_reduction", "fusion_size": 8}]
        cases = ("test_cuda_fp32_last_query_reduction_preserves_mask_rope_and_parameter_credit",)
    if args.sdpa_padded_recipe:
        fixture = args.test_source_root.resolve() / "test_compiled_fp32_last_attention.py"
        recipes = [{"name": "sdpa_padded", "fusion_size": 8}]
        cases = tuple("test_cuda_padded_sdpa_keeps_mask_and_full_parameter_credit[" + dtype + "]"
            for dtype in ("fp32", "bf16"))
    if args.sdpa_runtime_rejection_recipe:
        fixture = args.test_source_root.resolve() / "test_compiled_fp32_last_attention.py"
        recipes = [{"name": "padded_runtime_rejection", "fusion_size": 8}]
        cases = ("test_known_bad_padded_sdpa_is_rejected_before_forward",)
    atomic_write_json(args.output / "probe-spec.json", {"contract": "physical_fifo_compiler_admission_v1",
        "source": source, "fixture": str(fixture), "fixture_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        "cases": cases, "recipes": recipes, "coalesce_tiling_analysis": False,
        "scope": "synthetic financial/gradient CUDA oracles; not full-fold performance"})
    results = []
    for recipe in recipes:
        size = recipe["fusion_size"]
        _wait_for_idle_gpus(wait=True, poll_s=2.0)
        output = args.output / recipe["name"]
        output.mkdir()
        env = os.environ.copy()
        env.update(OMP_NUM_THREADS="1", MKL_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1",
            STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE=str(size),
            STOCKAGENT_DAY_TRADE_DISABLE_COALESCE_TILING="1", STOCKAGENT_STRICT_NO_FALLBACK="1")
        if args.calendar_recipes:
            env.update(STOCKAGENT_DAY_TRADE_CALENDAR_CACHE=str(int(recipe["calendar_cache"])),
                STOCKAGENT_DAY_TRADE_CALENDAR_COMPILE=str(int(recipe["calendar_compile"])))
        if args.graph_recipe:
            env["STOCKAGENT_DAY_TRADE_CUDAGRAPHS"] = "1"
        if args.prepare_recipe:
            env.update(STOCKAGENT_DAY_TRADE_PREPARE_COMPILE="1", STOCKAGENT_DAY_TRADE_CALENDAR_CACHE="1")
        if args.portfolio_graph_recipe:
            env["STOCKAGENT_FRONTIER_TEST_BASELINE_CONFIG"] = str(args.baseline_config.resolve())
        with (output / "preflight.log").open("w") as log:
            checked = subprocess.run([sys.executable, str(ROOT / "scripts/check_environment.py"),
                "--require-cuda", "--minimum-cuda-devices", "2", "--strict"],
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        if checked.returncode:
            row = {"fusion_size": size, "status": "preflight_failed", "returncode": checked.returncode}
        else:
            junit = output / "junit.xml"
            command = [sys.executable, "-m", "pytest", "-q", "-c", str(ROOT / "pyproject.toml"),
                *[str(fixture) + "::" + case for case in cases], "--junitxml", str(junit)]
            with (output / "tests.log").open("w") as log:
                worker = subprocess.Popen(_gpu_lease_command(command, [0, 1], sharing=False),
                    cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    returncode = worker.wait(timeout=900)
                except subprocess.TimeoutExpired:
                    _terminate_process_group(worker)
                    returncode = 124
            counts = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
            if junit.exists():
                for suite in ET.parse(junit).getroot().iter("testsuite"):
                    for key in counts:
                        counts[key] += int(suite.get(key, "0"))
            passed = (returncode == 0 and counts["tests"] == len(cases)
                and not any(counts[k] for k in ("failures", "errors", "skipped")))
            row = {"fusion_size": size, "status": "passed" if passed else "failed",
                "returncode": returncode, "counts": counts, "command": command}
        results.append(row)
        row["recipe"] = recipe
        atomic_write_json(output / "result.json", row)
        print(json.dumps(row), flush=True)
    atomic_write_json(args.output / "queue-exit.json", {"results": results,
        "all_passed": all(row["status"] == "passed" for row in results)})
    return 0 if all(row["status"] == "passed" for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
