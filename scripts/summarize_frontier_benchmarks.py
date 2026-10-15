#!/usr/bin/env python3
"""Retain measured, slower and failed trials; never equate probes with folds."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import sys
import statistics

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json


def _verified_admission_review(trial: Path, result: Path, curve: Path) -> dict | None:
    review_path = trial / "admission-review-v30.json"
    if not review_path.exists():
        return None
    review = json.loads(review_path.read_text())
    log = result.parent / "attempt_01/train.log"
    checks = (
        review.get("contract") == "frontier_existing_trial_canonical_admission_review_v1",
        review.get("original_result") == str(result),
        review.get("original_trial_not_modified") is True,
        review.get("original_sha256") == hashlib.sha256(result.read_bytes()).hexdigest(),
        log.is_file() and review.get("log_sha256") == hashlib.sha256(log.read_bytes()).hexdigest(),
        curve.is_file() and review.get("curve_sha256") == hashlib.sha256(curve.read_bytes()).hexdigest(),
    )
    valid = all(checks)
    return {"path": str(review_path), "sha256": hashlib.sha256(review_path.read_bytes()).hexdigest(),
        "original_evidence_hashes_valid": valid,
        "reviewed_ok": valid and review.get("reviewed_ok") is True,
        "discarded_autotune_candidate_count": len(review.get("discarded_autotune_candidates", []))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    folds, operators = [], []
    for path in sorted(args.root.glob("**/full-fold/batch_*/result.json")):
        result = json.loads(path.read_text())
        trial = path.parents[2]
        source = trial / "run-spec.json"
        spec = json.loads(source.read_text()) if source.exists() else {}
        curve = Path(result.get("epoch_curve", "/nonexistent"))
        epochs = [json.loads(line) for line in curve.read_text().splitlines() if line.strip()] if curve.exists() else []
        grad_norms = [r["train_grad_norm_before_clip_mean"] for r in epochs
            if isinstance(r.get("train_grad_norm_before_clip_mean"), (int, float))]
        observed_steady = [r.get("epoch_max_rank_s", r.get("epoch_wall_s_max_rank")) for r in epochs
            if r.get("epoch", 0) >= 3]
        observed_steady = [value for value in observed_steady
            if isinstance(value, (int, float)) and math.isfinite(value)]
        row = {"trial": str(trial.relative_to(args.root)), "result": str(path),
            "source_sha256": spec.get("source", {}).get("source_sha256"),
            "profile_timing": bool(spec.get("profile_timing", False)),
            "nsys_trace": bool(spec.get("nsys_trace", False)),
            "overrides": spec.get("overrides"), "runtime_environment": spec.get("runtime_environment"),
            "runtime_overrides": spec.get("runtime_overrides"),
            "host_thread_counts": spec.get("host_thread_counts", {"cpu": 8, "compiler": 16, "polars": 1}),
            "ok": result.get("ok"), "status": result.get("status"),
            "lifecycle_ok": result.get("lifecycle_ok"), "batch_size": result.get("batch_size"),
            "median_epoch_wall_s": result.get("median_epoch_wall_s"),
            "observed_steady_epoch_wall_s": statistics.median(observed_steady) if len(observed_steady) >= 2 else None,
            "complete_fold_wall_s": result.get("complete_fold_wall_s"),
            "epoch_wall_mad_s": result.get("epoch_wall_mad_s"),
            "memory": result.get("memory"), "failure_patterns": result.get("failure_patterns"),
            "reasons": result.get("reasons"),
            "minimum_epoch_grad_norm": min(grad_norms, default=None),
            "train_rows": result.get("train_rows"),
            "optimizer_steps_per_epoch": [r.get("train_optimizer_steps") for r in epochs],
            "epoch_wall_max_rank_s": [r.get("epoch_max_rank_s", r.get("epoch_wall_s_max_rank")) for r in epochs],
            "zero_gradient_batches": sum(r.get("train_zero_grad_batches", 0) for r in epochs),
            "steady_compile_graphs": sum(r.get("dynamo_unique_graphs_epoch_delta", 0) for r in epochs if r.get("epoch", 0) >= 3),
            "epoch_count": len(epochs)}
        row["calendar_runtime"] = [
            {key: value for key, value in epoch.items() if "calendar" in key}
            for epoch in epochs]
        row["admission_review"] = _verified_admission_review(trial, path, curve)
        row["effective_ok"] = bool(row["ok"] or
            (row["admission_review"] and row["admission_review"]["reviewed_ok"]))
        folds.append(row)
    for directory in sorted(args.root.glob("operators-*")):
        for path in sorted(directory.glob("*.json")):
            result = json.loads(path.read_text())
            if not isinstance(result, dict) or "case" not in result:
                continue
            operators.append({"queue": directory.name, "path": str(path), "case": result.get("case"),
                "dtype": result.get("dtype"), "status": result.get("status"),
                "shape": result.get("shapes"), "error": result.get("error"),
                "errors": result.get("errors"), "numeric_review_required": result.get("numeric_review_required"),
                "eager_forward_backward_ms": result.get("eager_forward_backward_ms"),
                "graph_forward_backward_ms": result.get("graph_forward_backward_ms"),
                "compile_and_first_call_s": result.get("compile_and_first_call_s"),
                "checkpoint": result.get("checkpoint"),
                "optimizer_step_ms": result.get("optimizer_step_ms")})
    args.output.mkdir(parents=True, exist_ok=True)
    atomic_write_json(args.output / "results.json", {"contract": "frontier_measurement_summary_v1",
        "root": str(args.root), "folds": folds, "operators": operators,
        "claims_global_compute_optimum": False})
    for label, values, fields in (
        ("folds", folds, ["trial", "ok", "effective_ok", "status", "lifecycle_ok", "batch_size", "median_epoch_wall_s",
            "observed_steady_epoch_wall_s", "complete_fold_wall_s", "epoch_wall_mad_s", "epoch_count",
            "reasons", "result", "source_sha256"]),
        ("operators", operators, ["queue", "case", "dtype", "status", "graph_forward_backward_ms",
            "eager_forward_backward_ms", "compile_and_first_call_s", "numeric_review_required", "error", "path"]),
    ):
        with (args.output / f"{label}.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fields, extrasaction="ignore")
            writer.writeheader(); writer.writerows(values)
    print(json.dumps({"fold_trials": len(folds), "accepted_folds": sum(bool(r["effective_ok"] and r["lifecycle_ok"]) for r in folds),
        "operator_trials": len(operators), "measured_operators": sum(r["status"] == "measured" for r in operators)}))


if __name__ == "__main__":
    main()
