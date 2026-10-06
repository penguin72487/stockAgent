#!/usr/bin/env python3
"""Compare completed canonical runs before promoting a performance setting.

This reads checkpoints with weights_only=True and NPZ with allow_pickle=False.
It never changes a checkpoint, replays a fill or executes a model. Timings and
run-start provenance vary; saved tensor/optimizer/RNG state and research outputs
must match exactly.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.training.checkpoint_contract import _stable_fingerprint  # noqa: E402
from stockagent.training.lifecycle import validate_completed_training_artifacts  # noqa: E402

EPOCH_SEMANTIC_FIELDS = (
    "epoch", "train_loss", "val_mean", "test_mean", "lr", "best_val_loss",
    "no_improve", "improved", "train_batches", "train_optimizer_steps",
    "train_grad_norm_before_clip_mean", "train_zero_grad_batches",
    "train_portfolio_final_alive", "train_first_dead_portfolio_batch",
    "train_first_settlement_default_row", "train_first_settlement_default_reason",
    "test_mean_scope", "test_sample_fold_id", "test_sample_rows",
)


def exact_differences(left: Any, right: Any, path: str = "value") -> list[str]:
    """Return bounded mismatch locations; preserve dtype and floating-point bits."""
    import numpy as np
    import torch

    if isinstance(left, torch.Tensor) and isinstance(right, torch.Tensor):
        if left.shape != right.shape or left.dtype != right.dtype:
            return [f"{path}: tensor shape/dtype differs"]
        a = left.detach().cpu().contiguous().reshape(-1).view(torch.uint8).numpy()
        b = right.detach().cpu().contiguous().reshape(-1).view(torch.uint8).numpy()
        return [] if np.array_equal(a, b) else [f"{path}: tensor bits differ"]
    if isinstance(left, np.ndarray) and isinstance(right, np.ndarray):
        if left.shape != right.shape or left.dtype != right.dtype:
            return [f"{path}: array shape/dtype differs"]
        return ([] if left.tobytes(order="C") == right.tobytes(order="C")
                else [f"{path}: array bits differ"])
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        if left.keys() != right.keys():
            return [f"{path}: mapping keys differ"]
        differences = []
        for key in left:
            differences.extend(exact_differences(left[key], right[key], f"{path}.{key}"))
            if len(differences) >= 100:
                return differences[:100]
        return differences
    if type(left) is not type(right):
        return [f"{path}: value type differs"]
    if isinstance(left, (list, tuple)):
        if len(left) != len(right):
            return [f"{path}: sequence length differs"]
        differences = []
        for index, (a, b) in enumerate(zip(left, right, strict=True)):
            differences.extend(exact_differences(a, b, f"{path}[{index}]"))
            if len(differences) >= 100:
                return differences[:100]
        return differences
    if isinstance(left, float) and math.isnan(left) and math.isnan(right):
        return []
    if isinstance(left, (str, bytes, int, float, bool, type(None))):
        return [] if left == right else [f"{path}: scalar differs"]
    raise TypeError(f"unsupported comparison value at {path}: {type(left).__name__}")


def checkpoint_differences(
    left: Any, right: Any, *, path: str,
    allow_runtime_profile_differences: bool = False,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Admit explicit CPU/output/fold lifecycle/eval schedules, never state.

    Both configuration checksums must be valid. All other configuration,
    semantic fingerprints and saved state remain subject to exact comparison.
    This never changes a checkpoint or its resume compatibility.
    """
    if not allow_runtime_profile_differences:
        return exact_differences(left, right, path), []
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return exact_differences(left, right, path), []
    manifests = [value.get("experiment_manifest") for value in (left, right)]
    if not all(isinstance(value, Mapping) for value in manifests):
        return exact_differences(left, right, path), []
    configurations = [value.get("configuration") for value in manifests]
    if not all(isinstance(value, Mapping) for value in configurations):
        return [f"{path}: runtime profile comparison requires recorded configurations"], []
    for index, (manifest, configuration) in enumerate(zip(manifests, configurations, strict=True)):
        if manifest.get("configuration_fingerprint") != _stable_fingerprint(configuration):
            return [f"{path}: configuration fingerprint invalid on side {index}"], []
    normalized = dict(configurations[1])
    admitted = []
    for section, field in (("environment", "cpu_threads"), ("runner", "output_dir"),
                           ("runner", "resume"), ("runner", "isolate_train_folds"),
                           ("training", "eval_backtest_chunk_rows")):
        if field in {"resume", "isolate_train_folds", "eval_backtest_chunk_rows"} and all(
            not isinstance(configuration.get(section), Mapping)
            or field not in configuration[section]
            for configuration in configurations
        ):
            # Old lightweight checkpoints did not record this runtime knob.
            continue
        values = []
        for configuration in configurations:
            fields = configuration.get(section)
            if not isinstance(fields, Mapping) or field not in fields:
                return [f"{path}: runtime profile field missing: {section}.{field}"], []
            value = fields[field]
            if ((field in {"cpu_threads", "eval_backtest_chunk_rows"} and
                 (type(value) is not int or value <= 0))
                    or (field in {"resume", "isolate_train_folds"} and type(value) is not bool)
                    or (field == "output_dir" and (not isinstance(value, str) or not value))):
                return [f"{path}: runtime profile field invalid: {section}.{field}"], []
            values.append(value)
        normalized[section] = {**normalized[section], field: values[0]}
        if values[0] != values[1]:
            admitted.append({"checkpoint": path, "field": f"configuration.{section}.{field}",
                             "left": values[0], "right": values[1]})
    if admitted:
        admitted.append({
            "checkpoint": path, "field": "configuration_fingerprint",
            "left": manifests[0]["configuration_fingerprint"],
            "right": manifests[1]["configuration_fingerprint"],
            "verified_from_configuration": True,
        })
    normalized_manifest = {**manifests[1], "configuration": normalized,
                           "configuration_fingerprint": manifests[0]["configuration_fingerprint"]}
    normalized_right = {**right, "experiment_manifest": normalized_manifest}
    return exact_differences(left, normalized_right, path), admitted


def compare_runs(
    left: Path, right: Path, *, folds: list[int],
    allow_runtime_profile_differences: bool = False,
) -> dict[str, Any]:
    import numpy as np
    import torch

    roots = [left.resolve(), right.resolve()]
    group_sets = [{p.name for p in root.glob("train_*") if p.is_dir()} for root in roots]
    if not group_sets[0] or group_sets[0] != group_sets[1]:
        raise ValueError("completed runs must have matching training groups")
    groups = sorted(group_sets[0])
    for root in roots:
        validate_completed_training_artifacts(root, fold_ids=folds, group_names=groups).require()
    differences: list[str] = []
    runtime_profile_differences: list[dict[str, Any]] = []
    compared: list[str] = []
    paths = [f"{group}/checkpoint_last.pt" for group in groups]
    paths.extend(f"fold_{fold:02d}/{name}" for fold in folds
                 for name in ("checkpoint_best.pt", "model.pt"))
    for name in paths:
        values = [torch.load(root/name, map_location="cpu", weights_only=True) for root in roots]
        changed, admitted = checkpoint_differences(
            *values, path=name,
            allow_runtime_profile_differences=allow_runtime_profile_differences,
        )
        differences.extend(changed)
        runtime_profile_differences.extend(admitted)
        compared.append(name)
    array_count = 0
    for fold in folds:
        for name in ("test_backtest.npz", "deployment_test_backtest.npz"):
            relative = f"fold_{fold:02d}/{name}"
            with np.load(roots[0]/relative, allow_pickle=False) as a, np.load(roots[1]/relative, allow_pickle=False) as b:
                if set(a.files) != set(b.files):
                    differences.append(f"{relative}: NPZ keys differ")
                else:
                    for key in a.files:
                        differences.extend(exact_differences(a[key], b[key], f"{relative}.{key}"))
                        array_count += 1
            compared.append(relative)
        for name in ("metrics.json", "mode_artifact_contract.json"):
            relative = f"fold_{fold:02d}/{name}"
            values = [json.loads((root/relative).read_bytes()) for root in roots]
            differences.extend(exact_differences(*values, path=relative))
            compared.append(relative)
    summaries = [json.loads((root/"summary.json").read_bytes()) for root in roots]
    differences.extend(exact_differences(*summaries, path="summary.json"))
    compared.append("summary.json")
    performance: dict[str, Any] = {}
    for group in groups:
        relative = f"{group}/epoch_curve.jsonl"
        curves = [[json.loads(line) for line in (root/relative).read_text().splitlines()
                   if line.strip()] for root in roots]
        semantic = [[{key: row[key] for key in EPOCH_SEMANTIC_FIELDS if key in row}
                     for row in rows] for rows in curves]
        differences.extend(exact_differences(*semantic, path=relative))
        compared.append(relative)
        performance[group] = [[{key: row.get(key) for key in (
            "epoch", "epoch_max_rank_s", "train_max_rank_s", "val_eval_s",
            "test_curve_s", "epoch_total_s", "timing_synchronized",
        )} for row in rows] for rows in curves]
    return {
        "schema_version": 1, "parity": not differences,
        "roots": [str(root) for root in roots], "folds": folds,
        "compared": compared, "npz_arrays_compared": array_count,
        "differences": differences, "performance": performance,
        "runtime_profile_difference_policy": (
            "explicit_cpu_output_fold_lifecycle_and_eval_replay_only" if allow_runtime_profile_differences else "strict"
        ),
        "declared_runtime_profile_differences": runtime_profile_differences,
        "comparison_tool_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "complete fold envelopes, exact saved checkpoint state and NPZ bits, semantic epoch/metrics parity; not live readiness",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--folds", type=int, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-runtime-profile-differences", action="store_true",
                        help="admit only CPU/output/fold-lifecycle/eval-replay with verified config fingerprints")
    args = parser.parse_args()
    result = compare_runs(args.left, args.right, folds=args.folds,
                          allow_runtime_profile_differences=args.allow_runtime_profile_differences)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("parity", "npz_arrays_compared", "differences")}))
    return 0 if result["parity"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
