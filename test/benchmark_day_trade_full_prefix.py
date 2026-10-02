"""Reproducible synthetic physical-prefix microbenchmark, never a readiness proof.

Run with run_fintech_python test/benchmark_day_trade_full_prefix.py --output ...
Both variants receive the same already-evaluated physical account. The baseline
is the pinned Git implementation; imports, source construction and model eval
are not included in the prefix samples. This does not measure provider latency,
cold source reconstruction, CUDA throughput or the whole inference workflow.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import torch

import stockagent.training.trainer as trainer
from stockagent.backtest.tw_day_trade_carry import _compact_detached_carry_state
from stockagent.data_sync.desync_snapshots import atomic_write_json
from test_day_trade_carry_training import Policy, fixture
from test_day_trade_inference_source import inference_config


def _baseline(commit: str):
    resolved = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", commit + "^{commit}"],
        cwd=ROOT, text=True,
    ).strip()
    source = subprocess.check_output(
        ["git", "show", resolved + ":stockagent/training/trainer.py"],
        cwd=ROOT, text=True,
    )
    node = next(
        node for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "_replay_physical_carry_split_prefix"
    )
    namespace = dict(vars(trainer))
    exec(compile(ast.Module(body=[node], type_ignores=[]), resolved, "exec"), namespace)
    return namespace[node.name], resolved


def _assert_account_parity(actual, expected):
    for name in ("minute_nav", "shares_history", "strategy_returns", "turnovers",
                 "weights_history", "requested_weights_history", "settlement_default"):
        np.testing.assert_allclose(
            getattr(actual, name), getattr(expected, name), rtol=0, atol=1e-8,
        )
    actual_state = _compact_detached_carry_state(actual.day_trade_carry_state)
    expected_state = _compact_detached_carry_state(expected.day_trade_carry_state)
    assert actual_state.last_session_day == expected_state.last_session_day
    torch.testing.assert_close(actual_state.last_nav, expected_state.last_nav, rtol=0, atol=1e-8)
    assert torch.equal(actual_state.alive, expected_state.alive)
    for field in fields(actual_state.inventory):
        torch.testing.assert_close(
            getattr(actual_state.inventory, field.name), getattr(expected_state.inventory, field.name),
            rtol=0, atol=1e-8,
        )


def _input_sha256(*values):
    digest = hashlib.sha256()

    def visit(value):
        digest.update(type(value).__name__.encode() + b"\0")
        if isinstance(value, torch.Tensor):
            visit(value.detach().cpu().numpy())
        elif isinstance(value, np.ndarray):
            digest.update(str(value.dtype).encode() + str(value.shape).encode())
            digest.update(np.ascontiguousarray(value).tobytes())
        elif is_dataclass(value):
            for field in fields(value):
                digest.update(field.name.encode() + b"\0")
                visit(getattr(value, field.name))
        elif isinstance(value, (tuple, list)):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for key in sorted(value):
                visit(key)
                visit(value[key])
        else:
            digest.update(json.dumps(value, sort_keys=True, default=str).encode())

    for value in values:
        visit(value)
    return digest.hexdigest()


def benchmark(*, rows=10, rounds=2, baseline_commit="HEAD"):
    if not 1 <= rows <= 40 or not 1 <= rounds <= 5:
        raise ValueError("bounded synthetic benchmark requires rows 1..40 and rounds 1..5")
    baseline, commit = _baseline(baseline_commit)
    split, runtime, _ = fixture(rows=rows)
    source = runtime.day_trade_carry_source
    recorded_tensor, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        Policy(), None, split, device=torch.device("cpu"), amp_dtype=None,
        non_blocking=False, long_only=False, buy_fee_rate=0.001425, sell_fee_rate=0.002925,
        max_turnover_ratio=0.0, gross_leverage=1.0, min_trade_weight=0.0,
        model_chunk_rows=2, backtest_chunk_rows=2, portfolio_activation="pre_normalized",
        max_volume_participation=0.5, volume_participation_equity=10_000_000.0,
        execution_runtime=runtime,
    )
    recorded = recorded_tensor.to_numpy()
    original_nav = np.array(recorded.minute_nav, copy=True)
    original_state = recorded.day_trade_carry_state.detached()
    config = inference_config()
    original_digest = _input_sha256(recorded, runtime, config)
    samples = []
    variants = {
        "baseline_replay": baseline,
        "recorded_endpoint": trainer._replay_physical_carry_split_prefix,
    }
    for variant in ("baseline_replay", "recorded_endpoint", "recorded_endpoint", "baseline_replay") * rounds:
        started = time.perf_counter()
        observed = variants[variant](recorded, split, len(split), runtime=runtime, config=config)
        elapsed = time.perf_counter() - started
        _assert_account_parity(observed, recorded)
        samples.append({"variant": variant, "elapsed_seconds": elapsed})
    np.testing.assert_array_equal(recorded.minute_nav, original_nav)
    for field in fields(original_state.inventory):
        torch.testing.assert_close(
            getattr(recorded.day_trade_carry_state.inventory, field.name),
            getattr(original_state.inventory, field.name), rtol=0, atol=0,
        )
    assert runtime.day_trade_carry_source is source
    assert _input_sha256(recorded, runtime, config) == original_digest
    return {
        "schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
        "scope": "ABBA_same_eager_synthetic_account_full_owned_prefix_only",
        "baseline_git_commit": commit,
        "current_trainer_sha256": hashlib.sha256((ROOT / "stockagent/training/trainer.py").read_bytes()).hexdigest(),
        "rows": rows, "symbols": 2, "minute_points_per_row": 270,
        "same_account_parity": True, "input_unchanged": True,
        "input_sha256": original_digest,
        "formal_artifact_writes": 0, "orders": 0, "samples": samples,
        "median_seconds": {
            variant: statistics.median(sample["elapsed_seconds"] for sample in samples if sample["variant"] == variant)
            for variant in variants
        },
        "excluded_from_samples": ["imports", "source_preparation", "model_inference", "artifact_writes",
                                  "plots", "global_stitched_replay", "provider", "cold_filesystem", "CUDA"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--baseline-commit", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = benchmark(rows=args.rows, rounds=args.rounds, baseline_commit=args.baseline_commit)
    atomic_write_json(args.output, report)
    print(json.dumps({"receipt": str(args.output), "median_seconds": report["median_seconds"]}))


if __name__ == "__main__":
    main()
