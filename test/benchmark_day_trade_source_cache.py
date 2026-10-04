"""ABBA cache-path benchmark on receipt-backed synthetic source files.

Includes all dense/packed/compact source access and a separate complete two-
session FIFO replay. Excludes imports, provider requests, real full-market
history, model inference, reporting/plots, CUDA and cold filesystem latency.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BASELINE_COMMIT = "862fc3e08b156a411d1b64f54e925faed44247cc"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import torch

import stockagent.data.tw_day_trade_carry_source as module
from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession, run_day_trade_carry_sessions,
)
from stockagent.training.day_trade_carry_bridge import PackedDayTradeCarrySession
from stockagent.data_sync.desync_snapshots import atomic_write_json
from benchmark_day_trade_full_prefix import _input_sha256
from test_day_trade_carry_source import _physical_source_inputs


def _baseline(commit):
    resolved = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", commit + "^{commit}"],
        cwd=ROOT, text=True,
    ).strip()
    raw = subprocess.check_output(
        ["git", "show", resolved + ":stockagent/data/tw_day_trade_carry_source.py"],
        cwd=ROOT, text=True,
    )
    node = next(node for node in ast.parse(raw).body if isinstance(node, ast.FunctionDef)
                and node.name == "build_prepared_day_trade_carry_source")
    namespace = dict(vars(module))
    exec(compile(ast.Module(body=[node], type_ignores=[]), resolved, "exec"), namespace)
    return namespace[node.name], resolved


@contextmanager
def _budget(setting):
    name = "STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB"
    previous = os.environ.get(name)
    os.environ[name] = setting
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _retained_bytes(source):
    if source.runtime_cache_info is not None:
        return source.runtime_cache_info()["retained_tensor_bytes"]
    # Independent census of the pinned baseline's three actual cache dictionaries.
    storages, visited = {}, set()
    for loader in (source.session_loader, source.packed_session_loader, source.compact_session_loader):
        for cell in loader.__closure__ or ():
            values = cell.cell_contents
            if not isinstance(values, dict) or id(values) in visited:
                continue
            visited.add(id(values))
            for value in values.values():
                if isinstance(value, (DayTradeCarrySession, PackedDayTradeCarrySession)):
                    storages.update(module._session_tensor_storages(value))
    return sum(storages.values())


def _assert_exact(actual, expected):
    if isinstance(actual, torch.Tensor):
        torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)
    elif is_dataclass(actual):
        for field in fields(actual):
            _assert_exact(getattr(actual, field.name), getattr(expected, field.name))
    else:
        assert actual == expected


def _file_hashes(paths):
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def benchmark(*, rounds=2, passes=2, baseline_commit=BASELINE_COMMIT):
    if not 1 <= rounds <= 3 or not 1 <= passes <= 5:
        raise ValueError("bounded synthetic benchmark requires rounds 1..3 and passes 1..5")
    baseline, commit = _baseline(baseline_commit)
    builders = {"baseline": baseline, "shared_bounded": module.build_prepared_day_trade_carry_source}
    samples, expected_digests = [], {}
    expected_account = None
    with tempfile.TemporaryDirectory(prefix="stockagent-source-cache-benchmark-") as temp:
        panel, public, minute = _physical_source_inputs(Path(temp))
        input_sha256 = _input_sha256(vars(panel))
        kwargs = dict(
            panel=panel, minute_root=minute,
            public_feature_path=public / "features/tw_public_stock_daily.parquet",
            cache_dir=Path(temp) / "cache", allow_daily_proxy=True,
            daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid",
        )
        with _budget("0"):
            seed = module.build_prepared_day_trade_carry_source(**kwargs)
        cache_root = Path(seed.audit_receipt["cache_manifest"]).parent
        immutable_files = list(public.rglob("*")) + list(minute.rglob("*"))
        immutable_files += list(cache_root.glob("session-*.npz"))
        before = _file_hashes([path for path in immutable_files if path.is_file()])
        release_id = seed.release_id
        for setting in ("1", "0.00001", "0"):
            for variant in ("baseline", "shared_bounded", "shared_bounded", "baseline") * rounds:
                with _budget(setting):
                    prep_started = time.perf_counter()
                    source = builders[variant](**kwargs)
                    prep_s = time.perf_counter() - prep_started
                assert source.release_id == release_id
                loads, durations = [], []
                original_load = np.load

                def counted_load(path, *args, **options):
                    if Path(path).name.startswith("session-") and str(path).endswith(".npz"):
                        loads.append(str(path))
                    return original_load(path, *args, **options)

                np.load = counted_load
                try:
                    for pass_index in range(passes):
                        duration = 0.0
                        for row in range(len(source)):
                            for kind, loader in (
                                ("dense", source.session_at), ("packed", source.packed_session_loader),
                                ("compact", source.compact_session_at),
                            ):
                                started = time.perf_counter()
                                value = loader(row)
                                duration += time.perf_counter() - started
                                # Parity digest is outside the measured accessor interval.
                                key = row, kind
                                digest = _input_sha256(value)
                                assert digest == expected_digests.setdefault(key, digest)
                        durations.append(duration)
                    retained = _retained_bytes(source)
                    cache_info = source.runtime_cache_info() if source.runtime_cache_info else None
                    counted_npz_loads = len(loads)
                finally:
                    np.load = original_load
                replay_started = time.perf_counter()
                sessions = tuple(source.session_at(row) for row in range(len(source)))
                weights = torch.tensor([[0.2, -0.1, 0.1]] * len(source), dtype=torch.float64)
                rate = lambda number: weights.new_full((3,), number)
                with torch.no_grad():
                    account = run_day_trade_carry_sessions(
                        weights, sessions, can_enter=torch.ones_like(weights),
                        buy_fee_rate=rate(0.001425), day_sell_fee_rate=rate(0.002925),
                        normal_sell_fee_rate=rate(0.004425), rebate_rate=rate(0.00114),
                        initial_capital=10_000_000.0,
                    )
                replay_s = time.perf_counter() - replay_started
                assert account.minute_nav.shape == (len(source), 270)
                if expected_account is None:
                    expected_account = account
                _assert_exact(account, expected_account)
                samples.append(dict(
                    variant=variant, budget_setting_gib=setting,
                    source_preparation_s=prep_s, accessor_pass_s=durations,
                    session_npz_loads=counted_npz_loads,
                    retained_tensor_bytes=retained, cache_info=cache_info,
                    source_to_complete_fifo_replay_s=replay_s,
                ))
                del value, source, sessions, account
                gc.collect()
        assert _file_hashes([Path(path) for path in before]) == before
        assert _input_sha256(vars(panel)) == input_sha256
    summaries = {}
    for setting in ("1", "0.00001", "0"):
        summaries[setting] = {}
        for variant in builders:
            group = [sample for sample in samples
                     if sample["budget_setting_gib"] == setting and sample["variant"] == variant]
            summaries[setting][variant] = dict(
                median_first_accessor_pass_s=statistics.median(sample["accessor_pass_s"][0] for sample in group),
                median_all_accessor_pass_s=statistics.median(sum(sample["accessor_pass_s"]) for sample in group),
                median_complete_fifo_replay_s=statistics.median(sample["source_to_complete_fifo_replay_s"] for sample in group),
                retained_tensor_bytes=group[-1]["retained_tensor_bytes"],
                session_npz_loads=group[-1]["session_npz_loads"],
            )
    return dict(
        observed_at=datetime.now(timezone.utc).isoformat(), baseline_commit=commit,
        process_pid=os.getpid(), process_maxrss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        scope="receipt-backed synthetic 2-session/3-symbol cache paths and complete FIFO replay, CPU eager",
        samples=samples, summaries=summaries, release_id=release_id,
        exact_source_and_account_parity=True, immutable_files_unchanged=True,
        input_sha256=input_sha256, input_unchanged=True,
        source_session_sha256={f"{row}:{kind}": digest for (row, kind), digest in expected_digests.items()},
        minute_points_per_session=270, orders=0, formal_artifact_writes=0,
        excluded_from_accessor_samples=["imports", "source_preparation", "parity_digest", "FIFO_replay",
                                        "model_inference", "reports", "plots", "CUDA", "cold_filesystem"],
        versions={"python": sys.version, "torch": torch.__version__, "numpy": np.__version__},
        code_sha256={str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in (Path(module.__file__), Path(__file__))},
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--passes", type=int, default=2)
    parser.add_argument("--baseline-commit", default=BASELINE_COMMIT)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = benchmark(rounds=args.rounds, passes=args.passes, baseline_commit=args.baseline_commit)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summaries"], indent=2))


if __name__ == "__main__":
    main()
