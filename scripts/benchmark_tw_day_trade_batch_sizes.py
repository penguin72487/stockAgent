#!/usr/bin/env python3
"""Measure the production TW daily day-trade batch-size frontier.

The sweep intentionally changes only the global training batch size (plus the
number of epochs/early-stopping guard needed to obtain repeated measurements).
Every candidate uses a fresh artifact directory and the canonical train,
validation, test-curve, loss, and backtest path.  Both the single-device and
DDP production paths are supported because a chronological settlement ledger
can make one GPU faster than two even when the model itself parallelizes.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config


DEFAULT_CONFIG = (
    ROOT
    / "artifacts/ablations/tw_day_trade_daily_tplus2_close_commission20_v3_ofat"
    / "generated_configs/attention_pooling.yaml"
)
DEFAULT_BATCH_SIZES = (64, 128, 256, 512, 1024, 2048)
_TRAIN_ROWS_RE = re.compile(r"prepared train windowed tensors .*? rows=(\d+)")
_FAILURE_PATTERNS = (
    "CUDA out of memory",
    "OutOfMemoryError",
    "CUDA error",
    "ChildFailedError",
)
_FALLBACK_KEYS = (
    "bt_compile_failures",
    "bt_prep_compile_failures",
    "bt_runtime_fallback_calls",
    "bt_eager_runner_calls",
    "bt_prep_compile_nonhit",
    "bt_compile_nonhit",
)


def _is_power_of_two(value: int) -> bool:
    return value > 0 and value & (value - 1) == 0


def _parse_batch_sizes(
    raw: str,
    *,
    world_size: int,
    require_power_of_two: bool = True,
) -> list[int]:
    values: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError as exc:
            raise ValueError(f"invalid batch size {part!r}") from exc
        if value <= 0:
            raise ValueError(f"batch size must be positive, got {value}")
        if require_power_of_two and not _is_power_of_two(value):
            raise ValueError(f"batch size must be a power of two, got {value}")
        if value % world_size != 0:
            raise ValueError(
                f"global batch size {value} must be divisible by world size {world_size}"
            )
        local_batch = value // world_size
        if require_power_of_two and not _is_power_of_two(local_batch):
            raise ValueError(
                f"per-rank batch size must be a power of two, got {local_batch} "
                f"from global={value}, world_size={world_size}"
            )
        if value not in values:
            values.append(value)
    if not values:
        raise ValueError("at least one batch size is required")
    return sorted(values)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON at {path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"epoch row at {path}:{line_number} is not an object")
            rows.append(value)
    return rows


def _single_epoch_curve(run_dir: Path) -> Path:
    curves = sorted(run_dir.rglob("epoch_curve.jsonl"))
    if len(curves) != 1:
        raise ValueError(
            f"expected exactly one epoch_curve.jsonl under {run_dir}, found {len(curves)}"
        )
    return curves[0]


def _finite_float(value: object) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _median(values: Iterable[float]) -> float:
    materialized = [float(value) for value in values]
    if not materialized:
        raise ValueError("cannot take the median of an empty sequence")
    return float(statistics.median(materialized))


def _median_absolute_deviation(values: Iterable[float]) -> float:
    materialized = [float(value) for value in values]
    center = _median(materialized)
    return _median(abs(value - center) for value in materialized)


def _parse_train_rows(log_path: Path) -> int:
    matches = [int(value) for value in _TRAIN_ROWS_RE.findall(log_path.read_text(errors="replace"))]
    if not matches:
        raise ValueError(f"could not find prepared train row count in {log_path}")
    # DDP prints one identical line per rank. The selected late fold is the
    # largest match if startup/preflight also prepared smaller tensors.
    return max(matches)


def _gpu_memory_summary(path: Path, *, gpu_indices: set[int]) -> dict[str, Any]:
    peaks: dict[int, tuple[float, float]] = {}
    if path.is_file():
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    index = int(str(row["index"]).strip())
                    used_mib = float(str(row["memory.used"]).strip())
                    total_mib = float(str(row["memory.total"]).strip())
                except (KeyError, TypeError, ValueError):
                    continue
                if index not in gpu_indices or total_mib <= 0:
                    continue
                old = peaks.get(index)
                if old is None or used_mib > old[0]:
                    peaks[index] = (used_mib, total_mib)
    if set(peaks) != gpu_indices:
        return {
            "ok": False,
            "reason": f"missing GPU memory samples: expected={sorted(gpu_indices)} got={sorted(peaks)}",
        }
    per_gpu = {
        str(index): {
            "peak_used_mib": used,
            "total_mib": total,
            "peak_fraction": used / total,
            "headroom_gib": (total - used) / 1024.0,
        }
        for index, (used, total) in sorted(peaks.items())
    }
    return {
        "ok": True,
        "per_gpu": per_gpu,
        "max_peak_fraction": max(item["peak_fraction"] for item in per_gpu.values()),
        "min_headroom_gib": min(item["headroom_gib"] for item in per_gpu.values()),
    }


def _score_curve(
    rows: list[dict[str, Any]],
    *,
    skip_epochs: int,
    minimum_steady_epochs: int,
    train_rows: int,
    global_batch_size: int,
    memory: dict[str, Any],
    max_peak_fraction: float,
    min_headroom_gib: float,
) -> dict[str, Any]:
    steady = [row for row in rows if int(row.get("epoch", 0) or 0) > skip_epochs]
    reasons: list[str] = []
    if len(steady) < minimum_steady_epochs:
        reasons.append(
            f"need at least {minimum_steady_epochs} steady epochs after epoch {skip_epochs}, "
            f"found {len(steady)}"
        )

    required_finite = ("epoch_wall_s", "train_total_s", "train_loss", "val_mean", "test_mean")
    for row in steady:
        epoch = int(row.get("epoch", 0) or 0)
        for key in required_finite:
            value = _finite_float(row.get(key))
            if value is None or (key.endswith("_s") and value <= 0):
                reasons.append(f"epoch {epoch} has invalid {key}={row.get(key)!r}")
        if int(row.get("train_zero_grad_batches", 0) or 0) != 0:
            reasons.append(f"epoch {epoch} contains zero-gradient optimizer batches")
        grad_norm = _finite_float(row.get("train_grad_norm_before_clip_mean"))
        if grad_norm is None or grad_norm <= 0:
            reasons.append(f"epoch {epoch} has non-positive/non-finite gradient norm")
        if int(row.get("dynamo_unique_graphs_epoch_delta", 0) or 0) != 0:
            reasons.append(f"epoch {epoch} compiled a new Dynamo graph after warmup")
        for key in _FALLBACK_KEYS:
            if int(row.get(key, 0) or 0) != 0:
                reasons.append(f"epoch {epoch} has {key}={row.get(key)!r}")

    if not memory.get("ok"):
        reasons.append(str(memory.get("reason", "GPU memory sampling failed")))
    else:
        peak_fraction = float(memory["max_peak_fraction"])
        headroom_gib = float(memory["min_headroom_gib"])
        if peak_fraction > max_peak_fraction:
            reasons.append(
                f"peak VRAM fraction {peak_fraction:.3f} exceeds limit {max_peak_fraction:.3f}"
            )
        if headroom_gib < min_headroom_gib:
            reasons.append(
                f"minimum VRAM headroom {headroom_gib:.2f} GiB is below {min_headroom_gib:.2f} GiB"
            )

    if reasons:
        return {
            "ok": False,
            "reasons": sorted(set(reasons)),
            "steady_epochs": [int(row.get("epoch", 0) or 0) for row in steady],
            "memory": memory,
        }

    epoch_wall = [float(row["epoch_wall_s"]) for row in steady]
    train_wall = [float(row["train_total_s"]) for row in steady]
    train_batches = [int(row.get("train_batches", 0) or 0) for row in steady]
    expected_batches = math.ceil(train_rows / global_batch_size)
    if any(value != expected_batches for value in train_batches):
        return {
            "ok": False,
            "reasons": [
                f"train batch count disagrees with row count: expected={expected_batches}, "
                f"observed={train_batches}"
            ],
            "steady_epochs": [int(row.get("epoch", 0) or 0) for row in steady],
            "memory": memory,
        }

    median_epoch_wall = _median(epoch_wall)
    median_train_wall = _median(train_wall)
    padded_slots = expected_batches * global_batch_size
    return {
        "ok": True,
        "steady_epochs": [int(row.get("epoch", 0) or 0) for row in steady],
        "train_rows": train_rows,
        "global_batch_size": global_batch_size,
        "local_batch_size": None,
        "train_batches": expected_batches,
        "padded_slots": padded_slots,
        "padding_fraction": (padded_slots - train_rows) / padded_slots,
        "median_epoch_wall_s": median_epoch_wall,
        "epoch_wall_mad_s": _median_absolute_deviation(epoch_wall),
        "median_train_wall_s": median_train_wall,
        "train_wall_mad_s": _median_absolute_deviation(train_wall),
        "complete_epoch_real_rows_per_s": train_rows / median_epoch_wall,
        "train_phase_real_rows_per_s": train_rows / median_train_wall,
        "median_grad_norm_before_clip": _median(
            float(row["train_grad_norm_before_clip_mean"]) for row in steady
        ),
        "memory": memory,
    }


def _read_busy_compute_processes() -> list[dict[str, str]]:
    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
            "--format=csv,noheader,nounits",
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"nvidia-smi compute-process query failed: {result.stderr.strip()}")
    processes: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) >= 4 and parts[1]:
            processes.append(
                {
                    "gpu_uuid": parts[0],
                    "pid": parts[1],
                    "process_name": parts[2],
                    "used_memory_mib": parts[3],
                }
            )
    return processes


def _wait_for_idle_gpus(*, wait: bool, poll_s: float) -> None:
    last_report = 0.0
    while True:
        busy = _read_busy_compute_processes()
        if not busy:
            return
        if not wait:
            raise RuntimeError(
                "GPU benchmark requires idle GPUs; active compute processes="
                + json.dumps(busy, ensure_ascii=False)
            )
        now = time.monotonic()
        if now - last_report >= 60.0 or last_report == 0.0:
            print(
                "[batch-benchmark] waiting for idle GPUs: "
                + json.dumps(busy, ensure_ascii=False),
                flush=True,
            )
            last_report = now
        time.sleep(poll_s)


def _sample_gpus(stop: threading.Event, path: Path, interval_s: float) -> None:
    fields = (
        "timestamp,index,memory.used,memory.total,utilization.gpu,"
        "utilization.memory,power.draw"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields.split(","))
        while not stop.is_set():
            result = subprocess.run(
                [
                    "nvidia-smi",
                    f"--query-gpu={fields}",
                    "--format=csv,noheader,nounits",
                ],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            for line in result.stdout.splitlines():
                if line.strip():
                    writer.writerow([part.strip() for part in line.split(",")])
            handle.flush()
            stop.wait(interval_s)


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=20)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)


def _next_attempt_dir(batch_dir: Path) -> Path:
    attempt = 1
    while (batch_dir / f"attempt_{attempt:02d}").exists():
        attempt += 1
    return batch_dir / f"attempt_{attempt:02d}"


def _write_candidate_config(
    base: dict[str, Any],
    *,
    path: Path,
    output_dir: Path,
    batch_size: int,
    epochs: int,
    start_fold: int,
) -> None:
    config = deepcopy(base)
    runner = config.setdefault("runner", {})
    training = config.setdefault("training", {})
    runner.update(
        {
            "output_dir": str(output_dir),
            "resume": False,
            "post_train_infer": False,
            "start_fold": start_fold,
            "isolate_train_folds": False,
        }
    )
    training.update(
        {
            "batch_size_train": batch_size,
            "auto_batch_size": False,
            "epochs": epochs,
            # The short measurement must not stop before enough steady epochs
            # exist. This does not alter any epoch's compute path.
            "early_stopping_no_improve_ratio": 0.0,
        }
    )
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _plain_config_value(value: Any) -> Any:
    """Convert the resolved dataclass tree to portable safe-YAML values."""

    if isinstance(value, dict):
        return {str(key): _plain_config_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_config_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _validate_source_contract(config: dict[str, Any]) -> dict[str, Any]:
    training = config.get("training")
    trading = config.get("trading")
    data = config.get("data")
    if (
        not isinstance(training, dict)
        or not isinstance(trading, dict)
        or not isinstance(data, dict)
    ):
        raise ValueError(
            "source config must contain data, training, and trading mappings"
        )
    expected = {
        "trading.execution_mode": "tw_day_trade",
        "trading.frequency": "daily",
        "training.loss_type": "log_utility",
    }
    actual = {
        "trading.execution_mode": trading.get("execution_mode"),
        "trading.frequency": trading.get("frequency"),
        "training.loss_type": training.get("loss_type"),
    }
    disagreements = [
        f"{key}: expected={expected_value!r} actual={actual[key]!r}"
        for key, expected_value in expected.items()
        if actual[key] != expected_value
    ]
    if disagreements:
        raise ValueError("source config is not the requested daily day-trade contract: " + "; ".join(disagreements))
    model_name = str(training.get("model_name", "")).strip().lower()
    if model_name not in {
        "financial_transformer",
        "executable_portfolio_transformer",
    }:
        raise ValueError(
            "batch benchmark supports financial_transformer or "
            f"executable_portfolio_transformer, got {model_name!r}"
        )
    actual["training.model_name"] = model_name
    if model_name == "executable_portfolio_transformer":
        minute_execution = bool(data.get("day_trade_minute_execution_root"))
        actual["objective"] = (
            "strict_minute" if minute_execution else "daily_stateful_carry"
        )
        if minute_execution:
            if bool(trading.get("tw_day_trade_unlimited_margin_conversion")):
                raise ValueError(
                    "strict-minute executable benchmark must not stack the daily "
                    "stateful margin-conversion ledger"
                )
            if bool(data.get("day_trade_minute_execution_allow_daily_proxy", True)):
                raise ValueError(
                    "strict-minute executable benchmark requires "
                    "day_trade_minute_execution_allow_daily_proxy=false"
                )
        elif not bool(trading.get("tw_day_trade_unlimited_margin_conversion")):
            raise ValueError(
                "daily executable portfolio benchmark requires stateful margin conversion"
            )
        if bool(trading.get("tw_short_capacity_limit_enabled")):
            raise ValueError(
                "executable portfolio benchmark requires unbounded short capacity"
            )
    return actual


def _run_environment_preflight(python: Path, *, output_path: Path) -> None:
    command = [
        str(python),
        "scripts/check_environment.py",
        "--require-cuda",
        "--strict",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output_path.write_text(result.stdout, encoding="utf-8")
    if result.returncode != 0:
        raise RuntimeError(
            f"strict CUDA environment preflight failed with return code {result.returncode}; "
            f"inspect {output_path}"
        )


def _run_candidate(args: argparse.Namespace, base: dict[str, Any], batch_size: int) -> dict[str, Any]:
    batch_dir = args.output_root / f"batch_{batch_size:04d}"
    prior_result = batch_dir / "result.json"
    if prior_result.is_file():
        prior = json.loads(prior_result.read_text(encoding="utf-8"))
        if prior.get("ok"):
            print(f"[batch-benchmark] reuse completed batch={batch_size}", flush=True)
            return prior

    attempt_dir = _next_attempt_dir(batch_dir)
    attempt_dir.mkdir(parents=True, exist_ok=False)
    config_path = attempt_dir / "config.yaml"
    log_path = attempt_dir / "train.log"
    gpu_path = attempt_dir / "gpu_samples.csv"
    run_dir = attempt_dir / "artifacts"
    _write_candidate_config(
        base,
        path=config_path,
        output_dir=run_dir,
        batch_size=batch_size,
        epochs=args.epochs,
        start_fold=args.start_fold,
    )

    env = os.environ.copy()
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "CUDA_VISIBLE_DEVICES": args.cuda_visible_devices,
            "STOCKAGENT_CPU_THREADS": str(args.cpu_threads),
            "STOCKAGENT_TORCH_COMPILE_THREADS": str(args.compile_threads),
            "STOCKAGENT_POLARS_THREADS": str(args.polars_threads),
            "POLARS_MAX_THREADS": str(args.polars_threads),
            "RAYON_NUM_THREADS": str(args.polars_threads),
            "STOCKAGENT_BACKTEST_COMPILE_PREP": "1",
            "STOCKAGENT_STRICT_NO_FALLBACK": "1",
            "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
            "PYTORCH_ALLOC_CONF": "expandable_segments:True",
        }
    )
    cmd = [
        str(args.python),
        "train.py",
        "--config",
        str(config_path),
        "--output-dir",
        str(run_dir),
        "--start-fold",
        str(args.start_fold),
        "--max-folds",
        "1",
        "--epochs",
        str(args.epochs),
        "--no-resume",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
        "--multi-gpu-strategy",
        args.multi_gpu_strategy,
        "--batch-size-train",
        str(batch_size),
        "--batch-size-eval",
        str(args.batch_size_eval),
        "--cpu-threads",
        str(args.cpu_threads),
        "--torch-compile-threads",
        str(args.compile_threads),
    ]
    print(
        f"[batch-benchmark] start strategy={args.multi_gpu_strategy} "
        f"global_batch={batch_size} local_batch={batch_size // args.world_size}",
        flush=True,
    )
    stop = threading.Event()
    sampler = threading.Thread(
        target=_sample_gpus,
        args=(stop, gpu_path, args.gpu_sample_interval_s),
        daemon=True,
    )
    sampler.start()
    started = time.perf_counter()
    status = "ok"
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            cmd,
            cwd=ROOT,
            env=env,
            text=True,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            return_code = process.wait(timeout=args.timeout_s)
        except subprocess.TimeoutExpired:
            status = "timeout"
            _terminate_process_group(process)
            return_code = process.returncode
    stop.set()
    sampler.join(timeout=5)
    elapsed_s = time.perf_counter() - started

    result: dict[str, Any] = {
        "ok": False,
        "status": status,
        "return_code": return_code,
        "batch_size": batch_size,
        "local_batch_size": batch_size // args.world_size,
        "world_size": args.world_size,
        "elapsed_s": elapsed_s,
        "attempt_dir": str(attempt_dir),
        "command": cmd,
        "log_path": str(log_path),
        "gpu_samples_path": str(gpu_path),
    }
    log_text = log_path.read_text(errors="replace")
    result["failure_patterns"] = [pattern for pattern in _FAILURE_PATTERNS if pattern in log_text]
    if return_code == 0 and status == "ok":
        try:
            curve_path = _single_epoch_curve(run_dir)
            rows = _read_jsonl(curve_path)
            train_rows = _parse_train_rows(log_path)
            memory = _gpu_memory_summary(gpu_path, gpu_indices=args.gpu_indices)
            score = _score_curve(
                rows,
                skip_epochs=args.skip_epochs,
                minimum_steady_epochs=args.minimum_steady_epochs,
                train_rows=train_rows,
                global_batch_size=batch_size,
                memory=memory,
                max_peak_fraction=args.max_peak_vram_fraction,
                min_headroom_gib=args.min_vram_headroom_gib,
            )
            result.update(score)
            result["epoch_curve"] = str(curve_path)
            if result.get("ok"):
                result["local_batch_size"] = batch_size // args.world_size
        except Exception as exc:
            result["reasons"] = [f"artifact scoring failed: {type(exc).__name__}: {exc}"]
    else:
        result["reasons"] = [f"training failed: status={status} return_code={return_code}"]

    batch_dir.mkdir(parents=True, exist_ok=True)
    prior_result.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(
        f"[batch-benchmark] done batch={batch_size} ok={result['ok']} "
        f"elapsed={elapsed_s:.1f}s",
        flush=True,
    )
    return result


def _select_winner(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [
        row
        for row in results
        if row.get("ok") and _finite_float(row.get("complete_epoch_real_rows_per_s")) is not None
    ]
    if not valid:
        return None
    return max(valid, key=lambda row: float(row["complete_epoch_real_rows_per_s"]))


def _default_output_root() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return ROOT / "artifacts/benchmarks" / f"tw_day_trade_daily_batch_power2_{timestamp}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Find the highest-throughput power-of-two global batch for the canonical "
            "single-device or DDP TW daily day-trade workload."
        )
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument(
        "--batch-sizes",
        default=",".join(str(value) for value in DEFAULT_BATCH_SIZES),
    )
    parser.add_argument(
        "--allow-non-power-of-two",
        action="store_true",
        help=(
            "allow fixed global/local batches outside the default power-of-two "
            "frontier (useful for bounded searches between a winner and OOM)"
        ),
    )
    parser.add_argument("--batch-size-eval", type=int, default=128)
    parser.add_argument("--start-fold", type=int, default=12)
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--skip-epochs", type=int, default=2)
    parser.add_argument("--minimum-steady-epochs", type=int, default=4)
    parser.add_argument("--cuda-visible-devices", default="0,1")
    parser.add_argument(
        "--multi-gpu-strategy",
        choices=("none", "distributed_data_parallel"),
        default="distributed_data_parallel",
        help="production training topology to benchmark",
    )
    parser.add_argument("--python", type=Path, default=Path(sys.executable).resolve())
    parser.add_argument("--cpu-threads", type=int, default=112)
    parser.add_argument("--compile-threads", type=int, default=16)
    parser.add_argument("--polars-threads", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=1800.0)
    parser.add_argument("--gpu-sample-interval-s", type=float, default=0.25)
    parser.add_argument("--max-peak-vram-fraction", type=float, default=0.90)
    parser.add_argument("--min-vram-headroom-gib", type=float, default=3.0)
    parser.add_argument("--wait-for-idle", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--idle-poll-s", type=float, default=30.0)
    args = parser.parse_args()

    args.config = args.config.expanduser().resolve()
    if not args.config.is_file():
        raise SystemExit(f"config does not exist: {args.config}")
    args.output_root = (
        args.output_root.expanduser().resolve() if args.output_root is not None else _default_output_root()
    )
    gpu_ids = [part.strip() for part in args.cuda_visible_devices.split(",") if part.strip()]
    if not gpu_ids or any(not value.isdigit() for value in gpu_ids):
        raise SystemExit("--cuda-visible-devices must be a comma-separated list of numeric GPU indices")
    args.gpu_indices = {int(value) for value in gpu_ids}
    args.world_size = len(gpu_ids)
    if args.multi_gpu_strategy == "none" and args.world_size != 1:
        raise SystemExit(
            "--multi-gpu-strategy none requires exactly one visible GPU"
        )
    if (
        args.multi_gpu_strategy == "distributed_data_parallel"
        and args.world_size < 2
    ):
        raise SystemExit(
            "--multi-gpu-strategy distributed_data_parallel requires at least two GPUs"
        )
    try:
        batch_sizes = _parse_batch_sizes(
            args.batch_sizes,
            world_size=args.world_size,
            require_power_of_two=not args.allow_non_power_of_two,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not _is_power_of_two(args.batch_size_eval):
        raise SystemExit(f"--batch-size-eval must be a power of two, got {args.batch_size_eval}")
    if args.epochs - args.skip_epochs < args.minimum_steady_epochs:
        raise SystemExit(
            "epochs - skip_epochs must be at least minimum_steady_epochs: "
            f"{args.epochs} - {args.skip_epochs} < {args.minimum_steady_epochs}"
        )
    if not 0.0 < args.max_peak_vram_fraction <= 1.0:
        raise SystemExit("--max-peak-vram-fraction must be in (0, 1]")
    if args.min_vram_headroom_gib < 0:
        raise SystemExit("--min-vram-headroom-gib must be non-negative")

    try:
        _wait_for_idle_gpus(wait=args.wait_for_idle, poll_s=args.idle_poll_s)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    args.output_root.mkdir(parents=True, exist_ok=True)
    try:
        base = _plain_config_value(asdict(load_config(args.config)))
        if not isinstance(base, dict):
            raise ValueError(f"resolved config root must be a mapping: {args.config}")
        source_contract = _validate_source_contract(base)
        _run_environment_preflight(
            args.python,
            output_path=args.output_root / "environment_preflight.log",
        )
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    request = {
        "schema_version": 1,
        "source_config": str(args.config),
        "source_contract": source_contract,
        "output_root": str(args.output_root),
        "batch_sizes": batch_sizes,
        "world_size": args.world_size,
        "multi_gpu_strategy": args.multi_gpu_strategy,
        "batch_size_eval": args.batch_size_eval,
        "start_fold": args.start_fold,
        "epochs": args.epochs,
        "skip_epochs": args.skip_epochs,
        "minimum_steady_epochs": args.minimum_steady_epochs,
        "selection_metric": "complete_epoch_real_rows_per_s",
        "max_peak_vram_fraction": args.max_peak_vram_fraction,
        "min_vram_headroom_gib": args.min_vram_headroom_gib,
    }
    (args.output_root / "request.json").write_text(
        json.dumps(request, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    results: list[dict[str, Any]] = []
    for batch_size in batch_sizes:
        # Recheck between candidates so an unrelated job cannot silently make
        # the measurements incomparable.
        try:
            _wait_for_idle_gpus(wait=args.wait_for_idle, poll_s=args.idle_poll_s)
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        result = _run_candidate(args, base, batch_size)
        results.append(result)
        (args.output_root / "summary.json").write_text(
            json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    winner = _select_winner(results)
    if winner is None:
        raise SystemExit(f"no valid batch candidate; inspect {args.output_root / 'summary.json'}")
    winner_payload = {
        "schema_version": 1,
        "selection_metric": "complete_epoch_real_rows_per_s",
        "batch_size_train": int(winner["batch_size"]),
        "local_batch_size": int(winner["local_batch_size"]),
        "complete_epoch_real_rows_per_s": float(winner["complete_epoch_real_rows_per_s"]),
        "median_epoch_wall_s": float(winner["median_epoch_wall_s"]),
        "train_phase_real_rows_per_s": float(winner["train_phase_real_rows_per_s"]),
        "memory": winner["memory"],
        "source_result": winner,
    }
    (args.output_root / "winner.json").write_text(
        json.dumps(winner_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (args.output_root / "winner_override.yaml").write_text(
        yaml.safe_dump(
            {
                "training": {
                    "batch_size_train": int(winner["batch_size"]),
                    "auto_batch_size": False,
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    print("[batch-benchmark] WINNER " + json.dumps(winner_payload, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
