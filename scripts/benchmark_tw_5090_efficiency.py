#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PYTHON = Path(sys.executable).resolve()


@dataclass(frozen=True)
class Variant:
    name: str
    cuda_visible_devices: str
    args: tuple[str, ...]
    notes: str


VARIANTS: dict[str, Variant] = {
    "dual_ddp_b128_cached": Variant(
        name="dual_ddp_b128_cached",
        cuda_visible_devices="0,1",
        args=(
            "--multi-gpu-strategy",
            "distributed_data_parallel",
            "--batch-size-train",
            "128",
            "--batch-size-eval",
            "128",
            "--cache-eval-tensors-on-gpu",
        ),
        notes="Dual-RTX-5090 DDP, global batch 128, shared panel cached on GPU.",
    ),
    "dual_ddp_b256_cached": Variant(
        name="dual_ddp_b256_cached",
        cuda_visible_devices="0,1",
        args=(
            "--multi-gpu-strategy",
            "distributed_data_parallel",
            "--batch-size-train",
            "256",
            "--batch-size-eval",
            "128",
            "--cache-eval-tensors-on-gpu",
        ),
        notes="Dual-RTX-5090 DDP, global batch 256, shared panel cached on GPU.",
    ),
    "dual_ddp_b128": Variant(
        name="dual_ddp_b128",
        cuda_visible_devices="0,1",
        args=(
            "--multi-gpu-strategy",
            "distributed_data_parallel",
            "--batch-size-train",
            "128",
            "--batch-size-eval",
            "128",
            "--no-cache-eval-tensors-on-gpu",
        ),
        notes="Canonical dual-RTX-5090 DDP fixed-shape panel-slab production path.",
    ),
    "single_b64": Variant(
        name="single_b64",
        cuda_visible_devices="0",
        args=(
            "--multi-gpu-strategy",
            "none",
            "--batch-size-train",
            "64",
            "--batch-size-eval",
            "64",
            "--no-cache-eval-tensors-on-gpu",
        ),
        notes="Fair single-GPU baseline; per-step GPU batch matches one DDP shard.",
    ),
}


def _parse_variants(raw: str) -> list[Variant]:
    selected: list[Variant] = []
    for name in raw.split(","):
        key = name.strip()
        if not key:
            continue
        if key not in VARIANTS:
            raise SystemExit(f"unknown variant {key!r}; choices: {', '.join(sorted(VARIANTS))}")
        selected.append(VARIANTS[key])
    if not selected:
        raise SystemExit("at least one variant is required")
    return selected


def _latest_epoch_curve(output_dir: Path) -> Path | None:
    curves = list(output_dir.rglob("epoch_curve.jsonl"))
    if not curves:
        return None
    return max(curves, key=lambda path: path.stat().st_mtime)


def _read_epoch_rows(path: Path | None) -> list[dict[str, object]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _mean(values: Iterable[float]) -> float | None:
    values = [float(value) for value in values]
    if not values:
        return None
    return sum(values) / len(values)


def _steady_summary(rows: list[dict[str, object]], *, skip_epochs: int) -> dict[str, object]:
    steady = [row for row in rows if int(row.get("epoch", 0) or 0) > skip_epochs]
    if not steady:
        steady = rows[-1:]
    keys = (
        "epoch_wall_s",
        "train_total_s",
        "train_total_ms_per_batch",
        "train_model_forward_ms_per_batch",
        "train_model_forward_cuda_ms_per_batch",
        "train_loss_ms_per_batch",
        "train_loss_cuda_ms_per_batch",
        "train_backward_total_ms_per_batch",
        "train_backward_autograd_cuda_ms_per_batch",
        "val_eval_s",
        "test_curve_s",
    )
    out: dict[str, object] = {
        "epochs": [int(row.get("epoch", 0) or 0) for row in steady],
        "train_batches": steady[-1].get("train_batches") if steady else None,
    }
    for key in keys:
        vals = [float(row[key]) for row in steady if isinstance(row.get(key), (int, float))]
        out[f"{key}_mean"] = _mean(vals)
    return out


def _sample_gpus(stop: threading.Event, path: Path, interval_s: float) -> None:
    fields = [
        "timestamp",
        "index",
        "memory.used",
        "memory.total",
        "utilization.gpu",
        "utilization.memory",
        "power.draw",
    ]
    query = ",".join(fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        while not stop.is_set():
            try:
                result = subprocess.run(
                    [
                        "nvidia-smi",
                        f"--query-gpu={query}",
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
            except Exception:
                pass
            stop.wait(interval_s)


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=20)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _run_variant(args: argparse.Namespace, variant: Variant) -> dict[str, object]:
    python_bin = Path(args.python)
    output_dir = Path(args.output_base) / variant.name
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "train.log"
    gpu_samples_path = output_dir / "gpu_samples.csv"
    env = os.environ.copy()
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "CUDA_VISIBLE_DEVICES": variant.cuda_visible_devices,
            "STOCKAGENT_CPU_THREADS": str(args.cpu_threads),
            "STOCKAGENT_TORCH_COMPILE_THREADS": str(args.compile_threads),
            "STOCKAGENT_POLARS_THREADS": str(args.polars_threads),
            "POLARS_MAX_THREADS": str(args.polars_threads),
            "RAYON_NUM_THREADS": str(args.polars_threads),
            "STOCKAGENT_BACKTEST_COMPILE_PREP": "1",
            "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True",
            "PYTORCH_ALLOC_CONF": "expandable_segments:True",
            "TORCHINDUCTOR_CACHE_DIR": str(Path(args.torchinductor_cache_dir).expanduser()),
            "TRITON_CACHE_DIR": str(Path(args.triton_cache_dir).expanduser()),
            "CUDA_CACHE_PATH": str(Path(args.cuda_cache_path).expanduser()),
        }
    )
    cmd = [
        str(python_bin),
        "train.py",
        "--config",
        args.config,
        "--output-dir",
        str(output_dir),
        "--start-fold",
        str(args.start_fold),
        "--max-folds",
        str(args.max_folds),
        "--epochs",
        str(args.epochs),
        "--no-resume",
        *variant.args,
    ]
    print(f"[benchmark] start {variant.name}: {' '.join(cmd)}")
    stop = threading.Event()
    sampler = threading.Thread(
        target=_sample_gpus,
        args=(stop, gpu_samples_path, float(args.gpu_sample_interval_s)),
        daemon=True,
    )
    sampler.start()
    start = time.perf_counter()
    status = "ok"
    return_code: int | None = None
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            cmd,
            cwd=ROOT,
            env=env,
            text=True,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        try:
            return_code = process.wait(timeout=float(args.timeout_s))
        except subprocess.TimeoutExpired:
            status = "timeout"
            _terminate_process(process)
            return_code = process.poll()
    stop.set()
    sampler.join(timeout=5)
    elapsed_s = time.perf_counter() - start
    if return_code not in (0, None) and status == "ok":
        status = "failed"
    curve_path = _latest_epoch_curve(output_dir)
    rows = _read_epoch_rows(curve_path)
    summary = {
        "name": variant.name,
        "status": status,
        "return_code": return_code,
        "elapsed_s": elapsed_s,
        "notes": variant.notes,
        "command": cmd,
        "output_dir": str(output_dir),
        "log_path": str(log_path),
        "gpu_samples_path": str(gpu_samples_path),
        "epoch_curve": str(curve_path) if curve_path else None,
        "epoch_count": len(rows),
        "steady": _steady_summary(rows, skip_epochs=int(args.skip_epochs)),
    }
    print(
        "[benchmark] done "
        f"{variant.name}: status={status} return_code={return_code} "
        f"elapsed={elapsed_s:.1f}s epochs={len(rows)}"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark TW RTX 5090 training efficiency variants.")
    parser.add_argument("--config", default="configs/markets/tw_parallel.yaml")
    parser.add_argument("--output-base", default="artifacts/benchmarks/tw_5090_efficiency")
    parser.add_argument("--variants", default="dual_ddp_b128,single_b64")
    parser.add_argument("--python", default=str(DEFAULT_PYTHON))
    parser.add_argument("--start-fold", type=int, default=23)
    parser.add_argument("--max-folds", type=int, default=1)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--skip-epochs", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=900.0)
    parser.add_argument("--gpu-sample-interval-s", type=float, default=2.0)
    parser.add_argument("--cpu-threads", type=int, default=128)
    parser.add_argument("--compile-threads", type=int, default=32)
    parser.add_argument("--polars-threads", type=int, default=1)
    parser.add_argument("--torchinductor-cache-dir", default="~/.cache/torchinductor")
    parser.add_argument("--triton-cache-dir", default="~/.cache/triton")
    parser.add_argument("--cuda-cache-path", default="~/.cache/nv_cuda")
    args = parser.parse_args()

    summaries = [_run_variant(args, variant) for variant in _parse_variants(args.variants)]
    output_base = Path(args.output_base)
    output_base.mkdir(parents=True, exist_ok=True)
    summary_path = output_base / "summary.json"
    summary_path.write_text(json.dumps(summaries, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[benchmark] summary={summary_path}")
    print(json.dumps(summaries, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
