#!/usr/bin/env python3
"""Validated, resumable TW day-trade ablation supervisor with live progress."""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "configs/ablations/financial_transformer_tw_day_trade.yaml"


def _last_json(path: Path) -> dict:
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 131_072))
            lines = fh.read().splitlines()
            line = lines[-1].strip() if lines else b""
        return json.loads(line) if line else {}
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _progress(root: Path, names: list[str], folds: int, epochs: int) -> tuple[float, str]:
    complete = sum(
        (root / name / f"fold_{fold:02d}" / "fold_complete.json").is_file()
        for name in names
        for fold in range(1, folds + 1)
    )
    active_name, active_fold, active_epoch = "preflight", 0, 0
    newest = 0.0
    for name in names:
        for curve in (root / name).glob("train_*/epoch_curve.jsonl"):
            try:
                modified = curve.stat().st_mtime
            except OSError:
                continue
            if modified <= newest:
                continue
            row = _last_json(curve)
            newest = modified
            active_name = name
            active_fold = len(curve.parent.name.removeprefix("train_").split("-"))
            active_epoch = int(row.get("epoch", 0))
    total = len(names) * folds
    fractional = min(1.0, active_epoch / max(1, epochs)) if active_fold else 0.0
    percent = 100.0 * min(total, complete + fractional) / max(1, total)
    label = f"{active_name} fold {active_fold}/{folds} epoch {active_epoch}/{epochs}" if active_fold else active_name
    return percent, f"folds {complete}/{total} | {label}"


def _bar(percent: float, label: str, *, width: int = 36) -> str:
    filled = min(width, max(0, int(round(width * percent / 100.0))))
    return f"[ablation] |{'█' * filled}{'░' * (width - filled)}| {percent:6.2f}% | {label}"


def _terminate_group(process: subprocess.Popen, grace_s: float = 10.0) -> None:
    if process.poll() is not None:
        return
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=grace_s if sig != signal.SIGKILL else 2.0)
            return
        except subprocess.TimeoutExpired:
            continue


def _run_checked(command: list[str], *, log=None) -> None:
    result = subprocess.run(command, cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT if log else None)
    if result.returncode:
        raise SystemExit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--multi-gpu-strategy", default="distributed_data_parallel")
    args = parser.parse_args()

    spec_path = args.spec.resolve()
    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    folds = int(spec["expected_fold_count"])
    root = (args.output_root or (REPO_ROOT / spec["output_root"])).resolve()
    root.mkdir(parents=True, exist_ok=True)

    # Generate and validate effective configs without training.
    dry_run = [sys.executable, str(REPO_ROOT / "scripts/run_ablation_experiments.py"),
               "--spec", str(spec_path), "--output-root", str(root), "--dry-run",
               "--multi-gpu-strategy", args.multi_gpu_strategy]
    _run_checked(dry_run)
    configs = sorted((root / "generated_configs").glob("*.yaml"))
    names = [path.stem for path in configs]
    if not names:
        raise SystemExit("no generated ablation configs")
    epochs = max(int(yaml.safe_load(path.read_text())["training"]["epochs"]) for path in configs)

    # Fail before expensive work if CUDA/toolchain contracts are not satisfied.
    _run_checked([sys.executable, str(REPO_ROOT / "scripts/check_environment.py"), "--require-cuda", "--strict"])

    command = [sys.executable, str(REPO_ROOT / "scripts/run_ablation_experiments.py"),
               "--spec", str(spec_path), "--output-root", str(root),
               "--multi-gpu-strategy", args.multi_gpu_strategy, "--stop-on-fail"]
    log_path = root / "ablation_run.log"
    started = time.monotonic()
    with log_path.open("a", encoding="utf-8", buffering=1) as log:
        process = subprocess.Popen(command, cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True, text=True)
        try:
            while process.poll() is None:
                percent, label = _progress(root, names, folds, epochs)
                elapsed = time.monotonic() - started
                eta = elapsed * (100.0 - percent) / percent if percent > 0 else math.inf
                eta_text = f"ETA {eta / 3600:.1f}h" if math.isfinite(eta) else "ETA warming up"
                print("\r" + _bar(percent, f"{label} | {eta_text}"), end="", flush=True)
                time.sleep(max(.5, args.poll_seconds))
        except KeyboardInterrupt:
            print("\n[ablation] interrupt received; stopping the entire DDP process group...", flush=True)
            _terminate_group(process)
            raise SystemExit(130)
        returncode = process.wait()
    print()
    if returncode:
        raise SystemExit(f"ablation failed with exit code {returncode}; inspect {log_path}")

    # Validation is the selection surface; test charts remain descriptive and separate.
    plotter = str(REPO_ROOT / "scripts/plot_ablation_analysis.py")
    for split in ("val", "test"):
        _run_checked([sys.executable, plotter, "--root", str(root), "--output-dir", str(root),
                      "--split", split, "--prefix", split])
    print(_bar(100.0, f"complete | charts and CSVs: {root}"), flush=True)


if __name__ == "__main__":
    main()
