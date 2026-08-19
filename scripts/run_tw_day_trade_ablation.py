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

# ``deployment_test_backtest.npz`` is the non-overlapping test ownership
# interval: this model's first valid in-split lookback row through the row just
# before the next model's first valid in-split lookback row. It is therefore
# the primary test surface. The expanding full-future artifact remains a
# diagnostic only and must never be presented as the canonical test period.
POSTPROCESS_PLOT_SPECS = (
    ("val", "val", "Validation tensor loss contract"),
    (
        "deployment",
        "test",
        "Owned test: current-year lookback complete through next-year pre-lookback",
    ),
    (
        "test",
        "full_horizon_integer_audit",
        "Diagnostic only: expanding full-future exact whole-lot horizon",
    ),
)


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
    active_rows: list[tuple[float, str, int, int]] = []
    for name in names:
        newest_for_name: tuple[float, str, int, int] | None = None
        for curve in (root / name).glob("train_*/epoch_curve.jsonl"):
            try:
                modified = curve.stat().st_mtime
            except OSError:
                continue
            row = _last_json(curve)
            fold = len(curve.parent.name.removeprefix("train_").split("-"))
            if (root / name / f"fold_{fold:02d}" / "fold_complete.json").is_file():
                continue
            candidate = (modified, name, fold, int(row.get("epoch", 0)))
            if newest_for_name is None or modified > newest_for_name[0]:
                newest_for_name = candidate
        if newest_for_name is not None:
            active_rows.append(newest_for_name)
    total = len(names) * folds
    fractional = sum(
        min(1.0, epoch / max(1, epochs))
        for _, _, _, epoch in active_rows
    )
    percent = 100.0 * min(total, complete + fractional) / max(1, total)
    active_rows.sort(reverse=True)
    # This supervisor deliberately runs one experiment at a time. Historical
    # incomplete epoch curves can remain for several variants after a resume;
    # showing the two newest made a sequential DDP run look concurrent. The
    # newest writer is the sole current-job hint, while every durable partial
    # epoch still contributes to the aggregate completion percentage.
    descriptions = [
        f"{name} fold {fold}/{folds} epoch {epoch}/{epochs}"
        for _, name, fold, epoch in active_rows[:1]
    ]
    label = " + ".join(descriptions) if descriptions else "preflight"
    return percent, f"folds {complete}/{total} | {label}"


def _single_experiment_concurrency(requested: int) -> int:
    """Make the TW day-trade ablation wrapper sequential by contract."""

    if int(requested) <= 0:
        raise ValueError("parallel_jobs must be positive")
    return 1


def _bar(percent: float, label: str, *, width: int = 36) -> str:
    filled = min(width, max(0, int(round(width * percent / 100.0))))
    return f"[ablation] |{'█' * filled}{'░' * (width - filled)}| {percent:6.2f}% | {label}"


def _descendant_process_ids(root_pid: int) -> list[int]:
    try:
        result = subprocess.run(
            ["ps", "-e", "-o", "pid=,ppid="],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    children: dict[int, list[int]] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            pid, parent = (int(field) for field in fields)
        except ValueError:
            continue
        children.setdefault(parent, []).append(pid)
    descendants: list[int] = []
    frontier = list(children.get(int(root_pid), ()))
    while frontier:
        pid = frontier.pop()
        descendants.append(pid)
        frontier.extend(children.get(pid, ()))
    return descendants


def _pid_is_live(pid: int) -> bool:
    try:
        state = Path(f"/proc/{int(pid)}/stat").read_text(encoding="utf-8").split()[2]
    except (OSError, IndexError):
        return False
    return state != "Z"


def _terminate_group(process: subprocess.Popen, grace_s: float = 10.0) -> None:
    known = {int(process.pid), *_descendant_process_ids(int(process.pid))}
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        known.update(_descendant_process_ids(int(process.pid)))
        for pid in sorted(known, reverse=True):
            if not _pid_is_live(pid):
                continue
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + (grace_s if sig != signal.SIGKILL else 2.0)
        while time.monotonic() < deadline:
            if not any(_pid_is_live(pid) for pid in known):
                process.poll()
                return
            time.sleep(0.1)
    process.poll()


def _run_checked(command: list[str], *, log=None) -> None:
    result = subprocess.run(command, cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT if log else None)
    if result.returncode:
        raise SystemExit(result.returncode)


def _load_effective_spec(spec_path: Path) -> tuple[dict, list[dict]]:
    """Load the same recursively inherited spec used by the worker runner."""

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from scripts.run_ablation_experiments import _experiment_rows

    return _experiment_rows(spec_path.resolve())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--multi-gpu-strategy", default="distributed_data_parallel")
    parser.add_argument(
        "--parallel-jobs",
        type=int,
        default=1,
        help=(
            "Compatibility input; this TW day-trade supervisor always runs "
            "exactly one independent experiment using all visible GPUs."
        ),
    )
    parser.add_argument("--max-no-progress-retries", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=5.0)
    parser.add_argument("--cuda-health-poll-seconds", type=float, default=30.0)
    args = parser.parse_args()
    if args.cuda_health_poll_seconds <= 0:
        raise SystemExit("--cuda-health-poll-seconds must be positive")
    parallel_jobs = _single_experiment_concurrency(args.parallel_jobs)
    if int(args.parallel_jobs) != parallel_jobs:
        print(
            "[ablation] forcing independent experiment concurrency to 1 so "
            "the current run owns all visible GPUs and host thread budgets",
            flush=True,
        )

    spec_path = args.spec.resolve()
    spec, experiment_rows = _load_effective_spec(spec_path)
    folds = int(spec["expected_fold_count"])
    root = (args.output_root or (REPO_ROOT / spec["output_root"])).resolve()
    root.mkdir(parents=True, exist_ok=True)
    baseline_root_raw = spec.get("baseline_artifact_root")
    baseline_root = (
        (REPO_ROOT / str(baseline_root_raw)).resolve()
        if baseline_root_raw
        else None
    )
    if baseline_root is not None and not (baseline_root / "summary.json").is_file():
        raise SystemExit(
            "baseline_artifact_root does not contain summary.json: "
            f"{baseline_root}"
        )

    # Generate and validate effective configs without training.
    dry_run = [sys.executable, str(REPO_ROOT / "scripts/run_ablation_experiments.py"),
               "--spec", str(spec_path), "--output-root", str(root), "--dry-run",
               "--multi-gpu-strategy", args.multi_gpu_strategy,
               "--parallel-jobs", str(parallel_jobs)]
    _run_checked(dry_run)
    # Derive the run set from the current spec, not from every historical file
    # left under generated_configs. This keeps removed/renamed experiments out
    # of the progress denominator without deleting their audit artifacts.
    from scripts.run_ablation_experiments import _cuda_runtime_health

    names = [str(row["name"]) for row in experiment_rows]
    configs = [root / "generated_configs" / f"{name}.yaml" for name in names]
    missing_configs = [str(path) for path in configs if not path.is_file()]
    if missing_configs:
        raise SystemExit(
            "dry-run did not generate every configured ablation: "
            + ", ".join(missing_configs)
        )
    if not names:
        raise SystemExit("no generated ablation configs")
    epochs = max(int(yaml.safe_load(path.read_text())["training"]["epochs"]) for path in configs)

    # NVML/nvidia-smi can remain readable while CUDA Driver API or UVM is
    # broken. Wait for a real allocation before running the strict compiler and
    # environment preflight. This lets a long ablation supervisor survive host
    # driver recovery without consuming any experiment retry budget.
    while True:
        cuda_healthy, cuda_health_detail = _cuda_runtime_health()
        if cuda_healthy:
            print(f"[ablation] CUDA compute path healthy: {cuda_health_detail}")
            break
        print(
            "[ablation] CUDA compute path unavailable; waiting without "
            "starting or charging an experiment retry. "
            f"next_probe={args.cuda_health_poll_seconds:.1f}s "
            f"detail={cuda_health_detail}",
            flush=True,
        )
        time.sleep(args.cuda_health_poll_seconds)

    # Fail before expensive work if non-CUDA toolchain contracts are not satisfied.
    _run_checked([sys.executable, str(REPO_ROOT / "scripts/check_environment.py"), "--require-cuda", "--strict"])

    command = [sys.executable, str(REPO_ROOT / "scripts/run_ablation_experiments.py"),
               "--spec", str(spec_path), "--output-root", str(root),
               "--multi-gpu-strategy", args.multi_gpu_strategy,
               "--parallel-jobs", str(parallel_jobs), "--auto-resume",
               "--max-no-progress-retries", str(args.max_no_progress_retries),
               "--retry-backoff-seconds", str(args.retry_backoff_seconds),
               "--cuda-health-poll-seconds", str(args.cuda_health_poll_seconds),
               "--stop-on-fail"]
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
            print(
                "\n[ablation] interrupt received; stopping the entire DDP "
                "descendant tree...",
                flush=True,
            )
            _terminate_group(process)
            raise SystemExit(130)
        returncode = process.wait()
    print()
    if returncode:
        raise SystemExit(f"ablation failed with exit code {returncode}; inspect {log_path}")

    # Validation is the selection surface. The primary test calculation uses
    # the non-overlapping handoff interval. The expanding full-future exact
    # whole-lot result is retained under an explicit diagnostic-only prefix.
    plotter = str(REPO_ROOT / "scripts/plot_ablation_analysis.py")
    for split, prefix, scope_label in POSTPROCESS_PLOT_SPECS:
        command = [
            sys.executable,
            plotter,
            "--root",
            str(root),
            "--output-dir",
            str(root),
            "--split",
            split,
            "--prefix",
            prefix,
            "--scope-label",
            scope_label,
        ]
        if baseline_root is not None:
            command.extend(["--baseline-root", str(baseline_root)])
        _run_checked(command)
    print(_bar(100.0, f"complete | charts and CSVs: {root}"), flush=True)


if __name__ == "__main__":
    main()
