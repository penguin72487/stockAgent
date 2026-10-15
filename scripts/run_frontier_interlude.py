#!/usr/bin/env python3
"""Finish a serialized diagnostic interlude and resume its receipt-bound queue."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

bootstrap = argparse.ArgumentParser(add_help=False)
bootstrap.add_argument("--code-root", type=Path, required=True)
selection, _ = bootstrap.parse_known_args()
ROOT = selection.code_root.resolve()
sys.path.insert(0, str(ROOT))
import psutil
from downloader.artifact_io import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    ownership = parser.add_mutually_exclusive_group(required=True)
    ownership.add_argument("--hold-receipt", type=Path)
    ownership.add_argument("--hold-controller-pid", type=int)
    parser.add_argument("--after-exit", type=Path)
    parser.add_argument("--code-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-s", type=float, default=10800)
    parser.add_argument("--roofline-affinity-map")
    parser.add_argument("--diagnostic-command", type=Path, help="JSON argv; no shell interpolation")
    parser.add_argument("--diagnostic-timeout-s", type=float, default=2400)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.hold_controller_pid:
        controller = psutil.Process(args.hold_controller_pid)
        argv = controller.cmdline()
        if not any(Path(a).name == "run_frontier_fold_queue.py" for a in argv):
            raise ValueError("only the owned CPU fold dispatcher may be held")
        trials = []
        for child in controller.children(recursive=True):
            try:
                if any(Path(a).name == "run_frontier_training_trial.py" for a in child.cmdline()):
                    trials.append(child)
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                # Compile workers may exit during process inventory. Their
                # disappearance does not change ownership of the live trial.
                continue
        if len(trials) != 1:
            raise RuntimeError("hold requires exactly one active trial; no ambiguous GPU work")
        child_argv = trials[0].cmdline()
        args.after_exit = Path(child_argv[child_argv.index("--output") + 1]) / "exit.json"
        args.hold_receipt = args.output.parent / "hold.json"
        hold = {"pid": controller.pid, "create_time": controller.create_time(), "cmdline": argv,
            "active_trial": child_argv, "active_trial_exit": str(args.after_exit),
            "gpu_trainer_interrupted": False}
        atomic_write_json(args.hold_receipt, hold)
    else:
        hold = json.loads(args.hold_receipt.read_text())
    if args.after_exit is None:
        parser.error("--after-exit is required with --hold-receipt")
    process = psutil.Process(hold["pid"])
    if process.create_time() != hold["create_time"] or process.cmdline() != hold["cmdline"]:
        raise RuntimeError("queue identity differs from the hold receipt")
    receipt = {"contract": "serialized_frontier_interlude_v1", "hold": str(args.hold_receipt),
        "pid": process.pid, "after_exit": str(args.after_exit), "resumed": False}
    started = time.monotonic()
    try:
        if args.hold_controller_pid:
            process.suspend()
        while not args.after_exit.exists():
            if time.monotonic() - started > args.timeout_s:
                raise TimeoutError("diagnostic queue did not finish")
            time.sleep(10)
        # The exit receipt is written just before the child's lease is released.
        time.sleep(3)
        env = os.environ.copy()
        env.update(PYTHON_BIN=sys.executable, FINTECH_ENV_PATH=str(Path(sys.executable).parent.parent))
        if args.diagnostic_command:
            command = json.loads(args.diagnostic_command.read_text())
            if not isinstance(command, list) or not command or not all(isinstance(a, str) for a in command):
                raise ValueError("diagnostic command must be a nonempty JSON argv")
            log_path = args.output.parent / "diagnostic.log"
            receipt["diagnostic_command"] = command
            command_timeout = args.diagnostic_timeout_s
        else:
            roofline = args.output.parent / "device-roofline.json"
            command = [sys.executable, str(args.code_root / "scripts/benchmark_frontier_roofline.py"),
                "--output", str(roofline)]
            if args.roofline_affinity_map:
                command += ["--affinity-map", args.roofline_affinity_map]
                receipt["roofline_affinity_map"] = args.roofline_affinity_map
            log_path, command_timeout = args.output.parent / "device-roofline.log", 600
            receipt["roofline"] = str(roofline)
        with log_path.open("w") as log:
            measured = subprocess.run(command, cwd=args.code_root, env=env,
                stdout=log, stderr=subprocess.STDOUT, timeout=command_timeout)
        receipt["diagnostic_returncode" if args.diagnostic_command else "roofline_returncode"] = measured.returncode
    except Exception as exc:
        receipt.update(error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if process.is_running() and process.create_time() == hold["create_time"]:
            os.kill(process.pid, signal.SIGCONT)
            receipt["resumed"] = True
        receipt["elapsed_s"] = time.monotonic() - started
        atomic_write_json(args.output, receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
