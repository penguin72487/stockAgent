#!/usr/bin/env python3
"""Start, inspect, and stop market training jobs assigned to physical GPUs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "configs" / "gpu_jobs.yaml"


def _load_spec(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict) or not isinstance(raw.get("jobs"), list):
        raise ValueError("GPU job config must contain a 'jobs' list")
    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError("'defaults' must be a mapping")

    jobs: list[dict[str, Any]] = []
    names: set[str] = set()
    assigned: dict[int, str] = {}
    output_owners: dict[str, str] = {}
    allow_gpu_sharing = bool(raw.get("allow_gpu_sharing", False))
    for index, source in enumerate(raw["jobs"]):
        if not isinstance(source, dict):
            raise ValueError(f"jobs[{index}] must be a mapping")
        job = {**defaults, **source}
        name = str(job.get("name", "")).strip()
        config = str(job.get("config", "")).strip()
        gpus = job.get("gpus")
        if not name or not config or not isinstance(gpus, list) or not gpus:
            raise ValueError(f"jobs[{index}] requires name, config, and non-empty gpus")
        if name in names:
            raise ValueError(f"duplicate job name: {name}")
        names.add(name)
        job["name"] = name
        job["config"] = config
        job["gpus"] = [int(gpu) for gpu in gpus]
        if len(set(job["gpus"])) != len(job["gpus"]) or min(job["gpus"]) < 0:
            raise ValueError(f"job {name!r} has invalid or duplicate GPU indices")
        if bool(job.get("enabled", True)) and not allow_gpu_sharing:
            for gpu in job["gpus"]:
                if gpu in assigned:
                    raise ValueError(
                        f"GPU {gpu} is assigned to both {assigned[gpu]!r} and {name!r}; "
                        "set allow_gpu_sharing: true only if this is intentional"
                    )
                assigned[gpu] = name
        output_dir = job.get("output_dir")
        if output_dir is not None:
            output_dir = str(output_dir).strip()
            if not output_dir:
                raise ValueError(f"job {name!r} output_dir must not be empty")
            job["output_dir"] = output_dir
            if bool(job.get("enabled", True)) and output_dir in output_owners:
                raise ValueError(
                    f"output_dir {output_dir!r} is shared by {output_owners[output_dir]!r} "
                    f"and {name!r}; parallel jobs require distinct output directories"
                )
            output_owners[output_dir] = name
        fold_range = job.get("fold_range")
        if fold_range is not None:
            if not isinstance(fold_range, list) or len(fold_range) != 2:
                raise ValueError(f"job {name!r} fold_range must be [start, end]")
            start, end = (int(value) for value in fold_range)
            if start < 1 or end < start:
                raise ValueError(f"job {name!r} has invalid fold_range: {fold_range}")
            job["fold_range"] = [start, end]
        jobs.append(job)
    return raw, jobs


def _state_dir(spec_path: Path, raw: dict[str, Any]) -> Path:
    configured = raw.get("state_dir", "artifacts/gpu_jobs")
    path = Path(str(configured)).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def _state_path(state_dir: Path, name: str) -> Path:
    return state_dir / f"{name}.json"


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _selected(jobs: list[dict[str, Any]], names: list[str]) -> list[dict[str, Any]]:
    wanted = set(names)
    if not wanted:
        return jobs
    known = {job["name"] for job in jobs}
    missing = sorted(wanted - known)
    if missing:
        raise ValueError(f"unknown job(s): {', '.join(missing)}")
    return [job for job in jobs if job["name"] in wanted]


def _gpu_count() -> int:
    result = subprocess.run(
        ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader,nounits"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return len([line for line in result.stdout.splitlines() if line.strip()])


def _validate_files_and_gpus(jobs: list[dict[str, Any]]) -> None:
    count = _gpu_count()
    for job in jobs:
        config_path = REPO_ROOT / job["config"]
        if not config_path.is_file():
            raise ValueError(f"job {job['name']!r} config does not exist: {config_path}")
        invalid = [gpu for gpu in job["gpus"] if gpu >= count]
        if invalid:
            raise ValueError(f"job {job['name']!r} references unavailable GPU(s): {invalid}")


def _start(spec_path: Path, raw: dict[str, Any], jobs: list[dict[str, Any]]) -> int:
    state_dir = _state_dir(spec_path, raw)
    state_dir.mkdir(parents=True, exist_ok=True)
    _validate_files_and_gpus([job for job in jobs if bool(job.get("enabled", True))])
    failures = 0
    for job in jobs:
        name = job["name"]
        if not bool(job.get("enabled", True)):
            print(f"{name}: disabled")
            continue
        state_path = _state_path(state_dir, name)
        old = _read_state(state_path)
        if old and _alive(int(old.get("pid", -1))):
            print(f"{name}: already running (pid={old['pid']})")
            continue

        stamp = time.strftime("%Y%m%d_%H%M%S")
        log_path = state_dir / f"{name}_{stamp}.log"
        command = [str(REPO_ROOT / "coda_runner.sh"), "-c", job["config"]]
        extra_args = job.get("extra_args") or []
        if not isinstance(extra_args, list) or not all(isinstance(arg, str) for arg in extra_args):
            raise ValueError(f"job {name!r} extra_args must be a list of strings")
        train_args: list[str] = []
        if job.get("output_dir"):
            train_args.extend(["--output-dir", job["output_dir"]])
        if job.get("fold_range"):
            start, end = job["fold_range"]
            train_args.extend(["--start-fold", str(start), "--max-folds", str(end - start + 1)])
        train_args.extend(extra_args)
        if train_args:
            command.extend(["--", *train_args])
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, job["gpus"]))
        env.update({str(k): str(v) for k, v in (job.get("env") or {}).items()})
        with log_path.open("ab", buffering=0) as log:
            process = subprocess.Popen(
                command,
                cwd=REPO_ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        time.sleep(0.5)
        if process.poll() is not None:
            print(f"{name}: failed to start (exit={process.returncode}, log={log_path})")
            failures += 1
            continue
        state = {
            "name": name,
            "pid": process.pid,
            "gpus": job["gpus"],
            "config": job["config"],
            "fold_range": job.get("fold_range"),
            "output_dir": job.get("output_dir"),
            "log": str(log_path),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        print(f"{name}: started pid={process.pid} gpus={job['gpus']} log={log_path}")
    return 1 if failures else 0


def _status(spec_path: Path, raw: dict[str, Any], jobs: list[dict[str, Any]]) -> int:
    state_dir = _state_dir(spec_path, raw)
    for job in jobs:
        state = _read_state(_state_path(state_dir, job["name"]))
        if not state:
            print(f"{job['name']}: stopped (no state) gpus={job['gpus']} config={job['config']}")
            continue
        running = _alive(int(state.get("pid", -1)))
        label = "running" if running else "exited"
        print(
            f"{job['name']}: {label} pid={state.get('pid')} gpus={state.get('gpus')} "
            f"folds={state.get('fold_range')} config={state.get('config')} log={state.get('log')}"
        )
    return 0


def _stop(spec_path: Path, raw: dict[str, Any], jobs: list[dict[str, Any]], timeout: float) -> int:
    state_dir = _state_dir(spec_path, raw)
    failures = 0
    for job in jobs:
        state_path = _state_path(state_dir, job["name"])
        state = _read_state(state_path)
        pid = int(state.get("pid", -1)) if state else -1
        if pid <= 0 or not _alive(pid):
            print(f"{job['name']}: already stopped")
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
            deadline = time.monotonic() + timeout
            while _alive(pid) and time.monotonic() < deadline:
                time.sleep(0.2)
            if _alive(pid):
                os.killpg(pid, signal.SIGKILL)
            print(f"{job['name']}: stopped pid={pid}")
        except (ProcessLookupError, PermissionError) as exc:
            print(f"{job['name']}: stop failed: {exc}", file=sys.stderr)
            failures += 1
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "start", "status", "stop", "restart"))
    parser.add_argument("jobs", nargs="*", help="job names; omit to select all jobs")
    parser.add_argument("--config", type=Path, default=DEFAULT_SPEC, help="GPU job YAML")
    parser.add_argument("--stop-timeout", type=float, default=30.0)
    args = parser.parse_args()
    spec_path = args.config.expanduser().resolve()
    try:
        raw, all_jobs = _load_spec(spec_path)
        jobs = _selected(all_jobs, args.jobs)
        if args.command == "validate":
            _validate_files_and_gpus([job for job in jobs if bool(job.get("enabled", True))])
            print(f"valid: {spec_path} ({len(jobs)} job(s))")
            return 0
        if args.command == "status":
            return _status(spec_path, raw, jobs)
        if args.command == "stop":
            return _stop(spec_path, raw, jobs, args.stop_timeout)
        if args.command == "restart":
            result = _stop(spec_path, raw, jobs, args.stop_timeout)
            return result or _start(spec_path, raw, jobs)
        return _start(spec_path, raw, jobs)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
