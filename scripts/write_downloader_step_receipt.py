#!/usr/bin/env python3
"""Publish one sanitized, durable scheduler step receipt.

The daily shell scheduler intentionally does not persist command arguments:
provider URLs and CLI arguments can contain credentials.  A receipt records
only lifecycle, timing, exit status, and stable step identity.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json


SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_name(value: str) -> str:
    cleaned = SAFE_NAME.sub("_", str(value).strip()).strip("._")
    if not cleaned:
        raise ValueError("step/run identity cannot be empty")
    return cleaned[:180]


def write_receipt(
    *,
    receipt_dir: Path,
    latest_dir: Path,
    run_id: str,
    run_mode: str,
    step: str,
    state: str,
    started_epoch: float,
    exit_code: int | None,
    elapsed_seconds: float | None,
    runner_pid: int,
) -> dict[str, object]:
    observed = datetime.now(timezone.utc)
    started = datetime.fromtimestamp(float(started_epoch), tz=timezone.utc)
    step_name = safe_name(step)
    payload: dict[str, object] = {
        "schema_version": 1,
        "run_id": safe_name(run_id),
        "run_mode": str(run_mode),
        "step": str(step),
        "state": str(state),
        "started_at_utc": started.isoformat(),
        "updated_at_utc": observed.isoformat(),
        "ended_at_utc": observed.isoformat() if state != "running" else None,
        "elapsed_seconds": (
            max(0.0, float(elapsed_seconds))
            if elapsed_seconds is not None
            else max(0.0, observed.timestamp() - float(started_epoch))
        ),
        "exit_code": int(exit_code) if exit_code is not None else None,
        "runner_pid": int(runner_pid),
        "command_recorded": False,
        "command_omission_reason": "credentials_may_be_present",
    }
    atomic_write_json(receipt_dir / f"{step_name}.json", payload, durable=True)
    atomic_write_json(latest_dir / f"{step_name}.json", payload, durable=True)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-dir", type=Path, required=True)
    parser.add_argument("--latest-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-mode", required=True)
    parser.add_argument("--step", required=True)
    parser.add_argument(
        "--state", choices=("running", "complete", "failed"), required=True
    )
    parser.add_argument("--started-epoch", type=float, required=True)
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--elapsed-seconds", type=float)
    parser.add_argument("--runner-pid", type=int, required=True)
    args = parser.parse_args()
    write_receipt(
        receipt_dir=args.receipt_dir,
        latest_dir=args.latest_dir,
        run_id=args.run_id,
        run_mode=args.run_mode,
        step=args.step,
        state=args.state,
        started_epoch=args.started_epoch,
        exit_code=args.exit_code,
        elapsed_seconds=args.elapsed_seconds,
        runner_pid=args.runner_pid,
    )


if __name__ == "__main__":
    main()
