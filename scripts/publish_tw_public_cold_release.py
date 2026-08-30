#!/usr/bin/env python3
"""Publish TW public live data to cold storage outside the opening path.

This job creates an immutable packed release for Syncthing backup only.  It
never calls ``stockagent-data use`` and never changes the runtime data link.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from typing import Any, Mapping
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
TAIPEI = ZoneInfo("Asia/Taipei")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/cold_publish/latest.json"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=7200.0)
    return parser.parse_args()


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _parse_output(value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def main() -> int:
    args = parse_args()
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    started = datetime.now(TAIPEI)
    command = [
        str(REPO_ROOT / "scripts" / "run_data_cache.sh"),
        "publish",
        "tw-public",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=args.timeout_seconds,
        )
        return_code = int(completed.returncode)
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        release = _parse_output(stdout)
        error = None if return_code == 0 else (stderr or stdout)[-4000:]
    except subprocess.TimeoutExpired as exc:
        return_code = 124
        release = None
        error = f"cold publish timed out after {args.timeout_seconds:.0f}s: {exc}"
    completed_at = datetime.now(TAIPEI)
    payload = {
        "schema_version": 1,
        "status": "ok" if return_code == 0 and release is not None else "failed",
        "started_at_taipei": started.isoformat(),
        "completed_at_taipei": completed_at.isoformat(),
        "elapsed_seconds": (completed_at - started).total_seconds(),
        "dataset": "tw-public",
        "source_authority": "catalog_mutable_live_root",
        "opening_dependency": False,
        "runtime_link_changed": False,
        "materialization_performed": False,
        "return_code": return_code,
        "release": release,
        "error": error,
    }
    receipt = _repo_path(args.receipt)
    _atomic_json(receipt, payload)
    run_path = receipt.parent / "runs" / (
        started.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
