#!/usr/bin/env python3
"""Publish one sanitized, durable scheduler step receipt.

The daily shell scheduler intentionally does not persist command arguments:
provider URLs and CLI arguments can contain credentials.  A receipt records
only lifecycle, timing, exit status, and stable step identity.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.status import count_reported_source_gaps


SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")
YAHOO_SUMMARY_STEP = re.compile(
    r"yahoo_(?P<asset>[a-z0-9_]+)_(?P<kind>daily_update|history_head_repair|1m_update)"
)
YAHOO_SUMMARY_MODES = {
    "daily_update": "daily-update",
    "history_head_repair": "repair",
    "1m_update": "incremental",
}


def safe_name(value: str) -> str:
    cleaned = SAFE_NAME.sub("_", str(value).strip()).strip("._")
    if not cleaned:
        raise ValueError("step/run identity cannot be empty")
    return cleaned[:180]


def _matched_source_summary(
    path: Path, *, run_id: str, step: str, started_epoch: float,
    observed: datetime,
) -> dict[str, object] | None:
    """Take only bounded counts from a Yahoo summary for this exact run."""

    match = YAHOO_SUMMARY_STEP.fullmatch(step)
    if match is None or path.is_symlink():
        return None
    try:
        if path.stat().st_size > 16_384:
            return None
        source = json.loads(path.read_text(encoding="utf-8"))
        generated = datetime.fromisoformat(str(source["generated_at_utc"]))
    except (OSError, UnicodeError, ValueError, KeyError, TypeError):
        return None
    if (
        not isinstance(source, dict)
        or source.get("schema_version") != 1
        or source.get("run_id") != run_id
        or source.get("asset_class") != match.group("asset")
        or source.get("mode") != YAHOO_SUMMARY_MODES[match.group("kind")]
        or generated.tzinfo is None
        or not started_epoch - 1 <= generated.timestamp() <= observed.timestamp() + 1
    ):
        return None
    counts = source.get("status_counts")
    if not isinstance(counts, dict) or len(counts) > 64 or not all(
        isinstance(key, str)
        and re.fullmatch(r"[a-z0-9_]{1,64}", key)
        and type(value) is int
        and 0 <= value <= 1_000_000_000
        for key, value in counts.items()
    ):
        return None
    elapsed = source.get("elapsed_seconds")
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        return None
    unresolved = count_reported_source_gaps(counts)
    return {
        "asset_class": match.group("asset"),
        "mode": source["mode"],
        "generated_at_utc": generated.isoformat(),
        "elapsed_seconds": float(elapsed),
        "status_counts": counts,
        "unresolved_count": unresolved,
    }


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
    source_summary: Path | None = None,
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
    if source_summary is not None and state != "running":
        matched = _matched_source_summary(
            source_summary,
            run_id=str(payload["run_id"]),
            step=step_name,
            started_epoch=started_epoch,
            observed=observed,
        )
        payload["source_summary_status"] = "matched" if matched else "unavailable"
        if matched is not None:
            payload["source_summary"] = matched
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
    parser.add_argument("--source-summary", type=Path)
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
        source_summary=args.source_summary,
    )


if __name__ == "__main__":
    main()
