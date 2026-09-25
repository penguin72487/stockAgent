#!/usr/bin/env python3
"""Publish TW public live data to cold storage outside the opening path.

This job creates an immutable packed release for Syncthing backup only.  It
never calls ``stockagent-data use`` and never changes the runtime data link.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
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


class SourceRefreshBusy(RuntimeError):
    """Another canonical refresh or publication owns the mutable source."""


class StaleDerivedReceipts(RuntimeError):
    """The cold source cannot be released until upstream derivatives are fresh."""

    def __init__(self, codes: list[str]) -> None:
        self.codes = sorted(set(codes))
        super().__init__(
            "TW public derived receipts are stale: " + ", ".join(self.codes)
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/cold_publish/latest.json"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=7200.0)
    parser.add_argument(
        "--defer-stale-derived-receipts",
        action="store_true",
        help="Record a deferred cold publish after a separate source-only job succeeded",
    )
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


def _active_writer_labels(payload: Mapping[str, Any] | None) -> list[str]:
    """Return sanitized active-writer identities from publish-status output."""

    if not isinstance(payload, Mapping):
        return []
    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or len(datasets) != 1:
        return []
    row = datasets[0]
    if not isinstance(row, Mapping):
        return []
    blockers = row.get("active_blockers")
    if not isinstance(blockers, list):
        return []
    return sorted(
        {
            str(blocker.get("pattern") or "registered_writer")
            for blocker in blockers
            if isinstance(blocker, Mapping)
        }
    )


def _live_root() -> Path:
    catalog = json.loads(
        (REPO_ROOT / "configs/data_sync/packed_datasets.json").read_text(
            encoding="utf-8"
        )
    )
    for entry in catalog.get("datasets", []):
        if entry.get("dataset") == "tw-public":
            source = Path(entry["source"])
            return (source if source.is_absolute() else REPO_ROOT / source).resolve()
    raise ValueError("tw-public is absent from the publication catalog")


def _check_training_receipts(live_root: Path) -> None:
    """Reuse the strict data-layer receipt checks before freezing derived files."""

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from scripts.audit_tw_public_data_layer import (
        audit_feature_build_receipt,
        audit_official_symbol_build,
    )

    stocks = live_root / "stocks"
    features = live_root / "features/tw_public_stock_daily.parquet"
    _, symbol_findings = audit_official_symbol_build(stocks, live_root)
    _, feature_findings = audit_feature_build_receipt(features, live_root, stocks)
    blocking = [
        finding.code
        for finding in [*symbol_findings, *feature_findings]
        if finding.severity in {"critical", "high"}
    ]
    if blocking:
        raise StaleDerivedReceipts(blocking)


def _publish_while_source_stable(command: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    """Exclude the canonical source monitor while packing one immutable release."""

    lock_path = _live_root().parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SourceRefreshBusy("canonical TW public refresh is active; retry publication") from exc
        _check_training_receipts(_live_root())
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


def _persist_receipt(
    receipt: Path,
    *,
    started: datetime,
    payload: Mapping[str, Any],
) -> None:
    _atomic_json(receipt, payload)
    run_path = receipt.parent / "runs" / (
        started.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)


def main() -> int:
    args = parse_args()
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive")
    started = datetime.now(TAIPEI)
    receipt = _repo_path(args.receipt)
    status_command = [
        str(REPO_ROOT / "scripts" / "run_data_cache.sh"),
        "publish-status",
        "tw-public",
    ]
    try:
        status_result = subprocess.run(
            status_command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=min(args.timeout_seconds, 60.0),
        )
    except subprocess.TimeoutExpired:
        # The status probe is only an optimization. The authoritative publish
        # command still owns locking and must remain able to report its error.
        writer_labels: list[str] = []
    else:
        writer_labels = _active_writer_labels(
            _parse_output(status_result.stdout.strip())
            if status_result.returncode == 0
            else None
        )
    if writer_labels:
        completed_at = datetime.now(TAIPEI)
        payload = {
            "schema_version": 1,
            "status": "deferred",
            "reason": "active_registered_writer",
            "started_at_taipei": started.isoformat(),
            "completed_at_taipei": completed_at.isoformat(),
            "elapsed_seconds": (completed_at - started).total_seconds(),
            "dataset": "tw-public",
            "source_authority": "catalog_mutable_live_root",
            "opening_dependency": False,
            "runtime_link_changed": False,
            "materialization_performed": False,
            "return_code": 0,
            "release": None,
            "error": None,
            "active_writer_count": len(writer_labels),
            "active_writer_patterns": writer_labels,
        }
        _persist_receipt(receipt, started=started, payload=payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    command = [
        str(REPO_ROOT / "scripts" / "run_data_cache.sh"),
        "publish",
        "tw-public",
    ]
    try:
        completed = _publish_while_source_stable(command, args.timeout_seconds)
        return_code = int(completed.returncode)
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        release = _parse_output(stdout)
        error = None if return_code == 0 else (stderr or stdout)[-4000:]
    except subprocess.TimeoutExpired as exc:
        return_code = 124
        release = None
        error = f"cold publish timed out after {args.timeout_seconds:.0f}s: {exc}"
    except SourceRefreshBusy:
        completed_at = datetime.now(TAIPEI)
        payload = {
            "schema_version": 1,
            "status": "deferred",
            "reason": "canonical_refresh_lock_busy",
            "started_at_taipei": started.isoformat(),
            "completed_at_taipei": completed_at.isoformat(),
            "elapsed_seconds": (completed_at - started).total_seconds(),
            "dataset": "tw-public",
            "source_authority": "catalog_mutable_live_root",
            "opening_dependency": False,
            "runtime_link_changed": False,
            "materialization_performed": False,
            "return_code": 0,
            "release": None,
            "error": None,
        }
        _persist_receipt(receipt, started=started, payload=payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    except StaleDerivedReceipts as exc:
        if not args.defer_stale_derived_receipts:
            return_code = 75
            release = None
            error = str(exc)
        else:
            completed_at = datetime.now(TAIPEI)
            payload = {
                "schema_version": 1,
                "status": "deferred",
                "reason": "stale_derived_receipts",
                "blocking_findings": exc.codes,
                "started_at_taipei": started.isoformat(),
                "completed_at_taipei": completed_at.isoformat(),
                "elapsed_seconds": (completed_at - started).total_seconds(),
                "dataset": "tw-public",
                "source_authority": "catalog_mutable_live_root",
                "opening_dependency": False,
                "runtime_link_changed": False,
                "materialization_performed": False,
                "return_code": 0,
                "release": None,
                "error": None,
            }
            _persist_receipt(receipt, started=started, payload=payload)
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0
    except (OSError, ValueError, RuntimeError) as exc:
        return_code = 75
        release = None
        error = str(exc)
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
    _persist_receipt(receipt, started=started, payload=payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
