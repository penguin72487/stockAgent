#!/usr/bin/env python3
"""Observe registered TEJ listing tasks without performing provider requests."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_priority import retained_request
from scripts.audit_tw_futures_tej_listing_snapshots import TABLE_ID, audit


ROOT = Path(__file__).resolve().parents[1]


def observe(root: Path, plan: dict, registration: dict) -> dict:
    requests, registered = plan.get("requests"), registration.get("tasks")
    if (not isinstance(requests, list) or not isinstance(registered, list)
            or len(requests) != len(registered) or not requests):
        raise ValueError("registered source plan is incomplete")
    ids = [item["task_id"] for item in registered]
    if len(ids) != len(set(ids)):
        raise ValueError("registered task ids are duplicated")
    with sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=10) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        definition = db.execute("SELECT * FROM tables WHERE table_id=?", (TABLE_ID,)).fetchone()
        if definition is None:
            raise ValueError("registered native table is missing")
        rows = db.execute("SELECT * FROM tasks WHERE task_id IN (" + ",".join("?" for _ in ids) + ")", ids).fetchall()
        found = {row["task_id"]: dict(row) for row in rows}
        if set(found) != set(ids):
            raise ValueError("registered native task is missing")
        status = []
        for item, record in zip(requests, registered, strict=True):
            row = found[record["task_id"]]
            actual = retained_request(row, dict(definition))
            expected = item["request"]
            if (row["table_id"] != TABLE_ID or item["table_id"] != TABLE_ID or actual is None
                    or any(actual.get(k) != expected.get(k) for k in
                           ("company_labels", "date_labels", "source_key_mode", "key_layout_contract", "frequency"))):
                raise ValueError("native task scope differs from registered request")
            status.append({"task_id": row["task_id"], "state": row["state"],
                           "actual_rows": row["actual_rows"], "receipt_path": row["receipt_path"],
                           "last_error_code": row["last_error_code"]})
        db.rollback()
    return {"contract": "tw_futures_tej_first_listing_watch_v1",
            "observed_at_utc": datetime.now(UTC).isoformat(), "items": status,
            "state_counts": dict(Counter(item["state"] for item in status)),
            "provider_calls": 0, "training_admitted": False,
            "source_completion_requires_receipt_audit": True}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--plan-dir", required=True, type=Path)
    p.add_argument("--candidates", required=True, type=Path)
    p.add_argument("--candidate-report", required=True, type=Path)
    p.add_argument("--hours", type=float, default=168)
    p.add_argument("--poll-seconds", type=int, default=120)
    p.add_argument("--once", action="store_true")
    args = p.parse_args()
    out = args.plan_dir.resolve(strict=True)
    if (not out.is_relative_to(ROOT / "artifacts/markets") or not out.is_dir()
            or not 0 < args.hours <= 168 or args.poll_seconds < 30):
        raise ValueError("bounded market-artifact observer required")
    paths = [out / "plan.json", out / "registration.json", args.candidates.resolve(strict=True),
             args.candidate_report.resolve(strict=True), Path(__file__).resolve(),
             ROOT / "scripts/audit_tw_futures_tej_listing_snapshots.py"]
    pins = {str(path): sha256_file(path) for path in paths}
    plan, registration = (json.loads(path.read_bytes()) for path in paths[:2])
    if registration.get("plan_sha256") != pins[str(paths[0])]:
        raise ValueError("registration no longer matches its plan")
    deadline = datetime.now(UTC) + timedelta(hours=args.hours)
    previous = None
    while True:
        if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
            raise ValueError("observer inputs or implementation changed")
        status = observe(ROOT / "data_tej", plan, registration)
        status.update(deadline_utc=deadline.isoformat(), input_sha256=pins)
        states = status["state_counts"]
        if all(item["state"] == "complete" for item in status["items"]):
            destination = out / "tej-listing-snapshot-audit-after-priority.json"
            if destination.exists():
                raise ValueError("preserve the existing post-priority audit")
            try:
                report = audit(ROOT, paths[2], paths[3], destination)
            except ValueError as exc:
                if str(exc) != "source task inventory or candidate input changed during audit":
                    raise
                status["audit_state"] = "source_inventory_transition_retry"
            else:
                status.update(audit_state="complete", audit_path=str(destination),
                              audit_sha256=sha256_file(destination),
                              rejected_source_tasks=len(report["vendor_tasks_rejected"]))
        atomic_write_json(out / "watch-status.json", status, durable=True)
        if states != previous:
            print(json.dumps({"observed_at_utc": status["observed_at_utc"],
                              "state_counts": states, "provider_calls": 0}, ensure_ascii=False), flush=True)
            previous = states
        if status.get("audit_state") == "complete":
            return 0 if status["rejected_source_tasks"] == 0 else 75
        if (args.once or datetime.now(UTC) >= deadline
                or any(item["state"] not in {"pending", "running", "complete"} for item in status["items"])):
            return 75
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
