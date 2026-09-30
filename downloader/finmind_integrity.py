"""Bounded verification/recovery inside the existing FinMind owner queues.

No network, quota bucket, or alternative scheduler. Call mutation only while
holding that lane's worker.lock; original receipts and queue rows are retained.
"""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json
from downloader.finmind_parent_recovery import _local_path, _transaction
from downloader.parquet_integrity import parquet_receipt_error


def inspect_completed(root: Path, task: dict) -> tuple[str | None, bytes | None]:
    path = _local_path(root, task.get("receipt_path"))
    if path is None:
        return "unsafe_or_missing_receipt_path", None
    raw = None
    try:
        raw = path.read_bytes()
        receipt = json.loads(raw)
        if not isinstance(receipt, dict) or any(
            receipt.get(key) != task[key] for key in ("dataset", "data_id", "partition", "rows")
        ) or receipt.get("status") != "complete":
            return "receipt_identity_or_rows_mismatch", raw
        error = parquet_receipt_error(root, receipt)
        if path.read_bytes() != raw:
            return "receipt_changed_during_check", raw
        return error, raw
    except (OSError, ValueError):
        return "unreadable_receipt", raw


def audit_queue(connection: sqlite3.Connection, root: Path, *, after_rowid: int = 0,
                limit: int = 512, repair: bool = False,
                datasets: tuple[str, ...] = ()) -> dict:
    if limit < 1 or limit > 10000:
        raise ValueError("integrity batch limit must be 1..10000")
    query = "SELECT rowid AS integrity_rowid,* FROM tasks WHERE state='complete' AND rows>0 AND rowid>?"
    params: list = [after_rowid]
    if datasets:
        query += " AND dataset IN (" + ",".join("?" for _ in datasets) + ")"
        params.extend(datasets)
    query += " ORDER BY rowid LIMIT ?"
    cursor = connection.execute(query, (*params, limit))
    names = [field[0] for field in cursor.description]
    tasks = [dict(zip(names, row)) for row in cursor.fetchall()]
    failures = []
    for task in tasks:
        error, raw = inspect_completed(root, task)
        if error is None:
            continue
        item = {key: task[key] for key in ("dataset", "data_id", "partition", "receipt_path")}
        item.update(error=error, requeued=False)
        # Concurrent changes are not proof of permanent corruption.
        if repair and "changed_during_check" not in error:
            with _transaction(connection):
                current = connection.execute(
                    "SELECT rowid AS integrity_rowid,* FROM tasks WHERE rowid=?",
                    (task["integrity_rowid"],),
                )
                found = current.fetchone()
                latest = dict(zip([field[0] for field in current.description], found)) if found else None
                checked_error, checked_raw = inspect_completed(root, task)
                if latest == task and checked_error == error and checked_raw == raw:
                    connection.execute("""CREATE TABLE IF NOT EXISTS local_integrity_audit (
                        id INTEGER PRIMARY KEY, observed_at_utc TEXT NOT NULL,
                        dataset TEXT NOT NULL,data_id TEXT NOT NULL,partition TEXT NOT NULL,
                        reason TEXT NOT NULL,prior_task_json TEXT NOT NULL,receipt_bytes BLOB,
                        receipt_sha256 TEXT)""")
                    record = connection.execute(
                        "INSERT INTO local_integrity_audit (observed_at_utc,dataset,data_id,partition,"
                        "reason,prior_task_json,receipt_bytes,receipt_sha256) VALUES (?,?,?,?,?,?,?,?)",
                        (datetime.now(UTC).isoformat(), task["dataset"], task["data_id"], task["partition"],
                         error, json.dumps(task, sort_keys=True), raw,
                         hashlib.sha256(raw).hexdigest() if raw is not None else None),
                    )
                    connection.execute(
                        "UPDATE tasks SET state='pending',next_attempt_at_utc=NULL,error_code=? WHERE rowid=?",
                        (f"local_integrity:{error}", task["integrity_rowid"]),
                    )
                    item.update(requeued=True, audit_id=record.lastrowid)
        failures.append(item)
    return {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
            "checked": len(tasks), "failed": len(failures),
            "requeued": sum(row["requeued"] for row in failures), "failures": failures,
            "last_rowid": tasks[-1]["integrity_rowid"] if tasks else after_rowid,
            "end_of_sweep": len(tasks) < limit,
            "proof_scope": "current_receipt_bytes_rows_sha256_not_source_completeness_or_pit"}


def audit_completed_batch(connection: sqlite3.Connection, root: Path, *, limit: int = 512) -> dict:
    """Resume a rotating audit on each normal worker batch, without full rescans."""
    path = root / "integrity_status.json"
    try:
        previous = json.loads(path.read_text())
        cursor = int(previous.get("cursor", 0))
    except (OSError, ValueError, TypeError, AttributeError):
        previous, cursor = {}, 0
    result = audit_queue(connection, root, after_rowid=cursor, limit=limit, repair=True)
    result.update(cursor=0 if result["end_of_sweep"] else result["last_rowid"],
                  completed_sweeps=int(previous.get("completed_sweeps", 0)) + int(result["end_of_sweep"]),
                  total_requeued=int(previous.get("total_requeued", 0)) + result["requeued"])
    atomic_write_json(path, result)
    return result
