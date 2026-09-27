"""Bounded recovery of one failed FinMind institutional parent partition.

These helpers perform no requests and do not change scheduling priorities.
The existing owner, quota checks, and derived-parent gate still own dispatch.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
from typing import Any, Iterator
import uuid

from downloader.artifact_io import atomic_write_json, sha256_file


_LONG = "TaiwanStockInstitutionalInvestorsBuySell"
_WIDE = "TaiwanStockInstitutionalInvestorsBuySellWide"
_RECOVERABLE = frozenset({"corrupt_long_parquet", "missing_long_parquet"})


def _signature(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"device": stat.st_dev, "inode": stat.st_ino, "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns, "ctime_ns": stat.st_ctime_ns}


def _local_path(root: Path, relative: object) -> Path | None:
    if not isinstance(relative, str) or not relative:
        return None
    part = Path(relative)
    if part.is_absolute() or ".." in part.parts:
        return None
    path = root / part
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        return None
    return path


@contextmanager
def _transaction(connection: sqlite3.Connection) -> Iterator[None]:
    """Commit our own recovery, or preserve a caller's transaction boundary."""
    nested = connection.in_transaction
    connection.execute("SAVEPOINT finmind_parent_recovery" if nested else "BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        if nested:
            connection.execute("ROLLBACK TO SAVEPOINT finmind_parent_recovery")
            connection.execute("RELEASE SAVEPOINT finmind_parent_recovery")
        else:
            connection.rollback()
        raise
    else:
        if nested:
            connection.execute("RELEASE SAVEPOINT finmind_parent_recovery")
        else:
            connection.commit()


def _row(connection: sqlite3.Connection, dataset: str, data_id: str,
         partition: str) -> dict[str, Any] | None:
    cursor = connection.execute(
        "SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
        (dataset, data_id, partition),
    )
    values = cursor.fetchone()
    return dict(zip((field[0] for field in cursor.description), values)) if values else None


def _invalid_parent(root: Path, parent: dict[str, Any]) -> tuple[bytes, dict[str, Any]] | None:
    """Independently prove a missing/corrupt file; unsafe identity is not proof."""
    path = _local_path(root, parent.get("receipt_path"))
    if path is None:
        return None
    try:
        receipt_bytes = path.read_bytes()
        receipt = json.loads(receipt_bytes)
    except (OSError, ValueError):
        return None
    if not isinstance(receipt, dict) or any(
        receipt.get(field) != parent[field] for field in ("dataset", "data_id", "partition")
    ) or receipt.get("status") != "complete":
        return None
    relative = receipt.get("parquet_path")
    evidence: dict[str, Any] = {
        "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "parquet_path": relative,
        "expected_sha256": receipt.get("sha256"),
        "expected_size_bytes": receipt.get("parquet_size_bytes"),
        "expected_rows": receipt.get("rows"),
    }
    if not isinstance(relative, str) or not relative:
        evidence["reason"] = "missing_long_parquet_reference"
        return receipt_bytes, evidence
    source = _local_path(root, relative)
    if source is None:
        return None
    try:
        before = _signature(source)
        if not source.is_file():
            return None
        actual_hash = sha256_file(source)
        after = _signature(source)
    except FileNotFoundError:
        evidence["reason"] = "missing_long_parquet"
        return receipt_bytes, evidence
    except OSError:
        return None
    if before != after:
        return None  # Another writer changed the file while it was inspected.
    evidence.update({"actual_signature": after, "actual_sha256": actual_hash})
    if actual_hash != receipt.get("sha256"):
        evidence["reason"] = "long_parquet_hash_mismatch"
    elif (receipt.get("parquet_size_bytes") is not None and
          after["size"] != receipt["parquet_size_bytes"]):
        evidence["reason"] = "long_parquet_size_mismatch"
    else:
        return None
    return receipt_bytes, evidence


def recover_failed_long_parent(connection: sqlite3.Connection, root: Path,
                               task: Any, error: Any, now: datetime) -> dict[str, Any] | None:
    """Audit and requeue just the failed wide task's proven invalid LONG parent.

    Return ``None`` when there is no independently verified recovery to perform.
    A returned receipt means the caller must not overwrite the derived wait with
    its ordinary failure handler. Both queue updates and the append-only audit
    are atomic. An existing caller transaction remains the caller's to commit.
    """
    code = getattr(error, "code", error)
    if task.dataset != _WIDE or task.kind != "derived" or code not in _RECOVERABLE:
        return None
    with _transaction(connection):
        parent = _row(connection, _LONG, task.data_id, task.partition)
        derived = _row(connection, _WIDE, task.data_id, task.partition)
        if (parent is None or derived is None or parent["state"] != "complete" or
                derived["kind"] != "derived"):
            return None
        invalid = _invalid_parent(Path(root), parent)
        if invalid is None:
            return None
        receipt_bytes, evidence = invalid
        stamp = now.astimezone(UTC).isoformat()
        connection.execute("""
            CREATE TABLE IF NOT EXISTS long_parent_recovery_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recovered_at_utc TEXT NOT NULL,
                dataset TEXT NOT NULL, data_id TEXT NOT NULL, partition TEXT NOT NULL,
                error_code TEXT NOT NULL, validation_reason TEXT NOT NULL,
                parent_state_json TEXT NOT NULL, derived_state_json TEXT NOT NULL,
                receipt_path TEXT NOT NULL, receipt_bytes BLOB NOT NULL,
                validation_json TEXT NOT NULL
            )
        """)
        cursor = connection.execute(
            "INSERT INTO long_parent_recovery_audit "
            "(recovered_at_utc,dataset,data_id,partition,error_code,validation_reason,"
            "parent_state_json,derived_state_json,receipt_path,receipt_bytes,validation_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (stamp, _LONG, task.data_id, task.partition, code, evidence["reason"],
             json.dumps(parent, sort_keys=True), json.dumps(derived, sort_keys=True),
             parent["receipt_path"], receipt_bytes, json.dumps(evidence, sort_keys=True)),
        )
        connection.execute(
            "UPDATE tasks SET state='pending',next_attempt_at_utc=NULL,error_code=? "
            "WHERE dataset=? AND data_id=? AND partition=?",
            (f"invalidated_long_parent:{code}", _LONG, task.data_id, task.partition),
        )
        # Pending plus the existing scheduler's parent-state gate is an event-
        # driven wait: no periodic failed derived job can run before its parent.
        connection.execute(
            "UPDATE tasks SET state='pending',next_attempt_at_utc=NULL,"
            "last_attempt_at_utc=?,error_code=? WHERE dataset=? AND data_id=? AND partition=?",
            (stamp, f"waiting_long_parent:{code}", _WIDE, task.data_id, task.partition),
        )
        return {"status": "waiting_long_parent", "audit_id": cursor.lastrowid,
                "dataset": _LONG, "data_id": task.data_id, "partition": task.partition,
                "error_code": code, "validation": evidence}


def repair_content_addressed_collision(staged: Path, final: Path, root: Path) -> bool:
    """Preserve one corrupt digest-named file before installing its verified bytes.

    The caller must own the downloader's normal writer lock. This operates only
    on the two explicitly supplied sibling files under its mutable workspace.
    ``True`` means staged was installed; ``False`` means final was already valid.
    It does not delete evidence or attempt to repair any other source file.
    """
    root = Path(root).resolve()
    staged, final = Path(staged), Path(final)
    for path in (staged, final):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError("unsafe FinMind collision path")
        if not path.is_file():
            raise ValueError("missing FinMind collision file")
    if staged.parent.resolve() != final.parent.resolve() or staged.name != "latest.parquet":
        raise ValueError("FinMind collision must use the canonical sibling staging file")
    if re.fullmatch(r"[0-9a-f]{64}\.parquet", final.name) is None:
        raise ValueError("FinMind collision target must be content-addressed")
    expected = final.stem
    staged_signature = _signature(staged)
    if sha256_file(staged) != expected or staged_signature != _signature(staged):
        raise ValueError("FinMind collision replacement digest mismatch")
    old_signature = _signature(final)
    old_hash = sha256_file(final)
    if old_signature != _signature(final):
        raise ValueError("FinMind collision source changed during verification")
    if old_hash == expected:
        return False
    evidence_root = root / "corrupt_parquet_evidence" / uuid.uuid4().hex
    if not evidence_root.resolve().is_relative_to(root):
        raise ValueError("unsafe FinMind collision evidence path")
    evidence_root.mkdir(parents=True, exist_ok=False)
    preserved = evidence_root / "original.parquet"
    with final.open("rb") as source, preserved.open("xb") as target:
        shutil.copyfileobj(source, target, length=1 << 20)
        target.flush()
        os.fsync(target.fileno())
    if sha256_file(preserved) != old_hash or _signature(final) != old_signature:
        raise ValueError("FinMind collision source changed during preservation")
    receipt = {
        "schema_version": 1, "status": "verified_replacement_ready",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "original_path": str(final.resolve().relative_to(root)),
        "preserved_path": str(preserved.relative_to(root)),
        "original_sha256": old_hash, "original_signature": old_signature,
        "replacement_sha256": expected, "replacement_signature": staged_signature,
    }
    atomic_write_json(evidence_root / "prepared.json", receipt)
    if (_signature(final) != old_signature or _signature(staged) != staged_signature or
            sha256_file(staged) != expected):
        raise ValueError("FinMind collision files changed before replacement")
    os.replace(staged, final)
    atomic_write_json(evidence_root / "installed.json", {
        **receipt, "status": "installed", "installed_at_utc": datetime.now(UTC).isoformat(),
    })
    return True
