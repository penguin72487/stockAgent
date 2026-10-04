"""Fail-closed task mutation journal for bounded OpenBB L1 source audits.

This module does not enable the journal by itself. Callers must install the
triggers, complete one exact full source audit, then commit its checkpoint.
Every fallback reason returns ``full``; no file mtime is used as proof that
the mutable task table stayed unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3


JOURNAL_TABLE = "l1_source_task_changes"
CHECKPOINT_KEY = "l1_source_audit_checkpoint_v1"
MAX_INCREMENTAL_TASK_IDS = 100_000
FULL_AUDIT_INTERVAL = timedelta(hours=24)
TRACKED_TASK_COLUMNS = frozenset({
    "task_id", "active", "status", "endpoint", "output_path", "rows", "updated_at",
})

_TRIGGERS = {
    "l1_source_task_insert": f"""
        CREATE TRIGGER IF NOT EXISTS l1_source_task_insert
        AFTER INSERT ON tasks BEGIN
            INSERT INTO {JOURNAL_TABLE}(task_id) VALUES(NEW.task_id);
        END
    """,
    "l1_source_task_delete": f"""
        CREATE TRIGGER IF NOT EXISTS l1_source_task_delete
        AFTER DELETE ON tasks BEGIN
            INSERT INTO {JOURNAL_TABLE}(task_id) VALUES(OLD.task_id);
        END
    """,
    "l1_source_task_update": f"""
        CREATE TRIGGER IF NOT EXISTS l1_source_task_update
        AFTER UPDATE ON tasks
        WHEN OLD.task_id IS NOT NEW.task_id
          OR OLD.active IS NOT NEW.active
          OR OLD.status IS NOT NEW.status
          OR OLD.endpoint IS NOT NEW.endpoint
          OR OLD.output_path IS NOT NEW.output_path
          OR OLD.rows IS NOT NEW.rows
          OR OLD.updated_at IS NOT NEW.updated_at
        BEGIN
            INSERT INTO {JOURNAL_TABLE}(task_id) VALUES(OLD.task_id);
            INSERT INTO {JOURNAL_TABLE}(task_id)
                SELECT NEW.task_id WHERE NEW.task_id IS NOT OLD.task_id;
        END
    """,
}


@dataclass(frozen=True, slots=True)
class SourceAuditPlan:
    mode: str
    reason: str
    task_ids: tuple[str, ...]
    cutoff_seq: int
    schema_version: int
    database_device: int
    database_inode: int
    contract_fingerprint: str
    last_full_at_utc: str | None


def _canonical_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).replace("IF NOT EXISTS ", "")


def _schema_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA schema_version").fetchone()[0])


def _latest_seq(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT seq FROM sqlite_sequence WHERE name=?", (JOURNAL_TABLE,)
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _database_identity(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return int(stat.st_dev), int(stat.st_ino)


def _install_journal(connection: sqlite3.Connection) -> None:
    """Install all capture hooks atomically; reject name-colliding SQL."""

    update_columns = set(re.findall(
        r"OLD\.([a-z_]+) IS NOT NEW\.\1", _TRIGGERS["l1_source_task_update"]
    ))
    if update_columns != TRACKED_TASK_COLUMNS:
        raise RuntimeError("OpenBB L1 source journal tracked-column contract changed")
    with connection:
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS {JOURNAL_TABLE} "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL)"
        )
        table = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
            (JOURNAL_TABLE,),
        ).fetchone()
        expected_table = (
            f"CREATE TABLE {JOURNAL_TABLE} "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL)"
        )
        if table is None or _canonical_sql(str(table[0])) != expected_table:
            raise RuntimeError("OpenBB L1 source journal table schema changed")
        for name, ddl in _TRIGGERS.items():
            connection.execute(ddl)
            row = connection.execute(
                "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone()
            if row is None or _canonical_sql(str(row[0])) != _canonical_sql(ddl):
                raise RuntimeError(f"OpenBB L1 source journal trigger changed: {name}")


def _checkpoint(connection: sqlite3.Connection) -> dict[str, object] | None:
    row = connection.execute(
        "SELECT value FROM archive_meta WHERE key=?", (CHECKPOINT_KEY,)
    ).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(str(row[0]))
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def prepare_source_audit(
    connection: sqlite3.Connection,
    database_path: Path,
    *,
    contract_fingerprint: str,
    now: datetime | None = None,
    force_full: bool = False,
    max_task_ids: int = MAX_INCREMENTAL_TASK_IDS,
) -> SourceAuditPlan:
    """Plan a full or journal-scoped source audit using a durable cutoff."""

    if max_task_ids < 1:
        raise ValueError("max_task_ids must be positive")
    if not contract_fingerprint:
        raise ValueError("contract_fingerprint must not be empty")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    _install_journal(connection)
    schema_version = _schema_version(connection)
    device, inode = _database_identity(database_path)
    cutoff = _latest_seq(connection)
    checkpoint = _checkpoint(connection)
    reason = "force_full" if force_full else "checkpoint_missing"
    last_full: str | None = None
    if not force_full and checkpoint is not None:
        last_full = str(checkpoint.get("last_full_at_utc") or "") or None
        if checkpoint.get("schema_version") != schema_version:
            reason = "schema_changed"
        elif checkpoint.get("database_device") != device or checkpoint.get(
            "database_inode"
        ) != inode:
            reason = "database_replaced"
        elif checkpoint.get("contract_fingerprint") != contract_fingerprint:
            reason = "source_contract_changed"
        elif type(checkpoint.get("cutoff_seq")) is not int or int(
            checkpoint["cutoff_seq"]
        ) > cutoff:
            reason = "journal_regressed"
        else:
            try:
                last_full_at = datetime.fromisoformat(last_full or "")
                if last_full_at.tzinfo is None:
                    raise ValueError("full-audit timestamp has no time zone")
                age = now - last_full_at.astimezone(timezone.utc)
            except (ValueError, TypeError):
                age = FULL_AUDIT_INTERVAL
            if age < timedelta(0) or age >= FULL_AUDIT_INTERVAL:
                reason = "full_audit_due"
            else:
                prior_seq = int(checkpoint["cutoff_seq"])
                # A removed journal row must never make a mutated source look
                # unchanged. AUTOINCREMENT can legitimately have gaps, so an
                # unexplained gap is a conservative full-audit fallback.
                journal_rows = int(connection.execute(
                    f"SELECT COUNT(*) FROM {JOURNAL_TABLE} WHERE seq>? AND seq<=?",
                    (prior_seq, cutoff),
                ).fetchone()[0])
                if journal_rows != cutoff - prior_seq:
                    return SourceAuditPlan(
                        "full", "journal_gap", (), cutoff, schema_version,
                        device, inode, contract_fingerprint, last_full,
                    )
                changed = tuple(
                    str(row[0])
                    for row in connection.execute(
                        f"SELECT DISTINCT task_id FROM {JOURNAL_TABLE} "
                        "WHERE seq>? AND seq<=? LIMIT ?",
                        (prior_seq, cutoff, max_task_ids + 1),
                    )
                )
                if len(changed) > max_task_ids:
                    reason = "change_budget_exceeded"
                else:
                    return SourceAuditPlan(
                        "incremental", "journal_verified", changed, cutoff,
                        schema_version, device, inode, contract_fingerprint,
                        last_full,
                    )
    return SourceAuditPlan(
        "full", reason, (), cutoff, schema_version, device, inode,
        contract_fingerprint, last_full,
    )


def finish_source_audit(
    connection: sqlite3.Connection,
    database_path: Path,
    plan: SourceAuditPlan,
    *,
    now: datetime | None = None,
) -> None:
    """Advance only through the audited cutoff; later task writes stay queued."""

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if _schema_version(connection) != plan.schema_version or _database_identity(
        database_path
    ) != (plan.database_device, plan.database_inode):
        raise RuntimeError("OpenBB L1 source schema or database changed during audit")
    if _latest_seq(connection) < plan.cutoff_seq:
        raise RuntimeError("OpenBB L1 source journal regressed during audit")
    last_full = (
        now.astimezone(timezone.utc).isoformat()
        if plan.mode == "full" else plan.last_full_at_utc
    )
    if not last_full:
        raise RuntimeError("incremental audit has no full baseline")
    checkpoint = {
        "schema_version": plan.schema_version,
        "database_device": plan.database_device,
        "database_inode": plan.database_inode,
        "contract_fingerprint": plan.contract_fingerprint,
        "cutoff_seq": plan.cutoff_seq,
        "last_full_at_utc": last_full,
    }
    with connection:
        connection.execute(
            "INSERT INTO archive_meta(key,value,updated_at) VALUES (?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value=excluded.value,updated_at=excluded.updated_at",
            (
                CHECKPOINT_KEY, json.dumps(checkpoint, sort_keys=True),
                now.astimezone(timezone.utc).isoformat(),
            ),
        )
        connection.execute(
            f"DELETE FROM {JOURNAL_TABLE} WHERE seq<=?", (plan.cutoff_seq,)
        )


def journal_task_ids(connection: sqlite3.Connection) -> tuple[str, ...]:
    """Small diagnostic for tests and benchmark receipts."""

    return tuple(
        str(row[0]) for row in connection.execute(
            f"SELECT task_id FROM {JOURNAL_TABLE} ORDER BY seq"
        )
    )
