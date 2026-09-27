from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sqlite3

import pytest

from scripts.openbb_l1_source_journal import (
    finish_source_audit,
    journal_task_ids,
    prepare_source_audit,
)


NOW = datetime(2026, 9, 26, tzinfo=timezone.utc)


def _database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE tasks (
            task_id TEXT PRIMARY KEY,
            active INTEGER NOT NULL,
            status TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            output_path TEXT NOT NULL,
            rows INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE archive_meta (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        """
    )
    connection.commit()
    return connection


def _baseline(connection: sqlite3.Connection, path: Path) -> None:
    plan = prepare_source_audit(
        connection, path, contract_fingerprint="contract-v1", now=NOW
    )
    assert plan.mode == "full"
    finish_source_audit(connection, path, plan, now=NOW)


def test_journal_captures_contract_changes_and_preserves_later_cutoff(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manifest.sqlite3"
    connection = _database(path)
    try:
        _baseline(connection, path)
        connection.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            ("a", 1, "success", "e", "a.parquet", 10, "t1", 0),
        )
        connection.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            ("b", 1, "success", "e", "b.parquet", 10, "t1", 0),
        )
        connection.commit()
        connection.execute("UPDATE tasks SET attempts=1 WHERE task_id='a'")
        connection.commit()
        assert journal_task_ids(connection) == ("a", "b")

        first = prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1", now=NOW
        )
        assert first.mode == "incremental"
        assert set(first.task_ids) == {"a", "b"}
        connection.execute("UPDATE tasks SET rows=11 WHERE task_id='b'")
        connection.commit()
        finish_source_audit(connection, path, first, now=NOW)
        assert journal_task_ids(connection) == ("b",)

        second = prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1", now=NOW
        )
        assert second.task_ids == ("b",)
        finish_source_audit(connection, path, second, now=NOW)
        assert journal_task_ids(connection) == ()

        connection.execute("UPDATE tasks SET task_id='c' WHERE task_id='a'")
        connection.commit()
        assert journal_task_ids(connection) == ("a", "c")
        connection.execute("DELETE FROM tasks WHERE task_id='b'")
        connection.commit()
        assert journal_task_ids(connection) == ("a", "c", "b")
    finally:
        connection.close()


def test_journal_full_fallbacks_and_rolled_back_writes(tmp_path: Path) -> None:
    path = tmp_path / "manifest.sqlite3"
    connection = _database(path)
    try:
        _baseline(connection, path)
        connection.execute("BEGIN")
        connection.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            ("rolled", 1, "success", "e", "x", 1, "t1", 0),
        )
        connection.rollback()
        assert journal_task_ids(connection) == ()
        connection.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            ("kept", 1, "success", "e", "x", 1, "t1", 0),
        )
        connection.commit()
        too_many = prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1",
            now=NOW, max_task_ids=1,
        )
        assert too_many.mode == "incremental"
        assert prepare_source_audit(
            connection, path, contract_fingerprint="contract-v2", now=NOW,
        ).reason == "source_contract_changed"
        assert prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1",
            now=NOW + timedelta(hours=25),
        ).reason == "full_audit_due"
        connection.execute("CREATE TABLE unrelated(x INTEGER)")
        connection.commit()
        assert prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1", now=NOW,
        ).reason == "schema_changed"
    finally:
        connection.close()


def test_journal_rejects_wrong_trigger_definition(tmp_path: Path) -> None:
    path = tmp_path / "manifest.sqlite3"
    connection = _database(path)
    try:
        _baseline(connection, path)
        connection.execute("DROP TRIGGER l1_source_task_update")
        connection.execute(
            "CREATE TRIGGER l1_source_task_update AFTER UPDATE ON tasks "
            "BEGIN SELECT 1; END"
        )
        connection.commit()
        with pytest.raises(RuntimeError, match="trigger changed"):
            prepare_source_audit(
                connection, path, contract_fingerprint="contract-v1", now=NOW,
            )
    finally:
        connection.close()


def test_journal_deleted_unread_changes_force_full_audit(tmp_path: Path) -> None:
    path = tmp_path / "manifest.sqlite3"
    connection = _database(path)
    try:
        _baseline(connection, path)
        for task_id in ("a", "b"):
            connection.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
                (task_id, 1, "success", "e", f"{task_id}.parquet", 1, "t1", 0),
            )
        connection.execute(
            "DELETE FROM l1_source_task_changes WHERE task_id='a'"
        )
        connection.commit()
        plan = prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1", now=NOW
        )
        assert (plan.mode, plan.reason) == ("full", "journal_gap")
    finally:
        connection.close()


def test_journal_naive_full_timestamp_forces_full_audit(tmp_path: Path) -> None:
    path = tmp_path / "manifest.sqlite3"
    connection = _database(path)
    try:
        _baseline(connection, path)
        row = connection.execute(
            "SELECT value FROM archive_meta WHERE key='l1_source_audit_checkpoint_v1'"
        ).fetchone()
        checkpoint = json.loads(str(row[0]))
        checkpoint["last_full_at_utc"] = "2026-09-26T00:00:00"
        connection.execute(
            "UPDATE archive_meta SET value=? "
            "WHERE key='l1_source_audit_checkpoint_v1'",
            (json.dumps(checkpoint),),
        )
        connection.commit()
        plan = prepare_source_audit(
            connection, path, contract_fingerprint="contract-v1", now=NOW
        )
        assert (plan.mode, plan.reason) == ("full", "full_audit_due")
    finally:
        connection.close()


def test_preexisting_writer_connection_observes_installed_triggers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "manifest.sqlite3"
    owner = _database(path)
    writer = sqlite3.connect(path)
    try:
        writer.execute("SELECT COUNT(*) FROM tasks").fetchone()
        _baseline(owner, path)
        writer.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            ("a", 1, "success", "e", "a.parquet", 1, "t1", 0),
        )
        writer.commit()
        assert journal_task_ids(owner) == ("a",)
        writer.execute("UPDATE tasks SET updated_at='t2' WHERE task_id='a'")
        writer.commit()
        assert journal_task_ids(owner) == ("a", "a")
    finally:
        writer.close()
        owner.close()
