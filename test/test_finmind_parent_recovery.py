from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from downloader import download_finmind_complement as complement
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_parent_recovery as recovery


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
LONG = complement.LONG_INSTITUTIONAL
WIDE = complement.WIDE_INSTITUTIONAL


def _fixture(connection: sqlite3.Connection, root: Path, *, data_id: str = "",
             partition: str = "2023-08-21") -> tuple[complement.Task, Path, Path]:
    kind = "id_history" if data_id else "day"
    parent = complement.Task(LONG, data_id, partition, kind, 2, "pending")
    derived = complement.Task(WIDE, data_id, partition, "derived", 2, "inflight")
    connection.executemany(
        "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,?,?,?,?,?)",
        [(task.dataset, task.data_id, task.partition, task.kind, task.priority, task.state)
         for task in (parent, derived)],
    )
    receipt = complement._store(root, parent, [{
        "date": "2023-08-21", "stock_id": data_id or "2330", "name": "Foreign_Investor",
        "buy": 10, "sell": 3,
    }], NOW)
    connection.execute(
        "UPDATE tasks SET state='complete',receipt_path=?,rows=?,bytes=?,last_attempt_at_utc=? "
        "WHERE dataset=? AND data_id=? AND partition=?",
        (receipt["receipt_path"], receipt["rows"], receipt["parquet_size_bytes"], NOW.isoformat(),
         LONG, data_id, partition),
    )
    connection.commit()
    return derived, root / receipt["receipt_path"], root / receipt["parquet_path"]


@pytest.mark.parametrize("damage", ["missing", "zero_bytes"])
def test_recovery_only_requeues_verified_invalid_partition_and_preserves_audit(
    tmp_path: Path, damage: str,
) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        task, receipt_path, source = _fixture(connection, tmp_path)
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,'','2023-08-22','day',2,'complete')", (LONG,),
        )
        connection.commit()
        original_receipt = receipt_path.read_bytes()
        if damage == "missing":
            source.unlink()
        else:
            source.write_bytes(b"")
        code = "missing_long_parquet" if damage == "missing" else "corrupt_long_parquet"
        result = recovery.recover_failed_long_parent(
            connection, tmp_path, task, complement.SourceError(code), NOW,
        )
        assert result is not None and result["status"] == "waiting_long_parent"
        assert receipt_path.read_bytes() == original_receipt
        assert source.exists() is (damage != "missing")
        if source.exists():
            assert source.read_bytes() == b""
        states = connection.execute(
            "SELECT dataset,partition,state,priority,next_attempt_at_utc FROM tasks "
            "ORDER BY dataset,partition",
        ).fetchall()
        assert states == [(LONG, "2023-08-21", "pending", 2, None),
                          (LONG, "2023-08-22", "complete", 2, None),
                          (WIDE, "2023-08-21", "pending", 2, None)]
        audit = connection.execute(
            "SELECT parent_state_json,derived_state_json,receipt_bytes,validation_json "
            "FROM long_parent_recovery_audit",
        ).fetchone()
        assert json.loads(audit[0])["state"] == "complete"
        assert json.loads(audit[0])["rows"] == 1
        assert json.loads(audit[1])["state"] == "inflight"
        assert audit[2] == original_receipt
        assert json.loads(audit[3])["expected_sha256"] == json.loads(original_receipt)["sha256"]
        assert recovery.recover_failed_long_parent(connection, tmp_path, task, code, NOW) is None
        assert connection.execute("SELECT count(*) FROM long_parent_recovery_audit").fetchone() == (1,)
        # A derived task is locally free, but the old parent still needs the
        # existing historical quota; the reserved incremental lane cannot run it.
        assert sponsor._next(connection, NOW, incremental_only=True) is None
        selected = sponsor._next(connection, NOW)
        assert selected is not None and selected.dataset == LONG
        assert sponsor._next(connection, NOW) is None


def test_complement_recovers_only_same_symbol_history_and_waits_for_parent(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        task, _, source = _fixture(connection, tmp_path, data_id="2330", partition="history")
        other, _, _ = _fixture(connection, tmp_path, data_id="2317", partition="history")
        connection.execute("UPDATE tasks SET state='complete' WHERE dataset=? AND data_id=?",
                           (other.dataset, other.data_id))
        connection.commit()
        source.write_bytes(b"")
        assert recovery.recover_failed_long_parent(
            connection, tmp_path, task, "corrupt_long_parquet", NOW,
        ) is not None
        assert complement._next_task(connection, NOW, incremental_only=True) is None
        selected = complement._next_task(connection, NOW)
        assert selected is not None and selected.dataset == LONG and selected.data_id == "2330"
        # Normal successful parent storage wakes the existing derived gate.
        receipt = complement._store(tmp_path, selected, [{
            "date": "2023-08-21", "stock_id": "2330", "name": "Foreign_Investor",
            "buy": 11, "sell": 3,
        }], NOW)
        complement._save_result(connection, selected, receipt, NOW)
        ready = complement._next_task(connection, NOW)
        assert ready is not None and ready.dataset == WIDE and ready.data_id == "2330"
        assert connection.execute("SELECT state FROM tasks WHERE data_id='2317'").fetchall() == [
            ("complete",), ("complete",),
        ]


@pytest.mark.parametrize("variant", ["valid", "wrong_receipt_partition", "unsafe_path",
                                    "unsupported_error", "parent_inflight"])
def test_no_reset_without_independent_safe_parent_failure(tmp_path: Path, variant: str) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        task, receipt_path, source = _fixture(connection, tmp_path)
        if variant != "valid":
            source.write_bytes(b"")
        if variant in {"wrong_receipt_partition", "unsafe_path"}:
            receipt = json.loads(receipt_path.read_text())
            if variant == "wrong_receipt_partition":
                receipt["partition"] = "2023-08-22"
            else:
                receipt["parquet_path"] = "../external.parquet"
            receipt_path.write_text(json.dumps(receipt))
        if variant == "parent_inflight":
            connection.execute("UPDATE tasks SET state='inflight' WHERE dataset=?", (LONG,))
        connection.commit()
        before = connection.execute("SELECT * FROM tasks ORDER BY dataset").fetchall()
        error = "invalid_long_row" if variant == "unsupported_error" else "corrupt_long_parquet"
        assert recovery.recover_failed_long_parent(connection, tmp_path, task, error, NOW) is None
        assert connection.execute("SELECT * FROM tasks ORDER BY dataset").fetchall() == before


def test_recovery_rolls_back_parent_and_audit_if_derived_update_fails(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        task, _, source = _fixture(connection, tmp_path)
        source.write_bytes(b"")
        connection.execute(f"""
            CREATE TRIGGER reject_derived_update BEFORE UPDATE ON tasks
            WHEN NEW.dataset='{WIDE}'
            BEGIN SELECT RAISE(FAIL, 'fixture rejection'); END
        """)
        connection.commit()
        before = connection.execute("SELECT * FROM tasks ORDER BY dataset").fetchall()
        with pytest.raises(sqlite3.IntegrityError, match="fixture rejection"):
            recovery.recover_failed_long_parent(connection, tmp_path, task, "corrupt_long_parquet", NOW)
        assert connection.execute("SELECT * FROM tasks ORDER BY dataset").fetchall() == before
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name='long_parent_recovery_audit'",
        ).fetchone() is None


def test_recovery_respects_existing_transaction_rollback(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        task, _, source = _fixture(connection, tmp_path)
        source.write_bytes(b"")
        connection.execute("BEGIN")
        assert recovery.recover_failed_long_parent(
            connection, tmp_path, task, "corrupt_long_parquet", NOW,
        ) is not None
        assert connection.in_transaction
        connection.rollback()
        assert connection.execute("SELECT state FROM tasks WHERE dataset=?", (LONG,)).fetchone() == (
            "complete",
        )


def _collision_fixture(root: Path, *, original: bytes = b"") -> tuple[Path, Path, bytes]:
    folder = root / "parquet" / LONG / "all" / "2023-08-21"
    folder.mkdir(parents=True)
    replacement = b"verified replacement payload"
    staged = folder / "latest.parquet"
    final = folder / f"{hashlib.sha256(replacement).hexdigest()}.parquet"
    staged.write_bytes(replacement)
    final.write_bytes(original)
    return staged, final, replacement


@pytest.mark.parametrize("original", [b"", b"previous corrupt payload"])
def test_collision_preserves_exact_original_and_installs_verified_replacement(
    tmp_path: Path, original: bytes,
) -> None:
    staged, final, replacement = _collision_fixture(tmp_path, original=original)
    assert recovery.repair_content_addressed_collision(staged, final, tmp_path)
    assert final.read_bytes() == replacement and not staged.exists()
    evidence_dirs = list((tmp_path / "corrupt_parquet_evidence").iterdir())
    assert len(evidence_dirs) == 1
    evidence = evidence_dirs[0]
    assert (evidence / "original.parquet").read_bytes() == original
    prepared = json.loads((evidence / "prepared.json").read_text())
    installed = json.loads((evidence / "installed.json").read_text())
    assert prepared["status"] == "verified_replacement_ready"
    assert installed["status"] == "installed"
    assert installed["original_sha256"] == hashlib.sha256(original).hexdigest()
    assert installed["replacement_sha256"] == final.stem
    assert installed["original_path"] == str(final.relative_to(tmp_path))


def test_valid_content_addressed_collision_needs_no_repair(tmp_path: Path) -> None:
    staged, final, replacement = _collision_fixture(tmp_path)
    final.write_bytes(replacement)
    assert not recovery.repair_content_addressed_collision(staged, final, tmp_path)
    assert staged.read_bytes() == replacement and final.read_bytes() == replacement
    assert not (tmp_path / "corrupt_parquet_evidence").exists()


@pytest.mark.parametrize("variant", ["wrong_digest", "outside_root", "symlink", "wrong_staging"])
def test_collision_rejects_unverified_or_unsafe_targets(tmp_path: Path, variant: str) -> None:
    root = tmp_path / "workspace"
    staged, final, _ = _collision_fixture(root, original=b"retain me")
    if variant == "wrong_digest":
        staged.write_bytes(b"unexpected response")
    elif variant == "outside_root":
        root = tmp_path / "different_root"
    elif variant == "symlink":
        link = final.with_name("aliased.parquet")
        link.symlink_to(final)
        final = link
    else:
        staged = staged.rename(staged.with_name("noncanonical.parquet"))
    with pytest.raises(ValueError):
        recovery.repair_content_addressed_collision(staged, final, root)
    assert final.read_bytes() == b"retain me"
    assert staged.exists()


def test_collision_refuses_replacement_if_preservation_receipt_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged, final, replacement = _collision_fixture(tmp_path, original=b"retain me")

    def fail_receipt(*_args: object, **_kwargs: object) -> None:
        raise OSError("fixture receipt failure")

    monkeypatch.setattr(recovery, "atomic_write_json", fail_receipt)
    with pytest.raises(OSError, match="fixture receipt failure"):
        recovery.repair_content_addressed_collision(staged, final, tmp_path)
    assert final.read_bytes() == b"retain me"
    assert staged.read_bytes() == replacement
    assert next((tmp_path / "corrupt_parquet_evidence").glob("*/original.parquet")).read_bytes() == b"retain me"
