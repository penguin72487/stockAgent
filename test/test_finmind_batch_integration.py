from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import SourceError, Task
from downloader.finmind_batching import BatchContractError


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
DATASET = "TaiwanBusinessIndicator"


def _years(conn: sqlite3.Connection, *years: int) -> None:
    conn.executemany(
        "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
        "VALUES (?,'',?,'year',2,'pending')",
        [(DATASET, f"{year}-01-01") for year in years],
    )
    conn.commit()


def _runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(sponsor, "datetime", Clock)
    monkeypatch.setattr(sponsor, "load_env_file", lambda *_a, **_kw: None)
    monkeypatch.setattr(sponsor, "verified_account", lambda *_a: {
        "tier": "Sponsor", "official_requests_per_hour": 6_000,
    })
    monkeypatch.setattr(sponsor, "rate_limiter", lambda *_a: object())
    monkeypatch.setattr(sponsor, "_seed", lambda *_a, **_kw: {"state": "test_fixture"})
    monkeypatch.setattr(sponsor, "_official_price_coverage", lambda: None)
    monkeypatch.setattr(sponsor, "_official_session_calendar", lambda: None)
    monkeypatch.setattr(sponsor, "_fixed_incremental_demand", lambda *_a: 0)
    monkeypatch.setattr(sponsor, "backfill_budget", lambda *_a, **_kw: {"allowed": True})
    monkeypatch.setattr(sponsor, "protected_stock_opening", lambda *_a: False)
    monkeypatch.setattr(sponsor.shutil, "disk_usage", lambda *_a: SimpleNamespace(free=100 * 1024**3))


def test_claim_batches_only_due_contiguous_pending_neighbors(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2020, 2021, 2022, 2023, 2024, 2025)
        conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE partition='2022-01-01'",
                     ((NOW + timedelta(hours=1)).isoformat(),))
        conn.commit()
        task = sponsor._next(conn, NOW)
        assert task is not None and task.partition == "2025-01-01"
        batch = sponsor._claim_batch(conn, task, NOW)
        assert batch is not None
        assert [part.partition for part in batch.tasks] == ["2023-01-01", "2024-01-01", "2025-01-01"]
        assert conn.execute("SELECT partition FROM tasks WHERE state='inflight' ORDER BY partition").fetchall() == [
            ("2023-01-01",), ("2024-01-01",), ("2025-01-01",),
        ]


def test_claim_is_atomic_and_does_not_undo_the_existing_seed_claim(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2023, 2024, 2025)
        task = sponsor._next(conn, NOW)
        conn.execute("""
            CREATE TRIGGER reject_second_batch_neighbor BEFORE UPDATE ON tasks
            WHEN NEW.partition='2024-01-01' AND NEW.state='inflight'
            BEGIN SELECT RAISE(FAIL, 'fixture reject'); END
        """)
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="fixture reject"):
            sponsor._claim_batch(conn, task, NOW)
        assert conn.execute("SELECT partition,state FROM tasks ORDER BY partition").fetchall() == [
            ("2023-01-01", "pending"), ("2024-01-01", "pending"), ("2025-01-01", "inflight"),
        ]


def test_claim_preserves_callers_transaction_boundary(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2024, 2025)
        task = sponsor._next(conn, NOW)
        conn.execute("BEGIN")
        assert sponsor._claim_batch(conn, task, NOW) is not None
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT state FROM tasks WHERE partition='2024-01-01'").fetchone() == ("pending",)


def test_claim_maximum_span_crosses_training_horizon_priority_not_completed_holes(tmp_path: Path):
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, *range(1982, 2026))
        conn.execute("UPDATE tasks SET priority=4 WHERE partition<'2014-01-01'")
        conn.commit()
        batch = sponsor._claim_batch(conn, sponsor._next(conn, NOW), NOW)
        assert batch and len(batch.tasks) == 44
        assert batch.start_date.isoformat() == '1982-01-01'
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='inflight'").fetchone()[0] == 44


@pytest.mark.parametrize('error', ['response_size_limit','batch_response_row_limit','ReadTimeout','http_504'])
def test_resource_failure_learns_smaller_batch_and_preserves_acquisition(tmp_path: Path, error: str):
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, *range(1982, 2026))
        batch = sponsor._claim_batch(conn, sponsor._next(conn, NOW), NOW)
        assert len(batch.tasks) == 44
        result = sponsor._defer_failed_batch(conn, batch, error, NOW)
        assert result == 'batch_reduced_retry'
        assert conn.execute('SELECT count(*) FROM request_batch_policy').fetchone()[0] == 0
        assert conn.execute('SELECT max_partitions FROM request_batch_limits').fetchone()[0] == 22
        retry_at = NOW + timedelta(seconds=61)
        smaller = sponsor._claim_batch(conn, sponsor._next(conn, retry_at), retry_at)
        assert len(smaller.tasks) == 22
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='pending'").fetchone()[0] == 22


def test_reserved_incremental_lane_does_not_expand_into_history(tmp_path: Path, monkeypatch):
    _runtime(monkeypatch)
    monkeypatch.setattr(sponsor, 'backfill_budget', lambda *_a, **_kw: {'allowed': False, 'basis': 'reserve'})
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, 2024, 2025, 2026)
        conn.execute("UPDATE tasks SET priority=0 WHERE partition='2026-01-01'")
    calls = []
    monkeypatch.setattr(sponsor, '_fetch_rows', lambda *_a, **_kw: calls.append(_a[-1]) or [{'date':'2026-01-01'}])
    sponsor.run_once(tmp_path, workers=1, max_requests=1)
    assert len(calls) == 1 and calls[0]['start_date'] == '2026-01-01'
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='pending'").fetchone()[0] == 2


def test_rejected_batch_disables_only_batching_and_preserves_exact_audit(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2020, 2023, 2024, 2025)
        task = sponsor._next(conn, NOW)
        batch = sponsor._claim_batch(conn, task, NOW)
        assert batch is not None
        sponsor._defer_failed_batch(conn, batch, "response_outside_batch", NOW)
        states = conn.execute("SELECT partition,state,next_attempt_at_utc FROM tasks ORDER BY partition").fetchall()
        assert states[0] == ("2020-01-01", "pending", None)
        assert all(state == "pending" and after == (NOW + timedelta(seconds=60)).isoformat()
                   for _, state, after in states[1:])
        audit = conn.execute("SELECT request_metadata_json,prior_tasks_json FROM request_batch_failure_audit").fetchone()
        assert json.loads(audit[0])["partitions"] == ["2023-01-01", "2024-01-01", "2025-01-01"]
        assert {item["state"] for item in json.loads(audit[1])} == {"inflight"}
        # A future run uses normal single-date requests, not the same bad batch.
        next_task = sponsor._next(conn, NOW + timedelta(seconds=61))
        assert next_task is not None and next_task.partition == "2025-01-01"
        assert sponsor._claim_batch(conn, next_task, NOW + timedelta(seconds=61)) is None
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT error_code FROM request_batch_policy WHERE dataset=?", (DATASET,)).fetchone() == (
            "response_outside_batch",
        )


def test_fetch_batch_uses_exactly_one_shared_request_and_validates_before_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a, **_kw: calls.append(_a[-1]) or [
        {"date": "2024-02-01", "value": 3}, {"date": "2025-12-01", "value": -1},
    ])
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2024, 2025)
        batch = sponsor._claim_batch(conn, sponsor._next(conn, NOW), NOW)
        result = sponsor._fetch_batch(batch, "not-a-real-token", object(), tmp_path)
        assert len(calls) == 1
        assert calls[0] == {"dataset": DATASET, "start_date": "2024-01-01", "end_date": "2025-12-31"}
        assert [len(rows) for rows in result.values()] == [1, 1]
        assert not (tmp_path / "receipts").exists()
        monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a, **_kw: [{"date": "2026-01-01"}])
        with pytest.raises(BatchContractError, match="response_outside_batch"):
            sponsor._fetch_batch(batch, "not-a-real-token", object(), tmp_path)
        assert not (tmp_path / "receipts").exists()


def test_run_once_one_request_finishes_three_receipt_partitions_and_flattened_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime(monkeypatch)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2023, 2024, 2025)
    calls = []
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a, **_kw: calls.append(_a[-1]) or [
        {"date": "2023-01-01", "value": 10}, {"date": "2025-12-01", "value": 20},
    ])
    original_wait = sponsor.wait
    observations = []

    def first_wait(futures, **kwargs):
        if not observations:
            observations.append(True)
            return set(), set(futures)
        return original_wait(futures, **kwargs)

    monkeypatch.setattr(sponsor, "wait", first_wait)
    original_status = sponsor._status
    active = []

    def status(conn, root, state, **kwargs):
        active.extend(kwargs.get("active", []))
        return original_status(conn, root, state, **kwargs)

    monkeypatch.setattr(sponsor, "_status", status)
    result = sponsor.run_once(tmp_path, workers=1, max_requests=1)
    assert len(calls) == 1 and result["state"] == "batch_complete"
    assert {task.partition for task in active} == {"2023-01-01", "2024-01-01", "2025-01-01"}
    assert all(isinstance(task, Task) for task in active)
    assert result["last_task"]["request_batch"]["partition_count"] == 3
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        records = conn.execute("SELECT partition,state,receipt_path FROM tasks ORDER BY partition").fetchall()
    assert [state for _, state, _ in records] == ["complete", "observed_empty", "complete"]
    for partition, state, path in records:
        receipt = json.loads((tmp_path / path).read_text())
        assert receipt["partition"] == partition and receipt["status"] == state
        assert receipt["request"]["request_start_date"] == "2023-01-01"
        assert receipt["request"]["request_end_date"] == "2025-12-31"


@pytest.mark.parametrize("failure", ["bad_date", "provider_bad_request"])
def test_run_once_bad_batch_is_not_written_reprobed_or_dataset_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    _runtime(monkeypatch)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2023, 2024, 2025)
    calls = []

    def response(*args, **kwargs):
        calls.append(args[-1])
        if failure == "provider_bad_request":
            raise SourceError("provider_bad_request", retry_after=0)
        return [{"date": "2023-01-01", "value": 1}, {"date": "2026-01-01", "value": 2}]

    monkeypatch.setattr(sponsor, "_fetch_rows", response)
    result = sponsor.run_once(tmp_path, workers=1, max_requests=1)
    assert len(calls) == 1
    assert result["last_task"]["status"] == "batch_disabled_single_retry"
    assert not (tmp_path / "receipts").exists()
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='pending'").fetchone() == (3,)
        assert conn.execute("SELECT count(*) FROM request_batch_failure_audit").fetchone() == (1,)


def test_batch_storage_failure_does_not_revert_already_persisted_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime(monkeypatch)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2023, 2024, 2025)
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a, **_kw: [
        {"date": "2023-01-01", "value": 1}, {"date": "2024-01-01", "value": 2},
    ])
    original_finish = sponsor._finish

    def finish(conn, root, task, rows, now, **kwargs):
        if task.partition == "2024-01-01":
            raise OSError("fixture disk failure")
        return original_finish(conn, root, task, rows, now, **kwargs)

    monkeypatch.setattr(sponsor, "_finish", finish)
    sponsor.run_once(tmp_path, workers=1, max_requests=1)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT partition,state FROM tasks ORDER BY partition").fetchall() == [
            ("2023-01-01", "complete"), ("2024-01-01", "failed"), ("2025-01-01", "failed"),
        ]


def test_shared_rate_limit_remains_global_and_every_batch_member_leaves_inflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime(monkeypatch)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, 2023, 2024, 2025)

    def rate_limited(*_args, **_kwargs):
        raise SourceError("rate_limited", retry_after=3600)

    monkeypatch.setattr(sponsor, "_fetch_rows", rate_limited)
    result = sponsor.run_once(tmp_path, workers=1, max_requests=1)
    assert result["state"] == "rate_limited"
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='failed'").fetchone() == (3,)
        assert conn.execute("SELECT count(*) FROM request_batch_policy").fetchone() == (0,)


def test_run_once_parent_recovery_does_not_overwrite_event_driven_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime(monkeypatch)
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        conn.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,'','2023-08-21',?,2,?)",
            [(sponsor.LONG_INSTITUTIONAL, "day", "complete"),
             (sponsor.WIDE_INSTITUTIONAL, "derived", "pending")],
        )
        conn.commit()

    def broken_derived(*_args):
        raise SourceError("corrupt_long_parquet", retry_after=0)

    monkeypatch.setattr(sponsor, "_fetch", broken_derived)
    monkeypatch.setattr(sponsor, "_fail", lambda *_a: pytest.fail("_fail overwrote recovered wait"))
    recovered = []

    def recovery(conn, root, task, error, now):
        recovered.append(task)
        # Controlled stand-in for the helper's independently tested disk proof.
        conn.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=? WHERE dataset=?",
                     ((now + timedelta(hours=1)).isoformat(), task.dataset))
        conn.commit()
        return {"status": "waiting_long_parent", "audit_id": 1}

    monkeypatch.setattr(sponsor, "recover_failed_long_parent", recovery)
    result = sponsor.run_once(tmp_path, workers=1, max_requests=1)
    assert len(recovered) == 1
    assert result["last_task"]["status"] == "waiting_long_parent"
    assert result["last_task"]["parent_recovery"]["audit_id"] == 1
