from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from downloader import download_finmind_complement as c


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
DATASET = "TaiwanStockDelisting"


def _years(conn, years, *, state="pending", rows=0, due=True):
    for year in years:
        conn.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows,next_attempt_at_utc) "
            "VALUES (?,'',?,'year',?,?,?,?)",
            (DATASET, str(year), 0 if year == NOW.year else 1 if year >= 2014 else 3,
             state, rows, (NOW - timedelta(seconds=1) if due else NOW + timedelta(days=1)).isoformat()),
        )
    conn.commit()


def _task(conn, year):
    return c.Task(*conn.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks WHERE dataset=? AND partition=?",
        (DATASET, str(year)),
    ).fetchone())


def _claim(conn, year):
    return c._claim_bulk_years(conn, _task(conn, year), NOW, allow_history=True)


def _runtime(monkeypatch):
    monkeypatch.setenv("FINMIND_TOKEN", "test-placeholder")
    monkeypatch.setattr(c, "_now", lambda: NOW)
    monkeypatch.setattr(c, "load_env_file", lambda *_a, **_kw: None)
    monkeypatch.setattr(c, "verified_account", lambda *_a: {"tier": "Free", "official_requests_per_hour": 600})
    monkeypatch.setattr(c, "rate_limiter", lambda *_a: object())
    monkeypatch.setattr(c, "_populate", lambda *_a, **_kw: None)
    monkeypatch.setattr(c, "incremental_reservation", lambda *_a: {'reserve_requests': 0})
    monkeypatch.setattr(c, "backfill_budget", lambda *_a, **_kw: {"allowed": True})
    monkeypatch.setattr(c.shutil, "disk_usage", lambda *_a: SimpleNamespace(free=100 * 1024**3))
    monkeypatch.setattr(c, "_status", lambda conn, root, **kw: kw)


def test_revision_refresh_batches_due_completed_and_empty_years_across_priority_boundary(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, range(2001, 2026), state="complete", rows=1)
        conn.execute("UPDATE tasks SET state='observed_empty',rows=0 WHERE partition='2013'")
        conn.commit()
        batch = _claim(conn, 2014)
        assert [t.partition for t in batch.tasks] == [str(y) for y in range(2001, 2026)]
        assert batch.params() == {"dataset": DATASET, "start_date": "2001-01-01", "end_date": "2025-12-31"}
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='inflight'").fetchone() == (25,)
        c._release_bulk_years(conn, batch)
        assert conn.execute("SELECT state FROM tasks WHERE partition='2013'").fetchone() == ("observed_empty",)


def test_not_due_holes_remain_byte_identical_and_do_not_enter_receipts(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2021, 2022, 2024, 2025])
        _years(conn, [2023, 2026], state="complete", rows=1, due=False)
        before = conn.execute("SELECT * FROM tasks WHERE partition IN ('2023','2026') ORDER BY partition").fetchall()
        batch = _claim(conn, 2025)
        assert [t.partition for t in batch.tasks] == ["2024", "2025"]
        c._store_bulk_years(conn, tmp_path, batch.tasks[-1], [{"date": "2025-12-31", "value": 1}],
                           NOW, NOW.date(), batch=batch)
        c._release_bulk_years(conn, batch)
        assert conn.execute("SELECT * FROM tasks WHERE partition IN ('2023','2026') ORDER BY partition").fetchall() == before
        assert conn.execute("SELECT state FROM tasks WHERE partition='2021'").fetchone() == ("pending",)
        receipts = list((tmp_path / "receipts" / DATASET / "all").glob('*.json'))
        assert {p.stem for p in receipts} == {"2024", "2025"}
        payloads = [json.loads(p.read_text()) for p in receipts]
        assert {p['request']['request_id'] for p in payloads} == {batch.request_id}
        assert all(p['request']['request_count'] == 1 for p in payloads)
        assert all(p['request']['response_partition_rows'] == {"2024": 0, "2025": 1} for p in payloads)


def test_reserve_gate_does_not_claim_history_for_current_year(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2024, 2025, 2026])
        assert c._claim_bulk_years(conn, _task(conn, 2026), NOW, allow_history=False) is None
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (3,)


def test_claim_is_atomic_and_preserves_outer_transaction(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2023, 2024, 2025])
        conn.execute("CREATE TRIGGER fail_second BEFORE UPDATE ON tasks WHEN NEW.partition='2024' "
                     "AND NEW.state='inflight' BEGIN SELECT RAISE(FAIL,'test rejected'); END")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="test rejected"):
            _claim(conn, 2025)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (3,)
        conn.execute("DROP TRIGGER fail_second")
        conn.commit()
        conn.execute("BEGIN")
        assert _claim(conn, 2025) is not None
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (3,)


def test_restart_recovers_claimed_states_but_preserves_completed_parts(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2023], state="complete", rows=1)
        _years(conn, [2024], state="observed_empty")
        _years(conn, [2025])
        batch = _claim(conn, 2025)
        receipt = c._store(tmp_path, batch.tasks[0], [{"date": "2023-01-01"}], NOW,
                           request_metadata=batch.metadata())
        c._save_result(conn, batch.tasks[0], receipt, NOW)
        complete_before = conn.execute("SELECT * FROM tasks WHERE partition='2023'").fetchone()
    with c._db(tmp_path / "queue.sqlite3") as conn:
        c._recover_bulk_year_claims(conn)
        assert conn.execute("SELECT * FROM tasks WHERE partition='2023'").fetchone() == complete_before
        assert conn.execute("SELECT partition,state FROM tasks ORDER BY partition").fetchall() == [
            ("2023", "complete"), ("2024", "observed_empty"), ("2025", "pending"),
        ]
        assert conn.execute("SELECT COUNT(*) FROM complement_year_batch_claims").fetchone() == (0,)


@pytest.mark.parametrize("stamp,error", [("2025-02-30", "invalid_bulk_date"),
                                          ("2026-01-01", "invalid_bulk_range")])
def test_entire_body_validated_before_any_receipt(tmp_path, stamp, error):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2024, 2025])
        batch = _claim(conn, 2025)
        with pytest.raises(c.SourceError, match=error):
            c._store_bulk_years(conn, tmp_path, batch.tasks[-1],
                               [{"date": "2024-01-01"}, {"date": stamp}], NOW, NOW.date(), batch=batch)
        assert not (tmp_path / "receipts").exists()


def test_empty_cannot_overwrite_previous_nonempty_even_when_claimed_or_failed(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2024], state="failed", rows=1)
        _years(conn, [2025])
        batch = _claim(conn, 2025)
        with pytest.raises(c.SourceError, match="incomplete_bulk_response"):
            c._store_bulk_years(conn, tmp_path, batch.tasks[-1], [{"date": "2025-01-01"}],
                               NOW, NOW.date(), batch=batch)
        assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize("error", ["response_size_limit", "ReadTimeout", "http_502", "http_504"])
def test_resource_failure_learns_half_size_preserves_history_and_retries_after_cooldown(tmp_path, error):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, range(2021, 2026), state="complete", rows=1)
        batch = _claim(conn, 2025)
        c._fail_bulk_years(conn, batch, c.SourceError(error, retry_after=3600), NOW)
        c._release_bulk_years(conn, batch)
        assert conn.execute("SELECT max_partitions FROM complement_year_batch_policy").fetchone() == (2,)
        assert conn.execute("SELECT COUNT(*),SUM(rows) FROM tasks WHERE state='complete'").fetchone() == (5, 5)
        assert _claim(conn, 2025) is None
        retry = c._claim_bulk_years(conn, _task(conn, 2025), NOW + timedelta(seconds=61), allow_history=True)
        assert [part.partition for part in retry.tasks] == ["2024", "2025"]
        assert conn.execute("SELECT COUNT(*) FROM complement_year_batch_failures").fetchone() == (1,)


def test_invalid_batch_falls_back_to_single_years_without_blocking_dataset(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, range(2021, 2026))
        batch = _claim(conn, 2025)
        c._fail_bulk_years(conn, batch, c.SourceError("provider_bad_request", retry_after=0), NOW)
        c._release_bulk_years(conn, batch)
        assert conn.execute("SELECT max_partitions FROM complement_year_batch_policy").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (5,)
        assert c._claim_bulk_years(conn, _task(conn, 2025), NOW + timedelta(seconds=61), allow_history=True) is None


def test_run_once_uses_one_bounded_request_for_due_revisions(tmp_path, monkeypatch):
    _runtime(monkeypatch)
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2023, 2024, 2025], state="complete", rows=1)
    calls = []

    def fetch(*args, **kwargs):
        calls.append((args[-1], kwargs))
        return [{"date": f"{year}-12-31", "value": year} for year in [2023, 2024, 2025]]

    monkeypatch.setattr(c, "_fetch_rows", fetch)
    result = c.run_once(tmp_path, max_requests=1)
    assert len(calls) == 1
    assert calls[0] == ({"dataset": DATASET, "start_date": "2023-01-01", "end_date": "2025-12-31"},
                         {"max_response_bytes": c.BULK_MAX_RESPONSE_BYTES})
    assert result['last']['request_batch']['partition_count'] == 3
    with c._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='complete'").fetchone() == (3,)
        assert conn.execute("SELECT COUNT(*) FROM complement_year_batch_claims").fetchone() == (0,)


def test_partial_storage_failure_keeps_finished_receipt_and_retries_only_unfinished(tmp_path, monkeypatch):
    _runtime(monkeypatch)
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2023, 2024, 2025])
    monkeypatch.setattr(c, "_fetch_rows", lambda *_a, **_kw: [
        {"date": "2023-01-01"}, {"date": "2024-01-01"}, {"date": "2025-01-01"},
    ])
    original = c._store

    def store(root, task, *args, **kwargs):
        if task.partition == "2024":
            raise OSError("test storage failure")
        return original(root, task, *args, **kwargs)

    monkeypatch.setattr(c, "_store", store)
    c.run_once(tmp_path, max_requests=1)
    with c._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT partition,state FROM tasks ORDER BY partition").fetchall() == [
            ("2023", "complete"), ("2024", "failed"), ("2025", "failed"),
        ]


def test_quota_denial_runs_only_current_year_and_preserves_old_tasks(tmp_path, monkeypatch):
    _runtime(monkeypatch)
    monkeypatch.setattr(c, "backfill_budget", lambda *_a, **_kw: {"allowed": False})
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2024, 2025, 2026])
    calls = []
    monkeypatch.setattr(c, "_request", lambda *_a, **_kw: calls.append(_a[3].partition) or [])
    c.run_once(tmp_path, max_requests=1)
    assert calls == ["2026"]
    with c._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (2,)


def test_filtered_complement_dispatch_preserves_unselected_higher_priority_tasks(tmp_path, monkeypatch):
    _runtime(monkeypatch)
    with c._db(tmp_path / "queue.sqlite3") as conn:
        _years(conn, [2024, 2025])
        c._add_tasks(conn, [("TaiwanStockInfo", "", "latest", "snapshot", 0)])
        before = conn.execute("SELECT * FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone()
    calls = []
    monkeypatch.setattr(c, "_fetch_rows", lambda *_a, **_kw: calls.append(_a[-1]) or [])
    result = c.run_once(tmp_path, max_requests=1, datasets=(DATASET,))
    assert [call['dataset'] for call in calls] == [DATASET]
    assert result['last']['dataset'] == DATASET
    with c._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT * FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone() == before
        assert c._next_task(conn, NOW, datasets=("x') OR 1=1 --",)) is None
        assert c._next_task(conn, NOW, datasets=()) is None
        assert c._next_task(conn, NOW).dataset == "TaiwanStockInfo"


def test_filter_unknown_rejected_before_files_and_cli_passes_repeated_selection(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(c, "verified_account", lambda *_a: pytest.fail("network reached"))
    for selection in [(), ("unknown",)]:
        with pytest.raises(ValueError, match="existing Complement sources"):
            c.run_once(tmp_path / 'absent', datasets=selection)
    assert not (tmp_path / 'absent').exists()
    calls = []
    monkeypatch.setattr(c, "run_once", lambda root, **kwargs: calls.append(kwargs) or {"state": "batch_complete"})
    assert c.main(["--root", str(tmp_path), "--dataset", DATASET, "--dataset", "GoldPrice"]) == 0
    assert calls[0]['datasets'] == (DATASET, "GoldPrice")
    with pytest.raises(SystemExit) as error:
        c.main(["--root", str(tmp_path / 'absent'), "--dataset", "unknown"])
    assert error.value.code == 2 and len(calls) == 1
    assert "invalid choice" in capsys.readouterr().err
    assert not (tmp_path / 'absent').exists()
