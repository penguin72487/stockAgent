"""Physical release reservation indexes cannot change quota or task semantics."""
from datetime import UTC, datetime, timedelta
import json
import sqlite3

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_retry_cohorts as cohorts
from scripts.benchmark_finmind_reservation import (
    RELEASE_RESERVE_COLUMNS, RELEASE_RESERVE_INDEX, RELEASE_RESERVE_PREDICATE,
    ensure_release_reserve_index, release_reserve_index, main,
)
from downloader.finmind_runtime import optimize_queue
from downloader.finmind_scheduling import incremental_reservation

NOW = datetime(2026, 10, 14, 9, 10, tzinfo=UTC)


def free_current(root):
    (root / "calendar.json").write_text(json.dumps({"observed_at_utc": NOW.isoformat(), "dates": []}))
    folder = root / "receipts/TaiwanStockInfoWithWarrant"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "2026-10-14.json").write_text(json.dumps({"status": "complete"}))


def add(conn, identifier, state="pending", *, dataset="TaiwanStockNews", priority=0,
        kind="day", due=None, rows=0, error=None):
    conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc,rows,error_code) "
                 "VALUES (?,?,?,?,?,?,?,?,?)", (dataset, identifier, "2026-10-14", kind,
                                                priority, state, due, rows, error))


def test_candidate_index_idempotent_exact_partial_membership_no_counters(tmp_path):
    with worker._db(tmp_path / "queue.sqlite3") as conn:
        ensure_release_reserve_index(conn)
        add(conn, "release", rows=2**52)
        add(conn, "history", priority=1)
        add(conn, "derived", kind="derived")
        conn.commit()
        before = conn.execute("SELECT * FROM tasks ORDER BY data_id").fetchall()
        changes = conn.total_changes
        ensure_release_reserve_index(conn)
        ensure_release_reserve_index(conn)
        assert conn.total_changes == changes
        assert conn.execute("SELECT * FROM tasks ORDER BY data_id").fetchall() == before
        assert tuple(row[2] for row in conn.execute(f"PRAGMA index_info({RELEASE_RESERVE_INDEX})")) == RELEASE_RESERVE_COLUMNS
        assert release_reserve_index(conn) == f" INDEXED BY {RELEASE_RESERVE_INDEX}"
        assert not conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
        assert conn.execute(f"SELECT count(*) FROM tasks INDEXED BY {RELEASE_RESERVE_INDEX} "
                            f"WHERE {RELEASE_RESERVE_PREDICATE}").fetchone() == (1,)
        conn.execute("UPDATE tasks SET priority=0 WHERE data_id='history'")
        conn.execute("UPDATE tasks SET kind='day' WHERE data_id='derived'")
        assert conn.execute(f"SELECT count(*) FROM tasks INDEXED BY {RELEASE_RESERVE_INDEX} "
                            f"WHERE {RELEASE_RESERVE_PREDICATE}").fetchone() == (3,)
        conn.rollback()
        assert conn.execute("SELECT * FROM tasks ORDER BY data_id").fetchall() == before
        assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_same_name_wrong_definition_is_not_forced_or_rewritten(tmp_path):
    with worker._db(tmp_path / "queue.sqlite3") as conn:
        ensure_release_reserve_index(conn)
        conn.execute(f"DROP INDEX IF EXISTS {RELEASE_RESERVE_INDEX}")
        conn.execute(f"CREATE INDEX {RELEASE_RESERVE_INDEX} ON tasks(dataset)")
        before = conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (RELEASE_RESERVE_INDEX,)).fetchone()
        assert release_reserve_index(conn) == ""
        assert conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (RELEASE_RESERVE_INDEX,)).fetchone() == before


@pytest.mark.parametrize("offset", (0, 39, 50, 60, 120))
def test_full_reservation_exact_clock_states_due_cohort_and_delegation(tmp_path, offset):
    free_current(tmp_path)
    root = tmp_path / "complement"
    with worker._db(root / "queue.sqlite3") as conn:
        for number, state in enumerate(("pending", "complete", "observed_empty", "failed", "inflight",
                                        "invalid_request", "not_entitled", "retry_exhausted", "identifier_alias")):
            add(conn, f"state-{number}", state, due=(NOW + timedelta(minutes=offset)).isoformat())
        add(conn, "pending-null")
        add(conn, "complete-null", "complete")
        add(conn, "history", priority=4)
        add(conn, "derived", kind="derived")
        add(conn, "past", "failed", due=(NOW - timedelta(days=1)).isoformat(), rows=-1)
        for identifier in ("canary", "deferred", "healthy"):
            add(conn, identifier, "complete" if identifier == "healthy" else "failed",
                dataset="EuropeStockPrice", kind="id_history", rows=2**40,
                error=cohorts.ERROR, due=NOW.isoformat())
        conn.execute("INSERT INTO finmind_retry_cohorts(dataset,active,canary_id,next_probe_at_utc) VALUES (?,?,?,?)",
                     ("EuropeStockPrice", 1, "canary", (NOW + timedelta(minutes=20)).isoformat()))
        conn.commit()
        original = conn.execute("SELECT * FROM tasks ORDER BY dataset,data_id").fetchall()
        conn.execute(f"DROP INDEX IF EXISTS {RELEASE_RESERVE_INDEX}")
        before = incremental_reservation(tmp_path, NOW, sources=())
        ensure_release_reserve_index(conn)
        optimize_queue(conn)
        conn.commit()
        after = incremental_reservation(tmp_path, NOW, sources=())
        assert before == after
        assert not after["queue_errors"]
        assert conn.execute("SELECT * FROM tasks ORDER BY dataset,data_id").fetchall() == original
        # Exact boundary is not admitted into this hour, nor are derived/history,
        # terminal failures, peers in the cohort or completed jobs without a due.
        news = [event for event in after["events"] if event["dataset"] == "TaiwanStockNews"]
        assert sum(event["requests"] for event in news) == (6 if offset < 50 else 2)
        europe = [event for event in after["events"] if event["dataset"] == "EuropeStockPrice"]
        assert sum(event["requests"] for event in europe) == 2


def test_budget_reader_does_not_create_index_or_touch_legacy_queue(tmp_path):
    free_current(tmp_path)
    root = tmp_path / "complement"
    root.mkdir()
    with sqlite3.connect(root / "queue.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(dataset,priority,kind,state,next_attempt_at_utc)")
        conn.execute("INSERT INTO tasks VALUES ('TaiwanStockNews',0,'day','pending',NULL)")
        conn.commit()
        before = conn.execute("SELECT * FROM tasks").fetchall()
        assert incremental_reservation(tmp_path, NOW, sources=())["ready_requests"] == 1
        assert not conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        assert conn.execute("SELECT * FROM tasks").fetchall() == before


def test_full_real_fixture_benchmark_retains_inputs_outputs_and_source(tmp_path, monkeypatch):
    root = tmp_path / "source"
    owner = root / "complement"
    with worker._db(owner / "queue.sqlite3") as conn:
        add(conn, "2330", "complete", dataset="TaiwanStockNews", rows=12)
        add(conn, "history", "complete", dataset="GoldPrice", priority=4, kind="year", rows=23)
        conn.commit()
        before = conn.execute("SELECT * FROM tasks ORDER BY dataset,data_id").fetchall()
        schema = conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall()
    (owner / "status.json").write_text(json.dumps({"state": "running", "last_task": {"rows": 12}}))
    monkeypatch.setattr(worker, "_snapshot_ids", lambda *_args: [])
    monkeypatch.setattr(worker, "_official_delisted_ids", lambda *_args: set())
    fixture, output = tmp_path / "fixture", tmp_path / "result.json"
    args = ["--root", str(root), "--fixture", str(fixture), "--output", str(output), "--repeats", "1", "--write-tasks", "1"]
    assert main(args) == 0
    report = json.loads(output.read_bytes())
    assert report["exact_output_parity"] and report["code_stable"]
    assert report["provider_calls"] == report["production_queue_mutations"] == 0
    assert len(report["trials"]) == 4
    assert len({row["whole_output_sha256"] for row in report["trials"]}) == 1
    assert all(row["exact_task_rows_restored"] for row in report["claim_release_trials"])
    assert (fixture / "baseline/complement/queue.sqlite3").is_file()
    assert (fixture / "candidate/complement/queue.sqlite3").is_file()
    assert len(list(fixture.glob("*-output-*.json"))) == 4
    with sqlite3.connect(owner / "queue.sqlite3") as conn:
        assert conn.execute("SELECT * FROM tasks ORDER BY dataset,data_id").fetchall() == before
        assert conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name").fetchall() == schema
    with pytest.raises(SystemExit):
        main(args)
    repeated = tmp_path / "three-plans.json"
    assert main(["--root", str(root), "--fixture", str(fixture), "--output", str(repeated),
                 "--prepared-from", str(output), "--compare-automatic", "--repeats", "1", "--write-tasks", "1"]) == 0
    reused = json.loads(repeated.read_bytes())
    assert reused["exact_output_parity"] and reused["prepared_from"] == str(output.resolve())
    assert len(reused["trials"]) == 6
    assert set(reused["control_medians"]) == {"baseline", "automatic", "candidate"}
    assert len(list(fixture.glob("*-output-*.json"))) == 10
    assert json.loads((fixture / "three-plans-context.json").read_bytes())["current_stock_ids"] == []
