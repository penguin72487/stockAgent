from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from downloader import download_finmind_sponsor as sponsor


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
SELECTED = "TaiwanBusinessIndicator"
OTHER = "TaiwanStockPrice"


def _insert(conn, dataset, *, priority=2, state="pending", kind="year"):
    conn.execute(
        "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,'',?,?,?,?)",
        (dataset, "2025-01-01", kind, priority, state),
    )
    conn.commit()


@pytest.mark.parametrize("cursor", ["", "ZZZ"])
def test_filter_selects_only_requested_dataset_and_preserves_other_rows(tmp_path, cursor):
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _insert(conn, OTHER, priority=0)
        _insert(conn, SELECTED)
        conn.execute("CREATE TABLE dispatch_cursor (priority INTEGER PRIMARY KEY,last_dataset TEXT NOT NULL)")
        conn.execute("INSERT INTO dispatch_cursor VALUES (2,?)", (cursor,))
        conn.commit()
        before = conn.execute("SELECT * FROM tasks WHERE dataset=?", (OTHER,)).fetchone()
        task = sponsor._next(conn, NOW, datasets=(SELECTED,))
        assert task is not None and task.dataset == SELECTED
        assert conn.execute("SELECT * FROM tasks WHERE dataset=?", (OTHER,)).fetchone() == before
        assert sponsor._next(conn, NOW, datasets=(SELECTED,)) is None
        assert sponsor._next(conn, NOW).dataset == OTHER


def test_filter_keeps_incremental_reserve_and_bound_sql_parameters(tmp_path):
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _insert(conn, OTHER, priority=0)
        _insert(conn, SELECTED)
        assert sponsor._next(conn, NOW, datasets=(SELECTED,), incremental_only=True) is None
        assert sponsor._next(conn, NOW, datasets=("x') OR 1=1 --",)) is None
        assert sponsor._next(conn, NOW, datasets=()) is None
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (2,)


@pytest.mark.parametrize("state", ["pending", "failed", "blocked", "inflight"])
def test_selected_secondary_still_waits_for_unselected_necessary_work(tmp_path, state):
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _insert(conn, OTHER, priority=1, state=state)
        _insert(conn, SELECTED, priority=sponsor.SECONDARY_VALIDATION_PRIORITY)
        conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=?",
                     ((NOW + timedelta(hours=1)).isoformat(), OTHER))
        conn.commit()
        assert sponsor._next(conn, NOW, datasets=(SELECTED,),
                             secondary_admission=lambda: {"allowed": True}) is None
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset=?", (OTHER,))
        conn.commit()
        assert sponsor._next(conn, NOW, datasets=(SELECTED,),
                             secondary_admission=lambda: {"allowed": False}) is None
        assert sponsor._next(conn, NOW, datasets=(SELECTED,),
                             secondary_admission=lambda: {"allowed": True}).dataset == SELECTED


def test_derived_dependency_can_be_outside_selected_datasets(tmp_path):
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _insert(conn, sponsor.LONG_INSTITUTIONAL, priority=1, kind="day")
        _insert(conn, sponsor.WIDE_INSTITUTIONAL, kind="derived")
        selected = (sponsor.WIDE_INSTITUTIONAL,)
        assert sponsor._next(conn, NOW, datasets=selected) is None
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset=?", (sponsor.LONG_INSTITUTIONAL,))
        conn.commit()
        assert sponsor._next(conn, NOW, datasets=selected).dataset == sponsor.WIDE_INSTITUTIONAL


@pytest.mark.parametrize("datasets", [("unknown",), ()])
def test_invalid_selection_fails_before_files_or_network(tmp_path, monkeypatch, datasets):
    monkeypatch.setattr(sponsor, "verified_account", lambda *_a: pytest.fail("network reached"))
    root = tmp_path / "must_not_exist"
    with pytest.raises(ValueError, match="existing Sponsor sources"):
        sponsor.run_once(root, datasets=datasets)
    assert not root.exists()


def test_cli_repeated_selection_and_unknown_rejected_before_worker(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(sponsor, "run_once", lambda root, **kwargs:
                        calls.append((root, kwargs)) or {"state": "batch_complete"})
    assert sponsor.main(["--root", str(tmp_path), "--dataset", SELECTED, "--dataset", OTHER]) == 0
    assert calls[0][1]["datasets"] == (SELECTED, OTHER)
    with pytest.raises(SystemExit) as exc:
        sponsor.main(["--root", str(tmp_path / "unknown"), "--dataset", "unknown"])
    assert exc.value.code == 2 and len(calls) == 1
    assert not (tmp_path / "unknown").exists()
    assert "invalid choice" in capsys.readouterr().err


def test_run_once_dispatches_only_selected_tasks_with_canonical_receipts(tmp_path, monkeypatch):
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
    calls = []
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a, **_kw: calls.append(_a[-1]) or [])
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        _insert(conn, OTHER, priority=0, kind="day")
        _insert(conn, SELECTED)
        before = conn.execute("SELECT * FROM tasks WHERE dataset=?", (OTHER,)).fetchone()
    result = sponsor.run_once(tmp_path, workers=1, max_requests=1, datasets=(SELECTED,))
    assert [call["dataset"] for call in calls] == [SELECTED]
    assert result["last_task"]["dataset"] == SELECTED
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        assert conn.execute("SELECT * FROM tasks WHERE dataset=?", (OTHER,)).fetchone() == before
        state, receipt = conn.execute("SELECT state,receipt_path FROM tasks WHERE dataset=?",
                                      (SELECTED,)).fetchone()
    assert state == "observed_empty" and (tmp_path / receipt).is_file()
