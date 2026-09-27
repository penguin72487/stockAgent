from datetime import UTC, datetime

import pytest
import requests

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import SourceError


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
DATASET = "TaiwanBusinessIndicator"


@pytest.fixture(autouse=True)
def prohibit_provider_requests(monkeypatch):
    def reject(*_args, **_kwargs):
        raise AssertionError("Schema regression tests must not contact providers")
    monkeypatch.setattr(requests.Session, "request", reject)


def _seed(connection):
    connection.executemany(
        "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
        "VALUES (?,'',?,'year',2,'pending')",
        [(DATASET, f"{year}-01-01") for year in (2024, 2025)],
    )
    connection.commit()


@pytest.mark.parametrize("late_rows", [
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": "bad"}],
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": 2, "late_only": 9}],
    [{"date": "2025-01-01", "value": 1, "new_year_only": 9}],
    [{"date": "2025-01-01", "value": {}}],
])
def test_fetch_rejects_late_year_schema_before_any_store(tmp_path, monkeypatch, late_rows):
    stores = []
    monkeypatch.setattr(sponsor, "_store", lambda *args, **kwargs: stores.append(True))
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *args, **kwargs: [
        {"date": "2024-01-01", "value": 100}, *late_rows,
    ])
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        _seed(connection)
        task = sponsor._next(connection, NOW)
        batch = sponsor._claim_batch(connection, task, NOW)
        assert batch is not None
        before = connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall()
        with pytest.raises(SourceError, match="^invalid_bulk_schema$"):
            sponsor._fetch_batch(batch, "not-a-token", object(), tmp_path)
        assert connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall() == before
        assert stores == []
        assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize("rows", [
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": 2, "late_only": 9}],
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": "bad"}],
    [{"date": "2025-01-01", "value": {"nested": 1}}],
])
def test_single_year_fallback_preserves_last_good_and_never_drops_columns(tmp_path, monkeypatch, rows):
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        _seed(connection)
        task = sponsor._next(connection, NOW)
        assert task.partition == "2025-01-01"
        sponsor._finish(connection, tmp_path, task, [{"date": "2025-01-01", "value": 100}], NOW)
        connection.commit()
        before = connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall()
        before_files = {str(path.relative_to(tmp_path)): path.read_bytes()
                        for directory in ("receipts", "receipt_history", "parquet")
                        for path in (tmp_path / directory).rglob("*") if path.is_file()}
        stores = []
        monkeypatch.setattr(sponsor, "_store", lambda *args, **kwargs: stores.append(True))
        with pytest.raises(SourceError, match="^invalid_bulk_schema$"):
            sponsor._finish(connection, tmp_path, task, rows, NOW)
        assert stores == []
        assert connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall() == before
        assert {str(path.relative_to(tmp_path)): path.read_bytes()
                for directory in ("receipts", "receipt_history", "parquet")
                for path in (tmp_path / directory).rglob("*") if path.is_file()} == before_files


def test_rejected_schema_uses_existing_audited_batch_degrade(tmp_path):
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        _seed(connection)
        batch = sponsor._claim_batch(connection, sponsor._next(connection, NOW), NOW)
        assert sponsor._defer_failed_batch(connection, batch, "invalid_bulk_schema", NOW) == (
            "batch_disabled_single_retry")
        assert connection.execute("SELECT error_code FROM request_batch_policy").fetchone() == ("invalid_bulk_schema",)
        assert connection.execute("SELECT error_code FROM request_batch_failure_audit").fetchone() == ("invalid_bulk_schema",)
        assert connection.execute("SELECT DISTINCT state FROM tasks").fetchall() == [("pending",)]
