from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace
import weakref

import pyarrow as pa
import pytest

from downloader import download_finmind_complement as c


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)
DATASET = "TaiwanStockDelisting"


def _seed_last_good(connection, root):
    for year in (2024, 2025):
        task = c.Task(DATASET, "", str(year), "year", 1, "pending")
        c._add_tasks(connection, [(task.dataset, task.data_id, task.partition, task.kind, task.priority)])
        receipt = c._store(root, task, [{"date": f"{year}-01-01", "value": 100}], NOW - timedelta(days=100))
        c._save_result(connection, task, receipt, NOW - timedelta(days=100))


def _canonical_bytes(root):
    return {str(path.relative_to(root)): path.read_bytes()
            for directory in ("receipts", "receipt_history", "parquet")
            for path in (root / directory).rglob("*") if path.is_file()}


@pytest.mark.parametrize("later_rows", [
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": "bad"}],
    [{"date": "2025-01-01", "value": 1}, {"date": "2025-01-02", "value": 2, "late_only": 9}],
    [{"date": "2025-01-01", "value": 1, "new_year_only": 9}],
    [{"date": "2025-01-01"}],
    [{"date": "2025-01-01", "value": {}}],
    [{"date": "2025-01-01", "value": {"nested": 1}}],
    [{"date": "2025-01-01", "value": [1, 2]}],
])
def test_later_year_invalid_schema_keeps_every_head_and_queue_row_unchanged(tmp_path, later_rows):
    with c._db(tmp_path / "queue.sqlite3") as connection:
        _seed_last_good(connection, tmp_path)
        task = c.Task(DATASET, "", "2025", "year", 1, "complete")
        batch = c._claim_bulk_years(connection, task, NOW, allow_history=True)
        assert batch is not None
        before_tasks = connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall()
        before_claims = connection.execute("SELECT * FROM complement_year_batch_claims ORDER BY partition").fetchall()
        before_files = _canonical_bytes(tmp_path)
        with pytest.raises(c.SourceError, match="^invalid_bulk_schema$"):
            c._store_bulk_years(connection, tmp_path, task,
                               [{"date": "2024-01-01", "value": 200}, *later_rows],
                               NOW, NOW.date(), batch=batch)
        assert connection.execute("SELECT * FROM tasks ORDER BY partition").fetchall() == before_tasks
        assert connection.execute("SELECT * FROM complement_year_batch_claims ORDER BY partition").fetchall() == before_claims
        assert _canonical_bytes(tmp_path) == before_files


def test_schema_failure_is_audited_and_does_not_demote_last_good_to_empty(tmp_path):
    with c._db(tmp_path / "queue.sqlite3") as connection:
        _seed_last_good(connection, tmp_path)
        task = c.Task(DATASET, "", "2025", "year", 1, "complete")
        batch = c._claim_bulk_years(connection, task, NOW, allow_history=True)
        before_files = _canonical_bytes(tmp_path)
        with pytest.raises(c.SourceError) as caught:
            c._preflight_bulk_year_schemas(DATASET, {
                "2024": [{"date": "2024-01-01", "value": 200}],
                "2025": [{"date": "2025-01-01", "value": "bad"},
                         {"date": "2025-01-02", "value": 1}],
            })
        c._fail_bulk_years(connection, batch, caught.value, NOW)
        c._release_bulk_years(connection, batch)
        assert _canonical_bytes(tmp_path) == before_files
        assert connection.execute("SELECT state,rows FROM tasks ORDER BY partition").fetchall() == [
            ("complete", 1), ("complete", 1)]
        audit = connection.execute(
            "SELECT error_code,request_metadata_json FROM complement_year_batch_failures"
        ).fetchone()
        assert audit[0] == "invalid_bulk_schema"
        assert json.loads(audit[1])["partitions"] == ["2024", "2025"]
        assert connection.execute("SELECT max_partitions FROM complement_year_batch_policy").fetchone() == (1,)


def test_preflight_reuses_annotation_and_releases_each_temporary_table(monkeypatch):
    references = []
    annotated_years = []

    class TemporaryTable:
        schema = pa.schema([("date", pa.string()), ("value", pa.int64())])

    def annotate(dataset, rows):
        assert dataset == DATASET
        assert all(reference() is None for reference in references)
        annotated_years.append(rows[0]["date"][:4])
        return [{**row, "canonical_value": 3} for row in rows], {}

    def from_pylist(rows):
        assert rows[0]["canonical_value"] == 3
        assert all(reference() is None for reference in references)
        table = TemporaryTable()
        references.append(weakref.ref(table))
        return table

    monkeypatch.setattr(c, "annotate_stock_share_units", annotate)
    monkeypatch.setattr(c, "pa", SimpleNamespace(
        Table=SimpleNamespace(from_pylist=from_pylist), ArrowException=pa.ArrowException,
        types=pa.types,
    ))
    grouped = {"2023": [], "2024": [{"date": "2024-01-01", "value": None}],
               "2025": [{"value": 1, "date": "2025-01-01"}]}
    c._preflight_bulk_year_schemas(DATASET, grouped)
    assert annotated_years == ["2024", "2025"]
    assert all(reference() is None for reference in references)
    assert "canonical_value" not in grouped["2024"][0]


def test_null_values_and_reordered_keys_do_not_change_provider_field_contract():
    c._preflight_bulk_year_schemas(DATASET, {
        "2024": [{"date": "2024-01-01", "value": None},
                 {"value": 1.25, "date": "2024-01-02"}],
        "2025": [{"value": 2, "date": "2025-01-01"}],
    })


def test_arrow_first_row_inference_is_not_provider_schema_validation():
    rows = [{"date": "2025-01-01", "value": 1},
            {"date": "2025-01-02", "value": 2, "late_only": 9}]
    assert "late_only" not in pa.Table.from_pylist(rows).column_names
    with pytest.raises(c.SourceError, match="^invalid_bulk_schema$"):
        c._preflight_bulk_year_schemas(DATASET, {"2025": rows})
