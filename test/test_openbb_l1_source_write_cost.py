from __future__ import annotations

from collections import Counter
import json

import pytest

from scripts import benchmark_openbb_l1_source_write_cost as bench
from scripts.openbb_l1_source_journal import _install_journal


def database(path):
    manifest = bench.archive.Manifest(path)
    connection = manifest.connection
    connection.execute("CREATE TABLE l1_compaction_members(task_id TEXT PRIMARY KEY)")
    for n in range(8):
        key = f"task-{n}"
        connection.execute("""INSERT INTO tasks(task_id,endpoint,category,scope_key,kwargs_json,
            providers_json,output_path,status,selected_provider,attempts,rows,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key, "history", "test", str(n), json.dumps({"symbol": str(n)}), '["source"]',
             f"source/{n}.parquet", "success", "source", n, 3, "old", "old"))
        connection.execute("INSERT INTO l1_compaction_members VALUES(?)", (key,))
    connection.commit()
    _install_journal(connection)
    return connection


@pytest.mark.parametrize("covering", (False, True))
def test_replay_native_lifecycle_and_complete_journal_without_payloads(tmp_path, covering):
    connection = database(tmp_path / "manifest.sqlite3")
    try:
        if covering:
            bench.source.create_index(connection)
            connection.commit()
        records = bench.task_records(connection, 4)
        bench.reset_records(connection, records)
        observed = bench.replay(connection, records)
        assert observed["journal_rows"] == 12
        assert observed["direct_and_trigger_changes"] == 24
        assert observed["output_sha256"] == bench.json_digest(bench.selected_readback(connection, records))
        assert [row["name"] for row in observed["stages"]] == [
            "native_claim", "native_mark_executing", "native_completion_batches", "durable_wal_checkpoint"]
        assert observed["completion_batch_cap"] == bench.archive.COMPLETION_PERSISTENCE_BATCH_CAP
        assert not (tmp_path / "source").exists()
        journal = Counter(row[0] for row in connection.execute(
            f"SELECT task_id FROM {bench.JOURNAL_TABLE}"))
        assert all(journal[row["task_id"]] == 4 for row in records)
        actual = bench.selected_readback(connection, records)
        assert all(row["attempts"] == original["attempts"] + 1
                   for row, original in zip(actual, records, strict=True))
    finally:
        connection.close()


def test_same_source_task_and_journal_output_for_index_variants(tmp_path):
    proofs = []
    for variant in ("original", "covering"):
        connection = database(tmp_path / f"{variant}.sqlite3")
        try:
            if variant == "covering":
                bench.source.create_index(connection)
                connection.commit()
            records = bench.task_records(connection, 4)
            bench.reset_records(connection, records)
            result = bench.replay(connection, records)
            proofs.append(tuple(result[key] for key in ("output_sha256", "journal_multiset_sha256")))
        finally:
            connection.close()
    assert proofs[0] == proofs[1]


@pytest.mark.parametrize("count", (0, 4097, 9))
def test_never_silently_shrink_or_unbound_workload(tmp_path, count):
    connection = database(tmp_path / "manifest.sqlite3")
    try:
        with pytest.raises(ValueError, match="bounded|insufficient"):
            bench.task_records(connection, count)
    finally:
        connection.close()


def test_stratified_workload_is_not_just_first_page(tmp_path):
    connection = database(tmp_path / "manifest.sqlite3")
    try:
        assert [row["task_id"] for row in bench.task_records(connection, 4)] == [
            "task-0", "task-2", "task-4", "task-6"]
    finally:
        connection.close()


def test_missing_or_changed_journal_is_not_silently_installed(tmp_path):
    connection = database(tmp_path / "manifest.sqlite3")
    try:
        connection.execute("DROP TRIGGER l1_source_task_update")
        with pytest.raises(ValueError, match="journal required"):
            bench.verify_journal(connection)
    finally:
        connection.close()


def test_unexpected_task_mutation_is_rejected(tmp_path, monkeypatch):
    connection = database(tmp_path / "manifest.sqlite3")
    try:
        records = bench.task_records(connection, 2)
        bench.reset_records(connection, records)
        original = bench.archive.Manifest.complete

        def corrupt(self, result):
            original(self, result)
            self.connection.execute("UPDATE tasks SET rows=99 WHERE task_id=?", (result.task.task_id,))

        monkeypatch.setattr(bench.archive.Manifest, "complete", corrupt)
        with pytest.raises(RuntimeError, match="persisted tasks or mutation journal"):
            bench.replay(connection, records)
    finally:
        connection.close()


def test_private_prepare_four_workers_and_independent_durable_readback(tmp_path, monkeypatch):
    base = tmp_path / "artifacts/benchmarks"
    base.mkdir(parents=True)
    sql = base / "sql"
    sql.mkdir()
    manifest_path = sql / "manifest.sqlite3"
    connection = database(manifest_path)
    connection.execute("CREATE TABLE l1_compaction_segments(segment_id TEXT PRIMARY KEY)")
    bench.source.create_index(connection)
    connection.commit()
    connection.close()
    original_sha = bench.source.digest(manifest_path)
    source_identity = {"code": "fixed-source"}
    writer_identity = {"code": "fixed-writer"}
    (sql / "fixture.json").write_text(json.dumps({
        "contract": bench.source.CONTRACT, "manifest_sha256": original_sha,
        "implementation": source_identity}))
    monkeypatch.setattr(bench, "ROOT", tmp_path)
    monkeypatch.setattr(bench.source, "ROOT", tmp_path)
    monkeypatch.setattr(bench.source, "native_budget", lambda: {"unit": "private-test"})
    monkeypatch.setattr(bench.source, "implementation", lambda: source_identity)
    monkeypatch.setattr(bench, "identity", lambda: writer_identity)
    # This test also runs in broader pytest collections which may import torch;
    # dedicated benchmark roles keep the real fresh-process import rejection.
    monkeypatch.setattr(bench, "model_runtime_imported", lambda: False)
    root = base / "writer"
    bench.prepare(root, sql, 4)
    preparation = json.loads((root / "fixture.json").read_bytes())
    assert preparation["counts"] == {"tasks": 8, "members": 8, "segments": 0}
    assert preparation["copies"]["original"]["physical"]["freelist_count"] > 0
    for n, variant in enumerate(bench.VARIANTS, 1):
        bench.worker(root, root / f"{n:02d}-{variant}.json", variant)
    bench.aggregate(root, root / "aggregate.json")
    result = json.loads((root / "aggregate.json").read_bytes())
    assert result["state"] == "accepted_bounded_persistence_comparison"
    assert result["exact_native_output_and_journal_parity"]
    assert result["production_writes"] is False
    assert bench.source.digest(manifest_path) == original_sha
    assert all(row["journal_rows"] == 12 for row in result["trials"])
    # A substituted private path is never followed to a different writer.
    target = root / "original.sqlite3"
    retained = root / "original-retained.sqlite3"
    target.rename(retained)
    target.symlink_to(manifest_path)
    with pytest.raises(ValueError, match="inode changed"):
        bench.worker(root, root / "redirected.json", "original")
    assert not (root / "redirected.json").exists()
    assert bench.source.digest(manifest_path) == original_sha


def test_incomplete_checkpoint_is_not_accepted():
    class BusyConnection:
        def execute(self, sql):
            assert sql == "PRAGMA wal_checkpoint(TRUNCATE)"
            return self

        def fetchone(self):
            return (1, 2, 1)

    with pytest.raises(RuntimeError, match="checkpoint incomplete"):
        bench.checkpoint(BusyConnection())


@pytest.mark.parametrize("field", ("output_sha256", "journal_multiset_sha256", "workload_sha256",
                                   "state", "production_writes", "model_runtime_imported"))
def test_aggregate_requires_actual_complete_identity_and_parity(tmp_path, field):
    fixture = {"implementation": {"code": "fixed"}, "source_manifest_sha256": "source",
               "workload_sha256": "workload", "workload_records": 4, "counts": {"tasks": 8}}
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    for n, variant in enumerate(bench.VARIANTS, 1):
        row = {"contract": bench.CONTRACT, "state": "completed_trial", "variant": variant,
               **fixture, "production_writes": False, "payload_writes": False, "provider_calls": 0,
               "model_runtime_imported": False, "output_sha256": "output", "journal_rows": 12,
               "journal_multiset_sha256": "journal", "direct_and_trigger_changes": 24,
               "persistence_wall_seconds": 2, "persistence_cpu_seconds": 1, "workflow_wall_seconds": 3}
        if n == 3:
            row[field] = "changed"
        (tmp_path / f"{n:02d}-{variant}.json").write_text(json.dumps(row))
    with pytest.raises(ValueError, match="ABBA receipts|parity failed"):
        bench.aggregate(tmp_path, tmp_path / "aggregate.json")
    assert not (tmp_path / "aggregate.json").exists()
