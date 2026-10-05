from datetime import UTC, datetime
import hashlib
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import artifact_io
from downloader import download_finmind_complement as complement
from downloader.finmind_integrity import audit_queue, audit_completed_batch
from downloader.parquet_integrity import parquet_receipt_error
from scripts import download_finlab_history as finlab

NOW = datetime(2026, 9, 28, 10, tzinfo=UTC)


def seed(root, conn, partition="history"):
    task = complement.Task("UKStockPrice", "TEST.L", partition, "id_history", 2, "pending")
    conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,?,?,?,?,?)",
                 tuple(getattr(task, key) for key in ("dataset", "data_id", "partition", "kind", "priority", "state")))
    receipt = complement._store(root, task, [{"date": "2026-09-24", "stock_id": "TEST.L", "close": 100.}], NOW)
    complement._save_result(conn, task, receipt, NOW)
    return task, receipt, root / receipt["parquet_path"]


@pytest.mark.parametrize("damage", ["empty", "missing", "hash", "rows", "receipt", "unsafe"])
def test_proven_corruption_requeues_only_target_and_keeps_original_evidence(tmp_path, damage):
    with complement._db(tmp_path / "queue.sqlite3") as conn:
        task, receipt, source = seed(tmp_path, conn)
        _, other, _ = seed(tmp_path, conn, "other")
        receipt_path = tmp_path / receipt["receipt_path"]
        if damage == "empty":
            source.write_bytes(b"")
        elif damage == "missing":
            source.unlink()
        elif damage == "hash":
            receipt["sha256"] = "0" * 64
        elif damage == "rows":
            receipt["rows"] = 9
        elif damage == "receipt":
            receipt["partition"] = "wrong"
        elif damage == "unsafe":
            receipt["parquet_path"] = "../outside.parquet"
        if damage not in {"empty", "missing"}:
            receipt_path.write_text(json.dumps(receipt))
        raw = receipt_path.read_bytes()
        dry = audit_queue(conn, tmp_path)
        assert dry["failed"] == 1 and dry["requeued"] == 0
        repaired = audit_queue(conn, tmp_path, repair=True)
        assert repaired["checked"] == 2 and repaired["requeued"] == 1
        assert receipt_path.read_bytes() == raw
        assert conn.execute("SELECT state FROM tasks WHERE partition='history'").fetchone()[0] == "pending"
        assert conn.execute("SELECT priority FROM tasks WHERE partition='history'").fetchone()[0] <= 1
        assert conn.execute("SELECT state FROM tasks WHERE partition='other'").fetchone()[0] == "complete"
        audit = conn.execute("SELECT receipt_bytes,prior_task_json FROM local_integrity_audit").fetchone()
        assert audit[0] == raw and json.loads(audit[1])["state"] == "complete"
        assert audit_queue(conn, tmp_path, repair=True)["failed"] == 0
        assert complement._next_task(conn, NOW).partition == "history"


def test_same_size_change_invalidates_hash_cache(tmp_path):
    with complement._db(tmp_path / "queue.sqlite3") as conn:
        _, receipt, source = seed(tmp_path, conn)
        assert parquet_receipt_error(tmp_path, receipt) is None
        raw = bytearray(source.read_bytes())
        raw[16] ^= 1
        source.write_bytes(raw)
        assert parquet_receipt_error(tmp_path, receipt) == "parquet_hash_mismatch"


def test_integrity_repair_promotes_background_priority_but_preserves_live_zero(tmp_path):
    with complement._db(tmp_path / 'queue.sqlite3') as conn:
        _, _, source = seed(tmp_path, conn, 'background')
        conn.execute("UPDATE tasks SET priority=80 WHERE partition='background'")
        source.write_bytes(b'')
        assert audit_queue(conn, tmp_path, repair=True)['requeued'] == 1
        assert conn.execute("SELECT priority FROM tasks WHERE partition='background'").fetchone()[0] == 1


def test_rotating_cursor_reaches_later_files_and_wraps(tmp_path):
    with complement._db(tmp_path / "queue.sqlite3") as conn:
        seed(tmp_path, conn, "first")
        _, _, source = seed(tmp_path, conn, "second")
        source.write_bytes(b"")
        one = audit_completed_batch(conn, tmp_path, limit=1)
        two = audit_completed_batch(conn, tmp_path, limit=1)
        end = audit_completed_batch(conn, tmp_path, limit=1)
        assert one["checked"] == 1 and one["requeued"] == 0
        assert two["requeued"] == 1
        assert end["cursor"] == 0 and end["completed_sweeps"] == 1
        assert end["total_requeued"] == 1


def test_local_audit_never_treats_observed_empty_as_broken_file(tmp_path):
    with complement._db(tmp_path / "queue.sqlite3") as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows) "
                     "VALUES ('UKStockPrice','EMPTY','history','id_history',2,'observed_empty',0)")
        assert audit_queue(conn, tmp_path, repair=True)["checked"] == 0


def test_late_fields_and_legitimate_nulls_survive_store(tmp_path):
    task = complement.Task("UKStockPrice", "TEST.L", "history", "id_history", 2, "pending")
    receipt = complement._store(tmp_path, task, [
        {"date": "2026-09-23", "close": 1., "optional": None},
        {"date": "2026-09-24", "close": 2., "optional": None, "new_field": 0.},
    ], NOW)
    table = pq.read_table(tmp_path / receipt["parquet_path"])
    assert table["new_field"].to_pylist() == [None, 0.]
    assert receipt["all_null_fields"] == ["optional"]
    assert receipt["field_non_null_counts"]["new_field"] == 1
    assert receipt["storage_contract_version"] == 2


def test_all_null_response_never_publishes_complete_receipt(tmp_path):
    task = complement.Task("UKStockPrice", "TEST.L", "history", "id_history", 2, "pending")
    with pytest.raises(complement.SourceError, match="all_null_response"):
        complement._store(tmp_path, task, [{"close": None}], NOW)
    assert not list(tmp_path.rglob("*.json"))


def test_durable_data_publish_syncs_before_rename_and_directory_after(tmp_path, monkeypatch):
    source, target = tmp_path / "stage", tmp_path / "final"
    source.write_bytes(b"data")
    events = []
    original = artifact_io.os.replace
    monkeypatch.setattr(artifact_io.os, "fsync", lambda fd: events.append("fsync"))
    monkeypatch.setattr(artifact_io.os, "replace", lambda a, b: (events.append("rename"), original(a, b)))
    artifact_io.durable_replace(source, target)
    assert events == ["fsync", "rename", "fsync"]
    assert target.read_bytes() == b"data"


def test_finlab_stale_receipt_does_not_hide_corruption(tmp_path, monkeypatch):
    from finlab import data
    monkeypatch.setattr(data, "get", lambda *a, **kw: pd.DataFrame({"2330": [0.], "2317": [None]}))
    key = "test:value"
    receipt = finlab.fetch_one(key, tmp_path)
    assert receipt["all_null_source_columns"] == ["2317"]
    assert finlab.has_local_download(key, tmp_path)
    (tmp_path / receipt["parquet_path"]).write_bytes(b"")
    assert not finlab.has_local_download(key, tmp_path)
    repaired = finlab.fetch_one(key, tmp_path)
    assert repaired["sha256"] == receipt["sha256"]
    assert finlab.has_local_download(key, tmp_path)
    preserved = list((tmp_path / "corrupt_parquet_evidence").glob("*/original.parquet"))
    assert len(preserved) == 1 and preserved[0].read_bytes() == b""


def test_duplicate_finlab_labels_fail_instead_of_merging_dimensions():
    with pytest.raises(ValueError, match="duplicate"):
        finlab.serialize_provider_frame(pd.DataFrame([[1, 2]], columns=[2330, "2330"]))
