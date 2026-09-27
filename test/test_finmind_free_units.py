from datetime import UTC, date, datetime
import fcntl
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_free as worker
from downloader.finmind_volume_units import (
    ORDER_BOOK_CANONICAL_FIELDS, ORDER_BOOK_DATASET, annotate_stock_share_units,
)


DAY = date(2010, 1, 4)
NOW = datetime(2026, 9, 26, 8, tzinfo=UTC)


def _rows(day=DAY):
    return [{
        "Time": f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:00",
        "date": str(day), "TotalBuyOrder": 7, "TotalSellOrder": 8, "TotalDealOrder": 3,
        "TotalBuyVolume": 200, "TotalSellVolume": 150, "TotalDealVolume": 12,
        "TotalDealMoney": 1.23456789,
    } for seconds in range(9 * 3600, 13 * 3600 + 30 * 60 + 1, 60)]


def _legacy(root, *, rows=None, status="complete"):
    rows = _rows() if rows is None else rows
    path = worker._session_path(root, ORDER_BOOK_DATASET, DAY)
    path.parent.mkdir(parents=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")
    receipt = {
        "schema_version": 1, "dataset": ORDER_BOOK_DATASET, "date": str(DAY),
        "status": status, "rows": len(rows), "observed_grain": "1m",
        "fetched_at_utc": "2020-01-01T00:00:00+00:00", "point_in_time_training_safe": False,
        "parquet_path": str(path.relative_to(root)), "parquet_size_bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    receipt_path = root / "receipts" / ORDER_BOOK_DATASET / f"{DAY}.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps(receipt))
    return path, receipt_path, receipt, table


def test_order_book_units_are_documented_and_do_not_change_counts_or_raw_values():
    raw = _rows()[:1]
    normalized, units = annotate_stock_share_units(ORDER_BOOK_DATASET, raw)
    assert {key: normalized[0][key] for key in raw[0]} == raw[0]
    assert normalized[0]["TotalBuyVolume_shares"] == 200_000
    assert normalized[0]["TotalSellVolume_shares"] == 150_000
    assert normalized[0]["TotalDealVolume_shares"] == 12_000
    assert normalized[0]["TotalDealMoney_twd"] == pytest.approx(1_234_567.89)
    assert normalized[0]["TotalDealOrder"] == 3
    assert units["normalization_valid"] is True
    assert units["market_scope"] == "twse_regular_trading_only"


@pytest.mark.parametrize("value", [None, -1, float("nan"), float("inf"), True, "broken", 0.0001, 2**63])
def test_invalid_order_book_quantity_is_retained_without_canonical_capacity(value):
    raw = _rows()[:1]
    raw[0]["TotalBuyVolume"] = value
    normalized, units = annotate_stock_share_units(ORDER_BOOK_DATASET, raw)
    assert normalized[0]["TotalBuyVolume"] is value
    assert normalized[0]["TotalBuyVolume_shares"] is None
    assert units["normalization_valid"] is False
    assert units["invalid_fields"] == {"TotalBuyVolume": 1}


@pytest.mark.parametrize("status", ["complete", "partial"])
def test_local_cli_preserves_sources_status_grain_and_freshness(tmp_path, monkeypatch, status):
    source, receipt_path, old, raw = _legacy(tmp_path, status=status)
    old_bytes, old_receipt_bytes = source.read_bytes(), receipt_path.read_bytes()
    other = tmp_path / "receipts" / worker.SESSION_DATASETS[1] / f"{DAY}.json"
    other.parent.mkdir()
    other.write_text('{"unchanged": true}')
    monkeypatch.setattr(worker, "run_once", lambda *_a, **_k: pytest.fail("local mode called API workflow"))
    monkeypatch.setattr(worker.requests, "Session", lambda: pytest.fail("local mode opened API session"))
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 0
    updated = json.loads(receipt_path.read_text())
    for name in ("status", "observed_grain", "fetched_at_utc", "rows", "point_in_time_training_safe"):
        assert updated[name] == old[name]
    normalized = pq.ParquetFile(source).read()
    assert normalized.drop(ORDER_BOOK_CANONICAL_FIELDS).equals(raw)
    assert normalized["TotalDealVolume_shares"][0].as_py() == 12_000
    archive = updated["unit_normalization_previous_source"]
    assert (tmp_path / archive["parquet_path"]).read_bytes() == old_bytes
    assert (tmp_path / archive["receipt_path"]).read_bytes() == old_receipt_bytes
    assert other.read_text() == '{"unchanged": true}'
    stable_source, stable_receipt = source.read_bytes(), receipt_path.read_bytes()
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 0
    assert source.read_bytes() == stable_source
    assert receipt_path.read_bytes() == stable_receipt


@pytest.mark.parametrize("defect", ["hash", "negative", "missing_field"])
def test_local_cli_refuses_bad_legacy_without_writing_source_or_receipt(tmp_path, defect):
    rows = _rows()
    if defect == "negative":
        rows[0]["TotalSellVolume"] = -1
    if defect == "missing_field":
        for row in rows:
            row.pop("TotalBuyVolume")
    source, receipt_path, _, _ = _legacy(tmp_path, rows=rows)
    if defect == "hash":
        payload = bytearray(source.read_bytes())
        payload[20] ^= 1
        source.write_bytes(payload)
    original_source, original_receipt = source.read_bytes(), receipt_path.read_bytes()
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 1
    assert source.read_bytes() == original_source
    assert receipt_path.read_bytes() == original_receipt
    report = json.loads((tmp_path / "unit_normalization" / f"{ORDER_BOOK_DATASET}.json").read_text())
    assert len(report["failures"]) == 1
    assert not (tmp_path / "versions").exists()


def test_local_cli_uses_the_existing_worker_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "normalize_units_local", lambda *_: pytest.fail("bypassed worker lock"))
    with (tmp_path / "worker.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 2


def test_new_download_archives_previous_source_before_unit_upgrade(tmp_path):
    path, receipt_path, old, _ = _legacy(tmp_path)
    old_bytes, receipt_bytes = path.read_bytes(), receipt_path.read_bytes()
    receipt = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, _rows(), now=NOW)
    assert receipt["status"] == "complete"
    assert receipt["volume_units"]["normalization_valid"] is True
    archive = receipt["previous_source"]
    assert (tmp_path / archive["parquet_path"]).read_bytes() == old_bytes
    assert (tmp_path / archive["receipt_path"]).read_bytes() == receipt_bytes
    assert archive["sha256"] == old["sha256"]


@pytest.mark.parametrize("value", [None, -1, float("inf"), True, "bad"])
def test_invalid_new_response_is_preserved_but_never_complete(tmp_path, value):
    rows = _rows()
    rows[0]["TotalSellVolume"] = value
    receipt = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, rows, now=NOW)
    assert receipt["status"] == "partial"
    assert receipt["observed_grain"] == "1m"
    assert receipt["volume_units"]["normalization_valid"] is False
    if receipt.get("parquet_path"):
        source = pq.ParquetFile(tmp_path / receipt["parquet_path"]).read()
        assert source["TotalSellVolume"][0].as_py() == value
        assert source["TotalSellVolume_shares"][0].as_py() is None
    else:
        source = json.loads((tmp_path / receipt["raw_response_path"]).read_text())
        assert source["rows"][0]["TotalSellVolume"] == value


def test_failed_or_unrepresentable_refresh_keeps_previous_source_recoverable(tmp_path):
    path, _, old, _ = _legacy(tmp_path)
    old_bytes = path.read_bytes()
    failed = worker._record_failure(tmp_path, ORDER_BOOK_DATASET, DAY, worker.ProviderError("network"), now=NOW)
    assert (tmp_path / failed["previous_source"]["parquet_path"]).read_bytes() == old_bytes
    rows = _rows()
    rows[0]["TotalSellVolume"] = "bad"
    malformed = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, rows, now=NOW)
    assert malformed["raw_response_path"]
    assert path.read_bytes() == old_bytes
    final = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, _rows(), now=NOW)
    assert final["status"] == "complete"
    assert final["previous_source"]["sha256"] == old["sha256"]


def test_empty_refresh_keeps_previous_source_for_next_success(tmp_path):
    path, _, old, _ = _legacy(tmp_path)
    old_bytes = path.read_bytes()
    empty = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, [], now=NOW)
    assert empty["status"] == "provider_empty"
    assert empty["previous_source"]["sha256"] == old["sha256"]
    assert path.read_bytes() == old_bytes
    final = worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, _rows(), now=NOW)
    assert final["status"] == "complete"
    assert (tmp_path / final["previous_source"]["parquet_path"]).read_bytes() == old_bytes


def test_existing_orphan_source_cannot_be_overwritten_without_original_proof(tmp_path):
    path, receipt_path, _, _ = _legacy(tmp_path)
    receipt_path.write_text(json.dumps({"dataset": ORDER_BOOK_DATASET, "date": str(DAY), "status": "failed"}))
    source_bytes, receipt_bytes = path.read_bytes(), receipt_path.read_bytes()
    with pytest.raises(ValueError, match="identity/path"):
        worker._record_session(tmp_path, ORDER_BOOK_DATASET, DAY, _rows(), now=NOW)
    assert path.read_bytes() == source_bytes
    assert receipt_path.read_bytes() == receipt_bytes


def _interrupt_after_parquet(root, receipt_path, monkeypatch):
    original_write = worker.atomic_write_json

    def fail_receipt(path, payload, **kwargs):
        if path == receipt_path:
            raise OSError("simulated process interruption before receipt commit")
        return original_write(path, payload, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(worker, "atomic_write_json", fail_receipt)
        result = worker.normalize_units_local(root)
    assert result["state"] == "failed"
    assert len(result["failures"]) == 1


def test_local_cli_recovers_only_proven_interrupted_parquet_receipt_commit(tmp_path, monkeypatch):
    path, receipt_path, old, _ = _legacy(tmp_path, status="partial")
    _interrupt_after_parquet(tmp_path, receipt_path, monkeypatch)
    assert worker._read_json(receipt_path) == old
    new_bytes = path.read_bytes()
    new_sha = hashlib.sha256(new_bytes).hexdigest()
    assert new_sha != old["sha256"]
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 0
    result = worker._read_json(tmp_path / "unit_normalization" / f"{ORDER_BOOK_DATASET}.json")
    assert result["recovered_interrupted"] == result["updated"] == 1
    assert result["failures"] == []
    receipt = worker._read_json(receipt_path)
    assert receipt["sha256"] == new_sha
    assert receipt["unit_normalization_recovery"]["canonical_table_equal"] is True
    for name in ("fetched_at_utc", "status", "observed_grain", "rows"):
        assert receipt[name] == old[name]
    assert path.read_bytes() == new_bytes  # recovery only completes the receipt
    stable_receipt = receipt_path.read_bytes()
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 0
    assert receipt_path.read_bytes() == stable_receipt
    assert path.read_bytes() == new_bytes


@pytest.mark.parametrize("tamper", ["raw", "shares", "archive", "metadata"])
def test_interrupted_recovery_refuses_source_or_canonical_tampering(tmp_path, monkeypatch, tamper):
    path, receipt_path, old, _ = _legacy(tmp_path)
    _interrupt_after_parquet(tmp_path, receipt_path, monkeypatch)
    table = pq.ParquetFile(path).read()
    if tamper in {"raw", "shares"}:
        field = "TotalBuyVolume" if tamper == "raw" else "TotalBuyVolume_shares"
        values = table[field].to_pylist()
        values[0] += 1
        table = table.set_column(table.schema.get_field_index(field), field, pa.array(values, type=table[field].type))
        pq.write_table(table, path)
    elif tamper == "metadata":
        pq.write_table(table.replace_schema_metadata({b"changed": b"yes"}), path)
    else:
        archive = tmp_path / "versions" / ORDER_BOOK_DATASET / str(DAY) / f"{old['sha256']}.parquet"
        archive.write_bytes(b"broken archive")
    source_bytes, receipt_bytes = path.read_bytes(), receipt_path.read_bytes()
    assert worker.main(["--root", str(tmp_path), "--normalize-units-local"]) == 1
    result = worker._read_json(tmp_path / "unit_normalization" / f"{ORDER_BOOK_DATASET}.json")
    assert result["recovered_interrupted"] == 0
    assert len(result["failures"]) == 1
    assert "mismatch" in result["failures"][0]["error"]
    assert path.read_bytes() == source_bytes
    assert receipt_path.read_bytes() == receipt_bytes
