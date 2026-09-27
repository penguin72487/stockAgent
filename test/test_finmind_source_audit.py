from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.download_finmind_complement import Task
from downloader.download_finmind_sponsor import _db, _finish
from downloader.artifact_io import sha256_file
import scripts.audit_finmind_sponsor_overlap as source_audit
from scripts.audit_finmind_sponsor_overlap import audit


def _write_official_receipt(official: Path) -> None:
    path = official / "stocks/official_symbol_build_summary.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps({
        "source": "twse_tpex_official", "source_receipts": [
            {"name": name, "size": (official / name).stat().st_size,
             "sha256": sha256_file(official / name)} for name in source_audit.OFFICIAL_NAMES
        ],
    }))


def _seed(tmp_path: Path, *, finmind_rows=None, twse_rows=None, tpex_rows=None) -> Path:
    root = tmp_path / "data_finmind/sponsor"
    root.mkdir(parents=True)
    official = tmp_path / "data_tw_public"
    official.mkdir()
    pq.write_table(pa.Table.from_pylist(twse_rows if twse_rows is not None else [{
        "date": "2026-09-22", "證券代號": "2330", "收盤價": "100.00", "成交股數": "1,000",
    }]), official / "twse_daily_ohlcv.parquet")
    pq.write_table(pa.Table.from_pylist(tpex_rows if tpex_rows is not None else [{
        "date": "2026-09-22", "代號": "1234", "收盤": "50.00", "成交股數": "500",
    }]), official / "tpex_daily_ohlcv.parquet")
    _write_official_receipt(official)
    task = Task("TaiwanStockPrice", "", "2026-09-22", "day", 0, "inflight")
    with _db(root / "queue.sqlite3") as conn:
        conn.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,?,?,?,?,'inflight')", (task.dataset, "", task.partition, task.kind, 0)
        )
        _finish(conn, root, task, finmind_rows if finmind_rows is not None else [
            {"date": "2026-09-22", "stock_id": "2330", "close": 100.0, "Trading_Volume": 1000},
            {"date": "2026-09-22", "stock_id": "1234", "close": 51.0, "Trading_Volume": 500},
        ], datetime(2026, 9, 26, tzinfo=UTC))
    return root / "receipts/TaiwanStockPrice/all/2026-09-22.json"


def test_source_audit_checks_exact_join_and_never_replaces_source(tmp_path: Path) -> None:
    _seed(tmp_path)
    originals = {path: path.read_bytes() for path in tmp_path.rglob("*.parquet")}
    result = audit(tmp_path)
    assert result["state"] == "compared"
    assert result["exact_symbol_date_pairs"] == 2
    assert result["close_mismatches"] == 1
    assert result["volume_mismatches"] == 0
    assert result["point_in_time_training_approved"] is False
    assert result["source_hashes_verified"] is True
    assert result["full_history_verified"] is False
    assert result["missing_in_finmind"] == result["missing_in_official"] == 0
    assert all(path.read_bytes() == content for path, content in originals.items())


def test_source_audit_rejects_same_size_corruption_before_cached_comparison(tmp_path: Path) -> None:
    receipt_path = _seed(tmp_path)
    assert audit(tmp_path)["state"] == "compared"
    receipt = json.loads(receipt_path.read_text())
    source = tmp_path / "data_finmind/sponsor" / receipt["parquet_path"]
    raw = source.read_bytes()
    source.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    result = audit(tmp_path)
    assert result["state"] == "source_hash_mismatch"
    assert result["source_hashes_verified"] is False
    assert "exact_symbol_date_pairs" not in result


def test_source_audit_rejects_unreceipted_official_change(tmp_path: Path) -> None:
    _seed(tmp_path)
    official = tmp_path / "data_tw_public/twse_daily_ohlcv.parquet"
    raw = official.read_bytes()
    official.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    assert audit(tmp_path)["state"] == "source_hash_mismatch"


def test_source_audit_requires_official_hash_receipt(tmp_path: Path) -> None:
    _seed(tmp_path)
    path = tmp_path / "data_tw_public/stocks/official_symbol_build_summary.json"
    receipt = json.loads(path.read_text())
    del receipt["source_receipts"][0]["sha256"]
    path.write_text(json.dumps(receipt))
    assert audit(tmp_path)["state"] == "missing_source_hash_receipt"


def test_source_audit_refuses_official_duplicate_even_identical_across_venues(tmp_path: Path) -> None:
    _seed(tmp_path, tpex_rows=[{
        "date": "2026-09-22", "代號": "2330", "收盤": "100.00", "成交股數": "1,000",
    }])
    result = audit(tmp_path)
    assert result["state"] == "invalid_source_grain"
    assert result["official_duplicate_pairs"] == 1
    assert "exact_symbol_date_pairs" not in result


def test_source_audit_refuses_finmind_duplicate(tmp_path: Path) -> None:
    row = {"date": "2026-09-22", "stock_id": "2330", "close": 100.0, "Trading_Volume": 1000}
    _seed(tmp_path, finmind_rows=[row, row])
    assert audit(tmp_path)["finmind_duplicate_pairs"] == 1


def test_source_audit_diagnoses_both_missing_directions_only_on_observed_dates(tmp_path: Path) -> None:
    _seed(tmp_path, finmind_rows=[
        {"date": "2026-09-22", "stock_id": "2330", "close": 100.0, "Trading_Volume": 1000},
        {"date": "2026-09-22", "stock_id": "9999", "close": 40.0, "Trading_Volume": 30},
    ], twse_rows=[
        {"date": "2026-09-22", "證券代號": "2330", "收盤價": "100.00", "成交股數": "1,000"},
        {"date": "2026-09-23", "證券代號": "1101", "收盤價": "10.00", "成交股數": "300"},
    ])
    result = audit(tmp_path)
    assert result["state"] == "compared"
    assert result["exact_symbol_date_pairs"] == 1
    assert result["missing_in_official"] == result["missing_in_finmind"] == 1
    assert result["missing_in_official_sample"][0]["stock_id"] == "9999"
    assert result["missing_in_finmind_sample"][0]["stock_id"] == "1234"


def test_source_audit_reuses_only_verified_fingerprint(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path)
    first = audit(tmp_path)
    monkeypatch.setattr(source_audit, "_compare", lambda *args: (_ for _ in ()).throw(AssertionError("recomputed")))
    second = audit(tmp_path)
    assert second["comparison_reused"] is True
    assert first["source_fingerprint"] == second["source_fingerprint"]
    assert first["compared_at_utc"] == second["compared_at_utc"]


def test_source_audit_detects_source_commit_during_query(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path)
    original = source_audit._compare
    def change(*args):
        result = original(*args)
        path = tmp_path / "data_tw_public/twse_daily_ohlcv.parquet"
        path.touch()
        return result
    monkeypatch.setattr(source_audit, "_compare", change)
    result = audit(tmp_path)
    assert result["state"] == "source_changed_during_audit"
    assert "exact_symbol_date_pairs" not in result


def test_source_audit_reports_unusable_close_instead_of_matching_zero(tmp_path: Path) -> None:
    _seed(tmp_path, finmind_rows=[{
        "date": "2026-09-22", "stock_id": "2330", "close": 0.0, "Trading_Volume": 0,
    }])
    result = audit(tmp_path)
    assert result["state"] == "compared"
    assert result["close_uncomparable_pairs"] == 1
    assert result["volume_mismatches"] == 1


def test_source_audit_does_not_round_fractional_shares(tmp_path: Path) -> None:
    _seed(tmp_path, finmind_rows=[{
        "date": "2026-09-22", "stock_id": "2330", "close": 100.0, "Trading_Volume": 1000.4,
    }])
    result = audit(tmp_path)
    assert result["state"] == "invalid_source_values"
    assert result["finmind_invalid_rows"] == 1


def test_source_audit_rejects_foreign_receipt_paths_without_leaking_them(tmp_path: Path) -> None:
    receipt_path = _seed(tmp_path)
    receipt = json.loads(receipt_path.read_text())
    receipt["parquet_path"] = "../../../account_secret.parquet"
    receipt_path.write_text(json.dumps(receipt))
    result = audit(tmp_path)
    assert result["state"] == "invalid_receipt_path"
    assert "account_secret" not in json.dumps(result)


def test_source_audit_rejects_conflicting_declared_unit(tmp_path: Path) -> None:
    receipt_path = _seed(tmp_path)
    receipt = json.loads(receipt_path.read_text())
    receipt["volume_units"]["source_volume_units"]["Trading_Volume"] = "stock_trading_lots"
    receipt_path.write_text(json.dumps(receipt))
    assert audit(tmp_path)["state"] == "incompatible_source_units"


def test_source_audit_enforces_bound_before_decode(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path)
    monkeypatch.setattr(source_audit, "MAX_PARTITION_ROWS", 1)
    monkeypatch.setattr(source_audit, "_compare", lambda *args: (_ for _ in ()).throw(AssertionError("decoded")))
    assert audit(tmp_path)["state"] == "partition_exceeds_audit_bound"


def test_source_audit_uses_catalog_authority_not_stale_local_alias(tmp_path: Path) -> None:
    _seed(tmp_path)
    catalog = tmp_path / "configs/data_sync/packed_datasets.json"
    catalog.parent.mkdir(parents=True)
    catalog.write_text(json.dumps({"datasets": [{"dataset": "tw-public", "source": "official-live"}]}))
    (tmp_path / "data_tw_public").rename(tmp_path / "official-live")
    assert audit(tmp_path)["state"] == "compared"
