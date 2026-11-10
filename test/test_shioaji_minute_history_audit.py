from datetime import date
import csv
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.audit_shioaji_minute_history import audit_history, inventory_recorded_retention_scope


def test_historical_inventory_separates_gaps_unavailable_and_pending(tmp_path):
    stock_root = tmp_path / "stocks"
    source_root = tmp_path / "source"
    output_root = tmp_path / "audit"
    stock_root.mkdir()
    (source_root / "symbols").mkdir(parents=True)
    pl.DataFrame({
        "code": ["1111", "2222", "3333"],
        "name": ["A", "B", "C"],
        "market": ["twse", "twse", "twse"],
        "security_type": ["stock", "stock", "stock"],
    }).write_csv(stock_root / "symbols.csv")
    for symbol in ("1111", "2222"):
        pl.DataFrame({
            "date": [date(2020, 3, 2), date(2020, 3, 3), date(2020, 3, 4)],
            "Trading_Volume": [100.0, 100.0, 100.0],
        }).write_parquet(stock_root / f"{symbol}_features.parquet")
    pl.DataFrame({
        "date": [date(2020, 3, 4)],
        "Trading_Volume": [100.0],
    }).write_parquet(stock_root / "3333_features.parquet")
    (source_root / "download_summary.json").write_text(json.dumps({
        "start_date": "2020-03-02",
        "end_date": "2020-03-03",
        "selected_symbols": 2,
        "reported_symbols": 2,
        "resumable_collection_complete": True,
    }))
    pl.DataFrame({
        "symbol": ["1111", "2222"],
        "status": ["complete_with_source_gaps", "contract_unavailable"],
    }).write_csv(source_root / "download_report.csv")
    (source_root / "symbols" / "1111.manifest.json").write_text(json.dumps({
        "symbol": "1111",
        "requested_start": "2020-03-02",
        "requested_end": "2020-03-03",
        "terminal_coverage_dates": ["2020-03-02", "2020-03-03"],
        "source_gap_dates": ["2020-03-03"],
        "first_date": "2020-03-02",
        "last_date": "2020-03-02",
    }))

    sealed = audit_history(
        stock_root=stock_root, source_root=source_root,
        start=date(2020, 3, 2), end=date(2020, 3, 3), output_root=output_root,
    )
    assert sealed["status"] == "classified_with_limits"
    assert sealed["unreported_current_symbols"] == ["3333"]
    assert sealed["counts"] == {
        "observed": 1, "source_gap": 1, "contract_unavailable": 2,
        "unclassified": 0, "pending": 0,
        "source_gap_outside_public_daily": 0,
        "source_returned_outside_public_daily": 0,
        "expected": 4,
    }
    extended = audit_history(
        stock_root=stock_root, source_root=source_root,
        start=date(2020, 3, 2), end=date(2020, 3, 4), output_root=output_root,
    )
    assert extended["status"] == "incomplete"
    assert extended["counts"]["pending"] == 3
    assert extended["counts"]["expected"] == 7


def test_historical_inventory_keeps_broker_outcomes_outside_current_daily(tmp_path):
    stock_root = tmp_path / "stocks"
    source_root = tmp_path / "source"
    output_root = tmp_path / "audit"
    stock_root.mkdir()
    (source_root / "symbols").mkdir(parents=True)
    pl.DataFrame({
        "code": ["1111"], "name": ["A"], "market": ["twse"],
        "security_type": ["stock"],
    }).write_csv(stock_root / "symbols.csv")
    pl.DataFrame({
        "date": [date(2020, 10, 27)], "Trading_Volume": [100.0],
    }).write_parquet(stock_root / "1111_features.parquet")
    (source_root / "download_summary.json").write_text(json.dumps({
        "start_date": "2020-03-02", "end_date": "2020-10-29",
        "selected_symbols": 1, "reported_symbols": 1,
        "resumable_collection_complete": True,
    }))
    pl.DataFrame({
        "symbol": ["1111"], "status": ["complete_with_source_gaps"],
    }).write_csv(source_root / "download_report.csv")
    (source_root / "symbols" / "1111.manifest.json").write_text(json.dumps({
        "symbol": "1111", "requested_start": "2020-03-02",
        "requested_end": "2020-10-29",
        "terminal_coverage_dates": ["2020-10-27", "2020-10-28", "2020-10-29"],
        "source_gap_dates": ["2020-10-28"],
        "first_date": "2020-10-27", "last_date": "2020-10-29",
    }))

    report = audit_history(
        stock_root=stock_root, source_root=source_root,
        start=date(2020, 3, 2), end=date(2020, 10, 29),
        output_root=output_root,
    )
    assert report["counts"]["expected"] == 1
    assert report["counts"]["observed"] == 1
    assert report["counts"]["source_gap_outside_public_daily"] == 1
    assert report["counts"]["source_returned_outside_public_daily"] == 1
    with (output_root / "missing_symbol_days.csv").open(newline="") as handle:
        missing = list(csv.DictReader(handle))
    assert missing == [{
        "symbol": "1111", "trade_date": "2020-10-28",
        "year": "2020", "category": "source_gap_outside_public_daily",
    }]


def _retention_fixture(tmp_path):
    root = tmp_path / "data_tw_minute" / "shioaji_1m"
    (root / "symbols").mkdir(parents=True)
    (root / "download_summary.json").write_text(json.dumps({
        "source": "shioaji_kbars_1m", "storage_frequency": "minute",
        "selected_symbols": 2, "reported_symbols": 2,
    }))
    (root / "download_report.csv").write_text("symbol,status\n2330,complete\n9999,contract_unavailable\n")
    prefix = "data_tw_minute/shioaji_1m/minute_chunks/2330/"
    chunks = [
        {"start_date": "2025-01-01", "end_date": "2025-01-29", "status": "ok",
         "rows": 2, "data_path": prefix + "old.parquet", "data_sha256": "a" * 64,
         "receipt_path": prefix + "old.receipt.json"},
        {"start_date": "2025-12-18", "end_date": "2026-01-15", "status": "ok",
         "rows": 3, "data_path": prefix + "mixed.parquet", "data_sha256": "b" * 64,
         "receipt_path": prefix + "mixed.receipt.json"},
        {"start_date": "2025-02-01", "end_date": "2025-02-28", "status": "empty",
         "rows": 0, "data_path": None, "data_sha256": None,
         "receipt_path": prefix + "empty.receipt.json"},
        {"start_date": "2026-01-16", "end_date": "2026-02-01", "status": "ok",
         "rows": 4, "data_path": prefix + "new.parquet", "data_sha256": "c" * 64,
         "receipt_path": prefix + "new.receipt.json"},
    ]
    manifest = root / "symbols" / "2330.manifest.json"
    manifest.write_text(json.dumps({"symbol": "2330", "market": "twse",
        "source": "shioaji_kbars_1m", "storage_frequency": "minute", "chunks": chunks}))
    return root, manifest, chunks


def test_recorded_retention_inventory_separates_old_mixed_and_empty_without_deletion(tmp_path):
    root, manifest, _ = _retention_fixture(tmp_path)
    original = manifest.read_bytes()
    output = tmp_path / "inventory"
    result = inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
                                                cutoff=date(2026, 1, 1), output_root=output)
    assert result["status"] == "recorded_candidates_with_unverified_files"
    assert result["deletion_authorized"] is False
    assert result["bytes_or_file_content_verified"] is False
    assert result["unreferenced_files_included"] is False
    assert result["counts"] == {"contract_unavailable_symbols": 1, "declared_before_cutoff": 2,
        "declared_crosses_cutoff": 1, "declared_data_files": 2, "manifests_read": 1,
        "receipt_only_empty": 1, "receipt_only_source_gap": 0, "reported_symbols": 2,
        "source_gap_chunks": 0}
    with (output / "recorded_chunks.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["classification"] for row in rows] == ["declared_before_cutoff",
        "declared_crosses_cutoff", "declared_before_cutoff"]
    assert rows[1]["data_path"].endswith("mixed.parquet")
    assert rows[2]["data_path"] == "" and rows[2]["receipt_path"].endswith("empty.receipt.json")
    assert len({row["source_manifest_sha256"] for row in rows}) == 1
    assert manifest.read_bytes() == original
    with pytest.raises(FileExistsError):
        inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
                                           cutoff=date(2026, 1, 1), output_root=output)


@pytest.mark.parametrize("defect", ["outside_root", "duplicate_path", "bad_hash", "changed_identity"])
def test_recorded_retention_inventory_fails_closed_per_symbol(tmp_path, defect):
    root, manifest, chunks = _retention_fixture(tmp_path)
    document = json.loads(manifest.read_text())
    if defect == "outside_root":
        chunks[0]["data_path"] = "data_tw_minute/shioaji_1m/minute_chunks/2330/../../outside.parquet"
    elif defect == "duplicate_path":
        chunks[1]["data_path"] = chunks[0]["data_path"]
    elif defect == "bad_hash":
        chunks[0]["data_sha256"] = "not-a-hash"
    else:
        document["symbol"] = "other"
    document["chunks"] = chunks
    manifest.write_text(json.dumps(document))
    output = tmp_path / "inventory"
    result = inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
                                                cutoff=date(2026, 1, 1), output_root=output)
    assert result["status"] == "incomplete" and result["deletion_authorized"] is False
    assert result["errors"][0]["symbol"] == "2330"
    with (output / "recorded_chunks.csv").open(newline="") as handle:
        assert list(csv.DictReader(handle)) == []


@pytest.mark.parametrize("has_data", [True, False])
def test_recorded_retention_inventory_retains_source_gap_without_claiming_empty(tmp_path, has_data):
    root, manifest, chunks = _retention_fixture(tmp_path)
    document = json.loads(manifest.read_text())
    chunks[0]["status"] = "source_gap"
    chunks[0]["source_gap_dates"] = ["2025-01-02"]
    if not has_data:
        chunks[0].update(rows=0, data_path=None, data_sha256=None)
    document["chunks"] = chunks
    manifest.write_text(json.dumps(document))
    result = inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
        cutoff=date(2026, 1, 1), output_root=tmp_path / "inventory")
    assert result["status"] == "recorded_candidates_with_unverified_files"
    assert result["counts"]["source_gap_chunks"] == 1
    assert result["counts"]["receipt_only_source_gap"] == int(not has_data)
    with (tmp_path / "inventory" / "recorded_chunks.csv").open(newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["source_status"] == "source_gap"
    assert bool(row["data_path"]) is has_data


def test_recorded_retention_inventory_rejects_unproven_source_gap(tmp_path):
    root, manifest, chunks = _retention_fixture(tmp_path)
    document = json.loads(manifest.read_text())
    chunks[0]["status"] = "source_gap"
    chunks[0]["source_gap_dates"] = []
    document["chunks"] = chunks
    manifest.write_text(json.dumps(document))
    result = inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
        cutoff=date(2026, 1, 1), output_root=tmp_path / "inventory")
    assert result["status"] == "incomplete" and result["deletion_authorized"] is False


def test_recorded_retention_inventory_marks_concurrent_catalog_change_incomplete(tmp_path, monkeypatch):
    root, _, _ = _retention_fixture(tmp_path)
    report = root / "download_report.csv"
    original_open = Path.open
    changed = False

    def racing_open(path, *args, **kwargs):
        nonlocal changed
        stream = original_open(path, *args, **kwargs)
        if path.name == "2330.manifest.json" and not changed:
            changed = True
            report.write_text("symbol,status\n2330,complete\n9999,contract_unavailable\n\n")
        return stream

    monkeypatch.setattr(Path, "open", racing_open)
    result = inventory_recorded_retention_scope(source_root=root, repo_root=tmp_path,
        cutoff=date(2026, 1, 1), output_root=tmp_path / "inventory")
    assert changed and result["status"] == "incomplete"
    assert result["catalog_unchanged_during_pass"] is False
    assert result["deletion_authorized"] is False
