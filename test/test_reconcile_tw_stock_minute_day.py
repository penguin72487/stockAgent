from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from scripts.reconcile_tw_stock_minute_day import (
    compare_raw_frames,
    reconcile,
    validate_source_day,
)
from scripts import reconcile_tw_stock_minute_day as minute_reconcile
from scripts.build_shioaji_tw_minute_dataset import build_research_frame, _sha256


def _raw(symbol: str, clock: str, close: float = 10.0) -> dict:
    return {
        "symbol": symbol, "date": date(2026, 2, 25),
        "ts": datetime.fromisoformat(f"2026-02-25T{clock}:00"),
        "market": "twse", "contract_unit": 1000.0,
        "Open": 10.0, "High": max(10.0, close), "Low": min(10.0, close),
        "Close": close, "Volume": 1.0, "Amount": 10000.0,
    }


def test_source_parity_detects_lost_and_altered_rows() -> None:
    source = pl.DataFrame([_raw("0056", "09:01"), _raw("2330", "09:02")])
    derived = pl.DataFrame([_raw("0056", "09:01", 11.0)])
    result = compare_raw_frames(source, derived)
    assert result["source_rows_missing_from_derived"] == 1
    assert result["raw_value_mismatches"] == 1
    assert result["derived_rows_without_source"] == 0


def test_duplicate_source_key_blocks_multiplicative_value_join() -> None:
    source = pl.DataFrame([_raw("0056", "09:01"), _raw("0056", "09:01")])
    result = compare_raw_frames(source, pl.DataFrame([_raw("0056", "09:01")]))
    assert result["source_duplicate_keys"] == 1
    assert result["raw_value_comparison_skipped"] is True


def test_source_validation_checks_clock_and_ohlc_without_dense_grid_requirement() -> None:
    day = date(2026, 2, 25)
    sparse = pl.DataFrame([_raw("0056", "09:03"), _raw("0056", "13:30")])
    assert validate_source_day(sparse, day)["invalid_source_rows"] == 0
    outside = pl.DataFrame([_raw("0056", "08:59")])
    assert validate_source_day(outside, day)["invalid_source_rows"] == 1
    malformed = pl.DataFrame([_raw("0056", "09:01", -1.0)])
    assert validate_source_day(malformed, day)["invalid_source_rows"] == 1


def test_repair_promotes_verified_missing_source_row_and_quarantines_old(
    tmp_path: Path, monkeypatch,
) -> None:
    day = date(2026, 2, 25)
    input_root = tmp_path / "source"
    output_root = tmp_path / "research"
    symbol = "2330"
    chunk = input_root / "minute_chunks" / symbol / f"{day}_{day}.parquet"
    chunk.parent.mkdir(parents=True)
    raw = pl.DataFrame([_raw(symbol, "09:01"), _raw(symbol, "09:02")])
    raw.write_parquet(chunk)
    receipt = chunk.with_suffix(".receipt.json")
    receipt.write_text(json.dumps({
        "schema_version": 1, "source": "shioaji_kbars_1m",
        "storage_frequency": "minute", "simulation": True,
        "symbol": symbol, "start_date": str(day), "end_date": str(day),
        "status": "ok", "rows": 2, "source_gap_dates": [],
        "returned_dates": [str(day)],
        "output_receipt": {"path": str(chunk), "size": chunk.stat().st_size,
                           "sha256": _sha256(chunk)},
    }))
    manifest_dir = input_root / "symbols"
    manifest_dir.mkdir()
    (manifest_dir / f"{symbol}.manifest.json").write_text(json.dumps({
        "symbol": symbol, "chunks": [{
            "start_date": str(day), "end_date": str(day),
            "receipt_path": str(receipt), "data_path": str(chunk),
            "data_sha256": _sha256(chunk), "source_gap_dates": [],
        }],
    }))
    report_csv = input_root / "download_report.csv"
    report_csv.write_text("symbol,status\n2330,complete\n")
    (input_root / "download_summary.json").write_text(json.dumps({
        "schema_version": 1, "source": "shioaji_kbars_1m",
        "storage_frequency": "minute", "simulation": True,
        "start_date": str(day), "end_date": str(day),
        "selected_symbols": 1, "reported_symbols": 1,
        "complete_symbols": 1, "complete_with_source_gap_symbols": 0,
        "contract_unavailable_symbols": 0, "failed_symbols": 0,
        "partial_symbols": 0, "resumable_collection_complete": True,
        "report_path": str(report_csv),
    }))
    partition_dir = output_root / f"trade_date={day}"
    partition_dir.mkdir(parents=True)
    derived = build_research_frame(raw.head(1).lazy()).collect()
    partition = partition_dir / "data.parquet"
    derived.write_parquet(partition)
    old_sha = _sha256(partition)
    old_summary = {"status": "ok", "trade_date": str(day),
                   "output_sha256": old_sha,
                   "input_chunk_start": str(day), "input_chunk_end": str(day)}
    (partition_dir / "summary.json").write_text(json.dumps(old_summary))
    (output_root / "manifest.json").write_text(json.dumps({
        "status": "research_ready", "dates": [str(day)],
        "partitions": [old_summary],
    }))

    class AfterClose(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 2, 26, 15, 0, tzinfo=tz or timezone.utc)

    monkeypatch.setattr(minute_reconcile, "datetime", AfterClose)
    monkeypatch.setattr(minute_reconcile, "_expected_by_date",
                        lambda *_args: ([], {day: {symbol: object()}}))
    result = reconcile(input_root, output_root, day, stock_root=tmp_path,
                       repair=True)
    assert result["status"] == "locally_repaired"
    assert pl.read_parquet(partition).height == 2
    assert _sha256(partition) != old_sha
    assert json.loads((partition_dir / "summary.json").read_text())[
        "output_sha256"] == _sha256(partition)
    assert json.loads((output_root / "manifest.json").read_text())[
        "partitions"][0]["output_sha256"] == _sha256(partition)
    quarantine = Path(result["quarantined_original"])
    assert _sha256(quarantine / "data.parquet") == old_sha
