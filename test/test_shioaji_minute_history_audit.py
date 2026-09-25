from datetime import date
import csv
import json

import polars as pl

from scripts.audit_shioaji_minute_history import audit_history


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
