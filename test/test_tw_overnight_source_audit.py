from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_tw_overnight_auction_sources import audit_partition, file_sha256


def write_bars(tmp_path, *, duplicate=False, wrong_date=False):
    rows = [
        {"symbol": "2330", "ts": datetime(2026, 9, 8, 9, 1),
         "minutes_from_open": 1, "Open": 100.0, "Close": 101.0, "volume_shares": 3000.0},
        {"symbol": "2330", "ts": datetime(2026, 9, 8, 13, 25),
         "minutes_from_open": 265, "Open": 103.0, "Close": 104.0, "volume_shares": 2000.0},
        {"symbol": "2330", "ts": datetime(2026, 9, 8, 13, 30),
         "minutes_from_open": 270, "Open": 105.0, "Close": 105.0, "volume_shares": 0.0},
    ]
    if duplicate:
        rows.append(rows[1].copy())
    if wrong_date:
        rows[1]["ts"] = datetime(2026, 9, 9, 13, 25)
    path = tmp_path / "bars.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_prices_are_distinct_from_auction_and_liquidity_proof(tmp_path):
    path = write_bars(tmp_path)
    result = audit_partition(path, "2026-09-08", file_sha256(path))
    assert result["receipt_valid"]
    assert result["decision_symbols"] == ["2330"]
    assert result["opening_bar_price_volume_symbols"] == ["2330"]
    assert result["close_price_volume_symbols"] == []
    assert not result["auction_identity_proven"]
    assert result["error"] is None


def test_modified_source_is_rejected_before_price_counts(tmp_path):
    path = write_bars(tmp_path)
    result = audit_partition(path, "2026-09-08", "0" * 64)
    assert not result["receipt_valid"]
    assert result["error"] == "sha256_mismatch"
    assert "decision_symbols" not in result


def test_duplicate_symbol_minute_is_invalid(tmp_path):
    path = write_bars(tmp_path, duplicate=True)
    result = audit_partition(path, "2026-09-08", file_sha256(path))
    assert result["error"] == "invalid_row_identity"
    assert result["duplicate_keys"] == [["2330", 265]]


def test_renamed_partition_cannot_hide_wrong_exchange_date(tmp_path):
    path = write_bars(tmp_path, wrong_date=True)
    result = audit_partition(path, "2026-09-08", file_sha256(path))
    assert result["error"] == "invalid_row_identity"
    assert result["decision_symbols"] == []
    assert len(result["timestamp_errors"]) == 1
