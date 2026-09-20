from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_symbol_history_coverage import _field_interpretation, build_audit
from stockagent.live.data_monitor_inventory import parquet_footer_stats


def test_audit_keeps_hot_tail_separate_and_reports_all_null_fields(tmp_path: Path) -> None:
    directory = tmp_path / "data_okx/1m"
    directory.mkdir(parents=True)
    with (directory / "symbols.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["code", "list_time"])
        writer.writeheader()
        writer.writerow({"code": "BTC", "list_time": "2026-09-01 00:00:00"})
        writer.writerow({"code": "ETH", "list_time": "2026-09-01 00:00:00"})
    base = directory / "BTC_features.parquet"
    tail = directory / "_hot_tail/BTC_features.parquet"
    tail.parent.mkdir()
    pq.write_table(pa.table({"date": [datetime(2026, 9, 1, tzinfo=UTC)], "unused": pa.array([None], type=pa.float64())}), base)
    pq.write_table(pa.table({"date": [datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 2, tzinfo=UTC)]}), tail)
    cache = {}
    schemas = {}
    for path in (base, tail):
        stats = parquet_footer_stats(path)
        assert stats is not None
        schemas[stats["schema_id"]] = stats.pop("fields")
        stat = path.stat()
        cache[str(path)] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "stats": stats}

    symbols, fields, summary = build_audit(tmp_path, {"files": cache, "schemas": schemas})
    okx = summary["sources"]["okx_swap"]
    assert okx["manifest_symbols"] == 2
    assert okx["base_verified"] == 1
    assert okx["base_missing"] == 1
    btc = next(row for row in symbols if row["source"] == "okx_swap" and row["symbol"] == "BTC")
    assert btc["base_rows"] == 1
    assert btc["tail_physical_rows"] == 2  # overlaps the base; not a unique total
    assert btc["latest_stored"].startswith("2026-09-02")
    assert any(row["field"] == "unused" and row["state"] == "all_null" for row in fields)


def test_tw_required_bar_gaps_are_distinct_from_conditional_evidence() -> None:
    assert _field_interpretation("tw_official", "close", "all_null") == "required_bar_gap"
    assert _field_interpretation("tw_official", "close", "all_present") == "required_bar_present"
    assert (
        _field_interpretation("tw_official", "fallback_reason", "all_null")
        == "conditional_evidence_not_a_required_value"
    )
