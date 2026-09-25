from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.download_finmind_complement import Task
from downloader.download_finmind_sponsor import _db, _finish
from scripts.audit_finmind_sponsor_overlap import audit


def test_source_audit_checks_exact_join_and_never_replaces_source(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind" / "sponsor"
    root.mkdir(parents=True)
    official = tmp_path / "data_tw_public"
    official.mkdir()
    pq.write_table(pa.Table.from_pylist([{
        "date": "2026-09-22", "證券代號": "2330", "收盤價": "100.00", "成交股數": "1,000",
    }]), official / "twse_daily_ohlcv.parquet")
    pq.write_table(pa.Table.from_pylist([{
        "date": "2026-09-22", "代號": "1234", "收盤": "50.00", "成交股數": "500",
    }]), official / "tpex_daily_ohlcv.parquet")
    task = Task("TaiwanStockPrice", "", "2026-09-22", "two_day", 0, "inflight")
    with _db(root / "queue.sqlite3") as conn:
        conn.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,?,?,?,?,'inflight')", (task.dataset, "", task.partition, task.kind, 0)
        )
        _finish(conn, root, task, [
            {"date": "2026-09-22", "stock_id": "2330", "close": 100.0, "Trading_Volume": 1000},
            {"date": "2026-09-22", "stock_id": "1234", "close": 51.0, "Trading_Volume": 500},
        ], datetime(2026, 9, 26, tzinfo=UTC))
    result = audit(tmp_path)
    assert result["state"] == "compared"
    assert result["exact_symbol_date_pairs"] == 2
    assert result["close_mismatches"] == 1
    assert result["volume_mismatches"] == 0
    assert result["point_in_time_training_approved"] is False
