"""Read-only source-to-source check of stored FinMind and official TW daily prices.

This spends no provider quota. It never repairs or overwrites either source.
Only exact date/symbol intersections and unadjusted prices are compared.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

import duckdb

from downloader.artifact_io import atomic_write_json


def audit(repo_root: Path) -> dict[str, object]:
    raw_root = repo_root / "data_finmind" / "sponsor"
    db_path = raw_root / "queue.sqlite3"
    output = repo_root / "artifacts" / "data_quality" / "finmind_sponsor_source_audit" / "latest.json"
    result: dict[str, object] = {
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "state": "waiting_sponsor_price", "source": "FinMind TaiwanStockPrice",
        "reference": "TWSE and TPEx official daily OHLCV",
        "query_scope": "latest_completed_sponsor_partition_intersection_only",
        "point_in_time_training_approved": False,
    }
    if not db_path.is_file():
        atomic_write_json(output, result)
        return result
    with sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True) as connection:
        record = connection.execute(
            "SELECT receipt_path FROM tasks WHERE dataset='TaiwanStockPrice' "
            "AND state='complete' AND rows>0 ORDER BY last_data_date DESC LIMIT 1"
        ).fetchone()
    if not record or not record[0]:
        atomic_write_json(output, result)
        return result
    receipt_path = (raw_root / record[0]).resolve()
    if not receipt_path.is_relative_to(raw_root.resolve()):
        result["state"] = "invalid_receipt_path"
        atomic_write_json(output, result)
        return result
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    relative = receipt.get("parquet_path")
    if not isinstance(relative, str):
        result["state"] = "missing_sponsor_parquet"
        atomic_write_json(output, result)
        return result
    sponsor_path = (raw_root / relative).resolve()
    if not sponsor_path.is_relative_to(raw_root.resolve()) or not sponsor_path.is_file() or \
            sponsor_path.stat().st_size != receipt.get("parquet_size_bytes"):
        result["state"] = "missing_sponsor_parquet"
        atomic_write_json(output, result)
        return result
    twse = repo_root / "data_tw_public" / "twse_daily_ohlcv.parquet"
    tpex = repo_root / "data_tw_public" / "tpex_daily_ohlcv.parquet"
    if not twse.is_file() or not tpex.is_file():
        result["state"] = "waiting_official_price"
        atomic_write_json(output, result)
        return result
    with duckdb.connect(":memory:") as connection:
        connection.execute("PRAGMA threads=2")
        connection.execute("PRAGMA memory_limit='1GB'")
        connection.execute("""
            CREATE TEMP TABLE comparison AS
            WITH finmind AS (
                SELECT date, stock_id, close,
                       TRY_CAST(Trading_Volume AS BIGINT) AS volume
                FROM read_parquet(?)
            ), official_raw AS (
                SELECT date, "證券代號" AS stock_id,
                       TRY_CAST(REPLACE("收盤價", ',', '') AS DOUBLE) AS close,
                       TRY_CAST(REPLACE("成交股數", ',', '') AS BIGINT) AS volume
                FROM read_parquet(?)
                UNION ALL
                SELECT date, "代號" AS stock_id,
                       TRY_CAST(REPLACE("收盤", ',', '') AS DOUBLE) AS close,
                       TRY_CAST(REPLACE("成交股數", ',', '') AS BIGINT) AS volume
                FROM read_parquet(?)
            ), official AS (
                SELECT date, stock_id, MAX(close) AS close, MAX(volume) AS volume,
                       COUNT(*) AS raw_rows
                FROM official_raw GROUP BY date,stock_id
            )
            SELECT f.date,f.stock_id,f.close AS finmind_close,o.close AS official_close,
                   f.volume AS finmind_volume,o.volume AS official_volume,
                   o.raw_rows,
                   f.close > 0 AND o.close IS NOT NULL AND ABS(f.close-o.close)>0.005 AS close_mismatch,
                   f.volume IS NOT NULL AND o.volume IS NOT NULL AND f.volume<>o.volume AS volume_mismatch
            FROM finmind f JOIN official o USING(date,stock_id)
        """, [str(sponsor_path), str(twse), str(tpex)])
        counts = connection.execute("""
            SELECT COUNT(*), SUM(CAST(close_mismatch AS BIGINT)),
                   SUM(CAST(volume_mismatch AS BIGINT)),
                   SUM(CAST(raw_rows>1 AS BIGINT)),MIN(date),MAX(date)
            FROM comparison
        """).fetchone()
        sample = connection.execute("""
            SELECT date,stock_id,finmind_close,official_close,
                   finmind_volume,official_volume,close_mismatch,volume_mismatch
            FROM comparison WHERE close_mismatch OR volume_mismatch
            ORDER BY date DESC,stock_id LIMIT 20
        """).fetchall()
    result.update({
        "state": "compared" if counts[0] else "no_exact_symbol_date_intersection",
        "sponsor_partition": receipt.get("partition"),
        "exact_symbol_date_pairs": counts[0],
        "close_mismatches": counts[1] or 0,
        "volume_mismatches": counts[2] or 0,
        "official_duplicate_pairs": counts[3] or 0,
        "first_date": counts[4], "last_date": counts[5],
        "mismatch_sample": [dict(zip(("date", "stock_id", "finmind_close", "official_close",
                                      "finmind_volume", "official_volume", "close_mismatch",
                                      "volume_mismatch"), row)) for row in sample],
        "interpretation": "Differences are flags for source/definition investigation, never automatic replacement.",
    })
    atomic_write_json(output, result)
    return result


def main() -> int:
    result = audit(Path(__file__).resolve().parents[1])
    print(json.dumps({key: result.get(key) for key in
                      ("state", "exact_symbol_date_pairs", "close_mismatches", "volume_mismatches")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
