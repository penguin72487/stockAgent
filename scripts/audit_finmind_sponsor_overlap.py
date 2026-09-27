"""Bounded, no-API comparison of stored raw TW daily prices and share volumes.

This checks the latest completed Sponsor partition, not full history. Both
official inputs and the Sponsor input must match their exact-byte receipts.
Missing keys are diagnostics, never permission to overwrite either source.
"""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any

import duckdb
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file

AUDIT_VERSION = 2
MAX_PARTITION_ROWS = 200_000
MAX_PARTITION_DAYS = 31
OFFICIAL_NAMES = ("twse_daily_ohlcv.parquet", "tpex_daily_ohlcv.parquet")


class EvidenceError(ValueError):
    """Fixed public-safe code; provider exceptions never enter the report."""


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EvidenceError("invalid_source_receipt")
    return value


def _identity(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _verified_file(path: Path, receipt: dict[str, Any], *, size_key: str = "size") -> dict[str, Any]:
    expected, size = receipt.get("sha256"), receipt.get(size_key)
    if (not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected)
            or not isinstance(size, int) or isinstance(size, bool) or size < 0):
        raise EvidenceError("missing_source_hash_receipt")
    before = _identity(path)
    if before[2] != size or sha256_file(path) != expected:
        raise EvidenceError("source_hash_mismatch")
    if before != _identity(path):
        raise EvidenceError("source_changed_during_audit")
    return {"path": path, "identity": before, "sha256": expected}


def _within(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or Path(relative).is_absolute():
        raise EvidenceError("invalid_receipt_path")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise EvidenceError("invalid_receipt_path")
    return path


def _official_root(repo_root: Path) -> Path:
    catalog = repo_root / "configs/data_sync/packed_datasets.json"
    if not catalog.exists():
        return (repo_root / "data_tw_public").resolve()
    matches = [item for item in _read_json(catalog).get("datasets", [])
               if isinstance(item, dict) and item.get("dataset") == "tw-public"]
    if len(matches) != 1 or not isinstance(matches[0].get("source"), str):
        raise EvidenceError("invalid_official_catalog")
    path = Path(matches[0]["source"])
    return (path if path.is_absolute() else repo_root / path).resolve()


def _compare(sponsor: Path, official: Path, expected_rows: int) -> dict[str, Any]:
    # Do not trust a falsely small JSON row count to bound the decode.
    actual_rows = pq.read_metadata(sponsor).num_rows
    if actual_rows != expected_rows:
        raise EvidenceError("source_row_count_mismatch")
    if actual_rows > MAX_PARTITION_ROWS:
        raise EvidenceError("partition_exceeds_audit_bound")
    with duckdb.connect(":memory:") as connection:
        connection.execute("SET threads=2")
        connection.execute("SET memory_limit='1GB'")
        connection.execute("SET temp_directory=''")  # Never unbounded disk spill.
        connection.execute("""
            CREATE TEMP TABLE finmind AS SELECT CAST(date AS VARCHAR) AS date,
                CAST(stock_id AS VARCHAR) AS stock_id,TRY_CAST(close AS DOUBLE) AS close,
                TRY_CAST(Trading_Volume AS DOUBLE) AS volume FROM read_parquet(?)
        """, [str(sponsor)])
        rows, days, first, last = connection.execute(
            "SELECT COUNT(*),COUNT(DISTINCT date),MIN(date),MAX(date) FROM finmind"
        ).fetchone()
        if rows != expected_rows:
            raise EvidenceError("source_row_count_mismatch")
        if rows > MAX_PARTITION_ROWS or days > MAX_PARTITION_DAYS:
            raise EvidenceError("partition_exceeds_audit_bound")
        bad_dates = connection.execute("""
            SELECT COUNT(*) FROM finmind WHERE TRY_CAST(date AS DATE) IS NULL
                OR date != CAST(TRY_CAST(date AS DATE) AS VARCHAR)
        """).fetchone()[0]
        if bad_dates:
            raise EvidenceError("invalid_source_dates")
        connection.execute("""
            CREATE TEMP TABLE official AS
            SELECT CAST(date AS VARCHAR) AS date,CAST("證券代號" AS VARCHAR) AS stock_id,
                TRY_CAST(REPLACE("收盤價", ',', '') AS DOUBLE) AS close,
                TRY_CAST(REPLACE("成交股數", ',', '') AS DOUBLE) AS volume,'TWSE' AS venue
            FROM read_parquet(?) WHERE CAST(date AS VARCHAR) IN (SELECT DISTINCT date FROM finmind)
            UNION ALL
            SELECT CAST(date AS VARCHAR),CAST("代號" AS VARCHAR),
                TRY_CAST(REPLACE("收盤", ',', '') AS DOUBLE),
                TRY_CAST(REPLACE("成交股數", ',', '') AS DOUBLE),'TPEx'
            FROM read_parquet(?) WHERE CAST(date AS VARCHAR) IN (SELECT DISTINCT date FROM finmind)
        """, [str(official / name) for name in OFFICIAL_NAMES])
        result: dict[str, Any] = {"finmind_rows": rows, "observed_days": days,
                                  "first_date": first, "last_date": last}
        for table in ("official", "finmind"):
            duplicates = connection.execute(f"""
                SELECT date,stock_id,COUNT(*) FROM {table} GROUP BY date,stock_id
                HAVING COUNT(*)>1 ORDER BY date,stock_id
            """).fetchall()
            result[table + "_duplicate_pairs"] = len(duplicates)
            result[table + "_duplicate_sample"] = [
                {"date": row[0], "stock_id": row[1], "rows": row[2]} for row in duplicates[:20]
            ]
            result[table + "_invalid_rows"] = connection.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE stock_id IS NULL OR stock_id=''
                    OR stock_id!=TRIM(stock_id) OR volume IS NULL OR NOT isfinite(volume)
                    OR volume<0 OR volume!=FLOOR(volume) OR volume>9007199254740991
            """).fetchone()[0]
        if result["official_duplicate_pairs"] or result["finmind_duplicate_pairs"]:
            return {**result, "state": "invalid_source_grain"}
        if result["official_invalid_rows"] or result["finmind_invalid_rows"]:
            return {**result, "state": "invalid_source_values"}
        connection.execute("""
            CREATE TEMP TABLE comparison AS
            SELECT COALESCE(f.date,o.date) AS date,COALESCE(f.stock_id,o.stock_id) AS stock_id,
                o.venue,f.date IS NOT NULL AS in_finmind,o.date IS NOT NULL AS in_official,
                f.close AS finmind_close,o.close AS official_close,
                f.volume::BIGINT AS finmind_volume,o.volume::BIGINT AS official_volume,
                f.close>0 AND o.close>0 AND isfinite(f.close) AND isfinite(o.close) AS close_comparable,
                f.close>0 AND o.close>0 AND isfinite(f.close) AND isfinite(o.close)
                    AND ABS(f.close-o.close)>0.005 AS close_mismatch,
                f.volume IS NOT NULL AND o.volume IS NOT NULL AND f.volume<>o.volume AS volume_mismatch
            FROM finmind f FULL OUTER JOIN official o USING(date,stock_id)
        """)
        counts = connection.execute("""
            SELECT COUNT(*) FILTER(WHERE in_finmind AND in_official),
                COUNT(*) FILTER(WHERE close_mismatch),COUNT(*) FILTER(WHERE volume_mismatch),
                COUNT(*) FILTER(WHERE in_finmind AND NOT in_official),
                COUNT(*) FILTER(WHERE in_official AND NOT in_finmind),
                COUNT(*) FILTER(WHERE in_finmind AND in_official AND NOT COALESCE(close_comparable,FALSE))
            FROM comparison
        """).fetchone()
        sample = connection.execute("""
            SELECT date,stock_id,finmind_close,official_close,finmind_volume,official_volume,
                   close_mismatch,volume_mismatch FROM comparison
            WHERE close_mismatch OR volume_mismatch ORDER BY date DESC,stock_id LIMIT 20
        """).fetchall()
        for label, condition in (("missing_in_official", "in_finmind AND NOT in_official"),
                                 ("missing_in_finmind", "in_official AND NOT in_finmind")):
            result[label + "_sample"] = [
                {"date": row[0], "stock_id": row[1]} for row in connection.execute(
                    f"SELECT date,stock_id FROM comparison WHERE {condition} ORDER BY date,stock_id LIMIT 20"
                ).fetchall()
            ]
        return {**result, "state": "compared" if counts[0] else "no_exact_symbol_date_intersection",
                "exact_symbol_date_pairs": counts[0], "close_mismatches": counts[1],
                "volume_mismatches": counts[2], "missing_in_official": counts[3],
                "missing_in_finmind": counts[4], "close_uncomparable_pairs": counts[5],
                "mismatch_sample": [dict(zip(("date", "stock_id", "finmind_close", "official_close",
                    "finmind_volume", "official_volume", "close_mismatch", "volume_mismatch"), row))
                    for row in sample]}


def audit(repo_root: Path) -> dict[str, object]:
    raw_root = repo_root / "data_finmind/sponsor"
    db_path = raw_root / "queue.sqlite3"
    output = repo_root / "artifacts/data_quality/finmind_sponsor_source_audit/latest.json"
    base: dict[str, Any] = {
        "schema_version": AUDIT_VERSION, "observed_at_utc": datetime.now(UTC).isoformat(),
        "state": "waiting_sponsor_price", "source": "FinMind TaiwanStockPrice",
        "reference": "TWSE and TPEx official daily OHLCV",
        "query_scope": "latest_completed_sponsor_partition_observed_dates_outer_join",
        "point_in_time_training_approved": False, "full_history_verified": False,
        "unit_contract": {"price": "unadjusted_TWD", "volume": "shares",
                          "market": "unique_official_TWSE_or_TPEx_symbol_date_only"},
        "interpretation": "Differences and missing keys require scope/source investigation; never automatic replacement.",
    }
    result = dict(base)
    evidence: list[dict[str, Any]] = []
    receipts: list[tuple[Path, tuple[int, ...]]] = []
    try:
        if not db_path.is_file():
            atomic_write_json(output, result)
            return result
        with sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            record = connection.execute(
                "SELECT receipt_path,partition,rows FROM tasks WHERE dataset='TaiwanStockPrice' "
                "AND data_id='' AND state='complete' AND rows>0 "
                "ORDER BY last_data_date DESC,partition DESC LIMIT 1"
            ).fetchone()
        if not record or not record[0]:
            atomic_write_json(output, result)
            return result
        receipt_path = _within(raw_root, record[0])
        receipts.append((receipt_path, _identity(receipt_path)))
        receipt = _read_json(receipt_path)
        if (receipt.get("dataset") != "TaiwanStockPrice" or receipt.get("status") != "complete"
                or receipt.get("data_id") != "" or receipt.get("partition") != record[1]
                or receipt.get("rows") != record[2]):
            raise EvidenceError("invalid_source_receipt")
        if record[2] > MAX_PARTITION_ROWS:
            raise EvidenceError("partition_exceeds_audit_bound")
        units = receipt.get("volume_units") or {}
        if units and units.get("source_volume_units", {}).get("Trading_Volume") not in (None, "shares"):
            raise EvidenceError("incompatible_source_units")
        sponsor = _within(raw_root, receipt.get("parquet_path"))
        evidence.append(_verified_file(sponsor, receipt, size_key="parquet_size_bytes"))
        official = _official_root(repo_root)
        official_receipt_path = official / "stocks/official_symbol_build_summary.json"
        receipts.append((official_receipt_path, _identity(official_receipt_path)))
        official_receipt = _read_json(official_receipt_path)
        if official_receipt.get("source") != "twse_tpex_official":
            raise EvidenceError("invalid_official_receipt")
        for name in OFFICIAL_NAMES:
            matches = [item for item in official_receipt.get("source_receipts", [])
                       if isinstance(item, dict) and item.get("name") == name]
            if len(matches) != 1:
                raise EvidenceError("missing_official_hash_receipt")
            evidence.append(_verified_file(official / name, matches[0]))
        lineage = {"finmind": evidence[0]["sha256"], "twse": evidence[1]["sha256"],
                   "tpex": evidence[2]["sha256"], "sponsor_receipt": sha256_file(receipt_path),
                   "official_receipt": sha256_file(official_receipt_path)}
        fingerprint = hashlib.sha256(json.dumps(
            {"version": AUDIT_VERSION, "lineage": lineage}, sort_keys=True
        ).encode()).hexdigest()
        try:
            previous = _read_json(output)
        except (OSError, ValueError):
            previous = {}
        if (previous.get("schema_version") == AUDIT_VERSION
                and previous.get("source_fingerprint") == fingerprint
                and previous.get("source_hashes_verified") is True
                and previous.get("state") in {"compared", "no_exact_symbol_date_intersection"}):
            result = {**previous, "observed_at_utc": base["observed_at_utc"], "comparison_reused": True}
        else:
            result.update(_compare(sponsor, official, record[2]))
            result.update(comparison_reused=False, compared_at_utc=base["observed_at_utc"])
        if any(item["identity"] != _identity(item["path"]) for item in evidence) or any(
                identity != _identity(path) for path, identity in receipts):
            raise EvidenceError("source_changed_during_audit")
        result.update(source_fingerprint=fingerprint, source_sha256=lineage,
                      source_hashes_verified=True, sponsor_partition=receipt["partition"])
    except (OSError, ValueError, TypeError, AttributeError, sqlite3.Error, duckdb.Error) as exc:
        result = {**base, "state": str(exc) if isinstance(exc, EvidenceError) else "source_evidence_unavailable",
                  "source_hashes_verified": False, "comparison_reused": False}
    atomic_write_json(output, result)
    return result


def main() -> int:
    result = audit(Path(__file__).resolve().parents[1])
    print(json.dumps({key: result.get(key) for key in
                      ("state", "exact_symbol_date_pairs", "close_mismatches", "volume_mismatches",
                       "missing_in_official", "missing_in_finmind", "source_hashes_verified")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
