#!/usr/bin/env python3
"""Audit omitted futures' first observations against retained TEJ snapshots.

Current vendor snapshots are retrospective evidence, not point-in-time listing
permissions, executable prices, or a training admission. Original preview
cells, native receipts, and materialized rows must agree before counting them.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sqlite3

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


TABLE_ID = "1bea200ec21f8f1a39056861"
TABLE_FIELDS = ("Future Code", "Listed Date", "Expiration Moon", "Trade Date_End")
NO_VALUE = {"", "(null)", "null", "none", "-"}


def safe_source(root: Path, relative: str) -> Path:
    path = Path(relative)
    resolved = (root / path).resolve()
    if path.is_absolute() or not resolved.is_relative_to(root.resolve()) or not resolved.is_file():
        raise ValueError("source path is absent or escapes its owner")
    return resolved


def parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if text.lower() in NO_VALUE:
        return None
    try:
        return date.fromisoformat(text.replace("/", "-"))
    except ValueError:
        return None


def snapshot_rows(receipt: dict, raw: dict, frame: pl.DataFrame) -> list[dict]:
    """Prove the compared vendor cells against the retained preview grid."""
    if (receipt.get("provider") != "tej_smart_wizard"
            or receipt.get("table_id") != TABLE_ID
            or receipt.get("source_values_retained") is not True
            or receipt.get("date_axis") != "observed_source_snapshot_no_historical_date_claim"
            or receipt.get("row_grain") != "native_company_current_snapshot_not_historical_observation"
            or raw.get("task_id") != receipt.get("task_id")
            or raw.get("table") != "Future Attribute"):
        raise ValueError("unsupported native snapshot identity")
    if receipt.get("source_outcome") == "explicit_empty_scope":
        expected = {"capture_method": "owned_vendor_empty_dialog",
                    "source_outcome": "explicit_empty_scope",
                    "empty_acknowledgement_contract": "normal_ok_control_notification_no_foreground_v1"}
        ownership = receipt.get("empty_dialog_ownership_contract")
        if (receipt.get("exported_rows") != 0 or frame.height != 0
                or ownership not in {"exact_owned_modal_v1", "unique_same_thread_ownerless_modal_v1"}
                or raw.get("empty_dialog_ownership_contract") != ownership
                or any(receipt.get(key) != value or raw.get(key) != value for key, value in expected.items())):
            raise ValueError("unverified empty snapshot scope")
        return []
    if raw.get("capture_method") != "native_msaa_preview_full":
        raise ValueError("unsupported native snapshot capture")
    cells = raw.get("cells")
    if not isinstance(cells, list) or not cells or not isinstance(cells[0], list):
        raise ValueError("missing source preview")
    header = cells[0]
    if len(set(header)) != len(header) or any(field not in header for field in TABLE_FIELDS):
        raise ValueError("ambiguous source headers")
    if (len(cells) - 1 != receipt.get("exported_rows")
            or frame.height != receipt.get("exported_rows")
            or not set(TABLE_FIELDS + ("_query_symbol", "_snapshot_observed_at_utc")).issubset(frame.columns)):
        raise ValueError("native row count or columns disagree")
    native = {}
    for values in cells[1:]:
        if not isinstance(values, list) or len(values) != len(header):
            raise ValueError("invalid source grid row")
        row = dict(zip(header, values, strict=True))
        key = str(row["Future Code"]).split(maxsplit=1)[0]
        if not key or key in native:
            raise ValueError("ambiguous native contract key")
        native[key] = row
    output = []
    seen = set()
    for row in frame.select(*TABLE_FIELDS, "_query_symbol", "_snapshot_observed_at_utc").iter_rows(named=True):
        key = row["_query_symbol"]
        if (key in seen or key not in native
                or row["_snapshot_observed_at_utc"] != receipt.get("observed_at_utc")
                or any(row[field] != native[key][field] for field in TABLE_FIELDS)):
            raise ValueError("original preview and parquet differ")
        seen.add(key)
        output.append({"symbol": key, "listed": parse_date(row["Listed Date"]),
                       "month": str(row["Expiration Moon"]).strip(),
                       "trade_end": parse_date(row["Trade Date_End"]),
                       "task_id": receipt["task_id"]})
    if seen != set(native):
        raise ValueError("source preview has unmatched rows")
    return output


def compare_first_observation(row: dict, values: list[dict]) -> str:
    """Keep missing, conflicting, and after-observation claims distinct."""
    if not values:
        return "vendor_symbol_not_observed"
    dates = {entry["listed"] for entry in values}
    months = {entry["month"] for entry in values}
    if len(dates) != 1 or len(months) != 1:
        return "vendor_snapshot_conflict"
    listed = next(iter(dates))
    expected_month = row["contract"][:4] + "/" + row["contract"][4:]
    if listed is None or months != {expected_month}:
        return "vendor_date_or_month_unknown"
    if listed == row["date"]:
        return "listed_on_first_observation"
    if listed < row["date"]:
        return "listed_before_first_observation"
    return "listed_after_first_observation_conflict"


def completed_tasks(db: sqlite3.Connection) -> list[tuple]:
    return db.execute("SELECT task_id, receipt_path, output_path, actual_rows FROM tasks "
                      "WHERE table_id=? AND kind='download' AND state='complete' ORDER BY task_id",
                      (TABLE_ID,)).fetchall()


def audit(workspace: Path, candidates: Path, candidate_report: Path, report: Path) -> dict:
    workspace = workspace.resolve()
    candidates, candidate_report, report = candidates.resolve(), candidate_report.resolve(), report.resolve()
    if (not candidates.is_relative_to(workspace / "data_tw_index_futures")
            or not candidate_report.is_relative_to(workspace / "artifacts/markets")
            or not report.is_relative_to(workspace / "artifacts/markets")
            or report.exists() or report.is_symlink()):
        raise ValueError("require existing workspace candidates and a new market report")
    source_report = json.loads(candidate_report.read_bytes())
    candidate_sha = sha256_file(candidates)
    if (source_report.get("contract") != "taifex_stock_future_2009_manual_schedule_candidates_v1"
            or source_report.get("data_sha256") != candidate_sha):
        raise ValueError("first-observation input differs from its report")
    panel = pl.read_parquet(candidates)
    if panel.is_empty() or panel.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("first-observation axis is missing or duplicated")
    tej = (workspace / "data_tej").resolve()
    db_path = tej / "queue.sqlite3"
    uri = db_path.as_uri() + "?mode=ro"
    values: dict[str, list[dict]] = defaultdict(list)
    pins, errors, empty_count = {}, [], 0
    with sqlite3.connect(uri, uri=True, timeout=10) as db:
        before = completed_tasks(db)
        for task_id, receipt_name, raw_name, expected_rows in before:
            try:
                receipt_path = safe_source(tej, receipt_name)
                receipt_sha = sha256_file(receipt_path)
                receipt = json.loads(receipt_path.read_bytes())
                if (receipt.get("task_id") != task_id or receipt.get("raw_path") != raw_name
                        or receipt.get("exported_rows") != expected_rows):
                    raise ValueError("database and receipt identity disagree")
                raw_path = safe_source(tej, receipt["raw_path"])
                parquet_path = safe_source(tej, receipt["parquet_path"])
                current = {receipt_path: receipt_sha, raw_path: sha256_file(raw_path),
                           parquet_path: sha256_file(parquet_path)}
                if (current[raw_path] != receipt.get("raw_sha256")
                        or current[parquet_path] != receipt.get("parquet_sha256")):
                    raise ValueError("native file digest disagrees with receipt")
                raw = json.loads(raw_path.read_bytes())
                rows = snapshot_rows(receipt, raw, pl.read_parquet(parquet_path))
                if any(sha256_file(path) != digest for path, digest in current.items()):
                    raise ValueError("native files changed during read")
                if receipt.get("source_outcome") == "explicit_empty_scope":
                    empty_count += 1
                pins.update({str(path.relative_to(workspace)): digest for path, digest in current.items()})
                for row in rows:
                    values[row["symbol"]].append(row)
            except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError, pl.exceptions.PolarsError) as exc:
                errors.append({"task_id": task_id, "error": str(exc)})
        after = completed_tasks(db)
    if before != after or sha256_file(candidates) != candidate_sha:
        raise ValueError("source task inventory or candidate input changed during audit")
    counts, manual_counts, positive = Counter(), Counter(), {"0845": Counter(), "0900": Counter()}
    samples = []
    for row in panel.iter_rows(named=True):
        symbol = row["product"] + row["contract"]
        result = compare_first_observation(row, values.get(symbol, []))
        counts[result] += 1
        if row["state"] == "matches_2009_manual_schedule_candidate":
            manual_counts[result] += 1
        for clock in positive:
            if (row["source_status"] == "raw_receipt_matched"
                    and row[f"{clock}_raw_print_rows"] > 0 and row[f"{clock}_invalid_raw_rows"] == 0):
                positive[clock][result] += 1
        if result == "listed_after_first_observation_conflict" and len(samples) < 10:
            samples.append({"date": row["date"].isoformat(), "symbol": symbol,
                            "vendor_task_ids": sorted({v["task_id"] for v in values[symbol]})})
    result = {
        "contract": "tw_futures_tej_first_observation_snapshot_audit_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "candidate_rows": panel.height, "vendor_tasks_complete": len(before),
        "vendor_tasks_verified": len(before) - len(errors), "vendor_tasks_rejected": errors,
        "vendor_verified_explicit_empty_tasks": empty_count,
        "vendor_verified_symbols": len(values), "comparison_counts": dict(sorted(counts.items())),
        "manual_schedule_candidate_counts": dict(sorted(manual_counts.items())),
        "positive_raw_window_counts": {key: dict(sorted(value.items())) for key, value in positive.items()},
        "later_listing_conflict_samples": samples,
        "input_sha256": {str(candidates.relative_to(workspace)): candidate_sha,
                         str(candidate_report.relative_to(workspace)): sha256_file(candidate_report),
                         str(Path(__file__).resolve().relative_to(workspace)): sha256_file(Path(__file__)),
                         **pins},
        "scope": "Retrospective native vendor snapshots compared with first official observed trades. "
                 "Not a historical listing announcement, fill, position capacity, or model input.",
        "historical_point_in_time_verified": False, "financial_or_training_auto_adopted": False,
    }
    atomic_write_json(report, result)
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workspace", type=Path, default=Path.cwd())
    p.add_argument("--candidates", type=Path, required=True)
    p.add_argument("--candidate-report", type=Path, required=True)
    p.add_argument("--report", type=Path, required=True)
    args = p.parse_args()
    result = audit(args.workspace, args.candidates, args.candidate_report, args.report)
    print(json.dumps({key: result[key] for key in ("candidate_rows", "vendor_tasks_complete",
        "vendor_tasks_verified", "vendor_tasks_rejected", "comparison_counts",
        "manual_schedule_candidate_counts", "positive_raw_window_counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
