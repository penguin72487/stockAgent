#!/usr/bin/env python3
"""Read-only bounded audit of the priority repair scope, never a full-provider claim.

Reads small receipts and exact OpenBB task primary keys. It does not call a
provider, load credentials or scan the whole manifest. Optional --validate-macro
checks Census/BEA hashes and values with bounded Arrow batches.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import io
import json
from pathlib import Path
import sqlite3
import subprocess

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from scripts.queue_openbb_public_priority import footer_evidence


def read(path: Path) -> dict:
    return json.loads(path.read_text()) if path.is_file() else {}


def economic_value_audit(root: Path, receipt: dict) -> dict:
    """Bounded column batches, not one huge pandas materialization."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    rows = numeric = nulls = 0
    for item in receipt.get("files", []):
        path = root / item["path"]
        if not path.resolve().is_relative_to(root.resolve()) or sha256_file(path) != item["sha256"]:
            raise ValueError("economic source checksum/path mismatch")
        if item.get("kind") != "parquet":
            continue
        for batch in pq.ParquetFile(path).iter_batches(columns=[receipt["value_column"]],
                                                       batch_size=65536, use_threads=False):
            values = batch.column(0)
            rows += len(values)
            nulls += values.null_count
            cleaned = pc.replace_substring(values, ",", "")
            valid = pc.match_substring_regex(cleaned, r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$")
            numeric += pc.sum(pc.cast(valid, pa.int64())).as_py() or 0
    if rows != receipt.get("rows"):
        raise ValueError("economic decoded row count mismatch")
    return {"sha256_verified": True, "decoded_rows": rows, "numeric_value_rows": numeric,
            "null_value_rows": nulls, "suppressed_or_other_text_rows": rows - numeric - nulls,
            "scope": "numeric text/null/suppression and checksums; not historical PIT or missing-calendar proof"}


def finlab_repair_evidence(root: Path) -> list[dict]:
    from scripts.download_finlab_history import safe_stem
    keys = ["broker_transactions", "after_market_fixed_price:市場別", "after_market_fixed_price:資料來源",
            "dividend_otc:權息", "management_change_events:變更交易開始日"]
    records = []
    for key in keys:
        stem = safe_stem(key)
        receipt = read(root / f"data_finlab/receipts/{stem}.json")
        attempt = read(root / f"data_finlab/attempts/{stem}.json")
        records.append({"dataset": key, "receipt_present": bool(receipt),
                        **{k: receipt.get(k) for k in ("rows", "source_rows", "field_columns", "first_event_at",
                            "last_event_at", "source_check_mode", "source_checked_at_utc", "storage_layout",
                            "parquet_size_bytes", "raw_bytes", "last_fetch_elapsed_seconds")},
                        "last_attempt_status": attempt.get("status"),
                        "raw_non_null_values": attempt.get("raw_non_null_values"),
                        "empty_evidence_path": attempt.get("empty_evidence_path")})
    return records


def capture(root: Path, *, validate_macro: bool = False) -> dict:
    economic = []
    locations = [(root / "data_public_economic", read(root / "data_public_economic/download_summary.json").get("providers", [])),
                 (root / "data_forex_frankfurter/official_v2", [read(root / "data_forex_frankfurter/official_v2/download_summary.json")])]
    for folder, providers in locations:
        for provider in providers:
            records, files = [], {}
            for item in provider.get("datasets", []):
                receipt = read(folder / "receipts" / item["provider"] / f"{item['dataset']}.json")
                row = {k: receipt.get(k) for k in ("rows", "first_observation", "last_observation", "observed_at_utc",
                          "rejected_rows", "suspicious_transaction_dates", "invalid_transaction_dates", "source_empty")}
                row.update(provider=item["provider"], dataset=item["dataset"], status=item["status"],
                           receipt_present=bool(receipt), history_complete=False)
                if validate_macro and item["provider"] in {"census", "bea"} and receipt:
                    row["value_quality"] = economic_value_audit(folder, receipt)
                for file in receipt.get("files", []):
                    files[file["path"]] = file["bytes"]
                records.append(row)
            economic.append({"provider": provider.get("provider"), "datasets": records,
                             "states": dict(Counter(r["status"] for r in records)),
                             "receipt_count": sum(r["receipt_present"] for r in records),
                             "rows_including_child_tables": sum(r.get("rows") or 0 for r in records),
                             "rejected_rows": sum(r.get("rejected_rows") or 0 for r in records),
                             "suspicious_transaction_dates": sum(r.get("suspicious_transaction_dates") or 0 for r in records),
                             "referenced_bytes": sum(files.values()), "unique_referenced_files": len(files),
                             "validation": "existing receipt statistics; not fresh full-content validation"})
    archive = root / "data_openBB"
    plan = read(archive / "_state/public_priority_plan.json")
    cursors = read(archive / "_state/public_priority_cursors.json")
    tasks = []
    connection = sqlite3.connect(f"file:{archive}/_state/openbb_archive.sqlite3?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        for entry in plan.get("datasets", []):
            if not entry.get("task_id"):
                continue
            ids = [("initial", entry["task_id"])]
            if entry["task_id"] in cursors:
                ids.append(("incremental", cursors[entry["task_id"]]["task_id"]))
            for kind, task_id in ids:
                row = connection.execute("SELECT status,active,rows,output_path,selected_provider FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                if row is None:
                    tasks.append({"task_id": task_id, "status": "missing", "kind": kind})
                    continue
                item = {"task_id": task_id, "endpoint": entry["endpoint"], "scope": entry["scope"], "kind": kind, **dict(row)}
                if row["status"] == "success":
                    path = Path(row["output_path"])
                    item["stored"] = footer_evidence(path if path.is_absolute() else root / path, int(row["rows"]))
                tasks.append(item)
    finally:
        connection.close()
    units = ["stockagent-openbb-archive.service", "stockagent-public-economic-history.timer", "stockagent-openbb-public-priority.timer"]
    service = subprocess.run(["systemctl", "show", *units, "--property=Id,ActiveState,SubState"], capture_output=True, text=True, timeout=10)
    return {"observed_at_utc": datetime.now(UTC).isoformat(), "provider_network_calls": 0,
            "economic": economic, "openbb_tasks": tasks,
            "openbb_counts": dict(Counter(f"{t['kind']}:{t['status']}" for t in tasks)),
            "services": service.stdout, "history_complete": False, "historical_point_in_time": False,
            "finlab_repair_scope": finlab_repair_evidence(root),
            "eodhd_quota": read(root / "artifacts/credentials/eodhd_quota.json"),
            "scope": "Only explicitly registered priority tasks; overlapping shards are not unique observations"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/data_quality/provider_repair_2026-09-27"))
    parser.add_argument("--validate-macro", action="store_true", help="Verify Census/BEA checksums and value coverage in bounded batches")
    args = parser.parse_args()
    payload = capture(args.root, validate_macro=args.validate_macro)
    name = datetime.now(UTC).strftime("verification_%Y%m%dT%H%M%SZ")
    atomic_write_json(args.output_dir / f"{name}.json", payload)
    records = [item for p in payload["economic"] for item in p["datasets"]]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(dict.fromkeys(key for row in records for key in row)))
    writer.writeheader()
    writer.writerows(records)
    atomic_write_bytes(args.output_dir / f"{name}.csv", buffer.getvalue().encode("utf-8-sig"))
    print(json.dumps({"report": str(args.output_dir / f"{name}.json"), "openbb_counts": payload["openbb_counts"],
                      "economic": [{k: p[k] for k in ("provider", "receipt_count", "rows_including_child_tables", "rejected_rows", "referenced_bytes")} for p in payload["economic"]]}))


if __name__ == "__main__":
    main()
