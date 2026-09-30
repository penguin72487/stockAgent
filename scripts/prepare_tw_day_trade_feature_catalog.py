#!/usr/bin/env python3
"""Local, receipt-aware feature inventory; never promote raw fields to PIT inputs.

Reuse the monitor's physical field inventory, then add the current-receipt
FinLab/FinMind/economic sources it does not describe at their semantic grain.
The output is a training admission worklist, not a second acquisition system.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.export_data_acquisition_inventory import rows_from_snapshot, enrich_finmind_receipts
from stockagent.live.data_monitor_inventory import parquet_footer_stats

CONTRACT_VERSION = 2
KEY_FIELDS = frozenset({
    "date", "trade_date", "trading_date", "ts", "timestamp", "time", "datetime",
    "symbol", "stock_id", "data_id", "contract", "source_index", "source", "data_source",
    "exchange", "market", "country", "type", "name", "unit", "currency",
    "HoldingSharesLevel", "investors", "futures_id", "option_id", "expiration_date",
    "strike_price", "call_put", "SeriesCode", "TableName", "TimePeriod", "LineNumber",
    "adjustment_reference_price", "raw_ohlc_scale_factor",
})
INVENTORY_FIELDS = (
    "catalog_id", "provider", "dataset_id", "field", "role", "admission", "reason",
    "source_first", "source_last", "bounds_basis", "rows", "non_null_count", "files",
    "types", "source_path", "receipt_path", "schema_verified", "sha256_verified",
    "publication_clock", "value_vintage", "redistribution", "next_action",
)


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def contained(root: Path, relative: str) -> Path:
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts or not relative:
        raise ValueError("unsafe receipt path")
    path = (root / rel).resolve(strict=True)
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("receipt path escapes source root")
    return path


def write_csv(path: Path, rows: list[dict], fields: tuple | list | None = None) -> None:
    fields = fields or sorted({key for row in rows for key in row})
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: json.dumps(v, ensure_ascii=False, sort_keys=True)
                         if isinstance(v, (dict, list, tuple)) else v for k, v in row.items()})
    atomic_write_text(path, buffer.getvalue())


def numeric_type(dtype: str) -> bool:
    return dtype.startswith(("int", "uint", "float", "double", "decimal", "bool"))


def field_role(name: str, types: list[str]) -> str:
    if name in KEY_FIELDS or name.endswith(("_id", "_sha256", "_path", "_issue", "_warning")):
        return "key_or_provenance"
    return "numeric_candidate" if types and all(numeric_type(t) for t in types) else "text_or_unparsed"


def monitor_rows(payload: dict) -> list[dict]:
    rows = []
    for item in payload["rows"]:
        # FinLab wide columns are securities/dimensions, not independent ML
        # features. Preserve them in physical_fields.csv, but catalogue keys
        # separately from their authoritative current receipts below.
        if item["dataset_id"] == "physical:finlab:downloaded-datasets":
            continue
        role = field_role(item["field"], item["types"])
        admission = "needs_adapter_and_pit_evidence"
        reason = "physical_schema_is_not_feature_timing_or_vintage_proof"
        if role != "numeric_candidate":
            admission, reason = "not_model_input", "identifier_text_or_provenance_not_numeric_signal"
        elif item.get("schema_state") != "verified":
            admission, reason = "incomplete_source", "physical_inventory_partial"
        elif item.get("non_null_count") == 0:
            admission, reason = "missing_values", "all_inspected_values_null_not_entire_provider"
            if item["dataset_id"] == "physical:openbb:archives":
                admission, reason = "needs_current_l1_check", "legacy_compact_does_not_cover_newer_l1"
        rows.append({
            "catalog_id": f"{item['dataset_id']}::{item['field']}",
            "provider": item["provider"], "dataset_id": item["dataset_id"],
            "field": item["field"], "role": role, "admission": admission,
            "reason": reason, "source_first": item.get("dataset_first"),
            "source_last": item.get("dataset_last"),
            "bounds_basis": "dataset_bounds_not_feature_non_null_or_publication_bounds",
            "rows": item.get("rows_with_field"), "non_null_count": item.get("non_null_count"),
            "files": item.get("files_with_field"), "types": item.get("types"),
            "source_path": "artifacts/live/data_monitor/feature_inventory.json",
            "schema_verified": item.get("schema_state") == "verified", "sha256_verified": False,
            "publication_clock": "not_certified_by_schema", "value_vintage": "not_certified_by_schema",
            "next_action": "use_admitted_model_feature_catalog_or_add_dated_mapping_and_tests",
        })
    return rows


def finlab_rows(root: Path) -> tuple[list[dict], list[dict]]:
    rows, failures = [], []
    for receipt_path in sorted((root / "receipts").glob("*.json")):
        receipt = read_json(receipt_path)
        name = str(receipt.get("dataset") or receipt_path.stem)
        item = {
            "catalog_id": f"finlab:{name}", "provider": "FinLab", "dataset_id": name,
            "field": name, "role": "source_measure_with_instrument_or_dimension_axis",
            "source_first": receipt.get("first_non_null_source_index"),
            "source_last": receipt.get("last_non_null_source_index"),
            "bounds_basis": receipt.get("index_semantics", "unknown"),
            "rows": receipt.get("rows"), "files": 1, "receipt_path": str(receipt_path),
            "sha256_verified": False, "publication_clock": receipt.get("publication_time_status"),
            "value_vintage": "current_provider_revision_not_original_release_vintage",
            "redistribution": receipt.get("redistribution"),
            "admission": "research_only_needs_adapter", "reason": "no_historical_pit_receipt",
            "next_action": "preserve_dimension_axis; map_actual_announcement_time_and_original_version",
        }
        try:
            path = contained(root, str(receipt.get("parquet_path") or ""))
            stats = parquet_footer_stats(path)
            if not stats or stats["count"] != receipt.get("rows"):
                raise ValueError("missing_schema_or_receipt_row_mismatch")
            item.update(source_path=str(path), schema_verified=True,
                        types=sorted({t for _, t in stats["fields"]}),
                        source_value_columns=len([f for f, _ in stats["fields"] if f != "source_index"]))
            value_stats = [count for (field, _), count in zip(stats["fields"], stats["non_null"], strict=True)
                           if field != "source_index"]
            item["non_null_count"] = sum(value_stats) if all(c is not None for c in value_stats) else None
            if item["non_null_count"] == 0:
                item.update(admission="missing_values", reason="all_stored_values_null")
            elif not any(numeric_type(t) for f, t in stats["fields"] if f not in KEY_FIELDS):
                item.update(admission="not_model_input", reason="text_event_or_metadata_requires_explicit_encoding")
        except (OSError, ValueError) as exc:
            item.update(admission="incomplete_source", reason=str(exc), schema_verified=False)
            failures.append({"provider": "FinLab", "dataset": name, "error": str(exc)})
        rows.append(item)
    return rows, failures


def finmind_tasks(root: Path) -> tuple[list[tuple[Path, dict]], list[dict]]:
    """Read one SQLite snapshot per lane, not every old content-addressed version."""
    tasks, counts = [], []
    for lane in ("sponsor", "complement"):
        base = root / lane
        db = base / "queue.sqlite3"
        if not db.exists():
            continue
        with sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            counts.extend({"lane": lane, **dict(row)} for row in conn.execute(
                "SELECT dataset,state,COUNT(*) AS partitions,SUM(rows) AS receipt_rows "
                "FROM tasks GROUP BY dataset,state ORDER BY dataset,state"))
            tasks.extend((base, dict(row)) for row in conn.execute(
                "SELECT dataset,data_id,partition,rows,receipt_path FROM tasks "
                "WHERE state='complete' AND rows>0 ORDER BY dataset,data_id,partition"))
    # Free-lane date partitions have no shared SQLite tasks. Current receipts
    # select one version; never sum versions/ or retries into history coverage.
    for receipt_path in sorted((root / "receipts").glob("*/*.json")):
        receipt = read_json(receipt_path)
        if receipt.get("parquet_path") and receipt.get("rows", 0) > 0:
            tasks.append((root, {"dataset": receipt.get("dataset", receipt_path.parent.name),
                                "receipt_path": str(receipt_path.relative_to(root)),
                                "rows": receipt["rows"]}))
    for receipt_path in sorted((root / "futures_intraday").glob("*/*/*/receipt.json")):
        receipt = read_json(receipt_path)
        if receipt.get("source") == "finmind_futures_tick_contract_day_v1" and receipt.get("rows", 0) > 0:
            tasks.append((root, {"dataset": receipt.get("query", {}).get("dataset", "TaiwanFuturesTick"),
                                "receipt_path": str(receipt_path.relative_to(root)), "rows": receipt["rows"]}))
    return tasks, counts


def inspect_task(task: tuple[Path, dict]) -> dict:
    base, record = task
    output = {"provider": "FinMind", "dataset": record["dataset"], "lane": base.name,
              "receipt_path": str(base / str(record.get("receipt_path") or "")),
              "data_id": record.get("data_id"), "partition": record.get("partition")}
    try:
        receipt_path = contained(base, str(record["receipt_path"] or ""))
        receipt = read_json(receipt_path)
        relative = receipt.get("parquet_path") or ""
        if not relative and receipt.get("source") == "finmind_futures_tick_contract_day_v1":
            relative = str((receipt_path.parent / "data.parquet").relative_to(base.resolve()))
        path = contained(base, str(relative))
        before = path.stat()
        output.update(source_path=str(path), bytes=before.st_size)
        stats = parquet_footer_stats(path)
        after = path.stat()
        output.update(queue_rows=record.get("rows"), receipt_rows=receipt.get("rows"),
                      actual_rows=stats["count"] if stats else None)
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("source_changed_during_schema_read")
        if before.st_size == 0:
            raise ValueError("zero_byte_parquet_despite_complete_receipt")
        if not stats or stats["count"] != record["rows"]:
            raise ValueError("missing_schema_or_receipt_row_mismatch")
        output.update(stats=stats, source_path=str(path), receipt_path=str(receipt_path),
                      declared_sha256=receipt.get("sha256"), bytes=after.st_size,
                      first=receipt.get("source_first_date") or receipt.get("date"),
                      last=receipt.get("source_last_date") or receipt.get("date"))
    except (OSError, ValueError, KeyError) as exc:
        output["error"] = str(exc)
    return output


def aggregate_fields(records: list[dict]) -> list[dict]:
    groups: dict[tuple, dict] = {}
    failed = {(r["provider"], r["lane"], r["dataset"]) for r in records if r.get("error")}
    for record in records:
        if record.get("error"):
            continue
        stats = record["stats"]
        for (name, dtype), non_null in zip(stats["fields"], stats["non_null"], strict=True):
            key = (record["provider"], record["lane"], record["dataset"], name)
            group = groups.setdefault(key, {"rows": 0, "non_null_count": 0, "types": set(),
                                           "files": 0, "first": [], "last": [],
                                           "source_path": record["source_path"]})
            group["rows"] += stats["count"]
            group["files"] += 1
            group["types"].add(dtype)
            group["non_null_count"] = (None if non_null is None or group["non_null_count"] is None
                                       else group["non_null_count"] + non_null)
            if record.get("first"): group["first"].append(str(record["first"]))
            if record.get("last"): group["last"].append(str(record["last"]))
    result = []
    for (provider, lane, dataset, name), group in sorted(groups.items()):
        types = sorted(group["types"])
        role = field_role(name, types)
        admission = "research_only_needs_adapter" if role == "numeric_candidate" else "not_model_input"
        if group["non_null_count"] == 0 and role == "numeric_candidate": admission = "missing_values"
        partial = (provider, lane, dataset) in failed
        if partial: admission = "incomplete_source"
        result.append({
            "catalog_id": f"{provider}:{lane}:{dataset}::{name}", "provider": provider,
            "dataset_id": f"{lane}:{dataset}", "field": name, "role": role,
            "admission": admission,
            "reason": "some_current_receipts_failed_validation" if partial else "publication_and_historical_value_vintage_unverified",
            "source_first": min(group["first"], default=None),
            "source_last": max(group["last"], default=None),
            "bounds_basis": "receipt_dataset_bounds_not_field_publication_time",
            "rows": group["rows"], "non_null_count": group["non_null_count"], "files": group["files"],
            "types": types, "source_path": group["source_path"], "schema_verified": not partial,
            "sha256_verified": False, "publication_clock": "unverified",
            "value_vintage": "historical_point_in_time_false_in_source_receipts",
            "next_action": "reconcile_official_overlap; map_entity_units_release_clock; preserve_missingness",
        })
    return result


def economic_records(root: Path) -> list[dict]:
    """Use only normalized files selected by current acquisition receipts."""
    records = []
    for receipt_path in sorted((root / "receipts").glob("*/*.json")):
        receipt = read_json(receipt_path)
        for item in receipt.get("files", []):
            if item.get("kind") != "parquet":
                continue
            record = {"provider": str(receipt.get("provider", receipt_path.parent.name)),
                      "lane": "public_economic", "dataset": receipt.get("dataset", receipt_path.stem),
                      "receipt_path": str(receipt_path)}
            try:
                path = contained(root, str(item.get("path") or ""))
                stats = parquet_footer_stats(path)
                if not stats or (item.get("rows") is not None and stats["count"] != item["rows"]):
                    raise ValueError("missing_schema_or_receipt_row_mismatch")
                record.update(stats=stats, source_path=str(path),
                              first=receipt.get("first_observation"), last=receipt.get("last_observation"),
                              declared_sha256=item.get("sha256"), bytes=path.stat().st_size)
            except (OSError, ValueError) as exc:
                record["error"] = str(exc)
            records.append(record)
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--recheck-failed-datasets", action="store_true")
    args = parser.parse_args(argv)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    if args.recheck_failed_datasets:
        with (out / "source_failures.csv").open(newline="") as handle:
            failed = {(r["lane"], r["dataset"]) for r in csv.DictReader(handle) if r["provider"] == "FinMind"}
        tasks = []
        for lane, dataset in sorted(failed):
            if lane not in {"sponsor", "complement"}:
                raise ValueError("failed lane needs its canonical non-queue receipt adapter")
            base = args.root / "data_finmind" / lane
            with sqlite3.connect(f"file:{(base / 'queue.sqlite3').resolve()}?mode=ro", uri=True) as conn:
                conn.row_factory = sqlite3.Row
                tasks.extend((base, dict(row)) for row in conn.execute(
                    "SELECT dataset,data_id,partition,rows,receipt_path FROM tasks WHERE dataset=? AND state='complete' AND rows>0",
                    (dataset,)))
        with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 8))) as executor:
            results = list(executor.map(inspect_task, tasks))
        errors = [{k: v for k, v in r.items() if k != "stats"} for r in results if r.get("error")]
        write_csv(out / "source_failure_recheck.csv", errors,
                  ["provider", "lane", "dataset", "data_id", "partition", "receipt_path", "source_path", "bytes", "queue_rows", "receipt_rows", "actual_rows", "error"])
        payload = {"checked_at_utc": datetime.now(UTC).isoformat(), "datasets": sorted(failed),
                   "current_complete_partitions_inspected": len(results), "failures": len(errors),
                   "scope": "read_only_recheck; does_not_erase_original_inventory_snapshot"}
        atomic_write_json(out / "source_failure_recheck.json", payload)
        print(json.dumps(payload))
        return 0
    started = datetime.now(UTC).isoformat()
    feature_path = args.root / "artifacts/live/data_monitor/feature_inventory.json"
    status_path = args.root / "artifacts/live/data_monitor/public_status.json"
    feature_bytes, status_bytes = feature_path.read_bytes(), status_path.read_bytes()
    physical, status = json.loads(feature_bytes), json.loads(status_bytes)
    atomic_write_text(out / "monitor_features.input.json", feature_bytes.decode())
    atomic_write_text(out / "monitor_sources.input.json", status_bytes.decode())
    write_csv(out / "physical_fields.csv", physical["rows"])
    source_rows = rows_from_snapshot(status)
    enrich_finmind_receipts(source_rows, args.root)
    write_csv(out / "source_inventory.csv", source_rows)
    candidates = monitor_rows(physical)
    print(f"[feature-catalog] monitor_fields={len(physical['rows'])} sources={len(source_rows)}", flush=True)
    finlab, failures = finlab_rows(args.root / "data_finlab")
    candidates.extend(finlab)
    write_csv(out / "finlab_keys.csv", finlab)
    print(f"[feature-catalog] finlab_keys={len(finlab)}", flush=True)
    tasks, queue = finmind_tasks(args.root / "data_finmind")
    write_csv(out / "finmind_partition_states.csv", queue)
    inspected = []
    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 8))) as executor:
        # Bound queued Futures rather than constructing one per history file.
        for start in range(0, len(tasks), 512):
            inspected.extend(executor.map(inspect_task, tasks[start:start + 512]))
            if start % 5120 == 0:
                print(f"[feature-catalog] finmind_receipts={len(inspected)}/{len(tasks)}", flush=True)
    candidates.extend(aggregate_fields(inspected))
    economic = economic_records(args.root / "data_public_economic")
    candidates.extend(aggregate_fields(economic))
    failures.extend({k: v for k, v in r.items() if k != "stats"}
                    for r in inspected + economic if r.get("error"))
    write_csv(out / "finmind_source_files.csv", [{k: v for k, v in r.items() if k != "stats"}
                                                for r in inspected])
    write_csv(out / "economic_source_files.csv", [{k: v for k, v in r.items() if k != "stats"}
                                                 for r in economic])
    write_csv(out / "feature_candidates.csv", candidates, INVENTORY_FIELDS)
    write_csv(out / "source_failures.csv", failures, ["provider", "lane", "dataset", "receipt_path", "data_id", "partition", "error"])
    summary = {
        "contract_version": CONTRACT_VERSION, "started_at_utc": started,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "monitor_generated_at_utc": physical.get("generated_at_utc"),
        "monitor_schema_coverage": physical["summary"], "registered_sources": len(source_rows),
        "physical_field_rows": len(physical["rows"]), "finlab_source_keys": len(finlab),
        "finmind_current_receipts_inspected": len(inspected),
        "economic_current_files_inspected": len(economic),
        "source_failure_count": len(failures), "candidate_catalog_rows": len(candidates),
        "admission_counts": dict(Counter(r["admission"] for r in candidates)),
        "providers": dict(Counter(r["provider"] for r in candidates)),
        "input_sha256": {"monitor_features.input.json": hashlib.sha256(feature_bytes).hexdigest(),
                         "monitor_sources.input.json": hashlib.sha256(status_bytes).hexdigest()},
        "scope": "local_registered_sources_and_current_FinLab_FinMind_receipts",
        "limitations": [
            "catalog_rows_are_not_independent_model_dimensions",
            "schema_and_receipt_inventory_is_not_full_byte_or_historical_PIT_validation",
            "active_downloaders_continue; queue snapshots_and_failed_concurrent_reads_are_explicit",
            "source_bounds_are_not_field_validity_or_publication_bounds",
            "no_download_no_broker_call_no_remote_sync_no_training",
        ],
    }
    atomic_write_json(out / "catalog_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
