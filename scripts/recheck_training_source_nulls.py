#!/usr/bin/env python3
"""Recheck the prior null-field finding against source receipts and newer L1.

This is a scoped follow-up, not a whole OpenBB archive or historical-PIT audit.
It never downloads, substitutes providers, changes source bytes, or fills nulls.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.compute as pc

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.parquet_integrity import parquet_receipt_error, signature
from scripts.build_tw_feature_admission_bundle import csv_rows
from scripts.prepare_tw_day_trade_feature_catalog import (
    aggregate_fields, contained, field_role, inspect_task, read_json, write_csv,
)
from stockagent.live.data_monitor_inventory import parquet_footer_stats


def recheck_openbb(root: Path, fields: set[str]) -> tuple[list[dict], list[dict]]:
    manifest = root / "_state/openbb_archive.sqlite3"
    totals, files, rows = Counter(), Counter(), Counter()
    evidence = []
    with sqlite3.connect(f"file:{manifest.resolve()}?mode=ro", uri=True) as conn:
        segments = conn.execute(
            "SELECT endpoint,output_path,output_rows,output_bytes FROM l1_compaction_segments "
            "WHERE status='success' AND endpoint IN ('cftc.cot','currency.price.historical') "
            "ORDER BY endpoint,output_path").fetchall()
    for endpoint, name, expected_rows, expected_bytes in segments:
        path = Path(name)
        if path.is_symlink() or not path.resolve().is_relative_to((root / "compact_l1").resolve()):
            raise ValueError("L1 path outside compact_l1")
        before = signature(path)
        stats = parquet_footer_stats(path)
        if not stats or stats["count"] != expected_rows or before[2] != expected_bytes:
            raise ValueError("L1 metadata/receipt mismatch")
        digest = sha256_file(path)
        if signature(path) != before:
            raise ValueError("L1 changed during recheck")
        evidence.append({"endpoint": endpoint, "path": str(path), "rows": expected_rows,
                         "bytes": expected_bytes, "observed_sha256": digest})
        for (name, _), count in zip(stats["fields"], stats["non_null"], strict=True):
            if name in fields:
                if count is None:
                    raise ValueError("L1 null-count statistics unavailable")
                totals[name] += count
                rows[name] += stats["count"]
                files[name] += 1
    return [{"field": name, "non_null_count": totals[name], "rows_with_field": rows[name],
             "files_with_field": files[name],
             "decision": "observed_in_newer_l1" if totals[name] else "no_observation_in_inspected_l1",
             "scope": "manifest_success_l1_cftc_cot_and_currency_price_only_not_entire_provider"}
            for name in sorted(fields)], evidence


def recheck_finlab(root: Path) -> dict:
    checked, errors = 0, []
    for path in sorted((root / "receipts").glob("*.json")):
        receipt = read_json(path)
        error = parquet_receipt_error(root, receipt)
        if error:
            errors.append({"dataset": receipt.get("dataset"), "error": error, "receipt_path": str(path)})
        checked += 1
    upstream_empty = []
    for path in sorted((root / "attempts").glob("*.json")):
        attempt = read_json(path)
        if attempt.get("status") != "provider_empty" or not attempt.get("empty_evidence_path"):
            continue
        receipt = read_json(contained(root, attempt["empty_evidence_path"]))
        source = contained(root, receipt["raw_path"])
        before = signature(source)
        if sha256_file(source) != receipt["raw_sha256"]:
            raise ValueError("FinLab raw null-evidence hash mismatch")
        count = 0
        with pa.memory_map(str(source), "r") as handle:
            reader = pa.ipc.open_file(handle)
            for index in range(reader.num_record_batches):
                batch = reader.get_batch(index)
                for field, array in zip(batch.schema, batch.columns):
                    if field.name == "date":
                        continue
                    valid = pc.is_valid(array)
                    if pa.types.is_floating(field.type):
                        valid = pc.and_kleene(valid, pc.invert(pc.is_nan(array)))
                    count += pc.sum(pc.cast(valid, pa.int64())).as_py() or 0
        if signature(source) != before:
            raise ValueError("FinLab null source changed")
        upstream_empty.append({"dataset": attempt["dataset"], "raw_non_null_values": count,
                               "raw_sha256": receipt["raw_sha256"], "raw_path": str(source),
                               "decision": "upstream_all_null" if count == 0 else "adapter_needs_repair"})
    return {"current_receipts_checked": checked, "integrity_errors": errors,
            "upstream_empty": upstream_empty, "historical_pit_verified": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--research-features", type=Path,
                        help="Optional newer research table for checking legacy derived-column nulls")
    parser.add_argument("--refresh-finmind-repaired-datasets", action="store_true")
    args = parser.parse_args()
    all_candidates = csv_rows(args.catalog)
    refreshed_finmind, finmind_summary = {}, None
    if args.refresh_finmind_repaired_datasets:
        selected = {tuple(row["dataset_id"].split(":", 1)) for row in all_candidates
                    if row["provider"] == "FinMind" and row["admission"] == "incomplete_source"}
        tasks = []
        for lane, dataset in sorted(selected):
            if lane not in {"sponsor", "complement"}:
                raise ValueError("unsupported FinMind receipt owner")
            base = args.root / "data_finmind" / lane
            with sqlite3.connect(f"file:{base / 'queue.sqlite3'}?mode=ro", uri=True) as conn:
                conn.row_factory = sqlite3.Row
                tasks.extend((base, dict(row)) for row in conn.execute(
                    "SELECT dataset,data_id,partition,rows,receipt_path FROM tasks WHERE dataset=? "
                    "AND state='complete' AND rows>0", (dataset,)))
        with ThreadPoolExecutor(max_workers=4) as pool:
            records = list(pool.map(inspect_task, tasks))
        refreshed_finmind = {row["catalog_id"]: row for row in aggregate_fields(records)}
        finmind_summary = {"current_partitions_checked": len(records),
                           "failed": sum(bool(row.get("error")) for row in records),
                           "datasets": sorted(selected)}
    candidates = [row for row in all_candidates if row["admission"] == "missing_values"]
    fields = {row["field"] for row in candidates if row["provider"] == "OpenBB 多供應商"}
    openbb, evidence = recheck_openbb(args.root / "data_openBB", fields)
    finlab = recheck_finlab(args.root / "data_finlab")
    newer_stats = parquet_footer_stats(args.research_features) if args.research_features else None
    newer_counts = ({name: count for (name, _), count in zip(newer_stats["fields"], newer_stats["non_null"], strict=True)}
                    if newer_stats else {})
    other = []
    for row in candidates:
        if row["provider"] == "OpenBB 多供應商":
            continue
        role = field_role(row["field"], json.loads(row["types"]))
        decision = ("diagnostic_or_provenance_not_model_signal" if role == "key_or_provenance"
                    else "requires_source_specific_evidence_not_zero_fill")
        if newer_counts.get(row["field"], 0):
            decision = "observed_in_newer_research_table_not_quality_or_pit_approval"
        other.append({**row, "current_role": role, "followup_decision": decision,
                      "newer_research_non_null_count": newer_counts.get(row["field"]),
                      "newer_research_path": str(args.research_features) if args.research_features else None})
    write_csv(args.output_dir / "openbb_null_recheck.csv", openbb)
    write_csv(args.output_dir / "other_null_review.csv", other)
    # Preserve the original audit; publish a separate, explicitly scoped worklist.
    openbb_by_name = {row["field"]: row for row in openbb}
    other_by_id = {row["catalog_id"]: row for row in other}
    corrected = []
    for original in all_candidates:
        row = dict(original)
        if row["catalog_id"] in refreshed_finmind:
            row.update(refreshed_finmind[row["catalog_id"]])
        if row["admission"] == "missing_values" and row["provider"] == "OpenBB 多供應商":
            check = openbb_by_name[row["field"]]
            row.update(non_null_count=check["non_null_count"], rows=check["rows_with_field"],
                       files=check["files_with_field"], reason=check["scope"],
                       source_path=str(args.output_dir / "null_recheck.json"),
                       admission="needs_adapter_and_pit_evidence" if check["non_null_count"] else "missing_values")
            if row["field"].endswith("_code_quotes"):
                row.update(role="key_or_provenance", admission="not_model_input")
        elif row["catalog_id"] in other_by_id:
            check = other_by_id[row["catalog_id"]]
            row.update(role=check["current_role"], reason=check["followup_decision"])
            if check["current_role"] == "key_or_provenance":
                row["admission"] = "not_model_input"
            elif check["followup_decision"].startswith("observed_in_newer_research"):
                # A stock research observation is not a valid fill for an old
                # futures-derived table. Keep its original missing values.
                row["admission"] = "legacy_derived_table_needs_scoped_rebuild"
                row["next_action"] = "source_present_in_newer_research; verify_selected_universe_adapter_and_ABI"
        corrected.append(row)
    existing_finlab = {row["dataset_id"] for row in corrected if row["provider"] == "FinLab"}
    for item in finlab["upstream_empty"]:
        if item["dataset"] not in existing_finlab:
            corrected.append({"catalog_id": "finlab:" + item["dataset"], "provider": "FinLab",
                              "dataset_id": item["dataset"], "field": item["dataset"],
                              "role": "source_measure_with_instrument_or_dimension_axis",
                              "admission": item["decision"], "reason": "verified_raw_arrow_has_no_observed_values",
                              "source_path": item["raw_path"], "non_null_count": item["raw_non_null_values"],
                              "sha256_verified": True, "publication_clock": "not_verified",
                              "next_action": "retain_canonical_provider_retry; do_not_fill_or_backdate"})
    write_csv(args.output_dir / "feature_candidates_corrected.csv", corrected)
    result = {"observed_at_utc": datetime.now(UTC).isoformat(), "catalog_path": str(args.catalog),
              "catalog_sha256": sha256_file(args.catalog), "original_null_candidates": len(candidates),
              "openbb": {"original_candidates": len(fields),
                         "observed_in_newer_l1": sum(row["non_null_count"] > 0 for row in openbb),
                         "still_without_observation": [row["field"] for row in openbb if not row["non_null_count"]],
                         "files": evidence}, "finlab": finlab,
              "other_classification": dict(Counter(row["followup_decision"] for row in other)),
              "finmind_repaired_dataset_recheck": finmind_summary,
              "newer_research_path": str(args.research_features) if args.research_features else None,
              "training_abi_changed": False, "source_values_filled": False}
    result["corrected_catalog_rows"] = len(corrected)
    result["corrected_catalog_path"] = str(args.output_dir / "feature_candidates_corrected.csv")
    atomic_write_json(args.output_dir / "null_recheck.json", result)
    print({"FinLab_receipts_checked": finlab["current_receipts_checked"],
           "FinLab_integrity_errors": len(finlab["integrity_errors"]),
           "FinLab_upstream_empty": len(finlab["upstream_empty"]),
           "OpenBB_previously_misclassified_nulls": result["openbb"]["observed_in_newer_l1"]}, flush=True)
    return int(bool(finlab["integrity_errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
