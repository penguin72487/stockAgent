#!/usr/bin/env python3
"""Independently read original tick rows for the first-observation audit."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import json
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


def raw_counts(path: Path, day: str, keys: set[tuple[str, str]]) -> dict:
    """Count original source records with Arrow, without the Polars reducer."""
    result = {key: {"0845": [0, 0, None], "0900": [0, 0, None]} for key in keys}
    parquet = pq.ParquetFile(path)
    products = pa.array(sorted({p for p, _ in keys}), type=parquet.schema_arrow.field("futures_id").type)
    contracts = pa.array(sorted({c for _, c in keys}), type=parquet.schema_arrow.field("contract_date").type)
    for batch in parquet.iter_batches(columns=["date", "futures_id", "contract_date", "price", "volume"]):
        selected = pc.and_(pc.is_in(batch.column("futures_id"), value_set=products),
                           pc.is_in(batch.column("contract_date"), value_set=contracts))
        cols = batch.filter(selected).to_pydict()
        for timestamp, product, contract, price, volume in zip(
                cols["date"], cols["futures_id"], cols["contract_date"], cols["price"], cols["volume"], strict=True):
            key = product, contract
            if key not in result:
                continue
            if not isinstance(timestamp, str) or len(timestamp) != 19:
                raise ValueError(f"invalid raw tick timestamp: {day}")
            when = datetime.fromisoformat(timestamp)
            if when.date().isoformat() != day:
                raise ValueError(f"raw tick does not match receipt date: {day}")
            minute = when.hour * 60 + when.minute
            label = "0845" if minute == 526 else "0900" if minute == 541 else None
            if label is None:
                continue
            state = result[key][label]
            state[0] += 1
            if (price is None or not math.isfinite(price) or price <= 0 or volume is None
                    or volume <= 0 or volume != math.floor(volume) or volume % 2):
                state[1] += 1
            if state[2] is None or when < state[2]:
                state[2] = when
    return result


def verify_rows(rows: list[dict], counts: dict) -> list[dict]:
    failures = []
    for row in rows:
        key = row["product"], row["contract"]
        if key not in counts:
            failures.append({"date": str(row["date"]), "physical_contract": row["physical_contract"],
                             "reason": "missing_target"})
            continue
        for label in ("0845", "0900"):
            actual = [row[f"{label}_raw_print_rows"], row[f"{label}_invalid_raw_rows"],
                      row[f"{label}_first_raw_timestamp"]]
            if actual != counts[key][label]:
                failures.append({"date": str(row["date"]), "physical_contract": row["physical_contract"],
                                 "reason": f"{label}_count_or_time"})
    return failures


def build(manifest_path: Path, report: Path) -> dict:
    if report.exists() or report.is_symlink():
        raise ValueError("independent readback report already exists")
    manifest_path = manifest_path.resolve()
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("contract") != "tw_futures_omitted_first_day_windows_v1" or manifest.get("training_ready") is not False:
        raise ValueError("unsupported first-day research artifact")
    data = Path(manifest["data_path"])
    if sha256_file(data) != manifest["data_sha256"]:
        raise ValueError("observation data differs from the manifest")
    pins = manifest["input_sha256"]
    checkpoint_paths = [Path(p) for p in pins if "/bulk_readback_checkpoints/" in p and p.endswith("/manifest.json")]
    if len(checkpoint_paths) != 1:
        raise ValueError("cannot identify the reviewed bulk checkpoint")
    checkpoint = checkpoint_paths[0]
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("first-day parent input changed")
    inventory_doc = json.loads(checkpoint.read_text())
    inventory = {item["date"]: item for item in inventory_doc["identity"]["objects"]}
    rows = pq.read_table(data).to_pylist()
    if len(rows) != manifest["target_contract_days"]:
        raise ValueError("first-day observation count differs")
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["date"].isoformat()].append(row)
    failures, checked = [], 0
    for index, (day, group) in enumerate(sorted(grouped.items()), 1):
        if any(row["source_status"] != "raw_receipt_matched" for row in group):
            failures.append({"date": day, "reason": "source_not_read_back"})
            continue
        item = inventory.get(day)
        if item is None or item.get("source_kind") or any(
                row["source_sha256"] != item["sha256"] or row["receipt_sha256"] != item["receipt_sha256"]
                for row in group):
            raise ValueError(f"observation does not bind the original source identity: {day}")
        path, receipt = Path(item["path"]), Path(item["receipt_path"])
        if (path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]
                or sha256_file(receipt) != item["receipt_sha256"]
                or pq.ParquetFile(path).metadata.num_rows != item["rows"]):
            raise ValueError(f"source or receipt differs: {day}")
        keys = {(row["product"], row["contract"]) for row in group}
        if len(keys) != len(group):
            raise ValueError("physical first-day target is ambiguous")
        counts = raw_counts(path, day, keys)
        failures.extend(verify_rows(group, counts))
        if sha256_file(path) != item["sha256"] or sha256_file(receipt) != item["receipt_sha256"]:
            raise ValueError(f"source changed during independent readback: {day}")
        checked += len(group)
        if index % 100 == 0 or index == len(grouped):
            print(json.dumps({"stage": "independent_first_day_raw_readback", "dates_checked": index,
                              "dates_total": len(grouped)}), flush=True)
    if (sha256_file(manifest_path) != manifest_sha or sha256_file(data) != manifest["data_sha256"]
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError("input changed during independent readback")
    output = {"contract": "tw_futures_omitted_first_day_independent_readback_v1", "created_at_utc": datetime.now(UTC).isoformat(),
              "accepted": not failures and checked == len(rows), "failure_count": len(failures),
              "failures": failures, "checked_contract_days": checked, "checked_source_dates": len(grouped),
              "candidate_sha256": manifest["data_sha256"], "manifest_sha256": manifest_sha,
              "implementation_sha256": sha256_file(Path(__file__)), "training_ready": False,
              "limitations": ["Independent reader of the same provider objects, not a second data source.",
                              "No historical receipt-time, listing-at-decision, complete-book or fill proof."]}
    atomic_write_json(report, output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = build(args.manifest, args.report)
    print(json.dumps({"accepted": result["accepted"], "failure_count": result["failure_count"],
                      "checked_contract_days": result["checked_contract_days"]}))
    if not result["accepted"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
