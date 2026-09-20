#!/usr/bin/env python3
"""Extend an accepted historical share-replacement catalog with an accepted tail.

This does not infer corporate-action terms or turn a partial download into a
complete one. Both source tables and every retained raw response are verified
before the overlap and coverage gates permit an atomic canonical replacement.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import sha256_file  # noqa: E402
from downloader.download_tw_corporate_action_entitlements import (  # noqa: E402
    _file_receipt,
    _write_bytes_atomic,
    _write_json_atomic,
    _write_parquet_atomic,
)
from stockagent.live.tw_share_replacement import load_share_replacements  # noqa: E402


NAME = "tw_share_replacement_reference"
IDENTITY = ("market", "symbol", "resume_date")
ACCOUNTING_FIELDS = (
    "suspension_date",
    "new_shares_per_1000_old",
    "cash_return_per_old_share",
    "cash_payment_date",
    "cash_dividend_per_old_share",
    "subscription_shares_per_1000",
    "subscription_terms_present",
    "historical_halt_evidence",
    "executable_price",
    "accounting_terms_complete",
    "unresolved_terms",
)


def _accepted(root: Path, *, start: date, end: date) -> tuple[pl.DataFrame, dict]:
    # This loader verifies the accepted parquet, its summary and each raw
    # request/response referenced by its content-addressed manifest.
    load_share_replacements(root, required_start=start, required_end=end)
    summary = json.loads((root / f"{NAME}.summary.json").read_text())
    if (
        int(summary.get("schema_version", 0)) < 3
        or summary.get("complete_all_markets") is not True
        or summary.get("lifecycle_catalog_complete") is not True
        or summary.get("source_download_complete") is not True
        or summary.get("failure_count") != 0
        or set(summary.get("covered_markets") or ()) != {"twse", "tpex"}
    ):
        raise ValueError(f"share-replacement input is not accepted: {root}")
    return pl.read_parquet(root / f"{NAME}.parquet"), summary


def _keys(frame: pl.DataFrame) -> set[tuple[str, str, date]]:
    if frame.select(pl.len()).item() != frame.select(IDENTITY).unique().height:
        raise ValueError("duplicate share-replacement event identity")
    return {
        (str(row["market"]), str(row["symbol"]), row["resume_date"])
        for row in frame.select(IDENTITY).iter_rows(named=True)
    }


def _manifest_lines(root: Path, summary: dict) -> list[dict]:
    rel = Path(str(summary["raw_receipt_manifest"]["relative_path"]))
    path = (root / rel).resolve()
    if not path.is_relative_to(root / "raw"):
        raise ValueError("raw receipt manifest escapes source raw directory")
    if sha256_file(path) != summary["raw_receipt_manifest"]["sha256"]:
        raise ValueError("raw receipt manifest hash mismatch")
    return [json.loads(line) for line in path.read_text().splitlines()]


def _selected_events(items: list[dict], *, cutover: date, before: bool) -> list[dict]:
    result = []
    for item in items:
        observed = date.fromisoformat(str(item["resume_date"]))
        if (observed < cutover) == before:
            result.append(item)
    return result


def _exact_core(row: dict) -> bool:
    ratio = row.get("new_shares_per_1000_old")
    cash = row.get("cash_return_per_old_share")
    return (
        row.get("suspension_date") is not None
        and ratio is not None
        and abs(float(ratio) - round(float(ratio))) <= 1e-8
        and cash is not None
        and not (row.get("cash_dividend_per_old_share") or 0)
        and not (row.get("subscription_shares_per_1000") or 0)
        and not row.get("subscription_terms_present")
        and (cash == 0 or row.get("cash_payment_date") is not None)
    )


def merge(
    public_root: Path,
    tail_root: Path,
    *,
    cutover: date,
    required_start: date,
    required_end: date,
    apply: bool,
) -> dict:
    public_root = public_root.resolve(strict=True)
    tail_root = tail_root.resolve(strict=True)
    if tail_root == public_root or not tail_root.is_relative_to(public_root):
        raise ValueError("tail must be a distinct child of the public producer")
    baseline, old = _accepted(
        public_root, start=required_start, end=cutover - timedelta(days=1)
    )
    tail, recent = _accepted(tail_root, start=cutover, end=required_end)
    old_end = date.fromisoformat(old["coverage_end"])
    if old_end < cutover or date.fromisoformat(recent["coverage_start"]) > cutover:
        raise ValueError("historical and tail source intervals do not overlap")
    if baseline.schema["resume_date"] != pl.Date or tail.schema["resume_date"] != pl.Date:
        raise ValueError("share-replacement resumption dates must be Date")
    if not set(tail.columns) <= set(baseline.columns):
        raise ValueError("tail schema contains unreviewed new columns")
    old_overlap = baseline.filter(
        (pl.col("resume_date") >= cutover) & (pl.col("resume_date") <= old_end)
    )
    new_overlap = tail.filter(
        (pl.col("resume_date") >= cutover) & (pl.col("resume_date") <= old_end)
    )
    old_keys, new_keys = _keys(old_overlap), _keys(new_overlap)
    if old_keys != new_keys:
        raise ValueError(
            "accepted tail changes overlapping exchange event identities: "
            f"historical_only={sorted(old_keys - new_keys)} "
            f"tail_only={sorted(new_keys - old_keys)}"
        )
    old_by_key = {
        tuple(row[field] for field in IDENTITY): row
        for row in old_overlap.iter_rows(named=True)
    }
    revisions = []
    for row in new_overlap.iter_rows(named=True):
        key = tuple(row[field] for field in IDENTITY)
        changed = [
            field for field in ACCOUNTING_FIELDS
            if old_by_key[key].get(field) != row.get(field)
        ]
        if changed:
            revisions.append({"identity": [str(part) for part in key], "fields": changed})
    earlier = baseline.filter(pl.col("resume_date") < cutover)
    later = tail.filter(pl.col("resume_date") >= cutover)
    for column, dtype in baseline.schema.items():
        if column not in later.columns:
            later = later.with_columns(pl.lit(None, dtype=dtype).alias(column))
        else:
            later = later.with_columns(pl.col(column).cast(dtype, strict=True))
    merged = pl.concat(
        [earlier.select(baseline.columns), later.select(baseline.columns)],
        how="vertical",
    ).sort(["resume_date", "symbol"])
    _keys(merged)
    old_manifest = _manifest_lines(public_root, old)
    tail_manifest = _manifest_lines(tail_root, recent)
    namespace = f"merged_tail_{recent['output_receipt']['sha256'][:16]}"
    copies: list[tuple[Path, Path, str]] = []
    rewritten = []
    raw_prefix = Path("raw") / NAME
    for item in tail_manifest:
        source_rel = Path(str(item["path"]))
        if not source_rel.is_relative_to(raw_prefix):
            raise ValueError("tail receipt points outside its raw source")
        source = (tail_root / source_rel).resolve()
        if not source.is_relative_to(tail_root / "raw"):
            raise ValueError("tail raw source escapes its root")
        destination_rel = raw_prefix / namespace / source_rel.relative_to(raw_prefix)
        destination = public_root / destination_rel
        if source.stat().st_size != item["response_size"] or sha256_file(source) != item["response_sha256"]:
            raise ValueError(f"tail raw response hash mismatch: {source_rel}")
        copies.append((source, destination, str(item["response_sha256"])))
        rewritten.append({**item, "path": destination_rel.as_posix()})
    lines = sorted(old_manifest + rewritten, key=lambda item: (item["path"], item["request_sha256"]))
    manifest_bytes = b"".join(
        (json.dumps(item, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode()
        for item in lines
    )
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_rel = raw_prefix / "manifests" / f"{manifest_hash}.jsonl"
    rows = merged.to_dicts()
    exact = sum(_exact_core(row) for row in rows)
    accounting_failures = _selected_events(
        old.get("accounting_failures") or [], cutover=cutover, before=True
    ) + _selected_events(
        recent.get("accounting_failures") or [], cutover=cutover, before=False
    )
    recovered = _selected_events(
        old.get("recovered_exchange_detail_failures") or [], cutover=cutover, before=True
    ) + _selected_events(
        recent.get("recovered_exchange_detail_failures") or [], cutover=cutover, before=False
    )
    output = public_root / f"{NAME}.parquet"
    summary = {
        **recent,
        "coverage_start": old["coverage_start"],
        "coverage_end": recent["coverage_end"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": merged.height,
        "exact_physical_core_events": exact,
        "unresolved_physical_accounting_events": merged.height - exact,
        "exact_cash_return_payment_events": sum(row.get("cash_payment_date") is not None for row in rows),
        "accounting_failures": accounting_failures,
        "accounting_failure_count": len(accounting_failures),
        "recovered_exchange_detail_failures": recovered,
        "official_issuer_reference_recovery_count": len(recovered),
        "raw_receipt_manifest": {
            "entries": len(lines),
            "relative_path": manifest_rel.as_posix(),
            "sha256": manifest_hash,
            "size": len(manifest_bytes),
        },
        "merged_input_receipts": {
            "historical": old["output_receipt"]["sha256"],
            "tail": recent["output_receipt"]["sha256"],
            "cutover_date": cutover.isoformat(),
            "overlap_event_count": len(old_keys),
            "overlap_accounting_revisions": revisions,
        },
    }
    plan = {
        "apply": apply,
        "historical_rows": baseline.height,
        "tail_rows": tail.height,
        "merged_rows": merged.height,
        "overlap_event_count": len(old_keys),
        "accounting_revisions": len(revisions),
        "tail_raw_responses": len(copies),
        "coverage": [summary["coverage_start"], summary["coverage_end"]],
    }
    if not apply:
        return plan
    for source, destination, digest in copies:
        if destination.exists():
            if sha256_file(destination) != digest:
                raise ValueError(f"existing merged raw response differs: {destination}")
            continue
        _write_bytes_atomic(destination, source.read_bytes())
        if sha256_file(destination) != digest:
            raise ValueError(f"merged raw response differs after write: {destination}")
    manifest_path = public_root / manifest_rel
    if manifest_path.exists() and sha256_file(manifest_path) != manifest_hash:
        raise ValueError("existing merged raw manifest differs")
    if not manifest_path.exists():
        _write_bytes_atomic(manifest_path, manifest_bytes)
    _write_parquet_atomic(merged, output)
    summary["output_receipt"] = _file_receipt(output)
    _write_json_atomic(output.with_suffix(".summary.json"), summary)
    load_share_replacements(
        public_root, required_start=required_start, required_end=required_end
    )
    return {**plan, "output_sha256": summary["output_receipt"]["sha256"], "verified": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-root", type=Path, required=True)
    parser.add_argument("--tail-root", type=Path, required=True)
    parser.add_argument("--cutover-date", type=date.fromisoformat, required=True)
    parser.add_argument("--required-start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--required-end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    lock = args.public_root / "state/locks/tw_share_replacement_reference.lock"
    tail_lock = args.tail_root / "state/locks/tw_share_replacement_reference.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    tail_lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as handle, tail_lock.open("a") as tail_handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(tail_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = merge(
            args.public_root,
            args.tail_root,
            cutover=args.cutover_date,
            required_start=args.required_start_date,
            required_end=args.required_end_date,
            apply=args.apply,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
