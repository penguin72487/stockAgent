"""Inventory or reproducibly merge receipt-selected TW native minute rows.

Trial retains methods/audit/lineage only, not the assembled dataset. No provider
API, raw overwrite, training launch or live deployment belongs to this command.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, date, datetime
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.parquet_integrity import signature
from stockagent.data.source_rows import SourceRows, merge_source_rows, lineage_segments
from stockagent.data.tw_minute_sources import (
    CONTRACT, KEYS, FIELDS, SEMANTICS, normalize_shioaji, normalize_finmind,
    same_day_share_proofs, dated_market_share_proofs, resolve_share_proofs,
)
from stockagent.data.tw_minute_unit_evidence import (
    SCHEMA as MARKET_EVIDENCE_SCHEMA, pin_official_market_inputs, read_official_market_evidence,
)
from stockagent.data_sync.source_validation import VerificationCache
from stockagent.data_sync.desync_snapshots import SnapshotError


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _day(value):
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("noncanonical source session date")
    return value


def inventory(finmind_root: Path, shioaji_root: Path, *, official_root: Path | None = None) -> dict:
    """Pin current receipt pointers, never UNION every historic object version."""
    records, states = [], []
    with sqlite3.connect(f"file:{finmind_root / 'queue.sqlite3'}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        states = [dict(row) for row in connection.execute(
            "SELECT CASE WHEN data_id='TAIEX' THEN 'index_id' WHEN data_id='' THEN 'market_object' "
            "ELSE 'stock_id' END scope,state,COUNT(*) tasks,SUM(rows) rows,MIN(partition) first_query,"
            "MAX(partition) last_query,MIN(first_data_date) first_data,MAX(last_data_date) last_data "
            "FROM tasks WHERE dataset='TaiwanStockKBar' GROUP BY scope,state")]
        tasks = [dict(row) for row in connection.execute(
            "SELECT dataset,data_id,partition,rows,receipt_path FROM tasks WHERE dataset='TaiwanStockKBar' "
            "AND state='complete' ORDER BY partition,data_id")]
    for task in tasks:
        _day(task["partition"])
        relative = Path(task["receipt_path"])
        path = finmind_root / relative
        if relative.is_absolute() or ".." in relative.parts or not path.resolve().is_relative_to(finmind_root.resolve()):
            raise ValueError("unsafe FinMind receipt")
        receipt = json.loads(path.read_text())
        if receipt.get("status") != "complete" or any(receipt.get(k) != task[k]
                for k in ("dataset", "data_id", "partition", "rows")):
            raise ValueError(f"FinMind receipt identity changed: {path}")
        raw = Path(receipt["parquet_path"])
        if raw.is_absolute() or ".." in raw.parts or not (finmind_root / raw).resolve().is_relative_to(finmind_root.resolve()):
            raise ValueError("unsafe FinMind parquet")
        records.append({"provider": "finmind", "day": task["partition"],
            "scope": "index_id" if task["data_id"] == "TAIEX" else "market_object" if not task["data_id"] else "stock_id",
            "path": str((finmind_root / raw).resolve()), "sha256": receipt["sha256"],
            "rows": receipt["rows"], "size": receipt["parquet_size_bytes"],
            "receipt": str(path.resolve()), "receipt_sha256": sha256_file(path),
            "fetched_at_utc": receipt.get("fetched_at_utc"),
            "volume_units": receipt.get("volume_units", {})})
    manifest_path = shioaji_root / "manifest.json"
    before = signature(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("source") != "shioaji_kbars_1m" or not manifest.get("research_ready"):
        raise ValueError("selected Shioaji source is not audited research_ready")
    manifest_digest = sha256_file(manifest_path)
    if signature(manifest_path) != before:
        raise ValueError("source manifest changed during inventory")
    for row in manifest["partitions"]:
        day = _day(row["trade_date"])
        # Ignore producer paths: only the selected source root may be read.
        path = shioaji_root / f"trade_date={day}" / "data.parquet"
        summary = path.with_name("summary.json")
        if any(not p.resolve().is_relative_to(shioaji_root.resolve()) for p in (path, summary)):
            raise ValueError("Shioaji member escapes selected source root")
        before_summary = signature(summary)
        summary_value = json.loads(summary.read_text())
        summary_digest = sha256_file(summary)
        if signature(summary) != before_summary or summary_value.get("status") != "ok" or any(
            summary_value.get(key) != value for key, value in {
                "source": "shioaji_kbars_1m", "trade_date": day,
                "output_sha256": row["output_sha256"], "rows": row["rows"]}.items()):
            raise ValueError(f"Shioaji summary identity changed: {summary}")
        records.append({"provider": "shioaji", "day": day, "scope": "stock_session",
            "path": str(path.resolve()), "sha256": row["output_sha256"], "rows": row["rows"],
            "size": path.stat().st_size, "receipt": str(summary.resolve()),
            "receipt_sha256": summary_digest, "source_manifest_sha256": manifest_digest})
    methods = {"contract": CONTRACT, "semantics": SEMANTICS, "key": KEYS,
        "value_fields": FIELDS, "priority": ["shioaji", "finmind"],
        "selection": "valid_primary_else_missing_or_rejected_primary_backup; never_average_conflicts",
        "finmind_clock": "regular native label +1 minute; 13:30/13:33/14:30 actual auction labels unchanged",
        "quantity": "documented market unit plus same-symbol/same-day official raw venue or matching positive peer bars; disagreeing unit proofs remain NULL",
        "input_errors": "quarantine fingerprint/footer/schema/session failures; read other verified provider without reading rejected bytes",
        "native_missing_values": "preserve explicit NULL; no fieldwise OHLC mixing, minute padding or invented Amount",
        "scope": "cleaned_native_observations_not_fitted_training_panel",
        "private_personal_research": True, "historical_point_in_time": False,
        "training_ready": False, "no_provider_model_channels": True}
    from dataclasses import asdict
    from stockagent.data.tw_public_cross_source_fill import MAPPINGS
    pinned = {"contract": CONTRACT, "finmind_states": states, "records": records,
        "source_inventory_sha256": _digest(records), "methods": methods,
        "reviewed_financial_aliases": {name: asdict(spec) for name, spec in MAPPINGS.items()},
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "all_source_content_validated": False, "all_history_complete_claim": False}
    return attach_unit_evidence(pinned, official_root=official_root)


def attach_unit_evidence(pinned: dict, *, official_root: Path | None) -> dict:
    """Explicitly version the unit-evidence successor; do not move raw receipts."""
    if pinned["contract"] not in {CONTRACT, "tw_raw_minute_provider_union_v1"}:
        raise ValueError("unsupported parent minute source inventory")
    if _digest(pinned["records"]) != pinned["source_inventory_sha256"]:
        raise ValueError("pinned raw source inventory changed")
    inputs = pin_official_market_inputs(official_root, {r["day"] for r in pinned["records"]
        if r["provider"] == "finmind" and r["scope"] != "index_id"}) if official_root else []
    return {**pinned, "parent_contract": pinned["contract"], "contract": CONTRACT,
        "methods": {**pinned["methods"], "contract": CONTRACT, "semantics": SEMANTICS,
            "quantity": "documented market unit + dated official venue or same-day positive peer bars; contradictory proofs remain NULL"},
        "unit_evidence_inputs": inputs, "unit_evidence_inventory_sha256": _digest(inputs)}


def _empty():
    return pl.DataFrame(schema={"asset_class": pl.String, "symbol": pl.String,
        "ts": pl.Datetime("ns"), "phase": pl.String,
        **{name: pl.Float64 for name in FIELDS}, "_invalid_reason": pl.String})


def _read(record, cache, *, symbols, columns=None):
    path = Path(record["path"])
    receipt_path = Path(record["receipt"])
    if sha256_file(receipt_path) != record["receipt_sha256"]:
        raise ValueError(f"receipt changed after selection: {receipt_path}")
    before = signature(path)
    def validate():
        rows = pq.ParquetFile(path).metadata.num_rows
        if rows != record["rows"] or path.stat().st_size != record["size"]:
            raise ValueError("source footer disagrees with selected receipt")
        return {"rows": rows}
    cache.check(path, record["sha256"], validate)
    frame = pl.scan_parquet(path)
    if symbols:
        identity = "stock_id" if record["provider"] == "finmind" else "symbol"
        frame = frame.filter(pl.col(identity).is_in(symbols))
    if columns is not None:
        frame = frame.select(columns)
    frame = frame.collect(engine="streaming")
    if signature(path) != before:
        raise ValueError("source changed during projection")
    day_column = pl.col("date").cast(pl.String) if record["provider"] == "finmind" else pl.col("ts").cast(pl.Datetime("ns")).dt.strftime("%Y-%m-%d")
    if frame.select((day_column.is_null() | (day_column != record["day"])).any()).item():
        raise ValueError("source observations do not belong to selected session")
    return frame


def assemble(pinned: dict, *, report_root: Path, output_root: Path, symbols: list[str],
             first: str | None = None, last: str | None = None,
             previous_report: dict | None = None) -> dict:
    """One session in memory; retain every lineage/conflict, not top-K samples."""
    if output_root.exists() and any(output_root.iterdir()):
        raise ValueError("use an empty isolated output root; existing source/run is never overwritten")
    if (report_root / "sessions").exists() and any((report_root / "sessions").iterdir()):
        raise ValueError("use a fresh report root; audit sessions are never mixed across runs")
    if pinned["methods"]["semantics"] != SEMANTICS:
        raise ValueError("pinned methods need an explicitly compatible normalizer")
    if _digest(pinned.get("unit_evidence_inputs", [])) != pinned.get("unit_evidence_inventory_sha256"):
        raise ValueError("pinned unit evidence inventory changed")
    previous = {p["day"]: p for p in previous_report["partitions"]} if previous_report else {}
    if previous_report and (previous_report["source_inventory_sha256"] != pinned["source_inventory_sha256"]
                            or previous_report["selected_symbols"] != symbols):
        raise ValueError("repair comparison needs identical raw source inventory and symbols")
    repair_days = {day for day, p in previous.items() if p["unknown_share_quantity_rows"]} if previous_report else None
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    by_day = defaultdict(list)
    for record in pinned["records"]:
        if ((first is None or record["day"] >= first) and (last is None or record["day"] <= last)
                and (repair_days is None or record["day"] in repair_days)):
            by_day[record["day"]].append(record)
    unit_by_day = defaultdict(list)
    for record in pinned.get("unit_evidence_inputs", []):
        unit_by_day[record["day"]].append(record)
    total = defaultdict(int)
    total["selected_input_files"] = sum(len(records) for records in by_day.values())
    provider_rows, selection = defaultdict(int), defaultdict(int)
    first_last, partitions = {}, []
    primary_floor = min((r["day"] for r in pinned["records"] if r["provider"] == "shioaji"), default="9999-12-31")
    code_files = [Path(__file__), ROOT / "stockagent/data/source_rows.py", ROOT / "stockagent/data/tw_minute_sources.py",
                  ROOT / "stockagent/data/tw_minute_unit_evidence.py", ROOT / "downloader/finmind_volume_units.py",
                  ROOT / "downloader/download_tw_public_data.py", ROOT / "downloader/stock_volume_units.py",
                  ROOT / "stockagent/data_sync/source_validation.py"]
    code_identity = {str(p.relative_to(ROOT)): sha256_file(p) for p in code_files}
    started = time.monotonic()
    cache = VerificationCache(report_root / "input_verification.sqlite3", CONTRACT)
    try:
        for number, (day, records) in enumerate(sorted(by_day.items()), 1):
            primary, backup, rejected_inputs = [], [], []
            for record in records:
                try:
                    if record["provider"] == "shioaji":
                        raw = _read(record, cache, symbols=symbols,
                            columns=["symbol", "ts", "Open", "High", "Low", "Close", "Volume", "Amount", "contract_unit"])
                        primary.append(normalize_shioaji(raw))
                    else:
                        raw = _read(record, cache, symbols=symbols)
                        # Validate the native schema before accepting this input.
                        normalize_finmind(raw)
                        backup.append(raw)
                except (OSError, ValueError, KeyError, SnapshotError, pl.exceptions.PolarsError) as error:
                    rejected_inputs.append({"source": record, "error_type": type(error).__name__, "error": str(error)})
            s = pl.concat(primary, how="vertical") if primary else _empty()
            f_raw = pl.concat(backup, how="diagonal_relaxed") if backup else None
            refs = {provider: _digest([r for r in records if r["provider"] == provider])
                for provider in ("shioaji", "finmind")}
            evidence, rejected_units = [], []
            for record in unit_by_day[day]:
                try:
                    evidence.append(read_official_market_evidence(record, symbols=symbols))
                except (OSError, ValueError, KeyError, pl.exceptions.PolarsError) as error:
                    rejected_units.append({"source": record, "error_type": type(error).__name__, "error": str(error)})
            dated = dated_market_share_proofs(pl.concat(evidence) if evidence else pl.DataFrame(schema=MARKET_EVIDENCE_SCHEMA))
            peer = same_day_share_proofs(f_raw, s) if f_raw is not None else None
            proofs, unit_conflicts = resolve_share_proofs(peer, dated, peer_ref=refs["shioaji"]) if peer is not None else (None, dated.clear())
            f = normalize_finmind(f_raw, share_proofs=proofs) if f_raw is not None else _empty()
            result = merge_source_rows([
                SourceRows("shioaji", refs["shioaji"], SEMANTICS, s),
                SourceRows("finmind", refs["finmind"], SEMANTICS, f)],
                keys=KEYS, fields=FIELDS, semantics=SEMANTICS)
            comparison = {}
            if previous_report:
                before = merge_source_rows([
                    SourceRows("shioaji", refs["shioaji"], SEMANTICS, s),
                    SourceRows("finmind", refs["finmind"], SEMANTICS,
                               normalize_finmind(f_raw, share_proofs=peer) if f_raw is not None else _empty())],
                    keys=KEYS, fields=FIELDS, semantics=SEMANTICS)
                old_unknown = before.values.filter((pl.col("asset_class") == "equity") & pl.col("volume_shares").is_null()).height
                if before.values.height != previous[day]["canonical_rows"] or old_unknown != previous[day]["unknown_share_quantity_rows"]:
                    raise ValueError(f"baseline reconstruction changed: {day}")
                before_bytes = io.BytesIO()
                before.values.write_parquet(before_bytes, compression="zstd")
                if hashlib.sha256(before_bytes.getvalue()).hexdigest() != previous[day]["output_sha256"]:
                    raise ValueError(f"baseline output fingerprint changed: {day}")
                unchanged = KEYS + [name for name in FIELDS if name != "volume_shares"]
                if not before.values.select(unchanged).equals(result.values.select(unchanged)) or not before.lineage.equals(result.lineage):
                    raise ValueError(f"unit repair changed non-quantity values or provider selection: {day}")
                known = before.values.filter(pl.col("volume_shares").is_not_null()).select(KEYS + ["volume_shares"])
                if not known.equals(result.values.join(known.select(KEYS), on=KEYS, how="semi").select(KEYS + ["volume_shares"]).sort(KEYS)):
                    raise ValueError(f"unit repair changed previously proved shares: {day}")
                new_known = result.values.join(before.values.filter(pl.col("volume_shares").is_null()).select(KEYS),
                                               on=KEYS, how="semi").filter(pl.col("volume_shares").is_not_null())
                total["previously_unknown_share_rows_resolved"] += new_known.height
                comparison = {"before_unknown_share_rows": old_unknown, "resolved_share_rows": new_known.height,
                    "previous_output_sha256_reproduced": True,
                    "prices_amount_keys_provider_and_known_shares_unchanged": True}
            rejected_primary = any(r["source"]["provider"] == "shioaji" for r in rejected_inputs)
            lineage = result.lineage.with_columns(
                pl.when((pl.col("_provider") == "finmind") & (pl.col("asset_class") == "index"))
                .then(pl.lit("native_index_source"))
                .when((pl.col("_provider") == "finmind") & (pl.col("selection_reason") == "missing_primary_filled")
                      & pl.lit(rejected_primary)).then(pl.lit("repaired_rejected_primary_input"))
                .when((pl.col("_provider") == "finmind") & (pl.col("selection_reason") == "missing_primary_filled")
                      & pl.lit(day < primary_floor)).then(pl.lit("historical_extension"))
                .otherwise(pl.col("selection_reason")).alias("selection_reason"))
            audit = {**result.audit, "selection": {r["selection_reason"]: r["len"] for r in
                lineage.group_by("selection_reason").len().to_dicts()}}
            destination = output_root / f"trade_date={day}"
            destination.mkdir()
            result.values.write_parquet(destination / "data.parquet", compression="zstd")
            ledger = report_root / "sessions" / day
            ledger.mkdir(parents=True, exist_ok=True)
            lineage_segments(lineage, groups=["asset_class", "symbol", "phase"]).write_parquet(
                ledger / "lineage_segments.parquet", compression="zstd")
            result.conflicts.write_parquet(ledger / "conflicts.parquet", compression="zstd")
            result.rejected.write_parquet(ledger / "rejected.parquet", compression="zstd")
            atomic_write_json(ledger / "source_refs.json", records)
            atomic_write_json(ledger / "rejected_inputs.json", rejected_inputs)
            atomic_write_json(ledger / "unit_evidence_refs.json", unit_by_day[day])
            atomic_write_json(ledger / "rejected_unit_inputs.json", rejected_units)
            dated.write_parquet(ledger / "dated_market_share_proofs.parquet", compression="zstd")
            unit_conflicts.write_parquet(ledger / "unit_proof_conflicts.parquet", compression="zstd")
            if proofs is not None:
                proofs.write_parquet(ledger / "same_day_share_proofs.parquet", compression="zstd")
            for name in ("source_rows", "canonical_rows", "duplicate_rows", "rejected_rows", "conflicting_valid_rows"):
                total[name] += audit[name]
            total["rejected_input_files"] += len(rejected_inputs)
            total["rejected_unit_evidence_files"] += len(rejected_units)
            total["conflicting_unit_proof_rows"] += unit_conflicts.height
            total["sessions_with_no_accepted_rows"] += int(result.values.is_empty())
            for provider, count in audit["provider_rows"].items(): provider_rows[provider] += count
            for reason, count in audit["selection"].items(): selection[reason] += count
            unknown = result.values.filter((pl.col("asset_class") == "equity") & pl.col("volume_shares").is_null()).height
            total["equity_rows_without_share_quantity"] += unknown
            for kind, symbol, provider in lineage.select("asset_class", "symbol", "_provider").unique().iter_rows():
                key = f"{kind}:{symbol}:{provider}"
                first_last.setdefault(key, {"first": day, "last": day})["last"] = day
            receipt = {"day": day, **audit, **comparison, "rejected_input_files": len(rejected_inputs), "unknown_share_quantity_rows": unknown,
                "output_sha256": sha256_file(destination / "data.parquet")}
            atomic_write_json(ledger / "merge.json", receipt)
            partitions.append(receipt)
            if number % 100 == 0 or number == len(by_day):
                print(f"[source-row-merge] sessions={number}/{len(by_day)} canonical_rows={total['canonical_rows']} "
                      f"conflicts={total['conflicting_valid_rows']} elapsed={time.monotonic()-started:.1f}s", flush=True)
    finally:
        cache.close()
    report = {"contract": CONTRACT, "source_inventory_sha256": pinned["source_inventory_sha256"],
        "unit_evidence_inventory_sha256": pinned["unit_evidence_inventory_sha256"],
        "methods": pinned["methods"], "selected_symbols": symbols, "sessions": len(partitions),
        "counts": dict(total), "provider_rows": dict(provider_rows), "selection": dict(selection),
        "coverage_by_symbol_provider": first_last, "partitions": partitions,
        "merger_code_files": code_identity, "merger_code_sha256": _digest(code_identity),
        "all_selected_input_bytes_validated": total["rejected_input_files"] == 0,
        "all_source_content_validated": not symbols and total["selected_input_files"] == len(pinned["records"])
                                        and total["rejected_input_files"] == 0,
        "validation_scope": "all_bytes_SHA_and_footer; selected_rows_semantic_merge",
        "wall_seconds": time.monotonic() - started, "training_ready": False,
        "source_equivalence_not_investment_result": True, "all_history_complete_claim": False}
    if previous_report:
        report["repair_comparison"] = {
            "affected_sessions_only": True, "previous_sessions": previous_report["sessions"],
            "before_unknown_share_rows": previous_report["counts"]["equity_rows_without_share_quantity"],
            "resolved_share_rows": total["previously_unknown_share_rows_resolved"],
            "remaining_share_rows": previous_report["counts"]["equity_rows_without_share_quantity"]
                                   - total["previously_unknown_share_rows_resolved"],
            "prices_amount_keys_provider_and_known_shares_unchanged": True,
            "unaffected_sessions_not_rerun": previous_report["sessions"] - len(partitions)}
    atomic_write_json(output_root / "source_manifest.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inventory", "trial", "build"))
    parser.add_argument("--finmind-root", type=Path, default=ROOT / "data_finmind/complement")
    parser.add_argument("--shioaji-root", type=Path, default=ROOT / "data_tw_minute/research_dataset")
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--symbols", default="", help="Comma-separated explicit scope; empty means all observed symbols.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--inventory", type=Path, help="Use a previously pinned inventory, not current queue heads.")
    parser.add_argument("--official-root", type=Path, default=ROOT / "data_tw_public",
                        help="Receipt-selected official raw daily reports proving each dated venue.")
    parser.add_argument("--repair-report", type=Path,
                        help="Trial only: verify unchanged values and reassemble only prior unit-unknown sessions.")
    args = parser.parse_args()
    if args.mode == "build" and args.output_root is None: parser.error("build requires --output-root")
    if args.mode != "build" and args.output_root is not None: parser.error("trial never retains an assembled dataset")
    if args.repair_report and (args.mode != "trial" or args.start_date or args.end_date):
        parser.error("--repair-report requires a trial without date narrowing")
    args.report_root.mkdir(parents=True, exist_ok=True)
    pinned = json.loads(args.inventory.read_text()) if args.inventory else inventory(
        args.finmind_root, args.shioaji_root, official_root=args.official_root)
    if args.inventory and pinned.get("contract") == "tw_raw_minute_provider_union_v1":
        pinned = attach_unit_evidence(pinned, official_root=args.official_root)
    if pinned.get("contract") != CONTRACT or _digest(pinned["records"]) != pinned["source_inventory_sha256"]:
        raise ValueError("pinned source inventory changed")
    atomic_write_json(args.report_root / "source_inventory.json", pinned)
    if args.mode == "inventory":
        print(json.dumps({"source_inventory": str(args.report_root / "source_inventory.json"),
            "records": len(pinned["records"]), "finmind_states": pinned["finmind_states"]}, ensure_ascii=False))
        return
    kwargs = dict(report_root=args.report_root, symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
                  first=args.start_date, last=args.end_date,
                  previous_report=json.loads(args.repair_report.read_text()) if args.repair_report else None)
    if args.repair_report:
        atomic_write_json(args.report_root / "comparison_parent.json", {
            "path": str(args.repair_report.resolve()), "sha256": sha256_file(args.repair_report)})
    if args.mode == "trial":
        # Use owned disk scratch, not /tmp tmpfs for hundreds of millions of
        # native rows. Successful and failed trials remove only their own view.
        with tempfile.TemporaryDirectory(prefix="stockagent-minute-merge-", dir=args.report_root) as temporary:
            report = assemble(pinned, output_root=Path(temporary), **kwargs)
        report["assembled_dataset_retained"] = False
        report["owned_temporary_directory_removed"] = not Path(temporary).exists()
    else:
        report = assemble(pinned, output_root=args.output_root, **kwargs)
        report["assembled_dataset_retained"] = True
        report["output_root"] = str(args.output_root.resolve())
    atomic_write_json(args.report_root / "merge_report.json", report)
    print(json.dumps({k: v for k, v in report.items() if k not in {"partitions", "coverage_by_symbol_provider", "methods"}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
