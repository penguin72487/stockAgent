#!/usr/bin/env python3
"""Inventory Shioaji stock-minute *day* coverage against public daily volume.

This is deliberately not a proof that every trade or every 1-minute bar exists.
The source manifest is the terminal downloader catalog; byte/row integrity is
checked separately by ``audit_shioaji_tw_minute_dataset``.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re

from downloader.download_shioaji_tw_kbars import (
    SHIOAJI_STOCK_HISTORY_START,
    _load_universe,
    _positive_volume_dates,
)
from stockagent.live.shioaji_schedule import latest_completed_tw_stock_session


TERMINAL_STATUSES = {"complete", "complete_with_source_gaps", "contract_unavailable"}
MISSING_FIELDS = ("symbol", "trade_date", "year", "category")
RETENTION_FIELDS = (
    "symbol", "market", "start_date", "end_date", "classification",
    "source_status", "declared_rows", "data_path", "data_sha256",
    "receipt_path", "source_manifest", "source_manifest_sha256",
)


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def inventory_recorded_retention_scope(
    *, source_root: Path, repo_root: Path, cutoff: date, output_root: Path,
) -> dict:
    """Inventory manifest-declared chunks; never infer deletion readiness.

    This bounded pass reads the original catalog and per-symbol manifests, not
    the large market objects. Date windows are *declared* source windows, not
    proof of file content, absence of other files, reachability or recoverability.
    In particular, empty-provider receipts are kept in the listing as evidence.
    """
    source_root = source_root.absolute()
    repo_root = repo_root.absolute()
    source_rel = source_root.relative_to(repo_root)
    if ".." in source_rel.parts or cutoff <= SHIOAJI_STOCK_HISTORY_START:
        raise ValueError("invalid source root or retention cutoff")
    output_root = output_root.absolute()
    resolved_output = output_root.resolve(strict=False)
    resolved_source = source_root.resolve(strict=True)
    if resolved_output == resolved_source or resolved_output.is_relative_to(resolved_source):
        raise ValueError("inventory output must not be inside the source tree")
    summary_path, report_path = source_root / "download_summary.json", source_root / "download_report.csv"

    def generation(st: os.stat_result) -> tuple[int, ...]:
        return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)

    def stable_bytes(path: Path) -> tuple[bytes, tuple[int, ...]]:
        # Bind the bytes actually parsed to the opened file and its current
        # pathname; a separate hash-then-read would admit a changing catalog.
        with path.open("rb") as stream:
            before = os.fstat(stream.fileno())
            body = stream.read()
            after = os.fstat(stream.fileno())
        identity = generation(after)
        if (generation(before) != identity or len(body) != after.st_size
                or generation(path.stat()) != identity):
            raise ValueError(f"inventory input changed while reading: {path}")
        return body, identity

    catalog_bytes, catalog_generation = stable_bytes(summary_path)
    report_bytes, report_generation = stable_bytes(report_path)
    catalog_hash = hashlib.sha256(catalog_bytes).hexdigest()
    report_hash = hashlib.sha256(report_bytes).hexdigest()
    catalog = json.loads(catalog_bytes)
    if not isinstance(catalog, dict):
        raise ValueError("source catalog is not an object")
    records = list(csv.DictReader(io.StringIO(report_bytes.decode("utf-8"), newline="")))
    symbols = [str(row.get("symbol") or "") for row in records]
    if (len(symbols) != len(set(symbols))
            or any(not re.fullmatch(r"[A-Za-z0-9]+", symbol) for symbol in symbols)
            or catalog.get("source") != "shioaji_kbars_1m"
            or catalog.get("storage_frequency") != "minute"
            or int(catalog.get("selected_symbols", -1)) != len(records)
            or int(catalog.get("reported_symbols", -1)) != len(records)):
        raise ValueError("source catalog identity, symbols or report counts differ")
    output_root.mkdir(parents=True, exist_ok=False)
    output = output_root / "recorded_chunks.csv"
    failures: list[dict[str, str]] = []
    counts: Counter[str] = Counter()
    manifest_digest = hashlib.sha256()
    seen_paths: set[str] = set()
    manifest_generations: list[tuple[str, Path, tuple[int, ...]]] = []

    def declared_path(raw: object, symbol: str, *, suffix: str) -> str:
        if not isinstance(raw, str) or not raw:
            raise ValueError(f"missing declared {suffix} path")
        candidate = Path(raw)
        prefix = (*source_rel.parts, "minute_chunks", symbol)
        if (candidate.is_absolute() or ".." in candidate.parts
                or candidate.parts[:len(prefix)] != prefix
                or len(candidate.parts) != len(prefix) + 1
                or not candidate.name.endswith(suffix)):
            raise ValueError(f"declared {suffix} path is outside its symbol chunk root")
        return str(candidate)

    with output.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RETENTION_FIELDS)
        writer.writeheader()
        for row in sorted(records, key=lambda item: item["symbol"]):
            symbol, status = str(row["symbol"]), str(row.get("status") or "")
            counts["reported_symbols"] += 1
            if status == "contract_unavailable":
                counts["contract_unavailable_symbols"] += 1
                continue
            if status not in {"complete", "complete_with_source_gaps"}:
                failures.append({"symbol": symbol, "error": f"unresolved report status: {status}"})
                continue
            manifest = source_root / "symbols" / f"{symbol}.manifest.json"
            try:
                body, manifest_generation = stable_bytes(manifest)
                digest = hashlib.sha256(body).hexdigest()
                document = json.loads(body)
                if (not isinstance(document, dict) or document.get("symbol") != symbol
                        or document.get("source") != "shioaji_kbars_1m"
                        or document.get("storage_frequency") != "minute"
                        or not isinstance(document.get("chunks"), list)):
                    raise ValueError("manifest identity or chunk structure differs")
                # Validate a complete symbol before writing any of its rows.
                pending = []
                local_paths: set[str] = set()
                for chunk in document["chunks"]:
                    start, end = date.fromisoformat(chunk["start_date"]), date.fromisoformat(chunk["end_date"])
                    if end < start:
                        raise ValueError("chunk end precedes start")
                    if start >= cutoff:
                        continue
                    classification = "declared_before_cutoff" if end < cutoff else "declared_crosses_cutoff"
                    receipt = declared_path(chunk.get("receipt_path"), symbol, suffix=".receipt.json")
                    data = chunk.get("data_path")
                    chunk_status, rows = chunk.get("status"), chunk.get("rows")
                    if type(rows) is not int or rows < 0:
                        raise ValueError("invalid declared row count")
                    if chunk_status == "source_gap":
                        gap_dates = chunk.get("source_gap_dates")
                        if (not isinstance(gap_dates, list) or not gap_dates
                                or any(not start <= date.fromisoformat(item) <= end for item in gap_dates)):
                            raise ValueError("source-gap chunk lacks dated gap evidence")
                    if chunk_status in {"ok", "source_gap"} and rows > 0:
                        data = declared_path(data, symbol, suffix=".parquet")
                        source_hash = chunk.get("data_sha256")
                        if not isinstance(source_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", source_hash):
                            raise ValueError("nonempty chunk lacks rows or SHA256")
                    elif chunk_status in {"empty", "source_gap"} and rows == 0 and data is None:
                        source_hash = ""
                    else:
                        raise ValueError(f"unsupported chunk outcome: {chunk_status}")
                    for path in (receipt, data):
                        if path and (path in seen_paths or path in local_paths):
                            raise ValueError("duplicate declared chunk path")
                        if path:
                            local_paths.add(path)
                    pending.append({"symbol": symbol, "market": document.get("market") or "",
                        "start_date": start.isoformat(), "end_date": end.isoformat(),
                        "classification": classification, "source_status": chunk_status,
                        "declared_rows": rows, "data_path": data or "", "data_sha256": source_hash,
                        "receipt_path": receipt, "source_manifest": str(manifest),
                        "source_manifest_sha256": digest})
                seen_paths.update(local_paths)
                for item in pending:
                    writer.writerow(item)
                    counts[item["classification"]] += 1
                    counts["receipt_only_empty"] += int(item["source_status"] == "empty")
                    counts["source_gap_chunks"] += int(item["source_status"] == "source_gap")
                    counts["receipt_only_source_gap"] += int(item["source_status"] == "source_gap" and not item["data_path"])
                    counts["declared_data_files"] += int(bool(item["data_path"]))
                counts["manifests_read"] += 1
                manifest_digest.update(f"{symbol}\0{digest}\n".encode())
                manifest_generations.append((symbol, manifest, manifest_generation))
            except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                failures.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
    for symbol, manifest, observed in manifest_generations:
        try:
            if generation(manifest.stat()) != observed:
                raise ValueError("manifest generation changed after read")
        except (OSError, ValueError) as exc:
            failures.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
    try:
        current_catalog, current_catalog_generation = stable_bytes(summary_path)
        current_report, current_report_generation = stable_bytes(report_path)
        stable_catalog = (current_catalog_generation == catalog_generation
                          and current_report_generation == report_generation
                          and hashlib.sha256(current_catalog).hexdigest() == catalog_hash
                          and hashlib.sha256(current_report).hexdigest() == report_hash)
    except (OSError, ValueError):
        stable_catalog = False
    if not stable_catalog:
        failures.append({"symbol": "", "error": "source catalog changed during inventory"})
    result = {
        "schema_version": 1, "contract": "shioaji_recorded_retention_inventory_v1",
        "status": "recorded_candidates_with_unverified_files" if stable_catalog and not failures else "incomplete",
        "cutoff_exclusive": cutoff.isoformat(), "source_root": str(source_root),
        "scope": "terminal_report_referenced_symbol_manifests_only",
        "catalog_sha256": catalog_hash, "report_sha256": report_hash,
        "catalog_unchanged_during_pass": stable_catalog,
        "reported_catalog_complete": catalog.get("resumable_collection_complete") is True,
        "manifest_digest_sha256": manifest_digest.hexdigest(),
        "counts": dict(sorted(counts.items())), "errors": failures,
        "candidates_csv": str(output), "candidates_csv_sha256": _sha256(output),
        "bytes_or_file_content_verified": False, "unreferenced_files_included": False,
        "references_or_recovery_verified": False, "deletion_authorized": False,
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(output_root / "summary.json", result)
    return result


def audit_history(
    *,
    stock_root: Path,
    source_root: Path,
    start: date,
    end: date,
    output_root: Path,
) -> dict:
    if start < SHIOAJI_STOCK_HISTORY_START:
        raise ValueError(f"Shioaji stock history starts {SHIOAJI_STOCK_HISTORY_START}")
    if end < start:
        raise ValueError("end date precedes start date")
    catalog = _read_json(source_root / "download_summary.json")
    catalog_start = date.fromisoformat(str(catalog["start_date"]))
    catalog_end = date.fromisoformat(str(catalog["end_date"]))
    if catalog_start > start:
        raise ValueError(f"terminal catalog starts too late: {catalog_start} > {start}")
    with (source_root / "download_report.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    reported = {str(row["symbol"]): row for row in rows}
    if len(reported) != len(rows):
        raise ValueError("duplicate symbols in terminal download report")
    universe = _load_universe(stock_root)
    if len({row.symbol for row in universe}) != len(universe):
        raise ValueError("duplicate symbols in public universe")

    output_root.mkdir(parents=True, exist_ok=True)
    missing_path = output_root / "missing_symbol_days.csv"
    symbols_path = output_root / "symbols.csv"
    missing_tmp = missing_path.with_name(missing_path.name + ".tmp")
    symbols_tmp = symbols_path.with_name(symbols_path.name + ".tmp")
    totals: Counter[str] = Counter()
    yearly: dict[str, Counter[str]] = defaultdict(Counter)
    catalog_mismatches: list[str] = []
    with missing_tmp.open("w", newline="", encoding="utf-8") as missing_file, \
            symbols_tmp.open("w", newline="", encoding="utf-8") as symbols_file:
        missing_writer = csv.DictWriter(missing_file, fieldnames=MISSING_FIELDS)
        symbol_fields = (
            "symbol", "market", "security_type", "source_status", "first_expected",
            "last_expected", "first_source", "last_source", "expected_days",
            "observed_days", "source_gap_days", "contract_unavailable_days",
            "unclassified_days", "pending_days",
            "source_gap_outside_public_daily_days",
            "source_returned_outside_public_daily_days",
        )
        symbol_writer = csv.DictWriter(symbols_file, fieldnames=symbol_fields)
        missing_writer.writeheader()
        symbol_writer.writeheader()
        for row in universe:
            expected = _positive_volume_dates(row.base_path, start, end)
            record = reported.get(row.symbol)
            status = str(record["status"]) if record else "unreported"
            returned: set[date] = set()
            gaps: set[date] = set()
            first_source = last_source = ""
            if status in {"complete", "complete_with_source_gaps"}:
                manifest_path = source_root / "symbols" / f"{row.symbol}.manifest.json"
                try:
                    manifest = _read_json(manifest_path)
                    if (
                        manifest.get("symbol") != row.symbol
                        or date.fromisoformat(str(manifest["requested_start"])) > start
                        or date.fromisoformat(str(manifest["requested_end"])) < min(end, catalog_end)
                    ):
                        raise ValueError("manifest symbol/range disagrees with terminal catalog")
                    terminal = {
                        date.fromisoformat(str(value))
                        for value in manifest["terminal_coverage_dates"]
                    }
                    gaps = {
                        date.fromisoformat(str(value))
                        for value in manifest["source_gap_dates"]
                    }
                    if not gaps.issubset(terminal):
                        raise ValueError("source-gap dates are not terminal dates")
                    returned = terminal - gaps
                    first_source = str(manifest.get("first_date") or "")
                    last_source = str(manifest.get("last_date") or "")
                except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    catalog_mismatches.append(f"{row.symbol}: {exc}")
                    status = "invalid_manifest"
                    returned.clear()
                    gaps.clear()
            counts: Counter[str] = Counter()
            for session in sorted(expected):
                if session > catalog_end:
                    category = "pending"
                elif session in returned:
                    category = "observed"
                elif session in gaps:
                    category = "source_gap"
                elif status == "contract_unavailable":
                    category = "contract_unavailable"
                else:
                    category = "unclassified"
                counts[category] += 1
                totals[category] += 1
                yearly[str(session.year)][category] += 1
                if category != "observed":
                    missing_writer.writerow({
                        "symbol": row.symbol,
                        "trade_date": session.isoformat(),
                        "year": session.year,
                        "category": category,
                    })
            # The public daily reference can change after a broker query. Keep
            # historical broker outcomes visible instead of erasing them from
            # the inventory when a current daily row is absent.
            extra_gaps = {
                session for session in gaps if start <= session <= end
            } - expected
            extra_returned = {
                session for session in returned if start <= session <= end
            } - expected
            counts["source_gap_outside_public_daily"] = len(extra_gaps)
            totals["source_gap_outside_public_daily"] += len(extra_gaps)
            counts["source_returned_outside_public_daily"] = len(extra_returned)
            totals["source_returned_outside_public_daily"] += len(extra_returned)
            for session in sorted(extra_gaps):
                yearly[str(session.year)]["source_gap_outside_public_daily"] += 1
                missing_writer.writerow({
                    "symbol": row.symbol,
                    "trade_date": session.isoformat(),
                    "year": session.year,
                    "category": "source_gap_outside_public_daily",
                })
            for session in extra_returned:
                yearly[str(session.year)]["source_returned_outside_public_daily"] += 1
            symbol_writer.writerow({
                "symbol": row.symbol,
                "market": row.market,
                "security_type": row.security_type,
                "source_status": status,
                "first_expected": min(expected).isoformat() if expected else "",
                "last_expected": max(expected).isoformat() if expected else "",
                "first_source": first_source,
                "last_source": last_source,
                "expected_days": len(expected),
                "observed_days": counts["observed"],
                "source_gap_days": counts["source_gap"],
                "contract_unavailable_days": counts["contract_unavailable"],
                "unclassified_days": counts["unclassified"],
                "pending_days": counts["pending"],
                "source_gap_outside_public_daily_days": counts["source_gap_outside_public_daily"],
                "source_returned_outside_public_daily_days": counts["source_returned_outside_public_daily"],
            })
    os.replace(missing_tmp, missing_path)
    os.replace(symbols_tmp, symbols_path)
    terminal_catalog_consistent = (
        int(catalog.get("selected_symbols", -1)) == len(reported)
        and int(catalog.get("reported_symbols", -1)) == len(reported)
    )
    universe_symbols = {row.symbol for row in universe}
    unreported_symbols = sorted(universe_symbols - set(reported))
    source_only_symbols = sorted(set(reported) - universe_symbols)
    classified = (
        terminal_catalog_consistent
        and bool(catalog.get("resumable_collection_complete"))
        and not catalog_mismatches
        and totals["unclassified"] == 0
        and totals["pending"] == 0
        and catalog_end >= end
        and all(str(row["status"]) in TERMINAL_STATUSES for row in rows)
    )
    report = {
        "schema_version": 2,
        "status": "classified_with_limits" if classified else "incomplete",
        "coverage_basis": "public_positive_volume_symbol_days_plus_broker_only_outcomes_vs_terminal_source_manifest",
        "universe_basis": "currently_registered_stock_etf_public_symbols_not_all_ever_listed",
        "not_proven": "per_minute_trade_completeness_or_chunk_byte_integrity",
        "public_symbols_csv_sha256": _sha256(stock_root / "symbols.csv"),
        "terminal_download_summary_sha256": _sha256(source_root / "download_summary.json"),
        "terminal_download_report_sha256": _sha256(source_root / "download_report.csv"),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "terminal_catalog_end": catalog_end.isoformat(),
        "public_symbols": len(universe),
        "terminal_report_symbols": len(reported),
        "terminal_catalog_consistent": terminal_catalog_consistent,
        "catalog_matches_current_universe": not unreported_symbols and not source_only_symbols,
        "unreported_current_symbols": unreported_symbols,
        "source_only_symbols": source_only_symbols,
        "counts": {
            **{key: totals[key] for key in (
                "observed", "source_gap", "contract_unavailable", "unclassified",
                "pending", "source_gap_outside_public_daily",
                "source_returned_outside_public_daily",
            )},
            "expected": sum(totals[key] for key in (
                "observed", "source_gap", "contract_unavailable", "unclassified", "pending",
            )),
        },
        "by_year": {
            year: dict(sorted(counts.items()))
            for year, counts in sorted(yearly.items())
        },
        "catalog_mismatches": catalog_mismatches,
        "missing_symbol_days_csv": str(missing_path),
        "missing_symbol_days_sha256": _sha256(missing_path),
        "symbols_csv": str(symbols_path),
        "symbols_sha256": _sha256(symbols_path),
        "written_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _write_json(output_root / "summary.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-root", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--source-root", type=Path, default=Path("data_tw_minute/shioaji_1m"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=SHIOAJI_STOCK_HISTORY_START)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--inventory-before", type=date.fromisoformat,
                        help="List recorded chunks before/crossing this date; no deletion or provider calls")
    parser.add_argument("--require-classified", action="store_true")
    args = parser.parse_args()
    if args.inventory_before is not None:
        if args.output_root is None:
            parser.error("--inventory-before requires a new explicit --output-root")
        result = inventory_recorded_retention_scope(
            source_root=args.source_root, repo_root=Path.cwd(), cutoff=args.inventory_before,
            output_root=args.output_root,
        )
        print(json.dumps(result, ensure_ascii=False))
        if result["status"] == "incomplete":
            raise SystemExit(2)
        return
    if args.output_root is None:
        args.output_root = Path("artifacts/data_quality/shioaji_minute_history")
    end = args.end_date or latest_completed_tw_stock_session(parquet_root=args.stock_root.parent)
    report = audit_history(
        stock_root=args.stock_root,
        source_root=args.source_root,
        start=args.start_date,
        end=end,
        output_root=args.output_root,
    )
    print(json.dumps({
        "status": report["status"], "counts": report["counts"],
        "catalog_mismatches": len(report["catalog_mismatches"]),
        "output": str(args.output_root / "summary.json"),
    }, ensure_ascii=False))
    if args.require_classified and report["status"] != "classified_with_limits":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
