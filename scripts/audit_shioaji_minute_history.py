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
import json
import os
from pathlib import Path

from downloader.download_shioaji_tw_kbars import (
    SHIOAJI_STOCK_HISTORY_START,
    _load_universe,
    _positive_volume_dates,
)
from stockagent.live.shioaji_schedule import latest_completed_tw_stock_session


TERMINAL_STATUSES = {"complete", "complete_with_source_gaps", "contract_unavailable"}
MISSING_FIELDS = ("symbol", "trade_date", "year", "category")


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
    parser.add_argument("--output-root", type=Path, default=Path("artifacts/data_quality/shioaji_minute_history"))
    parser.add_argument("--require-classified", action="store_true")
    args = parser.parse_args()
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
