"""Bounded TEJ Excel/preview parity acceptance, not a bulk downloader or PIT gate.

Usage: run_fintech_python scripts/verify_tej_smart_wizard_export.py
  --input <workbook-readback.json> --preview <preview-readback.json>
  --symbol 2330 --dates 2014-01-02,2014-01-03,... --output-dir <new-directory>

Read the workbook with read_tej_smart_wizard_workbook.ps1. Supply only the
market preview cells, never Smart Wizard's account-bearing query-settings file.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import io
import hashlib
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file

CONTRACT_VERSION = 1
HEADERS = (
    "CO_ID", "Date", "Open(NTD)", "High(NTD)", "Low(NTD)", "Close(NTD)",
    "Volume(1000S)", "Amount(NTD1000)",
)


def _number(value: object) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("missing/boolean numeric cell")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid numeric cell") from exc
    if not result.is_finite():
        raise ValueError("non-finite numeric cell")
    return result


def _excel_date(value: object, system: str) -> date:
    number = _number(value)
    if system not in ("excel_1900", "excel_1904"):
        raise ValueError("unknown Excel date system")
    minimum = 61 if system == "excel_1900" else 0
    if number != number.to_integral_value() or not minimum <= number <= 2_900_000:
        raise ValueError("invalid/fractional daily Excel serial date")
    base = date(1899, 12, 30) if system == "excel_1900" else date(1904, 1, 1)
    return base + timedelta(days=int(number))


def _grid(cells: object) -> list[list]:
    if not isinstance(cells, list) or len(cells) < 2 or len(cells) > 5001:
        raise ValueError("empty or oversized export")
    if any(not isinstance(row, list) or len(row) != len(HEADERS) for row in cells):
        raise ValueError("incomplete or unexpected row width")
    if tuple(cells[0]) != HEADERS:
        raise ValueError("unexpected schema or source units")
    return cells


def verify_export(
    payload: dict, preview: dict, *, symbol: str, expected_dates: list[str],
) -> tuple[dict, list[dict]]:
    """Require exact planned keys and full preview parity; never repair by filling."""
    if not isinstance(payload, dict) or not isinstance(preview, dict):
        raise ValueError("expected readback objects")
    if (
        type(payload.get("contract_version")) is not int
        or payload["contract_version"] != CONTRACT_VERSION
        or payload.get("provider") != "tej_smart_wizard"
        or payload.get("extraction_method") != "excel_com_value2"
        or preview.get("extraction_method") != "msaa_preview_grid"
    ):
        raise ValueError("unrecognized readback contract")
    if not isinstance(symbol, str) or not symbol.isascii() or not symbol.isdigit():
        raise ValueError("expected an explicitly selected Taiwan security code")
    planned = [date.fromisoformat(value) for value in expected_dates]
    if not planned or len(set(planned)) != len(planned):
        raise ValueError("expected dates must be nonempty and unique")
    if not isinstance(payload.get("observed_at_utc"), str):
        raise ValueError("expected an observation clock")
    observed = datetime.fromisoformat(payload["observed_at_utc"].replace("Z", "+00:00"))
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("observation clock requires a timezone")
    if max(planned) > observed.astimezone(ZoneInfo("Asia/Taipei")).date():
        raise ValueError("planned daily data is in the future")
    cells = _grid(payload.get("cells"))
    preview_cells = _grid(preview.get("cells"))
    preview_by_date = {}
    for row in preview_cells[1:]:
        day = date.fromisoformat(str(row[1]).replace("/", "-"))
        if day in preview_by_date:
            raise ValueError("duplicate preview key")
        preview_by_date[day] = row
    seen = set()
    output = []
    for row in cells[1:]:
        day = _excel_date(row[1], payload.get("date_system"))
        if not isinstance(row[0], str) or row[0].split(maxsplit=1)[:1] != [symbol]:
            raise ValueError("unexpected symbol")
        if day in seen:
            raise ValueError("duplicate export key")
        seen.add(day)
        numeric = [_number(value) for value in row[2:]]
        opening, high, low, close, source_volume, source_amount = numeric
        if min(numeric[:4]) <= 0 or not low <= min(opening, close) <= max(opening, close) <= high:
            raise ValueError("invalid OHLC relationship")
        shares, amount_twd = source_volume * 1000, source_amount * 1000
        if shares < 0 or shares != shares.to_integral_value() or shares > 2**63 - 1 or amount_twd < 0:
            raise ValueError("invalid source volume or money")
        match = preview_by_date.get(day)
        if match is None or match[0] != row[0] or numeric != [_number(value) for value in match[2:]]:
            raise ValueError("Excel/preview mismatch")
        output.append({
            "provider": "tej_smart_wizard", "symbol": symbol, "date": day.isoformat(),
            "open_twd": str(opening), "high_twd": str(high), "low_twd": str(low),
            "close_twd": str(close), "source_volume_thousand_shares": str(source_volume),
            "source_amount_thousand_twd": str(source_amount), "volume_shares": int(shares),
            "amount_twd": str(amount_twd), "observed_at_utc": observed.isoformat(),
        })
    if seen != set(planned) or set(preview_by_date) != set(planned):
        raise ValueError("missing or unexpected planned date")
    output.sort(key=lambda row: row["date"])
    report = {
        "contract_version": CONTRACT_VERSION, "status": "passed_bounded_export_acceptance",
        "rows": len(output), "numeric_features": 6, "numeric_values": len(output) * 6,
        "earliest_date": min(planned).isoformat(), "latest_date": max(planned).isoformat(),
        "symbol": symbol, "observed_at_utc": observed.isoformat(),
        "checks": {"exact_planned_keys": True, "unique_keys": True,
                   "complete_numeric_cells": True, "valid_ohlc": True,
                   "excel_preview_parity": True, "explicit_unit_conversion": True},
        "units": {"source_volume": "thousand_shares", "volume_shares": "shares",
                  "source_amount": "thousand_TWD", "amount_twd": "TWD",
                  "conversion_multiplier": 1000, "precision_policy": "retain_provider_precision"},
        "limits": {"all_history_complete_claim": False, "historical_publication_time_verified": False,
                   "strict_training_eligible_claim": False, "independent_source_reconciled": False,
                   "source_rounding_recovered": False, "bulk_or_unattended_acquisition_verified": False},
    }
    return report, output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--preview", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--dates", required=True, help="Comma-separated expected ISO trading dates")
    parser.add_argument("--output-dir", type=Path, required=True, help="A new directory, never overwritten")
    args = parser.parse_args(argv)
    try:
        input_bytes = args.input.read_bytes()
        preview_bytes = args.preview.read_bytes()
        report, rows = verify_export(
            json.loads(input_bytes.decode("utf-8-sig")),
            json.loads(preview_bytes.decode("utf-8-sig")),
            symbol=args.symbol, expected_dates=args.dates.split(","),
        )
        # Do not replace prior evidence or publish anything on a failed validation.
        args.output_dir.mkdir(parents=True, exist_ok=False)
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        csv_path = args.output_dir / "historical_sample.csv"
        atomic_write_text(csv_path, buffer.getvalue(), durable=True)
        report["inputs"] = {"excel_sha256": hashlib.sha256(input_bytes).hexdigest(),
                            "preview_sha256": hashlib.sha256(preview_bytes).hexdigest()}
        report["output"] = {"file": csv_path.name, "sha256": sha256_file(csv_path)}
        atomic_write_json(args.output_dir / "verification.json", report)
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f"TEJ bounded export verification failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({"status": report["status"], "rows": report["rows"],
                      "numeric_values": report["numeric_values"], "output_dir": str(args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
