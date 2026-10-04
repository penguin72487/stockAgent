"""Cheap, read-only scheduling gates for the full-market Shioaji 1m runner."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


def _json_object(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def _source_report(path: Path) -> tuple[list[dict[str, str]], set[str], int]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    symbols = {str(row["symbol"]).strip() for row in rows}
    total_rows = sum(int(row.get("source_minute_rows") or 0) for row in rows)
    return rows, symbols, total_rows


def _source_fingerprint(source_root: Path, report_rows: list[dict[str, str]]) -> str:
    """Fingerprint data hashes and gap classifications, ignoring write times."""
    digest = hashlib.sha256()
    for row in sorted(report_rows, key=lambda value: value["symbol"]):
        symbol = str(row["symbol"]).strip()
        status = str(row.get("status") or "")
        item: dict = {"symbol": symbol, "status": status}
        if status in {"complete", "complete_with_source_gaps"}:
            manifest = _json_object(source_root / "symbols" / f"{symbol}.manifest.json")
            item.update({
                "requested_start": manifest["requested_start"],
                "requested_end": manifest["requested_end"],
                "source_gap_dates": manifest["source_gap_dates"],
                "terminal_coverage_dates": manifest["terminal_coverage_dates"],
                "chunks": [
                    {
                        "start_date": chunk["start_date"],
                        "end_date": chunk["end_date"],
                        "rows": chunk["rows"],
                        "data_sha256": chunk.get("data_sha256"),
                        "source_gap_dates": chunk.get("source_gap_dates"),
                        "underlying_data_method": chunk.get("underlying_data_method"),
                    }
                    for chunk in manifest["chunks"]
                ],
            })
        digest.update(json.dumps(item, sort_keys=True, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def source_fingerprint(source_root: Path) -> str:
    """Fingerprint the terminal source catalog used by a research build."""
    report_rows, _, _ = _source_report(source_root / "download_report.csv")
    return _source_fingerprint(source_root, report_rows)


def frontier_state(
    *, source_root: Path, stock_root: Path, target_date: str,
) -> tuple[bool, int, str]:
    """Return terminal frontier validity and its source-row count.

    Date and count alone are insufficient: a newly listed symbol can replace
    another in a same-sized catalog, leaving an incomplete all-market frontier.
    """
    try:
        summary = _json_object(source_root / "download_summary.json")
        report_rows, reported, source_rows = _source_report(
            source_root / "download_report.csv"
        )
        with (stock_root / "symbols.csv").open(newline="", encoding="utf-8") as handle:
            selected_rows = [
                row for row in csv.DictReader(handle)
                if str(row.get("security_type") or "").strip().lower() in {"stock", "etf"}
            ]
        selected = {str(row["code"]).strip() for row in selected_rows}
        ready = (
            bool(summary.get("resumable_collection_complete"))
            and summary.get("start_date") == "2020-03-02"
            and summary.get("end_date") == target_date
            and int(summary.get("failed_symbols", 0)) == 0
            and int(summary.get("partial_symbols", 0)) == 0
            and int(summary.get("selected_symbols", -1)) == len(selected_rows)
            and int(summary.get("reported_symbols", -1)) == len(report_rows)
            and len(reported) == len(report_rows)
            and len(selected) == len(selected_rows)
            and reported == selected
            and all(
                row.get("status") in {
                    "complete", "complete_with_source_gaps", "contract_unavailable"
                }
                for row in report_rows
            )
        )
        return (
            ready,
            source_rows,
            _source_fingerprint(source_root, report_rows) if ready else "-",
        )
    except (OSError, KeyError, ValueError, TypeError, json.JSONDecodeError):
        return False, -1, "-"


def may_reuse_materialization(
    *,
    source_root: Path,
    research_root: Path,
    audit_path: Path,
    target_date: str,
    frontier_was_ready: bool,
    source_rows_before: int,
    source_fingerprint_before: str,
) -> bool:
    """Reuse only an already audited, byte-identical terminal source frontier.

    Row count alone is insufficient: a normal frontier query can correct an
    existing K-bar. The receipt-backed chunk hashes and gap classifications
    must also be unchanged before skipping full materialization.
    """
    if not frontier_was_ready or source_rows_before < 0 or source_fingerprint_before == "-":
        return False
    try:
        summary = _json_object(source_root / "download_summary.json")
        report_rows, _, source_rows_after = _source_report(
            source_root / "download_report.csv"
        )
        manifest_bytes = (research_root / "manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        audit = _json_object(audit_path)
        return (
            bool(summary.get("resumable_collection_complete"))
            and source_rows_after == source_rows_before
            and _source_fingerprint(source_root, report_rows) == source_fingerprint_before
            and manifest.get("research_ready") is True
            and manifest.get("download_start_date") == "2020-03-02"
            and manifest.get("download_end_date") == target_date
            and manifest.get("source_fingerprint_sha256") == source_fingerprint_before
            and audit.get("status") == "research_ready"
            and audit.get("last_date") == target_date
            and audit.get("manifest_sha256") == hashlib.sha256(manifest_bytes).hexdigest()
        )
    except (OSError, KeyError, ValueError, TypeError, json.JSONDecodeError):
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("frontier", "reuse"))
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--source-root", type=Path, default=Path("data_tw_minute/shioaji_1m"))
    parser.add_argument("--stock-root", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--research-root", type=Path, default=Path("data_tw_minute/research_dataset"))
    parser.add_argument("--audit-path", type=Path, default=Path("data_tw_minute/audits/full_latest.json"))
    parser.add_argument("--frontier-was-ready", type=int, choices=(0, 1), default=0)
    parser.add_argument("--source-rows-before", type=int, default=-1)
    parser.add_argument("--source-fingerprint-before", default="-")
    args = parser.parse_args()
    if args.mode == "frontier":
        ready, rows, fingerprint = frontier_state(
            source_root=args.source_root,
            stock_root=args.stock_root,
            target_date=args.target_date,
        )
        print(f"{1 if ready else 0} {rows} {fingerprint}")
    else:
        reusable = may_reuse_materialization(
            source_root=args.source_root,
            research_root=args.research_root,
            audit_path=args.audit_path,
            target_date=args.target_date,
            frontier_was_ready=bool(args.frontier_was_ready),
            source_rows_before=args.source_rows_before,
            source_fingerprint_before=args.source_fingerprint_before,
        )
        print("1" if reusable else "0")


if __name__ == "__main__":
    main()
