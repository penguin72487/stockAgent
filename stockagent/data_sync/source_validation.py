"""Receipt-bound, incremental validation before canonical data publication.

This is not a collector or an execution simulator. Validated files are cached by
content contract and full native signature; changing bytes, inode, ctime or the
validator invalidates the local cache. Publication still performs its own SHA
checks. Neither a successful query nor a source-gap receipt proves completeness.
"""
from __future__ import annotations

import csv
from datetime import date
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Callable, Any

from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file


def signature(path: Path) -> list[int]:
    st = path.stat()
    return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]


def read_object(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SnapshotError(f"expected JSON object: {path}")
    return value


class VerificationCache:
    """Local evidence cache, never a replacement for cold/transport SHA checks."""

    def __init__(self, path: Path, contract: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=60)
        self.db.execute("CREATE TABLE IF NOT EXISTS verified "
                        "(path TEXT PRIMARY KEY, signature TEXT, contract TEXT, "
                        "sha256 TEXT, result TEXT)")
        self.contract = contract
        self.reused = self.checked = 0

    def check(self, path: Path, expected: str, validate: Callable[[], dict]) -> dict:
        before = signature(path)
        key = str(path.resolve(strict=True))
        sig = json.dumps(before)
        row = self.db.execute("SELECT signature, contract, sha256, result FROM verified "
                              "WHERE path=?", (key,)).fetchone()
        if row and row[:3] == (sig, self.contract, expected):
            result = json.loads(row[3])
            self.reused += 1
        else:
            if len(expected) != 64 or sha256_file(path) != expected:
                raise SnapshotError(f"source fingerprint mismatch: {path}")
            result = validate()
            if signature(path) != before:
                raise SnapshotError(f"source changed during validation: {path}")
            self.db.execute("INSERT OR REPLACE INTO verified VALUES (?, ?, ?, ?, ?)",
                            (key, sig, self.contract, expected, json.dumps(result)))
            self.checked += 1
            if self.checked % 100 == 0:
                self.db.commit()
        if signature(path) != before:
            raise SnapshotError(f"source changed during validation reuse: {path}")
        return result

    def close(self):
        self.db.commit()
        self.db.close()


def canonical_chunk_path(root: Path, symbol: str, start: str, end: str,
                         *, suffix: str = ".parquet") -> Path:
    """Resolve this selected release, not a producer's possibly existing path."""
    if not symbol or not symbol.isalnum():
        raise SnapshotError("invalid minute symbol")
    for value in (start, end):
        if date.fromisoformat(value).isoformat() != value:
            raise SnapshotError("noncanonical chunk date")
    path = root / "minute_chunks" / symbol / f"{start}_{end}{suffix}"
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root.resolve(strict=True)):
        raise SnapshotError(f"minute member escapes release: {path}")
    return resolved


def validator_identity(*paths: Path) -> str:
    return hashlib.sha256("".join(sha256_file(p) for p in paths).encode()).hexdigest()


def audit_stock_minute_source(root: Path, cache_path: Path) -> dict[str, Any]:
    """All terminal members, including explicit empty/source-gap classifications.

    Raw non-session observations are preserved. The canonical research builder
    quarantines them against the independently receipt-verified exchange calendar.
    """
    import polars as pl
    from downloader import download_shioaji_tw_minute_kbars as collector
    from scripts.shioaji_minute_backfill_state import source_fingerprint

    summary_path = root / "download_summary.json"
    report_path = root / "download_report.csv"
    summary_sha = sha256_file(summary_path)
    report_sha = sha256_file(report_path)
    summary = read_object(summary_path)
    if (summary.get("source") != "shioaji_kbars_1m"
            or summary.get("resumable_collection_complete") is not True
            or summary.get("failed_symbols") != 0 or summary.get("partial_symbols") != 0
            or any(summary.get(k) for k in ("fatal_error", "stopped_for_traffic",
                   "stopped_for_market_hours", "stopped_for_schedule"))):
        raise SnapshotError("stock minute collection is not terminal")
    with report_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    symbols = [r["symbol"] for r in rows]
    if (not rows or len(set(symbols)) != len(rows)
            or len(rows) != summary["selected_symbols"]
            or len(rows) != summary["reported_symbols"]):
        raise SnapshotError("stock minute terminal universe mismatch")
    contract = validator_identity(Path(__file__), Path(collector.__file__))
    cache = VerificationCache(cache_path, contract)
    chunks = raw_rows = bytes_count = empty_chunks = unavailable = 0
    status_counts: dict[str, int] = {}
    manifests: list[tuple[Path, str]] = []
    try:
        for position, row in enumerate(rows, start=1):
            status = row["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
            if status == "contract_unavailable":
                unavailable += 1
                continue
            if status not in {"complete", "complete_with_source_gaps"}:
                raise SnapshotError(f"nonterminal minute symbol: {row['symbol']}: {status}")
            symbol = row["symbol"]
            manifest_path = root / "symbols" / f"{symbol}.manifest.json"
            manifest_sha = sha256_file(manifest_path)
            manifest = read_object(manifest_path)
            manifests.append((manifest_path, manifest_sha))
            if (manifest.get("symbol") != symbol
                    or manifest.get("source") != summary["source"]
                    or manifest.get("simulation") is not summary["simulation"]
                    or manifest.get("requested_start") != summary["start_date"]
                    or manifest.get("requested_end") != summary["end_date"]):
                raise SnapshotError(f"minute symbol identity/range mismatch: {symbol}")
            total = 0
            keys = set()
            for entry in manifest["chunks"]:
                start, end = entry["start_date"], entry["end_date"]
                if (start, end) in keys:
                    raise SnapshotError(f"duplicate minute chunk: {symbol} {start}")
                keys.add((start, end))
                receipt_path = canonical_chunk_path(root, symbol, start, end,
                                                    suffix=".receipt.json")
                receipt = read_object(receipt_path)
                if (receipt.get("schema_version") != collector.RECEIPT_SCHEMA_VERSION
                        or receipt.get("source") != summary["source"]
                        or receipt.get("symbol") != symbol
                        or receipt.get("simulation") is not summary["simulation"]
                        or receipt.get("start_date") != start or receipt.get("end_date") != end
                        or receipt.get("status") != entry["status"]
                        or receipt.get("status") not in {"ok", "empty", "source_gap"}
                        or receipt.get("rows") != entry["rows"]):
                    raise SnapshotError(f"minute receipt disagrees with manifest: {receipt_path}")
                if receipt["status"] == "source_gap" and not receipt.get("source_gap_dates"):
                    raise SnapshotError(f"source gap lacks dated evidence: {receipt_path}")
                count = int(entry["rows"])
                if count == 0:
                    if receipt["status"] == "ok":
                        raise SnapshotError(f"empty chunk falsely marked ok: {receipt_path}")
                    empty_chunks += 1
                    continue
                output = receipt.get("output_receipt") or {}
                expected = entry.get("data_sha256")
                path = canonical_chunk_path(root, symbol, start, end)
                if output.get("sha256") != expected or output.get("size") != path.stat().st_size:
                    raise SnapshotError(f"minute output receipt mismatch: {path}")
                def validate(path=path, symbol=symbol, start=start, end=end):
                    return collector.validate_minute_kbars(pl.read_parquet(path), symbol=symbol,
                        start=date.fromisoformat(start), end=date.fromisoformat(end))
                result = cache.check(path, expected, validate)
                if result["rows"] != count:
                    raise SnapshotError(f"minute row count differs from receipt: {path}")
                chunks += 1
                total += count
                raw_rows += count
                bytes_count += path.stat().st_size
            if total != int(row["source_minute_rows"]) or total != manifest["minute_rows"]:
                raise SnapshotError(f"minute symbol row accounting mismatch: {symbol}")
            if position % 200 == 0:
                print(f"[minute-source-audit] symbols={position}/{len(rows)} "
                      f"chunks={chunks} checked={cache.checked} reused={cache.reused}", flush=True)
        for path, expected in manifests:
            if sha256_file(path) != expected:
                raise SnapshotError(f"minute manifest changed during audit: {path}")
        if sha256_file(summary_path) != summary_sha or sha256_file(report_path) != report_sha:
            raise SnapshotError("minute terminal catalog changed during audit")
        for key, status in (("complete_symbols", "complete"),
                            ("complete_with_source_gap_symbols", "complete_with_source_gaps"),
                            ("contract_unavailable_symbols", "contract_unavailable")):
            if summary[key] != status_counts.get(status, 0):
                raise SnapshotError(f"terminal summary status count mismatch: {key}")
        return dict(status="validated_available_source", source_root=str(root.resolve()),
                    end_date=summary["end_date"], start_date=summary["start_date"],
                    selected_symbols=len(rows), available_symbols=len(rows)-unavailable,
                    status_counts=status_counts, rows=raw_rows, chunks=chunks,
                    empty_chunks=empty_chunks, bytes=bytes_count, checked_files=cache.checked,
                    reused_files=cache.reused, validator_sha256=contract,
                    summary_sha256=summary_sha, report_sha256=report_sha,
                    source_fingerprint_sha256=source_fingerprint(root),
                    coverage_complete=summary.get("selected_coverage_complete") is True,
                    broker_fills_proven=False, historical_depth_proven=False)
    finally:
        cache.close()


def verify_minute_release_receipt(root: Path, receipt_path: Path) -> dict:
    """Bind an existing full semantic audit to the exact selected minute release."""
    receipt = read_object(receipt_path)
    manifest = read_object(root / "manifest.json")
    if (receipt.get("status") != "research_ready" or receipt.get("failures")
            or receipt.get("manifest_sha256") != sha256_file(root / "manifest.json")
            or receipt.get("partitions") != len(manifest.get("partitions", []))
            or receipt.get("calendar", {}).get("checked") is not True
            or manifest.get("research_ready") is not True):
        raise SnapshotError("minute release has no matching full calendar/semantic audit")
    return receipt
