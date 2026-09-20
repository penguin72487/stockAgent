#!/usr/bin/env python3
"""Import an audited local MOPS ZIP set through the canonical XBRL parser.

This is an offline import. Local files never gain verified-source or historical
point-in-time status merely because their ZIPs and normalized facts are valid.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import multiprocessing
from pathlib import Path
import shutil
import sys
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.download_tw_mops_xbrl import (
    ArchiveLink, FILE_PATTERN, TAIPEI, _ingest, _quarter_end, summarize,
)


def _load_inventory(audit_path: Path, input_dir: Path) -> list[ArchiveLink]:
    audit = json.loads(audit_path.read_text(encoding="utf-8-sig"))
    records = audit.get("records")
    if (not isinstance(records, list) or not records
            or audit.get("remaining_count") != 0
            or audit.get("valid_count") != audit.get("completed_quarter_links")
            or len(records) != audit.get("completed_quarter_links")):
        raise ValueError("local MOPS audit is incomplete or malformed")
    links: list[ArchiveLink] = []
    names: set[str] = set()
    for record in records:
        if not isinstance(record, dict) or record.get("status") != "valid":
            raise ValueError("MOPS audit contains an unverified local ZIP")
        name = str(record.get("name") or "")
        match = FILE_PATTERN.fullmatch(name)
        if not match or name in names:
            raise ValueError(f"invalid or duplicate MOPS ZIP name: {name!r}")
        names.add(name)
        standard = "ifrs" if match.group(1) == "tifrs" else "tw_gaap"
        year, quarter = int(match.group(2)), int(match.group(3))
        if _quarter_end(year, quarter) >= datetime.now(TAIPEI).date():
            raise ValueError(f"unfinished MOPS quarter: {name}")
        url = str(record.get("url") or "")
        parsed = urlparse(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        folder = "ifrs" if standard == "ifrs" else "xbrl"
        if (parsed.scheme != "https" or parsed.hostname != "mopsov.twse.com.tw"
                or parsed.path != "/server-java/FileDownLoad"
                or set(query) != {"step", "functionName", "fileName", "filePath"}
                or query["step"] != ["9"]
                or query["functionName"] != ["show_file2"]
                or query["fileName"] != [name]
                or query["filePath"] != [f"/{folder}/{year}/"]):
            raise ValueError(f"MOPS audit link changed or is unsafe: {name}")
        path = input_dir / name
        if not path.is_file() or path.stat().st_size != record.get("bytes"):
            raise ValueError(f"MOPS ZIP missing or changed size since audit: {name}")
        links.append(ArchiveLink(year, quarter, standard, name, url))
    extra = {path.name for path in input_dir.glob("*.zip")} - names
    if extra:
        raise ValueError(f"untracked top-level MOPS ZIPs: {sorted(extra)}")
    return sorted(links)


def _import_one(input_path: str, output_dir: str) -> dict[str, object]:
    return _ingest(Path(input_path), root=Path(output_dir), source_url=None)


def _progress(status: str, *, fingerprint: str, total: int,
              receipts: dict[str, dict[str, object]],
              failures: dict[str, str], input_dir: Path,
              audit_path: Path) -> dict[str, object]:
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "status": status,
        "input_dir": str(input_dir),
        "audit_report": str(audit_path),
        "inventory_fingerprint_sha256": fingerprint,
        "total_periods": total,
        "imported_periods_this_run": len(receipts),
        "failed_periods_this_run": len(failures),
        "archive_bytes_this_run": sum(int(item["archive_bytes"]) for item in receipts.values()),
        "fact_rows_this_run": sum(int(item["fact_count"]) for item in receipts.values()),
        "receipts": receipts,
        "failures": failures,
        "source_authenticity_verified": False,
        "historical_point_in_time": False,
        "training_eligible": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--audit-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.workers <= 8:
        parser.error("--workers must be in 1..8")
    input_dir = args.input_dir.resolve(strict=True)
    audit_path = args.audit_report.resolve(strict=True)
    output_dir = args.output_dir.resolve()
    links = _load_inventory(audit_path, input_dir)
    free_bytes = shutil.disk_usage(output_dir.parent).free
    if free_bytes < 40 * 1024**3:
        raise RuntimeError("less than 40 GiB free at the MOPS import destination")
    print(json.dumps({"audit": str(audit_path), "archives": len(links),
                      "workers": args.workers, "free_bytes": free_bytes,
                      "output_dir": str(output_dir)}, ensure_ascii=False), flush=True)
    if args.dry_run:
        return 0

    lock_path = output_dir.parent.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        print(json.dumps({"waiting_for_lock": str(lock_path)}), flush=True)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        output_dir.mkdir(parents=True, exist_ok=True)
        progress_path = output_dir / "local_import_progress.json"
        fingerprint = hashlib.sha256(json.dumps(
            [(link.filename, (input_dir / link.filename).stat().st_size, link.url)
             for link in links], sort_keys=True
        ).encode()).hexdigest()
        receipts: dict[str, dict[str, object]] = {}
        failures: dict[str, str] = {}
        atomic_write_json(progress_path, _progress(
            "running", fingerprint=fingerprint, total=len(links),
            receipts=receipts, failures=failures, input_dir=input_dir,
            audit_path=audit_path,
        ))
        with ProcessPoolExecutor(
            max_workers=args.workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(_import_one, str(input_dir / link.filename), str(output_dir)): link
                for link in links
            }
            for future in as_completed(futures):
                link = futures[future]
                try:
                    receipt = future.result()
                except Exception as exc:
                    failures[link.filename] = f"{type(exc).__name__}: {exc}"
                    print(json.dumps({"archive": link.filename, "status": "failed",
                                      "error": failures[link.filename]}, ensure_ascii=False), flush=True)
                else:
                    receipts[link.filename] = {
                        "archive_sha256": receipt["archive_sha256"],
                        "archive_bytes": receipt["archive_bytes"],
                        "fact_count": receipt["fact_count"],
                        "facts_sha256": receipt["facts_sha256"],
                        "raw_path": receipt["raw_path"],
                        "facts_path": receipt["facts_path"],
                    }
                    print(json.dumps({"archive": link.filename, "status": "imported",
                                      "completed": len(receipts), "total": len(links),
                                      "fact_count": receipt["fact_count"]}, ensure_ascii=False), flush=True)
                atomic_write_json(progress_path, _progress(
                    "running", fingerprint=fingerprint, total=len(links),
                    receipts=receipts, failures=failures, input_dir=input_dir,
                    audit_path=audit_path,
                ))
        summary = summarize(links, output_dir, authorized=False)
        complete = not failures and summary["local_imported_periods"] == len(links)
        summary["local_import_audit_report"] = str(audit_path)
        summary["local_import_progress"] = str(progress_path)
        atomic_write_json(output_dir / "state.json", summary)
        atomic_write_json(progress_path, _progress(
            "complete" if complete else "partial", fingerprint=fingerprint,
            total=len(links), receipts=receipts, failures=failures,
            input_dir=input_dir, audit_path=audit_path,
        ))
        print(json.dumps({"local_import_complete": complete,
                          "local_imported_periods": summary["local_imported_periods"],
                          "verified_source_periods": summary["completed_periods"],
                          "local_fact_rows": summary["local_fact_rows"],
                          "failures": failures}, ensure_ascii=False), flush=True)
        return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
