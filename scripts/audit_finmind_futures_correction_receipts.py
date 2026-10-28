"""Audit retained FinMind futures objects against three dated corrections.

This reads the canonical, hash-verified announcement snapshot and local
whole-market receipts. It makes no data API call, does not change a queue or
source release, and does not prove the provider's response was complete.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json
from downloader.finmind_announcements import _load_cache


NOTICES = (
    ("2026-09-20", "2018 年部分交易日", 70),
    ("2026-09-17", "2017-05 ~ 2018-03", 43),
    ("2026-09-19", "2018-02-13", 1),
)
DATASETS = ("TaiwanFuturesTick", "TaiwanFuturesKBar")
DATE = re.compile(r"201[78]-\d\d-\d\d")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def inspect_receipt(root: Path, dataset: str, day: str, notice_date: str) -> dict:
    path = root / "receipts" / dataset / "all" / f"{day}.json"
    result = {"dataset": dataset, "partition": day, "notice_date": notice_date,
              "receipt_path": str(path), "state": "receipt_missing"}
    if not path.is_file():
        return result
    try:
        raw = path.read_bytes()
        item = json.loads(raw)
        result["receipt_sha256"] = hashlib.sha256(raw).hexdigest()
        if not isinstance(item, dict):
            result["state"] = "receipt_identity_or_status_invalid"
            return result
        stamp = item.get("fetched_at_utc")
        fetched = datetime.fromisoformat(stamp) if isinstance(stamp, str) else None
        if (item.get("dataset") != dataset or item.get("partition") != day
                or item.get("status") != "complete" or item.get("data_id") != ""
                or fetched is None or fetched.utcoffset() is None):
            result["state"] = "receipt_identity_or_status_invalid"
            return result
        result.update(fetched_at_utc=stamp, rows=item.get("rows"), source_sha256=item.get("sha256"))
        if fetched.astimezone(UTC).date().isoformat() <= notice_date:
            result["state"] = "fetch_not_proven_after_notice"
            return result
        relative = Path(item.get("parquet_path", ""))
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            result["state"] = "invalid_parquet_path"
            return result
        parquet = root / relative
        result["parquet_path"] = str(parquet)
        if not parquet.is_file():
            result["state"] = "parquet_not_materialized"
        elif _sha256(parquet) != item.get("sha256"):
            result["state"] = "parquet_sha256_mismatch"
        else:
            result["state"] = "fetched_after_notice_sha256_verified"
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        result["state"] = "receipt_unreadable"
    return result


def audit(root: Path, now: datetime) -> dict:
    cache, error = _load_cache(root / "announcements", now)
    if cache is None:
        raise ValueError(f"official announcement snapshot unavailable or invalid: {error}")
    notices, results = [], []
    for notice_date, fragment, expected_count in NOTICES:
        found = [entry for entry in cache["entries"]
                 if entry["notice_date"] == notice_date and fragment in entry["leading_text"]
                 and all(dataset in entry["datasets"] for dataset in DATASETS)]
        if len(found) != 1:
            raise ValueError(f"official notice identity changed: {notice_date}")
        entry = found[0]
        date_text = entry["nested_items"][0]["text"] if expected_count > 1 else entry["leading_text"]
        days = sorted(set(DATE.findall(date_text)))
        if len(days) != expected_count:
            raise ValueError(f"official notice date count changed: {notice_date}")
        notices.append({"notice_date": notice_date, "entry_id": entry["entry_id"],
                        "source_url": entry["source_url"], "date_count": len(days)})
        for dataset in DATASETS:
            results.extend(inspect_receipt(root / "complement", dataset, day, notice_date) for day in days)
    return {
        "contract": "finmind_futures_correction_local_receipt_readback_v1",
        "checked_at_utc": now.astimezone(UTC).isoformat(),
        "announcement_snapshot_sha256": cache["snapshot_sha256"],
        "notice_scope": notices, "receipt_count": len(results),
        "state_counts": dict(sorted(Counter(row["state"] for row in results).items())),
        "data_api_requests": 0,
        "limitations": ["Receipt freshness and payload hashes do not prove provider completeness or executable fills.",
                        "This audit does not publish a source release or change any download queue."],
        "receipts": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.root, datetime.now(UTC))
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("receipt_count", "state_counts", "data_api_requests")},
                     ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
