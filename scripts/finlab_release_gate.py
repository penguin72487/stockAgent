"""Verify the complete general-history catalog for a private derived release.

Use the collector's source-expiry clocks, not an unrelated 24-hour check for
monthly tables. Partitioned minute/tick capabilities belong to their original
partition queues, never to data.get(key) without a date window. Retained all-null
source evidence remains an explicit source condition, not fabricated observations.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    UPSTREAM_CHECK_MODES, attempt_retry_at, next_source_check,
    read_verified_empty_evidence, safe_stem,
)
MAX_CHECK_AGE = timedelta(hours=24)
CONTRACT = "finlab_general_catalog_release_v2"


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except (TypeError, ValueError):
        return None


def catalog_readiness(output_root: Path, *, now: datetime | None = None,
                      verify_hashes: bool = False) -> dict:
    """Return bounded public-safe counts; only hash files when all other gates pass."""
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    root = Path(output_root).resolve()
    discovery = _read(root / "catalog/discovery.json")
    keys = discovery.get("keys")
    keys = sorted(set(keys)) if isinstance(keys, list) and all(
        isinstance(key, str) and key for key in keys
    ) else []
    catalog_at = _time(discovery.get("observed_at_utc"))
    counts = {name: 0 for name in ("missing", "stale", "invalid", "verified")}
    partitioned = [key for key in keys if key.split(":", 1)[0] in {"tw_minute", "tw_tick"}]
    partitioned_set = set(partitioned)
    general = [key for key in keys if key not in partitioned_set]
    empty_keys = []
    files: list[tuple[str, Path, str]] = []
    deadlines = [catalog_at + MAX_CHECK_AGE] if catalog_at else []
    digest = hashlib.sha256()
    for key in general:
        receipt = _read(root / "receipts" / f"{safe_stem(key)}.json")
        if not receipt:
            attempt = _read(root / "attempts" / f"{safe_stem(key)}.json")
            empty = read_verified_empty_evidence(key, root, attempt)
            checked = _time(empty.get("observed_at_utc")) if empty else None
            due = attempt_retry_at(attempt) if empty else None
            if empty and checked is not None and checked <= observed and due is not None and observed < due:
                counts["verified"] += 1
                empty_keys.append(key)
                deadlines.append(due)
                digest.update(key.encode("utf-8") + b"\0source_all_null\0" + empty["raw_sha256"].encode("ascii"))
                continue
            counts["missing"] += 1
            continue
        relative = Path(str(receipt.get("parquet_path") or ""))
        path = (root / relative).resolve()
        if (receipt.get("dataset") != key
                or receipt.get("status") != "downloaded_unverified_for_pit"
                or relative.is_absolute() or relative.parts[:1] != ("datasets",)
                or not path.is_relative_to(root) or not path.is_file()
                or not isinstance(receipt.get("sha256"), str)
                or len(receipt["sha256"]) != 64
                or any(char not in "0123456789abcdef" for char in receipt["sha256"])):
            counts["invalid"] += 1
            continue
        checked = _time(receipt.get("source_checked_at_utc"))
        due = next_source_check(receipt)
        if (receipt.get("source_check_mode") not in UPSTREAM_CHECK_MODES
                or checked is None or checked > observed or due is None or not observed < due):
            counts["stale"] += 1
            continue
        counts["verified"] += 1
        deadlines.append(due)
        digest.update(key.encode("utf-8"))
        digest.update(receipt["sha256"].encode("ascii"))
        files.append((key, path, receipt["sha256"]))
    catalog_fresh = catalog_at is not None and observed - MAX_CHECK_AGE <= catalog_at <= observed
    ready = bool(general) and catalog_fresh and counts["verified"] == len(general)
    physical_files_hashed = 0
    hash_bytes_read = 0
    if ready and verify_hashes:
        from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
        verified_inodes = {}
        observations = []
        invalid_keys = set()
        for key, path, expected in files:
            try:
                before = payload_info(path)
                binding = tuple(before[k] for k in ("device", "inode", "size", "mtime_ns", "ctime_ns", "nlink"))
                actual = verified_inodes.get(binding)
                if actual is None:
                    actual = stable_payload_hash(path, before)
                    verified_inodes[binding] = actual
                    physical_files_hashed += 1
                    hash_bytes_read += before["size"]
                observations.append((key, path, before))
            except (OSError, ValueError):
                actual = None
            if actual != expected:
                invalid_keys.add(key)
        # Recheck every logical path, including aliases sharing one inode.
        for key, path, before in observations:
            try:
                unchanged = payload_info(path) == before
            except (OSError, ValueError):
                unchanged = False
            if not unchanged:
                invalid_keys.add(key)
        counts["invalid"] += len(invalid_keys)
        counts["verified"] -= len(invalid_keys)
        ready = counts["invalid"] == 0
    return {
        "contract": CONTRACT,
        "ready": ready,
        "catalog_total": len(keys),
        "general_history_total": len(general),
        "partitioned_endpoints": partitioned,
        "partitioned_history_verified_by_this_gate": False,
        "verified_source_empty": empty_keys,
        "catalog_observed_at_utc": catalog_at.isoformat() if catalog_at else None,
        "catalog_fresh": catalog_fresh,
        "max_check_age_hours": 24,
        "max_check_age_scope": "catalog_discovery_only",
        "source_check_policy": "canonical_collector_sdk_expiry_or_original_retry_deadline",
        "missing": counts["missing"],
        "stale": counts["stale"],
        "invalid": counts["invalid"],
        "verified": counts["verified"],
        "hashes_verified": bool(ready and verify_hashes),
        "physical_files_hashed": physical_files_hashed,
        "hash_bytes_read": hash_bytes_read,
        "source_receipts_sha256": digest.hexdigest() if ready else None,
        "checked_at_utc": observed.isoformat(),
        "valid_until_utc": min(deadlines).isoformat() if ready and deadlines else None,
        "basis": "全部一般歷史目錄鍵依原下載器 SDK 到期時鐘核驗；分區分鐘/tick 另走原佇列。來源全空值須保留原始證據並通過 SHA，絕不補零。不是歷史 PIT 或逐股完整度。",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--verify-hashes", action="store_true")
    args = parser.parse_args()
    status = catalog_readiness(args.output_root, verify_hashes=args.verify_hashes)
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0 if status["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
