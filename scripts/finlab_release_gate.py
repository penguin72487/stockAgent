"""Fail-closed FinLab catalog freshness check for a private derived release.

The SDK has no per-key version watermark.  A successful force-refresh check is
therefore evidence only that this account observed the current SDK response at
that instant; it does not prove historical point-in-time values or future
updates.  All catalog keys must pass within one rolling day before packaging.
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
from scripts.download_finlab_history import safe_stem  # noqa: E402
MAX_CHECK_AGE = timedelta(hours=24)


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
    files: list[tuple[str, Path, str]] = []
    digest = hashlib.sha256()
    for key in keys:
        receipt = _read(root / "receipts" / f"{safe_stem(key)}.json")
        if not receipt:
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
        if (receipt.get("source_check_mode") != "upstream_forced"
                or checked is None or not observed - MAX_CHECK_AGE <= checked <= observed):
            counts["stale"] += 1
            continue
        counts["verified"] += 1
        digest.update(key.encode("utf-8"))
        digest.update(receipt["sha256"].encode("ascii"))
        files.append((key, path, receipt["sha256"]))
    catalog_fresh = catalog_at is not None and observed - MAX_CHECK_AGE <= catalog_at <= observed
    ready = bool(keys) and catalog_fresh and counts["verified"] == len(keys)
    if ready and verify_hashes:
        for _key, path, expected in files:
            actual = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                    actual.update(chunk)
            if actual.hexdigest() != expected:
                counts["invalid"] += 1
                counts["verified"] -= 1
        ready = counts["invalid"] == 0
    return {
        "ready": ready,
        "catalog_total": len(keys),
        "catalog_observed_at_utc": catalog_at.isoformat() if catalog_at else None,
        "catalog_fresh": catalog_fresh,
        "max_check_age_hours": 24,
        "missing": counts["missing"],
        "stale": counts["stale"],
        "invalid": counts["invalid"],
        "verified": counts["verified"],
        "hashes_verified": bool(ready and verify_hashes),
        "source_receipts_sha256": digest.hexdigest() if ready else None,
        "checked_at_utc": observed.isoformat(),
        "basis": "每個 SDK 目錄鍵 24 小時內成功查詢且本地檔案存在；發版時另驗 SHA-256。此證據不是歷史 PIT 或逐股完整度。",
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
