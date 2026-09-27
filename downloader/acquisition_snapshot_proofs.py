"""Small local proofs for FinMind's current calendar and security snapshot.

These are admission hints for optional API validation, never a historical
coverage/PIT certificate. ``root`` is the existing ``data_finmind`` directory.
No function calls a provider or writes data. The calendar is a current JSON
observation without a separate producer hash; it is not labelled hash-verified.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, time as wall_time, timedelta
from functools import lru_cache
import json
from pathlib import Path
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

from downloader.artifact_io import sha256_file

CALENDAR = "TaiwanStockTradingDate"
MASTER = "TaiwanStockInfoWithWarrant"
TAIPEI = ZoneInfo("Asia/Taipei")
MAX_AGE = timedelta(hours=20)
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_MASTER_BYTES = 256 * 1024 * 1024


def _identity(path: Path) -> tuple[int, ...]:
    value = path.stat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_json(path: Path, root: Path) -> tuple[dict[str, Any], tuple[int, ...]]:
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("snapshot receipt is outside its source root")
    identity = _identity(path)
    if not 0 < identity[2] <= MAX_JSON_BYTES:
        raise ValueError("snapshot receipt exceeds local read bound")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or _identity(path) != identity:
        raise ValueError("snapshot receipt changed or is not an object")
    return value, identity


def _fresh(value: Any, now: datetime, *, max_age: timedelta = MAX_AGE) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if observed.tzinfo is None:
        return None
    observed = observed.astimezone(UTC)
    return observed if timedelta(0) <= now - observed < max_age else None


def _calendar_ready(root: Path, now: datetime) -> bool:
    path = root / "calendar.json"
    receipt, signature = _read_json(path, root)
    if (type(receipt.get("schema_version")) is not int or receipt.get("schema_version") != 1
            or receipt.get("source_dataset") != CALENDAR
            or _fresh(receipt.get("observed_at_utc"), now) is None):
        return False
    values = receipt.get("dates")
    if not isinstance(values, list) or not values or len(values) > 50_000:
        return False
    previous: date | None = None
    for value in values:
        if not isinstance(value, str):
            return False
        # Future *calendar dates* are expected; future observation timestamps
        # are not. Do not trim strings or deduplicate corrupted source entries.
        parsed = date.fromisoformat(value)
        if (parsed.isoformat() != value or parsed < date(2005, 1, 1)
                or previous is not None and parsed <= previous):
            return False
        previous = parsed
    return _identity(path) == signature


@lru_cache(maxsize=16)
def _master_bytes_ready(path_text: str, identity: tuple[int, ...], expected_hash: str,
                        expected_rows: int, epoch: int) -> bool:
    del epoch
    import pyarrow.parquet as pq

    path = Path(path_text)
    if _identity(path) != identity or sha256_file(path) != expected_hash:
        return False
    if pq.read_metadata(path).num_rows != expected_rows:
        return False
    return _identity(path) == identity


def _master_ready(root: Path, now: datetime) -> bool:
    local = now.astimezone(TAIPEI)
    today = local.date()
    # Match the existing Free worker's _master_due 14:00 daily boundary.
    # Unlike the calendar's 20h refresh, yesterday's master is still current
    # before that boundary. Do not invent four hours of acquisition debt.
    days = (today,) if local.time() >= wall_time(14) else (today, today - timedelta(days=1))
    for day in days:
        receipt_path = root / "receipts" / MASTER / f"{day}.json"
        if not receipt_path.exists():
            continue
        receipt, receipt_signature = _read_json(receipt_path, root)
        observed = _fresh(receipt.get("fetched_at_utc"), now, max_age=timedelta(days=2))
        if (type(receipt.get("schema_version")) is not int or receipt.get("schema_version") != 1
                or receipt.get("dataset") != MASTER
                or receipt.get("status") != "complete"
                or receipt.get("query_scope") != "full_table_snapshot"
                or receipt.get("snapshot_date_taipei") != str(day)
                or observed is None or observed.astimezone(TAIPEI).date() != day):
            return False
        relative = f"snapshots/{MASTER}/snapshot={day}-full.parquet"
        if receipt.get("parquet_path") != relative:
            return False
        source = root / relative
        if not source.resolve().is_relative_to(root.resolve()):
            return False
        rows, size, digest = receipt.get("rows"), receipt.get("parquet_size_bytes"), receipt.get("sha256")
        if (not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0
                or not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= MAX_MASTER_BYTES
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            return False
        identity = _identity(source)
        if identity[2] != size:
            return False
        valid = _master_bytes_ready(str(source.resolve()), identity, digest, rows,
                                    int(time.monotonic() // 30))
        return valid and _identity(source) == identity and _identity(receipt_path) == receipt_signature
    return False


def finmind_snapshot_ready(root: Path, endpoint_id: str, now: datetime) -> bool | None:
    """True=current checked snapshot, False=missing/unverified, None=other ID."""
    if endpoint_id not in {f"finmind:{CALENDAR}", f"finmind:{MASTER}"}:
        return None
    if not isinstance(now, datetime) or now.tzinfo is None:
        return False
    try:
        checked = now.astimezone(UTC)
        return (_calendar_ready(Path(root), checked) if endpoint_id == f"finmind:{CALENDAR}"
                else _master_ready(Path(root), checked))
    except (OSError, ValueError, TypeError, OverflowError):
        # PyArrow malformed-file/schema errors inherit ValueError or OSError.
        return False
