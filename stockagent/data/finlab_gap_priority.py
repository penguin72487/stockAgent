"""Owner-requested gap rechecks consumed by the existing FinLab scheduler.

This is a finite priority intent, not a second downloader or a completeness
receipt. One genuine upstream check discharges an intent even if NULL remains.
Source cooldowns, quota and account locks continue to belong to the collector.
"""
from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import re

from stockagent.data.finlab_acquisition_contract import UPSTREAM_CHECK_MODES, safe_stem, utc_time

CONTRACT = "finlab_owner_gap_recheck_priority_v1"
MAX_REQUESTS = 4096


def pending_gap_requests(root: Path, *, now: datetime, available=None) -> dict[str, dict]:
    path = root / "gap_priority_requests.json"
    if not path.exists():
        return {}
    if path.is_symlink() or path.stat().st_size > 4 * 1024**2:
        raise ValueError("unsafe or oversized owner gap priority intent")
    plan = json.loads(path.read_text())
    if not isinstance(plan, dict):
        raise ValueError("invalid owner gap priority document")
    rows = plan.get("requests")
    if (plan.get("contract") != CONTRACT or not isinstance(rows, list)
            or len(rows) > MAX_REQUESTS):
        raise ValueError("invalid owner gap priority contract")
    selected = {}
    allowed = None if available is None else set(available)
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid owner gap priority request")
        key = row.get("dataset")
        required = utc_time(row.get("required_after_utc"))
        expiry = utc_time(row.get("expires_at_utc"))
        if (not isinstance(key, str) or not key or len(key) > 512
                or not required or not expiry or expiry <= required
                or not row.get("request_id")
                or not re.fullmatch(r"[0-9a-f]{64}", str(row.get("evidence_sha256", "")))
                or type(row.get("priority")) is not int or not 0 <= row["priority"] <= 100
                or row.get("reason") not in {"interior_observation_gap_recheck", "source_integrity_repair"}):
            raise ValueError("invalid gap recheck identity or evidence")
        if required > now or expiry <= now or (allowed is not None and key not in allowed):
            continue
        try:
            receipt = json.loads((root / "receipts" / (safe_stem(key) + ".json")).read_text())
        except (OSError, ValueError):
            receipt = {}
        if not isinstance(receipt, dict):
            receipt = {}
        checked = utc_time(receipt.get("source_checked_at_utc"))
        if (receipt.get("dataset") == key and receipt.get("source_check_mode") in UPSTREAM_CHECK_MODES
                and checked and required <= checked <= now):
            continue
        if key not in selected or row["priority"] < selected[key]["priority"]:
            selected[key] = row
    return selected
