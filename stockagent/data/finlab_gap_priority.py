"""Owner-requested gap rechecks consumed by the existing FinLab scheduler.

This is a finite priority intent, not a second downloader or a completeness
receipt. One genuine upstream check discharges an intent even if NULL remains.
Source cooldowns, quota and account locks continue to belong to the collector.
"""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

from stockagent.data.finlab_acquisition_contract import UPSTREAM_CHECK_MODES, safe_stem, utc_time

CONTRACT = "finlab_owner_gap_recheck_priority_v1"
DAYTRADE_PRIORITY_REASON = "當沖模型已登錄依賴的到期來源核對"


def pending_gap_requests(root: Path, *, now: datetime, available=None) -> dict[str, dict]:
    path = root / "gap_priority_requests.json"
    if not path.exists():
        return {}
    if path.is_symlink() or not path.is_file():
        raise ValueError("unsafe owner gap priority intent")
    plan = json.loads(path.read_text())
    if not isinstance(plan, dict):
        raise ValueError("invalid owner gap priority document")
    rows = plan.get("requests")
    if plan.get("contract") != CONTRACT or not isinstance(rows, list):
        raise ValueError("invalid owner gap priority contract")
    selected = {}
    receipts = {}
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
                or row.get("reason") not in {"interior_observation_gap_recheck", "source_integrity_repair", "interactive_query"}):
            raise ValueError("invalid gap recheck identity or evidence")
        if required > now or expiry <= now or (allowed is not None and key not in allowed):
            continue
        if key not in receipts:
            try:
                receipt = json.loads((root / "receipts" / (safe_stem(key) + ".json")).read_text())
            except (OSError, ValueError):
                receipt = {}
            receipts[key] = receipt if isinstance(receipt, dict) else {}
        receipt = receipts[key]
        checked = utc_time(receipt.get("source_checked_at_utc"))
        if (receipt.get("dataset") == key and receipt.get("source_check_mode") in UPSTREAM_CHECK_MODES
                and checked and required <= checked <= now):
            continue
        if key not in selected or row["priority"] < selected[key]["priority"]:
            selected[key] = row
    return selected


def pending_daytrade_priority(root: Path, *, now: datetime, available=None) -> bool:
    """Recognize a still-pending finite day-trade intent by its sealed plan.

    A request's label in the common registry is deliberately generic. Check
    the plan's content hash, reason, exact key and actual source-check clock;
    a different interactive request must not preempt account work merely
    because it shares a dataset name.
    """
    pending = pending_gap_requests(root, now=now, available=available)
    if not pending:
        return False
    document = json.loads((root / "gap_priority_requests.json").read_text())
    plans: dict[str, dict] = {}
    for row in document["requests"]:
        key, digest = row["dataset"], row["evidence_sha256"]
        required, expiry = utc_time(row["required_after_utc"]), utc_time(row["expires_at_utc"])
        if key not in pending or required is None or expiry is None or not required <= now < expiry:
            continue
        if digest not in plans:
            path = root / "priority_queries" / (digest + ".json")
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                receipt = json.loads(path.read_text())
                plan = receipt["plan"]
                if not isinstance(plan, dict) or not isinstance(plan.get("items"), list):
                    continue
                encoded = json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()
                if receipt.get("request_id") != digest or hashlib.sha256(encoded).hexdigest() != digest:
                    continue
                plans[digest] = plan
            except (OSError, ValueError, TypeError, KeyError):
                continue
        plan = plans[digest]
        if (plan.get("provider") != "finlab" or plan.get("reason") != DAYTRADE_PRIORITY_REASON
                or key not in plan.get("items", [])):
            continue
        try:
            receipt = json.loads((root / "receipts" / (safe_stem(key) + ".json")).read_text())
        except (OSError, ValueError):
            receipt = {}
        checked = utc_time(receipt.get("source_checked_at_utc")) if isinstance(receipt, dict) else None
        if not (isinstance(receipt, dict) and receipt.get("dataset") == key
                and receipt.get("source_check_mode") in UPSTREAM_CHECK_MODES
                and checked and required <= checked <= now):
            return True
    return False
