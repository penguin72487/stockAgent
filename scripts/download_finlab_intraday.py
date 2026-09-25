#!/usr/bin/env python3
"""Bounded, receipt-backed FinLab stock-day tick downloads.

The FinLab catalog advertises intraday *families*, not a complete history.
Each day is requested separately so an unpublished day cannot hide good days.
No partition is treated as a point-in-time research release.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import gc
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_finlab_history import (  # noqa: E402
    DEFAULT_OUTPUT, TAIPEI, _atomic_json, classify_provider_error,
    credential_available, quota_cycle_start, quota_room_mb, safe_stem,
)


DEFAULT_START = date(2026, 6, 1)  # FinLab's documented free sample, not earliest coverage.
FAMILIES = ("tw_tick:2330",)


def _partition_paths(root: Path, key: str, day: date) -> tuple[Path, Path]:
    stem = safe_stem(key)
    name = f"{day.isoformat()}.json"
    return (root / "intraday/receipts" / stem / name,
            root / "intraday/attempts" / stem / name)


def _checked_this_cycle(path: Path, field: str, now: datetime) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        checked = datetime.fromisoformat(payload[field])
        return checked.tzinfo is not None and checked.astimezone(UTC) >= quota_cycle_start(now)
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _stored_receipt(path: Path, root: Path, key: str, day: date) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("dataset") != key or payload.get("trade_date") != day.isoformat():
            return None
        if payload.get("status") in {"verified_closed_date", "verified_no_trade"}:
            return payload
        relative = Path(payload["parquet_path"])
        object_path = root / relative
        if (payload.get("status") == "downloaded_unverified_for_pit"
                and not relative.is_absolute() and relative.parts[:2] == ("intraday", "objects")
                and ".." not in relative.parts and object_path.is_file()
                and object_path.stat().st_size == int(payload.get("parquet_size_bytes", -1))
                and re.fullmatch(r"[0-9a-f]{64}", str(payload.get("sha256", "")))):
            return payload
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def _validate_frame(frame: pd.DataFrame, key: str, day: date) -> str:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("intraday response is not a DataFrame")
    listed = {str(value) for value in frame.attrs.get("closed_dates", [])}
    if day.isoformat() in listed:
        if not frame.empty:
            raise ValueError("closed date contains trades")
        return "verified_closed_date"
    listed = {str(value) for value in frame.attrs.get("no_trade_dates", [])}
    if day.isoformat() in listed:
        if not frame.empty:
            raise ValueError("no-trade date contains trades")
        return "verified_no_trade"
    if frame.empty:
        raise ValueError("empty intraday frame lacks provider closed/no-trade evidence")
    required = {"stock_id", "trade_date", "timestamp"}
    if not required.issubset(frame.columns):
        raise ValueError("intraday schema lacks stock/date/timestamp")
    symbol = key.split(":", 1)[1]
    if frame["stock_id"].isna().any() or not frame["stock_id"].astype(str).eq(symbol).all():
        raise ValueError("intraday rows contain another stock")
    observed_days = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    if observed_days.isna().any() or not observed_days.eq(day).all():
        raise ValueError("intraday rows contain another trade date")
    if not isinstance(frame["timestamp"].dtype, pd.DatetimeTZDtype):
        raise ValueError("intraday timestamps must carry timezone")
    if frame["timestamp"].isna().any():
        raise ValueError("intraday timestamps contain nulls")
    if key.startswith("tw_tick:"):
        if "sequence" not in frame.columns:
            raise ValueError("tick rows need source sequence; same timestamps are distinct trades")
        if frame.duplicated(["timestamp", "sequence"]).any():
            raise ValueError("duplicate tick timestamp and source sequence")
    else:
        required_prices = {"open", "high", "low", "close", "volume"}
        if not required_prices.issubset(frame.columns):
            raise ValueError("minute bars lack OHLCV")
        if frame["timestamp"].duplicated().any():
            raise ValueError("duplicate minute timestamp")
        if ((frame["high"] < frame[["open", "low", "close"]].max(axis=1))
                | (frame["low"] > frame[["open", "high", "close"]].min(axis=1))
                | (frame["volume"] < 0)).any():
            raise ValueError("invalid minute OHLCV range")
    return "downloaded_unverified_for_pit"


def fetch_partition(root: Path, key: str, day: date, *, now: datetime) -> dict:
    from finlab import data

    frame = data.get(key, start=day.isoformat(), end=day.isoformat(),
                     force_download=True, progress="silent")
    status = _validate_frame(frame, key, day)
    receipt_path, _ = _partition_paths(root, key, day)
    payload = {
        "schema_version": 1, "dataset": key, "trade_date": day.isoformat(),
        "status": status, "source_checked_at_utc": datetime.now(UTC).isoformat(),
        "rows": len(frame), "fields": list(map(str, frame.columns)),
        "first_timestamp": str(frame["timestamp"].min()) if len(frame) else None,
        "last_timestamp": str(frame["timestamp"].max()) if len(frame) else None,
        "source_grain": "stock_trade_day_tick" if key.startswith("tw_tick:")
                        else "stock_trade_day_left_labelled_minute",
        "publication_time_status": "not_verified_for_training",
    }
    if status == "downloaded_unverified_for_pit":
        objects = root / "intraday/objects"
        objects.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=objects, prefix=".finlab-intraday-", suffix=".parquet.tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
        try:
            frame.to_parquet(temporary, index=False, compression="zstd")
            digest = hashlib.sha256()
            with temporary.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            digest_text = digest.hexdigest()
            destination = objects / f"{safe_stem(key)}-{day.isoformat()}-{digest_text[:24]}.parquet"
            if destination.exists():
                existing = hashlib.sha256()
                with destination.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                        existing.update(chunk)
                if existing.hexdigest() != digest_text:
                    raise ValueError("intraday content-addressed object hash mismatch")
            else:
                os.replace(temporary, destination)
            payload.update({
                "parquet_path": str(destination.relative_to(root)),
                "parquet_size_bytes": destination.stat().st_size,
                "sha256": digest_text,
            })
        finally:
            temporary.unlink(missing_ok=True)
    _atomic_json(receipt_path, payload)
    return payload


def record_failed_partition(root: Path, key: str, day: date, exc: Exception) -> str:
    """Keep actionable provider code without persisting private response text."""
    _, attempt_path = _partition_paths(root, key, day)
    provider_code_match = re.match(r"^([A-Za-z][A-Za-z0-9_]{0,64}):", str(exc))
    provider_code = provider_code_match.group(1) if provider_code_match else None
    status = "partition_not_ready" if provider_code == "not_ready" else classify_provider_error(exc)
    _atomic_json(attempt_path, {
        "dataset": key, "trade_date": day.isoformat(),
        "status": status, "provider_code": provider_code,
        "exception_class": type(exc).__name__,
        "attempted_at_utc": datetime.now(UTC).isoformat(),
        "message_retained": False,
    })
    return status


def _task_days(start: date, end: date) -> list[date]:
    weekdays = [start + timedelta(days=offset)
                for offset in range((end - start).days + 1)
                if (start + timedelta(days=offset)).weekday() < 5]
    if not weekdays:
        return []
    return [weekdays[0], weekdays[-1], *weekdays[1:-1]] if len(weekdays) > 1 else weekdays


def sync(root: Path, *, start: date, end: date, limit: int,
         reserve_mb: float, now: datetime) -> dict:
    if not credential_available():
        raise RuntimeError("no usable FinLab session")
    discovered = json.loads((root / "catalog/discovery.json").read_text(encoding="utf-8"))
    keys = [key for key in FAMILIES if key in discovered.get("keys", [])]
    days = _task_days(start, end)
    attempts = successes = 0
    stopped = "pass_complete"
    for day in days:
        for key in keys:
            receipt_path, attempt_path = _partition_paths(root, key, day)
            receipt = _stored_receipt(receipt_path, root, key, day)
            # Historical partitions are immutable in the local ledger; revisit
            # the latest five calendar days once per quota cycle for revisions.
            if receipt and (end - day).days > 5:
                continue
            if receipt and _checked_this_cycle(receipt_path, "source_checked_at_utc", now):
                continue
            if _checked_this_cycle(attempt_path, "attempted_at_utc", now):
                continue
            if attempts >= limit:
                stopped = "batch_limit"
                break
            room = quota_room_mb()
            if room is None:
                stopped = "quota_unknown"
                break
            if room[0] <= reserve_mb:
                stopped = "quota_margin_reached"
                break
            attempts += 1
            try:
                receipt = fetch_partition(root, key, day, now=datetime.now(UTC))
                successes += 1
                print(f"[finlab-intraday] {key} {day}: {receipt['status']} rows={receipt['rows']}", flush=True)
            except Exception as exc:
                record_failed_partition(root, key, day, exc)
                print(f"[finlab-intraday] {key} {day}: {type(exc).__name__}; message withheld", flush=True)
            gc.collect()
        if stopped != "pass_complete":
            break
    requested = len(keys) * len(days)
    by_key = {}
    for key in keys:
        received = [
            receipt for day in days
            if (receipt := _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)) is not None
        ]
        data_dates = [receipt["trade_date"] for receipt in received
                      if receipt["status"] == "downloaded_unverified_for_pit"]
        receipted_days = {receipt["trade_date"] for receipt in received}
        not_ready = 0
        other_failed = 0
        for day in days:
            if day.isoformat() in receipted_days:
                continue
            attempt = _partition_paths(root, key, day)[1]
            try:
                status = json.loads(attempt.read_text(encoding="utf-8")).get("status")
                not_ready += status == "partition_not_ready"
                other_failed += bool(status) and status != "partition_not_ready"
            except (OSError, ValueError):
                pass
        by_key[key] = {
            "requested_weekday_partitions": len(days),
            "receipted_partitions": len(received),
            "rows": sum(int(receipt.get("rows") or 0) for receipt in received),
            "parquet_bytes": sum(int(receipt.get("parquet_size_bytes") or 0) for receipt in received),
            "provider_not_ready_partitions": not_ready,
            "other_failed_partitions": other_failed,
            "first_data_date": min(data_dates) if data_dates else None,
            "last_data_date": max(data_dates) if data_dates else None,
        }
    stored = sum(item["receipted_partitions"] for item in by_key.values())
    summary = {
        "schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "catalog_keys": keys, "requested_weekday_partitions": requested,
        "receipted_partitions": stored, "remaining_partitions": requested - stored,
        "by_key": by_key,
        "attempted_this_run": attempts, "successes_this_run": successes,
        "state": stopped,
        "coverage_basis": "documented_sample_date_forward; weekday stock-days, including provider-verified closures; earlier FinLab history unknown",
    }
    _atomic_json(root / "intraday/status.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--end-date", type=date.fromisoformat,
                        default=datetime.now(TAIPEI).date() - timedelta(days=1))
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--reserve-mb", type=float, default=50.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.end_date < args.start_date or args.limit < 1 or args.reserve_mb < 0:
        parser.error("invalid intraday range, limit, or quota reserve")
    result = sync(args.output_root, start=args.start_date, end=args.end_date,
                  limit=args.limit, reserve_mb=args.reserve_mb, now=datetime.now(UTC))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 1 if result["state"] == "quota_unknown" else 0


if __name__ == "__main__":
    raise SystemExit(main())
