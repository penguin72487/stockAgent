#!/usr/bin/env python3
"""Bounded, receipt-backed FinLab stock-day tick downloads.

The FinLab catalog advertises intraday *families*, not a complete history.
Bounded official ranges share request overhead; missing days retain evidence.
No partition is treated as a point-in-time research release.
"""

from __future__ import annotations

import argparse
import ast
from datetime import UTC, date, datetime, timedelta
import gc
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_finlab_history import (  # noqa: E402
    DEFAULT_OUTPUT, TAIPEI, _atomic_json, classify_provider_error,
    credential_available, quota_cycle_start, quota_room_mb, safe_stem,
)
from stockagent.data.finlab_transport import (  # noqa: E402
    HTTP_COUNTERS, WorkerHTTPTransport, SignedMinutePrefetch, validate_prefetch_sdk,
)
from scripts.finlab_columnar import (  # noqa: E402
    PROCESSING_CONTRACT_VERSION, PROCESSING_ENGINE, date_column, source_attrs,
    to_polars, write_parquet, timestamp_bounds,
)


DEFAULT_START = date(2026, 6, 1)  # FinLab's documented free sample, not earliest coverage.
FAMILIES = ("tw_tick:2330",)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _partition_paths(root: Path, key: str, day: date) -> tuple[Path, Path]:
    stem = safe_stem(key)
    name = f"{day.isoformat()}.json"
    return (root / "intraday/receipts" / stem / name,
            root / "intraday/attempts" / stem / name)


def _checked_this_cycle(path: Path, field: str, now: datetime) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        checked = datetime.fromisoformat(payload[field])
        this_cycle = checked.tzinfo is not None and checked.astimezone(UTC) >= quota_cycle_start(now)
        if this_cycle and (payload.get("provider_code") == "request_conflict"
                           or payload.get("status") == "provider_error"):
            attempts = payload.get("cycle_attempts", 1)
            retry = datetime.fromisoformat(payload.get("retry_at_utc") or (checked + timedelta(seconds=30)).isoformat())
            return type(attempts) is not int or attempts >= 3 or retry.tzinfo is None or now < retry
        if this_cycle and payload.get("status") == "source_rate_limited":
            retry = datetime.fromisoformat(payload.get("retry_at_utc") or (checked + timedelta(seconds=60)).isoformat())
            return retry.tzinfo is None or now < retry
        return this_cycle
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


def _validate_frame(frame, key: str, day: date, *, attrs: dict | None = None) -> str:
    attrs = source_attrs(frame) if attrs is None else attrs
    frame = to_polars(frame)
    listed = {str(value) for value in attrs.get("closed_dates", [])}
    if day.isoformat() in listed:
        if not frame.is_empty():
            raise ValueError("closed date contains trades")
        return "verified_closed_date"
    listed = {str(value) for value in attrs.get("no_trade_dates", [])}
    if day.isoformat() in listed:
        if not frame.is_empty():
            raise ValueError("no-trade date contains trades")
        return "verified_no_trade"
    if frame.is_empty():
        raise ValueError("empty intraday frame lacks provider closed/no-trade evidence")
    required = {"stock_id", "trade_date", "timestamp"}
    if not required.issubset(frame.columns):
        raise ValueError("intraday schema lacks stock/date/timestamp")
    symbol = key.split(":", 1)[1]
    if frame["stock_id"].null_count() or not (frame["stock_id"].cast(pl.String) == symbol).all():
        raise ValueError("intraday rows contain another stock")
    observed_days = date_column(frame)
    if observed_days.null_count() or not (observed_days == day).all():
        raise ValueError("intraday rows contain another trade date")
    dtype = frame.schema["timestamp"]
    if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
        raise ValueError("intraday timestamps must carry timezone")
    if frame["timestamp"].null_count():
        raise ValueError("intraday timestamps contain nulls")
    local_times = frame["timestamp"].dt.convert_time_zone("Asia/Taipei")
    if key.startswith(("tw_tick:", "tw_minute:")) and not (local_times.dt.date() == day).all():
        raise ValueError("intraday timestamps contain another Taipei trade date")
    if key.startswith("tw_tick:"):
        if "sequence" not in frame.columns:
            raise ValueError("tick rows need source sequence; same timestamps are distinct trades")
        if frame.select("timestamp", "sequence").is_duplicated().any():
            raise ValueError("duplicate tick timestamp and source sequence")
        if "volume" not in frame.columns:
            raise ValueError("tick rows need source volume")
        volume = frame["volume"].cast(pl.Float64, strict=False)
        if volume.null_count() or not volume.is_finite().all() or (volume < 0).any():
            raise ValueError("invalid provider-native tick volume")
    else:
        required_prices = {"open", "high", "low", "close", "volume"}
        if not required_prices.issubset(frame.columns):
            raise ValueError("minute bars lack OHLCV")
        prices = frame.select(pl.col("open", "high", "low", "close").cast(pl.Float64, strict=False))
        if not prices.select(pl.all_horizontal(pl.all().is_finite() & (pl.all() > 0)).fill_null(False).all()).item():
            raise ValueError("invalid non-finite/non-positive minute prices")
        if (local_times.cast(pl.Int64) % (60 * {"ns": 10**9, "us": 10**6, "ms": 10**3}[dtype.time_unit]) != 0).any():
            raise ValueError("minute timestamps are not minute aligned")
        if frame["timestamp"].is_duplicated().any():
            raise ValueError("duplicate minute timestamp")
        volume = frame["volume"].cast(pl.Float64, strict=False)
        if volume.null_count() or not volume.is_finite().all():
            raise ValueError("invalid provider-native minute volume")
        if (prices.select((pl.col("high") < pl.max_horizontal("open", "low", "close"))
                          | (pl.col("low") > pl.min_horizontal("open", "high", "close"))).to_series().any()
                or (volume < 0).any()):
            raise ValueError("invalid minute OHLCV range")
    return "downloaded_unverified_for_pit"


def fetch_partition(root: Path, key: str, day: date, *, now: datetime,
                    reference_root: Path | None = None, force_download: bool = True) -> dict:
    from finlab import data

    frame = data.get(key, start=day.isoformat(), end=day.isoformat(),
                     force_download=force_download, progress="silent")
    return persist_partition_frame(root, key, day, frame, reference_root=reference_root)


def persist_partition_frame(root: Path, key: str, day: date, frame, *,
                            reference_root: Path | None = None, attrs: dict | None = None) -> dict:
    """One shared validation/unit/object/receipt path for single dates and ranges."""
    attrs = source_attrs(frame) if attrs is None else attrs
    frame = to_polars(frame)
    status = _validate_frame(frame, key, day, attrs=attrs)
    volume_evidence = {"raw_volume_unit": "provider_native",
                       "canonical_volume_unit": "unresolved"}
    if status == "downloaded_unverified_for_pit" and key.startswith(("tw_tick:", "tw_minute:")):
        from scripts.finlab_volume_units import with_verified_volume_shares

        frame, volume_evidence = with_verified_volume_shares(
            frame, key.split(":", 1)[1], day, reference_root=reference_root,
        )
    receipt_path, _ = _partition_paths(root, key, day)
    first_timestamp, last_timestamp = timestamp_bounds(frame)
    payload = {
        "schema_version": 2, "dataset": key, "trade_date": day.isoformat(),
        "status": status, "source_checked_at_utc": datetime.now(UTC).isoformat(),
        "rows": len(frame), "fields": list(map(str, frame.columns)),
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
        "source_grain": "stock_trade_day_tick" if key.startswith("tw_tick:")
                        else "stock_trade_day_left_labelled_minute",
        "publication_time_status": "not_verified_for_training",
        "processing_contract_version": PROCESSING_CONTRACT_VERSION,
        "processing_engine": PROCESSING_ENGINE,
        **volume_evidence,
    }
    if status == "downloaded_unverified_for_pit":
        objects = root / "intraday/objects"
        objects.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=objects, prefix=".finlab-intraday-", suffix=".parquet.tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
        try:
            write_parquet(frame, temporary, attrs=attrs)
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


def initialize_intraday_worker(session: dict | None, keepalive: bool = True,
                               download_threads: int = 1, rate_gate=None, authorization_gate=None) -> None:
    """Isolate SDK mutable context and credential refresh in each pool process.

    Copy the owner's already refreshed session through private process memory,
    never argv/logs/files. The SDK's auth persistence adapter is worker-local:
    concurrent refreshes cannot truncate the owner's encrypted credentials.
    Network authentication, account entitlement and quota remain SDK-owned.
    """
    if session:
        from finlab import auth
        local = {**session, "source": "env"}
        auth.get_session = lambda: local

        def save_local(updated):
            snapshot = dict(updated)
            local.clear()
            local.update(snapshot, source="env")

        auth.save_session = save_local
    if keepalive or download_threads > 1 or rate_gate is not None:
        from finlab import utils
        utils.requests = WorkerHTTPTransport(utils.requests, keepalive=keepalive, rate_gate=rate_gate,
                                             authorization_gate=authorization_gate)
    if download_threads > 1:
        from finlab.data import intraday
        validate_prefetch_sdk(intraday)
        SignedMinutePrefetch(intraday, download_threads).install()


def _transport_metrics() -> dict:
    from finlab import utils
    return utils.requests.metrics() if isinstance(utils.requests, WorkerHTTPTransport) else {}


def missing_window_dates(exc: Exception, days: list[date]) -> set[date] | None:
    """Use only an explicit, bounded SDK missing-date list; never guess dates.

    The SDK renders the provider's ``missing_dates`` as a Python list. Parse
    that list locally, without retaining the potentially private error text.
    An unknown/malformed list keeps the conservative single-day fallback.
    """
    message = str(exc)
    match = re.search(r"(\[[^\[\]]*\])\s*$", message) if len(message) <= 8192 else None
    if not match:
        return None
    try:
        values = ast.literal_eval(match.group(1))
        if not isinstance(values, list) or not 1 <= len(values) <= 31:
            return None
        parsed = {date.fromisoformat(value) for value in values
                  if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)}
        return parsed if len(parsed) == len(values) and parsed <= set(days) else None
    except (ValueError, TypeError, SyntaxError):
        return None


def fetch_partitions(root: Path, key: str, days: list[date], *, now: datetime,
                     reference_root: Path | None = None) -> dict:
    """Official <=31-calendar-day query; persist only exact requested stock-days.

    A not-ready window falls back to individual days without inventing supply.
    Other errors do not cause an immediate repeated range download. SDK caches
    remain generation/checksum-bound, preserving partially downloaded objects
    when a later transport fails. SQLite/frontier writes remain with the owner.
    """
    from finlab import data

    if (not days or len(set(days)) != len(days)
            or not 0 <= (max(days) - min(days)).days < 31):
        raise ValueError("invalid bounded FinLab partition window")
    before_http = _transport_metrics()
    sdk_calls = fallback_calls = 0
    source_seconds = persist_seconds = 0.0
    outcomes = []

    def failed(day, exc):
        failure = record_failed_partition(root, key, day, exc)
        return {"trade_date": day.isoformat(), "failure": failure}

    try:
        sdk_calls += 1
        started = time.monotonic()
        try:
            frame = data.get(key, start=min(days).isoformat(), end=max(days).isoformat(),
                             force_download=False, progress="silent")
        finally:
            source_seconds += time.monotonic() - started
        started = time.monotonic()
        try:
            attrs = source_attrs(frame)
            frame = to_polars(frame)
            if "trade_date" not in frame.columns:
                raise ValueError("intraday range lacks trade dates")
            dates = date_column(frame)
            if dates.null_count() or not dates.is_in(days).all():
                raise ValueError("intraday range contains unexpected trade dates")
            partition_key = "__finlab_partition_day"
            while partition_key in frame.columns:
                partition_key += "_"
            grouped = frame.with_columns(dates.alias(partition_key)).partition_by(
                partition_key, as_dict=True, maintain_order=True)
            partitions = []
            for day in days:
                part = grouped.get((day,))
                part = part.drop(partition_key) if part is not None else frame.head(0)
                part_attrs = dict(attrs)
                for field in ("closed_dates", "no_trade_dates"):
                    if field in part_attrs:
                        # Canonical per-day Parquet metadata must not depend
                        # on which neighbouring dates shared the SDK window.
                        part_attrs[field] = [value for value in part_attrs[field] if str(value) == day.isoformat()]
                _validate_frame(part, key, day, attrs=part_attrs)  # Validate the full range before any commit.
                partitions.append((day, part, part_attrs))
            for day, part, part_attrs in partitions:
                receipt = persist_partition_frame(root, key, day, part, reference_root=reference_root, attrs=part_attrs)
                outcomes.append({"trade_date": day.isoformat(), "receipt": receipt})
        finally:
            persist_seconds += time.monotonic() - started
    except Exception as exc:
        reason = intraday_failure_category(exc)
        if reason == "partition_not_ready" and len(days) > 1:
            missing = missing_window_dates(exc, days)
            if missing is not None:
                outcomes.extend(failed(day, exc) for day in days if day in missing)
                windows = []
                current = []
                for day in sorted(days):
                    if day in missing:
                        if current:
                            windows.append(current)
                            current = []
                    else:
                        current.append(day)
                if current:
                    windows.append(current)
                for window in windows:
                    result = fetch_partitions(root, key, window, now=now, reference_root=reference_root)
                    sdk_calls += result["sdk_calls"]
                    fallback_calls += 1 + result["fallback_calls"]
                    source_seconds += result["source_seconds"]
                    persist_seconds += result["persist_seconds"]
                    outcomes.extend(result["outcomes"])
                    if any(item.get("failure") in {"quota_exhausted", "authentication_failed", "source_rate_limited"}
                           for item in result["outcomes"]):
                        break
            else:
                # Unknown missing-date details retain single-day recovery.
                for day in days:
                    sdk_calls += 1
                    fallback_calls += 1
                    started = time.monotonic()
                    try:
                        receipt = fetch_partition(root, key, day, now=now,
                            reference_root=reference_root, force_download=False)
                        outcome = {"trade_date": day.isoformat(), "receipt": receipt}
                    except Exception as single_error:
                        outcome = failed(day, single_error)
                    source_seconds += time.monotonic() - started
                    outcomes.append(outcome)
                    if outcome.get("failure") in {"quota_exhausted", "authentication_failed", "source_rate_limited"}:
                        break
        else:
            # Already committed immutable receipts survive a late persistence
            # error; report only the unresolved suffix, never replace successes.
            committed = {item["trade_date"] for item in outcomes}
            outcomes.extend(failed(day, exc) for day in days if day.isoformat() not in committed)
    outcomes.sort(key=lambda item: item["trade_date"])
    after_http = _transport_metrics()
    http = {key: after_http.get(key, 0) - before_http.get(key, 0) for key in HTTP_COUNTERS}
    http.update({key: after_http.get(key) for key in ("http_peak_inflight", "source_backoff_until_utc")})
    return {"outcomes": outcomes, "sdk_calls": sdk_calls, "fallback_calls": fallback_calls,
            "source_seconds": source_seconds, "persist_seconds": persist_seconds,
            "http": http,
            "timing_basis": "SDK_range_and_validation_persistence; fallback_includes_persistence"}


def reconcile_stored_tick_unit(
    root: Path, key: str, day: date, *, reference_root: Path | None = None,
) -> dict | None:
    """Upgrade an existing stock tick/minute partition without API or data loss."""

    if not key.startswith(("tw_tick:", "tw_minute:")):
        raise ValueError("only stock tick/minute partitions have this unit contract")
    receipt_path, _ = _partition_paths(root, key, day)
    old = _stored_receipt(receipt_path, root, key, day)
    if old is None or old.get("status") != "downloaded_unverified_for_pit":
        return old
    original = root / old["parquet_path"]
    digest = _file_sha256(original)
    if digest != old["sha256"]:
        raise ValueError("stored FinLab intraday object hash mismatch")
    frame = pl.read_parquet(original)
    _validate_frame(frame, key, day)
    from scripts.finlab_volume_units import with_verified_volume_shares

    normalized, evidence = with_verified_volume_shares(
        frame, key.split(":", 1)[1], day, reference_root=reference_root,
    )
    if (all(old.get(name) == value for name, value in evidence.items())
            and list(old.get("fields") or []) == list(normalized.columns)
            and frame.equals(normalized)):
        return old
    objects = root / "intraday/objects"
    with tempfile.NamedTemporaryFile(
        dir=objects, prefix=".finlab-unit-", suffix=".parquet.tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        write_parquet(normalized, temporary)
        new_digest = _file_sha256(temporary)
        destination = objects / f"{safe_stem(key)}-{day.isoformat()}-{new_digest[:24]}.parquet"
        if destination.exists():
            if _file_sha256(destination) != new_digest:
                raise ValueError("content-addressed unit object hash mismatch")
        else:
            os.replace(temporary, destination)
        archive = root / "intraday/versions" / safe_stem(key) / day.isoformat() / f"{digest}.json"
        if not archive.is_file():
            _atomic_json(archive, old)
        updated = {
            **old, "schema_version": max(2, int(old.get("schema_version") or 0)),
            "fields": list(map(str, normalized.columns)),
            "parquet_path": str(destination.relative_to(root)),
            "parquet_size_bytes": destination.stat().st_size,
            "sha256": new_digest, "raw_object_sha256": old.get("raw_object_sha256", digest),
            "unit_reconciled_at_utc": datetime.now(UTC).isoformat(),
            "processing_contract_version": PROCESSING_CONTRACT_VERSION,
            "processing_engine": PROCESSING_ENGINE,
            **evidence,
        }
        _atomic_json(receipt_path, updated)
        return updated
    finally:
        temporary.unlink(missing_ok=True)


def intraday_failure_category(exc: Exception) -> str:
    code = re.match(r"^([A-Za-z][A-Za-z0-9_]{0,64}):", str(exc))
    if code and code.group(1) == "not_ready":
        return "partition_not_ready"
    if code and code.group(1) in {"rate_limited", "rate_limit", "too_many_requests"}:
        return "source_rate_limited"
    if (code and code.group(1) in {"token_expired", "invalid_token", "session_expired", "unauthenticated"}
            or "authentication failed" in str(exc).lower()):
        return "authentication_failed"
    return classify_provider_error(exc)


def record_failed_partition(root: Path, key: str, day: date, exc: Exception) -> str:
    """Keep actionable provider code without persisting private response text."""
    _, attempt_path = _partition_paths(root, key, day)
    provider_code_match = re.match(r"^([A-Za-z][A-Za-z0-9_]{0,64}):", str(exc))
    provider_code = provider_code_match.group(1) if provider_code_match else None
    status = intraday_failure_category(exc)
    observed = datetime.now(UTC)
    retry = {}
    if status == "source_rate_limited":
        retry = {"retry_at_utc": _transport_metrics().get("source_backoff_until_utc")
                 or (observed+timedelta(seconds=60)).isoformat(),
                 "retry_basis": "shared_source_cooldown; not_until_next_daily_reset"}
    elif status == "provider_error":
        try:
            previous = json.loads(attempt_path.read_text())
            previous_at = datetime.fromisoformat(previous["attempted_at_utc"])
        except (OSError, ValueError, KeyError, TypeError):
            previous, previous_at = {}, datetime.min.replace(tzinfo=UTC)
        count = previous.get("cycle_attempts", 1)
        count = count if type(count) is int and 0 <= count < 100 else 1
        count = count + 1 if (previous.get("status") == status and previous_at.tzinfo is not None
                             and previous_at >= quota_cycle_start(observed)) else 1
        base = 30 if provider_code == "request_conflict" else 60
        retry = {"cycle_attempts": count, "retry_exhausted": count >= 3,
                 "retry_at_utc": (observed + timedelta(seconds=base * 2**min(count-1, 4))).isoformat(),
                 "retry_basis": "bounded_unresolved_source_recheck; not_source_empty_or_corruption"}
    _atomic_json(attempt_path, {
        "dataset": key, "trade_date": day.isoformat(),
        "status": status, "provider_code": provider_code,
        "exception_class": type(exc).__name__,
        "attempted_at_utc": observed.isoformat(),
        "message_retained": False,
        **retry,
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
