#!/usr/bin/env python3
"""Inspect FinLab's catalog and acquire resumable, local-only history.

Provider-advertised dates and account entitlement are *not* interchangeable.
Outputs preserve the provider's index labels and remain unapproved for PIT training.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG = ROOT / "configs/finlab_history_candidates.json"
DEFAULT_OUTPUT = ROOT / "data_finlab"
TAIPEI = ZoneInfo("Asia/Taipei")
PERIOD_LABEL = re.compile(r"^\d{4}-(?:M\d{2}|Q[1-4])$")
# This is a provider-origin label, not a numerical market feature. Its wide
# string frame exceeded the bounded research worker's memory during a live
# account sweep; leave it discoverable and manually fetchable, but do not let
# it stall every subsequent daily incremental run.
AUTOMATICALLY_DEFERRED_REASONS = {
    "after_market_fixed_price:資料來源": "oversized_metadata",
    # 2.1.1 forced refresh on 2026-09-25 reached ~9.9 GiB RSS without
    # finishing. Preserve its existing receipt but never let a routine
    # full-table refresh compete with the user's live WSL workload.
    "after_market_fixed_price:市場別": "oversized_wide_refresh",
    # 2026-09-23: one whole-table SDK fetch used ~726 MB of daily quota,
    # stayed above the worker's 6 GiB high watermark for >50 minutes, and
    # produced no durable receipt. Retry only after a bounded partition plan.
    "broker_transactions": "oversized_table",
    # These are dated partitions, not whole-table matrix keys. Calling
    # data.get(key) without both dates always fails, regardless of entitlement.
    "tw_minute:2330": "requires_date_window",
    "tw_tick:2330": "requires_date_window",
}
AUTOMATICALLY_DEFERRED_KEYS = frozenset(AUTOMATICALLY_DEFERRED_REASONS)
# Short, bounded backoff avoids both a multi-day blind spot and a tight loop
# against a deterministic provider failure. The timer supplies the retry clock.
ATTEMPT_RETRY_SECONDS = {
    "vip_only": (30 * 60, 4 * 60 * 60),
    "provider_error": (5 * 60, 60 * 60),
    "provider_empty": (30 * 60, 2 * 60 * 60),
    "timed_out": (30 * 60, 2 * 60 * 60),
}


class EmptyProviderFrame(ValueError):
    """The SDK returned a frame, but no non-null market observation exists."""

    def __init__(self, *, rows: int, fields: int):
        super().__init__("provider returned no non-null rows")
        self.rows = rows
        self.fields = fields


def load_catalog(path: Path = DEFAULT_CATALOG) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    keys = [item["key"] for item in payload["datasets"]]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate FinLab candidate key")
    return payload


def safe_stem(key: str) -> str:
    """Stable filenames, without allowing provider names to become paths."""
    readable = re.sub(r"[^A-Za-z0-9_-]+", "_", key.split(":", 1)[0])[:56]
    return f"{readable}-{hashlib.sha256(key.encode()).hexdigest()[:12]}"


def has_local_download(key: str, output_root: Path) -> bool:
    stem = safe_stem(key)
    receipt_path = output_root / "receipts" / f"{stem}.json"
    if not receipt_path.is_file():
        return False
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    relative = Path(str(receipt.get("parquet_path") or ""))
    if relative.is_absolute() or ".." in relative.parts or relative.parts[:1] != ("datasets",):
        return False
    return (
        receipt.get("dataset") == key
        and (output_root / relative).is_file()
        and receipt.get("status") == "downloaded_unverified_for_pit"
    )


def unavailable_attempt(key: str, output_root: Path) -> bool:
    path = output_root / "attempts" / f"{safe_stem(key)}.json"
    if not path.is_file():
        return False
    try:
        attempt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return attempt.get("dataset") == key and attempt.get("status") == "vip_only"


def vip_retry_due(key: str, output_root: Path, *, now: datetime) -> bool:
    path = output_root / "attempts" / f"{safe_stem(key)}.json"
    try:
        attempt = json.loads(path.read_text(encoding="utf-8"))
        attempted = datetime.fromisoformat(attempt["attempted_at_utc"])
        if attempted.tzinfo and attempted.astimezone(UTC) < quota_cycle_start(now):
            return True
        due = attempt_retry_at(attempt)
        return due is None or now >= due
    except (OSError, ValueError, KeyError, TypeError):
        return True


def attempt_retry_at(attempt: dict, *, downloaded: bool = False) -> datetime | None:
    """Only per-key failures get a cooldown; account-wide failures do not."""
    policy = ATTEMPT_RETRY_SECONDS.get(attempt.get("status"))
    if policy is None:
        return None
    base, maximum = policy
    if downloaded and attempt.get("status") == "timed_out":
        base, maximum = 15 * 60, 60 * 60
    streak = attempt.get("failure_streak", 1)
    if not isinstance(streak, int) or isinstance(streak, bool) or streak < 1:
        streak = 1
    delay = timedelta(seconds=min(maximum, base * (2 ** min(streak - 1, 10))))
    try:
        attempted = datetime.fromisoformat(attempt["attempted_at_utc"])
        return attempted.astimezone(UTC) + delay if attempted.tzinfo else None
    except (ValueError, KeyError, TypeError):
        return None


def quota_cycle_start(now: datetime) -> datetime:
    """FinLab daily quota day begins at 08:00 Asia/Taipei, including holidays."""
    local = now.astimezone(TAIPEI)
    reset = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local < reset:
        reset -= timedelta(days=1)
    return reset.astimezone(UTC)


def recent_attempt(key: str, output_root: Path, *, now: datetime,
                   downloaded: bool = False) -> bool:
    """Skip only the key's measured failure window, not every error for 7 days."""
    path = output_root / "attempts" / f"{safe_stem(key)}.json"
    if not path.is_file():
        return False
    try:
        attempt = json.loads(path.read_text(encoding="utf-8"))
        if attempt.get("dataset") != key:
            return False
        attempted = datetime.fromisoformat(attempt["attempted_at_utc"])
        if attempted.tzinfo and attempted.astimezone(UTC) < quota_cycle_start(now):
            return False
        if downloaded:
            receipt = json.loads((output_root / "receipts" / f"{safe_stem(key)}.json").read_text())
            checked = datetime.fromisoformat(receipt.get("source_checked_at_utc") or receipt["fetched_at_utc"])
            if checked.tzinfo and attempted.tzinfo and checked >= attempted:
                return False
        due = attempt_retry_at(attempt, downloaded=downloaded)
        return due is not None and now < due
    except (OSError, ValueError, KeyError, TypeError):
        return False


def refresh_due(key: str, output_root: Path, *, now: datetime, days: int) -> bool:
    if not has_local_download(key, output_root):
        return False
    try:
        receipt = json.loads((output_root / "receipts" / f"{safe_stem(key)}.json").read_text())
        checked = datetime.fromisoformat(receipt.get("source_checked_at_utc") or receipt["fetched_at_utc"])
        if days == 1:
            return checked.astimezone(UTC) < quota_cycle_start(now)
        return now - checked.astimezone(UTC) >= timedelta(days=days)
    except (OSError, ValueError, KeyError, TypeError):
        return True


def last_source_check(key: str, output_root: Path) -> datetime:
    """Order due catalog refreshes by evidence age, not catalog spelling.

    A malformed or absent check timestamp must be retried first.  The value is
    only a scheduling key; ``refresh_due`` still owns the freshness decision.
    """
    try:
        receipt = json.loads(
            (output_root / "receipts" / f"{safe_stem(key)}.json").read_text()
        )
        checked = datetime.fromisoformat(
            receipt.get("source_checked_at_utc") or receipt["fetched_at_utc"]
        )
        return checked.astimezone(UTC) if checked.tzinfo else datetime.min.replace(tzinfo=UTC)
    except (OSError, ValueError, KeyError, TypeError):
        return datetime.min.replace(tzinfo=UTC)


def quota_room_mb() -> tuple[float, float] | None:
    """FinLab's own daily quota, not a guessed request-per-second limit."""
    from finlab.auth import get_data_status

    try:
        status = get_data_status() or {}
    except Exception:
        return None
    try:
        return float(status["limit_size"]) - float(status["quota"]), float(status["limit_size"])
    except (KeyError, TypeError, ValueError):
        return None


def classify_provider_error(exc: Exception) -> str:
    """Persist only a bounded category; exception text may contain secrets."""
    if isinstance(exc, EmptyProviderFrame):
        return "provider_empty"
    message = str(exc).lower()
    if "only for vip" in message or (
        "vip" in message and "please" in message and "to vip" in message
    ):
        return "vip_only"
    if "quota" in message or "limit exceeded" in message:
        return "quota_exhausted"
    if "login" in message or "unauthorized" in message or "session expired" in message:
        return "authentication_failed"
    return "provider_error"


def credential_available() -> bool:
    from dotenv import load_dotenv
    from finlab.auth import get_session

    load_dotenv(ROOT / ".env", override=False)
    legacy = bool(os.environ.get("FINLAB_API_TOKEN"))
    # The SDK validates/decrypts a machine-bound browser session or reads all
    # three headless env vars. File existence alone is not proof of login.
    session = get_session()
    return legacy or bool(
        session
        and session.get("refresh_token")
        and session.get("session_id")
        and session.get("api_key")
    )


def provider_catalog() -> list[str]:
    from finlab import data

    return sorted(set(str(value) for value in data.search().tolist()))


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=".finlab-",
        suffix=".json.tmp", delete=False,
    ) as handle:
        temp = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def serialize_provider_frame(frame):
    """Keep exact source labels; do not convert monthly/quarterly periods to dates."""
    import pandas as pd

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("FinLab response was not a DataFrame")
    source = pd.DataFrame(frame)
    valid = source.notna().any(axis=1)
    index_frame = source.index.to_frame(index=False)
    index_columns = [
        "source_index" if len(index_frame.columns) == 1 else f"source_index_{n}"
        for n in range(len(index_frame.columns))
    ]
    index_frame.columns = index_columns
    for name in index_columns:
        index_frame[name] = index_frame[name].map(str)
    # Wide storage preserves every provider field and symbol without melting.
    values = source.reset_index(drop=True)
    values.columns = [str(column) for column in values.columns]
    if set(index_columns) & set(values.columns):
        raise ValueError("source field conflicts with reserved source_index column")
    output = pd.concat([index_frame, values], axis=1)
    observed_index = source.index[valid.to_numpy()]
    first = str(observed_index.min()) if len(observed_index) else None
    last = str(observed_index.max()) if len(observed_index) else None
    sample = str(index_frame.iloc[0, 0]) if len(index_frame) else ""
    semantics = (
        "period_label_not_publication_date" if PERIOD_LABEL.fullmatch(sample)
        else "source_timestamp_not_verified_publication_date"
        if isinstance(source.index, pd.DatetimeIndex)
        else "provider_index_unverified"
    )
    event_bounds = _event_bounds(source["date"]) if "date" in source.columns else {}
    return output, {
        "rows": len(source),
        "rows_with_values": int(valid.sum()),
        "field_columns": len(values.columns),
        "source_index_columns": index_columns,
        "source_index_dtype": str(source.index.dtype),
        "first_non_null_source_index": first,
        "last_non_null_source_index": last,
        "index_semantics": semantics,
        **event_bounds,
    }


def _event_bounds(values) -> dict:
    """A long-table event date is distinct from its row index and key_date."""
    import pandas as pd

    parsed = pd.to_datetime(values, errors="coerce")
    observed = parsed.dropna()
    if not len(observed):
        return {
            "event_time_column": "date", "event_date_rows": 0,
            "first_event_at": None, "last_event_at": None,
        }
    return {
        "event_time_column": "date",
        "event_date_rows": len(observed),
        "first_event_at": observed.min().isoformat(),
        "last_event_at": observed.max().isoformat(),
    }


def audit_local(output_root: Path) -> tuple[int, int]:
    """Recheck stored bytes and repair date bounds without another API request."""
    import pandas as pd
    import pyarrow.parquet as pq

    passed = failed = 0
    root = output_root.resolve()
    for receipt_path in sorted((root / "receipts").glob("*.json")):
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            relative = Path(receipt["parquet_path"])
            data_path = (root / relative).resolve()
            if relative.is_absolute() or not data_path.is_relative_to(root):
                raise ValueError("receipt path escapes FinLab root")
            digest = hashlib.sha256()
            with data_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != receipt.get("sha256"):
                raise ValueError("stored Parquet SHA-256 mismatch")
            parquet = pq.ParquetFile(data_path)
            if parquet.metadata.num_rows != receipt.get("rows"):
                raise ValueError("stored Parquet row count mismatch")
            fields = parquet.schema_arrow.names
            if "date" in fields:
                event_dates = parquet.read(columns=["date"]).column("date").to_pandas()
                receipt.update(_event_bounds(event_dates))
            if "source_index" in fields and receipt.get("rows_with_values") == receipt.get("rows"):
                indexes = parquet.read(columns=["source_index"]).column("source_index").to_pandas()
                if str(receipt.get("source_index_dtype")) in {"int32", "int64", "uint32", "uint64"}:
                    indexes = pd.to_numeric(indexes, errors="raise")
                receipt["first_non_null_source_index"] = str(indexes.min()) if len(indexes) else None
                receipt["last_non_null_source_index"] = str(indexes.max()) if len(indexes) else None
            receipt["schema_version"] = max(2, int(receipt.get("schema_version") or 0))
            receipt["storage_audited_at_utc"] = datetime.now(UTC).isoformat()
            _atomic_json(receipt_path, receipt)
            passed += 1
            print(f"{receipt.get('dataset')}: verified {receipt.get('rows')} rows; event {receipt.get('first_event_at')}..{receipt.get('last_event_at')}")
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            failed += 1
            print(f"{receipt_path.name}: local audit failed ({type(exc).__name__})", file=sys.stderr)
    return passed, failed


def fetch_one(key: str, output_root: Path, *, refresh: bool = False) -> dict:
    from finlab import data

    started = time.monotonic()
    frame = data.get(key, force_download=refresh, progress="silent")
    table, stats = serialize_provider_frame(frame)
    if not stats["rows_with_values"]:
        raise EmptyProviderFrame(rows=stats["rows"], fields=stats["field_columns"])
    stem = safe_stem(key)
    receipt_path = output_root / "receipts" / f"{stem}.json"
    datasets_root = output_root / "datasets"
    datasets_root.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=datasets_root, prefix=".finlab-", suffix=".parquet.tmp", delete=False,
    ) as handle:
        temp = Path(handle.name)
    try:
        table.to_parquet(temp, index=False, compression="zstd")
        digest = hashlib.sha256()
        with temp.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
        content_hash = digest.hexdigest()
        checked_at = datetime.now(UTC).isoformat()
        previous: dict = {}
        if receipt_path.is_file():
            try:
                previous = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                previous = {}
            if previous.get("dataset") == key and previous.get("sha256") == content_hash:
                old_relative = Path(str(previous.get("parquet_path") or ""))
                if (not old_relative.is_absolute() and ".." not in old_relative.parts
                        and old_relative.parts[:1] == ("datasets",)
                        and (output_root / old_relative).is_file()):
                    existing = hashlib.sha256()
                    with (output_root / old_relative).open("rb") as handle:
                        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                            existing.update(chunk)
                    if existing.hexdigest() == content_hash:
                        previous["source_checked_at_utc"] = checked_at
                        previous["source_check_mode"] = "upstream_forced" if refresh else "sdk_cache_allowed"
                        previous["last_check_result"] = "unchanged"
                        previous["last_fetch_elapsed_seconds"] = round(time.monotonic() - started, 3)
                        previous["parquet_size_bytes"] = (output_root / old_relative).stat().st_size
                        _atomic_json(receipt_path, previous)
                        return previous
        # Content-addressed versions make a changed provider response additive.
        # The receipt pointer switches only after the new file is complete.
        data_path = datasets_root / f"{stem}-{content_hash[:24]}.parquet"
        if data_path.exists():
            existing = hashlib.sha256()
            with data_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                    existing.update(chunk)
            if existing.hexdigest() != content_hash:
                raise ValueError("content-addressed FinLab file has a hash mismatch")
        else:
            os.replace(temp, data_path)
        if previous.get("dataset") == key and previous.get("sha256") != content_hash:
            old_relative = Path(str(previous.get("parquet_path") or ""))
            if (not old_relative.is_absolute() and ".." not in old_relative.parts
                    and old_relative.parts[:1] == ("datasets",)
                    and (output_root / old_relative).is_file()):
                old_hash = hashlib.sha256()
                with (output_root / old_relative).open("rb") as handle:
                    for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                        old_hash.update(chunk)
                if old_hash.hexdigest() == previous.get("sha256"):
                    archive = output_root / "versions" / stem / f"{old_hash.hexdigest()}.json"
                    if not archive.is_file():
                        _atomic_json(archive, previous)
        receipt = {
            "schema_version": 3,
            "dataset": key,
            "status": "downloaded_unverified_for_pit",
            "fetched_at_utc": checked_at,
            "source_checked_at_utc": checked_at,
            "source_check_mode": "upstream_forced" if refresh else "sdk_cache_allowed",
            "last_check_result": "downloaded",
            "last_fetch_elapsed_seconds": round(time.monotonic() - started, 3),
            "parquet_size_bytes": data_path.stat().st_size,
            "parquet_path": str(data_path.relative_to(output_root)),
            "sha256": content_hash,
            "account_entitlement": "response_observed_only_not_inferred_from_catalog",
            "publication_time_status": "not_verified",
            "redistribution": "local_only_not_published",
            **stats,
        }
        _atomic_json(receipt_path, receipt)
        return receipt
    finally:
        temp.unlink(missing_ok=True)


def sync_selection(
    available: list[str], curated: dict[str, dict], output_root: Path,
    *, now: datetime, refresh_days: int, retry_unavailable: bool,
) -> list[str]:
    """Prioritize known gaps, then a bounded pass over the TW catalog."""
    available_set = set(available)
    curated_keys = [key for key in curated if key in available_set]
    extra_keys = [key for key in available if key not in curated]
    eligible_curated: list[str] = []
    missing_extra: list[str] = []
    refresh_extra: list[str] = []
    for key in [*curated_keys, *extra_keys]:
        if key in AUTOMATICALLY_DEFERRED_KEYS:
            continue
        if key not in available_set:
            continue
        downloaded = has_local_download(key, output_root)
        if (not downloaded and unavailable_attempt(key, output_root)
                and not retry_unavailable and not vip_retry_due(key, output_root, now=now)):
            continue
        if recent_attempt(key, output_root, now=now, downloaded=downloaded) and not (
            retry_unavailable and unavailable_attempt(key, output_root)
        ):
            continue
        if key in curated and (not downloaded or refresh_due(
                key, output_root, now=now, days=refresh_days)):
            eligible_curated.append(key)
        elif key not in curated and not downloaded:
            missing_extra.append(key)
        elif key not in curated and refresh_due(
                key, output_root, now=now, days=refresh_days):
            refresh_extra.append(key)
    # A quota-limited daily run may check only a fraction of the catalog.  If
    # extras stay in alphabetical order, the same prefix is checked after each
    # 08:00 reset while later keys can starve indefinitely.  Preserve curated
    # and missing-key priority, then visit the stalest downloaded extras first.
    refresh_extra.sort(key=lambda key: (last_source_check(key, output_root), key))
    return [*eligible_curated, *missing_extra, *refresh_extra]


def general_work_status(
    discovery: dict, curated: dict[str, dict], output_root: Path,
    *, now: datetime, refresh_days: int,
) -> dict:
    """Read-only gate for lower-priority Tick work; no provider API calls.

    ``idle`` means no *currently actionable* whole-table work, not that every
    historical field exists or has passed a point-in-time audit.
    """
    if not isinstance(discovery, dict) or refresh_days < 1:
        return {"state": "discovery_unverified", "actionable_pending": None,
                "basis": "Validated catalog and refresh policy are required before Tick work."}
    observed = discovery.get("observed_at_utc")
    keys = discovery.get("keys")
    try:
        checked = datetime.fromisoformat(str(observed).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        checked = None
    if (checked is None or checked.tzinfo is None
            or not timedelta(0) <= now - checked.astimezone(UTC) <= timedelta(hours=4)
            or not isinstance(keys, list) or not keys
            or any(not isinstance(key, str) or not key for key in keys)):
        return {"state": "discovery_unverified", "actionable_pending": None,
                "basis": "Recent validated local FinLab discovery is required before Tick work."}
    available = list(dict.fromkeys(keys))
    pending = sync_selection(available, curated, output_root, now=now,
                             refresh_days=refresh_days, retry_unavailable=False)
    general_keys = [key for key in available if key not in {
        "tw_minute:2330", "tw_tick:2330",
    }]
    missing_general = {key for key in general_keys
                       if not has_local_download(key, output_root)}
    return {
        "state": "general_work_pending" if pending else "general_work_idle",
        "actionable_pending": len(pending),
        "catalog_keys": len(available),
        "deferred_keys": sum(key in AUTOMATICALLY_DEFERRED_KEYS for key in available),
        "missing_receipts": sum(not has_local_download(key, output_root) for key in available),
        "general_catalog_keys": len(general_keys),
        "general_missing_receipts": len(missing_general),
        "general_missing_not_actionable_now": len(missing_general - set(pending)),
        "discovery_at_utc": checked.astimezone(UTC).isoformat(),
        "basis": "Current catalog selection after per-key retry/defer rules; idle is not historical completeness or PIT readiness.",
    }


def record_attempt(key: str, output_root: Path, exc: Exception,
                   *, elapsed_seconds: float | None = None) -> str:
    # FinLab errors may include account-scoped URLs or credentials.
    reason = classify_provider_error(exc)
    streak = _next_failure_streak(key, output_root, reason)
    _atomic_json(output_root / "attempts" / f"{safe_stem(key)}.json", {
        "dataset": key,
        "attempted_at_utc": datetime.now(UTC).isoformat(),
        "status": reason,
        "failure_streak": streak,
        "provider_rows": exc.rows if isinstance(exc, EmptyProviderFrame) else None,
        "provider_fields": exc.fields if isinstance(exc, EmptyProviderFrame) else None,
        "exception_class": type(exc).__name__,
        "message_retained": False,
        "last_attempt_elapsed_seconds": round(elapsed_seconds, 3) if elapsed_seconds is not None else None,
    })
    return reason


def _next_failure_streak(key: str, output_root: Path, status: str) -> int:
    """Count consecutive same-key failures only since the last success."""
    try:
        previous = json.loads((output_root / "attempts" / f"{safe_stem(key)}.json").read_text())
        if previous.get("dataset") != key or previous.get("status") != status:
            return 1
        attempted = datetime.fromisoformat(previous["attempted_at_utc"])
        if attempted.tzinfo is None:
            return 1
        receipt_path = output_root / "receipts" / f"{safe_stem(key)}.json"
        if receipt_path.is_file():
            receipt = json.loads(receipt_path.read_text())
            checked = datetime.fromisoformat(receipt.get("source_checked_at_utc") or receipt["fetched_at_utc"])
            if checked.tzinfo and checked >= attempted:
                return 1
        return min(11, max(1, int(previous.get("failure_streak") or 1)) + 1)
    except (OSError, ValueError, KeyError, TypeError):
        return 1


def record_timed_out_sync(
    output_root: Path, *, attempt_id: str, timeout_seconds: int,
) -> dict:
    """Close only the exact in-flight key killed by the bounded SDK wrapper."""
    if not attempt_id or timeout_seconds <= 0:
        raise ValueError("a positive timeout and exact attempt id are required")
    summary_path = output_root / "runs" / "latest.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if (
        isinstance(summary, dict)
        and summary.get("attempt_id") == attempt_id
        and summary.get("state") != "running"
        and not summary.get("active_key")
    ):
        # A SIGTERM-aware SDK may have completed its own terminal receipt
        # before GNU timeout returned 124. Do not overwrite that evidence.
        return summary
    if (
        not isinstance(summary, dict)
        or summary.get("state") != "running"
        or summary.get("attempt_id") != attempt_id
        or not isinstance(summary.get("active_key"), str)
        or not summary["active_key"]
    ):
        raise ValueError("no matching in-flight FinLab dataset to mark timed out")
    key = summary["active_key"]
    started_at = summary.get("active_started_at_utc")
    if not isinstance(started_at, str):
        raise ValueError("in-flight FinLab start time is missing")
    started = datetime.fromisoformat(started_at)
    if started.tzinfo is None:
        raise ValueError("in-flight FinLab start time lacks timezone")
    now = datetime.now(UTC)
    elapsed = max(0.0, (now - started.astimezone(UTC)).total_seconds())
    _atomic_json(output_root / "attempts" / f"{safe_stem(key)}.json", {
        "dataset": key,
        "attempted_at_utc": now.isoformat(),
        "status": "timed_out",
        "failure_streak": _next_failure_streak(key, output_root, "timed_out"),
        "exception_class": "ExternalSdkTimeout",
        "message_retained": False,
        "last_attempt_elapsed_seconds": round(elapsed, 3),
        "timeout_seconds": timeout_seconds,
        "attempt_id": attempt_id,
    })
    summary["state"] = "partial"
    summary["provider_errors"] = int(summary.get("provider_errors") or 0) + 1
    summary["timed_out"] = int(summary.get("timed_out") or 0) + 1
    summary["last_timeout_key"] = key
    summary["last_timeout_seconds"] = timeout_seconds
    summary["finished_at_utc"] = now.isoformat()
    summary["pending_after_estimate"] = max(
        0, int(summary.get("pending_before") or 0) - int(summary.get("attempted") or 0)
    )
    summary.pop("active_key", None)
    summary.pop("active_started_at_utc", None)
    _atomic_json(summary_path, summary)
    return summary


def sync_catalog(
    available: list[str], curated: dict[str, dict], output_root: Path,
    *, limit: int, refresh_days: int, min_quota_remaining_mb: float,
    retry_unavailable: bool, attempt_id: str | None = None,
) -> dict:
    if not credential_available():
        raise RuntimeError("no usable FinLab session")
    now = datetime.now(UTC)
    pending = sync_selection(
        available, curated, output_root, now=now,
        refresh_days=refresh_days, retry_unavailable=retry_unavailable,
    )
    summary = {
        "started_at_utc": now.isoformat(), "catalog_keys": len(available),
        "pending_before": len(pending), "attempted": 0, "downloaded": 0,
        "unchanged": 0, "vip_only": 0, "provider_errors": 0,
        "state": "running", "quota_remaining_mb": None,
        "quota_limit_mb": None, "quota_reserve_mb": min_quota_remaining_mb,
        "attempt_id": attempt_id,
    }
    try:
        for key in pending[:limit]:
            room = quota_room_mb()
            if room is None:
                summary["state"] = "quota_unknown"
                break
            remaining, total = room
            summary["quota_remaining_mb"] = round(remaining, 2)
            summary["quota_limit_mb"] = round(total, 2)
            # The caller owns the account reserve.  An implicit 10% floor
            # stranded 450 MB/day on a 5 GB account despite the CLI's 50 MB
            # default; FinLab enforces the authoritative limit on requests.
            if remaining <= min_quota_remaining_mb:
                summary["state"] = "quota_margin_reached"
                break
            summary["attempted"] += 1
            summary["active_key"] = key
            summary["active_started_at_utc"] = datetime.now(UTC).isoformat()
            _atomic_json(output_root / "runs" / "latest.json", summary)
            attempt_started = time.monotonic()
            try:
                # A successful SDK cache read is not evidence that the provider
                # was checked for current revisions. Sync must query upstream.
                receipt = fetch_one(key, output_root, refresh=True)
                result = receipt.get("last_check_result", "downloaded")
                summary[result] += 1
                print(f"[finlab] {key}: {result}; rows={receipt['rows_with_values']}", flush=True)
            except Exception as exc:
                reason = record_attempt(
                    key, output_root, exc,
                    elapsed_seconds=time.monotonic() - attempt_started,
                )
                if reason == "vip_only":
                    summary["vip_only"] += 1
                else:
                    summary["provider_errors"] += 1
                print(f"[finlab] {key}: {reason}; provider message withheld", file=sys.stderr, flush=True)
                if reason in {"quota_exhausted", "authentication_failed"}:
                    summary["state"] = reason
                    break
        if summary["state"] == "running":
            summary["state"] = "partial" if len(pending) > summary["attempted"] else "pass_complete"
    finally:
        summary["finished_at_utc"] = datetime.now(UTC).isoformat()
        summary.pop("active_key", None)
        summary.pop("active_started_at_utc", None)
        summary["pending_after_estimate"] = max(0, len(pending) - summary["attempted"])
        downloaded = sum(has_local_download(key, output_root) for key in available)
        vip_only = sum(
            unavailable_attempt(key, output_root) and not has_local_download(key, output_root)
            for key in available
        )
        summary["downloaded_total"] = downloaded
        summary["known_vip_only_total"] = vip_only
        summary["not_downloaded_total"] = len(available) - downloaded
        room = quota_room_mb()
        if room is not None:
            summary["quota_remaining_mb"] = round(room[0], 2)
            summary["quota_limit_mb"] = round(room[1], 2)
        _atomic_json(output_root / "runs" / "latest.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("catalog", "discover", "fetch", "audit-local", "sync", "record-timeout", "pending"))
    parser.add_argument("--dataset", action="append", default=[], help="Exact FinLab key; repeatable")
    parser.add_argument("--all-candidates", action="store_true", help="Fetch all curated candidates")
    parser.add_argument("--missing-candidates", action="store_true", help="Fetch curated candidates without a local data/receipt pair, excluding known VIP-only keys")
    parser.add_argument("--retry-unavailable", action="store_true", help="Retry known VIP-only keys after an entitlement change")
    parser.add_argument("--limit", type=int, default=0, help="Limit selected fetches for a trial")
    parser.add_argument("--refresh", action="store_true", help="Bypass FinLab's SDK cache")
    parser.add_argument("--refresh-days", type=int, default=30, help="Minimum days between source checks for downloaded datasets in sync mode")
    parser.add_argument("--min-quota-remaining-mb", type=float, default=50.0, help="Stop sync before this much of the provider's daily quota remains")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--attempt-id", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    catalog = load_catalog()
    candidates = {item["key"]: item for item in catalog["datasets"]}
    if args.action == "catalog":
        for item in catalog["datasets"]:
            print(f"{item['key']}\t{item['advertised_start'] or '?'}\t{item['group']}")
        print(
            f"curated={len(candidates)}; "
            f"free_plan_faq_claimed_end={catalog['free_plan_faq_claimed_end']}; "
            f"free_plan_observed_common_end={catalog['free_plan_observed_common_end']}"
        )
        return 0
    if args.action == "audit-local":
        passed, failed = audit_local(args.output_root)
        print(f"local_verified={passed}; local_failed={failed}")
        return 1 if failed else 0
    if args.action == "record-timeout":
        try:
            summary = record_timed_out_sync(
                args.output_root,
                attempt_id=args.attempt_id or "",
                timeout_seconds=args.timeout_seconds,
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            print(f"FinLab timeout recovery failed: {type(exc).__name__}", file=sys.stderr)
            return 2
        print("finlab_sync_timeout=" + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        return 0
    if args.action == "pending":
        try:
            discovery = json.loads((args.output_root / "catalog/discovery.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            discovery = {}
        status = general_work_status(
            discovery, candidates, args.output_root,
            now=datetime.now(UTC), refresh_days=args.refresh_days,
        )
        print(json.dumps(status, ensure_ascii=False, sort_keys=True), flush=True)
        return 0 if status["state"] != "discovery_unverified" else 2
    available = provider_catalog()
    if args.action == "discover":
        missing = sorted(set(candidates) - set(available))
        payload = {
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "provider_catalog_count": len(available),
            "keys": available,
            "curated_missing_from_sdk_catalog": missing,
            "entitlement": "not_checked",
            "history_coverage": "not_checked",
        }
        _atomic_json(args.output_root / "catalog" / "discovery.json", payload)
        print(f"provider_catalog_count={len(available)}; curated_missing={len(missing)}")
        return 0 if not missing else 2
    if args.action == "sync":
        if args.limit < 0 or args.refresh_days < 1 or args.min_quota_remaining_mb < 0:
            parser.error("sync limit/quota must be nonnegative and refresh-days must be positive")
        try:
            summary = sync_catalog(
                available, candidates, args.output_root, limit=args.limit or 8,
                refresh_days=args.refresh_days,
                min_quota_remaining_mb=args.min_quota_remaining_mb,
                retry_unavailable=args.retry_unavailable,
                attempt_id=args.attempt_id,
            )
        except RuntimeError as exc:
            print(f"FinLab sync unavailable: {type(exc).__name__}", file=sys.stderr)
            return 2
        print("finlab_sync=" + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        # An individual dataset error is durable in attempts/ and should not
        # starve later keys. Authentication/quota uncertainty still stops the
        # account-wide sweep; a provider-specific failure does not.
        return 1 if summary["state"] in {"authentication_failed", "quota_unknown"} else 0
    keys = list(args.dataset)
    if args.all_candidates:
        keys.extend(candidates)
    if args.missing_candidates:
        keys.extend(
            key for key in candidates
            if not has_local_download(key, args.output_root)
            and (args.retry_unavailable or not unavailable_attempt(key, args.output_root))
        )
    keys = list(dict.fromkeys(keys))
    if not keys:
        if args.missing_candidates:
            downloaded = sum(has_local_download(key, args.output_root) for key in candidates)
            vip_only = sum(unavailable_attempt(key, args.output_root) for key in candidates)
            print(f"no eligible missing candidates; downloaded={downloaded}; vip_only={vip_only}")
            return 0
        parser.error("fetch requires --dataset KEY, --all-candidates or --missing-candidates")
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    unknown = sorted(set(keys) - set(available))
    if unknown:
        print(f"Unknown SDK dataset keys: {unknown}", file=sys.stderr)
        return 2
    if not credential_available():
        print("No usable FinLab session found. In this WSL environment run: run_fintech_python -m finlab login; then complete the one-time browser URL. Headless env credentials are optional.", file=sys.stderr)
        return 2
    if args.limit:
        keys = keys[:args.limit]
    failed = 0
    for offset, key in enumerate(keys, start=1):
        attempt_started = time.monotonic()
        try:
            receipt = fetch_one(key, args.output_root, refresh=args.refresh)
            print(f"[{offset}/{len(keys)}] {key}: {receipt['rows_with_values']} non-null rows, {receipt['first_non_null_source_index']}..{receipt['last_non_null_source_index']}", flush=True)
        except Exception as exc:
            # Authentication and API exceptions can embed private URLs/tokens.
            failed += 1
            reason = record_attempt(
                key, args.output_root, exc,
                elapsed_seconds=time.monotonic() - attempt_started,
            )
            print(f"[{offset}/{len(keys)}] {key}: {reason}; error text withheld to avoid credential leakage", file=sys.stderr, flush=True)
            if reason in {"quota_exhausted", "authentication_failed"} or type(exc).__name__.lower() in {"loginerror", "permissionerror", "ratelimiterror"}:
                break
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
