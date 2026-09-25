#!/usr/bin/env python3
"""Persist credential-free, one-minute observations of FinLab account data quota.

This is a separate account-status worker. Public dashboard requests never import
the FinLab SDK, read credentials, or contact the provider.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, time, timedelta
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time as clock
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
TAIPEI = ZoneInfo("Asia/Taipei")
DEFAULT_OUTPUT = ROOT / "artifacts/live/finlab"
HISTORY_DAYS = 30
HISTORY_DB_NAME = "quota_history.sqlite3"


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent,
        prefix=".finlab-quota-", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _next_reset(now: datetime) -> datetime:
    """Official CLI documents the daily reset at 08:00 Asia/Taipei."""

    local = now.astimezone(TAIPEI)
    reset = datetime.combine(local.date(), time(8), tzinfo=TAIPEI)
    if local >= reset:
        reset += timedelta(days=1)
    return reset.astimezone(UTC)


def quota_observation(status: dict, *, now: datetime) -> dict:
    used = float(status["quota"])
    limit = float(status["limit_size"])
    if not math.isfinite(used) or not math.isfinite(limit) or used < 0 or limit <= 0:
        raise ValueError("FinLab quota response is not a finite positive limit")
    return {
        "schema_version": 1,
        "observed_at_utc": now.astimezone(UTC).isoformat(),
        "used_mb": round(used, 3),
        "limit_mb": round(limit, 3),
        "remaining_mb": round(max(0.0, limit - used), 3),
        "used_ratio": used / limit,
        "reset_at_utc": _next_reset(now).isoformat(),
        "reset_basis": "official_published_08:00_asia_taipei_not_account_response",
        "source": "finlab.auth.get_data_status",
    }


def _read_history(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return []
    return payload if isinstance(payload, list) else []


def _history_row(row: dict) -> tuple[str, float, float] | None:
    try:
        observed = datetime.fromisoformat(row["observed_at_utc"])
        used = float(row["used_mb"])
        limit = float(row["limit_mb"])
        if observed.tzinfo is None or not math.isfinite(used) or not math.isfinite(limit):
            return None
        if used < 0 or limit <= 0:
            return None
        return observed.astimezone(UTC).isoformat(), used, limit
    except (KeyError, TypeError, ValueError):
        return None


def load_quota_history(output_root: Path, *, now: datetime | None = None) -> list[dict]:
    """Read the last 30 days without consulting the provider or mutating history."""

    observed = (now or datetime.now(UTC)).astimezone(UTC)
    cutoff = (observed - timedelta(days=HISTORY_DAYS)).isoformat()
    db_path = output_root / HISTORY_DB_NAME
    if db_path.is_file():
        connection = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True, timeout=3)
        try:
            rows = connection.execute(
                "SELECT observed_at_utc, used_mb, limit_mb FROM quota_samples "
                "WHERE observed_at_utc >= ? AND observed_at_utc <= ? ORDER BY observed_at_utc",
                (cutoff, observed.isoformat()),
            ).fetchall()
        finally:
            connection.close()
    else:
        rows = [parsed for row in _read_history(output_root / "quota_history.json")
                if isinstance(row, dict) and (parsed := _history_row(row)) is not None
                and cutoff <= parsed[0] <= observed.isoformat()]
        rows.sort(key=lambda row: row[0])
    return [{"observed_at_utc": timestamp, "used_mb": used, "limit_mb": limit}
            for timestamp, used, limit in rows]


def persist_observation(output_root: Path, observation: dict) -> int:
    parsed = _history_row(observation)
    if parsed is None:
        raise ValueError("invalid FinLab quota observation")
    output_root.mkdir(parents=True, exist_ok=True)
    db_path = output_root / HISTORY_DB_NAME
    try:
        descriptor = os.open(db_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        pass
    else:
        os.close(descriptor)
    connection = sqlite3.connect(db_path, timeout=5)
    try:
        # The public gateway has a read-only filesystem namespace. WAL mode
        # needs writable -shm sidecars for readers, so use rollback journaling.
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS quota_samples ("
            "observed_at_utc TEXT PRIMARY KEY, used_mb REAL NOT NULL, limit_mb REAL NOT NULL)"
        )
        with connection:
            if connection.execute("SELECT 1 FROM quota_samples LIMIT 1").fetchone() is None:
                legacy = (_history_row(row)
                          for row in _read_history(output_root / "quota_history.json")
                          if isinstance(row, dict))
                connection.executemany(
                    "INSERT OR IGNORE INTO quota_samples VALUES (?, ?, ?)",
                    (row for row in legacy if row is not None),
                )
            connection.execute(
                "INSERT OR REPLACE INTO quota_samples VALUES (?, ?, ?)", parsed,
            )
        cutoff = (datetime.fromisoformat(parsed[0]) - timedelta(days=HISTORY_DAYS)).isoformat()
        samples = connection.execute(
            "SELECT COUNT(*) FROM quota_samples WHERE observed_at_utc >= ?", (cutoff,),
        ).fetchone()[0]
    finally:
        connection.close()
    _atomic_json(output_root / "quota_latest.json", observation)
    return samples


def daily_reset_evidence(observation: dict, history: list[dict]) -> str | None:
    """Confirm a full post-08:00 quota or a large drop from a recent pre-reset sample."""

    observed = datetime.fromisoformat(observation["observed_at_utc"]).astimezone(TAIPEI)
    reset = datetime.combine(observed.date(), time(8), tzinfo=TAIPEI)
    if observed < reset:
        return None
    used = observation["used_mb"]
    limit = observation["limit_mb"]
    if observation["remaining_mb"] < max(50.0, limit * 0.1):
        return None
    if used <= 1.0:
        return "full_quota_observed_after_08"
    before: list[tuple[datetime, float]] = []
    for row in history:
        if not isinstance(row, dict):
            continue
        try:
            when = datetime.fromisoformat(row["observed_at_utc"]).astimezone(TAIPEI)
            previous_used = float(row["used_mb"])
            previous_limit = float(row["limit_mb"])
        except (KeyError, TypeError, ValueError):
            continue
        if (reset - timedelta(minutes=30) <= when < reset
                and abs(previous_limit - limit) < 0.01 and math.isfinite(previous_used)):
            before.append((when, previous_used))
    if before:
        previous_used = max(before, key=lambda item: item[0])[1]
    else:
        previous_used = None
    if previous_used is not None and previous_used >= 10 and used <= previous_used * 0.5:
        return "observed_quota_drop_across_08"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--confirm-daily-reset", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    from finlab.auth import get_data_status

    if args.confirm_daily_reset:
        if not 0 <= args.wait_seconds <= 120:
            parser.error("--wait-seconds must be 0..120")
        history = load_quota_history(args.output_root)
        deadline = clock.monotonic() + args.wait_seconds
        while True:
            try:
                status = get_data_status()
                observation = quota_observation(status, now=datetime.now(UTC)) if isinstance(status, dict) else None
            except Exception:
                observation = None
            if observation is not None:
                basis = daily_reset_evidence(observation, history)
                if basis is not None:
                    print(json.dumps({
                        "event": "finlab_daily_quota_ready",
                        "observed_at_utc": observation["observed_at_utc"],
                        "used_mb": observation["used_mb"],
                        "limit_mb": observation["limit_mb"],
                        "basis": basis,
                    }, separators=(",", ":")), flush=True)
                    return 0
            remaining = deadline - clock.monotonic()
            if remaining <= 0:
                print("[finlab-quota] daily reset not confirmed; no download started", file=sys.stderr)
                return 1
            clock.sleep(min(5.0, remaining))

    status = get_data_status()
    if not isinstance(status, dict):
        print("[finlab-quota] provider status unavailable", file=sys.stderr)
        return 1
    try:
        observation = quota_observation(status, now=datetime.now(UTC))
    except (KeyError, TypeError, ValueError) as error:
        print(f"[finlab-quota] invalid provider status: {type(error).__name__}", file=sys.stderr)
        return 1
    samples = persist_observation(args.output_root, observation)
    print(json.dumps({
        "event": "finlab_quota_observed",
        "observed_at_utc": observation["observed_at_utc"],
        "used_mb": observation["used_mb"],
        "limit_mb": observation["limit_mb"],
        "history_samples": samples,
    }, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
