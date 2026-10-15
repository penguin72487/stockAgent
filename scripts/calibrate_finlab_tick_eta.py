#!/usr/bin/env python3
"""One-time local journal/quota calibration for existing Tick batch history.

No login, SDK, provider requests, raw data or writer-index access. Only bounded
aggregate batch receipts are exported; private log messages are not persisted.
The running acquisition owner has its own index and never writes this seed.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.download_finlab_history import _atomic_json  # noqa: E402
from stockagent.data.finlab_tick_eta import BATCH_METRICS_VERSION, measured_capacity, timestamp  # noqa: E402


def interpolate_quota(samples: list[tuple], at: datetime) -> float | None:
    index = bisect_left([row[0] for row in samples], at)
    if not 0 < index < len(samples):
        return None
    before, after = samples[index - 1], samples[index]
    if ((at - before[0]).total_seconds() > 90 or (after[0] - at).total_seconds() > 90
            or before[0].date() != after[0].date() or before[1] > after[1]):
        return None
    span = (after[0] - before[0]).total_seconds()
    return before[1] + (after[1] - before[1]) * (at - before[0]).total_seconds() / span if span else after[1]


def reconstruct(messages: list[str], samples: list[tuple]) -> list[dict]:
    """Only full committed summaries, with cumulative acquisition reconciliation."""
    records = {}
    previous_received = None
    for message in messages:
        if not message.startswith("{"):
            continue
        try:
            raw = json.loads(message)
        except ValueError:
            continue
        if raw.get("acquisition_mode") != "tick_api_only_minute_derived_locally":
            continue
        current = raw.get("by_kind", {}).get("tw_tick", {}).get("receipted_partitions")
        attempts, successes = raw.get("attempted_this_run", 0), raw.get("successes_this_run", 0)
        start, end = timestamp(raw.get("run_started_at_utc")), timestamp(raw.get("observed_at_utc"))
        new = max(0, current - previous_received) if isinstance(current, int) and previous_received is not None else None
        if isinstance(current, int):
            previous_received = current
        if not attempts or not start or not end or end <= start or new is None or new > successes:
            continue
        before, after = interpolate_quota(samples, start), interpolate_quota(samples, end)
        delta = ((after - before) * 1024**2 if before is not None and after is not None
                 and start.date() == end.date() and after >= before else None)
        records[start] = {
            "contract_version": BATCH_METRICS_VERSION, "origin": "journal_reconstructed",
            "started_at_utc": start.isoformat(), "finished_at_utc": end.isoformat(),
            "elapsed_seconds": (end - start).total_seconds(), "attempt_limit": 256,
            "attempted": attempts, "completed_attempts": attempts, "successful": successes,
            "new_receipted_partitions": new, "state": raw.get("state"),
            "account_delta_bytes": round(delta) if delta is not None else None,
            "quota_sample_successes": successes if delta is not None else 0,
            "billing_basis": "one_minute_shared_account_interpolation_tick_worker_proxy",
            "receipt_counter_reconciled": new <= successes,
        }
    return [records[key] for key in sorted(records)[-256:]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--days", type=int, default=3, choices=range(1, 8))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    now = datetime.now(UTC)
    since = now - timedelta(days=args.days)
    log = subprocess.run(["journalctl", "-u", "stockagent-finlab-local-refresh.service", "--since",
                          since.strftime("%Y-%m-%d %H:%M:%S UTC"), "--no-pager", "--all", "-o", "json"],
                         capture_output=True, text=True, timeout=30, check=True)
    messages = []
    for line in log.stdout.splitlines():
        try:
            message = json.loads(line).get("MESSAGE")
        except ValueError:
            continue
        if isinstance(message, str) and message.startswith('{"acquisition_mode":'):
            messages.append(message)
    database = args.root / "artifacts/live/finlab/quota_history.sqlite3"
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5) as connection:
        samples = [(timestamp(at), used) for at, used in connection.execute(
            "SELECT observed_at_utc,used_mb FROM quota_samples WHERE observed_at_utc >= ? "
            "ORDER BY observed_at_utc", (since.isoformat(),))]
    samples = [row for row in samples if row[0] is not None]
    batches = reconstruct(messages, samples)
    destination = args.output or args.root / "data_finlab/intraday/batch_metrics_seed.json"
    _atomic_json(destination, {"contract_version": BATCH_METRICS_VERSION,
                              "generated_at_utc": now.isoformat(), "provider_calls": 0,
                              "source": "canonical_finlab_service_journal_and_minute_quota_samples",
                              "batches": batches})
    print(json.dumps({"provider_calls": 0, "batches": len(batches),
                      "capacity": measured_capacity(batches, now=now), "output": str(destination)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
