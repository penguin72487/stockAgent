#!/usr/bin/env python3
"""Fresh-process ABBA of legacy imports versus the same receipt-only decision.

No dispatch or inference. Both variants read one private byte-identical snapshot
of a real job receipt and the current market configs/source-event hints. This is
not a formal-history, provider, cold-filesystem or end-to-end signal benchmark.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time as wall_time, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WORKER = """
import json, resource, sys
from scripts import run_discord_artifact_maintenance as runner
if sys.argv[1] == "legacy_import":
    runner._load_discord_bot()
probe = runner.retry_probe()
print(json.dumps({
    "benchmark_probe": probe,
    "worker_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
}, sort_keys=True), flush=True)
"""
DECISION_FIELDS = (
    "due",
    "due_keys",
    "reason",
    "job_count",
    "ignored_invalid_jobs",
    "wakeup_fingerprint",
)


def benchmark(status_path: Path, *, rounds: int = 2, timeout: float = 60) -> dict:
    if rounds < 1 or rounds > 10 or timeout <= 0 or timeout > 60:
        raise ValueError("rounds must be 1..10 and timeout in (0, 60]")
    if sys.platform != "linux":
        raise ValueError("RSS accounting for this benchmark requires Linux")
    from scripts.check_outside_tw_opening_resource_window import evaluate
    from scripts.run_discord_artifact_maintenance import RETRY_RECEIPT_MAX_BYTES

    now = datetime.now(timezone.utc)
    if not evaluate(now, minimum_runway_minutes=120, protected_until=wall_time(13, 40))[
        "allowed"
    ]:
        raise RuntimeError("legacy-import benchmark is outside the safe market runway")
    with status_path.open("rb") as handle:
        source = handle.read(RETRY_RECEIPT_MAX_BYTES + 1)
    if len(source) > RETRY_RECEIPT_MAX_BYTES:
        raise ValueError("source receipt exceeds bounded benchmark reader")
    source_digest = hashlib.sha256(source).hexdigest()
    samples = []
    decision = None
    with tempfile.TemporaryDirectory(
        prefix="stockagent-artifact-retry-benchmark-"
    ) as tmp:
        snapshot = Path(tmp) / "artifact_backfill_status.json"
        snapshot.write_bytes(source)
        env = dict(os.environ, STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH=str(snapshot))
        for variant in (
            "legacy_import",
            "receipt_probe",
            "receipt_probe",
            "legacy_import",
        ) * rounds:
            started = time.perf_counter()
            completed = subprocess.run(
                [sys.executable, "-c", WORKER, variant],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout,
            )
            wall_seconds = time.perf_counter() - started
            records = []
            for line in completed.stdout.splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict) and "benchmark_probe" in record:
                    records.append(record)
            if len(records) != 1:
                raise ValueError("worker did not emit exactly one benchmark result")
            record = records[0]
            probe = record["benchmark_probe"]
            current_decision = {key: probe.get(key) for key in DECISION_FIELDS}
            if decision is None:
                decision = current_decision
            if current_decision != decision:
                raise ValueError(
                    "retry decision/source-event hints changed between samples"
                )
            if variant == "receipt_probe" and probe["model_runtime_imported"]:
                raise ValueError("receipt-only probe imported the model runtime")
            if snapshot.read_bytes() != source:
                raise ValueError("benchmark worker mutated the receipt snapshot")
            samples.append(
                {
                    "variant": variant,
                    "wall_seconds": wall_seconds,
                    "probe_seconds": probe["probe_seconds"],
                    "worker_peak_rss_bytes": record["worker_peak_rss_bytes"],
                    "model_runtime_imported": probe["model_runtime_imported"],
                }
            )
    with status_path.open("rb") as handle:
        current = handle.read(RETRY_RECEIPT_MAX_BYTES + 1)
    source_unchanged = hashlib.sha256(current).hexdigest() == source_digest
    medians = {}
    for variant in ("legacy_import", "receipt_probe"):
        rows = [row for row in samples if row["variant"] == variant]
        medians[variant] = {
            field: statistics.median(row[field] for row in rows)
            for field in ("wall_seconds", "probe_seconds", "worker_peak_rss_bytes")
        }
    return {
        "schema_version": 1,
        "observed_at": now.isoformat(),
        "scope": "fresh_process_same_snapshot_receipt_decision_not_inference",
        "filesystem_cache": "uncontrolled_not_cold_source_claim",
        "dispatch_requests": 0,
        "source_path": str(status_path),
        "source_sha256": source_digest,
        "source_unchanged": source_unchanged,
        "worker_source_sha256": hashlib.sha256(
            (ROOT / "scripts/run_discord_artifact_maintenance.py").read_bytes()
        ).hexdigest(),
        "decision_parity": True,
        "decision": decision,
        "samples_per_variant": 2 * rounds,
        "samples": samples,
        "medians": medians,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status-path", type=Path, default=None)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    path = args.status_path or Path(
        os.environ.get(
            "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
            str(ROOT / "artifacts/discord_bot/artifact_backfill_status.json"),
        )
    )
    if not path.is_absolute():
        path = ROOT / path
    result = benchmark(path, rounds=args.rounds, timeout=args.timeout)
    from stockagent.data_sync.desync_snapshots import atomic_write_json

    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "medians": result["medians"]}))


if __name__ == "__main__":
    main()
