"""Read-only summary of a terminal py-spy run and its local request window.

Stack weights are NOT CPU percentages or successful acquisition receipts.
Never read process locals, credentials, full source trees or historical bodies.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess

from downloader.artifact_io import atomic_write_json


def summarize(raw):
    categories, functions, threads = Counter(), Counter(), Counter()
    for line in raw.splitlines():
        stack, weight = line.rsplit(" ", 1)
        weight = int(weight)
        if weight <= 0:
            raise ValueError("invalid_profile_weight")
        frames = stack.split(";")
        threads[frames[0]] += weight
        names = {frame.split(" (", 1)[0] for frame in frames[1:]}
        for name in names:
            functions[name] += weight
        category = next((label for name, label in (
            ("_status", "full_status"), ("_next_task", "dispatch"),
            ("row_fingerprint", "semantic_fingerprint"), ("read_baseline", "validated_baseline_read"),
            ("incremental_reservation", "release_reservation"), ("_store", "other_publication"),
            ("_fetch_rows", "request_transport"), ("backfill_budget", "shared_quota"),
        ) if name in names), "other")
        categories[category] += weight
    total = sum(threads.values())
    if not total:
        raise ValueError("empty_profile")
    return {"total_stack_weight": total, "threads": dict(threads),
            "disjoint_categories": {key: {"weight": value, "fraction": value / total}
                                     for key, value in categories.most_common()},
            "inclusive_functions": functions.most_common(25)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--root", type=Path, default=Path("data_finmind"))
    parser.add_argument("--prior-control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or not re.fullmatch(r"[a-zA-Z0-9_-]+", args.run_id):
        parser.error("new output and valid run ID required")
    receipt = Path("artifacts/operations/agent-workflow/runs") / args.run_id / "run.json"
    run = json.loads(receipt.read_bytes())
    argv = run["argv"]
    if (run.get("state") != "succeeded" or run.get("exit_code") != 0
            or argv[:2] != ["py-spy", "record"] or "--locals" in argv):
        raise ValueError("terminal_no_locals_profile_required")
    path = Path(argv[argv.index("--output") + 1])
    if path.stat().st_size > 8 * 1024**2:
        raise ValueError("profile_exceeds_read_budget")
    summary = summarize(path.read_text())
    log = Path(run["log"]).read_text()
    counts = re.search(r"Samples: (\d+) Errors: (\d+)", log)
    if not counts or int(counts[1]) != summary["total_stack_weight"]:
        raise ValueError("profiler_log_and_raw_weight_disagree")
    summary.update(profiler_errors=int(counts[2]), duration_seconds=int(argv[argv.index("--duration") + 1]),
                   rate_hz=int(argv[argv.index("--rate") + 1]), nonblocking="--nonblocking" in argv)
    unit = "stockagent-finmind-complement.service"
    native = subprocess.run(["systemctl", "show", unit, "-p", "MainPID", "-p", "InvocationID", "-p", "ActiveState"],
                            capture_output=True, text=True, check=True, timeout=5)
    identity = dict(line.split("=", 1) for line in native.stdout.splitlines() if "=" in line)
    prior = json.loads(args.prior_control.read_bytes())["protected"][unit]
    start, end = run["started_at_utc"], run["finished_at_utc"]
    with closing(sqlite3.connect((args.root / "request_traffic.sqlite3").resolve().as_uri() + "?mode=ro",
                                 uri=True, timeout=.3)) as conn:
        starts = conn.execute("SELECT dataset,count(*) FROM requests WHERE started_at_utc>=? "
                              "AND started_at_utc<=? GROUP BY dataset", (start, end)).fetchall()
    report = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
              "state": "native_sampling_and_request_starts_observed", "run_id": args.run_id,
              "profile_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "profile": summary,
              "current_service": identity, "service_unchanged_from_prior": identity == prior,
              "profile_window_start": start, "profile_window_end": end, "request_starts": dict(starts),
              "provider_calls_by_observer": 0, "production_queue_writes": 0, "service_controls": 0,
              "caveats": ["Nonblocking low-rate stack weights can be biased; native workers/idle time not comprehensively sampled.",
                          "Inclusive function weights overlap; categories are exclusive but not CPU fractions.",
                          "Request starts include other shared-account owners and do not prove successful rows.",
                          "Profile error count is retained, not silently discarded."]}
    atomic_write_json(args.output, report)
    print(json.dumps({"state": report["state"], "profile": summary, "service_unchanged_from_prior": report["service_unchanged_from_prior"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
