"""One-off canonical FinMind reload, only inside a proved drained idle window.

No provider requests, task transitions, source writes or trading-owner actions.
Never interrupt an in-flight batch to adopt a performance change. This is an
explicit operator command, not an installed poller or a second queue owner.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json
from downloader.finmind_queue_indexes import STATUS_INDEX
from downloader.finmind_runtime import idle_heartbeat
from downloader.finmind_scheduling import protected_stock_opening
from scripts.benchmark_finmind_status import AGGREGATE_SQL, file_sha

UNIT = "stockagent-finmind-complement.service"
PROTECTED = ("stockagent-tw-day-trade-simulation.service", "stockagent-shioaji-taifex-bidask.service")
ROOT = REPO / "data_finmind/complement"


def metadata(path, maximum=4 * 1024**2):
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("oversized_local_status")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("invalid_local_metadata_object")
    return value


def identity(unit):
    result = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState", "-p", "InvocationID", "-p", "MainPID"],
                            capture_output=True, text=True, check=True, timeout=5)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def idle_proof(root, now):
    status, waiting = metadata(root / "status.json"), metadata(root / "worker_status.json", 8192)
    reason = waiting.get("reason")
    # Complement's completed-cycle wait has no wake_if. Only quota-reserve
    # waits are reactive in wait_for_next_cycle(); opening protection is a
    # genuine drained wait, but authorize it only while the canonical calendar
    # gate is still in force. Never create an artificial idle window.
    nonreactive = reason in {"batch_complete", "current_queue"} or (
        reason == "protected_opening" and protected_stock_opening(now)
    )
    if (idle_heartbeat(root, status, now).get("state") != "waiting"
            or not nonreactive
            or status.get("active_task") is not None):
        return None
    deadline = datetime.fromisoformat(waiting["next_check_at_utc"])
    if deadline - now < timedelta(seconds=2):
        return None
    with closing(sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
        claims = {}
        for table in ("finmind_object_prefetch_claims", "complement_year_batch_claims"):
            if connection.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
                claims[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if any(claims.values()):
            return None
    return {"cycle_observed_at_utc": status["observed_at_utc"], "reason": status["state"],
            "deadline": deadline.isoformat(), "claims": claims, "last_task": status.get("last_task")}


def prior_files(root, last):
    """Bounded preservation sample, not a full source-completeness claim."""
    if not isinstance(last, dict):
        return []
    with closing(sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
        row = connection.execute("SELECT receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
                                 tuple(last.get(key) for key in ("dataset", "data_id", "partition"))).fetchone()
    if not row or not row[0]:
        return []
    receipt = (root / row[0]).resolve()
    if not receipt.is_relative_to(root.resolve()):
        raise ValueError("prior_receipt_outside_owner_root")
    value = metadata(receipt)
    paths = [receipt]
    if value.get("parquet_path"):
        parquet = (root / value["parquet_path"]).resolve()
        if not parquet.is_relative_to(root.resolve()):
            raise ValueError("prior_parquet_outside_owner_root")
        paths.append(parquet)
    return [{"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha(path)}
            for path in paths if path.stat().st_size <= 8 * 1024**2]


def code_matches(accepted):
    hashes = accepted.get("code_sha256", {})
    required = {str(REPO / "downloader/download_finmind_complement.py"),
                str(REPO / "downloader/finmind_queue_indexes.py")}
    if "whole_publication_wall_improvement_observed" in accepted:
        required.add(str(REPO / "downloader/finmind_status_aggregates.py"))
    return (required.issubset(hashes)
            and all(file_sha(Path(path)) == digest for path, digest in hashes.items()))


def accepted_local_measurement(accepted):
    """An exact full local publication quantum, not only a fast status query."""
    if "whole_publication_wall_improvement_observed" not in accepted:
        return accepted.get("status_wall_improvement_observed") is True
    return bool(accepted.get("state") == "measurement_complete"
                and accepted.get("whole_publication_wall_improvement_observed") is True
                and accepted.get("all_task_values_equal") is True
                and accepted.get("source_files_stable") is True
                and accepted.get("resource_limits_stable") is True
                and accepted.get("task_count", 0) > 0
                and accepted.get("rows_published_per_trial", 0) > 0
                and [row.get("label") for row in accepted.get("trials", [])]
                    == ["baseline", "candidate", "candidate", "baseline"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-seconds", type=int, default=300)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if args.output.exists() or not 0 <= args.wait_seconds <= 600:
        parser.error("new output required; bounded wait must be 0..600 seconds")
    accepted = metadata(args.acceptance)
    if (not accepted.get("exact_output_parity") or not accepted.get("code_stable")
            or not accepted_local_measurement(accepted)
            or Path(accepted["source_queue"]).resolve() != (ROOT / "queue.sqlite3").resolve()
            or not code_matches(accepted)):
        raise ValueError("current_code_not_bound_to_accepted_measurement")
    if shutil.disk_usage(ROOT).free < 26 * 1024**3:
        raise ValueError("queue_index_headroom_guard")
    before = identity(UNIT)
    if before.get("ActiveState") != "active" or not before.get("InvocationID") or before.get("MainPID") in {None, "0"}:
        raise ValueError("reload_requires_the_current_running_owner")
    protected = {unit: identity(unit) for unit in PROTECTED}
    end = time.monotonic() + args.wait_seconds
    proof = None
    while True:
        try:
            proof = idle_proof(ROOT, datetime.now(UTC))
        except (OSError, ValueError, KeyError, sqlite3.Error):
            proof = None
        if proof or time.monotonic() >= end or not args.apply:
            break
        time.sleep(.5)
    report = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(), "unit": UNIT,
              "service_before": before, "idle_proof": proof, "apply_requested": args.apply,
              "restart_requested": False, "provider_calls_by_this_command": 0, "source_writes": 0,
              "queue_task_mutations": 0, "protected_services_before": protected}
    if not proof or not args.apply:
        report["state"] = "deferred_no_safe_idle_window" if not proof else "idle_observed_no_action"
        atomic_write_json(args.output, report)
        return 0
    samples = prior_files(ROOT, proof["last_task"])
    report["sampled_prior_files"] = samples
    # The short batch-complete sleep is non-reactive. Check the exact same
    # service generation and remaining deadline immediately before dispatch.
    if (identity(UNIT) != before or not code_matches(accepted)
            or idle_proof(ROOT, datetime.now(UTC)) != proof
            or datetime.fromisoformat(proof["deadline"]) - datetime.now(UTC) < timedelta(seconds=1)):
        report["state"] = "deferred_generation_or_deadline_changed"
        atomic_write_json(args.output, report)
        return 0
    stamp = datetime.now(UTC)
    report.update(state="restart_requested", restart_requested=True, restart_at_utc=stamp.isoformat())
    atomic_write_json(args.output, report)  # Retain action evidence even if interrupted.
    subprocess.run(["systemctl", "restart", UNIT], check=True, timeout=30)
    until = time.monotonic() + 90
    while time.monotonic() < until:
        after = identity(UNIT)
        try:
            status = metadata(ROOT / "status.json")
            with closing(sqlite3.connect((ROOT / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as connection:
                plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + AGGREGATE_SQL)]
                statistics = connection.execute("SELECT stat FROM sqlite_stat1 WHERE idx=?", (STATUS_INDEX,)).fetchone()
            if (after["ActiveState"] == "active" and after["InvocationID"] != before["InvocationID"]
                    and datetime.fromisoformat(status["observed_at_utc"]) > stamp
                    and statistics and any("COVERING INDEX " + STATUS_INDEX in row for row in plan)):
                report.update(state="canonical_owner_schema_and_status_verified", service_after=after,
                              status_observed_at_utc=status["observed_at_utc"], index_stat=statistics[0], aggregate_plan=plan)
                break
        except (OSError, ValueError, KeyError, sqlite3.Error):
            pass
        time.sleep(1)
    else:
        report["state"] = "restart_readback_unverified"
    report["protected_services_after"] = {unit: identity(unit) for unit in PROTECTED}
    report["protected_service_identities_unchanged"] = report["protected_services_after"] == protected
    report["sampled_prior_files_unchanged"] = all(
        Path(item["path"]).is_file() and file_sha(Path(item["path"])) == item["sha256"] for item in samples
    ) if samples else None
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("state", "restart_requested", "protected_service_identities_unchanged")}))
    return 0 if report["state"] == "canonical_owner_schema_and_status_verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
