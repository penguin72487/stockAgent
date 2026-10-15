"""Independent full-SQL readback of the existing FinMind owner's memo adoption.

No API, queue/source writes, service control, CUDA or inference. Compare the
complete current status with the uncached canonical SQL path, not a cache flag.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest.mock import patch

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from scripts.benchmark_finmind_status import file_sha
from scripts.reload_finmind_at_idle import UNIT, ROOT, code_matches, identity


def replay_status(connection, root, status):
    """Replay both recorded clocks, without excluding any field from equality.

    The canonical publisher records observed_at and frontier estimate_as_of
    with separate _now() calls. Freezing both to observed_at would manufacture
    a microsecond mismatch even when every SQL aggregate is identical.
    """
    observed = datetime.fromisoformat(status["observed_at_utc"])
    frontier_clocks = {item["estimate_as_of_utc"]
                       for item in status["historical_frontiers"].values()}
    if len(frontier_clocks) > 1:
        raise ValueError("native_frontier_clocks_inconsistent")
    frontier = datetime.fromisoformat(next(iter(frontier_clocks))) if frontier_clocks else observed
    if observed.utcoffset() is None or frontier.utcoffset() is None or frontier < observed:
        raise ValueError("native_status_clock_order_invalid")
    # _next_task, top-level observation, frontier estimate. Unexpected extra
    # clock reads fail instead of silently masking a changed status contract.
    with patch.object(worker, "_now", side_effect=[observed, observed, frontier]), \
            patch.object(worker, "atomic_write_json", side_effect=lambda *a, **k: None):
        result = worker._status(connection, root, state=status["state"], last=status.get("last_task"),
                                delegated=frozenset(status.get("delegated_to_sponsor", ())))
    return result, {"observed_at_utc": observed.isoformat(), "estimate_as_of_utc": frontier.isoformat()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--reload", type=Path, required=True)
    parser.add_argument("--prior-control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("new output required")
    accepted = json.loads(args.acceptance.read_bytes())
    control = json.loads(args.reload.read_bytes())
    if (control.get("state") != "canonical_owner_schema_and_status_verified" or not code_matches(accepted)
            or not control.get("protected_service_identities_unchanged")
            or control.get("sampled_prior_files_unchanged") is not True):
        raise ValueError("native_reload_and_preservation_proof_required")
    generation = identity(UNIT)
    if generation != control["service_after"]:
        raise ValueError("native_owner_changed_after_reload")
    status_path = ROOT / "status.json"
    body = status_path.read_bytes()
    status = json.loads(body)
    if status.get("active_task") is not None:
        raise ValueError("independent_readback_requires_drained_status")
    statements = []
    with closing(sqlite3.connect((ROOT / "queue.sqlite3").as_uri() + "?mode=ro", uri=True, timeout=.3)) as conn, \
            patch.object(worker.requests.Session, "request", side_effect=AssertionError("provider_forbidden")):
        conn.set_trace_callback(statements.append)
        uncached, clocks = replay_status(conn, ROOT, status)
        persistent_memo_triggers = conn.execute("SELECT name FROM main.sqlite_master "
                                                "WHERE type='trigger' AND name LIKE '_finmind_status_memo%'").fetchall()
    raw = subprocess.run(["journalctl", "--no-pager", "-u", UNIT, "--since", control["restart_at_utc"],
                          "-n", "500", "-o", "json"], capture_output=True, text=True, check=True, timeout=10)
    modes = []
    for line in raw.stdout.splitlines():
        row = json.loads(line)
        if row.get("_SYSTEMD_INVOCATION_ID") != generation["InvocationID"]:
            continue
        try:
            value = json.loads(row.get("MESSAGE", ""))
        except (ValueError, TypeError):
            continue
        value = value.get("finmind_status_aggregation") if isinstance(value, dict) else None
        if isinstance(value, dict):
            modes.append({key: value.get(key) for key in ("contract_version", "mode", "groups_recomputed", "total_groups")})
    prior = json.loads(args.prior_control.read_bytes())["protected"]
    protected = {unit: identity(unit) for unit in prior if unit != UNIT}
    protected_equal = all(value == prior[unit] for unit, value in protected.items())
    control_group = subprocess.run(["systemctl", "show", UNIT, "-p", "ControlGroup", "--value"],
                                   capture_output=True, text=True, check=True, timeout=5).stdout.strip()
    pids = (Path("/sys/fs/cgroup") / control_group.lstrip("/") / "cgroup.procs").read_text().split()
    gpu_handles, unreadable = [], []
    for pid in pids:
        try:
            for descriptor in (Path("/proc") / pid / "fd").iterdir():
                try:
                    target = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                if target == "/dev/dxg" or target.startswith(("/dev/nvidia", "/dev/dri/render")):
                    gpu_handles.append({"pid": int(pid), "target": target})
        except OSError:
            unreadable.append(int(pid))
    complete_query = any(query.startswith("SELECT dataset,state,COUNT(*),SUM(rows)") for query in statements)
    reuse = any(row["mode"] in {"changed_groups", "unchanged"} for row in modes)
    stable = status_path.read_bytes() == body and identity(UNIT) == generation and code_matches(accepted)
    passed = (uncached == status and complete_query and reuse and protected_equal and stable
              and not persistent_memo_triggers and not gpu_handles and not unreadable and "torch" not in sys.modules)
    report = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
              "state": "accepted_bounded_finmind_status_cache" if passed else "native_adoption_unverified",
              "native_owner": generation, "native_aggregation_modes": modes,
              "native_complete_status_equals_uncached_canonical_sql": uncached == status,
              "replayed_recorded_status_clocks": clocks,
              "complete_aggregate_query_executed": complete_query, "native_generation_and_status_stable": stable,
              "protected_services": protected, "protected_services_unchanged": protected_equal,
              "prior_source_files_preserved": control["sampled_prior_files_unchanged"],
              "status_sha256": hashlib.sha256(body).hexdigest(), "code_sha256": accepted["code_sha256"],
              "observer_code_sha256": file_sha(Path(__file__)),
              "sampled_native_pids": [int(pid) for pid in pids], "gpu_device_handles": gpu_handles,
              "unreadable_sampled_pids": unreadable, "observer_torch_imported": "torch" in sys.modules,
              "provider_calls_by_observer": 0, "source_writes": 0, "service_controls": 0,
              "caveats": ["Bounded local workflow and native aggregation adoption only, not all services optimized.",
                          "Idle opening protection is not evidence of all history completion or API saturation.",
                          "Linux role-specific FD sample, not all Windows VRAM or GPU context history."]}
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("state", "native_complete_status_equals_uncached_canonical_sql",
                                                  "native_aggregation_modes", "protected_services_unchanged")}))
    if not passed:
        raise ValueError("independent_native_status_adoption_not_proven")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
