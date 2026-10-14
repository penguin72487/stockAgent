"""Profile one complete private history audit, preserving its accepted oracle.

Profile timings include instrumentation overhead and are never speed claims.
Reuse the accepted canonical snapshot; do not issue provider queries or mutate
the running source/queue/SDK. Only the requested private engineering artifacts
and the existing private fixture's canonical summaries may be written.
"""
from __future__ import annotations

import argparse
import cProfile
from datetime import datetime, timezone
import json
from pathlib import Path
import pstats
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts import benchmark_shioaji_history_cache_policy as probe  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402


def profile(fixture: Path, reference: Path, output: Path, policy: str) -> dict:
    output = probe.audit.bounded_new_path(output)
    control = json.loads(reference.read_bytes())
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    if control.get("state") != "accepted_isolated_cache_policy_only" or control.get("fixture") != manifest:
        raise ValueError("profile requires the accepted exact memo workload")
    proofs = {**control["code_sha256"], str(Path(__file__)): sha256_file(Path(__file__))}
    if any(sha256_file(Path(name)) != digest for name, digest in proofs.items()):
        raise ValueError("profile code differs from the accepted memo workload")
    expected = control["trials"][0]
    before = probe.audit.fixture_fence(fixture, manifest)
    node = observe_node(fixture / "archive", fixture)
    timer = cProfile.Profile()
    result = timer.runcall(probe.worker, fixture, policy)
    if (result["task_sha256"] != expected["task_sha256"]
            or result["summary_sha256"] != expected["summary_sha256"]):
        raise AssertionError("profile changed complete plan/summary outputs")
    if (probe.audit.fixture_fence(fixture, manifest) != before
            or any(sha256_file(Path(name)) != digest for name, digest in proofs.items())):
        raise AssertionError("profile code/private source drift")
    stats = pstats.Stats(timer)
    rows = [{"file": key[0], "line": key[1], "function": key[2], "primitive_calls": value[0],
             "calls": value[1], "self_seconds": value[2], "cumulative_seconds": value[3]}
            for key, value in stats.stats.items()]
    output_value = {
        "contract": "shioaji_complete_local_audit_profile_v1", "state": "profile_diagnostic_only",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "reference": {
            "path": str(reference), "sha256": sha256_file(reference),
        }, "code_sha256": proofs, "fixture": manifest, "node": node,
        "instrumentation_overhead_included": True, "all_tasks_and_summaries_equal": True,
        "provider_requests": 0, "live_source_files_written": 0, "services_restarted": False,
        "worker_result": result,
        "top_self": sorted(rows, key=lambda row: row["self_seconds"], reverse=True)[:50],
        "top_cumulative": sorted(rows, key=lambda row: row["cumulative_seconds"], reverse=True)[:50],
        "total_calls": stats.total_calls, "primitive_calls": stats.prim_calls,
    }
    atomic_write_json(output, output_value)
    return output_value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--policy", choices=tuple(probe.POLICIES), default="compact")
    args = parser.parse_args()
    result = profile(args.fixture, args.reference, args.output, args.policy)
    print(json.dumps({"state": result["state"], "total_calls": result["total_calls"],
                      "all_tasks_and_summaries_equal": result["all_tasks_and_summaries_equal"]}))


if __name__ == "__main__":
    main()
