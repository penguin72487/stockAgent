"""Measure receipt/SHA memo capacity on the exact accepted local audit.

Only memo decorators change inside isolated workers. All canonical validation,
source-signature checks, retry rules, plans and persisted summaries stay real.
No running owner, SDK, provider, source or package is changed by this probe.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts import benchmark_shioaji_history_audit as audit  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

POLICIES = {"original": (131_072, 16_384), "compact": (4_096, 131_072)}


def worker(fixture: Path, policy: str) -> dict:
    receipts, hashes = POLICIES[policy]
    receipt_cache = lru_cache(maxsize=receipts)(audit.current._valid_receipt_at_signature.__wrapped__)
    hash_cache = lru_cache(maxsize=hashes)(audit.repair._signature_sha.__wrapped__)
    with ExitStack() as guards:
        guards.enter_context(patch.object(audit.current, "_valid_receipt_at_signature", receipt_cache))
        guards.enter_context(patch.object(audit.repair, "_signature_sha", hash_cache))
        result = audit.worker(fixture, "candidate")
        result["hash_cache"] = hash_cache.cache_info()._asdict()
    result["policy"] = policy
    result["memo_capacity"] = {"receipts": receipts, "hashes": hashes}
    return result


def compare(fixture: Path, reference: Path, output: Path) -> dict:
    output = audit.bounded_new_path(output)
    control = json.loads(reference.read_bytes())
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    if control.get("state") != "accepted_local_audit_only" or control.get("fixture") != manifest:
        raise ValueError("cache probe must reuse the exact accepted real workload")
    paths = (*audit.CODE_FILES, Path(__file__), ROOT / "stockagent/remote_build.py")
    proofs = {str(path): sha256_file(path) for path in paths}
    # The accepted baseline implementation and core validator must still match;
    # the cache probe itself is new and is fenced separately.
    for name, digest in control["code_sha256"].items():
        if sha256_file(ROOT / name) != digest:
            raise ValueError("accepted local-audit code changed before cache probe")
    before = audit.fixture_fence(fixture, manifest)
    node = observe_node(fixture / "archive", fixture)
    expected = control["trials"][0]
    trials = []
    for policy in ("original", "compact", "compact", "original"):
        started = time.perf_counter()
        result = subprocess.run(
            [sys.executable, __file__, "--fixture", str(fixture), "--worker", policy],
            capture_output=True, text=True, check=True, timeout=1200, cwd=ROOT,
        )
        trial = json.loads(result.stdout)
        trial["invocation_seconds"] = time.perf_counter() - started
        if trial["task_sha256"] != expected["task_sha256"] or trial["summary_sha256"] != expected["summary_sha256"]:
            raise AssertionError("complete canonical plan/summary changed under memo policy")
        trials.append(trial)
        print(json.dumps({key: trial[key] for key in (
            "policy", "elapsed_seconds", "cpu_seconds", "peak_rss_bytes", "pending_queries",
        )}), flush=True)
    if audit.fixture_fence(fixture, manifest) != before or proofs != {
        str(path): sha256_file(path) for path in paths
    }:
        raise AssertionError("cache probe code/source drift")
    result = {
        "contract": "shioaji_local_audit_memo_capacity_abba_v1",
        "state": "accepted_isolated_cache_policy_only",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "reference": {"path": str(reference), "sha256": sha256_file(reference)},
        "fixture": manifest, "all_tasks_and_summaries_equal": True,
        "scope": control["scope"], "excluded": control["excluded"],
        "provider_requests": 0, "source_files_written": 0, "services_restarted": False,
        "policies": POLICIES, "os_cache_controlled": False, "code_sha256": proofs,
        "node_before": node, "node_after": observe_node(fixture / "archive", fixture),
        "trials": trials,
        "medians": {policy: {key: statistics.median(row[key] for row in trials if row["policy"] == policy)
                              for key in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes", "invocation_seconds")}
                    for policy in POLICIES},
    }
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--worker", choices=tuple(POLICIES))
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
    elif args.reference is None or args.output is None:
        parser.error("--reference and --output required for paired probe")
    else:
        result = compare(args.fixture, args.reference, args.output)
        print(json.dumps({"state": result["state"], "medians": result["medians"]}))


if __name__ == "__main__":
    main()
