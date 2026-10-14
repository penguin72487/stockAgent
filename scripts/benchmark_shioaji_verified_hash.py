"""Compare owned-FD SHA verification on the complete accepted local audit.

Preserve the original repair implementation before editing it. Independent
workers compare full plans, three full coverage summaries and atomic readback;
the old digest is not rebuilt from the candidate. No provider/SDK/service action.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import UTC, datetime
from functools import lru_cache
import importlib.util
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

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file  # noqa: E402
from scripts import benchmark_shioaji_history_audit as audit  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

REPAIR = ROOT / "downloader/shioaji_history_repair.py"
POLICIES = {"original": (131_072, 16_384), "default_fd": (131_072, 16_384),
            "compact_fd": (4_096, 131_072)}
MUTABLE = {str(REPAIR), str(audit.COLLECTOR)}


def prepare_reference(fixture: Path, accepted: Path, bundle: Path) -> dict:
    """Capture the unchanged accepted implementation before candidate edits."""
    bundle = audit.bounded_new_path(bundle)
    control = json.loads(accepted.read_bytes())
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    if control.get("state") != "accepted_isolated_cache_policy_only" or control.get("fixture") != manifest:
        raise ValueError("reference requires the exact accepted memo workload")
    if any(sha256_file(Path(name)) != value for name, value in control["code_sha256"].items()):
        raise ValueError("original implementation changed before reference capture")
    original = REPAIR.read_bytes()
    bundle.mkdir(parents=True, mode=0o700)
    atomic_write_bytes(bundle / "repair_reference.py", original, mode=0o600)
    proof = {
        "contract": "shioaji_verified_hash_independent_reference_v1",
        "state": "captured_original_reference", "fixture": manifest,
        "accepted": {"path": str(accepted), "sha256": sha256_file(accepted)},
        "original_code_sha256": control["code_sha256"],
        "repair_sha256": sha256_file(bundle / "repair_reference.py"),
        "expected": {name: control["trials"][0][name]
                     for name in ("task_sha256", "summary_sha256", "pending_queries")},
    }
    if proof["repair_sha256"] != control["code_sha256"][str(REPAIR)]:
        raise AssertionError("original reference copy changed")
    atomic_write_bytes(bundle / "reference.json", json.dumps(proof, indent=2).encode(), mode=0o600)
    return proof


def reference_proof(fixture: Path, bundle: Path) -> dict:
    proof = json.loads((bundle / "reference.json").read_bytes())
    accepted_path = Path(proof["accepted"]["path"])
    accepted = json.loads(accepted_path.read_bytes())
    if (proof.get("state") != "captured_original_reference"
            or proof.get("fixture") != json.loads((fixture / "manifest.json").read_bytes())
            or sha256_file(bundle / "repair_reference.py") != proof["repair_sha256"]
            or sha256_file(accepted_path) != proof["accepted"]["sha256"]
            or accepted.get("state") != "accepted_isolated_cache_policy_only"
            or accepted.get("fixture") != proof["fixture"]
            or proof["original_code_sha256"] != accepted.get("code_sha256")
            or proof["repair_sha256"] != accepted["code_sha256"][str(REPAIR)]
            or proof["expected"] != {name: accepted["trials"][0][name]
                                     for name in ("task_sha256", "summary_sha256", "pending_queries")}):
        raise ValueError("independent reference/fixture identity changed")
    return proof


def load_original(bundle: Path):
    name = "_shioaji_verified_hash_original"
    spec = importlib.util.spec_from_file_location(name, bundle / "repair_reference.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def worker(fixture: Path, bundle: Path, policy: str) -> dict:
    proof = reference_proof(fixture, bundle)
    implementation = load_original(bundle) if policy == "original" else audit.repair
    receipts, hashes = POLICIES[policy]
    receipt_cache = lru_cache(maxsize=receipts)(audit.current._valid_receipt_at_signature.__wrapped__)
    hash_cache = lru_cache(maxsize=hashes)(implementation._signature_sha.__wrapped__)
    with ExitStack() as guards:
        guards.enter_context(patch.object(audit.current, "_valid_receipt_at_signature", receipt_cache))
        guards.enter_context(patch.object(implementation, "_signature_sha", hash_cache))
        guards.enter_context(patch.object(audit.current, "verified_sha", implementation.verified_sha))
        result = audit.worker(fixture, "candidate")
        result["hash_cache"] = hash_cache.cache_info()._asdict()
    result["policy"] = policy
    result["memo_capacity"] = {"receipts": receipts, "hashes": hashes}
    for name in ("task_sha256", "summary_sha256", "pending_queries"):
        if result[name] != proof["expected"][name]:
            raise AssertionError(f"complete accepted oracle changed: {name}")
    return result


def compare(fixture: Path, bundle: Path, output: Path) -> dict:
    output = audit.bounded_new_path(output)
    proof = reference_proof(fixture, bundle)
    # Only the two deliberately optimized source files may differ from capture.
    for name, value in proof["original_code_sha256"].items():
        if name not in MUTABLE and sha256_file(Path(name)) != value:
            raise ValueError("unrelated accepted workload code changed")
    paths = (*audit.CODE_FILES, Path(__file__), ROOT / "stockagent/remote_build.py",
             ROOT / "downloader/download_shioaji_tx_futures_ticks.py",
             bundle / "repair_reference.py", bundle / "reference.json")
    codes = {str(path): sha256_file(path) for path in paths}
    manifest = proof["fixture"]
    before = audit.fixture_fence(fixture, manifest)
    node = observe_node(fixture / "archive", fixture)
    result = {
        "contract": "shioaji_complete_verified_hash_balanced_v1", "state": "running",
        "observed_at_utc": datetime.now(UTC).isoformat(), "fixture": manifest,
        "reference": proof, "code_sha256": codes, "policies": POLICIES,
        "scope": "inventory+official activity+full plan+three complete coverage summaries+atomic persistence",
        "excluded": ["native SDK/login", "provider query loop", "publication"],
        "provider_requests": 0, "live_source_files_written": 0, "services_restarted": False,
        "os_cache_controlled": False, "node_before": node, "trials": [],
    }
    try:
        for policy in ("original", "compact_fd", "default_fd", "default_fd", "compact_fd", "original"):
            started = time.perf_counter()
            completed = subprocess.run(
                [sys.executable, __file__, "--fixture", str(fixture), "--bundle", str(bundle),
                 "--worker", policy], capture_output=True, text=True, check=True, timeout=1200, cwd=ROOT,
            )
            trial = json.loads(completed.stdout)
            if any(trial.get(name) != proof["expected"][name]
                   for name in ("task_sha256", "summary_sha256", "pending_queries")):
                raise AssertionError("independent worker complete output oracle failed")
            trial["invocation_seconds"] = time.perf_counter() - started
            result["trials"].append(trial)
            print(json.dumps({name: trial[name] for name in (
                "policy", "elapsed_seconds", "cpu_seconds", "peak_rss_bytes", "pending_queries",
            )}), flush=True)
        if (audit.fixture_fence(fixture, manifest) != before
                or codes != {str(path): sha256_file(path) for path in paths}):
            raise AssertionError("full comparison code/source drift")
        result.update(
            state="accepted_local_verified_hash_only", all_tasks_and_summaries_equal=True,
            node_after=observe_node(fixture / "archive", fixture),
            medians={policy: {name: statistics.median(row[name] for row in result["trials"]
                                                     if row["policy"] == policy)
                              for name in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes",
                                           "invocation_seconds")}
                     for policy in POLICIES},
        )
    except Exception as exc:
        result.update(state="failed", failure_type=type(exc).__name__, all_tasks_and_summaries_equal=False)
        raise
    finally:
        atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--prepare-reference", action="store_true")
    parser.add_argument("--accepted", type=Path)
    parser.add_argument("--worker", choices=tuple(POLICIES))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.prepare_reference:
        if args.accepted is None or args.worker or args.output:
            parser.error("reference capture needs --accepted, without --worker/--output")
        value = prepare_reference(args.fixture, args.accepted, args.bundle)
        print(json.dumps({"state": value["state"], "repair_sha256": value["repair_sha256"]}))
    elif args.worker:
        print(json.dumps(worker(args.fixture, args.bundle, args.worker)))
    elif args.output is None:
        parser.error("paired comparison requires --output")
    else:
        value = compare(args.fixture, args.bundle, args.output)
        print(json.dumps({"state": value["state"], "medians": value["medians"]}))


if __name__ == "__main__":
    main()
