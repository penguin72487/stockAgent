"""Bind selected memo capacities to an unpatched full production-default audit.

Capture the measured source before changing only its two LRU capacities. Verify
the normalized module AST, exact source/oracle and the real default decorators;
then run the complete canonical local operation without monkeypatching memos.
No provider, live source, queue, SDK, package or service action.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file  # noqa: E402
from scripts import benchmark_shioaji_history_audit as audit  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

TARGETS = {audit.COLLECTOR: ("_valid_receipt_at_signature", 4_096),
           ROOT / "downloader/shioaji_history_repair.py": ("_signature_sha", 131_072)}


def normalized_module(payload: bytes, function: str) -> str:
    """Ignore exactly one LRU capacity, not other code/config/source changes."""
    tree = ast.parse(payload)
    found = 0
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function:
            for decorator in node.decorator_list:
                if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name)
                        and decorator.func.id == "lru_cache" and not decorator.args
                        and len(decorator.keywords) == 1 and decorator.keywords[0].arg == "maxsize"
                        and isinstance(decorator.keywords[0].value, ast.Constant)
                        and type(decorator.keywords[0].value.value) is int):
                    decorator.keywords[0].value = ast.Constant(0)
                    found += 1
    if found != 1:
        raise ValueError("expected exactly one literal LRU capacity")
    return hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()


def control_value(accepted: Path) -> dict:
    control = json.loads(accepted.read_bytes())
    if control.get("state") != "accepted_local_verified_hash_only" or not control.get("all_tasks_and_summaries_equal"):
        raise ValueError("requires accepted complete independent-reference benchmark")
    best = control["medians"]["compact_fd"]
    if any(not isinstance(row[name], (int, float)) or not math.isfinite(row[name]) or row[name] <= 0
           for row in control["medians"].values()
           for name in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes")):
        raise ValueError("invalid complete-workflow measurements")
    if any(best[name] >= control["medians"][other][name]
           for name in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes")
           for other in ("original", "default_fd")):
        raise ValueError("selected policy does not improve all measured resource dimensions")
    return control


def capture(accepted: Path, bundle: Path) -> dict:
    bundle = audit.bounded_new_path(bundle)
    control = control_value(accepted)
    if any(sha256_file(Path(name)) != value for name, value in control["code_sha256"].items()):
        raise ValueError("measured source changed before configuration capture")
    bundle.mkdir(parents=True, mode=0o700)
    values = {}
    for path, (function, capacity) in TARGETS.items():
        body = path.read_bytes()
        copied = bundle / path.name
        atomic_write_bytes(copied, body, mode=0o600)
        values[str(path)] = {"function": function, "selected_capacity": capacity,
                             "saved_path": str(copied), "saved_sha256": sha256_file(copied),
                             "normalized_sha256": normalized_module(body, function)}
        if values[str(path)]["saved_sha256"] != control["code_sha256"][str(path)]:
            raise AssertionError("measured implementation copy changed")
    result = {"state": "captured_measured_capacity_reference", "targets": values,
              "accepted": {"path": str(accepted), "sha256": sha256_file(accepted)}}
    atomic_write_bytes(bundle / "reference.json", json.dumps(result, indent=2).encode(), mode=0o600)
    return result


def verify(accepted: Path, bundle: Path, fixture: Path, output: Path) -> dict:
    output = audit.bounded_new_path(output)
    control = control_value(accepted)
    proof = json.loads((bundle / "reference.json").read_bytes())
    if (proof.get("state") != "captured_measured_capacity_reference"
            or proof["accepted"]["sha256"] != sha256_file(accepted)
            or set(proof["targets"]) != {str(path) for path in TARGETS}):
        raise ValueError("measured configuration identity changed")
    for path, (function, capacity) in TARGETS.items():
        original = proof["targets"][str(path)]
        saved = Path(original["saved_path"])
        if (original["function"] != function or original["selected_capacity"] != capacity
                or saved.resolve() != (bundle / path.name).resolve()
                or sha256_file(saved) != original["saved_sha256"]
                or original["saved_sha256"] != control["code_sha256"][str(path)]
                or normalized_module(saved.read_bytes(), function) != original["normalized_sha256"]
                or normalized_module(path.read_bytes(), function) != original["normalized_sha256"]):
            raise ValueError("more than the measured memo capacity changed")
    capacities = {"receipts": audit.current._valid_receipt_at_signature.cache_info().maxsize,
                  "hashes": audit.repair._signature_sha.cache_info().maxsize}
    if capacities != {"receipts": 4_096, "hashes": 131_072}:
        raise ValueError("actual loaded production decorators differ from selected capacities")
    paths = {Path(name) for name in control["code_sha256"]} | {Path(__file__), bundle / "reference.json"}
    paths.update(Path(value["saved_path"]) for value in proof["targets"].values())
    for name, value in control["code_sha256"].items():
        if Path(name) not in TARGETS and sha256_file(Path(name)) != value:
            raise ValueError("unrelated measured code/source changed")
    codes = {str(path): sha256_file(path) for path in paths}
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    if manifest != control["fixture"]:
        raise ValueError("full canonical workload changed")
    before = audit.fixture_fence(fixture, manifest)
    node = observe_node(fixture / "archive", fixture)
    result = audit.worker(fixture, "candidate")
    for name in ("task_sha256", "summary_sha256", "pending_queries"):
        if result[name] != control["trials"][0][name]:
            raise AssertionError("unpatched complete plan/summary differs from original oracle")
    if (audit.fixture_fence(fixture, manifest) != before
            or codes != {str(path): sha256_file(path) for path in paths}):
        raise AssertionError("production-default audit code/source drift")
    value = {
        "state": "accepted_production_default_local_audit_only",
        "observed_at_utc": datetime.now(UTC).isoformat(), "code_sha256": codes,
        "reference": proof, "fixture": manifest, "actual_memo_capacities": capacities,
        "all_tasks_and_summaries_equal": True, "worker_result": result,
        "node_before": node, "node_after": observe_node(fixture / "archive", fixture),
        "source_changes_only_measured_lru_capacities": True,
        "unpatched_default_decorators": True, "matched_timing_retested": False,
        "provider_requests": 0, "live_source_files_written": 0,
        "services_restarted": False, "live_owner_adoption_verified": False,
    }
    atomic_write_json(output, value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accepted", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.capture:
        value = capture(args.accepted, args.bundle)
    elif args.fixture is None or args.output is None:
        parser.error("--fixture/--output required for unpatched full validation")
    else:
        value = verify(args.accepted, args.bundle, args.fixture, args.output)
    print({"state": value["state"], "live_owner_adoption_verified": False})


if __name__ == "__main__":
    main()
