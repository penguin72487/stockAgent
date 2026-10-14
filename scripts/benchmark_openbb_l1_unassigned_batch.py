#!/usr/bin/env python3
"""Exact complete canonical source-selector trials on a private DB backup.

No production DB writes, compaction, publication, provider calls or deletion.
Selected source footers remain real canonical reads and are included in timing.
This is the unassigned-source phase, never a whole-compaction speed claim.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import inspect
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def capture(root: Path, source: Path):
    from downloader.artifact_io import atomic_write_json
    from scripts import compact_openbb_l1 as module
    if root.exists() or not root.resolve().is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("new private benchmark root required")
    if shutil.disk_usage(root.parent).free < source.stat().st_size + 5 * 1024**3:
        raise ValueError("insufficient retained private scratch")
    root.mkdir(mode=0o700)
    path = root / "manifest.sqlite3"
    with sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True) as original:
        original.execute("PRAGMA query_only=ON")
        with sqlite3.connect(path) as target:
            original.backup(target, pages=1024, sleep=0.02)
    source_function = inspect.getsource(module._load_unassigned_shards)
    (root / "original_selector.py").write_text(source_function)
    (root / "original_selector.py").chmod(0o400)
    path.chmod(0o400)
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        total = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        indexes = [str(row[1]) for row in connection.execute("PRAGMA index_list(tasks)")]
        plan = module._active_plan_token(connection)
    atomic_write_json(root / "fixture.json", {
        "contract": "openbb_private_full_manifest_source_selector_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest_sha256": sha(path), "selector_sha256": sha(root / "original_selector.py"),
        "module_sha256": sha(Path(module.__file__)), "tasks": total,
        "indexes": indexes, "active_plan_present": plan is not None,
        "production_writes": False, "provider_calls": 0,
    })
    print(json.dumps({"tasks": total, "active_plan_present": plan is not None,
                      "ordered_source_index_present": module.TASK_COMPACTION_INDEX in indexes}), flush=True)


def worker(root: Path, output: Path, variant: str, limit: int):
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)
    from downloader.artifact_io import atomic_write_json
    from scripts import compact_openbb_l1 as module
    fixture = json.loads((root / "fixture.json").read_bytes())
    manifest = root / "manifest.sqlite3"
    if fixture.get("contract") != "openbb_private_full_manifest_source_selector_v1":
        raise ValueError("unknown fixture")
    expected = (fixture["manifest_sha256"], fixture["selector_sha256"], fixture["module_sha256"])
    if expected != (sha(manifest), sha(root / "original_selector.py"), sha(Path(module.__file__))):
        raise RuntimeError("fixed manifest/selector/shared helpers changed")
    tree = ast.parse((root / "original_selector.py").read_bytes())
    if variant == "candidate":
        matches = []
        for branch in ast.walk(tree):
            if not isinstance(branch, ast.If) or "TASK_COMPACTION_INDEX" not in ast.unparse(branch.test):
                continue
            for comparison in ast.walk(branch.test):
                if isinstance(comparison, ast.Compare) and ast.unparse(comparison.left) == "0":
                    for item in comparison.comparators:
                        if isinstance(item, ast.Constant) and item.value == 8192:
                            item.value = 32768
                            matches.append(item)
        if len(matches) != 1:
            raise RuntimeError("canonical ordered-index guard changed shape")
    namespace = module.__dict__.copy()
    exec(compile(ast.fix_missing_locations(tree), "<fixed_canonical_source_selector>", "exec"), namespace)
    if output.exists() or output.is_symlink():
        raise ValueError("new output receipt required")
    with sqlite3.connect(f"file:{manifest}?mode=ro", uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.row_factory = sqlite3.Row
        timings = {}
        cpu_started, started = time.process_time(), time.perf_counter()
        selected = namespace["_load_unassigned_shards"](connection, [], limit=limit,
                                                     show_progress=False, timing=timings)
        wall, cpu = time.perf_counter()-started, time.process_time()-cpu_started
    digest = hashlib.sha256()
    for row in selected:
        digest.update(json.dumps(asdict(row), sort_keys=True, separators=(",", ":")).encode())
        digest.update(b"\n")
    if expected != (sha(manifest), sha(root / "original_selector.py"), sha(Path(module.__file__))):
        raise RuntimeError("fixture/shared helper changed during canonical selection")
    if "torch" in sys.modules:
        raise RuntimeError("source-selection trial imported a model runtime")
    peak = next(int(line.split()[1])*1024 for line in Path("/proc/self/status").read_text().splitlines()
                if line.startswith("VmHWM:"))
    result = {"state": "completed_trial", "variant": variant, "limit": limit,
              "wall_seconds": wall, "cpu_seconds": cpu, "rss_peak_bytes": peak,
              "selected_files": len(selected), "full_ordered_source_contract_sha256": digest.hexdigest(),
              "stage_seconds": timings, "fixture_manifest_sha256": fixture["manifest_sha256"],
              "shared_code_sha256": fixture["module_sha256"], "provider_calls": 0,
              "production_writes": False, "model_runtime_imported": "torch" in sys.modules,
              "scope": "Full canonical unassigned-source selector and all selected footers, not compaction/publication"}
    atomic_write_json(output, result)
    print(json.dumps(result), flush=True)


def aggregate(root: Path, output: Path):
    """Validate saved ABBA trials without replaying or mutating the database."""
    from downloader.artifact_io import atomic_write_json
    fixture = json.loads((root / "fixture.json").read_bytes())
    names = ("01-original.json", "02-candidate.json", "03-candidate.json", "04-original.json")
    trials = [json.loads((root / name).read_bytes()) for name in names]
    if [row["variant"] for row in trials] != ["original", "candidate", "candidate", "original"]:
        raise ValueError("complete ABBA ordering required")
    for row in trials:
        if (row["state"] != "completed_trial" or row["limit"] != 32768
                or row["fixture_manifest_sha256"] != fixture["manifest_sha256"]
                or row["shared_code_sha256"] != fixture["module_sha256"]
                or row["provider_calls"] != 0 or row["production_writes"] is not False
                or row["model_runtime_imported"] is not False):
            raise ValueError("unmatched/incomplete canonical trial")
    if len({(row["selected_files"], row["full_ordered_source_contract_sha256"]) for row in trials}) != 1:
        raise ValueError("full ordered source/footer parity failed")
    means = {}
    for variant in ("original", "candidate"):
        selected = [row for row in trials if row["variant"] == variant]
        means[variant] = {
            name: sum(row[name] for row in selected) / len(selected)
            for name in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")
        }
    reductions = {
        name: 100 * (1 - means["candidate"][name] / means["original"][name])
        for name in means["original"]
    }
    if output.exists() or output.is_symlink():
        raise ValueError("preserve original acceptance receipt")
    result = {
        "contract": "openbb_full_manifest_unassigned_source_abba_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "state": "accepted_bounded_phase_comparison", "tasks": fixture["tasks"],
        "fixture_manifest_sha256": fixture["manifest_sha256"],
        "trials": [dict(name=name, **row) for name, row in zip(names, trials, strict=True)],
        "means": means, "reductions_percent": reductions,
        "exact_full_ordered_source_parity": True,
        "os_page_cache_controlled": False, "whole_compaction_speed_verified": False,
        "scope": "Full fixed manifest, current no-unassigned phase; nonempty semantics require independent tests",
        "provider_calls": 0, "production_writes": False,
    }
    atomic_write_json(output, result)
    print(json.dumps({"state": result["state"], "reductions_percent": reductions}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--capture", action="store_true")
    mode.add_argument("--aggregate", action="store_true")
    mode.add_argument("--native-readback", action="store_true",
                      help="Read the completed canonical owner/phase only; no DB access or replay")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data_openBB/_state/openbb_archive.sqlite3")
    parser.add_argument("--variant", choices=("original", "candidate"), default="original")
    parser.add_argument("--limit", type=int, default=32768)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.native_readback:
        if args.output is None:
            parser.error("--native-readback requires a new engineering --output")
        native_readback(args.output.absolute())
    elif args.fixture is None:
        parser.error("--fixture is required for capture, aggregate or worker trials")
    elif args.capture:
        capture(args.fixture.absolute(), args.manifest.resolve())
    else:
        if args.output is None or args.limit <= 0 or not args.output.resolve().is_relative_to(args.fixture.resolve()):
            parser.error("positive limit and new output under fixture required")
        if args.aggregate:
            aggregate(args.fixture.resolve(), args.output.absolute())
        else:
            worker(args.fixture.resolve(), args.output.absolute(), args.variant, args.limit)


def native_readback(output: Path):
    """Accept actual indexed phase adoption, not a controlled native speedup."""
    from downloader.artifact_io import atomic_write_json

    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        raise ValueError("new engineering output under artifacts/operations required")
    path = ROOT / "data_openBB/_state/l1_compaction_attempt_latest.json"
    raw = path.read_bytes()
    receipt = json.loads(raw)
    unit = "stockagent-openbb-l1-compaction.service"
    keys = ("InvocationID", "ActiveState", "Result", "ExecMainStatus", "MainPID",
            "ExecMainStartTimestamp", "ExecMainExitTimestamp", "CPUUsageNSec", "MemoryPeak", "ExecStart")

    def identity():
        result = subprocess.run(["systemctl", "show", unit, *[f"--property={key}" for key in keys]],
                                check=True, capture_output=True, text=True, timeout=5)
        return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

    owner = identity()
    stages = receipt["stage_seconds"]
    if (receipt["state"] != "completed" or receipt["exit_code"] != 0
            or receipt["systemd_invocation_id"] != owner["InvocationID"]
            or owner["ActiveState"] != "inactive" or owner["Result"] != "success"
            or owner["ExecMainStatus"] != "0" or owner["MainPID"] != "0"
            or "--max-source-files 32768 " not in owner["ExecStart"]
            or stages.get("unassigned_source_index_order", 0) <= 0
            or stages.get("unassigned_source_global_fallback", 0) != 0
            or any(receipt[key] != 0 for key in ("pending_files", "pending_rows", "new_segments", "stale_segments", "failed_segments"))
            or receipt["compacted_files"] != receipt["success_files"]
            or receipt["compacted_rows"] != receipt["success_rows"]):
        raise ValueError("canonical idle indexed phase did not complete with matching source/segment counts")
    snapshots = receipt["stage_resources"]
    phase_cpu = (snapshots["after_unassigned_source_load"]["process_cpu_ns"]
                 - snapshots["after_stale_contract_audit"]["process_cpu_ns"]) / 1e9
    if phase_cpu < 0 or identity() != owner or path.read_bytes() != raw or "torch" in sys.modules:
        raise ValueError("owner/receipt changed or model runtime was imported")
    result = {
        "contract": "openbb_native_indexed_unassigned_phase_readback_v1",
        "state": "accepted_native_index_order_phase_adoption",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "owner": owner, "attempt": receipt, "attempt_sha256": hashlib.sha256(raw).hexdigest(),
        "implementation_sha256": sha(ROOT / "scripts/compact_openbb_l1.py"),
        "unassigned_source_wall_seconds": stages["unassigned_source_load"],
        "unassigned_source_cpu_seconds": phase_cpu,
        "production_db_writes_by_verifier": False, "services_restarted_by_verifier": False,
        "provider_calls": 0, "model_runtime_imported": False,
        "whole_compaction_controlled_speedup_verified": False,
        "scope": "Completed native 32768-file indexed phase and canonical source/segment receipt; not independent raw re-compaction",
    }
    atomic_write_json(output, result)
    print(json.dumps({"state": result["state"], "invocation_id": owner["InvocationID"],
                      "unassigned_source_wall_seconds": result["unassigned_source_wall_seconds"],
                      "unassigned_source_cpu_seconds": phase_cpu}), flush=True)


if __name__ == "__main__":
    main()
