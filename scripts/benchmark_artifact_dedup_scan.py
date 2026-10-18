"""Compare the complete, read-only artifact dedup audit on existing inputs.

Preserve one small original source file, not another benchmark database. Fresh
ABBA workers compare every duplicate-group field and counter at one fixed age
cutoff. This proves audit parity, not apply safety or cold recovery.
"""
from __future__ import annotations

import argparse
import ast
import dataclasses
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import resource
import socket
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json

SOURCE = ROOT / "stockagent/data_sync/artifact_dedup.py"
CONTRACT = "complete_artifact_dedup_audit_abba_v1"


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def capture(reference: Path) -> dict:
    reference.mkdir(parents=True, exist_ok=False)
    raw = SOURCE.read_bytes()
    atomic_write_bytes(reference / "artifact_dedup.py", raw)
    proof = {"contract": CONTRACT, "source_sha256": digest(raw),
             "source_bytes": len(raw), "captured_at_utc": datetime.now(UTC).isoformat()}
    atomic_write_json(reference / "reference.json", proof)
    if raw != SOURCE.read_bytes():
        raise RuntimeError("original source changed during capture")
    return proof


def verify_census_only(reference: Path) -> None:
    """The eligibility, sampling, full SHA and apply implementations stay exact."""
    trees = [ast.parse(path.read_bytes()) for path in (reference / "artifact_dedup.py", SOURCE)]
    for tree in trees:
        selected = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "_artifact_completion_roots"]
        if len(selected) != 1:
            raise ValueError("one original completion census function is required")
        tree.body.remove(selected[0])
    if ast.dump(trees[0]) != ast.dump(trees[1]):
        raise ValueError("artifact behavior changed outside the directory census")


def run_case(reference: Path, root: Path, label: str, cutoff_ns: int) -> dict:
    def no_network(*args, **kwargs):
        raise AssertionError("an artifact audit must not connect to a provider")

    socket.socket.connect = no_network
    socket.socket.connect_ex = no_network
    socket.create_connection = no_network
    original = reference / "artifact_dedup.py"
    proof = json.loads((reference / "reference.json").read_bytes())
    if proof["contract"] != CONTRACT or digest(original.read_bytes()) != proof["source_sha256"]:
        raise ValueError("original artifact audit source changed")
    started, cpu = time.perf_counter(), time.process_time()
    if label == "baseline":
        spec = importlib.util.spec_from_file_location("original_artifact_dedup", original)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    else:
        from stockagent.data_sync import artifact_dedup as module
    # Freeze only the audit's eligibility clock, not the payload stat fences,
    # performance clocks, or the source files. Each worker retains full SHA.
    module.time.time_ns = lambda: cutoff_ns
    groups, counters = module.find_duplicate_groups(
        root, min_age_hours=24, require_complete_marker=True,
    )
    full_records = json.dumps([dataclasses.asdict(group) for group in groups], sort_keys=True,
                              default=lambda value: value.hex() if isinstance(value, bytes) else value)
    result = {"groups": module.groups_as_json(groups), "counters": counters,
              "all_group_metadata_sha256": digest(full_records.encode())}
    return {"label": label, "wall_seconds": time.perf_counter() - started,
            "cpu_seconds": time.process_time() - cpu,
            "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "result_sha256": digest(json.dumps(result, sort_keys=True).encode()), **result}


def compare(reference: Path, root: Path, output: Path) -> dict:
    if output.exists() or output.is_symlink():
        raise FileExistsError("preserve earlier comparison evidence")
    verify_census_only(reference)
    paths = (Path(__file__).resolve(), SOURCE)
    before = {str(path): digest(path.read_bytes()) for path in paths}
    cutoff = time.time_ns()
    rows, targets = [], []
    for index, label in enumerate(("baseline", "candidate", "candidate", "baseline")):
        target = output.with_name(f"{output.stem}-{index}-{label}.json")
        if target.exists() or target.is_symlink():
            raise FileExistsError("preserve earlier worker evidence")
        process = subprocess.run([
            sys.executable, str(Path(__file__).resolve()), "--reference", str(reference),
            "--root", str(root), "--label", label, "--cutoff-ns", str(cutoff),
        ], capture_output=True, text=True, timeout=300)
        if process.returncode:
            atomic_write_json(target, {"state": "failed", "label": label,
                                       "returncode": process.returncode,
                                       "stderr": process.stderr[-16000:]})
            raise RuntimeError(f"complete {label} audit failed: {process.stderr[-16000:]}")
        row = json.loads(process.stdout)
        atomic_write_json(target, row)
        if row["label"] != label or rows and any(
            row[key] != rows[0][key] for key in (
                "groups", "counters", "all_group_metadata_sha256", "result_sha256")
        ):
            raise RuntimeError("the complete audit changed during comparison")
        rows.append(row)
        targets.append(str(target))
    if before != {str(path): digest(path.read_bytes()) for path in paths}:
        raise RuntimeError("source changed during the complete comparison")
    verify_census_only(reference)
    result = {"contract": CONTRACT, "state": "accepted", "root": str(root.resolve()),
              "observed_at_utc": datetime.now(UTC).isoformat(), "age_clock_ns": cutoff,
              "source_sha256": before, "fresh_processes": len(rows),
              "complete_audit_result_sha256": rows[0]["result_sha256"],
              "all_groups_and_counters_equal": True, "counters": rows[0]["counters"],
              "full_ast_change_confined_to_completion_census": True,
              "means": {label: {key: statistics.mean(row[key] for row in rows if row["label"] == label)
                                 for key in ("wall_seconds", "cpu_seconds", "peak_rss_bytes")}
                        for label in ("baseline", "candidate")},
              "cases": targets, "payload_copies": 0, "apply_operations": 0,
              "caveats": ["Nonexclusive host; OS page cache not cleared.",
                          "Complete read-only audit; not the entire three-phase daily service.",
                          "Directory discovery is current, not a frozen source namespace."]}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--label", choices=("baseline", "candidate"))
    parser.add_argument("--cutoff-ns", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.capture:
        if args.label or args.output or args.cutoff_ns is not None:
            parser.error("capture accepts only a reference directory")
        result = capture(args.reference)
    elif args.label:
        if args.output or args.cutoff_ns is None:
            parser.error("one worker requires a fixed eligibility clock")
        result = run_case(args.reference, args.root, args.label, args.cutoff_ns)
    else:
        if args.output is None or args.cutoff_ns is not None:
            parser.error("a complete comparison requires --output")
        result = compare(args.reference, args.root, args.output)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
