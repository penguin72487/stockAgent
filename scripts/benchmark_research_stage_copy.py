"""Real-byte ABBA probe of unchanged research staging; no source publication.

Uses an explicitly selected sealed private capture. The baseline is the exact
original Git function, not a simulated sleep or a shorter input. Full source
and target SHA verification remains required on every candidate call.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
BASELINE_REVISION = "c5b0d1add80b195a39d5d6808600581cea8dfa71"
BASELINE_AST_SHA256 = "a0fe847b509d4b067c206aacc88d28618f415dbfe7c628afe207b373a5129923"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.stage_tw_public_research_release import _stage_copy
from stockagent.data_sync.cache_capacity import load_capacity_policy, reserve_cache_write
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
from stockagent.data_sync.desync_snapshots import sha256_file


def benchmark(source, output, *, baseline_revision=BASELINE_REVISION):
    source, output = source.absolute(), output.absolute()
    if (source.resolve(strict=True) != source or not source.is_relative_to(ROOT / "artifacts/benchmarks")
            or output.exists() or output.resolve() != output or not output.is_relative_to(ROOT / "artifacts/benchmarks")):
        raise ValueError("sealed private capture and fresh private benchmark output required")
    before = payload_info(source)
    if before["mode"] & 0o222 or before["uid"] != os.geteuid():
        raise ValueError("source must be an owned readonly captured original")
    expected = stable_payload_hash(source, before)
    name = "scripts/stage_tw_public_research_release.py"
    baseline_source = subprocess.run(["git", "show", f"{baseline_revision}:{name}"], cwd=ROOT, check=True,
                                     stdout=subprocess.PIPE).stdout
    tree = ast.parse(baseline_source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_stage_copy")
    baseline_ast = ast.dump(function, include_attributes=False)
    if hashlib.sha256(baseline_ast.encode()).hexdigest() != BASELINE_AST_SHA256:
        raise ValueError("selected Git revision does not contain the original audited baseline")
    namespace = dict(shutil=shutil, os=os, uuid=uuid, sha256_file=sha256_file, Path=Path)
    exec(compile(ast.Module(body=[function], type_ignores=[]), "original_git_stage_copy", "exec"), namespace)
    codes = {str(path): sha256_file(path) for path in (Path(__file__), ROOT / name,
             ROOT / "stockagent/data_sync/immutable_cache_files.py")}
    result = {"contract": "real_captured_research_staging_abba_v1", "source": {
        "path": str(source), "bytes": before["size"], "sha256": expected}, "code_sha256": codes,
        "baseline_ast_sha256": hashlib.sha256(baseline_ast.encode()).hexdigest(),
        "baseline_revision": baseline_revision,
        "scope": "unchanged research file staging only; not a full research build/publication/model workflow",
        "baseline_has_no_explicit_fsync": True, "candidate_new_writes_fsync": True,
        "provider_calls": 0, "production_writes": False, "service_restarts": 0,
        "load_average_before": os.getloadavg(), "samples": []}
    output.mkdir(parents=True, mode=0o700)
    allowance = 3*before["size"] + 16*1024**2
    with reserve_cache_write(output, key=expected, maximum_growth_bytes=allowance,
                             family="prepared", policy=load_capacity_policy()):
        targets = {name: output / name / source.name for name in ("baseline", "candidate")}
        for label, operation in (("baseline", namespace["_stage_copy"]), ("candidate", _stage_copy)):
            wall, cpu = time.perf_counter(), time.process_time()
            operation(source, targets[label], expected)
            result[label + "_seed"] = {"wall_seconds": time.perf_counter()-wall, "cpu_seconds": time.process_time()-cpu}
        candidate_inode = payload_info(targets["candidate"])
        for label in ("baseline", "candidate", "candidate", "baseline"):
            operation = namespace["_stage_copy"] if label == "baseline" else _stage_copy
            wall, cpu = time.perf_counter(), time.process_time()
            copied = operation(source, targets[label], expected)
            elapsed, cpu = time.perf_counter()-wall, time.process_time()-cpu
            if label == "candidate" and (copied is not False or payload_info(targets[label]) != candidate_inode):
                raise ValueError("unchanged staging unexpectedly rewrote the target")
            if stable_payload_hash(targets[label], payload_info(targets[label])) != expected:
                raise ValueError("target original byte oracle failed")
            if payload_info(source) != before:
                raise ValueError("captured original changed")
            result["samples"].append({"variant": label, "wall_seconds": elapsed, "cpu_seconds": cpu,
                "written_payload_bytes": before["size"] if label == "baseline" else 0})
            print(json.dumps(result["samples"][-1]), flush=True)
        if stable_payload_hash(source, before) != expected or any(sha256_file(Path(p)) != sha for p, sha in codes.items()):
            raise ValueError("source/code changed during benchmark")
        result.update(status="accepted_real_byte_staging_not_full_pipeline", rss_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                      load_average_after=os.getloadavg())
        atomic_write_json(output / "receipt.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-revision", default=BASELINE_REVISION)
    args = parser.parse_args()
    benchmark(args.source, args.output, baseline_revision=args.baseline_revision)
