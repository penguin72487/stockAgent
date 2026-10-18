"""Bounded real-block A/B/B/A of the original factorized persistence workflow.

Capture once under prepare.lock; retry that same immutable input. Both variants
write an initial generation, update, verify every byte and decode/read back all
selected values/coordinates. This excludes source normalization, the causal
sweep and the 217 MB dictionary; it is not a full preparation benchmark.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl
import pyarrow as pa

from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import (
    VALUE_ONLY_CONTRACT, _BlockCache, file_sha256, verify_factorized_members, write_array_block,
)
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.data_sync.benchmark_fixtures import ImmutableFixtureInputs
from stockagent.data_sync.factorized_projection_reuse import (
    FactorizedProjectionReuse, implementation_identity, seal_new_file,
)
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
from stockagent.data_sync.physical_cache_reuse import _metadata
from scripts.benchmark_project_data_processing import load_baseline

CONTRACT = "factorized_bounded_persistence_reuse_benchmark_v1"
MAX_CAPTURE_BYTES = 32 * 1024**2
MAX_SERIALIZED_BYTES = 128 * 1024**2


def capture(old_path, new_path, output, members):
    old_path, new_path = (path.resolve(strict=True) for path in (old_path, new_path))
    if old_path.parent.parent != new_path.parent.parent or old_path.parent == new_path.parent:
        raise ValueError("select two distinct generations of the same original owner")
    lock = old_path.parent.parent.parent / "prepare.lock"
    with lock.open("rb") as lease:
        fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
        old, _ = _metadata(old_path)
        new, _ = _metadata(new_path)
        if (old.get("contract") != VALUE_ONLY_CONTRACT or new.get("contract") != VALUE_ONLY_CONTRACT
                or old["symbols"] != new["symbols"] or old["dates"] != new["dates"]
                or old["feature_dictionary_sha256"] != new["feature_dictionary_sha256"]):
            raise ValueError("bounded comparison requires matching original axes/dictionary")
        previous = {p["path"]: p for b in old["blocks"] for p in b["symbol_blocks"]}
        pairs = [(previous[p["path"]], p) for b in new["blocks"] for p in b["symbol_blocks"]
                 if p["path"] in previous and previous[p["path"]]["shape"] == p["shape"]]
        unchanged = [pair for pair in pairs if pair[0]["sha256"] == pair[1]["sha256"]]
        changed = [pair for pair in pairs if pair[0]["sha256"] != pair[1]["sha256"]]
        if len(unchanged) < members - 1 or not changed:
            raise ValueError("need both unchanged real history and a real changed tail")
        selected = unchanged[:members - 1] + changed[:1]
        if sum(p["serialized_bytes"] for pair in selected for p in pair) > MAX_SERIALIZED_BYTES:
            raise ValueError("selected compact arrays exceed bounded serialization budget")
        output.mkdir(parents=True, mode=0o700)
        captured, source_proofs, variants = {}, [], {}
        with ImmutableFixtureInputs(output) as inputs:
            def member(root, proof, suffix):
                source = root / proof["path"]
                if source.resolve(strict=True) != source.absolute():
                    raise ValueError("source member is redirected")
                before = payload_info(source)
                digest = stable_payload_hash(source, before)
                if digest != proof["sha256"] or before["size"] != proof["bytes"]:
                    raise ValueError("selected original source member changed")
                if digest not in captured:
                    if sum(p["size"] for p in captured.values()) + before["size"] > MAX_CAPTURE_BYTES:
                        raise ValueError("selected capture exceeds bounded byte budget")
                    captured[digest] = inputs.copy(source, f"input-{digest}{suffix}", expected_sha256=digest)
                source_proofs.append({"path": str(source), "sha256": digest, "metadata": before})
                return {**proof, "path": captured[digest]["name"]}
            for index, (label, manifest, path) in enumerate((("old", old, old_path), ("new", new, new_path))):
                variants[label] = {"blocks": [member(path.parent, pair[index], ".zst") for pair in selected],
                                   "common": member(path.parent, manifest["common"], ".npy")}
            for proof in source_proofs:
                if payload_info(Path(proof["path"])) != proof["metadata"]:
                    raise ValueError("source changed during bounded capture")
            input_receipt = inputs.commit()
        metadata = {"contract": CONTRACT, "variants": variants, "source_proofs": source_proofs,
                    "immutable_inputs": input_receipt, "unchanged_blocks": members - 1,
                    "changed_blocks": 1, "dictionary_captured": False,
                    "source_manifests": {str(p): file_sha256(p) for p in (old_path, new_path)}}
        atomic_write_json(output / "fixture.json", metadata)
        seal_new_file(output / "fixture.json")
    return metadata


def verify_fixture(output):
    metadata, _ = _metadata(output / "fixture.json")
    if metadata["contract"] != CONTRACT or metadata["dictionary_captured"] is not False:
        raise ValueError("wrong bounded persistence fixture")
    files = metadata["immutable_inputs"]["files"]
    if len(files) > 34 or sum(p["size"] for p in files) > MAX_CAPTURE_BYTES:
        raise ValueError("fixture exceeds its capture budget")
    for proof in files:
        name = proof["name"]
        if Path(name).name != name or name in {"", ".", ".."}:
            raise ValueError("fixture member escaped its private root")
        info = payload_info(output / name)
        if info["mode"] & 0o222 or info["size"] != proof["size"] or stable_payload_hash(output / name, info) != proof["sha256"]:
            raise ValueError("immutable bounded fixture changed")
    allowed = {proof["name"]: proof for proof in files}
    serialized = 0
    for source in metadata["variants"].values():
        if not 2 <= len(source["blocks"]) <= 16:
            raise ValueError("fixture block count exceeds its bound")
        for proof in [source["common"], *source["blocks"]]:
            indexed = allowed.get(proof["path"])
            if indexed is None or indexed["sha256"] != proof["sha256"] or indexed["size"] != proof["bytes"]:
                raise ValueError("fixture references an uncaptured input")
            serialized += proof.get("serialized_bytes", 0)
    if serialized > MAX_SERIALIZED_BYTES:
        raise ValueError("fixture compact arrays exceed its serialization budget")
    return metadata


def persist(fixture, root, source, variant, donor=None, *, producer_factory=FactorizedProjectionReuse,
            resume_generation=None, complete=True):
    started, cpu = time.perf_counter(), time.process_time()
    root.mkdir(parents=True, mode=0o700)
    reuse = (producer_factory(root, donor, **({"resume_generation": resume_generation}
                                            if resume_generation is not None else {}))
             if variant == "reuse" else None)
    expected, parts = {}, []
    for index, proof in enumerate(source["blocks"]):
        block = _BlockCache(fixture, [proof], MAX_SERIALIZED_BYTES).get(0)
        name = f"block-{index}.zst"
        expected[name] = {"values": array_content_fingerprint(block.values),
                          "columns": array_content_fingerprint(block.columns), "shape": proof["shape"]}
        kwargs = {"logical_columns": block.columns, "logical_shape": tuple(proof["shape"])}
        part = (reuse.block(name, block.values, **kwargs) if reuse else
                write_array_block(root / name, block.values, omit_zero_columns=True, **kwargs))
        if part["sha256"] != proof["sha256"]:
            raise ValueError("reencoded complete payload differs from original")
        parts.append(part)
    shared = np.load(fixture / source["common"]["path"], allow_pickle=False)
    expected["common"] = array_content_fingerprint(shared)
    if reuse:
        common = reuse.common(shared)
        storage = reuse.stats()
    else:
        np.save(root / "common.npy", shared, allow_pickle=False)
        common = {"path": "common.npy", "shape": list(shared.shape),
                  "sha256": file_sha256(root / "common.npy"), "bytes": (root / "common.npy").stat().st_size}
        storage = {"written_payload_bytes": sum(p["bytes"] for p in parts) + common["bytes"],
                   "avoided_payload_bytes": 0, "reused_payloads": 0}
    manifest = {"contract": VALUE_ONLY_CONTRACT, "common": common,
                "blocks": [{"symbol_blocks": parts}], "storage_build": storage,
                "paper_inference": {"status": "verified"}}
    verify_factorized_members(root, manifest)
    manifest_path = root / "bounded_manifest.json"
    if complete:
        atomic_write_json(manifest_path, manifest)
        if reuse:
            seal_new_file(manifest_path)
    actual = {"common": array_content_fingerprint(np.load(root / "common.npy", allow_pickle=False))}
    for proof in parts:
        value = _BlockCache(root, [proof], MAX_SERIALIZED_BYTES).get(0)
        actual[proof["path"]] = {"values": array_content_fingerprint(value.values),
                                 "columns": array_content_fingerprint(value.columns), "shape": proof["shape"]}
    if actual != expected:
        raise ValueError("complete selected values/coordinates differ after persistence")
    return {"wall_seconds": time.perf_counter() - started, "cpu_seconds": time.process_time() - cpu,
            "storage_build": storage, "complete_arrays": actual}


def worker(output, trial, variant, threads):
    if (not 1 <= threads <= 4 or trial.absolute() != trial.resolve()
            or not trial.is_relative_to(output) or trial.exists()):
        raise ValueError("worker requires bounded pools and a new private trial below its fixture")
    pa.set_cpu_count(threads)
    metadata = verify_fixture(output)
    initial = persist(output, trial / "generations/initial", metadata["variants"]["old"], variant)
    update = persist(output, trial / "generations/update", metadata["variants"]["new"], variant,
                     trial / "generations/initial/bounded_manifest.json")
    return {"state": "completed_trial", "variant": variant, "initial": initial, "update": update,
            "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "polars_threads": pl.thread_pool_size(), "arrow_threads": pa.cpu_count()}


def benchmark(old_path, new_path, output, *, threads=2, members=5, reuse_fixture=False):
    if (not 1 <= threads <= 4 or not 2 <= members <= 16 or output.absolute() != output.resolve()
            or not output.is_relative_to(ROOT / "artifacts/benchmarks") or output.exists() != reuse_fixture):
        raise ValueError("bounded pools/members and a fresh real benchmark root (or explicit fixed-input retry) required")
    metadata = (verify_fixture(output) if reuse_fixture else capture(old_path, new_path, output, members))
    paths = [Path(__file__), ROOT / "scripts/prepare_tw_day_trade_factorized_live.py",
             output / "fixture.json", *map(Path, implementation_identity())]
    code = {str(p): file_sha256(p) for p in paths}
    attempt = output / f"attempt-{uuid.uuid4().hex}"
    attempt.mkdir(mode=0o700)
    trials = []
    for index, variant in enumerate(("baseline", "reuse", "reuse", "baseline")):
        child = subprocess.run([sys.executable, str(Path(__file__)), "--output", str(output), "--worker", variant,
                                "--trial", str(attempt / f"trial-{index}"), "--threads", str(threads)],
                               capture_output=True, text=True, timeout=120, check=False)
        if child.returncode:
            atomic_write_json(attempt / f"{index}-failed.json", {"exit_code": child.returncode, "stderr": child.stderr})
            raise RuntimeError(f"bounded worker failed: {child.stderr[-4000:]}")
        row = json.loads(child.stdout.splitlines()[-1])
        if row["polars_threads"] != threads or row["arrow_threads"] != threads:
            raise ValueError("actual worker pools differ from selected budget")
        trials.append(row)
        atomic_write_json(attempt / f"{index}-{variant}.json", row)
    if code != {str(p): file_sha256(p) for p in paths}:
        raise ValueError("implementation/fixed input changed during benchmark")
    verify_fixture(output)
    for phase in ("initial", "update"):
        if any(row[phase]["complete_arrays"] != trials[0][phase]["complete_arrays"] for row in trials):
            raise ValueError("complete phase values differ between workers")
    result = {"contract": CONTRACT, "state": "accepted_bounded_factorized_persistence",
              "observed_at_utc": datetime.now(UTC).isoformat(), "fixture": metadata,
              "input_and_code_sha256": code, "trials": trials, "reused_fixed_fixture": reuse_fixture,
              "means": {variant: {phase: {metric: statistics.mean(row[phase][metric] for row in trials if row["variant"] == variant)
                                         for metric in ("wall_seconds", "cpu_seconds")}
                                  for phase in ("initial", "update")} for variant in ("baseline", "reuse")},
              "scope": "bounded block/common persistence, full SHA and complete compact-array readback; not full normalization/sweep/dictionary",
              "production_writes": 0, "provider_calls": 0, "gpu_calls": 0}
    atomic_write_json(attempt / "abba-receipt.json", result)
    if not (output / "abba-receipt.json").exists():
        atomic_write_json(output / "abba-receipt.json", result)
    return result


def resume_worker(output, trial, variant, threads, baseline_helper):
    """Simulate interruption before a manifest; a fresh producer retries once."""
    if (not 1 <= threads <= 4 or trial.absolute() != trial.resolve()
            or not trial.is_relative_to(output) or trial.exists()):
        raise ValueError("resume worker needs a new private trial and bounded pools")
    pa.set_cpu_count(threads)
    metadata = verify_fixture(output)
    baseline = load_baseline(baseline_helper)
    factory = baseline.FactorizedProjectionReuse if variant == "baseline" else FactorizedProjectionReuse
    generation = file_sha256(output / "fixture.json")
    roots = [trial / "generations" / f".{generation}.attempt.{uuid.uuid4().hex}" for _ in range(2)]
    kwargs = {"producer_factory": factory,
              **({"resume_generation": generation} if variant == "resume" else {})}
    interrupted = persist(output, roots[0], metadata["variants"]["new"], "reuse", complete=False, **kwargs)
    if (roots[0] / "bounded_manifest.json").exists():
        raise ValueError("interrupted attempt unexpectedly committed a complete manifest")
    retry = persist(output, roots[1], metadata["variants"]["new"], "reuse", **kwargs)
    if interrupted["complete_arrays"] != retry["complete_arrays"]:
        raise ValueError("retry changed complete original values/coordinates")
    all_inodes, payload_inodes = {}, {}
    for path in trial.rglob("*"):
        info = path.lstat()
        all_inodes[(info.st_dev, info.st_ino)] = info.st_blocks * 512
        if path.suffix in {".zst", ".npy"}:
            payload_inodes[(info.st_dev, info.st_ino)] = info.st_blocks * 512
    return {"variant": variant, "interrupted": interrupted, "retry": retry,
            "total_wall_seconds": interrupted["wall_seconds"] + retry["wall_seconds"],
            "total_cpu_seconds": interrupted["cpu_seconds"] + retry["cpu_seconds"],
            "unique_allocated_bytes": sum(all_inodes.values()),
            "unique_payload_allocated_bytes": sum(payload_inodes.values()),
            "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "polars_threads": pl.thread_pool_size(), "arrow_threads": pa.cpu_count()}


def benchmark_resume(output, baseline_helper, *, threads=2):
    """Reuse a captured fixture; never create another raw-data benchmark copy."""
    if (not 1 <= threads <= 4 or output.absolute() != output.resolve(strict=True)
            or not output.is_relative_to(ROOT / "artifacts/benchmarks")):
        raise ValueError("resume benchmark needs an existing private fixed fixture")
    baseline_helper = baseline_helper.absolute()
    if (baseline_helper.resolve(strict=True) != baseline_helper
            or not baseline_helper.is_relative_to(ROOT / "artifacts/operations")):
        raise ValueError("explicit saved original helper below operations is required")
    metadata = verify_fixture(output)
    paths = [Path(__file__), baseline_helper, output / "fixture.json", *map(Path, implementation_identity())]
    code = {str(p): file_sha256(p) for p in paths}
    attempt = output / f"resume-attempt-{uuid.uuid4().hex}"
    attempt.mkdir(mode=0o700)
    trials = []
    for index, variant in enumerate(("baseline", "resume", "resume", "baseline")):
        child = subprocess.run([sys.executable, str(Path(__file__)), "--output", str(output),
                                "--resume-worker", variant, "--trial", str(attempt / f"trial-{index}"),
                                "--threads", str(threads), "--baseline-helper", str(baseline_helper)],
                               capture_output=True, text=True, timeout=120, check=False)
        if child.returncode:
            atomic_write_json(attempt / f"{index}-failed.json", {"exit_code": child.returncode, "stderr": child.stderr})
            raise RuntimeError(f"bounded retry worker failed: {child.stderr[-4000:]}")
        row = json.loads(child.stdout.splitlines()[-1])
        if row["polars_threads"] != threads or row["arrow_threads"] != threads:
            raise ValueError("actual retry worker pools differ from selected budget")
        trials.append(row)
        atomic_write_json(attempt / f"{index}-{variant}.json", row)
    if code != {str(p): file_sha256(p) for p in paths}:
        raise ValueError("implementation/fixed input changed during retry benchmark")
    verify_fixture(output)
    if any(row["retry"]["complete_arrays"] != trials[0]["retry"]["complete_arrays"] for row in trials):
        raise ValueError("complete retry values differ between independent workers")
    metrics = ("total_wall_seconds", "total_cpu_seconds", "unique_allocated_bytes",
               "unique_payload_allocated_bytes", "rss_peak_bytes")
    result = {"contract": "factorized_bounded_attempt_resume_benchmark_v1",
              "state": "accepted_bounded_factorized_attempt_resume",
              "observed_at_utc": datetime.now(UTC).isoformat(), "input_and_code_sha256": code,
              "fixture": metadata, "trials": trials,
              "means": {variant: {metric: statistics.mean(row[metric] for row in trials if row["variant"] == variant)
                                  for metric in metrics} for variant in ("baseline", "resume")},
              "scope": "same captured real blocks/common, interrupted persistence plus retry, complete SHA and array readback; excludes normalization/sweep/dictionary",
              "production_writes": 0, "provider_calls": 0, "gpu_calls": 0,
              "new_source_fixture_bytes": 0}
    atomic_write_json(attempt / "abba-receipt.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-manifest", type=Path)
    parser.add_argument("--new-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--members", type=int, default=5)
    parser.add_argument("--reuse-fixture", action="store_true")
    parser.add_argument("--resume-attempts", action="store_true",
                        help="benchmark interrupted attempt and retry on the existing fixed fixture")
    parser.add_argument("--baseline-helper", type=Path,
                        help="explicit saved original producer for the retry comparison")
    parser.add_argument("--resume-worker", choices=("baseline", "resume"), help=argparse.SUPPRESS)
    parser.add_argument("--worker", choices=("baseline", "reuse"), help=argparse.SUPPRESS)
    parser.add_argument("--trial", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.resume_worker or args.resume_attempts:
        if args.baseline_helper is None or args.worker:
            parser.error("attempt resume requires --baseline-helper and no ordinary worker")
        result = (resume_worker(args.output, args.trial, args.resume_worker, args.threads, args.baseline_helper)
                  if args.resume_worker else benchmark_resume(args.output, args.baseline_helper, threads=args.threads))
    elif args.worker:
        result = worker(args.output, args.trial, args.worker, args.threads)
    else:
        if not args.reuse_fixture and (args.old_manifest is None or args.new_manifest is None):
            parser.error("first capture requires old/new original manifests")
        result = benchmark(args.old_manifest, args.new_manifest, args.output, threads=args.threads,
                           members=args.members, reuse_fixture=args.reuse_fixture)
    print(json.dumps(result if args.worker or args.resume_worker else {"state": result["state"], "means": result["means"]}))


if __name__ == "__main__":
    main()
