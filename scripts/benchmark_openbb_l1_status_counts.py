"""Full L1 status publication/readback trials against one fixed private archive.

Every task, member and successful segment remains in scope. The candidate
changes only the member-count query. No source DB, source file, compactor,
provider, model, schedule or storage owner is mutated by this benchmark.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CONTRACT = "openbb_full_status_endpoint_seek_counts_abba_v2"
CODE = ("scripts/benchmark_openbb_l1_status_counts.py", "scripts/openbb_l1_status_counts.py",
        "scripts/compact_openbb_l1.py", "scripts/openbb_l1_view_schema.py",
        "scripts/openbb_l1_task_contract_index.py", "scripts/openbb_l1_source_journal.py",
        "stockagent/data/columnar_lake.py", "stockagent/data/source_processing_runtime.py",
        "scripts/benchmark_openbb_l1_source_contract.py", "downloader/artifact_io.py")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            h.update(block)
    return h.hexdigest()


def signature(path: Path) -> tuple[int, ...]:
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def prototype_source(source: str) -> str:
    start = source.index("    member_counts = {")
    end = source.index('    timings["status_member_count"]', start)
    replacement = (
        "    member_count_audit: dict[str, object] = {}\n"
        "    member_counts = member_endpoint_counts(\n"
        "        connection, member_filter, member_parameters, audit=member_count_audit,\n"
        "    )\n"
    )
    result = source[:start] + replacement + source[end:]
    marker = '        "stage_seconds": timings,\n'
    if result.count(marker) != 1:
        raise ValueError("canonical complete status shape changed")
    return result.replace(marker, marker + '        "member_count_audit": member_count_audit,\n')


def load_reference(path: Path):
    name = "_fixed_openbb_status_oracle"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def normalized(payload: dict) -> dict:
    # Only attempt clocks, durations, private publication roots and the new
    # observational policy differ. Every pre-existing business field remains.
    result = dict(payload)
    for key in ("stage_seconds", "member_count_audit"):
        result.pop(key, None)
    for key in ("query_database", "status_parquet"):
        result[key] = Path(result[key]).name
    return result


def run(fixture: Path, manifest: Path, *, warm_balanced: bool = False) -> None:
    from downloader.artifact_io import atomic_write_json
    from scripts.benchmark_openbb_l1_source_contract import native_budget
    from scripts.openbb_l1_status_counts import member_endpoint_counts
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime

    if fixture.exists() or fixture.is_symlink() or not fixture.resolve().is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("fresh private benchmark directory required")
    manifest = manifest.resolve(strict=True)
    if not manifest.is_relative_to(ROOT / "artifacts/benchmarks") or manifest.stat().st_mode & 0o222:
        raise ValueError("an existing read-only private complete archive is required")
    if shutil.disk_usage(fixture.parent).free < 32 * 1024**3:
        raise ValueError("32 GiB safety reserve required; no source copies or removals")
    configure_cpu_source_runtime(2)
    budget = native_budget()
    before = signature(manifest)
    fixed_sha = sha(manifest)
    fixture.mkdir(mode=0o700)
    reference = fixture / "original_compactor.py"
    shutil.copy2(ROOT / "scripts/compact_openbb_l1.py", reference)
    reference.chmod(0o400)
    code = {path: sha(ROOT / path) for path in CODE}
    closure = fixture / "code-closure"
    for path in CODE:
        preserved = closure / path
        preserved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, preserved)
        preserved.chmod(0o400)
        if sha(preserved) != code[path]:
            raise ValueError("complete implementation capture changed")
    oracle = load_reference(reference)
    source = inspect.getsource(oracle._write_status)
    namespace = oracle.__dict__.copy()
    namespace["member_endpoint_counts"] = member_endpoint_counts
    exec(compile(prototype_source(source), "<complete_status_member_count_candidate>", "exec"), namespace)
    functions = {"reference": oracle._write_status, "candidate": namespace["_write_status"]}
    # Both functions have this exact global dict: clocks are controlled but no
    # source-dependent value, SQL predicate, validation or publication is mocked.
    fixed_now = "2026-10-15T00:00:00+00:00"
    oracle._now = namespace["_now"] = lambda: fixed_now
    input_receipt = {"contract": CONTRACT, "observed_at_utc": datetime.now(UTC).isoformat(),
                     "manifest": str(manifest), "manifest_sha256": fixed_sha,
                     "manifest_signature": before, "code_sha256": code,
                     "reference_sha256": sha(reference), "budget": budget,
                     "threads": 2, "provider_calls": 0, "production_writes": False,
                     "warm_balanced_requested": warm_balanced}
    atomic_write_json(fixture / "fixture.json", input_receipt)
    warmup = None
    if warm_balanced:
        with sqlite3.connect(manifest.as_uri() + "?mode=ro", uri=True) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            started, cpu = time.perf_counter(), time.process_time()
            payload = functions["reference"](connection, fixture / "warmup", (), stale_segments=0,
                                             new_segments=0, deferred_query_views={}, started_at_utc=fixed_now)
            warmup = {"wall_seconds": time.perf_counter() - started,
                      "cpu_seconds": time.process_time() - cpu, "stage_seconds": payload["stage_seconds"]}
            connection.rollback()
        atomic_write_json(fixture / "warmup.json", warmup)
    trials = []
    oracle_digest = None
    sequence = ("reference", "candidate", "candidate", "reference")
    if warm_balanced:
        sequence += ("candidate", "reference", "reference", "candidate")
    for index, variant in enumerate(sequence):
        if before != signature(manifest) or code != {p: sha(ROOT / p) for p in CODE}:
            raise ValueError("fixed source/code generation changed")
        output = fixture / f"case-{index}-{variant}"
        with sqlite3.connect(manifest.as_uri() + "?mode=ro", uri=True) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            start, cpu = time.perf_counter(), time.process_time()
            payload = functions[variant](connection, output, (), stale_segments=0,
                                         new_segments=0, deferred_query_views={},
                                         started_at_utc=fixed_now)
            # Include fresh full Parquet and JSON readback in complete timing.
            import polars as pl
            frame = pl.read_parquet(output / "catalog/l1_compaction_status.parquet")
            stored = json.loads((output / "_state/l1_compaction_latest.json").read_bytes())
            if payload != stored or frame.to_dicts() != payload["endpoint_status"]:
                raise ValueError("complete durable publication readback mismatch")
            complete = {"payload": normalized(payload), "frame": frame.to_dicts(),
                        "parquet_sha256": sha(output / "catalog/l1_compaction_status.parquet")}
            digest = hashlib.sha256(json.dumps(complete, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            elapsed, cpu = time.perf_counter() - start, time.process_time() - cpu
            connection.rollback()
        if oracle_digest is None:
            oracle_digest = digest
        if digest != oracle_digest:
            raise ValueError("complete status publication/parquet exact output parity failed")
        trial = {"variant": variant, "wall_seconds": elapsed, "cpu_seconds": cpu,
                 "full_output_sha256": digest, "stage_seconds": payload["stage_seconds"],
                 "success_files": payload["success_files"], "compacted_files": payload["compacted_files"],
                 "success_rows": payload["success_rows"], "compacted_rows": payload["compacted_rows"],
                 "active_segments": payload["active_segments"], "endpoints": payload["endpoints"],
                 "member_count_audit": payload.get("member_count_audit")}
        if variant == "candidate" and trial["member_count_audit"]["path"] != "endpoint_index_seeks":
            raise ValueError("candidate did not actually execute the endpoint seek query")
        trials.append(trial)
        atomic_write_json(fixture / f"trial-{index}-{variant}.json", trial)
        print(json.dumps(trial), flush=True)
    if before != signature(manifest) or fixed_sha != sha(manifest) or code != {p: sha(ROOT / p) for p in CODE}:
        raise ValueError("fixed entire archive/code changed during the complete trials")
    means = {variant: {key: sum(t[key] for t in trials if t["variant"] == variant) / sequence.count(variant)
                       for key in ("wall_seconds", "cpu_seconds")}
             for variant in functions}
    reduction = {key: 100 * (1 - means["candidate"][key] / means["reference"][key]) for key in means["reference"]}
    result = {"contract": CONTRACT, "observed_at_utc": datetime.now(UTC).isoformat(),
              "state": "accepted_complete_status_output_parity", "trials": trials,
              "means": means, "reductions_percent": reduction, "input": input_receipt,
              "warmup": warmup, "trial_sequence": sequence,
              "full_archive_sha_unchanged": True, "provider_calls": 0, "production_writes": False,
              "os_page_cache_controlled": False, "whole_compaction_speed_verified": False,
              "scope": "complete status publication/readback; not footer/source-contract/compaction or payload audit"}
    atomic_write_json(fixture / "comparison.json", result)
    print(json.dumps({"state": result["state"], "reductions_percent": reduction}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--warm-balanced", action="store_true", help="separate full warmup, then ABBA and BAAB")
    args = parser.parse_args()
    run(args.fixture.absolute(), args.manifest, warm_balanced=args.warm_balanced)


if __name__ == "__main__":
    main()
