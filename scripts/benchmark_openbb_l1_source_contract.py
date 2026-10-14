#!/usr/bin/env python3
"""Full canonical source-contract SQL trials on an immutable private backup.

Only the lookup index changes. The original CASE, NULL semantics, DISTINCT,
ordering and path normalizer are reused from the actual compactor. This is not
a derivative/payload audit, compaction run, or production index migration.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CONTRACT = "openbb_full_source_contract_covering_index_v1"
INDEX = "bench_l1_source_task_contract"
COLUMNS = ("task_id", "active", "status", "endpoint", "output_path", "rows", "updated_at")
RESERVE = 30 * 1024**3
CODE_PATHS = (
    "scripts/benchmark_openbb_l1_source_contract.py",
    "scripts/benchmark_openbb_l1_stale_scan.py",
    "scripts/compact_openbb_l1.py",
    "scripts/openbb_l1_view_schema.py",
    "scripts/openbb_l1_source_journal.py",
    "stockagent/data/columnar_lake.py",
    "stockagent/data/source_processing_runtime.py",
    "downloader/artifact_io.py",
)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()


def implementation() -> dict:
    import pyarrow as pa
    return {"code": {name: digest(ROOT / name) for name in CODE_PATHS},
            "python": sys.version, "sqlite": sqlite3.sqlite_version,
            "arrow": pa.__version__, "executable_sha256": digest(Path(sys.executable).resolve())}


def fresh(path: Path, *, root: Path | None = None) -> None:
    if path.exists() or path.is_symlink():
        raise ValueError("preserve earlier fixture/result; choose a new path")
    if not path.resolve().is_relative_to(root or ROOT / "artifacts/benchmarks"):
        raise ValueError("private benchmark path required")


def readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def native_budget() -> dict:
    """Restrict only this engineering transient, never a production owner."""
    invocation = os.environ.get("INVOCATION_ID")
    groups = [line.split("::", 1)[1] for line in Path("/proc/self/cgroup").read_text().splitlines()
              if line.startswith("0::")]
    if len(groups) != 1:
        raise RuntimeError("one canonical systemd engineering cgroup required")
    unit = Path(groups[0]).name
    if not invocation or not unit.startswith("stockagent-agent-") or not unit.endswith(".service"):
        raise RuntimeError("use stockagent-agent run; refusing to alter another owner")
    observed = subprocess.run(["systemctl", "show", unit, "-p", "InvocationID", "--value"],
                              check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    if observed != invocation:
        raise RuntimeError("engineering owner identity changed")
    subprocess.run(["systemctl", "set-property", "--runtime", unit,
                    "MemoryHigh=2816M", "MemoryMax=3G", "CPUWeight=10", "IOWeight=10"],
                   check=True, capture_output=True, text=True, timeout=10)
    os.setpriority(os.PRIO_PROCESS, 0, 15)
    group = Path("/sys/fs/cgroup") / groups[0].lstrip("/")
    caps = {name: int((group / name).read_text()) for name in ("memory.high", "memory.max")}
    if caps != {"memory.high": 2816 * 1024**2, "memory.max": 3 * 1024**3}:
        raise RuntimeError("actual engineering memory budgets do not match the native owner")
    return {"unit": unit, "invocation_id": invocation, **caps,
            "nice": os.getpriority(os.PRIO_PROCESS, 0)}


def query_for(index: str) -> str:
    from scripts.compact_openbb_l1 import _stale_source_contract_sql
    if index not in {"sqlite_autoindex_tasks_1", INDEX}:
        raise ValueError("only the measured exact lookup indexes are allowed")
    sql = _stale_source_contract_sql("", member_first=True)
    old = "LEFT JOIN tasks AS t ON t.task_id=m.task_id"
    if sql.count(old) != 1:
        raise RuntimeError("canonical query shape changed; do not silently rewrite its predicates")
    return sql.replace(old, f"LEFT JOIN tasks AS t INDEXED BY {index} ON t.task_id=m.task_id")


def create_index(connection: sqlite3.Connection) -> None:
    from scripts.openbb_l1_source_journal import TRACKED_TASK_COLUMNS
    if set(COLUMNS) != TRACKED_TASK_COLUMNS:
        raise RuntimeError("source-contract columns changed; index needs a reviewed new version")
    connection.execute(f"CREATE INDEX {INDEX} ON tasks({','.join(COLUMNS)})")


def counts(connection: sqlite3.Connection) -> dict:
    return {name: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for name, table in (("tasks", "tasks"), ("members", "l1_compaction_members"),
                                ("segments", "l1_compaction_segments"))}


def capture(root: Path, source: Path) -> None:
    from downloader.artifact_io import atomic_write_json
    fresh(root)
    if shutil.disk_usage(root.parent).free < 2 * source.stat().st_size + RESERVE:
        raise ValueError("insufficient capacity for retained backup, index/sort and 30 GiB reserve")
    budget, identity = native_budget(), implementation()
    root.mkdir(mode=0o700)
    target = root / "manifest.sqlite3"
    started, cpu_started = time.perf_counter(), time.process_time()
    last_progress = [started]

    def progress(status: int, remaining: int, total: int) -> None:
        now = time.perf_counter()
        if now - last_progress[0] >= 20:
            if shutil.disk_usage(root).free < RESERVE:
                raise RuntimeError("private backup stopped at the physical reserve")
            print(json.dumps({"phase": "private_backup", "remaining_pages": remaining,
                              "total_pages": total}), flush=True)
            last_progress[0] = now

    with readonly(source) as original, sqlite3.connect(target) as private:
        original.backup(private, pages=1024, progress=progress, sleep=0.02)
    backup_seconds = time.perf_counter() - started
    baseline_sha = digest(target)
    with sqlite3.connect(target) as private:
        private.execute("PRAGMA cache_size=-262144")
        private.execute("PRAGMA temp_store=FILE")
        private.execute("PRAGMA threads=2")
        before = counts(private)
        original_size = target.stat().st_size
        index_started, index_cpu = time.perf_counter(), time.process_time()
        print(json.dumps({"phase": "private_index_build", "counts": before}), flush=True)
        create_index(private)
        private.commit()
        build = {"wall_seconds": time.perf_counter() - index_started,
                 "cpu_seconds": time.process_time() - index_cpu,
                 "database_growth_bytes": target.stat().st_size - original_size}
        if counts(private) != before:
            raise RuntimeError("private index changed logical source/member/segment counts")
        if shutil.disk_usage(root).free < RESERVE:
            raise RuntimeError("private index exceeded the physical reserve")
    if implementation() != identity:
        raise RuntimeError("capture implementation changed")
    target.chmod(0o400)
    closure = root / "code-closure"
    closure.mkdir(mode=0o700)
    for name, expected in identity["code"].items():
        frozen = closure / name
        frozen.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, frozen)
        if digest(frozen) != expected:
            raise RuntimeError("code closure changed while freezing")
        frozen.chmod(0o400)
    receipt = {"contract": CONTRACT, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
               "implementation": identity, "manifest_sha256": digest(target),
               "baseline_backup_sha256": baseline_sha, "counts": before,
               "manifest_bytes": target.stat().st_size, "index_columns": COLUMNS,
               "index_build": build, "backup_wall_seconds": backup_seconds,
               "capture_wall_seconds": time.perf_counter() - started,
               "capture_cpu_seconds": time.process_time() - cpu_started, "budget": budget,
               "production_writes": False, "provider_calls": 0}
    atomic_write_json(root / "fixture.json", receipt)
    print(json.dumps({"phase": "captured", "counts": before, "index_build": build}), flush=True)


def worker(root: Path, output: Path, variant: str) -> None:
    from downloader.artifact_io import atomic_write_json
    from scripts.compact_openbb_l1 import _lexical_absolute_path, _stage_resources
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    fresh(output, root=root)
    budget = native_budget()
    configure_cpu_source_runtime(2)
    fixture = json.loads((root / "fixture.json").read_bytes())
    manifest = root / "manifest.sqlite3"
    workflow_started = time.perf_counter()
    if (fixture.get("contract") != CONTRACT or implementation() != fixture["implementation"]
            or digest(manifest) != fixture["manifest_sha256"]):
        raise RuntimeError("complete private source/runtime/code identity mismatch")
    index = "sqlite_autoindex_tasks_1" if variant == "original" else INDEX
    query = query_for(index)
    hashes = []
    with readonly(manifest) as connection:
        # Match the actual owner's default 2,000 KiB SQLite page cache. A
        # larger page cache is a different candidate, not part of this index A/B.
        connection.execute("PRAGMA cache_size=-2000")
        connection.execute("PRAGMA temp_store=FILE")
        connection.create_function("stockagent_resolve_path", 1, _lexical_absolute_path, deterministic=True)
        connection.execute("BEGIN")
        params = (str(ROOT) + os.sep,) * 2
        plan = [str(row[3]) for row in connection.execute("EXPLAIN QUERY PLAN " + query, params)]
        print(json.dumps({"phase": "full_source_query", "variant": variant, "plan": plan}), flush=True)
        before = _stage_resources()
        started, cpu_started = time.perf_counter(), time.process_time()
        for row in connection.execute(query, params):
            hashes.append(hashlib.sha256(json.dumps(tuple(row), ensure_ascii=False,
                                                   separators=(",", ":")).encode()).digest())
        wall, cpu = time.perf_counter() - started, time.process_time() - cpu_started
        after = _stage_resources()
    result_digest = hashlib.sha256(b"".join(sorted(hashes))).hexdigest()
    if (implementation() != fixture["implementation"] or digest(manifest) != fixture["manifest_sha256"]
            or "torch" in sys.modules):
        raise RuntimeError("private source/code changed or model runtime imported")
    result = {"contract": CONTRACT, "state": "completed_trial", "variant": variant,
              "wall_seconds": wall, "cpu_seconds": cpu, "query_plan": plan,
              "stale_rows": len(hashes), "stale_rows_sha256": result_digest,
              "resources_before": before, "resources_after": after, "budget": budget,
              "sqlite_cache_kib": 2000,
              "workflow_wall_seconds": time.perf_counter() - workflow_started,
              "manifest_sha256": fixture["manifest_sha256"], "implementation": fixture["implementation"],
              "production_writes": False, "provider_calls": 0, "model_runtime_imported": False,
              "scope": "complete source-contract SQL, not derivative/payload audit or compaction"}
    atomic_write_json(output, result)
    print(json.dumps({"state": result["state"], "variant": variant, "wall_seconds": wall,
                      "cpu_seconds": cpu, "stale_rows": len(hashes)}), flush=True)


def aggregate(root: Path, output: Path) -> None:
    from downloader.artifact_io import atomic_write_json
    fresh(output, root=root)
    fixture = json.loads((root / "fixture.json").read_bytes())
    variants = ("original", "covering", "covering", "original")
    trials = [json.loads((root / f"{n:02d}-{v}.json").read_bytes())
              for n, v in enumerate(variants, 1)]
    for row, variant in zip(trials, variants, strict=True):
        if (row.get("contract") != CONTRACT or row.get("state") != "completed_trial"
                or row.get("variant") != variant or row.get("manifest_sha256") != fixture["manifest_sha256"]
                or row.get("implementation") != fixture["implementation"]
                or row.get("production_writes") is not False or row.get("provider_calls") != 0
                or row.get("model_runtime_imported") is not False):
            raise ValueError("complete fixed-source/runtime ABBA receipts required")
    if len({(row["stale_rows"], row["stale_rows_sha256"]) for row in trials}) != 1:
        raise ValueError("canonical full-source output parity failed")
    means = {variant: {name: sum(row[name] for row in trials if row["variant"] == variant) / 2
                       for name in ("wall_seconds", "cpu_seconds", "workflow_wall_seconds")}
             for variant in ("original", "covering")}
    result = {"contract": CONTRACT, "state": "accepted_bounded_phase_comparison",
              "trials": trials, "means": means, "fixture": fixture,
              "reductions_percent": {name: 100 * (1 - means["covering"][name] / means["original"][name])
                                     for name in means["original"]},
              "exact_canonical_complete_output_parity": True, "os_page_cache_controlled": False,
              "exclusive_host": False, "production_index_installed": False,
              "production_writes": False, "provider_calls": 0,
              "scope": "complete full-source SQL; index build/storage recorded, writer overhead and native adoption unverified"}
    atomic_write_json(output, result)
    print(json.dumps({"state": result["state"], "reductions_percent": result["reductions_percent"]}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--capture", action="store_true")
    mode.add_argument("--worker", choices=("original", "covering"))
    mode.add_argument("--aggregate", action="store_true")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data_openBB/_state/openbb_archive.sqlite3")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.capture:
        capture(args.fixture.absolute(), args.manifest.resolve(strict=True))
    elif args.output is None:
        parser.error("worker/aggregate requires a new output under the fixture")
    elif args.worker:
        worker(args.fixture.resolve(strict=True), args.output.absolute(), args.worker)
    else:
        aggregate(args.fixture.resolve(strict=True), args.output.absolute())


if __name__ == "__main__":
    main()
