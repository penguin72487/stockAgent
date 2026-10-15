#!/usr/bin/env python3
"""Replay native task persistence on retained full-size private manifests.

The bounded workload uses existing successful L1 task records, not downloads.
Only the covering index differs. No provider, payload, production database or
checkpoint is written. Preparation, durable commits, checkpoint and readback
costs are recorded separately; the source snapshot remains hash-verified.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader import download_openbb_archive as archive  # noqa: E402
from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts import benchmark_openbb_l1_source_contract as source  # noqa: E402
from scripts.compact_openbb_l1 import _stage_resources  # noqa: E402
from scripts.openbb_l1_source_journal import (  # noqa: E402
    JOURNAL_TABLE, _canonical_sql, _TRIGGERS,
)

CONTRACT = "openbb_full_size_native_task_persistence_index_v1"
BASE_TIME = datetime(2026, 10, 14, 1, 0, tzinfo=timezone.utc)
VARIANTS = ("original", "covering", "covering", "original")


def identity() -> dict:
    result = source.implementation()
    names = set(result["code"])
    names.add("scripts/benchmark_openbb_l1_source_write_cost.py")
    for module in tuple(sys.modules.values()):
        filename = getattr(module, "__file__", None)
        if not filename:
            continue
        path = Path(filename).resolve()
        if path.suffix == ".py" and path.is_relative_to(ROOT):
            name = path.relative_to(ROOT).as_posix()
            if name.startswith(("scripts/", "stockagent/", "downloader/")):
                names.add(name)
    result["code"] = {name: source.digest(ROOT / name) for name in sorted(names)}
    return result


def json_digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":")).encode()).hexdigest()


def model_runtime_imported() -> bool:
    return "torch" in sys.modules


def verify_journal(connection: sqlite3.Connection) -> None:
    for name, expected in _TRIGGERS.items():
        row = connection.execute("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=?",
                                 (name,)).fetchone()
        if row is None or _canonical_sql(str(row[0])) != _canonical_sql(expected):
            raise ValueError("complete native mutation journal required; no silent installation")


def task_records(connection: sqlite3.Connection, count: int) -> list[dict]:
    """Stratify by actual member rowid without OFFSET or a biased first page."""
    if not 1 <= count <= 4096:
        raise ValueError("bounded persistence workload must contain 1..4096 records")
    maximum = int(connection.execute("SELECT COALESCE(MAX(rowid),0) FROM l1_compaction_members").fetchone()[0])
    if maximum < count:
        raise ValueError("insufficient L1 members for the requested workload")
    result: dict[str, dict] = {}
    connection.row_factory = sqlite3.Row
    for n in range(count):
        start = 1 + n * maximum // count
        for member in connection.execute(
                "SELECT task_id FROM l1_compaction_members WHERE rowid>=? ORDER BY rowid LIMIT 64",
                (start,)):
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (member[0],)).fetchone()
            if row is not None and row["active"] == 1 and row["status"] == "success" and row["task_id"] not in result:
                result[str(row["task_id"])] = dict(row)
                break
    if len(result) != count:
        raise ValueError("stratified successful tasks incomplete; do not silently shrink workload")
    return [result[key] for key in sorted(result)]


def private_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=60.0)
    connection.row_factory = sqlite3.Row
    if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
        connection.close()
        raise RuntimeError("native WAL journal mode required")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA cache_size=-2000")
    return connection


def prepare(root: Path, fixture_root: Path, count: int) -> None:
    source.fresh(root)
    budget, started, cpu = source.native_budget(), time.perf_counter(), time.process_time()
    fixture = json.loads((fixture_root / "fixture.json").read_bytes())
    manifest = fixture_root / "manifest.sqlite3"
    if fixture.get("contract") != source.CONTRACT or source.digest(manifest) != fixture["manifest_sha256"]:
        raise ValueError("complete immutable source fixture required")
    if source.implementation() != fixture["implementation"]:
        raise ValueError("source fixture code/runtime changed")
    if shutil.disk_usage(root.parent).free < 2 * manifest.stat().st_size + source.RESERVE:
        raise ValueError("capacity required for two retained private copies plus 30 GiB reserve")
    code = identity()
    root.mkdir(mode=0o700)
    with closing(source.readonly(manifest)) as original:
        original.row_factory = sqlite3.Row
        verify_journal(original)
        records = task_records(original, count)
        logical_counts = source.counts(original)
        copies = {}
        for variant in ("original", "covering"):
            target = root / f"{variant}.sqlite3"
            copy_started = time.perf_counter()
            progress_at = [copy_started]

            def progress(status: int, remaining: int, total: int) -> None:
                now = time.perf_counter()
                if now - progress_at[0] >= 20:
                    if shutil.disk_usage(root).free < source.RESERVE:
                        raise RuntimeError("private copy stopped at physical reserve")
                    print(json.dumps({"phase": "private_writer_copy", "variant": variant,
                                      "remaining_pages": remaining, "total_pages": total}), flush=True)
                    progress_at[0] = now

            with closing(sqlite3.connect(target)) as private:
                original.backup(private, pages=1024, progress=progress, sleep=0.02)
                if variant == "original":
                    private.execute(f"DROP INDEX {source.INDEX}")
                    private.commit()
                verify_journal(private)
                if source.counts(private) != logical_counts:
                    raise RuntimeError("private copy changed source/member/segment counts")
                columns = [row[2] for row in private.execute(f"PRAGMA index_info({source.INDEX})")]
                if columns != ([] if variant == "original" else list(source.COLUMNS)):
                    raise RuntimeError("private index variant differs from measured SQL candidate")
                physical = {name: int(private.execute(f"PRAGMA {name}").fetchone()[0])
                            for name in ("page_size", "page_count", "freelist_count", "schema_version")}
            copies[variant] = {"bytes": target.stat().st_size, "sha256": source.digest(target),
                               "device": target.stat().st_dev, "inode": target.stat().st_ino,
                               "physical": physical,
                               "prepare_wall_seconds": time.perf_counter() - copy_started}
            target.chmod(0o600)
    if identity() != code or source.digest(manifest) != fixture["manifest_sha256"]:
        raise RuntimeError("fixed source/code changed during private copy")
    if shutil.disk_usage(root).free < source.RESERVE:
        raise RuntimeError("private copies exceeded physical reserve")
    atomic_write_json(root / "workload.json", {"records": records, "sha256": json_digest(records)})
    receipt = {"contract": CONTRACT, "state": "private_workload_prepared", "implementation": code,
               "source_fixture": str(fixture_root), "source_manifest_sha256": fixture["manifest_sha256"],
               "counts": logical_counts, "workload_records": count, "workload_sha256": json_digest(records),
               "copies": copies, "budget": budget, "prepare_wall_seconds": time.perf_counter() - started,
               "prepare_cpu_seconds": time.process_time() - cpu, "production_writes": False,
               "provider_calls": 0, "payload_writes": False, "source_snapshot_mutated": False}
    atomic_write_json(root / "fixture.json", receipt)
    print(json.dumps({"phase": receipt["state"], "workload_records": count, "copies": copies}), flush=True)


def task_and_result(record: dict) -> tuple[archive.DownloadTask, archive.TaskResult]:
    task = archive.DownloadTask(task_id=record["task_id"], endpoint=record["endpoint"],
                                category=record["category"], scope_key=record["scope_key"],
                                kwargs=json.loads(record["kwargs_json"]),
                                providers=tuple(json.loads(record["providers_json"])),
                                output_path=record["output_path"])
    result = archive.TaskResult(task=task, status="success", provider=record["selected_provider"],
                                rows=record["rows"], output_path=record["output_path"], attempts=1,
                                error=None, provider_outcomes=json.loads(record["provider_outcomes_json"]),
                                provider_evidence=json.loads(record["provider_evidence_json"]))
    return task, result


def reset_records(connection: sqlite3.Connection, records: list[dict]) -> None:
    columns = [key for key in records[0] if key != "task_id"]
    if not all(name.isidentifier() for name in columns):
        raise ValueError("unexpected task column identifier")
    rows = []
    for record in records:
        pending = {**record, "status": "pending", "execution_started_at": None,
                   "updated_at": (BASE_TIME - timedelta(seconds=1)).isoformat()}
        rows.append(tuple(pending[key] for key in columns) + (record["task_id"],))
    with connection:
        connection.executemany("UPDATE tasks SET " + ",".join(f"{key}=?" for key in columns)
                               + " WHERE task_id=?", rows)
    checkpoint(connection)


def checkpoint(connection: sqlite3.Connection) -> None:
    row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if tuple(row) != (0, 0, 0):
        raise RuntimeError(f"private durable WAL checkpoint incomplete: {tuple(row)}")


class FixedClock:
    value = BASE_TIME

    @classmethod
    def now(cls, tz=None):
        return cls.value if tz is None else cls.value.astimezone(tz)


def selected_readback(connection: sqlite3.Connection, records: list[dict]) -> list[dict]:
    result = []
    for first in range(0, len(records), 256):
        keys = [row["task_id"] for row in records[first:first + 256]]
        rows = connection.execute("SELECT * FROM tasks WHERE task_id IN ("
                                  + ",".join("?" for _ in keys) + ")", keys)
        result.extend(dict(row) for row in rows)
    return sorted(result, key=lambda row: row["task_id"])


def replay(connection: sqlite3.Connection, records: list[dict]) -> dict:
    """Use native methods, without their migration/startup/download paths."""
    verify_journal(connection)
    manifest = archive.Manifest.__new__(archive.Manifest)
    manifest.connection = connection
    manifest._completion_batch_depth = 0
    manifest._completion_batch_quarantines = []
    tasks, results = zip(*(task_and_result(row) for row in records), strict=True)
    journal_start = int(connection.execute(f"SELECT COALESCE(MAX(seq),0) FROM {JOURNAL_TABLE}").fetchone()[0])
    changes_start = connection.total_changes
    stages = []

    def timed(name, call):
        before, started, cpu = _stage_resources(), time.perf_counter(), time.process_time()
        call()
        stages.append({"name": name, "wall_seconds": time.perf_counter() - started,
                       "cpu_seconds": time.process_time() - cpu,
                       "resources_before": before, "resources_after": _stage_resources()})

    def forbidden_quarantine(*args, **kwargs):
        raise RuntimeError("private writer benchmark cannot mutate any payload")

    with patch.object(archive, "datetime", FixedClock), patch.object(
            archive, "_quarantine_obsolete_task_output", forbidden_quarantine):
        FixedClock.value = BASE_TIME
        timed("native_claim", lambda: manifest.claim(tasks))
        FixedClock.value = BASE_TIME + timedelta(seconds=1)
        timed("native_mark_executing", lambda: manifest.mark_executing(tasks))

        def complete():
            for first in range(0, len(results), archive.COMPLETION_PERSISTENCE_BATCH_CAP):
                with manifest.completion_batch():
                    for result in results[first:first + archive.COMPLETION_PERSISTENCE_BATCH_CAP]:
                        manifest.complete(result)

        FixedClock.value = BASE_TIME + timedelta(seconds=2)
        timed("native_completion_batches", complete)
    timed("durable_wal_checkpoint", lambda: checkpoint(connection))
    expected = []
    for record, result in zip(records, results, strict=True):
        expected.append({**record, "status": "success", "attempts": record["attempts"] + 1,
                         "kwargs_json": archive._canonical_json(result.task.kwargs), "error": None,
                         "provider_outcomes_json": archive._canonical_json(result.provider_outcomes),
                         "provider_evidence_json": archive._canonical_json(result.provider_evidence),
                         "retry_not_before": None, "transient_failures": 0,
                         "execution_started_at": None, "updated_at": FixedClock.value.isoformat()})
    # Close/reopen the DB in worker for independent persisted readback too.
    actual = selected_readback(connection, records)
    journal = [str(row[0]) for row in connection.execute(
        f"SELECT task_id FROM {JOURNAL_TABLE} WHERE seq>? ORDER BY seq", (journal_start,))]
    wanted_journal = Counter({record["task_id"]: 3 for record in records})
    if actual != expected or Counter(journal) != wanted_journal or connection.total_changes - changes_start != 6 * len(records):
        raise RuntimeError("native persisted tasks or mutation journal differ from exact workload")
    return {"stages": stages, "output_sha256": json_digest(actual),
            "journal_rows": len(journal), "journal_multiset_sha256": json_digest(sorted(journal)),
            "direct_and_trigger_changes": connection.total_changes - changes_start,
            "completion_batch_cap": archive.COMPLETION_PERSISTENCE_BATCH_CAP,
            "persistence_wall_seconds": sum(row["wall_seconds"] for row in stages),
            "persistence_cpu_seconds": sum(row["cpu_seconds"] for row in stages)}


def worker(root: Path, output: Path, variant: str) -> None:
    source.fresh(output, root=root)
    if not root.is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("private benchmark fixture required")
    budget, started = source.native_budget(), time.perf_counter()
    fixture = json.loads((root / "fixture.json").read_bytes())
    workload = json.loads((root / "workload.json").read_bytes())
    if (fixture.get("contract") != CONTRACT or identity() != fixture["implementation"]
            or json_digest(workload["records"]) != fixture["workload_sha256"]
            or workload["sha256"] != fixture["workload_sha256"]):
        raise ValueError("fixed writer code/runtime/workload identity required")
    records, database = workload["records"], root / f"{variant}.sqlite3"
    saved = fixture["copies"][variant]
    if (database.is_symlink() or database.resolve(strict=True).parent != root
            or (database.stat().st_dev, database.stat().st_ino) != (saved["device"], saved["inode"])):
        raise ValueError("private database inode changed; never follow a redirected writer")
    if shutil.disk_usage(root).free < source.RESERVE:
        raise ValueError("private writer requires the 30 GiB physical reserve")
    original = Path(fixture["source_fixture"]) / "manifest.sqlite3"
    if source.digest(original) != fixture["source_manifest_sha256"]:
        raise RuntimeError("original immutable SQL snapshot changed")
    prep = time.perf_counter()
    with closing(private_connection(database)) as connection:
        columns = [row[2] for row in connection.execute(f"PRAGMA index_info({source.INDEX})")]
        if columns != ([] if variant == "original" else list(source.COLUMNS)):
            raise ValueError("unexpected private index variant")
        reset_records(connection, records)
        reset_seconds = time.perf_counter() - prep
        result = replay(connection, records)
    with closing(source.readonly(database)) as observer:
        observer.row_factory = sqlite3.Row
        if json_digest(selected_readback(observer, records)) != result["output_sha256"]:
            raise RuntimeError("independent durable task readback differs")
        if source.counts(observer) != fixture["counts"]:
            raise RuntimeError("private lifecycle changed total task/member/segment scope")
    if (identity() != fixture["implementation"] or model_runtime_imported()
            or source.digest(original) != fixture["source_manifest_sha256"]):
        raise RuntimeError("source/code changed or model runtime imported")
    private_sha = source.digest(database)
    receipt = {"contract": CONTRACT, "state": "completed_trial", "variant": variant,
               "implementation": fixture["implementation"], "workload_sha256": fixture["workload_sha256"],
               "workload_records": len(records), "counts": fixture["counts"], "budget": budget,
               **result, "reset_wall_seconds": reset_seconds,
               "workflow_wall_seconds": time.perf_counter() - started,
               "source_manifest_sha256": fixture["source_manifest_sha256"],
               "private_database_sha256": private_sha, "production_writes": False,
               "provider_calls": 0, "payload_writes": False, "model_runtime_imported": False,
               "scope": "bounded native persistence replay on full-size DB, not full downloader throughput"}
    atomic_write_json(output, receipt)
    print(json.dumps({"state": receipt["state"], "variant": variant,
                      "persistence_wall_seconds": result["persistence_wall_seconds"]}), flush=True)


def aggregate(root: Path, output: Path) -> None:
    source.fresh(output, root=root)
    fixture = json.loads((root / "fixture.json").read_bytes())
    trials = [json.loads((root / f"{n:02d}-{variant}.json").read_bytes())
              for n, variant in enumerate(VARIANTS, 1)]
    for row, variant in zip(trials, VARIANTS, strict=True):
        if (row.get("contract") != CONTRACT or row.get("state") != "completed_trial"
                or row.get("variant") != variant or row.get("implementation") != fixture["implementation"]
                or row.get("source_manifest_sha256") != fixture["source_manifest_sha256"]
                or row.get("workload_sha256") != fixture["workload_sha256"]
                or row.get("counts") != fixture["counts"] or row.get("workload_records") != fixture["workload_records"]
                or row.get("production_writes") is not False or row.get("payload_writes") is not False
                or row.get("provider_calls") != 0 or row.get("model_runtime_imported") is not False):
            raise ValueError("complete fixed-source native-persistence ABBA receipts required")
    proofs = ("output_sha256", "journal_rows", "journal_multiset_sha256", "direct_and_trigger_changes")
    if len({tuple(row[name] for name in proofs) for row in trials}) != 1:
        raise ValueError("native task output/journal parity failed")
    means = {v: {name: sum(row[name] for row in trials if row["variant"] == v) / 2
                 for name in ("persistence_wall_seconds", "persistence_cpu_seconds", "workflow_wall_seconds")}
             for v in ("original", "covering")}
    receipt = {"contract": CONTRACT, "state": "accepted_bounded_persistence_comparison",
               "trials": trials, "fixture": fixture, "means": means,
               "relative_change_percent": {key: 100 * (means["covering"][key] / means["original"][key] - 1)
                                           for key in means["original"]},
               "exact_native_output_and_journal_parity": True, "production_writes": False,
               "production_index_installed": False, "os_page_cache_controlled": False,
               "exclusive_host": False, "scope": "bounded native replay on full-size private DB; no native DDL/adoption proof"}
    atomic_write_json(output, receipt)
    print(json.dumps({"state": receipt["state"], "relative_change_percent": receipt["relative_change_percent"]}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--prepare", type=Path, metavar="SQL_FIXTURE")
    mode.add_argument("--worker", choices=("original", "covering"))
    mode.add_argument("--aggregate", action="store_true")
    parser.add_argument("--records", type=int, default=1024)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.prepare:
        prepare(args.fixture.absolute(), args.prepare.resolve(strict=True), args.records)
    elif args.output is None:
        parser.error("worker/aggregate require a new output within private fixture")
    elif args.worker:
        worker(args.fixture.resolve(strict=True), args.output.absolute(), args.worker)
    else:
        aggregate(args.fixture.resolve(strict=True), args.output.absolute())


if __name__ == "__main__":
    main()
