"""Compare complete heartbeat/publication on two copies of one real queue.

Includes the exact selector, history frontiers, retry summaries and JSON write.
Fixes the clock, captured identifier sets and last task. Excludes provider calls,
source receipt discovery and live queue mutations. Also measures the unchanged
claim/release transaction path on disposable copies, rather than hiding the
covering index's storage/write cost behind a read-only microbenchmark.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import shutil
import sqlite3
import statistics
import tempfile
import time
from unittest.mock import patch

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.finmind_queue_indexes import STATUS_INDEX, ensure_status_index
from downloader.finmind_runtime import optimize_queue
from downloader import finmind_runtime, finmind_history_order

AGGREGATE_SQL = (
    "SELECT dataset,state,COUNT(*),SUM(rows),SUM(bytes),MIN(first_data_date),"
    "MAX(last_data_date),MAX(last_attempt_at_utc) FROM tasks GROUP BY dataset,state"
)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def file_sha(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def describe(connection):
    plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + AGGREGATE_SQL)]
    return {"aggregate_plan": plan,
            "task_count": connection.execute("SELECT count(*) FROM tasks").fetchone()[0],
            "total_changes": connection.total_changes}


def claim_release(connection, tasks):
    from downloader.finmind_object_pipeline import _claim, _release

    keys = [(item.dataset, item.data_id, item.partition) for item in tasks]
    def before_after():
        return [connection.execute("SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?", key).fetchone()
                for key in keys]
    before = before_after()
    started, cpu = time.perf_counter(), time.process_time()
    for task in tasks:
        _claim(connection, task)
        _release(connection, task)
    elapsed, used = time.perf_counter() - started, time.process_time() - cpu
    if before_after() != before:
        raise ValueError("scratch_claim_release_changed_task_values")
    return {"wall_seconds": elapsed, "cpu_seconds": used, "tasks": len(tasks),
            "transactions": 2 * len(tasks), "exact_task_rows_restored": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind/complement"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--write-tasks", type=int, default=64)
    parser.add_argument("--candidate", choices=("covering_index", "table_scan"), default="covering_index")
    args = parser.parse_args(argv)
    if not 1 <= args.repeats <= 5 or not 1 <= args.write_tasks <= 256:
        parser.error("bounded repeats/write-tasks must be 1..5 / 1..256")
    if args.output.exists():
        parser.error("refusing to replace an existing measurement")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    source = args.root.resolve()
    if shutil.disk_usage(args.output.parent).free < 3 * (source / "queue.sqlite3").stat().st_size + 5 * 1024**3:
        raise ValueError("insufficient_disposable_benchmark_headroom")
    now = datetime.now(UTC)
    status = json.loads((source / "status.json").read_bytes())
    last = status.get("last_task")
    current_ids = worker._snapshot_ids(source, "TaiwanStockInfo", "stock_id")
    delisted_ids = worker._official_delisted_ids(source)
    delegated = frozenset(status.get("delegated_to_sponsor", ()))
    dataset = (status.get("active_task") or last or {}).get("dataset")
    if not dataset:
        raise ValueError("live_status_has_no_current_dataset")
    code_files = (Path(worker.__file__), Path(__file__),
                  Path(__file__).resolve().parents[1] / "downloader/finmind_queue_indexes.py",
                  *(Path(module.__file__) for module in (worker.supplemental, worker.retry_cohorts,
                    worker.retry_policy, worker.object_pipeline, worker.json_pipeline,
                    finmind_runtime, finmind_history_order)))
    code_before = {str(path): file_sha(path) for path in code_files}
    results, writes = [], []
    with tempfile.TemporaryDirectory(prefix="finmind-status-", dir=args.output.parent) as scratch, ExitStack() as stack:
        folder = Path(scratch)
        old_path, new_path = folder / "baseline.sqlite3", folder / "candidate.sqlite3"
        copy_started = time.perf_counter()
        with closing(sqlite3.connect((source / "queue.sqlite3").as_uri() + "?mode=ro", uri=True, timeout=2)) as live, \
                closing(sqlite3.connect(old_path)) as target:
            live.backup(target)
        snapshot_sha = file_sha(old_path)
        shutil.copyfile(old_path, new_path)
        if file_sha(new_path) != snapshot_sha:
            raise ValueError("queue_copies_are_not_identical")
        copy_seconds = time.perf_counter() - copy_started
        old = stack.enter_context(closing(sqlite3.connect(old_path, timeout=10)))
        new = stack.enter_context(closing(sqlite3.connect(new_path, timeout=10)))
        source_had_index = bool(old.execute("SELECT 1 FROM sqlite_master WHERE name=?", (STATUS_INDEX,)).fetchone())
        # Later rechecks reconstruct only the old physical index surface on
        # the disposable copies; never drop an index on the live queue.
        if source_had_index:
            for connection in (old, new):
                connection.execute(f"DROP INDEX {STATUS_INDEX}")
                connection.commit()
        # Match the canonical owner's WAL/full-durability setup on both copies.
        # DELETE journal fsyncs are not a proxy for this live transaction path.
        for connection in (old, new):
            connection.execute("PRAGMA journal_mode=WAL").fetchall()
            optimize_queue(connection)
            connection.commit()
        baseline = describe(old)
        non_integer_counts = old.execute(
            "SELECT count(*) FROM tasks WHERE typeof(rows)!='integer' OR typeof(bytes)!='integer'"
        ).fetchone()[0]
        if non_integer_counts:
            raise ValueError("non_integer_task_counts_not_covered_by_exact_integer_aggregate_contract")
        before_pages = new.execute("PRAGMA page_count").fetchone()[0]
        used_pages = before_pages - new.execute("PRAGMA freelist_count").fetchone()[0]
        started, cpu = time.perf_counter(), time.process_time()
        if args.candidate == "covering_index":
            ensure_status_index(new)
            # _db already maintains bounded statistics after schema setup. A
            # new wide index without these stats can damage frontier/selector
            # join planning even when the status query itself gets faster.
            optimize_queue(new)
            new.commit()
        setup = {"wall_seconds": time.perf_counter() - started,
                 "cpu_seconds": time.process_time() - cpu,
                 "added_bytes": (new.execute("PRAGMA page_count").fetchone()[0] - before_pages)
                                * new.execute("PRAGMA page_size").fetchone()[0],
                 "allocated_index_bytes": (new.execute("PRAGMA page_count").fetchone()[0]
                                           - new.execute("PRAGMA freelist_count").fetchone()[0] - used_pages)
                                          * new.execute("PRAGMA page_size").fetchone()[0]}
        candidate = describe(new)
        if args.candidate == "covering_index" and not any("COVERING INDEX " + STATUS_INDEX in line
                                                         for line in candidate["aggregate_plan"]):
            raise ValueError("automatic_planner_did_not_select_covering_index")
        tasks = [worker.Task(*row) for row in old.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset=? AND state='complete' ORDER BY data_id,partition LIMIT ?",
            (dataset, args.write_tasks))]
        if len(tasks) != args.write_tasks:
            raise ValueError("insufficient_verified_real_tasks_for_write_check")
        # All filesystem-dependent identifier input is captured once; full
        # clock-dependent frontier/selector/retry logic still runs on each copy.
        stack.enter_context(patch.object(worker, "_now", return_value=now))
        stack.enter_context(patch.object(worker, "_snapshot_ids", return_value=current_ids))
        stack.enter_context(patch.object(worker, "_official_delisted_ids", return_value=delisted_ids))
        reference_sha = None
        class SequentialAggregate(sqlite3.Connection):
            def execute(self, query, *arguments):
                if query == AGGREGATE_SQL:
                    query = query.replace("FROM tasks GROUP BY", "FROM tasks NOT INDEXED GROUP BY")
                return super().execute(query, *arguments)
        if args.candidate == "table_scan":
            new.close()
            new = stack.enter_context(closing(sqlite3.connect(new_path, timeout=10, factory=SequentialAggregate)))
            candidate["aggregate_plan"] = [row[3] for row in new.execute(
                "EXPLAIN QUERY PLAN " + AGGREGATE_SQL.replace("FROM tasks GROUP BY", "FROM tasks NOT INDEXED GROUP BY"))]
        for repeat in range(args.repeats):
            for name in ("baseline", "candidate", "candidate", "baseline"):
                connection = old if name == "baseline" else new
                changes = connection.total_changes
                started, cpu = time.perf_counter(), time.process_time()
                result = worker._status(connection, folder / name, state="running", last=last, delegated=delegated)
                elapsed, used = time.perf_counter() - started, time.process_time() - cpu
                current_sha = digest(result)
                reference_sha = reference_sha or current_sha
                if current_sha != reference_sha or connection.total_changes != changes:
                    raise ValueError("full_status_changed_or_mutated_the_queue")
                results.append({"variant": name, "repeat": repeat, "wall_seconds": elapsed,
                                "cpu_seconds": used, "whole_status_sha256": current_sha,
                                "queue_changes": connection.total_changes - changes})
            for name in (("baseline", "candidate") if repeat % 2 == 0 else ("candidate", "baseline")):
                connection = old if name == "baseline" else new
                writes.append({"variant": name, "repeat": repeat, **claim_release(connection, tasks)})
        code_after = {str(path): file_sha(path) for path in code_files}
        if code_after != code_before:
            raise ValueError("code_changed_during_measurement")
        medians = {name: {key: statistics.median(item[key] for item in results if item["variant"] == name)
                          for key in ("wall_seconds", "cpu_seconds")} for name in ("baseline", "candidate")}
        faster = medians["candidate"]["wall_seconds"] < medians["baseline"]["wall_seconds"]
        atomic_write_json(args.output, {"schema_version": 2, "state": "measurement_complete", "candidate_kind": args.candidate,
            "exact_output_parity": True, "status_wall_improvement_observed": faster,
            "observed_at_utc": now.isoformat(), "scope": "fixed_real_queue_full_status_with_captured_identifier_input",
            "production_queue_mutations": 0, "provider_calls": 0, "download_throughput_improvement_proven": False,
            "source_queue": str(source / "queue.sqlite3"), "snapshot_sha256": snapshot_sha,
            "source_had_status_index": source_had_index, "non_integer_count_fields": non_integer_counts,
            "journal_mode": "wal", "synchronous": old.execute("PRAGMA synchronous").fetchone()[0],
            "snapshot_bytes": old_path.stat().st_size, "snapshot_copy_seconds": copy_seconds,
            "fixed_clock": now.isoformat(), "current_dataset": dataset, "code_sha256": code_before,
            "code_stable": True, "baseline": baseline, "candidate": candidate,
            "index_setup": setup, "whole_status_sha256": reference_sha, "trials": results,
            "claim_release_trials": writes, "status_medians": medians,
            "wall_reduction_fraction": 1 - medians["candidate"]["wall_seconds"] / medians["baseline"]["wall_seconds"],
            "cpu_reduction_fraction": 1 - medians["candidate"]["cpu_seconds"] / medians["baseline"]["cpu_seconds"],
            "whole_benchmark_peak_rss_bytes_not_variant_comparison": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024})
    print(json.dumps({"state": "measurement_complete", "exact_output_parity": True,
                      "status_wall_improvement_observed": faster, "trials": len(results), "status_medians": medians,
                      "provider_calls": 0, "production_queue_mutations": 0}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
