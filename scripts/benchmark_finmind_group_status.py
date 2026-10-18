"""Full real-queue publication/status A/B/B/A for exact TEMP invalidation.

No provider calls or live queue writes. Both sides retain canonical source
validation, Parquet/receipt durability, fingerprinting, transactions, selector,
frontiers and status output. This is an offline local-workflow benchmark, not
an API throughput claim. Source bodies and metadata are frozen before timing.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import UTC, datetime, timedelta
import hashlib
from itertools import zip_longest
import json
import os
from pathlib import Path
import shutil
import sqlite3
import statistics
import subprocess
import time
from unittest.mock import patch

import pyarrow.parquet as pq

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.finmind_updates import ResponseRows
from scripts.benchmark_finmind_status import claim_release, digest, file_sha
from downloader.finmind_status_aggregates import StatusConnection


def resource_limits():
    group = Path("/proc/self/cgroup").read_text().strip().split(":", 2)[-1]
    unit = group.rsplit("/", 1)[-1]
    result = subprocess.run(["systemctl", "show", unit, "-p", "MemoryHigh", "-p", "MemoryMax",
                             "-p", "CPUWeight", "-p", "IOWeight"], capture_output=True, text=True,
                            check=True, timeout=5)
    return {"unit": unit, "properties": dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line),
            "process_nice": os.getpriority(os.PRIO_PROCESS, 0)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind/complement"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tasks-per-status", type=int, default=16)
    parser.add_argument("--prepared-from", type=Path)
    args = parser.parse_args(argv)
    if args.output.exists() or not 1 <= args.tasks_per_status <= 64:
        parser.error("new output required; tasks-per-status must be 1..64")
    root = args.root.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    path = root / "queue.sqlite3"
    if shutil.disk_usage(path).free < 30 * 1024**3 + (0 if args.prepared_from else 3 * path.stat().st_size):
        raise ValueError("disposable_queue_headroom_guard")
    now = datetime.now(UTC)
    status = json.loads((root / "status.json").read_bytes())
    dataset = "UKStockPrice"  # Actual native 08:03 profile's nonempty history.
    ids = worker._snapshot_ids(root, "TaiwanStockInfo", "stock_id")
    delisted = worker._official_delisted_ids(root)
    delegated = frozenset(status.get("delegated_to_sponsor", ()))
    code_paths = [Path(__file__), Path(worker.__file__), Path("downloader/download_finmind_free.py"),
                  Path("downloader/finmind_status_aggregates.py"), Path("downloader/finmind_queue_indexes.py"),
                  Path("downloader/finmind_updates.py"), Path("downloader/artifact_io.py"),
                  Path("downloader/finmind_supplemental.py"), Path("downloader/finmind_scheduling.py"),
                  Path("downloader/finmind_json_pipeline.py"), Path("downloader/finmind_object_pipeline.py"),
                  Path("downloader/finmind_corrections.py"),
                  Path("configs/finmind_json_pipeline.json")]
    code_before = {str(p.resolve()): file_sha(p) for p in code_paths}
    prior = json.loads(args.prepared_from.read_bytes()) if args.prepared_from else None
    if prior and (prior.get("state") != "measurement_complete" or not prior.get("exact_output_parity")
                  or not prior.get("all_task_values_equal") or prior.get("source_queue") != str(path)):
        raise ValueError("prepared_input_requires_complete_same_source_parity_proof")
    folder = Path(prior["fixture"]) if prior else args.output.parent / (args.output.stem + "-fixture")
    if not prior:
        if folder.exists():
            raise ValueError("new_fixture_required")
        folder.mkdir()
    old_root, new_root = folder / "baseline", folder / "candidate"
    for target in (old_root, new_root):
        if not prior:
            target.mkdir()
    snapshot_started = time.perf_counter()
    if not prior:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as source, \
                closing(sqlite3.connect(old_root / "queue.sqlite3")) as output:
            # Pin one read snapshot; continuous writes otherwise restart a
            # stepped multi-GiB backup. WAL still admits the canonical writer.
            source.execute("BEGIN")
            source.execute("SELECT dataset FROM tasks LIMIT 1").fetchone()
            def progress(_status, _remaining, _total):
                if time.perf_counter() - snapshot_started > 90:
                    raise TimeoutError("bounded_read_snapshot_copy_deadline")
            try:
                source.backup(output, pages=2048, sleep=.01, progress=progress)
            finally:
                source.rollback()
    frozen_sha = file_sha(old_root / "queue.sqlite3")
    if not prior:
        shutil.copyfile(old_root / "queue.sqlite3", new_root / "queue.sqlite3")
    if frozen_sha != file_sha(new_root / "queue.sqlite3"):
        raise ValueError("snapshot_copy_or_prepared_private_queue_bytes_mismatch")
    bodies, receipt_heads = [], []
    with closing(sqlite3.connect(old_root / "queue.sqlite3")) as conn:
        count = conn.execute("SELECT count(*) FROM tasks").fetchone()[0]
        selected = conn.execute("SELECT dataset,data_id,partition,kind,priority,state,receipt_path "
                                "FROM tasks WHERE dataset=? AND state='complete' AND rows>0 "
                                "ORDER BY last_attempt_at_utc DESC LIMIT ?", (dataset, args.tasks_per_status)).fetchall()
    if len(selected) != args.tasks_per_status:
        raise ValueError("insufficient_real_history_bodies")
    for *fields, relative in selected:
        task = worker.Task(*fields)
        origin = (root / relative).resolve()
        if not origin.is_relative_to(root) or origin.stat().st_size > 1024**2:
            raise ValueError("unsafe_source_receipt")
        raw = origin.read_bytes()
        receipt = json.loads(raw)
        parquet = (root / receipt["parquet_path"]).resolve()
        if not parquet.is_relative_to(root) or file_sha(parquet) != receipt["sha256"]:
            raise ValueError("source_parquet_hash_mismatch")
        rows = pq.read_table(parquet, use_threads=False).to_pylist()
        if not rows or len(rows) != receipt["rows"]:
            raise ValueError("source_row_count_mismatch")
        bodies.append((task, rows))
        receipt_heads.append({"path": str(origin), "sha256": hashlib.sha256(raw).hexdigest(),
                              "parquet_path": str(parquet), "parquet_sha256": receipt["sha256"],
                              "rows": len(rows)})
        for target in (() if prior else (old_root, new_root)):
            (target / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(origin, target / relative)
    preparation = time.perf_counter() - snapshot_started
    resource_before = resource_limits()
    trials, initial = [], {}
    with ExitStack() as stack:
        old = stack.enter_context(closing(sqlite3.connect(old_root / "queue.sqlite3", timeout=10)))
        new = stack.enter_context(closing(sqlite3.connect(new_root / "queue.sqlite3", timeout=10,
                                                        factory=StatusConnection)))
        for conn in (old, new):
            conn.execute("PRAGMA journal_mode=WAL")
        stack.enter_context(patch.object(worker, "_snapshot_ids", return_value=ids))
        stack.enter_context(patch.object(worker, "_official_delisted_ids", return_value=delisted))
        stack.enter_context(patch.object(worker.requests.Session, "request", side_effect=AssertionError("provider_forbidden")))
        for label, conn, owner in (("baseline", old, old_root), ("candidate", new, new_root)):
            with patch.object(worker, "_now", return_value=now):
                began, cpu = time.perf_counter(), time.process_time()
                result = worker._status(conn, owner, state="running", last=status.get("last_task"), delegated=delegated)
                initial[label] = {"wall_seconds": time.perf_counter() - began,
                                  "cpu_seconds": time.process_time() - cpu, "status_sha256": digest(result)}
        if initial["baseline"]["status_sha256"] != initial["candidate"]["status_sha256"]:
            raise ValueError("initial_full_status_mismatch")
        ordinals = {"baseline": 0, "candidate": 0}
        for label in ("baseline", "candidate", "candidate", "baseline"):
            conn, owner = (old, old_root) if label == "baseline" else (new, new_root)
            ordinal = ordinals[label]
            ordinals[label] += 1
            stamp = now + timedelta(seconds=ordinal + 1)
            produced = []
            with patch.object(worker, "_now", return_value=stamp):
                began, cpu = time.perf_counter(), time.process_time()
                claim_release(conn, [task for task, _ in bodies])
                for task, rows in bodies:
                    observed = ResponseRows(rows, observation={"request_id": f"{args.output.stem}-{ordinal}-{task.data_id}",
                        "request_started_at_utc": stamp.isoformat(), "response_received_at_utc": stamp.isoformat(),
                        "response_rows": len(rows), "network_seconds": 0.0})
                    receipt = worker._store(owner, task, observed, stamp)
                    worker._save_result(conn, task, receipt, stamp)
                    produced.append(receipt)
                last_task = bodies[-1][0]
                last = {"dataset": last_task.dataset, "data_id": last_task.data_id,
                        "partition": last_task.partition, "status": "complete", "rows": len(bodies[-1][1]),
                        "observed_at_utc": stamp.isoformat()}
                summary = worker._status(conn, owner, state="running", last=last, delegated=delegated)
                elapsed, used = time.perf_counter() - began, time.process_time() - cpu
            trials.append({"label": label, "ordinal": ordinal, "wall_seconds": elapsed, "cpu_seconds": used,
                           "whole_status_sha256": digest(summary), "all_receipt_sha256": digest(produced),
                           "parquet_bytes_sha256": [file_sha(owner / r["parquet_path"]) for r in produced]})
        exact = all(len({digest([r["whole_status_sha256"], r["all_receipt_sha256"], r["parquet_bytes_sha256"]])
                             for r in trials if r["ordinal"] == index}) == 1 for index in (0, 1))
        resource_after = resource_limits()
        # All keys/values, constant memory. Never materialize both multi-million
        # row queues in RAM merely to validate a lower-idle-cost candidate.
        task_values_equal = all(a == b for a, b in zip_longest(
            old.execute("SELECT rowid,* FROM tasks ORDER BY rowid"),
            new.execute("SELECT rowid,* FROM tasks ORDER BY rowid")))
    code_after = {str(p.resolve()): file_sha(p) for p in code_paths}
    source_stable = all(file_sha(Path(r["path"])) == r["sha256"] and
                        file_sha(Path(r["parquet_path"])) == r["parquet_sha256"] for r in receipt_heads)
    means = {label: {metric: statistics.mean(r[metric] for r in trials if r["label"] == label)
                     for metric in ("wall_seconds", "cpu_seconds")} for label in ("baseline", "candidate")}
    result = {"schema_version": 1, "state": "measurement_complete", "observed_at_utc": now.isoformat(),
              "source_queue": str(path), "task_count": count, "fixture": str(folder), "snapshot_sha256": frozen_sha,
              "preparation_wall_seconds": preparation, "dataset": dataset, "tasks_per_status": len(bodies),
              "prepared_from": str(args.prepared_from) if args.prepared_from else None,
              "resource_limits": resource_before, "resource_limits_stable": resource_before == resource_after,
              "rows_published_per_trial": sum(len(rows) for _, rows in bodies), "initial_full_status": initial,
              "trials": trials, "means": means, "exact_output_parity": exact, "all_task_values_equal": task_values_equal,
              "source_files_stable": source_stable, "code_stable": code_before == code_after,
              "code_sha256": code_after, "source_receipts": receipt_heads,
              "provider_calls": 0, "production_queue_writes": 0, "production_deployed": False,
              "whole_publication_wall_improvement_observed": means["candidate"]["wall_seconds"] < means["baseline"]["wall_seconds"],
              "caveats": ["Offline full local publication/status only; no network latency or throughput proof.",
                          "Full queue, real source rows and all native publication validations retained.",
                          "Clock, identifiers and receipt inputs frozen; metadata capture/preparation reported separately.",
                          "Shared host; OS cache not cleared; not a first-fill RAM or whole-machine optimum claim."]}
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("state", "task_count", "means", "exact_output_parity",
                                            "all_task_values_equal", "code_stable", "source_files_stable")}))
    if not (exact and task_values_equal and source_stable and result["code_stable"] and result["resource_limits_stable"]):
        raise ValueError("complete_workflow_parity_or_preservation_failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
