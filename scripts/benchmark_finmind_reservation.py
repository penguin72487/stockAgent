"""Compare unchanged full release reservation/status/write paths on real queues.

Retains both online-backup fixtures and exact outputs. No provider requests,
live queue schema writes, historical scope changes or task reclassification.
This is a control-path benchmark, not proof of end-to-end API throughput.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, nullcontext
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
import time
from unittest.mock import patch

from downloader import download_finmind_complement as worker
from downloader import finmind_scheduling as scheduling
from downloader import finmind_runtime, finmind_history_order, finmind_corrections
from downloader.artifact_io import atomic_write_json
from scripts.benchmark_finmind_status import AGGREGATE_SQL, claim_release, digest, file_sha
from downloader.finmind_queue_indexes import (
    RELEASE_RESERVE_INDEX, RELEASE_RESERVE_COLUMNS, RELEASE_RESERVE_PREDICATE,
    ensure_release_reserve_index, release_reserve_index,
    US_PUBLICATION_INDEX, US_PUBLICATION_PREDICATE, ensure_us_publication_index,
    PERIODIC_INDEX, PERIODIC_PREDICATE, ensure_periodic_index,
)


def guarded_planner():
    """Prototype a locked plan inside a bounded benchmark, not production SQL."""
    native_connect = sqlite3.connect

    class Guarded:
        def execute(self, sql, parameters=()):
            if sql.startswith("SELECT dataset,next_attempt_at_utc,count(*) FROM tasks WHERE"):
                hint = release_reserve_index(self)
                sql = sql.replace("FROM tasks WHERE", f"FROM tasks{hint} WHERE", 1)
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        base = kwargs.pop("factory", sqlite3.Connection)
        factory = type("GuardedConnection", (Guarded, base), {})
        return native_connect(*args, factory=factory, **kwargs)

    return patch.object(scheduling.sqlite3, "connect", side_effect=connect)


def copy_heads(source: Path, target: Path, now: datetime) -> None:
    """Copy the bounded metadata actually consulted, not source Parquet data."""
    relative = [Path(name) for name in ("calendar.json", "calendar_refresh.json",
                "sponsor/status.json", "sponsor/worker_status.json", "complement/status.json")]
    local = now.astimezone(scheduling.TAIPEI)
    cutoff = local.date() if local.hour >= 14 else (local.date() - timedelta(days=1))
    calendar = scheduling._read_metadata(source / "calendar.json")
    dates = calendar.get("dates")
    dates = [day for day in dates if isinstance(day, str) and day <= str(cutoff)] if isinstance(dates, list) else []
    relative.append(Path("receipts/TaiwanStockInfoWithWarrant") / f"{local.date()}.json")
    try:
        latest = date.fromisoformat(max(dates)) if dates else None
    except ValueError:
        latest = None
    if latest:
        relative.extend(Path("receipts") / dataset / f"{latest}.json" for dataset in
                        ("TaiwanStockStatisticsOfOrderBookAndTrade", "TaiwanVariousIndicators5Seconds"))
    for name in relative:
        origin = source / name
        if origin.is_file():
            if origin.stat().st_size > 4 * 1024**2:
                raise ValueError("metadata_head_exceeds_bounded_fixture")
            (target / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(origin, target / name)


def reservation_plans(root: Path, now: datetime) -> dict:
    """Observe the actual unchanged statement, not a hand-written SQL proxy."""
    plans = {}
    native_connect = sqlite3.connect

    class PlanConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql.startswith("SELECT dataset,next_attempt_at_utc,count(*) FROM tasks"):
                plans[self.owner] = [row[3] for row in super().execute("EXPLAIN QUERY PLAN " + sql, parameters)]
            return super().execute(sql, parameters)

    def connect(path, **kwargs):
        owner = next((owner for owner in ("sponsor", "complement") if f"/{owner}/queue.sqlite3" in str(path)), None)
        if owner is None:
            return native_connect(path, **kwargs)
        connection = native_connect(path, factory=PlanConnection, **kwargs)
        connection.owner = owner
        return connection

    with patch.object(scheduling.sqlite3, "connect", side_effect=connect):
        result = scheduling.incremental_reservation(root, now)
    if result["queue_errors"]:
        raise ValueError("reservation_plan_read_failed")
    return plans


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_finmind"))
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--write-tasks", type=int, default=64)
    parser.add_argument("--compare-automatic", action="store_true",
                        help="also measure the same indexed fixture with automatic query planning")
    parser.add_argument("--prepared-from", type=Path,
                        help="reuse the retained private fixture bound to this earlier parity receipt")
    parser.add_argument('--include-us-publication-recheck', action='store_true',
                        help='include the complete early-session repair query, preserving its original threshold')
    parser.add_argument('--include-periodic-preparation', action='store_true',
                        help='also compare the original full-queue periodic selection with its canonical indexed query')
    parser.add_argument('--include-correction-candidates', action='store_true',
                        help='compare the original whole-dataset correction scan with necessary SQL bounds')
    args = parser.parse_args(argv)
    if (args.fixture.exists() != bool(args.prepared_from)) or args.output.exists():
        parser.error("new fixture/output required; retained measurements are never overwritten")
    if not 1 <= args.repeats <= 5 or not 1 <= args.write_tasks <= 64:
        parser.error("repeats/write-tasks must be 1..5 / 1..64")
    source = args.root.resolve()
    source_queues = [source / owner / "queue.sqlite3" for owner in ("sponsor", "complement")]
    if not source_queues[1].is_file():
        raise ValueError("missing_complement_queue")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(args.output.parent).free < 30 * 1024**3 + 3 * sum(
            path.stat().st_size for path in source_queues if path.is_file()):
        raise ValueError("retained_fixture_headroom_guard")
    if args.prepared_from:
        prior = json.loads(args.prepared_from.read_bytes())
        if (prior.get("state") != "measurement_complete" or not prior.get("exact_output_parity")
                or Path(prior["fixture"]).resolve() != args.fixture.resolve()
                or Path(prior["source_queue"]).resolve() != source_queues[1]):
            raise ValueError("prepared_fixture_not_bound_to_prior_parity_receipt")
    else:
        args.fixture.mkdir(parents=True)
    now = datetime.now(UTC)
    code_files = (Path(__file__), Path(worker.__file__), Path(scheduling.__file__),
                  Path(__file__).resolve().parents[1] / "downloader/finmind_queue_indexes.py",
                  *(Path(module.__file__) for module in (worker.supplemental, worker.retry_cohorts,
                    worker.retry_policy, worker.object_pipeline, worker.json_pipeline,
                    finmind_runtime, finmind_history_order, finmind_corrections)))
    code_before = {str(path.resolve()): file_sha(path) for path in code_files}
    status = scheduling._read_metadata(source / "complement/status.json")
    current_ids = worker._snapshot_ids(source / "complement", "TaiwanStockInfo", "stock_id")
    delisted_ids = worker._official_delisted_ids(source / "complement")
    snapshots, setup, trials, writes = {}, {}, [], []
    paths = {name: args.fixture / name for name in ("baseline", "candidate")}
    # Read each provider owner once, using SQLite's online backup. The original
    # function also reads owners separately; this makes both variants identical.
    if not args.prepared_from:
        copy_heads(source, paths["baseline"], now)
    for origin in source_queues if not args.prepared_from else ():
        if not origin.is_file():
            continue
        relative = origin.relative_to(source)
        baseline = paths["baseline"] / relative
        baseline.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(origin.as_uri() + "?mode=ro", uri=True, timeout=2)) as live, \
                closing(sqlite3.connect(baseline)) as target:
            live.backup(target)
        snapshots[str(relative)] = {"source_path": str(origin), "bytes": baseline.stat().st_size,
                                   "sha256": file_sha(baseline)}
    if not args.prepared_from:
        shutil.copytree(paths["baseline"], paths["candidate"])
    else:
        for name, root in paths.items():
            for owner in ("sponsor", "complement"):
                path = root / owner / "queue.sqlite3"
                if path.is_file():
                    snapshots[f"{name}/{owner}"] = {"path": str(path.resolve()), "bytes": path.stat().st_size,
                                                     "sha256_before_setup": file_sha(path)}
    context = {"fixed_clock": now.isoformat(), "current_stock_ids": current_ids,
               "official_delisted_ids": sorted(delisted_ids), "captured_live_status": status}
    correction_requests = []
    if args.include_correction_candidates:
        plan, plan_sha = finmind_corrections._load_plan(source / 'complement', 'complement')
        correction_requests = [request for request in finmind_corrections._expanded_requests(plan)
                               if request['owner'] == 'complement']
        if not correction_requests:
            raise ValueError('nonempty_real_correction_plan_required')
        context['correction_plan_sha256'] = plan_sha
        context['correction_requests'] = correction_requests
    atomic_write_json(args.fixture / f"{args.output.stem}-context.json", context)
    connections = {}
    with ExitStack() as stack:
        for name, root in paths.items():
            connections[name] = {}
            for owner in ("sponsor", "complement"):
                path = root / owner / "queue.sqlite3"
                if not path.is_file():
                    continue
                connection = stack.enter_context(closing(sqlite3.connect(path, timeout=10)))
                connections[name][owner] = connection
                # Source queues remain untouched. A future recheck reconstructs
                # only this candidate's absent/present physical index surface.
                connection.execute(f"DROP INDEX IF EXISTS {RELEASE_RESERVE_INDEX}")
                if args.include_us_publication_recheck:
                    connection.execute(f"DROP INDEX IF EXISTS {US_PUBLICATION_INDEX}")
                if args.include_periodic_preparation:
                    connection.execute(f"DROP INDEX IF EXISTS {PERIODIC_INDEX}")
                connection.execute("PRAGMA journal_mode=WAL").fetchall()
                # Match the native pipeline's claim table setup without
                # recovering/removing another work item's existing claim.
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS finmind_object_prefetch_claims "
                    "(dataset TEXT,data_id TEXT,partition TEXT,prior_state TEXT,"
                    "PRIMARY KEY(dataset,data_id,partition))"
                )
                finmind_runtime.optimize_queue(connection)
                connection.commit()
                if name == "candidate":
                    allocated = connection.execute("PRAGMA page_count").fetchone()[0] - connection.execute("PRAGMA freelist_count").fetchone()[0]
                    started, cpu = time.perf_counter(), time.process_time()
                    ensure_release_reserve_index(connection)
                    if args.include_us_publication_recheck:
                        ensure_us_publication_index(connection)
                    if args.include_periodic_preparation:
                        ensure_periodic_index(connection)
                    finmind_runtime.optimize_queue(connection)
                    connection.commit()
                    setup[owner] = {"wall_seconds": time.perf_counter() - started,
                                    "cpu_seconds": time.process_time() - cpu,
                                    "allocated_index_bytes": (connection.execute("PRAGMA page_count").fetchone()[0]
                                      - connection.execute("PRAGMA freelist_count").fetchone()[0] - allocated)
                                      * connection.execute("PRAGMA page_size").fetchone()[0],
                                    "indexed_rows": connection.execute("SELECT count(*) FROM tasks WHERE "
                                                     + RELEASE_RESERVE_PREDICATE).fetchone()[0]}
                    if args.include_us_publication_recheck:
                        setup[owner]['publication_indexed_rows'] = connection.execute(
                            'SELECT COUNT(*) FROM tasks WHERE ' + US_PUBLICATION_PREDICATE).fetchone()[0]
                    if args.include_periodic_preparation:
                        setup[owner]['periodic_indexed_rows'] = connection.execute(
                            'SELECT COUNT(*) FROM tasks WHERE ' + PERIODIC_PREDICATE).fetchone()[0]
        old = connections["baseline"]["complement"]
        task_groups = {
            "history": [worker.Task(*row) for row in old.execute(
                "SELECT dataset,data_id,partition,kind,priority,state FROM tasks WHERE priority>0 AND state='complete' "
                "ORDER BY dataset,data_id,partition LIMIT ?", (args.write_tasks,))],
            "release": [worker.Task(*row) for row in old.execute(
                "SELECT dataset,data_id,partition,kind,priority,state FROM tasks WHERE priority=0 AND kind!='derived' "
                "AND state='complete' ORDER BY dataset,data_id,partition LIMIT ?", (args.write_tasks,))],
        }
        if any(len(tasks) != args.write_tasks for tasks in task_groups.values()):
            raise ValueError("insufficient_verified_real_write_tasks")
        stack.enter_context(patch.object(worker, "_now", return_value=now))
        stack.enter_context(patch.object(worker, "_snapshot_ids", return_value=current_ids))
        stack.enter_context(patch.object(worker, "_official_delisted_ids", return_value=delisted_ids))
        variants = ("baseline", "automatic", "candidate") if args.compare_automatic else tuple(paths)
        variant_paths = {name: paths["candidate" if name == "automatic" else name] for name in variants}
        def planner(name):
            return guarded_planner() if name == "candidate" else nullcontext()
        release_plans = {}
        for name, root in variant_paths.items():
            with planner(name):
                release_plans[name] = reservation_plans(root, now)
        reference = None
        for repeat in range(args.repeats):
            for name in (*variants, *reversed(variants)):
                # Include the whole native reservation function, its metadata
                # reads, cohort gates, exact next task/frontiers and JSON fsync.
                wall, cpu = time.perf_counter(), time.process_time()
                with planner(name):
                    reservation = scheduling.incremental_reservation(variant_paths[name], now)
                reservation_wall, reservation_cpu = time.perf_counter() - wall, time.process_time() - cpu
                connection = connections["candidate" if name == "automatic" else name]["complement"]
                changes = connection.total_changes
                publication_rows = None
                publication_seconds = None
                periodic_rows = None
                periodic_seconds = None
                correction_rows = None
                correction_seconds = None
                if args.include_us_publication_recheck:
                    source = worker.supplemental.SOURCES['USStockPriceMinute']
                    publication_started = time.perf_counter()
                    if name == 'baseline':
                        publication_rows = connection.execute(
                            "SELECT data_id,partition FROM tasks WHERE dataset=? AND state='complete' AND rows>0 "
                            'AND julianday(last_attempt_at_utc)<julianday(partition)+1+(?-8)/24.0+?/1440.0',
                            ('USStockPriceMinute', source.release_hour, source.release_minute)).fetchall()
                    else:
                        publication_rows = worker.supplemental.unfinalized_us_observations(
                            connection, 'USStockPriceMinute', source)
                    publication_rows.sort()
                    publication_seconds = time.perf_counter() - publication_started
                if args.include_periodic_preparation:
                    periodic_started = time.perf_counter()
                    if name == 'baseline':
                        periodic_rows = connection.execute(
                            'SELECT dataset,data_id,partition,kind,state,last_attempt_at_utc,next_attempt_at_utc '
                            "FROM tasks WHERE state IN ('complete','observed_empty') "
                            "AND (kind='snapshot' OR (kind='year' AND partition=?))",
                            (str(now.astimezone(scheduling.TAIPEI).year),)).fetchall()
                    else:
                        periodic_rows = worker._periodic_refresh_tasks(connection, now.astimezone(scheduling.TAIPEI).year)
                    periodic_rows.sort()
                    periodic_seconds = time.perf_counter() - periodic_started
                if args.include_correction_candidates:
                    correction_started = time.perf_counter()
                    correction_rows = []
                    for request in correction_requests:
                        if name == 'baseline':
                            cursor = connection.execute('SELECT * FROM tasks WHERE dataset=?', (request['dataset'],))
                            columns = [column[0] for column in cursor.description]
                            tasks = [dict(zip(columns, row)) for row in cursor.fetchall()]
                            matched = [task for task in tasks if finmind_corrections._matches(task, request, now)]
                        else:
                            matched = finmind_corrections._matching_tasks(connection, request, now)
                        matched.sort(key=lambda task: (task['dataset'], task['data_id'], task['partition']))
                        correction_rows.append(matched)
                    correction_seconds = time.perf_counter() - correction_started
                result = worker._status(connection, variant_paths[name] / "complement", state="running",
                    last=status.get("last_task"), delegated=frozenset(status.get("delegated_to_sponsor", ())))
                elapsed, used = time.perf_counter() - wall, time.process_time() - cpu
                complete = {"reservation": reservation, "status": result}
                if publication_rows is not None:
                    complete['publication_rechecks'] = publication_rows
                if periodic_rows is not None:
                    complete['periodic_refreshes'] = periodic_rows
                if correction_rows is not None:
                    complete['correction_candidates'] = correction_rows
                parity = digest(complete)
                reference = reference or parity
                if parity != reference or connection.total_changes != changes or reservation["queue_errors"]:
                    raise ValueError("full_control_output_or_readonly_contract_changed")
                atomic_write_json(args.fixture / f"{args.output.stem}-{name}-output-{repeat}-{len(trials)}.json",
                                  complete)
                trials.append({"variant": name, "repeat": repeat, "wall_seconds": elapsed, "cpu_seconds": used,
                    "reservation_wall_seconds": reservation_wall, "reservation_cpu_seconds": reservation_cpu,
                    "publication_recheck_seconds": publication_seconds,
                    "publication_recheck_rows": len(publication_rows) if publication_rows is not None else None,
                    "periodic_preparation_seconds": periodic_seconds,
                    "periodic_refresh_rows": len(periodic_rows) if periodic_rows is not None else None,
                    "correction_candidate_seconds": correction_seconds,
                    "correction_candidate_rows": sum(map(len, correction_rows)) if correction_rows is not None else None,
                    "whole_output_sha256": parity, "queue_changes": connection.total_changes - changes})
            for name in (("baseline", "candidate") if repeat % 2 == 0 else ("candidate", "baseline")):
                for group, tasks in task_groups.items():
                    writes.append({"variant": name, "repeat": repeat, "group": group,
                                   **claim_release(connections[name]["complement"], tasks)})
        if code_before != {str(path.resolve()): file_sha(path) for path in code_files}:
            raise ValueError("code_changed_during_measurement")
        keys = ("wall_seconds", "cpu_seconds", "reservation_wall_seconds", "reservation_cpu_seconds")
        if args.include_us_publication_recheck:
            keys += ('publication_recheck_seconds',)
        if args.include_periodic_preparation:
            keys += ('periodic_preparation_seconds',)
        if args.include_correction_candidates:
            keys += ('correction_candidate_seconds',)
        medians = {name: {key: statistics.median(row[key] for row in trials if row["variant"] == name)
                         for key in keys} for name in variants}
        plans = {name: {owner: [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + AGGREGATE_SQL)]
                       for owner, connection in owners.items()} for name, owners in connections.items()}
        report = {"schema_version": 1, "state": "measurement_complete", "observed_at_utc": now.isoformat(),
            "scope": "fixed_real_two_owner_queues_full_reservation_status_and_durable_claim_release",
            "exact_output_parity": True, "code_stable": True, "provider_calls": 0, "production_queue_mutations": 0,
            "download_throughput_improvement_proven": False, "source_queue": str(source_queues[1]),
            "fixture": str(args.fixture.resolve()), "snapshots": snapshots, "index_setup": setup,
            "code_sha256": code_before, "control_medians": medians, "trials": trials,
            "claim_release_trials": writes, "status_plans": plans, "reservation_plans": release_plans,
            "whole_output_sha256": reference,
            "control_wall_improvement_observed": medians["candidate"]["wall_seconds"] < medians["baseline"]["wall_seconds"],
            "source_workflow_unchanged": True}
        report['includes_us_publication_recheck'] = args.include_us_publication_recheck
        report['includes_periodic_preparation'] = args.include_periodic_preparation
        report['includes_correction_candidates'] = args.include_correction_candidates
        report["prepared_from"] = str(args.prepared_from.resolve()) if args.prepared_from else None
        report["captured_context"] = str((args.fixture / f"{args.output.stem}-context.json").resolve())
        atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("state", "exact_output_parity", "control_medians",
                                                  "control_wall_improvement_observed")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
