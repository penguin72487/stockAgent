"""Read-only ABBA measurement of bounded catalog-child membership lookups.

This measures one lookup batch in one SQLite snapshot, not archive downloads,
catalog reconstruction, cold storage, or end-to-end service latency.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.download_openbb_archive import _write_json_atomic  # noqa: E402
from scripts.monitor_openbb_archive import (  # noqa: E402
    _active_plan_token,
    _open_read_only,
)


def benchmark_membership(
    connection: sqlite3.Connection,
    *,
    plan_token: str,
    task_ids: list[str],
    query_timeout: float = 30.0,
) -> dict:
    if not 1 <= len(task_ids) <= 512 or query_timeout <= 0:
        raise ValueError("require 1..512 task IDs and a positive query timeout")
    marks = ",".join("?" for _ in task_ids)
    queries = {
        "baseline": (
            "SELECT task_id FROM tasks WHERE active=1 AND plan_token=? "
            f"AND task_id IN ({marks})",
            (plan_token, *task_ids),
        ),
        "candidate": (
            f"SELECT task_id,active,plan_token FROM tasks WHERE task_id IN ({marks})",
            tuple(task_ids),
        ),
    }
    plans = {
        variant: [
            row["detail"]
            for row in connection.execute("EXPLAIN QUERY PLAN " + sql, parameters)
        ]
        for variant, (sql, parameters) in queries.items()
    }
    samples = []
    accepted = None
    for variant in ("baseline", "candidate", "candidate", "baseline"):
        sql, parameters = queries[variant]
        started, cpu = time.perf_counter(), time.process_time()
        connection.set_progress_handler(
            lambda: int(time.perf_counter() - started > query_timeout), 10000
        )
        try:
            rows = connection.execute(sql, parameters).fetchall()
            members = {
                str(row["task_id"])
                for row in rows
                if variant == "baseline"
                or (row["active"] == 1 and row["plan_token"] == plan_token)
            }
            wall_seconds, cpu_seconds = (
                time.perf_counter() - started,
                time.process_time() - cpu,
            )
        finally:
            connection.set_progress_handler(None, 0)
        if accepted is not None and members != accepted:
            raise ValueError("lookup variants disagree within the same source snapshot")
        accepted = members
        samples.append(
            {
                "variant": variant,
                "wall_seconds": wall_seconds,
                "cpu_seconds": cpu_seconds,
                "matched_tasks": len(members),
            }
        )
    return {
        "schema_version": 1,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "claim_boundary": "One bounded lookup batch; warm OS; not full service or p95.",
        "source_snapshot_transaction": connection.in_transaction,
        "source_query_only": bool(
            connection.execute("PRAGMA query_only").fetchone()[0]
        ),
        "source_total_changes": connection.total_changes,
        "sqlite_version": sqlite3.sqlite_version,
        "plan_token": plan_token,
        "sample_task_ids": task_ids,
        "query_plans": plans,
        "samples": samples,
        "results_identical": True,
        "median_wall_seconds": {
            variant: statistics.median(
                row["wall_seconds"] for row in samples if row["variant"] == variant
            )
            for variant in queries
        },
    }


def run(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("data_openBB"))
    parser.add_argument("--endpoint", default="regulators.sec.symbol_map")
    parser.add_argument("--query-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    connection = _open_read_only(args.source_root / "_state" / "openbb_archive.sqlite3")
    try:
        connection.execute("BEGIN")
        plan = _active_plan_token(connection)
        if plan is None:
            raise ValueError("manifest has no active plan")
        task_ids = [
            str(row[0])
            for row in connection.execute(
                "SELECT task_id FROM tasks WHERE active=1 AND plan_token=? "
                "AND status='success' AND endpoint=? LIMIT 512",
                (plan, args.endpoint),
            )
        ]
        result = benchmark_membership(
            connection,
            plan_token=plan,
            task_ids=task_ids,
            query_timeout=args.query_timeout,
        )
        result["source_manifest"] = str(
            args.source_root / "_state" / "openbb_archive.sqlite3"
        )
    finally:
        connection.close()
    _write_json_atomic(args.output, result)
    return result


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False))
