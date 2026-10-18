"""Offline, full-index FinLab inventory/write comparison; never calls providers.

One SQLite online backup pins the complete native partition index. Both cases
retain the same full catalog, calendar, fields, source choice and durability.
Only private copies and receipts are written. This is not API throughput proof.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack, closing
import cProfile
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import pstats
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import download_finlab_market_intraday as market


REFERENCE_VIEW = """CREATE TEMP VIEW effective_partitions AS
    SELECT kind,symbol,trade_date,status,rows,parquet_bytes,checked_at_utc,
           'finlab_api' AS source_kind FROM partitions WHERE kind='tw_tick'
    UNION ALL
    SELECT 'tw_minute',symbol,trade_date,status,rows,parquet_bytes,checked_at_utc,
           CASE WHEN kind='tw_minute' THEN 'finlab_api' ELSE 'derived_from_tw_tick' END
    FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY symbol,trade_date ORDER BY
      CASE WHEN status IN ('downloaded_unverified_for_pit','derived_unverified_for_pit',
        'derived_no_regular_trades','verified_closed_date','verified_no_trade') THEN 0 ELSE 1 END,
      CASE WHEN kind='tw_minute' THEN 0 ELSE 1 END) AS preference
      FROM partitions WHERE kind IN ('tw_minute','tw_minute_derived'))
    WHERE preference=1"""


def candidate_view(connection: sqlite3.Connection) -> None:
    """Exercise the canonical candidate, never a benchmark-only SQL substitute."""
    connection.execute("DROP VIEW IF EXISTS temp.effective_partitions")
    market._create_effective_partitions_view(connection)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def file_sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def effective_digest(connection) -> tuple[int, str]:
    result = hashlib.sha256()
    count = 0
    for row in connection.execute(
            "SELECT * FROM effective_partitions ORDER BY kind,symbol,trade_date"):
        result.update(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode() + b"\n")
        count += 1
    return count, result.hexdigest()


def coverage_reference():
    """Load the pre-change function from tracked HEAD, not a proxy query."""
    source = subprocess.run(['git', 'show', 'HEAD:scripts/download_finlab_market_intraday.py'],
                            cwd=ROOT, capture_output=True, text=True, check=True, timeout=5).stdout
    tree = ast.parse(source)
    selected = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_coverage_phases')
    namespace = dict(vars(market))
    exec(compile(ast.Module(body=[selected], type_ignores=[]), '<tracked-coverage-reference>', 'exec'), namespace)
    return namespace['_coverage_phases'], hashlib.sha256(ast.unparse(selected).encode()).hexdigest()


def benchmark(source_root: Path, public_root: Path, output: Path, *, pairs: int = 2,
              candidate: str = 'effective_view') -> dict:
    if pairs not in (1, 2, 3):
        raise ValueError("one to three matched pairs required")
    # Retain all evidence and never overwrite a previous trial or native root.
    if output.resolve().is_relative_to(source_root.resolve()) or output.resolve().is_relative_to(public_root.resolve()):
        raise ValueError("benchmark output must be separate from source roots")
    source = source_root / "intraday/market_index.sqlite3"
    code_paths = [Path(market.__file__), Path(__file__)]
    code_identity = {str(path.relative_to(ROOT)): file_sha(path) for path in code_paths}
    native_phases = market._coverage_phases
    reference_phases, oracle_sha = coverage_reference() if candidate == 'coverage' else (native_phases, None)
    if shutil.disk_usage(output.parent).free < 32 * 1024**3 + source.stat().st_size * 4:
        raise RuntimeError("private benchmark disk reserve unavailable")
    output.mkdir(parents=False, exist_ok=False)
    index = output / "fixed-index.sqlite3"
    with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as native:
        with closing(sqlite3.connect(index)) as private:
            native.backup(private)
    fixed_sha = file_sha(index)
    native_schema = []
    with closing(sqlite3.connect(index)) as private:
        native_schema = private.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").fetchall()
        partitions = private.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
        writes = private.execute("SELECT kind,symbol,trade_date,status,rows,parquet_bytes "
                                 "FROM partitions ORDER BY kind,symbol,trade_date LIMIT 64").fetchall()
    stamp = datetime.now(UTC)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return stamp.astimezone(tz) if tz is not None else stamp.replace(tzinfo=None)

    cases, profile_rows = [], []
    with ExitStack() as guards:
        guards.enter_context(patch.object(market, "datetime", FrozenDateTime))
        for name in ("quota_room_mb", "fetch_partition", "fetch_partitions", "credential_available"):
            guards.enter_context(patch.object(market, name, side_effect=AssertionError("offline provider forbidden")))
        import requests
        guards.enter_context(patch.object(requests.Session, "request", side_effect=AssertionError("offline network forbidden")))
        # Canonical whole-market inputs are built once, without narrowing scope.
        preparation = time.monotonic()
        universe = market.load_universe(public_root)
        daily = market.annotate_daily_close_coverage(source_root, universe)
        days = market.session_days(public_root, start=date(2003, 8, 1), end=stamp.astimezone(market.TAIPEI).date())
        if not days or not universe:
            raise ValueError("missing canonical full-market input")
        preparation_seconds = time.monotonic() - preparation
        from scripts.benchmark_finlab_minute_admission import snapshot_attempts
        pinned_source = output / 'pinned-attempts'
        pinned = snapshot_attempts(index, source_root, pinned_source)
        atomic_write_json(output / "fixed-input-context.json", {
            "observed_at_utc": stamp.isoformat(), "universe": universe,
            "days": [str(day) for day in days], "daily": daily,
        }, durable=True)
        first = None
        summaries, effective = {}, {}
        order = ["reference", "candidate", "candidate", "reference"][:pairs * 2] if pairs < 3 else ["reference", "candidate", "candidate", "reference", "reference", "candidate"]
        for number, variant in enumerate(order):
            trial = output / f"case-{number}-{variant}.sqlite3"
            shutil.copyfile(index, trial)
            phases = reference_phases if variant == 'reference' else native_phases
            with closing(market._open_index(trial)) as connection, patch.object(market, '_coverage_phases', phases):
                connection.execute("DROP VIEW IF EXISTS temp.effective_partitions")
                if variant == "candidate" or candidate == 'coverage':
                    candidate_view(connection)
                else:
                    connection.execute(REFERENCE_VIEW)
                started, cpu = time.monotonic(), time.process_time()
                # Real canonical durable upserts and immediate effective readback.
                readbacks = []
                for kind, symbol, text, status, rows, size in writes:
                    market._record(connection, kind, symbol, date.fromisoformat(text), status, rows, size)
                    readbacks.append(connection.execute("SELECT * FROM effective_partitions "
                        "WHERE kind=? AND symbol=? AND trade_date=?", (kind, symbol, text)).fetchall())
                write_seconds = time.monotonic() - started
                summary_start = time.monotonic()
                summary = market._summarize(connection, universe, days, started=stamp, attempts=64,
                    successes=64, state="offline_index_benchmark", daily_price_coverage=daily,
                    source_root=pinned_source)
                first = summary["by_kind"]["tw_minute"]["first_data_date"]
                holes = market._unchecked_known_minutes(connection, universe, days,
                    first_observed=first, limit=256)
                market._atomic_json(output / f"summary-{number}-{variant}.json", summary)
                seconds, cpu_seconds = time.monotonic() - started, time.process_time() - cpu
                summary_seconds = time.monotonic() - summary_start
                output_sha = digest({"summary": summary, "holes": holes, "readbacks": readbacks})
                summaries[variant] = output_sha
                if len(set(summaries.values())) != 1:
                    raise AssertionError("full FinLab inventory/candidates/readback output mismatch")
                count, full_sha = effective_digest(connection)
                effective[variant] = [count, full_sha]
                if len({tuple(row) for row in effective.values()}) != 1:
                    raise AssertionError("full effective partition relation mismatch")
                plans = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN SELECT * FROM effective_partitions "
                    "WHERE kind='tw_minute' AND symbol='2330' AND trade_date=?", (days[-1].isoformat(),))]
                case = {"variant": variant, "owner_wall_seconds": seconds, "owner_cpu_seconds": cpu_seconds,
                    "durable_write_and_readback_seconds": write_seconds,
                    "complete_inventory_candidates_publish_seconds": summary_seconds,
                    "full_output_sha256": output_sha, "effective_rows": count,
                    "effective_sha256": full_sha, "query_plan": plans}
                cases.append(case)
                # Profile outside measured cases, preserving all actual work.
                if number == 0:
                    profiler = cProfile.Profile()
                    profiler.runcall(market._summarize, connection, universe, days, started=stamp,
                        attempts=64, successes=64, state="offline_index_benchmark",
                        daily_price_coverage=daily, source_root=pinned_source)
                    profiler.dump_stats(str(output / "reference-full-summary.prof"))
                    stats = pstats.Stats(profiler)
                    profile_rows = [{"file": key[0], "line": key[1], "function": key[2],
                        "calls": value[1], "own_cpu_seconds": value[2], "cumulative_seconds": value[3]}
                        for key, value in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)[:20]]
                print(json.dumps({key: case[key] for key in ("variant", "owner_wall_seconds", "owner_cpu_seconds")}), flush=True)
    if file_sha(index) != fixed_sha:
        raise AssertionError("immutable complete snapshot changed")
    if {str(path.relative_to(ROOT)): file_sha(path) for path in code_paths} != code_identity:
        raise AssertionError("benchmark code changed during comparison")
    reference = statistics.median(case["owner_wall_seconds"] for case in cases if case["variant"] == "reference")
    candidate_seconds = statistics.median(case["owner_wall_seconds"] for case in cases if case["variant"] == "candidate")
    result = {"contract": "finlab_full_partition_inventory_trial_v1", "state": "accepted_bounded_comparison",
        "observed_at_utc": stamp.isoformat(), "source_snapshot_sha256": fixed_sha,
        "source_partition_rows": partitions, "source_schema": native_schema, "code_identity": code_identity,
        "universe_symbols": len(universe), "sessions": len(days), "target_start": days[0].isoformat(),
        "target_end": days[-1].isoformat(), "input_preparation_seconds": preparation_seconds,
        "full_input_context_sha256": digest({"universe": universe, "days": [str(day) for day in days], "daily": daily}),
        "cases": cases, "reference_median_seconds": reference, "candidate_median_seconds": candidate_seconds,
        "bounded_full_inventory_speed_ratio": reference / candidate_seconds, "full_outputs_equal": True,
        "reference_profile": profile_rows, "provider_calls": 0, "native_source_writes": 0,
        "production_deployment_proven": False, "full_download_speedup_claim": False,
        "candidate": candidate, "reference_function_sha256": oracle_sha, "pinned_attempts": pinned,
        "os_cache_not_cleared": True, "host_nonexclusive": True}
    atomic_write_json(output / "comparison.json", result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=market.DEFAULT_OUTPUT)
    parser.add_argument("--public-root", type=Path, default=market.DEFAULT_PUBLIC_ROOT)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pairs", type=int, choices=(1, 2, 3), default=2)
    parser.add_argument('--candidate', choices=('effective_view', 'coverage'), default='effective_view')
    args = parser.parse_args()
    result = benchmark(args.source_root, args.public_root, args.output, pairs=args.pairs, candidate=args.candidate)
    print(json.dumps({key: result[key] for key in ("state", "reference_median_seconds", "candidate_median_seconds", "bounded_full_inventory_speed_ratio")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
