"""Offline complete-index admission/control cycle with a preserved old oracle.

Both cases retain full initial/final inventories, all symbols/sessions, durable
index commits, admission decisions, hole candidates and readback. Only the
discarded intermediate full inventories are removed. No SDK/network calls or
native source writes are authorized by this comparison.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import UTC, date, datetime
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import download_finlab_market_intraday as market
from scripts.benchmark_finlab_effective_partitions import digest, effective_digest, file_sha


def load_reference(path: Path):
    spec = importlib.util.spec_from_file_location("finlab_preserved_admission_reference", path)
    if spec is None or spec.loader is None:
        raise ValueError("missing preserved reference module")
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)
    return reference


def snapshot_attempts(index: Path, source: Path, target: Path) -> dict:
    """Pin the exact receipt inputs; don't read changing native attempts in A/B."""
    identities = []
    with closing(sqlite3.connect(index)) as connection:
        market._create_effective_partitions_view(connection)
        for symbol, day in connection.execute("SELECT symbol,trade_date FROM effective_partitions "
                "WHERE kind='tw_minute' AND status NOT IN "
                "('downloaded_unverified_for_pit','derived_unverified_for_pit','derived_no_regular_trades',"
                "'verified_closed_date','verified_no_trade','partition_not_ready') ORDER BY symbol,trade_date"):
            native = market._partition_paths(source, f"tw_minute:{symbol}", date.fromisoformat(day))[1]
            relative = native.relative_to(source)
            if not native.exists():
                identities.append({"path": str(relative), "present": False})
                continue
            before = file_sha(native)
            private = target / relative
            private.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(native, private)
            if file_sha(native) != before or file_sha(private) != before:
                raise RuntimeError("attempt receipt changed while pinning input")
            identities.append({"path": str(relative), "present": True, "sha256": before})
    return {"count": len(identities), "identities": identities, "sha256": digest(identities)}


def benchmark(source: Path, public: Path, output: Path, reference_path: Path) -> dict:
    source, public = source.resolve(), public.resolve()
    output, reference_path = output.resolve(), reference_path.resolve()
    if output == source or source in output.parents or output in source.parents:
        raise ValueError("private output must be separate from source")
    if output.exists() or not output.parent.is_dir():
        raise ValueError("fresh output with an existing parent required")
    native_index = source / "intraday/market_index.sqlite3"
    if shutil.disk_usage(output.parent).free < 32 * 1024**3 + native_index.stat().st_size * 5:
        raise RuntimeError("private benchmark disk reserve unavailable")
    code_paths = [Path(market.__file__), Path(__file__), reference_path,
                  ROOT / "scripts/benchmark_finlab_effective_partitions.py"]
    code_identity = {str(path.relative_to(ROOT)): file_sha(path) for path in code_paths}
    reference = load_reference(reference_path)
    if hasattr(reference, "_minute_admission"):
        raise ValueError("reference must precede the minute-only optimization")
    output.mkdir()
    fixed = output / "fixed-index.sqlite3"
    with closing(sqlite3.connect(native_index.as_uri() + "?mode=ro", uri=True)) as native:
        with closing(sqlite3.connect(fixed)) as private:
            native.backup(private)
    fixed_sha = file_sha(fixed)
    stamp = datetime.now(UTC)
    policy = market.load_policy()

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return stamp.astimezone(tz) if tz is not None else stamp.replace(tzinfo=None)

    cases = []
    with ExitStack() as guards:
        for module in (market, reference):
            guards.enter_context(patch.object(module, "datetime", FrozenDateTime))
            for name in ("quota_room_mb", "fetch_partition", "fetch_partitions", "credential_available"):
                guards.enter_context(patch.object(module, name, side_effect=AssertionError("offline provider forbidden")))
        import requests
        guards.enter_context(patch.object(requests.Session, "request", side_effect=AssertionError("offline network forbidden")))
        preparation = time.monotonic()
        universe = market.load_universe(public)
        daily = market.annotate_daily_close_coverage(source, universe)
        days = market.session_days(public, start=date(2003, 8, 1), end=stamp.astimezone(market.TAIPEI).date())
        if not universe or not days:
            raise ValueError("missing canonical full-market inputs")
        pinned_source = output / "pinned-attempts"
        pinned_source.mkdir()
        receipt_inputs = snapshot_attempts(fixed, source, pinned_source)
        preparation_seconds = time.monotonic() - preparation
        context = {"universe": universe, "days": [str(day) for day in days], "daily": daily,
                   "attempt_receipts": receipt_inputs, "quota_policy": policy.fingerprint}
        atomic_write_json(output / "fixed-input-context.json", context, durable=True)
        with closing(sqlite3.connect(fixed)) as connection:
            partition_count = connection.execute("SELECT COUNT(*) FROM partitions").fetchone()[0]
            writes = connection.execute("SELECT kind,symbol,trade_date,status,rows,parquet_bytes "
                "FROM partitions ORDER BY kind,symbol,trade_date LIMIT 32").fetchall()
        if len(writes) != 32:
            raise ValueError("complete comparison requires 32 indexed durable writes")
        expected_output = expected_effective = None
        for number, variant in enumerate(("reference", "candidate", "candidate", "reference")):
            trial = output / f"case-{number}-{variant}.sqlite3"
            shutil.copyfile(fixed, trial)
            module = reference if variant == "reference" else market
            with closing(module._open_index(trial)) as connection:
                def summary():
                    return module._summarize(connection, universe, days, started=stamp, attempts=32,
                        successes=32, state="offline_control_benchmark", source_root=pinned_source,
                        daily_price_coverage=daily, quota_policy=policy)
                started, cpu = time.monotonic(), time.process_time()
                first = summary()
                module._atomic_json(output / f"inventory-{number}-initial.json", first)
                decisions, readbacks = [], []
                admission_seconds = 0.0
                for check in range(8):
                    for kind, symbol, text, status, rows, size in writes[check*4:(check+1)*4]:
                        module._record(connection, kind, symbol, date.fromisoformat(text), status, rows, size)
                        readbacks.append(connection.execute("SELECT * FROM effective_partitions "
                            "WHERE kind=? AND symbol=? AND trade_date=?", (kind, symbol, text)).fetchall())
                    admission_start = time.monotonic()
                    decision = (summary()["minute_priority"] if variant == "reference" else
                        module._minute_admission(connection, universe, days, source_root=pinned_source, quota_policy=policy))
                    admission_seconds += time.monotonic() - admission_start
                    decisions.append(decision)
                    module._atomic_json(output / f"admission-{number}-{check}.json", decision)
                final = summary()
                holes = module._unchecked_known_minutes(connection, universe, days,
                    first_observed=final["by_kind"]["tw_minute"]["first_data_date"], limit=256)
                module._atomic_json(output / f"inventory-{number}-final.json", final)
                wall_seconds, cpu_seconds = time.monotonic() - started, time.process_time() - cpu
                full_output = digest({"initial": first, "final": final, "decisions": decisions,
                                      "holes": holes, "readbacks": readbacks})
                relation = effective_digest(connection)
                if expected_output is not None and (full_output != expected_output or relation != expected_effective):
                    raise AssertionError("full inventory/admission/holes/readbacks/effective relation mismatch")
                expected_output, expected_effective = full_output, relation
                case = {"variant": variant, "wall_seconds": wall_seconds, "cpu_seconds": cpu_seconds,
                    "admission_seconds": admission_seconds, "admission_checks": 8, "durable_writes": 32,
                    "published_full_inventories": 2, "full_output_sha256": full_output,
                    "effective_rows": relation[0], "effective_sha256": relation[1]}
                cases.append(case)
                print(json.dumps({key: case[key] for key in ("variant", "wall_seconds", "cpu_seconds")}), flush=True)
    if file_sha(fixed) != fixed_sha:
        raise AssertionError("immutable complete snapshot changed")
    if {str(path.relative_to(ROOT)): file_sha(path) for path in code_paths} != code_identity:
        raise AssertionError("benchmark code changed during comparison")
    reference_wall = statistics.median(case["wall_seconds"] for case in cases if case["variant"] == "reference")
    candidate_wall = statistics.median(case["wall_seconds"] for case in cases if case["variant"] == "candidate")
    reference_cpu = statistics.median(case["cpu_seconds"] for case in cases if case["variant"] == "reference")
    candidate_cpu = statistics.median(case["cpu_seconds"] for case in cases if case["variant"] == "candidate")
    result = {"contract": "finlab_complete_index_minute_admission_trial_v1", "state": "accepted_bounded_comparison",
        "observed_at_utc": stamp.isoformat(), "code_identity": code_identity, "source_snapshot_sha256": fixed_sha,
        "source_partition_rows": partition_count, "universe_symbols": len(universe), "sessions": len(days),
        "target_start": str(days[0]), "target_end": str(days[-1]), "input_preparation_seconds": preparation_seconds,
        "full_input_context_sha256": digest(context), "cases": cases, "full_outputs_equal": True,
        "reference_median_seconds": reference_wall, "candidate_median_seconds": candidate_wall,
        "bounded_control_cycle_speed_ratio": reference_wall / candidate_wall,
        "reference_median_cpu_seconds": reference_cpu, "candidate_median_cpu_seconds": candidate_cpu,
        "provider_calls": 0, "native_source_writes": 0, "production_deployment_proven": False,
        "full_download_speedup_claim": False, "host_nonexclusive": True, "os_cache_not_cleared": True}
    atomic_write_json(output / "comparison.json", result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=market.DEFAULT_OUTPUT)
    parser.add_argument("--public-root", type=Path, default=market.DEFAULT_PUBLIC_ROOT)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = benchmark(args.source_root, args.public_root, args.output, args.reference)
    print(json.dumps({key: result[key] for key in ("state", "reference_median_seconds", "candidate_median_seconds", "bounded_control_cycle_speed_ratio")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
