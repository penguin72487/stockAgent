"""Paired full ETA replay on private, online-backup copies; no provider calls.

The source owner keeps writing. Each queue/ledger is frozen independently with
SQLite's backup API, as in the production estimator's separate read snapshots.
The fixture is not an acquisition receipt or an atomic cross-owner release.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402

MODULES = ("finmind_queue_indexes", "finmind_runtime", "acquisition_policy", "finmind_eta_work", "finmind_eta_telemetry", "finmind_eta")
DBS = ("request_traffic.sqlite3", "sponsor/queue.sqlite3", "complement/queue.sqlite3",
       "sponsor/update_observations.sqlite3", "complement/update_observations.sqlite3")
JSONS = ("account_status.json", "status.json", "worker_status.json", "calendar.json",
         "calendar_refresh.json", "sponsor/status.json", "sponsor/worker_status.json",
         "complement/status.json", "complement/worker_status.json")


def prepare(source: Path, fixture: Path, reference_root: Path | None = None) -> dict:
    if fixture.exists():
        raise ValueError("fixture already exists; preserve previous evidence")
    fixture.mkdir(parents=True, mode=0o700)
    if shutil.disk_usage(fixture).free < 30 * 2**30 + sum(
        (source / name).stat().st_size for name in DBS if (source / name).is_file()
    ):
        raise ValueError("insufficient space for bounded private copies")
    references = fixture / "reference"
    references.mkdir(mode=0o700)
    for name in MODULES:
        shutil.copy2((reference_root or ROOT / "downloader") / f"{name}.py", references / f"{name}.py")
    local = fixture / "data"
    local.mkdir(mode=0o700)
    for name in DBS:
        path, target = source / name, local / name
        if not path.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as src:
            with closing(sqlite3.connect(target)) as dest:
                # Pin this read snapshot; a busy source must not keep restarting
                # the multi-GiB backup forever. Bound its lifetime as well.
                src.execute("BEGIN")
                src.execute("SELECT count(*) FROM sqlite_master").fetchone()
                deadline = time.monotonic() + 120
                def progress(_status, _remaining, _total):
                    if time.monotonic() > deadline:
                        raise TimeoutError("bounded_eta_snapshot_timeout")
                src.backup(dest, pages=512, progress=progress, sleep=0.005)
    for name in JSONS:
        path, target = source / name, local / name
        if path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(path, target)
    # Only bounded snapshot heads; never copy historical Parquet or credentials.
    for dataset in ("TaiwanStockInfoWithWarrant",):
        paths = list((source / "receipts" / dataset).glob("*.json"))
        if len(paths) > 4000:
            raise ValueError("snapshot receipt inventory exceeds fixture budget")
        for path in paths:
            target = local / "receipts" / dataset / path.name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(path, target)
    files = {str(path.relative_to(fixture)): sha256_file(path)
             for path in sorted(fixture.rglob("*")) if path.is_file()}
    manifest = {"schema_version": 1, "now": datetime.now(UTC).isoformat(), "files": files,
                "source": str(source.resolve()), "sqlite_version": sqlite3.sqlite_version,
                "scope": "separate_readonly_queue_snapshots_not_atomic_source_release"}
    atomic_write_json(fixture / "manifest.json", manifest)
    return manifest


def worker(fixture: Path, variant: str, evidence_output: Path | None = None,
           data_root: Path | None = None, *, include_wake: bool = False,
           candidate_reference: Path | None = None) -> dict:
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    now = datetime.fromisoformat(manifest["now"])
    with ExitStack() as stack:
        reference = fixture / 'reference' if variant == 'baseline' else candidate_reference
        if reference is not None:
            for name in MODULES:
                full_name = f"downloader.{name}"
                spec = importlib.util.spec_from_file_location(
                    full_name, reference / f"{name}.py")
                module = importlib.util.module_from_spec(spec)
                # Preserve production-relative config/calendar resolution.
                # Loading the frozen source from scratch must not redirect its
                # __file__-derived repository root into the benchmark fixture.
                module.__file__ = str(ROOT / 'downloader' / f'{name}.py')
                stack.enter_context(patch.dict(sys.modules, {full_name: module}))
                spec.loader.exec_module(module)
                import downloader
                stack.enter_context(patch.object(downloader, name, module, create=True))
        from downloader import acquisition_policy, finmind_eta

        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        # The old spare-work cache takes its own clock; replay the same instant.
        stack.enter_context(patch.object(acquisition_policy, "datetime", FrozenDatetime))
        captured = []
        stack.enter_context(patch.object(finmind_eta, "atomic_write_json",
                                        side_effect=lambda _path, value: captured.append(value)))
        started, cpu = time.perf_counter(), time.process_time()
        data = data_root or fixture / "data"
        estimate = finmind_eta.snapshot_finmind_estimate(data, now=now)
        eta_elapsed, eta_cpu = time.perf_counter() - started, time.process_time() - cpu
        wake_delays = {}
        if include_wake:
            from downloader.finmind_runtime import next_cycle_delay
            wake_delays = {owner: next_cycle_delay(data / owner, now, 3600)
                           for owner in ("sponsor", "complement")}
        elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu
        if len(captured) != 1 or captured[0]["estimate"] != estimate:
            raise ValueError("full estimate publication was not observed")
        payload = json.dumps({"estimate": captured[0], "wake_delays": wake_delays},
                             sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
        if evidence_output is not None:
            atomic_write_json(evidence_output, captured[0])
        peak = next(int(line.split()[1]) * 1024 for line in Path("/proc/self/status").read_text().splitlines()
                    if line.startswith("VmHWM:"))
        return {"variant": variant, "elapsed_seconds": elapsed, "cpu_seconds": cpu,
                "eta_elapsed_seconds": eta_elapsed, "eta_cpu_seconds": eta_cpu,
                "wake_elapsed_seconds": elapsed - eta_elapsed, "wake_delays": wake_delays,
                "peak_rss_bytes": peak, "output_sha256": hashlib.sha256(payload).hexdigest(),
                "state": estimate["state"], "dataset_count": len(captured[0]["workload"]["datasets"])}


def queue_index_candidate(fixture: Path, target: Path) -> dict:
    """Physical indexes on disposable copies; never initialize a source owner."""
    from downloader.finmind_queue_indexes import ensure_eta_read_indexes
    from downloader.finmind_runtime import optimize_queue

    needed = sum((fixture / "data" / owner / "queue.sqlite3").stat().st_size
                 for owner in ("sponsor", "complement")
                 if (fixture / "data" / owner / "queue.sqlite3").is_file())
    if shutil.disk_usage(target.parent).free < 30 * 2**30 + needed:
        raise ValueError("insufficient_space_for_eta_index_comparison")
    def copy_private(path, dest):
        # Shared read-only ledgers already belong to this immutable fixture.
        # Only queues need independent physical indexes/write-cost trials.
        if Path(path).suffix == '.sqlite3' and Path(path).name != 'queue.sqlite3':
            Path(dest).symlink_to(Path(path).resolve())
            return dest
        return shutil.copy2(path, dest)
    shutil.copytree(fixture / "data", target, copy_function=copy_private)
    setup = {}
    for owner in ("sponsor", "complement"):
        path = target / owner / "queue.sqlite3"
        if not path.is_file():
            continue
        with closing(sqlite3.connect(path)) as connection:
            pages = connection.execute("PRAGMA page_count").fetchone()[0]
            used = pages - connection.execute("PRAGMA freelist_count").fetchone()[0]
            changes = connection.total_changes
            started, cpu = time.perf_counter(), time.process_time()
            ensure_eta_read_indexes(connection)
            optimize_queue(connection)
            connection.commit()
            setup[owner] = {"wall_seconds": time.perf_counter() - started,
                            "cpu_seconds": time.process_time() - cpu,
                            "allocated_bytes": (connection.execute("PRAGMA page_count").fetchone()[0]
                                                - connection.execute("PRAGMA freelist_count").fetchone()[0] - used)
                                               * connection.execute("PRAGMA page_size").fetchone()[0],
                            "task_changes": connection.total_changes - changes}
            if connection.total_changes != changes:
                raise ValueError("physical_index_setup_changed_task_rows")
    return setup


def queue_write_trials(baseline: Path, candidate: Path) -> list[dict]:
    """Native durable claim/release cost after read parity; restore exact rows."""
    from scripts.benchmark_finmind_status import claim_release
    from downloader import download_finmind_complement as owner

    results = []
    with ExitStack() as stack:
        connections = {name: stack.enter_context(closing(sqlite3.connect(
            root / "complement/queue.sqlite3", factory=owner.StatusConnection)))
            for name, root in (("baseline", baseline), ("candidate", candidate))}
        for connection in connections.values():
            connection.execute("PRAGMA journal_mode=WAL")
        tasks = [owner.Task(*row) for row in connections["baseline"].execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset='USStockPriceMinute' AND state='complete' ORDER BY data_id,partition LIMIT 32")]
        if len(tasks) != 32:
            raise ValueError("insufficient_real_tasks_for_eta_index_write_check")
        for name in ("baseline", "candidate", "candidate", "baseline"):
            results.append({"variant": name, **claim_release(connections[name], tasks)})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=ROOT / "data_finmind")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evidence-output", type=Path, help="Optional private full replay evidence for diagnosing parity")
    parser.add_argument("--data-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--reference-root", type=Path, help="Frozen pre-change modules for --prepare-only")
    parser.add_argument("--candidate-reference-root", type=Path,
                        help="Optional private proposed modules; this benchmark never deploys them")
    parser.add_argument("--include-wake", action="store_true", help="Also replay both native next-cycle boundary calculations")
    parser.add_argument("--queue-read-indexes", action="store_true",
                        help="Compare owner-maintained ETA indexes on temporary private copies, including claim/release")
    args = parser.parse_args()
    if args.prepare_only:
        result = prepare(args.source, args.fixture, args.reference_root)
        print(json.dumps({"files": len(result["files"]), "fixture": str(args.fixture)}))
        return
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker, args.evidence_output, args.data_root,
                                include_wake=args.include_wake, candidate_reference=args.candidate_reference_root)))
        return
    if args.output is None:
        parser.error("--output required for paired replay")
    if args.output.exists():
        parser.error("new output required; preserve previous evidence")
    manifest = json.loads((args.fixture / "manifest.json").read_bytes())
    code_paths = [ROOT / "downloader" / f"{name}.py" for name in MODULES]
    code_paths.extend((Path(__file__).resolve(), ROOT / "downloader/download_finmind_complement.py"))
    sources = {str(path): sha256_file(path) for path in code_paths}
    candidate_sources = ({str(ROOT / 'downloader' / f'{name}.py'):
                          sha256_file(args.candidate_reference_root / f'{name}.py') for name in MODULES}
                         if args.candidate_reference_root else {})
    trials = []
    with tempfile.TemporaryDirectory(prefix="finmind-eta-index-", dir=args.fixture.parent) as scratch:
        candidate = Path(scratch) / "data"
        setup = queue_index_candidate(args.fixture, candidate) if args.queue_read_indexes else {}
        for variant in ("baseline", "candidate", "candidate", "baseline") * 2:
            command = [sys.executable, str(Path(__file__).resolve()), "--fixture",
                       str(args.fixture), "--worker", variant]
            if args.queue_read_indexes and variant == "candidate":
                command += ["--data-root", str(candidate)]
            if args.include_wake:
                command += ["--include-wake"]
            if variant == 'candidate' and args.candidate_reference_root:
                command += ['--candidate-reference-root', str(args.candidate_reference_root.resolve())]
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=180, cwd=ROOT)
            trial = json.loads(result.stdout)
            trials.append(trial)
            print(json.dumps(trial), flush=True)
        if any(sha256_file(args.fixture / name) != digest for name, digest in manifest["files"].items()):
            raise ValueError("private_source_snapshot_changed_during_read_replay")
        writes = queue_write_trials(args.fixture / "data", candidate) if args.queue_read_indexes else []
    if len({row["output_sha256"] for row in trials}) != 1:
        raise ValueError("complete ETA/workload/telemetry/admission output parity failed")
    if sources != {str(path): sha256_file(path) for path in code_paths}:
        raise ValueError("code changed during paired measurement")
    if candidate_sources and candidate_sources != {
            str(ROOT / 'downloader' / f'{name}.py'): sha256_file(args.candidate_reference_root / f'{name}.py')
            for name in MODULES}:
        raise ValueError('private_proposal_changed_during_measurement')
    medians = {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
                         for key in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes")}
               for variant in ("baseline", "candidate")}
    scope = "full_offline_finmind_eta_and_wake_snapshot" if args.include_wake else "full_offline_finmind_eta_snapshot"
    contract = ("finmind_full_eta_queue_indexes_v2" if args.include_wake else "finmind_full_eta_queue_indexes_v1")
    atomic_write_json(args.output, {"state": "accepted_bounded_replay", "scope": scope,
                                  "trials": trials, "medians": medians, "exact_full_output_parity": True,
                                  "source_manifest": manifest, "code_sha256": sources,
                                  "candidate_code_sha256": candidate_sources,
                                  "code_stable": True, "exact_output_parity": True,
                                  "source_queue": str(args.source.resolve() / "complement/queue.sqlite3"),
                                  "measurement_contract": ('finmind_full_eta_cohort_ranges_v1' if candidate_sources else
                                                           contract if args.queue_read_indexes else None),
                                  "queue_index_setup": setup, "claim_release_trials": writes,
                                  "read_snapshot_preserved": True,
                                  "provider_calls": 0, "production_queue_writes": 0,
                                  "limits": ["nonexclusive_host_load", "os_page_cache_not_flushed",
                                             "separate_owner_snapshots", "not_full_acquisition_throughput"]})


if __name__ == "__main__":
    main()
