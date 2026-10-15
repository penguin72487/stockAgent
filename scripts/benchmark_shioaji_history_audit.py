"""Compare canonical plan/coverage/persist on a frozen, copied real archive.

The provider query loop, login, native SDK and publication are deliberately not
run. This is a full local archive audit, not end-to-end provider throughput.
Each source receipt/data pair is signature-fenced while copied; the fixture is
not asserted to be a simultaneous snapshot of an actively changing provider.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import UTC, datetime
import hashlib
import importlib.util
from itertools import islice
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests  # noqa: E402

from downloader import download_shioaji_historical_market_data as current  # noqa: E402
from downloader import shioaji_history_repair as repair  # noqa: E402
from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

COLLECTOR = ROOT / "downloader/download_shioaji_historical_market_data.py"
CODE_FILES = (
    COLLECTOR, ROOT / "downloader/shioaji_history_repair.py",
    ROOT / "downloader/download_shioaji_tw_kbars.py", ROOT / "downloader/artifact_io.py",
    ROOT / "downloader/common.py", ROOT / "stockagent/data/taifex_sessions.py",
    Path(__file__),
)


def signature(path: Path) -> tuple[int, ...] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def bounded_new_path(path: Path) -> Path:
    if path.exists() or path.is_symlink():
        raise FileExistsError("preserve previous benchmark; choose a new path")
    path = path.resolve()
    if not path.is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        raise ValueError("private benchmark must remain under artifacts/benchmarks")
    if path.exists() or path.is_symlink():
        raise FileExistsError("preserve previous benchmark; choose a new path")
    return path


def copy_group(paths: tuple[Path, ...], source: Path, target: Path) -> list[dict]:
    """Retry only a changing local pair; never issue a provider request."""
    for attempt in range(3):
        before = [signature(path) for path in paths]
        copied = []
        try:
            for path, stamp in zip(paths, before, strict=True):
                if stamp is None:
                    # A prior retry might have copied a file that the source
                    # subsequently removed. Never retain that private stale copy.
                    (target / path.relative_to(source)).unlink(missing_ok=True)
                    continue
                relative = path.relative_to(source)
                output = target / relative
                output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                shutil.copyfile(path, output)
                copied.append({"path": str(relative), "source_signature": stamp,
                               "fixture_signature": signature(output),
                               "sha256": sha256_file(output), "size": stamp[2]})
            if before == [signature(path) for path in paths]:
                return copied
        except FileNotFoundError:
            pass
        if attempt == 2:
            raise RuntimeError(f"source pair changed during copy: {paths[0].relative_to(source)}")
    raise AssertionError("unreachable")


def kbar_groups(source: Path, rows):
    for row in rows:
        for start, end in current.iter_date_chunks(row.begin_date, row.end_date, 29):
            data, receipt = current._kbar_paths(source, row, start, end)
            yield receipt, data


def tick_groups(source: Path, archive: Path, rows, activity):
    for row in rows:
        dates = set(current.observed_tick_dates(archive, row, chunk_days=29)) | activity.exact_dates(row)
        for day in sorted(dates):
            data, receipt = current._tick_paths(source, row, day)
            yield receipt, data


def prepare(source: Path, activity_source: Path, fixture: Path, revision: str) -> dict:
    fixture = bounded_new_path(fixture)
    if shutil.disk_usage(fixture.parent).free < 30 * 2**30:
        raise ValueError("retain at least 30 GiB headroom before fixture preparation")
    fixture.mkdir(mode=0o700)
    started, cpu = time.perf_counter(), time.process_time()
    baseline = subprocess.run(
        ["git", "show", f"{revision}:downloader/download_shioaji_historical_market_data.py"],
        capture_output=True, check=True, cwd=ROOT,
    ).stdout
    (fixture / "reference.py").write_bytes(baseline)  # Exact generated Git comparator, not a manual edit.
    archive = fixture / "archive"
    archive.mkdir(mode=0o700)
    activity_stamp = signature(activity_source)
    shutil.copyfile(activity_source, fixture / "activity.parquet")
    if activity_stamp is None or signature(activity_source) != activity_stamp:
        raise RuntimeError("official activity changed during fixture copy")
    counts = {"files": 0, "bytes": 0, "receipts": 0, "data_files": 0, "availability": 0}
    catalog = fixture / "files.jsonl"
    seen = set()
    with catalog.open("x", encoding="utf-8") as output, ThreadPoolExecutor(max_workers=4) as executor:
        def unique_groups(groups):
            for paths in groups:
                key = str(paths[0])
                if key in seen:
                    continue
                seen.add(key)
                yield paths

        def copy_groups(groups):
            pending = unique_groups(groups)
            while batch := list(islice(pending, 128)):
                for group in executor.map(lambda paths: copy_group(paths, source, archive), batch):
                    for item in group:
                        counts["files"] += 1
                        counts["bytes"] += item["size"]
                        counts["receipts"] += item["path"].endswith("/receipt.json")
                        counts["data_files"] += item["path"].endswith("/data.parquet")
                        counts["availability"] += item["path"].startswith("availability/")
                        if counts["files"] > 300_000 or counts["bytes"] > 8 * 2**30:
                            raise ValueError("bounded fixture exceeded 300k files or 8 GiB")
                        output.write(json.dumps(item, sort_keys=True) + "\n")

        copy_groups([(source / "inventory/contracts.parquet",)])
        contracts = current.load_inventory(archive)
        if not contracts:
            raise ValueError("real archive inventory must be nonempty")
        activity = repair.load_futures_activity(
            fixture / "activity.parquet", start=min(row.begin_date for row in contracts),
            end=max(row.end_date for row in contracts),
        )
        # Only paths actually traversed by the canonical audit belong to this
        # workload. Old final-chunk ranges remain preserved in the live archive.
        copy_groups(kbar_groups(source, contracts))
        print(json.dumps({"snapshot_phase": "kbars", **counts}), flush=True)
        copy_groups(tick_groups(source, archive, contracts, activity))
        print(json.dumps({"snapshot_phase": "ticks", **counts}), flush=True)
        copy_groups((source / "availability" / f"{row.code}.json",) for row in contracts)
        if counts["receipts"] == 0:
            raise ValueError("real archive fixture must include receipts")
    result = {
        "schema_version": 1, "contract": "shioaji-local-history-audit-abba-v1",
        "source": str(source), "prepared_at_utc": datetime.now(UTC).isoformat(),
        "freeze_at_utc": datetime.now(UTC).isoformat(), "baseline_revision": revision,
        "baseline_sha256": sha256_file(fixture / "reference.py"),
        "catalog_sha256": sha256_file(catalog), "activity_sha256": sha256_file(fixture / "activity.parquet"),
        "activity_source_signature": activity_stamp, "contracts": len(contracts), **counts,
        "prepare_elapsed_seconds": time.perf_counter() - started,
        "prepare_cpu_seconds": time.process_time() - cpu,
        "target_scope": "all current inventory KBar chunks and verified/official tick target union",
        "source_snapshot_scope": "signature-stable copied pairs, not simultaneous global source state",
        "source_files_modified": 0, "provider_requests": 0,
    }
    atomic_write_json(fixture / "manifest.json", result)
    return result


def fixture_fence(fixture: Path, manifest: dict) -> str:
    catalog = fixture / "files.jsonl"
    if sha256_file(catalog) != manifest["catalog_sha256"]:
        raise AssertionError("fixture file catalog changed")
    digest = hashlib.sha256()
    with catalog.open(encoding="utf-8") as handle:
        for line in handle:
            item = json.loads(line)
            path = (fixture / "archive" / item["path"]).resolve()
            if not path.is_relative_to((fixture / "archive").resolve()):
                raise ValueError("invalid private fixture path")
            if list(signature(path) or ()) != item["fixture_signature"]:
                raise AssertionError("fixture bytes/signature changed")
            digest.update(line.encode())
    for name, key in (("reference.py", "baseline_sha256"), ("activity.parquet", "activity_sha256")):
        if sha256_file(fixture / name) != manifest[key]:
            raise AssertionError("fixture reference/activity changed")
    return digest.hexdigest()


def load_reference(fixture: Path):
    name = "_shioaji_history_reference"
    spec = importlib.util.spec_from_file_location(name, fixture / "reference.py")
    module = importlib.util.module_from_spec(spec)
    module.__file__ = str(COLLECTOR)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def io_counters() -> dict[str, int]:
    return {key: int(value.strip()) for line in Path("/proc/self/io").read_text().splitlines()
            for key, _, value in [line.partition(":")]}


def worker(fixture: Path, variant: str) -> dict:
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    module = load_reference(fixture) if variant == "baseline" else current
    archive = fixture / "archive"
    observed = datetime.fromisoformat(manifest["freeze_at_utc"])

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed.astimezone(tz) if tz is not None else observed.replace(tzinfo=None)

    def retry_at_snapshot(receipt, *, positive_activity=False):
        return repair.retry_due(receipt, positive_activity=positive_activity, now=observed)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("local audit attempted a live provider query")

    with ExitStack() as guards:
        guards.enter_context(patch.object(module, "datetime", FrozenDatetime))
        guards.enter_context(patch.object(module, "retry_due", retry_at_snapshot))
        guards.enter_context(patch.object(module, "shioaji_query", forbidden))
        guards.enter_context(patch.object(requests.Session, "request", forbidden))
        started, cpu, io_before = time.perf_counter(), time.process_time(), io_counters()
        rows = [replace(row, priority=-1) if row.collection == "exact_futures" else row
                for row in module.load_inventory(archive)]
        activity = repair.load_futures_activity(
            fixture / "activity.parquet", start=min(row.begin_date for row in rows),
            end=max(row.end_date for row in rows),
        )
        tasks = module.build_tasks(archive, rows, chunk_days=29, refresh_empty=True, activity=activity)
        task_digest = hashlib.sha256(json.dumps([asdict(task) for task in tasks],
                                               sort_keys=True, default=str).encode()).hexdigest()
        summaries, timings = [], []
        for query in (1, 250, 500):
            begin = time.perf_counter()
            result = module._write_summary(
                archive, rows, chunk_days=29, state="running", usage=(100, 2**31),
                progress_path=archive / "progress.json", refresh_empty=True,
                activity=activity, persist=True,
            )
            saved = json.loads((archive / "summary.json").read_bytes())
            if result != saved or result["pending_queries"] != len(tasks):
                raise AssertionError("canonical full plan/coverage/persist disagreement")
            summaries.append(hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest())
            timings.append({"query": query, "seconds": time.perf_counter() - begin})
        elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu
        io_after = io_counters()
        cache = module._valid_receipt_at_signature.cache_info()
    return {
        "variant": variant, "elapsed_seconds": elapsed, "cpu_seconds": cpu,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "io_deltas": {key: io_after[key] - io_before[key] for key in io_before},
        "contracts": len(rows), "pending_queries": len(tasks),
        "task_sha256": task_digest, "summary_sha256": summaries,
        "summary_timings": timings, "receipt_cache": cache._asdict(),
        "final_summary": result, "provider_requests": 0,
    }


def compare(fixture: Path, output: Path) -> dict:
    output = bounded_new_path(output)
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    code_sha = {str(path.relative_to(ROOT)): sha256_file(path) for path in CODE_FILES}
    before = fixture_fence(fixture, manifest)
    node_before = observe_node(fixture / "archive", fixture)
    trials = []
    for variant in ("baseline", "candidate", "candidate", "baseline"):
        started = time.perf_counter()
        result = subprocess.run(
            [sys.executable, __file__, "--fixture", str(fixture), "--worker", variant],
            capture_output=True, text=True, check=True, timeout=1200, cwd=ROOT,
        )
        trial = json.loads(result.stdout)
        trial["invocation_seconds"] = time.perf_counter() - started
        trials.append(trial)
        print(json.dumps({key: trial[key] for key in (
            "variant", "elapsed_seconds", "cpu_seconds", "peak_rss_bytes", "pending_queries",
        )}), flush=True)
    if len({row["task_sha256"] for row in trials}) != 1 or len({
        tuple(row["summary_sha256"]) for row in trials
    }) != 1:
        raise AssertionError("complete plan/summary output parity failed")
    if fixture_fence(fixture, manifest) != before or code_sha != {
        str(path.relative_to(ROOT)): sha256_file(path) for path in CODE_FILES
    }:
        raise AssertionError("code/fixture changed during paired comparison")
    result = {
        "schema_version": 1, "state": "accepted_local_audit_only",
        "generated_at_utc": datetime.now(UTC).isoformat(), "fixture": manifest,
        "scope": "inventory+official activity+full plan+three complete coverage summaries+atomic persistence",
        "excluded": ["native SDK import/login", "provider query loop", "remote latency", "publication"],
        "all_tasks_and_summaries_equal": True, "source_files_modified": 0,
        "provider_requests": 0, "live_services_restarted": False,
        "os_cache_controlled": False, "code_sha256": code_sha, "trials": trials,
        "node_before": node_before, "node_after": observe_node(fixture / "archive", fixture),
        "runtime": {"python": sys.version, "polars": current.pl.__version__,
                    "polars_threads": current.pl.thread_pool_size()},
        "affinity": sorted(os.sched_getaffinity(0)),
        "medians": {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
                               for key in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes", "invocation_seconds")}
                    for variant in ("baseline", "candidate")},
    }
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=ROOT / "data_tw_shioaji_history")
    parser.add_argument("--activity", type=Path, default=ROOT / repair.DEFAULT_FUTURES_ACTIVITY)
    parser.add_argument("--baseline-revision", default="aacc15783907ccb83093dbd8ae78f0745d927a8d")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.prepare_only:
        result = prepare(args.source, args.activity, args.fixture, args.baseline_revision)
        print(json.dumps(result))
    elif args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
    elif args.output is None:
        parser.error("--output required for paired comparison")
    else:
        result = compare(args.fixture, args.output)
        print(json.dumps({"state": result["state"], "medians": result["medians"]}))


if __name__ == "__main__":
    main()
