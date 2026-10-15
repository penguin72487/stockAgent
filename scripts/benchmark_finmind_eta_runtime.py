"""Paired full ETA replay on private, online-backup copies; no provider calls.

The source owner keeps writing. Each queue/ledger is frozen independently with
SQLite's backup API, as in the production estimator's separate read snapshots.
The fixture is not an acquisition receipt or an atomic cross-owner release.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
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
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402

MODULES = ("acquisition_policy", "finmind_eta_work", "finmind_eta_telemetry", "finmind_eta")
DBS = ("request_traffic.sqlite3", "sponsor/queue.sqlite3", "complement/queue.sqlite3",
       "sponsor/update_observations.sqlite3", "complement/update_observations.sqlite3")
JSONS = ("account_status.json", "status.json", "worker_status.json", "calendar.json",
         "calendar_refresh.json", "sponsor/status.json", "sponsor/worker_status.json",
         "complement/status.json", "complement/worker_status.json")


def prepare(source: Path, fixture: Path) -> dict:
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
        shutil.copy2(ROOT / "downloader" / f"{name}.py", references / f"{name}.py")
    local = fixture / "data"
    local.mkdir(mode=0o700)
    for name in DBS:
        path, target = source / name, local / name
        if not path.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as src:
            with sqlite3.connect(target) as dest:
                src.backup(dest, pages=4096, sleep=0.005)
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


def worker(fixture: Path, variant: str, evidence_output: Path | None = None) -> dict:
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    now = datetime.fromisoformat(manifest["now"])
    with ExitStack() as stack:
        if variant == "baseline":
            for name in MODULES:
                full_name = f"downloader.{name}"
                spec = importlib.util.spec_from_file_location(
                    full_name, fixture / "reference" / f"{name}.py")
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
        estimate = finmind_eta.snapshot_finmind_estimate(fixture / "data", now=now)
        elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu
        if len(captured) != 1 or captured[0]["estimate"] != estimate:
            raise ValueError("full estimate publication was not observed")
        payload = json.dumps(captured[0], sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
        if evidence_output is not None:
            atomic_write_json(evidence_output, captured[0])
        peak = next(int(line.split()[1]) * 1024 for line in Path("/proc/self/status").read_text().splitlines()
                    if line.startswith("VmHWM:"))
        return {"variant": variant, "elapsed_seconds": elapsed, "cpu_seconds": cpu,
                "peak_rss_bytes": peak, "output_sha256": hashlib.sha256(payload).hexdigest(),
                "state": estimate["state"], "dataset_count": len(captured[0]["workload"]["datasets"])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=ROOT / "data_finmind")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evidence-output", type=Path, help="Optional private full replay evidence for diagnosing parity")
    args = parser.parse_args()
    if args.prepare_only:
        result = prepare(args.source, args.fixture)
        print(json.dumps({"files": len(result["files"]), "fixture": str(args.fixture)}))
        return
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker, args.evidence_output)))
        return
    if args.output is None:
        parser.error("--output required for paired replay")
    manifest = json.loads((args.fixture / "manifest.json").read_bytes())
    sources = {name: sha256_file(ROOT / "downloader" / f"{name}.py") for name in MODULES}
    trials = []
    for variant in ("baseline", "candidate", "candidate", "baseline", "baseline", "candidate"):
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--fixture",
                                 str(args.fixture), "--worker", variant], capture_output=True,
                                text=True, check=True, timeout=180, cwd=ROOT)
        trial = json.loads(result.stdout)
        trials.append(trial)
        print(json.dumps(trial), flush=True)
    if len({row["output_sha256"] for row in trials}) != 1:
        raise ValueError("complete ETA/workload/telemetry/admission output parity failed")
    if sources != {name: sha256_file(ROOT / "downloader" / f"{name}.py") for name in MODULES}:
        raise ValueError("code changed during paired measurement")
    if any(sha256_file(args.fixture / name) != digest for name, digest in manifest["files"].items()):
        raise ValueError("private source snapshot changed during replay")
    medians = {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
                         for key in ("elapsed_seconds", "cpu_seconds", "peak_rss_bytes")}
               for variant in ("baseline", "candidate")}
    atomic_write_json(args.output, {"state": "accepted_bounded_replay", "scope": "full_offline_finmind_eta_snapshot",
                                  "trials": trials, "medians": medians, "exact_full_output_parity": True,
                                  "source_manifest": str(args.fixture / "manifest.json"), "code_sha256": sources,
                                  "provider_calls": 0, "production_queue_writes": 0,
                                  "limits": ["nonexclusive_host_load", "os_page_cache_not_flushed",
                                             "separate_owner_snapshots", "not_full_acquisition_throughput"]})


if __name__ == "__main__":
    main()
