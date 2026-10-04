"""ABBA ownership-stage benchmark using bounded, read-only real source samples.

Each fresh worker scans all sampled file paths. The private fixture contains
copies and only a path-ownership projection, never a replacement archive.
This does not audit Parquet contents or measure the complete service workflow.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monitor_openbb_archive import (  # noqa: E402
    _active_plan_token,
    _audit_parquet_ownership,
    _open_read_only,
    _proc_runtime_metrics,
)


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    value = path.stat()
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _worker(root: Path, variant: str) -> dict:
    connection = _open_read_only(root / "_state" / "owners.sqlite3")
    started, cpu = time.perf_counter(), time.process_time()
    try:
        if variant == "baseline":
            # Exact pre-change active-success path-set ownership operation.
            paths = {
                Path(row[0]).resolve()
                for row in connection.execute(
                    "SELECT output_path FROM tasks WHERE active=1 "
                    "AND plan_token='benchmark' AND status='success'"
                )
            }
            scanned = bad = 0
            for path in (root / "data").rglob("*.parquet"):
                scanned += 1
                bad += path.resolve() not in paths
            result = {
                "scanned_parquet_files": scanned,
                "non_success_parquet_files": bad,
            }
        else:
            result = _audit_parquet_ownership(
                connection, output_dir=root, plan_token="benchmark", show_progress=False
            )
        result.update(
            variant=variant,
            elapsed_seconds=time.perf_counter() - started,
            cpu_seconds=time.process_time() - cpu,
            process_peak_rss_bytes=_proc_runtime_metrics(os.getpid())["rss_peak_bytes"],
            rusage_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        )
        return result
    finally:
        connection.close()


def run(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("data_openBB"))
    parser.add_argument("--endpoint", default="regulators.sec.filing_headers")
    parser.add_argument("--sample-files", type=int, default=20000)
    parser.add_argument(
        "--scratch-parent", type=Path, default=Path("artifacts/benchmarks")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker-root", type=Path)
    parser.add_argument("--variant", choices=("baseline", "candidate"))
    args = parser.parse_args(argv)
    if args.worker_root:
        if args.variant is None:
            parser.error("worker requires --variant")
        return _worker(args.worker_root, args.variant)
    if args.sample_files <= 0:
        parser.error("--sample-files must be positive")
    connection = _open_read_only(args.source_root / "_state" / "openbb_archive.sqlite3")
    try:
        plan = _active_plan_token(connection)
        rows = connection.execute(
            "SELECT task_id,output_path FROM tasks WHERE active=1 "
            "AND plan_token=? AND status='success' AND endpoint=? LIMIT ?",
            (plan, args.endpoint, args.sample_files),
        ).fetchall()
    finally:
        connection.close()
    if not rows:
        raise ValueError("no accepted source samples for this plan and endpoint")
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    sources = [(Path(row["output_path"]), str(row["task_id"])) for row in rows]
    before = [(path, _identity(path), _digest(path)) for path, _ in sources]
    implementation = ROOT / "scripts/monitor_openbb_archive.py"
    implementation_sha = _digest(implementation)
    with tempfile.TemporaryDirectory(
        prefix="openbb-ownership-benchmark-", dir=args.scratch_parent.resolve()
    ) as temporary:
        root = Path(temporary)
        (root / "_state").mkdir()
        shadow = sqlite3.connect(root / "_state" / "owners.sqlite3")
        try:
            shadow.execute(
                "CREATE TABLE tasks(output_path TEXT, active INTEGER, plan_token TEXT, status TEXT)"
            )
            for source, task_id in sources:
                if re.fullmatch(r"[0-9a-f]{64}", task_id) is None:
                    raise ValueError(
                        "source task ID is not a canonical archive identity"
                    )
                target = root / "data" / task_id[:2] / f"{task_id}.parquet"
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                shadow.execute(
                    "INSERT INTO tasks VALUES (?,1,'benchmark','success')",
                    (str(target),),
                )
            shadow.commit()
        finally:
            shadow.close()
        trials = []
        for variant in ("baseline", "candidate", "candidate", "baseline"):
            completed = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker-root",
                    str(root),
                    "--variant",
                    variant,
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
                timeout=600,
            )
            trial = json.loads(completed.stdout)
            if (
                trial["scanned_parquet_files"] != len(sources)
                or trial["non_success_parquet_files"] != 0
            ):
                raise ValueError("ownership output parity failed")
            trials.append(trial)
        if any(
            _identity(path) != identity or _digest(path) != digest
            for path, identity, digest in before
        ):
            raise ValueError("source changed during benchmark; reject timings")
        if _digest(implementation) != implementation_sha:
            raise ValueError("implementation changed during benchmark; reject timings")
        source_digest = hashlib.sha256()
        for path, identity, digest in before:
            source_digest.update(json.dumps([str(path), identity, digest]).encode())
        result = {
            "schema_version": 1,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "scope": "path_ownership_stage_only_not_full_archive_or_service",
            "endpoint": args.endpoint,
            "source_plan_token": plan,
            "sample_files": len(sources),
            "source_sample_identity_sha256": source_digest.hexdigest(),
            "source_content_and_identity_unchanged_during_trials": True,
            "canonical_implementation_sha256": implementation_sha,
            "cache_state": "warm_os_cache_fresh_workers_fresh_private_index_each_candidate",
            "peak_rss_clock": "worker_proc_VmHWM_not_fork_inherited_parent_rusage",
            "trials": trials,
            "medians": {
                variant: {
                    field: statistics.median(
                        trial[field] for trial in trials if trial["variant"] == variant
                    )
                    for field in (
                        "elapsed_seconds",
                        "cpu_seconds",
                        "process_peak_rss_bytes",
                    )
                }
                for variant in ("baseline", "candidate")
            },
        }
    if any(
        _identity(path) != identity or _digest(path) != digest
        for path, identity, digest in before
    ):
        raise ValueError("source changed after private fixture cleanup")
    result["source_content_unchanged_after_fixture_cleanup"] = True
    if args.output:
        from downloader.download_openbb_archive import _write_json_atomic

        _write_json_atomic(args.output, result)
    return result


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True))
