"""ABBA archive-stage benchmark using bounded, read-only real source samples.

Each fresh worker scans all sampled file paths. The private fixture contains
copies and only a path-ownership projection, never a replacement archive.
The file-audit mode includes the canonical footer and first-group provenance
checks. No mode measures the complete archive or the complete service workflow.
"""

from __future__ import annotations

import argparse
import ast
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
import textwrap
import threading
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
from scripts import monitor_openbb_archive as monitor  # noqa: E402
from downloader import download_openbb_archive as archive  # noqa: E402


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


def _process_tree_rss(pid: int, proc_root: Path = Path("/proc")) -> int:
    """Sum current RSS, including children created by any parent thread.

    Summed RSS double-counts shared pages: it is a conservative sampled
    footprint, not unique physical memory or an exact peak between samples.
    A child may exit while /proc is being read; ignore that disappearing entry.
    """
    pending, seen, total = [pid], set(), 0
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        directory = proc_root / str(current)
        try:
            for line in (directory / "status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1]) * 1024
                    break
            # Linux records children on the thread which created them, not
            # necessarily the process's main thread.
            tasks = list((directory / "task").iterdir())
        except (OSError, ValueError):
            continue
        for task in tasks:
            try:
                pending.extend(int(value) for value in (task / "children").read_text().split())
            except (OSError, ValueError):
                continue
    return total


class _ProcessTreeSampler:
    def __init__(self, interval: float = 0.02) -> None:
        self.interval = interval
        self.peak_rss_bytes = 0
        self.samples = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="benchmark-process-tree", daemon=True)

    def _sample(self) -> None:
        self.peak_rss_bytes = max(self.peak_rss_bytes, _process_tree_rss(os.getpid()))
        self.samples += 1

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._sample()

    def start(self) -> None:
        self._sample()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        self._sample()


def _reference_reconciliation(revision: str):
    source = subprocess.run(
        ["git", "show", f"{revision}:downloader/download_openbb_archive.py"],
        cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
    ).stdout
    owner = next(node for node in ast.parse(source).body
                 if isinstance(node, ast.ClassDef) and node.name == "Manifest")
    method = next(node for node in owner.body
                  if isinstance(node, ast.FunctionDef) and node.name == "quarantine_terminal_output_shards")
    implementation = textwrap.dedent("\n".join(source.splitlines()[method.lineno - 1:method.end_lineno]))
    return implementation, hashlib.sha256(implementation.encode()).hexdigest()


def _worker(root: Path, variant: str, mode: str = "ownership", baseline_revision: str = "HEAD", audit_workers: int = 4) -> dict:
    connection = _open_read_only(root / "_state" / "owners.sqlite3")
    reference_sha = None
    if mode == "reconcile":
        connection.close()
        connection = sqlite3.connect(root / "_state" / "owners.sqlite3")
        connection.row_factory = sqlite3.Row
        # Parse the large original module in the parent, not just in A workers:
        # otherwise AST allocation pollutes A's VmHWM before the timer starts.
        implementation = (root / "_state/reference_reconcile.py").read_text()
        namespace = dict(vars(archive))
        exec(compile(implementation, "pinned-original-reconciliation", "exec"), namespace)
        reference_sha = hashlib.sha256(implementation.encode()).hexdigest()
        reconciliation = (namespace["quarantine_terminal_output_shards"] if variant == "baseline"
                          else archive.Manifest.quarantine_terminal_output_shards)
        manifest = archive.Manifest.__new__(archive.Manifest)
        manifest.connection, manifest.path, manifest.show_progress = connection, root / "_state/owners.sqlite3", False
        # Canonical startup creates this partial exception index in the schema
        # migration. Charge that one-time work to every B trial, not just B1;
        # reset only this private fixture before starting the timed workflow.
        connection.execute("DROP INDEX IF EXISTS idx_tasks_output_contract_violations_v1")
        connection.commit()
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    child_cpu_before = children.ru_utime + children.ru_stime
    sampler = _ProcessTreeSampler()
    started, cpu = time.perf_counter(), time.process_time()
    sampler.start()
    try:
        if variant == "baseline":
            # Same SQLite index, scan and ownership rules; only the resolver
            # changes. The old active-only set was not a like-for-like oracle.
            monitor._resolve_ownership_paths = monitor._resolve_ownership_paths_reference
        if mode == "reconcile":
            if variant == "candidate":
                connection.execute(archive.OUTPUT_CONTRACT_INDEX_SQL)
                connection.commit()
            scanned, quarantined = reconciliation(manifest, plan_token="benchmark")
            result = {"scanned_parquet_files": scanned, "non_success_parquet_files": quarantined,
                      "quarantined_files": quarantined, "reference_method_sha256": reference_sha}
        else:
            audit = monitor._audit_success_files if mode == "file-audit" else _audit_parquet_ownership
            options = {"workers": 1 if variant == "baseline" else audit_workers} if mode == "file-audit" else {}
            result = audit(connection, output_dir=root, plan_token="benchmark", show_progress=False, **options)
        sampler.stop()
        parent_cpu = time.process_time() - cpu
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        child_cpu = children.ru_utime + children.ru_stime - child_cpu_before
        result.update(
            variant=variant,
            elapsed_seconds=time.perf_counter() - started,
            cpu_seconds=parent_cpu,
            waited_child_cpu_seconds=child_cpu,
            workflow_tree_cpu_seconds=parent_cpu + child_cpu,
            process_peak_rss_bytes=_proc_runtime_metrics(os.getpid())["rss_peak_bytes"],
            rusage_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            sampled_tree_peak_rss_bytes=sampler.peak_rss_bytes,
            process_tree_samples=sampler.samples,
            process_tree_sampling_interval_seconds=sampler.interval,
        )
        return result
    finally:
        if sampler._thread.is_alive():
            sampler.stop()
        connection.close()


def run(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("data_openBB"))
    parser.add_argument("--endpoint", default="regulators.sec.filing_headers")
    parser.add_argument("--sample-files", type=int, default=20000)
    parser.add_argument("--maximum-fixture-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument(
        "--scratch-parent", type=Path, default=Path("artifacts/benchmarks")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker-root", type=Path)
    parser.add_argument("--variant", choices=("baseline", "candidate"))
    parser.add_argument("--mode", choices=("ownership", "file-audit", "reconcile"), default="ownership")
    parser.add_argument("--baseline-revision", default="HEAD")
    parser.add_argument("--audit-workers", type=int, choices=range(1, 33), default=4)
    args = parser.parse_args(argv)
    if args.worker_root:
        if args.variant is None:
            parser.error("worker requires --variant")
        return _worker(args.worker_root, args.variant, args.mode, args.baseline_revision, args.audit_workers)
    if args.sample_files <= 0:
        parser.error("--sample-files must be positive")
    connection = _open_read_only(args.source_root / "_state" / "openbb_archive.sqlite3")
    try:
        plan = _active_plan_token(connection)
        rows = connection.execute(
            "SELECT task_id,output_path,endpoint,scope_key,kwargs_json,selected_provider,rows "
            "FROM tasks WHERE active=1 "
            "AND plan_token=? AND status='success' AND endpoint=? LIMIT ?",
            (plan, args.endpoint, args.sample_files),
        ).fetchall()
    finally:
        connection.close()
    if not rows:
        raise ValueError("no accepted source samples for this plan and endpoint")
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    sources = [(Path(row["output_path"]), str(row["task_id"])) for row in rows]
    source_bytes = sum(path.stat().st_size for path, _ in sources)
    if source_bytes > args.maximum_fixture_bytes:
        raise ValueError("sample exceeds bounded fixture byte budget")
    if shutil.disk_usage(args.scratch_parent).free < 2 * source_bytes + 2 * 1024 ** 3:
        raise ValueError("insufficient headroom for bounded private fixture")
    before = [(path, _identity(path), _digest(path)) for path, _ in sources]
    implementation = ROOT / "scripts/monitor_openbb_archive.py"
    implementation_sha = _digest(implementation)
    code_paths = (implementation, Path(__file__).resolve(), ROOT / "downloader/download_openbb_archive.py")
    code_sha = {str(path.relative_to(ROOT)): _digest(path) for path in code_paths}
    baseline_revision = subprocess.run(
        ["git", "rev-parse", "--verify", f"{args.baseline_revision}^{{commit}}"],
        cwd=ROOT, capture_output=True, text=True, check=True, timeout=30,
    ).stdout.strip()
    with tempfile.TemporaryDirectory(
        prefix="openbb-ownership-benchmark-", dir=args.scratch_parent.resolve()
    ) as temporary:
        root = Path(temporary)
        (root / "_state").mkdir()
        if args.mode == "reconcile":
            reference, _ = _reference_reconciliation(baseline_revision)
            (root / "_state/reference_reconcile.py").write_text(reference)
        shadow = sqlite3.connect(root / "_state" / "owners.sqlite3")
        copies = []
        try:
            shadow.execute(
                "CREATE TABLE tasks(output_path TEXT, active INTEGER, plan_token TEXT, status TEXT, "
                "task_id TEXT PRIMARY KEY,endpoint TEXT,scope_key TEXT,kwargs_json TEXT,selected_provider TEXT,rows INTEGER)"
            )
            for (source, task_id), row in zip(sources, rows, strict=True):
                if re.fullmatch(r"[0-9a-f]{64}", task_id) is None:
                    raise ValueError(
                        "source task ID is not a canonical archive identity"
                    )
                endpoint = str(row["endpoint"])
                if re.fullmatch(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*", endpoint) is None:
                    raise ValueError("source endpoint is not a canonical archive path")
                target = root / "data" / endpoint.replace(".", "/") / task_id[:2] / source.name
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise ValueError("sample source output paths collide")
                shutil.copyfile(source, target)
                copies.append((target, _digest(target)))
                shadow.execute(
                    "INSERT INTO tasks VALUES (?,1,'benchmark','success',?,?,?,?,?,?)",
                    (str(target), task_id, row["endpoint"], row["scope_key"],
                     row["kwargs_json"], row["selected_provider"], row["rows"]),
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
                    "--mode", args.mode,
                    "--baseline-revision", baseline_revision,
                    "--audit-workers", str(args.audit_workers),
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
            if args.mode == "file-audit" and (not trial["passed"] or trial["checked_files"] != len(sources)):
                raise ValueError("full sampled file audit failed")
            # Timings and OS resources are not semantic counters. Compare all
            # counts, source checks and issue samples rather than just passed.
            semantic = {
                key: value for key, value in trial.items()
                if key in {"checked_files", "checked_rows", "passed", "issue_samples", "quarantined_files"}
                or key.endswith(("_files", "_tasks", "_paths"))
            }
            if trials and semantic != trials[0]["semantic_counters"]:
                raise ValueError("sampled archive audit semantic parity failed")
            trial["semantic_counters"] = semantic
            trials.append(trial)
        if any(_digest(path) != digest for path, digest in copies):
            raise ValueError("private sampled files changed during benchmark")
        if any(
            _identity(path) != identity or _digest(path) != digest
            for path, identity, digest in before
        ):
            raise ValueError("source changed during benchmark; reject timings")
        if any(_digest(path) != code_sha[str(path.relative_to(ROOT))] for path in code_paths):
            raise ValueError("implementation changed during benchmark; reject timings")
        source_digest = hashlib.sha256()
        for path, identity, digest in before:
            source_digest.update(json.dumps([str(path), identity, digest]).encode())
        result = {
            "schema_version": 5,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "ownership": "path_ownership_stage_only_not_full_archive_or_service",
                "file-audit": "complete_sampled_file_audit_not_full_archive_or_service",
                "reconcile": "complete_sampled_startup_reconciliation_not_full_archive_or_service",
            }[args.mode],
            "mode": args.mode,
            "candidate_file_audit_workers": args.audit_workers if args.mode == "file-audit" else None,
            "baseline": ("pinned_repository_original_reconciliation" if args.mode == "reconcile"
                         else "same_private_sqlite_ownership_with_per_file_Path_resolve"),
            "candidate": ("canonical_task_id_primary_key_with_spillable_legacy_fallback" if args.mode == "reconcile"
                          else "batch_scoped_certified_ordinary_directories_with_reference_fallback"),
            "code_sha256": code_sha,
            "baseline_revision": baseline_revision,
            "endpoint": args.endpoint,
            "source_plan_token": plan,
            "sample_files": len(sources),
            "sample_source_bytes": source_bytes,
            "private_sample_content_unchanged": True,
            "source_filenames_preserved": True,
            "candidate_contract_index_build_included_each_trial": args.mode == "reconcile",
            "source_sample_identity_sha256": source_digest.hexdigest(),
            "source_content_and_identity_unchanged_during_trials": True,
            "canonical_implementation_sha256": implementation_sha,
            "cache_state": "warm_os_cache_fresh_workers_fresh_private_index_each_candidate",
            "peak_rss_clock": "parent_VmHWM_diagnostic_only_and_20ms_sampled_process_tree_RSS_sum",
            "resource_measurement": {
                "cpu": "parent_process_time_plus_waited_children_rusage_delta_including_spawn_and_IPC",
                "tree_rss": "20ms_sampled_RSS_sum_double_counts_shared_pages_not_exact_physical_peak",
                "unwaited_children": "small_resource_tracker_may_outlive_pool_not_in_waited_child_CPU",
            },
            "trials": trials,
            "medians": {
                variant: {
                    field: statistics.median(
                        trial[field] for trial in trials if trial["variant"] == variant
                    )
                    for field in (
                        "elapsed_seconds",
                        "cpu_seconds",
                        "workflow_tree_cpu_seconds",
                        "sampled_tree_peak_rss_bytes",
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
