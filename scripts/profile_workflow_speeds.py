#!/usr/bin/env python3
"""Index workflow owners and retain speed evidence without dispatching their work.

Reuse architecture/source registries, service audits, benchmark receipts and the
trainer's timing files. This collector never starts a collector, training job,
publication, order or remote command. Missing operation timings stay missing.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
from collections import Counter, OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import csv
from datetime import UTC, datetime, timedelta
import gzip
import hashlib
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
import json
import math
import os
from pathlib import Path
import re
import socket
import sqlite3
from stat import S_ISLNK, S_ISREG
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent import private_json
from stockagent.remote_build import observe_node
from stockagent.workflow_metric_tree import load_backend

CONTRACT = "stockagent_workflow_speed_evidence_v1"
TRAINING_NAMES = {
    "startup_timing.jsonl", "trainer_startup_timing.jsonl", "pre_epoch_timing.jsonl",
    "epoch_curve.jsonl", "epoch_workflow_timing.jsonl", "save_timing.json",
    "plot_timing.json", "explainability_timing.json",
}
# Full component/per-batch rows remain byte-for-byte in evidence/*.gz. Keep
# normalized epoch observations at lifecycle grain to avoid multiplying every
# epoch into hundreds of overlapping component pseudo-workflows.
EPOCH_PHASES = {
    "epoch_total_s", "epoch_wall_s_max_rank", "train_total_s_max_rank", "train_total_s",
    "val_eval_s", "test_curve_s", "fold_checkpoint_save_s", "group_checkpoint_save_s",
    "checkpoint_save_s", "scheduler_s", "progress_update_s", "curve_record_s",
    "scalar_sync_s", "cuda_sync_s", "gc_s", "epoch_wall_s", "epoch_max_rank_s", "train_max_rank_s",
}
SKIP_DIRS = {".git", "__pycache__", "node_modules", "toolchain", "code",
             "source_snapshot", "reviewed-source", "site-packages"}
TIME_TERMS = ("seconds", "elapsed_s", "wall_s", "total_s", "duration_s", "elapsed_ms", "total_ms", "median_ms", "median_seconds")
TIME_FIELD = re.compile(r"(?:^seconds$|(?:^|_)(?:" + "|".join(TIME_TERMS) + r")$)")
POSSIBLE_TIME_BYTES = (*[term.encode("ascii") for term in TIME_TERMS], b"\\", b"\x00")
JSON_CONTAINERS = (dict, list)
SENSITIVE_FIELD = re.compile(r"(?:token|password|secret|authorization|credential|api_key)", re.I)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def finite_seconds(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def category(name: str) -> str:
    name = name.lower()
    for group, words in (
        ("benchmark", ("benchmark", "profile_")),
        ("storage_delivery", ("packed", "backup", "archive", "rclone", "syncthing", "lake", "data_sync", "retire", "cache_gc", "storage", "cold_", "remote_build", "bulk_return")),
        ("presentation", ("dashboard", "monitor", "discord", "plot", "report", "explain")),
        ("inference_execution", ("signal", "inference", "simulation", "execution", "ledger", "opening", "replay", "backtest")),
        ("training", ("train", "optimizer", "gradient", "checkpoint", "ablation", "fold")),
        ("preparation", ("prepare", "panel", "feature", "tensor", "dataset", "normaliz", "eligib", "universe")),
        ("acquisition", ("download", "finmind", "finlab", "news", "tej", "shioaji", "taifex", "openbb", "quote", "capture", "collector")),
    ):
        if any(word in name for word in words):
            return group
    return "operations"


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        if all(type(row) is dict for row in rows):
            # A direct ordered row avoids DictWriter's per-row dictionary,
            # key-set difference and wrapper calls. Missing cells and nested
            # JSON retain exactly the original wire representation.
            writer = csv.writer(handle)
            writer.writerow(fields)
            writer.writerows([
                json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value := row.get(key, ""), JSON_CONTAINERS) else value
                for key in fields
            ] for row in rows)
            return
        writer = csv.DictWriter(handle, fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                             if isinstance(value, (dict, list)) else value
                             for key, value in row.items()})


def json_lines(path: Path, rows) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")


def write_evidence_manifest(path: Path, evidence: Evidence, traversal_gaps: list[dict]) -> None:
    payload = {"files": list(evidence.sources.values()), "issues": evidence.issues,
               "traversal_gaps": traversal_gaps}
    atomic_write_bytes(path, private_json.pretty_metadata_bytes(payload) + b"\n", durable=True)


class Evidence:
    def __init__(self, output: Path, *, max_file_bytes: int, max_total_bytes: int):
        self.output = output
        self.max_file_bytes, self.max_total_bytes = max_file_bytes, max_total_bytes
        self.total_bytes = 0
        self.sources: dict[str, dict] = {}
        self.issues: list[dict] = []
        # Cache only negative selections, by complete bytes. Large immutable
        # dictionaries repeat under many benchmark generations; retaining a
        # parsed object here would keep their entire payload resident.
        self.benchmark_negatives: OrderedDict[str, tuple[str, bool, str]] = OrderedDict()
        self.read_stats = Counter()
        self.inspection_timings = None
        (output / "evidence").mkdir()

    @staticmethod
    def _signature(info):
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

    def _prepare_reads(self, batch, skip, byte_limit):
        """Read bytes only. The consuming thread owns all evidence decisions."""
        prepared, remaining = [], byte_limit
        timing = self.inspection_timings
        for path in batch:
            value = None
            if path not in skip:
                try:
                    before = timing.call("stat", path.stat) if timing else path.stat()
                    if S_ISREG(before.st_mode) and before.st_size <= min(remaining, self.max_file_bytes):
                        # Bound allocations even when a producer grows a file.
                        handle = timing.call("file_open", path.open, "rb") if timing else path.open("rb")
                        with handle:
                            raw = (timing.call("bounded_read", handle.read, before.st_size + 1)
                                   if timing else handle.read(before.st_size + 1))
                        after = timing.call("stat", path.stat) if timing else path.stat()
                        remaining -= len(raw)
                        sha = (timing.call("sha256", hashlib.sha256, raw) if timing else hashlib.sha256(raw)).hexdigest()
                        value = before, after, raw, sha
                except OSError:
                    # Retry on consumption: a speculative failure must not
                    # hide a file created before its place in the sequence.
                    pass
            prepared.append((path, value))
        return prepared

    def ordered_reads(self, paths, *, workers=0, skip=(), batch_size=32, batch_bytes=2 << 20):
        """Bounded read-ahead, original consumption order and synchronous fallback.

        At most `workers` batches are pending plus one being consumed. Raw
        payloads occupy at most (workers + 1) * (batch_bytes + 1) bytes. Large,
        explicit and non-regular inputs use the original synchronous path.
        """
        if workers == 0:
            for path in paths:
                yield path, None
            return
        if not 1 <= workers <= 4 or batch_size < 1 or batch_bytes < 1:
            raise ValueError("invalid evidence prefetch bounds")
        paths, skip = iter(paths), frozenset(skip)
        pending = deque()
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="evidence-read")

        def submit():
            batch = []
            for _ in range(batch_size):
                try:
                    batch.append(next(paths))
                except StopIteration:
                    break
            if batch:
                pending.append(pool.submit(self._prepare_reads, batch, skip, batch_bytes))

        try:
            for _ in range(workers):
                submit()
            while pending:
                batch = pending.popleft().result()
                submit()
                # Pop consumed payloads so no previous batch remains resident.
                batch = deque(batch)
                while batch:
                    yield batch.popleft()
        finally:
            for future in pending:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)

    def read(self, path: Path, *, retain_if=None, prepared=None):
        """Freeze exact producer bytes; refuse changing/oversized evidence."""
        timing = self.inspection_timings
        key = str(path.absolute())
        if key in self.sources:
            row = self.sources[key]
            if row["state"] != "captured":
                return None
            snapshot = self.output / row["snapshot"]
            body = timing.call("snapshot_read", snapshot.read_bytes) if timing else snapshot.read_bytes()
            return (timing.call("gzip_decompress", gzip.decompress, body) if timing else gzip.decompress(body)), row["sha256"]
        row = {"path": key, "state": "unavailable"}
        self.sources[key] = row
        try:
            if prepared is not None:
                before, after, raw, sha = prepared
                if self._signature(before) != self._signature(after) or len(raw) != before.st_size:
                    row.update(bytes=before.st_size, state="changed_during_read")
                    self.issues.append({"path": key, "reason": row["state"]})
                    return None
                current = timing.call("stat", path.stat) if timing else path.stat()
                if self._signature(after) != self._signature(current):
                    self.read_stats["prefetch_generation_retries"] += 1
                    prepared = None
            if prepared is None:
                before = timing.call("stat", path.stat) if timing else path.stat()
            row["bytes"] = before.st_size
            if before.st_size > self.max_file_bytes or (retain_if is None and self.total_bytes + before.st_size > self.max_total_bytes):
                row["state"] = "size_budget_exceeded"
                self.issues.append({"path": key, "reason": row["state"]})
                return None
            if prepared is None:
                raw = timing.call("sync_read_open_close", path.read_bytes) if timing else path.read_bytes()
                after = timing.call("stat", path.stat) if timing else path.stat()
                sha = (timing.call("sha256", hashlib.sha256, raw) if timing else hashlib.sha256(raw)).hexdigest()
                self.read_stats["synchronous_reads"] += 1
            else:
                self.read_stats["prefetched_reads"] += 1
            if self._signature(before) != self._signature(after) or len(raw) != before.st_size:
                row["state"] = "changed_during_read"
                self.issues.append({"path": key, "reason": row["state"]})
                return None
            row["sha256"] = sha
            retain = ((timing.call("classification_inclusive", retain_if, raw) if timing else retain_if(raw))
                      if retain_if is not None else True)
            if not retain:
                row["state"] = "not_timing_evidence"
                return None
            if self.total_bytes + len(raw) > self.max_total_bytes:
                row["state"] = "size_budget_exceeded"
                self.issues.append({"path": key, "reason": row["state"]})
                return None
            snapshot = f"evidence/{sha}.gz"
            destination = self.output / snapshot
            exists = timing.call("snapshot_exists", destination.exists) if timing else destination.exists()
            if not exists:
                compressed = (timing.call("gzip_compress", gzip.compress, raw, compresslevel=3, mtime=0)
                              if timing else gzip.compress(raw, compresslevel=3, mtime=0))
                if timing:
                    timing.call("snapshot_write", destination.write_bytes, compressed)
                else:
                    destination.write_bytes(compressed)
            self.total_bytes += len(raw)
            row.update(state="captured", sha256=sha, snapshot=snapshot, mtime_ns=before.st_mtime_ns)
            return raw, sha
        except OSError as exc:
            row["error_type"] = type(exc).__name__
            self.issues.append({"path": key, "reason": row["error_type"]})
            return None

    def object(self, path: Path):
        captured = self.read(path)
        if captured is None:
            return None
        raw, sha = captured
        try:
            timing = self.inspection_timings
            return (timing.call("json_decode", private_json.loads, raw) if timing else private_json.loads(raw)), sha
        except (ValueError, UnicodeError) as exc:
            self.issues.append({"path": str(path), "reason": type(exc).__name__})
            return None


def visible_code(root: Path) -> tuple[list[dict], list[dict]]:
    """Static discovery, never import arbitrary executable scripts."""
    result = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                            cwd=root, check=True, capture_output=True)
    inventory, code = [], []
    for name in sorted(set(os.fsdecode(v) for v in result.stdout.split(b"\0") if v)):
        path = root / name
        if path.suffix not in {".py", ".sh", ".ps1", ".mjs", ".js", ".in", ".yaml", ".json"}:
            continue
        if not (name.startswith(("scripts/", "downloader/", "stockagent/", "deploy/", "configs/", "services/")) or name == "train.py"):
            continue
        if not path.is_file():
            continue
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        code.append({"path": name, "sha256": sha, "bytes": len(raw)})
        if path.suffix not in {".py", ".sh", ".ps1", ".mjs"}:
            continue
        text = raw.decode("utf-8-sig", errors="replace")
        flags, timing_lines, error, description = [], [], None, ""
        entrypoint = path.suffix != ".py" and name.startswith("scripts/")
        if path.suffix == ".py":
            try:
                tree = ast.parse(text)
                description = (ast.get_docstring(tree) or "").split("\n")[0][:240]
                entrypoint = any(isinstance(node, ast.If)
                                 and any(isinstance(n, ast.Name) and n.id == "__name__" for n in ast.walk(node.test))
                                 and any(isinstance(n, ast.Constant) and n.value == "__main__" for n in ast.walk(node.test))
                                 for node in ast.walk(tree))
                if not entrypoint:
                    # These calls cannot produce inventory rows. Their raw
                    # source identity is already retained in the code list.
                    continue
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    name_tail = getattr(node.func, "attr", getattr(node.func, "id", ""))
                    if name_tail == "add_argument":
                        flags.extend(a.value for a in node.args if isinstance(a, ast.Constant)
                                     and isinstance(a.value, str) and a.value.startswith("--"))
                    if name_tail in {"perf_counter", "monotonic", "perf_counter_ns", "monotonic_ns", "process_time"}:
                        timing_lines.append(node.lineno)
            except SyntaxError as exc:
                error = str(exc)
                entrypoint = name.startswith("scripts/")
        if not entrypoint:
            continue
        inventory.append({"kind": "entrypoint", "id": name, "category": category(name),
                          "sha256": sha, "description": description,
                          "argument_flags": sorted(set(flags)), "timing_call_lines": sorted(set(timing_lines)),
                          "discovery_error": error, "coverage": "entrypoint_discovered_operation_not_measured",
                          "next_measurement": "use canonical owner with exact config/input/output and retain its acceptance"})
    return inventory, code


def discover(root: Path, *, depth: int, wanted=None, exclude_roots=(), directory_budget=50_000) -> tuple[list[Path], list[dict]]:
    """Bounded metadata traversal: don't follow dataset/cold-store symlinks."""
    found, gaps, visited = [], [], 0
    if not root.is_dir():
        return found, [{"path": str(root), "reason": "root_absent"}]
    excluded = {str(Path(path).absolute()) for path in exclude_roots}
    stack = [(str(root), 0)]
    while stack:
        directory, level = stack.pop()
        kept, files = [], []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        is_dir = entry.is_dir()
                    except OSError:
                        is_dir = False
                    if is_dir:
                        if entry.name in SKIP_DIRS or entry.is_symlink():
                            continue
                        child = os.path.join(directory, entry.name)
                        absolute = child if os.path.isabs(child) else str(Path(child).absolute())
                        if absolute not in excluded:
                            kept.append(child)
                    else:
                        files.append(entry.name)
        except OSError as error:
            gaps.append({"path": str(error.filename), "reason": "directory_unreadable",
                         "error_type": type(error).__name__})
            continue
        visited += 1
        if level >= depth or visited >= directory_budget:
            if kept:
                gaps.append({"path": str(Path(directory)), "reason": "depth_or_directory_budget", "directories": len(kept)})
            kept = []
        current = None
        for name in files:
            if wanted is None and (not name.endswith(".json") or name == ".json"):
                continue
            if current is None:
                current = Path(directory)
            path = current / name
            if wanted is None or wanted(path):
                found.append(path)
        # Retain os.walk's second, fresh link check after predicate callbacks.
        # DirEntry's cached type alone would miss a link retargeted here.
        stack.extend((child, level + 1) for child in reversed(kept) if not os.path.islink(child))
    return sorted(found), gaps


def select_benchmark_paths(paths: list[Path], output: Path) -> list[Path]:
    """Reuse resolved directory prefixes, retaining generation and link checks.

    Discovery does not follow directory links. Checking a parent's generation
    still invalidates reuse if it is replaced or a directory link is retargeted
    between discovery and selection. Missing/unreadable parents retain the
    original per-file resolve behavior rather than becoming silent exclusions.
    """
    parents = {}
    output_key = os.path.normcase(str(output))
    output_prefix = output_key.rstrip(os.sep) + os.sep

    def resolved_directory(name, info):
        generation = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns)
        cached = parents.get(name)
        if cached is not None and cached[0] == generation:
            return cached[1]
        upper, basename = os.path.dirname(name), os.path.basename(name)
        try:
            linked = S_ISLNK(os.lstat(name).st_mode)
        except OSError:
            resolved = str(Path(name).resolve())
        else:
            if upper == name or basename in {".", ".."} or linked:
                resolved = str(Path(name).resolve())
            else:
                try:
                    upper_stat = os.stat(upper)
                except OSError:
                    resolved = str(Path(name).resolve())
                else:
                    # Keep metadata paths as strings: constructing and hashing
                    # long Path component tuples dominated the previous cache.
                    resolved = os.path.join(resolved_directory(upper, upper_stat), basename)
        parents[name] = (generation, resolved)
        if len(parents) > 128:
            parents.pop(next(iter(parents)))
        return resolved

    selected = []
    for path in paths:
        name = str(path)
        parent = os.path.dirname(name) or "."
        try:
            info = os.stat(parent)
            linked = S_ISLNK(os.lstat(name).st_mode)
        except OSError:
            resolved = str(path.resolve())
        else:
            resolved = str(path.resolve()) if linked else os.path.join(resolved_directory(parent, info), os.path.basename(name))
        resolved_key = os.path.normcase(resolved)
        if resolved_key != output_key and not resolved_key.startswith(output_prefix) and "workflow_speed_" not in name:
            selected.append(path)
    return selected


def discover_training(root: Path, *, depth: int, threads: int = 0, observations=None) -> tuple[list[Path], list[dict]]:
    command = ["rg", "--files", "--hidden", "--no-ignore", "-0"]
    if threads:
        command.extend(["--threads", str(threads)])
    for name in sorted(TRAINING_NAMES):
        command.extend(["-g", name])
    for name in sorted(SKIP_DIRS):
        command.extend(["-g", f"!**/{name}/**"])
    command.append(str(root))
    started = time.perf_counter()
    usage, resource = None, None
    observed = {"path": str(root), "command": command}
    if observations is not None:
        try:
            import resource
            usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        except ImportError:
            pass
    try:
        result = subprocess.run(command, capture_output=True, timeout=60)
        observed["exit_code"] = result.returncode
        if result.returncode not in (0, 1):
            return [], [{"path": str(root), "reason": "timing_discovery_failed", "exit_code": result.returncode}]
        paths = [Path(os.fsdecode(value)) for value in result.stdout.split(b"\0") if value]
        deep = {path for path in paths if len(path.relative_to(root).parts) > depth + 1}
        return sorted(path for path in paths if path not in deep), ([
            {"path": str(root), "reason": "training_depth_budget", "files_beyond_depth": len(deep)}] if deep else [])
    except (OSError, subprocess.TimeoutExpired) as exc:
        observed["error_type"] = type(exc).__name__
        return [], [{"path": str(root), "reason": f"timing_discovery_unavailable:{type(exc).__name__}"}]
    finally:
        if observations is not None:
            observed["wall_seconds"] = time.perf_counter() - started
            observed["child_resource_counters_available"] = usage is not None
            if usage is not None:
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                for name in ("utime", "stime", "minflt", "majflt", "inblock", "oublock", "nvcsw", "nivcsw"):
                    observed["child_" + name] = getattr(after, "ru_" + name) - getattr(usage, "ru_" + name)
            observed["resource_scope"] = "waited child resource deltas during this serial rg call; CPU includes child threads; block counters are not logical bytes or a bandwidth bound"
            observations.append(observed)


class Ledger:
    def __init__(self):
        self.samples: list[dict] = []
        self.inventory: list[dict] = []

    def add(self, *, workflow: str, stage: str, seconds, source: str, locator: str,
            origin: str, outcome: str = "recorded", context=None, metric="wall_seconds", work=None):
        if not finite_seconds(seconds):
            return
        context = context or {}
        row = {"workflow_id": workflow, "stage": stage, "metric": metric,
               "seconds": float(seconds), "origin": origin, "outcome": outcome,
               "source_sha256": source, "locator": locator, "context": context,
               "context_id": digest(context), "work": work or {}}
        units = (work or {}).get("units")
        row["units_per_second"] = units / seconds if finite_seconds(units) and seconds > 0 else None
        self.samples.append(row)

    def summaries(self) -> list[dict]:
        groups = defaultdict(list)
        for row in self.samples:
            key = tuple(row[k] for k in ("workflow_id", "stage", "metric", "origin", "outcome", "context_id"))
            groups[key].append(row)
        result = []
        for key, values in sorted(groups.items()):
            seconds = [row["seconds"] for row in values]
            result.append({**dict(zip(("workflow_id", "stage", "metric", "origin", "outcome", "context_id"), key)),
                           "samples": len(values), "median_seconds": statistics.median(seconds),
                           "min_seconds": min(seconds), "max_seconds": max(seconds),
                           "context": values[0]["context"],
                           "comparison_rule": "match work/semantics/input/hardware/cache; retain compared code identities; phases may overlap"})
        return result


def collect_services(ledger: Ledger, payload: dict, sha: str) -> None:
    for row in payload.get("services", []):
        unit = row["unit"]
        ledger.inventory.append({"kind": "service", "id": unit, "category": category(unit),
                                 "coverage": row.get("operation_latency_coverage"),
                                 "active_state": row.get("active_state"), "outcome": row.get("last_attempt_outcome"),
                                 "next_measurement": "completed natural invocation with matched operation receipts"})
        ledger.add(workflow=unit, stage="whole_process_attempt", seconds=row.get("last_attempt_wall_seconds"),
                   source=sha, locator=f"services/{unit}", origin="recorded_service_attempt",
                   outcome=str(row.get("last_attempt_outcome")),
                   context={"source": row.get("last_attempt_source"), "observed_at_utc": payload.get("observed_at_utc"),
                            "input_identity": "not_in_systemd_duration", "code_identity": "not_verified_loaded_code"})
        receipt = row.get("business_receipt") or {}
        if receipt.get("matches_last_attempt") is True and receipt.get("step_receipts_scope") == "exact_matched_run":
            for step in receipt.get("step_receipts", []):
                ledger.add(workflow=unit, stage=str(step.get("step")), seconds=step.get("elapsed_seconds"),
                           source=sha, locator=f"services/{unit}/business_receipt/{step.get('step')}",
                           origin="recorded_service_step", outcome=str(step.get("state")),
                           context={"run_id": receipt.get("run_id"), "input_identity": "producer_receipt"})
    for kind, key in (("timer", "timers"), ("path_trigger", "paths")):
        for name in payload.get(key, {}):
            ledger.inventory.append({"kind": kind, "id": name, "category": category(name),
                                     "coverage": "schedule_observed_not_work_duration"})


def collect_http(ledger: Ledger, payload: dict, sha: str) -> None:
    origin_url = payload.get("base_url", "unknown")
    for row in payload.get("http", []):
        workflow = f"http:{origin_url}{row['path']}"
        ledger.inventory.append({"kind": "http_route", "id": workflow, "category": "presentation",
                                 "coverage": "measured_responses" if not row.get("errors") else "responses_include_errors",
                                 "errors": row.get("errors"), "next_measurement": "same audience, route, response bytes and cache state"})
        for index, sample in enumerate([row["first_observed"], *row["samples"]]):
            context = {"origin": origin_url, "connection": "first_observed" if index == 0 else "sequential_followup",
                       "collector_host": payload.get("platform"), "cache": "uncontrolled_live",
                       "response_status": sample.get("status"),
                       "code_identity": "probe_source_only_not_verified_loaded_server_code"}
            for metric in ("total_ms", "headers_ms", "body_ms", "parse_ms", "server_ms"):
                value = sample.get(metric)
                ledger.add(workflow=workflow, stage=metric, seconds=value / 1000 if finite_seconds(value) else None,
                           source=sha, locator=f"http/{row['path']}/samples/{index}/{metric}", origin="live_http_probe",
                           outcome="http_success" if sample.get("ok") else "http_error", context=context,
                           work={"bytes": sample.get("decoded_bytes")})


def collect_tej(ledger: Ledger, payload: dict, sha: str) -> None:
    for index, row in enumerate(payload.get("samples", [])):
        context = {"table": row.get("table"), "fields": row.get("fields"), "companies": row.get("companies"),
                   "dates": row.get("dates"), "result_kind": "nonempty" if row.get("actual_rows") else "empty",
                   "timing_basis": payload.get("timing_basis"), "machine": "windows_bridge_producer"}
        ledger.add(workflow="acquisition:tej", stage="complete_download_task", seconds=row.get("seconds"), source=sha,
                   locator=f"samples/{index}", origin="recorded_tej_task", outcome="completed", context=context,
                   work={"unit": "exported_rows", "units": row.get("actual_rows")})
        for phase, seconds in (row.get("windows_flow") or {}).get("phases", {}).items():
            ledger.add(workflow="acquisition:tej", stage=phase, seconds=seconds, source=sha,
                       locator=f"samples/{index}/windows_flow/{phase}", origin="recorded_tej_task", context=context)
        for phase, seconds in row.get("scope_preparation_seconds", {}).items():
            ledger.add(workflow="acquisition:tej", stage="scope_preparation/" + phase, seconds=seconds, source=sha,
                       locator=f"samples/{index}/scope_preparation_seconds/{phase}", origin="recorded_tej_task", context=context)
        cache = (row.get("windows_flow") or {}).get("native_helper_cache") or {}
        for phase, seconds in cache.get("timings", {}).items():
            ledger.add(workflow="acquisition:tej", stage="native_helper_cache/" + phase, seconds=seconds, source=sha,
                       locator=f"samples/{index}/windows_flow/native_helper_cache/timings/{phase}",
                       origin="recorded_tej_task", context={**context, "native_cache_state": cache.get("state")})


def collect_finlab(ledger: Ledger, evidence: Evidence, root: Path) -> None:
    """Existing per-key fetch/attempt intervals; never dispatch SDK work.

    The producer stops this clock before its final receipt write. Unchanged
    checks retain the old object's size/rows, so they have no throughput units.
    """
    from stockagent.data.finlab_acquisition_contract import safe_stem

    for kind, field in (("receipts", "last_fetch_elapsed_seconds"), ("attempts", "last_attempt_elapsed_seconds")):
        for path in sorted((root / kind).glob("*.json")):
            captured = evidence.object(path)
            if not captured:
                continue
            row, sha = captured
            dataset = row.get("dataset") if isinstance(row, dict) else None
            if not isinstance(dataset, str) or path.name != safe_stem(dataset) + ".json":
                evidence.issues.append({"path": str(path), "reason": "finlab_dataset_receipt_identity_mismatch"})
                continue
            if kind == "receipts" and row.get("status") != "downloaded_unverified_for_pit":
                continue
            outcome = row.get("last_check_result", "downloaded") if kind == "receipts" else row.get("status", "unknown_attempt")
            ledger.add(workflow="acquisition:finlab:" + dataset, stage="fetch_body_before_receipt" if kind == "receipts" else "failed_attempt_before_receipt",
                       seconds=row.get(field), source=sha, locator=field, origin="recorded_finlab_receipt", outcome=str(outcome),
                       context={"dataset": dataset, "input_identity": "canonical_finlab_key_receipt",
                                "source_check_mode": row.get("source_check_mode"), "processing_engine": row.get("processing_engine"),
                                "sdk_version": row.get("finlab_sdk_version"), "object_sha256": row.get("sha256"),
                                "retained_object_rows": row.get("rows"), "retained_object_bytes": row.get("parquet_size_bytes"),
                                "source_checked_at_utc": row.get("source_checked_at_utc"),
                                "attempted_at_utc": row.get("attempted_at_utc"),
                                "scope": "producer_monotonic_body_excludes_final_receipt_write_and_independent_readback"})


def collect_training(ledger: Ledger, path: Path, raw: bytes, sha: str, issues: list[dict]) -> None:
    rows = []
    for index, line in enumerate(raw.splitlines() if path.suffix == ".jsonl" else [raw]):
        if not line.strip():
            continue
        try:
            rows.append((index + 1, private_json.loads(line)))
        except (ValueError, UnicodeError) as exc:
            issues.append({"path": str(path), "line": index + 1,
                           "reason": f"invalid_or_partial_timing_row:{type(exc).__name__}"})
    for line_number, row in rows:
        if not isinstance(row, dict):
            continue
        epoch = row.get("epoch")
        context = {"timing_file": str(path), "run_id": row.get("run_id"),
                   "epoch_regime": "first_two_epochs" if type(epoch) is int and epoch < 3 else "epoch_3_plus" if type(epoch) is int else None,
                   "rank": row.get("rank"), "world_size": row.get("world_size", row.get("timing_rank_count")),
                   "train_batches": row.get("train_batches"), "timing_synchronized": row.get("timing_synchronized"),
                   "source_config_hardware": "retain producer manifests; not inferred from collector host"}
        if path.name in {"startup_timing.jsonl", "trainer_startup_timing.jsonl", "pre_epoch_timing.jsonl"}:
            fields = {str(row.get("stage", "unknown")): row.get("elapsed_s")}
        elif path.name == "epoch_curve.jsonl":
            fields = {key: row[key] for key in EPOCH_PHASES if key in row}
        else:
            fields = {key: value for key, value in row.items()
                      if (key.endswith("_s") or key.endswith("_s_max_rank") or key.endswith("_ms_per_batch"))
                      and not key.endswith("_per_s")}
            if path.name == "explainability_timing.json":
                for group in ("compute_timing", "write_timing"):
                    if isinstance(row.get(group), dict):
                        fields.update({f"{group}/{key}": value for key, value in row[group].items() if key.endswith("_s")})
        for stage, value in fields.items():
            per_batch = stage.endswith("_ms_per_batch")
            ledger.add(workflow=f"training:{path.parent}", stage=stage, seconds=value / 1000 if per_batch and finite_seconds(value) else value,
                       source=sha, locator=f"line/{line_number}/{stage}", origin="recorded_training",
                       outcome="producer_reported_epoch_or_stage_not_fold_acceptance", context=context,
                       metric="seconds_per_batch" if per_batch else "wall_or_component_seconds")


_METRIC_CONTAINERS = JSON_CONTAINERS
_METRIC_TREE = None
_METRIC_DESCRIPTOR = None


def configure_metric_backend(mode: str):
    """Configure this collector process; service and trainer owners are untouched."""
    global _METRIC_TREE, _METRIC_DESCRIPTOR
    tree, metadata = load_backend(ROOT, mode)
    descriptor = tree.cache_descriptor(_metric_descriptor) if tree is not None else None
    _METRIC_TREE, _METRIC_DESCRIPTOR = tree, descriptor
    if descriptor is not None:
        metadata["descriptor_cache"] = {"scope": "key semantics only", "maxsize": 4096,
                                        "max_key_characters": 256, "eviction": "clear_at_capacity"}
    return metadata


def _metric_descriptor(name):
    return _metric_key(name) if len(name) <= 256 else _metric_key.__wrapped__(name)


def _metric_rows(value):
    if _METRIC_TREE is not None:
        try:
            return _METRIC_TREE.rows(value, "", _METRIC_DESCRIPTOR or _metric_descriptor, finite_seconds)
        except NotImplementedError:
            # Custom containers/keys and deep trees preserve the original
            # generator's behavior. Numeric/parser errors are not swallowed.
            pass
    return list(reported_metrics(value))


@lru_cache(maxsize=4096)
def _metric_key(name: str) -> tuple[bool, bool, str]:
    """Cache key semantics only; never retain a producer value or subtree."""
    visible = SENSITIVE_FIELD.search(name) is None
    timing = ((name == "seconds" or name.endswith(("_s", "_ms", "_seconds")))
              and TIME_FIELD.search(name) is not None and visible)
    return visible, timing, name.replace("~", "~0").replace("/", "~1")


def reported_metrics(value, *, pointer="", parent=None):
    """Unreviewed producer fields stay separate from accepted operation samples."""
    if isinstance(value, dict):
        for key, child in value.items():
            name = str(key)
            # Bound both cache cardinality and individual key length. Unusual
            # long keys retain the exact uncached matching/escaping behavior.
            visible, timing, escaped = (_metric_key(name) if len(name) <= 256
                                        else _metric_key.__wrapped__(name))
            if isinstance(child, _METRIC_CONTAINERS):
                if visible:
                    path = pointer + "/" + escaped
                    yield from reported_metrics(child, pointer=path, parent=key)
            elif timing and finite_seconds(child):
                path = pointer + "/" + escaped
                yield {"pointer": path, "reported_value": child, "reported_unit": "milliseconds" if key.endswith("_ms") else "seconds",
                       "semantic_state": "producer_reported_requires_scope_review"}
    elif isinstance(value, list):
        for index, child in enumerate(value):
            if isinstance(child, _METRIC_CONTAINERS):
                yield from reported_metrics(child, pointer=pointer + f"/{index}", parent=parent)


def capture_benchmark(evidence: Evidence, path: Path, *, explicit: bool = False, prepared=None):
    """Retain full timing receipts; inventory non-timing payloads by identity.

    Explicit receipts always keep their original bytes. Other benchmark-tree
    JSON files include large data dictionaries and contract archives. Inspect
    all candidates without letting those payloads consume the evidence budget.
    Escapes and non-ASCII encodings take the full parser path so encoded timing
    keys cannot evade selection. Lexical negatives are labelled as unparsed.
    """
    timing = evidence.inspection_timings
    if explicit:
        captured = evidence.object(path)
        if captured:
            payload, sha = captured
            return payload, sha, (timing.call("metric_tree", _metric_rows, payload) if timing else _metric_rows(payload))
        return None
    observed = {}

    def retain(raw):
        sha = evidence.sources[str(path.absolute())]["sha256"]
        previous = evidence.benchmark_negatives.get(sha)
        if previous is not None:
            classification, _parsed, source_path = previous
            observed.update(classification=classification, json_parsed=False,
                            classification_source_path=source_path,
                            classification_reused_from_sha256=sha)
            return False
        if not any(token in raw for token in (b"seconds", b"_ms", b"_s", b"\\", b"\x00")):
            observed.update(classification="no_possible_timing_tokens", json_parsed=False)
        else:
            payload = timing.call("json_decode", private_json.loads, raw) if timing else private_json.loads(raw)
            # Parsing still validates every previously parsed document. With
            # no escapes/UTF-16/32 bytes, every supported timing key contains
            # one of the same literal ASCII terms used by TIME_FIELD. A
            # shallow tree without those bytes cannot yield any metrics.
            # Deep trees retain the old walk and its recursion behavior.
            walk_required = (any(token in raw for token in POSSIBLE_TIME_BYTES)
                             or raw.count(b"{") + raw.count(b"[") >= 64
                             or sys.getrecursionlimit() < 256)
            metrics = ((timing.call("metric_tree", _metric_rows, payload) if timing else _metric_rows(payload))
                       if walk_required else [])
            observed.update(payload=payload, metrics=metrics,
                            classification="parsed_reported_timing_fields" if metrics else "parsed_no_reported_timing_fields",
                            json_parsed=True)
            if metrics:
                return True
        evidence.benchmark_negatives[sha] = (observed["classification"], observed["json_parsed"], str(path.absolute()))
        if len(evidence.benchmark_negatives) > 8192:
            evidence.benchmark_negatives.popitem(last=False)
        return False

    try:
        captured = evidence.read(path, retain_if=retain, prepared=prepared)
    except (ValueError, UnicodeError) as exc:
        evidence.sources[str(path.absolute())].update(state="invalid_json", error_type=type(exc).__name__)
        evidence.issues.append({"path": str(path), "reason": type(exc).__name__})
        return None
    source = evidence.sources[str(path.absolute())]
    source.update({key: observed[key] for key in ("classification", "json_parsed", "classification_source_path",
                                               "classification_reused_from_sha256") if key in observed})
    if captured:
        raw, sha = captured
        # A previously captured source may already be shared with an explicit
        # registry input, in which case read() doesn't invoke retain again.
        payload = (observed["payload"] if "payload" in observed else
                   timing.call("json_decode", private_json.loads, raw) if timing else private_json.loads(raw))
        metrics = (observed["metrics"] if "metrics" in observed else
                   timing.call("metric_tree", _metric_rows, payload) if timing else _metric_rows(payload))
        return payload, sha, metrics
    return None


def collect_registered_steps(ledger: Ledger, evidence: Evidence, root: Path, *, runs: int) -> None:
    for scope in ("daily", "intraday", "features", "backfill"):
        directory = root / f"registered_{scope}/step_receipts"
        if not directory.is_dir():
            continue
        selected = sorted((p for p in directory.iterdir() if p.is_dir() and not p.is_symlink()
                           and re.fullmatch(r"registered-[a-z]+-[0-9TZ]+", p.name)),
                          key=lambda p: p.stat().st_mtime_ns, reverse=True)[:runs]
        for run in selected:
            for path in sorted(run.glob("*.json")):
                captured = evidence.object(path)
                if not captured:
                    continue
                payload, sha = captured
                if (not isinstance(payload, dict) or payload.get("schema_version") != 1
                        or payload.get("run_id") != run.name or payload.get("step") != path.stem):
                    evidence.issues.append({"path": str(path), "reason": "registered_step_identity_mismatch"})
                    continue
                ledger.add(workflow=f"registered:{scope}", stage=path.stem,
                           seconds=payload.get("elapsed_seconds"), source=sha, locator="/elapsed_seconds",
                           origin="recorded_registered_step", outcome=str(payload.get("state")),
                           context={"run_id": run.name, "exit_code": payload.get("exit_code"),
                                    "input_identity": "not_in_step_receipt", "scope": "step_not_entire_run"})
            ledger.inventory.append({"kind": "registered_run", "id": f"{scope}:{run.name}",
                                     "category": "acquisition", "coverage": "recorded_steps_not_entire_run"})


def collect_finmind(ledger: Ledger, evidence: Evidence, root: Path) -> None:
    """Read existing producer telemetry in bounded query-only transactions."""
    tables = {
        "finmind_object_transfer_samples": ("object_transfer", "id,dataset,partition,seconds,bytes,rows,observed_at_utc"),
        "finmind_object_derivation_samples": ("object_derivation", "id,dataset,partition,seconds,observed_at_utc,engine"),
        "finmind_pipeline_samples": ("pipeline_calibration", "id,dataset,requests,seconds,rows,configuration_sha256,observed_at_utc"),
        "finmind_json_pipeline_samples": ("json_pipeline_calibration", "id,dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc,fetch_validate_publish_seconds,cycle_wall_verified,cycle_id,cycle_wall_seconds,excluded_work_seconds,cycle_started_at_utc,cycle_finished_at_utc"),
    }
    observations = []
    for owner in ("sponsor", "complement"):
        path = root / owner / "queue.sqlite3"
        if not path.is_file():
            continue
        connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=.5)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 3
            connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            connection.execute("BEGIN")
            existing = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table, (stage, columns) in tables.items():
                if table not in existing:
                    continue
                actual = {r[1] for r in connection.execute(f"PRAGMA table_info({table})")}
                selected = [column for column in columns.split(",") if column in actual]
                query = f"SELECT {','.join(selected)} FROM {table} ORDER BY id DESC LIMIT 128"
                observations.append({"owner": owner, "table": table, "stage": stage, "query": query,
                                     "snapshot_at_utc": datetime.now(UTC).isoformat(),
                                     "rows": [dict(row) for row in connection.execute(query)]})
            connection.rollback()
        except sqlite3.Error as exc:
            evidence.issues.append({"path": str(path), "reason": f"finmind_read_only_snapshot:{type(exc).__name__}"})
        finally:
            connection.close()
    snapshot = evidence.output / "finmind_telemetry_snapshot.json"
    atomic_write_json(snapshot, {"provider_calls": 0, "query_only": True, "observations": observations})
    captured = evidence.object(snapshot)
    if not captured:
        return
    _, sha = captured
    cycles = set()
    for observation in observations:
        for row in observation["rows"]:
            context = {"owner": observation["owner"], "dataset": row.get("dataset"),
                       "configuration_sha256": row.get("configuration_sha256"), "engine": row.get("engine"),
                       "partition": row.get("partition"), "requests": row.get("requests"),
                       "nonempty_requests": row.get("nonempty_requests"),
                       "scope": "producer_calibration_not_full_history_or_entire_service"}
            locator = f"{observation['owner']}/{observation['table']}/{row['id']}"
            ledger.add(workflow=f"acquisition:finmind:{observation['owner']}:{row.get('dataset')}",
                       stage=observation["stage"], seconds=row.get("seconds"), source=sha, locator=locator,
                       origin="recorded_finmind_telemetry", context=context,
                       work={"units": row.get("rows"), "unit": "rows", "bytes": row.get("bytes")})
            cycle = (observation["owner"], row.get("cycle_id"))
            if row.get("cycle_wall_verified") == 1 and cycle[1] and cycle not in cycles:
                cycles.add(cycle)
                ledger.add(workflow=f"acquisition:finmind:{observation['owner']}", stage="verified_complete_cycle_wall",
                           seconds=row.get("cycle_wall_seconds"), source=sha, locator=locator,
                           origin="recorded_finmind_telemetry", context={"cycle_id": cycle[1],
                           "configuration_sha256": row.get("configuration_sha256"), "input_identity": "producer_cycle"})


def collect_engineering_runs(ledger: Ledger, evidence: Evidence, root: Path, *, limit: int) -> None:
    """Project timing fields only; do not copy command arguments or log bodies."""
    if not root.is_dir():
        return
    paths = sorted(root.glob("*/run.json"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
    rows = []
    for path in paths:
        if len(rows) >= limit:
            break
        try:
            before = path.stat()
            raw = path.read_bytes()
            after = path.stat()
            if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                continue
            payload = json.loads(raw)
        except (OSError, ValueError):
            continue
        if (payload.get("host") != socket.gethostname() or payload.get("repo_root") != str(ROOT)
                or payload.get("state") not in {"succeeded", "failed", "cancelled"}):
            continue
        selected = {key: payload.get(key) for key in (
            "run_id", "name", "task_id", "state", "exit_code", "host", "repo_root",
            "started_at_utc", "finished_at_utc", "performance", "workflow_code_sha256")}
        selected.update(receipt_path=str(path), original_receipt_sha256=hashlib.sha256(raw).hexdigest())
        rows.append(selected)
    snapshot = evidence.output / "engineering_timing_snapshot.json"
    atomic_write_json(snapshot, {"selection": "latest terminal local-owner runs; command arguments omitted", "rows": rows})
    captured = evidence.object(snapshot)
    if not captured:
        return
    _, sha = captured
    for row in rows:
        performance = row.get("performance") or {}
        seconds = performance.get("wall_seconds") if performance.get("contract") == "waited_command_monotonic_resources_v1" else None
        basis = "monotonic_child_launch_through_wait"
        if seconds is None:
            basis = "legacy_wall_clock_estimate_not_monotonic"
            try:
                seconds = (datetime.fromisoformat(row["finished_at_utc"]) - datetime.fromisoformat(row["started_at_utc"])).total_seconds()
            except (TypeError, ValueError):
                continue
        ledger.add(workflow=f"engineering:{row['name']}", stage="complete_command", seconds=seconds,
                   source=sha, locator=row["run_id"], origin="recorded_engineering_command", outcome=row["state"],
                   context={"run_id": row["run_id"], "task_id": row["task_id"], "clock_basis": basis,
                            "workflow_code_sha256": row.get("workflow_code_sha256"), "input_identity": "not_in_timing_projection"})


def bind_source_stages(ledger: Ledger) -> int:
    """Bind exact registered IDs; shared cycle wall is not per-source."""
    sources = {row["id"]: row for row in ledger.inventory if row["kind"] == "source_endpoint"}
    bound = defaultdict(list)
    for sample in ledger.samples:
        prefix = {"recorded_finmind_telemetry": "finmind:", "recorded_finlab_receipt": "finlab:"}.get(sample["origin"])
        if prefix is None or sample["context"].get("input_identity") == "producer_cycle":
            continue
        dataset = sample["context"].get("dataset")
        identifier = prefix + dataset if isinstance(dataset, str) else None
        if identifier in sources:
            sample["source_endpoint_id"] = identifier
            bound[identifier].append(sample)
    for identifier, rows in bound.items():
        sources[identifier].update(
            coverage="recorded_stage_timing_only",
            timed_stages=sorted({row["stage"] for row in rows}),
            speed_sample_count=len(rows),
            next_measurement="retain partition/units and measure the remaining source workflow; shared cycle time is not source time")
    return len(bound)


def collect_benchmark(ledger: Ledger, payload, sha: str, path: Path) -> None:
    """Explicitly selected producer samples; full receipt retains acceptance."""
    if not isinstance(payload, dict):
        return
    workflow = f"benchmark:{path.parent.name}/{path.stem}"
    history = payload.get("local_history")
    if isinstance(history, dict) and isinstance(history.get("samples"), list):
        accepted = history.get("source_stable") is True and history.get("outputs_identical") is True
        context = {"receipt_sha256": sha, "scope": history.get("boundary"),
                   "producer_source_sha256": history.get("source_sha256"),
                   "input_identity": digest(history.get("inputs", {})),
                   "full_source_rebuild": history.get("full_source_rebuild"),
                   "wire_and_independent_readback": history.get("wire_and_independent_readback"),
                   "source_stable": history.get("source_stable"),
                   "outputs_identical": history.get("outputs_identical")}
        for index, row in enumerate(history["samples"]):
            for field in ("build_ms", "representations_ms", "independent_readback_ms",
                          "workflow_ms", "cpu_seconds"):
                value = row.get(field)
                if finite_seconds(value):
                    ledger.add(workflow=workflow, stage="history:" + field,
                        seconds=value / 1000 if field.endswith("_ms") else value,
                        source=sha, locator=f"/local_history/samples/{index}/{field}",
                        origin="selected_benchmark_receipt",
                        outcome="stable_history_profile" if accepted else "source_or_output_unstable",
                        metric="cpu_seconds" if field == "cpu_seconds" else "wall_seconds",
                        context={**context, "point_count": row.get("point_count"),
                                 "output_sha256": row.get("sha256")},
                        work={"units": row.get("point_count"), "unit": "points"})
    inventory = payload.get("local_inventory")
    if isinstance(inventory, dict) and inventory.get("kind") == "frozen_inventory_cpu_replay_not_source_rebuild":
        inputs = inventory.get("inputs") or {}
        context = {"receipt_sha256": sha, "payload_sha256": inputs.get("payload_sha256"),
                   "files": inputs.get("files"), "scope": inventory["kind"],
                   "outputs_identical": inventory.get("outputs_identical"),
                   "stat_includes_partition_proof": inputs.get("stat_includes_partition_proof")}
        for field in ("decode_ms", "discovery_ms", "stat_ms"):
            if finite_seconds(inputs.get(field)):
                ledger.add(workflow=workflow, stage="inventory_input:" + field,
                    seconds=inputs[field] / 1000, source=sha, locator=f"/local_inventory/inputs/{field}",
                    origin="selected_benchmark_receipt", context=context)
        for variant, values in (inventory.get("implementations") or {}).items():
            for index, milliseconds in enumerate(values.get("samples_ms") or []):
                ledger.add(workflow=workflow, stage="frozen_inventory_cpu_replay", seconds=milliseconds / 1000 if finite_seconds(milliseconds) else None,
                    source=sha, locator=f"/local_inventory/implementations/{variant}/samples_ms/{index}",
                    origin="selected_benchmark_receipt", context={**context, "variant": variant,
                        "fields": values.get("fields"), "producer_source_sha256": values.get("source_sha256")})
    if payload.get("contract") == "read_only_dashboard_browser_timing_v1":
        # Browser timestamps use navigation-relative milliseconds. Keep the
        # audience and rendering/cache boundary instead of pooling owner/WAN.
        context = {"receipt_sha256": sha, "url": payload.get("url"),
                   "scope": payload.get("boundary"), "errors": payload.get("errors"),
                   "sse_messages": payload.get("sseMessages"),
                   "timing_basis": "browser_performance_clock_navigation_relative_ms"}
        for section, fields in (("initial", ("readyMs", "renderMs", "memoizedChartMs")),
                                ("fullHistory", ("loadMs", "memoizedMs", "rebuildMs"))):
            row = payload.get(section) or {}
            for field in fields:
                if finite_seconds(row.get(field)):
                    ledger.add(workflow=workflow, stage=f"browser:{section}:{field}",
                        seconds=row[field] / 1000, source=sha, locator=f"/{section}/{field}",
                        origin="selected_benchmark_receipt", context={**context,
                            "points": row.get("points"), "series": row.get("series"),
                            "identical_canvas": row.get("identicalCanvas")})
        initial = payload.get("initial") or {}
        for index, row in enumerate(initial.get("navigation") or []):
            for field in ("domContentLoadedMs", "loadMs", "responseEndMs"):
                if finite_seconds(row.get(field)):
                    ledger.add(workflow=workflow, stage="navigation_to:" + field,
                        seconds=row[field] / 1000, source=sha, locator=f"/initial/navigation/{index}/{field}",
                        origin="selected_benchmark_receipt", context=context)
        for index, row in enumerate(initial.get("paint") or []):
            if finite_seconds(row.get("startMs")):
                ledger.add(workflow=workflow, stage="navigation_to:" + str(row.get("name")),
                    seconds=row["startMs"] / 1000, source=sha, locator=f"/initial/paint/{index}/startMs",
                    origin="selected_benchmark_receipt", context=context)
        for index, row in enumerate((payload.get("final") or {}).get("resources") or []):
            if finite_seconds(row.get("durationMs")):
                ledger.add(workflow=workflow, stage="resource:" + str(row.get("path")),
                    seconds=row["durationMs"] / 1000, source=sha, locator=f"/final/resources/{index}/durationMs",
                    origin="selected_benchmark_receipt", context=context)
    for key in ("trials", "samples", "results"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            context = {"receipt_sha256": sha, "scope": payload.get("scope", payload.get("claim_boundary", payload.get("contract"))),
                       "variant": row.get("variant", row.get("mode")), "cache_state": row.get("cache_state"),
                       "producer_host": payload.get("host"), "observed_at": payload.get("observed_at_utc", payload.get("started_at_utc")),
                       "timing_scope": "producer_defined_interval; verify acceptance and excluded work in frozen receipt"}
            for field in ("complete_wall_seconds", "wall_seconds", "elapsed_seconds", "elapsed_ms",
                          "workflow_seconds", "build_seconds", "representations_and_digest_seconds",
                          "workflow_ms", "build_ms", "representations_ms", "independent_readback_ms",
                          "independent_readback_seconds", "process_seconds", "worker_seconds_including_imports",
                          "cpu_seconds", "complete_cpu_seconds"):
                value = row.get(field)
                if not finite_seconds(value):
                    continue
                ledger.add(workflow=workflow, stage=field, seconds=value / 1000 if field.endswith("_ms") else value,
                           source=sha, locator=f"/{key}/{index}/{field}", origin="selected_benchmark_receipt",
                           outcome="producer_reported_sample_acceptance_in_receipt", context=context,
                           metric="cpu_seconds" if "cpu" in field else "wall_seconds",
                           work={"units": row.get("rows"), "unit": "rows", "bytes": row.get("bytes_read")})
            phases = [("phases", i, p) for i, p in enumerate(row.get("phases", []))]
            phases.extend(("writes", i, p) for i, p in enumerate(row.get("writes", [])))
            for phase_kind, phase_index, phase in phases:
                if not isinstance(phase, dict):
                    continue
                for field, value in phase.items():
                    if TIME_FIELD.search(field) and finite_seconds(value):
                        ledger.add(workflow=workflow, stage=str(phase.get("stage", phase.get("file"))) + ":" + field,
                                   seconds=value / 1000 if field.endswith("_ms") else value, source=sha, locator=f"/{key}/{index}/{phase_kind}/{phase_index}/{field}",
                                   origin="selected_benchmark_receipt", context=context)
            for field, value in (row.get("stages") or {}).items():
                if field.endswith("_ms") and finite_seconds(value):
                    ledger.add(workflow=workflow, stage="live_stage:" + field, seconds=value / 1000,
                               source=sha, locator=f"/{key}/{index}/stages/{field}",
                               origin="selected_benchmark_receipt", context=context)


def storage_activity_timings(history: dict) -> tuple[list[dict], list[dict]]:
    """Pair native history IDs; never give a running activity a zero duration."""
    scheduled, started, terminal, rows = {}, {}, set(), []
    attributes = {
        "activityTaskCompletedEventAttributes": "completed",
        "activityTaskFailedEventAttributes": "failed",
        "activityTaskTimedOutEventAttributes": "timed_out",
        "activityTaskCanceledEventAttributes": "cancelled",
    }
    for event in history.get("events", []):
        event_id = int(event["eventId"])
        when = datetime.fromisoformat(event["eventTime"].replace("Z", "+00:00"))
        if when.tzinfo is None:
            raise ValueError("native history event has no timezone")
        if "activityTaskScheduledEventAttributes" in event:
            scheduled[event_id] = (event, when, event["activityTaskScheduledEventAttributes"])
        elif "activityTaskStartedEventAttributes" in event:
            data = event["activityTaskStartedEventAttributes"]
            started[event_id] = (int(data["scheduledEventId"]), when, data)
        else:
            for field, outcome in attributes.items():
                if field not in event:
                    continue
                data = event[field]
                schedule_id, start_id = int(data["scheduledEventId"]), int(data.get("startedEventId", 0))
                terminal.add(schedule_id)
                if schedule_id not in scheduled:
                    continue
                _, scheduled_when, schedule = scheduled[schedule_id]
                begin = started.get(start_id)
                valid = begin is not None and begin[0] == schedule_id and scheduled_when <= begin[1] <= when
                rows.append({"scheduled_event_id": schedule_id, "started_event_id": start_id,
                             "terminal_event_id": event_id, "activity": schedule.get("activityType", {}).get("name"),
                             "activity_id": schedule.get("activityId"), "outcome": outcome,
                             "attempt": begin[2].get("attempt") if begin else None,
                             "server_start_to_terminal_seconds": (when - begin[1]).total_seconds() if valid else None,
                             "schedule_to_recorded_start_seconds": (begin[1] - scheduled_when).total_seconds() if valid else None,
                             "clock_basis": "Temporal_server_event_UTC_not_worker_monotonic",
                             "timing_state": "paired" if valid else "start_missing_or_clock_invalid"})
    pending = [{"scheduled_event_id": key, "activity": row[2].get("activityType", {}).get("name"),
                "state": "no_terminal_event_in_snapshot", "seconds": None}
               for key, row in scheduled.items() if key not in terminal]
    return rows, pending


async def profile_storage_history(output: Path) -> dict:
    """Use only the enrolled read-only history API in the locked storage role."""
    from temporalio.client import Client
    from scripts.rollout_lakehouse_source_policy import IDS, OPTIONAL_IDS
    from scripts.verify_lakehouse_workflow_history import owner_identity, validate_events
    from stockagent.control.lakehouse import configuration, guard

    output = output.absolute()
    if output.exists() or not output.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        raise ValueError("storage history observation needs a new private benchmark directory")
    config = configuration()
    guard(config)
    output.mkdir(parents=True, mode=0o700)
    owner = owner_identity()
    client = await Client.connect(config["temporal_endpoint"], namespace=config["temporal_namespace"])
    workflows, errors = [], []
    for name in (*IDS, *OPTIONAL_IDS):
        try:
            current = await client.get_workflow_handle(name).describe(rpc_timeout=timedelta(seconds=20))
            pinned = client.get_workflow_handle(name, run_id=current.run_id)
            history = await pinned.fetch_history(rpc_timeout=timedelta(seconds=30))
            validate_events(history)
            raw = history.to_json().encode()
            if len(raw) > 64 << 20:
                raise ValueError("history_exceeds_64_MiB_budget")
            path = output / (name + ".history.json.gz")
            atomic_write_bytes(path, gzip.compress(raw, compresslevel=3, mtime=0), durable=True)
            if gzip.decompress(path.read_bytes()) != raw:
                raise ValueError("complete_history_independent_readback_failed")
            samples, pending = storage_activity_timings(json.loads(raw))
            after = await pinned.describe(rpc_timeout=timedelta(seconds=20))
            workflows.append({"workflow_id": name, "run_id": current.run_id, "status": current.status.name,
                              "status_after": after.status.name, "history_sha256": hashlib.sha256(raw).hexdigest(),
                              "history_path": str(path), "history_events": len(history.events),
                              "samples": samples, "pending": pending})
        except Exception as exc:
            # Retain an unavailable workflow without executing its activities,
            # resetting it, or starting a replacement workflow.
            errors.append({"workflow_id": name, "error_type": type(exc).__name__})
    result = {"contract": "native_storage_activity_speed_history_v1", "observed_at_utc": datetime.now(UTC).isoformat(),
              "state": "observed" if not errors else "observed_with_gaps", "workflows": workflows, "errors": errors,
              "owner_before": owner, "owner_after": owner_identity(), "provider_calls": 0, "activities_executed": 0,
              "scope": "current pinned persistent runs only; server-observed activity intervals include RPC overhead; no whole-backup or transfer-rate claim"}
    atomic_write_json(output / "storage_profile.json", result, durable=True)
    return result


def collect_storage(ledger: Ledger, payload: dict, sha: str) -> None:
    for workflow in payload.get("workflows", []):
        for row in workflow.get("samples", []):
            for field in ("server_start_to_terminal_seconds", "schedule_to_recorded_start_seconds"):
                ledger.add(workflow="storage:" + workflow["workflow_id"], stage=str(row.get("activity")),
                           seconds=row.get(field), source=sha, locator=f"{workflow['run_id']}/{row['terminal_event_id']}/{field}",
                           origin="recorded_storage_history", outcome=row["outcome"], metric=field,
                           context={"run_id": workflow["run_id"], "attempt": row.get("attempt"),
                                    "history_sha256": workflow["history_sha256"], "clock_basis": row["clock_basis"],
                                    "input_identity": "activity_payload_in_frozen_history; not a controlled equal-work comparison"})
        ledger.inventory.append({"kind": "persistent_storage_workflow", "id": workflow["workflow_id"],
                                 "category": "storage_delivery", "coverage": "native_activity_intervals",
                                 "samples": len(workflow.get("samples", [])), "pending_activities": len(workflow.get("pending", []))})
    for row in payload.get("errors", []):
        ledger.inventory.append({"kind": "persistent_storage_workflow", "id": row["workflow_id"],
                                 "category": "storage_delivery", "coverage": "history_unavailable", "error": row["error_type"]})


def verify_collection(output: Path) -> dict:
    """Re-read frozen evidence and independently regroup the final CSV/JSONL."""
    output = output.resolve()
    report_raw = (output / "collection.json").read_bytes()
    report = json.loads(report_raw)

    def require(ok, message):
        if not ok:
            raise ValueError(message)

    def local(name):
        path = output / name
        require(path.resolve().is_relative_to(output), "evidence path escapes observation")
        return path

    for name, expected in report["outputs"].items():
        path = local(name)
        actual = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(1 << 20):
                actual.update(block)
        require(path.stat().st_size == expected["bytes"] and actual.hexdigest() == expected["sha256"],
                f"final output differs: {name}")
    manifest = json.loads((output / "evidence_manifest.json").read_bytes())
    snapshots, source_shas = {}, set()
    sources_by_path = {row["path"]: row for row in manifest["files"]}
    negative_rows = [row for row in manifest["files"] if row["state"] == "not_timing_evidence"]
    reused_inspections = 0
    for row in negative_rows:
        require(row.get("classification") in {"no_possible_timing_tokens", "parsed_no_reported_timing_fields"},
                "unknown non-timing classification")
        require(re.fullmatch(r"[0-9a-f]{64}", row.get("sha256", "")) is not None
                and isinstance(row.get("bytes"), int) and row["bytes"] >= 0,
                "invalid non-timing content identity")
        reused = row.get("classification_reused_from_sha256")
        if reused:
            source = sources_by_path.get(row.get("classification_source_path"), {})
            require(reused == row["sha256"] and source.get("path") != row["path"]
                    and source.get("state") == "not_timing_evidence"
                    and all(source.get(key) == row[key] for key in ("sha256", "bytes", "classification"))
                    and not source.get("classification_reused_from_sha256")
                    and row.get("json_parsed") is False,
                    "non-timing classification reuse identity differs")
            reused_inspections += 1
    if "non_timing_inspections_reused" in report:
        require(reused_inspections == report["non_timing_inspections_reused"], "inspection reuse count differs")
    for row in manifest["files"]:
        if row["state"] != "captured":
            continue
        source_shas.add(row["sha256"])
        if row["snapshot"] not in snapshots:
            with gzip.open(local(row["snapshot"]), "rb") as handle:
                actual, size = hashlib.sha256(), 0
                while block := handle.read(1 << 20):
                    actual.update(block)
                    size += len(block)
            snapshots[row["snapshot"]] = (actual.hexdigest(), size)
        require(snapshots[row["snapshot"]] == (row["sha256"], row["bytes"]),
                "frozen producer SHA or byte count differs")
    native_backend_verified = 0
    backend = report.get("metric_backend", {})
    if backend.get("selected") == "cpp":
        for path_key, sha_key in (("source_path", "source_sha256"), ("module_path", "module_sha256"),
                                  ("build_receipt", "build_receipt_sha256")):
            source = sources_by_path.get(backend.get(path_key), {})
            require(source.get("state") == "captured" and source.get("sha256") == backend.get(sha_key),
                    "native backend has no matching frozen identity")
        build_source = sources_by_path[backend["build_receipt"]]
        build = json.loads(gzip.decompress(local(build_source["snapshot"]).read_bytes()))
        require(build.get("contract") == "source_bound_metric_tree_cpp_build_v1"
                and build.get("identity", {}).get("source_sha256") == backend["source_sha256"]
                and build.get("module_sha256") == backend["module_sha256"],
                "native backend build identity differs")
        environment = json.loads((output / "environment.json").read_bytes())
        require(build["identity"].get("python") == environment.get("python"),
                "native backend build runtime differs")
        native_backend_verified = 1
    groups, origins = defaultdict(list), Counter()
    with (output / "speed_samples.jsonl").open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            require(finite_seconds(row["seconds"]), "invalid speed sample")
            require(row["source_sha256"] in source_shas, "sample has no frozen producer")
            require(row["context_id"] == digest(row["context"]), "sample context identity differs")
            units = row["work"].get("units")
            rate = units / row["seconds"] if finite_seconds(units) and row["seconds"] > 0 else None
            require(row["units_per_second"] == rate, "throughput denominator differs")
            key = tuple(row[k] for k in ("workflow_id", "stage", "metric", "origin", "outcome", "context_id"))
            groups[key].append(row["seconds"])
            origins[row["origin"]] += 1
    require(sum(origins.values()) == report["speed_samples"] and dict(origins) == report["sample_counts"],
            "sample count differs")
    seen = set()
    with (output / "speed_summary.csv").open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = tuple(row[k] for k in ("workflow_id", "stage", "metric", "origin", "outcome", "context_id"))
            require(key in groups and key not in seen, "missing or duplicate summary group")
            values = groups[key]
            require(int(row["samples"]) == len(values), "summary count differs")
            for field, expected in (("median_seconds", statistics.median(values)),
                                    ("min_seconds", min(values)), ("max_seconds", max(values))):
                require(float(row[field]) == expected, f"summary {field} differs")
            seen.add(key)
    require(len(seen) == len(groups) == report["summary_groups"], "summary coverage differs")
    inventory = json.loads((output / "inventory.json").read_bytes())["rows"]
    require(dict(Counter(row["kind"] for row in inventory)) == report["inventory_counts"], "inventory count differs")
    phases = report.get("phase_timings", {})
    if "phase_timings" in report:
        require(isinstance(phases, dict) and bool(phases), "invalid collector phase timings")
        for row in phases.values():
            require(isinstance(row, dict) and all(finite_seconds(row.get(key)) for key in ("wall_seconds", "cpu_seconds")),
                    "invalid collector phase duration")
        require(finite_seconds(report.get("pre_manifest_wall_seconds"))
                and math.isclose(sum(row["wall_seconds"] for row in phases.values()),
                                 report["pre_manifest_wall_seconds"], rel_tol=1e-9, abs_tol=1e-7),
                "collector phase total differs")
    return {"contract": "workflow_speed_evidence_readback_v1", "state": "verified",
            "observed_at_utc": datetime.now(UTC).isoformat(), "collection": str(output),
            "collection_sha256": hashlib.sha256(report_raw).hexdigest(),
            "outputs_verified": len(report["outputs"]), "unique_originals_verified": len(snapshots),
            "speed_samples_verified": sum(origins.values()), "summary_groups_verified": len(groups),
            "collector_phases_verified": len(phases),
            "native_metric_backend_verified": native_backend_verified,
            "non_timing_identities_recorded": len(negative_rows), "classification_reuse_links_verified": reused_inspections,
            "scope": "frozen-byte integrity, duration domain, source links, throughput, independent CSV regrouping and non-timing classification identity links; non-timing payloads are not frozen/read back; coverage gaps and producer acceptance remain explicit"}


def inspect_benchmarks(evidence: Evidence, ledger: Ledger, benchmark_paths, *, explicit_receipts=(), read_workers=0,
                       profile_output=None, read_batch_bytes=2 << 20):
    """Inspect the complete ordered selection through the shared evidence owner."""
    if profile_output is not None:
        return profile_benchmark_inspection(evidence, ledger, benchmark_paths,
            explicit_receipts=explicit_receipts, read_workers=read_workers,
            profile_output=profile_output, read_batch_bytes=read_batch_bytes)
    explicit_receipts = set(explicit_receipts)
    # Filenames are bounded by the filesystem. Repeated receipt.json names do
    # not need to rerun the same category rules hundreds of thousands of times.
    classify = lru_cache(maxsize=1024)(category)
    metrics, receipt_catalog = [], []
    with closing(evidence.ordered_reads(benchmark_paths, workers=read_workers, skip=explicit_receipts,
                                       batch_bytes=read_batch_bytes)) as reads:
        for path, prepared in reads:
            captured = capture_benchmark(evidence, path, explicit=path in explicit_receipts, prepared=prepared)
            prepared = None
            receipt = {"path": str(path), "category": classify(path.name), "state": "unavailable", "timing_fields": 0}
            if captured:
                payload, sha, selected = captured
                observed = next((payload.get(k) for k in ("observed_at_utc", "generated_at_utc", "created_at_utc", "measured_at") if payload.get(k)), None) if isinstance(payload, dict) else None
                for row in selected:
                    row["source_path"] = str(path)
                    row["source_sha256"] = sha
                    row["producer_observed_at"] = observed
                metrics.extend(selected)
                if path in explicit_receipts:
                    collect_benchmark(ledger, payload, sha, path)
                receipt.update(state="captured", sha256=sha, timing_fields=len(selected), producer_observed_at=observed)
            else:
                source = evidence.sources.get(str(path.absolute()), {})
                receipt.update({key: source[key] for key in ("state", "sha256", "bytes", "classification", "json_parsed",
                                                            "classification_source_path", "classification_reused_from_sha256") if key in source})
            receipt_catalog.append(receipt)
    return metrics, receipt_catalog


class _InspectionTimings:
    """Coarse, thread-local counters; never retain producer payloads."""
    def __init__(self):
        import threading
        self.local, self.lock = threading.local(), threading.Lock()
        self.threads = []

    def call(self, stage, function, *args, **kwargs):
        rows = getattr(self.local, "rows", None)
        if rows is None:
            import threading
            rows = self.local.rows = {}
            with self.lock:
                self.threads.append({"thread": threading.current_thread().name, "stages": rows})
        row = rows.get(stage)
        if row is None:
            row = rows[stage] = {"calls": 0, "failed_calls": 0, "wall_nanoseconds": 0, "cpu_nanoseconds": 0}
        started, cpu = time.perf_counter_ns(), time.thread_time_ns()
        try:
            return function(*args, **kwargs)
        except BaseException:
            row["failed_calls"] += 1
            raise
        finally:
            finished, cpu_finished = time.perf_counter_ns(), time.thread_time_ns()
            row["wall_nanoseconds"] += finished - started
            row["cpu_nanoseconds"] += cpu_finished - cpu
            row["calls"] += 1


def profile_benchmark_inspection(evidence, ledger, benchmark_paths, *, explicit_receipts, read_workers, profile_output,
                                read_batch_bytes):
    """Opt-in operation clocks, with overlapping inclusive boundaries labelled."""

    profile_output = Path(profile_output)
    if profile_output.exists():
        raise FileExistsError(profile_output)
    timing = _InspectionTimings()
    previous = evidence.inspection_timings
    original_read, original_prepare = evidence.read, evidence._prepare_reads
    overrides = {name: name in evidence.__dict__ for name in ("read", "_prepare_reads")}
    evidence.read = lambda *a, **k: timing.call("evidence_read_inclusive", original_read, *a, **k)
    evidence._prepare_reads = lambda *a, **k: timing.call("read_batch_inclusive", original_prepare, *a, **k)
    evidence.inspection_timings = timing
    try:
        result = inspect_benchmarks(evidence, ledger, benchmark_paths,
            explicit_receipts=explicit_receipts, read_workers=read_workers, read_batch_bytes=read_batch_bytes)
    finally:
        evidence.inspection_timings = previous
        for name, original in (("read", original_read), ("_prepare_reads", original_prepare)):
            if overrides[name]:
                setattr(evidence, name, original)
            else:
                delattr(evidence, name)
    atomic_write_json(profile_output, {"contract": "workflow_inspection_operation_timings_v1",
        "diagnostic_only": True, "read_workers": read_workers,
        "read_batch_bytes": read_batch_bytes,
        "profiles": timing.threads,
        "scope": "per-thread operation wall and thread CPU; inclusive evidence/classification/batch ranges overlap their children; wall includes blocking; excludes output serialization/readback; clocks and counters add overhead; thread ranges must not be summed as workflow wall"})
    return result


def collect(args) -> dict:
    started = time.perf_counter()
    phase_started = started
    phase_cpu_started = time.process_time()
    phase_timings = {}

    def finish_phase(name):
        nonlocal phase_started, phase_cpu_started
        now, cpu_now = time.perf_counter(), time.process_time()
        phase_timings[name] = {"wall_seconds": now - phase_started,
                               "cpu_seconds": cpu_now - phase_cpu_started}
        phase_started, phase_cpu_started = now, cpu_now
        print(json.dumps({"phase_completed": name, **phase_timings[name]}), flush=True)

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    evidence = Evidence(output, max_file_bytes=args.max_file_mib << 20, max_total_bytes=args.max_total_mib << 20)
    ledger = Ledger()
    metric_backend = configure_metric_backend(getattr(args, "metric_backend", "auto"))
    entries, code = visible_code(ROOT)
    ledger.inventory.extend(entries)
    print(json.dumps({"phase": "entrypoint_inventory", "entrypoints": len(entries)}), flush=True)
    code_identity = digest(code)
    atomic_write_json(output / "code_manifest.json", {"sha256": code_identity, "files": code})
    # Retain the measurement implementation itself. Other working-tree hashes
    # identify observed source only, not code loaded by a long-lived service or
    # by a historical training producer.
    measurement_sources = []
    for name in ("scripts/profile_workflow_speeds.py", "scripts/agent_workflow.py", "scripts/runtime_env.sh",
                 "scripts/audit_service_latency_coverage.py", "scripts/benchmark_dashboard_latency.py",
                 "scripts/benchmark_live_inference.py", "scripts/benchmark_day_trade_persistence.py",
                 "scripts/benchmark_dataset_version_pipeline.py", "scripts/benchmark_downloader_http.py",
                 "scripts/benchmark_dashboard_browser.mjs", "stockagent/private_json.py",
                 "stockagent/workflow_metric_tree.py", "stockagent/workflow_metric_tree.cpp",
                 "stockagent/live/market_status.py", "stockagent/live/signal_engine.py",
                 "stockagent/live/tw_day_trade_dashboard.py", "stockagent/live/completed_inference_cache.py",
                 "stockagent/live/service_code_release.py"):
        captured = evidence.read(ROOT / name)
        if captured:
            measurement_sources.append({"path": name, "sha256": captured[1]})
    if metric_backend["selected"] == "cpp":
        for name in ("module_path", "build_receipt"):
            path = Path(metric_backend[name])
            captured = evidence.read(path)
            if captured:
                measurement_sources.append({"path": str(path.relative_to(ROOT)), "sha256": captured[1]})
    atomic_write_json(output / "measurement_sources.json", {
        "scope": "observed collector/benchmark source; not proof of loaded service or historical producer code",
        "files": measurement_sources})
    finish_phase("code_inventory_and_source_capture")
    environment = observe_node(ROOT, output)
    environment["runtime_versions"] = {}
    for package in ("numpy", "numba", "pyarrow", "polars", "torch", "pandas", "msgspec"):
        try:
            environment["runtime_versions"][package] = version(package)
        except PackageNotFoundError:
            environment["runtime_versions"][package] = None
    environment["python"] = sys.version
    environment["host"] = socket.gethostname()
    environment["thread_environment"] = {key: os.environ.get(key) for key in
        ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMBA_NUM_THREADS", "POLARS_MAX_THREADS")}
    atomic_write_json(output / "environment.json", environment)
    finish_phase("environment_observation")
    for path in args.architecture:
        captured = evidence.object(path)
        if captured is None:
            continue
        payload, _ = captured
        for key, kind, identity in (("training_modes", "training_mode", "execution_mode"),
                                     ("dataset_catalog", "dataset", "dataset"),
                                     ("market_configs", "market_config", "path"),
                                     ("deployment_configs", "deployment_config", "path"),
                                     ("systemd_templates", "service_template", "unit")):
            for row in payload.get(key, []):
                ledger.inventory.append({"kind": kind, "id": row[identity], "category": category(str(row)),
                                         "coverage": "registered_scope_operation_timing_not_bound", "owner": row})
        for key in ("market_config_errors", "deployment_config_errors"):
            for row in payload.get(key, []):
                ledger.inventory.append({"kind": "config_error", "id": row["path"], "category": "training", "coverage": "configuration_unresolved", "error": row["error"]})
    captured = evidence.object(args.monitor_snapshot)
    if captured:
        payload, _ = captured
        for row in payload.get("sources", []):
            ledger.inventory.append({"kind": "source_endpoint", "id": row["id"], "category": "acquisition",
                                     "provider": row.get("provider"), "active_scope": row.get("in_active_scope"),
                                     "service_keys": (row.get("automation") or {}).get("service_keys", []),
                                     "snapshot_at_utc": payload.get("generated_at_utc"),
                                     "coverage": "source_specific_operation_timing_not_bound",
                                     "next_measurement": "match source/task identity, completed units and elapsed time; shared service time is not per-source time"})
    captured = evidence.object(ROOT / "configs/data_sync/node_storage_roles.json")
    if captured:
        for name, row in captured[0].get("nodes", {}).items():
            ledger.inventory.append({"kind": "node", "id": name, "category": "operations", "owner": row,
                                     "coverage": "local_observation" if name == socket.gethostname() else "remote_live_speed_not_observed"})
    finish_phase("scope_inventory")
    for paths, adapter in ((args.services, collect_services), (args.http, collect_http), (args.tej, collect_tej), (args.storage, collect_storage)):
        for path in paths:
            captured = evidence.object(path)
            if captured:
                if adapter is collect_storage:
                    # A storage profile refers to complete pinned histories;
                    # retain those originals too and independently check them.
                    for workflow in captured[0].get("workflows", []):
                        history = evidence.read(Path(workflow["history_path"]))
                        if history:
                            raw = gzip.decompress(history[0])
                            if hashlib.sha256(raw).hexdigest() != workflow["history_sha256"]:
                                raise ValueError("storage history SHA does not match its profile")
                adapter(ledger, *captured)
    finish_phase("service_http_tej_storage_receipts")
    collect_registered_steps(ledger, evidence, ROOT / "artifacts/daily_downloader", runs=args.registered_runs)
    collect_finmind(ledger, evidence, ROOT / "data_finmind")
    collect_finlab(ledger, evidence, ROOT / "data_finlab")
    finish_phase("acquisition_receipts")
    collect_engineering_runs(ledger, evidence, ROOT / "artifacts/operations/agent-workflow/runs", limit=args.agent_runs)
    finish_phase("engineering_command_receipts")
    print(json.dumps({"phase": "current_receipts", "samples": len(ledger.samples)}), flush=True)
    traversal_gaps = []
    training_paths = []
    training_discovery_resources = []
    for root in args.training_root:
        paths, gaps = discover_training(root, depth=args.training_depth,
            threads=getattr(args, "training_discovery_threads", 0), observations=training_discovery_resources)
        training_paths.extend(paths)
        traversal_gaps.extend(gaps)
    finish_phase("training_timing_discovery")
    for path in sorted(set(training_paths)):
        captured = evidence.read(path)
        ledger.inventory.append({"kind": "training_timing_file", "id": str(path), "category": "training",
                                 "coverage": "captured_producer_timing" if captured else "timing_evidence_unavailable"})
        if captured:
            collect_training(ledger, path, *captured, evidence.issues)
    finish_phase("training_timing_ingestion")
    print(json.dumps({"phase": "training_timings", "files": len(set(training_paths)), "samples": len(ledger.samples)}), flush=True)
    benchmark_root = ROOT / "artifacts/benchmarks"
    # These collector generations are consumed only through explicit receipt
    # arguments below. Prune them before walking their retained input payloads.
    excluded_benchmarks = [output, *(path for path in benchmark_root.iterdir()
                                   if path.is_dir() and path.name.startswith("workflow_speed_"))] if benchmark_root.is_dir() else [output]
    benchmark_paths, gaps = discover(benchmark_root, depth=args.benchmark_depth,
                                    exclude_roots=excluded_benchmarks,
                                    directory_budget=args.benchmark_directory_budget)
    traversal_gaps.extend(gaps)
    finish_phase("benchmark_discovery")
    benchmark_paths = select_benchmark_paths(benchmark_paths, output)
    explicit_receipts = set(args.benchmark_receipt)
    benchmark_paths = list(dict.fromkeys([*sorted(explicit_receipts), *sorted(set(benchmark_paths))]))
    finish_phase("benchmark_path_selection")
    metrics, receipt_catalog = inspect_benchmarks(evidence, ledger, benchmark_paths,
                                                explicit_receipts=explicit_receipts,
                                                read_workers=getattr(args, "evidence_read_workers", 0),
                                                read_batch_bytes=getattr(args, "evidence_read_batch_mib", 2) << 20,
                                                profile_output=(output / "inspection_profile.json")
                                                if getattr(args, "profile_benchmark_inspection", False) else None)
    finish_phase("benchmark_evidence_inspection")
    print(json.dumps({"phase": "benchmark_index", "receipts": len(receipt_catalog), "reported_metrics": len(metrics)}), flush=True)
    # Deduplicate inventory rows across repeated snapshots without hiding an
    # earlier outcome: per-snapshot data remain in frozen evidence and samples.
    inventory = {(row["kind"], row["id"]): row for row in ledger.inventory}
    ledger.inventory = list(inventory.values())
    source_stage_count = bind_source_stages(ledger)
    json_lines(output / "speed_samples.jsonl", ledger.samples)
    summaries = ledger.summaries()
    write_csv(output / "speed_summary.csv", summaries)
    write_csv(output / "coverage.csv", ledger.inventory)
    write_csv(output / "reported_metrics.csv", metrics)
    write_csv(output / "benchmark_receipts.csv", receipt_catalog)
    atomic_write_json(output / "inventory.json", {"contract": CONTRACT, "rows": ledger.inventory})
    write_evidence_manifest(output / "evidence_manifest.json", evidence, traversal_gaps)
    finish_phase("aggregation_and_output_serialization")
    report = {
        "contract": CONTRACT, "state": "collected_with_explicit_coverage_gaps",
        "metric_backend": metric_backend,
        "observed_at_utc": datetime.now(UTC).isoformat(), "code_manifest_sha256": code_identity,
        "inventory_counts": dict(Counter(row["kind"] for row in ledger.inventory)),
        "sample_counts": dict(Counter(row["origin"] for row in ledger.samples)),
        "speed_samples": len(ledger.samples), "summary_groups": len(summaries),
        "benchmark_receipts": len(receipt_catalog), "unreviewed_reported_metrics": len(metrics),
        "benchmark_receipt_states": dict(Counter(row["state"] for row in receipt_catalog)),
        "non_timing_inspections_reused": sum(bool(row.get("classification_reused_from_sha256")) for row in receipt_catalog),
        "evidence_sources": len(evidence.sources), "evidence_bytes": evidence.total_bytes,
        "evidence_read_stats": dict(evidence.read_stats),
        "training_discovery_resources": training_discovery_resources,
        "issues": len(evidence.issues), "traversal_gaps": len(traversal_gaps),
        "source_endpoints_individually_timed": 0,
        "source_endpoints_with_stage_timing": source_stage_count,
        "input_paths": {**{key: [str(path) for path in getattr(args, key)] for key in
                              ("architecture", "services", "http", "tej", "storage", "benchmark_receipt", "training_root")},
                        "monitor_snapshot": str(args.monitor_snapshot)},
        "limits": {"training_depth": args.training_depth, "benchmark_depth": args.benchmark_depth,
                   "benchmark_directory_budget": args.benchmark_directory_budget,
                   "excluded_directory_names": sorted(SKIP_DIRS), "max_file_mib": args.max_file_mib,
                   "max_total_mib": args.max_total_mib, "registered_runs": args.registered_runs,
                   "agent_runs": args.agent_runs,
                   "evidence_read_workers": getattr(args, "evidence_read_workers", 0),
                   "evidence_read_batch_mib": getattr(args, "evidence_read_batch_mib", 2),
                   "training_discovery_threads": getattr(args, "training_discovery_threads", 0)},
        "interpretation": [
            "Inventories have different grains; never combine their denominators into one completion percentage.",
            "Source/service/config discovery is not an individually timed operation.",
            "Historical producer timings retain their own identity; collector hardware is not their hardware.",
            "No source data, fee, mask, checkpoint, validation, plotting or durability work was omitted to claim acceleration.",
            "A daemon resource sample is not operation latency; failed attempts and empty results remain separate.",
            "Nested/parallel component timings overlap; never sum them as total workflow wall time.",
            "reported_metrics.csv is a discovery index, not validated comparable benchmark samples.",
            "Non-timing benchmark candidates retain identity/classification only; explicit and timing-bearing receipts retain complete bytes, subject to visible evidence limits.",
            "collection_wall_seconds ends before final file hashing/manifest write; the canonical command receipt measures through child wait.",
        ],
        "collection_wall_seconds": time.perf_counter() - started,
        "phase_timings": phase_timings,
        "phase_timing_scope": "serial monotonic intervals through final output hashing; excludes final collection.json write; CPU is collector process time, excluding subprocess CPU",
    }
    # Bind every final output and every retained original snapshot independently.
    report["outputs"] = {str(path.relative_to(output)): {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
                         for path in sorted(output.iterdir()) if path.is_file()}
    finish_phase("final_output_hashes")
    report["pre_manifest_wall_seconds"] = phase_started - started
    atomic_write_json(output / "collection.json", report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="New immutable observation directory")
    parser.add_argument("--architecture", type=Path, action="append", default=[])
    parser.add_argument("--monitor-snapshot", type=Path, default=ROOT / "artifacts/live/data_monitor/public_status.json")
    parser.add_argument("--services", type=Path, action="append", default=[])
    parser.add_argument("--http", type=Path, action="append", default=[])
    parser.add_argument("--tej", type=Path, action="append", default=[])
    parser.add_argument("--storage", type=Path, action="append", default=[])
    parser.add_argument("--storage-history-only", action="store_true", help="Use run_lakehouse_control.sh python in the locked role; read pinned histories only")
    parser.add_argument("--verify-only", action="store_true", help="Read an existing observation; check every frozen SHA and regroup every sample")
    parser.add_argument("--validation-output", type=Path, help="New independent readback receipt, used with --verify-only")
    parser.add_argument("--benchmark-receipt", type=Path, action="append", default=[])
    parser.add_argument("--metric-backend", choices=("python", "auto", "cpp"), default="auto",
                        help="Benchmark field walker; auto only uses a source/ABI-verified cache; cpp explicitly builds an absent cache")
    parser.add_argument("--evidence-read-workers", type=int, choices=(0, 1, 2, 4), default=0,
                        help="Ordered benchmark read-ahead; 0 is synchronous, positive counts use bounded read-only workers")
    parser.add_argument("--profile-benchmark-inspection", action="store_true",
                        help="Diagnostic per-thread operation clocks; changes timings, excluded from optimization samples")
    parser.add_argument("--evidence-read-batch-mib", type=int, choices=(1, 2, 8, 16, 256), default=2,
                        help="Raw bytes per read-ahead batch; total prefetch buffers are at most (workers + 1) times this limit plus one byte per batch")
    parser.add_argument("--training-root", type=Path, action="append")
    parser.add_argument("--training-depth", type=int, default=9)
    parser.add_argument("--training-discovery-threads", type=int, choices=(0, 1, 2, 4), default=0,
                        help="rg traversal threads; 0 retains rg's default. Re-measure on the configured roots")
    parser.add_argument("--benchmark-depth", type=int, default=15)
    parser.add_argument("--benchmark-directory-budget", type=int, default=50_000)
    parser.add_argument("--registered-runs", type=int, default=3, help="Latest recorded step sets per canonical scope; not whole-run acceptance")
    parser.add_argument("--agent-runs", type=int, default=256, help="Latest terminal local-owner engineering commands")
    parser.add_argument("--max-file-mib", type=int, default=64)
    parser.add_argument("--max-total-mib", type=int, default=1024)
    args = parser.parse_args(argv)
    if args.verify_only:
        if args.storage_history_only:
            parser.error("verification and live storage observation are separate commands")
        if args.validation_output and args.validation_output.exists():
            parser.error("validation output must be new")
        report = verify_collection(args.output_dir)
        if args.validation_output:
            atomic_write_json(args.validation_output, report, durable=True)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    if args.validation_output:
        parser.error("--validation-output requires --verify-only")
    if not 1 <= args.training_depth <= 15 or not 0 <= args.benchmark_depth <= 32:
        parser.error("training depth must be 1..15 and benchmark depth 0..32")
    if not 1 <= args.benchmark_directory_budget <= 1_000_000:
        parser.error("benchmark directory budget must be 1..1000000")
    if not 1 <= args.max_file_mib <= 1024 or not args.max_file_mib <= args.max_total_mib <= 16_384:
        parser.error("invalid evidence size budget")
    if not 1 <= args.registered_runs <= 100:
        parser.error("registered runs must be 1..100")
    if not 1 <= args.agent_runs <= 10_000:
        parser.error("agent runs must be 1..10000")
    args.training_root = args.training_root or [ROOT / f"artifacts/{name}" for name in ("markets", "ablations", "operations")]
    if args.storage_history_only:
        report = asyncio.run(profile_storage_history(args.output_dir))
        print(json.dumps({"state": report["state"], "workflows": len(report["workflows"]), "errors": report["errors"]}))
        return 0
    report = collect(args)
    print(json.dumps({key: value for key, value in report.items() if key not in {"outputs", "interpretation"}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
