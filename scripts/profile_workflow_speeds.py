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
from collections import Counter, OrderedDict, defaultdict, deque, namedtuple
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing
import csv
from datetime import UTC, datetime, timedelta
import gzip
import hashlib
import io
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
import json
import math
import multiprocessing
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
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent import private_json
from stockagent.remote_build import observe_node
from stockagent.workflow_metric_tree import load_backend
from stockagent.workflow_snapshot_pack import (SnapshotPackStore, SnapshotStreams as PinnedSnapshotStreams,
                          snapshot_json, validate_manifest_storage)

CONTRACT = "stockagent_workflow_speed_evidence_v1"
TRAINING_NAMES = {
    "startup_timing.jsonl", "trainer_startup_timing.jsonl", "pre_epoch_timing.jsonl",
    "epoch_curve.jsonl", "epoch_workflow_timing.jsonl", "save_timing.json",
    "plot_timing.json", "explainability_timing.json",
}
EPOCH_PLOT_TIMING = re.compile(r"epoch_curve_plot_timing_every[1-9][0-9]*\.json")
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
    with path.open("w", encoding="utf-8-sig", newline="", buffering=1 << 20) as handle:
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
    with path.open("w", encoding="utf-8", buffering=1 << 20) as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")


def write_evidence_manifest(path: Path, evidence: Evidence, traversal_gaps: list[dict]) -> None:
    payload = {"files": list(evidence.sources.values()), "issues": evidence.issues,
               "traversal_gaps": traversal_gaps}
    if evidence.snapshot_store is not None:
        payload["snapshot_storage"] = evidence.snapshot_store.seal(payload["files"])
    atomic_write_bytes(path, private_json.pretty_metadata_bytes(payload) + b"\n", durable=True)


_PreparedFileStat = namedtuple("_PreparedFileStat", "st_dev st_ino st_size st_mtime_ns st_ctime_ns")


class _BenchmarkMetricCache:
    """Bound immutable metric rows, retaining parsing and per-path annotation."""

    def __init__(self, *, max_bytes=64 << 20, max_entries=4096):
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self.entries, self.bytes = OrderedDict(), 0
        self.context = None
        self.hits = self.misses = self.evictions = self.bypasses = 0

    def get(self, key, context):
        if (self.context is None or len(self.context) != len(context)
                or any(a is not b for a, b in zip(self.context, context))):
            self.entries.clear()
            self.bytes = 0
            self.context = context
        value = self.entries.get(key)
        if value is None:
            self.misses += 1
            return None
        self.entries.move_to_end(key)
        self.hits += 1
        return [dict(row) for row in value[0]]

    def remember(self, key, rows):
        if not rows or type(rows) is not list or self.max_entries <= 0:
            self.bypasses += 1
            return
        frozen, size = [], sys.getsizeof(key) + 512
        for row in rows:
            if type(row) is not dict or any(type(k) is not str or type(v) not in (str, int, float)
                                           for k, v in row.items()):
                self.bypasses += 1
                return
            values = tuple(row.items())
            frozen.append(values)
            size += sys.getsizeof(values) + 32
            size += sum(sys.getsizeof(pair) + sys.getsizeof(pair[0]) + sys.getsizeof(pair[1])
                        for pair in values)
            if size > self.max_bytes:
                self.bypasses += 1
                return
        frozen = tuple(frozen)
        size += sys.getsizeof(frozen)
        if size > self.max_bytes:
            self.bypasses += 1
            return
        previous = self.entries.pop(key, None)
        if previous is not None:
            self.bytes -= previous[1]
        while self.entries and (len(self.entries) >= self.max_entries or self.bytes + size > self.max_bytes):
            _, (_, removed) = self.entries.popitem(last=False)
            self.bytes -= removed
            self.evictions += 1
        self.entries[key] = frozen, size
        self.bytes += size


def _cached_benchmark_metrics(evidence, payload, sha):
    cache = evidence.benchmark_metrics
    selected = cache.get(sha, (_METRIC_TREE, _METRIC_DESCRIPTOR, _metric_rows, reported_metrics))
    if selected is not None:
        return selected
    rows = _metric_rows(payload)
    cache.remember(sha, rows)
    return rows


class Evidence:
    def __init__(self, output: Path, *, max_file_bytes: int, max_total_bytes: int,
                 snapshot_storage: str = "individual_gzip"):
        self.output = output
        self.max_file_bytes, self.max_total_bytes = max_file_bytes, max_total_bytes
        self.total_bytes = 0
        self.sources: dict[str, dict] = {}
        self.issues: list[dict] = []
        # Cache only negative selections, by complete bytes. Large immutable
        # dictionaries repeat under many benchmark generations; retaining a
        # parsed object here would keep their entire payload resident.
        self.benchmark_negatives: OrderedDict[str, tuple[str, bool, str]] = OrderedDict()
        self.benchmark_metrics = _BenchmarkMetricCache()
        self.read_stats = Counter()
        self.inspection_timings = None
        if snapshot_storage not in ("individual_gzip", "packed_gzip_v1"):
            raise ValueError("unknown evidence storage layout")
        self.snapshot_store = None
        (output / "evidence").mkdir()
        if snapshot_storage == "packed_gzip_v1":
            self.snapshot_store = SnapshotPackStore(output, max_file_bytes=max_file_bytes)

    def _snapshot_destination(self, name, *, raw_bytes):
        if self.snapshot_store is not None:
            return self.snapshot_store.destination(name, raw_bytes)
        return self.output / name

    def close_snapshot_store(self):
        if self.snapshot_store is not None:
            self.snapshot_store.abort()

    @staticmethod
    def _signature(info):
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

    @staticmethod
    def _bounded_regular_read(handle, expected_size):
        """Read a stat-admitted regular file without buffering or an EOF probe.

        A short raw read can occur before EOF. Complete it within the same
        size-plus-one allocation bound; the caller still validates both stat
        generations and the exact final length before consuming these bytes.
        """
        raw = handle.read(expected_size + 1)
        if len(raw) >= expected_size or not raw:
            return raw
        chunks, size = [raw], len(raw)
        while raw and size < expected_size:
            raw = handle.read(expected_size + 1 - size)
            chunks.append(raw)
            size += len(raw)
        return b"".join(chunks)

    def _prepare_reads(self, batch, skip, byte_limit):
        """Read bytes only. The consuming thread owns all evidence decisions."""
        native = getattr(_METRIC_TREE, "read_batch", None)
        if (callable(native) and type(batch) in (list, tuple) and len(batch) <= 1024
                and type(byte_limit) is int and 0 <= byte_limit <= 256 << 20
                and type(self.max_file_bytes) is int and 0 <= self.max_file_bytes <= sys.maxsize
                and all(type(path) is type(Path()) for path in batch)):
            paths = [None if path in skip else os.fsencode(path) for path in batch]
            timing = self.inspection_timings
            values = (timing.call("native_batch_stat_read_stat", native, paths, byte_limit, self.max_file_bytes)
                      if timing else native(paths, byte_limit, self.max_file_bytes))
            self.read_stats["native_prefetch_batches"] += 1
            prepared = []
            for path, value in zip(batch, values, strict=True):
                if value is not None:
                    before, after, raw = value
                    sha = (timing.call("sha256", hashlib.sha256, raw) if timing else hashlib.sha256(raw)).hexdigest()
                    value = _PreparedFileStat(*before), _PreparedFileStat(*after), raw, sha
                prepared.append((path, value))
            return prepared
        prepared, remaining = [], byte_limit
        timing = self.inspection_timings
        for path in batch:
            value = None
            if path not in skip:
                try:
                    before = timing.call("stat", path.stat) if timing else path.stat()
                    if S_ISREG(before.st_mode) and before.st_size <= min(remaining, self.max_file_bytes):
                        # Bound allocations even when a producer grows a file.
                        handle = (timing.call("file_open", path.open, "rb", buffering=0)
                                  if timing else path.open("rb", buffering=0))
                        with handle:
                            raw = (timing.call("bounded_read", self._bounded_regular_read, handle, before.st_size)
                                   if timing else self._bounded_regular_read(handle, before.st_size))
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
        payloads occupy at most (workers + 1) * (batch_bytes + 1) bytes in
        Python mode. Native conversion can briefly add one batch per worker,
        bounding native raw copies by (2 * workers + 1) * (batch_bytes + 1). Large,
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
            snapshot = self._snapshot_destination(row["snapshot"], raw_bytes=row["bytes"])
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
            destination = self._snapshot_destination(snapshot, raw_bytes=len(raw))
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


def _entrypoint_inventory(item):
    """Inspect fixed source bytes without importing the inspected program."""
    name, suffix, raw, sha = item
    text = raw.decode("utf-8-sig", errors="replace")
    flags, timing_lines, error, description = [], [], None, ""
    entrypoint = suffix != ".py" and name.startswith("scripts/")
    if suffix == ".py":
        try:
            tree = ast.parse(text)
            description = (ast.get_docstring(tree) or "").split("\n")[0][:240]
            entrypoint = any(isinstance(node, ast.If)
                             and any(isinstance(n, ast.Name) and n.id == "__name__" for n in ast.walk(node.test))
                             and any(isinstance(n, ast.Constant) and n.value == "__main__" for n in ast.walk(node.test))
                             for node in ast.walk(tree))
            if not entrypoint:
                return None
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
        return None
    return {"kind": "entrypoint", "id": name, "category": category(name),
            "sha256": sha, "description": description,
            "argument_flags": sorted(set(flags)), "timing_call_lines": sorted(set(timing_lines)),
            "discovery_error": error, "coverage": "entrypoint_discovered_operation_not_measured",
            "next_measurement": "use canonical owner with exact config/input/output and retain its acceptance"}


def _visible_source_inventory_batch(root: Path, names):
    inventory, code = [], []
    for name in names:
        path = root / name
        if not path.is_file():
            continue
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        code.append({"path": name, "sha256": sha, "bytes": len(raw)})
        if path.suffix in {".py", ".sh", ".ps1", ".mjs"}:
            row = _entrypoint_inventory((name, path.suffix, raw, sha))
            if row is not None:
                inventory.append(row)
    return inventory, code


def visible_code(root: Path, *, workers: int = 0, observations=None) -> tuple[list[dict], list[dict]]:
    """Static discovery; queue paths, keeping source read/hash/parse together."""
    if type(workers) is not int or workers not in (0, 1, 2, 4):
        raise ValueError("code inventory workers must be 0, 1, 2 or 4")
    result = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                            cwd=root, check=True, capture_output=True)
    inventory, code = [], []

    def names():
        for name in sorted(set(os.fsdecode(v) for v in result.stdout.split(b"\0") if v)):
            path = root / name
            if path.suffix not in {".py", ".sh", ".ps1", ".mjs", ".js", ".in", ".yaml", ".json"}:
                continue
            if not (name.startswith(("scripts/", "downloader/", "stockagent/", "deploy/", "configs/", "services/")) or name == "train.py"):
                continue
            yield name

    if not workers:
        inventory, code = _visible_source_inventory_batch(root, names())
    else:
        import resource
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        pending, batch = deque(), []

        def consume(future):
            rows, sources = future.result()
            inventory.extend(rows)
            code.extend(sources)

        # Explicit spawn never inherits a caller's threads, native pools or CUDA.
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            for name in names():
                if len(batch) >= 32:
                    pending.append(pool.submit(_visible_source_inventory_batch, root, batch))
                    batch = []
                    if len(pending) >= workers * 2:
                        consume(pending.popleft())
                batch.append(name)
            if batch:
                pending.append(pool.submit(_visible_source_inventory_batch, root, batch))
            for future in pending:
                consume(future)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        if observations is not None:
            observations.update(workers=workers, start_method="spawn", batch_max_files=32,
                maximum_pending_batches=workers * 2,
                source_byte_policy="one complete source per active worker; pending queues contain paths only",
                child_user_cpu_seconds=after.ru_utime-before.ru_utime,
                child_system_cpu_seconds=after.ru_stime-before.ru_stime,
                child_peak_rss_bytes=after.ru_maxrss * 1024,
                cpu_scope="waited parser children; phase cpu_seconds is parent-only",
                rss_scope="maximum individual waited-child peak, may include earlier children; not simultaneous process-tree RSS")
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
    for name in sorted([*TRAINING_NAMES, "epoch_curve_plot_timing_every[0-9]*.json"]):
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
        paths = [path for path in paths if path.name in TRAINING_NAMES or EPOCH_PLOT_TIMING.fullmatch(path.name)]
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
        context = {"table": row.get("table"), "table_id": row.get("table_id"), "task_id": row.get("task_id"),
                   "active_attempt_id": row.get("active_attempt_id"), "prepared_sha256": row.get("prepared_sha256"),
                   "completed_at_utc": row.get("completed_at_utc"), "profile_contract": payload.get("contract"),
                   "source_scope_contract": payload.get("source_scope_contract"),
                   "fields": row.get("fields"), "companies": row.get("companies"),
                   "dates": row.get("dates"), "result_kind": "nonempty" if row.get("actual_rows") else "empty",
                   "timing_basis": payload.get("timing_basis"), "machine": "windows_bridge_producer",
                   "scope": "one_completed_task; nested phases overlap and must not be summed; not full source history"}
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
        for prefix, timings, pointer in (
                ("prebridge", (row.get("windows_flow") or {}).get("prebridge_timing") or {}, "windows_flow/prebridge_timing"),
                ("transport", row.get("transport_timing") or {}, "transport_timing")):
            for phase, seconds in timings.items():
                ledger.add(workflow="acquisition:tej", stage=prefix + "/" + phase, seconds=seconds, source=sha,
                           locator=f"samples/{index}/{pointer}/{phase}", origin="recorded_tej_task", context=context)


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
            return payload, sha, (timing.call("metric_tree", _cached_benchmark_metrics, evidence, payload, sha)
                                  if timing else _cached_benchmark_metrics(evidence, payload, sha))
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
            metrics = ((timing.call("metric_tree", _cached_benchmark_metrics, evidence, payload, sha)
                        if timing else _cached_benchmark_metrics(evidence, payload, sha))
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
                   timing.call("metric_tree", _cached_benchmark_metrics, evidence, payload, sha)
                   if timing else _cached_benchmark_metrics(evidence, payload, sha))
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


def _finmind_sample_rows(connection, table, columns):
    """Keep the recent window and one latest row for every bounded dataset.

    The global recent window alone hides infrequently dispatched datasets. All
    identifiers come from the fixed table specification, never source metadata.
    The owning read-only transaction supplies the time and consistency bound.
    """
    recent_query = f"SELECT {','.join(columns)} FROM {table} ORDER BY id DESC LIMIT 128"
    recent = [dict(row) for row in connection.execute(recent_query)]
    selection = {"mode": "recent_plus_latest_per_dataset_v1", "recent_limit": 128,
                 "dataset_limit": 256, "recent_rows": len(recent),
                 "dataset_selection_truncated": False}
    queries = [recent_query]
    if "dataset" not in columns:
        selection["latest_per_dataset_selected"] = 0
        return recent, selection, queries
    latest_query = (f"SELECT MAX(id) AS id FROM {table} WHERE dataset IS NOT NULL "
                    "GROUP BY dataset ORDER BY dataset LIMIT 257")
    latest = [row[0] for row in connection.execute(latest_query)]
    queries.append(latest_query)
    selection.update(dataset_groups_observed=len(latest),
                     dataset_selection_truncated=len(latest) > 256,
                     latest_per_dataset_selected=min(len(latest), 256))
    missing = sorted(set(latest[:256]) - {row["id"] for row in recent})
    if missing:
        query = f"SELECT {','.join(columns)} FROM {table} WHERE id IN ({','.join('?' for _ in missing)})"
        recent.extend(dict(row) for row in connection.execute(query, missing))
        queries.append({"sql": query, "parameters": missing})
    return sorted(recent, key=lambda row: row["id"], reverse=True), selection, queries


def _finmind_update_rows(connection):
    """Use exact successful checks, not a head that may include a later failure."""
    heads = [row[0] for row in connection.execute("SELECT dataset FROM dataset_heads ORDER BY dataset LIMIT 257")]
    rows = []
    for dataset in heads[:256]:
        row = connection.execute(
            "SELECT id,request_id,dataset,data_id,partition,checked_at_utc,kind,metadata_json "
            "FROM checks WHERE dataset=? AND kind!='failed' ORDER BY checked_at_utc DESC,id DESC LIMIT 1",
            (dataset,)).fetchone()
        if row:
            rows.append(dict(row))
    return rows, {"mode": "latest_success_per_dataset_v1", "dataset_limit": 256,
                  "dataset_groups_observed": len(heads), "dataset_selection_truncated": len(heads) > 256,
                  "selected_rows": len(rows), "older_successes_searched": False}


def collect_finmind(ledger: Ledger, evidence: Evidence, root: Path) -> None:
    """Read existing producer telemetry in bounded query-only transactions."""
    from downloader.finmind_updates import TRANSPORT_TIMING_CONTRACT
    tables = {
        "finmind_object_transfer_samples": ("object_transfer", "id,dataset,partition,seconds,bytes,rows,observed_at_utc"),
        "finmind_object_derivation_samples": ("object_derivation", "id,dataset,partition,seconds,observed_at_utc,engine"),
        "finmind_pipeline_samples": ("pipeline_calibration", "id,dataset,requests,seconds,rows,configuration_sha256,observed_at_utc"),
        "finmind_json_pipeline_samples": ("json_pipeline_calibration", "id,dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc,fetch_validate_publish_seconds,cycle_wall_verified,cycle_id,cycle_wall_seconds,excluded_work_seconds,cycle_started_at_utc,cycle_finished_at_utc"),
    }
    observations = []
    for owner, database in ((owner, database) for owner in ("free", "sponsor", "complement")
                            for database in ("queue.sqlite3", "update_observations.sqlite3")):
        owner_root = root if owner == "free" else root / owner
        path = owner_root / database
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
            for table, (stage, columns) in tables.items() if database == "queue.sqlite3" else ():
                if table not in existing:
                    continue
                actual = {r[1] for r in connection.execute(f"PRAGMA table_info({table})")}
                selected = [column for column in columns.split(",") if column in actual]
                rows, selection, queries = _finmind_sample_rows(connection, table, selected)
                observations.append({"owner": owner, "table": table, "stage": stage, "query": queries[0],
                                     "database": database, "database_relative_path": str(path.relative_to(root)),
                                     "snapshot_at_utc": datetime.now(UTC).isoformat(),
                                     "rows": rows, "selection": selection, "queries": queries})
            if database == "update_observations.sqlite3" and {"dataset_heads", "checks"} <= existing:
                rows, selection = _finmind_update_rows(connection)
                observations.append({"owner": owner, "table": "checks", "database": database,
                                     "database_relative_path": str(path.relative_to(root)),
                                     "stage": "request_to_parsed_response_wall_clock",
                                     "snapshot_at_utc": datetime.now(UTC).isoformat(),
                                     "rows": rows, "selection": selection})
            connection.rollback()
        except sqlite3.Error as exc:
            evidence.issues.append({"path": str(path), "reason": f"finmind_read_only_snapshot:{type(exc).__name__}"})
        finally:
            connection.close()
    snapshot = evidence.output / "finmind_telemetry_snapshot.json"
    atomic_write_json(snapshot, {"contract": "finmind_existing_timing_snapshot_v2",
                                "provider_calls": 0, "query_only": True, "observations": observations})
    captured = evidence.object(snapshot)
    if not captured:
        return
    _, sha = captured
    cycles = set()
    for observation in observations:
        for row in observation["rows"]:
            if observation["table"] == "checks":
                try:
                    payload = json.loads(row["metadata_json"])
                    started = datetime.fromisoformat(payload["request_started_at_utc"])
                    finished = datetime.fromisoformat(payload["checked_at_utc"])
                    elapsed = (finished - started).total_seconds()
                    timing = payload.get("transport_timing")
                    monotonic = (isinstance(timing, dict) and timing.get("contract") == TRANSPORT_TIMING_CONTRACT
                                 and finite_seconds(timing.get("seconds")))
                    valid = (started.utcoffset() is not None and finished.utcoffset() is not None
                             and row["request_id"] == payload.get("request_id")
                             and row["checked_at_utc"] == payload["checked_at_utc"]
                             and (monotonic or (elapsed >= 0 and finite_seconds(payload.get("network_seconds"))
                                  and math.isclose(elapsed, payload["network_seconds"], rel_tol=1e-9, abs_tol=1e-6))))
                except (KeyError, TypeError, ValueError, AttributeError):
                    valid = False
                if not valid:
                    evidence.issues.append({"path": str(root / observation["database_relative_path"]),
                                            "reason": "finmind_latest_success_without_consistent_transport_clock",
                                            "row_id": row["id"], "dataset": row.get("dataset")})
                    continue
                context = {"owner": observation["owner"], "dataset": row["dataset"],
                           "partition": row["partition"], "data_id": row["data_id"],
                           "request_id": row["request_id"], "event": row["kind"],
                           "request_started_at_utc": payload["request_started_at_utc"],
                           "response_received_at_utc": payload["checked_at_utc"],
                           "clock_basis": TRANSPORT_TIMING_CONTRACT if monotonic else "recorded_UTC_difference_not_monotonic",
                           "input_identity": "producer_request",
                           "scope": "request_start_through_basic_response_validation_excludes_rate_limit_product_validation_and_publication; batch response may fan out"}
                ledger.add(workflow=f"acquisition:finmind:{observation['owner']}:{row['dataset']}",
                           stage="request_to_parsed_response_monotonic" if monotonic else observation["stage"],
                           seconds=timing["seconds"] if monotonic else payload["network_seconds"], source=sha,
                           locator=f"{observation['owner']}/checks/{row['id']}/metadata_json/" + (
                               "transport_timing/seconds" if monotonic else "network_seconds"),
                           origin="recorded_finmind_telemetry", metric="wall_seconds" if monotonic else "wall_clock_delta_seconds", context=context,
                           work={"units": payload.get("response_rows"), "unit": "provider_response_rows",
                                 "stored_task_rows": payload.get("rows")})
                continue
            context = {"owner": observation["owner"], "dataset": row.get("dataset"),
                       "configuration_sha256": row.get("configuration_sha256"), "engine": row.get("engine"),
                       "partition": row.get("partition"), "requests": row.get("requests"),
                       "nonempty_requests": row.get("nonempty_requests"),
                       "scope": "producer_calibration_not_full_history_or_entire_service"}
            locator = f"{observation['owner']}/{observation['table']}/{row['id']}"
            allocated = observation["table"] == "finmind_json_pipeline_samples" and row.get("cycle_wall_verified") == 1
            if observation["table"] == "finmind_json_pipeline_samples":
                context.update(timing_basis="proportional_cycle_allocation" if allocated else "recorded_batch_wall",
                               cycle_id=row.get("cycle_id"), observed_at_utc=row.get("observed_at_utc"))
            ledger.add(workflow=f"acquisition:finmind:{observation['owner']}:{row.get('dataset')}",
                       stage=observation["stage"], seconds=row.get("seconds"), source=sha, locator=locator,
                       origin="recorded_finmind_telemetry", context=context,
                       metric="allocated_cycle_wall_seconds" if allocated else "wall_seconds",
                       work={"units": row.get("rows"), "unit": "rows", "bytes": row.get("bytes")})
            if observation["table"] == "finmind_json_pipeline_samples":
                direct_context = {**context, "timing_basis": "direct_monotonic_batch_wall",
                                  "scope": "overlapping fetch_validate_publish_batch; excludes shared cycle preparation/status/pool shutdown"}
                ledger.add(workflow=f"acquisition:finmind:{observation['owner']}:{row.get('dataset')}",
                           stage="json_fetch_validate_publish", seconds=row.get("fetch_validate_publish_seconds"),
                           source=sha, locator=locator + "/fetch_validate_publish_seconds",
                           origin="recorded_finmind_telemetry", context=direct_context,
                           work={"units": row.get("rows"), "unit": "rows", "requests": row.get("requests"),
                                 "nonempty_requests": row.get("nonempty_requests")})
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
        user_cpu = performance.get("child_cpu_user_seconds")
        system_cpu = performance.get("child_cpu_system_seconds")
        if (basis == "monotonic_child_launch_through_wait" and finite_seconds(seconds)
                and finite_seconds(user_cpu) and finite_seconds(system_cpu)):
            ledger.add(workflow=f"engineering:{row['name']}", stage="complete_command_reaped_children",
                       metric="cpu_seconds", seconds=user_cpu + system_cpu,
                       source=sha, locator=row["run_id"] + "/performance",
                       origin="recorded_engineering_command", outcome=row["state"],
                       context={"run_id": row["run_id"], "task_id": row["task_id"],
                                "clock_basis": "reaped_child_user_plus_system_resource_delta",
                                "native_cpu_user_seconds": user_cpu, "native_cpu_system_seconds": system_cpu,
                                "original_receipt_sha256": row["original_receipt_sha256"],
                                "producer_scope": performance.get("scope"),
                                "cpu_scope": "reaped children; excludes collector, owner parent and detached workers; overlaps complete command wall",
                                "workflow_code_sha256": row.get("workflow_code_sha256"),
                                "input_identity": "not_in_timing_projection"})


def _crypto_feature_rows(raw, provider):
    """Validate complete native worker clocks before emitting any samples."""
    from downloader.binance_historical_features import FEATURE_STAGE_IDS as BINANCE_STAGES
    from downloader.okx_historical_features import FEATURE_STAGE_IDS as OKX_STAGES
    stages = {"binance": set(BINANCE_STAGES), "okx": set(OKX_STAGES)}
    if provider not in stages:
        raise ValueError("unsupported crypto feature provider")
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    required = {"code", provider + "_symbol", "status", "rows", "output_path", "changed",
                "stage_status_json", "coverage_json", "errors_json",
                "stage_elapsed_seconds_json", "total_elapsed_seconds"}
    fields = reader.fieldnames or []
    if not required <= set(fields) or len(fields) != len(set(fields)):
        raise ValueError("crypto feature CSV headers differ")
    rows, codes = [], set()
    local = {"read_existing", "derive", "compare", "write", "coverage"}
    for row in reader:
        if len(rows) >= 10_000 or None in row or any(value is None for value in row.values()):
            raise ValueError("crypto feature CSV row shape or budget differs")
        if not row["code"] or row["code"] in codes or not row[provider + "_symbol"] or not row["status"]:
            raise ValueError("crypto feature worker identity differs")
        codes.add(row["code"])
        elapsed = json.loads(row["stage_elapsed_seconds_json"])
        status = json.loads(row["stage_status_json"])
        total = float(row["total_elapsed_seconds"])
        if (not isinstance(elapsed, dict) or not isinstance(status, dict)
                or not elapsed.keys() <= stages[provider] | local
                or not status.keys() <= stages[provider]
                or not all(isinstance(value, str) for value in status.values())
                or not all(finite_seconds(value) for value in elapsed.values())
                or not finite_seconds(total) or int(row["rows"]) < 0):
            raise ValueError("crypto feature native clock or stage differs")
        if elapsed and ("read_existing" not in elapsed
                        or any(value > total + .000001 for value in elapsed.values())):
            raise ValueError("crypto feature stage exceeds rounded worker total")
        row.update(_elapsed=elapsed, _stage_status=status, _total=total)
        rows.append(row)
    if not rows:
        raise ValueError("crypto feature report has no workers")
    return rows


def collect_crypto_feature_samples(ledger, provider, report_path, report_raw, report_sha,
                                   summary_raw, summary_sha):
    """Reuse native CEX clocks; stage sums and stored rows are not work units."""
    from downloader.feature_stage_timing import stage_latency_summary
    rows = _crypto_feature_rows(report_raw, provider)
    summary = json.loads(summary_raw)
    if (not isinstance(summary, dict) or summary.get("interval") != "1m"
            or summary.get("historical_features_enabled") is not True
            or summary.get("historical_feature_report_is_current_run") is not True
            or summary.get("asset_class") != "crypto_" + provider + "_" + ("usdm_perp" if provider == "binance" else "perp")
            or not isinstance(summary.get("historical_feature_report"), str)
            or not isinstance(summary.get("started_at_utc"), str)
            or not isinstance(summary.get("ended_at_utc"), str)):
        raise ValueError("crypto feature summary scope differs")
    if (ROOT / summary["historical_feature_report"]).resolve() != Path(report_path).resolve():
        raise ValueError("crypto feature summary belongs to a different report path")
    for field in ("work_elapsed_seconds", "lock_wait_seconds", "elapsed_seconds"):
        value = summary.get(field)
        if type(value) in (int, float) and not math.isfinite(value):
            raise ValueError("crypto feature summary contains a nonfinite clock")
    reconstructed = stage_latency_summary(SimpleNamespace(
        stage_elapsed_seconds_json=row["stage_elapsed_seconds_json"]) for row in rows)
    if reconstructed != summary.get("historical_feature_stage_latency"):
        raise ValueError("crypto feature CSV and summary stage clocks differ")
    empty, emitted = [], 0
    for index, row in enumerate(rows, 1):
        if not row["_elapsed"]:
            empty.append(dict(record=index, code=row["code"], status=row["status"],
                reason="empty_stage_clock_default_is_not_a_measured_worker",
                reported_total=row["total_elapsed_seconds"]))
            continue
        base = dict(provider=provider, instrument_code=row["code"],
            instrument_symbol=row[provider + "_symbol"], report_path=str(report_path),
            report_record=index, report_status=row["status"], summary_sha256=summary_sha,
            producer_started_at_utc=summary["started_at_utc"], producer_ended_at_utc=summary["ended_at_utc"],
            grain="1m", clock_basis="producer_reported_perf_counter_rounded_6_observed_source_not_loaded_identity_proof",
            scope="per_instrument_worker_stages_may_include_limiter_retries_fetch_parse_and_merge",
            existing_output_rows_not_work_denominator=int(row["rows"]), source_binding_eligible=False,
            whole_loaded_source_identity_verified=False, acquisition_completeness_verified=False,
            live_readiness_verified=False, full_source_end_to_end_verified=False,
            cross_file_atomic_snapshot_verified=False)
        for stage, seconds in row["_elapsed"].items():
            ledger.add(workflow="crypto-features:" + provider + ":" + row["code"], stage=stage,
                seconds=seconds, source=report_sha,
                locator=f"csv/records/{index}/stage_elapsed_seconds_json/{stage}",
                origin="recorded_crypto_feature_worker",
                outcome=row["_stage_status"].get(stage, "returned_result:" + row["status"]),
                context=dict(base, native_stage_status=row["_stage_status"].get(stage)))
            emitted += 1
        ledger.add(workflow="crypto-features:" + provider + ":" + row["code"], stage="worker_return_total",
            seconds=row["_total"], source=report_sha, locator=f"csv/records/{index}/total_elapsed_seconds",
            origin="recorded_crypto_feature_worker", outcome=row["status"], context=base)
        emitted += 1
    run_clock = summary.get("work_elapsed_seconds")
    if finite_seconds(run_clock):
        ledger.add(workflow="crypto-features:" + provider, stage="run_work_before_summary_publication",
            seconds=run_clock, source=summary_sha, locator="/work_elapsed_seconds",
            origin="recorded_crypto_feature_run", outcome="producer_returned_summary_not_acquisition_acceptance",
            context=dict(provider=provider, grain="1m", clock_basis="producer_monotonic_observed_source",
                scope="main_work_excludes_lock_wait_and_later_summary_receipt_publication",
                source_binding_eligible=False, whole_loaded_source_identity_verified=False,
                acquisition_completeness_verified=False, cross_file_atomic_snapshot_verified=False))
    return dict(provider=provider, report_rows=len(rows), workers_with_stage_clocks=len(rows) - len(empty),
        worker_stage_and_total_samples=emitted, empty_stage_clock_gaps=empty,
        source_report_sha256=report_sha, source_summary_sha256=summary_sha,
        stage_summary_exactly_reconstructed=True, per_stage=reconstructed,
        recorded_run_work_seconds=run_clock, recorded_lock_wait_seconds=summary.get("lock_wait_seconds"),
        utc_duration_estimate_not_monotonic=summary.get("elapsed_seconds"),
        aggregate_worker_seconds_is_not_run_wall=True, full_source_or_loaded_identity_verified=False)


def collect_crypto_feature_operations(ledger, evidence, reports):
    """Freeze explicit native CSV/summary pairs without launching source work."""
    seen, count = set(), 0
    for provider, report, summary in reports:
        report, summary = Path(report).absolute(), Path(summary).absolute()
        captured, captured_summary = evidence.read(report), evidence.read(summary)
        if captured is None or captured_summary is None:
            evidence.issues.append({"path": str(report), "summary": str(summary),
                                    "reason": "crypto_feature_clock_pair_unavailable"})
            continue
        identity = provider, str(report.resolve()), captured[1], captured_summary[1]
        if identity in seen:
            continue
        pending = Ledger()
        try:
            result = collect_crypto_feature_samples(pending, provider, report, *captured,
                                                     *captured_summary)
        except (KeyError, TypeError, ValueError, UnicodeError, csv.Error, OverflowError) as exc:
            evidence.issues.append({"path": str(report), "summary": str(summary),
                                    "reason": "invalid_crypto_feature_clock_pair", "error_type": type(exc).__name__})
            continue
        seen.add(identity)
        ledger.samples.extend(pending.samples)
        ledger.inventory.append(dict(kind="crypto_feature_report", id=provider + ":" + str(report),
            contract="native_crypto_feature_csv_summary_v1", summary_path=str(summary),
            coverage="captured_native_worker_and_run_clocks_only", **result))
        count += len(pending.samples)
    return count


CRYPTO_ISSUER_PARSER_SCOPE = (
    "captured_raw_read_SHA_gzip_decompressed_SHA_and_native_parse_excludes_imports_capture_and_output_write"
)
CRYPTO_ISSUER_STREAMS = {
    "daily_metrics": ("issuer_daily_fund_metrics.parquet", ("source_id", "ticker", "event_date", "metric")),
    "holdings": ("issuer_holdings_snapshots.parquet", ("source_id", "ticker", "as_of_date", "holding_ticker")),
    "reserves": ("issuer_reserve_snapshots.parquet", ("source_id", "ticker", "snapshot_at_utc")),
}


def _crypto_issuer_parser_samples(ledger, evidence, captured):
    """Read the retained-parser ABI; these clocks exclude external acquisition."""
    import polars as pl

    (timing, timing_sha), (protocol, protocol_sha), (outputs, outputs_sha), (readback, readback_sha) = captured

    def require(condition, reason):
        if not condition:
            raise ValueError(reason)

    def nonnegative_int(value):
        return type(value) is int and value >= 0 and finite_seconds(value / 1e9)

    require(all(isinstance(value, dict) for value in (timing, protocol, readback)) and isinstance(outputs, list),
            "issuer parser bundle shape differs")
    require(timing.get("state") == "measured_all_seven_retained_native_issuer_parsers"
            and timing.get("scope") == CRYPTO_ISSUER_PARSER_SCOPE
            and timing.get("profiler_enabled") is False and timing.get("source_modified") is False
            and timing.get("full_acquisition_measured") is False and type(timing.get("new_provider_calls")) is int
            and timing["new_provider_calls"] == 0
            and timing.get("parser_loaded_from_exact_archived_source") is True
            and timing.get("native_result_request_counts_are_preserved_metadata_not_new_HTTP") is True,
            "issuer parser clock scope differs")
    require(all(protocol.get(key) is True for key in (
                "native_parser_only", "no_new_provider_or_GPU_jobs", "all_seven_issuer_sources_required",
                "complete_original_normalized_row_oracle_required"))
            and all(protocol.get(key) is False for key in (
                "source_or_receipts_modified", "source_bindings_globally_recomputed", "full_acquisition_measured")),
            "issuer parser protocol scope differs")
    require(readback.get("state") == "verified_all_seven_native_outputs_against_original_normalized_rows"
            and readback.get("output_sha256") == outputs_sha
            and readback.get("original_observation_clocks_and_raw_SHA_preserved") is True
            and readback.get("source_stage_bindings_globally_recomputed") is False
            and readback.get("acquisition_completeness_verified") is False,
            "issuer parser output oracle differs")
    require(isinstance(protocol.get("inputs"), list) and 0 < len(protocol["inputs"]) <= 256
            and isinstance(protocol.get("code"), list) and 0 < len(protocol["code"]) <= 64,
            "issuer parser evidence budget differs")
    inputs, code, original_bytes = {}, {}, 0
    for collection, destination in ((protocol["inputs"], inputs), (protocol["code"], code)):
        for row in collection:
            require(isinstance(row, dict) and isinstance(row.get("path"), str)
                    and isinstance(row.get("archive"), str) and row["path"] not in destination,
                    "issuer parser evidence identity differs")
            body = evidence.read(Path(row["archive"]))
            require(body is not None and body[1] == row.get("sha256"), "issuer parser archived SHA differs")
            if destination is inputs:
                require(row.get("stable") is True and type(row.get("bytes")) is int
                        and row["bytes"] == len(body[0]), "issuer parser archived size differs")
                original_bytes += row["bytes"]
            destination[row["path"]] = row
    require(type(protocol.get("original_bytes")) is int and original_bytes == protocol["original_bytes"],
            "issuer parser original byte denominator differs")

    def original(path):
        row = inputs[str(path)]
        body = evidence.read(Path(row["archive"]))
        require(body is not None and body[1] == row["sha256"], "issuer parser original unavailable")
        return body[0]

    config = json.loads(original(ROOT / "configs/crypto_etf_sources.json"))
    specs, issuers, clocks = config["issuer_sources"], protocol.get("issuers"), timing.get("per_issuer")
    require(isinstance(specs, list) and isinstance(issuers, list) and isinstance(clocks, list)
            and 0 < len(specs) == len(issuers) == len(clocks) == len(outputs) <= 128,
            "issuer parser complete source set differs")
    owner = code[str(ROOT / "downloader/download_crypto_etf_history.py")]
    adapters = {"ishares_gross_proceeds_xls": "_parse_ishares_xls",
                "ishares_holdings_csv": "_parse_ishares_holdings", "bitwise_next_data": "_parse_bitwise"}
    ids, work, raw_bytes, wall_sum, cpu_sum = set(), 0, 0, 0, 0
    verified = []
    for spec, item, clock, parsed in zip(specs, issuers, clocks, outputs):
        require(all(isinstance(value, dict) for value in (spec, item, clock, parsed))
                and isinstance(spec.get("id"), str) and spec["id"] and spec["id"] not in ids
                and spec.get("adapter") in adapters and item.get("spec") == spec,
                "issuer parser source definition differs")
        identifier = spec["id"]
        ids.add(identifier)
        require(clock.get("source_endpoint_id") == "crypto-etf:issuer:" + identifier
                and isinstance(item.get("raw_path"), str), "issuer parser source clock differs")
        receipt = json.loads(original(ROOT / "data_crypto_etf/receipts/issuers" / (identifier + ".json")))
        require(isinstance(receipt, dict)
                and all(receipt.get(key) == item.get(key) for key in ("raw_path", "raw_sha256", "observed_at_utc"))
                and isinstance(item.get("observed_at_utc"), str) and item["observed_at_utc"],
                "issuer parser native receipt differs")
        raw = original(item["raw_path"])
        if Path(item["raw_path"]).suffix == ".gz":
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                raw = stream.read(evidence.max_file_bytes + 1)
        require(len(raw) <= evidence.max_file_bytes and hashlib.sha256(raw).hexdigest() == item.get("raw_sha256")
                and clock.get("raw_sha256") == item["raw_sha256"], "issuer parser decompressed SHA differs")
        result = parsed.get("result")
        require(isinstance(result, dict) and result.get("source_id") == identifier
                and result.get("provider") == spec["provider"] and result.get("status") == "complete"
                and all(isinstance(parsed.get(key), list) for key in CRYPTO_ISSUER_STREAMS),
                "issuer parser native result differs")
        rows = sum(len(parsed[key]) for key in CRYPTO_ISSUER_STREAMS)
        require(type(result.get("rows")) is int and result["rows"] == rows
                and type(clock.get("output_rows")) is int and clock["output_rows"] == rows
                and type(clock.get("input_bytes")) is int and clock["input_bytes"] == len(raw)
                and all(nonnegative_int(clock.get(key)) for key in ("wall_ns", "process_cpu_ns")),
                "issuer parser clock or work denominator differs")
        for key in CRYPTO_ISSUER_STREAMS:
            for row in parsed[key]:
                require(isinstance(row, dict) and row.get("source_id") == identifier
                        and row.get("raw_sha256") == item["raw_sha256"]
                        and row.get("observed_at_utc") == row.get("available_at_utc") == item["observed_at_utc"],
                        "issuer parser returned provenance differs")
        verified.append((spec, item, clock, result))
        wall_sum += clock["wall_ns"]
        cpu_sum += clock["process_cpu_ns"]
        work += rows
        raw_bytes += len(raw)
    require(raw_bytes <= evidence.max_total_bytes
            and nonnegative_int(timing.get("native_workflow_wall_ns"))
            and nonnegative_int(timing.get("native_workflow_process_cpu_ns"))
            and timing["native_workflow_wall_ns"] >= wall_sum
            and timing["native_workflow_process_cpu_ns"] >= cpu_sum,
            "issuer parser sequential workflow clock differs")
    streams = []
    for stream, (filename, keys) in CRYPTO_ISSUER_STREAMS.items():
        original_rows = pl.read_parquet(io.BytesIO(original(ROOT / "data_crypto_etf/normalized" / filename))).to_dicts()
        count = 0
        for item, parsed in zip(issuers, outputs):
            expected = {tuple(row[key] for key in keys): row for row in parsed[stream]}
            matched = [row for row in original_rows if row["source_id"] == item["spec"]["id"]
                       and row["raw_sha256"] == item["raw_sha256"]]
            actual = {tuple(row[key] for key in keys): row for row in matched}
            require(len(actual) == len(matched) and expected.keys() == actual.keys(),
                    "issuer parser complete normalized keys differ")
            for key, row in expected.items():
                require(digest(row) == digest({field: actual[key][field] for field in row}),
                        "issuer parser complete normalized values differ")
            count += len(expected)
        streams.append(dict(stream=stream, complete_current_raw_rows_verified=count,
                            native_ordered_rows_compared_between_both_full_calls=True))
    require(streams == readback.get("streams"), "issuer parser normalized row counts differ")
    base = dict(contract="observed_retained_crypto_issuer_parser_v1", scope=CRYPTO_ISSUER_PARSER_SCOPE,
        input_protocol_sha256=protocol_sha, native_outputs_sha256=outputs_sha, native_readback_sha256=readback_sha,
        parser_source_sha256=owner["sha256"], input_identity="retained_native_issuer_raw",
        source_capsule_capture_excluded=True, returned_row_capture_included_in_workflow_total=True,
        imports_and_output_write_excluded=True, source_cache="previous_capture_may_be_warm",
        clock_is_source_publication_time=False, new_provider_calls=0,
        native_request_counts_are_preserved_metadata=True, producer_reported_exact_archived_parser=True,
        whole_loaded_source_identity_verified=False, acquisition_completeness_verified=False,
        live_readiness_verified=False, stage_totals_overlap=True,
        pandas_version=timing.get("pandas_version"), polars_version=timing.get("polars_version"))
    for index, (spec, item, clock, result) in enumerate(verified):
        context = dict(base, dataset=spec["id"], source_binding_eligible=True,
            parser_function=adapters[spec["adapter"]], raw_sha256=item["raw_sha256"],
            native_observed_at_utc=item["observed_at_utc"], native_result_requests=result.get("requests"))
        for field, metric in (("wall_ns", "wall_seconds"), ("process_cpu_ns", "process_cpu_seconds")):
            ledger.add(workflow="preparation:crypto_etf_issuer", stage="retained_raw_verify_decode_parse",
                seconds=clock[field] / 1e9, source=timing_sha, locator=f"per_issuer/{index}/{field}",
                origin="observed_crypto_issuer_parser", outcome="verified_retained_native_output", metric=metric,
                context=dict(context, native_nanoseconds=clock[field]),
                work={"unit": "parsed_output_rows", "units": clock["output_rows"], "input_bytes": clock["input_bytes"]})
    for field, metric in (("native_workflow_wall_ns", "wall_seconds"),
                          ("native_workflow_process_cpu_ns", "process_cpu_seconds")):
        ledger.add(workflow="preparation:crypto_etf_issuer", stage="all_retained_issuers_with_result_capture",
            seconds=timing[field] / 1e9, source=timing_sha, locator=field,
            origin="observed_crypto_issuer_parser", outcome="verified_retained_native_output", metric=metric,
            context=dict(base, source_binding_eligible=False, native_nanoseconds=timing[field]),
            work={"unit": "parsed_output_rows", "units": work, "input_bytes": raw_bytes})
    return dict(issuers=len(issuers), native_rows=work, complete_normalized_streams=streams,
                protocol_sha256=protocol_sha, outputs_sha256=outputs_sha, readback_sha256=readback_sha,
                frozen_originals_verified=len(inputs), full_acquisition_verified=False)


def collect_crypto_issuer_parser_operations(ledger, evidence, reports):
    """Keep complete report bundles and visible failures; never launch a parser."""
    if not reports:
        return 0
    from polars.exceptions import PolarsError
    from zlib import error as ZlibError

    seen, emitted = set(), 0
    for report in reports:
        paths = [Path(path).absolute() for path in report]
        captured = [evidence.object(path) for path in paths]
        if len(captured) != 4 or any(item is None for item in captured):
            evidence.issues.append(dict(paths=[str(path) for path in paths], reason="crypto_issuer_parser_bundle_unavailable"))
            continue
        identity = tuple(item[1] for item in captured)
        if identity in seen:
            continue
        pending = Ledger()
        try:
            result = _crypto_issuer_parser_samples(pending, evidence, captured)
        except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, OSError, EOFError,
                PolarsError, ZlibError) as exc:
            evidence.issues.append(dict(paths=[str(path) for path in paths],
                reason="invalid_crypto_issuer_parser_bundle", error_type=type(exc).__name__))
            continue
        seen.add(identity)
        ledger.samples.extend(pending.samples)
        ledger.inventory.append(dict(kind="crypto_issuer_parser_report", id=str(paths[0]),
            contract="observed_retained_crypto_issuer_parser_v1", source_report_sha256=captured[0][1],
            coverage="observed_retained_parser_stage_only", **result))
        emitted += len(pending.samples)
    return emitted


RETAINED_DUNE_PARTITION_SCOPE = (
    "retained_page_read_SHA_gzip_JSON_native_partition_transform_and_atomic_outputs_with_receipt_supplied_status_and_clock_excludes_HTTP_capture_imports_and_verification"
)


class RetainedReplayContractError(ValueError):
    """A static replay-contract reason safe to retain with rejected evidence."""


def _replay_require(condition, reason):
    if not condition:
        raise RetainedReplayContractError(reason)


def _replay_nonnegative_int(value):
    return type(value) is int and value >= 0


def _retained_replay_inputs(evidence, protocol):
    """Retain originals in order, with bounded speculative input reads."""
    require = _replay_require
    require(isinstance(protocol.get("inputs"), list) and 0 < len(protocol["inputs"]) <= 8192
            and isinstance(protocol.get("code"), list) and 0 < len(protocol["code"]) <= 64,
            "retained replay evidence budget differs")
    inputs, code, total = {}, {}, 0
    # This is only acceleration eligibility. The original ordered checks below
    # still determine every error and all retained evidence. Malformed later
    # rows must not move an exception ahead of an earlier archive failure.
    prefetch, paths, declared_bytes = True, [], 0
    for collection in (protocol["inputs"], protocol["code"]):
        identities = set()
        for row in collection:
            if (not isinstance(row, dict) or not isinstance(row.get("path"), str)
                    or not isinstance(row.get("archive"), str) or row["path"] in identities):
                prefetch = False
                break
            identities.add(row["path"])
            if collection is protocol["inputs"]:
                if (row.get("stable") is not True or not _replay_nonnegative_int(row.get("bytes"))
                        or row["bytes"] > evidence.max_file_bytes):
                    prefetch = False
                    break
                declared_bytes += row["bytes"]
                try:
                    path = Path(row["archive"])
                    encoded = os.fsencode(path)
                    if b"\0" in encoded:
                        prefetch = False
                        break
                except (OSError, TypeError, ValueError, UnicodeError):
                    prefetch = False
                    break
                paths.append(path)
        if not prefetch:
            break
    prefetch = (prefetch and _replay_nonnegative_int(protocol.get("original_bytes"))
                and protocol["original_bytes"] == declared_bytes
                and declared_bytes <= evidence.max_total_bytes - evidence.total_bytes)
    # Two 2 MiB batches pending plus one consumed; native conversion is bounded
    # by (2 * workers + 1) * (batch_bytes + 1), just over 10 MiB of raw bytes.
    # Code archives are few and remain synchronous. Previously frozen inputs
    # are read from their own snapshots instead of speculatively reopening them.
    skip = [path for path in paths if str(path.absolute()) in evidence.sources] if prefetch else ()
    reads = evidence.ordered_reads(paths if prefetch else (), workers=2 if prefetch else 0,
                                   skip=skip, batch_size=32, batch_bytes=2 << 20)
    with closing(reads):
        for collection, destination in ((protocol["inputs"], inputs), (protocol["code"], code)):
            for row in collection:
                require(isinstance(row, dict) and isinstance(row.get("path"), str)
                        and isinstance(row.get("archive"), str) and row["path"] not in destination,
                        "retained replay evidence identity differs")
                prepared = next(reads)[1] if prefetch and destination is inputs else None
                archive = Path(row["archive"])
                body = (evidence.read(archive, prepared=prepared) if prepared is not None
                        else evidence.read(archive))
                require(body is not None and body[1] == row.get("sha256"), "retained replay archive SHA differs")
                if destination is inputs:
                    require(row.get("stable") is True and _replay_nonnegative_int(row.get("bytes"))
                            and row["bytes"] == len(body[0]), "retained replay archive size differs")
                    total += row["bytes"]
                destination[row["path"]] = row
    require(_replay_nonnegative_int(protocol.get("original_bytes")) and protocol["original_bytes"] == total,
            "retained replay original byte denominator differs")

    def original(path):
        row = inputs[str(path)]
        body = evidence.read(Path(row["archive"]))
        require(body is not None and body[1] == row["sha256"], "retained replay original unavailable")
        return body[0]

    def output(path):
        body = evidence.read(Path(path))
        require(body is not None, "retained replay native output unavailable")
        return body

    return inputs, code, original, output


def _replay_gzip(raw, limit):
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
        result = stream.read(limit + 1)
    _replay_require(len(result) <= limit, "retained replay expanded page exceeds budget")
    return result


def _replay_frame_equal(actual, original):
    """Keep null masks and signed floating bits in addition to ordered values."""
    import numpy as np
    import polars as pl

    require = _replay_require
    require(dict(actual.schema) == dict(original.schema) and actual.shape == original.shape,
            "retained replay normalized schema or shape differs")
    require(actual.select(original.columns).equals(original, null_equal=True), "retained replay ordered values differ")
    for name, dtype in original.schema.items():
        if dtype in (pl.Float32, pl.Float64):
            a, b = actual[name], original[name]
            require(a.is_null().equals(b.is_null()), "retained replay null masks differ")
            bits = np.uint32 if dtype == pl.Float32 else np.uint64
            require(np.array_equal(a.fill_null(0).to_numpy().view(bits), b.fill_null(0).to_numpy().view(bits)),
                    "retained replay floating bits differ")


def _retained_dune_partition_samples(ledger, evidence, captured):
    import polars as pl

    require = _replay_require
    (timing, timing_sha), (protocol, protocol_sha), (readback, readback_sha) = captured
    require(all(isinstance(value, dict) for value in (timing, protocol, readback)), "Dune replay bundle shape differs")
    require(timing.get("state") == "measured_all_complete_retained_Dune_partitions"
            and timing.get("scope") == RETAINED_DUNE_PARTITION_SCOPE and timing.get("profiler_enabled") is False
            and type(timing.get("new_provider_calls")) is int and timing["new_provider_calls"] == 0
            and type(timing.get("new_provider_executions")) is int and timing["new_provider_executions"] == 0
            and timing.get("status_and_retrieval_clock_supplied_from_retained_receipts") is True
            and timing.get("native_source_loaded_from_exact_archive") is True
            and timing.get("full_external_acquisition_verified") is False
            and timing.get("global_source_bindings_recomputed") is False, "Dune replay clock scope differs")
    require(protocol.get("state") == "prepared_all_complete_retained_Dune_partition_replays"
            and all(protocol.get(key) is True for key in ("no_source_modification",
                "original_observation_clocks_supplied_from_receipts", "complete_ordered_row_and_raw_page_oracle_required",
                "numerical_values_null_masks_and_floating_bits_required"))
            and protocol.get("full_external_acquisition_measured") is False
            and protocol.get("source_bindings_globally_recomputed") is False
            and all(type(protocol.get(key)) is int and protocol[key] == 0
                    for key in ("new_provider_calls", "new_provider_executions")),
            "Dune replay protocol scope differs")
    require(readback.get("state") == "verified_both_full_retained_Dune_replays_against_every_original_row_and_page"
            and readback.get("native_timing_sha256") == timing_sha
            and readback.get("source_or_receipts_modified") is False
            and readback.get("no_new_provider_calls") is True
            and readback.get("full_external_acquisition_verified") is False
            and readback.get("global_source_bindings_recomputed") is False
            and readback.get("whole_production_loaded_identity_verified") is False,
            "Dune replay readback scope differs")
    inputs, code, original, output = _retained_replay_inputs(evidence, protocol)
    owner = code[str(ROOT / "downloader/download_dune_crypto_history.py")]
    root = ROOT / "data_dune_crypto"
    config = json.loads(original(ROOT / "configs/dune_crypto_queries.json"))
    require(isinstance(config, dict) and isinstance(config.get("queries"), list)
            and all(isinstance(row, dict) for row in config["queries"]), "Dune query config shape differs")
    definitions = {row["id"]: row for row in config["queries"]}
    require(len(definitions) == len(config["queries"]), "Dune duplicate query definitions")
    items, clocks, checks = protocol.get("partitions"), timing.get("partitions"), readback.get("partitions")
    require(all(isinstance(rows, list) for rows in (items, clocks, checks))
            and 0 < len(items) == len(clocks) == len(checks) <= 8192, "Dune replay complete partition set differs")
    receipt_paths = {name for name in inputs if Path(name).is_relative_to(root / "receipts")}
    supplied, pages, verified, rows_total, compressed_total = set(), set(), [], 0, 0
    wall_sum, cpu_sum = 0, 0
    for index, (item, clock, check) in enumerate(zip(items, clocks, checks)):
        require(all(isinstance(row, dict) for row in (item, clock, check)), "Dune replay partition shape differs")
        receipt_path = item["receipt_path"]
        receipt = json.loads(original(receipt_path))
        require(isinstance(receipt, dict) and receipt == item.get("receipt") and receipt.get("status") == "complete"
                and receipt_path not in supplied
                and all(_replay_nonnegative_int(receipt.get(key)) for key in ("rows", "pages")),
                "Dune replay original receipt differs")
        supplied.add(receipt_path)
        spec = item["spec"]
        require(spec == definitions[receipt["query_id"]] and spec["id"] == receipt["query_id"]
                and spec["expected_columns"] == receipt["expected_columns"]
                and spec["primary_key"] == receipt["primary_key"]
                and hashlib.sha256(original(item["sql_path"])).hexdigest() == receipt["sql_sha256"],
                "Dune replay query definition differs")
        require(clock.get("source_endpoint_id") == "dune-query:" + spec["id"]
                and clock.get("partition_id") == receipt["partition_id"]
                and clock.get("original_receipt_path") == receipt_path
                and all(_replay_nonnegative_int(clock.get(key)) for key in ("wall_ns", "process_cpu_ns", "rows", "pages"))
                and clock["rows"] == receipt["rows"] and clock["pages"] == receipt["pages"],
                "Dune replay source clock or work differs")
        require(check.get("query_id") == spec["id"] and check.get("partition_id") == receipt["partition_id"]
                and check.get("rows") == receipt["rows"]
                and check.get("original_receipt_sha256") == inputs[receipt_path]["sha256"]
                and all(check.get(key) is True for key in ("all_ordered_values_schema_null_masks_and_floating_bits_equal",
                    "all_native_receipt_fields_except_verified_output_roots_equal",
                    "historical_metadata_column_order_retained_in_original_and_compared_by_name",
                    "both_full_replays_raw_uncompressed_page_bytes_equal")), "Dune replay partition oracle differs")
        original_frame = pl.read_parquet(io.BytesIO(original(receipt["parquet_path"])))
        actual_bytes, parquet_sha = output(clock["parquet_path"])
        actual = pl.read_parquet(io.BytesIO(actual_bytes))
        require(original_frame.height == receipt["rows"] and
                [name for name in actual.columns if name != "_dune_contract_id"] ==
                [name for name in original_frame.columns if name != "_dune_contract_id"], "Dune replay column order differs")
        _replay_frame_equal(actual, original_frame)
        relative = Path(receipt["parquet_path"]).relative_to(root)
        output_root = Path(clock["parquet_path"]).parents[len(relative.parts) - 1]
        require(output_root / relative == Path(clock["parquet_path"])
                and output_root / Path(receipt_path).relative_to(root) == Path(clock["receipt_path"]),
                "Dune replay output roots differ")
        receipt_bytes, receipt_sha = output(clock["receipt_path"])
        native_receipt = json.loads(receipt_bytes)
        migration = {key: receipt[key] for key in ("lineage_upgraded_at_utc", "migrated_from_partition_id") if key in receipt}
        raw_components = (spec["id"], receipt["partition_id"], receipt["execution_id"])
        require(all(isinstance(value, str) and value not in {"", ".", ".."}
                    and Path(value).name == value for value in raw_components),
                "Dune replay native raw path component differs")
        # Historical lineage migration preserves its old raw partition folder.
        # The canonical writer uses the current query/partition/execution tuple.
        native_raw_root = output_root.joinpath("raw", *raw_components)
        require(check.get("original_migration_metadata_retained") == migration
                and set(native_receipt) == set(receipt) - migration.keys()
                and {k:v for k,v in native_receipt.items() if k not in {"parquet_path", "raw_root"}} ==
                    {k:v for k,v in receipt.items() if k not in {"parquet_path", "raw_root"} | migration.keys()}
                and native_receipt["parquet_path"] == clock["parquet_path"]
                and Path(native_receipt["raw_root"]) == native_raw_root,
                "Dune replay native receipt fields differ")
        partition_pages = item["pages"]
        require(isinstance(partition_pages, list) and len(partition_pages) == len(set(partition_pages)) == receipt["pages"]
                and receipt["pages"] > 0 and not pages.intersection(partition_pages), "Dune replay page set differs")
        page_rows, compressed = 0, 0
        for path in partition_pages:
            require(Path(path).parent == Path(receipt["raw_root"]), "Dune replay original page root differs")
            body = original(path)
            plain = _replay_gzip(body, evidence.max_file_bytes)
            actual_page, _ = output(Path(native_receipt["raw_root"]) / Path(path).name)
            require(plain == _replay_gzip(actual_page, evidence.max_file_bytes), "Dune replay raw page bytes differ")
            payload = json.loads(plain)
            require(isinstance(payload, dict), "Dune replay raw page shape differs")
            result = payload.get("result") or {}
            require(isinstance(result, dict), "Dune replay raw result shape differs")
            values = result.get("rows") or []
            require(isinstance(values, list), "Dune replay raw row shape differs")
            page_rows += sum(isinstance(row, dict) for row in values)
            compressed += len(body)
        require(page_rows == receipt["rows"], "Dune replay raw row denominator differs")
        pages.update(partition_pages)
        rows_total += receipt["rows"]
        compressed_total += compressed
        wall_sum += clock["wall_ns"]
        cpu_sum += clock["process_cpu_ns"]
        verified.append((clock, dict(dataset=spec["id"], partition_id=receipt["partition_id"],
            original_receipt_sha256=inputs[receipt_path]["sha256"], native_receipt_sha256=receipt_sha,
            original_parquet_sha256=inputs[receipt["parquet_path"]]["sha256"], native_parquet_sha256=parquet_sha,
            pages=clock["pages"], compressed_original_page_bytes=compressed,
            original_available_at_utc=receipt["available_at_utc"], original_retrieved_at_utc=receipt["completed_at_utc"])))
    retained_pages = {name for name in inputs if Path(name).is_relative_to(root / "raw") and name.endswith(".json.gz")}
    require(supplied == receipt_paths and pages <= retained_pages
            and _replay_nonnegative_int(protocol.get("original_raw_pages"))
            and len(retained_pages) == protocol["original_raw_pages"]
            and protocol.get("completed_partition_raw_pages") == len(pages)
            and sorted(retained_pages - pages) == protocol.get("unreferenced_pages")
            and protocol.get("unreferenced_pages_are_not_completed_partition_evidence") is True
            and rows_total == protocol.get("original_rows") == readback.get("total_rows")
            and compressed_total == readback.get("raw_compressed_bytes"), "Dune replay complete coverage differs")
    require(all(_replay_nonnegative_int(timing.get(key)) for key in
                ("native_workflow_wall_ns", "native_workflow_process_cpu_ns"))
            and timing["native_workflow_wall_ns"] >= wall_sum
            and timing["native_workflow_process_cpu_ns"] >= cpu_sum, "Dune replay workflow clocks differ")
    base = dict(contract="observed_retained_dune_partition_v1", scope=RETAINED_DUNE_PARTITION_SCOPE,
        input_protocol_sha256=protocol_sha, native_readback_sha256=readback_sha,
        parser_source_sha256=owner["sha256"], input_identity="retained_Dune_pages_and_original_receipts",
        new_provider_calls=0, new_provider_executions=0, supplied_provider_status_and_clocks=True,
        imports_capture_and_verification_excluded=True, native_atomic_outputs_included=True,
        whole_loaded_source_identity_verified=False, acquisition_completeness_verified=False,
        live_readiness_verified=False, stage_totals_overlap=True, source_cache="capture_warmed_existing_pages")
    for index, (clock, context) in enumerate(verified):
        for field, metric in (("wall_ns", "wall_seconds"), ("process_cpu_ns", "process_cpu_seconds")):
            ledger.add(workflow="preparation:retained_dune", stage="retained_page_transform_and_atomic_outputs",
                seconds=clock[field] / 1e9, source=timing_sha, locator=f"partitions/{index}/{field}",
                origin="observed_retained_dune_partition", outcome="verified_retained_native_output", metric=metric,
                context=dict(base, **context, source_binding_eligible=True, native_nanoseconds=clock[field]),
                work={"unit": "normalized_output_rows", "units": clock["rows"], "pages": clock["pages"]})
    for field, metric in (("native_workflow_wall_ns", "wall_seconds"), ("native_workflow_process_cpu_ns", "process_cpu_seconds")):
        ledger.add(workflow="preparation:retained_dune", stage="all_complete_retained_partitions",
            seconds=timing[field] / 1e9, source=timing_sha, locator=field,
            origin="observed_retained_dune_partition", outcome="verified_retained_native_output", metric=metric,
            context=dict(base, source_binding_eligible=False, native_nanoseconds=timing[field]),
            work={"unit": "normalized_output_rows", "units": rows_total, "pages": len(pages)})
    return dict(partitions=len(items), native_rows=rows_total, completed_pages=len(pages),
        retained_other_execution_pages=len(retained_pages - pages), frozen_originals_verified=len(inputs),
        protocol_sha256=protocol_sha, readback_sha256=readback_sha, full_acquisition_verified=False)


RETAINED_SEC_NATIVE_SCOPE = (
    "retained_response_read_SHA_gzip_native_mapping_and_40_serial_cached_entity_workflows_with_atomic_outputs_excludes_HTTP_capture_imports_cache_staging_and_verification"
)


def _retained_sec_native_samples(ledger, evidence, captured):
    import polars as pl

    require = _replay_require
    (timing, timing_sha), (protocol, protocol_sha), (results, results_sha), (readback, readback_sha) = captured
    require(all(isinstance(row, dict) for row in (timing, protocol, readback)) and isinstance(results, list),
            "SEC replay bundle shape differs")
    require(timing.get("state") == "measured_all_retained_native_SEC_mapping_and_entities"
            and timing.get("scope") == RETAINED_SEC_NATIVE_SCOPE and timing.get("profiler_enabled") is False
            and type(timing.get("new_provider_calls")) is int and timing["new_provider_calls"] == 0
            and all(timing.get(key) is True for key in ("native_result_requests_are_replayed_metadata",
                "retrieval_and_manifest_clocks_supplied_from_retained_originals",
                "missing_companyfacts_branch_is_receipt_supplied_not_observed_HTTP_status",
                "native_source_loaded_from_exact_archive"))
            and timing.get("full_external_acquisition_verified") is False
            and timing.get("global_source_bindings_recomputed") is False, "SEC replay clock scope differs")
    require(protocol.get("state") == "prepared_complete_retained_SEC_mapping_and_40_entity_replays"
            and all(protocol.get(key) is True for key in ("native_cached_document_state_preserved",
                "complete_original_rows_manifests_and_documents_required", "numerical_values_null_masks_and_floating_bits_required",
                "status_and_clocks_replayed_from_retained_originals",
                "mapping_rows_are_entity_definitions_not_filing_or_companyfact_observations"))
            and all(protocol.get(key) is False for key in ("full_external_acquisition_measured",
                "production_worker_concurrency_reproduced", "source_bindings_globally_recomputed"))
            and all(type(protocol.get(key)) is int and protocol[key] == 0 for key in ("new_provider_calls", "new_gpu_jobs")),
            "SEC replay protocol scope differs")
    require(readback.get("state") == "verified_both_complete_SEC_replays_against_all_original_rows_manifests_and_documents"
            and readback.get("native_timing_sha256") == timing_sha
            and all(readback.get(key) is False for key in ("full_external_acquisition_verified",
                "production_concurrency_reproduced", "global_source_bindings_recomputed", "whole_loaded_production_identity_verified")),
            "SEC replay readback scope differs")
    inputs, code, original, output = _retained_replay_inputs(evidence, protocol)
    owner = code[str(ROOT / "downloader/download_crypto_etf_history.py")]
    root = ROOT / "data_crypto_etf"
    config = json.loads(original(ROOT / "configs/crypto_etf_sources.json"))
    require(config == protocol.get("config"), "SEC replay frozen config differs")
    mapping = json.loads(original(root / "receipts/sec/company_tickers.json"))
    require(isinstance(mapping, dict) and mapping == protocol.get("mapping_receipt") and mapping.get("missing_tickers") == [],
            "SEC replay original mapping receipt differs")
    mapping_raw = _replay_gzip(original(mapping["raw_path"]), evidence.max_file_bytes)
    require(hashlib.sha256(mapping_raw).hexdigest() == mapping["raw_sha256"], "SEC replay original mapping bytes differ")
    items, clocks, checks = protocol.get("entities"), timing.get("per_stage"), readback.get("entities")
    require(all(isinstance(rows, list) for rows in (items, clocks, checks))
            and len(items) == len(results) == len(checks) == 40 and len(clocks) == 41,
            "SEC replay complete entity set differs")
    require(all(isinstance(row, dict) for row in clocks)
            and all(_replay_nonnegative_int(row.get(key)) for row in clocks for key in ("wall_ns", "process_cpu_ns", "native_result_requests")),
            "SEC replay stage clocks differ")
    require(clocks[0].get("stage") == "resolve_mapping" and type(clocks[0].get("entities")) is int
            and clocks[0]["entities"] == mapping.get("resolved_entities") == 40,
            "SEC replay mapping definition count differs")
    summary = json.loads(original(root / "download_summary.json"))
    require(isinstance(summary, dict) and isinstance(summary.get("results"), list)
            and all(isinstance(row, dict) for row in summary["results"]), "SEC replay summary shape differs")
    sec_results = [row for row in summary["results"] if row.get("provider") == "SEC EDGAR"]
    original_results = {row["source_id"]: row for row in sec_results}
    require(len(original_results) == len(sec_results) == 41
            and original_results["sec_company_tickers"]["status"] == "complete"
            and original_results["sec_company_tickers"]["rows"] == 40,
            "SEC replay summary source set differs")
    supplied, identifiers, verified, rows_total, documents, output_root = set(), set(), [], 0, 0, None
    for item, clock, check, result in zip(items, clocks[1:], checks, results):
        require(all(isinstance(row, dict) for row in (item, check, result)), "SEC replay entity shape differs")
        manifest_path = item["manifest_path"]
        manifest = json.loads(original(manifest_path))
        require(isinstance(manifest, dict) and manifest == item.get("manifest") and manifest_path not in supplied
                and all(_replay_nonnegative_int(manifest.get(key)) for key in ("filing_rows", "companyfact_rows")),
                "SEC replay original manifest differs")
        supplied.add(manifest_path)
        cik = manifest["cik"]
        identifier = "sec_cik_" + cik
        identifiers.add(identifier)
        rows = manifest["filing_rows"] + manifest["companyfact_rows"]
        require(clock.get("stage") == "native_cached_entity" and clock.get("source_endpoint_id") == "crypto-etf:sec-edgar"
                and clock.get("source_entity_id") == identifier and clock.get("original_manifest_path") == manifest_path
                and type(clock.get("rows")) is int and clock["rows"] == rows
                and type(clock.get("cached_documents")) is int and clock["cached_documents"] == len(manifest["primary_documents"])
                and result.get("source_id") == identifier and result.get("status") == "complete"
                and result.get("provider") == "SEC EDGAR" and type(result.get("rows")) is int and result["rows"] == rows
                and type(result.get("requests")) is int and result["requests"] == clock["native_result_requests"]
                and original_results[identifier]["rows"] == rows and original_results[identifier]["status"] == "complete",
                "SEC replay native source clock or result differs")
        paths = result["output_paths"]
        require(isinstance(paths, list) and all(isinstance(path, str) for path in paths), "SEC replay native paths differ")
        filing_path, = [Path(path) for path in paths if Path(path).name == "filings.parquet"]
        entity_root = filing_path.parents[3]
        require(filing_path == entity_root / "normalized/sec" / cik / "filings.parquet"
                and (output_root is None or output_root == entity_root), "SEC replay output root differs")
        output_root = entity_root
        expected_paths = [str(entity_root / Path(item["submissions_path"]).relative_to(root)), str(filing_path)]
        if item["companyfacts_path"] is not None:
            expected_paths.append(str(entity_root / Path(item["companyfacts_path"]).relative_to(root)))
        expected_paths.extend(str(entity_root / "normalized/sec" / cik / name)
                              for name in ("companyfacts.parquet", "manifest.json"))
        require(paths == expected_paths, "SEC replay complete native output path list differs")
        observed = set()
        for filename, field in (("filings.parquet", "filing_rows"), ("companyfacts.parquet", "companyfact_rows")):
            original_frame = pl.read_parquet(io.BytesIO(original(Path(manifest_path).parent / filename)))
            body, _ = output(entity_root / "normalized/sec" / cik / filename)
            actual = pl.read_parquet(io.BytesIO(body))
            require(actual.columns == original_frame.columns and original_frame.height == manifest[field],
                    "SEC replay ordered column or row counts differ")
            _replay_frame_equal(actual, original_frame)
            if original_frame.height:
                observed.update(original_frame["retrieved_at_utc"].unique().to_list())
        require(observed == {item["retrieved_at_utc"]}, "SEC replay original retrieval clock differs")
        for key, hash_key in (("submissions_path", "submissions_raw_sha256"), ("companyfacts_path", "companyfacts_raw_sha256")):
            path = item[key]
            if path is None:
                require(manifest[hash_key] is None and key == "companyfacts_path", "SEC replay missing response differs")
                continue
            raw = _replay_gzip(original(path), evidence.max_file_bytes)
            body, _ = output(entity_root / Path(path).relative_to(root))
            require(hashlib.sha256(raw).hexdigest() == manifest[hash_key]
                    and raw == _replay_gzip(body, evidence.max_file_bytes), "SEC replay retained response bytes differ")
        native_manifest_bytes, manifest_sha = output(entity_root / "normalized/sec" / cik / "manifest.json")
        native_manifest = json.loads(native_manifest_bytes)
        require(isinstance(native_manifest, dict), "SEC replay native manifest shape differs")
        for document in native_manifest["primary_documents"]:
            actual_path = Path(document["raw_path"])
            original_path = root / actual_path.relative_to(entity_root)
            raw = original(original_path)
            actual, _ = output(actual_path)
            plain = _replay_gzip(raw, evidence.max_file_bytes)
            require(_replay_nonnegative_int(document.get("raw_bytes"))
                    and raw == actual and hashlib.sha256(plain).hexdigest() == document["raw_sha256"]
                    and len(plain) == document["raw_bytes"], "SEC replay cached document bytes differ")
            document["raw_path"] = str(original_path)
        require(native_manifest == manifest and check.get("cik") == cik and check.get("complete_rows") == rows
                and check.get("all_ordered_values_schema_null_masks_and_float_bits_equal") is True
                and check.get("complete_native_manifest_and_original_document_bytes_equal_after_verified_output_root_mapping") is True,
                "SEC replay complete manifest or original oracle differs")
        rows_total += rows
        documents += len(manifest["primary_documents"])
        verified.append((clock, dict(cik=cik, original_manifest_sha256=inputs[manifest_path]["sha256"],
            native_manifest_sha256=manifest_sha, cached_documents=clock["cached_documents"],
            original_retrieved_at_utc=item["retrieved_at_utc"], original_completed_at_utc=manifest["completed_at_utc"])))
    expected_manifests = {path for path in inputs if Path(path).is_relative_to(root / "normalized/sec") and Path(path).name == "manifest.json"}
    require(supplied == expected_manifests and identifiers == set(original_results) - {"sec_company_tickers"}
            and rows_total == protocol.get("entity_data_rows") == readback.get("complete_rows")
            and protocol.get("mapping_entity_definitions") == 40
            and protocol.get("original_summary_SEC_rows") == rows_total + 40 == sum(r["rows"] for r in sec_results)
            and documents == readback.get("cached_documents"), "SEC replay complete coverage differs")
    require(type(timing.get("replay_client_calls")) is int and timing["replay_client_calls"] == 1 + 2 * len(items),
            "SEC replay retained metadata request count differs")
    native_mapping_bytes, _ = output(output_root / "receipts/sec/company_tickers.json")
    native_mapping = json.loads(native_mapping_bytes)
    expected_raw = output_root / Path(mapping["raw_path"]).relative_to(root)
    require(native_mapping == dict(mapping, raw_path=str(expected_raw)), "SEC replay native mapping receipt differs")
    raw, _ = output(expected_raw)
    require(_replay_gzip(raw, evidence.max_file_bytes) == mapping_raw, "SEC replay native mapping bytes differ")
    require(all(_replay_nonnegative_int(timing.get(key)) for key in ("native_workflow_wall_ns", "native_workflow_process_cpu_ns"))
            and timing["native_workflow_wall_ns"] >= sum(row["wall_ns"] for row in clocks)
            and timing["native_workflow_process_cpu_ns"] >= sum(row["process_cpu_ns"] for row in clocks),
            "SEC replay sequential total clocks differ")
    base = dict(contract="observed_retained_sec_native_v1", scope=RETAINED_SEC_NATIVE_SCOPE,
        input_protocol_sha256=protocol_sha, native_results_sha256=results_sha, native_readback_sha256=readback_sha,
        parser_source_sha256=owner["sha256"], input_identity="retained_SEC_responses_and_cached_documents",
        new_provider_calls=0, native_result_requests_are_replayed_metadata=True, supplied_receipt_clocks=True,
        imports_capture_cache_staging_and_verification_excluded=True, native_atomic_outputs_included=True,
        source_cache="complete_original_document_cache_staged_before_native_clock",
        whole_loaded_source_identity_verified=False, acquisition_completeness_verified=False,
        production_concurrency_reproduced=False, live_readiness_verified=False, stage_totals_overlap=True)
    for index, (clock, context) in enumerate([(clocks[0], {}), *verified]):
        for field, metric in (("wall_ns", "wall_seconds"), ("process_cpu_ns", "process_cpu_seconds")):
            ledger.add(workflow="preparation:retained_sec", stage=clock["stage"], seconds=clock[field] / 1e9,
                source=timing_sha, locator=f"per_stage/{index}/{field}", origin="observed_retained_sec_native",
                outcome="verified_retained_native_output", metric=metric,
                context=dict(base, **context, dataset="sec-edgar", source_binding_eligible=True,
                    native_nanoseconds=clock[field], native_result_requests=clock["native_result_requests"]),
                work={"unit": "mapped_entity_definitions" if index == 0 else "financial_data_rows",
                      "units": 40 if index == 0 else clock["rows"]})
    for field, metric in (("native_workflow_wall_ns", "wall_seconds"), ("native_workflow_process_cpu_ns", "process_cpu_seconds")):
        ledger.add(workflow="preparation:retained_sec", stage="all_retained_mapping_and_cached_entities",
            seconds=timing[field] / 1e9, source=timing_sha, locator=field, origin="observed_retained_sec_native",
            outcome="verified_retained_native_output", metric=metric,
            context=dict(base, source_binding_eligible=False, native_nanoseconds=timing[field]),
            work={"unit": "financial_data_rows", "units": rows_total, "mapping_entity_definitions": 40})
    return dict(entities=len(items), native_rows=rows_total, mapping_entity_definitions=40,
        cached_documents=documents, frozen_originals_verified=len(inputs), protocol_sha256=protocol_sha,
        results_sha256=results_sha, readback_sha256=readback_sha, full_acquisition_verified=False)


def _collect_retained_native_operations(ledger, evidence, reports, *, size, accept, kind, contract, scope_name):
    """Explicit offline clocks require fresh independent original/output comparisons."""
    if not reports:
        return 0
    from polars.exceptions import PolarsError
    from zlib import error as ZlibError

    seen, emitted = set(), 0
    for report in reports:
        paths = [Path(path).absolute() for path in report]
        captured = [evidence.object(path) for path in paths]
        if len(captured) != size or any(item is None for item in captured):
            evidence.issues.append(dict(paths=[str(path) for path in paths], reason=scope_name + "_bundle_unavailable"))
            continue
        identity = tuple(item[1] for item in captured)
        if identity in seen:
            continue
        pending = Ledger()
        try:
            result = accept(pending, evidence, captured)
        except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, OSError, EOFError,
                PolarsError, ZlibError) as exc:
            issue = dict(paths=[str(path) for path in paths],
                         reason="invalid_" + scope_name + "_bundle", error_type=type(exc).__name__)
            if isinstance(exc, RetainedReplayContractError):
                issue["contract_violation"] = str(exc)
            evidence.issues.append(issue)
            continue
        seen.add(identity)
        ledger.samples.extend(pending.samples)
        ledger.inventory.append(dict(kind=kind, id=str(paths[0]), contract=contract,
            source_report_sha256=captured[0][1], coverage="observed_retained_native_stage_only", **result))
        emitted += len(pending.samples)
    return emitted


def collect_retained_dune_operations(ledger, evidence, reports):
    return _collect_retained_native_operations(ledger, evidence, reports, size=3,
        accept=_retained_dune_partition_samples, kind="retained_dune_partition_report",
        contract="observed_retained_dune_partition_v1", scope_name="retained_dune")


def collect_retained_sec_operations(ledger, evidence, reports):
    return _collect_retained_native_operations(ledger, evidence, reports, size=4,
        accept=_retained_sec_native_samples, kind="retained_sec_native_report",
        contract="observed_retained_sec_native_v1", scope_name="retained_sec")


"""Candidate extension inserted into the existing workflow collector, never run here."""


LOCAL_CREDENTIAL_AUDIT_FILES = (
    "operation_timings.json", "protocol.json", "baseline_status.json", "instrumented_status.json",
    "baseline_receipt.json", "instrumented_receipt.json", "readback.json", "completion.json",
)


def _local_credential_audit_samples(ledger, evidence, captured):
    require = _replay_require
    (timing, timing_sha), (protocol, protocol_sha), (baseline, baseline_sha), (status, status_sha), \
        (base_receipt, _), (receipt, _), (readback, readback_sha), (completion, _) = captured
    require(all(isinstance(row, dict) for row, _ in captured), "credential bundle objects differ")
    require(protocol.get("state") == "prepared_canonical_local_credential_audit_observation"
            and protocol.get("expected_declared_providers") == 45
            and protocol.get("all_45_original_presence_predicates_required") is True
            and protocol.get("baseline_and_instrumented_public_receipt_semantic_parity_required") is True
            and protocol.get("no_secret_files_or_secret_values_archived") is True
            and protocol.get("diagnostic_instrumentation_adds_overhead") is True
            and protocol.get("instrumented_clocks_not_speed_comparison") is True
            and protocol.get("production_service_environment_or_loaded_identity_verified") is False,
            "credential protocol scope differs")
    for row in (protocol, readback, completion):
        require(all(type(row.get(k)) is int and row[k] == 0 for k in ("new_provider_calls", "new_gpu_jobs"))
                and row.get("all_workflows_measured") is False and row.get("hardware_limit_proven") is False,
                "credential external scope differs")
        require(row.get("source_registry_stage_bindings_recomputed",
                        row.get("source_stage_bindings_recomputed")) is False,
                "credential producer claims global coverage")
    require(readback.get("state") == "verified_all_45_canonical_presence_predicates_and_receipt_parity"
            and readback.get("protocol_sha256") == protocol_sha
            and readback.get("operation_timings_sha256") == timing_sha
            and readback.get("complete_public_status_semantic_parity") is True
            and readback.get("original_presence_predicates") == 45
            and readback.get("all_clock_values_revalidated") is True
            and readback.get("authentication_or_provider_access_verified") is False
            and readback.get("full_external_acquisition_verified") is False,
            "credential independent readback differs")
    require(completion.get("state") == "verified_complete_canonical_credential_audit_diagnostic"
            and completion.get("readback_sha256") == readback_sha
            and completion.get("counts") == dict(tests=55, failures=0, errors=0, skipped=0)
            and completion.get("controlled_speedup_verified") is False
            and completion.get("canonical_sources_modified") is False,
            "credential completion differs")

    # Retain explicit public sources only. Secret files are not producer inputs.
    roles = {
        str(ROOT / "scripts/audit_data_credentials.py"): protocol.get("original_audit_source_sha256"),
        str(ROOT / "downloader/operation_timing.py"): protocol.get("original_timing_helper_sha256"),
        str(ROOT / "configs/data_api_credentials.json"): None,
    }
    sources = protocol.get("sources")
    require(isinstance(sources, list) and 3 <= len(sources) <= 32
            and all(isinstance(row, dict) for row in sources), "credential source inventory differs")
    driver_sha = protocol.get("driver_sha256")
    require(isinstance(driver_sha, str) and re.fullmatch(r"[0-9a-f]{64}", driver_sha) is not None,
            "credential observer source SHA absent")
    observers = [row for row in sources if isinstance(row.get("path"), str)
        and Path(row["path"]).is_relative_to(ROOT / "artifacts/benchmarks")
        and Path(row["path"]).suffix == ".py" and row.get("sha256") == driver_sha]
    require(len(observers) == 1, "credential observer source absent or ambiguous")
    roles[observers[0]["path"]] = driver_sha
    require(protocol.get("environment_scope") ==
            "benchmark inherited process environment plus canonical literal-presence file reader",
            "credential environment scope differs")
    archives, source_names = {}, set()
    for row in sources:
        name = row.get("path")
        require(isinstance(name, str) and name not in source_names, "credential source is duplicate")
        source_names.add(name)
        if name not in roles:
            continue
        require(isinstance(row.get("archive"), str)
                and re.fullmatch(r"[0-9a-f]{64}", row.get("sha256", "")) is not None
                and (roles[name] is None or row["sha256"] == roles[name]),
                "credential public source identity differs")
        body = evidence.read(Path(row["archive"]))
        require(body is not None and body[1] == row["sha256"], "credential public archive SHA differs")
        archives[name] = body
    require(archives.keys() == roles.keys(), "credential public sources incomplete")
    registry = json.loads(archives[str(ROOT / "configs/data_api_credentials.json")][0])
    require(isinstance(registry, dict) and registry.get("secret_values_permitted") is False,
            "credential registry permits secret values")
    declared = registry.get("providers")
    require(isinstance(declared, list) and len(declared) == 45
            and all(isinstance(row, dict) and isinstance(row.get("id"), str) and row["id"] for row in declared),
            "credential declared providers differ")
    # Registry catalog IDs and the producer's public IDs are distinct.
    # This is the data-only projection of the captured audit's _presence_row;
    # observed source bytes are never executed to interpret a receipt.
    public_ids = {}
    for item in declared:
        catalog_id = item["id"]
        require(catalog_id not in public_ids, "credential declared providers duplicate")
        field = item.get("openbb_field", "")
        require(isinstance(field, str), "credential public field differs")
        field = field.strip()
        public_id = catalog_id
        if field:
            public_id = "openbb:" + field
        elif (item.get("location") == "openbb_settings"
              and isinstance(item.get("required_names"), list)
              and len(item["required_names"]) == 1 and not item.get("any_of_names")):
            require(isinstance(item["required_names"][0], str), "credential public name differs")
            public_id = "openbb:" + item["required_names"][0].strip()
        public_ids[catalog_id] = public_id
    expected = set(public_ids.values())
    require(len(public_ids) == len(expected) == 45, "credential public identities duplicate")

    for public, public_sha, producer, mode in (
            (baseline, baseline_sha, base_receipt, "baseline"),
            (status, status_sha, receipt, "instrumented")):
        require(public.get("secret_values_included") is False
                and public.get("validation_scope") == "local_presence_only; no_authentication_or_entitlement_check"
                and isinstance(public.get("providers"), list) and len(public["providers"]) == 45
                and all(isinstance(row, dict) and isinstance(row.get("id"), str) for row in public["providers"])
                and {row["id"] for row in public["providers"]} == expected,
                "credential public status identities differ")
        require(all(isinstance(row.get("catalog_id"), str)
                    and row["catalog_id"] in public_ids
                    and row["id"] == public_ids[row["catalog_id"]] for row in public["providers"])
                and {row["catalog_id"] for row in public["providers"]} == public_ids.keys(),
                "credential public catalog mapping differs")
        require(producer.get("state") == "completed_canonical_local_presence_audit"
                and producer.get("mode") == mode and producer.get("status_sha256") == public_sha
                and producer.get("original_audit_source_sha256") == protocol["original_audit_source_sha256"]
                and producer.get("original_timing_helper_sha256") == protocol["original_timing_helper_sha256"]
                and producer.get("status_mode") == "0o600" and producer.get("original_provider_count") == 45
                and producer.get("no_secret_files_archived") is True
                and producer.get("production_service_loaded_identity_verified") is False,
                "credential producer receipt differs")
    require({k: v for k, v in baseline.items() if k != "generated_at_utc"}
            == {k: v for k, v in status.items() if k != "generated_at_utc"},
            "credential complete public status changed")
    require(timing.get("contract") == "source_operation_monotonic_v1"
            and timing.get("clock") == "perf_counter_ns" and timing.get("invalid_clock") is False
            and timing.get("aggregation") == "inclusive stages overlap total; do not sum"
            and timing.get("scope") == "after_complete_canonical_status_publication"
            and timing.get("diagnostic_instrumentation_adds_overhead") is True
            and timing.get("nested_and_inclusive_ranges_overlap") is True
            and timing.get("process_cpu_excludes_isolated_sdk_subprocess") is True
            and timing.get("authentication_or_entitlement_verified") is False
            and timing.get("external_acquisition_measured") is False,
            "credential clock scope differs")
    stages = timing.get("stages")
    allowed = {"registry_validation", "literal_presence_parse", "credential_presence_predicate",
               "finlab_presence_resolution", "finlab_local_session_probe", "legacy_settings_read",
               "legacy_presence_projection", "file_permission_check", "environment_name_inventory"}
    require(isinstance(stages, list) and 45 <= len(stages) <= 256, "credential stage inventory differs")
    predicates = []
    for row in stages:
        require(isinstance(row, dict) and row.get("stage") in allowed
                and row.get("outcome") == "returned"
                and type(row.get("seconds")) in (int, float) and finite_seconds(row["seconds"])
                and _replay_nonnegative_int(row.get("process_cpu_nanoseconds"))
                and ("provider" not in row or row["provider"] in {"credential:" + v for v in expected}),
                "credential stage clock differs")
        if row["stage"] == "credential_presence_predicate":
            predicates.append(row.get("provider"))
    require(len(predicates) == 45 and set(predicates) == {"credential:" + v for v in expected},
            "credential 45 predicate clocks incomplete or duplicate")
    require(type(timing.get("seconds")) in (int, float) and finite_seconds(timing["seconds"]),
            "credential total wall clock differs")
    base = dict(contract="observed_local_credential_audit_v1", protocol_sha256=protocol_sha,
        public_status_sha256=status_sha, readback_sha256=readback_sha, observer_source_sha256=driver_sha,
        registry_source_sha256=archives[str(ROOT / "configs/data_api_credentials.json")][1],
        audit_source_sha256=protocol["original_audit_source_sha256"],
        timing_helper_sha256=protocol["original_timing_helper_sha256"],
        environment_scope=protocol.get("environment_scope"), diagnostic_instrumentation_adds_overhead=True,
        nested_and_inclusive_ranges_overlap=True, process_cpu_excludes_isolated_sdk_subprocess=True,
        production_service_environment_or_loaded_identity_verified=False,
        authentication_or_entitlement_verified=False, external_acquisition_measured=False,
        new_provider_calls=0, new_gpu_jobs=0)
    for index, row in enumerate(stages):
        eligible = row["stage"] == "credential_presence_predicate"
        context = dict(base, source_binding_eligible=eligible,
                       dataset=row["provider"].removeprefix("credential:") if eligible else None)
        for metric, seconds, field in (("wall_seconds", row["seconds"], "seconds"),
                ("process_cpu_seconds", row["process_cpu_nanoseconds"] / 1e9, "process_cpu_nanoseconds")):
            ledger.add(workflow="operations:local_credential_audit", stage=row["stage"], seconds=seconds,
                source=timing_sha, locator=f"stages/{index}/{field}",
                origin="observed_local_credential_audit", outcome="verified_local_presence_only", metric=metric,
                context=dict(context, **({"native_nanoseconds": row[field]} if field.endswith("nanoseconds") else {})),
                work=dict(unit="local_presence_predicates" if eligible else "local_audit_calls", units=1))
    ledger.add(workflow="operations:local_credential_audit", stage="complete_instrumented_local_audit",
        seconds=timing["seconds"], source=timing_sha, locator="seconds",
        origin="observed_local_credential_audit", outcome="verified_local_presence_only",
        context=dict(base, source_binding_eligible=False), work=dict(unit="declared_local_credential_gates", units=45))
    return dict(declared_providers=45, original_presence_predicates=45, stage_observations=len(stages),
                public_source_archives_verified=4, full_acquisition_verified=False)


def collect_local_credential_audits(ledger, evidence, directories):
    reports = [[Path(directory) / name for name in LOCAL_CREDENTIAL_AUDIT_FILES] for directory in directories]
    return _collect_retained_native_operations(ledger, evidence, reports, size=8,
        accept=_local_credential_audit_samples, kind="local_credential_audit_report",
        contract="observed_local_credential_audit_v1", scope_name="local_credential_audit")


def bind_source_stages(ledger: Ledger) -> int:
    """Bind exact registered IDs; shared cycle wall is not per-source."""
    sources = {row["id"]: row for row in ledger.inventory if row["kind"] == "source_endpoint"}
    bound = defaultdict(list)
    for sample in ledger.samples:
        prefix = {"recorded_finmind_telemetry": "finmind:", "recorded_finlab_receipt": "finlab:",
                  "recorded_tej_task": "tej:", "recorded_economic_receipt": "economic:",
                  "observed_economic_receipt_validation": "economic:",
                  "recorded_openbb_task": "openbb:",
                  "observed_crypto_issuer_parser": "crypto-etf:issuer:",
                  "observed_retained_dune_partition": "dune-query:",
                  "observed_retained_sec_native": "crypto-etf:",
                  "observed_local_credential_audit": "credential:",
                  "recorded_tw_public_operation": "tw-public:"}.get(sample["origin"])
        if (prefix is None or sample["context"].get("input_identity") == "producer_cycle"
                or sample["context"].get("source_binding_eligible") is False
                or sample["context"].get("timing_basis") == "proportional_cycle_allocation"):
            continue
        if prefix == "tej:" and (sample["context"].get("timing_basis") != "fresh_end_to_end"
                                 or not isinstance(sample["context"].get("task_id"), str)
                                 or not sample["context"]["task_id"]):
            continue
        dataset = sample["context"].get("table_id" if prefix == "tej:" else "dataset")
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


def collect_known_source_inventory(ledger: Ledger, evidence: Evidence, path: Path) -> int:
    """Retain declared historical identities; never import their old timings."""
    captured = evidence.object(path)
    if captured is None:
        raise ValueError("requested known-source inventory is unavailable")
    payload, sha = captured
    if (not isinstance(payload, dict) or payload.get("contract") != CONTRACT
            or not isinstance(payload.get("rows"), list)):
        raise ValueError("unknown known-source inventory contract")
    known = {}
    for row in payload["rows"]:
        if not isinstance(row, dict) or row.get("kind") != "source_endpoint":
            continue
        identifier = row.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in known:
            raise ValueError("invalid or duplicate known-source identity")
        known[identifier] = row
    if not known:
        raise ValueError("known-source inventory has no source identities")
    inventory_path = str(path.absolute())
    existing = next((row for row in ledger.inventory
                     if row["kind"] == "known_source_inventory" and row["id"] == inventory_path), None)
    if existing is not None:
        if existing.get("source_sha256") != sha:
            raise ValueError("known-source inventory identity changed within collection")
        return 0
    present = {row["id"] for row in ledger.inventory if row["kind"] == "source_endpoint"}
    missing = sorted(known.keys() - present)
    for identifier in missing:
        row = known[identifier]
        ledger.inventory.append({
            "kind": "source_endpoint", "id": identifier, "category": "acquisition",
            "provider": row.get("provider"), "active_scope": None, "service_keys": [],
            "snapshot_at_utc": row.get("snapshot_at_utc"),
            "historical_active_scope": row.get("historical_active_scope", row.get("active_scope")),
            "historical_service_keys": row.get("historical_service_keys", row.get("service_keys")),
            "registry_presence": "historical_identity_missing_from_current_snapshot",
            "current_visibility_verified": False, "live_readiness_verified": False,
            "known_inventory_path": inventory_path, "known_inventory_sha256": sha,
            "coverage": "historical_source_identity_operation_timing_not_bound",
            "next_measurement": "retain known identity; verify current visibility and measure remaining source workflow; historical service state is not current health",
        })
    ledger.inventory.append({
        "kind": "known_source_inventory", "id": inventory_path, "category": "operations",
        "contract": "known_source_identity_scope_v1", "source_sha256": sha,
        "declared_identities": len(known), "already_present_identities": len(known) - len(missing),
        "historical_identities_added": len(missing), "old_timings_imported": 0,
        "current_snapshot_retained": True,
    })
    return len(missing)


def collect_source_operation(ledger, row, sha, *, locator, owner, origin=None):
    """Consume only the named direct-clock ABI, with exact source identity."""
    from downloader.operation_timing import CONTRACT
    if not isinstance(row, dict):
        return
    timing = row.get("operation_timing") or {}
    scheduler = row.get("scheduler_timing") or {}
    scheduler_valid = (isinstance(scheduler, dict) and scheduler.get("contract") == "openbb_scheduler_monotonic_v1"
                       and scheduler.get("clock") == "monotonic")
    scopes = {"acquire_return_including_receipt_publication", "payloads_verified_before_receipt_publication",
              "failure_before_attempt_publication", "worker_return_before_discovery_and_manifest_commit",
              "verified_receipt_readonly"}
    if owner == "tw-public":
        scopes = {"native_dataset_dispatch_excludes_executor_queue_timing_log_parent_reports_and_later_audits"}
        from downloader.release_archive_timing import SCOPE as release_scope
        scopes.add(release_scope)
    valid = (isinstance(timing, dict) and timing.get("contract") == CONTRACT
             and timing.get("clock") == "perf_counter_ns" and timing.get("invalid_clock") is False
             and timing.get("scope") in scopes and finite_seconds(timing.get("seconds")))
    if not valid and not scheduler_valid:
        return
    if not valid:
        timing = {}
    provider, dataset = row.get("provider"), row.get("dataset")
    if not isinstance(provider, str) or not provider or (owner == "economic" and not isinstance(dataset, str)):
        return
    origin = origin or ("recorded_economic_receipt" if owner == "economic" else "recorded_openbb_task")
    stages = timing.get("stages", [])
    if not isinstance(stages, list):
        return
    context = {"dataset": f"{provider}:{dataset}" if owner == "economic" else provider,
               "provider": provider, "endpoint": row.get("endpoint"), "task_id": row.get("task_id"),
               "query_fingerprint": row.get("query_fingerprint"), "observed_at_utc": row.get("observed_at_utc"),
               "clock_basis": "producer_monotonic", "timing_scope": timing.get("scope", "scheduler_only"),
               "input_identity": "source_receipt" if owner == "economic" else "manifest_task",
               "source_binding_eligible": (row.get("source_binding_eligible", True) if owner == "economic"
                                           else row.get("declared_providers") == [provider]),
               "transport_scope": "adapter/fetch may include limiter, retries, local cache and parsing; not pure HTTP latency"}
    if owner == "tw-public":
        context.update(row.get("native_context", {}))
        release_collect = timing.get("scope") == release_scope
        context.update(dataset=dataset, input_identity=("native_release_archive_collect" if release_collect
                                                       else "native_dataset_dispatch"),
                       source_binding_eligible=row.get("source_binding_eligible") is True)
        workflow = f"tw-public:{dataset}"
    else:
        workflow = f"{owner}:{provider}:{dataset or row.get('endpoint')}"
    if valid:
        stage = "producer_collect_total" if owner == "tw-public" and release_collect else "producer_call_total"
        ledger.add(workflow=workflow, stage=stage, seconds=timing["seconds"], source=sha,
                   locator=locator + "/operation_timing/seconds", origin=origin, outcome=str(row.get("status")), context=context)
    for index, stage in enumerate(stages):
        if (not isinstance(stage, dict) or not isinstance(stage.get("stage"), str)
                or not finite_seconds(stage.get("seconds")) or stage["seconds"] > timing["seconds"]):
            continue
        stage_context = dict(context)
        if owner == "openbb":
            stage_provider = stage.get("provider")
            if not isinstance(stage_provider, str) or stage_provider not in row.get("declared_providers", []):
                continue
            stage_context.update(dataset=stage_provider, provider=stage_provider, source_binding_eligible=True)
        stage_context["stage_outcome"] = stage.get("outcome")
        ledger.add(workflow=workflow, stage=stage["stage"], seconds=stage["seconds"], source=sha,
                   locator=f"{locator}/operation_timing/stages/{index}", origin=origin,
                   outcome=str(row.get("status")), context=stage_context)
    if scheduler_valid:
        for field in ("executor_queue_seconds", "worker_and_discovery_seconds", "discovery_seconds", "submit_to_reap_seconds"):
            ledger.add(workflow=workflow, stage=field, seconds=scheduler.get(field), source=sha,
                       locator=locator + "/scheduler_timing/" + field, origin=origin,
                       outcome=str(row.get("status")), context={**context, "clock_basis": "producer_monotonic_scheduler"})


def _tw_public_journal_operation(entry, registry):
    from dataclasses import asdict

    if not isinstance(entry, dict):
        raise ValueError("invalid_journal_entry")
    message = entry.get("MESSAGE", "")
    if isinstance(message, list):
        if not all(type(value) is int and 0 <= value <= 255 for value in message):
            raise ValueError("invalid_journal_message_bytes")
        message = bytes(message).decode("utf-8")
    if not isinstance(message, str):
        raise ValueError("invalid_journal_message")
    prefix = "[tw-public:operation-timing] "
    if prefix not in message:
        return None
    offset = message.index(prefix) + len(prefix)
    event, consumed = json.JSONDecoder().raw_decode(message[offset:])
    if message[offset + consumed:].strip() or not isinstance(event, dict):
        raise ValueError("invalid_native_timing_json")
    if event.get("contract") != "tw_public_dataset_operation_timing_v1":
        raise ValueError("unknown_native_timing_contract")
    timing = event.get("operation_timing")
    scope = "native_dataset_dispatch_excludes_executor_queue_timing_log_parent_reports_and_later_audits"
    if (not isinstance(timing, dict) or timing.get("contract") != "source_operation_monotonic_v1"
            or timing.get("clock") != "perf_counter_ns" or timing.get("invalid_clock") is not False
            or timing.get("scope") != scope or not finite_seconds(timing.get("seconds"))
            or timing.get("stages") != [] or type(timing.get("started_monotonic_ns")) is not int
            or timing["started_monotonic_ns"] < 0):
        raise ValueError("invalid_native_operation_clock")
    dataset = event.get("dataset")
    if not isinstance(dataset, str) or not dataset:
        raise ValueError("invalid_native_dataset")
    for field in ("dataset_spec_sha256", "request_policy_sha256"):
        if not isinstance(event.get(field), str) or re.fullmatch(r"[0-9a-f]{64}", event[field]) is None:
            raise ValueError("invalid_native_configuration_identity")
    for field in ("producer_pid", "producer_thread_id"):
        if type(event.get(field)) is not int or event[field] <= 0:
            raise ValueError("invalid_native_producer_identity")
    if str(event["producer_pid"]) != entry.get("_PID"):
        raise ValueError("journal_producer_pid_differs")
    unit = entry.get("_SYSTEMD_UNIT")
    if not isinstance(unit, str) or re.fullmatch(r"stockagent-[A-Za-z0-9_.@:-]+\.service", unit) is None:
        raise ValueError("unknown_native_journal_owner")
    for field in ("_BOOT_ID", "_SYSTEMD_INVOCATION_ID"):
        if not isinstance(entry.get(field), str) or re.fullmatch(r"[0-9a-f]{32}", entry[field]) is None:
            raise ValueError("invalid_journal_invocation_identity")
    if "work_units_denominator" not in event or event["work_units_denominator"] is not None:
        raise ValueError("native_work_denominator_contract_differs")
    for field in ("clock_is_source_publication_time", "configuration_is_resolved_request_range",
                  "acquisition_completeness_verified", "live_readiness_verified"):
        if event.get(field) is not False:
            raise ValueError("native_observation_boundary_differs")
    spec = registry.get(dataset)
    known = spec is not None and digest(asdict(spec)) == event["dataset_spec_sha256"]
    eligible = (known and event.get("dataset_kind") == spec.kind
                and spec.kind in {"historical_json_table", "delisted_history", "snapshot_url",
                                  "data_gov", "gcis_catalog", "fsc_catalog"}
                and event.get("source_binding_eligible") is True
                and event.get("source_endpoint_id") == "tw-public:" + dataset
                and event.get("returned_dataset") == dataset
                and event.get("result_dataset_matches_request") is True
                and event.get("outcome") == "returned"
                and isinstance(event.get("status"), str) and event["status"] != "unsupported")
    identity = (entry["_BOOT_ID"], entry["_SYSTEMD_INVOCATION_ID"], event["producer_pid"],
                event["producer_thread_id"], timing["started_monotonic_ns"], dataset)
    context = {"producer_pid": event["producer_pid"], "producer_thread_id": event["producer_thread_id"],
        "producer_started_monotonic_ns": timing["started_monotonic_ns"],
        "dataset_spec_sha256": event["dataset_spec_sha256"],
        "request_policy_sha256": event["request_policy_sha256"],
        "journal_unit": unit, "journal_boot_id": entry["_BOOT_ID"],
        "journal_invocation_id": entry["_SYSTEMD_INVOCATION_ID"],
        "native_outcome": event.get("outcome"), "native_event_sha256": digest(event),
        "configuration_is_resolved_request_range": False,
        "whole_loaded_source_identity_verified": False,
        "acquisition_completeness_verified": False, "live_readiness_verified": False,
        "work_units_denominator": None}
    return {"identity": identity, "event_sha256": context["native_event_sha256"],
        "row": {"provider": "tw_public", "dataset": dataset, "status": event.get("status"),
                "observed_at_utc": event.get("observed_at_utc"), "operation_timing": timing,
                "source_binding_eligible": eligible, "native_context": context}}


def _tw_public_release_journal_operation(entry):
    """Decode the distinct collect clock; the shared journal owner handles evidence and deduplication."""
    from downloader.release_archive_timing import CONTRACT as release_contract, PREFIX, SCOPE, SOURCES

    if not isinstance(entry, dict):
        raise ValueError("invalid_journal_entry")
    message = entry.get("MESSAGE", "")
    if isinstance(message, list):
        if not all(type(value) is int and 0 <= value <= 255 for value in message):
            raise ValueError("invalid_journal_message_bytes")
        message = bytes(message).decode("utf-8")
    if not isinstance(message, str):
        raise ValueError("invalid_journal_message")
    if PREFIX not in message:
        return None
    offset = message.index(PREFIX) + len(PREFIX)
    event, consumed = json.JSONDecoder().raw_decode(message[offset:])
    if message[offset + consumed:].strip() or not isinstance(event, dict):
        raise ValueError("invalid_release_timing_json")
    if event.get("contract") != release_contract:
        raise ValueError("unknown_release_timing_contract")
    timing = event.get("operation_timing")
    if (not isinstance(timing, dict) or timing.get("contract") != "source_operation_monotonic_v1"
            or timing.get("clock") != "perf_counter_ns" or timing.get("invalid_clock") is not False
            or timing.get("scope") != SCOPE or not finite_seconds(timing.get("seconds"))
            or timing.get("stages") != [] or type(timing.get("started_monotonic_ns")) is not int
            or timing["started_monotonic_ns"] < 0):
        raise ValueError("invalid_release_operation_clock")
    dataset = event.get("dataset")
    if not isinstance(dataset, str) or not dataset:
        raise ValueError("invalid_release_dataset")
    if (not isinstance(event.get("definition_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", event["definition_sha256"]) is None):
        raise ValueError("invalid_release_definition_identity")
    for field in ("producer_pid", "producer_thread_id"):
        if type(event.get(field)) is not int or event[field] <= 0:
            raise ValueError("invalid_release_producer_identity")
    if str(event["producer_pid"]) != entry.get("_PID"):
        raise ValueError("journal_producer_pid_differs")
    unit = entry.get("_SYSTEMD_UNIT")
    if not isinstance(unit, str) or re.fullmatch(r"stockagent-[A-Za-z0-9_.@:-]+\.service", unit) is None:
        raise ValueError("unknown_release_journal_owner")
    for field in ("_BOOT_ID", "_SYSTEMD_INVOCATION_ID"):
        if not isinstance(entry.get(field), str) or re.fullmatch(r"[0-9a-f]{32}", entry[field]) is None:
            raise ValueError("invalid_journal_invocation_identity")
    if "work_units_denominator" not in event or event["work_units_denominator"] is not None:
        raise ValueError("release_work_denominator_contract_differs")
    for field in ("clock_is_source_publication_time", "configuration_is_resolved_request_range",
                  "whole_loaded_source_identity_verified", "acquisition_completeness_verified",
                  "live_readiness_verified"):
        if event.get(field) is not False:
            raise ValueError("release_observation_boundary_differs")
    definition = {"dataset": dataset, "producer_file": SOURCES.get(dataset),
                  "producer_function": "collect", "timing_scope": SCOPE}
    definition_sha = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()
    eligible = (dataset in SOURCES and event["definition_sha256"] == definition_sha
                and event.get("producer_file") == SOURCES[dataset]
                and event.get("producer_function") == "collect"
                and event.get("source_binding_eligible") is True
                and event.get("source_endpoint_id") == "tw-public:" + dataset
                and event.get("returned_dataset") == dataset
                and event.get("result_dataset_matches_request") is True
                and event.get("outcome") == "returned"
                and isinstance(event.get("status"), str) and event["status"] != "unsupported")
    identity = (release_contract, entry["_BOOT_ID"], entry["_SYSTEMD_INVOCATION_ID"],
                event["producer_pid"], event["producer_thread_id"], timing["started_monotonic_ns"], dataset)
    context = {"producer_pid": event["producer_pid"], "producer_thread_id": event["producer_thread_id"],
        "producer_started_monotonic_ns": timing["started_monotonic_ns"],
        "native_timing_contract": release_contract, "definition_sha256": event["definition_sha256"],
        "producer_file": event.get("producer_file"), "producer_function": event.get("producer_function"),
        "journal_unit": unit, "journal_boot_id": entry["_BOOT_ID"],
        "journal_invocation_id": entry["_SYSTEMD_INVOCATION_ID"],
        "native_outcome": event.get("outcome"), "native_event_sha256": digest(event),
        "configuration_is_resolved_request_range": False,
        "whole_loaded_source_identity_verified": False,
        "acquisition_completeness_verified": False, "live_readiness_verified": False,
        "work_units_denominator": None,
        "transport_scope": "collect includes final state write; excludes CLI admission, outer locks and logging; not pure HTTP latency"}
    return {"identity": identity, "event_sha256": context["native_event_sha256"],
        "row": {"provider": "tw_public", "dataset": dataset, "status": event.get("status"),
                "observed_at_utc": event.get("observed_at_utc"), "operation_timing": timing,
                "source_binding_eligible": eligible, "native_context": context}}


def collect_tw_public_operations(ledger, evidence, paths, *, registry=None):
    """Freeze supplied journals; bind exact native dispatch and release collect clocks.

    Overlapping journal exports do not multiply a call. Conflicting observations
    retain their distinct durations but cannot establish source-stage coverage.
    No journal/provider process is started here.
    """
    if not paths:
        return 0
    if registry is None:
        from downloader.download_tw_public_data import EXTENDED_DATASETS
        registry = EXTENDED_DATASETS
    groups, observations, statistics_by_path = defaultdict(dict), [], {}
    for path in paths:
        path = Path(path)
        key = str(path.absolute())
        if key in statistics_by_path:
            continue
        captured = evidence.read(path)
        if captured is None:
            continue
        raw, sha = captured
        counts = {"kind": "source_clock_journal", "id": key, "source_sha256": sha,
            "contract": "tw_public_operation_journal_ingestion_v1",
            "native_events": 0, "unique_events_emitted": 0, "duplicate_events": 0,
            "conflicting_events": 0, "invalid_entries": 0,
            "scope": "frozen journal native dispatch observations; not acquisition E2E or verified loaded code"}
        statistics_by_path[key] = counts
        for index, line in enumerate(raw.splitlines()):
            try:
                entry = json.loads(line)
                value = _tw_public_journal_operation(entry, registry)
                if value is None:
                    value = _tw_public_release_journal_operation(entry)
            except (ValueError, TypeError, KeyError, UnicodeError):
                counts["invalid_entries"] += 1
                evidence.issues.append({"path": key, "line": index + 1,
                                        "reason": "invalid_tw_public_journal_operation"})
                continue
            if value is None:
                continue
            if value["row"]["native_context"].get("native_timing_contract") == "tw_public_release_archive_operation_timing_v1":
                counts["contract"] = "tw_public_operation_journal_ingestion_v2"
                counts["scope"] = "frozen journal native dispatch and release collect observations; not acquisition E2E or verified loaded code"
            counts["native_events"] += 1
            family = groups[value["identity"]]
            if value["event_sha256"] in family:
                counts["duplicate_events"] += 1
                continue
            value.update(source_sha256=sha, locator=f"journal/lines/{index + 1}", counts=counts)
            family[value["event_sha256"]] = value
            observations.append(value)
    for value in observations:
        conflict = len(groups[value["identity"]]) > 1
        row = value["row"]
        row["native_context"]["native_operation_conflict"] = conflict
        if conflict:
            row["source_binding_eligible"] = False
            value["counts"]["conflicting_events"] += 1
            evidence.issues.append({"path": value["counts"]["id"], "locator": value["locator"],
                                    "reason": "conflicting_tw_public_native_operation"})
        collect_source_operation(ledger, row, value["source_sha256"], locator=value["locator"],
                                 owner="tw-public", origin="recorded_tw_public_operation")
        value["counts"]["unique_events_emitted"] += 1
    ledger.inventory.extend(statistics_by_path.values())
    return len(observations)


def collect_economic_operations(ledger, evidence, root):
    for kind, pattern in (("status", "*.json"), ("receipts", "*/*.json"), ("attempts", "*/*.json")):
        for path in sorted((root / kind).glob(pattern)):
            captured = evidence.object(path)
            if not captured or not isinstance(captured[0], dict):
                continue
            payload, sha = captured
            rows = payload.get("datasets", []) if kind == "status" else [payload]
            for index, row in enumerate(rows):
                if isinstance(row, dict):
                    collect_source_operation(ledger, row, sha, locator=f"{kind}/{index}", owner="economic")
    summary = root / "download_summary.json"
    if summary.is_file():
        captured = evidence.object(summary)
        if captured and isinstance(captured[0], dict):
            for index, row in enumerate(captured[0].get("datasets", [])):
                collect_source_operation(ledger, row, captured[1], locator=f"download_summary/datasets/{index}", owner="economic")


def collect_openbb_operations(ledger, evidence, root):
    """Bounded query-only snapshot of native committed latest task clocks."""
    from downloader.operation_timing import OPENBB_META_PREFIX
    state_path = root / "_state/provider_scheduler.json"
    if state_path.is_file():
        captured = evidence.object(state_path)
        if captured and isinstance(captured[0], dict):
            batch = captured[0].get("last_completion_timing") or {}
            from downloader.operation_timing import CONTRACT
            if (isinstance(batch, dict) and batch.get("contract") == CONTRACT and batch.get("invalid_clock") is False
                    and batch.get("scope") == "manifest_completion_batch_including_commit_and_terminal_quarantine"):
                ledger.add(workflow="openbb:completion_batch", stage="manifest_commit_and_quarantine",
                           seconds=batch.get("seconds"), source=captured[1], locator="last_completion_timing/seconds",
                           origin="recorded_openbb_completion_batch", context={"input_identity": "producer_batch",
                           "task_ids": batch.get("task_ids"), "allocation": "whole batch; never per source"},
                           work={"units": batch.get("task_count"), "unit": "completed_tasks"})
    path = root / "_state/openbb_archive.sqlite3"
    if not path.is_file():
        return
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=.5)
    try:
        connection.execute("PRAGMA query_only=ON")
        deadline = time.monotonic() + 3
        connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        connection.execute("BEGIN")
        query = "SELECT key,value,updated_at FROM archive_meta WHERE key>=? AND key<? ORDER BY key LIMIT 4097"
        rows = [dict(zip(("key", "value", "updated_at"), row)) for row in connection.execute(
            query, (OPENBB_META_PREFIX, "operation_timing_v1;"))]
    except sqlite3.Error as exc:
        evidence.issues.append({"path": str(path), "reason": type(exc).__name__})
        return
    finally:
        connection.rollback()
        connection.close()
    snapshot = evidence.output / "openbb_operation_observation.json"
    atomic_write_json(snapshot, {"database": str(path), "query": query, "rows": rows[:4096],
                                "selection_limit": 4096, "selection_truncated": len(rows) > 4096,
                                "scope": "latest native committed sample per provider/endpoint/outcome; not complete task history"})
    captured = evidence.object(snapshot)
    if captured:
        for index, row in enumerate(rows[:4096]):
            try:
                payload = json.loads(row["value"])
            except (TypeError, ValueError):
                evidence.issues.append({"path": str(path), "reason": "invalid_operation_timing_json", "key": row["key"]})
                continue
            if isinstance(payload, dict):
                collect_source_operation(ledger, payload, captured[1], locator=f"rows/{index}", owner="openbb")


def collect_benchmark(ledger: Ledger, payload, sha: str, path: Path) -> None:
    """Explicitly selected producer samples; full receipt retains acceptance."""
    cpu_fields = ("cpu_seconds", "complete_cpu_seconds", "descendant_cpu_seconds",
                  "process_cpu_seconds", "child_cpu_user_seconds", "child_cpu_system_seconds",
                  "reaped_child_cpu_seconds", "parent_cpu_seconds", "self_cpu_seconds",
                  "user_cpu_seconds", "system_cpu_seconds")
    if not isinstance(payload, dict):
        return
    if payload.get("contract") == "economic_receipt_validation_v1":
        for index, row in enumerate(payload.get("sources", [])):
            collect_source_operation(ledger, row, sha, locator=f"sources/{index}", owner="economic",
                                     origin="observed_economic_receipt_validation")
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
                          *cpu_fields):
                value = row.get(field)
                if not finite_seconds(value):
                    continue
                ledger.add(workflow=workflow, stage=field, seconds=value / 1000 if field.endswith("_ms") else value,
                           source=sha, locator=f"/{key}/{index}/{field}", origin="selected_benchmark_receipt",
                           outcome="producer_reported_sample_acceptance_in_receipt", context=context,
                           metric="cpu_seconds" if field in cpu_fields else "wall_seconds",
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
                                   origin="selected_benchmark_receipt", context=context,
                                   metric="cpu_seconds" if field in cpu_fields else "wall_seconds")
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
    validate_manifest_storage(manifest)
    with PinnedSnapshotStreams(output, local, storage=manifest.get("snapshot_storage")) as snapshot_streams:
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
                with snapshot_streams.open(row["snapshot"]) as handle:
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
            build = snapshot_json(snapshot_streams, build_source["snapshot"])
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
        known_inventories, known_links = {}, 0
        source_ids = {row["id"] for row in inventory if row["kind"] == "source_endpoint"}
        for scope in (row for row in inventory if row["kind"] == "known_source_inventory"):
            source = sources_by_path.get(scope["id"], {})
            require(source.get("state") == "captured" and source.get("sha256") == scope.get("source_sha256"),
                    "known-source inventory has no frozen identity")
            original = snapshot_json(snapshot_streams, source["snapshot"])
            require(original.get("contract") == CONTRACT and isinstance(original.get("rows"), list),
                    "known-source inventory contract differs")
            rows = [r for r in original["rows"] if isinstance(r, dict) and r.get("kind") == "source_endpoint"]
            known_inventories[scope["id"]] = {r["id"]: r for r in rows}
            declared = known_inventories[scope["id"]]
            added = sum(r.get("known_inventory_path") == scope["id"] for r in inventory)
            require(bool(declared) and len(rows) == len(declared) == scope.get("declared_identities")
                    and declared.keys() <= source_ids and added == scope.get("historical_identities_added")
                    and scope.get("already_present_identities") == len(declared) - added
                    and scope.get("old_timings_imported") == 0 and scope.get("current_snapshot_retained") is True,
                    "known-source registry scope differs")
        for row in inventory:
            if row.get("registry_presence") == "historical_identity_missing_from_current_snapshot":
                require(bool(row.get("known_inventory_path")), "historical identity has no inventory provenance")
            if not row.get("known_inventory_path"):
                continue
            path = row["known_inventory_path"]
            source = sources_by_path.get(path, {})
            require(source.get("state") == "captured" and source.get("sha256") == row.get("known_inventory_sha256"),
                    "known-source inventory has no frozen identity")
            require(path in known_inventories, "known-source inventory has no declared scope")
            original = known_inventories[path].get(row["id"], {})
            require(original.get("id") == row["id"] and original.get("provider") == row.get("provider")
                    and original.get("snapshot_at_utc") == row.get("snapshot_at_utc")
                    and original.get("historical_active_scope", original.get("active_scope")) == row.get("historical_active_scope")
                    and original.get("historical_service_keys", original.get("service_keys")) == row.get("historical_service_keys")
                    and row.get("active_scope") is None and row.get("service_keys") == []
                    and row.get("registry_presence") == "historical_identity_missing_from_current_snapshot"
                    and row.get("current_visibility_verified") is False and row.get("live_readiness_verified") is False,
                    "known-source identity or visibility boundary differs")
            known_links += 1
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
                "known_source_identity_links_verified": known_links,
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
    evidence = Evidence(output, max_file_bytes=args.max_file_mib << 20, max_total_bytes=args.max_total_mib << 20,
                        snapshot_storage=getattr(args, "evidence_storage_layout", "individual_gzip"))
    try:
        ledger = Ledger()
        metric_backend = configure_metric_backend(getattr(args, "metric_backend", "auto"))
        code_inventory_resources = {}
        entries, code = visible_code(ROOT, workers=getattr(args, "code_inventory_workers", 0),
                                     observations=code_inventory_resources)
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
                     "stockagent/workflow_snapshot_streams.py",
                 "stockagent/workflow_snapshot_pack.py",
                 "downloader/artifact_io.py",
                 "scripts/audit_data_credentials.py",
                 "stockagent/data_sync/packed_snapshots.py",
                 "stockagent/data_sync/packed_metadata.py",
                 "stockagent/data_sync/desync_snapshots.py",
                     "stockagent/live/market_status.py", "stockagent/live/signal_engine.py",
                     "stockagent/live/tw_day_trade_dashboard.py", "stockagent/live/completed_inference_cache.py",
                     "stockagent/live/service_code_release.py", "downloader/operation_timing.py",
                     "downloader/feature_stage_timing.py", "downloader/binance_historical_features.py",
                     "downloader/okx_historical_features.py",
                     "downloader/download_public_economic_history.py", "downloader/public_economic_sources.py",
                     "downloader/frankfurter_official_history.py", "downloader/download_openbb_archive.py",
                     "downloader/openbb_archive_types.py", "downloader/download_tw_public_data.py",
                     "downloader/release_archive_timing.py", "downloader/download_tw_cbc_overnight_pages.py",
                     "downloader/download_tw_cbc_fx_annual_pages.py", "downloader/download_tw_mof_macro_release_dates.py",
                     "downloader/download_tw_mof_trade_release_values.py", "downloader/download_tw_mof_release_archive.py",
                     "downloader/download_tw_dgbas_release_archive.py",
                     "scripts/benchmark_public_economic_receipts.py"):
            captured = evidence.read(ROOT / name)
            if captured:
                measurement_sources.append({"path": name, "sha256": captured[1]})
        if getattr(args, "crypto_issuer_parser_report", []) or getattr(args, "retained_sec_report", []):
            name = "downloader/download_crypto_etf_history.py"
            captured = evidence.read(ROOT / name)
            if captured:
                measurement_sources.append({"path": name, "sha256": captured[1]})
        if getattr(args, "retained_dune_report", []):
            name = "downloader/download_dune_crypto_history.py"
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
                                         "registry_presence": "current_snapshot", "current_visibility_verified": True,
                                         "live_readiness_verified": False,
                                         "coverage": "source_specific_operation_timing_not_bound",
                                         "next_measurement": "match source/task identity, completed units and elapsed time; shared service time is not per-source time"})
        for path in getattr(args, "known_source_inventory", []):
            collect_known_source_inventory(ledger, evidence, path)
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
        collect_economic_operations(ledger, evidence, ROOT / "data_public_economic")
        collect_economic_operations(ledger, evidence, ROOT / "data_forex_frankfurter/official_v2")
        collect_openbb_operations(ledger, evidence, ROOT / "data_openBB")
        collect_tw_public_operations(ledger, evidence, getattr(args, "tw_public_operation_journal", []))
        collect_crypto_feature_operations(ledger, evidence, getattr(args, "crypto_feature_report", []) or [])
        collect_crypto_issuer_parser_operations(ledger, evidence, getattr(args, "crypto_issuer_parser_report", []))
        collect_retained_dune_operations(ledger, evidence, getattr(args, "retained_dune_report", []))
        collect_retained_sec_operations(ledger, evidence, getattr(args, "retained_sec_report", []))
        collect_local_credential_audits(ledger, evidence, getattr(args, "local_credential_audit_report", []))
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
            "code_inventory_resources": code_inventory_resources,
            "issues": len(evidence.issues), "traversal_gaps": len(traversal_gaps),
            "source_endpoints_individually_timed": 0,
            "source_endpoints_with_stage_timing": source_stage_count,
            "input_paths": {**{key: [str(path) for path in getattr(args, key)] for key in
                                  ("architecture", "services", "http", "tej", "storage", "benchmark_receipt", "training_root")},
                            "tw_public_operation_journal": [str(path) for path in getattr(args, "tw_public_operation_journal", [])],
                            "monitor_snapshot": str(args.monitor_snapshot)},
            "tw_public_operation_ingestion_contract": "tw_public_operation_journal_ingestion_v1",
            "tw_public_release_operation_ingestion_contract": "tw_public_operation_journal_ingestion_v2",
            "benchmark_receipt_clock_ingestion_contract": "producer_benchmark_cpu_scopes_v2",
            "engineering_command_cpu_ingestion_contract": "waited_command_reaped_child_cpu_v1",
            "limits": {"training_depth": args.training_depth, "benchmark_depth": args.benchmark_depth,
                       "benchmark_directory_budget": args.benchmark_directory_budget,
                       "excluded_directory_names": sorted(SKIP_DIRS), "max_file_mib": args.max_file_mib,
                       "max_total_mib": args.max_total_mib, "registered_runs": args.registered_runs,
                       "agent_runs": args.agent_runs,
                       "evidence_read_workers": getattr(args, "evidence_read_workers", 0),
                       "evidence_read_batch_mib": getattr(args, "evidence_read_batch_mib", 2),
                       "code_inventory_workers": getattr(args, "code_inventory_workers", 0),
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
        if getattr(args, "crypto_issuer_parser_report", []):
            report["crypto_issuer_parser_ingestion_contract"] = "observed_retained_crypto_issuer_parser_v1"
            report["input_paths"]["crypto_issuer_parser_report"] = [
                [str(path) for path in bundle] for bundle in args.crypto_issuer_parser_report]
        if getattr(args, "retained_dune_report", []):
            report["retained_dune_ingestion_contract"] = "observed_retained_dune_partition_v1"
            report["input_paths"]["retained_dune_report"] = [
                [str(path) for path in bundle] for bundle in args.retained_dune_report]
        if getattr(args, "retained_sec_report", []):
            report["retained_sec_ingestion_contract"] = "observed_retained_sec_native_v1"
            report["input_paths"]["retained_sec_report"] = [
                [str(path) for path in bundle] for bundle in args.retained_sec_report]
        # Bind every final output and every retained original snapshot independently.
        report["outputs"] = {str(path.relative_to(output)): {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
                             for path in sorted(output.iterdir()) if path.is_file()}
        if getattr(args, "local_credential_audit_report", []):
            report["local_credential_audit_ingestion_contract"] = "observed_local_credential_audit_v1"
            report["input_paths"]["local_credential_audit_report"] = [str(path) for path in args.local_credential_audit_report]
        finish_phase("final_output_hashes")
        report["pre_manifest_wall_seconds"] = phase_started - started
        atomic_write_json(output / "collection.json", report)
        return report

    finally:
        evidence.close_snapshot_store()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="New immutable observation directory")
    parser.add_argument("--architecture", type=Path, action="append", default=[])
    parser.add_argument("--monitor-snapshot", type=Path, default=ROOT / "artifacts/live/data_monitor/public_status.json")
    parser.add_argument("--known-source-inventory", type=Path, action="append", default=[],
                        help="Retain known identities from a frozen collector inventory when absent from the current snapshot; import no old timings or current-health claims; current entries win")
    parser.add_argument("--services", type=Path, action="append", default=[])
    parser.add_argument("--http", type=Path, action="append", default=[])
    parser.add_argument("--tej", type=Path, action="append", default=[])
    parser.add_argument("--storage", type=Path, action="append", default=[])
    parser.add_argument("--storage-history-only", action="store_true", help="Use run_lakehouse_control.sh python in the locked role; read pinned histories only")
    parser.add_argument("--verify-only", action="store_true", help="Read an existing observation; check every frozen SHA and regroup every sample")
    parser.add_argument("--validation-output", type=Path, help="New independent readback receipt, used with --verify-only")
    parser.add_argument("--tw-public-operation-journal", type=Path, action="append", default=[],
                        help="Freeze supplied journald JSONL; bind exact registered native TW-public dispatch clocks without launching providers")
    parser.add_argument("--crypto-feature-report", nargs=3, action="append",
                        metavar=("PROVIDER", "CSV", "SUMMARY"),
                        help="Native binance/okx worker CSV and matching feature-run summary; defaults to available canonical data_PROVIDER/1m pairs. No provider calls or source-coverage promotion")
    parser.add_argument("--crypto-issuer-parser-report", nargs=4, action="append", default=[],
                        metavar=("TIMING", "PROTOCOL", "OUTPUTS", "READBACK"),
                        help="Explicit retained issuer-parser bundle; verify all frozen raw/normalized rows, retain offline stage scope, and make no provider calls")
    parser.add_argument("--retained-dune-report", nargs=3, action="append", default=[],
                        metavar=("TIMING", "PROTOCOL", "READBACK"),
                        help="Explicit retained Dune native clocks; independently verify every original/output row, receipt and raw page, with no provider operations")
    parser.add_argument("--retained-sec-report", nargs=4, action="append", default=[],
                        metavar=("TIMING", "PROTOCOL", "RESULTS", "READBACK"),
                        help="Explicit retained SEC native clocks; independently verify every original/output row, mapping, manifest and cached document, with no provider operations")
    parser.add_argument("--local-credential-audit-report", type=Path, action="append", default=[],
                        help="Fixed completed local credential audit directory; retain 45 presence-gate clocks and original public sources; no provider calls or entitlement claim")
    parser.add_argument("--benchmark-receipt", type=Path, action="append", default=[])
    parser.add_argument("--metric-backend", choices=("python", "auto", "cpp"), default="auto",
                        help="Benchmark field walker and optional Linux batch reads; auto uses a source/ABI-verified cache; cpp explicitly builds an absent cache")
    parser.add_argument("--code-inventory-workers", type=int, choices=(0, 1, 2, 4), default=0,
                        help="Bounded spawn processes for static source parsing; 0 remains synchronous; measure complete workflow CPU/RSS")
    parser.add_argument("--evidence-read-workers", type=int, choices=(0, 1, 2, 4), default=0,
                        help="Ordered benchmark read-ahead; 0 is synchronous, positive counts use bounded read-only workers")
    parser.add_argument("--profile-benchmark-inspection", action="store_true",
                        help="Diagnostic per-thread operation clocks; changes timings, excluded from optimization samples")
    parser.add_argument("--evidence-read-batch-mib", type=int, choices=(1, 2, 8, 16, 256), default=2,
                        help="Raw bytes per read-ahead batch; Python raw buffers <= (workers + 1) * (limit + 1); native conversion raw copies <= (2 * workers + 1) * (limit + 1)")
    parser.add_argument("--training-root", type=Path, action="append")
    parser.add_argument("--training-depth", type=int, default=9)
    parser.add_argument("--training-discovery-threads", type=int, choices=(0, 1, 2, 4), default=0,
                        help="rg traversal threads; 0 retains rg's default. Re-measure on the configured roots")
    parser.add_argument("--benchmark-depth", type=int, default=15)
    parser.add_argument("--benchmark-directory-budget", type=int, default=50_000)
    parser.add_argument("--registered-runs", type=int, default=3, help="Latest recorded step sets per canonical scope; not whole-run acceptance")
    parser.add_argument("--agent-runs", type=int, default=256, help="Latest terminal local-owner engineering commands")
    parser.add_argument("--evidence-storage-layout", choices=("individual_gzip", "packed_gzip_v1"),
                        default="individual_gzip", help="Explicit snapshot artifact storage ABI")
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
    if args.crypto_feature_report is None:
        args.crypto_feature_report = []
        for provider in ("binance", "okx"):
            root = ROOT / ("data_" + provider) / "1m"
            report = root / "historical_feature_report.csv"
            summary = root / "download_summary.historical_features.json"
            if report.is_file() or summary.is_file():
                args.crypto_feature_report.append((provider, str(report), str(summary)))
    if any(provider not in {"binance", "okx"} for provider, _, _ in args.crypto_feature_report):
        parser.error("crypto feature provider must be binance or okx")
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
