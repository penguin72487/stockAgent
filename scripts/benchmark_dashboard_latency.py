#!/usr/bin/env python3
"""Read-only HTTP timings plus an isolated atomic-receipt-to-SSE benchmark."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, UTC
from http.client import HTTPConnection
import json
from pathlib import Path
import statistics
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
from urllib.parse import urlsplit
from unittest.mock import patch
from contextlib import ExitStack, nullcontext

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.serve_public_dashboards import PublicDashboardServer  # noqa: E402

DEFAULT_PATHS = (
    "/", "/healthz", "/api/overview", "/taifex/", "/taifex/api/status", "/taifex/api/history?range=1d",
    *tuple(path for prefix in ("tw-day-trade", "tw-overnight") for path in (
        f"/{prefix}/", f"/{prefix}/api/revision", f"/{prefix}/api/status",
        f"/{prefix}/api/signals?limit=100", f"/{prefix}/api/positions?limit=100",
        f"/{prefix}/api/events?limit=100", f"/{prefix}/api/summary",
        f"/{prefix}/api/history?range=all&resolution=1m&encoding=v2",
        f"/{prefix}/api/public-data-status",
    )),
    "/shioaji/", "/shioaji/api/status", "/finlab/", "/finlab/api/status",
    "/finmind/", "/finmind/api/status", "/openbb/", "/openbb/api/status",
    "/openbb/api/history?range=1d", "/data-monitor/", "/data-monitor/api/summary",
    "/data-monitor/api/status", "/data-monitor/api/details", "/data-monitor/api/features",
    "/traffic/", "/traffic/api/status", "/traffic/api/history?range=24h",
    # Broad-range detail filters are materially different from the latest-day
    # paths above: they exercise indexed ledgers and bounded projection reads.
    *tuple(
        f"/tw-day-trade/api/{kind}?start_date=2026-02-25&end_date={datetime.now().astimezone().date().isoformat()}&limit=100"
        for kind in ("signals", "events")
    ),
)


def summarize(values):
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "median_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    return {"n": len(values), "median_ms": round(statistics.median(values), 3),
            "p95_ms": round(ordered[max(0, math.ceil(len(ordered) * .95) - 1)], 3),
            "p99_ms": round(ordered[max(0, math.ceil(len(ordered) * .99) - 1)], 3),
            "max_ms": round(max(values), 3)}


def service_snapshot(*, include_host_services=False):
    """Allowlisted local runtime and last-attempt evidence for every installed service.

    Last-attempt duration is distinct from the age of a daemon and from an
    end-to-end request latency. Never map a failed or never-run job to zero.
    """
    started = time.perf_counter()
    text_properties = ("Id", "ActiveState", "SubState", "Type", "MainPID", "NRestarts", "Result", "InvocationID", "Transient", "UnitFileState")
    counter_properties = ("CPUUsageNSec", "MemoryCurrent", "MemoryPeak", "MemoryHigh", "MemoryMax", "IOReadBytes", "IOWriteBytes",
                          "ExecMainStatus", "ExecMainStartTimestampMonotonic", "ExecMainExitTimestampMonotonic")
    properties = (*text_properties, "ControlGroup", *counter_properties)
    result = subprocess.run(
        ["systemctl", "show", "*.service" if include_host_services else "stockagent-*.service", "--all", "--no-pager",
         "--property=" + ",".join(properties)],
        check=True, capture_output=True, text=True, timeout=10,
    )
    units = {}
    for block in result.stdout.split("\n\n"):
        values = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        unit = values.get("Id", "")
        if not unit.endswith(".service") or (not include_host_services and not unit.startswith("stockagent-")):
            continue
        units[unit] = {key: values.get(key) for key in text_properties}
        for key in counter_properties:
            raw = values.get(key, "")
            number = int(raw) if raw.isdecimal() else None
            # systemd's UINT64_MAX is unavailable accounting, never a real size.
            units[unit][key] = number if number is not None and number < 2**64 - 1 else None
        units[unit].update(_cgroup_memory_breakdown(values.get("ControlGroup"), unit))
        units[unit].update(_cgroup_memory_events(values.get("ControlGroup"), unit))
        units[unit].update(_cgroup_io_bytes(values.get("ControlGroup"), unit))
        start = units[unit]["ExecMainStartTimestampMonotonic"]
        end = units[unit]["ExecMainExitTimestampMonotonic"]
        units[unit]["last_attempt_seconds"] = (
            (end - start) / 1e6 if start and end and end >= start else None
        )
        units[unit]["running_age_seconds"] = (
            max(0, time.monotonic() - start / 1e6)
            if start and not end and values.get("ActiveState") in {"active", "activating"}
            else None
        )
        units[unit]["last_attempt_outcome"] = classify_service_attempt(units[unit])
    if not units:
        raise ValueError("no StockAgent service evidence")
    return {"observed_at": datetime.now(UTC).isoformat(),
            "monotonic": time.monotonic(), "query_ms": (time.perf_counter()-started)*1000,
            "units": units}


def _cgroup_memory_breakdown(control_group, unit, *, root=Path("/sys/fs/cgroup")):
    """Split cgroup memory into anon/file; MemoryCurrent is not process RSS."""

    unknown = {"MemoryAnon": None, "MemoryFile": None}
    if not isinstance(control_group, str) or not control_group.startswith("/"):
        return unknown
    parts = Path(control_group).parts
    if parts[-1] != unit or any(part in {".", ".."} for part in parts):
        return unknown
    try:
        values = dict(
            line.split(" ", 1)
            for line in (root.joinpath(*parts[1:]) / "memory.stat")
            .read_text(encoding="ascii").splitlines()
            if " " in line
        )
        anon = int(values["anon"])
        file = int(values["file"])
        if anon < 0 or file < 0:
            return unknown
    except (OSError, ValueError, KeyError):
        return unknown
    return {"MemoryAnon": anon, "MemoryFile": file}


def _cgroup_memory_events(control_group, unit, *, root=Path("/sys/fs/cgroup")):
    """Read cumulative cgroup pressure counters without treating peak as current."""

    fields = {
        "high": "MemoryHighEvents",
        "max": "MemoryMaxEvents",
        "oom": "MemoryOomEvents",
        "oom_kill": "MemoryOomKillEvents",
    }
    unknown = {name: None for name in fields.values()}
    if not isinstance(control_group, str) or not control_group.startswith("/"):
        return unknown
    parts = Path(control_group).parts
    if parts[-1] != unit or any(part in {".", ".."} for part in parts):
        return unknown
    try:
        values = dict(
            line.split(" ", 1)
            for line in (root.joinpath(*parts[1:]) / "memory.events")
            .read_text(encoding="ascii").splitlines()
            if " " in line
        )
        counters = {name: int(values[key]) for key, name in fields.items()}
        if any(value < 0 for value in counters.values()):
            return unknown
    except (OSError, ValueError, KeyError):
        return unknown
    return counters


def _cgroup_io_bytes(control_group, unit, *, root=Path("/sys/fs/cgroup")):
    """Read block-device I/O counters, not logical file growth or network use."""

    unknown = {"CgroupIOReadBytes": None, "CgroupIOWriteBytes": None}
    if not isinstance(control_group, str) or not control_group.startswith("/"):
        return unknown
    parts = Path(control_group).parts
    if parts[-1] != unit or any(part in {".", ".."} for part in parts):
        return unknown
    try:
        rows = (root.joinpath(*parts[1:]) / "io.stat").read_text(
            encoding="ascii"
        ).splitlines()
        if not rows:
            return unknown
        read_bytes = write_bytes = 0
        for row in rows:
            fields = dict(field.split("=", 1) for field in row.split()[1:] if "=" in field)
            read_bytes += int(fields["rbytes"])
            write_bytes += int(fields["wbytes"])
        if read_bytes < 0 or write_bytes < 0:
            return unknown
    except (OSError, ValueError, KeyError):
        return unknown
    return {"CgroupIOReadBytes": read_bytes, "CgroupIOWriteBytes": write_bytes}


def classify_service_attempt(row):
    """Classify process evidence without treating systemd Result as data health."""
    if row.get("ActiveState") in {"active", "activating"}:
        return "running"
    if row.get("ActiveState") == "failed" or row.get("Result") not in {None, "success"}:
        return "failed"
    if row.get("last_attempt_seconds") is None:
        return "not_observed"
    status = row.get("ExecMainStatus")
    if status is None:
        return "exit_status_unknown"
    return "process_exited_zero" if status == 0 else "nonzero_exit"


def service_deltas(before, after):
    if "error" in before or "error" in after:
        return {"error": "service_snapshot_unavailable"}
    seconds = after["monotonic"] - before["monotonic"]
    rows = []
    for unit, current in after["units"].items():
        old = before["units"].get(unit, {})
        same_process = bool(old.get("InvocationID") and old.get("InvocationID") == current.get("InvocationID")
                        and old.get("MainPID") == current.get("MainPID")
                        and old.get("NRestarts") == current.get("NRestarts"))
        continuous_process = (
            same_process
            and old.get("ActiveState") in {"active", "activating"}
            and current.get("ActiveState") in {"active", "activating"}
        )
        previous_cpu, cpu = old.get("CPUUsageNSec"), current.get("CPUUsageNSec")
        cpu_delta = (cpu - previous_cpu if continuous_process and previous_cpu is not None
                     and cpu is not None and cpu >= previous_cpu else None)
        memory_event_deltas = {}
        for field in ("MemoryHighEvents", "MemoryMaxEvents", "MemoryOomEvents", "MemoryOomKillEvents"):
            previous, present = old.get(field), current.get(field)
            memory_event_deltas[field + "Delta"] = (
                present - previous
                if continuous_process and previous is not None and present is not None
                and present >= previous else None
            )
        io_deltas = {}
        for field in ("CgroupIOReadBytes", "CgroupIOWriteBytes"):
            previous, present = old.get(field), current.get(field)
            io_deltas[field + "Delta"] = (
                present - previous
                if continuous_process and previous is not None and present is not None
                and present >= previous else None
            )
        rows.append({"unit": unit, **current,
                     "cpu_used_ms": cpu_delta / 1e6 if cpu_delta is not None else None,
                     "cpu_cores_average": cpu_delta / 1e9 / seconds if cpu_delta is not None and seconds > 0 else None,
                     **memory_event_deltas,
                     **io_deltas,
                     "same_process": same_process})
    return {"sample_seconds": seconds, "rows": rows,
            "meaning": "Measured local service resource counters; not end-to-end latency or data-health proof."}


def profile_inventory(root, repeats, reference=None):
    """Replay one frozen footer inventory, isolating CPU from changing sources.

    Discovery, JSON decode and file-stat observation are measured separately.
    No producer, production cache, ledger, or broker connection is modified.
    """
    from stockagent.live import data_monitor_inventory as current
    root = Path(root)
    started = time.perf_counter()
    payload = current._read_json(root / "artifacts/live/data_monitor/record_inventory_cache.json")
    decoded_at = time.perf_counter()
    if not isinstance(payload, dict) or payload.get("version") != current.INVENTORY_VERSION:
        raise ValueError("current footer inventory required")
    selected = current._selected_files(root)
    discovered_at = time.perf_counter()

    class ObservedPath:
        def __init__(self, path):
            self.path = str(path)
            try:
                self.observation = path.stat()
            except OSError:
                self.observation = None
            self.name = path.name
            self.identity = (current._source_file_identity(path, self.observation)
                             if self.observation is not None else None)

        def __str__(self):
            return self.path

        def __fspath__(self):
            return self.path

        def stat(self):
            if self.observation is None:
                raise FileNotFoundError(self.path)
            return self.observation

    frozen = {key: [ObservedPath(path) for path in paths] for key, paths in selected.items()}
    identities = {str(path): path.identity for paths in frozen.values() for path in paths}
    observed_at = time.perf_counter()
    modules = {"current": current}
    if reference is not None:
        spec = importlib.util.spec_from_file_location("_inventory_benchmark_reference", reference)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        # Reference releases are immutable. A SourceFileLoader would otherwise
        # create __pycache__ beside this private source when run as root, making
        # the qualified release fail its extra-member cold-start gate.
        exec(compile(reference.read_bytes(), str(reference), "exec"), vars(module))
        modules["reference"] = module
    samples = {name: [] for name in modules}
    digests = {name: [] for name in modules}
    counts = {}
    # Alternate order so one implementation cannot always inherit the warm CPU.
    for repeat in range(repeats):
        for name in (list(modules) if repeat % 2 == 0 else list(reversed(modules))):
            module = modules[name]
            with ExitStack() as stack:
                stack.enter_context(patch.object(module, "_read_json", return_value=payload))
                stack.enter_context(patch.object(module, "_selected_files", return_value=frozen))
                if hasattr(module, "_source_file_identity"):
                    stack.enter_context(patch.object(module, "_source_file_identity",
                        side_effect=lambda path, metadata: identities[str(path)]))
                start = time.perf_counter()
                result = module.build_feature_inventory(root)
                samples[name].append((time.perf_counter() - start) * 1000)
            result["rows"].sort(key=lambda row: (row["dataset_id"], row["field"]))
            digests[name].append(hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
            counts[name] = len(result["rows"])
    hashes = {digest for values in digests.values() for digest in values}
    report = {"kind": "frozen_inventory_cpu_replay_not_source_rebuild",
            "inputs": {"decode_ms": (decoded_at-started)*1000,
                       "discovery_ms": (discovered_at-decoded_at)*1000,
                       "stat_ms": (observed_at-discovered_at)*1000,
                       "stat_includes_partition_proof": True,
                       "files": sum(map(len, frozen.values())),
                       "payload_sha256": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()},
            "implementations": {name: {"timing": summarize(samples[name]), "samples_ms": samples[name],
                                       "output_sha256": digests[name], "fields": counts[name],
                                       "source_sha256": hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()}
                                for name, module in modules.items()},
            "outputs_identical": len(hashes) == 1}
    if len(hashes) != 1:
        report["error"] = "inventory_output_mismatch"
    return report


def profile_shioaji_monitor(root, repeats):
    """Real local I/O with identical sequential/parallel task definitions.

    Clears only this benchmark process's caches. No broker calls, service
    restart, OS cache flush or production gateway cache invalidation.
    """
    from stockagent.live import shioaji_api_dashboard as monitor

    class SerialExecutor:
        def __init__(self, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as error:
                future.set_exception(error)
            return future

    samples = {"sequential_control": [], "parallel": []}
    hashes = {key: [] for key in samples}
    observed = datetime.now(UTC)
    for repeat in range(repeats):
        for label in (list(samples) if repeat % 2 == 0 else list(reversed(samples))):
            monitor._JSON_FILE_CACHE.clear()
            monitor._JOURNAL_CACHE.clear()
            context = patch.object(monitor, "ThreadPoolExecutor", SerialExecutor) if label == "sequential_control" else nullcontext()
            with context:
                started = time.perf_counter()
                result = monitor.build_shioaji_public_status(root, now=observed)
                samples[label].append((time.perf_counter()-started)*1000)
            hashes[label].append(hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest())
    return {"boundary": "local systemd/journal/files; process caches cleared; OS cache may be warm; live evidence can change",
            "implementations": {key: {"timing": summarize(values), "samples_ms": values,
                                       "output_sha256": hashes[key]} for key, values in samples.items()},
            "outputs_stable": len({value for values in hashes.values() for value in values}) == 1}


def http_sample(session, url, timeout):
    started = time.perf_counter()
    try:
        with session.get(url, stream=True, timeout=(min(10., timeout), timeout), allow_redirects=False) as response:
            headers_at = time.perf_counter()
            body = response.content
            ended = time.perf_counter()
            parse_started = ended
            valid_json = None
            content_type = response.headers.get("Content-Type", "").lower()
            is_api = "/api/" in urlsplit(url).path
            if "application/json" in content_type:
                try:
                    valid_json = isinstance(json.loads(body), dict)
                except (ValueError, UnicodeError):
                    valid_json = False
            parse_ms = (time.perf_counter() - parse_started) * 1000
            return {"status": response.status_code,
                    "ok": response.status_code == 200 and (valid_json is True if is_api else valid_json is not False),
                    "content_type": content_type, "valid_json_object": valid_json,
                    "total_ms": (ended-started)*1000, "headers_ms": (headers_at-started)*1000,
                    "body_ms": (ended-headers_at)*1000, "parse_ms": parse_ms,
                    "decoded_bytes": len(body), "wire_bytes": response.headers.get("Content-Length"),
                    "encoding": response.headers.get("Content-Encoding", "identity"),
                    "server_timing": response.headers.get("Server-Timing", ""),
                    "server_ms": next((float(v) for v in re.findall(r'(?:^|,\s*)app;dur=([\d.]+)', response.headers.get("Server-Timing", ""))), None)}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__, "total_ms": (time.perf_counter()-started)*1000}


def timing_summary(samples):
    successes = [sample for sample in samples if sample["ok"]]
    return {field: summarize([s[field] for s in successes if s.get(field) is not None])
            for field in ("total_ms", "headers_ms", "body_ms", "parse_ms", "server_ms")}


def warm_samples(row, repeats):
    # Schema 2 originally flattened each worker's sequential batch. Preserve
    # compatibility while excluding its first (new-session) connection attempt.
    return [sample for index, sample in enumerate(row["samples"]) if index % repeats]


def capture_measurement(builder):
    try:
        return builder()
    except Exception as exc:
        # Persist failed optional phases as failures, never as zero latency.
        return {"error": type(exc).__name__}


def profile_tw_status_projection(payload, repeats, reference=None):
    """ABBA on one full receipt: projection -> JSON -> gzip -> full readback.

    The visible-value oracle ignores ONLY the unrendered per-attempt history.
    No source/fill/failure count, latest reason or minute point is discarded
    from that oracle. This is not an HTTP, browser or opening SLA measurement.
    """
    import gzip
    from scripts.serve_public_dashboards import _prepared
    from stockagent.live import public_dashboards as current
    modules = {"current": current}
    if reference is not None:
        spec = importlib.util.spec_from_file_location("_tw_status_projection_reference", reference)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        # Never add bytecode files to an immutable qualified reference release.
        exec(compile(reference.read_bytes(), str(reference), "exec"), vars(module))
        modules = {"reference": module, **modules}
    def digest(value):
        return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                         separators=(",", ":")).encode()).hexdigest()
    before = digest(payload)
    samples, visible_digests = [], set()
    for repeat in range(repeats):
        order = list(modules) if repeat % 2 == 0 else list(reversed(modules))
        for variant in order:
            started = time.perf_counter()
            projected = modules[variant].sanitize_tw_status(payload)
            projection_done = time.perf_counter()
            body = (json.dumps(projected, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            json_done = time.perf_counter()
            prepared = _prepared(body, content_type="application/json; charset=utf-8", cache_control="no-store")
            gzip_done = time.perf_counter()
            decoded = json.loads(gzip.decompress(prepared.gzip_body)
                                 if prepared.gzip_body is not prepared.body else prepared.body)
            readback_done = time.perf_counter()
            if decoded != projected:
                raise ValueError("status wire readback changed values")
            visible = dict(decoded)
            if isinstance(visible.get("opening_signal_latency"), dict):
                visible["opening_signal_latency"] = dict(visible["opening_signal_latency"])
                visible["opening_signal_latency"].pop("failures", None)
            visible_digests.add(digest(visible))
            if len(visible_digests) != 1:
                raise ValueError("status projection changed retained display values")
            samples.append({"variant": variant, "repeat": repeat,
                "projection_ms": (projection_done - started) * 1000,
                "json_encode_ms": (json_done - projection_done) * 1000,
                "gzip_etag_ms": (gzip_done - json_done) * 1000,
                "full_readback_ms": (readback_done - gzip_done) * 1000,
                "complete_ms": (readback_done - started) * 1000,
                "decoded_bytes": len(body), "wire_bytes": len(prepared.gzip_body),
                "visible_contract_sha256": next(iter(visible_digests))})
    if digest(payload) != before:
        raise ValueError("status projection mutated source receipt")
    return {"scope": "fixed_full_receipt_public_projection_wire_readback_not_opening_sla",
        "input_sha256": before, "input_failure_count": len((payload.get("opening_signal_latency") or {}).get("failures") or []),
        "retained_display_values_equal": True, "source_receipt_unchanged": True,
        "source_build_network_browser_and_executor_excluded": True,
        "samples": samples, "variants": {name: summarize([row["complete_ms"] for row in samples
                                                            if row["variant"] == name]) for name in modules}}


def profile_opening_attempt_tail(state_dir, repeats):
    """Cold process-tail-cache ABBA; full rows, failures and summary parity.

    The original append-only JSONL is never rewritten. File/OS caches are not
    flushed, and this local reader is not a quote/executor/browser SLA probe.
    """
    from stockagent.live import tw_day_trade_dashboard as dashboard

    source = Path(state_dir) / "opening_signal_latency.jsonl"
    from stockagent.live.paper_mode_snapshot import read_paper_state
    state = read_paper_state(Path(state_dir) / "state.json")
    markets = sorted((state.get("modes") or {}).keys())
    session_date = datetime.now(dashboard.TAIPEI).date().isoformat()

    def signature():
        stat = source.stat()
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)

    def source_digest():
        digest = hashlib.sha256()
        with source.open("rb") as handle:
            while chunk := handle.read(1 << 20):
                digest.update(chunk)
        return digest.hexdigest()

    observed = signature()
    original_digest = source_digest()
    samples, expected_digest = [], None
    for repeat in range(repeats):
        for variant in ("stdlib", "native", "native", "stdlib"):
            if signature() != observed:
                raise ValueError("opening tail source changed during comparison")
            # Only this benchmark's private process cache is replaced. All
            # native service owners and their caches remain untouched.
            with patch.object(dashboard, "_TAIL_CACHE", {}):
                started, cpu = time.perf_counter(), time.thread_time()
                kwargs = ({"row_decoder": dashboard._decode_opening_latency_row}
                          if variant == "native" else
                          {"row_projection": dashboard._opening_latency_row_projection})
                rows = dashboard._tail(source, 10_000, **kwargs)
                tail_done = time.perf_counter()
                summary = dashboard._opening_signal_latency_summary(
                    rows, expected_markets=markets, session_date=session_date,
                )
                encoded = json.dumps(summary, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
                # Compare the complete JSON contract, including any preserved
                # nonfinite source extensions, without Python NaN != NaN noise.
                readback = json.dumps(json.loads(encoded), sort_keys=True,
                                      ensure_ascii=True, separators=(",", ":"))
                digest = hashlib.sha256(readback.encode()).hexdigest()
                if encoded != readback:
                    raise ValueError("opening summary wire readback changed values")
                if expected_digest is None:
                    expected_digest = digest
                if digest != expected_digest:
                    raise ValueError("opening tail decoder changed the complete summary")
                samples.append({"repeat": repeat, "variant": variant,
                    "tail_read_decode_project_ms": (tail_done - started) * 1000,
                    "complete_ms": (time.perf_counter() - started) * 1000,
                    "owner_thread_cpu_ms": (time.thread_time() - cpu) * 1000,
                    "row_count": len(rows), "summary_failure_count": summary["failure_count"],
                    "complete_summary_sha256": digest})
    if signature() != observed or source_digest() != original_digest:
        raise ValueError("opening tail source mutated during comparison")
    return {"scope": "fixed_source_full_tail_summary_readback_not_opening_sla",
        "source": str(source), "source_signature": observed, "source_sha256": original_digest,
        "native_decoder_available": dashboard._OPENING_FAILURE_DECODER is not None,
        "source_unchanged": True, "complete_summary_parity": True,
        "tail_cache_cleared_each_sample": True, "os_cache_flushed": False,
        "broker_orders": False, "ledger_writes": False,
        "samples": samples, "variants": {name: summarize([row["complete_ms"] for row in samples
            if row["variant"] == name]) for name in ("stdlib", "native")}}


def profile_opening_attempt_append(state_dir, repeats):
    """Full native-decode refresh versus prefix-verified incremental refresh.

    Use one private byte-for-byte source copy and append its last physical row
    verbatim as a labeled engineering fixture. Neither variant changes history,
    drops failed attempts, touches provider owners, or pretends to be 09:00.
    Cache seeding and file copying are retained outside the refresh interval.
    """
    from stockagent.live import tw_day_trade_dashboard as dashboard
    source = Path(state_dir) / "opening_signal_latency.jsonl"
    from stockagent.live.paper_mode_snapshot import read_paper_state
    state = read_paper_state(Path(state_dir) / "state.json")
    markets = sorted((state.get("modes") or {}).keys())
    session_date = datetime.now(dashboard.TAIPEI).date().isoformat()
    signature = lambda: dashboard._tail_file_signature(source.stat())
    def file_digest(path):
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1 << 20):
                digest.update(chunk)
        return digest.hexdigest()
    observed, original_digest = signature(), file_digest(source)
    with source.open("rb") as handle:
        handle.seek(dashboard._jsonl_tail_start(handle, observed[2], 1))
        last_line = handle.read()
    if not last_line.strip():
        raise ValueError("opening append fixture needs a nonblank final physical row")
    # Validate the captured physical record; never append a partial JSON row.
    json.loads(last_line)
    appended = (b"" if last_line.endswith(b"\n") else b"\n") + last_line.rstrip(b"\r\n") + b"\n"
    samples, expected_digest = [], None
    with tempfile.TemporaryDirectory(prefix="stockagent-opening-append-") as directory:
        fixture = Path(directory) / "opening_signal_latency.jsonl"
        copy_started = time.perf_counter()
        shutil.copyfile(source, fixture)
        if signature() != observed or file_digest(fixture) != original_digest:
            raise ValueError("opening append source changed during private snapshot")
        copy_ms = (time.perf_counter() - copy_started) * 1000
        for repeat in range(repeats):
            for variant in ("full_native", "verified_append", "verified_append", "full_native"):
                if signature() != observed:
                    raise ValueError("opening append source changed during comparison")
                with fixture.open("r+b") as handle:
                    handle.truncate(observed[2])
                with patch.object(dashboard, "_TAIL_CACHE", {}):
                    kwargs = {"row_decoder": dashboard._decode_opening_latency_row,
                              "verified_append": variant == "verified_append"}
                    warm_started = time.perf_counter()
                    dashboard._tail(fixture, 10_000, **kwargs)
                    initial_cache_seed_ms = (time.perf_counter() - warm_started) * 1000
                    with fixture.open("ab") as handle:
                        handle.write(appended)
                    started, cpu = time.perf_counter(), time.thread_time()
                    rows = dashboard._tail(fixture, 10_000, **kwargs)
                    decoded = time.perf_counter()
                    summary = dashboard._opening_signal_latency_summary(
                        rows, expected_markets=markets, session_date=session_date)
                    encoded = json.dumps(summary, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
                    readback = json.dumps(json.loads(encoded), sort_keys=True, ensure_ascii=True, separators=(",", ":"))
                    digest = hashlib.sha256(readback.encode()).hexdigest()
                    if encoded != readback or (expected_digest is not None and expected_digest != digest):
                        raise ValueError("verified append changed the complete opening summary")
                    expected_digest = digest
                    samples.append({"repeat": repeat, "variant": variant,
                        "initial_cache_seed_ms_outside_refresh": initial_cache_seed_ms,
                        "tail_refresh_ms": (decoded - started) * 1000,
                        "complete_ms": (time.perf_counter() - started) * 1000,
                        "owner_thread_cpu_ms": (time.thread_time() - cpu) * 1000,
                        "row_count": len(rows), "summary_failure_count": summary["failure_count"],
                        "complete_summary_sha256": digest})
    if signature() != observed or file_digest(source) != original_digest:
        raise ValueError("opening append source changed during comparison")
    return {"scope": "private_verbatim_row_append_full_summary_readback_not_native_opening_sla",
        "source": str(source), "source_signature": observed, "source_sha256": original_digest,
        "source_unchanged": True, "private_source_copy_and_validation_ms": copy_ms,
        "append_fixture_origin": "verbatim_final_physical_source_row_not_new_observation",
        "append_fixture_bytes": len(appended), "append_fixture_sha256": hashlib.sha256(appended).hexdigest(),
        "complete_summary_parity": True, "prefix_policy": "sha256_all_previously_observed_source_bytes_before_decoded_row_reuse",
        "remaining_complexity": "O(source_bytes)_integrity_scan_plus_delta_decode_not_O(delta)_IO",
        "native_decoder_available": dashboard._OPENING_FAILURE_DECODER is not None,
        "broker_orders": False, "ledger_writes": False, "samples": samples,
        "variants": {name: summarize([row["complete_ms"] for row in samples if row["variant"] == name])
                     for name in ("full_native", "verified_append")}}


def code_fingerprints():
    """Bind measurements to actual dirty-worktree web code, not only Git HEAD."""
    paths = {
        REPO_ROOT / "scripts/serve_public_dashboards.py",
        REPO_ROOT / "scripts/benchmark_dashboard_latency.py",
        REPO_ROOT / "scripts/audit_public_dashboards_browser.mjs",
        REPO_ROOT / "stockagent/live/dashboard_updates.py",
        REPO_ROOT / "stockagent/live/data_monitor_inventory.py",
        REPO_ROOT / "scripts/snapshot_data_refresh_services.py",
        *REPO_ROOT.glob("stockagent/live/*dashboard*.py"),
        *(p for p in (REPO_ROOT / "services").rglob("*")
          if p.is_file() and p.suffix in {".js", ".css", ".html"}),
    }
    return {str(p.relative_to(REPO_ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths)}


def measure_http(base_url, paths, *, repeats, concurrency, timeout):
    import requests
    rows = []
    for path in paths:
        # Sessions are reused sequentially, never shared by concurrent threads.
        def batch(count):
            with requests.Session() as session:
                return [http_sample(session, base_url.rstrip("/")+path, timeout) for _ in range(count)]
        first = batch(1)[0]
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            groups = list(pool.map(batch, [repeats] * concurrency))
        elapsed = time.perf_counter()-started
        samples = [sample for group in groups for sample in group]
        successes = [s for s in samples if s["ok"]]
        row = {"path": path, "first_observed": first, "samples": samples,
               "errors": sum(not s["ok"] for s in [first, *samples]),
               "requests_per_second": len(successes)/elapsed,
               "steady": timing_summary(samples),
               "connection_first": timing_summary([group[0] for group in groups]),
               "warm_reuse": timing_summary([s for group in groups for s in group[1:]])}
        rows.append(row)
        print(json.dumps({"path": path, "first_ms": round(first["total_ms"], 3),
                          **row["steady"]["total_ms"], "errors": row["errors"]}), flush=True)
    return rows


def profile_history(
    state_dir, repeats, *, source_rebuild=False, full_source_rebuild=False,
    source_reference=None, wire=False,
):
    """Profile the canonical builder in this fresh CLI process, not the service.

    ``source_rebuild`` bypasses the final combined projection while retaining
    verified immutable session projections. ``full_source_rebuild`` also
    bypasses those shards and measures the complete canonical scan. Neither
    mode flushes the OS page cache or compact helper indexes.
    """
    from stockagent.live.dashboard_updates import file_signature, metadata_signature
    from stockagent.live.tw_day_trade_dashboard import (
        OVERNIGHT_HISTORY_FILENAME,
        build_dashboard_history_snapshot,
    )
    from stockagent.live import tw_day_trade_dashboard
    import resource

    source_path = Path(tw_day_trade_dashboard.__file__)
    source = source_path.read_bytes()
    if source_reference is not None:
        source = Path(source_reference).read_bytes()
        module = types.ModuleType("_dashboard_history_benchmark_reference")
        module.__file__ = str(source_path)
        sys.modules[module.__name__] = module
        exec(compile(source, str(source_path), "exec"), module.__dict__)
        build_dashboard_history_snapshot = module.build_dashboard_history_snapshot
    if wire:
        import gzip
        from stockagent.live.public_dashboards import sanitize_tw_history
        from scripts.serve_public_dashboards import _prepared

    root = state_dir.resolve(strict=True)
    index_cache_dir = os.environ.get("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", "").strip()
    session_projection_enabled = bool(
        not full_source_rebuild
        and index_cache_dir
        and Path(index_cache_dir).is_dir()
    )
    source_names = {
        "marks.jsonl",
        "benchmark_history.json",
        "benchmark_marks.jsonl",
        OVERNIGHT_HISTORY_FILENAME,
        "state.json",
    }

    def inventory():
        # Stability must cover the builder's inputs, not unrelated live status,
        # latency, order or fill receipts that legitimately change in parallel.
        result = {}
        for name in sorted(source_names):
            path = root / name
            try:
                result[name] = metadata_signature(path.stat())
            except FileNotFoundError:
                continue
        return result
    before = inventory()
    samples = []
    for _ in range(repeats):
        started = time.perf_counter()
        cpu_started = time.process_time()
        build_options = dict(
            state_dir=root,
            range_key="all",
            resolution="1m",
            history_encoding="minute_columns_v2",
            use_memory_cache=not source_rebuild,
            use_persistent_cache=not source_rebuild,
        )
        if full_source_rebuild:
            build_options["use_session_projection"] = False
        payload = build_dashboard_history_snapshot(**build_options)
        elapsed = (time.perf_counter() - started) * 1000
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        sample = {"build_ms": elapsed, "sha256": hashlib.sha256(body).hexdigest(),
                  "decoded_bytes": len(body), "point_count": payload.get("returned_points"),
                  "series_count": len(payload.get("minute_series", []))}
        if wire:
            projected = sanitize_tw_history(payload, private_prices=True)
            encoded = (json.dumps(projected, ensure_ascii=False, separators=(",", ":"),
                                  allow_nan=False) + "\n").encode()
            response = _prepared(encoded, content_type="application/json; charset=utf-8",
                                 cache_control="no-cache")
            represented_at = time.perf_counter()
            decoded = gzip.decompress(response.gzip_body)
            if decoded != response.body or json.loads(decoded) != projected:
                raise ValueError("complete HTTP representation readback differs")
            sample.update(
                representations_ms=(represented_at - started) * 1000 - elapsed,
                independent_readback_ms=(time.perf_counter() - represented_at) * 1000,
                http_sha256=hashlib.sha256(response.body).hexdigest(),
                gzip_sha256=hashlib.sha256(response.gzip_body).hexdigest(),
                http_bytes=len(response.body), gzip_bytes=len(response.gzip_body),
            )
        sample.update(workflow_ms=(time.perf_counter() - started) * 1000,
                      cpu_seconds=time.process_time() - cpu_started,
                      peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
        samples.append(sample)
    signature_samples = []
    for name in sorted(source_names & before.keys()):
        path = root / name
        size = path.stat().st_size
        if size > 16 * 1024 * 1024:
            signature_samples.append({"file": name, "bytes": size, "skipped": "signature microbenchmark is bounded to 16 MiB files"})
            continue
        cold_start = time.perf_counter()
        file_signature(path)
        first_ms = (time.perf_counter() - cold_start) * 1000
        timings = []
        for _ in range(100):
            started = time.perf_counter()
            file_signature(path)
            timings.append((time.perf_counter() - started) * 1000)
        signature_samples.append({"file": name, "bytes": size,
                                  "first_ms": first_ms, "hot": summarize(timings)})
    stable = before == inventory()
    boundary = (
        "fresh CLI process; final projection and immutable session projection "
        "bypassed; OS page cache and helper indexes may be warm"
        if full_source_rebuild
        else "fresh CLI process; final memory/persisted projection bypassed; "
        "immutable session projection unavailable because the dashboard index "
        "cache directory is not configured or does not exist; OS page cache may be warm"
        if source_rebuild and not session_projection_enabled
        else "fresh CLI process; final memory/persisted projection bypassed; "
        "verified immutable session projection enabled; OS page cache and helper "
        "indexes may be warm"
        if source_rebuild
        else "fresh CLI process; OS/persisted cache may be warm; no production cache flush"
    )
    return {"boundary": boundary,
            "source_rebuild": bool(source_rebuild),
            "full_source_rebuild": bool(full_source_rebuild),
            "session_projection_enabled": session_projection_enabled,
            "index_cache_dir": index_cache_dir or None,
            "state_dir": str(root), "source_stable": stable, "samples": samples,
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "source_reference": str(source_reference) if source_reference is not None else None,
            "wire_and_independent_readback": bool(wire),
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "inputs": before,
            "outputs_identical": len({s["sha256"] for s in samples}) == 1,
            "signature": signature_samples}


def isolated_notification(repeats):
    with tempfile.TemporaryDirectory(prefix="dashboard-latency-") as directory:
        root = Path(directory)
        state = root / "artifacts/live/tw_day_trade_simulation"
        state.mkdir(parents=True)
        path = state / "status.json"
        server = PublicDashboardServer(
            ("127.0.0.1", 0), repo_root=root,
            **{f"{name}_static_root": root for name in ("public", "taifex", "tw", "shioaji", "openbb", "data_monitor", "traffic")},
            taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1",
        )
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        samples = []
        try:
            connection.request("GET", "/tw-day-trade/api/updates")
            response = connection.getresponse()
            assert response.status == 200

            def receive(expected):
                while True:
                    line = response.fp.readline()
                    if not line:
                        raise RuntimeError("stream closed before receipt")
                    if line.startswith(b"data: "):
                        event = json.loads(line[6:])
                        if int(event.get("state_revision") or 0) == expected:
                            return event

            receive(0)
            for revision in range(1, repeats + 1):
                staged = state / "status.tmp"
                staged.write_text(json.dumps({"state_revision": revision, "content_revision": revision}))
                started = time.perf_counter()
                staged.replace(path)
                receive(revision)
                samples.append((time.perf_counter() - started) * 1000)
            native = server.update_hub.native
            response.close()
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)
    return {"boundary": "isolated atomic receipt replacement to loopback SSE client; no trading data or browser paint", "inotify": native, **summarize(samples)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8770")
    parser.add_argument("--repeats", type=int, default=10, help="Samples per worker per route, including its new connection")
    parser.add_argument("--concurrency", type=int, default=1, help="Read-only benchmark clients, not a server limit")
    parser.add_argument("--timeout", type=float, default=30.)
    parser.add_argument("--path", action="append", help="Repeat to select routes; default covers all 39 finite dashboard routes")
    parser.add_argument("--skip-notification", action="store_true")
    parser.add_argument("--profile-history", type=Path, help="Optional canonical state directory; profile all-minute history locally, no service restart")
    parser.add_argument("--profile-history-reference", type=Path, help="Frozen dashboard module; retains its canonical repository path for imports")
    parser.add_argument("--profile-history-wire", action="store_true", help="Include canonical public JSON/gzip and independent complete readback")
    parser.add_argument("--local-only", action="store_true", help="Run selected local profiles without HTTP requests or notification fixtures")
    parser.add_argument(
        "--profile-history-source-rebuild",
        action="store_true",
        help=(
            "With --profile-history, bypass the final memory/persisted projection; "
            "does not flush OS cache or helper indexes"
        ),
    )
    parser.add_argument(
        "--profile-history-full-source-rebuild",
        action="store_true",
        help=(
            "With --profile-history-source-rebuild, also bypass immutable session "
            "projections to retain an honest complete-scan baseline"
        ),
    )
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--profile-services", action="store_true", help="Read local systemd resource counters before/after HTTP measurements")
    parser.add_argument("--profile-inventory", action="store_true", help="Benchmark all footer fields on one frozen local input")
    parser.add_argument("--profile-inventory-reference", type=Path, help="Optional pre-change inventory module for exact paired replay")
    parser.add_argument("--profile-shioaji-monitor", action="store_true", help="Compare sequential/parallel local monitor I/O (no broker API calls)")
    parser.add_argument("--profile-tw-status-projection", action="store_true",
                        help="ABBA projection/JSON/gzip/full-readback on one captured public TW status")
    parser.add_argument("--profile-tw-status-projection-reference", type=Path)
    parser.add_argument("--profile-tw-status-input", type=Path,
                        help="Replay a prior full status capture instead of reading the local/public status API")
    parser.add_argument("--profile-opening-tail", type=Path,
                        help="State directory: fixed-source complete opening telemetry tail/summary decoder ABBA, without provider requests")
    parser.add_argument("--profile-opening-append", type=Path,
                        help="State directory: private copy + verbatim row append, full native versus prefix-verified refresh ABBA")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output and args.output.exists():
        parser.error("output receipt must be new")
    if (args.profile_history_reference or args.profile_history_wire) and not args.profile_history:
        parser.error("history reference/wire requires --profile-history")
    if args.local_only and not (args.profile_history or args.profile_inventory or args.profile_shioaji_monitor
                               or args.profile_tw_status_projection or args.profile_opening_tail or args.profile_opening_append):
        parser.error("--local-only requires a local profile")
    if (args.profile_tw_status_projection_reference or args.profile_tw_status_input) and not args.profile_tw_status_projection:
        parser.error("status projection reference/input requires --profile-tw-status-projection")
    if args.profile_tw_status_projection and not args.profile_tw_status_input and not args.output:
        parser.error("live status capture requires an explicit new --output receipt")
    if args.profile_inventory_reference and not args.profile_inventory:
        parser.error("--profile-inventory-reference requires --profile-inventory")
    if args.profile_history_source_rebuild and not args.profile_history:
        parser.error("--profile-history-source-rebuild requires --profile-history")
    if args.profile_history_full_source_rebuild and not args.profile_history_source_rebuild:
        parser.error(
            "--profile-history-full-source-rebuild requires "
            "--profile-history-source-rebuild"
        )
    if not 1 <= args.repeats <= 1000:
        parser.error("repeats must be 1..1000")
    if not 1 <= args.concurrency <= 32 or not 0 < args.timeout <= 60:
        parser.error("concurrency must be 1..32 and timeout 0..60")
    paths = args.path or DEFAULT_PATHS
    if any(not p.startswith("/") or p.startswith("//") or "api/updates" in p for p in paths):
        parser.error("HTTP paths must be relative non-streaming routes")
    result = {"schema_version": 2, "measured_at": datetime.now(UTC).isoformat(),
              "base_url": args.base_url, "repeats": args.repeats, "concurrency": args.concurrency,
              "python": platform.python_version(), "platform": platform.platform(),
              "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip(),
              "dirty_files": subprocess.check_output(["git", "status", "--short"], cwd=REPO_ROOT, text=True).splitlines(),
              "code_fingerprints": code_fingerprints(),
              "caveats": ["first_observed is NOT proof of cold server cache; no cache flush or live restart",
                          "TTFB/headers includes server work; do not add Server-Timing again",
                          "body includes decompression; total excludes client JSON parsing",
                          "steady includes each worker's new connection; warm_reuse excludes it (reuse is attempted, not guaranteed)",
                          "requests_per_second includes worker connection setup; it is achieved client throughput, not server capacity",
                          "p95/p99 are nearest-rank estimates; small samples are not SLA evidence",
                          "SSE benchmark uses isolated synthetic receipts, not trading or broker latency"],
              "notification": None if args.skip_notification or args.local_only else capture_measurement(lambda: isolated_notification(args.repeats))}
    if args.profile_history:
        result["local_history"] = capture_measurement(
            lambda: profile_history(
                args.profile_history,
                min(args.repeats, 3),
                source_rebuild=args.profile_history_source_rebuild,
                full_source_rebuild=args.profile_history_full_source_rebuild,
                source_reference=args.profile_history_reference,
                wire=args.profile_history_wire,
            )
        )
    if args.profile_inventory:
        result["local_inventory"] = capture_measurement(
            lambda: profile_inventory(REPO_ROOT, min(args.repeats, 10), args.profile_inventory_reference)
        )
    if args.profile_shioaji_monitor:
        result["local_shioaji_monitor"] = capture_measurement(
            lambda: profile_shioaji_monitor(REPO_ROOT, min(args.repeats, 10))
        )
    if args.profile_tw_status_projection:
        def status_projection():
            from downloader.artifact_io import atomic_write_json
            if args.profile_tw_status_input:
                input_path = args.profile_tw_status_input
                payload = json.loads(input_path.read_bytes())
            else:
                import requests
                input_path = args.output.with_name(args.output.stem + ".status-input.json")
                if input_path.exists() or input_path.is_symlink():
                    raise FileExistsError("status input receipt must be new")
                response = requests.get(args.base_url.rstrip("/") + "/tw-day-trade/api/status", timeout=args.timeout)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("status capture must be a complete JSON object")
                atomic_write_json(input_path, payload)
            report = profile_tw_status_projection(payload, min(args.repeats, 10), args.profile_tw_status_projection_reference)
            report["input_receipt"] = str(input_path)
            report["input_receipt_sha256"] = hashlib.sha256(input_path.read_bytes()).hexdigest()
            return report
        result["local_tw_status_projection"] = capture_measurement(status_projection)
    if args.profile_opening_tail:
        result["local_opening_tail"] = capture_measurement(
            lambda: profile_opening_attempt_tail(args.profile_opening_tail, min(args.repeats, 3))
        )
    if args.profile_opening_append:
        result["local_opening_append"] = capture_measurement(
            lambda: profile_opening_attempt_append(args.profile_opening_append, min(args.repeats, 3)))
    if args.profile_services:
        result["services_before"] = capture_measurement(service_snapshot)
    result["http"] = [] if args.local_only else measure_http(
        args.base_url, paths, repeats=args.repeats,
        concurrency=args.concurrency, timeout=args.timeout)
    if args.profile_services:
        result["services_after"] = capture_measurement(service_snapshot)
        result["services"] = service_deltas(result["services_before"], result["services_after"])
    if args.compare:
        before = json.loads(args.compare.read_text())
        if before.get("schema_version") != 2 or any(before.get(k) != result[k] for k in ("base_url", "repeats", "concurrency")):
            parser.error("comparison requires schema 2 and matching origin/repeats/concurrency")
        old = {r["path"]: r for r in before["http"]}
        result["comparison"] = [{"path": row["path"], "median_delta_ms": round(
            row["steady"]["total_ms"]["median_ms"] - old[row["path"]]["steady"]["total_ms"]["median_ms"], 3),
            "warm_reuse_median_delta_ms": round(
                row["warm_reuse"]["total_ms"]["median_ms"] - timing_summary(warm_samples(old[row["path"]], args.repeats))["total_ms"]["median_ms"], 3)
                if args.repeats > 1 else None}
            for row in result["http"] if row["path"] in old and not row["errors"] and not old[row["path"]]["errors"]]
    output = args.output or Path("artifacts/benchmarks/dashboards") / f"http-{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"receipt={output}", flush=True)
    return int(any(row["errors"] for row in result["http"]) or any(
        (result.get(phase) or {}).get("error") for phase in ("notification", "local_history", "local_inventory", "local_shioaji_monitor", "local_tw_status_projection", "local_opening_tail", "local_opening_append", "services")))


if __name__ == "__main__":
    raise SystemExit(main())
