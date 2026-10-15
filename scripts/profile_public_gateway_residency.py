"""Attribute canonical gateway prewarm memory without replacing its live owner.

One isolated loopback server runs the existing complete prewarm (not another
collector or inference service). Source data, broker/model and live services
are never modified. Existing derived-view indexes may be reused/materialized
by their normal builders. Telemetry goes only to the requested private receipt.
Optional allocator reclamation is a one-off diagnostic, not runtime policy.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json

ROLE_ENV = frozenset({"MALLOC_ARENA_MAX", "KMP_AFFINITY", "OMP_NUM_THREADS",
                      "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
                      "POLARS_MAX_THREADS", "ARROW_DEFAULT_MEMORY_POOL",
                      "_RJEM_MALLOC_CONF",
                      "STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"})


def owner_environment():
    response = subprocess.run(["systemctl", "show", "stockagent-public-dashboards.service",
                               "-p", "MainPID", "-p", "InvocationID", "-p", "ActiveState"],
                              check=True, capture_output=True, text=True, timeout=5)
    identity = dict(line.split("=", 1) for line in response.stdout.splitlines() if "=" in line)
    if identity.get("ActiveState") != "active" or int(identity.get("MainPID", 0)) <= 0:
        raise ValueError("formal gateway owner not active")
    pid = int(identity["MainPID"])
    if b"scripts/serve_public_dashboards.py" not in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0"):
        raise ValueError("unexpected gateway process")
    if (Path(f"/proc/{pid}/cwd").resolve() != ROOT.resolve()
            or Path(f"/proc/{pid}/exe").resolve() != Path(sys.executable).resolve()):
        raise ValueError("unexpected gateway checkout or canonical Python runtime")
    values = (row.split(b"=", 1) for row in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in row)
    return identity, {key.decode(): value.decode() for key, value in values if key.decode() in ROLE_ENV}


def memory():
    result = {"rss_bytes": int(Path("/proc/self/statm").read_text().split()[1])
              * os.sysconf("SC_PAGE_SIZE")}
    arrow = sys.modules.get("pyarrow")
    if arrow is not None:
        pool = arrow.default_memory_pool()
        result.update(arrow_backend=pool.backend_name,
                      arrow_live_bytes=pool.bytes_allocated(),
                      arrow_peak_bytes=pool.max_memory(),
                      arrow_total_allocated_bytes=pool.total_bytes_allocated())
    return result


def signature(paths):
    result = {}
    for path in paths:
        try:
            stat = path.stat()
            result[str(path)] = [stat.st_dev, stat.st_ino, stat.st_size,
                                stat.st_mtime_ns, stat.st_ctime_ns]
        except OSError:
            result[str(path)] = None
    return result


def profile(output, *, release_arrow=False, trace_python=False):
    if output.exists():
        raise FileExistsError("do not overwrite previous diagnostic evidence")
    began, cpu = time.perf_counter(), time.process_time()
    before_import = memory()
    from scripts.serve_public_dashboards import PublicDashboardServer

    after_import = memory()
    if trace_python:
        import tracemalloc
        tracemalloc.start(4)
    server = PublicDashboardServer(("127.0.0.1", 0), repo_root=ROOT,
        **{f"{name}_static_root": ROOT / "services" / directory for name, directory in (
            ("public", "public_dashboards"), ("taifex", "taifex_dashboard"),
            ("tw", "tw_day_trade_dashboard"), ("shioaji", "shioaji_api_dashboard"),
            ("openbb", "openbb_archive_dashboard"), ("data_monitor", "data_monitor_dashboard"),
            ("traffic", "traffic_dashboard"))},
        taifex_upstream="http://127.0.0.1:8765", tw_upstream="http://127.0.0.1:8766",
        traffic_history_root=None)
    phases, loads = [], []
    source_before = signature(server.public_quote_source.paths)

    def snapshot():
        return {**memory(), "response_cache_bytes": server._cache_bytes,
                "response_cache_entries": len(server._cache),
                "quote_cache_sessions": len(server.public_quote_source._sessions),
                "quote_cache_rows": sum(len(rows) for rows in server.public_quote_source._sessions.values())}

    load = server.public_quote_source._load

    def observed_load(session):
        initial, started, consumed = memory(), time.perf_counter(), time.process_time()
        rows = load(session)
        loads.append({"session": session, "rows": len(rows), "before": initial,
                      "after": memory(), "wall_seconds": time.perf_counter()-started,
                      "cpu_seconds": time.process_time()-consumed})
        return rows

    server.public_quote_source._load = observed_load

    def wrap(name):
        original = getattr(server, name)

        def observed(*args, **kwargs):
            initial, started, consumed = snapshot(), time.perf_counter(), time.process_time()
            value = original(*args, **kwargs)
            body = getattr(value, "body", None)
            phases.append({"phase": name, "before": initial, "after": snapshot(),
                           "wall_seconds": time.perf_counter()-started,
                           "cpu_seconds": time.process_time()-consumed,
                           "response_bytes": len(body) if isinstance(body, bytes) else None,
                           "response_sha256": hashlib.sha256(body).hexdigest()
                           if isinstance(body, bytes) else None})
            print(json.dumps({"phase": name, "rss_bytes": phases[-1]["after"]["rss_bytes"],
                              "wall_seconds": phases[-1]["wall_seconds"]}), flush=True)
            return value

        setattr(server, name, observed)

    for name in ("prewarm_overview", "tw_status", "tw_history", "cached_json", "openbb_history"):
        wrap(name)
    try:
        server.prewarm_default_views()
        prewarm_wall, prewarm_cpu = time.perf_counter()-began, time.process_time()-cpu
        after_prewarm = snapshot()
        allocations = None
        if trace_python:
            traced_live, traced_peak = tracemalloc.get_traced_memory()
            captured = tracemalloc.take_snapshot()
            # Aggregation must not trace its own millions of temporary objects.
            # A three-frame stack stops inside json.loads, before its caller.
            tracemalloc.stop()
            allocations = {"live_bytes": traced_live, "peak_bytes": traced_peak,
                "top_live_sites": [{"file": str(site.traceback[0].filename),
                                    "line": site.traceback[0].lineno,
                                    "bytes": site.size, "objects": site.count}
                                   for site in captured.statistics("lineno")[:25]],
                "top_live_callers": [{"frames": [{"file": frame.filename, "line": frame.lineno}
                                                  for frame in site.traceback],
                                      "bytes": site.size, "objects": site.count}
                                     for site in captured.statistics("traceback")[:15]]}
            del captured
        gc.collect()
        after_gc = snapshot()
        idle_start = time.perf_counter()
        time.sleep(3)
        after_idle = snapshot()
        idle_seconds = time.perf_counter()-idle_start
        release = None
        if release_arrow and "pyarrow" in sys.modules:
            start, used = time.perf_counter(), time.process_time()
            sys.modules["pyarrow"].default_memory_pool().release_unused()
            release = {"wall_seconds": time.perf_counter()-start,
                       "cpu_seconds": time.process_time()-used, "after": snapshot()}
        warm = server.tw_status()
        after_warm = snapshot()
        result = {"contract": "public_gateway_complete_prewarm_residency_diagnostic_v1",
                  "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                  "before_import": before_import, "after_import": after_import,
                  "after_prewarm": after_prewarm, "after_gc": after_gc,
                  "after_idle": after_idle, "after_warm_status": after_warm,
                  "prewarm_wall_seconds": prewarm_wall, "prewarm_cpu_seconds": prewarm_cpu,
                  "idle_seconds": idle_seconds,
                  "arrow_release_diagnostic": release,
                  "python_allocation_diagnostic": allocations,
                  "phases": phases, "official_quote_loads": loads,
                  "official_source_before": source_before,
                  "official_source_after": signature(server.public_quote_source.paths),
                  "warm_response_bytes": len(warm.body),
                  "no_explicit_model_or_broker_calls": True,
                  "torch_imported": "torch" in sys.modules,
                  "cuda_initialized": bool("torch" in sys.modules and sys.modules["torch"].cuda.is_initialized()),
                  "live_service_restarts": 0, "source_writes": False,
                  "role_environment": {key: os.environ[key] for key in ROLE_ENV if key in os.environ},
                  "matched_formal_owner": json.loads(os.environ.get("STOCKAGENT_PROFILE_MATCHED_OWNER", "null")),
                  "boundary": "live local snapshots and upstreams may change; diagnostic is not frozen parity or speed acceptance"}
        atomic_write_json(output, result)
        return result
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-arrow", action="store_true")
    parser.add_argument("--trace-python", action="store_true",
                        help="four-frame Python allocation attribution; timings are diagnostic, not speed benchmarks")
    parser.add_argument("--match-owner", action="store_true",
                        help="use only the running gateway numerical/index-cache environment, never credentials")
    args = parser.parse_args()
    if args.match_owner and not os.environ.get("STOCKAGENT_PROFILE_MATCHED_OWNER"):
        owner, role = owner_environment()
        env = {key: value for key, value in os.environ.items() if key not in ROLE_ENV}
        env.update(role, STOCKAGENT_PROFILE_MATCHED_OWNER=json.dumps(owner))
        os.execvpe(sys.executable, [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]], env)
    result = profile(args.output, release_arrow=args.release_arrow, trace_python=args.trace_python)
    print(json.dumps({key: result[key] for key in (
        "after_import", "after_prewarm", "after_idle", "arrow_release_diagnostic",
        "after_warm_status", "prewarm_wall_seconds", "prewarm_cpu_seconds")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
