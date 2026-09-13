#!/usr/bin/env python3
"""Read-only HTTP timings plus an isolated atomic-receipt-to-SSE benchmark."""

from __future__ import annotations

import argparse
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, UTC
from http.client import HTTPConnection
import json
from pathlib import Path
import statistics
import math
import platform
import re
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.serve_public_dashboards import PublicDashboardServer  # noqa: E402

DEFAULT_PATHS = (
    "/", "/api/overview", "/taifex/", "/taifex/api/status", "/taifex/api/history?range=1d",
    *tuple(path for prefix in ("tw-day-trade", "tw-overnight") for path in (
        f"/{prefix}/", f"/{prefix}/api/revision", f"/{prefix}/api/status",
        f"/{prefix}/api/signals?limit=100", f"/{prefix}/api/positions?limit=100",
        f"/{prefix}/api/events?limit=100", f"/{prefix}/api/history?range=all&resolution=1m",
        f"/{prefix}/api/public-data-status",
    )),
    "/shioaji/", "/shioaji/api/status", "/openbb/", "/openbb/api/status",
    "/openbb/api/history?range=1d", "/data-monitor/", "/data-monitor/api/summary",
    "/data-monitor/api/status", "/data-monitor/api/details", "/traffic/", "/traffic/api/status",
)


def summarize(values):
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "median_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    return {"n": len(values), "median_ms": round(statistics.median(values), 3),
            "p95_ms": round(ordered[max(0, math.ceil(len(ordered) * .95) - 1)], 3),
            "p99_ms": round(ordered[max(0, math.ceil(len(ordered) * .99) - 1)], 3),
            "max_ms": round(max(values), 3)}


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


def code_fingerprints():
    """Bind measurements to actual dirty-worktree web code, not only Git HEAD."""
    paths = {
        REPO_ROOT / "scripts/serve_public_dashboards.py",
        REPO_ROOT / "scripts/benchmark_dashboard_latency.py",
        REPO_ROOT / "scripts/audit_public_dashboards_browser.mjs",
        REPO_ROOT / "stockagent/live/dashboard_updates.py",
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


def profile_history(state_dir, repeats):
    """Profile the canonical builder in this fresh CLI process, not the service.

    This does not flush OS or persisted indexes and is not a disk-cold test.
    Checksums are outside the measured build and prove equality only if source
    metadata remained stable. Existing canonical cache persistence may occur.
    """
    from stockagent.live.dashboard_updates import file_signature, metadata_signature
    from stockagent.live.tw_day_trade_dashboard import build_dashboard_history_snapshot

    root = state_dir.resolve(strict=True)
    def inventory():
        return {p.name: metadata_signature(p.stat()) for p in sorted(root.iterdir())
                if p.is_file() and p.suffix in {".json", ".jsonl", ".parquet"}}
    before = inventory()
    samples = []
    for _ in range(repeats):
        started = time.perf_counter()
        payload = build_dashboard_history_snapshot(state_dir=root, range_key="all", resolution="1m")
        elapsed = (time.perf_counter() - started) * 1000
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        samples.append({"build_ms": elapsed, "sha256": hashlib.sha256(body).hexdigest(),
                        "decoded_bytes": len(body), "point_count": payload.get("returned_points"),
                        "series_count": len(payload.get("minute_series", []))})
    signature_samples = []
    for name in before:
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
    return {"boundary": "fresh CLI process; OS/persisted cache may be warm; no production cache flush",
            "state_dir": str(root), "source_stable": stable, "samples": samples,
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
    parser.add_argument("--path", action="append", help="Repeat to select routes; default covers all eight pages")
    parser.add_argument("--skip-notification", action="store_true")
    parser.add_argument("--profile-history", type=Path, help="Optional canonical state directory; profile all-minute history locally, no service restart")
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
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
              "notification": None if args.skip_notification else capture_measurement(lambda: isolated_notification(args.repeats))}
    if args.profile_history:
        result["local_history"] = capture_measurement(lambda: profile_history(args.profile_history, min(args.repeats, 3)))
    result["http"] = measure_http(args.base_url, paths, repeats=args.repeats,
                                  concurrency=args.concurrency, timeout=args.timeout)
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
        (result.get(phase) or {}).get("error") for phase in ("notification", "local_history")))


if __name__ == "__main__":
    raise SystemExit(main())
