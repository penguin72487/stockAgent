"""Bounded offline A/B/B/A: HTTP -> canonical candles -> atomic Parquet -> SHA.

The local server adds a labelled per-connection setup cost, NOT a claim about
Internet TLS latency or all-provider throughput. No provider API is contacted.
Live acceptance must come from the canonical collectors' source receipts.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import resource
import socket
import statistics
import sys
import tempfile
import threading
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.common import SharedRateLimiter
from downloader.download_binance_perp_15m import (
    _normalize_candles, _read_parquet, _validate_candles, _write_parquet_atomic,
)
from downloader.http_pool import PooledUrlOpener
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport


def run_benchmark(output: Path, *, pages: int = 80, workers: int = 8,
                  setup_seconds: float = 0.005, interval_seconds: float = 0.001) -> dict:
    if pages < 1 or pages > 1000 or workers < 1 or workers > 64:
        raise ValueError("bounded benchmark requires 1..1000 pages and 1..64 workers")
    if setup_seconds < 0 or interval_seconds < 0:
        raise ValueError("benchmark delays must be non-negative")
    output.mkdir(parents=True, exist_ok=True)
    page_rows = 499
    base_ms = 1_700_000_040_000
    payloads = [json.dumps([
        [base_ms + (page * page_rows + row) * 60_000, "100", "102", "99", "101",
         "12.5", base_ms + (page * page_rows + row) * 60_000 + 59_999,
         "1262.5", 42, "7.0", "707.0", "0"]
        for row in range(page_rows)
    ], separators=(",", ":")).encode() for page in range(pages)]

    class Server(ThreadingHTTPServer):
        daemon_threads = True
        connections = 0

        def get_request(self):
            connection, address = super().get_request()
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.connections += 1
            # Synthetic setup penalty, identical for old/new transport.
            time.sleep(setup_seconds)
            return connection, address

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            body = payloads[int(self.path.rsplit("/", 1)[1])]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    trials = []
    expected_sha = None
    try:
        for index, mode in enumerate(("stdlib", "pooled", "pooled", "stdlib")):
            pool = PooledUrlOpener(connections_per_origin=workers)
            start_connections = server.connections
            usage = resource.getrusage(resource.RUSAGE_SELF)
            cpu_before = usage.ru_utime + usage.ru_stime
            started = time.perf_counter()
            try:
                with tempfile.TemporaryDirectory(prefix="http-benchmark-", dir=output) as scratch:
                    limiter = SharedRateLimiter(interval_seconds, name="loopback-only", state_dir=scratch,
                                                idle_timeout_seconds=0.01)
                    transport = ResilientHttpTransport(
                        HttpRequestPolicy(provider="loopback-only", max_retries=0),
                        limiter=limiter, opener=pool if mode == "pooled" else urlopen,
                    )

                    def fetch(page):
                        response = transport.request_bytes(f"http://127.0.0.1:{server.server_port}/{page}")
                        if response.body != payloads[page]:
                            raise AssertionError("HTTP bytes changed")
                        return json.loads(response.body)

                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        chunks = list(executor.map(fetch, range(pages)))
                    frame = _normalize_candles([row for chunk in chunks for row in chunk])
                    _validate_candles(frame)
                    if frame.height != pages * page_rows:
                        raise AssertionError("normalized rows changed")
                    path = output / f"trial-{index}-{mode}.parquet"
                    _write_parquet_atomic(frame, path)
                    readback = _read_parquet(path)
                    if not frame.equals(readback):
                        raise AssertionError("independent Parquet readback changed")
                    sha = hashlib.sha256(path.read_bytes()).hexdigest()
                    if expected_sha is not None and sha != expected_sha:
                        raise AssertionError("A/B source/Parquet values differ")
                    expected_sha = sha
                    usage = resource.getrusage(resource.RUSAGE_SELF)
                    trials.append({
                        "mode": mode, "wall_seconds": time.perf_counter() - started,
                        "cpu_seconds": usage.ru_utime + usage.ru_stime - cpu_before,
                        "connections": server.connections - start_connections,
                        "http_attempts": limiter.grant_activity()["grants_total"],
                        "rows": readback.height, "parquet_bytes": path.stat().st_size,
                        "sha256": sha,
                    })
            finally:
                pool.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    medians = {mode: statistics.median(t["wall_seconds"] for t in trials if t["mode"] == mode)
               for mode in ("stdlib", "pooled")}
    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "offline labelled setup penalty; HTTP, normalize/validate, atomic publish, full SHA and independent readback",
        "internet_speed_claim": False, "all_provider_acceptance": False,
        "settings": {"pages": pages, "workers": workers, "rows_per_page": page_rows,
                     "connection_setup_seconds": setup_seconds, "request_interval_seconds": interval_seconds},
        "trials": trials, "median_seconds": medians,
        "fixture_workflow_speedup": medians["stdlib"] / medians["pooled"],
        "byte_and_value_parity": True,
    }
    atomic_write_json(output / "result.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pages", type=int, default=80)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--setup-seconds", type=float, default=0.005)
    parser.add_argument("--interval-seconds", type=float, default=0.001)
    args = parser.parse_args()
    result = run_benchmark(args.output_dir, pages=args.pages, workers=args.workers,
                           setup_seconds=args.setup_seconds, interval_seconds=args.interval_seconds)
    print(json.dumps({key: result[key] for key in
                      ("median_seconds", "fixture_workflow_speedup", "byte_and_value_parity")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
