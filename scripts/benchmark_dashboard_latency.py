#!/usr/bin/env python3
"""Read-only HTTP timings plus an isolated atomic-receipt-to-SSE benchmark."""

from __future__ import annotations

import argparse
from http.client import HTTPConnection
import json
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.serve_public_dashboards import PublicDashboardServer  # noqa: E402


def summarize(values):
    ordered = sorted(values)
    return {"n": len(values), "median_ms": round(statistics.median(values), 3),
            "p95_ms": round(ordered[min(len(ordered) - 1, int(len(ordered) * .95))], 3),
            "max_ms": round(max(values), 3)}


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
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 1000:
        parser.error("repeats must be 1..1000")
    import requests
    result = {"notification": isolated_notification(args.repeats), "http": []}
    session = requests.Session()
    for path in ("/", "/tw-day-trade/api/revision", "/tw-day-trade/api/status",
                 "/tw-day-trade/api/signals?limit=100", "/tw-day-trade/api/history?range=all&resolution=1m",
                 "/taifex/api/status", "/api/overview"):
        values = []
        for _ in range(5):
            started = time.perf_counter()
            response = session.get(args.base_url.rstrip("/") + path, timeout=60)
            response.raise_for_status()
            body = response.content
            values.append((time.perf_counter() - started) * 1000)
        row = {"path": path, "first_ms": round(values[0], 3), "decoded_bytes": len(body), "encoding": response.headers.get("Content-Encoding"), **summarize(values[1:])}
        result["http"].append(row)
        print(json.dumps(row), flush=True)
    print(json.dumps(result["notification"]), flush=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
