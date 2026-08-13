#!/usr/bin/env python3
"""Serve the read-only TAIFEX simulation dashboard on localhost."""

from __future__ import annotations

import argparse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
import time
from typing import Final
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.taifex_volatility_dashboard import (  # noqa: E402
    DEFAULT_MARK_LIMIT_PER_STRATEGY,
    build_dashboard_snapshot,
)


DEFAULT_STATE_DIR: Final[Path] = Path(
    "artifacts/live/shioaji_taifex_volatility_simulation"
)
DEFAULT_API_RECEIPTS: Final[Path] = Path("artifacts/orders/shioaji_futures_simulation")
DEFAULT_STATIC_ROOT: Final[Path] = Path("services/taifex_dashboard")
HISTORY_DISPLAY_FIELDS: Final[tuple[str, ...]] = (
    "strategy_id",
    "decision_ts_ns",
    "cumulative_pnl_twd",
    "initial_capital_twd",
    "total_equity_twd",
    "fixed_capital_return",
    "valuation_carried_forward",
)

STATIC_ROUTES: Final[dict[str, tuple[str, str]]] = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        state_dir: Path,
        api_receipt_dir: Path,
        static_root: Path,
        mark_limit_per_strategy: int,
    ) -> None:
        super().__init__(server_address, DashboardRequestHandler)
        self.state_dir = state_dir
        self.api_receipt_dir = api_receipt_dir
        self.static_root = static_root
        self.mark_limit_per_strategy = mark_limit_per_strategy
        self._snapshot_cache: tuple[float, dict[str, object]] | None = None
        self._snapshot_lock = threading.Lock()
        self._history_cache: tuple[float, dict[str, object]] | None = None
        self._history_lock = threading.Lock()

    def snapshot(self) -> dict[str, object]:
        now_monotonic = time.monotonic()
        cached = self._snapshot_cache
        if cached is not None and now_monotonic - cached[0] < 2.0:
            return cached[1]
        # Multiple dashboard tabs refresh on the same five-second boundary.
        # Build one immutable read snapshot and share it instead of stampeding
        # the append-only marks ledger once per connection.
        with self._snapshot_lock:
            now_monotonic = time.monotonic()
            cached = self._snapshot_cache
            if cached is not None and now_monotonic - cached[0] < 2.0:
                return cached[1]
            snapshot = build_dashboard_snapshot(
                state_dir=self.state_dir,
                api_receipt_dir=self.api_receipt_dir,
                mark_limit_per_strategy=self.mark_limit_per_strategy,
                include_history=False,
            )
            self._snapshot_cache = (time.monotonic(), snapshot)
            return snapshot

    def history_snapshot(self) -> dict[str, object]:
        now_monotonic = time.monotonic()
        cached = self._history_cache
        if cached is not None and now_monotonic - cached[0] < 55.0:
            return cached[1]
        with self._history_lock:
            now_monotonic = time.monotonic()
            cached = self._history_cache
            if cached is not None and now_monotonic - cached[0] < 55.0:
                return cached[1]
            full = build_dashboard_snapshot(
                state_dir=self.state_dir,
                api_receipt_dir=self.api_receipt_dir,
                mark_limit_per_strategy=self.mark_limit_per_strategy,
                include_history=True,
            )
            snapshot = {
                "dashboard_schema_version": full["dashboard_schema_version"],
                "generated_at_utc": full["generated_at_utc"],
                "source_updated_at_utc": full["source_updated_at_utc"],
                "history": [
                    {key: row.get(key) for key in HISTORY_DISPLAY_FIELDS}
                    for row in full["history"]
                ],
                "record_counts": full["record_counts"],
            }
            self._history_cache = (time.monotonic(), snapshot)
            return snapshot


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer
    server_version = "StockAgentDashboard/1"
    sys_version = ""

    def _headers(self, *, content_type: str, content_length: int) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(content_length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'",
        )

    def _send_bytes(
        self, status: HTTPStatus, payload: bytes, *, content_type: str
    ) -> None:
        self.send_response(status)
        self._headers(content_type=content_type, content_length=len(payload))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, status: HTTPStatus, payload: object) -> None:
        encoded = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        self._send_bytes(
            status, encoded, content_type="application/json; charset=utf-8"
        )

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path
        if path == "/api/status":
            try:
                self._send_json(HTTPStatus.OK, self.server.snapshot())
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/history":
            try:
                self._send_json(HTTPStatus.OK, self.server.history_snapshot())
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/healthz":
            try:
                snapshot = self.server.snapshot()
                healthy = snapshot.get("health") not in {"blocked", "stale"}
                self._send_json(
                    HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "health": snapshot.get("health"),
                        "source_age_seconds": snapshot.get("source_age_seconds"),
                    },
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        static = STATIC_ROUTES.get(path)
        if static is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        filename, content_type = static
        target = self.server.static_root / filename
        try:
            payload = target.read_bytes()
        except OSError as exc:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            return
        self._send_bytes(HTTPStatus.OK, payload, content_type=content_type)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), format % args)
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--api-receipt-dir", type=Path, default=DEFAULT_API_RECEIPTS)
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT)
    parser.add_argument(
        "--mark-limit-per-strategy",
        type=int,
        default=DEFAULT_MARK_LIMIT_PER_STRATEGY,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= int(args.port) <= 65535:
        raise ValueError("port must be between 1 and 65535")
    if not 1 <= int(args.mark_limit_per_strategy) <= 1_440:
        raise ValueError("mark-limit-per-strategy must be between 1 and 1440")
    server = DashboardHTTPServer(
        (str(args.host), int(args.port)),
        state_dir=Path(args.state_dir),
        api_receipt_dir=Path(args.api_receipt_dir),
        static_root=Path(args.static_root),
        mark_limit_per_strategy=int(args.mark_limit_per_strategy),
    )
    print(
        f"[taifex-dashboard] listening=http://{args.host}:{args.port} "
        f"state_dir={args.state_dir}",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
