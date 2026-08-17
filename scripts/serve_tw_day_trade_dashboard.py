#!/usr/bin/env python3
"""Serve the read-only Taiwan stock day-trade simulation dashboard."""

from __future__ import annotations

import argparse
from datetime import date as datetime_date
import gzip
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.tw_day_trade_dashboard import (  # noqa: E402
    build_dashboard_event_page,
    build_dashboard_history_snapshot,
    build_dashboard_position_page,
    build_dashboard_signal_page,
    build_dashboard_snapshot,
    build_dashboard_summary,
)


STATIC_ROUTES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}


def _session_date_query(raw_query: str) -> str | None:
    query = parse_qs(
        raw_query,
        keep_blank_values=True,
        strict_parsing=False,
        max_num_fields=1,
    )
    if set(query) - {"date"} or any(len(values) != 1 for values in query.values()):
        raise ValueError("unsupported or repeated query field")
    value = str(query.get("date", [""])[0]).strip()
    return value or None


def _history_query(raw_query: str) -> dict[str, str | None]:
    query = parse_qs(
        raw_query,
        keep_blank_values=True,
        strict_parsing=False,
        max_num_fields=3,
    )
    if set(query) - {"range", "start_date", "end_date"} or any(
        len(values) != 1 for values in query.values()
    ):
        raise ValueError("unsupported or repeated query field")
    range_key = str(query.get("range", ["1d"])[0]).strip() or "1d"
    start_date = str(query.get("start_date", [""])[0]).strip() or None
    end_date = str(query.get("end_date", [""])[0]).strip() or None
    if start_date is not None:
        datetime_date.fromisoformat(start_date)
    if end_date is not None:
        datetime_date.fromisoformat(end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("history start_date must not be after end_date")
    return {"range_key": range_key, "start_date": start_date, "end_date": end_date}


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        state_dir: Path,
        static_root: Path,
        preopen_readiness_path: Path | None,
    ) -> None:
        super().__init__(address, DashboardHandler)
        self.state_dir = Path(state_dir)
        self.static_root = Path(static_root)
        self.preopen_readiness_path = (
            None if preopen_readiness_path is None else Path(preopen_readiness_path)
        )

    def snapshot(self, *, session_date: str | None = None) -> dict[str, object]:
        return build_dashboard_snapshot(
            state_dir=self.state_dir,
            preopen_readiness_path=self.preopen_readiness_path,
            session_date=session_date,
        )

    def signal_page(self, **kwargs: object) -> dict[str, object]:
        return build_dashboard_signal_page(state_dir=self.state_dir, **kwargs)

    def position_page(self, **kwargs: object) -> dict[str, object]:
        return build_dashboard_position_page(state_dir=self.state_dir, **kwargs)

    def event_page(self, **kwargs: object) -> dict[str, object]:
        return build_dashboard_event_page(state_dir=self.state_dir, **kwargs)

    def history(
        self,
        *,
        range_key: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, object]:
        return build_dashboard_history_snapshot(
            state_dir=self.state_dir,
            range_key=range_key,
            start_date=start_date,
            end_date=end_date,
        )

    def summary(self, *, session_date: str | None = None) -> dict[str, object]:
        return build_dashboard_summary(
            state_dir=self.state_dir,
            preopen_readiness_path=self.preopen_readiness_path,
            session_date=session_date,
        )


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardServer
    server_version = "StockAgentDayTradeDashboard/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def _send(
        self,
        status: HTTPStatus,
        payload: bytes,
        content_type: str,
        *,
        cache_control: str = "no-store",
    ) -> None:
        accepts_gzip = "gzip" in str(self.headers.get("Accept-Encoding") or "").lower()
        if accepts_gzip and len(payload) >= 1_024:
            payload = gzip.compress(payload, compresslevel=5)
            self.send_response(status)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
        else:
            self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", cache_control)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'",
        )
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: HTTPStatus, payload: object) -> None:
        encoded = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        self._send(status, encoded, "application/json; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/status":
            try:
                self._json(
                    HTTPStatus.OK,
                    self.server.snapshot(
                        session_date=_session_date_query(parsed.query)
                    ),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/summary":
            try:
                self._json(
                    HTTPStatus.OK,
                    self.server.summary(
                        session_date=_session_date_query(parsed.query)
                    ),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/history":
            try:
                history_query = _history_query(parsed.query)
                self._json(
                    HTTPStatus.OK,
                    self.server.history(**history_query),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/signals":
            try:
                query = parse_qs(parsed.query)
                self._json(
                    HTTPStatus.OK,
                    self.server.signal_page(
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", ["250"])[0]),
                    ),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/positions":
            try:
                query = parse_qs(parsed.query)
                self._json(
                    HTTPStatus.OK,
                    self.server.position_page(
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        status=str(query.get("status", ["all"])[0]),
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", ["250"])[0]),
                    ),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/api/events":
            try:
                query = parse_qs(parsed.query)
                self._json(
                    HTTPStatus.OK,
                    self.server.event_page(
                        mode=str(query.get("mode", [""])[0]),
                        symbol=str(query.get("symbol", [""])[0]),
                        session_date=str(query.get("date", [""])[0]) or None,
                        start_date=str(query.get("start_date", [""])[0]) or None,
                        end_date=str(query.get("end_date", [""])[0]) or None,
                        offset=int(query.get("offset", ["0"])[0]),
                        limit=int(query.get("limit", ["250"])[0]),
                    ),
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        if path == "/healthz":
            try:
                snapshot = self.server.snapshot()
                healthy = snapshot.get("health") not in {"critical", "stale"}
                self._json(
                    HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "health": snapshot.get("health"),
                        "source_age_seconds": snapshot.get("source_age_seconds"),
                    },
                )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"health": "unavailable", "error": f"{type(exc).__name__}: {exc}"},
                )
            return
        static = STATIC_ROUTES.get(path)
        if static is None:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        filename, content_type = static
        try:
            payload = (self.server.static_root / filename).read_bytes()
        except OSError as exc:
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            return
        self._send(
            HTTPStatus.OK,
            payload,
            content_type,
            cache_control="public, max-age=60",
        )

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), format % args)
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument(
        "--state-dir", type=Path, default=Path("artifacts/live/tw_day_trade_simulation")
    )
    parser.add_argument(
        "--static-root", type=Path, default=Path("services/tw_day_trade_dashboard")
    )
    parser.add_argument(
        "--preopen-readiness-path",
        type=Path,
        default=Path("artifacts/discord_bot/preopen_readiness.json"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= int(args.port) <= 65535:
        raise ValueError("port must be between 1 and 65535")
    server = DashboardServer(
        (str(args.host), int(args.port)),
        state_dir=Path(args.state_dir),
        static_root=Path(args.static_root),
        preopen_readiness_path=Path(args.preopen_readiness_path),
    )
    print(
        f"[tw-day-trade-dashboard] listening=http://{args.host}:{args.port} "
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
