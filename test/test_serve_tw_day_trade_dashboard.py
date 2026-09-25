from __future__ import annotations

from http import HTTPStatus
import json
import threading
from types import SimpleNamespace

from scripts import serve_tw_day_trade_dashboard as dashboard_server
from scripts.serve_tw_day_trade_dashboard import DashboardHandler, DashboardServer


class _DisconnectedWriter:
    def write(self, _payload: bytes) -> None:
        raise BrokenPipeError("client disconnected")


class _BufferWriter:
    def __init__(self) -> None:
        self.payload = b""

    def write(self, payload: bytes) -> None:
        self.payload += payload


def test_json_response_reports_preparation_and_serialization_timing() -> None:
    handler = object.__new__(DashboardHandler)
    handler.headers = {}
    handler.wfile = _BufferWriter()
    headers: dict[str, str] = {}
    handler.send_response = lambda _status: None
    handler.send_header = lambda name, value: headers.__setitem__(name, value)
    handler.end_headers = lambda: None
    handler._json(HTTPStatus.OK, {"health": "ok"})

    assert handler.wfile.payload == b'{"health":"ok"}\n'
    assert headers["Content-Type"] == "application/json; charset=utf-8"
    assert headers["Server-Timing"].startswith("prepare;dur=")
    assert ", serialize;dur=" in headers["Server-Timing"]
    assert ", compress;dur=" in headers["Server-Timing"]


def test_disconnected_dashboard_client_does_not_raise() -> None:
    handler = object.__new__(DashboardHandler)
    handler.headers = {}
    handler.wfile = _DisconnectedWriter()
    handler.close_connection = False
    handler.send_response = lambda _status: None
    handler.send_header = lambda _name, _value: None
    handler.end_headers = lambda: None

    handler._send(HTTPStatus.OK, b"{}\n", "application/json")

    assert handler.close_connection is True


def test_dashboard_serves_fast_summary_while_large_indexes_warm(
    tmp_path, monkeypatch
) -> None:
    release = threading.Event()
    warm_started = threading.Event()
    observed: list[bool] = []

    def slow_warm(*, state_dir):
        assert state_dir == tmp_path
        warm_started.set()
        assert release.wait(timeout=2.0)
        return {}

    def summary(**kwargs):
        observed.append(bool(kwargs["include_ledger_session_dates"]))
        return {"record_counts_ready": observed[-1]}

    monkeypatch.setattr(dashboard_server, "warm_dashboard_session_indexes", slow_warm)
    monkeypatch.setattr(dashboard_server, "build_dashboard_summary", summary)
    server = DashboardServer(
        ("127.0.0.1", 0),
        state_dir=tmp_path,
        static_root=tmp_path,
        preopen_readiness_path=None,
    )
    try:
        assert warm_started.wait(timeout=1.0)
        assert server.session_indexes_ready.is_set() is False
        assert server.session_index_warm_elapsed_ms is None
        assert server.summary()["record_counts_ready"] is False
        release.set()
        server.session_index_warm_thread.join(timeout=2.0)
        assert server.summary()["record_counts_ready"] is True
        assert server.session_index_warm_elapsed_ms is not None
        assert server.session_index_warm_elapsed_ms >= 0
        assert observed == [False, True]
    finally:
        release.set()
        server.server_close()


def test_dashboard_records_failed_index_warm_duration(tmp_path, monkeypatch) -> None:
    def fail_warm(*, state_dir):
        assert state_dir == tmp_path
        raise OSError("private source path")

    monkeypatch.setattr(dashboard_server, "warm_dashboard_session_indexes", fail_warm)
    server = DashboardServer(
        ("127.0.0.1", 0),
        state_dir=tmp_path,
        static_root=tmp_path,
        preopen_readiness_path=None,
    )
    try:
        server.session_index_warm_thread.join(timeout=2.0)
        assert not server.session_indexes_ready.is_set()
        assert server.session_index_warm_elapsed_ms is not None
        assert "private source path" in server.session_index_warm_error
    finally:
        server.server_close()


def test_dashboard_health_reports_warm_failure_without_private_error(tmp_path) -> None:
    (tmp_path / "status.json").write_text(
        json.dumps({"health": "ready"}), encoding="utf-8"
    )
    handler = object.__new__(DashboardHandler)
    handler.path = "/healthz"
    handler.server = SimpleNamespace(
        state_dir=tmp_path,
        revision=lambda: {"state_revision": 1, "status": "ready"},
        session_indexes_ready=threading.Event(),
        session_index_warm_error="/private/source/path failed",
        session_index_warm_elapsed_ms=123.4567,
    )
    responses: list[tuple[HTTPStatus, object]] = []
    handler._json = lambda status, payload: responses.append((status, payload))
    handler.do_GET()

    assert responses[0][0] == HTTPStatus.OK
    payload = responses[0][1]
    assert payload["session_index_warm_status"] == "failed"
    assert payload["session_index_warm_elapsed_ms"] == 123.457
    assert "/private/source/path" not in json.dumps(payload)
