from __future__ import annotations

from http import HTTPStatus
import threading

from scripts import serve_tw_day_trade_dashboard as dashboard_server
from scripts.serve_tw_day_trade_dashboard import DashboardHandler, DashboardServer


class _DisconnectedWriter:
    def write(self, _payload: bytes) -> None:
        raise BrokenPipeError("client disconnected")


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
        assert server.summary()["record_counts_ready"] is False
        release.set()
        server.session_index_warm_thread.join(timeout=2.0)
        assert server.summary()["record_counts_ready"] is True
        assert observed == [False, True]
    finally:
        release.set()
        server.server_close()
