from __future__ import annotations

from http import HTTPStatus

from scripts.serve_tw_day_trade_dashboard import DashboardHandler


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
