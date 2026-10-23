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


def test_private_signal_route_forwards_zero_target_display_filter() -> None:
    handler = object.__new__(DashboardHandler)
    calls, responses = [], []
    handler.server = SimpleNamespace(
        signal_page=lambda **kwargs: calls.append(kwargs) or {"rows": []},
    )
    handler._json = lambda status, payload: responses.append((status, payload))
    for value in ("1", "0"):
        handler.path = f"/api/signals?hide_zero_targets={value}"
        handler.do_GET()
        assert responses[-1][0] == HTTPStatus.OK
        assert calls[-1]["hide_zero_targets"] is (value == "1")
    handler.path = "/api/signals?hide_zero_targets=true"
    handler.do_GET()
    assert responses[-1][0] == HTTPStatus.BAD_REQUEST


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


def test_browser_mode_acceptance_checks_rendered_cards_not_only_status():
    from scripts.verify_tw_dashboard_date_range import _check_mode_visibility

    observed = {"markets": ["base", "factorized"],
                "labels": ["Base", "Factorized"], "headings": ["Base", "Factorized"]}
    _check_mode_visibility(observed, ["factorized", "base"])
    _check_mode_visibility(observed, [])


def test_browser_mode_acceptance_rejects_hidden_or_mislabelled_mode():
    import pytest
    from scripts.verify_tw_dashboard_date_range import _check_mode_visibility

    for markets, labels, headings in [
        (["base"], ["Base"], ["Base"]),
        (["base", "base"], ["Base", "Base"], ["Base", "Base"]),
        (["base", "factorized"], ["Base", "Factorized"], ["Base"]),
        (["base", "factorized"], ["Base", "Factorized"], ["Base", "Wrong model"]),
    ]:
        with pytest.raises(ValueError):
            _check_mode_visibility({"markets": markets, "labels": labels, "headings": headings},
                                   ["base", "factorized"])


def test_browser_counterfactual_disclosure_requires_rendered_provenance():
    from scripts.verify_tw_dashboard_date_range import _check_counterfactual_disclosure

    observed = {"rows": [{"session_date": "2026-01-02", "counterfactual_open_replay": True,
                          "replay_signal_origin": "current_model_counterfactual"}],
                "rendered_clocks": ["01-02 09:01:00 · 目前模型反事實回放（非當時訊號）"]}
    _check_counterfactual_disclosure(observed, "2026-01-02")


def test_browser_counterfactual_disclosure_rejects_missing_wrong_or_backdated_rows():
    import pytest
    from scripts.verify_tw_dashboard_date_range import _check_counterfactual_disclosure

    row = {"session_date": "2026-01-02", "counterfactual_open_replay": True,
           "replay_signal_origin": "current_model_counterfactual"}
    label = "01-02 09:01:00 · 目前模型反事實回放（非當時訊號）"
    defects = [
        {"rows": [], "rendered_clocks": []},
        {"rows": [row], "rendered_clocks": []},
        {"rows": [{**row, "session_date": "2026-01-05"}], "rendered_clocks": [label]},
        {"rows": [{**row, "counterfactual_open_replay": False}], "rendered_clocks": [label]},
        {"rows": [{**row, "replay_signal_origin": "retained-original"}], "rendered_clocks": [label]},
        {"rows": [{**row, "has_private_signal_id": True}], "rendered_clocks": [label]},
        {"rows": [row], "rendered_clocks": ["01-02 09:01:00 · 開盤價重建；原始訊號 01-02 09:01:00"]},
        {"rows": [row], "rendered_clocks": [label + "；原始訊號 01-02 09:01:00"]},
    ]
    for observed in defects:
        with pytest.raises(ValueError):
            _check_counterfactual_disclosure(observed, "2026-01-02")
