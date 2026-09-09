from __future__ import annotations

from http.client import HTTPConnection
import json
from pathlib import Path
import threading

import pytest

from stockagent.live.dashboard_updates import DashboardUpdateHub
from test_public_dashboards import _test_server


def test_update_hub_atomic_replace_capacity_and_close(tmp_path: Path):
    path = tmp_path / "receipt.json"
    hub = DashboardUpdateHub({"tw": (path,)}, max_clients=1)
    try:
        assert hub.acquire("tw")
        assert not hub.acquire("tw")
        version = hub.wait("tw", 0, timeout=1)
        staged = tmp_path / "receipt.tmp"
        staged.write_text('{"revision": 2}')
        staged.replace(path)
        assert hub.wait("tw", version, timeout=2) > version
        hub.release()
        assert hub.acquire("tw")
    finally:
        hub.close()
    assert not hub.acquire("tw")
    assert not hub._thread.is_alive()


def test_update_hub_recovers_directory_created_after_subscription(tmp_path: Path):
    directory = tmp_path / "late"
    hub = DashboardUpdateHub({"tw": (directory / "receipt.json",)})
    try:
        hub.acquire("tw")
        version = hub.wait("tw", 0, timeout=1)
        directory.mkdir()
        (directory / "receipt.json").write_text("{}")
        assert hub.wait("tw", version, timeout=2) > version
    finally:
        hub.close()


def test_sse_flushes_initial_and_committed_view_without_polling():
    server = _test_server()
    server.tw_revision = lambda: server.cached_local_json(
        cache_key="test-revision", ttl_seconds=60, cache_control="no-store",
        builder=lambda: {"revision_token": "2", "state_revision": 2},
    )
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", "/tw-day-trade/api/updates")
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("X-Accel-Buffering") == "no"
        assert response.getheader("Content-Encoding") is None
        assert response.getheader("Content-Length") is None
        assert response.fp.readline() == b"event: revision\n"
        first = json.loads(response.fp.readline().removeprefix(b"data: "))
        assert first["revision_token"] == "2"
        assert response.fp.readline() == b"\n"
        server._store_cached_response("tw-status:latest:2", server.tw_revision(), 60)
        assert response.fp.readline() == b"event: revision\n"
        second = json.loads(response.fp.readline().removeprefix(b"data: "))
        assert second["view_generation"] > first["view_generation"]
        response.close()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@pytest.mark.parametrize("topic", ["tw", "overnight"])
def test_history_cache_invalidates_immediately_on_content_revision(monkeypatch, topic):
    server = _test_server()
    token = {"value": "one"}
    server.content_token = lambda *_: token["value"]
    calls = []

    def build(**kwargs):
        calls.append(kwargs)
        return {"range": "all", "history": [], "generated_at_utc": token["value"], "simulation_only": True, "production_order_possible": False}

    monkeypatch.setattr("scripts.serve_public_dashboards.build_dashboard_history_snapshot", build)
    method = server.tw_history if topic == "tw" else server.overnight_history
    try:
        first = method("all", resolution="1m")
        assert method("all", resolution="1m") is first
        token["value"] = "two"
        assert method("all", resolution="1m") is not first
        assert len(calls) == 2
        assert all(call["resolution"] == "1m" for call in calls)
    finally:
        server.server_close()


def test_revision_file_change_bypasses_even_unexpired_cache(tmp_path):
    server = _test_server()
    server.repo_root = tmp_path
    root = tmp_path / "artifacts/live/tw_day_trade_simulation"
    root.mkdir(parents=True)
    path = root / "status.json"
    server.update_hub.paths["tw"] = (path,)
    try:
        path.write_text(json.dumps({"state_revision": 1, "content_revision": 1}))
        first = json.loads(server.tw_revision().body)
        path.write_text(json.dumps({"state_revision": 2, "content_revision": 2}))
        second = json.loads(server.tw_revision().body)
        assert first["state_revision"] == 1
        assert second["state_revision"] == 2
    finally:
        server.server_close()


def test_signals_are_requested_before_lossless_history_and_client_keys_include_revision():
    source = Path("services/tw_day_trade_dashboard/app.js").read_text()
    start = source.index("async function refresh(")
    end = source.index("function activateTwPublicMonitor", start)
    refresh = source[start:end]
    assert refresh.index("loadSignals({force: true})") < refresh.index("loadChartHistory")
    assert "Promise.allSettled([signalsReady, historyReady])" in refresh
    assert "await loadSignals" not in refresh
    key = source[source.index("function chartRequestKey"):source.index("function chartHistoryMatchesSelection")]
    assert "lastServiceRevision" in key


@pytest.mark.parametrize("method,path,headers,status", [
    ("HEAD", "/tw-day-trade/api/updates", {}, 200),
    ("GET", "/tw-day-trade/api/updates?unexpected=1", {}, 400),
    ("POST", "/tw-day-trade/api/updates", {}, 405),
    ("GET", "/tw-day-trade/api/updates", {"Accept-Encoding": "identity;q=0, gzip;q=1"}, 406),
])
def test_sse_retains_read_only_protocol_boundaries(method, path, headers, status):
    server = _test_server()
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        assert response.status == status
        response.read()
        assert server.update_hub._clients == 0
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def test_sse_capacity_does_not_block_ordinary_api():
    server = _test_server()
    server.update_hub.max_clients = 0
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", "/tw-day-trade/api/updates")
        response = connection.getresponse()
        assert response.status == 503
        assert json.loads(response.read())["error"] == "update_stream_capacity"
        connection.request("GET", "/healthz")
        response = connection.getresponse()
        assert response.status == 200
        response.read()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
