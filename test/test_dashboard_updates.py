from __future__ import annotations

from http.client import HTTPConnection
import json
from pathlib import Path
import threading
import zlib

import pytest

from stockagent.live.dashboard_updates import DashboardUpdateHub
from test_public_dashboards import _test_server


@pytest.mark.parametrize("topic", ["tw", "overnight"])
def test_server_watches_post_fsync_ack_so_early_marker_notice_cannot_stall_push(topic):
    from stockagent.live.tw_day_trade_service_sync import SERVICE_SYNC_DURABLE_ACK_FILENAME
    server = _test_server()
    try:
        directory = "tw_day_trade_simulation" if topic == "tw" else "tw_overnight_simulation"
        expected = server.repo_root / "artifacts/live" / directory / SERVICE_SYNC_DURABLE_ACK_FILENAME
        assert expected in server.update_hub.paths[topic]
    finally:
        server.server_close()


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


def test_large_append_only_source_uses_metadata_invalidation_without_hashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "marks.jsonl"
    path.write_text("first\n", encoding="utf-8")
    hub = DashboardUpdateHub(
        {"tw": (path,)}, metadata_only_paths={path},
    )
    monkeypatch.setattr(
        "stockagent.live.dashboard_updates.file_signature",
        lambda _path: pytest.fail("large append-only file must not be hashed"),
    )
    before = hub.signature(path)
    with path.open("a", encoding="utf-8") as stream:
        stream.write("second\n")
    assert hub.signature(path) != before
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


@pytest.mark.parametrize("topic", ["tw-day-trade", "tw-overnight"])
@pytest.mark.parametrize("matching", [True, False])
def test_sse_status_is_the_same_scoped_verified_body_and_never_stale(topic, matching):
    server = _test_server(audience="public")
    sync = {"revision_token": "current", "state_revision": 2,
            "content_revision": 2, "engine_generation": "engine"}
    server.tw_revision = server.overnight_revision = lambda: server.cached_local_json(
        cache_key="test-push-revision", ttl_seconds=60, cache_control="no-store", builder=lambda: sync)
    body = {"modes": [{"market": "fixture"}], "simulation_only": True,
            "production_order_possible": False,
            "service_sync": sync | {"revision_token": "current" if matching else "old"}}
    def status():
        response = server.cached_local_json(cache_key="test-push-status", ttl_seconds=60,
            cache_control="no-store", builder=lambda: body)
        if not matching:
            # End after the discarded stale status instead of waiting for the
            # real heartbeat interval. No invented successful status is sent.
            server.update_hub.close()
        return response
    server.tw_status = server.overnight_status = status
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", f"/{topic}/api/updates?status=1")
        response = connection.getresponse()
        assert response.status == 200
        assert response.fp.readline() == b"event: revision\n"
        response.fp.readline()
        assert response.fp.readline() == b"\n"
        if matching:
            assert response.fp.readline() == b"event: status\n"
            pushed = json.loads(response.fp.readline().removeprefix(b"data: "))
            assert pushed == body
        else:
            assert response.fp.readline() == b""  # no unverified old payload
        response.close()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def test_sse_status_race_recovers_on_the_same_connection_and_deduplicates_heartbeats():
    from stockagent.live.tw_day_trade_dashboard import DashboardSnapshotChanged
    server = _test_server()
    sync = {"revision_token": "current", "engine_generation": "engine", "content_revision": 2,
            "state_revision": 2}
    server.tw_revision = lambda: server.cached_local_json(cache_key=f"push-race-revision:{sync}",
        ttl_seconds=60, cache_control="no-store", builder=lambda: sync)
    calls = []

    def status():
        calls.append(None)
        if len(calls) == 1:
            server.update_hub.publish("tw")
            raise DashboardSnapshotChanged("append not yet committed")
        if len(calls) == 3:
            server.update_hub.close()
        return server.cached_local_json(cache_key=f"push-race-status:{sync}", ttl_seconds=60,
            cache_control="no-store", builder=lambda: {"service_sync": sync, "modes": []})

    server.tw_status = status
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", "/tw-day-trade/api/updates?status=1")
        response = connection.getresponse()

        def event(expected):
            assert response.fp.readline() == b"event: " + expected + b"\n"
            data = []
            while (line := response.fp.readline()) != b"\n":
                assert line.startswith(b"data: ")
                data.append(line.removeprefix(b"data: ").removesuffix(b"\n"))
            return json.loads(b"\n".join(data))

        for expected in (b"revision", b"revision", b"status"):
            event(expected)
        sync["state_revision"] = 3  # a fresh heartbeat would change body bytes
        server.update_hub.publish("tw")
        # Cache publication can itself enqueue a ready hint. Do not assume a
        # one-to-one mapping between explicit publish() and socket frames.
        # Every intervening frame must still be a small revision, not status.
        for _ in range(8):
            if event(b"revision")["state_revision"] == 3:
                break
        else:
            pytest.fail("heartbeat revision did not reach the same stream")
        sync.update(revision_token="next", content_revision=3, state_revision=4)
        server.update_hub.publish("tw")
        for _ in range(8):
            if event(b"revision")["revision_token"] == "next":
                break
        else:
            pytest.fail("material revision did not reach the same stream")
        assert event(b"status")["service_sync"]["revision_token"] == "next"
        assert response.fp.readline() == b""
        assert len(calls) == 3
        response.close()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@pytest.mark.parametrize("topic", ["tw-day-trade", "tw-overnight"])
@pytest.mark.parametrize("accept,encoding", [
    ("gzip", "gzip"), ("gzip;q=0", None), ("identity;q=0, gzip;q=1", "gzip"),
])
def test_status_stream_encoding_flushes_revision_before_builder_and_preserves_all_bytes(topic, accept, encoding):
    server = _test_server()
    sync = {"revision_token": "current", "engine_generation": "engine", "content_revision": 2}
    server.tw_revision = server.overnight_revision = lambda: server.cached_local_json(
        cache_key="gzip-revision", ttl_seconds=60, cache_control="no-store", builder=lambda: sync)
    body = {"service_sync": sync, "modes": [{"label": "逐版測試" * 4096}]}
    entered, release = threading.Event(), threading.Event()

    def status():
        entered.set()
        assert release.wait(5), "first revision must be readable before status is ready"
        server.update_hub.close()
        return server.cached_local_json(cache_key="gzip-status", ttl_seconds=60,
            cache_control="no-store", builder=lambda: body)

    server.tw_status = server.overnight_status = status
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
    try:
        connection.request("GET", f"/{topic}/api/updates?status=1", headers={"Accept-Encoding": accept})
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Encoding") == encoding
        decoder = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS) if encoding else None
        decoded, wire = b"", b""
        while b"\n\n" not in decoded:
            part = response.read1(1024)
            assert part
            wire += part
            decoded += decoder.decompress(part) if decoder else part
        assert b"event: revision\n" in decoded and b"event: status\n" not in decoded
        assert entered.wait(1)
        release.set()
        tail = response.read()
        wire += tail
        decoded += decoder.decompress(tail) if decoder else tail
        if decoder:
            assert decoder.eof  # complete valid gzip, not merely a prefix
            assert len(wire) < len(decoded) / 4
        status_frame = decoded.split(b"event: status\n", 1)[1].split(b"\n\n", 1)[0]
        pushed = json.loads(b"\n".join(line[6:] for line in status_frame.splitlines()
                                      if line.startswith(b"data: ")))
        assert pushed == body
    finally:
        release.set()
        connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@pytest.mark.parametrize("topic", ["tw", "overnight"])
@pytest.mark.parametrize("body", [b"{", b"null", b"[]", b'{"schema_version":2}',
                                   b'{"schema_version":"broken"}'])
def test_corrupt_paper_marker_cannot_downgrade_a_financial_read_to_legacy(tmp_path, topic, body):
    from stockagent.live.tw_day_trade_service_sync import PaperReadChanged
    server = _test_server()
    server.repo_root = tmp_path
    directory = "tw_day_trade_simulation" if topic == "tw" else "tw_overnight_simulation"
    root = tmp_path / "artifacts/live" / directory
    root.mkdir(parents=True)
    (root / "service_sync.json").write_bytes(body)
    server.content_token = lambda *_args, **_kwargs: "source"
    calls = []
    try:
        with pytest.raises(PaperReadChanged):
            server.cached_paper_json(topic, source_token="source", cache_key="corrupt:source",
                ttl_seconds=60, cache_control="no-store", builder=lambda: calls.append(True))
        assert not calls
        assert "corrupt:source" not in server._cache
    finally:
        server.server_close()


@pytest.mark.parametrize("topic", ["tw", "overnight"])
@pytest.mark.parametrize("when", ["before", "during"])
def test_all_paper_views_reject_uncommitted_projection_without_caching(tmp_path, monkeypatch, topic, when):
    from stockagent.live.tw_day_trade_service_sync import PaperReadChanged
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    server = _test_server()
    server.repo_root = tmp_path
    directory = "tw_day_trade_simulation" if topic == "tw" else "tw_overnight_simulation"
    engine = TwDayTradeSimulationEngine(tmp_path / "artifacts/live" / directory)
    engine._persist()
    server.content_token = lambda *_args, **_kwargs: "source"
    built = []

    def uncommitted():
        with engine.state_path.open("ab") as handle:
            handle.write(b"\n")  # valid JSON, unchanged journals, different state generation

    def build():
        built.append(True)
        if when == "during":
            uncommitted()
        return {"rows": [{"verified": True}]}

    if when == "before":
        uncommitted()
    key = f"{topic}-guard-fixture:source"
    try:
        with pytest.raises(PaperReadChanged):
            server.cached_paper_json(topic, source_token="source", cache_key=key,
                ttl_seconds=60, cache_control="no-store", builder=build)
        assert len(built) == int(when == "during")
        assert key not in server._cache
        engine._persist()
        response = server.cached_paper_json(topic, source_token="source", cache_key=key,
            ttl_seconds=60, cache_control="no-store", builder=lambda: {"ready": True})
        assert json.loads(response.body) == {"ready": True}
    finally:
        server.server_close()


@pytest.mark.parametrize("advance", ["before", "during"])
def test_paper_view_cache_rebinds_exact_source_and_discards_superseded_build(advance):
    server = _test_server()
    token = {"value": "new" if advance == "before" else "old"}
    server.content_token = lambda *_args, **_kwargs: token["value"]
    calls = []

    def build():
        observed = token["value"]
        calls.append(observed)
        token["value"] = "new"
        return {"observed_source": observed}

    try:
        response = server.cached_paper_json("tw", source_token="old", cache_key="tw-signals:query:old",
            ttl_seconds=60, cache_control="no-store", builder=build)
        assert json.loads(response.body) == {"observed_source": "new"}
        assert calls == (["new"] if advance == "before" else ["old", "new"])
        assert "tw-signals:query:old" not in server._cache
        assert "tw-signals:query:new" in server._cache
    finally:
        server.server_close()


@pytest.mark.parametrize("topic", ["tw", "overnight"])
def test_history_cache_invalidates_immediately_on_content_revision(monkeypatch, topic, tmp_path):
    server = _test_server()
    # This cache test owns synthetic tokens/builders, not the live engine's
    # independently advancing journal and commit-marker generations.
    server.repo_root = tmp_path
    token = {"value": "one"}
    def content_token(_topic="tw", *, history_only=False):
        assert history_only is True
        return token["value"]

    server.content_token = content_token
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
        assert all(call["use_memory_cache"] is False for call in calls)
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
    assert refresh.index("loadSignals({force})") < refresh.index("loadChartHistory")
    assert "Promise.allSettled([signalsReady, historyReady])" not in refresh
    assert refresh.index("loadPositions({force})") < refresh.index("loadChartHistory")
    assert refresh.index("loadEvents({force})") < refresh.index("loadChartHistory")
    assert "await loadSignals" not in refresh
    key = source[source.index("function detailDataRevision"):source.index("function chartHistoryMatchesSelection")]
    assert "service.content_revision" in key
    assert "history.generated_at" in key
    assert "const sourceRevision = service.history_revision || null;" in key
    assert "sourceRevision || service.revision_token || service.content_revision || null" in key
    assert "detailRangeKey()" in key
    assert "signalLoading && signalRequestRevisionInFlight === requestRevision" in source
    assert "positionLoading && positionRequestRevisionInFlight === requestRevision" in source
    assert "eventLoading && eventRequestRevisionInFlight === requestRevision" in source


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
