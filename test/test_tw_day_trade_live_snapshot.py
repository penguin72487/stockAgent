from __future__ import annotations

import json
import os
from http.client import HTTPConnection
from pathlib import Path
import threading

import pytest

from stockagent.live import tw_day_trade_live_snapshot as live
from stockagent.live import tw_day_trade_live_events as live_events
from stockagent.live.tw_day_trade_service_sync import (
    PaperReadChanged, SERVICE_SYNC_DURABLE_ACK_CONTRACT, paper_projection_generations, paper_journal_generations,
)


def _write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def committed(tmp_path):
    envelope = {"schema_version": 1, "simulation_only": True, "production_order_possible": False,
                "engine_run_id": "run", "state_revision": 7, "content_revision": 5}
    mode = {"market": "m", "label": "策略", "session_date": "2026-10-28", "engine_status": "active",
            "open_position_count": 1, "readiness_error": "private failure details",
            "initial_capital_twd": 10000, "total_equity_twd": 10010}
    row = {"market": "m", "symbol": "2330", "session_date": "2026-10-28", "signed_shares": 1000,
           "filled_shares": 1000, "entry_price": 999, "signal_id": "private-id", "password": "secret"}
    _write(tmp_path / "status.json", {**envelope, "updated_at": "2026-10-28T09:00:01+08:00", "modes": {"m": mode}})
    _write(tmp_path / "positions.json", {**envelope, "generated_at": "2026-10-28T09:00:01+08:00",
           "modes": {"m": {"market": "m", "open_position_count": 1, "positions": [row]}}})
    for filename, event in {
        "orders.jsonl": {"market": "m", "symbol": "2330", "session_date": "2026-10-28",
                         "recorded_at": "2026-10-28T09:00:01+08:00", "status": "submitted", "quantity": 1000},
        "fills.jsonl": {"market": "m", "symbol": "2330", "session_date": "2026-10-28",
                        "recorded_at": "2026-10-28T09:00:02+08:00", "quantity": 1000, "price": 999,
                        "simulation_replay": True, "password": "private fill secret"},
        "events.jsonl": {"market": "m", "session_date": "2026-10-28", "recorded_at": "2026-10-28T09:00:03+08:00",
                         "event": "signal_registered", "error": "private diagnostic"},
    }.items():
        (tmp_path / filename).write_text(json.dumps(event) + "\n")
    ack = {"contract": SERVICE_SYNC_DURABLE_ACK_CONTRACT, "commit_id": "commit"}
    receipt = {**envelope, "durability_ack": ack, "projection_generations": paper_projection_generations(tmp_path),
               "journal_generations": paper_journal_generations(tmp_path), "enabled_markets": ["m"],
               "published_at": "2026-10-28T09:00:04+08:00"}
    _write(tmp_path / "service_sync.json", receipt)
    st = (tmp_path / "service_sync.json").stat()
    _write(tmp_path / "service_sync_durable_ack.json", {"schema_version": 1, "durability_ack": ack,
            "engine_run_id": "run", "state_revision": 7, "content_revision": 5,
            "marker_generation": [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]})
    return tmp_path


def test_acknowledged_projection_is_sanitized_and_preserves_complete_open_positions(committed):
    snapshot = live.build_live_paper_snapshot(state_dir=committed)
    assert snapshot["state_revision"] == 7
    assert snapshot["scope"] == "committed_open_positions_only"
    assert snapshot["modes"][0]["open_position_count"] == len(snapshot["rows"]) == 1
    assert snapshot["modes"][0]["readiness_error"] == "unavailable"
    assert snapshot["rows"][0]["symbol"] == "2330"
    body = json.dumps(snapshot)
    assert "private-id" not in body and "secret" not in body and "private failure details" not in body
    assert snapshot["rows"][0]["entry_price"] is None
    owner = live.build_live_paper_snapshot(state_dir=committed, private_prices=True)
    assert owner["rows"][0]["entry_price"] == 999
    assert "secret" not in json.dumps(owner)


def test_live_mode_retains_nontrade_disclosure_from_same_commit(committed, monkeypatch):
    original = live._read_once
    def read(root):
        receipt, files = original(root)
        files["status.json"]["modes"]["m"].update(nontrade_settlement_count=2,
            nontrade_settlement_shares=2000, cumulative_nontrade_settlement_net_pnl_twd=-100.)
        return receipt, files
    monkeypatch.setattr(live, "_read_once", read)
    mode = live.build_live_paper_snapshot(state_dir=committed)["modes"][0]
    assert mode["nontrade_settlement_count"] == 2
    assert mode["nontrade_settlement_shares"] == 2000
    assert mode["cumulative_nontrade_settlement_net_pnl_twd"] == -100.


@pytest.mark.parametrize("damage", ["missing_ack", "wrong_ack", "ahead", "missing_generation", "unsafe_receipt"])
def test_unacknowledged_or_ahead_files_are_never_published(committed, monkeypatch, damage):
    monkeypatch.setattr(live.time, "sleep", lambda _: None)
    if damage == "missing_ack":
        (committed / "service_sync_durable_ack.json").unlink()
    elif damage == "wrong_ack":
        _write(committed / "service_sync_durable_ack.json", {})
    elif damage == "ahead":
        _write(committed / "positions.json", {"state_revision": 8})
    elif damage == "missing_generation":
        original = live.load_service_sync
        monkeypatch.setattr(live, "load_service_sync", lambda root: {**original(root), "projection_generations": {}})
    else:
        original = live.load_service_sync
        monkeypatch.setattr(live, "load_service_sync", lambda root: {**original(root), "production_order_possible": True})
    with pytest.raises(PaperReadChanged):
        live.build_live_paper_snapshot(state_dir=committed)


def test_mixed_revision_is_rejected_even_if_file_metadata_matches_receipt(committed, monkeypatch):
    monkeypatch.setattr(live.time, "sleep", lambda _: None)
    old_load = live.load_service_sync
    changed = json.loads((committed / "positions.json").read_text())
    changed["state_revision"] = 8
    _write(committed / "positions.json", changed)
    monkeypatch.setattr(live, "load_service_sync", lambda root: {
        **old_load(root), "projection_generations": paper_projection_generations(root)})
    with pytest.raises(PaperReadChanged):
        live.build_live_paper_snapshot(state_dir=committed)


def test_replace_during_capture_is_rejected_not_a_mixed_view(committed, monkeypatch):
    monkeypatch.setattr(live.time, "sleep", lambda _: None)
    original = live.os.fstat
    changed = False

    def replace(fd):
        nonlocal changed
        info = original(fd)
        if not changed and info.st_ino == (committed / "positions.json").stat().st_ino:
            changed = True
            new = committed / "replacement.json"
            new.write_bytes((committed / "positions.json").read_bytes())
            os.replace(new, committed / "positions.json")
        return info

    monkeypatch.setattr(live.os, "fstat", replace)
    with pytest.raises(PaperReadChanged):
        live.build_live_paper_snapshot(state_dir=committed)


def test_mode_counts_cannot_silently_hide_positions(committed, monkeypatch):
    original = live._read_once

    def read(root):
        receipt, files = original(root)
        files["positions.json"]["modes"]["m"]["positions"] = []
        return receipt, files

    monkeypatch.setattr(live, "_read_once", read)
    with pytest.raises(PaperReadChanged):
        live.build_live_paper_snapshot(state_dir=committed)


def test_recent_committed_paper_events_keep_replay_and_strip_private_content(committed):
    result = live_events.build_live_paper_events(state_dir=committed)
    assert result["scope"] == "recent_committed_events_only"
    assert [row["event_kind"] for row in result["rows"]] == ["notice", "fill", "order"]
    assert result["rows"][1]["replay"] is True
    assert result["rows"][1]["status"] == "filled"
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("damage", ["ahead", "missing_generation", "immutable_layout", "unsafe"])
def test_recent_events_fail_closed_on_generation_or_contract_change(committed, monkeypatch, damage):
    monkeypatch.setattr(live_events.time, "sleep", lambda _: None)
    original = live_events.load_service_sync
    if damage == "ahead":
        with (committed / "fills.jsonl").open("a") as stream:
            stream.write('{}\n')
    elif damage == "missing_generation":
        monkeypatch.setattr(live_events, "load_service_sync", lambda root: {**original(root), "journal_generations": {}})
    elif damage == "immutable_layout":
        monkeypatch.setattr(live_events, "load_service_sync", lambda root: {**original(root),
            "journal_generations": {**original(root)["journal_generations"], "journal_heads/fills.jsonl.json": [1]}})
    else:
        monkeypatch.setattr(live_events, "load_service_sync", lambda root: {**original(root), "simulation_only": False})
    with pytest.raises(PaperReadChanged):
        live_events.build_live_paper_events(state_dir=committed)


def test_public_gateway_serves_committed_live_snapshot_without_scanning_journals(committed, monkeypatch):
    from scripts import serve_public_dashboards as gateway
    monkeypatch.setattr(gateway, "build_live_paper_snapshot", lambda **_: live.build_live_paper_snapshot(state_dir=committed))
    monkeypatch.setattr(gateway, "build_live_paper_events", lambda **_: live_events.build_live_paper_events(state_dir=committed))
    monkeypatch.setattr(gateway, "committed_paper_read_guard", lambda *_: pytest.fail("must not require a journal scan"))
    code_root = Path(__file__).resolve().parents[1]
    server = gateway.PublicDashboardServer(
        ("127.0.0.1", 0), repo_root=committed, audience="public",
        public_static_root=code_root / "services/public_dashboards",
        taifex_static_root=code_root / "services/taifex_dashboard",
        tw_static_root=code_root / "services/tw_day_trade_dashboard",
        shioaji_static_root=code_root / "services/shioaji_api_dashboard",
        openbb_static_root=code_root / "services/openbb_archive_dashboard",
        data_monitor_static_root=code_root / "services/data_monitor_dashboard",
        traffic_static_root=code_root / "services/traffic_dashboard",
        taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    try:
        conn.request("GET", "/tw-day-trade/api/live-snapshot")
        response = conn.getresponse()
        assert response.status == 200
        etag = response.getheader("ETag")
        assert json.loads(response.read())["rows"][0]["symbol"] == "2330"
        conn.request("GET", "/tw-day-trade/api/live-snapshot", headers={"If-None-Match": etag})
        response = conn.getresponse()
        assert response.status == 304
        response.read()
        conn.request("GET", "/tw-day-trade/api/live-snapshot?date=2026-10-27")
        response = conn.getresponse()
        assert response.status == 400
        response.read()
        conn.request("GET", "/tw-day-trade/api/live-events")
        response = conn.getresponse()
        assert response.status == 200
        assert [row["event_kind"] for row in json.loads(response.read())["rows"]] == ["notice", "fill", "order"]
        conn.request("GET", "/tw-day-trade/live-snapshot.js?v=1")
        response = conn.getresponse()
        assert response.status == 200 and b"api/live-snapshot" in response.read()
    finally:
        conn.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
