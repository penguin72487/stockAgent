from __future__ import annotations

from datetime import datetime
from http.client import HTTPConnection
import json
from pathlib import Path
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stockagent.live import tw_day_trade_published_signals as published


DAY = "2026-10-28"
NOW = datetime(2026, 10, 28, 9, 1, tzinfo=ZoneInfo("Asia/Taipei"))


@pytest.fixture
def source(tmp_path, monkeypatch):
    published._read_market.cache_clear()
    configs = {}
    monkeypatch.setattr(published, "load_market_configs", lambda _: configs)
    monkeypatch.setattr(published, "resolved_live_output_dir", lambda cfg: cfg.live_output_dir)

    def add(market="m", *, session=DAY, rows=None, revision=0):
        root = tmp_path / "signals" / market
        signal_id = f"{market}-signal"
        directory = root / f"{session}T09:00:00+08:00" / signal_id
        directory.mkdir(parents=True)
        ready = f"{session}T09:00:01+08:00"
        stamp = f"{session}T09:00:02+08:00"
        summary = {"market": market, "signal_id": signal_id, "asof_date": session,
                   "execution_mode": "tw_day_trade", "day_trade_model_observation": "session_open",
                   "signal_ready_at": ready, "opening_signal_revision": {"index": revision}}
        payload = {"schema_version": 1, "market": market, "signal_id": signal_id,
                   "rows": rows if rows is not None else [
                       {"symbol": "2330", "name": "台積電", "target_weight": 0.2,
                        "open_price": 999, "current_price": 998, "filled_shares": 123},
                       {"symbol": "1101", "name": "台泥", "target_weight": -0.4},
                       {"symbol": "9999", "target_weight": 0},
                   ]}
        (directory / "summary.json").write_text(json.dumps(summary))
        (directory / "execution_weights.json").write_text(json.dumps(payload))
        pointer = {"schema_version": 2, "market": market, "signal_id": signal_id,
                   "signal_ready_at": ready, "artifact_published_at": stamp,
                   "summary_path": str(directory / "summary.json"),
                   "execution_weights_path": str(directory / "execution_weights.json")}
        (root / "latest_signal.json").write_text(json.dumps(pointer))
        configs[market] = SimpleNamespace(enabled=True, day_trade_simulation_enabled=True,
                                           live_output_dir=root, label=f"Label {market}")
        return root, directory, pointer

    return add, configs


def test_published_targets_are_independent_of_paper_ledger_and_exclude_prices_and_fills(tmp_path, source):
    add, _ = source
    add(revision=3)
    result = published.build_published_signal_snapshot(repo_root=tmp_path, observed_at=NOW)
    assert result["session_date"] == DAY
    assert result["simulation_only"] is True and result["production_order_possible"] is False
    assert result["modes"][0]["status"] == "published"
    assert result["modes"][0]["revision_index"] == 3
    assert [row["symbol"] for row in result["rows"]] == ["1101", "2330"]
    assert [row["side"] for row in result["rows"]] == ["short", "long"]
    body = json.dumps(result)
    assert all(secret not in body for secret in ("open_price", "current_price", "filled_shares", "signal_id", str(tmp_path)))
    assert len(result["rows"][0]["public_signal_key"]) == 32
    assert not (tmp_path / "artifacts/live/tw_day_trade_simulation").exists()


@pytest.mark.parametrize("corruption", ["market", "signal_id", "date", "observation", "payload_id", "escape", "duplicate", "nan", "future"])
def test_unverified_or_changed_artifact_never_becomes_a_published_signal(tmp_path, source, corruption):
    add, _ = source
    root, directory, pointer = add()
    summary_path = directory / "summary.json"
    weights_path = directory / "execution_weights.json"
    summary = json.loads(summary_path.read_text())
    weights = json.loads(weights_path.read_text())
    if corruption == "market":
        summary["market"] = "other"
    elif corruption == "signal_id":
        summary["signal_id"] = "other"
    elif corruption == "date":
        summary["asof_date"] = "2026-10-27"
    elif corruption == "observation":
        summary["day_trade_model_observation"] = "latest_quote"
    elif corruption == "payload_id":
        weights["signal_id"] = "other"
    elif corruption == "escape":
        pointer["execution_weights_path"] = str(tmp_path / "outside.json")
        (tmp_path / "outside.json").write_text(json.dumps(weights))
    elif corruption == "duplicate":
        weights["rows"].append(dict(weights["rows"][0]))
    elif corruption == "nan":
        weights["rows"][0]["target_weight"] = float("nan")
    elif corruption == "future":
        pointer["artifact_published_at"] = f"{DAY}T09:02:00+08:00"
    summary_path.write_text(json.dumps(summary))
    weights_path.write_text(json.dumps(weights))
    (root / "latest_signal.json").write_text(json.dumps(pointer))
    result = published.build_published_signal_snapshot(repo_root=tmp_path, observed_at=NOW)
    assert result["modes"][0]["status"] == "unavailable"
    assert result["rows"] == []


def test_one_mode_failure_does_not_hide_an_independently_published_mode(tmp_path, source):
    add, configs = source
    add("good")
    configs["waiting"] = SimpleNamespace(enabled=True, day_trade_simulation_enabled=True,
                                           live_output_dir=tmp_path / "no-pointer", label="Waiting")
    result = published.build_published_signal_snapshot(repo_root=tmp_path, observed_at=NOW)
    assert {m["market"]: m["status"] for m in result["modes"]} == {"good": "published", "waiting": "waiting"}
    assert len(result["rows"]) == 2


def test_previous_session_is_not_shown_as_todays_signal(tmp_path, source):
    add, _ = source
    add(session="2026-10-27")
    result = published.build_published_signal_snapshot(repo_root=tmp_path, observed_at=NOW)
    assert result["modes"][0]["status"] == "unavailable"
    assert result["rows"] == []


def test_pointer_change_during_read_rejects_old_revision(tmp_path, source, monkeypatch):
    add, _ = source
    root, _, pointer = add()
    original = published.read_execution_weight_rows

    def swap(*args, **kwargs):
        rows = original(*args, **kwargs)
        (root / "latest_signal.json").write_text(json.dumps({**pointer, "signal_id": "new"}))
        return rows

    monkeypatch.setattr(published, "read_execution_weight_rows", swap)
    result = published.build_published_signal_snapshot(repo_root=tmp_path, observed_at=NOW)
    assert result["modes"][0]["status"] == "unavailable"
    assert result["rows"] == []


def test_public_http_signal_first_and_validator_do_not_touch_paper_commit(tmp_path, source, monkeypatch):
    from scripts import serve_public_dashboards as gateway

    add, _ = source
    add()
    monkeypatch.setattr(gateway, "build_published_signal_snapshot",
                        lambda *, repo_root: published.build_published_signal_snapshot(repo_root=repo_root, observed_at=NOW))
    monkeypatch.setattr(gateway, "committed_paper_read_guard", lambda *_: pytest.fail("must not read paper ledger"))
    code_root = Path(__file__).resolve().parents[1]
    server = gateway.PublicDashboardServer(
        ("127.0.0.1", 0), repo_root=tmp_path, audience="public",
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
    connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
    try:
        connection.request("GET", "/tw-day-trade/api/published-signals")
        response = connection.getresponse()
        assert response.status == 200
        etag = response.getheader("ETag")
        body = json.loads(response.read())
        assert len(body["rows"]) == 2 and body["modes"][0]["status"] == "published"
        connection.request("GET", "/tw-day-trade/api/published-signals", headers={"If-None-Match": etag})
        response = connection.getresponse()
        assert response.status == 304
        response.read()
        connection.request("GET", "/tw-day-trade/published-signals.js?v=1")
        response = connection.getresponse()
        assert response.status == 200
        assert b"api/published-signals" in response.read()
        connection.request("GET", "/tw-day-trade/api/published-signals?date=2026-10-27")
        response = connection.getresponse()
        assert response.status == 400
        response.read()
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
