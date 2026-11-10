from datetime import date
import json

import pytest

from scripts import audit_tw_day_trade_historical_anchor as audit


@pytest.fixture
def anchored(tmp_path, monkeypatch):
    anchor, live = tmp_path / "anchor", tmp_path / "live"
    anchor.mkdir()
    live.mkdir()
    position = {"position_id": "p1", "symbol": "2330", "side": "long", "signed_shares": 0,
                "requested_shares": 1000, "filled_shares": 1000, "entry_price": 100.0,
                "closed_shares": 1000, "realized_net_pnl_twd": 50.0}
    mode = {"session_date": "2026-01-02", "market": "m", "signal_id": "s",
            "config_fingerprint": "c", "checkpoint_fingerprint": "k", "positions": {"p1": position},
            "open_position_count": 0, "initial_capital_twd": 1000.0,
            "cumulative_realized_net_pnl_twd": 50.0, "total_equity_twd": 1050.0}
    (anchor / "state.json").write_text(json.dumps({"modes": {"m": mode}}))
    (anchor / "events.jsonl").write_text("")
    mark = {"market": "m", "session_date": "2026-01-02", "minute": "2026-01-02T13:30+08:00",
            "recorded_at": "2026-01-02T13:30:00+08:00", "initial_capital_twd": 1000.0,
            "cumulative_realized_net_pnl_twd": 50.0, "open_position_count": 0, "total_equity_twd": 1050.0}
    fill = {"market": "m", "session_date": "2026-01-02", "symbol": "2330", "position_id": "p1",
            "order_id": "o1", "quantity": 1000, "price": 100.0, "fee_and_tax_twd": 2.0,
            "simulation_only": True}
    for folder in (anchor, live):
        (folder / "marks.jsonl").write_text(json.dumps(mark) + "\n")
        (folder / "fills.jsonl").write_text(json.dumps(fill) + "\n")
    history = live / "position_history/2026-01-02"
    history.mkdir(parents=True)
    (history / "m.json").write_text(json.dumps({"market": "m", "session_date": "2026-01-02",
        "signal_id": "s", "config_fingerprint": "c", "checkpoint_fingerprint": "k",
        "simulation_only": True, "production_order_possible": False, "positions": [position]}))
    monkeypatch.setattr(audit, "_validated_taiex_session_dates", lambda *args:
                        ({date(2026, 1, 2), date(2026, 1, 5)}, "calendar-hash"))
    monkeypatch.setattr(audit, "_require_missing_session_target", lambda *args, **kwargs: None)
    return anchor, live


def _audit(anchored):
    anchor, live = anchored
    return audit.audit(anchor_dir=anchor, live_dir=live, market="m", anchor_day=date(2026, 1, 2),
                       target_day=date(2026, 1, 5), calendar_root=live)


def test_anchor_audit_proves_prior_scope_only(anchored):
    result = _audit(anchored)
    assert result["status"] == "prior_flat_anchor_component_verified_not_replayed"
    assert result["matching_fill_count"] == result["position_count"] == 1
    assert result["ledger_modified"] is False and result["publication_authorized"] is False
    assert result["subsequent_sessions_validated"] is False


def test_anchor_audit_rejects_changed_live_fee(anchored):
    _, live = anchored
    path = live / "fills.jsonl"
    fill = json.loads(path.read_text())
    fill["fee_and_tax_twd"] = 3.0
    path.write_text(json.dumps(fill) + "\n")
    with pytest.raises(RuntimeError, match="fills or fees differ"):
        _audit(anchored)


def test_anchor_audit_rejects_nonflat_archived_inventory(anchored):
    _, live = anchored
    path = live / "position_history/2026-01-02/m.json"
    archive = json.loads(path.read_text())
    archive["positions"][0]["signed_shares"] = 1
    path.write_text(json.dumps(archive))
    with pytest.raises(ValueError, match="nonflat"):
        _audit(anchored)


def test_anchor_audit_rejects_intervening_official_session(anchored, monkeypatch):
    monkeypatch.setattr(audit, "_validated_taiex_session_dates", lambda *args:
                        ({date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 5)}, "hash"))
    with pytest.raises(RuntimeError, match="immediately preceding"):
        _audit(anchored)


def test_anchor_audit_rejects_managed_journals_without_segment_pins(anchored, monkeypatch):
    monkeypatch.setattr(audit, "managed", lambda path: path.name == "fills.jsonl")
    with pytest.raises(ValueError, match="managed journal segments"):
        _audit(anchored)


def test_anchor_audit_rejects_naive_recorded_clock(anchored):
    anchor, _ = anchored
    path = anchor / "marks.jsonl"
    mark = json.loads(path.read_text())
    mark["recorded_at"] = "2026-01-02T13:30:00"
    path.write_text(json.dumps(mark) + "\n")
    with pytest.raises(RuntimeError, match="ambiguous"):
        _audit(anchored)
