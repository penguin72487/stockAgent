from __future__ import annotations

import json

import pytest

from scripts.audit_tw_day_trade_restart import audit, verify_prefix
from stockagent.live.tw_day_trade_simulation import SIMULATION_SCHEMA_VERSION, _atomic_json


def _state(tmp_path, pending=False):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    _atomic_json(state_dir / "state.json", {"schema_version": SIMULATION_SCHEMA_VERSION, "modes": {
        "paper": {"signal_id": "original", "processed_signal_ids": ["original"],
                  "positions": {"original-position": {"symbol": "2330", "signed_shares": 1000}},
                  "entry_retry_pending_commit": "in-flight" if pending else None}}})
    (state_dir / "fills.jsonl").write_text('{"fill":"original"}\n')
    (state_dir / "orders.jsonl").write_text('{"order":"original"}\n')
    return state_dir


def test_restart_audit_keeps_production_state_and_ledger_unchanged(tmp_path):
    state_dir = _state(tmp_path)
    before = {p.name: p.read_bytes() for p in state_dir.iterdir()}
    receipt = audit(state_dir, tmp_path / "proof" / "restart.json")
    assert receipt["inventory_restored_exactly"] and receipt["open_position_count"] == 1
    assert {p.name: p.read_bytes() for p in state_dir.iterdir()} == before
    assert receipt["markets"]["paper"]["signal_id"] == "original"


def test_restart_audit_rejects_inflight_commit(tmp_path):
    with pytest.raises(RuntimeError, match="pending or divergent"):
        audit(_state(tmp_path, pending=True), tmp_path / "restart.json")


def test_ledger_prefix_accepts_appends_but_rejects_history_change(tmp_path):
    state_dir = _state(tmp_path)
    receipt = audit(state_dir, tmp_path / "restart.json")
    with (state_dir / "fills.jsonl").open("a") as handle:
        handle.write('{"fill":"later"}\n')
    assert verify_prefix(state_dir, receipt)["fills.jsonl"]["current_size_bytes"] > receipt["inputs"]["fills.jsonl"]["size_bytes"]
    (state_dir / "fills.jsonl").write_text('{"fill":"tampered"}\n')
    with pytest.raises(RuntimeError, match="ledger prefix changed|ledger was truncated"):
        verify_prefix(state_dir, receipt)
