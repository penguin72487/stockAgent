import json

import pytest

from scripts import audit_tw_day_trade_nontrade as audit
from stockagent.live.tw_day_trade_nontrade import CONTRACT


def fixture(tmp_path):
    root, candidate = tmp_path / "operation", tmp_path / "candidate"
    root.mkdir()
    candidate.mkdir()
    entry_fee = 28.5
    exit_fee = 1000 * 101. * (.002925 - .00114)
    net = 1000. - entry_fee - exit_fee
    detail = {"contract": CONTRACT, "accounting_at": "2026-10-29T13:30:00+08:00",
        "reference": {"reference_price": 101., "reference_observed_at": "2026-10-29T12:50:00+08:00",
                      "same_clock_price_tie_resolved_by_official_close": False},
        "reference_before_simulated_entry": False, "quantity": 1000,
        "entry_fee_allocated_twd": entry_fee, "gross_pnl_twd": 1000.,
        "assumed_exit_fee_and_tax_twd": exit_fee,
        "assumed_commission_rebate_twd": 1000 * 101. * .00114, "net_pnl_twd": net,
        "simulated_fill": False, "simulated_order": False, "broker_fill": False,
        "exchange_fill": False, "source_completeness_verified": False}
    position = {"symbol": "2330", "session_date": "2026-10-29", "entry_at": "2026-10-29T09:01:00+08:00",
        "side": "long", "lot_size": 1000, "signed_shares": 0, "entry_price": 100.,
        "buy_fee_rate": .001425, "sell_fee_rate": .002925, "commission_rebate_rate": .00114,
        "realized_net_pnl_twd": net, "nontrade_settlement": detail}
    mode = {"positions": {"p": position}, "cumulative_nontrade_settlement_net_pnl_twd": net,
            "execution_evidence_complete": False, "nontrade_settlement_count": 1, "nontrade_settlement_shares": 1000}
    manifest = {"status": "candidate_validated_not_applied", "nontrade_tick_receipt": "receipt",
        "provisional_minute_source_coverage": True, "source_pins": [], "candidate_dir": str(candidate),
        "market": "m", "sessions": [{"session_date": "2026-10-29"}], "anchor": {}}
    event = {"event": "nontrade_last_observed_accounting_settlement", "recorded_at": "2026-10-29T18:00:00+08:00",
             "position_id": "p", **detail}
    (root / "ledger_repair_manifest.json").write_text(json.dumps(manifest))
    (candidate / "state.json").write_text(json.dumps({"modes": {"m": mode}}))
    (candidate / "fills.jsonl").write_text(json.dumps({"position_id": "p", "quantity": 1000,
        "purpose": "entry", "fee_and_tax_twd": entry_fee}) + "\n")
    (candidate / "orders.jsonl").write_text("")
    (candidate / "events.jsonl").write_text(json.dumps(event) + "\n")
    return root, candidate


def test_nontrade_audit_reduces_fees_and_inventory_without_a_fill(tmp_path):
    root, _ = fixture(tmp_path)
    result = audit.audit(root)
    assert result["status"] == "verified_with_data_gaps"
    assert result["settlement_shares"] == 1000
    assert result["simulated_fill"] is False and result["broker_fill"] is False
    assert result["settlement_net_pnl_twd"] == pytest.approx(791.215)


@pytest.mark.parametrize("damage", ["fake_order", "net", "fee", "clock", "quantity", "unproven_fill", "account"])
def test_nontrade_audit_refuses_financial_clock_or_fill_tampering(tmp_path, damage):
    root, candidate = fixture(tmp_path)
    state = json.loads((candidate / "state.json").read_text())
    event = json.loads((candidate / "events.jsonl").read_text())
    mode = state["modes"]["m"]
    details = mode["positions"]["p"]["nontrade_settlement"]
    if damage == "fake_order":
        (candidate / "orders.jsonl").write_text(json.dumps({"purpose": "nontrade_fake"}) + "\n")
    elif damage == "account":
        mode["cumulative_nontrade_settlement_net_pnl_twd"] += 1
    elif damage == "unproven_fill":
        mode["execution_evidence_complete"] = True
    else:
        if damage == "net": details["net_pnl_twd"] += 1
        if damage == "fee": details["assumed_exit_fee_and_tax_twd"] += 1
        if damage == "clock": details["reference"]["reference_observed_at"] = "2026-10-29T14:00:00+08:00"
        if damage == "quantity": details["quantity"] = 2000
        event.update(details)
    (candidate / "state.json").write_text(json.dumps(state))
    (candidate / "events.jsonl").write_text(json.dumps(event) + "\n")
    with pytest.raises(RuntimeError):
        audit.audit(root)
