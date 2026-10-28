from datetime import date
import hashlib

import polars as pl

from scripts import audit_tw_day_trade_fill_replay_candidate as candidate


def _fixture(tmp_path):
    weights = tmp_path / "weights.parquet"
    pl.DataFrame({"symbol": ["2330", "0050"], "target_weight": [0.5, 0.2]}).write_parquet(weights)
    digest = hashlib.sha256(weights.read_bytes()).hexdigest()
    inventory = {"modes": {"mode": {"weights_path": str(weights), "weights_sha256": digest,
                                    "first_registered_matches_first_publication": False,
                                    "registrations": [{"signal_id": "later"}]}}}
    modes = {"mode": {"session_date": "2026-10-28", "positions": {
        "p": {"position_id": "p", "symbol": "2330", "side": "short",
              "filled_shares": 1000, "signed_shares": 0}}}}
    base = {"market": "mode", "position_id": "p", "symbol": "2330", "quantity": 1000,
            "price": 100., "simulation_only": True}
    fills = [base | {"purpose": "entry", "fill_at": "2026-10-28T09:00:30+08:00"},
             base | {"purpose": "13_24_market_force_exit", "fill_at": "2026-10-28T13:24:00+08:00"}]
    return modes, fills, inventory


def test_quantity_replay_keeps_first_signal_comparison_diagnostic(tmp_path):
    modes, fills, inventory = _fixture(tmp_path)
    result = candidate.reconstruct(day=date(2026, 10, 28), modes=modes, fills=fills, signal_inventory=inventory)
    assert result["quantity_reconciliation_passed"]
    assert result["modes"]["mode"]["held_symbols_at_0901"] == 1
    assert result["modes"]["mode"]["first_signal_only_symbols"] == ["0050"]
    assert result["modes"]["mode"]["first_registered_matches_first_publication"] is False
    assert result["pnl_fee_source_price_and_minute_coverage_verified"] is False


def test_quantity_replay_rejects_unknown_purpose_or_reduction_before_entry(tmp_path):
    modes, fills, inventory = _fixture(tmp_path)
    fills[0]["fill_at"] = "2026-10-28T13:25:00+08:00"
    result = candidate.reconstruct(day=date(2026, 10, 28), modes=modes, fills=fills, signal_inventory=inventory)
    assert not result["quantity_reconciliation_passed"]
    assert any("exceeds prior inventory" in error for error in result["errors"])
    fills[0]["purpose"] = "unknown"
    result = candidate.reconstruct(day=date(2026, 10, 28), modes=modes, fills=fills, signal_inventory=inventory)
    assert not result["quantity_reconciliation_passed"]


def test_quantity_replay_rejects_weight_or_snapshot_mismatch(tmp_path):
    modes, fills, inventory = _fixture(tmp_path)
    modes["mode"]["positions"]["p"]["filled_shares"] = 2000
    inventory["modes"]["mode"]["weights_sha256"] = "0" * 64
    result = candidate.reconstruct(day=date(2026, 10, 28), modes=modes, fills=fills, signal_inventory=inventory)
    assert not result["quantity_reconciliation_passed"]
    assert any("snapshot" in error for error in result["errors"])
    assert any("weights changed" in error for error in result["errors"])


def test_recorded_pnl_arithmetic_reconciles_without_claiming_fee_or_fill_correctness(tmp_path):
    modes, fills, _ = _fixture(tmp_path)
    modes["mode"]["positions"]["p"]["realized_net_pnl_twd"] = -18.0
    fills[1].update(gross_pnl_twd=0.0, fee_and_tax_twd=6.0,
                    entry_fee_allocated_twd=12.0, net_pnl_twd=-18.0)
    result = candidate.audit_pnl_arithmetic(modes=modes, fills=fills)
    assert result["exit_rows_reconciled"] == 1
    assert result["arithmetic_consistency_passed"]
    assert not result["fee_schedule_execution_prices_and_economic_correctness_verified"]
    fills[1]["fee_and_tax_twd"] = 7.0
    assert not candidate.audit_pnl_arithmetic(modes=modes, fills=fills)["arithmetic_consistency_passed"]


def test_recorded_pnl_rejects_snapshot_disagreement_and_missing_fields(tmp_path):
    modes, fills, _ = _fixture(tmp_path)
    modes["mode"]["positions"]["p"]["realized_net_pnl_twd"] = -17.0
    fills[1].update(gross_pnl_twd=0.0, fee_and_tax_twd=6.0,
                    entry_fee_allocated_twd=12.0, net_pnl_twd=-18.0)
    assert not candidate.audit_pnl_arithmetic(modes=modes, fills=fills)["arithmetic_consistency_passed"]
    del fills[1]["entry_fee_allocated_twd"]
    assert not candidate.audit_pnl_arithmetic(modes=modes, fills=fills)["arithmetic_consistency_passed"]


def test_pnl_arithmetic_without_exits_is_not_positive_evidence(tmp_path):
    modes, fills, _ = _fixture(tmp_path)
    result = candidate.audit_pnl_arithmetic(modes=modes, fills=fills[:1])
    assert not result["arithmetic_consistency_passed"]
    assert result["exit_rows_reconciled"] == 0


def test_fee_arithmetic_uses_correct_short_entry_and_cover_rates(tmp_path):
    modes, fills, _ = _fixture(tmp_path)
    position = modes["mode"]["positions"]["p"]
    position.update(buy_fee_rate=0.001425, sell_fee_rate=0.002925,
                    commission_rebate_rate=0.00114)
    fills[0].update(gross_fee_and_tax_twd=292.5, commission_rebate_accrued_twd=114.0,
                    fee_and_tax_twd=178.5)
    fills[1].update(gross_fee_and_tax_twd=142.5, commission_rebate_accrued_twd=114.0,
                    fee_and_tax_twd=28.5)
    result = candidate.audit_recorded_fee_rates(modes=modes, fills=fills)
    assert result["fill_rows_reconciled"] == 2
    assert result["recorded_fee_arithmetic_passed"]
    assert not result["historical_broker_fee_terms_and_rebate_payment_verified"]
    fills[0]["fee_and_tax_twd"] += 1
    assert not candidate.audit_recorded_fee_rates(modes=modes, fills=fills)["recorded_fee_arithmetic_passed"]


def test_fee_arithmetic_does_not_guess_carry_conversion_time(tmp_path):
    modes, fills, _ = _fixture(tmp_path)
    modes["mode"]["positions"]["p"]["margin_carry_contract"] = "carry"
    result = candidate.audit_recorded_fee_rates(modes=modes, fills=fills)
    assert len(result["unresolved"]) == 2
    assert not result["recorded_fee_arithmetic_passed"]
