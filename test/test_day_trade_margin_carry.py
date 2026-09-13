"""User-authorized all-marginable residuals remain inventory, never fills."""
from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json

import polars as pl
import pytest

from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE, ENTRY_FILL_POLICY_CAUSAL_BOOK, MARGIN_CARRY_CONTRACT,
    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE, TwDayTradeSimulationEngine,
    position_net_liquidation_pnl,
)
from test_tw_day_trade_simulation import _spec, _row, _quote, _eligibility, _summary, _now


def action_reference(tmp_path, symbol="OTHER", ex_date="2026-08-01"):
    path = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame({"date": [ex_date], "symbol": [symbol], "reference_price": [999.0],
                  "event_type": ["除息"]}).with_columns(pl.col("date").str.to_date()).write_parquet(path)
    path.with_suffix(".summary.json").write_text(json.dumps({
        "baseline_established": True, "coverage_complete": True, "failure_count": 0,
        "schema_version": 3, "rows": 1, "requested_start_year": 2026,
        "coverage_start_year": 2026, "end_date": "2026-09-10",
        "output_receipt": {"size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
    }))
    return path


def cash_entitlement(tmp_path, amount=10., ex_date="2026-08-14", pay_date="2026-08-18"):
    reference = action_reference(tmp_path, "2330", ex_date)
    path = tmp_path / "tw_corporate_action_entitlements.parquet"
    pl.DataFrame({"date": [date.fromisoformat(ex_date)], "symbol": ["2330"],
        "handling": ["exact_cash"], "cash_dividend_per_share": [amount],
        "cash_payment_date": [date.fromisoformat(pay_date)], "stop_transfer_start": [None]},
        schema_overrides={"stop_transfer_start": pl.Date}).write_parquet(path)
    raw = b'{"fixture":true}\n'
    digest = hashlib.sha256(raw).hexdigest()
    manifest = tmp_path / f"{digest}.jsonl"
    manifest.write_bytes(raw)
    path.with_suffix(".summary.json").write_text(json.dumps({
        "schema_version": 3, "baseline_established": True, "coverage_complete": True,
        "failure_count": 0, "rows": 1, "reference_rows": 1,
        "coverage_start": "2026-01-01", "coverage_end": "2026-09-10",
        "raw_receipt_manifest": {"relative_path": manifest.name, "entries": 1,
                                 "size": len(raw), "sha256": digest},
        "reference_receipt": {"size": reference.stat().st_size, "sha256": hashlib.sha256(reference.read_bytes()).hexdigest()},
        "output_receipt": {"size": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
    }))


def setup_account(tmp_path, weight=.2):
    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
                   margin_corporate_action_reference_path=action_reference(tmp_path),
                   entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    register(engine, spec, 0, weight)
    close = _quote(minute_volume_lots=0) | {"quote_at": _now(13, 30).isoformat()}
    engine.process_quotes(quotes={"2330": close}, now=_now(13, 30))
    return engine, spec


def register(engine, spec, days, weight, volume=100, price=1000, missing=False, omit_target=False, quote_updates=None):
    at = _now(9, 1) + timedelta(days=days)
    summary = _summary(f"signal-{days}") | {
        "generated_at": (at - timedelta(minutes=1)).isoformat(),
        "simulation_replay": True, "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    }
    quote = _quote(bid=price, ask=price, minute_volume_lots=volume) | {
        "open": price, "execution_price_0901": None if missing else price,
        "execution_price_0901_method": "minute_close", "quote_at": at.isoformat(),
    }
    quote.update(quote_updates or {})
    return engine.register_signal(spec=spec, summary=summary, signal_rows=[] if omit_target else [_row(weight)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={},
        now=at, counterfactual_open_replay=True)


def fills(engine):
    return [json.loads(s) for s in engine.fills_path.read_text().splitlines()]


def share_proof(path):
    from downloader.download_tw_corporate_action_entitlements import (
        _reset_raw_receipt_requests, _record_raw_receipt_request,
        _write_content_addressed_receipt_manifest,
    )
    raw = path.parent / "raw/tw_share_replacement_reference/fixture.json"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text('{"official_fixture":true}\n')
    _reset_raw_receipt_requests()
    _record_raw_receipt_request(raw, url="https://www.twse.com.tw/exchangeReport/TWTAUU",
                               data={}, content=raw.read_bytes(), method="GET")
    manifest = _write_content_addressed_receipt_manifest(output_dir=path.parent, raw_root=raw.parent)
    path.with_suffix(".summary.json").write_text(json.dumps({
        "source_download_complete": True, "failure_count": 0, "rows": 1,
        "coverage_start": "2026-01-01", "coverage_end": "2026-09-10",
        "announced_resumption_query_end": "2026-12-09",
        "covered_markets": ["twse", "tpex"],
        "output_receipt": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
        "raw_receipt_manifest": manifest}))


def test_guardian_distinguishes_user_approved_carry_from_unresolved_delivery():
    from scripts.check_tw_day_trade_unattended_health import _accepted_margin_residual
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    row = dict(session_date="2026-08-13", open_position_count=1, margin_carry_position_count=1,
               margin_carry_contract=MARGIN_CARRY_CONTRACT, odd_lot_execution_policy=ODD_LOT_BOARD_PRICE,
               valuation_complete=True, closing_auction_settled_at="2026-08-13T13:30:01+08:00",
               residual_conversion_completed_at="2026-08-13T13:30:01+08:00")
    assert _accepted_margin_residual(row, "2026-08-13")
    for changes in [dict(odd_lot_execution_policy="reject"), dict(margin_carry_position_count=0),
                    dict(valuation_complete=False), dict(session_date="2026-08-12"),
                    dict(margin_corporate_action_receipt={"status": "blocked"})]:
        assert not _accepted_margin_residual(row | changes, "2026-08-13")


def test_stale_price_is_revalued_using_current_quantity_basis_and_fees(tmp_path):
    from stockagent.live.tw_day_trade_simulation import position_net_liquidation_pnl
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    p = next(iter(mode["positions"].values()))
    p.update(signed_shares=750, inventory_basis_price=1300., last_mark_price=1290.,
             remaining_entry_fee_twd=15., last_complete_net_pnl_twd=999999.)
    expected = position_net_liquidation_pnl(p, 1290.)
    engine._mark_mode(spec.market, _now(13, 30), {}, append_history=False)
    assert mode["open_net_liquidation_pnl_twd"] == pytest.approx(expected)
    assert p["valuation_stale"] and p["last_mark_price"] == 1290.


def test_passive_order_touch_price_is_not_the_minute_inventory_mark(tmp_path):
    from scripts.rebuild_tw_day_trade_open_price_replay import _eod_kbar_quotes
    from stockagent.live.tw_day_trade_simulation import position_net_liquidation_pnl
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    mode["historical_minute_valuation"] = True
    at = _now(13, 21)
    bar = dict(open=1000., high=1100., low=900., close=1010., vwap=1005., volume_shares=10000.)
    quotes = _eod_kbar_quotes(mode, {"2330": {at.isoformat(timespec="minutes"): bar}}, observed=at)
    assert quotes["2330"]["bid"] == 1100.
    engine._mark_mode(spec.market, at, quotes, append_history=False)
    p = next(iter(mode["positions"].values()))
    assert p["last_mark_price"] == 1010.
    assert mode["open_net_liquidation_pnl_twd"] == pytest.approx(position_net_liquidation_pnl(p, 1010.))


def test_inventory_removed_from_model_universe_is_reduced_without_fee_key_error(tmp_path):
    engine, spec = setup_account(tmp_path)
    assert register(engine, spec, 1, 0, omit_target=True) == "registered"
    assert all(not int(p["signed_shares"]) for p in engine.state["modes"][spec.market]["positions"].values())
    assert fills(engine)[-1]["purpose"] == "next_signal_inventory_delta"
    assert fills(engine)[-1]["fill_contract"] == REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE


def test_known_share_replacement_is_not_retried_as_missing_quote(tmp_path):
    engine, spec = setup_account(tmp_path)
    path = tmp_path / "tw_share_replacement_reference.parquet"
    pl.DataFrame({"symbol": ["2330"], "suspension_date": [date(2026, 8, 14)],
        "resume_date": [date(2026, 8, 24)], "new_shares_per_1000_old": [750.],
        "cash_return_per_old_share": [2.5], "unresolved_terms": ["payment_and_odd_lots"]}).write_parquet(path)
    share_proof(path)
    before = len(fills(engine))
    assert register(engine, spec, 1, 0) == "waiting_margin_corporate_action"
    mode = engine.state["modes"][spec.market]
    assert len(fills(engine)) == before
    assert mode["margin_corporate_action_receipt"]["blocking_domain"] == "corporate_action_accounting_not_quote_download"
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000


def replacement_reference(tmp_path, *, ratio=750., payment=date(2026, 8, 20)):
    path = tmp_path / "tw_share_replacement_reference.parquet"
    pl.DataFrame({"symbol": ["2330"], "suspension_date": [date(2026, 8, 14)],
        "resume_date": [date(2026, 8, 17)], "new_shares_per_1000_old": [ratio],
        "cash_return_per_old_share": [2.5], "cash_payment_date": [payment],
        "cash_dividend_per_old_share": [0.], "subscription_shares_per_1000": [0.]}).write_parquet(path)
    share_proof(path)


def test_issuer_reference_cannot_silently_become_complete_physical_accounting(tmp_path):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    engine, spec = setup_account(tmp_path)
    replacement_reference(tmp_path)
    path = tmp_path / "tw_share_replacement_reference.parquet"
    pl.read_parquet(path).with_columns(pl.lit(True).alias("reference_only")).write_parquet(path)
    share_proof(path)
    mode = engine.state["modes"][spec.market]
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    assert not engine._margin_corporate_action_gate(mode, _now(9, 1) + timedelta(days=4))
    assert "lacks complete physical accounting terms" in mode["margin_corporate_action_receipt"]["error"]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000
    assert not mode.get("share_replacement_ledger")


@pytest.mark.parametrize("patch", [{"coverage_end": "2026-08-13"}, {"covered_markets": ["twse"]},
                                   {"announced_resumption_query_end": "2026-09-10"}])
def test_odd_lot_carry_rejects_stale_or_partial_market_action_coverage(tmp_path, patch):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    engine, spec = setup_account(tmp_path)
    replacement_reference(tmp_path)
    proof = tmp_path / "tw_share_replacement_reference.summary.json"
    proof.write_text(json.dumps(json.loads(proof.read_text()) | patch))
    mode = engine.state["modes"][spec.market]
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    assert not engine._margin_corporate_action_gate(mode, _now(9, 1) + timedelta(days=4))
    assert "coverage does not cover" in mode["margin_corporate_action_receipt"]["error"]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000


@pytest.mark.parametrize("direction", [1, -1])
def test_share_replacement_cash_claim_and_odd_lot_exit_restart(tmp_path, direction):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    engine, spec = setup_account(tmp_path, .2 * direction)
    spec = replace(spec, odd_lot_execution_policy=ODD_LOT_BOARD_PRICE)
    replacement_reference(tmp_path)
    mode = engine.state["modes"][spec.market]
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    p = next(iter(mode["positions"].values()))
    assert engine._margin_corporate_action_gate(mode, _now(9, 1) + timedelta(days=1))
    assert p["signed_shares"] == 2000 * direction and not mode.get("share_replacement_ledger")
    engine._close_position(p, mode, price=1000, quote=_quote(), now=_now(9, 1) + timedelta(days=1),
                           reason="halt_test", order_type="MKT", quantity=2000)
    assert len(fills(engine)) == 1
    assert register(engine, spec, 4, 0., price=1330., quote_updates={"upper_limit": 1500., "lower_limit": 1200.}) == "registered"
    assert p["signed_shares"] == 0 and p["inventory_basis_price"] == pytest.approx(1000/.75)
    assert p["entry_price"] == 1000  # historical execution must stay immutable
    assert len(mode["share_replacement_ledger"]) == 1
    assert mode["corporate_action_receivable_twd"] == (5000. if direction > 0 else 0.)
    assert mode["corporate_action_payable_twd"] == (5000. if direction < 0 else 0.)
    sold = fills(engine)[-1]
    assert sold["quantity"] == 1500 and sold["price"] == 1330
    assert sold["odd_lot_execution_policy"] == ODD_LOT_BOARD_PRICE
    assert sold["gross_pnl_twd"] + mode["cumulative_corporate_action_net_twd"] == pytest.approx(0., abs=1e-8)
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    mode = restarted.state["modes"][spec.market]
    restarted._settle_corporate_action_claims(mode, _now(9, 1) + timedelta(days=7))
    restarted._settle_corporate_action_claims(mode, _now(9, 2) + timedelta(days=7))
    assert len(mode["share_replacement_ledger"]) == len(mode["corporate_action_ledger"]) == 1
    assert mode["corporate_action_cash_net_twd"] == 5000 * direction


@pytest.mark.parametrize("ratio,payment", [(750., None), (750.12345, date(2026, 8, 20))])
def test_invalid_share_terms_do_not_partially_mutate_inventory(tmp_path, ratio, payment):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    engine, spec = setup_account(tmp_path)
    replacement_reference(tmp_path, ratio=ratio, payment=payment)
    mode = engine.state["modes"][spec.market]
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    assert not engine._margin_corporate_action_gate(mode, _now(9, 1) + timedelta(days=4))
    p = next(iter(mode["positions"].values()))
    assert p["signed_shares"] == 2000 and p["entry_price"] == 1000
    assert not mode.get("share_replacement_ledger") and not mode.get("corporate_action_ledger")


def test_repeated_conversion_is_idempotent_and_target_addition_can_be_odd_lot(tmp_path):
    from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE
    engine, spec = setup_account(tmp_path)
    spec = replace(spec, odd_lot_execution_policy=ODD_LOT_BOARD_PRICE)
    replacement_reference(tmp_path)
    mode = engine.state["modes"][spec.market]
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    p = next(iter(mode["positions"].values()))
    for _ in range(2):
        assert engine._margin_corporate_action_gate(mode, _now(9, 0) + timedelta(days=4))
        assert p["signed_shares"] == 1500
        assert p["inventory_basis_price"] == pytest.approx(1000/.75)
    assert len(mode["corporate_action_ledger"]) == len(mode["share_replacement_ledger"]) == 1
    assert register(engine, spec, 4, .3, price=1330., quote_updates={"upper_limit": 1500., "lower_limit": 1200.}) == "registered"
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000
    assert fills(engine)[-1]["purpose"] == "entry" and fills(engine)[-1]["quantity"] == 500
    assert fills(engine)[-1]["odd_lot_execution_policy"] == ODD_LOT_BOARD_PRICE


@pytest.mark.parametrize("weight,limit,opening,low,high", [
    (.2, 1001., 1005., 1004., 1006.), (-.2, 999., 995., 994., 996.)])
def test_resting_limit_gap_uses_observed_open_not_price_outside_bar(tmp_path, weight, limit, opening, low, high):
    engine, spec = setup_account(tmp_path, weight)
    mode = engine.state["modes"][spec.market]
    p = next(iter(mode["positions"].values()))
    at = _now(13, 21) + timedelta(days=1)
    p.update(eod_limit_order_status="working", eod_limit_price=limit,
             eod_limit_submitted_at=(at - timedelta(minutes=1)).isoformat())
    quote = _quote(bid=high, ask=low, minute_volume_lots=100) | {
        "quote_at": at.isoformat(), "historical_bar_open": opening,
        "historical_bar_low": low, "historical_bar_high": high,
        "fill_contract": "historical_1m_ohlcv_counterfactual_not_executable_bid_ask"}
    engine._fill_crossed_exit_limits(mode, {"2330": quote}, at)
    assert fills(engine)[-1]["price"] == opening
    assert low <= fills(engine)[-1]["price"] <= high


def test_new_limit_does_not_fill_against_trades_before_submission(tmp_path):
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    p = next(iter(mode["positions"].values()))
    at = _now(13, 21) + timedelta(days=1)
    p.update(eod_limit_order_status="working", eod_limit_price=1000., eod_limit_submitted_at=at.isoformat())
    quote = _quote(bid=1001., minute_volume_lots=100) | {
        "quote_at": at.isoformat(), "historical_bar_open": 1001.,
        "historical_bar_low": 1001., "historical_bar_high": 1002.,
        "fill_contract": "historical_1m_ohlcv_counterfactual_not_executable_bid_ask"}
    engine._fill_crossed_exit_limits(mode, {"2330": quote}, at)
    assert len(fills(engine)) == 1 and p["signed_shares"] == 2000


@pytest.mark.parametrize("weight, kind", [(.2, "assumed_margin_financing"), (-.2, "assumed_margin_short")])
def test_conversion_preserves_basis_and_has_no_fictional_transaction(tmp_path, weight, kind):
    engine, spec = setup_account(tmp_path, weight)
    mode = engine.state["modes"][spec.market]
    p = next(iter(mode["positions"].values()))
    assert p["carry_type"] == kind
    assert p["margin_carry_contract"] == MARGIN_CARRY_CONTRACT
    assert p["entry_price"] == 1000
    assert abs(p["signed_shares"]) == 2000
    assert len(fills(engine)) == 1
    cost = mode.get("cumulative_carry_cost_twd", 0)
    engine.process_quotes(quotes={}, now=_now(14, 0))
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    assert restarted.state["modes"][spec.market].get("cumulative_carry_cost_twd", 0) == cost
    assert restarted.state["modes"][spec.market]["engine_status"] == "margin_carried_waiting_next_signal"


@pytest.mark.parametrize("weight", [.2002, -.2008])
def test_same_target_retains_inventory_without_roundtrip(tmp_path, weight):
    engine, spec = setup_account(tmp_path, .2 if weight > 0 else -.2)
    assert register(engine, spec, 1, weight) == "registered"
    mode = engine.state["modes"][spec.market]
    assert len(fills(engine)) == 1
    assert sum(int(p["signed_shares"]) for p in mode["positions"].values()) == (2000 if weight > 0 else -2000)
    assert mode["entry_0901_minute_price_fill_count"] == mode["entry_fill_count"] == 0
    assert mode["cumulative_carry_cost_twd"] > 0
    assert register(engine, spec, 1, weight) == "already_processed"
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    assert not restarted.state["modes"][spec.market].get("ledger_state_divergence")


def test_reduction_reversal_and_shared_minute_capacity(tmp_path):
    engine, spec = setup_account(tmp_path)
    assert register(engine, spec, 1, -.4, volume=6) == "registered"
    rows = fills(engine)[1:]
    assert [(r["purpose"], r["quantity"]) for r in rows] == [("next_signal_inventory_delta", 2000), ("entry", 1000)]
    mode = engine.state["modes"][spec.market]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == -1000
    assert sum(r["quantity"] for r in rows) == 3000


def test_blocked_reversal_does_not_open_opposite_inventory(tmp_path):
    engine, spec = setup_account(tmp_path)
    register(engine, spec, 1, -.4, volume=2)
    mode = engine.state["modes"][spec.market]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 1000
    assert len(fills(engine)) == 2
    assert fills(engine)[-1]["quantity"] == 1000


def test_missing_price_freezes_inventory_not_simulated_close(tmp_path):
    engine, spec = setup_account(tmp_path)
    register(engine, spec, 1, 0.0, missing=True)
    mode = engine.state["modes"][spec.market]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000
    assert len(fills(engine)) == 1


def test_calendar_accrual_restarts_and_nav_reconcile(tmp_path):
    engine, spec = setup_account(tmp_path)
    before = engine.state["modes"][spec.market]
    at = _now(8, 30) + timedelta(days=4)
    quote = _quote(bid=1010, ask=1010) | {"quote_at": at.isoformat()}
    engine.process_quotes(quotes={"2330": quote}, now=at)
    expected = 2000 * 1000 * .6 * .16 * 4 / 365
    assert before["cumulative_carry_cost_twd"] == pytest.approx(expected)
    p = next(iter(before["positions"].values()))
    assert before["total_equity_twd"] == pytest.approx(10_000_000 + position_net_liquidation_pnl(p, 1010) - expected)
    engine = TwDayTradeSimulationEngine(engine.state_dir)
    engine.process_quotes(quotes={"2330": quote}, now=at)
    mode = engine.state["modes"][spec.market]
    assert mode["cumulative_carry_cost_twd"] == pytest.approx(expected)
    assert sum(r["amount_twd"] for r in mode["carry_cost_ledger"]) == pytest.approx(expected)
    assert len(fills(engine)) == 1  # no stale prior-day bracket


def test_addition_preserves_old_lot_and_exit_capacity_is_shared(tmp_path):
    engine, spec = setup_account(tmp_path)
    register(engine, spec, 1, .4005, volume=100)
    mode = engine.state["modes"][spec.market]
    assert len(mode["positions"]) == 2
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 4000
    at = _now(13, 30) + timedelta(days=1)
    quote = _quote(bid=1000, ask=1000, minute_volume_lots=2) | {"quote_at": at.isoformat()}
    engine.process_quotes(quotes={"2330": quote}, now=at)
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 3000
    assert fills(engine)[-1]["quantity"] == 1000


def test_zero_target_closes_old_inventory_with_cash_tax(tmp_path):
    engine, spec = setup_account(tmp_path)
    register(engine, spec, 1, 0)
    mode = engine.state["modes"][spec.market]
    assert not sum(abs(p["signed_shares"]) for p in mode["positions"].values())
    row = fills(engine)[-1]
    p = next(iter(mode["positions"].values()))
    assert row["gross_fee_and_tax_twd"] == pytest.approx(2000 * 1000 * p["cash_sell_fee_rate"])
    assert mode["total_equity_twd"] == pytest.approx(10_000_000 + row["net_pnl_twd"] - mode["cumulative_carry_cost_twd"])


def test_exact_no_trade_evidence_freezes_only_that_inventory(tmp_path):
    engine, spec = setup_account(tmp_path)
    at = _now(9, 1) + timedelta(days=1)
    summary = _summary("next") | {"generated_at": at.isoformat(), "simulation_replay": True,
        "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE}
    quote = {"open": None, "quote_at": at.isoformat(), "official_session_no_trade_print": True}
    assert engine.register_signal(spec=spec, summary=summary, signal_rows=[_row(0)],
        quotes={"2330": quote}, eligibility=_eligibility(), eligibility_coverage={},
        now=at, counterfactual_open_replay=True) == "registered"
    mode = engine.state["modes"][spec.market]
    assert mode["sizing_nav_carried_price_symbols"] == ["2330"]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 2000
    assert len(fills(engine)) == 1


def test_prior_close_archive_not_rewritten_by_next_session_interest(tmp_path):
    engine, spec = setup_account(tmp_path)
    archive = engine.position_history_dir / "2026-08-13" / "tw_day_trade.json"
    original = archive.read_bytes()
    register(engine, spec, 1, .2002)
    assert archive.read_bytes() == original


def test_completed_opening_close_marks_nav_not_vwap_and_has_source(tmp_path):
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    mode["historical_minute_valuation"] = True
    at = _now(9, 1)
    p = next(iter(mode["positions"].values()))
    engine._mark_mode(spec.market, at, {"2330": {"historical_minute_valuation": True,
        "valuation_price_0901": 1010.0, "quote_at": at.isoformat()}})
    mark = json.loads(engine.marks_path.read_text().splitlines()[-1])
    assert mark["total_equity_twd"] == pytest.approx(10_000_000 + position_net_liquidation_pnl(p, 1010))
    from scripts.rebuild_tw_day_trade_minute_curves import historical_minute_mark_has_source
    assert historical_minute_mark_has_source(mark)


def test_missing_open_without_no_trade_receipt_remains_blocked(tmp_path):
    engine, spec = setup_account(tmp_path)
    at = _now(9, 1) + timedelta(days=1)
    summary = _summary("next") | {"generated_at": at.isoformat(), "simulation_replay": True,
        "entry_fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE}
    assert engine.register_signal(spec=spec, summary=summary, signal_rows=[_row(0)],
        quotes={"2330": {"open": None}}, eligibility=_eligibility(), eligibility_coverage={},
        now=at, counterfactual_open_replay=True) == "waiting_quote"
    assert len(fills(engine)) == 1


def test_carried_exdate_blocks_without_consuming_signal_or_faking_entitlements(tmp_path):
    engine, spec = setup_account(tmp_path)
    action_reference(tmp_path, symbol="2330", ex_date="2026-08-14")
    assert register(engine, spec, 1, .2) == "waiting_margin_corporate_action"
    mode = engine.state["modes"][spec.market]
    assert "signal-1" not in mode["processed_signal_ids"]
    assert mode["margin_corporate_action_receipt"]["events"] == [{"symbol": "2330", "ex_date": "2026-08-14"}]
    original_equity = mode["total_equity_twd"]
    engine.process_quotes(quotes={"2330": _quote(bid=999, ask=999)}, now=_now(9, 2) + timedelta(days=1))
    assert len(fills(engine)) == 1
    assert mode["total_equity_twd"] == original_equity  # explicitly stale, not silently ex-dividend
    assert mode["valuation_complete"] is False
    from stockagent.live.performance_contract import paper_account_performance
    assert paper_account_performance(mode)["status"] == "unavailable"


def test_corporate_action_receipt_mismatch_is_recoverable(tmp_path):
    engine, spec = setup_account(tmp_path)
    path = spec.margin_corporate_action_reference_path.with_suffix(".summary.json")
    receipt = json.loads(path.read_text())
    receipt["output_receipt"]["sha256"] = "bad"
    path.write_text(json.dumps(receipt))
    assert register(engine, spec, 1, .2) == "waiting_margin_corporate_action"
    action_reference(tmp_path)
    assert register(engine, spec, 1, .2) == "registered"


def test_same_day_exdate_purchase_does_not_receive_prior_holder_entitlement(tmp_path):
    engine, spec = setup_account(tmp_path)
    action_reference(tmp_path, symbol="2330", ex_date="2026-08-13")
    assert register(engine, spec, 1, .2) == "registered"


def test_stale_carried_bracket_cannot_execute_from_historical_helper(tmp_path):
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    p = next(iter(mode["positions"].values()))
    p["bracket_prices_current"] = False
    engine._close_position(p, mode, price=900, quote=_quote(), now=_now(13, 30),
                           reason="stop_loss_historical", order_type="MKT", quantity=2000)
    assert len(fills(engine)) == 1


@pytest.mark.parametrize("weight, bid, ask, expected", [(.2, 995, 1000, 995), (-.2, 1000, 1005, 1005)])
def test_live_delta_uses_causally_later_best_quote_not_replay_price(tmp_path, weight, bid, ask, expected):
    engine, spec = setup_account(tmp_path, weight)
    spec = replace(spec, entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK)
    signal_at = _now(9, 0, 0) + timedelta(days=1)
    quote_at = signal_at + timedelta(seconds=1)
    quote = _quote(bid=bid, ask=ask) | {"quote_at": quote_at.isoformat(),
        "bid_volume": 2, "ask_volume": 2, "execution_price_0901": 1234}
    result = engine.register_signal(spec=spec,
        summary=_summary("live-next") | {"generated_at": signal_at.isoformat()},
        signal_rows=[_row(0)], quotes={"2330": quote}, eligibility=_eligibility(),
        eligibility_coverage={}, now=quote_at + timedelta(seconds=1))
    assert result == "registered"
    row = fills(engine)[-1]
    assert row["purpose"] == "next_signal_inventory_delta"
    assert row["price"] == expected
    assert row["quantity"] == 2000


def test_flat_minute_rebuilder_refuses_carried_account_before_writes(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import rebuild_tw_day_trade_minute_curves as rebuild
    path = tmp_path / "marks.jsonl"
    path.write_text(json.dumps({"margin_carry_contract": MARGIN_CARRY_CONTRACT}) + "\n")
    before = path.read_bytes()
    monkeypatch.setattr(rebuild, "parse_args", lambda: SimpleNamespace(
        revalue_opening_marks=False, recompute_existing_strategy_marks=False,
        validate_existing_strategy_marks=False, start_date="2026-08-13", end_date="2026-08-14",
        state_dir=tmp_path, repair_terminal_only=True,
    ))
    with pytest.raises(RuntimeError, match="stateful open-price replay"):
        rebuild.main()
    assert path.read_bytes() == before


def test_margin_contract_rejects_legacy_unbounded_terminal_execution(tmp_path):
    with pytest.raises(ValueError, match="margin carry requires"):
        replace(_spec(tmp_path), residual_margin_conversion=True, realistic_execution=False)


@pytest.mark.parametrize("weight, expected", [(.2, 20_000), (-.2, -20_000)])
def test_signed_cash_claim_precedes_exdate_reduction_and_survives_close_restart(tmp_path, weight, expected):
    engine, spec = setup_account(tmp_path, weight)
    cash_entitlement(tmp_path)
    assert register(engine, spec, 1, 0., price=990) == "registered"
    mode = engine.state["modes"][spec.market]
    claim = mode["corporate_action_ledger"][0]
    assert claim["entitled_signed_shares"] == (2000 if weight > 0 else -2000)
    assert claim["amount_twd"] == expected
    assert claim["paid_at"] is None
    assert mode["cumulative_corporate_action_net_twd"] == expected
    assert mode["corporate_action_cash_net_twd"] == 0
    assert mode["total_equity_twd"] == pytest.approx(10_000_000 + mode["cumulative_realized_net_pnl_twd"]
        + expected - mode["cumulative_carry_cost_twd"])
    old_nav = mode["total_equity_twd"]
    engine = TwDayTradeSimulationEngine(engine.state_dir)
    engine.process_quotes(quotes={}, now=_now(9, 0) + timedelta(days=5))
    mode = engine.state["modes"][spec.market]
    assert mode["corporate_action_cash_net_twd"] == expected
    assert mode["corporate_action_receivable_twd"] == mode["corporate_action_payable_twd"] == 0
    assert len(mode["corporate_action_ledger"]) == 1
    assert mode["total_equity_twd"] == old_nav  # payment is not another profit


def test_cash_claim_is_not_recomputed_from_smaller_remaining_quantity(tmp_path):
    engine, spec = setup_account(tmp_path)
    cash_entitlement(tmp_path)
    register(engine, spec, 1, 0, price=990, volume=2)
    mode = engine.state["modes"][spec.market]
    assert sum(p["signed_shares"] for p in mode["positions"].values()) == 1000
    engine.process_quotes(quotes={}, now=_now(9, 2) + timedelta(days=1))
    assert len(mode["corporate_action_ledger"]) == 1
    assert mode["cumulative_corporate_action_net_twd"] == 20_000


def test_changed_earned_cash_terms_require_reconciliation_not_double_credit(tmp_path):
    engine, spec = setup_account(tmp_path)
    cash_entitlement(tmp_path)
    register(engine, spec, 1, .3, price=990)
    mode = engine.state["modes"][spec.market]
    cash_entitlement(tmp_path, amount=20.)
    engine.process_quotes(quotes={}, now=_now(9, 2) + timedelta(days=1))
    assert mode["engine_status"] == "waiting_margin_corporate_action"
    assert mode["cumulative_corporate_action_net_twd"] == 20_000
