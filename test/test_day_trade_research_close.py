"""Research close stress is separate from fees and from real broker fills."""
import math
from copy import deepcopy

import pytest

from stockagent.live.tw_day_trade_simulation import research_short_limit_up_close_cost


def test_research_cost_only_charges_uncovered_shorts_at_official_limit_up():
    result = research_short_limit_up_close_cost(
        signed_shares=-3_000, official_close=110.0, upper_limit=110.0,
        available_source_capacity_shares=1_000, rate=0.07,
    )
    assert result["source_covered_shares"] == 1_000
    assert result["uncovered_short_shares"] == 2_000
    assert result["amount_twd"] == pytest.approx(15_400.0)
    assert result["simulation_only"] is True
    assert result["broker_fee"] is False


@pytest.mark.parametrize("shares,close,capacity", [
    (2_000, 110.0, None), (0, 110.0, None),
    (-2_000, 109.0, None), (-2_000, 110.0, 2_000),
])
def test_non_short_non_limit_or_fully_covered_never_pays(shares, close, capacity):
    result = research_short_limit_up_close_cost(
        signed_shares=shares, official_close=close, upper_limit=110.0,
        available_source_capacity_shares=capacity, rate=0.07,
    )
    assert result["amount_twd"] == 0.0


def test_unknown_capacity_is_not_fabricated_as_zero():
    with pytest.raises(ValueError, match="source capacity"):
        research_short_limit_up_close_cost(
            signed_shares=-2_000, official_close=110.0, upper_limit=110.0,
            available_source_capacity_shares=None, rate=0.07,
        )


def test_research_expense_cannot_overflow_financial_state():
    with pytest.raises(ValueError, match="finite accounting range"):
        research_short_limit_up_close_cost(
            signed_shares=-(10**400), official_close=110.0, upper_limit=110.0,
            available_source_capacity_shares=0, rate=0.07)


@pytest.mark.parametrize("rate", [-0.01, 1.01, math.nan, True, "0.07"])
def test_research_rate_must_be_a_finite_explicit_numeric_contract(tmp_path, rate):
    from dataclasses import replace
    from test_tw_day_trade_simulation import _spec

    with pytest.raises(ValueError, match="research"):
        replace(_spec(tmp_path), research_short_limit_up_close_penalty_rate=rate)


def test_research_rate_is_frozen_when_intraday_signal_revises(tmp_path):
    from dataclasses import replace
    from stockagent.live.tw_day_trade_simulation import ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TwDayTradeSimulationEngine
    from test_day_trade_margin_carry import register
    from test_tw_day_trade_simulation import _now, _spec

    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        strict_intraday=True, terminal_liquidation_unlimited_capacity=True,
        research_short_limit_up_close_penalty_rate=0.07)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert register(engine, spec, 0, -0.2) == "registered"
    mode = engine.state["modes"][spec.market]
    original_signal = mode["signal_id"]
    revised = replace(spec, research_short_limit_up_close_penalty_rate=0.08)
    status = engine.register_signal(spec=revised, summary={"signal_id": "new-revision"},
        signal_rows=[], quotes={}, eligibility={}, eligibility_coverage={}, now=_now(9, 2))
    assert status == "blocked_research_close_policy"
    assert mode["signal_id"] == original_signal
    assert mode["research_short_limit_up_close_penalty_rate"] == 0.07
    assert mode["configured_research_short_limit_up_close_penalty_rate"] == 0.08


@pytest.mark.parametrize("path", ["quotes", "next_signal"])
def test_research_prior_session_never_liquidates_or_accrues_before_close_reconciliation(
        tmp_path, monkeypatch, path):
    """A next-day price cannot stand in for an unverified research close cost."""
    from dataclasses import replace
    from datetime import timedelta
    from stockagent.live.tw_day_trade_simulation import (
        ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TwDayTradeSimulationEngine,
    )
    from test_day_trade_margin_carry import register
    from test_tw_day_trade_simulation import _now, _spec

    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        strict_intraday=True, terminal_liquidation_unlimited_capacity=True,
        research_short_limit_up_close_penalty_rate=0.07)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert register(engine, spec, 0, -0.2) == "registered"
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    original_shares = position["signed_shares"]
    original_fills = engine.fills_path.read_bytes()
    calls = []
    monkeypatch.setattr(engine, "_margin_corporate_action_gate", lambda *args: True)
    monkeypatch.setattr(engine, "_accrue_margin_carry_cost", lambda *args: calls.append("accrue"))
    monkeypatch.setattr(engine, "_liquidate_prior_inventory", lambda *args: calls.append("liquidate"))

    if path == "quotes":
        engine.process_quotes(quotes={}, now=_now(9, 2) + timedelta(days=1))
    else:
        assert register(engine, spec, 1, -0.2) == "waiting_prior_liquidation"

    assert calls == []
    assert position["signed_shares"] == original_shares
    assert engine.fills_path.read_bytes() == original_fills
    assert mode["engine_status"] == "critical_prior_inventory_liquidation"
    assert mode["research_prior_session_reconciliation_required"] == "2026-08-13"
    restored = TwDayTradeSimulationEngine(engine.state_dir)
    restored_mode = restored.state["modes"][spec.market]
    assert restored_mode["research_prior_session_reconciliation_required"] == "2026-08-13"
    assert next(iter(restored_mode["positions"].values()))["signed_shares"] == original_shares
    assert restored.fills_path.read_bytes() == original_fills


def test_research_prior_direct_liquidation_also_fails_closed(tmp_path):
    from dataclasses import replace
    from datetime import timedelta
    from stockagent.live.tw_day_trade_simulation import (
        ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TwDayTradeSimulationEngine,
    )
    from test_day_trade_margin_carry import register
    from test_tw_day_trade_simulation import _now, _quote, _spec

    spec = replace(_spec(tmp_path), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        strict_intraday=True, terminal_liquidation_unlimited_capacity=True,
        research_short_limit_up_close_penalty_rate=0.07)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    assert register(engine, spec, 0, -0.2) == "registered"
    mode = engine.state["modes"][spec.market]
    before_mode = deepcopy(mode)
    before_fills = engine.fills_path.read_bytes()
    now = _now(9, 2) + timedelta(days=1)
    quote = _quote(ask=1000.0, ask_volume=100.0) | {"quote_at": now.isoformat()}
    engine._liquidate_prior_inventory(mode, {"2330": quote}, now)
    assert next(iter(mode["positions"].values())) == next(iter(before_mode["positions"].values()))
    assert engine.fills_path.read_bytes() == before_fills
    assert mode["research_prior_session_reconciliation_required"] == "2026-08-13"


@pytest.mark.parametrize("overrides", [
    {"official_close": math.nan}, {"upper_limit": None},
    {"official_close": 111.0}, {"rate": -0.1}, {"rate": 1.1},
    {"rate": math.nan}, {"signed_shares": 1.5},
    {"available_source_capacity_shares": -1},
])
def test_invalid_evidence_never_creates_a_research_expense(overrides):
    args = dict(signed_shares=-2_000, official_close=110.0, upper_limit=110.0,
        available_source_capacity_shares=0, rate=0.07)
    with pytest.raises(ValueError):
        research_short_limit_up_close_cost(**(args | overrides))


def _expense_envelope(position, mode, *, volume_shares=0, used_shares=0):
    price = 1100.0
    symbol = position["symbol"]
    day = mode["session_date"]
    available = (volume_shares // 2000) * 1000 - used_shares
    return {
        "official_close_evidence": {"session_date": day, "symbol": symbol,
            "price_basis": "official_session_close", "source": "twse_official_daily_close",
            "source_path": "/fixture/official.json", "source_sha256": "a" * 64, "price": price},
        "upper_limit_evidence": {"session_date": day, "symbol": symbol,
            "source_path": "/fixture/limits.parquet", "source_sha256": "b" * 64, "price": price},
        "closing_capacity_evidence": {"contract": "source_backed_closing_minute_capacity_v1",
            "session_date": day, "symbol": symbol, "minute": f"{day}T13:30:00+08:00",
            "source_path": "/fixture/minute.parquet", "source_sha256": "c" * 64,
            "volume_shares": volume_shares, "used_capacity_shares": used_shares},
        "cost": research_short_limit_up_close_cost(signed_shares=position["signed_shares"],
            official_close=price, upper_limit=price,
            available_source_capacity_shares=available, rate=0.07),
    }


def test_research_expense_is_realized_separately_from_fee_and_not_a_new_fill(tmp_path):
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    original_mode = engine.state["modes"][spec.market]
    position = next(iter(original_mode["positions"].values()))
    position["upper_limit"] = 1100.0
    original_mode["research_short_limit_up_close_penalty_rate"] = 0.07
    baseline_mode = deepcopy(original_mode)
    baseline_mode["research_short_limit_up_close_penalty_rate"] = 0.0
    baseline_position = next(iter(baseline_mode["positions"].values()))
    evidence = _expense_envelope(position, original_mode, volume_shares=2_000)
    quantity = abs(position["signed_shares"])
    baseline_orders, baseline_fills, orders, fills = [], [], [], []
    kwargs = dict(price=1100.0, quote={}, now=_now(13, 30),
        reason="13_30_unlimited_close_paper_settlement", order_type="PAPER_CLOSE_SETTLEMENT",
        quantity=quantity)
    engine._book_position_close(baseline_position, baseline_mode, **kwargs,
        ledger_rows=(baseline_orders, baseline_fills))
    engine._book_position_close(position, original_mode, **kwargs,
        ledger_rows=(orders, fills), research_close_expense=evidence)
    cost = evidence["cost"]["amount_twd"]
    assert len(fills) == len(baseline_fills) == 1
    assert fills[0]["fee_and_tax_twd"] == baseline_fills[0]["fee_and_tax_twd"]
    assert fills[0]["net_pnl_twd"] == pytest.approx(baseline_fills[0]["net_pnl_twd"] - cost)
    assert position["realized_net_pnl_twd"] == pytest.approx(baseline_position["realized_net_pnl_twd"] - cost)
    assert original_mode["cumulative_realized_net_pnl_twd"] == pytest.approx(
        baseline_mode["cumulative_realized_net_pnl_twd"] - cost)
    assert original_mode["cumulative_research_close_expense_twd"] == pytest.approx(cost)
    assert fills[0]["research_close_expense"]["cost"]["broker_fee"] is False


def test_unproven_research_expense_never_mutates_position_or_ledger(tmp_path):
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    evidence = _expense_envelope(position, mode)
    evidence["closing_capacity_evidence"]["source_sha256"] = ""
    before = deepcopy(mode)
    orders, fills = [], []
    with pytest.raises(ValueError, match="source-backed closing capacity"):
        engine._book_position_close(position, mode, price=1100.0, quote={}, now=_now(13, 30),
            reason="13_30_unlimited_close_paper_settlement", order_type="PAPER_CLOSE_SETTLEMENT",
            quantity=abs(position["signed_shares"]), ledger_rows=(orders, fills),
            research_close_expense=evidence)
    assert mode == before and orders == [] and fills == []


def test_research_terminal_short_cannot_silently_skip_its_cost_evidence(tmp_path):
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    before = deepcopy(mode)
    orders, fills = [], []
    with pytest.raises(ValueError, match="requires cost evidence"):
        engine._book_position_close(position, mode, price=1100.0, quote={}, now=_now(13, 30),
            reason="13_30_unlimited_close_paper_settlement", order_type="PAPER_CLOSE_SETTLEMENT",
            quantity=abs(position["signed_shares"]), ledger_rows=(orders, fills))
    assert mode == before and orders == [] and fills == []


def test_close_capacity_uses_exact_integer_floor_even_above_float_precision(tmp_path):
    from stockagent.live.tw_day_trade_simulation import _research_close_expense_amount
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    volume = 2 * 1000 * (2**53 // 2000 + 1) - 1
    used = volume // 2000 * 1000
    evidence = _expense_envelope(position, mode, volume_shares=volume, used_shares=used)
    cost = _research_close_expense_amount(position=position, mode=mode, price=1100.0,
        now=_now(13, 30), reason="13_30_unlimited_close_paper_settlement",
        order_type="PAPER_CLOSE_SETTLEMENT", quantity=abs(position["signed_shares"]),
        evidence=evidence)
    assert cost == pytest.approx(abs(position["signed_shares"]) * 1100 * 0.07)


@pytest.mark.parametrize("source_key", ["official_close_evidence", "upper_limit_evidence"])
def test_claimed_hash_without_source_path_cannot_enter_ledger(tmp_path, source_key):
    from stockagent.live.tw_day_trade_simulation import _research_close_expense_amount
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    evidence = _expense_envelope(position, mode)
    del evidence[source_key]["source_path"]
    with pytest.raises(ValueError, match="evidence"):
        _research_close_expense_amount(position=position, mode=mode, price=1100.0,
            now=_now(13, 30), reason="13_30_unlimited_close_paper_settlement",
            order_type="PAPER_CLOSE_SETTLEMENT", quantity=abs(position["signed_shares"]),
            evidence=evidence)


def test_non_limit_close_does_not_wait_for_irrelevant_closing_volume(tmp_path):
    from stockagent.live.tw_day_trade_simulation import _research_close_expense_amount
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    evidence = _expense_envelope(position, mode)
    evidence["official_close_evidence"]["price"] = 1095.0
    evidence["closing_capacity_evidence"] = None
    evidence["cost"] = research_short_limit_up_close_cost(
        signed_shares=position["signed_shares"], official_close=1095.0,
        upper_limit=1100.0, available_source_capacity_shares=None, rate=0.07)
    assert _research_close_expense_amount(position=position, mode=mode, price=1095.0,
        now=_now(13, 30), reason="13_30_unlimited_close_paper_settlement",
        order_type="PAPER_CLOSE_SETTLEMENT", quantity=abs(position["signed_shares"]),
        evidence=evidence) == 0.0


def test_research_cost_matches_authoritative_training_after_partial_close():
    from dataclasses import replace

    from test_tw_day_trade_carry import run, session, v

    original = session(volume=6000, exits=False)
    prices, capacity = original.exit_prices.clone(), original.exit_capacity.clone()
    prices[0, 269, 1], capacity[0, 269, 1] = 1100.0, 1000.0
    baseline_session = replace(original, exit_prices=prices, exit_capacity=capacity,
        terminal_liquidation_price=v(1100.0))
    stressed_session = replace(baseline_session, short_limit_up_penalty_per_share=v(77.0))
    weights = v(-0.4).reshape(1, 1)
    baseline = run(weights, [baseline_session], fees=False)
    stressed = run(weights, [stressed_session], fees=False)
    calculated = research_short_limit_up_close_cost(signed_shares=-3000,
        official_close=1100.0, upper_limit=1100.0,
        available_source_capacity_shares=1000, rate=0.07)
    assert (baseline.final_state.last_nav - stressed.final_state.last_nav).item() == pytest.approx(
        calculated["amount_twd"])


def _source_verified_close_fixture(tmp_path):
    from datetime import date, datetime
    import hashlib
    import json

    import polars as pl

    from scripts.settle_tw_day_trade_official_close import official_closes
    from test_day_trade_official_close_settlement import reports

    day, symbol = "2026-08-13", "2330"
    raw = reports(tmp_path, close="1100", day=day, volume="10000")
    _, sources = official_closes(raw, date.fromisoformat(day))
    limit_path = tmp_path / "limits.parquet"
    pl.DataFrame({"symbol": [symbol], "upper_limit_price": [1100.0]}).write_parquet(limit_path)
    minute_path = tmp_path / "research_dataset" / f"trade_date={day}" / "data.parquet"
    minute_path.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": [symbol], "ts": [datetime(2026, 8, 13, 13, 30)],
        "Open": [1100.0], "High": [1100.0], "Low": [1100.0], "Close": [1100.0],
        "Amount": [2_200_000.0], "volume_shares": [2000]}).write_parquet(minute_path)
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    manifest_path = minute_path.parent.parent / "manifest.json"
    manifest_path.write_text(json.dumps({"partitions": [{"status": "ok", "trade_date": day,
        "output": f"trade_date={day}/data.parquet", "output_sha256": digest(minute_path)}]}))
    position = {"symbol": symbol, "signed_shares": -3000, "upper_limit": 1100.0}
    mode = {"session_date": day}
    envelope = _expense_envelope(position, mode, volume_shares=2000)
    envelope["official_close_evidence"].update(source_path=sources[0]["path"],
        source_sha256=sources[0]["sha256"])
    envelope["upper_limit_evidence"].update(source_path=str(limit_path), source_sha256=digest(limit_path))
    envelope["closing_capacity_evidence"].update(source_path=str(minute_path),
        source_sha256=digest(minute_path))
    fill = {"session_date": day, "symbol": symbol, "quantity": 3000, "side": "buy_to_cover",
        "price": 1100.0, "fill_at": f"{day}T13:30:00+08:00",
        "purpose": "13_30_unlimited_close_paper_settlement", "research_close_expense": envelope}
    session = {"session_date": day, "close": {"terminal_close_sources": sources},
        "price_limit_path": str(limit_path), "price_limit_sha256": digest(limit_path),
        "intraday_replay": {"source_files": [{"path": str(minute_path), "sha256": digest(minute_path),
                                                "symbols": [symbol]}]}}
    return fill, session, minute_path, limit_path


def test_research_cost_independently_reads_pinned_official_limit_and_minute_sources(tmp_path):
    from scripts.audit_tw_day_trade_margin_replay import _verify_research_close_source

    fill, session, _, _ = _source_verified_close_fixture(tmp_path)
    cost, hashes = _verify_research_close_source(fill, session, rate=0.07, expected_used=0)
    assert cost == pytest.approx(2000 * 1100 * 0.07)
    assert len(hashes) == 4  # official close, limits, minute partition and its receipt


@pytest.mark.parametrize("tamper", [False, True])
def test_research_cost_rechecks_private_finmind_projection_and_native_pins(tmp_path, monkeypatch, tamper):
    import json
    import polars as pl

    from scripts import prepare_finmind_tw_day_trade_repair_source as source
    from scripts.audit_tw_day_trade_margin_replay import _verify_research_close_source

    fill, session, minute_path, _ = _source_verified_close_fixture(tmp_path)
    origin = tmp_path / "native-provider-object.parquet"
    origin.write_bytes(b"original-provider-evidence")
    origin_pin = source._pin(origin)
    summary = {"requested_symbols": 1, "missing_minutes_are_unknown_not_zero": True}
    manifest = {"contract": source.CONTRACT, "session_date": "2026-08-13",
        "simulation_only": True, "production_order_possible": False, "public_export_authorized": False,
        "finmind_root": str(tmp_path), "public_root": str(tmp_path), "inventory_root": str(tmp_path),
        "output": source._pin(minute_path), "source_pins": [origin_pin], "summary": summary}
    (minute_path.parent.parent / source.MANIFEST).write_text(json.dumps(manifest))
    monkeypatch.setattr(source, "_inputs", lambda *args, **kwargs:
                        (pl.read_parquet(minute_path), summary, [origin_pin]))
    if tamper:
        origin.write_bytes(b"modified-provider-evidence")
        with pytest.raises(RuntimeError, match="pinned replay input changed"):
            _verify_research_close_source(fill, session, rate=0.07, expected_used=0)
    else:
        cost, hashes = _verify_research_close_source(fill, session, rate=0.07, expected_used=0)
        assert cost == pytest.approx(154000.0)
        assert hashes[str(origin)] == origin_pin["sha256"]
        assert str(minute_path.parent.parent / source.MANIFEST) in hashes


@pytest.mark.parametrize("tamper", ["volume", "limit", "missing_symbol_pin", "used", "missing_bar"])
def test_research_cost_source_audit_refuses_tampering(tmp_path, tamper):
    import polars as pl

    from scripts.audit_tw_day_trade_margin_replay import _verify_research_close_source

    fill, session, minute_path, limit_path = _source_verified_close_fixture(tmp_path)
    if tamper == "volume":
        fill["research_close_expense"]["closing_capacity_evidence"]["volume_shares"] = 1000
    elif tamper == "limit":
        pl.DataFrame({"symbol": ["2330"], "upper_limit_price": [1095.0]}).write_parquet(limit_path)
    elif tamper == "missing_symbol_pin":
        session["intraday_replay"]["source_files"][0]["symbols"] = []
    elif tamper == "used":
        fill["research_close_expense"]["closing_capacity_evidence"]["used_capacity_shares"] = 1000
    else:
        minute_path.unlink()
    with pytest.raises(ValueError, match="research close|research closing"):
        _verify_research_close_source(fill, session, rate=0.07, expected_used=0)


def _research_cost_ledger_fixture(tmp_path):
    fill, session, _, _ = _source_verified_close_fixture(tmp_path)
    market, day = "new_2m", fill["session_date"]
    fill.update(market=market, position_id="p1", order_id="close1")
    entry = {"market": market, "session_date": day, "symbol": "2330", "position_id": "p1",
             "purpose": "entry", "side": "sell_short", "fill_at": f"{day}T09:01:00+08:00"}
    session["modes"] = [{"market": market, "after_close": {
        "research_short_limit_up_close_penalty_rate": 0.07}}]
    rebuild = {"replay_contract": {"research_close_penalty_by_market_v1": {market: 0.07}},
               "sessions": [session]}
    modes = {market: {"research_short_limit_up_close_penalty_rate": 0.07,
                      "cumulative_research_close_expense_twd": 2000 * 1100 * 0.07}}
    return [entry, fill], rebuild, modes


def test_research_cost_audit_reconciles_shared_source_with_ledger(tmp_path):
    from scripts.audit_tw_day_trade_margin_replay import _verify_research_close_costs

    fills, rebuild, modes = _research_cost_ledger_fixture(tmp_path)
    costs, sources = _verify_research_close_costs(fills, rebuild, modes, verify_sources=True)
    assert costs == {("new_2m", "close1"): pytest.approx(154000.0)}
    assert len(sources) == 4


def test_research_cost_audit_uses_capacity_once_across_fifo_cohorts(tmp_path, monkeypatch):
    from collections import Counter
    from scripts import audit_tw_day_trade_margin_replay as audit_module

    _verify_research_close_costs = audit_module._verify_research_close_costs

    fills, rebuild, modes = _research_cost_ledger_fixture(tmp_path)
    first = fills[1]
    second = deepcopy(first)
    second.update(position_id="p2", order_id="close2", quantity=1000)
    evidence = second["research_close_expense"]
    evidence["closing_capacity_evidence"]["used_capacity_shares"] = 1000
    evidence["cost"] = research_short_limit_up_close_cost(signed_shares=-1000,
        official_close=1100.0, upper_limit=1100.0, available_source_capacity_shares=0, rate=0.07)
    entry = dict(fills[0], position_id="p2", fill_at="2026-08-13T09:02:00+08:00")
    fills.insert(1, entry)
    fills.append(second)
    modes["new_2m"]["cumulative_research_close_expense_twd"] += 1000 * 1100 * 0.07
    calls = Counter()
    original_sha256 = audit_module._sha256

    def counted_sha256(path):
        calls[str(path)] += 1
        return original_sha256(path)

    monkeypatch.setattr(audit_module, "_sha256", counted_sha256)
    costs, _ = _verify_research_close_costs(fills, rebuild, modes, verify_sources=True)
    assert costs[("new_2m", "close1")] == pytest.approx(154000.0)
    assert costs[("new_2m", "close2")] == pytest.approx(77000.0)
    assert calls and max(calls.values()) == 1  # full audit rechecks hashes at its end
    reversed_closes = fills[:2] + [fills[3], fills[2]]
    with pytest.raises(ValueError, match="FIFO"):
        _verify_research_close_costs(reversed_closes, rebuild, modes, verify_sources=True)


@pytest.mark.parametrize("defect", ["unverified", "omitted", "wrong_market", "wrong_total", "wrong_rate", "wrong_side", "wrong_entry_clock", "state_rate"])
def test_research_cost_audit_never_accepts_unproven_or_missing_charge(tmp_path, defect):
    from scripts.audit_tw_day_trade_margin_replay import _verify_research_close_costs

    fills, rebuild, modes = _research_cost_ledger_fixture(tmp_path)
    if defect == "omitted":
        del fills[1]["research_close_expense"]
    elif defect == "wrong_market":
        rebuild["replay_contract"]["research_close_penalty_by_market_v1"] = {}
    elif defect == "wrong_total":
        modes["new_2m"]["cumulative_research_close_expense_twd"] = 0.0
    elif defect == "wrong_rate":
        rebuild["sessions"][0]["modes"][0]["after_close"]["research_short_limit_up_close_penalty_rate"] = 0.0
    elif defect == "wrong_side":
        fills[0]["side"] = "buy"
    elif defect == "wrong_entry_clock":
        fills[0]["fill_at"] = "2026-08-13T13:30:00+08:00"
    elif defect == "state_rate":
        modes["new_2m"]["research_short_limit_up_close_penalty_rate"] = 0.0
    with pytest.raises(ValueError, match="research"):
        _verify_research_close_costs(fills, rebuild, modes, verify_sources=defect != "unverified")


def _terminal_commit_fixture(tmp_path):
    from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    expense = _expense_envelope(position, mode, volume_shares=2000)
    effective, recorded = _now(13, 30), _now(13, 35)
    receipt = {"contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT,
        "source_evidence": deepcopy(expense["official_close_evidence"]),
        "recorded_at": recorded.isoformat(), "effective_at": effective.isoformat(),
        "broker_fill": False, "full_quantity_is_user_assumption": True}
    args = dict(price=1100.0, effective_at=effective, recorded_at=recorded,
                receipt=receipt, expense=expense)
    return engine, spec, mode, position, args


def test_research_terminal_commit_records_exactly_once_and_restarts(tmp_path):
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, mode, position, args = _terminal_commit_fixture(tmp_path)
    engine._commit_research_terminal_close(mode, position, **args)
    fills = engine.fills_path.read_bytes()
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    restored = restarted.state["modes"][spec.market]
    assert not restored.get("research_terminal_pending_commit")
    assert not restored.get("ledger_state_divergence")
    assert restored["terminal_flatten_count"] == 1
    assert restored["open_position_count"] == 0
    assert restored["total_equity_twd"] == pytest.approx(
        restored["initial_capital_twd"] + restored["cumulative_realized_net_pnl_twd"])
    assert engine.fills_path.read_bytes() == fills
    with pytest.raises(ValueError, match="terminal close intent"):
        engine._commit_research_terminal_close(mode, position, **args)
    assert engine.fills_path.read_bytes() == fills


def test_research_terminal_long_has_same_durable_commit_without_short_cost(tmp_path):
    from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path, weight=0.2)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    assert position["signed_shares"] > 0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    effective, recorded = _now(13, 30), _now(13, 35)
    receipt = {"contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT,
        "source_evidence": {"session_date": mode["session_date"], "price": 1100.0,
            "price_basis": "official_session_close", "source": "twse_official_daily_close",
            "source_sha256": "a" * 64},
        "recorded_at": recorded.isoformat(), "effective_at": effective.isoformat(),
        "broker_fill": False, "full_quantity_is_user_assumption": True}
    engine._commit_research_terminal_close(mode, position, price=1100.0,
        effective_at=effective, recorded_at=recorded, receipt=receipt, expense=None)
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert not mode.get("cumulative_research_close_expense_twd")


@pytest.mark.parametrize("mismatch", ["market", "session", "source", "clock"])
def test_research_terminal_rejects_inconsistent_intent_before_journal_write(tmp_path, mismatch):
    engine, _, mode, position, args = _terminal_commit_fixture(tmp_path)
    if mismatch == "market":
        position["market"] = "different_market"
    elif mismatch == "session":
        position["session_date"] = "2026-08-12"
    elif mismatch == "source":
        args["receipt"]["source_evidence"]["source_sha256"] = "f" * 64
    else:
        args["receipt"]["effective_at"] = "2026-08-13T13:29:00+08:00"
    before, fills = deepcopy(mode), engine.fills_path.read_bytes()
    with pytest.raises(ValueError, match="research"):
        engine._commit_research_terminal_close(mode, position, **args)
    assert mode == before and engine.fills_path.read_bytes() == fills


@pytest.mark.parametrize("failure", ["before_order", "after_order", "after_fill", "duplicate_order"])
def test_research_terminal_crash_never_double_books(tmp_path, monkeypatch, failure):
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, mode, position, args = _terminal_commit_fixture(tmp_path)
    original_quantity = position["signed_shares"]
    if failure == "before_order":
        monkeypatch.setattr(engine, "_order", lambda row: (_ for _ in ()).throw(OSError("injected")))
    elif failure == "after_order":
        monkeypatch.setattr(engine, "_fill", lambda row: (_ for _ in ()).throw(OSError("injected")))
    else:
        persist = engine._persist
        calls = 0

        def fail_final_persist(now):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected")
            persist(now)

        monkeypatch.setattr(engine, "_persist", fail_final_persist)
    with pytest.raises(OSError, match="injected"):
        engine._commit_research_terminal_close(mode, position, **args)
    if failure == "duplicate_order":
        engine._order(mode["research_terminal_pending_commit"]["order"])
    fills_before_restart = engine.fills_path.read_bytes()
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    restored = restarted.state["modes"][spec.market]
    restored_position = next(iter(restored["positions"].values()))
    assert engine.fills_path.read_bytes() == fills_before_restart
    if failure == "before_order":
        assert not restored.get("research_terminal_pending_commit")
        assert not restored.get("ledger_state_divergence")
        assert restored_position["signed_shares"] == original_quantity
    elif failure == "after_fill":
        assert not restored.get("research_terminal_pending_commit")
        assert not restored.get("ledger_state_divergence")
        assert restored_position["signed_shares"] == 0
        assert restored["terminal_flatten_count"] == 1
        assert restored["open_position_count"] == 0
        assert restored["total_equity_twd"] == pytest.approx(
            restored["initial_capital_twd"] + restored["cumulative_realized_net_pnl_twd"])
        assert restored["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
        again = TwDayTradeSimulationEngine(engine.state_dir).state["modes"][spec.market]
        assert again["terminal_flatten_count"] == 1
    else:
        assert restored.get("research_terminal_pending_commit")
        assert restored["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
        assert restored_position["signed_shares"] == original_quantity


def test_research_terminal_rejects_reusing_the_same_capacity_for_another_cohort(tmp_path):
    engine, _, mode, position, args = _terminal_commit_fixture(tmp_path)
    second = deepcopy(position)
    second["position_id"] += ":second"
    mode["positions"][second["position_id"]] = second
    engine._commit_research_terminal_close(mode, position, **args)
    before = deepcopy(mode)
    with pytest.raises(ValueError, match="already consumed"):
        engine._commit_research_terminal_close(mode, second, **args)
    assert mode == before


@pytest.mark.parametrize("damage", ["torn_tail", "invalid_json", "changed_rate", "missing_orders", "missing_fills"])
def test_research_terminal_recovery_quarantines_corruption(tmp_path, monkeypatch, damage):
    import json

    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, mode, position, args = _terminal_commit_fixture(tmp_path)
    persist, calls = engine._persist, 0

    def fail_final_persist(now):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected")
        persist(now)

    monkeypatch.setattr(engine, "_persist", fail_final_persist)
    with pytest.raises(OSError):
        engine._commit_research_terminal_close(mode, position, **args)
    if damage == "changed_rate":
        state = json.loads(engine.state_path.read_text())
        state["modes"][spec.market]["research_short_limit_up_close_penalty_rate"] = 0.05
        engine.state_path.write_text(json.dumps(state))
    elif damage in {"missing_orders", "missing_fills"}:
        (engine.orders_path if damage == "missing_orders" else engine.fills_path).unlink()
    else:
        with engine.fills_path.open("a") as stream:
            stream.write('{"partial":' if damage == "torn_tail" else 'not-json\n')
    restored = TwDayTradeSimulationEngine(engine.state_dir).state["modes"][spec.market]
    assert restored["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
    assert restored.get("research_terminal_pending_commit")
    assert next(iter(restored["positions"].values()))["signed_shares"] < 0


def test_research_terminal_recovery_supports_immutable_journals_and_mode_snapshots(tmp_path, monkeypatch):
    from stockagent.live import paper_journal
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    initial, spec, _, _, args = _terminal_commit_fixture(tmp_path)
    initial._persist(args["effective_at"])
    for path in (initial.orders_path, initial.fills_path):
        paper_journal.publish_snapshot(path, operation="seed")
    engine = TwDayTradeSimulationEngine(initial.state_dir, immutable_mode_snapshots=True)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    persist, calls = engine._persist, 0

    def fail_final_persist(now):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected")
        persist(now)

    monkeypatch.setattr(engine, "_persist", fail_final_persist)
    with pytest.raises(OSError):
        engine._commit_research_terminal_close(mode, position, **args)
    restarted = TwDayTradeSimulationEngine(engine.state_dir, immutable_mode_snapshots=True)
    restored = restarted.state["modes"][spec.market]
    assert not restored.get("ledger_state_divergence")
    assert not restored.get("research_terminal_pending_commit")
    assert restored["open_position_count"] == 0
    assert restored["terminal_flatten_count"] == 1
    assert restored["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)


def _engine_source_quotes(tmp_path):
    from test_day_trade_terminal_close import setup

    engine, spec = setup(tmp_path)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    fill, context, _, _ = _source_verified_close_fixture(tmp_path)
    envelope = fill["research_close_expense"]
    quote = {"session_close_evidence": envelope["official_close_evidence"],
             "research_terminal_source_context": context,
             "research_closing_minute_evidence": envelope["closing_capacity_evidence"]}
    return engine, spec, mode, position, {"2330": quote}


def _managed_interrupted_research_close(tmp_path, monkeypatch, *, before_order=False, managed_events=False):
    from stockagent.live import paper_journal
    from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT
    from test_tw_day_trade_simulation import _now

    engine, spec, mode, position, quotes = _engine_source_quotes(tmp_path)
    paths = (engine.orders_path, engine.fills_path, engine.events_path) if managed_events else (engine.orders_path, engine.fills_path)
    for path in paths:
        paper_journal.publish_snapshot(path, operation="seed")
    effective, recorded = _now(13, 30), _now(13, 35)
    quote = quotes["2330"]
    proof = quote["session_close_evidence"]
    expense = engine._research_terminal_expense(mode, position, price=1100.0,
        proof=proof, quote=quote, effective_at=effective)
    receipt = {"contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT,
        "source_evidence": deepcopy(proof), "recorded_at": recorded.isoformat(),
        "effective_at": effective.isoformat(), "broker_fill": False,
        "full_quantity_is_user_assumption": True}
    if before_order:
        monkeypatch.setattr(engine, "_order", lambda row: (_ for _ in ()).throw(OSError("injected")))
    else:
        monkeypatch.setattr(engine, "_fill", lambda row: (_ for _ in ()).throw(OSError("injected")))
    with pytest.raises(OSError, match="injected"):
        engine._commit_research_terminal_close(mode, position, price=1100.0,
            effective_at=effective, recorded_at=recorded, receipt=receipt, expense=expense)
    marker = mode["research_terminal_pending_commit"]
    assert marker["contract"] == "research_terminal_paper_commit_managed_v2"
    assert position["signed_shares"] < 0
    return engine, spec, quotes


@pytest.mark.parametrize("managed_events", [False, True])
def test_managed_research_half_commit_repairs_only_with_owner_and_exact_source(tmp_path, monkeypatch, managed_events):
    import json
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, _ = _managed_interrupted_research_close(tmp_path, monkeypatch, managed_events=managed_events)
    before = engine.fills_path.read_bytes()
    # Read-only/offline constructors do not create a missing financial row.
    inspected = TwDayTradeSimulationEngine(engine.state_dir)
    assert inspected.state["modes"][spec.market]["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
    assert engine.fills_path.read_bytes() == before
    restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    mode = restored.state["modes"][spec.market]
    assert not mode.get("ledger_state_divergence")
    assert not mode.get("research_terminal_pending_commit")
    assert mode["terminal_flatten_count"] == 1 and mode["open_position_count"] == 0
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
    assert mode["research_terminal_recovery"]["source_reverified"] is True
    assert mode["research_terminal_recovery"]["broker_fill"] is False
    assert mode["research_terminal_recovery"]["intent_recorded_at"] == "2026-08-13T13:35:00+08:00"
    events = [json.loads(line) for line in engine.events_path.read_text().splitlines()]
    attempt = next(row for row in events if row["event"] == "research_terminal_paper_commit_recovery_attempt")
    assert attempt["intent_recorded_at"] == "2026-08-13T13:35:00+08:00"
    assert attempt["recorded_at"] != attempt["intent_recorded_at"]
    assert attempt["outcome"] == "attempting_missing_paper_settlement_row"
    assert len(engine.fills_path.read_bytes().splitlines()) == len(before.splitlines()) + 1
    assert mode["total_equity_twd"] == pytest.approx(
        mode["initial_capital_twd"] + mode["cumulative_realized_net_pnl_twd"])
    after = engine.fills_path.read_bytes()
    again = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    assert again.state["modes"][spec.market]["terminal_flatten_count"] == 1
    assert engine.fills_path.read_bytes() == after


@pytest.mark.parametrize("damage", ["source", "foreign_order", "missing_owner"])
def test_managed_research_half_commit_quarantines_unproven_repair(tmp_path, monkeypatch, damage):
    from stockagent.live.paper_journal import append_paper_journal
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    from stockagent.private_json import ledger_lines_bytes

    engine, spec, quotes = _managed_interrupted_research_close(tmp_path, monkeypatch)
    if damage == "source":
        from pathlib import Path
        Path(quotes["2330"]["research_terminal_source_context"]["price_limit_path"]).write_bytes(b"changed")
    elif damage == "foreign_order":
        append_paper_journal(engine.orders_path, ledger_lines_bytes(({"order_id": "foreign"},)))
    before = engine.fills_path.read_bytes()
    restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=damage != "missing_owner")
    mode = restored.state["modes"][spec.market]
    assert mode["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
    assert mode["research_terminal_pending_commit"]
    assert next(iter(mode["positions"].values()))["signed_shares"] < 0
    assert engine.fills_path.read_bytes() == before


@pytest.mark.parametrize("source", ["official", "changed_official", "auction_only"])
def test_managed_research_long_repair_requires_retained_official_trade(tmp_path, monkeypatch, source):
    from stockagent.live import paper_journal
    from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT, TwDayTradeSimulationEngine
    from test_day_trade_terminal_close import setup
    from test_tw_day_trade_simulation import _now

    engine, spec = setup(tmp_path, weight=0.2)
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    mode["research_short_limit_up_close_penalty_rate"] = 0.07
    fill, _, _, _ = _source_verified_close_fixture(tmp_path)
    proof = deepcopy(fill["research_close_expense"]["official_close_evidence"])
    effective, recorded = _now(13, 30), _now(13, 35)
    if source == "auction_only":
        proof = {"session_date": mode["session_date"], "price": 1100.0,
            "price_basis": "observed_regular_closing_auction", "source": "exchange_non_trial_tick",
            "observed_trade_volume_lots": 1, "exchange_quote_at": effective.isoformat(),
            "received_at": effective.isoformat()}
    receipt = {"contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT, "source_evidence": proof,
        "recorded_at": recorded.isoformat(), "effective_at": effective.isoformat(),
        "broker_fill": False, "full_quantity_is_user_assumption": True}
    for path in (engine.orders_path, engine.fills_path):
        paper_journal.publish_snapshot(path, operation="seed")
    monkeypatch.setattr(engine, "_fill", lambda row: (_ for _ in ()).throw(OSError("injected")))
    with pytest.raises(OSError, match="injected"):
        engine._commit_research_terminal_close(mode, position, price=1100.0,
            effective_at=effective, recorded_at=recorded, receipt=receipt, expense=None)
    before = engine.fills_path.read_bytes()
    if source == "changed_official":
        from pathlib import Path
        Path(proof["source_path"]).write_bytes(b"changed")
    restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    recovered = restored.state["modes"][spec.market]
    if source == "official":
        assert not recovered.get("ledger_state_divergence")
        assert recovered["terminal_flatten_count"] == 1 and recovered["open_position_count"] == 0
        assert not recovered.get("cumulative_research_close_expense_twd")
        assert len(engine.fills_path.read_bytes().splitlines()) == len(before.splitlines()) + 1
    else:
        assert recovered["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
        assert next(iter(recovered["positions"].values()))["signed_shares"] > 0
        assert engine.fills_path.read_bytes() == before


def test_managed_research_no_written_rows_remains_a_source_retry(tmp_path, monkeypatch):
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, _ = _managed_interrupted_research_close(tmp_path, monkeypatch, before_order=True)
    before_orders, before_fills = engine.orders_path.read_bytes(), engine.fills_path.read_bytes()
    restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    mode = restored.state["modes"][spec.market]
    assert not mode.get("research_terminal_pending_commit")
    assert not mode.get("ledger_state_divergence")
    assert next(iter(mode["positions"].values()))["signed_shares"] < 0
    assert engine.orders_path.read_bytes() == before_orders
    assert engine.fills_path.read_bytes() == before_fills


def test_managed_research_repair_interrupted_after_append_never_double_books(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_simulation as simulation
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, _ = _managed_interrupted_research_close(tmp_path, monkeypatch)
    original = simulation.append_paper_journal

    def append_then_fail(path, body, **kwargs):
        result = original(path, body, **kwargs)
        if kwargs.get("expected_parent") is not None:
            raise OSError("injected after append")
        return result

    monkeypatch.setattr(simulation, "append_paper_journal", append_then_fail)
    first = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    assert first.state["modes"][spec.market]["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
    after = engine.fills_path.read_bytes()
    monkeypatch.setattr(simulation, "append_paper_journal", original)
    recovered = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    mode = recovered.state["modes"][spec.market]
    assert not mode.get("ledger_state_divergence")
    assert mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
    assert engine.fills_path.read_bytes() == after


def test_managed_research_repair_rejects_a_race_at_the_parent_compare(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_simulation as simulation
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, _ = _managed_interrupted_research_close(tmp_path, monkeypatch)
    before = engine.fills_path.read_bytes()
    original = simulation.append_paper_journal
    foreign = b'{"order_id":"foreign"}\n'

    def racing_append(path, body, **kwargs):
        if kwargs.get("expected_parent") is not None:
            original(path, foreign)
        return original(path, body, **kwargs)

    monkeypatch.setattr(simulation, "append_paper_journal", racing_append)
    restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
    mode = restored.state["modes"][spec.market]
    assert mode["ledger_state_divergence"]["kind"] == "research_terminal_commit_interrupted"
    assert mode["research_terminal_pending_commit"]
    assert engine.fills_path.read_bytes() == before + foreign
    assert next(iter(mode["positions"].values()))["signed_shares"] < 0


def test_managed_research_native_owner_repair_holds_the_engine_lease(tmp_path, monkeypatch):
    from scripts.run_tw_day_trade_simulation import _acquire_engine_lock
    from stockagent.live.paper_journal import recover_pending_journal_heads
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine

    engine, spec, _ = _managed_interrupted_research_close(tmp_path, monkeypatch, managed_events=True)
    lease = _acquire_engine_lock(engine.state_dir)
    try:
        with pytest.raises(RuntimeError, match="already has a live writer"):
            _acquire_engine_lock(engine.state_dir)
        assert recover_pending_journal_heads(engine.state_dir) == []
        restored = TwDayTradeSimulationEngine(engine.state_dir, allow_research_commit_repair=True)
        mode = restored.state["modes"][spec.market]
        assert not mode.get("ledger_state_divergence")
        assert mode["research_terminal_recovery"]["broker_fill"] is False
        assert mode["terminal_flatten_count"] == 1
    finally:
        lease.close()


def test_research_terminal_engine_reads_real_sources_and_commits_only_once(tmp_path):
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    from test_tw_day_trade_simulation import _now

    engine, spec, mode, position, quotes = _engine_source_quotes(tmp_path)
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
    assert mode["closing_auction_pending_count"] == 0
    fills = engine.fills_path.read_bytes()
    restarted = TwDayTradeSimulationEngine(engine.state_dir)
    restarted.process_quotes(quotes=quotes, now=_now(13, 36))
    assert restarted.state["modes"][spec.market]["terminal_flatten_count"] == 1
    assert engine.fills_path.read_bytes() == fills


@pytest.mark.parametrize("missing", ["context", "minute", "official_path", "changed_limit"])
def test_research_terminal_engine_waits_for_source_instead_of_fabricating_cost(tmp_path, missing):
    import polars as pl
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    quote = quotes["2330"]
    if missing == "context":
        del quote["research_terminal_source_context"]
    elif missing == "minute":
        del quote["research_closing_minute_evidence"]
    elif missing == "official_path":
        del quote["session_close_evidence"]["source_path"]
    else:
        limit_path = quote["research_terminal_source_context"]["price_limit_path"]
        pl.DataFrame({"symbol": ["2330"], "upper_limit_price": [1095.0]}).write_parquet(limit_path)
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] < 0
    assert mode["engine_status"] == "waiting_research_close_evidence"
    assert mode["research_close_pending_source_count"] == 1
    assert engine.fills_path.read_bytes() == before


def test_research_terminal_retries_after_missing_minute_arrives_without_double_booking(tmp_path):
    import json
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    minute = quotes["2330"].pop("research_closing_minute_evidence")
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] < 0 and mode["research_close_pending_source_count"] == 1
    quotes["2330"]["research_closing_minute_evidence"] = minute
    engine.process_quotes(quotes=quotes, now=_now(13, 36))
    assert position["signed_shares"] == 0 and mode["research_close_pending_source_count"] == 0
    terminal = [json.loads(line) for line in engine.fills_path.read_text().splitlines()
                if "13_30_unlimited_close_paper_settlement" in line]
    assert len(terminal) == 1
    assert terminal[0]["fill_at"] == _now(13, 30).isoformat()
    assert terminal[0]["recorded_at"] == _now(13, 36).isoformat()
    engine.process_quotes(quotes=quotes, now=_now(13, 37))
    assert mode["terminal_flatten_count"] == 1


def test_research_terminal_mixed_directions_share_no_unproven_capacity(tmp_path):
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    opposite = deepcopy(position)
    opposite.update(position_id=position["position_id"] + ":opposite", signed_shares=1000, side="long")
    mode["positions"][opposite["position_id"]] = opposite
    before = engine.fills_path.read_bytes()
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] < 0 and opposite["signed_shares"] > 0
    assert mode["research_close_pending_source_count"] == 2
    assert engine.fills_path.read_bytes() == before


@pytest.mark.parametrize("damage", ["direction", "market", "quantity", "identity"])
def test_research_corrupt_position_is_quarantined_before_any_order(tmp_path, damage):
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    if damage == "direction":
        position["side"] = "long"
    elif damage == "market":
        position["market"] = "other"
    elif damage == "quantity":
        position["signed_shares"] = -2000.5
    else:
        position["position_id"] = "unexpected"
    before_orders, before_fills = engine.orders_path.read_bytes(), engine.fills_path.read_bytes()
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert mode["ledger_state_divergence"]["kind"] == "research_terminal_position_identity_mismatch"
    assert mode["engine_status"] == "critical_ledger_state_divergence"
    assert engine.orders_path.read_bytes() == before_orders and engine.fills_path.read_bytes() == before_fills


def test_research_terminal_local_source_builder_uses_retained_minute_without_network(tmp_path):
    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from test_tw_day_trade_simulation import _now

    fill, _, minute_path, limit_path = _source_verified_close_fixture(tmp_path)
    proof = fill["research_close_expense"]["official_close_evidence"]
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0, "official_close_evidence": proof}}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=limit_path, minute_roots=[minute_path.parent.parent])
    item = result["2330"]
    assert item["research_closing_minute_evidence"]["volume_shares"] == 2000
    assert item["research_closing_minute_evidence"]["source_path"] == str(minute_path)
    assert item["research_terminal_source_context"]["close"]["terminal_close_sources"][0]["path"] == proof["source_path"]


def test_research_terminal_can_use_a_later_source_when_earlier_root_lacks_closing_bar(tmp_path):
    from datetime import datetime
    from pathlib import Path
    import polars as pl

    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    original_context = quotes["2330"]["research_terminal_source_context"]
    minute_path = Path(original_context["intraday_replay"]["source_files"][0]["path"])
    limit_path = Path(original_context["price_limit_path"])
    earlier_root = tmp_path / "earlier_partial"
    partial = earlier_root / "trade_date=2026-08-13" / "data.parquet"
    partial.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["2330"], "ts": [datetime(2026, 8, 13, 13, 29)],
        "Open": [1095.0], "High": [1095.0], "Low": [1095.0], "Close": [1095.0],
        "volume_shares": [1000]}).write_parquet(partial)
    proof = quotes["2330"]["session_close_evidence"]
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0, "official_close_evidence": proof}}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=limit_path, minute_roots=[earlier_root, minute_path.parent.parent])
    item = result["2330"]
    assert item["research_closing_minute_evidence"]["source_path"] == str(minute_path)
    assert item["research_closing_minute_evidence"]["volume_shares"] == 2000
    assert [entry["path"] for entry in item["research_terminal_source_context"]
            ["intraday_replay"]["source_files"]] == [str(minute_path)]
    quotes["2330"].update(item)
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] == 0
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)


def test_research_terminal_resolves_each_symbol_from_one_ordered_source(tmp_path):
    from datetime import datetime
    import polars as pl

    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from test_tw_day_trade_simulation import _now

    first, second = tmp_path / "first", tmp_path / "second"
    paths = []
    for root, stamps, volumes in (
        (first, [datetime(2026, 8, 13, 13, 30), datetime(2026, 8, 13, 13, 29)], [1000, 2000]),
        (second, [datetime(2026, 8, 13, 13, 30)] * 2, [9000, 3000]),
    ):
        path = root / "trade_date=2026-08-13" / "data.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame({"symbol": ["2330", "2317"], "ts": stamps, "Open": [1100.0] * 2,
            "High": [1100.0] * 2, "Low": [1100.0] * 2, "Close": [1100.0] * 2,
            "volume_shares": volumes}).write_parquet(path)
        paths.append(path)
    limit = tmp_path / "limits.parquet"
    pl.DataFrame({"symbol": ["2330", "2317"], "upper_limit_price": [1100.0] * 2}).write_parquet(limit)
    targets = {symbol: {"price": 1100.0, "upper_limit": 1100.0,
                       "official_close_evidence": {"source_path": "/fixture", "source_sha256": "a" * 64}}
               for symbol in ("2330", "2317")}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=limit, minute_roots=[first, second])
    for symbol, path, shares in (("2330", paths[0], 1000), ("2317", paths[1], 3000)):
        item = result[symbol]
        assert item["research_closing_minute_evidence"]["source_path"] == str(path)
        assert item["research_closing_minute_evidence"]["volume_shares"] == shares
        assert item["research_terminal_source_context"]["intraday_replay"]["source_files"][0]["symbols"] == [symbol]


def test_research_terminal_local_builder_does_not_infer_missing_minute_as_zero(tmp_path):
    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from test_tw_day_trade_simulation import _now

    fill, _, _, limit_path = _source_verified_close_fixture(tmp_path)
    proof = fill["research_close_expense"]["official_close_evidence"]
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0, "official_close_evidence": proof}}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=limit_path, minute_roots=[])
    assert result["2330"]["minute_required"] is True
    assert result["2330"]["research_closing_minute_evidence"] is None


def test_research_terminal_local_builder_connects_to_source_checked_execution(tmp_path):
    from pathlib import Path
    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    proof = quotes["2330"]["session_close_evidence"]
    context = quotes["2330"]["research_terminal_source_context"]
    minute_path = Path(context["intraday_replay"]["source_files"][0]["path"])
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0, "official_close_evidence": proof}}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=context["price_limit_path"], minute_roots=[minute_path.parent.parent])
    quotes["2330"].update(result["2330"])
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)


def test_source_checked_paper_research_cost_matches_training_residual_math(tmp_path):
    from dataclasses import replace
    from test_tw_day_trade_carry import run, session, v
    from test_tw_day_trade_simulation import _now

    # Two whole lots enter; one source-covered lot exits before the synthetic
    # terminal cover, leaving one lot subject to the training stress expense.
    base = session(volume=6000, exits=False)
    prices, capacity = base.exit_prices.clone(), base.exit_capacity.clone()
    prices[0, 264, 1], capacity[0, 264, 1] = 1100.0, 1000.0
    baseline = replace(base, exit_prices=prices, exit_capacity=capacity,
                       terminal_liquidation_price=v(1100.0))
    stressed = replace(baseline, short_limit_up_penalty_per_share=v(77.0))
    weights = v(-0.2).reshape(1, 1)
    training_cost = (run(weights, [baseline], fees=False).final_state.last_nav
                     - run(weights, [stressed], fees=False).final_state.last_nav).item()

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    assert position["signed_shares"] == -2000
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] == 0
    assert training_cost == pytest.approx(77000.0)
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(training_cost)


def test_research_terminal_historical_replay_reuses_its_pinned_source(tmp_path):
    from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
    from test_tw_day_trade_simulation import _now

    engine, spec, mode, position, quotes = _engine_source_quotes(tmp_path)
    original = quotes["2330"]
    context = original["research_terminal_source_context"]
    bars = {"2330": {"2026-08-13T13:30+08:00": {"open": 1100.0, "high": 1100.0,
        "low": 1100.0, "close": 1100.0, "vwap": 1100.0, "volume_shares": 2000.0}}}
    engine.begin_deferred_ledger_writes()
    _replay_historical_intraday(engine, markets=[spec.market], bars=bars, trading_date=_now(13, 30).date(),
        terminal_close_quote_provider=lambda: {"2330": {"session_close_evidence": original["session_close_evidence"]}},
        research_source_context=context)
    engine.flush_deferred_ledger_writes()
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)


def test_research_terminal_replay_reconciles_full_270_minute_curve_and_cost(tmp_path, monkeypatch):
    from dataclasses import replace
    from datetime import datetime, timedelta
    import hashlib
    import json
    from pathlib import Path

    import polars as pl

    from scripts import audit_tw_day_trade_margin_replay as audit_module
    from scripts.rebuild_tw_day_trade_minute_curves import validate_existing_strategy_marks
    from scripts.rebuild_tw_day_trade_open_price_replay import _minute_bar_rows, _replay_historical_intraday
    from stockagent.live.tw_day_trade_simulation import ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TwDayTradeSimulationEngine
    from test_day_trade_margin_carry import register
    from test_tw_day_trade_simulation import _now, _spec

    account = tmp_path / "account"
    account.mkdir()
    spec = replace(_spec(account), entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        strict_intraday=True, terminal_liquidation_unlimited_capacity=True,
        research_short_limit_up_close_penalty_rate=0.07)
    engine = TwDayTradeSimulationEngine(account / "state")
    assert register(engine, spec, 0, -0.2) == "registered"
    mode = engine.state["modes"][spec.market]
    position = next(iter(mode["positions"].values()))
    position["upper_limit"] = 1100.0
    mode["historical_minute_valuation"] = True
    fill, context, minute_path, _ = _source_verified_close_fixture(tmp_path)
    proof = fill["research_close_expense"]["official_close_evidence"]
    beginning = datetime(2026, 8, 13, 9, 1)
    stamps = [beginning + timedelta(minutes=index) for index in range(270)]
    prices = [1000.0] * 269 + [1100.0]
    volume = [10000] + [1000] * 268 + [2000]
    pl.DataFrame({"symbol": ["2330"] * 270, "ts": stamps, "Open": prices, "High": prices,
        "Low": prices, "Close": prices, "volume_shares": volume,
        "Amount": [value * shares for value, shares in zip(prices, volume)]}).write_parquet(minute_path)
    digest = hashlib.sha256(minute_path.read_bytes()).hexdigest()
    manifest_path = minute_path.parent.parent / "manifest.json"
    manifest_path.write_text(json.dumps({"partitions": [{"status": "ok", "trade_date": "2026-08-13",
        "output": "trade_date=2026-08-13/data.parquet", "output_sha256": digest}]}))
    bars, coverage = _minute_bar_rows([minute_path.parent.parent], trading_date=_now(13, 30).date(),
                                     symbols={"2330"})
    context["intraday_replay"] = coverage
    engine.begin_deferred_ledger_writes()
    _replay_historical_intraday(engine, markets=[spec.market], bars=bars, trading_date=_now(13, 30).date(),
        terminal_close_quote_provider=lambda: {"2330": {"session_close_evidence": proof}},
        research_source_context=context)
    engine.flush_deferred_ledger_writes()
    engine._persist(_now(13, 30))
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    orders = {row["order_id"]: row for row in map(json.loads, engine.orders_path.read_text().splitlines())}
    for fill in fills:
        fill["side"] = orders[fill["order_id"]]["side"]
    marks = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    _, stats = validate_existing_strategy_marks(marks, start=_now(13, 30).date(), end=_now(13, 30).date())
    assert len(marks) == 270
    assert stats["generated_rows"] == 270
    assert position["signed_shares"] == 0
    assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)
    assert marks[-1]["cumulative_realized_net_pnl_twd"] == pytest.approx(mode["cumulative_realized_net_pnl_twd"])
    assert marks[-1]["total_equity_twd"] == pytest.approx(mode["initial_capital_twd"] + mode["cumulative_realized_net_pnl_twd"])
    context["modes"] = [{"market": spec.market, "after_close": {
        "research_short_limit_up_close_penalty_rate": 0.07,
        "terminal_close_contract": mode["terminal_close_contract"],
        "terminal_flatten_count": 1, "open_position_rows": 0}}]
    calendar_path = tmp_path / "twse_taiex_ohlc.parquet"
    calendar_path.write_bytes(b"test-calendar")
    calendar_sha = hashlib.sha256(calendar_path.read_bytes()).hexdigest()
    monkeypatch.setattr(audit_module, "_validated_taiex_session_dates",
        lambda *_: ({_now(13, 30).date()}, calendar_sha))
    rebuild = {"sessions": [context], "replay_contract": {
        "research_close_penalty_by_market_v1": {spec.market: 0.07},
        "terminal_close_contract": mode["terminal_close_contract"]},
        "official_session_calendar": {"path": str(calendar_path), "start_date": "2026-08-13",
            "end_date": "2026-08-13", "session_count": 1, "sha256": calendar_sha}}
    (engine.state_dir / "rebuild_receipt.json").write_text(json.dumps(rebuild))
    costs, _ = audit_module._verify_research_close_costs(fills, rebuild, {spec.market: mode}, verify_sources=True)
    assert sum(costs.values()) == pytest.approx(77000.0)
    assert audit_module.audit(engine.state_dir, verify_sources=True, require_margin_carry=False)["passed"]


def test_non_limit_research_short_settles_without_waiting_for_minute_source(tmp_path):
    from scripts.run_tw_day_trade_simulation import _build_research_terminal_source_contexts
    from scripts.settle_tw_day_trade_official_close import official_closes
    from test_day_trade_official_close_settlement import reports
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    root = reports(tmp_path, close="1095", volume="10000")
    prices, _ = official_closes(root, _now(13, 30).date())
    official = prices["2330"]
    proof = {"session_date": official["official_date"], "symbol": "2330", "price": official["price"],
        "source": official["source"], "source_sha256": official["source_sha256"],
        "source_path": official["source_path"], "price_basis": official["price_basis"]}
    context = quotes["2330"]["research_terminal_source_context"]
    targets = {"2330": {"price": 1095.0, "upper_limit": 1100.0, "official_close_evidence": proof}}
    result = _build_research_terminal_source_contexts(observed=_now(13, 35), targets=targets,
        limit_path=context["price_limit_path"], minute_roots=[])
    quotes["2330"] = {"session_close_evidence": proof, **result["2330"]}
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    assert position["signed_shares"] == 0 and mode["terminal_flatten_count"] == 1
    assert mode["cumulative_research_close_expense_twd"] == 0.0


def test_research_terminal_background_reader_never_blocks_engine_or_reuses_previous_session(tmp_path, monkeypatch):
    import threading
    from datetime import timedelta
    from scripts import run_tw_day_trade_simulation as runtime
    from test_tw_day_trade_simulation import _now

    started, release = threading.Event(), threading.Event()

    def slow_reader(**kwargs):
        started.set()
        assert release.wait(5)
        return {"2330": {"proof_sha256": "a" * 64, "research_terminal_source_context": {"session_date": "2026-08-13"},
                         "research_closing_minute_evidence": None, "minute_required": True}}

    monkeypatch.setattr(runtime, "_build_research_terminal_source_contexts", slow_reader)
    worker = runtime._ResearchTerminalSourceRecovery(limit_root=tmp_path, minute_roots=[])
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0,
                        "official_close_evidence": {"source_sha256": "a" * 64}}}
    assert worker.poll(_now(13, 35), targets) == {}
    assert started.wait(2)
    assert worker.poll(_now(13, 36), targets) == {}  # worker is still waiting
    assert worker.poll(_now(13, 36) + timedelta(days=1), targets) == {}
    release.set()
    worker._thread.join(timeout=2)
    result = worker.poll(_now(13, 36), targets)
    assert result["2330"]["research_terminal_source_context"]["session_date"] == "2026-08-13"


def test_research_terminal_background_reader_uses_all_canonical_local_roots(tmp_path):
    from scripts.run_tw_day_trade_simulation import _ResearchTerminalSourceRecovery
    from stockagent.live.minute_source_paths import DEFAULT_LOCAL_MINUTE_ROOTS

    worker = _ResearchTerminalSourceRecovery(limit_root=tmp_path)
    assert worker.minute_roots == DEFAULT_LOCAL_MINUTE_ROOTS


@pytest.mark.parametrize("tamper_receipt", [False, True])
def test_research_terminal_background_reader_accepts_receipted_repair_chunk(tmp_path, monkeypatch, tamper_receipt):
    import hashlib
    import json
    import shutil
    from pathlib import Path

    from downloader.download_shioaji_tw_minute_kbars import (
        RECEIPT_SCHEMA_VERSION, SOURCE_NAME, STORAGE_FREQUENCY,
    )
    from scripts import run_tw_day_trade_simulation as runtime
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    context = quotes["2330"]["research_terminal_source_context"]
    original_minute = Path(context["intraday_replay"]["source_files"][0]["path"])
    root = tmp_path / "fetched_kbars"
    minute = root / "minute_chunks" / "2330" / "2026-08-13_2026-08-13.parquet"
    minute.parent.mkdir(parents=True)
    shutil.copyfile(original_minute, minute)
    digest = hashlib.sha256(minute.read_bytes()).hexdigest()
    receipt = {"schema_version": RECEIPT_SCHEMA_VERSION, "source": SOURCE_NAME,
        "storage_frequency": STORAGE_FREQUENCY, "symbol": "2330",
        "start_date": "2026-08-13", "end_date": "2026-08-13", "status": "ok",
        "rows": 1, "returned_dates": ["2026-08-13"], "source_gap_dates": [],
        "output_receipt": {"path": str(minute), "size": minute.stat().st_size,
                           "sha256": "0" * 64 if tamper_receipt else digest}}
    minute.with_suffix(".receipt.json").write_text(json.dumps(receipt))
    limit_root = tmp_path / "live_limits"
    limit_root.mkdir()
    shutil.copyfile(context["price_limit_path"], limit_root / "2026-08-13.parquet")
    monkeypatch.setattr(runtime, "DEFAULT_LOCAL_MINUTE_ROOTS", (root,))
    worker = runtime._ResearchTerminalSourceRecovery(limit_root=limit_root)
    proof = quotes["2330"]["session_close_evidence"]
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0,
                        "official_close_evidence": proof}}
    assert worker.poll(_now(13, 35), targets) == {}
    worker._thread.join(timeout=5)
    result = worker.poll(_now(13, 35), targets)["2330"]
    assert result["research_closing_minute_evidence"]["source_path"] == str(minute)
    quotes["2330"].update(result)
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    if tamper_receipt:
        assert position["signed_shares"] < 0
        assert mode["engine_status"] == "waiting_research_close_evidence"
        assert mode.get("cumulative_research_close_expense_twd", 0) == 0
    else:
        assert position["signed_shares"] == 0
        assert mode["cumulative_research_close_expense_twd"] == pytest.approx(77000.0)


def test_research_terminal_background_reader_wakes_when_new_minute_file_appears(tmp_path, monkeypatch):
    import threading
    from scripts import run_tw_day_trade_simulation as runtime
    from test_tw_day_trade_simulation import _now

    calls, second = [], threading.Event()

    def reader(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            second.set()
        return {"2330": {"proof_sha256": "a" * 64,
            "research_terminal_source_context": {"session_date": "2026-08-13"},
            "research_closing_minute_evidence": None, "minute_required": True}}

    monkeypatch.setattr(runtime, "_build_research_terminal_source_contexts", reader)
    root = tmp_path / "research_dataset"
    worker = runtime._ResearchTerminalSourceRecovery(limit_root=tmp_path, minute_roots=[root])
    targets = {"2330": {"price": 1100.0, "upper_limit": 1100.0,
                        "official_close_evidence": {"source_sha256": "a" * 64}}}
    worker.poll(_now(13, 35), targets)
    worker._thread.join(timeout=2)
    worker.poll(_now(13, 35), targets)
    assert len(calls) == 1  # No needless repeated full scan while nothing changed.
    new_file = root / "trade_date=2026-08-13" / "data.parquet"
    new_file.parent.mkdir(parents=True)
    new_file.write_bytes(b"new-source")
    worker.poll(_now(13, 36), targets)
    assert second.wait(2)  # A changed local source wakes immediately, not after the backoff.
    worker._thread.join(timeout=2)


def test_research_terminal_source_hook_never_queries_for_existing_zero_rate_modes(tmp_path):
    from scripts.run_tw_day_trade_simulation import _attach_research_terminal_source_context
    from test_tw_day_trade_simulation import _now

    engine, _, mode, _, quotes = _engine_source_quotes(tmp_path)
    mode["research_short_limit_up_close_penalty_rate"] = 0.0

    class Forbidden:
        def poll(self, *_):
            pytest.fail("ordinary modes must not start research source work")

    _attach_research_terminal_source_context(quotes, engine=engine, observed=_now(13, 35), recovery=Forbidden())


def test_research_terminal_source_hook_rejects_conflicting_symbol_limits(tmp_path):
    from scripts.run_tw_day_trade_simulation import _attach_research_terminal_source_context
    from test_tw_day_trade_simulation import _now

    engine, _, mode, position, quotes = _engine_source_quotes(tmp_path)
    other = deepcopy(position)
    other.update(position_id=position["position_id"] + ":other", upper_limit=1095.0)
    mode["positions"][other["position_id"]] = other

    class Forbidden:
        def poll(self, *_):
            pytest.fail("conflicting source identities must not be submitted")

    _attach_research_terminal_source_context(quotes, engine=engine, observed=_now(13, 35), recovery=Forbidden())
    assert "research_terminal_source_context" not in quotes["2330"]
    assert "research_closing_minute_evidence" not in quotes["2330"]
