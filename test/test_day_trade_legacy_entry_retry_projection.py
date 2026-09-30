from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta

import pytest

from scripts.rebuild_tw_day_trade_minute_curves import (
    _validated_legacy_paper_entry_retries,
    rebuild_carried_strategy_marks,
)
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT


def _entry_rows():
    return [
        dict(
            market="a", symbol="2330", position_id="p", purpose="entry",
            order_id=order_id, quantity=quantity, price=price, fee_and_tax_twd=fee,
            session_date="2026-09-30", fill_at=f"2026-09-30T{clock}:00+08:00",
            recorded_at=f"2026-09-30T{clock}:00+08:00", simulation_only=True,
            fill_contract="paper_market_full_target_at_causal_shioaji_best_quote",
        )
        for order_id, quantity, price, fee, clock in (
            ("p:entry", 1_000, 100.0, 10.0, "09:01"),
            ("p:entry_retry:1", 2_000, 101.0, 20.0, "09:02"),
        )
    ]


@pytest.mark.parametrize("contract", [
    "causal_best_quote_remaining_order",
    "paper_market_full_target_at_causal_shioaji_best_quote",
])
def test_legacy_retry_identification_does_not_change_source_rows(contract):
    rows = _entry_rows()
    rows[1]["fill_contract"] = contract
    before = deepcopy(rows)
    assert _validated_legacy_paper_entry_retries(
        rows, {"p": {"symbol": "2330"}}, market="a"
    ) == frozenset({"p:entry_retry:1"})
    assert rows == before


@pytest.mark.parametrize("change", [
    {"simulation_only": False},
    {"simulation_only": None},
    {"fill_contract": "broker_fill"},
    {"fill_contract": None},
    {"symbol": "0050"},
    {"order_id": "another-position:entry_retry:1"},
    {"order_id": "p:entry_retry:0"},
    {"order_id": "p:entry_retry:01"},
    {"order_id": "p:entry_retry:١"},
    {"order_id": "p:entry_retry:-1"},
    {"order_id": "p:entry_retry:"},
    {"order_id": None},
    {"quantity": 0},
    {"quantity": -1_000},
    {"position_id": "missing"},
])
def test_ambiguous_duplicate_is_rejected_before_price_work(change):
    rows = _entry_rows()
    rows[1].update(change)
    with pytest.raises(RuntimeError):
        _validated_legacy_paper_entry_retries(rows, {"p": {"symbol": "2330"}}, market="a")


@pytest.mark.parametrize("purpose", ["entry", "entry_completion"])
def test_duplicate_paper_retry_order_is_not_double_counted(purpose):
    rows = _entry_rows()
    rows[1]["purpose"] = purpose
    rows.append(dict(rows[1]))
    with pytest.raises(RuntimeError, match="duplicate paper entry order"):
        _validated_legacy_paper_entry_retries(rows, {"p": {"symbol": "2330"}}, market="a")


def test_completion_cannot_create_the_original_cohort():
    rows = _entry_rows()[1:]
    rows[0]["purpose"] = "entry_completion"
    with pytest.raises(RuntimeError, match="no accepted original entry"):
        _validated_legacy_paper_entry_retries(rows, {"p": {"symbol": "2330"}}, market="a")


@pytest.mark.parametrize("side", ["long", "short"])
def test_legacy_retry_and_modern_completion_have_identical_minute_accounting(side):
    day = "2026-09-30"
    sign = 1 if side == "long" else -1
    position = dict(
        position_id="p", symbol="2330", side=side, entry_price=302_000.0 / 3_000,
        buy_fee_rate=0.0, sell_fee_rate=0.0, cash_buy_fee_rate=0.0,
        cash_sell_fee_rate=0.0, margin_carry_contract=MARGIN_CARRY_CONTRACT,
    )
    fills = _entry_rows()
    rows = [
        dict(
            market="a", session_date=day, minute=f"{day}T{clock}+08:00",
            initial_capital_twd=100_000.0, cumulative_realized_net_pnl_twd=0.0,
            open_net_liquidation_pnl_twd=net, total_equity_twd=100_000.0 + net,
            cumulative_carry_cost_twd=0.0, cumulative_corporate_action_net_twd=0.0,
            margin_carry_contract=MARGIN_CARRY_CONTRACT, open_position_count=1,
        )
        for clock, net in (("09:01", -10.0), ("13:30", sign * 4_000.0 - 30.0))
    ]
    state = {"modes": {"a": dict(
        initial_capital_twd=100_000.0, margin_carry_contract=MARGIN_CARRY_CONTRACT,
        share_replacement_ledger=[], corporate_action_ledger=[], carry_cost_ledger=[],
    )}}

    class Store:
        def prices(self, symbol, session_date):
            assert (symbol, session_date) == ("2330", day)
            first = datetime.fromisoformat(f"{day}T09:01:00+08:00")
            return {
                (first + timedelta(minutes=index)).isoformat(timespec="minutes"):
                100.0 if index == 0 else 102.0 for index in range(270)
            }

    positions = {day: {"a": [position]}}
    unchanged = deepcopy((rows, positions, state, fills))
    legacy, legacy_stats = rebuild_carried_strategy_marks(
        rows, positions, Store(), state=state, fill_rows=fills,
        start=date(2026, 9, 30), end=date(2026, 9, 30),
    )
    modern_fills = deepcopy(fills)
    modern_fills[1]["purpose"] = "entry_completion"
    modern, modern_stats = rebuild_carried_strategy_marks(
        rows, positions, Store(), state=state, fill_rows=modern_fills,
        start=date(2026, 9, 30), end=date(2026, 9, 30),
    )
    assert len(legacy) == 270
    assert legacy == modern
    assert legacy[1]["total_equity_twd"] == pytest.approx(100_000.0 + sign * 4_000.0 - 30.0)
    assert legacy_stats["legacy_paper_entry_retry_rows_reclassified_for_valuation"] == 1
    assert modern_stats["legacy_paper_entry_retry_rows_reclassified_for_valuation"] == 0
    assert (rows, positions, state, fills) == unchanged
