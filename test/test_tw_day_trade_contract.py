"""Shared arithmetic is a prerequisite, not proof of executor-state parity."""

from datetime import time

import pytest
import torch

from stockagent.backtest import tw_day_trade_contract as contract
from stockagent.backtest import tw_day_trade_minute as training
from stockagent.live import tw_day_trade_simulation as paper
from stockagent.live.tw_share_replacement import ODD_LOT_BOARD_PRICE


def test_paper_clock_and_research_assumption_aliases_remain_compatible():
    assert paper.ENTRY_GATE == time(9, 1)
    assert paper.EXIT_LIMIT_TIME == time(13, 20)
    assert paper.FORCE_EXIT_TIME == time(13, 24)
    assert paper.CLOSING_AUCTION_TIME == time(13, 25)
    assert paper.SESSION_CLOSE == time(13, 30)
    assert paper.MARGIN_CARRY_CONTRACT == contract.MARGIN_CARRY_CONTRACT
    assert ODD_LOT_BOARD_PRICE == contract.ODD_LOT_BOARD_PRICE
    assert training.MINUTE_VOLUME_PARTICIPATION == paper.MINUTE_VOLUME_PARTICIPATION == 0.5
    assert training.MARGIN_FINANCING_ANNUAL_RATE == paper.MARGIN_FINANCING_ANNUAL_RATE == 0.16
    assert training.MARGIN_FINANCING_PRINCIPAL_RATIO == paper.MARGIN_FINANCING_RATIO == 0.6


@pytest.mark.parametrize("minute", [-1, 271, True, 1.5, "1"])
def test_invalid_clock_is_rejected(minute):
    with pytest.raises(ValueError, match="session minute"):
        contract.session_time(minute)


def test_right_labelled_bar_cannot_fill_a_new_order_at_its_end():
    assert not contract.historical_order_can_fill(submitted_minute=264, bar_end_minute=264)
    assert contract.historical_order_can_fill(submitted_minute=264, bar_end_minute=265)
    assert not contract.historical_order_can_fill(submitted_minute=260, bar_end_minute=259)
    assert contract.historical_order_can_fill(submitted_minute=265, bar_end_minute=270)


@pytest.mark.parametrize("shares", [2000, -2000, 751, -751])
@pytest.mark.parametrize("carried", [False, True])
def test_tensor_and_paper_marks_match_including_odd_lots_and_net_fee(shares, carried):
    # Physical share replacement changes basis; historical original price is
    # not the cost basis. Entry cost and commission rebate are charged once.
    position = {
        "signed_shares": shares,
        "entry_price": 80.0,
        "inventory_basis_price": 100.0,
        "remaining_entry_fee_twd": 12.0,
        "buy_fee_rate": 0.001425,
        "sell_fee_rate": 0.002925,
        "cash_buy_fee_rate": 0.001425,
        "cash_sell_fee_rate": 0.004425,
        "commission_rebate_rate": 0.00114,
    }
    if carried:
        position["margin_carry_contract"] = contract.MARGIN_CARRY_CONTRACT
    prefix = "cash_" if carried else ""
    rate = position[prefix + ("sell_fee_rate" if shares > 0 else "buy_fee_rate")] - 0.00114
    mark = torch.tensor(110.0, dtype=torch.float64, requires_grad=True)
    result = contract.net_liquidation_pnl(shares, 100.0, mark, 12.0, rate)
    assert result.item() == pytest.approx(paper.position_net_liquidation_pnl(position, 110.0))
    result.backward()
    assert mark.grad.item() == pytest.approx(shares - abs(shares) * rate)
    assert position["signed_shares"] == shares  # Valuation is not a closing fill.


@pytest.mark.parametrize("shares,rate", [(2000, 0.60 * 0.16), (-751, 0.20)])
def test_interest_uses_outstanding_basis_and_calendar_days(shares, rate):
    principal_price = torch.tensor(100.0, dtype=torch.float64, requires_grad=True)
    result = contract.inventory_carry_interest(shares, principal_price, rate, 3)
    assert result.item() == pytest.approx(abs(shares) * 100.0 * rate * 3 / 365)
    result.backward()
    assert principal_price.grad.item() == pytest.approx(abs(shares) * rate * 3 / 365)
    assert contract.inventory_carry_interest(0, 100.0, rate, 3) == 0.0
    assert contract.inventory_carry_interest(shares, 100.0, rate, 0) == 0.0
