from __future__ import annotations

import math

import pytest
import torch

from stockagent.backtest import tw_continuous as tw_continuous_module
from stockagent.backtest.tw_continuous import (
    TaiwanContinuousResult,
    run_tw_cash_continuous,
    run_tw_day_trade_continuous,
)


def _cash_result(
    target: torch.Tensor,
    *,
    asset_log_returns: torch.Tensor | None = None,
    buy_fee: float = 0.01,
    sell_fee: float = 0.01,
    **kwargs: object,
) -> TaiwanContinuousResult:
    returns = (
        torch.zeros_like(target)
        if asset_log_returns is None
        else asset_log_returns
    )
    mask = torch.ones_like(target, dtype=torch.bool)
    symbols = int(target.size(1))
    return run_tw_cash_continuous(
        target,
        returns,
        mask,
        mask,
        mask,
        target.new_full((symbols,), buy_fee),
        target.new_full((symbols,), sell_fee),
        unresolved_corporate_action_mask=torch.zeros_like(mask),
        **kwargs,
    )


def _day_trade_result(
    target: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    *,
    buy_fee: float = 0.01,
    sell_fee: float = 0.02,
    **kwargs: object,
) -> TaiwanContinuousResult:
    mask = torch.ones_like(target, dtype=torch.bool)
    symbols = int(target.size(1))
    return run_tw_day_trade_continuous(
        target,
        intraday_log_returns,
        mask,
        mask,
        mask,
        mask,
        mask,
        target.new_full((symbols,), buy_fee),
        target.new_full((symbols,), sell_fee),
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        **kwargs,
    )


def _assert_result_rows_close(
    actual: TaiwanContinuousResult,
    expected: TaiwanContinuousResult,
    *,
    rtol: float,
    atol: float,
) -> None:
    names = (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "settlement_default",
        "equity_scale_history",
        "commission_rebate_accrued_history",
        "commission_rebate_paid_history",
        "commission_rebate_current_history",
        "commission_rebate_due_history",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
        "final_commission_rebate_current",
        "final_commission_rebate_due",
        "final_commission_rebate_month_id",
    )
    for name in names:
        torch.testing.assert_close(
            getattr(actual, name),
            getattr(expected, name),
            rtol=rtol,
            atol=atol,
        )


def test_zero_rebate_preserves_default_cash_contract_bitwise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    target = torch.tensor([[0.4, 0.0], [0.2, 0.3]], dtype=torch.float64)
    returns = torch.tensor([[0.01, 0.0], [0.0, -0.02]], dtype=torch.float64)

    omitted = _cash_result(target, asset_log_returns=returns)
    explicit = _cash_result(
        target,
        asset_log_returns=returns,
        commission_rebate_rates=0.0,
    )

    _assert_result_rows_close(omitted, explicit, rtol=0.0, atol=0.0)
    assert torch.count_nonzero(omitted.commission_rebate_accrued_history) == 0
    assert torch.count_nonzero(omitted.commission_rebate_paid_history) == 0
    assert torch.count_nonzero(omitted.commission_rebate_current_history) == 0
    assert torch.count_nonzero(omitted.commission_rebate_due_history) == 0


def test_cash_daily_rebate_uses_actual_trade_and_keeps_gross_t2_charge() -> None:
    target = torch.tensor([[0.5], [0.0]], dtype=torch.float64)
    result = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="daily_close",
    )

    torch.testing.assert_close(
        result.commission_rebate_accrued_history,
        result.turnovers * 0.008,
        rtol=0.0,
        atol=0.0,
    )
    torch.testing.assert_close(
        result.commission_rebate_paid_history,
        result.commission_rebate_accrued_history,
        rtol=0.0,
        atol=0.0,
    )
    assert torch.count_nonzero(result.commission_rebate_current_history) == 0
    assert torch.count_nonzero(result.commission_rebate_due_history) == 0

    # Execution still books the original 1% commission into the T+2 payable:
    # 0.5 opening notional * 1.01.  Rebate timing changes cash/assets only.
    raw_first_payable = (
        result.payables_history[0, -1] * result.equity_scale_history[0]
    )
    torch.testing.assert_close(
        raw_first_payable,
        torch.tensor(0.505, dtype=torch.float64),
        rtol=1.0e-12,
        atol=1.0e-12,
    )


def test_day_trade_rebate_uses_actual_open_and_close_notionals() -> None:
    target = torch.tensor([[0.5]], dtype=torch.float64)
    intraday = torch.tensor([[math.log(1.10)]], dtype=torch.float64)
    result = _day_trade_result(
        target,
        intraday,
        commission_rebate_rates=0.008,
        commission_rebate_timing="daily_close",
    )

    # Long buy is 0.50 at the open and its actual closing sale is 0.55.
    expected_turnover = torch.tensor([1.05], dtype=torch.float64)
    expected_rebate = expected_turnover * 0.008
    torch.testing.assert_close(
        result.turnovers,
        expected_turnover,
        rtol=1.0e-12,
        atol=1.0e-12,
    )
    torch.testing.assert_close(
        result.commission_rebate_accrued_history,
        expected_rebate,
        rtol=1.0e-12,
        atol=1.0e-12,
    )
    torch.testing.assert_close(
        result.commission_rebate_paid_history,
        expected_rebate,
        rtol=1.0e-12,
        atol=1.0e-12,
    )


def test_monthly_rebate_rolls_prior_month_then_pays_on_eligible_close() -> None:
    target = torch.tensor([[0.5], [0.0], [0.5], [0.0]], dtype=torch.float64)
    daily = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="daily_close",
    )
    result = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor(
            [202601, 202602, 202602, 202602],
            dtype=torch.int64,
        ),
        commission_rebate_payment_eligible_mask=torch.tensor(
            [False, False, True, True],
        ),
    )

    assert result.commission_rebate_current_history[0] > 0.0
    assert result.commission_rebate_due_history[0] == 0.0
    assert result.commission_rebate_due_history[1] > 0.0
    assert result.commission_rebate_paid_history[0] == 0.0
    assert result.commission_rebate_paid_history[1] == 0.0
    assert result.commission_rebate_paid_history[2] > 0.0
    assert result.commission_rebate_due_history[2] == 0.0
    assert result.commission_rebate_current_history[2] > 0.0
    assert int(result.final_commission_rebate_month_id.item()) == 202602
    # Accrual accounting recognizes the earned discount immediately; changing
    # only its cash-payment date must not create a return jump.
    torch.testing.assert_close(
        result.strategy_returns,
        daily.strategy_returns,
        rtol=1.0e-12,
        atol=1.0e-12,
    )


def test_monthly_pending_rebate_is_nav_asset_but_not_buying_power() -> None:
    target = torch.tensor([[1.0]], dtype=torch.float64)
    result = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([202601], dtype=torch.int64),
        commission_rebate_payment_eligible_mask=torch.tensor([False]),
        initial_cash=torch.tensor(0.2, dtype=torch.float64),
        initial_commission_rebate_current=torch.tensor(0.8, dtype=torch.float64),
        initial_commission_rebate_due=torch.tensor(0.0, dtype=torch.float64),
        initial_commission_rebate_month_id=torch.tensor(202601),
    )

    # NAV is one, but only settled cash 0.2 may fund the new T+2 payable.
    torch.testing.assert_close(
        result.turnovers,
        torch.tensor([0.2 / 1.01], dtype=torch.float64),
        rtol=1.0e-12,
        atol=1.0e-12,
    )
    assert result.commission_rebate_current_history[0] > 0.8


def test_settlement_default_forfeits_all_pending_rebates() -> None:
    target = torch.zeros((1, 1), dtype=torch.float64)
    result = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([202602], dtype=torch.int64),
        commission_rebate_payment_eligible_mask=torch.tensor([False]),
        initial_cash=torch.tensor(0.5, dtype=torch.float64),
        initial_payables=torch.tensor([0.6, 0.0], dtype=torch.float64),
        initial_receivables=torch.zeros(2, dtype=torch.float64),
        initial_commission_rebate_current=torch.tensor(0.6, dtype=torch.float64),
        initial_commission_rebate_due=torch.tensor(0.5, dtype=torch.float64),
        initial_commission_rebate_month_id=torch.tensor(202601),
        initial_alive=torch.tensor(True),
        initial_equity_scale=torch.tensor(1.0, dtype=torch.float64),
    )

    assert bool(result.settlement_default[0])
    assert result.final_commission_rebate_current == 0.0
    assert result.final_commission_rebate_due == 0.0
    assert result.final_commission_rebate_month_id == 0
    assert result.commission_rebate_current_history[0] == 0.0
    assert result.commission_rebate_due_history[0] == 0.0


def test_day_trade_monthly_state_matches_manual_eager_chunk_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    target = torch.tensor(
        [[0.4, -0.2], [0.0, 0.3], [-0.25, 0.0], [0.2, -0.1]],
        dtype=torch.float64,
    )
    intraday = torch.tensor(
        [[0.02, -0.01], [0.00, 0.03], [-0.02, 0.00], [0.01, -0.03]],
        dtype=torch.float64,
    )
    months = torch.tensor([202601, 202601, 202602, 202602])
    eligible = torch.tensor([False, False, False, True])
    common = {
        "commission_rebate_rates": torch.tensor(
            [0.006, 0.004],
            dtype=torch.float64,
        ),
        "commission_rebate_timing": "monthly_15th",
    }

    full = _day_trade_result(
        target,
        intraday,
        session_month_ids=months,
        commission_rebate_payment_eligible_mask=eligible,
        **common,
    )
    first = _day_trade_result(
        target[:2],
        intraday[:2],
        session_month_ids=months[:2],
        commission_rebate_payment_eligible_mask=eligible[:2],
        **common,
    )
    second = _day_trade_result(
        target[2:],
        intraday[2:],
        session_month_ids=months[2:],
        commission_rebate_payment_eligible_mask=eligible[2:],
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_commission_rebate_current=(
            first.final_commission_rebate_current
        ),
        initial_commission_rebate_due=first.final_commission_rebate_due,
        initial_commission_rebate_month_id=(
            first.final_commission_rebate_month_id
        ),
        initial_alive=first.final_alive,
        initial_equity_scale=first.final_equity_scale,
        **common,
    )

    for name in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "settlement_default",
        "equity_scale_history",
        "commission_rebate_accrued_history",
        "commission_rebate_paid_history",
        "commission_rebate_current_history",
        "commission_rebate_due_history",
    ):
        torch.testing.assert_close(
            torch.cat((getattr(first, name), getattr(second, name))),
            getattr(full, name),
            rtol=0.0,
            atol=0.0,
        )
    for name in (
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
        "final_commission_rebate_current",
        "final_commission_rebate_due",
        "final_commission_rebate_month_id",
    ):
        torch.testing.assert_close(
            getattr(second, name),
            getattr(full, name),
            rtol=0.0,
            atol=0.0,
        )


def test_monthly_padding_month_zero_is_ignored_when_state_does_not_advance() -> None:
    target = torch.tensor([[0.5], [0.9]], dtype=torch.float64)
    result = _cash_result(
        target,
        commission_rebate_rates=0.008,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([202601, 0]),
        commission_rebate_payment_eligible_mask=torch.tensor([False, False]),
        state_advance_mask=torch.tensor([True, False]),
    )

    assert result.commission_rebate_accrued_history[0] > 0.0
    assert result.commission_rebate_accrued_history[1] == 0.0
    torch.testing.assert_close(
        result.commission_rebate_current_history[1],
        result.commission_rebate_current_history[0],
        rtol=0.0,
        atol=0.0,
    )


def test_rebate_rate_cannot_refund_tax_or_exceed_gross_commission() -> None:
    target = torch.tensor([[0.5]], dtype=torch.float64)
    with pytest.raises(ValueError, match="cannot exceed"):
        _cash_result(
            target,
            buy_fee=0.01,
            sell_fee=0.02,
            commission_rebate_rates=0.011,
        )


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA fullgraph commission-rebate parity test",
)
@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_monthly_rebate_compiled_chunks_match_eager_and_gradients(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    tw_continuous_module._TW_CASH_COMPILED_CHUNK_CACHE.clear()
    tw_continuous_module._TW_CASH_FAILED_COMPILE_KEYS.clear()

    base_target = torch.tensor(
        [
            [0.30, -0.15],
            [0.10, 0.20],
            [-0.20, 0.10],
            [0.25, -0.10],
            [0.00, 0.30],
        ],
        device="cuda",
    )
    log_returns = torch.tensor(
        [
            [0.01, -0.02],
            [0.00, 0.03],
            [-0.01, 0.01],
            [0.02, -0.01],
            [0.00, 0.02],
        ],
        device="cuda",
    )
    months = torch.tensor(
        [202601, 202601, 202602, 202602, 202602],
        device="cuda",
    )
    eligible = torch.tensor(
        [False, False, False, True, True],
        device="cuda",
    )
    fees_buy = torch.tensor([0.0010, 0.0010], device="cuda")
    fees_sell = torch.tensor([0.0040, 0.0020], device="cuda")
    rebate_rates = torch.tensor([0.0006, 0.0005], device="cuda")
    mask = torch.ones_like(base_target, dtype=torch.bool)

    def run(target: torch.Tensor) -> TaiwanContinuousResult:
        common = {
            "commission_rebate_rates": rebate_rates,
            "commission_rebate_timing": "monthly_15th",
            "session_month_ids": months,
            "commission_rebate_payment_eligible_mask": eligible,
        }
        if mode == "tw_cash":
            return run_tw_cash_continuous(
                target,
                log_returns,
                mask,
                mask,
                mask,
                fees_buy,
                fees_sell,
                can_short_open_mask=mask,
                short_margin_rate=torch.full_like(target, 0.9),
                short_capacity_weights=torch.full_like(target, 1.0),
                unresolved_corporate_action_mask=torch.zeros_like(mask),
                **common,
            )
        return run_tw_day_trade_continuous(
            target,
            log_returns,
            mask,
            mask,
            mask,
            mask,
            mask,
            fees_buy,
            fees_sell,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            **common,
        )

    eager_target = base_target.detach().clone().requires_grad_(True)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    eager = run(eager_target)
    eager_loss = eager.strategy_returns.sum()
    eager_grad = torch.autograd.grad(eager_loss, eager_target)[0]

    compiled_target = base_target.detach().clone().requires_grad_(True)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    compiled = run(compiled_target)
    compiled_loss = compiled.strategy_returns.sum()
    compiled_grad = torch.autograd.grad(compiled_loss, compiled_target)[0]

    _assert_result_rows_close(compiled, eager, rtol=2.0e-5, atol=2.0e-6)
    torch.testing.assert_close(
        compiled_grad,
        eager_grad,
        rtol=5.0e-4,
        atol=5.0e-5,
    )
    assert compiled.commission_rebate_paid_history[3] > 0.0
