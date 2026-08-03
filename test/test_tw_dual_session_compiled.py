from __future__ import annotations

from dataclasses import fields

import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_dual_session import (
    TaiwanDualSessionResult,
    run_tw_cash_dual_session,
    run_tw_overnight_dual_session,
)
from stockagent.backtest.tw_dual_session_compiled import (
    clear_tw_dual_session_compile_cache,
    get_tw_dual_session_compile_stats,
    run_tw_cash_dual_session_compiled,
    run_tw_overnight_dual_session_compiled,
)


def _case(
    *,
    mode: str,
    rows: int,
    symbols: int,
    device: str,
    requires_grad: bool = False,
) -> dict[str, object]:
    base = torch.linspace(-1.0, 1.0, symbols, device=device)
    base = base / base.abs().sum().clamp_min(1.0) * 0.55
    phases = 2 if mode == "tw_cash" else 3
    actions = torch.zeros((rows, phases, symbols), device=device)
    if mode == "tw_cash":
        actions[:, 0] = base
        actions[:, 1] = base * 0.8
    else:
        actions[:, 0] = 0.75
        actions[:, 1] = base * 0.6
        actions[:, 2] = base * 0.3
    actions.requires_grad_(requires_grad)
    time = torch.arange(rows, device=device, dtype=torch.float32).unsqueeze(1)
    symbol = torch.arange(
        symbols,
        device=device,
        dtype=torch.float32,
    ).unsqueeze(0)
    overnight = torch.sin(time * 0.07 + symbol * 0.11) * 0.002
    intraday = torch.cos(time * 0.09 - symbol * 0.05) * 0.004
    phase_mask = torch.ones(
        (rows, 2, symbols),
        device=device,
        dtype=torch.bool,
    )
    return {
        "actions": actions,
        "overnight_log_returns": overnight,
        "intraday_log_returns": intraday,
        "tradable_mask": phase_mask,
        "can_buy_mask": phase_mask,
        "can_sell_mask": phase_mask,
        "buy_fee_rates": 0.001425,
        "sell_fee_rates": 0.004425,
        "can_short_open_mask": phase_mask,
        "short_margin_rate": 0.9,
        "short_capacity_weights": torch.ones(
            (rows, symbols),
            device=device,
        ),
        "volume_limit_weights": torch.full(
            (rows, symbols),
            0.75,
            device=device,
        ),
        "short_handling_fee_rate": 0.0008,
    }


def _assert_result_close(
    actual: TaiwanDualSessionResult,
    expected: TaiwanDualSessionResult,
) -> None:
    for field in fields(expected):
        actual_value = getattr(actual, field.name)
        expected_value = getattr(expected, field.name)
        if isinstance(expected_value, torch.Tensor):
            torch.testing.assert_close(
                actual_value,
                expected_value,
                rtol=2.0e-5,
                atol=2.0e-6,
            )
        else:
            assert actual_value == expected_value


@pytest.mark.parametrize(
    ("mode", "eager_runner", "compiled_runner"),
    [
        (
            "tw_cash",
            run_tw_cash_dual_session,
            run_tw_cash_dual_session_compiled,
        ),
        (
            "tw_overnight",
            run_tw_overnight_dual_session,
            run_tw_overnight_dual_session_compiled,
        ),
    ],
)
@pytest.mark.parametrize("return_history", [False, True])
def test_cpu_path_is_the_eager_oracle(
    mode: str,
    eager_runner,
    compiled_runner,
    return_history: bool,
) -> None:
    kwargs = _case(mode=mode, rows=35, symbols=5, device="cpu")
    expected = eager_runner(
        **kwargs,
        return_weights_history=return_history,
    )
    actual = compiled_runner(
        **kwargs,
        return_weights_history=return_history,
        strict_compile=True,
    )
    _assert_result_close(actual, expected)
    expected_event_rows = 35 if return_history else 0
    assert tuple(actual.event_turnovers.shape) == (expected_event_rows, 2)


def test_compiled_wrapper_preserves_scalar_validation() -> None:
    kwargs = _case(mode="tw_cash", rows=32, symbols=2, device="cpu")
    with pytest.raises(ValueError, match="short_maintenance_ratio"):
        run_tw_cash_dual_session_compiled(
            **kwargs,
            short_maintenance_ratio=True,
        )


@pytest.mark.parametrize(
    ("phase_values", "message"),
    [
        ((1.01, 0.0, 0.0), r"must be within \[0,1\]"),
        (
            (0.5, 0.2, -0.1),
            "may not reverse the same symbol within one session",
        ),
    ],
)
def test_compiled_overnight_wrapper_rejects_malformed_phase_actions(
    phase_values: tuple[float, float, float],
    message: str,
) -> None:
    kwargs = _case(
        mode="tw_overnight",
        rows=32,
        symbols=2,
        device="cpu",
    )
    actions = torch.as_tensor(kwargs["actions"]).clone()
    actions[0, :, 0] = actions.new_tensor(phase_values)
    kwargs["actions"] = actions

    with pytest.raises(ValueError, match=message):
        run_tw_overnight_dual_session_compiled(
            **kwargs,
            strict_compile=True,
        )


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA fixed-block compile contract",
)
def test_fixed_block_and_eager_tail_match_and_preserve_gradient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_compile = torch.compile
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    def compile_with_counting_backend(function, **kwargs):
        assert kwargs["fullgraph"] is True
        assert kwargs["dynamic"] is False
        assert kwargs["options"] == {"triton.cudagraphs": False}
        kwargs.pop("options")
        return real_compile(
            function,
            backend=counting_backend,
            **kwargs,
        )

    def add_monthly_rebate(kwargs: dict[str, object]) -> None:
        actions = torch.as_tensor(kwargs["actions"])
        device = actions.device
        kwargs["commission_rebate_rates"] = 0.00114
        kwargs["commission_rebate_timing"] = "monthly_15th"
        kwargs["session_month_ids"] = torch.cat(
            (
                torch.full(
                    (16,),
                    2026 * 12 + 1,
                    device=device,
                    dtype=torch.int64,
                ),
                torch.full(
                    (19,),
                    2026 * 12 + 2,
                    device=device,
                    dtype=torch.int64,
                ),
            )
        )
        payment_eligible = torch.zeros(35, device=device, dtype=torch.bool)
        payment_eligible[30:] = True
        kwargs["commission_rebate_payment_eligible_mask"] = payment_eligible

    eager_kwargs = _case(
        mode="tw_cash",
        rows=35,
        symbols=5,
        device="cuda",
        requires_grad=True,
    )
    add_monthly_rebate(eager_kwargs)
    eager = run_tw_cash_dual_session(
        **eager_kwargs,
        return_weights_history=False,
    )
    eager_objective = (
        eager.strategy_returns.sum()
        + 0.03 * eager.turnovers.sum()
        + 0.01 * eager.final_weights.square().sum()
    )
    eager_objective.backward()
    eager_grad = eager_kwargs["actions"].grad.detach().clone()

    compiled_kwargs = _case(
        mode="tw_cash",
        rows=35,
        symbols=5,
        device="cuda",
        requires_grad=True,
    )
    add_monthly_rebate(compiled_kwargs)
    torch._dynamo.reset()
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", compile_with_counting_backend)
    actual = run_tw_cash_dual_session_compiled(
        **compiled_kwargs,
        return_weights_history=False,
        strict_compile=True,
    )
    actual_objective = (
        actual.strategy_returns.sum()
        + 0.03 * actual.turnovers.sum()
        + 0.01 * actual.final_weights.square().sum()
    )
    actual_objective.backward()

    _assert_result_close(actual, eager)
    torch.testing.assert_close(
        compiled_kwargs["actions"].grad,
        eager_grad,
        rtol=3.0e-5,
        atol=3.0e-6,
    )
    assert tuple(actual.event_turnovers.shape) == (0, 2)
    assert graph_count == 1
    stats = get_tw_dual_session_compile_stats()
    assert stats == {
        "compile_constructors": 1,
        "compiled_block_calls": 1,
        "compiled_day_calls": 32,
        "eager_tail_calls": 1,
        "eager_fallback_calls": 0,
    }

    torch._dynamo.reset()
    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA non-monotone cover-funding parity",
)
def test_compiled_finds_global_producer_first_cover_and_matches_gradient() -> None:
    nav = 190.0

    def case() -> dict[str, object]:
        actions = torch.zeros((32, 3, 2), device="cuda")
        actions[0, 0] = torch.tensor([0.5, 1.0], device="cuda")
        actions.requires_grad_()
        returns = torch.zeros((32, 2), device="cuda")
        phase_mask = torch.ones(
            (32, 2, 2),
            device="cuda",
            dtype=torch.bool,
        )
        return {
            "actions": actions,
            "overnight_log_returns": returns,
            "intraday_log_returns": returns,
            "tradable_mask": phase_mask,
            "can_buy_mask": phase_mask,
            "can_sell_mask": phase_mask,
            "buy_fee_rates": 0.0,
            "sell_fee_rates": 0.0,
            "can_short_open_mask": phase_mask,
            "short_margin_rate": 0.9,
            "short_capacity_weights": torch.ones(
                (32, 2),
                device="cuda",
            ),
            "initial_weights": torch.tensor(
                [-1000.0 / nav, -1000.0 / nav],
                device="cuda",
            ),
            "initial_cash": torch.tensor(390.0 / nav, device="cuda"),
            "initial_payables": torch.zeros(2, device="cuda"),
            "initial_receivables": torch.zeros(2, device="cuda"),
            "initial_short_sale_collateral": torch.tensor(
                [700.0 / nav, 200.0 / nav],
                device="cuda",
            ),
            "initial_short_margin_collateral": torch.tensor(
                [700.0 / nav, 200.0 / nav],
                device="cuda",
            ),
            "gross_budget": 20.0,
            "return_weights_history": True,
        }

    eager_kwargs = case()
    expected = run_tw_overnight_dual_session(**eager_kwargs)
    expected.executed_short_cover_weights[0, 0].sum().backward()
    eager_grad = eager_kwargs["actions"].grad.detach().clone()

    compiled_kwargs = case()
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    actual = run_tw_overnight_dual_session_compiled(
        **compiled_kwargs,
        strict_compile=True,
    )
    actual.executed_short_cover_weights[0, 0].sum().backward()

    # The selected point is an FP32 funding boundary.  Inductor may reassociate
    # the pooled-collateral reductions by a few ulps, so use a boundary-specific
    # tolerance while still comparing every returned tensor and scalar.
    for field in fields(expected):
        actual_value = getattr(actual, field.name)
        expected_value = getattr(expected, field.name)
        if isinstance(expected_value, torch.Tensor):
            torch.testing.assert_close(
                actual_value,
                expected_value,
                rtol=2.0e-4,
                atol=2.0e-5,
            )
        else:
            assert actual_value == expected_value
    torch.testing.assert_close(
        actual.executed_short_cover_weights[0, 0]
        * actual.executed_short_cover_weights.new_tensor(nav),
        torch.tensor([500.0, 2950.0 / 3.0], device="cuda"),
        rtol=2.0e-5,
        atol=2.0e-4,
    )
    torch.testing.assert_close(
        compiled_kwargs["actions"].grad,
        eager_grad,
        rtol=3.0e-5,
        atol=3.0e-6,
    )
    assert torch.isfinite(compiled_kwargs["actions"].grad).all()
    assert bool((compiled_kwargs["actions"].grad.abs() > 0.0).any())
    stats = get_tw_dual_session_compile_stats()
    assert stats["compiled_block_calls"] == 1
    assert stats["compiled_day_calls"] == 32
    assert stats["eager_fallback_calls"] == 0
    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA unbounded-capacity recurrent gradient contract",
)
def test_compiled_overnight_zero_actions_keep_unbounded_capacity_gradient_finite() -> None:
    kwargs = _case(
        mode="tw_overnight",
        rows=32,
        symbols=5,
        device="cuda",
    )
    actions = torch.zeros(
        (32, 3, 5),
        device="cuda",
        requires_grad=True,
    )
    kwargs["actions"] = actions
    kwargs["short_capacity_weights"] = torch.full(
        (32, 5),
        float("inf"),
        device="cuda",
    )

    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    result = run_tw_overnight_dual_session_compiled(
        **kwargs,
        strict_compile=True,
    )
    (
        result.strategy_returns.sum()
        + result.turnovers.sum()
        + result.final_equity_scale
        + result.final_weights.square().sum()
    ).backward()

    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    stats = get_tw_dual_session_compile_stats()
    assert stats["compiled_block_calls"] == 1
    assert stats["compiled_day_calls"] == 32
    assert stats["eager_fallback_calls"] == 0
    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA dividend execution-boundary contract",
)
def test_compiled_history_preserves_pre_dividend_close_weight() -> None:
    kwargs = _case(
        mode="tw_cash",
        rows=32,
        symbols=2,
        device="cuda",
    )
    actions = torch.zeros_like(kwargs["actions"])
    actions[-1, 1, 0] = 0.5
    kwargs["actions"] = actions
    kwargs["overnight_log_returns"] = torch.zeros((32, 2), device="cuda")
    kwargs["intraday_log_returns"] = torch.zeros((32, 2), device="cuda")
    kwargs["buy_fee_rates"] = 0.0
    kwargs["sell_fee_rates"] = 0.0
    yields = torch.zeros((32, 2), device="cuda")
    delays = torch.zeros((32, 2), device="cuda", dtype=torch.int64)
    yields[-1, 0] = 0.1
    delays[-1, 0] = 3
    kwargs["cash_dividend_yield"] = yields
    kwargs["cash_dividend_payment_delay_sessions"] = delays
    kwargs["claim_queue_sessions"] = 3

    expected = run_tw_cash_dual_session(
        **kwargs,
        return_weights_history=True,
    )
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    actual = run_tw_cash_dual_session_compiled(
        **kwargs,
        return_weights_history=True,
        strict_compile=True,
    )

    _assert_result_close(actual, expected)
    torch.testing.assert_close(
        actual.weights_history[-1, 0],
        torch.tensor(0.5, device="cuda"),
    )
    torch.testing.assert_close(
        actual.final_weights[0],
        torch.tensor(0.5 / 1.05, device="cuda"),
    )
    stats = get_tw_dual_session_compile_stats()
    assert stats["compiled_block_calls"] == 1
    assert stats["compiled_day_calls"] == 32
    assert stats["eager_fallback_calls"] == 0
    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA overnight tail contract",
)
@pytest.mark.parametrize("return_history", [False, True])
def test_overnight_block_tail_preserves_due_contract(
    monkeypatch: pytest.MonkeyPatch,
    return_history: bool,
) -> None:
    monkeypatch.setattr(
        torch,
        "compile",
        lambda function, **_kwargs: function,
    )
    kwargs = _case(
        mode="tw_overnight",
        rows=33,
        symbols=4,
        device="cuda",
    )
    expected = run_tw_overnight_dual_session(
        **kwargs,
        return_weights_history=return_history,
    )
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    actual = run_tw_overnight_dual_session_compiled(
        **kwargs,
        return_weights_history=return_history,
        strict_compile=True,
    )

    _assert_result_close(actual, expected)
    if return_history:
        assert actual.due_weights_history is not None
        assert tuple(actual.due_weights_history.shape) == (33, 4)
    else:
        assert actual.due_weights_history is None
        assert tuple(actual.event_turnovers.shape) == (0, 2)
    assert actual.final_due_weights is not None
    stats = get_tw_dual_session_compile_stats()
    assert stats["compiled_block_calls"] == 1
    assert stats["compiled_day_calls"] == 32
    assert stats["eager_tail_calls"] == 1
    assert stats["eager_fallback_calls"] == 0

    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="CUDA compile fallback contract",
)
def test_compile_failure_is_counted_and_strict_mode_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable_compile(_function, **_kwargs):
        def fail(*_args, **_call_kwargs):
            raise RuntimeError("synthetic backend failure")

        return fail

    kwargs = _case(mode="tw_cash", rows=32, symbols=3, device="cuda")
    expected = run_tw_cash_dual_session(
        **kwargs,
        return_weights_history=False,
    )
    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    monkeypatch.setattr(torch, "compile", unavailable_compile)
    actual = run_tw_cash_dual_session_compiled(
        **kwargs,
        return_weights_history=False,
    )
    _assert_result_close(actual, expected)
    assert get_tw_dual_session_compile_stats()["eager_fallback_calls"] == 1

    with pytest.raises(
        RuntimeError,
        match="previously failed",
    ):
        run_tw_cash_dual_session_compiled(
            **kwargs,
            return_weights_history=False,
            strict_compile=True,
        )

    clear_tw_dual_session_compile_cache()


@pytest.mark.skipif(
    not torch.cuda.is_available(),
    reason="canonical CUDA dispatcher integration",
)
def test_canonical_dispatcher_uses_fixed_block_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE_STATEFUL", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    case = _case(
        mode="tw_cash",
        rows=32,
        symbols=5,
        device="cuda",
        requires_grad=True,
    )
    phase_mask = case["tradable_mask"]
    assert isinstance(phase_mask, torch.Tensor)
    actions = case["actions"]
    assert isinstance(actions, torch.Tensor)
    intraday = case["intraday_log_returns"]
    overnight = case["overnight_log_returns"]
    assert isinstance(intraday, torch.Tensor)
    assert isinstance(overnight, torch.Tensor)

    clear_tw_dual_session_compile_cache()
    get_tw_dual_session_compile_stats(reset=True)
    result = run_backtest_torch(
        actions,
        intraday,
        phase_mask[:, 1],
        torch.zeros(32, device="cuda"),
        0.001425,
        0.004425,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=phase_mask[:, 1],
        can_sell_mask=phase_mask[:, 1],
        can_short_open_mask=phase_mask[:, 1],
        can_short_open_open_mask=phase_mask[:, 0],
        force_short_cover_mask=torch.zeros_like(phase_mask[:, 1]),
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((32, 5), device="cuda"),
        short_handling_fee_rate=0.0008,
        execution_mode="tw_cash",
        buy_fee_rates=torch.full((5,), 0.001425, device="cuda"),
        sell_fee_rates=torch.full((5,), 0.004425, device="cuda"),
        day_trade_can_buy_open_mask=phase_mask[:, 0],
        day_trade_can_sell_open_mask=phase_mask[:, 0],
        unresolved_corporate_action_mask=torch.zeros_like(
            phase_mask[:, 1]
        ),
        overnight_returns=overnight,
        return_weights_history=False,
    )
    (
        result.strategy_returns.sum()
        + 0.01 * result.final_weights.square().sum()
    ).backward()

    stats = get_tw_dual_session_compile_stats()
    assert stats["compiled_block_calls"] == 1
    assert stats["compiled_day_calls"] == 32
    assert stats["eager_fallback_calls"] == 0
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    clear_tw_dual_session_compile_cache()
