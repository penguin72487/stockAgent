import math

import torch
from torch import nn

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _CompiledLossFallback, _detach_portfolio_state, _evaluate_tensor_batch


class _EchoWeightModel(nn.Module):
    def forward(self, x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        del mask
        return x[:, -1, :, 0]


def test_detach_portfolio_state_clones_independent_buffer() -> None:
    state = (torch.arange(1, 6, dtype=torch.float32, requires_grad=True) * 0.25).contiguous()

    detached = _detach_portfolio_state(state)

    assert detached is not None
    assert detached.device == state.device
    assert detached.dtype == state.dtype
    assert detached.is_contiguous()
    assert not detached.requires_grad
    assert detached.data_ptr() != state.data_ptr()
    assert torch.allclose(detached, state.detach())

    detached[0] = -123.0
    assert not torch.allclose(detached, state.detach())


def test_compiled_loss_fallback_disables_after_cudagraph_state_overwrite() -> None:
    calls = {"compiled": 0, "eager": 0}

    def compiled_fn(x: torch.Tensor) -> torch.Tensor:
        calls["compiled"] += 1
        raise RuntimeError("tensor output of CUDAGraphs that has been overwritten by a subsequent run")

    def eager_fn(x: torch.Tensor) -> torch.Tensor:
        calls["eager"] += 1
        return x + 1.0

    wrapped = _CompiledLossFallback(compiled_fn, eager_fn, label="test")
    x = torch.tensor(2.0)

    assert torch.equal(wrapped(x), torch.tensor(3.0))
    assert torch.equal(wrapped(x), torch.tensor(3.0))
    assert calls == {"compiled": 1, "eager": 2}


def _chunked_backtest(
    weights: torch.Tensor,
    returns: torch.Tensor,
    tradable: torch.Tensor,
    benchmark: torch.Tensor,
    can_buy: torch.Tensor,
    can_sell: torch.Tensor,
    *,
    chunk_rows: int,
):
    strategy_chunks = []
    turnover_chunks = []
    weight_chunks = []
    prev = None
    for start in range(0, weights.size(0), chunk_rows):
        end = min(start + chunk_rows, weights.size(0))
        bt = run_backtest_torch(
            weights[start:end],
            returns[start:end],
            tradable[start:end],
            benchmark[start:end],
            buy_fee_rate=0.001,
            sell_fee_rate=0.003,
            long_only=True,
            max_turnover_ratio=0.65,
            gross_leverage=1.0,
            can_buy_mask=can_buy[start:end],
            can_sell_mask=can_sell[start:end],
            initial_weights=prev,
        )
        strategy_chunks.append(bt.strategy_returns)
        turnover_chunks.append(bt.turnovers)
        weight_chunks.append(bt.weights_history)
        prev = bt.final_weights
    return (
        torch.cat(strategy_chunks, dim=0),
        torch.cat(turnover_chunks, dim=0),
        torch.cat(weight_chunks, dim=0),
        prev,
    )


def test_torch_backtest_chunk_continuation_matches_full_run() -> None:
    torch.manual_seed(123)
    rows, symbols = 17, 9
    weights = torch.randn(rows, symbols).softmax(dim=1)
    returns = torch.randn(rows, symbols) * 0.015
    tradable = torch.ones(rows, symbols, dtype=torch.bool)
    can_buy = torch.rand(rows, symbols) > 0.15
    can_sell = torch.rand(rows, symbols) > 0.20
    can_buy[0] = True
    can_sell[0] = True
    benchmark = returns.mean(dim=1)

    full = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.001,
        sell_fee_rate=0.003,
        long_only=True,
        max_turnover_ratio=0.65,
        gross_leverage=1.0,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
    )
    chunk_returns, chunk_turnovers, chunk_weights, chunk_final = _chunked_backtest(
        weights,
        returns,
        tradable,
        benchmark,
        can_buy,
        can_sell,
        chunk_rows=4,
    )

    assert torch.allclose(chunk_returns, full.strategy_returns, atol=1e-7, rtol=1e-6)
    assert torch.allclose(chunk_turnovers, full.turnovers, atol=1e-7, rtol=1e-6)
    assert torch.allclose(chunk_weights, full.weights_history, atol=1e-7, rtol=1e-6)
    assert full.final_weights is not None
    assert chunk_final is not None
    assert torch.allclose(chunk_final, full.final_weights, atol=1e-7, rtol=1e-6)


def test_evaluate_tensor_batch_resets_only_at_segment_boundaries() -> None:
    torch.manual_seed(321)
    rows, symbols = 13, 7
    raw_weights = torch.randn(rows, symbols).softmax(dim=1)
    x = raw_weights[:, None, :, None].contiguous()
    returns = torch.randn(rows, symbols) * 0.01
    tradable = torch.ones(rows, symbols, dtype=torch.bool)
    can_buy = torch.rand(rows, symbols) > 0.10
    can_sell = torch.rand(rows, symbols) > 0.10
    can_buy[0] = True
    can_sell[0] = True
    can_buy[5] = True
    can_sell[5] = True
    benchmark = returns.mean(dim=1)

    split = 5
    expected_parts = []
    for start, end in ((0, split), (split, rows)):
        expected_parts.append(
            run_backtest_torch(
                raw_weights[start:end],
                returns[start:end],
                tradable[start:end],
                benchmark[start:end],
                buy_fee_rate=0.001,
                sell_fee_rate=0.003,
                long_only=True,
                max_turnover_ratio=0.55,
                gross_leverage=1.0,
                can_buy_mask=can_buy[start:end],
                can_sell_mask=can_sell[start:end],
            )
        )
    expected_returns = torch.cat([item.strategy_returns for item in expected_parts], dim=0)
    expected_turnovers = torch.cat([item.turnovers for item in expected_parts], dim=0)
    expected_weights = torch.cat([item.weights_history for item in expected_parts], dim=0)

    backtest, _, _ = _evaluate_tensor_batch(
        _EchoWeightModel(),
        x,
        returns,
        tradable,
        can_buy,
        can_sell,
        benchmark,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
        reset_at_rows=[0, split, rows],
    )

    assert torch.allclose(backtest.strategy_returns.cpu(), expected_returns, atol=1e-7, rtol=1e-6)
    assert torch.allclose(backtest.turnovers.cpu(), expected_turnovers, atol=1e-7, rtol=1e-6)
    assert torch.allclose(backtest.weights_history.cpu(), expected_weights, atol=1e-7, rtol=1e-6)


def test_sortino_loss_uses_canonical_tensor_backtest_returns() -> None:
    weights = torch.tensor(
        [
            [0.70, 0.20, 0.10],
            [0.10, 0.80, 0.10],
            [0.55, 0.15, 0.30],
            [0.00, 0.65, 0.35],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.020, -0.010, 0.005],
            [-0.012, 0.015, 0.002],
            [0.006, -0.020, 0.018],
            [-0.004, 0.003, -0.011],
        ],
        dtype=torch.float32,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    benchmark = returns.mean(dim=1)

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=True,
        buy_fee_rate=0.0005,
        sell_fee_rate=0.0040,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
        objective="sortino",
    )

    bt = run_backtest_torch(
        weights,
        returns,
        mask,
        benchmark,
        buy_fee_rate=0.0005,
        sell_fee_rate=0.0040,
        long_only=True,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        can_buy_mask=mask,
        can_sell_mask=mask,
        return_weights_history=False,
    )
    net = bt.strategy_returns
    downside = torch.minimum(net, torch.zeros_like(net))
    expected = -(net.mean() / torch.sqrt(downside.pow(2).mean() + 1e-8) * math.sqrt(252.0))

    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_sortino_loss_accepts_initial_weights_for_stateful_batches() -> None:
    weights = torch.tensor(
        [
            [0.60, 0.25, 0.15],
            [0.20, 0.70, 0.10],
            [0.10, 0.45, 0.45],
            [0.50, 0.10, 0.40],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.010, -0.005, 0.002],
            [0.004, 0.012, -0.006],
            [-0.007, 0.003, 0.014],
            [0.009, -0.011, 0.001],
        ],
        dtype=torch.float32,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    benchmark = returns.mean(dim=1)

    aux_first: dict[str, torch.Tensor | None] = {}
    _ = risk_aware_loss(
        weights[:2],
        returns[:2],
        mask[:2],
        benchmark_returns=benchmark[:2],
        can_buy_mask=mask[:2],
        can_sell_mask=mask[:2],
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        objective="sortino",
        gamma_turnover=0.0,
        concentration_weight=0.0,
        aux_outputs=aux_first,  # type: ignore[arg-type]
    )
    prev = aux_first.get("_final_weights")
    assert prev is not None

    prev_cloned = _detach_portfolio_state(prev)
    assert prev_cloned is not None
    assert prev_cloned.data_ptr() != prev.data_ptr()

    aux_second = {"initial_weights": prev_cloned}
    loss = risk_aware_loss(
        weights[2:],
        returns[2:],
        mask[2:],
        benchmark_returns=benchmark[2:],
        can_buy_mask=mask[2:],
        can_sell_mask=mask[2:],
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        objective="sortino",
        gamma_turnover=0.0,
        concentration_weight=0.0,
        aux_outputs=aux_second,
    )
    bt = run_backtest_torch(
        weights[2:],
        returns[2:],
        mask[2:],
        benchmark[2:],
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        can_buy_mask=mask[2:],
        can_sell_mask=mask[2:],
        return_weights_history=False,
        initial_weights=prev_cloned,
    )
    downside = torch.minimum(bt.strategy_returns, torch.zeros_like(bt.strategy_returns))
    expected = -(bt.strategy_returns.mean() / torch.sqrt(downside.pow(2).mean() + 1e-8) * math.sqrt(252.0))

    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)
    assert bt.final_weights is not None
    assert torch.allclose(aux_second["_final_weights"], bt.final_weights, atol=1e-7, rtol=1e-6)
