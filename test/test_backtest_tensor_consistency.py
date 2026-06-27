import math

import numpy as np
import pytest
import torch
from torch import nn

from stockagent.backtest.simulator import run_backtest_torch, run_backtest_torch_reduced
import stockagent.backtest.simulator as simulator
from stockagent.data.panel import PanelData
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import _dense_masked_clean_mean, get_loss_runtime_stats, risk_aware_loss
from stockagent.training.fused_loss import fused_log_utility_loss_tensor
from stockagent.training.trainer import (
    _batched_loss_from_backtest_segments,
    _CompiledLossFallback,
    _dataset_to_tensors,
    _detach_portfolio_state,
    _estimate_eval_chunk_rows,
    _evaluate_tensor_batch,
    _evaluate_windowed_tensor_batch,
    _maybe_share_windowed_base_from_cached,
    _PanelSlabForwardWrapper,
    _panel_indices_to_tensors,
    _pad_eval_chunk_first_dim,
    _pad_eval_metadata_first_dim,
    _pad_training_tensors,
    _pad_windowed_training_split,
    _prepare_windowed_split,
    TimingBreakdown,
)
from stockagent.training.windowed import dataset_to_windowed_tensors


class _EchoWeightModel(nn.Module):
    def forward(self, x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        del mask
        return x[:, -1, :, 0]


class _EchoPanelWeightModel(_EchoWeightModel):
    def __init__(self, lookback: int) -> None:
        super().__init__()
        self.lookback = int(lookback)

    def forward_from_panel(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor:
        del return_aux
        date_indices = date_indices.to(device=features.device, dtype=torch.long)
        offsets = torch.arange(self.lookback - 1, -1, -1, device=features.device, dtype=torch.long)
        x = features[date_indices[:, None] - offsets[None, :]]
        return self.forward(x.to(device=mask.device), mask)

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor:
        del return_aux
        x = feature_slab.unfold(0, self.lookback, 1).permute(0, 3, 1, 2).contiguous()
        return self.forward(x.to(device=mask.device), mask)


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


def test_min_trade_weight_zeroes_small_positions_and_redistributes() -> None:
    weights = torch.tensor([[8.0, 0.02, -8.0]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    base = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        min_trade_weight=0.0,
    )
    thresholded = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        min_trade_weight=0.05,
    )

    assert base.weights_history[0, 1].abs() > 0.0
    assert thresholded.weights_history[0, 1].item() == 0.0
    assert torch.allclose(
        thresholded.weights_history[0].abs().sum(),
        base.weights_history[0].abs().sum(),
        atol=1e-7,
        rtol=1e-6,
    )
    assert torch.allclose(
        thresholded.weights_history[0, 0] / thresholded.weights_history[0, 2],
        base.weights_history[0, 0] / base.weights_history[0, 2],
        atol=1e-7,
        rtol=1e-6,
    )


def test_reduced_and_fused_log_utility_convert_asset_log_returns_for_short_pnl() -> None:
    weights = torch.tensor([[-1.0]], dtype=torch.float32, requires_grad=True)
    returns = torch.tensor([[math.log(0.4)]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)
    sample_mask = torch.ones((1,), dtype=torch.bool)
    initial_weights = torch.zeros((1,), dtype=torch.float32)
    expected_strategy_log_return = torch.tensor(math.log1p(0.6), dtype=torch.float32)

    reduced = run_backtest_torch_reduced(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        sample_mask=sample_mask,
        initial_weights=initial_weights,
        reduction="log_utility",
    )
    assert torch.allclose(reduced.return_sum.cpu(), expected_strategy_log_return, atol=1e-7, rtol=1e-6)

    fused_loss, final_weights = fused_log_utility_loss_tensor(
        weights,
        returns,
        tradable,
        tradable,
        tradable,
        sample_mask,
        initial_weights,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
    )
    expected_loss = -expected_strategy_log_return * 252.0
    assert torch.allclose(fused_loss.cpu(), expected_loss, atol=1e-6, rtol=1e-6)
    assert torch.allclose(final_weights.cpu(), torch.tensor([-1.0]), atol=1e-7, rtol=1e-6)


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


def test_compiled_loss_strict_no_fallback_raises_after_cudagraph_state_overwrite() -> None:
    calls = {"compiled": 0, "eager": 0}

    def compiled_fn(x: torch.Tensor) -> torch.Tensor:
        calls["compiled"] += 1
        raise RuntimeError("tensor output of CUDAGraphs that has been overwritten by a subsequent run")

    def eager_fn(x: torch.Tensor) -> torch.Tensor:
        calls["eager"] += 1
        return x + 1.0

    wrapped = _CompiledLossFallback(compiled_fn, eager_fn, label="test", strict_no_fallback=True)

    with pytest.raises(RuntimeError, match="strict_no_fallback=true"):
        wrapped(torch.tensor(2.0))

    assert calls == {"compiled": 1, "eager": 0}


def test_eval_chunk_estimate_uses_full_eval_rows_not_probe_rows() -> None:
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048) == 2048
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=999999) == 4096
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=0) == 1
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048, max_chunk_rows=64) == 64
    assert _estimate_eval_chunk_rows(total_rows=32, estimated_rows=2048, max_chunk_rows=64) == 32
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048, max_chunk_rows=0) == 2048


def test_backtest_compile_gate_skips_toolchain_lookup_while_dynamo_compiling(monkeypatch) -> None:
    monkeypatch.setattr(simulator, "_torch_dynamo_is_compiling", lambda: True)
    monkeypatch.setattr(
        simulator,
        "_prepend_cuda_toolchain_paths",
        lambda: (_ for _ in ()).throw(AssertionError("toolchain lookup should be skipped")),
    )
    monkeypatch.setattr(
        simulator.shutil,
        "which",
        lambda name: (_ for _ in ()).throw(AssertionError(f"which({name}) should be skipped")),
    )

    assert simulator._compile_enabled() is False


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


def test_evaluate_tensor_batch_ragged_chunk_padding_matches_full_long_short_backtest() -> None:
    torch.manual_seed(654)
    rows, symbols = 13, 6
    raw_weights = torch.randn(rows, symbols)
    x = raw_weights[:, None, :, None].contiguous()
    returns = torch.randn(rows, symbols) * 0.01
    tradable = torch.ones(rows, symbols, dtype=torch.bool)
    can_buy = torch.rand(rows, symbols) > 0.20
    can_sell = torch.rand(rows, symbols) > 0.20
    can_buy[0] = True
    can_sell[0] = True
    benchmark = returns.mean(dim=1)

    expected = run_backtest_torch(
        raw_weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.001,
        sell_fee_rate=0.003,
        long_only=False,
        max_turnover_ratio=0.55,
        gross_leverage=1.0,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
    )
    actual, _, _ = _evaluate_tensor_batch(
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
        False,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=4,
    )

    assert actual.strategy_returns.numel() == rows
    assert actual.weights_history.shape == expected.weights_history.shape
    assert torch.allclose(actual.strategy_returns.cpu(), expected.strategy_returns, atol=1e-7, rtol=1e-6)
    assert torch.allclose(actual.turnovers.cpu(), expected.turnovers, atol=1e-7, rtol=1e-6)
    assert torch.allclose(actual.weights_history.cpu(), expected.weights_history, atol=1e-7, rtol=1e-6)


def test_evaluate_tensor_batch_decoupled_backtest_chunk_matches_old_chunking() -> None:
    torch.manual_seed(777)
    rows, symbols = 19, 8
    raw_weights = torch.randn(rows, symbols)
    x = raw_weights[:, None, :, None].contiguous()
    returns = torch.randn(rows, symbols) * 0.01
    tradable = torch.rand(rows, symbols) > 0.08
    can_buy = torch.rand(rows, symbols) > 0.15
    can_sell = torch.rand(rows, symbols) > 0.18
    tradable[0] = True
    can_buy[0] = True
    can_sell[0] = True
    tradable[9] = True
    can_buy[9] = True
    can_sell[9] = True
    benchmark = returns.mean(dim=1)
    reset_rows = [0, 9, rows]

    simulator.get_backtest_runtime_stats(reset=True)
    old, old_ic, old_metrics = _evaluate_tensor_batch(
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
        False,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=4,
        reset_at_rows=reset_rows,
    )
    old_calls = int(simulator.get_backtest_runtime_stats(reset=True)["calls"])
    new, new_ic, new_metrics = _evaluate_tensor_batch(
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
        False,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=4,
        backtest_chunk_rows=11,
        reset_at_rows=reset_rows,
    )
    new_calls = int(simulator.get_backtest_runtime_stats(reset=True)["calls"])

    assert torch.allclose(new.strategy_returns.cpu(), old.strategy_returns.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.benchmark_returns.cpu(), old.benchmark_returns.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.turnovers.cpu(), old.turnovers.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.weights_history.cpu(), old.weights_history.cpu(), atol=1e-7, rtol=1e-6)
    for key, value in old_metrics.items():
        assert math.isclose(new_metrics[key], value, rel_tol=1e-6, abs_tol=1e-8), key
    for key, value in old_ic.items():
        assert math.isclose(new_ic[key], value, rel_tol=1e-6, abs_tol=1e-8), key
    assert old_calls == 6
    assert new_calls == 2


def _make_panel(rows: int = 8, symbols: int = 4, features: int = 3) -> PanelData:
    values = torch.arange(rows * symbols * features, dtype=torch.float32).reshape(rows, symbols, features)
    returns = torch.linspace(-0.02, 0.02, rows * symbols, dtype=torch.float32).reshape(rows, symbols)
    mask = torch.ones(rows, symbols, dtype=torch.bool)
    return PanelData(
        dates=torch.arange(rows).numpy().astype("datetime64[D]"),
        symbols=[f"S{i}" for i in range(symbols)],
        feature_names=[f"f{i}" for i in range(features)],
        features=values.numpy(),
        returns_1d=returns.numpy(),
        tradable_mask=mask.numpy(),
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
        alive_mask=mask.numpy(),
        benchmark_returns=returns.mean(dim=1).numpy(),
        close_prices=torch.ones(rows, symbols).numpy(),
    )


def test_windowed_split_matches_materialized_dataset_tensors() -> None:
    panel = _make_panel()
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=3)
    expected = _dataset_to_tensors(dataset)
    split = dataset_to_windowed_tensors(dataset)
    actual = split.materialize_windows()

    for got, want in zip(actual, expected, strict=True):
        assert torch.equal(got, want)

    batch = split.batch_by_rows(1, 4, torch.device("cpu"), non_blocking=False)
    assert torch.equal(batch["x"], expected[0][1:4])
    assert torch.equal(batch["future_log_returns"], expected[1][1:4])
    assert torch.equal(batch["tradable_mask"], expected[2][1:4])
    assert torch.equal(batch["can_buy_mask"], expected[3][1:4])
    assert torch.equal(batch["can_sell_mask"], expected[4][1:4])
    assert torch.equal(batch["benchmark"], expected[5][1:4])
    assert torch.equal(batch["sample_mask"], torch.ones(3, dtype=torch.bool))


def test_dataset_excludes_dates_without_any_finite_target_return() -> None:
    panel = _make_panel(rows=6, symbols=4, features=3)
    panel.returns_1d[-1, :] = np.nan
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)

    assert dataset.valid_indices.tolist() == [1, 2, 3, 4]

    valid_indices, _, _, masks, _, _, _ = _panel_indices_to_tensors(
        panel,
        torch.arange(panel.num_dates).numpy(),
        lookback=2,
    )

    assert valid_indices.tolist() == [1, 2, 3, 4]
    assert masks.all(dim=1).tolist() == [True, True, True, True]


def test_windowed_contiguous_fast_path_matches_indexed_path() -> None:
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=4)
    split = dataset_to_windowed_tensors(dataset)
    assert split._valid_indices_are_contiguous

    fast = split.batch_by_rows(1, 5, torch.device("cpu"), non_blocking=False)
    rows = torch.arange(1, 5, dtype=torch.long)
    indexed = split._batch_from_row_indices(rows, torch.device("cpu"), non_blocking=False)

    assert set(fast) == set(indexed)
    for key in fast:
        assert torch.equal(fast[key], indexed[key]), key


def test_padded_windowed_training_split_keeps_contiguous_prefix_fast_path() -> None:
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)

    assert not split._valid_indices_are_contiguous
    assert split._contiguous_prefix_len == len(dataset)

    first_batch = split.batch_metadata_by_rows(0, 4, torch.device("cpu"), non_blocking=False)
    tail_batch = split.batch_metadata_by_rows(8, 12, torch.device("cpu"), non_blocking=False)

    assert first_batch["date_indices"].tolist() == [1, 2, 3, 4]
    assert first_batch["date_start"].tolist() == [1]
    assert bool(first_batch["rows_are_contiguous"].item()) is True
    assert first_batch["sample_mask"].tolist() == [True, True, True, True]
    assert tail_batch["date_indices"].tolist() == [9, 9, 9, 9]
    assert tail_batch["date_start"].tolist() == [9]
    assert bool(tail_batch["rows_are_contiguous"].item()) is False
    assert tail_batch["sample_mask"].tolist() == [True, False, False, False]
    assert tail_batch["tradable_mask"].all(dim=1).tolist() == [True, True, True, True]


def test_padding_rows_copy_last_valid_mask_for_no_fallback_attention() -> None:
    x = torch.randn(2, 3, 4, 2)
    returns = torch.randn(2, 4)
    masks = torch.tensor(
        [
            [True, False, False, False],
            [False, True, True, False],
        ],
        dtype=torch.bool,
    )
    buy_masks = masks.clone()
    sell_masks = masks.clone()
    benchmark = torch.randn(2)

    padded = _pad_training_tensors(
        x,
        returns,
        masks,
        buy_masks,
        sell_masks,
        benchmark,
        batch_size=4,
    )
    _, _, padded_masks, padded_buy, padded_sell, _, sample_mask = padded

    assert sample_mask.tolist() == [True, True, False, False]
    assert torch.equal(padded_masks[2], masks[-1])
    assert torch.equal(padded_masks[3], masks[-1])
    assert torch.equal(padded_buy[2], buy_masks[-1])
    assert torch.equal(padded_sell[3], sell_masks[-1])

    date_indices = torch.tensor([5, 6], dtype=torch.long)
    padded_meta = _pad_eval_metadata_first_dim(
        date_indices,
        returns,
        masks,
        buy_masks,
        sell_masks,
        benchmark,
        target_rows=4,
    )
    padded_dates, _, meta_masks, meta_buy, meta_sell, _, valid_rows = padded_meta

    assert valid_rows == 2
    assert padded_dates.tolist() == [5, 6, 6, 6]
    assert torch.equal(meta_masks[2], masks[-1])
    assert torch.equal(meta_buy[3], buy_masks[-1])
    assert torch.equal(meta_sell[2], sell_masks[-1])

    padded_chunk = _pad_eval_chunk_first_dim(
        x,
        returns,
        masks,
        buy_masks,
        sell_masks,
        benchmark,
        target_rows=4,
    )
    _, _, chunk_masks, chunk_buy, chunk_sell, _, valid_chunk_rows = padded_chunk

    assert valid_chunk_rows == 2
    assert torch.equal(chunk_masks[2], masks[-1])
    assert torch.equal(chunk_buy[3], buy_masks[-1])
    assert torch.equal(chunk_sell[2], sell_masks[-1])


def test_windowed_shared_base_cache_preserves_batches_without_copying_base() -> None:
    panel = _make_panel(rows=12, symbols=4, features=3)
    first_ds = CrossSectionalDataset(panel, torch.arange(0, 8).numpy(), lookback=3)
    second_ds = CrossSectionalDataset(panel, torch.arange(4, 12).numpy(), lookback=3)
    first = dataset_to_windowed_tensors(first_ds)
    second = dataset_to_windowed_tensors(second_ds)

    shared = _maybe_share_windowed_base_from_cached(
        name="test split",
        split=second,
        cached_base=first,
        device=torch.device("cpu"),
        non_blocking=False,
        enabled=True,
    )

    assert shared is not None
    assert shared.features.data_ptr() == first.features.data_ptr()
    assert shared.future_log_returns.data_ptr() == first.future_log_returns.data_ptr()
    assert shared.valid_indices.data_ptr() != first.valid_indices.data_ptr()

    expected = second.batch_by_rows(0, len(second), torch.device("cpu"), non_blocking=False)
    actual = shared.batch_by_rows(0, len(shared), torch.device("cpu"), non_blocking=False)
    for key in expected:
        assert torch.equal(actual[key], expected[key]), key


def test_prepare_windowed_split_reuses_prepared_shared_base() -> None:
    panel = _make_panel(rows=12, symbols=4, features=3)
    first_ds = CrossSectionalDataset(panel, torch.arange(0, 8).numpy(), lookback=3)
    second_ds = CrossSectionalDataset(panel, torch.arange(4, 12).numpy(), lookback=3)
    first = _prepare_windowed_split(
        dataset_to_windowed_tensors(first_ds),
        torch.device("cpu"),
        non_blocking=False,
        name="first",
    )
    second_raw = dataset_to_windowed_tensors(second_ds)
    second = _prepare_windowed_split(
        second_raw,
        torch.device("cpu"),
        non_blocking=False,
        shared_base=first,
        name="second",
    )

    assert second.features.data_ptr() == first.features.data_ptr()
    assert second.future_log_returns.data_ptr() == first.future_log_returns.data_ptr()
    assert second.tradable_mask.data_ptr() == first.tradable_mask.data_ptr()
    assert second.valid_indices.data_ptr() != first.valid_indices.data_ptr()

    expected = second_raw.batch_by_rows(0, len(second_raw), torch.device("cpu"), non_blocking=False)
    actual = second.batch_by_rows(0, len(second), torch.device("cpu"), non_blocking=False)
    for key in expected:
        assert torch.equal(actual[key], expected[key]), key


def test_prepare_windowed_split_reuses_gpu_shared_base_with_device_metadata() -> None:
    if not torch.cuda.is_available():
        return
    device = torch.device("cuda")
    panel = _make_panel(rows=12, symbols=4, features=3)
    first_ds = CrossSectionalDataset(panel, torch.arange(0, 8).numpy(), lookback=3)
    second_ds = CrossSectionalDataset(panel, torch.arange(4, 12).numpy(), lookback=3)
    first = dataset_to_windowed_tensors(first_ds).to_device_cache(device, non_blocking=False)
    second = _prepare_windowed_split(
        dataset_to_windowed_tensors(second_ds),
        device,
        non_blocking=False,
        shared_base=first,
        name="second gpu",
    )

    assert second.features.device.type == "cuda"
    assert second.valid_indices.device.type == "cuda"
    assert second.features.data_ptr() == first.features.data_ptr()
    batch = second.batch_metadata_by_rows(0, len(second), device, non_blocking=False)
    assert "x" not in batch
    assert batch["date_indices"].device.type == "cuda"
    assert batch["future_log_returns"].device.type == "cuda"


def test_evaluate_windowed_tensor_batch_matches_materialized_eval() -> None:
    panel = _make_panel(rows=9, symbols=5, features=1)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)
    x, returns, masks, can_buy, can_sell, bench = _dataset_to_tensors(dataset)
    split = dataset_to_windowed_tensors(dataset)
    materialized_bt, _, _ = _evaluate_tensor_batch(
        _EchoWeightModel(),
        x,
        returns,
        masks,
        can_buy,
        can_sell,
        bench,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
    )
    windowed_bt, _, _ = _evaluate_windowed_tensor_batch(
        _EchoWeightModel(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
    )

    assert torch.allclose(windowed_bt.strategy_returns.cpu(), materialized_bt.strategy_returns.cpu())
    assert torch.allclose(windowed_bt.turnovers.cpu(), materialized_bt.turnovers.cpu())
    assert torch.allclose(windowed_bt.weights_history.cpu(), materialized_bt.weights_history.cpu())


def test_evaluate_windowed_tensor_batch_panel_slab_wrapper_matches_generic_panel() -> None:
    panel = _make_panel(rows=14, symbols=5, features=2)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)
    split = dataset_to_windowed_tensors(dataset)
    model = _EchoPanelWeightModel(lookback=2)

    generic_bt, _, _ = _evaluate_windowed_tensor_batch(
        model,
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
    )
    slab_bt, _, _ = _evaluate_windowed_tensor_batch(
        model,
        _PanelSlabForwardWrapper(model),
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
    )

    assert torch.allclose(slab_bt.strategy_returns.cpu(), generic_bt.strategy_returns.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(slab_bt.turnovers.cpu(), generic_bt.turnovers.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(slab_bt.weights_history.cpu(), generic_bt.weights_history.cpu(), atol=1e-7, rtol=1e-6)


def test_windowed_eval_timing_breaks_out_batch_prepare_and_h2d() -> None:
    panel = _make_panel(rows=14, symbols=5, features=1)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)
    split = dataset_to_windowed_tensors(dataset)
    timing = TimingBreakdown()

    _evaluate_windowed_tensor_batch(
        _EchoWeightModel(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
        timing_out=timing,
    )

    assert timing.batch_prepare_s > 0.0
    assert timing.window_materialize_s > 0.0
    assert timing.h2d_transfer_s >= 0.0
    assert timing.transfer_s + 1e-9 >= timing.batch_prepare_s + timing.h2d_transfer_s


def test_evaluate_windowed_tensor_batch_decoupled_matches_old_chunking() -> None:
    panel = _make_panel(rows=14, symbols=5, features=1)
    dataset = CrossSectionalDataset(panel, torch.arange(panel.num_dates).numpy(), lookback=2)
    split = dataset_to_windowed_tensors(dataset)

    old, old_ic, old_metrics = _evaluate_windowed_tensor_batch(
        _EchoWeightModel(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
        reset_at_rows=[0, 5, len(split)],
    )
    new, new_ic, new_metrics = _evaluate_windowed_tensor_batch(
        _EchoWeightModel(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        True,
        0.001,
        0.003,
        0.55,
        1.0,
        chunk_rows=3,
        backtest_chunk_rows=8,
        reset_at_rows=[0, 5, len(split)],
    )

    assert torch.allclose(new.strategy_returns.cpu(), old.strategy_returns.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.benchmark_returns.cpu(), old.benchmark_returns.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.turnovers.cpu(), old.turnovers.cpu(), atol=1e-7, rtol=1e-6)
    assert torch.allclose(new.weights_history.cpu(), old.weights_history.cpu(), atol=1e-7, rtol=1e-6)
    for key, value in old_metrics.items():
        assert math.isclose(new_metrics[key], value, rel_tol=1e-6, abs_tol=1e-8), key
    for key, value in old_ic.items():
        assert math.isclose(new_ic[key], value, rel_tol=1e-6, abs_tol=1e-8), key


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


def test_log_utility_loss_uses_fee_adjusted_canonical_tensor_backtest_returns() -> None:
    weights = torch.tensor(
        [
            [0.65, 0.25, 0.10],
            [0.15, 0.75, 0.10],
            [0.50, 0.20, 0.30],
            [0.05, 0.60, 0.35],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.018, -0.008, 0.004],
            [-0.010, 0.014, 0.003],
            [0.005, -0.017, 0.016],
            [-0.003, 0.004, -0.010],
        ],
        dtype=torch.float32,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    benchmark = returns.mean(dim=1)
    buy_fee_rate = 0.000855
    sell_fee_rate = 0.003855

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=True,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
        objective="log_utility",
    )

    bt = run_backtest_torch(
        weights,
        returns,
        mask,
        benchmark,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=True,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        can_buy_mask=mask,
        can_sell_mask=mask,
        return_weights_history=False,
    )
    expected = -bt.strategy_returns.mean() * 252.0

    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_dense_masked_clean_mean_matches_boolean_indexing_semantics() -> None:
    values = torch.tensor(
        [0.01, float("nan"), -0.02, float("inf"), -float("inf"), 0.03],
        dtype=torch.float32,
    )
    valid_mask = torch.tensor([True, True, False, True, False, True])

    old_effective_returns = torch.nan_to_num(values[valid_mask], nan=0.0, posinf=0.0, neginf=0.0)
    old_mean = old_effective_returns.mean()

    new_mean, new_count = _dense_masked_clean_mean(values, valid_mask)

    assert int(new_count.item()) == int(valid_mask.sum().item())
    assert torch.allclose(new_mean, old_mean, atol=1e-8, rtol=1e-6)
    assert torch.allclose(
        torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)[valid_mask],
        old_effective_returns,
        atol=0.0,
        rtol=0.0,
    )


def test_log_utility_loss_sample_mask_dense_path_matches_canonical_backtest_returns() -> None:
    weights = torch.tensor(
        [
            [0.65, 0.25, 0.10],
            [0.15, 0.75, 0.10],
            [0.50, 0.20, 0.30],
            [0.05, 0.60, 0.35],
            [0.40, 0.10, 0.50],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.018, -0.008, 0.004],
            [-0.010, 0.014, 0.003],
            [0.005, -0.017, 0.016],
            [-0.003, 0.004, -0.010],
            [0.012, -0.006, 0.002],
        ],
        dtype=torch.float32,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    sample_mask = torch.tensor([True, False, True, True, False])
    benchmark = returns.mean(dim=1)
    buy_fee_rate = 0.000855
    sell_fee_rate = 0.003855

    get_loss_runtime_stats(reset=True)
    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        sample_mask=sample_mask,
        long_only=True,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
        objective="log_utility",
    )

    bt = run_backtest_torch(
        weights,
        returns,
        mask,
        benchmark,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=True,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        can_buy_mask=mask,
        can_sell_mask=mask,
        return_weights_history=False,
    )
    old_valid_returns = torch.nan_to_num(bt.strategy_returns[sample_mask], nan=0.0, posinf=0.0, neginf=0.0)
    expected = -old_valid_returns.mean() * 252.0

    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()

    stats = get_loss_runtime_stats(reset=True)
    assert stats["prepare_inputs_calls"] >= 1
    assert stats["backtest_calls"] >= 1
    assert stats["log_utility_calls"] >= 1


def test_reduced_and_fused_log_utility_match_canonical_curve_loss_and_gradients() -> None:
    torch.manual_seed(888)
    rows, symbols = 7, 5
    base_weights = torch.randn(rows, symbols, dtype=torch.float32)
    returns = torch.randn(rows, symbols, dtype=torch.float32) * 0.01
    tradable = torch.rand(rows, symbols) > 0.10
    can_buy = torch.rand(rows, symbols) > 0.15
    can_sell = torch.rand(rows, symbols) > 0.15
    tradable[0] = True
    can_buy[0] = True
    can_sell[0] = True
    benchmark = returns.mean(dim=1)
    sample_mask = torch.tensor([True, False, True, True, False, True, True])
    initial_weights = torch.randn(symbols).mul(0.05)
    buy_fee_rate = 0.000855
    sell_fee_rate = 0.003855
    gamma_turnover = 0.2

    old_weights = base_weights.clone().requires_grad_(True)
    old_bt = run_backtest_torch(
        old_weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=False,
        max_turnover_ratio=0.6,
        gross_leverage=1.0,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        return_weights_history=False,
        initial_weights=initial_weights,
    )
    valid_f = sample_mask.to(dtype=torch.float32)
    old_returns = torch.nan_to_num(old_bt.strategy_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)
    old_turnovers = torch.nan_to_num(old_bt.turnovers.float(), nan=0.0, posinf=0.0, neginf=0.0)
    denom = valid_f.sum().clamp_min(1.0)
    old_loss = -(old_returns * valid_f).sum() / denom * 252.0 + gamma_turnover * (old_turnovers * valid_f).sum() / denom
    old_loss.backward()

    new_weights = base_weights.clone().requires_grad_(True)
    reduced = run_backtest_torch_reduced(
        new_weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=False,
        max_turnover_ratio=0.6,
        gross_leverage=1.0,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        sample_mask=sample_mask,
        initial_weights=initial_weights,
        reduction="log_utility",
        gamma_sharpe=1.0,
        gamma_turnover=gamma_turnover,
    )
    reduced.loss.backward()

    fused_weights = base_weights.clone().requires_grad_(True)
    fused_loss, fused_final = fused_log_utility_loss_tensor(
        fused_weights,
        returns,
        tradable,
        can_buy,
        can_sell,
        sample_mask,
        initial_weights,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=False,
        max_turnover_ratio=0.6,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=gamma_turnover,
    )
    fused_loss.backward()

    assert torch.allclose(reduced.loss, old_loss, atol=1e-7, rtol=1e-6)
    assert torch.allclose(fused_loss, old_loss, atol=1e-7, rtol=1e-6)
    assert old_bt.final_weights is not None
    assert reduced.final_weights is not None
    assert torch.allclose(reduced.final_weights, old_bt.final_weights, atol=1e-7, rtol=1e-6)
    assert torch.allclose(fused_final, old_bt.final_weights, atol=1e-7, rtol=1e-6)
    assert old_weights.grad is not None
    assert new_weights.grad is not None
    assert fused_weights.grad is not None
    assert torch.allclose(new_weights.grad, old_weights.grad, atol=1e-7, rtol=1e-5)
    assert torch.allclose(fused_weights.grad, old_weights.grad, atol=1e-7, rtol=1e-5)


def test_fused_log_utility_loss_matches_canonical_backtest_and_gradients() -> None:
    torch.manual_seed(2026)
    rows, symbols = 11, 9
    base_weights = torch.randn(rows, symbols, dtype=torch.float32)
    returns = torch.randn(rows, symbols, dtype=torch.float32) * 0.01
    tradable = torch.rand(rows, symbols) > 0.10
    can_buy = torch.rand(rows, symbols) > 0.15
    can_sell = torch.rand(rows, symbols) > 0.15
    tradable[0] = True
    can_buy[0] = True
    can_sell[0] = True
    benchmark = returns.mean(dim=1)
    sample_mask = torch.tensor([True, True, False, True, False, True, True, False, True, True, True])
    initial_weights = torch.randn(symbols).mul(0.03)
    buy_fee_rate = 0.0005
    sell_fee_rate = 0.0005
    gamma_turnover = 0.15

    ref_weights = base_weights.clone().requires_grad_(True)
    backtest = run_backtest_torch(
        ref_weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=False,
        max_turnover_ratio=0.8,
        gross_leverage=1.0,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        return_weights_history=False,
        initial_weights=initial_weights,
    )
    valid_f = sample_mask.to(dtype=torch.float32)
    denom = valid_f.sum().clamp_min(1.0)
    expected_returns = torch.nan_to_num(backtest.strategy_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)
    expected_turnovers = torch.nan_to_num(backtest.turnovers.float(), nan=0.0, posinf=0.0, neginf=0.0)
    expected_loss = -252.0 * (expected_returns * valid_f).sum() / denom
    expected_loss = expected_loss + gamma_turnover * (expected_turnovers * valid_f).sum() / denom
    expected_loss.backward()

    fused_weights = base_weights.clone().requires_grad_(True)
    fused_loss, fused_final = fused_log_utility_loss_tensor(
        fused_weights,
        returns,
        tradable,
        can_buy,
        can_sell,
        sample_mask,
        initial_weights,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=False,
        max_turnover_ratio=0.8,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=gamma_turnover,
    )
    fused_loss.backward()

    assert torch.allclose(fused_loss, expected_loss, atol=1e-7, rtol=1e-6)
    assert backtest.final_weights is not None
    assert torch.allclose(fused_final, backtest.final_weights, atol=1e-7, rtol=1e-6)
    assert ref_weights.grad is not None
    assert fused_weights.grad is not None
    assert torch.allclose(fused_weights.grad, ref_weights.grad, atol=1e-7, rtol=1e-5)


def test_segmented_log_utility_eval_loss_matches_fused_backtest_rules() -> None:
    torch.manual_seed(2028)
    rows, symbols = 13, 8
    offsets = [0, 4, 9, rows]
    base_weights = torch.randn(rows, symbols, dtype=torch.float32)
    returns = torch.randn(rows, symbols, dtype=torch.float32) * 0.012
    tradable = torch.rand(rows, symbols) > 0.12
    can_buy = torch.rand(rows, symbols) > 0.18
    can_sell = torch.rand(rows, symbols) > 0.16
    for start in offsets[:-1]:
        tradable[start] = True
        can_buy[start] = True
        can_sell[start] = True
    benchmark = returns.mean(dim=1)
    buy_fee_rate = 0.0007
    sell_fee_rate = 0.0011
    gamma_turnover = 0.08

    strategy_parts = []
    benchmark_parts = []
    turnover_parts = []
    fused_losses = []
    for start, end in zip(offsets[:-1], offsets[1:]):
        segment_weights = base_weights[start:end].clone().requires_grad_(True)
        segment_returns = returns[start:end]
        segment_tradable = tradable[start:end]
        segment_can_buy = can_buy[start:end]
        segment_can_sell = can_sell[start:end]
        segment_benchmark = benchmark[start:end]
        sample_mask = torch.ones(end - start, dtype=torch.bool)
        initial_weights = torch.zeros(symbols, dtype=torch.float32)

        backtest = run_backtest_torch(
            segment_weights,
            segment_returns,
            segment_tradable,
            segment_benchmark,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            long_only=False,
            max_turnover_ratio=0.75,
            gross_leverage=1.0,
            can_buy_mask=segment_can_buy,
            can_sell_mask=segment_can_sell,
            return_weights_history=False,
            initial_weights=initial_weights,
        )
        fused_loss, fused_final = fused_log_utility_loss_tensor(
            segment_weights,
            segment_returns,
            segment_tradable,
            segment_can_buy,
            segment_can_sell,
            sample_mask,
            initial_weights,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            long_only=False,
            max_turnover_ratio=0.75,
            gross_leverage=1.0,
            gamma_sharpe=1.0,
            gamma_turnover=gamma_turnover,
        )

        assert backtest.final_weights is not None
        assert torch.allclose(fused_final, backtest.final_weights, atol=1e-7, rtol=1e-6)
        strategy_parts.append(backtest.strategy_returns.detach())
        benchmark_parts.append(backtest.benchmark_returns.detach())
        turnover_parts.append(backtest.turnovers.detach())
        fused_losses.append(fused_loss.detach())

    eval_losses = _batched_loss_from_backtest_segments(
        torch.cat(strategy_parts, dim=0),
        torch.cat(benchmark_parts, dim=0),
        torch.cat(turnover_parts, dim=0),
        offsets,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=gamma_turnover,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
    )
    assert torch.allclose(eval_losses, torch.stack(fused_losses), atol=1e-7, rtol=1e-6)


def test_fused_log_utility_loss_compile_fullgraph_smoke() -> None:
    if not torch.cuda.is_available() or not hasattr(torch, "compile"):
        return
    torch.manual_seed(2027)
    rows, symbols = 8, 6
    device = torch.device("cuda")
    weights = torch.randn(rows, symbols, device=device, dtype=torch.float32, requires_grad=True)
    returns = torch.randn(rows, symbols, device=device, dtype=torch.float32) * 0.01
    mask = torch.ones(rows, symbols, device=device, dtype=torch.bool)
    sample_mask = torch.ones(rows, device=device, dtype=torch.bool)
    initial = torch.zeros(symbols, device=device, dtype=torch.float32)
    compiled = torch.compile(
        fused_log_utility_loss_tensor,
        dynamic=False,
        fullgraph=True,
        options={"triton.cudagraphs": False},
    )
    loss, final_weights = compiled(
        weights,
        returns,
        mask,
        mask,
        mask,
        sample_mask,
        initial,
        buy_fee_rate=0.0005,
        sell_fee_rate=0.0005,
        long_only=False,
        max_turnover_ratio=50.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
    )
    assert torch.isfinite(loss)
    assert final_weights.shape == (symbols,)
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_dense_fast_path_detector_requires_all_masks_true_and_no_turnover_cap() -> None:
    all_true = torch.ones(4, 3, dtype=torch.bool)
    assert simulator.can_use_dense_fast_path(all_true, all_true, all_true, 0.0)
    assert not simulator.can_use_dense_fast_path(all_true, all_true, all_true, 0.1)

    tradable = all_true.clone()
    tradable[1, 2] = False
    assert not simulator.can_use_dense_fast_path(tradable, all_true, all_true, 0.0)

    can_buy = all_true.clone()
    can_buy[2, 1] = False
    assert not simulator.can_use_dense_fast_path(all_true, can_buy, all_true, 0.0)

    can_sell = all_true.clone()
    can_sell[3, 0] = False
    assert not simulator.can_use_dense_fast_path(all_true, all_true, can_sell, 0.0)


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
