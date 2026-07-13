import json
import inspect
import math
import os
import random

import numpy as np
import pytest
import torch
from torch import nn

from stockagent.backtest.simulator import BacktestResultTensor, run_backtest_torch
import stockagent.backtest.simulator as simulator
import stockagent.training.trainer as trainer_module
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import (
    _dense_masked_clean_mean,
    _masked_zscore,
    _resolve_rank_scores,
    factor_generalization_loss,
    get_loss_runtime_stats,
    risk_aware_loss,
    sharpe_aware_loss,
)
from stockagent.training.trainer import (
    _auto_backtest_chunk_rows,
    _batched_loss_from_backtest_segments,
    _CompiledLossFallback,
    _dataset_to_tensors,
    _detach_portfolio_state,
    _distributed_min_int,
    _distributed_probe_succeeded,
    _deployment_test_indices,
    _deployment_test_prefix_rows,
    _estimate_eval_chunk_rows,
    _evaluate_tensor_batch,
    _evaluate_windowed_aux_objective_loss,
    _evaluate_windowed_tensor_batch,
    _maybe_cache_tensors_on_device,
    _maybe_share_windowed_base_from_cached,
    _normalize_ddp_global_batch_size,
    _PanelSlabForwardWrapper,
    _pad_eval_chunk_first_dim,
    _pad_eval_metadata_first_dim,
    _pad_windowed_training_split,
    _prepend_compile_toolchain_paths,
    _prepare_training_split_batch_shape,
    _prepare_windowed_split,
    _probe_compiled_loss_forward_backward,
    _probe_compiled_train_forward,
    _raise_if_distributed_phase_failed,
    _resolve_train_feature_cache_dtype,
    _require_training_aux_outputs,
    _requires_full_objective_evaluation,
    _should_check_finite,
    _train_epoch_windowed_tensor,
    _train_epoch_windowed_tensor_ddp,
    _write_loss_contract_metadata,
    run_inference,
    run_training,
    _loss_from_backtest_series,
    TimingBreakdown,
)
from stockagent.training.windowed import dataset_to_windowed_tensors


class _EchoWeightModel(nn.Module):
    def forward(self, x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        del mask
        return x[:, -1, :, 0]


class _EchoAuxWeightModel(nn.Module):
    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor | dict[str, torch.Tensor]:
        weights = x[:, -1, :, 0].masked_fill(~mask, 0.0)
        if not return_aux:
            return weights
        return {
            "weights": weights,
            "rank_logits": weights,
            "score_logits": weights,
            "latent_z": weights.mean(dim=1, keepdim=True),
        }


class _RequiredAuxTrainModel(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.scale = nn.Parameter(torch.tensor(0.1))
        self.return_aux_requests: list[bool | None] = []

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor | dict[str, torch.Tensor]:
        self.return_aux_requests.append(return_aux)
        weights = (x[:, -1, :, 0] * self.scale).masked_fill(~mask, 0.0)
        if return_aux is not True:
            return weights
        return {
            "weights": weights,
            "score_logits": weights,
            "rank_logits": weights,
            "latent_z": weights.unsqueeze(-1),
        }


class _SlabOnlyTrainModel(nn.Module):
    def __init__(self, lookback: int) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.scale = nn.Parameter(torch.tensor(0.01))
        self.generic_panel_calls = 0

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool = False,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        del return_aux, symbol_indices
        scores = feature_slab[self.lookback - 1 :, :, 0] * self.scale
        return scores.masked_fill(~mask, 0.0)

    def forward_from_panel(self, *args, **kwargs):
        self.generic_panel_calls += 1
        raise AssertionError("padded fixed-shape training must not use generic panel gather")

    def forward(self, *args, **kwargs):
        raise AssertionError("padded fixed-shape training must use panel-slab forward")


class _FeatureDtypeSlabTrainModel(_SlabOnlyTrainModel):
    def __init__(self, lookback: int) -> None:
        super().__init__(lookback)
        self.seen_feature_dtypes: list[torch.dtype] = []

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool = False,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        self.seen_feature_dtypes.append(feature_slab.dtype)
        return super().forward_from_panel_slab(
            feature_slab,
            mask,
            return_aux=return_aux,
            symbol_indices=symbol_indices,
        )


class _BatchNormProbeModel(nn.Module):
    def __init__(self, num_symbols: int) -> None:
        super().__init__()
        self.norm = nn.BatchNorm1d(num_symbols)
        self.scale = nn.Parameter(torch.tensor(1.0))

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        del return_aux, symbol_indices
        scores = self.norm(x[:, -1, :, 0]) * self.scale
        return scores.masked_fill(~mask, 0.0)


def test_backtest_result_tensor_to_numpy_casts_bfloat16_before_numpy_boundary() -> None:
    result = BacktestResultTensor(
        strategy_returns=torch.tensor([0.01, -0.02], dtype=torch.bfloat16),
        benchmark_returns=torch.tensor([0.0, 0.01], dtype=torch.bfloat16),
        turnovers=torch.tensor([0.1, 0.2], dtype=torch.bfloat16),
        weights_history=torch.tensor([[0.5, -0.5], [0.4, -0.4]], dtype=torch.bfloat16),
    ).to_numpy()

    assert result.strategy_returns.dtype == np.float32
    assert result.benchmark_returns.dtype == np.float32
    assert result.turnovers.dtype == np.float32
    assert result.weights_history.dtype == np.float32


def test_bfloat16_model_weights_use_float32_finance_backtest_with_gradients() -> None:
    raw_weights = torch.tensor(
        [[0.6, -0.4], [0.5, -0.5]],
        dtype=torch.bfloat16,
        requires_grad=True,
    )
    future_returns = torch.tensor(
        [[0.01, -0.02], [0.03, 0.01]],
        dtype=torch.float32,
    )
    tradable = torch.ones_like(raw_weights, dtype=torch.bool)

    result = run_backtest_torch(
        raw_weights,
        future_returns,
        tradable,
        torch.zeros(2, dtype=torch.float32),
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        long_only=False,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
    )

    assert result.strategy_returns.dtype == torch.float32
    assert result.benchmark_returns.dtype == torch.float32
    assert result.turnovers.dtype == torch.float32
    assert result.weights_history.dtype == torch.float32
    result.strategy_returns.sum().backward()
    assert raw_weights.grad is not None
    assert torch.isfinite(raw_weights.grad.float()).all()


def test_canonical_log_utility_loss_cpu_fullgraph_matches_eager_with_execution_rules_and_state() -> None:
    if not hasattr(torch, "compile"):
        pytest.skip("torch.compile is unavailable")

    torch.manual_seed(97)
    rows, symbols = 6, 4
    base_weights = torch.randn(rows, symbols, dtype=torch.float32)
    future_returns = torch.randn(rows, symbols, dtype=torch.float32) * 0.01
    tradable = torch.ones(rows, symbols, dtype=torch.bool)
    benchmark = torch.linspace(-0.002, 0.003, rows, dtype=torch.float32)
    can_buy = tradable.clone()
    can_sell = tradable.clone()
    can_short_open = tradable.clone()
    can_short_open[0, 0] = False
    can_short_open[1:3, 2] = False
    force_short_cover = torch.zeros_like(tradable)
    force_short_cover[3, 1] = True
    sample_mask = torch.tensor([True, True, True, True, True, False])
    volume_limit_weights = torch.full((rows, symbols), 0.18, dtype=torch.float32)
    volume_limit_weights[:, -1] = 0.07
    initial_weights = torch.tensor([0.12, -0.10, 0.04, -0.03], dtype=torch.float32)

    def loss_with_state(raw_weights: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        aux_outputs = {"initial_weights": initial_weights}
        loss = risk_aware_loss(
            raw_weights,
            future_returns,
            tradable,
            benchmark_returns=benchmark,
            can_buy_mask=can_buy,
            can_sell_mask=can_sell,
            can_short_open_mask=can_short_open,
            force_short_cover_mask=force_short_cover,
            sample_mask=sample_mask,
            long_only=False,
            buy_fee_rate=0.001425,
            sell_fee_rate=0.004425,
            max_turnover_ratio=0.45,
            volume_limit_weights=volume_limit_weights,
            gross_leverage=0.8,
            min_trade_weight=0.0,
            portfolio_activation="identity",
            gamma_sharpe=1.0,
            gamma_turnover=0.05,
            objective="log_utility",
            aux_outputs=aux_outputs,
            concentration_weight=0.0,
            net_exposure_weight=0.0,
        )
        return loss, aux_outputs["_final_weights"]

    eager_weights = base_weights.clone().requires_grad_(True)
    eager_loss, eager_state = loss_with_state(eager_weights)
    eager_loss.backward()
    eager_grad = eager_weights.grad.detach().clone()

    compiled_loss_fn = torch.compile(
        loss_with_state,
        backend="eager",
        fullgraph=True,
        dynamic=False,
    )
    compiled_weights = base_weights.clone().requires_grad_(True)
    compiled_loss, compiled_state = compiled_loss_fn(compiled_weights)
    compiled_loss.backward()

    assert torch.allclose(compiled_loss, eager_loss, atol=1e-7, rtol=1e-6)
    assert torch.allclose(compiled_state, eager_state, atol=1e-7, rtol=1e-6)
    assert compiled_weights.grad is not None
    assert torch.isfinite(compiled_weights.grad).all()
    assert torch.allclose(compiled_weights.grad, eager_grad, atol=1e-7, rtol=1e-6)


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


def test_long_short_backtest_blocks_new_borrowed_short_without_blocking_long_sell() -> None:
    weights = torch.tensor([[0.4], [-0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((2,), dtype=torch.float32)
    can_buy = torch.ones_like(tradable)
    can_sell = torch.ones_like(tradable)
    can_short_open = torch.zeros_like(tradable)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        can_short_open_mask=can_short_open,
    )

    assert torch.allclose(result.weights_history[:, 0], torch.tensor([0.4, 0.0]))


def test_long_short_backtest_force_cover_clamps_negative_target_to_zero() -> None:
    weights = torch.tensor([[-0.4], [-0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((2,), dtype=torch.float32)
    can_buy = torch.ones_like(tradable)
    can_sell = torch.ones_like(tradable)
    can_short_open = torch.ones_like(tradable)
    force_cover = torch.tensor([[False], [True]], dtype=torch.bool)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        can_short_open_mask=can_short_open,
        force_short_cover_mask=force_cover,
    )

    assert torch.allclose(result.weights_history[:, 0], torch.tensor([-0.4, 0.0]))


def test_force_cover_allows_same_day_flip_from_short_to_long() -> None:
    weights = np.asarray([[-0.4], [0.2]], dtype=np.float32)
    returns = np.zeros_like(weights)
    mask = np.ones_like(weights, dtype=bool)
    force_cover = np.asarray([[False], [True]], dtype=bool)

    numpy_result = simulator.run_backtest(
        weights,
        returns,
        mask,
        np.zeros((2,), dtype=np.float32),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        force_short_cover_mask=force_cover,
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights),
        torch.from_numpy(returns),
        torch.from_numpy(mask),
        torch.zeros((2,), dtype=torch.float32),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(mask),
        can_sell_mask=torch.from_numpy(mask),
        can_short_open_mask=torch.from_numpy(mask),
        force_short_cover_mask=torch.from_numpy(force_cover),
    )

    np.testing.assert_allclose(numpy_result.weights_history[:, 0], [-0.4, 0.2])
    np.testing.assert_allclose(torch_result.weights_history[:, 0].numpy(), [-0.4, 0.2])
    np.testing.assert_allclose(torch_result.turnovers.numpy(), [0.4, 0.6])


def test_mtm_overbudget_state_keeps_large_turnover_cap_active_numpy_torch_parity() -> None:
    weights = np.asarray([[-0.5], [1.0]], dtype=np.float32)
    returns = np.asarray([[math.log(2.0)], [0.0]], dtype=np.float32)
    mask = np.ones_like(weights, dtype=bool)
    common = {
        "buy_fee_rate": 0.0,
        "sell_fee_rate": 0.0,
        "long_only": False,
        "max_turnover_ratio": 2.0,
        "gross_leverage": 1.0,
        "portfolio_activation": "pre_normalized",
    }

    numpy_result = simulator.run_backtest(
        weights,
        returns,
        mask,
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        **common,
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights),
        torch.from_numpy(returns),
        torch.from_numpy(mask),
        torch.zeros((2,), dtype=torch.float32),
        can_buy_mask=torch.from_numpy(mask),
        can_sell_mask=torch.from_numpy(mask),
        can_short_open_mask=torch.from_numpy(mask),
        **common,
    )

    np.testing.assert_allclose(numpy_result.weights_history[:, 0], [-0.5, 0.0])
    np.testing.assert_allclose(torch_result.weights_history[:, 0].numpy(), [-0.5, 0.0])
    np.testing.assert_allclose(numpy_result.turnovers, [0.5, 2.0])
    np.testing.assert_allclose(torch_result.turnovers.numpy(), [0.5, 2.0])


def test_forced_cover_overrides_voluntary_turnover_and_volume_caps_and_charges_buy_fee() -> None:
    weights = torch.tensor([[-1.0]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    buy_fee = 0.01
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        tradable,
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=buy_fee,
        sell_fee_rate=0.02,
        long_only=False,
        max_turnover_ratio=0.05,
        volume_limit_weights=torch.tensor([[0.01]], dtype=torch.float32),
        portfolio_activation="pre_normalized",
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=tradable,
        force_short_cover_mask=tradable,
        initial_weights=torch.tensor([-1.0], dtype=torch.float32),
    )

    assert torch.equal(result.weights_history, torch.zeros_like(weights))
    assert torch.allclose(result.turnovers, torch.tensor([1.0]))
    assert torch.allclose(
        result.strategy_returns,
        torch.log1p(torch.tensor([-buy_fee], dtype=torch.float32)),
        atol=1e-7,
        rtol=1e-6,
    )


def test_backtest_settles_on_first_permanently_untradable_row() -> None:
    weights = torch.tensor([[0.4], [0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.tensor([[True], [False]], dtype=torch.bool)
    benchmark = torch.zeros((2,), dtype=torch.float32)
    can_buy = tradable.clone()
    can_sell = tradable.clone()

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        force_exit_mask=torch.tensor([[False], [True]]),
    )

    assert torch.allclose(result.weights_history[:, 0], torch.tensor([0.4, 0.0]))
    assert torch.allclose(result.turnovers, torch.tensor([0.4, 0.4]))


def test_official_suspension_freezes_position_without_fake_sale_or_fee() -> None:
    weights = torch.tensor([[0.4], [0.0], [0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    can_trade = torch.tensor([[True], [False], [True]], dtype=torch.bool)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        torch.zeros(3),
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=can_trade,
        can_sell_mask=can_trade,
        can_short_open_mask=can_trade,
    )

    drifted = 0.4 / (1.0 - 0.01 * 0.4)
    assert torch.allclose(
        result.weights_history[:, 0],
        torch.tensor([0.4, drifted, 0.4]),
    )
    assert torch.allclose(
        result.turnovers,
        torch.tensor([0.4, 0.0, drifted - 0.4]),
    )
    assert result.strategy_returns[1].item() == pytest.approx(0.0)


def test_generic_nontradable_row_freezes_without_terminal_exit_event() -> None:
    weights = torch.tensor([[0.4], [0.0], [0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.tensor([[True], [False], [True]], dtype=torch.bool)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        torch.zeros(3),
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=tradable,
    )

    drifted = 0.4 / (1.0 - 0.01 * 0.4)
    assert torch.allclose(
        result.weights_history[:, 0],
        torch.tensor([0.4, drifted, 0.4]),
    )
    assert torch.allclose(
        result.turnovers,
        torch.tensor([0.4, 0.0, drifted - 0.4]),
    )
    assert result.strategy_returns[1].item() == pytest.approx(0.0)


def test_nontradable_outer_gate_overrides_inconsistent_side_masks_numpy_and_torch() -> None:
    weights = np.asarray(
        [[0.4, 0.3], [0.0, 0.0], [0.0, 0.0]],
        dtype=np.float32,
    )
    returns = np.zeros_like(weights)
    tradable = np.asarray(
        [[True, False], [False, False], [True, True]],
        dtype=bool,
    )
    # Side masks may restrict tradability, but may not turn an ordinary halt
    # or data hole into an executable row.
    side_masks = np.ones_like(tradable, dtype=bool)
    drifted_after_buy_fee = 0.4 / (1.0 - 0.01 * 0.4)
    expected_weights = np.asarray(
        [[0.4, 0.0], [drifted_after_buy_fee, 0.0], [0.0, 0.0]],
        dtype=np.float32,
    )
    expected_turnover = np.asarray(
        [0.4, 0.0, drifted_after_buy_fee],
        dtype=np.float32,
    )

    numpy_result = simulator.run_backtest(
        weights,
        returns,
        tradable,
        np.zeros((3,), dtype=np.float32),
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=side_masks,
        can_sell_mask=side_masks,
        can_short_open_mask=side_masks,
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights),
        torch.from_numpy(returns),
        torch.from_numpy(tradable),
        torch.zeros((3,), dtype=torch.float32),
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(side_masks),
        can_sell_mask=torch.from_numpy(side_masks),
        can_short_open_mask=torch.from_numpy(side_masks),
    )

    np.testing.assert_allclose(numpy_result.weights_history, expected_weights)
    np.testing.assert_allclose(numpy_result.turnovers, expected_turnover)
    np.testing.assert_allclose(
        torch_result.weights_history.detach().cpu().numpy(),
        expected_weights,
    )
    np.testing.assert_allclose(
        torch_result.turnovers.detach().cpu().numpy(),
        expected_turnover,
    )
    np.testing.assert_allclose(
        torch_result.strategy_returns.detach().cpu().numpy(),
        numpy_result.strategy_returns,
    )


def test_initial_weights_are_detached_trading_state_not_cross_chunk_gradient() -> None:
    weights = torch.tensor([[0.2]], dtype=torch.float32, requires_grad=True)
    initial_weights = torch.tensor([0.5], dtype=torch.float32, requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)

    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        initial_weights=initial_weights,
    )
    result.strategy_returns.sum().backward()

    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert initial_weights.grad is None


def test_mark_to_market_drift_creates_real_rebalance_turnover_and_chunk_state() -> None:
    weights = torch.tensor(
        [[0.5, 0.5], [0.5, 0.5]],
        dtype=torch.float32,
    )
    future_log_returns = torch.tensor(
        [[math.log(2.0), 0.0], [0.0, 0.0]],
        dtype=torch.float32,
    )
    mask = torch.ones_like(weights, dtype=torch.bool)
    common = {
        "buy_fee_rate": 0.0,
        "sell_fee_rate": 0.0,
        "long_only": True,
        "portfolio_activation": "pre_normalized",
    }

    full = run_backtest_torch(
        weights,
        future_log_returns,
        mask,
        torch.zeros((2,), dtype=torch.float32),
        can_buy_mask=mask,
        can_sell_mask=mask,
        **common,
    )
    first = run_backtest_torch(
        weights[:1],
        future_log_returns[:1],
        mask[:1],
        torch.zeros((1,), dtype=torch.float32),
        can_buy_mask=mask[:1],
        can_sell_mask=mask[:1],
        **common,
    )
    second = run_backtest_torch(
        weights[1:],
        future_log_returns[1:],
        mask[1:],
        torch.zeros((1,), dtype=torch.float32),
        can_buy_mask=mask[1:],
        can_sell_mask=mask[1:],
        initial_weights=first.final_weights,
        **common,
    )
    numpy_result = simulator.run_backtest(
        weights.numpy(),
        future_log_returns.numpy(),
        mask.numpy(),
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
        **common,
    )

    assert first.final_weights is not None
    assert torch.allclose(
        first.final_weights,
        torch.tensor([2.0 / 3.0, 1.0 / 3.0]),
        atol=1e-7,
        rtol=1e-6,
    )
    assert torch.allclose(
        full.turnovers,
        torch.tensor([1.0, 1.0 / 3.0]),
        atol=1e-7,
        rtol=1e-6,
    )
    assert torch.allclose(
        torch.cat((first.turnovers, second.turnovers)),
        full.turnovers,
        atol=1e-7,
        rtol=1e-6,
    )
    assert torch.allclose(second.final_weights, full.final_weights)
    np.testing.assert_allclose(
        numpy_result.turnovers,
        full.turnovers.detach().cpu().numpy(),
        atol=1e-7,
        rtol=1e-6,
    )


def test_fee_is_paid_from_cash_in_end_of_period_drift_state() -> None:
    weights = torch.tensor([[0.5]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    mask = torch.ones_like(weights, dtype=torch.bool)
    expected_wealth = 0.95
    expected_state = 0.5 / expected_wealth

    torch_result = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.10,
        sell_fee_rate=0.20,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
    )
    numpy_result = simulator.run_backtest(
        weights.numpy(),
        returns.numpy(),
        mask.numpy(),
        np.zeros((1,), dtype=np.float32),
        buy_fee_rate=0.10,
        sell_fee_rate=0.20,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
    )

    assert torch_result.final_weights is not None
    assert torch_result.final_weights.item() == pytest.approx(expected_state)
    assert torch_result.strategy_returns.item() == pytest.approx(math.log(expected_wealth))
    assert numpy_result.strategy_returns.item() == pytest.approx(math.log(expected_wealth))


def test_bankrupt_net_wealth_is_absorbing_in_full_and_chunked_paths() -> None:
    weights = torch.ones((2, 1), dtype=torch.float32)
    returns = torch.tensor([[math.log(0.01)], [0.0]], dtype=torch.float32)
    mask = torch.ones_like(weights, dtype=torch.bool)

    full = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros((2,), dtype=torch.float32),
        buy_fee_rate=0.02,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
    )
    first = run_backtest_torch(
        weights[:1],
        returns[:1],
        mask[:1],
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.02,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask[:1],
        can_sell_mask=mask[:1],
    )
    second = run_backtest_torch(
        weights[1:],
        returns[1:],
        mask[1:],
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.02,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask[1:],
        can_sell_mask=mask[1:],
        initial_weights=first.final_weights,
        initial_alive=first.final_alive,
    )
    numpy_result = simulator.run_backtest(
        weights.numpy(),
        returns.numpy(),
        mask.numpy(),
        np.zeros((2,), dtype=np.float32),
        buy_fee_rate=0.02,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask.numpy(),
        can_sell_mask=mask.numpy(),
    )

    assert full.final_weights is not None and full.final_alive is not None
    assert torch.equal(full.final_weights, torch.zeros_like(full.final_weights))
    assert not bool(full.final_alive.item())
    assert torch.equal(full.weights_history[1], torch.zeros_like(full.weights_history[1]))
    assert full.turnovers.tolist() == pytest.approx([1.0, 0.0])
    assert torch.isfinite(full.strategy_returns).all()
    assert torch.equal(second.weights_history, full.weights_history[1:])
    assert torch.equal(second.turnovers, full.turnovers[1:])
    assert torch.equal(second.final_alive, full.final_alive)
    np.testing.assert_allclose(numpy_result.weights_history[1], [0.0])
    np.testing.assert_allclose(numpy_result.turnovers, [1.0, 0.0])


def test_integer_backtest_ruin_is_absorbing() -> None:
    weights = np.ones((2, 1), dtype=np.float32)
    returns = np.asarray([[math.log(1e-8)], [0.0]], dtype=np.float32)
    mask = np.ones_like(weights, dtype=bool)

    result, holdings = simulator.run_backtest_integer_shares(
        weights,
        returns,
        mask,
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=mask,
        can_sell_mask=mask,
        initial_capital=100.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="pre_normalized",
        close_prices=np.ones((2, 1), dtype=np.float32),
        symbols=["A"],
        dates=np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    assert result.turnovers.tolist() == pytest.approx([1.0, 0.0])
    np.testing.assert_allclose(result.weights_history[1], [0.0])
    final_rows = [row for row in holdings if row.date == "2024-01-03"]
    assert len(final_rows) == 1
    assert final_rows[0].is_cash and final_rows[0].market_value == 0.0


def test_risk_loss_aux_propagates_absorbing_alive_state() -> None:
    weights = torch.ones((1, 1), dtype=torch.float32, requires_grad=True)
    returns = torch.zeros_like(weights)
    mask = torch.ones_like(weights, dtype=torch.bool)
    aux = {
        "initial_weights": torch.zeros((1,), dtype=torch.float32),
        "initial_alive": torch.tensor(False),
    }

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=True,
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
        objective="log_utility",
        gamma_turnover=0.0,
        concentration_weight=0.0,
        aux_outputs=aux,
    )

    assert loss.item() == pytest.approx(0.0)
    assert torch.equal(aux["_final_weights"], torch.zeros((1,)))
    assert not bool(aux["_final_alive"].item())


def test_eval_backtest_chunk_carries_absorbing_ruin_state() -> None:
    rows = 2
    raw_weights = torch.ones((rows, 1), dtype=torch.float32)
    x = raw_weights[:, None, :, None].contiguous()
    returns = torch.tensor([[math.log(0.01)], [0.0]], dtype=torch.float32)
    mask = torch.ones_like(raw_weights, dtype=torch.bool)
    benchmark = torch.zeros((rows,), dtype=torch.float32)

    def evaluate(backtest_chunk_rows: int) -> BacktestResultTensor:
        result, _, _ = _evaluate_tensor_batch(
            _EchoWeightModel(),
            x,
            returns,
            mask,
            mask,
            mask,
            benchmark,
            torch.device("cpu"),
            None,
            False,
            True,
            0.02,
            0.0,
            0.0,
            1.0,
            chunk_rows=2,
            backtest_chunk_rows=backtest_chunk_rows,
        )
        return result

    full = evaluate(2)
    chunked = evaluate(1)
    assert torch.equal(chunked.weights_history, full.weights_history)
    assert torch.equal(chunked.turnovers, full.turnovers)
    assert torch.equal(chunked.strategy_returns, full.strategy_returns)
    assert chunked.final_alive is not None and not bool(chunked.final_alive.item())


def test_forced_liquidation_charges_sell_fee() -> None:
    weights = torch.tensor([[0.4], [0.4]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.tensor([[True], [False]], dtype=torch.bool)
    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        torch.zeros(2),
        buy_fee_rate=0.0,
        sell_fee_rate=0.01,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        force_exit_mask=torch.tensor([[False], [True]]),
    )
    assert torch.allclose(result.turnovers, torch.tensor([0.4, 0.4]))
    assert torch.allclose(result.strategy_returns[1], torch.log1p(torch.tensor(-0.004)))


@pytest.mark.parametrize(
    ("target_weight", "buy_fee", "sell_fee"),
    [
        (1.0, 0.0, 0.01),
        (-1.0, 0.01, 0.0),
    ],
)
def test_integer_terminal_settlement_matches_canonical_direction_fee(
    target_weight: float,
    buy_fee: float,
    sell_fee: float,
) -> None:
    weights_np = np.asarray([[target_weight], [target_weight]], dtype=np.float32)
    returns_np = np.zeros_like(weights_np)
    tradable_np = np.asarray([[True], [False]], dtype=bool)
    benchmark_np = np.zeros((2,), dtype=np.float32)

    tensor = run_backtest_torch(
        torch.from_numpy(weights_np),
        torch.from_numpy(returns_np),
        torch.from_numpy(tradable_np),
        torch.from_numpy(benchmark_np),
        buy_fee_rate=buy_fee,
        sell_fee_rate=sell_fee,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(tradable_np),
        can_sell_mask=torch.from_numpy(tradable_np),
        can_short_open_mask=torch.from_numpy(tradable_np),
        force_exit_mask=torch.from_numpy(~tradable_np),
    )
    integer, holdings = simulator.run_backtest_integer_shares(
        weights_np,
        returns_np,
        tradable_np,
        benchmark_np,
        can_buy_mask=tradable_np,
        can_sell_mask=tradable_np,
        can_short_open_mask=tradable_np,
        force_exit_mask=np.asarray([[False], [True]], dtype=bool),
        initial_capital=1000.0,
        buy_fee_rate=buy_fee,
        sell_fee_rate=sell_fee,
        long_only=False,
        portfolio_activation="pre_normalized",
        close_prices=np.asarray([[100.0], [100.0]], dtype=np.float32),
        symbols=["A"],
        dates=np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    assert np.allclose(integer.turnovers, tensor.turnovers.cpu().numpy(), atol=1e-7)
    assert np.allclose(
        integer.strategy_returns,
        tensor.strategy_returns.cpu().numpy(),
        atol=1e-7,
    )
    terminal_stock_rows = [
        row
        for row in holdings
        if row.date == "2024-01-03" and not row.is_cash
    ]
    assert terminal_stock_rows == []


@pytest.mark.parametrize("terminal_exit", [False, True])
def test_integer_cash_hold_settles_sale_fee_turnover_and_cash(
    terminal_exit: bool,
) -> None:
    weights = np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    returns = np.zeros_like(weights)
    tradable = np.ones_like(weights, dtype=bool)
    force_exit = np.zeros_like(tradable, dtype=bool)
    force_exit[1, 0] = terminal_exit

    result, holdings = simulator.run_backtest_integer_shares(
        weights,
        returns,
        tradable,
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        force_exit_mask=force_exit,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.10,
        long_only=True,
        portfolio_activation="pre_normalized",
        close_prices=np.asarray(
            [[100.0, 2000.0], [100.0, 2000.0]],
            dtype=np.float32,
        ),
        symbols=["A", "B"],
        dates=np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    # Day two sells the old 1,000 notional, pays 100, and cannot afford one
    # share of B.  Entering cash mode must not erase that real execution.
    np.testing.assert_allclose(result.turnovers, np.asarray([1.0, 1.0]))
    np.testing.assert_allclose(
        result.strategy_returns,
        np.asarray([0.0, math.log(0.9)], dtype=np.float32),
    )
    final_cash = [
        row.market_value
        for row in holdings
        if row.date == "2024-01-03" and row.is_cash
    ]
    assert final_cash == pytest.approx([900.0])


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


def test_compiled_loss_dynamic_symbols_marks_only_asset_axes() -> None:
    observed: dict[str, set[int]] = {}

    def compiled_fn(
        weights: torch.Tensor,
        returns: torch.Tensor,
        tradable: torch.Tensor,
        **kwargs,
    ) -> torch.Tensor:
        observed["weights"] = set(getattr(weights, "_dynamo_dynamic_indices", set()))
        observed["returns"] = set(getattr(returns, "_dynamo_dynamic_indices", set()))
        observed["tradable"] = set(getattr(tradable, "_dynamo_dynamic_indices", set()))
        benchmark = kwargs["benchmark_returns"]
        observed["benchmark"] = set(
            getattr(benchmark, "_dynamo_dynamic_indices", set())
        )
        initial = kwargs["aux_outputs"]["initial_weights"]
        observed["initial"] = set(getattr(initial, "_dynamo_dynamic_indices", set()))
        return weights.sum()

    wrapped = _CompiledLossFallback(
        compiled_fn,
        lambda *args, **kwargs: args[0].sum(),
        label="dynamic-symbol-test",
        dynamic_symbol_axis=True,
        dynamic_symbol_max=2304,
    )
    weights = torch.zeros((8, 13))
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros(8)
    initial = torch.zeros(13)

    wrapped(
        weights,
        returns,
        tradable,
        benchmark_returns=benchmark,
        can_buy_mask=tradable.clone(),
        aux_outputs={"initial_weights": initial},
    )

    assert observed == {
        "weights": {1},
        "returns": {1},
        "tradable": {1},
        "benchmark": set(),
        "initial": {0},
    }


def test_compiled_loss_dynamic_symbols_reuses_one_graph_across_symbol_counts() -> None:
    compile_calls = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal compile_calls
        compile_calls += 1
        return graph_module.forward

    def loss_fn(
        weights: torch.Tensor,
        returns: torch.Tensor,
        tradable: torch.Tensor,
        **_kwargs,
    ) -> torch.Tensor:
        return torch.where(tradable, weights * returns, 0.0).sum()

    compiled = torch.compile(
        loss_fn,
        backend=counting_backend,
        fullgraph=True,
        dynamic=None,
    )
    wrapped = _CompiledLossFallback(
        compiled,
        loss_fn,
        label="dynamic-symbol-graph-test",
        dynamic_symbol_axis=True,
        dynamic_symbol_max=2304,
    )

    for symbols in (7, 19):
        weights = torch.ones((8, symbols))
        returns = torch.ones_like(weights)
        tradable = torch.ones_like(weights, dtype=torch.bool)
        wrapped(
            weights,
            returns,
            tradable,
            benchmark_returns=torch.zeros(8),
            aux_outputs={"initial_weights": torch.zeros(symbols)},
        )

    assert compile_calls == 1


def test_eval_chunk_estimate_uses_full_eval_rows_not_probe_rows() -> None:
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048) == 2048
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=999999) == 4096
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=0) == 1
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048, max_chunk_rows=64) == 64
    assert _estimate_eval_chunk_rows(total_rows=32, estimated_rows=2048, max_chunk_rows=64) == 32
    assert _estimate_eval_chunk_rows(total_rows=4096, estimated_rows=2048, max_chunk_rows=0) == 2048


@pytest.mark.parametrize(
    ("total_rows", "expected"),
    [(64, 64), (65, 128), (98, 128), (128, 128), (129, 256), (513, 512)],
)
def test_auto_backtest_chunk_rows_uses_stable_power_of_two_bucket(
    total_rows: int,
    expected: int,
) -> None:
    assert _auto_backtest_chunk_rows(
        total_rows=total_rows,
        model_chunk_rows=64,
        configured_cap=512,
    ) == expected


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


def test_force_exit_at_backtest_chunk_boundary_matches_full_and_ragged_tail() -> None:
    rows = 5
    raw_weights = torch.ones((rows, 1), dtype=torch.float32)
    x = raw_weights[:, None, :, None].contiguous()
    returns = torch.zeros_like(raw_weights)
    tradable = torch.tensor([[True], [True], [False], [False], [False]])
    can_buy = tradable.clone()
    can_sell = tradable.clone()
    force_exit = torch.tensor([[False], [False], [True], [False], [False]])
    benchmark = torch.zeros((rows,), dtype=torch.float32)

    full, _, _ = _evaluate_tensor_batch(
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
        0.0,
        1.0,
        force_exit_mask=force_exit,
        chunk_rows=3,
        backtest_chunk_rows=rows,
    )
    chunked, _, _ = _evaluate_tensor_batch(
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
        0.0,
        1.0,
        force_exit_mask=force_exit,
        chunk_rows=3,
        backtest_chunk_rows=2,
    )

    assert torch.allclose(chunked.strategy_returns, full.strategy_returns)
    assert torch.allclose(chunked.turnovers, full.turnovers)
    assert torch.equal(chunked.weights_history, full.weights_history)
    assert torch.equal(chunked.final_weights, torch.zeros((1,), dtype=torch.float32))
    after_entry_fee = 1.0 / (1.0 - 0.001)
    day_one_turnover = after_entry_fee - 1.0
    before_force_exit = 1.0 / (1.0 - 0.003 * day_one_turnover)
    assert chunked.turnovers.tolist() == pytest.approx(
        [1.0, day_one_turnover, before_force_exit, 0.0, 0.0]
    )


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


def test_windowed_aux_objective_is_independent_of_model_inference_chunking() -> None:
    panel = _make_panel(rows=11, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = dataset_to_windowed_tensors(dataset)
    model = _EchoAuxWeightModel()

    def evaluate(chunk_rows: int) -> tuple[float, float | None]:
        loss, ic, _ = _evaluate_windowed_aux_objective_loss(
            model,
            split,
            0,
            len(split),
            torch.device("cpu"),
            None,
            False,
            chunk_rows,
            objective="portfolio_autoencoder",
            long_only=False,
            buy_fee_rate=0.001,
            sell_fee_rate=0.002,
            max_turnover_ratio=0.0,
            gross_leverage=1.0,
            gamma_sharpe=1.0,
            gamma_excess=0.0,
            gamma_cvar=0.0,
            cvar_alpha=0.95,
            gamma_drawdown=0.0,
            drawdown_target=0.2,
            gamma_turnover=0.0,
            gamma_underperformance=0.0,
            excess_target=0.0,
            cvar_budget=0.03,
            drawdown_budget=0.2,
            turnover_budget=0.3,
            gamma_cvar_budget=0.0,
            gamma_drawdown_budget=0.0,
            gamma_turnover_budget=0.0,
            rank_ic_weight=1.0,
            return_rank_ic_weight=0.0,
            direction_weight=0.0,
            volatility_regime_weight=0.0,
            concentration_weight=0.0,
            regime_up_threshold=0.002,
            regime_down_threshold=-0.002,
            factor_loss_kwargs={
                "min_trade_weight": 0.0,
                "portfolio_activation": "identity",
                "autoencoder_lambda_turnover": 0.1,
                "autoencoder_lambda_concentration": 0.01,
                "autoencoder_lambda_latent": 0.001,
            },
        )
        return loss, ic

    chunked_loss, chunked_ic = evaluate(2)
    single_loss, single_ic = evaluate(len(split))
    assert chunked_loss == pytest.approx(single_loss, rel=1e-6, abs=1e-7)
    assert chunked_ic == pytest.approx(single_ic, rel=1e-6, abs=1e-7)


def test_windowed_eval_uses_same_deterministic_aux_loss_as_training() -> None:
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = dataset_to_windowed_tensors(dataset)
    model = _EchoAuxWeightModel()
    batch = split.batch_by_rows(0, len(split), torch.device("cpu"), False)
    model_output = model(batch["x"], batch["tradable_mask"], return_aux=True)
    assert isinstance(model_output, dict)
    weights = model_output["weights"]
    aux_outputs = {
        "rank_logits": model_output["rank_logits"],
        "score_logits": model_output["score_logits"],
    }
    loss_kwargs = {
        "long_only": False,
        "buy_fee_rate": 0.001,
        "sell_fee_rate": 0.002,
        "max_turnover_ratio": 0.0,
        "gross_leverage": 1.0,
        "gamma_sharpe": 1.0,
        "gamma_excess": 0.0,
        "gamma_cvar": 0.0,
        "cvar_alpha": 0.95,
        "gamma_drawdown": 0.0,
        "drawdown_target": 0.2,
        "gamma_turnover": 0.0,
        "gamma_underperformance": 0.0,
        "excess_target": 0.0,
        "cvar_budget": 0.03,
        "drawdown_budget": 0.2,
        "turnover_budget": 0.3,
        "gamma_cvar_budget": 0.0,
        "gamma_drawdown_budget": 0.0,
        "gamma_turnover_budget": 0.0,
        "objective": "log_utility",
        "rank_ic_weight": 1.0,
        "return_rank_ic_weight": 0.4,
        "direction_weight": 0.3,
        "volatility_regime_weight": 0.0,
        "concentration_weight": 0.0,
        "regime_up_threshold": 0.002,
        "regime_down_threshold": -0.002,
    }
    direct = risk_aware_loss(
        weights,
        batch["future_log_returns"],
        batch["tradable_mask"],
        benchmark_returns=batch["benchmark"],
        can_buy_mask=batch["can_buy_mask"],
        can_sell_mask=batch["can_sell_mask"],
        can_short_open_mask=batch["can_short_open_mask"],
        force_short_cover_mask=batch["force_short_cover_mask"],
        sample_mask=batch["sample_mask"],
        aux_outputs=aux_outputs,
        min_trade_weight=0.0,
        portfolio_activation="identity",
        **loss_kwargs,
    )
    evaluated, _, _ = _evaluate_windowed_aux_objective_loss(
        model,
        split,
        0,
        len(split),
        torch.device("cpu"),
        None,
        False,
        2,
        factor_loss_kwargs={"min_trade_weight": 0.0, "portfolio_activation": "identity"},
        **loss_kwargs,
    )

    assert evaluated == pytest.approx(float(direct), rel=1e-6, abs=1e-7)


def test_aux_objective_training_forces_return_aux_true() -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = dataset_to_windowed_tensors(dataset)
    model = _RequiredAuxTrainModel()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    scaler = torch.amp.GradScaler("cuda", enabled=False)

    loss, timing = _train_epoch_windowed_tensor(
        model,
        None,
        risk_aware_loss,
        split,
        optimizer,
        scaler,
        batch_size=3,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        long_only=False,
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.95,
        gamma_drawdown=0.0,
        drawdown_target=0.2,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.03,
        drawdown_budget=0.2,
        turnover_budget=0.3,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="portfolio_autoencoder",
        grad_clip_norm=1.0,
    )

    assert torch.isfinite(loss)
    assert timing.batches > 0
    assert model.return_aux_requests
    assert all(request is True for request in model.return_aux_requests)


def test_compile_probe_uses_fixed_aux_shape_and_restores_rng_and_gradients() -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    model = _RequiredAuxTrainModel()
    torch.manual_seed(1234)
    rng_before = torch.get_rng_state().clone()
    observed_output_dtypes: list[torch.dtype] = []

    ok, error = _probe_compiled_train_forward(
        model,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        use_panel_slab=False,
        return_aux=True,
        objective="portfolio_autoencoder",
        factor_aug_kwargs=None,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        observed_output_dtypes=observed_output_dtypes,
    )

    assert ok, error
    assert torch.equal(torch.get_rng_state(), rng_before)
    assert model.return_aux_requests == [True]
    assert observed_output_dtypes == [torch.float32]
    assert all(parameter.grad is None for parameter in model.parameters())


def test_compile_probe_routes_panel_slab_without_generic_gather() -> None:
    lookback = 2
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=lookback)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    base_model = _SlabOnlyTrainModel(lookback)
    slab_model = _PanelSlabForwardWrapper(base_model)

    ok, error = _probe_compiled_train_forward(
        slab_model,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        use_panel_slab=True,
        return_aux=False,
        objective="log_utility",
        factor_aug_kwargs=None,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
    )

    assert ok, error
    assert base_model.generic_panel_calls == 0
    assert all(parameter.grad is None for parameter in base_model.parameters())


def test_compile_probe_uses_actual_cached_feature_dtype_for_panel_slab() -> None:
    lookback = 2
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=lookback)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    split.features = split.features.to(dtype=torch.bfloat16)
    base_model = _FeatureDtypeSlabTrainModel(lookback)
    slab_model = _PanelSlabForwardWrapper(base_model)

    ok, error = _probe_compiled_train_forward(
        slab_model,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        use_panel_slab=True,
        return_aux=False,
        objective="log_utility",
        factor_aug_kwargs=None,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
    )

    assert ok, error
    assert base_model.seen_feature_dtypes == [torch.bfloat16]
    assert split.features.dtype == torch.bfloat16
    assert all(parameter.grad is None for parameter in base_model.parameters())


def test_compile_probe_restores_stateful_buffers_mixed_modes_and_existing_gradients() -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    model = _BatchNormProbeModel(num_symbols=panel.num_symbols)
    model.train()
    model.norm.eval()
    model.scale.grad = torch.tensor(7.0)
    modes_before = {name: module.training for name, module in model.named_modules()}
    buffers_before = {name: value.detach().clone() for name, value in model.named_buffers()}
    grad_before = model.scale.grad

    ok, error = _probe_compiled_train_forward(
        model,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        use_panel_slab=False,
        return_aux=False,
        objective="log_utility",
        factor_aug_kwargs=None,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
    )

    assert ok, error
    assert {name: module.training for name, module in model.named_modules()} == modes_before
    for name, value in model.named_buffers():
        assert torch.equal(value, buffers_before[name])
    assert model.scale.grad is grad_before
    assert torch.equal(model.scale.grad, torch.tensor(7.0))


def test_compiled_loss_probe_executes_rules_and_backward_at_fixed_batch_shape() -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    split.force_exit_mask[split.valid_indices[0], 0] = True
    captured: dict[str, torch.Tensor] = {}

    def loss_fn(
        weights: torch.Tensor,
        returns: torch.Tensor,
        tradable: torch.Tensor,
        **kwargs,
    ) -> torch.Tensor:
        del tradable
        captured["weights"] = weights
        captured["force_exit"] = kwargs["force_exit_mask"].detach().clone()
        return (weights.square() + weights * returns).mean()

    ok, error = _probe_compiled_loss_forward_backward(
        loss_fn,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        loss_kwargs={},
        max_volume_participation=0.0,
        volume_participation_equity=1_000_000.0,
        weights_dtype=torch.float64,
    )

    assert ok, error
    assert captured["weights"].shape == (4, panel.num_symbols)
    assert captured["weights"].dtype == torch.float64
    assert captured["weights"].grad is not None
    assert bool(captured["force_exit"][0, 0]) is True


def test_compiled_loss_probe_reproduces_ddp_gathered_inputs(monkeypatch) -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    gather_calls = {"autograd": 0, "no_grad": 0}
    captured: dict[str, torch.Tensor] = {}

    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 0)

    def gather_autograd(value: torch.Tensor) -> torch.Tensor:
        gather_calls["autograd"] += 1
        return torch.cat((value, value), dim=0)

    def gather_no_grad(value: torch.Tensor) -> torch.Tensor:
        gather_calls["no_grad"] += 1
        return torch.cat((value, value), dim=0)

    monkeypatch.setattr(trainer_module, "_all_gather_autograd", gather_autograd)
    monkeypatch.setattr(trainer_module, "_all_gather_no_grad", gather_no_grad)

    def loss_fn(
        weights: torch.Tensor,
        returns: torch.Tensor,
        tradable: torch.Tensor,
        **_kwargs,
    ) -> torch.Tensor:
        captured["weights"] = weights
        captured["returns"] = returns
        captured["tradable"] = tradable
        return (weights.square() + weights * returns).mean()

    ok, error = _probe_compiled_loss_forward_backward(
        loss_fn,
        split,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        loss_kwargs={},
        max_volume_participation=0.0,
        volume_participation_equity=1_000_000.0,
    )

    assert ok, error
    assert gather_calls == {"autograd": 1, "no_grad": 9}
    assert captured["weights"].shape == (4, panel.num_symbols)
    assert captured["returns"].shape == captured["weights"].shape
    assert captured["tradable"].shape == captured["weights"].shape


def test_portfolio_autoencoder_aux_contract_fails_without_latent() -> None:
    with pytest.raises(RuntimeError, match="latent_z"):
        _require_training_aux_outputs(
            "portfolio_autoencoder",
            {"rank_logits": torch.ones(2, 3)},
        )


def test_rank_score_contract_prefers_explicit_heads_then_canonical_weights() -> None:
    weights = torch.tensor([[0.2, -0.2]], dtype=torch.float32)
    score_logits = torch.tensor([[1.0, 2.0]], dtype=torch.float32)
    rank_logits = torch.tensor([[3.0, 4.0]], dtype=torch.float32)

    assert _resolve_rank_scores(weights, None) is weights
    assert _resolve_rank_scores(weights, {"score_logits": score_logits}) is score_logits
    assert _resolve_rank_scores(
        weights,
        {"score_logits": score_logits, "rank_logits": rank_logits},
    ) is rank_logits


def test_enabled_aux_penalties_fail_fast_only_for_tensors_they_need() -> None:
    # Rank scores may canonically be the portfolio weights, so rank objectives
    # do not require an independent rank head.
    _require_training_aux_outputs("rank_ic", None)
    _require_training_aux_outputs("pure_rank", None)

    with pytest.raises(RuntimeError, match="score_logits"):
        _require_training_aux_outputs("rank_ic", None, direction_weight=0.1)
    _require_training_aux_outputs(
        "rank_ic",
        {"score_logits": torch.ones(2, 3)},
        direction_weight=0.1,
    )

    with pytest.raises(RuntimeError, match="volatility_pred"):
        _require_training_aux_outputs("log_utility", None, volatility_regime_weight=0.1)
    _require_training_aux_outputs(
        "log_utility",
        {"regime_logits": torch.ones(2, 3)},
        volatility_regime_weight=0.1,
    )

    with pytest.raises(RuntimeError, match="aug_score_logits"):
        _require_training_aux_outputs(
            "factor_generalization",
            {"rank_logits": torch.ones(2, 3)},
            factor_aug_required=True,
        )


def test_deterministic_aux_penalties_require_full_validation_loss() -> None:
    assert not _requires_full_objective_evaluation(
        "log_utility",
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
    )
    assert _requires_full_objective_evaluation(
        "log_utility",
        return_rank_ic_weight=0.1,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
    )
    assert _requires_full_objective_evaluation(
        "sharpe",
        return_rank_ic_weight=0.0,
        direction_weight=0.1,
        volatility_regime_weight=0.0,
    )


def test_loss_contract_marks_factor_augmentation_train_only(tmp_path) -> None:
    path = tmp_path / "loss_contract.json"
    _write_loss_contract_metadata(
        path,
        objective="factor_generalization",
        return_rank_ic_weight=0.2,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        factor_aug_kwargs={"noise_std": 0.01},
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["deterministic_aux_penalties"]["scope"] == "train_validation_test"
    assert payload["factor_consistency_augmentation"]["scope"] == "train_only"
    assert payload["factor_consistency_augmentation"]["validation_test_included"] is False


def test_rank_auxiliary_losses_ignore_sample_masked_padding_rows() -> None:
    weights = torch.tensor(
        [[0.9, -0.2, -0.7], [-0.6, 0.8, -0.2]],
        dtype=torch.float32,
    )
    returns = torch.tensor(
        [[0.03, -0.01, -0.02], [-0.02, 0.04, -0.01]],
        dtype=torch.float32,
    )
    tradable = torch.ones_like(weights, dtype=torch.bool)
    padded_weights = torch.cat((weights, weights[-1:].expand(2, -1)), dim=0)
    padded_returns = torch.cat((returns, returns[-1:].expand(2, -1)), dim=0)
    padded_tradable = torch.ones_like(padded_weights, dtype=torch.bool)
    sample_mask = torch.tensor([True, True, False, False])

    common = {
        "long_only": False,
        "buy_fee_rate": 0.0,
        "sell_fee_rate": 0.0,
        "gamma_sharpe": 0.0,
        "gamma_turnover": 0.0,
        "rank_ic_weight": 1.0,
    }
    base_return_rank = risk_aware_loss(
        weights,
        returns,
        tradable,
        objective="log_utility",
        return_rank_ic_weight=1.0,
        **common,
    )
    padded_return_rank = risk_aware_loss(
        padded_weights,
        padded_returns,
        padded_tradable,
        sample_mask=sample_mask,
        objective="log_utility",
        return_rank_ic_weight=1.0,
        **common,
    )
    base_sharpe_rank = risk_aware_loss(
        weights,
        returns,
        tradable,
        objective="sharpe",
        return_rank_ic_weight=1.0,
        **common,
    )
    padded_sharpe_rank = risk_aware_loss(
        padded_weights,
        padded_returns,
        padded_tradable,
        sample_mask=sample_mask,
        objective="sharpe",
        return_rank_ic_weight=1.0,
        **common,
    )
    base_pure_rank = risk_aware_loss(
        weights,
        returns,
        tradable,
        objective="pure_rank",
        **common,
    )
    padded_pure_rank = risk_aware_loss(
        padded_weights,
        padded_returns,
        padded_tradable,
        sample_mask=sample_mask,
        objective="pure_rank",
        **common,
    )

    assert torch.allclose(padded_return_rank, base_return_rank, atol=1e-7, rtol=1e-7)
    assert torch.allclose(padded_sharpe_rank, base_sharpe_rank, atol=1e-7, rtol=1e-7)
    assert torch.allclose(padded_pure_rank, base_pure_rank, atol=1e-7, rtol=1e-7)


def test_rank_multitask_aux_penalties_ignore_sample_masked_padding_rows() -> None:
    weights = torch.tensor(
        [[0.5, -0.2, -0.3], [-0.4, 0.7, -0.3]],
        dtype=torch.float32,
    )
    returns = torch.tensor(
        [[0.03, -0.02, 0.01], [-0.01, 0.04, -0.03]],
        dtype=torch.float32,
    )
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = returns.mean(dim=1)
    score_logits = weights * 2.0
    volatility_pred = returns.abs() + 0.01
    regime_logits = torch.tensor([[0.1, 0.2, 0.7], [0.6, 0.3, 0.1]], dtype=torch.float32)

    base = risk_aware_loss(
        weights,
        returns,
        tradable,
        benchmark_returns=benchmark,
        objective="rank_ic",
        aux_outputs={
            "rank_logits": score_logits,
            "score_logits": score_logits,
            "volatility_pred": volatility_pred,
            "regime_logits": regime_logits,
        },
        rank_ic_weight=1.0,
        direction_weight=0.7,
        volatility_regime_weight=0.4,
    )

    padded_rows = 2
    padded_weights = torch.cat((weights, torch.full((padded_rows, 3), 50.0)), dim=0)
    padded_returns = torch.cat((returns, torch.full((padded_rows, 3), -50.0)), dim=0)
    padded_tradable = torch.ones_like(padded_weights, dtype=torch.bool)
    padded = risk_aware_loss(
        padded_weights,
        padded_returns,
        padded_tradable,
        benchmark_returns=torch.cat((benchmark, torch.full((padded_rows,), -50.0))),
        sample_mask=torch.tensor([True, True, False, False]),
        objective="rank_ic",
        aux_outputs={
            "rank_logits": torch.cat((score_logits, torch.full((padded_rows, 3), -50.0))),
            "score_logits": torch.cat((score_logits, torch.full((padded_rows, 3), 50.0))),
            "volatility_pred": torch.cat((volatility_pred, torch.full((padded_rows, 3), 50.0))),
            "regime_logits": torch.cat((regime_logits, torch.full((padded_rows, 3), -50.0))),
        },
        rank_ic_weight=1.0,
        direction_weight=0.7,
        volatility_regime_weight=0.4,
    )

    assert torch.allclose(padded, base, atol=1e-7, rtol=1e-7)


def test_autoencoder_latent_regularizer_ignores_sample_masked_padding_rows() -> None:
    weights = torch.tensor([[0.5, -0.3, -0.2], [-0.2, 0.6, -0.4]], dtype=torch.float32)
    returns = torch.tensor([[0.02, -0.01, 0.00], [-0.01, 0.03, -0.02]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    latent = torch.tensor([[[0.1], [0.2], [0.3]], [[0.2], [0.1], [0.4]]], dtype=torch.float32)
    common = {
        "objective": "portfolio_autoencoder",
        "long_only": False,
        "buy_fee_rate": 0.0,
        "sell_fee_rate": 0.0,
        "autoencoder_lambda_turnover": 0.0,
        "autoencoder_lambda_concentration": 0.0,
        "autoencoder_lambda_latent": 0.7,
    }
    base = risk_aware_loss(
        weights,
        returns,
        tradable,
        aux_outputs={"latent_z": latent},
        **common,
    )

    padded = risk_aware_loss(
        torch.cat((weights, torch.full((2, 3), 25.0))),
        torch.cat((returns, torch.full((2, 3), -25.0))),
        torch.ones((4, 3), dtype=torch.bool),
        sample_mask=torch.tensor([True, True, False, False]),
        aux_outputs={"latent_z": torch.cat((latent, torch.full((2, 3, 1), 100.0)))},
        **common,
    )

    assert torch.allclose(padded, base, atol=1e-7, rtol=1e-7)


def test_factor_generalization_applies_portfolio_activation_once_in_canonical_backtest() -> None:
    scores = torch.tensor(
        [
            [3.0, -1.0, 0.25],
            [-0.5, 2.0, -3.0],
            [1.5, -2.5, 0.75],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.tensor(
        [
            [0.03, -0.01, 0.01],
            [-0.02, 0.04, -0.01],
            [0.02, -0.03, 0.005],
        ],
        dtype=torch.float32,
    )
    tradable = torch.ones_like(scores, dtype=torch.bool)
    temperature = 0.7

    actual = factor_generalization_loss(
        scores,
        returns,
        tradable,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        long_only=False,
        buy_fee_rate=0.001,
        sell_fee_rate=0.003,
        portfolio_activation="softsign",
        slope_tstat_weight=0.0,
        rank_ic_weight=0.0,
        factor_sharpe_weight=1.0,
        block_stability_weight=0.0,
        regime_stability_weight=0.0,
        consistency_weight=0.0,
        net_exposure_weight=0.0,
        gross_exposure_weight=0.0,
        concentration_weight=0.0,
        turnover_weight=0.0,
        score_l2_weight=0.0,
        factor_temperature=temperature,
    )

    factor_scores = _masked_zscore(scores, tradable) / temperature
    canonical = run_backtest_torch(
        factor_scores,
        returns,
        tradable,
        returns.mean(dim=1),
        buy_fee_rate=0.001,
        sell_fee_rate=0.003,
        long_only=False,
        portfolio_activation="softsign",
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        return_weights_history=False,
    )
    factor_returns = canonical.strategy_returns
    mean_return = factor_returns.mean()
    std_return = torch.sqrt((factor_returns - mean_return).pow(2).mean() + 1e-8)
    expected = -(mean_return / std_return) * math.sqrt(float(factor_returns.numel()))

    assert torch.allclose(actual, expected, atol=1e-7, rtol=1e-6)
    actual.backward()
    assert scores.grad is not None
    assert torch.isfinite(scores.grad).all()


def test_factor_consistency_regularizer_ignores_sample_masked_padding_rows() -> None:
    weights = torch.tensor([[0.5, -0.3, -0.2], [-0.2, 0.6, -0.4]], dtype=torch.float32)
    returns = torch.tensor([[0.02, -0.01, 0.00], [-0.01, 0.03, -0.02]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    scores = weights * 1.3
    aug_scores = weights * 0.9
    common = {
        "objective": "factor_generalization",
        "long_only": False,
        "buy_fee_rate": 0.0,
        "sell_fee_rate": 0.0,
        "factor_slope_tstat_weight": 0.0,
        "factor_rank_ic_weight": 0.0,
        "factor_sharpe_weight": 0.0,
        "factor_block_stability_weight": 0.0,
        "factor_regime_stability_weight": 0.0,
        "factor_consistency_weight": 1.0,
        "factor_net_exposure_weight": 0.0,
        "factor_gross_exposure_weight": 0.0,
        "factor_concentration_weight": 0.0,
        "factor_turnover_weight": 0.0,
        "factor_score_l2_weight": 0.0,
    }
    base = risk_aware_loss(
        weights,
        returns,
        tradable,
        aux_outputs={"rank_logits": scores, "aug_score_logits": aug_scores},
        **common,
    )
    padded = risk_aware_loss(
        torch.cat((weights, torch.full((2, 3), 25.0))),
        torch.cat((returns, torch.full((2, 3), -25.0))),
        torch.ones((4, 3), dtype=torch.bool),
        sample_mask=torch.tensor([True, True, False, False]),
        aux_outputs={
            "rank_logits": torch.cat((scores, torch.full((2, 3), 50.0))),
            "aug_score_logits": torch.cat((aug_scores, torch.full((2, 3), -50.0))),
        },
        **common,
    )

    assert torch.allclose(padded, base, atol=1e-7, rtol=1e-7)


def test_finite_check_always_runs_on_final_step() -> None:
    assert not _should_check_finite(3, 100, final_step=False)
    assert _should_check_finite(3, 100, final_step=True)
    assert _should_check_finite(100, 100, final_step=False)


def test_deployment_test_indices_assign_next_year_warmup_to_previous_model() -> None:
    lookback = 4
    dates = np.concatenate(
        [
            np.arange("2020-01-01", "2020-01-11", dtype="datetime64[D]"),
            np.arange("2021-01-01", "2021-01-11", dtype="datetime64[D]"),
            np.arange("2022-01-01", "2022-01-11", dtype="datetime64[D]"),
            np.arange("2023-01-01", "2023-01-11", dtype="datetime64[D]"),
        ]
    )
    rows = int(dates.size)
    symbols = 2
    features = 3
    mask = np.ones((rows, symbols), dtype=bool)
    panel = PanelData(
        dates=dates,
        symbols=["A", "B"],
        feature_names=[f"f{i}" for i in range(features)],
        features=np.ones((rows, symbols, features), dtype=np.float32),
        returns_1d=np.ones((rows, symbols), dtype=np.float32) * 0.001,
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros((rows,), dtype=np.float32),
        close_prices=np.ones((rows, symbols), dtype=np.float32),
    )
    folds = build_expanding_year_folds(
        dates,
        min_train_years=1,
        val_years=1,
        require_future_test_year=True,
    )
    assert [fold.test_years[0] for fold in folds] == [2022, 2023]

    first_indices = _deployment_test_indices(panel, folds[0], folds[1], lookback)
    second_indices = _deployment_test_indices(panel, folds[1], None, lookback)
    first_ds = CrossSectionalDataset(panel, first_indices, lookback)
    second_ds = CrossSectionalDataset(panel, second_indices, lookback)
    first_full_ds = CrossSectionalDataset(panel, folds[0].test_indices, lookback)

    assert panel.dates[first_ds.valid_indices[0]] == np.datetime64("2022-01-04")
    assert panel.dates[first_ds.valid_indices[-1]] == np.datetime64("2023-01-03")
    assert panel.dates[second_ds.valid_indices[0]] == np.datetime64("2023-01-04")
    assert np.intersect1d(first_ds.valid_indices, second_ds.valid_indices).size == 0
    assert _deployment_test_prefix_rows(
        panel,
        folds[0],
        folds[1],
        lookback,
        first_full_ds.valid_indices,
    ) == len(first_ds)
    assert np.array_equal(
        first_ds.valid_indices,
        first_full_ds.valid_indices[: len(first_ds)],
    )


def test_full_test_survives_zero_row_experimental_deployment_handoff() -> None:
    lookback = 4
    dates = np.concatenate(
        [
            np.arange(f"{year}-01-01", f"{year}-01-11", dtype="datetime64[D]")
            for year in range(2020, 2024)
        ]
    )
    rows = int(dates.size)
    mask = np.ones((rows, 2), dtype=bool)
    panel = PanelData(
        dates=dates,
        symbols=["A", "B"],
        feature_names=["f0"],
        features=np.ones((rows, 2, 1), dtype=np.float32),
        returns_1d=np.ones((rows, 2), dtype=np.float32) * 0.001,
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros((rows,), dtype=np.float32),
        close_prices=np.ones((rows, 2), dtype=np.float32),
    )
    folds = build_expanding_year_folds(
        dates,
        min_train_years=1,
        val_years=1,
        require_future_test_year=False,
    )
    penultimate = folds[-2]
    experimental = folds[-1]
    assert penultimate.test_years == experimental.test_years == [2023]

    full_ds = CrossSectionalDataset(panel, penultimate.test_indices, lookback)
    assert len(full_ds) > 0
    assert _deployment_test_prefix_rows(
        panel,
        penultimate,
        experimental,
        lookback,
        full_ds.valid_indices,
    ) == 0


def test_tensor_metrics_max_drawdown_includes_initial_nav() -> None:
    log_returns = torch.log1p(torch.tensor([-0.10, 0.01, 0.01], dtype=torch.float64))
    metrics = trainer_module._compute_metrics_from_tensors(
        log_returns,
        torch.zeros_like(log_returns),
        torch.zeros_like(log_returns),
    )

    assert metrics["max_drawdown"] == pytest.approx(-0.10)


def test_artifact_migration_rng_guard_restores_process_streams() -> None:
    original_python_state = random.getstate()
    original_numpy_state = np.random.get_state()
    original_torch_state = torch.get_rng_state().clone()
    try:
        random.seed(71)
        np.random.seed(72)
        torch.manual_seed(73)
        python_state = random.getstate()
        numpy_state = np.random.get_state()
        torch_state = torch.get_rng_state().clone()

        with trainer_module._preserve_process_rng_state():
            random.random()
            np.random.random()
            torch.rand(4)

        restored_numpy_state = np.random.get_state()
        assert random.getstate() == python_state
        assert restored_numpy_state[0] == numpy_state[0]
        np.testing.assert_array_equal(restored_numpy_state[1], numpy_state[1])
        assert restored_numpy_state[2:] == numpy_state[2:]
        assert torch.equal(torch.get_rng_state(), torch_state)
    finally:
        random.setstate(original_python_state)
        np.random.set_state(original_numpy_state)
        torch.set_rng_state(original_torch_state)


def test_rank0_store_phase_rejects_uninitialized_multi_rank(monkeypatch) -> None:
    called = False

    def _operation() -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: False)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)

    with pytest.raises(RuntimeError, match="requires an initialized process group"):
        trainer_module._run_rank0_store_synchronized_phase("probe", _operation)
    assert not called


def test_cross_sectional_dataset_allows_empty_split_when_requested() -> None:
    panel = _make_panel(rows=3)
    date_indices = np.arange(panel.num_dates)

    with pytest.raises(ValueError, match="insufficient data"):
        CrossSectionalDataset(panel, date_indices, lookback=4)

    dataset = CrossSectionalDataset(panel, date_indices, lookback=4, allow_empty=True)

    assert len(dataset) == 0
    assert dataset.valid_indices.size == 0


def test_dataset_can_omit_disabled_volume_notional() -> None:
    panel = _make_panel(rows=6, symbols=4, features=3)
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        include_volume_notional=False,
    )
    split = dataset_to_windowed_tensors(dataset)

    assert dataset.volume_notional_t is None
    assert split.volume_notional is None
    assert "volume_notional" not in dataset[0]
    assert "volume_notional" not in split.panel_slab_batch_by_rows(
        0,
        2,
        torch.device("cpu"),
        non_blocking=False,
    )


def test_optional_volume_none_survives_windowed_cache_and_padding() -> None:
    panel = _make_panel(rows=7, symbols=4, features=3)
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=3,
        include_volume_notional=False,
    )
    split = dataset_to_windowed_tensors(dataset)
    prepared = _prepare_windowed_split(split, torch.device("cpu"), non_blocking=False)
    cached = prepared.to_device_cache(torch.device("cpu"), non_blocking=False)
    symbol_padded = cached.subset_symbols(torch.tensor([0, 2])).pad_symbols(
        4,
        pad_symbol_index=0,
    )
    row_padded = _pad_windowed_training_split(cached, batch_size=4)

    for candidate in (split, prepared, cached, symbol_padded, row_padded):
        assert candidate.volume_notional is None
        batch = candidate.batch_by_rows(0, min(2, len(candidate)), torch.device("cpu"), non_blocking=False)
        assert "volume_notional" not in batch
    tail_batch = row_padded.panel_slab_batch_by_rows(
        4,
        8,
        torch.device("cpu"),
        non_blocking=False,
    )
    assert tail_batch is not None
    assert "volume_notional" not in tail_batch
    assert tail_batch["sample_mask"].tolist() == [True, False, False, False]
    assert symbol_padded.symbol_indices is not None
    assert symbol_padded.symbol_indices.tolist() == [0, 2, 0, 0]
    assert not symbol_padded.tradable_mask[:, 2:].any()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA cache dtype conversion contract")
def test_gpu_cache_target_dtypes_convert_feature_only_and_preserve_none() -> None:
    source = (
        torch.randn(4, 3, dtype=torch.float32),
        torch.randn(4, 3, dtype=torch.float32),
        torch.arange(4, dtype=torch.long),
        None,
    )
    cached = _maybe_cache_tensors_on_device(
        name="test dtype-aware cache",
        tensors=source,
        device=torch.device("cuda"),
        enabled=True,
        target_fraction=1.0,
        safety_margin_gb=0.0,
        target_dtypes=(torch.bfloat16, None, torch.bfloat16, torch.bfloat16),
    )

    assert cached[0] is not None and cached[0].device.type == "cuda"
    assert cached[0].dtype == torch.bfloat16
    assert cached[1] is not None and cached[1].device.type == "cuda"
    assert cached[1].dtype == torch.float32
    assert cached[2] is not None and cached[2].device.type == "cuda"
    assert cached[2].dtype == torch.long
    assert cached[3] is None
    cached_again = _maybe_cache_tensors_on_device(
        name="test dtype-aware cache reuse",
        tensors=cached,
        device=torch.device("cuda"),
        enabled=True,
        target_fraction=1.0,
        safety_margin_gb=0.0,
        target_dtypes=(torch.bfloat16, None, torch.bfloat16, torch.bfloat16),
    )
    assert cached_again is cached


def test_train_feature_amp_cache_default_is_backward_compatible_noop() -> None:
    config = load_config("configs/experiment_baseline.yaml")
    assert config.training.cache_train_features_in_amp_dtype is False
    assert _resolve_train_feature_cache_dtype(
        config,
        torch.device("cpu"),
        None,
        train_uses_panel_slab=False,
        has_categorical_features=True,
    ) is None


@pytest.mark.parametrize("amp_dtype", [torch.float16, torch.bfloat16])
def test_train_feature_amp_cache_resolves_only_supported_dtype(amp_dtype: torch.dtype) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.cache_train_features_in_amp_dtype = True
    config.training.model_name = "transformer_base_portfolio"
    config.training.transformer_base_portfolio.sanitize_inputs = False
    config.training.transformer_base_portfolio.categorical_feature_names = []

    assert _resolve_train_feature_cache_dtype(
        config,
        torch.device("cuda"),
        amp_dtype,
        train_uses_panel_slab=True,
        has_categorical_features=False,
    ) == amp_dtype


@pytest.mark.parametrize(
    "unsupported",
    [
        "cpu",
        "amp_none",
        "amp_float32",
        "model",
        "sanitize",
        "categorical",
        "no_panel_slab",
    ],
)
def test_train_feature_amp_cache_rejects_unsupported_capability(unsupported: str) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.cache_train_features_in_amp_dtype = True
    config.training.model_name = "transformer_base_portfolio"
    config.training.transformer_base_portfolio.sanitize_inputs = False
    config.training.transformer_base_portfolio.categorical_feature_names = []
    device = torch.device("cuda")
    amp_dtype: torch.dtype | None = torch.bfloat16
    train_uses_panel_slab = True
    has_categorical_features = False
    if unsupported == "cpu":
        device = torch.device("cpu")
    elif unsupported == "amp_none":
        amp_dtype = None
    elif unsupported == "amp_float32":
        amp_dtype = torch.float32
    elif unsupported == "model":
        config.training.model_name = "mlp"
    elif unsupported == "sanitize":
        config.training.transformer_base_portfolio.sanitize_inputs = True
    elif unsupported == "categorical":
        has_categorical_features = True
    elif unsupported == "no_panel_slab":
        train_uses_panel_slab = False

    with pytest.raises(ValueError, match="cache_train_features_in_amp_dtype requires"):
        _resolve_train_feature_cache_dtype(
            config,
            device,
            amp_dtype,
            train_uses_panel_slab=train_uses_panel_slab,
            has_categorical_features=has_categorical_features,
        )


def test_no_volume_limit_matches_explicit_infinite_limit_for_backtest_loss_and_grad() -> None:
    torch.manual_seed(109)
    rows, symbols = 5, 4
    raw = torch.randn(rows, symbols) * 0.2
    returns = torch.randn(rows, symbols) * 0.01
    tradable = torch.ones(rows, symbols, dtype=torch.bool)
    tradable[2, 3] = False
    can_buy = tradable.clone()
    can_sell = tradable.clone()
    can_short_open = tradable.clone()
    can_short_open[1, 0] = False
    force_short_cover = torch.zeros_like(tradable)
    force_short_cover[3, 1] = True
    force_exit = torch.zeros_like(tradable)
    force_exit[4, 2] = True
    benchmark = returns.mean(dim=1)
    initial_weights = torch.tensor([0.08, -0.04, 0.03, 0.0])
    initial_alive = torch.tensor(True)
    backtest_kwargs = {
        "buy_fee_rate": 0.001,
        "sell_fee_rate": 0.002,
        "long_only": False,
        "max_turnover_ratio": 0.5,
        "gross_leverage": 1.0,
        "min_trade_weight": 0.0,
        "portfolio_activation": "identity",
        "can_buy_mask": can_buy,
        "can_sell_mask": can_sell,
        "can_short_open_mask": can_short_open,
        "force_short_cover_mask": force_short_cover,
        "force_exit_mask": force_exit,
        "initial_weights": initial_weights,
        "initial_alive": initial_alive,
    }
    no_limit = run_backtest_torch(
        raw,
        returns,
        tradable,
        benchmark,
        volume_limit_weights=None,
        **backtest_kwargs,
    )
    infinite_limit = run_backtest_torch(
        raw,
        returns,
        tradable,
        benchmark,
        volume_limit_weights=torch.full_like(raw, float("inf")),
        **backtest_kwargs,
    )

    for field in (
        "strategy_returns",
        "benchmark_returns",
        "turnovers",
        "weights_history",
        "final_weights",
        "final_alive",
    ):
        actual = getattr(no_limit, field)
        expected = getattr(infinite_limit, field)
        assert actual is not None and expected is not None
        assert torch.allclose(actual, expected, atol=0.0, rtol=0.0), field

    no_limit_weights = raw.detach().clone().requires_grad_(True)
    infinite_weights = raw.detach().clone().requires_grad_(True)
    no_limit_state = {"initial_weights": initial_weights, "initial_alive": initial_alive}
    infinite_state = {"initial_weights": initial_weights, "initial_alive": initial_alive}
    loss_kwargs = {
        "benchmark_returns": benchmark,
        "can_buy_mask": can_buy,
        "can_sell_mask": can_sell,
        "can_short_open_mask": can_short_open,
        "force_short_cover_mask": force_short_cover,
        "force_exit_mask": force_exit,
        "long_only": False,
        "buy_fee_rate": 0.001,
        "sell_fee_rate": 0.002,
        "max_turnover_ratio": 0.5,
        "gross_leverage": 1.0,
        "min_trade_weight": 0.0,
        "portfolio_activation": "identity",
        "gamma_sharpe": 1.0,
        "gamma_turnover": 0.05,
        "objective": "log_utility",
        "concentration_weight": 0.0,
        "net_exposure_weight": 0.0,
    }
    no_limit_loss = risk_aware_loss(
        no_limit_weights,
        returns,
        tradable,
        volume_limit_weights=None,
        aux_outputs=no_limit_state,
        **loss_kwargs,
    )
    infinite_loss = risk_aware_loss(
        infinite_weights,
        returns,
        tradable,
        volume_limit_weights=torch.full_like(raw, float("inf")),
        aux_outputs=infinite_state,
        **loss_kwargs,
    )
    no_limit_loss.backward()
    infinite_loss.backward()

    assert torch.allclose(no_limit_loss, infinite_loss, atol=0.0, rtol=0.0)
    assert no_limit_weights.grad is not None and infinite_weights.grad is not None
    assert torch.allclose(no_limit_weights.grad, infinite_weights.grad, atol=0.0, rtol=0.0)
    assert torch.equal(no_limit_state["_final_alive"], infinite_state["_final_alive"])
    assert torch.allclose(
        no_limit_state["_final_weights"],
        infinite_state["_final_weights"],
        atol=0.0,
        rtol=0.0,
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


def test_dataset_keeps_all_untradable_terminal_exit_and_charges_sell_fee() -> None:
    panel = _make_panel(rows=6, symbols=1, features=2)
    terminal_idx = panel.num_dates - 1
    panel.tradable_mask[terminal_idx, :] = False
    panel.alive_mask[terminal_idx, :] = False
    panel.returns_1d[terminal_idx, :] = np.nan
    panel.can_buy_mask[terminal_idx, :] = False
    panel.can_sell_mask[terminal_idx, :] = False
    panel.force_exit_mask = np.zeros_like(panel.tradable_mask, dtype=bool)
    panel.force_exit_mask[terminal_idx, :] = True
    dataset = CrossSectionalDataset(
        panel,
        np.array([terminal_idx - 1, terminal_idx], dtype=np.int64),
        lookback=1,
    )

    assert dataset.valid_indices.tolist() == [terminal_idx - 1, terminal_idx]
    split = dataset_to_windowed_tensors(dataset)
    batch = split.batch_by_rows(0, 2, torch.device("cpu"), non_blocking=False)
    result = run_backtest_torch(
        torch.ones((2, 1), dtype=torch.float32),
        batch["future_log_returns"],
        batch["tradable_mask"],
        batch["benchmark"],
        buy_fee_rate=0.0,
        sell_fee_rate=0.01,
        long_only=True,
        can_buy_mask=batch["can_buy_mask"],
        can_sell_mask=batch["can_sell_mask"],
        force_exit_mask=batch["force_exit_mask"],
    )

    assert result.turnovers.tolist() == pytest.approx([1.0, 1.0])
    assert result.strategy_returns[-1].item() == pytest.approx(math.log1p(-0.01))
    assert torch.equal(result.final_weights, torch.zeros((1,), dtype=torch.float32))


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
    tail_slab = split.panel_slab_batch_by_rows(8, 12, torch.device("cpu"), non_blocking=False)

    assert first_batch["date_indices"].tolist() == [1, 2, 3, 4]
    assert first_batch["date_start"].tolist() == [1]
    assert bool(first_batch["rows_are_contiguous"].item()) is True
    assert first_batch["sample_mask"].tolist() == [True, True, True, True]
    assert tail_batch["date_indices"].tolist() == [9, 9, 9, 9]
    assert tail_batch["date_start"].tolist() == [9]
    assert bool(tail_batch["rows_are_contiguous"].item()) is False
    assert tail_batch["sample_mask"].tolist() == [True, False, False, False]
    assert tail_batch["tradable_mask"].all(dim=1).tolist() == [True, True, True, True]
    assert tail_slab is not None
    assert tail_slab["feature_slab"].shape == (5, 4, 3)
    assert torch.equal(tail_slab["feature_slab"][:2], split.features[8:10])
    assert tail_slab["sample_mask"].tolist() == [True, False, False, False]

    padded_only_slab = split.panel_slab_batch_by_rows(9, 12, torch.device("cpu"), non_blocking=False)
    assert padded_only_slab is not None
    assert padded_only_slab["feature_slab"].shape == (4, 4, 3)
    assert padded_only_slab["sample_mask"].tolist() == [False, False, False]


def test_batchnorm_training_tail_stays_ragged_and_ddp_requires_exact_divisibility() -> None:
    panel = _make_panel(rows=9, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=3)
    split = dataset_to_windowed_tensors(dataset)
    assert len(split) % 4 != 0
    model = _BatchNormProbeModel(num_symbols=panel.num_symbols)

    single_device = _prepare_training_split_batch_shape(
        split,
        model,
        batch_size=4,
        ddp_enabled=False,
    )
    assert len(single_device) == len(split)
    assert single_device.sample_mask is None

    with pytest.raises(ValueError, match="batch-coupled.*exactly divides.*not be dropped"):
        _prepare_training_split_batch_shape(
            split,
            model,
            batch_size=4,
            ddp_enabled=True,
        )

    sample_independent = _prepare_training_split_batch_shape(
        split,
        nn.Linear(3, 1),
        batch_size=4,
        ddp_enabled=False,
    )
    assert len(sample_independent) == 8
    assert torch.equal(
        sample_independent.sample_mask,
        torch.tensor([True] * len(split) + [False] * (8 - len(split))),
    )

    valid = torch.tensor(
        [
            [0.0, 1.0, 2.0, 3.0],
            [1.0, 2.0, 4.0, 8.0],
            [3.0, 5.0, 7.0, 9.0],
        ]
    )
    ragged_norm = nn.BatchNorm1d(4, affine=False)
    padded_norm = nn.BatchNorm1d(4, affine=False)
    padded_norm.load_state_dict(ragged_norm.state_dict())
    ragged_output = ragged_norm(valid)
    padded_output = padded_norm(torch.cat([valid, valid[-1:]], dim=0))[: len(valid)]
    assert not torch.allclose(ragged_output, padded_output)


def test_windowed_training_uses_panel_slab_for_padded_tail_without_generic_graph() -> None:
    lookback = 2
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=lookback)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    model = _SlabOnlyTrainModel(lookback)
    slab_model = _PanelSlabForwardWrapper(model)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    scaler = torch.amp.GradScaler("cuda", enabled=False)

    loss, timing = _train_epoch_windowed_tensor(
        model,
        slab_model,
        risk_aware_loss,
        split,
        optimizer,
        scaler,
        4,
        torch.device("cpu"),
        None,
        False,
        False,
        0.001,
        0.002,
        0.0,
        1.0,
        1.0,
        0.0,
        0.0,
        0.95,
        0.0,
        0.2,
        0.0,
        0.0,
        0.0,
        0.03,
        0.2,
        0.3,
        0.0,
        0.0,
        0.0,
            "log_utility",
            1.0,
            direction_weight=0.0,
            volatility_regime_weight=0.0,
        )

    assert torch.isfinite(loss)
    assert timing.batches == 3
    assert model.generic_panel_calls == 0


def test_ddp_executor_routes_fixed_local_shards_through_panel_slab(monkeypatch) -> None:
    lookback = 2
    panel = _make_panel(rows=10, symbols=4, features=3)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=lookback)
    split = _pad_windowed_training_split(dataset_to_windowed_tensors(dataset), batch_size=4)
    base_model = _SlabOnlyTrainModel(lookback)
    slab_model = _PanelSlabForwardWrapper(base_model)
    optimizer = torch.optim.Adam(base_model.parameters(), lr=1e-3)
    scaler = torch.amp.GradScaler("cuda", enabled=False)

    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 1)
    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer_module, "_distributed_is_rank0", lambda: False)
    monkeypatch.setattr(
        trainer_module,
        "_all_gather_autograd",
        lambda value: torch.cat((value, value), dim=0),
    )
    monkeypatch.setattr(
        trainer_module,
        "_all_gather_no_grad",
        lambda value: torch.cat((value, value), dim=0),
    )

    loss, timing = _train_epoch_windowed_tensor_ddp(
        slab_model,
        risk_aware_loss,
        split,
        optimizer,
        scaler,
        batch_size=4,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        objective="log_utility",
        grad_clip_norm=1.0,
        long_only=False,
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.95,
        gamma_drawdown=0.0,
        drawdown_target=0.2,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.03,
        drawdown_budget=0.2,
        turnover_budget=0.3,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        use_panel_slab=True,
    )

    assert torch.isfinite(loss)
    assert timing.batches == 3
    assert base_model.generic_panel_calls == 0


def test_panel_slab_training_densifies_filtered_dates_without_bridging_split_gaps() -> None:
    panel = _make_panel(rows=12, symbols=4, features=3)
    date_indices = np.array([0, 1, 2, 3, 4, 8, 9, 10, 11], dtype=np.int64)
    # Date 3 belongs to the split but is filtered by CrossSectionalDataset.
    panel.tradable_mask[3] = False
    panel.can_buy_mask[3] = False
    panel.can_sell_mask[3] = False
    dataset = CrossSectionalDataset(panel, date_indices, lookback=2)
    split = dataset_to_windowed_tensors(dataset)

    dense = trainer_module._densify_windowed_training_split_for_panel_slab(split, date_indices)

    assert dense.valid_indices.tolist() == [1, 2, 3, 4, 8, 9, 10, 11]
    assert dense.sample_mask.tolist() == [True, True, False, True, True, True, True, True]
    # Dates outside the split must not become executable backtest state.
    assert 5 not in dense.valid_indices.tolist()
    assert dense.panel_slab_batch_by_rows(0, 4, torch.device("cpu"), False) is not None


def test_ddp_rejects_auxiliary_objective_instead_of_silently_dropping_aux(tmp_path) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.multi_gpu_strategy = "distributed_data_parallel"
    config.training.loss_type = "portfolio_autoencoder"

    with pytest.raises(ValueError, match="requires aux tensors"):
        run_training(_make_panel(), [], config, tmp_path, resume=False)


def test_removed_data_parallel_strategy_fails_with_migration_message(tmp_path) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.multi_gpu_strategy = "data_parallel"

    with pytest.raises(ValueError, match="was removed.*distributed_data_parallel"):
        run_training(_make_panel(), [], config, tmp_path, resume=False)


def test_tree_training_empty_fold_smoke_has_no_neural_factor_state(tmp_path) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.model_name = "lightgbm"
    assert run_training(_make_panel(), [], config, tmp_path, resume=False) == []


def test_neural_inference_empty_fold_smoke_initializes_fold_successor_map(tmp_path) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.environment.device = "cpu"
    assert run_inference(_make_panel(), [], config, tmp_path) == []


def test_run_training_restores_strict_backtest_runtime_after_empty_fold(tmp_path, monkeypatch) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.environment.device = "cpu"
    config.environment.use_tensor_cores = False
    config.training.strict_no_fallback = True
    monkeypatch.delenv("STOCKAGENT_STRICT_NO_FALLBACK", raising=False)

    assert run_training(_make_panel(), [], config, tmp_path, resume=False) == []
    assert "STOCKAGENT_STRICT_NO_FALLBACK" not in os.environ

    weights = torch.tensor([[0.6, 0.4]], dtype=torch.float32)
    future_returns = torch.tensor([[0.01, -0.02]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    result = run_backtest_torch(
        weights,
        future_returns,
        tradable,
        torch.zeros(1, dtype=torch.float32),
        buy_fee_rate=0.001,
        sell_fee_rate=0.002,
        long_only=True,
    )
    assert torch.isfinite(result.strategy_returns).all()


def test_ddp_batch_size_contract_only_rounds_automatic_candidates() -> None:
    assert _normalize_ddp_global_batch_size(32, 2, auto_selected=False) == 32
    assert _normalize_ddp_global_batch_size(31, 2, auto_selected=True) == 30
    with pytest.raises(ValueError, match="must be divisible"):
        _normalize_ddp_global_batch_size(31, 2, auto_selected=False)
    with pytest.raises(ValueError, match="at least world_size"):
        _normalize_ddp_global_batch_size(1, 2, auto_selected=True)


def test_distributed_compile_and_auto_batch_consensus_observe_remote_minimum(monkeypatch) -> None:
    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)

    def remote_rank_is_lower(value: torch.Tensor, op) -> None:
        assert op == torch.distributed.ReduceOp.MIN
        if value.dtype == torch.int32:
            value.fill_(0)
        else:
            value.fill_(24)

    monkeypatch.setattr(trainer_module.dist, "all_reduce", remote_rank_is_lower)

    # A locally successful compile constructor must still observe a remote
    # failure and take the same fallback/strict branch on every rank.
    assert not _distributed_probe_succeeded(True, torch.device("cpu"))
    assert _distributed_min_int(32, torch.device("cpu")) == 24


def test_rank_ordered_compile_probe_runs_once_on_each_rank_turn(monkeypatch) -> None:
    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 3)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 1)
    events: list[str] = []
    monkeypatch.setattr(
        trainer_module,
        "_distributed_barrier",
        lambda: events.append("barrier"),
    )
    monkeypatch.setattr(
        trainer_module,
        "_distributed_probe_succeeded",
        lambda local_success, device: bool(local_success),
    )

    ok, error = trainer_module._run_distributed_compile_probe(
        lambda: (events.append("probe") or True, None),
        device=torch.device("cpu"),
        rank_ordered=True,
    )

    assert ok and error is None
    assert events == ["barrier", "probe", "barrier", "barrier"]


def test_rank0_final_artifact_failure_is_raised_on_waiting_worker(monkeypatch) -> None:
    monkeypatch.setattr(trainer_module, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 1)

    def gather_rank0_failure(statuses, local_status) -> None:
        statuses[0] = {
            "rank": 0,
            "ok": False,
            "error": "OSError: artifact disk full",
        }
        statuses[1] = local_status

    monkeypatch.setattr(trainer_module.dist, "all_gather_object", gather_rank0_failure)

    with pytest.raises(RuntimeError, match="final_fold_artifacts.*rank0.*disk full"):
        _raise_if_distributed_phase_failed("final_fold_artifacts", None)


def test_final_group_checkpoint_collective_precedes_rank0_best_model_artifacts() -> None:
    source = inspect.getsource(trainer_module.run_training)
    rank0_artifact_split = source.split(
        "if ddp_enabled and not _distributed_should_write():",
        maxsplit=1,
    )
    assert len(rank0_artifact_split) == 2
    before_rank0_artifacts, after_rank0_artifacts = rank0_artifact_split
    assert "_save_group_checkpoint(" in before_rank0_artifacts
    # Saving after per-fold best checkpoint reload would mix best-model weights
    # with the shared final optimizer/scheduler state and mismatch DDP collectives.
    assert "_save_group_checkpoint(" not in after_rank0_artifacts
    assert after_rank0_artifacts.index("_load_state_dict(model, checkpoint") > 0
    assert '_raise_if_distributed_phase_failed("final_fold_artifacts"' in source
    assert '_raise_if_distributed_phase_failed("final_postprocess"' in source


def test_final_fold_validation_is_recomputed_after_each_best_checkpoint_load() -> None:
    source = inspect.getsource(trainer_module.run_training)
    artifact_source = source.split(
        "if ddp_enabled and not _distributed_should_write():",
        maxsplit=1,
    )[1]
    load_index = artifact_source.index('_load_state_dict(model, checkpoint["model_state_dict"])')
    val_index = artifact_source.index("best-checkpoint val windowed tensors")
    test_index = artifact_source.index("final-test windowed tensors")
    assert load_index < val_index < test_index
    assert "val_backtest.weights_history[start:end]" not in artifact_source
    assert "checkpoint_path=artifact_checkpoint_path" in artifact_source


def test_eval_padding_rows_copy_last_valid_mask_for_no_fallback_attention() -> None:
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


def test_full_eval_splits_share_base_when_train_symbols_are_compacted() -> None:
    panel = _make_panel(rows=14, symbols=5, features=3)
    train_raw = dataset_to_windowed_tensors(
        CrossSectionalDataset(panel, torch.arange(0, 8).numpy(), lookback=3)
    )
    train = _prepare_windowed_split(
        train_raw.subset_symbols(torch.tensor([0, 2, 4])),
        torch.device("cpu"),
        non_blocking=False,
        name="compacted train",
    )
    validation = _prepare_windowed_split(
        dataset_to_windowed_tensors(
            CrossSectionalDataset(panel, torch.arange(4, 11).numpy(), lookback=3)
        ),
        torch.device("cpu"),
        non_blocking=False,
        shared_base=train,
        name="validation",
    )
    test = _prepare_windowed_split(
        dataset_to_windowed_tensors(
            CrossSectionalDataset(panel, torch.arange(7, 14).numpy(), lookback=3)
        ),
        torch.device("cpu"),
        non_blocking=False,
        shared_base=validation,
        name="test",
    )

    assert validation.num_symbols == panel.num_symbols
    assert validation.features.data_ptr() != train.features.data_ptr()
    assert test.features.data_ptr() == validation.features.data_ptr()
    assert test.future_log_returns.data_ptr() == validation.future_log_returns.data_ptr()
    assert test.tradable_mask.data_ptr() == validation.tradable_mask.data_ptr()
    assert test.valid_indices.data_ptr() != validation.valid_indices.data_ptr()


def test_short_open_fallback_does_not_alias_sell_mask_storage() -> None:
    panel = _make_panel(rows=6, symbols=2, features=1)
    panel.can_short_open_mask = None
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=2)
    assert dataset.can_short_open_mask_t.data_ptr() != dataset.can_sell_mask_t.data_ptr()


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


def test_windowed_eval_propagates_short_open_force_cover_and_force_exit_masks(monkeypatch) -> None:
    panel = _make_panel(rows=6, symbols=2, features=1)
    dataset = CrossSectionalDataset(panel, np.arange(panel.num_dates), lookback=2)
    split = dataset_to_windowed_tensors(dataset)
    valid = split.valid_indices.to(dtype=torch.long)
    split.tradable_mask = torch.ones_like(split.tradable_mask)
    split.can_buy_mask = torch.ones_like(split.can_buy_mask)
    split.can_sell_mask = torch.ones_like(split.can_sell_mask)
    split.can_short_open_mask = torch.zeros_like(split.can_short_open_mask)
    split.can_short_open_mask[valid[0]] = True
    split.force_short_cover_mask = torch.zeros_like(split.force_short_cover_mask)
    split.force_short_cover_mask[valid[1:]] = True
    split.force_exit_mask = torch.zeros_like(split.force_exit_mask)
    split.force_exit_mask[valid[-1]] = True

    class _AlwaysShort(nn.Module):
        def forward(self, x: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
            return -torch.ones_like(mask, dtype=x.dtype)

    captured_force_masks: list[torch.Tensor] = []
    captured_exit_masks: list[torch.Tensor] = []
    original_run_backtest = simulator.run_backtest_torch

    def _capture_backtest(*args, **kwargs):
        captured_force_masks.append(kwargs["force_short_cover_mask"].detach().clone())
        captured_exit_masks.append(kwargs["force_exit_mask"].detach().clone())
        result = original_run_backtest(*args, **kwargs)
        return result

    monkeypatch.setattr("stockagent.training.trainer.run_backtest_torch", _capture_backtest)

    backtest, _, _ = _evaluate_windowed_tensor_batch(
        _AlwaysShort(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        False,
        0.0,
        0.0,
        0.0,
        1.0,
        chunk_rows=4,
        backtest_chunk_rows=4,
    )

    assert torch.all(backtest.weights_history[0] < 0.0)
    assert any(mask.any() for mask in captured_force_masks)
    assert any(mask.any() for mask in captured_exit_masks)
    assert torch.equal(captured_force_masks[0][1], torch.ones((2,), dtype=torch.bool))
    assert torch.equal(captured_exit_masks[-1][0], torch.ones((2,), dtype=torch.bool))
    assert torch.equal(backtest.weights_history[-1], torch.zeros_like(backtest.weights_history[-1]))


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


@pytest.mark.parametrize(
    ("targets", "can_buy", "can_sell", "expected_last", "expected_day_one_simple"),
    [
        # A sell-blocked long consumes 0.8 gross; only the remaining 0.2 may
        # be allocated to the newly requested long.
        (
            [[0.8, 0.0], [0.0, 1.0]],
            [[True, True], [True, True]],
            [[True, True], [False, True]],
            [0.8 / (1.0 - 0.01 * 0.8), 1.0 - 0.8 / (1.0 - 0.01 * 0.8)],
            -0.01 * (1.0 - 0.8 / (1.0 - 0.01 * 0.8)),
        ),
        # A buy-blocked short likewise cannot be silently scaled toward zero.
        (
            [[-0.8, 0.0], [0.0, -1.0]],
            [[True, True], [False, True]],
            [[True, True], [True, True]],
            [
                -0.8 / (1.0 - 0.03 * 0.8),
                -(1.0 - 0.8 / (1.0 - 0.03 * 0.8)),
            ],
            -0.03 * (1.0 - 0.8 / (1.0 - 0.03 * 0.8)),
        ),
        # Crossing the first asset through zero is allowed and releases its
        # old long gross before opening the new short; the frozen second long
        # retains its exact 0.4 weight.
        (
            [[0.6, 0.4], [-1.0, 0.0]],
            [[True, True], [True, True]],
            [[True, True], [True, False]],
            [-(1.0 - 0.4 / (1.0 - 0.01)), 0.4 / (1.0 - 0.01)],
            -0.03
            * (
                0.6 / (1.0 - 0.01)
                + (1.0 - 0.4 / (1.0 - 0.01))
            ),
        ),
    ],
)
def test_long_short_gross_cap_never_manufactures_trades_in_side_blocked_positions(
    targets,
    can_buy,
    can_sell,
    expected_last,
    expected_day_one_simple,
) -> None:
    weights_np = np.asarray(targets, dtype=np.float32)
    returns_np = np.zeros_like(weights_np)
    tradable_np = np.ones_like(weights_np, dtype=bool)
    buy_np = np.asarray(can_buy, dtype=bool)
    sell_np = np.asarray(can_sell, dtype=bool)
    short_np = np.ones_like(tradable_np, dtype=bool)
    benchmark_np = np.zeros((weights_np.shape[0],), dtype=np.float32)

    numpy_result = simulator.run_backtest(
        weights_np,
        returns_np,
        tradable_np,
        benchmark_np,
        buy_fee_rate=0.01,
        sell_fee_rate=0.03,
        long_only=False,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        can_buy_mask=buy_np,
        can_sell_mask=sell_np,
        can_short_open_mask=short_np,
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights_np),
        torch.from_numpy(returns_np),
        torch.from_numpy(tradable_np),
        torch.from_numpy(benchmark_np),
        buy_fee_rate=0.01,
        sell_fee_rate=0.03,
        long_only=False,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(buy_np),
        can_sell_mask=torch.from_numpy(sell_np),
        can_short_open_mask=torch.from_numpy(short_np),
    )

    expected = np.asarray(expected_last, dtype=np.float32)
    np.testing.assert_allclose(numpy_result.weights_history[-1], expected, atol=1e-7, rtol=1e-6)
    np.testing.assert_allclose(
        torch_result.weights_history[-1].detach().cpu().numpy(),
        expected,
        atol=1e-7,
        rtol=1e-6,
    )
    expected_log = math.log1p(expected_day_one_simple)
    assert math.isclose(float(numpy_result.strategy_returns[1]), expected_log, rel_tol=1e-6, abs_tol=1e-7)
    assert math.isclose(float(torch_result.strategy_returns[1]), expected_log, rel_tol=1e-6, abs_tol=1e-7)
    np.testing.assert_allclose(
        torch_result.weights_history.detach().cpu().numpy(),
        numpy_result.weights_history,
        atol=1e-7,
        rtol=1e-6,
    )
    np.testing.assert_allclose(
        torch_result.strategy_returns.detach().cpu().numpy(),
        numpy_result.strategy_returns,
        atol=1e-7,
        rtol=1e-6,
    )


def test_long_only_buy_capacity_respects_subunit_gross_budget_with_frozen_position() -> None:
    weights_np = np.asarray([[0.3, 0.0], [0.0, 0.5]], dtype=np.float32)
    returns_np = np.zeros_like(weights_np)
    tradable_np = np.ones_like(weights_np, dtype=bool)
    buy_np = tradable_np.copy()
    sell_np = tradable_np.copy()
    sell_np[1, 0] = False
    benchmark_np = np.zeros((2,), dtype=np.float32)

    numpy_result = simulator.run_backtest(
        weights_np,
        returns_np,
        tradable_np,
        benchmark_np,
        buy_fee_rate=0.01,
        sell_fee_rate=0.03,
        long_only=True,
        gross_leverage=0.5,
        portfolio_activation="pre_normalized",
        can_buy_mask=buy_np,
        can_sell_mask=sell_np,
    )
    torch_result = run_backtest_torch(
        torch.from_numpy(weights_np),
        torch.from_numpy(returns_np),
        torch.from_numpy(tradable_np),
        torch.from_numpy(benchmark_np),
        buy_fee_rate=0.01,
        sell_fee_rate=0.03,
        long_only=True,
        gross_leverage=0.5,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(buy_np),
        can_sell_mask=torch.from_numpy(sell_np),
    )

    frozen_weight = 0.3 / (1.0 - 0.01 * 0.3)
    expected = np.asarray([frozen_weight, 0.5 - frozen_weight], dtype=np.float32)
    np.testing.assert_allclose(numpy_result.weights_history[-1], expected, atol=1e-7, rtol=1e-6)
    np.testing.assert_allclose(
        torch_result.weights_history[-1].detach().cpu().numpy(),
        expected,
        atol=1e-7,
        rtol=1e-6,
    )
    assert math.isclose(
        float(numpy_result.strategy_returns[1]),
        math.log1p(-0.01 * (0.5 - frozen_weight)),
        rel_tol=1e-6,
        abs_tol=1e-7,
    )
    np.testing.assert_allclose(
        torch_result.strategy_returns.detach().cpu().numpy(),
        numpy_result.strategy_returns,
        atol=1e-7,
        rtol=1e-6,
    )


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


def test_log_utility_concentration_penalty_uses_canonical_backtest_weights() -> None:
    weights = torch.tensor(
        [
            [100.0, -100.0, 0.5],
            [50.0, -25.0, -25.0],
        ],
        dtype=torch.float32,
        requires_grad=True,
    )
    returns = torch.zeros_like(weights)
    mask = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((2,), dtype=torch.float32)
    sample_mask = torch.tensor([True, False], dtype=torch.bool)
    concentration_weight = 0.7

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        sample_mask=sample_mask,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
        concentration_weight=concentration_weight,
        objective="log_utility",
    )

    bt = run_backtest_torch(
        weights,
        returns,
        mask,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        can_buy_mask=mask,
        can_sell_mask=mask,
        return_weights_history=True,
    )
    tradable_f = mask.to(dtype=bt.weights_history.dtype)
    active_count = tradable_f.sum(dim=1).clamp_min(1.0)
    concentration = (bt.weights_history.pow(2) * tradable_f).sum(dim=1) * active_count
    expected = concentration_weight * concentration[sample_mask].mean()

    raw_concentration = (weights.detach().pow(2) * tradable_f).sum(dim=1) * active_count
    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)
    assert loss.detach() < concentration_weight * raw_concentration[sample_mask].mean() * 0.01
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_log_utility_net_exposure_penalty_uses_realised_backtest_weights() -> None:
    base_weights = torch.tensor([[1.0, -1.0]], dtype=torch.float32)
    returns = torch.zeros_like(base_weights)
    tradable = torch.ones_like(base_weights, dtype=torch.bool)
    can_buy = tradable.clone()
    can_sell = torch.tensor([[True, False]], dtype=torch.bool)
    sample_mask = torch.ones((1,), dtype=torch.bool)
    initial_weights = torch.zeros((2,), dtype=torch.float32)
    net_exposure_weight = 2.0

    general_weights = base_weights.clone().requires_grad_(True)
    general_loss = risk_aware_loss(
        general_weights,
        returns,
        tradable,
        benchmark_returns=torch.zeros((1,), dtype=torch.float32),
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        sample_mask=sample_mask,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
        net_exposure_weight=net_exposure_weight,
        objective="log_utility",
    )

    expected_penalty = torch.tensor(0.5, dtype=torch.float32)
    assert torch.allclose(general_loss, expected_penalty, atol=1e-7, rtol=1e-6)

    general_loss.backward()
    assert general_weights.grad is not None
    assert torch.isfinite(general_weights.grad).all()


def test_risk_aware_loss_applies_short_open_ban_and_forced_cover() -> None:
    weights = torch.tensor([[-1.0], [-1.0]], dtype=torch.float32, requires_grad=True)
    returns = torch.tensor([[0.0], [0.0]], dtype=torch.float32)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    aux_outputs = {"initial_weights": torch.zeros((1,), dtype=torch.float32)}

    loss = risk_aware_loss(
        weights,
        returns,
        tradable,
        benchmark_returns=torch.zeros((2,), dtype=torch.float32),
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=torch.tensor([[False], [True]]),
        force_short_cover_mask=torch.tensor([[False], [True]]),
        long_only=False,
        objective="log_utility",
        aux_outputs=aux_outputs,
    )

    assert torch.isfinite(loss)
    assert torch.equal(aux_outputs["_final_weights"], torch.zeros((1,), dtype=torch.float32))


def test_sharpe_aware_loss_uses_short_open_and_forced_cover_rules() -> None:
    weights = torch.tensor([[-1.0], [-1.0]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    open_short = torch.tensor([[True], [True]])
    no_force_cover = torch.zeros_like(open_short)
    force_cover = torch.tensor([[False], [True]])

    blocked_loss = sharpe_aware_loss(
        weights,
        returns,
        tradable,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=torch.zeros_like(open_short),
        long_only=False,
        gamma_sharpe=0.0,
        gamma_turnover=1.0,
    )
    held_loss = sharpe_aware_loss(
        weights,
        returns,
        tradable,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=open_short,
        force_short_cover_mask=no_force_cover,
        long_only=False,
        gamma_sharpe=0.0,
        gamma_turnover=1.0,
    )
    covered_loss = sharpe_aware_loss(
        weights,
        returns,
        tradable,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        can_short_open_mask=open_short,
        force_short_cover_mask=force_cover,
        long_only=False,
        gamma_sharpe=0.0,
        gamma_turnover=1.0,
    )

    assert blocked_loss.item() == pytest.approx(0.0)
    assert held_loss.item() > blocked_loss.item()
    assert covered_loss.item() > held_loss.item()


def test_eval_log_utility_can_transform_geometric_utility_returns() -> None:
    strategy_returns = torch.tensor([0.02, -0.01, 0.03, 0.01], dtype=torch.float32)
    benchmark_returns = torch.zeros_like(strategy_returns)
    turnovers = torch.tensor([0.1, 0.2, 0.3], dtype=torch.float32)

    loss = _loss_from_backtest_series(
        strategy_returns,
        benchmark_returns,
        turnovers,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        log_utility_pre_log_power=0.0,
        log_utility_periods_per_year=2.0,
        log_utility_log_shift=0.001,
    )

    expected = -(strategy_returns.sum() / 2.0 - 0.001)
    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)


def test_eval_log_utility_manual_power_means_manual_years() -> None:
    strategy_returns = torch.tensor([0.02, 0.01, -0.005], dtype=torch.float32)
    benchmark_returns = torch.zeros_like(strategy_returns)
    turnovers = torch.zeros_like(strategy_returns)

    loss = _loss_from_backtest_series(
        strategy_returns,
        benchmark_returns,
        turnovers,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        log_utility_pre_log_power=3.0,
        log_utility_periods_per_year=252.0,
        log_utility_log_shift=0.0,
    )

    expected = -(strategy_returns.sum() / 3.0)
    assert torch.allclose(loss, expected, atol=1e-7, rtol=1e-6)


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


def test_segmented_log_utility_eval_loss_applies_geometric_utility_transform() -> None:
    strategy_returns = torch.tensor([0.02, -0.01, 0.03, 0.01, -0.02, 0.04], dtype=torch.float32)
    benchmark_returns = torch.zeros_like(strategy_returns)
    turnovers = torch.zeros_like(strategy_returns)
    offsets = [0, 4, 6]

    eval_losses = _batched_loss_from_backtest_segments(
        strategy_returns,
        benchmark_returns,
        turnovers,
        offsets,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        cvar_alpha=0.05,
        gamma_drawdown=0.0,
        drawdown_target=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        excess_target=0.0,
        cvar_budget=0.0,
        drawdown_budget=0.0,
        turnover_budget=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        log_utility_pre_log_power=0.0,
        log_utility_periods_per_year=2.0,
        log_utility_log_shift=0.0005,
    )

    expected = torch.stack(
        [
            -(strategy_returns[0:4].sum() / 2.0 - 0.0005),
            -(strategy_returns[4:6].sum() / 1.0 - 0.0005),
        ]
    )
    assert torch.allclose(eval_losses, expected, atol=1e-7, rtol=1e-6)


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


def test_torch_backtest_applies_volume_participation_weight_cap() -> None:
    weights = torch.tensor(
        [
            [1.0, 0.0],
            [0.0, 1.0],
        ],
        dtype=torch.float32,
    )
    returns = torch.zeros_like(weights)
    mask = torch.ones_like(weights, dtype=torch.bool)
    volume_limit = torch.tensor(
        [
            [0.25, float("inf")],
            [0.10, 0.30],
        ],
        dtype=torch.float32,
    )

    bt = run_backtest_torch(
        weights,
        returns,
        mask,
        torch.zeros(2, dtype=torch.float32),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        can_buy_mask=mask,
        can_sell_mask=mask,
        volume_limit_weights=volume_limit,
    )

    expected = torch.tensor(
        [
            [0.25, 0.0],
            [0.15, 0.30],
        ],
        dtype=torch.float32,
    )
    assert torch.allclose(bt.weights_history.cpu(), expected, atol=1e-7, rtol=1e-6)
    assert torch.allclose(bt.turnovers.cpu(), torch.tensor([0.25, 0.40]), atol=1e-7, rtol=1e-6)


def test_integer_volume_then_turnover_cap_matches_canonical_tensor_order() -> None:
    weights_np = np.asarray([[0.8, 0.2]], dtype=np.float32)
    returns_np = np.zeros_like(weights_np)
    mask_np = np.ones_like(weights_np, dtype=bool)

    tensor = run_backtest_torch(
        torch.from_numpy(weights_np),
        torch.from_numpy(returns_np),
        torch.from_numpy(mask_np),
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        max_turnover_ratio=0.5,
        portfolio_activation="pre_normalized",
        can_buy_mask=torch.from_numpy(mask_np),
        can_sell_mask=torch.from_numpy(mask_np),
        volume_limit_weights=torch.tensor([[0.2, 2.0]], dtype=torch.float32),
    )
    integer, _ = simulator.run_backtest_integer_shares(
        weights_np,
        returns_np,
        mask_np,
        np.zeros((1,), dtype=np.float32),
        can_buy_mask=mask_np,
        can_sell_mask=mask_np,
        initial_capital=100.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        max_turnover_ratio=0.5,
        max_volume_participation=0.2,
        portfolio_activation="pre_normalized",
        close_prices=np.asarray([[1.0, 1.0]], dtype=np.float32),
        daily_volumes=np.asarray([[100.0, 1000.0]], dtype=np.float32),
        collect_holdings=False,
    )

    expected = np.asarray([[0.2, 0.2]], dtype=np.float32)
    np.testing.assert_allclose(tensor.weights_history.detach().cpu().numpy(), expected)
    np.testing.assert_allclose(integer.weights_history, expected)
    np.testing.assert_allclose(tensor.turnovers.detach().cpu().numpy(), [0.4])
    np.testing.assert_allclose(integer.turnovers, [0.4])


def test_integer_normalizes_before_nontradable_gate_like_canonical_tensor() -> None:
    weights_np = np.asarray([[0.5, 0.5]], dtype=np.float32)
    returns_np = np.zeros_like(weights_np)
    tradable_np = np.asarray([[True, False]], dtype=bool)

    tensor = run_backtest_torch(
        torch.from_numpy(weights_np),
        torch.from_numpy(returns_np),
        torch.from_numpy(tradable_np),
        torch.zeros((1,), dtype=torch.float32),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="identity",
        can_buy_mask=torch.from_numpy(tradable_np),
        can_sell_mask=torch.from_numpy(tradable_np),
    )
    integer, _ = simulator.run_backtest_integer_shares(
        weights_np,
        returns_np,
        tradable_np,
        np.zeros((1,), dtype=np.float32),
        can_buy_mask=tradable_np,
        can_sell_mask=tradable_np,
        initial_capital=100.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="identity",
        close_prices=np.asarray([[1.0, 1.0]], dtype=np.float32),
        collect_holdings=False,
    )

    expected = np.asarray([[0.5, 0.0]], dtype=np.float32)
    np.testing.assert_allclose(tensor.weights_history.detach().cpu().numpy(), expected)
    np.testing.assert_allclose(integer.weights_history, expected)
    np.testing.assert_allclose(tensor.turnovers.detach().cpu().numpy(), [0.5])
    np.testing.assert_allclose(integer.turnovers, [0.5])


def test_integer_short_holdings_report_real_equity_ratios_without_second_activation() -> None:
    weights = np.asarray([[-1.0]], dtype=np.float32)
    returns = np.zeros_like(weights)
    mask = np.ones_like(weights, dtype=bool)

    result, holdings = simulator.run_backtest_integer_shares(
        weights,
        returns,
        mask,
        np.zeros((1,), dtype=np.float32),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="softsign",
        close_prices=np.asarray([[100.0]], dtype=np.float32),
        symbols=["A"],
        dates=np.asarray(["2024-01-02"], dtype="datetime64[D]"),
    )

    np.testing.assert_allclose(result.weights_history, [[-1.0]])
    ratios = {row.symbol: row.holding_ratio for row in holdings}
    assert ratios["A"] == pytest.approx(-1.0)
    assert ratios["CASH"] == pytest.approx(2.0)
    assert sum(ratios.values()) == pytest.approx(1.0)


def test_integer_forced_cover_has_priority_over_cash_affordability() -> None:
    weights = np.asarray(
        [[-0.5, 0.5], [0.0, 0.5]],
        dtype=np.float32,
    )
    returns = np.zeros_like(weights)
    tradable = np.ones_like(weights, dtype=bool)
    can_sell = tradable.copy()
    can_sell[1, 1] = False  # the long collateral cannot be sold on cover day
    force_cover = np.asarray(
        [[False, False], [True, False]],
        dtype=bool,
    )

    result, holdings = simulator.run_backtest_integer_shares(
        weights,
        returns,
        tradable,
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=tradable,
        can_sell_mask=can_sell,
        can_short_open_mask=tradable,
        force_short_cover_mask=force_cover,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        close_prices=np.asarray(
            [[100.0, 100.0], [250.0, 300.0]],
            dtype=np.float32,
        ),
        symbols=["SHORT", "HALTED_LONG"],
        dates=np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    final_rows = {row.symbol: row for row in holdings if row.date == "2024-01-03"}
    assert "SHORT" not in final_rows
    assert final_rows["HALTED_LONG"].shares == 5
    assert final_rows["CASH"].market_value == pytest.approx(-250.0)
    np.testing.assert_allclose(result.turnovers, [1.0, 1.0])
    np.testing.assert_allclose(result.weights_history[-1], [0.0, 1.2])


def test_integer_force_cover_allows_discretionary_long_flip() -> None:
    weights = np.asarray([[-0.4], [0.2]], dtype=np.float32)
    returns = np.zeros_like(weights)
    mask = np.ones_like(weights, dtype=bool)
    force_cover = np.asarray([[False], [True]], dtype=bool)

    result, _ = simulator.run_backtest_integer_shares(
        weights,
        returns,
        mask,
        np.zeros((2,), dtype=np.float32),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        force_short_cover_mask=force_cover,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        close_prices=np.full((2, 1), 100.0, dtype=np.float32),
        collect_holdings=False,
    )

    np.testing.assert_allclose(result.weights_history[:, 0], [-0.4, 0.2])
    np.testing.assert_allclose(result.turnovers, [0.4, 0.6])
