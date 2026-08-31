from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_futures_portfolio import (
    TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE,
    run_tw_futures_portfolio_integer_surrogate_torch,
    run_tw_futures_portfolio_integer_torch,
)
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
)
from stockagent.data.tw_stock_context_futures_portfolio import (
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
    TaiwanStockContextFuturesPortfolioDaily,
    attach_stock_context_futures_portfolio_daily,
    fixed_futures_slot_symbols,
)
from stockagent.models.cross_sectional_all_futures import (
    CrossSectionalAllFuturesModel,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.checkpoint_contract import build_checkpoint_manifest
from stockagent.training.windowed import dataset_to_windowed_tensors
from stockagent.training.trainer import (
    _split_recurrent_symbol_count,
    _split_uses_recurrent_futures_equity_scale,
)


def _stock_panel(rows: int = 3, symbols: int = 2) -> PanelData:
    dates = np.asarray(
        [np.datetime64("2026-01-02") + np.timedelta64(idx, "D") for idx in range(rows)],
        dtype="datetime64[D]",
    )
    shape = (rows, symbols)
    mask = np.ones(shape, dtype=bool)
    prices = np.full(shape, 100.0, dtype=np.float32)
    return PanelData(
        dates=dates,
        symbols=[f"S{idx}" for idx in range(symbols)],
        feature_names=["f0", "f1", "f2"],
        features=np.zeros((*shape, 3), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=prices.copy(),
        daily_volumes=np.ones(shape, dtype=np.float32),
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        force_short_cover_mask=np.zeros(shape, dtype=bool),
        force_exit_mask=np.zeros(shape, dtype=bool),
        open_prices=prices.copy(),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _integer_execution(
    simple_returns: torch.Tensor,
    *,
    group_indices: torch.Tensor | None = None,
    executable: bool = True,
) -> torch.Tensor:
    rows, slots = simple_returns.shape
    execution = torch.zeros((rows, slots, 11), dtype=torch.float32)
    execution[..., 0] = torch.log1p(simple_returns)
    execution[..., 1] = float(executable)
    execution[..., 3] = 100_000.0
    execution[..., 4] = 100_000.0 * (1.0 + simple_returns)
    execution[..., 5:8] = 0.0
    execution[..., 8] = 100.0 if executable else 0.0
    execution[..., 9] = (
        torch.arange(slots, dtype=torch.float32)[None, :]
        if group_indices is None
        else group_indices.to(dtype=torch.float32)[None, :]
    )
    execution[..., 10] = 0.0
    return execution


def test_attach_uses_only_prior_futures_features_and_separate_action_axis(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    session_dates = [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)]
    for idx, session_date in enumerate(session_dates):
        row: dict[str, object] = {
            "date": session_date,
            "product": "TX",
            "symbol": "TAIFEX_SLOT_0001",
            "tenor_rank": 1,
            "open": 100.0 + idx,
            "close": 100.5 + idx,
            "volume": 10 if idx != 1 else 0,
            "holding_log_return": 0.01 * (idx + 1),
            "executable": idx != 1,
            "must_liquidate": idx == 2,
            "can_hold_overnight": idx < 2,
            "same_contract_as_previous_session": idx > 0,
            "contract_multiplier": 200.0,
            "sinopac_network_fee_group": "large",
            "underlying_symbol": "S0",
        }
        for feature_idx, name in enumerate(FUTURES_MODEL_FEATURE_COLUMNS):
            row[name] = (
                7 if name == "taifex_product_id" else 100.0 * idx + feature_idx
            )
        rows.append(row)
    data_path = tmp_path / "continuous_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows), data_path)
    manifest = {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "feature_contract_version": TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        "fixed_model_output_slots": TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        "outputs": {"continuous_daily": {"sha256": _sha256(data_path)}},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
    )
    daily = panel.stock_context_futures_portfolio_daily
    assert daily is not None
    assert panel.num_symbols == 2
    assert len(daily.symbols) == TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT
    assert not daily.candidate_mask[0].any()
    assert daily.candidate_mask[1, 0]
    assert daily.candidate_mask[2, 0]
    # Date 1 receives date 0's completed feature, never date 1's outcome.
    settlement_feature = FUTURES_MODEL_FEATURE_COLUMNS.index(
        "taifex_settlement_logret_1d"
    )
    assert daily.candidate_features[1, 0, settlement_feature] == pytest.approx(1.0)
    assert daily.candidate_features[2, 0, settlement_feature] == pytest.approx(101.0)
    assert daily.candidate_features.shape[-1] == len(
        TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
    )
    # The structural routing key is also lagged with the futures token. It is
    # consumed only as an integer gather index, never as a continuous feature.
    assert daily.candidate_features[1, 0, -1] == pytest.approx(0.0)
    # Zero current volume/executability is executor-only and cannot erase the
    # already known same-physical-contract policy token.
    assert not daily.executable_mask[1, 0]
    assert daily.candidate_mask[1, 0]
    assert daily.must_liquidate_mask[2, 0]


def test_dataset_keeps_stock_attention_axis_and_packs_futures_execution() -> None:
    panel = _stock_panel(rows=4, symbols=3)
    futures_shape = (4, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)
    candidate_mask = np.zeros(futures_shape, dtype=bool)
    candidate_mask[1:, 0] = True
    executable = np.zeros(futures_shape, dtype=bool)
    executable[1:, 0] = True
    returns = np.full(futures_shape, np.nan, dtype=np.float32)
    returns[1:, 0] = 0.0
    fee = np.full(futures_shape, np.nan, dtype=np.float32)
    fee[1:, 0] = 0.001
    panel.stock_context_futures_portfolio_daily = (
        TaiwanStockContextFuturesPortfolioDaily(
            dates=panel.dates,
            symbols=fixed_futures_slot_symbols(),
            candidate_features=np.zeros(
                (
                    *futures_shape,
                    len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
                ),
                dtype=np.float32,
            ),
            candidate_mask=candidate_mask,
            holding_log_returns=returns,
            executable_mask=executable,
            must_liquidate_mask=np.zeros(futures_shape, dtype=bool),
            can_hold_overnight_mask=executable.copy(),
            fee_rate_per_open_notional=fee,
            open_prices=np.ones(futures_shape, dtype=np.float32),
            close_prices=np.ones(futures_shape, dtype=np.float32),
            volumes=np.ones(futures_shape, dtype=np.float32),
            benchmark_log_returns=np.zeros(4, dtype=np.float32),
            source_path="synthetic",
            manifest_path="synthetic",
        )
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(4, dtype=np.int64),
        lookback=1,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    assert tuple(dataset.tradable_mask_t.shape) == (4, 3)
    assert tuple(dataset.derivative_candidate_mask_t.shape) == futures_shape
    assert tuple(dataset.overnight_log_returns_t.shape) == (*futures_shape, 4)
    final_row = int(dataset.valid_indices[-1])
    assert bool(
        (dataset.overnight_log_returns_t[final_row, :, 2] > 0.5).all().item()
    )
    windowed = dataset_to_windowed_tensors(dataset)
    assert windowed.execution_mode == "tw_stock_context_futures_portfolio"
    assert tuple(windowed.derivative_candidate_features.shape) == (
        4,
        TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
    )
    assert _split_recurrent_symbol_count(windowed) == 1936
    assert not _split_uses_recurrent_futures_equity_scale(windowed)
    assert _split_uses_recurrent_futures_equity_scale(
        SimpleNamespace(
            execution_mode="tw_stock_context_futures_portfolio",
            overnight_log_returns=torch.zeros((2, 3, 11)),
        )
    )


def test_model_masks_before_projection_and_can_retain_cash() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=2,
        num_features=3,
        num_symbols=4,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        center_long_short_logits=False,
        projection_l1_scale_by_active_count=True,
        return_aux=False,
        execution_mode="tw_stock_context_futures_portfolio",
    ).eval()
    with torch.no_grad():
        model.futures_action_head.weight.zero_()
        model.futures_action_head.bias.fill_(0.5)
    candidate_features = torch.zeros((1, 1936, 18), dtype=torch.float32)
    candidate_mask = torch.zeros((1, 1936), dtype=torch.bool)
    candidate_mask[:, [2, 9]] = True
    with torch.no_grad():
        weights = model(
            torch.zeros((1, 2, 4, 3), dtype=torch.float32),
            torch.ones((1, 4), dtype=torch.bool),
            portfolio_context={
                "candidate_features": candidate_features,
                "candidate_mask": candidate_mask,
            },
        )
    assert tuple(weights.shape) == (1, 1936)
    assert torch.count_nonzero(weights.masked_select(~candidate_mask)) == 0
    assert weights.abs().sum().item() == pytest.approx(0.5, abs=1e-6)
    with pytest.raises(ValueError, match="requires causal futures context"):
        model(
            torch.zeros((1, 2, 4, 3), dtype=torch.float32),
            torch.ones((1, 4), dtype=torch.bool),
        )


def test_model_routes_each_futures_token_to_its_underlying_stock() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=2,
        num_features=3,
        num_symbols=4,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
        center_long_short_logits=False,
        return_aux=True,
        return_aux_details=True,
        execution_mode="tw_stock_context_futures_portfolio",
    ).eval()
    features = torch.zeros(
        (
            1,
            TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
            len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
        ),
        dtype=torch.float32,
    )
    features[..., -1] = -1.0
    features[0, 2, -1] = 0.0
    features[0, 9, -1] = 1.0
    candidate_mask = torch.zeros((1, 1936), dtype=torch.bool)
    candidate_mask[:, [2, 9]] = True
    stock_embeddings = torch.randn((1, 4, 8), dtype=torch.float32)

    with torch.no_grad():
        output = model._portfolio_outputs_from_stock_embeddings(
            stock_embeddings,
            torch.ones((1, 4), dtype=torch.bool),
            {},
            portfolio_context={
                "candidate_features": features,
                "candidate_mask": candidate_mask,
            },
        )

    link_mask = output["aux"]["futures_underlying_link_mask"]
    link_gate = output["aux"]["futures_underlying_gate"]
    assert link_mask[0, 2]
    assert link_mask[0, 9]
    assert int(link_mask.sum().item()) == 2
    assert 0.0 < link_gate[0, 2, 0].item() < 1.0
    assert 0.0 < link_gate[0, 9, 0].item() < 1.0


def test_executor_carries_until_mandatory_contract_close_without_redistribution() -> None:
    actions = torch.zeros((3, 1936), dtype=torch.float32, requires_grad=True)
    with torch.no_grad():
        actions[:, 0] = torch.tensor([0.4, -0.8, 0.2])
    execution = torch.zeros((3, 1936, 4), dtype=torch.float32)
    execution[:, 0, 0] = 0.0
    execution[[0, 2], 0, 1] = 1.0
    execution[2, 0, 2] = 1.0
    execution[:, 0, 3] = 0.0
    # Stock masks intentionally have a different width. The dedicated futures
    # executor must consume only its packed 1,936-slot sidecar.
    stock_mask = torch.ones((3, 2), dtype=torch.bool)
    result = run_backtest_torch(
        actions,
        torch.zeros((3, 2), dtype=torch.float32),
        stock_mask,
        torch.zeros(3, dtype=torch.float32),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=stock_mask,
        can_sell_mask=stock_mask,
        force_exit_mask=torch.zeros_like(stock_mask),
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    assert result.weights_history[:, 0].tolist() == pytest.approx([0.4, 0.4, 0.2])
    assert result.final_weights is not None
    assert result.final_weights[0].item() == pytest.approx(0.0)
    # Row 1's unavailable request stays in its own slot; it is neither closed
    # nor redistributed. Row 2 includes rebalance plus mandatory close.
    assert result.turnovers.tolist() == pytest.approx([0.4, 0.0, 0.4])
    (-result.strategy_returns.mean()).backward()
    assert actions.grad is not None


def test_integer_carry_jointly_packs_standard_and_mini_and_closes_at_expiry() -> None:
    actions = torch.zeros((1, 1936), dtype=torch.float32, requires_grad=True)
    with torch.no_grad():
        actions[0, 0] = 0.55
    execution = torch.full((1, 1936, 11), float("nan"), dtype=torch.float32)
    execution[..., 1:3] = 0.0
    execution[..., 9] = torch.arange(1936, dtype=torch.float32)[None, :]
    execution[..., 10] = 0.0
    # One group contains a TWD 400k standard and a TWD 20k mini.  Both earn
    # 10%; the exact TWD 550k sleeve fits 1 + 7 contracts after current and
    # reserved close fees, and the expiry row then charges the actual close.
    execution[0, :2, 0] = torch.log(torch.tensor(1.1))
    execution[0, :2, 1] = 1.0
    execution[0, :2, 2] = 1.0
    execution[0, :2, 3] = torch.tensor([400_000.0, 20_000.0])
    execution[0, :2, 4] = torch.tensor([440_000.0, 22_000.0])
    execution[0, :2, 5] = 40.0
    execution[0, :2, 6:8] = 0.0
    execution[0, :2, 8] = 100.0
    execution[0, :2, 9] = 0.0
    execution[0, :2, 10] = torch.tensor([0.0, 1.0])
    result = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    assert result.contract_quantities_history is not None
    assert result.contract_quantities_history.dtype == torch.int64
    assert result.contract_quantities_history[0, :2].tolist() == [1, 7]
    assert result.final_weights[:2].tolist() == pytest.approx([0.0, 0.0])
    # 540k gross notional earns 54k; eight opens and eight closes each cost 40.
    assert torch.expm1(result.strategy_returns[0]).item() == pytest.approx(
        0.05336, abs=1e-6
    )
    (-result.strategy_returns.mean()).backward()
    assert actions.grad is not None

    wrapped = run_backtest_torch(
        actions.detach(),
        torch.zeros((1, 2), dtype=torch.float32),
        torch.ones((1, 2), dtype=torch.bool),
        torch.zeros(1, dtype=torch.float32),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
    )
    assert wrapped.settlement_ledger_unit == "contract_quantity"
    assert wrapped.weights_history[0, :2].tolist() == pytest.approx([1.0, 7.0])
    assert torch.equal(
        wrapped.weights_history,
        torch.round(wrapped.weights_history),
    )


def test_integer_surrogate_has_gradient_inside_hard_quantization_plateau() -> None:
    actions = torch.tensor(
        [[0.20, 0.10], [0.25, 0.05]],
        dtype=torch.float32,
        requires_grad=True,
    )
    execution = _integer_execution(
        torch.tensor([[0.010, -0.005], [0.020, 0.004]], dtype=torch.float32)
    )

    def objective(value: torch.Tensor) -> torch.Tensor:
        result = run_tw_futures_portfolio_integer_surrogate_torch(
            value,
            execution,
            initial_capital=1_000_000.0,
            return_weights_history=False,
        )
        return -result.strategy_returns.mean()

    loss = objective(actions)
    loss.backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    epsilon = 1.0e-4
    plus = actions.detach().clone()
    minus = actions.detach().clone()
    plus[1, 0] += epsilon
    minus[1, 0] -= epsilon
    finite_difference = ((objective(plus) - objective(minus)) / (2.0 * epsilon)).item()
    # The forward path is a hard whole-contract floor, so a small perturbation
    # inside one plateau leaves its value unchanged. The explicit
    # straight-through backward remains non-zero and supplies the optimizer
    # with a direction toward the next executable contract.
    assert finite_difference == pytest.approx(0.0, abs=1.0e-7)
    assert abs(actions.grad[1, 0].item()) > 1.0e-6


def test_integer_surrogate_groups_standard_and_mini_and_masks_capacity_gradient() -> None:
    same_group = torch.tensor([0, 0], dtype=torch.int64)
    actions = torch.tensor([[0.20, 0.30]], requires_grad=True)
    execution = _integer_execution(
        torch.tensor([[0.01, 0.01]], dtype=torch.float32),
        group_indices=same_group,
    )
    grouped = run_tw_futures_portfolio_integer_surrogate_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    (-grouped.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert actions.grad[0, 0].item() == pytest.approx(
        actions.grad[0, 1].item(), abs=1.0e-8
    )
    assert actions.grad.abs().sum().item() > 0.0

    blocked_actions = actions.detach().clone().requires_grad_(True)
    blocked_execution = _integer_execution(
        torch.tensor([[0.01, 0.01]], dtype=torch.float32),
        group_indices=same_group,
        executable=False,
    )
    blocked = run_tw_futures_portfolio_integer_surrogate_torch(
        blocked_actions,
        blocked_execution,
        initial_capital=1_000_000.0,
    )
    (-blocked.strategy_returns.sum()).backward()
    assert blocked_actions.grad is not None
    assert torch.count_nonzero(blocked_actions.grad) == 0


def test_integer_training_surrogate_carries_continuous_state_without_account_death() -> None:
    actions = torch.tensor(
        [[0.05, 0.0], [0.20, 0.0]],
        dtype=torch.float32,
        requires_grad=True,
    )
    execution = _integer_execution(
        torch.tensor([[0.01, 0.0], [0.02, 0.0]], dtype=torch.float32)
    )
    first = run_backtest_torch(
        actions[:1],
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution[:1],
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=True,
    )
    assert first.final_alive is not None and bool(first.final_alive)
    assert first.final_weights is not None
    assert first.final_equity_scale is not None
    assert first.settlement_ledger_unit == "notional_weight_training_surrogate"
    # One contract is 10% of capital. A 5% request has zero executable forward
    # exposure but retains a straight-through gradient.
    assert first.weights_history[0, 0].item() == pytest.approx(0.0, abs=1.0e-7)

    second = run_backtest_torch(
        actions[1:],
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution[1:],
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=True,
        initial_weights=first.final_weights,
        initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
    )
    assert second.final_alive is not None and bool(second.final_alive)
    (-first.strategy_returns.sum() - second.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    assert torch.count_nonzero(actions.grad) == 2


def test_integer_executor_chunking_carries_quantities_and_equity_together() -> None:
    actions = torch.tensor([[0.30, 0.10], [0.35, 0.05]], dtype=torch.float32)
    execution = _integer_execution(
        torch.tensor([[0.10, -0.02], [0.03, 0.01]], dtype=torch.float32)
    )
    full = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    first = run_tw_futures_portfolio_integer_torch(
        actions[:1],
        execution[:1],
        initial_capital=1_000_000.0,
    )
    second = run_tw_futures_portfolio_integer_torch(
        actions[1:],
        execution[1:],
        initial_capital=1_000_000.0,
        initial_quantities=first.final_weights,
        initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
    )
    assert second.strategy_returns[0].item() == pytest.approx(
        full.strategy_returns[1].item(), abs=1.0e-7
    )
    assert torch.equal(second.final_weights, full.final_weights)
    assert second.final_equity_scale is not None
    assert full.final_equity_scale is not None
    assert second.final_equity_scale.item() == pytest.approx(
        full.final_equity_scale.item(), abs=1.0e-7
    )


def test_formal_config_preserves_full_contract_and_1000_epochs() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml"
    )
    assert config.trading.execution_mode == "tw_stock_context_futures_portfolio"
    assert config.training.model_name == "cross_sectional_all_futures"
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == 0.0
    assert config.training.batch_size_train == 96
    assert config.training.eval_backtest_chunk_rows == 64
    assert config.training.eval_backtest_chunk_rows_auto is False
    assert config.training.eval_backtest_compile is True
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_multi_basis_projection_l1_"
        "v3_full1000_batch96_chunk64"
    )
    assert len(config.data.feature_include) >= 90
    assert config.data.feature_exclude == ["next_session_open_gap_logret"]
    assert "twpub_pe_log" in config.data.feature_include
    assert "twpub_margin_balance_log" in config.data.feature_include
    assert "twpub_foreign_net_buy_flow" in config.data.feature_include
    assert "twpub_material_event_count_log" in config.data.feature_include
    assert "twpub_cbc_m2_yoy" in config.data.feature_include
    assert "twpub_taifex_tx_open_interest_log" in config.data.feature_include
    assert not config.training.train_symbol_compaction
    assert not config.training.distributed_symbol_sharded_ledger
    assert config.training.transformer_base_portfolio.portfolio_output_mode == (
        "projection_l1"
    )
    assert config.trading.portfolio_activation == "pre_normalized"
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    assert manifest["contracts"]["model"]["model_name"] == (
        "cross_sectional_all_futures"
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["fixed_model_output_slots"] == 1936
    assert futures_contract["holding"] == (
        "same_physical_contract_cross_session_until_own_expiry"
    )


def test_integer_0900_carry_config_is_fresh_full_feature_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0900_integer_full_features_"
        "multi_basis_projection_l1_cash_capital10m.yaml"
    )
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.trading.tw_futures_portfolio_integer_initial_capital == pytest.approx(
        10_000_000.0
    )
    assert config.trading.tw_futures_portfolio_integer_fee_per_contract_per_side_twd == pytest.approx(
        40.0
    )
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert config.data.day_trade_open_feature is True
    assert config.data.feature_exclude == []
    assert len(config.data.feature_include) >= 90
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.compile_loss is False
    assert config.training.futures_portfolio_training_surrogate_only is True
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_0900_integer_full_features_"
        "multi_basis_projection_l1_cash_capital10m_v5_early_stop_010"
    )
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["integer_training_surrogate"] == (
        TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE
    )
    assert futures_contract["candidate_feature_columns"] == list(
        TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
    )
    assert manifest["contracts"]["model"]["contract_version"] == 2
