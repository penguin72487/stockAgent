from pathlib import Path
import json

import pytest
import torch
import numpy as np

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import load_config
from stockagent.data.tw_stock_context_futures_portfolio import (
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
)
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from stockagent.training.trainer import (
    _split_recurrent_symbol_count,
    _split_uses_recurrent_futures_equity_scale,
)
from test_tw_stock_futures_minute_gradients import profitable_tape
from test_tw_stock_context_futures_portfolio import _stock_panel


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("missing_exit", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_intraday_adapter_preserves_exact_kernel_and_gradients(direction, missing_exit, strict, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1" if strict else "0")
    tape = profitable_tape()
    if missing_exit:
        tape[..., 3 + 2 * 5:] = 0
    results, grads = [], []
    for mode in ("tw_stock_futures_day_trade_0845_minute", "tw_stock_context_futures_portfolio"):
        weights = torch.tensor([[direction * .25]], requires_grad=True)
        # All-futures actions have their own axis and permissions. The input
        # stock-context masks can have a different width and no tradable stock.
        side_mask = (torch.zeros(1, 3, dtype=torch.bool)
                     if mode == "tw_stock_context_futures_portfolio"
                     else torch.ones_like(weights, dtype=torch.bool))
        result = run_backtest_torch(
            weights, torch.zeros_like(weights), side_mask,
            torch.zeros(1), 0., 0., execution_mode=mode,
            can_buy_mask=side_mask, can_sell_mask=side_mask,
            overnight_returns=tape, day_trade_execution_initial_capital=1e6,
            long_only=False, portfolio_activation="pre_normalized",
        )
        result.strategy_returns.sum().backward()
        results.append(result)
        grads.append(weights.grad)
    for key in ("strategy_returns", "turnovers", "final_alive", "final_weights", "final_equity_scale", "futures_contract_quantities_history", "futures_residual_contract_quantities_history"):
        torch.testing.assert_close(getattr(results[0], key), getattr(results[1], key), rtol=0, atol=0)
    torch.testing.assert_close(grads[0], grads[1], rtol=0, atol=0)


def test_financial_encoder_outputs_only_masked_futures():
    torch.set_num_threads(2)
    model = FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode="market_token", num_market_tokens=2,
        temporal_heads=2, temporal_layers=1, temporal_pooling="last",
        temporal_query_mode="last_only", use_symbol_pos=False,
        portfolio_mode="long_short", portfolio_output_mode="score_entmax_log_cash",
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        feature_bottleneck_dim=2, causal_feature_rms_normalization=True,
        dropout=0., return_aux=False,
    ).eval()
    features = torch.zeros(1, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS))
    mask = torch.zeros(1, 1936, dtype=torch.bool)
    mask[:, :2] = True
    x = torch.randn(1, 4, 2, 3)
    weights = model(x, torch.ones(1, 2, dtype=torch.bool), portfolio_context={
        "candidate_features": features, "candidate_mask": mask,
    })
    assert weights.shape == (1, 1936)
    assert not weights[:, 2:].any()
    assert torch.isfinite(weights).all()
    assert (weights.abs().sum(-1) <= 1.000001).all()
    weights[:, 0].sum().backward()
    assert model.futures_action_head.weight.grad is not None
    assert hasattr(model, "candle_encoder")


def test_flat_futures_initialization_learns_through_exact_minute_objective():
    from stockagent.training.loss import risk_aware_loss
    from stockagent.training.trainer import _reset_pretrained_futures_action_head_to_flat_

    torch.manual_seed(41)
    torch.set_num_threads(2)
    model = FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode="market_token", num_market_tokens=2,
        temporal_heads=2, temporal_layers=1, temporal_pooling="last",
        temporal_query_mode="last_only", use_symbol_pos=False,
        portfolio_mode="long_short", portfolio_output_mode="score_entmax_log_cash",
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        feature_bottleneck_dim=2, causal_feature_rms_normalization=True,
        dropout=0., return_aux=False,
    ).eval()
    _reset_pretrained_futures_action_head_to_flat_(model)
    # No weight decay or auxiliary losses: the update must come from the
    # actual minute execution objective's recoverable backward path.
    optimizer = torch.optim.AdamW(model.parameters(), lr=.01, weight_decay=0.)
    features = torch.zeros(1, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS))
    candidate_mask = torch.zeros(1, 1936, dtype=torch.bool)
    candidate_mask[:, 0] = True
    context = {"candidate_features": features, "candidate_mask": candidate_mask}
    x = torch.randn(1, 4, 2, 3)
    stock_mask = torch.ones(1, 2, dtype=torch.bool)
    weights = model(x, stock_mask, portfolio_context=context)
    assert not weights.any()
    tape = torch.zeros(1, 1936, 2, 63)
    tape[:, :1] = profitable_tape()
    loss = risk_aware_loss(
        weights, torch.zeros(1, 2), stock_mask,
        benchmark_returns=torch.zeros(1), overnight_log_returns=tape,
        execution_mode="tw_stock_context_futures_portfolio", objective="log_utility",
        buy_fee_rate=0., sell_fee_rate=0., long_only=False,
        portfolio_activation="pre_normalized", day_trade_execution_initial_capital=1e6,
        futures_portfolio_recoverable_backward=True,
        futures_minute_recovery_objective="execution_utility",
        rank_ic_weight=0., return_rank_ic_weight=0., direction_weight=0.,
        volatility_regime_weight=0., concentration_weight=0.,
    )
    # Exact forward has no invented fractional fills; the differentiable
    # shadow still teaches the model which profitable whole-unit action to try.
    assert loss.item() == 0.
    loss.backward()
    head_gradient = model.futures_action_head.weight.grad
    assert head_gradient is not None and torch.isfinite(head_gradient).all()
    assert torch.count_nonzero(head_gradient) > 0
    optimizer.step()
    assert optimizer.state[model.futures_action_head.weight]["exp_avg"].abs().sum() > 0
    updated = model(x, stock_mask, portfolio_context=context)
    assert torch.isfinite(updated).all()
    assert updated[0, 0] > 0
    assert not updated[:, 1:].any()


@pytest.mark.parametrize("policy", ["carry", "intraday"])
def test_frozen_v5_futures_configuration(policy):
    from dataclasses import asdict
    from stockagent.config import _load_raw_config
    name = "general" if policy == "carry" else "intraday"
    config = load_config(Path(__file__).resolve().parents[1] / f"configs/markets/tw_futures_v8_{name}.yaml")
    assert config.training.financial_transformer.portfolio_output_mode == "score_entmax_log_cash"
    assert len(config.training.financial_transformer.temporal_basis_families) == 22
    assert config.trading.tw_futures_portfolio_holding_policy == policy
    assert config.training.futures_portfolio_optimizer_step_per_trajectory
    assert not config.training.day_trade_training_annual_episodes
    assert "next_session_open_gap_logret" in config.data.feature_shift_next_session
    # Inspect the historical artifact as data: its stock annual/sub-lot policy
    # need not be executable in the current, continuous-account stock runtime.
    baseline = _load_raw_config(Path(__file__).resolve().parents[1] / "configs/deployments/tw_day_trade_v8_v5_frozen_training_snapshot.yaml")
    source_training = baseline["training"]
    source_model = source_training["financial_transformer"]
    target_model = asdict(config.training.financial_transformer)
    product_fields = {"futures_denomination_aware_output", "futures_denomination_hard_projection", "futures_current_open_feature"}
    for key, value in source_model.items():
        if key not in product_fields:
            assert target_model[key] == value, key
    assert config.training.batch_size_train == source_training["batch_size_train"]
    assert config.training.epochs == source_training["epochs"]
    assert config.training.loss_type == source_training["loss_type"]
    assert config.runner.output_dir == f"artifacts/markets/tw_futures_v8_{name}"


def test_intraday_batches_preserve_cash_equity():
    from types import SimpleNamespace
    split = SimpleNamespace(execution_mode="tw_stock_context_futures_portfolio", overnight_log_returns=torch.zeros(2, 3, 2, 63))
    assert _split_uses_recurrent_futures_equity_scale(split)
    assert _split_recurrent_symbol_count(split) == 3


@pytest.mark.parametrize("chunk_rows", [1, 2, 3])
@pytest.mark.parametrize("missing_exit", [False, True])
def test_intraday_chunked_eval_and_artifact_preserve_equity_and_contracts(tmp_path, monkeypatch, chunk_rows, missing_exit):
    from dataclasses import replace
    from stockagent.training import trainer as t

    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    tape = profitable_tape().expand(3, 1, 2, -1).clone()
    if missing_exit:
        tape[1, ..., 3 + 2 * 5:] = 0
    weights = torch.tensor([[.25], [-.25], [.25]])
    stock_yes = torch.ones(3, 2, dtype=torch.bool)
    stock_no = torch.zeros_like(stock_yes)
    runtime = t._ExecutionRuntime(mode="tw_stock_context_futures_portfolio", buy_fee_rates=None,
        sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0, futures_initial_capital=1e6)
    direct = run_backtest_torch(
        weights, torch.zeros(3, 2), stock_yes, torch.zeros(3), 0., 0.,
        can_buy_mask=stock_yes, can_sell_mask=stock_yes,
        execution_mode=runtime.mode, overnight_returns=tape,
        day_trade_execution_initial_capital=1e6, long_only=False,
        portfolio_activation="pre_normalized", return_weights_history=True,
    )
    actual, _ = t._run_eval_backtest_from_weight_buffers(
        weights, torch.zeros(3, 2), stock_yes, stock_yes, stock_yes, stock_yes,
        stock_no, stock_no, torch.zeros(3), device=torch.device("cpu"),
        non_blocking=False, long_only=False, buy_fee_rate=0., sell_fee_rate=0.,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0., backtest_chunk_rows=chunk_rows,
        compute_metrics_summary=False, return_weights_history=True, profile_timing=False,
        progress_label=None, timing=t.TimingBreakdown(), reset_at_rows=None,
        portfolio_activation="pre_normalized", overnight_log_returns_all=tape, execution_runtime=runtime,
    )
    assert actual.settlement_ledger_unit == "contract_quantity"
    fields = ("strategy_returns", "turnovers", "weights_history", "equity_scale_history",
              "final_equity_scale", "settlement_default", "futures_contract_quantities_history",
              "futures_residual_contract_quantities_history")
    for name in fields:
        torch.testing.assert_close(getattr(actual, name), getattr(direct, name), rtol=0, atol=0)
    path = tmp_path / "intraday_backtest.npz"
    dates = np.arange("2026-01-05", "2026-01-08", dtype="datetime64[D]")
    t._save_backtest_artifact(path, actual.to_numpy(), dates)
    restored, restored_dates = t._load_backtest_artifact(path)
    assert restored.settlement_ledger_unit == "contract_quantity"
    np.testing.assert_array_equal(restored_dates, dates)
    for name in fields:
        np.testing.assert_array_equal(getattr(restored, name), getattr(direct.to_numpy(), name))
    if missing_exit:
        assert restored.futures_residual_contract_quantities_history.any()
        with pytest.raises(ValueError, match="residual was relabelled"):
            t._save_backtest_artifact(tmp_path / "invalid_flat.npz",
                replace(restored, settlement_default=np.zeros(3, dtype=bool)), dates)


@pytest.mark.parametrize("violation", ["unshifted_open", "overnight_settlement"])
def test_intraday_rejects_incompatible_clock_and_holding_policy(tmp_path, violation):
    from stockagent.config import _load_raw_config
    path = Path(__file__).resolve().parents[1] / "configs/markets/tw_futures_v8_intraday.yaml"
    payload = _load_raw_config(path)
    if violation == "unshifted_open":
        payload["data"]["feature_shift_next_session"] = []
    else:
        payload["data"]["tw_futures_expiry_settlement_valuation"] = True
    bad = tmp_path / "bad.yaml"
    bad.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        load_config(bad)


def _intraday_panel():
    from stockagent.data.tw_stock_context_futures_portfolio import (
        TaiwanStockContextFuturesPortfolioDaily, fixed_futures_slot_symbols,
    )
    panel = _stock_panel(rows=4, symbols=3)
    shape = (4, 1936)
    zero = np.zeros(shape, dtype=np.float32)
    mask = np.zeros(shape, dtype=bool)
    mask[1:, 0] = True
    tape = np.zeros((*shape, 2, 63), dtype=np.float32)
    tape[1:3, :1] = profitable_tape().numpy()
    executable = mask.copy()
    executable[3] = False
    panel.stock_context_futures_portfolio_daily = TaiwanStockContextFuturesPortfolioDaily(
        dates=panel.dates, symbols=fixed_futures_slot_symbols(),
        candidate_features=np.zeros((*shape, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)), dtype=np.float32),
        candidate_mask=mask, holding_log_returns=zero,
        executable_mask=executable, must_liquidate_mask=np.zeros(shape, dtype=bool),
        can_hold_overnight_mask=np.zeros(shape, dtype=bool), fee_rate_per_open_notional=zero,
        open_prices=zero, close_prices=zero, volumes=zero,
        benchmark_log_returns=np.zeros(4, dtype=np.float32),
        source_path="synthetic", manifest_path="synthetic",
        intraday_execution=tape, intraday_session_mask=np.ones(4, dtype=bool),
    )
    return panel


def test_intraday_stock_context_dataset_loss_and_no_fill_calendar():
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.loss import risk_aware_loss
    from stockagent.training.windowed import dataset_to_windowed_tensors

    panel = _intraday_panel()
    dataset = CrossSectionalDataset(panel, np.arange(4), lookback=1,
                                   execution_mode="tw_stock_context_futures_portfolio")
    split = dataset_to_windowed_tensors(dataset)
    assert 3 in dataset.valid_indices  # zero entry volume does not drop a day
    assert _split_recurrent_symbol_count(split) == 1936
    rows = dataset.valid_indices
    weights = torch.zeros(len(rows), 1936)
    weights[:, 0] = .25
    weights.requires_grad_()
    loss = risk_aware_loss(
        weights, split.future_log_returns[rows], split.tradable_mask[rows],
        benchmark_returns=split.benchmark[rows],
        overnight_log_returns=split.overnight_log_returns[rows],
        execution_mode=split.execution_mode, objective="log_utility",
        buy_fee_rate=0., sell_fee_rate=0., long_only=False,
        portfolio_activation="pre_normalized", day_trade_execution_initial_capital=1e6,
        futures_portfolio_recoverable_backward=True,
        futures_minute_recovery_objective="execution_utility",
        rank_ic_weight=0., return_rank_ic_weight=0., direction_weight=0.,
        volatility_regime_weight=0., concentration_weight=0.,
    )
    assert torch.isfinite(loss)
    loss.backward()
    assert weights.grad is not None and torch.isfinite(weights.grad).all()
    assert weights.grad[:, 0].abs().sum() > 0


@pytest.mark.parametrize("panel_date_unit", ["D", "ns"])
def test_intraday_source_gate_and_future_price_causality(tmp_path, panel_date_unit):
    from dataclasses import replace
    import polars as pl
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_all_futures_intraday import attach_all_futures_intraday
    from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES, HYBRID_CONTRACT_VERSION
    from stockagent.data.tw_stock_futures_history import HISTORY_DATASET, HISTORY_SOURCE

    panel = _intraday_panel()
    dates = panel.dates.tolist()
    daily = tmp_path / "daily.parquet"
    pl.DataFrame({
        "date": dates, "physical_contract": ["TX:202601"] * 4,
        "symbol": ["TAIFEX_SLOT_0001"] * 4, "product": ["TX"] * 4,
        "tenor_rank": [1] * 4, "same_contract_as_previous_session": [False, True, True, True],
        "fixed_fee_research_supported": [True] * 4, "asset_class": ["index_future"] * 4,
        "contract_multiplier": [200.] * 4,
    }).write_parquet(daily)
    minutes = tmp_path / "minutes.parquet"
    bars = pl.DataFrame({
        "date": dates[1:], "physical_contract": ["TX:202601"] * 3,
        "minute": [EVENT_MINUTES[0]] * 3, "vwap": [100.] * 3, "high": [100.] * 3,
        "low": [100.] * 3, "close": [100.] * 3, "volume": [3.] * 3,
        "source_file_sha256": ["a" * 64] * 3,
    })
    bars.write_parquet(minutes)
    coverage = bars.select("date", "physical_contract", "source_file_sha256").with_columns(pl.lit("minute_verified").alias("status"))
    coverage.write_parquet(tmp_path / "coverage.parquet")
    manifest = {"dataset": HISTORY_DATASET, "contract_version": HYBRID_CONTRACT_VERSION,
                "source_kind": HISTORY_SOURCE, "daily_proxy_before": "2026-01-01",
                "status": "complete", "source_daily_sha256": sha256_file(daily), "outputs": {},
                "requested_dates": [str(d) for d in dates], "covered_dates": [str(d) for d in dates]}

    def attach():
        manifest["outputs"] = {key: {"file": f"{key}.parquet", "sha256": sha256_file(tmp_path / f"{key}.parquet")} for key in ("minutes", "coverage")}
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))
        fresh = _intraday_panel()
        fresh.dates = fresh.dates.astype(f"datetime64[{panel_date_unit}]")
        fresh.stock_context_futures_portfolio_daily = replace(
            fresh.stock_context_futures_portfolio_daily, dates=fresh.dates, source_path=str(daily),
            integer_execution=np.zeros((4, 1936, 11), dtype=np.float32),
        )
        return attach_all_futures_intraday(fresh, minutes, participation=.5).stock_context_futures_portfolio_daily

    before = attach()
    assert before.intraday_execution[1, 0, 0, 7] == 1  # floor(3 * .5), never ceil
    assert not before.benchmark_log_returns.any()
    bars = bars.with_columns(*(pl.when(pl.col("date") == dates[-1]).then(110.).otherwise(pl.col(k)).alias(k) for k in ("vwap", "high", "low", "close")))
    bars.write_parquet(minutes)
    after = attach()
    np.testing.assert_array_equal(before.candidate_features, after.candidate_features)
    np.testing.assert_array_equal(before.candidate_mask, after.candidate_mask)
    np.testing.assert_array_equal(before.intraday_execution[:-1], after.intraday_execution[:-1])
    coverage.head(2).write_parquet(tmp_path / "coverage.parquet")
    with pytest.raises(ValueError, match="misses 1 physical contract-days"):
        attach()
