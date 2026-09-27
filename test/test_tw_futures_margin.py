from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.config import load_config, _load_raw_config
from stockagent.data import tw_futures_margin as m
from stockagent.data.tw_stock_context_futures_portfolio import (
    TaiwanStockContextFuturesPortfolioDaily, fixed_futures_slot_symbols,
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
)
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from test_tw_stock_context_futures_portfolio import _stock_panel


def tape(rows=1, slots=1):
    x = torch.zeros(rows, slots, m.MARGIN_EXECUTION_WIDTH)
    x[..., 1] = 1
    x[..., 3:5] = 1000
    x[..., 8] = 1000
    x[..., 9] = torch.arange(slots)
    x[..., m.INITIAL] = x[..., m.END_INITIAL] = x[..., m.PREVIOUS_INITIAL] = 100
    x[..., m.MAINTENANCE] = x[..., m.END_MAINTENANCE] = x[..., m.PREVIOUS_MAINTENANCE] = 75
    x[..., m.PREVIOUS_MARK] = x[..., m.TERMINAL_MARK] = 1000
    x[..., m.CAN_BUY] = x[..., m.CAN_SELL] = 1
    x[..., m.TERMINAL_CAN_BUY] = x[..., m.TERMINAL_CAN_SELL] = 1
    x[..., m.POSITION_GROUP] = torch.arange(slots)
    x[..., m.POSITION_UNIT] = 1
    x[..., m.POSITION_LIMIT] = 1000
    x[..., m.LIQUIDATION_RATIO] = .25
    x[..., m.TERMINAL_CAPACITY] = 1000
    return x


def run(weights, execution, **kw):
    return run_tw_futures_portfolio_integer_torch(
        torch.as_tensor(weights, dtype=torch.float32), execution,
        initial_capital=1000., **kw)


@pytest.mark.parametrize("budget,quantity", [(0., 0), (.2, 2), (.5, 5), (-.5, -5)])
def test_margin_budget_learns_cash_and_leverage(budget, quantity):
    result = run([[budget]], tape())
    assert result.contract_quantities_history.item() == quantity
    assert result.margin_audit_history[0, 7] == abs(quantity)
    assert result.final_equity_scale == 1
    normal = run([[budget]], tape()[..., :11])
    assert normal.contract_quantities_history.item() == 0


def test_price_profit_and_sign_cross_charge_both_sides():
    x = tape(2)
    x[..., 5:7] = 1  # fee + tax = 2 per contract per side
    result = run([[.6], [-.6]], x)
    assert result.contract_quantities_history[:, 0].tolist() == [5, -5]
    assert result.margin_audit_history[:, 2].tolist() == [990, 970]


def test_settlement_gap_and_chunk_boundary_match_exact_account():
    x = tape(3)
    x[:, 0, 3] = torch.tensor([1000, 1020, 1015])
    x[:, 0, 4] = torch.tensor([1010, 1010, 1030])
    x[:, 0, m.PREVIOUS_MARK] = torch.tensor([1000, 1010, 1010])
    weights = [[.5], [.5], [.5]]
    whole = run(weights, x)
    first = run(weights[:1], x[:1])
    rest = run(weights[1:], x[1:], initial_quantities=first.final_weights,
               initial_equity_scale=first.final_equity_scale, initial_alive=first.final_alive)
    torch.testing.assert_close(whole.strategy_returns, torch.cat((first.strategy_returns, rest.strategy_returns)))
    torch.testing.assert_close(whole.margin_audit_history, torch.cat((first.margin_audit_history, rest.margin_audit_history)))
    assert whole.margin_audit_history[:, 11].tolist() == [0, 50, 25]
    assert whole.margin_audit_history[:, 2].tolist() == [1050, 1050, 1150]


@pytest.mark.parametrize("locked", [False, True])
def test_margin_call_flattens_or_records_unfilled_liquidation(locked):
    x = tape(2)
    x[0, :, 4] = 860
    x[1, :, 3:5] = 870
    x[1, :, m.PREVIOUS_MARK] = 860
    x[1, :, m.CAN_SELL] = 0 if locked else 1
    result = run([[.5], [.9]], x)
    assert result.margin_audit_history[0, 8] == 1
    assert result.margin_audit_history[1, 9] == 1
    assert result.margin_audit_history[1, 2] == 350
    assert result.residual_contract_quantities_history[1].item() == (5 if locked else 0)
    assert result.default_reason_history[1] == (4 if locked else 0)
    assert bool(result.final_alive) is not locked


def test_opening_risk_close_can_realize_debt_and_never_resurrects():
    x = tape(3)
    x[1, :, 3:5] = 700
    x[2, :, 3:5] = 2000
    x[2, :, m.PREVIOUS_MARK] = 700
    result = run([[.5], [.5], [.9]], x)
    assert result.margin_audit_history[1, 2] == -500
    assert result.margin_audit_history[1, 9] == 1
    assert result.default_reason_history[1] == 3
    assert result.residual_contract_quantities_history[1].item() == 0
    assert result.turnovers[1] > 0
    assert result.contract_quantities_history[2].item() == 0
    assert not result.final_alive


@pytest.mark.parametrize("sign", [1, -1])
def test_existing_position_between_initial_and_maintenance_is_not_bankrupt(sign):
    x = tape(2, slots=2)
    x[0, 0, 4] = 1000 - sign * 25
    x[1, 0, 3:5] = x[0, 0, 4]
    x[1, 0, m.PREVIOUS_MARK] = x[0, 0, 4]
    x[1, 0, 8] = 0  # Cannot reduce the old position on this session.
    result = run([[sign * .9, 0.], [sign * .8, .1]], x)
    assert result.margin_audit_history[1, 2] == 775
    assert result.final_weights.tolist() == [sign * 9, 0]
    assert not result.default_history.any()
    assert not result.margin_audit_history[:, 8].any()
    assert result.final_alive


@pytest.mark.parametrize("sign", [1, -1])
def test_locked_limit_blocks_only_forbidden_direction(sign):
    x = tape()
    x[..., m.CAN_BUY if sign == 1 else m.CAN_SELL] = 0
    blocked = run([[sign * .5]], x)
    allowed = run([[-sign * .5]], x)
    assert blocked.contract_quantities_history.item() == 0
    assert allowed.contract_quantities_history.item() == -sign * 5


def test_standard_mini_and_months_share_position_capacity():
    x = tape(slots=3)
    x[..., m.POSITION_GROUP] = 0
    x[..., m.POSITION_UNIT] = torch.tensor([1., .25, .05])
    x[..., m.POSITION_LIMIT] = 1
    result = run([[.3, .3, .3]], x)
    units = (result.final_weights.abs() * x[0, :, m.POSITION_UNIT]).sum()
    assert units <= 1
    assert not result.default_history.any()


@pytest.mark.parametrize("expiry", [False, True])
def test_terminal_capacity_and_official_cash_settlement(expiry):
    x = tape()
    x[..., 2] = 1
    x[..., 4] = 1050
    x[..., m.TERMINAL_MARK] = 1100
    x[..., m.TERMINAL_CAPACITY] = 2
    x[..., m.CASH_SETTLEMENT] = int(expiry)
    result = run([[.5]], x, initial_quantities=torch.tensor([5]))
    assert result.residual_contract_quantities_history.item() == (0 if expiry else 3)
    assert result.margin_audit_history[0, 2] == (1500 if expiry else 1350)
    assert result.default_reason_history.item() == (0 if expiry else 4)


@pytest.mark.parametrize("sign", [1, -1])
def test_leverage_surrogate_has_finite_cash_and_subcontract_gradients(sign):
    x = tape(2)
    x[..., 4] += sign * 50
    x[1, :, m.PREVIOUS_MARK] = x[0, :, 4]
    weights = torch.tensor([[.01 * sign], [.01 * sign]], requires_grad=True)
    result = run(weights, x, recoverable_backward=True)
    result.strategy_returns.sum().backward()
    assert not result.contract_quantities_history.any()
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rule_panel(tmp_path):
    panel = _stock_panel(rows=3)
    x = tape(rows=3, slots=1936).numpy()
    x[:, 1:] = 0
    daily_path = tmp_path / "daily.parquet"
    dates = [date(2026, 1, 2) + timedelta(days=i) for i in range(3)]
    pl.DataFrame({"date": dates, "physical_contract": ["TX202601"] * 3,
        "symbol": ["TAIFEX_SLOT_0001"] * 3, "open": [100.] * 3,
        "close": [100.] * 3, "settlement": [100.] * 3, "previous_settlement": [100.] * 3,
        "contract_multiplier": [10.] * 3, "volume": [100.] * 3,
        "liquidation_reason": [""] * 3}).write_parquet(daily_path)
    mask = x[..., 1].astype(bool)
    zero = np.zeros_like(x[..., 0])
    panel.stock_context_futures_portfolio_daily = TaiwanStockContextFuturesPortfolioDaily(
        dates=panel.dates, symbols=fixed_futures_slot_symbols(),
        candidate_features=np.zeros((3, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)), np.float32),
        candidate_mask=mask.copy(), holding_log_returns=zero, executable_mask=mask,
        must_liquidate_mask=zero.astype(bool), can_hold_overnight_mask=mask,
        fee_rate_per_open_notional=zero, open_prices=x[..., 3], close_prices=x[..., 4],
        volumes=zero, benchmark_log_returns=np.zeros(3), source_path=str(daily_path),
        manifest_path="synthetic", integer_execution=x[..., :11].copy())
    rows = [{"date": d, "physical_contract": "TX202601", "margin_kind": "fixed_twd",
        "initial": 100., "maintenance": 75., "settlement_initial": 100., "settlement_maintenance": 75.,
        "known_at": "2026-01-01T00:00:00+08:00", "effective_at": "2026-01-01T00:00:00+08:00",
        "settlement_known_at": "2026-01-01T00:00:00+08:00", "settlement_effective_at": "2026-01-01T00:00:00+08:00",
        "settlement_time": "13:45:00", "position_group": "TX", "position_unit": 1.,
        "position_limit": 12000., "upper_limit": 110., "lower_limit": 90.} for d in dates]
    return panel, rows


def write_rules(tmp_path, panel, rows):
    root = tmp_path / "rules"
    root.mkdir(exist_ok=True)
    path = root / "rules.parquet"
    pl.DataFrame(rows).write_parquet(path)
    source = root / "source.json"
    source.write_text('{"synthetic_test_only": true}')
    (root / "manifest.json").write_text(json.dumps({
        "dataset": "taifex_futures_margin_rules", "schema_version": 1,
        "status": "complete", "point_in_time_verified": True,
        "source_daily_sha256": _hash(Path(panel.stock_context_futures_portfolio_daily.source_path)),
        "outputs": {"rules": {"sha256": _hash(path)}},
        "sources": [{"path": "source.json", "sha256": _hash(source)}]}))
    return path


@pytest.mark.parametrize("date_unit", ["D", "ms", "ns"])
def test_margin_rule_alignment_prior_features_and_no_fill_calendar(tmp_path, date_unit):
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    panel, rows = rule_panel(tmp_path)
    panel = replace(panel, dates=panel.dates.astype(f"datetime64[{date_unit}]"))
    path = write_rules(tmp_path, panel, rows)
    attached = m.attach_futures_margin_rules(panel, path)
    daily = attached.stock_context_futures_portfolio_daily
    assert daily.integer_execution.shape[-1] == m.MARGIN_EXECUTION_WIDTH
    np.testing.assert_allclose(daily.candidate_features[:, 0, -2:], [[.1, .75]] * 3)
    # Current OPEN can change fills but cannot change the model margin features.
    source = Path(daily.source_path)
    pl.read_parquet(source).with_columns(pl.lit(105.).alias("open")).write_parquet(source)
    path = write_rules(tmp_path, panel, rows)
    changed = m.attach_futures_margin_rules(panel, path)
    np.testing.assert_array_equal(daily.candidate_features, changed.stock_context_futures_portfolio_daily.candidate_features)
    attached.stock_context_futures_portfolio_daily = replace(daily, executable_mask=np.zeros_like(daily.executable_mask))
    dataset = CrossSectionalDataset(attached, np.arange(3), lookback=1,
                                   execution_mode="tw_stock_context_futures_portfolio")
    assert 1 in dataset.valid_indices
    assert dataset_to_windowed_tensors(dataset).overnight_log_returns.shape[-1] == 29
    assert not dataset.overnight_log_returns_t[-1, :, 2].any(), "A margin sample boundary must retain marked positions"


@pytest.mark.parametrize("failure", ["future", "missing", "hash", "hierarchy", "group", "timezone"])
def test_historical_rule_release_rejects_unsafe_inputs(tmp_path, failure):
    panel, rows = rule_panel(tmp_path)
    if failure == "future":
        rows[0]["known_at"] = "2026-01-02T08:46:00+08:00"
    if failure == "missing":
        rows.pop()
    if failure == "hierarchy":
        rows[0]["maintenance"] = 200.
    if failure == "group":
        rows[0]["position_group"] = ""
    if failure == "timezone":
        rows[0]["known_at"] = "2026-01-01T00:00:00"
    path = write_rules(tmp_path, panel, rows)
    if failure == "hash":
        (path.parent / "source.json").write_text("changed")
    with pytest.raises(ValueError):
        m.attach_futures_margin_rules(panel, path)


def test_margin_config_keeps_v5_and_separates_action_abi():
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/markets/tw_futures_v8_margin.yaml")
    assert config.runner.output_dir == "artifacts/markets/tw_futures_v8_margin"
    assert config.training.epochs == 1000
    assert len(config.training.financial_transformer.temporal_basis_families) == 22
    assert config.trading.tw_futures_portfolio_capital_basis == "initial_margin"
    margin_contract = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    general = load_config(root / "configs/markets/tw_futures_v8_general.yaml")
    general_contract = build_checkpoint_manifest(_stock_panel(), general, include_data_content=False)
    assert margin_contract["contracts"]["trading"] != general_contract["contracts"]["trading"]
    detail = margin_contract["contracts"]["trading"]["taiwan_stock_context_futures_portfolio"]
    assert detail["integer_training_forward"] == "exact_integer_margin_account_v3_marked_boundary"
    assert detail["sample_boundary_policy"] == "official_settlement_mark_keep_open_positions"
    assert detail["candidate_feature_columns"][-2:] == list(m.MARGIN_FEATURE_COLUMNS)


@pytest.mark.parametrize("change", ["intraday", "surrogate", "compile", "ratio", "missing"])
def test_margin_config_rejects_incompatible_contract(tmp_path, change):
    root = Path(__file__).resolve().parents[1]
    raw = _load_raw_config(root / "configs/markets/tw_futures_v8_margin.yaml")
    if change == "intraday":
        raw["trading"]["tw_futures_portfolio_holding_policy"] = "intraday"
    elif change == "surrogate":
        raw["training"]["futures_portfolio_training_surrogate_only"] = True
    elif change == "compile":
        raw["training"]["backtest_compile"] = True
    elif change == "ratio":
        raw["trading"]["tw_futures_portfolio_margin_liquidation_ratio"] = .1
    else:
        raw["trading"]["tw_futures_portfolio_margin_rules_path"] = None
    path = tmp_path / "bad.yaml"
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_config(path)


def test_v5_margin_encoder_is_causal_and_trainable():
    torch.set_num_threads(2)
    model = FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode="market_token", num_market_tokens=2,
        temporal_heads=2, temporal_layers=1, temporal_pooling="last",
        temporal_query_mode="last_only", use_symbol_pos=False,
        portfolio_mode="long_short", portfolio_output_mode="score_entmax_log_cash",
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        futures_margin_budget_output=True, feature_bottleneck_dim=2,
        causal_feature_rms_normalization=True, dropout=0., return_aux=False,
    ).eval()
    features = torch.zeros(1, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS) + 2)
    features[..., -2:] = torch.tensor([.1, .75])
    mask = torch.zeros(1, 1936, dtype=torch.bool)
    mask[:, :2] = True
    x = torch.randn(1, 4, 2, 3)
    context = {"candidate_features": features, "candidate_mask": mask}
    weights = model(x, torch.ones(1, 2, dtype=torch.bool), portfolio_context=context)
    changed = features.clone()
    changed[..., -3] = 1e8  # ignored current OPEN denomination field
    other = model(x, torch.ones(1, 2, dtype=torch.bool), portfolio_context={**context, "candidate_features": changed})
    torch.testing.assert_close(weights, other, atol=0, rtol=0)
    assert weights.abs().sum() <= 1.000001 and not weights[:, 2:].any()
    weights[:, 0].sum().backward()
    assert model.futures_margin_encoder.weight.grad.abs().sum() > 0


@pytest.mark.parametrize("chunk_rows", [1, 2, 3])
def test_canonical_evaluation_and_npz_keep_margin_audit(tmp_path, chunk_rows):
    from stockagent.training import trainer as t
    x = tape(3)
    x[0, :, 4] = 860
    x[1, :, 3:5] = 870
    x[1, :, m.PREVIOUS_MARK] = 860
    x[1, :, m.CAN_SELL] = 0
    weights = torch.full((3, 1), .5)
    truth = run(weights, x)
    yes, no = torch.ones_like(weights, dtype=torch.bool), torch.zeros_like(weights, dtype=torch.bool)
    runtime = t._ExecutionRuntime(mode="tw_stock_context_futures_portfolio", buy_fee_rates=None,
        sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0, futures_initial_capital=1000.)
    result, _ = t._run_eval_backtest_from_weight_buffers(
        weights, torch.zeros_like(weights), yes, yes, yes, yes, no, no, torch.zeros(3),
        device=torch.device("cpu"), non_blocking=False, long_only=False, buy_fee_rate=0., sell_fee_rate=0.,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0., backtest_chunk_rows=chunk_rows,
        compute_metrics_summary=False, return_weights_history=True, profile_timing=False,
        progress_label=None, timing=t.TimingBreakdown(), reset_at_rows=None,
        portfolio_activation="pre_normalized", overnight_log_returns_all=x, execution_runtime=runtime,
    )
    torch.testing.assert_close(result.strategy_returns, truth.strategy_returns)
    torch.testing.assert_close(result.futures_margin_audit, truth.margin_audit_history)
    torch.testing.assert_close(result.futures_residual_contract_quantities_history, truth.residual_contract_quantities_history)
    path = tmp_path / "backtest.npz"
    t._save_backtest_artifact(path, result.to_numpy(), np.arange(3))
    loaded, _ = t._load_backtest_artifact(path)
    np.testing.assert_array_equal(loaded.futures_margin_audit, truth.margin_audit_history.numpy())
    assert loaded.futures_residual_contract_quantities_history[1, 0] == 5
    with np.load(path) as saved:
        assert list(saved["futures_margin_audit_columns"]) == list(m.MARGIN_AUDIT_COLUMNS)


@pytest.mark.parametrize("batch_size", [1, 2])
def test_canonical_epoch_updates_once_and_preserves_margin_equity(tmp_path, batch_size):
    from functools import partial
    from torch.amp import GradScaler
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    from stockagent.training.loss import risk_aware_loss
    from stockagent.training.trainer import _train_epoch_windowed_tensor, _ExecutionRuntime
    from test_futures_trajectory_optimizer import _CountingSGD, _CountingScheduler

    panel, rules = rule_panel(tmp_path)
    path = write_rules(tmp_path, panel, rules)
    panel = m.attach_futures_margin_rules(panel, path)
    daily = panel.stock_context_futures_portfolio_daily
    daily.integer_execution[1:, 0, 4] += 50
    daily.integer_execution[1:, 0, m.TERMINAL_MARK] += 50
    dataset = CrossSectionalDataset(panel, np.arange(3), lookback=1,
                                   execution_mode="tw_stock_context_futures_portfolio")
    split = dataset_to_windowed_tensors(dataset)
    class Policy(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.budget = torch.nn.Parameter(torch.tensor(.5))
        def forward(self, x, mask, *, portfolio_context):
            action = self.budget * torch.ones(x.shape[0], 1936)
            return action * portfolio_context["candidate_mask"]
    model = Policy()
    optimizer = _CountingSGD(model.parameters())
    scheduler = _CountingScheduler()
    loss_fn = partial(risk_aware_loss, execution_mode=split.execution_mode,
        portfolio_activation="pre_normalized", day_trade_execution_initial_capital=1000.,
        futures_portfolio_recoverable_backward=True)
    kwargs = dict(long_only=False, buy_fee_rate=0., sell_fee_rate=0., max_turnover_ratio=0.,
        gross_leverage=1., gamma_sharpe=1., gamma_excess=0., gamma_cvar=0., cvar_alpha=.05,
        gamma_drawdown=0., drawdown_target=0., gamma_turnover=0., gamma_underperformance=0.,
        excess_target=0., cvar_budget=0., drawdown_budget=0., turnover_budget=0.,
        gamma_cvar_budget=0., gamma_drawdown_budget=0., gamma_turnover_budget=0.,
        objective="log_utility", grad_clip_norm=1., rank_ic_weight=0., return_rank_ic_weight=0.,
        direction_weight=0., volatility_regime_weight=0., concentration_weight=0.)
    loss, timing = _train_epoch_windowed_tensor(model, None, loss_fn, split, optimizer,
        GradScaler("cpu", enabled=False), batch_size=batch_size, device=torch.device("cpu"),
        amp_dtype=None, non_blocking=False, optimizer_step_per_trajectory=True,
        execution_runtime=_ExecutionRuntime(mode=split.execution_mode, buy_fee_rates=None,
            sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0, futures_initial_capital=1000.),
        lr_scheduler=scheduler, lr_scheduler_interval="step", **kwargs)
    assert torch.isfinite(loss)
    assert optimizer.step_calls == scheduler.step_calls == 1
    assert timing.gradient_norm_observations == 1
    assert model.budget.item() != pytest.approx(.5)
