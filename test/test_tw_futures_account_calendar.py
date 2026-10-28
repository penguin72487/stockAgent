from datetime import date
from types import SimpleNamespace
import json

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.data import tw_futures_margin as margin
from stockagent.data.tw_futures_account_calendar import (
    aggregate_account_log_returns_to_decisions,
    build_futures_account_calendar,
    interleave_account_only_ledger_rows,
    next_decision_after_market_rows,
    slice_futures_account_calendar,
    slice_futures_account_calendar_batch,
)
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.backtest.tw_futures_account_calendar import run_futures_account_calendar_torch
from stockagent.models.futures_account_policy import (
    FUTURES_ACCOUNT_STATE_COLUMNS, pack_futures_account_policy,
)
from test_tw_futures_margin import tape


def calendar():
    return build_futures_account_calendar(
        ["2016-02-03", "2016-02-15"],
        ["2010-12-31", "2016-02-03", "2016-02-04", "2016-02-15"],
        [False, False, True, False],
    )


def test_calendar_keeps_account_day_and_original_decision_axis():
    result = calendar()
    assert result.dates.tolist() == [date(2016, 2, 3), date(2016, 2, 4), date(2016, 2, 15)]
    assert result.account_only_mask.tolist() == [False, True, False]
    assert result.decision_indices.tolist() == [0, 2]
    assert result.account_only_indices.tolist() == [1]
    assert result.prior_decision_index.tolist() == [0, 0, 1]
    assert result.fingerprint == calendar().fingerprint
    changed = build_futures_account_calendar(
        ["2016-02-03", "2016-02-15"],
        ["2016-02-03", "2016-02-05", "2016-02-15"],
        [False, True, False],
    )
    assert changed.fingerprint != result.fingerprint


def test_calendar_rejects_missing_market_session_and_bad_flags():
    with pytest.raises(ValueError, match="omits 1 futures market sessions"):
        build_futures_account_calendar(
            ["2016-02-03", "2016-02-15"],
            ["2016-02-03", "2016-02-04", "2016-02-15"],
            [False, False, False],
        )
    with pytest.raises(ValueError, match="complete booleans"):
        build_futures_account_calendar(
            ["2016-02-03", "2016-02-15"],
            ["2016-02-03", "2016-02-04", "2016-02-15"],
            [False, None, False],
        )
    with pytest.raises(ValueError, match="strictly increasing"):
        build_futures_account_calendar(
            ["2016-02-03", "2016-02-03"], ["2016-02-03"], [False],
        )
    with pytest.raises(ValueError, match="coincides with a futures-account-only"):
        build_futures_account_calendar(
            ["2016-02-03", "2016-02-15"], ["2016-02-03", "2016-02-15"], [False, True],
        )
    with pytest.raises(ValueError, match="futures source omits 1 stock decision sessions"):
        build_futures_account_calendar(
            ["2016-02-03", "2016-02-15"], ["2016-02-03"], [False],
        )


def test_prior_market_features_skip_account_only_nodes():
    dates = calendar()
    # Repeated calendar indices represent different source physical contracts.
    indices = np.asarray([0, 0, 1, 1, 2], dtype=np.int64)
    assert next_decision_after_market_rows(dates, indices).tolist() == [2, 2, -1, -1, -1]
    with pytest.raises(ValueError, match="valid integer account nodes"):
        next_decision_after_market_rows(dates, np.asarray([-1, 3]))
    with pytest.raises(ValueError, match="valid integer account nodes"):
        next_decision_after_market_rows(dates, np.asarray([0.0]))


@pytest.mark.parametrize("channel,value", [(1, float("nan")), (8, float("nan")),
                                           (2, float("nan")), (margin.CASH_SETTLEMENT, .5)])
def test_interleave_rejects_unknown_account_capacity_and_terminal_flags(channel, value):
    event = tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., channel] = value
    with pytest.raises(ValueError, match="account-only"):
        interleave_account_only_ledger_rows(calendar(), torch.zeros((2, 1)), tape(rows=2), event)


@pytest.mark.parametrize("sign,expected_equity", [(1, 1475), (-1, 475)])
def test_off_session_cash_settlement_preserves_ledger_and_gradients(sign, expected_equity):
    dates = calendar()
    decisions = tape(rows=2)
    event = tape(rows=1)
    event[..., 1] = 0  # no executable exchange session
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., 4] = 1050
    event[..., margin.TERMINAL_MARK] = 1100
    event[..., 5] = 2  # terminal fee
    event[..., 7] = 3  # terminal tax
    actions = torch.tensor([[sign * .5], [0.]], requires_grad=True)
    expanded_actions, expanded_execution = interleave_account_only_ledger_rows(
        dates, actions, decisions, event,
    )
    actual = run_tw_futures_portfolio_integer_torch(
        expanded_actions, expanded_execution, initial_capital=1000.,
    )
    expected_actions = torch.tensor([[sign * .5], [0.], [0.]], requires_grad=True)
    reference = run_tw_futures_portfolio_integer_torch(
        expected_actions, torch.cat((decisions[:1], event, decisions[1:])), initial_capital=1000.,
    )
    # The first quantity history is pre-terminal; residual history is the
    # inventory after the cash settlement has actually been applied.
    assert actual.contract_quantities_history[:, 0].tolist() == [sign * 5, sign * 5, 0]
    assert actual.residual_contract_quantities_history[:, 0].tolist() == [sign * 5, 0, 0]
    assert actual.margin_audit_history[1, 2].item() == expected_equity
    assert actual.final_equity_scale.item() == pytest.approx(expected_equity / 1000)
    torch.testing.assert_close(actual.strategy_returns, reference.strategy_returns)
    torch.testing.assert_close(actual.margin_audit_history, reference.margin_audit_history)
    actual.strategy_returns.sum().backward()
    reference.strategy_returns.sum().backward()
    torch.testing.assert_close(actions.grad, expected_actions.grad[[0, 2]])
    assert torch.isfinite(actions.grad).all()
    market_returns = aggregate_account_log_returns_to_decisions(dates, actual.strategy_returns)
    torch.testing.assert_close(market_returns.sum(), actual.strategy_returns.sum())
    torch.testing.assert_close(market_returns[1], actual.strategy_returns[1:].sum())


@pytest.mark.parametrize("field,value", [(1, 1), (8, 1), (2, 1)])
def test_account_row_never_fabricates_a_market_trade(field, value):
    event = tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., field] = value
    with pytest.raises(ValueError):
        interleave_account_only_ledger_rows(calendar(), torch.zeros(2, 1), tape(rows=2), event)


def test_market_log_return_aggregation_keeps_event_pnl_and_gradient():
    values = torch.tensor([.1, -.2, .3], requires_grad=True)
    collapsed = aggregate_account_log_returns_to_decisions(calendar(), values)
    torch.testing.assert_close(collapsed, torch.tensor([.1, .1]))
    collapsed.sum().backward()
    torch.testing.assert_close(values.grad, torch.ones_like(values))


def test_account_calendar_runner_preserves_event_and_model_gradient():
    dates = calendar()
    decisions = tape(rows=2)
    event = tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1100
    event[..., 5] = 2
    event[..., 7] = 3
    actions = torch.tensor([[.5], [0.]], requires_grad=True)
    result = run_futures_account_calendar_torch(
        dates, actions, decisions, event, initial_capital=1000.,
        decision_state_advance=torch.tensor([True, True]), account_state_advance=torch.tensor([True]),
    )
    assert len(result.account_ledger.strategy_returns) == 3
    assert len(result.decision_log_returns) == 2
    assert result.account_ledger.residual_contract_quantities_history[1, 0] == 0
    torch.testing.assert_close(result.decision_log_returns[1], result.account_ledger.strategy_returns[1:].sum())
    result.decision_log_returns.sum().backward()
    assert torch.isfinite(actions.grad).all()
    reference_actions = torch.tensor([[.5], [0.], [0.]], requires_grad=True)
    reference = run_tw_futures_portfolio_integer_torch(
        reference_actions, torch.cat((decisions[:1], event, decisions[1:])), initial_capital=1000.,
    )
    reference.strategy_returns.sum().backward()
    torch.testing.assert_close(actions.grad, reference_actions.grad[[0, 2]])
    with pytest.raises(ValueError, match="explicitly cover"):
        run_futures_account_calendar_torch(
            dates, actions, decisions, event, initial_capital=1000.,
            decision_state_advance=torch.tensor([True, True]),
        )


def test_account_calendar_runner_keeps_ruin_absorbing_across_next_decision():
    decisions, event = tape(rows=2), tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1
    result = run_futures_account_calendar_torch(
        calendar(), torch.tensor([[.5], [.5]]), decisions, event, initial_capital=1000.,
    )
    assert result.account_ledger.default_history[1]
    assert not result.account_ledger.final_alive
    assert result.account_ledger.final_equity_scale == 0
    assert result.account_ledger.contract_quantities_history[2, 0] == 0
    torch.testing.assert_close(result.decision_log_returns.sum(), result.account_ledger.strategy_returns.sum())


def test_chunk_boundary_replays_prior_inventory_before_the_next_decision():
    full_calendar = calendar()
    first_calendar = slice_futures_account_calendar(full_calendar, 0, 1)
    next_calendar = slice_futures_account_calendar(full_calendar, 1, 2)
    assert first_calendar.dates.tolist() == [date(2016, 2, 3)]
    assert next_calendar.dates.tolist() == [date(2016, 2, 4), date(2016, 2, 15)]
    assert next_calendar.prior_decision_index.tolist() == [-1, 0]
    decisions = tape(rows=2)
    event = tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1100
    event[..., 5] = 2
    event[..., 7] = 3
    actions = torch.tensor([[.5], [0.]])
    all_actions, all_execution = interleave_account_only_ledger_rows(
        full_calendar, actions, decisions, event,
    )
    full = run_tw_futures_portfolio_integer_torch(
        all_actions, all_execution, initial_capital=1000.,
    )
    first = run_tw_futures_portfolio_integer_torch(
        actions[:1], decisions[:1], initial_capital=1000.,
    )
    following_actions, following_execution = interleave_account_only_ledger_rows(
        next_calendar, actions[1:], decisions[1:], event,
    )
    following = run_tw_futures_portfolio_integer_torch(
        following_actions, following_execution, initial_capital=1000.,
        initial_quantities=first.final_weights, initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
    )
    torch.testing.assert_close(following.strategy_returns, full.strategy_returns[1:])
    torch.testing.assert_close(following.margin_audit_history, full.margin_audit_history[1:])
    assert following.residual_contract_quantities_history[0, 0] == 0
    assert following.final_equity_scale == full.final_equity_scale
    with pytest.raises(IndexError):
        slice_futures_account_calendar(full_calendar, 1, 1)


def test_sparse_event_batch_keeps_original_tape_and_requires_incoming_state():
    dates = calendar()
    decisions, event = tape(rows=2), tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1100
    actions = torch.tensor([[.5], [0.]])
    first = slice_futures_account_calendar_batch(dates, event, np.array([0]))
    second = slice_futures_account_calendar_batch(dates, event, np.array([1]))
    assert (first.account_event_start, first.account_event_stop) == (0, 0)
    assert (second.account_event_start, second.account_event_stop) == (0, 1)
    assert second.account_execution.data_ptr() == event.data_ptr()
    assert second.calendar.dates.tolist() == [date(2016, 2, 4), date(2016, 2, 15)]
    with pytest.raises(ValueError, match="requires explicit incoming"):
        run_futures_account_calendar_torch(
            second.calendar, actions[1:], decisions[1:], second.account_execution,
            initial_capital=1000.,
        )
    opening = run_futures_account_calendar_torch(
        first.calendar, actions[:1], decisions[:1], first.account_execution,
        initial_capital=1000.,
    )
    following = run_futures_account_calendar_torch(
        second.calendar, actions[1:], decisions[1:], second.account_execution,
        initial_capital=1000., initial_quantities=opening.account_ledger.final_weights,
        initial_equity_scale=opening.account_ledger.final_equity_scale,
        initial_alive=opening.account_ledger.final_alive,
    )
    whole = run_futures_account_calendar_torch(
        dates, actions, decisions, event, initial_capital=1000.,
    )
    torch.testing.assert_close(following.decision_log_returns, whole.decision_log_returns[1:])
    torch.testing.assert_close(following.account_ledger.final_equity_scale,
                               whole.account_ledger.final_equity_scale)
    with pytest.raises(ValueError, match="contiguous decision"):
        slice_futures_account_calendar_batch(dates, event, np.array([0, 2]))
    with pytest.raises(ValueError, match="original account calendar"):
        slice_futures_account_calendar_batch(dates, event[:0], np.array([0]))


def test_canonical_simulator_and_loss_include_event_and_preserve_gradients():
    from stockagent.backtest.simulator import run_backtest_torch
    from stockagent.training.loss import risk_aware_loss

    slots = 1936
    decisions, event = tape(rows=2, slots=slots), tape(rows=1, slots=slots)
    event[..., 1] = 0
    event[..., 8] = 0
    event[0, 0, 2] = 1
    event[0, 0, margin.CASH_SETTLEMENT] = 1
    event[0, 0, margin.TERMINAL_MARK] = 1100
    event[0, 0, 5] = 2
    event[0, 0, 7] = 3
    actions = torch.zeros(2, slots)
    actions[0, 0] = .5
    actions.requires_grad_()
    stock_mask = torch.ones(2, 2, dtype=torch.bool)
    common = dict(long_only=False, portfolio_activation="pre_normalized",
                  overnight_returns=decisions, execution_mode="tw_stock_context_futures_portfolio",
                  day_trade_execution_initial_capital=1000.)
    simulated = run_backtest_torch(actions, torch.zeros(2, 2), stock_mask, torch.zeros(2),
        0., 0., futures_account_calendar=calendar(), account_only_execution=event, **common)
    reference_actions = torch.zeros(3, slots)
    reference_actions[0, 0] = .5
    reference_actions.requires_grad_()
    reference = run_tw_futures_portfolio_integer_torch(
        reference_actions, torch.cat((decisions[:1], event, decisions[1:])), initial_capital=1000.,
    )
    torch.testing.assert_close(simulated.strategy_returns,
                               torch.stack((reference.strategy_returns[0], reference.strategy_returns[1:].sum())))
    assert simulated.futures_account_calendar_result is not None
    assert simulated.futures_account_calendar_result.account_ledger.margin_audit_history[1, 2] == 1475
    with pytest.raises(ValueError, match="dated account-ledger artifact writer"):
        simulated.to_numpy()
    simulated.strategy_returns.sum().backward()
    reference.strategy_returns.sum().backward()
    torch.testing.assert_close(actions.grad, reference_actions.grad[[0, 2]])

    loss_actions = actions.detach().clone().requires_grad_()
    aux = {"_capture_daily_log_returns": True}
    loss = risk_aware_loss(loss_actions, torch.zeros(2, 2), stock_mask,
        benchmark_returns=torch.zeros(2), long_only=False, portfolio_activation="pre_normalized",
        execution_mode="tw_stock_context_futures_portfolio", overnight_log_returns=decisions,
        day_trade_execution_initial_capital=1000., objective="log_utility",
        log_utility_periods_per_year=1., gamma_turnover=0., concentration_weight=0.,
        futures_account_calendar=calendar(), account_only_execution=event, aux_outputs=aux)
    torch.testing.assert_close(loss, -simulated.strategy_returns.detach().mean())
    torch.testing.assert_close(aux["_daily_log_returns"], simulated.strategy_returns.detach())
    loss.backward()
    torch.testing.assert_close(loss_actions.grad, -actions.grad / 2)


def test_canonical_simulator_attributes_event_ruin_and_rejects_incomplete_context():
    from stockagent.backtest.simulator import run_backtest_torch

    slots = 1936
    decisions, event = tape(rows=2, slots=slots), tape(rows=1, slots=slots)
    event[..., 1] = 0
    event[..., 8] = 0
    event[0, 0, 2] = 1
    event[0, 0, margin.CASH_SETTLEMENT] = 1
    event[0, 0, margin.TERMINAL_MARK] = 1
    actions = torch.zeros(2, slots)
    actions[:, 0] = .5
    mask = torch.ones(2, 2, dtype=torch.bool)
    args = (actions, torch.zeros(2, 2), mask, torch.zeros(2), 0., 0.)
    kwargs = dict(long_only=False, portfolio_activation="pre_normalized",
                  overnight_returns=decisions, execution_mode="tw_stock_context_futures_portfolio",
                  day_trade_execution_initial_capital=1000.)
    result = run_backtest_torch(*args, **kwargs, futures_account_calendar=calendar(),
                                account_only_execution=event, return_weights_history=False)
    assert result.settlement_default.tolist() == [False, True]
    assert result.default_reason_history[1] != 0
    assert not result.final_alive
    assert result.weights_history.shape == (0, slots)
    with pytest.raises(ValueError, match="both calendar and event tape"):
        run_backtest_torch(*args, **kwargs, futures_account_calendar=calendar())


def test_causal_account_policy_is_not_asked_to_trade_on_a_cash_event():
    logits = torch.tensor([[5.], [0.]], requires_grad=True)
    zeros = torch.zeros_like(logits)
    ones = torch.ones_like(logits)
    packet = pack_futures_account_policy(
        zeros, logits, torch.zeros(2, 1, len(FUTURES_ACCOUNT_STATE_COLUMNS)),
        torch.tensor([[True], [False]]), 1000 * ones, 100 * ones, 75 * ones,
        zeros, zeros, zeros,
    )
    decisions = tape(rows=2)
    event = tape(rows=1)
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1050
    actions, execution = interleave_account_only_ledger_rows(calendar(), packet, decisions, event)
    result = run_tw_futures_portfolio_integer_torch(actions, execution, initial_capital=1000.)
    assert result.contract_quantities_history[0, 0] > 0
    assert result.requested_weights_history[1, 0] == 0
    assert result.residual_contract_quantities_history[1, 0] == 0
    result.strategy_returns.sum().backward()
    assert torch.isfinite(logits.grad).all()


def test_source_audit_binds_dates_and_cash_settlement_to_release(monkeypatch, tmp_path):
    from downloader.artifact_io import sha256_file
    from scripts import audit_tw_futures_account_calendar as source_audit

    daily_dir, rules_dir = tmp_path / "daily", tmp_path / "rules"
    daily_dir.mkdir()
    rules_dir.mkdir()
    daily_path, rules_path = daily_dir / "continuous_daily.parquet", rules_dir / "rules.parquet"
    axes_path, config_path, output = tmp_path / "axes.json", tmp_path / "config.yaml", tmp_path / "audit.json"
    config_path.write_text("fixture: true\n")
    axes_path.write_text(json.dumps({"dates": ["2016-02-03", "2016-02-15"]}))
    pl.DataFrame({
        "date": [date(2016, 2, 3), date(2016, 2, 4), date(2016, 2, 15)],
        "accounting_only_date": [False, True, False], "product": ["TJF"] * 3,
        "physical_contract": ["TJF201602"] * 3, "symbol": ["TAIFEX_SLOT_1"] * 3,
        "cash_settlement": [False, True, False], "executable": [True, False, False],
    }).write_parquet(daily_path)
    pl.DataFrame({
        "date": [date(2016, 2, 4)], "physical_contract": ["TJF201602"],
        "terminal_event": ["cash_settlement"], "carry_from_date": [date(2016, 2, 3)],
        "carry_from_physical_contract": ["TJF201602"], "inventory_entry_reachable": [True],
        "opening_contract_value_twd": [1000.], "terminal_contract_value_twd": [1100.],
        "terminal_tax_twd": [3.],
    }).write_parquet(rules_path)
    (daily_dir / "manifest.json").write_text(json.dumps({
        "status": "complete", "outputs": {"continuous_daily": {"sha256": sha256_file(daily_path)}},
        "execution_rules_sha256": sha256_file(rules_path),
    }))
    (rules_dir / "manifest.json").write_text(json.dumps({
        "status": "complete", "outputs": {"rules": {"sha256": sha256_file(rules_path)}},
        "source_daily_sha256": sha256_file(daily_path),
    }))
    config = SimpleNamespace(
        trading=SimpleNamespace(execution_mode="tw_stock_context_futures_portfolio",
            tw_futures_portfolio_capital_basis="initial_margin", tw_futures_portfolio_data_path=str(daily_path),
            tw_futures_portfolio_margin_rules_path=str(rules_path),
            tw_futures_portfolio_benchmark_mode="legacy_front_holding_return",
            tw_futures_portfolio_valuation_research_contract=None),
        data=SimpleNamespace(factorized_feature_manifest=str(axes_path), panel_start_date="2011-01-03"),
    )
    monkeypatch.setattr(source_audit, "load_config", lambda _: config)
    receipt = source_audit.audit(config_path, output)
    assert receipt["account_only_dates"] == ["2016-02-04"]
    assert receipt["cash_settlement_rows"] == 1
    assert receipt["cash_settlement_details"][0]["carry_from_date"] == "2016-02-03"
    assert not receipt["model_attachment_verified"] and not receipt["training_started"]
    assert json.loads(output.read_text())["inputs_sha256"][str(daily_path)] == sha256_file(daily_path)
    with daily_path.open("ab") as stream:
        stream.write(b"corruption")
    with pytest.raises(ValueError, match="identity"):
        source_audit.audit(config_path, output)
