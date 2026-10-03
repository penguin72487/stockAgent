"""Conservation, whole-inventory and gradient checks for dated carry events."""
import numpy as np
import polars as pl
import pytest
import torch

from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape, run, rule_panel, write_rules


def corporate_tape(rows=1, slots=1):
    old = tape(rows, slots)
    x = torch.zeros(rows, slots, m.MARGIN_CORPORATE_EXECUTION_WIDTH)
    x[..., :m.MARGIN_EXECUTION_WIDTH] = old
    x[..., m.CARRY_SOURCE_SLOT] = torch.arange(1, slots + 1)
    x[..., m.CARRY_QUANTITY_NUMERATOR:m.CARRY_QUANTITY_DENOMINATOR + 1] = 1
    return x


@pytest.mark.parametrize('width', [*m.MARGIN_EXECUTION_WIDTHS, 35])
@pytest.mark.parametrize('slots',[1936,2560])
def test_training_window_accepts_versioned_margin_tape_and_rejects_unknown_width(width,slots):
    from stockagent.training.windowed import WindowedSplitTensors
    from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
    kwargs=dict(features=torch.zeros(2,1,1), valid_indices=torch.tensor([1]),
        future_log_returns=torch.zeros(2,1), tradable_mask=torch.ones(2,1,dtype=torch.bool),
        can_buy_mask=torch.ones(2,1,dtype=torch.bool), can_sell_mask=torch.ones(2,1,dtype=torch.bool),
        benchmark=torch.zeros(2), lookback=1, execution_mode='tw_stock_context_futures_portfolio',
        overnight_log_returns=torch.zeros(2,slots,width),
        derivative_candidate_features=torch.zeros(2,slots,len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)),
        derivative_candidate_mask=torch.ones(2,slots,dtype=torch.bool))
    if width not in m.MARGIN_EXECUTION_WIDTHS:
        with pytest.raises(ValueError,match='execution tensor'):
            WindowedSplitTensors(**kwargs)
    else:
        split=WindowedSplitTensors(**kwargs)
        assert split.overnight_log_returns.shape==(2,slots,width)


@pytest.mark.parametrize("sign", [1, -1])
def test_cash_dividend_conserves_wealth_only_for_old_inventory(sign):
    x = corporate_tape(3)
    x[1:, :, 3:5] = 980
    x[1:, :, m.TERMINAL_MARK] = 980
    x[1, :, m.CARRY_CASH] = 20
    x[2, :, m.PREVIOUS_MARK] = 980
    x[..., 8] = 0  # Nothing can trade; dividend is not a fill.
    result = run([[0.]] * 3, x, initial_quantities=torch.tensor([sign * 5]))
    torch.testing.assert_close(result.equity_scale_history, torch.ones(3))
    assert result.final_weights.tolist() == [sign * 5]
    assert not result.turnovers.any()
    empty = run([[0.]] * 3, x)
    assert empty.final_equity_scale == 1
    assert not empty.contract_quantities_history.any()


@pytest.mark.parametrize("sign", [1, -1])
def test_split_changes_contract_count_not_wealth_or_turnover(sign):
    x = corporate_tape()
    x[..., 3:5] = x[..., m.PREVIOUS_MARK] = x[..., m.TERMINAL_MARK] = 100
    x[..., m.INITIAL] = x[..., m.END_INITIAL] = x[..., m.PREVIOUS_INITIAL] = 10
    x[..., m.MAINTENANCE] = x[..., m.END_MAINTENANCE] = x[..., m.PREVIOUS_MAINTENANCE] = 7.5
    x[..., m.CARRY_QUANTITY_NUMERATOR] = 10
    x[..., 8] = 0
    x[..., 5:8] = 2  # No fees are charged to the legal quantity conversion.
    result = run([[sign * .5]], x, initial_quantities=torch.tensor([sign * 5]))
    assert result.final_weights.tolist() == [sign * 50]
    assert result.final_equity_scale == 1
    assert not result.turnovers.any()
    assert result.final_alive


def test_code_transfer_does_not_attach_old_inventory_to_new_standard_code():
    x = corporate_tape(2, 2)
    x[0, 1] = 0
    x[1, :, m.CARRY_SOURCE_SLOT] = torch.tensor([0, 1])
    x[1, 1, 3:5] = x[1, 1, m.TERMINAL_MARK] = 980
    x[1, 1, m.CARRY_CASH] = 20
    x[1, :, 8] = 0
    result = run([[.5, 0.], [0., .5]], x)
    assert result.contract_quantities_history.tolist() == [[5, 0], [0, 5]]
    assert result.final_equity_scale == 1
    assert result.turnovers[1] == 0
    first = run([[.5, 0.]], x[:1])
    tail = run([[0., .5]], x[1:], initial_quantities=first.final_weights,
               initial_equity_scale=first.final_equity_scale, initial_alive=first.final_alive)
    torch.testing.assert_close(result.strategy_returns[1:], tail.strategy_returns)
    torch.testing.assert_close(result.margin_audit_history[1:], tail.margin_audit_history)


@pytest.mark.parametrize("bad", ["lost", "duplicated", "fractional", "invalid_index", "invalid_ratio"])
def test_invalid_carry_never_erases_or_invents_a_held_contract(bad):
    x = corporate_tape(slots=2)
    x[..., 8] = 0
    if bad == "lost": x[0, 0, m.CARRY_SOURCE_SLOT] = 0
    if bad == "duplicated": x[0, 1, m.CARRY_SOURCE_SLOT] = 1
    if bad == "fractional": x[0, 0, m.CARRY_QUANTITY_DENOMINATOR] = 3
    if bad == "invalid_index": x[0, 0, m.CARRY_SOURCE_SLOT] = .5
    if bad == "invalid_ratio": x[0, 0, m.CARRY_QUANTITY_NUMERATOR] = float('nan')
    result = run([[0., 0.]], x, initial_quantities=torch.tensor([5, 0]))
    assert not result.final_alive
    assert result.default_reason_history.item() == 1


def test_non_advancing_padding_cannot_apply_corporate_actions():
    x = corporate_tape()
    x[..., m.CARRY_CASH] = 999
    x[..., m.CARRY_QUANTITY_NUMERATOR] = 10
    result = run([[0.]], x, initial_quantities=torch.tensor([5]), state_advance_mask=torch.tensor([False]))
    assert result.final_weights.tolist() == [5]
    assert result.final_equity_scale == 1
    assert not result.strategy_returns.any()


def test_gradient_follows_old_inventory_across_transfer_and_cash_is_not_a_free_profit():
    x = corporate_tape(2, 2)
    x[0, 1] = 0
    x[1, :, m.CARRY_SOURCE_SLOT] = torch.tensor([0, 1])
    x[1, 1, 3] = 980
    x[1, 1, 4] = x[1, 1, m.TERMINAL_MARK] = 990
    x[1, 1, m.CARRY_CASH] = 20
    x[1, :, 8] = 0
    weights = torch.tensor([[.5, 0.], [0., .5]], requires_grad=True)
    result = run(weights, x)
    exact = run(weights.detach(), x)
    torch.testing.assert_close(result.strategy_returns, exact.strategy_returns, rtol=0, atol=0)
    result.strategy_returns.sum().backward()
    # Only the actual +10 move is earned: 5 old contracts make TWD 50.
    assert result.final_equity_scale.item() == pytest.approx(1.05)
    assert weights.grad[0, 0].item() == pytest.approx(100. / 1050., rel=1e-5)
    assert not weights.grad[1].any()


@pytest.mark.parametrize('width', [m.MARGIN_CORPORATE_EXECUTION_WIDTH,m.MARGIN_MULTI_LIMIT_EXECUTION_WIDTH])
@pytest.mark.parametrize('slots',[1936,2560])
def test_canonical_training_loss_accepts_and_differentiates_corporate_carry(width,slots):
    from stockagent.training.loss import risk_aware_loss
    x = torch.zeros(2, slots, width)
    active = corporate_tape(2, 2)
    active[0, 1] = 0
    active[1, :, m.CARRY_SOURCE_SLOT] = torch.tensor([0, 1])
    active[1, 1, 3] = 980
    active[1, 1, 4] = active[1, 1, m.TERMINAL_MARK] = 990
    active[1, 1, m.CARRY_CASH] = 20
    active[1, :, 8] = 0
    x[:, :2, :m.MARGIN_CORPORATE_EXECUTION_WIDTH] = active
    if width==m.MARGIN_MULTI_LIMIT_EXECUTION_WIDTH:
        x[...,m.SECOND_POSITION_GROUP:m.SECOND_POSITION_LIMIT+1]=x[...,m.POSITION_GROUP:m.POSITION_LIMIT+1]
    weights = torch.zeros(2, slots)
    weights[0, 0] = weights[1, 1] = .5
    weights.requires_grad_()
    loss = risk_aware_loss(weights, torch.zeros(2, 1), torch.ones(2, 1, dtype=torch.bool),
        execution_mode='tw_stock_context_futures_portfolio', objective='log_utility',
        overnight_log_returns=x, day_trade_execution_initial_capital=1000.,
        long_only=False, portfolio_activation='pre_normalized', concentration_weight=0.,
        return_rank_ic_weight=0., rank_ic_weight=0., direction_weight=0., volatility_regime_weight=0.)
    assert torch.isfinite(loss)
    loss.backward()
    assert torch.isfinite(weights.grad).all()
    assert weights.grad[0, 0] < 0  # The old position earns the subsequent real move.
    assert not weights.grad[1].any()  # A zero-capacity target cannot earn the dividend.
    from stockagent.backtest.simulator import run_backtest_torch
    audited=run_backtest_torch(weights.detach(),torch.zeros(2,1),torch.ones(2,1,dtype=torch.bool),
        torch.zeros(2),0.,0.,long_only=False,portfolio_activation='pre_normalized',
        overnight_returns=x,execution_mode='tw_stock_context_futures_portfolio',
        day_trade_execution_initial_capital=1000.)
    assert audited.futures_margin_audit is not None
    assert audited.futures_contract_quantities_history is not None
    assert audited.futures_residual_contract_quantities_history is not None
    assert audited.futures_contract_quantities_history[:, :2].tolist()==[[5,0],[0,5]]


def test_monthly_and_aggregate_caps_intersect_without_double_spending_close_capacity():
    from stockagent.backtest.tw_futures_portfolio import _project_margin_position_limit_axes
    x=torch.zeros(2,m.MARGIN_MULTI_LIMIT_EXECUTION_WIDTH)
    x[:,m.SECOND_POSITION_GROUP]=torch.tensor([0.,1.])
    x[:,m.SECOND_POSITION_UNIT]=1.
    x[:,m.SECOND_POSITION_LIMIT]=torch.tensor([3.,6.])
    kwargs=dict(execution_row=x,position_group=torch.zeros(2,dtype=torch.long),
        position_units=torch.ones(2),group_limits=torch.tensor([8.,0.]),whole_contracts=True)
    q,failed,_=_project_margin_position_limit_axes(torch.tensor([4,4]),torch.tensor([0,0]),
        close_capacity=torch.tensor([100,100]),**kwargs)
    assert q.tolist()==[3,4] and not failed.any()
    q,failed,reduced=_project_margin_position_limit_axes(torch.tensor([5,5]),torch.tensor([5,5]),
        close_capacity=torch.tensor([1,1]),**kwargs)
    assert q.tolist()==[4,4]  # The monthly cap cannot consume another close lot.
    assert failed.tolist()==[True,False] and reduced


@pytest.mark.parametrize('whole', [True,False])
@pytest.mark.parametrize('sign', [1,-1])
def test_dated_lower_limit_preserves_old_inventory_but_forbids_additions_or_rolls(whole,sign):
    from stockagent.backtest.tw_futures_portfolio import _project_margin_position_limit_axes
    x=torch.zeros(2,m.MARGIN_GRANDFATHER_EXECUTION_WIDTH)
    x[:,m.SECOND_POSITION_GROUP]=torch.tensor([0.,1.])
    x[:,m.SECOND_POSITION_UNIT]=1.
    x[:,m.SECOND_POSITION_LIMIT]=100.
    x[:,m.POSITION_GRANDFATHER]=1.
    kwargs=dict(execution_row=x,position_group=torch.zeros(2,dtype=torch.long),
        position_units=torch.ones(2),group_limits=torch.tensor([3.,0.]),
        close_capacity=torch.tensor([100.,100.]),whole_contracts=whole)
    old=sign*torch.tensor([5.,0.])
    for wanted,expected in [([5,0],[5,0]),([6,1],[5,0]),([0,5],[0,0]),([-5,0],[0,0]),([2,1],[2,0])]:
        q,failed,_=_project_margin_position_limit_axes(sign*torch.tensor(wanted,dtype=torch.float32),old,**kwargs)
        assert q.tolist()==(sign*torch.tensor(expected)).tolist()
        assert not failed.any()
    # After reducing below the limit, a later decision may add within the cap.
    q,failed,_=_project_margin_position_limit_axes(sign*torch.tensor([2.,1.]),sign*torch.tensor([2.,0.]),**kwargs)
    assert q.tolist()==[sign*2,sign*1] and not failed.any()
    # Permission is source dated. Earlier schemas/false flags retain hard caps.
    x[:,m.POSITION_GRANDFATHER]=0.
    kwargs['close_capacity']=torch.zeros(2)
    q,failed,_=_project_margin_position_limit_axes(old,old,**kwargs)
    assert q.tolist()==old.tolist() and failed.any()


def test_schema3_adapter_keeps_distinct_limit_axes_and_rejects_missing_axis(tmp_path):
    import json
    panel,rows=rule_panel(tmp_path)
    rows=corporate_rules(rows)
    for i,r in enumerate(rows):r.update(second_position_group=f'contract-month-{i}',second_position_unit=1.,second_position_limit=5.)
    path=write_rules(tmp_path,panel,rows)
    manifest_path=path.with_name('manifest.json');manifest=json.loads(manifest_path.read_text())
    manifest['schema_version']=3;manifest_path.write_text(json.dumps(manifest))
    attached=m.attach_futures_margin_rules(panel,path)
    execution=attached.stock_context_futures_portfolio_daily.integer_execution
    assert execution.shape[-1]==36
    assert execution[:,0,m.SECOND_POSITION_LIMIT].tolist()==[5.,5.,5.]
    assert execution[:,0,m.SECOND_POSITION_GROUP].tolist()==[0.,0.,0.]
    assert execution[:,0,m.CARRY_SOURCE_SLOT].tolist()==[0,1,1]
    with pytest.raises(ValueError,match='both axes'):
        m.validate_margin_second_position_limit(pl.DataFrame(rows).drop('second_position_limit'))


def test_schema5_same_direction_limits_do_not_net_or_add_opposite_months():
    from stockagent.backtest.tw_futures_portfolio import _project_margin_position_limit_axes,_margin_position_slack
    x=torch.zeros(2,m.MARGIN_GRANDFATHER_EXECUTION_WIDTH)
    x[:,m.SECOND_POSITION_GROUP]=torch.tensor([0.,1.])
    x[:,m.SECOND_POSITION_UNIT]=1.;x[:,m.SECOND_POSITION_LIMIT]=100.
    group=torch.zeros(2,dtype=torch.long);unit=torch.ones(2);limit=torch.tensor([5.,0.])
    q,failed,_=_project_margin_position_limit_axes(torch.tensor([6.,-6.]),torch.tensor([5.,-5.]),
        execution_row=x,position_group=group,position_units=unit,group_limits=limit,
        close_capacity=torch.zeros(2),whole_contracts=True)
    assert q.tolist()==[5,-5] and not failed.any()
    assert _margin_position_slack(q,q,group,unit,limit,x[:,m.POSITION_GRANDFATHER])==0
    q,failed,_=_project_margin_position_limit_axes(torch.tensor([4.,4.]),torch.zeros(2),
        execution_row=x,position_group=group,position_units=unit,group_limits=limit,
        close_capacity=torch.full((2,),100.),whole_contracts=True)
    assert q.abs().sum()<=5 and not failed.any()


def corporate_rules(rows):
    for i, r in enumerate(rows):
        r.update(opening_time="08:45:00", carry_from_date=rows[i-1]['date'] if i else None,
            carry_from_physical_contract=r['physical_contract'] if i else '',
            carry_quantity_numerator=1, carry_quantity_denominator=1, carry_cash_twd=0.,
            carry_known_at=r['known_at'], carry_effective_at=r['effective_at'],
            carry_previous_value_twd=1000., carry_previous_initial_twd=100.,
            carry_previous_maintenance_twd=75., opening_contract_value_twd=1000.,
            settlement_contract_value_twd=1000., terminal_contract_value_twd=1000.,
            known_margin_contract_value_twd=1000., opening_tax_twd=1., terminal_tax_twd=2.,
            terminal_event="mark_only")
    return rows


def test_schema4_margin_base_is_distinct_from_deliverable_pnl_and_prior_features(tmp_path):
    import json
    panel,rows=rule_panel(tmp_path)
    rows=corporate_rules(rows)
    for r in rows:
        r.update(margin_kind='notional_rate',initial=.1,maintenance=.075,
            settlement_initial=.1,settlement_maintenance=.075,
            opening_margin_value_twd=800.,settlement_margin_value_twd=800.,known_margin_value_twd=800.,
            carry_previous_initial_twd=80.,carry_previous_maintenance_twd=60.,
            second_position_group='monthly',second_position_unit=1.,second_position_limit=20.)
    def attach():
        path=write_rules(tmp_path,panel,rows)
        meta=path.with_name('manifest.json');receipt=json.loads(meta.read_text())
        receipt['schema_version']=4;meta.write_text(json.dumps(receipt))
        return m.attach_futures_margin_rules(panel,path).stock_context_futures_portfolio_daily
    result=attach()
    assert result.margin_contract_version==4
    assert result.integer_execution[:,0,3:5].tolist()==[[1000.,1000.]]*3
    assert result.integer_execution[:,0,m.INITIAL].tolist()==[80.]*3
    assert result.integer_execution[:,0,m.MAINTENANCE].tolist()==[60.]*3
    np.testing.assert_allclose(result.candidate_features[:,0,-2:],[[.08,.75]]*3)
    for r in rows:r['opening_margin_value_twd']=900.
    changed=attach()
    assert changed.integer_execution[:,0,m.INITIAL].tolist()==[90.]*3
    np.testing.assert_array_equal(changed.candidate_features,result.candidate_features)
    with pytest.raises(ValueError,match='separate dated margin'):
        m.validate_margin_value_bases(pl.DataFrame(rows).drop('known_margin_value_twd'))
    rows[0]['known_margin_value_twd']=None
    with pytest.raises(ValueError,match='positive dated'):
        attach()


def test_schema5_preserves_dated_permissions_through_adapter_and_shared_account(tmp_path):
    import json
    panel,rows=rule_panel(tmp_path)
    rows=corporate_rules(rows)
    for row in rows:
        row.update(opening_margin_value_twd=1000.,settlement_margin_value_twd=1000.,
            known_margin_value_twd=1000.,second_position_group='monthly',second_position_unit=1.,
            second_position_limit=20.,position_grandfather_existing=True,
            second_position_grandfather_existing=False)
    path=write_rules(tmp_path,panel,rows)
    manifest_path=path.with_name('manifest.json');manifest=json.loads(manifest_path.read_text())
    manifest['schema_version']=5;manifest_path.write_text(json.dumps(manifest))
    result=m.attach_futures_margin_rules(panel,path).stock_context_futures_portfolio_daily
    assert result.integer_execution.shape[-1]==38 and result.margin_contract_version==5
    assert result.integer_execution[:,0,m.POSITION_GRANDFATHER].tolist()==[1.,1.,1.]
    assert result.integer_execution[:,0,m.SECOND_POSITION_GRANDFATHER].tolist()==[0.,0.,0.]
    rows[0]['position_grandfather_existing']=None
    with pytest.raises(ValueError,match='explicit dated boolean'):
        m.validate_margin_grandfather_rules(pl.DataFrame(rows))
    x=torch.zeros(1,1,38)
    x[:,:,:33]=corporate_tape()
    x[...,m.POSITION_LIMIT]=3
    x[...,m.SECOND_POSITION_UNIT]=1
    x[...,m.SECOND_POSITION_LIMIT]=100
    x[...,m.POSITION_GRANDFATHER]=1
    x[...,8]=0
    actual=run([[0.]],x,initial_quantities=torch.tensor([5]))
    assert actual.final_weights.tolist()==[5] and actual.final_equity_scale==1
    x[...,m.POSITION_GRANDFATHER]=0
    hard=run([[0.]],x,initial_quantities=torch.tensor([5]))
    assert hard.final_equity_scale<1e-6


def test_carry_rule_timestamps_and_source_identity_are_mandatory(tmp_path):
    panel, rows = rule_panel(tmp_path)
    rows = corporate_rules(rows)
    m.validate_margin_carry_rules(pl.DataFrame(rows))
    rows[1]['carry_known_at'] = '2026-01-03T08:45:01+08:00'
    with pytest.raises(ValueError, match="future/unannounced"):
        m.validate_margin_carry_rules(pl.DataFrame(rows))


def test_schema2_adapter_preserves_prior_value_tax_and_symbol_map(tmp_path):
    import json
    panel, rows = rule_panel(tmp_path)
    path = write_rules(tmp_path, panel, corporate_rules(rows))
    manifest_path = path.with_name('manifest.json')
    manifest = json.loads(manifest_path.read_text())
    manifest['schema_version'] = 2
    manifest_path.write_text(json.dumps(manifest))
    result = m.attach_futures_margin_rules(panel, path).stock_context_futures_portfolio_daily
    assert result.margin_contract_version == 2
    assert result.integer_execution.shape[-1] == 33
    assert result.integer_execution[:, 0, m.CARRY_SOURCE_SLOT].tolist() == [0, 1, 1]
    assert result.integer_execution[:, 0, 6].tolist() == [1, 1, 1]
    assert result.integer_execution[:, 0, 7].tolist() == [2, 2, 2]
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    attached = m.attach_futures_margin_rules(panel, path)
    dataset = CrossSectionalDataset(attached, np.arange(3), lookback=1,
                                   execution_mode="tw_stock_context_futures_portfolio")
    assert dataset_to_windowed_tensors(dataset).overnight_log_returns.shape[-1] == 33


@pytest.mark.parametrize("event,cash", [("market_close_required", False), ("cash_settlement", True)])
def test_last_trading_day_does_not_imply_cash_settlement(tmp_path, event, cash):
    import json
    from pathlib import Path
    panel, rows = rule_panel(tmp_path)
    daily = panel.stock_context_futures_portfolio_daily
    raw = pl.read_parquet(daily.source_path).with_columns(
        pl.when(pl.col("date") == rows[-1]["date"]).then(pl.lit("last_trade_date"))
        .otherwise(pl.lit("")).alias("liquidation_reason"), pl.lit(0.).alias("volume"))
    raw.write_parquet(daily.source_path)
    rows = corporate_rules(rows)
    rows[-1]["terminal_event"] = event
    path = write_rules(tmp_path, panel, rows)
    manifest_path = path.with_name('manifest.json')
    manifest = json.loads(manifest_path.read_text()); manifest['schema_version'] = 2
    manifest_path.write_text(json.dumps(manifest))
    result = m.attach_futures_margin_rules(panel, path).stock_context_futures_portfolio_daily
    assert result.integer_execution[-1, 0, 2] == 1
    assert bool(result.integer_execution[-1, 0, m.CASH_SETTLEMENT]) is cash
    assert result.integer_execution[-1, 0, m.TERMINAL_CAPACITY] == 0
    # The same source date has radically different obligations: only an
    # explicitly cash-settled product may retire inventory without a trade.
    executed = run([[.5]], torch.from_numpy(result.integer_execution[-1:, :1]),
                   initial_quantities=torch.tensor([5]))
    assert executed.residual_contract_quantities_history.item() == (0 if cash else 5)
    assert executed.default_reason_history.item() == (0 if cash else 4)


def mixed_corporate_source(tmp_path, product, *, change_current=False):
    """Synthetic fixture only: verify the adapter, not historical admission."""
    import json
    from pathlib import Path
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS
    panel, rows = rule_panel(tmp_path)
    all_rows=[]; all_rules=[]
    for code, slot, multiplier in [('TX', 1, 200.), (product, 2, 2100.)]:
        for i, r in enumerate(corporate_rules([dict(r) for r in rows])):
            adjusted = slot == 2
            deliverable_cash = 5000. if adjusted else 0.
            value = 100. * multiplier + deliverable_cash
            open_value = value + (10000. if change_current and adjusted and i == 1 else 0.)
            physical = code+':202603'
            row = dict.fromkeys(FUTURES_MODEL_FEATURE_COLUMNS, 0.)
            row.update(date=r['date'], product=code, symbol=f'TAIFEX_SLOT_{slot:04d}', tenor_rank=1,
                open=100. if open_value == value else 100.+10000./multiplier, close=100., settlement=100.,
                previous_settlement=100., volume=100., previous_volume=100., holding_log_return=0.,
                executable=True, must_liquidate=False, can_hold_overnight=True,
                same_contract_as_previous_session=i>0, contract_multiplier=multiplier,
                sinopac_network_fee_group='stock', underlying_symbol='S1' if adjusted else None,
                contract='202603', physical_contract=physical, asset_class='stock_future' if adjusted else 'index_future',
                liquidation_reason='')
            all_rows.append(row)
            r.update(physical_contract=physical, carry_from_physical_contract=physical if i else '',
                contract_multiplier=multiplier, opening_contract_value_twd=open_value,
                settlement_contract_value_twd=value, terminal_contract_value_twd=value,
                known_margin_contract_value_twd=value, carry_previous_value_twd=value,
                opening_tax_twd=75. if change_current and adjusted and i==1 else 7.,
                terminal_tax_twd=7., known_tax_twd=5., position_group=code,
                upper_limit=110., lower_limit=90.)
            all_rules.append(r)
    source=Path(panel.stock_context_futures_portfolio_daily.source_path)
    pl.DataFrame(all_rows).write_parquet(source)
    source.with_name('manifest.json').write_text(json.dumps(dict(contract_version=4, feature_contract_version=3,
        fixed_model_output_slots=1936,outputs={'continuous_daily':{'sha256':sha256_file(source)}})))
    rules=write_rules(tmp_path,panel,all_rules)
    proof=rules.with_name('manifest.json');payload=json.loads(proof.read_text());payload['schema_version']=2
    proof.write_text(json.dumps(payload))
    return panel,source,rules


@pytest.mark.parametrize('product',['DL2','BRF','TGF','CPF','GBF'])
def test_dated_all_product_adapter_uses_full_value_and_product_tax(tmp_path, product):
    from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
    panel, path, rules = mixed_corporate_source(tmp_path, product)
    panel=attach_stock_context_futures_portfolio_daily(panel,path,
        fee_per_side_twd_by_group={'stock':40.},integer_contracts=True,
        expiry_settlement_valuation=True, margin_rules_path=rules,max_volume_participation=.5)
    base=panel.stock_context_futures_portfolio_daily
    assert base.integer_execution[1,1,3] == 215000.  # includes a synthetic deliverable cash component
    assert base.integer_execution[1,1,6] == 7.
    assert base.candidate_features[1,1,-1] == 215000.+80.+10.  # known tax, not current tax
    result=m.attach_futures_margin_rules(panel,rules).stock_context_futures_portfolio_daily
    assert result.integer_execution.shape[-1] == 33
    assert result.candidate_mask[1,1]
    # Unchanged inventory retains its exact physical origin on the next row.
    assert result.integer_execution[1,1,m.CARRY_SOURCE_SLOT] == 2


def terminal_component_fixture(tmp_path, *, future_rights=False, official=None):
    """Synthetic official receipt and independently dated deliverable terms."""
    import json
    from datetime import date
    from downloader.artifact_io import sha256_file
    day=date(2012,3,21);physical='AA1:201203'
    frame=pl.DataFrame([dict(date=day,product='AA1',contract='201203',
        physical_contract=physical,contract_multiplier=2000.)])
    value=2010.+7.
    row=dict(date=day,physical_contract=physical,contract_multiplier=2000.,
        opening_contract_value_twd=value,settlement_contract_value_twd=value,
        terminal_contract_value_twd=value,known_margin_contract_value_twd=value,
        opening_tax_twd=1.,terminal_tax_twd=1.,known_tax_twd=1.,terminal_event='cash_settlement')
    final=tmp_path/'final';final.mkdir()
    raw=final/'official.html';raw.write_text('synthetic test evidence only')
    source=final/'final.parquet'
    pl.DataFrame([dict(settlement_date=day,product='AA1',contract='201203',
        final_settlement_price=1.005,final_settlement_value=official,
        reported_date_role='final_settlement_day',settlement_method='cash_settlement',
        source_sha256=sha256_file(raw))],schema_overrides={'final_settlement_value':pl.Float64}).write_parquet(source)
    (final/'manifest.json').write_text(json.dumps(dict(schema_version=2,status='complete',
        requires_product_specific_settlement_clock=True,
        outputs={'futures_final_settlement_history':{'sha256':sha256_file(source)}},
        receipts=[dict(path=raw.name,sha256=sha256_file(raw))])))
    terms=pl.DataFrame([dict(product='AA1',contract='201203',effective_date='2012-03-01',
        valid_until_exclusive='2012-03-22',known_at='2012-02-20T23:59:59+08:00',
        contract_multiplier=2000.,deliverable_cash_twd=0. if future_rights else 7.,
        subscription_rights_at_final_settlement=future_rights,source_content_sha256s=['a'*64],
        point_in_time_verified=True)])
    rights=frame.select('date','product','contract').with_columns(pl.lit(7.).alias('rights_twd'),
        pl.lit('a'*64).alias('notice_content_sha256'),pl.lit(day).alias('fixing_date'))
    return frame,pl.DataFrame([row]),source,terms,rights


@pytest.mark.parametrize('future_rights',[False,True])
def test_account_recomputes_missing_adjusted_full_value_without_filling_original_cell(tmp_path,future_rights):
    frame,rules,source,terms,rights=terminal_component_fixture(tmp_path,future_rights=future_rights)
    with pytest.raises(ValueError,match='official product settlement'):
        m.corporate_margin_base_values(frame,rules,source)
    attached=m.corporate_margin_base_values(frame,rules,source,terminal_component_terms=terms,
        terminal_subscription_values=rights if future_rights else None)
    assert attached['_rule_terminal_contract_value_twd'][0]==2017.
    assert pl.read_parquet(source)['final_settlement_value'][0] is None
    if future_rights:
        with pytest.raises(ValueError,match='official product settlement'):
            m.corporate_margin_base_values(frame,rules,source,terminal_component_terms=terms)
    with pytest.raises(ValueError,match='official product settlement'):
        m.corporate_margin_base_values(frame,rules.with_columns(
            pl.lit(2018.).alias('terminal_contract_value_twd')),source,
            terminal_component_terms=terms,terminal_subscription_values=rights if future_rights else None)


def test_exchange_full_value_stays_authoritative_over_derived_components(tmp_path):
    frame,rules,source,terms,rights=terminal_component_fixture(tmp_path,official=2018.)
    with pytest.raises(ValueError,match='official product settlement'):
        m.corporate_margin_base_values(frame,rules,source,terminal_component_terms=terms)
    result=m.corporate_margin_base_values(frame,rules.with_columns(
        pl.lit(2018.).alias('terminal_contract_value_twd')),source,terminal_component_terms=terms)
    assert result['_rule_terminal_contract_value_twd'][0]==2018.


def test_component_release_rejects_candidates_unreceipted_values_and_future_fixing(tmp_path):
    from downloader.artifact_io import sha256_file
    frame,rules,source,terms,rights=terminal_component_fixture(tmp_path,future_rights=True)
    path=tmp_path/'rules.parquet';term_path=tmp_path/'terms.parquet';right_path=tmp_path/'rights.parquet'
    def proof():
        terms.write_parquet(term_path);rights.write_parquet(right_path)
        term=dict(path=term_path.name,sha256=sha256_file(term_path))
        right=dict(path=right_path.name,sha256=sha256_file(right_path))
        return dict(sources=[term,right],adjusted_terminal_components=dict(
            status='admitted',corporate_terms=term,subscription_values=right))
    manifest=proof()
    assert m.load_margin_terminal_components(path,manifest)[0].equals(terms)
    manifest['sources']=[]
    with pytest.raises(ValueError,match='release receipts'):
        m.load_margin_terminal_components(path,manifest)
    terms=terms.with_columns(pl.lit(False).alias('point_in_time_verified'))
    with pytest.raises(ValueError,match='candidate corporate'):
        m.load_margin_terminal_components(path,proof())
    terms=terms.with_columns(pl.lit(True).alias('point_in_time_verified'))
    rights=rights.with_columns((pl.col('date')+pl.duration(days=1)).alias('fixing_date'))
    with pytest.raises(ValueError,match='known by terminal'):
        m.load_margin_terminal_components(path,proof())


def test_schema6_terminal_components_reach_the_canonical_margin_execution(tmp_path):
    import json
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
    panel,source,rules_path=mixed_corporate_source(tmp_path,'AA1')
    rows=pl.read_parquet(source);last=rows['date'].max()
    ending=(pl.col('product')=='AA1') & (pl.col('date')==last)
    rows=rows.with_columns(ending.alias('must_liquidate'),(~ending).alias('can_hold_overnight'),
        pl.when(ending).then(pl.lit('last_trade_date')).otherwise(pl.lit('')).alias('liquidation_reason'))
    rows.write_parquet(source)
    daily_manifest=json.loads(source.with_name('manifest.json').read_text())
    daily_manifest['outputs']['continuous_daily']['sha256']=sha256_file(source)
    source.with_name('manifest.json').write_text(json.dumps(daily_manifest))
    rules=pl.read_parquet(rules_path).with_columns(
        pl.col('opening_contract_value_twd').alias('opening_margin_value_twd'),
        pl.col('settlement_contract_value_twd').alias('settlement_margin_value_twd'),
        pl.col('known_margin_contract_value_twd').alias('known_margin_value_twd'),
        pl.col('position_group').alias('second_position_group'),pl.lit(1.).alias('second_position_unit'),
        pl.lit(100.).alias('second_position_limit'),pl.lit(False).alias('position_grandfather_existing'),
        pl.lit(False).alias('second_position_grandfather_existing'),
        pl.when((pl.col('physical_contract')=='AA1:202603') & (pl.col('date')==last))
            .then(pl.lit('cash_settlement')).otherwise(pl.col('terminal_event')).alias('terminal_event'))
    rules.write_parquet(rules_path)
    proof=json.loads(rules_path.with_name('manifest.json').read_text())
    proof.update(schema_version=6,source_daily_sha256=sha256_file(source))
    proof['outputs']['rules']['sha256']=sha256_file(rules_path)
    terms=pl.DataFrame([dict(product='AA1',contract='202603',effective_date='2026-01-01',
        valid_until_exclusive=None,known_at='2025-12-20T23:59:59+08:00',contract_multiplier=2100.,
        deliverable_cash_twd=5000.,subscription_rights_at_final_settlement=False,
        source_content_sha256s=['a'*64],point_in_time_verified=True)],
        schema_overrides={'valid_until_exclusive':pl.String})
    term_path=rules_path.parent/'terminal_terms.parquet';terms.write_parquet(term_path)
    entry=dict(path=term_path.name,sha256=sha256_file(term_path))
    proof['sources'].append(entry)
    proof['adjusted_terminal_components']=dict(status='admitted',corporate_terms=entry)
    rules_path.with_name('manifest.json').write_text(json.dumps(proof))
    fixture=tmp_path/'synthetic_final';fixture.mkdir()
    _,_,final_path,_,_=terminal_component_fixture(fixture)
    final=pl.read_parquet(final_path).with_columns(pl.lit(last).alias('settlement_date'),
        pl.lit('202603').alias('contract'),pl.lit(100.).alias('final_settlement_price'))
    final.write_parquet(final_path)
    receipt=json.loads(final_path.with_name('manifest.json').read_text())
    receipt['outputs']['futures_final_settlement_history']['sha256']=sha256_file(final_path)
    final_path.with_name('manifest.json').write_text(json.dumps(receipt))
    panel=attach_stock_context_futures_portfolio_daily(panel,source,
        fee_per_side_twd_by_group={'stock':40.},integer_contracts=True,
        expiry_settlement_valuation=True,margin_rules_path=rules_path,final_settlement_path=final_path,
        max_volume_participation=.5)
    result=m.attach_futures_margin_rules(panel,rules_path).stock_context_futures_portfolio_daily
    assert result.margin_contract_version==6 and result.integer_execution.shape[-1]==38
    assert result.integer_execution[-1,1,m.CASH_SETTLEMENT]==1
    assert result.integer_execution[-1,1,m.TERMINAL_MARK]==215000.


def test_wide_layout_keeps_the_2505th_physical_origin_and_rejects_legacy_layout(tmp_path):
    import json
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
    panel,path,rules=mixed_corporate_source(tmp_path,'GBF')
    kwargs=dict(fee_per_side_twd_by_group={'stock':40.},integer_contracts=True,
        expiry_settlement_valuation=True,margin_rules_path=rules,max_volume_participation=.5)
    with pytest.raises(ValueError,match='contract version'):
        attach_stock_context_futures_portfolio_daily(panel,path,futures_slot_count=2560,**kwargs)
    frame=pl.read_parquet(path).with_columns(pl.when(pl.col('product')=='GBF')
        .then(pl.lit('TAIFEX_SLOT_2505')).otherwise(pl.col('symbol')).alias('symbol'))
    frame.write_parquet(path)
    p=path.with_name('manifest.json');proof=json.loads(p.read_text())
    proof.update(contract_version=5,fixed_model_output_slots=2560)
    proof['outputs']['continuous_daily']['sha256']=sha256_file(path);p.write_text(json.dumps(proof))
    p=rules.with_name('manifest.json');proof=json.loads(p.read_text())
    proof['source_daily_sha256']=sha256_file(path);p.write_text(json.dumps(proof))
    with pytest.raises(ValueError,match='contract version'):
        attach_stock_context_futures_portfolio_daily(panel,path,**kwargs)
    panel=attach_stock_context_futures_portfolio_daily(panel,path,futures_slot_count=2560,**kwargs)
    result=m.attach_futures_margin_rules(panel,rules).stock_context_futures_portfolio_daily
    assert len(result.symbols)==2560 and result.candidate_mask[1,2504]
    assert result.integer_execution[1,2504,m.CARRY_SOURCE_SLOT]==2505
    assert result.integer_execution.shape[1:]==(2560,33)


def test_dated_corporate_current_price_and_tax_cannot_enter_model_context(tmp_path):
    from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
    result=[]
    for changed in (False,True):
        root=tmp_path/str(changed);root.mkdir()
        panel,path,rules=mixed_corporate_source(root,'DL2',change_current=changed)
        panel=attach_stock_context_futures_portfolio_daily(panel,path,fee_per_side_twd_by_group={'stock':40.},
            integer_contracts=True,expiry_settlement_valuation=True,margin_rules_path=rules,max_volume_participation=.5)
        result.append(m.attach_futures_margin_rules(panel,rules).stock_context_futures_portfolio_daily)
    np.testing.assert_array_equal(result[0].candidate_features[:2],result[1].candidate_features[:2])
    np.testing.assert_array_equal(result[0].candidate_mask[:2],result[1].candidate_mask[:2])
    assert result[0].integer_execution[1,1,3] != result[1].integer_execution[1,1,3]
