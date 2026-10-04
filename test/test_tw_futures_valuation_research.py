from datetime import date, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_futures_valuation_research import (
    VALUATION_RESEARCH_COLUMNS, prepare_frozen_value_physical_history,
    attach_research_valuation_provenance, validate_research_valuation_rows,
)
from stockagent.data.tw_futures_execution_terms import SPEC_FIELDS
from test_tw_futures_execution_terms import specification, CORPORATE_SCHEMA


def case():
    days = [date(2026, 1, 5) + timedelta(days=i) for i in range(4)]
    physical = pl.DataFrame([dict(date=day, product='AAF', contract='202603',
        physical_contract='AAF:202603', physical_instance='AAF:202603#g0',
        first_observed_date=days[0], last_observed_date=days[-1], official_expiry=None,
        cash_settlement=False, valuation_price=100. if i == 0 else 110. if i == 3 else None,
        daily_mark=100. if i == 0 else 110. if i == 3 else None,
        official_open=None, official_close=None, outright_volume=0., spread_leg_volume=0.)
        for i, day in enumerate(days)]).with_columns(pl.col('official_expiry').cast(pl.Date))
    specs = pl.DataFrame([specification('AAF', contract_multiplier=2000.)], schema=SPEC_FIELDS)
    policy = dict(contract='frozen_contract_value_research_v1', research_only=True,
        financial_point_in_time_verified=False, products=['AAF'], physical_instances=['AAF:202603#g0'],
        continuation_transfers=[], terminal_cash_conversion_authorized=False,
        **{c:'a'*64 for c in ('source_manifest_sha256','rule_delta_manifest_sha256',
                             'prior_replay_manifest_sha256','prior_gap_worklist_sha256')})
    return physical, physical.select('date'), pl.DataFrame(schema=CORPORATE_SCHEMA), specs, policy


def apply(physical, calendar, terms, specs, policy):
    return prepare_frozen_value_physical_history(physical, calendar, terms=terms,
        specifications=specs, policy=policy)


def test_freezes_full_value_without_modifying_any_source_quote_or_volume():
    physical, *args = case()
    result, audit = apply(physical, *args)
    assert result['valuation_price'].to_list() == [100., 100., 100., 110.]
    assert audit['valuation_research_value_twd'].to_list() == [200000., 200000.]
    assert audit['valuation_research_seed_date'].to_list() == [date(2026, 1, 5)] * 2
    for c in physical.columns:
        if c != 'valuation_price':
            assert result[c].equals(physical[c])
    assert physical['valuation_price'].null_count() == 2


@pytest.mark.parametrize('problem', ['missing_seed', 'new_generation', 'missing_previous_session', 'printed_day'])
def test_missing_or_different_owner_evidence_is_not_invented(problem):
    physical, calendar, terms, specs, policy = case()
    if problem == 'missing_seed':
        physical = physical.with_columns(pl.when(pl.col('date') == date(2026, 1, 5))
            .then(None).otherwise(pl.col('valuation_price')).alias('valuation_price'))
    elif problem == 'new_generation':
        physical = physical.with_columns(pl.when(pl.col('date') > date(2026, 1, 5))
            .then(pl.lit('AAF:202603#g1')).otherwise(pl.col('physical_instance')).alias('physical_instance'))
        policy['physical_instances'].append('AAF:202603#g1')
    elif problem == 'missing_previous_session':
        physical = physical.filter(pl.col('date') != date(2026, 1, 6))
    else:
        physical = physical.with_columns(pl.when(pl.col('date') == date(2026, 1, 6))
            .then(1.).otherwise(pl.col('outright_volume')).alias('outright_volume'))
    result, audit = apply(physical, calendar, terms, specs, policy)
    assert audit.is_empty()
    assert result['valuation_price'].equals(physical['valuation_price'])


def test_future_actual_mark_cannot_change_earlier_estimates():
    physical, *args = case()
    first, _ = apply(physical, *args)
    physical = physical.with_columns(pl.when(pl.col('date') == date(2026, 1, 8))
        .then(9999.).otherwise(pl.col('valuation_price')).alias('valuation_price'))
    second, _ = apply(physical, *args)
    assert first.filter(pl.col('date') < date(2026, 1, 8)).equals(
        second.filter(pl.col('date') < date(2026, 1, 8)))


def test_verified_cash_and_units_preserve_full_value_across_missing_destination_days():
    physical, calendar, terms, specs, policy = case()
    physical = physical.with_columns(pl.when(pl.col('date') >= date(2026, 1, 7))
        .then(pl.lit('AA1')).otherwise(pl.col('product')).alias('product'))
    physical = physical.with_columns((pl.col('product') + ':202603').alias('physical_contract'),
        (pl.col('product') + ':202603#g0').alias('physical_instance'))
    physical = physical.filter(pl.col('date') != date(2026, 1, 7))
    terms = pl.DataFrame([dict(product='AA1', contract='202603', effective_date='2026-01-07',
        valid_until_exclusive=None, from_product='AAF', contract_multiplier=1600., deliverable_cash_twd=4000.,
        fixed_subscription_rights_twd=0., subscription_rights_at_final_settlement=False,
        known_at='2026-01-04T12:00:00+08:00', source_content_sha256s=['b'*64],
        effective_at='2026-01-07T08:45:00+08:00', equity_cash_credit_twd=6000., has_equity_credit_fields=True,
        carry_quantity_numerator=1, carry_quantity_denominator=1)], schema=CORPORATE_SCHEMA)
    specs = pl.concat([specs, pl.DataFrame([specification('AA1')],schema=SPEC_FIELDS)])
    policy['products'].append('AA1')
    policy['continuation_transfers'] = [dict(date='2026-01-06',product='AAF',contract='202603',
        physical_contract='AAF:202603#g0',corporate_boundary='2026-01-07',corporate_transfer_target='AA1')]
    result, audit = apply(physical, calendar, terms, specs, policy)
    new = result.filter(pl.col('date') == date(2026, 1, 7)).row(0,named=True)
    assert new['valuation_price'] == (200000.-6000.-4000.)/1600.
    assert new['valuation_research_value_twd'] == 194000.
    assert new['valuation_research_owner_extension']
    assert new['official_open'] is None and new['daily_mark'] is None and new['outright_volume'] is None
    assert result.filter(pl.col('date') == date(2026, 1, 8))['valuation_price'][0] == 110.
    assert audit.height == 2


def test_actual_final_mark_has_priority_and_missing_terminal_is_not_estimated():
    physical, *args = case()
    physical = physical.with_columns(pl.col('date').eq(date(2026, 1, 7)).alias('cash_settlement'))
    result, audit = apply(physical, *args)
    assert result.filter(pl.col('date') == date(2026, 1, 7))['valuation_price'][0] is None
    assert audit.height == 1


def test_opening_reference_estimates_are_disclosed_on_actual_resumption_day():
    physical, *args = case()
    result, _ = apply(physical, *args)
    frame = result.rename({'physical_instance':'_id'}).drop('physical_contract').rename({'_id':'physical_contract'})
    rules = frame.select('date','physical_contract').with_columns(
        pl.col('date').shift(1).alias('carry_from_date'),
        pl.when(pl.col('date') == date(2026, 1, 5)).then(pl.lit(''))
            .otherwise(pl.col('physical_contract')).alias('carry_from_physical_contract'))
    attached = attach_research_valuation_provenance(frame, rules)
    assert attached['valuation_research_applied'].to_list() == [False,True,True,True]
    assert attached['valuation_research_method'][-1] == 'opening_reference_from_frozen_inventory_value'
    validate_research_valuation_rows(attached)
    with pytest.raises(ValueError, match='seed'):
        validate_research_valuation_rows(attached.with_columns(pl.col('date').alias('valuation_research_seed_date')))


def test_opt_in_hook_preserves_the_entire_legacy_financial_kernel():
    import ast, copy, hashlib, inspect
    from scripts.prepare_tw_futures_margin_training import _compile_accounting, _calculation_identity, _verify_valuation_research_extension
    tree = ast.parse(inspect.getsource(_compile_accounting)); function = tree.body[0]
    function.args.kwonlyargs.pop(); function.args.kw_defaults.pop()
    function.body = [node for node in function.body if not (
        isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name) and node.test.left.id == 'valuation_research_policy')]
    baseline = ast.unparse(tree) + '\n'
    previous = copy.deepcopy(_calculation_identity())
    previous['functions']['_compile_accounting'] = hashlib.sha256(baseline.encode()).hexdigest()
    policy = dict(baseline_calculation=previous, baseline_accounting_function_source=baseline)
    proof = _verify_valuation_research_extension(policy['baseline_calculation'], _calculation_identity(), policy)
    assert proof['canonical_nonresearch_path_unchanged']


def test_research_valuation_changes_the_checkpoint_execution_fingerprint():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract
    cfg=load_config('configs/experiment_baseline.yaml')
    cfg.trading.execution_mode='tw_stock_context_futures_portfolio'
    cfg.trading.tw_futures_portfolio_capital_basis='initial_margin'
    official=_trading_checkpoint_contract(cfg)
    assert official['futures_margin_contract']['marking']=='official_settlement_then_next_open_gap'
    assert 'valuation_research_contract' not in official['futures_margin_contract']
    cfg.trading.tw_futures_portfolio_valuation_research_contract='frozen_contract_value_research_v1'
    research=_trading_checkpoint_contract(cfg)
    assert research!=official
    assert research['futures_margin_contract']['marking']=='frozen_contract_value_research_v1'


@pytest.mark.parametrize('authorized', [False, True])
def test_research_terminal_conversion_requires_authorization_and_keeps_observations(authorized):
    from stockagent.data.tw_futures_valuation_research import apply_research_terminal_cash_conversions
    physical, calendar, terms, specs, policy = case()
    physical, _ = apply(physical, calendar, terms, specs, policy)
    frame = physical.drop('physical_contract').rename({'physical_instance':'physical_contract'}).with_columns(
        pl.lit('zero_open_interest_delisting').alias('lifetime_status'),pl.lit(0.).alias('open_interest'))
    rules = frame.select('date','physical_contract',*VALUATION_RESEARCH_COLUMNS).with_columns(
        pl.col('date').shift(1).alias('carry_from_date'),
        pl.when(pl.col('date') == date(2026,1,5)).then(pl.lit(''))
            .otherwise(pl.col('physical_contract')).alias('carry_from_physical_contract'),
        pl.lit('mark_only').alias('terminal_event'),pl.lit(220000.).alias('settlement_contract_value_twd'),
        pl.lit(220000.).alias('terminal_contract_value_twd'),pl.lit(10.).alias('terminal_tax_twd'))
    flags = frame.select('date','physical_contract').with_columns(
        pl.col('date').eq(date(2026,1,8)).alias('lost_inventory_continuation'))
    policy['terminal_cash_conversion_authorized'] = authorized
    updated, chosen, blockers, audit = apply_research_terminal_cash_conversions(frame,rules,flags,policy)
    if not authorized:
        assert updated.equals(frame) and chosen.equals(rules) and blockers.equals(flags) and audit.is_empty()
    else:
        assert audit.height == 1
        assert chosen['terminal_event'][-1] == 'research_cash_conversion'
        assert chosen['terminal_contract_value_twd'][-1] == 220000.
        assert chosen['terminal_tax_twd'][-1] == 0.
        assert not blockers['lost_inventory_continuation'].any()
        assert not updated['cash_settlement'].any()
        for c in ('official_open','official_close','outright_volume','valuation_price'):
            assert updated[c].equals(frame[c])
        validate_research_valuation_rows(chosen)


@pytest.mark.parametrize('with_official_final', [False, True])
@pytest.mark.parametrize('closed_effective_day', [False, True])
def test_unobserved_converted_month_uses_its_own_final_or_disclosed_research_cash_conversion(with_official_final, closed_effective_day):
    physical, calendar, _, specs, policy = case()
    physical = physical.filter(pl.col('date') <= date(2026,1,6))
    if closed_effective_day:
        calendar = calendar.filter(pl.col('date') != date(2026,1,7))
    cf = pl.DataFrame([dict(product='AA1',from_product='AAF',effective_date='2026-01-07',
        contract_months=['202603'],source_content_sha256='b'*64)])
    terms = pl.DataFrame([dict(product='AA1',contract='202603',effective_date='2026-01-07',
        valid_until_exclusive=None,from_product='AAF',contract_multiplier=1600.,deliverable_cash_twd=4000.,
        fixed_subscription_rights_twd=0.,subscription_rights_at_final_settlement=False,
        known_at='2026-01-04T12:00:00+08:00',source_content_sha256s=['b'*64],
        effective_at='2026-01-07T08:45:00+08:00',equity_cash_credit_twd=6000.,has_equity_credit_fields=True,
        carry_quantity_numerator=1,carry_quantity_denominator=1)],schema=CORPORATE_SCHEMA)
    specs = pl.concat([specs,pl.DataFrame([specification('AA1')],schema=SPEC_FIELDS)])
    policy['terminal_cash_conversion_authorized'] = True
    policy['continuation_transfers'] = [dict(date='2026-01-06',product='AAF',contract='202603',
        physical_contract='AAF:202603#g0',corporate_boundary='2026-01-07',corporate_transfer_target='AA1',
        source_open_interest=0.)]
    finals = pl.DataFrame([dict(product='AA1',contract='202603',settlement_date=date(2026,1,8),
        final_settlement_price=123.4,final_settlement_value=None)]) if with_official_final else None
    result, audit = prepare_frozen_value_physical_history(physical,calendar,terms=terms,specifications=specs,
        policy=policy,corporate_candidates=cf,final_settlements=finals)
    converted = result.filter(pl.col('product') == 'AA1')
    if not (with_official_final and closed_effective_day):
        assert converted['valuation_price'][0] == (200000.-6000.-4000.)/1600.
    assert converted['official_open'].null_count() == converted.height
    assert converted['outright_volume'].null_count() == converted.height
    if with_official_final:
        assert converted.height == (1 if closed_effective_day else 2) and converted['valuation_price'][-1] == 123.4
        assert converted['cash_settlement'][-1]
        assert not converted['valuation_research_terminal_assumption'].any()
    else:
        assert converted.height == 1 and converted['valuation_research_terminal_assumption'][0]
        assert not converted['cash_settlement'].any()


@pytest.mark.parametrize('problem', [None, 'parent_hash', 'source', 'rule_bytes', 'policy_bytes'])
def test_cached_research_publication_owns_the_latest_complete_product_repair(tmp_path, monkeypatch, problem):
    """A local repair replaces its product, while other accepted products survive."""
    import scripts.prepare_tw_futures_margin_training as builder
    from downloader.artifact_io import sha256_file
    source, delta = tmp_path/'source', tmp_path/'delta'
    source.mkdir(); delta.mkdir()
    (source/'source_manifest.json').write_text('{}')
    (delta/'manifest.json').write_text('{"delta":true}')
    identity = {'files':{'core':'unchanged'},'functions':{'financial':'unchanged'}}
    monkeypatch.setattr(builder,'_calculation_identity',lambda:identity)

    def make(name, products, marker, parent=None):
        root = tmp_path/name; root.mkdir()
        daily = pl.DataFrame([dict(date=date(2026,1,5),physical_contract=p+':202601',product=p,marker=marker)
            for p in products])
        original = daily.head(0)
        policy = case()[-1]
        policy.update(source_manifest_sha256=sha256_file(source/'source_manifest.json'),
            rule_delta_manifest_sha256=sha256_file(delta/'manifest.json'))
        path=root/'policy.json'; path.write_text(json.dumps(policy))
        names=['frame.parquet','compiled_rules.parquet','contract_day_blockers.parquet','execution_dependencies.parquet']
        for filename in names:daily.write_parquet(root/filename)
        for filename in ['context_only_original_rules.parquet','context_only_suppressed_carries.parquet']:
            original.write_parquet(root/filename); names.append(filename)
        manifest=dict(source_manifest_sha256=policy['source_manifest_sha256'],
            delta_manifest_sha256=policy['rule_delta_manifest_sha256'],calculation=identity,
            end='2026-01-05',slots=2816,affected_products=products,
            valuation_research_policy=dict(path=str(path),sha256=sha256_file(path),implementation_sha256='a'*64),
            previous_replay=str(parent) if parent else None,
            previous_replay_manifest_sha256=sha256_file(parent/'manifest.json') if parent else None,
            outputs={filename:dict(sha256=sha256_file(root/filename)) for filename in names})
        (root/'manifest.json').write_text(json.dumps(manifest))
        return root

    parent=make('parent',['AAF','BBF'],1)
    child=make('child',['AAF'],2,parent)
    if problem=='parent_hash':
        path=child/'manifest.json'; m=json.loads(path.read_text()); m['previous_replay_manifest_sha256']='f'*64
        path.write_text(json.dumps(m))
    elif problem=='source':
        path=child/'manifest.json'; m=json.loads(path.read_text()); m['source_manifest_sha256']='f'*64
        path.write_text(json.dumps(m))
    elif problem=='rule_bytes':
        (child/'compiled_rules.parquet').write_bytes(b'changed')
    elif problem=='policy_bytes':
        (child/'policy.json').write_text('{}')
    if problem:
        with pytest.raises(ValueError):
            builder._read_research_replay_components(child,source,delta,end=date(2026,1,5),slots=2816)
    else:
        cached,receipts,policies=builder._read_research_replay_components(
            child,source,delta,end=date(2026,1,5),slots=2816)
        assert len(receipts)==len(policies)==2
        for data in cached.values():
            assert data.sort('product')['marker'].to_list()==[2,1]
            assert set(data['product'])=={'AAF','BBF'}
