"""Unobserved limit stages must never become invented executable limits."""
from datetime import date, timedelta
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_execution_terms import bind_product_specifications, compile_execution_terms
from stockagent.data.tw_futures_margin_release import load_dated_product_specifications
from stockagent.data.tw_futures_price_limits import (
    STAGED_INNER_BAND_CONTRACT, STAGED_LIMIT_FIELDS, etf_ratio_band,
    STAGED_FIXED_INNER_BAND_CONTRACT, fixed_ratio_band, price_limit_release_contract,
    STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT,
    price_limit_permissions, validate_price_limit_rules,
)
from test_tw_futures_execution_terms import inputs, specification


def staged_spec(**updates):
    return specification(effective_date=date(2026,7,6),tick_kind='etf_future',tick_size=None,
        limit_kind='staged_ratio',limit_up_ratio=None,limit_down_ratio=None,
        carry_split_numerator=1,carry_split_denominator=1,
        limit_stage_up_ratios=[1.07,1.10,1.15],limit_stage_down_ratios=[.93,.90,.85],**updates)


def certificate(reference=100., bond=False):
    up=[1.05,1.10,1.15] if bond else [1.07,1.10,1.15]
    down=[.95,.90,.85] if bond else [.93,.90,.85]
    dates=np.array([np.datetime64('2026-07-06')])
    return pl.DataFrame(dict(date=[date(2026,7,6)],upper_limit=[None],lower_limit=[None],
        price_limit_permission_contract=[STAGED_INNER_BAND_CONTRACT],
        price_limit_reference_quote=[reference],price_limit_stage_up_ratios=[up],
        price_limit_stage_down_ratios=[down],
        price_limit_inner_upper=etf_ratio_band([reference],[up[0]],dates,upper=True),
        price_limit_inner_lower=etf_ratio_band([reference],[down[0]],dates,upper=False)),
        schema_overrides={'upper_limit':pl.Float64,'lower_limit':pl.Float64,**STAGED_LIMIT_FIELDS})


MANIFEST=dict(research_only=True,price_limit_permission_contract=STAGED_INNER_BAND_CONTRACT)


@pytest.mark.parametrize('bond',[False,True])
@pytest.mark.parametrize('reference',[10.,47.63,49.99,50.,100.])
def test_inner_band_permissions_are_safe_for_every_actual_stage(reference,bond):
    rules=certificate(reference,bond)
    validate_price_limit_rules(rules,MANIFEST)
    lo=rules['price_limit_inner_lower'][0];hi=rules['price_limit_inner_upper'][0]
    quotes=np.array([lo-1.,lo,(lo+hi)/2,hi,hi+1.,np.nan])
    expanded=pl.concat([rules]*len(quotes))
    buy,sell=price_limit_permissions(expanded,quotes)
    np.testing.assert_array_equal(buy,[False,False,True,False,False,False])
    np.testing.assert_array_equal(sell,buy)
    # Independent one-sided states may be at different stages. No permitted
    # action may reach any possible upper or lower exchange limit.
    for up in rules['price_limit_stage_up_ratios'][0]:
        for down in rules['price_limit_stage_down_ratios'][0]:
            dates=expanded['date'].to_numpy()
            upper=etf_ratio_band(np.full(len(quotes),reference),up,dates,upper=True)
            lower=etf_ratio_band(np.full(len(quotes),reference),down,dates,upper=False)
            assert np.all(quotes[buy]<upper[buy]) and np.all(quotes[sell]>lower[sell])
    assert rules['upper_limit'].null_count()==rules['lower_limit'].null_count()==1


def test_legacy_static_rules_keep_directional_boundary_behavior():
    q=np.array([90.,100.,110.,np.nan])
    f=pl.DataFrame(dict(upper_limit=[110.]*len(q),lower_limit=[90.]*len(q)))
    validate_price_limit_rules(f,{})
    buy,sell=price_limit_permissions(f,q)
    np.testing.assert_array_equal(buy,q<110.)
    np.testing.assert_array_equal(sell,q>90.)


@pytest.mark.parametrize('quote',[89.,111.,-1.,0.,-np.inf,np.inf,np.nan])
def test_static_quote_outside_either_boundary_has_no_directional_permission(quote):
    rules=pl.DataFrame(dict(upper_limit=[110.],lower_limit=[90.]))
    buy,sell=price_limit_permissions(rules,[quote])
    assert not buy[0] and not sell[0]


@pytest.mark.parametrize('boundary',['lower','upper'])
def test_static_legal_tick_keeps_its_direction_despite_float_representation(boundary):
    lo=22.450000000000003;hi=27.350000000000005
    rules=pl.DataFrame(dict(upper_limit=[hi],lower_limit=[lo]))
    quote=22.45 if boundary=='lower' else 27.35
    buy,sell=price_limit_permissions(rules,[quote])
    assert bool(buy[0])==(boundary=='lower')
    assert bool(sell[0])==(boundary=='upper')


@pytest.mark.parametrize('quote',[89.,111.])
def test_decision_clock_rejects_static_out_of_range_quote_without_changing_accounting(quote):
    from test_tw_futures_decision_clock import source,execution
    from stockagent.data.tw_futures_decision_clock import reprice_margin_entries
    facts=source(quote,'observed_minute').with_columns(
        pl.lit(110.).alias('upper_limit'),pl.lit(90.).alias('lower_limit'))
    result=reprice_margin_entries(execution(),facts,participation=.5,broker_multiplier=1.)
    assert not bool(result[0,18]) and not bool(result[0,19])
    np.testing.assert_array_equal(result[0,29:33],execution()[0,29:33])
    assert result[0,25]==execution()[0,25]


@pytest.mark.parametrize('failure',['no_manifest','not_research','invented_exact','bad_contract',
    'wrong_ladder','reversed_ladder','future_early','partial_certificate','bad_reference','wrong_rounding',
    'unbound_static_certificate','null_reference'])
def test_invalid_certificates_are_rejected(failure):
    f=certificate();manifest=dict(MANIFEST)
    if failure=='no_manifest':manifest.pop('price_limit_permission_contract')
    elif failure=='not_research':manifest['research_only']=False
    elif failure=='invented_exact':f=f.with_columns(pl.lit(115.).alias('upper_limit'))
    elif failure=='bad_contract':f=f.with_columns(pl.lit('guessed_stage3').alias('price_limit_permission_contract'))
    elif failure=='wrong_ladder':f=f.with_columns(pl.lit([1.05,1.08,1.15]).alias('price_limit_stage_up_ratios'))
    elif failure=='reversed_ladder':f=f.with_columns(pl.lit([1.15,1.10,1.07]).alias('price_limit_stage_up_ratios'))
    elif failure=='future_early':f=f.with_columns(pl.lit(date(2026,7,3)).alias('date'))
    elif failure=='partial_certificate':f=f.drop('price_limit_inner_upper')
    elif failure=='bad_reference':f=f.with_columns(pl.lit(float('inf')).alias('price_limit_reference_quote'))
    elif failure=='wrong_rounding':f=f.with_columns(pl.lit(107.001).alias('price_limit_inner_upper'))
    elif failure=='unbound_static_certificate':f=f.with_columns(pl.lit(None,dtype=pl.String).alias('price_limit_permission_contract'))
    elif failure=='null_reference':f=f.with_columns(pl.lit(None,dtype=pl.Float64).alias('price_limit_reference_quote'))
    with pytest.raises(ValueError):validate_price_limit_rules(f,manifest)


@pytest.mark.parametrize('failure',['early','stock_tick','flat_ratio','missing_ladder','unsorted_ladder'])
def test_staged_specification_requires_its_exact_source_contract(failure):
    row=staged_spec()
    if failure=='early':row['effective_date']=date(2026,7,3)
    elif failure=='stock_tick':row['tick_kind']='stock_future'
    elif failure=='flat_ratio':row['limit_up_ratio']=1.15
    elif failure=='missing_ladder':row['limit_stage_down_ratios']=None
    elif failure=='unsorted_ladder':row['limit_stage_up_ratios']=[1.10,1.07,1.15]
    f=pl.DataFrame([row],schema_overrides={'limit_stage_up_ratios':pl.List(pl.Float64),
        'limit_stage_down_ratios':pl.List(pl.Float64)})
    with pytest.raises(ValueError):
        bind_product_specifications(pl.DataFrame({'date':[date(2026,7,6)],'product':['ABC']}),f)


def test_compiler_keeps_exact_limits_unknown_and_uses_canonical_prior_reference():
    data=inputs();delta=timedelta(days=182)
    for key in ('frame','margins','positions'):
        frame=data[key]
        names=[c for c in ('date','previous_market_date','next_market_date','previous_symbol_date') if c in frame.columns]
        data[key]=frame.with_columns((pl.col(c)+delta).alias(c) for c in names)
    data['specifications']=pl.DataFrame([staged_spec()],schema_overrides={
        'limit_stage_up_ratios':pl.List(pl.Float64),'limit_stage_down_ratios':pl.List(pl.Float64)})
    rules,flags=compile_execution_terms(**data)
    assert rules.height==3
    assert rules['upper_limit'].null_count()==rules['lower_limit'].null_count()==3
    assert rules['price_limit_permission_contract'].to_list()==[STAGED_INNER_BAND_CONTRACT]*3
    np.testing.assert_array_equal(rules['price_limit_reference_quote'].to_numpy(),[102.,103.,104.])
    assert not flags.filter(~pl.col('is_warmup'))['missing_price_limits'].any()
    validate_price_limit_rules(rules,MANIFEST)


@pytest.mark.parametrize('quote,expected',[(100.,True),(107.,False),(108.,False),(None,False)])
def test_decision_clock_uses_the_same_guard_without_changing_accounting(quote,expected):
    from test_tw_futures_decision_clock import source,execution
    from stockagent.data.tw_futures_decision_clock import reprice_margin_entries
    facts=source(quote, 'observed_minute' if quote is not None else 'verified_no_window_trade').with_columns(
        *[certificate()[c] for c in STAGED_LIMIT_FIELDS],
        pl.lit(None,dtype=pl.Float64).alias('upper_limit'),pl.lit(None,dtype=pl.Float64).alias('lower_limit'))
    result=reprice_margin_entries(execution(),facts,participation=.5,broker_multiplier=1.)
    assert bool(result[0,18])==bool(result[0,19])==expected
    np.testing.assert_array_equal(result[0,29:33],execution()[0,29:33])
    assert result[0,25]==execution()[0,25]


def source_delta(tmp_path):
    source=tmp_path/'source';delta=tmp_path/'delta';spec=source/'specifications';overlay=delta/'specification_delta'
    spec.mkdir(parents=True);overlay.mkdir(parents=True)
    original=pl.DataFrame([specification(valid_until_exclusive=date(2026,7,6),limit_kind='ratio',
        carry_split_numerator=1,carry_split_denominator=1)])
    atomic_write_parquet(spec/'specifications.parquet',original)
    atomic_write_json(spec/'manifest.json',dict(outputs={'specifications.parquet':dict(sha256=sha256_file(spec/'specifications.parquet'))}))
    atomic_write_json(source/'source_manifest.json',{'source':'immutable_parent'})
    combined=pl.concat([original,pl.DataFrame([staged_spec()])],how='diagonal_relaxed')
    target=overlay/'specifications.parquet';atomic_write_parquet(target,combined)
    atomic_write_json(overlay/'manifest.json',dict(outputs={'specifications.parquet':dict(sha256=sha256_file(target))}))
    proof=dict(contract='append_only_staged_etf_specifications_v1',path='specification_delta/specifications.parquet',
        sha256=sha256_file(target),parent_specifications_sha256=sha256_file(spec/'specifications.parquet'))
    manifest=dict(parent_source_manifest_sha256=sha256_file(source/'source_manifest.json'),product_specification_delta=proof)
    atomic_write_json(delta/'manifest.json',manifest)
    return source,delta,combined


def test_dated_specification_delta_appends_without_touching_old_intervals(tmp_path):
    source,delta,expected=source_delta(tmp_path)
    assert load_dated_product_specifications(source,delta).equals(expected)
    assert load_dated_product_specifications(source).height==1


@pytest.mark.parametrize('bad_review', [False, True])
def test_reviewed_dated_delta_can_add_static_rules_and_cash_posting_without_rewriting_parent(tmp_path, bad_review):
    source, delta, _ = source_delta(tmp_path)
    original = pl.read_parquet(source/'specifications/specifications.parquet')
    added = pl.DataFrame([specification(product='F1F', effective_date=date(2026,7,6),
        expiry_time='17:15:00', final_cash_accounting_policy='official_final_day_end_v1',
        final_cash_accounting_time='17:00:00', limit_kind='ratio',
        carry_split_numerator=1, carry_split_denominator=1)])
    combined = pl.concat([original.with_columns(
        pl.lit('expiry_event_time').alias('final_cash_accounting_policy')),
        added], how='diagonal_relaxed').sort('product','effective_date')
    path = delta/'specification_delta/specifications.parquet'
    review = path.parent/'dated_source_review.json'
    atomic_write_parquet(path, combined)
    atomic_write_json(path.with_name('manifest.json'), dict(outputs={
        'specifications.parquet':dict(sha256=sha256_file(path))}))
    payload = dict(contract='source_reviewed_dated_specification_rows_v2',
        specifications=json.loads(combined.filter(pl.col('product')=='F1F').write_json()),
        source_content_sha256s=['a'*64])
    if bad_review:
        payload['specifications'][0]['final_cash_accounting_time']='09:30:00'
    atomic_write_json(review, payload)
    manifest=json.loads((delta/'manifest.json').read_text())
    manifest['product_specification_delta'].update(
        contract='append_only_reviewed_dated_specifications_v3', sha256=sha256_file(path),
        review=dict(path='specification_delta/dated_source_review.json', sha256=sha256_file(review)))
    atomic_write_json(delta/'manifest.json', manifest)
    if bad_review:
        with pytest.raises(ValueError, match='explicit source review'):
            load_dated_product_specifications(source, delta)
    else:
        result=load_dated_product_specifications(source, delta)
        assert result.equals(combined)
        assert result.filter(pl.col('product')=='ABC').select(original.columns).equals(original)


def fixed_staged_spec(**updates):
    row=staged_spec()
    row.update(effective_date=date(2017,5,15),known_at='2017-04-18T12:00:00+08:00',
        tick_kind='fixed',tick_size=.25,limit_stage_up_ratios=[1.07,1.13,1.20],
        limit_stage_down_ratios=[.93,.87,.80])
    return dict(row,**updates)


def fixed_certificate(reference=2367.75,tick=.25):
    return pl.DataFrame(dict(date=[date(2017,5,16)],upper_limit=[None],lower_limit=[None],
        price_limit_permission_contract=[STAGED_FIXED_INNER_BAND_CONTRACT],
        price_limit_reference_quote=[reference],price_limit_stage_up_ratios=[[1.07,1.13,1.20]],
        price_limit_stage_down_ratios=[[.93,.87,.80]],price_limit_fixed_tick=[tick],
        price_limit_inner_upper=fixed_ratio_band([reference],[1.07],[tick],upper=True),
        price_limit_inner_lower=fixed_ratio_band([reference],[.93],[tick],upper=False)),
        schema_overrides={'upper_limit':pl.Float64,'lower_limit':pl.Float64,**STAGED_LIMIT_FIELDS})


@pytest.mark.parametrize('up,down,tick', [
    ([1.05,1.10,1.20],[.95,.90,.80],.5),
    ([1.10,1.15,1.20],[.90,.85,.80],1.),
    ([1.08,1.12,1.16],[.92,.88,.84],.25),
    ([1.05,1.10,1.15],[.95,.90,.85],.5),
])
def test_reviewed_historical_ladders_compile_without_guessing_an_active_stage(up,down,tick):
    data=inputs()
    spec=fixed_staged_spec(effective_date=date(2016,1,1),known_at='2015-12-01T12:00:00+08:00',
        limit_stage_up_ratios=up,limit_stage_down_ratios=down,tick_size=tick,
        fixed_staged_ladder_contract='reviewed_fixed_ladder_v3')
    data['specifications']=pl.DataFrame([spec])
    rules,flags=compile_execution_terms(**data)
    contract=STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT
    assert price_limit_release_contract(rules)==contract
    validate_price_limit_rules(rules,dict(research_only=True,price_limit_permission_contract=contract))
    assert not flags.filter(~pl.col('is_warmup'))['missing_price_limits'].any()
    assert rules['upper_limit'].null_count()==rules['lower_limit'].null_count()==rules.height
    # Independently enumerate both sides' possible stages. Every admitted
    # quote must be strictly legal under all nine combinations.
    first=rules.head(1)
    lo=first['price_limit_inner_lower'][0];hi=first['price_limit_inner_upper'][0]
    quotes=np.array([lo-tick,lo,lo+tick,hi-tick,hi,hi+tick])
    expanded=pl.concat([first]*len(quotes));buy,sell=price_limit_permissions(expanded,quotes)
    np.testing.assert_array_equal(buy,[False,False,True,True,False,False])
    np.testing.assert_array_equal(sell,buy)
    reference=first['price_limit_reference_quote'][0]
    for u in up:
        for d in down:
            assert np.all(quotes[buy]<fixed_ratio_band([reference],[u],[tick],upper=True)[0])
            assert np.all(quotes[sell]>fixed_ratio_band([reference],[d],[tick],upper=False)[0])
    # An unreviewed non-v2 ladder cannot enter the compiler.
    with pytest.raises(ValueError,match='unsupported staged'):
        bind_product_specifications(data['frame'],data['specifications'].drop('fixed_staged_ladder_contract'))
    mixed=pl.concat([certificate(),fixed_certificate(),first],how='diagonal_relaxed')
    assert price_limit_release_contract(mixed)==contract
    validate_price_limit_rules(mixed,dict(research_only=True,price_limit_permission_contract=contract))
    with pytest.raises(ValueError):
        validate_price_limit_rules(first,dict(research_only=True,
            price_limit_permission_contract=STAGED_FIXED_INNER_BAND_CONTRACT))


@pytest.mark.parametrize('up,down', [
    ([1.1,1.1,1.2],[.9,.85,.8]), ([1.,1.1,1.2],[.9,.85,.8]),
    ([1.05,1.1,1.2],[.95,.8,.9]), ([1.05,float('nan'),1.2],[.95,.9,.8]),
    ([1.05,1.1],[.95,.9,.8]), ([1.05,1.1,1.2],[.95,.9,0.]),
])
def test_reviewed_ladder_rejects_invalid_stage_order_and_values(up,down):
    spec=fixed_staged_spec(limit_stage_up_ratios=up,limit_stage_down_ratios=down,
        fixed_staged_ladder_contract='reviewed_fixed_ladder_v3')
    with pytest.raises(ValueError,match='unsupported staged'):
        bind_product_specifications(inputs()['frame'],pl.DataFrame([spec]))


@pytest.mark.parametrize('reference,tick',[(20805.,1.),(2367.75,.25),(102.1,.25)])
def test_historical_fixed_tick_inner_band_is_safe_for_every_independent_stage(reference,tick):
    f=fixed_certificate(reference,tick)
    manifest=dict(research_only=True,price_limit_permission_contract=STAGED_FIXED_INNER_BAND_CONTRACT)
    validate_price_limit_rules(f,manifest)
    lo=f['price_limit_inner_lower'][0];hi=f['price_limit_inner_upper'][0]
    # Compare to integer arithmetic, independently of the compiler's rounding.
    from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
    assert hi==float((Decimal(str(reference))*Decimal('1.07')/Decimal(str(tick)))
                    .to_integral_value(rounding=ROUND_FLOOR)*Decimal(str(tick)))
    assert lo==float((Decimal(str(reference))*Decimal('.93')/Decimal(str(tick)))
                    .to_integral_value(rounding=ROUND_CEILING)*Decimal(str(tick)))
    q=np.array([lo-tick,lo,lo+tick,hi-tick,hi,hi+tick,np.nan])
    buy,sell=price_limit_permissions(pl.concat([f]*len(q)),q)
    np.testing.assert_array_equal(buy,[False,False,True,True,False,False,False])
    np.testing.assert_array_equal(sell,buy)
    assert f['upper_limit'].null_count()==f['lower_limit'].null_count()==1


@pytest.mark.parametrize('failure',['old_manifest','tick_missing','tick_zero','tick_nan',
    'wrong_tick_band','wrong_ladder','early','invented_exact'])
def test_fixed_tick_certificate_rejects_unbound_or_invented_rules(failure):
    f=fixed_certificate();m=dict(research_only=True,price_limit_permission_contract=STAGED_FIXED_INNER_BAND_CONTRACT)
    if failure=='old_manifest':m['price_limit_permission_contract']=STAGED_INNER_BAND_CONTRACT
    elif failure=='tick_missing':f=f.drop('price_limit_fixed_tick')
    elif failure=='tick_zero':f=f.with_columns(pl.lit(0.).alias('price_limit_fixed_tick'))
    elif failure=='tick_nan':f=f.with_columns(pl.lit(float('nan')).alias('price_limit_fixed_tick'))
    elif failure=='wrong_tick_band':f=f.with_columns(pl.lit(2533.493).alias('price_limit_inner_upper'))
    elif failure=='wrong_ladder':f=f.with_columns(pl.lit([1.07,1.10,1.15]).alias('price_limit_stage_up_ratios'))
    elif failure=='early':f=f.with_columns(pl.lit(date(2017,5,12)).alias('date'))
    elif failure=='invented_exact':f=f.with_columns(pl.lit(2800.).alias('upper_limit'))
    with pytest.raises(ValueError):validate_price_limit_rules(f,m)


def test_mixed_legacy_etf_and_fixed_certificates_require_new_release_contract():
    f=pl.concat([certificate(),fixed_certificate()],how='diagonal_relaxed')
    assert price_limit_release_contract(f)==STAGED_FIXED_INNER_BAND_CONTRACT
    validate_price_limit_rules(f,dict(research_only=True,
        price_limit_permission_contract=STAGED_FIXED_INNER_BAND_CONTRACT))
    with pytest.raises(ValueError):validate_price_limit_rules(f,MANIFEST)
    assert price_limit_release_contract(certificate())==STAGED_INNER_BAND_CONTRACT


def test_compiler_binds_fixed_source_tick_and_preserves_canonical_account_values():
    data=inputs();row=fixed_staged_spec()
    data['specifications']=pl.DataFrame([row],schema_overrides={
        'limit_stage_up_ratios':pl.List(pl.Float64),'limit_stage_down_ratios':pl.List(pl.Float64)})
    rules,flags=compile_execution_terms(**data)
    assert rules['price_limit_permission_contract'].to_list()==[STAGED_FIXED_INNER_BAND_CONTRACT]*3
    np.testing.assert_array_equal(rules['price_limit_reference_quote'],[102.,103.,104.])
    np.testing.assert_array_equal(rules['price_limit_fixed_tick'],[.25]*3)
    assert rules['upper_limit'].null_count()==rules['lower_limit'].null_count()==3
    assert not flags.filter(~pl.col('is_warmup'))['missing_price_limits'].any()
    validate_price_limit_rules(rules,dict(research_only=True,
        price_limit_permission_contract=STAGED_FIXED_INNER_BAND_CONTRACT))
    # Same static contract differs only in the limit declaration, not valuation,
    # fees, whole-contract multiplier or margin inputs.
    row.update(limit_kind='ratio',limit_stage_up_ratios=None,limit_stage_down_ratios=None,
               limit_up_ratio=1.2,limit_down_ratio=.8)
    data['specifications']=pl.DataFrame([row],schema_overrides={
        'limit_stage_up_ratios':pl.List(pl.Float64),'limit_stage_down_ratios':pl.List(pl.Float64)})
    control,_=compile_execution_terms(**data)
    shared=[c for c in control.columns if c not in {'upper_limit','lower_limit'}]
    assert rules.select(shared).equals(control.select(shared))


@pytest.mark.parametrize('failure',[None,'review_hash','review_values','unreviewed_source','escape','old_interval'])
def test_reviewed_fixed_specification_delta_requires_pinned_exact_rows(tmp_path,failure):
    source,delta,combined=source_delta(tmp_path)
    row=fixed_staged_spec(product='SPF',valid_until_exclusive=date(2020,11,23))
    combined=pl.concat([combined,pl.DataFrame([row])],how='diagonal_relaxed')
    path=delta/'specification_delta/specifications.parquet'
    atomic_write_parquet(path,combined)
    atomic_write_json(path.with_name('manifest.json'),dict(outputs={'specifications.parquet':dict(sha256=sha256_file(path))}))
    original=pl.read_parquet(source/'specifications/specifications.parquet')
    added=combined.join(original.select('product','effective_date'),on=['product','effective_date'],how='anti')
    review=delta/'specification_delta/source_review.json'
    payload=dict(contract='source_reviewed_staged_specification_rows_v1',
        specifications=json.loads(added.sort('product','effective_date').write_json()),source_content_sha256s=['a'*64])
    if failure=='review_values':payload['specifications'][0]['tick_size']=999.
    if failure=='unreviewed_source':payload['source_content_sha256s']=['b'*64]
    atomic_write_json(review,payload)
    manifest=json.loads((delta/'manifest.json').read_text())
    proof=manifest['product_specification_delta']
    proof.update(contract='append_only_reviewed_staged_specifications_v2',sha256=sha256_file(path),
        review=dict(path='specification_delta/source_review.json',sha256=sha256_file(review)))
    if failure=='review_hash':proof['review']['sha256']='b'*64
    if failure=='escape':proof['review']['path']='../source_review.json'
    if failure=='old_interval':
        combined=combined.with_columns(pl.when(pl.col('limit_kind')=='ratio').then(999.)
            .otherwise(pl.col('contract_multiplier')).alias('contract_multiplier'))
        atomic_write_parquet(path,combined);proof['sha256']=sha256_file(path)
        atomic_write_json(path.with_name('manifest.json'),dict(outputs={'specifications.parquet':dict(sha256=sha256_file(path))}))
    atomic_write_json(delta/'manifest.json',manifest)
    if failure:
        with pytest.raises(ValueError):load_dated_product_specifications(source,delta)
    else:
        assert load_dated_product_specifications(source,delta).equals(combined)


@pytest.mark.parametrize('failure',['old_value','parent_hash','file_hash','path','wrong_boundary'])
def test_specification_delta_rejects_changes_outside_its_source_scope(tmp_path,failure):
    source,delta,combined=source_delta(tmp_path)
    path=delta/'specification_delta/specifications.parquet';manifest=json.loads((delta/'manifest.json').read_text())
    if failure in ('old_value','wrong_boundary'):
        if failure=='old_value':combined=combined.with_columns(pl.when(pl.col('limit_kind')=='ratio').then(999.)
            .otherwise(pl.col('contract_multiplier')).alias('contract_multiplier'))
        else:combined=combined.with_columns(pl.when(pl.col('limit_kind')=='staged_ratio').then(pl.lit(date(2026,7,7)))
            .otherwise(pl.col('effective_date')).alias('effective_date'))
        atomic_write_parquet(path,combined);h=sha256_file(path)
        manifest['product_specification_delta']['sha256']=h
        atomic_write_json(path.with_name('manifest.json'),dict(outputs={'specifications.parquet':dict(sha256=h)}))
    elif failure=='parent_hash':manifest['product_specification_delta']['parent_specifications_sha256']='a'*64
    elif failure=='file_hash':manifest['product_specification_delta']['sha256']='a'*64
    elif failure=='path':manifest['product_specification_delta']['path']='../escape.parquet'
    atomic_write_json(delta/'manifest.json',manifest)
    with pytest.raises(ValueError):load_dated_product_specifications(source,delta)
